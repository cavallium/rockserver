package it.cavallium.rockserver.distributed;

import io.grpc.Status;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Test client for a cluster: one gRPC connection per node, with failover.
 *
 * <p>Each call starts on the node that last answered. On a retryable failure ({@link #isRetryable}) it moves to the
 * next node and tries again until the retry timeout; every attempt uses the given {@link RequestContext}, so its
 * timeout bounds a single attempt. The core client itself retries reads on {@code UNAVAILABLE} with backoff, so a
 * read sent to a node that just died fails over only after a few seconds or its request timeout, whichever comes
 * first: fault tests should use short request timeouts.</p>
 *
 * <p>A connection knows one node only, and the core client sends a numbered write (with sessions on: every write
 * outside a transaction) again on it until the call's deadline, whatever the failure, and then reports an unknown
 * outcome: a refused connection no longer surfaces as one. So before a call on an open connection whose node has not
 * answered this client for a second, the node's port is asked for a TCP connection, and a refusal moves the call to
 * the next node with nothing sent (R62-5 and its review). A node that goes down within that second is found the old
 * way: the write waits out its deadline there and is {@code UNKNOWN}.</p>
 *
 * <p>The first attempt on a node opens its connection, whose handshake waits up to 10 seconds for an answer. A
 * frozen node (SIGSTOP) still accepts TCP connections, so the attempt waits for the handshake only as long as the
 * request timeout and the remaining retry time allow, then moves on; the handshake goes on in the background and
 * later attempts reuse its connection once it completes.</p>
 *
 * <p>Every call reports an {@link OpStatus}. A read has no effect, so it is {@code OK} or {@code FAILED}. A write is
 * {@code OK} only when an attempt succeeded and no earlier attempt may have reached a server; it is {@code FAILED}
 * only when no attempt may have reached a server; otherwise it is {@code UNKNOWN}, even when a later attempt
 * succeeded, because the earlier one may still be applied after this call returns. Only a refused connection
 * proves that a request was not delivered: any server-reported error is treated as possibly applied, since a
 * forwarded write can fail after its leader committed it.</p>
 *
 * <p>Retrying a write after an ambiguous failure can apply it twice, as there is no exactly-once retry yet. That is
 * fine for availability tests but breaks register histories, so it is a constructor option.</p>
 */
public final class ClusterClient implements AutoCloseable {

	private static final Duration ROUND_PAUSE = Duration.ofMillis(100);
	private static final int PROBE_TIMEOUT_MILLIS = 1_000;
	/**
	 * A node that has not answered this client for this long is asked for a TCP connection before a call on its open
	 * connection. Not before every call: a workload's calls follow each other within milliseconds.
	 */
	private static final Duration QUIET_BEFORE_PROBE = Duration.ofSeconds(1);
	private static final Set<RocksDBErrorType> RETRYABLE_TYPES = Set.of(RocksDBErrorType.SERVER_OVERLOADED,
			RocksDBErrorType.OUTCOME_UNKNOWN,
			RocksDBErrorType.UPDATE_RETRY,
			RocksDBErrorType.READ_DEADLINE_EXCEEDED,
			RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN);
	private static final Set<Status.Code> RETRYABLE_CODES = Set.of(Status.Code.UNAVAILABLE,
			Status.Code.DEADLINE_EXCEEDED,
			Status.Code.ABORTED,
			Status.Code.RESOURCE_EXHAUSTED);

	private final List<String> nodeIds;
	private final Map<String, HostAndPort> addresses;
	private final Duration retryTimeout;
	private final boolean retryAmbiguousWrites;
	private final Map<String, CompletableFuture<GrpcConnection>> connections = new ConcurrentHashMap<>();
	/** When each node last answered a call of this client ({@code System.nanoTime}). */
	private final Map<String, Long> lastAnswer = new ConcurrentHashMap<>();
	private volatile String preferred;

	/**
	 * @param addresses API address of every node, in failover order
	 * @param retryTimeout how long one call keeps retrying
	 * @param retryAmbiguousWrites whether to retry a write after a failure that may have applied it
	 */
	public ClusterClient(Map<String, HostAndPort> addresses, Duration retryTimeout, boolean retryAmbiguousWrites) {
		if (addresses.isEmpty()) {
			throw new IllegalArgumentException("No nodes");
		}
		this.nodeIds = List.copyOf(addresses.keySet());
		this.addresses = Map.copyOf(addresses);
		this.retryTimeout = retryTimeout;
		this.retryAmbiguousWrites = retryAmbiguousWrites;
		this.preferred = nodeIds.getFirst();
	}

	/**
	 * Outcome of one call.
	 *
	 * @param value the result of the successful attempt, null if none succeeded
	 * @param error the failure of the last attempt, null if an attempt succeeded
	 * @param node the node of the last attempt
	 */
	public record Result<T>(OpStatus status, @Nullable T value, @Nullable RuntimeException error, String node,
			int attempts) {

		/** True if an attempt succeeded, so {@link #value} is its result. */
		public boolean acknowledged() {
			return error == null;
		}

		public T valueOrThrow() {
			if (error != null) {
				throw error;
			}
			return value;
		}
	}

	/** Statements of a transaction, run on the node that opened it. Re-run from scratch on every attempt. */
	@FunctionalInterface
	public interface TransactionBody {
		void run(RocksDBSyncAPI api, long transactionId);
	}

	public Result<Long> createColumn(String name, ColumnSchema schema, RequestContext context) {
		return write(context, api -> api.createColumn(name, schema));
	}

	public Result<Long> getColumnId(String name, RequestContext context) {
		return read(context, api -> api.getColumnId(name));
	}

	public Result<Void> put(long columnId, Keys keys, Buf value, RequestContext context) {
		return write(context, api -> api.put(0, columnId, keys, value, RequestType.none()));
	}

	public Result<Void> merge(long columnId, Keys keys, Buf value, RequestContext context) {
		return write(context, api -> api.merge(0, columnId, keys, value, RequestType.none()));
	}

	/** The current value, null if absent. */
	public Result<Buf> get(long columnId, Keys keys, RequestContext context) {
		return read(context, api -> api.get(0, columnId, keys, RequestType.current()));
	}

	/**
	 * Opens a transaction, runs {@code body} and commits, all on one node. A failure before the commit is sent
	 * leaves the database unchanged, so it never makes the outcome ambiguous; a retryable one restarts the whole
	 * transaction on the next node. The value is the commit result: false means a conflict, nothing applied
	 * (the transaction is then rolled back).
	 */
	public Result<Boolean> transaction(RequestContext context, Duration leaseTtl, TransactionBody body) {
		return execute(context, true, api -> {
			long transactionId;
			try {
				transactionId = api.openTransaction(leaseTtl);
			} catch (RuntimeException e) {
				throw new NotApplied(e);
			}
			try {
				body.run(api, transactionId);
			} catch (RuntimeException e) {
				abort(api, transactionId);
				throw new NotApplied(e);
			}
			boolean committed = api.closeTransaction(transactionId, true);
			if (!committed) {
				// A conflicting commit leaves the transaction open: release it instead of waiting for its lease.
				abort(api, transactionId);
			}
			return committed;
		});
	}

	/** Runs an arbitrary write with failover. */
	public <T> Result<T> write(RequestContext context, Function<RocksDBSyncAPI, T> operation) {
		return execute(context, true, operation);
	}

	/** Runs an arbitrary read with failover. */
	public <T> Result<T> read(RequestContext context, Function<RocksDBSyncAPI, T> operation) {
		return execute(context, false, operation);
	}

	/** Direct access to one node, without failover. */
	public RocksDBSyncAPI api(String nodeId, RequestContext context) {
		return connection(nodeId, context.timeoutNanos()).getSyncApi(context);
	}

	/** True for failures worth retrying on another node: overload, conflicts, timeouts, unreachable nodes. */
	public static boolean isRetryable(Throwable error) {
		if (error instanceof RocksDBException e) {
			return RETRYABLE_TYPES.contains(e.getErrorUniqueId());
		}
		return RETRYABLE_CODES.contains(Status.fromThrowable(error).getCode());
	}

	@Override
	public void close() {
		// A handshake still in progress closes its connection when it completes.
		connections.values().forEach(pending -> pending.thenAccept(GrpcConnection::close));
		connections.clear();
	}

	private <T> Result<T> execute(RequestContext context, boolean mutation, Function<RocksDBSyncAPI, T> operation) {
		long deadline = System.nanoTime() + retryTimeout.toNanos();
		boolean ambiguous = false;
		int attempts = 0;
		String node = preferred;
		while (true) {
			attempts++;
			long connectTimeout = Math.min(context.timeoutNanos(), Math.max(0, deadline - System.nanoTime()));
			var attempt = attempt(node, context, connectTimeout, operation);
			if (attempt.error() == null) {
				preferred = node;
				var status = mutation && ambiguous ? OpStatus.UNKNOWN : OpStatus.OK;
				return new Result<>(status, attempt.value(), null, node, attempts);
			}
			ambiguous |= mutation && attempt.delivered();
			boolean retry = attempt.retryable()
					&& (!mutation || !ambiguous || retryAmbiguousWrites)
					&& System.nanoTime() < deadline;
			if (!retry) {
				var status = ambiguous ? OpStatus.UNKNOWN : OpStatus.FAILED;
				return new Result<>(status, null, attempt.error(), node, attempts);
			}
			node = nextNode(node);
			if (attempts % nodeIds.size() == 0) {
				pauseBeforeNextRound(deadline);
			}
		}
	}

	/**
	 * One attempt on one node.
	 *
	 * @param delivered false only if the request certainly did not reach the node, or certainly left it unchanged
	 */
	private record Attempt<T>(@Nullable T value, @Nullable RuntimeException error, boolean delivered,
			boolean retryable) {}

	private <T> Attempt<T> attempt(String node, RequestContext context, long connectTimeoutNanos,
			Function<RocksDBSyncAPI, T> operation) {
		GrpcConnection connection;
		try {
			connection = connection(node, connectTimeoutNanos);
		} catch (RuntimeException e) {
			// Unreachable or frozen node: no connection, so no request was sent. Always worth another node.
			return new Attempt<>(null, e, false, true);
		}
		var answered = lastAnswer.get(node);
		if (answered != null && System.nanoTime() - answered > QUIET_BEFORE_PROBE.toNanos()) {
			try {
				probe(addresses.get(node));
			} catch (UncheckedIOException e) {
				if (isRefusedConnection(e)) {
					// The node went down since it last answered: nothing is sent, and the call moves on. Without this a
					// numbered write (R62-5) would be sent again to this one address until the call's own deadline and
					// end as of unknown outcome, since the connection knows no other node.
					return new Attempt<>(null, e, false, true);
				}
				// A node that does not answer the probe in time (frozen): the call finds out, as before.
			}
		}
		try {
			var value = operation.apply(connection.getSyncApi(context));
			lastAnswer.put(node, System.nanoTime());
			return new Attempt<>(value, null, true, false);
		} catch (NotApplied e) {
			return new Attempt<>(null, e.getCause(), false, isRetryable(e.getCause()));
		} catch (RuntimeException e) {
			return new Attempt<>(null, e, !isRefusedConnection(e), isRetryable(e));
		}
	}

	/**
	 * The node's connection, opening it if needed. Waits at most {@code timeoutNanos} for the handshake, which
	 * keeps going after a timeout: concurrent and later callers wait on the same one, and a failed one is replaced.
	 */
	private GrpcConnection connection(String nodeId, long timeoutNanos) {
		var address = addresses.get(nodeId);
		if (address == null) {
			throw new IllegalArgumentException("Unknown node " + nodeId);
		}
		var pending = connections.compute(nodeId, (id, current) ->
				current == null || current.isCompletedExceptionally() ? connect(id, address) : current);
		try {
			return pending.get(timeoutNanos, TimeUnit.NANOSECONDS);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof RuntimeException cause) {
				throw cause;
			}
			throw new IllegalStateException("Cannot connect to node " + nodeId, e.getCause());
		} catch (TimeoutException e) {
			throw new IllegalStateException("Node " + nodeId + " did not complete the handshake within "
					+ Duration.ofNanos(timeoutNanos), e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while connecting to node " + nodeId, e);
		}
	}

	/** Opens a connection on a virtual thread. The constructor performs the handshake and throws if it fails. */
	private static CompletableFuture<GrpcConnection> connect(String nodeId, HostAndPort address) {
		var connection = new CompletableFuture<GrpcConnection>();
		Thread.ofVirtual().name("cluster-client-connect-" + nodeId).start(() -> {
			try {
				probe(address);
				connection.complete(GrpcConnection.forHostAndPort("cluster-client-" + nodeId, address));
			} catch (Throwable e) {
				connection.completeExceptionally(e);
			}
		});
		return connection;
	}

	/** The handshake retries a refused connection for seconds: fail fast on a node that is down instead. */
	private static void probe(HostAndPort address) {
		try (var socket = new Socket()) {
			// Closed with a reset: no port of this host waits out a TIME_WAIT for a probe.
			socket.setSoLinger(true, 0);
			socket.connect(new InetSocketAddress(address.host(), address.port()), PROBE_TIMEOUT_MILLIS);
		} catch (IOException e) {
			throw new UncheckedIOException("Node unreachable at " + address, e);
		}
	}

	private String nextNode(String node) {
		return nodeIds.get((nodeIds.indexOf(node) + 1) % nodeIds.size());
	}

	/** Every node failed once: wait a little (an election, a restart) instead of spinning. */
	private static void pauseBeforeNextRound(long deadlineNanos) {
		long pause = Math.min(ROUND_PAUSE.toNanos(), deadlineNanos - System.nanoTime());
		if (pause > 0) {
			try {
				Thread.sleep(Duration.ofNanos(pause));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Interrupted while retrying", e);
			}
		}
	}

	/** A refused TCP connection is the only failure proving that the request never reached the node. */
	private static boolean isRefusedConnection(Throwable error) {
		for (Throwable cause = error; cause != null; cause = cause.getCause()) {
			if (cause instanceof ConnectException) {
				return true;
			}
		}
		return false;
	}

	private static void abort(RocksDBSyncAPI api, long transactionId) {
		try {
			api.closeTransaction(transactionId, false);
		} catch (RuntimeException ignored) {
			// The transaction dies with its node or its leadership anyway.
		}
	}

	/** Wraps a failure that certainly left the database unchanged. */
	private static final class NotApplied extends RuntimeException {
		NotApplied(RuntimeException cause) {
			super(cause);
		}

		@Override
		public synchronized RuntimeException getCause() {
			return (RuntimeException) super.getCause();
		}
	}
}
