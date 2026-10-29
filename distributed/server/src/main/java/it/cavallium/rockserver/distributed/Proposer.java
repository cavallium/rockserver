package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import java.io.Closeable;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import java.util.function.Supplier;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.jetbrains.annotations.Nullable;

/**
 * Submits log entries to the current leader through a Ratis client, from any node.
 *
 * <p>Every proposal is a numbered {@link Request}. It is resent (leader changes, unreachable nodes, timeouts) with
 * the same number until it gets an answer or the request timeout passes, and applying a copy of a request that
 * already took effect returns the recorded result instead of executing it again. So a write takes effect at most
 * once even when copies reach a new leader or a restarted node. Each attempt is a single Ratis call, which follows
 * the leader hints Ratis returns; the deadline bounds the whole proposal.</p>
 *
 * <p>Each proposal is an independent blocking call on its own virtual thread, not a request of the client's
 * ordered async stream: once one request of that stream runs out of retries, Ratis closes the stream for good, and
 * every later proposal of this node would fail without being sent.</p>
 */
final class Proposer implements Closeable {

	/** The leader rejected a transaction commit because one of its keys changed after its snapshot. */
	static final class ConflictException extends RocksDBRetryException {}

	private static final long NOT_READY_RETRY_MS = 50;
	private static final Duration MIN_ATTEMPT_TIMEOUT = Duration.ofSeconds(3);
	private static final long ATTEMPT_SLEEP_MS = 100;

	private final long proposerId = new SecureRandom().nextLong();
	private final AtomicLong nextSequence = new AtomicLong();
	private final ConcurrentSkipListSet<Long> inFlight = new ConcurrentSkipListSet<>();
	private final RaftClient client;
	private final Supplier<RaftServer> localServer;
	private final RaftGroupId groupId;
	private final RaftPeerId self;
	private final ClientId localClientId = ClientId.randomId();
	private final AtomicLong localCallIds = new AtomicLong();
	private final ExecutorService sender = Executors.newThreadPerTaskExecutor(
			Thread.ofVirtual().name("rockserver-propose-", 0).factory());
	private final long requestTimeoutNanos;

	Proposer(RaftGroup group, RaftProperties properties, NodeConfig config, Supplier<RaftServer> localServer) {
		this.requestTimeoutNanos = config.requestTimeout().toNanos();
		this.localServer = localServer;
		this.groupId = group.getGroupId();
		this.self = RaftPeerId.valueOf(config.nodeId());
		var attemptProperties = new RaftProperties(properties);
		// Long enough that a slow but healthy commit is not resent: a resend is safe, but re-replicates the entry.
		var attemptTimeout = config.electionTimeoutMax().multipliedBy(2);
		RaftClientConfigKeys.Rpc.setRequestTimeout(attemptProperties,
				RaftSettings.duration(attemptTimeout.compareTo(MIN_ATTEMPT_TIMEOUT) > 0 ? attemptTimeout : MIN_ATTEMPT_TIMEOUT));
		// One Ratis call per attempt; send() resends until the deadline.
		this.client = RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(group)
				.setProperties(attemptProperties)
				.setRetryPolicy(RetryPolicies.noRetry())
				.build();
	}

	RaftClient client() {
		return client;
	}

	/**
	 * Proposes {@code entry} and completes with its result. Fails with a {@link RocksDBException}: the result's own
	 * error, {@link RocksDBRetryException} if the leader refused it (stale transaction), or {@code SERVER_OVERLOADED}
	 * when no leader answered in time; then the outcome is unknown.
	 */
	CompletableFuture<Object> propose(LogEntry entry) {
		return propose(entry, true);
	}

	/**
	 * Proposes a transaction commit to this node's own Raft server, which must be the leader that opened it, in a
	 * single attempt: a resend could reach another leader, which would refuse it although the first attempt may
	 * have been committed. Fails with {@link ConflictException} on a conflict, {@link RocksDBRetryException} if it
	 * was refused before being appended, and {@code SERVER_OVERLOADED} if its outcome is unknown.
	 */
	CompletableFuture<Object> proposeOnce(LogEntry.Guarded entry) {
		return propose(entry, false);
	}

	private CompletableFuture<Object> propose(LogEntry entry, boolean anyLeader) {
		long sequence;
		long completedBelow;
		synchronized (inFlight) {
			// Allocate and register together: completedBelow must never pass a number that is taken but not done.
			sequence = nextSequence.getAndIncrement();
			inFlight.add(sequence);
			completedBelow = inFlight.first();
		}
		byte[] data;
		try {
			data = LogEntryCodec.encodeRequest(new Request(proposerId, sequence, completedBelow, entry));
		} catch (IllegalArgumentException ex) {
			inFlight.remove(sequence);
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, ex.getMessage()));
		}
		if (data.length > RaftSettings.MAX_ENTRY_BYTES) {
			inFlight.remove(sequence);
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"Write too large for one replicated entry: " + data.length + " bytes"));
		}
		var message = Message.valueOf(ByteString.copyFrom(data));
		var result = anyLeader ? send(client, message, System.nanoTime() + requestTimeoutNanos) : sendLocal(message);
		return result.whenComplete((_, _) -> {
			synchronized (inFlight) {
				inFlight.remove(sequence);
			}
		});
	}

	/** Submits straight to the local Raft server, with no client in between that could redirect it elsewhere. */
	private CompletableFuture<Object> sendLocal(Message message) {
		var request = RaftClientRequest.newBuilder()
				.setClientId(localClientId)
				.setServerId(self)
				.setGroupId(groupId)
				.setCallId(localCallIds.incrementAndGet())
				.setMessage(message)
				.setType(RaftClientRequest.writeRequestType())
				.build();
		CompletableFuture<RaftClientReply> reply;
		try {
			reply = localServer.get().submitClientRequestAsync(request);
		} catch (IOException ex) {
			return CompletableFuture.failedFuture(new RocksDBRetryException());
		}
		return reply.orTimeout(requestTimeoutNanos, TimeUnit.NANOSECONDS).handle((answer, error) -> {
			var refusal = error != null ? stateMachineException(error)
					: !answer.isSuccess() && answer.getException() instanceof StateMachineException e ? e : null;
			if (refusal != null) {
				throw new CompletionException(refusal(refusal));
			}
			if (error == null && !answer.isSuccess() && isRefusedBeforeAppend(answer.getException())) {
				throw new CompletionException(new RocksDBRetryException());
			}
			if (error != null) {
				throw new CompletionException(unavailable(error));
			}
			return toResult(answer);
		});
	}

	/** Errors Ratis returns only before appending: not ready to lead yet, or no room for another pending request. */
	private static boolean isRefusedBeforeAppend(@Nullable Throwable error) {
		return error instanceof LeaderNotReadyException || error instanceof ResourceUnavailableException;
	}

	private CompletableFuture<Object> send(RaftClient via, Message message, long deadlineNanos) {
		return CompletableFuture.supplyAsync(() -> sendBlocking(via, message), sender).handle((reply, error) -> {
			// Ratis reports state machine refusals either in the reply or as the future's failure.
			var refusal = error != null ? stateMachineException(error)
					: !reply.isSuccess() && reply.getException() instanceof StateMachineException e ? e : null;
			boolean retryable = System.nanoTime() - deadlineNanos < 0
					&& (refusal != null ? isNotReady(refusal) : error != null || !reply.isSuccess());
			if (retryable) {
				// No answer yet (no leader, unreachable, refused before append): send the same request again. If an
				// earlier copy took effect, applying this one returns its recorded result.
				long delay = refusal != null ? NOT_READY_RETRY_MS : ATTEMPT_SLEEP_MS;
				return CompletableFuture.supplyAsync(() -> send(via, message, deadlineNanos),
						CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS)).thenCompose(f -> f);
			}
			if (refusal != null) {
				throw new CompletionException(refusal(refusal));
			}
			if (error != null) {
				throw new CompletionException(unavailable(error));
			}
			return CompletableFuture.completedFuture(toResult(reply));
		}).thenCompose(f -> f);
	}

	private static RaftClientReply sendBlocking(RaftClient via, Message message) {
		try {
			return via.io().send(message);
		} catch (IOException ex) {
			throw new CompletionException(ex);
		}
	}

	private static StateMachineException stateMachineException(Throwable error) {
		for (Throwable t = error; t != null; t = t.getCause()) {
			if (t instanceof StateMachineException exception) {
				return exception;
			}
		}
		return null;
	}

	private static boolean isNotReady(StateMachineException exception) {
		return isCause(exception, RockserverStateMachine.NotReadyException.class);
	}

	private static boolean isCause(StateMachineException exception, Class<?> type) {
		// Ratis rebuilds the cause on the client from its class name.
		return exception.getCause() != null && exception.getCause().getClass().getName().equals(type.getName());
	}

	private static Object toResult(RaftClientReply reply) {
		if (!reply.isSuccess()) {
			var exception = reply.getException();
			if (exception instanceof StateMachineException stateMachineException) {
				throw new CompletionException(refusal(stateMachineException));
			}
			throw new CompletionException(unavailable(exception));
		}
		var result = ResultCodec.decode(reply.getMessage().getContent().toByteArray());
		return switch (result) {
			case ApplyResult.Ok ok -> ok.value();
			case ApplyResult.Failed failed when failed.type() == RocksDBErrorType.UPDATE_RETRY ->
					throw new CompletionException(new RocksDBRetryException());
			case ApplyResult.Failed failed -> throw new CompletionException(failed.toException());
		};
	}

	/**
	 * A state machine exception: the entry was refused before being appended (conflict, stale transaction, not
	 * ready), or anything else, such as the leader failing to apply it, which leaves its outcome unknown.
	 */
	private static RocksDBException refusal(StateMachineException exception) {
		if (isCause(exception, RockserverStateMachine.ConflictException.class)) {
			return new ConflictException();
		}
		if (isCause(exception, RockserverStateMachine.RetryableException.class)
				|| isCause(exception, RockserverStateMachine.NotReadyException.class)) {
			return new RocksDBRetryException();
		}
		return unavailable(exception);
	}

	static RocksDBException unavailable(Throwable error) {
		var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
		return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
				"Replicated write not acknowledged, its outcome is unknown: " + cause);
	}

	@Override
	public void close() throws IOException {
		try (client) {
			sender.shutdownNow();
		}
	}
}
