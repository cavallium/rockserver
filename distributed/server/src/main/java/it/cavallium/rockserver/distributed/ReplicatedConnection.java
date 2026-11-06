package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Delete;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Merge;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import java.net.URI;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * The Rockserver API of a replicated node. Reads on the leader wait only for a leadership confirmation
 * ({@link ReadBarrier#awaitLeadership}), not for the apply of what is committed: every write is acknowledged only
 * after the leader applied it (the result comes from the leader's apply), so whatever a client saw acknowledged is
 * already in the leader's database. Each command takes one of these paths:
 * <ul>
 *   <li><b>replicated write</b> (no transaction): proposed to the log from this node; the result is computed by
 *   the leader's apply;</li>
 *   <li><b>linearizable read</b>: on the leader, after the {@link ReadBarrier}, against its local database;
 *   forwarded to the leader from other nodes;</li>
 *   <li><b>leader-bound</b>: transactions, updates and iterators live on the leader of one term
 *   ({@link LeaderSession}); forwarded to the leader from other nodes;</li>
 *   <li><b>local</b>: maintenance (flush, compaction, SST metadata) of this node's own database.</li>
 * </ul>
 * The paths are chosen by {@link Router}; forwarding is {@link Forwarder}, transactions and updates
 * {@link UpdateProtocol} (with {@link CommitOutcomes}), replicated writes {@link ReplicatedWrites}, and commands on
 * the local database {@link LocalCommands}.
 */
final class ReplicatedConnection implements RocksDBConnection, it.cavallium.rockserver.core.common.LeaderHint {

	/**
	 * System property: how long a client session lives without a write applied, in milliseconds. Read by the node that
	 * opens a session, and written in its log entry, so nodes may differ.
	 */
	static final String SESSION_TIMEOUT_PROPERTY = "rockserver.client-session-timeout-ms";
	static final long SESSION_TIMEOUT_MILLIS = Long.getLong(SESSION_TIMEOUT_PROPERTY, TimeUnit.HOURS.toMillis(1));
	/**
	 * Shortest client session: a session that expires while a copy of one of its writes is still being resent (a
	 * request timeout, plus the client's own retries) could have that copy refused as expired after an earlier copy
	 * took effect. Well above both, and above the clock skew a new leader may bring.
	 */
	static final long MIN_SESSION_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10);

	/** The session timeout this node writes in a registration: the property, at least the minimum and 30 timeouts. */
	static long sessionTimeoutMillis(long configuredMillis, java.time.Duration requestTimeout) {
		return Math.max(configuredMillis, Math.max(MIN_SESSION_TIMEOUT_MILLIS, 30 * requestTimeout.toMillis()));
	}

	private final NodeConfig config;
	private final RockserverStateMachine stateMachine;
	private final LeaderForwarder forwarder;
	private final ReplicatedWrites writes;
	final CommitOutcomes outcomes;
	final UpdateProtocol updates;
	final Router router;
	private final URI url;

	ReplicatedConnection(NodeConfig config,
			RockserverStateMachine stateMachine,
			Proposer proposer,
			ReadBarrier barrier,
			FollowerReads followerReads,
			LeaderForwarder forwarder,
			NodeMetrics metrics,
			java.util.concurrent.ExecutorService inline) {
		this.config = config;
		this.stateMachine = stateMachine;
		this.forwarder = forwarder;
		var localCommands = new LocalCommands(stateMachine, barrier, metrics, inline);
		this.writes = new ReplicatedWrites(proposer);
		this.outcomes = new CommitOutcomes(stateMachine, localCommands, inline);
		this.updates = new UpdateProtocol(config, stateMachine, proposer, localCommands, outcomes, metrics);
		this.router = new Router(config, stateMachine, followerReads, forwarder, localCommands, writes, updates, metrics,
				inline);
		var self = config.self();
		this.url = URI.create("rockserver-replicated://" + self.apiHost() + ":" + self.apiPort() + "/" + self.id());
	}

	@Override
	public URI getUrl() {
		return url;
	}

	/** The leader and its API address, as last looked up. */
	private record CachedHint(org.apache.ratis.protocol.RaftPeerId leader, @Nullable String address) {}

	private volatile @Nullable CachedHint cachedHint;

	/**
	 * The leader's API address, which {@code GrpcServer} sends to clients with every response, so that a client of
	 * several nodes can send what the leader serves straight to it. This node's own address while it leads.
	 */
	@Override
	public @Nullable String leaderApiAddress() {
		var division = stateMachine.division();
		if (division == null) {
			return null;
		}
		var leaderId = division.getInfo().getLeaderId();
		if (leaderId == null) {
			return null;
		}
		var cached = cachedHint;
		if (cached != null && cached.leader().equals(leaderId) && cached.address() != null) {
			return cached.address();
		}
		var api = forwarder.apiAddress(leaderId.toString());
		var address = api == null ? null : api.host() + ":" + api.port();
		cachedHint = new CachedHint(leaderId, address);
		return address;
	}

	@Override
	public RockserverCapabilities getCapabilities() {
		return RockserverCapabilities.CURRENT.withClientSessions(true).withNumberedBatches(true);
	}

	/**
	 * Opens a client session: proposes a {@link LogEntry.RegisterClient}, whose log index becomes the session id on
	 * every replica. The session expires once none of its writes was applied for {@link #SESSION_TIMEOUT_PROPERTY} of
	 * leader time.
	 */
	@Override
	public CompletableFuture<Long> openClientSessionAsync(RequestContext context) {
		long start = System.nanoTime();
		return writes.propose(context.withRequestId(null), start, new LogEntry.RegisterClient(
				sessionTimeoutMillis(SESSION_TIMEOUT_MILLIS, config.requestTimeout())))
				.thenApply(id -> (Long) id);
	}

	@Override
	public it.cavallium.rockserver.core.common.RocksDBSyncAPI getSyncApi(RequestContext context) {
		return new Api(context);
	}

	@Override
	public it.cavallium.rockserver.core.common.RocksDBAsyncAPI getAsyncApi(RequestContext context) {
		return new Api(context);
	}

	@Override
	public void close() {
		// Resources belong to the ReplicatedNode.
	}

	private final class Api implements RocksDBAPI {

		private final RequestContext context;

		Api(RequestContext context) {
			this.context = context;
		}

		/**
		 * A replicated write waits at most until its caller's deadline: the proposer may keep resending it longer (at
		 * most one attempt past that deadline), and the caller's thread (in {@code GrpcServer}, a proxy permit) would
		 * be held meanwhile.
		 */
		@Override
		@SuppressWarnings("unchecked")
		public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
			long start = System.nanoTime();
			RA result = requestAsync(request);
			if (request instanceof RocksDBAPICommandStream<?>) {
				return (RS) Flux.from((Publisher<R>) result).toStream();
			}
			if (context.hasTimeout() && isSingleReplicatedWrite(request)) {
				return (RS) join((CompletableFuture<R>) result, remainingNanos(context, start));
			}
			return (RS) join((CompletableFuture<R>) result);
		}

		@Override
		@SuppressWarnings("unchecked")
		public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
			if (request instanceof RocksDBAPICommandStream<?> stream) {
				return (RA) router.stream(context, stream);
			}
			return (RA) router.single(context, (RocksDBAPICommandSingle<?>) request);
		}
	}

	/** What is left of {@code context}'s timeout, counted from {@code startNanos}; {@link Long#MAX_VALUE} if none. */
	static long remainingNanos(RequestContext context, long startNanos) {
		return context.hasTimeout() ? context.timeoutNanos() - (System.nanoTime() - startNanos) : Long.MAX_VALUE;
	}

	/** {@code context} with only the time left of its timeout, counted from {@code startNanos}. */
	static RequestContext remainingContext(RequestContext context, long startNanos) {
		if (!context.hasTimeout()) {
			return context;
		}
		return context.withTimeoutNanos(Math.max(1, remainingNanos(context, startNanos)));
	}

	static Throwable unwrap(Throwable error) {
		while ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null) {
			error = error.getCause();
		}
		return error;
	}

	/**
	 * Commands proposed from this node to the log as one request, as opposed to served or forwarded to the leader.
	 * Not batches: their chunks stop at the caller's deadline on their own, and their failure tells which chunks were
	 * applied.
	 */
	private static boolean isSingleReplicatedWrite(RocksDBAPICommand<?, ?, ?> command) {
		return switch (command) {
			case Put<?> c -> c.transactionOrUpdateId() == 0;
			case PutMulti<?> c -> c.transactionOrUpdateId() == 0;
			case Delete<?> c -> c.transactionOrUpdateId() == 0;
			case DeleteMulti<?> c -> c.transactionOrUpdateId() == 0;
			case Merge<?> c -> c.transactionOrUpdateId() == 0;
			case MergeMulti<?> c -> c.transactionOrUpdateId() == 0;
			case RocksDBAPICommandSingle.DeleteRange _, RocksDBAPICommandSingle.CreateColumn _,
					RocksDBAPICommandSingle.DeleteColumn _, RocksDBAPICommandSingle.DeleteColumnIfExists _,
					RocksDBAPICommandSingle.UploadMergeOperator _ -> true;
			default -> false;
		};
	}

	/**
	 * {@link #join(CompletableFuture)}, but waits at most {@code timeoutNanos}. Then the caller's copy is cancelled
	 * (a write not sent yet is then never sent) and the call fails with {@code OUTCOME_UNKNOWN}: a write
	 * still queued before that deadline already failed as not applied, so this one was sent and its outcome is
	 * unknown.
	 */
	static <T> T join(CompletableFuture<T> future, long timeoutNanos) {
		try {
			future.get(Math.max(0, timeoutNanos), TimeUnit.NANOSECONDS);
		} catch (TimeoutException ex) {
			future.cancel(false);
			throw RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
					"The request deadline passed before the replicated write was acknowledged; its outcome is unknown");
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			future.cancel(false);
			throw RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
					"Interrupted while waiting for a replicated write; its outcome is unknown");
		} catch (ExecutionException | CancellationException ex) {
			// Rethrown as join() does.
		}
		return join(future);
	}

	static <T> T join(CompletableFuture<T> future) {
		try {
			return future.join();
		} catch (CompletionException ex) {
			var cause = unwrap(ex);
			if (cause instanceof RuntimeException runtime) {
				throw runtime;
			}
			if (cause instanceof Error error) {
				throw error;
			}
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, cause);
		}
	}
}
