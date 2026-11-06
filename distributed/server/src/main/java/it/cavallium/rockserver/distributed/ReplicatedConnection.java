package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Delete;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Merge;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.UpdateContext;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
 */
final class ReplicatedConnection implements RocksDBConnection, it.cavallium.rockserver.core.common.LeaderHint {

	private static final Logger LOG = LoggerFactory.getLogger(ReplicatedConnection.class);

	/**
	 * Default lease of {@code getForUpdate} updates, as on a single node ({@code EmbeddedDB}); the native update
	 * transaction expires on its own. An abandoned update keeps its key locked this long.
	 */
	private static final Duration UPDATE_LEASE = Duration.ofSeconds(10);
	/** Pause before forwarding a request again after the leader could not be reached. */
	private static final long FORWARD_RETRY_MS = 50;
	/**
	 * How long an update waits for earlier updates of the same key before the client is told to retry: longer than
	 * the lease plus the expiry cadence, so that an update queued behind an abandoned one still gets the key.
	 */
	private static final Duration UPDATE_LOCK_WAIT = Duration.ofSeconds(12);
	/**
	 * How long the close of a chained update waits for the commit it read from. The bound lives in the future:
	 * {@code requestSync} joins a close without a timeout. {@code GrpcServer} takes the asynchronous path, so that no
	 * worker waits this long; how many closes can wait for one commit is {@link LeaderSession#MAX_LINK_READERS}.
	 */
	private static final Duration CHAINED_CLOSE_WAIT = Duration.ofSeconds(2);
	/** Tests only: replaces {@link #CHAINED_CLOSE_WAIT}. */
	volatile Duration chainedCloseWaitForTesting = CHAINED_CLOSE_WAIT;
	/** Bytes of keys and values per replicated chunk of a batch write. */
	private static final long BATCH_CHUNK_BYTES = 4L * 1024 * 1024;
	/** Encoding overhead counted per key segment and per value, so many tiny items still fit in one entry. */
	private static final long ITEM_OVERHEAD_BYTES = 16;
	private static final int ROUTING_ATTEMPTS = 3;
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

	/** A write of a client session, without its {@code completedBelow}, which differs between copies. */
	private record SessionWrite(long sessionId, long sequence) {
		static @Nullable SessionWrite of(RequestContext context) {
			var id = context.requestId();
			return id == null ? null : new SessionWrite(id.sessionId(), id.sequence());
		}
	}

	/**
	 * Commits of client session writes that this node proposed as the leader of the term they map to, and whose
	 * outcome is not settled: still in flight, or ended without an answer. Their entry may still be applied in that
	 * term, so a copy of the same write that finds no record must not be told that it was not applied. Once the term
	 * is over, every entry of it this node's log holds is applied before the next term serves (see the read barrier),
	 * so its markers say nothing more.
	 */
	private final java.util.concurrent.ConcurrentHashMap<SessionWrite, Long> unsettledCommits =
			new java.util.concurrent.ConcurrentHashMap<>();
	/** Size of {@link #unsettledCommits} at which {@link #pruneUnsettled} runs next. */
	private final AtomicLong pruneUnsettledAt = new AtomicLong(MIN_PRUNE_UNSETTLED);
	private static final long MIN_PRUNE_UNSETTLED = 1024;
	/** A follower further behind the leader's read index than this many log entries forwards a read instead. */
	private static final long FOLLOWER_READ_MAX_LAG = 1024;
	/**
	 * How long a follower that knows the read index is committed waits for its apply to reach it, before it forwards
	 * the read instead.
	 */
	private static final Duration FOLLOWER_READ_APPLY_WAIT = Duration.ofMillis(500);
	/**
	 * How long a follower that does not know yet that the read index is committed waits for it, before it forwards the
	 * read instead. Ratis tells followers the commit index with its next append: at once while writes flow, but on a
	 * quiet cluster only with the next heartbeat, half an election timeout later. Forwarding costs about two round
	 * trips, so waiting longer than that for the heartbeat is not worth it.
	 */
	private static final Duration FOLLOWER_READ_COMMIT_WAIT = Duration.ofMillis(5);
	/**
	 * {@code true} turns follower reads on for this node; off by default, a follower forwards every read to the leader.
	 * Nodes may differ. See "Reads" in the design document for what they cost on a quiet cluster.
	 */
	static final String FOLLOWER_READS_PROPERTY = "it.cavallium.rockserver.distributed.follower-reads";

	private final NodeConfig config;
	private final RockserverStateMachine stateMachine;
	private final Proposer proposer;
	private final ReadBarrier barrier;
	private final FollowerReads followerReads;
	private final LeaderForwarder forwarder;
	/** Whether this node serves point reads as a follower; see {@link #FOLLOWER_READS_PROPERTY}. */
	volatile boolean followerReadsEnabled = Boolean.getBoolean(FOLLOWER_READS_PROPERTY);
	/** Point reads this node served as a follower, and those it forwarded after trying. */
	private final AtomicLong followerReadsServed = new AtomicLong();
	private final AtomicLong followerReadsForwarded = new AtomicLong();
	private final NodeMetrics.Series followerReadTime;
	private final NodeMetrics.Series followerReadFallbacks;
	private final URI url;
	private final NodeMetrics.Series barrierTime;
	private final NodeMetrics.Series updateOpenTime;
	private final NodeMetrics.Series commitTime;
	private final NodeMetrics.Series forwardTime;
	private final NodeMetrics.Series lockWaitTime;
	private final NodeMetrics.Series chainedUpdates;
	private final NodeMetrics.Series chainConflicts;
	private final NodeMetrics.Series closeWaitTime;
	private final NodeMetrics.Series closeRetries;
	private final NodeMetrics.Series transactionConflicts;
	private final NodeMetrics.Series transactionChainConflicts;
	private final NodeMetrics.Series localTime;
	/** Runs short local commands, each on a virtual thread of its own ({@link NodeThreads#local()}); see {@link #local}. */
	private final java.util.concurrent.ExecutorService inline;

	ReplicatedConnection(NodeConfig config,
			RockserverStateMachine stateMachine,
			Proposer proposer,
			ReadBarrier barrier,
			FollowerReads followerReads,
			LeaderForwarder forwarder,
			NodeMetrics metrics,
			java.util.concurrent.ExecutorService inline) {
		this.config = config;
		this.inline = inline;
		this.followerReads = followerReads;
		this.followerReadTime = metrics.duration("read.follower");
		this.followerReadFallbacks = metrics.amount("read.follower.forwarded");
		this.barrierTime = metrics.duration("read.barrier");
		this.updateOpenTime = metrics.duration("update.open");
		this.commitTime = metrics.duration("commit");
		this.forwardTime = metrics.duration("forward");
		this.lockWaitTime = metrics.duration("update.lockWait");
		this.chainedUpdates = metrics.amount("update.chained");
		this.chainConflicts = metrics.amount("update.chainConflict");
		this.closeWaitTime = metrics.duration("update.closeWait");
		this.closeRetries = metrics.amount("update.closeRetry");
		this.transactionConflicts = metrics.amount("transaction.conflict");
		this.transactionChainConflicts = metrics.amount("transaction.chainConflict");
		this.localTime = metrics.duration("local");
		this.stateMachine = stateMachine;
		this.proposer = proposer;
		this.barrier = barrier;
		this.forwarder = forwarder;
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
		return RockserverCapabilities.CURRENT.withClientSessions(true);
	}

	/**
	 * Opens a client session: proposes a {@link LogEntry.RegisterClient}, whose log index becomes the session id on
	 * every replica. The session expires once none of its writes was applied for {@link #SESSION_TIMEOUT_PROPERTY} of
	 * leader time.
	 */
	@Override
	public CompletableFuture<Long> openClientSessionAsync(RequestContext context) {
		long start = System.nanoTime();
		return propose(context.withRequestId(null), start, new LogEntry.RegisterClient(
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
				return (RA) stream(context, stream);
			}
			return (RA) single(context, (RocksDBAPICommandSingle<?>) request);
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Routing

	private CompletableFuture<?> single(RequestContext context, RocksDBAPICommandSingle<?> command) {
		long start = System.nanoTime();
		try {
			requireUserColumn(command);
			return switch (command) {
				case Put<?> c when c.transactionOrUpdateId() == 0 -> propose(context, start,
						new LogEntry.Put(c.columnId(), c.keys(), c.value(), id(c.requestType())));
				case PutMulti<?> c when c.transactionOrUpdateId() == 0 -> propose(context, start,
						new LogEntry.PutMulti(c.columnId(), c.keys(), c.values(), id(c.requestType())));
				case Delete<?> c when c.transactionOrUpdateId() == 0 -> propose(context, start,
						new LogEntry.Delete(c.columnId(), c.keys(), id(c.requestType())));
				case DeleteMulti<?> c when c.transactionOrUpdateId() == 0 -> propose(context, start,
						new LogEntry.DeleteMulti(c.columnId(), c.keys(), id(c.requestType())));
				case Merge<?> c when c.transactionOrUpdateId() == 0 -> propose(context, start,
						new LogEntry.Merge(c.columnId(), c.keys(), c.value(), id(c.requestType())));
				case MergeMulti<?> c when c.transactionOrUpdateId() == 0 -> propose(context, start,
						new LogEntry.MergeMulti(c.columnId(), c.keys(), c.values(), id(c.requestType())));
				case Put<?> c -> leaderBound(context, c, () -> stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.Put(c.columnId(), c.keys(), c.value(), id(c.requestType())), List.of(c.keys())));
				case PutMulti<?> c -> leaderBound(context, c, () -> stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.PutMulti(c.columnId(), c.keys(), c.values(), id(c.requestType())), c.keys()));
				case Delete<?> c -> leaderBound(context, c, () -> stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.Delete(c.columnId(), c.keys(), id(c.requestType())), List.of(c.keys())));
				case DeleteMulti<?> c -> leaderBound(context, c, () -> stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.DeleteMulti(c.columnId(), c.keys(), id(c.requestType())), c.keys()));
				case Merge<?> c -> leaderBound(context, c, () -> stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.Merge(c.columnId(), c.keys(), c.value(), id(c.requestType())), List.of(c.keys())));
				case MergeMulti<?> c -> leaderBound(context, c, () -> stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.MergeMulti(c.columnId(), c.keys(), c.values(), id(c.requestType())), c.keys()));
				case RocksDBAPICommandSingle.DeleteRange c -> propose(context, start,
						new LogEntry.DeleteRange(c.columnId(), c.startKeysInclusive(), c.endKeysExclusive()));
				case RocksDBAPICommandSingle.PutBatch c -> batch(context, c.columnId(), c.batchPublisher(), false);
				case RocksDBAPICommandSingle.MergeBatch c -> batch(context, c.columnId(), c.batchPublisher(), true);
				case RocksDBAPICommandSingle.CreateColumn c -> createColumn(context, start, c.name(), c.schema());
				case RocksDBAPICommandSingle.DeleteColumn c ->
						propose(context, start, new LogEntry.DeleteColumn(c.columnId()));
				case RocksDBAPICommandSingle.DeleteColumnIfExists c ->
						propose(context, start, new LogEntry.DeleteColumnIfExists(c.name()));
				case RocksDBAPICommandSingle.UploadMergeOperator c -> propose(context, start,
						new LogEntry.UploadMergeOperator(c.name(), c.className(), c.jarData()));

				case OpenTransaction c -> leaderBound(context, c, () -> openTransaction(context, c));
				case CloseTransaction c -> leaderBound(context, c, () -> closeTransaction(context, c));
				case CloseFailedUpdate c -> leaderBound(context, c, () -> closeFailedUpdate(context, c));
				case Get<?> c when c.requestType() instanceof RequestType.RequestForUpdate<?> ->
						leaderBound(context, c, () -> getForUpdate(context, c));
				case Get<?> c when c.transactionOrUpdateId() != 0 ->
						leaderBound(context, c, () -> inTransaction(context, c, c.transactionOrUpdateId()));
				case RocksDBAPICommandSingle.ExistsMulti c when c.transactionId() != 0 ->
						leaderBound(context, c, () -> inTransaction(context, c, c.transactionId()));
				case RocksDBAPICommandSingle.OpenIterator c when c.transactionId() != 0 ->
						leaderBound(context, c, () -> inTransaction(context, c, c.transactionId()));
				case RocksDBAPICommandSingle.ReduceRange<?> c when c.transactionId() != 0 ->
						leaderBound(context, c, () -> inTransaction(context, c, c.transactionId()));
				case RocksDBAPICommandSingle.GetRangePage<?> c when c.transactionId() != 0 ->
						leaderBound(context, c, () -> inTransaction(context, c, c.transactionId()));
				case RocksDBAPICommandSingle.CloseIterator c -> leaderBound(context, c, () -> local(context, c));
				case RocksDBAPICommandSingle.SeekTo c -> leaderBound(context, c, () -> local(context, c));
				case RocksDBAPICommandSingle.Subsequent<?> c -> leaderBound(context, c, () -> local(context, c));

				case RocksDBAPICommandSingle.GetAllColumnDefinitions c ->
						leaderRead(context, c).thenApply(ReplicatedConnection::hideMetaColumn);
				case Get<?> c -> leaderRead(context, c);
				case RocksDBAPICommandSingle.GetMulti c -> leaderRead(context, c);
				case RocksDBAPICommandSingle.ExistsMulti c -> leaderRead(context, c);
				case RocksDBAPICommandSingle.OpenIterator c -> leaderRead(context, c);
				case RocksDBAPICommandSingle.ReduceRange<?> c -> leaderRead(context, c);
				case RocksDBAPICommandSingle.GetRangePage<?> c -> leaderRead(context, c);
				case RocksDBAPICommandSingle.GetColumnId c -> leaderRead(context, c);
				case RocksDBAPICommandSingle.EstimateNumKeys c -> leaderRead(context, c);
				case RocksDBAPICommandSingle.CheckMergeOperator c -> leaderRead(context, c);

				case RocksDBAPICommand.Flush c -> local(context, c);
				case RocksDBAPICommand.Compact c -> local(context, c);
				case RocksDBAPICommand.GetTableProperties c -> local(context, c);
				case RocksDBAPICommand.GetSstMetadata c -> local(context, c);
				case RocksDBAPICommand.CompactFiles c -> local(context, c);
				default -> CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
						"Not supported by replicated Rockserver: " + command.getClass().getSimpleName()));
			};
		} catch (RuntimeException ex) {
			return CompletableFuture.failedFuture(ex);
		}
	}

	private Publisher<Object> stream(RequestContext context, RocksDBAPICommandStream<?> command) {
		try {
			requireUserColumn(command);
		} catch (RocksDBException ex) {
			return Flux.error(ex);
		}
		if (command instanceof RocksDBAPICommandStream.GetRange<?> range && range.transactionId() != 0) {
			// Iterates inside a transaction: only the leader that opened it can serve it.
			return Flux.defer(() -> {
				if (!isLeader()) {
					return forwardStream(context, command);
				}
				requireLeaderTransaction(range.transactionId());
				return localStream(context, command);
			});
		}
		return Flux.defer(() -> {
			if (!isLeader()) {
				return forwardStream(context, command);
			}
			return Mono.fromFuture(this::leadership)
					.onErrorMap(ReplicatedConnection::barrierFailure)
					.thenMany(Flux.defer(() -> localStream(context, command)));
		});
	}

	/** Runs {@code onLeader} if this node leads, otherwise forwards {@code command} to the leader. */
	private CompletableFuture<?> leaderBound(RequestContext context,
			RocksDBAPICommandSingle<?> command,
			Supplier<CompletableFuture<?>> onLeader) {
		if (isLeader()) {
			try {
				return onLeader.get();
			} catch (NotLeader ex) {
				// Lost leadership just now: the new leader cannot know this node's transactions, but it can
				// serve requests that do not refer to them.
			}
		}
		return forward(context, command);
	}

	/** A linearizable read: on the leader after the read barrier, otherwise forwarded to the leader. */
	private CompletableFuture<?> leaderRead(RequestContext context, RocksDBAPICommandSingle<?> command) {
		return leaderRead(context, command, ROUTING_ATTEMPTS);
	}

	private CompletableFuture<?> leaderRead(RequestContext context, RocksDBAPICommandSingle<?> command, int attempts) {
		if (!isLeader()) {
			return followerReadsEnabled && isFollowerReadable(command)
					? followerRead(context, command)
					: forward(context, command);
		}
		long start = System.nanoTime();
		return leadership().handle((_, error) -> {
			barrierTime.since(start);
			if (error == null) {
				// The time spent confirming leadership counts toward the read's timeout.
				return local(remainingContext(context, start), command);
			}
			var cause = unwrap(error);
			if (cause instanceof ReadBarrier.NotLeaderException && attempts > 1) {
				// Leadership moved while confirming it: route again, most likely to the new leader.
				return leaderRead(context, command, attempts - 1);
			}
			return CompletableFuture.failedFuture(barrierFailure(cause));
		}).thenCompose(f -> f);
	}

	/**
	 * Point reads that a follower serves from its own database once it applied up to a read index from the leader
	 * ({@link FollowerReads}): not reads in a transaction or for an update, which live on the leader.
	 */
	private static boolean isFollowerReadable(RocksDBAPICommandSingle<?> command) {
		return switch (command) {
			case Get<?> c -> c.transactionOrUpdateId() == 0 && !(c.requestType() instanceof RequestType.RequestForUpdate<?>);
			case RocksDBAPICommandSingle.ExistsMulti c -> c.transactionId() == 0;
			case RocksDBAPICommandSingle.GetMulti _ -> true;
			default -> false;
		};
	}

	/**
	 * A point read on a follower: once this node applied up to a read index the leader confirmed after the read arrived,
	 * it has every write acknowledged before the read arrived, and serves it from its own database. Unlike the leader's
	 * {@link ReadBarrier#awaitLeadership()}, it waits for the whole index: this node did not acknowledge those writes
	 * itself. If no read index comes, or this node is too far behind, it forwards the read to the leader instead.
	 */
	private CompletableFuture<?> followerRead(RequestContext context, RocksDBAPICommandSingle<?> command) {
		if (stateMachine.isFailed()) {
			return forward(context, command);
		}
		long start = System.nanoTime();
		var applied = stateMachine.appliedIndex();
		var readIndex = followerReads.readIndex();
		if (context.hasTimeout()) {
			// This caller's copy: the request is shared with other reads.
			readIndex = readIndex.copy().orTimeout(Math.max(0, remainingNanos(context, start)), TimeUnit.NANOSECONDS);
		}
		return readIndex.thenCompose(index -> {
					if (index - applied.get() > FOLLOWER_READ_MAX_LAG) {
						return CompletableFuture.failedFuture(new ReadBarrier.ApplyLagException(index, applied.get()));
					}
					var division = stateMachine.division();
					boolean committedHere = division != null && division.getRaftLog().getLastCommittedIndex() >= index;
					long wait = Math.min((committedHere ? FOLLOWER_READ_APPLY_WAIT : FOLLOWER_READ_COMMIT_WAIT).toNanos(),
							remainingNanos(context, start));
					return applied.await(index).orTimeout(Math.max(0, wait), TimeUnit.NANOSECONDS);
				})
				// Off gRPC and apply threads: the read runs here.
				.handleAsync((_, error) -> {
					var remaining = remainingContext(context, start);
					if (error != null) {
						followerReadsForwarded.incrementAndGet();
						followerReadFallbacks.record(1);
						return forward(remaining, command);
					}
					followerReadsServed.incrementAndGet();
					followerReadTime.since(start);
					return local(remaining, command);
				}, inline)
				.thenCompose(f -> f);
	}

	/** Tests only: point reads this node served as a follower. */
	long followerReadsServedForTesting() {
		return followerReadsServed.get();
	}

	/** Tests only: point reads this node tried to serve as a follower, then forwarded. */
	long followerReadsForwardedForTesting() {
		return followerReadsForwarded.get();
	}

	@SuppressWarnings("unchecked")
	private CompletableFuture<?> forward(RequestContext context, RocksDBAPICommandSingle<?> command) {
		return forward(context, command, System.nanoTime() + config.requestTimeout().toNanos(), callerDeadline(context));
	}

	/** When the caller of a request that arrives now gives up, in {@link System#nanoTime()} units. */
	private static long callerDeadline(RequestContext context) {
		long now = System.nanoTime();
		return context.hasTimeout() ? now + Math.min(context.timeoutNanos(), Long.MAX_VALUE / 4) : now + Long.MAX_VALUE / 4;
	}

	/**
	 * How long a forwarded call may take, at most: the request timeout, which also bounds the leader's own waits of
	 * a read, plus the key wait for an update, which waits for its key on the leader longer than that.
	 */
	private long forwardTimeoutNanos(RocksDBAPICommandSingle<?> command) {
		long timeout = config.requestTimeout().toNanos();
		if (command instanceof Get<?> c && c.requestType() instanceof RequestType.RequestForUpdate<?>
				&& c.transactionOrUpdateId() == 0) {
			return timeout + UPDATE_LOCK_WAIT.toNanos();
		}
		return timeout;
	}

	/**
	 * {@code call}, failed with a {@link TimeoutException} if it does not end within {@code timeoutNanos}; the gRPC
	 * call is then cancelled.
	 */
	private static <T> CompletableFuture<T> withTimeout(CompletableFuture<T> call, long timeoutNanos) {
		call.orTimeout(timeoutNanos, TimeUnit.NANOSECONDS).whenComplete((_, error) -> {
			if (error != null && unwrap(error) instanceof TimeoutException) {
				// The core client's futures cancel their gRPC call; the leader also stops at the deadline it was given.
				call.cancel(false);
			}
		});
		return call;
	}

	/**
	 * Forwards {@code command} to the leader. When the leader cannot be reached (it died or lost leadership, and this
	 * node may not know yet), a command that can safely run again ({@link #isReplayable}) is sent again, to whichever
	 * node leads then, until {@code deadlineNanos}; any other command fails with a retryable error, or with an
	 * unknown outcome if it may have taken effect. Each attempt takes at most the caller's deadline or
	 * {@link #forwardTimeoutNanos}, whichever comes first, and the leader gets that deadline too: a leader that is alive
	 * but stuck does not hold the call, nor this node's caller, indefinitely.
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<?> forward(RequestContext context, RocksDBAPICommandSingle<?> command, long deadlineNanos,
			long callerDeadlineNanos) {
		return forwarder.leader(stateMachine).thenCompose(leader -> {
			if (leader.equals(config.nodeId())) {
				if (!isLeader()) {
					// Raft still names this node as leader, but it cannot serve (its replica failed).
					return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
							"This node cannot serve as leader"));
				}
				// We became the leader meanwhile.
				return (CompletableFuture<Object>) single(context, command);
			}
			long start = System.nanoTime();
			return forwarder.target(leader, stateMachine).thenCompose(target -> {
				long attempt = Math.min(forwardTimeoutNanos(command), callerDeadlineNanos - System.nanoTime());
				if (attempt <= 0) {
					return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
							"The request deadline passed before the request was forwarded to the leader"));
				}
				var bounded = context.withTimeoutNanos(attempt);
				var call = withTimeout(
						(CompletableFuture<Object>) target.connection().getAsyncApi(bounded).requestAsync(command), attempt);
				return call.whenComplete((_, error) -> {
					forwardTime.since(start);
					// Not after a timeout: the connection may be fine, and other calls use it.
					if (error != null && !(unwrap(error) instanceof TimeoutException) && isTransportFailure(unwrap(error))) {
						forwarder.invalidate(target);
					}
				});
			}).exceptionallyCompose(error -> {
				var cause = unwrap(error);
				boolean notSent = cause instanceof LeaderForwarder.NotConnectedException;
				boolean lost = !notSent && isTransportFailure(cause);
				if (!(notSent || lost || cause instanceof RocksDBRetryException)
						|| (!isReplayable(command, lost) && !notSent)) {
					return CompletableFuture.failedFuture(lost ? unreachableFailure(command, cause) : cause);
				}
				if (System.nanoTime() - deadlineNanos > 0) {
					// A retry signal from the leader (or a request never sent) is still the right answer.
					return CompletableFuture.failedFuture(lost ? RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
							"No leader could serve the request in time: " + cause) : cause);
				}
				return CompletableFuture.supplyAsync(() -> null,
								CompletableFuture.delayedExecutor(FORWARD_RETRY_MS, TimeUnit.MILLISECONDS))
						.thenCompose(_ -> (CompletableFuture<Object>) forward(context, command, deadlineNanos,
								callerDeadlineNanos));
			});
		});
	}

	/** The call reached the leader, or may have, and its connection then failed or the call timed out. */
	/**
	 * The forwarded call was lost: no answer, the connection failed ({@code UNAVAILABLE}), or the stream was reset
	 * ({@code CANCELLED}, as a leader whose API server stops with calls still running resets them).
	 */
	private static boolean isTransportFailure(Throwable cause) {
		if (cause instanceof TimeoutException) {
			return true;
		}
		var code = io.grpc.Status.fromThrowable(cause).getCode();
		return code == io.grpc.Status.Code.UNAVAILABLE || code == io.grpc.Status.Code.CANCELLED;
	}

	/**
	 * Commands that can run again on another leader after an attempt whose outcome is unknown: reads, and the
	 * opening of transactions and updates (one opened on a leader that is gone went away with it). An update is not
	 * opened again after an attempt that was {@code lost} in transport: the leader may be alive and have opened it,
	 * and that update would hold its key until its lease ends, also against the update sent again.
	 */
	private static boolean isReplayable(RocksDBAPICommandSingle<?> command, boolean lost) {
		return switch (command) {
			case Get<?> c when c.requestType() instanceof RequestType.RequestForUpdate<?> ->
					c.transactionOrUpdateId() == 0 && !lost;
			case Get<?> c -> c.transactionOrUpdateId() == 0;
			case OpenTransaction _ -> true;
			case RocksDBAPICommandSingle.GetMulti _, RocksDBAPICommandSingle.GetColumnId _,
					RocksDBAPICommandSingle.EstimateNumKeys _, RocksDBAPICommandSingle.CheckMergeOperator _,
					RocksDBAPICommandSingle.GetAllColumnDefinitions _ -> true;
			case RocksDBAPICommandSingle.ExistsMulti c -> c.transactionId() == 0;
			case RocksDBAPICommandSingle.ReduceRange<?> c -> c.transactionId() == 0;
			case RocksDBAPICommandSingle.GetRangePage<?> c -> c.transactionId() == 0;
			default -> false;
		};
	}

	/**
	 * The failure of a command that cannot run again after the leader became unreachable: retryable if it changes
	 * nothing by itself (it refers to a transaction, update or iterator that went away with the leader), of unknown
	 * outcome if it is a commit, which may have been applied.
	 */
	private static RuntimeException unreachableFailure(RocksDBAPICommandSingle<?> command, Throwable cause) {
		boolean commits = switch (command) {
			case CloseTransaction c -> c.commit();
			case Put<?> _, PutMulti<?> _, Delete<?> _, DeleteMulti<?> _, Merge<?> _, MergeMulti<?> _ -> true;
			default -> false;
		};
		if (commits) {
			return RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
					"The leader became unreachable, the outcome of the commit is unknown: " + cause);
		}
		return new RocksDBRetryException();
	}

	private Flux<Object> forwardStream(RequestContext context, RocksDBAPICommandStream<?> command) {
		return forwardStream(context, command, System.nanoTime() + config.requestTimeout().toNanos());
	}

	/**
	 * Forwards a stream to the leader. As for {@link #forward}, a stream that was never sent, or a non-transactional
	 * stream that failed before its first element, is sent again to whichever node leads then, until
	 * {@code deadlineNanos}. A stream inside a transaction fails with a retryable error when the leader becomes
	 * unreachable; a stream cut after some elements fails with {@code SERVER_OVERLOADED}, since the caller already
	 * received part of it.
	 */
	@SuppressWarnings("unchecked")
	private Flux<Object> forwardStream(RequestContext context, RocksDBAPICommandStream<?> command, long deadlineNanos) {
		return Mono.fromFuture(forwarder.leader(stateMachine)).flatMapMany(leader -> {
			if (leader.equals(config.nodeId())) {
				if (!isLeader()) {
					return Flux.error(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
							"This node cannot serve as leader"));
				}
				return Flux.from(stream(context, command));
			}
			var emitted = new AtomicBoolean();
			return Mono.fromFuture(forwarder.target(leader, stateMachine))
					.flatMapMany(target -> idleTimeout(Flux.from((Publisher<Object>) target.connection()
									.getAsyncApi(context)
									.requestAsync(command)), config.requestTimeout())
							.doOnNext(_ -> emitted.set(true))
							.doOnError(error -> {
								var cause = unwrap(error);
								if (!(cause instanceof TimeoutException) && isTransportFailure(cause)) {
									forwarder.invalidate(target);
								}
							}))
					.onErrorResume(error -> {
						var cause = unwrap(error);
						boolean notSent = cause instanceof LeaderForwarder.NotConnectedException;
						boolean lost = !notSent && isTransportFailure(cause);
						if (!(notSent || lost || cause instanceof RocksDBRetryException)) {
							return Flux.error(cause);
						}
						if (emitted.get()) {
							return Flux.error(lost ? RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
									"The leader became unreachable during the stream: " + cause) : cause);
						}
						if ((!notSent && isTransactional(command)) || System.nanoTime() - deadlineNanos > 0) {
							return Flux.error(lost ? new RocksDBRetryException() : cause);
						}
						return Mono.delay(Duration.ofMillis(FORWARD_RETRY_MS))
								.thenMany(Flux.defer(() -> forwardStream(context, command, deadlineNanos)));
					});
		});
	}

	/**
	 * {@code source}, failed with a {@link TimeoutException} (and cancelled) once it delivers no element for
	 * {@code idle} while its subscriber is waiting for one. A subscriber that requests nothing stops the clock: a slow
	 * consumer is not a stuck leader.
	 */
	static <T> Flux<T> idleTimeout(Flux<T> source, Duration idle) {
		return Flux.defer(() -> {
			var demand = new AtomicLong();
			var since = new AtomicLong(System.nanoTime());
			var timedOut = new AtomicBoolean();
			long idleNanos = idle.toNanos();
			var stuck = Flux.interval(idle.dividedBy(4).isZero() ? Duration.ofMillis(1) : idle.dividedBy(4))
					.filter(_ -> demand.get() > 0 && System.nanoTime() - since.get() > idleNanos)
					.take(1)
					.doOnNext(_ -> timedOut.set(true));
			return source
					.doOnNext(_ -> {
						demand.decrementAndGet();
						since.set(System.nanoTime());
					})
					.takeUntilOther(stuck)
					.concatWith(Mono.defer(() -> timedOut.get()
							? Mono.error(new TimeoutException("No element for " + idle))
							: Mono.empty()))
					.doOnRequest(n -> {
						long before = demand.getAndAccumulate(n, (a, b) -> a + b < 0 ? Long.MAX_VALUE : a + b);
						if (before <= 0) {
							since.set(System.nanoTime());
						}
					});
		});
	}

	/** Streams that iterate inside a transaction, which exists only on the leader that opened it. */
	private static boolean isTransactional(RocksDBAPICommandStream<?> command) {
		return command instanceof RocksDBAPICommandStream.GetRange<?> range && range.transactionId() != 0;
	}

	/**
	 * Runs a command on the local database. Point reads, single-key staged writes and the opening of native
	 * transactions run each on a virtual thread of its own ({@link #inline}), straight on the database: the requests
	 * were admitted already ({@code GrpcServer}'s proxy scheduler), and the embedded workload scheduler's queues cost
	 * milliseconds per request under load. (A pool of 4 platform threads per core, tried instead to keep RocksDB
	 * calls off the virtual threads' carriers, stalled for seconds once slow disk reads blocked all its threads.)
	 * The command's timeout counts from here: one that waited past its deadline fails instead of running. Longer
	 * commands (ranges, multi-key reads and staged writes, iterator calls) and maintenance keep the workload
	 * scheduler, for its admission and fairness. Rollbacks never come here: {@link #closeTransaction} and
	 * {@link #closeFailedUpdate} roll back at once, on the caller's thread.
	 */
	private CompletableFuture<?> local(RequestContext context, RocksDBAPICommandSingle<?> command) {
		long start = System.nanoTime();
		var connection = stateMachine.store().connection();
		CompletableFuture<?> result;
		if (isShort(command)) {
			result = CompletableFuture.supplyAsync(() -> connection.requestInline(context, start, command), inline);
		} else {
			result = (CompletableFuture<?>) connection.getAsyncApi(context).requestAsync(command);
		}
		result.whenComplete((_, _) -> localTime.since(start));
		return result;
	}

	private static boolean isShort(RocksDBAPICommandSingle<?> command) {
		return switch (command) {
			case Get<?> _, Put<?> _, Delete<?> _, Merge<?> _, OpenTransaction _ -> true;
			// CloseTransaction and CloseFailedUpdate never come here: rollbacks run at once on the caller's thread.
			default -> false;
		};
	}


	@SuppressWarnings("unchecked")
	private Flux<Object> localStream(RequestContext context, RocksDBAPICommandStream<?> command) {
		return Flux.from((Publisher<Object>) stateMachine.store().connection().getAsyncApi(context).requestAsync(command));
	}

	/** Rejects commands that name the internal replication column, whose contents only the log may change. */
	private void requireUserColumn(RocksDBAPICommand<?, ?, ?> command) {
		long internal = stateMachine.store().metaColumnId();
		Long column = switch (command) {
			case Put<?> c -> c.columnId();
			case PutMulti<?> c -> c.columnId();
			case Delete<?> c -> c.columnId();
			case DeleteMulti<?> c -> c.columnId();
			case Merge<?> c -> c.columnId();
			case MergeMulti<?> c -> c.columnId();
			case Get<?> c -> c.columnId();
			case RocksDBAPICommandSingle.DeleteRange c -> c.columnId();
			case RocksDBAPICommandSingle.PutBatch c -> c.columnId();
			case RocksDBAPICommandSingle.MergeBatch c -> c.columnId();
			case RocksDBAPICommandSingle.DeleteColumn c -> c.columnId();
			case RocksDBAPICommandSingle.EstimateNumKeys c -> c.columnId();
			case RocksDBAPICommandSingle.GetMulti c -> c.columnId();
			case RocksDBAPICommandSingle.ExistsMulti c -> c.columnId();
			case RocksDBAPICommandSingle.OpenIterator c -> c.columnId();
			case RocksDBAPICommandSingle.ReduceRange<?> c -> c.columnId();
			case RocksDBAPICommandSingle.GetRangePage<?> c -> c.columnId();
			case RocksDBAPICommandStream.GetRange<?> c -> c.columnId();
			case RocksDBAPICommandStream.ScanRaw c -> c.columnId();
			case RocksDBAPICommandStream.ScanRawResumable c -> c.columnId();
			case RocksDBAPICommand.GetTableProperties c -> c.columnId();
			case RocksDBAPICommand.GetSstMetadata c -> c.columnId();
			default -> null;
		};
		String name = switch (command) {
			case RocksDBAPICommandSingle.GetColumnId c -> c.name();
			case RocksDBAPICommandSingle.DeleteColumnIfExists c -> c.name();
			case RocksDBAPICommandSingle.CreateColumn c -> c.name();
			default -> null;
		};
		if ((column != null && column == internal) || NodeStore.META_COLUMN.equals(name)) {
			throw RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, "The column is internal to replication");
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Writes

	/**
	 * Proposes a replicated write of a request that arrived at {@code startNanos}: it is resent only until the
	 * caller's deadline, and is not sent at all once that passed.
	 */
	private CompletableFuture<Object> propose(RequestContext context, long startNanos, LogEntry entry) {
		return proposer.propose(entry, remainingNanos(context, startNanos), context.requestId());
	}

	private CompletableFuture<?> createColumn(RequestContext context, long startNanos, String name,
			ColumnSchema schema) {
		if (NodeStore.META_COLUMN.equals(name)) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"Reserved column name: " + name));
		}
		return propose(context, startNanos, new LogEntry.CreateColumn(name, schema)).thenCompose(id ->
				// Every replica checks that it assigned the same id; the leader's apply produced this one.
				proposer.propose(new LogEntry.VerifyColumn(name, (Long) id)).handle((_, _) -> id));
	}

	/**
	 * Splits a batch stream into bounded replicated entries, applied in order. The caller's deadline counts from
	 * the start of the call for every chunk. Each chunk is applied on its own, so once one was acknowledged, a later
	 * failure (even a retryable one, such as a chunk refused before being sent) leaves the call partly applied: it
	 * fails with {@code INVALID_REQUEST}, which clients do not resend, and says how much was applied.
	 */
	private CompletableFuture<Void> batch(RequestContext context, long columnId, Publisher<? extends KVBatch> batches,
			boolean merge) {
		long start = System.nanoTime();
		var appliedChunks = new AtomicLong();
		var appliedKeys = new AtomicLong();
		return Flux.from(batches)
				.concatMapIterable(ReplicatedConnection::chunks)
				// One chunk at a time: a later chunk must not overtake an earlier one writing the same key.
				.concatMap(chunk -> Mono.fromFuture(() -> propose(context.withRequestId(null), start, merge
								? new LogEntry.MergeMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING)
								: new LogEntry.PutMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING)))
						.doOnSuccess(_ -> {
							appliedChunks.incrementAndGet();
							appliedKeys.addAndGet(chunk.keys().size());
						}))
				.then()
				.onErrorMap(error -> appliedChunks.get() == 0 ? error : RocksDBException.of(
						RocksDBErrorType.INVALID_REQUEST,
						(merge ? "mergeBatch" : "putBatch") + " was partly applied: " + appliedChunks.get()
								+ " replicated chunks (" + appliedKeys.get() + " keys) were applied before it failed, so"
								+ " it is not safe to resend as a whole: " + unwrap(error)))
				.toFuture();
	}

	private static List<KVBatch.KVBatchRef> chunks(KVBatch batch) {
		var result = new ArrayList<KVBatch.KVBatchRef>();
		var keys = batch.keys();
		var values = batch.values();
		int start = 0;
		long bytes = 0;
		for (int i = 0; i < keys.size(); i++) {
			long size = size(keys.get(i)) + values.get(i).size() + ITEM_OVERHEAD_BYTES;
			if (i > start && bytes + size > BATCH_CHUNK_BYTES) {
				result.add(new KVBatch.KVBatchRef(keys.subList(start, i), values.subList(start, i)));
				start = i;
				bytes = 0;
			}
			bytes += size;
		}
		if (start < keys.size()) {
			result.add(new KVBatch.KVBatchRef(keys.subList(start, keys.size()), values.subList(start, values.size())));
		}
		return result;
	}

	private static long size(Keys keys) {
		long size = 0;
		for (Buf key : keys.keys()) {
			size += key.size() + ITEM_OVERHEAD_BYTES;
		}
		return size;
	}

	// ---------------------------------------------------------------------------------------------------------
	// Transactions (leader only)

	private CompletableFuture<?> openTransaction(RequestContext context, OpenTransaction command) {
		long start = System.nanoTime();
		long term = currentTerm();
		// A transaction that opens after its caller's deadline is rolled back: nobody would learn its id, and it
		// would keep its snapshot and write history for the whole lease.
		BooleanSupplier callerGone = () -> remainingNanos(context, start) <= 0;
		// The lease is capped: an open transaction keeps the write history after its snapshot on the leader. The
		// native transaction gets the same lease, so that both expire together.
		var lease = transactionLease(command.transactionLeaseTtl(), config.maxTransactionLease());
		var open = lease.equals(command.transactionLeaseTtl()) ? command : new OpenTransaction(lease);
		return leadership().handle((_, error) -> {
			if (error != null) {
				throw new CompletionException(barrierFailure(unwrap(error)));
			}
			return openNative(remainingContext(context, start), open, term, false, lease,
					result -> (Long) result, null, null, null, callerGone);
		}).thenCompose(f -> f);
	}

	/** The lease a replicated transaction gets: the requested one, at most {@code max}. */
	static Duration transactionLease(Duration requested, Duration max) {
		return requested.compareTo(max) > 0 ? max : requested;
	}

	private long currentTerm() {
		var division = stateMachine.division();
		return division != null ? division.getInfo().getCurrentTerm() : -1;
	}

	private CompletableFuture<?> getForUpdate(RequestContext context, Get<?> command) {
		if (command.transactionOrUpdateId() != 0) {
			var transaction = requireLeaderTransaction(command.transactionOrUpdateId());
			transaction.reserveBytes(LogEntryCodec.CONFLICT_KEY_BYTES);
			return stageLocally(context, command, transaction, LogEntryCodec.CONFLICT_KEY_BYTES).thenApply(result -> {
				transaction.addConflict(command.columnId(), command.keys());
				return result;
			});
		}
		// Updates of one key queue up here instead of conflicting: each one reads what the previous one committed.
		var session = currentSession();
		long start = System.nanoTime();
		// Leadership is confirmed while the update waits for its key: any confirmation after the request arrived will
		// do, and the session's term is checked again when the update opens.
		long term = currentTerm();
		var leadership = leadership().whenComplete((_, _) -> barrierTime.since(start));
		long hash = ConflictTracker.keyHash(command.columnId(), command.keys());
		var waiter = new CompletableFuture<LeaderSession.KeyLock>();
		if (session.pipelined()) {
			// The key is asked for once leadership is confirmed: its next holder may be running one round trip behind.
			leadership.whenComplete((_, error) -> {
				if (error == null) {
					session.lockKey(hash, command.columnId(), waiter);
				} else {
					waiter.completeExceptionally(barrierFailure(error));
				}
			});
		} else {
			session.lockKey(hash, command.columnId(), waiter);
		}
		var result = new CompletableFuture<Object>();
		// The caller is gone once its deadline passed or it cancelled: an update it cannot learn the id of would keep
		// the key locked for the whole lease.
		BooleanSupplier callerGone = () -> result.isCancelled() || remainingNanos(context, start) <= 0;
		waiter.orTimeout(Math.min(UPDATE_LOCK_WAIT.toNanos(), remainingNanos(context, start)), TimeUnit.NANOSECONDS)
				.handleAsync((lock, lockError) -> {
					if (lockError != null) {
						var cause = unwrap(lockError);
						boolean gaveUp = cause instanceof TimeoutException || cause instanceof CancellationException;
						throw new CompletionException(gaveUp ? new RocksDBRetryException() : cause);
					}
					lockWaitTime.since(start);
					return leadership.handle((_, error) -> {
						if (error != null) {
							session.unlockKey(lock);
							throw new CompletionException(barrierFailure(unwrap(error)));
						}
						if (callerGone.getAsBoolean()) {
							session.unlockKey(lock);
							throw new CompletionException(new RocksDBRetryException());
						}
						var base = lock.base;
						// With another key, it is a hash collision: that update reads the database.
						if (base != null && base.columnId == command.columnId() && base.keys.equals(command.keys())) {
							return openChained(command, term, session, lock);
						}
						return openNative(remainingContext(context, start), command, term, true, UPDATE_LEASE,
								r -> ((UpdateContext<?>) r).updateId(), command, session, lock, callerGone);
					}).thenCompose(f -> f);
				})
				.thenCompose(f -> f)
				.whenComplete((value, error) -> {
					updateOpenTime.since(start);
					if (error != null) {
						result.completeExceptionally(unwrap(error));
					} else if (!result.complete(value)) {
						// The caller cancelled after the update registered: nobody learns its id, so close it here
						// instead of keeping the key locked for the whole lease.
						closeAbandonedUpdate(session, ((UpdateContext<?>) value).updateId());
					}
				});
		// A queued update whose caller cancelled leaves the queue.
		result.whenComplete((_, _) -> {
			if (result.isCancelled()) {
				waiter.cancel(false);
			}
		});
		return result;
	}

	/** What is left of {@code context}'s timeout, counted from {@code startNanos}; {@link Long#MAX_VALUE} if none. */
	private static long remainingNanos(RequestContext context, long startNanos) {
		return context.hasTimeout() ? context.timeoutNanos() - (System.nanoTime() - startNanos) : Long.MAX_VALUE;
	}

	/** {@code context} with only the time left of its timeout, counted from {@code startNanos}. */
	private static RequestContext remainingContext(RequestContext context, long startNanos) {
		if (!context.hasTimeout()) {
			return context;
		}
		return context.withTimeoutNanos(Math.max(1, remainingNanos(context, startNanos)));
	}

	/**
	 * Opens a native transaction or update on the local database and registers it in the current leader session.
	 * The snapshot index is reserved first, so that no history it needs can be pruned meanwhile. The session must
	 * belong to the term the read barrier confirmed: a later term of this same node may not have applied, nor
	 * tracked, the entries another leader committed in between. If {@code callerGone} turns true before the
	 * registration, the native transaction is rolled back instead, since nobody would ever close it.
	 */
	private CompletableFuture<?> openNative(RequestContext context,
			RocksDBAPICommandSingle<?> command,
			long confirmedTerm,
			boolean update,
			Duration lease,
			java.util.function.Function<Object, Long> idOf,
			@Nullable Get<?> forUpdate,
			@Nullable LeaderSession lockSession,
			@Nullable LeaderSession.KeyLock keyLock,
			@Nullable BooleanSupplier callerGone) {
		LeaderSession session;
		try {
			session = currentSession();
			if (session.term() != confirmedTerm || (lockSession != null && lockSession != session)) {
				throw new RocksDBRetryException();
			}
		} catch (RuntimeException ex) {
			if (keyLock != null) {
				lockSession.unlockKey(keyLock);
			}
			throw ex;
		}
		var reservation = session.reserve(stateMachine.appliedIndex());
		CompletableFuture<?> opened;
		try {
			opened = local(context, command);
		} catch (RuntimeException ex) {
			session.cancel(reservation);
			if (keyLock != null) {
				session.unlockKey(keyLock);
			}
			throw ex;
		}
		return opened.handle((result, openError) -> {
			if (openError != null) {
				session.cancel(reservation);
				if (keyLock != null) {
					session.unlockKey(keyLock);
				}
				throw new CompletionException(unwrap(openError));
			}
			long id = idOf.apply(result);
			if (callerGone != null && callerGone.getAsBoolean()) {
				session.cancel(reservation);
				if (keyLock != null) {
					session.unlockKey(keyLock);
				}
				rollBackNativeQuietly(id, update);
				throw new CompletionException(new RocksDBRetryException());
			}
			LeaderSession.OpenTransaction transaction;
			try {
				transaction = session.register(reservation, id, update, keyLock, lease.toNanos());
			} catch (IllegalStateException leadershipEnded) {
				if (keyLock != null) {
					session.unlockKey(keyLock);
				}
				rollBackNativeQuietly(id, update);
				throw new CompletionException(new RocksDBRetryException());
			}
			if (forUpdate != null) {
				transaction.addConflict(forUpdate.columnId(), forUpdate.keys());
			}
			return result;
		});
	}

	/**
	 * Opens an update that reads the value its predecessor's commit writes. That commit was admitted but may not be
	 * applied, so the value is provisional until this update ends well: nothing is opened on the database, its commit
	 * carries a guard, and its close waits for the predecessor. Leadership was confirmed before the key was granted.
	 */
	private CompletableFuture<?> openChained(Get<?> command, long confirmedTerm, LeaderSession session,
			LeaderSession.KeyLock lock) {
		LeaderSession.OpenTransaction transaction;
		try {
			if (currentSession() != session || session.term() != confirmedTerm) {
				throw new RocksDBRetryException();
			}
			transaction = session.register(session.reserve(stateMachine.appliedIndex()), session.nextChainedId(), true, lock,
					lock.base, UPDATE_LEASE.toNanos());
		} catch (RuntimeException ex) {
			session.unlockKey(lock);
			throw ex instanceof IllegalStateException ? new RocksDBRetryException() : ex;
		}
		chainedUpdates.record(1);
		// Its commit conflicts on what follows the predecessor.
		transaction.addConflict(command.columnId(), command.keys());
		return CompletableFuture.completedFuture(new UpdateContext<>(lock.base.value, transaction.id));
	}

	/** A write with a transaction id: staged in an open transaction, or the final write of an update. */
	private CompletableFuture<?> stagedWrite(RequestContext context,
			RocksDBAPICommandSingle<?> command,
			long transactionId,
			LogEntry.Write write,
			List<Keys> keys) {
		var session = currentSession();
		var transaction = session.get(transactionId);
		if (transaction == null) {
			if (context.requestId() != null) {
				// A copy of the write that ended an update may have been applied already.
				return settledCommit(context.requestId());
			}
			throw new RocksDBRetryException();
		}
		if (transaction.update) {
			// Marked before anything can refuse it, and until the update left the session: a copy of this write that
			// arrives meanwhile answers "unknown", never "not applied" nor a second commit.
			var marked = markUnsettled(context, session);
			try {
				if (session.pipelined() && !transaction.startCommit()) {
					// A second write on this id: the first commit stays in flight, and the update with it.
					throw new RocksDBRetryException();
				}
				if (transaction.chained() && transaction.base.failed()) {
					// It read a value that was never written: its guard would reject it.
					session.remove(transaction);
					throw new RocksDBRetryException();
				}
			} catch (RuntimeException refused) {
				settle(marked, null);
				throw refused;
			}
			var entry = transaction.updateEntry(session.term(), write, keys, session.pipelined());
			return commit(context, session, transaction, entry).handle((result, error) -> {
				session.remove(transaction);
				rollBackNativeQuietly(transaction.id, true);
				settle(marked, error);
				// After remove: the commit can be admitted up to the moment its key is released. The link can only be this
				// commit's own, see startCommit.
				var link = transaction.link();
				if (link != null && error != null) {
					session.linkFailed(link);
				}
				if (error == null) {
					return result;
				}
				var cause = unwrap(error);
				if (cause instanceof Proposer.ConflictException && transaction.chained()) {
					chainConflicts.record(1);
				}
				// On a conflict the update is closed and must be retried, as on a single node.
				throw new CompletionException(cause instanceof Proposer.ConflictException
						? new RocksDBRetryException() : cause);
			});
		}
		if (context.requestId() != null) {
			// A client resends a numbered write after an unknown outcome: a staged write sent twice would be staged twice.
			throw RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"A write staged in a transaction cannot carry a client request id");
		}
		var staged = stagedOnly(write);
		// Refused before it is staged if the commit could not carry it, instead of failing the commit at the end.
		long bytes = LeaderSession.OpenTransaction.stagedBytes(staged, keys.size());
		transaction.reserveBytes(bytes);
		return stageLocally(context, command, transaction, bytes).thenApply(result -> {
			transaction.addWrite(staged, keys);
			return result;
		});
	}

	/** Runs a request staged in {@code transaction}, which reserved {@code bytes} for it, releasing them on failure. */
	private CompletableFuture<?> stageLocally(RequestContext context,
			RocksDBAPICommandSingle<?> command,
			LeaderSession.OpenTransaction transaction,
			long bytes) {
		CompletableFuture<?> result;
		try {
			result = local(context, command);
		} catch (RuntimeException ex) {
			transaction.releaseBytes(bytes);
			throw ex;
		}
		return result.whenComplete((_, error) -> {
			if (error != null) {
				transaction.releaseBytes(bytes);
			}
		});
	}

	private CompletableFuture<?> closeTransaction(RequestContext context, CloseTransaction command) {
		var session = currentSession();
		var transaction = session.get(command.transactionId());
		if (transaction == null && command.commit() && context.requestId() != null) {
			// A copy of a commit that may have been applied already.
			return settledCommit(context.requestId());
		}
		if (transaction == null) {
			if (command.commit() || command.transactionId() > 0) {
				// Expired, or opened by a previous leader: the client must run the transaction again. A positive id is a
				// chained update, and a clean end of one would confirm a value that may never be written.
				throw new RocksDBRetryException();
			}
			return CompletableFuture.completedFuture(true);
		}
		if (transaction.chained()) {
			throw refuseChained(session, transaction);
		}
		// Rollbacks run here, at once: an optimistic rollback does no I/O, and once the transaction left the session
		// nothing else would close it if a scheduler refused or expired the rollback.
		if (!command.commit()) {
			session.remove(transaction);
			rollBackNativeQuietly(transaction.id, false);
			return CompletableFuture.completedFuture(true);
		}
		// Marked until the transaction left the session (see stagedWrite).
		var marked = markUnsettled(context, session);
		LogEntry.Guarded entry;
		try {
			entry = transaction.commitEntry(session.term());
		} catch (RuntimeException refused) {
			settle(marked, null);
			throw refused;
		}
		if (entry.writes().isEmpty() && entry.conflictHashes().length == 0) {
			session.remove(transaction);
			rollBackNativeQuietly(transaction.id, false);
			settle(marked, null);
			return CompletableFuture.completedFuture(true);
		}
		return commit(context, session, transaction, entry).handle((_, error) -> {
			if (error != null && unwrap(error) instanceof Proposer.ConflictException) {
				// Same contract as a single node: false, and the transaction stays open.
				transactionConflicts.record(1);
				if (session.yieldChainsTo(entry)) {
					transactionChainConflicts.record(1);
				}
				settle(marked, null);
				return false;
			}
			session.remove(transaction);
			rollBackNativeQuietly(transaction.id, false);
			settle(marked, error);
			if (error != null) {
				throw new CompletionException(unwrap(error));
			}
			return true;
		});
	}

	/**
	 * Proposes a commit entry, keeping its snapshot's write history pinned until the leader validated it, whatever
	 * happens to the transaction meanwhile. A commit still queued at its caller's deadline is not sent.
	 */
	private CompletableFuture<Object> commit(RequestContext context,
			LeaderSession session,
			LeaderSession.OpenTransaction transaction,
			LogEntry.Guarded entry) {
		var pin = session.pin(transaction.snapshotIndex);
		long start = System.nanoTime();
		return proposer.proposeOnce(entry, remainingNanos(context, start), context.requestId())
				.whenComplete((_, error) -> {
					commitTime.since(start);
					session.unpin(pin);
				});
	}

	/** A commit of a client session write, marked unsettled in a term. */
	private record Marked(SessionWrite write, long term) {}

	/**
	 * Marks the commit of the client session write of {@code context} unsettled in {@code session}'s term, until
	 * {@link #settle}: null for a commit without a request id. Throws {@code OUTCOME_UNKNOWN} if a copy of it is
	 * unsettled in this term already, leaving that copy's transaction as it is.
	 */
	private @Nullable Marked markUnsettled(RequestContext context, LeaderSession session) {
		var write = SessionWrite.of(context);
		if (write == null) {
			return null;
		}
		long term = session.term();
		var previous = unsettledCommits.put(write, term);
		if (previous != null && previous == term) {
			throw earlierCopyInFlight();
		}
		if (unsettledCommits.size() >= pruneUnsettledAt.get()) {
			pruneUnsettled(term);
		}
		return new Marked(write, term);
	}

	/**
	 * Ends the mark of a commit once its transaction or update left the session: unless its outcome is unknown, in
	 * which case its entry may still be applied in this term, and the mark stays until its record exists
	 * ({@link #settledCommit}, {@link #pruneUnsettled}) or the term ends.
	 */
	private void settle(@Nullable Marked marked, @Nullable Throwable error) {
		if (marked != null && (error == null || !isOutcomeUnknown(unwrap(error)))) {
			unsettledCommits.remove(marked.write(), marked.term());
		}
	}

	/**
	 * Drops the marks of earlier terms and those whose record exists, on a virtual thread (it reads the database).
	 * Runs once the map doubled since the last run, so a commit does not scan it each time.
	 */
	private void pruneUnsettled(long term) {
		long size = unsettledCommits.size();
		long at = pruneUnsettledAt.get();
		if (size < at || !pruneUnsettledAt.compareAndSet(at, Long.MAX_VALUE)) {
			return;
		}
		try {
			inline.execute(() -> {
				try {
					unsettledCommits.entrySet().removeIf(mark -> mark.getValue() < term
							|| stateMachine.recordedResult(new it.cavallium.rockserver.core.common.ClientRequestId(
									mark.getKey().sessionId(), mark.getKey().sequence(), 0)) != null);
				} finally {
					pruneUnsettledAt.set(Math.max(MIN_PRUNE_UNSETTLED, 2L * unsettledCommits.size()));
				}
			});
		} catch (java.util.concurrent.RejectedExecutionException stopping) {
			pruneUnsettledAt.set(at);
		}
	}

	/** Test hook: the commits of session writes marked unsettled. */
	int unsettledCommitsForTesting() {
		return unsettledCommits.size();
	}

	private static boolean isOutcomeUnknown(Throwable error) {
		return error instanceof RocksDBException rocks && rocks.getErrorUniqueId() == RocksDBErrorType.OUTCOME_UNKNOWN;
	}

	private static RocksDBException earlierCopyInFlight() {
		return RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
				"An earlier copy of this commit is in flight or unsettled; its outcome is unknown yet");
	}

	/**
	 * The answer to a copy of a commit (or of the write that ends an update) of a client session whose transaction or
	 * update is not on this leader: the transaction ended, or another leader had it. After the read barrier every
	 * entry of earlier leaders is applied, so the record of an earlier copy that took effect is there, and is the
	 * answer. Without a record, and with no copy unsettled in this term, the commit was never applied: the client
	 * runs the transaction again ({@code UPDATE_RETRY}), as without sessions. A session that is gone has no records
	 * any more: {@code SESSION_EXPIRED}.
	 */
	/**
	 * {@link ReadBarrier#awaitLeadership}, completed on a virtual thread of {@link #inline}: the barrier completes its
	 * futures on the node's few control threads, which must never wait for the database, and its continuations here
	 * read the database, open transactions or start streams.
	 */
	private CompletableFuture<Void> leadership() {
		return barrier.awaitLeadership().whenCompleteAsync((_, _) -> {}, inline);
	}

	private CompletableFuture<?> settledCommit(it.cavallium.rockserver.core.common.ClientRequestId id) {
		long term = currentTerm();
		var write = new SessionWrite(id.sessionId(), id.sequence());
		return leadership().handle((_, error) -> {
			if (error != null) {
				throw new CompletionException(barrierFailure(error));
			}
			// The mark first: a commit removes it only after its record is in the database, so no mark and then no record
			// proves that no copy applied in this term.
			Long unsettledTerm = unsettledCommits.get(write);
			var recorded = stateMachine.recordedResult(id);
			if (recorded != null && unsettledTerm != null) {
				unsettledCommits.remove(write, unsettledTerm);
			}
			if (recorded != null) {
				return switch (recorded) {
					case ApplyResult.Ok ok -> ok.value();
					case ApplyResult.Failed failed when failed.type() == RocksDBErrorType.UPDATE_RETRY ->
							throw new CompletionException(new RocksDBRetryException());
					case ApplyResult.Failed failed -> throw new CompletionException(failed.toException());
					case ApplyResult.ResendAlone _ -> throw new CompletionException(
							RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "Invalid record"));
				};
			}
			if (unsettledTerm != null && unsettledTerm == term) {
				throw new CompletionException(earlierCopyInFlight());
			}
			var store = stateMachine.store();
			if (store.db().get(0L, store.metaColumnId(), NodeStore.sessionKey(id.sessionId()),
					RequestType.<it.cavallium.buffer.Buf>current()) == null) {
				throw new CompletionException(((ApplyResult.Failed) Applier.SESSION_GONE).toException());
			}
			throw new CompletionException(new RocksDBRetryException());
		});
	}

	private CompletableFuture<?> closeFailedUpdate(RequestContext context, CloseFailedUpdate command) {
		long start = System.nanoTime();
		var session = currentSession();
		long id = command.updateId();
		var transaction = session.get(id);
		if (id > 0) {
			return closeChained(context, start, session, transaction);
		}
		if (transaction != null) {
			session.remove(transaction);
		}
		// At once, like the rollbacks of closeTransaction.
		rollBackNativeQuietly(id, true);
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * The close of a chained update, which wrote nothing, so no log entry is needed: it hands the key on at once, then
	 * tells whether what the update read was real. Its read counts only if the commit it read from applied: the read
	 * then linearizes right after that commit (see {@link LeaderSession#admitted}, and the leadership confirmed before
	 * the grant). Otherwise, or if that is not known in time, or the update is unknown here (expired, or opened by a
	 * previous leader), the client must run the update again. The answer goes on a pool: {@code settled} completes on
	 * the thread that completes every applied entry.
	 */
	private CompletableFuture<?> closeChained(RequestContext context, long start, LeaderSession session,
			LeaderSession.@Nullable OpenTransaction transaction) {
		if (transaction == null) {
			return CompletableFuture.failedFuture(new RocksDBRetryException());
		}
		session.remove(transaction);
		long wait = Math.min(chainedCloseWaitForTesting.toNanos(), remainingNanos(context, start));
		// A copy: the timeout would otherwise complete the future every waiter shares.
		return transaction.base.settled.copy().orTimeout(wait, TimeUnit.NANOSECONDS).handleAsync((ok, error) -> {
			closeWaitTime.since(start);
			if (error == null && ok) {
				return null;
			}
			closeRetries.record(1);
			throw new CompletionException(new RocksDBRetryException());
		});
	}

	private CompletableFuture<?> inTransaction(RequestContext context, RocksDBAPICommandSingle<?> command, long id) {
		requireLeaderTransaction(id);
		return local(context, command);
	}

	private LeaderSession.OpenTransaction requireLeaderTransaction(long id) {
		var session = currentSession();
		var transaction = session.get(id);
		if (transaction == null) {
			// Unknown here: expired, or opened by a previous leader. Either way the client must start over.
			throw new RocksDBRetryException();
		}
		if (transaction.chained()) {
			throw refuseChained(session, transaction);
		}
		return transaction;
	}

	/**
	 * A native operation on a chained update: there is no snapshot to serve it from, and reading the database would
	 * hide a value the update already read. The update ends, and the next one of the key is a head.
	 */
	private static RocksDBRetryException refuseChained(LeaderSession session, LeaderSession.OpenTransaction transaction) {
		session.breakChain(transaction.keyLock.key);
		session.remove(transaction);
		return new RocksDBRetryException();
	}

	private LeaderSession currentSession() {
		var division = stateMachine.division();
		if (division == null || !division.getInfo().isLeader() || !division.getInfo().isLeaderReady()) {
			throw new NotLeader();
		}
		var session = stateMachine.leaderSession(division.getInfo().getCurrentTerm());
		if (session == null) {
			throw new NotLeader();
		}
		session.expireIfDue();
		return session;
	}

	/** Closes an update that registered in {@code session} but whose caller is gone: releases its key and rolls it back. */
	private void closeAbandonedUpdate(LeaderSession session, long updateId) {
		var transaction = session.get(updateId);
		if (transaction != null) {
			session.remove(transaction);
		}
		rollBackNativeQuietly(updateId, true);
	}

	private void rollBackNativeQuietly(long id, boolean update) {
		if (id > 0) {
			// A chained update has no native transaction.
			return;
		}
		try {
			var db = stateMachine.store().db();
			if (update) {
				db.closeFailedUpdate(id);
			} else {
				db.closeTransaction(id, false);
			}
		} catch (RocksDBException ex) {
			if (ex.getErrorUniqueId() != RocksDBErrorType.TX_NOT_FOUND
					&& ex.getErrorUniqueId() != RocksDBErrorType.TRANSACTION_NOT_FOUND) {
				LOG.warn("Failed to roll back native {} {}", update ? "update" : "transaction", id, ex);
			}
			// Otherwise already expired or closed.
		} catch (RuntimeException ex) {
			// Such as a database that is closing. Nothing else would close it: say so.
			LOG.warn("Failed to roll back native {} {}", update ? "update" : "transaction", id, ex);
		}
	}

	/** The write as recorded for the commit entry: staged writes return nothing at commit time. */
	private static LogEntry.Write stagedOnly(LogEntry.Write write) {
		return switch (write) {
			case LogEntry.Put put -> new LogEntry.Put(put.columnId(), put.keys(), put.value(), RequestTypeId.NOTHING);
			case LogEntry.PutMulti put ->
					new LogEntry.PutMulti(put.columnId(), put.keys(), put.values(), RequestTypeId.NOTHING);
			case LogEntry.Delete delete -> new LogEntry.Delete(delete.columnId(), delete.keys(), RequestTypeId.NOTHING);
			case LogEntry.DeleteMulti delete ->
					new LogEntry.DeleteMulti(delete.columnId(), delete.keys(), RequestTypeId.NOTHING);
			case LogEntry.Merge merge -> new LogEntry.Merge(merge.columnId(), merge.keys(), merge.value(), RequestTypeId.NOTHING);
			case LogEntry.MergeMulti merge ->
					new LogEntry.MergeMulti(merge.columnId(), merge.keys(), merge.values(), RequestTypeId.NOTHING);
			case LogEntry.DeleteRange _ -> throw new IllegalArgumentException("deleteRange cannot be transactional");
		};
	}

	// ---------------------------------------------------------------------------------------------------------
	// Helpers

	/** This node is the leader and may serve leader-only requests. */
	private boolean isLeader() {
		var division = stateMachine.division();
		return division != null && division.getInfo().isLeader() && division.getInfo().isLeaderReady()
				&& !stateMachine.isFailed();
	}

	/** Thrown when leadership was lost while routing a leader-bound request. */
	private static final class NotLeader extends RocksDBRetryException {}

	private static RequestTypeId id(RequestType<?, ?> requestType) {
		return requestType.getRequestTypeId();
	}

	@SuppressWarnings("unchecked")
	private static Object hideMetaColumn(Object definitions) {
		var map = new LinkedHashMap<>((Map<String, ?>) definitions);
		map.remove(NodeStore.META_COLUMN);
		return map;
	}

	private static RuntimeException barrierFailure(Throwable error) {
		var cause = unwrap(error);
		if (cause instanceof ReadBarrier.NotLeaderException) {
			return new RocksDBRetryException();
		}
		if (cause instanceof TimeoutException) {
			return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "Could not confirm leadership in time");
		}
		if (cause instanceof ReadBarrier.ApplyLagException lag) {
			return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, lag.getMessage());
		}
		if (cause instanceof RuntimeException runtime) {
			return runtime;
		}
		return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, String.valueOf(cause));
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
