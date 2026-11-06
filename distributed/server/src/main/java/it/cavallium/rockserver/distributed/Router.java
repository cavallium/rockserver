package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.RawScanEvent;
import it.cavallium.rockserver.core.common.RawSstToken;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
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
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import it.cavallium.rockserver.distributed.LocalCommands.NotLeader;

import static it.cavallium.rockserver.distributed.ReplicatedConnection.remainingContext;
import static it.cavallium.rockserver.distributed.ReplicatedConnection.remainingNanos;
import static it.cavallium.rockserver.distributed.ReplicatedConnection.unwrap;
import static it.cavallium.rockserver.distributed.LocalCommands.barrierFailure;

/**
 * Chooses the path of each command of a replicated node (see {@link ReplicatedConnection}): a replicated write, a
 * linearizable read (on the leader after the read barrier, on a follower after a read index, or forwarded), a
 * leader-bound command ({@link UpdateProtocol}, or forwarded by {@link Forwarder}), or a local one.
 */
final class Router {

	private static final int ROUTING_ATTEMPTS = 3;
	/** A follower further behind the leader's read index than this many log entries forwards a read instead. */
	private static final long FOLLOWER_READ_MAX_LAG = 1024;
	/**
	 * How long a follower waits, once it has the read index, to learn that it is committed and to apply up to it, before
	 * it forwards the read instead. The leader sends the commit index to its followers when it answers a read index
	 * request ({@link CommitHeartbeats}), and while writes flow with every append, so a follower that keeps up waits for
	 * its own log flush and apply of the latest entries. This bounds how much slower than a forwarded read a follower
	 * read can be: this wait, plus the read index round trip. See "Follower reads" in the design document.
	 */
	static final Duration FOLLOWER_READ_WAIT = Duration.ofMillis(20);
	/**
	 * {@code true} turns follower reads on for this node; off by default, a follower forwards every read to the leader.
	 * On a leader it also turns {@link ReadBarrier#followerReads} on from the start; a leader turns it on anyway when it
	 * answers its first read index. Nodes may differ, as during a rolling restart that turns the option on: a follower
	 * serves a read only while its database holds no entry of a term later than that of the leader that gave it the read
	 * index (see {@link #followerRead}), so never an entry that a leader with the shortcut may not have applied yet. See
	 * "Follower reads" in the design document.
	 */
	static final String FOLLOWER_READS_PROPERTY = DistributedProperties.PREFIX + "follower-reads";

	private final RockserverStateMachine stateMachine;
	private final String nodeId;
	private final FollowerReads followerReads;
	private final LocalCommands localCommands;
	private final ReplicatedWrites writes;
	private final UpdateProtocol updates;
	private final Forwarder forwarding;
	/** Whether this node serves point reads as a follower; see {@link #FOLLOWER_READS_PROPERTY}. */
	volatile boolean followerReadsEnabled = followerReadsConfigured();
	/** {@link #FOLLOWER_READ_WAIT}; tests change it. */
	volatile long followerReadWaitNanos = FOLLOWER_READ_WAIT.toNanos();
	/** Point reads this node served as a follower, and those it forwarded after trying. */
	private final AtomicLong followerReadsServed = new AtomicLong();
	private final AtomicLong followerReadsForwarded = new AtomicLong();
	private final NodeMetrics.Series followerReadTime;
	private final NodeMetrics.Series followerReadFallbacks;
	private final NodeMetrics.Series barrierTime;
	/** Runs short local commands, each on a virtual thread of its own ({@link NodeThreads#local()}). */
	private final java.util.concurrent.ExecutorService inline;

	Router(NodeConfig config,
			RockserverStateMachine stateMachine,
			FollowerReads followerReads,
			LeaderForwarder forwarder,
			LocalCommands localCommands,
			ReplicatedWrites writes,
			UpdateProtocol updates,
			NodeMetrics metrics,
			java.util.concurrent.ExecutorService inline) {
		this.stateMachine = stateMachine;
		this.nodeId = config.nodeId();
		this.followerReads = followerReads;
		this.localCommands = localCommands;
		this.writes = writes;
		this.updates = updates;
		this.inline = inline;
		this.followerReadTime = metrics.duration("read.follower");
		this.followerReadFallbacks = metrics.amount("read.follower.forwarded");
		this.barrierTime = metrics.duration("read.barrier");
		this.forwarding = new Forwarder(config, stateMachine, forwarder, localCommands, this, metrics);
	}

	/** Whether {@link #FOLLOWER_READS_PROPERTY} turns follower reads on. */
	static boolean followerReadsConfigured() {
		return DistributedProperties.getBoolean(false, FOLLOWER_READS_PROPERTY);
	}

	private CompletableFuture<?> local(RequestContext context, RocksDBAPICommandSingle<?> command) {
		return localCommands.local(context, command);
	}

	private Flux<Object> localStream(RequestContext context, RocksDBAPICommandStream<?> command) {
		return localCommands.localStream(context, command);
	}

	private CompletableFuture<Void> leadership() {
		return localCommands.leadership();
	}

	private boolean isLeader() {
		return localCommands.isLeader();
	}

	CompletableFuture<?> single(RequestContext context, RocksDBAPICommandSingle<?> command) {
		long start = System.nanoTime();
		try {
			requireUserColumn(command);
			return switch (command) {
				case Put<?> c when c.transactionOrUpdateId() == 0 -> writes.propose(context, start,
						new LogEntry.Put(c.columnId(), c.keys(), c.value(), id(c.requestType())));
				case PutMulti<?> c when c.transactionOrUpdateId() == 0 -> writes.propose(context, start,
						new LogEntry.PutMulti(c.columnId(), c.keys(), c.values(), id(c.requestType())));
				case Delete<?> c when c.transactionOrUpdateId() == 0 -> writes.propose(context, start,
						new LogEntry.Delete(c.columnId(), c.keys(), id(c.requestType())));
				case DeleteMulti<?> c when c.transactionOrUpdateId() == 0 -> writes.propose(context, start,
						new LogEntry.DeleteMulti(c.columnId(), c.keys(), id(c.requestType())));
				case Merge<?> c when c.transactionOrUpdateId() == 0 -> writes.propose(context, start,
						new LogEntry.Merge(c.columnId(), c.keys(), c.value(), id(c.requestType())));
				case MergeMulti<?> c when c.transactionOrUpdateId() == 0 -> writes.propose(context, start,
						new LogEntry.MergeMulti(c.columnId(), c.keys(), c.values(), id(c.requestType())));
				case Put<?> c -> leaderBound(context, c, () -> updates.stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.Put(c.columnId(), c.keys(), c.value(), id(c.requestType())), List.of(c.keys())));
				case PutMulti<?> c -> leaderBound(context, c, () -> updates.stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.PutMulti(c.columnId(), c.keys(), c.values(), id(c.requestType())), c.keys()));
				case Delete<?> c -> leaderBound(context, c, () -> updates.stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.Delete(c.columnId(), c.keys(), id(c.requestType())), List.of(c.keys())));
				case DeleteMulti<?> c -> leaderBound(context, c, () -> updates.stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.DeleteMulti(c.columnId(), c.keys(), id(c.requestType())), c.keys()));
				case Merge<?> c -> leaderBound(context, c, () -> updates.stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.Merge(c.columnId(), c.keys(), c.value(), id(c.requestType())), List.of(c.keys())));
				case MergeMulti<?> c -> leaderBound(context, c, () -> updates.stagedWrite(context, c, c.transactionOrUpdateId(),
						new LogEntry.MergeMulti(c.columnId(), c.keys(), c.values(), id(c.requestType())), c.keys()));
				case RocksDBAPICommandSingle.DeleteRange c -> writes.propose(context, start,
						new LogEntry.DeleteRange(c.columnId(), c.startKeysInclusive(), c.endKeysExclusive()));
				case RocksDBAPICommandSingle.PutBatch c -> writes.batch(context, c.columnId(), c.batchPublisher(), false);
				case RocksDBAPICommandSingle.MergeBatch c -> writes.batch(context, c.columnId(), c.batchPublisher(), true);
				case RocksDBAPICommandSingle.CreateColumn c -> writes.createColumn(context, start, c.name(), c.schema());
				case RocksDBAPICommandSingle.DeleteColumn c ->
						writes.propose(context, start, new LogEntry.DeleteColumn(c.columnId()));
				case RocksDBAPICommandSingle.DeleteColumnIfExists c ->
						writes.propose(context, start, new LogEntry.DeleteColumnIfExists(c.name()));
				case RocksDBAPICommandSingle.UploadMergeOperator c -> writes.propose(context, start,
						new LogEntry.UploadMergeOperator(c.name(), c.className(), c.jarData()));

				case OpenTransaction c -> leaderBound(context, c, () -> updates.openTransaction(context, c));
				case CloseTransaction c -> leaderBound(context, c, () -> updates.closeTransaction(context, c));
				case CloseFailedUpdate c -> leaderBound(context, c, () -> updates.closeFailedUpdate(context, c));
				case Get<?> c when c.requestType() instanceof RequestType.RequestForUpdate<?> ->
						leaderBound(context, c, () -> updates.getForUpdate(context, c));
				case Get<?> c when c.transactionOrUpdateId() != 0 ->
						leaderBound(context, c, () -> updates.inTransaction(context, c, c.transactionOrUpdateId()));
				case RocksDBAPICommandSingle.ExistsMulti c when c.transactionId() != 0 ->
						leaderBound(context, c, () -> updates.inTransaction(context, c, c.transactionId()));
				case RocksDBAPICommandSingle.OpenIterator c when c.transactionId() != 0 ->
						leaderBound(context, c, () -> updates.inTransaction(context, c, c.transactionId()));
				case RocksDBAPICommandSingle.ReduceRange<?> c when c.transactionId() != 0 ->
						leaderBound(context, c, () -> updates.inTransaction(context, c, c.transactionId()));
				case RocksDBAPICommandSingle.GetRangePage<?> c when c.transactionId() != 0 ->
						leaderBound(context, c, () -> updates.inTransaction(context, c, c.transactionId()));
				case RocksDBAPICommandSingle.CloseIterator c -> leaderBound(context, c, () -> local(context, c));
				case RocksDBAPICommandSingle.SeekTo c ->
						leaderBound(context, c, () -> iteratorCall(local(context, c), c.iterationId()));
				case RocksDBAPICommandSingle.Subsequent<?> c ->
						leaderBound(context, c, () -> iteratorCall(local(context, c), c.iterationId()));

				case RocksDBAPICommandSingle.GetAllColumnDefinitions c ->
						leaderRead(context, c).thenApply(Router::hideMetaColumn);
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
				// The SST maintenance calls describe and change the files of the node that serves scanRaw, the leader, so
				// that their names and session refer to the same database (see "SST maintenance" in the design document).
				case RocksDBAPICommand.GetTableProperties c -> leaderRead(context, c);
				case RocksDBAPICommand.GetSstMetadata c -> leaderRead(context, c);
				case RocksDBAPICommand.CompactFiles c -> leaderBound(context, c, () -> local(context, c));
				default -> CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
						"Not supported by replicated Rockserver: " + command.getClass().getSimpleName()));
			};
		} catch (RuntimeException ex) {
			return CompletableFuture.failedFuture(ex);
		}
	}

	Publisher<Object> stream(RequestContext context, RocksDBAPICommandStream<?> command) {
		try {
			requireUserColumn(command);
		} catch (RocksDBException ex) {
			return Flux.error(ex);
		}
		if (command instanceof RocksDBAPICommandStream.GetRange<?> range && range.transactionId() != 0) {
			// Iterates inside a transaction: only the leader that opened it can serve it.
			return Flux.defer(() -> {
				if (!isLeader()) {
					return forwarding.forwardStream(context, command);
				}
				updates.requireLeaderTransaction(range.transactionId());
				return localStream(context, command);
			});
		}
		return Flux.defer(() -> {
			if (!isLeader()) {
				return forwarding.forwardStream(context, command);
			}
			return Mono.fromFuture(this::leadership)
					.onErrorMap(LocalCommands::barrierFailure)
					.thenMany(Flux.defer(() -> command instanceof RocksDBAPICommandStream.ScanRawResumable scan
							? scanRawResumable(context, scan)
							: localStream(context, command)));
		});
	}

	/**
	 * A resumable raw scan on the leader. Its SST tokens name files of this node's database, whose file numbers mean
	 * nothing on another node, nor here after a snapshot install replaced the database. They leave tagged with this node
	 * and its current term ({@link #sstTokenOrigin}); tokens that come back with another tag, or none, are dropped, so
	 * their files are scanned again instead of a different file of the same name being skipped. A node installs
	 * snapshots only as a follower, so its database stays the same within one term of its leadership.
	 */
	private Flux<Object> scanRawResumable(RequestContext context, RocksDBAPICommandStream.ScanRawResumable command) {
		String origin = sstTokenOrigin(localCommands.currentTerm());
		var local = new java.util.HashSet<RawSstToken>();
		for (var token : command.completedSsts()) {
			if (origin.equals(token.origin())) {
				local.add(token.untagged());
			}
		}
		var untagged = new RocksDBAPICommandStream.ScanRawResumable(command.columnId(), command.shardIndex(),
				command.shardCount(), local);
		return localStream(context, untagged).map(event -> switch (event) {
			case RawScanEvent.Batch batch when batch.completedSstToken() != null ->
					new RawScanEvent.Batch(batch.serialized(), RawSstToken.tagged(origin, batch.completedSstToken()));
			case RawScanEvent.SstCompleted completed ->
					new RawScanEvent.SstCompleted(RawSstToken.tagged(origin, completed.token()));
			default -> event;
		});
	}

	/** The origin tag of the SST tokens this node issues in {@code term}: its node id (escaped) and the term. */
	String sstTokenOrigin(long term) {
		return sstTokenOrigin(nodeId, term);
	}

	static String sstTokenOrigin(String nodeId, long term) {
		var origin = new StringBuilder();
		for (byte b : nodeId.getBytes(java.nio.charset.StandardCharsets.UTF_8)) {
			char c = (char) (b & 0xFF);
			if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_'
					|| c == '-') {
				origin.append(c);
			} else {
				origin.append('~').append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
			}
		}
		if (origin.length() > RawSstToken.MAX_ORIGIN_CHARACTERS - 24) {
			origin.setLength(0);
			origin.append("h").append(Integer.toHexString(nodeId.hashCode()));
		}
		return origin.append("~t").append(term).toString();
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
		return forwarding.forward(context, command);
	}

	/**
	 * An iterator call on the leader: an iterator lives only on the node that opened it, so one this leader does not
	 * know was opened on an earlier leader (or expired, or was closed). The database reports it as an internal error;
	 * the caller gets {@code UPDATE_RETRY} instead, as for a transaction lost with its leader, and opens a new one.
	 */
	static CompletableFuture<?> iteratorCall(CompletableFuture<?> call, long iteratorId) {
		return call.exceptionallyCompose(error -> CompletableFuture.failedFuture(
				isUnknownIterator(unwrap(error), iteratorId) ? new it.cavallium.rockserver.core.common.RocksDBRetryException()
						: unwrap(error)));
	}

	/**
	 * The database's failure for an iterator id it does not hold ({@code EmbeddedDB.iteratorNotFound}). Read from its
	 * type and text, which {@code RouterIteratorTest} pins against the database.
	 */
	static boolean isUnknownIterator(Throwable error, long iteratorId) {
		return error instanceof RocksDBException ex && ex.getErrorUniqueId() == RocksDBErrorType.INTERNAL_ERROR
				&& String.valueOf(ex.getMessage()).contains("No iterator with id " + iteratorId);
	}

	/** A linearizable read: on the leader after the read barrier, otherwise forwarded to the leader. */
	private CompletableFuture<?> leaderRead(RequestContext context, RocksDBAPICommandSingle<?> command) {
		return leaderRead(context, command, ROUTING_ATTEMPTS);
	}

	private CompletableFuture<?> leaderRead(RequestContext context, RocksDBAPICommandSingle<?> command, int attempts) {
		if (!isLeader()) {
			return followerReadsEnabled && isFollowerReadable(command)
					? followerRead(context, command)
					: forwarding.forward(context, command);
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
	 *
	 * <p>The read is discarded and forwarded too if, once it ran, this node's database may hold an entry of a term later
	 * than the read index's ({@link RockserverStateMachine#visibleTerm()}): the leader of that term may not have
	 * answered a read index yet, so may still answer its own reads before applying its latest entries
	 * ({@link ReadBarrier#followerReads}), and a client could then read an older value there after reading the newer
	 * one here. Checked after the read, because the apply goes on meanwhile; the term is raised before an entry's writes
	 * become visible. Such reads are only those that straddle a change of leader.</p>
	 */
	private CompletableFuture<?> followerRead(RequestContext context, RocksDBAPICommandSingle<?> command) {
		if (stateMachine.isFailed()) {
			return forwarding.forward(context, command);
		}
		long start = System.nanoTime();
		var applied = stateMachine.appliedIndex();
		var readIndex = followerReads.readIndex();
		if (context.hasTimeout()) {
			// This caller's copy: the request is shared with other reads.
			readIndex = readIndex.copy().orTimeout(Math.max(0, remainingNanos(context, start)), TimeUnit.NANOSECONDS);
		}
		return readIndex.thenCompose(index -> {
					if (index.index() - applied.get() > FOLLOWER_READ_MAX_LAG) {
						return CompletableFuture.failedFuture(new ReadBarrier.ApplyLagException(index.index(), applied.get()));
					}
					long wait = Math.min(followerReadWaitNanos, remainingNanos(context, start));
					return applied.await(index.index()).orTimeout(Math.max(0, wait), TimeUnit.NANOSECONDS)
							.thenApply(_ -> index);
				})
				// Off gRPC and apply threads: the read runs here.
				.handleAsync((index, error) -> {
					if (error != null) {
						return forwardFollowerRead(context, start, command);
					}
					// A snapshot install closes the database, restores another and opens it: a read that fails meanwhile is
					// forwarded instead. Its answer could not be stale (the new database is further ahead), only lost.
					long generation = stateMachine.storeGeneration();
					CompletableFuture<Object> local;
					if ((generation & 1) != 0) {
						local = CompletableFuture.failedFuture(new IllegalStateException("The database is being replaced"));
					} else {
						try {
							@SuppressWarnings("unchecked")
							var started = (CompletableFuture<Object>) local(remainingContext(context, start), command);
							local = started;
						} catch (Throwable e) {
							local = CompletableFuture.failedFuture(e);
						}
					}
					return local.handle((value, localError) -> {
						if (localError == null) {
							if (stateMachine.visibleTerm() > index.term()) {
								return forwardFollowerRead(context, start, command);
							}
							followerReadsServed.incrementAndGet();
							followerReadTime.since(start);
							return CompletableFuture.<Object>completedFuture(value);
						}
						long now = stateMachine.storeGeneration();
						return (generation & 1) != 0 || now != generation
								? forwardFollowerRead(context, start, command)
								: CompletableFuture.<Object>failedFuture(localError);
					}).thenCompose(f -> f);
				}, inline)
				.thenCompose(f -> f);
	}

	/** Forwards a point read this node tried to serve as a follower. */
	private CompletableFuture<Object> forwardFollowerRead(RequestContext context, long start,
			RocksDBAPICommandSingle<?> command) {
		followerReadsForwarded.incrementAndGet();
		followerReadFallbacks.record(1);
		return forwarding.forward(remainingContext(context, start), command).thenApply(r -> (Object) r);
	}

	/** Tests only: point reads this node served as a follower. */
	long followerReadsServedForTesting() {
		return followerReadsServed.get();
	}

	/** Tests only: point reads this node tried to serve as a follower, then forwarded. */
	long followerReadsForwardedForTesting() {
		return followerReadsForwarded.get();
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
			case RocksDBAPICommand.CompactFiles c -> c.request().columnId();
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

	static RequestTypeId id(RequestType<?, ?> requestType) {
		return requestType.getRequestTypeId();
	}

	@SuppressWarnings("unchecked")
	private static Object hideMetaColumn(Object definitions) {
		var map = new LinkedHashMap<>((Map<String, ?>) definitions);
		map.remove(NodeStore.META_COLUMN);
		return map;
	}
}
