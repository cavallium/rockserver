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
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.UpdateContext;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
final class ReplicatedConnection implements RocksDBConnection {

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
	/** Bytes of keys and values per replicated chunk of a batch write. */
	private static final long BATCH_CHUNK_BYTES = 4L * 1024 * 1024;
	/** Encoding overhead counted per key segment and per value, so many tiny items still fit in one entry. */
	private static final long ITEM_OVERHEAD_BYTES = 16;
	private static final int ROUTING_ATTEMPTS = 3;

	private final NodeConfig config;
	private final RockserverStateMachine stateMachine;
	private final Proposer proposer;
	private final ReadBarrier barrier;
	private final LeaderForwarder forwarder;
	private final URI url;
	private final NodeMetrics.Series barrierTime;
	private final NodeMetrics.Series updateOpenTime;
	private final NodeMetrics.Series commitTime;
	private final NodeMetrics.Series forwardTime;
	private final NodeMetrics.Series lockWaitTime;

	ReplicatedConnection(NodeConfig config,
			RockserverStateMachine stateMachine,
			Proposer proposer,
			ReadBarrier barrier,
			LeaderForwarder forwarder,
			NodeMetrics metrics) {
		this.config = config;
		this.barrierTime = metrics.duration("read.barrier");
		this.updateOpenTime = metrics.duration("update.open");
		this.commitTime = metrics.duration("commit");
		this.forwardTime = metrics.duration("forward");
		this.lockWaitTime = metrics.duration("update.lockWait");
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

	@Override
	public RockserverCapabilities getCapabilities() {
		return RockserverCapabilities.CURRENT;
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

		@Override
		@SuppressWarnings("unchecked")
		public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
			RA result = requestAsync(request);
			if (request instanceof RocksDBAPICommandStream<?>) {
				return (RS) Flux.from((Publisher<R>) result).toStream();
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
		try {
			requireUserColumn(command);
			return switch (command) {
				case Put<?> c when c.transactionOrUpdateId() == 0 ->
						proposer.propose(new LogEntry.Put(c.columnId(), c.keys(), c.value(), id(c.requestType())));
				case PutMulti<?> c when c.transactionOrUpdateId() == 0 -> proposer.propose(
						new LogEntry.PutMulti(c.columnId(), c.keys(), c.values(), id(c.requestType())));
				case Delete<?> c when c.transactionOrUpdateId() == 0 ->
						proposer.propose(new LogEntry.Delete(c.columnId(), c.keys(), id(c.requestType())));
				case DeleteMulti<?> c when c.transactionOrUpdateId() == 0 ->
						proposer.propose(new LogEntry.DeleteMulti(c.columnId(), c.keys(), id(c.requestType())));
				case Merge<?> c when c.transactionOrUpdateId() == 0 ->
						proposer.propose(new LogEntry.Merge(c.columnId(), c.keys(), c.value(), id(c.requestType())));
				case MergeMulti<?> c when c.transactionOrUpdateId() == 0 -> proposer.propose(
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
				case RocksDBAPICommandSingle.DeleteRange c ->
						proposer.propose(new LogEntry.DeleteRange(c.columnId(), c.startKeysInclusive(), c.endKeysExclusive()));
				case RocksDBAPICommandSingle.PutBatch c -> batch(c.columnId(), c.batchPublisher(), false);
				case RocksDBAPICommandSingle.MergeBatch c -> batch(c.columnId(), c.batchPublisher(), true);
				case RocksDBAPICommandSingle.CreateColumn c -> createColumn(c.name(), c.schema());
				case RocksDBAPICommandSingle.DeleteColumn c -> proposer.propose(new LogEntry.DeleteColumn(c.columnId()));
				case RocksDBAPICommandSingle.DeleteColumnIfExists c ->
						proposer.propose(new LogEntry.DeleteColumnIfExists(c.name()));
				case RocksDBAPICommandSingle.UploadMergeOperator c ->
						proposer.propose(new LogEntry.UploadMergeOperator(c.name(), c.className(), c.jarData()));

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
			return Mono.fromFuture(barrier::awaitLeadership)
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
			return forward(context, command);
		}
		long start = System.nanoTime();
		return barrier.awaitLeadership().handle((_, error) -> {
			barrierTime.since(start);
			if (error == null) {
				return local(context, command);
			}
			var cause = unwrap(error);
			if (cause instanceof ReadBarrier.NotLeaderException && attempts > 1) {
				// Leadership moved while confirming it: route again, most likely to the new leader.
				return leaderRead(context, command, attempts - 1);
			}
			return CompletableFuture.failedFuture(barrierFailure(cause));
		}).thenCompose(f -> f);
	}

	@SuppressWarnings("unchecked")
	private CompletableFuture<?> forward(RequestContext context, RocksDBAPICommandSingle<?> command) {
		return forward(context, command, System.nanoTime() + config.requestTimeout().toNanos());
	}

	/**
	 * Forwards {@code command} to the leader. When the leader cannot be reached (it died or lost leadership, and this
	 * node may not know yet), a command that can safely run again ({@link #isReplayable}) is sent again, to whichever
	 * node leads then, until {@code deadlineNanos}; any other command fails with a retryable error, or with an
	 * unknown outcome if it may have taken effect.
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<?> forward(RequestContext context, RocksDBAPICommandSingle<?> command, long deadlineNanos) {
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
				var call = (CompletableFuture<Object>) target.connection().getAsyncApi(context).requestAsync(command);
				if (isCleanup(command)) {
					// Rollbacks carry no deadline of their own: bound them, so a frozen leader cannot hold them.
					call = call.orTimeout(config.requestTimeout().toNanos(), TimeUnit.NANOSECONDS);
				}
				return call.whenComplete((_, error) -> {
					forwardTime.since(start);
					if (error != null && isTransportFailure(unwrap(error))) {
						forwarder.invalidate(target);
					}
				});
			}).exceptionallyCompose(error -> {
				var cause = unwrap(error);
				boolean notSent = cause instanceof LeaderForwarder.NotConnectedException;
				boolean lost = !notSent && isTransportFailure(cause);
				if (!(notSent || lost || cause instanceof RocksDBRetryException) || (!isReplayable(command) && !notSent)) {
					return CompletableFuture.failedFuture(lost ? unreachableFailure(command, cause) : cause);
				}
				if (System.nanoTime() - deadlineNanos > 0) {
					// A retry signal from the leader (or a request never sent) is still the right answer.
					return CompletableFuture.failedFuture(lost ? RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
							"No leader could serve the request in time: " + cause) : cause);
				}
				return CompletableFuture.supplyAsync(() -> null,
								CompletableFuture.delayedExecutor(FORWARD_RETRY_MS, TimeUnit.MILLISECONDS))
						.thenCompose(_ -> (CompletableFuture<Object>) forward(context, command, deadlineNanos));
			});
		});
	}

	/** The call reached the leader, or may have, and its connection then failed or the call timed out. */
	private static boolean isTransportFailure(Throwable cause) {
		return cause instanceof TimeoutException
				|| io.grpc.Status.fromThrowable(cause).getCode() == io.grpc.Status.Code.UNAVAILABLE;
	}

	/** Rollbacks, sent without a deadline. */
	private static boolean isCleanup(RocksDBAPICommandSingle<?> command) {
		return command instanceof CloseFailedUpdate || (command instanceof CloseTransaction c && !c.commit());
	}

	/**
	 * Commands that can run again on another leader after an attempt whose outcome is unknown: reads, and the
	 * opening of transactions and updates (one opened on a leader that is gone went away with it).
	 */
	private static boolean isReplayable(RocksDBAPICommandSingle<?> command) {
		return switch (command) {
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
			return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
					"The leader became unreachable, the outcome of the commit is unknown: " + cause);
		}
		return new RocksDBRetryException();
	}

	@SuppressWarnings("unchecked")
	private Flux<Object> forwardStream(RequestContext context, RocksDBAPICommandStream<?> command) {
		return Mono.fromFuture(forwarder.leader(stateMachine)).flatMapMany(leader -> {
			if (leader.equals(config.nodeId())) {
				if (!isLeader()) {
					return Flux.error(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
							"This node cannot serve as leader"));
				}
				return Flux.from(stream(context, command));
			}
			return Mono.fromFuture(forwarder.target(leader, stateMachine))
					.flatMapMany(target -> Flux.from((Publisher<Object>) target.connection().getAsyncApi(context)
							.requestAsync(command)));
		});
	}

	private CompletableFuture<?> local(RequestContext context, RocksDBAPICommandSingle<?> command) {
		return (CompletableFuture<?>) stateMachine.store().connection().getAsyncApi(context).requestAsync(command);
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

	private CompletableFuture<?> createColumn(String name, ColumnSchema schema) {
		if (NodeStore.META_COLUMN.equals(name)) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"Reserved column name: " + name));
		}
		return proposer.propose(new LogEntry.CreateColumn(name, schema)).thenCompose(id ->
				// Every replica checks that it assigned the same id; the leader's apply produced this one.
				proposer.propose(new LogEntry.VerifyColumn(name, (Long) id)).handle((_, _) -> id));
	}

	/** Splits a batch stream into bounded replicated entries, applied in order. */
	private CompletableFuture<Void> batch(long columnId, Publisher<? extends KVBatch> batches, boolean merge) {
		return Flux.from(batches)
				.concatMapIterable(ReplicatedConnection::chunks)
				// One chunk at a time: a later chunk must not overtake an earlier one writing the same key.
				.concatMap(chunk -> Mono.fromFuture(() -> proposer.propose(merge
						? new LogEntry.MergeMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING)
						: new LogEntry.PutMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING))))
				.then()
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
		long term = currentTerm();
		return barrier.awaitLeadership().handle((_, error) -> {
			if (error != null) {
				throw new CompletionException(barrierFailure(unwrap(error)));
			}
			return openNative(context, command, term, false, command.transactionLeaseTtl(), result -> (Long) result,
					null, null, null);
		}).thenCompose(f -> f);
	}

	private long currentTerm() {
		var division = stateMachine.division();
		return division != null ? division.getInfo().getCurrentTerm() : -1;
	}

	private CompletableFuture<?> getForUpdate(RequestContext context, Get<?> command) {
		if (command.transactionOrUpdateId() != 0) {
			var transaction = requireLeaderTransaction(command.transactionOrUpdateId());
			return local(context, command).thenApply(result -> {
				transaction.addConflict(command.columnId(), command.keys());
				return result;
			});
		}
		// Updates of one key queue up here instead of conflicting: each one reads what the previous one committed.
		var session = currentSession();
		long start = System.nanoTime();
		return session.lockKey(ConflictTracker.keyHash(command.columnId(), command.keys()))
				.orTimeout(UPDATE_LOCK_WAIT.toNanos(), TimeUnit.NANOSECONDS)
				.exceptionallyCompose(error -> CompletableFuture.failedFuture(
						unwrap(error) instanceof TimeoutException ? new RocksDBRetryException() : unwrap(error)))
				.thenComposeAsync(lock -> {
					lockWaitTime.since(start);
					long term = currentTerm();
					long barrierStart = System.nanoTime();
					return barrier.awaitLeadership().handle((_, error) -> {
						barrierTime.since(barrierStart);
						if (error != null) {
							session.unlockKey(lock);
							throw new CompletionException(barrierFailure(unwrap(error)));
						}
						return openNative(context, command, term, true, UPDATE_LEASE,
								result -> ((UpdateContext<?>) result).updateId(), command, session, lock);
					}).thenCompose(f -> f);
				}).whenComplete((_, _) -> updateOpenTime.since(start));
	}

	/**
	 * Opens a native transaction or update on the local database and registers it in the current leader session.
	 * The snapshot index is reserved first, so that no history it needs can be pruned meanwhile. The session must
	 * belong to the term the read barrier confirmed: a later term of this same node may not have applied, nor
	 * tracked, the entries another leader committed in between.
	 */
	private CompletableFuture<?> openNative(RequestContext context,
			RocksDBAPICommandSingle<?> command,
			long confirmedTerm,
			boolean update,
			Duration lease,
			java.util.function.Function<Object, Long> idOf,
			@Nullable Get<?> forUpdate,
			@Nullable LeaderSession lockSession,
			@Nullable LeaderSession.KeyLock keyLock) {
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

	/** A write with a transaction id: staged in an open transaction, or the final write of an update. */
	private CompletableFuture<?> stagedWrite(RequestContext context,
			RocksDBAPICommandSingle<?> command,
			long transactionId,
			LogEntry.Write write,
			List<Keys> keys) {
		var session = currentSession();
		var transaction = session.get(transactionId);
		if (transaction == null) {
			throw new RocksDBRetryException();
		}
		if (transaction.update) {
			var entry = transaction.updateEntry(session.term(), write, keys);
			return commit(session, transaction, entry).handle((result, error) -> {
				session.remove(transaction);
				rollBackNativeQuietly(transaction.id, true);
				if (error == null) {
					return result;
				}
				var cause = unwrap(error);
				// On a conflict the update is closed and must be retried, as on a single node.
				throw new CompletionException(cause instanceof Proposer.ConflictException
						? new RocksDBRetryException() : cause);
			});
		}
		var staged = stagedOnly(write);
		return local(context, command).thenApply(result -> {
			transaction.addWrite(staged, keys);
			return result;
		});
	}

	private CompletableFuture<?> closeTransaction(RequestContext context, CloseTransaction command) {
		var session = currentSession();
		var transaction = session.get(command.transactionId());
		if (transaction == null) {
			if (command.commit()) {
				// Expired, or opened by a previous leader: the client must run the transaction again.
				throw new RocksDBRetryException();
			}
			return CompletableFuture.completedFuture(true);
		}
		if (!command.commit()) {
			session.remove(transaction);
			return local(context, command);
		}
		var entry = transaction.commitEntry(session.term());
		if (entry.writes().isEmpty() && entry.conflictHashes().length == 0) {
			session.remove(transaction);
			return local(context, new CloseTransaction(command.transactionId(), false));
		}
		return commit(session, transaction, entry).handle((_, error) -> {
			if (error != null && unwrap(error) instanceof Proposer.ConflictException) {
				// Same contract as a single node: false, and the transaction stays open.
				return false;
			}
			session.remove(transaction);
			rollBackNativeQuietly(transaction.id, false);
			if (error != null) {
				throw new CompletionException(unwrap(error));
			}
			return true;
		});
	}

	/**
	 * Proposes a commit entry, keeping its snapshot's write history pinned until the leader validated it, whatever
	 * happens to the transaction meanwhile.
	 */
	private CompletableFuture<Object> commit(LeaderSession session,
			LeaderSession.OpenTransaction transaction,
			LogEntry.Guarded entry) {
		var pin = session.pin(transaction.snapshotIndex);
		long start = System.nanoTime();
		return proposer.proposeOnce(entry).whenComplete((_, _) -> {
			commitTime.since(start);
			session.unpin(pin);
		});
	}

	private CompletableFuture<?> closeFailedUpdate(RequestContext context, CloseFailedUpdate command) {
		var session = currentSession();
		var transaction = session.get(command.updateId());
		if (transaction != null) {
			session.remove(transaction);
		}
		return local(context, command);
	}

	private CompletableFuture<?> inTransaction(RequestContext context, RocksDBAPICommandSingle<?> command, long id) {
		requireLeaderTransaction(id);
		return local(context, command);
	}

	private LeaderSession.OpenTransaction requireLeaderTransaction(long id) {
		var transaction = currentSession().get(id);
		if (transaction == null) {
			// Unknown here: expired, or opened by a previous leader. Either way the client must start over.
			throw new RocksDBRetryException();
		}
		return transaction;
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

	private void rollBackNativeQuietly(long id, boolean update) {
		try {
			var db = stateMachine.store().db();
			if (update) {
				db.closeFailedUpdate(id);
			} else {
				db.closeTransaction(id, false);
			}
		} catch (RuntimeException ignored) {
			// Already expired or closed.
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
