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
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The Rockserver API of a replicated node. Each command takes one of these paths:
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

	/** Default lease of {@code getForUpdate} updates; the native update transaction expires on its own. */
	private static final Duration UPDATE_LEASE = Duration.ofMinutes(2);
	/** Bytes of keys and values per replicated chunk of a batch write. */
	private static final long BATCH_CHUNK_BYTES = 4L * 1024 * 1024;
	private static final int BATCH_CONCURRENCY = 4;
	private static final int ROUTING_ATTEMPTS = 3;

	private final NodeConfig config;
	private final RockserverStateMachine stateMachine;
	private final Proposer proposer;
	private final ReadBarrier barrier;
	private final LeaderForwarder forwarder;
	private final URI url;

	ReplicatedConnection(NodeConfig config,
			RockserverStateMachine stateMachine,
			Proposer proposer,
			ReadBarrier barrier,
			LeaderForwarder forwarder) {
		this.config = config;
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

		/** Removed together with CDC from core; kept only until that removal lands on this branch. */
		@Override
		public reactor.core.publisher.Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch> cdcPollBatchAsync(
				String id, Long fromSeq, long maxEvents) {
			return reactor.core.publisher.Mono.error(RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
					"CDC is not available"));
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
			return Mono.fromFuture(barrier::await)
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
		return barrier.await().handle((_, error) -> {
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
		return forwarder.leader(stateMachine).thenCompose(leader -> {
			if (leader.equals(config.nodeId())) {
				// We became the leader meanwhile.
				return (CompletableFuture<Object>) single(context, command);
			}
			return forwarder.api(leader, stateMachine, context)
					.thenCompose(api -> (CompletableFuture<Object>) api.requestAsync(command));
		});
	}

	@SuppressWarnings("unchecked")
	private Flux<Object> forwardStream(RequestContext context, RocksDBAPICommandStream<?> command) {
		return Mono.fromFuture(forwarder.leader(stateMachine)).flatMapMany(leader -> {
			if (leader.equals(config.nodeId())) {
				return Flux.from(stream(context, command));
			}
			return Mono.fromFuture(forwarder.api(leader, stateMachine, context))
					.flatMapMany(api -> Flux.from((Publisher<Object>) api.requestAsync(command)));
		});
	}

	private CompletableFuture<?> local(RequestContext context, RocksDBAPICommandSingle<?> command) {
		return (CompletableFuture<?>) stateMachine.store().connection().getAsyncApi(context).requestAsync(command);
	}

	@SuppressWarnings("unchecked")
	private Flux<Object> localStream(RequestContext context, RocksDBAPICommandStream<?> command) {
		return Flux.from((Publisher<Object>) stateMachine.store().connection().getAsyncApi(context).requestAsync(command));
	}

	// ---------------------------------------------------------------------------------------------------------
	// Writes

	private CompletableFuture<?> createColumn(String name, ColumnSchema schema) {
		if (NodeStore.META_COLUMN.equals(name)) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"Reserved column name: " + name));
		}
		return proposer.propose(new LogEntry.CreateColumn(name, schema));
	}

	/** Splits a batch stream into bounded replicated entries, applied in order. */
	private CompletableFuture<Void> batch(long columnId, Publisher<? extends KVBatch> batches, boolean merge) {
		return Flux.from(batches)
				.concatMapIterable(ReplicatedConnection::chunks)
				.flatMapSequential(chunk -> Mono.fromFuture(() -> proposer.propose(merge
						? new LogEntry.MergeMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING)
						: new LogEntry.PutMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING))),
						BATCH_CONCURRENCY)
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
			long size = size(keys.get(i)) + values.get(i).size();
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
			size += key.size();
		}
		return size;
	}

	// ---------------------------------------------------------------------------------------------------------
	// Transactions (leader only)

	private CompletableFuture<?> openTransaction(RequestContext context, OpenTransaction command) {
		return barrier.await().handle((_, error) -> {
			if (error != null) {
				throw new CompletionException(barrierFailure(unwrap(error)));
			}
			var session = currentSession();
			var reservation = session.reserve(stateMachine.appliedIndex());
			return local(context, command).handle((id, openError) -> {
				if (openError != null) {
					session.cancel(reservation);
					throw new CompletionException(unwrap(openError));
				}
				session.register(reservation, (Long) id, false, command.transactionLeaseTtl().toNanos());
				return id;
			});
		}).thenCompose(f -> f);
	}

	private CompletableFuture<?> getForUpdate(RequestContext context, Get<?> command) {
		if (command.transactionOrUpdateId() != 0) {
			var transaction = requireLeaderTransaction(command.transactionOrUpdateId());
			return local(context, command).thenApply(result -> {
				transaction.addConflict(command.columnId(), command.keys());
				return result;
			});
		}
		return barrier.await().handle((_, error) -> {
			if (error != null) {
				throw new CompletionException(barrierFailure(unwrap(error)));
			}
			var session = currentSession();
			var reservation = session.reserve(stateMachine.appliedIndex());
			return local(context, command).handle((result, getError) -> {
				if (getError != null) {
					session.cancel(reservation);
					throw new CompletionException(unwrap(getError));
				}
				var update = (UpdateContext<?>) result;
				var transaction = session.register(reservation, update.updateId(), true, UPDATE_LEASE.toNanos());
				transaction.addConflict(command.columnId(), command.keys());
				return result;
			});
		}).thenCompose(f -> f);
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
			return proposer.propose(entry).whenComplete((_, error) -> {
				session.remove(transaction);
				closeNativeQuietly(transaction);
				if (error != null && unwrap(error) instanceof Proposer.ConflictException) {
					// Same contract as a single node: the update is closed and must be retried.
					throw new CompletionException(new RocksDBRetryException());
				}
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
				throw new RocksDBRetryException();
			}
			return CompletableFuture.completedFuture(true);
		}
		if (!command.commit()) {
			session.remove(transaction);
			return local(context, command);
		}
		if (!transaction.hasWrites() && transaction.commitEntry(session.term()).conflictHashes().length == 0) {
			session.remove(transaction);
			return local(context, new CloseTransaction(command.transactionId(), false));
		}
		return proposer.propose(transaction.commitEntry(session.term())).handle((_, error) -> {
			if (error != null && unwrap(error) instanceof Proposer.ConflictException) {
				// Same contract as a single node: false, and the transaction stays open.
				return false;
			}
			session.remove(transaction);
			closeNativeQuietly(transaction);
			if (error != null) {
				throw new CompletionException(unwrap(error));
			}
			return true;
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
		if (division == null || !division.getInfo().isLeader()) {
			throw new NotLeader();
		}
		var session = stateMachine.leaderSession(division.getInfo().getCurrentTerm());
		if (session == null) {
			throw new NotLeader();
		}
		session.expire();
		return session;
	}

	private void closeNativeQuietly(LeaderSession.OpenTransaction transaction) {
		try {
			var db = stateMachine.store().db();
			if (transaction.update) {
				db.closeFailedUpdate(transaction.id);
			} else {
				db.closeTransaction(transaction.id, false);
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
