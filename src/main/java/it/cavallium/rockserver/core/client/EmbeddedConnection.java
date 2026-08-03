package it.cavallium.rockserver.core.client;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/** Public embedded connection. Database operations are exposed only by context-bound API views. */
public final class EmbeddedConnection extends BaseConnection implements InternalConnection {

	public static final URI PRIVATE_MEMORY_URL = EmbeddedConnectionDelegate.PRIVATE_MEMORY_URL;

	private final EmbeddedConnectionDelegate delegate;

	public EmbeddedConnection(@Nullable Path path, String name, @Nullable Path embeddedConfig) throws IOException {
		super(name);
		this.delegate = new EmbeddedConnectionDelegate(path, name, embeddedConfig);
	}

	@VisibleForTesting
	public void closeTesting() throws IOException {
		delegate.closeTesting();
	}

	@Override
	public void close() throws IOException {
		delegate.close();
		super.close();
	}

	@Override
	public URI getUrl() {
		return delegate.getUrl();
	}

	@Override
	public EmbeddedDB getEmbeddedDB() {
		return delegate.getEmbeddedDB();
	}

	@Override
	public RWScheduler getScheduler() {
		return delegate.getScheduler();
	}

	public EmbeddedDB getInternalDB() {
		return delegate.getInternalDB();
	}

	@Override
	<R, RS, RA> RS requestSync(RequestContext context, RocksDBAPICommand<R, RS, RA> request) {
		return delegate.requestSync(context, request);
	}

	@Override
	<R, RS, RA> RA requestAsync(RequestContext context, RocksDBAPICommand<R, RS, RA> request) {
		return delegate.requestAsync(context, request);
	}

	@Override
	Mono<CdcBatch> cdcPollBatchAsync(RequestContext context,
			@NotNull String id,
			@Nullable Long fromSeq,
			long maxEvents) {
		return delegate.cdcPollBatchAsync(context, id, fromSeq, maxEvents);
	}
}

/** Package-private raw implementation behind {@link EmbeddedConnection}. */
final class EmbeddedConnectionDelegate extends BaseConnection implements RocksDBAPI, InternalConnection {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedConnection.class);
	private static final String REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY = "reactor.onErrorDropped.local";
	private static final int ITERATOR_READ_STEP_SIZE = 4_096;
	private static final int ASYNC_TASK_QUEUED = 0;
	private static final int ASYNC_TASK_RUNNING = 1;
	private static final int ASYNC_TASK_FINISHED = 2;
	private static final int ASYNC_TASK_CANCELLED = 3;
	private final EmbeddedDB db;
	private final Map<Long, AsyncIteratorOperation> asyncIteratorOperations = new ConcurrentHashMap<>();
	private final Map<Long, CompletableFuture<Void>> closingAsyncIterators = new ConcurrentHashMap<>();
	private volatile boolean closingConnection;
	public static final URI PRIVATE_MEMORY_URL = URI.create("memory://private");

	EmbeddedConnectionDelegate(@Nullable Path path, String name, @Nullable Path embeddedConfig) throws IOException {
		super(name);
		this.db = new EmbeddedDB(path, name, embeddedConfig);
	}

	@VisibleForTesting
	public void closeTesting() throws IOException {
		beginIteratorShutdown();
		db.closeTesting();
		super.close();
	}

	@Override
	public void close() throws IOException {
		beginIteratorShutdown();
		db.close();
		super.close();
	}

	private void beginIteratorShutdown() {
		closingConnection = true;
		for (var operation : asyncIteratorOperations.values()) {
			operation.requestCancellation();
		}
	}

	@Override
	public URI getUrl() {
		return Optional.ofNullable(db.getPath()).map(Path::toUri).orElse(PRIVATE_MEMORY_URL);
	}

	@Override
	public EmbeddedDB getEmbeddedDB() {
		return db;
	}

	@Override
	public long openTransaction(long timeoutMs) {
		return db.openTransaction(timeoutMs, currentRequestContext().profile());
	}

	@Override
	public boolean closeTransaction(long transactionId, boolean commit) {
		return db.closeTransaction(transactionId,
				commit,
				currentRequestContext().profile());
	}

	@Override
	public void closeFailedUpdate(long updateId) throws RocksDBException {
		db.closeFailedUpdate(updateId);
	}

	@Override
	public long createColumn(String name, @NotNull ColumnSchema schema) {
		return db.createColumn(name, schema);
	}

	@Override
	public long uploadMergeOperator(String name, String className, byte[] jarData) throws RocksDBException {
		return db.uploadMergeOperator(name, className, jarData);
	}

	@Override
	public Long checkMergeOperator(String name, byte[] hash) throws RocksDBException {
		return db.checkMergeOperator(name, hash);
	}

	@Override
	public void deleteColumn(long columnId) throws RocksDBException {
		db.deleteColumn(columnId);
	}

	@Override
	public boolean deleteColumnIfExists(@NotNull String name) throws RocksDBException {
		return db.deleteColumnIfExists(name);
	}

	@Override
	public long getColumnId(@NotNull String name) {
		return db.getColumnId(name);
	}

	@Override
	public long estimateNumKeys(long columnId) {
		return db.estimateNumKeys(columnId);
	}

	@Override
	public <R, RS, RA> RS requestSync(RequestContext context, RocksDBAPICommand<R, RS, RA> req) {
		resolveCommand(context, req);
		if (db.getScheduler().isExecutingWorkloadTask()) {
			return withRequestContext(context, () -> req.handleSync(this));
		}
		return withRequestContext(context, () -> {
			Object asyncResult = requestAsync(context, req);
			if (asyncResult instanceof CompletableFuture<?> future) {
				try {
					@SuppressWarnings("unchecked")
					var result = (RS) future.get();
					return result;
				} catch (InterruptedException interrupted) {
					future.cancel(false);
					Thread.currentThread().interrupt();
					throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_UNKNOWN_ERROR,
							"Interrupted while waiting for workload admission", interrupted);
				} catch (ExecutionException failure) {
					var rocksError = findRocksDBException(failure);
					if (rocksError != null) {
						throw rocksError;
					}
					throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_UNKNOWN_ERROR,
							failure.getCause() != null ? failure.getCause() : failure);
				}
			}
			if (asyncResult instanceof Publisher<?> publisher) {
				@SuppressWarnings("unchecked")
				var stream = (RS) Flux.from(publisher).toStream();
				return stream;
			}
			throw new IllegalStateException("Unsupported async result for " + req.getClass().getName());
		});
	}

	@SuppressWarnings("unchecked")
    @Override
	public <R, RS, RA> RA requestAsync(RequestContext context, RocksDBAPICommand<R, RS, RA> req) {
		resolveCommand(context, req);
		return withRequestContext(context, () -> (RA) switch (req) {
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch putBatch -> this.putBatchAsync(
					putBatch.columnId(), putBatch.batchPublisher(), putBatch.mode());
			case RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch mergeBatch -> this.mergeBatchAsync(
					mergeBatch.columnId(), mergeBatch.batchPublisher(), mergeBatch.mode());
			case RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti existsMulti -> this.existsMultiAsync(
					existsMulti.transactionId(), existsMulti.columnId(), existsMulti.keys(), existsMulti.timeoutMs());
			case RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator closeIterator -> this.closeIteratorAsync(
					closeIterator.iteratorId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo seekTo -> this.seekToAsync(
					seekTo.iterationId(), seekTo.keys());
			case RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<?> subsequent -> this.subsequentAsync(
					subsequent.iterationId(), subsequent.skipCount(), subsequent.takeCount(), subsequent.requestType());
			case RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> reduceRange -> this.reduceRangeAsync(
					reduceRange.transactionId(),
					reduceRange.columnId(),
					reduceRange.startKeysInclusive(),
					reduceRange.endKeysExclusive(),
					reduceRange.reverse(),
					reduceRange.requestType(),
					reduceRange.timeoutMs());
			case RocksDBAPICommand.RocksDBAPICommandSingle.GetRangePage<?> page -> this.getRangePageAsync(
					page.transactionId(),
					page.columnId(),
					page.startKeysInclusive(),
					page.endKeysExclusive(),
					page.reverse(),
					page.resumeAfter(),
					page.requestType(),
					page.timeoutMs(),
					page.budget());
            case RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> getRange -> this.getRangeAsync(getRange.transactionId(), getRange.columnId(), getRange.startKeysInclusive(), getRange.endKeysExclusive(), getRange.reverse(), getRange.requestType(), getRange.timeoutMs());
            case RocksDBAPICommand.RocksDBAPICommandStream.ScanRaw scanRaw -> this.scanRawAsync(scanRaw.columnId(), scanRaw.shardIndex(), scanRaw.shardCount());
            case RocksDBAPICommand.RocksDBAPICommandStream.CdcPoll cdcPoll -> this.cdcPollAsync(cdcPoll.id(), cdcPoll.fromSeq(), cdcPoll.maxEvents());
			case RocksDBAPICommand.CdcCommit cdcCommit -> db.cdcCommitAsyncInternal(cdcCommit.id(), cdcCommit.seq());
			case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ -> supplyAsyncPreservingRunningCompletion(
					() -> withRequestContext(context, () -> req.handleSync(this)), commandExecutor(req));
            case RocksDBAPICommand.RocksDBAPICommandStream<?> _ -> throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, "The request of type " + req.getClass().getName() + " is not implemented in class " + this.getClass().getName());
		});
    }

	private RWScheduler.WorkloadExecutor commandExecutor(RocksDBAPICommand<?, ?, ?> command) {
		var context = currentRequestContext();
		var profile = resolveCommand(context, command);
		return db.getScheduler().executor(profile,
				command.operationFamily(),
				context.deadlineEpochMillis());
	}

	private Scheduler commandScheduler(RocksDBAPICommand<?, ?, ?> command) {
		var context = currentRequestContext();
		var profile = resolveCommand(context, command);
		return db.getScheduler().scheduler(profile,
				command.operationFamily(),
				context.deadlineEpochMillis());
	}

	private WorkloadProfile resolveCommand(RequestContext context,
			RocksDBAPICommand<?, ?, ?> command) {
		var settings = db.getWorkloadSettings();
		return WorkloadAdmission.resolve(context,
				command,
				settings.latencyFanOutMaxItems(),
				settings.latencyFanOutMaxBytes(),
				settings.latencyRangeMaxItems(),
				settings.latencyRangeMaxBytes());
	}

	@Override
	public Flux<SerializedKVBatch> scanRawAsync(long columnId, int shardIndex, int shardCount) {
		var command = new RocksDBAPICommand.RocksDBAPICommandStream.ScanRaw(
				columnId, shardIndex, shardCount);
		return db.scanRawAsyncInternal(
				columnId,
				shardIndex,
				shardCount,
				commandScheduler(command),
				commandExecutor(command));
	}

	@Override
	public Stream<SerializedKVBatch> scanRaw(long columnId, int shardIndex, int shardCount) {
		return db.scanRaw(columnId, shardIndex, shardCount);
	}

	@Override
	public <T> T put(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		db.validateTransactionOrUpdateProfile(transactionOrUpdateId, currentRequestContext().profile());
		return db.put(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> T delete(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull RequestType.RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		db.validateTransactionOrUpdateProfile(transactionOrUpdateId, currentRequestContext().profile());
		return db.delete(transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public <T> T merge(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		db.validateTransactionOrUpdateProfile(transactionOrUpdateId, currentRequestContext().profile());
		return db.merge(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> List<T> deleteMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		db.validateTransactionOrUpdateProfile(transactionOrUpdateId, currentRequestContext().profile());
		return db.deleteMulti(transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public void deleteRange(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		db.deleteRange(columnId, startKeysInclusive, endKeysExclusive);
	}

	@Override
	public <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		db.validateTransactionOrUpdateProfile(transactionOrUpdateId, currentRequestContext().profile());
		return db.putMulti(transactionOrUpdateId, columnId, keys, values, requestType);
	}

	@Override
	public <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		db.validateTransactionOrUpdateProfile(transactionOrUpdateId, currentRequestContext().profile());
		return db.mergeMulti(transactionOrUpdateId, columnId, keys, values, requestType);
	}

	@Override
	public <T> CompletableFuture<List<T>> deleteMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		db.validateTransactionOrUpdateProfile(transactionOrUpdateId, currentRequestContext().profile());
		var executor = db.getScheduler().writeExecutor();
		return supplyAsyncPreservingRunningCompletion(
				() -> db.deleteMulti(transactionOrUpdateId, columnId, keys, requestType), executor);
	}

	@Override
	public CompletableFuture<Void> putBatchAsync(long columnId,
											 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
											 @NotNull PutBatchMode mode) throws RocksDBException {
		var context = currentRequestContext();
		return db.putBatchInternal(columnId,
				batchPublisher,
				mode,
				db.getScheduler().scheduler(context, OperationFamily.MUTATION));
	}

	@Override
	public CompletableFuture<Void> mergeBatchAsync(long columnId,
											@NotNull Publisher<@NotNull KVBatch> batchPublisher,
											@NotNull MergeBatchMode mode) throws RocksDBException {
		var context = currentRequestContext();
		return db.mergeBatchInternal(columnId,
				batchPublisher,
				mode,
				db.getScheduler().scheduler(context, OperationFamily.MUTATION));
	}

	@Override
	public void putBatch(long columnId,
					 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
					 @NotNull PutBatchMode mode) throws RocksDBException {
		db.putBatch(columnId, batchPublisher, mode);
	}

	@Override
	public void mergeBatch(long columnId,
				   @NotNull Publisher<@NotNull KVBatch> batchPublisher,
				   @NotNull MergeBatchMode mode) throws RocksDBException {
		db.mergeBatch(columnId, batchPublisher, mode);
	}

	@Override
	public <T> T get(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super Buf, T> requestType) throws RocksDBException {
		return db.get(transactionOrUpdateId,
				columnId,
				keys,
				requestType,
				currentRequestContext().profile());
	}

	@Override
	public List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			long timeoutMs) throws RocksDBException {
		var context = currentRequestContext();
		return db.existsMulti(transactionId,
				columnId,
				keys,
				timeoutMs,
				context.profile(),
				context.deadlineEpochMillis());
	}

	@Override
	public CompletableFuture<List<Boolean>> existsMultiAsync(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			long timeoutMs) throws RocksDBException {
		var context = currentRequestContext();
		return db.existsMultiAsyncInternal(transactionId,
				columnId,
				keys,
				timeoutMs,
				context.profile(),
				db.getScheduler().executor(context, OperationFamily.BOUNDED_FAN_OUT),
				context.deadlineEpochMillis());
	}

	@Override
	public long openIterator(long transactionId,
			long columnId,
			@NotNull Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return db.openIterator(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				timeoutMs,
				currentRequestContext().profile());
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		db.closeIterator(iteratorId);
	}

	@Override
	public void seekTo(long iterationId, Keys keys) throws RocksDBException {
		db.validateIteratorProfile(iterationId, currentRequestContext().profile());
		db.seekTo(iterationId, keys);
	}

	@Override
	public <T> T subsequent(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		db.validateIteratorProfile(iterationId, currentRequestContext().profile());
		return db.subsequent(iterationId, skipCount, takeCount, requestType);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> CompletableFuture<T> subsequentAsync(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		if (requestType == null) {
			return CompletableFuture.failedFuture(RocksDBException.of(
					RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
					"Iterator request type cannot be null"));
		}
		if (skipCount < 0 || takeCount < 0) {
			return CompletableFuture.failedFuture(RocksDBException.of(
					RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					"Iterator skip and take counts must be non-negative"));
		}
		var context = currentRequestContext();
		db.validateIteratorProfile(iterationId, context.profile());
		var iteratorOperation = acquireAsyncIteratorOperation(iterationId);
		if (iteratorOperation == null) {
			return CompletableFuture.failedFuture(RocksDBException.of(
					RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					"Concurrent operation on iterator " + iterationId + " is not supported"));
		}

		var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<>(
				iterationId, skipCount, takeCount, requestType);
		RWScheduler.WorkloadExecutor workloadExecutor;
		WorkloadProfile profile;
		try {
			profile = resolveCommand(context, command);
			workloadExecutor = db.getScheduler().executor(
					profile, command.operationFamily(), context.deadlineEpochMillis());
		} catch (Throwable error) {
			releaseAsyncIteratorOperation(iterationId, iteratorOperation);
			return CompletableFuture.failedFuture(error);
		}

		if (requiresCooperativeIteratorContinuation(profile, skipCount, takeCount)) {
			var continuation = new CooperativeIteratorContinuation<T>(
					iterationId, skipCount, takeCount, requestType, iteratorOperation);
			iteratorOperation.attachCancellation(() -> continuation.cancel(false));
			try {
				continuation.attach(workloadExecutor.executeCooperatively(continuation, 0L));
			} catch (Throwable admissionFailure) {
				continuation.finish(null, admissionFailure);
			}
			return continuation;
		}

		var cancelled = new AtomicBoolean();
		CompletableFuture<T> result = new CompletableFuture<>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				cancelled.set(true);
				return super.cancel(false);
			}
		};

		CompletableFuture<T> operation;
		try {
			if (skipCount == 0 && takeCount == 0) {
				operation = scheduleIteratorStep(
						iterationId, 0, requestType, cancelled, workloadExecutor);
			} else {
				var skipped = advanceIteratorAsync(
						iterationId, skipCount, cancelled, workloadExecutor);
				operation = switch (requestType) {
					case RequestType.RequestNothing<?> _ -> (CompletableFuture<T>) (CompletableFuture<?>) skipped
							.thenCompose(exhausted -> exhausted
									? CompletableFuture.completedFuture(null)
									: advanceIteratorAsync(
											iterationId, takeCount, cancelled, workloadExecutor).thenApply(_ -> null));
					case RequestType.RequestExists<?> _ -> (CompletableFuture<T>) (CompletableFuture<?>) skipped
							.thenCompose(exhausted -> exhausted
									? CompletableFuture.completedFuture(false)
									: subsequentExistsAsync(
											iterationId, takeCount, false, cancelled, workloadExecutor));
					case RequestType.RequestMulti<?> _ -> (CompletableFuture<T>) (CompletableFuture<?>) skipped
							.thenCompose(exhausted -> exhausted
									? CompletableFuture.completedFuture(List.of())
									: subsequentMultiAsync(
											iterationId,
											takeCount,
											new ArrayList<>(),
											cancelled,
											workloadExecutor));
				};
			}
		} catch (Throwable error) {
			releaseAsyncIteratorOperation(iterationId, iteratorOperation);
			return CompletableFuture.failedFuture(error);
		}

		operation.whenComplete((value, error) -> {
			releaseAsyncIteratorOperation(iterationId, iteratorOperation);
			if (error != null) {
				result.completeExceptionally(error instanceof CompletionException completionError
						&& completionError.getCause() != null
						? completionError.getCause()
						: error);
			} else {
				result.complete(value);
			}
		});
		return result;
	}

	private static boolean requiresCooperativeIteratorContinuation(WorkloadProfile profile,
			long skipCount,
			long takeCount) {
		return profile != WorkloadProfile.LATENCY
				&& (skipCount > ITERATOR_READ_STEP_SIZE
						|| takeCount > ITERATOR_READ_STEP_SIZE
						|| skipCount > ITERATOR_READ_STEP_SIZE - takeCount);
	}

	@Override
	public CompletableFuture<Void> seekToAsync(long iterationId, @NotNull Keys keys) throws RocksDBException {
		db.validateIteratorProfile(iterationId, currentRequestContext().profile());
		var iteratorOperation = acquireAsyncIteratorOperation(iterationId);
		if (iteratorOperation == null) {
			return CompletableFuture.failedFuture(concurrentIteratorOperation(iterationId));
		}
		CompletableFuture<Void> result;
		try {
			var command = new RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo(iterationId, keys);
			result = supplyAsyncPreservingRunningCompletion(() -> {
				try {
					db.seekTo(iterationId, keys);
					return null;
				} finally {
					// Release before the future becomes observable as complete. Completing the
					// worker future first lets a joining caller race its next iterator request
					// against a dependent whenComplete callback.
					releaseAsyncIteratorOperation(iterationId, iteratorOperation);
				}
			}, commandExecutor(command));
		} catch (Throwable error) {
			releaseAsyncIteratorOperation(iterationId, iteratorOperation);
			return CompletableFuture.failedFuture(error);
		}
		// Also covers cancellation while queued and executor rejection. Release is
		// idempotent, so running success/failure may safely arrive here after finally.
		result.whenComplete((_, _) -> releaseAsyncIteratorOperation(iterationId, iteratorOperation));
		return result;
	}

	@Override
	public CompletableFuture<Void> closeIteratorAsync(long iteratorId) throws RocksDBException {
		var closeToken = new CompletableFuture<Void>();
		var existingClose = closingAsyncIterators.putIfAbsent(iteratorId, closeToken);
		if (existingClose != null) {
			return existingClose.thenApply(_ -> null);
		}
		var closeOperation = closeAsyncIteratorWhenIdle(iteratorId);
		closeOperation.whenComplete((_, failure) -> {
			try {
				if (failure != null) {
					closeToken.completeExceptionally(failure);
				} else {
					closeToken.complete(null);
				}
			} finally {
				closingAsyncIterators.remove(iteratorId, closeToken);
			}
		});
		// Do not expose the coordination token itself: cancellation by one caller
		// must not reopen the iterator-operation admission gate for another caller.
		return closeToken.thenApply(_ -> null);
	}

	private CompletableFuture<Void> closeAsyncIteratorWhenIdle(long iteratorId) {
		var active = asyncIteratorOperations.get(iteratorId);
		if (active != null) {
			return active.finished.handle((_, _) -> null)
					.thenCompose(_ -> closeAsyncIteratorWhenIdle(iteratorId));
		}
		return supplyAsyncPreservingRunningCompletion(() -> {
			db.closeIterator(iteratorId);
			return null;
		}, db.getScheduler().controlExecutor());
	}

	private @Nullable AsyncIteratorOperation acquireAsyncIteratorOperation(long iteratorId) {
		if (closingConnection || closingAsyncIterators.containsKey(iteratorId)) {
			return null;
		}
		var operation = new AsyncIteratorOperation();
		if (asyncIteratorOperations.putIfAbsent(iteratorId, operation) != null) {
			return null;
		}
		if (closingConnection || closingAsyncIterators.containsKey(iteratorId)) {
			releaseAsyncIteratorOperation(iteratorId, operation);
			return null;
		}
		return operation;
	}

	private void releaseAsyncIteratorOperation(long iteratorId, AsyncIteratorOperation operation) {
		if (asyncIteratorOperations.remove(iteratorId, operation)) {
			operation.finished.complete(null);
		}
	}

	private RocksDBException concurrentIteratorOperation(long iteratorId) {
		return RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
				"Concurrent operation on iterator " + iteratorId + " is not supported");
	}

	private static final class AsyncIteratorOperation {

		private final CompletableFuture<Void> finished = new CompletableFuture<>();
		private volatile boolean cancellationRequested;
		private volatile @Nullable Runnable cancellation;

		private void attachCancellation(Runnable cancellation) {
			this.cancellation = Objects.requireNonNull(cancellation, "cancellation");
			if (cancellationRequested) {
				cancellation.run();
			}
		}

		private void requestCancellation() {
			cancellationRequested = true;
			var currentCancellation = cancellation;
			if (currentCancellation != null) {
				currentCancellation.run();
			}
		}
	}

	private enum IteratorContinuationStage {
		SKIP,
		TAKE,
		DONE
	}

	private enum IteratorRequestMode {
		NONE,
		EXISTS,
		MULTI
	}

	/**
	 * One logical iterator request whose scheduler node and bounded native progress survive
	 * cooperative redispatch. No JNI-owned state is retained here: every native step enters
	 * and leaves {@link EmbeddedDB} before this state can yield.
	 */
	private final class CooperativeIteratorContinuation<T> extends CompletableFuture<T>
			implements RWScheduler.CooperativeTask, reactor.core.Disposable {

		private final long iterationId;
		private final AsyncIteratorOperation iteratorOperation;
		private final IteratorRequestMode mode;
		private final @Nullable ArrayList<Buf> values;
		private final AtomicBoolean terminated = new AtomicBoolean();
		private volatile boolean cancellationRequested;
		private volatile @Nullable CancellationException cancellationFailure;
		private volatile @Nullable RWScheduler.CooperativeHandle handle;
		private IteratorContinuationStage stage;
		private long remainingSkip;
		private long remainingTake;
		private boolean exhausted;
		private boolean found;

		private CooperativeIteratorContinuation(long iterationId,
				long skipCount,
				long takeCount,
				RequestType.RequestIterate<? super Buf, T> requestType,
				AsyncIteratorOperation iteratorOperation) {
			this.iterationId = iterationId;
			this.iteratorOperation = iteratorOperation;
			this.mode = switch (requestType) {
				case RequestType.RequestNothing<?> _ -> IteratorRequestMode.NONE;
				case RequestType.RequestExists<?> _ -> IteratorRequestMode.EXISTS;
				case RequestType.RequestMulti<?> _ -> IteratorRequestMode.MULTI;
			};
			this.values = mode == IteratorRequestMode.MULTI
					? new ArrayList<>((int) Math.min(takeCount, 1_024L))
					: null;
			this.remainingSkip = skipCount;
			this.remainingTake = takeCount;
			this.stage = skipCount == 0L
					? (takeCount == 0L ? IteratorContinuationStage.DONE : IteratorContinuationStage.TAKE)
					: IteratorContinuationStage.SKIP;
		}

		private void attach(RWScheduler.CooperativeHandle handle) {
			this.handle = Objects.requireNonNull(handle, "handle");
			if (cancellationRequested) {
				handle.dispose();
			}
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (terminated.get()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			try {
				while (true) {
					var terminationFailure = requestedTermination(context);
					if (terminationFailure != null) {
						finish(null, terminationFailure);
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (stage == IteratorContinuationStage.DONE) {
						finish(completedValue(), null);
						return RWScheduler.CooperativeResult.COMPLETE;
					}

					switch (stage) {
						case SKIP -> runSkipStep();
						case TAKE -> runTakeStep();
						case DONE -> throw new IllegalStateException("Completed iterator stage was dispatched");
					}

					terminationFailure = requestedTermination(context);
					if (terminationFailure != null) {
						finish(null, terminationFailure);
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (stage == IteratorContinuationStage.DONE) {
						finish(completedValue(), null);
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (context.preemptionRequested()) {
						return RWScheduler.CooperativeResult.YIELD;
					}
				}
			} catch (VirtualMachineError fatal) {
				finish(null, fatal);
				throw fatal;
			} catch (Throwable failure) {
				finish(null, failure);
				return RWScheduler.CooperativeResult.COMPLETE;
			}
		}

		private @Nullable RuntimeException requestedTermination(RWScheduler.CooperativeContext context) {
			if (cancellationRequested) {
				return Objects.requireNonNull(cancellationFailure, "cancellationFailure");
			}
			if (!context.terminationRequested()) {
				return null;
			}
			return Objects.requireNonNull(
					context.terminationFailure(), "Cooperative termination failure");
		}

		private void runSkipStep() {
			long step = Math.min(remainingSkip, ITERATOR_READ_STEP_SIZE);
			long advanced = db.advanceIteratorInternal(iterationId, step);
			remainingSkip -= advanced;
			if (advanced < step) {
				exhausted = true;
				stage = IteratorContinuationStage.DONE;
			} else if (remainingSkip == 0L) {
				stage = remainingTake == 0L
						? IteratorContinuationStage.DONE
						: IteratorContinuationStage.TAKE;
			}
		}

		private void runTakeStep() {
			long step = Math.min(remainingTake, ITERATOR_READ_STEP_SIZE);
			switch (mode) {
				case NONE, EXISTS -> {
					long advanced = db.advanceIteratorInternal(iterationId, step);
					remainingTake -= advanced;
					if (mode == IteratorRequestMode.EXISTS) {
						found |= advanced > 0L;
					}
					if (advanced < step) {
						exhausted = true;
						stage = IteratorContinuationStage.DONE;
					} else if (remainingTake == 0L) {
						stage = IteratorContinuationStage.DONE;
					}
				}
				case MULTI -> {
					List<Buf> page = db.subsequent(iterationId, 0L, step, RequestType.multi());
					Objects.requireNonNull(values, "values").addAll(page);
					remainingTake -= page.size();
					if (page.size() < step) {
						exhausted = true;
						stage = IteratorContinuationStage.DONE;
					} else if (remainingTake == 0L) {
						stage = IteratorContinuationStage.DONE;
					}
				}
			}
		}

		private T completedValue() {
			if (exhausted && stage != IteratorContinuationStage.DONE) {
				throw new IllegalStateException("Exhausted iterator continuation is not terminal");
			}
			Object value = switch (mode) {
				case NONE -> null;
				case EXISTS -> found;
				case MULTI -> Objects.requireNonNull(values, "values");
			};
			return RequestType.safeCast(value);
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (!super.cancel(false)) {
				return false;
			}
			cancellationFailure = new CancellationException("Iterator continuation cancelled");
			cancellationRequested = true;
			var currentHandle = handle;
			if (currentHandle != null) {
				currentHandle.dispose();
			}
			return true;
		}

		@Override
		public void reject(RuntimeException failure) {
			finish(null, failure);
		}

		@Override
		public void dispose() {
			cancel(false);
		}

		@Override
		public boolean isDisposed() {
			return terminated.get();
		}

		private void finish(@Nullable T value, @Nullable Throwable failure) {
			if (!terminated.compareAndSet(false, true)) {
				return;
			}
			// Publish iterator-gate release before success/failure becomes observable.
			releaseAsyncIteratorOperation(iterationId, iteratorOperation);
			if (failure == null) {
				complete(value);
			} else {
				completeExceptionally(failure);
			}
		}
	}

	/** @return true when the iterator was exhausted before consuming the requested count. */
	private CompletableFuture<Boolean> advanceIteratorAsync(long iterationId,
			long remaining,
			AtomicBoolean cancelled,
			Executor workloadExecutor) {
		if (remaining <= 0) {
			return CompletableFuture.completedFuture(false);
		}
		long step = Math.min(remaining, ITERATOR_READ_STEP_SIZE);
		return scheduleIteratorAdvanceStep(iterationId, step, cancelled, workloadExecutor)
				.thenCompose(advanced -> advanced < step
						? CompletableFuture.completedFuture(true)
						: advanceIteratorAsync(iterationId, remaining - step, cancelled, workloadExecutor));
	}

	private CompletableFuture<Boolean> subsequentExistsAsync(long iterationId,
			long remaining,
			boolean found,
			AtomicBoolean cancelled,
			Executor workloadExecutor) {
		if (remaining <= 0 || cancelled.get()) {
			return cancelled.get()
					? CompletableFuture.failedFuture(new CancellationException())
					: CompletableFuture.completedFuture(found);
		}
		long step = Math.min(remaining, ITERATOR_READ_STEP_SIZE);
		return scheduleIteratorAdvanceStep(iterationId, step, cancelled, workloadExecutor)
				.thenCompose(advanced -> {
					boolean pageFound = found || advanced > 0L;
					return advanced < step
							? CompletableFuture.completedFuture(pageFound)
							: subsequentExistsAsync(
									iterationId, remaining - step, pageFound, cancelled, workloadExecutor);
				});
	}

	private CompletableFuture<List<Buf>> subsequentMultiAsync(long iterationId,
			long remaining,
			ArrayList<Buf> values,
			AtomicBoolean cancelled,
			Executor workloadExecutor) {
		if (remaining <= 0 || cancelled.get()) {
			return cancelled.get()
					? CompletableFuture.failedFuture(new CancellationException())
					: CompletableFuture.completedFuture(values);
		}
		long step = Math.min(remaining, ITERATOR_READ_STEP_SIZE);
		return scheduleIteratorStep(
				iterationId, step, RequestType.multi(), cancelled, workloadExecutor)
				.thenCompose(page -> {
					values.addAll(page);
					return page.size() < step
							? CompletableFuture.completedFuture(values)
							: subsequentMultiAsync(
									iterationId, remaining - step, values, cancelled, workloadExecutor);
				});
	}

	private <T> CompletableFuture<T> scheduleIteratorStep(long iterationId,
			long takeCount,
			RequestType.RequestIterate<? super Buf, T> requestType,
			AtomicBoolean cancelled,
			Executor workloadExecutor) {
		if (cancelled.get()) {
			return CompletableFuture.failedFuture(new CancellationException());
		}
		return CompletableFuture.supplyAsync(() -> {
			if (cancelled.get()) {
				throw new CancellationException();
			}
			return db.subsequent(iterationId, 0, takeCount, requestType);
		}, workloadExecutor);
	}

	private CompletableFuture<Long> scheduleIteratorAdvanceStep(long iterationId,
			long takeCount,
			AtomicBoolean cancelled,
			Executor workloadExecutor) {
		if (cancelled.get()) {
			return CompletableFuture.failedFuture(new CancellationException());
		}
		return CompletableFuture.supplyAsync(() -> {
			if (cancelled.get()) {
				throw new CancellationException();
			}
			return db.advanceIteratorInternal(iterationId, takeCount);
		}, workloadExecutor);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T reduceRange(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.@NotNull RequestReduceRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		var context = currentRequestContext();
		db.validateTransactionProfile(transactionId, context.profile());
		if (requestType instanceof RequestType.RequestEntriesCount<?>) {
			var command = new RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<>(
					transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					requestType,
					timeoutMs);
			return (T) db.countRangeAsyncInternal(transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					timeoutMs,
					context.deadlineEpochMillis(),
					resolveCommand(context, command)).block();
		}
		return db.reduceRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> CompletableFuture<T> reduceRangeAsync(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@NotNull RequestType.RequestReduceRange<? super KV, T> requestType,
		long timeoutMs) throws RocksDBException {
		var context = currentRequestContext();
		db.validateTransactionProfile(transactionId, context.profile());
		var command = new RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<>(
				transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs);
		if (requestType instanceof RequestType.RequestEntriesCount<?>) {
			Consumer<Throwable> lateFailureHandler = error -> logLateRangeCountFailure(
					transactionId, columnId, reverse, timeoutMs, error);
			return (CompletableFuture<T>) (CompletableFuture<?>) db.countRangeAsyncInternal(
					transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					timeoutMs,
					context.deadlineEpochMillis(),
					resolveCommand(context, command))
					.contextWrite(reactorContext -> reactorContext.put(
							REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY, lateFailureHandler))
					.toFuture();
		}
		long queuedAtNanos = System.nanoTime();
		return supplyAsyncPreservingRunningCompletion(() -> db.reduceRange(
				transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				remainingReadTimeoutMillis(timeoutMs, queuedAtNanos)), commandExecutor(command));
	}

	@Override
	public <T> RangePage<T> getRangePage(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@Nullable Keys resumeAfter,
			@NotNull RequestType.RequestGetRange<? super KV, T> requestType,
			long timeoutMs,
			@NotNull RangeBudget budget) throws RocksDBException {
		db.validateTransactionProfile(transactionId, currentRequestContext().profile());
		return db.getRangePage(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				resumeAfter,
				requestType,
				timeoutMs,
				budget);
	}

	@Override
	public <T> CompletableFuture<RangePage<T>> getRangePageAsync(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@Nullable Keys resumeAfter,
			@NotNull RequestType.RequestGetRange<? super KV, T> requestType,
			long timeoutMs,
			@NotNull RangeBudget budget) throws RocksDBException {
		db.validateTransactionProfile(transactionId, currentRequestContext().profile());
		var command = new RocksDBAPICommand.RocksDBAPICommandSingle.GetRangePage<>(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				resumeAfter,
				requestType,
				timeoutMs,
				budget);
		long queuedAtNanos = System.nanoTime();
		return supplyAsyncPreservingRunningCompletion(() -> db.getRangePage(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				resumeAfter,
				requestType,
				remainingReadTimeoutMillis(timeoutMs, queuedAtNanos),
				budget), commandExecutor(command), budget.maxBytes());
	}

	/**
	 * Convert a request-scoped timeout into the native budget remaining after scheduler
	 * admission. Millisecond rounding preserves a positive final fraction without allowing
	 * queueing time to grant the native read a fresh full timeout.
	 */
	private static long remainingReadTimeoutMillis(long timeoutMs, long queuedAtNanos) {
		if (timeoutMs <= 0 || timeoutMs == Long.MAX_VALUE) {
			return timeoutMs;
		}
		long elapsedNanos = Math.max(0L, System.nanoTime() - queuedAtNanos);
		long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
		if (elapsedMillis >= timeoutMs) {
			throw RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"Deadline exceeded");
		}
		return timeoutMs - elapsedMillis;
	}

	/**
	 * Cancellation removes work that has not started, but a running native call keeps
	 * its future observable so request-scoped transport logging can retain its real
	 * terminal failure instead of replacing it with CancellationException.
	 */
	private <T> CompletableFuture<T> supplyAsyncPreservingRunningCompletion(Supplier<T> supplier,
			Executor executor) {
		return supplyAsyncPreservingRunningCompletion(supplier, executor, 0L);
	}

	private <T> CompletableFuture<T> supplyAsyncPreservingRunningCompletion(Supplier<T> supplier,
			Executor executor,
			long estimatedBytes) {
		var future = new RunningCompletionFuture<>(
				supplier,
				executor,
				db.getScheduler()::removeQueuedTask);
		try {
			if (executor instanceof RWScheduler.WorkloadExecutor workloadExecutor) {
				workloadExecutor.execute(future, estimatedBytes);
			} else {
				executor.execute(future);
			}
		} catch (Throwable error) {
			future.reject(error);
		}
		return future;
	}

	private static void logLateRangeCountFailure(long transactionId,
			long columnId,
			boolean reverse,
			long timeoutMs,
			Throwable error) {
		var rocksError = findRocksDBException(error);
		if (rocksError != null) {
			LOG.warn("Late embedded read failure after cancellation: operation=reduceRangeEntriesCount, "
					+ "transactionId={}, columnId={}, reverse={}, timeoutMs={}, errorType={}, message={}",
					transactionId,
					columnId,
					reverse,
					timeoutMs,
					rocksError.getErrorUniqueId(),
					sanitizeForLog(rocksError.getMessage()));
			LOG.debug("Late embedded read failure stack: operation=reduceRangeEntriesCount, columnId={}",
					columnId,
					error);
		} else if (error instanceof CancellationException) {
			LOG.debug("Late embedded read cancellation: operation=reduceRangeEntriesCount, columnId={}", columnId);
		} else {
			LOG.error("Unexpected late embedded read failure after cancellation: "
					+ "operation=reduceRangeEntriesCount, transactionId={}, columnId={}, reverse={}, timeoutMs={}",
					transactionId,
					columnId,
					reverse,
					timeoutMs,
					error);
		}
	}

	private static @Nullable RocksDBException findRocksDBException(Throwable error) {
		var current = error;
		for (int depth = 0; current != null && depth < 32; depth++) {
			if (current instanceof RocksDBException rocksError) {
				return rocksError;
			}
			if (current.getCause() == current) {
				break;
			}
			current = current.getCause();
		}
		return null;
	}

	private static String sanitizeForLog(@Nullable String value) {
		if (value == null) {
			return "<none>";
		}
		var sanitized = value.replace("\\", "\\\\").replace("\r", "\\r").replace("\n", "\\n");
		return sanitized.length() <= 256 ? sanitized : sanitized.substring(0, 253) + "...";
	}

	private static final class RunningCompletionFuture<T> extends CompletableFuture<T> implements Runnable {

		private final Supplier<T> supplier;
		private final Executor executor;
		private final java.util.function.BiPredicate<Executor, Runnable> queuedTaskRemover;
		private final AtomicInteger state = new AtomicInteger(ASYNC_TASK_QUEUED);

		private RunningCompletionFuture(Supplier<T> supplier,
				Executor executor,
				java.util.function.BiPredicate<Executor, Runnable> queuedTaskRemover) {
			this.supplier = supplier;
			this.executor = executor;
			this.queuedTaskRemover = queuedTaskRemover;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (!state.compareAndSet(ASYNC_TASK_QUEUED, ASYNC_TASK_CANCELLED)) {
				return false;
			}
			var cancelled = super.cancel(mayInterruptIfRunning);
			queuedTaskRemover.test(executor, this);
			return cancelled;
		}

		@Override
		public void run() {
			if (!state.compareAndSet(ASYNC_TASK_QUEUED, ASYNC_TASK_RUNNING)) {
				return;
			}
			try {
				complete(supplier.get());
			} catch (Throwable error) {
				completeExceptionally(error);
			} finally {
				state.set(ASYNC_TASK_FINISHED);
			}
		}

		private void reject(Throwable error) {
			if (state.compareAndSet(ASYNC_TASK_QUEUED, ASYNC_TASK_FINISHED)) {
				completeExceptionally(error);
			}
		}
	}

	@Override
	public <T> Stream<T> getRange(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.@NotNull RequestGetRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		var context = currentRequestContext();
		db.validateTransactionProfile(transactionId, context.profile());
		var command = new RocksDBAPICommand.RocksDBAPICommandStream.GetRange<>(
				transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs);
		return Flux.from(db.getRangeAsyncInternal(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs,
				context.deadlineEpochMillis(),
				resolveCommand(context, command))).toStream();
	}

	@Override
	public <T> Publisher<T> getRangeAsync(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.RequestGetRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		var context = currentRequestContext();
		db.validateTransactionProfile(transactionId, context.profile());
		var command = new RocksDBAPICommand.RocksDBAPICommandStream.GetRange<>(
				transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs);
		return db.getRangeAsyncInternal(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs,
				context.deadlineEpochMillis(),
				resolveCommand(context, command));
	}

	@Override
	public void flush() {
		db.flush();
	}

	@Override
	public void compact() {
		db.compact();
	}

	@Override
 public Map<String, ColumnSchema> getAllColumnDefinitions() throws RocksDBException {
        return db.getAllColumnDefinitions();
    }

    @Override
    public RWScheduler getScheduler() {
        return db.getScheduler();
    }

    @org.jetbrains.annotations.VisibleForTesting
    public EmbeddedDB getInternalDB() {
        return db;
    }

    // CDC API implementation stubs delegating to EmbeddedDB
    public long cdcCreate(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds) throws RocksDBException {
        return db.cdcCreate(id, fromSeq, columnIds, null, null);
    }

    public long cdcCreate(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds, @Nullable Boolean resolvedValues) throws RocksDBException {
        return db.cdcCreate(id, fromSeq, columnIds, resolvedValues, null);
    }

    @Override
    public long cdcCreate(@NotNull String id,
                          @Nullable Long fromSeq,
                          @Nullable List<Long> columnIds,
                          @Nullable Boolean resolvedValues,
                          @Nullable OptionalLong expectedLastCommitted) throws RocksDBException {
        return db.cdcCreate(id, fromSeq, columnIds, resolvedValues, expectedLastCommitted);
    }

	public void cdcDelete(@NotNull String id) throws RocksDBException {
		db.cdcDelete(id);
	}

	@Override
	public long cdcGetEarliestAvailableSequence() throws RocksDBException {
		return db.cdcGetEarliestAvailableSequence();
	}

	@Override
	public java.util.OptionalLong cdcGetLastCommittedSequence(@NotNull String id) throws RocksDBException {
		return db.cdcGetLastCommittedSequence(id);
	}

	public void cdcCommit(@NotNull String id, long seq) throws RocksDBException {
        db.cdcCommit(id, seq);
    }

    public @NotNull java.util.stream.Stream<CDCEvent> cdcPoll(@NotNull String id, @Nullable Long fromSeq, long maxEvents) throws RocksDBException {
        return db.cdcPoll(id, fromSeq, maxEvents);
    }

    public @NotNull Publisher<CDCEvent> cdcPollAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) throws RocksDBException {
        // Default: defer to DB implementation; fallback to blocking stream if async is not supported
        return db.cdcPollAsyncInternal(id, fromSeq, maxEvents);
    }

	@Override
	Mono<CdcBatch> cdcPollBatchAsync(RequestContext context,
			@NotNull String id,
			@Nullable Long fromSeq,
			long maxEvents) {
		return withRequestContext(context, () -> cdcPollBatchAsync(id, fromSeq, maxEvents));
	}

    @Override
    public Mono<CdcBatch> cdcPollBatchAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) {
        try {
            return db.cdcPollBatchAsyncInternal(id, fromSeq, maxEvents);
        } catch (Throwable e) {
            return Mono.error(e);
        }
    }
}
