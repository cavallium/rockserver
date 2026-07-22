package it.cavallium.rockserver.core.client;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
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

public class EmbeddedConnection extends BaseConnection implements RocksDBAPI, InternalConnection {

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
	public static final URI PRIVATE_MEMORY_URL = URI.create("memory://private");

	public EmbeddedConnection(@Nullable Path path, String name, @Nullable Path embeddedConfig) throws IOException {
		super(name);
		this.db = new EmbeddedDB(path, name, embeddedConfig);
	}

	@VisibleForTesting
	public void closeTesting() throws IOException {
		db.closeTesting();
		super.close();
	}

	@Override
	public void close() throws IOException {
		db.close();
		super.close();
	}

	@Override
	public URI getUrl() {
		return Optional.ofNullable(db.getPath()).map(Path::toUri).orElse(PRIVATE_MEMORY_URL);
	}

	@Override
	public RocksDBSyncAPI getSyncApi() {
		return this;
	}

	@Override
	public RocksDBAsyncAPI getAsyncApi() {
		return this;
	}

	@Override
	public EmbeddedDB getEmbeddedDB() {
		return db;
	}

	@Override
	public long openTransaction(long timeoutMs) {
		return db.openTransaction(timeoutMs);
	}

	@Override
	public boolean closeTransaction(long transactionId, boolean commit) {
		return db.closeTransaction(transactionId, commit);
	}

	@Override
	public boolean closeTransaction(long transactionId, boolean commit, @NotNull WriteClass writeClass) {
		return db.closeTransaction(transactionId, commit);
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
	public long createColumn(String name, @NotNull ColumnSchema schema, @NotNull WriteClass writeClass) {
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
	public void deleteColumn(long columnId, @NotNull WriteClass writeClass) throws RocksDBException {
		db.deleteColumn(columnId);
	}

	@Override
	public boolean deleteColumnIfExists(@NotNull String name) throws RocksDBException {
		return db.deleteColumnIfExists(name);
	}

	@Override
	public boolean deleteColumnIfExists(@NotNull String name, @NotNull WriteClass writeClass) throws RocksDBException {
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
	public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
		return req.handleSync(this);
	}

	@SuppressWarnings("unchecked")
    @Override
    public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
        return (RA) switch (req) {
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch putBatch -> this.putBatchAsync(
                    putBatch.columnId(), putBatch.batchPublisher(), putBatch.mode(), putBatch.writeClass());
			case RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch mergeBatch -> this.mergeBatchAsync(
					mergeBatch.columnId(), mergeBatch.batchPublisher(), mergeBatch.mode(), mergeBatch.writeClass());
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
            case RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> getRange -> this.getRangeAsync(getRange.transactionId(), getRange.columnId(), getRange.startKeysInclusive(), getRange.endKeysExclusive(), getRange.reverse(), getRange.requestType(), getRange.timeoutMs());
            case RocksDBAPICommand.RocksDBAPICommandStream.ScanRaw scanRaw -> this.scanRawAsync(scanRaw.columnId(), scanRaw.shardIndex(), scanRaw.shardCount());
            case RocksDBAPICommand.RocksDBAPICommandStream.CdcPoll cdcPoll -> this.cdcPollAsync(cdcPoll.id(), cdcPoll.fromSeq(), cdcPoll.maxEvents());
            case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ -> CompletableFuture.supplyAsync(() -> req.handleSync(this),
                    commandExecutor(req));
            case RocksDBAPICommand.RocksDBAPICommandStream<?> _ -> throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, "The request of type " + req.getClass().getName() + " is not implemented in class " + this.getClass().getName());
        };
    }

	private java.util.concurrent.Executor commandExecutor(RocksDBAPICommand<?, ?, ?> command) {
		if (command instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator
				|| command instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate
				|| (command instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction closeTransaction
				&& !closeTransaction.commit())
				|| command instanceof RocksDBAPICommand.CdcCommit) {
			return db.getScheduler().controlExecutor();
		}
		if (command instanceof RocksDBAPICommand.Flush
					|| command instanceof RocksDBAPICommand.Compact) {
			return db.getScheduler().maintenanceExecutor();
		}
		if (command instanceof RocksDBAPICommand.CdcGetEarliestAvailableSequence) {
			return db.getScheduler().cdcExecutor();
		}
		if (command instanceof RocksDBAPICommand.CdcCreate create
				&& create.fromSeq() != null
				&& create.fromSeq() == 0L) {
			return db.getScheduler().cdcExecutor();
		}
		if (!command.isReadOnly()) {
			return db.getScheduler().writeExecutor(command.writeClass());
		}
		return command.readWorkClass() == RocksDBAPICommand.ReadWorkClass.INTERACTIVE
				? db.getScheduler().interactiveReadExecutor()
				: db.getScheduler().readExecutor();
	}

	@Override
	public Flux<SerializedKVBatch> scanRawAsync(long columnId, int shardIndex, int shardCount) {
		return db.scanRawAsyncInternal(columnId, shardIndex, shardCount);
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
		return db.put(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> T put(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return db.put(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> T delete(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull RequestType.RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return db.delete(transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public <T> T delete(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull RequestType.RequestDelete<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return db.delete(transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public <T> T merge(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return db.merge(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> T merge(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return db.merge(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> List<T> deleteMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return db.deleteMulti(transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public <T> List<T> deleteMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			RequestDelete<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return db.deleteMulti(transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public void deleteRange(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		db.deleteRange(columnId, startKeysInclusive, endKeysExclusive);
	}

	@Override
	public void deleteRange(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			@NotNull WriteClass writeClass) throws RocksDBException {
		db.deleteRange(columnId, startKeysInclusive, endKeysExclusive);
	}

	@Override
	public <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return db.putMulti(transactionOrUpdateId, columnId, keys, values, requestType);
	}

	@Override
	public <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return db.putMulti(transactionOrUpdateId, columnId, keys, values, requestType);
	}

	@Override
	public <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return db.mergeMulti(transactionOrUpdateId, columnId, keys, values, requestType);
	}

	@Override
	public <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestMerge<? super Buf, T> requestType,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return db.mergeMulti(transactionOrUpdateId, columnId, keys, values, requestType);
	}

	@Override
	public <T> CompletableFuture<List<T>> deleteMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return CompletableFuture.supplyAsync(() -> {
			try {
				return db.deleteMulti(transactionOrUpdateId, columnId, keys, requestType);
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}, (java.util.concurrent.Executor) db.getScheduler().write()::schedule);
	}

	@Override
	public CompletableFuture<Void> putBatchAsync(long columnId,
											 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
											 @NotNull PutBatchMode mode) throws RocksDBException {
		return db.putBatchInternal(columnId, batchPublisher, mode);
	}

	@Override
	public CompletableFuture<Void> putBatchAsync(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return db.putBatchInternal(columnId, batchPublisher, mode);
	}

	@Override
	public CompletableFuture<Void> mergeBatchAsync(long columnId,
											@NotNull Publisher<@NotNull KVBatch> batchPublisher,
											@NotNull MergeBatchMode mode) throws RocksDBException {
		return db.mergeBatchInternal(columnId, batchPublisher, mode);
	}

	@Override
	public CompletableFuture<Void> mergeBatchAsync(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode,
			@NotNull WriteClass writeClass) throws RocksDBException {
		return db.mergeBatchInternal(columnId, batchPublisher, mode);
	}

	@Override
	public void putBatch(long columnId,
					 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
					 @NotNull PutBatchMode mode) throws RocksDBException {
		db.putBatch(columnId, batchPublisher, mode);
	}

	@Override
	public void putBatch(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode,
			@NotNull WriteClass writeClass) throws RocksDBException {
		db.putBatch(columnId, batchPublisher, mode);
	}

	@Override
	public void mergeBatch(long columnId,
				   @NotNull Publisher<@NotNull KVBatch> batchPublisher,
				   @NotNull MergeBatchMode mode) throws RocksDBException {
		db.mergeBatch(columnId, batchPublisher, mode);
	}

	@Override
	public void mergeBatch(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode,
			@NotNull WriteClass writeClass) throws RocksDBException {
		db.mergeBatch(columnId, batchPublisher, mode);
	}

	@Override
	public <T> T get(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super Buf, T> requestType) throws RocksDBException {
		return db.get(transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			long timeoutMs) throws RocksDBException {
		return db.existsMulti(transactionId, columnId, keys, timeoutMs);
	}

	@Override
	public CompletableFuture<List<Boolean>> existsMultiAsync(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			long timeoutMs) throws RocksDBException {
		return db.existsMultiAsyncInternal(transactionId, columnId, keys, timeoutMs);
	}

	@Override
	public long openIterator(long transactionId,
			long columnId,
			@NotNull Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return db.openIterator(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs);
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		db.closeIterator(iteratorId);
	}

	@Override
	public void seekTo(long iterationId, Keys keys) throws RocksDBException {
		db.seekTo(iterationId, keys);
	}

	@Override
	public <T> T subsequent(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
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
		var iteratorOperation = acquireAsyncIteratorOperation(iterationId);
		if (iteratorOperation == null) {
			return CompletableFuture.failedFuture(RocksDBException.of(
					RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					"Concurrent operation on iterator " + iterationId + " is not supported"));
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
				operation = scheduleIteratorStep(iterationId, 0, requestType, cancelled);
			} else {
				var skipped = advanceIteratorAsync(iterationId, skipCount, cancelled);
				operation = switch (requestType) {
					case RequestType.RequestNothing<?> _ -> (CompletableFuture<T>) (CompletableFuture<?>) skipped
							.thenCompose(exhausted -> exhausted
									? CompletableFuture.completedFuture(null)
									: advanceIteratorAsync(iterationId, takeCount, cancelled).thenApply(_ -> null));
					case RequestType.RequestExists<?> _ -> (CompletableFuture<T>) (CompletableFuture<?>) skipped
							.thenCompose(exhausted -> exhausted
									? CompletableFuture.completedFuture(false)
									: subsequentExistsAsync(iterationId, takeCount, false, cancelled));
					case RequestType.RequestMulti<?> _ -> (CompletableFuture<T>) (CompletableFuture<?>) skipped
							.thenCompose(exhausted -> exhausted
									? CompletableFuture.completedFuture(List.of())
									: subsequentMultiAsync(
											iterationId, takeCount, new ArrayList<>(), cancelled));
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

	@Override
	public CompletableFuture<Void> seekToAsync(long iterationId, @NotNull Keys keys) throws RocksDBException {
		var iteratorOperation = acquireAsyncIteratorOperation(iterationId);
		if (iteratorOperation == null) {
			return CompletableFuture.failedFuture(concurrentIteratorOperation(iterationId));
		}
		CompletableFuture<Void> result;
		try {
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
			}, db.getScheduler().readExecutor());
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
		if (closingAsyncIterators.containsKey(iteratorId)) {
			return null;
		}
		var operation = new AsyncIteratorOperation();
		if (asyncIteratorOperations.putIfAbsent(iteratorId, operation) != null) {
			return null;
		}
		if (closingAsyncIterators.containsKey(iteratorId)) {
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
	}

	/** @return true when the iterator was exhausted before consuming the requested count. */
	private CompletableFuture<Boolean> advanceIteratorAsync(long iterationId,
			long remaining,
			AtomicBoolean cancelled) {
		if (remaining <= 0) {
			return CompletableFuture.completedFuture(false);
		}
		long step = Math.min(remaining, ITERATOR_READ_STEP_SIZE);
		return scheduleIteratorAdvanceStep(iterationId, step, cancelled)
				.thenCompose(advanced -> advanced < step
						? CompletableFuture.completedFuture(true)
						: advanceIteratorAsync(iterationId, remaining - step, cancelled));
	}

	private CompletableFuture<Boolean> subsequentExistsAsync(long iterationId,
			long remaining,
			boolean found,
			AtomicBoolean cancelled) {
		if (remaining <= 0 || cancelled.get()) {
			return cancelled.get()
					? CompletableFuture.failedFuture(new CancellationException())
					: CompletableFuture.completedFuture(found);
		}
		long step = Math.min(remaining, ITERATOR_READ_STEP_SIZE);
		return scheduleIteratorAdvanceStep(iterationId, step, cancelled)
				.thenCompose(advanced -> {
					boolean pageFound = found || advanced > 0L;
					return advanced < step
							? CompletableFuture.completedFuture(pageFound)
							: subsequentExistsAsync(iterationId, remaining - step, pageFound, cancelled);
				});
	}

	private CompletableFuture<List<Buf>> subsequentMultiAsync(long iterationId,
			long remaining,
			ArrayList<Buf> values,
			AtomicBoolean cancelled) {
		if (remaining <= 0 || cancelled.get()) {
			return cancelled.get()
					? CompletableFuture.failedFuture(new CancellationException())
					: CompletableFuture.completedFuture(values);
		}
		long step = Math.min(remaining, ITERATOR_READ_STEP_SIZE);
		return scheduleIteratorStep(iterationId, step, RequestType.multi(), cancelled)
				.thenCompose(page -> {
					values.addAll(page);
					return page.size() < step
							? CompletableFuture.completedFuture(values)
							: subsequentMultiAsync(iterationId, remaining - step, values, cancelled);
				});
	}

	private <T> CompletableFuture<T> scheduleIteratorStep(long iterationId,
			long takeCount,
			RequestType.RequestIterate<? super Buf, T> requestType,
			AtomicBoolean cancelled) {
		if (cancelled.get()) {
			return CompletableFuture.failedFuture(new CancellationException());
		}
		return CompletableFuture.supplyAsync(() -> {
			if (cancelled.get()) {
				throw new CancellationException();
			}
			return db.subsequent(iterationId, 0, takeCount, requestType);
		}, db.getScheduler().readExecutor());
	}

	private CompletableFuture<Long> scheduleIteratorAdvanceStep(long iterationId,
			long takeCount,
			AtomicBoolean cancelled) {
		if (cancelled.get()) {
			return CompletableFuture.failedFuture(new CancellationException());
		}
		return CompletableFuture.supplyAsync(() -> {
			if (cancelled.get()) {
				throw new CancellationException();
			}
			return db.advanceIteratorInternal(iterationId, takeCount);
		}, db.getScheduler().readExecutor());
	}

	@Override
	public <T> T reduceRange(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.@NotNull RequestReduceRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
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
		if (requestType instanceof RequestType.RequestEntriesCount<?>) {
			Consumer<Throwable> lateFailureHandler = error -> logLateRangeCountFailure(
					transactionId, columnId, reverse, timeoutMs, error);
			return (CompletableFuture<T>) (CompletableFuture<?>) db.countRangeAsyncInternal(
					transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					timeoutMs)
					.contextWrite(context -> context.put(
							REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY, lateFailureHandler))
					.toFuture();
		}
		long queuedAtNanos = System.nanoTime();
		var command = new RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<>(
				transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs);
		return supplyAsyncPreservingRunningCompletion(() -> db.reduceRange(
				transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				remainingReadTimeoutMillis(timeoutMs, queuedAtNanos)), commandExecutor(command));
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
		var future = new RunningCompletionFuture<>(
				supplier,
				executor,
				db.getScheduler()::removeQueuedTask);
		try {
			executor.execute(future);
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
		return db.getRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
	}

	@Override
	public <T> Publisher<T> getRangeAsync(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.RequestGetRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		return db.getRangeAsyncInternal(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
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
    public Mono<CdcBatch> cdcPollBatchAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) {
        try {
            return db.cdcPollBatchAsyncInternal(id, fromSeq, maxEvents);
        } catch (Throwable e) {
            return Mono.error(e);
        }
    }
}
