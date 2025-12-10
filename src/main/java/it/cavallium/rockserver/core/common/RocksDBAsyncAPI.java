package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Compact;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Flush;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.GetAllColumnDefinitions;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.GetColumnId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Merge;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.UploadMergeOperator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream.GetRange;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcCommitMode;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RocksDBAsyncAPI extends RocksDBAsyncAPIRequestHandler {

	/** See: {@link OpenTransaction}. */
	default CompletableFuture<Long> openTransactionAsync(long timeoutMs) throws RocksDBException {
		return requestAsync(new OpenTransaction(timeoutMs));
	}

	/** See: {@link CloseTransaction}. */
	default CompletableFuture<Boolean> closeTransactionAsync(long transactionId, boolean commit) throws RocksDBException {
		return requestAsync(new CloseTransaction(transactionId, commit));
	}

	/** See: {@link CloseFailedUpdate}. */
	default CompletableFuture<Void> closeFailedUpdateAsync(long updateId) throws RocksDBException {
		return requestAsync(new CloseFailedUpdate(updateId));
	}

	/** See: {@link CreateColumn}. */
	default CompletableFuture<Long> createColumnAsync(String name, @NotNull ColumnSchema schema) throws RocksDBException {
		return requestAsync(new CreateColumn(name, schema));
	}

	/** See: {@link UploadMergeOperator}. */
	default CompletableFuture<Long> uploadMergeOperatorAsync(String name, String className, byte[] jarData) throws RocksDBException {
		return requestAsync(new UploadMergeOperator(name, className, jarData));
	}

	/** See: {@link DeleteColumn}. */
	default CompletableFuture<Void> deleteColumnAsync(long columnId) throws RocksDBException {
		return requestAsync(new DeleteColumn(columnId));
	}

	/** See: {@link GetColumnId}. */
	default CompletableFuture<Long> getColumnIdAsync(@NotNull String name) throws RocksDBException {
		return requestAsync(new GetColumnId(name));
	}

	/** See: {@link Put}. */
	default <T> CompletableFuture<T> putAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Put<>(transactionOrUpdateId, columnId, keys, value, requestType));
	}

	/** See: {@link Merge}. */
	default <T> CompletableFuture<T> mergeAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Merge<>(transactionOrUpdateId, columnId, keys, value, requestType));
	}

	/** See: {@link PutMulti}. */
	default <T> CompletableFuture<List<T>> putMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new PutMulti<>(transactionOrUpdateId, columnId, keys, values, requestType));
	}

	/** See: {@link MergeMulti}. */
	default <T> CompletableFuture<List<T>> mergeMultiAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new MergeMulti<>(transactionOrUpdateId, columnId, keys, values, requestType));
	}

	/** See: {@link PutBatch}. */
	default CompletableFuture<Void> putBatchAsync(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode) throws RocksDBException {
		return requestAsync(new PutBatch(columnId, batchPublisher, mode));
	}

	/** See: {@link MergeBatch}. */
	default CompletableFuture<Void> mergeBatchAsync(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode) throws RocksDBException {
		return requestAsync(new MergeBatch(columnId, batchPublisher, mode));
	}

	/** See: {@link Get}. */
	default <T> CompletableFuture<T> getAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			RequestGet<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Get<>(transactionOrUpdateId, columnId, keys, requestType));
	}

	/** See: {@link OpenIterator}. */
	default CompletableFuture<Long> openIteratorAsync(long transactionId,
			long columnId,
			@NotNull Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return requestAsync(new OpenIterator(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				timeoutMs
		));
	}

	/** See: {@link CloseIterator}. */
	default CompletableFuture<Void> closeIteratorAsync(long iteratorId) throws RocksDBException {
		return requestAsync(new CloseIterator(iteratorId));
	}

	/** See: {@link SeekTo}. */
	default CompletableFuture<Void> seekToAsync(long iterationId, @NotNull Keys keys) throws RocksDBException {
		return requestAsync(new SeekTo(iterationId, keys));
	}

	/** See: {@link Subsequent}. */
	default <T> CompletableFuture<T> subsequentAsync(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Subsequent<>(iterationId, skipCount, takeCount, requestType));
	}

	/** See: {@link ReduceRange}. */
	default <T> CompletableFuture<T> reduceRangeAsync(long transactionId,
													  long columnId,
													  @Nullable Keys startKeysInclusive,
													  @Nullable Keys endKeysExclusive,
													  boolean reverse,
													  RequestType.RequestReduceRange<? super KV, T> requestType,
													  long timeoutMs) throws RocksDBException {
		return requestAsync(new ReduceRange<>(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs
		));
	}

	/** See: {@link GetRange}. */
	default <T> Publisher<T> getRangeAsync(long transactionId,
										   long columnId,
										   @Nullable Keys startKeysInclusive,
										   @Nullable Keys endKeysExclusive,
										   boolean reverse,
										   RequestType.RequestGetRange<? super KV, T> requestType,
										   long timeoutMs) throws RocksDBException {
		return requestAsync(new GetRange<>(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				timeoutMs
		));
	}

	/** See: {@link Flush}. */
	default CompletableFuture<Void> flushAsync() {
		return requestAsync(new Flush());
	}

	/** See: {@link Compact}. */
	default CompletableFuture<Void> compactAsync() {
		return requestAsync(new Compact());
	}

	/** See: {@link GetAllColumnDefinitions}. */
	default CompletableFuture<Map<String, ColumnSchema>> getAllColumnDefinitionsAsync() {
		return requestAsync(new GetAllColumnDefinitions());
	}

    // CDC API
    default CompletableFuture<Long> cdcCreateAsync(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds) throws RocksDBException {
        return requestAsync(new RocksDBAPICommand.CdcCreate(id, fromSeq, columnIds));
    }

    default CompletableFuture<Void> cdcDeleteAsync(@NotNull String id) throws RocksDBException {
        return requestAsync(new RocksDBAPICommand.CdcDelete(id));
    }

    default CompletableFuture<Void> cdcCommitAsync(@NotNull String id, long seq) throws RocksDBException {
        return requestAsync(new RocksDBAPICommand.CdcCommit(id, seq));
    }

    default Publisher<CDCEvent> cdcPollAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) throws RocksDBException {
        return requestAsync(new RocksDBAPICommand.RocksDBAPICommandStream.CdcPoll(id, fromSeq, maxEvents));
    }

    /**
     * Returns a continuous stream of CDC events.
     * <p>
     * This method automatically handles the polling loop, fetching batches of events
     * using {@link #cdcPollAsync(String, Long, long)}.
     * <p>
     * If the stream catches up (polling returns empty), it waits for the specified {@code pollInterval}
     * before polling again.
     *
     * @param id           CDC subscription ID
     * @param fromSeq      Start sequence. If null, starts from the server's default (usually last committed + 1).
     * @param batchSize    Max events per poll
     * @param pollInterval Wait time when no events are available. If null/zero, defaults to 100ms.
     * @return A Flux of CDC events
     */
    default Flux<CDCEvent> cdcStream(@NotNull String id, @Nullable Long fromSeq, long batchSize, @Nullable Duration pollInterval) {
        return Flux.defer(() -> {
            AtomicReference<Long> cursor = new AtomicReference<>(fromSeq);
            return Flux.generate(sink -> sink.next(cursor))
                    .concatMap(state -> {
                        @SuppressWarnings("unchecked")
                        AtomicReference<Long> c = (AtomicReference<Long>) state;
                        Long seq = c.get();
                        return Flux.from(cdcPollAsync(id, seq, batchSize))
                                .doOnNext(e -> c.set(e.seq() + 1))
                                .switchIfEmpty(
                                        (pollInterval != null && !pollInterval.isZero()
                                                ? Mono.delay(pollInterval)
                                                : Mono.delay(Duration.ofMillis(100)))
                                                .then(Mono.empty())
                                );
                    });
        });
    }

    /**
     * Returns a continuous stream of CDC processing results with configurable commit mode.
     * <p>
     * This method automatically handles the polling loop, processing of events using the provided processor,
     * and committing the offset based on the {@code commitMode}.
     * <p>
     * Events within a batch are processed sequentially to maintain ordering.
     *
     * @param id           CDC subscription ID
     * @param fromSeq      Start sequence. If null, starts from the server's default.
     * @param batchSize    Max events per poll
     * @param pollInterval Wait time when no events are available. If null/zero, defaults to 100ms.
     * @param commitMode   Commit mode (BATCH or PER_EVENT).
     * @param processor    Function to process each event. Must return a Mono<Void> that completes when processing is done.
     * @return A Flux<Void> that runs indefinitely (until error or cancellation).
     */
    default Flux<Void> cdcStream(@NotNull String id, @Nullable Long fromSeq, long batchSize, @Nullable Duration pollInterval, @NotNull CdcCommitMode commitMode, @NotNull java.util.function.Function<CDCEvent, Mono<Void>> processor) {
        return Flux.defer(() -> {
            AtomicReference<Long> cursor = new AtomicReference<>(fromSeq);
            return Flux.generate(sink -> sink.next(cursor))
                    .concatMap(state -> {
                        @SuppressWarnings("unchecked")
                        AtomicReference<Long> c = (AtomicReference<Long>) state;
                        Long seq = c.get();
                        return Flux.from(cdcPollAsync(id, seq, batchSize))
                                .collectList()
                                .flatMap(events -> {
                                    if (events.isEmpty()) {
                                        return (pollInterval != null && !pollInterval.isZero()
                                                ? Mono.delay(pollInterval)
                                                : Mono.delay(Duration.ofMillis(100)))
                                                .then(Mono.empty());
                                    }
                                    long lastSeq = events.get(events.size() - 1).seq();
                                    c.set(lastSeq + 1);

                                    return Flux.fromIterable(events)
                                            .concatMap(event -> {
                                                Mono<Void> p = processor.apply(event);
                                                if (commitMode == CdcCommitMode.PER_EVENT) {
                                                    return p.then(Mono.defer(() -> {
                                                        try {
                                                            return Mono.fromFuture(cdcCommitAsync(id, event.seq()));
                                                        } catch (RocksDBException e) {
                                                            return Mono.error(e);
                                                        }
                                                    }));
                                                }
                                                return p;
                                            })
                                            .then(Mono.defer(() -> {
                                                if (commitMode == CdcCommitMode.BATCH) {
                                                    try {
                                                        return Mono.fromFuture(cdcCommitAsync(id, lastSeq));
                                                    } catch (RocksDBException e) {
                                                        return Mono.error(e);
                                                    }
                                                }
                                                return Mono.empty();
                                            }));
                                });
                    });
        });
    }

    /**
     * Returns a continuous stream of CDC processing results.
     * <p>
     * This method automatically handles the polling loop, processing of events using the provided processor,
     * and committing the offset after each batch is successfully processed ({@link CdcCommitMode#BATCH}).
     * <p>
     * Events within a batch are processed sequentially to maintain ordering.
     *
     * @param id           CDC subscription ID
     * @param fromSeq      Start sequence. If null, starts from the server's default.
     * @param batchSize    Max events per poll
     * @param pollInterval Wait time when no events are available. If null/zero, defaults to 100ms.
     * @param processor    Function to process each event. Must return a Mono<Void> that completes when processing is done.
     * @return A Flux<Void> that runs indefinitely (until error or cancellation).
     */
    default Flux<Void> cdcStream(@NotNull String id, @Nullable Long fromSeq, long batchSize, @Nullable Duration pollInterval, @NotNull java.util.function.Function<CDCEvent, Mono<Void>> processor) {
        return cdcStream(id, fromSeq, batchSize, pollInterval, CdcCommitMode.BATCH, processor);
    }
}
