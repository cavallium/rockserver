package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Compact;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Flush;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.GetAllColumnDefinitions;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CheckMergeOperator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Delete;
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
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream.ScanRaw;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CDCEventAck;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.common.cdc.CdcCommitMode;
import it.cavallium.rockserver.core.common.cdc.CdcStreamOptions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

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

	default CompletableFuture<Long> checkMergeOperatorAsync(String name, byte[] hash) {
		return requestAsync(new CheckMergeOperator(name, hash));
	}

	default CompletableFuture<Long> ensureMergeOperatorAsync(String name, String className, byte[] jarData) {
		byte[] hash;
		try {
			java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
			hash = digest.digest(jarData);
		} catch (java.security.NoSuchAlgorithmException e) {
			return CompletableFuture.failedFuture(e);
		}
		return checkMergeOperatorAsync(name, hash).thenCompose(existing -> {
			if (existing != null) {
				return CompletableFuture.completedFuture(existing);
			} else {
				return uploadMergeOperatorAsync(name, className, jarData);
			}
		});
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

	/** See: {@link Delete}. */
	default <T> CompletableFuture<T> deleteAsync(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		return requestAsync(new Delete<>(transactionOrUpdateId, columnId, keys, requestType));
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

	/** See: {@link ScanRaw}. */
	default Publisher<SerializedKVBatch> scanRawAsync(long columnId, int shardIndex, int shardCount) {
		return requestAsync(new ScanRaw(columnId, shardIndex, shardCount));
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
        return requestAsync(new RocksDBAPICommand.CdcCreate(id, fromSeq, columnIds, null));
    }

    default CompletableFuture<Long> cdcCreateAsync(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds, @Nullable Boolean resolvedValues) throws RocksDBException {
        return requestAsync(new RocksDBAPICommand.CdcCreate(id, fromSeq, columnIds, resolvedValues));
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
     * Change Data Capture streaming – primary entrypoint.
     *
     * Semantics and safety:
     * - Source of truth is RocksDB WAL; sequences are strictly increasing and durable across restarts.
     * - Backpressure-aware: events are fetched in bounded batches and only when downstream requests.
     * - Crash/restart behavior depends on the chosen {@link CdcCommitMode}:
     *   - {@link it.cavallium.rockserver.core.common.cdc.CdcCommitMode#PER_EVENT PER_EVENT}: each event is
     *     processed via {@code processor}, then the offset is committed, and only after a successful commit the
     *     event is emitted to downstream. If the client crashes, upon resume the next event will be the first
     *     uncommitted one – i.e., already processed events will not be re-delivered. This is the safest mode if you
     *     want to avoid duplicates on unexpected crashes, at the cost of more frequent commits.
     *   - {@link it.cavallium.rockserver.core.common.cdc.CdcCommitMode#BATCH BATCH}: the whole polled batch is
     *     processed first; then a single commit of the last event’s sequence happens; finally the events are emitted.
     *     If a crash happens mid-batch before the commit, some events in the tail of that batch will be replayed on
     *     resume. Use when higher throughput with occasional replay is acceptable.
     *   - {@link it.cavallium.rockserver.core.common.cdc.CdcCommitMode#NONE NONE}: no auto-commit; the stream only
     *     emits polled events and advances its local cursor. The caller must explicitly commit using
     *     {@link #cdcCommitAsync(String, long)} to make progress durable on the server.
     *
     * Usage:
     * - Provide a {@code processor} to process each event in-order. The stream will ensure correct commit sequencing
     *   based on the selected {@code commitMode}. If {@code processor} is null or the mode is {@code NONE}, events are
     *   just emitted without auto-commit.
     * - To get “no duplicates on restart”, prefer {@code PER_EVENT}. To minimize commit overhead, use {@code BATCH} and
     *   accept possible replay of the last unfinished batch on crash.
     */
    default Flux<CDCEvent> cdcStream(@NotNull String id,
                                     @NotNull CdcStreamOptions options,
                                     @Nullable java.util.function.Function<CDCEvent, Mono<Void>> processor) {
        return Flux.defer(() -> {
            Long initialSeq = options.fromSeq();
            long batchSize = options.batchSize() > 0 ? options.batchSize() : CdcStreamOptions.DEFAULT_BATCH_SIZE;
            Duration idle = options.idleDelay() == null || options.idleDelay().isZero()
                    ? CdcStreamOptions.DEFAULT_IDLE_DELAY
                    : options.idleDelay();
            CdcCommitMode commitMode = options.commitMode() == null ? CdcStreamOptions.DEFAULT_COMMIT_MODE : options.commitMode();

            AtomicReference<Long> cursor = new AtomicReference<>(initialSeq);
            Retry retrySpec = defaultCdcStreamRetry();

            return Flux.generate(sink -> sink.next(cursor))
                .concatMap(state -> {
                    @SuppressWarnings("unchecked")
                    AtomicReference<Long> c = (AtomicReference<Long>) state;
                    Long seq = c.get();

                    Mono<CdcBatch> fetch = Mono.defer(() -> cdcPollBatchAsync(id, seq, batchSize))
                            .retryWhen(retrySpec);

                    Function<Long, Mono<Void>> commitFn = targetSeq -> Mono.defer(() -> {
                                if (commitMode == CdcCommitMode.NONE) return Mono.empty();
                                try {
                                    return Mono.fromFuture(cdcCommitAsync(id, targetSeq));
                                } catch (RocksDBException e) {
                                    return Mono.error(e);
                                }
                            })
                            .retryWhen(retrySpec)
                            .doOnSuccess(__ -> c.updateAndGet(prev -> prev == null ? (targetSeq + 1) : Math.max(prev, targetSeq + 1)));

                    return fetch.flatMapMany(batch -> {
                        List<CDCEvent> events = batch.events();
                        long nextSeq = batch.nextSeq();
                        c.set(nextSeq);

                        if (events.isEmpty()) {
                            if (seq == null || nextSeq > seq) return Mono.empty();
                            return Mono.delay(idle).then(Mono.empty());
                        }

                        if (processor == null || commitMode == CdcCommitMode.NONE) {
                            return Flux.fromIterable(events);
                        }

                        long lastSeq = events.get(events.size() - 1).seq();
                        if (commitMode == CdcCommitMode.PER_EVENT) {
                            return Flux.fromIterable(events)
                                    .concatMap(event -> processor.apply(event)
                                            .then(commitFn.apply(event.seq()))
                                            .thenReturn(event));
                        } else { // BATCH default
                            return Flux.fromIterable(events)
                                    .concatMap(processor::apply)
                                    .then(commitFn.apply(lastSeq))
                                    .thenMany(Flux.fromIterable(events));
                        }
                    });
                });
        });
    }

    /**
     * CDC streaming with per-event acknowledgments.
     *
     * This variant emits events wrapped with an {@link CDCEventAck} that exposes an {@code ack()} Mono. Calling
     * {@code ack()} commits the event's sequence on the server (with retries), enabling a simple at-least-once
     * processing style:
     *
     * api.cdcStreamAck("subId", options)
     *   .concatMap(ack -> process(ack.event()).then(ack.ack()).thenReturn(ack.event()))
     *   ...
     *
     * Notes and guarantees:
     * - Backpressure: identical to {@link #cdcStream}; events are fetched in bounded batches only when requested.
     * - Durability: ack() persists the offset via {@link #cdcCommitAsync(String, long)} with retry/backoff.
     * - Ordering: you should acknowledge events in the same order they are received. Committing a higher sequence
     *   while earlier events are not yet processed may cause those earlier ones to be skipped after a crash because
     *   the server keeps only the maximum committed sequence. Keep ack order to avoid data loss.
     * - Crash behavior: events for which ack() completed will not be re-delivered after restart (assuming resume
     *   from last committed). Unacked tail may be replayed – classic at-least-once.
     */
    default Flux<CDCEventAck> cdcStreamAck(@NotNull String id, @NotNull CdcStreamOptions options) {
        return Flux.defer(() -> {
            Long initialSeq = options.fromSeq();
            long batchSize = options.batchSize() > 0 ? options.batchSize() : CdcStreamOptions.DEFAULT_BATCH_SIZE;
            Duration idle = options.idleDelay() == null || options.idleDelay().isZero()
                    ? CdcStreamOptions.DEFAULT_IDLE_DELAY
                    : options.idleDelay();

            AtomicReference<Long> cursor = new AtomicReference<>(initialSeq);
            Retry retrySpec = defaultCdcStreamRetry();

            java.util.function.Function<Long, Mono<Void>> commitFn = targetSeq -> Mono.defer(() -> {
                        try {
                            return Mono.fromFuture(cdcCommitAsync(id, targetSeq));
                        } catch (RocksDBException e) {
                            return Mono.error(e);
                        }
                    })
                    .retryWhen(retrySpec)
                    .doOnSuccess(__ -> cursor.set(targetSeq + 1));

            return Flux.generate(sink -> sink.next(cursor))
                    .concatMap(state -> {
                        @SuppressWarnings("unchecked")
                        AtomicReference<Long> c = (AtomicReference<Long>) state;
                        Long seq = c.get();

                        Mono<CdcBatch> fetch = Mono.defer(() -> cdcPollBatchAsync(id, seq, batchSize))
                                .retryWhen(retrySpec);

                        return fetch.flatMapMany(batch -> {
                            List<CDCEvent> events = batch.events();
                            long nextSeq = batch.nextSeq();
                            c.set(nextSeq);

                            if (events.isEmpty()) {
                                if (seq == null || nextSeq > seq) return Mono.empty();
                                return Mono.delay(idle).then(Mono.empty());
                            }

                            return Flux.fromIterable(events)
                                    .map(ev -> new CDCEventAck(ev, () -> commitFn.apply(ev.seq())));
                        });
                    });
        });
    }

    default Mono<CdcBatch> cdcPollBatchAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) {
        return Flux.from(cdcPollAsync(id, fromSeq, maxEvents))
                .collectList()
                .map(events -> {
                    long nextSeq;
                    if (events.isEmpty()) {
                        nextSeq = fromSeq != null ? fromSeq : 0;
                    } else {
                        nextSeq = events.get(events.size() - 1).seq() + 1;
                    }
                    return new CdcBatch(events, nextSeq);
                });
    }

    /**
     * Default retry policy used by {@link #cdcStream} to make polling/commit resilient to transient failures.
     */
    private static Retry defaultCdcStreamRetry() {
        return Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(50))
                .doBeforeRetry(s -> System.err.println("CDC RETRY: " + s.failure()))
                .maxBackoff(Duration.ofSeconds(5))
                .transientErrors(true);
    }
}
