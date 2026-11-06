package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

/**
 * Streamed batch writes of {@link EmbeddedDB}: putBatch/mergeBatch in all write modes, including SST ingestion.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBBatchWrites {

	private final EmbeddedDB edb;

	EmbeddedDBBatchWrites(EmbeddedDB edb) {
		this.edb = edb;
	}

	public CompletableFuture<Void> putBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode) throws RocksDBException {
		return putBatchInternal(columnId, batchPublisher, mode,
				edb.getSchedulerForColumn(columnId).scheduler(WorkloadProfile.INGEST, OperationFamily.MUTATION, Long.MAX_VALUE));
	}
	public CompletableFuture<Void> putBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull PutBatchMode mode,
			@NotNull Scheduler workloadScheduler) throws RocksDBException {
		var start = System.nanoTime();
		edb.actionLogger.logAction("PutBatch (begin)",
				start,
				columnId,
				"multiple (async)",
				"multiple (async)",
				null,
				null,
				null,
				mode
		);

		// A queued task dropped by the view, or its deadline, ends the batch with an error instead of a stall.
		// The deadline ends a running batch only between two client batches, and no longer once the client has
		// ended its stream. Once the final write has started, neither the deadline nor a dropped task ends it,
		// and a failure that comes once every batch was applied is not reported.
		return Mono.defer(() -> {
					var progress = new StreamedBatchProgress();
					return RWScheduler.<Void>subscribeOn(commit -> Mono.using(
									() -> new PutBatchState(columnId, mode, start, progress),
									state -> RWScheduler.publishBatchesOn(progress.track(Flux.from(batchPublisher)),
													Function.identity(),
													workloadScheduler,
													Queues.SMALL_BUFFER_SIZE)
											.doOnNext(state::write)
											.then(Mono.fromRunnable(() -> commit.run(state::writePending))),
									BatchWriteState::close,
									true
							), workloadScheduler)
							.onErrorResume(failure -> progress.applied() ? Mono.empty() : Mono.error(failure));
				})
				.onErrorMap(EmbeddedDBBatchWrites::mapBatchWriteFailure)
				.doFinally(ignored -> edb.putBatchTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS))
				.toFuture();
	}
	public CompletableFuture<Void> mergeBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode) throws RocksDBException {
		return mergeBatchInternal(columnId, batchPublisher, mode,
				edb.getSchedulerForColumn(columnId).scheduler(WorkloadProfile.INGEST, OperationFamily.MUTATION, Long.MAX_VALUE));
	}
	public CompletableFuture<Void> mergeBatchInternal(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode,
			@NotNull Scheduler workloadScheduler) throws RocksDBException {
		final boolean ingestBehindEnabled;
		try {
			ingestBehindEnabled = edb.config.global().ingestBehind();
		} catch (GestaltException e) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, e);
		}
		var start = System.nanoTime();
		edb.actionLogger.logAction("MergeBatch (begin)",
				start,
				columnId,
				"multiple (async)",
				"multiple (async)",
				null,
				null,
				null,
				mode
		);

		// A queued task dropped by the view, or its deadline, ends the batch with an error instead of a stall.
		// The deadline ends a running batch only between two batches, and no longer once the client has ended its
		// stream. Once the final write has started, neither the deadline nor a dropped task ends it, and a
		// failure that comes once every batch was applied is not reported.
		return Mono.defer(() -> {
					var progress = new StreamedBatchProgress();
					return RWScheduler.<Void>subscribeOn(commit -> Mono.using(
									() -> new MergeBatchState(columnId, mode, ingestBehindEnabled, start, progress),
									// The batcher flushes partial batches from a timer of another scheduler, so the
									// writes move onto the workload scheduler after batching, one batch at a time
									state -> RWScheduler.publishBatchesOn(progress.track(Flux.from(batchPublisher)),
													batches -> AdaptiveBatcher.buffer(batches, 128, 4096, Duration.ofMillis(10)),
													workloadScheduler,
													1)
											.doOnNext(state::write)
											.then(Mono.fromRunnable(() -> commit.run(state::writePending))),
									BatchWriteState::close,
									true
							), workloadScheduler)
							.onErrorResume(failure -> progress.applied() ? Mono.empty() : Mono.error(failure));
				})
				.onErrorMap(EmbeddedDBBatchWrites::mapBatchWriteFailure)
				.toFuture();
	}

	private static Throwable mapBatchWriteFailure(Throwable failure) {
		var current = failure;
		for (int depth = 0; current != null && depth < 32; depth++) {
			if (current instanceof RocksDBException rocksError) {
				return rocksError;
			}
			if (current.getCause() == current) {
				break;
			}
			current = current.getCause();
		}
		return RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, failure);
	}

	/**
	 * Tells whether a streamed putBatch or mergeBatch was applied in full: the client ended its stream, every
	 * batch it sent was written, and the final write has nothing left to apply. Every batch can be applied before
	 * the end of the stream is delivered, when each write applies its batch (merge write batches of a column
	 * without buckets, write batches past the flush thresholds). A failure that comes after that, such as a
	 * dropped or refused drain that only carried the end of the stream, does not undo those writes, so the call
	 * reports success instead.
	 */
	private static final class StreamedBatchProgress {

		private final AtomicLong received = new AtomicLong();
		private final AtomicLong written = new AtomicLong();
		private volatile boolean clientCompleted;
		// Unknown until the first write
		private volatile boolean finalWritePending = true;

		private <T> Flux<T> track(Flux<T> clientStream) {
			return clientStream
					.doOnNext(_ -> received.incrementAndGet())
					.doOnComplete(() -> clientCompleted = true);
		}

		/** Records written client batches, after the write that applied them returned. */
		private void written(int batches, boolean finalWritePending) {
			// Published by the counter, which applied() reads first
			this.finalWritePending = finalWritePending;
			written.addAndGet(batches);
		}

		private boolean applied() {
			// The received count is final once the client completed
			return clientCompleted && received.get() == written.get() && !finalWritePending;
		}
	}

	private abstract class BatchWriteState implements AutoCloseable {

		private static final VarHandle LIFECYCLE =
				EmbeddedDB.varHandle(BatchWriteState.class, "lifecycle", int.class);
		private static final int ACTIVE = 1;
		private static final int CLOSE_REQUESTED = 1 << 1;
		private static final int CLOSED = 1 << 2;

		private volatile int lifecycle;
		private final StreamedBatchProgress progress;

		private BatchWriteState(StreamedBatchProgress progress) {
			this.progress = progress;
			edb.ops.beginOp();
		}

		protected final void runWhileOpen(Runnable operation) {
			if (!tryEnter()) {
				return;
			}
			try {
				operation.run();
			} finally {
				exit();
			}
		}

		private boolean tryEnter() {
			for (;;) {
				int state = lifecycle;
				if ((state & (CLOSE_REQUESTED | CLOSED)) != 0) {
					return false;
				}
				if (LIFECYCLE.compareAndSet(this, state, state | ACTIVE)) {
					return true;
				}
			}
		}

		private void exit() {
			for (;;) {
				int state = lifecycle;
				int next = state & ~ACTIVE;
				if (LIFECYCLE.compareAndSet(this, state, next)) {
					if ((next & CLOSE_REQUESTED) != 0) {
						closeNow();
					}
					return;
				}
			}
		}

		@Override
		public final void close() {
			for (;;) {
				int state = lifecycle;
				if ((state & CLOSED) != 0) {
					return;
				}
				int next = state | CLOSE_REQUESTED;
				if (LIFECYCLE.compareAndSet(this, state, next)) {
					if ((state & ACTIVE) == 0) {
						closeNow();
					}
					return;
				}
			}
		}

		private void closeNow() {
			for (;;) {
				int state = lifecycle;
				if ((state & CLOSED) != 0) {
					return;
				}
				if ((state & ACTIVE) != 0) {
					return;
				}
				if (LIFECYCLE.compareAndSet(this, state, state | CLOSED)) {
					break;
				}
			}

			try {
				closeResources();
			} catch (Throwable error) {
				edb.logger.error("Failed to close asynchronous batch-write resources", error);
			} finally {
				edb.ops.endOp();
			}
		}

		protected final void observeWrite() {
			var observer = edb.batchWriteObserver;
			if (observer != null) {
				observer.run();
			}
		}

		/** Records that a write of {@code batches} client batches returned. */
		protected final void written(int batches) {
			progress.written(batches, hasFinalWritePending());
		}

		/** Whether the final write still has something to apply. */
		protected abstract boolean hasFinalWritePending();

		protected final void observeFinalWrite() {
			var observer = edb.batchFinalWriteObserver;
			if (observer != null) {
				observer.run();
			}
		}

		protected abstract void closeResources();
	}

	private final class PutBatchState extends BatchWriteState {

		private final long columnId;
		private final PutBatchMode mode;
		private final long start;
		private ColumnInstance.ColumnUse columnUse;
		private ColumnInstance col;
		private DBWriter writer;
		private boolean sstEntriesWritten;

		private PutBatchState(long columnId, PutBatchMode mode, long start, StreamedBatchProgress progress) {
			super(progress);
			this.columnId = columnId;
			this.mode = mode;
			this.start = start;
			try {
				this.columnUse = edb.acquireColumnUse(columnId);
				this.col = columnUse.column();
				this.writer = switch (mode) {
					case WRITE_BATCH, WRITE_BATCH_NO_WAL ->
							new WB(edb.db.get(), new LeakSafeWriteBatch(), mode == PutBatchMode.WRITE_BATCH_NO_WAL);
					case SST_INGESTION, SST_INGEST_BEHIND ->
							getSSTWriter(columnId, null, false, mode == PutBatchMode.SST_INGEST_BEHIND);
				};
			} catch (Throwable error) {
				close();
				throw error;
			}
		}

		private void write(KVBatch batch) {
			runWhileOpen(() -> {
				observeWrite();
				validateBatch(batch);
				var keyIt = batch.keys().iterator();
				var valueIt = batch.values().iterator();
				while (keyIt.hasNext()) {
					var keys = keyIt.next();
					var value = valueIt.next();
					edb.actionLogger.logAction("PutBatch (next)", start, columnId, keys, value, null, null, null, mode);
					edb.writes.put(writer, col, 0, keys, value, RequestType.none());
					if (writer instanceof SSTWriter) {
						sstEntriesWritten = true;
					}
					flushFullWriteBatch(writer);
				}
				written(1);
			});
		}

		@Override
		protected boolean hasFinalWritePending() {
			return switch (writer) {
				case WB wb -> wb.wb().count() > 0;
				case SSTWriter _ -> sstEntriesWritten;
				default -> true;
			};
		}

		private void writePending() {
			runWhileOpen(() -> {
				observeFinalWrite();
				switch (writer) {
					case WB wb -> {
						if (wb.wb().count() > 0) {
							wb.writePending();
						}
					}
					case SSTWriter sst -> {
						if (sstEntriesWritten) {
							sst.writePending();
						}
					}
					case null -> { }
					default -> {
						writer.writePending();
					}
				}
			});
		}

		@Override
		protected void closeResources() {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (Exception error) {
				throw new RuntimeException(error);
			} finally {
				if (columnUse != null) {
					columnUse.close();
				}
			}
		}
	}

	private final class MergeBatchState extends BatchWriteState {

		private final long columnId;
		private final MergeBatchMode mode;
		private final long start;
		private ColumnInstance.ColumnUse columnUse;
		private ColumnInstance col;
		private DBWriter writer;
		private ArrayList<Map.Entry<Keys, Buf>> pendingSstEntries;

		private MergeBatchState(long columnId,
				MergeBatchMode mode,
				boolean ingestBehindEnabled,
				long start,
				StreamedBatchProgress progress) {
			super(progress);
			this.columnId = columnId;
			this.mode = mode;
			this.start = start;
			try {
				this.columnUse = edb.acquireColumnUse(columnId);
				this.col = columnUse.column();
				this.writer = switch (mode) {
					case MERGE_WRITE_BATCH, MERGE_WRITE_BATCH_NO_WAL -> {
						if (col.hasBuckets()) {
							yield edb.openTransactionInternal(120_000, false);
						}
						yield new WB(edb.db.get(), new LeakSafeWriteBatch(),
								mode == MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL);
					}
					case MERGE_SST_INGESTION -> {
						pendingSstEntries = new ArrayList<>();
						yield getSSTWriter(columnId, null, false, false);
					}
					case MERGE_SST_INGEST_BEHIND -> {
						if (!ingestBehindEnabled) {
							throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
									"MERGE_SST_INGEST_BEHIND requires database.global.ingest-behind=true");
						}
						pendingSstEntries = new ArrayList<>();
						yield getSSTWriter(columnId, null, false, true);
					}
				};
			} catch (Throwable error) {
				close();
				throw error;
			}
		}

		private void write(List<KVBatch> batches) {
			runWhileOpen(() -> {
				observeWrite();
				for (var batch : batches) {
					validateBatch(batch);
					var keyIt = batch.keys().iterator();
					var valueIt = batch.values().iterator();
					while (keyIt.hasNext()) {
						var keys = keyIt.next();
						var value = valueIt.next();
						edb.actionLogger.logAction("MergeBatch (next)", start, columnId, keys, value, null, null, null, mode);
						if (writer instanceof SSTWriter) {
							pendingSstEntries.add(Map.entry(keys, value));
						} else {
							edb.writes.merge(writer, col, 0L, keys, value, RequestType.none());
						}
						flushFullWriteBatch(writer);
					}
				}
				if (writer instanceof WB wb && wb.wb().count() > 0) {
					wb.flushAndReset();
				}
				written(batches.size());
			});
		}

		@Override
		protected boolean hasFinalWritePending() {
			return switch (writer) {
				case WB wb -> wb.wb().count() > 0;
				case SSTWriter _ -> !pendingSstEntries.isEmpty();
				// A transaction applies its writes only when committed
				default -> true;
			};
		}

		private void writePending() {
			runWhileOpen(() -> {
				observeFinalWrite();
				switch (writer) {
					case WB wb -> {
						if (wb.wb().count() > 0) {
							wb.writePending();
						}
					}
					case Tx tx -> {
						if (!edb.closeTransactionInternal(tx, true)) {
							throw new RocksDBRetryException();
						}
					}
					case SSTWriter sst -> {
						if (writeSstEntries(col, sst, pendingSstEntries)) {
							sst.writePending();
						}
					}
					case null -> { }
				}
			});
		}

		@Override
		protected void closeResources() {
			try {
				if (writer instanceof Tx tx) {
					if (tx.val().isOwningHandle()) {
						edb.closeTransactionInternal(tx, false);
					}
				} else if (writer != null) {
					try {
						writer.close();
					} catch (Exception error) {
						throw new RuntimeException(error);
					}
				}
			} finally {
				if (columnUse != null) {
					columnUse.close();
				}
			}
		}
	}

	private static void validateBatch(KVBatch batch) {
		int keyCount = batch.keys().size();
		int valueCount = batch.values().size();
		if (keyCount != valueCount) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Batch key/value count mismatch: " + keyCount + " keys, " + valueCount + " values");
		}
	}

	private void flushFullWriteBatch(DBWriter writer) {
		if (writer instanceof WB wb
				&& (wb.wb().count() >= 10_000 || wb.wb().getDataSize() >= 4 * 1024 * 1024)) {
			wb.flushAndReset();
		}
	}

	private boolean writeSstEntries(ColumnInstance col,
			SSTWriter sst,
			@Nullable List<Map.Entry<Keys, Buf>> entries) throws RocksDBException {
		if (entries == null || entries.isEmpty()) {
			return false;
		}
		boolean wroteEntry = false;
		entries.sort((a, b) -> {
			var ka = col.calculateKeyBytes(a.getKey().keys());
			var kb = col.calculateKeyBytes(b.getKey().keys());
			return Arrays.compareUnsigned(ka, kb);
		});
		try (var ro = edb.newPointReadOptions(null)) {
			for (var entry : entries) {
				var keys = entry.getKey();
				var value = entry.getValue();
				byte[] keyBytes = col.calculateKeyBytes(keys.keys());
				Buf calculatedKey = Buf.wrap(keyBytes);
				if (col.hasBuckets()) {
					var existingRawBucket = edb.dbGet(null, col, ro, calculatedKey);
					var bucket = existingRawBucket != null ? new Bucket(col, existingRawBucket) : new Bucket(col);
					var bucketElementKeys = col.getBucketElementKeys(keys.keys());
					var existing = bucket.getElement(bucketElementKeys);
					Buf mergedValue;
					if (existing != null) {
						var mergedRes = col.mergeOperator().merge(calculatedKey, existing, List.of(value));
						mergedValue = mergedRes != null ? mergedRes : existing;
					} else {
						mergedValue = value;
					}
					bucket.addElement(bucketElementKeys, mergedValue);
					byte[] valBytes = EmbeddedDBWriteElision.jniArray(bucket.toSegment());
					sst.put(keyBytes, valBytes);
					wroteEntry = true;
				} else {
					Buf existing = edb.dbGet(null, col, ro, calculatedKey);
					var mergeOp = col.mergeOperator();
					if (mergeOp == null) {
						throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "MergeBatch SST requires a merge operator");
					}
					Buf merged = existing != null ? mergeOp.merge(calculatedKey, existing, List.of(value)) : value;
					if (merged != null) {
						byte[] valBytes = Utils.toByteArray(merged);
						sst.put(keyBytes, valBytes);
						wroteEntry = true;
					}
				}
			}
		} catch (org.rocksdb.RocksDBException e) {
			throw RocksDBException.of(RocksDBErrorType.SST_WRITE_3, e);
		}
		return wroteEntry;
	}
	@VisibleForTesting
	public SSTWriter getSSTWriter(long colId,
			@Nullable GlobalDatabaseConfig globalDatabaseConfigOverride,
			boolean forceNoOptions,
			boolean ingestBehind) throws RocksDBException {
		RocksDBLoader.ColumnOptionsWithMerge columnConifg = null;
		RocksDBObjects refs = null;
		try {
			var col = edb.getColumn(colId);
			if (!forceNoOptions) {
				var name = new String(col.cfh().getName(), StandardCharsets.UTF_8);
				refs = new RocksDBObjects();
				if (globalDatabaseConfigOverride != null) {
					columnConifg = RocksDBLoader.getColumnOptions(name,
							edb.path,
							edb.definitiveDbPath,
							globalDatabaseConfigOverride,
							edb.logger,
							refs,
							false,
							edb.caches, edb.metadataCaches
					);
				} else {
					try {
						columnConifg = RocksDBLoader.getColumnOptions(name,
								edb.path,
								edb.definitiveDbPath,
								edb.config.global(),
								edb.logger,
								refs,
								false,
								edb.caches, edb.metadataCaches
						);
					} catch (GestaltException e) {
						throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, e);
					}
				}
			}
			if (Files.notExists(edb.tempSSTsPath)) {
				Files.createDirectories(edb.tempSSTsPath);
			}
			return SSTWriter.open(edb.tempSSTsPath,
					edb.db,
					col,
					columnConifg != null ? columnConifg.options() : null,
					forceNoOptions,
					ingestBehind,
					refs
			);
		} catch (IOException ex) {
			if (refs != null) {
				refs.close();
			}
			if (columnConifg != null) {
				columnConifg.options().close();
			}
			if (columnConifg != null && columnConifg.mergeOperator() != null) {
				columnConifg.mergeOperator().close();
			}
			throw RocksDBException.of(RocksDBErrorType.SST_WRITE_2, ex);
		} catch (org.rocksdb.RocksDBException ex) {
			if (refs != null) {
				refs.close();
			}
			if (columnConifg != null) {
				columnConifg.options().close();
			}
			if (columnConifg != null && columnConifg.mergeOperator() != null) {
				columnConifg.mergeOperator().close();
			}
			throw RocksDBException.of(RocksDBErrorType.SST_WRITE_3, ex);
		} catch (Throwable ex) {
			if (refs != null) {
				refs.close();
			}
			if (columnConifg != null) {
				columnConifg.options().close();
			}
			if (columnConifg != null && columnConifg.mergeOperator() != null) {
				columnConifg.mergeOperator().close();
			}
			throw ex;
		}
	}
	public void putBatch(long columnId, @NotNull Publisher<@NotNull KVBatch> batchPublisher, @NotNull PutBatchMode mode)
			throws RocksDBException {
		try {
			putBatchInternal(columnId, batchPublisher, mode).get();
		} catch (RocksDBException ex) {
			throw ex;
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} catch (Exception ex) {
			throw (RocksDBException) mapBatchWriteFailure(ex);
		}
	}
	public void mergeBatch(long columnId,
			@NotNull Publisher<@NotNull KVBatch> batchPublisher,
			@NotNull MergeBatchMode mode) throws RocksDBException {
		try {
			mergeBatchInternal(columnId, batchPublisher, mode).get();
		} catch (RocksDBException ex) {
			throw ex;
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} catch (Exception ex) {
			throw (RocksDBException) mapBatchWriteFailure(ex);
		}
	}
}
