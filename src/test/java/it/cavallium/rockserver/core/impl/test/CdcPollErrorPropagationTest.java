package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.CdcGapDetectedException;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Status;
import org.rocksdb.WriteBatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcPollErrorPropagationTest {

	@TempDir
	Path tempDir;

	@Test
	void writeBatchParserFailureFailsThePollInsteadOfReturningAPartialBatch() throws Exception {
		try (var db = new ParserFailingEmbeddedDB(tempDir, "cdc-parser-failure")) {
			long columnId = db.createColumn("data", ColumnSchema.of(
					IntArrayList.of(Integer.BYTES),
					new ObjectArrayList<>(),
					true,
					null,
					null,
					null));
			long startSeq = db.cdcCreate("sub", null, List.of(columnId), false);
			db.put(0, columnId,
					new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})}),
					Buf.wrap("first".getBytes(StandardCharsets.UTF_8)),
					RequestType.none());
			db.put(0, columnId,
					new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 2})}),
					Buf.wrap("second".getBytes(StandardCharsets.UTF_8)),
					RequestType.none());

			RocksDBException error = assertThrows(RocksDBException.class,
					() -> db.cdcPollBatchAsyncInternal("sub", startSeq, 100).block());

			assertEquals(RocksDBErrorType.INTERNAL_ERROR, error.getErrorUniqueId());
			assertTrue(error.getMessage().contains("Failed to parse WriteBatch at seq"));
			assertNotNull(db.failedHandler);
			assertFalse(db.failedHandler.isOwningHandle(),
					"The native WriteBatch.Handler must be closed when parsing fails");
		}
	}

	@Test
	void nativeCallbackIsReusedAcrossWalBatchesAndClosedAfterPoll() throws Exception {
		try (var db = new HandlerTrackingEmbeddedDB(tempDir, "cdc-handler-lifecycle")) {
			long columnId = db.createColumn("data", ColumnSchema.of(
					IntArrayList.of(Integer.BYTES),
					new ObjectArrayList<>(),
					true,
					null,
					null,
					null));
			long startSeq = db.cdcCreate("sub", null, List.of(columnId), false);
			db.put(0, columnId,
					new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})}),
					Buf.wrap("first".getBytes(StandardCharsets.UTF_8)),
					RequestType.none());
			db.put(0, columnId,
					new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 2})}),
					Buf.wrap("second".getBytes(StandardCharsets.UTF_8)),
					RequestType.none());

			var batch = db.cdcPollBatchAsyncInternal("sub", startSeq, 100).block();

			assertNotNull(batch);
			assertEquals(2, batch.events().size());
			assertTrue(db.iteratedBatches >= 2);
			assertTrue(db.reusedOneHandler,
					"A poll should allocate one native callback, not one callback per WAL batch");
			assertNotNull(db.firstHandler);
			assertFalse(db.firstHandler.isOwningHandle(),
					"The poll-owned native callback must be closed after iteration");
		}
	}

	@Test
	void fullyConsumedBatchAdvancesToTheNextWalSequence() throws Exception {
		try (var db = new HandlerTrackingEmbeddedDB(tempDir, "cdc-canonical-next-sequence")) {
			long columnId = db.createColumn("data", ColumnSchema.of(
					IntArrayList.of(Integer.BYTES),
					new ObjectArrayList<>(),
					true,
					null,
					null,
					null));
			long startSeq = db.cdcCreate("sub", null, List.of(columnId), false);
			long txId = db.openTransaction(10_000);
			for (int i = 0; i < 4; i++) {
				db.put(txId, columnId,
						new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, (byte) i})}),
						Buf.wrap(("value-" + i).getBytes(StandardCharsets.UTF_8)),
						RequestType.none());
			}
			db.closeTransaction(txId, true);

			var batch = db.cdcPollBatchAsyncInternal("sub", startSeq, 4).block();

			assertNotNull(batch);
			assertEquals(4, batch.events().size());
			long batchWalSequence = batch.events().getFirst().seq() >>> 20;
			for (var event : batch.events()) {
				assertEquals(batchWalSequence, event.seq() >>> 20,
						"The transaction must be represented by one WAL batch");
			}
			assertEquals((batchWalSequence + 4) << 20, batch.nextSeq(),
					"A fully consumed batch should not be retained as the next poll's dependency");

			int iteratedBatches = db.iteratedBatches;
			var emptyTail = db.cdcPollBatchAsyncInternal("sub", batch.nextSeq(), 100).block();
			assertNotNull(emptyTail);
			assertTrue(emptyTail.events().isEmpty());
			assertEquals(iteratedBatches, db.iteratedBatches,
					"An idle tail poll must not reopen and parse the fully-consumed batch");
		}
	}

	@Test
	void earliestWalProbeClosesTheFetchedWriteBatch() throws Exception {
		try (var db = new BatchTrackingEmbeddedDB(tempDir, "cdc-earliest-batch-close")) {
			long columnId = db.createColumn("data", ColumnSchema.of(
					IntArrayList.of(Integer.BYTES),
					new ObjectArrayList<>(),
					true,
					null,
					null,
					null));
			db.put(0, columnId,
					new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})}),
					Buf.wrap("value".getBytes(StandardCharsets.UTF_8)),
					RequestType.none());

			db.cdcCreate("probe", 0L, List.of(columnId), false);

			assertTrue(db.earliestBatchClosed,
					"The BatchResult owns a native WriteBatch that must be closed independently of its iterator");
		}
	}

	@Test
	void nativeWalContinuityLossStatusesBecomeCdcGapErrors() {
		var statuses = List.of(
				new Status(Status.Code.NotFound, Status.SubCode.None, "Gap in sequence numbers"),
				new Status(Status.Code.Corruption, Status.SubCode.None,
						"Gap in sequence number. Could not seek to required sequence number"),
				new Status(Status.Code.Corruption, Status.SubCode.None,
						"Start sequence was not found, skipping to the next available"));

		for (var status : statuses) {
			var nativeError = new org.rocksdb.RocksDBException(status);
			var mapped = assertThrows(CdcGapDetectedException.class,
					() -> StatusClassifyingEmbeddedDB.classify(nativeError));
			assertEquals(RocksDBErrorType.CDC_GAP_DETECTED, mapped.getErrorUniqueId());
			assertSame(nativeError, mapped.getCause());
		}
	}

	@Test
	void concurrentTailRefreshRequiresAFreshIteratorForPrefixlessProbes() throws Exception {
		assertTrue(StatusClassifyingEmbeddedDB.classify(new org.rocksdb.RocksDBException(
				new Status(Status.Code.TryAgain, Status.SubCode.None,
						"Create a new iterator to fetch the new tail."))));

		var unrelatedStatuses = List.of(
				new Status(Status.Code.IOError, Status.SubCode.None, "disk read failed"),
				new Status(Status.Code.Corruption, Status.SubCode.None, "checksum mismatch"),
				new Status(Status.Code.TryAgain, Status.SubCode.None, "unrelated retry"));
		for (var status : unrelatedStatuses) {
			var nativeError = new org.rocksdb.RocksDBException(status);
			assertSame(nativeError, assertThrows(org.rocksdb.RocksDBException.class,
					() -> StatusClassifyingEmbeddedDB.classify(nativeError)));
		}
	}

	@Test
	void prefixlessProbeReflushesAndReopensAfterTailRefresh() throws Exception {
		try (var db = new TailRefreshingEmbeddedDB(tempDir, "cdc-prefixless-refresh", 2, 37)) {
			long startSeq = db.cdcCreate("probe", 0L, List.of(), false);

			assertEquals(37L << 20, startSeq);
			assertEquals(3, db.flushes);
			assertEquals(3, db.probes);
		}
	}

	@Test
	void prefixlessProbeStopsAfterItsRetryBudget() throws Exception {
		try (var db = new TailRefreshingEmbeddedDB(tempDir, "cdc-prefixless-cap", Integer.MAX_VALUE, 37)) {
			var error = assertTimeoutPreemptively(Duration.ofSeconds(2),
					() -> assertThrows(RocksDBRetryException.class,
							() -> db.cdcCreate("probe", 0L, List.of(), false)));

			assertEquals(RocksDBErrorType.UPDATE_RETRY, error.getErrorUniqueId());
			assertEquals(3, db.flushes);
			assertEquals(3, db.probes);
		}
	}

	@Test
	void prefixlessEmptyFallbackRetriesWhenTheSequenceMoves() throws Exception {
		try (var db = new MovingEmptyEmbeddedDB(tempDir, "cdc-prefixless-moving-empty")) {
			long startSeq = db.cdcCreate("probe", 0L, List.of(), false);

			assertEquals(10L << 20, startSeq);
			assertEquals(3, db.flushes);
			assertEquals(3, db.probes);
		}
	}

	private static final class ParserFailingEmbeddedDB extends EmbeddedDB {
		private int parsedBatches;
		private WriteBatch.Handler failedHandler;

		private ParserFailingEmbeddedDB(Path path, String name) throws IOException {
			super(path, name, null);
		}

		@Override
		protected void iterateCdcWriteBatch(WriteBatch writeBatch, WriteBatch.Handler handler)
				throws org.rocksdb.RocksDBException {
			if (++parsedBatches == 3) {
				failedHandler = handler;
				throw new org.rocksdb.RocksDBException("synthetic malformed WriteBatch");
			}
			super.iterateCdcWriteBatch(writeBatch, handler);
		}
	}

	private static final class HandlerTrackingEmbeddedDB extends EmbeddedDB {
		private WriteBatch.Handler firstHandler;
		private int iteratedBatches;
		private boolean reusedOneHandler = true;

		private HandlerTrackingEmbeddedDB(Path path, String name) throws IOException {
			super(path, name, null);
		}

		@Override
		protected void iterateCdcWriteBatch(WriteBatch writeBatch, WriteBatch.Handler handler)
				throws org.rocksdb.RocksDBException {
			iteratedBatches++;
			if (firstHandler == null) {
				firstHandler = handler;
			} else {
				reusedOneHandler &= firstHandler == handler;
			}
			super.iterateCdcWriteBatch(writeBatch, handler);
		}
	}

	private static final class BatchTrackingEmbeddedDB extends EmbeddedDB {
		private boolean earliestBatchClosed;

		private BatchTrackingEmbeddedDB(Path path, String name) throws IOException {
			super(path, name, null);
		}

		@Override
		protected long readCdcBatchSequenceAndClose(
				org.rocksdb.TransactionLogIterator.BatchResult batch) {
			var writeBatch = batch.writeBatch();
			try {
				return super.readCdcBatchSequenceAndClose(batch);
			} finally {
				earliestBatchClosed = !writeBatch.isOwningHandle();
			}
		}
	}

	private static final class TailRefreshingEmbeddedDB extends EmbeddedDB {
		private int refreshesRemaining;
		private final long earliestSequence;
		private int flushes;
		private int probes;

		private TailRefreshingEmbeddedDB(Path path, String name, int refreshesRemaining, long earliestSequence)
				throws IOException {
			super(path, name, null);
			this.refreshesRemaining = refreshesRemaining;
			this.earliestSequence = earliestSequence;
		}

		@Override
		protected void flushCdcWalForPrefixlessProbe() {
			flushes++;
		}

		@Override
		protected long getLatestCdcWalSequence() {
			return 0;
		}

		@Override
		protected OptionalLong probeEarliestAvailableWalSeq() throws org.rocksdb.RocksDBException {
			probes++;
			if (refreshesRemaining-- > 0) {
				throw tailRefreshError();
			}
			return OptionalLong.of(earliestSequence);
		}
	}

	private static final class MovingEmptyEmbeddedDB extends EmbeddedDB {
		private final long[] latestReads = {7, 8, 8, 9, 9, 9};
		private int latestReadIndex;
		private int flushes;
		private int probes;

		private MovingEmptyEmbeddedDB(Path path, String name) throws IOException {
			super(path, name, null);
		}

		@Override
		protected void flushCdcWalForPrefixlessProbe() {
			flushes++;
		}

		@Override
		protected long getLatestCdcWalSequence() {
			return latestReads[latestReadIndex++];
		}

		@Override
		protected OptionalLong probeEarliestAvailableWalSeq() {
			probes++;
			return OptionalLong.empty();
		}
	}

	private static org.rocksdb.RocksDBException tailRefreshError() {
		return new org.rocksdb.RocksDBException(new Status(Status.Code.TryAgain, Status.SubCode.None,
				"Create a new iterator to fetch the new tail."));
	}

	private abstract static class StatusClassifyingEmbeddedDB extends EmbeddedDB {

		private StatusClassifyingEmbeddedDB(Path path, String name) throws IOException {
			super(path, name, null);
		}

		private static boolean classify(org.rocksdb.RocksDBException error)
				throws org.rocksdb.RocksDBException {
			return handleCdcIteratorStatus(error);
		}
	}
}
