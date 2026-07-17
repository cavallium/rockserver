package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.WriteBatchIterator;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteBatchIteratorTest {

	private static final int HEADER_SIZE = 12;

	static {
		RocksDB.loadLibrary();
	}

	@TempDir
	Path tempDir;

	@Test
	void matchesNativeIterationForEveryPublicWriteBatchRecord() throws Exception {
		byte[] longKey = "k".repeat(130).getBytes(StandardCharsets.UTF_8);
		byte[] longValue = "v".repeat(16_384).getBytes(StandardCharsets.UTF_8);

		try (Options options = new Options().setCreateIfMissing(true);
				RocksDB db = RocksDB.open(options, tempDir.toString());
				ColumnFamilyHandle columnFamily = db.createColumnFamily(
						new ColumnFamilyDescriptor(bytes("write-batch-cf"))
				);
				WriteBatch batch = new WriteBatch()) {
			batch.put(longKey, longValue);
			batch.put(new byte[0], new byte[0]);
			batch.put(columnFamily, bytes("cf-put-key"), bytes("cf-put-value"));
			batch.delete(bytes("delete-key"));
			batch.delete(columnFamily, bytes("cf-delete-key"));
			batch.singleDelete(bytes("single-delete-key"));
			batch.singleDelete(columnFamily, bytes("cf-single-delete-key"));
			batch.merge(bytes("merge-key"), bytes("merge-value"));
			batch.merge(columnFamily, bytes("cf-merge-key"), bytes("cf-merge-value"));
			batch.deleteRange(bytes("range-start"), bytes("range-end"));
			batch.deleteRange(columnFamily, bytes("cf-range-start"), bytes("cf-range-end"));
			batch.putLogData(bytes("log-data"));

			assertEquals(nativeEvents(batch.data()), customEvents(batch.data()));
		}
	}

	@Test
	void decodesUnsignedMultiByteColumnFamilyId() throws Exception {
		byte[] data = batch(1, record(0x05,
				varInt(300),
				varString("key".repeat(50)),
				varString("value".repeat(40))
		));

		List<String> decoded = customEvents(data);
		assertEquals(nativeEvents(data), decoded);
		assertEquals(1, decoded.size());
		assertTrue(decoded.getFirst().startsWith("PUT[300]"));
	}

	@Test
	void decodesMaximumUnsignedColumnFamilyId() throws Exception {
		byte[] data = batch(1, record(0x04, varInt(-1), varString("key")));
		List<String> decoded = customEvents(data);

		assertEquals(nativeEvents(data), decoded);
		assertEquals(List.of(event("DELETE", -1, bytes("key"), new byte[0])), decoded);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("specialRecordCases")
	void matchesNativeIterationForSpecialRecords(String name, byte[] data, List<String> expected) throws Exception {
		assertEquals(expected, nativeEvents(data), "native format fixture");
		assertEquals(expected, customEvents(data), "custom decoder");
	}

	private static Stream<Arguments> specialRecordCases() {
		return Stream.of(
				Arguments.of("default blob index", batch(1, record(0x11, varString("key"), varString("blob"))),
						List.of(event("BLOB", 0, bytes("key"), bytes("blob")))),
				Arguments.of("column-family blob index", batch(1,
						record(0x10, varInt(300), varString("key"), varString("blob"))),
						List.of(event("BLOB", 300, bytes("key"), bytes("blob")))),
				Arguments.of("begin prepare", batch(0, record(0x09)), List.of("BEGIN_PREPARE")),
				Arguments.of("begin persisted prepare", batch(0, record(0x12)), List.of("BEGIN_PREPARE")),
				Arguments.of("begin unprepare", batch(0, record(0x13)), List.of("BEGIN_PREPARE")),
				Arguments.of("end prepare", batch(0, record(0x0A, varString("end-xid"))),
						List.of(event("END_PREPARE", bytes("end-xid")))),
				Arguments.of("commit", batch(0, record(0x0B, varString("commit-xid"))),
						List.of(event("COMMIT", bytes("commit-xid")))),
				Arguments.of("rollback", batch(0, record(0x0C, varString("rollback-xid"))),
						List.of(event("ROLLBACK", bytes("rollback-xid")))),
				Arguments.of("empty batch noop", batch(0, record(0x0D)), List.of("NOOP[true]")),
				Arguments.of("non-empty batch noop", batch(1,
						record(0x01, varString("key"), varString("value")), record(0x0D)),
						List.of(event("PUT", 0, bytes("key"), bytes("value")), "NOOP[false]")),
				Arguments.of("noop after prepare boundary", batch(1,
						record(0x09),
						record(0x01, varString("key"), varString("value")),
						record(0x0A, varString("xid")),
						record(0x0D)),
						List.of(
								"BEGIN_PREPARE",
								event("PUT", 0, bytes("key"), bytes("value")),
								event("END_PREPARE", bytes("xid")),
								"NOOP[true]"
						)),
				Arguments.of("consecutive empty noops", batch(0, record(0x0D), record(0x0D)),
						List.of("NOOP[true]", "NOOP[true]")),
				Arguments.of("commit with timestamp", batch(0,
						record(0x15, varString("timestamp"), varString("commit-xid"))),
						List.of(event("COMMIT_TIMESTAMP", bytes("commit-xid"), bytes("timestamp"))))
		);
	}

	@Test
	void honorsHandlerEarlyTermination() throws Exception {
		byte[] data = batch(3,
				record(0x01, varString("key-1"), varString("value-1")),
				record(0x01, varString("key-2"), varString("value-2")),
				record(0x01, varString("key-3"), varString("value-3"))
		);

		try (RecordingHandler handler = new RecordingHandler(1)) {
			WriteBatchIterator.iterate(data, handler);
			assertEquals(List.of(event("PUT", 0, bytes("key-1"), bytes("value-1"))), handler.events);
		}
	}

	@Test
	void resumableCursorDecodesEachRecordExactlyOnceAcrossBoundedSlices() throws Exception {
		byte[] data = batch(3,
				record(0x01, varString("key-1"), varString("value-1")),
				record(0x01, varString("key-2"), varString("value-2")),
				record(0x01, varString("key-3"), varString("value-3"))
		);
		var cursor = WriteBatchIterator.cursor(data);

		try (RecordingHandler handler = new RecordingHandler()) {
			assertEquals(1, cursor.iterate(handler, 1));
			assertEquals(1L, cursor.recordsRead());
			assertFalse(cursor.isFinished());

			assertEquals(1, cursor.iterate(handler, 1));
			assertEquals(2L, cursor.recordsRead());
			assertFalse(cursor.isFinished());

			assertEquals(1, cursor.iterate(handler, 1));
			assertEquals(3L, cursor.recordsRead());
			assertTrue(cursor.isFinished());
			assertEquals(nativeEvents(data), handler.events);
		}
	}

	@Test
	void cursorCanRewindAByteBudgetRejectionWithoutSkippingTheRecord() throws Exception {
		byte[] data = batch(2,
				record(0x01, varString("key-1"), varString("value-1")),
				record(0x01, varString("key-2"), varString("value-2"))
		);
		var cursor = WriteBatchIterator.cursor(data);

		try (RecordingHandler firstAttempt = new RecordingHandler()) {
			assertEquals(1, cursor.iterate(firstAttempt, 1));
			assertEquals(1L, cursor.recordsRead());
			cursor.rewindLastRecord();
			assertEquals(0L, cursor.recordsRead());
			assertFalse(cursor.isFinished());
		}

		try (RecordingHandler resumed = new RecordingHandler()) {
			assertEquals(2, cursor.iterate(resumed, 2));
			assertTrue(cursor.isFinished());
			assertEquals(nativeEvents(data), resumed.events);
		}
	}

	@Test
	void propagatesHandlerFailureWithoutDecodingFurtherRecords() {
		byte[] data = batch(2,
				record(0x05, varInt(42), varString("key-1"), varString("value-1")),
				record(0x05, varInt(42), varString("key-2"), varString("value-2"))
		);
		RocksDBException expected = new RocksDBException("handler failure");

		try (RecordingHandler handler = new RecordingHandler() {
			@Override
			public void put(int columnFamilyId, byte[] key, byte[] value) throws RocksDBException {
				throw expected;
			}
		}) {
			assertSame(expected, assertThrows(RocksDBException.class,
					() -> WriteBatchIterator.iterate(data, handler)));
		}
	}

	@Test
	void acceptsAnEmptyWellFormedBatch() {
		assertDoesNotThrow(() -> {
			try (RecordingHandler handler = new RecordingHandler()) {
				WriteBatchIterator.iterate(batch(0), handler);
				assertEquals(List.of(), handler.events);
			}
		});
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("malformedBatches")
	void rejectsMalformedBatchWithCheckedDecoderError(String name, byte[] data) {
		try (RecordingHandler handler = new RecordingHandler()) {
			assertThrows(RocksDBException.class, () -> WriteBatchIterator.iterate(data, handler));
		}
	}

	private static Stream<Arguments> malformedBatches() {
		return Stream.of(
				Arguments.of("empty input", new byte[0]),
				Arguments.of("truncated header", new byte[HEADER_SIZE - 1]),
				Arguments.of("header count exceeds records", batch(1)),
				Arguments.of("records exceed header count", batch(0,
						record(0x01, varString("key"), varString("value")))),
				Arguments.of("unsigned header count exceeds records", batch(-1)),
				Arguments.of("missing key length", batch(1, record(0x01))),
				Arguments.of("unterminated key length", batch(1, record(0x01, new byte[] {(byte) 0x80}))),
				Arguments.of("overlong key length", batch(1,
						record(0x01, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80,
								(byte) 0x80, (byte) 0x80, 0x00}))),
				Arguments.of("overflowing key length", batch(1,
						record(0x01, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80,
								(byte) 0x80, 0x10}))),
				Arguments.of("key length exceeds input", batch(1,
						record(0x01, varInt(5), new byte[] {1, 2}))),
				Arguments.of("unsigned key length exceeds Java array", batch(1,
						record(0x01, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
								(byte) 0xFF, 0x0F}))),
				Arguments.of("malformed column-family id", batch(1,
						record(0x05, new byte[] {(byte) 0x80, (byte) 0x80, (byte) 0x80,
								(byte) 0x80, (byte) 0x80, 0x00})))
		);
	}

	@ParameterizedTest(name = "tag {0}")
	@MethodSource("unsupportedTags")
	void rejectsUnsupportedRecordTypesExplicitly(int type) {
		byte[] data = batch(0, record(type));

		try (RecordingHandler handler = new RecordingHandler()) {
			RocksDBException exception = assertThrows(RocksDBException.class,
					() -> WriteBatchIterator.iterate(data, handler));
			assertTrue(exception.getMessage().contains("Unsupported operation type"), exception::getMessage);
		}
	}

	private static Stream<Integer> unsupportedTags() {
		return Stream.of(
				0x14, // Deletion with timestamp is not representable by WriteBatch.Handler.
				0x16, // Wide-column entity.
				0x17, // Column-family wide-column entity.
				0x18, // Timed put.
				0x19, // Column-family timed put.
				0x7F, // Unknown/reserved record type.
				0xFF  // Unknown record type must be reported as unsigned.
		);
	}

	@Test
	void rejectsNullArguments() {
		byte[] data = batch(1, record(0x01, varString("key"), varString("value")));

		try (RecordingHandler handler = new RecordingHandler()) {
			assertThrows(NullPointerException.class, () -> WriteBatchIterator.iterate(null, handler));
		}
		assertThrows(NullPointerException.class, () -> WriteBatchIterator.iterate(data, null));
	}

	private static List<String> nativeEvents(byte[] data) throws Exception {
		try (WriteBatch batch = new WriteBatch(data); RecordingHandler handler = new RecordingHandler()) {
			batch.iterate(handler);
			return List.copyOf(handler.events);
		}
	}

	private static List<String> customEvents(byte[] data) throws Exception {
		try (RecordingHandler handler = new RecordingHandler()) {
			WriteBatchIterator.iterate(data, handler);
			return List.copyOf(handler.events);
		}
	}

	private static byte[] batch(int count, byte[]... records) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		output.writeBytes(new byte[8]);
		output.write(count & 0xFF);
		output.write((count >>> 8) & 0xFF);
		output.write((count >>> 16) & 0xFF);
		output.write((count >>> 24) & 0xFF);
		for (byte[] record : records) {
			output.writeBytes(record);
		}
		return output.toByteArray();
	}

	private static byte[] record(int type, byte[]... fields) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		output.write(type);
		for (byte[] field : fields) {
			output.writeBytes(field);
		}
		return output.toByteArray();
	}

	private static byte[] varString(String value) {
		byte[] bytes = bytes(value);
		return recordBytes(varInt(bytes.length), bytes);
	}

	private static byte[] varInt(int value) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		do {
			int next = value & 0x7F;
			value >>>= 7;
			if (value != 0) {
				next |= 0x80;
			}
			output.write(next);
		} while (value != 0);
		return output.toByteArray();
	}

	private static byte[] recordBytes(byte[]... fields) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		for (byte[] field : fields) {
			output.writeBytes(field);
		}
		return output.toByteArray();
	}

	private static byte[] bytes(String value) {
		return value.getBytes(StandardCharsets.UTF_8);
	}

	private static String event(String operation, int columnFamilyId, byte[] key, byte[] value) {
		return operation + "[" + columnFamilyId + "](" + hex(key) + "," + hex(value) + ")";
	}

	private static String event(String operation, byte[] value) {
		return operation + "(" + hex(value) + ")";
	}

	private static String event(String operation, byte[] first, byte[] second) {
		return operation + "(" + hex(first) + "," + hex(second) + ")";
	}

	private static String hex(byte[] value) {
		return HexFormat.of().formatHex(value);
	}

	private static class RecordingHandler extends WriteBatch.Handler {

		private final List<String> events = new ArrayList<>();
		private final int maximumEvents;

		private RecordingHandler() {
			this(Integer.MAX_VALUE);
		}

		private RecordingHandler(int maximumEvents) {
			this.maximumEvents = maximumEvents;
		}

		@Override
		public void put(int columnFamilyId, byte[] key, byte[] value) throws RocksDBException {
			events.add(event("PUT", columnFamilyId, key, value));
		}

		@Override
		public void put(byte[] key, byte[] value) {
			events.add(event("PUT", 0, key, value));
		}

		@Override
		public void merge(int columnFamilyId, byte[] key, byte[] value) throws RocksDBException {
			events.add(event("MERGE", columnFamilyId, key, value));
		}

		@Override
		public void merge(byte[] key, byte[] value) {
			events.add(event("MERGE", 0, key, value));
		}

		@Override
		public void delete(int columnFamilyId, byte[] key) throws RocksDBException {
			events.add(event("DELETE", columnFamilyId, key, new byte[0]));
		}

		@Override
		public void delete(byte[] key) {
			events.add(event("DELETE", 0, key, new byte[0]));
		}

		@Override
		public void singleDelete(int columnFamilyId, byte[] key) throws RocksDBException {
			events.add(event("SINGLE_DELETE", columnFamilyId, key, new byte[0]));
		}

		@Override
		public void singleDelete(byte[] key) {
			events.add(event("SINGLE_DELETE", 0, key, new byte[0]));
		}

		@Override
		public void deleteRange(int columnFamilyId, byte[] beginKey, byte[] endKey) throws RocksDBException {
			events.add(event("DELETE_RANGE", columnFamilyId, beginKey, endKey));
		}

		@Override
		public void deleteRange(byte[] beginKey, byte[] endKey) {
			events.add(event("DELETE_RANGE", 0, beginKey, endKey));
		}

		@Override
		public void logData(byte[] blob) {
			events.add(event("LOG_DATA", blob));
		}

		@Override
		public void putBlobIndex(int columnFamilyId, byte[] key, byte[] value) throws RocksDBException {
			events.add(event("BLOB", columnFamilyId, key, value));
		}

		@Override
		public void markBeginPrepare() throws RocksDBException {
			events.add("BEGIN_PREPARE");
		}

		@Override
		public void markEndPrepare(byte[] xid) throws RocksDBException {
			events.add(event("END_PREPARE", xid));
		}

		@Override
		public void markNoop(boolean emptyBatch) throws RocksDBException {
			events.add("NOOP[" + emptyBatch + "]");
		}

		@Override
		public void markRollback(byte[] xid) throws RocksDBException {
			events.add(event("ROLLBACK", xid));
		}

		@Override
		public void markCommit(byte[] xid) throws RocksDBException {
			events.add(event("COMMIT", xid));
		}

		@Override
		public void markCommitWithTimestamp(byte[] xid, byte[] timestamp) throws RocksDBException {
			events.add(event("COMMIT_TIMESTAMP", xid, timestamp));
		}

		@Override
		public boolean shouldContinue() {
			return events.size() < maximumEvents;
		}
	}
}
