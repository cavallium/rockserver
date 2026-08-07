package it.cavallium.rockserver.core.impl.test;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import it.cavallium.rockserver.core.impl.WriteBatchIterator;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WriteBatchIteratorFuzzTest {

	private static final int HEADER_SIZE = 12;
	private static final int MAX_FIELD_SIZE = 2_048;
	private static final int MAX_RECORDS = 64;
	private static final int MAX_REWINDS = 8;
	private static final int MAX_ARBITRARY_INPUT_SIZE = 64 * 1_024;
	private static final int[] SUPPORTED_TYPES = {
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
			0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
			0x10, 0x11, 0x12, 0x13, 0x15
	};

	static {
		RocksDB.loadLibrary();
	}

	@FuzzTest(maxDuration = "30s")
	void validBatchesMatchNativeIterationAndResumableCursor(FuzzedDataProvider data) throws Exception {
		int wireRecordCount = data.consumeInt(0, MAX_RECORDS);
		int sequenceRecordCount = 0;
		ByteArrayOutputStream records = new ByteArrayOutputStream();
		for (int i = 0; i < wireRecordCount; i++) {
			int type = data.pickValue(SUPPORTED_TYPES);
			record(records, type, data);
			if (isSequenceRecord(type)) {
				sequenceRecordCount++;
			}
		}

		byte[] batch = batch(data.consumeLong(), sequenceRecordCount, records.toByteArray());
		List<String> nativeEvents = nativeEvents(batch);
		assertEquals(nativeEvents, customEvents(batch));
		assertEquals(nativeEvents, cursorEvents(batch, data, sequenceRecordCount));
	}

	@FuzzTest(maxDuration = "30s")
	void arbitraryBatchesHaveTheSameMonolithicAndSlicedOutcome(byte[] input) {
		byte[] batch = input.length <= MAX_ARBITRARY_INPUT_SIZE
				? input
				: Arrays.copyOf(input, MAX_ARBITRARY_INPUT_SIZE);

		assertEquals(decodeMonolithically(batch), decodeInSlices(batch));
	}

	private static ParseOutcome decodeMonolithically(byte[] batch) {
		try (RecordingHandler handler = new RecordingHandler()) {
			try {
				WriteBatchIterator.iterate(batch, handler);
				return ParseOutcome.accepted(handler.events);
			} catch (RocksDBException expectedRejection) {
				return ParseOutcome.rejected(handler.events);
			}
		}
	}

	private static ParseOutcome decodeInSlices(byte[] batch) {
		try (RecordingHandler handler = new RecordingHandler()) {
			try {
				WriteBatchIterator.Cursor cursor = WriteBatchIterator.cursor(batch);
				int calls = 0;
				while (!cursor.isFinished()) {
					if (++calls > batch.length + 1) {
						fail("Sliced decoder made too many calls for " + batch.length + " input bytes");
					}
					int budget = 1 + (batch.length == 0 ? 0 : batch[(calls - 1) % batch.length] & 0x07);
					int decoded = cursor.iterate(handler, budget);
					if (decoded == 0 && !cursor.isFinished()) {
						fail("Sliced decoder made no progress");
					}
				}
				return ParseOutcome.accepted(handler.events);
			} catch (RocksDBException expectedRejection) {
				return ParseOutcome.rejected(handler.events);
			}
		}
	}

	private static List<String> cursorEvents(byte[] batch,
	                                         FuzzedDataProvider data,
	                                         int expectedSequenceRecords) throws RocksDBException {
		WriteBatchIterator.Cursor cursor = WriteBatchIterator.cursor(batch);
		int calls = 0;
		int rewinds = 0;
		try (RecordingHandler handler = new RecordingHandler()) {
			while (!cursor.isFinished()) {
				assertTrue(++calls <= MAX_RECORDS + MAX_REWINDS + 1, "cursor did not converge");
				int budget = data.consumeInt(1, 8);
				long recordsBefore = cursor.recordsRead();
				int eventsBefore = handler.events.size();
				int decoded = cursor.iterate(handler, budget);
				assertTrue(decoded >= 0 && decoded <= budget);

				if (decoded == budget && decoded > 0 && rewinds < MAX_REWINDS && data.consumeBoolean()) {
					assertTrue(handler.events.size() > eventsBefore);
					cursor.rewindLastRecord();
					handler.events.removeLast();
					assertEquals(recordsBefore + decoded - 1, cursor.recordsRead());
					assertFalse(cursor.isFinished());
					rewinds++;
				} else {
					assertEquals(recordsBefore + decoded, cursor.recordsRead());
				}
			}
			assertEquals(Integer.toUnsignedLong(expectedSequenceRecords), cursor.recordsRead());
			return List.copyOf(handler.events);
		}
	}

	private static List<String> nativeEvents(byte[] batch) throws RocksDBException {
		try (WriteBatch writeBatch = new WriteBatch(batch); RecordingHandler handler = new RecordingHandler()) {
			writeBatch.iterate(handler);
			return List.copyOf(handler.events);
		}
	}

	private static List<String> customEvents(byte[] batch) throws RocksDBException {
		try (RecordingHandler handler = new RecordingHandler()) {
			WriteBatchIterator.iterate(batch, handler);
			return List.copyOf(handler.events);
		}
	}

	private static void record(ByteArrayOutputStream output, int type, FuzzedDataProvider data) {
		output.write(type);
		switch (type) {
			case 0x00, 0x03, 0x07, 0x0A, 0x0B, 0x0C -> writeVarString(output, field(data));
			case 0x01, 0x02, 0x0F, 0x11 -> {
				writeVarString(output, field(data));
				writeVarString(output, field(data));
			}
			case 0x04, 0x08 -> {
				writeVarInt(output, data.consumeInt());
				writeVarString(output, field(data));
			}
			case 0x05, 0x06, 0x0E, 0x10 -> {
				writeVarInt(output, data.consumeInt());
				writeVarString(output, field(data));
				writeVarString(output, field(data));
			}
			case 0x15 -> {
				writeVarString(output, field(data)); // Timestamp precedes XID on the wire.
				writeVarString(output, field(data));
			}
			case 0x09, 0x0D, 0x12, 0x13 -> {
				// No payload.
			}
			default -> throw new AssertionError("Unexpected supported type: " + type);
		}
	}

	private static boolean isSequenceRecord(int type) {
		return switch (type) {
			case 0x00, 0x01, 0x02, 0x04, 0x05, 0x06, 0x07, 0x08,
			     0x0E, 0x0F, 0x10, 0x11 -> true;
			default -> false;
		};
	}

	private static byte[] field(FuzzedDataProvider data) {
		return data.consumeBytes(data.consumeInt(0, MAX_FIELD_SIZE));
	}

	private static byte[] batch(long sequence, int count, byte[] records) {
		ByteArrayOutputStream output = new ByteArrayOutputStream(HEADER_SIZE + records.length);
		writeLongLE(output, sequence);
		writeIntLE(output, count);
		output.writeBytes(records);
		return output.toByteArray();
	}

	private static void writeVarString(ByteArrayOutputStream output, byte[] value) {
		writeVarInt(output, value.length);
		output.writeBytes(value);
	}

	private static void writeVarInt(ByteArrayOutputStream output, int value) {
		do {
			int next = value & 0x7F;
			value >>>= 7;
			if (value != 0) {
				next |= 0x80;
			}
			output.write(next);
		} while (value != 0);
	}

	private static void writeIntLE(ByteArrayOutputStream output, int value) {
		for (int shift = 0; shift < Integer.SIZE; shift += Byte.SIZE) {
			output.write(value >>> shift);
		}
	}

	private static void writeLongLE(ByteArrayOutputStream output, long value) {
		for (int shift = 0; shift < Long.SIZE; shift += Byte.SIZE) {
			output.write((int) (value >>> shift));
		}
	}

	private static String event(String operation, int columnFamilyId, byte[] key, byte[] value) {
		return operation + "[" + Integer.toUnsignedString(columnFamilyId) + "](" + hex(key) + "," + hex(value) + ")";
	}

	private static String event(String operation, byte[] value) {
		return operation + "(" + hex(value) + ")";
	}

	private static String commitTimestampEvent(byte[] xid, byte[] timestamp) {
		return "COMMIT_TIMESTAMP(" + hex(xid) + "," + hex(timestamp) + ")";
	}

	private static String hex(byte[] value) {
		return HexFormat.of().formatHex(value);
	}

	private record ParseOutcome(List<String> events, boolean rejected) {

		private static ParseOutcome accepted(List<String> events) {
			return new ParseOutcome(List.copyOf(events), false);
		}

		private static ParseOutcome rejected(List<String> events) {
			return new ParseOutcome(List.copyOf(events), true);
		}
	}

	private static final class RecordingHandler extends WriteBatch.Handler {

		private final List<String> events = new ArrayList<>();

		@Override
		public void put(int columnFamilyId, byte[] key, byte[] value) {
			events.add(event("PUT", columnFamilyId, key, value));
		}

		@Override
		public void put(byte[] key, byte[] value) {
			events.add(event("PUT", 0, key, value));
		}

		@Override
		public void merge(int columnFamilyId, byte[] key, byte[] value) {
			events.add(event("MERGE", columnFamilyId, key, value));
		}

		@Override
		public void merge(byte[] key, byte[] value) {
			events.add(event("MERGE", 0, key, value));
		}

		@Override
		public void delete(int columnFamilyId, byte[] key) {
			events.add(event("DELETE", columnFamilyId, key, new byte[0]));
		}

		@Override
		public void delete(byte[] key) {
			events.add(event("DELETE", 0, key, new byte[0]));
		}

		@Override
		public void singleDelete(int columnFamilyId, byte[] key) {
			events.add(event("SINGLE_DELETE", columnFamilyId, key, new byte[0]));
		}

		@Override
		public void singleDelete(byte[] key) {
			events.add(event("SINGLE_DELETE", 0, key, new byte[0]));
		}

		@Override
		public void deleteRange(int columnFamilyId, byte[] beginKey, byte[] endKey) {
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
		public void putBlobIndex(int columnFamilyId, byte[] key, byte[] value) {
			events.add(event("BLOB", columnFamilyId, key, value));
		}

		@Override
		public void markBeginPrepare() {
			events.add("BEGIN_PREPARE");
		}

		@Override
		public void markEndPrepare(byte[] xid) {
			events.add(event("END_PREPARE", xid));
		}

		@Override
		public void markNoop(boolean emptyBatch) {
			events.add("NOOP[" + emptyBatch + "]");
		}

		@Override
		public void markRollback(byte[] xid) {
			events.add(event("ROLLBACK", xid));
		}

		@Override
		public void markCommit(byte[] xid) {
			events.add(event("COMMIT", xid));
		}

		@Override
		public void markCommitWithTimestamp(byte[] xid, byte[] timestamp) {
			events.add(commitTimestampEvent(xid, timestamp));
		}

		@Override
		public boolean shouldContinue() {
			return true;
		}
	}
}
