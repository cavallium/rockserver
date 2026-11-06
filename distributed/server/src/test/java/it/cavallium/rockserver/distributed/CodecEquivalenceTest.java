package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.MemorySegmentBuf;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.Arena;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The Raft log and the replies are persisted or exchanged between versions, so the exact-size encoders must write
 * the bytes of the stream-based ones they replaced, frozen in {@link OldLogEntryCodec} and {@link OldResultCodec}, and
 * the decoders must read from a buffer view what they read from an array.
 */
@Execution(ExecutionMode.SAME_THREAD)
class CodecEquivalenceTest {

	/** Value sizes around the boundaries of word and vector copies. */
	private static final int[] EDGE_SIZES = {0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 512, 1536, 3072};
	private static final String UNICODE = "colonna-è̈-列-🚀";

	static Stream<Long> seeds() {
		return LongStream.range(0, 40).boxed();
	}

	@ParameterizedTest
	@MethodSource("it.cavallium.rockserver.distributed.LogEntryCodecTest#entries")
	void encodesEveryKnownEntryAsBefore(LogEntry entry) {
		assertArrayEquals(OldLogEntryCodec.encode(entry), LogEntryCodec.encode(entry));
		assertArrayEquals(OldLogEntryCodec.encodeEntryBody(entry), LogEntryCodec.encodeEntryBody(entry));
		assertEquals(LogEntryCodec.encodeEntryBody(entry).length, LogEntryCodec.entrySize(entry));
	}

	@ParameterizedTest
	@MethodSource("seeds")
	void encodesRandomBatchesAsBefore(long seed) {
		var random = new Random(seed);
		var requests = new ArrayList<Request>();
		long proposerId = random.nextLong();
		long completedBelow = random.nextInt(1000);
		int count = 1 + random.nextInt(12);
		for (int i = 0; i < count; i++) {
			requests.add(new Request(proposerId, completedBelow + random.nextInt(1000), completedBelow,
					randomEntry(random)));
		}
		byte[] expected = OldLogEntryCodec.encodeBatch(requests);
		assertArrayEquals(expected, LogEntryCodec.encodeBatch(requests));

		var sequences = requests.stream().mapToLong(Request::sequence).toArray();
		var bodies = requests.stream().map(r -> LogEntryCodec.encodeEntryBody(r.entry())).toList();
		assertArrayEquals(expected, LogEntryCodec.encodeBatch(proposerId, completedBelow, sequences, bodies));

		// Decoded from an array, and from a read-only view in the middle of a larger array: both re-encode to the
		// same bytes with the old codec.
		assertArrayEquals(expected, OldLogEntryCodec.encodeBatch(LogEntryCodec.decodeBatch(expected)));
		assertArrayEquals(expected, OldLogEntryCodec.encodeBatch(LogEntryCodec.decodeBatch(view(expected, random))));
	}

	/**
	 * The proposer writes each entry straight into its batch: the bytes are those of the entry bodies encoded one by
	 * one and then joined, with or without client request ids, and so the bytes the old codec wrote. The requests it
	 * returns for the leader's own state machine are what decoding those bytes in place reads, every key and value a
	 * slice of the bytes; for a batch with a schema change there are none. The leader's stamp before the bytes changes
	 * nothing of what a follower decodes.
	 */
	@ParameterizedTest
	@MethodSource("seeds")
	void encodesEntriesStraightIntoTheBatchAsFromTheirBodies(long seed) {
		var random = new Random(seed);
		long proposerId = random.nextLong();
		long completedBelow = random.nextInt(1000);
		int count = 1 + random.nextInt(12);
		var entries = new ArrayList<LogEntry>();
		var sequences = new long[count];
		var clients = seed % 2 == 0 ? null : new ClientRequestId[count];
		long size = 0;
		for (int i = 0; i < count; i++) {
			var entry = randomEntry(random);
			// Half of the batches without schema changes, which have requests.
			while (seed % 4 >= 2
					&& (entry instanceof LogEntry.CreateColumn || entry instanceof LogEntry.UploadMergeOperator)) {
				entry = randomEntry(random);
			}
			entries.add(entry);
			size += LogEntryCodec.entrySize(entry);
			sequences[i] = completedBelow + random.nextInt(1000);
			if (clients != null && random.nextBoolean()) {
				long sequence = random.nextInt(100);
				clients[i] = new ClientRequestId(1 + random.nextInt(1000), sequence, random.nextLong(sequence + 1));
			}
		}
		var bodies = entries.stream().map(LogEntryCodec::encodeEntryBody).toList();
		byte[] expected = LogEntryCodec.encodeBatch(proposerId, completedBelow, sequences, clients, bodies);
		if (clients == null || Arrays.stream(clients).allMatch(java.util.Objects::isNull)) {
			assertEquals(LogEntryCodec.BATCH_VERSION, expected[0]);
			assertArrayEquals(OldLogEntryCodec.encodeBatch(proposerId, completedBelow, sequences, bodies), expected);
		} else {
			assertEquals(LogEntryCodec.BATCH_VERSION_CLIENTS, expected[0]);
		}

		var plain = LogEntryCodec.encodeBatch(proposerId, completedBelow, sequences, clients, entries, size, false);
		assertArrayEquals(expected, plain.bytes());
		assertNull(plain.requests());

		var encoded = LogEntryCodec.encodeBatch(proposerId, completedBelow, sequences, clients, entries, size, true);
		byte[] bytes = encoded.bytes();
		assertArrayEquals(expected, bytes);
		var requests = encoded.requests();
		if (entries.stream().anyMatch(e -> e instanceof LogEntry.CreateColumn || e instanceof LogEntry.UploadMergeOperator)) {
			assertNull(requests);
			return;
		}
		var decoded = LogEntryCodec.decodeBatchInPlace(bytes, 0, bytes.length);
		assertEquals(decoded.size(), requests.size());
		for (int i = 0; i < count; i++) {
			var request = requests.get(i);
			var read = decoded.get(i);
			assertEquals(read.proposerId(), request.proposerId());
			assertEquals(read.sequence(), request.sequence());
			assertEquals(read.completedBelow(), request.completedBelow());
			assertEquals(read.client(), request.client());
			assertEquals(read.entry().getClass(), request.entry().getClass());
			if (!(read.entry() instanceof LogEntry.Guarded)) {
				// A commit's arrays compare by identity: compared through its bytes below.
				assertEquals(read.entry(), request.entry());
			}
			assertArrayEquals(LogEntryCodec.encodeEntryBody(read.entry()), LogEntryCodec.encodeEntryBody(request.entry()));
			for (Buf buf : bufs(request.entry())) {
				if (buf != null && buf.size() > 0) {
					assertSame(bytes, buf.getBackingByteArrayStrict(), "a slice of the batch, not a copy");
				}
			}
			if (request.entry() instanceof LogEntry.Guarded written) {
				var original = (LogEntry.Guarded) entries.get(i);
				assertNotSame(original.conflictColumns(), written.conflictColumns());
				assertNotSame(original.conflictHashes(), written.conflictHashes());
			}
		}
		assertArrayEquals(expected, LogEntryCodec.encodeBatch(requests));

		// The leader's stamp before the same bytes: a follower decodes the same requests, and the leader's clock.
		var stamped = new byte[LogEntryCodec.STAMP_BYTES + bytes.length];
		System.arraycopy(LogEntryCodec.stamp(42), 0, stamped, 0, LogEntryCodec.STAMP_BYTES);
		System.arraycopy(bytes, 0, stamped, LogEntryCodec.STAMP_BYTES, bytes.length);
		assertEquals(42, LogEntryCodec.leaderTime(ByteBuffer.wrap(stamped)));
		assertArrayEquals(expected, LogEntryCodec.encodeBatch(LogEntryCodec.decodeBatch(stamped)));
		assertArrayEquals(expected, LogEntryCodec.encodeBatch(
				LogEntryCodec.decodeBatchInPlace(stamped, 0, stamped.length)));
	}

	/** The size the caller gives must be the entries' size: a write that changed meanwhile is caught, not sent. */
	@Test
	void encodingEntriesIntoABatchChecksTheirSize() {
		var entry = new LogEntry.Put(1, new Keys(Buf.wrap(new byte[] {1})), Buf.wrap(new byte[] {2}),
				RequestTypeId.NOTHING);
		long size = LogEntryCodec.entrySize(entry);
		assertThrows(IndexOutOfBoundsException.class, () -> LogEntryCodec.encodeBatch(1, 0, new long[] {0}, null,
				List.of(entry), size - 1, true));
		assertThrows(IllegalStateException.class, () -> LogEntryCodec.encodeBatch(1, 0, new long[] {0}, null,
				List.of(entry), size + 1, true));
		assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.encodeBatch(1, 5, new long[] {4}, null,
				List.of(entry), size, true));
	}

	/** Every key and value of {@code entry}. */
	private static List<Buf> bufs(LogEntry entry) {
		var bufs = new ArrayList<Buf>();
		switch (entry) {
			case LogEntry.Write write -> writeBufs(write, bufs);
			case LogEntry.Guarded guarded -> {
				guarded.writes().forEach(write -> writeBufs(write, bufs));
				if (guarded.guard() != null) {
					bufs.addAll(List.of(guarded.guard().keys().keys()));
				}
			}
			default -> {}
		}
		return bufs;
	}

	private static void writeBufs(LogEntry.Write write, List<Buf> bufs) {
		switch (write) {
			case LogEntry.Put(_, Keys keys, Buf value, _) -> {
				bufs.addAll(List.of(keys.keys()));
				bufs.add(value);
			}
			case LogEntry.Merge(_, Keys keys, Buf value, _) -> {
				bufs.addAll(List.of(keys.keys()));
				bufs.add(value);
			}
			case LogEntry.Delete(_, Keys keys, _) -> bufs.addAll(List.of(keys.keys()));
			case LogEntry.PutMulti(_, List<Keys> keys, List<Buf> values, _) -> {
				keys.forEach(k -> bufs.addAll(List.of(k.keys())));
				bufs.addAll(values);
			}
			case LogEntry.MergeMulti(_, List<Keys> keys, List<Buf> values, _) -> {
				keys.forEach(k -> bufs.addAll(List.of(k.keys())));
				bufs.addAll(values);
			}
			case LogEntry.DeleteMulti(_, List<Keys> keys, _) -> keys.forEach(k -> bufs.addAll(List.of(k.keys())));
			case LogEntry.DeleteRange(_, Keys start, Keys end) -> {
				if (start != null) {
					bufs.addAll(List.of(start.keys()));
				}
				if (end != null) {
					bufs.addAll(List.of(end.keys()));
				}
			}
		}
	}

	@Test
	void encodesEveryEdgeSizeAsBefore() {
		var random = new Random(7);
		for (int size : EDGE_SIZES) {
			for (Buf value : variants(randomBytes(random, size), random)) {
				for (Buf key : variants(randomBytes(random, Math.min(size, 19)), random)) {
					var entry = new LogEntry.Put(size, new Keys(key, value), value, RequestTypeId.PREVIOUS);
					assertArrayEquals(OldLogEntryCodec.encode(entry), LogEntryCodec.encode(entry), "size " + size);
					var result = new ApplyResult.Ok(new Delta<>(value, key));
					assertArrayEquals(OldResultCodec.encode(result), ResultCodec.encode(result), "size " + size);
				}
			}
		}
	}

	@Test
	void decodingDoesNotMoveOrShareTheView() {
		var entry = new LogEntry.Put(1, new Keys(Buf.wrap(new byte[] {1, 2})), Buf.wrap(new byte[] {3}),
				RequestTypeId.NOTHING);
		byte[] encoded = LogEntryCodec.encodeBatch(List.of(new Request(1, 2, 0, entry)));
		var view = ByteBuffer.wrap(encoded).asReadOnlyBuffer();
		var decoded = LogEntryCodec.decodeBatch(view);
		assertEquals(0, view.position());
		Arrays.fill(encoded, (byte) 0);
		assertEquals(entry, decoded.getFirst().entry());
	}

	@ParameterizedTest
	@MethodSource("it.cavallium.rockserver.distributed.LogEntryCodecTest#entries")
	void rejectsTruncatedBatchViews(LogEntry entry) {
		byte[] encoded = LogEntryCodec.encodeBatch(List.of(new Request(1, 2, 0, entry)));
		int step = Math.max(1, encoded.length / 512);
		for (int length = 0; length < encoded.length; length += step) {
			var truncated = ByteBuffer.wrap(encoded, 0, length).slice().asReadOnlyBuffer();
			assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.decodeBatch(truncated), "length " + length);
		}
		var trailing = ByteBuffer.wrap(Arrays.copyOf(encoded, encoded.length + 1)).asReadOnlyBuffer();
		assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.decodeBatch(trailing));
	}

	@ParameterizedTest
	@MethodSource("it.cavallium.rockserver.distributed.ResultCodecTest#results")
	void encodesEveryKnownResultAsBefore(ApplyResult result) {
		assertArrayEquals(OldResultCodec.encode(result), ResultCodec.encode(result));
		assertArrayEquals(OldResultCodec.encodeAll(List.of(result, result)),
				ResultCodec.encodeAll(List.of(result, result)));
	}

	@ParameterizedTest
	@MethodSource("seeds")
	void encodesRandomRepliesAsBefore(long seed) {
		var random = new Random(seed);
		var results = new ArrayList<ApplyResult>();
		int count = random.nextInt(20);
		for (int i = 0; i < count; i++) {
			results.add(randomResult(random, 0));
		}
		byte[] expected = OldResultCodec.encodeAll(results);
		assertArrayEquals(expected, ResultCodec.encodeAll(results));
		// A limit that forces some results to be resent alone
		int limit = Math.max(64, expected.length / 2);
		byte[] bounded = OldResultCodec.encodeAll(results, limit);
		assertArrayEquals(bounded, ResultCodec.encodeAll(results, limit));

		assertArrayEquals(expected, OldResultCodec.encodeAll(ResultCodec.decodeAll(expected)));
		assertArrayEquals(expected, OldResultCodec.encodeAll(ResultCodec.decodeAll(view(expected, random))));
	}

	@Test
	void rejectsMalformedReplyViews() {
		byte[] encoded = ResultCodec.encodeAll(List.of(ApplyResult.OK_NULL, new ApplyResult.Ok(Buf.wrap(new byte[40]))));
		for (int length = 0; length < encoded.length; length++) {
			var truncated = ByteBuffer.wrap(encoded, 0, length).slice().asReadOnlyBuffer();
			assertThrows(IllegalArgumentException.class, () -> ResultCodec.decodeAll(truncated), "length " + length);
		}
		// A result shorter than its declared length leaves bytes in its slot.
		byte[] padded = ByteBuffer.allocate(4 + 4 + 3).putInt(1).putInt(3).put(ResultCodec.encode(ApplyResult.OK_NULL))
				.put((byte) 0).array();
		assertThrows(IllegalArgumentException.class, () -> ResultCodec.decodeAll(ByteBuffer.wrap(padded)));
		// A result longer than its declared length is truncated in its slot.
		byte[] cut = ByteBuffer.allocate(4 + 4 + 2).putInt(1).putInt(1).put(ResultCodec.encode(ApplyResult.OK_NULL))
				.array();
		assertThrows(IllegalArgumentException.class, () -> ResultCodec.decodeAll(ByteBuffer.wrap(cut)));
	}

	/** {@code data} as a read-only view at a random offset of a larger array, as a protobuf substring would be. */
	private static ByteBuffer view(byte[] data, Random random) {
		int before = random.nextInt(9);
		var larger = new byte[before + data.length + random.nextInt(9)];
		random.nextBytes(larger);
		System.arraycopy(data, 0, larger, before, data.length);
		return ByteBuffer.wrap(larger, before, data.length).asReadOnlyBuffer();
	}

	private static byte[] randomBytes(Random random, int size) {
		var bytes = new byte[size];
		random.nextBytes(bytes);
		return bytes;
	}

	/** The same bytes as a plain array buffer, a slice of a larger array, and an off-heap segment. */
	private static List<Buf> variants(byte[] bytes, Random random) {
		var larger = new byte[bytes.length + 10];
		random.nextBytes(larger);
		System.arraycopy(bytes, 0, larger, 3, bytes.length);
		var offHeap = Arena.ofAuto().allocate(Math.max(1, bytes.length));
		offHeap.copyFrom(java.lang.foreign.MemorySegment.ofArray(bytes));
		return List.of(Buf.wrap(bytes),
				Buf.wrap(larger, 3, 3 + bytes.length),
				new MemorySegmentBuf(offHeap.asSlice(0, bytes.length)),
				new MemorySegmentBuf(java.lang.foreign.MemorySegment.ofArray(larger).asSlice(3, bytes.length)));
	}

	private static Buf randomBuf(Random random) {
		int size = random.nextInt(4) == 0 ? EDGE_SIZES[random.nextInt(EDGE_SIZES.length)] : random.nextInt(40);
		var all = variants(randomBytes(random, size), random);
		return all.get(random.nextInt(all.size()));
	}

	private static Keys randomKeys(Random random) {
		var segments = new Buf[random.nextInt(5)];
		for (int i = 0; i < segments.length; i++) {
			segments[i] = randomBuf(random);
		}
		return new Keys(segments);
	}

	private static List<Keys> randomKeyList(Random random, int count) {
		var keys = new ArrayList<Keys>();
		for (int i = 0; i < count; i++) {
			keys.add(randomKeys(random));
		}
		return keys;
	}

	private static List<Buf> randomValues(Random random, int count) {
		var values = new ArrayList<Buf>();
		for (int i = 0; i < count; i++) {
			values.add(randomBuf(random));
		}
		return values;
	}

	private static RequestTypeId randomRequestType(Random random) {
		var types = RequestTypeId.values();
		return types[random.nextInt(types.length)];
	}

	private static LogEntry.Write randomWrite(Random random) {
		long column = random.nextLong();
		return switch (random.nextInt(7)) {
			case 0 -> new LogEntry.Put(column, randomKeys(random), random.nextInt(5) == 0 ? null : randomBuf(random),
					randomRequestType(random));
			case 1 -> {
				int count = random.nextInt(6);
				yield new LogEntry.PutMulti(column, randomKeyList(random, count), randomValues(random, count),
						randomRequestType(random));
			}
			case 2 -> new LogEntry.Delete(column, randomKeys(random), randomRequestType(random));
			case 3 -> new LogEntry.DeleteMulti(column, randomKeyList(random, random.nextInt(6)),
					randomRequestType(random));
			case 4 -> new LogEntry.Merge(column, randomKeys(random), random.nextInt(5) == 0 ? null : randomBuf(random),
					randomRequestType(random));
			case 5 -> {
				int count = random.nextInt(6);
				yield new LogEntry.MergeMulti(column, randomKeyList(random, count), randomValues(random, count),
						randomRequestType(random));
			}
			default -> new LogEntry.DeleteRange(column, random.nextBoolean() ? null : randomKeys(random),
					random.nextBoolean() ? null : randomKeys(random));
		};
	}

	private static String randomString(Random random) {
		return switch (random.nextInt(3)) {
			case 0 -> "";
			case 1 -> UNICODE;
			default -> "name-" + random.nextInt(1000);
		};
	}

	private static LogEntry randomEntry(Random random) {
		return switch (random.nextInt(9)) {
			case 0, 1, 2 -> randomWrite(random);
			case 3 -> new LogEntry.CreateColumn(randomString(random), ColumnSchema.of(IntList.of(4, 1 + random.nextInt(64)),
					ObjectList.of(ColumnHashType.values()),
					random.nextBoolean(),
					random.nextBoolean() ? null : randomString(random),
					random.nextBoolean() ? null : random.nextLong(),
					random.nextBoolean() ? null : randomString(random)));
			case 4 -> new LogEntry.DeleteColumn(random.nextLong());
			case 5 -> new LogEntry.DeleteColumnIfExists(randomString(random));
			case 6 -> new LogEntry.UploadMergeOperator(randomString(random), randomString(random),
					randomBytes(random, random.nextInt(70)));
			case 7 -> {
				int conflicts = random.nextInt(4);
				var writes = new ArrayList<LogEntry.Write>();
				int writeCount = random.nextInt(5);
				for (int i = 0; i < writeCount; i++) {
					writes.add(randomWrite(random));
				}
				boolean singleResult = writeCount == 1 && random.nextBoolean();
				long updateId = singleResult && random.nextBoolean() ? random.nextLong() | 1 : 0;
				var guard = updateId != 0 && random.nextBoolean() ? randomGuard(random) : null;
				yield new LogEntry.Guarded(random.nextLong(), random.nextLong(),
						random.longs(conflicts).toArray(), random.longs(conflicts).toArray(), writes,
						singleResult, updateId, guard);
			}
			default -> random.nextBoolean() ? new LogEntry.Genesis(randomString(random), random.nextBoolean())
					: new LogEntry.VerifyColumn(randomString(random), random.nextLong());
		};
	}

	private static LogEntry.Guard randomGuard(Random random) {
		return LogEntry.Guard.of(random.nextLong(), randomKeys(random), random.nextBoolean() ? null : randomBuf(random));
	}

	private static ApplyResult randomResult(Random random, int depth) {
		return switch (random.nextInt(depth == 0 ? 6 : 5)) {
			case 0 -> ApplyResult.OK_NULL;
			case 1 -> new ApplyResult.Ok(randomBuf(random));
			case 2 -> new ApplyResult.Ok(random.nextBoolean() ? (Object) random.nextBoolean() : random.nextLong());
			case 3 -> new ApplyResult.Ok(new Delta<>(random.nextBoolean() ? null : randomBuf(random),
					random.nextBoolean() ? null : randomBuf(random)));
			case 4 -> {
				var list = new ArrayList<>();
				int size = depth < 3 ? random.nextInt(5) : 0;
				for (int i = 0; i < size; i++) {
					list.add(random.nextInt(4) == 0 ? null
							: ((ApplyResult.Ok) randomResult(random, depth + 1)).value());
				}
				yield new ApplyResult.Ok(list);
			}
			default -> random.nextInt(4) == 0 ? ApplyResult.RESEND_ALONE
					: new ApplyResult.Failed(RocksDBErrorType.values()[random.nextInt(RocksDBErrorType.values().length)],
							random.nextBoolean() ? "" : UNICODE + " \uD800 unpaired");
		};
	}
}
