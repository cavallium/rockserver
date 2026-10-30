package it.cavallium.rockserver.distributed;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.distributed.LogEntry.CreateColumn;
import it.cavallium.rockserver.distributed.LogEntry.Delete;
import it.cavallium.rockserver.distributed.LogEntry.DeleteColumn;
import it.cavallium.rockserver.distributed.LogEntry.DeleteColumnIfExists;
import it.cavallium.rockserver.distributed.LogEntry.DeleteMulti;
import it.cavallium.rockserver.distributed.LogEntry.DeleteRange;
import it.cavallium.rockserver.distributed.LogEntry.Genesis;
import it.cavallium.rockserver.distributed.LogEntry.Guard;
import it.cavallium.rockserver.distributed.LogEntry.Guarded;
import it.cavallium.rockserver.distributed.LogEntry.Merge;
import it.cavallium.rockserver.distributed.LogEntry.MergeMulti;
import it.cavallium.rockserver.distributed.LogEntry.Put;
import it.cavallium.rockserver.distributed.LogEntry.PutMulti;
import it.cavallium.rockserver.distributed.LogEntry.UploadMergeOperator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class LogEntryCodecTest {

	private static final String UNICODE = "colonna-è̈-列-🚀";

	static Stream<Named<LogEntry>> entries() {
		return Stream.of(
				Named.of("put", new Put(3, keys("user", "42"), buf("value"), RequestTypeId.PREVIOUS)),
				Named.of("put with empty keys and value", new Put(0, new Keys(), Buf.wrap(), RequestTypeId.NOTHING)),
				Named.of("put with an empty key segment", new Put(1, keys(""), buf("v"), RequestTypeId.DELTA)),
				Named.of("put with a null value", new Put(-1, keys("k"), null, RequestTypeId.CHANGED)),
				Named.of("put multi", new PutMulti(Long.MAX_VALUE,
						List.of(keys("a"), keys("b", "c"), new Keys()),
						List.of(buf("1"), buf("2"), Buf.wrap()),
						RequestTypeId.MULTI)),
				Named.of("put multi, empty", new PutMulti(2, List.of(), List.of(), RequestTypeId.NOTHING)),
				Named.of("put multi, large", largePutMulti()),
				Named.of("delete", new Delete(4, keys("x", "y"), RequestTypeId.PREVIOUS_PRESENCE)),
				Named.of("delete multi",
						new DeleteMulti(5, List.of(keys("a"), new Keys(), keys("")), RequestTypeId.EXISTS)),
				Named.of("merge", new Merge(6, keys("counter"), buf("+1"), RequestTypeId.MERGED)),
				Named.of("merge with a null value", new Merge(6, keys("counter"), null, RequestTypeId.NOTHING)),
				Named.of("merge multi", new MergeMulti(7,
						List.of(keys("a"), keys("b")),
						List.of(buf("x"), Buf.wrap()),
						RequestTypeId.ENSURE)),
				Named.of("delete range", new DeleteRange(8, keys("a"), keys("z"))),
				Named.of("delete range, open start", new DeleteRange(8, null, keys("m"))),
				Named.of("delete range, open end", new DeleteRange(8, keys("m"), null)),
				Named.of("delete range, unbounded", new DeleteRange(8, null, null)),
				Named.of("create column",
						new CreateColumn("plain", ColumnSchema.of(IntList.of(4, 8), ObjectList.of(), true))),
				Named.of("create column with every hash type", new CreateColumn(UNICODE, ColumnSchema.of(IntList.of(2),
						ObjectList.of(ColumnHashType.values()),
						false,
						"counter",
						7L,
						"com.example.CounterMergeOperator"))),
				Named.of("create column with a merge name only", new CreateColumn("",
						ColumnSchema.of(IntList.of(), ObjectList.of(), true, "op", null, null))),
				Named.of("delete column", new DeleteColumn(Long.MIN_VALUE)),
				Named.of("delete column if exists", new DeleteColumnIfExists(UNICODE)),
				Named.of("upload merge operator",
						new UploadMergeOperator("op", "com.example.Op", new byte[] {1, 2, 3})),
				Named.of("upload merge operator, empty jar", new UploadMergeOperator(UNICODE, "", new byte[0])),
				Named.of("guarded with every write type", new Guarded(-9_000_000_000L,
						123,
						new long[] {3, 3, 8},
						new long[] {-1, 0, Long.MAX_VALUE},
						List.of(
								new Put(3, keys("a"), buf("1"), RequestTypeId.NOTHING),
								new PutMulti(3, List.of(keys("b")), List.of(buf("2")), RequestTypeId.NOTHING),
								new Delete(3, keys("c"), RequestTypeId.NOTHING),
								new DeleteMulti(3, List.of(keys("d")), RequestTypeId.NOTHING),
								new Merge(3, keys("e"), null, RequestTypeId.NOTHING),
								new MergeMulti(3, List.of(keys("f")), List.of(buf("3")), RequestTypeId.NOTHING),
								new DeleteRange(8, null, keys("g"))),
						false)),
				Named.of("guarded update commit", new Guarded(1,
						0,
						new long[] {3},
						new long[] {77},
						List.of(new Put(3, keys("a"), buf("1"), RequestTypeId.DELTA)),
						true)),
				Named.of("guarded, empty", new Guarded(0, -1, new long[0], new long[0], List.of(), false)),
				Named.of("update commit with its id", new Guarded(1, 0, new long[] {3}, new long[] {77},
						List.of(new Delete(3, keys("a"), RequestTypeId.NOTHING)), true, -5, null)),
				Named.of("chained commit", new Guarded(1, 0, new long[] {3}, new long[] {77},
						List.of(new Put(3, keys("a"), buf("1"), RequestTypeId.NOTHING)), true, 42,
						Guard.of(3, keys("a", "b"), buf("previous")))),
				Named.of("chained commit after an absent value", new Guarded(1, 0, new long[] {3}, new long[] {77},
						List.of(new Put(3, keys("a"), buf("1"), RequestTypeId.NOTHING)), true, 42,
						Guard.of(3, keys("a"), null))),
				Named.of("chained commit after an empty value", new Guarded(1, 0, new long[] {3}, new long[] {77},
						List.of(new Put(3, keys("a"), buf("1"), RequestTypeId.NOTHING)), true, 42,
						Guard.of(3, keys(), Buf.wrap()))),
				Named.of("genesis", new Genesis("cluster-1", false)),
				Named.of("genesis, seeded", new Genesis(UNICODE, true)));
	}

	@ParameterizedTest
	@MethodSource("entries")
	void roundTripsEveryEntry(LogEntry entry) {
		byte[] encoded = LogEntryCodec.encode(entry);
		assertEquals(LogEntryCodec.VERSION, encoded[0]);
		LogEntry decoded = LogEntryCodec.decode(encoded);
		assertSameEntry(entry, decoded);
		assertArrayEquals(encoded, LogEntryCodec.encode(decoded), "re-encoding a decoded entry changed its bytes");
	}

	@ParameterizedTest
	@MethodSource("entries")
	void encodingIsDeterministic(LogEntry entry) {
		assertArrayEquals(LogEntryCodec.encode(entry), LogEntryCodec.encode(entry));
	}

	@ParameterizedTest
	@MethodSource("entries")
	void computedSizesMatchTheEncoding(LogEntry entry) {
		int encoded = LogEntryCodec.encodeEntryBody(entry).length;
		switch (entry) {
			case LogEntry.Write write -> assertEquals(encoded, LogEntryCodec.writeSize(write));
			case Guarded guarded -> {
				long size = LogEntryCodec.GUARDED_BASE_BYTES
						+ (long) guarded.conflictHashes().length * LogEntryCodec.CONFLICT_KEY_BYTES;
				for (LogEntry.Write write : guarded.writes()) {
					size += LogEntryCodec.writeSize(write);
				}
				if (guarded.guard() != null) {
					size += LogEntryCodec.guardSize(guarded.guard());
				}
				assertEquals(encoded, size);
			}
			default -> {}
		}
	}

	@Test
	void equalEntriesFromDifferentBuffersEncodeEqually() {
		byte[] backing = "__key__value__".getBytes(UTF_8);
		var sliced = new Put(1, new Keys(Buf.wrap(backing, 2, 5)), Buf.wrap(backing, 7, 12), RequestTypeId.NOTHING);
		var fresh = new Put(1, keys("key"), buf("value"), RequestTypeId.NOTHING);
		assertEquals(fresh, sliced);
		assertArrayEquals(LogEntryCodec.encode(fresh), LogEntryCodec.encode(sliced));
	}

	@Test
	void distinguishesNullFromEmptyValues() {
		var withNull = (Put) roundTrip(new Put(1, keys("k"), null, RequestTypeId.NOTHING));
		var withEmpty = (Put) roundTrip(new Put(1, keys("k"), Buf.wrap(), RequestTypeId.NOTHING));
		assertNull(withNull.value());
		assertEquals(Buf.wrap(), withEmpty.value());
	}

	@Test
	void distinguishesNoKeysFromOneEmptyKey() {
		var none = (Delete) roundTrip(new Delete(1, new Keys(), RequestTypeId.NOTHING));
		var oneEmpty = (Delete) roundTrip(new Delete(1, keys(""), RequestTypeId.NOTHING));
		assertEquals(0, none.keys().keys().length);
		assertEquals(1, oneEmpty.keys().keys().length);
	}

	@ParameterizedTest
	@EnumSource(RequestTypeId.class)
	void roundTripsEveryRequestType(RequestTypeId requestType) {
		var entry = new Delete(1, keys("k"), requestType);
		assertEquals(entry, roundTrip(entry));
	}

	@Test
	void decodedEntryDoesNotShareTheInputArray() {
		var entry = new Put(1, keys("key"), buf("value"), RequestTypeId.NOTHING);
		byte[] encoded = LogEntryCodec.encode(entry);
		LogEntry decoded = LogEntryCodec.decode(encoded);
		Arrays.fill(encoded, (byte) 0);
		assertEquals(entry, decoded);
	}

	@Test
	void rejectsStringsThatAreNotValidUnicode() {
		assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.encode(new DeleteColumnIfExists("bad\uD800")));
	}

	@ParameterizedTest
	@MethodSource("entries")
	void rejectsEveryTruncation(LogEntry entry) {
		byte[] encoded = LogEntryCodec.encode(entry);
		if (encoded.length > 4096) {
			// Every prefix of the large sample would take quadratic time; its structure is covered by the others.
			encoded = Arrays.copyOf(encoded, 4096);
		}
		for (int length = 0; length < encoded.length; length++) {
			assertRejected(Arrays.copyOf(encoded, length), "truncated to " + length + " bytes");
		}
	}

	@ParameterizedTest
	@MethodSource("entries")
	void rejectsTrailingBytes(LogEntry entry) {
		byte[] encoded = LogEntryCodec.encode(entry);
		assertRejected(Arrays.copyOf(encoded, encoded.length + 1), "one trailing byte");
	}

	@Test
	void roundTripsABatchOfEveryEntry() {
		var requests = new ArrayList<Request>();
		long sequence = 5;
		for (var named : entries().toList()) {
			requests.add(new Request(-77, sequence++, 5, named.getPayload()));
		}
		byte[] encoded = LogEntryCodec.encodeBatch(requests);
		var decoded = LogEntryCodec.decodeBatch(encoded);
		assertEquals(requests.size(), decoded.size());
		for (int i = 0; i < requests.size(); i++) {
			assertEquals(requests.get(i).proposerId(), decoded.get(i).proposerId());
			assertEquals(requests.get(i).sequence(), decoded.get(i).sequence());
			assertEquals(requests.get(i).completedBelow(), decoded.get(i).completedBelow());
			assertSameEntry(requests.get(i).entry(), decoded.get(i).entry());
		}
		assertArrayEquals(encoded, LogEntryCodec.encodeBatch(decoded));
	}

	@Test
	void rejectsMalformedBatches() {
		var request = new Request(1, 3, 2, new Genesis("x", false));
		byte[] encoded = LogEntryCodec.encodeBatch(List.of(request));
		for (int length = 0; length < encoded.length; length++) {
			byte[] truncated = Arrays.copyOf(encoded, length);
			assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.decodeBatch(truncated), "length " + length);
		}
		assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.decodeBatch(Arrays.copyOf(encoded,
				encoded.length + 1)));
		// An empty batch, a request numbered below the batch's completedBelow, mixed proposers.
		var empty = ByteBuffer.allocate(21).put(LogEntryCodec.BATCH_VERSION).putLong(1).putLong(0).putInt(0).array();
		assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.decodeBatch(empty));
		var below = encoded.clone();
		ByteBuffer.wrap(below).putLong(9, 4);
		assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.decodeBatch(below));
		assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.encodeBatch(List.of(request,
				new Request(2, 4, 2, new Genesis("x", false)))));
		// A single entry is not a batch.
		assertThrows(IllegalArgumentException.class, () -> LogEntryCodec.decodeBatch(LogEntryCodec.encode(
				new Genesis("x", false))));
	}

	@Test
	void rejectsUnknownVersion() {
		byte[] encoded = LogEntryCodec.encode(new DeleteColumn(5));
		for (byte version : new byte[] {0, 2, -1}) {
			encoded[0] = version;
			assertRejected(encoded, "version " + version);
		}
	}

	@Test
	void rejectsUnknownTag() {
		byte[] encoded = LogEntryCodec.encode(new DeleteColumn(5));
		for (byte tag : new byte[] {0, 14, 127, -1}) {
			encoded[1] = tag;
			assertRejected(encoded, "tag " + tag);
		}
	}

	@Test
	void rejectsUnknownRequestType() {
		// version, tag, column id (8), key count (4), request type
		byte[] encoded = LogEntryCodec.encode(new Delete(7, new Keys(), RequestTypeId.NOTHING));
		assertEquals(15, encoded.length);
		for (byte code : new byte[] {(byte) RequestTypeId.values().length, 100, -1}) {
			encoded[14] = code;
			assertRejected(encoded, "request type code " + code);
		}
	}

	@Test
	void rejectsNegativeAndOversizedLengths() {
		// version, tag, string length (4), "abc"
		byte[] string = LogEntryCodec.encode(new DeleteColumnIfExists("abc"));
		for (int length : new int[] {-1, -2, 4, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
			assertRejected(withInt(string, 2, length), "string length " + length);
		}
		// version, tag, column id (8), key count (4), value length (4), request type
		byte[] value = LogEntryCodec.encode(new Put(1, new Keys(), Buf.wrap(), RequestTypeId.NOTHING));
		for (int length : new int[] {-2, 2, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
			assertRejected(withInt(value, 14, length), "value length " + length);
		}
	}

	@Test
	void rejectsAbsurdListSizesWithoutAllocating() {
		// version, tag, column id (8), list size (4), request type
		byte[] keyList = LogEntryCodec.encode(new DeleteMulti(1, List.of(), RequestTypeId.NOTHING));
		// version, tag, column id (8), key segment count (4), request type
		byte[] segments = LogEntryCodec.encode(new Delete(1, new Keys(), RequestTypeId.NOTHING));
		// version, tag, epoch (8), snapshot index (8), conflict count (4), write count (4), single result, update id
		// (8), guard flag
		byte[] guarded = LogEntryCodec.encode(new Guarded(1, 2, new long[0], new long[0], List.of(), false));
		for (int size : new int[] {-1, 1, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
			assertRejected(withInt(keyList, 10, size), "key list size " + size);
			assertRejected(withInt(segments, 10, size), "key segment count " + size);
			assertRejected(withInt(guarded, 18, size), "conflict count " + size);
			assertRejected(withInt(guarded, 22, size), "write count " + size);
		}
	}

	@Test
	void rejectsNestedOversizedListsWithoutReservingMemoryForThem() {
		var write = new DeleteMulti(1, List.of(new Keys()), RequestTypeId.NOTHING);
		byte[] encoded = LogEntryCodec.encode(new Guarded(1, 2, new long[0], new long[0], List.of(write), false));
		// version, tag, epoch (8), snapshot index (8), conflict count (4), write count (4), write tag,
		// column id (8), key list size (4), key segment count (4), request type, single result, update id (8), guard
		// flag
		assertEquals(54, encoded.length);
		int size = 1 << 22;
		var buffer = ByteBuffer.allocate(size);
		buffer.put(encoded, 0, 43);
		// Each list claims as much as the bytes left allow, then the first key segment has length -1
		buffer.putInt(22, size - 26);
		buffer.putInt(35, (size - 39) / Integer.BYTES);
		buffer.putInt(39, (size - 43) / Integer.BYTES);
		while (buffer.hasRemaining()) {
			buffer.put((byte) -1);
		}
		byte[] malformed = buffer.array();
		long allocated = ResultCodecTest.allocatedBytes(
				() -> assertRejected(malformed, "nested lists claiming the whole input"));
		// Only the innermost key segment array may be sized from the input: at most one reference per 4 bytes
		assertTrue(allocated < 3L * size, "Rejecting " + size + " bytes allocated " + allocated + " bytes");
	}

	@Test
	void rejectsInvalidBoolean() {
		byte[] encoded = LogEntryCodec.encode(new Genesis("c", false));
		encoded[encoded.length - 1] = 2;
		assertRejected(encoded, "boolean 2");
	}

	@Test
	void rejectsInvalidUtf8() {
		// version, tag, string length (4), "abc"
		byte[] encoded = LogEntryCodec.encode(new DeleteColumnIfExists("abc"));
		encoded[6] = (byte) 0xFF;
		assertRejected(encoded, "invalid UTF-8");
	}

	@Test
	void rejectsInvalidColumnSchemas() {
		// version, tag, name length (4), "c", key count (4), first key length (4), ...
		var schema = ColumnSchema.of(IntList.of(4), ObjectList.of(), true);
		byte[] invalidLength = LogEntryCodec.encode(new CreateColumn("c", schema));
		assertRejected(withInt(invalidLength, 11, 0), "fixed key of length 0");

		// ..., first key length (4), hash type count (4), hash type ordinal, ...
		byte[] hashed = LogEntryCodec.encode(new CreateColumn("c",
				ColumnSchema.of(IntList.of(), ObjectList.of(ColumnHashType.XXHASH8), true)));
		assertEquals(ColumnHashType.XXHASH8.bytesSize(), ByteBuffer.wrap(hashed, 11, 4).getInt());
		assertRejected(withInt(hashed, 11, 4), "hash type with a wrong key length");
		hashed[19] = (byte) ColumnHashType.values().length;
		assertRejected(hashed, "unknown hash type");
	}

	@Test
	void rejectsGuardedWritesThatAreNotDataWrites() {
		var guarded = new Guarded(1, 2, new long[0], new long[0], List.of(new DeleteRange(1, null, null)), false);
		// version, tag, epoch (8), snapshot index (8), conflict count (4), write count (4), first write tag
		byte[] encoded = LogEntryCodec.encode(guarded);
		byte guardedTag = encoded[1];
		byte deleteColumnTag = LogEntryCodec.encode(new DeleteColumn(1))[1];
		for (byte tag : new byte[] {guardedTag, deleteColumnTag}) {
			encoded[26] = tag;
			assertRejected(encoded, "nested tag " + tag);
		}
	}

	@Test
	void rejectsSingleResultWithSeveralWrites() {
		var write = new Delete(1, keys("k"), RequestTypeId.NOTHING);
		var guarded = new Guarded(1, 2, new long[0], new long[0], List.of(write, write), false);
		byte[] encoded = LogEntryCodec.encode(guarded);
		encoded[encoded.length - 1] = 1;
		assertRejected(encoded, "single result with two writes");
	}

	@Test
	void corruptedInputIsDecodedOrRejectedNeverCrashes() {
		var random = new Random(0x5EED);
		var samples = entries().map(Named::getPayload).filter(e -> LogEntryCodec.encode(e).length < 4096).toList();
		for (LogEntry entry : samples) {
			byte[] encoded = LogEntryCodec.encode(entry);
			for (int attempt = 0; attempt < 300; attempt++) {
				byte[] corrupted = encoded.clone();
				int changes = 1 + random.nextInt(3);
				for (int i = 0; i < changes; i++) {
					corrupted[random.nextInt(corrupted.length)] = (byte) random.nextInt(256);
				}
				try {
					LogEntryCodec.decode(corrupted);
				} catch (IllegalArgumentException expected) {
					// rejected cleanly
				} catch (Throwable unexpected) {
					fail("Corrupted " + entry + " threw " + unexpected, unexpected);
				}
			}
		}
	}

	private static LogEntry roundTrip(LogEntry entry) {
		return LogEntryCodec.decode(LogEntryCodec.encode(entry));
	}

	private static void assertRejected(byte[] data, String description) {
		try {
			LogEntry decoded = LogEntryCodec.decode(data);
			fail("Decoded malformed input (" + description + ") as " + decoded);
		} catch (IllegalArgumentException expected) {
			assertFalse(expected.getMessage() == null || expected.getMessage().isEmpty(), "missing message");
		} catch (Throwable unexpected) {
			fail("Malformed input (" + description + ") threw " + unexpected, unexpected);
		}
	}

	private static byte[] withInt(byte[] data, int offset, int value) {
		byte[] copy = data.clone();
		ByteBuffer.wrap(copy).putInt(offset, value);
		return copy;
	}

	@Test
	void aGuardHoldsTheSha256OfTheValue() {
		// The digest of "abc" from FIPS 180-2.
		var guard = Guard.of(1, keys("k"), buf("abc"));
		assertEquals(3, guard.length());
		assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
				java.util.HexFormat.of().formatHex(guard.sha256()));
		var empty = Guard.of(1, keys("k"), Buf.wrap());
		assertEquals(0, empty.length());
		assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				java.util.HexFormat.of().formatHex(empty.sha256()));
		var absent = Guard.of(1, keys("k"), null);
		assertEquals(-1, absent.length());
		assertEquals(null, absent.sha256());
	}

	@Test
	void aGuardMatchesOnlyItsValue() {
		var value = Guard.of(1, keys("k"), buf("abc"));
		assertTrue(value.matches(buf("abc")));
		assertFalse(value.matches(buf("abd")), "same length, other bytes");
		assertFalse(value.matches(buf("abcd")));
		assertFalse(value.matches(Buf.wrap()));
		assertFalse(value.matches(null));

		var absent = Guard.of(1, keys("k"), null);
		assertTrue(absent.matches(null));
		assertFalse(absent.matches(Buf.wrap()), "absent is not empty");

		var empty = Guard.of(1, keys("k"), Buf.wrap());
		assertTrue(empty.matches(Buf.wrap()));
		assertFalse(empty.matches(null), "empty is not absent");

		// A value inside a larger array digests only its own bytes.
		byte[] backing = "__abc__".getBytes(UTF_8);
		assertTrue(value.matches(Buf.wrap(backing, 2, 5)));
		assertEquals(java.util.HexFormat.of().formatHex(value.sha256()),
				java.util.HexFormat.of().formatHex(Guard.of(1, keys("k"), Buf.wrap(backing, 2, 5)).sha256()));
	}

	@Test
	void rejectsInconsistentGuardsAndUpdateIds() {
		var guard = Guard.of(1, keys("k"), buf("abc"));
		var write = new Put(1, keys("k"), buf("v"), RequestTypeId.NOTHING);
		assertThrows(IllegalArgumentException.class,
				() -> new Guarded(1, 2, new long[0], new long[0], List.of(write), true, 0, guard), "a guard without an id");
		assertThrows(IllegalArgumentException.class,
				() -> new Guarded(1, 2, new long[0], new long[0], List.of(write), false, 7, null), "not a single result");
		assertThrows(IllegalArgumentException.class, () -> new Guard(1, keys("k"), 3, null));
		assertThrows(IllegalArgumentException.class, () -> new Guard(1, keys("k"), -1, new byte[32]));
		assertThrows(IllegalArgumentException.class, () -> new Guard(1, keys("k"), 3, new byte[31]));
		assertThrows(IllegalArgumentException.class, () -> new Guard(1, keys("k"), -2, null));

		// A decoder never returns one of those either: a guard without an update id is malformed.
		byte[] encoded = LogEntryCodec.encode(new Guarded(1, 2, new long[0], new long[0], List.of(write), true, 7, guard));
		int updateIdAt = encoded.length - (int) LogEntryCodec.guardSize(guard) - 1 - Long.BYTES;
		byte[] noId = encoded.clone();
		ByteBuffer.wrap(noId).putLong(updateIdAt, 0);
		assertRejected(noId, "guard without an update id");
	}

	@Test
	void everyTruncationOfAChainedCommitIsRejected() {
		byte[] encoded = LogEntryCodec.encode(new Guarded(1, 2, new long[] {1}, new long[] {2},
				List.of(new Put(1, keys("k"), buf("v"), RequestTypeId.NOTHING)), true, 7, Guard.of(1, keys("k"), buf("abc"))));
		for (int length = 0; length < encoded.length; length++) {
			assertRejected(java.util.Arrays.copyOf(encoded, length), "truncated to " + length);
		}
	}

	private static void assertSameEntry(LogEntry expected, LogEntry actual) {
		switch (expected) {
			case Guarded guarded -> {
				var decoded = assertInstanceOf(Guarded.class, actual);
				assertEquals(guarded.leaderEpoch(), decoded.leaderEpoch());
				assertEquals(guarded.snapshotIndex(), decoded.snapshotIndex());
				assertArrayEquals(guarded.conflictColumns(), decoded.conflictColumns());
				assertArrayEquals(guarded.conflictHashes(), decoded.conflictHashes());
				assertEquals(guarded.writes(), decoded.writes());
				assertEquals(guarded.singleResult(), decoded.singleResult());
				assertEquals(guarded.updateId(), decoded.updateId());
				if (guarded.guard() == null) {
					assertEquals(null, decoded.guard());
				} else {
					assertEquals(guarded.guard().columnId(), decoded.guard().columnId());
					assertEquals(guarded.guard().keys(), decoded.guard().keys());
					assertEquals(guarded.guard().length(), decoded.guard().length());
					assertArrayEquals(guarded.guard().sha256(), decoded.guard().sha256());
				}
			}
			case UploadMergeOperator upload -> {
				var decoded = assertInstanceOf(UploadMergeOperator.class, actual);
				assertEquals(upload.name(), decoded.name());
				assertEquals(upload.className(), decoded.className());
				assertArrayEquals(upload.jarData(), decoded.jarData());
			}
			default -> assertEquals(expected, actual);
		}
	}

	private static PutMulti largePutMulti() {
		var keys = new ArrayList<Keys>();
		var values = new ArrayList<Buf>();
		for (int i = 0; i < 20_000; i++) {
			keys.add(keys("bucket-" + (i % 17), "key-" + i));
			values.add(i % 5 == 0 ? Buf.wrap() : buf("value-" + i));
		}
		return new PutMulti(9, keys, values, RequestTypeId.PREVIOUS);
	}

	private static Keys keys(String... segments) {
		return new Keys(Arrays.stream(segments).map(LogEntryCodecTest::buf).toArray(Buf[]::new));
	}

	private static Buf buf(String value) {
		return Buf.wrap(value.getBytes(UTF_8));
	}
}
