package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionConfig;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A deterministic model-based test that sends the same state transition sequence through
 * every transport and through both the synchronous and asynchronous API adapters.
 */
@Timeout(90)
class DatabaseStateMachineTest {

	private static final long SEED = 0x5EED_C0FFEE_2026L;
	private static final int KEY_SPACE = 32;
	private static final int STEPS = 96;

	@TempDir
	Path tempDir;

	static Stream<Arguments> allImplementations() {
		return DBTest.allImplementations();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void deterministicMixedOperationsMatchReferenceModel(String name, ConnectionConfig connection) throws Exception {
		Path configFile = tempDir.resolve("state-machine.conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		Path databasePath = tempDir.resolve("db-" + name.replaceAll("[^a-zA-Z0-9]", "-"));

		try (var embedded = new EmbeddedConnection(databasePath, "state-machine", configFile);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			long columnId = api.createColumn("state", ColumnSchema.of(
					IntList.of(Long.BYTES), ObjectList.of(), true));
			var expected = new TreeMap<Long, Long>();
			var random = new SplittableRandom(SEED);
			exerciseMutationResultModes(api, columnId, expected, name + ", mutation result matrix");

			for (int step = 0; step < STEPS; step++) {
				String context = name + ", seed=" + SEED + ", step=" + step;
				int operation = random.nextInt(8);
				try {
					switch (operation) {
						case 0 -> putOne(api, columnId, expected, random, context);
						case 1 -> deleteOne(api, columnId, expected, random, context);
						case 2 -> putMany(api, columnId, expected, random, context);
						case 3 -> deleteMany(api, columnId, expected, random, context);
						case 4 -> deleteRange(api, columnId, expected, random);
						case 5 -> transact(api, columnId, expected, random, context);
						case 6 -> verifyRandomPoint(api, columnId, expected, random, context);
						case 7 -> api.flush();
						default -> throw new AssertionError("Unreachable operation " + operation);
					}
				} catch (Throwable failure) {
					throw new AssertionError(context + ", operation=" + operationName(operation), failure);
				}

				if (step % 8 == 0) {
					verifyRandomPoints(api, columnId, expected, random, context);
				}
				if (step % 16 == 0) {
					verifyFullRanges(api, columnId, expected, context);
				}
			}

			verifyEveryPoint(api, columnId, expected, name + ", final point verification");
			verifyFullRanges(api, columnId, expected, name + ", final range verification");
		}
	}

	private static void exerciseMutationResultModes(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			String context) {
		api.putMulti(0, columnId,
				List.of(key(0), key(1)),
				List.of(value(10), value(11)),
				RequestType.none());
		expected.put(0L, 10L);
		expected.put(1L, 11L);

		List<Buf> previous = api.putMulti(0, columnId,
				List.of(key(0), key(2)),
				List.of(value(20), value(22)),
				RequestType.previous());
		assertEquals(2, previous.size(), context + ", previous result count");
		assertValue(10L, previous.get(0), context + ", previous existing value");
		assertValue(null, previous.get(1), context + ", previous missing value");
		expected.put(0L, 20L);
		expected.put(2L, 22L);

		List<Delta<Buf>> deltas = api.putMulti(0, columnId,
				List.of(key(0), key(3)),
				List.of(value(30), value(33)),
				RequestType.delta());
		assertEquals(2, deltas.size(), context + ", delta result count");
		assertValue(20L, deltas.get(0).previous(), context + ", delta existing previous");
		assertValue(30L, deltas.get(0).current(), context + ", delta existing current");
		assertValue(null, deltas.get(1).previous(), context + ", delta missing previous");
		assertValue(33L, deltas.get(1).current(), context + ", delta missing current");
		expected.put(0L, 30L);
		expected.put(3L, 33L);

		List<Boolean> changed = api.putMulti(0, columnId,
				List.of(key(0), key(4)),
				List.of(value(30), value(44)),
				RequestType.changed());
		assertEquals(List.of(false, true), changed, context + ", changed results");
		expected.put(4L, 44L);

		List<Boolean> previousPresence = api.putMulti(0, columnId,
				List.of(key(0), key(5)),
				List.of(value(31), value(55)),
				RequestType.previousPresence());
		assertEquals(List.of(true, false), previousPresence, context + ", previous-presence results");
		expected.put(0L, 31L);
		expected.put(5L, 55L);

		List<Buf> deletedPrevious = api.deleteMulti(0, columnId,
				List.of(key(0), key(6)), RequestType.previous());
		assertEquals(2, deletedPrevious.size(), context + ", delete previous result count");
		assertValue(31L, deletedPrevious.get(0), context + ", delete existing previous");
		assertValue(null, deletedPrevious.get(1), context + ", delete missing previous");
		expected.remove(0L);

		List<Boolean> deletedPresence = api.deleteMulti(0, columnId,
				List.of(key(1), key(7)), RequestType.previousPresence());
		assertEquals(List.of(true, false), deletedPresence, context + ", delete previous-presence results");
		expected.remove(1L);

		api.deleteMulti(0, columnId, List.of(key(2), key(8)), RequestType.none());
		expected.remove(2L);

		api.put(0, columnId, key(9), value(99), RequestType.none());
		assertTrue(api.delete(0, columnId, key(9), RequestType.previousPresence()),
				context + ", single delete previous-presence");
		api.put(0, columnId, key(10), value(100), RequestType.none());
		assertValue(100L, api.delete(0, columnId, key(10), RequestType.previous()),
				context + ", single delete previous");
		api.put(0, columnId, key(11), value(111), RequestType.none());
		api.delete(0, columnId, key(11), RequestType.none());
	}

	private static String operationName(int operation) {
		return switch (operation) {
			case 0 -> "put";
			case 1 -> "delete";
			case 2 -> "putMulti";
			case 3 -> "deleteMulti";
			case 4 -> "deleteRange";
			case 5 -> "transaction";
			case 6 -> "get";
			case 7 -> "flush";
			default -> "unknown(" + operation + ")";
		};
	}

	private static void putOne(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			SplittableRandom random,
			String context) {
		long key = random.nextLong(KEY_SPACE);
		long value = random.nextLong();
		Long previousExpected = expected.put(key, value);
		Buf previousActual = api.put(0, columnId, key(key), value(value), RequestType.previous());
		assertValue(previousExpected, previousActual, context + ", single put key=" + key);
	}

	private static void deleteOne(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			SplittableRandom random,
			String context) {
		long key = random.nextLong(KEY_SPACE);
		Long previousExpected = expected.remove(key);
		Buf previousActual = api.delete(0, columnId, key(key), RequestType.previous());
		assertValue(previousExpected, previousActual, context + ", single delete key=" + key);
	}

	private static void putMany(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			SplittableRandom random,
			String context) {
		int count = random.nextInt(2, 6);
		var keys = new ArrayList<Keys>(count);
		var values = new ArrayList<Buf>(count);
		var previousPresence = new ArrayList<Boolean>(count);
		for (int index = 0; index < count; index++) {
			long key = random.nextLong(KEY_SPACE);
			long value = random.nextLong();
			keys.add(key(key));
			values.add(value(value));
			previousPresence.add(expected.put(key, value) != null);
		}

		List<Boolean> actual = api.putMulti(0, columnId, keys, values, RequestType.previousPresence());
		assertEquals(previousPresence, actual, context + ", putMulti previous-presence mismatch");
	}

	private static void deleteMany(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			SplittableRandom random,
			String context) {
		int count = random.nextInt(2, 6);
		var keys = new ArrayList<Keys>(count);
		var previousPresence = new ArrayList<Boolean>(count);
		for (int index = 0; index < count; index++) {
			long key = random.nextLong(KEY_SPACE);
			keys.add(key(key));
			previousPresence.add(expected.remove(key) != null);
		}

		List<Boolean> actual = api.deleteMulti(0, columnId, keys, RequestType.previousPresence());
		assertEquals(previousPresence, actual, context + ", deleteMulti previous-presence mismatch");
	}

	private static void deleteRange(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			SplittableRandom random) {
		Long start = random.nextInt(5) == 0 ? null : random.nextLong(KEY_SPACE + 1L);
		Long end = random.nextInt(5) == 0 ? null : random.nextLong(KEY_SPACE + 1L);
		if (start != null && end != null && start > end) {
			long swap = start;
			start = end;
			end = swap;
		}
		Long finalStart = start;
		Long finalEnd = end;

		api.deleteRange(columnId, start == null ? null : key(start), end == null ? null : key(end));
		expected.entrySet().removeIf(entry ->
				(finalStart == null || entry.getKey() >= finalStart)
						&& (finalEnd == null || entry.getKey() < finalEnd));
	}

	private static void transact(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			SplittableRandom random,
			String context) {
		long transactionId = api.openTransaction(30_000);
		var transactionExpected = new TreeMap<>(expected);
		var touchedKeys = new ArrayList<Long>();
		boolean closed = false;
		try {
			int operations = random.nextInt(2, 7);
			for (int operation = 0; operation < operations; operation++) {
				long key = random.nextLong(KEY_SPACE);
				touchedKeys.add(key);
				if (random.nextBoolean()) {
					long value = random.nextLong();
					Long previousExpected = transactionExpected.put(key, value);
					Buf previousActual = api.put(
							transactionId, columnId, key(key), value(value), RequestType.previous());
					assertValue(previousExpected, previousActual,
							context + ", transactional put key=" + key);
				} else {
					Long previousExpected = transactionExpected.remove(key);
					Buf previousActual = api.delete(
							transactionId, columnId, key(key), RequestType.previous());
					assertValue(previousExpected, previousActual,
							context + ", transactional delete key=" + key);
				}
			}

			long observedKey = touchedKeys.getLast();
			assertValue(transactionExpected.get(observedKey),
					api.get(transactionId, columnId, key(observedKey), RequestType.current()),
					context + ", transaction-local visibility key=" + observedKey);
			assertValue(expected.get(observedKey),
					api.get(0, columnId, key(observedKey), RequestType.current()),
					context + ", transaction isolation key=" + observedKey);

			boolean commit = random.nextInt(3) != 0;
			assertTrue(api.closeTransaction(transactionId, commit),
					context + ", uncontended transaction close should succeed");
			closed = true;
			if (commit) {
				expected.clear();
				expected.putAll(transactionExpected);
			}
		} finally {
			if (!closed) {
				api.closeTransaction(transactionId, false);
			}
		}
	}

	private static void verifyRandomPoints(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			SplittableRandom random,
			String context) {
		for (int index = 0; index < 6; index++) {
			verifyRandomPoint(api, columnId, expected, random, context);
		}
	}

	private static void verifyRandomPoint(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			SplittableRandom random,
			String context) {
		long key = random.nextLong(KEY_SPACE);
		Long expectedValue = expected.get(key);
		assertValue(expectedValue, api.get(0, columnId, key(key), RequestType.current()),
				context + ", point read key=" + key);
		assertEquals(expectedValue != null,
				api.get(0, columnId, key(key), RequestType.exists()),
				context + ", existence read key=" + key);
	}

	private static void verifyEveryPoint(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			String context) {
		for (long key = 0; key < KEY_SPACE; key++) {
			assertValue(expected.get(key), api.get(0, columnId, key(key), RequestType.current()),
					context + ", key=" + key);
		}
	}

	private static void verifyFullRanges(RocksDBAPI api,
			long columnId,
			TreeMap<Long, Long> expected,
			String context) {
		List<ObservedValue> expectedForward = expected.entrySet().stream()
				.map(entry -> new ObservedValue(entry.getKey(), entry.getValue()))
				.toList();
		List<ObservedValue> expectedReverse = expected.descendingMap().entrySet().stream()
				.map(entry -> new ObservedValue(entry.getKey(), entry.getValue()))
				.toList();

		assertEquals(expectedForward, readRange(api, columnId, false),
				context + ", forward full-range mismatch");
		assertEquals(expectedReverse, readRange(api, columnId, true),
				context + ", reverse full-range mismatch");
	}

	private static List<ObservedValue> readRange(RocksDBAPI api, long columnId, boolean reverse) {
		try (Stream<KV> range = api.getRange(
				0, columnId, null, null, reverse, RequestType.allInRange(), 10_000)) {
			return range.map(kv -> new ObservedValue(readKey(kv.keys()), readValue(kv.value()))).toList();
		}
	}

	private static Keys key(long key) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(key).array()));
	}

	private static Buf value(long value) {
		return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}

	private static long readKey(Keys keys) {
		assertEquals(1, keys.keys().length, "state-machine key must have one component");
		return ByteBuffer.wrap(keys.keys()[0].toByteArray()).getLong();
	}

	private static long readValue(Buf value) {
		return ByteBuffer.wrap(value.toByteArray()).getLong();
	}

	private static void assertValue(Long expected, Buf actual, String context) {
		if (expected == null) {
			assertNull(actual, context);
		} else {
			assertNotNull(actual, context);
			assertEquals(expected.longValue(), readValue(actual), context);
		}
	}

	private record ObservedValue(long key, long value) {
	}
}
