package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class CdcRandomizedTest {

	private static final String SEED_PROPERTY = "rockserver.test.cdc.random.seed";
	private static final String ITERATIONS_PROPERTY = "rockserver.test.cdc.random.iterations";
	private static final String KEY_RANGE_PROPERTY = "rockserver.test.cdc.random.key-range";
	private static final long DEFAULT_SEED = 42L;
	private static final int DEFAULT_ITERATIONS = 5_000;
	private static final int DEFAULT_KEY_RANGE = 100;
	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(10);
	private static final int MAX_TRANSACTION_MUTATIONS = 4;
	private static final int MAX_VALUE_BYTES = 64;
	private static final int MAX_POLL_EVENTS = 127;

	@TempDir
	Path tempDir;

	private EmbeddedDB db;
	private long columnId;
	private long subscriptionStart;
	private final String subId = "rand-sub";

	@BeforeEach
	void setUp() throws IOException, RocksDBException {
		db = new EmbeddedDB(tempDir, "test-db-rand", null);
		var schema = ColumnSchema.of(
				IntArrayList.of(Integer.BYTES),
				new ObjectArrayList<>(),
				true,
				null,
				null,
				null
		);
		columnId = db.createColumn("data", schema);
		subscriptionStart = db.cdcCreate(subId, null, List.of(columnId), false, java.util.OptionalLong.empty());
	}

	@AfterEach
	void tearDown() throws IOException {
		if (db != null) {
			db.closeTesting();
		}
	}

	@Test
	void testRandomizedOpsVerification() {
		runRandomizedTest(false);
	}

	@Test
	void testRandomizedOpsWithCommitVerification() {
		runRandomizedTest(true);
	}

	private void runRandomizedTest(boolean commitPeriodically) {
		long seed = configuredSeed();
		int iterations = positiveIntProperty(ITERATIONS_PROPERTY, DEFAULT_ITERATIONS);
		int keyRange = positiveIntProperty(KEY_RANGE_PROPERTY, DEFAULT_KEY_RANGE);
		var expectedEvents = new ArrayList<Mutation>(iterations);
		var expectedState = new HashMap<Integer, byte[]>();

		generateWorkload(new Random(seed), iterations, keyRange, expectedEvents, expectedState, seed);
		replayAndVerify(seed, commitPeriodically, keyRange, expectedEvents, expectedState);
	}

	private void generateWorkload(Random random,
	                              int iterations,
	                              int keyRange,
	                              List<Mutation> expectedEvents,
	                              Map<Integer, byte[]> expectedState,
	                              long seed) {
		for (int operationIndex = 0; operationIndex < iterations; operationIndex++) {
			int operation = random.nextInt(100);
			if (operation < 55) {
				var mutation = randomMutation(random, operationIndex, 0, keyRange, CDCEvent.Op.PUT);
				executeMutation(0, mutation);
				acceptMutation(mutation, expectedEvents, expectedState);
			} else if (operation < 75) {
				var mutation = randomMutation(random, operationIndex, 0, keyRange, CDCEvent.Op.DELETE);
				executeMutation(0, mutation);
				acceptMutation(mutation, expectedEvents, expectedState);
			} else {
				boolean commit = operation < 92;
				int mutationCount = 1 + random.nextInt(MAX_TRANSACTION_MUTATIONS);
				var mutations = new ArrayList<Mutation>(mutationCount);
				for (int mutationIndex = 0; mutationIndex < mutationCount; mutationIndex++) {
					CDCEvent.Op op = random.nextInt(4) == 0 ? CDCEvent.Op.DELETE : CDCEvent.Op.PUT;
					mutations.add(randomMutation(random, operationIndex, mutationIndex, keyRange, op));
				}
				executeTransaction(mutations, commit, expectedEvents, expectedState, seed);
			}
		}
	}

	private Mutation randomMutation(Random random,
	                                int operationIndex,
	                                int mutationIndex,
	                                int keyRange,
	                                CDCEvent.Op op) {
		int key = random.nextInt(keyRange);
		byte[] value = null;
		if (op == CDCEvent.Op.PUT) {
			value = new byte[random.nextInt(MAX_VALUE_BYTES + 1)];
			random.nextBytes(value);
		}
		return new Mutation(operationIndex, mutationIndex, op, key, value);
	}

	private void executeTransaction(List<Mutation> mutations,
	                                boolean commit,
	                                List<Mutation> expectedEvents,
	                                Map<Integer, byte[]> expectedState,
	                                long seed) {
		long transactionId = db.openTransaction( java.time.Duration.ofMillis(5_000));
		boolean closed = false;
		try {
			for (Mutation mutation : mutations) {
				executeMutation(transactionId, mutation);
			}
			assertTrue(db.closeTransaction(transactionId, commit),
					() -> "Transaction disappeared before close; " + mutationContext(seed, mutations.getFirst()));
			closed = true;
		} finally {
			if (!closed) {
				db.closeTransaction(transactionId, false);
			}
		}

		if (commit) {
			for (Mutation mutation : mutations) {
				acceptMutation(mutation, expectedEvents, expectedState);
			}
		}
	}

	private void executeMutation(long transactionId, Mutation mutation) {
		var key = keys(mutation.key());
		switch (mutation.op()) {
			case PUT -> db.put(transactionId,
					columnId,
					key,
					Buf.wrap(mutation.value()),
					RequestType.none());
			case DELETE -> db.delete(transactionId, columnId, key, RequestType.none());
			case MERGE -> throw new IllegalArgumentException("The randomized model does not generate MERGE operations");
		}
	}

	private static void acceptMutation(Mutation mutation,
	                                   List<Mutation> expectedEvents,
	                                   Map<Integer, byte[]> expectedState) {
		expectedEvents.add(mutation);
		if (mutation.op() == CDCEvent.Op.PUT) {
			expectedState.put(mutation.key(), mutation.value().clone());
		} else {
			expectedState.remove(mutation.key());
		}
	}

	private void replayAndVerify(long seed,
	                             boolean commitPeriodically,
	                             int keyRange,
	                             List<Mutation> expectedEvents,
	                             Map<Integer, byte[]> expectedState) {
		var reconstructedState = new HashMap<Integer, byte[]>();
		var pollRandom = new Random(seed ^ (commitPeriodically ? 0x43D_CDC_5EEDL : 0xCDC_5EEDL));
		Long cursor = null;
		long lastEventSequence = subscriptionStart - 1;
		int expectedIndex = 0;
		int nextCommitAt = 1 + pollRandom.nextInt(MAX_POLL_EVENTS);

		while (expectedIndex < expectedEvents.size()) {
			int maxEvents = 1 + pollRandom.nextInt(MAX_POLL_EVENTS);
			CdcBatch batch = db.cdcPollBatchAsyncInternal(subId, cursor, maxEvents).block(POLL_TIMEOUT);
			String batchContext = replayContext(seed, commitPeriodically, expectedIndex, cursor);
			assertNotNull(batch, "CDC poll completed without a batch; " + batchContext);
			assertFalse(batch.events().isEmpty(), "CDC ended before the expected event stream; " + batchContext);
			assertTrue(batch.events().size() <= maxEvents,
					() -> "Plain-column CDC exceeded the requested event limit; maxEvents=" + maxEvents + "; " + batchContext);

			for (CDCEvent actual : batch.events()) {
				assertTrue(expectedIndex < expectedEvents.size(),
						() -> "CDC emitted an unexpected trailing event " + actual + "; " + batchContext);
				Mutation expected = expectedEvents.get(expectedIndex);
				String eventContext = mutationContext(seed, expected) + "; eventIndex=" + expectedIndex;
				assertTrue(actual.seq() > lastEventSequence,
						"CDC sequence is not strictly increasing; previous=" + lastEventSequence
								+ ", actual=" + actual.seq() + "; " + eventContext);
				assertEquals(columnId, actual.columnId(), "Wrong CDC column; " + eventContext);
				assertEquals(expected.op(), actual.op(), "Wrong CDC operation; " + eventContext);
				assertEquals(expected.key(), bytesToInt(actual.key().toByteArray()), "Wrong CDC key; " + eventContext);
				if (expected.op() == CDCEvent.Op.PUT) {
					assertArrayEquals(expected.value(), actual.value().toByteArray(), "Wrong CDC value; " + eventContext);
					reconstructedState.put(expected.key(), actual.value().toByteArray());
				} else {
					reconstructedState.remove(expected.key());
				}
				lastEventSequence = actual.seq();
				expectedIndex++;
			}

			assertTrue(batch.nextSeq() > lastEventSequence,
					"CDC continuation did not advance past the last event; next=" + batch.nextSeq()
							+ ", last=" + lastEventSequence + "; " + batchContext);
			cursor = batch.nextSeq();
			if (commitPeriodically && expectedIndex >= nextCommitAt) {
				db.cdcCommit(subId, lastEventSequence);
				cursor = null;
				nextCommitAt = expectedIndex + 1 + pollRandom.nextInt(MAX_POLL_EVENTS * 2);
			}
		}

		CdcBatch tail = db.cdcPollBatchAsyncInternal(subId, cursor, MAX_POLL_EVENTS).block(POLL_TIMEOUT);
		assertNotNull(tail, "CDC tail poll completed without a batch; seed=" + seed);
		assertTrue(tail.events().isEmpty(),
				() -> "CDC emitted unexpected trailing events; count=" + tail.events().size() + "; seed=" + seed);
		assertEquals(expectedEvents.size(), expectedIndex, "CDC event count mismatch; seed=" + seed);
		assertStateEquals(expectedState, reconstructedState, seed);
		verifyDatabaseState(expectedState, keyRange, seed);
	}

	private void verifyDatabaseState(Map<Integer, byte[]> expectedState, int keyRange, long seed) {
		for (int key = 0; key < keyRange; key++) {
			Buf actual = db.get(0, columnId, keys(key), RequestType.current());
			byte[] expected = expectedState.get(key);
			if (expected == null) {
				assertNull(actual, "DB retained deleted key " + key + "; seed=" + seed);
			} else {
				assertNotNull(actual, "DB is missing key " + key + "; seed=" + seed);
				assertArrayEquals(expected, actual.toByteArray(), "Wrong DB value for key " + key + "; seed=" + seed);
			}
		}
	}

	private static void assertStateEquals(Map<Integer, byte[]> expected,
	                                      Map<Integer, byte[]> actual,
	                                      long seed) {
		assertEquals(expected.keySet(), actual.keySet(), "CDC replay key set mismatch; seed=" + seed);
		for (var entry : expected.entrySet()) {
			assertArrayEquals(entry.getValue(),
					actual.get(entry.getKey()),
					"CDC replay value mismatch for key " + entry.getKey() + "; seed=" + seed);
		}
	}

	private static Keys keys(int key) {
		return new Keys(Buf.wrap(intToBytes(key)));
	}

	private static byte[] intToBytes(int value) {
		return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
	}

	private static int bytesToInt(byte[] value) {
		assertEquals(Integer.BYTES, value.length, "CDC key has the wrong width");
		return ByteBuffer.wrap(value).getInt();
	}

	private static String replayContext(long seed, boolean commitPeriodically, int expectedIndex, Long cursor) {
		return "seed=" + seed + ", commitPeriodically=" + commitPeriodically
				+ ", expectedIndex=" + expectedIndex + ", cursor=" + cursor;
	}

	private static String mutationContext(long seed, Mutation mutation) {
		return "seed=" + seed + ", operation=" + mutation.operationIndex()
				+ ", mutation=" + mutation.mutationIndex() + ", op=" + mutation.op() + ", key=" + mutation.key();
	}

	private static int positiveIntProperty(String name, int defaultValue) {
		String configured = System.getProperty(name);
		int value = configured == null ? defaultValue : Integer.parseInt(configured);
		if (value <= 0) {
			throw new IllegalArgumentException(name + " must be positive, but was " + value);
		}
		return value;
	}

	private static long configuredSeed() {
		String configured = System.getProperty(SEED_PROPERTY);
		return configured == null ? DEFAULT_SEED : Long.parseLong(configured);
	}

	private record Mutation(int operationIndex, int mutationIndex, CDCEvent.Op op, int key, byte[] value) {

		private Mutation {
			value = value == null ? null : value.clone();
		}
	}
}
