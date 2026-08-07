package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Randomized stress test performing a wide variety of operations to detect native resource leaks.
 * The default run is time bounded. Set {@code -Drockserver.test.stress.operations=N} for an exact,
 * reproducible operation count; {@code -Drockserver.test.stress.seed=N} selects the random seed.
 *
 * <p>Coverage includes point and multi-key writes, merges and reads; transaction commit/rollback;
 * for-update success and abort; bounded forward/reverse ranges and cancellation; explicit and
 * expiring iterators; range reductions; flush; and compaction.</p>
 */
public class RandomizedLeakStressTest {

	private static final String LEAK_DETECTION_PROPERTY = "it.cavallium.rockserver.leakdetection";
	private static final String PRINT_CONFIG_PROPERTY = "rockserver.core.print-config";
	private static final String SEED_PROPERTY = "rockserver.test.stress.seed";
	private static final String OPERATIONS_PROPERTY = "rockserver.test.stress.operations";
	private static final String SECONDS_PROPERTY = "rockserver.test.stress.seconds";
	private static final long DEFAULT_SEED = 12_345L;
	private static final int DEFAULT_SECONDS = 6;
	private static final int KEY_SPACE = 256;
	private static final int MAX_RECENT_OPERATIONS = 64;
	private static final int MAX_LIVE_RANGES = 32;
	private static final int MAX_TRACKED_ITERATORS = 64;
	private static final long RESOURCE_RELEASE_TIMEOUT_MILLIS = 5_000L;

	private EmbeddedConnection db;
	private RocksDBSyncAPI syncApi;
	private RocksDBAsyncAPI asyncApi;
	private long colId;
	private Path configFile;
	private String previousLeakDetection;
	private String previousPrintConfig;
	private Set<Thread> baselineResourceThreads = Set.of();

	@BeforeEach
	void setUp() throws Exception {
		baselineResourceThreads = resourceThreads();
		previousLeakDetection = System.getProperty(LEAK_DETECTION_PROPERTY);
		previousPrintConfig = System.getProperty(PRINT_CONFIG_PROPERTY);
		System.setProperty(LEAK_DETECTION_PROPERTY, "true");
		System.setProperty(PRINT_CONFIG_PROPERTY, "false");

		configFile = Files.createTempFile("leak-rand", ".conf");
		Files.writeString(configFile, """
				database: {
				  global: {
				    ingest-behind: true
				    fallback-column-options: {
				      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
				    }
				  }
				}
				""");
		db = new EmbeddedConnection(null, "rnd-leaks", configFile);
		syncApi = db.getSyncApi(RequestContext.batch());
		asyncApi = db.getAsyncApi(RequestContext.batch());
		colId = syncApi.createColumn("rnd-col", ColumnSchema.of(
				IntList.of(Integer.BYTES), ObjectList.of(), true));
	}

	@AfterEach
	void tearDown() throws Exception {
		Throwable failure = null;
		if (db != null) {
			try {
				db.closeTesting();
			} catch (Throwable closeFailure) {
				failure = closeFailure;
			}
		}
		db = null;
		syncApi = null;
		asyncApi = null;

		if (configFile != null) {
			try {
				Files.deleteIfExists(configFile);
			} catch (Throwable deleteFailure) {
				failure = appendFailure(failure, deleteFailure);
			}
			configFile = null;
		}

		restoreProperty(LEAK_DETECTION_PROPERTY, previousLeakDetection);
		restoreProperty(PRINT_CONFIG_PROPERTY, previousPrintConfig);
		if (failure != null) {
			rethrow(failure);
		}
	}

	@Test
	void randomizedWorkloadHasNoLeaks() throws Exception {
		long seed = configuredSeed();
		Integer operationLimit = configuredOperationLimit();
		long durationNanos = operationLimit == null
				? TimeUnit.SECONDS.toNanos(configuredDurationSeconds())
				: Long.MAX_VALUE;
		long startedAt = System.nanoTime();
		var random = new Random(seed);
		var liveRanges = new CopyOnWriteArrayList<Disposable>();
		var openIterators = new ArrayList<Long>();
		var asyncFailures = new ConcurrentLinkedQueue<Throwable>();
		var recentOperations = new ArrayDeque<String>(MAX_RECENT_OPERATIONS);
		int operationIndex = 0;

		while (shouldContinue(operationIndex, operationLimit, startedAt, durationNanos)) {
			liveRanges.removeIf(Disposable::isDisposed);
			throwIfAsyncFailed(asyncFailures, seed, operationIndex, recentOperations);
			Operation operation = nextOperation(random);
			remember(recentOperations, operationIndex + ":" + operation);
			try {
				switch (operation) {
					case PUT -> doPut(random);
					case MERGE -> doMerge(random);
					case GET -> doGet(random);
					case TX_COMMIT -> doTxCommit(random);
					case TX_ROLLBACK -> doTxRollback(random);
					case RANGE_CANCEL -> doRangeCancel(random, asyncFailures);
					case FOR_UPDATE -> doForUpdateFlow(random);
					case START_RANGE -> startConcurrentRange(random, liveRanges, asyncFailures);
					case CANCEL_RANGE -> maybeCancelARange(random, liveRanges);
					case OPEN_ITERATOR -> openIteratorShortLived(random, openIterators);
					case CLOSE_ITERATOR -> maybeCloseAnIterator(random, openIterators);
					case REDUCE_RANGE -> doReduceRange(random);
					case PUT_MULTI -> doPutMulti(random);
					case DELETE_MULTI -> doDeleteMulti(random);
					case FLUSH -> syncApi.flush();
					case COMPACT -> syncApi.compact();
				}
			} catch (Exception | AssertionError failure) {
				throw fuzzFailure(seed, operationIndex, recentOperations, failure);
			}

			if (random.nextInt(100) == 0) {
				remember(recentOperations, operationIndex + ":EXPIRE_AND_CLEAN");
				try {
					openExpiringTxAndIteratorThenCleanup();
				} catch (Exception | AssertionError failure) {
					throw fuzzFailure(seed, operationIndex, recentOperations, failure);
				}
			}
			operationIndex++;
		}

		for (Disposable range : liveRanges) {
			range.dispose();
		}
		liveRanges.clear();
		for (long iteratorId : openIterators) {
			syncApi.closeIterator(iteratorId);
		}
		openIterators.clear();

		invokeCleanup(getInternal(db));
		awaitResourcesReleased(getInternal(db), seed, operationIndex);
		throwIfAsyncFailed(asyncFailures, seed, operationIndex, recentOperations);

		db.closeTesting();
		db = null;
		syncApi = null;
		asyncApi = null;
		awaitNoNewResourceThreads(seed, operationIndex);
	}

	private void doPut(Random random) {
		var key = key(random.nextInt(KEY_SPACE));
		var value = Buf.wrap((byte) random.nextInt(256));
		syncApi.put(0, colId, key, value, RequestType.none());
	}

	private void doMerge(Random random) {
		var key = key(random.nextInt(KEY_SPACE));
		var value = Buf.wrap(("+" + (char) ('a' + random.nextInt(26))).getBytes(StandardCharsets.UTF_8));
		syncApi.merge(0, colId, key, value, RequestType.none());
	}

	private void doGet(Random random) {
		syncApi.get(0, colId, key(random.nextInt(KEY_SPACE)), RequestType.current());
	}

	private void doTxCommit(Random random) {
		long transactionId = syncApi.openTransaction(5_000);
		boolean closed = false;
		try {
			syncApi.put(transactionId,
					colId,
					key(random.nextInt(KEY_SPACE)),
					Buf.wrap((byte) random.nextInt(256)),
					RequestType.none());
			assertTrue(syncApi.closeTransaction(transactionId, true), "Transaction disappeared before commit");
			closed = true;
		} finally {
			if (!closed) {
				syncApi.closeTransaction(transactionId, false);
			}
		}
	}

	private void doTxRollback(Random random) {
		long transactionId = syncApi.openTransaction(5_000);
		boolean closed = false;
		try {
			syncApi.put(transactionId,
					colId,
					key(random.nextInt(KEY_SPACE)),
					Buf.wrap((byte) random.nextInt(256)),
					RequestType.none());
			assertTrue(syncApi.closeTransaction(transactionId, false), "Transaction disappeared before rollback");
			closed = true;
		} finally {
			if (!closed) {
				syncApi.closeTransaction(transactionId, false);
			}
		}
	}

	private void doRangeCancel(Random random, ConcurrentLinkedQueue<Throwable> asyncFailures) {
		var publisher = asyncApi.getRangeAsync(0,
				colId,
				null,
				null,
				random.nextBoolean(),
				RequestType.allInRange(),
				Duration.ofSeconds(5).toMillis());
		var disposable = Flux.from(publisher)
				.take(1 + random.nextInt(5))
				.subscribe(ignored -> {
				}, failure -> recordUnexpectedAsyncFailure(asyncFailures, failure));
		disposable.dispose();
	}

	private void doForUpdateFlow(Random random) {
		var key = key(random.nextInt(KEY_SPACE));
		var context = syncApi.get(0, colId, key, RequestType.forUpdate());
		long updateId = context.updateId();
		boolean closed = false;
		try {
			if (random.nextBoolean()) {
				syncApi.put(updateId,
						colId,
						key,
						Buf.wrap((byte) random.nextInt(256)),
						RequestType.none());
			} else {
				syncApi.closeFailedUpdate(updateId);
			}
			closed = true;
		} finally {
			if (!closed) {
				syncApi.closeFailedUpdate(updateId);
			}
		}
	}

	private void startConcurrentRange(Random random,
	                                  CopyOnWriteArrayList<Disposable> subscriptions,
	                                  ConcurrentLinkedQueue<Throwable> asyncFailures) {
		while (subscriptions.size() >= MAX_LIVE_RANGES) {
			subscriptions.removeFirst().dispose();
		}
		Integer from = random.nextBoolean() ? random.nextInt(KEY_SPACE) : null;
		Integer to = random.nextBoolean() ? random.nextInt(KEY_SPACE) : null;
		Keys start = from == null ? null : key(from);
		Keys end = to == null ? null : key(to);
		var publisher = asyncApi.getRangeAsync(0,
				colId,
				start,
				end,
				random.nextBoolean(),
				RequestType.allInRange(),
				Duration.ofSeconds(10).toMillis());
		var subscription = Flux.from(publisher)
				.take(1 + random.nextInt(8))
				.subscribe(ignored -> {
				}, failure -> recordUnexpectedAsyncFailure(asyncFailures, failure));
		subscriptions.add(subscription);
	}

	private static void maybeCancelARange(Random random, CopyOnWriteArrayList<Disposable> subscriptions) {
		if (subscriptions.isEmpty()) {
			return;
		}
		subscriptions.remove(random.nextInt(subscriptions.size())).dispose();
	}

	private void openIteratorShortLived(Random random, List<Long> openIterators) {
		while (openIterators.size() >= MAX_TRACKED_ITERATORS) {
			syncApi.closeIterator(openIterators.removeFirst());
		}
		long iteratorId;
		try {
			iteratorId = syncApi.openIterator(0,
					colId,
					key(random.nextInt(KEY_SPACE)),
					null,
					random.nextBoolean(),
					50);
		} catch (RocksDBException deadline) {
			if (deadline.getErrorUniqueId() == RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED) {
				return;
			}
			throw deadline;
		}
		if (random.nextBoolean()) {
			openIterators.add(iteratorId);
		} else {
			syncApi.closeIterator(iteratorId);
		}
	}

	private void maybeCloseAnIterator(Random random, List<Long> openIterators) {
		if (openIterators.isEmpty()) {
			return;
		}
		syncApi.closeIterator(openIterators.remove(random.nextInt(openIterators.size())));
	}

	private void doReduceRange(Random random) {
		Integer startValue = random.nextBoolean() ? random.nextInt(KEY_SPACE) : null;
		Integer endValue = random.nextBoolean() ? random.nextInt(KEY_SPACE) : null;
		if (startValue != null && endValue != null && startValue > endValue) {
			int swapped = startValue;
			startValue = endValue;
			endValue = swapped;
		}
		Keys start = startValue != null ? key(startValue) : null;
		Keys end = endValue != null ? key(endValue) : null;
		boolean reverse = random.nextBoolean();
		long timeout = Duration.ofSeconds(5).toMillis();
		boolean countEntries = random.nextBoolean();
		try {
			if (countEntries) {
				syncApi.reduceRange(0, colId, start, end, reverse, RequestType.entriesCount(), timeout);
			} else {
				syncApi.reduceRange(0, colId, start, end, reverse, RequestType.firstAndLast(), timeout);
			}
		} catch (RuntimeException failure) {
			throw new AssertionError("reduceRange failed; start=" + startValue + ", end=" + endValue
					+ ", reverse=" + reverse + ", request=" + (countEntries ? "count" : "first-and-last"), failure);
		}
	}

	private void doPutMulti(Random random) {
		int size = 1 + random.nextInt(4);
		var keys = new ArrayList<Keys>(size);
		var values = new ArrayList<Buf>(size);
		for (int index = 0; index < size; index++) {
			keys.add(key(random.nextInt(KEY_SPACE)));
			values.add(Buf.wrap((byte) random.nextInt(256)));
		}
		syncApi.putMulti(0, colId, keys, values, RequestType.none());
	}

	private void doDeleteMulti(Random random) {
		int size = 1 + random.nextInt(4);
		var keys = new ArrayList<Keys>(size);
		for (int index = 0; index < size; index++) {
			keys.add(key(random.nextInt(KEY_SPACE)));
		}
		syncApi.deleteMulti(0, colId, keys, RequestType.none());
	}

	private void openExpiringTxAndIteratorThenCleanup() {
		long transactionId = syncApi.openTransaction(10);
		try {
			syncApi.openIterator(transactionId, colId, key(1), null, false, 10);
		} catch (RocksDBException expired) {
			switch (expired.getErrorUniqueId()) {
				case TX_NOT_FOUND, TRANSACTION_NOT_FOUND, READ_DEADLINE_EXCEEDED -> {
				}
				default -> throw expired;
			}
		}
		sleep(20);
		invokeCleanup(getInternal(db));
	}

	private static Operation nextOperation(Random random) {
		return switch (random.nextInt(25)) {
			case 0, 1, 2, 3 -> Operation.PUT;
			case 4, 5, 6 -> Operation.MERGE;
			case 7, 8, 9 -> Operation.GET;
			case 10, 11 -> Operation.TX_COMMIT;
			case 12 -> Operation.TX_ROLLBACK;
			case 13 -> Operation.RANGE_CANCEL;
			case 14, 15 -> Operation.FOR_UPDATE;
			case 16 -> Operation.START_RANGE;
			case 17 -> Operation.CANCEL_RANGE;
			case 18 -> Operation.OPEN_ITERATOR;
			case 19 -> Operation.CLOSE_ITERATOR;
			case 20 -> Operation.REDUCE_RANGE;
			case 21 -> Operation.PUT_MULTI;
			case 22 -> Operation.DELETE_MULTI;
			case 23 -> Operation.FLUSH;
			case 24 -> Operation.COMPACT;
			default -> throw new AssertionError("Unreachable operation selector");
		};
	}

	private static boolean shouldContinue(int operationIndex,
	                                      Integer operationLimit,
	                                      long startedAt,
	                                      long durationNanos) {
		return operationLimit != null
				? operationIndex < operationLimit
				: System.nanoTime() - startedAt < durationNanos;
	}

	private static void awaitResourcesReleased(EmbeddedDB internal, long seed, int operationCount) {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RESOURCE_RELEASE_TIMEOUT_MILLIS);
		while (System.nanoTime() < deadline) {
			if (internal.getPendingOpsCount() == 0
					&& internal.getOpenTransactionsCount() == 0
					&& internal.getOpenIteratorsCount() == 0) {
				return;
			}
			sleep(25);
			invokeCleanup(internal);
		}
		assertEquals(0, internal.getPendingOpsCount(),
				"Pending operations remained after fuzz cleanup; seed=" + seed + ", operations=" + operationCount);
		assertEquals(0, internal.getOpenTransactionsCount(),
				"Transactions remained after fuzz cleanup; seed=" + seed + ", operations=" + operationCount);
		assertEquals(0, internal.getOpenIteratorsCount(),
				"Iterators remained after fuzz cleanup; seed=" + seed + ", operations=" + operationCount);
	}

	private void awaitNoNewResourceThreads(long seed, int operationCount) {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RESOURCE_RELEASE_TIMEOUT_MILLIS);
		Set<Thread> leaked;
		do {
			leaked = resourceThreads();
			leaked.removeAll(baselineResourceThreads);
			if (leaked.isEmpty()) {
				return;
			}
			sleep(100);
		} while (System.nanoTime() < deadline);

		String names = leaked.stream()
				.map(thread -> thread.getName() + " (daemon=" + thread.isDaemon() + ")")
				.sorted()
				.collect(Collectors.joining(", "));
		assertTrue(leaked.isEmpty(),
				"Resource threads leaked: " + names + "; seed=" + seed + ", operations=" + operationCount);
	}

	private static Set<Thread> resourceThreads() {
		Set<Thread> result = Collections.newSetFromMap(new IdentityHashMap<>());
		for (Thread thread : Thread.getAllStackTraces().keySet()) {
			String name = thread.getName().toLowerCase(Locale.ROOT);
			if (thread.isAlive() && (name.contains("db-") || name.contains("rocksdb") || name.contains("influx"))) {
				result.add(thread);
			}
		}
		return result;
	}

	private static void throwIfAsyncFailed(ConcurrentLinkedQueue<Throwable> failures,
	                                       long seed,
	                                       int operationIndex,
	                                       Deque<String> recentOperations) {
		Throwable failure = failures.poll();
		if (failure == null) {
			return;
		}
		Throwable additional;
		while ((additional = failures.poll()) != null) {
			failure.addSuppressed(additional);
		}
		throw fuzzFailure(seed, operationIndex, recentOperations, failure);
	}

	private static void recordUnexpectedAsyncFailure(ConcurrentLinkedQueue<Throwable> failures, Throwable failure) {
		if (failure instanceof RocksDBException rocksFailure
				&& rocksFailure.getErrorUniqueId() == RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED) {
			return;
		}
		failures.add(failure);
	}

	private static AssertionError fuzzFailure(long seed,
	                                          int operationIndex,
	                                          Deque<String> recentOperations,
	                                          Throwable cause) {
		String message = "Randomized leak workload failed; seed=" + seed + ", operation=" + operationIndex
				+ ". Replay through this operation with -D" + SEED_PROPERTY + "=" + seed
				+ " -D" + OPERATIONS_PROPERTY + "=" + (operationIndex + 1)
				+ ". Recent operations: " + String.join(" -> ", recentOperations);
		return new AssertionError(message, cause);
	}

	private static void remember(Deque<String> recentOperations, String operation) {
		if (recentOperations.size() == MAX_RECENT_OPERATIONS) {
			recentOperations.removeFirst();
		}
		recentOperations.addLast(operation);
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(intKey(value)));
	}

	private static byte[] intKey(int value) {
		return new byte[]{
				(byte) (value & 0xFF),
				(byte) ((value >> 8) & 0xFF),
				(byte) ((value >> 16) & 0xFF),
				(byte) ((value >> 24) & 0xFF)
		};
	}

	private static EmbeddedDB getInternal(EmbeddedConnection connection) {
		return connection.getInternalDB();
	}

	private static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Interrupted while waiting for fuzz resource cleanup", interrupted);
		}
	}

	private static void invokeCleanup(EmbeddedDB internal) {
		try {
			Method transactionCleanup = EmbeddedDB.class.getDeclaredMethod("cleanupExpiredTransactionsNow");
			Method iteratorCleanup = EmbeddedDB.class.getDeclaredMethod("cleanupExpiredIteratorsNow");
			transactionCleanup.setAccessible(true);
			iteratorCleanup.setAccessible(true);
			transactionCleanup.invoke(internal);
			iteratorCleanup.invoke(internal);
		} catch (ReflectiveOperationException failure) {
			throw new AssertionError("Unable to invoke deterministic leak cleanup", failure);
		}
	}

	private static long configuredSeed() {
		String configured = System.getProperty(SEED_PROPERTY);
		return configured == null ? DEFAULT_SEED : Long.parseLong(configured);
	}

	private static Integer configuredOperationLimit() {
		String configured = System.getProperty(OPERATIONS_PROPERTY);
		if (configured == null) {
			return null;
		}
		int value = Integer.parseInt(configured);
		if (value <= 0) {
			throw new IllegalArgumentException(OPERATIONS_PROPERTY + " must be positive, but was " + value);
		}
		return value;
	}

	private static int configuredDurationSeconds() {
		String configured = System.getProperty(SECONDS_PROPERTY);
		int value = configured == null ? DEFAULT_SECONDS : Integer.parseInt(configured);
		if (value <= 0) {
			throw new IllegalArgumentException(SECONDS_PROPERTY + " must be positive, but was " + value);
		}
		return value;
	}

	private static void restoreProperty(String name, String previousValue) {
		if (previousValue == null) {
			System.clearProperty(name);
		} else {
			System.setProperty(name, previousValue);
		}
	}

	private static Throwable appendFailure(Throwable existing, Throwable additional) {
		if (existing == null) {
			return additional;
		}
		existing.addSuppressed(additional);
		return existing;
	}

	private static void rethrow(Throwable failure) throws Exception {
		if (failure instanceof Exception exception) {
			throw exception;
		}
		if (failure instanceof Error error) {
			throw error;
		}
		throw new AssertionError(failure);
	}

	private enum Operation {
		PUT,
		MERGE,
		GET,
		TX_COMMIT,
		TX_ROLLBACK,
		RANGE_CANCEL,
		FOR_UPDATE,
		START_RANGE,
		CANCEL_RANGE,
		OPEN_ITERATOR,
		CLOSE_ITERATOR,
		REDUCE_RANGE,
		PUT_MULTI,
		DELETE_MULTI,
		FLUSH,
		COMPACT
	}
}
