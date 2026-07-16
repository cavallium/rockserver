package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Holder;
import org.rocksdb.LRUCache;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

/**
 * Standalone reproducer for the upstream {@code DBImpl::KeyMayExist} {@code value_found} bug.
 *
 * <p>All probed keys are immutable and all stored values are non-empty. Writers churn a disjoint key range and
 * a flusher creates new SSTs. Therefore {@code Holder<byte[]> == byte[0]} followed by a non-empty {@code get}
 * cannot be an application-level write race. The reproducer reports zero findings without failing when the native
 * timing does not trigger; a mismatched non-empty holder is always treated as a correctness failure.
 */
public final class KeyMayExistBugReproducer {

	private static final long VALUE_MAGIC = 0xBB67AE8584CAA73BL;

	private KeyMayExistBugReproducer() {
	}

	public static void main(String[] args) throws Exception {
		Options options = Options.parse(args);
		RocksDBLoader.loadLibrary();
		Files.createDirectories(options.root());
		Path runDirectory = Files.createTempDirectory(options.root(), "key-may-exist-");
		try {
			run(options, runDirectory);
		} finally {
			if (!options.keepData()) {
				deleteRecursively(runDirectory);
			}
		}
	}

	private static void run(Options benchmark, Path runDirectory) throws Exception {
		byte[][] hotKeys = keys(0, benchmark.hotKeys());
		byte[][] writerKeys = keys(benchmark.hotKeys(), benchmark.writerKeys());
		String databasePath = runDirectory.resolve("db").toString();
		try (var cache = new LRUCache(benchmark.blockCacheBytes());
				var dbOptions = dbOptions(benchmark, cache);
				var db = RocksDB.open(dbOptions, databasePath);
				var writeOptions = new WriteOptions();
				var flushOptions = new FlushOptions().setWaitForFlush(true)) {
			for (int i = 0; i < hotKeys.length; i++) {
				db.put(writeOptions, hotKeys[i], value(i, 0, benchmark.valueBytes()));
			}
			for (int i = 0; i < writerKeys.length; i++) {
				db.put(writeOptions,
						writerKeys[i],
						value(benchmark.hotKeys() + (long) i, 0, benchmark.valueBytes()));
			}
			db.flush(flushOptions);
			db.compactRange();
		}

		// A fresh cache plus an always-open table reader deterministically reaches the native
		// resident-table/uncached-index Incomplete path that forgets to clear value_found.
		try (var cache = new LRUCache(benchmark.blockCacheBytes());
				var dbOptions = dbOptions(benchmark, cache);
				var db = RocksDB.open(dbOptions, databasePath);
				var writeOptions = new WriteOptions();
				var readOptions = new ReadOptions();
				var flushOptions = new FlushOptions().setWaitForFlush(true)) {
			int workers = benchmark.readers() + benchmark.writers() + 1;
			var executor = Executors.newFixedThreadPool(workers,
					Thread.ofPlatform().name("key-may-exist-repro-", 0).factory());
			var ready = new CountDownLatch(workers);
			var start = new CountDownLatch(1);
			var stop = new AtomicBoolean();
			var metrics = new Metrics();
			probe(db, readOptions, hotKeys[0], 0, benchmark.valueBytes(), ProbePhase.STARTUP, metrics);
			try {
				for (int i = 0; i < benchmark.readers(); i++) {
					long seed = benchmark.seed() + i;
					executor.submit(() -> readLoop(db,
							readOptions,
							hotKeys,
							benchmark.valueBytes(),
							seed,
							ready,
							start,
							stop,
							metrics));
				}
				for (int i = 0; i < benchmark.writers(); i++) {
					long seed = benchmark.seed() + 10_000L + i;
					executor.submit(() -> writeLoop(db,
							writeOptions,
							writerKeys,
							benchmark.hotKeys(),
							benchmark.valueBytes(),
							seed,
							ready,
							start,
							stop,
							metrics));
				}
				executor.submit(() -> flushLoop(db,
						flushOptions,
						benchmark.flushInterval(),
						ready,
						start,
						stop,
						metrics));

				if (!ready.await(30, TimeUnit.SECONDS)) {
					throw new IllegalStateException("Workers did not become ready");
				}
				long started = System.nanoTime();
				start.countDown();
				Thread.sleep(benchmark.duration());
				stop.set(true);
				long elapsed = System.nanoTime() - started;
				executor.shutdown();
				if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
					throw new IllegalStateException("Workers did not stop");
				}

				System.out.printf(Locale.ROOT,
						"KeyMayExist raw reproducer: duration=%.3fs reads=%,d writes=%,d flushes=%,d "
								+ "holderValue=%,d holderMissing=%,d emptyHolder=%,d "
								+ "startupConfirmedBug=%,d stressConfirmedBug=%,d errors=%,d%n",
						elapsed / 1_000_000_000.0,
						metrics.reads.sum(),
						metrics.writes.sum(),
						metrics.flushes.sum(),
						metrics.holderValues.sum(),
						metrics.holderMissing.sum(),
						metrics.emptyHolders.sum(),
						metrics.startupConfirmedBugs.sum(),
						metrics.stressConfirmedBugs.sum(),
						metrics.errors.sum());
				if (metrics.startupConfirmedBugs.sum() + metrics.stressConfirmedBugs.sum() == 0) {
					System.out.println("The native timing did not reproduce in this run; this is not proof of absence.");
				}
				if (metrics.errors.sum() != 0) {
					throw new AssertionError("Unexpected reproducer errors: " + metrics.errorSamples);
				}
			} finally {
				stop.set(true);
				start.countDown();
				executor.shutdownNow();
				executor.awaitTermination(30, TimeUnit.SECONDS);
			}
		}
	}

	private static org.rocksdb.Options dbOptions(Options benchmark, LRUCache cache) {
		var tableConfig = new BlockBasedTableConfig()
				.setBlockCache(cache)
				.setCacheIndexAndFilterBlocks(true);
		return new org.rocksdb.Options()
				.setCreateIfMissing(true)
				.setCompressionType(CompressionType.NO_COMPRESSION)
				.setWriteBufferSize(benchmark.writeBufferBytes())
				.setMaxWriteBufferNumber(4)
				.setMaxOpenFiles(-1)
				.setTableFormatConfig(tableConfig);
	}

	private static void readLoop(RocksDB db,
			ReadOptions readOptions,
			byte[][] keys,
			int valueBytes,
			long seed,
			CountDownLatch ready,
			CountDownLatch start,
			AtomicBoolean stop,
			Metrics metrics) {
		var random = new SplittableRandom(seed);
		ready.countDown();
		await(start);
		while (!stop.get()) {
			int keyId = random.nextInt(keys.length);
			try {
				probe(db, readOptions, keys[keyId], keyId, valueBytes, ProbePhase.STRESS, metrics);
			} catch (Throwable throwable) {
				metrics.recordError(throwable.toString());
			}
		}
	}

	private static void probe(RocksDB db,
			ReadOptions readOptions,
			byte[] key,
			long keyId,
			int valueBytes,
			ProbePhase phase,
			Metrics metrics) throws org.rocksdb.RocksDBException {
		var holder = new Holder<byte[]>();
		boolean mayExist = db.keyMayExist(readOptions, key, holder);
		byte[] held = holder.getValue();
		if (!mayExist) {
			metrics.recordError("false negative for immutable key " + keyId);
		} else if (held == null) {
			metrics.holderMissing.increment();
			validateFallback(db, readOptions, key, keyId, valueBytes, metrics);
		} else if (held.length == 0) {
			metrics.emptyHolders.increment();
			if (validateFallback(db, readOptions, key, keyId, valueBytes, metrics)) {
				switch (phase) {
					case STARTUP -> metrics.startupConfirmedBugs.increment();
					case STRESS -> metrics.stressConfirmedBugs.increment();
				}
			}
		} else if (validValue(held, keyId, valueBytes)) {
			metrics.holderValues.increment();
		} else {
			metrics.recordError("holder value mismatch for key " + keyId);
		}
		metrics.reads.increment();
	}

	private static boolean validateFallback(RocksDB db,
			ReadOptions readOptions,
			byte[] key,
			long keyId,
			int valueBytes,
			Metrics metrics) throws org.rocksdb.RocksDBException {
		byte[] actual = db.get(readOptions, key);
		if (validValue(actual, keyId, valueBytes)) {
			return true;
		}
		metrics.recordError("fallback value mismatch for key " + keyId);
		return false;
	}

	private static void writeLoop(RocksDB db,
			WriteOptions writeOptions,
			byte[][] keys,
			long keyBase,
			int valueBytes,
			long seed,
			CountDownLatch ready,
			CountDownLatch start,
			AtomicBoolean stop,
			Metrics metrics) {
		var random = new SplittableRandom(seed);
		long version = 1;
		ready.countDown();
		await(start);
		while (!stop.get()) {
			int index = random.nextInt(keys.length);
			try {
				db.put(writeOptions, keys[index], value(keyBase + index, version++, valueBytes));
				metrics.writes.increment();
			} catch (Throwable throwable) {
				metrics.recordError(throwable.toString());
			}
		}
	}

	private static void flushLoop(RocksDB db,
			FlushOptions flushOptions,
			Duration interval,
			CountDownLatch ready,
			CountDownLatch start,
			AtomicBoolean stop,
			Metrics metrics) {
		ready.countDown();
		await(start);
		while (!stop.get()) {
			try {
				Thread.sleep(interval);
				if (!stop.get()) {
					db.flush(flushOptions);
					metrics.flushes.increment();
				}
			} catch (InterruptedException ignored) {
				Thread.currentThread().interrupt();
				return;
			} catch (Throwable throwable) {
				metrics.recordError(throwable.toString());
			}
		}
	}

	private static byte[][] keys(long first, int count) {
		byte[][] keys = new byte[count][];
		for (int i = 0; i < count; i++) {
			keys[i] = ByteBuffer.allocate(Long.BYTES).putLong(first + i).array();
		}
		return keys;
	}

	private static byte[] value(long key, long version, int size) {
		byte[] value = new byte[size];
		ByteBuffer.wrap(value).putLong(VALUE_MAGIC).putLong(key).putLong(version);
		Arrays.fill(value, 24, value.length, (byte) (key * 31 + version));
		return value;
	}

	private static boolean validValue(byte[] value, long expectedKey, int expectedSize) {
		if (value == null || value.length != expectedSize) {
			return false;
		}
		ByteBuffer buffer = ByteBuffer.wrap(value);
		return buffer.getLong() == VALUE_MAGIC && buffer.getLong() == expectedKey;
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(ex);
		}
	}

	private static void deleteRecursively(Path root) throws Exception {
		try (var paths = Files.walk(root)) {
			for (var path : paths.sorted((left, right) -> right.compareTo(left)).toList()) {
				Files.deleteIfExists(path);
			}
		}
	}

	private static final class Metrics {
		private final LongAdder reads = new LongAdder();
		private final LongAdder writes = new LongAdder();
		private final LongAdder flushes = new LongAdder();
		private final LongAdder holderValues = new LongAdder();
		private final LongAdder holderMissing = new LongAdder();
		private final LongAdder emptyHolders = new LongAdder();
		private final LongAdder startupConfirmedBugs = new LongAdder();
		private final LongAdder stressConfirmedBugs = new LongAdder();
		private final LongAdder errors = new LongAdder();
		private final ConcurrentLinkedQueue<String> errorSamples = new ConcurrentLinkedQueue<>();

		private void recordError(String message) {
			errors.increment();
			if (errorSamples.size() < 8) {
				errorSamples.add(message);
			}
		}
	}

	private enum ProbePhase {
		STARTUP,
		STRESS
	}

	private record Options(Path root,
			int hotKeys,
			int writerKeys,
			int valueBytes,
			int readers,
			int writers,
			Duration duration,
			Duration flushInterval,
			long writeBufferBytes,
			long blockCacheBytes,
			long seed,
			boolean keepData) {

		private static Options parse(String[] args) {
			var values = new java.util.LinkedHashMap<String, String>();
			for (var arg : args) {
				if (!arg.startsWith("--") || !arg.contains("=")) {
					throw new IllegalArgumentException("Arguments must use --name=value: " + arg);
				}
				int separator = arg.indexOf('=');
				values.put(arg.substring(2, separator), arg.substring(separator + 1));
			}
			var result = new Options(Path.of(take(values, "root", "target/key-may-exist-reproducer")),
					integer(values, "hot-keys", 5_000),
					integer(values, "writer-keys", 1_000),
					integer(values, "value-bytes", 256),
					integer(values, "readers", 4),
					integer(values, "writers", 2),
					Duration.ofSeconds(longValue(values, "seconds", 5)),
					Duration.ofMillis(longValue(values, "flush-ms", 250)),
					longValue(values, "write-buffer-bytes", 2L * 1024 * 1024),
					longValue(values, "block-cache-bytes", 8L * 1024 * 1024),
					longValue(values, "seed", 0x12935L),
					strictBoolean(values, "keep-data", false));
			if (!values.isEmpty()) {
				throw new IllegalArgumentException("Unknown options: " + values.keySet());
			}
			if (result.hotKeys <= 0 || result.writerKeys <= 0 || result.valueBytes < 24
					|| result.readers <= 0 || result.writers <= 0 || result.duration.isZero()
					|| result.duration.isNegative() || result.flushInterval.isZero()
					|| result.flushInterval.isNegative() || result.writeBufferBytes <= 0
					|| result.blockCacheBytes <= 0) {
				throw new IllegalArgumentException("All sizes, durations, and worker counts must be positive");
			}
			return result;
		}

		private static String take(java.util.Map<String, String> values, String name, String defaultValue) {
			return values.containsKey(name) ? values.remove(name) : defaultValue;
		}

		private static int integer(java.util.Map<String, String> values, String name, int defaultValue) {
			return Integer.parseInt(take(values, name, Integer.toString(defaultValue)));
		}

		private static long longValue(java.util.Map<String, String> values, String name, long defaultValue) {
			return Long.parseLong(take(values, name, Long.toString(defaultValue)));
		}

		private static boolean strictBoolean(java.util.Map<String, String> values, String name, boolean defaultValue) {
			String value = take(values, name, Boolean.toString(defaultValue));
			if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
				throw new IllegalArgumentException(name + " must be true or false");
			}
			return Boolean.parseBoolean(value);
		}
	}
}
