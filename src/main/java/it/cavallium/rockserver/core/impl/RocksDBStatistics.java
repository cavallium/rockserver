package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.MultiGauge.Row;
import io.micrometer.core.instrument.Tags;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import it.cavallium.rockserver.core.impl.RocksDBLongProperty.AggregationMode;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.Cache;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBStatistics {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStatistics.class);

	private final Statistics statistics;
	private final MetricsManager metrics;
	private final EnumMap<TickerType, Counter> tickerMap;
	private final EnumMap<HistogramType, MultiGauge> histogramMap;
	private final EnumMap<RocksDBLongProperty, Gauge> longPropertyMap;
	private final EnumMap<RocksDBLongProperty, MultiGauge> perCfLongPropertyMap;
	private final Thread executor;
	private final MultiGauge cacheStats;

	private volatile boolean stopRequested = false;

	/**
	 * Upper-bound memory configuration derived from RocksDB options.
	 * Used to estimate maximum possible native memory usage.
	 *
	 * @param maxBackgroundJobs maximum concurrent compaction/flush jobs
	 * @param compactionReadaheadBytes readahead buffer size per compaction input file
	 * @param writableFileMaxBufferBytes writable file buffer size per compaction output file
	 */
	public record MemoryUpperBoundConfig(int maxBackgroundJobs, long compactionReadaheadBytes, long writableFileMaxBufferBytes) {}

	public RocksDBStatistics(String name,
			Statistics statistics,
			MetricsManager metrics,
			@Nullable Cache cache,
			BiFunction<String, AggregationMode, BigInteger> longPropertyGetter,
			Function<String, Map<String, Long>> perCfLongPropertyGetter,
			MemoryUpperBoundConfig memoryUpperBoundConfig) {
		this.statistics = statistics;
		this.metrics = metrics;
		this.tickerMap = new EnumMap<>(Arrays
				.stream(TickerType.values())
				.collect(Collectors.toMap(Function.identity(),
						tickerType -> io.micrometer.core.instrument.Counter
								.builder("rocksdb.statistics")
								.tag("database", name)
								.tag("ticker_name", tickerType.name())
								.register(metrics.getRegistry())
				)));
		this.histogramMap = new EnumMap<>(Arrays
				.stream(HistogramType.values())
				.collect(Collectors.toMap(Function.identity(),
						histogramType -> MultiGauge
								.builder("rocksdb.statistics")
								.tag("database", name)
								.tag("histogram_name", histogramType.name())
								.register(metrics.getRegistry())
				)));
		// Register non-PER_CF properties as single gauges (DB_WIDE, SINGLE_CF)
		this.longPropertyMap = new EnumMap<>(Arrays
				.stream(RocksDBLongProperty.values())
				.filter(p -> p.getAggregationMode() != AggregationMode.PER_CF)
				.collect(Collectors.toMap(Function.identity(),
						longProperty -> Gauge
								.builder("rocksdb.property.long", () -> longPropertyGetter.apply(longProperty.getName(), longProperty.getAggregationMode()))
								.tag("database", name)
								.tag("property_name", longProperty.getName())
								.register(metrics.getRegistry())
				)));
		// Register PER_CF properties as MultiGauges with column_family tag
		this.perCfLongPropertyMap = new EnumMap<>(Arrays
				.stream(RocksDBLongProperty.values())
				.filter(p -> p.getAggregationMode() == AggregationMode.PER_CF)
				.collect(Collectors.toMap(Function.identity(),
						longProperty -> MultiGauge
								.builder("rocksdb.property.long")
								.tag("database", name)
								.tag("property_name", longProperty.getName())
								.register(metrics.getRegistry())
				)));
		this.cacheStats = MultiGauge
				.builder("rocksdb.cache")
				.tag("database", name)
				.register(metrics.getRegistry());

		// Realistic total memory gauge: block cache + table readers (index/filter outside cache) + memtables
		// Note: block-cache-pinned-usage is a subset of block-cache-usage, so it's not added separately
		Gauge.builder("rocksdb.memory.total", () -> {
			try {
				long blockCacheUsage = longPropertyGetter.apply(
						RocksDBLongProperty.BLOCK_CACHE_USAGE.getName(),
						RocksDBLongProperty.BLOCK_CACHE_USAGE.getAggregationMode()
				).longValue();
				long tableReadersMem = longPropertyGetter.apply(
						RocksDBLongProperty.ESTIMATE_TABLE_READERS_MEM.getName(),
						RocksDBLongProperty.ESTIMATE_TABLE_READERS_MEM.getAggregationMode()
				).longValue();
				long memtables = longPropertyGetter.apply(
						RocksDBLongProperty.CUR_SIZE_ALL_MEM_TABLES.getName(),
						RocksDBLongProperty.CUR_SIZE_ALL_MEM_TABLES.getAggregationMode()
				).longValue();
				return blockCacheUsage + tableReadersMem + memtables;
			} catch (Throwable ex) {
				LOG.error("Failed to compute total RocksDB memory", ex);
				return 0;
			}
		}).tag("database", name).register(metrics.getRegistry());

		// Upper-bound memory estimate: uses block cache capacity (not usage) + memtables including
		// pending free + table readers + worst-case compaction I/O buffers.
		// Each background job can read from ~10 input files (readahead buffer each) and write 1 output
		// file (writable file buffer). This gives a realistic upper bound on native memory.
		final int estimatedInputFilesPerCompaction = 10;
		Gauge.builder("rocksdb.memory.max-estimate", () -> {
			try {
				// Block cache capacity (configured max, not current usage)
				long blockCacheCapacity = longPropertyGetter.apply(
						RocksDBLongProperty.BLOCK_CACHE_CAPACITY.getName(),
						RocksDBLongProperty.BLOCK_CACHE_CAPACITY.getAggregationMode()
				).longValue();
				// All memtables including those pending flush (not yet freed)
				long allMemtables = longPropertyGetter.apply(
						RocksDBLongProperty.SIZE_ALL_MEM_TABLES.getName(),
						RocksDBLongProperty.SIZE_ALL_MEM_TABLES.getAggregationMode()
				).longValue();
				// Index/filter blocks held outside the block cache
				long tableReadersMem = longPropertyGetter.apply(
						RocksDBLongProperty.ESTIMATE_TABLE_READERS_MEM.getName(),
						RocksDBLongProperty.ESTIMATE_TABLE_READERS_MEM.getAggregationMode()
				).longValue();
				// Upper bound for compaction I/O buffers:
				// readahead per input file + writable buffer per output file, per background job
				long compactionBuffers = (long) memoryUpperBoundConfig.maxBackgroundJobs()
						* (memoryUpperBoundConfig.compactionReadaheadBytes() * estimatedInputFilesPerCompaction
						   + memoryUpperBoundConfig.writableFileMaxBufferBytes());
				return blockCacheCapacity + allMemtables + tableReadersMem + compactionBuffers;
			} catch (Throwable ex) {
				LOG.error("Failed to compute max-estimate RocksDB memory", ex);
				return 0;
			}
		}).tag("database", name).register(metrics.getRegistry());

		EnumMap<HistogramType, HistogramData> histogramDataRef = new EnumMap<>(HistogramType.class);
		AtomicReference<CacheStats> cacheStatsRef = new AtomicReference<>(cache != null ? getCacheStats(cache) : new CacheStats(0L, 0L));
		// Per-CF property snapshots: property -> (column_name -> value)
		ConcurrentHashMap<RocksDBLongProperty, Map<String, Long>> perCfSnapshots = new ConcurrentHashMap<>();

		histogramMap.forEach((histogramType, multiGauge) -> {
			// Pre populate all
			histogramDataRef.put(histogramType, statistics.getHistogramData(histogramType));

			multiGauge.register(List.of(
					Row.of(Tags.of("field", "average"), () -> histogramDataRef.get(histogramType).getAverage()),
					Row.of(Tags.of("field", "count"), () -> histogramDataRef.get(histogramType).getCount()),
					Row.of(Tags.of("field", "max"), () -> histogramDataRef.get(histogramType).getMax()),
					Row.of(Tags.of("field", "min"), () -> histogramDataRef.get(histogramType).getMin()),
					Row.of(Tags.of("field", "median"), () -> histogramDataRef.get(histogramType).getMedian()),
					Row.of(Tags.of("field", "percentile95"), () -> histogramDataRef.get(histogramType).getPercentile95()),
					Row.of(Tags.of("field", "percentile99"), () -> histogramDataRef.get(histogramType).getPercentile99()),
					Row.of(Tags.of("field", "standard_deviation"), () -> histogramDataRef.get(histogramType).getStandardDeviation()),
					Row.of(Tags.of("field", "sum"), () -> histogramDataRef.get(histogramType).getSum())
			), true);
		});

		if (cache != null) {
			cacheStats.register(List.of(
					Row.of(Tags.of("field", "usage"), () -> cacheStatsRef.get().usage()),
					Row.of(Tags.of("field", "pinned_usage"), () -> cacheStatsRef.get().pinnedUsage())
			), true);
		}

		this.executor = new Thread(() -> {
			while (!stopRequested) {
				var taskStartTime = System.nanoTime();

				try {
					for (TickerType tickerType : tickerMap.keySet()) {
						var tickerCount = statistics.getAndResetTickerCount(tickerType);
						tickerMap.get(tickerType).increment(tickerCount);
					}

					for (HistogramType histogramType : histogramMap.keySet()) {
						histogramDataRef.put(histogramType, statistics.getHistogramData(histogramType));
					}

					if (cache != null) {
						cacheStatsRef.set(getCacheStats(cache));
					}

					// Update per-CF property snapshots and re-register MultiGauge rows
					for (var entry : perCfLongPropertyMap.entrySet()) {
						RocksDBLongProperty prop = entry.getKey();
						MultiGauge multiGauge = entry.getValue();
						Map<String, Long> snapshot = perCfLongPropertyGetter.apply(prop.getName());
						perCfSnapshots.put(prop, snapshot);
						List<Row<?>> rows = new ArrayList<>();
						for (var cfEntry : snapshot.entrySet()) {
							String colName = cfEntry.getKey();
							rows.add(Row.of(Tags.of("column_family", colName), cfEntry::getValue));
						}
						multiGauge.register(rows, true);
					}
				} catch (Throwable ex) {
					LOG.error("Fatal error during stats collection", ex);
				}

				var taskEndTime = System.nanoTime();

				long nextTaskStartTime = taskStartTime;
				while (nextTaskStartTime <= taskEndTime) {
					nextTaskStartTime += 60000000000L;
				}

				try {
					Thread.sleep(Duration.ofNanos(nextTaskStartTime - taskEndTime));
				} catch (InterruptedException e) {
					LOG.debug("Statistics sleep interrupted");
				}
			}
		});
		executor.setName("rocksdb-statistics");
		executor.setDaemon(false);
		executor.start();
	}

	private CacheStats getCacheStats(@NotNull Cache cache) {
		return new CacheStats(cache.getUsage(), cache.getPinnedUsage());
	}

	private record CacheStats(long usage, long pinnedUsage) {}

	public void close() {
		stopRequested = true;
		executor.interrupt();
		try {
			executor.join();
		} catch (InterruptedException e) {
			LOG.error("Failed to close executor", e);
		}
	}
}
