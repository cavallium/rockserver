package it.cavallium.rockserver.core.impl;

import com.google.common.cache.CacheStats;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.MultiGauge.Row;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
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
	private final Thread executor;
	private final MultiGauge cacheStats;

	private volatile boolean stopRequested = false;

	public RocksDBStatistics(String name, Statistics statistics, MetricsManager metrics, @Nullable Cache cache) {
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
		this.cacheStats = MultiGauge
				.builder("rocksdb.cache")
				.tag("database", name)
				.register(metrics.getRegistry());


		EnumMap<HistogramType, HistogramData> histogramDataRef = new EnumMap<>(HistogramType.class);
		AtomicReference<CacheStats> cacheStatsRef = new AtomicReference<>(cache != null ? getCacheStats(cache) : new CacheStats(0L, 0L));

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
					throw new RuntimeException(e);
				}
			}
		});
		executor.setName("rocksdb-statistics");
		executor.setDaemon(true);
		executor.start();
	}

	private CacheStats getCacheStats(@NotNull Cache cache) {
		return new CacheStats(cache.getUsage(), cache.getPinnedUsage());
	}

	private record CacheStats(long usage, long pinnedUsage) {}

	public void close() {
		stopRequested = true;
		try {
			executor.join();
		} catch (InterruptedException e) {
			LOG.error("Failed to close executor", e);
		}
	}
}
