package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.MultiGauge.Row;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.Cache;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

public class RocksDBStatistics {

	private final Statistics statistics;
	private final MetricsManager metrics;
	private final EnumMap<TickerType, Counter> tickerMap;
	private final EnumMap<HistogramType, MultiGauge> histogramMap;
	private final ScheduledExecutorService executor;
	private final ScheduledFuture<?> scheduledTask;
	private final MultiGauge cacheStats;

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

		this.executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("rocksdb-statistics"));

		EnumMap<HistogramType, HistogramData> histogramDataRef = new EnumMap<>(HistogramType.class);

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
					Row.of(Tags.of("field", "usage"), cache.getUsage()),
					Row.of(Tags.of("field", "pinned_usage"), cache.getPinnedUsage())
			), true);
		}

		this.scheduledTask = executor.scheduleAtFixedRate(() -> {
			for (TickerType tickerType : tickerMap.keySet()) {
				var tickerCount = statistics.getAndResetTickerCount(tickerType);
				tickerMap.get(tickerType).increment(tickerCount);
			}

			for (HistogramType histogramType : histogramMap.keySet()) {
				histogramDataRef.put(histogramType, statistics.getHistogramData(histogramType));
			}

		}, 10, 60, TimeUnit.SECONDS);
	}

	public void close() {
		scheduledTask.cancel(false);
		executor.close();
	}
}
