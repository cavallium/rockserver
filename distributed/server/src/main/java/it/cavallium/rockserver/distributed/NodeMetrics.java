package it.cavallium.rockserver.distributed;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cheap counters of where replicated requests spend their time (proposer queue, Raft round trip, apply, read
 * barrier...), logged as one line every {@link #INTERVAL_SECONDS} seconds when there was activity. Each series keeps
 * the count, the sum and the maximum of its values over the interval.
 *
 * <p>Once {@link #bind bound} to the registry of the node's database (the core server's metrics pipeline: JMX, and
 * InfluxDB when configured), every series and gauge is also exported, tagged with the node id:</p>
 * <ul>
 *   <li>a duration series as the function timer {@code rockserver.replication.duration}, tagged {@code name} and
 *   {@code kind} ({@link #kindOf});</li>
 *   <li>an amount series as the function counters {@code rockserver.replication.count} (values recorded) and
 *   {@code rockserver.replication.amount} (their sum), tagged the same way;</li>
 *   <li>a gauge as {@code rockserver.replication.<name>}.</li>
 * </ul>
 * <p>The exported values are cumulative: the log line's interval sums and maxima are kept apart.</p>
 */
final class NodeMetrics {

	private static final Logger LOG = LoggerFactory.getLogger(NodeMetrics.class);
	private static final long INTERVAL_SECONDS = 10;

	/** One series: values are durations in nanoseconds, or plain amounts for {@link #amount}. */
	static final class Series {
		private final LongAdder count = new LongAdder();
		/** Cumulative: the logger subtracts {@link #loggedSum}. */
		private final LongAdder sum = new LongAdder();
		private final AtomicLong max = new AtomicLong();
		/** Values already logged. Used by the logger only. */
		private long logged;
		/** Sum already logged. Used by the logger only. */
		private long loggedSum;

		void record(long value) {
			count.increment();
			sum.add(value);
			max.accumulateAndGet(value, Math::max);
		}

		/** Records the time elapsed since {@code startNanos}. */
		void since(long startNanos) {
			record(System.nanoTime() - startNanos);
		}

		/** Every value recorded so far, for tests. */
		long count() {
			return count.sum();
		}

		/** The sum of every value recorded so far. */
		long sum() {
			return sum.sum();
		}
	}

	/** Sampling period of {@link #sampleEveryMillisecond}, see {@link #LAG_SAMPLE_PROPERTY}. */
	static final String LAG_SAMPLE_PROPERTY = "it.cavallium.rockserver.distributed.metrics.lag-sample-ms";
	private static final long DEFAULT_LAG_SAMPLE_MS = 10;

	private final String nodeId;
	/** The {@code node} tag of the exported meters: that of the database bound last. Guarded by {@link #exportLock}. */
	private String nodeTag;
	private final Map<String, Series> durations = new ConcurrentHashMap<>();
	private final Map<String, Series> amounts = new ConcurrentHashMap<>();
	private final ScheduledExecutorService logger;
	/** Guards {@link #registry} and {@link #registered}. */
	private final Object exportLock = new Object();
	private @Nullable MeterRegistry registry;
	private final List<Meter> registered = new ArrayList<>();
	/** A gauge exported but not logged, see {@link #exportedGauge}; {@code tag} is a key and a value, or empty. */
	private record ExportedGauge(String name, List<String> tag, java.util.function.DoubleSupplier value) {}

	/** Gauges exported but not logged, by name and tag. */
	private final Map<List<String>, ExportedGauge> exportedGauges = new ConcurrentHashMap<>();

	NodeMetrics(String nodeId) {
		this.nodeId = nodeId;
		this.nodeTag = nodeId;
		this.logger = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon()
				.name("rockserver-metrics-" + nodeId).factory());
		logger.scheduleAtFixedRate(this::log, INTERVAL_SECONDS, INTERVAL_SECONDS, TimeUnit.SECONDS);
	}

	/**
	 * Runs {@code sampler} periodically until {@link #close()}, to record gauges into series: every
	 * {@value #DEFAULT_LAG_SAMPLE_MS} ms, or every {@link #LAG_SAMPLE_PROPERTY} ms; 0 turns it off. It used to run every
	 * millisecond on every node, always: the alerts read the gauges of {@link NodeMonitor} instead, so the samples only
	 * add the average and maximum lags to the log line.
	 */
	void sampleEveryMillisecond(Runnable sampler) {
		long periodMs = lagSamplePeriodMs();
		if (periodMs <= 0) {
			return;
		}
		logger.scheduleAtFixedRate(() -> {
			try {
				sampler.run();
			} catch (RuntimeException ignored) {
				// Not ready yet, or stopping.
			}
		}, periodMs, periodMs, TimeUnit.MILLISECONDS);
	}

	static long lagSamplePeriodMs() {
		var value = System.getProperty(LAG_SAMPLE_PROPERTY);
		if (value == null || value.isBlank()) {
			return DEFAULT_LAG_SAMPLE_MS;
		}
		try {
			return Long.parseLong(value.trim());
		} catch (NumberFormatException ex) {
			LOG.warn("Ignoring {}={}: not a number of milliseconds", LAG_SAMPLE_PROPERTY, value);
			return DEFAULT_LAG_SAMPLE_MS;
		}
	}

	private final Map<String, java.util.function.LongSupplier> gauges = new ConcurrentHashMap<>();

	/** A value read and logged at every interval, such as a queue length. */
	void gauge(String name, java.util.function.LongSupplier value) {
		if (gauges.put(name, value) == null) {
			export(registry -> registerGauge(registry, name, () -> value.getAsLong()));
		}
	}

	/**
	 * A value exported to the metrics pipeline only, not logged: the state that alerts read ({@link NodeMonitor}). Read
	 * by the pipeline's own thread, so it must be cheap: NaN when unknown.
	 */
	void exportedGauge(String name, java.util.function.DoubleSupplier value) {
		exportedGauge(new ExportedGauge(name, List.of(), value));
	}

	/** {@link #exportedGauge(String, java.util.function.DoubleSupplier)} with one more tag, such as a follower's id. */
	void exportedGauge(String name, String tagKey, String tagValue, java.util.function.DoubleSupplier value) {
		exportedGauge(new ExportedGauge(name, List.of(tagKey, tagValue), value));
	}

	private void exportedGauge(ExportedGauge gauge) {
		var key = new ArrayList<String>(gauge.tag());
		key.addFirst(gauge.name());
		if (exportedGauges.put(List.copyOf(key), gauge) == null) {
			export(registry -> registerGauge(registry, gauge.name(), gauge.tag(), gauge.value()));
		}
	}

	/** A series of durations in nanoseconds, logged in milliseconds. */
	Series duration(String name) {
		return durations.computeIfAbsent(name, key -> {
			var series = new Series();
			export(registry -> registerDuration(registry, key, series));
			return series;
		});
	}

	/** A series of plain amounts, such as batch sizes. */
	Series amount(String name) {
		return amounts.computeIfAbsent(name, key -> {
			var series = new Series();
			export(registry -> registerAmount(registry, key, series));
			return series;
		});
	}

	/**
	 * Exports every series and gauge, those to come included, to {@code registry}, the registry of the database the node
	 * just opened: a snapshot install closes the database, and its registry with it, and opens a new one. Meters stay
	 * in the registries bound before, which their database closed.
	 */
	void bind(MeterRegistry registry) {
		bind(registry, nodeId);
	}

	/**
	 * As {@link #bind(MeterRegistry)}, tagging the meters with {@code node} = {@code nodeTag}: the tag the database gives
	 * its own meters ({@code database.metrics.node-name}), so that both families name the node the same way.
	 */
	void bind(MeterRegistry registry, String nodeTag) {
		synchronized (exportLock) {
			if (this.registry == registry && this.nodeTag.equals(nodeTag)) {
				return;
			}
			this.registry = registry;
			this.nodeTag = nodeTag;
			registered.clear();
			durations.forEach((name, series) -> registerDuration(registry, name, series));
			amounts.forEach((name, series) -> registerAmount(registry, name, series));
			gauges.forEach((name, value) -> registerGauge(registry, name, () -> value.getAsLong()));
			exportedGauges.values().forEach(gauge -> registerGauge(registry, gauge.name(), gauge.tag(), gauge.value()));
		}
	}

	/** The meters exported to the registry bound last, for tests. */
	List<Meter> exportedMeters() {
		synchronized (exportLock) {
			return List.copyOf(registered);
		}
	}

	private void export(java.util.function.Consumer<MeterRegistry> registration) {
		synchronized (exportLock) {
			if (registry != null) {
				registration.accept(registry);
			}
		}
	}

	private void registerDuration(MeterRegistry registry, String name, Series series) {
		registered.add(FunctionTimer.builder("rockserver.replication.duration", series, Series::count, Series::sum,
						TimeUnit.NANOSECONDS)
				.tag("node", nodeTag)
				.tag("name", name)
				.tag("kind", kindOf(name))
				.description("Replication timings of this node, see NodeMetrics")
				.register(registry));
	}

	private void registerAmount(MeterRegistry registry, String name, Series series) {
		registered.add(FunctionCounter.builder("rockserver.replication.count", series, s -> s.count())
				.tag("node", nodeTag)
				.tag("name", name)
				.tag("kind", kindOf(name))
				.register(registry));
		registered.add(FunctionCounter.builder("rockserver.replication.amount", series, s -> s.sum())
				.tag("node", nodeTag)
				.tag("name", name)
				.tag("kind", kindOf(name))
				.register(registry));
	}

	private void registerGauge(MeterRegistry registry, String name, java.util.function.DoubleSupplier value) {
		registerGauge(registry, name, List.of(), value);
	}

	private void registerGauge(MeterRegistry registry, String name, List<String> tag,
			java.util.function.DoubleSupplier value) {
		var builder = Gauge.builder("rockserver.replication." + name, () -> {
					try {
						return value.getAsDouble();
					} catch (RuntimeException notAvailable) {
						return Double.NaN;
					}
				})
				.tag("node", nodeTag);
		if (!tag.isEmpty()) {
			builder.tag(tag.get(0), tag.get(1));
		}
		registered.add(builder.register(registry));
	}

	/**
	 * What a series measures: {@code apply} (the apply of committed entries, on every node, and snapshots), {@code raft}
	 * (replication itself: lags, leader steps down) or {@code client} (the work of a client request on the node that
	 * received it, or on the leader for it: queues, round trips, barriers, forwarding, local commands). The database's own
	 * {@code rocksdb.operation.timer} mixes the first and the last, see "Metrics" in the design document.
	 */
	static String kindOf(String name) {
		if (name.startsWith("apply") || name.equals("snapshot") || name.equals("raft.appendToApply")) {
			return "apply";
		}
		if (name.startsWith("raft.") || name.startsWith("lag.") || name.startsWith("leader.")) {
			return "raft";
		}
		return "client";
	}

	private void log() {
		var line = new StringBuilder();
		append(line, durations, 1e6, "ms");
		append(line, amounts, 1, "");
		if (!line.isEmpty()) {
			for (var gauge : new TreeMap<>(gauges).entrySet()) {
				try {
					line.append(' ').append(gauge.getKey()).append('=').append(gauge.getValue().getAsLong()).append(';');
				} catch (RuntimeException ignored) {
					// Not available right now.
				}
			}
			LOG.info("{}: last {}s:{}", nodeId, INTERVAL_SECONDS, line);
		}
	}

	private static void append(StringBuilder line, Map<String, Series> series, double scale, String unit) {
		for (var entry : new TreeMap<>(series).entrySet()) {
			var s = entry.getValue();
			long total = s.count.sum();
			long count = total - s.logged;
			s.logged = total;
			long totalSum = s.sum.sum();
			long sum = totalSum - s.loggedSum;
			s.loggedSum = totalSum;
			long max = s.max.getAndSet(0);
			if (count == 0) {
				continue;
			}
			line.append(String.format(Locale.ROOT, " %s=%d×avg %.2f%s max %.2f%s;", entry.getKey(), count,
					sum / (double) count / scale, unit, max / scale, unit));
		}
	}

	void close() {
		logger.shutdownNow();
	}
}
