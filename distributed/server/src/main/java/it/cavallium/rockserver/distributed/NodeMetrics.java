package it.cavallium.rockserver.distributed;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
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
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cheap counters of where replicated requests spend their time (proposer queue, Raft round trip, apply, read
 * barrier...), logged every {@link #INTERVAL_SECONDS} seconds as one short line for a person ({@link #summary}) and, at debug level, as one line with every series. Each series keeps
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
 * <p>The exported values are cumulative: the log line's interval sums and maxima are kept apart. They count from
 * the {@link #bind} of the registry, not from the node's start (R60-2): the registry of a new database (start, and each
 * snapshot install) exports only what the registries bound before did not read ({@link Exported}). The InfluxDB
 * registry is a step registry, which publishes a function counter as the difference of its function between steps,
 * from 0 when it is made: on the lifetime totals, its first step after an install carried every count since the node
 * started, and an old checksum mismatch, leader stall or failed install record fired its alert again. A cumulative
 * registry (JMX) restarts its counts at each install.</p>
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

		/** What the registry bound last reads of this series, or null before the first registry. Guarded by exportLock. */
		private @Nullable Exported exported;
	}

	/**
	 * One series as one registry reads it (R60-2): its count and sum minus those that the registries bound before read
	 * (the base), each read never below the last one. A {@link #bind} retires the reading of the registry bound before:
	 * that registry then reads the values it read last, whether its database closed it (its last publish) or not, and
	 * the new registry starts from them. So every value is published once, by one registry, and none is lost: what
	 * comes between the last read of the old registry and the bind goes to the new one.
	 */
	static final class Exported {
		private final Series series;
		private final long countBase;
		private final long sumBase;
		/** The count and sum read last, from the series itself (not less the bases). Guarded by this. */
		private long countRead;
		private long sumRead;
		private boolean retired;

		Exported(Series series, @Nullable Exported before) {
			this.series = series;
			if (before == null) {
				countBase = 0;
				sumBase = 0;
			} else {
				synchronized (before) {
					before.retired = true;
					countBase = before.countRead;
					sumBase = before.sumRead;
				}
			}
			countRead = countBase;
			sumRead = sumBase;
		}

		synchronized long count() {
			if (!retired) {
				countRead = Math.max(countRead, series.count());
			}
			return countRead - countBase;
		}

		synchronized long sum() {
			if (!retired) {
				sumRead = Math.max(sumRead, series.sum());
			}
			return sumRead - sumBase;
		}
	}

	/** A new {@link Exported} of {@code series} for the registry being bound, retiring the one bound before. */
	private static Exported exportedFor(Series series) {
		var exported = new Exported(series, series.exported);
		series.exported = exported;
		return exported;
	}

	/** Sampling period of {@link #sampleEveryMillisecond}, see {@link #LAG_SAMPLE_PROPERTY}. */
	static final String LAG_SAMPLE_PROPERTY = DistributedProperties.PREFIX + "metrics.lag-sample-ms";
	private static final long DEFAULT_LAG_SAMPLE_MS = 10;

	private final String nodeId;
	/** The {@code node} tag of the exported meters: that of the database bound last. Guarded by {@link #exportLock}. */
	private String nodeTag;
	private final Map<String, Series> durations = new ConcurrentHashMap<>();
	private final Map<String, Series> amounts = new ConcurrentHashMap<>();
	private final ScheduledExecutorService logger;
	/**
	 * Runs the samplers of {@link #sampleEveryMillisecond}: the node's reads Ratis' log under its read lock, which a hung
	 * disk can keep away for good, and the log line must go on (R25-W). Created by the first sampler.
	 */
	private volatile @Nullable ScheduledExecutorService samplerThread;
	/** Guards {@link #registry}. */
	private final Object exportLock = new Object();
	private @Nullable MeterRegistry registry;
	/** A gauge exported but not logged, see {@link #exportedGauge}; {@code tag} is a key and a value, or empty. */
	private record ExportedGauge(String name, List<String> tag, DoubleSupplier value) {}

	/** Gauges exported but not logged, by name and tag. */
	private final Map<List<String>, ExportedGauge> exportedGauges = new ConcurrentHashMap<>();

	NodeMetrics(String nodeId) {
		this.nodeId = nodeId;
		this.nodeTag = nodeId;
		this.logger = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon()
				.name("rockserver-metrics-" + nodeId).factory());
		logger.scheduleAtFixedRate(() -> {
			try {
				log();
			} catch (Throwable error) {
				// scheduleAtFixedRate cancels every later run of a task that throws, silently.
				ReplicatedNode.taskError(nodeId, "the metrics log line", error);
			}
		}, INTERVAL_SECONDS, INTERVAL_SECONDS, TimeUnit.SECONDS);
	}

	/**
	 * Runs {@code sampler} periodically until {@link #close()}, to record gauges into series: every
	 * {@value #DEFAULT_LAG_SAMPLE_MS} ms, or every {@link #LAG_SAMPLE_PROPERTY} ms; 0 turns it off. It used to run every
	 * millisecond on every node, always: the alerts read the gauges of {@link NodeMonitor} instead, so the samples only
	 * add the average and maximum lags to the log line. Samplers run on a thread of their own, not the logger's: a
	 * sampler that waits for good (for Ratis' log lock on a hung disk) stops the samples, not the log line.
	 */
	synchronized void sampleEveryMillisecond(Runnable sampler) {
		long periodMs = lagSamplePeriodMs();
		if (periodMs <= 0 || logger.isShutdown()) {
			return;
		}
		var thread = samplerThread;
		if (thread == null) {
			thread = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon()
					.name("rockserver-metrics-sampler-" + nodeId).factory());
			samplerThread = thread;
		}
		thread.scheduleAtFixedRate(() -> {
			try {
				sampler.run();
			} catch (RuntimeException ignored) {
				// Not ready yet, or stopping.
			} catch (Throwable error) {
				// scheduleAtFixedRate cancels every later run of a task that throws, silently.
				ReplicatedNode.taskError(nodeId, "the lag sampler", error);
			}
		}, periodMs, periodMs, TimeUnit.MILLISECONDS);
	}

	static long lagSamplePeriodMs() {
		return DistributedProperties.getLong(DEFAULT_LAG_SAMPLE_MS, LAG_SAMPLE_PROPERTY);
	}

	private final Map<String, LongSupplier> gauges = new ConcurrentHashMap<>();

	/** A value read and logged at every interval, such as a queue length. */
	void gauge(String name, LongSupplier value) {
		if (gauges.put(name, value) == null) {
			export(registry -> registerGauge(registry, name, () -> value.getAsLong()));
		}
	}

	/**
	 * A value exported to the metrics pipeline only, not logged: the state that alerts read ({@link NodeMonitor}). Read
	 * by the pipeline's own thread, so it must be cheap: NaN when unknown.
	 */
	void exportedGauge(String name, DoubleSupplier value) {
		exportedGauge(new ExportedGauge(name, List.of(), value));
	}

	/** {@link #exportedGauge(String, java.util.function.DoubleSupplier)} with one more tag, such as a follower's id. */
	void exportedGauge(String name, String tagKey, String tagValue, DoubleSupplier value) {
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
	 * in the registries bound before, which their database closed; their series stop at the values they read last, and
	 * the new registry exports only what came after (see {@link Exported}).
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
			durations.forEach((name, series) -> registerDuration(registry, name, series));
			amounts.forEach((name, series) -> registerAmount(registry, name, series));
			gauges.forEach((name, value) -> registerGauge(registry, name, () -> value.getAsLong()));
			exportedGauges.values().forEach(gauge -> registerGauge(registry, gauge.name(), gauge.tag(), gauge.value()));
		}
	}

	private void export(Consumer<MeterRegistry> registration) {
		synchronized (exportLock) {
			if (registry != null) {
				registration.accept(registry);
			}
		}
	}

	private void registerDuration(MeterRegistry registry, String name, Series series) {
		// The meters hold their object weakly: the series holds the one of the registry bound last.
		FunctionTimer.builder("rockserver.replication.duration", exportedFor(series), Exported::count, Exported::sum,
						TimeUnit.NANOSECONDS)
				.tag("node", nodeTag)
				.tag("name", name)
				.tag("kind", kindOf(name))
				.description("Replication timings of this node, see NodeMetrics")
				.register(registry);
	}

	private void registerAmount(MeterRegistry registry, String name, Series series) {
		var exported = exportedFor(series);
		FunctionCounter.builder("rockserver.replication.count", exported, Exported::count)
				.tag("node", nodeTag)
				.tag("name", name)
				.tag("kind", kindOf(name))
				.register(registry);
		FunctionCounter.builder("rockserver.replication.amount", exported, Exported::sum)
				.tag("node", nodeTag)
				.tag("name", name)
				.tag("kind", kindOf(name))
				.register(registry);
	}

	private void registerGauge(MeterRegistry registry, String name, DoubleSupplier value) {
		registerGauge(registry, name, List.of(), value);
	}

	private void registerGauge(MeterRegistry registry, String name, List<String> tag,
			DoubleSupplier value) {
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
		builder.register(registry);
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

	/** What a series recorded since the last line: how many, their average and their largest (durations in ms). */
	record Window(long count, double avg, double max) {}

	private void log() {
		var window = new TreeMap<String, Window>();
		collect(window, durations, 1e6);
		var durationNames = List.copyOf(window.keySet());
		collect(window, amounts, 1);
		if (window.isEmpty()) {
			return;
		}
		var levels = new TreeMap<String, Long>();
		for (var gauge : new TreeMap<>(gauges).entrySet()) {
			try {
				levels.put(gauge.getKey(), gauge.getValue().getAsLong());
			} catch (RuntimeException ignored) {
				// Not available right now.
			}
		}
		LOG.info("{}", summary(nodeId, INTERVAL_SECONDS, window, levels));
		if (LOG.isDebugEnabled()) {
			// Every series, as one line: for a search in the journal, not for reading. The same numbers are exported
			// to the metrics registry.
			var line = new StringBuilder();
			window.forEach((name, w) -> {
				String unit = durationNames.contains(name) ? "ms" : "";
				line.append(String.format(Locale.ROOT, " %s=%d×avg %.2f%s max %.2f%s;", name, w.count(), w.avg(), unit,
						w.max(), unit));
			});
			levels.forEach((name, value) -> line.append(' ').append(name).append('=').append(value).append(';'));
			LOG.debug("{}: last {}s:{}", nodeId, INTERVAL_SECONDS, line);
		}
	}

	private static void collect(Map<String, Window> window, Map<String, Series> series, double scale) {
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
			window.put(entry.getKey(), new Window(count, sum / (double) count / scale, max / scale));
		}
	}

	/**
	 * The line a person reads: what the node did since the last one, in words and per second (entries applied, client
	 * writes, commits, reads, each with its average and slowest time), the delay from the leader's log to the apply,
	 * the backlog, the conflicts and the heap. A part with nothing to say is left out; a node that did nothing says
	 * "idle" on one line; otherwise a line of work and a line of health under a heading. {@code seconds} is the length
	 * of the window, to turn counts into rates. Every series is in the debug
	 * line and in the exported metrics.
	 */
	static String summary(String nodeId, long seconds, Map<String, Window> window, Map<String, Long> levels) {
		var parts = new ArrayList<String>();
		activity(parts, "applying %s entries/s", window.get("apply"), seconds);
		activity(parts, "client writes %s/s", window.get("writes.batch"), seconds);
		activity(parts, "commits %s/s", window.get("commit"), seconds);
		activity(parts, "reads %s/s", window.get("local"), seconds);
		var work = List.copyOf(parts);
		parts.clear();
		var delay = window.get("raft.appendToApply");
		if (delay != null) {
			parts.add("delay from log to apply avg " + ms(delay.avg()) + ", worst " + ms(delay.max()));
		}
		double backlog = 0;
		for (var lag : List.of("lag.unapplied", "lag.uncommitted", "lag.unflushed")) {
			var w = window.get(lag);
			if (w != null) {
				backlog = Math.max(backlog, w.max());
			}
		}
		if (backlog > 0) {
			parts.add(String.format(Locale.ROOT, "backlog up to %,d entries", (long) backlog));
		}
		long conflicts = 0;
		for (var name : List.of("transaction.conflict", "transaction.chainConflict", "update.chainConflict")) {
			var w = window.get(name);
			if (w != null) {
				conflicts += w.count();
			}
		}
		if (conflicts > 0) {
			parts.add(String.format(Locale.ROOT, "%,d transaction conflict%s", conflicts, conflicts == 1 ? "" : "s"));
		}
		Long open = levels.get("leader.openTransactions");
		if (open != null && open > 0) {
			parts.add(String.format(Locale.ROOT, "%,d open transaction%s", open, open == 1 ? "" : "s"));
		}
		Long heap = levels.get("heapUsedMiB");
		if (heap != null) {
			parts.add(heap < 1024 ? "heap " + heap + " MB" : String.format(Locale.ROOT, "heap %.1f GB", heap / 1024.0));
		}
		if (work.isEmpty()) {
			return nodeId + ": idle" + (parts.isEmpty() ? "" : " | " + String.join(" | ", parts));
		}
		// Three lines at most: what it did, then how it is doing.
		return nodeId + ", last " + seconds + " seconds:\n  work:   " + String.join(" | ", work)
				+ (parts.isEmpty() ? "" : "\n  health: " + String.join(" | ", parts));
	}

	private static void activity(List<String> parts, String what, @Nullable Window w, long seconds) {
		if (w != null) {
			double rate = w.count() / (double) seconds;
			String perSecond = rate >= 10 ? String.format(Locale.ROOT, "%,d", Math.round(rate))
					: String.format(Locale.ROOT, "%.1f", rate);
			parts.add(String.format(Locale.ROOT, what, perSecond) + " (avg " + ms(w.avg()) + ", slowest " + ms(w.max()) + ")");
		}
	}

	/** A duration with the digits that matter and its unit: 0.46 ms, 4.4 ms, 73 ms, 2.5 s. */
	private static String ms(double value) {
		if (value >= 1000) {
			return String.format(Locale.ROOT, "%.1f s", value / 1000);
		}
		return String.format(Locale.ROOT, value < 1 ? "%.2f ms" : value < 10 ? "%.1f ms" : "%.0f ms", value);
	}

	synchronized void close() {
		logger.shutdownNow();
		var thread = samplerThread;
		if (thread != null) {
			thread.shutdownNow();
		}
	}
}
