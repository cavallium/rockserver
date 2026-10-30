package it.cavallium.rockserver.distributed;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cheap counters of where replicated requests spend their time (proposer queue, Raft round trip, apply, read
 * barrier...), logged as one line every {@link #INTERVAL_SECONDS} seconds when there was activity. Each series keeps
 * the count, the sum and the maximum of its values over the interval.
 */
final class NodeMetrics {

	private static final Logger LOG = LoggerFactory.getLogger(NodeMetrics.class);
	private static final long INTERVAL_SECONDS = 10;

	/** One series: values are durations in nanoseconds, or plain amounts for {@link #amount}. */
	static final class Series {
		private final LongAdder count = new LongAdder();
		private final LongAdder sum = new LongAdder();
		private final AtomicLong max = new AtomicLong();
		/** Values already logged. Written by the logger only. */
		private volatile long logged;

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
			return logged + count.sum();
		}
	}

	private final String nodeId;
	private final Map<String, Series> durations = new ConcurrentHashMap<>();
	private final Map<String, Series> amounts = new ConcurrentHashMap<>();
	private final ScheduledExecutorService logger;

	NodeMetrics(String nodeId) {
		this.nodeId = nodeId;
		this.logger = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon()
				.name("rockserver-metrics-" + nodeId).factory());
		logger.scheduleAtFixedRate(this::log, INTERVAL_SECONDS, INTERVAL_SECONDS, TimeUnit.SECONDS);
	}

	/** Runs {@code sampler} every millisecond until {@link #close()}, to record gauges into series. */
	void sampleEveryMillisecond(Runnable sampler) {
		logger.scheduleAtFixedRate(() -> {
			try {
				sampler.run();
			} catch (RuntimeException ignored) {
				// Not ready yet, or stopping.
			}
		}, 1, 1, TimeUnit.MILLISECONDS);
	}

	private final Map<String, java.util.function.LongSupplier> gauges = new ConcurrentHashMap<>();

	/** A value read and logged at every interval, such as a queue length. */
	void gauge(String name, java.util.function.LongSupplier value) {
		gauges.put(name, value);
	}

	/** A series of durations in nanoseconds, logged in milliseconds. */
	Series duration(String name) {
		return durations.computeIfAbsent(name, _ -> new Series());
	}

	/** A series of plain amounts, such as batch sizes. */
	Series amount(String name) {
		return amounts.computeIfAbsent(name, _ -> new Series());
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
			long count = s.count.sumThenReset();
			s.logged += count;
			long sum = s.sum.sumThenReset();
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
