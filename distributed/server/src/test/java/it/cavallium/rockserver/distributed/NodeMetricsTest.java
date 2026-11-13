package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepRegistryConfig;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * R60-2: the replication meters of {@link NodeMetrics} move to the registry of every database the node opens (start,
 * and each snapshot install). The InfluxDB registry is a step registry, which publishes a function counter as the
 * difference of its function between steps, from 0 when the meter is made. Exported on the lifetime totals, the first
 * step after a rebind carried every count since the node started: an old checksum mismatch, leader stall or failed
 * install record fired its alert again (rules with {@code for: 0s} on {@code sum("value")}), and rates double-counted.
 * A rebind now exports only what the registries bound before have not read.
 */
class NodeMetricsTest {

	private static final Duration STEP = Duration.ofMinutes(1);

	/** A step registry that publishes nothing: the test reads each meter's last step, as a publish would. */
	private static final class StepRegistry extends StepMeterRegistry {
		StepRegistry(MockClock clock) {
			super(new StepRegistryConfig() {
				@Override
				public String prefix() {
					return "test";
				}

				@Override
				public String get(String key) {
					return null;
				}

				@Override
				public Duration step() {
					return STEP;
				}
			}, clock);
		}

		@Override
		protected void publish() {
		}

		@Override
		protected TimeUnit getBaseTimeUnit() {
			return TimeUnit.MILLISECONDS;
		}

		/**
		 * Reads every function meter, as the registry's polling thread does once per step ({@code
		 * pollMetersToRollover}): what is read is what the registry publishes for the step.
		 */
		void poll() {
			getMeters().forEach(meter -> meter.match(gauge -> null, counter -> null, timer -> null, summary -> null,
					longTask -> null, timeGauge -> null, FunctionCounter::count, FunctionTimer::count, other -> null));
		}

		FunctionCounter counter(String name, String series) {
			return find(name).tag("name", series).functionCounter();
		}

		FunctionTimer timer(String series) {
			return find("rockserver.replication.duration").tag("name", series).functionTimer();
		}
	}

	/** Ends the current step: each registry polls its meters, then the clock moves to the next step. */
	private static void step(MockClock clock, StepRegistry... registries) {
		for (var registry : registries) {
			registry.poll();
		}
		clock.add(STEP);
	}

	@Test
	void aRebindPublishesOnlyWhatThePreviousRegistryDidNotRead() {
		var clock = new MockClock();
		var metrics = new NodeMetrics("n1");
		try {
			var mismatches = metrics.amount("checksum.mismatch");
			var apply = metrics.duration("apply");
			mismatches.record(1);
			apply.record(TimeUnit.MILLISECONDS.toNanos(4));

			// The first database: what came before it is published once, in its first step.
			var first = new StepRegistry(clock);
			metrics.bind(first);
			step(clock, first);
			assertEquals(1, first.counter("rockserver.replication.count", "checksum.mismatch").count());
			assertEquals(1, first.counter("rockserver.replication.amount", "checksum.mismatch").count());
			assertEquals(1, first.timer("apply").count());
			assertEquals(4, first.timer("apply").totalTime(TimeUnit.MILLISECONDS), 0.001);

			mismatches.record(2);
			step(clock, first);
			assertEquals(1, first.counter("rockserver.replication.count", "checksum.mismatch").count());
			assertEquals(2, first.counter("rockserver.replication.amount", "checksum.mismatch").count());
			assertEquals(0, first.timer("apply").count());
			// A snapshot install closes the database, and its registry with it.
			first.close();

			// The next database: its first step holds only what happened since, not the node's lifetime totals.
			var second = new StepRegistry(clock);
			metrics.bind(second);
			apply.record(TimeUnit.MILLISECONDS.toNanos(6));
			step(clock, second);
			assertEquals(0, second.counter("rockserver.replication.count", "checksum.mismatch").count());
			assertEquals(0, second.counter("rockserver.replication.amount", "checksum.mismatch").count());
			assertEquals(1, second.timer("apply").count());
			assertEquals(6, second.timer("apply").totalTime(TimeUnit.MILLISECONDS), 0.001);

			mismatches.record(1);
			step(clock, second);
			assertEquals(1, second.counter("rockserver.replication.count", "checksum.mismatch").count());
			assertEquals(0, second.timer("apply").count());
		} finally {
			metrics.close();
		}
	}

	/**
	 * A registry left open when the node binds the next one (nothing closes it) stops at what it read: the same
	 * values are not published by both.
	 */
	@Test
	void aRegistryBoundBeforeStopsAtTheRebind() {
		var clock = new MockClock();
		var metrics = new NodeMetrics("n1");
		try {
			var stalls = metrics.amount("leader.stall");
			var first = new StepRegistry(clock);
			metrics.bind(first);
			stalls.record(1);
			step(clock, first);
			assertEquals(1, first.counter("rockserver.replication.count", "leader.stall").count());

			stalls.record(1);
			var second = new StepRegistry(clock);
			metrics.bind(second);
			stalls.record(1);
			step(clock, first, second);
			long both = Math.round(first.counter("rockserver.replication.count", "leader.stall").count()
					+ second.counter("rockserver.replication.count", "leader.stall").count());
			assertEquals(2, both, "each value is published by one registry only");
			assertEquals(2, second.counter("rockserver.replication.count", "leader.stall").count());

			stalls.record(1);
			step(clock, first, second);
			assertEquals(0, first.counter("rockserver.replication.count", "leader.stall").count());
			assertEquals(1, second.counter("rockserver.replication.count", "leader.stall").count());
			first.close();
			second.close();
		} finally {
			metrics.close();
		}
	}

	@org.junit.jupiter.api.Test
	void theLineOfEveryTenSecondsIsShortAndSaysWhatTheNodeDid() {
		var window = new java.util.TreeMap<String, NodeMetrics.Window>();
		window.put("apply", new NodeMetrics.Window(5337, 0.46, 12.82));
		window.put("apply.commit", new NodeMetrics.Window(5337, 0.05, 0.37));
		window.put("writes.batch", new NodeMetrics.Window(7098, 4.35, 29.49));
		window.put("commit", new NodeMetrics.Window(659, 4.24, 20.97));
		window.put("local", new NodeMetrics.Window(6785, 0.07, 3.63));
		window.put("raft.appendToApply", new NodeMetrics.Window(7745, 3.83, 29.16));
		window.put("lag.unapplied", new NodeMetrics.Window(1000, 0.54, 9));
		window.put("lag.uncommitted", new NodeMetrics.Window(1000, 2.72, 33));
		window.put("transaction.conflict", new NodeMetrics.Window(11, 1, 1));
		var levels = java.util.Map.of("heapUsedMiB", 1740L, "leader.openTransactions", 1L, "leader.trackedKeys", 9L);
		org.junit.jupiter.api.Assertions.assertEquals("""
				n1, last 10 seconds:
				  work:   applying 534 entries/s (avg 0.46 ms, slowest 13 ms) | client writes 710/s (avg 4.4 ms, slowest 29 ms) \
				| commits 66/s (avg 4.2 ms, slowest 21 ms) | reads 679/s (avg 0.07 ms, slowest 3.6 ms)
				  health: delay from log to apply avg 3.8 ms, worst 29 ms | backlog up to 33 entries \
				| 11 transaction conflicts | 1 open transaction | heap 1.7 GB""",
				NodeMetrics.summary("n1", 10, window, levels));
		org.junit.jupiter.api.Assertions.assertEquals("n3: idle | heap 512 MB", NodeMetrics.summary("n3", 10,
				java.util.Map.of("lag.unapplied", new NodeMetrics.Window(1000, 0, 0)), java.util.Map.of("heapUsedMiB", 512L)));
		org.junit.jupiter.api.Assertions.assertEquals("n2, last 10 seconds:\n  work:   client writes 0.2/s (avg 8.9 ms, slowest 2.5 s)",
				NodeMetrics.summary("n2", 10, java.util.Map.of("writes.batch", new NodeMetrics.Window(2, 8.88, 2500)),
						java.util.Map.of()));
	}
}
