package it.cavallium.rockserver.distributed.bench;

import it.cavallium.rockserver.core.common.RocksDBException;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Latency and outcome counters of one measured interval, per operation kind. Latencies are measured from the
 * operation's intended start (its slot in the open-loop schedule), so a stalled server shows up as latency instead of
 * fewer samples.
 */
final class BenchStats {

	/** Counters of one operation kind. */
	static final class Op {
		final Recorder latency = new Recorder(3);
		final LongAdder ok = new LongAdder();
		final LongAdder retries = new LongAdder();
		final LongAdder bytes = new LongAdder();
		final Map<String, LongAdder> errors = new ConcurrentHashMap<>();

		void success(long latencyNanos, long payloadBytes) {
			latency.recordValue(Math.max(1, latencyNanos / 1000));
			ok.increment();
			bytes.add(payloadBytes);
			TIMELINE_OK.increment();
		}

		void failure(Throwable error) {
			TIMELINE_ERRORS.increment();
			var kind = describe(error);
			errors.computeIfAbsent(kind, _ -> new LongAdder()).increment();
			SAMPLES.putIfAbsent(kind, String.valueOf(unwrap(error).getMessage()));
		}
	}

	/** Successes and failures of every operation, for the per-second timeline. */
	static final LongAdder TIMELINE_OK = new LongAdder();
	static final LongAdder TIMELINE_ERRORS = new LongAdder();

	/** The first message seen for each error kind, for the report. */
	static final Map<String, String> SAMPLES = new ConcurrentHashMap<>();

	private final Map<String, Op> ops = new ConcurrentHashMap<>();
	final LongAdder dropped = new LongAdder();

	Op op(String name) {
		return ops.computeIfAbsent(name, _ -> new Op());
	}

	/** One interval's results, after {@link #snapshot}. */
	record Row(String op, long ok, long errors, long retries, double perSecond, double mbPerSecond, double p50ms,
			double p99ms, double p999ms, double maxMs, Map<String, Long> errorKinds) {}

	/** Takes and resets every counter; {@code seconds} is the interval length. */
	Map<String, Row> snapshot(double seconds) {
		var rows = new TreeMap<String, Row>();
		for (var entry : ops.entrySet()) {
			var op = entry.getValue();
			Histogram histogram = op.latency.getIntervalHistogram();
			long ok = op.ok.sumThenReset();
			long retries = op.retries.sumThenReset();
			long bytes = op.bytes.sumThenReset();
			var kinds = new TreeMap<String, Long>();
			long errors = 0;
			for (var error : op.errors.entrySet()) {
				long count = error.getValue().sumThenReset();
				if (count > 0) {
					kinds.put(error.getKey(), count);
					errors += count;
				}
			}
			rows.put(entry.getKey(), new Row(entry.getKey(), ok, errors, retries, ok / seconds,
					bytes / seconds / (1 << 20), ms(histogram, 50), ms(histogram, 99), ms(histogram, 99.9),
					histogram.getMaxValue() / 1000.0, kinds));
		}
		return rows;
	}

	private static double ms(Histogram histogram, double percentile) {
		return histogram.getTotalCount() == 0 ? 0 : histogram.getValueAtPercentile(percentile) / 1000.0;
	}

	static String format(Map<String, Row> rows) {
		var out = new StringBuilder();
		out.append(String.format(Locale.ROOT, "%-14s %9s %9s %7s %7s %8s %8s %8s %9s%n", "op", "ok/s", "MiB/s",
				"err", "retry", "p50ms", "p99ms", "p999ms", "maxms"));
		double total = 0;
		for (var row : rows.values()) {
			total += row.perSecond();
			out.append(String.format(Locale.ROOT, "%-14s %9.0f %9.2f %7d %7d %8.2f %8.2f %8.2f %9.1f%n", row.op(),
					row.perSecond(), row.mbPerSecond(), row.errors(), row.retries(), row.p50ms(), row.p99ms(),
					row.p999ms(), row.maxMs()));
			if (!row.errorKinds().isEmpty()) {
				out.append("               errors: ").append(row.errorKinds()).append('\n');
			}
		}
		out.append(String.format(Locale.ROOT, "%-14s %9.0f%n", "TOTAL", total));
		return out.toString();
	}

	static Throwable unwrap(Throwable error) {
		var cause = error;
		while ((cause instanceof java.util.concurrent.CompletionException
				|| cause instanceof java.util.concurrent.ExecutionException) && cause.getCause() != null) {
			cause = cause.getCause();
		}
		return cause;
	}

	static String describe(Throwable error) {
		var cause = unwrap(error);
		if (cause instanceof RocksDBException rocks) {
			return rocks.getErrorUniqueId().name();
		}
		var status = io.grpc.Status.fromThrowable(cause);
		if (status.getCode() != io.grpc.Status.Code.UNKNOWN) {
			return "grpc-" + status.getCode();
		}
		return cause.getClass().getSimpleName();
	}
}
