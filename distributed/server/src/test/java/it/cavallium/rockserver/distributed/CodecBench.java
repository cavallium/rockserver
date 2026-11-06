package it.cavallium.rockserver.distributed;

import com.sun.management.ThreadMXBean;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * Microbenchmark of the Raft codecs: the frozen {@link OldLogEntryCodec}/{@link OldResultCodec} against the current
 * ones, on the shapes seen in production. Not a test: run its main with the test classpath. Reports the median
 * nanoseconds and allocated bytes per operation over several rounds, old and new interleaved.
 */
final class CodecBench {

	private static final ThreadMXBean THREADS =
			(ThreadMXBean) ManagementFactory.getThreadMXBean();
	private static final Random RANDOM = new Random(42);
	private static volatile long sink;

	private record Shape(String name, List<Request> requests, List<ApplyResult> results) {}

	public static void main(String[] args) {
		long roundNanos = args.length > 0 ? Long.parseLong(args[0]) * 1_000_000L : 400_000_000L;
		int rounds = args.length > 1 ? Integer.parseInt(args[1]) : 7;
		var shapes = List.of(
				new Shape("put 12B key + 512B", batch(one(put(512))), okNulls(1)),
				new Shape("put 12B key + 1.5KiB", batch(one(put(1536))), okNulls(1)),
				new Shape("put 12B key + 3KiB", batch(one(put(3072))), okNulls(1)),
				new Shape("16 puts x 1.5KiB batch", batch(repeat(16, () -> put(1536))), okNulls(16)),
				new Shape("putMulti 16 x [14,16,1,8] 1-3KiB", batch(one(putMulti(16))), okNulls(1)),
				new Shape("guarded commit, 8 writes", batch(one(guarded(8))), okNulls(1)),
				new Shape("16 results Ok(1.5KiB)", batch(one(put(16))), okBufs(16, 1536)));
		for (var shape : shapes) {
			byte[] oldBatch = oldEncode(shape.requests);
			byte[] newBatch = newEncode(shape.requests);
			if (!Arrays.equals(oldBatch, newBatch)
					|| !Arrays.equals(OldResultCodec.encodeAll(shape.results), ResultCodec.encodeAll(shape.results))) {
				throw new AssertionError("Encodings differ: " + shape.name);
			}
			ByteString batchBytes = UnsafeByteOperations.unsafeWrap(newBatch);
			ByteString resultBytes = UnsafeByteOperations.unsafeWrap(ResultCodec.encodeAll(shape.results));
			System.out.printf(Locale.ROOT, "%n== %s (batch %d B, reply %d B)%n", shape.name, newBatch.length,
					resultBytes.size());
			compare("encode batch", roundNanos, rounds, _ -> oldEncode(shape.requests).length,
					_ -> newEncode(shape.requests).length);
			compare("decode batch", roundNanos, rounds, _ -> OldLogEntryCodec.decodeBatch(batchBytes.toByteArray()).size(),
					_ -> newDecode(batchBytes));
			compare("encode results", roundNanos, rounds, _ -> OldResultCodec.encodeAll(shape.results).length,
					_ -> ResultCodec.encodeAll(shape.results).length);
			compare("decode results", roundNanos, rounds,
					_ -> OldResultCodec.decodeAll(resultBytes.toByteArray()).size(), _ -> newDecodeResults(resultBytes));
		}
	}

	/** As the proposer did: a body per request when submitted, then the batch when sent. */
	private static byte[] oldEncode(List<Request> requests) {
		var sequences = new long[requests.size()];
		var bodies = new ArrayList<byte[]>(requests.size());
		for (int i = 0; i < sequences.length; i++) {
			sequences[i] = requests.get(i).sequence();
			bodies.add(OldLogEntryCodec.encodeEntryBody(requests.get(i).entry()));
		}
		return OldLogEntryCodec.encodeBatch(requests.getFirst().proposerId(), requests.getFirst().completedBelow(),
				sequences, bodies);
	}

	private static byte[] newEncode(List<Request> requests) {
		var sequences = new long[requests.size()];
		var bodies = new ArrayList<byte[]>(requests.size());
		for (int i = 0; i < sequences.length; i++) {
			sequences[i] = requests.get(i).sequence();
			bodies.add(LogEntryCodec.encodeEntryBody(requests.get(i).entry()));
		}
		return LogEntryCodec.encodeBatch(requests.getFirst().proposerId(), requests.getFirst().completedBelow(),
				sequences, bodies);
	}

	private static long newDecode(ByteString bytes) {
		return LogEntryCodec.decodeBatch(bytes.asReadOnlyByteBuffer()).size();
	}

	private static long newDecodeResults(ByteString bytes) {
		return ResultCodec.decodeAll(bytes.asReadOnlyByteBuffer()).size();
	}

	private static void compare(String name, long roundNanos, int rounds, ToLongFunction<Object> old,
			ToLongFunction<Object> current) {
		// Warmup of both
		run(old, roundNanos * 2);
		run(current, roundNanos * 2);
		var oldNs = new double[rounds];
		var oldBytes = new double[rounds];
		var newNs = new double[rounds];
		var newBytes = new double[rounds];
		for (int r = 0; r < rounds; r++) {
			double[] o = run(old, roundNanos);
			double[] n = run(current, roundNanos);
			oldNs[r] = o[0];
			oldBytes[r] = o[1];
			newNs[r] = n[0];
			newBytes[r] = n[1];
		}
		double on = median(oldNs);
		double nn = median(newNs);
		System.out.printf(Locale.ROOT, "  %-15s old %9.1f ns %8.0f B | new %9.1f ns %8.0f B | speedup %.2fx%n", name,
				on, median(oldBytes), nn, median(newBytes), on / nn);
	}

	/** Nanoseconds and allocated bytes per call, over about {@code nanos}. */
	private static double[] run(ToLongFunction<Object> op, long nanos) {
		long calls = 0;
		long acc = 0;
		long allocStart = THREADS.getCurrentThreadAllocatedBytes();
		long start = System.nanoTime();
		long end = start + nanos;
		long now;
		do {
			for (int i = 0; i < 256; i++) {
				acc += op.applyAsLong(null);
			}
			calls += 256;
			now = System.nanoTime();
		} while (now < end);
		long alloc = THREADS.getCurrentThreadAllocatedBytes() - allocStart;
		sink += acc;
		return new double[] {(double) (now - start) / calls, (double) alloc / calls};
	}

	private static double median(double[] values) {
		var sorted = values.clone();
		Arrays.sort(sorted);
		return sorted[sorted.length / 2];
	}

	private static List<Request> batch(List<LogEntry> entries) {
		var requests = new ArrayList<Request>();
		for (int i = 0; i < entries.size(); i++) {
			requests.add(new Request(0x1234_5678_9ABCL, 1000 + i, 990, entries.get(i)));
		}
		return requests;
	}

	private static List<LogEntry> one(LogEntry entry) {
		return List.of(entry);
	}

	private static List<LogEntry> repeat(int count, Supplier<LogEntry> entry) {
		var entries = new ArrayList<LogEntry>();
		for (int i = 0; i < count; i++) {
			entries.add(entry.get());
		}
		return entries;
	}

	private static Buf random(int size) {
		var bytes = new byte[size];
		RANDOM.nextBytes(bytes);
		return Buf.wrap(bytes);
	}

	private static LogEntry.Put put(int valueSize) {
		return new LogEntry.Put(7, new Keys(random(12)), random(valueSize), RequestTypeId.NOTHING);
	}

	private static LogEntry.PutMulti putMulti(int count) {
		var keys = new ArrayList<Keys>();
		var values = new ArrayList<Buf>();
		for (int i = 0; i < count; i++) {
			keys.add(new Keys(random(14), random(16), random(1), random(8)));
			values.add(random(1024 + RANDOM.nextInt(2048)));
		}
		return new LogEntry.PutMulti(9, keys, values, RequestTypeId.NOTHING);
	}

	private static LogEntry.Guarded guarded(int writes) {
		var list = new ArrayList<LogEntry.Write>();
		var columns = new long[writes];
		var hashes = new long[writes];
		for (int i = 0; i < writes; i++) {
			list.add(new LogEntry.Put(7, new Keys(random(12)), random(1024), RequestTypeId.NOTHING));
			columns[i] = 7;
			hashes[i] = RANDOM.nextLong();
		}
		return new LogEntry.Guarded(3, 100, columns, hashes, list, false);
	}

	private static List<ApplyResult> okNulls(int count) {
		return Collections.nCopies(count, ApplyResult.OK_NULL);
	}

	private static List<ApplyResult> okBufs(int count, int size) {
		var results = new ArrayList<ApplyResult>();
		for (int i = 0; i < count; i++) {
			results.add(new ApplyResult.Ok(random(size)));
		}
		return results;
	}
}
