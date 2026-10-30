package it.cavallium.rockserver.core.impl.benchmark;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.impl.NativeStaging;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.function.LongSupplier;

/**
 * Opt-in microbenchmark for {@link NativeStaging}: the legacy per-key {@code Arena.allocate} staging and the
 * byte-by-byte equality loop against the single-block slicing and {@link MemorySegment#mismatch}.
 *
 * <p>Not a JUnit test. Run after {@code test-compile}:
 * {@code java --enable-native-access=ALL-UNNAMED [--add-modules jdk.incubator.vector]
 * -cp target/classes:target/test-classes:<deps> it.cavallium.rockserver.core.impl.benchmark.NativeStagingBenchmark}
 */
public final class NativeStagingBenchmark {

	private static final int ROUNDS = 15;
	private static final long ROUND_NANOS = 200_000_000L;
	private static volatile long sink;

	private NativeStagingBenchmark() {
	}

	public static void main(String[] args) {
		System.out.println("java.version=" + System.getProperty("java.version") + " vectorModule="
				+ ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent());
		var random = new SplittableRandom(42);
		for (int count : new int[] {16, 256}) {
			var keys = randomKeys(random, count, 12, 19);
			compare("existsMulti staging " + count + " keys 12-19B",
					() -> legacyExistsStaging(keys),
					() -> newExistsStaging(keys));
		}
		for (int count : new int[] {16, 64}) {
			var keys = new byte[count][];
			var valueSizes = new long[count];
			for (int i = 0; i < count; i++) {
				keys[i] = randomBytes(random, 12 + random.nextInt(8));
				valueSizes[i] = 512 + random.nextInt(2561);
			}
			compare("write-elision staging " + count + " keys + 0.5-3KiB values",
					() -> legacyProbeStaging(keys, valueSizes),
					() -> newProbeStaging(keys, valueSizes));
		}
		try (var arena = Arena.ofConfined()) {
			for (int size : new int[] {512, 1536, 3072}) {
				byte[] bytes = randomBytes(random, size);
				var expected = Buf.wrap(bytes.clone());
				var actual = arena.allocate(size, 1);
				MemorySegment.copy(bytes, 0, actual, ValueLayout.JAVA_BYTE, 0, size);
				compare("equality " + size + "B (equal)",
						() -> legacyEquals(actual, expected, size) ? 1 : 0,
						() -> NativeStaging.contentEquals(actual, expected, size) ? 1 : 0);
			}
		}
	}

	private static List<Buf> randomKeys(SplittableRandom random, int count, int minLength, int maxLength) {
		var keys = new ArrayList<Buf>(count);
		for (int i = 0; i < count; i++) {
			keys.add(Buf.wrap(randomBytes(random, minLength + random.nextInt(maxLength - minLength + 1))));
		}
		return keys;
	}

	private static byte[] randomBytes(SplittableRandom random, int length) {
		var bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = (byte) random.nextInt(256);
		}
		return bytes;
	}

	// Legacy code, verbatim from EmbeddedDB before NativeStaging

	private static MemorySegment legacyCopy(Arena arena, Buf value) {
		var segment = arena.allocate(value.size());
		MemorySegment.copy(value.getBackingByteArray(), value.getBackingByteArrayOffset(), segment,
				ValueLayout.JAVA_BYTE, 0, value.getBackingByteArrayLength());
		return segment;
	}

	private static boolean legacyEquals(MemorySegment actual, Buf expected, int length) {
		if (length != expected.size() || actual.byteSize() < length) {
			return false;
		}
		for (int i = 0; i < length; i++) {
			if (actual.get(ValueLayout.JAVA_BYTE, i) != expected.getByte(i)) {
				return false;
			}
		}
		return true;
	}

	private static long legacyExistsStaging(List<Buf> keys) {
		long acc = 0;
		try (var arena = Arena.ofConfined()) {
			var nativeKeys = new MemorySegment[keys.size()];
			var emptyValues = new MemorySegment[keys.size()];
			var emptyValue = arena.allocate(0);
			for (int i = 0; i < keys.size(); i++) {
				nativeKeys[i] = legacyCopy(arena, keys.get(i));
				emptyValues[i] = emptyValue;
			}
			for (var key : nativeKeys) {
				acc += key.byteSize() + key.get(ValueLayout.JAVA_BYTE, 0);
			}
		}
		return acc;
	}

	private static long newExistsStaging(List<Buf> keys) {
		long acc = 0;
		try (var arena = Arena.ofConfined()) {
			var nativeKeys = NativeStaging.stageKeys(arena, keys);
			var emptyValues = new MemorySegment[keys.size()];
			Arrays.fill(emptyValues, arena.allocate(0));
			for (var key : nativeKeys) {
				acc += key.byteSize() + key.get(ValueLayout.JAVA_BYTE, 0);
			}
		}
		return acc;
	}

	private static long legacyProbeStaging(byte[][] keys, long[] valueSizes) {
		long acc = 0;
		try (var arena = Arena.ofConfined()) {
			var nativeKeys = new MemorySegment[keys.length];
			var nativeValues = new MemorySegment[keys.length];
			for (int i = 0; i < keys.length; i++) {
				nativeKeys[i] = arena.allocateFrom(ValueLayout.JAVA_BYTE, keys[i]);
				nativeValues[i] = arena.allocate(valueSizes[i]);
			}
			for (int i = 0; i < keys.length; i++) {
				acc += nativeKeys[i].byteSize() + nativeValues[i].byteSize();
			}
		}
		return acc;
	}

	private static long newProbeStaging(byte[][] keys, long[] valueSizes) {
		long acc = 0;
		try (var arena = Arena.ofConfined()) {
			var nativeKeys = NativeStaging.stageKeys(arena, keys);
			var nativeValues = NativeStaging.allocateSlices(arena, valueSizes);
			for (int i = 0; i < keys.length; i++) {
				acc += nativeKeys[i].byteSize() + nativeValues[i].byteSize();
			}
		}
		return acc;
	}

	private static void compare(String name, LongSupplier legacy, LongSupplier optimized) {
		if (legacy.getAsLong() != optimized.getAsLong()) {
			throw new AssertionError(name + ": legacy and optimized results differ");
		}
		// Warmup
		measure(legacy, 1_000_000_000L);
		measure(optimized, 1_000_000_000L);
		var legacyNs = new double[ROUNDS];
		var optimizedNs = new double[ROUNDS];
		for (int round = 0; round < ROUNDS; round++) {
			legacyNs[round] = measure(legacy, ROUND_NANOS);
			optimizedNs[round] = measure(optimized, ROUND_NANOS);
		}
		Arrays.sort(legacyNs);
		Arrays.sort(optimizedNs);
		double legacyMedian = legacyNs[ROUNDS / 2];
		double optimizedMedian = optimizedNs[ROUNDS / 2];
		System.out.printf(Locale.ROOT, "%-52s legacy %10.1f ns/op  new %10.1f ns/op  speedup %6.2fx%n",
				name, legacyMedian, optimizedMedian, legacyMedian / optimizedMedian);
	}

	private static double measure(LongSupplier op, long budgetNanos) {
		long acc = 0;
		long ops = 0;
		long start = System.nanoTime();
		long elapsed;
		do {
			for (int i = 0; i < 64; i++) {
				acc += op.getAsLong();
			}
			ops += 64;
			elapsed = System.nanoTime() - start;
		} while (elapsed < budgetNanos);
		sink += acc;
		return (double) elapsed / ops;
	}
}
