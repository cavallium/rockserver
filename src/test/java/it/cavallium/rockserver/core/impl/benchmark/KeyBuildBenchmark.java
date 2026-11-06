package it.cavallium.rockserver.core.impl.benchmark;

import com.sun.management.ThreadMXBean;
import it.cavallium.buffer.Buf;
import it.cavallium.buffer.MemorySegmentBuf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.cavallium.rockserver.core.impl.test.LegacyColumnKeyBuilder;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Single-thread microbenchmark of composite-key building with the production key shapes.
 *
 * <p>{@code legacy} is a frozen copy of the old {@code calculateKey(keys).toByteArray()} path, {@code key} is the
 * current {@code calculateKey(keys).toByteArray()} and {@code bytes} is {@code calculateKeyBytes(keys)} when the
 * method exists. Arguments optionally filter the op, the shape and the input kind: one case per JVM keeps the call
 * sites monomorphic, while the default run mixes all Buf kinds as the server does. Each case reports the median ns/op of several rounds and the allocated bytes/op.</p>
 */
public final class KeyBuildBenchmark {

	private static final int TUPLES = 1024;
	private static final int ROUNDS = 7;
	private static final int OPS_PER_ROUND = 4_000_000;

	private static volatile long sink;

	private KeyBuildBenchmark() {
	}

	private record Shape(String name, ColumnSchema schema, int minTail, int maxTail) {
	}

	private interface Op {
		byte[] apply(Buf[] keys) throws Throwable;
	}

	public static void main(String[] args) throws Throwable {
		var shapes = List.of(
				new Shape("[8,4]", ColumnSchema.of(IntList.of(8, 4), ObjectList.of(), true), 0, 0),
				new Shape("[8,8,3]", ColumnSchema.of(IntList.of(8, 8, 3), ObjectList.of(), true), 0, 0),
				new Shape("[14,16,1,8]", ColumnSchema.of(IntList.of(14, 16, 1, 8), ObjectList.of(), true), 0, 0),
				new Shape("[19]", ColumnSchema.of(IntList.of(19), ObjectList.of(), true), 0, 0),
				new Shape("[4]+XXH32x2", ColumnSchema.of(IntList.of(4),
						ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH32), true), 40, 120),
				new Shape("[8]+XXH8", ColumnSchema.of(IntList.of(8), ObjectList.of(ColumnHashType.XXHASH8), true), 8, 24)
		);
		// Optional filters: op (legacy|key|bytes), shape name, input kind (heap|slice|native)
		String only = args.length > 0 ? args[0] : "";
		String onlyShape = args.length > 1 ? args[1] : "";
		String onlyKind = args.length > 2 ? args[2] : "";
		var threadMetrics = (ThreadMXBean) ManagementFactory.getThreadMXBean();
		threadMetrics.setThreadAllocatedMemoryEnabled(true);

		MethodHandle bytesHandle;
		try {
			bytesHandle = MethodHandles.publicLookup().findVirtual(ColumnInstance.class, "calculateKeyBytes",
					MethodType.methodType(byte[].class, Buf[].class));
		} catch (NoSuchMethodException e) {
			bytesHandle = null;
		}
		System.out.printf("vector module: %s%n", ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent());
		System.out.printf("%-14s %-8s %-7s %10s %10s%n", "shape", "input", "op", "ns/op", "B/op");
		for (var shape : shapes) {
			if (!onlyShape.isEmpty() && !onlyShape.equals(shape.name())) {
				continue;
			}
			var col = new ColumnInstance(null, shape.schema());
			for (String kind : List.of("heap", "slice", "native")) {
				if (!onlyKind.isEmpty() && !onlyKind.equals(kind)) {
					continue;
				}
				var tuples = generate(shape, kind);
				var ops = new ArrayList<String>();
				ops.add("legacy");
				ops.add("key");
				if (bytesHandle != null) {
					ops.add("bytes");
				}
				for (String opName : ops) {
					if (!only.isEmpty() && !only.equals(opName)) {
						continue;
					}
					final MethodHandle mh = bytesHandle;
					Op op = switch (opName) {
						case "legacy" -> keys -> LegacyColumnKeyBuilder.calculateKey(shape.schema(), keys).toByteArray();
						case "key" -> keys -> col.calculateKey(keys).toByteArray();
						default -> keys -> (byte[]) mh.invokeExact(col, keys);
					};
					double[] ns = new double[ROUNDS];
					long allocated = 0;
					for (int r = 0; r < ROUNDS; r++) {
						long a0 = threadMetrics.getCurrentThreadAllocatedBytes();
						long t0 = System.nanoTime();
						long acc = 0;
						for (int i = 0; i < OPS_PER_ROUND; i++) {
							byte[] out = op.apply(tuples[i & (TUPLES - 1)]);
							acc += out.length + out[out.length - 1];
						}
						long t1 = System.nanoTime();
						long a1 = threadMetrics.getCurrentThreadAllocatedBytes();
						sink += acc;
						ns[r] = (t1 - t0) / (double) OPS_PER_ROUND;
						allocated = a1 - a0;
					}
					Arrays.sort(ns);
					System.out.printf("%-14s %-8s %-7s %10.2f %10.1f%n", shape.name(), kind, opName, ns[ROUNDS / 2],
							allocated / (double) OPS_PER_ROUND);
				}
			}
		}
	}

	private static Buf[][] generate(Shape shape, String kind) {
		var random = new SplittableRandom(42);
		var schema = shape.schema();
		var tuples = new Buf[TUPLES][];
		for (int t = 0; t < TUPLES; t++) {
			var keys = new Buf[schema.keysCount()];
			for (int i = 0; i < keys.length; i++) {
				int len = i < schema.fixedLengthKeysCount() ? schema.key(i)
						: shape.minTail() + random.nextInt(shape.maxTail() - shape.minTail() + 1);
				byte[] data = new byte[len];
				random.nextBytes(data);
				keys[i] = switch (kind) {
					case "heap" -> Buf.wrap(data);
					case "slice" -> {
						byte[] padded = new byte[len + 7];
						System.arraycopy(data, 0, padded, 3, len);
						yield Buf.wrap(padded, 3, 3 + len);
					}
					default -> {
						var segment = Arena.global().allocate(len);
						segment.copyFrom(MemorySegment.ofArray(data));
						yield new MemorySegmentBuf(segment);
					}
				};
			}
			tuples[t] = keys;
		}
		return tuples;
	}
}
