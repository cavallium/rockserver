package it.cavallium.rockserver.core.impl.benchmark;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.ReadTier;
import org.rocksdb.RocksDB;

/**
 * Feasibility prototype for calling RocksDB's exported C get functions through FFM.
 *
 * <p><strong>Not production API:</strong> RocksJava exposes raw C++ pointers, while the C API expects opaque wrapper
 * structs. This prototype relies on the exact RocksDB 10.10.1 layout: {@code rocksdb_t} starts with a {@code DB*},
 * {@code rocksdb_column_family_handle_t} starts with a {@code ColumnFamilyHandle*}, and
 * {@code rocksdb_readoptions_t} starts with an inline {@code ReadOptions}. It therefore wraps the DB and column-family
 * pointers and passes the raw RocksJava {@code ReadOptions*} as the address of that first inline member. This is an ABI
 * experiment, not a supported interop contract.
 *
 * <p>The downcalls are intentionally non-critical. Both variants return an exact heap byte array and distinguish an
 * empty value from a missing key. The into-buffer route copies through native scratch memory. The pinned route avoids
 * that intermediate native copy and keeps the pinned value alive until the Java-array copy completes.
 */
public final class FfmCGetPrototype {

	private static final Linker LINKER = Linker.nativeLinker();
	private static final SymbolLookup LOOKUP;
	private static final MethodHandle GET_INTO_BUFFER_CF;
	private static final MethodHandle GET_PINNED_CF;
	private static final MethodHandle PINNED_GET_VALUE;
	private static final MethodHandle PINNED_DESTROY;
	private static final MethodHandle ROCKSDB_FREE;
	private static final int DEFAULT_ITERATIONS = 200_000;
	private static final int WARMUP_ITERATIONS = 20_000;
	private static final int SCRATCH_BYTES = 64 * 1024;
	private static final long MAX_ERROR_BYTES = 64 * 1024;
	private static volatile long blackhole;

	static {
		RocksDB.loadLibrary();
		LOOKUP = name -> SymbolLookup.loaderLookup().find(name)
				.or(() -> LINKER.defaultLookup().find(name));
		GET_INTO_BUFFER_CF = LINKER.downcallHandle(find("rocksdb_get_into_buffer_cf"),
				FunctionDescriptor.of(ValueLayout.JAVA_BYTE,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS,
						ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS));
		GET_PINNED_CF = LINKER.downcallHandle(find("rocksdb_get_pinned_cf_v2"),
				FunctionDescriptor.of(ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.ADDRESS,
						ValueLayout.JAVA_LONG,
						ValueLayout.ADDRESS));
		PINNED_GET_VALUE = LINKER.downcallHandle(find("rocksdb_pinnable_handle_get_value"),
				FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
		PINNED_DESTROY = LINKER.downcallHandle(find("rocksdb_pinnable_handle_destroy"),
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
		ROCKSDB_FREE = LINKER.downcallHandle(find("rocksdb_free"),
				FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
	}

	private FfmCGetPrototype() {
	}

	public static void main(String[] args) throws Throwable {
		int iterations = args.length == 0 ? DEFAULT_ITERATIONS : Integer.parseInt(args[0]);
		int valueBytes = args.length < 2 ? 1_024 : Integer.parseInt(args[1]);
		if (iterations <= 0) {
			throw new IllegalArgumentException("iterations must be positive");
		}
		if (valueBytes <= 0 || valueBytes > SCRATCH_BYTES) {
			throw new IllegalArgumentException("valueBytes must be in [1, " + SCRATCH_BYTES + "]");
		}

		Path target = Path.of("target").toAbsolutePath().normalize();
		Files.createDirectories(target);
		Path databasePath = Files.createTempDirectory(target, "ffm-c-get-");
		byte[] nonEmptyKey = "non-empty".getBytes(java.nio.charset.StandardCharsets.UTF_8);
		byte[] nonEmptyValue = new byte[valueBytes];
		Arrays.fill(nonEmptyValue, (byte) 0x5a);
		byte[] emptyKey = "empty".getBytes(java.nio.charset.StandardCharsets.UTF_8);
		byte[] emptyValue = new byte[0];
		byte[] missingKey = "missing".getBytes(java.nio.charset.StandardCharsets.UTF_8);

		try (var options = new Options().setCreateIfMissing(true)) {
			try (var db = RocksDB.open(options, databasePath.toString());
					var flushOptions = new FlushOptions().setWaitForFlush(true)) {
				db.put(nonEmptyKey, nonEmptyValue);
				db.put(emptyKey, emptyValue);
				db.flush(flushOptions);
			}

			// Reopen so correctness is proven against persisted state rather than only a live memtable.
			try (var db = RocksDB.open(options, databasePath.toString());
					var readOptions = new ReadOptions().setReadTier(ReadTier.READ_ALL_TIER);
					var arena = Arena.ofConfined()) {
				ColumnFamilyHandle columnFamily = db.getDefaultColumnFamily();
				var nonEmpty = NativeCall.create(arena, db, columnFamily, readOptions, nonEmptyKey);
				var empty = NativeCall.create(arena, db, columnFamily, readOptions, emptyKey);
				var missing = NativeCall.create(arena, db, columnFamily, readOptions, missingKey);

				assertBytes("into/non-empty", nonEmptyValue, getIntoBuffer(nonEmpty));
				assertBytes("into/empty", emptyValue, getIntoBuffer(empty));
				assertBytes("into/missing", null, getIntoBuffer(missing));
				assertBytes("pinned/non-empty", nonEmptyValue, getPinned(nonEmpty));
				assertBytes("pinned/empty", emptyValue, getPinned(empty));
				assertBytes("pinned/missing", null, getPinned(missing));
				System.out.println("READ_ALL correctness after reopen: PASS (non-empty, empty, missing; both FFM routes)");

				warmUp(db, columnFamily, readOptions, nonEmptyKey, nonEmpty);
				long jniNanos = benchmarkJni(db, columnFamily, readOptions, nonEmptyKey, iterations);
				long intoNanos = benchmarkInto(nonEmpty, iterations);
				long pinnedNanos = benchmarkPinned(nonEmpty, iterations);
				System.out.printf(Locale.ROOT,
						"Single-thread %,d ops (%d-byte value): JNI=%.1f ns/op, FFM-into=%.1f ns/op, FFM-pinned=%.1f ns/op, blackhole=%d%n",
						iterations,
						valueBytes,
						jniNanos / (double) iterations,
						intoNanos / (double) iterations,
						pinnedNanos / (double) iterations,
						blackhole);
			}
		} finally {
			deleteOwnedTemporaryDirectory(databasePath, target);
		}
	}

	private static MemorySegment find(String symbol) {
		return LOOKUP.find(symbol).orElseThrow(() -> new UnsatisfiedLinkError("Native symbol not found: " + symbol));
	}

	private static byte[] getIntoBuffer(NativeCall call) throws Throwable {
		call.resetOutputs();
		byte copied = (byte) GET_INTO_BUFFER_CF.invokeExact(call.databaseWrapper(),
				call.readOptions(),
				call.columnFamilyWrapper(),
				call.key(),
				call.keyLength(),
				call.scratch(),
				call.scratch().byteSize(),
				call.valueLengthOut(),
				call.foundOut(),
				call.errorOut());
		throwIfNativeError(call.errorOut());
		if (call.foundOut().get(ValueLayout.JAVA_BYTE, 0) == 0) {
			return null;
		}
		long valueLength = call.valueLengthOut().get(ValueLayout.JAVA_LONG, 0);
		if (copied == 0) {
			throw new IllegalStateException("Native scratch buffer is too small: required=" + valueLength
					+ ", capacity=" + call.scratch().byteSize());
		}
		byte[] value = new byte[Math.toIntExact(valueLength)];
		MemorySegment.copy(call.scratch(), 0, MemorySegment.ofArray(value), 0, valueLength);
		return value;
	}

	private static byte[] getPinned(NativeCall call) throws Throwable {
		call.resetOutputs();
		MemorySegment handle = (MemorySegment) GET_PINNED_CF.invokeExact(call.databaseWrapper(),
				call.readOptions(),
				call.columnFamilyWrapper(),
				call.key(),
				call.keyLength(),
				call.errorOut());
		if (handle.equals(MemorySegment.NULL)) {
			throwIfNativeError(call.errorOut());
			return null;
		}
		try {
			MemorySegment valueAddress = (MemorySegment) PINNED_GET_VALUE.invokeExact(handle, call.valueLengthOut());
			long valueLength = call.valueLengthOut().get(ValueLayout.JAVA_LONG, 0);
			byte[] value = new byte[Math.toIntExact(valueLength)];
			if (valueLength != 0) {
				if (valueAddress.equals(MemorySegment.NULL)) {
					throw new IllegalStateException("Pinned get returned a null address for " + valueLength + " bytes");
				}
				MemorySegment.copy(valueAddress.reinterpret(valueLength),
						0,
						MemorySegment.ofArray(value),
						0,
						valueLength);
			}
			return value;
		} finally {
			PINNED_DESTROY.invokeExact(handle);
		}
	}

	private static void throwIfNativeError(MemorySegment errorOut) throws Throwable {
		MemorySegment error = errorOut.get(ValueLayout.ADDRESS, 0);
		if (error.equals(MemorySegment.NULL)) {
			return;
		}
		String message;
		try {
			message = error.reinterpret(MAX_ERROR_BYTES).getString(0);
		} finally {
			ROCKSDB_FREE.invokeExact(error);
			errorOut.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
		}
		throw new IllegalStateException("RocksDB C API error: " + message);
	}

	private static void warmUp(RocksDB db,
			ColumnFamilyHandle columnFamily,
			ReadOptions readOptions,
			byte[] key,
			NativeCall call) throws Throwable {
		for (int i = 0; i < WARMUP_ITERATIONS; i++) {
			consume(db.get(columnFamily, readOptions, key));
			consume(getIntoBuffer(call));
			consume(getPinned(call));
		}
	}

	private static long benchmarkJni(RocksDB db,
			ColumnFamilyHandle columnFamily,
			ReadOptions readOptions,
			byte[] key,
			int iterations) throws Exception {
		long start = System.nanoTime();
		for (int i = 0; i < iterations; i++) {
			consume(db.get(columnFamily, readOptions, key));
		}
		return System.nanoTime() - start;
	}

	private static long benchmarkInto(NativeCall call, int iterations) throws Throwable {
		long start = System.nanoTime();
		for (int i = 0; i < iterations; i++) {
			consume(getIntoBuffer(call));
		}
		return System.nanoTime() - start;
	}

	private static long benchmarkPinned(NativeCall call, int iterations) throws Throwable {
		long start = System.nanoTime();
		for (int i = 0; i < iterations; i++) {
			consume(getPinned(call));
		}
		return System.nanoTime() - start;
	}

	private static void consume(byte[] value) {
		blackhole += value == null ? 0 : value.length == 0 ? 1 : value.length + value[0];
	}

	private static void assertBytes(String label, byte[] expected, byte[] actual) {
		if (!Arrays.equals(expected, actual)) {
			throw new AssertionError(label + " expected=" + Arrays.toString(expected)
					+ " actual=" + Arrays.toString(actual));
		}
	}

	private static void deleteOwnedTemporaryDirectory(Path directory, Path expectedParent) throws IOException {
		Path normalized = directory.toAbsolutePath().normalize();
		if (!normalized.getParent().equals(expectedParent) || !normalized.getFileName().toString().startsWith("ffm-c-get-")) {
			throw new IOException("Refusing to delete unexpected prototype directory: " + normalized);
		}
		try (var paths = Files.walk(normalized)) {
			for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(path);
			}
		}
	}

	private record NativeCall(MemorySegment databaseWrapper,
			MemorySegment readOptions,
			MemorySegment columnFamilyWrapper,
			MemorySegment key,
			long keyLength,
			MemorySegment scratch,
			MemorySegment valueLengthOut,
			MemorySegment foundOut,
			MemorySegment errorOut) {

		private static NativeCall create(Arena arena,
				RocksDB db,
				ColumnFamilyHandle columnFamily,
				ReadOptions readOptions,
				byte[] keyBytes) {
			MemorySegment databaseWrapper = arena.allocate(ValueLayout.ADDRESS);
			databaseWrapper.set(ValueLayout.ADDRESS, 0, MemorySegment.ofAddress(db.getNativeHandle()));
			// The real C wrapper also has an `immortal` byte after this pointer. The get functions only read `rep`.
			MemorySegment columnFamilyWrapper = arena.allocate(2 * ValueLayout.ADDRESS.byteSize(),
					ValueLayout.ADDRESS.byteAlignment());
			columnFamilyWrapper.set(ValueLayout.ADDRESS,
					0,
					MemorySegment.ofAddress(columnFamily.getNativeHandle()));
			MemorySegment key = arena.allocate(keyBytes.length, 1);
			MemorySegment.copy(MemorySegment.ofArray(keyBytes), 0, key, 0, keyBytes.length);
			return new NativeCall(databaseWrapper,
					// ABI experiment: ReadOptions is the first inline member of rocksdb_readoptions_t.
					MemorySegment.ofAddress(readOptions.getNativeHandle()),
					columnFamilyWrapper,
					key,
					keyBytes.length,
					arena.allocate(SCRATCH_BYTES, 8),
					arena.allocate(ValueLayout.JAVA_LONG),
					arena.allocate(ValueLayout.JAVA_BYTE),
					arena.allocate(ValueLayout.ADDRESS));
		}

		private void resetOutputs() {
			valueLengthOut.set(ValueLayout.JAVA_LONG, 0, 0L);
			foundOut.set(ValueLayout.JAVA_BYTE, 0, (byte) 0);
			errorOut.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
		}
	}
}
