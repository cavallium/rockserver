import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Isolated C API DB fixture and measured-latency feedback, using one native runtime. */
public final class OpeningAdmissionExperiment {
    private static final int FILES = 12, KEYS_PER_FILE = 256, VALUE_BYTES = 1024;
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final AddressLayout PTR = ValueLayout.ADDRESS;

    public static void main(String[] args) throws Throwable {
        if (args.length != 4) throw new IllegalArgumentException(
                "Usage: OpeningAdmissionExperiment <library.so> <db> <seed|baseline|adaptive> <baseline-p95-nanos>");
        Path library = Path.of(args[0]).toAbsolutePath();
        Path path = Path.of(args[1]).toAbsolutePath();
        String mode = args[2];
        if (!mode.equals("seed") && !mode.equals("baseline") && !mode.equals("adaptive")) {
            throw new IllegalArgumentException("Unknown mode: " + mode);
        }
        boolean seed = mode.equals("seed"), adaptive = mode.equals("adaptive");
        long baseline = Long.parseLong(args[3]);
        if (adaptive && (baseline <= 0 || baseline > Long.MAX_VALUE / 2)) {
            throw new IllegalArgumentException("Adaptive mode requires a positive bounded baseline p95");
        }
        if (seed && Files.exists(path)) throw new IllegalArgumentException("Seed DB must be a new path");
        try (Arena arena = Arena.ofConfined()) {
            Api api = new Api(library, arena);
            MemorySegment options = (MemorySegment) api.call("options_create");
            MemorySegment readOptions = (MemorySegment) api.call("readoptions_create");
            MemorySegment writeOptions = (MemorySegment) api.call("writeoptions_create");
            MemorySegment flushOptions = (MemorySegment) api.call("flushoptions_create");
            MemorySegment db = MemorySegment.NULL;
            MemorySegment error = arena.allocate(PTR), length = arena.allocate(LONG);
            MemorySegment value = arena.allocate(VALUE_BYTES).fill((byte) 7);
            MemorySegment[] keys = new MemorySegment[FILES * KEYS_PER_FILE];
            for (int i = 0; i < keys.length; i++) keys[i] = arena.allocateFrom("key" + i);
            long reads = 0, totalNanos = 0, p95 = 0;
            long[] samples = new long[256];
            int sampleCount = 0, pressureSteps = 0, healthySteps = 0;
            try {
                api.call("options_set_create_if_missing", options, (byte) (seed ? 1 : 0));
                api.call("options_set_open_files_async", options, (byte) (seed ? 0 : 1));
                api.call("options_set_skip_stats_update_on_db_open", options, (byte) (seed ? 0 : 1));
                api.call("options_set_disable_auto_compactions", options, 1);
                api.call("options_set_max_open_files", options, -1);
                api.call("options_set_max_file_opening_threads", options, 4);
                api.call("options_set_compression", options, 0);
                api.call("flushoptions_set_wait", flushOptions, (byte) 1);
                if (adaptive) {
                    int status = (int) api.call("async_open_admission_enable", 4, 50);
                    if (status != 0) throw new IllegalStateException("Admission enable failed: " + status);
                    api.call("async_open_admission_feedback", 1);
                }
                db = (MemorySegment) api.call("open", options, arena.allocateFrom(path.toString()), error);
                api.checkError(error);
                if (db.address() == 0) throw new IllegalStateException("Native DB open returned null");
                if (seed) {
                    for (int file = 0; file < FILES; file++) {
                        for (int offset = 0; offset < KEYS_PER_FILE; offset++) {
                            MemorySegment key = keys[file * KEYS_PER_FILE + offset];
                            api.call("put", db, writeOptions, key, key.byteSize() - 1, value, (long) VALUE_BYTES, error);
                            api.checkError(error);
                        }
                        api.call("flush", db, flushOptions, error);
                        api.checkError(error);
                    }
                } else {
                    long end = System.nanoTime() + 2_000_000_000L;
                    long nextFeedback = System.nanoTime() + 50_000_000L;
                    while (System.nanoTime() < end) {
                        // Spread demand over every startup SST without random allocator state.
                        MemorySegment key = keys[(int) ((reads * 997) % keys.length)];
                        long start = System.nanoTime();
                        MemorySegment found = (MemorySegment) api.call("get", db, readOptions, key,
                                key.byteSize() - 1, length, error);
                        long nanos = System.nanoTime() - start;
                        api.checkError(error);
                        if (found.address() == 0) throw new IllegalStateException("Fixture key missing");
                        try {
                            if (length.get(LONG, 0) != VALUE_BYTES || found.reinterpret(VALUE_BYTES).mismatch(value) != -1) {
                                throw new IllegalStateException("Wrong fixture value");
                            }
                        } finally { api.call("free", found); }
                        samples[sampleCount++ % samples.length] = nanos;
                        totalNanos += nanos;
                        reads++;
                        if (System.nanoTime() >= nextFeedback) {
                            p95 = percentile(samples, Math.min(sampleCount, samples.length));
                            if (adaptive) {
                                boolean pressure = p95 > baseline * 2;
                                api.call("async_open_admission_feedback", pressure ? 1 : 0);
                                if (pressure) pressureSteps++; else healthySteps++;
                            }
                            nextFeedback = System.nanoTime() + 50_000_000L;
                        }
                    }
                    p95 = percentile(samples, Math.min(sampleCount, samples.length));
                }
            } finally {
                // Close joins native background work before borrowed memory or the library closes.
                if (db.address() != 0) api.call("close", db);
                api.call("flushoptions_destroy", flushOptions);
                api.call("writeoptions_destroy", writeOptions);
                api.call("readoptions_destroy", readOptions);
                api.call("options_destroy", options);
            }
            long active = api.counter(0), waiters = api.counter(1);
            if (active != 0 || waiters != 0) throw new IllegalStateException("Admission work survived DB close");
            System.out.println("{\"mode\":\"" + mode + "\",\"reads\":" + reads
                    + ",\"mean_nanos\":" + (reads == 0 ? 0 : totalNanos / reads)
                    + ",\"last_window_p95_nanos\":" + p95 + ",\"pressure_steps\":" + pressureSteps
                    + ",\"healthy_steps\":" + healthySteps + ",\"admitted\":" + api.counter(2)
                    + ",\"completed\":" + api.counter(7) + ",\"cancelled\":" + api.counter(3)
                    + ",\"peak_active\":" + api.counter(8) + ",\"final_limit\":" + api.counter(5)
                    + ",\"wait_micros\":" + api.counter(4) + ",\"active_after_close\":" + active
                    + ",\"waiters_after_close\":" + waiters + "}");
        }
    }

    private static long percentile(long[] samples, int size) {
        if (size == 0) return 0;
        long[] copy = Arrays.copyOf(samples, size);
        Arrays.sort(copy);
        return copy[(size - 1) * 95 / 100];
    }

    private static final class Api {
        private final Map<String, MethodHandle> handles = new HashMap<>();
        private final Linker linker = Linker.nativeLinker();
        private final SymbolLookup symbols;
        Api(Path library, Arena arena) {
            if (linker.canonicalLayouts().get("size_t").byteSize() != 8) {
                throw new UnsupportedOperationException("Fixture requires 64-bit size_t");
            }
            symbols = SymbolLookup.libraryLookup(library, arena);
            for (String type : new String[]{"options", "readoptions", "writeoptions", "flushoptions"}) {
                bind(type + "_create", PTR);
                bind(type + "_destroy", null, PTR);
            }
            bind("options_set_create_if_missing", null, PTR, BYTE);
            bind("options_set_open_files_async", null, PTR, BYTE);
            bind("options_set_skip_stats_update_on_db_open", null, PTR, BYTE);
            bind("flushoptions_set_wait", null, PTR, BYTE);
            for (String option : new String[]{"disable_auto_compactions", "max_open_files", "max_file_opening_threads", "compression"}) {
                bind("options_set_" + option, null, PTR, INT);
            }
            bind("open", PTR, PTR, PTR, PTR);
            bind("close", null, PTR);
            bind("put", null, PTR, PTR, PTR, LONG, PTR, LONG, PTR);
            bind("flush", null, PTR, PTR, PTR);
            bind("get", PTR, PTR, PTR, PTR, LONG, PTR, PTR);
            bind("free", null, PTR);
            bind("async_open_admission_enable", INT, INT, INT);
            bind("async_open_admission_feedback", null, INT);
            bind("async_open_admission_counter", LONG, INT);
        }
        private void bind(String name, MemoryLayout result, MemoryLayout... arguments) {
            MemorySegment symbol = symbols.find("rocksdb_" + name).orElseThrow(
                    () -> new UnsatisfiedLinkError("Missing rocksdb_" + name));
            handles.put(name, linker.downcallHandle(symbol, result == null
                    ? FunctionDescriptor.ofVoid(arguments) : FunctionDescriptor.of(result, arguments)));
        }
        Object call(String name, Object... arguments) throws Throwable {
            return handles.get(name).invokeWithArguments(arguments);
        }
        long counter(int selector) throws Throwable {
            return (long) call("async_open_admission_counter", selector);
        }
        void checkError(MemorySegment slot) throws Throwable {
            MemorySegment error = slot.get(PTR, 0);
            if (error.address() == 0) return;
            try { throw new IllegalStateException(error.reinterpret(64 * 1024).getString(0)); }
            finally { call("free", error); slot.set(PTR, 0, MemorySegment.NULL); }
        }
    }
}
