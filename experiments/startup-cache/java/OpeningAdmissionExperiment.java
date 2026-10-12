import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Standalone, bounded C API fixture. All DB calls use one native runtime. */
public final class OpeningAdmissionExperiment {
    private static final int FILES = 12, KEYS_PER_FILE = 256, VALUE_BYTES = 1024;
    private static final long RUN_NANOS = 2_000_000_000L, WINDOW_NANOS = 50_000_000L;
    private static final String FIXTURE_MARKER = ".rockserver-opening-fixture";
    private static final String FIXTURE_MAGIC = "opening-admission-v2\n";
    private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG;
    private static final ValueLayout.OfInt INT = ValueLayout.JAVA_INT;
    private static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
    private static final AddressLayout PTR = ValueLayout.ADDRESS;

    public static void main(String[] args) throws Throwable {
        if (args.length == 1 && args[0].equals("--self-test")) {
            testHistogram();
            return;
        }
        if (args.length != 4) throw new IllegalArgumentException(
                "Usage: OpeningAdmissionExperiment <library.so> <db> <seed|baseline|adaptive> <reference-p95-upper-nanos>");
        Path library = Path.of(args[0]).toRealPath();
        Path path = Path.of(args[1]).toAbsolutePath().normalize();
        String mode = args[2];
        if (!mode.equals("seed") && !mode.equals("baseline") && !mode.equals("adaptive")) {
            throw new IllegalArgumentException("Unknown mode: " + mode);
        }
        boolean seed = mode.equals("seed"), adaptive = mode.equals("adaptive");
        long reference = Long.parseLong(args[3]);
        if (adaptive && (reference <= 0 || reference > Long.MAX_VALUE / 2)) {
            throw new IllegalArgumentException("Adaptive mode needs a positive bounded reference p95");
        }
        if (seed && Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Seed DB must be a new path");
        }
        Path marker = path.resolve(FIXTURE_MARKER);
        if (!seed && (!Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS)
                || Files.size(marker) != FIXTURE_MAGIC.length()
                || !Files.readString(marker).equals(FIXTURE_MAGIC))) {
            throw new IllegalArgumentException("Missing or incompatible opening fixture marker");
        }
        try (Arena arena = Arena.ofConfined()) {
            Api api = new Api(library, arena);
            Metrics metrics = new Metrics();
            try (Resource options = new Resource(api, "options");
                 Resource readOptions = new Resource(api, "readoptions");
                 Resource writeOptions = new Resource(api, "writeoptions");
                 Resource flushOptions = new Resource(api, "flushoptions")) {
                api.call("options_set_create_if_missing", options.pointer, (byte) (seed ? 1 : 0));
                api.call("options_set_error_if_exists", options.pointer, (byte) (seed ? 1 : 0));
                api.call("options_set_open_files_async", options.pointer, (byte) (seed ? 0 : 1));
                api.call("options_set_skip_stats_update_on_db_open", options.pointer, (byte) (seed ? 0 : 1));
                api.call("options_set_disable_auto_compactions", options.pointer, 1);
                api.call("options_set_max_open_files", options.pointer, -1);
                api.call("options_set_max_file_opening_threads", options.pointer, 4);
                api.call("options_set_compression", options.pointer, 0);
                api.call("flushoptions_set_wait", flushOptions.pointer, (byte) 1);
                if (adaptive) {
                    int status = (int) api.call("async_open_admission_enable", 4, 25);
                    if (status != 0) throw new IllegalStateException("Admission enable failed: " + status);
                    api.feedback(true); // Labelled, commanded startup-pressure probe.
                }
                MemorySegment error = arena.allocate(PTR), length = arena.allocate(LONG);
                MemorySegment[] keys = new MemorySegment[FILES * KEYS_PER_FILE];
                MemorySegment[] values = new MemorySegment[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    // Fixed-width ranges keep foreground demand out of six L0 SSTs.
                    keys[i] = arena.allocateFrom(String.format(Locale.ROOT, "key%08d", i));
                    values[i] = arena.allocate(VALUE_BYTES).fill((byte) (i * 31));
                    values[i].set(INT, 0, i);
                }
                try (OpenedDb db = new OpenedDb(api, options.pointer, arena.allocateFrom(path.toString()), error)) {
                    if (seed) {
                        for (int file = 0; file < FILES; file++) {
                            for (int offset = 0; offset < KEYS_PER_FILE; offset++) {
                                int i = file * KEYS_PER_FILE + offset;
                                api.call("put", db.pointer, writeOptions.pointer, keys[i], keys[i].byteSize() - 1,
                                        values[i], (long) VALUE_BYTES, error);
                                api.checkError(error);
                            }
                            api.call("flush", db.pointer, flushOptions.pointer, error);
                            api.checkError(error);
                        }
                    } else {
                        measure(api, db.pointer, readOptions.pointer, keys, values, error, length,
                                adaptive, reference, metrics);
                    }
                } // DB close joins background work before options, Arena or library close.
                if (seed) Files.writeString(marker, FIXTURE_MAGIC, StandardOpenOption.CREATE_NEW);
            }
            if (api.counter(0) != 0 || api.counter(1) != 0) {
                throw new IllegalStateException("Admission work survived DB close");
            }
            System.out.println(metrics.report(mode, api));
        }
    }

    private static void measure(Api api, MemorySegment db, MemorySegment readOptions,
                                MemorySegment[] keys, MemorySegment[] values,
                                MemorySegment error, MemorySegment length,
                                boolean adaptive, long reference, Metrics metrics) throws Throwable {
        Histogram window = new Histogram();
        long started = System.nanoTime(), windowStarted = started;
        int foregroundKeys = keys.length / 2;
        while (System.nanoTime() - started < RUN_NANOS) {
            int i = (int) ((metrics.reads * 997) % foregroundKeys);
            long before = System.nanoTime();
            MemorySegment found = (MemorySegment) api.get.invokeExact(db, readOptions, keys[i],
                    keys[i].byteSize() - 1, length, error);
            long nanos = System.nanoTime() - before;
            try {
                api.checkError(error);
                if (found.address() == 0) throw new IllegalStateException("Fixture key missing");
                if (length.get(LONG, 0) != VALUE_BYTES || found.reinterpret(VALUE_BYTES).mismatch(values[i]) != -1) {
                    throw new IllegalStateException("Wrong value for fixture key " + i);
                }
            } finally {
                if (found.address() != 0) api.free.invokeExact(found);
            }
            window.record(nanos);
            metrics.all.record(nanos);
            metrics.reads++;
            long now = System.nanoTime();
            if (now - windowStarted >= WINDOW_NANOS) {
                metrics.window(window, now - windowStarted);
                if (adaptive) {
                    if (api.counter(0) > 0 || api.counter(1) > 0) metrics.overlapFeedbackSteps++;
                    boolean pressure = window.p95Upper() > reference * 2;
                    api.feedback(pressure);
                    if (pressure) metrics.pressureSteps++; else metrics.healthySteps++;
                }
                window.clear();
                windowStarted = now;
            }
        }
        if (window.count > 0) metrics.window(window, System.nanoTime() - windowStarted);
    }

    /** Full-window histogram: eight buckets per power of two, no dropped reads. */
    private static final class Histogram {
        final long[] buckets = new long[512];
        long count, sum;
        void record(long nanos) {
            if (nanos < 0) throw new IllegalArgumentException("Negative native-call duration");
            int bucket;
            if (nanos < 8) bucket = (int) nanos;
            else {
                int shift = 60 - Long.numberOfLeadingZeros(nanos);
                bucket = shift * 8 + (int) (nanos >>> shift);
            }
            buckets[bucket]++;
            count++;
            sum = Math.addExact(sum, nanos);
        }
        long p95Upper() {
            long rank = count - count / 20, seen = 0;
            if (count == 0) return 0;
            for (int i = 0; i < buckets.length; i++) {
                seen += buckets[i];
                if (seen >= rank) {
                    if (i < 8) return i;
                    int shift = i / 8 - 1;
                    return ((9L + i % 8) << shift) - 1;
                }
            }
            throw new IllegalStateException("Histogram count mismatch");
        }
        void clear() { Arrays.fill(buckets, 0); count = 0; sum = 0; }
    }

    private static final class Metrics {
        final Histogram all = new Histogram();
        long reads, windows, lastWindowP95, lastWindowSamples, lastWindowNanos, maximumWindowNanos;
        int pressureSteps, healthySteps, overlapFeedbackSteps;
        void window(Histogram histogram, long elapsed) {
            windows++;
            lastWindowP95 = histogram.p95Upper();
            lastWindowSamples = histogram.count;
            lastWindowNanos = elapsed;
            maximumWindowNanos = Math.max(maximumWindowNanos, elapsed);
        }
        String report(String mode, Api api) throws Throwable {
            return "{\"mode\":\"" + mode + "\",\"reads\":" + reads
                    + ",\"mean_native_call_nanos\":" + (reads == 0 ? 0 : all.sum / reads)
                    + ",\"p95_upper_nanos\":" + all.p95Upper()
                    + ",\"latency_samples\":" + all.count + ",\"windows\":" + windows
                    + ",\"last_window_p95_upper_nanos\":" + lastWindowP95
                    + ",\"last_window_samples\":" + lastWindowSamples
                    + ",\"last_window_nanos\":" + lastWindowNanos
                    + ",\"maximum_window_nanos\":" + maximumWindowNanos
                    + ",\"commanded_pressure_steps\":" + (mode.equals("adaptive") ? 1 : 0)
                    + ",\"pressure_steps\":" + pressureSteps + ",\"healthy_steps\":" + healthySteps
                    + ",\"overlap_feedback_steps\":" + overlapFeedbackSteps
                    + ",\"admitted\":" + api.counter(2) + ",\"released\":" + api.counter(7)
                    + ",\"cancelled\":" + api.counter(3) + ",\"peak_active\":" + api.counter(8)
                    + ",\"final_limit\":" + api.counter(5) + ",\"wait_micros\":" + api.counter(4)
                    + ",\"table_load_calls\":" + api.counter(9) + ",\"failed_table_load_calls\":" + api.counter(10)
                    + ",\"table_load_micros\":" + api.counter(11) + ",\"logical_open_read_bytes\":" + api.counter(12)
                    + ",\"pressured_admissions\":" + api.counter(13)
                    + ",\"active_after_close\":" + api.counter(0) + ",\"waiters_after_close\":" + api.counter(1) + "}";
        }
    }

    private static final class Resource implements AutoCloseable {
        final Api api;
        final String kind;
        final MethodHandle destroy;
        MemorySegment pointer;
        Resource(Api api, String kind) throws Throwable {
            this.api = api;
            this.kind = kind;
            destroy = api.handles.get(kind + "_destroy");
            pointer = (MemorySegment) api.call(kind + "_create");
            if (pointer.address() == 0) throw new IllegalStateException("Native " + kind + " allocation failed");
        }
        public void close() {
            MemorySegment owned = pointer;
            pointer = MemorySegment.NULL;
            if (owned.address() == 0) return;
            try { destroy.invokeExact(owned); }
            catch (Error error) { throw error; }
            catch (Throwable failure) { throw new IllegalStateException("Destroying native " + kind + " failed", failure); }
        }
    }

    private static final class OpenedDb implements AutoCloseable {
        final Api api;
        MemorySegment pointer;
        OpenedDb(Api api, MemorySegment options, MemorySegment path, MemorySegment error) throws Throwable {
            this.api = api;
            pointer = (MemorySegment) api.open.invokeExact(options, path, error);
            try {
                api.checkError(error);
                if (pointer.address() == 0) throw new IllegalStateException("Native DB open returned null");
            } catch (Throwable failure) {
                try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        public void close() {
            MemorySegment owned = pointer;
            pointer = MemorySegment.NULL;
            if (owned.address() == 0) return;
            try { api.close.invokeExact(owned); }
            catch (Error error) { throw error; }
            catch (Throwable failure) { throw new IllegalStateException("Closing native DB failed", failure); }
        }
    }

    private static final class Api {
        final Map<String, MethodHandle> handles = new HashMap<>();
        final Linker linker = Linker.nativeLinker();
        final SymbolLookup symbols;
        final MethodHandle get, open, close, free, counter, feedback;
        Api(Path library, Arena arena) throws Throwable {
            MemoryLayout sizeT = linker.canonicalLayouts().get("size_t");
            if (sizeT == null || sizeT.byteSize() != 8 || PTR.byteSize() != 8) {
                throw new UnsupportedOperationException("Fixture requires a 64-bit C ABI");
            }
            symbols = SymbolLookup.libraryLookup(library, arena);
            MethodHandle version = bind("async_open_admission_abi_version", INT);
            if ((int) version.invokeExact() != 1) throw new IllegalStateException("Unsupported admission ABI");
            for (String type : new String[]{"options", "readoptions", "writeoptions", "flushoptions"}) {
                bind(type + "_create", PTR);
                bind(type + "_destroy", null, PTR);
            }
            for (String option : new String[]{"create_if_missing", "error_if_exists", "open_files_async", "skip_stats_update_on_db_open"}) {
                bind("options_set_" + option, null, PTR, BYTE);
            }
            bind("flushoptions_set_wait", null, PTR, BYTE);
            for (String option : new String[]{"disable_auto_compactions", "max_open_files", "max_file_opening_threads", "compression"}) {
                bind("options_set_" + option, null, PTR, INT);
            }
            open = bind("open", PTR, PTR, PTR, PTR);
            close = bind("close", null, PTR);
            bind("put", null, PTR, PTR, PTR, LONG, PTR, LONG, PTR);
            bind("flush", null, PTR, PTR, PTR);
            get = bind("get", PTR, PTR, PTR, PTR, LONG, PTR, PTR);
            free = bind("free", null, PTR);
            bind("async_open_admission_enable", INT, INT, INT);
            feedback = bind("async_open_admission_feedback", INT, INT);
            counter = bind("async_open_admission_counter", LONG, INT);
        }
        private MethodHandle bind(String name, MemoryLayout result, MemoryLayout... arguments) {
            MemorySegment symbol = symbols.find("rocksdb_" + name).orElseThrow(
                    () -> new UnsatisfiedLinkError("Missing rocksdb_" + name));
            MethodHandle handle = linker.downcallHandle(symbol, result == null
                    ? FunctionDescriptor.ofVoid(arguments) : FunctionDescriptor.of(result, arguments));
            handles.put(name, handle);
            return handle;
        }
        Object call(String name, Object... arguments) throws Throwable {
            return handles.get(name).invokeWithArguments(arguments); // Cold configuration/seed calls only.
        }
        long counter(int selector) throws Throwable {
            long value = (long) counter.invokeExact(selector);
            if (value < 0) throw new IllegalStateException("Native counter failed: " + selector);
            return value;
        }
        void feedback(boolean pressure) throws Throwable {
            int result = (int) feedback.invokeExact(pressure ? 1 : 0);
            if (result != 0) throw new IllegalStateException("Native feedback failed: " + result);
        }
        void checkError(MemorySegment slot) throws Throwable {
            MemorySegment error = slot.get(PTR, 0);
            if (error.address() == 0) return;
            slot.set(PTR, 0, MemorySegment.NULL);
            try { throw new IllegalStateException(error.reinterpret(64 * 1024).getString(0)); }
            finally { free.invokeExact(error); }
        }
    }

    private static void testHistogram() {
        Histogram histogram = new Histogram();
        for (int i = 0; i < 64; i++) histogram.record(1_000_000);
        for (int i = 0; i < 960; i++) histogram.record(100);
        if (histogram.count != 1024 || histogram.p95Upper() < 1_000_000
                || histogram.p95Upper() > 1_125_000) throw new AssertionError("Early slow reads were dropped");
        for (long value : new long[]{0, 1, 7, 8, 15, 16, 1024, Long.MAX_VALUE}) {
            histogram.clear();
            histogram.record(value);
            if (histogram.p95Upper() < value) throw new AssertionError("Percentile is not an upper bound");
        }
        histogram.clear();
        if (histogram.count != 0 || histogram.p95Upper() != 0) throw new AssertionError("Window did not reset");
        System.out.println("histogram checks passed");
    }
}
