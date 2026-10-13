package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.IOException;
import io.vertx.core.json.JsonObject;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;

import static java.lang.foreign.ValueLayout.*;

/** Versioned extensions of the already loaded RocksJava runtime. */
public final class NativeStartupBridge {
    private NativeStartupBridge() {}

    // RshcCacheConfigV1: fixed LP64 layout, including the three trailing pointers.
    static final MemoryLayout CACHE_LAYOUT = MemoryLayout.structLayout(
            JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT,
            JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG,
            JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG,
            JAVA_INT, JAVA_INT, JAVA_DOUBLE, ADDRESS, ADDRESS, ADDRESS);

    public record CacheConfig(int cacheType, int numShardBits, long capacity,
            long estimatedEntryCharge, long diskBudgetBytes, long snapshotBudgetBytes,
            long collectorEntries, long indexRamBytes, long maxBlockBytes,
            long minFreeDiskBytes, long lookupBufferBytes, long snapshotDeadlineMs,
            long periodMs, long readBytesPerSecond, long writeBytesPerSecond,
            int sampleEvery, int lookupConcurrency, double highPriorityRatio,
            Path directory, String cacheName) {
        public CacheConfig {
            Objects.requireNonNull(directory);
            Objects.requireNonNull(cacheName);
        }
    }

    public record BudgetConfig(String storageId, int maximum, int progressMs, int staleMs) {
        public BudgetConfig { Objects.requireNonNull(storageId); }
    }
    public record SnapshotResult(int status, String message) {}
    /** One native stats poll; cache hit counters describe secondary-cache replay. */
    public record CacheStats(long sampleCandidates, long sampled, long dropped,
            long snapshots, long snapshotBytes, long aborts, long errors, long rejected,
            long dataHits, long indexHits, long filterHits, long dictionaryHits,
            long replayBytes, long lookupReservedBytes, long oversizedDecoderRejections,
            long feedback) {}

    static CacheStats parseCacheStats(String json) {
        try {
            var values = new JsonObject(json);
            return new CacheStats(counter(values, "sample_candidates"), counter(values, "sampled"),
                    counter(values, "dropped"), counter(values, "snapshots"), counter(values, "snapshot_bytes"),
                    counter(values, "aborts"), counter(values, "errors"), counter(values, "rejected"),
                    counter(values, "data_hits"), counter(values, "index_hits"), counter(values, "filter_hits"),
                    counter(values, "dictionary_hits"), counter(values, "replay_bytes"),
                    counter(values, "lookup_reserved_bytes"), counter(values, "oversized_decoder_rejections"),
                    counter(values, "feedback"));
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Invalid native snapshot cache stats", failure);
        }
    }

    private static long counter(JsonObject values, String field) {
        Object value = values.getValue(field);
        if (!(value instanceof Long || value instanceof Integer) || ((Number) value).longValue() < 0) {
            throw new IllegalArgumentException("Invalid or missing native snapshot cache counter " + field);
        }
        return ((Number) value).longValue();
    }

    /** File opens count successful owned FS opens, including later reader failure, not device reads. */
    public record AdmissionStats(long active, long queued, long admitted, long cancelled,
            long waitMicros, long limit, long pressure, long released, long peakActive,
            long completedFindTable, long failed, long findTableMicros, long logicalBytes,
            long pressuredAdmissions, long pressureMask, long publishers, long healthyEpoch,
            long ownedSuccessfulFileOpens) {}

    private static MethodHandle bind(String name, MemoryLayout result, MemoryLayout... args) {
        var symbol = SymbolLookup.loaderLookup().find(name).orElseThrow(() ->
                new UnsatisfiedLinkError("Loaded RocksDB runtime lacks startup symbol " + name));
        return Linker.nativeLinker().downcallHandle(symbol, result == null
                ? FunctionDescriptor.ofVoid(args) : FunctionDescriptor.of(result, args));
    }

    private static Object call(MethodHandle function, Object... args) {
        try { return function.invokeWithArguments(args); }
        catch (RuntimeException | Error e) { throw e; }
        catch (Throwable e) { throw new IllegalStateException("Native startup bridge invocation failed", e); }
    }
    private static void check(int status, String operation) {
        if (status != 0) throw new IllegalStateException(operation + " failed (native status " + status + ")");
    }
    private static MemorySegment string(Arena arena, String value) {
        if (value.indexOf('\0') >= 0) throw new IllegalArgumentException("Native string contains NUL");
        return arena.allocateFrom(value);
    }

    private static final class CacheApi {
        static { RocksDBLoader.loadLibrary(); }
        static final MethodHandle SIZE = bind("rshc_cache_config_size_v1", JAVA_LONG);
        static final MethodHandle PATH = bind("rshc_cache_library_path_v1", JAVA_INT, ADDRESS, JAVA_LONG);
        static final MethodHandle CREATE = bind("rshc_cache_create_v1", JAVA_LONG, ADDRESS, ADDRESS, JAVA_LONG);
        static final MethodHandle STOP = bind("rshc_cache_stop_v1", null, JAVA_LONG);
        static final MethodHandle DESTROY = bind("rshc_cache_destroy_v1", null, JAVA_LONG);
        static final MethodHandle SNAPSHOT = bind("rshc_cache_snapshot_v1", JAVA_INT, JAVA_LONG, ADDRESS, JAVA_LONG);
        static final MethodHandle FEEDBACK = bind("rshc_cache_feedback_v1", null, JAVA_LONG, JAVA_INT);
        static final MethodHandle STATS = bind("rshc_cache_stats_v1", JAVA_INT, JAVA_LONG, ADDRESS, JAVA_LONG);
        static final String COMPATIBILITY = fingerprint();
        static {
            long size = (long) call(SIZE);
            if (ADDRESS.byteSize() != 8 || size != CACHE_LAYOUT.byteSize()) {
                throw new LinkageError("Unsupported native cache layout: " + size);
            }
        }
        private static String fingerprint() {
            try (var arena = Arena.ofConfined()) {
                var buffer = arena.allocate(65536);
                check((int) call(PATH, buffer, buffer.byteSize()), "Resolve loaded RocksDB library path");
                return artifactFingerprint(Path.of(buffer.getString(0)));
            } catch (IOException e) {
                throw new IllegalStateException("Cannot fingerprint loaded RocksDB library", e);
            }
        }
    }

    static String artifactFingerprint(Path path) throws IOException {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(path)) {
                byte[] bytes = new byte[65536];
                for (int n; (n = input.read(bytes)) >= 0;) digest.update(bytes, 0, n);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 is required by the Java platform", e);
        }
    }

    public static String cacheCompatibilityId() { return CacheApi.COMPATIBILITY; }

    static MemorySegment encodeCacheConfig(Arena arena, CacheConfig config, String compatibility) {
        var data = arena.allocate(CACHE_LAYOUT);
        data.set(JAVA_INT, 0, 160);
        data.set(JAVA_INT, 4, 1);
        data.set(JAVA_INT, 8, config.cacheType());
        data.set(JAVA_INT, 12, config.numShardBits());
        long[] values = {config.capacity(), config.estimatedEntryCharge(), config.diskBudgetBytes(),
                config.snapshotBudgetBytes(), config.collectorEntries(), config.indexRamBytes(),
                config.maxBlockBytes(), config.minFreeDiskBytes(), config.lookupBufferBytes(),
                config.snapshotDeadlineMs(), config.periodMs(), config.readBytesPerSecond(), config.writeBytesPerSecond()};
        for (int i = 0; i < values.length; i++) data.set(JAVA_LONG, 16L + i * 8L, values[i]);
        data.set(JAVA_INT, 120, config.sampleEvery());
        data.set(JAVA_INT, 124, config.lookupConcurrency());
        data.set(JAVA_DOUBLE, 128, config.highPriorityRatio());
        data.set(ADDRESS, 136, string(arena, config.directory().toAbsolutePath().normalize().toString()));
        data.set(ADDRESS, 144, string(arena, config.cacheName()));
        data.set(ADDRESS, 152, string(arena, compatibility));
        return data;
    }

    static long createCache(CacheConfig config) {
        String compatibility = cacheCompatibilityId();
        try { Files.createDirectories(config.directory()); }
        catch (IOException failure) {
            throw new IllegalStateException("Cannot create snapshot parent directory " + config.directory(), failure);
        }
        try (var arena = Arena.ofConfined()) {
            var error = arena.allocate(4096);
            long handle = (long) call(CacheApi.CREATE, encodeCacheConfig(arena, config, compatibility), error, error.byteSize());
            if (handle == 0) throw new IllegalStateException("Create snapshot cache failed: " + error.getString(0));
            return handle;
        }
    }
    static void stopCache(long handle) { call(CacheApi.STOP, handle); }
    static void destroyCache(long handle) { call(CacheApi.DESTROY, handle); }
    static void cacheFeedback(long handle, int mask) { call(CacheApi.FEEDBACK, handle, mask); }
    static SnapshotResult capture(long handle) {
        try (var arena = Arena.ofConfined()) {
            var error = arena.allocate(4096);
            int status = (int) call(CacheApi.SNAPSHOT, handle, error, error.byteSize());
            return new SnapshotResult(status, error.getString(0));
        }
    }
    static String cacheStats(long handle) {
        try (var arena = Arena.ofConfined()) {
            var buffer = arena.allocate(65536);
            int status = (int) call(CacheApi.STATS, handle, buffer, buffer.byteSize());
            if (status != 0) throw new IllegalStateException("Snapshot cache stats failed (native status "
                    + status + "): " + buffer.getString(0));
            return buffer.getString(0);
        }
    }

    private static final class AdmissionApi {
        static { RocksDBLoader.loadLibrary(); }
        static final MethodHandle VERSION = bind("rocksdb_async_open_admission_abi_version", JAVA_INT);
        static {
            int version = (int) call(VERSION);
            if (version != 3) throw new LinkageError("Unsupported admission ABI version " + version);
        }
        static final MethodHandle CREATE_BUDGET = bind("rocksdb_async_open_admission_budget_create", JAVA_INT,
                ADDRESS, JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS);
        static final MethodHandle DESTROY_BUDGET = bind("rocksdb_async_open_admission_budget_destroy", null, ADDRESS);
        static final MethodHandle FEEDBACK = bind("rocksdb_async_open_admission_budget_feedback", JAVA_INT, ADDRESS, JAVA_INT, JAVA_LONG);
        static final MethodHandle SNAPSHOT = bind("rocksdb_async_open_admission_budget_snapshot", JAVA_INT, ADDRESS, ADDRESS, JAVA_INT);
        static final MethodHandle CREATE_CONFIG = bind("rocksdb_async_open_admission_config_create", JAVA_INT, ADDRESS, ADDRESS);
        static final MethodHandle ROUTE = bind("rocksdb_async_open_admission_config_add_route", JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS);
        static final MethodHandle DESTROY_CONFIG = bind("rocksdb_async_open_admission_config_destroy", null, ADDRESS);
        static final MethodHandle ATTACH_DBOPTIONS = bind("rocksdb_async_open_admission_config_attach_jni_dboptions", JAVA_INT, ADDRESS, ADDRESS);
        static final MethodHandle ATTACH_OPTIONS = bind("rocksdb_async_open_admission_config_attach_jni_options", JAVA_INT, ADDRESS, ADDRESS);
    }

    public static Budget createBudget(BudgetConfig config) {
        try (var arena = Arena.ofConfined()) {
            var out = arena.allocate(ADDRESS);
            check((int) call(AdmissionApi.CREATE_BUDGET, string(arena, config.storageId()),
                    config.maximum(), config.progressMs(), config.staleMs(), out), "Create admission budget");
            return new Budget(out.get(ADDRESS, 0));
        }
    }
    public static AdmissionConfig createAdmissionConfig(Budget defaultBudget) {
        synchronized (defaultBudget) {
            try (var arena = Arena.ofConfined()) {
                var out = arena.allocate(ADDRESS);
                check((int) call(AdmissionApi.CREATE_CONFIG, defaultBudget.requireOpen(), out), "Create admission config");
                return new AdmissionConfig(out.get(ADDRESS, 0));
            }
        }
    }
    public static final class Budget implements AutoCloseable {
        private MemorySegment handle;
        private Budget(MemorySegment handle) {
            if (handle.address() == 0) throw new IllegalStateException("Native admission budget returned null");
            this.handle = handle;
        }
        private MemorySegment requireOpen() {
            if (handle.address() == 0) throw new IllegalStateException("Admission budget is closed");
            return handle;
        }
        public synchronized void feedback(int mask, long epoch) {
            check((int) call(AdmissionApi.FEEDBACK, requireOpen(), mask, epoch), "Admission feedback");
        }
        public synchronized AdmissionStats snapshot() {
            try (var arena = Arena.ofConfined()) {
                var values = arena.allocate(JAVA_LONG, 18);
                check((int) call(AdmissionApi.SNAPSHOT, requireOpen(), values, 18), "Admission snapshot");
                long[] v = values.toArray(JAVA_LONG);
                return new AdmissionStats(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8],
                        v[9], v[10], v[11], v[12], v[13], v[14], v[15], v[16], v[17]);
            }
        }
        @Override public synchronized void close() {
            if (handle.address() != 0) { call(AdmissionApi.DESTROY_BUDGET, handle); handle = MemorySegment.NULL; }
        }
    }
    public static final class AdmissionConfig implements AutoCloseable {
        private MemorySegment handle;
        private AdmissionConfig(MemorySegment handle) {
            if (handle.address() == 0) throw new IllegalStateException("Native admission config returned null");
            this.handle = handle;
        }
        private MemorySegment requireOpen() {
            if (handle.address() == 0) throw new IllegalStateException("Admission config is closed");
            return handle;
        }
        public synchronized void addRoute(String columnFamily, Path directory, Budget budget) {
            synchronized (budget) {
                try (var arena = Arena.ofConfined()) {
                    check((int) call(AdmissionApi.ROUTE, requireOpen(), string(arena, columnFamily),
                            string(arena, directory.toAbsolutePath().normalize().toString()), budget.requireOpen()), "Add admission route");
                }
            }
        }
        /** Caller must attach before open and keep the options alive for this call. */
        public synchronized void attach(DBOptions options) {
            synchronized (options) {
                check((int) call(AdmissionApi.ATTACH_DBOPTIONS, requireOpen(), optionsHandle(options.isOwningHandle(), options.getNativeHandle())), "Attach admission DBOptions");
            }
        }
        public synchronized void attach(Options options) {
            synchronized (options) {
                check((int) call(AdmissionApi.ATTACH_OPTIONS, requireOpen(), optionsHandle(options.isOwningHandle(), options.getNativeHandle())), "Attach admission Options");
            }
        }
        private static MemorySegment optionsHandle(boolean owning, long handle) {
            if (!owning || handle == 0) throw new IllegalStateException("Options are closed");
            return MemorySegment.ofAddress(handle);
        }
        @Override public synchronized void close() {
            if (handle.address() != 0) { call(AdmissionApi.DESTROY_CONFIG, handle); handle = MemorySegment.NULL; }
        }
    }
}
