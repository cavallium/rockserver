package it.cavallium.rockserver.core.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.github.gestalt.config.exceptions.GestaltException;

/** Immutable startup configuration, resolved before native resources are allocated. */
public record StartupSettings(StorageAdmission admission, Map<String, CacheSnapshot> caches) {
    private static final long MIB = 1L << 20, GIB = 1L << 30;
    private static final SnapshotPolicy DEFAULT_DATA = new SnapshotPolicy(4 * GIB, GIB, 65536,
            16 * MIB, 16 * MIB, 65 * MIB, 8 * GIB, 30000, 300000, 64, 4, 0, 64 * MIB);
    private static final SnapshotPolicy DEFAULT_METADATA = new SnapshotPolicy(GIB, 256 * MIB, 16384,
            4 * MIB, 16 * MIB, 65 * MIB, 8 * GIB, 30000, 300000, 16, 4, 0, 64 * MIB);

    public StartupSettings { Objects.requireNonNull(admission); caches = Map.copyOf(caches); }

    /** Known collector, index/working-vector and payload quotas; excludes allocator/thread overhead. */
    public long snapshotAuxiliaryBytes() {
        long total = 0;
        for (var cache : caches.values()) if (cache.enabled()) {
            total = Math.addExact(total, Math.addExact(cache.data().auxiliaryBytes(), cache.metadata().auxiliaryBytes()));
        }
        return total;
    }

    public record StorageAdmission(boolean enabled, String defaultStorageId, Map<String, StorageGroup> storageGroups) {
        public StorageAdmission { storageGroups = Map.copyOf(storageGroups); }

        /** Routes normalized absolute paths by their longest prefix, then by the explicit default. */
        public StorageGroup storageFor(Path path) {
            if (!enabled) return null;
            Path absolute = path.toAbsolutePath().normalize();
            StorageGroup selected = storageGroups.get(defaultStorageId);
            int depth = -1;
            for (var group : storageGroups.values()) for (var prefix : group.paths()) {
                if (absolute.startsWith(prefix) && prefix.getNameCount() > depth) {
                    selected = group; depth = prefix.getNameCount();
                }
            }
            return selected;
        }

        public StorageGroup storageForWorkload(String workloadGroup) {
            if (!enabled) return null;
            String name = workloadGroup == null ? "default" : workloadGroup;
            for (var group : storageGroups.values()) if (group.workloadGroups().contains(name)) return group;
            return storageGroups.get(defaultStorageId);
        }
    }
    public record StorageGroup(String id, int maximumOpens, long progressIntervalMillis, long staleAfterMillis,
                               List<Path> paths, List<String> workloadGroups) {
        public StorageGroup { paths = List.copyOf(paths); workloadGroups = List.copyOf(workloadGroups); }
    }
    public record CacheSnapshot(boolean enabled, Path directory, SnapshotPolicy data, SnapshotPolicy metadata) {}

    /** Byte quotas and millisecond durations match hot_cache_snapshot.h version 1. Zero rate means unlimited. */
    public record SnapshotPolicy(long diskBudgetBytes, long snapshotBudgetBytes, long collectorEntries,
                                 long indexRamBytes, long maximumBlockBytes, long lookupBufferBytes,
                                 long minimumFreeDiskBytes, long snapshotDeadlineMillis, long periodMillis,
                                 int sampleEvery, int lookupConcurrency,
                                 long readCeilingBytesPerSecond, long writeCeilingBytesPerSecond) {
        public SnapshotPolicy {
            positive("disk-budget", diskBudgetBytes); positive("snapshot-budget", snapshotBudgetBytes);
            positive("index-ram", indexRamBytes); positive("maximum-block", maximumBlockBytes);
            positive("lookup-buffer", lookupBufferBytes);
            nonnegative("minimum-free-disk", minimumFreeDiskBytes);
            nonnegative("read-ceiling", readCeilingBytesPerSecond);
            nonnegative("write-ceiling", writeCeilingBytesPerSecond);
            if (snapshotBudgetBytes < 128 || snapshotBudgetBytes > diskBudgetBytes / 3)
                throw invalid("snapshot-budget must hold the header and not exceed disk-budget / 3 (live, retired and temporary generations)");
            if (!powerOfTwo(collectorEntries) || collectorEntries < 64 || collectorEntries > 1048576)
                throw invalid("collector-entries must be a power of two in [64, 1048576]");
            if (!powerOfTwo(sampleEvery) || sampleEvery > 65536)
                throw invalid("sample-every must be a power of two in [1, 65536]");
            if (lookupConcurrency < 1 || lookupConcurrency > 64)
                throw invalid("lookup-concurrency must be in [1, 64]");
            if (maximumBlockBytes > 16 * MIB) throw invalid("maximum-block must not exceed 16MiB");
            if (snapshotBudgetBytes < 128 + collectorEntries * 40 + Math.min(maximumBlockBytes, 4096))
                throw invalid("snapshot-budget must cover the full candidate index and one small eligible block");
            if (indexRamBytes < collectorEntries * 240)
                throw invalid("index-ram must cover collector-entries * 240 bytes of native index accounting");
            if (lookupBufferBytes < maximumBlockBytes * 4 + 65536)
                throw invalid("lookup-buffer must cover four maximum-block buffers plus 64KiB");
            if (snapshotDeadlineMillis < 1 || snapshotDeadlineMillis > 3600000)
                throw invalid("snapshot-deadline must be in [1ms, 1h]");
            if (periodMillis < 0 || periodMillis > 86400000)
                throw invalid("period must be zero (explicit captures) or at most 24h");
            if (writeCeilingBytesPerSecond != 0
                    && writeCeilingBytesPerSecond < maximumBlockBytes + 128 + collectorEntries * 40)
                throw invalid("write-ceiling must cover one maximum block plus the snapshot header and index");
        }

        public long auxiliaryBytes() {
            return Math.addExact(collectorEntries * 32,
                    Math.addExact(indexRamBytes, Math.addExact(maximumBlockBytes, lookupBufferBytes)));
        }
    }

    public static StartupSettings resolve(DatabaseConfig config, Path databaseDirectory, int defaultMaximumOpens)
            throws GestaltException {
        Objects.requireNonNull(config); Objects.requireNonNull(databaseDirectory);
        Path base = databaseDirectory.toAbsolutePath().normalize();
        var global = config.global();
        var admission = admission(config, global, base, defaultMaximumOpens);
        var parent = snapshot(global.hotCacheSnapshot(), null, base);
        var caches = new LinkedHashMap<String, CacheSnapshot>();
        validateCapacity("default", global.blockCache(), global.blockCacheMetadataSize(),
                global.writeBufferManager(), parent.enabled(), true);
        caches.put("default", parent);
        for (var cache : Objects.requireNonNullElse(global.blockCaches(), new BlockCacheConfig[0])) {
            String name = cache.name();
            if (name == null || name.isBlank() || name.equals("default") || caches.containsKey(name))
                throw invalid("named block cache must have a unique nonblank name other than default: " + name);
            var resolved = snapshot(cache.snapshot(), parent, base);
            validateCapacity(name, cache.size(), cache.metadataSize(), null, resolved.enabled(), false);
            caches.put(name, resolved);
        }
        var result = new StartupSettings(admission, caches);
        try { result.snapshotAuxiliaryBytes(); }
        catch (ArithmeticException overflow) { throw invalid("combined snapshot auxiliary quotas overflow"); }
        return result;
    }

    private static StorageAdmission admission(DatabaseConfig config, GlobalDatabaseConfig global,
                                               Path base, int defaultMaximumOpens) throws GestaltException {
        var io = global.startupIo();
        if (io == null || !io.enabled()) return new StorageAdmission(false, null, Map.of());
        if (!global.openFilesAsync() || global.maximumOpenFiles() != null && global.maximumOpenFiles() != -1)
            throw invalid("startup-io requires open-files-async=true and maximum-open-files=-1");
        positive("default maximum opens", defaultMaximumOpens);
        String defaultId = io.defaultStorageId();
        identifier("default-storage-id", defaultId);
        var knownWorkloads = new HashSet<>(WorkloadSettings.resolveGroups(config).keySet());
        knownWorkloads.add("default");
        var assignedWorkloads = new HashSet<String>();
        var assignedPaths = new HashSet<Path>();
        var groups = new LinkedHashMap<String, StorageGroup>();
        for (var source : Objects.requireNonNullElse(io.storageGroups(), new StartupStorageConfig[0])) {
            String id = source.id(); identifier("storage id", id);
            int maximum = or(source.maximumOpens(), defaultMaximumOpens);
            if (maximum < 1 || maximum > 1024) throw invalid("maximum-opens must be in [1, 1024]");
            long progress = millis("progress-interval", or(source.progressInterval(), Duration.ofSeconds(1)), false);
            long stale = millis("stale-after", or(source.staleAfter(), Duration.ofSeconds(2)), false);
            if (progress < 1 || progress > 60000) throw invalid("progress-interval must be in [1ms, 60s]");
            if (stale < 50 || stale > 60000) throw invalid("stale-after must be in [50ms, 60s]");
            if (stale < progress) throw invalid("stale-after must not be shorter than progress-interval");
            var paths = new ArrayList<Path>();
            for (var path : Objects.requireNonNullElse(source.paths(), new Path[0])) {
                var resolved = path(base, path);
                if (!assignedPaths.add(resolved)) throw invalid("duplicate startup storage path: " + resolved);
                paths.add(resolved);
            }
            var workloads = new ArrayList<String>();
            for (String name : Objects.requireNonNullElse(source.workloadGroups(), new String[0])) {
                if (!knownWorkloads.contains(name)) throw invalid("unknown startup workload group: " + name);
                if (!assignedWorkloads.add(name)) throw invalid("duplicate startup workload group: " + name);
                workloads.add(name);
            }
            var group = new StorageGroup(id, maximum, progress, stale, paths, workloads);
            if (groups.putIfAbsent(id, group) != null) throw invalid("duplicate startup storage id: " + id);
        }
        if (!groups.containsKey(defaultId)) throw invalid("default-storage-id must reference a configured storage group");
        return new StorageAdmission(true, defaultId, groups);
    }

    private static CacheSnapshot snapshot(HotCacheSnapshotConfig source, CacheSnapshot parent, Path base)
            throws GestaltException {
        boolean enabled = parent != null && parent.enabled();
        Path directory = parent == null ? null : parent.directory();
        SnapshotPolicy data = parent == null ? DEFAULT_DATA : parent.data();
        SnapshotPolicy metadata = parent == null ? DEFAULT_METADATA : parent.metadata();
        if (source != null) {
            enabled = or(source.enabled(), enabled);
            if (source.directory() != null) directory = path(base, source.directory());
            data = policy(source.data(), data); metadata = policy(source.metadata(), metadata);
        }
        if (enabled && directory == null) throw invalid("enabled hot-cache-snapshot requires an explicit directory");
        return new CacheSnapshot(enabled, directory, data, metadata);
    }

    private static SnapshotPolicy policy(CacheSnapshotPolicyConfig source, SnapshotPolicy parent)
            throws GestaltException {
        if (source == null) return parent;
        return new SnapshotPolicy(bytes(source.diskBudget(), parent.diskBudgetBytes()),
                bytes(source.snapshotBudget(), parent.snapshotBudgetBytes()), or(source.collectorEntries(), parent.collectorEntries()),
                bytes(source.indexRam(), parent.indexRamBytes()), bytes(source.maximumBlock(), parent.maximumBlockBytes()),
                bytes(source.lookupBuffer(), parent.lookupBufferBytes()), bytes(source.minimumFreeDisk(), parent.minimumFreeDiskBytes()),
                millis("snapshot-deadline", source.snapshotDeadline(), parent.snapshotDeadlineMillis(), false),
                millis("period", source.period(), parent.periodMillis(), true), or(source.sampleEvery(), parent.sampleEvery()),
                or(source.lookupConcurrency(), parent.lookupConcurrency()), bytes(source.readCeiling(), parent.readCeilingBytesPerSecond()),
                bytes(source.writeCeiling(), parent.writeCeilingBytesPerSecond()));
    }

    private static void validateCapacity(String name, DataSize configuredSize, DataSize configuredMetadata,
                                         DataSize configuredWbm, boolean enabled, boolean defaultCache) {
        long total = bytes(configuredSize, defaultCache ? 512 * MIB : -1);
        long metadata = bytes(configuredMetadata, 0), wbm = bytes(configuredWbm, 0);
        if (total < 0 || !defaultCache && total == 0 || metadata < 0 || metadata > 0 && metadata >= total || wbm < 0)
            throw invalid("invalid block cache capacity: " + name);
        try { Math.addExact(total, wbm); }
        catch (ArithmeticException overflow) { throw invalid("block cache plus write-buffer-manager overflows: " + name); }
        if (enabled && (total <= 0 || metadata <= 0))
            throw invalid("snapshot cache requires positive split DATA and METADATA capacities: " + name);
    }
    private static Path path(Path base, Path path) {
        if (path == null || path.toString().isBlank()) throw invalid("startup path must not be blank");
        return base.resolve(path).toAbsolutePath().normalize();
    }
    private static long millis(String name, Duration duration, long fallback, boolean zeroAllowed) {
        return duration == null ? fallback : millis(name, duration, zeroAllowed);
    }
    private static long millis(String name, Duration duration, boolean zeroAllowed) {
        if (duration.isNegative() || !zeroAllowed && duration.isZero()) throw invalid(name + " must be positive");
        try {
            long result = duration.toMillis();
            if (!duration.isZero() && result == 0) throw invalid(name + " must be at least 1ms");
            // No truncation: the C ABI cannot represent sub-millisecond precision.
            if (!duration.equals(Duration.ofMillis(result))) throw invalid(name + " must use whole milliseconds");
            return result;
        } catch (ArithmeticException overflow) { throw invalid(name + " overflows milliseconds"); }
    }
    private static long bytes(DataSize value, long fallback) { return value == null ? fallback : value.longValue(); }
    private static <T> T or(T value, T fallback) { return value == null ? fallback : value; }
    private static boolean powerOfTwo(long value) { return value > 0 && (value & (value - 1)) == 0; }
    private static void identifier(String name, String value) {
        if (value == null || !value.matches("[A-Za-z0-9_-]+") || value.length() > 64)
            throw invalid(name + " must contain 1..64 letters, digits, underscores or dashes");
    }
    private static void positive(String name, long value) { if (value <= 0) throw invalid(name + " must be positive"); }
    private static void nonnegative(String name, long value) { if (value < 0) throw invalid(name + " must not be negative"); }
    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException("Startup configuration: " + message); }
}
