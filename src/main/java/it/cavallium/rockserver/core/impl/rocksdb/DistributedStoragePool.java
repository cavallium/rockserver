package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.config.GlobalDatabaseConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.github.gestalt.config.exceptions.GestaltException;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

/** Native owner for ranges sharing one persistent database, not a catalogue or node bootstrap. */
public final class DistributedStoragePool implements AutoCloseable {
    public static final String STATE_COLUMN = "_distributed_range_state_";
    public static final String RECEIPTS_COLUMN = "_distributed_range_receipts_";
    private static final Set<String> INTERNAL_COLUMNS = Set.of(STATE_COLUMN, RECEIPTS_COLUMN);
    private static final Set<String> RESERVED_COLUMNS = Set.of("default", "_column_schemas_",
            "_merge_operators_", "_cdc_meta_", STATE_COLUMN, RECEIPTS_COLUMN);

    private final RocksDBLoader.LoadedDb loaded;
    private final GlobalDatabaseConfig globalConfig;
    private final Logger logger;
    private final RangeOwnership ownership = new RangeOwnership();
    // Serializes native schema mutation/close, without holding the admission metadata monitor.
    private final Semaphore nativeMutation = new Semaphore(1);
    private final Object admission = new Object();
    private final Map<String, ColumnFamilyHandle> handles = new HashMap<>();
    private final Map<UUID, Binding> bindings = new HashMap<>();
    private final List<ColumnFamilyHandle> newHandles = new ArrayList<>();
    private final List<ReplicatedRangeStore> ranges = new ArrayList<>();
    private boolean closing;
    private boolean closed;

    private record Binding(String name, byte[] schema, boolean mergeAdmitted) {}

    private DistributedStoragePool(RocksDBLoader.LoadedDb loaded, GlobalDatabaseConfig globalConfig, Logger logger) {
        this.loaded = loaded;
        this.globalConfig = globalConfig;
        this.logger = logger;
        loaded.db().getStartupColumns().forEach((descriptor, handle) ->
                handles.put(new String(descriptor.getName(), StandardCharsets.US_ASCII), handle));
    }

    public static DistributedStoragePool open(Path persistentPath, DatabaseConfig config, Logger logger) {
        Objects.requireNonNull(persistentPath, "persistentPath");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(logger, "logger");
        final GlobalDatabaseConfig globalConfig;
        try {
            globalConfig = config.global();
            if (!globalConfig.optimistic()) {
                throw new IllegalArgumentException("Distributed pools require global.optimistic=true");
            }
        } catch (GestaltException ex) {
            throw it.cavallium.rockserver.core.common.RocksDBException.of(
                    it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.CONFIG_ERROR,
                    "Failed to resolve distributed pool configuration", ex);
        }
        return new DistributedStoragePool(RocksDBLoader.load(persistentPath, config, logger,
                null, null, INTERNAL_COLUMNS), globalConfig, logger);
    }

    /** Bind the supplied pool-schema names; admission is supplied by the qualified range adapter. */
    public ReplicatedRangeStore openRange(RangeOwnership.Owner owner, RangeOwnership.Interval interval,
            byte[] schemaDigest, Map<UUID, String> physicalColumns, Set<UUID> admittedMergeColumns)
            throws RocksDBException {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(interval, "interval");
        byte[] schema = schemaDigest.clone();
        var names = Map.copyOf(physicalColumns);
        var merges = Set.copyOf(admittedMergeColumns);
        if (schema.length != 32 || !names.keySet().equals(interval.columns())
                || !names.keySet().containsAll(merges) || Set.copyOf(names.values()).size() != names.size()) {
            throw new IllegalArgumentException("Invalid range schema, column coverage or aliases");
        }
        for (String name : names.values()) {
            if (name.isEmpty() || name.indexOf('\0') >= 0 || RESERVED_COLUMNS.contains(name)
                    || !StandardCharsets.US_ASCII.newEncoder().canEncode(name)) {
                throw new IllegalArgumentException("Physical columns require unique nonreserved ASCII names");
            }
        }
        nativeMutation.acquireUninterruptibly();
        try {
            synchronized (admission) {
                if (closing) throw new IllegalStateException("Pool is closing");
            }
            // No metadata monitor is held during loader calls, CF creation or store recovery.
            for (var entry : names.entrySet()) {
                Binding existing = bindings.get(entry.getKey());
                if (existing != null && (!existing.name.equals(entry.getValue())
                        || !Arrays.equals(existing.schema, schema)
                        || existing.mergeAdmitted != merges.contains(entry.getKey()))) {
                    throw new IllegalArgumentException("Incompatible logical column binding or merge admission");
                }
                if (bindings.entrySet().stream().anyMatch(other -> !other.getKey().equals(entry.getKey())
                        && other.getValue().name.equals(entry.getValue()))) {
                    throw new IllegalArgumentException("Physical column is already bound to another logical column");
                }
            }
            var rangeColumns = new HashMap<UUID, ColumnFamilyHandle>();
            for (var entry : names.entrySet()) {
                String name = entry.getValue();
                var handle = handles.get(name);
                boolean mergeConfigured;
                if (handle == null) {
                    var options = RocksDBLoader.getColumnOptions(name, loaded.path(), loaded.definitiveDbPath(),
                            globalConfig, logger, loaded.refs(), false, loaded.caches());
                    mergeConfigured = options.mergeOperator() != null;
                    if (merges.contains(entry.getKey()) && !mergeConfigured) {
                        throw new IllegalArgumentException("Merge admission requires a configured operator");
                    }
                    handle = loaded.db().get().createColumnFamily(new ColumnFamilyDescriptor(
                            name.getBytes(StandardCharsets.US_ASCII), options.options()));
                    newHandles.add(handle);
                    handles.put(name, handle);
                    loaded.definitiveColumnFamilyOptionsMap().put(name, options.options());
                    loaded.mergeOperators().put(name, options.mergeOperator());
                } else {
                    mergeConfigured = loaded.mergeOperators().get(name) != null;
                    if (merges.contains(entry.getKey()) && !mergeConfigured) {
                        throw new IllegalArgumentException("Merge admission requires a configured operator");
                    }
                }
                rangeColumns.put(entry.getKey(), handle);
            }
            ownership.register(owner, interval, RangeOwnership.State.ACTIVE);
            try {
                var store = new ReplicatedRangeStore(loaded.db().get(), ownership, owner, schema, rangeColumns,
                        handles.get(STATE_COLUMN), handles.get(RECEIPTS_COLUMN), merges);
                names.forEach((id, name) -> bindings.putIfAbsent(id, new Binding(name, schema, merges.contains(id))));
                ranges.add(store);
                return store;
            } catch (RuntimeException | RocksDBException failure) {
                var current = ownership.get(owner);
                if (current.state() == RangeOwnership.State.ACTIVE) ownership.fence(owner);
                ownership.retire(owner);
                throw failure;
            }
        } finally {
            nativeMutation.release();
        }
    }

    /** Stop admission and drain borrowed range work. A timeout retains every native resource for retry. */
    public boolean close(Duration timeout) throws IOException, InterruptedException {
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative close timeout");
        long budget = timeout.toNanos();
        long started = System.nanoTime();
        synchronized (admission) {
            if (closed) return true;
            closing = true;
        }
        if (!nativeMutation.tryAcquire(budget, TimeUnit.NANOSECONDS)) return false;
        try {
            synchronized (admission) { if (closed) return true; }
            boolean drained = true;
            for (var range : ranges) {
                drained &= range.close(Duration.ofNanos(Math.max(0, budget - (System.nanoTime() - started))));
            }
            if (!drained) return false;
            IOException failure = null;
            for (var handle : newHandles) {
                try { handle.close(); } catch (RuntimeException ex) { failure = append(failure, ex); }
            }
            try { loaded.db().close(); } catch (Exception ex) { failure = append(failure, ex); }
            // Keep refs if a native close failure left the original database owner live.
            if (loaded.db().get().isOwningHandle()) {
                if (failure != null) throw failure;
                throw new IOException("Native database did not release ownership");
            }
            try { loaded.refs().close(); } catch (RuntimeException ex) { failure = append(failure, ex); }
            synchronized (admission) { closed = true; }
            if (failure != null) throw failure;
            return true;
        } finally { nativeMutation.release(); }
    }

    private static IOException append(IOException failure, Exception next) {
        if (failure == null) return new IOException("Failed to close distributed pool", next);
        failure.addSuppressed(next);
        return failure;
    }

    @Override public void close() throws IOException {
        try {
            if (!close(Duration.ofSeconds(30))) throw new IOException("Range operations or snapshot pins remain active");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while draining distributed pool", ex);
        }
    }

}
