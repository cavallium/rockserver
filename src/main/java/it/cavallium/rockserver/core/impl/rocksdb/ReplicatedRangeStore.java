package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import it.cavallium.rockserver.core.common.distributed.CommandResult;
import it.cavallium.rockserver.core.common.distributed.TransactionScope;
import it.cavallium.rockserver.core.common.distributed.PhysicalRangePage;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.TimeUnit;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.TransactionDB;

/**
 * Native atomic apply seam for one range in a shared storage pool.
 * The pool owns the database and CF handles and must outlive this store and its
 * snapshot pins. Only committed, canonical plans may enter this seam; admission,
 * consensus, ownership and operator qualification belong to the range adapter.
 */
public final class ReplicatedRangeStore {
    private static final int STATE_VERSION = 1;
    private static final int MAX_RESULT_BYTES = 2 * 1024 * 1024;
    private final RocksDB db;
    // getBaseDB() is borrowed and does not retain its original Java/native owner.
    private final RocksDB databaseOwner;
    private final UUID groupId;
    private final long rangeEpoch;
    private final byte[] schemaDigest;
    private final Map<UUID, ColumnFamilyHandle> columns;
    private final Set<UUID> deterministicMergeColumns;
    private final ColumnFamilyHandle state;
    private final ColumnFamilyHandle receipts;
    private final byte[] markerKey;
    private final byte[] installingKey;
    private final byte[] configurationKey;
    private final RangeOwnership ownership;
    private final RangeOwnership.Owner owner;
    private final RangeOwnership.Interval interval;
    private final ReentrantLock applyLane = new ReentrantLock();
    private final ReentrantReadWriteLock nativeLifecycle = new ReentrantReadWriteLock();
    private volatile boolean closing;
    private volatile ApplyPosition applied;
    private final AtomicInteger snapshotPins = new AtomicInteger();
    private volatile byte[] pendingInstallation;
    private volatile long installationGeneration;

    public ReplicatedRangeStore(RocksDB db, RangeOwnership ownership, RangeOwnership.Owner owner,
            byte[] schemaDigest, Map<UUID, ColumnFamilyHandle> columns,
            ColumnFamilyHandle state, ColumnFamilyHandle receipts,
            Set<UUID> deterministicMergeColumns) throws RocksDBException {
        this.databaseOwner = Objects.requireNonNull(db);
        if (db instanceof TransactionDB) {
            throw new IllegalArgumentException("Distributed pools require regular or optimistic-base RocksDB for atomic range deletion");
        }
        this.db = db instanceof OptimisticTransactionDB optimistic ? optimistic.getBaseDB() : db;
        this.ownership = Objects.requireNonNull(ownership);
        this.owner = Objects.requireNonNull(owner);
        this.interval = ownership.get(owner).interval();
        this.groupId = owner.groupId();
        this.rangeEpoch = owner.epoch();
        if (schemaDigest.length != 32 || !interval.columns().equals(columns.keySet())) {
            throw new IllegalArgumentException("Invalid range schema or physical column coverage");
        }
        this.schemaDigest = schemaDigest.clone();
        this.columns = Map.copyOf(columns);
        this.deterministicMergeColumns = Set.copyOf(deterministicMergeColumns);
        if (!columns.keySet().containsAll(this.deterministicMergeColumns)) {
            throw new IllegalArgumentException("Merge admission refers to an unknown column");
        }
        this.state = Objects.requireNonNull(state);
        this.receipts = Objects.requireNonNull(receipts);
        if (state.getID() == receipts.getID() || columns.values().stream().anyMatch(
                cf -> cf.getID() == state.getID() || cf.getID() == receipts.getID())) {
            throw new IllegalArgumentException("User, state and receipt CFs must be distinct");
        }
        if (columns.values().stream().map(ColumnFamilyHandle::getID).distinct().count() != columns.size()) {
            throw new IllegalArgumentException("Logical columns must have distinct physical column families");
        }
        markerKey = uuidBytes(groupId);
        installingKey = ByteBuffer.allocate(17).put(markerKey).put((byte) 0x7f).array();
        configurationKey = ByteBuffer.allocate(17).put(markerKey).put((byte) 1).array();
        byte[] recovered = this.db.get(state, markerKey);
        applied = recovered == null ? null : decodePosition(recovered);
        pendingInstallation = this.db.get(state, installingKey);
        if (pendingInstallation != null) {
            if (pendingInstallation.length != 52 || ByteBuffer.wrap(pendingInstallation).getInt() != STATE_VERSION) {
                throw new IllegalStateException("Invalid pending installation marker");
            }
            if (ownership.get(owner).state() == RangeOwnership.State.ACTIVE) ownership.fence(owner);
        }
    }

    /** Null means no log entry has yet completed. This is not a durable snapshot watermark. */
    public ApplyPosition applied() { return applied; }
    public int activeSnapshotPins() { return snapshotPins.get(); }
    public boolean pendingInstallation() { return pendingInstallation != null; }
    public RangeOwnership.Ownership ownership() { return ownership.get(owner); }
    public byte[] schemaDigest() { return schemaDigest.clone(); }
    public Set<UUID> deterministicMergeColumns() { return deterministicMergeColumns; }

    public record Configuration(long term, long index, byte[] data) {
        public Configuration {
            if (term < 0 || index < -1 || (index == -1 && term != 0) || data.length == 0 || data.length > 65536) {
                throw new IllegalArgumentException("Invalid cut configuration");
            }
            data = data.clone();
        }
        @Override public byte[] data() { return data.clone(); }
        public static Configuration decode(byte[] value) { return decodeConfiguration(value); }
    }

    public Configuration configuration() throws RocksDBException {
        enterNative();
        try { return configurationUnderAdmission(); }
        finally { leaveNative(); }
    }

    private Configuration configurationUnderAdmission() throws RocksDBException {
        byte[] value = db.get(state, configurationKey);
        return value == null ? null : decodeConfiguration(value);
    }

    public void initializeConfiguration(byte[] initialConfiguration) throws RocksDBException {
        enterNative();
        try { initializeConfigurationUnderAdmission(initialConfiguration); }
        finally { leaveNative(); }
    }

    private void initializeConfigurationUnderAdmission(byte[] initialConfiguration) throws RocksDBException {
        applyLane.lock();
        try {
            if (configurationUnderAdmission() != null) return;
            if (applied != null || pendingInstallation != null) throw new IllegalStateException("Recovered range has no cut configuration");
            var bootstrap = new Configuration(0, -1, initialConfiguration);
            try (var options = new WriteOptions().setDisableWAL(false).setSync(true)) {
                db.put(state, options, configurationKey, encodeConfiguration(bootstrap));
            }
        } finally { applyLane.unlock(); }
    }

    public void fence() {
        applyLane.lock();
        try {
            var current = ownership.get(owner);
            if (current.state() == RangeOwnership.State.ACTIVE) ownership.fence(owner);
            else if (current.state() != RangeOwnership.State.FENCED) throw new IllegalStateException("Range cannot be fenced");
        } finally { applyLane.unlock(); }
    }

    /** Local activation only; the range adapter must supply the committed ownership authority. */
    public void activate() {
        enterNative();
        try { activateUnderAdmission(); }
        finally { leaveNative(); }
    }

    private void activateUnderAdmission() {
        applyLane.lock();
        try {
            if (pendingInstallation != null) throw new IllegalStateException("Installation remains incomplete");
            ownership.activate(owner);
        } finally { applyLane.unlock(); }
    }

    private void requireActive() {
        if (pendingInstallation != null || ownership.get(owner).state() != RangeOwnership.State.ACTIVE) {
            throw new IllegalStateException("Range is fenced");
        }
    }

    private boolean contains(byte[] key) {
        byte[] lower = interval.lowerInclusive();
        byte[] upper = interval.upperExclusive();
        return (lower == null || Arrays.compareUnsigned(key, lower) >= 0)
                && (upper == null || Arrays.compareUnsigned(key, upper) < 0);
    }

    public void validate(DistributedCommand command) {
        if (closing) throw new IllegalStateException("Range admission is closed");
        requireActive();
        if (!groupId.equals(command.groupId()) || rangeEpoch != command.rangeEpoch()
                || !Arrays.equals(schemaDigest, command.schemaDigest())) {
            throw new IllegalArgumentException("Wrong group, epoch or schema");
        }
        for (var mutation : command.mutations()) {
            if (!columns.containsKey(mutation.columnId())) throw new IllegalArgumentException("Unknown logical column");
            if (mutation.kind() == Mutation.Kind.MERGE && !deterministicMergeColumns.contains(mutation.columnId())) {
                throw new IllegalArgumentException("Column has no admitted deterministic merge operator");
            }
            if (!contains(mutation.key())) throw new IllegalArgumentException("Mutation outside owned interval");
            if (mutation.kind() == Mutation.Kind.DELETE_RANGE) {
                byte[] upper = interval.upperExclusive();
                if (upper != null && Arrays.compareUnsigned(mutation.value(), upper) > 0) {
                    throw new IllegalArgumentException("Range deletion outside owned interval");
                }
            }
        }
        for (var predicate : command.predicates()) {
            if (!columns.containsKey(predicate.columnId()) || !contains(predicate.key())) {
                throw new IllegalArgumentException("Predicate outside owned column/interval");
            }
        }
        if (command.mutations().stream().anyMatch(m -> m.returnMode() != Mutation.ReturnMode.NONE)
                && command.mutations().stream().anyMatch(m -> m.kind() == Mutation.Kind.DELETE_RANGE)) {
            throw new IllegalArgumentException("Point results with range deletion require a range-aware overlay");
        }
    }

    /** Evaluates autocommit return values in committed order, preserving native merge operands. */
    public byte[] apply(ApplyPosition position, DistributedCommand command) throws RocksDBException {
        return applyInternal(position, command, null);
    }

    /**
     * Installs user effects, the original result and the completed marker in one
     * WAL-enabled cross-CF batch. The result is evaluated by ordered leader
     * planning before consensus, never recomputed during replay or retry.
     */
    public byte[] apply(ApplyPosition position, DistributedCommand command, byte[] result)
            throws RocksDBException {
        return applyInternal(position, command, Objects.requireNonNull(result));
    }

    private byte[] applyInternal(ApplyPosition position, DistributedCommand command, byte[] result)
            throws RocksDBException {
        enterNative();
        try { return applyUnderAdmission(position, command, result); }
        finally { leaveNative(); }
    }

    private byte[] applyUnderAdmission(ApplyPosition position, DistributedCommand command, byte[] result)
            throws RocksDBException {
        Objects.requireNonNull(position);
        validate(command);
        if (result != null && !command.predicates().isEmpty()) {
            throw new IllegalArgumentException("Precomputed results require a fully evaluated plan without live predicates");
        }
        if (result != null && result.length > MAX_RESULT_BYTES) throw new IllegalArgumentException("Result exceeds receipt budget");
        byte[] stableResult = result == null ? null : result.clone();
        byte[] digest = command.payloadDigest();
        byte[] receiptKey = receiptKey(command.operationId());
        applyLane.lock();
        try {
            requireActive();
            byte[] saved = db.get(receipts, receiptKey);
            byte[] original = saved == null ? null : decodeReceipt(saved, digest);
            if (applied != null && position.index() <= applied.index()) {
                if (saved == null) throw new IllegalStateException("Applied command has no retained receipt");
                return original;
            }
            requireNext(position);
            boolean installUserEffects = true;
            if (saved == null && stableResult == null) {
                var evaluated = evaluate(command);
                stableResult = evaluated.encode();
                installUserEffects = evaluated.status() == CommandResult.Status.SUCCESS;
            }
            try (WriteBatch batch = new WriteBatch();
                    WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                if (saved == null) {
                    if (installUserEffects) for (var mutation : command.mutations()) {
                        var cf = columns.get(mutation.columnId());
                        if (cf == null) throw new IllegalArgumentException("Unknown logical column");
                        switch (mutation.kind()) {
                            case PUT -> batch.put(cf, mutation.key(), mutation.value());
                            case DELETE -> batch.delete(cf, mutation.key());
                            case MERGE -> batch.merge(cf, mutation.key(), mutation.value());
                            case DELETE_RANGE -> batch.deleteRange(cf, mutation.key(), mutation.value());
                        }
                    }
                    batch.put(receipts, receiptKey, encodeReceipt(digest, stableResult));
                }
                batch.put(state, markerKey, encodePosition(position));
                db.write(options, batch);
            }
            applied = position;
            return saved == null ? stableResult : original;
        } finally {
            applyLane.unlock();
        }
    }

    private CommandResult evaluate(DistributedCommand command) throws RocksDBException {
        for (var predicate : command.predicates()) {
            byte[] current = db.get(columns.get(predicate.columnId()), predicate.key());
            if (!Arrays.equals(current, predicate.expectedValue())) {
                return new CommandResult(CommandResult.Status.CONFLICT, List.of());
            }
        }
        if (command.mutations().stream().allMatch(m -> m.returnMode() == Mutation.ReturnMode.NONE)) {
            return new CommandResult(CommandResult.Status.SUCCESS, List.of());
        }
        var values = new ArrayList<byte[]>();
        long resultBytes = 8;
        try (var overlay = new WriteBatchWithIndex(true); var options = new ReadOptions()) {
            for (var mutation : command.mutations()) {
                var cf = columns.get(mutation.columnId());
                byte[] key = mutation.key();
                byte[] value = null;
                if (mutation.returnMode() == Mutation.ReturnMode.PREVIOUS) {
                    value = overlay.getFromBatchAndDB(db, cf, options, key);
                }
                switch (mutation.kind()) {
                    case PUT -> overlay.put(cf, key, mutation.value());
                    case DELETE -> overlay.delete(cf, key);
                    case MERGE -> overlay.merge(cf, key, mutation.value());
                    case DELETE_RANGE -> throw new IllegalStateException("Range result plan escaped validation");
                }
                if (mutation.returnMode() == Mutation.ReturnMode.CURRENT) {
                    value = overlay.getFromBatchAndDB(db, cf, options, key);
                }
                if (mutation.returnMode() != Mutation.ReturnMode.NONE) {
                    resultBytes += 4L + (value == null ? 0 : value.length);
                    if (resultBytes > MAX_RESULT_BYTES) return new CommandResult(CommandResult.Status.RESULT_TOO_LARGE, List.of());
                    values.add(value);
                }
            }
        }
        return new CommandResult(CommandResult.Status.SUCCESS, values);
    }

    /** Every committed configuration/no-op entry must be accounted for in log order. */
    public void applyNoop(ApplyPosition position) throws RocksDBException {
        applyNoop(position, null);
    }

    public void applyNoop(ApplyPosition position, byte[] configuration) throws RocksDBException {
        enterNative();
        try { applyNoopUnderAdmission(position, configuration); }
        finally { leaveNative(); }
    }

    private void applyNoopUnderAdmission(ApplyPosition position, byte[] configuration) throws RocksDBException {
        applyLane.lock();
        try {
            var current = ownership.get(owner).state();
            if (pendingInstallation != null || (current != RangeOwnership.State.ACTIVE && current != RangeOwnership.State.FENCED)) {
                throw new IllegalStateException("Range cannot complete Raft accounting");
            }
            if (applied != null && position.index() <= applied.index()) return;
            requireNext(position);
            try (WriteBatch batch = new WriteBatch();
                    WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                batch.put(state, markerKey, encodePosition(position));
                if (configuration != null) {
                    batch.put(state, configurationKey, encodeConfiguration(new Configuration(position.term(), position.index(), configuration)));
                }
                db.write(options, batch);
            }
            applied = position;
        } finally {
            applyLane.unlock();
        }
    }

    /**
     * Replaces only this fenced interval, retaining a durable INSTALLING marker
     * until all imported data and metadata have been flushed through the WAL.
     * The caller supplies consensus/ownership authority and keeps the pool open.
     * Successful installation leaves ownership fenced for explicit activation.
     */
    public NativeRangeImage.Manifest installSnapshot(Path image, NativeRangeImage.Limits limits)
            throws IOException, RocksDBException {
        return installSnapshot(image, limits, batches -> {});
    }

    public NativeRangeImage.Manifest installSnapshot(Path image, NativeRangeImage.Limits limits,
            InstallationObserver observer) throws IOException, RocksDBException {
        enterNative();
        try { return installUnderAdmission(image, limits, observer); }
        finally { leaveNative(); }
    }

    private NativeRangeImage.Manifest installUnderAdmission(Path image, NativeRangeImage.Limits limits,
            InstallationObserver observer) throws IOException, RocksDBException {
        var expected = NativeRangeImage.inspect(image, limits);
        if (!expected.owner().equals(owner) || !expected.interval().equals(interval)
                || !Arrays.equals(expected.schemaDigest(), schemaDigest)
                || !expected.deterministicMergeColumns().equals(deterministicMergeColumns)) {
            throw new IOException("Snapshot target identity/schema/coverage mismatch");
        }
        Objects.requireNonNull(observer);
        applyLane.lock();
        try {
            if (snapshotPins.get() != 0) throw new IllegalStateException("Native snapshot readers have not drained");
            if (ownership.get(owner).state() != RangeOwnership.State.FENCED) {
                throw new IllegalStateException("Snapshot installation requires a fenced interval");
            }
            if (pendingInstallation != null) {
                var pending = ByteBuffer.wrap(pendingInstallation);
                pending.getInt();
                long pendingTerm = pending.getLong();
                long pendingIndex = pending.getLong();
                byte[] hash = new byte[32];
                pending.get(hash);
                if (expected.position().index() < pendingIndex || expected.position().term() < pendingTerm
                        || (expected.position().index() == pendingIndex && !Arrays.equals(hash, expected.sha256()))) {
                    throw new IOException("Snapshot regresses or disagrees with pending installation");
                }
            }
            try (var lease = ownership.claimInstall(owner)) {
                installationGeneration++;
                pendingInstallation = ByteBuffer.allocate(52).putInt(STATE_VERSION)
                        .putLong(expected.position().term()).putLong(expected.position().index()).put(expected.sha256()).array();
                // Publish recovery authority before the first destructive interval change.
                try (var batch = new WriteBatch(); var options = new WriteOptions().setDisableWAL(false).setSync(true)) {
                    batch.put(state, installingKey, pendingInstallation);
                    batch.delete(state, markerKey);
                    db.write(options, batch);
                }
                applied = null;
                try (var writer = new InstallWriter(observer)) {
                    for (var cf : columns.values()) clearForInstall(cf, interval.lowerInclusive(), interval.upperExclusive(), null, writer);
                    clearForInstall(state, markerKey, null, markerKey, writer);
                    clearForInstall(receipts, markerKey, null, markerKey, writer);
                    writer.flush();
                    var actual = NativeRangeImage.visit(image, limits, (section, column, key, value) -> {
                        try {
                            switch (section) {
                                case USER -> writer.put(columns.get(column), key, value);
                                case RECEIPTS -> writer.put(receipts, key, value);
                                case STATE -> {
                                    if (Arrays.equals(key, installingKey)) throw new IOException("Image contains incomplete-install metadata");
                                    if (!Arrays.equals(key, markerKey)) writer.put(state, key, value);
                                }
                            }
                        } catch (RocksDBException storage) { throw new IOException("Native snapshot import failed", storage); }
                    });
                    if (!Arrays.equals(expected.sha256(), actual.sha256())) throw new IOException("Snapshot changed during installation");
                    writer.flush();
                    // Flush every prior CF mutation before publishing the final applied marker.
                    db.flushWal(true);
                    try (var batch = new WriteBatch(); var options = new WriteOptions().setDisableWAL(false).setSync(true)) {
                        batch.put(state, markerKey, encodePosition(expected.position()));
                        batch.delete(state, installingKey);
                        db.write(options, batch);
                    }
                    applied = expected.position();
                    installationGeneration++;
                    pendingInstallation = null;
                    return actual;
                }
            }
        } finally { applyLane.unlock(); }
    }

    @FunctionalInterface
    public interface InstallationObserver {
        /** Fault/measurement hook; the interval remains exclusively leased and unservable. */
        void afterBatch(int completedBatches) throws IOException;
    }

    private final class InstallWriter implements AutoCloseable {
        private final InstallationObserver observer;
        private final WriteBatch batch = new WriteBatch();
        private final WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false);
        private long bytes;
        private int batches;
        InstallWriter(InstallationObserver observer) { this.observer = observer; }
        void put(ColumnFamilyHandle cf, byte[] key, byte[] value) throws IOException, RocksDBException {
            reserve(key.length + (long) value.length);
            batch.put(cf, key, value);
            bytes += key.length + (long) value.length + 32;
        }
        void delete(ColumnFamilyHandle cf, byte[] key) throws IOException, RocksDBException {
            reserve(key.length);
            batch.delete(cf, key);
            bytes += key.length + 32L;
        }
        private void reserve(long added) throws IOException, RocksDBException {
            if (batch.count() >= 256 || (batch.count() != 0 && bytes + added + 32 > 2L * 1024 * 1024)) flush();
        }
        void flush() throws IOException, RocksDBException {
            if (batch.count() == 0) return;
            db.write(options, batch);
            batch.clear();
            bytes = 0;
            observer.afterBatch(++batches);
        }
        @Override public void close() { batch.close(); options.close(); }
    }

    private void clearForInstall(ColumnFamilyHandle cf, byte[] lower, byte[] upper, byte[] prefix, InstallWriter writer)
            throws IOException, RocksDBException {
        try (var options = new ReadOptions().setFillCache(false); var iterator = db.newIterator(cf, options)) {
            if (lower == null) iterator.seekToFirst(); else iterator.seek(lower);
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (upper != null && Arrays.compareUnsigned(key, upper) >= 0) break;
                if (prefix != null && (key.length < prefix.length
                        || !Arrays.equals(prefix, 0, prefix.length, key, 0, prefix.length))) break;
                if (cf.getID() != state.getID() || !Arrays.equals(key, installingKey)) writer.delete(cf, key);
                iterator.next();
            }
            iterator.status();
        }
    }

    public byte[] operationResult(OperationId id, byte[] payloadDigest) throws RocksDBException {
        enterNative();
        try { return operationResultUnderAdmission(id, payloadDigest); }
        finally { leaveNative(); }
    }

    private byte[] operationResultUnderAdmission(OperationId id, byte[] payloadDigest) throws RocksDBException {
        long generation = installationGeneration;
        if (pendingInstallation != null) throw new IllegalStateException("Range metadata is being installed");
        byte[] saved = db.get(receipts, receiptKey(id));
        if (generation != installationGeneration || pendingInstallation != null) {
            throw new IllegalStateException("Range metadata changed during status lookup");
        }
        return saved == null ? null : decodeReceipt(saved, payloadDigest);
    }

    /**
     * Captures user data, receipts and marker from the same native cut.
     * The adapter must establish the read/fence barrier before invoking this.
     * A native cut alone does not constitute a durable installable Raft snapshot.
     */
    public SnapshotPin capture() throws RocksDBException {
        enterNative();
        try { return captureUnderAdmission(); }
        finally { leaveNative(); }
    }

    /** One local cut restricted to the complete declared scope; the adapter supplies the read barrier. */
    public ScopedSnapshot capture(TransactionScope scope) throws RocksDBException {
        Objects.requireNonNull(scope);
        enterNative();
        try {
            requireActive();
            for (var span : scope.spans()) {
                if (!columns.containsKey(span.columnId())) throw new IllegalArgumentException("Scope column is outside this range");
                byte[] lower = span.lowerInclusive(), upper = span.upperExclusive();
                if (span.isPoint()) {
                    if (!contains(lower)) throw new IllegalArgumentException("Scope key is outside this range");
                } else if ((interval.lowerInclusive() != null && (lower == null || Arrays.compareUnsigned(lower, interval.lowerInclusive()) < 0))
                        || (interval.upperExclusive() != null && (upper == null || Arrays.compareUnsigned(upper, interval.upperExclusive()) > 0))) {
                    throw new IllegalArgumentException("Scope interval is outside this range");
                }
            }
            return new ScopedSnapshot(scope, captureUnderAdmission());
        } finally { leaveNative(); }
    }

    /** Scope cannot expand; native reader ownership and close/drain semantics are retained by the pin. */
    public final class ScopedSnapshot implements AutoCloseable {
        private final TransactionScope scope;
        private final SnapshotPin pin;
        private ScopedSnapshot(TransactionScope scope, SnapshotPin pin) { this.scope = scope; this.pin = pin; }
        public TransactionScope scope() { return scope; }
        public ApplyPosition position() { return pin.position(); }
        public UUID groupId() { return pin.groupId(); }
        public long rangeEpoch() { return pin.rangeEpoch(); }
        public byte[] schemaDigest() { return pin.schemaDigest(); }
        public byte[] get(UUID columnId, byte[] key) throws RocksDBException {
            byte[] physicalKey = Objects.requireNonNull(key).clone();
            if (!scope.contains(columnId, physicalKey)) throw new IllegalArgumentException("Read outside declared scope");
            return pin.get(columnId, physicalKey);
        }
        /** Null bounds request the entire owned interval, rather than silently clipping to the declared scope. */
        public void visitColumn(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive, SnapshotVisitor visitor)
                throws RocksDBException, IOException {
            byte[] lower = lowerInclusive == null ? interval.lowerInclusive() : lowerInclusive.clone();
            byte[] upper = upperExclusive == null ? interval.upperExclusive() : upperExclusive.clone();
            if (!scope.containsRange(columnId, lower, upper)) throw new IllegalArgumentException("Scan outside declared scope");
            pin.visitColumn(columnId, lower, upper, Objects.requireNonNull(visitor));
        }
        /** Exclusive full-physical-key continuation, in either direction, at this retained cut. */
        public PhysicalRangePage page(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive,
                                      boolean reverse, byte[] resumeAfter, int itemLimit, int byteLimit) throws RocksDBException {
            if (itemLimit < 1 || itemLimit > PhysicalRangePage.MAX_ITEMS
                    || byteLimit < PhysicalRangePage.HEADER_BYTES || byteLimit > PhysicalRangePage.MAX_BYTES) {
                throw new IllegalArgumentException("Invalid page budget");
            }
            if ((long)(lowerInclusive == null ? 0 : lowerInclusive.length) + (upperExclusive == null ? 0 : upperExclusive.length) > TransactionScope.MAX_BOUND_BYTES
                    || resumeAfter != null && resumeAfter.length > PhysicalRangePage.MAX_BYTES) {
                throw new IllegalArgumentException("Page request exceeds bound budget");
            }
            byte[] lower = lowerInclusive == null ? interval.lowerInclusive() : lowerInclusive.clone();
            byte[] upper = upperExclusive == null ? interval.upperExclusive() : upperExclusive.clone();
            if (!scope.containsRange(columnId, lower, upper)) throw new IllegalArgumentException("Page outside declared scope");
            byte[] resume = resumeAfter == null ? null : resumeAfter.clone();
            if (resume != null && (resume.length > PhysicalRangePage.MAX_BYTES
                    || lower != null && Arrays.compareUnsigned(resume, lower) < 0
                    || upper != null && Arrays.compareUnsigned(resume, upper) >= 0)) {
                throw new IllegalArgumentException("Continuation outside page bounds");
            }
            return pin.page(columnId, lower, upper, reverse, resume, itemLimit, byteLimit);
        }
        @Override public void close() { pin.close(); }
    }

    private SnapshotPin captureUnderAdmission() throws RocksDBException {
        requireActive();
        applyLane.lock();
        try {
            requireActive();
            Snapshot snapshot = db.getSnapshot();
            try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                byte[] marker = db.get(state, options, markerKey);
                ApplyPosition cut = marker == null ? null : decodePosition(marker);
                if (!Objects.equals(cut, applied)) throw new IllegalStateException("Snapshot marker differs from completed apply");
                var pin = new SnapshotPin(snapshot, cut);
                snapshotPins.incrementAndGet();
                return pin;
            } catch (Throwable failure) {
                db.releaseSnapshot(snapshot);
                throw failure;
            }
        } finally {
            applyLane.unlock();
        }
    }

    private void enterNative() {
        if (closing) throw new IllegalStateException("Range admission is closed");
        nativeLifecycle.readLock().lock();
        if (closing) {
            nativeLifecycle.readLock().unlock();
            throw new IllegalStateException("Range admission is closed");
        }
    }

    private void leaveNative() { nativeLifecycle.readLock().unlock(); }

    /**
     * Permanently closes borrowed native admission. Pool ownership stays intact
     * until true is returned. Existing pins retain their exact cut and may finish;
     * this method never closes their database or forcibly releases a native user.
     */
    public boolean close(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative close timeout");
        long nanos = timeout.toNanos();
        closing = true;
        if (!nativeLifecycle.writeLock().tryLock(nanos, TimeUnit.NANOSECONDS)) return false;
        try {
            fence();
            return snapshotPins.get() == 0;
        } finally { nativeLifecycle.writeLock().unlock(); }
    }

    public final class SnapshotPin implements AutoCloseable {
        private final Snapshot snapshot;
        private final ApplyPosition position;
        private boolean closed;
        private int nativeUsers;
        private SnapshotPin(Snapshot snapshot, ApplyPosition position) {
            this.snapshot = snapshot;
            this.position = position;
        }
        public ApplyPosition position() { return position; }
        public UUID groupId() { return groupId; }
        public long rangeEpoch() { return rangeEpoch; }
        public byte[] schemaDigest() { return schemaDigest.clone(); }
        public Set<UUID> columns() { return columns.keySet(); }
        public Set<UUID> deterministicMergeColumns() { return deterministicMergeColumns; }
        public RangeOwnership.Interval interval() { return interval; }
        public Configuration configuration() throws RocksDBException {
            acquireUse();
            try (var options = new ReadOptions().setSnapshot(snapshot)) {
                byte[] value = db.get(state, options, configurationKey);
                return value == null ? null : decodeConfiguration(value);
            } finally { releaseUse(); }
        }
        public byte[] get(UUID columnId, byte[] key) throws RocksDBException {
            acquireUse();
            try {
                var cf = columns.get(columnId);
                if (cf == null) throw new IllegalArgumentException("Unknown logical column");
                if (!contains(key)) throw new IllegalArgumentException("Read outside owned interval");
                try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                    return db.get(cf, options, key);
                }
            } finally {
                releaseUse();
            }
        }
        public byte[] operationResult(OperationId id, byte[] payloadDigest) throws RocksDBException {
            acquireUse();
            try {
                try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                    byte[] saved = db.get(receipts, options, receiptKey(id));
                    return saved == null ? null : decodeReceipt(saved, payloadDigest);
                }
            } finally {
                releaseUse();
            }
        }
        /** Visits copied key/value rows from this exact native cut in physical comparator order. */
        public void visitColumn(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive, SnapshotVisitor visitor)
                throws RocksDBException, IOException {
            var cf = columns.get(columnId);
            if (cf == null) throw new IllegalArgumentException("Unknown logical column");
            byte[] lower = lowerInclusive == null ? interval.lowerInclusive() : lowerInclusive.clone();
            byte[] upper = upperExclusive == null ? interval.upperExclusive() : upperExclusive.clone();
            if ((interval.lowerInclusive() != null && (lower == null || Arrays.compareUnsigned(lower, interval.lowerInclusive()) < 0))
                    || (interval.upperExclusive() != null && (upper == null || Arrays.compareUnsigned(upper, interval.upperExclusive()) > 0))) {
                throw new IllegalArgumentException("Export outside owned interval");
            }
            visit(cf, lower, upper, null, visitor);
        }
        private PhysicalRangePage page(UUID columnId, byte[] lower, byte[] upper, boolean reverse,
                                       byte[] resume, int itemLimit, int byteLimit) throws RocksDBException {
            var cf = columns.get(columnId);
            if (cf == null) throw new IllegalArgumentException("Unknown logical column");
            acquireUse();
            try (var lowerBound = lower == null ? null : new org.rocksdb.Slice(lower);
                    var upperBound = upper == null ? null : new org.rocksdb.Slice(upper);
                    var options = new ReadOptions().setSnapshot(snapshot).setFillCache(false)) {
                if (lowerBound != null) options.setIterateLowerBound(lowerBound);
                if (upperBound != null) options.setIterateUpperBound(upperBound);
                try (var iterator = db.newIterator(cf, options)) {
                    byte[] sizingBuffer = new byte[0];
                    if (resume != null) {
                        if (reverse) iterator.seekForPrev(resume); else iterator.seek(resume);
                        if (iterator.isValid() && iterator.key(sizingBuffer) == resume.length && Arrays.equals(iterator.key(), resume)) {
                            if (reverse) iterator.prev(); else iterator.next();
                        }
                    } else if (reverse) iterator.seekToLast();
                    else if (lower == null) iterator.seekToFirst(); else iterator.seek(lower);
                    var rows = new ArrayList<PhysicalRangePage.Row>();
                    int bytes = PhysicalRangePage.HEADER_BYTES, lastKeyBytes = 0;
                    while (iterator.isValid() && rows.size() < itemLimit) {
                        int keyBytes = iterator.key(sizingBuffer), valueBytes = iterator.value(sizingBuffer);
                        if (keyBytes < 0 || valueBytes < 0) throw new IllegalStateException("Invalid native row length");
                        long required = (long)bytes - lastKeyBytes + 8L + 2L * keyBytes + valueBytes;
                        if (required > byteLimit) {
                            iterator.status();
                            if (rows.isEmpty()) throw new PhysicalRangePage.LimitExceededException(required);
                            break;
                        }
                        rows.add(new PhysicalRangePage.Row(iterator.key(), iterator.value()));
                        bytes = (int)required; lastKeyBytes = keyBytes;
                        if (reverse) iterator.prev(); else iterator.next();
                    }
                    iterator.status();
                    return new PhysicalRangePage(rows, iterator.isValid());
                }
            } finally { releaseUse(); }
        }
        public void visitState(SnapshotVisitor visitor) throws RocksDBException, IOException {
            visit(state, markerKey, null, markerKey, visitor);
        }
        public void visitReceipts(SnapshotVisitor visitor) throws RocksDBException, IOException {
            visit(receipts, markerKey, null, markerKey, visitor);
        }
        private void visit(ColumnFamilyHandle cf, byte[] lower, byte[] upper, byte[] prefix, SnapshotVisitor visitor)
                throws RocksDBException, IOException {
            if (lower != null && upper != null && Arrays.compareUnsigned(lower, upper) >= 0) {
                throw new IllegalArgumentException("Invalid export interval");
            }
            acquireUse();
            try (var options = new ReadOptions().setSnapshot(snapshot).setFillCache(false);
                    var iterator = db.newIterator(cf, options)) {
                if (lower == null) iterator.seekToFirst(); else iterator.seek(lower);
                while (iterator.isValid()) {
                    byte[] key = iterator.key();
                    if (upper != null && Arrays.compareUnsigned(key, upper) >= 0) break;
                    if (prefix != null && (key.length < prefix.length
                            || !Arrays.equals(prefix, 0, prefix.length, key, 0, prefix.length))) break;
                    visitor.accept(key, iterator.value());
                    iterator.next();
                }
                iterator.status();
            } finally {
                releaseUse();
            }
        }
        private synchronized void acquireUse() {
            if (closed) throw new IllegalStateException("Snapshot pin closed");
            nativeUsers++;
        }
        private void releaseUse() {
            boolean release;
            synchronized (this) { release = --nativeUsers == 0 && closed; }
            if (release) releaseNative();
        }
        @Override public void close() {
            boolean release;
            synchronized (this) {
                if (closed) return;
                closed = true;
                release = nativeUsers == 0;
            }
            if (release) releaseNative();
        }
        private void releaseNative() {
            try { db.releaseSnapshot(snapshot); }
            finally { snapshotPins.decrementAndGet(); }
        }
    }

    @FunctionalInterface
    public interface SnapshotVisitor {
        void accept(byte[] key, byte[] value) throws IOException;
    }

    private void requireNext(ApplyPosition next) {
        long expected = applied == null ? 0 : Math.addExact(applied.index(), 1);
        if (next.index() != expected || (applied != null && next.term() < applied.term())) {
            throw new IllegalStateException("Noncontiguous or regressing committed apply: " + next);
        }
    }

    private byte[] receiptKey(OperationId id) {
        return ByteBuffer.allocate(48).put(uuidBytes(groupId)).put(uuidBytes(id.sessionId()))
                .putLong(id.epoch()).putLong(id.sequence()).array();
    }
    private static byte[] uuidBytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
    }
    private static byte[] encodePosition(ApplyPosition position) {
        return ByteBuffer.allocate(20).putInt(STATE_VERSION).putLong(position.term()).putLong(position.index()).array();
    }
    private static byte[] encodeConfiguration(Configuration configuration) {
        return ByteBuffer.allocate(20 + configuration.data.length).putInt(STATE_VERSION)
                .putLong(configuration.term()).putLong(configuration.index()).put(configuration.data).array();
    }
    private static Configuration decodeConfiguration(byte[] value) {
        if (value.length <= 20 || value.length > 20 + 65536) throw new IllegalStateException("Invalid cut configuration bytes");
        var input = ByteBuffer.wrap(value);
        if (input.getInt() != STATE_VERSION) throw new IllegalStateException("Unsupported configuration version");
        long term = input.getLong();
        long index = input.getLong();
        byte[] data = new byte[input.remaining()];
        input.get(data);
        return new Configuration(term, index, data);
    }
    private static ApplyPosition decodePosition(byte[] value) {
        if (value.length != 20) throw new IllegalStateException("Invalid applied marker");
        var buffer = ByteBuffer.wrap(value);
        if (buffer.getInt() != STATE_VERSION) throw new IllegalStateException("Unsupported applied marker version");
        return new ApplyPosition(buffer.getLong(), buffer.getLong());
    }
    private static byte[] encodeReceipt(byte[] digest, byte[] result) {
        return ByteBuffer.allocate(36 + result.length).putInt(STATE_VERSION).put(digest).put(result).array();
    }
    private static byte[] decodeReceipt(byte[] value, byte[] digest) {
        if (digest.length != 32 || value.length < 36 || value.length > 36 + MAX_RESULT_BYTES) {
            throw new IllegalStateException("Invalid durable receipt");
        }
        var buffer = ByteBuffer.wrap(value);
        if (buffer.getInt() != STATE_VERSION) throw new IllegalStateException("Unsupported receipt version");
        byte[] originalHash = new byte[32];
        buffer.get(originalHash);
        if (!Arrays.equals(originalHash, digest)) throw new IllegalArgumentException("Operation identity reused with different payload");
        return Arrays.copyOfRange(value, 36, value.length);
    }
}
