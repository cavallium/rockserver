package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.PreparedParticipant;
import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import it.cavallium.rockserver.core.common.distributed.DistributedBucket;
import it.cavallium.rockserver.core.common.distributed.KeyPredicate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import it.cavallium.rockserver.core.common.distributed.RangeMutationHistory;
import it.cavallium.rockserver.core.common.distributed.RangeSessionControl;
import it.cavallium.rockserver.core.common.distributed.SessionAuthority;
import it.cavallium.rockserver.core.common.distributed.SessionEpochLedger;
import it.cavallium.rockserver.core.common.distributed.RangeStateView;
import it.cavallium.rockserver.core.common.distributed.ReceiptUsage;
import it.cavallium.rockserver.core.common.distributed.ReceiptRetentionEstimate;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
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
    /** Consensus bound for original bucket bytes plus retained staged bucket replacements. */
    public static final long MAX_BUCKET_PLAN_BYTES=16L*1024*1024;
    public static final int MAX_PREPARED_PARTICIPANTS=64;
    public static final long MAX_PREPARED_BYTES=16L*1024*1024;
    private record PreparedIndex(List<PreparedParticipant> records,long bytes,long scopeBytes) {
        static PreparedIndex empty() { return new PreparedIndex(List.of(),0,0); }
    }
    private volatile PreparedIndex preparedIndex=PreparedIndex.empty();
    private volatile DistributedSchema logicalSchema;
    private RangeMutationHistory scopedHistory;
    private RangeMutationHistory.Limits scopedLimits;
    private final Map<RangeMutationHistory.Pin,ScopedHistoryPin> scopedPins=new IdentityHashMap<>();
    private final ConcurrentLinkedQueue<ScopedSnapshot> scopedReleases=new ConcurrentLinkedQueue<>();
    private volatile Object scopedGeneration=new Object();
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
    private final byte[] usageKey;
    private final byte[] usageBucketsPrefix;
    private final RangeOwnership ownership;
    private final RangeOwnership.Owner owner;
    private final RangeOwnership.Interval interval;
    private final ReentrantLock applyLane = new ReentrantLock();
    private final ReentrantReadWriteLock nativeLifecycle = new ReentrantReadWriteLock();
    private volatile boolean closing;
    private volatile ApplyPosition applied;
    private final AtomicInteger snapshotPins = new AtomicInteger();
    private final AtomicInteger trackedSnapshotPins=new AtomicInteger();
    private final AtomicLong trackedSnapshotScopeBytes=new AtomicLong();
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
        usageKey = ByteBuffer.allocate(17).put(markerKey).put((byte) 0x10).array();
        usageBucketsPrefix = ByteBuffer.allocate(17).put(markerKey).put((byte) 0x11).array();
        byte[] recovered = this.db.get(state, markerKey);
        applied = recovered == null ? null : decodePosition(recovered);
        pendingInstallation = this.db.get(state, installingKey);
        if (pendingInstallation != null) {
            if (pendingInstallation.length != 52 || ByteBuffer.wrap(pendingInstallation).getInt() != STATE_VERSION) {
                throw new IllegalStateException("Invalid pending installation marker");
            }
            if (ownership.get(owner).state() == RangeOwnership.State.ACTIVE) ownership.fence(owner);
        } else {
            ensureReceiptUsage();
            validateReceiptUsage(applied);
            preparedIndex=loadPreparedIndex();
        }
    }

    /** Null means no log entry has yet completed. This is not a durable snapshot watermark. */
    public ApplyPosition applied() { return applied; }
    public int activeSnapshotPins() { return snapshotPins.get(); }
    public int activeTrackedSnapshotPins() { return trackedSnapshotPins.get(); }
    public long trackedSnapshotScopeBytes() { return trackedSnapshotScopeBytes.get(); }
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
            invalidateScopedUnderLane(RangeMutationHistory.Status.INVALIDATED);
        } finally { applyLane.unlock(); drainScopedReleases(); }
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
            preparedIndex=loadPreparedIndex();
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

    private volatile boolean sessionControlsEnabled;

    /** Bootstrap must install catalogue proposal authority before enabling new controls. Persisted data enforcement is unconditional. */
    public void enableSessionControls() {
        enterNative(); applyLane.lock();
        try { requireActive(); sessionControlsEnabled=true; }
        finally { applyLane.unlock(); leaveNative(); }
    }

    /** Group-local ephemeral limits, configured once; ordinary read/export pins remain independent. */
    public void enableScopedHistory(RangeMutationHistory.Limits limits) {
        Objects.requireNonNull(limits); enterNative(); applyLane.lock();
        try {
            requireActive();
            if (scopedHistory!=null) {
                if (!scopedLimits.equals(limits)) throw new IllegalArgumentException("Scoped history limits already configured"); return;
            }
            scopedHistory=new RangeMutationHistory(limits); scopedLimits=limits;
            if (applied!=null && applied.term()>0) scopedHistory.advance(applied,List.of());
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public RangeMutationHistory.Stats scopedHistoryStats() {
        applyLane.lock(); try { if (scopedHistory==null) throw new UnsupportedOperationException("Scoped history is not enabled"); return scopedHistory.stats(); }
        finally { applyLane.unlock(); }
    }
    /** Leader-loss seam: invalidates writable-history observations without fencing follower apply. */
    public void invalidateScopedSnapshots() {
        applyLane.lock(); try { invalidateScopedUnderLane(RangeMutationHistory.Status.INVALIDATED); }
        finally { applyLane.unlock(); drainScopedReleases(); }
    }
    private void invalidateScopedUnderLane(RangeMutationHistory.Status reason) {
        if (scopedHistory==null) return;
        scopedGeneration=new Object(); scopedHistory.invalidateAll(reason);
        for (var pin:scopedPins.values()) scopedReleases.add(pin.snapshot); scopedPins.clear();
    }
    private void resetScopedUnderLane() {
        if (scopedHistory==null) return;
        scopedGeneration=new Object(); scopedHistory.reset();
        for (var pin:scopedPins.values()) scopedReleases.add(pin.snapshot); scopedPins.clear();
    }
    private void retireScoped(List<RangeMutationHistory.Pin> pins) {
        for (var pin:pins) { var owned=scopedPins.remove(pin); if (owned!=null) scopedReleases.add(owned.snapshot); }
    }
    private void retireScoped(RangeMutationHistory.Pin pin) {
        var owned=scopedPins.remove(pin); if (owned!=null) scopedReleases.add(owned.snapshot);
    }
    private void drainScopedReleases() { ScopedSnapshot pin; while ((pin=scopedReleases.poll())!=null) pin.close(); }
    private RangeMutationHistory.PreparedWrites prepareScopedWrites(boolean business,List<Mutation> effects,Map<BucketKey,DistributedBucket> buckets) {
        if (scopedHistory==null) return null;
        if (!business || scopedPins.isEmpty()) return scopedHistory.prepare(List.of());
        var spans=new ArrayList<TransactionScope.Span>(effects.size()+buckets.size());
        for (var mutation:effects) spans.add(mutation.kind()==Mutation.Kind.DELETE_RANGE
                ? TransactionScope.Span.range(mutation.columnId(),mutation.key(),mutation.value()) : TransactionScope.Span.point(mutation.columnId(),mutation.key()));
        for (var key:buckets.keySet()) spans.add(TransactionScope.Span.point(key.column,key.key));
        return scopedHistory.prepare(spans);
    }
    private void publishScoped(ApplyPosition position,RangeMutationHistory.PreparedWrites writes,boolean configurationChanged) {
        if (scopedHistory==null) return;
        try {
            if (configurationChanged || closing) invalidateScopedUnderLane(RangeMutationHistory.Status.INVALIDATED);
            retireScoped(scopedHistory.advance(position,writes));
        } catch (RuntimeException failure) {
            // Native data is already committed: no old handle may certify against a partial publication.
            resetScopedUnderLane();
        }
    }
    /** Install exact immutable decoding material before accepting/replaying bucket operations. This grants no schema activation authority. */
    public void enableLogicalSchema(DistributedSchema schema) {
        Objects.requireNonNull(schema); enterNative(); applyLane.lock();
        try {
            var current=ownership.get(owner).state();
            if (current!=RangeOwnership.State.ACTIVE && current!=RangeOwnership.State.FENCED) throw new IllegalStateException("Range cannot install decoding schema");
            int prefix=schema.routingPrefixBytes();
            if (!Arrays.equals(schemaDigest,schema.digest()) || !interval.domainId().equals(schema.domainId()) || !interval.columns().equals(schema.columnIds())
                    || logicalSchema!=null && !Arrays.equals(logicalSchema.digest(),schema.digest())
                    || prefix>0 && (interval.lowerInclusive()!=null && interval.lowerInclusive().length!=prefix
                            || interval.upperExclusive()!=null && interval.upperExclusive().length!=prefix)) throw new IllegalArgumentException("Logical schema differs from native ownership");
            logicalSchema=schema;
            for (var prepared:preparedIndex.records()) requirePreparation(prepared.preparation(),true);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    private void validateLogical(DistributedCommand command) {
        var schema=logicalSchema; if (schema==null) throw new UnsupportedOperationException("Bucket decoding schema is not installed");
        for (var mutation:command.mutations()) {
            if (mutation.kind()==Mutation.Kind.DELETE_RANGE) continue; // Bounds address whole physical rows, not member keys or stored values.
            var shape=schema.column(mutation.columnId()).schema(); boolean bucket=shape.variableLengthKeysCount()!=0;
            if (bucket!=mutation.bucket()) throw new IllegalArgumentException("Bucket mutation differs from qualified column kind");
            if (bucket) schema.requireBucketKey(mutation.columnId(),mutation.key(),mutation.memberKeys());
            else schema.requireFixedKey(mutation.columnId(),mutation.key());
            if (!shape.hasValue() && mutation.value()!=null && mutation.value().length!=0) throw new IllegalArgumentException("Key-only mutation has value bytes");
        }
        for (var predicate:command.predicates()) {
            var shape=schema.column(predicate.columnId()).schema(); boolean bucket=shape.variableLengthKeysCount()!=0;
            if (bucket!=predicate.bucket()) throw new IllegalArgumentException("Bucket predicate differs from qualified column kind");
            if (bucket) schema.requireBucketKey(predicate.columnId(),predicate.key(),predicate.memberKeys());
            else schema.requireFixedKey(predicate.columnId(),predicate.key());
            if (!shape.hasValue() && predicate.expectedValue()!=null && predicate.expectedValue().length!=0) throw new IllegalArgumentException("Key-only predicate has value bytes");
        }
    }
    public void validate(DistributedCommand command) { validateCommand(command,true); }
    private void validateCommand(DistributedCommand command,boolean active) { validateCommand(command,active,true); }
    private void validateCommand(DistributedCommand command,boolean active,boolean logical) {
        if (command.sessionControl()!=null && !sessionControlsEnabled) throw new UnsupportedOperationException("Replicated session binding is not activated");
        if (closing) throw new IllegalStateException("Range admission is closed");
        if (active) requireActive();
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
        if (logical && (command.hasBucketData() || command.acceptance()!=null && logicalSchema!=null)) validateLogical(command);
    }

    private byte[] preparedKey(UUID transaction) { return ByteBuffer.allocate(33).put(markerKey).put((byte)0x20).putLong(transaction.getMostSignificantBits()).putLong(transaction.getLeastSignificantBits()).array(); }
    private void requirePreparation(PreparedParticipant.Preparation preparation,boolean material) {
        var business=preparation.business(); validateCommand(business,false,material || logicalSchema!=null); validateSnapshotScope(preparation.scope());
        if (!preparation.registration().domainId().equals(interval.domainId())) throw new IllegalArgumentException("Prepared domain differs from range");
        if (logicalSchema!=null && (!logicalSchema.clusterId().equals(preparation.registration().clusterId()) || !logicalSchema.databaseId().equals(preparation.registration().databaseId()))) throw new IllegalArgumentException("Prepared database differs from schema");
    }
    private PreparedIndex addPrepared(PreparedIndex index,PreparedParticipant value) {
        var preparation=value.preparation();
        for (var old:index.records()) {
            if (old.preparation().transactionId().equals(preparation.transactionId()) || old.preparation().sharesOperation(preparation)) throw new IllegalArgumentException("Prepared transaction or operation identity reused");
            if (old.preparation().scope().intersects(preparation.scope())) throw new IllegalStateException("Transaction overlaps durable prepared scope");
        }
        long bytes=index.bytes()+33L+value.encode().length, scope=index.scopeBytes()+RangeMutationHistory.accountedScopeBytes(preparation.scope());
        if (index.records().size()>=MAX_PREPARED_PARTICIPANTS || bytes>MAX_PREPARED_BYTES || scope>MAX_PREPARED_BYTES) throw new IllegalStateException("Durable prepared capacity exhausted");
        var records=new ArrayList<>(index.records()); records.add(value); return new PreparedIndex(List.copyOf(records),bytes,scope);
    }
    private PreparedIndex loadPreparedIndex() {
        var index=PreparedIndex.empty();
        try (var low=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x20).array());
             var high=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x21).array());
             var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high);
             var iterator=db.newIterator(state,options)) {
            iterator.seekToFirst(); var sizing=new byte[0];
            while (iterator.isValid()) {
                if (iterator.key(sizing)!=33 || iterator.value(sizing)>16+PreparedParticipant.Preparation.MAX_BYTES) throw new IllegalStateException("Prepared record exceeds native recovery bounds");
                byte[] key=iterator.key(); var value=PreparedParticipant.decode(iterator.value()); requirePreparation(value.preparation(),false);
                if (!Arrays.equals(key,preparedKey(value.preparation().transactionId())) || applied==null || value.position().index()>applied.index() || value.position().term()>applied.term()) throw new IllegalStateException("Prepared record differs from completed cut");
                index=addPrepared(index,value); iterator.next();
            }
            iterator.status(); return index;
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot recover durable prepared scopes",failure); }
    }
    private void requireUnlocked(TransactionScope scope) {
        for (var record:preparedIndex.records()) if (record.preparation().scope().intersects(scope)) throw new IllegalStateException("Access overlaps durable prepared transaction");
    }
    private void requireUnlocked(DistributedCommand command) {
        for (var record:preparedIndex.records()) if (record.preparation().ownsOperation(command.operationId())) throw new IllegalArgumentException("Operation identity is owned by durable preparation");
        if (command.acceptance()!=null) { requireUnlocked(command.acceptance().scope()); return; }
        var spans=new ArrayList<TransactionScope.Span>();
        for (var m:command.mutations()) spans.add(m.kind()==Mutation.Kind.DELETE_RANGE ? TransactionScope.Span.range(m.columnId(),m.key(),m.value()) : TransactionScope.Span.point(m.columnId(),m.key()));
        for (var p:command.predicates()) spans.add(TransactionScope.Span.point(p.columnId(),p.key()));
        if (!spans.isEmpty()) requireUnlocked(new TransactionScope(spans));
    }
    public record PreparedCapacity(ApplyPosition position,int count,long bytes,long scopeBytes) {}
    public record PreparationEstimate(PreparedCapacity capacity,PreparedParticipant existing) {}
    public PreparedCapacity preparedCapacity() {
        enterNative(); applyLane.lock();
        try { requireActive(); return new PreparedCapacity(applied,preparedIndex.records().size(),preparedIndex.bytes(),preparedIndex.scopeBytes()); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    /** Native checks and quota observation from one completed cut, before entering the adapter admission monitor. */
    public PreparationEstimate estimatePreparation(PreparedParticipant.Preparation preparation) throws RocksDBException {
        enterNative(); applyLane.lock();
        try {
            requireActive(); requirePreparation(preparation,true);
            var control=preparation.operation(); long minimumControl=minimumSessionEpoch(null); long controlFloor=control.epoch()<minimumControl ? -1 : acknowledgedThrough(control,null);
            var business=preparation.business();
            var controlCommand=new DistributedCommand(business.groupId(),business.rangeEpoch(),business.schemaDigest(),control,business.mutations(),business.predicates());
            if (control.epoch()<minimumControl || control.sequence()<=controlFloor || sessionDecision(controlCommand,controlFloor,sessionAuthority())!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Prepare control session is fenced; query its durable state");
            for (var record:preparedIndex.records()) if (record.preparation().transactionId().equals(preparation.transactionId())) {
                if (!record.preparation().equals(preparation)) throw new IllegalArgumentException("Prepared transaction payload changed");
                return new PreparationEstimate(preparedCapacity(),record);
            }
            var id=business.operationId(); long minimum=minimumSessionEpoch(null); long floor=id.epoch()<minimum ? -1 : acknowledgedThrough(id,null);
            if (id.epoch()<minimum || id.sequence()<=floor || db.get(receipts,receiptKey(id))!=null || db.get(receipts,receiptKey(preparation.operation()))!=null
                    || sessionDecision(business,floor,sessionAuthority())!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Prepared identity is no longer eligible");
            if (applied==null || applied.index()==Long.MAX_VALUE || business.acceptance().snapshot().index()==Long.MAX_VALUE) throw new IllegalStateException("Participant completed prefix unavailable/exhausted");
            addPrepared(preparedIndex,new PreparedParticipant(preparation,new ApplyPosition(business.acceptance().snapshot().term(),Math.max(applied.index()+1,business.acceptance().snapshot().index()+1))));
            return new PreparationEstimate(preparedCapacity(),null);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Exact native recovery lookup is permitted while fenced; it grants no user admission authority. */
    public PreparedParticipant recoveredParticipant(PreparedParticipant.Preparation expected) throws RocksDBException {
        enterNative(); applyLane.lock();
        try {
            byte[] bytes=db.get(state,preparedKey(expected.transactionId())); if (bytes==null) return null;
            var record=PreparedParticipant.decode(bytes); requirePreparation(record.preparation(),false);
            if (!record.preparation().equals(expected) || applied==null || record.position().index()>applied.index() || record.position().term()>applied.term()) throw new IllegalArgumentException("Recovered prepared receipt differs");
            return record;
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Only committed accepted preparations may enter. The adapter must retain its complete proposal reservation through this return. */
    public PreparedParticipant applyPrepared(ApplyPosition position,PreparedParticipant.Preparation preparation) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(preparation); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); requirePreparation(preparation,true);
            PreparedParticipant saved=null;
            for (var old:preparedIndex.records()) if (old.preparation().transactionId().equals(preparation.transactionId())) {
                if (!Arrays.equals(old.preparation().digest(),preparation.digest())) throw new IllegalArgumentException("Prepared transaction payload changed"); saved=old;
            }
            if (applied!=null && position.index()<=applied.index()) {
                if (saved==null) throw new IllegalStateException("Completed prepare has no durable receipt"); return saved;
            }
            requireNext(position);
            if (saved==null) {
                var business=preparation.business(); var id=business.operationId(); long minimum=minimumSessionEpoch(null); long floor=id.epoch()<minimum ? -1 : acknowledgedThrough(id,null);
                if (id.epoch()<minimum || id.sequence()<=floor || db.get(receipts,receiptKey(id))!=null || db.get(receipts,receiptKey(preparation.operation()))!=null
                        || sessionDecision(business,floor,sessionAuthority())!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Prepared business/control identity is no longer eligible");
            }
            var value=saved==null ? new PreparedParticipant(preparation,position) : saved;
            var next=saved==null ? addPrepared(preparedIndex,value) : preparedIndex;
            if (saved==null && !Arrays.equals(evaluate(preparation.business()).result().encode(),preparation.business().acceptance().result())) throw new IllegalStateException("Prepared result disagrees with ordered native state");
            // Canonical acceptance was certified before append; replicas do not consult volatile history or expose effects.
            var history=prepareScopedWrites(false,List.of(),Map.of());
            try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if (saved==null) batch.put(state,preparedKey(preparation.transactionId()),value.encode());
                batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            preparedIndex=next; publishScoped(position,history,false); applied=position; return value;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    /** Barrier/query ownership is supplied by the adapter; this record survives prepare-receipt/session GC. */
    public PreparedParticipant preparedParticipant(UUID transaction) {
        enterNative(); applyLane.lock();
        try { requireActive(); return preparedIndex.records().stream().filter(p -> p.preparation().transactionId().equals(transaction)).findFirst().orElse(null); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    public int preparedParticipantCount() { return preparedIndex.records().size(); }
    public long preparedParticipantBytes() { return preparedIndex.bytes(); }
    /** Fresh user reads must reject unresolved prepared scopes; administrative capture remains available for export/recovery. */
    public ScopedSnapshot captureReadable(TransactionScope scope) throws RocksDBException {
        Objects.requireNonNull(scope); enterNative(); applyLane.lock();
        try { requireActive(); validateSnapshotScope(scope); requireUnlocked(scope); return new ScopedSnapshot(scope,captureUnderAdmission()); }
        finally { applyLane.unlock(); leaveNative(); }
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
        finally { try { drainScopedReleases(); } finally { leaveNative(); } }
    }

    private byte[] applyUnderAdmission(ApplyPosition position, DistributedCommand command, byte[] result)
            throws RocksDBException {
        Objects.requireNonNull(position);
        validate(command);
        if (result!=null && (command.sessionControl()!=null || command.hasBucketData() || command.acceptance()!=null || command.transactionRejection())) throw new IllegalArgumentException("Controls, buckets and transaction plans require ordered native evaluation");
        if (result != null && !command.predicates().isEmpty()) {
            throw new IllegalArgumentException("Precomputed results require a fully evaluated plan without live predicates");
        }
        if (result != null && result.length > MAX_RESULT_BYTES) throw new IllegalArgumentException("Result exceeds receipt budget");
        byte[] stableResult = result == null ? null : result.clone();
        byte[] digest = command.receiptDigest();
        byte[] receiptKey = receiptKey(command.operationId());
        applyLane.lock();
        try {
            requireActive();
            ensureReceiptUsage();
            long currentEpochFloor = minimumSessionEpoch(null);
            long currentFloor = command.operationId().epoch() < currentEpochFloor ? -1 : acknowledgedThrough(command.operationId(), null);
            if (command.operationId().epoch() < currentEpochFloor || command.operationId().sequence() <= currentFloor) {
                if (applied == null || position.index() > applied.index()) {
                    requireNext(position);
                    var writes=prepareScopedWrites(false,List.of(),Map.of());
                    try (var batch = new WriteBatch(); var options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                        batch.put(state, markerKey, encodePosition(position));
                        batch.put(state, usageKey, encodeTotalUsage(readUsage(usageKey), position));
                        db.write(options, batch);
                    }
                    publishScoped(position,writes,false); applied = position;
                }
                return expiredResult();
            }
            byte[] saved = db.get(receipts, receiptKey);
            byte[] original = saved == null ? null : decodeReceipt(saved, digest);
            if (applied != null && position.index() <= applied.index()) {
                if (saved == null) throw new IllegalStateException("Applied command has no retained receipt");
                return original;
            }
            requireNext(position);
            if (saved==null) requireUnlocked(command);
            if (saved==null && command.acceptance()!=null && (command.acceptance().snapshot().index()>=position.index()
                    || command.acceptance().snapshot().term()!=position.term())) throw new IllegalArgumentException("Accepted transaction does not follow its audited leadership cut");
            var authority=saved==null ? sessionAuthority() : null;
            var sessionStatus = saved==null ? sessionDecision(command,currentFloor,authority) : CommandResult.Status.SUCCESS;
            boolean terminalExpiry = sessionStatus==CommandResult.Status.SESSION_EXPIRED && expiredBoundOutcome(command,currentFloor,authority);
            boolean acceptMaintenance = sessionStatus==CommandResult.Status.SUCCESS || terminalExpiry;
            long effectiveMinimum = acceptMaintenance ? command.minimumSessionEpoch() : -1;
            long effectiveAcknowledgement = acceptMaintenance ? command.acknowledgedThrough() : -1;
            boolean installUserEffects = true;
            if (saved==null && sessionStatus!=CommandResult.Status.SUCCESS) {
                stableResult=new CommandResult(sessionStatus,List.of()).encode(); installUserEffects=false;
            }
            List<Mutation> effects=command.mutations(); Map<BucketKey,DistributedBucket> bucketEffects=Map.of();
            if (saved==null && stableResult==null && command.transactionRejection()) {
                stableResult=new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode(); installUserEffects=false;
            }
            if (saved == null && stableResult == null) {
                var evaluated = evaluate(command);
                stableResult = evaluated.result().encode(); effects=evaluated.effects(); bucketEffects=evaluated.buckets();
                if (command.acceptance()!=null && !Arrays.equals(stableResult,command.acceptance().result())) {
                    throw new IllegalStateException("Accepted transaction result disagrees with ordered native effects");
                }
                installUserEffects = evaluated.result().status() == CommandResult.Status.SUCCESS;
            }
            var writes=prepareScopedWrites(saved==null && installUserEffects,effects,bucketEffects);
            try (WriteBatch batch = new WriteBatch();
                    WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                if (saved == null) {
                    if (installUserEffects) for (var mutation : effects) {
                        var cf = columns.get(mutation.columnId());
                        if (cf == null) throw new IllegalArgumentException("Unknown logical column");
                        switch (mutation.kind()) {
                            case PUT -> batch.put(cf, mutation.key(), mutation.value());
                            case DELETE -> batch.delete(cf, mutation.key());
                            case MERGE -> batch.merge(cf, mutation.key(), mutation.value());
                            case DELETE_RANGE -> batch.deleteRange(cf, mutation.key(), mutation.value());
                            case BUCKET_PUT, BUCKET_DELETE -> throw new IllegalStateException("Bucket effect was not lowered into a physical batch");
                        }
                    }
                    if (installUserEffects) for (var entry:bucketEffects.entrySet()) batch.put(columns.get(entry.getKey().column),entry.getKey().key,entry.getValue().encode());
                    if (acceptMaintenance && command.sessionControl()!=null) installSessionControl(batch,command.sessionControl());
                    if (effectiveMinimum > currentEpochFloor) {
                        batch.put(state, epochFloorKey(), ByteBuffer.allocate(12).putInt(STATE_VERSION).putLong(effectiveMinimum).array());
                        batch.deleteRange(state, bindingPrefix(), bindingKey(effectiveMinimum));
                    }
                    if (effectiveAcknowledgement > currentFloor) {
                        long floor = effectiveAcknowledgement;
                        batch.put(state, sessionFloorKey(command.operationId()),
                                ByteBuffer.allocate(12).putInt(STATE_VERSION).putLong(floor).array());
                        batch.deleteRange(receipts, receiptKey(new OperationId(command.operationId().sessionId(), command.operationId().epoch(), 0)),
                                receiptKey(new OperationId(command.operationId().sessionId(), command.operationId().epoch(), floor + 1)));
                    }
                    byte[] encodedReceipt = encodeReceipt(digest, stableResult);
                    updateReceiptUsage(batch, command, effectiveMinimum, effectiveAcknowledgement, currentEpochFloor, currentFloor, encodedReceipt.length, position);
                    batch.put(receipts, receiptKey, encodedReceipt);
                }
                batch.put(state, markerKey, encodePosition(position));
                if (saved != null) batch.put(state, usageKey, encodeTotalUsage(readUsage(usageKey), position));
                db.write(options, batch);
            }
            publishScoped(position,writes,false); applied = position;
            return saved == null ? stableResult : original;
        } catch (RocksDBException | RuntimeException | Error failure) { resetScopedUnderLane(); throw failure; }
        finally { applyLane.unlock(); }
    }

    private byte[] authorityKey() { return ByteBuffer.allocate(17).put(markerKey).put((byte)0x13).array(); }
    private byte[] bindingPrefix() { return ByteBuffer.allocate(17).put(markerKey).put((byte)0x14).array(); }
    private byte[] bindingKey(long epoch) { return ByteBuffer.allocate(25).put(bindingPrefix()).putLong(epoch).array(); }
    private SessionAuthority sessionAuthority() throws RocksDBException { return sessionAuthority(null); }
    private SessionAuthority sessionAuthority(ReadOptions options) throws RocksDBException {
        byte[] bytes=options==null ? db.get(state,authorityKey()) : db.get(state,options,authorityKey());
        if (bytes==null) return null;
        var control=RangeSessionControl.decode(bytes);
        if (control.binding()) throw new IllegalStateException("Invalid persisted session authority");
        return control.authority();
    }
    private RangeSessionControl sessionBinding(long epoch) throws RocksDBException {
        byte[] bytes=db.get(state,bindingKey(epoch));
        if (bytes==null) return null;
        var control=RangeSessionControl.decode(bytes);
        if (!control.binding() || control.grant().epoch()!=epoch) throw new IllegalStateException("Invalid persisted session binding");
        return control;
    }
    private SessionAuthority effectiveAuthority(RangeSessionControl control,SessionAuthority current) {
        if (current==null) return control.authority();
        try { control.authority().requireAtLeast(current); return control.authority(); }
        catch (IllegalArgumentException older) {
            current.requireAtLeast(control.authority()); return current;
        }
    }
    private CommandResult.Status sessionDecision(DistributedCommand command,long acknowledged,SessionAuthority authority) throws RocksDBException {
        var control=command.sessionControl();
        if (control!=null) {
            final SessionAuthority effective;
            try { effective=effectiveAuthority(control,authority); }
            catch (IllegalArgumentException stale) { return CommandResult.Status.CONFLICT; }
            if (control.binding()) {
                if (!effective.permits(control.grant()) || control.grant().epoch()<minimumSessionEpoch(null)) return CommandResult.Status.SESSION_EXPIRED;
                var previous=sessionBinding(control.grant().epoch());
                if (previous!=null && (!previous.grant().equals(control.grant()) || previous.outstandingWindow()!=control.outstandingWindow())) return CommandResult.Status.CONFLICT;
                if (previous==null) {
                    int count=0;
                    byte[] prefix=bindingPrefix(), end=prefixEnd(prefix);
                    try (var lower=new org.rocksdb.Slice(prefix); var upper=new org.rocksdb.Slice(end);
                            var options=new ReadOptions().setFillCache(false).setIterateLowerBound(lower).setIterateUpperBound(upper);
                            var iterator=db.newIterator(state,options)) {
                        iterator.seek(bindingKey(Math.max(minimumSessionEpoch(null),control.authority().minimumEpoch())));
                        while (iterator.isValid()) {
                            if (++count>=SessionEpochLedger.MAX_GRANTS) return CommandResult.Status.CONFLICT;
                            iterator.next();
                        }
                        iterator.status();
                    }
                }
            }
            return CommandResult.Status.SUCCESS;
        }
        if (authority==null || command.minimumSessionEpoch()>=0) return CommandResult.Status.SUCCESS;
        var id=command.operationId(); var binding=sessionBinding(id.epoch());
        if (binding==null || !binding.grant().sessionId().equals(id.sessionId())) return CommandResult.Status.SESSION_EXPIRED;
        boolean acknowledgementOnly=!command.transactionRejection() && command.acknowledgedThrough()>=0 && command.mutations().isEmpty() && command.predicates().isEmpty();
        if (!acknowledgementOnly && !authority.permits(binding.grant())) return CommandResult.Status.SESSION_EXPIRED;
        return withinSessionWindow(command,acknowledged,binding.outstandingWindow()) ? CommandResult.Status.SUCCESS : CommandResult.Status.CONFLICT;
    }
    private static boolean withinSessionWindow(DistributedCommand command,long acknowledged,int window) {
        long floor=Math.max(acknowledged,command.acknowledgedThrough());
        return floor>Long.MAX_VALUE-window || command.operationId().sequence()<=floor+window;
    }
    // Only an installed immutable grant proves individual expiry. Unbound requests remain retryable until binding.
    private boolean expiredBoundOutcome(DistributedCommand command,long acknowledged,SessionAuthority authority) throws RocksDBException {
        if (authority==null || command.minimumSessionEpoch()>=0) return false;
        var id=command.operationId(); var binding=sessionBinding(id.epoch());
        return binding!=null && binding.grant().sessionId().equals(id.sessionId())
                && binding.grant().expiresAtMillis()<=authority.logicalTimeMillis()
                && withinSessionWindow(command,acknowledged,binding.outstandingWindow());
    }
    private void installSessionControl(WriteBatch batch,RangeSessionControl control) throws RocksDBException {
        var effective=effectiveAuthority(control,sessionAuthority());
        batch.put(state,authorityKey(),new RangeSessionControl(effective,null,0).encode());
        if (control.binding()) batch.put(state,bindingKey(control.grant().epoch()),control.encode());
    }

    private record Evaluation(CommandResult result,List<Mutation> effects,Map<BucketKey,DistributedBucket> buckets) {
        Evaluation(CommandResult result,List<Mutation> effects) { this(result,effects,Map.of()); }
    }
    private Evaluation evaluate(DistributedCommand command) throws RocksDBException {
        if (!command.hasBucketData()) return new Evaluation(evaluateRaw(command),command.mutations());
        try { return evaluateBuckets(command); }
        catch (DistributedBucket.LimitExceededException limit) { return new Evaluation(new CommandResult(CommandResult.Status.RESULT_TOO_LARGE,List.of()),List.of()); }
    }
    private static final class BucketKey {
        final UUID column; final byte[] key; final int hash;
        BucketKey(UUID column,byte[] key) { this.column=column; this.key=key.clone(); hash=31*column.hashCode()+Arrays.hashCode(key); }
        @Override public int hashCode() { return hash; }
        @Override public boolean equals(Object other) { return other instanceof BucketKey bucket && column.equals(bucket.column) && Arrays.equals(key,bucket.key); }
    }
    /** At most one entry per command point/range; no stored key is enumerated to represent a deletion. */
    private static final class RangeOverlay {
        private record Deletion(UUID column,byte[] lower,byte[] upper,int order) {}
        final boolean enabled;
        private final List<Deletion> ranges=new ArrayList<>();
        private final Map<UUID,java.util.NavigableMap<BucketKey,Integer>> points=new HashMap<>();
        RangeOverlay(DistributedCommand command) { enabled=command.mutations().stream().anyMatch(m -> m.kind()==Mutation.Kind.DELETE_RANGE); }
        void delete(Mutation mutation,int order) { ranges.add(new Deletion(mutation.columnId(),mutation.key(),mutation.value(),order)); }
        void point(UUID column,byte[] key,int order) {
            if (enabled) points.computeIfAbsent(column,ignored -> new java.util.TreeMap<>((a,b) -> Arrays.compareUnsigned(a.key,b.key))).put(new BucketKey(column,key),order);
        }
        Deletion covering(UUID column,byte[] key) {
            for (int i=ranges.size()-1;i>=0;i--) {
                var range=ranges.get(i);
                if (range.column().equals(column) && Arrays.compareUnsigned(key,range.lower())>=0 && Arrays.compareUnsigned(key,range.upper())<0) return range;
            }
            return null;
        }
        boolean deleted(UUID column,byte[] key) {
            var range=covering(column,key); if (range==null) return false;
            var writes=points.get(column);
            return range.order()>(writes==null ? -1 : writes.getOrDefault(new BucketKey(column,key),-1));
        }
        void hideDeleted(WriteBatchWithIndex batch,Map<UUID,ColumnFamilyHandle> columns) throws RocksDBException {
            for (var column:points.entrySet()) for (var point:column.getValue().keySet()) {
                if (deleted(column.getKey(),point.key)) batch.delete(columns.get(column.getKey()),point.key);
            }
        }
        /** Seek past an erased interval, stopping at a later staged point inside it if necessary. */
        void skipDeleted(org.rocksdb.RocksIterator iterator,UUID column,boolean reverse,byte[] lower,byte[] upper) {
            while (enabled && withinPageBounds(iterator,lower,upper)) {
                byte[] key=iterator.key(); var range=covering(column,key);
                if (range==null || !deleted(column,key)) return;
                var writes=points.get(column); byte[] target=reverse ? range.lower() : range.upper(); boolean resurrection=false;
                if (writes!=null) {
                    var point=reverse ? writes.lowerEntry(new BucketKey(column,key)) : writes.higherEntry(new BucketKey(column,key));
                    while (point!=null && Arrays.compareUnsigned(point.getKey().key,range.lower())>=0 && Arrays.compareUnsigned(point.getKey().key,range.upper())<0) {
                        if (!deleted(column,point.getKey().key)) { target=point.getKey().key; resurrection=true; break; }
                        point=reverse ? writes.lowerEntry(point.getKey()) : writes.higherEntry(point.getKey());
                    }
                }
                if (reverse) {
                    iterator.seekForPrev(target);
                    if (!resurrection && iterator.isValid() && Arrays.equals(iterator.key(),target)) iterator.prev();
                } else iterator.seek(target);
            }
        }
    }
    private static final class BucketBudget {
        long bytes;
        void replace(long previous,long next) {
            long total=bytes-previous+next;
            if (total>MAX_BUCKET_PLAN_BYTES) throw new DistributedBucket.LimitExceededException("Bucket apply plan exceeds protocol byte budget"); bytes=total;
        }
    }
    private DistributedBucket originalBucket(BucketKey key,Map<BucketKey,DistributedBucket> originals,BucketBudget budget,ReadOptions options) throws RocksDBException {
        var bucket=originals.get(key); if (bucket!=null) return bucket;
        byte[] raw=db.get(columns.get(key.column),options,key.key); budget.replace(0,raw==null ? 0 : raw.length);
        bucket=logicalSchema.decodeBucket(key.column,key.key,raw);
        originals.put(key,bucket); return bucket;
    }
    private Evaluation evaluateBuckets(DistributedCommand command) throws RocksDBException {
        try (var overlay=new WriteBatchWithIndex(true); var options=new ReadOptions()) {
            return evaluatePoints(command,overlay,options);
        }
    }
    private Evaluation evaluatePoints(DistributedCommand command,WriteBatchWithIndex overlay,ReadOptions options) throws RocksDBException {
        return evaluatePoints(command,overlay,options,new RangeOverlay(command));
    }
    private Evaluation evaluatePoints(DistributedCommand command,WriteBatchWithIndex overlay,ReadOptions options,RangeOverlay visibility) throws RocksDBException {
        var budget=new BucketBudget(); var originals=new HashMap<BucketKey,DistributedBucket>();
        if (command.acceptance()==null) for (var predicate:command.predicates()) {
            byte[] current=predicate.bucket() ? originalBucket(new BucketKey(predicate.columnId(),predicate.key()),originals,budget,options).get(predicate.memberKeys())
                    : db.get(columns.get(predicate.columnId()),options,predicate.key());
            if (!Arrays.equals(current,predicate.expectedValue())) return new Evaluation(new CommandResult(CommandResult.Status.CONFLICT,List.of()),List.of());
        }
        var staged=new LinkedHashMap<BucketKey,DistributedBucket>(); var effects=new ArrayList<Mutation>(); var values=new ArrayList<byte[]>(); long resultBytes=8;
        int order=0;
        for (var mutation:command.mutations()) {
            if (mutation.kind()==Mutation.Kind.DELETE_RANGE) { visibility.delete(mutation,order++); effects.add(mutation); continue; }
            byte[] value=null;
            if (mutation.bucket()) {
                var key=new BucketKey(mutation.columnId(),mutation.key()); var prior=staged.get(key);
                var bucket=visibility.deleted(mutation.columnId(),mutation.key()) ? logicalSchema.decodeBucket(mutation.columnId(),mutation.key(),null)
                        : prior==null ? originalBucket(key,originals,budget,options) : prior;
                if (mutation.returnMode()==Mutation.ReturnMode.PREVIOUS) value=bucket.get(mutation.memberKeys());
                var next=mutation.kind()==Mutation.Kind.BUCKET_PUT ? bucket.put(mutation.memberKeys(),mutation.value()) : bucket.remove(mutation.memberKeys());
                budget.replace(prior==null ? 0 : prior.encodedSize(),next.encodedSize()); staged.put(key,next);
                if (mutation.returnMode()==Mutation.ReturnMode.CURRENT) value=next.get(mutation.memberKeys());
            } else {
                var cf=columns.get(mutation.columnId()); byte[] key=mutation.key();
                if (visibility.deleted(mutation.columnId(),key)) overlay.delete(cf,key);
                if (mutation.returnMode()==Mutation.ReturnMode.PREVIOUS) value=overlay.getFromBatchAndDB(db,cf,options,key);
                switch (mutation.kind()) {
                    case PUT -> overlay.put(cf,key,mutation.value()); case DELETE -> overlay.delete(cf,key); case MERGE -> overlay.merge(cf,key,mutation.value());
                    default -> throw new IllegalStateException("Non-point effect escaped bucket validation");
                }
                effects.add(mutation);
                if (mutation.returnMode()==Mutation.ReturnMode.CURRENT) value=overlay.getFromBatchAndDB(db,cf,options,key);
            }
            visibility.point(mutation.columnId(),mutation.key(),order++);
            if (mutation.returnMode()!=Mutation.ReturnMode.NONE) {
                resultBytes+=4L+(value==null ? 0 : value.length);
                if (resultBytes>MAX_RESULT_BYTES) return new Evaluation(new CommandResult(CommandResult.Status.RESULT_TOO_LARGE,List.of()),List.of());
                values.add(value);
            }
        }
        visibility.hideDeleted(overlay,columns);
        staged.entrySet().removeIf(entry -> visibility.deleted(entry.getKey().column,entry.getKey().key));
        return new Evaluation(new CommandResult(CommandResult.Status.SUCCESS,values),List.copyOf(effects),staged);
    }
    private CommandResult evaluateRaw(DistributedCommand command) throws RocksDBException {
        if (command.mutations().stream().allMatch(m -> m.returnMode()==Mutation.ReturnMode.NONE)) {
            if (command.acceptance()==null) for (var predicate:command.predicates()) {
                if (!Arrays.equals(db.get(columns.get(predicate.columnId()),predicate.key()),predicate.expectedValue())) {
                    return new CommandResult(CommandResult.Status.CONFLICT,List.of());
                }
            }
            return new CommandResult(CommandResult.Status.SUCCESS,List.of());
        }
        try (var overlay=new WriteBatchWithIndex(true); var options=new ReadOptions()) {
            return evaluatePoints(command,overlay,options).result();
        }
    }

    /** Every committed configuration/no-op entry must be accounted for in log order. */
    public void applyNoop(ApplyPosition position) throws RocksDBException {
        applyNoop(position, null);
    }

    public void applyNoop(ApplyPosition position, byte[] configuration) throws RocksDBException {
        enterNative();
        try { applyNoopUnderAdmission(position, configuration); }
        finally { try { drainScopedReleases(); } finally { leaveNative(); } }
    }

    private void applyNoopUnderAdmission(ApplyPosition position, byte[] configuration) throws RocksDBException {
        applyLane.lock();
        try {
            var current = ownership.get(owner).state();
            if (pendingInstallation != null || (current != RangeOwnership.State.ACTIVE && current != RangeOwnership.State.FENCED)) {
                throw new IllegalStateException("Range cannot complete Raft accounting");
            }
            ensureReceiptUsage();
            if (applied != null && position.index() <= applied.index()) return;
            requireNext(position);
            var writes=prepareScopedWrites(false,List.of(),Map.of());
            try (WriteBatch batch = new WriteBatch();
                    WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                batch.put(state, markerKey, encodePosition(position));
                batch.put(state, usageKey, encodeTotalUsage(readUsage(usageKey), position));
                if (configuration != null) {
                    batch.put(state, configurationKey, encodeConfiguration(new Configuration(position.term(), position.index(), configuration)));
                }
                db.write(options, batch);
            }
            publishScoped(position,writes,configuration!=null); applied = position;
        } catch (RocksDBException | RuntimeException | Error failure) { resetScopedUnderLane(); throw failure; }
        finally { applyLane.unlock(); }
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
        finally { try { drainScopedReleases(); } finally { leaveNative(); } }
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
                resetScopedUnderLane(); installationGeneration++;
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
                    // The current image supplies counters bound to its completed position; never normalize imported state.
                    validateReceiptUsage(expected.position());
                    // Flush every prior CF mutation before publishing the final applied marker.
                    db.flushWal(true);
                    try (var batch = new WriteBatch(); var options = new WriteOptions().setDisableWAL(false).setSync(true)) {
                        batch.put(state, markerKey, encodePosition(expected.position()));
                        batch.delete(state, installingKey);
                        db.write(options, batch);
                    }
                    publishScoped(expected.position(),prepareScopedWrites(false,List.of(),Map.of()),false); applied = expected.position();
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
        if (payloadDigest.length != 32) throw new IllegalArgumentException("Invalid payload digest");
        applyLane.lock();
        try {
            if (pendingInstallation != null) throw new IllegalStateException("Range metadata is being installed");
            if (id.epoch() < minimumSessionEpoch(null) || id.sequence() <= acknowledgedThrough(id, null)) return expiredResult();
            byte[] saved = db.get(receipts, receiptKey(id));
            return saved == null ? null : decodeReceipt(saved, payloadDigest);
        } finally { applyLane.unlock(); }
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
            validateSnapshotScope(scope);
            return new ScopedSnapshot(scope, captureUnderAdmission());
        } finally { leaveNative(); }
    }

    /** Memory-only admission state; no native reads or snapshot acquisition. */
    public boolean admissionOpen() { return !closing && pendingInstallation==null && ownership.get(owner).state()==RangeOwnership.State.ACTIVE; }
    /** Memory-only scope validation before entering the short proposal coordination lane. */
    public void validateScope(TransactionScope scope) {
        Objects.requireNonNull(scope); if (!admissionOpen()) throw new IllegalStateException("Range admission is closed"); validateSnapshotScope(scope);
    }
    private void validateSnapshotScope(TransactionScope scope) {
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
    }
    /** The adapter supplies a fresh leader/read barrier; validation alone confers no commit authority. */
    public ScopedHistoryPin captureTracked(TransactionScope scope) throws RocksDBException {
        Objects.requireNonNull(scope); enterNative(); applyLane.lock(); ScopedSnapshot snapshot=null; RangeMutationHistory.Pin registration=null;
        try {
            requireActive(); validateSnapshotScope(scope); ensureReceiptUsage(); requireUnlocked(scope);
            if (scopedHistory==null) throw new UnsupportedOperationException("Scoped mutation history is not enabled");
            if (applied==null || applied.term()==0) throw new IllegalStateException("Tracked snapshot requires a positive completed term");
            scopedHistory.checkOpenCapacity(scope); long scopeBytes=RangeMutationHistory.accountedScopeBytes(scope);
            if (trackedSnapshotPins.get()>=scopedLimits.maxPins() || scopeBytes>scopedLimits.maxScopeBytes()-trackedSnapshotScopeBytes.get()) throw new IllegalStateException("Retired native readers retain scoped admission");
            snapshot=new ScopedSnapshot(scope,captureUnderAdmission());
            snapshot.pin.historyScopeBytes=scopeBytes; trackedSnapshotScopeBytes.addAndGet(scopeBytes); trackedSnapshotPins.incrementAndGet(); snapshot.pin.historyTracked=true;
            if (closing) throw new IllegalStateException("Range admission is closing");
            try { registration=scopedHistory.open(snapshot.position(),scope); }
            catch (RuntimeException | Error failure) { resetScopedUnderLane(); throw failure; }
            var owned=new ScopedHistoryPin(snapshot,scopedHistory,registration,scopedGeneration,installationGeneration); scopedPins.put(registration,owned); snapshot=null; return owned;
        } finally {
            if (snapshot!=null) { if (registration!=null) { scopedHistory.close(registration); scopedPins.remove(registration); } scopedReleases.add(snapshot); }
            applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); }
        }
    }
    /**
     * Caller must already own the complete exclusive proposal scope. This only certifies
     * and freezes the ordered plan; it neither writes native state nor confers append authority.
     */
    public DistributedCommand planScopedCommit(ScopedHistoryPin pin,DistributedCommand command,long generation,UUID incarnation) throws RocksDBException {
        Objects.requireNonNull(pin); Objects.requireNonNull(command);
        if (pin.owner()!=this) throw new IllegalArgumentException("Tracked snapshot belongs to another range");
        if (command.acceptance()!=null || command.sessionControl()!=null || command.minimumSessionEpoch()>=0 || command.mutations().isEmpty()) throw new IllegalArgumentException("Expected an unaccepted business command");
        validate(command);
        validateScopedCommand(pin,command);
        enterNative(); applyLane.lock();
        try {
            requireActive();
            requireUnlocked(pin.scope());
            if (pin.validateUnderLane()!=RangeMutationHistory.Status.ACTIVE) throw new IllegalStateException("Transaction snapshot failed serializable validation: "+pin.status());
            var id=command.operationId(); long minimum=minimumSessionEpoch(null);
            long floor=id.epoch()<minimum ? -1 : acknowledgedThrough(id,null);
            if (id.epoch()<minimum || id.sequence()<=floor || db.get(receipts,receiptKey(id))!=null
                    || sessionDecision(command,floor,sessionAuthority())!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Transaction operation/session is no longer eligible");
            var evaluation=evaluate(command);
            if (evaluation.result().status()!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Transaction plan rejected: "+evaluation.result().status());
            // Close/fence can begin without this lane; it must not race certification unnoticed.
            requireActive(); if (pin.status()!=RangeMutationHistory.Status.ACTIVE) throw new IllegalStateException("Transaction snapshot lost certification ownership");
            var accepted=DistributedCommand.acceptedTransaction(command,new it.cavallium.rockserver.core.common.distributed.TransactionAcceptance(pin.position(),generation,incarnation,pin.scope(),evaluation.result().encode()));
            validate(accepted); return accepted;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private void validateScopedCommand(ScopedHistoryPin pin,DistributedCommand command) {
        for (var mutation:command.mutations()) {
            if (mutation.kind()==Mutation.Kind.DELETE_RANGE ? !pin.scope().containsRange(mutation.columnId(),mutation.key(),mutation.value())
                    : !pin.scope().contains(mutation.columnId(),mutation.key())) throw new IllegalArgumentException("Mutation outside transaction scope");
        }
        for (var predicate:command.predicates()) if (!pin.scope().contains(predicate.columnId(),predicate.key())) throw new IllegalArgumentException("Predicate outside transaction scope");
    }
    /** Unprepared physical-scope snapshot. All member accesses in a collision bucket share its one physical stamp. */
    public final class ScopedHistoryPin implements AutoCloseable {
        private final ScopedSnapshot snapshot; private final RangeMutationHistory history; private final RangeMutationHistory.Pin registration;
        private final Object generation; private final long installation; private final AtomicBoolean closed=new AtomicBoolean();
        private ScopedHistoryPin(ScopedSnapshot snapshot,RangeMutationHistory history,RangeMutationHistory.Pin registration,Object generation,long installation) {
            this.snapshot=snapshot; this.history=history; this.registration=registration; this.generation=generation; this.installation=installation;
        }
        public TransactionScope scope() { return snapshot.scope(); }
        private ReplicatedRangeStore owner() { return ReplicatedRangeStore.this; }
        public ApplyPosition position() { return snapshot.position(); }
        public RangeMutationHistory.Status status() {
            var status=registration.status(); if (status!=RangeMutationHistory.Status.ACTIVE) return status;
            if (closed.get()) return RangeMutationHistory.Status.CLOSED;
            return closing || generation!=scopedGeneration || installation!=installationGeneration ? RangeMutationHistory.Status.INVALIDATED : status;
        }
        private void requireReadable() { if (status()!=RangeMutationHistory.Status.ACTIVE) throw new IllegalStateException("Tracked snapshot is no longer valid: "+status()); }
        public byte[] get(UUID column,byte[] key) throws RocksDBException { requireReadable(); return snapshot.get(column,key); }
        public PhysicalRangePage page(UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws RocksDBException {
            requireReadable(); return snapshot.page(column,lower,upper,reverse,resume,items,bytes);
        }
        public void visitColumn(UUID column,byte[] lower,byte[] upper,SnapshotVisitor visitor) throws RocksDBException,IOException {
            requireReadable(); snapshot.visitColumn(column,lower,upper,visitor);
        }
        /** Builds a bounded read-your-writes view at this exact cut, without applying or accepting anything. */
        public ScopedStaging stage(DistributedCommand command) throws RocksDBException {
            requireReadable(); Objects.requireNonNull(command);
            if (command.kind()!=DistributedCommand.Kind.DATA || command.mutations().isEmpty()) throw new IllegalArgumentException("Expected business DATA for staging");
            ReplicatedRangeStore.this.validate(command); validateScopedCommand(this,command);
            if (logicalSchema!=null) validateLogical(command);
            return new ScopedStaging(this,command);
        }
        /** Observes complete declared-scope history under the apply lane; a later proposal still needs reservations/revalidation. */
        public RangeMutationHistory.Status validate() {
            applyLane.lock();
            try { return validateUnderLane(); }
            finally { applyLane.unlock(); drainScopedReleases(); }
        }
        private RangeMutationHistory.Status validateUnderLane() {
            var result=status();
            if (result==RangeMutationHistory.Status.ACTIVE) {
                try { requireActive(); result=history.validate(registration); }
                catch (RuntimeException failure) { resetScopedUnderLane(); result=RangeMutationHistory.Status.INVALIDATED; }
            } else if (registration.status()==RangeMutationHistory.Status.ACTIVE) {
                if (result==RangeMutationHistory.Status.CLOSED) history.close(registration);
                else invalidateScopedUnderLane(RangeMutationHistory.Status.INVALIDATED);
            }
            if (result!=RangeMutationHistory.Status.ACTIVE) retireScoped(registration); return result;
        }
        @Override public void close() {
            if (!closed.compareAndSet(false,true)) return;
            try { snapshot.close(); } // Existing JNI users own their deferral; never wait for them under the apply lane.
            finally {
                applyLane.lock(); try { history.close(registration); scopedPins.remove(registration); }
                finally { applyLane.unlock(); drainScopedReleases(); }
            }
        }
    }

    /** Immutable staged prefix. Owns its indexed batch and a native use until close; its parent owns certification. */
    public final class ScopedStaging implements AutoCloseable {
        private final ScopedHistoryPin parent;
        private final DistributedCommand command;
        private final WriteBatchWithIndex overlay;
        private final RangeOverlay visibility;
        private final CommandResult result;
        private final long accountedBytes;
        private boolean closed;
        private ScopedStaging(ScopedHistoryPin parent,DistributedCommand command) throws RocksDBException {
            this.parent=parent; this.command=command; this.visibility=new RangeOverlay(command);
            var pin=parent.snapshot.pin; pin.acquireUse();
            WriteBatchWithIndex building=null; boolean success=false;
            try {
                building=new WriteBatchWithIndex(true);
                building.setMaxBytes(MAX_BUCKET_PLAN_BYTES+3L*DistributedCommandCodec.MAX_ENCODED_BYTES);
                try (var options=new ReadOptions().setSnapshot(pin.snapshot)) {
                    var evaluation=evaluatePoints(command,building,options,visibility);
                    if (evaluation.result().status()!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Transaction staging rejected: "+evaluation.result().status());
                    for (var entry:evaluation.buckets().entrySet()) building.put(columns.get(entry.getKey().column),entry.getKey().key,entry.getValue().encode());
                    result=evaluation.result();
                    // Original operands, raw indexed effects, materialized buckets, result and bounded entry overhead.
                    long bytes=(visibility.enabled ? 5L : 2L)*DistributedCommandCodec.encodedSize(command)+result.encode().length+4096L+128L*(command.mutations().size()+command.predicates().size());
                    for (var bucket:evaluation.buckets().values()) bytes+=bucket.encodedSize();
                    accountedBytes=bytes;
                }
                parent.requireReadable(); overlay=building; building=null; success=true;
            } finally {
                if (!success) { try { if (building!=null) building.close(); } finally { pin.releaseUse(); } }
            }
        }
        public DistributedCommand command() { return command; }
        public CommandResult result() { return result; }
        public long accountedBytes() { return accountedBytes; }
        private void requireReadable() {
            if (closed) throw new IllegalStateException("Staged transaction view is closed");
            parent.requireReadable();
        }
        public synchronized byte[] get(UUID column,byte[] key) throws RocksDBException {
            requireReadable(); key=Objects.requireNonNull(key).clone();
            if (!parent.scope().contains(column,key)) throw new IllegalArgumentException("Read outside declared scope");
            try (var options=new ReadOptions().setSnapshot(parent.snapshot.pin.snapshot)) {
                byte[] value=visibility.deleted(column,key) ? null : overlay.getFromBatchAndDB(db,columns.get(column),options,key);
                if (value!=null && value.length>MAX_RESULT_BYTES) throw new IllegalStateException("Staged value exceeds reply budget");
                return value;
            }
        }
        public synchronized byte[] getMember(UUID column,byte[] key,List<byte[]> members) throws RocksDBException {
            requireReadable();
            if (logicalSchema==null) throw new UnsupportedOperationException("Logical schema is not qualified");
            key=Objects.requireNonNull(key).clone(); members=members.stream().map(byte[]::clone).toList();
            logicalSchema.requireBucketKey(column,key,members);
            // Physical bucket bytes have their own budget; only the selected member is a point reply.
            if (!parent.scope().contains(column,key)) throw new IllegalArgumentException("Read outside declared scope");
            try (var options=new ReadOptions().setSnapshot(parent.snapshot.pin.snapshot)) {
                byte[] raw=visibility.deleted(column,key) ? null : overlay.getFromBatchAndDB(db,columns.get(column),options,key);
                if (raw!=null && raw.length>MAX_BUCKET_PLAN_BYTES) throw new IllegalStateException("Staged bucket exceeds byte budget");
                byte[] value=logicalSchema.decodeBucket(column,key,raw).get(members);
                if (value!=null && value.length>MAX_RESULT_BYTES) throw new IllegalStateException("Staged member exceeds reply budget");
                return value;
            }
        }
        public synchronized PhysicalRangePage page(UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws RocksDBException {
            requireReadable(); validatePageBudget(items,bytes);
            if ((long)(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length)>TransactionScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("Page bounds exceed budget");
            lower=lower==null ? interval.lowerInclusive() : lower.clone(); upper=upper==null ? interval.upperExclusive() : upper.clone();
            if (!parent.scope().containsRange(column,lower,upper)) throw new IllegalArgumentException("Page outside declared scope");
            resume=resume==null ? null : resume.clone();
            if (resume!=null && (resume.length>PhysicalRangePage.MAX_BYTES || lower!=null && Arrays.compareUnsigned(resume,lower)<0 || upper!=null && Arrays.compareUnsigned(resume,upper)>=0)) throw new IllegalArgumentException("Continuation outside page bounds");
            try (var low=lower==null ? null : new org.rocksdb.Slice(lower);
                 var high=upper==null ? null : new org.rocksdb.Slice(upper);
                 var options=new ReadOptions().setSnapshot(parent.snapshot.pin.snapshot).setFillCache(false)) {
                if (low!=null) options.setIterateLowerBound(low); if (high!=null) options.setIterateUpperBound(high);
                try (var base=db.newIterator(columns.get(column),options);
                     var iterator=overlay.newIteratorWithBase(columns.get(column),base,options)) {
                    return readPage(iterator,lower,upper,reverse,resume,items,bytes,true,visibility,column);
                }
            }
        }
        @Override public synchronized void close() {
            if (closed) return; closed=true;
            try { overlay.close(); } finally { parent.snapshot.pin.releaseUse(); }
        }
    }

    /** Scope cannot expand; native reader ownership and close/drain semantics are retained by the pin. */
    public final class ScopedSnapshot implements AutoCloseable {
        private final TransactionScope scope;
        private final SnapshotPin pin;
        private ScopedSnapshot(TransactionScope scope, SnapshotPin pin) { this.scope = scope; this.pin = pin; }
        boolean belongsTo(ReplicatedRangeStore store) { return ReplicatedRangeStore.this == store; }
        public TransactionScope scope() { return scope; }
        public RangeOwnership.Interval interval() { return pin.interval(); }
        NativeUse retainForExport() {
            var released = new AtomicBoolean();
            NativeUse use = () -> { if (released.compareAndSet(false, true)) pin.releaseUse(); };
            pin.acquireUse();
            return use;
        }
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
            validatePageBudget(itemLimit, byteLimit);
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
        /** Bounds/direction remain fixed; this cursor borrows its parent snapshot and never reopens at latest. */
        public PageCursor openCursor(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive, boolean reverse) {
            if ((long)(lowerInclusive == null ? 0 : lowerInclusive.length) + (upperExclusive == null ? 0 : upperExclusive.length) > TransactionScope.MAX_BOUND_BYTES) {
                throw new IllegalArgumentException("Cursor bounds exceed budget");
            }
            byte[] lower = lowerInclusive == null ? interval.lowerInclusive() : lowerInclusive.clone();
            byte[] upper = upperExclusive == null ? interval.upperExclusive() : upperExclusive.clone();
            if (!scope.containsRange(columnId, lower, upper)) throw new IllegalArgumentException("Cursor outside declared scope");
            pin.requireOpen();
            return new PageCursor(columnId, lower, upper, reverse);
        }

        public final class PageCursor implements AutoCloseable {
            private final UUID columnId;
            private final byte[] lower, upper;
            private final boolean reverse;
            private final AtomicBoolean cursorClosed = new AtomicBoolean();
            private byte[] resume;
            private boolean exhausted;
            private PageCursor(UUID columnId, byte[] lower, byte[] upper, boolean reverse) {
                this.columnId = columnId; this.lower = lower; this.upper = upper; this.reverse = reverse;
            }
            /** Calls serialize continuation changes; closing allows an already-started page to finish. */
            public synchronized PhysicalRangePage nextPage(int itemLimit, int byteLimit) throws RocksDBException {
                if (cursorClosed.get()) throw new IllegalStateException("Page cursor closed");
                validatePageBudget(itemLimit, byteLimit);
                pin.requireOpen();
                if (exhausted) return new PhysicalRangePage(List.of(), false);
                var result = pin.page(columnId, lower, upper, reverse, resume, itemLimit, byteLimit);
                resume = result.resumeAfter();
                exhausted = !result.hasMore();
                return result;
            }
            /** Stops this borrowed cursor; the parent snapshot continues to own its native pin. */
            @Override public void close() { cursorClosed.set(true); }
        }
        @Override public void close() { pin.close(); }
    }

    private static void validatePageBudget(int itemLimit, int byteLimit) {
        if (itemLimit < 1 || itemLimit > PhysicalRangePage.MAX_ITEMS
                || byteLimit < PhysicalRangePage.HEADER_BYTES || byteLimit > PhysicalRangePage.MAX_BYTES) {
            throw new IllegalArgumentException("Invalid page budget");
        }
    }

    private SnapshotPin captureUnderAdmission() throws RocksDBException {
        requireActive();
        applyLane.lock();
        try {
            requireActive();
            ensureReceiptUsage();
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
        private long historyScopeBytes; private volatile boolean historyTracked;
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
        /** Immutable identity and authority from this same completed native snapshot cut; no user keys are accessed. */
        public RangeStateView stateView() throws RocksDBException {
            acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) {
                return new RangeStateView(groupId,rangeEpoch,interval.domainId(),schemaDigest,columns.keySet(),interval.lowerInclusive(),interval.upperExclusive(),position,sessionAuthority(options));
            } finally { releaseUse(); }
        }
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
        public PreparedParticipant preparedParticipant(UUID transaction) throws RocksDBException {
            acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) {
                byte[] bytes=db.get(state,options,preparedKey(transaction)); if (bytes==null) return null;
                var record=PreparedParticipant.decode(bytes);
                if (!record.preparation().transactionId().equals(transaction) || position==null || record.position().index()>position.index() || record.position().term()>position.term()) throw new IllegalStateException("Prepared receipt differs from snapshot cut");
                return record;
            } finally { releaseUse(); }
        }
        public byte[] operationResult(OperationId id, byte[] payloadDigest) throws RocksDBException {
            acquireUse();
            try {
                try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                    if (payloadDigest.length != 32) throw new IllegalArgumentException("Invalid payload digest");
                    if (id.epoch() < minimumSessionEpoch(options) || id.sequence() <= acknowledgedThrough(id, options)) return expiredResult();
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
                    return readPage(iterator,lower,upper,reverse,resume,itemLimit,byteLimit,false,null,null);
                }
            } finally { releaseUse(); }
        }
        public void visitState(SnapshotVisitor visitor) throws RocksDBException, IOException {
            acquireUse();
            try (var options = new ReadOptions().setSnapshot(snapshot)) {
                long minimum = minimumSessionEpoch(options);
                visit(state, markerKey, null, markerKey, (key, value) -> {
                    if (key.length == 41 && key[16] == 2) {
                        long epoch = ByteBuffer.wrap(key,33,8).getLong();
                        if (epoch >= 0 && epoch < minimum) return;
                    }
                    visitor.accept(key,value);
                });
            } finally { releaseUse(); }
        }
        public void visitReceipts(SnapshotVisitor visitor) throws RocksDBException, IOException {
            acquireUse();
            try (var options = new ReadOptions().setSnapshot(snapshot)) {
                long minimum = minimumSessionEpoch(options);
                visit(receipts, markerKey, null, markerKey, (key, value) -> {
                    if (key.length == 48) {
                        long epoch = ByteBuffer.wrap(key,32,8).getLong();
                        if (epoch >= 0 && epoch < minimum) return;
                    }
                    visitor.accept(key,value);
                });
            } finally { releaseUse(); }
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
        private synchronized void requireOpen() {
            if (closed) throw new IllegalStateException("Snapshot pin closed");
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
            finally {
                if (historyTracked) { trackedSnapshotScopeBytes.addAndGet(-historyScopeBytes); trackedSnapshotPins.decrementAndGet(); }
                snapshotPins.decrementAndGet();
            }
        }
    }

    private static boolean withinPageBounds(org.rocksdb.RocksIterator iterator,byte[] lower,byte[] upper) {
        if (!iterator.isValid()) return false;
        byte[] key=iterator.key();
        return (lower==null || Arrays.compareUnsigned(key,lower)>=0) && (upper==null || Arrays.compareUnsigned(key,upper)<0);
    }
    private static PhysicalRangePage readPage(org.rocksdb.RocksIterator iterator,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int itemLimit,int byteLimit,boolean checkBounds,RangeOverlay visibility,UUID column) throws RocksDBException {
        byte[] sizingBuffer = new byte[0];
        if (resume != null) {
            if (reverse) iterator.seekForPrev(resume); else iterator.seek(resume);
            if (iterator.isValid() && iterator.key(sizingBuffer) == resume.length && Arrays.equals(iterator.key(), resume)) {
                if (reverse) iterator.prev(); else iterator.next();
            }
        } else if (reverse && upper!=null) {
            iterator.seekForPrev(upper);
            if (iterator.isValid() && Arrays.equals(iterator.key(),upper)) iterator.prev();
        } else if (reverse) iterator.seekToLast();
        else if (lower == null) iterator.seekToFirst(); else iterator.seek(lower);
        if (visibility!=null) visibility.skipDeleted(iterator,column,reverse,lower,upper);
        var rows = new ArrayList<PhysicalRangePage.Row>();
        int bytes = PhysicalRangePage.HEADER_BYTES, lastKeyBytes = 0;
        while (iterator.isValid() && (!checkBounds || withinPageBounds(iterator,lower,upper)) && rows.size() < itemLimit) {
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
            if (visibility!=null) visibility.skipDeleted(iterator,column,reverse,lower,upper);
        }
        iterator.status();
        return new PhysicalRangePage(rows, iterator.isValid() && (!checkBounds || withinPageBounds(iterator,lower,upper)));
    }

    @FunctionalInterface
    interface NativeUse extends AutoCloseable {
        @Override void close();
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

    /** Immutable local progress; no native pin/iterator survives between reclamation pages. */
    public final class ReclamationCursor {
        private final long cutoff, generation;
        private final int phase;
        private final byte[] after;
        private ReclamationCursor(long cutoff, long generation, int phase, byte[] after) {
            this.cutoff = cutoff; this.generation = generation; this.phase = phase;
            this.after = after == null ? null : after.clone();
        }
        public long minimumEpoch() { return cutoff; }
        private boolean belongsTo(ReplicatedRangeStore store) { return ReplicatedRangeStore.this == store; }
    }
    public record ReclamationPage(long minimumEpoch, ReclamationCursor continuation,
                                  int examined, int deleted, long scannedBytes) {}

    /** Physical garbage collection after a committed epoch fence; never advances A or changes user data. */
    public ReclamationPage reclaimExpiredSessions(ReclamationCursor cursor, int rowLimit, int byteLimit) throws RocksDBException {
        if (rowLimit < 1 || rowLimit > 4096 || byteLimit < 1 || byteLimit > 8 * 1024 * 1024) {
            throw new IllegalArgumentException("Invalid session reclamation budget");
        }
        enterNative();
        applyLane.lock();
        try {
            requireActive();
            long minimum = minimumSessionEpoch(null);
            if (cursor != null && (!cursor.belongsTo(this) || cursor.generation != installationGeneration || cursor.cutoff > minimum)) {
                throw new IllegalArgumentException("Session reclamation cursor lost its floor or owner");
            }
            long cutoff = cursor == null ? minimum : cursor.cutoff;
            long generation = installationGeneration;
            if (cutoff == 0) return new ReclamationPage(0,null,0,0,0);
            int phase = cursor == null ? 0 : cursor.phase;
            byte[] after = cursor == null ? null : cursor.after;
            // Persist the fence before any local deletion can become durable without it.
            if (cursor == null) db.flushWal(true);
            int examined = 0, deleted = 0;
            long bytes = 0;
            try (var batch = new WriteBatch(); var write = new WriteOptions().setDisableWAL(false).setSync(false)) {
                while (phase < 2 && examined < rowLimit) {
                    byte[] prefix = phase == 0 ? ByteBuffer.allocate(17).put(markerKey).put((byte)2).array() : markerKey;
                    byte[] end = prefixEnd(prefix);
                    var cf = phase == 0 ? state : receipts;
                    try (var lower = new org.rocksdb.Slice(prefix); var upper = end == null ? null : new org.rocksdb.Slice(end);
                            var options = new ReadOptions().setFillCache(false).setIterateLowerBound(lower)) {
                        if (upper != null) options.setIterateUpperBound(upper);
                        try (var iterator = db.newIterator(cf,options)) {
                            byte[] sizing = new byte[0];
                            if (after == null) iterator.seek(prefix);
                            else {
                                iterator.seek(after);
                                if (iterator.isValid() && iterator.key(sizing) == after.length && Arrays.equals(iterator.key(),after)) iterator.next();
                            }
                            while (iterator.isValid() && examined < rowLimit) {
                                int keyBytes = iterator.key(sizing), valueBytes = iterator.value(sizing);
                                if (keyBytes != (phase == 0 ? 41 : 48) || valueBytes < 0) {
                                    throw new IllegalStateException("Invalid session reclamation metadata shape");
                                }
                                long needed = (long)keyBytes + valueBytes;
                                if (needed > byteLimit - bytes) {
                                    if (examined == 0) throw new IllegalArgumentException("Session reclamation needs at least " + needed + " bytes");
                                    break;
                                }
                                byte[] key = iterator.key();
                                long epoch = ByteBuffer.wrap(key,phase == 0 ? 33 : 32,8).getLong();
                                if (epoch < 0) throw new IllegalStateException("Invalid reclamation session epoch");
                                if (epoch < cutoff) { batch.delete(cf,key); deleted++; }
                                examined++; bytes += needed; after = key;
                                iterator.next();
                            }
                            iterator.status();
                            if (iterator.isValid()) break; // row/byte budget exhausted; continue this phase later
                        }
                    }
                    phase++; after = null;
                }
                if (deleted != 0) db.write(write,batch);
            }
            return new ReclamationPage(cutoff,phase == 2 ? null : new ReclamationCursor(cutoff,generation,phase,after),
                    examined,deleted,bytes);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    private static byte[] prefixEnd(byte[] prefix) {
        byte[] end = prefix.clone();
        int i = end.length - 1;
        while (i >= 0 && Byte.toUnsignedInt(end[i]) == 255) i--;
        if (i < 0) return null;
        end[i]++;
        return Arrays.copyOf(end,i+1);
    }

    /** Totals and completed position from one ordered apply cut. */
    public ReceiptUsage receiptUsage() throws RocksDBException {
        enterNative();
        applyLane.lock();
        try {
            if (pendingInstallation != null) throw new IllegalStateException("Range metadata is being installed");
            ensureReceiptUsage();
            var totals = readUsage(usageKey);
            return new ReceiptUsage(applied, totals.count, totals.bytes);
        } finally { applyLane.unlock(); leaveNative(); }
    }

    /**
     * Worst net retention growth at one completed apply cut; this reserves nothing and changes no command state.
     * Callers must serialize outstanding maintenance proposals to avoid spending overlapping ACK/floor removal twice.
     */
    public ReceiptRetentionEstimate estimateReceiptRetention(DistributedCommand command) throws RocksDBException {
        return estimateReceiptRetention(command,false);
    }

    /** Session-enabled adapters stop new data admission until their first replicated authority cut is installed. */
    public ReceiptRetentionEstimate estimateReceiptRetention(DistributedCommand command,boolean requireSessionAuthority) throws RocksDBException {
        Objects.requireNonNull(command);
        enterNative();
        applyLane.lock();
        try {
            validate(command);
            ensureReceiptUsage();
            Usage total = readUsage(usageKey);
            var usage = new ReceiptUsage(applied, total.count, total.bytes);
            boolean maintenance = command.acknowledgedThrough() >= 0 || command.minimumSessionEpoch() >= 0;
            var id = command.operationId();
            long minimum = minimumSessionEpoch(null);
            long floor = id.epoch() < minimum ? -1 : acknowledgedThrough(id, null);
            if (id.epoch() < minimum || id.sequence() <= floor) {
                return new ReceiptRetentionEstimate(usage, false, 0, 0, maintenance);
            }
            byte[] saved = db.get(receipts, receiptKey(id));
            if (saved != null) {
                decodeReceipt(saved, command.receiptDigest());
                return new ReceiptRetentionEstimate(usage, false, 0, 0, maintenance);
            }
            requireUnlocked(command);
            var authority=sessionAuthority();
            if (requireSessionAuthority && command.sessionControl()==null && command.minimumSessionEpoch()<0 && authority==null) {
                throw new IllegalStateException("Replicated session authority has not been initialized");
            }
            var sessionStatus=sessionDecision(command,floor,authority);
            boolean terminalExpiry=sessionStatus==CommandResult.Status.SESSION_EXPIRED && expiredBoundOutcome(command,floor,authority);
            if (sessionStatus!=CommandResult.Status.SUCCESS && !terminalExpiry) throw new IllegalStateException("Session admission rejected: " + sessionStatus);
            Usage removed = new Usage(0, 0);
            if (command.minimumSessionEpoch() > minimum) {
                byte[] end = ByteBuffer.allocate(25).put(usageBucketsPrefix).putLong(command.minimumSessionEpoch()).array();
                removed = scanUsage(state, usageBucketsPrefix, end, false);
            }
            if (command.acknowledgedThrough() > floor) {
                removed = removed.plus(scanUsage(receipts, receiptKey(new OperationId(id.sessionId(), id.epoch(), 0)),
                        receiptKey(new OperationId(id.sessionId(), id.epoch(), command.acknowledgedThrough() + 1)), true));
            }
            long maximumBytes = 84L + (terminalExpiry || command.mutations().stream().allMatch(m -> m.returnMode() == Mutation.ReturnMode.NONE)
                    ? 8 : CommandResult.MAX_BYTES);
            return new ReceiptRetentionEstimate(usage, true, Math.max(0, 1 - removed.count),
                    Math.max(0, maximumBytes - removed.bytes), maintenance);
        } finally { applyLane.unlock(); leaveNative(); }
    }

    private record Usage(long count, long bytes) {
        Usage {
            if (count < 0 || bytes < 0 || (count == 0 ? bytes != 0 : bytes / count < 84)) {
                throw new IllegalStateException("Invalid receipt usage");
            }
        }
        Usage plus(Usage other) { return new Usage(Math.addExact(count, other.count), Math.addExact(bytes, other.bytes)); }
        Usage minus(Usage other) { return new Usage(Math.subtractExact(count, other.count), Math.subtractExact(bytes, other.bytes)); }
        byte[] encode() { return ByteBuffer.allocate(20).putInt(STATE_VERSION).putLong(count).putLong(bytes).array(); }
    }

    private static Usage decodeUsage(byte[] value) {
        if (value == null) return new Usage(0, 0);
        if (value.length != 20 || ByteBuffer.wrap(value).getInt() != STATE_VERSION) {
            throw new IllegalStateException("Invalid receipt usage metadata");
        }
        return new Usage(ByteBuffer.wrap(value, 4, 16).getLong(), ByteBuffer.wrap(value, 12, 8).getLong());
    }

    // Current distinct records: total/count/bytes/term/index (36 bytes), epoch bucket/count/bytes (20 bytes).
    private static byte[] encodeTotalUsage(Usage usage, ApplyPosition position) {
        return ByteBuffer.allocate(36).put(usage.encode()).putLong(position == null ? 0 : position.term())
                .putLong(position == null ? -1 : position.index()).array();
    }

    private Usage readUsage(byte[] key) throws RocksDBException {
        byte[] value = db.get(state, key);
        return Arrays.equals(key, usageKey) ? decodeTotalUsage(value,applied) : decodeUsage(value);
    }

    // Epoch comes first so a coarse fence deletes all obsolete buckets in one range tombstone.
    private byte[] usageBucketKey(OperationId id) {
        return ByteBuffer.allocate(41).put(usageBucketsPrefix).putLong(id.epoch()).put(uuidBytes(id.sessionId())).array();
    }

    private Usage scanUsage(ColumnFamilyHandle cf, byte[] lower, byte[] upper, boolean receiptRows) throws RocksDBException {
        Usage total = new Usage(0, 0);
        try (var options = new ReadOptions().setFillCache(false); var iterator = db.newIterator(cf, options)) {
            for (iterator.seek(lower); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (upper != null && Arrays.compareUnsigned(key, upper) >= 0) break;
                if (upper == null && (key.length < markerKey.length || !Arrays.equals(key, 0, markerKey.length, markerKey, 0, markerKey.length))) break;
                if (receiptRows) {
                    byte[] value = iterator.value();
                    validateReceiptRow(key, value);
                    total = total.plus(new Usage(1, (long) key.length + value.length));
                } else {
                    if (key.length != 41) throw new IllegalStateException("Invalid usage bucket key");
                    total = total.plus(decodeUsage(iterator.value()));
                }
            }
            iterator.status();
        }
        return total;
    }

    private void updateReceiptUsage(WriteBatch batch, DistributedCommand command, long minimumEpoch, long acknowledgement, long epochFloor,
            long acknowledged, int valueBytes, ApplyPosition position) throws RocksDBException {
        Usage total = readUsage(usageKey);
        byte[] bucketKey = usageBucketKey(command.operationId());
        Usage bucket = readUsage(bucketKey);
        if (minimumEpoch > epochFloor) {
            byte[] end = ByteBuffer.allocate(25).put(usageBucketsPrefix).putLong(minimumEpoch).array();
            total = total.minus(scanUsage(state, usageBucketsPrefix, end, false));
            batch.deleteRange(state, usageBucketsPrefix, end);
        }
        if (acknowledgement > acknowledged) {
            var id = command.operationId();
            Usage removed = scanUsage(receipts, receiptKey(new OperationId(id.sessionId(), id.epoch(), 0)),
                    receiptKey(new OperationId(id.sessionId(), id.epoch(), acknowledgement + 1)), true);
            total = total.minus(removed);
            bucket = bucket.minus(removed);
        }
        Usage added = new Usage(1, 48L + valueBytes);
        batch.put(state, bucketKey, bucket.plus(added).encode());
        batch.put(state, usageKey, encodeTotalUsage(total.plus(added), position));
    }

    private static void validateReceiptRow(byte[] key, byte[] value) {
        if (key.length != 48 || ByteBuffer.wrap(key, 32, 8).getLong() < 0
                || ByteBuffer.wrap(key, 40, 8).getLong() < 0 || value.length < 36
                || value.length > 36 + MAX_RESULT_BYTES || ByteBuffer.wrap(value).getInt() != STATE_VERSION) {
            throw new IllegalStateException("Invalid durable receipt row");
        }
    }

    private static Usage decodeTotalUsage(byte[] value,ApplyPosition position) {
        if (value==null || value.length!=36) throw new IllegalStateException("Missing or malformed total receipt usage");
        var usage=decodeUsage(Arrays.copyOf(value,20));
        var input=ByteBuffer.wrap(value,20,16);
        if (input.getLong()!=(position==null ? 0 : position.term()) || input.getLong()!=(position==null ? -1 : position.index())) {
            throw new IllegalStateException("Receipt usage differs from completed apply position");
        }
        return usage;
    }

    private void ensureReceiptUsage() throws RocksDBException {
        byte[] marker=db.get(state,markerKey);
        var observed=marker==null ? null : decodePosition(marker);
        if (!Objects.equals(applied,observed)) throw new IllegalStateException("Completed apply marker changed outside ordered apply");
        if (db.get(state,ByteBuffer.allocate(17).put(markerKey).put((byte)0x12).array())!=null) {
            throw new IllegalStateException("Unsupported receipt accounting metadata");
        }
        byte[] total=db.get(state,usageKey);
        if (total==null) {
            if (applied!=null || hasRows(receipts,markerKey) || hasRows(state,usageBucketsPrefix)) {
                throw new IllegalStateException("Established range has no total receipt usage");
            }
            // Only a genuinely fresh owned range can create the current empty accounting record.
            total=encodeTotalUsage(new Usage(0,0),null);
            try (var write=new WriteOptions().setDisableWAL(false).setSync(true)) { db.put(state,write,usageKey,total); }
        }
        decodeTotalUsage(total,applied);
    }

    private boolean hasRows(ColumnFamilyHandle cf,byte[] prefix) throws RocksDBException {
        try (var options=new ReadOptions().setFillCache(false); var iterator=db.newIterator(cf,options)) {
            iterator.seek(prefix);
            byte[] key=iterator.isValid() ? iterator.key() : null;
            boolean present=key!=null && key.length>=prefix.length && Arrays.equals(key,0,prefix.length,prefix,0,prefix.length);
            iterator.status(); return present;
        }
    }

    /** Read-only validation at open/import, with one receipt and one session/epoch accumulator in memory. */
    private void validateReceiptUsage(ApplyPosition position) throws RocksDBException {
        Usage expected=decodeTotalUsage(db.get(state,usageKey),position), total=new Usage(0,0), bucket=new Usage(0,0);
        long minimum=minimumSessionEpoch(null), floor=-1, validatedBuckets=0;
        byte[] previousBucket=null;
        try (var options=new ReadOptions().setFillCache(false); var iterator=db.newIterator(receipts,options)) {
            byte[] end=prefixEnd(markerKey);
            for (iterator.seek(markerKey);iterator.isValid();iterator.next()) {
                byte[] key=iterator.key();
                if (end!=null && Arrays.compareUnsigned(key,end)>=0) break;
                byte[] value=iterator.value(); validateReceiptRow(key,value);
                var input=ByteBuffer.wrap(key,16,32);
                var id=new OperationId(new UUID(input.getLong(),input.getLong()),input.getLong(),input.getLong());
                if (id.epoch()<minimum) continue;
                byte[] currentBucket=usageBucketKey(id);
                if (!Arrays.equals(previousBucket,currentBucket)) {
                    if (previousBucket!=null && bucket.count!=0) { requireUsageBucket(previousBucket,bucket); validatedBuckets++; }
                    previousBucket=currentBucket; bucket=new Usage(0,0); floor=acknowledgedThrough(id,null);
                }
                if (id.sequence()<=floor) continue;
                Usage row=new Usage(1,(long)key.length+value.length); bucket=bucket.plus(row); total=total.plus(row);
            }
            iterator.status();
        }
        if (previousBucket!=null && bucket.count!=0) { requireUsageBucket(previousBucket,bucket); validatedBuckets++; }
        Usage bucketTotal=new Usage(0,0); long buckets=0;
        try (var options=new ReadOptions().setFillCache(false); var iterator=db.newIterator(state,options)) {
            byte[] end=prefixEnd(usageBucketsPrefix);
            for (iterator.seek(usageBucketsPrefix);iterator.isValid();iterator.next()) {
                byte[] key=iterator.key(); if (end!=null && Arrays.compareUnsigned(key,end)>=0) break;
                if (key.length!=41 || ByteBuffer.wrap(key,17,8).getLong()<minimum) throw new IllegalStateException("Invalid receipt usage bucket identity");
                Usage usage=decodeUsage(iterator.value());
                if (usage.count==0) throw new IllegalStateException("Empty receipt usage bucket");
                bucketTotal=bucketTotal.plus(usage); buckets++;
            }
            iterator.status();
        }
        if (!expected.equals(total) || !expected.equals(bucketTotal) || buckets!=validatedBuckets) {
            throw new IllegalStateException("Receipt usage differs from retained receipts/buckets");
        }
    }

    private void requireUsageBucket(byte[] key,Usage expected) throws RocksDBException {
        if (expected.count==0 || !expected.equals(decodeUsage(db.get(state,key)))) {
            throw new IllegalStateException("Receipt usage bucket differs from retained receipts");
        }
    }

    private byte[] epochFloorKey() { return ByteBuffer.allocate(17).put(uuidBytes(groupId)).put((byte) 3).array(); }
    private long minimumSessionEpoch(ReadOptions options) throws RocksDBException {
        byte[] value = options == null ? db.get(state, epochFloorKey()) : db.get(state, options, epochFloorKey());
        if (value == null) return 0;
        if (value.length != 12) throw new IllegalStateException("Invalid session epoch floor");
        var input = ByteBuffer.wrap(value);
        if (input.getInt() != STATE_VERSION) throw new IllegalStateException("Unsupported session epoch floor version");
        long floor = input.getLong();
        if (floor < 0) throw new IllegalStateException("Invalid session epoch floor");
        return floor;
    }

    private static byte[] expiredResult() {
        return new CommandResult(CommandResult.Status.SESSION_EXPIRED, List.of()).encode();
    }
    private byte[] sessionFloorKey(OperationId id) {
        return ByteBuffer.allocate(41).put(uuidBytes(groupId)).put((byte) 2).put(uuidBytes(id.sessionId())).putLong(id.epoch()).array();
    }
    private long acknowledgedThrough(OperationId id, ReadOptions options) throws RocksDBException {
        byte[] value = options == null ? db.get(state, sessionFloorKey(id)) : db.get(state, options, sessionFloorKey(id));
        if (value == null) return -1;
        if (value.length != 12) throw new IllegalStateException("Invalid session floor");
        var input = ByteBuffer.wrap(value);
        if (input.getInt() != STATE_VERSION) throw new IllegalStateException("Unsupported session floor version");
        long floor = input.getLong();
        if (floor < 0 || floor == Long.MAX_VALUE) throw new IllegalStateException("Invalid session floor sequence");
        return floor;
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
