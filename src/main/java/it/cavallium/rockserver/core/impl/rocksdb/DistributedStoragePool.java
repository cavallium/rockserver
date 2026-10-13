package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.impl.StoragePressureSignal;
import it.cavallium.rockserver.core.impl.RocksDBLongProperty;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import it.cavallium.rockserver.core.config.GlobalDatabaseConfig;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

/** Native owner for ranges sharing one persistent database, not a catalogue or node bootstrap. */
public final class DistributedStoragePool implements AutoCloseable {
    public static final String STATE_COLUMN = "_distributed_range_state_";
    public static final String RECEIPTS_COLUMN = "_distributed_range_receipts_";
    private static final Set<String> INTERNAL_COLUMNS = Set.of(STATE_COLUMN, RECEIPTS_COLUMN);
    private static final Set<String> RESERVED_COLUMNS = Set.of("default", "_column_schemas_",
            "_merge_operators_", "_cdc_meta_", STATE_COLUMN, RECEIPTS_COLUMN);

    static boolean isReservedColumn(String name) { return RESERVED_COLUMNS.contains(name); }
    private final RocksDBLoader.LoadedDb loaded;
    private final GlobalDatabaseConfig globalConfig;
    private final Logger logger;
    private final DistributedOperatorArtifacts artifacts;
    private final RangeOwnership ownership = new RangeOwnership();
    // Serializes native schema mutation/close, without holding the admission metadata monitor.
    private final Semaphore nativeMutation = new Semaphore(1);
    private final java.util.concurrent.locks.ReentrantReadWriteLock handleLifetime=new java.util.concurrent.locks.ReentrantReadWriteLock();
    private final Object admission = new Object();
    private final Map<String, ColumnFamilyHandle> handles = new HashMap<>();
    private final Map<UUID, Binding> bindings = new HashMap<>();
    private final List<ColumnFamilyHandle> newHandles = new ArrayList<>();
    private final List<ReplicatedRangeStore> ranges = new ArrayList<>();
    private final Set<UUID> deferredDrops=new HashSet<>();
    private final Map<UUID,SchemaChange.Definition> deletionOrigins=new HashMap<>();
    private final Map<UUID,SchemaChange.Definition> schemaOrigins=new HashMap<>();
    private static byte[] groupSchemaKey(UUID group,int tag) { return ByteBuffer.allocate(17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)tag).array(); }
    private volatile PoolSnapshotAdmission snapshotAdmission;
    private volatile List<ColumnFamilyHandle> pressureColumns = List.of();
    private volatile List<Path> storageVolumes = List.of();
    private final Set<SchemaInstallPlan> schemaPlans=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private final Set<ReplicatedRangeStore.SchemaInstallCaptureGate> schemaCaptures=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private final Map<Object,java.util.concurrent.CompletableFuture<Void>> schemaCleanups=new java.util.IdentityHashMap<>();
    private long schemaPlanBytes;
    private java.util.concurrent.ThreadPoolExecutor schemaCleanupExecutor;
    private boolean closing;
    private boolean closed;

    private static final UUID BOOTSTRAP_BIRTH=new UUID(0,0);
    private static final byte[] BIRTHS_KEY="CF-BIRTH".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_BINDINGS=4096,MAX_BINDING_BYTES=8*1024*1024;
    private enum BirthKind { BOOTSTRAP, CREATE }
    private record Binding(String name,boolean mergeAdmitted,BirthKind birthKind,UUID birthChange) {
        Binding(String name,boolean mergeAdmitted) { this(name,mergeAdmitted,BirthKind.BOOTSTRAP,BOOTSTRAP_BIRTH); }
    }
    private boolean birthsDurable=true;
    private static final byte[] OWNERSHIP_HISTORY_KEY="OWNERSHIP-HISTORY".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_OWNERSHIP_HISTORY=4096,MAX_OWNERSHIP_BYTES=8*1024*1024;
    private record HistoricalOwner(RangeOwnership.Owner owner,RangeOwnership.Interval interval) {}
    private List<HistoricalOwner> ownershipHistory=List.of();
    private boolean ownershipHistoryDurable=true;
    // Exact unpublished claims survive unknown native completion and block identity substitution.
    private final Map<UUID,NativeSstImportManifest> newImportReservations=new HashMap<>();
    @FunctionalInterface public interface ImportBirthForceObserver { void boundary(boolean afterPersistence) throws RocksDBException; }
    private volatile ImportBirthForceObserver importBirthForceObserver=after -> {};
    public void importBirthForceObserver(ImportBirthForceObserver observer) { importBirthForceObserver=Objects.requireNonNull(observer); }
    private static final byte[] TOPOLOGY_SEEDS_KEY="TOPO-SEEDS".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_TOPOLOGY_SEEDS=128,MAX_TOPOLOGY_SEED_BYTES=64*1024*1024;
    private volatile Map<UUID,NativeTopologySeed> topologySeeds=Map.of();
    private volatile Map<UUID,NativeTopologyBirth> topologyBirths=Map.of();
    private volatile Map<UUID,NativeTopologySeed> topologyRetirements=Map.of();
    private static final int MAX_TOPOLOGY_LEARNERS=8,MAX_TOPOLOGY_LEARNER_BYTES=32*1024*1024;
    private volatile Map<UUID,NativeTopologyLearnerRegistration> topologyLearners=Map.of();
    private final Map<UUID,NativeTopologyLearnerRegistration> pendingTopologyLearners=new HashMap<>();
    private UUID pendingLearnerHistory;
    private volatile TopologySeedForceObserver topologyLearnerForceObserver=after -> {};
    public void topologyLearnerForceObserver(TopologySeedForceObserver observer) { topologyLearnerForceObserver=Objects.requireNonNull(observer); }
    public List<NativeTopologyLearnerRegistration> topologyLearners() { return List.copyOf(topologyLearners.values()); }
    private void recoverTopologyLearners() {
        try { var found=new HashMap<UUID,NativeTopologyLearnerRegistration>();long bytes=0;
            for(var history:ownershipHistory) { UUID group=history.owner().groupId();if(found.containsKey(group))continue;byte[] key=groupSchemaKey(group,0x67);int size=loaded.db().get().get(handles.get(STATE_COLUMN),key,new byte[0]);if(size==org.rocksdb.RocksDB.NOT_FOUND)continue;
                if(size>NativeTopologyLearnerRegistration.MAX_BYTES || found.size()>=MAX_TOPOLOGY_LEARNERS || (bytes+=size)>MAX_TOPOLOGY_LEARNER_BYTES)throw new IllegalArgumentException("Learner registration retention exceeded");var registration=NativeTopologyLearnerRegistration.decode(loaded.db().get().get(handles.get(STATE_COLUMN),key));var descriptor=registration.definition().targets().getFirst();
                if(!descriptor.groupId().equals(group) || !history.owner().equals(new RangeOwnership.Owner(group,descriptor.rangeEpoch())) || !history.interval().domainId().equals(learnerInterval(registration).domainId()) || !history.interval().columns().containsAll(registration.schema().columnIds()) || !Arrays.equals(history.interval().lowerInclusive(),descriptor.lowerInclusive()) || !Arrays.equals(history.interval().upperExclusive(),descriptor.upperExclusive()) || topologyBirths.containsKey(group) || topologyRetirements.containsKey(group))throw new IllegalArgumentException("Learner lacks exact permanent receiver history");found.put(group,registration);
            }
            topologyLearners=Map.copyOf(found);
        } catch(RocksDBException failure) { throw new IllegalStateException("Cannot recover learner registration",failure); }
    }
    private RangeOwnership.Interval learnerInterval(NativeTopologyLearnerRegistration registration) {var range=registration.definition().targets().getFirst();return new RangeOwnership.Interval(registration.schema().domainId(),registration.schema().columnIds(),range.lowerInclusive(),range.upperExclusive());}
    /** Trusted fresh committed relocation qualification; serialized registration is not client authority. */
    public ReplicatedRangeStore openTopologyLearner(NativeTopologyLearnerRegistration registration) throws RocksDBException {
        Objects.requireNonNull(registration);var range=registration.definition().targets().getFirst();UUID group=range.groupId();var owner=new RangeOwnership.Owner(group,range.rangeEpoch());var interval=learnerInterval(registration);
        nativeMutation.acquireUninterruptibly();try {
            synchronized(admission) { if(closing)throw new IllegalStateException("Pool is closing"); }
            if(pendingTopologySeed!=null || !newImportReservations.isEmpty())throw new IllegalStateException("Unknown seed/import history requires exact original retry");
            if(pendingLearnerHistory!=null && !pendingLearnerHistory.equals(group))throw new IllegalStateException("Unknown learner history requires exact original retry");
            var pending=pendingTopologyLearners.get(group);var saved=topologyLearners.get(group);if(pending!=null && !pending.equals(registration) || saved!=null && !saved.equals(registration))throw new IllegalArgumentException("Learner registration changed original authorization/image");
            byte[] persisted=loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(group,0x67));if(persisted!=null && !Arrays.equals(persisted,registration.encode()))throw new IllegalArgumentException("Persisted learner identity changed");
            try { if(artifacts.qualified() || !registration.mergeColumns().isEmpty())artifacts.bind(registration.schema(),registration.physicalNames()); } catch(IOException failure) { throw new IllegalStateException("Learner current operators are not locally qualified",failure); }
            if(saved==null) {
                if(topologyBirths.containsKey(group) || topologyRetirements.containsKey(group) || pending==null && persisted==null && ownershipHistory.stream().anyMatch(history->history.owner().groupId().equals(group)))throw new IllegalArgumentException("Learner cannot reuse a historical local group");
                var charged=new HashMap<>(topologyLearners);charged.putAll(pendingTopologyLearners);charged.put(group,registration);long total=charged.values().stream().mapToLong(value->value.encode().length).sum();if(charged.size()>MAX_TOPOLOGY_LEARNERS || total>MAX_TOPOLOGY_LEARNER_BYTES)throw new IllegalStateException("Learner metadata capacity exhausted");pendingTopologyLearners.put(group,registration);topologyLearnerForceObserver.boundary(false);
                var history=proposedOwnershipHistory(owner,interval);final byte[] historyBytes;try { historyBytes=encodeOwnershipHistory(history); } catch(IOException invalid) { throw new IllegalStateException("Learner ownership history capacity exhausted",invalid); }ownershipHistory=history;ownershipHistoryDurable=false;pendingLearnerHistory=group;try(var batch=new org.rocksdb.WriteBatch();var options=new org.rocksdb.WriteOptions().setDisableWAL(false).setSync(true)) {batch.put(handles.get(STATE_COLUMN),OWNERSHIP_HISTORY_KEY,historyBytes);batch.put(handles.get(STATE_COLUMN),groupSchemaKey(group,0x67),registration.encode());loaded.db().get().write(options,batch);}topologyLearnerForceObserver.boundary(true);loaded.db().get().flushWal(true);ownershipHistory=history;ownershipHistoryDurable=true;var next=new HashMap<>(topologyLearners);next.put(group,registration);topologyLearners=Map.copyOf(next);pendingTopologyLearners.remove(group);pendingLearnerHistory=null;
            }
        } finally { nativeMutation.release(); }
        var store=openRange(owner,interval,registration.schema().digest(),registration.physicalNames(),registration.mergeColumns());
        var schema=registration.schema();
        if(!Arrays.equals(store.schemaDigest(),schema.digest())) {
            var preparation=store.learnerSchemaPreparation();if(preparation!=null && Arrays.equals(store.schemaDigest(),preparation.schema().digest()))schema=preparation.schema();
            else {var checkpoint=recoverSchema(owner.groupId());
            if(checkpoint.current()==null || !Arrays.equals(store.schemaDigest(),checkpoint.current().definition().targetSchema().digest())) throw new IllegalArgumentException("Learner lacks verified current schema");
            schema=checkpoint.current().definition().targetSchema();}
        }
        store.enableLogicalSchema(schema);var preparation=store.learnerSchemaPreparation();if(preparation!=null && !store.pendingInstallation())prepareTopologyLearnerImage(store,preparation);return store;
    }
    /** Fresh authority has qualified this image; create only receiver-owned CF/operator bindings. */
    public void prepareTopologyLearnerImage(ReplicatedRangeStore store,NativeTopologyLearnerImagePermission permission) throws RocksDBException {
        Objects.requireNonNull(store);Objects.requireNonNull(permission);nativeMutation.acquireUninterruptibly();var options=new HashMap<String,PreparedColumn>();
        try {
            synchronized(admission){if(closing)throw new IllegalStateException("Pool is closing");}if(!ranges.contains(store))throw new IllegalArgumentException("Foreign learner store");permission.requireRegistration(Objects.requireNonNull(store.topologyLearnerRegistration(),"No learner registration"));
            for(var column:permission.physicalNames().entrySet()){if(RESERVED_COLUMNS.contains(column.getValue()))throw new IllegalArgumentException("Reserved learner CF alias");var old=bindings.get(column.getKey());if(old!=null && (!old.name().equals(column.getValue()) || old.mergeAdmitted()!=permission.mergeColumns().contains(column.getKey())))throw new IllegalArgumentException("Learner preparation changed permanent local binding");if(bindings.entrySet().stream().anyMatch(other->!other.getKey().equals(column.getKey()) && other.getValue().name().equals(column.getValue())))throw new IllegalArgumentException("Learner alias belongs to another column");}
            try {if(artifacts.qualified() || !permission.mergeColumns().isEmpty())artifacts.validate(permission.schema(),permission.physicalNames());}catch(IOException invalid){throw new IllegalStateException("Learner schema operators are not locally qualified",invalid);}
            store.prepareLearnerImageSchema(permission,()->{
                try {if(artifacts.qualified() || !permission.mergeColumns().isEmpty()){artifacts.bindPermanentPlainBirths(plainBirths(bindings));artifacts.bind(permission.schema(),permission.physicalNames());}}catch(IOException invalid){throw new IllegalStateException("Cannot force receiver learner operator bindings",invalid);}
            for(var column:permission.physicalNames().entrySet()){var name=column.getValue();if(handles.containsKey(name)){if((loaded.mergeOperators().get(name)!=null)!=permission.mergeColumns().contains(column.getKey()))throw new IllegalArgumentException("Learner prepared operator differs from actual CF");}else {var prepared=new PreparedColumn(name);options.put(name,prepared);if((prepared.options.mergeOperator()!=null)!=permission.mergeColumns().contains(column.getKey()))throw new IllegalArgumentException("Learner operator was not prepared");}}
                reserveBirths(permission.physicalNames(),permission.mergeColumns(),BOOTSTRAP_BIRTH,Set.of());var owner=store.ownership();retainOwnershipHistory(owner.owner(),new RangeOwnership.Interval(owner.interval().domainId(),permission.physicalNames().keySet(),owner.interval().lowerInclusive(),owner.interval().upperExclusive()));
                var target=new HashMap<UUID,ColumnFamilyHandle>();for(var column:permission.physicalNames().entrySet()){var handle=handles.get(column.getValue());if(handle==null)handle=createColumn(Objects.requireNonNull(options.get(column.getValue())));target.put(column.getKey(),handle);bindings.putIfAbsent(column.getKey(),new Binding(column.getValue(),permission.mergeColumns().contains(column.getKey())));}pressureColumns=List.copyOf(handles.values());return target;
            });
        } finally {options.values().forEach(PreparedColumn::close);nativeMutation.release();}
    }
    private volatile PendingTopologySeed pendingTopologySeed;
    @FunctionalInterface public interface TopologySeedForceObserver { void boundary(boolean afterPersistence) throws RocksDBException; }
    private volatile TopologySeedForceObserver topologySeedForceObserver=after -> {};
    public void topologySeedForceObserver(TopologySeedForceObserver observer) { topologySeedForceObserver=Objects.requireNonNull(observer); }
    /** Completed, forced local artifacts; pending force attempts confer no runtime discovery authority. */
    public List<NativeTopologySeed> topologySeeds() {
        return topologySeeds.values().stream().sorted(Comparator.comparingLong(seed->seed.definition().sourceDirectory().generation())).toList();
    }
    public NativeTopologySeed topologySeed(UUID intent) { return topologySeeds.get(Objects.requireNonNull(intent)); }
    private record PendingTopologySeed(NativeTopologySeed seed,List<ReplicatedRangeStore> parents,List<HistoricalOwner> history,Map<UUID,NativeTopologySeed> seeds,RangeOwnership.TopologyTransfer transfer) {}
    private byte[] encodeTopologySeeds(Map<UUID,NativeTopologySeed> seeds) throws IOException {
        if(seeds.size()>MAX_TOPOLOGY_SEEDS) throw new IllegalStateException("Topology seed count exhausted");
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(0x54535247); out.writeInt(seeds.size());
        for(var id:seeds.keySet().stream().sorted().toList()) { byte[] encoded=seeds.get(id).encode(); out.writeInt(encoded.length); out.write(encoded); if(bytes.size()>MAX_TOPOLOGY_SEED_BYTES) throw new IllegalStateException("Topology seed bytes exhausted"); }
        return bytes.toByteArray();
    }
    private void requireTopologySeedRows(NativeTopologySeed seed) throws RocksDBException {
        for(var range:java.util.stream.Stream.concat(seed.definition().parents().stream(),seed.definition().targets().stream()).toList()) {
            var expected=topologyInterval(seed,range);
            if(ownershipHistory.stream().noneMatch(record -> record.owner().equals(new RangeOwnership.Owner(range.groupId(),range.rangeEpoch())) && record.interval().domainId().equals(expected.domainId()) && record.interval().columns().containsAll(expected.columns()) && Arrays.equals(record.interval().lowerInclusive(),expected.lowerInclusive()) && Arrays.equals(record.interval().upperExclusive(),expected.upperExclusive()))) throw new IllegalArgumentException("Topology seed lacks permanent ownership history");
        }
        var db=loaded.db().get(); var state=handles.get(STATE_COLUMN); byte[] digest=seed.digest();
        for(var parent:seed.definition().parents()) {
            if(!Arrays.equals(db.get(state,groupSchemaKey(parent.groupId(),0x62)),digest)) throw new IllegalArgumentException("Topology retirement pointer missing/changed");
            byte[] value=db.get(state,groupSchemaKey(parent.groupId(),0x60)); if(value==null || !NativeTopologyFreeze.decode(value).equals(seed.parentFreezes().get(parent.groupId()))) throw new IllegalArgumentException("Topology retirement original FREEZE changed");
            byte[] marker=db.get(state,Arrays.copyOf(groupSchemaKey(parent.groupId(),0),16));
            if(marker==null || marker.length!=20 || ByteBuffer.wrap(marker).getInt()!=1) throw new IllegalArgumentException("Retired parent lacks completed native marker");
            var cut=new ApplyPosition(ByteBuffer.wrap(marker,4,16).getLong(),ByteBuffer.wrap(marker,12,8).getLong()); seed.parentFreezes().get(parent.groupId()).requireCut(cut);
        }
        for(var target:seed.definition().targets()) { byte[] bytes=db.get(state,groupSchemaKey(target.groupId(),0x61)); if(bytes==null || !NativeTopologyBirth.decode(bytes).equals(seed.birth(target.groupId()))) throw new IllegalArgumentException("Topology child birth missing/changed"); }
        for(var entry:seed.physicalNames().entrySet()) { var binding=bindings.get(entry.getKey()); if(binding==null || !binding.name().equals(entry.getValue()) || binding.mergeAdmitted()!=seed.mergeColumns().contains(entry.getKey())) throw new IllegalArgumentException("Topology physical binding changed"); }
    }
    private RangeOwnership.Interval topologyInterval(NativeTopologySeed seed,RangeDescriptor range) { return new RangeOwnership.Interval(seed.header().domainId(),seed.definition().sourceDirectory().columns(),range.lowerInclusive(),range.upperExclusive()); }
    private void recoverTopologySeeds() {
        try {
            var db=loaded.db().get(); var state=handles.get(STATE_COLUMN); int size=db.get(state,TOPOLOGY_SEEDS_KEY,new byte[0]); if(size==org.rocksdb.RocksDB.NOT_FOUND) return;
            if(size>MAX_TOPOLOGY_SEED_BYTES) throw new IllegalArgumentException("Topology registry exceeds budget"); var in=new DataInputStream(new ByteArrayInputStream(db.get(state,TOPOLOGY_SEEDS_KEY)));
            if(in.readInt()!=0x54535247) throw new IllegalArgumentException("Invalid topology registry"); int count=in.readInt(); if(count<0 || count>MAX_TOPOLOGY_SEEDS) throw new IllegalArgumentException("Topology registry count exceeded");
            var seeds=new HashMap<UUID,NativeTopologySeed>(); var retired=new HashMap<RangeOwnership.Owner,RangeOwnership.Interval>(); var children=new HashMap<RangeOwnership.Owner,RangeOwnership.Interval>(); var births=new HashMap<UUID,NativeTopologyBirth>(); var retirements=new HashMap<UUID,NativeTopologySeed>();
            for(int i=0;i<count;i++) {
                int length=in.readInt(); if(length<1 || length>NativeTopologySeed.MAX_BYTES || length>in.available()) throw new IllegalArgumentException("Topology artifact length exceeded"); var seed=NativeTopologySeed.decode(in.readNBytes(length)); requireTopologySeedRows(seed);
                if(seeds.put(seed.definition().intentId(),seed)!=null) throw new IllegalArgumentException("Duplicate topology intent");
                for(var parent:seed.definition().parents()) { if(retirements.put(parent.groupId(),seed)!=null) throw new IllegalArgumentException("Parent retired twice"); retired.put(new RangeOwnership.Owner(parent.groupId(),parent.rangeEpoch()),topologyInterval(seed,parent)); }
                for(var target:seed.definition().targets()) { if(births.put(target.groupId(),seed.birth(target.groupId()))!=null) throw new IllegalArgumentException("Target born twice"); var childInterval=topologyInterval(seed,target); var evolution=recoverSchema(target.groupId());
                    if(evolution.current()!=null) {
                        if(evolution.chain().stream().noneMatch(record -> Arrays.equals(record.definition().sourceSchema().digest(),seed.header().schemaDigest()) && record.sourceNames().equals(seed.physicalNames()) && record.sourceMerges().equals(seed.mergeColumns()))) throw new IllegalArgumentException("Topology child evolution lacks original seed schema");
                        evolution.current().requireRange(target.groupId(),target.rangeEpoch(),seed.header().domainId(),target.lowerInclusive(),target.upperExclusive());
                        childInterval=new RangeOwnership.Interval(seed.header().domainId(),evolution.current().definition().targetSchema().columnIds(),target.lowerInclusive(),target.upperExclusive());
                    }
                    children.put(new RangeOwnership.Owner(target.groupId(),target.rangeEpoch()),childInterval); }
            }
            if(in.available()!=0 || !Arrays.equals(db.get(state,TOPOLOGY_SEEDS_KEY),encodeTopologySeeds(seeds))) throw new IllegalArgumentException("Noncanonical topology registry");
            // Startup never publishes an ownership transfer from merely buffered metadata.
            db.flushWal(true); children.keySet().removeIf(owner -> retired.keySet().stream().anyMatch(parent -> parent.groupId().equals(owner.groupId())));
            ownership.recoverTopology(retired,children); topologySeeds=Map.copyOf(seeds); topologyBirths=Map.copyOf(births); topologyRetirements=Map.copyOf(retirements);
        } catch(IOException|RocksDBException failure) { throw new IllegalStateException("Cannot recover topology births",failure); }
    }
    private void requireSeedRetry(NativeTopologySeed saved,NativeTopologySeed candidate) {
        if(!Arrays.equals(saved.definition().encode(),candidate.definition().encode()) || !Arrays.equals(saved.authority().freezeDigest(),candidate.authority().freezeDigest()) || !saved.localReplica().equals(candidate.localReplica()) || !saved.parentFreezes().equals(candidate.parentFreezes()) || !saved.physicalNames().equals(candidate.physicalNames()) || !saved.mergeColumns().equals(candidate.mergeColumns())) throw new IllegalArgumentException("Original topology seed inputs changed");
    }
    @FunctionalInterface private interface SeedWork { NativeTopologySeed run() throws RocksDBException; }
    private NativeTopologySeed withTopologyParents(List<ReplicatedRangeStore> parents,int at,SeedWork work) throws RocksDBException { return at==parents.size() ? work.run() : parents.get(at).readSchemaMetadata(() -> withTopologyParents(parents,at+1,work)); }
    public NativeTopologySeed seedTopology(TopologyChange.Definition definition,TopologyChange.State authority,List<ReplicatedRangeStore> parents,ReplicaDescriptor local) throws RocksDBException {
        Objects.requireNonNull(definition); Objects.requireNonNull(authority); Objects.requireNonNull(local); parents=List.copyOf(parents);
        if(parents.size()!=definition.parents().size() || new HashSet<>(parents).size()!=parents.size() || parents.stream().anyMatch(parent -> !ranges.contains(parent))) throw new IllegalArgumentException("Topology seed requires exact local pool parents");
        var ordered=parents.stream().sorted(java.util.Comparator.comparing(parent -> parent.ownership().owner().groupId().toString())).toList(); nativeMutation.acquireUninterruptibly();
        try {
            synchronized(admission) { if(closing) throw new IllegalStateException("Pool is closing"); }
            if(pendingLearnerHistory!=null)throw new IllegalStateException("Unknown learner history requires exact original retry");
            return withTopologyParents(ordered,0,() -> {
                var freezes=new HashMap<UUID,NativeTopologyFreeze>(); Map<UUID,String> names=null; Set<UUID> merges=null;
                for(var parent:ordered) {
                    var freeze=parent.topologyFreeze(); if(freeze==null || !parent.topologyFreezeState().ready()) throw new IllegalStateException("Seed parent lacks drained native FREEZE"); freeze.requireCut(parent.applied());
                    if(!Arrays.equals(parent.schemaDigest(),definition.sourceDirectory().schemaDigest())) throw new IllegalArgumentException("Seed source schema changed");
                    var current=schemaPhysicalNamesUnderMutation(parent); if(names!=null && (!names.equals(current) || !merges.equals(parent.deterministicMergeColumns()))) throw new IllegalArgumentException("Seed parents use different native CFs/operators"); names=current; merges=parent.deterministicMergeColumns(); freezes.put(parent.ownership().owner().groupId(),freeze);
                }
                var candidate=new NativeTopologySeed(definition,authority,local,freezes,names,merges); var saved=topologySeeds.get(definition.intentId());
                if(saved!=null) { requireSeedRetry(saved,candidate); requireTopologySeedRows(saved); return saved; }
                if(pendingTopologySeed!=null) { requireSeedRetry(pendingTopologySeed.seed(),candidate); return forceTopologySeed(pendingTopologySeed); }
                var sources=new HashMap<RangeOwnership.Owner,RangeOwnership.Interval>(); ordered.forEach(parent -> sources.put(parent.ownership().owner(),parent.ownership().interval()));
                var targets=new HashMap<RangeOwnership.Owner,RangeOwnership.Interval>(); var history=new ArrayList<>(ownershipHistory);
                for(var target:definition.targets()) {
                    if(ownershipHistory.stream().anyMatch(previous -> previous.owner().groupId().equals(target.groupId()))) throw new IllegalArgumentException("Topology target group was already used");
                    byte[] prefix=Arrays.copyOf(groupSchemaKey(target.groupId(),0),16);
                    for(var cf:List.of(handles.get(STATE_COLUMN),handles.get(RECEIPTS_COLUMN))) try(var iterator=loaded.db().get().newIterator(cf)) { iterator.seek(prefix); if(iterator.isValid() && startsWith(iterator.key(),prefix)) throw new IllegalArgumentException("Topology target has historical metadata"); iterator.status(); }
                    var owner=new RangeOwnership.Owner(target.groupId(),target.rangeEpoch()); var interval=topologyInterval(candidate,target); targets.put(owner,interval); history.add(new HistoricalOwner(owner,interval));
                }
                if(history.size()>MAX_OWNERSHIP_HISTORY) throw new IllegalStateException("Topology ownership history exhausted"); var next=new HashMap<>(topologySeeds); next.put(definition.intentId(),candidate);
                try { encodeOwnershipHistory(history); encodeTopologySeeds(next); } catch(IOException failure) { throw new IllegalStateException("Topology metadata budget exhausted",failure); }
                var transfer=ownership.prepareTopologyTransfer(sources,targets); pendingTopologySeed=new PendingTopologySeed(candidate,ordered,List.copyOf(history),Map.copyOf(next),transfer); return forceTopologySeed(pendingTopologySeed);
            });
        } finally { nativeMutation.release(); }
    }
    private NativeTopologySeed forceTopologySeed(PendingTopologySeed pending) throws RocksDBException {
        if(pendingLearnerHistory!=null)throw new IllegalStateException("Unknown learner history requires exact original retry");
        var seed=pending.seed(); var db=loaded.db().get(); var state=handles.get(STATE_COLUMN);
        try(var batch=new org.rocksdb.WriteBatch();var options=new org.rocksdb.WriteOptions().setDisableWAL(false).setSync(true)) {
            batch.put(state,TOPOLOGY_SEEDS_KEY,encodeTopologySeeds(pending.seeds())); batch.put(state,OWNERSHIP_HISTORY_KEY,encodeOwnershipHistory(pending.history()));
            for(var parent:seed.definition().parents()) batch.put(state,groupSchemaKey(parent.groupId(),0x62),seed.digest());
            for(var target:seed.definition().targets()) { batch.put(state,groupSchemaKey(target.groupId(),0x61),seed.birth(target.groupId()).encode()); batch.put(state,groupSchemaKey(target.groupId(),0x10),ReplicatedRangeStore.topologyVirginUsage()); }
            db.flushWal(true); topologySeedForceObserver.boundary(false); db.write(options,batch); topologySeedForceObserver.boundary(true);
            for(var parent:pending.parents()) parent.publishTopologyRetirement(seed);
            pending.transfer().publish(); topologySeeds=pending.seeds(); ownershipHistory=pending.history(); ownershipHistoryDurable=true;
            var births=new HashMap<>(topologyBirths); var retirements=new HashMap<>(topologyRetirements);
            for(var target:seed.definition().targets()) births.put(target.groupId(),seed.birth(target.groupId()));
            for(var parent:pending.parents()) { retirements.put(parent.ownership().owner().groupId(),seed); parent.publishTopologyRetirement(seed); }
            topologyBirths=Map.copyOf(births); topologyRetirements=Map.copyOf(retirements); pendingTopologySeed=null; return seed;
        } catch(IOException failure) { throw new IllegalStateException("Cannot encode topology metadata",failure); }
    }
    /** Opens only retained parent metadata, independent of historic USER CF availability. */
    public ReplicatedRangeStore openRetiredTopologyRange(UUID group) throws RocksDBException {
        Objects.requireNonNull(group);nativeMutation.acquireUninterruptibly();
        try {
            synchronized(admission) { if(closing)throw new IllegalStateException("Pool is closing"); }
            if(pendingTopologySeed!=null)throw new IllegalStateException("Unknown topology seed completion requires exact retry");
            var seed=topologyRetirements.get(group);if(seed==null)throw new IllegalArgumentException("Group has no durable topology retirement");requireTopologySeedRows(seed);
            var descriptor=seed.definition().parents().stream().filter(range->range.groupId().equals(group)).findFirst().orElseThrow();var owner=new RangeOwnership.Owner(group,descriptor.rangeEpoch());
            for(var existing:ranges)if(existing.ownership().owner().equals(owner)) { if(existing.detachRetiredUserHandles()) { existing.configureRetiredTopologyDirectory(seed.definition().sourceDirectory());drainDeferredDrops(); }return existing; }
            var registered=ownership.get(owner);if(registered.state()!=RangeOwnership.State.RETIRED || !registered.interval().equals(topologyInterval(seed,descriptor)))throw new IllegalArgumentException("Retired ownership history changed");
            var store=new ReplicatedRangeStore(loaded.db().get(),ownership,owner,seed.header().schemaDigest(),Map.of(),handles.get(STATE_COLUMN),handles.get(RECEIPTS_COLUMN),seed.mergeColumns(),true);
            store.configureSnapshotAdmission(snapshotAdmission);store.configureSchemaPreparationPool(this);store.validateTopologyActivationRecovery();store.configureRetiredTopologyDirectory(seed.definition().sourceDirectory());ranges.add(store);drainDeferredDrops();return store;
        } finally { nativeMutation.release(); }
    }
    /** Releases only this parent's borrowed USER references after actual native users/pins drain. */
    public boolean detachRetiredUserHandles(ReplicatedRangeStore store) throws RocksDBException {
        Objects.requireNonNull(store);nativeMutation.acquireUninterruptibly();
        try { synchronized(admission) { if(closing)throw new IllegalStateException("Pool is closing"); }if(!ranges.contains(store))throw new IllegalArgumentException("Range is not owned by this pool");var seed=topologyRetirements.get(store.ownership().owner().groupId());if(seed==null)throw new IllegalArgumentException("Range has no topology retirement");requireTopologySeedRows(seed);boolean detached=store.detachRetiredUserHandles();if(detached) { store.configureRetiredTopologyDirectory(seed.definition().sourceDirectory());drainDeferredDrops(); }return detached; }
        finally { nativeMutation.release(); }
    }
    public ReplicatedRangeStore reopenTopologyRange(NativeTopologyBirth birth,DistributedSchema schema) throws RocksDBException {
        Objects.requireNonNull(birth); Objects.requireNonNull(schema); var seed=topologySeeds.get(birth.intentId()); if(seed==null) throw new IllegalArgumentException("Topology birth artifact missing"); birth.requireSeed(seed); schema.requireMatches(seed.definition().targetDirectory()); var range=birth.resolveDescriptor(seed);
        var store=openRange(new RangeOwnership.Owner(range.groupId(),range.rangeEpoch()),topologyInterval(seed,range),schema.digest(),seed.physicalNames(),seed.mergeColumns());
        if(!Arrays.equals(store.schemaDigest(),schema.digest())) { var checkpoint=recoverSchema(range.groupId()); if(checkpoint.current()==null || !Arrays.equals(store.schemaDigest(),checkpoint.current().definition().targetSchema().digest())) throw new IllegalArgumentException("Topology child lacks verified current schema"); schema=checkpoint.current().definition().targetSchema(); }
        store.enableLogicalSchema(schema); return store;
    }

    private byte[] encodeOwnershipHistory(List<HistoricalOwner> records) throws IOException {
        long size=8;
        for(var record:records) { size+=52L+16L*record.interval().columns().size(); for(var bound:new byte[][]{record.interval().lowerInclusive(),record.interval().upperExclusive()}) { if(bound!=null && bound.length>RangeDirectoryCodec.MAX_ENCODED_BYTES)throw new IOException("Ownership bound exceeds directory capacity"); size+=bound==null?0:bound.length; } if(size>MAX_OWNERSHIP_BYTES)throw new IOException("Native ownership history byte capacity exhausted"); }
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(0x4f574e48); out.writeInt(records.size());
        for(var record:records) {
            out.writeLong(record.owner().groupId().getMostSignificantBits()); out.writeLong(record.owner().groupId().getLeastSignificantBits()); out.writeLong(record.owner().epoch());
            out.writeLong(record.interval().domainId().getMostSignificantBits()); out.writeLong(record.interval().domainId().getLeastSignificantBits());
            var columns=record.interval().columns().stream().sorted(java.util.Comparator.comparing(UUID::toString)).toList(); out.writeInt(columns.size()); for(var column:columns) { out.writeLong(column.getMostSignificantBits()); out.writeLong(column.getLeastSignificantBits()); }
            for(var bound:new byte[][]{record.interval().lowerInclusive(),record.interval().upperExclusive()}) { if(bound!=null && bound.length>RangeDirectoryCodec.MAX_ENCODED_BYTES)throw new IOException("Ownership bound exceeds directory capacity"); out.writeInt(bound==null?-1:bound.length); if(bound!=null)out.write(bound); }
        }
        if(bytes.size()>MAX_OWNERSHIP_BYTES)throw new IOException("Native ownership history byte capacity exhausted"); return bytes.toByteArray();
    }
    private void recoverOwnershipHistory() {
        try {
            byte[] bytes=loaded.db().get().get(handles.get(STATE_COLUMN),OWNERSHIP_HISTORY_KEY); if(bytes==null)return;
            if(bytes.length>MAX_OWNERSHIP_BYTES)throw new IOException("Ownership history exceeds bound"); var in=new DataInputStream(new ByteArrayInputStream(bytes)); if(in.readInt()!=0x4f574e48)throw new IOException("Invalid ownership history"); int count=in.readInt(); if(count<0 || count>MAX_OWNERSHIP_HISTORY)throw new IOException("Ownership history count bound");
            var records=new ArrayList<HistoricalOwner>(); var unique=new HashSet<HistoricalOwner>();
            for(int i=0;i<count;i++) {
                var owner=new RangeOwnership.Owner(new UUID(in.readLong(),in.readLong()),in.readLong()); UUID domain=new UUID(in.readLong(),in.readLong()); int n=in.readInt(); if(n<0 || n>4096)throw new IOException("Ownership columns exceed bound"); var columns=new HashSet<UUID>(); String previous=null;
                for(int c=0;c<n;c++) { UUID column=new UUID(in.readLong(),in.readLong()); if(previous!=null && previous.compareTo(column.toString())>=0 || !columns.add(column))throw new IOException("Noncanonical ownership columns"); previous=column.toString(); }
                byte[][] bounds=new byte[2][]; for(int b=0;b<2;b++) { int size=in.readInt(); if(size==-1)continue; if(size<0 || size>RangeDirectoryCodec.MAX_ENCODED_BYTES || size>in.available())throw new IOException("Ownership bound exceeds capacity"); bounds[b]=in.readNBytes(size); }
                var record=new HistoricalOwner(owner,new RangeOwnership.Interval(domain,columns,bounds[0],bounds[1])); if(!unique.add(record))throw new IOException("Duplicate ownership history"); records.add(record);
            }
            if(in.available()!=0 || !Arrays.equals(bytes,encodeOwnershipHistory(records)))throw new IOException("Noncanonical ownership history"); ownershipHistory=List.copyOf(records);
        } catch(IOException|RocksDBException|IllegalArgumentException failure) { throw new IllegalStateException("Cannot recover native ownership history",failure); }
    }
    private List<HistoricalOwner> proposedOwnershipHistory(RangeOwnership.Owner owner,RangeOwnership.Interval interval) {
        if(pendingLearnerHistory!=null && !pendingLearnerHistory.equals(owner.groupId()))throw new IllegalStateException("Unknown learner history requires exact original retry");
        var next=new ArrayList<>(ownershipHistory); int match=-1;
        for(int i=0;i<next.size();i++) { var existing=next.get(i); if(existing.owner().equals(owner) && existing.interval().domainId().equals(interval.domainId()) && Arrays.equals(existing.interval().lowerInclusive(),interval.lowerInclusive()) && Arrays.equals(existing.interval().upperExclusive(),interval.upperExclusive())) { match=i; break; } }
        if(match>=0) { var columns=new HashSet<>(next.get(match).interval().columns()); columns.addAll(interval.columns()); interval=new RangeOwnership.Interval(interval.domainId(),columns,interval.lowerInclusive(),interval.upperExclusive()); }
        var record=new HistoricalOwner(owner,interval); if(match<0)next.add(record); else next.set(match,record);
        if(next.size()>MAX_OWNERSHIP_HISTORY)throw new IllegalStateException("Native ownership history count exhausted");
        return List.copyOf(next);
    }
    private void retainOwnershipHistory(RangeOwnership.Owner owner,RangeOwnership.Interval interval) throws RocksDBException {
        if(pendingLearnerHistory!=null)throw new IllegalStateException("Unknown learner history requires exact original retry");
        if(pendingTopologySeed!=null) throw new IllegalStateException("Unknown topology seed completion requires exact retry");
        var next=proposedOwnershipHistory(owner,interval);
        try { byte[] encoded=encodeOwnershipHistory(next); if(next.equals(ownershipHistory) && ownershipHistoryDurable)return; ownershipHistory=next; ownershipHistoryDurable=false; try(var batch=new org.rocksdb.WriteBatch();var options=new org.rocksdb.WriteOptions().setDisableWAL(false).setSync(true)) { batch.put(handles.get(STATE_COLUMN),OWNERSHIP_HISTORY_KEY,encoded); for(var pending:newImportReservations.values())batch.put(handles.get(STATE_COLUMN),groupSchemaKey(pending.groupId(),0x40),pending.encode()); if(!newImportReservations.isEmpty())batch.put(handles.get(STATE_COLUMN),BIRTHS_KEY,encodeBirthRegistry(bindings)); loaded.db().get().write(options,batch); } ownershipHistoryDurable=true; if(!newImportReservations.isEmpty())birthsDurable=true; }
        catch(IOException failure) { throw new IllegalStateException("Native ownership history exhausted",failure); }
    }
    private void forceNewImportBirth(NativeSstImportManifest manifest,RangeOwnership.Owner owner,RangeOwnership.Interval interval) throws RocksDBException {
        if(pendingLearnerHistory!=null)throw new IllegalStateException("Unknown learner history requires exact original retry");
        var next=proposedOwnershipHistory(owner,interval); var proposedBindings=proposedBirths(manifest.physicalNames(),manifest.mergeColumns(),BOOTSTRAP_BIRTH,Set.of()); final byte[] history,births;
        try { history=encodeOwnershipHistory(next); births=encodeBirthRegistry(proposedBindings); } catch(IOException invalid) { throw new IllegalStateException("Native import ownership/binding capacity exhausted",invalid); }
        boolean reserved=newImportReservations.containsKey(owner.groupId());
        if(!reserved) { ownership.register(owner,interval,RangeOwnership.State.FENCED); newImportReservations.put(owner.groupId(),manifest); }
        else { var held=ownership.get(owner); if(held.state()!=RangeOwnership.State.FENCED || !held.interval().equals(interval))throw new IllegalStateException("Unpublished import reservation changed"); }
        ownershipHistory=next; ownershipHistoryDurable=false; bindings.clear(); bindings.putAll(proposedBindings); birthsDurable=false;
        try(var batch=new org.rocksdb.WriteBatch();var options=new org.rocksdb.WriteOptions().setDisableWAL(false).setSync(true)) {
            batch.put(handles.get(STATE_COLUMN),OWNERSHIP_HISTORY_KEY,history); batch.put(handles.get(STATE_COLUMN),BIRTHS_KEY,births); for(var pending:newImportReservations.values())batch.put(handles.get(STATE_COLUMN),groupSchemaKey(pending.groupId(),0x40),pending.encode());
            importBirthForceObserver.boundary(false); loaded.db().get().write(options,batch); importBirthForceObserver.boundary(true); ownershipHistoryDurable=true; birthsDurable=true;
        }
    }
    private void requireVirginHistory(RangeOwnership.Interval interval) throws RocksDBException {
        for(var record:ownershipHistory) { var previous=record.interval(); if(java.util.Collections.disjoint(interval.columns(),previous.columns()))continue;
            if((interval.upperExclusive()==null || previous.lowerInclusive()==null || Arrays.compareUnsigned(previous.lowerInclusive(),interval.upperExclusive())<0) && (previous.upperExclusive()==null || interval.lowerInclusive()==null || Arrays.compareUnsigned(interval.lowerInclusive(),previous.upperExclusive())<0))throw new IllegalStateException("NEW import overlaps historical native ownership");
        }
        var known=new HashSet<UUID>(); ownershipHistory.forEach(record->known.add(record.owner().groupId()));
        for(var cf:List.of(handles.get(STATE_COLUMN),handles.get(RECEIPTS_COLUMN)))try(var iterator=loaded.db().get().newIterator(cf)) { for(iterator.seekToFirst();iterator.isValid();iterator.next()) { byte[] key=iterator.key(); if(key.length<16 || key.length==OWNERSHIP_HISTORY_KEY.length && Arrays.equals(key,OWNERSHIP_HISTORY_KEY))continue;
                boolean groupKey=cf==handles.get(RECEIPTS_COLUMN) || key.length==16 || key.length>16 && key[16]>=1 && key[16]<=0x7f;
                if(groupKey) { var b=ByteBuffer.wrap(key); if(!known.contains(new UUID(b.getLong(),b.getLong())))throw new IllegalStateException("Unknown preexisting native group lacks ownership-history authority"); }
            } iterator.status(); }
    }
    private static final byte[] CAPACITY_KEY="SCHEMA-CAPACITY".getBytes(StandardCharsets.US_ASCII);
    private volatile Map<UUID,SchemaCapacityClaims.Claim> capacityClaims=Map.of();
    private volatile boolean capacityDurable=true;
    private volatile Map<UUID,SchemaCapacityClaims.Claim> uncertainCapacityClaims=Map.of();
    private volatile SchemaCapacityForceObserver capacityForceObserver=(after) -> {};
    @FunctionalInterface public interface SchemaCapacityForceObserver { void boundary(boolean afterPersistence) throws RocksDBException; }
    public void schemaCapacityForceObserver(SchemaCapacityForceObserver observer) { capacityForceObserver=Objects.requireNonNull(observer); }

    private final Map<String,RocksDBObjects> columnResources=new HashMap<>();
    private final List<RocksDBObjects> quarantinedColumnResources=new ArrayList<>();
    private final class PreparedColumn implements AutoCloseable {
        final String name; final RocksDBObjects resources=new RocksDBObjects(); final RocksDBLoader.ColumnOptionsWithMerge options;
        boolean adopted;
        PreparedColumn(String name) { this(name,artifacts.qualified() ? artifacts::resolve : null); }
        PreparedColumn(String name,RocksDBLoader.OperatorResolver resolver) {
            this.name=name;
            try { options=RocksDBLoader.getColumnOptions(name,loaded.path(),loaded.definitiveDbPath(),globalConfig,logger,resources,false,loaded.caches(),resolver); }
            catch(RuntimeException|Error failure) { resources.close(); throw failure; }
        }
        @Override public void close() { if(!adopted) resources.close(); }
    }
    private ColumnFamilyHandle createColumn(PreparedColumn prepared) throws RocksDBException {
        handleLifetime.writeLock().lock();
        try {
            var handle=loaded.db().get().createColumnFamily(new ColumnFamilyDescriptor(prepared.name.getBytes(StandardCharsets.US_ASCII),prepared.options.options()));
            newHandles.add(handle); handles.put(prepared.name,handle); columnResources.put(prepared.name,prepared.resources); prepared.adopted=true;
            loaded.definitiveColumnFamilyOptionsMap().put(prepared.name,prepared.options.options()); loaded.mergeOperators().put(prepared.name,prepared.options.mergeOperator()); return handle;
        } finally { handleLifetime.writeLock().unlock(); }
    }
    private static Map<UUID,Binding> decodeBirths(byte[] encoded) throws IOException {
        if(encoded.length>MAX_BINDING_BYTES) throw new IOException("Birth registry exceeds budget");
        var result=new HashMap<UUID,Binding>(); var input=new DataInputStream(new ByteArrayInputStream(encoded)); int count=input.readInt();
        if(count<0 || count>MAX_BINDINGS) throw new IOException("Birth registry count exceeds budget");
        var names=new HashSet<String>();
        for(int i=0;i<count;i++) {
            var id=new UUID(input.readLong(),input.readLong()); var birth=new UUID(input.readLong(),input.readLong());
            int kind=input.readUnsignedByte(); if(kind<1 || kind>BirthKind.values().length) throw new IOException("Invalid CF birth kind");
            var birthKind=BirthKind.values()[kind-1]; boolean merge=input.readBoolean(); String name=input.readUTF();
            if(name.isEmpty() || name.length()>4096 || RESERVED_COLUMNS.contains(name) || !StandardCharsets.US_ASCII.newEncoder().canEncode(name) || name.indexOf('\0')>=0 || !names.add(name) || result.put(id,new Binding(name,merge,birthKind,birth))!=null) throw new IOException("Invalid birth identity");
        }
        if(input.available()!=0) throw new IOException("Trailing birth registry bytes"); return result;
    }
    private void recoverBirths() {
        try {
            var encoded=loaded.db().get().get(handles.get(STATE_COLUMN),BIRTHS_KEY); if(encoded==null) return;
            bindings.putAll(decodeBirths(encoded));
            if(artifacts.qualified()) for(var entry:bindings.entrySet()) artifacts.requireBirth(entry.getKey(),entry.getValue().name(),entry.getValue().mergeAdmitted());
        } catch(IOException|RocksDBException failure) { throw new IllegalStateException("Cannot recover native CF births",failure); }
    }
    /** Reads forced metadata, including WAL, without installing user callbacks or enabling native writes. */
    private static void preflightNativeBindings(Path path,DatabaseConfig config,DistributedOperatorArtifacts artifacts) throws IOException,RocksDBException {
        org.rocksdb.RocksDB.loadLibrary();
        List<byte[]> names; try(var listing=new org.rocksdb.Options()) { names=org.rocksdb.RocksDB.listColumnFamilies(listing,path.toString()); }
        if(names.size()>MAX_BINDINGS+RESERVED_COLUMNS.size()) throw new IOException("Native CF count exceeds budget");
        boolean hasState=names.stream().anyMatch(name -> Arrays.equals(name,STATE_COLUMN.getBytes(StandardCharsets.US_ASCII)));
        if(!hasState) { if(artifacts.qualified()) throw new IOException("Qualified native pool lacks STATE"); return; }
        try(var refs=new RocksDBObjects(); var dbOptions=RocksDBLoader.readOnlyOptions(path,config)) {
            dbOptions.setWalRecoveryMode(org.rocksdb.WALRecoveryMode.AbsoluteConsistency);
            var descriptors=new ArrayList<ColumnFamilyDescriptor>(); var nativeHandles=new ArrayList<ColumnFamilyHandle>();
            // Read-only subsets replay only the opened metadata families, avoiding USER MERGE callbacks.
            // The complete on-disk listing still drives exact live-CF birth/operator coverage below.
            for(var name:names){String physical=new String(name,StandardCharsets.US_ASCII);if(physical.equals("default") || physical.equals(STATE_COLUMN))descriptors.add(new ColumnFamilyDescriptor(name,RocksDBLoader.readOnlyColumnOptions(physical,path,config,refs)));}
            try(var db=org.rocksdb.RocksDB.openReadOnly(dbOptions,path.toString(),descriptors,nativeHandles)) {
                try {
                    ColumnFamilyHandle state=null; for(var handle:nativeHandles) if(Arrays.equals(handle.getName(),STATE_COLUMN.getBytes(StandardCharsets.US_ASCII))) state=handle;
                    if(state==null) throw new IOException("Read-only native STATE missing");
                    var births=new HashMap<UUID,Binding>();
                    var buffer=new byte[Math.max(SchemaCapacityClaims.MAX_BYTES,Math.max(MAX_BINDING_BYTES,NativeSchemaInstallation.MAX_BYTES))];
                    int birthLength=db.get(state,BIRTHS_KEY,buffer);
                    if(birthLength!=org.rocksdb.RocksDB.NOT_FOUND) { if(birthLength>MAX_BINDING_BYTES) throw new IOException("Native birth metadata exceeds budget"); births.putAll(decodeBirths(Arrays.copyOf(buffer,birthLength))); }
                    if(artifacts.qualified()) for(var entry:births.entrySet()) artifacts.requireBirth(entry.getKey(),entry.getValue().name(),entry.getValue().mergeAdmitted());
                    int capacityLength=db.get(state,CAPACITY_KEY,buffer);
                    if(capacityLength!=org.rocksdb.RocksDB.NOT_FOUND) {
                        if(capacityLength>SchemaCapacityClaims.MAX_BYTES) throw new IOException("Capacity registry exceeds budget");
                        artifacts.validateCapacity(SchemaCapacityClaims.decode(Arrays.copyOf(buffer,capacityLength)).values(),plainBirths(births));
                    }
                    var live=new HashSet<String>(); for(var name:names) live.add(new String(name,StandardCharsets.US_ASCII));
                    var key=new byte[34];
                    try(var iterator=db.newIterator(state)) {
                        iterator.seekToFirst();
                        while(iterator.isValid()) {
                            int length=iterator.key(key); if(length<16) { iterator.next(); continue; }
                            byte[] group=Arrays.copyOf(key,16); byte[] currentBytes=null; NativeSchemaInstallation current=null;
                            for(int tag:List.of(0x28,0x29)) {
                                byte[] nativeKey=Arrays.copyOf(group,17); nativeKey[16]=(byte)tag;
                                int size=db.get(state,nativeKey,buffer); if(size==org.rocksdb.RocksDB.NOT_FOUND) continue;
                                if(size>NativeSchemaInstallation.MAX_BYTES) throw new IOException("Native schema record exceeds budget");
                                byte[] encoded=Arrays.copyOf(buffer,size); var record=NativeSchemaInstallation.decode(encoded);
                                if(tag==0x28) { currentBytes=encoded; current=record; }
                                if(!artifacts.qualified()) {
                                    boolean neededOperator=record.definition().sourceSchema().columns().stream().anyMatch(column -> column.operator()!=null && live.contains(record.sourceNames().get(column.columnId())))
                                            || record.definition().targetSchema().columns().stream().anyMatch(column -> column.operator()!=null);
                                    if(neededOperator) throw new IOException("Operator-bearing native schema lacks trusted cache");
                                } else {
                                    artifacts.requireSchemaBindings(record.definition().sourceSchema(),record.sourceNames(),live);
                                    artifacts.requireSchemaBindings(record.definition().targetSchema(),record.targetNames());
                                    for(var column:record.definition().targetSchema().columns()) if(column.operator()!=null) artifacts.require(column.operator());
                                }
                            }
                            byte[] historyPrefix=Arrays.copyOf(group,17); historyPrefix[16]=0x2a; iterator.seek(historyPrefix);
                            if(iterator.isValid()) {
                                int found=iterator.key(key);
                                if(found>=17 && Arrays.equals(key,0,17,historyPrefix,0,17)) {
                                    if(current==null) throw new IOException("Native schema history lacks current generation");
                                    var change=current.definition().changeId(); byte[] original=ByteBuffer.allocate(33).put(historyPrefix).putLong(change.getMostSignificantBits()).putLong(change.getLeastSignificantBits()).array();
                                    int size=db.get(state,original,buffer);
                                    if(size==org.rocksdb.RocksDB.NOT_FOUND || size>NativeSchemaInstallation.MAX_BYTES || !Arrays.equals(currentBytes,Arrays.copyOf(buffer,size))) throw new IOException("Native current schema differs from immutable history");
                                } else if(current!=null) throw new IOException("Native current schema lacks immutable history");
                            } else if(current!=null) throw new IOException("Native current schema lacks immutable history");
                            // Advance one UUID prefix, avoiding unrelated session/ACK/floor records.
                            int offset=15; while(offset>=0 && (group[offset]&255)==255) group[offset--]=0;
                            if(offset<0) break; group[offset]++; iterator.seek(group);
                        }
                        iterator.status();
                    }
                    if(artifacts.qualified()) for(var name:names) {
                        String physical=new String(name,StandardCharsets.US_ASCII); if(RESERVED_COLUMNS.contains(physical)) continue;
                        if(!artifacts.contains(physical) || births.values().stream().noneMatch(birth -> birth.name().equals(physical))) throw new IOException("Live native CF lacks exact birth binding: "+physical+" (artifact="+artifacts.contains(physical)+", birth="+births.values().stream().anyMatch(birth->birth.name().equals(physical))+")");
                    }
                } finally { nativeHandles.forEach(ColumnFamilyHandle::close); }
            }
        }
    }
    private Map<UUID,Binding> proposedBirths(Map<UUID,String> names,Set<UUID> merges,UUID change,Set<UUID> fresh) {
        var next=new HashMap<>(bindings);
        for(var entry:names.entrySet()) {
            var old=next.get(entry.getKey()); var name=entry.getValue();
            if(old!=null && (!old.name().equals(name) || old.mergeAdmitted()!=merges.contains(entry.getKey()) || fresh.contains(entry.getKey()) && (old.birthKind()!=BirthKind.CREATE || !old.birthChange().equals(change)))) throw new IllegalArgumentException("Column birth changed");
            if(next.entrySet().stream().anyMatch(other -> !other.getKey().equals(entry.getKey()) && other.getValue().name().equals(name))) throw new IllegalArgumentException("Physical CF name is already reserved");
            if(fresh.contains(entry.getKey()) && old==null && handles.containsKey(name)) throw new IllegalArgumentException("CREATE cannot adopt an unbound physical CF");
            next.putIfAbsent(entry.getKey(),new Binding(name,merges.contains(entry.getKey()),fresh.contains(entry.getKey()) ? BirthKind.CREATE : BirthKind.BOOTSTRAP,fresh.contains(entry.getKey()) ? change : BOOTSTRAP_BIRTH));
        }
        validateBirthCapacity(next,chargedCapacityClaims());
        if(next.size()>MAX_BINDINGS) throw new IllegalStateException("Native CF birth budget exhausted");
        long bytes=4; for(var binding:next.values()) bytes+=36L+binding.name().length();
        if(bytes>MAX_BINDING_BYTES) throw new IllegalStateException("Native CF birth byte budget exhausted"); return next;
    }
    private static Map<UUID,String> plainBirths(Map<UUID,Binding> births) {
        var result=new HashMap<UUID,String>(); for(var entry:births.entrySet()) if(!entry.getValue().mergeAdmitted()) result.put(entry.getKey(),entry.getValue().name()); return result;
    }
    private void validateBirthCapacity(Map<UUID,Binding> actual,Map<UUID,SchemaCapacityClaims.Claim> claims) {
        var combined=new HashMap<>(actual);
        for(var claim:claims.values()) if(claim.outstanding()) for(var entry:claim.targetNames().entrySet()) {
            var expected=new Binding(entry.getValue(),claim.targetMerges().contains(entry.getKey())); var previous=combined.putIfAbsent(entry.getKey(),expected);
            if(previous!=null && (!previous.name().equals(expected.name()) || previous.mergeAdmitted()!=expected.mergeAdmitted()) || combined.entrySet().stream().anyMatch(other -> !other.getKey().equals(entry.getKey()) && other.getValue().name().equals(entry.getValue()))) throw new IllegalArgumentException("Column is reserved by another durable schema admission");
        }
        long bytes=4; for(var binding:combined.values()) bytes+=36L+binding.name().length();
        if(combined.size()>MAX_BINDINGS || bytes>MAX_BINDING_BYTES) throw new IllegalStateException("Reserved native birth capacity exhausted");
    }
    private void recoverCapacityClaims() {
        try {
            byte[] bytes=loaded.db().get().get(handles.get(STATE_COLUMN),CAPACITY_KEY); if(bytes==null) return;
            capacityClaims=SchemaCapacityClaims.decode(bytes); validateBirthCapacity(bindings,capacityClaims); artifacts.validateCapacity(capacityClaims.values(),plainBirths(bindings));
            for(var claim:capacityClaims.values()) for(var entry:claim.groups().entrySet()) if(entry.getValue().state()==SchemaCapacityClaims.State.CONSUMED || entry.getValue().state()==SchemaCapacityClaims.State.RETIRED) {
                byte[] key=ByteBuffer.allocate(33).putLong(entry.getKey().getMostSignificantBits()).putLong(entry.getKey().getLeastSignificantBits()).put((byte)0x2a).putLong(claim.definition().changeId().getMostSignificantBits()).putLong(claim.definition().changeId().getLeastSignificantBits()).array();
                var installed=loaded.db().get().get(handles.get(STATE_COLUMN),key); if(installed==null) throw new IOException("Consumed capacity lacks original INSTALL");
                var record=NativeSchemaInstallation.decode(installed); record.requireDefinition(claim.definition());
                claim.requireSame(new SchemaCapacityClaims.Claim(record.definition(),record.sourceNames(),record.targetNames(),record.sourceMerges(),record.targetMerges(),Map.of()));
            }
        } catch(IOException|RocksDBException failure) { throw new IllegalStateException("Cannot recover schema capacity admissions",failure); }
    }
    private Map<UUID,SchemaCapacityClaims.Claim> chargedCapacityClaims() { return capacityDurable ? capacityClaims : uncertainCapacityClaims; }
    private void forceCapacityClaims(Map<UUID,SchemaCapacityClaims.Claim> proposed) throws RocksDBException {
        byte[] bytes=SchemaCapacityClaims.encode(proposed);
        // Unknown completion protects the union in memory; same retry always re-forces before acknowledgement.
        var protectedClaims=new HashMap<>(chargedCapacityClaims());
        for(var entry:proposed.entrySet()) {
            var old=protectedClaims.get(entry.getKey()); var next=entry.getValue();
            if(old!=null) for(var group:old.groups().entrySet()) if(group.getValue().outstanding()) {
                var replacement=next.groups().get(group.getKey()); if(replacement==null || !replacement.outstanding()) next=next.group(group.getKey(),group.getValue());
            }
            protectedClaims.put(entry.getKey(),next);
        }
        uncertainCapacityClaims=Map.copyOf(protectedClaims); capacityDurable=false; capacityClaims=Map.copyOf(proposed);
        capacityForceObserver.boundary(false);
        try(var options=new org.rocksdb.WriteOptions().setSync(true).setDisableWAL(false)) { loaded.db().get().put(handles.get(STATE_COLUMN),options,CAPACITY_KEY,bytes); }
        capacityForceObserver.boundary(true); capacityDurable=true; uncertainCapacityClaims=Map.of();
    }
    List<NativeSchemaCapacityReservation> outstandingCapacity(UUID group,UUID except) {
        var proofs=new ArrayList<NativeSchemaCapacityReservation>();
        for(var claim:chargedCapacityClaims().values()) if(!claim.definition().changeId().equals(except)) { var member=claim.groups().get(group); if(member!=null && member.outstanding()) proofs.add(member.proof()); }
        return List.copyOf(proofs);
    }
    void validateSnapshotCapacity(ReplicatedRangeStore store,Path image,NativeRangeImage.Limits limits) throws IOException,RocksDBException { validateSnapshotCapacity(store,image,limits,null); }
    void validateSnapshotCapacity(ReplicatedRangeStore store,Path image,NativeRangeImage.Limits limits,Map<String,byte[]> localSchemaRows) throws IOException,RocksDBException {
        UUID group=store.ownership().owner().groupId(); var claims=chargedCapacityClaims();
        var expected=new HashMap<UUID,byte[]>(); int reservedCount=0; long reservedBytes=0;
        for(var claim:claims.values()) {
            var member=claim.groups().get(group); if(member==null) continue;
            if(member.outstanding()) { reservedCount++; reservedBytes=Math.addExact(reservedBytes,member.proof().historyBytes()); }
            else if(member.state()==SchemaCapacityClaims.State.CONSUMED || member.state()==SchemaCapacityClaims.State.RETIRED) {
                byte[] original=store.schemaCapacityInstalledRow(claim.definition().changeId());
                if(original==null) throw new IOException("Consumed schema capacity lacks original INSTALL"); expected.put(claim.definition().changeId(),original);
            }
        }
        if(reservedCount==0 && expected.isEmpty()) return;
        int[] installedCount={0}; long[] bytes={reservedBytes}; var retained=new HashSet<UUID>();
        NativeRangeImage.visit(image,limits,(section,column,key,value) -> {
            if(section!=NativeRangeImage.Section.STATE) return;
            value=ReplicatedRangeStore.topologySchemaValue(localSchemaRows,key,value);
            if(key.length==17 && (key[16]==0x28 || key[16]==0x2c) || key.length==33 && (key[16]==0x2a || key[16]==0x2b)) bytes[0]=Math.addExact(bytes[0],value.length);
            if(key.length==33 && key[16]==0x2a) {
                installedCount[0]++; var id=new UUID(ByteBuffer.wrap(key,17,16).getLong(),ByteBuffer.wrap(key,25,8).getLong()); var original=expected.get(id);
                if(original!=null) { if(!Arrays.equals(original,value)) throw new IOException("Snapshot changes original capacity INSTALL evidence"); retained.add(id); }
            }
        });
        if(installedCount[0]+reservedCount>64 || bytes[0]>16L*1024*1024) throw new IOException("Snapshot would consume outstanding schema capacity");
        if(!retained.containsAll(expected.keySet())) throw new IOException("Snapshot discards original capacity INSTALL evidence");
    }
    void requireCapacityFence(UUID group,SchemaChange.Header header) {
        var claim=capacityClaims.get(header.changeId()); if(claim==null) return;
        var member=claim.groups().get(group); if(member==null) throw new IllegalStateException("Group lacks original capacity admission"); member.proof().requireHeader(header);
        if(member.state()==SchemaCapacityClaims.State.ABORTED || member.state()==SchemaCapacityClaims.State.RETIRED) throw new IllegalStateException("Schema capacity admission was authoritatively released");
        if(!capacityDurable) throw new IllegalStateException("Capacity admission force is unresolved");
    }
    public NativeSchemaCapacityReservation reserveSchemaCapacity(ReplicatedRangeStore store,SchemaChange.Definition definition,Map<UUID,String> targetNames,Set<UUID> targetMerges) throws RocksDBException {
        nativeMutation.acquireUninterruptibly();
        try {
            synchronized(admission) { if(closing) throw new IllegalStateException("Pool is closing"); }
            if(!ranges.contains(store)) throw new IllegalArgumentException("Foreign range store");
            return store.readSchemaMetadata(() -> {
                var group=store.ownership().owner().groupId(); var previous=capacityClaims.get(definition.changeId());
                var sourceNames=new HashMap<UUID,String>(); for(var id:definition.sourceSchema().columnIds()) sourceNames.put(id,Objects.requireNonNull(bindings.get(id)).name());
                var candidate=new SchemaCapacityClaims.Claim(definition,sourceNames,targetNames,previous!=null && previous.groups().containsKey(group) ? previous.sourceMerges() : store.deterministicMergeColumns(),targetMerges,Map.of());
                if(previous!=null) { previous.requireSame(candidate); var original=previous.groups().get(group); if(original!=null) { if(!capacityDurable) forceCapacityClaims(capacityClaims); return original.proof(); } }
                store.preflightSchemaCapacity(definition,candidate.installBytes());
                validateCapacityOptions(candidate);
                var proof=new NativeSchemaCapacityReservation(definition.header(),group,candidate.bindingDigest(),candidate.historyBytes());
                var next=new HashMap<>(capacityClaims); var claim=previous==null ? candidate : previous;
                next.put(definition.changeId(),claim.group(group,new SchemaCapacityClaims.Group(proof,SchemaCapacityClaims.State.ACTIVE)));
                SchemaCapacityClaims.encode(next); validateBirthCapacity(bindings,next);
                try { artifacts.validateCapacity(next.values(),plainBirths(bindings)); } catch(IOException failure) { throw new IllegalStateException("Schema operator capacity qualification failed",failure); }
                forceCapacityClaims(next); return proof;
            });
        } finally { nativeMutation.release(); }
    }
    private void validateCapacityOptions(SchemaCapacityClaims.Claim claim) {
        var definition=claim.definition();
        if(!claim.targetNames().keySet().equals(definition.targetSchema().columnIds()) || !claim.targetNames().keySet().containsAll(claim.targetMerges()) || new HashSet<>(claim.targetNames().values()).size()!=claim.targetNames().size()) throw new IllegalArgumentException("Capacity target physical coverage changed");
        for(var name:claim.targetNames().values()) if(name.isEmpty() || name.length()>4096 || name.indexOf('\0')>=0 || RESERVED_COLUMNS.contains(name) || !StandardCharsets.US_ASCII.newEncoder().canEncode(name)) throw new IllegalArgumentException("Invalid capacity target name");
        boolean qualified=artifacts.qualified() || definition.targetSchema().columns().stream().anyMatch(column -> column.operator()!=null);
        if(qualified) {
            if(!artifacts.qualified()) for(var name:claim.sourceNames().values()) if(loaded.mergeOperators().get(name)!=null) throw new IllegalArgumentException("Configured source operator lacks qualified artifact identity");
            for(var column:definition.targetSchema().columns()) if(claim.targetMerges().contains(column.columnId())!=(column.operator()!=null)) throw new IllegalArgumentException("Capacity operator admission differs from schema");
            try { artifacts.validate(definition.sourceSchema(),claim.sourceNames()); artifacts.validate(definition.targetSchema(),claim.targetNames()); }
            catch(IOException failure) { throw new IllegalStateException("Capacity artifact qualification failed",failure); }
        }
        var fresh=new HashSet<>(claim.targetNames().keySet()); fresh.removeAll(claim.sourceNames().keySet()); proposedBirths(claim.targetNames(),claim.targetMerges(),definition.changeId(),fresh);
        for(var entry:claim.targetNames().entrySet()) if(handles.containsKey(entry.getValue()) && claim.targetMerges().contains(entry.getKey()) && loaded.mergeOperators().get(entry.getValue())==null) throw new IllegalArgumentException("Capacity existing merge resource missing");
        for(var entry:claim.targetNames().entrySet()) if(!handles.containsKey(entry.getValue())) {
            var column=definition.targetSchema().column(entry.getKey());
            RocksDBLoader.OperatorResolver resolver=qualified ? ignored -> artifacts.instantiate(column) : null;
            try(var prepared=new PreparedColumn(entry.getValue(),resolver)) {
                if(!qualified && prepared.options.mergeOperator()!=null) throw new UnsupportedOperationException("New configured merge CF requires trusted artifact staging");
                if(claim.targetMerges().contains(entry.getKey())!=(prepared.options.mergeOperator()!=null)) throw new IllegalArgumentException("Capacity merge resource missing or unqualified");
            }
        }
    }
    public enum SchemaCapacityState { NONE, ACTIVE, FENCED, CONSUMED, ABORTED, RETIRED }
    public record SchemaCapacityStatus(NativeSchemaCapacityReservation reservation,SchemaCapacityState state,boolean forcedDurable) {
        public SchemaCapacityStatus {
            Objects.requireNonNull(state);
            if((state==SchemaCapacityState.NONE)!=(reservation==null) || state==SchemaCapacityState.NONE && forcedDurable) throw new IllegalArgumentException("Capacity status identity/state mismatch");
        }
        public boolean ready() { return state==SchemaCapacityState.ACTIVE && forcedDurable; }
    }
    /** Cold local status: retained identity is separate from a currently forced ACTIVE admission. */
    public SchemaCapacityStatus lookupSchemaCapacityStatus(ReplicatedRangeStore store,SchemaChange.Definition definition,String changedPhysicalName,boolean merge) throws RocksDBException {
        Objects.requireNonNull(store); Objects.requireNonNull(definition);
        if(definition.kind()==SchemaChange.Kind.DELETE_COLUMN) {
            if(changedPhysicalName!=null || merge) throw new IllegalArgumentException("DELETE capacity has no target physical binding");
        } else if(changedPhysicalName==null || changedPhysicalName.isEmpty() || changedPhysicalName.length()>4096 || changedPhysicalName.indexOf('\0')>=0 || RESERVED_COLUMNS.contains(changedPhysicalName) || !StandardCharsets.US_ASCII.newEncoder().canEncode(changedPhysicalName)) throw new IllegalArgumentException("Invalid CREATE capacity binding");
        nativeMutation.acquireUninterruptibly();
        try {
            if(!ranges.contains(store)) throw new IllegalArgumentException("Foreign range store");
            return store.readSchemaMetadata(() -> {
                var owner=store.ownership(); var group=owner.owner().groupId();
                var participant=definition.header().participants().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Group outside original capacity membership"));
                var descriptor=definition.sourceDirectory().ranges().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow();
                if(participant.rangeEpoch()!=owner.owner().epoch() || !definition.sourceSchema().domainId().equals(owner.interval().domainId()) || !Arrays.equals(descriptor.lowerInclusive(),owner.interval().lowerInclusive()) || !Arrays.equals(descriptor.upperExclusive(),owner.interval().upperExclusive())) throw new IllegalArgumentException("Capacity status differs from source lineage/bounds");
                var claim=capacityClaims.get(definition.changeId());
                if(claim==null) return new SchemaCapacityStatus(null,SchemaCapacityState.NONE,false);
                if(!Arrays.equals(claim.definition().encode(),definition.encode())) throw new IllegalArgumentException("Original capacity definition changed");
                var changed=definition.header().changedColumnId();
                if(!Objects.equals(claim.targetNames().get(changed),changedPhysicalName) || claim.targetMerges().contains(changed)!=merge) throw new IllegalArgumentException("Original capacity changed binding differs");
                var member=claim.groups().get(group);
                if(member==null) return new SchemaCapacityStatus(null,SchemaCapacityState.NONE,false);
                member.proof().requireDefinition(definition);
                return new SchemaCapacityStatus(member.proof(),SchemaCapacityState.valueOf(member.state().name()),capacityDurable);
            });
        } finally { nativeMutation.release(); }
    }
    /** Qualified fresh FENCE preparation; terminal retained evidence is never a live capacity hold. */
    public NativeSchemaCapacityReservation requireSchemaFenceCapacity(ReplicatedRangeStore store,SchemaChange.Header header) throws RocksDBException {
        Objects.requireNonNull(store); Objects.requireNonNull(header); nativeMutation.acquireUninterruptibly();
        try {
            if(!ranges.contains(store)) throw new IllegalArgumentException("Foreign range store");
            return store.readSchemaMetadata(() -> {
                var owner=store.ownership(); var group=owner.owner().groupId(); var claim=capacityClaims.get(header.changeId()); var member=claim==null ? null : claim.groups().get(group);
                if(member==null) throw new IllegalStateException("Original forced capacity admission missing");
                member.proof().requireHeader(header);
                var descriptor=claim.definition().sourceDirectory().ranges().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow();
                if(member.proof().participant().rangeEpoch()!=owner.owner().epoch() || !claim.definition().sourceSchema().domainId().equals(owner.interval().domainId()) || !Arrays.equals(descriptor.lowerInclusive(),owner.interval().lowerInclusive()) || !Arrays.equals(descriptor.upperExclusive(),owner.interval().upperExclusive())) throw new IllegalArgumentException("FENCE capacity differs from source lineage/bounds");
                if(!capacityDurable || member.state()!=SchemaCapacityClaims.State.ACTIVE && member.state()!=SchemaCapacityClaims.State.FENCED) throw new IllegalStateException("Schema capacity is not a forced ACTIVE/FENCED admission");
                return member.proof();
            });
        } finally { nativeMutation.release(); }
    }
    /** Validate the complete frozen admission against this pool's original forced claim and exact local owner. */
    public NativeSchemaCapacityReservation requireSchemaFencingAdmission(ReplicatedRangeStore store,SchemaChange.Header header,
                                                                         byte[] manifestBytes,ReplicaDescriptor local) throws RocksDBException {
        Objects.requireNonNull(store); Objects.requireNonNull(header); Objects.requireNonNull(manifestBytes); Objects.requireNonNull(local);
        nativeMutation.acquireUninterruptibly();
        try {
            if(!ranges.contains(store)) throw new IllegalArgumentException("Foreign range store");
            return store.readSchemaMetadata(() -> {
                var owner=store.ownership(); var group=owner.owner().groupId(); var claim=capacityClaims.get(header.changeId());
                var member=claim==null ? null : claim.groups().get(group);
                if(member==null) throw new IllegalStateException("Original forced capacity admission missing");
                member.proof().requireHeader(header);
                var manifest=SchemaAdmissionManifest.decode(manifestBytes,claim.definition());
                var descriptor=claim.definition().sourceDirectory().ranges().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow();
                if(member.proof().participant().rangeEpoch()!=owner.owner().epoch() || !claim.definition().sourceSchema().domainId().equals(owner.interval().domainId())
                        || !Arrays.equals(descriptor.lowerInclusive(),owner.interval().lowerInclusive()) || !Arrays.equals(descriptor.upperExclusive(),owner.interval().upperExclusive())
                        || !descriptor.replicas().contains(local) || !manifest.owner(group,local.replicaId()).equals(local)) throw new IllegalArgumentException("FENCE admission differs from source placement/local owner");
                var binding=manifest.binding(group); var changed=header.changedColumnId();
                if(!Objects.equals(binding.changedPhysicalName(),claim.targetNames().get(changed)) || binding.merge()!=claim.targetMerges().contains(changed)) throw new IllegalArgumentException("FENCE admission changes original local physical binding");
                if(!manifest.replica(group,local.replicaId()).reservation().equals(member.proof())) throw new IllegalArgumentException("FENCE admission changes original local capacity proof");
                if(!capacityDurable) throw new IllegalStateException("Schema capacity admission force is unresolved");
                var retained=store.lookupSchemaFence(header);
                if(retained!=null) {
                    retained.requireAdmissionDigest(manifest.digest());
                    if(member.state()==SchemaCapacityClaims.State.ABORTED) throw new IllegalStateException("Retained FENCE contradicts aborted admission");
                } else {
                    if(!member.outstanding()) throw new IllegalStateException("Fresh FENCE requires forced ACTIVE/FENCED admission");
                    if(!Arrays.equals(store.schemaDigest(),claim.definition().sourceSchema().digest()) || !owner.interval().columns().equals(claim.definition().sourceSchema().columnIds())) throw new IllegalArgumentException("Fresh FENCE differs from original source schema/coverage");
                }
                return member.proof();
            });
        } finally { nativeMutation.release(); }
    }
    public NativeSchemaCapacityReservation lookupSchemaCapacity(ReplicatedRangeStore store,SchemaChange.Header header) {
        if(!store.schemaPreparationBelongsTo(this)) throw new IllegalArgumentException("Foreign range store");
        var claim=capacityClaims.get(header.changeId()); if(claim==null) return null; var member=claim.groups().get(store.ownership().owner().groupId());
        if(member==null) return null; member.proof().requireHeader(header); return member.proof();
    }
    /** Explicit trusted fixture path without qualified replica admission. */
    public NativeSchemaFence applySchemaFence(ReplicatedRangeStore store,ApplyPosition position,SchemaChange.Header header,NativeSchemaCapacityReservation reservation) throws RocksDBException { return applySchemaFence(store,position,header,reservation,null); }
    public NativeSchemaFence applySchemaFence(ReplicatedRangeStore store,ApplyPosition position,SchemaChange.Header header,NativeSchemaCapacityReservation reservation,byte[] admissionDigest) throws RocksDBException {
        nativeMutation.acquireUninterruptibly();
        try {
            if(!ranges.contains(store)) throw new IllegalArgumentException("Foreign range store");
            return store.readSchemaMetadata(() -> {
                var claim=capacityClaims.get(header.changeId()); var member=claim==null ? null : claim.groups().get(store.ownership().owner().groupId());
                if(member==null || !member.proof().equals(reservation)) throw new IllegalArgumentException("Original forced capacity admission missing"); member.proof().requireHeader(header);
                if(!capacityDurable) throw new IllegalStateException("Schema capacity admission force is unresolved");
                var retained=store.lookupSchemaFence(header);
                if(retained!=null) { if(member.state()==SchemaCapacityClaims.State.ABORTED) throw new IllegalStateException("Retained FENCE contradicts aborted admission"); retained.requireAdmissionDigest(admissionDigest); return store.applySchemaFence(position,header,admissionDigest); }
                requireCapacityFence(member.proof().participant().groupId(),header);
                if(member.state()==SchemaCapacityClaims.State.ACTIVE) { var next=new HashMap<>(capacityClaims); next.put(header.changeId(),claim.group(member.proof().participant().groupId(),new SchemaCapacityClaims.Group(member.proof(),SchemaCapacityClaims.State.FENCED))); forceCapacityClaims(next); }
                return store.applySchemaFence(position,header,admissionDigest);
            });
        } finally { nativeMutation.release(); }
    }
    public void releaseSchemaCapacity(ReplicatedRangeStore store,NativeSchemaCapacityReservation reservation,SchemaCapacityRelease authority) throws RocksDBException {
        nativeMutation.acquireUninterruptibly();
        try {
            if(!ranges.contains(store)) throw new IllegalArgumentException("Foreign range store");
            store.readSchemaMetadata(() -> {
                var claim=capacityClaims.get(reservation.changeId()); var group=store.ownership().owner().groupId(); var member=claim==null ? null : claim.groups().get(group);
                if(member==null || !member.proof().equals(reservation)) throw new IllegalArgumentException("Capacity release identity changed");
                SchemaCapacityClaims.State terminal;
                if(authority.kind()==SchemaCapacityRelease.Kind.ABORTED_BEFORE_FENCING) {
                    reservation.requireHeader(authority.status().header());
                    if(member.state()!=SchemaCapacityClaims.State.ACTIVE && member.state()!=SchemaCapacityClaims.State.ABORTED || store.lookupSchemaFence(claim.definition().header())!=null) throw new IllegalStateException("Irreversible FENCE capacity cannot be aborted");
                    terminal=SchemaCapacityClaims.State.ABORTED;
                } else {
                    var proof=store.lookupSchemaInstallation(claim.definition().header()); if(proof==null) throw new IllegalStateException("Capacity completion lacks durable original INSTALL");
                    if(authority.kind()==SchemaCapacityRelease.Kind.INSTALLED) { authority.installation().requireHeader(claim.definition().header()); if(!proof.equals(authority.installation())) throw new IllegalArgumentException("Completion proof changed"); terminal=SchemaCapacityClaims.State.CONSUMED; }
                    else { reservation.requireHeader(authority.status().header()); if(!Objects.equals(authority.status().installed().get(group),proof.position())) throw new IllegalArgumentException("Retirement differs from original INSTALL"); terminal=SchemaCapacityClaims.State.RETIRED; }
                }
                var next=new HashMap<>(capacityClaims); next.put(reservation.changeId(),claim.group(group,new SchemaCapacityClaims.Group(member.proof(),terminal))); forceCapacityClaims(next); return null;
            });
        } finally { nativeMutation.release(); }
    }
    interface CapacityTransfer { void append(org.rocksdb.WriteBatch batch) throws RocksDBException; void committed(); }
    private CapacityTransfer capacityTransfer(ReplicatedRangeStore store,NativeSchemaInstallation record) {
        var claim=capacityClaims.get(record.definition().changeId()); if(claim==null) return null;
        var actual=new SchemaCapacityClaims.Claim(record.definition(),record.sourceNames(),record.targetNames(),record.sourceMerges(),record.targetMerges(),Map.of()); claim.requireSame(actual);
        UUID group=store.ownership().owner().groupId(); var member=claim.groups().get(group);
        if(member==null || !member.outstanding()) throw new IllegalStateException("INSTALL lacks outstanding original capacity admission");
        var next=new HashMap<>(capacityClaims); next.put(record.definition().changeId(),claim.group(group,new SchemaCapacityClaims.Group(member.proof(),SchemaCapacityClaims.State.CONSUMED)));
        var immutable=Map.copyOf(next); byte[] bytes=SchemaCapacityClaims.encode(next);
        return new CapacityTransfer() {
            public void append(org.rocksdb.WriteBatch batch) throws RocksDBException { batch.put(handles.get(STATE_COLUMN),CAPACITY_KEY,bytes); }
            public void committed() { capacityClaims=immutable; capacityDurable=true; uncertainCapacityClaims=Map.of(); }
        };
    }
    private void reserveBirths(Map<UUID,String> names,Set<UUID> merges,UUID change,Set<UUID> fresh) throws RocksDBException {
        if(!newImportReservations.isEmpty() && !birthsDurable)throw new IllegalStateException("Unknown import birth completion requires exact retry before CF mutation");
        var next=proposedBirths(names,merges,change,fresh);
        if(next.equals(bindings) && birthsDurable) return;
        try {
            byte[] bytes=encodeBirthRegistry(next);
            // Unknown native completion keeps every proposed reservation until a successful force.
            birthsDurable=false; bindings.clear(); bindings.putAll(next);
            try(var options=new org.rocksdb.WriteOptions().setSync(true).setDisableWAL(false)) { loaded.db().get().put(handles.get(STATE_COLUMN),options,BIRTHS_KEY,bytes); }
            birthsDurable=true;
        } catch(IOException impossible) { throw new AssertionError(impossible); }
    }
    private static byte[] encodeBirthRegistry(Map<UUID,Binding> bindings) throws IOException {
        var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes); output.writeInt(bindings.size());
        for(var id:bindings.keySet().stream().sorted().toList()) { var binding=bindings.get(id); output.writeLong(id.getMostSignificantBits()); output.writeLong(id.getLeastSignificantBits()); output.writeLong(binding.birthChange().getMostSignificantBits()); output.writeLong(binding.birthChange().getLeastSignificantBits()); output.writeByte(binding.birthKind().ordinal()+1); output.writeBoolean(binding.mergeAdmitted()); output.writeUTF(binding.name()); }
        output.flush(); if(bytes.size()>MAX_BINDING_BYTES)throw new IllegalStateException("Native CF birth byte budget exhausted"); return bytes.toByteArray();
    }

    private DistributedStoragePool(RocksDBLoader.LoadedDb loaded, GlobalDatabaseConfig globalConfig, Logger logger, DistributedOperatorArtifacts artifacts) {
        this.loaded = loaded;
        this.artifacts=artifacts;
        this.globalConfig = globalConfig;
        this.logger = logger;
        loaded.db().getStartupColumns().forEach((descriptor, handle) ->
                handles.put(new String(descriptor.getName(), StandardCharsets.US_ASCII), handle));
        recoverBirths();
        recoverOwnershipHistory();
        recoverCapacityClaims(); recoverTopologySeeds();recoverTopologyLearners();
    }

    public static void stageOperator(Path poolPath,DistributedSchema.Operator operator,
            it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract contract,byte[] jar) throws IOException {
        new DistributedOperatorArtifacts(poolPath).stage(operator,contract,jar);
    }
    public static DistributedStoragePool open(Path persistentPath, DatabaseConfig config, Logger logger) {
        return open(persistentPath,config,logger,null,null,Map.of());
    }
    public static DistributedStoragePool open(Path persistentPath, DatabaseConfig config, Logger logger,
            java.util.Collection<DistributedSchema> bootstrapSchemas,Map<UUID,String> bootstrapPhysicalColumns,Map<UUID,String> nonDATAPlainBindings) {
        Objects.requireNonNull(persistentPath, "persistentPath");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(logger, "logger");
        if(bootstrapSchemas!=null){bootstrapSchemas=List.copyOf(bootstrapSchemas);bootstrapPhysicalColumns=Map.copyOf(bootstrapPhysicalColumns);nonDATAPlainBindings=Map.copyOf(nonDATAPlainBindings);}
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
        final DistributedOperatorArtifacts artifacts;
        try {
            artifacts=new DistributedOperatorArtifacts(persistentPath);
            if(bootstrapSchemas!=null && !Files.exists(persistentPath.resolve("CURRENT"))) {
                if(Files.exists(persistentPath)) {
                    if(!Files.isDirectory(persistentPath)) throw new IOException("Native pool path is not a directory");
                    try(var entries=Files.newDirectoryStream(persistentPath)) { if(entries.iterator().hasNext()) throw new IOException("Unidentified nonempty native pool cannot authorize bootstrap"); }
                }
                artifacts.qualifyBootstrap(List.copyOf(bootstrapSchemas),Objects.requireNonNull(bootstrapPhysicalColumns),Objects.requireNonNull(nonDATAPlainBindings),true);
            }
            if(bootstrapSchemas!=null && Files.exists(persistentPath.resolve("CURRENT"))) artifacts.qualifyBootstrap(List.copyOf(bootstrapSchemas),Objects.requireNonNull(bootstrapPhysicalColumns),Objects.requireNonNull(nonDATAPlainBindings),false);
            if(Files.exists(persistentPath.resolve("CURRENT"))) preflightNativeBindings(persistentPath,config,artifacts);
        } catch(IOException|RocksDBException failure) { throw new IllegalStateException("Cannot qualify native operator bindings",failure); }
        RocksDBLoader.OperatorResolver resolver=artifacts.qualified() ? name -> RESERVED_COLUMNS.contains(name) ? null : artifacts.resolve(name) : null;
        var nativeLoaded=RocksDBLoader.load(persistentPath,config,logger,null,null,INTERNAL_COLUMNS,resolver);
        try { return new DistributedStoragePool(nativeLoaded,globalConfig,logger,artifacts); }
        catch(RuntimeException|Error failure) {
            try { nativeLoaded.db().close(); } catch(Exception cleanup) { failure.addSuppressed(cleanup); }
            if(!nativeLoaded.db().get().isOwningHandle()) try { nativeLoaded.refs().close(); } catch(RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    public record GateCaptureRequest(ReplicatedRangeStore store,SnapshotFence fence) {
        public GateCaptureRequest {Objects.requireNonNull(store);Objects.requireNonNull(fence);}
    }
    /** Every alias belongs to this owner; close the cohort even when only some images were exported. */
    public static final class SnapshotCohort implements AutoCloseable {
        private final Map<UUID,ReplicatedRangeStore.SnapshotPin> pins;
        private SnapshotCohort(Map<UUID,ReplicatedRangeStore.SnapshotPin> pins){this.pins=java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(pins));}
        public Map<UUID,ReplicatedRangeStore.SnapshotPin> pins(){return pins;}
        @Override public void close(){Throwable failure=null;for(var pin:pins.values())try {pin.close();}catch(RuntimeException | Error problem){if(failure==null)failure=problem;else if(problem!=failure)failure.addSuppressed(problem);}if(failure instanceof RuntimeException problem)throw problem;if(failure instanceof Error problem)throw problem;}
    }
    @FunctionalInterface public interface SnapshotCohortObserver { void boundary(boolean beforeRelease); }
    private volatile SnapshotCohortObserver snapshotCohortObserver=release -> {};
    public void snapshotCohortObserver(SnapshotCohortObserver observer){snapshotCohortObserver=Objects.requireNonNull(observer);}
    /** Full native intervals share one physical cut and one pool pressure reservation. */
    public SnapshotCohort captureGateCohort(List<GateCaptureRequest> requested) throws RocksDBException {
        Objects.requireNonNull(requested);if(requested.isEmpty() || requested.size()>1024)throw new IllegalArgumentException("Invalid native snapshot cohort size");var requests=List.copyOf(requested);var groups=new HashSet<UUID>();for(var request:requests)if(!groups.add(request.store().ownership().owner().groupId()))throw new IllegalArgumentException("Duplicate snapshot cohort group");
        var ordered=requests.stream().sorted(Comparator.comparing(request->request.store().ownership().owner().groupId())).toList();nativeMutation.acquireUninterruptibly();PoolSnapshotAdmission.Lease lease=null;ReplicatedRangeStore.PhysicalSnapshotOwner physical=null;int nativeLocked=0,applyLocked=0;SnapshotCohort cohort;
        var observer=snapshotCohortObserver;
        try {
            synchronized(admission){if(closing)throw new IllegalStateException("Pool is closing");for(var request:ordered)if(!ranges.contains(request.store()))throw new IllegalArgumentException("Foreign snapshot cohort range");}
            var guard=Objects.requireNonNull(snapshotAdmission,"Snapshot cohort requires configured pool admission");lease=guard.reserve();
            for(var request:ordered){request.store().enterCohortNative();nativeLocked++;}for(var request:ordered){request.store().lockCohortApply();applyLocked++;}for(var request:ordered)request.store().requireCohortGate(request.fence());
            var members=ordered.stream().map(GateCaptureRequest::store).toList();physical=new ReplicatedRangeStore.PhysicalSnapshotOwner(loaded.db().get(),lease,members,()->observer.boundary(true));var snapshot=Objects.requireNonNull(loaded.db().get().getSnapshot(),"Native cohort snapshot creation failed");physical.attach(snapshot);
            var pins=new java.util.LinkedHashMap<UUID,ReplicatedRangeStore.SnapshotPin>();for(var request:requests)pins.put(request.store().ownership().owner().groupId(),request.store().cohortPin(snapshot,lease,physical,request.fence()));cohort=new SnapshotCohort(pins);
        } catch(Throwable failure) {
            for(int n=applyLocked-1;n>=0;n--)ordered.get(n).store().unlockCohortApply();applyLocked=0;for(int n=nativeLocked-1;n>=0;n--)ordered.get(n).store().leaveCohortNative();nativeLocked=0;nativeMutation.release();
            try {if(physical!=null)physical.abandon();else if(lease!=null)lease.close();}catch(Throwable release){if(release!=failure)release.addSuppressed(failure);throw release;}throw failure;
        }
        for(int n=applyLocked-1;n>=0;n--)ordered.get(n).store().unlockCohortApply();for(int n=nativeLocked-1;n>=0;n--)ordered.get(n).store().leaveCohortNative();nativeMutation.release();
        try {observer.boundary(false);return cohort;}catch(Throwable failure){try {physical.abandon();}catch(Throwable release){if(release!=failure)release.addSuppressed(failure);throw release;}throw failure;}
    }

    /** Configure once, before opening any range. Every native snapshot in this pool shares the guard. */
    /** Permanent metadata owners, independent of currently exposed Raft groups. */
    public static final int MAX_RETAINED_METADATA_OWNERS=MAX_OWNERSHIP_HISTORY;
    public record RetainedMetadataOwner(RangeOwnership.Owner owner,RangeOwnership.Interval interval,boolean retired,boolean pending) {}
    public List<RetainedMetadataOwner> retainedMetadataOwners() throws RocksDBException {
        nativeMutation.acquireUninterruptibly();handleLifetime.readLock().lock();try{
            synchronized(admission){if(closing || closed)throw new IllegalStateException("Pool is closing");}
            var result=new ArrayList<RetainedMetadataOwner>();var db=loaded.db().get();var state=handles.get(STATE_COLUMN);
            for(var record:ownershipHistory){var group=record.owner().groupId();boolean retired=topologyRetirements.containsKey(group);boolean pending=topologyBirths.containsKey(group)&&db.get(state,groupSchemaKey(group,0x63))==null || topologyLearners.containsKey(group)&&db.get(state,groupSchemaKey(group,0x68))==null;
                byte[] key=groupSchemaKey(group,0x40);int size=db.get(state,key,new byte[0]);if(size!=org.rocksdb.RocksDB.NOT_FOUND){if(size>NativeSstImportManifest.MAX_BYTES)throw new IllegalStateException("Import metadata inventory exceeds bound");var manifest=NativeSstImportManifest.decode(db.get(state,key));if(!manifest.groupId().equals(group) || ownershipHistory.stream().noneMatch(original->original.owner().groupId().equals(group) && manifest.rangeEpoch()==original.owner().epoch() && manifest.domainId().equals(original.interval().domainId()) && original.interval().columns().containsAll(manifest.physicalNames().keySet()) && Arrays.equals(manifest.lowerInclusive(),original.interval().lowerInclusive()) && Arrays.equals(manifest.upperExclusive(),original.interval().upperExclusive())))throw new IllegalStateException("Import metadata inventory changed ownership");pending|=db.get(state,groupSchemaKey(group,0x43))==null;}
                result.add(new RetainedMetadataOwner(record.owner(),record.interval(),retired,pending));
            }return List.copyOf(result);
        }finally{handleLifetime.readLock().unlock();nativeMutation.release();}
    }
    /** Bounded durable group inventory, including virgin births and retained inactive metadata. */
    public Set<UUID> persistedRangeGroups(int maximum) throws RocksDBException {
        if(maximum<1 || maximum>65536)throw new IllegalArgumentException("Invalid native group inventory ceiling");nativeMutation.acquireUninterruptibly();handleLifetime.readLock().lock();try{synchronized(admission){if(closing || closed)throw new IllegalStateException("Pool is closing");}var groups=new HashSet<UUID>();for(var entry:ownershipHistory){groups.add(entry.owner().groupId());if(groups.size()>maximum)throw new IllegalStateException("Native group inventory exceeds declared ceiling");}try(var iterator=loaded.db().get().newIterator(handles.get(STATE_COLUMN))){iterator.seekToFirst();while(iterator.isValid()){var key=iterator.key();if(key.length<16 || Arrays.equals(key,OWNERSHIP_HISTORY_KEY) || Arrays.equals(key,BIRTHS_KEY) || Arrays.equals(key,TOPOLOGY_SEEDS_KEY) || Arrays.equals(key,CAPACITY_KEY)){iterator.next();continue;}var input=java.nio.ByteBuffer.wrap(key);groups.add(new UUID(input.getLong(),input.getLong()));if(groups.size()>maximum)throw new IllegalStateException("Native group inventory exceeds declared ceiling");var next=Arrays.copyOf(key,16);int index=15;while(index>=0 && (next[index]&255)==255){next[index]=0;index--;}if(index<0)break;next[index]++;iterator.seek(next);}iterator.status();}return Set.copyOf(groups);}finally{handleLifetime.readLock().unlock();nativeMutation.release();}
    }
    public void configureSnapshotAdmission(PoolSnapshotAdmission.Limits limits) {configureSnapshotAdmission(limits,null);}
    public void configureSnapshotAdmission(PoolSnapshotAdmission.Limits limits,PoolSnapshotAdmission.Shared shared) {
        Objects.requireNonNull(limits);
        nativeMutation.acquireUninterruptibly();
        try {
            synchronized (admission) {
                if (closing || !ranges.isEmpty() || snapshotAdmission != null) {
                    throw new IllegalStateException("Snapshot admission must be configured before opening ranges");
                }
            }
            var volumes = new HashSet<Path>();
            volumes.add(loaded.definitiveDbPath());
            try {
                for (var volume : RocksDBLoader.getVolumeConfigs(loaded.definitiveDbPath(), globalConfig.fallbackColumnOptions())) volumes.add(volume.path());
                for (var column : globalConfig.columnOptions()) {
                    for (var volume : RocksDBLoader.getVolumeConfigs(loaded.definitiveDbPath(), column)) volumes.add(volume.path());
                }
            } catch (GestaltException failure) {
                throw it.cavallium.rockserver.core.common.RocksDBException.of(
                        it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.CONFIG_ERROR,
                        "Cannot resolve snapshot storage volumes", failure);
            }
            storageVolumes = List.copyOf(volumes);
            pressureColumns = List.copyOf(handles.values());
            snapshotAdmission = new PoolSnapshotAdmission(limits, this::sampleSnapshotPressure, System::nanoTime,shared);
        } finally { nativeMutation.release(); }
    }

    /** Metadata only; estimated bytes do not measure exact physical SST retention. */
    public PoolSnapshotAdmission.Stats snapshotAdmissionStats() {
        var guard = snapshotAdmission;
        return guard == null ? new PoolSnapshotAdmission.Stats(0, 0, Duration.ZERO) : guard.stats();
    }

    /** Fresh observation only; later capture admission remains authoritative under concurrent load. */
    public record SnapshotPressure(long estimatedBytes,long freeBytes,int pinReservations) {}
    public SnapshotPressure currentSnapshotPressure() throws IOException,RocksDBException {
        nativeMutation.acquireUninterruptibly();try {
            synchronized(admission){if(closing || closed)throw new IllegalStateException("Pool is closing");}
            var guard=Objects.requireNonNull(snapshotAdmission,"Pressure probe requires configured pool snapshot admission");if(guard.releaseFailure()!=null)throw new IllegalStateException("Native snapshot release outcome unknown; pool retained",guard.releaseFailure());
            var sample=sampleSnapshotPressure();long age=System.nanoTime()-sample.sampledAtNanos();if(sample.estimatedBytes()<0 || sample.minimumFreeBytes()<0 || age<0 || age>Duration.ofSeconds(5).toNanos())throw new IOException("Pool snapshot pressure metrics invalid or stale");return new SnapshotPressure(sample.estimatedBytes(),sample.minimumFreeBytes(),guard.stats().pins());
        } finally {nativeMutation.release();}
    }

    private PoolSnapshotAdmission.Sample sampleSnapshotPressure() throws IOException, RocksDBException {
        long sampledAt = System.nanoTime(), bytes = 0, free = Long.MAX_VALUE;
        handleLifetime.readLock().lock();
        try {
        synchronized (admission) { if (closing) throw new IOException("Pool is closing"); }
        for (var column : pressureColumns) {
            long sst = loaded.db().get().getLongProperty(column, "rocksdb.total-sst-files-size");
            long memtable = loaded.db().get().getLongProperty(column, "rocksdb.size-all-mem-tables");
            if (sst < 0 || memtable < 0) throw new IOException("Negative native snapshot pressure metric");
            bytes = Math.addExact(bytes, Math.addExact(sst, memtable));
        }
        for (Path volume : storageVolumes) free = Math.min(free, Files.getFileStore(volume).getUsableSpace());
        return new PoolSnapshotAdmission.Sample(bytes, free, sampledAt);
        } finally { handleLifetime.readLock().unlock(); }
    }

    public record WorkloadPressure(boolean pressured,int reasonMask,boolean diskPressure,long freeBytes,long sampledAtNanos) {}
    /** Fresh pool-local sampling holds DB/CF lifetime protection through every property read. */
    public WorkloadPressure currentWorkloadPressure(long minimumFreeBytes) throws IOException,RocksDBException {
        if(minimumFreeBytes<0)throw new IllegalArgumentException("Negative free-space reserve");
        handleLifetime.readLock().lock();
        try {
            synchronized(admission){if(closing || closed)throw new IOException("Pool is closing");}
            long sampled=System.nanoTime();var db=loaded.db().get();var signal=new StoragePressureSignal();
            signal.reset(db.getLongProperty(RocksDBLongProperty.IS_WRITE_STOPPED.getName()),
                    db.getLongProperty(RocksDBLongProperty.ACTUAL_DELAYED_WRITE_RATE.getName()));
            if(!signal.pressured())for(var entry:handles.entrySet()) {
                var options=loaded.definitiveColumnFamilyOptionsMap().get(entry.getKey());
                if(options==null)throw new IOException("Pressure column has no effective options");
                signal.observeColumn(entry.getValue().getID(),db.getLongProperty(entry.getValue(),RocksDBLongProperty.ESTIMATE_PENDING_COMPACTION_BYTES.getName()),options.softPendingCompactionBytesLimit());
                if(signal.pressured())break;
            }
            long free=Long.MAX_VALUE;
            for(var volume:storageVolumes.isEmpty()?List.of(loaded.definitiveDbPath()):storageVolumes)
                free=Math.min(free,Files.getFileStore(volume).getUsableSpace());
            boolean disk=free<minimumFreeBytes;
            return new WorkloadPressure(signal.pressured() || disk,signal.reasonMask(),disk,free,sampled);
        } finally {handleLifetime.readLock().unlock();}
    }

    private record RecoveredSchema(NativeSchemaInstallation current,List<NativeSchemaInstallation> chain,NativeSchemaFence pending) {}
    private RecoveredSchema recoverSchema(UUID group) throws RocksDBException {
        for(var range:ranges) if(range.ownership().owner().groupId().equals(group)) return range.readSchemaMetadata(() -> recoverSchemaUnderLane(group));
        return recoverSchemaUnderLane(group);
    }
    private RecoveredSchema recoverSchemaUnderLane(UUID group) throws RocksDBException {
        var state=handles.get(STATE_COLUMN); var db=loaded.db().get();
        byte[] marker=ByteBuffer.allocate(16).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).array();
        var markerBytes=db.get(state,marker); var cut=markerBytes==null ? ReplicatedRangeStore.snapshotInstallationCut(db.get(state,groupSchemaKey(group,0x7f))) : ReplicatedRangeStore.decodePosition(markerBytes);
        var currentBytes=db.get(state,groupSchemaKey(group,0x28)); var current=currentBytes==null ? null : NativeSchemaInstallation.decode(currentBytes);
        var activationBytes=db.get(state,groupSchemaKey(group,0x2c)); var activation=activationBytes==null ? null : NativeSchemaActivationProof.decode(activationBytes);
        var pendingBytes=db.get(state,groupSchemaKey(group,0x27)); var pending=pendingBytes==null ? null : NativeSchemaFence.decode(pendingBytes);
        var history=new ArrayList<NativeSchemaInstallation>(); var activations=new HashMap<UUID,NativeSchemaActivationProof>();
        long bytes=(currentBytes==null ? 0 : currentBytes.length)+(activationBytes==null ? 0 : activationBytes.length);
        for(int tag:List.of(0x2a,0x2b)) try(var iterator=db.newIterator(state)) {
            var prefix=groupSchemaKey(group,tag); iterator.seek(prefix);
            while(iterator.isValid() && iterator.key().length>=prefix.length && Arrays.equals(iterator.key(),0,prefix.length,prefix,0,prefix.length)) {
                byte[] value=iterator.value(); if((bytes+=value.length)>16L*1024*1024) throw new IllegalStateException("Schema recovery metadata exceeds budget");
                UUID id;
                if(tag==0x2a) { var record=NativeSchemaInstallation.decode(value); if(history.size()>=64) throw new IllegalStateException("Schema history count exceeds budget"); history.add(record); id=record.definition().changeId(); }
                else { var proof=NativeSchemaActivationProof.decode(value); if(activations.size()>=64) throw new IllegalStateException("Activation history count exceeds budget"); id=proof.installation().fence().changeId(); if(activations.put(id,proof)!=null) throw new IllegalStateException("Duplicate activation"); }
                var expected=ByteBuffer.allocate(33).put(marker).put((byte)tag).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
                if(!Arrays.equals(iterator.key(),expected)) throw new IllegalStateException("Schema recovery history key changed"); iterator.next();
            }
            iterator.status();
        }
        return new RecoveredSchema(current,NativeSchemaInstallation.validateLifecycle(current,history,activation,activations,pending,cut),pending);
    }
    private RangeOwnership.SchemaRecovery prepareSchemaRecovery(UUID domain,List<NativeSchemaInstallation> chain) throws RocksDBException {
        var participants=new HashMap<UUID,RangeDescriptor>(); var baseSchemas=new HashMap<UUID,DistributedSchema>();
        for(var record:chain) for(var descriptor:record.definition().sourceDirectory().ranges()) {
            participants.put(descriptor.groupId(),descriptor); baseSchemas.putIfAbsent(descriptor.groupId(),record.definition().sourceSchema());
        }
        var expected=new HashMap<RangeOwnership.Owner,RangeOwnership.Interval>();
        for(var descriptor:participants.values()) {
            var group=descriptor.groupId(); var marker=ByteBuffer.allocate(16).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).array();
            if(loaded.db().get().get(handles.get(STATE_COLUMN),marker)==null) continue;
            var recovered=recoverSchema(group); var schema=recovered.current()==null ? baseSchemas.get(group) : recovered.current().definition().targetSchema();
            var owner=new RangeOwnership.Owner(group,descriptor.rangeEpoch()); var interval=new RangeOwnership.Interval(domain,schema.columnIds(),descriptor.lowerInclusive(),descriptor.upperExclusive());
            for(var record:recovered.chain()) record.requireRange(owner.groupId(),owner.epoch(),domain,interval.lowerInclusive(),interval.upperExclusive());
            expected.put(owner,interval);
        }
        return ownership.prepareSchemaRecovery(domain,expected);
    }

    /** Bind the supplied pool-schema names; admission is supplied by the qualified range adapter. */
    public ReplicatedRangeStore openRange(RangeOwnership.Owner owner, RangeOwnership.Interval interval,
            byte[] schemaDigest, Map<UUID, String> physicalColumns, Set<UUID> admittedMergeColumns)
            throws RocksDBException {
        return openRange(owner,interval,schemaDigest,physicalColumns,admittedMergeColumns,null,null,false);
    }

    /** Creates only a virgin manifest-bound fenced owner; never temporarily publishes ACTIVE ownership. */
    public ReplicatedRangeStore openNewFencedRange(NativeSstImportManifest manifest,DistributedSchema schema) throws RocksDBException {
        manifest.requireSchema(schema);
        var store=openRange(new RangeOwnership.Owner(manifest.groupId(),manifest.rangeEpoch()),new RangeOwnership.Interval(manifest.domainId(),manifest.physicalNames().keySet(),manifest.lowerInclusive(),manifest.upperExclusive()),schema.digest(),manifest.physicalNames(),manifest.mergeColumns(),manifest,schema,false);
        store.enableLogicalSchema(schema); return store;
    }

    /** Recovery-only opener: exact durable birth is mandatory; ownership remains fenced until native validation. */
    public ReplicatedRangeStore reopenImportRange(NativeSstImportManifest manifest,DistributedSchema originalSchema) throws RocksDBException {
        manifest.requireSchema(originalSchema);
        var store=openRange(new RangeOwnership.Owner(manifest.groupId(),manifest.rangeEpoch()),new RangeOwnership.Interval(manifest.domainId(),manifest.physicalNames().keySet(),manifest.lowerInclusive(),manifest.upperExclusive()),originalSchema.digest(),manifest.physicalNames(),manifest.mergeColumns(),manifest,originalSchema,true);
        var installed=store.installedSchema(); store.enableLogicalSchema(installed==null ? originalSchema : installed); return store;
    }

    private ReplicatedRangeStore openRange(RangeOwnership.Owner owner, RangeOwnership.Interval interval,
            byte[] schemaDigest,Map<UUID,String> physicalColumns,Set<UUID> admittedMergeColumns,NativeSstImportManifest newImport,DistributedSchema importSchema,boolean recoverImport) throws RocksDBException {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(interval, "interval");
        final byte[] bootstrapSchema=schemaDigest.clone();
        final var bootstrapNames=Map.copyOf(physicalColumns); final var bootstrapMerges=Set.copyOf(admittedMergeColumns);
        byte[] schema=bootstrapSchema.clone(); var names=bootstrapNames; var merges=bootstrapMerges;
        NativeSchemaInstallation recovered=null;
        RangeOwnership.SchemaTransition recoveryTransition=null;
        RangeOwnership.SchemaRecovery recoveryOwnership=null;
        if (schema.length != 32 || !names.keySet().equals(interval.columns())
                || !names.keySet().containsAll(merges) || Set.copyOf(names.values()).size() != names.size()) {
            throw new IllegalArgumentException("Invalid range schema, column coverage or aliases");
        }
        for (String name : names.values()) {
            if (name.isEmpty() || name.length()>4096 || name.indexOf('\0') >= 0 || RESERVED_COLUMNS.contains(name)
                    || !StandardCharsets.US_ASCII.newEncoder().canEncode(name)) {
                throw new IllegalArgumentException("Physical columns require unique nonreserved ASCII names");
            }
        }
        nativeMutation.acquireUninterruptibly();
        try {
            synchronized (admission) {
                if (closing) throw new IllegalStateException("Pool is closing");
            }
            if(pendingTopologySeed!=null) throw new IllegalStateException("Unknown topology seed completion requires exact retry");
            if(pendingLearnerHistory!=null)throw new IllegalStateException("Unknown learner history requires exact original retry");
            var topologySource=topologyRetirements.get(owner.groupId()); var topologyChild=topologyBirths.get(owner.groupId());var learner=topologyLearners.get(owner.groupId());
            if(pendingTopologyLearners.containsKey(owner.groupId()))throw new IllegalStateException("Learner force requires exact registration retry");
            if(learner==null && loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(owner.groupId(),0x67))!=null)throw new IllegalArgumentException("Learner registration lacks qualified pool history");
            if(learner!=null && (!interval.equals(learnerInterval(learner)) || !Arrays.equals(schema,learner.schema().digest()) || !names.equals(learner.physicalNames()) || !merges.equals(learner.mergeColumns()) || newImport!=null))throw new IllegalArgumentException("Learner open changed qualified receiver bindings");
            byte[] nativeBirth=loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(owner.groupId(),0x61)),nativeRetirement=loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(owner.groupId(),0x62));
            if(nativeBirth!=null && topologyChild==null && learner==null || nativeRetirement!=null && topologySource==null) throw new IllegalArgumentException("Topology owner lacks original seed artifact");
            if(topologyChild!=null) {
                var original=topologySeeds.get(topologyChild.intentId()); var descriptor=topologyChild.resolveDescriptor(original);
                if(!owner.equals(new RangeOwnership.Owner(descriptor.groupId(),descriptor.rangeEpoch())) || !interval.equals(topologyInterval(original,descriptor)) || !Arrays.equals(schema,original.header().schemaDigest()) || !names.equals(original.physicalNames()) || !merges.equals(original.mergeColumns())) throw new IllegalArgumentException("Topology target open changed original birth");
            }
            if(topologySource!=null && (newImport!=null || !owner.equals(new RangeOwnership.Owner(owner.groupId(),topologySource.header().participant(TopologyChange.Stage.FREEZE,owner.groupId()).rangeEpoch())))) throw new IllegalArgumentException("Topology source cannot be reused");
            if(topologySource!=null || topologyChild!=null || learner!=null) for(var existing:ranges) if(existing.ownership().owner().equals(owner)) {
                if(learner!=null && !Arrays.equals(existing.schemaDigest(),schema)) {
                    var preparation=existing.learnerSchemaPreparation();
                    if(preparation!=null){schema=preparation.schema().digest();names=preparation.physicalNames();merges=preparation.mergeColumns();}
                    else {var checkpoint=recoverSchema(owner.groupId());var current=checkpoint.current();
                    if(current==null)throw new IllegalArgumentException("Opened learner lacks verified current schema");
                    current.requireRange(owner.groupId(),owner.epoch(),interval.domainId(),interval.lowerInclusive(),interval.upperExclusive());
                    schema=current.definition().targetSchema().digest();names=current.targetNames();merges=current.targetMerges();}
                    interval=new RangeOwnership.Interval(interval.domainId(),names.keySet(),interval.lowerInclusive(),interval.upperExclusive());
                }
                if(!existing.ownership().interval().equals(interval) || !Arrays.equals(existing.schemaDigest(),schema) || !schemaPhysicalNamesUnderMutation(existing).equals(names) || !existing.deterministicMergeColumns().equals(merges)) throw new IllegalArgumentException("Opened topology/range identity changed"); return existing;
            }
            byte[] importBirth=loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(owner.groupId(),0x40));
            if(recoverImport) {
                if(importBirth==null || !Arrays.equals(importBirth,newImport.encode()))throw new IllegalStateException("Import recovery requires exact persisted native birth");
                newImport=null; // Recover through ordinary generation qualification, without NEW admission or birth publication.
            }
            var heldImport=newImportReservations.get(owner.groupId());
            if(heldImport!=null && (newImport==null || !Arrays.equals(heldImport.encode(),newImport.encode())))throw new IllegalArgumentException("Unpublished native import requires exact original-manifest retry");
            if(newImport!=null) {
                for(var entry:physicalColumns.entrySet()) {
                    var binding=bindings.get(entry.getKey()); if(binding!=null && (!binding.name().equals(entry.getValue()) || binding.mergeAdmitted()!=admittedMergeColumns.contains(entry.getKey())))throw new IllegalArgumentException("Import column binding differs from native birth");
                    if(bindings.entrySet().stream().anyMatch(other->!other.getKey().equals(entry.getKey()) && other.getValue().name().equals(entry.getValue())))throw new IllegalArgumentException("Import physical column alias is already bound");
                }
                try { if(artifacts.qualified())artifacts.validate(importSchema,physicalColumns); else if(!admittedMergeColumns.isEmpty())throw new IllegalArgumentException("NEW operator import requires trusted artifact staging"); }
                catch(IOException invalid) { throw new IllegalArgumentException("Import artifact bindings differ",invalid); }
                for(var entry:physicalColumns.entrySet()) {
                    if(handles.containsKey(entry.getValue())) { if((loaded.mergeOperators().get(entry.getValue())!=null)!=admittedMergeColumns.contains(entry.getKey()))throw new IllegalArgumentException("Import configured operator differs"); SSTWriter.rejectUnsafeMultiPathIngestion(loaded.definitiveColumnFamilyOptionsMap().get(entry.getValue()),false); }
                    else try(var prepared=new PreparedColumn(entry.getValue())) { if((prepared.options.mergeOperator()!=null)!=admittedMergeColumns.contains(entry.getKey()))throw new IllegalArgumentException("Import configured operator differs"); SSTWriter.rejectUnsafeMultiPathIngestion(prepared.options.options(),false); }
                }
                if(importBirth!=null) { if(!Arrays.equals(importBirth,newImport.encode()))throw new IllegalArgumentException("NEW import identity changed"); if(loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(owner.groupId(),0x43))!=null)throw new IllegalStateException("Published import is not a NEW target"); }
                else if(heldImport==null) {
                    requireVirginHistory(interval);
                    byte[] prefix=ByteBuffer.allocate(16).putLong(owner.groupId().getMostSignificantBits()).putLong(owner.groupId().getLeastSignificantBits()).array();
                    for(var cf:List.of(handles.get(STATE_COLUMN),handles.get(RECEIPTS_COLUMN))) try(var iterator=loaded.db().get().newIterator(cf)) { iterator.seek(prefix); if(iterator.isValid() && startsWith(iterator.key(),prefix))throw new IllegalStateException("NEW import cannot adopt historical group metadata"); iterator.status(); }
                    for(String name:physicalColumns.values()) { var cf=handles.get(name); if(cf==null)continue; try(var iterator=loaded.db().get().newIterator(cf)) { if(interval.lowerInclusive()==null)iterator.seekToFirst(); else iterator.seek(interval.lowerInclusive()); if(iterator.isValid() && (interval.upperExclusive()==null || Arrays.compareUnsigned(iterator.key(),interval.upperExclusive())<0))throw new IllegalStateException("NEW import interval contains historical user data"); iterator.status(); } }
                }
                for(var existing:ranges)if(existing.ownership().owner().equals(owner))return existing;
                forceNewImportBirth(newImport,owner,interval);
                importBirth=newImport.encode();
            }
            var checkpoint=recoverSchema(owner.groupId());
            if(checkpoint.current()!=null) {
                recovered=checkpoint.current(); recovered.requireRange(owner.groupId(),owner.epoch(),interval.domainId(),interval.lowerInclusive(),interval.upperExclusive());
                boolean original=false;
                for(var historical:checkpoint.chain()) {
                    historical.requireRange(owner.groupId(),owner.epoch(),interval.domainId(),interval.lowerInclusive(),interval.upperExclusive());
                    if(Arrays.equals(schema,historical.definition().sourceSchema().digest()) && names.equals(historical.sourceNames()) && merges.equals(historical.sourceMerges())
                            || Arrays.equals(schema,historical.definition().targetSchema().digest()) && names.equals(historical.targetNames()) && merges.equals(historical.targetMerges())) original=true;
                }
                if(!original) throw new IllegalArgumentException("Bootstrap differs from verified schema lifecycle");

                var definition=recovered.definition(); schemaOrigins.put(definition.sourceSchema().domainId(),definition);
                if(topologySource==null && topologyChild==null && learner==null) recoveryOwnership=prepareSchemaRecovery(interval.domainId(),checkpoint.chain());
                schema=definition.targetSchema().digest(); names=recovered.targetNames(); merges=recovered.targetMerges();
                interval=new RangeOwnership.Interval(interval.domainId(),names.keySet(),interval.lowerInclusive(),interval.upperExclusive());
            }
            if(topologySource==null && topologyChild==null && learner==null && recoveryOwnership==null) for(var origin:schemaOrigins.values()) {
                if(!origin.sourceSchema().domainId().equals(interval.domainId())) continue;
                var descriptor=origin.sourceDirectory().ranges().stream().filter(participant -> participant.groupId().equals(owner.groupId()) && participant.rangeEpoch()==owner.epoch()).findFirst().orElse(null);
                if(descriptor==null || !Arrays.equals(descriptor.lowerInclusive(),interval.lowerInclusive()) || !Arrays.equals(descriptor.upperExclusive(),interval.upperExclusive())) continue;
                for(var participant:origin.sourceDirectory().ranges()) {
                    var other=recoverSchema(participant.groupId());
                    if(other.current()==null) continue;
                    boolean known=other.chain().stream().anyMatch(record -> Arrays.equals(record.definition().sourceSchema().digest(),bootstrapSchema) && record.sourceNames().equals(bootstrapNames) && record.sourceMerges().equals(bootstrapMerges));
                    if(!known) throw new IllegalArgumentException("Source bootstrap differs from qualified local schema chain");
                    recoveryOwnership=prepareSchemaRecovery(interval.domainId(),other.chain()); break;
                }
                if(recoveryOwnership!=null) break;
            }
            if(topologySource==null)ownership.validateRegistration(owner,interval);
            // No metadata monitor is held during loader calls, CF creation or store recovery.
            for (var entry : names.entrySet()) {
                Binding existing = bindings.get(entry.getKey());
                if (existing != null && (!existing.name.equals(entry.getValue())
                        || existing.mergeAdmitted != merges.contains(entry.getKey()))) {
                    throw new IllegalArgumentException("Incompatible logical column binding or merge admission");
                }
                if (bindings.entrySet().stream().anyMatch(other -> !other.getKey().equals(entry.getKey())
                        && other.getValue().name.equals(entry.getValue()))) {
                    throw new IllegalArgumentException("Physical column is already bound to another logical column");
                }
            }
            reserveBirths(names,merges,BOOTSTRAP_BIRTH,Set.of());
            var marker=ByteBuffer.allocate(16).putLong(owner.groupId().getMostSignificantBits()).putLong(owner.groupId().getLeastSignificantBits()).array();
            boolean established=loaded.db().get().get(handles.get(STATE_COLUMN),marker)!=null;
            var rangeColumns = new HashMap<UUID, ColumnFamilyHandle>();
            for (var entry : names.entrySet()) {
                String name = entry.getValue();
                var handle = handles.get(name);
                boolean mergeConfigured;
                if (handle == null) {
                    if(established) throw new IllegalStateException("Established range references a missing CF without creation intent");
                    try(var prepared=new PreparedColumn(name)) {
                        mergeConfigured=prepared.options.mergeOperator()!=null;
                        if(merges.contains(entry.getKey()) && !mergeConfigured) throw new IllegalArgumentException("Merge admission requires a configured operator");
                        handle=createColumn(prepared);
                    }
                } else {
                    mergeConfigured = loaded.mergeOperators().get(name) != null;
                    if (merges.contains(entry.getKey()) && !mergeConfigured) {
                        throw new IllegalArgumentException("Merge admission requires a configured operator");
                    }
                }
                rangeColumns.put(entry.getKey(), handle);
            }
            pressureColumns = List.copyOf(handles.values());
            retainOwnershipHistory(owner,interval);
            if(topologySource==null && topologyChild==null && learner==null && recoveryOwnership==null && recoveryTransition==null) {
                for(var origin:schemaOrigins.values()) if(origin.sourceSchema().domainId().equals(interval.domainId())
                        && Arrays.equals(origin.sourceSchema().digest(),schema) && origin.sourceSchema().columnIds().equals(interval.columns())
                        && origin.sourceDirectory().ranges().stream().anyMatch(participant -> participant.groupId().equals(owner.groupId()) && participant.rangeEpoch()==owner.epoch())) {
                    recoveryTransition=ownership.beginSchemaTransition(origin.changeId(),interval.domainId(),origin.sourceSchema().columnIds(),origin.targetSchema().columnIds()); break;
                }
            }
            if(topologySource!=null || topologyChild!=null) { var registered=ownership.get(owner); if(!registered.interval().equals(interval) || registered.state()!=(topologySource!=null ? RangeOwnership.State.RETIRED : RangeOwnership.State.FENCED)) throw new IllegalArgumentException("Recovered topology ownership changed"); }
            else if(newImport!=null) { /* Already registered fenced before native birth. */ }
            else if(importBirth!=null || learner!=null) ownership.register(owner,interval,RangeOwnership.State.FENCED);
            else if(recoveryOwnership!=null) ownership.registerRecoveredSchema(owner,interval,RangeOwnership.State.FENCED,recoveryOwnership);
            else if(recoveryTransition==null) ownership.register(owner, interval, RangeOwnership.State.ACTIVE);
            else ownership.registerSchema(owner,interval,RangeOwnership.State.FENCED,recoveryTransition);
            try {
                var store = new ReplicatedRangeStore(loaded.db().get(), ownership, owner, schema, rangeColumns,
                        handles.get(STATE_COLUMN), handles.get(RECEIPTS_COLUMN), merges);
                store.configureSnapshotAdmission(snapshotAdmission); store.configureSchemaPreparationPool(this);
                store.validateTopologyActivationRecovery();
                if(topologySource==null && (topologyChild!=null && learner==null && store.topologyActivation()!=null || learner!=null && store.topologyLearnerServing()) && !store.pendingInstallation()) ownership.activate(owner);
                for(var entry:names.entrySet()) bindings.putIfAbsent(entry.getKey(),new Binding(entry.getValue(),merges.contains(entry.getKey())));
                ranges.add(store);
                if(newImport!=null)newImportReservations.remove(owner.groupId());
                if(topologySource==null && topologyChild==null && (recoveryOwnership!=null || recoveryTransition!=null) && importBirth==null) ownership.activate(owner);
                if(topologySource==null && topologyChild==null && newImport==null && importBirth!=null && !store.importPending()) ownership.activate(owner);
                if(recovered!=null) {
                    for(var entry:recovered.sourceNames().entrySet()) if(!recovered.targetNames().containsKey(entry.getKey())) {
                        bindings.putIfAbsent(entry.getKey(),new Binding(entry.getValue(),recovered.sourceMerges().contains(entry.getKey()))); deferredDrops.add(entry.getKey()); deletionOrigins.put(entry.getKey(),recovered.definition());
                    }
                }
                var intent=loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(owner.groupId(),0x29));
                if(intent!=null) {
                    var record=NativeSchemaInstallation.decode(intent);
                    store.enableLogicalSchema(record.definition().sourceSchema());
                    installSchemaUnderLock(store,record);
                }
                drainDeferredDrops(); pressureColumns=List.copyOf(handles.values());
                return store;
            } catch (RuntimeException | RocksDBException failure) {
                ranges.removeIf(range -> range.ownership().owner().equals(owner));
                var current = ownership.get(owner);
                if (current.state() == RangeOwnership.State.ACTIVE) ownership.fence(owner);
                if(current.state()!=RangeOwnership.State.RETIRED) ownership.retire(owner);
                throw failure;
            }
        } finally {
            nativeMutation.release();
        }
    }

    private static boolean startsWith(byte[] key,byte[] prefix) { return key.length>=prefix.length && Arrays.equals(key,0,prefix.length,prefix,0,prefix.length); }

    /** Trusted local source paths are copied to forced immutable pool-owned originals before native mutation. */
    public void validateSstImport(ReplicatedRangeStore store,NativeSstImportManifest manifest,Map<UUID,Path> originals) throws IOException,RocksDBException {
        nativeMutation.acquireUninterruptibly();
        try {
            synchronized(admission) { if(closing || !ranges.contains(store))throw new IllegalStateException("Import store is not owned by this open pool"); }
            store.readSchemaMetadata(()->{
                if(store.pendingInstallation() || !store.importPending() || store.ownership().state()!=RangeOwnership.State.FENCED)throw new IllegalStateException("Import proposal requires unpublished fenced native owner");
                byte[] birth=loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(manifest.groupId(),0x40));
                if(!store.ownership().owner().equals(new RangeOwnership.Owner(manifest.groupId(),manifest.rangeEpoch())) || !Arrays.equals(birth,manifest.encode()))throw new IllegalArgumentException("Import proposal differs from original native birth");
                try { qualifySstOriginals(store,manifest,originals); } catch(IOException failure) { throw new UncheckedIOException(failure); } return null;
            });
        } catch(UncheckedIOException failure) { throw failure.getCause(); }
        finally { nativeMutation.release(); }
    }
    private void qualifySstOriginals(ReplicatedRangeStore store,NativeSstImportManifest manifest,Map<UUID,Path> originals) throws IOException,RocksDBException {
        var schema=store.installedSchema(); manifest.requireSchema(schema);
        var actualNames=new HashMap<UUID,String>(); for(var column:store.ownership().interval().columns())actualNames.put(column,Objects.requireNonNull(bindings.get(column)).name());
        if(!manifest.physicalNames().equals(actualNames))throw new IllegalArgumentException("Import physical bindings differ");
        Path root=loaded.definitiveDbPath().resolve("range-imports"); NativeRangeSstImport.retain(root,manifest,originals);
        for(var column:manifest.columns()) {
            var config=loaded.definitiveColumnFamilyOptionsMap().get(column.physicalName()); SSTWriter.rejectUnsafeMultiPathIngestion(config,false);
            try(var options=new org.rocksdb.Options(loaded.dbOptions(),config)) { for(var file:column.files())NativeRangeSstImport.qualify(NativeRangeSstImport.original(root,manifest,file),file,manifest,column.columnId(),schema,options); }
        }
    }
    public NativeSstImportManifest.Receipt importSst(ReplicatedRangeStore store,NativeSstImportManifest manifest,Map<UUID,Path> originals,ApplyPosition position) throws IOException,RocksDBException {
        return importSst(store,manifest,originals,position,stage -> {});
    }
    public NativeSstImportManifest.Receipt importSst(ReplicatedRangeStore store,NativeSstImportManifest manifest,Map<UUID,Path> originals,ApplyPosition position,NativeRangeSstImport.Observer observer) throws IOException,RocksDBException {
        nativeMutation.acquireUninterruptibly();
        try {
            synchronized(admission) { if(closing || !ranges.contains(store))throw new IllegalStateException("Import store is not owned by this open pool"); }
            if(store.importReceipt()!=null)return store.installSstUnderPool(manifest,position,()->{},(db,columns)->{},observer);
            Path root=loaded.definitiveDbPath().resolve("range-imports");
            return store.installSstUnderPool(manifest,position,() -> {
                qualifySstOriginals(store,manifest,originals);
                observer.at(NativeRangeSstImport.Stage.ORIGINALS_FORCED);
            },(db,columns) -> {
                try(var options=new org.rocksdb.IngestExternalFileOptions().setMoveFiles(false).setIngestBehind(false).setWriteGlobalSeqno(false).setSnapshotConsistency(true).setAllowGlobalSeqNo(true).setAllowBlockingFlush(true)) {
                    for(var column:manifest.columns()) {
                        int width=manifest.limits().jniFilesPerBatch();
                        for(int start=0;start<column.files().size();start+=width){int end=Math.min(column.files().size(),start+width);var paths=new ArrayList<String>(end-start);for(int n=start;n<end;n++)paths.add(NativeRangeSstImport.original(root,manifest,column.files().get(n)).toString());db.ingestExternalFile(columns.get(column.columnId()),paths,options);observer.at(NativeRangeSstImport.Stage.BATCH_INGESTED);}
                        observer.at(NativeRangeSstImport.Stage.COLUMN_INGESTED);
                    }
                }
                for(var column:manifest.columns())for(var file:column.files())NativeRangeSstImport.verifyBytes(NativeRangeSstImport.original(root,manifest,file),file);
            },observer);
        } finally { nativeMutation.release(); }
    }

    public void activateImport(ReplicatedRangeStore store,NativeRangeSstImport.Activation authority) throws RocksDBException {
        nativeMutation.acquireUninterruptibly(); try { synchronized(admission) { if(closing || !ranges.contains(store))throw new IllegalStateException("Import store not owned"); } store.activateImportUnderPool(authority); } finally { nativeMutation.release(); }
    }

    public Map<UUID,String> schemaPhysicalNames(ReplicatedRangeStore store) {
        nativeMutation.acquireUninterruptibly();
        try { return schemaPhysicalNamesUnderMutation(store); } finally { nativeMutation.release(); }
    }
    private Map<UUID,String> schemaPhysicalNamesUnderMutation(ReplicatedRangeStore store) {
        if(!ranges.contains(store)) throw new IllegalArgumentException("Foreign range store");
        var names=new HashMap<UUID,String>(); for(var id:store.ownership().interval().columns()) names.put(id,Objects.requireNonNull(bindings.get(id)).name()); return Map.copyOf(names);
    }
    public final class SchemaInstallPlan implements AutoCloseable {
        private final DistributedStoragePool pool=DistributedStoragePool.this;
        private final ReplicatedRangeStore store;
        private final SchemaChange.Definition definition;
        private final Map<UUID,String> sourceNames,targetNames;
        private final Set<UUID> sourceMerges,targetMerges;
        private final NativeSchemaFence fence;
        private final Map<String,PreparedColumn> options;
        private volatile boolean closed;
        private final long heldBytes;
        private SchemaInstallPlan(ReplicatedRangeStore store,NativeSchemaInstallation record,Map<String,PreparedColumn> options) {
            this.store=store; definition=record.definition(); sourceNames=record.sourceNames(); targetNames=record.targetNames(); sourceMerges=record.sourceMerges(); targetMerges=record.targetMerges(); fence=record.fence(); this.options=Map.copyOf(options); heldBytes=record.encode().length;
            synchronized(admission) {
                if(closing || schemaPlans.size()>=64 || heldBytes>16L*1024*1024-schemaPlanBytes) throw new IllegalStateException("Pool schema preparation capacity exhausted");
                schemaPlans.add(this); schemaPlanBytes+=heldBytes;
            }
        }
        public SchemaChange.Definition definition() { return definition; }
        public Map<UUID,String> targetNames() { return targetNames; }
        public Set<UUID> targetMerges() { return targetMerges; }
        public long heldBytes() { return heldBytes; }
        public java.util.concurrent.CompletionStage<Void> closeAsync() { return submitSchemaCleanup(this,null); }
        @Override public void close() {
            nativeMutation.acquireUninterruptibly();
            try {
                if(closed) return; options.values().forEach(PreparedColumn::close); closed=true;
                synchronized(admission) { if(schemaPlans.remove(this)) schemaPlanBytes-=heldBytes; admission.notifyAll(); }
            } finally { nativeMutation.release(); }
        }
    }
    void registerSchemaCapture(ReplicatedRangeStore.SchemaInstallCaptureGate capture) {
        synchronized(admission) {
            if(closing || schemaCaptures.size()>=64) throw new IllegalStateException("Pool schema capture capacity exhausted"); schemaCaptures.add(capture);
        }
    }
    void unregisterSchemaCapture(ReplicatedRangeStore.SchemaInstallCaptureGate capture) {
        synchronized(admission) { schemaCaptures.remove(capture); admission.notifyAll(); }
    }
    /** Enqueues ownership cleanup only; completion follows actual releases outside pool locks. */
    public java.util.concurrent.CompletionStage<Void> submitSchemaCleanup(SchemaInstallPlan plan,ReplicatedRangeStore.SchemaInstallCaptureGate capture) {
        if(plan!=null && plan.pool!=this || capture!=null && !capture.belongsToPool(this)) throw new IllegalArgumentException("Foreign schema preparation owner");
        return submitPreparationCleanup(new Object[]{plan,capture});
    }
    public java.util.concurrent.CompletionStage<Void> submitTopologyCleanup(ReplicatedRangeStore.TopologyFreezeGate capture){
        if(capture!=null && !capture.belongsToPool(this))throw new IllegalArgumentException("Foreign topology preparation owner");
        return submitPreparationCleanup(new Object[]{capture});
    }
    private java.util.concurrent.CompletionStage<Void> submitPreparationCleanup(Object[] requested){
        var pending=new ArrayList<java.util.concurrent.CompletableFuture<Void>>(); var owners=new ArrayList<Object>();
        java.util.concurrent.CompletableFuture<Void> completion=null;
        java.util.concurrent.RejectedExecutionException rejected=null;
        synchronized(admission) {
            for(var owner:requested) {
                if(owner==null || owner instanceof SchemaInstallPlan p && p.closed || owner instanceof ReplicatedRangeStore.SchemaInstallCaptureGate c && c.closed() || owner instanceof ReplicatedRangeStore.TopologyFreezeGate t && t.closed()) continue;
                var existing=schemaCleanups.get(owner);
                if(existing!=null) pending.add(existing); else owners.add(owner);
            }
            if(!owners.isEmpty()) {
                completion=new java.util.concurrent.CompletableFuture<>(); var finished=completion; owners.forEach(owner -> schemaCleanups.put(owner,finished)); pending.add(finished);
                if(schemaCleanupExecutor==null) {
                    schemaCleanupExecutor=new java.util.concurrent.ThreadPoolExecutor(1,1,30,TimeUnit.SECONDS,new java.util.concurrent.ArrayBlockingQueue<>(128),task -> { var thread=new Thread(task,"rockserver-schema-cleanup"); thread.setDaemon(true); return thread; });
                    schemaCleanupExecutor.allowCoreThreadTimeOut(true);
                }
                try {
                    schemaCleanupExecutor.execute(() -> {
                        Throwable failure=null;
                        try {
                            for(var owner:owners) if(owner instanceof ReplicatedRangeStore.SchemaInstallCaptureGate c) c.close();
                            for(var owner:owners) if(owner instanceof ReplicatedRangeStore.TopologyFreezeGate t) t.close();
                            for(var owner:owners) if(owner instanceof SchemaInstallPlan p) p.close();
                        } catch(Throwable thrown) { failure=thrown; }
                        synchronized(admission) { owners.forEach(schemaCleanups::remove); admission.notifyAll(); }
                        if(failure==null) finished.complete(null); else finished.completeExceptionally(failure);
                    });
                } catch(java.util.concurrent.RejectedExecutionException rejection) {
                    owners.forEach(schemaCleanups::remove);
                    // The owners remain registered and charged, allowing an explicit retry.
                    rejected=rejection;
                }
            }
        }
        if(rejected!=null) completion.completeExceptionally(rejected);
        return java.util.concurrent.CompletableFuture.allOf(pending.toArray(java.util.concurrent.CompletableFuture[]::new)).minimalCompletionStage();
    }
    public SchemaInstallPlan prepareSchemaInstallation(ReplicatedRangeStore store,SchemaChange.Definition definition) throws RocksDBException {
        var saved=store.lookupSchemaInstallationRecord(definition.header());
        if(saved!=null) return prepareSchemaInstallation(store,definition,saved.targetNames(),saved.targetMerges());
        var names=new HashMap<>(schemaPhysicalNames(store)); var merges=new HashSet<>(store.deterministicMergeColumns());
        names.keySet().retainAll(definition.targetSchema().columnIds()); merges.retainAll(definition.targetSchema().columnIds());
        for(var id:definition.targetSchema().columnIds()) { names.putIfAbsent(id,"distributed-"+id); if(definition.targetSchema().column(id).operator()!=null) merges.add(id); }
        return prepareSchemaInstallation(store,definition,names,merges);
    }
    public SchemaInstallPlan prepareSchemaInstallation(ReplicatedRangeStore store,SchemaChange.Definition definition,Map<UUID,String> targetNames,Set<UUID> targetMerges) throws RocksDBException {
        nativeMutation.acquireUninterruptibly(); var options=new HashMap<String,PreparedColumn>(); boolean retained=false;
        try {
            synchronized(admission) { if(closing) throw new IllegalStateException("Pool is closing"); }
            if(!ranges.contains(store)) throw new IllegalArgumentException("Foreign range store");
            var saved=store.lookupSchemaInstallationRecord(definition.header());
            if(saved!=null) {
                saved.requireDefinition(definition); if(!saved.targetNames().equals(targetNames) || !saved.targetMerges().equals(targetMerges)) throw new IllegalArgumentException("Original INSTALL binding changed");
                var plan=new SchemaInstallPlan(store,saved,options); retained=true; return plan;
            }
            if(!Arrays.equals(store.schemaDigest(),definition.sourceSchema().digest())) throw new IllegalArgumentException("INSTALL source generation changed");
            var sourceNames=new HashMap<UUID,String>(); for(var id:definition.sourceSchema().columnIds()) sourceNames.put(id,Objects.requireNonNull(bindings.get(id)).name());
            var fence=store.lookupSchemaFence(definition.header()); if(fence==null) throw new IllegalStateException("Native FENCE missing");
            var cut=store.applied(); if(cut==null || cut.index()==Long.MAX_VALUE) throw new IllegalStateException("Native prefix unavailable");
            var record=new NativeSchemaInstallation(definition,fence,new ApplyPosition(Math.max(1,cut.term()),cut.index()+1),sourceNames,targetNames,store.deterministicMergeColumns(),targetMerges);
            var owner=store.ownership(); record.requireRange(owner.owner().groupId(),owner.owner().epoch(),owner.interval().domainId(),owner.interval().lowerInclusive(),owner.interval().upperExclusive());
            store.preflightSchemaInstallation(record);
            long held=record.encode().length;
            synchronized(admission) { if(closing || schemaPlans.size()>=64 || held>16L*1024*1024-schemaPlanBytes) throw new IllegalStateException("Pool schema preparation capacity exhausted"); }
            prepareSchemaOptions(record,options);
            var fresh=new HashSet<>(definition.targetSchema().columnIds()); fresh.removeAll(definition.sourceSchema().columnIds());
            proposedBirths(record.targetNames(),record.targetMerges(),definition.changeId(),fresh);
            var plan=new SchemaInstallPlan(store,record,options); retained=true; return plan;
        } finally { if(!retained) options.values().forEach(PreparedColumn::close); nativeMutation.release(); }
    }
    public NativeSchemaInstallation installSchema(ReplicatedRangeStore store,ApplyPosition position,SchemaInstallPlan plan) throws RocksDBException {
        Objects.requireNonNull(plan); nativeMutation.acquireUninterruptibly();
        try {
            if(plan.store!=store || plan.closed) throw new IllegalArgumentException("Foreign or closed INSTALL plan");
            var saved=store.lookupSchemaInstallationRecord(plan.definition.header());
            if(saved!=null) {
                if(!saved.sourceNames().equals(plan.sourceNames) || !saved.targetNames().equals(plan.targetNames) || !saved.sourceMerges().equals(plan.sourceMerges) || !saved.targetMerges().equals(plan.targetMerges)) throw new IllegalArgumentException("Original INSTALL plan changed");
                store.applyNoop(position); return saved;
            }
            var record=new NativeSchemaInstallation(plan.definition,plan.fence,position,plan.sourceNames,plan.targetNames,plan.sourceMerges,plan.targetMerges);
            return installPreparedSchema(store,record,stage -> {},plan.options);
        } finally { nativeMutation.release(); }
    }
    private void prepareSchemaOptions(NativeSchemaInstallation record,Map<String,PreparedColumn> options) {
        var definition=record.definition();
        var fresh=new HashSet<>(definition.targetSchema().columnIds()); fresh.removeAll(definition.sourceSchema().columnIds());
        for(var name:record.targetNames().values()) if(RESERVED_COLUMNS.contains(name)) throw new IllegalArgumentException("Reserved target CF name");
        proposedBirths(record.targetNames(),record.targetMerges(),definition.changeId(),fresh);
        for(var plan:schemaPlans) if(!plan.closed) for(var entry:record.targetNames().entrySet()) {
            var previous=plan.targetNames.get(entry.getKey());
            if(previous!=null && !previous.equals(entry.getValue()) || plan.targetNames.entrySet().stream().anyMatch(other -> !other.getKey().equals(entry.getKey()) && other.getValue().equals(entry.getValue()))) throw new IllegalArgumentException("Physical CF is reserved by another prepared INSTALL");
        }
        boolean qualified=artifacts.qualified() || definition.targetSchema().columns().stream().anyMatch(column -> column.operator()!=null);
        if(qualified) {
            if(!artifacts.qualified()) for(var name:record.sourceNames().values()) if(loaded.mergeOperators().get(name)!=null) throw new IllegalArgumentException("Configured operator requires qualified bootstrap before native open");
            for(var column:definition.targetSchema().columns()) if(record.targetMerges().contains(column.columnId())!=(column.operator()!=null)) throw new IllegalArgumentException("Target merge admission differs from schema operator");
            try { artifacts.validate(definition.sourceSchema(),record.sourceNames()); artifacts.validate(definition.targetSchema(),record.targetNames()); }
            catch(IOException failure) { throw new IllegalStateException("Cannot qualify native INSTALL operators",failure); }
        }
        for(var entry:record.targetNames().entrySet()) {
            var name=entry.getValue();
            if(handles.containsKey(name) && record.targetMerges().contains(entry.getKey()) && loaded.mergeOperators().get(name)==null) throw new IllegalArgumentException("Missing merge operator");
            if(!handles.containsKey(name)) {
                var column=definition.targetSchema().column(entry.getKey());
                RocksDBLoader.OperatorResolver resolver=qualified ? ignored -> artifacts.instantiate(column) : null;
                var prepared=new PreparedColumn(name,resolver); options.put(name,prepared);
                if(!qualified && prepared.options.mergeOperator()!=null) throw new UnsupportedOperationException("New configured merge CF requires trusted artifact staging");
                if(record.targetMerges().contains(entry.getKey()) && prepared.options.mergeOperator()==null) throw new IllegalArgumentException("Missing merge operator");
            }
        }
    }

    public enum SchemaInstallStage { INTENT, COLUMN_FAMILIES, COMMITTED }
    @FunctionalInterface public interface SchemaInstallObserver { void after(SchemaInstallStage stage) throws RocksDBException; }
    /** Applies one committed, source-qualified schema transition; target DATA admission stays fenced. */
    public NativeSchemaInstallation installSchema(ReplicatedRangeStore store,ApplyPosition position,SchemaChange.Definition definition,
            Map<UUID,String> targetNames,Set<UUID> targetMerges) throws RocksDBException {
        return installSchema(store,position,definition,targetNames,targetMerges,stage -> {});
    }
    public NativeSchemaInstallation installSchema(ReplicatedRangeStore store,ApplyPosition position,SchemaChange.Definition definition,
            Map<UUID,String> targetNames,Set<UUID> targetMerges,SchemaInstallObserver observer) throws RocksDBException {
        nativeMutation.acquireUninterruptibly();
        try {
            synchronized(admission) { if(closing) throw new IllegalStateException("Pool is closing"); }
            if(!ranges.contains(store)) throw new IllegalArgumentException("Store belongs to another pool");
            var previous=store.lookupSchemaInstallationRecord(definition.header());
            if(previous!=null) {
                previous.requireDefinition(definition);
                if(!previous.targetNames().equals(targetNames) || !previous.targetMerges().equals(targetMerges)) throw new IllegalArgumentException("Install binding changed");
                store.applyNoop(position); return previous;
            }
            var sourceNames=new HashMap<UUID,String>();
            for(var id:definition.sourceSchema().columnIds()) sourceNames.put(id,Objects.requireNonNull(bindings.get(id)).name());
            var record=new NativeSchemaInstallation(definition,Objects.requireNonNull(store.schemaFence()),position,sourceNames,targetNames,store.deterministicMergeColumns(),targetMerges);
            return installSchemaUnderLock(store,record,observer);
        } finally { nativeMutation.release(); }
    }
    private NativeSchemaInstallation installSchemaUnderLock(ReplicatedRangeStore store,NativeSchemaInstallation record) throws RocksDBException { return installSchemaUnderLock(store,record,stage -> {}); }
    private NativeSchemaInstallation installSchemaUnderLock(ReplicatedRangeStore store,NativeSchemaInstallation record,SchemaInstallObserver observer) throws RocksDBException {
        var options=new HashMap<String,PreparedColumn>();
        try {
            var owner=store.ownership(); record.requireRange(owner.owner().groupId(),owner.owner().epoch(),owner.interval().domainId(),owner.interval().lowerInclusive(),owner.interval().upperExclusive());
            store.preflightSchemaInstallation(record); prepareSchemaOptions(record,options);
            return installPreparedSchema(store,record,observer,options);
        } finally { options.values().forEach(PreparedColumn::close); }
    }
    private RangeOwnership.SchemaTransition qualifiedSchemaTransition(SchemaChange.Definition definition) throws RocksDBException {
        UUID domain=definition.sourceSchema().domainId();
        var descriptors=new HashMap<UUID,RangeDescriptor>(); for(var descriptor:definition.sourceDirectory().ranges()) descriptors.put(descriptor.groupId(),descriptor);
        var checkpoints=new HashMap<UUID,RecoveredSchema>(); var known=new ArrayList<NativeSchemaInstallation>();
        for(var descriptor:definition.sourceDirectory().ranges()) {
            var group=descriptor.groupId(); var marker=ByteBuffer.allocate(16).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).array();
            if(loaded.db().get().get(handles.get(STATE_COLUMN),marker)==null) continue;
            var checkpoint=recoverSchema(group); checkpoints.put(group,checkpoint); known.addAll(checkpoint.chain());
        }
        known.sort(java.util.Comparator.comparingLong(record -> record.definition().sourceSchema().schemaEpoch()));
        var expected=new HashMap<RangeOwnership.Owner,RangeOwnership.Interval>(); var actualSchemas=new HashMap<UUID,DistributedSchema>();
        for(var range:ranges) {
            var owner=range.ownership(); if(owner.state()==RangeOwnership.State.RETIRED || !owner.interval().domainId().equals(domain)) continue;
            expected.put(owner.owner(),owner.interval());
            var actual=range.installedSchema(); if(actual!=null) actualSchemas.put(owner.owner().groupId(),actual);
        }
        for(var entry:checkpoints.entrySet()) {
            var descriptor=descriptors.get(entry.getKey()); var checkpoint=entry.getValue(); var schema=actualSchemas.get(entry.getKey());
            if(schema==null && checkpoint.current()!=null) schema=checkpoint.current().definition().targetSchema();
            if(schema==null) {
                for(var record:known) if(record.definition().sourceDirectory().ranges().stream().anyMatch(member -> member.groupId().equals(entry.getKey()))) { schema=record.definition().sourceSchema(); break; }
            }
            if(schema==null) schema=definition.sourceSchema();
            if(checkpoint.pending()!=null && checkpoint.current()==null && !Arrays.equals(checkpoint.pending().sourceSchemaDigest(),schema.digest())) throw new IllegalStateException("Unopened source checkpoint differs from verified logical schema");
            var owner=new RangeOwnership.Owner(entry.getKey(),descriptor.rangeEpoch()); var interval=new RangeOwnership.Interval(domain,schema.columnIds(),descriptor.lowerInclusive(),descriptor.upperExclusive());
            for(var record:checkpoint.chain()) record.requireRange(owner.groupId(),owner.epoch(),domain,interval.lowerInclusive(),interval.upperExclusive());
            var previous=expected.putIfAbsent(owner,interval); if(previous!=null && !previous.equals(interval)) throw new IllegalStateException("Logical checkpoint differs from live geometry"); actualSchemas.put(entry.getKey(),schema);
        }
        var members=new HashSet<RangeOwnership.Owner>();
        for(var descriptor:definition.sourceDirectory().ranges()) {
            var schema=actualSchemas.get(descriptor.groupId()); if(schema==null) continue;
            boolean source=Arrays.equals(schema.digest(),definition.sourceSchema().digest()),target=Arrays.equals(schema.digest(),definition.targetSchema().digest());
            if(!source && !target) continue;
            if(target) {
                var checkpoint=checkpoints.get(descriptor.groupId()); var original=checkpoint==null ? null : checkpoint.chain().stream().filter(record -> record.definition().changeId().equals(definition.changeId())).findFirst().orElse(null);
                if(original==null) throw new IllegalStateException("Target checkpoint lacks original schema INSTALL"); original.requireDefinition(definition);
            }
            members.add(new RangeOwnership.Owner(descriptor.groupId(),descriptor.rangeEpoch()));
        }
        var recovery=ownership.prepareSchemaRecovery(domain,expected);
        return ownership.beginRecoveredSchemaTransition(definition.changeId(),domain,definition.sourceSchema().columnIds(),definition.targetSchema().columnIds(),recovery,members);
    }
    private NativeSchemaInstallation installPreparedSchema(ReplicatedRangeStore store,NativeSchemaInstallation record,SchemaInstallObserver observer,Map<String,PreparedColumn> options) throws RocksDBException {
        var definition=record.definition();
        store.preflightSchemaInstallation(record);
        var transition=qualifiedSchemaTransition(definition);
        if(!capacityDurable) forceCapacityClaims(capacityClaims);
        var transfer=capacityTransfer(store,record);
        var fresh=new HashSet<>(definition.targetSchema().columnIds()); fresh.removeAll(definition.sourceSchema().columnIds());
        if(artifacts.qualified() || definition.targetSchema().columns().stream().anyMatch(column -> column.operator()!=null)) {
            try { artifacts.bindPermanentPlainBirths(plainBirths(bindings)); artifacts.bind(definition.sourceSchema(),record.sourceNames()); artifacts.bind(definition.targetSchema(),record.targetNames()); }
            catch(IOException failure) { throw new IllegalStateException("Cannot publish committed native operator bindings",failure); }
        }
        reserveBirths(record.targetNames(),record.targetMerges(),definition.changeId(),fresh);
        var currentOwner=store.ownership(); retainOwnershipHistory(currentOwner.owner(),new RangeOwnership.Interval(currentOwner.interval().domainId(),record.targetNames().keySet(),currentOwner.interval().lowerInclusive(),currentOwner.interval().upperExclusive()));
        schemaOrigins.put(definition.sourceSchema().domainId(),definition);
        var result=store.installSchema(record,transition,observer,() -> {
            var target=new HashMap<UUID,ColumnFamilyHandle>();
            for(var entry:record.targetNames().entrySet()) {
                var handle=handles.get(entry.getValue());
                if(handle==null) {
                    handle=createColumn(Objects.requireNonNull(options.get(entry.getValue())));
                }
                target.put(entry.getKey(),handle);
            }
            return target;
        },transfer);
        for(var entry:record.targetNames().entrySet()) bindings.putIfAbsent(entry.getKey(),new Binding(entry.getValue(),record.targetMerges().contains(entry.getKey())));
        for(var id:record.sourceNames().keySet()) if(!record.targetNames().containsKey(id)) { deferredDrops.add(id); deletionOrigins.put(id,definition); }
        drainDeferredDrops(); pressureColumns=List.copyOf(handles.values()); ownership.tryCompleteSchemaTransition(transition);
        return result;
    }
    private void drainDeferredDrops() throws RocksDBException {
        var iterator=deferredDrops.iterator();
        while(iterator.hasNext()) {
            var id=iterator.next();
            if(ranges.stream().anyMatch(range -> !range.userHandlesDetached() && range.ownership().interval().columns().contains(id))) continue;
            var origin=Objects.requireNonNull(deletionOrigins.get(id)); boolean needed=false;
            for(var participant:origin.sourceDirectory().ranges()) {
                var group=participant.groupId(); var marker=ByteBuffer.allocate(16).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).array();
                if(loaded.db().get().get(handles.get(STATE_COLUMN),marker)==null) continue;
                var installed=loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(group,0x28));
                if(installed==null || NativeSchemaInstallation.decode(installed).definition().targetSchema().columnIds().contains(id)) { needed=true; break; }
            }
            if(needed) continue;
            var binding=bindings.get(id); if(binding==null) { iterator.remove(); continue; }
            var handle=handles.get(binding.name());
            if(handle!=null) {
                handleLifetime.writeLock().lock();
                try { loaded.db().get().dropColumnFamily(handle); handles.remove(binding.name()); pressureColumns=List.copyOf(handles.values()); handle.close();
                    var resources=columnResources.get(binding.name());
                    if(resources!=null) {
                        if(loaded.mergeOperators().get(binding.name())!=null) quarantinedColumnResources.add(resources);
                        else resources.close();
                        columnResources.remove(binding.name());
                    }
                    loaded.definitiveColumnFamilyOptionsMap().remove(binding.name()); loaded.mergeOperators().remove(binding.name());
                }
                finally { handleLifetime.writeLock().unlock(); }
            }
            iterator.remove(); deletionOrigins.remove(id);
        }
    }

    private final UUID maintenanceSession=UUID.randomUUID();
    public UUID maintenanceSession() { return maintenanceSession; }
    private volatile java.util.function.Consumer<UUID> localMaintenanceObserver=column -> {};
    /** Native lifetime remains held while this diagnostic boundary executes. */
    public void localMaintenanceObserver(java.util.function.Consumer<UUID> observer) { localMaintenanceObserver=Objects.requireNonNull(observer); }
    @FunctionalInterface private interface MaintenanceAction<T> { T run(org.rocksdb.RocksDB db,ColumnFamilyHandle column,List<String> paths) throws RocksDBException,IOException; }
    private <T>T localMaintenance(UUID session,UUID column,MaintenanceAction<T> action) throws RocksDBException,IOException {
        Objects.requireNonNull(session);Objects.requireNonNull(column);
        ColumnFamilyHandle handle;String name;List<String> paths;
        nativeMutation.acquireUninterruptibly();
        try {
            synchronized(admission){if(closing || closed)throw new IOException("Pool is closing");}
            if(!maintenanceSession.equals(session))throw new IllegalArgumentException("Stale physical maintenance session");
            var binding=bindings.get(column);if(binding==null || deferredDrops.contains(column))throw new IllegalArgumentException("Unknown or retired physical column");
            name=binding.name();handle=handles.get(name);if(handle==null)throw new IllegalArgumentException("Physical column is absent");
            paths=maintenancePaths(name);handleLifetime.readLock().lock();
        } finally {nativeMutation.release();}
        try {localMaintenanceObserver.accept(column);return action.run(loaded.db().get(),handle,paths);}
        finally {handleLifetime.readLock().unlock();}
    }
    /** Exact configured data/WAL/log paths; the caller groups their actual filesystems. No byte estimate. */
    public List<Path> storagePaths()throws IOException {
        handleLifetime.readLock().lock();try {
            synchronized(admission){if(closing || closed)throw new IOException("Pool is closing");}
            var paths=new java.util.LinkedHashSet<Path>(storageVolumes);paths.add(loaded.definitiveDbPath());
            for(String configured:new String[]{loaded.dbOptions().walDir(),loaded.dbOptions().dbLogDir()})if(configured!=null && !configured.isEmpty())paths.add(Path.of(configured));
            return paths.stream().map(path->path.toAbsolutePath().normalize()).toList();
        }finally{handleLifetime.readLock().unlock();}
    }
    /** Flush all actual CFs, including metadata, while drop/close cannot invalidate the handles. WAL sync setting is unchanged. */
    public void flushWholePool()throws RocksDBException,IOException {
        List<ColumnFamilyHandle> retained;nativeMutation.acquireUninterruptibly();
        try{synchronized(admission){if(closing || closed)throw new IOException("Pool is closing");}handleLifetime.readLock().lock();retained=List.copyOf(handles.values());}
        finally{nativeMutation.release();}
        try{localMaintenanceObserver.accept(maintenanceSession);try(var options=new org.rocksdb.FlushOptions().setWaitForFlush(true)){loaded.db().get().flush(options,retained);}}
        finally{handleLifetime.readLock().unlock();}
    }
    public void flushLocal(UUID session,UUID column) throws RocksDBException,IOException {
        localMaintenance(session,column,(db,handle,paths)->{try(var options=new org.rocksdb.FlushOptions().setWaitForFlush(true)){db.flush(options,handle);}return null;});
    }
    public void compactLocal(UUID session,UUID column) throws RocksDBException,IOException {
        localMaintenance(session,column,(db,handle,paths)->{db.compactRange(handle);return null;});
    }
    private List<String> maintenancePaths(String name) throws IOException {
        try {
            var selected=globalConfig.fallbackColumnOptions();
            for(var configured:globalConfig.columnOptions())if(configured.name().equals(name)){selected=configured;break;}
            return RocksDBLoader.getVolumeConfigs(loaded.definitiveDbPath(),selected).stream().map(path->path.path().toAbsolutePath().normalize().toString()).toList();
        } catch(GestaltException failure){throw new IOException("Physical column volume configuration unavailable",failure);}
    }
    public it.cavallium.rockserver.core.common.SstMaintenance.Metadata sstMetadataLocal(UUID session,UUID column,int level,int maxFiles,long maxBytes) throws RocksDBException,IOException {
        return localMaintenance(session,column,(db,handle,paths)->{var metadata=it.cavallium.rockserver.core.impl.SelectiveCompaction.boundedMetadata(db,handle,paths,session+"-"+handle.getID(),handle.getID(),level,maxFiles,maxBytes);return metadata;});
    }
    public it.cavallium.rockserver.core.common.SstMaintenance.Result compactFilesLocal(UUID session,UUID column,it.cavallium.rockserver.core.common.SstMaintenance.Request request,int maxFiles,long maxBytes) throws RocksDBException,IOException {
        return localMaintenance(session,column,(db,handle,paths)->{var metadata=it.cavallium.rockserver.core.impl.SelectiveCompaction.boundedMetadata(db,handle,paths,session+"-"+handle.getID(),handle.getID(),request.level(),maxFiles,maxBytes);return it.cavallium.rockserver.core.impl.SelectiveCompaction.compact(db,handle,metadata,request);});
    }
    private static long checkedMaintenanceProperty(long value) throws IOException {if(value<0)throw new IOException("Unsigned physical table property exceeds signed range");return value;}
    public record LocalTableProperties(long files,long entries,long deletions,long mergeOperands,long dataBytes,long indexBytes,long filterBytes) {}
    // One native inspection graph per pool: caller-bounded inventory plus <=1MiB property-block scratch.
    private final Semaphore tablePropertyInspection=new Semaphore(1);
    public LocalTableProperties tablePropertiesLocal(UUID session,UUID column,int maxFiles,long maxBytes) throws RocksDBException,IOException {
        if(maxFiles<1 || maxFiles>it.cavallium.rockserver.core.common.SstMaintenance.MAX_INPUT_FILES
                || maxBytes<1 || maxBytes>it.cavallium.rockserver.core.common.SstMaintenance.MAX_METADATA_RESPONSE_BYTES)throw new IllegalArgumentException("Invalid table inventory limit");
        if(!tablePropertyInspection.tryAcquire())throw new IOException("Physical table inspection is already active");
        try {
            return localMaintenance(session,column,(db,handle,paths)->{
                // Native inventory admission precedes selected-file copies; property blocks are scanned serially.
                var tables=db.getBoundedTableProperties(handle,maxFiles,maxBytes,1024*1024,4096,64*1024);
                return new LocalTableProperties(checkedMaintenanceProperty(tables.files),checkedMaintenanceProperty(tables.numEntries),
                        checkedMaintenanceProperty(tables.numDeletions),checkedMaintenanceProperty(tables.numMergeOperands),
                        checkedMaintenanceProperty(tables.dataSize),checkedMaintenanceProperty(tables.indexSize),checkedMaintenanceProperty(tables.filterSize));
            });
        }finally{tablePropertyInspection.release();}
    }

    /** Export a caller-owned cut with this pool's exact column options; no schema lock spans file I/O. */
    public NativeRangeSstExport.Manifest exportSst(Path destination, ReplicatedRangeStore.ScopedSnapshot snapshot,
            UUID column, byte[] lowerInclusive, byte[] upperExclusive, NativeRangeSstExport.Limits limits)
            throws IOException, RocksDBException {
        return exportSstPrepared(destination,snapshot,column,lowerInclusive,upperExclusive,limits,false).manifest();
    }
    public NativeRangeSstExport.Chunk exportSstChunk(Path destination,ReplicatedRangeStore.ScopedSnapshot snapshot,
            UUID column,byte[] lowerInclusive,byte[] upperExclusive,NativeRangeSstExport.Limits limits) throws IOException,RocksDBException {
        return exportSstPrepared(destination,snapshot,column,lowerInclusive,upperExclusive,limits,true);
    }
    private NativeRangeSstExport.Chunk exportSstPrepared(Path destination,ReplicatedRangeStore.ScopedSnapshot snapshot,
            UUID column,byte[] lowerInclusive,byte[] upperExclusive,NativeRangeSstExport.Limits limits,boolean chunk) throws IOException,RocksDBException {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(column, "column");
        ReplicatedRangeStore.NativeUse nativeUse;
        ColumnFamilyOptions columnOptions;
        nativeMutation.acquireUninterruptibly();
        try {
            synchronized (admission) {
                if (closing) throw new IllegalStateException("Pool is closing");
            }
            if (ranges.stream().noneMatch(snapshot::belongsTo)) {
                throw new IllegalArgumentException("Snapshot belongs to another pool");
            }
            Binding binding = bindings.get(column);
            if (binding == null || !snapshot.interval().columns().contains(column)) {
                throw new IllegalArgumentException("Column is outside the snapshot range");
            }
            columnOptions = Objects.requireNonNull(loaded.definitiveColumnFamilyOptionsMap().get(binding.name));
            // Pool close drains this range's pins before closing options, operators, caches or DB.
            nativeUse = snapshot.retainForExport();
        } finally { nativeMutation.release(); }
        NativeRangeSstExport.Prepared prepared=null;
        try {
            try (nativeUse) { prepared=NativeRangeSstExport.prepare(destination,snapshot,column,lowerInclusive,upperExclusive,loaded.dbOptions(),columnOptions,limits,chunk); }
            return NativeRangeSstExport.publish(prepared);
        } finally { NativeRangeSstExport.discard(prepared); }
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
        List<SchemaInstallPlan> plans; List<ReplicatedRangeStore.SchemaInstallCaptureGate> captures;
        synchronized(admission) { plans=List.copyOf(schemaPlans); captures=List.copyOf(schemaCaptures); }
        var cleanup=new ArrayList<java.util.concurrent.CompletableFuture<Void>>();
        synchronized(admission){cleanup.addAll(schemaCleanups.values());}
        for(var plan:plans) cleanup.add(submitSchemaCleanup(plan,null).toCompletableFuture());
        for(var capture:captures) cleanup.add(submitSchemaCleanup(null,capture).toCompletableFuture());
        try { java.util.concurrent.CompletableFuture.allOf(cleanup.toArray(java.util.concurrent.CompletableFuture[]::new)).get(Math.max(0,budget-(System.nanoTime()-started)),TimeUnit.NANOSECONDS); }
        catch(java.util.concurrent.TimeoutException timeoutFailure) { return false; }
        catch(java.util.concurrent.ExecutionException failure) { throw new IOException("Schema preparation cleanup did not release its owners",failure.getCause()); }
        if(!nativeMutation.tryAcquire(Math.max(0,budget-(System.nanoTime()-started)),TimeUnit.NANOSECONDS)) return false;
        try {
            synchronized (admission) { if (closed) return true; }
            if(pendingTopologySeed!=null) { pendingTopologySeed.transfer().close(); pendingTopologySeed=null; }
            boolean drained = true;
            for (var range : ranges) {
                drained &= range.close(Duration.ofNanos(Math.max(0, budget - (System.nanoTime() - started))));
            }
            if (!drained) return false;
            if(!handleLifetime.writeLock().tryLock(Math.max(0,budget-(System.nanoTime()-started)),TimeUnit.NANOSECONDS)) return false;
            try {
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
            var resourceIterator=columnResources.values().iterator();
            while(resourceIterator.hasNext()) try { resourceIterator.next().close(); resourceIterator.remove(); } catch(RuntimeException ex) { failure=append(failure,ex); }
            var quarantinedIterator=quarantinedColumnResources.iterator();
            while(quarantinedIterator.hasNext()) try { quarantinedIterator.next().close(); quarantinedIterator.remove(); } catch(RuntimeException ex) { failure=append(failure,ex); }
            try { loaded.refs().close(); } catch (RuntimeException ex) { failure = append(failure, ex); }
            synchronized(admission) { closed=failure==null; if(closed && schemaCleanupExecutor!=null) schemaCleanupExecutor.shutdown(); }
            if (failure != null) throw failure;
            return true;
            } finally { handleLifetime.writeLock().unlock(); }
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
