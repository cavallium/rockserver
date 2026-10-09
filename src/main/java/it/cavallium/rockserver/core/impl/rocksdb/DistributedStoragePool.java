package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import it.cavallium.rockserver.core.config.GlobalDatabaseConfig;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
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

    private final RocksDBLoader.LoadedDb loaded;
    private final GlobalDatabaseConfig globalConfig;
    private final Logger logger;
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
    private List<Path> storageVolumes = List.of();
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
    private final Map<String,RocksDBObjects> columnResources=new HashMap<>();
    private final List<RocksDBObjects> quarantinedColumnResources=new ArrayList<>();
    private final class PreparedColumn implements AutoCloseable {
        final String name; final RocksDBObjects resources=new RocksDBObjects(); final RocksDBLoader.ColumnOptionsWithMerge options;
        boolean adopted;
        PreparedColumn(String name) {
            this.name=name;
            try { options=RocksDBLoader.getColumnOptions(name,loaded.path(),loaded.definitiveDbPath(),globalConfig,logger,resources,false,loaded.caches()); }
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
    private void recoverBirths() {
        try {
            var encoded=loaded.db().get().get(handles.get(STATE_COLUMN),BIRTHS_KEY); if(encoded==null) return;
            if(encoded.length>MAX_BINDING_BYTES) throw new IOException("Birth registry exceeds budget");
            var input=new DataInputStream(new ByteArrayInputStream(encoded)); int count=input.readInt();
            if(count<0 || count>MAX_BINDINGS) throw new IOException("Birth registry count exceeds budget");
            var names=new HashSet<String>();
            for(int i=0;i<count;i++) {
                var id=new UUID(input.readLong(),input.readLong()); var birth=new UUID(input.readLong(),input.readLong());
                int kind=input.readUnsignedByte(); if(kind<1 || kind>BirthKind.values().length) throw new IOException("Invalid CF birth kind");
                var birthKind=BirthKind.values()[kind-1];
                boolean merge=input.readBoolean(); String name=input.readUTF();
                if(name.isEmpty() || name.length()>4096 || RESERVED_COLUMNS.contains(name) || !StandardCharsets.US_ASCII.newEncoder().canEncode(name)
                        || name.indexOf('\0')>=0 || !names.add(name) || bindings.put(id,new Binding(name,merge,birthKind,birth))!=null) throw new IOException("Invalid birth identity");
            }
            if(input.available()!=0) throw new IOException("Trailing birth registry bytes");
        } catch(IOException|RocksDBException failure) { throw new IllegalStateException("Cannot recover native CF births",failure); }
    }
    private void reserveBirths(Map<UUID,String> names,Set<UUID> merges,UUID change,Set<UUID> fresh) throws RocksDBException {
        var next=new HashMap<>(bindings);
        for(var entry:names.entrySet()) {
            var old=next.get(entry.getKey()); var name=entry.getValue();
            if(old!=null && (!old.name().equals(name) || old.mergeAdmitted()!=merges.contains(entry.getKey()) || fresh.contains(entry.getKey()) && (old.birthKind()!=BirthKind.CREATE || !old.birthChange().equals(change)))) throw new IllegalArgumentException("Column birth changed");
            if(next.entrySet().stream().anyMatch(other -> !other.getKey().equals(entry.getKey()) && other.getValue().name().equals(name))) throw new IllegalArgumentException("Physical CF name is already reserved");
            if(fresh.contains(entry.getKey()) && old==null && handles.containsKey(name)) throw new IllegalArgumentException("CREATE cannot adopt an unbound physical CF");
            next.putIfAbsent(entry.getKey(),new Binding(name,merges.contains(entry.getKey()),fresh.contains(entry.getKey()) ? BirthKind.CREATE : BirthKind.BOOTSTRAP,fresh.contains(entry.getKey()) ? change : BOOTSTRAP_BIRTH));
        }
        if(next.size()>MAX_BINDINGS) throw new IllegalStateException("Native CF birth budget exhausted");
        if(next.equals(bindings) && birthsDurable) return;
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes); output.writeInt(next.size());
            for(var id:next.keySet().stream().sorted().toList()) {
                var binding=next.get(id); output.writeLong(id.getMostSignificantBits()); output.writeLong(id.getLeastSignificantBits());
                output.writeLong(binding.birthChange().getMostSignificantBits()); output.writeLong(binding.birthChange().getLeastSignificantBits()); output.writeByte(binding.birthKind().ordinal()+1); output.writeBoolean(binding.mergeAdmitted()); output.writeUTF(binding.name());
            }
            output.flush(); if(bytes.size()>MAX_BINDING_BYTES) throw new IllegalStateException("Native CF birth byte budget exhausted");
            // Unknown native completion keeps every proposed reservation until a successful force.
            birthsDurable=false; bindings.clear(); bindings.putAll(next);
            try(var options=new org.rocksdb.WriteOptions().setSync(true).setDisableWAL(false)) { loaded.db().get().put(handles.get(STATE_COLUMN),options,BIRTHS_KEY,bytes.toByteArray()); }
            birthsDurable=true;
        } catch(IOException impossible) { throw new AssertionError(impossible); }
    }

    private DistributedStoragePool(RocksDBLoader.LoadedDb loaded, GlobalDatabaseConfig globalConfig, Logger logger) {
        this.loaded = loaded;
        this.globalConfig = globalConfig;
        this.logger = logger;
        loaded.db().getStartupColumns().forEach((descriptor, handle) ->
                handles.put(new String(descriptor.getName(), StandardCharsets.US_ASCII), handle));
        recoverBirths();
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
        var nativeLoaded=RocksDBLoader.load(persistentPath,config,logger,null,null,INTERNAL_COLUMNS);
        try { return new DistributedStoragePool(nativeLoaded,globalConfig,logger); }
        catch(RuntimeException|Error failure) {
            try { nativeLoaded.db().close(); } catch(Exception cleanup) { failure.addSuppressed(cleanup); }
            if(!nativeLoaded.db().get().isOwningHandle()) try { nativeLoaded.refs().close(); } catch(RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Configure once, before opening any range. Every native snapshot in this pool shares the guard. */
    public void configureSnapshotAdmission(PoolSnapshotAdmission.Limits limits) {
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
            snapshotAdmission = new PoolSnapshotAdmission(limits, this::sampleSnapshotPressure, System::nanoTime);
        } finally { nativeMutation.release(); }
    }

    /** Metadata only; estimated bytes do not measure exact physical SST retention. */
    public PoolSnapshotAdmission.Stats snapshotAdmissionStats() {
        var guard = snapshotAdmission;
        return guard == null ? new PoolSnapshotAdmission.Stats(0, 0, Duration.ZERO) : guard.stats();
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

    /** Bind the supplied pool-schema names; admission is supplied by the qualified range adapter. */
    public ReplicatedRangeStore openRange(RangeOwnership.Owner owner, RangeOwnership.Interval interval,
            byte[] schemaDigest, Map<UUID, String> physicalColumns, Set<UUID> admittedMergeColumns)
            throws RocksDBException {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(interval, "interval");
        byte[] schema = schemaDigest.clone();
        var names = Map.copyOf(physicalColumns);
        var merges = Set.copyOf(admittedMergeColumns);
        NativeSchemaInstallation recovered=null;
        RangeOwnership.SchemaTransition recoveryTransition=null;
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
            var installed=loaded.db().get().get(handles.get(STATE_COLUMN),groupSchemaKey(owner.groupId(),0x28));
            if(installed!=null) {
                recovered=NativeSchemaInstallation.decode(installed);
                recovered.requireRange(owner.groupId(),owner.epoch(),interval.domainId(),interval.lowerInclusive(),interval.upperExclusive());
                var definition=recovered.definition(); schemaOrigins.put(definition.sourceSchema().domainId(),definition);
                if(!definition.sourceSchema().domainId().equals(interval.domainId())
                        || !recovered.fence().participant().groupId().equals(owner.groupId()) || recovered.fence().participant().rangeEpoch()!=owner.epoch()
                        || !Arrays.equals(schema,definition.sourceSchema().digest()) && !Arrays.equals(schema,definition.targetSchema().digest())) throw new IllegalArgumentException("Bootstrap differs from durable installed generation");
                recoveryTransition=ownership.beginSchemaTransition(definition.changeId(),interval.domainId(),definition.sourceSchema().columnIds(),definition.targetSchema().columnIds());
                schema=definition.targetSchema().digest(); names=recovered.targetNames(); merges=recovered.targetMerges();
                interval=new RangeOwnership.Interval(interval.domainId(),names.keySet(),interval.lowerInclusive(),interval.upperExclusive());
            }
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
            if(recoveryTransition==null) {
                for(var origin:schemaOrigins.values()) if(origin.sourceSchema().domainId().equals(interval.domainId())
                        && Arrays.equals(origin.sourceSchema().digest(),schema) && origin.sourceSchema().columnIds().equals(interval.columns())
                        && origin.sourceDirectory().ranges().stream().anyMatch(participant -> participant.groupId().equals(owner.groupId()) && participant.rangeEpoch()==owner.epoch())) {
                    recoveryTransition=ownership.beginSchemaTransition(origin.changeId(),interval.domainId(),origin.sourceSchema().columnIds(),origin.targetSchema().columnIds()); break;
                }
            }
            if(recoveryTransition==null) ownership.register(owner, interval, RangeOwnership.State.ACTIVE);
            else ownership.registerSchema(owner,interval,RangeOwnership.State.FENCED,recoveryTransition);
            try {
                var store = new ReplicatedRangeStore(loaded.db().get(), ownership, owner, schema, rangeColumns,
                        handles.get(STATE_COLUMN), handles.get(RECEIPTS_COLUMN), merges);
                store.configureSnapshotAdmission(snapshotAdmission);
                for(var entry:names.entrySet()) bindings.putIfAbsent(entry.getKey(),new Binding(entry.getValue(),merges.contains(entry.getKey())));
                ranges.add(store);
                if(recoveryTransition!=null) ownership.activate(owner);
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
                ownership.retire(owner);
                throw failure;
            }
        } finally {
            nativeMutation.release();
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
            if(store.schemaInstallation()!=null && store.schemaInstallation().definition().changeId().equals(definition.changeId())) {
                var previous=store.schemaInstallation(); previous.requireDefinition(definition);
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
        var definition=record.definition();
        var nativeOwner=store.ownership();
        record.requireRange(nativeOwner.owner().groupId(),nativeOwner.owner().epoch(),nativeOwner.interval().domainId(),nativeOwner.interval().lowerInclusive(),nativeOwner.interval().upperExclusive());
        var transition=ownership.beginSchemaTransition(definition.changeId(),definition.sourceSchema().domainId(),definition.sourceSchema().columnIds(),definition.targetSchema().columnIds());
        schemaOrigins.put(definition.sourceSchema().domainId(),definition);
        var preparedOptions=new HashMap<String,PreparedColumn>();
        try {
        // All binding and merge checks occur before the forced intent and native DDL.
        for(var entry:record.targetNames().entrySet()) {
            var name=entry.getValue();
            if(RESERVED_COLUMNS.contains(name)) throw new IllegalArgumentException("Reserved target CF name");
            var old=bindings.get(entry.getKey());
            if(old!=null && (!old.name().equals(name) || old.mergeAdmitted()!=record.targetMerges().contains(entry.getKey()))) throw new IllegalArgumentException("Column identity rebound");
            if(bindings.entrySet().stream().anyMatch(other -> !other.getKey().equals(entry.getKey()) && other.getValue().name().equals(name))) throw new IllegalArgumentException("Physical name belongs to another identity");
            if(!definition.sourceSchema().columnIds().contains(entry.getKey()) && definition.targetSchema().column(entry.getKey()).operator()!=null)
                throw new UnsupportedOperationException("New merge column requires admitted operator artifact staging");
            if(handles.containsKey(name) && record.targetMerges().contains(entry.getKey()) && loaded.mergeOperators().get(name)==null) throw new IllegalArgumentException("Missing merge operator");
            if(!handles.containsKey(name)) {
                var prepared=new PreparedColumn(name); preparedOptions.put(name,prepared);
                if(prepared.options.mergeOperator()!=null) throw new UnsupportedOperationException("New configured merge CF requires operator background-work lifecycle");
                if(record.targetMerges().contains(entry.getKey()) && prepared.options.mergeOperator()==null) throw new IllegalArgumentException("Missing merge operator");
            }
        }
        var fresh=new HashSet<>(definition.targetSchema().columnIds()); fresh.removeAll(definition.sourceSchema().columnIds());
        reserveBirths(record.targetNames(),record.targetMerges(),definition.changeId(),fresh);
        var result=store.installSchema(record,transition,observer,() -> {
            var target=new HashMap<UUID,ColumnFamilyHandle>();
            for(var entry:record.targetNames().entrySet()) {
                var handle=handles.get(entry.getValue());
                if(handle==null) {
                    handle=createColumn(Objects.requireNonNull(preparedOptions.get(entry.getValue())));
                }
                target.put(entry.getKey(),handle);
            }
            return target;
        });
        for(var entry:record.targetNames().entrySet()) bindings.putIfAbsent(entry.getKey(),new Binding(entry.getValue(),record.targetMerges().contains(entry.getKey())));
        for(var id:record.sourceNames().keySet()) if(!record.targetNames().containsKey(id)) { deferredDrops.add(id); deletionOrigins.put(id,definition); }
        drainDeferredDrops(); pressureColumns=List.copyOf(handles.values());
        return result;
        } finally { preparedOptions.values().forEach(PreparedColumn::close); }
    }
    private void drainDeferredDrops() throws RocksDBException {
        var iterator=deferredDrops.iterator();
        while(iterator.hasNext()) {
            var id=iterator.next();
            if(ranges.stream().anyMatch(range -> range.ownership().interval().columns().contains(id))) continue;
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
        if (!nativeMutation.tryAcquire(budget, TimeUnit.NANOSECONDS)) return false;
        try {
            synchronized (admission) { if (closed) return true; }
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
            synchronized (admission) { closed = failure==null; }
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
