package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.SnapshotFence;

import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import it.cavallium.rockserver.core.common.distributed.NativeTopologySeed;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyRecoveryFence;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerRegistration;
import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyActivation;
import it.cavallium.rockserver.core.common.distributed.TopologyChange;
import it.cavallium.rockserver.core.common.distributed.OperatorUpload;
import it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor;
import it.cavallium.rockserver.core.common.distributed.RangeDescriptor;
import it.cavallium.rockserver.core.common.distributed.NativeSchemaActivationProof;
import it.cavallium.rockserver.core.common.distributed.RangeDirectory;
import it.cavallium.rockserver.core.common.distributed.RangeDirectoryCodec;
import it.cavallium.rockserver.core.common.distributed.SchemaChange;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongSupplier;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;

/** Bootstrap and forced-topology node owner. Running means verified divisions, not quorum/read/API readiness. */
public final class RatisNodeRuntime implements AutoCloseable {
    public enum State { OPEN, STARTING, RUNNING, STOPPING, CLOSED }
    public enum Startup { RECOVER, AUTHORIZED_INITIAL_PLACEMENT }
    /** Creation must not expose transport or start any group; the runtime seals exposure before calling start. */
    @FunctionalInterface interface ServerFactory {
        RaftServer create(RatisNodePlan plan,Path logs,Map<RaftGroupId,RatisRangeStateMachine> machines) throws IOException;
    }
    private static final ServerFactory DEFAULT_SERVER_FACTORY=RatisNodeRuntime::buildServer;
    @FunctionalInterface interface RestoreNativeInitialization { void prepare(RatisNodeResourceBudget resources)throws IOException; }
    @FunctionalInterface interface SchemaReplicaClientFactory { RatisSchemaReplicaClient create() throws IOException; }
    @FunctionalInterface interface HomeClientFactory { RatisRoutedClient connect(it.cavallium.rockserver.core.common.distributed.RangeDirectory directory) throws IOException; }
    public record ResourceLimits(RatisNodeResourceBudget.Limits node,RatisNodeResourceBudget.Limits pool,long maximumSourceImageDiskBytes,long maximumDurableMetadataBytes,long maximumOperatorCacheBytes,long maximumNativeCacheBytes,RatisCommittedApplyDispatcher.Policy committedApply,RatisDiskRunway.Policy diskRunway,RatisMigrationController.Policy migration,RatisNativeExtentPolicy nativeExtents) {
        public ResourceLimits {Objects.requireNonNull(nativeExtents);Objects.requireNonNull(migration);if(migration.burstBytes()<org.apache.ratis.server.leader.MigrationLogStateMachine.MAX_TAIL_BYTES || migration.poolRecoveryBytes()<RatisTopologySourceSnapshotService.TRANSFER_BYTES)throw new IllegalArgumentException("Migration policy cannot admit legal log heads/recovery bulk");Objects.requireNonNull(committedApply);Objects.requireNonNull(diskRunway);if(maximumNativeCacheBytes<1 || maximumNativeCacheBytes>RatisNodeResourceBudget.MAX_BYTES)throw new IllegalArgumentException("Invalid node native cache/WBM ceiling");if(maximumOperatorCacheBytes<1 || maximumOperatorCacheBytes>RatisNodeResourceBudget.MAX_BYTES)throw new IllegalArgumentException("Invalid node operator cache ceiling");if(maximumDurableMetadataBytes<1 || maximumDurableMetadataBytes>RatisNodeResourceBudget.MAX_BYTES)throw new IllegalArgumentException("Invalid canonical node metadata ceiling");Objects.requireNonNull(node);Objects.requireNonNull(pool);if(maximumSourceImageDiskBytes<1 || maximumSourceImageDiskBytes>RatisNodeResourceBudget.MAX_BYTES)throw new IllegalArgumentException("Invalid node source image disk budget");}
        public static ResourceLimits defaults() {
            return new ResourceLimits(new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(256L*1024*1024,1024),new RatisNodeResourceBudget.Lane(1L<<30,256)),
                    new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(128L*1024*1024,512),new RatisNodeResourceBudget.Lane(512L<<20,128)),1L<<30,1L<<40,16L<<30,16L<<30,RatisCommittedApplyDispatcher.Policy.defaults(),RatisDiskRunway.Policy.defaults(),RatisMigrationController.Policy.defaults(),RatisNativeExtentPolicy.defaults());
        }
    }
    public record Settings(NativeRangeImage.Limits images,RatisRangeStateMachine.RetentionLimits retention,
                           Duration clockPeriod,int propagationVisits,int maxPropagationRanges,Duration shutdownTimeout,
                           ExportResources exports,ResourceLimits resources) {
        public Settings {
            Objects.requireNonNull(images); Objects.requireNonNull(retention); Objects.requireNonNull(clockPeriod); Objects.requireNonNull(shutdownTimeout); Objects.requireNonNull(exports);Objects.requireNonNull(resources);
            if (clockPeriod.isNegative() || clockPeriod.isZero() || propagationVisits<1 || propagationVisits>32
                    || maxPropagationRanges<1 || maxPropagationRanges>1024 || shutdownTimeout.isNegative() || shutdownTimeout.isZero()
                    || shutdownTimeout.compareTo(Duration.ofMinutes(1))>0) throw new IllegalArgumentException("Invalid node resource/lifecycle settings");
        }
    }
    public record ExportResources(PoolSnapshotAdmission.Limits pins,RatisExportSourceRegistry.Budgets sources,
                                  RatisExportSourceService.IoLimits io) {
        public ExportResources { Objects.requireNonNull(pins); Objects.requireNonNull(sources); Objects.requireNonNull(io); }
        public static ExportResources defaults() {
            var source=RatisExportSourceRegistry.Budgets.defaults();
            return new ExportResources(PoolSnapshotAdmission.Limits.defaults(),
                    new RatisExportSourceRegistry.Budgets(source.maximumJobs(),source.maximumChunks(),source.maximumDiskBytes(),
                            source.maximumMetadataBytes(),source.maximumBufferBytes(),1),RatisExportSourceService.IoLimits.defaults());
        }
    }
    /** Node-local trust is injected, never accepted from a schema request or serialized as a lambda. */
    public record SchemaReplicaConfig(RatisSchemaReplicaService.Limits limits,RatisSchemaReplicaService.OperatorPolicy policy) {
        public SchemaReplicaConfig { Objects.requireNonNull(limits); Objects.requireNonNull(policy); }
        public static SchemaReplicaConfig defaults() {
            return new SchemaReplicaConfig(RatisSchemaReplicaService.Limits.defaults(),operator -> { throw new IOException("Node operator policy has not admitted this artifact"); });
        }
    }
    public record RecoverySettings(RatisTransactionStatuses homes,boolean takeover,RatisParticipantRecoveryRegistry.Limits limits,Duration period,int visits) {
        public RecoverySettings {
            Objects.requireNonNull(homes); Objects.requireNonNull(limits); Objects.requireNonNull(period);
            if (period.isNegative() || period.isZero() || period.compareTo(Duration.ofMinutes(1))>0 || visits<1 || visits>32) throw new IllegalArgumentException("Invalid participant recovery settings");
        }
    }
    private final RatisNodeResourceBudget resources;private final RatisMigrationController migration;
    private final RatisNodePlan plan; private final Map<UUID,DistributedSchema> logicalSchemas; private final Path root; private final Settings settings;
    private final SchemaReplicaConfig schemaReplicaConfig;
    private final RatisSessionEpochs epochs; private final Map<UUID,RatisRoutedClient> routedClients; private final RatisSnapshotFences snapshotHomes;
    private final RatisCommittedApplyDispatcher committedDispatcher;
    private final RatisLogResourceBudget.Shared logNode=new RatisLogResourceBudget.Shared(RatisLogResourceBudget.Limits.defaults());private final Map<UUID,RatisLogResourceBudget.Shared> logPools=new HashMap<>();private final Map<UUID,RatisLogResourceBudget> logGroups=new ConcurrentHashMap<>();private org.apache.ratis.server.impl.LogResourceStateMachine.Registration logRegistration;
    private final RatisDiskRunway diskRunway;private RatisDiskRunwayService diskCensus;private RatisDiskRunwayRecovery diskRecovery;private final org.apache.ratis.protocol.ClientId snapshotClient=org.apache.ratis.protocol.ClientId.randomId();private final java.util.concurrent.atomic.AtomicLong snapshotCalls=new java.util.concurrent.atomic.AtomicLong();
    private static final long MAX_COMMITTED_ENTRY_BYTES=it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_FILE_BYTES+4096;
    private final RWScheduler scheduler; private final Scheduler timer; private final LongSupplier time;
    private final Map<RaftGroupId,RatisRangeStateMachine> machines=new ConcurrentHashMap<>();
    private final Map<UUID,ReplicatedRangeStore> stores=new ConcurrentHashMap<>();
    private final RatisRuntimeGroups groups;
    private RatisSnapshotFences captureHomes;private RatisTransactionStatuses transactionHomes;
    @FunctionalInterface interface SeedAuthorityReader {RatisTopologyAuthority.View read(TopologyChange.Header header,TopologyChange.Stage stage)throws IOException;}
    private SeedAuthorityReader seedAuthority;
    @FunctionalInterface interface JoinAuthorityReader { RatisTopologyJoinAuthority.View read(RatisTopologyJoinAuthority.Request request)throws IOException; }
    private JoinAuthorityReader joinAuthority;
    /** The trusted adapter owns the source division, snapshot pin and immutable image artifact. */
    @FunctionalInterface public interface LearnerSourceReader { LearnerSourceEvidence read(RatisTopologyJoinAuthority.View authority,ReplicaDescriptor receiver)throws IOException; }
    public record LearnerSourceEvidence(NativeRangeImage.Manifest image,DistributedSchema schema,ReplicaDescriptor sourceOwner,
                                       ReplicatedRangeStore.Configuration configuration,ApplyPosition completedCut,
                                       Map<UUID,String> physicalNames,Set<UUID> mergeColumns,Map<String,byte[]> sourceSchemaRows,UUID sourceTransferId) {
        public LearnerSourceEvidence { Objects.requireNonNull(sourceTransferId);Objects.requireNonNull(image);Objects.requireNonNull(schema);Objects.requireNonNull(sourceOwner);Objects.requireNonNull(configuration);Objects.requireNonNull(completedCut);physicalNames=Map.copyOf(physicalNames);mergeColumns=Set.copyOf(mergeColumns);var rows=new HashMap<String,byte[]>();sourceSchemaRows.forEach((key,value)->rows.put(key,value.clone()));sourceSchemaRows=Map.copyOf(rows); }
        @Override public Map<String,byte[]> sourceSchemaRows(){var copy=new HashMap<String,byte[]>();sourceSchemaRows.forEach((key,value)->copy.put(key,value.clone()));return Map.copyOf(copy);}
    }
    private LearnerSourceReader learnerSource=this::loadLearnerSource;
    private NativeTopologyLearnerRegistration learnerAttempt;private UUID learnerAttemptPool;
    synchronized void relocationTransport(RatisClientPool.Factory leaders,RatisClientPool.OwnerFactory owners)throws IOException{
        if(started || starting || installing || closeRequested || closing || closed)throw new IllegalStateException("Relocation transport fixed before start");var replacement=new RatisRelocationDataClient(plan.clusterId(),plan.databaseId(),plan.catalogue().groupId(),leaders,owners);try{relocationClients.close();}catch(IOException|RuntimeException failed){try{replacement.close();}catch(IOException cleanup){failed.addSuppressed(cleanup);}throw failed;}relocationClients=replacement;
    }
    public synchronized void learnerSource(LearnerSourceReader reader){if(started || starting || installing || closeRequested)throw new IllegalStateException("Learner source policy is fixed before start");learnerSource=Objects.requireNonNull(reader);}

    private record SeedAttempt(TopologyChange.Definition definition,TopologyChange.State authority,ReplicaDescriptor local,ApplyPosition catalogueCut) {}
    private SeedAttempt seedAttempt;
    private RatisTopologyOwnerClient topologyOwners;private boolean topologyOwnersClosed;
    private long groupManagementCall;
    private final Set<UUID> virginSeedAdds=new HashSet<>();
    private static final int NODE_IDENTITY_BYTES=65+RatisNativeExtentPolicy.BYTES;
    private org.rocksdb.NativeExtentEnv nativeExtentEnv;private boolean nativeExtentBound;
    private final Map<UUID,PoolOwner> pools=new LinkedHashMap<>();private final Map<UUID,UUID> groupPools=new ConcurrentHashMap<>(); private RaftServer server; private RatisCatalogueClock clock;
    static RatisNodeResourceBudget.Limits poolResources(RatisNodeResourceBudget.Limits spec,RatisNodeResourceBudget.Limits ceiling){return new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(Math.min(spec.ordinary().bytes(),ceiling.ordinary().bytes()),Math.min(spec.ordinary().owners(),ceiling.ordinary().owners())),new RatisNodeResourceBudget.Lane(Math.min(spec.protectedLane().bytes(),ceiling.protectedLane().bytes()),Math.min(spec.protectedLane().owners(),ceiling.protectedLane().owners())));}
    private RatisMetadataClaims metadataClaims;
    private final class PoolOwner{final RatisNativePoolSpec spec;final DistributedStoragePool pool;RatisSchemaReplicaService schemas;RatisSnapshotCohortService cohorts;RatisTopologySourceSnapshotArtifacts artifacts;RatisTopologyRecoveryAnchors anchors;RatisTopologySourceSnapshotService sources;final java.util.concurrent.ConcurrentHashMap<UUID,AutoCloseable> sourceCleanup=new java.util.concurrent.ConcurrentHashMap<>();boolean closed;PoolOwner(RatisNativePoolSpec spec,DistributedStoragePool pool){this.spec=spec;this.pool=pool;}NativeRangeImage.Limits images(){var value=spec.images();return new NativeRangeImage.Limits(Math.min(value.maxRowBytes(),settings.images().maxRowBytes()),Math.min(value.maxImageBytes(),settings.images().maxImageBytes()),Math.min(value.maxColumns(),settings.images().maxColumns()));}RatisRangeStateMachine.RetentionLimits retention(){var value=spec.retention();var node=settings.retention();return new RatisRangeStateMachine.RetentionLimits(Math.min(value.dataCount(),node.dataCount()),Math.min(value.dataBytes(),node.dataBytes()),Math.min(value.maintenanceCount(),node.maintenanceCount()),Math.min(value.maintenanceBytes(),node.maintenanceBytes()));}PoolSnapshotAdmission.Limits pins(){var value=spec.pins();var node=settings.exports().pins();return new PoolSnapshotAdmission.Limits(Math.min(value.maximumPins(),node.maximumPins()),Math.min(value.maximumEstimatedBytes(),node.maximumEstimatedBytes()),value.maximumAge().compareTo(node.maximumAge())<0?value.maximumAge():node.maximumAge(),Math.max(value.minimumFreeBytes(),node.minimumFreeBytes()));}}
    public UUID poolId(UUID group){return owner(group).spec.id();}DistributedStoragePool nativePool(UUID poolId){var owner=pools.get(poolId);if(owner==null)throw new IllegalArgumentException("Undeclared pool");return owner.pool;}
    private PoolOwner owner(UUID group){var id=groupPools.get(group);var owner=id==null?null:pools.get(id);if(owner==null)throw new IllegalArgumentException("Group has no opened native pool: "+group);return owner;}
    private void requirePoolCapacity(UUID pool,Collection<UUID> requested){requireGroupCapacity(requested);var proposed=new HashSet<UUID>();groupPools.forEach((group,id)->{if(id.equals(pool))proposed.add(group);});proposed.addAll(requested);if(seedAttempt!=null && pool.equals(groupPools.get(seedAttempt.definition().parents().getFirst().groupId())))seedAttempt.definition().targets().forEach(target->proposed.add(target.groupId()));if(learnerAttempt!=null && pool.equals(learnerAttemptPool))proposed.add(learnerAttempt.definition().targets().getFirst().groupId());if(proposed.size()>Math.min(plan.pools().get(pool).maximumGroups(),settings.resources().committedApply().pool().groups()))throw new IllegalArgumentException("Physical pool group ceiling exhausted");}
    private void requireGroupCapacity(Collection<UUID> requested){
        groups.requireCapacity(requested);var proposed=new HashSet<>(groupPools.keySet());proposed.addAll(requested);if(seedAttempt!=null)seedAttempt.definition().targets().forEach(target->proposed.add(target.groupId()));if(learnerAttempt!=null)proposed.add(learnerAttempt.definition().targets().getFirst().groupId());
        if(proposed.size()>Math.min(plan.maxLocalGroups(),settings.resources().committedApply().node().groups()))throw new IllegalArgumentException("Committed node group ceiling exhausted before native ownership");
    }
    private synchronized void assignPool(UUID group,UUID pool){requirePoolCapacity(pool,List.of(group));var previous=groupPools.putIfAbsent(group,pool);if(previous!=null && !previous.equals(pool))throw new IllegalArgumentException("Recovered group conflicts with declared native pool");}
    private RatisMetadataClaims.Claim metadataClaim(RatisNativePoolSpec spec,UUID group,RatisNodePlan.Role role){var value=spec.retention();var node=settings.retention();var effective=new RatisRangeStateMachine.RetentionLimits(Math.min(value.dataCount(),node.dataCount()),Math.min(value.dataBytes(),node.dataBytes()),Math.min(value.maintenanceCount(),node.maintenanceCount()),Math.min(value.maintenanceBytes(),node.maintenanceBytes()));return RatisMetadataClaims.claim(spec.id(),group,role,effective,spec.replicas());}
    private void reserveDataMetadata(PoolOwner owner,Collection<UUID> groups)throws IOException{var claims=new ArrayList<RatisMetadataClaims.Claim>();for(var group:groups)claims.add(metadataClaim(owner.spec,group,RatisNodePlan.Role.DATA));metadataClaims.reserve(claims);}
    private PoolOwner seedOwner(UUID intent){PoolOwner found=null;for(var owner:pools.values())if(owner.pool.topologySeed(intent)!=null){if(found!=null)throw new IllegalArgumentException("Seed is duplicated across pools");found=owner;}if(found==null)throw new IllegalArgumentException("Unknown forced seed pool");return found;}
    private PoolOwner learnerOwner(NativeTopologyLearnerRegistration registration){PoolOwner found=null;for(var owner:pools.values())if(owner.pool.topologyLearners().stream().anyMatch(registration::equals)){if(found!=null)throw new IllegalArgumentException("Learner is duplicated across pools");found=owner;}if(found==null)throw new IllegalArgumentException("Unknown forced learner pool");return found;}
    private void requireIncomingOwnership(RangeDirectory directory,Collection<RangeDescriptor> incoming,Set<UUID> replaced){for(var proposed:incoming)for(var existing:groups.entries()){if(existing.role()!=RatisNodePlan.Role.DATA || existing.retired() || existing.descriptor().groupId().equals(proposed.groupId()) || replaced.contains(existing.descriptor().groupId()) || Collections.disjoint(directory.columns(),existing.directory().columns()))continue;var current=existing.descriptor();if((current.upperExclusive()==null || proposed.lowerInclusive()==null || Arrays.compareUnsigned(proposed.lowerInclusive(),current.upperExclusive())<0) && (proposed.upperExclusive()==null || current.lowerInclusive()==null || Arrays.compareUnsigned(current.lowerInclusive(),proposed.upperExclusive())<0))throw new IllegalArgumentException("Node-local ownership intervals overlap across native pools");}}
    private PoolOwner requireSeedPool(TopologyChange.Definition definition){PoolOwner found=null;for(var parent:definition.parents()){var actual=owner(parent.groupId());if(found!=null && found!=actual)throw new IllegalArgumentException("Split/merge parents cross native pools");found=actual;}if(found==null)throw new IllegalArgumentException("No seed parent pool");requireIncomingOwnership(definition.targetDirectory(),definition.targets(),definition.parents().stream().map(RangeDescriptor::groupId).collect(java.util.stream.Collectors.toSet()));requirePoolCapacity(found.spec.id(),definition.targets().stream().map(RangeDescriptor::groupId).toList());for(var target:definition.targets())if(plan.groupPools().containsKey(target.groupId()) && !found.spec.id().equals(plan.groupPools().get(target.groupId())))throw new IllegalArgumentException("Born child cannot cross native pools");return found;}
    private RatisLogResourceBudget logBudget(UUID group,UUID pool){return logGroups.computeIfAbsent(group,id->new RatisLogResourceBudget(id,logNode,java.util.Objects.requireNonNull(logPools.get(pool)),RatisLogResourceBudget.Limits.defaults(),(bound,protectedProgress)->{var lease=diskRunway.reserveRaftAppend(pool,bound,protectedProgress);return lease::actualFinished;}));}
    private List<RatisDiskRunwayService.Scope> diskScopes(){
        var scopes=new ArrayList<RatisDiskRunwayService.Scope>();try{for(var owner:pools.values())for(var path:owner.pool.storagePaths())scopes.add(new RatisDiskRunwayService.Scope(owner.spec.id(),null,path));}catch(IOException failed){throw new java.io.UncheckedIOException(failed);}
        for(var group:groups.raftGroups())scopes.add(new RatisDiskRunwayService.Scope(null,group.getGroupId().getUuid(),root.resolve("raft").resolve(group.getGroupId().getUuid().toString()).resolve("current")));
        return List.copyOf(scopes);
    }
    private List<RatisDiskRunwayRecovery.Target> recoveryTargets(){
        var targets=new ArrayList<RatisDiskRunwayRecovery.Target>();if(server==null || closeRequested || closed)return List.of();
        for(var entry:machines.entrySet()){
            var store=stores.get(entry.getKey().getUuid());var machine=entry.getValue();if(store==null || store.importPending() || store.topologyBirthPending() || store.ownership().state()!=it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.State.ACTIVE)continue;
            var applied=machine.getLastAppliedTermIndex();var image=machine.getLatestSnapshot();long snapshot=image==null ? -1:image.getIndex();
            if(applied.getIndex()<=snapshot || diskRunway.state()==RatisDiskRunway.State.CLEAR && applied.getIndex()-snapshot<128)continue;
            var owner=owner(entry.getKey().getUuid());targets.add(new RatisDiskRunwayRecovery.Target(owner.spec.id(),entry.getKey().getUuid(),owner.images().maxImageBytes(),()->{try{owner.pool.flushWholePool();}catch(Exception failed){throw new java.util.concurrent.CompletionException(failed);}},()->{
                var request=org.apache.ratis.protocol.SnapshotManagementRequest.newCreate(snapshotClient,RaftPeerId.valueOf(plan.peerId()),entry.getKey(),snapshotCalls.incrementAndGet(),settings.shutdownTimeout().toMillis());
                return server.snapshotManagementAsync(request).thenApply(reply->{if(!reply.isSuccess() || !reply.getRaftGroupId().equals(entry.getKey()) || !reply.getServerId().toString().equals(plan.peerId()))throw new java.util.concurrent.CompletionException(new IOException("Local recovery snapshot was not published by actual division"));return null;});
            }));
        }
        targets.sort(Comparator.comparing(target->target.group().toString()));return List.copyOf(targets);
    }
    public RatisDiskRunway.Stats diskRunwayStats(){return diskRunway.stats();}
    public RatisDiskRunway.AppendLease reserveRaftAppend(UUID pool,long upperBound,boolean protectedProgress)throws IOException{return diskRunway.reserveRaftAppend(pool,upperBound,protectedProgress);}
    private DistributedStoragePool.WorkloadPressure workloadPressure()throws IOException,org.rocksdb.RocksDBException{boolean pressured=false,disk=false;int reasons=0;long free=Long.MAX_VALUE,sampled=Long.MAX_VALUE;for(var owner:pools.values()){var value=owner.pool.currentWorkloadPressure(Math.max(owner.spec.pins().minimumFreeBytes(),settings.exports().io().minimumFreeBytes()));pressured|=value.pressured();disk|=value.diskPressure();reasons|=value.reasonMask();free=Math.min(free,value.freeBytes());sampled=Math.min(sampled,value.sampledAtNanos());}return new DistributedStoragePool.WorkloadPressure(pressured,reasons,disk,free,sampled);}
    private void initializePoolServices(PoolOwner owner)throws IOException{
        owner.cohorts=new RatisSnapshotCohortService(new RatisSnapshotCohortService.Host(){
            public UUID nodeId(){return plan.nodeId();}public UUID nodeIncarnation(){return plan.nodeIncarnation();}public UUID processIncarnation(){return snapshotProcessIncarnation;}public UUID poolId(){return owner.spec.id();}
            public DistributedStoragePool pool(){return owner.pool;}public PoolSnapshotAdmission.Limits poolLimits(){return owner.pins();}
            public boolean originalGone(RatisSnapshotCohortRpc.Reference reference)throws IOException{
                synchronized(RatisNodeRuntime.this){
                    if(!reference.nodeIncarnation().equals(plan.nodeIncarnation()) || reference.processIncarnation().equals(snapshotProcessIncarnation) || !started || closeRequested || closing || closed || lock==null || !lock.isValid() || lockChannel==null || !lockChannel.isOpen())return false;
                    // This incarnation has one established root. Its prior owner must drain native resources before surrendering this lock, or exit the process.
                    return identityExposed(SessionJournalFiles.read(root.resolve("node.identity"),NODE_IDENTITY_BYTES));
                }
            }
            public RatisSnapshotCohortService.Member member(UUID group,SnapshotFence fence,boolean ready)throws Exception{
                synchronized(RatisNodeRuntime.this){if(!started || closeRequested || closing || closed)throw new IOException("Cohort node owner unavailable");}
                var entry=groups.require(group);var machine=machines.get(RaftGroupId.valueOf(group));if(!owner.spec.id().equals(groupPools.get(group)) || entry.role()!=RatisNodePlan.Role.DATA || entry.retired() || machine==null || stores.get(group)==null)throw new IOException("Cohort has no actual local DATA owner");
                return machine.snapshotCohortMember(fence,ready,entry.descriptor(),ownReplica(entry.descriptor()));
            }
        },scheduler,RatisSnapshotCohortService.Limits.defaults(),System::nanoTime,resources);
        owner.schemas=new RatisSchemaReplicaService(owner.spec.nativePath(root),root.resolve("schema-replicas").resolve(owner.spec.id().toString()),owner.pool,scheduler,schemaReplicaConfig.policy(),owner.spec.replicas(),schemaAdmission,owner.spec.id());
        long imageCeiling=Math.min(Math.multiplyExact(8,owner.images().maxImageBytes()),settings.resources().maximumSourceImageDiskBytes()/plan.pools().size());if(imageCeiling<Math.multiplyExact(2,owner.images().maxImageBytes()))throw new IOException("Node source image disk budget cannot cover declared pool image");
        owner.artifacts=new RatisTopologySourceSnapshotArtifacts(Files.createDirectories(root.resolve("topology-source-artifacts").resolve(owner.spec.id().toString())),owner.images(),imageCeiling,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES,4);owner.anchors=new RatisTopologyRecoveryAnchors(owner.artifacts,owner.images());owner.sources=new RatisTopologySourceSnapshotService(sourceSnapshotHost(owner),new RatisTopologySourceSnapshotService.Limits(4,owner.images().maxImageBytes(),imageCeiling/2,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES),scheduler,(request,bytes)->{if(request.purpose()==RatisTopologySourceSnapshotRpc.Purpose.RECOVERY)return migration.reserveRecovery(owner.spec.id(),bytes);var lease=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.ORDINARY,bytes,1);return lease::close;});
    }
    private PoolSnapshotAdmission.Shared sharedSnapshots;private RatisSchemaReplicaAdmission schemaAdmission;
    private final Map<UUID,RangeDirectory> startupDirectories=new HashMap<>();
    private final Map<UUID,RatisSessionPropagation> dataPropagations=new HashMap<>(); private RatisSessionClockService service;
    private final List<RatisSessionPropagation> propagations=new ArrayList<>();
    private final Map<RatisNodePlan.Role,RatisRoutedClient> homeClients=new EnumMap<>(RatisNodePlan.Role.class);
    private RatisTransactionExpiryService transactionExpiry;
    private RatisPoolPressureService poolPressure;
    private RatisNodePhysicalApi physicalApi;
    private RatisNodeMaintenance localMaintenance;
    private RatisNodeMaintenance.Policy localMaintenancePolicy;
    private RecoverySettings recoverySettings;
    private RatisParticipantRecoveryRegistry participantRecoveries;
    private RatisTransactionRecoveryService participantRecoveryService;
    private RatisRetirementCoordinator retirementCoordinator;
    private RatisTransactionRecoveryService retirementService;
    private RatisNativeParticipantDiscovery nativeParticipantDiscovery;
    private RatisSnapshotHelpService snapshotHelp;
    private RatisProofQueryService proofQueries;
    private RatisProofQueryService sourceOutcomeQueries;
    private RatisProofQueryService topologyImageQueries,relocationOwnerQueries,importPreflightQueries;private RatisRelocationDataClient relocationClients;
    private RatisProofQueryService authorityQueries;
    private RatisProofQueryService importPublicationQueries;
    private RatisProofQueryService operationRerouteQueries;
    private final Map<UUID,RatisReferencedFiles> importFiles=new LinkedHashMap<>();
    private ThreadPoolExecutor importFileIo;
    private RatisExportSourceService exportSources;

    private final UUID snapshotProcessIncarnation=UUID.randomUUID();

    private RatisSchemaReplicaClient schemaFencingReplicas;
    private RatisRangeImportOwnerClient importOwners;
    private boolean importOwnersClosed;
    private boolean schemaFencingReplicasClosed;
    private FileChannel lockChannel; private FileLock lock;
    private boolean installing,started,starting,closeRequested,closing,closed,serverClosed,poolClosed;

    /** Fresh placement is an explicit trusted bootstrap assertion, never inferred from the directory. Borrowed clients/schedulers remain caller-owned. */
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,null,null);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,null,null);
    }
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,Collection<DistributedSchema> schemas) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,schemas,null);
    }
    /** Operator-qualified home readers are fixed on DATA state machines before the server factory may initialize them. */
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,schemas,snapshotHomes);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,Collection<DistributedSchema> schemas) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schemas,null);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schemas,snapshotHomes,null);
    }
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,schemas,snapshotHomes,recoveries);
    }
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,SchemaReplicaConfig replicas) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,schemas,snapshotHomes,recoveries,RatisRoutedClient::connect,replicas);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schemas,snapshotHomes,recoveries,RatisRoutedClient::connect);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,HomeClientFactory homeFactory) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schemas,snapshotHomes,recoveries,homeFactory,SchemaReplicaConfig.defaults());
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,HomeClientFactory homeFactory,SchemaReplicaConfig replicas) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schemas,snapshotHomes,recoveries,homeFactory,replicas,() -> new RatisSchemaReplicaClient(RatisSchemaReplicaClient.MAX_CALLS),null);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,HomeClientFactory homeFactory,SchemaReplicaConfig replicas,SchemaReplicaClientFactory replicaFactory,RestoreNativeInitialization restoreInitialization) throws IOException {
        if(factory==null)factory=DEFAULT_SERVER_FACTORY;
        if(restoreInitialization!=null && startup!=Startup.AUTHORIZED_INITIAL_PLACEMENT)throw new IOException("Restore staging requires exact authorized initialization");
        requireNativeCacheCapacity(plan,settings.resources().maximumNativeCacheBytes());
        if(factory==DEFAULT_SERVER_FACTORY)RatisStreamingTransport.server(plan,new RaftProperties());
        var node=new RatisNodeRuntime(plan,root,epochs,routed,scheduler,timer,time,settings,schemas,snapshotHomes,recoveries,replicas);
        try { node.initialize(config,startup,factory,homeFactory,replicaFactory,restoreInitialization); return node; }
        catch (IOException | RuntimeException | Error failure) {
            try { node.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    static long requireNativeCacheCapacity(RatisNodePlan plan,long maximum)throws IOException {
        long total=0;try{for(var spec:plan.pools().values())total=Math.addExact(total,spec.nativeCacheCapacity().totalBytes());}catch(ArithmeticException overflow){throw new IOException("Declared native cache/WBM capacity overflow",overflow);}if(total>maximum)throw new IOException("Node declared native cache/WBM ceiling exhausted");return total;
    }
    private RatisNodeRuntime(RatisNodePlan plan,Path root,RatisSessionEpochs epochs,Map<UUID,RatisRoutedClient> routed,
            RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,SchemaReplicaConfig replicas) {
        this.plan=Objects.requireNonNull(plan);groups=new RatisRuntimeGroups(plan); this.root=root.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs);
        this.routedClients=Map.copyOf(routed);seedAuthority=epochs.topologyAuthority()::read;joinAuthority=epochs.topologyJoinAuthority()::read; this.scheduler=Objects.requireNonNull(scheduler); this.timer=Objects.requireNonNull(timer);
        this.time=Objects.requireNonNull(time); this.settings=Objects.requireNonNull(settings);diskRunway=new RatisDiskRunway(settings.resources().diskRunway(),System::nanoTime);if(settings.resources().diskRunway().recoveryHeadroomBytes()<Math.addExact(2*settings.images().maxImageBytes(),65536))throw new IllegalArgumentException("Declared recovery headroom cannot cover bounded snapshot image");long minimumFree=settings.exports().io().minimumFreeBytes();for(var spec:plan.pools().values())minimumFree=Math.max(minimumFree,spec.pins().minimumFreeBytes());long logGrowth=Math.addExact(2*MAX_COMMITTED_ENTRY_BYTES,RatisRangeStateMachine.COMMITTED_SEGMENT_BYTES);if(settings.resources().diskRunway().emergencyFreeBytes()<Math.addExact(Math.addExact(minimumFree,settings.resources().diskRunway().recoveryHeadroomBytes()),logGrowth))throw new IllegalArgumentException("Disk emergency runway cannot cover declared free reserve, recovery headroom and legal log growth");var declared=new HashMap<UUID,RatisNodeResourceBudget.Limits>();plan.pools().forEach((id,spec)->declared.put(id,poolResources(spec.resources(),settings.resources().pool())));resources=new RatisNodeResourceBudget(settings.resources().node(),declared);migration=new RatisMigrationController(settings.resources().migration(),plan.pools().keySet(),System::nanoTime,diskRunway::state);
        var dispatchPools=new HashMap<UUID,RatisCommittedApplyDispatcher.Limits>();
        plan.pools().keySet().forEach(id->logPools.put(id,new RatisLogResourceBudget.Shared(RatisLogResourceBudget.Limits.defaults())));
        var policy=settings.resources().committedApply();
        plan.pools().forEach((id,spec)->{var value=policy.pool();dispatchPools.put(id,new RatisCommittedApplyDispatcher.Limits(value.read(),value.ordinary(),value.protectedLane(),Math.min(value.groups(),spec.maximumGroups())));});
        for(var value:List.of(policy.node(),policy.pool()))if(value.read().bytes()<RatisRangeStateMachine.committedReadWeight(MAX_COMMITTED_ENTRY_BYTES) || Math.min(value.ordinary().bytes(),value.protectedLane().bytes())<RatisRangeStateMachine.committedExecutionWeight(MAX_COMMITTED_ENTRY_BYTES))throw new IllegalArgumentException("Committed policy cannot admit legal native import/schema heads");
        if(plan.localGroups().size()>policy.node().groups())throw new IllegalArgumentException("Committed node group bound exceeded");
        var counts=new HashMap<UUID,Integer>();for(var group:plan.localGroups())counts.merge(plan.poolId(group.getGroupId().getUuid()),1,Integer::sum);
        for(var count:counts.entrySet())if(count.getValue()>Math.min(policy.pool().groups(),plan.pools().get(count.getKey()).maximumGroups()))throw new IllegalArgumentException("Committed pool group bound exceeded before native open");
        committedDispatcher=new RatisCommittedApplyDispatcher(policy.node(),dispatchPools); schemaReplicaConfig=Objects.requireNonNull(replicas); var qualified=new HashMap<UUID,DistributedSchema>();if(schemas!=null)for(var schema:schemas)if(qualified.putIfAbsent(schema.domainId(),schema)!=null)throw new IllegalArgumentException("Duplicate node schema domain");logicalSchemas=Map.copyOf(qualified); this.snapshotHomes=snapshotHomes; recoverySettings=recoveries;
        if (!plan.imports().isEmpty() && logicalSchemas.isEmpty()) throw new IllegalArgumentException("Planned native imports require their qualified logical schemas");
        if (recoveries!=null) {
            recoveries.homes().requireDatabaseBinding(plan.clusterId(),plan.databaseId());
            var home=plan.home(RatisNodePlan.Role.TRANSACTION_HOME); if (home!=null) recoveries.homes().requireDirectoryBinding(home.directory());
        }
        if (snapshotHomes!=null) {
            snapshotHomes.requireDatabaseBinding(plan.clusterId(),plan.databaseId());
            var home=plan.home(RatisNodePlan.Role.SNAPSHOT_HOME); if (home!=null) snapshotHomes.requireDirectoryBinding(home.directory());
        }
        var domains=plan.domains().stream().map(domain->domain.directory().domainId()).collect(java.util.stream.Collectors.toSet());
        if(!routedClients.keySet().equals(domains) || !logicalSchemas.isEmpty() && !logicalSchemas.keySet().equals(domains))throw new IllegalArgumentException("Node DATA domain coverage differs");
        var merges=new HashSet<UUID>();for(var schema:logicalSchemas.values()){schema.requireMatches(plan.directory(schema.domainId()));for(var column:schema.columns())if(column.operator()!=null)merges.add(column.columnId());}
        if(!plan.merges().equals(merges))throw new IllegalArgumentException("Node merge bindings differ from qualified schemas");
        if (groups.raftGroups().stream().anyMatch(group -> group.getGroupId().getUuid().equals(plan.catalogue().groupId()))
                && plan.domains().stream().mapToInt(domain->domain.directory().ranges().size()).sum()+plan.homes().stream().mapToInt(home -> home.directory().ranges().size()).sum()>settings.maxPropagationRanges()) throw new IllegalArgumentException("Node clock propagation scope exceeds configured range budget");
        var catalogue=plan.catalogue(); epochs.requireCatalogueBinding(plan.clusterId(),plan.databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
        for(var entry:routedClients.entrySet())if(!entry.getKey().equals(entry.getValue().directory().domainId()) || !Arrays.equals(entry.getValue().bootstrapDirectory().payloadDigest(),plan.directory(entry.getKey()).payloadDigest()))throw new IllegalArgumentException("Node routed bootstrap directory differs from placement plan");
    }
    private RatisRoutedClient data(UUID domain){var client=routedClients.get(domain);if(client==null)throw new IllegalArgumentException("DATA domain is not configured");return client;}
    private void freezeStartupDirectories(){startupDirectories.clear();routedClients.forEach((domain,client)->startupDirectories.put(domain,client.directory()));}
    private void requirePropagationCoverage(UUID domain,RangeDirectory target){
        if(plan.localGroups().stream().noneMatch(group->group.getGroupId().getUuid().equals(plan.catalogue().groupId())))return;
        long ranges=plan.homes().stream().mapToLong(home->home.directory().ranges().size()).sum();for(var entry:routedClients.entrySet())ranges+=entry.getKey().equals(domain)?target.ranges().size():entry.getValue().directory().ranges().size();
        if(ranges>settings.maxPropagationRanges())throw new IllegalArgumentException("Aggregate clock propagation capacity exhausted");
    }
    private void requireInitialPoolStaging(Path nativeRoot)throws IOException {
        if(!Files.exists(nativeRoot,LinkOption.NOFOLLOW_LINKS))return;
        if(!Files.isDirectory(nativeRoot,LinkOption.NOFOLLOW_LINKS))throw new IOException("Unidentified native root");
        var names=new HashSet<String>();plan.pools().values().forEach(spec->names.add(spec.id()+".operators"));
        try(var paths=Files.list(nativeRoot)){for(var path:paths.toList()){
            if(!names.contains(path.getFileName().toString()) || !Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Unidentified native pool data must not be adopted");
            int count=0;long bytes=0;try(var artifacts=Files.list(path)){for(var file:artifacts.toList()){
                var name=file.getFileName().toString();if(!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS) || !(name.equals("lock") || name.matches("[0-9a-f]{64}\\.(jar|operator)")) || ++count>129 || (bytes=Math.addExact(bytes,Files.size(file)))>136L*1024*1024)throw new IOException("Unidentified operator staging must not be adopted");
            }}
        }}
    }
    private static final List<String> NODE_DIRECTORIES=List.of("raft","sessions","recovery","schema-replicas","referenced-files","topology-exposure","topology-source-artifacts","retirement","topology-relocations");
    private static final List<String> NODE_FILES=List.of("node.identity","node.lock","metadata-claims","native-extents.journal","restore.owner");
    private record NativeExtentLayout(List<Path> directories,List<Path> roots) {}
    private NativeExtentLayout nativeExtentLayout()throws IOException {
        Path journal=root.resolve("native-extents.journal").toAbsolutePath().normalize();Path exports=root.resolve("exports").toAbsolutePath().normalize();
        var reserved=new ArrayList<Path>();for(var directory:NODE_DIRECTORIES)reserved.add(root.resolve(directory).toAbsolutePath().normalize());
        for(var name:NODE_FILES)for(var suffix:List.of("",".pending",".lock"))reserved.add(root.resolve(name+suffix).toAbsolutePath().normalize());
        for(var spec:plan.pools().values()){var nativePath=spec.nativePath(root);reserved.add(nativePath.resolveSibling(nativePath.getFileName()+".operators").toAbsolutePath().normalize());}
        var directories=new LinkedHashSet<Path>();var managed=new LinkedHashMap<UUID,List<Path>>();
        for(var spec:plan.pools().values()){
            var paths=new ArrayList<Path>();for(var value:RocksDBLoader.nativeExtentDirectories(spec.nativePath(root),spec.config())){var path=value.toAbsolutePath().normalize();requireNoSymlinks(path);directories.add(path);paths.add(path);}
            paths.sort(Comparator.comparingInt(Path::getNameCount).thenComparing(Path::toString));var roots=new ArrayList<Path>();for(var path:paths)if(roots.stream().noneMatch(path::startsWith))roots.add(path);managed.put(spec.id(),List.copyOf(roots));
        }
        var roots=new ArrayList<Path>();for(var entry:managed.entrySet())for(var path:entry.getValue()){
            for(var other:roots)if(path.startsWith(other)||other.startsWith(path))throw new IOException("Native roots overlap across physical pools");
            if(exports.startsWith(path)||path.startsWith(exports)||journal.startsWith(path)||path.startsWith(journal))throw new IOException("Native pool root overlaps export root or extent journal");
            for(var owned:reserved)if(path.startsWith(owned)||owned.startsWith(path))throw new IOException("Native pool root overlaps reserved node namespace: "+owned);
            roots.add(path);
        }
        requireNoSymlinks(exports);requireNoSymlinks(journal);directories.add(exports);roots.add(exports);
        if(roots.size()>128)throw new IOException("Native extent root count exceeds SDK bound");roots.sort(Comparator.comparing(Path::toString));return new NativeExtentLayout(List.copyOf(directories),List.copyOf(roots));
    }
    private void initializeNativeExtentEnv(Path identity,NativeExtentLayout layout)throws IOException {
        Path journal=root.resolve("native-extents.journal").toAbsolutePath().normalize();
        if(nativeExtentBound && !Files.isRegularFile(journal,LinkOption.NOFOLLOW_LINKS))throw new IOException("Original native extent journal is missing");
        for(var path:layout.directories()){if(nativeExtentBound && !Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Established native extent directory is missing: "+path);createAndForceNativeDirectory(path);}
        SessionJournalFiles.force(root);
        try(var memoryOwner=new org.rocksdb.NativeRawFlushLimits(settings.resources().nativeExtents().raw())){nativeExtentEnv=new org.rocksdb.NativeExtentEnv(journal.toString(),layout.roots().stream().map(Path::toString).toList(),settings.resources().nativeExtents().limits(),memoryOwner);}
        catch(org.rocksdb.RocksDBException failure){throw new IOException("Cannot bind original shared native extent environment",failure);}
        if(!nativeExtentBound){nativeExtentBound=true;SessionJournalFiles.write(identity,identity(false),NODE_IDENTITY_BYTES);}
    }
    private static void createAndForceNativeDirectory(Path path)throws IOException {
        var missing=new ArrayList<Path>();for(Path current=path;current!=null && !Files.exists(current,LinkOption.NOFOLLOW_LINKS);current=current.getParent())missing.add(current);
        Files.createDirectories(path);requireNoSymlinks(path);SessionJournalFiles.force(path);
        for(var created:missing){SessionJournalFiles.force(created);Path parent=created.getParent();if(parent!=null)SessionJournalFiles.force(parent);}
        if(missing.isEmpty() && path.getParent()!=null)SessionJournalFiles.force(path.getParent());
    }
    private static void requireNoSymlinks(Path path)throws IOException{for(Path current=path;current!=null;current=current.getParent())if(Files.isSymbolicLink(current))throw new IOException("Native extent path contains a symlink: "+current);}
    private void initialize(DatabaseConfig config,Startup startup,ServerFactory factory,HomeClientFactory homeFactory,SchemaReplicaClientFactory replicaFactory,RestoreNativeInitialization restoreInitialization) throws IOException {
        Objects.requireNonNull(startup); Objects.requireNonNull(factory);
        if (!Files.isDirectory(root)) throw new IOException("Node root must already exist on durable storage");
        for (var home:plan.homes()) {
            var client=Objects.requireNonNull(homeFactory.connect(home.directory())); homeClients.put(home.role(),client);
            if (!Arrays.equals(client.directory().payloadDigest(),home.directory().payloadDigest())) throw new IllegalArgumentException("HOME client differs from declared placement");
        }
        var transactionPlan=plan.home(RatisNodePlan.Role.TRANSACTION_HOME);
        if (recoverySettings==null && transactionPlan!=null) recoverySettings=new RecoverySettings(new RatisTransactionStatuses(homeClients.get(transactionPlan.role()),transactionPlan.column()),false,RatisParticipantRecoveryRegistry.Limits.defaults(),Duration.ofSeconds(1),4);
        transactionHomes=recoverySettings==null ? null : recoverySettings.homes();
        lockChannel=FileChannel.open(root.resolve("node.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        try { lock=lockChannel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Node root already owned",owned); }
        if (lock==null) throw new IOException("Node root already owned");
        var extentLayout=nativeExtentLayout();
        Path identity=root.resolve("node.identity"), logs=root.resolve("raft"), nativePath=root.resolve("native");
        boolean establishedPlacement=false;
        if (Files.exists(identity,LinkOption.NOFOLLOW_LINKS)) {
            int phase=identityPhase(SessionJournalFiles.read(identity,NODE_IDENTITY_BYTES));nativeExtentBound=phase>=1;boolean exposed=phase==2;establishedPlacement=exposed;
            if (!exposed && startup!=Startup.AUTHORIZED_INITIAL_PLACEMENT) throw new IOException("Interrupted initial placement requires explicit authorization to resume");
            if (exposed) {
                for(var spec:plan.pools().values())if(!Files.isRegularFile(spec.nativePath(root).resolve("CURRENT")))throw new IOException("Established native pool state is missing; refuse empty-voter recovery: "+spec.id());
                for (var group:groups.raftGroups()) if (!Files.isRegularFile(logs.resolve(group.getGroupId().getUuid().toString()).resolve("current/raft-meta"))) {
                    throw new IOException("Established Raft state is missing for "+group.getGroupId());
                }
            }
        } else {
            if (startup!=Startup.AUTHORIZED_INITIAL_PLACEMENT) throw new IOException("Node identity is absent; explicit placement initialization is required");
            if(Files.exists(root.resolve("exports"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Unidentified source export authority must not be adopted");
            for(var name:NODE_FILES)for(var suffix:List.of("",".pending",".lock")){if(name.equals("node.lock") && suffix.isEmpty())continue;if(Files.exists(root.resolve(name+suffix),LinkOption.NOFOLLOW_LINKS))throw new IOException("Unidentified node authority must not be adopted: "+name+suffix);}
            if(Files.exists(root.resolve("metadata-claims"),LinkOption.NOFOLLOW_LINKS) || Files.exists(root.resolve("metadata-claims.pending"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Unidentified metadata claims must not be adopted");
            for (String directory:java.util.stream.Stream.concat(java.util.stream.Stream.of("native"),NODE_DIRECTORIES.stream()).toList()) {
                Path path=root.resolve(directory);
                if(directory.equals("native")){requireInitialPoolStaging(path);continue;}
                if (Files.exists(path)) try (var entries=Files.list(path)) { if (entries.findAny().isPresent()) throw new IOException("Unidentified node data must not be adopted"); }
            }
            SessionJournalFiles.write(identity,identity(false),NODE_IDENTITY_BYTES);
        }
        for (String directory:List.of("native","raft","sessions","recovery","schema-replicas")) { Path path=Files.createDirectories(root.resolve(directory)); SessionJournalFiles.force(path); }
        SessionJournalFiles.force(root);
        // The exact original node identity and fresh staging tree have been checked while node.lock is held.
        if(restoreInitialization!=null)restoreInitialization.prepare(resources);
        initializeNativeExtentEnv(identity,extentLayout);
        captureHomes=snapshotHomes;
        var snapshotPlan=plan.home(RatisNodePlan.Role.SNAPSHOT_HOME);
        if (snapshotPlan!=null) {
            if (captureHomes==null) captureHomes=new RatisSnapshotFences(homeClients.get(snapshotPlan.role()),snapshotPlan.column());
        }
        RatisProofQueryService.ResourceAdmission proofAdmission=(group,bytes)-> {
            var pool=groupPools.get(group);
            if(pool==null)throw new IOException("Proof query has no declared native pool");
            return resources.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,bytes,1)::close;
        };
        proofQueries=new RatisProofQueryService(proofAdmission);importPreflightQueries=RatisProofQueryService.importPreflightLane(proofAdmission);sourceOutcomeQueries=RatisProofQueryService.sourceOutcomeLane(proofAdmission);topologyImageQueries=RatisProofQueryService.topologyImageLane(proofAdmission);relocationOwnerQueries=RatisProofQueryService.relocationOwnerLane(proofAdmission);relocationClients=new RatisRelocationDataClient(plan.clusterId(),plan.databaseId(),plan.catalogue().groupId());
        if(groups.raftGroups().stream().anyMatch(group->groups.require(group.getGroupId().getUuid()).role()==RatisNodePlan.Role.TRANSACTION_HOME))
            operationRerouteQueries=RatisProofQueryService.operationRerouteLane(proofAdmission);
        if (groups.raftGroups().stream().anyMatch(group -> groups.require(group.getGroupId().getUuid()).role()==RatisNodePlan.Role.CATALOGUE)) {
            authorityQueries=RatisProofQueryService.authorityLane(proofAdmission);
            importPublicationQueries=RatisProofQueryService.importPublicationLane(proofAdmission);
        }
        if (transactionHomes!=null) snapshotHelp=new RatisSnapshotHelpService(scheduler);
        var metadataBindings=new HashMap<UUID,String>();
        for (var group:groups.raftGroups()) switch (groups.require(group.getGroupId().getUuid()).role()) {
            case DATA -> {}
            case CATALOGUE -> {
                metadataBindings.put(plan.catalogue().directoryColumn(),"catalogue-directory");
                metadataBindings.put(plan.catalogue().sessionsColumn(),"catalogue-sessions");
            }
            case TRANSACTION_HOME, SNAPSHOT_HOME -> {
                var home=plan.home(group.getGroupId().getUuid()); metadataBindings.put(home.column(),home.physicalName());
            }
        }
        sharedSnapshots=new PoolSnapshotAdmission.Shared(settings.exports().pins(),System::nanoTime);schemaAdmission=new RatisSchemaReplicaAdmission(schemaReplicaConfig.limits(),resources);for(var group:groups.raftGroups())assignPool(group.getGroupId().getUuid(),plan.poolId(group.getGroupId().getUuid()));
        // Caches have no reclamation protocol: reserve every declared pool's entire bounded envelope.
        if(Math.multiplyExact((long)plan.pools().size(),DistributedOperatorArtifacts.MAX_CACHE_BYTES)>settings.resources().maximumOperatorCacheBytes())
            throw new IOException("Node immutable operator cache ceiling exhausted");
        for(var spec:plan.pools().values())DistributedOperatorArtifacts.inventory(spec.nativePath(root));
        var metadataCeilings=new HashMap<UUID,Long>();plan.pools().forEach((id,spec)->metadataCeilings.put(id,spec.maximumDurableMetadataBytes()));metadataClaims=new RatisMetadataClaims(root.resolve("metadata-claims"),settings.resources().maximumDurableMetadataBytes(),metadataCeilings);
        var initialClaims=new ArrayList<RatisMetadataClaims.Claim>();for(var group:groups.raftGroups()){var id=group.getGroupId().getUuid();initialClaims.add(metadataClaim(plan.pools().get(groupPools.get(id)),id,groups.require(id).role()));}metadataClaims.reserve(initialClaims);
        for(var spec:plan.pools().values()){
            var bindings=new HashMap<UUID,String>();var schemas=new ArrayList<DistributedSchema>();for(var domain:plan.domains())if(domain.directory().ranges().stream().anyMatch(range->spec.id().equals(plan.poolId(range.groupId())) && plan.localGroups().stream().anyMatch(group->group.getGroupId().getUuid().equals(range.groupId())))){bindings.putAll(domain.columns());var schema=logicalSchemas.get(domain.domainId());if(schema!=null)schemas.add(schema);}var metadata=new HashMap<UUID,String>();for(var group:groups.raftGroups())if(spec.id().equals(groupPools.get(group.getGroupId().getUuid())))switch(groups.require(group.getGroupId().getUuid()).role()){case DATA->{}case CATALOGUE->{metadata.put(plan.catalogue().directoryColumn(),"catalogue-directory");metadata.put(plan.catalogue().sessionsColumn(),"catalogue-sessions");}case TRANSACTION_HOME,SNAPSHOT_HOME->{var home=plan.home(group.getGroupId().getUuid());metadata.put(home.column(),home.physicalName());}}
            var nativePool=DistributedStoragePool.open(spec.nativePath(root),spec.config(),LoggerFactory.getLogger(RatisNodeRuntime.class),schemas.isEmpty()?null:schemas,schemas.isEmpty()?null:bindings,schemas.isEmpty()?Map.of():metadata,nativeExtentEnv);var owner=new PoolOwner(spec,nativePool);pools.put(spec.id(),owner);nativePool.configureSnapshotAdmission(owner.pins(),sharedSnapshots);initializePoolServices(owner);
        }
        // Discover every pool before creating any Raft group; a group cannot silently move between databases.
        var discovered=new ArrayList<NativeTopologySeed>();for(var owner:pools.values()){for(var seed:owner.pool.topologySeeds()){for(var parent:seed.definition().parents())assignPool(parent.groupId(),owner.spec.id());for(var target:seed.definition().targets())assignPool(target.groupId(),owner.spec.id());discovered.add(seed);}for(var learner:owner.pool.topologyLearners()){assignPool(learner.definition().targets().getFirst().groupId(),owner.spec.id());}}
        discovered.sort(Comparator.comparingLong(seed->seed.definition().sourceDirectory().generation()));for(var seed:discovered)groups.register(seed);for(var owner:pools.values())for(var learner:owner.pool.topologyLearners())groups.register(learner);
        try{
            var exposed=new HashMap<UUID,RatisRuntimeGroups.Entry>();for(var entry:groups.entries())exposed.put(entry.group().getGroupId().getUuid(),entry);
            var retainedClaims=new ArrayList<RatisMetadataClaims.Claim>();var unqualified=new ArrayList<UUID>();
            for(var owner:pools.values()){
                var historical=new HashSet<UUID>();for(var retained:owner.pool.retainedMetadataOwners()){
                    var id=retained.owner().groupId();historical.add(id);var entry=exposed.get(id);RatisNodePlan.Role role;
                    if(entry!=null){if(!owner.spec.id().equals(groupPools.get(id)))throw new IOException("Misplaced durable native group: "+id);role=entry.role();}
                    else{role=RatisNodePlan.Role.DATA;if(!plan.domainPools().containsKey(retained.interval().domainId()) || !retained.retired()&&!retained.pending())unqualified.add(id);}
                    retainedClaims.add(metadataClaim(owner.spec,id,role));
                }
                for(var id:owner.pool.persistedRangeGroups(RatisNodePlan.MAX_LOCAL_GROUPS))if(!historical.contains(id))throw new IOException("Native metadata has no permanent ownership history: "+id);
            }
            metadataClaims.reserve(retainedClaims);if(!unqualified.isEmpty())throw new IOException("Unqualified unexposed native owners: "+unqualified);
        }catch(org.rocksdb.RocksDBException failure){throw new IOException("Cannot reconstruct retained metadata claims",failure);}
        loadRelocationIntents();
        var allowed=new HashSet<UUID>(); for (var group:groups.raftGroups()) allowed.add(group.getGroupId().getUuid());
        try (var entries=Files.list(logs)) {
            for (var path:entries.toList()) if (Files.isDirectory(path)) {
                UUID group; try { group=UUID.fromString(path.getFileName().toString()); } catch (IllegalArgumentException other) { continue; }
                if (!path.getFileName().toString().equals(group.toString())) throw new IOException("Noncanonical persisted Raft group path");
                if (!allowed.contains(group)) throw new IOException("Unplanned persisted Raft group "+group);
            }
        }

        if (groups.raftGroups().stream().anyMatch(group -> groups.require(group.getGroupId().getUuid()).role()==RatisNodePlan.Role.CATALOGUE)) {
            schemaFencingReplicas=Objects.requireNonNull(replicaFactory.create());
            topologyOwners=new RatisTopologyOwnerClient(plan.clusterId(),plan.databaseId(),plan.catalogue().groupId());
            importOwners=new RatisRangeImportOwnerClient(plan.clusterId(),plan.databaseId(),plan.catalogue().groupId());
        }
        if (plan.localGroups().stream().anyMatch(group -> plan.importDefinition(group.getGroupId().getUuid())!=null))
            importFileIo=new ThreadPoolExecutor(2,2,0L,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(32),
                    Thread.ofPlatform().daemon(true).name("ratis-import-file-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
        try {
            for (var group:groups.raftGroups()) {
                var id=group.getGroupId().getUuid();var owner=owner(id);var pool=owner.pool; RatisRangeStateMachine machine;
                var role=groups.require(id).role();
                if (role==RatisNodePlan.Role.CATALOGUE) {
                    var c=plan.catalogue();
                    var store=pool.openRange(new RangeOwnership.Owner(id,c.epoch()),new RangeOwnership.Interval(id,Set.of(c.directoryColumn(),c.sessionsColumn()),null,null),
                            c.schemaDigest(),Map.of(c.directoryColumn(),"catalogue-directory",c.sessionsColumn(),"catalogue-sessions"),Set.of());
                    stores.put(id,store);
                    var operatorLimits=new RatisOperatorCatalogue.Limits(owner.spec.replicas().maximumUploads(),owner.spec.replicas().maximumDiskBytes());
                    var operators=RatisOperatorCatalogue.validator(plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),operatorLimits,this::requireOperatorAdmission,key -> committedCatalogueValue(store,c.directoryColumn(),key));
                    var catalogueValidator=RatisCatalogue.validator(plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),c.sessionsColumn(),operators,key -> committedCatalogueValue(store,c.directoryColumn(),key));store.enableCatalogueScopeCompaction();store.configureCatalogueHistory(c.directoryColumn(),c.sessionsColumn(),RatisSchemaCatalogue::isHistoryKey,RatisSchemaCatalogue.historyPolicy(c.directoryColumn(),catalogueValidator,key->committedCatalogueValue(store,c.directoryColumn(),key)));
                    machine=new RatisRangeStateMachine(store,group,owner.images(),catalogueValidator,
                            command -> {throw new IllegalStateException("Session epoch floors require catalogue lifetime authority");},owner.retention());
                    machine.role(role);
                    machine.schemaCatalogue(plan.clusterId(),plan.databaseId(),c.directoryColumn());
                    machine.schemaFencingReplicaClient(schemaFencingReplicas);
                    machine.schemaAuthorityQueries(authorityQueries);
                    machine.importPublicationQueries(importPublicationQueries);
                    RatisCdcDeliveryAuthority.DirectoryReader cdcDirectories=domain->{
                        byte[] key=java.nio.ByteBuffer.allocate(16).putLong(domain.getMostSignificantBits()).putLong(domain.getLeastSignificantBits()).array();byte[] value=committedCatalogueValue(store,c.directoryColumn(),key);return value==null ? null : RangeDirectoryCodec.decode(value);
                    };
                    RatisCdcDeliveryAuthority.DefinitionReader cdcDefinitions=header->RatisCdcLineageDirectory.readDefinition(store,c.directoryColumn(),header);
                    var cdcLineages=new RatisCdcLineageDirectory(cdcDirectories,()->RatisCdcLineageDirectory.readStates(store,c.directoryColumn()),cdcDefinitions);
                    RatisTopologyCatalogue.OwnerProofReader cdcOwners=(definition,expected)->{var range=(expected.stage()==TopologyChange.Stage.FREEZE ? definition.parents() : definition.targets()).stream().filter(candidate->candidate.groupId().equals(expected.groupId())).findFirst().orElseThrow();var replica=range.replicas().stream().filter(candidate->candidate.replicaId().equals(expected.owner().replicaId())).findFirst().orElseThrow();var evidence=topologyOwners.read(definition,expected.stage(),range,replica);return new RatisTopologyCatalogue.QualifiedOwner(evidence.proof(),evidence.completedCut());};
                    var cdcAuthority=new RatisCdcDeliveryAuthority(routedClients,cdcDirectories,cdcLineages::read,cdcOwners,cdcDefinitions);
                    machine.cdcDeliveryAdmission(cdcAuthority::validate);
                    machine.topologyExecutionAdmission(command->{if(RatisTopologyCatalogue.relocationExecution(command)){RatisTopologyCatalogue.admitRelocation(command,plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),key->committedCatalogueValue(store,c.directoryColumn(),key),this::qualifyRelocationPublication);return;}RatisTopologyCatalogue.admitExecution(command,plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),key->committedCatalogueValue(store,c.directoryColumn(),key),(definition,expected)->{
                        var range=(expected.stage()==TopologyChange.Stage.FREEZE ? definition.parents() : definition.targets()).stream().filter(candidate->candidate.groupId().equals(expected.groupId())).findFirst().orElseThrow();var replica=range.replicas().stream().filter(candidate->candidate.replicaId().equals(expected.owner().replicaId())).findFirst().orElseThrow();var evidence=topologyOwners.read(definition,expected.stage(),range,replica);if(expected.stage()==TopologyChange.Stage.ACTIVATE){var activation=NativeTopologyActivation.decode(evidence.artifact());activation.requireCut(evidence.completedCut());if(!Arrays.equals(activation.proof().encode(),evidence.proof().encode()))throw new IOException("Clock witness differs from genuine owner activation proof");qualifyTopologyActivation(definition,activation);}return new RatisTopologyCatalogue.QualifiedOwner(evidence.proof(),evidence.completedCut());
                    });});
                    machine.importPublicationAdmission(command->{
                        if(RatisRangeImportCatalogue.isPublication(command))RatisRangeImportCatalogue.admitPublication(command,plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),key->committedCatalogueValue(store,c.directoryColumn(),key),importOwners::receipt);
                        else RatisRangeImportCatalogue.admitCompletion(command,plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),key->committedCatalogueValue(store,c.directoryColumn(),key),importOwners::activation);
                    });
                } else {
                    var registration=groups.require(id);var directory=registration.directory();var descriptor=registration.descriptor();var home=registration.seed()==null && registration.learner()==null ? plan.home(id) : null;
                    var recipe=registration.retired() ? groups.retirement(id) : registration.seed();
                    var bindings=registration.learner()!=null ? registration.learner().physicalNames() : recipe!=null ? recipe.physicalNames() : home==null ? plan.domain(directory.domainId()).columns() : Map.of(home.column(),home.physicalName());
                    var importDefinition=registration.seed()==null && registration.learner()==null ? plan.importDefinition(id) : null;
                    if(importDefinition!=null){var manifest=importDefinition.manifest(id);RatisRangeImportRecord.requirePolicy(manifest);if(manifest.columns().size()>owner.images().maxColumns() || (long)manifest.encode().length+17>owner.images().maxRowBytes() || (long)manifest.encode().length+17>owner.images().maxImageBytes())throw new IOException("Imported manifest exceeds configured native image column or byte budget");}
                    var store=registration.learner()!=null ? pool.openTopologyLearner(registration.learner()) : registration.retired() ? pool.openRetiredTopologyRange(id) : registration.seed()!=null ? pool.reopenTopologyRange(registration.seed().birth(id),seedSchema(registration.seed())) : importDefinition==null ? pool.openRange(new RangeOwnership.Owner(id,descriptor.rangeEpoch()),new RangeOwnership.Interval(directory.domainId(),directory.columns(),descriptor.lowerInclusive(),descriptor.upperExclusive()),
                            directory.schemaDigest(),bindings,recipe!=null ? recipe.mergeColumns() : home==null ? plan.domain(directory.domainId()).merges() : Set.of())
                            : startup==Startup.RECOVER || establishedPlacement ? pool.reopenImportRange(importDefinition.manifest(id),logicalSchemas.get(directory.domainId()))
                            : pool.openNewFencedRange(importDefinition.manifest(id),logicalSchemas.get(directory.domainId()));
                    if(registration.relocation()!=null){var configuration=store.configuration();var cut=store.applied();if(configuration==null || cut==null || configuration.term()>cut.term() || configuration.index()>cut.index() || configuration.index()==cut.index() && configuration.term()!=cut.term())throw new IOException("Relocation cold configuration lacks completed native prefix");group=groups.reconcileRelocationConfiguration(id,configuration.data());}
                    stores.put(id,store);
                    java.util.function.Consumer<it.cavallium.rockserver.core.common.distributed.DistributedCommand> validator=command -> {};
                    if (role==RatisNodePlan.Role.TRANSACTION_HOME) validator=RatisTransactionStatuses.validator(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),directory.schemaDigest(),home.column());
                    else if (role==RatisNodePlan.Role.SNAPSHOT_HOME) validator=RatisSnapshotFences.validator(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),directory.schemaDigest(),home.column());
                    if (role==RatisNodePlan.Role.DATA && registration.learner()==null && !registration.retired() && (logicalSchemas.containsKey(directory.domainId()) || registration.seed()!=null)) {
                        var effectiveSchema=store.installedSchema();
                        store.enableLogicalSchema(effectiveSchema==null ? logicalSchemas.get(directory.domainId()) : effectiveSchema);
                    }
                    var effectiveDirectory=role==RatisNodePlan.Role.DATA && store.schemaInstallation()!=null
                            ? store.schemaInstallation().definition().targetDirectory()
                            : importDefinition==null ? directory : importDefinition.targetDirectory();
                    if(role==RatisNodePlan.Role.DATA && registration.learner()!=null && !Arrays.equals(effectiveDirectory.schemaDigest(),store.schemaDigest())){
                        var permission=store.topologyLearnerImagePermission();if(permission!=null && Arrays.equals(permission.schema().digest(),store.schemaDigest())){permission.requireRegistration(registration.learner());effectiveDirectory=permission.definition().sourceDirectory();}
                    }
                    // Bind future session controls to the frozen published placement; the native import fence
                    // still rejects DATA/control admission until the exact catalogue activation is applied.
                    machine=RatisRangeStateMachine.withSessions(store,group,owner.images(),validator,epochs,effectiveDirectory,owner.retention());
                    if (role==RatisNodePlan.Role.TRANSACTION_HOME) {
                        machine.role(role,(key,value) -> RatisTransactionStatuses.validateRecord(directory.clusterId(),directory.databaseId(),key,value));
                        machine.homeRecoveryProofs(recovery->data(recovery.before().domainId()).finalizedParticipant(recovery.proof().participant(),recovery.before().transactionId()));machine.homeRecoveryFences(participantIdentity->data(participantIdentity.definition().domainId()).participantFence(participantIdentity)); machine.homeRecoveryReservations(directory.clusterId(),directory.databaseId(),home.column());
                        machine.operationRerouteQueries(operationRerouteQueries);
                        var rerouteAuthority=new RatisOperationRerouteAdmission(new RatisOperationRerouteAdmission.Authority(){
                            public it.cavallium.rockserver.core.common.distributed.TopologyChange.State intent(UUID intent)throws IOException{return epochs.topologyCatalogue(plan.catalogue().directoryColumn()).readIntent(intent);}
                            public it.cavallium.rockserver.core.common.distributed.TopologyChange.Definition definition(byte[] digest)throws IOException{return epochs.topologyCatalogue(plan.catalogue().directoryColumn()).readDefinition(digest);}
                            public RangeDirectory directory(UUID domain)throws IOException{return epochs.catalogue(plan.catalogue().directoryColumn()).read(domain);}
                            public RatisTopologyOperationOutcomeRpc.Reply outcome(RangeDirectory source,RangeDescriptor descriptor,it.cavallium.rockserver.core.common.distributed.DistributedCommand original)throws IOException{var client=data(source.domainId());return client.operationOutcome(client.restoreSessionCommand(source,original,descriptor));}
                        },plan.catalogue().groupId());
                        machine.operationRerouteProofAdmission(rerouteAuthority::validate);
                    }
                    else if (role==RatisNodePlan.Role.SNAPSHOT_HOME) {
                        machine.role(role,(key,value) -> RatisSnapshotFences.validateRecord(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),key,value));
                        machine.snapshotHomeStorage(directory.clusterId(),directory.databaseId(),home.column()); machine.snapshotHomeProofs((fence,participantGroup)->data(fence.domainId()).snapshotFence(fence,participantGroup));
                    }
                    else { configureDataMachine(machine,effectiveDirectory,descriptor); }
                    if (importDefinition!=null) {
                        var limits=new RatisReferencedFiles.Limits(4096,4096,8,32,
                                it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_FILE_BYTES,
                                RatisRangeImportRecord.DISTRIBUTED_LIMITS.maximumTotalBytes()+RatisRangeImportRecord.DISTRIBUTED_LIMITS.maximumEncodedBytes());
                        var files=new RatisReferencedFiles(root.resolve("referenced-files").resolve(id.toString()),id,limits,importFileIo,
                                request->RatisRangeImportRpc.requireFile(importDefinition.manifest(id),request));
                        importFiles.put(id,files); machine.referencedFiles(files);
                        machine.importPreflightQueries(importPreflightQueries);machine.rangeImportApply(new RatisRangeImportApply(pool,store,files,importDefinition),epochs.importAuthority(),ownReplica(descriptor));
                    }
                }
                machine.learnerInstaller(topologyImageQueries,this::joinTopologyLearner);machine.learnerRecoveryInstaller(this::prepareLearnerRecovery);machine.proofQueries(proofQueries);
                machine.nodeResources(resources,owner.spec.id());machine.logResources(logBudget(group.getGroupId().getUuid(),owner.spec.id()));machine.diskRunway(diskRunway,owner.spec.id());machine.committedApply(committedDispatcher,owner.spec.id(),MAX_COMMITTED_ENTRY_BYTES);machines.put(group.getGroupId(),machine);
            }
            for (var entry:machines.entrySet()) if (groups.require(entry.getKey().getUuid()).role()==RatisNodePlan.Role.DATA)
                for (var schema:entry.getValue().schemaRoutingHistory()) observeSchemaActivation(schema.definition(),schema.proof());
            freezeStartupDirectories();
        } catch (org.rocksdb.RocksDBException failure) { throw new IOException("Native node groups could not open",failure); }
        exportSources=new RatisExportSourceService(root.resolve("exports"),settings.exports().sources(),settings.exports().io(),
                new RatisExportSourceRegistry.NativeBuilder() {
                    public it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport.Chunk build(
                            RatisExportSourceRpc.JobDefinition definition,RatisExportJournal.ChunkIntent intent,
                            Path artifact,RatisExportSourceRegistry.Completion completion) throws Exception {
                        var machine=machines.get(RaftGroupId.valueOf(definition.group()));
                        if (machine==null) throw new RatisSnapshotLostException("Original export group is absent");
                        machine.requireExportPin(definition);
                        return machine.exportSnapshotPinChunk(definition.capture(),pin -> {
                            var chunk=owner(definition.group()).pool.exportSstChunk(artifact,pin,intent.column(),intent.scopeStart(),
                                    definition.scope(intent.column()).upperExclusive(),intent.limits());
                            completion.persist(chunk); return chunk;
                        });
                    }
                    public boolean tokenUsable(RatisExportSourceRpc.JobDefinition definition) throws Exception {
                        var machine=machines.get(RaftGroupId.valueOf(definition.group()));
                        if (machine==null) return false;
                        try { machine.requireExportPin(definition); return true; }
                        catch (RatisSnapshotLostException lost) { return false; }
                    }
                },scheduler);
        for (var entry:machines.entrySet()) if (groups.require(entry.getKey().getUuid()).role()==RatisNodePlan.Role.DATA)
            entry.getValue().exportSources(exportSources);
        var retirementSources=new ArrayList<RatisRetirementCoordinator.Source>();
        for (var id:machines.keySet().stream().sorted(java.util.Comparator.comparing(RaftGroupId::toString)).toList()) {
            var machine=machines.get(id); var role=groups.require(id.getUuid()).role();
            if (role==RatisNodePlan.Role.TRANSACTION_HOME) {
                retirementSources.add(new RatisRetirementCoordinator.Source(id.getUuid(),RatisRetirementCoordinator.Mode.TRANSACTION,(after,scans) -> machine.retirementCandidates(false,after,scans)));
                retirementSources.add(new RatisRetirementCoordinator.Source(id.getUuid(),RatisRetirementCoordinator.Mode.ORPHAN,(after,scans) -> machine.retirementCandidates(true,after,scans)));
            } else if (role==RatisNodePlan.Role.SNAPSHOT_HOME) retirementSources.add(new RatisRetirementCoordinator.Source(id.getUuid(),RatisRetirementCoordinator.Mode.SNAPSHOT,(after,scans) -> machine.retirementCandidates(false,after,scans)));
        }
        if (!retirementSources.isEmpty()) {
            retirementCoordinator=new RatisRetirementCoordinator(root.resolve("retirement"),transactionHomes,captureHomes,routedClients,retirementSources,RatisRetirementJournal.Limits.defaults());
            retirementService=new RatisTransactionRecoveryService(scheduler,() -> retirementCoordinator.cycle(4));
        }
        validateSeedExposures();
        for(var entry:groups.entries())requireSeedStorage(entry);
        for(var entry:groups.entries())markSeedExposure(entry);
        diskCensus=new RatisDiskRunwayService(diskRunway,this::diskScopes,scheduler);diskCensus.initialCensus();
        server=factory.create(plan,logs,Collections.unmodifiableMap(machines));
        logRegistration=org.apache.ratis.server.impl.LogResourceStateMachine.bindServer(server,logGroups.values().iterator().next());
        transactionExpiry=new RatisTransactionExpiryService(scheduler,this::expiryMachines,32);
        diskRecovery=new RatisDiskRunwayRecovery(scheduler,diskRunway,this::recoveryTargets);
        poolPressure=new RatisPoolPressureService(scheduler,this::workloadPressure,diskRunway,diskCensus,diskRecovery);
        var local=new HashSet<UUID>(); for (var id:machines.keySet()) if (groups.require(id.getUuid()).role()==RatisNodePlan.Role.DATA) local.add(id.getUuid());
        if (recoverySettings!=null && !local.isEmpty()) {
            participantRecoveries=new RatisParticipantRecoveryRegistry(root.resolve("recovery"),recoverySettings.homes(),routedClients,group->groups.require(group).directory(),recoverySettings.takeover(),groups::dataGroups,recoverySettings.limits());
            var sources=new ArrayList<RatisNativeParticipantDiscovery.Source>();
            for (var group:local.stream().sorted().toList()) sources.add(new RatisNativeParticipantDiscovery.Source(group,machines.get(RaftGroupId.valueOf(group))::preparedRecoveries));
            nativeParticipantDiscovery=new RatisNativeParticipantDiscovery(participantRecoveries,this::recoverySources);
            participantRecoveryService=new RatisTransactionRecoveryService(scheduler,() -> {
                nativeParticipantDiscovery.cycle(recoverySettings.visits());
            });
        }
        if (!machines.containsKey(RaftGroupId.valueOf(plan.catalogue().groupId()))) return;
        clock=new RatisCatalogueClock(root.resolve("sessions/catalogue-clock.bin"),epochs);
        for(var domain:routedClients.keySet().stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList()){Path scope=Files.createDirectories(root.resolve("sessions/data").resolve(domain.toString()));SessionJournalFiles.force(scope);SessionJournalFiles.force(scope.getParent());var propagation=new RatisSessionPropagation(scope,epochs,data(domain),settings.maxPropagationRanges());dataPropagations.put(domain,propagation);propagations.add(propagation);}
        for (var home:plan.homes()) {
            Path scope=Files.createDirectories(root.resolve("sessions").resolve(home.role().name().toLowerCase(Locale.ROOT))); SessionJournalFiles.force(scope); SessionJournalFiles.force(scope.getParent());
            propagations.add(new RatisSessionPropagation(scope,epochs,homeClients.get(home.role()),settings.maxPropagationRanges()));
        }
        for(var owner:pools.values())for(var seed:owner.pool.topologySeeds())for(var target:seed.definition().targets()){var store=stores.get(target.groupId());var activation=store==null ? null : store.topologyActivation();if(activation!=null)observeTopologyActivation(seed.definition(),activation);}
        freezeStartupDirectories();
        service=new RatisSessionClockService(scheduler,clock,propagations,time,settings.propagationVisits(),this::clockOwnerReady);
    }
    private DistributedSchema seedSchema(NativeTopologySeed seed)throws IOException{
        var logicalSchema=logicalSchemas.get(seed.definition().header().domainId());if(logicalSchema!=null && Arrays.equals(logicalSchema.digest(),seed.definition().targetDirectory().schemaDigest())){logicalSchema.requireMatches(seed.definition().targetDirectory());return logicalSchema;}
        for(var parent:seed.definition().parents()){var store=stores.get(parent.groupId());var schema=store==null ? null : store.installedSchema();if(schema!=null){schema.requireMatches(seed.definition().targetDirectory());return schema;}}
        throw new IOException("Forced topology seed has no qualified local native source schema");
    }
    private RatisTopologyCatalogue.QualifiedRelocation qualifyRelocationPublication(TopologyChange.Definition definition,TopologyChange.RelocationProof expected)throws IOException {
        var actual=relocationClients.read(new RatisRelocationOwnerRpc.Request(definition,expected.owner().replicaId(),RatisRelocationOwnerRpc.Expected.TARGET,expected.requiredCatchupCut(),null));
        var configuration=actual.configuration();
        if(!actual.caughtUp() || !actual.owner().equals(expected.owner()) || !new ApplyPosition(configuration.term(),configuration.index()).equals(expected.configurationPosition()) || !Arrays.equals(OperatorUpload.sha256(configuration.data()),expected.configurationDigest()) || !RatisRelocationOwnerRpc.covers(actual.completedCut(),expected.completedPosition()))throw new IOException("Relocation publication lost its original owner/configuration/catch-up proof");
        for(var target:definition.targets().getFirst().replicas())if(definition.parents().getFirst().replicas().stream().noneMatch(old->old.replicaId().equals(target.replicaId()) && old.role()==target.role())){
            var ready=relocationClients.read(new RatisRelocationOwnerRpc.Request(definition,target.replicaId(),RatisRelocationOwnerRpc.Expected.TARGET,expected.requiredCatchupCut(),null));
            if(!ready.caughtUp() || !Arrays.equals(ready.configuration().data(),configuration.data()))throw new IOException("Changed target owner has not completed actual target configuration/tail");
        }
        return new RatisTopologyCatalogue.QualifiedRelocation(expected,actual.completedCut());
    }
    private void configureDataMachine(RatisRangeStateMachine machine,RangeDirectory directory,it.cavallium.rockserver.core.common.distributed.RangeDescriptor descriptor){var owner=owner(descriptor.groupId());var pool=owner.pool;var schemaReplicas=owner.schemas;var snapshotCohorts=owner.cohorts;var sourceSnapshots=owner.sources;
        machine.migrationTail(bytes->{try{var lease=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.ORDINARY,bytes,1);try{if(!migration.acquire(descriptor.groupId(),owner.spec.id(),bytes,false)){lease.close();return null;}return new org.apache.ratis.server.leader.MigrationLogStateMachine.Lease(){boolean materialized;public void materialized(long actual)throws IOException{if(materialized || actual<0 || actual>bytes)throw new IOException("Listener payload exceeds its admitted upper bound");materialized=true;migration.refundUnused(descriptor.groupId(),owner.spec.id(),bytes-actual);lease.shrinkTo(actual,1);}public void close(){lease.close();}};}catch(Exception failed){lease.close();return null;}}catch(IOException exhausted){return null;}});
        machine.role(RatisNodePlan.Role.DATA);machine.snapshotCohorts(snapshotCohorts);machine.snapshotReadOnlyMaximumTtl(Duration.ofMillis(Math.min(owner.pins().maximumAge().toMillis(),RatisSnapshotCaptureRequest.maximumTtl(RatisSnapshotCaptureRequest.Mode.READ_ONLY).toMillis())));machine.relocationOwnerQueries(relocationOwnerQueries);machine.sourceSnapshots(topologyImageQueries,sourceSnapshots);machine.relocationObserver(new RatisRangeStateMachine.RelocationObserver(){public void persist(RatisTopologyRelocationRecord record,ApplyPosition position)throws IOException{persistRelocation(record,position);}public boolean saved(RatisTopologyRelocationRecord record)throws IOException{return savedRelocation(record);}});machine.relocationAuthority(epochs.topologyJoinAuthority()::read);machine.sourceOutcomeQueries(sourceOutcomeQueries);machine.cdcBinding(directory.clusterId(),directory.databaseId(),descriptor.rangeId());machine.schemaStorage(pool);machine.schemaActivated(this::observeSchemaActivation);
        var authority=epochs.schemaAuthority();machine.schemaAuthority((header,digest)->readSchemaAuthority(authority,header,digest),directory);machine.schemaReplicas(schemaReplicas,ownReplica(descriptor));machine.topologyAuthority(epochs.topologyAuthority()::read,directory,ownReplica(descriptor),pool);machine.topologySeedInstaller(this::seedTopology);machine.topologyActivated(this::observeTopologyActivation);
        if(transactionHomes!=null)machine.sourceOutcomeAdmission(new RatisSourceOutcomeAdmission(new RatisSourceOutcomeAdmission.Authority(){
            public it.cavallium.rockserver.core.common.distributed.TransactionStatus home(it.cavallium.rockserver.core.common.distributed.HomeIdentity original)throws IOException{var configured=plan.home(RatisNodePlan.Role.TRANSACTION_HOME);if(configured==null || !configured.column().equals(original.columnId()))throw new IOException("Source proof HOME column is not configured");var read=transactionHomes.readRegistration(original.transactionId());if(!read.descriptor().groupId().equals(original.groupId()) || read.descriptor().rangeEpoch()!=original.rangeEpoch() || !Arrays.equals(configured.directory().schemaDigest(),original.schemaDigest()))throw new IOException("Source proof changed exact HOME placement");return read.status();}
            public it.cavallium.rockserver.core.common.distributed.ParticipantOutcome outcome(RangeDirectory frozen,RangeDescriptor route,UUID transaction)throws IOException{return data(frozen.domainId()).finalizedParticipant(frozen,route,transaction);}
            public it.cavallium.rockserver.core.common.distributed.ParticipantFenceProof fence(RangeDirectory frozen,RangeDescriptor route,it.cavallium.rockserver.core.common.distributed.ParticipantIdentity identity)throws IOException{return data(frozen.domainId()).participantFence(frozen,route,identity);}
        })::qualify);
        if(captureHomes!=null)machine.snapshotHomes(captureHomes);if(transactionHomes!=null){machine.transactionHomes(transactionHomes);machine.snapshotRecovery(this::recoverParticipant,snapshotHelp);}
    }
    static RatisTopologySourceSnapshotRpc.Request sourceRequest(TopologyChange.Definition definition,UUID catalogue,ReplicaDescriptor receiver){var purpose=definition.parents().getFirst().replicas().stream().anyMatch(r->r.replicaId().equals(receiver.replicaId()))?RatisTopologySourceSnapshotRpc.Purpose.RECOVERY:RatisTopologySourceSnapshotRpc.Purpose.BOOTSTRAP;var bytes=ByteBuffer.allocate(65).put(definition.digest()).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).putLong(receiver.replicaId().getMostSignificantBits()).putLong(receiver.replicaId().getLeastSignificantBits()).put((byte)purpose.ordinal());return new RatisTopologySourceSnapshotRpc.Request(UUID.nameUUIDFromBytes(bytes.array()),definition.header(),catalogue,receiver,purpose);}
    static RatisTopologySourceSnapshotRpc.Request frozenSourceRequest(TopologyChange.Definition definition,UUID catalogue,ReplicaDescriptor receiver,UUID sourceTransferId){var initial=sourceRequest(definition,catalogue,receiver);return new RatisTopologySourceSnapshotRpc.Request(sourceTransferId,initial.header(),initial.catalogue(),initial.receiver(),initial.purpose());}
    private RatisTopologySourceSnapshotRpc.Prepared prepareLearnerRecovery(RatisTopologySourceSnapshotRpc.Request request)throws Exception{
        synchronized(this){if(!started || closing || closeRequested || installing)throw new IOException("Learner recovery node owner is unavailable");installing=true;}
        try{
            if(request.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.RECOVERY)throw new IOException("Registered image recovery purpose required");var receiver=request.receiver();if(!receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation()) || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address()))throw new IOException("Recovery receiver changed local node identity");
            var view=sourceView(request);var definition=view.definition();var owner=owner(definition.parents().getFirst().groupId());var pool=owner.pool;var store=stores.get(definition.parents().getFirst().groupId());if(store==null)throw new IOException("No registered local recovery store");var registration=store.topologyLearnerRegistration();if(registration==null || !sameLearnerIdentity(registration.receiver(),receiver))throw new IOException("Recovery lacks original receiver registration");
            var prepared=relocationClients.prepare(request,definition);var names=new HashMap<UUID,String>();for(var local:stores.entrySet())if(owner.spec.id().equals(groupPools.get(local.getKey())))names.putAll(pool.schemaPhysicalNames(local.getValue()));var permission=recoveryPermission(store,view,prepared,names);
            var saved=store.topologyLearnerImagePermission();if(saved!=null && Arrays.equals(saved.encode(),permission.encode()))return prepared;
            var after=sourceView(request);if(!Arrays.equals(after.definition().encode(),definition.encode()) || !RatisRelocationOwnerRpc.covers(after.cut(),view.cut()))throw new IOException("Recovery catalogue witness changed");pool.prepareTopologyLearnerImage(store,permission);return prepared;
        }finally{synchronized(this){installing=false;}}
    }
    static it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerImagePermission recoveryPermission(ReplicatedRangeStore store,RatisTopologyJoinAuthority.View view,RatisTopologySourceSnapshotRpc.Prepared prepared,Map<UUID,String> receiverBindings)throws IOException{
        var request=prepared.request();request.header().requireDefinition(view.definition());if(request.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.RECOVERY || !request.catalogue().equals(view.catalogue()) || view.state().phase()!=TopologyChange.Phase.JOINING)throw new IOException("Recovery image lacks current irreversible catalogue qualification");var registration=store.topologyLearnerRegistration();if(registration==null || !sameLearnerIdentity(registration.receiver(),request.receiver()))throw new IOException("Recovery changed registered receiver identity");
        var definition=view.definition();RatisRangeStateMachine.requireSchemaMembership(definition.parents().getFirst(),prepared.sourceCfg().data());prepared.schema().requireMatches(definition.sourceDirectory());prepared.schema().requireMatches(definition.targetDirectory());var own=new HashMap<UUID,String>();var merges=new HashSet<UUID>();for(var column:prepared.schema().columns()){own.put(column.columnId(),receiverBindings.getOrDefault(column.columnId(),"relocation-"+column.columnId()));if(column.operator()!=null)merges.add(column.columnId());}
        return new it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerImagePermission(registration,definition,view.catalogue(),view.cut(),prepared.image().topologyOrigin(),prepared.schema(),prepared.image().position(),prepared.image().sha256(),prepared.sourceCfg().data(),own,merges,prepared.schemaRows(),prepared.request().transferId());
    }
    private static boolean sameLearnerIdentity(ReplicaDescriptor before,ReplicaDescriptor after){return before.nodeId().equals(after.nodeId()) && before.nodeIncarnation().equals(after.nodeIncarnation()) && before.replicaId().equals(after.replicaId()) && before.peerId().equals(after.peerId()) && before.address().equals(after.address());}
    private LearnerSourceEvidence loadLearnerSource(RatisTopologyJoinAuthority.View view,ReplicaDescriptor receiver)throws IOException{if(view.state().phase()!=TopologyChange.Phase.JOINING)throw new IOException("Production learner bootstrap requires irreversible JOINING");var chosen=sourceRequest(view.definition(),view.catalogue(),receiver);RatisTopologySourceSnapshotRpc.Prepared prepared=null;for(int attempts=0;attempts<RatisTopologySourceSnapshotService.MAX_RELEASED;attempts++){try{prepared=relocationClients.prepare(chosen,view.definition());break;}catch(RatisTopologySourceSnapshotRpc.AbandonedException terminal){chosen=terminal.receipt().successorRequest();}}if(prepared==null)throw new IOException("Source successor publication chain exhausted");var bindings=plan.domain(prepared.schema().domainId());if(!bindings.columns().keySet().equals(prepared.schema().columnIds()))throw new IOException("Receiver has no configured physical bindings for source schema");return new LearnerSourceEvidence(prepared.image(),prepared.schema(),prepared.sourceOwner(),prepared.sourceCfg(),prepared.image().position(),bindings.columns(),bindings.merges(),prepared.schemaRows(),prepared.request().transferId());}
    private RatisTopologyJoinAuthority.View sourceView(RatisTopologySourceSnapshotRpc.Request request)throws IOException{var query=RatisTopologyJoinAuthority.Request.of(request.header(),request.receiver().replicaId(),request.purpose()==RatisTopologySourceSnapshotRpc.Purpose.BOOTSTRAP?RatisTopologyJoinAuthority.Purpose.REGISTER:RatisTopologyJoinAuthority.Purpose.CONFIGURATION);var view=joinAuthority.read(query);view.require(query);if(!view.catalogue().equals(plan.catalogue().groupId()) || !view.catalogue().equals(request.catalogue()) || view.state().phase()!=TopologyChange.Phase.JOINING)throw new IOException("Source image requires exact irreversible JOINING");return view;}
    private RatisTopologySourceSnapshotService.Host sourceSnapshotHost(PoolOwner owner){var sourceArtifacts=owner.artifacts;return new RatisTopologySourceSnapshotService.Host(){
        public RatisTopologySourceSnapshotService.Lease capture(RatisTopologySourceSnapshotRpc.Request request,NativeRangeImage.IoAdmission admission)throws Exception{
            if(!owner.spec.id().equals(groupPools.get(request.header().parents().getFirst().groupId())))throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Source request crossed physical pool");
            final RatisNodeResourceBudget.Lease reservation;try{reservation=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,65536L+6L*RatisTopologySourceSnapshotRpc.MAX_REPLY_BYTES,1);}catch(IOException exhausted){throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Source metadata graph admission exhausted");}
            RatisTopologySourceSnapshotService.Lease captured=null;boolean returned=false;
            try{
                var view=sourceView(request);var machine=machines.get(RaftGroupId.valueOf(view.definition().parents().getFirst().groupId()));if(machine==null)throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Source snapshot has no local source owner");sourceArtifacts.reserve(request);
                captured=machine.captureSourceSnapshot(request,view.definition(),logicalSchemas.get(request.header().domainId()),sourceArtifacts,admission);reservation.shrinkTo(Math.addExact(65536L,6L*RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,captured.prepared()).length),1);
                var actual=captured;var lease=new RatisTopologySourceSnapshotService.Lease(actual.prepared(),actual.source(),actual.snapshot(),()->{actual.close();reservation.close();});returned=true;return lease;
            }finally{if(!returned){if(captured==null){var sourceMachine=machines.get(RaftGroupId.valueOf(request.header().parents().getFirst().groupId()));AutoCloseable cleanup=()->{if(sourceMachine!=null)sourceMachine.drainSourceCapture(request.transferId());reservation.close();};try{cleanup.close();}catch(Exception failed){owner.sourceCleanup.put(request.transferId(),cleanup);throw failed;}}else{var actual=captured;AutoCloseable cleanup=()->{actual.close();reservation.close();};try{cleanup.close();}catch(Exception failure){owner.sourceCleanup.put(request.transferId(),cleanup);throw failure;}}}}
        }
        public RatisTopologySourceSnapshotRpc.Abandoned abandoned(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return sourceArtifacts.abandoned(request);}
        public RatisTopologySourceSnapshotRpc.Abandoned abandon(RatisTopologySourceSnapshotRpc.Request request)throws Exception{
            var view=sourceView(request);var group=view.definition().parents().getFirst().groupId();var machine=machines.get(RaftGroupId.valueOf(group));var store=stores.get(group);if(machine==null||store==null)throw new IOException("Original source capture owner unavailable");var cleanup=owner.sourceCleanup.get(request.transferId());if(cleanup!=null){cleanup.close();owner.sourceCleanup.remove(request.transferId(),cleanup);}machine.drainSourceCapture(request.transferId());
            var evidence=machine.relocationEvidence(new RatisRelocationOwnerRpc.Request(view.definition(),null,RatisRelocationOwnerRpc.Expected.SOURCE,null,null));return sourceArtifacts.abandon(request,evidence.configuration(),evidence.completedCut());
        }
        public void requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{sourceArtifacts.requireReady(lease.prepared());var view=sourceView(lease.prepared().request());var machine=machines.get(RaftGroupId.valueOf(view.definition().parents().getFirst().groupId()));if(machine==null || lease.source().getStateMachine()!=machine)throw new IOException("Source snapshot division changed");var evidence=machine.relocationEvidence(new RatisRelocationOwnerRpc.Request(view.definition(),null,RatisRelocationOwnerRpc.Expected.SOURCE,null,null));if(!evidence.owner().replicaId().equals(lease.prepared().sourceOwner().replicaId()) || !RatisRelocationOwnerRpc.covers(evidence.completedCut(),lease.prepared().image().position()) || !Arrays.equals(evidence.configuration().data(),lease.prepared().sourceCfg().data()))throw new IOException("Retained source image lost actual native owner/configuration");}
        public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{var view=sourceView(lease.prepared().request());var image=lease.prepared().image();var evidence=relocationClients.read(new RatisRelocationOwnerRpc.Request(view.definition(),lease.prepared().request().receiver().replicaId(),RatisRelocationOwnerRpc.Expected.IMAGE,image.position(),image.sha256()));if(!evidence.imageInstalled())throw new IOException("Receiver has not forced the exact native image");}
        public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{sourceArtifacts.released(lease.prepared().request());}
        public boolean isReleased(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return sourceArtifacts.isReleased(request);}
        public boolean chunkReady(RatisTopologySourceSnapshotRpc.Request request,long bytes)throws IOException{return migration.acquire(request.header().parents().getFirst().groupId(),owner.spec.id(),bytes,request.purpose()==RatisTopologySourceSnapshotRpc.Purpose.RECOVERY);}
        public void transferFinished(RatisTopologySourceSnapshotRpc.Request request){migration.forget(request.header().parents().getFirst().groupId(),owner.spec.id());}
        public org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto send(org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto request)throws IOException{if(server==null || serverClosed)throw new IOException("Source server unavailable");var receiver=RaftPeerId.valueOf(request.getServerRequest().getReplyId());server.getServerRpc().addRaftPeers(List.of(RaftPeer.newBuilder().setId(receiver).setAddress(sourceViewFromRequest(request,receiver)).build()));return server.getServerRpc().installSnapshot(request);}
    };}
    private String sourceViewFromRequest(org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto request,RaftPeerId receiver)throws IOException{var group=org.apache.ratis.util.ProtoUtils.toRaftGroupId(request.getServerRequest().getRaftGroupId());var entry=groups.require(group.getUuid());if(entry.relocation()==null)throw new IOException("Snapshot transfer lacks forced relocation intent");return entry.relocation().definition().targets().getFirst().replicas().stream().filter(r->r.peerId().equals(receiver.toString())).findFirst().orElseThrow(()->new IOException("Snapshot receiver not in exact target")).address();}
    interface RelocationJournalIo {
        void write(Path file,byte[] body,int maximum)throws IOException;
        void force(Path file)throws IOException;
    }
    static final RelocationJournalIo RELOCATION_FILES=new RelocationJournalIo(){
        public void write(Path file,byte[] body,int maximum)throws IOException{SessionJournalFiles.write(file,body,maximum);}
        public void force(Path file)throws IOException{try(var channel=FileChannel.open(file,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)){channel.force(true);}SessionJournalFiles.force(file.getParent());SessionJournalFiles.force(file.getParent().getParent());}
    };
    private RelocationJournalIo relocationJournalIo=RELOCATION_FILES;
    private Path relocationPath(RatisTopologyRelocationRecord record){return root.resolve("topology-relocations").resolve(record.definition().intentId()+"."+record.action().name());}
    private void persistRelocation(RatisTopologyRelocationRecord record,ApplyPosition position)throws IOException{
        var directory=Files.createDirectories(root.resolve("topology-relocations"));SessionJournalFiles.force(root);var path=relocationPath(record);byte[] encoded=record.encode();
        if(Files.exists(path)){var original=SessionJournalFiles.read(path,RatisTopologyRelocationRecord.MAX_BYTES+16);if(original.length<16 || !Arrays.equals(Arrays.copyOfRange(original,16,original.length),encoded))throw new IOException("Relocation journal changed original committed record");relocationJournalIo.force(path);}
        else{int count=0;long bytes=0;try(var files=Files.list(directory)){for(var file:files.toList())if(Files.isRegularFile(file)){count++;bytes+=Files.size(file);}}if(count>=128 || bytes+encoded.length+16>64L*1024*1024)throw new IOException("Node relocation history budget exhausted");var out=ByteBuffer.allocate(16+encoded.length).putLong(position.term()).putLong(position.index()).put(encoded);relocationJournalIo.write(path,out.array(),RatisTopologyRelocationRecord.MAX_BYTES+16);relocationJournalIo.force(path);}
        groups.relocation(record);
        var routed=data(record.definition().header().domainId());if(record.action()==RatisTopologyRelocationRecord.Action.COMPLETE && routed.directory().domainId().equals(record.definition().header().domainId()) && routed.directory().generation()<=record.definition().targetDirectory().generation())routed.refresh(record.definition().targetDirectory());
    }
    private boolean savedRelocation(RatisTopologyRelocationRecord record)throws IOException{
        var path=relocationPath(record);if(!Files.exists(path))return false;var bytes=SessionJournalFiles.read(path,RatisTopologyRelocationRecord.MAX_BYTES+16);if(bytes.length<16 || !Arrays.equals(Arrays.copyOfRange(bytes,16,bytes.length),record.encode()))throw new IOException("Relocation saved record differs");relocationJournalIo.force(path);var in=ByteBuffer.wrap(bytes);var position=new ApplyPosition(in.getLong(),in.getLong());var store=stores.get(record.definition().parents().getFirst().groupId());return store!=null && store.applied()!=null && RatisRelocationOwnerRpc.covers(store.applied(),position);
    }
    private void recoverRelocationSidecars(Path directory)throws IOException{
        try(var files=Files.list(directory)){for(var pending:files.filter(p->p.getFileName().toString().endsWith(".pending")).sorted().toList()){
            if(!Files.isRegularFile(pending,LinkOption.NOFOLLOW_LINKS))throw new IOException("Relocation sidecar is not an owned regular file");
            var body=SessionJournalFiles.read(pending,RatisTopologyRelocationRecord.MAX_BYTES+16);if(body.length<16)throw new IOException("Truncated relocation sidecar");var in=ByteBuffer.wrap(body);new ApplyPosition(in.getLong(),in.getLong());var record=RatisTopologyRelocationRecord.decode(Arrays.copyOfRange(body,16,body.length));var original=relocationPath(record);
            if(!pending.equals(original.resolveSibling(original.getFileName()+".pending")) || !record.catalogue().equals(plan.catalogue().groupId()) || !record.definition().header().clusterId().equals(plan.clusterId()) || !record.definition().header().databaseId().equals(plan.databaseId()))throw new IOException("Foreign relocation sidecar");
            if(Files.exists(original,LinkOption.NOFOLLOW_LINKS)){if(!Arrays.equals(body,SessionJournalFiles.read(original,RatisTopologyRelocationRecord.MAX_BYTES+16)))throw new IOException("Relocation sidecar changed original intent");relocationJournalIo.force(original);Files.delete(pending);SessionJournalFiles.force(directory);}
            else{Files.move(pending,original,StandardCopyOption.ATOMIC_MOVE);relocationJournalIo.force(original);}
        }}
    }
    private void loadRelocationIntents()throws IOException{
        var directory=root.resolve("topology-relocations");if(!Files.exists(directory))return;recoverRelocationSidecars(directory);var records=new ArrayList<RatisTopologyRelocationRecord>();long bytes=0;try(var files=Files.list(directory)){for(var path:files.toList()){if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || (bytes+=Files.size(path))>64L*1024*1024 || records.size()>=128)throw new IOException("Invalid node relocation history");var encoded=SessionJournalFiles.read(path,RatisTopologyRelocationRecord.MAX_BYTES+16);if(encoded.length<16)throw new IOException("Truncated relocation history");var in=ByteBuffer.wrap(encoded);new ApplyPosition(in.getLong(),in.getLong());var record=RatisTopologyRelocationRecord.decode(Arrays.copyOfRange(encoded,16,encoded.length));if(!path.equals(relocationPath(record)) || !record.catalogue().equals(plan.catalogue().groupId()))throw new IOException("Foreign node relocation history");records.add(record);}}
        records.sort(Comparator.comparingLong((RatisTopologyRelocationRecord r)->r.definition().sourceDirectory().generation()).thenComparing(r->r.action().ordinal()));for(var record:records)groups.relocation(record);
    }
    private boolean nativeAdmissionOpen(UUID group){var store=stores.get(group);return store!=null && store.admissionOpen();}
    private List<RatisRangeStateMachine> expiryMachines(){return groups.entries().stream().filter(entry->entry.role()==RatisNodePlan.Role.DATA && !entry.retired() && nativeAdmissionOpen(entry.group().getGroupId().getUuid())).map(entry->machines.get(entry.group().getGroupId())).filter(Objects::nonNull).toList();}
    private List<RatisNativeParticipantDiscovery.Source> recoverySources(){return groups.entries().stream().filter(entry->entry.role()==RatisNodePlan.Role.DATA && !entry.retired() && nativeAdmissionOpen(entry.group().getGroupId().getUuid())).filter(entry->machines.containsKey(entry.group().getGroupId())).map(entry->new RatisNativeParticipantDiscovery.Source(entry.group().getGroupId().getUuid(),machines.get(entry.group().getGroupId())::preparedRecoveries)).toList();}
    private Path seedExposure(UUID group){return root.resolve("topology-exposure").resolve(group.toString());}
    private byte[] seedExposureBytes(RatisRuntimeGroups.Entry entry){return ByteBuffer.allocate(80).putLong(entry.group().getGroupId().getUuid().getMostSignificantBits()).putLong(entry.group().getGroupId().getUuid().getLeastSignificantBits()).put((entry.learner()!=null ? entry.learner().digest() : entry.seed().digest())).put(plan.identityDigest()).array();}
    private void validateSeedExposures()throws IOException{
        var directory=root.resolve("topology-exposure");if(!Files.exists(directory))return;
        try(var files=Files.list(directory)){for(var path:files.toList()){final UUID id;try{id=UUID.fromString(path.getFileName().toString());}catch(IllegalArgumentException malformed){throw new IOException("Invalid topology exposure path",malformed);}if(!path.getFileName().toString().equals(id.toString()))throw new IOException("Noncanonical topology exposure path");final RatisRuntimeGroups.Entry entry;try{entry=groups.require(id);}catch(IllegalArgumentException unknown){throw new IOException("Topology exposure has no forced registered birth",unknown);}if(entry.seed()==null && entry.learner()==null || !Files.isRegularFile(path) || !Arrays.equals(SessionJournalFiles.read(path,128),seedExposureBytes(entry)))throw new IOException("Topology exposure differs from forced birth/bootstrap");}}
    }
    private void requireSeedStorage(RatisRuntimeGroups.Entry entry)throws IOException{
        if(entry.seed()==null && entry.learner()==null)return;var id=entry.group().getGroupId().getUuid();var exposure=seedExposure(id);
        boolean exposed=Files.exists(exposure);if(exposed && (!Files.isRegularFile(exposure) || !Arrays.equals(SessionJournalFiles.read(exposure,128),seedExposureBytes(entry))))throw new IOException("Topology exposure differs from forced birth/bootstrap");
        var directory=root.resolve("raft").resolve(id.toString());
        if(Files.isRegularFile(directory.resolve("current/raft-meta"))){if(!exposed)throw new IOException("Topology child Raft state lacks prior exposure intent");return;}
        if(exposed && !virginSeedAdds.contains(id))throw new IOException("Exposed topology child Raft storage is missing: "+entry.group().getGroupId());
        var store=stores.get(id);if(store==null || store.applied()!=null || !store.topologyBirthPending())throw new IOException("Established topology child Raft storage is missing: "+entry.group().getGroupId());
        if(Files.exists(directory))try(var files=Files.walk(directory)){if(files.anyMatch(Files::isRegularFile))throw new IOException("Virgin topology birth has unidentified partial Raft storage");}
    }
    private void markSeedExposure(RatisRuntimeGroups.Entry entry)throws IOException{
        if(entry.seed()==null && entry.learner()==null)return;var id=entry.group().getGroupId().getUuid();var exposure=seedExposure(id);if(Files.exists(exposure))return;
        // Record validation precedes sealing; only this live registration attempt can finish adding a virgin.
        requireSeedStorage(entry);var directory=Files.createDirectories(exposure.getParent());SessionJournalFiles.force(root);SessionJournalFiles.write(exposure,seedExposureBytes(entry),128);SessionJournalFiles.force(directory);virginSeedAdds.add(id);
    }
    private void addGroup(RaftGroup group)throws IOException{
        requireSeedStorage(groups.require(group.getGroupId().getUuid()));IOException uncertainty=null;
        try{var reply=server.groupManagement(GroupManagementRequest.newAdd(ClientId.randomId(),server.getId(),++groupManagementCall,group,false));if(reply!=null && reply.isSuccess()){verify(group);return;}uncertainty=new IOException("Local group addition failed: "+group.getGroupId(),reply==null ? null : reply.getException());}
        catch(IOException failed){uncertainty=failed;}finally{virginSeedAdds.remove(group.getGroupId().getUuid());}
        var actual=new HashSet<RaftGroupId>();server.getGroupIds().forEach(actual::add);
        if(actual.contains(group.getGroupId())){try{verify(group);return;}catch(IOException mismatch){mismatch.addSuppressed(uncertainty);throw mismatch;}}
        throw uncertainty;
    }
    public List<RaftGroup> localGroups(){return groups.raftGroups();}
    public RatisNodePlan.Role role(UUID group){return groups.require(group).role();}
    public RangeDirectory groupDirectory(UUID group){var directory=groups.require(group).directory();if(directory==null)throw new IllegalArgumentException("Catalogue has no DATA directory");return directory;}
    public it.cavallium.rockserver.core.common.distributed.RangeDescriptor groupDescriptor(UUID group){var descriptor=groups.require(group).descriptor();if(descriptor==null)throw new IllegalArgumentException("Catalogue has no DATA descriptor");return descriptor;}
    /** Only an already forced native recipe may extend the local voting registry. Exact retry never formats an owner. */
    public void installTopologySeed(NativeTopologySeed seed)throws IOException{
        beginSeedLifecycle(true);try{installTopologySeedOwned(seed);}catch(org.rocksdb.RocksDBException failure){throw new IOException("Native topology children could not register",failure);}finally{endSeedLifecycle();}
    }
    private void beginSeedLifecycle(boolean seed)throws IOException{synchronized(this){if(installing || starting || closing || closeRequested || closed || seed && learnerAttempt!=null || !seed && seedAttempt!=null)throw new IOException("Node lifecycle or retained force attempt prevents another topology mutation");installing=true;}}
    private void endSeedLifecycle(){synchronized(this){installing=false;}}
    private void installTopologySeedOwned(NativeTopologySeed seed)throws IOException,org.rocksdb.RocksDBException{
            Objects.requireNonNull(seed);var owner=seedOwner(seed.definition().intentId());reserveDataMetadata(owner,seed.definition().targets().stream().map(RangeDescriptor::groupId).toList());var pool=owner.pool;var forced=pool.topologySeed(seed.definition().intentId());if(forced==null || !forced.equals(seed))throw new IOException("Topology seed is not the exact forced native artifact");for(var parent:forced.definition().parents())assignPool(parent.groupId(),owner.spec.id());for(var target:forced.definition().targets())assignPool(target.groupId(),owner.spec.id());groups.register(forced);
            for(var target:forced.definition().targets()){
                var id=RaftGroupId.valueOf(target.groupId());if(machines.containsKey(id))continue;var entry=groups.require(target.groupId());var store=pool.reopenTopologyRange(forced.birth(target.groupId()),seedSchema(forced));
                var directory=store.schemaInstallation()==null ? entry.directory() : store.schemaInstallation().definition().targetDirectory();
                var machine=RatisRangeStateMachine.withSessions(store,entry.group(),owner.images(),command->{},epochs,directory,owner.retention());machine.nodeResources(resources,owner.spec.id());machine.logResources(logBudget(store.ownership().owner().groupId(),owner.spec.id()));machine.diskRunway(diskRunway,owner.spec.id());machine.committedApply(committedDispatcher,owner.spec.id(),MAX_COMMITTED_ENTRY_BYTES);configureDataMachine(machine,directory,target);machine.learnerInstaller(topologyImageQueries,this::joinTopologyLearner);machine.learnerRecoveryInstaller(this::prepareLearnerRecovery);machine.proofQueries(proofQueries);machine.exportSources(exportSources);stores.put(target.groupId(),store);machines.put(id,machine);
            }
            // Every machine and borrowed node service is configured before any target can vote.
            for(var target:forced.definition().targets())requireSeedStorage(groups.require(target.groupId()));
            for(var target:forced.definition().targets())markSeedExposure(groups.require(target.groupId()));
            if(started){var actual=new HashSet<RaftGroupId>();server.getGroupIds().forEach(actual::add);for(var target:forced.definition().targets()){var group=groups.require(target.groupId()).group();if(!actual.contains(group.getGroupId()))addGroup(group);else verify(group);}}
    }
    /** Forces receiver-local provenance before adding a fenced LISTENER. No source membership change is performed. */
    public NativeTopologyLearnerRegistration joinTopologyLearner(RatisTopologyJoinAuthority.Request request)throws IOException {
        beginSeedLifecycle(false);try{
            if(!started || server==null)throw new IOException("Learner join requires running node owner");
            if(learnerAttempt!=null){var attempt=learnerAttempt;var pool=pools.get(learnerAttemptPool).pool;var expected=RatisTopologyJoinAuthority.Request.of(attempt.definition(),attempt.receiver().replicaId());if(!Arrays.equals(RatisTopologyJoinAuthority.request(request),RatisTopologyJoinAuthority.request(expected)))throw new IOException("Unknown learner force requires exact original request");var authority=new RatisTopologyJoinAuthority.View(attempt.catalogue(),attempt.catalogueCut(),attempt.definition(),TopologyChange.State.begin(attempt.definition().header()));requireLearnerReceiver(authority,authority.require(request));pool.openTopologyLearner(attempt);installTopologyLearnerOwned(attempt);learnerAttempt=null;return attempt;}

            for(var owner:pools.values())for(var saved:owner.pool.topologyLearners())if(saved.definition().intentId().equals(request.intent())){var view=new RatisTopologyJoinAuthority.View(saved.catalogue(),saved.catalogueCut(),saved.definition(),TopologyChange.State.begin(saved.definition().header()));view.require(request);installTopologyLearnerOwned(saved);return saved;}
            if(learnerSource==null)throw new IOException("Trusted pinned source image reader is not configured");
            var view=joinAuthority.read(request);var receiver=view.require(request);requireLearnerReceiver(view,receiver);
            var evidence=learnerSource.read(view,receiver);var source=view.definition().parents().getFirst();evidence.schema().requireMatches(view.definition().sourceDirectory());
            var image=evidence.image();var cfg=evidence.configuration();var cut=evidence.completedCut();
            if(source.replicas().stream().noneMatch(evidence.sourceOwner()::equals) || !image.owner().equals(new RangeOwnership.Owner(source.groupId(),source.rangeEpoch()))
                    || !image.interval().equals(new RangeOwnership.Interval(view.definition().sourceDirectory().domainId(),view.definition().sourceDirectory().columns(),source.lowerInclusive(),source.upperExclusive()))
                    || !Arrays.equals(image.schemaDigest(),evidence.schema().digest()) || cut.term()<1 || cut.index()<image.position().index() || cut.term()<image.position().term() || cut.index()==image.position().index() && cut.term()!=image.position().term()
                    || cfg.term()>image.position().term() || cfg.index()>image.position().index() || cfg.index()==image.position().index() && cfg.term()!=image.position().term())throw new IOException("Source image/native owner/configuration evidence differs from relocation");
            RatisRangeStateMachine.requireSchemaMembership(source,cfg.data());
            var current=joinAuthority.read(request);current.require(request);requireLearnerReceiver(current,receiver);if(!Arrays.equals(current.definition().encode(),view.definition().encode()))throw new IOException("Relocation definition changed during source export");
            var target=current.definition().targets().getFirst();var proposed=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder();
            for(var peer:groups.group(target).getPeers().stream().sorted(Comparator.comparing(peer->peer.getId().toString())).toList()){if(peer.getStartupRole()==RaftPeerRole.LISTENER)proposed.addListeners(peer.getRaftPeerProto());else proposed.addPeers(peer.getRaftPeerProto());}
            var registration=new NativeTopologyLearnerRegistration(current.definition(),image.topologyOrigin(),evidence.schema(),receiver,current.catalogue(),current.cut(),image.position(),image.sha256(),cfg.data(),proposed.build().toByteArray(),evidence.physicalNames(),evidence.mergeColumns(),evidence.sourceSchemaRows(),evidence.sourceTransferId());
            var owner=pools.get(plan.incomingPoolId(view.definition().sourceDirectory().domainId(),target.groupId()));requirePoolCapacity(owner.spec.id(),List.of(target.groupId()));requireIncomingOwnership(view.definition().targetDirectory(),List.of(target),Set.of());reserveDataMetadata(owner,List.of(target.groupId()));learnerAttemptPool=owner.spec.id();learnerAttempt=registration;owner.pool.openTopologyLearner(registration);installTopologyLearnerOwned(registration);learnerAttempt=null;return registration;
        }catch(org.rocksdb.RocksDBException|IllegalArgumentException|IllegalStateException failure){throw new IOException("Learner force/register failed",failure);}finally{endSeedLifecycle();}
    }
    private void requireLearnerReceiver(RatisTopologyJoinAuthority.View view,ReplicaDescriptor receiver)throws IOException{
        requirePropagationCoverage(view.definition().header().domainId(),view.definition().targetDirectory());requireGroupCapacity(view.definition().targets().stream().map(RangeDescriptor::groupId).toList());
        var source=view.definition().sourceDirectory();if(!view.catalogue().equals(plan.catalogue().groupId()) || !receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation()) || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address())
                || !source.clusterId().equals(plan.clusterId()) || !source.databaseId().equals(plan.databaseId()) || !plan.directory(source.domainId()).domainId().equals(source.domainId()))throw new IOException("Learner receiver/catalogue/bootstrap identity differs");
    }
    private void installTopologyLearnerOwned(NativeTopologyLearnerRegistration registration)throws IOException,org.rocksdb.RocksDBException{
        var owner=learnerOwner(registration);reserveDataMetadata(owner,List.of(registration.definition().targets().getFirst().groupId()));var pool=owner.pool;assignPool(registration.definition().targets().getFirst().groupId(),owner.spec.id());var forced=pool.topologyLearners().stream().filter(registration::equals).findFirst().orElseThrow(()->new IOException("Learner is not exact forced native history"));groups.register(forced);var id=RaftGroupId.valueOf(forced.definition().targets().getFirst().groupId());var entry=groups.require(id.getUuid());
        if(!machines.containsKey(id)){var store=pool.openTopologyLearner(forced);var machine=RatisRangeStateMachine.withSessions(store,entry.group(),owner.images(),command->{},epochs,entry.directory(),owner.retention());machine.nodeResources(resources,owner.spec.id());machine.logResources(logBudget(store.ownership().owner().groupId(),owner.spec.id()));machine.diskRunway(diskRunway,owner.spec.id());machine.committedApply(committedDispatcher,owner.spec.id(),MAX_COMMITTED_ENTRY_BYTES);configureDataMachine(machine,entry.directory(),entry.descriptor());machine.learnerInstaller(topologyImageQueries,this::joinTopologyLearner);machine.learnerRecoveryInstaller(this::prepareLearnerRecovery);machine.proofQueries(proofQueries);machine.exportSources(exportSources);stores.put(id.getUuid(),store);machines.put(id,machine);}
        requireSeedStorage(entry);markSeedExposure(entry);var actual=new HashSet<RaftGroupId>();server.getGroupIds().forEach(actual::add);if(!actual.contains(id))addGroup(entry.group());else verify(entry.group());
    }
    private List<ReplicatedRangeStore> requireSeedParents(RatisTopologySeedRpc.Request request,boolean fresh)throws IOException,org.rocksdb.RocksDBException{
        var definition=request.definition();var directory=definition.sourceDirectory();var anchor=groups.require(request.anchorGroup());
        if(!started || server==null || !server.getId().toString().equals(plan.peerId()) || !request.catalogue().equals(plan.catalogue().groupId()) || anchor.role()!=RatisNodePlan.Role.DATA || !anchor.descriptor().equals(request.range())
                || !directory.clusterId().equals(plan.clusterId()) || !directory.databaseId().equals(plan.databaseId()) || !plan.directory(directory.domainId()).domainId().equals(directory.domainId()) || !ownReplica(request.range()).equals(request.owner()))throw new IOException("SEED anchor/local identity/catalogue differs from runtime");
        var parents=new ArrayList<ReplicatedRangeStore>();
        for(var range:definition.parents()){
            var entry=groups.require(range.groupId());var id=RaftGroupId.valueOf(range.groupId());var store=stores.get(range.groupId());var division=server.getDivision(id);
            final long raftTerm;
            synchronized(division){if(entry.role()!=RatisNodePlan.Role.DATA || !entry.descriptor().equals(range) || store==null || division.getStateMachine()!=machines.get(id) || machines.get(id).getLifeCycleState()!=org.apache.ratis.util.LifeCycle.State.RUNNING || division.getInfo().getLifeCycleState()!=org.apache.ratis.util.LifeCycle.State.RUNNING || !division.getInfo().isAlive() || !ownReplica(range).equals(request.owner()))throw new IOException("SEED lacks exact actual running local parent owner");raftTerm=division.getInfo().getCurrentTerm();}
            verify(entry.group());var nativeOwner=store.ownership();var interval=nativeOwner.interval();if(nativeOwner.owner().epoch()!=range.rangeEpoch() || !interval.domainId().equals(directory.domainId()) || !Arrays.equals(interval.lowerInclusive(),range.lowerInclusive()) || !Arrays.equals(interval.upperExclusive(),range.upperExclusive()))throw new IOException("SEED native parent geometry changed");
            var cut=store.applied();var configuration=store.configuration();if(configuration==null || cut==null || configuration.term()>cut.term() || configuration.index()>cut.index() || configuration.index()==cut.index() && configuration.term()!=cut.term() || fresh && cut.term()!=raftTerm)throw new IOException("SEED parent lacks covered actual current native prefix/configuration");RatisRangeStateMachine.requireSchemaMembership(range,configuration.data());
            var freeze=store.lookupTopologyFreeze(definition.header(),request.originalAdmissionDigest());if(freeze==null)throw new IOException("SEED parent has no original native FREEZE");freeze.requireCut(store.applied());freeze.requireParent(range.groupId(),range.rangeEpoch(),directory.domainId(),directory.schemaDigest(),range.lowerInclusive(),range.upperExclusive());
            if(fresh){var schema=store.installedSchema();if(schema==null)throw new IOException("SEED native parent schema missing");schema.requireMatches(directory);}
            parents.add(store);
        }
        return List.copyOf(parents);
    }
    /** Executable local mutation, addressed through an original parent. Returns only the forced local recipe. */
    public NativeTopologySeed seedTopology(RatisTopologySeedRpc.Request request)throws IOException{
        beginSeedLifecycle(true);try{
            Objects.requireNonNull(request);requirePropagationCoverage(request.definition().header().domainId(),request.definition().targetDirectory());requireGroupCapacity(request.definition().targets().stream().map(RangeDescriptor::groupId).toList());var owner=requireSeedPool(request.definition());var pool=owner.pool;var saved=pool.topologySeed(request.definition().intentId());var parents=requireSeedParents(request,saved==null);
            if(saved!=null){request.requireSeed(saved);installTopologySeedOwned(saved);return saved;}
            var attempt=seedAttempt;if(attempt!=null && (!Arrays.equals(attempt.definition().encode(),request.definition().encode()) || !attempt.local().equals(request.owner())))throw new IOException("Unknown SEED force requires exact original definition/local retry");
            if(attempt==null){var view=seedAuthority.read(request.definition().header(),TopologyChange.Stage.SEED);view.require(RatisTopologyAuthority.Request.of(request.definition().header(),TopologyChange.Stage.SEED));if(!view.catalogue().equals(request.catalogue()) || !Arrays.equals(view.definition().encode(),request.definition().encode()) || !Arrays.equals(view.admissionDigest(),request.originalAdmissionDigest()))throw new IOException("Fresh SEED authority changed original definition/catalogue");parents=requireSeedParents(request,true);attempt=new SeedAttempt(request.definition(),view.state(),request.owner(),view.cut());seedAttempt=attempt;}
            reserveDataMetadata(owner,attempt.definition().targets().stream().map(RangeDescriptor::groupId).toList());
            for(var parent:parents) {
                var disk=diskRunway==null?null:diskRunway.reserveRecovery(owner.spec.id(),Math.addExact(2*owner.images().maxImageBytes(),RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES));
                try(var bulk=migration.reserveRecovery(owner.spec.id(),1024*1024);var graph=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,Math.addExact(owner.images().maxRowBytes(),RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES),1)) {
                    owner.anchors.capture(attempt.definition(),parent,parent.topologyFreeze(),attempt.local(),attempt.catalogueCut());
                } catch(org.rocksdb.RocksDBException|IOException failure){throw failure;}catch(Exception failure){throw new IOException("Source recovery anchor did not complete",failure);}finally{if(disk!=null)disk.actualFinished();}
            }
            var seed=pool.seedTopology(attempt.definition(),attempt.authority(),parents,attempt.local());request.requireSeed(seed);seedAttempt=null;installTopologySeedOwned(seed);return seed;
        }catch(org.rocksdb.RocksDBException failure){throw new IOException("Native SEED force/register did not complete",failure);}catch(IllegalArgumentException|IllegalStateException invalid){throw new IOException("Invalid SEED owner/authority/native binding",invalid);}finally{endSeedLifecycle();}
    }
    RatisTopologyRecoveryAnchors.Saved topologyRecoveryAnchor(UUID parent)throws IOException {
        var store=stores.get(parent);if(store==null || store.topologyFreeze()==null)throw new IOException("Source has no retained original native FREEZE");return owner(parent).anchors.read(store.topologyFreeze(),ownReplica(groups.require(parent).descriptor()));
    }
    /** Only a freshly qualified current leaf can leave durable replay-only admission. */
    public void completeTopologyRecovery(UUID group,NativeTopologyRecoveryFence fence)throws IOException {
        beginSeedLifecycle(true);try {
            var store=stores.get(group);var machine=machines.get(RaftGroupId.valueOf(group));if(store==null || machine==null || !fence.equals(store.topologyRecoveryFence()))throw new IOException("Recovery completion lacks exact local native fence");
            var before=seedAuthority.read(fence.header(),TopologyChange.Stage.ACTIVATE);before.require(RatisTopologyAuthority.Request.of(fence.header(),TopologyChange.Stage.ACTIVATE));
            if(!before.catalogue().equals(plan.catalogue().groupId()) || !RatisRelocationOwnerRpc.covers(before.cut(),fence.catalogueCut()) || before.state().phase()!=TopologyChange.Phase.ACTIVE && before.state().phase()!=TopologyChange.Phase.RETIRED)throw new IOException("Recovery leaf lacks current catalogue activation authority");
            var range=before.definition().targets().stream().filter(r->r.groupId().equals(group)).findFirst().orElseThrow();var local=ownReplica(range);if(!TopologyChange.Owner.of(local).equals(fence.owner()))throw new IOException("Recovery receiver identity changed");
            var target=new RatisTopologyOwnerRpc.Target(before.definition(),TopologyChange.Stage.ACTIVATE,plan.catalogue().groupId(),group,local.replicaId());
            var reply=machine.queryStale(org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(RatisTopologyOwnerRpc.request(target))),0).get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);
            var evidence=RatisTopologyOwnerRpc.decodeReply(reply.getContent().toByteArray(),target);var division=server.getDivision(RaftGroupId.valueOf(group));
            if(evidence.completedCut().index()<division.getRaftLog().getLastCommittedIndex())throw new IOException("Recovery owner has not completed its actual committed prefix");
            var after=seedAuthority.read(fence.header(),TopologyChange.Stage.ACTIVATE);if(!Arrays.equals(before.state().encode(),after.state().encode()) || !Arrays.equals(before.definition().encode(),after.definition().encode()))throw new IOException("Current recovery authority changed during owner qualification");
            store.completeTopologyRecovery(fence,before.definition(),evidence.completedCut(),new ReplicatedRangeStore.Configuration(evidence.configurationTerm(),evidence.configurationIndex(),evidence.configuration()));
        }catch(InterruptedException failure){Thread.currentThread().interrupt();throw new IOException("Recovery qualification interrupted",failure);}catch(Exception failure){if(failure instanceof IOException io)throw io;throw new IOException("Current recovery leaf did not qualify",failure);}finally{endSeedLifecycle();}
    }
    private void observeTopologyActivation(TopologyChange.Definition definition,NativeTopologyActivation activation){
        var seed=owner(activation.birth().groupId()).pool.topologySeed(definition.intentId());if(seed==null || !activation.catalogueGroup().equals(plan.catalogue().groupId()))throw new IllegalStateException("Native topology activation has no configured catalogue/forced seed");activation.requireSeed(seed);definition.header().requireDefinition(seed.definition());var store=stores.get(activation.birth().groupId());if(store==null || store.topologyActivation()==null || !Arrays.equals(store.topologyActivation().encode(),activation.encode()))throw new IllegalArgumentException("Topology observer lacks original committed local native activation");activation.requireCut(store.applied());groups.topologyActivated(definition,activation);try{qualifyTopologyActivation(definition,activation);}catch(IOException failure){throw new IllegalStateException("Committed topology clock witness could not persist",failure);}
    }
    private void qualifyTopologyActivation(TopologyChange.Definition definition,NativeTopologyActivation activation)throws IOException{
        activation.header().requireDefinition(definition);requirePropagationCoverage(definition.header().domainId(),definition.targetDirectory());if(!activation.catalogueGroup().equals(plan.catalogue().groupId()))throw new IOException("Topology clock witness catalogue changed");var source=definition.sourceDirectory();var target=definition.targetDirectory();var routed=data(source.domainId());var current=routed.directory();
        if(!current.clusterId().equals(source.clusterId()) || !current.databaseId().equals(source.databaseId()) || !current.domainId().equals(source.domainId()))throw new IOException("Topology clock witness namespace changed");
        if(current.generation()<=target.generation() && !Arrays.equals(current.payloadDigest(),source.payloadDigest()) && !Arrays.equals(current.payloadDigest(),target.payloadDigest()))throw new IOException("Topology clock witness does not follow qualified current source/target");
        var propagation=dataPropagations.get(source.domainId());if(propagation!=null)propagation.qualifyTopology(definition,activation);if(current.generation()<=target.generation())routed.refresh(target);
    }
    public void start() throws IOException {
        synchronized (this) {
            if (installing || starting || started || closeRequested || closed) throw new IllegalStateException("Node cannot start"); starting=true;
        }
        try {
            for(var entry:routedClients.entrySet())if(!Arrays.equals(entry.getValue().directory().payloadDigest(),startupDirectories.get(entry.getKey()).payloadDigest()))throw new IOException("Node placement changed before start");
            // Persist before the first possible network/voting exposure, including a start attempt that later fails.
            SessionJournalFiles.write(root.resolve("node.identity"),identity(true),NODE_IDENTITY_BYTES);
            server.start(); var existing=new HashSet<RaftGroupId>(); server.getGroupIds().forEach(existing::add);
            if (!machines.keySet().containsAll(existing)) throw new IOException("Server recovered an unplanned group");
            for (var group:groups.raftGroups()) if (!existing.contains(group.getGroupId()))addGroup(group);
            var actualIds=new HashSet<RaftGroupId>(); server.getGroupIds().forEach(actualIds::add);
            if (!actualIds.equals(machines.keySet())) throw new IOException("Server division coverage differs from placement");
            for (var group:groups.raftGroups()){verify(group);virginSeedAdds.remove(group.getGroupId().getUuid());}
            if (service!=null) service.start(timer,settings.clockPeriod());
            transactionExpiry.start(timer,Duration.ofMillis(100));
            poolPressure.start(timer);
            if (participantRecoveryService!=null) participantRecoveryService.start(timer,recoverySettings.period());
            if (retirementService!=null) retirementService.start(timer,Duration.ofSeconds(1));
            synchronized (this) { started=true; }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) { closeRequested=true; }
            for (var machine:machines.values()) machine.beginShutdown();
            committedDispatcher.close(); throw failure;
        } finally { synchronized (this) { starting=false; } }
    }
    private static RatisSchemaAuthority.View readSchemaAuthority(RatisSchemaAuthority authority,SchemaChange.Header header,byte[] digest) throws IOException {
        final IOException currentFailure;
        try {
            var current=authority.read(header,digest,false);
            if (current.head()!=null && Arrays.equals(current.head().status().header().encode(),header.encode())) return current;
            currentFailure=new IOException("Current schema head differs from the original operation");
        } catch (IOException failure) {
            return retainedSchemaOutcome(authority,header,digest,failure);
        }
        return retainedSchemaOutcome(authority,header,digest,currentFailure);
    }
    private static RatisSchemaAuthority.View retainedSchemaOutcome(RatisSchemaAuthority authority,SchemaChange.Header header,byte[] digest,IOException currentFailure) throws IOException {
        try { return authority.read(header,digest,true); }
        catch (IOException outcomeFailure) { if (outcomeFailure!=currentFailure) outcomeFailure.addSuppressed(currentFailure); throw outcomeFailure; }
    }
    private static byte[] committedCatalogueValue(ReplicatedRangeStore store,UUID column,byte[] key) {
        try { return store.readCurrentPoint(column,key); }
        catch (org.rocksdb.RocksDBException failure) { throw new IllegalStateException("Committed native catalogue read failed",failure); }
    }
    private void requireOperatorAdmission(it.cavallium.rockserver.core.common.distributed.OperatorUpload upload) {
        var descriptor=upload.operator();
        final it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract contract;
        try { contract=schemaReplicaConfig.policy().admit(descriptor); }
        catch (Exception failure) { throw new IllegalArgumentException("Node operator policy rejected the artifact",failure); }
        if (contract==null || !contract.admitted() || !contract.identifier().equals(descriptor.deterministicContract()) || contract.version()!=descriptor.deterministicContractVersion()) throw new IllegalArgumentException("Node operator policy differs from the exact deterministic contract");
    }
    private it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor ownReplica(it.cavallium.rockserver.core.common.distributed.RangeDescriptor descriptor) {
        return descriptor.replicas().stream().filter(replica -> replica.nodeId().equals(plan.nodeId()) && replica.nodeIncarnation().equals(plan.nodeIncarnation())
                && replica.peerId().equals(plan.peerId()) && replica.address().equals(plan.address())).findFirst().orElseThrow(() -> new IllegalArgumentException("DATA placement lacks this durable node replica identity"));
    }
    private byte[] identity(boolean exposed) {
        return ByteBuffer.allocate(33+RatisNativeExtentPolicy.BYTES).put((byte)(exposed?2:nativeExtentBound?1:0)).put(plan.identityDigest()).put(settings.resources().nativeExtents().encode()).array();
    }
    private void observeSchemaActivation(SchemaChange.Definition definition,NativeSchemaActivationProof proof) {
        groups.schemaActivated(definition,proof);data(definition.header().domainId()).refreshSchema(definition,proof);
    }
    private boolean identityExposed(byte[] bytes)throws IOException{return identityPhase(bytes)==2;}
    private int identityPhase(byte[] bytes) throws IOException {
        if (bytes.length!=33+RatisNativeExtentPolicy.BYTES) throw new IOException("Invalid node identity shape");
        var input=ByteBuffer.wrap(bytes);
        int phase=input.get()&255;
        if (phase>2) throw new IOException("Invalid node exposure phase");
        byte[] digest=new byte[32]; input.get(digest);
        if (!Arrays.equals(digest,plan.identityDigest())) throw new IOException("Node identity/schema/placement binding changed");
        byte[] policy=new byte[RatisNativeExtentPolicy.BYTES];input.get(policy);if(!Arrays.equals(policy,settings.resources().nativeExtents().encode()))throw new IOException("Original native extent policy changed");
        return phase;
    }
    private boolean clockOwnerReady() {
        synchronized (this) { if (closeRequested || closed || serverClosed) return false; }
        var id=RaftGroupId.valueOf(plan.catalogue().groupId()); var machine=machines.get(id);
        if (machine==null) return false;
        try {
            var division=server.getDivision(id);
            synchronized (division) {
                var info=division.getInfo();
                var applied=machine.getLastAppliedTermIndex();
                return info.isLeader() && info.isLeaderReady() && applied.getIndex()>=0 && applied.getTerm()==info.getCurrentTerm();
            }
        } catch (IOException unavailable) { return false; }
    }
    public synchronized State state() { return closed ? State.CLOSED : closeRequested ? State.STOPPING : starting ? State.STARTING : started ? State.RUNNING : State.OPEN; }
    public synchronized boolean started() { return state()==State.RUNNING; }
    /** Local native/Raft evidence only; this does not establish remote quorum availability. */
    public RatisNodeReadiness.Report readiness() {
        var sources=new ArrayList<RatisNodeReadiness.GroupSource>();
        for (var entry:groups.entries()) {
            var id=entry.group().getGroupId();var machine=machines.get(id);var store=stores.get(id.getUuid());
            if (machine==null || store==null) throw new IllegalStateException("Registered readiness owner is missing");
            var local=entry.group().getPeer(RaftPeerId.valueOf(plan.peerId()));
            boolean serving=!store.topologyRecoveryOnly() && !entry.retired() && local!=null && local.getStartupRole()!=RaftPeerRole.LISTENER;
            sources.add(new RatisNodeReadiness.GroupSource(entry.group(),entry.descriptor(),entry.directory()==null ? null : entry.directory().domainId(),
                    entry.directory()==null ? plan.catalogue().schemaDigest() : entry.directory().schemaDigest(),entry.role(),RaftPeerId.valueOf(plan.peerId()),serving,store,machine,
                    () -> server==null ? null : server.getDivision(id),machine::readinessStamp));
        }
        return RatisNodeReadiness.evaluate(plan.nodeId(),plan.nodeIncarnation(),this::readinessStamp,sources);
    }
    private synchronized RatisNodeReadiness.NodeStamp readinessStamp() {
        var registry=participantRecoveries;
        var recovery=registry==null ? new RatisNodeReadiness.Recovery(groups.dataGroups().isEmpty(),0,0,participantRecoveryFailure())
                : recoveryStamp(registry.recoveryStatus());
        Throwable failure=maintenanceFailure();if(failure==null)failure=retirementFailure();if(failure==null)failure=transactionExpiryFailure();if(failure==null && poolPressure!=null)failure=poolPressure.failure();
        return new RatisNodeReadiness.NodeStamp(state(),installing,groups.entries(),recovery,failure);
    }
    private RatisNodeReadiness.Recovery recoveryStamp(RatisParticipantRecoveryRegistry.RecoveryStatus status) {
        return new RatisNodeReadiness.Recovery(status.scanned(),status.unresolvedDecisions(),status.activeWorkers(),participantRecoveryFailure());
    }
    /** Trusted embedding API. Policy is fixed for this node owner; the returned controller is borrowed. */
    public synchronized RatisNodeMaintenance localMaintenance(RatisNodeMaintenance.Policy policy) throws IOException {
        if(!started || closeRequested || closing || closed)throw new IOException("Local maintenance node is unavailable");
        Objects.requireNonNull(policy);
        if(localMaintenance==null){localMaintenance=new RatisNodeMaintenance(plan.nodeId(),plan.nodeIncarnation(),pools.values().stream().map(owner->owner.pool).toList(),scheduler,policy);localMaintenancePolicy=policy;}
        else if(!localMaintenancePolicy.equals(policy))throw new IOException("Local maintenance policy is already fixed");
        return localMaintenance;
    }
    public synchronized RatisNodePhysicalApi physicalApi(RatisNodeMaintenance.Policy policy)throws IOException {
        var maintenance=localMaintenance(policy);if(physicalApi==null){var selected=new LinkedHashMap<UUID,DistributedStoragePool>();pools.forEach((id,owner)->selected.put(id,owner.pool));physicalApi=new RatisNodePhysicalApi(maintenance,selected,policy);}return physicalApi;
    }
    public RatisNodeMaintenance.Target localMaintenanceTarget(UUID poolId,UUID column)throws IOException {
        final RatisNodeMaintenance maintenance;final DistributedStoragePool pool;
        synchronized(this){if(!started || closeRequested || closing || closed || localMaintenance==null)throw new IOException("Local maintenance unavailable");var owner=pools.get(poolId);if(owner==null)throw new IOException("Unknown local pool");maintenance=localMaintenance;pool=owner.pool;}
        try{return maintenance.target(pool,column);}catch(org.rocksdb.RocksDBException failure){throw new IOException("Native physical target unavailable",failure);}
    }
    public RatisSessionClockService.Snapshot maintenanceStatus() { return service==null ? null : service.latest(); }
    public Throwable maintenanceFailure() { return service==null ? null : service.lastFailure(); }
    public Throwable retirementFailure() { if (retirementCoordinator!=null && retirementCoordinator.lastFailure()!=null) return retirementCoordinator.lastFailure(); return retirementService==null ? null : retirementService.lastFailure(); }
    public int pendingRetirements() throws IOException { return retirementCoordinator==null ? 0 : retirementCoordinator.stats().count(); }
    public Throwable participantRecoveryFailure() { return participantRecoveryService==null ? null : participantRecoveryService.lastFailure(); }
    /** Borrowed synchronous helper; callers invoke it within their existing CONTROL quantum. */
    public RatisSnapshotReadinessHelper.Recovery participantRecovery() { if (participantRecoveries==null) throw new IllegalStateException("Participant recovery is not configured"); return this::recoverParticipant; }
    private boolean recoverParticipant(UUID group,it.cavallium.rockserver.core.common.distributed.PreparedParticipant prepared) throws IOException {
        synchronized (this) { if (closeRequested || closed) throw new IOException("Node participant recovery is stopping"); }
        var registry=participantRecoveries; if (registry==null) throw new IOException("Node participant recovery is not initialized");
        return registry.step(group,prepared);
    }
    public RatisParticipantRecoveryRegistry.Snapshot participantRecoveryStatus() { if (participantRecoveries==null) throw new IllegalStateException("Participant recovery is not configured"); return participantRecoveries.stats(); }
    public Throwable transactionExpiryFailure() { return transactionExpiry==null ? null : transactionExpiry.lastFailure(); }
    private void verify(RaftGroup group) throws IOException {
        var division=server.getDivision(group.getGroupId()); var configuration=division.getRaftConf();var registration=groups.require(group.getGroupId().getUuid());
        boolean joint=false;
        if(registration.relocation()!=null){ReplicatedRangeStore.Configuration cfg;try{cfg=stores.get(group.getGroupId().getUuid()).configuration();}catch(org.rocksdb.RocksDBException failure){throw new IOException("Relocation native configuration unavailable",failure);}if(cfg==null)throw new IOException("Relocation has no native configuration");joint=registration.relocation().requireNativeConfiguration(cfg.data());RatisRangeStateMachine.requireActualRelocationMembership(configuration,registration.relocation(),joint);if(!joint)group=groups.group(RatisTopologyRelocationRecord.sameConfiguration(cfg.data(),registration.relocation().targetConfiguration())?registration.relocation().definition().targets().getFirst():registration.relocation().definition().parents().getFirst());}
        if(!joint)for (var role:List.of(RaftPeerRole.FOLLOWER,RaftPeerRole.LISTENER)) {
            if (!configuration.getPreviousPeers(role).isEmpty()) throw new IOException("Joint membership lacks a forced relocation intent");
            var expected=new TreeMap<String,String>(); var actual=new TreeMap<String,String>();
            for (var peer:group.getPeers()) if (peer.getStartupRole()==role) expected.put(peer.getId().toString(),peer.getAddress()+"|"+Objects.toString(peer.getDataStreamAddress(),""));
            for (var peer:configuration.getCurrentPeers(role)) actual.put(peer.getId().toString(),peer.getAddress()+"|"+Objects.toString(peer.getDataStreamAddress(),""));
            if (!actual.equals(expected)) throw new IOException("Recovered group membership differs from static placement: "+group.getGroupId());
        }
        if (division.getStateMachine()!=machines.get(group.getGroupId()) || machines.get(group.getGroupId()).getLifeCycleState()!=org.apache.ratis.util.LifeCycle.State.RUNNING) throw new IOException("Recovered state-machine owner is not the initialized registered group");
        var entry=groups.require(group.getGroupId().getUuid());var nativeStore=stores.get(group.getGroupId().getUuid());
        if(nativeStore==null || !nativeStore.ownership().owner().groupId().equals(group.getGroupId().getUuid()) || entry.descriptor()!=null && nativeStore.ownership().owner().epoch()!=entry.descriptor().rangeEpoch())throw new IOException("Recovered division native owner differs from registration");
    }
    @Override public void close() throws IOException {
        synchronized (this) {
            if (closed) return;
            if (installing || starting || closing) throw new IOException("Node lifecycle operation is active"); closeRequested=true; closing=true;
        }
        try {
            if(localMaintenance!=null) {
                try{localMaintenance.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);}
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Local maintenance drain interrupted",interrupted);}
                catch(ExecutionException | TimeoutException failure){throw new IOException("Local maintenance work did not drain",failure);}
            }
            if(topologyImageQueries!=null){try{topologyImageQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Topology image drain interrupted",interrupted);}catch(ExecutionException|TimeoutException failure){throw new IOException("Topology image owners did not drain",failure);}}
            for(var owner:pools.values()){if(owner.sources!=null){try{owner.sources.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Source transfer drain interrupted",interrupted);}catch(ExecutionException|TimeoutException failed){throw new IOException("Actual source transfer owners did not drain",failed);}}for(var entry:owner.sourceCleanup.entrySet()){try{entry.getValue().close();owner.sourceCleanup.remove(entry.getKey(),entry.getValue());}catch(Exception failed){throw new IOException("Actual source construction owner did not drain",failed);}}if(owner.artifacts!=null)owner.artifacts.close();}
            if(importPreflightQueries!=null){try{importPreflightQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Import preflight drain interrupted",interrupted);}catch(ExecutionException|TimeoutException failure){throw new IOException("Import preflight owners did not drain",failure);}}
            // Ordinary proofs can await both catalogue authority and local replica preparation.
            if(operationRerouteQueries!=null) {
                try { operationRerouteQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch(InterruptedException interrupted) { Thread.currentThread().interrupt();throw new IOException("Reroute preparation drain interrupted",interrupted); }
                catch(ExecutionException | TimeoutException failure) { throw new IOException("Reroute preparation did not drain",failure); }
            }
            if(sourceOutcomeQueries!=null){
                try{sourceOutcomeQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);}
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Source outcome proof drain interrupted",interrupted);}
                catch(ExecutionException|TimeoutException failure){throw new IOException("Source outcome proof did not drain",failure);}
            }
            if (proofQueries!=null) {
                try { proofQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Proof query drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Proof queries did not drain",failure); }
            }
            // Import preparation can await owner reads which themselves need catalogue authority.
            if (importPublicationQueries!=null) {
                try { importPublicationQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Import publication drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Import publication queries did not drain",failure); }
            }
            if(relocationOwnerQueries!=null){try{relocationOwnerQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Relocation owner drain interrupted",interrupted);}catch(ExecutionException|TimeoutException failure){throw new IOException("Relocation owner lane did not drain",failure);}}
            if(relocationClients!=null)relocationClients.close();
            for(var owner:pools.values())if (owner.schemas!=null) {
                try { owner.schemas.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Schema replica drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Schema replica service did not drain",failure); }
            }
            if (authorityQueries!=null) {
                try { authorityQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Schema authority drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Schema authority queries did not drain",failure); }
            }
            if (exportSources!=null) {
                try { exportSources.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Export source drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Export sources did not drain",failure); }
            }
            for(var owner:pools.values())if(owner.cohorts!=null)try{owner.cohorts.close();}catch(Exception failure){throw new IOException("Cohort owners did not drain",failure);}
            if (retirementService!=null) {
                try { retirementService.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Retirement drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Retirement did not drain",failure); }
            }
            if (retirementCoordinator!=null) retirementCoordinator.close();
            if (participantRecoveryService!=null) {
                try { participantRecoveryService.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Participant recovery drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Participant recovery did not drain",failure); }
            }
            if (snapshotHelp!=null) {
                try { snapshotHelp.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Snapshot HELP drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Snapshot HELP did not drain",failure); }
            }
            if (participantRecoveries!=null) participantRecoveries.close();
            if(poolPressure!=null) {
                try{poolPressure.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);}
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Pool pressure sampler drain interrupted",interrupted);}
                catch(ExecutionException | TimeoutException failure){throw new IOException("Pool pressure sampler did not drain",failure);}
            }
            if (transactionExpiry!=null) {
                try { transactionExpiry.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Transaction expiry drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Transaction expiry did not drain",failure); }
            }
            if (service!=null) {
                try { service.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Node service drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Node service did not drain",failure); }
            }
            for (var scope:propagations) scope.close(); if (clock!=null) clock.close();
            for (var machine:machines.values()) machine.beginShutdown();
            committedDispatcher.close();
            if (server!=null && !serverClosed) { server.close(); serverClosed=true; }
            for(var budget:logGroups.values())budget.close();if(logRegistration!=null){logRegistration.close();logRegistration=null;}
            for (var machine:machines.values()) machine.awaitNativeReaders(settings.shutdownTimeout());
            committedDispatcher.awaitDrained(settings.shutdownTimeout());
            diskRunway.awaitActualRecovery(settings.shutdownTimeout());
            for (var files:importFiles.values()) files.close();
            if (importFileIo!=null) {
                importFileIo.shutdown();
                try { if (!importFileIo.awaitTermination(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS)) throw new IOException("Import file executor did not drain"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Import file drain interrupted",interrupted); }
            }
            if (schemaFencingReplicas!=null && !schemaFencingReplicasClosed) { schemaFencingReplicas.close(); schemaFencingReplicasClosed=true; }
            if(topologyOwners!=null && !topologyOwnersClosed){topologyOwners.close();topologyOwnersClosed=true;}
            if (importOwners!=null && !importOwnersClosed) { importOwners.close(); importOwnersClosed=true; }
            for(var owner:pools.values())if(!owner.closed){owner.pool.close();owner.closed=true;}poolClosed=true;
            for (var client:homeClients.values()) client.close();
            if(nativeExtentEnv!=null){nativeExtentEnv.close();nativeExtentEnv=null;}
            if (lock!=null && lock.isValid()) lock.release(); if (lockChannel!=null) lockChannel.close();
            synchronized (this) { closed=true; }
        } finally { synchronized (this) { closing=false; } }
    }
    private static RaftServer buildServer(RatisNodePlan plan,Path logs,Map<RaftGroupId,RatisRangeStateMachine> machines) throws IOException {
        RaftProperties properties=RatisDurability.properties(logs);GrpcConfigKeys.setMessageSizeMax(properties,org.apache.ratis.util.SizeInBytes.valueOf(RatisTopologySourceSnapshotRpc.MAX_REPLY_BYTES+4096L)); org.apache.ratis.RaftConfigKeys.Rpc.setType(properties,org.apache.ratis.rpc.SupportedRpcType.GRPC);
        String address=plan.address(); int colon=address.lastIndexOf(':'); String host=address.substring(0,colon);
        if (host.startsWith("[")) host=host.substring(1,host.length()-1);
        GrpcConfigKeys.Server.setHost(properties,host); GrpcConfigKeys.Server.setPort(properties,Integer.parseInt(address.substring(colon+1)));
        GrpcConfigKeys.Server.setBossGroupSize(properties,1); GrpcConfigKeys.Server.setWorkerGroupSize(properties,2);
        GrpcConfigKeys.Server.setAsyncRequestThreadPoolCached(properties,false); GrpcConfigKeys.Server.setAsyncRequestThreadPoolSize(properties,4);
        RatisStreamingTransport.server(plan,properties);
        return org.apache.ratis.server.impl.RockserverRaftServers.create(RaftPeerId.valueOf(plan.peerId()),plan.localGroups().getFirst(),
                RaftStorage.StartupOption.RECOVER,id -> {
                    var machine=machines.get(id); if (machine==null) throw new IllegalArgumentException("Unplanned state-machine group"); return machine;
                },properties,RatisSnapshotAckInterceptor.parameters((id,term,index) -> {
                    var machine=machines.get(id); if (machine==null) throw new IOException("Unplanned snapshot group"); machine.syncReceivedSnapshot(term,index);
                }));
    }
}
