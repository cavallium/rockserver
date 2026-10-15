package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeRecoverySupersession;

import it.cavallium.rockserver.core.common.distributed.SnapshotFence;

import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import it.cavallium.rockserver.core.common.distributed.DistributedSchemaCodec;
import it.cavallium.rockserver.core.common.distributed.NativeTopologySeed;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyRecoveryFence;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerRegistration;
import it.cavallium.rockserver.core.common.distributed.NativeCurrentRangeRecovery;
import it.cavallium.rockserver.core.common.distributed.NativeMetadataRecoveryRegistration;
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
    public enum Startup { RECOVER, AUTHORIZED_GENESIS, AUTHORIZED_INITIAL_PLACEMENT, AUTHORIZED_RESTORE_TARGET }
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
    private record RecoveryAttempt(UUID pool,UUID group,byte[] preparedDigest,NativeCurrentRangeRecovery current,NativeMetadataRecoveryRegistration metadata,int previousBytes) {}
    private RecoveryAttempt recoveryAttempt;
    private record SupersessionAttempt(UUID pool,NativeRecoverySupersession.Scope scope) {}
    private SupersessionAttempt supersessionAttempt;
    private final Map<UUID,byte[]> recoveryCompletionAttempts=new HashMap<>();
    private volatile Map<UUID,RatisLocalNoneDiscovery> localNoneDiscoveries=Map.of();
    private RatisNodeResourceBudget.Lease localNoneCredit;
    private RatisConfiguredRecoveries configuredRecoveries;private RatisConfiguredRecoveryGraph recoveryGraph;private RatisNodeResourceBudget.Lease recoveryGraphCredit;private RatisTransactionRecoveryService configuredRecoveryService;private boolean configuredRecoveryServiceStarted,configuredDiscoveryNext=true;private CompletableFuture<Void> configuredRecoveryServiceStart;
    synchronized void relocationTransport(RatisClientPool.Factory leaders,RatisClientPool.OwnerFactory owners)throws IOException{
        if(started || starting || installing || closeRequested || closing || closed)throw new IllegalStateException("Relocation transport fixed before start");var replacement=new RatisRelocationDataClient(plan.clusterId(),plan.databaseId(),plan.catalogue().groupId(),leaders,owners);try{relocationClients.close();}catch(IOException|RuntimeException failed){try{replacement.close();}catch(IOException cleanup){failed.addSuppressed(cleanup);}throw failed;}relocationClients=replacement;
    }
    public synchronized void learnerSource(LearnerSourceReader reader){if(started || starting || installing || closeRequested)throw new IllegalStateException("Learner source policy is fixed before start");learnerSource=Objects.requireNonNull(reader);}

    private record SeedAttempt(TopologyChange.Definition definition,TopologyChange.State authority,ReplicaDescriptor local,ApplyPosition catalogueCut) {}
    private SeedAttempt seedAttempt;
    private RatisTopologyOwnerClient topologyOwners;private boolean topologyOwnersClosed;
    private long groupManagementCall;
    private final Set<UUID> virginSeedAdds=new HashSet<>();
    private static final int NODE_IDENTITY_BYTES=RatisNodeIdentity.MAX_BYTES;
    private RatisNodeIdentity nodeIdentity;
    private RatisJournalAccess identityJournal=RatisJournalAccess.standalone();
    private boolean genesisIdentityForcePending;
    private org.rocksdb.NativeExtentEnv nativeExtentEnv;private boolean nativeExtentBound;
    private final Map<UUID,PoolOwner> pools=new LinkedHashMap<>();private final Map<UUID,UUID> groupPools=new ConcurrentHashMap<>(); private RaftServer server; private RatisCatalogueClock clock;
    static RatisNodeResourceBudget.Limits poolResources(RatisNodeResourceBudget.Limits spec,RatisNodeResourceBudget.Limits ceiling){return new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(Math.min(spec.ordinary().bytes(),ceiling.ordinary().bytes()),Math.min(spec.ordinary().owners(),ceiling.ordinary().owners())),new RatisNodeResourceBudget.Lane(Math.min(spec.protectedLane().bytes(),ceiling.protectedLane().bytes()),Math.min(spec.protectedLane().owners(),ceiling.protectedLane().owners())));}
    private RatisMetadataClaims metadataClaims;
    private final class PoolOwner{final RatisNativePoolSpec spec;final DistributedStoragePool pool;RatisSchemaReplicaService schemas;RatisSnapshotCohortService cohorts;RatisTopologySourceSnapshotArtifacts artifacts;RatisTopologyRecoveryAnchors anchors;RatisTopologySourceSnapshotService sources;final java.util.concurrent.ConcurrentHashMap<UUID,AutoCloseable> sourceCleanup=new java.util.concurrent.ConcurrentHashMap<>();boolean closed;RatisNodeResourceBudget.Lease recoveryMetadata;long recoveryEncodedBytes,recoveryExposureBytes;PoolOwner(RatisNativePoolSpec spec,DistributedStoragePool pool){this.spec=spec;this.pool=pool;}NativeRangeImage.Limits images(){var value=spec.images();return new NativeRangeImage.Limits(Math.min(value.maxRowBytes(),settings.images().maxRowBytes()),Math.min(value.maxImageBytes(),settings.images().maxImageBytes()),Math.min(value.maxColumns(),settings.images().maxColumns()));}RatisRangeStateMachine.RetentionLimits retention(){var value=spec.retention();var node=settings.retention();return new RatisRangeStateMachine.RetentionLimits(Math.min(value.dataCount(),node.dataCount()),Math.min(value.dataBytes(),node.dataBytes()),Math.min(value.maintenanceCount(),node.maintenanceCount()),Math.min(value.maintenanceBytes(),node.maintenanceBytes()));}PoolSnapshotAdmission.Limits pins(){var value=spec.pins();var node=settings.exports().pins();return new PoolSnapshotAdmission.Limits(Math.min(value.maximumPins(),node.maximumPins()),Math.min(value.maximumEstimatedBytes(),node.maximumEstimatedBytes()),value.maximumAge().compareTo(node.maximumAge())<0?value.maximumAge():node.maximumAge(),Math.max(value.minimumFreeBytes(),node.minimumFreeBytes()));}}
    public UUID poolId(UUID group){return owner(group).spec.id();}DistributedStoragePool nativePool(UUID poolId){var owner=pools.get(poolId);if(owner==null)throw new IllegalArgumentException("Undeclared pool");return owner.pool;}
    private PoolOwner owner(UUID group){var id=groupPools.get(group);var owner=id==null?null:pools.get(id);if(owner==null)throw new IllegalArgumentException("Group has no opened native pool: "+group);return owner;}
    private void requirePoolCapacity(UUID pool,Collection<UUID> requested){requireGroupCapacity(requested);var proposed=new HashSet<UUID>();groupPools.forEach((group,id)->{if(id.equals(pool))proposed.add(group);});proposed.addAll(requested);if(seedAttempt!=null && pool.equals(groupPools.get(seedAttempt.definition().parents().getFirst().groupId())))seedAttempt.definition().targets().forEach(target->proposed.add(target.groupId()));if(learnerAttempt!=null && pool.equals(learnerAttemptPool))proposed.add(learnerAttempt.definition().targets().getFirst().groupId());if(recoveryAttempt!=null && recoveryAttempt.pool().equals(pool))proposed.add(recoveryAttempt.group());if(proposed.size()>Math.min(plan.pools().get(pool).maximumGroups(),settings.resources().committedApply().pool().groups()))throw new IllegalArgumentException("Physical pool group ceiling exhausted");}
    private void requireGroupCapacity(Collection<UUID> requested){
        groups.requireCapacity(requested);var proposed=new HashSet<>(groupPools.keySet());proposed.addAll(requested);if(seedAttempt!=null)seedAttempt.definition().targets().forEach(target->proposed.add(target.groupId()));if(learnerAttempt!=null)proposed.add(learnerAttempt.definition().targets().getFirst().groupId());
        if(recoveryAttempt!=null)proposed.add(recoveryAttempt.group());if(proposed.size()>Math.min(plan.maxLocalGroups(),settings.resources().committedApply().node().groups()))throw new IllegalArgumentException("Committed node group ceiling exhausted before native ownership");
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
        for(var owner:pools.values())scopes.add(new RatisDiskRunwayService.Scope(owner.spec.id(),null,root.resolve("configured-recovery").resolve(owner.spec.id().toString())));
        if(metadataClaims!=null)scopes.add(new RatisDiskRunwayService.Scope(metadataClaims.graphPool(),null,root.resolve("configured-recovery-graph")));
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
        owner.artifacts=new RatisTopologySourceSnapshotArtifacts(Files.createDirectories(root.resolve("topology-source-artifacts").resolve(owner.spec.id().toString())),owner.images(),imageCeiling,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES,4,resources.imageMetadataAdmission(owner.spec.id(),NativeRangeImage.IoAdmission.UNLIMITED));owner.anchors=new RatisTopologyRecoveryAnchors(owner.artifacts,owner.images());owner.sources=new RatisTopologySourceSnapshotService(sourceSnapshotHost(owner),new RatisTopologySourceSnapshotService.Limits(4,owner.images().maxImageBytes(),imageCeiling/2,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES),scheduler,(request,bytes,control)->{if(control){var lease=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,bytes,1);return lease::close;}if(request.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.BOOTSTRAP)return migration.reserveRecovery(owner.spec.id(),bytes);var lease=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.ORDINARY,bytes,1);return lease::close;});
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
        if(restoreInitialization!=null && startup!=Startup.AUTHORIZED_RESTORE_TARGET)throw new IOException("Restore staging requires exact authorized initialization");
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
    private static final List<String> NODE_DIRECTORIES=List.of("raft","sessions","recovery","schema-replicas","referenced-files","topology-exposure","topology-source-artifacts","retirement","topology-relocations","configured-recovery","configured-recovery-graph");
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
    private void requireRecoveryPool(PoolOwner owner,RangeDirectory directory)throws IOException {
        if(!directory.clusterId().equals(plan.clusterId()) || !directory.databaseId().equals(plan.databaseId()) || !owner.spec.id().equals(plan.domainPools().get(directory.domainId())))throw new IOException("Native recovery namespace/domain pool differs from configured placement");
        if(directory.columns().size()>owner.images().maxColumns())throw new IOException("Native recovery schema exceeds configured column bound");
    }
    private void requireRecoveryMetadataCapacity()throws IOException {
        long total=metadataClaims.bytes();for(var owner:pools.values()){
            total=Math.addExact(total,owner.recoveryExposureBytes);
            if(Math.addExact(metadataClaims.bytes(owner.spec.id()),owner.recoveryExposureBytes)>owner.spec.maximumDurableMetadataBytes())throw new IOException("Native recovery exposure exceeds physical pool durable metadata ceiling");
        }
        if(total>settings.resources().maximumDurableMetadataBytes())throw new IOException("Native recovery exposure exceeds node durable metadata ceiling");
    }
    private void initializeNativeExtentEnv(Path identity,NativeExtentLayout layout)throws IOException {
        Path journal=root.resolve("native-extents.journal").toAbsolutePath().normalize();
        if(nativeExtentBound && !Files.isRegularFile(journal,LinkOption.NOFOLLOW_LINKS))throw new IOException("Original native extent journal is missing");
        for(var path:layout.directories()){if(nativeExtentBound && !Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Established native extent directory is missing: "+path);createAndForceNativeDirectory(path);}
        SessionJournalFiles.force(root);
        try(var memoryOwner=new org.rocksdb.NativeRawFlushLimits(settings.resources().nativeExtents().raw())){nativeExtentEnv=new org.rocksdb.NativeExtentEnv(journal.toString(),layout.roots().stream().map(Path::toString).toList(),settings.resources().nativeExtents().limits(),memoryOwner);}
        catch(org.rocksdb.RocksDBException failure){throw new IOException("Cannot bind original shared native extent environment",failure);}
        if(!nativeExtentBound){nativeExtentBound=true;identityJournal.write(identity,identity(false),NODE_IDENTITY_BYTES);}
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
            nodeIdentity=readIdentity(SessionJournalFiles.read(identity,NODE_IDENTITY_BYTES));int phase=nodeIdentity.phase();nativeExtentBound=phase>=1;boolean exposed=phase==2;establishedPlacement=exposed;
            if(startup!=Startup.RECOVER && nodeIdentity.purpose()!=initializationPurpose(startup))throw new IOException("Original node initialization purpose changed");
            if (!exposed && startup==Startup.RECOVER) throw new IOException("Interrupted initial placement requires explicit authorization to resume");
            if (exposed) {
                for(var spec:plan.pools().values())if(!Files.isRegularFile(spec.nativePath(root).resolve("CURRENT")))throw new IOException("Established native pool state is missing; refuse empty-voter recovery: "+spec.id());
            }
        } else {
            if (startup==Startup.RECOVER) throw new IOException("Node identity is absent; explicit placement initialization is required");
            if(Files.exists(root.resolve("exports"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Unidentified source export authority must not be adopted");
            for(var name:NODE_FILES)for(var suffix:List.of("",".pending",".lock")){if(name.equals("node.lock") && suffix.isEmpty())continue;if(Files.exists(root.resolve(name+suffix),LinkOption.NOFOLLOW_LINKS))throw new IOException("Unidentified node authority must not be adopted: "+name+suffix);}
            if(Files.exists(root.resolve("metadata-claims"),LinkOption.NOFOLLOW_LINKS) || Files.exists(root.resolve("metadata-claims.pending"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Unidentified metadata claims must not be adopted");
            for (String directory:java.util.stream.Stream.concat(java.util.stream.Stream.of("native"),NODE_DIRECTORIES.stream()).toList()) {
                Path path=root.resolve(directory);
                if(directory.equals("native")){requireInitialPoolStaging(path);continue;}
                if (Files.exists(path)) try (var entries=Files.list(path)) { if (entries.findAny().isPresent()) throw new IOException("Unidentified node data must not be adopted"); }
            }
            nodeIdentity=RatisNodeIdentity.create(plan,settings.resources().nativeExtents(),initializationPurpose(startup));
            identityJournal.write(identity,identity(false),NODE_IDENTITY_BYTES);
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
        // Native registries are bounded and decoded by pool.open. Acquire the node's retained graph
        // ownership before constructing any additional runtime group, peer or alias projections.
        for(var owner:pools.values()){
            var current=owner.pool.currentRangeRecoveries();var metadata=owner.pool.metadataRecoveries();var supersessions=owner.pool.recoverySupersessions();long encoded=0,registry=16;
            for(var seed:owner.pool.topologySeeds())registry=Math.addExact(registry,4L+seed.encodedSize());
            for(var registration:metadata){encoded=Math.addExact(encoded,registration.encodedSize());registry=Math.addExact(registry,260L+65620L+registration.encodedSize());}
            for(var registration:current){encoded=Math.addExact(encoded,registration.encodedSize());registry=Math.addExact(registry,260L+65620+registration.encodedSize());}
            for(var value:supersessions){encoded=Math.addExact(encoded,value.encodedSize());registry=Math.addExact(registry,value.encodedSize());}
            // POOL_BYTES already reserves the native shared 64 MiB registry, including per-group
            // registration/digest/image/completion rows. Count that promise once, not once per DTO.
            if(registry>64L*1024*1024)throw new IOException("Native recovery registry exceeds covered metadata reservation");
            if(encoded!=0)owner.recoveryMetadata=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,4096L+16L*encoded,1);owner.recoveryEncodedBytes=encoded;
            var exposedOwners=new HashSet<UUID>();for(var value:current)exposedOwners.add(value.groupId());for(var value:metadata)exposedOwners.add(value.groupId());for(var value:supersessions)exposedOwners.add(value.groupId());owner.recoveryExposureBytes=Math.multiplyExact(224L,exposedOwners.size()); // 80-byte identity + checksum; main and pending.
            for(var registration:metadata){requireRecoveryPool(owner,registration.sourceDirectory());assignPool(registration.groupId(),owner.spec.id());groups.register(registration);}
        }
        for(var owner:pools.values())for(var registration:owner.pool.currentRangeRecoveries()){
            requireRecoveryPool(owner,registration.directory());assignPool(registration.groupId(),owner.spec.id());groups.register(registration);
        }
        for(var owner:pools.values())for(var value:owner.pool.recoverySupersessions()){
            requireRecoveryPool(owner,value.sourceDirectory());assignPool(value.groupId(),owner.spec.id());groups.register(value);
        }
        discovered.sort(Comparator.comparingLong(seed->seed.definition().sourceDirectory().generation()));for(var seed:discovered)groups.register(seed);for(var owner:pools.values())for(var learner:owner.pool.topologyLearners())groups.register(learner);
        for(var entry:groups.entries())if(entry.current()!=null && !entry.retired())requireIncomingOwnership(entry.directory(),List.of(entry.descriptor()),Set.of());
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
            metadataClaims.reserve(retainedClaims);requireRecoveryMetadataCapacity();if(!unqualified.isEmpty())throw new IOException("Unqualified unexposed native owners: "+unqualified);
        }catch(org.rocksdb.RocksDBException failure){throw new IOException("Cannot reconstruct retained metadata claims",failure);}
        loadRelocationIntents();
        if(establishedPlacement)for(var entry:groups.entries())if(entry.seed()==null && entry.learner()==null && entry.current()==null && entry.metadata()==null && entry.supersession()==null
                && !Files.isRegularFile(logs.resolve(entry.group().getGroupId().getUuid().toString()).resolve("current/raft-meta")))throw new IOException("Established Raft state is missing for "+entry.group().getGroupId());
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
                    var registration=groups.require(id);var directory=registration.directory();var descriptor=registration.descriptor();var home=role==RatisNodePlan.Role.DATA ? null : plan.home(id);
                    var recipe=registration.retired() ? groups.retirement(id) : registration.seed();
                    var bindings=registration.metadata()!=null || registration.supersession()!=null ? Map.<UUID,String>of() : registration.current()!=null ? registration.current().physicalNames() : registration.learner()!=null ? registration.learner().physicalNames() : recipe!=null ? recipe.physicalNames() : home==null ? plan.domain(directory.domainId()).columns() : Map.of(home.column(),home.physicalName());
                    var importDefinition=registration.seed()==null && registration.learner()==null && registration.current()==null && registration.metadata()==null && registration.supersession()==null ? plan.importDefinition(id) : null;
                    if(importDefinition!=null){var manifest=importDefinition.manifest(id);RatisRangeImportRecord.requirePolicy(manifest);if(manifest.columns().size()>owner.images().maxColumns() || (long)manifest.encode().length+17>owner.images().maxRowBytes() || (long)manifest.encode().length+17>owner.images().maxImageBytes())throw new IOException("Imported manifest exceeds configured native image column or byte budget");}
                    var superseded=registration.supersession()==null?null:pool.forceRecoverySupersession(registration.supersession().scope());
                    var store=registration.metadata()!=null ? pool.openMetadataRecovery(registration.metadata()) : superseded!=null ? superseded : registration.current()!=null && !registration.retired() ? pool.openCurrentRangeRecovery(registration.current()) : registration.learner()!=null ? pool.openTopologyLearner(registration.learner()) : registration.retired() ? pool.openRetiredTopologyRange(id) : registration.seed()!=null ? pool.reopenTopologyRange(registration.seed().birth(id),seedSchema(registration.seed())) : importDefinition==null ? pool.openRange(new RangeOwnership.Owner(id,descriptor.rangeEpoch()),new RangeOwnership.Interval(directory.domainId(),directory.columns(),descriptor.lowerInclusive(),descriptor.upperExclusive()),
                            directory.schemaDigest(),bindings,recipe!=null ? recipe.mergeColumns() : home==null ? plan.domain(directory.domainId()).merges() : Set.of())
                            : startup==Startup.RECOVER || establishedPlacement ? pool.reopenImportRange(importDefinition.manifest(id),logicalSchemas.get(directory.domainId()))
                            : pool.openNewFencedRange(importDefinition.manifest(id),logicalSchemas.get(directory.domainId()));
                    if(registration.relocation()!=null){var configuration=store.configuration();var cut=store.applied();if(configuration==null || cut==null || configuration.term()>cut.term() || configuration.index()>cut.index() || configuration.index()==cut.index() && configuration.term()!=cut.term())throw new IOException("Relocation cold configuration lacks completed native prefix");group=groups.reconcileRelocationConfiguration(id,configuration.data());}
                    stores.put(id,store);
                    java.util.function.Consumer<it.cavallium.rockserver.core.common.distributed.DistributedCommand> validator=command -> {};
                    if (role==RatisNodePlan.Role.TRANSACTION_HOME) validator=RatisTransactionStatuses.validator(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),directory.schemaDigest(),home.column());
                    else if (role==RatisNodePlan.Role.SNAPSHOT_HOME) validator=RatisSnapshotFences.validator(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),directory.schemaDigest(),home.column());
                    if (role==RatisNodePlan.Role.DATA && registration.learner()==null && !registration.retired() && (logicalSchemas.containsKey(directory.domainId()) || registration.seed()!=null || registration.current()!=null)) {
                        var effectiveSchema=store.installedSchema();
                        store.enableLogicalSchema(effectiveSchema==null ? logicalSchemas.get(directory.domainId()) : effectiveSchema);
                    }
                    var effectiveDirectory=role==RatisNodePlan.Role.DATA && store.schemaInstallation()!=null
                            ? store.schemaInstallation().definition().targetDirectory()
                            : registration.current()!=null && !registration.retired() ? registration.current().directory() : importDefinition==null ? directory : importDefinition.targetDirectory();
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
                if(groups.require(id).role()==RatisNodePlan.Role.DATA && (nodeIdentity.purpose()==RatisNodeIdentity.Purpose.GENESIS || groups.require(id).supersession()!=null && groups.require(id).metadata()==null))machine.deferRecoveryDiscovery();
                machine.learnerInstaller(topologyImageQueries,this::joinTopologyLearner);machine.learnerRecoveryInstaller(this::prepareLearnerRecovery);machine.proofQueries(proofQueries);
                machine.nodeResources(resources,owner.spec.id());machine.logResources(logBudget(group.getGroupId().getUuid(),owner.spec.id()));machine.diskRunway(diskRunway,owner.spec.id());machine.committedApply(committedDispatcher,owner.spec.id(),MAX_COMMITTED_ENTRY_BYTES);machines.put(group.getGroupId(),machine);
            }
            for (var entry:machines.entrySet()) if (groups.require(entry.getKey().getUuid()).role()==RatisNodePlan.Role.DATA)
                if(groups.require(entry.getKey().getUuid()).metadata()==null && groups.require(entry.getKey().getUuid()).supersession()==null)for (var schema:entry.getValue().schemaRoutingHistory()) observeSchemaActivation(schema.definition(),schema.proof());
            for(var entry:groups.entries())if(entry.current()!=null && !entry.retired()){var store=stores.get(entry.current().groupId());if(store.currentRangeRecoveryReady()){if(!entry.current().equals(store.currentRangeRecoveryRegistration()) || !store.admissionOpen() && !machines.get(RaftGroupId.valueOf(entry.current().groupId())).recoveryEligibilityGate().discoveryPending())throw new IOException("Recovered CURRENT readiness differs from original registration");var routed=data(entry.directory().domainId());if(routed.directory().generation()<=entry.directory().generation())routed.refresh(entry.directory());}}
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
        Path configuredRoot=Files.createDirectories(root.resolve("configured-recovery"));SessionJournalFiles.force(configuredRoot);SessionJournalFiles.force(root);
        var recoveryPools=new LinkedHashMap<UUID,Integer>();for(var owner:pools.values())recoveryPools.put(owner.spec.id(),Math.min(plan.maxLocalGroups(),owner.spec.maximumGroups()));
        configuredRecoveries=new RatisConfiguredRecoveries(configuredRoot,plan.identityDigest(),plan.maxLocalGroups(),recoveryPools,resources,new RatisConfiguredRecoveries.Owner(){
            public UUID pool(RatisTopologySourceSnapshotRpc.Request request)throws IOException{return configuredRecoveryPool(request);}
            public RatisJournalAccess journal(UUID pool,UUID group,Path directory)throws IOException{return configuredRecoveryJournal(pool,group,directory);}
        },this::configuredRecoveryHost);
        configuredRecoveryService=new RatisTransactionRecoveryService(scheduler,this::configuredRecoveryCycle);
        Path graphRoot=Files.createDirectories(root.resolve("configured-recovery-graph"));SessionJournalFiles.force(graphRoot);SessionJournalFiles.force(root);
        diskCensus=new RatisDiskRunwayService(diskRunway,this::diskScopes,scheduler);diskCensus.initialCensus();
        recoveryGraphCredit=resources.reserve(metadataClaims.graphPool(),RatisNodeResourceBudget.Kind.PROTECTED,4096,1);
        recoveryGraph=new RatisConfiguredRecoveryGraph(graphRoot,plan,recoveryGraphJournal(),(retained,incoming)->recoveryGraphCredit.growTo(Math.max(recoveryGraphCredit.bytes(),Math.addExact(4096L,Math.multiplyExact(16L,Math.addExact(retained,incoming))))));shrinkRecoveryGraphCredit();
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
        if (machines.containsKey(RaftGroupId.valueOf(plan.catalogue().groupId()))) {
        clock=new RatisCatalogueClock(root.resolve("sessions/catalogue-clock.bin"),epochs);
        for(var domain:routedClients.keySet().stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList()){Path scope=Files.createDirectories(root.resolve("sessions/data").resolve(domain.toString()));SessionJournalFiles.force(scope);SessionJournalFiles.force(scope.getParent());var propagation=new RatisSessionPropagation(scope,epochs,data(domain),settings.maxPropagationRanges());dataPropagations.put(domain,propagation);propagations.add(propagation);}
        for (var home:plan.homes()) {
            Path scope=Files.createDirectories(root.resolve("sessions").resolve(home.role().name().toLowerCase(Locale.ROOT))); SessionJournalFiles.force(scope); SessionJournalFiles.force(scope.getParent());
            propagations.add(new RatisSessionPropagation(scope,epochs,homeClients.get(home.role()),settings.maxPropagationRanges()));
        }
        }
        for(var owner:pools.values())for(var seed:owner.pool.topologySeeds())for(var target:seed.definition().targets()){var store=stores.get(target.groupId());var activation=store==null ? null : store.topologyActivation();if(activation!=null)observeTopologyActivation(seed.definition(),activation);}
        freezeStartupDirectories();
        if(clock!=null)service=new RatisSessionClockService(scheduler,clock,propagations,time,settings.propagationVisits(),this::clockOwnerReady);
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
        synchronized(this){if(!started || closing || closeRequested || installing || recoveryAttempt!=null || supersessionAttempt!=null)throw new IOException("Learner recovery node owner is unavailable");installing=true;}
        try{
            if(request.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.RECOVERY)throw new IOException("Registered image recovery purpose required");var receiver=request.receiver();if(!receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation()) || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address()))throw new IOException("Recovery receiver changed local node identity");
            var view=relocationSourceView(request);var definition=view.definition();var owner=owner(definition.parents().getFirst().groupId());var pool=owner.pool;var store=stores.get(definition.parents().getFirst().groupId());if(store==null)throw new IOException("No registered local recovery store");var registration=store.topologyLearnerRegistration();if(registration==null || !sameLearnerIdentity(registration.receiver(),receiver))throw new IOException("Recovery lacks original receiver registration");
            var prepared=relocationClients.prepare(request,definition);var names=new HashMap<UUID,String>();for(var local:stores.entrySet())if(owner.spec.id().equals(groupPools.get(local.getKey())))names.putAll(pool.schemaPhysicalNames(local.getValue()));var permission=recoveryPermission(store,view,prepared,names);
            var saved=store.topologyLearnerImagePermission();if(saved!=null && Arrays.equals(saved.encode(),permission.encode()))return prepared;
            var after=relocationSourceView(request);if(!Arrays.equals(after.definition().encode(),definition.encode()) || !RatisRelocationOwnerRpc.covers(after.cut(),view.cut()))throw new IOException("Recovery catalogue witness changed");pool.prepareTopologyLearnerImage(store,permission);return prepared;
        }finally{synchronized(this){installing=false;}}
    }
    static it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerImagePermission recoveryPermission(ReplicatedRangeStore store,RatisTopologyJoinAuthority.View view,RatisTopologySourceSnapshotRpc.Prepared prepared,Map<UUID,String> receiverBindings)throws IOException{
        var request=prepared.request();request.header().requireDefinition(view.definition());if(request.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.RECOVERY || !request.catalogue().equals(view.catalogue()) || view.state().phase()!=TopologyChange.Phase.JOINING)throw new IOException("Recovery image lacks current irreversible catalogue qualification");var registration=store.topologyLearnerRegistration();if(registration==null || !sameLearnerIdentity(registration.receiver(),request.receiver()))throw new IOException("Recovery changed registered receiver identity");
        var definition=view.definition();RatisRangeStateMachine.requireSchemaMembership(definition.parents().getFirst(),prepared.sourceCfg().data());prepared.schema().requireMatches(definition.sourceDirectory());prepared.schema().requireMatches(definition.targetDirectory());var own=new HashMap<UUID,String>();var merges=new HashSet<UUID>();for(var column:prepared.schema().columns()){own.put(column.columnId(),receiverBindings.getOrDefault(column.columnId(),"relocation-"+column.columnId()));if(column.operator()!=null)merges.add(column.columnId());}
        return new it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerImagePermission(registration,definition,view.catalogue(),view.cut(),prepared.image().topologyOrigin(),prepared.schema(),prepared.image().position(),prepared.image().sha256(),prepared.sourceCfg().data(),own,merges,prepared.schemaRows(),prepared.request().transferId());
    }
    private static boolean sameLearnerIdentity(ReplicaDescriptor before,ReplicaDescriptor after){return before.nodeId().equals(after.nodeId()) && before.nodeIncarnation().equals(after.nodeIncarnation()) && before.replicaId().equals(after.replicaId()) && before.peerId().equals(after.peerId()) && before.address().equals(after.address());}
    private LearnerSourceEvidence loadLearnerSource(RatisTopologyJoinAuthority.View view,ReplicaDescriptor receiver)throws IOException{if(view.state().phase()!=TopologyChange.Phase.JOINING)throw new IOException("Production learner bootstrap requires irreversible JOINING");var chosen=sourceRequest(view.definition(),view.catalogue(),receiver);RatisTopologySourceSnapshotRpc.Prepared prepared=null;for(int attempts=0;attempts<RatisTopologySourceSnapshotService.MAX_RELEASED;attempts++){try{prepared=relocationClients.prepare(chosen,view.definition());break;}catch(RatisTopologySourceSnapshotRpc.AbandonedException terminal){chosen=terminal.receipt().successorRequest();}}if(prepared==null)throw new IOException("Source successor publication chain exhausted");var bindings=plan.domain(prepared.schema().domainId());if(!bindings.columns().keySet().equals(prepared.schema().columnIds()))throw new IOException("Receiver has no configured physical bindings for source schema");return new LearnerSourceEvidence(prepared.image(),prepared.schema(),prepared.sourceOwner(),prepared.sourceCfg(),prepared.image().position(),bindings.columns(),bindings.merges(),prepared.schemaRows(),prepared.request().transferId());}
    private RatisTopologyJoinAuthority.View relocationSourceView(RatisTopologySourceSnapshotRpc.Request request)throws IOException {
        if(!request.relocation())throw new IOException("Relocation source purpose required");var query=RatisTopologyJoinAuthority.Request.of(request.header(),request.receiver().replicaId(),request.purpose()==RatisTopologySourceSnapshotRpc.Purpose.BOOTSTRAP?RatisTopologyJoinAuthority.Purpose.REGISTER:RatisTopologyJoinAuthority.Purpose.CONFIGURATION);var view=joinAuthority.read(query);view.require(query);
        if(!view.catalogue().equals(plan.catalogue().groupId()) || !view.catalogue().equals(request.catalogue()) || view.state().phase()!=TopologyChange.Phase.JOINING)throw new IOException("Source image requires exact irreversible JOINING");return view;
    }
    private record SourceAuthority(TopologyChange.Definition relocation,RangeDirectory directory,DistributedSchema schema) {}
    private SourceAuthority sourceView(RatisTopologySourceSnapshotRpc.Request request)throws IOException {return sourceView(request,false);}
    private SourceAuthority sourceView(RatisTopologySourceSnapshotRpc.Request request,boolean continuity)throws IOException {
        if(!request.catalogue().equals(plan.catalogue().groupId()) || !request.clusterId().equals(plan.clusterId()) || !request.databaseId().equals(plan.databaseId()))throw new IOException("Source image authority namespace differs");
        RangeDirectory directory;TopologyChange.Definition relocation=null;
        if(request.relocation()) {
            var view=relocationSourceView(request);relocation=view.definition();directory=relocation.sourceDirectory();
        }else if(request.metadataOnly()) {
            var catalogue=epochs.topologyCatalogue(plan.catalogue().directoryColumn());var definition=catalogue.readDefinition(request.header().definitionDigest());var state=catalogue.readIntent(request.header().intentId());
            if(definition==null || state==null || state.phase()!=TopologyChange.Phase.PUBLISHED && state.phase()!=TopologyChange.Phase.ACTIVE && state.phase()!=TopologyChange.Phase.RETIRED)throw new IOException("Ancestor metadata lacks retained committed publication");
            state.requireDefinition(definition);request.header().requireDefinition(definition);directory=definition.sourceDirectory();
            if(!Arrays.equals(directory.payloadDigest(),request.sourceDirectory().payloadDigest()) || definition.parents().stream().noneMatch(range->range.groupId().equals(request.sourceGroup())))throw new IOException("Ancestor metadata changed original source directory/group");
        }else {
            directory=epochs.catalogue(plan.catalogue().directoryColumn()).read(request.domainId());
            if(directory==null)throw new IOException("Current source group retired; rediscover placement");
            if(continuity){try{RatisSourceHeadRpc.requireContinuity(request,directory);}catch(IllegalArgumentException invalid){throw new IOException("Source continuity requires rediscovery",invalid);}}
            else if(!Arrays.equals(directory.payloadDigest(),request.sourceDirectory().payloadDigest()))throw new IOException("Current source image placement changed; qualify a current leaf again");
        }
        var store=stores.get(request.sourceGroup());if(store==null)throw new IOException("Source image has no actual native owner");var schema=request.metadataOnly()?epochs.schemaCatalogue(plan.catalogue().directoryColumn()).read(directory.schemaDigest()):store.installedSchema();
        if(schema==null){var bootstrap=logicalSchemas.get(directory.domainId());if(bootstrap!=null && bootstrap.clusterId().equals(directory.clusterId()) && bootstrap.databaseId().equals(directory.databaseId()) && Arrays.equals(bootstrap.digest(),directory.schemaDigest()))schema=bootstrap;}
        if(schema==null)throw new IOException("Source image has no qualified native logical schema");try{schema.requireMatches(directory);}catch(IllegalArgumentException invalid){throw new IOException("Source image native schema differs from qualified directory",invalid);}
        if(!request.relocation() && !request.metadataOnly())requireSourcePublication(request.sourceGroup(),directory,store);
        return new SourceAuthority(relocation,directory,schema);
    }
    private void requireSourcePublication(UUID group,RangeDirectory directory,ReplicatedRangeStore store)throws IOException {
        if(Arrays.equals(plan.directory(directory.domainId()).payloadDigest(),directory.payloadDigest()))return;
        try {
            var entry=groups.require(group);var seed=entry.seed();var topology=store.topologyActivation();
            if(seed!=null && topology!=null && Arrays.equals(seed.definition().targetDirectory().payloadDigest(),directory.payloadDigest())) {
                if(!seed.equals(owner(group).pool.topologySeed(seed.definition().intentId())) || !topology.catalogueGroup().equals(plan.catalogue().groupId()))throw new IOException("Current source lost its forced topology publication");topology.requireSeed(seed);topology.requireCut(store.applied());return;
            }
            var installation=store.schemaInstallation();var schema=store.schemaActivation();
            if(installation!=null && schema!=null && Arrays.equals(installation.definition().targetDirectory().payloadDigest(),directory.payloadDigest())) {
                if(!schema.catalogueGroup().equals(plan.catalogue().groupId()) || !schema.installation().equals(installation.proof()))throw new IOException("Current source lost its forced schema publication");schema.requireHeader(installation.definition().header());schema.requireCut(store.applied());return;
            }
            var imported=store.importActivation();var definition=imported==null?null:plan.importDefinition(group);
            if(imported!=null && definition!=null && Arrays.equals(definition.targetDirectory().payloadDigest(),directory.payloadDigest())) {
                if(!imported.catalogueGroup().equals(plan.catalogue().groupId()))throw new IOException("Current source import catalogue changed");imported.receipt().requireManifest(definition.manifest(group));imported.requireCut(store.applied());return;
            }
            var relocation=entry.relocation();
            if(relocation!=null && relocation.action()==RatisTopologyRelocationRecord.Action.COMPLETE && Arrays.equals(relocation.definition().targetDirectory().payloadDigest(),directory.payloadDigest()) && relocation.catalogue().equals(plan.catalogue().groupId()) && savedRelocation(relocation)) {
                var saved=ByteBuffer.wrap(SessionJournalFiles.read(relocationPath(relocation),RatisTopologyRelocationRecord.MAX_BYTES+16));if(!RatisRelocationOwnerRpc.covers(store.applied(),new ApplyPosition(saved.getLong(),saved.getLong())))throw new IOException("Current source relocation exceeds actual completed prefix");return;
            }
            requireCommittedPublication(directory);
        }catch(org.rocksdb.RocksDBException|IllegalArgumentException|IllegalStateException invalid){throw new IOException("Current source directory lacks qualified native publication",invalid);}
    }
    private void qualifySourceObsolete(PoolOwner owner,RatisRecoveryObsoleteRpc.Request request)throws Exception {
        var original=request.original();var scope=request.scope();var group=scope.groupId();
        if(!owner.spec.id().equals(groupPools.get(group)) || !plan.clusterId().equals(original.clusterId()) || !plan.databaseId().equals(original.databaseId()) || !plan.catalogue().groupId().equals(original.catalogue()))throw new IOException("Obsolete capture crossed source pool/namespace");
        var local=ownReplica(original.range());if(!original.sourceOwner(local))throw new IOException("Obsolete capture does not address an original source voter");
        var machine=machines.get(RaftGroupId.valueOf(group));var store=stores.get(group);
        if(server==null || machine==null || store==null || groups.require(group).role()!=RatisNodePlan.Role.DATA || !server.getId().toString().equals(local.peerId()))throw new IOException("Obsolete capture lacks actual source owner");
        var division=server.getDivision(machine.getGroupId());if(division.getStateMachine()!=machine || !division.getInfo().isAlive() || division.getInfo().getLifeCycleState()!=org.apache.ratis.util.LifeCycle.State.RUNNING || machine.getLifeCycleState()!=org.apache.ratis.util.LifeCycle.State.RUNNING)throw new IOException("Obsolete source division is not running");
        try(var workspace=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,4096L+8L*(TopologyChange.MAX_DEFINITION_BYTES+RatisCatalogue.MAX_DIRECTORY_BYTES+TopologyChange.MAX_STATUS_BYTES),1)) {
            var topology=epochs.topologyCatalogue(plan.catalogue().directoryColumn());var definition=topology.readDefinition(scope.header().definitionDigest());var state=topology.readIntent(scope.header().intentId());
            if(definition==null || state==null || state.phase()!=TopologyChange.Phase.PUBLISHED && state.phase()!=TopologyChange.Phase.ACTIVE && state.phase()!=TopologyChange.Phase.RETIRED)throw new IOException("Obsolete capture lacks irreversible catalogue consumption");
            state.requireDefinition(definition);scope.header().requireDefinition(definition);
            if(!Arrays.equals(definition.sourceDirectory().payloadDigest(),scope.sourceDirectory().payloadDigest()))throw new IOException("Obsolete capture changed frozen source directory");
            var current=epochs.catalogue(plan.catalogue().directoryColumn()).readView(original.domainId());
            if(current.directory()==null || !RatisRelocationOwnerRpc.covers(current.cut(),scope.catalogueCut()) || current.directory().generation()<definition.targetDirectory().generation() || current.directory().ranges().stream().anyMatch(range->range.groupId().equals(group)))throw new IOException("Obsolete capture lacks fresh consumed CURRENT placement");
            var freeze=store.topologyFreeze();if(freeze==null)throw new IOException("Obsolete capture source lacks actual native FREEZE");var range=scope.range();
            freeze.requireRequest(scope.header(),new RatisTopologyAuthority.View(scope.catalogue(),current.cut(),definition,state).admissionDigest());freeze.requireParent(group,range.rangeEpoch(),scope.sourceDirectory().domainId(),scope.sourceDirectory().schemaDigest(),range.lowerInclusive(),range.upperExclusive());freeze.requireCut(store.applied());
            if(!store.ownership().owner().equals(new RangeOwnership.Owner(group,range.rangeEpoch())))throw new IOException("Obsolete capture changed actual native source lineage");
            relocationClients.superseded(request.evidence(),range,scope.receiver());
            var after=server.getDivision(machine.getGroupId());if(!after.getMemberId().equals(division.getMemberId()) || after.getStateMachine()!=machine || !after.getInfo().isAlive() || after.getInfo().getLifeCycleState()!=org.apache.ratis.util.LifeCycle.State.RUNNING || !freeze.equals(store.topologyFreeze()))throw new IOException("Obsolete source owner changed during receiver proof");freeze.requireCut(store.applied());
        }
    }
    private RatisTopologySourceSnapshotService.Host sourceSnapshotHost(PoolOwner owner){var sourceArtifacts=owner.artifacts;return new RatisTopologySourceSnapshotService.Host(){
        public RatisTopologySourceSnapshotService.Lease capture(RatisTopologySourceSnapshotRpc.Request request,NativeRangeImage.IoAdmission admission)throws Exception{
            if(!owner.spec.id().equals(groupPools.get(request.sourceGroup())))throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Source request crossed physical pool");
            final RatisNodeResourceBudget.Lease reservation;try{reservation=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,65536L+6L*RatisTopologySourceSnapshotRpc.maximumPreparedBytes(request),1);}catch(IOException exhausted){throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Source metadata graph admission exhausted");}
            RatisTopologySourceSnapshotService.Lease captured=null;boolean returned=false;
            try{
                var retained=sourceArtifacts.lookupAdmitted(request,admission);final SourceAuthority view;try{view=sourceView(request,retained!=null);}catch(IOException rejected){var failure=new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Source authority rejected before capture");failure.initCause(rejected);throw failure;}var machine=machines.get(RaftGroupId.valueOf(request.sourceGroup()));if(machine==null)throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Source snapshot has no local source division");sourceArtifacts.reserve(request);
                captured=machine.captureSourceSnapshot(request,view.relocation(),view.directory(),view.schema(),sourceArtifacts,resources.imageMetadataAdmission(owner.spec.id(),admission));reservation.shrinkTo(Math.addExact(65536L,6L*RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,captured.prepared()).length),1);
                var actual=captured;var lease=new RatisTopologySourceSnapshotService.Lease(actual.prepared(),actual.source(),actual.snapshot(),()->{actual.close();reservation.close();});returned=true;return lease;
            }finally{if(!returned){if(captured==null){var sourceMachine=machines.get(RaftGroupId.valueOf(request.sourceGroup()));AutoCloseable cleanup=()->{if(sourceMachine!=null)sourceMachine.drainSourceCapture(request.transferId());reservation.close();};try{cleanup.close();}catch(Exception failed){owner.sourceCleanup.put(request.transferId(),cleanup);throw failed;}}else{var actual=captured;AutoCloseable cleanup=()->{actual.close();reservation.close();};try{cleanup.close();}catch(Exception failure){owner.sourceCleanup.put(request.transferId(),cleanup);throw failure;}}}}
        }
        public RatisSourceHeadRpc.Head head(RatisSourceHeadRpc.Request query)throws Exception {
            var request=query.source();if(!owner.spec.id().equals(groupPools.get(request.sourceGroup())))throw new IOException("Source head crossed physical pool");
            var machine=machines.get(RaftGroupId.valueOf(request.sourceGroup()));if(machine==null)throw new IOException("Source head has no actual source division");
            var before=sourceView(request,true);var first=machine.sourceSnapshotEvidence(request,before.relocation(),before.directory(),true);
            var catalogue=epochs.catalogue(plan.catalogue().directoryColumn()).readView(request.domainId());if(catalogue.directory()==null)throw new IOException("Source domain missing; rediscover placement");
            var after=sourceView(request,true);var last=machine.sourceSnapshotEvidence(request,after.relocation(),after.directory(),true);
            if(!Arrays.equals(before.directory().payloadDigest(),after.directory().payloadDigest()) || !first.owner().equals(last.owner()) || !first.completedCut().equals(last.completedCut()) || first.configuration().term()!=last.configuration().term() || first.configuration().index()!=last.configuration().index() || !Arrays.equals(first.configuration().data(),last.configuration().data()) || !request.metadataOnly() && !request.relocation() && !Arrays.equals(after.directory().payloadDigest(),catalogue.directory().payloadDigest()))throw new IOException("Source head changed during catalogue/native proof");
            return new RatisSourceHeadRpc.Head(query,catalogue.cut(),catalogue.directory(),last.owner(),last.completedCut(),last.configuration());
        }
        public RatisTopologySourceSnapshotRpc.Abandoned abandoned(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return sourceArtifacts.abandoned(request);}
        public RatisTopologySourceSnapshotRpc.Abandoned abandon(RatisTopologySourceSnapshotRpc.Request request)throws Exception{
            var view=sourceView(request);var machine=machines.get(RaftGroupId.valueOf(request.sourceGroup()));if(machine==null)throw new IOException("Original source capture owner unavailable");var cleanup=owner.sourceCleanup.get(request.transferId());if(cleanup!=null){cleanup.close();owner.sourceCleanup.remove(request.transferId(),cleanup);}machine.drainSourceCapture(request.transferId());
            var evidence=machine.sourceSnapshotEvidence(request,view.relocation(),view.directory());return sourceArtifacts.abandon(request,evidence.configuration(),evidence.completedCut());
        }
        public org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Gate.SnapshotPermit requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{
            var request=lease.prepared().request();var machine=machines.get(RaftGroupId.valueOf(request.sourceGroup()));if(machine==null || lease.source().getStateMachine()!=machine)throw new IOException("Source snapshot division changed");var permit=machine.recoveryEligibilityGate().snapshotPermit();
            sourceArtifacts.requireReady(lease.prepared());var view=sourceView(request,true);var evidence=machine.sourceSnapshotEvidence(request,view.relocation(),view.directory(),true,true);
            if(!evidence.owner().equals(lease.prepared().sourceOwner()) || !RatisRelocationOwnerRpc.covers(evidence.completedCut(),lease.prepared().image().position()))throw new IOException("Retained source image lost actual native owner/configuration");permit.requireCurrent();return permit;
        }
        public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{
            var request=lease.prepared().request();var view=sourceView(request,true);var image=lease.prepared().image();
            if(request.relocation()){var evidence=relocationClients.read(new RatisRelocationOwnerRpc.Request(view.relocation(),request.receiver().replicaId(),RatisRelocationOwnerRpc.Expected.IMAGE,image.position(),image.sha256()));if(!evidence.imageInstalled())throw new IOException("Receiver has not forced the exact native image");}
            else if(!relocationClients.imageInstalled(new RatisRecoveryImageRpc.Request(request,image.position(),image.sha256())).imageInstalled())throw new IOException("Receiver has not forced the exact registered current/metadata image");
        }
        public boolean retryObsolete(RatisRecoveryObsoleteRpc.Request request)throws Exception{return sourceArtifacts.retryObsolete(request);}
        public void requireObsolete(RatisRecoveryObsoleteRpc.Request request)throws Exception{qualifySourceObsolete(owner,request);}
        public void forceObsolete(RatisRecoveryObsoleteRpc.Request request)throws Exception{sourceArtifacts.forceObsolete(request);}
        public void obsolete(RatisRecoveryObsoleteRpc.Request request)throws Exception{
            var original=request.original();var cleanup=owner.sourceCleanup.get(original.transferId());if(cleanup!=null){cleanup.close();owner.sourceCleanup.remove(original.transferId(),cleanup);}
            var machine=machines.get(RaftGroupId.valueOf(original.sourceGroup()));if(machine==null)throw new IOException("Obsolete source capture owner disappeared");machine.drainSourceCapture(original.transferId());sourceArtifacts.obsolete(request);
        }
        public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{sourceArtifacts.released(lease.prepared().request());}
        public boolean isReleased(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return sourceArtifacts.isReleased(request);}
        public boolean chunkReady(RatisTopologySourceSnapshotRpc.Request request,long bytes)throws IOException{return migration.acquire(request.sourceGroup(),owner.spec.id(),bytes,request.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.BOOTSTRAP);}
        public void transferFinished(RatisTopologySourceSnapshotRpc.Request request){migration.forget(request.sourceGroup(),owner.spec.id());}
        public org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto send(RatisTopologySourceSnapshotRpc.Request scope,org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto chunk)throws IOException{
            if(server==null || serverClosed)throw new IOException("Source server unavailable");var receiver=RaftPeerId.valueOf(chunk.getServerRequest().getReplyId());
            if(!receiver.toString().equals(scope.receiver().peerId()) || !org.apache.ratis.util.ProtoUtils.toRaftGroupId(chunk.getServerRequest().getRaftGroupId()).getUuid().equals(scope.sourceGroup()))throw new IOException("Snapshot chunk crossed its admitted receiver/group");
            server.getServerRpc().addRaftPeers(List.of(RaftPeer.newBuilder().setId(receiver).setAddress(scope.receiver().address()).build()));return server.getServerRpc().installSnapshot(chunk);
        }
    };}
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
    private static boolean registeredRecovery(RatisRuntimeGroups.Entry entry){return entry.current()!=null || entry.metadata()!=null || entry.supersession()!=null;}
    private Path registeredExposure(UUID group){return root.resolve("topology-exposure/registered").resolve(group.toString());}
    private byte[] registeredExposureBytes(RatisRuntimeGroups.Entry entry){
        var range=entry.descriptor();var receiver=entry.metadata()!=null?entry.metadata().receiver():entry.supersession()!=null?entry.supersession().receiver():entry.current().receiver();var identity=ByteBuffer.allocate(88);
        for(var id:List.of(range.groupId(),range.rangeId(),entry.directory().domainId(),plan.catalogue().groupId(),receiver.replicaId()))identity.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());identity.putLong(range.rangeEpoch());
        // Exposure belongs to this native group/receiver. Exact image permissions live in the forced native registry.
        return ByteBuffer.allocate(80).putLong(range.groupId().getMostSignificantBits()).putLong(range.groupId().getLeastSignificantBits()).put(OperatorUpload.sha256(identity.array())).put(plan.identityDigest()).array();
    }
    private void validateSeedExposures()throws IOException{
        var directory=root.resolve("topology-exposure");if(!Files.exists(directory))return;
        try(var files=Files.newDirectoryStream(directory)){int count=0;for(var path:files){if(path.getFileName().toString().equals("registered")){if(!Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid registered recovery exposure directory");continue;}if(++count>RatisNodePlan.MAX_LOCAL_GROUPS)throw new IOException("Topology exposure count exceeds group bound");validateExposure(path,false);}}
        var registered=directory.resolve("registered");if(Files.exists(registered))try(var files=Files.newDirectoryStream(registered)){int count=0;for(var path:files){if(++count>RatisNodePlan.MAX_LOCAL_GROUPS)throw new IOException("Recovery exposure count exceeds group bound");validateExposure(path,true);}}
    }
    private void validateExposure(Path path,boolean recovery)throws IOException{
        final UUID id;try{id=UUID.fromString(path.getFileName().toString());}catch(IllegalArgumentException malformed){throw new IOException("Invalid topology exposure path",malformed);}if(!path.getFileName().toString().equals(id.toString()))throw new IOException("Noncanonical topology exposure path");
        final RatisRuntimeGroups.Entry entry;try{entry=groups.require(id);}catch(IllegalArgumentException unknown){throw new IOException("Exposure has no forced native registration",unknown);}
        if((recovery?!registeredRecovery(entry):entry.seed()==null && entry.learner()==null) || !Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || !Arrays.equals(SessionJournalFiles.read(path,128),recovery?registeredExposureBytes(entry):seedExposureBytes(entry)))throw new IOException("Exposure differs from original native registration/bootstrap");
    }
    private void requireRegisteredStorage(RatisRuntimeGroups.Entry entry)throws IOException{
        var id=entry.group().getGroupId().getUuid();var exposure=registeredExposure(id);boolean exposed=Files.exists(exposure);
        if(exposed && (!Files.isRegularFile(exposure,LinkOption.NOFOLLOW_LINKS) || !Arrays.equals(SessionJournalFiles.read(exposure,128),registeredExposureBytes(entry))))throw new IOException("Recovery exposure differs from original native registration");
        var directory=root.resolve("raft").resolve(id.toString());
        if(Files.isRegularFile(directory.resolve("current/raft-meta"))){boolean original=plan.localGroups().stream().anyMatch(group->group.getGroupId().getUuid().equals(id));if(!exposed && !original)throw new IOException("Recovered Raft state lacks registration exposure intent");return;}
        if(exposed && !virginSeedAdds.contains(id))throw new IOException("Exposed registered recovery Raft storage is missing: "+id);
        var store=stores.get(id);boolean pending=store!=null && (entry.metadata()!=null?store.metadataRecoveryPending():store.topologyRecoveryOnly());
        if(entry.supersession()!=null && entry.metadata()==null){var captured=entry.supersession().captured();if(captured.localImageReceipt()!=null || captured.pendingIntent()!=null || captured.marker()!=null || captured.configuration()!=null)throw new IOException("Superseded retained owner needs original Raft/snapshot storage: "+id);}
        if(!pending || store.applied()!=null)throw new IOException("Established recovery needs original Raft/snapshot storage: "+id);
        if(Files.exists(directory))try(var files=Files.walk(directory)){if(files.anyMatch(Files::isRegularFile))throw new IOException("Registered recovery has unidentified partial Raft storage");}
    }
    private void requireSeedStorage(RatisRuntimeGroups.Entry entry)throws IOException{
        if(registeredRecovery(entry)){requireRegisteredStorage(entry);return;}
        if(entry.seed()==null && entry.learner()==null)return;var id=entry.group().getGroupId().getUuid();var exposure=seedExposure(id);
        boolean exposed=Files.exists(exposure);if(exposed && (!Files.isRegularFile(exposure) || !Arrays.equals(SessionJournalFiles.read(exposure,128),seedExposureBytes(entry))))throw new IOException("Topology exposure differs from forced birth/bootstrap");
        var directory=root.resolve("raft").resolve(id.toString());
        if(Files.isRegularFile(directory.resolve("current/raft-meta"))){if(!exposed)throw new IOException("Topology child Raft state lacks prior exposure intent");return;}
        if(exposed && !virginSeedAdds.contains(id))throw new IOException("Exposed topology child Raft storage is missing: "+entry.group().getGroupId());
        var store=stores.get(id);if(store==null || store.applied()!=null || !store.topologyBirthPending())throw new IOException("Established topology child Raft storage is missing: "+entry.group().getGroupId());
        if(Files.exists(directory))try(var files=Files.walk(directory)){if(files.anyMatch(Files::isRegularFile))throw new IOException("Virgin topology birth has unidentified partial Raft storage");}
    }
    private void markSeedExposure(RatisRuntimeGroups.Entry entry)throws IOException{
        if(registeredRecovery(entry)){var id=entry.group().getGroupId().getUuid();var exposure=registeredExposure(id);if(Files.exists(exposure))return;requireRegisteredStorage(entry);var directory=Files.createDirectories(exposure.getParent());SessionJournalFiles.force(directory.getParent());SessionJournalFiles.force(root);SessionJournalFiles.write(exposure,registeredExposureBytes(entry),128);SessionJournalFiles.force(directory);virginSeedAdds.add(id);return;}
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
    /** Structural current-head qualification from forced native history; this does not assert Raft readiness. */
    public void requireCurrentPlacement(Map<UUID,RangeDirectory> directories,Map<UUID,DistributedSchema> schemas)throws IOException {
        Objects.requireNonNull(directories);Objects.requireNonNull(schemas);
        synchronized(this){if(closing || closeRequested || closed || installing)throw new IOException("Native placement ownership is changing/unavailable");}
        if(!directories.keySet().equals(routedClients.keySet()) || !schemas.keySet().equals(directories.keySet()))throw new IOException("Current placement domain scope differs from configured node");
        try {
            var local=new HashSet<UUID>();
            for(var domain:directories.keySet()) {
                var candidate=directories.get(domain);var schema=schemas.get(domain);var bootstrap=plan.directory(domain);
                if(candidate==null || schema==null || !candidate.domainId().equals(domain) || !candidate.clusterId().equals(plan.clusterId()) || !candidate.databaseId().equals(plan.databaseId()))throw new IOException("Current placement namespace changed");
                schema.requireMatches(candidate);
                if(!Arrays.equals(data(domain).directory().payloadDigest(),candidate.payloadDigest()))throw new IOException("Current route does not match qualified catalogue placement");
                boolean directoryQualified=Arrays.equals(bootstrap.payloadDigest(),candidate.payloadDigest());
                for(var range:candidate.ranges()) {
                    var replica=range.replicas().stream().filter(r->r.nodeId().equals(plan.nodeId())).findFirst().orElse(null);if(replica==null)continue;
                    if(!replica.nodeIncarnation().equals(plan.nodeIncarnation()) || !replica.peerId().equals(plan.peerId()) || !replica.address().equals(plan.address()))throw new IOException("Current placement changed local node incarnation/endpoint");
                    local.add(range.groupId());var entry=groups.require(range.groupId());var store=stores.get(range.groupId());
                    if(entry.role()!=RatisNodePlan.Role.DATA || entry.retired() || store==null || !entry.directory().domainId().equals(domain))throw new IOException("Current range lacks exact recovered native registration");
                    var imported=store.importActivation();var importDefinition=imported==null?null:plan.importDefinition(range.groupId());
                    if(imported!=null) {
                        if(importDefinition==null || !imported.catalogueGroup().equals(plan.catalogue().groupId()))throw new IOException("Current import lacks original configured publication");
                        imported.receipt().requireManifest(importDefinition.manifest(range.groupId()));imported.requireCut(store.applied());
                    }
                    if(!entry.descriptor().equals(range) && !(imported!=null
                            && Arrays.equals(entry.directory().payloadDigest(),importDefinition.sourceDirectory().payloadDigest())
                            && importDefinition.sourceDirectory().ranges().stream().anyMatch(entry.descriptor()::equals)
                            && importDefinition.targetDirectory().ranges().stream().anyMatch(range::equals)))throw new IOException("Current range lacks exact recovered native registration");
                    boolean original=bootstrap.ranges().stream().anyMatch(range::equals) && Arrays.equals(bootstrap.schemaDigest(),candidate.schemaDigest());
                    boolean plannedFenced=original && range.topologyState()==RangeDescriptor.TopologyState.FENCED;
                    if(store.pendingInstallation() || store.topologyBirthPending() || store.topologyRecoveryOnly() || store.topologyRetired() || !plannedFenced && (store.importPending() || store.topologyFreeze()!=null))throw new IOException("Current range retains pending/recovery/retired ownership");
                    if(store.importPending() && plan.importDefinition(range.groupId())==null)throw new IOException("Fenced import lacks original planned ownership");
                    var ownership=store.ownership();var interval=ownership.interval();
                    if(!ownership.owner().groupId().equals(range.groupId()) || ownership.owner().epoch()!=range.rangeEpoch() || !interval.domainId().equals(domain) || !interval.columns().equals(candidate.columns()) || !Arrays.equals(interval.lowerInclusive(),range.lowerInclusive()) || !Arrays.equals(interval.upperExclusive(),range.upperExclusive()))throw new IOException("Current native ownership geometry changed");
                    var nativeSchema=store.installedSchema();if(nativeSchema==null || !Arrays.equals(DistributedSchemaCodec.encode(nativeSchema),DistributedSchemaCodec.encode(schema)))throw new IOException("Current schema lacks exact recovered native installation");nativeSchema.requireMatches(candidate);
                    boolean qualified=original;
                    if(entry.seed()!=null) {
                        var seed=owner(range.groupId()).pool.topologySeed(entry.seed().definition().intentId());var activation=store.topologyActivation();
                        if(seed==null || !seed.equals(entry.seed()) || store.topologyBirth()==null || activation==null || !activation.catalogueGroup().equals(plan.catalogue().groupId()))throw new IOException("Current child lacks forced birth/activation history");
                        store.topologyBirth().requireSeed(seed);activation.requireSeed(seed);activation.requireCut(store.applied());qualified=true;
                        if(Arrays.equals(seed.definition().targetDirectory().payloadDigest(),candidate.payloadDigest()))directoryQualified=true;
                    }
                    if(entry.current()!=null){if(!entry.current().equals(store.currentRangeRecoveryRegistration()) || !owner(range.groupId()).pool.currentRangeRecoveries().contains(entry.current()) || !store.currentRangeRecoveryReady() || !store.admissionOpen())throw new IOException("Current range lacks completed registered image/tail authority");qualified=true;if(Arrays.equals(entry.current().directory().payloadDigest(),candidate.payloadDigest()))directoryQualified=true;}
                    if(entry.learner()!=null) {
                        if(!entry.learner().equals(store.topologyLearnerRegistration()) || !owner(range.groupId()).pool.topologyLearners().contains(entry.learner()) || !store.topologyLearnerServing())throw new IOException("Current learner lacks forced serving adoption");qualified=true;
                    }
                    var installation=store.schemaInstallation();var activation=store.schemaActivation();
                    if(installation!=null) {
                        if(activation==null || !activation.catalogueGroup().equals(plan.catalogue().groupId()) || !activation.installation().equals(installation.proof()))throw new IOException("Current schema has no completed native activation");
                        activation.requireHeader(installation.definition().header());activation.requireCut(store.applied());installation.definition().targetSchema().requireMatches(candidate);qualified=true;
                        if(Arrays.equals(installation.definition().targetDirectory().payloadDigest(),candidate.payloadDigest()))directoryQualified=true;
                    }
                    if(imported!=null) {
                        qualified=true;
                        if(Arrays.equals(importDefinition.targetDirectory().payloadDigest(),candidate.payloadDigest()))directoryQualified=true;
                    }
                    if(entry.relocation()!=null) {
                        var record=entry.relocation();var cfg=store.configuration();
                        if(record.action()!=RatisTopologyRelocationRecord.Action.COMPLETE || !record.catalogue().equals(plan.catalogue().groupId()) || !record.definition().targets().getFirst().equals(range) || cfg==null || !RatisTopologyRelocationRecord.sameConfiguration(cfg.data(),record.targetConfiguration()) || !savedRelocation(record))throw new IOException("Current relocation lacks forced completed configuration history");
                        byte[] saved=SessionJournalFiles.read(relocationPath(record),RatisTopologyRelocationRecord.MAX_BYTES+16);var cut=ByteBuffer.wrap(saved);if(store.applied()==null || !RatisRelocationOwnerRpc.covers(store.applied(),new ApplyPosition(cut.getLong(),cut.getLong())))throw new IOException("Relocation sidecar exceeds completed native prefix");qualified=true;
                        if(Arrays.equals(record.definition().targetDirectory().payloadDigest(),candidate.payloadDigest()))directoryQualified=true;
                    }
                    if(!qualified)throw new IOException("Current placement is not backed by original or forced native history");
                    if(groups.require(range.groupId())!=entry || !Arrays.equals(DistributedSchemaCodec.encode(store.installedSchema()),DistributedSchemaCodec.encode(nativeSchema)))throw new IOException("Native placement changed during qualification");
                }
                if(!directoryQualified)requireCommittedPublication(candidate);
            }
            for(var entry:groups.entries())if(entry.role()==RatisNodePlan.Role.DATA && !entry.retired() && entry.descriptor().replicas().stream().anyMatch(r->r.nodeId().equals(plan.nodeId())) && !local.contains(entry.descriptor().groupId()))throw new IOException("Recovered local DATA owner is absent from current placement");
        }catch(org.rocksdb.RocksDBException|IllegalArgumentException|IllegalStateException invalid){throw new IOException("Current placement differs from forced native history",invalid);}
    }
    /** Remote publication is qualified by its immutable administrative intent, never a bare directory CAS. */
    private void requireCommittedPublication(RangeDirectory candidate)throws IOException {
        var column=plan.catalogue().directoryColumn();var domain=candidate.domainId();
        var topologies=epochs.topologyCatalogue(column);var topology=topologies.readHead(domain);
        if(topology!=null && (topology.phase()==TopologyChange.Phase.PUBLISHED || topology.phase()==TopologyChange.Phase.ACTIVE || topology.phase()==TopologyChange.Phase.RETIRED)) {
            var definition=topologies.readDefinition(topology.header().definitionDigest());
            if(definition!=null) {topology.requireDefinition(definition);if(Arrays.equals(definition.targetDirectory().payloadDigest(),candidate.payloadDigest()) && topology.equals(topologies.readHead(domain)))return;}
        }
        var administrations=epochs.schemaAdministration(column);var schema=administrations.readHead(domain);
        if(schema!=null && (schema.status().phase()==SchemaChange.Phase.PUBLISHED || schema.status().phase()==SchemaChange.Phase.RETIRED)) {
            var definition=administrations.readDefinition(schema.status().header().definitionDigest());
            if(definition!=null && Arrays.equals(definition.header().encode(),schema.status().header().encode()) && Arrays.equals(definition.targetDirectory().payloadDigest(),candidate.payloadDigest())) {var confirmed=administrations.readHead(domain);if(confirmed!=null && Arrays.equals(confirmed.encode(),schema.encode()))return;}
        }
        var imports=epochs.rangeImports(column);var imported=imports.readHead(domain);
        if(imported!=null && imported.phase()!=RatisRangeImportCatalogue.Phase.PREPARED) {
            var definition=imports.readDefinition(imported.definitionDigest());
            if(definition!=null) {imported.requireDefinition(definition);if(Arrays.equals(definition.targetDirectory().payloadDigest(),candidate.payloadDigest())) {var confirmed=imports.readHead(domain);if(confirmed!=null && Arrays.equals(confirmed.encode(),imported.encode()))return;}}
        }
        throw new IOException("Current directory generation lacks original, native, or immutable committed publication");
    }
    /** Only an already forced native recipe may extend the local voting registry. Exact retry never formats an owner. */
    public void installTopologySeed(NativeTopologySeed seed)throws IOException{
        beginSeedLifecycle(true);try{installTopologySeedOwned(seed);}catch(org.rocksdb.RocksDBException failure){throw new IOException("Native topology children could not register",failure);}finally{endSeedLifecycle();}
    }
    private void beginSeedLifecycle(boolean seed)throws IOException{synchronized(this){if(installing || starting || closing || closeRequested || closed || recoveryAttempt!=null || supersessionAttempt!=null || seed && learnerAttempt!=null || !seed && seedAttempt!=null)throw new IOException("Node lifecycle or retained force attempt prevents another topology mutation");installing=true;}}
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
    /** Owned recovery IO forces negative receiver authority before requesting any META image. */
    NativeRecoverySupersession supersedeRecovery(NativeRecoverySupersession.Scope scope)throws IOException {
        Objects.requireNonNull(scope);
        synchronized(this){if(!started || server==null || installing || starting || closing || closeRequested || closed || learnerAttempt!=null || seedAttempt!=null)throw new IOException("Supersession node owner unavailable");installing=true;}
        try {
            var owner=pools.get(plan.incomingPoolId(scope.sourceDirectory().domainId(),scope.groupId()));if(owner==null)throw new IOException("Supersession has no configured native pool");
            try(var workspace=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,8192L+16L*NativeRecoverySupersession.MAX_BYTES,1)) {
                var frozen=requireSupersessionGraph(scope);requireRecoveryPool(owner,scope.sourceDirectory());requirePoolCapacity(owner.spec.id(),List.of(scope.groupId()));groups.preflightSupersession(scope);
                if(supersessionAttempt!=null && (!supersessionAttempt.pool().equals(owner.spec.id()) || !supersessionAttempt.scope().equals(scope)))throw new IOException("Unknown supersession force requires its exact original scope");
                if(recoveryAttempt!=null && (recoveryAttempt.current()==null || !recoveryAttempt.pool().equals(owner.spec.id()) || !recoveryAttempt.group().equals(scope.groupId())))throw new IOException("Supersession cannot replace another native force owner");
                if(recoveryAttempt!=null){
                    var current=recoveryAttempt.current();scope.requireCurrent(current);owner.pool.openCurrentRangeRecovery(current);recoveryAttempt=null;
                }
                if(supersessionAttempt==null){
                    long originalCredit=owner.recoveryMetadata==null?0:owner.recoveryMetadata.bytes();boolean saved=false;
                    try{long retained=4096L+16L*Math.addExact(recoveryRegistryBytes(owner),NativeRecoverySupersession.MAX_BYTES);
                    if(owner.recoveryMetadata==null)owner.recoveryMetadata=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,retained,1);else owner.recoveryMetadata.growTo(Math.max(owner.recoveryMetadata.bytes(),retained));
                    long claim=metadataClaims.contains(scope.groupId())?0:metadataClaim(owner.spec,scope.groupId(),RatisNodePlan.Role.DATA).bytes();
                    boolean registered=owner.pool.currentRangeRecoveries().stream().anyMatch(value->value.groupId().equals(scope.groupId())) || owner.pool.recoverySupersessions().stream().anyMatch(value->value.groupId().equals(scope.groupId()));
                    long exposure=registered?0:224,total=metadataClaims.bytes()+claim+exposure;for(var pool:pools.values())total=Math.addExact(total,pool.recoveryExposureBytes);
                    if(total>settings.resources().maximumDurableMetadataBytes() || metadataClaims.bytes(owner.spec.id())+claim+owner.recoveryExposureBytes+exposure>owner.spec.maximumDurableMetadataBytes())throw new IOException("Supersession durable metadata/exposure ceiling exhausted");
                    reserveDataMetadata(owner,List.of(scope.groupId()));owner.recoveryExposureBytes+=exposure;supersessionAttempt=new SupersessionAttempt(owner.spec.id(),scope);saved=true;
                    }finally{if(!saved){if(originalCredit==0){if(owner.recoveryMetadata!=null)owner.recoveryMetadata.close();owner.recoveryMetadata=null;}else owner.recoveryMetadata.shrinkTo(originalCredit,1);}}
                }
                var id=RaftGroupId.valueOf(scope.groupId());var machine=machines.get(id);var existing=stores.get(scope.groupId());
                var forced=new ReplicatedRangeStore[1];RatisRangeStateMachine.LocalDiscoveryPublication force=()->{
                    if(recoveryGraph.snapshot()!=frozen)throw new IOException("Supersession graph owner changed before force");requireSupersessionGraph(scope);
                    synchronized(this){if(closeRequested || closing || closed)throw new IOException("Node close prevents supersession force");}
                    try{forced[0]=owner.pool.forceRecoverySupersession(scope);}catch(org.rocksdb.RocksDBException failure){throw new IOException("Supersession native force did not acknowledge",failure);}
                };
                if(machine!=null && (existing==null || existing.acknowledgedRecoverySupersession()==null))machine.supersedeCurrentDiscovery(scope,this::publishLocalDiscovery,force,settings.shutdownTimeout());
                else force.publish();
                var store=forced[0];var value=store.acknowledgedRecoverySupersession();if(value==null || !value.scope().equals(scope))throw new IOException("Supersession lacks exact acknowledged native fence");
                assignPool(scope.groupId(),owner.spec.id());groups.register(value);var entry=groups.require(scope.groupId());
                if(machine==null){
                    machine=RatisRangeStateMachine.withSessions(store,entry.group(),owner.images(),command->{},epochs,entry.directory(),owner.retention());machine.nodeResources(resources,owner.spec.id());machine.logResources(logBudget(scope.groupId(),owner.spec.id()));machine.diskRunway(diskRunway,owner.spec.id());machine.committedApply(committedDispatcher,owner.spec.id(),MAX_COMMITTED_ENTRY_BYTES);configureDataMachine(machine,entry.directory(),entry.descriptor());machine.learnerInstaller(topologyImageQueries,this::joinTopologyLearner);machine.learnerRecoveryInstaller(this::prepareLearnerRecovery);machine.proofQueries(proofQueries);machine.exportSources(exportSources);machine.deferRecoveryDiscovery();stores.put(scope.groupId(),store);machines.put(id,machine);
                }else machine.acknowledgeRecoverySupersession(value);
                requireSeedStorage(entry);markSeedExposure(entry);var actual=new HashSet<RaftGroupId>();server.getGroupIds().forEach(actual::add);if(!actual.contains(id))addGroup(entry.group());
                owner.recoveryEncodedBytes=recoveryRegistryBytes(owner);owner.recoveryMetadata.shrinkTo(4096L+16L*owner.recoveryEncodedBytes,1);supersessionAttempt=null;return value;
            }
        }catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException("Configured receiver supersession failed",failure);}finally{endSeedLifecycle();}
    }
    private static long recoveryRegistryBytes(PoolOwner owner){long bytes=0;for(var value:owner.pool.currentRangeRecoveries())bytes=Math.addExact(bytes,value.encodedSize());for(var value:owner.pool.metadataRecoveries())bytes=Math.addExact(bytes,value.encodedSize());for(var value:owner.pool.recoverySupersessions())bytes=Math.addExact(bytes,value.encodedSize());return bytes;}
    private RatisConfiguredRecoveryGraph.Snapshot requireSupersessionGraph(NativeRecoverySupersession.Scope scope)throws IOException {
        var frozen=recoveryGraph.snapshot();var receiver=scope.receiver();
        if(frozen==null || !scope.catalogue().equals(plan.catalogue().groupId()) || !scope.sourceDirectory().clusterId().equals(plan.clusterId()) || !scope.sourceDirectory().databaseId().equals(plan.databaseId()) || !receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation()) || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address()) || !RatisRelocationOwnerRpc.covers(frozen.plan().catalogueCut(),scope.catalogueCut()))throw new IOException("Supersession lacks its forced local recovery graph");
        var selected=frozen.activeRequests().get(scope.groupId());
        if(selected==null || !selected.metadataOnly() || !Arrays.equals(selected.header().encode(),scope.header().encode()) || !Arrays.equals(selected.sourceDirectory().payloadDigest(),scope.sourceDirectory().payloadDigest()) || !Arrays.equals(OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,selected)),scope.selectedMetadataRequestDigest()))throw new IOException("Supersession changed selected META request");
        var aliases=new HashSet<String>();for(var request:frozen.frozenRequests())if(request.purpose()==RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF && request.sourceGroup().equals(scope.groupId())){
            byte[] digest=OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request));new RatisRecoveryObsoleteRpc.Request(request,scope,new byte[32]);aliases.add(HexFormat.of().formatHex(digest));
        }
        if(aliases.size()!=scope.currentRequestDigests().size())throw new IOException("Supersession omitted or added retained CURRENT cleanup identities");
        var current=stores.get(scope.groupId());var registration=current==null?null:current.currentRangeRecoveryRegistration();
        if(registration==null)for(var pool:pools.values())for(var value:pool.pool.currentRangeRecoveries())if(value.groupId().equals(scope.groupId()))registration=value;
        if(registration!=null){
            scope.requireCurrent(registration);var actual=new RatisTopologySourceSnapshotRpc.Request(registration.transferId(),null,registration.catalogue(),registration.receiver(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,registration.groupId(),registration.directory());
            if(!scope.authorizesCurrentRequestDigest(OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,actual))))throw new IOException("Supersession omitted actual CURRENT registration body");
        }
        return frozen;
    }
    /** Qualifies and forces a configured CURRENT or sealed META receiver before division exposure.
     * Trusted actor seam: Prepared must come from actual source PREPARE transport. Fresh HEAD/CAT
     * prove owner and lineage; the exact registered image bytes/hash are verified during native install. */
    public ReplicatedRangeStore registerRecovery(RatisTopologySourceSnapshotRpc.Prepared prepared)throws IOException {
        Objects.requireNonNull(prepared);var request=prepared.request();
        synchronized(this){if(!started || server==null || installing || starting || closing || closeRequested || closed || learnerAttempt!=null || seedAttempt!=null || supersessionAttempt!=null)throw new IOException("Recovery registration node owner unavailable");installing=true;}
        try {
            if(request.relocation() || !request.catalogue().equals(plan.catalogue().groupId()) || !request.clusterId().equals(plan.clusterId()) || !request.databaseId().equals(plan.databaseId()))throw new IOException("Configured recovery namespace/purpose differs");
            var receiver=request.receiver();if(!receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation()) || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address()))throw new IOException("Configured recovery receiver incarnation/endpoint differs");
            var owner=pools.get(plan.incomingPoolId(request.domainId(),request.sourceGroup()));requireRecoveryPool(owner,request.sourceDirectory());requirePoolCapacity(owner.spec.id(),List.of(request.sourceGroup()));
            try(var workspace=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,4096L+16L*prepared.encodedSize(),1)) {
                byte[] digest=OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,prepared));
                if(request.metadataOnly() && recoveryAttempt!=null && recoveryAttempt.current()!=null
                        && !Arrays.equals(recoveryAttempt.preparedDigest(),digest))settleFrozenCurrentRegistration(owner,prepared,workspace);
                if(recoveryAttempt!=null && (!recoveryAttempt.group().equals(request.sourceGroup()) || !recoveryAttempt.pool().equals(owner.spec.id()) || !Arrays.equals(recoveryAttempt.preparedDigest(),digest)))throw new IOException("Unknown recovery force requires exact original Prepared retry");
                var authority=qualifyRecovery(prepared,bytes->workspace.growTo(Math.max(workspace.bytes(),4096L+16L*Math.addExact(prepared.encodedSize(),bytes))));requireGraphRegistrationRequest(prepared.request(),authority.directory());var current=owner.pool.currentRangeRecoveries().stream().filter(r->r.groupId().equals(request.sourceGroup())).findFirst().orElse(null);var metadata=owner.pool.metadataRecoveries().stream().filter(r->r.groupId().equals(request.sourceGroup())).findFirst().orElse(null);
                var frozen=recoveryAttempt;ApplyPosition cut=frozen!=null?(frozen.current()!=null?frozen.current().catalogueCut():frozen.metadata().catalogueCut()):current!=null && !request.metadataOnly()?current.catalogueCut():metadata!=null && metadata.transferId().equals(request.transferId())?metadata.catalogueCut():authority.cut();
                int previousGraphBytes=frozen!=null?frozen.previousBytes():metadata!=null?metadata.encodedSize():current!=null?current.encodedSize():0;workspace.growTo(Math.max(workspace.bytes(),4096L+16L*Math.addExact(prepared.encodedSize(),previousGraphBytes)));
                NativeCurrentRangeRecovery proposedCurrent=null;NativeMetadataRecoveryRegistration proposedMetadata=null;
                if(request.metadataOnly()) {
                    proposedMetadata=new NativeMetadataRecoveryRegistration(request.transferId(),request.catalogue(),cut,request.sourceDirectory(),prepared.metadataFreeze(),receiver,prepared.sourceOwner(),prepared.sourceCfg().term(),prepared.sourceCfg().index(),prepared.sourceCfg().data(),prepared.image().position(),prepared.image().sha256(),prepared.image().deterministicMergeColumns(),current!=null?NativeMetadataRecoveryRegistration.CurrentPredecessor.of(current):metadata==null?null:metadata.currentPredecessor());
                    groups.preflight(proposedMetadata);if(current!=null){proposedMetadata.requireTransitionOf(current);}if(metadata!=null && !metadata.equals(proposedMetadata)){if(metadata.transferId().equals(request.transferId()))throw new IOException("META retry changed its original image recipe");if(recoveryCompletionAttempts.containsKey(request.sourceGroup()))throw new IOException("META successor waits for original completion settlement");proposedMetadata.requireSuccessorOf(metadata);}
                } else {
                    if(metadata!=null)throw new IOException("Sealed META group cannot adopt CURRENT image");
                    var names=new HashMap<UUID,String>();var configured=plan.domain(request.domainId()).columns();for(var column:prepared.schema().columnIds())names.put(column,configured.getOrDefault(column,"recovery-"+column));
                    proposedCurrent=new NativeCurrentRangeRecovery(request.transferId(),request.catalogue(),cut,request.sourceDirectory(),prepared.schema(),request.sourceGroup(),receiver,prepared.sourceOwner(),prepared.image().position(),prepared.image().sha256(),prepared.sourceCfg().term(),prepared.sourceCfg().index(),prepared.sourceCfg().data(),prepared.image().topologyOrigin(),names,prepared.image().deterministicMergeColumns(),prepared.schemaRows());
                    groups.preflight(proposedCurrent);if(current!=null && !current.equals(proposedCurrent))throw new IOException("CURRENT registration changed original installed recipe");
                    if(current==null && stores.containsKey(request.sourceGroup()))throw new IOException("CURRENT registration requires absent local native group");requireIncomingOwnership(request.sourceDirectory(),List.of(request.range()),Set.of());
                }
                if(frozen!=null && (!Objects.equals(frozen.current(),proposedCurrent) || !Objects.equals(frozen.metadata(),proposedMetadata)))throw new IOException("Recovery retry changed frozen native recipe");
                var existing=stores.get(request.sourceGroup());var machine=machines.get(RaftGroupId.valueOf(request.sourceGroup()));
                boolean registeredMetadata=request.metadataOnly() && existing!=null && existing.metadataRecoveryRegistrationForced(proposedMetadata);
                if(request.metadataOnly() && metadata==null && existing!=null && !registeredMetadata){
                    if(existing.currentRangeRecoveryRegistration()!=null && current==null || current!=null && frozen==null && existing.currentRangeRecoveryReady() && !existing.currentRangeRecoveryImageInstalled() || existing.activeSnapshotPins()!=0 || existing.applied()!=null && !RatisRelocationOwnerRpc.covers(prepared.image().position(),existing.applied()))throw new IOException("Existing META source has busy or newer native prefix; recapture first");
                    if(machine!=null){machine.requireRecoveryRegistrationDrained();machine.requireMetadataRecoveryCoverage(proposedMetadata);}
                }
                boolean changedMetadata=proposedMetadata!=null && !proposedMetadata.equals(metadata);
                if(request.metadataOnly() && machine!=null && !registeredMetadata && (changedMetadata || frozen!=null)){machine.requireRecoveryRegistrationDrained();machine.qualifyMetadataRecoveryFloor(proposedMetadata);}
                if(frozen==null && (proposedMetadata!=null?changedMetadata:current==null)) {
                    int bytes=proposedCurrent!=null?proposedCurrent.encodedSize():proposedMetadata.encodedSize(),previousBytes=metadata!=null?metadata.encodedSize():current!=null?current.encodedSize():0;long originalCredit=owner.recoveryMetadata==null?0:owner.recoveryMetadata.bytes();boolean saved=false;
                    try {
                        long retained=4096L+16L*Math.addExact(owner.recoveryEncodedBytes,bytes);if(owner.recoveryMetadata==null)owner.recoveryMetadata=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,retained,1);else owner.recoveryMetadata.growTo(Math.max(owner.recoveryMetadata.bytes(),retained));
                        metadataClaims.reserve(List.of());long claim=metadataClaims.contains(request.sourceGroup())?0:metadataClaim(owner.spec,request.sourceGroup(),RatisNodePlan.Role.DATA).bytes();
                        long exposure=previousBytes==0?224:0;long total=metadataClaims.bytes()+claim+exposure;for(var pool:pools.values())total=Math.addExact(total,pool.recoveryExposureBytes);
                        if(total>settings.resources().maximumDurableMetadataBytes() || metadataClaims.bytes(owner.spec.id())+claim+owner.recoveryExposureBytes+exposure>owner.spec.maximumDurableMetadataBytes())throw new IOException("Recovery registration durable metadata/exposure ceiling exhausted");
                        reserveDataMetadata(owner,List.of(request.sourceGroup()));
                        if(machine!=null && request.metadataOnly() && !registeredMetadata){
                            machine.pauseRecoveryRegistration(prepared.image().position());machine.awaitNativeReaders(settings.shutdownTimeout());
                            machine.requireMetadataRecoveryCoverage(proposedMetadata);
                            if(existing.activeSnapshotPins()!=0 || existing.applied()!=null && !RatisRelocationOwnerRpc.covers(prepared.image().position(),existing.applied()))throw new IOException("Drained META source requires a newer image or original native owner release");
                        }
                        owner.recoveryExposureBytes+=exposure;recoveryAttempt=new RecoveryAttempt(owner.spec.id(),request.sourceGroup(),digest,proposedCurrent,proposedMetadata,previousBytes);saved=true;
                    }finally{if(!saved){if(originalCredit==0){if(owner.recoveryMetadata!=null)owner.recoveryMetadata.close();owner.recoveryMetadata=null;}else owner.recoveryMetadata.shrinkTo(originalCredit,1);}}
                }
                if(machine!=null && request.metadataOnly() && !registeredMetadata && (changedMetadata || recoveryAttempt!=null)) {machine.requireRecoveryRegistrationDrained();machine.pauseRecoveryRegistration(prepared.image().position());machine.awaitNativeReaders(settings.shutdownTimeout());machine.requireRecoveryRegistrationDrained();}
                var store=proposedCurrent!=null?owner.pool.openCurrentRangeRecovery(proposedCurrent):owner.pool.openMetadataRecovery(proposedMetadata);
                assignPool(request.sourceGroup(),owner.spec.id());if(proposedCurrent!=null)groups.register(proposedCurrent);else groups.register(proposedMetadata);
                var entry=groups.require(request.sourceGroup());var id=entry.group().getGroupId();
                if(machine==null){
                    machine=RatisRangeStateMachine.withSessions(store,entry.group(),owner.images(),command->{},epochs,entry.directory(),owner.retention());machine.nodeResources(resources,owner.spec.id());machine.logResources(logBudget(request.sourceGroup(),owner.spec.id()));machine.diskRunway(diskRunway,owner.spec.id());machine.committedApply(committedDispatcher,owner.spec.id(),MAX_COMMITTED_ENTRY_BYTES);configureDataMachine(machine,entry.directory(),entry.descriptor());machine.learnerInstaller(topologyImageQueries,this::joinTopologyLearner);machine.learnerRecoveryInstaller(this::prepareLearnerRecovery);machine.proofQueries(proofQueries);machine.exportSources(exportSources);stores.put(request.sourceGroup(),store);machines.put(id,machine);
                }
                if(recoveryAttempt!=null)virginSeedAdds.add(request.sourceGroup());requireSeedStorage(entry);markSeedExposure(entry);var actual=new HashSet<RaftGroupId>();server.getGroupIds().forEach(actual::add);if(!actual.contains(id))addGroup(entry.group());else {if(machine.getLifeCycleState()==org.apache.ratis.util.LifeCycle.State.PAUSED)machine.resumeRecoveryRegistration();verify(entry.group());}
                virginSeedAdds.remove(request.sourceGroup());if(recoveryAttempt!=null){owner.recoveryEncodedBytes=Math.addExact(owner.recoveryEncodedBytes-recoveryAttempt.previousBytes(),proposedCurrent!=null?proposedCurrent.encodedSize():proposedMetadata.encodedSize());owner.recoveryMetadata.shrinkTo(4096L+16L*owner.recoveryEncodedBytes,1);}recoveryAttempt=null;
                if(proposedMetadata!=null && proposedMetadata.currentPredecessor()!=null && store.metadataRecoveryRegistrationForced(proposedMetadata))recoveryCompletionAttempts.remove(request.sourceGroup());
                allowGraphRegisteredRecovery(prepared,machine,authority.directory());return store;
            }
        }catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException("Configured native recovery registration failed",failure);}finally{endSeedLifecycle();}
    }
    /** Settle only the already admitted original control force; no obsolete CURRENT receive authority is published. */
    private void settleFrozenCurrentRegistration(PoolOwner owner,RatisTopologySourceSnapshotRpc.Prepared next,RatisNodeResourceBudget.Lease workspace)throws IOException {
        var frozen=recoveryAttempt;var previous=frozen.current();var request=next.request();
        if(previous==null || !request.metadataOnly() || !frozen.pool().equals(owner.spec.id()) || !frozen.group().equals(request.sourceGroup()))throw new IOException("META cannot settle another frozen registration owner");
        workspace.growTo(Math.max(workspace.bytes(),4096L+16L*Math.addExact(next.encodedSize(),previous.encodedSize())));
        try {
            var transition=new NativeMetadataRecoveryRegistration(request.transferId(),request.catalogue(),previous.catalogueCut(),request.sourceDirectory(),next.metadataFreeze(),request.receiver(),next.sourceOwner(),next.sourceCfg().term(),next.sourceCfg().index(),next.sourceCfg().data(),next.image().position(),next.image().sha256(),next.image().deterministicMergeColumns(),NativeMetadataRecoveryRegistration.CurrentPredecessor.of(previous));
            transition.requireTransitionOf(previous);
            var machine=machines.get(RaftGroupId.valueOf(previous.groupId()));
            if(machine!=null && !machine.recoveryEligibilityGate().discoveryPending())throw new IOException("Frozen CURRENT receiver must remain discovery-fenced during control settlement");
            long retained=Math.addExact(Math.subtractExact(owner.recoveryEncodedBytes,frozen.previousBytes()),previous.encodedSize());
            if(retained<0 || owner.recoveryMetadata==null)throw new IOException("Frozen CURRENT registration lost its retained metadata claim");
            reserveDataMetadata(owner,List.of(previous.groupId()));
            // The original native API retains pendingCurrentRecovery and forces this exact recipe
            // again after a lost ACK. No fresh placement proof, image import or division exposure occurs.
            var store=owner.pool.openCurrentRangeRecovery(previous);
            if(!owner.pool.currentRangeRecoveries().contains(previous) || !previous.equals(store.currentRangeRecoveryRegistration()) || store.admissionOpen())throw new IOException("Original CURRENT control force has no exact fenced completion");
            store.deferDiscovery();
            if(recoveryAttempt!=frozen)throw new IOException("Frozen CURRENT registration ownership changed during settlement");
            owner.recoveryEncodedBytes=retained;owner.recoveryMetadata.shrinkTo(4096L+16L*retained,1);recoveryAttempt=null;
        }catch(org.rocksdb.RocksDBException|IllegalArgumentException invalid){throw new IOException("Original CURRENT control force remains unresolved",invalid);}
    }
    private RatisConfiguredRecoveryGraph.Snapshot requireGraphRegistrationRequest(RatisTopologySourceSnapshotRpc.Request request,RangeDirectory currentDirectory)throws IOException {
        var frozen=recoveryGraph.snapshot();if(frozen==null)return null;
        var active=frozen.activeRequests().get(request.sourceGroup());if(active==null)throw new IOException("Recovery registration has no forced active request");
        try{RatisTopologySourceSnapshotRpc.requireRequest(active,request);}catch(IllegalArgumentException stale){throw new IOException("Recovery registration differs from forced active attempt",stale);}
        if(frozen.plan().roots().stream().noneMatch(root->root.directory().domainId().equals(request.domainId()) && Arrays.equals(root.directory().payloadDigest(),currentDirectory.payloadDigest())))throw new IOException("Recovery registration lacks freshly qualified CURRENT graph");
        return frozen;
    }
    private void allowGraphRegisteredRecovery(RatisTopologySourceSnapshotRpc.Prepared prepared,RatisRangeStateMachine machine,RangeDirectory currentDirectory)throws IOException {
        if(!machine.recoveryEligibilityGate().discoveryPending())return;
        var request=prepared.request();var frozen=requireGraphRegistrationRequest(request,currentDirectory);if(frozen==null)throw new IOException("Registered receive permission requires a forced active recovery graph");
        var obligation=frozen.plan().localObligations().stream().filter(value->value.range().groupId().equals(request.sourceGroup())).findFirst().orElseThrow(()->new IOException("Registered receive permission has no local obligation"));
        requireLocalRecoveryGraphOwner(frozen,obligation,machine);
        machine.allowRegisteredRecovery(prepared,publication->{
            synchronized(this){if(closeRequested || closing || closed || recoveryGraph.snapshot()!=frozen)throw new IOException("Registered receive graph owner changed");publication.publish();}
        });
    }
    /** Trusted actor seam: both values come from actual transport and are journaled before dispatch.
     * False means no completion force was attempted; the actor may obtain a newer source head. */
    public boolean completeRecovery(RatisTopologySourceSnapshotRpc.Prepared prepared,RatisSourceHeadRpc.Head frozenHead)throws IOException {
        Objects.requireNonNull(prepared);Objects.requireNonNull(frozenHead);var scope=prepared.request();
        synchronized(this){if(!started || server==null || installing || starting || closing || closeRequested || closed || recoveryAttempt!=null || supersessionAttempt!=null || learnerAttempt!=null || seedAttempt!=null)throw new IOException("Recovery completion node owner unavailable");installing=true;}
        try {
            if(scope.relocation() || !scope.catalogue().equals(plan.catalogue().groupId()) || !scope.clusterId().equals(plan.clusterId()) || !scope.databaseId().equals(plan.databaseId()))throw new IOException("Recovery completion namespace/purpose differs");
            var receiver=scope.receiver();if(!receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation()) || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address()))throw new IOException("Recovery completion receiver identity differs");
            RatisTopologySourceSnapshotRpc.requireRequest(scope,frozenHead.request().source());if(!prepared.image().position().equals(frozenHead.request().minimumImageCut()))throw new IOException("Recovery completion changed original image cut");
            var owner=owner(scope.sourceGroup());if(!owner.spec.id().equals(plan.incomingPoolId(scope.domainId(),scope.sourceGroup())))throw new IOException("Recovery completion changed configured pool");
            int raw=Math.addExact(prepared.encodedSize(),RatisSourceHeadRpc.encodedReplyBytes(frozenHead));
            try(var workspace=resources.reserve(owner.spec.id(),RatisNodeResourceBudget.Kind.PROTECTED,4096L+16L*raw,1)){
                var store=stores.get(scope.sourceGroup());var machine=machines.get(RaftGroupId.valueOf(scope.sourceGroup()));if(store==null || machine==null)throw new IOException("Recovery completion has no forced local registration");
                if(scope.metadataOnly()){
                    var saved=store.metadataRecoveryRegistration();if(saved==null)throw new IOException("Recovery completion has no META registration");var expected=new NativeMetadataRecoveryRegistration(scope.transferId(),scope.catalogue(),saved.catalogueCut(),scope.sourceDirectory(),prepared.metadataFreeze(),receiver,prepared.sourceOwner(),prepared.sourceCfg().term(),prepared.sourceCfg().index(),prepared.sourceCfg().data(),prepared.image().position(),prepared.image().sha256(),prepared.image().deterministicMergeColumns(),saved.currentPredecessor());if(!saved.equals(expected))throw new IOException("META completion changed original Prepared recipe");
                }else{
                    var saved=store.currentRangeRecoveryRegistration();if(saved==null)throw new IOException("Recovery completion has no CURRENT registration");var expected=new NativeCurrentRangeRecovery(scope.transferId(),scope.catalogue(),saved.catalogueCut(),scope.sourceDirectory(),prepared.schema(),scope.sourceGroup(),receiver,prepared.sourceOwner(),prepared.image().position(),prepared.image().sha256(),prepared.sourceCfg().term(),prepared.sourceCfg().index(),prepared.sourceCfg().data(),prepared.image().topologyOrigin(),saved.physicalNames(),saved.mergeColumns(),prepared.schemaRows());if(!saved.equals(expected))throw new IOException("CURRENT completion changed original Prepared recipe");
                }
                byte[] identity=OperatorUpload.sha256(ByteBuffer.allocate(64).put(OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,prepared))).put(OperatorUpload.sha256(RatisSourceHeadRpc.reply(frozenHead))).array());var pending=recoveryCompletionAttempts.get(scope.sourceGroup());if(pending!=null && !Arrays.equals(pending,identity))throw new IOException("Unknown recovery completion requires exact frozen head retry");
                if(pending==null && store.recoveryCompletionRecorded(frozenHead.completedCut(),frozenHead.configuration())){recoveryCompletionAttempts.remove(scope.sourceGroup());return true;}
                var catalogue=epochs.catalogue(plan.catalogue().directoryColumn());var current=catalogue.readView(scope.domainId());if(current.directory()==null)throw new IOException("Recovery completion domain missing; rediscover placement");var fresh=relocationClients.head(frozenHead.request(),bytes->workspace.growTo(4096L+16L*Math.addExact(raw,bytes)));
                if(!Arrays.equals(fresh.currentDirectory().payloadDigest(),current.directory().payloadDigest()) || !RatisRelocationOwnerRpc.covers(fresh.catalogueCut(),current.cut()) || !RatisRelocationOwnerRpc.covers(fresh.catalogueCut(),frozenHead.catalogueCut()) || !RatisRelocationOwnerRpc.covers(fresh.completedCut(),frozenHead.completedCut()))throw new IOException("Recovery completion authority regressed or changed during proof");
                if(scope.metadataOnly()){
                    var definition=epochs.topologyCatalogue(plan.catalogue().directoryColumn()).readDefinition(scope.header().definitionDigest());if(definition==null)throw new IOException("Recovery completion original META definition absent");scope.header().requireDefinition(definition);var target=definition.targets().getFirst();var birth=catalogue.readRecoveryBirth(scope.domainId(),target.rangeId(),target.groupId(),target.rangeEpoch(),fresh.catalogueCut());if(birth.birth()==null || !Arrays.equals(birth.birth().encode(),definition.encode()))throw new IOException("Recovery completion META publication changed");
                }else{
                    var frozenRange=RatisSourceHeadRpc.requireContinuity(scope,frozenHead.currentDirectory());var freshRange=RatisSourceHeadRpc.requireContinuity(scope,fresh.currentDirectory());if(!frozenRange.equals(freshRange))throw new IOException("Recovery completion configured cohort changed; rediscover placement");
                    if(fresh.currentDirectory().schemaEpoch()<frozenHead.currentDirectory().schemaEpoch() || fresh.currentDirectory().generation()<frozenHead.currentDirectory().generation())throw new IOException("Recovery completion placement regressed");
                    var birth=catalogue.readRecoveryBirth(scope.domainId(),scope.range().rangeId(),scope.sourceGroup(),scope.rangeEpoch(),fresh.catalogueCut());if(birth.birth()!=null){if(prepared.image().topologyOrigin()==null)throw new IOException("Recovery completion born group lost image origin");prepared.image().topologyOrigin().seed().header().requireDefinition(birth.birth());}else if(prepared.image().topologyOrigin()!=null || plan.directory(scope.domainId()).ranges().stream().noneMatch(range->range.rangeId().equals(scope.range().rangeId()) && range.groupId().equals(scope.sourceGroup()) && range.rangeEpoch()==scope.rangeEpoch()))throw new IOException("Recovery completion lacks immutable ordinary bootstrap witness");
                }
                boolean complete=machine.completeRecoveryRegistration(prepared,frozenHead,pending!=null,()->recoveryCompletionAttempts.put(scope.sourceGroup(),identity));if(!complete){if(pending!=null)throw new IOException("Unknown completion retry is not currently ready; frozen head remains required");return false;}
                if(!scope.metadataOnly()){
                    var route=data(scope.domainId());var target=frozenHead.currentDirectory();if(route.directory().schemaEpoch()<=target.schemaEpoch() && !Arrays.equals(route.directory().schemaDigest(),target.schemaDigest()))for(var activation:store.activatedSchemas())observeSchemaActivation(activation.definition(),activation.proof());
                    if(route.directory().generation()<=target.generation())route.refresh(target);
                }
                recoveryCompletionAttempts.remove(scope.sourceGroup());return true;
            }
        }catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException("Recovery completion failed; exact frozen head remains required after force",failure);}finally{endSeedLifecycle();}
    }
    /** Acknowledges the forced original request; source and native work continue under node ownership. */
    public CompletableFuture<Void> recoverConfigured(RatisTopologySourceSnapshotRpc.Request request){
        final CompletableFuture<Void> accepted;
        synchronized(this){if(!started || starting || closeRequested || closing || closed)return CompletableFuture.failedFuture(new IOException("Configured recovery node unavailable"));accepted=configuredRecoveries.enqueue(request);}
        return accepted.isCompletedExceptionally()?accepted:accepted.thenCombine(startConfiguredRecoveryService(),(ignored,started)->null);
    }
    /** Freezes the discovered graph on owned IO; it grants neither dispatch nor DATA startup permission. */
    CompletableFuture<Void> discoverConfiguredRecovery() {
        synchronized(this){if(!started || starting || installing || closing || closeRequested || closed)return CompletableFuture.failedFuture(new IOException("Discovery node owner unavailable"));}
        return configuredRecoveries.discover(()->{
            synchronized(this){if(!started || starting || installing || closing || closeRequested || closed)throw new IOException("Discovery node owner unavailable");installing=true;}
            try {
                if(recoveryGraph.retryPending())return null;
                freezeFreshRecoveryGraph();return null;
            }finally{try{shrinkRecoveryGraphCredit();}finally{endSeedLifecycle();}}
        });
    }
    /** Fresh CAT and the forced active selection precede any local role settlement. */
    CompletableFuture<Void> bootstrapDeferredRecovery() {
        synchronized(this){if(!started || starting || installing || closing || closeRequested || closed)return CompletableFuture.failedFuture(new IOException("Discovery bootstrap node unavailable"));}
        return configuredRecoveries.discover(()->{
            synchronized(this){if(!started || starting || installing || closing || closeRequested || closed || recoveryAttempt!=null || supersessionAttempt!=null || seedAttempt!=null || learnerAttempt!=null)throw new IOException("Discovery bootstrap owner unavailable");installing=true;}
            try {
                recoveryGraph.retryPending();forcePendingGenesisIdentity();
                if(needsGenesisObservation())try(var workspace=resources.reserve(metadataClaims.graphPool(),RatisNodeResourceBudget.Kind.PROTECTED,4096,1)) {
                    boolean complete=relocationClients.discoverGenesis(plan,(retained,incoming)->workspace.growTo(Math.max(workspace.bytes(),4096L+16L*incoming)),this::observeGenesisDiscovery);
                    if(!complete)return null;
                }
                var frozen=freezeFreshRecoveryGraph();
                var selections=new HashMap<UUID,RatisConfiguredRecoveryDiscovery.Obligation>();for(var obligation:frozen.plan().localObligations())selections.put(obligation.range().groupId(),obligation);
                for(var local:machines.values())if(local.genesisActive()) {
                    var selected=selections.get(local.getGroupId().getUuid());
                    if(selected==null || selected.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF || !local.matchesGenesis(selected.sourceDirectory()))local.abandonGenesis(this::publishLocalDiscovery);
                }
                settleLocalNoneDiscovery(frozen);
                for(var obligation:frozen.plan().localObligations()) {
                    var machine=machines.get(RaftGroupId.valueOf(obligation.range().groupId()));
                    if(machine==null || !machine.recoveryEligibilityGate().discoveryPending() && !machine.genesisActive())continue;
                    requireLocalRecoveryGraphOwner(frozen,obligation,machine);
                    if(machine.genesisActive()) {
                        if(obligation.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF || nodeIdentity.genesisOpen(obligation.sourceDirectory().domainId()) || genesisIdentityForcePending)throw new IOException("Genesis completion lacks forced published CURRENT authority");
                        if(!machine.completeGenesis(obligation.sourceDirectory(),this::publishLocalDiscovery))throw new IOException("Genesis metadata prefix has not reached its actual updater");continue;
                    }
                    if(obligation.purpose()==RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA) {
                        var consuming=frozen.plan().definitions().stream().filter(definition->Arrays.equals(definition.digest(),obligation.header().definitionDigest())).findFirst().orElseThrow(()->new IOException("Local META consuming definition absent"));
                        var candidate=machine.prepareLocalMetadataDiscovery(frozen.plan().catalogueCut(),consuming);
                        awaitLocalRecoveryReload(machine.reloadLocalMetadataDiscovery(candidate));
                        requireLocalRecoveryGraphOwner(frozen,obligation,machine);
                        if(!machine.settleLocalMetadataDiscovery(candidate,this::publishLocalDiscovery))throw new IOException("Local META discovery did not settle");
                    }else if(obligation.purpose()==RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF) {
                        try {
                            var candidate=machine.prepareLocalCurrentDiscovery(frozen.plan().catalogueCut(),obligation.sourceDirectory());
                            awaitLocalRecoveryReload(machine.reloadLocalCurrentDiscovery(candidate));
                            requireLocalRecoveryGraphOwner(frozen,obligation,machine);
                            if(!machine.settleLocalCurrentDiscovery(candidate,this::publishLocalDiscovery))throw new IOException("Local CURRENT discovery did not settle");
                            var route=data(obligation.sourceDirectory().domainId());var target=obligation.sourceDirectory();
                            if(route.directory().schemaEpoch()<=target.schemaEpoch() && !Arrays.equals(route.directory().schemaDigest(),target.schemaDigest()))for(var activation:stores.get(obligation.range().groupId()).activatedSchemas())observeSchemaActivation(activation.definition(),activation.proof());
                            if(route.directory().generation()<=target.generation())route.refresh(target);
                        }catch(Exception|Error failure) {
                            boolean stopping;synchronized(this){stopping=closeRequested || closing || closed;}
                            if(stopping)try{awaitLocalRecoveryReload(machine.abortLocalCurrentDiscovery());}catch(Exception|Error cleanup){failure.addSuppressed(cleanup);}
                            throw failure;
                        }
                    }
                }
                return null;
            }finally{try{shrinkRecoveryGraphCredit();}finally{endSeedLifecycle();}}
        });
    }
    private void publishLocalDiscovery(RatisRangeStateMachine.LocalDiscoveryPublication publication)throws IOException {
        synchronized(this){if(closeRequested || closing || closed)throw new IOException("Node close prevents discovery settlement");publication.publish();}
    }
    private RatisConfiguredRecoveryGraph.Snapshot freezeFreshRecoveryGraph()throws IOException {
        localNoneDiscoveries=Map.of();forcePendingGenesisIdentity();
        try(var workspace=resources.reserve(metadataClaims.graphPool(),RatisNodeResourceBudget.Kind.PROTECTED,4096,1)){
            var discovered=relocationClients.discover(plan,(retained,incoming)->workspace.growTo(Math.max(workspace.bytes(),Math.addExact(4096L,Math.multiplyExact(16L,Math.addExact(retained,incoming))))),this::observeGenesisIssuance);
            var requests=new ArrayList<RatisTopologySourceSnapshotRpc.Request>();var retained=recoveryGraph.snapshot();
            for(var obligation:discovered.localObligations()){
                RatisTopologySourceSnapshotRpc.Request candidate=null;
                if(retained!=null){var previous=retained.activeRequests().get(obligation.range().groupId());if(previous!=null && matchesDiscoveredRequest(previous,obligation))candidate=previous;}
                if(candidate==null)candidate=new RatisTopologySourceSnapshotRpc.Request(UUID.randomUUID(),obligation.header(),plan.catalogue().groupId(),obligation.localReplica(),obligation.purpose(),obligation.range().groupId(),obligation.sourceDirectory());
                requests.add(candidate);
            }
            recoveryGraph.force(discovered,requests);return recoveryGraph.snapshot();
        }
    }
    private void observeGenesisIssuance(RatisCatalogueViewRpc.View view)throws IOException {
        if(nodeIdentity.purpose()!=RatisNodeIdentity.Purpose.GENESIS)return;
        var domain=view.request().domain();if(!nodeIdentity.genesisDomains().contains(domain))throw new IOException("Fresh CAT observation crossed original genesis scope");
        var issued=new HashSet<UUID>();if(view.directory()!=null)issued.add(domain);
        if(view.roster()!=null)for(var original:nodeIdentity.genesisDomains())if(view.roster().domainIds().contains(original))issued.add(original);
        for(var original:issued)for(var range:plan.directory(original).ranges()){var machine=machines.get(RaftGroupId.valueOf(range.groupId()));if(machine!=null)machine.suspendGenesis();}
        var closed=nodeIdentity.closeGenesis(issued);if(closed!=nodeIdentity){nodeIdentity=closed;genesisIdentityForcePending=true;forcePendingGenesisIdentity();}
    }
    private boolean needsGenesisObservation(){return nodeIdentity.purpose()==RatisNodeIdentity.Purpose.GENESIS && (nodeIdentity.genesisDomains().stream().anyMatch(nodeIdentity::genesisOpen) || machines.values().stream().anyMatch(RatisRangeStateMachine::genesisActive));}
    private void observeGenesisDiscovery(RatisCatalogueViewRpc.View view)throws IOException {
        observeGenesisIssuance(view);var domain=view.request().domain();
        if(view.directory()!=null || nodeIdentity.purpose()!=RatisNodeIdentity.Purpose.GENESIS || !nodeIdentity.genesisOpen(domain) || genesisIdentityForcePending)return;
        var directory=plan.directory(domain);
        for(var range:directory.ranges()) {
            var machine=machines.get(RaftGroupId.valueOf(range.groupId()));if(machine==null || machine.genesisActive())continue;
            nodeIdentity.requireGenesisScope(directory,range,ownReplica(range));
            if(!machine.authorizeGenesis(directory,this::publishLocalDiscovery))throw new IOException("Original genesis metadata prefix is not qualified yet");
        }
    }
    private void forcePendingGenesisIdentity()throws IOException {
        if(!genesisIdentityForcePending)return;
        identityJournal.write(root.resolve("node.identity"),nodeIdentity.encode(),NODE_IDENTITY_BYTES);genesisIdentityForcePending=false;
    }
    private void settleLocalNoneDiscovery(RatisConfiguredRecoveryGraph.Snapshot frozen)throws IOException {
        var obligated=new HashSet<UUID>();for(var obligation:frozen.plan().localObligations())obligated.add(obligation.range().groupId());
        var selected=new HashMap<UUID,RatisLocalNoneDiscovery>();
        for(var local:machines.entrySet()) {
            var id=local.getKey();var machine=local.getValue();var entry=groups.require(id.getUuid());var store=stores.get(id.getUuid());
            if(entry.role()!=RatisNodePlan.Role.DATA || obligated.contains(id.getUuid()) || machine==null || !machine.recoveryEligibilityGate().discoveryPending())continue;
            if(store==null || !pools.containsKey(groupPools.get(id.getUuid())))throw new IOException("NONE lost registered native pool ownership");
            awaitLocalRecoveryReload(machine.abandonLocalDiscovery());
            long required=4096L+1024L*(selected.size()+1);
            if(localNoneCredit==null)localNoneCredit=resources.reserve(metadataClaims.graphPool(),RatisNodeResourceBudget.Kind.PROTECTED,required,1);
            else localNoneCredit.growTo(Math.max(localNoneCredit.bytes(),required));
            selected.put(id.getUuid(),new RatisLocalNoneDiscovery(frozen,entry,store,machine,server.getDivision(id),plan.catalogue().groupId()));
        }
        var publication=Map.copyOf(selected);
        synchronized(this) {
            if(closeRequested || closing || closed || recoveryGraph.snapshot()!=frozen)throw new IOException("NONE lost fresh graph/node ownership before publication");
            for(var entry:publication.entrySet()) {
                var id=RaftGroupId.valueOf(entry.getKey());var machine=machines.get(id);
                if(!entry.getValue().current(frozen,groups.require(entry.getKey()),stores.get(entry.getKey()),machine,server.getDivision(id),machine.readinessStamp()))throw new IOException("NONE local owner changed before publication");
            }
            localNoneDiscoveries=publication;
        }
    }
    private void requireLocalRecoveryGraphOwner(RatisConfiguredRecoveryGraph.Snapshot frozen,RatisConfiguredRecoveryDiscovery.Obligation obligation,RatisRangeStateMachine machine)throws IOException {
        var group=obligation.range().groupId();var request=frozen.activeRequests().get(group);var id=RaftGroupId.valueOf(group);var entry=groups.require(group);var store=stores.get(group);var pool=groupPools.get(group);
        if(recoveryGraph.snapshot()!=frozen || request==null || !matchesDiscoveredRequest(request,obligation) || !request.catalogue().equals(plan.catalogue().groupId()) || !ownReplica(obligation.range()).equals(obligation.localReplica())
                || entry.role()!=RatisNodePlan.Role.DATA || pool==null || !pools.containsKey(pool) || store==null || !group.equals(store.ownership().owner().groupId()) || !obligation.sourceDirectory().domainId().equals(store.ownership().interval().domainId())
                || entry.descriptor()==null || !entry.descriptor().rangeId().equals(obligation.range().rangeId()) || store.ownership().owner().epoch()!=obligation.range().rangeEpoch()
                || !Arrays.equals(store.ownership().interval().lowerInclusive(),obligation.range().lowerInclusive()) || !Arrays.equals(store.ownership().interval().upperExclusive(),obligation.range().upperExclusive())
                || obligation.purpose()==RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA && !Arrays.equals(entry.directory().payloadDigest(),obligation.sourceDirectory().payloadDigest())
                || machines.get(id)!=machine || server.getDivision(id).getStateMachine()!=machine)
            throw new IOException("Local discovery bootstrap lost exact forced graph/native/division ownership");
    }
    private static void awaitLocalRecoveryReload(CompletableFuture<Void> actual)throws IOException {
        boolean interrupted=false;
        try {
            for(;;)try{actual.get();return;}catch(InterruptedException wait){interrupted=true;}catch(java.util.concurrent.ExecutionException failure){throw new IOException("Local discovery actual reload failed",failure.getCause());}
        }finally{if(interrupted)Thread.currentThread().interrupt();}
    }
    private static boolean matchesDiscoveredRequest(RatisTopologySourceSnapshotRpc.Request request,RatisConfiguredRecoveryDiscovery.Obligation obligation){
        // Both inputs were validated against canonical definitions; compare their cached scope digests without serializing directories.
        return request.purpose()==obligation.purpose() && request.sourceGroup().equals(obligation.range().groupId()) && request.receiver().equals(obligation.localReplica()) && Arrays.equals(request.sourceDirectory().payloadDigest(),obligation.sourceDirectory().payloadDigest())
                && (request.header()==null?obligation.header()==null:obligation.header()!=null && Arrays.equals(request.header().definitionDigest(),obligation.header().definitionDigest()));
    }
    private void shrinkRecoveryGraphCredit(){if(recoveryGraphCredit!=null && recoveryGraph!=null)recoveryGraphCredit.shrinkTo(Math.addExact(4096L,Math.multiplyExact(16L,recoveryGraph.retainedEncodedBytes())),1);}
    private RatisJournalAccess recoveryGraphJournal(){
        Path main=root.resolve("configured-recovery-graph/graph").toAbsolutePath().normalize(),pending=main.resolveSibling("graph.pending");UUID pool=metadataClaims.graphPool();
        return new RatisJournalAccess(){
            private void require(Path path,int maximum,boolean sidecar)throws IOException{var actual=path.toAbsolutePath().normalize();if(maximum!=RatisConfiguredRecoveryGraph.MAX_BYTES || !actual.equals(main) && !(sidecar && actual.equals(pending)))throw new IOException("Recovery graph file escaped paid scope");requireNoSymlinks(actual);}
            public RootLease backendRoot(Path path)throws IOException{throw new IOException("Recovery graph is not a backend root");}
            public void register(Path path,int maximum)throws IOException{require(path,maximum,false);}
            public byte[] read(Path path,int maximum)throws IOException{require(path,maximum,true);return SessionJournalFiles.read(path,maximum);}
            public void write(Path path,byte[] body,int maximum)throws IOException{require(path,maximum,false);if(body.length>maximum-32)throw new IOException("Recovery graph exceeds paid frame");var runway=diskRunway.reserveRaftAppend(pool,body.length+32L,true);try{SessionJournalFiles.write(path,body,maximum);}finally{runway.actualFinished();}}
            public void remove(Path path,byte[] body,int maximum,SessionJournalFiles.Removal removal)throws IOException{throw new IOException("Recovery graph reclamation requires separate authority");}
            public void deleted(Path path)throws IOException{require(path,RatisConfiguredRecoveryGraph.MAX_BYTES,true);if(!path.toAbsolutePath().normalize().equals(pending))throw new IOException("Recovery graph promise remains retained");}
            public void reclaimedTree(Path path)throws IOException{throw new IOException("Recovery graph promise remains retained");}
            public void reserve(Reservation reservation)throws IOException{require(reservation.path(),reservation.maximum(),false);}
            public void fulfilled(Reservation reservation)throws IOException{require(reservation.path(),reservation.maximum(),false);}
        };
    }
    private boolean needsRecoveryDiscovery(){return needsGenesisObservation() || machines.values().stream().anyMatch(machine->machine.recoveryEligibilityGate().discoveryPending());}
    private void configuredRecoveryCycle() {
        if(!configuredRecoveries.idle())return;
        final boolean discover;
        synchronized(this){if(!started || starting || installing || closeRequested || closing || closed || !configuredRecoveries.idle())return;discover=needsRecoveryDiscovery() && (configuredRecoveries.size()==0 || configuredDiscoveryNext);configuredDiscoveryNext=!discover;}
        if(discover)bootstrapDeferredRecovery();else configuredRecoveries.cycle();
    }
    private CompletableFuture<Void> startConfiguredRecoveryService(){
        final CompletableFuture<Void> actual;
        synchronized(this){if(configuredRecoveryServiceStarted)return configuredRecoveryServiceStart.copy();if(closeRequested || closing || closed)return CompletableFuture.failedFuture(new IOException("Configured recovery service owner is stopping"));configuredRecoveryServiceStarted=true;configuredRecoveryServiceStart=actual=new CompletableFuture<>();}
        try{configuredRecoveryService.start(timer,Duration.ofSeconds(1));actual.complete(null);}catch(RuntimeException failed){synchronized(this){configuredRecoveryServiceStarted=false;}actual.completeExceptionally(failed);}
        return actual.copy();
    }
    RatisConfiguredRecovery.Phase configuredRecoveryPhase(UUID group){return configuredRecoveries.phase(group);}
    public Throwable configuredRecoveryFailure(){return configuredRecoveries==null?null:configuredRecoveries.failure();}
    private UUID configuredRecoveryPool(RatisTopologySourceSnapshotRpc.Request request)throws IOException {
        var receiver=request.receiver();
        if(request.relocation() || !request.catalogue().equals(plan.catalogue().groupId()) || !request.clusterId().equals(plan.clusterId()) || !request.databaseId().equals(plan.databaseId()) || !receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation()) || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address()))throw new IOException("Configured recovery namespace/receiver differs from this node");
        UUID pool=plan.incomingPoolId(request.domainId(),request.sourceGroup());var owner=pools.get(pool);if(owner==null)throw new IOException("Configured recovery has no declared native pool");requireRecoveryPool(owner,request.sourceDirectory());return pool;
    }
    private RatisJournalAccess configuredRecoveryJournal(UUID pool,UUID group,Path directory)throws IOException {
        var owner=pools.get(pool);Path expected=root.resolve("configured-recovery").resolve(pool.toString()).resolve(group.toString()).toAbsolutePath().normalize();
        if(owner==null || !expected.equals(directory.toAbsolutePath().normalize()))throw new IOException("Configured recovery journal changed paid slot");requireNoSymlinks(expected);
        synchronized(this){
            if(installing || starting || closing || closeRequested || closed)throw new IOException("Configured recovery journal node owner unavailable");
            metadataClaims.reserve(List.of());var claim=metadataClaim(owner.spec,group,RatisNodePlan.Role.DATA);long extra=metadataClaims.contains(group)?0:claim.bytes();long total=Math.addExact(metadataClaims.bytes(),extra);for(var retained:pools.values())total=Math.addExact(total,retained.recoveryExposureBytes);
            if(total>settings.resources().maximumDurableMetadataBytes() || metadataClaims.bytes(pool)+extra+owner.recoveryExposureBytes>owner.spec.maximumDurableMetadataBytes())throw new IOException("Configured recovery durable metadata ceiling exhausted");metadataClaims.reserve(List.of(claim));
        }
        return new RatisJournalAccess(){
            private final Path main=expected.resolve("recovery"),pending=expected.resolve("recovery.pending");
            private void require(Path path,int maximum,boolean sidecar)throws IOException {var actual=path.toAbsolutePath().normalize();if(maximum!=RatisSchemaClientJournal.MAX_BYTES || !actual.equals(main) && !(sidecar && actual.equals(pending)))throw new IOException("Configured recovery file escaped its paid slot");requireNoSymlinks(actual);}
            public RootLease backendRoot(Path path)throws IOException{throw new IOException("Configured recovery slot is not a backend root");}
            public void register(Path path,int maximum)throws IOException{require(path,maximum,false);}
            public byte[] read(Path path,int maximum)throws IOException{require(path,maximum,true);return SessionJournalFiles.read(path,maximum);}
            public void write(Path path,byte[] bytes,int maximum)throws IOException{require(path,maximum,false);if(bytes.length>maximum-32)throw new IOException("Configured recovery frame exceeds paid slot");var runway=diskRunway.reserveRaftAppend(pool,bytes.length+32L,true);try{SessionJournalFiles.write(path,bytes,maximum);}finally{runway.actualFinished();}}
            public void remove(Path path,byte[] bytes,int maximum,SessionJournalFiles.Removal removal)throws IOException{throw new IOException("Configured recovery reclamation requires separate authority");}
            public void deleted(Path path)throws IOException{throw new IOException("Configured recovery slot remains retained");}
            public void reclaimedTree(Path path)throws IOException{throw new IOException("Configured recovery slot remains retained");}
            public void reserve(Reservation reservation)throws IOException{require(reservation.path(),reservation.maximum(),false);}
            public void fulfilled(Reservation reservation)throws IOException{require(reservation.path(),reservation.maximum(),false);}
        };
    }
    /** Borrowed actor ports; RatisConfiguredRecovery invokes them only on its admitted IO executor. */
    RatisConfiguredRecovery.Host configuredRecoveryHost(RatisTopologySourceSnapshotClient.ReplyAdmission admission){return new RatisConfiguredRecovery.Host(){
        public CompletableFuture<RatisTopologySourceSnapshotRpc.Prepared> prepare(RatisTopologySourceSnapshotRpc.Request request){return configuredRecoveryAction(()->{
            try{return relocationClients.prepare(request,admission);}
            catch(RatisTopologySourceSnapshotRpc.AbandonedException abandoned){
                if(recoveryGraph.snapshot()!=null)recoveryGraph.observeSourceAbandonment(abandoned.receipt());
                throw abandoned;
            }
        });}
        public CompletableFuture<byte[]> register(RatisTopologySourceSnapshotRpc.Prepared prepared){return configuredRecoveryAction(()->{var store=registerRecovery(prepared);return prepared.request().metadataOnly()?store.metadataRecoveryRegistration().digest():store.currentRangeRecoveryRegistration().digest();});}
        public CompletableFuture<RatisTopologySourceSnapshotRpc.Prepared> transfer(RatisTopologySourceSnapshotRpc.Request request){return configuredRecoveryAction(()->relocationClients.transfer(request,admission));}
        public CompletableFuture<RatisRecoveryImageRpc.Evidence> probe(RatisRecoveryImageRpc.Request request){return configuredRecoveryAction(()->{var machine=machines.get(RaftGroupId.valueOf(request.source().sourceGroup()));if(machine==null)throw new IOException("Recovery probe has no local receiver");return machine.recoveryImageEvidence(request);});}
        public CompletableFuture<RatisSourceHeadRpc.Head> head(RatisSourceHeadRpc.Request request){return configuredRecoveryAction(()->relocationClients.head(request,admission));}
        public CompletableFuture<Boolean> complete(RatisTopologySourceSnapshotRpc.Prepared prepared,RatisSourceHeadRpc.Head head){return configuredRecoveryAction(()->completeRecovery(prepared,head));}
        public CompletableFuture<Void> release(RatisTopologySourceSnapshotRpc.Prepared prepared){return configuredRecoveryAction(()->{relocationClients.release(prepared.request(),prepared.sourceOwner());return null;});}
    };}
    private <T>CompletableFuture<T> configuredRecoveryAction(java.util.concurrent.Callable<T> action){synchronized(this){if(!started || starting || closing || closeRequested || closed)return CompletableFuture.failedFuture(new IOException("Configured recovery host is stopping/not started"));}return recoveryAction(action);}
    private static <T>CompletableFuture<T> recoveryAction(java.util.concurrent.Callable<T> action){try{return CompletableFuture.completedFuture(action.call());}catch(Exception failed){return CompletableFuture.failedFuture(failed);}}
    private record QualifiedRecovery(ApplyPosition cut,RangeDirectory directory) {}
    private QualifiedRecovery qualifyRecovery(RatisTopologySourceSnapshotRpc.Prepared prepared,RatisTopologySourceSnapshotClient.ReplyAdmission admission)throws IOException {
        var request=prepared.request();RatisRangeStateMachine.requireSchemaMembership(request.range(),prepared.sourceCfg().data());var catalogue=epochs.catalogue(plan.catalogue().directoryColumn());var view=catalogue.readView(request.domainId());if(view.directory()==null)throw new IOException("Recovery domain missing; rediscover placement");requirePropagationCoverage(request.domainId(),view.directory());
        var head=relocationClients.head(new RatisSourceHeadRpc.Request(request,prepared.image().position()),admission);if(!head.sourceOwner().equals(prepared.sourceOwner()) || !Arrays.equals(head.currentDirectory().payloadDigest(),view.directory().payloadDigest()) || !RatisRelocationOwnerRpc.covers(head.catalogueCut(),view.cut()))throw new IOException("Recovery source/CAT proof changed during qualification");
        if(request.metadataOnly()){
            var definition=epochs.topologyCatalogue(plan.catalogue().directoryColumn()).readDefinition(request.header().definitionDigest());if(definition==null)throw new IOException("Original META definition missing");request.header().requireDefinition(definition);var target=definition.targets().getFirst();var birth=catalogue.readRecoveryBirth(request.domainId(),target.rangeId(),target.groupId(),target.rangeEpoch(),head.catalogueCut());if(birth.birth()==null || !Arrays.equals(birth.birth().encode(),definition.encode()))throw new IOException("META lacks exact committed source publication");return new QualifiedRecovery(birth.catalogueCut(),view.directory());
        }
        RatisSourceHeadRpc.requireContinuity(request,view.directory());var range=request.range();var birth=catalogue.readRecoveryBirth(request.domainId(),range.rangeId(),range.groupId(),range.rangeEpoch(),head.catalogueCut());var origin=prepared.image().topologyOrigin();
        if(birth.birth()!=null){if(origin==null)throw new IOException("Born CURRENT image lacks actual source birth provenance");origin.seed().header().requireDefinition(birth.birth());}
        else {var bootstrap=plan.directory(request.domainId()).ranges().stream().filter(r->r.groupId().equals(range.groupId()) && r.rangeId().equals(range.rangeId()) && r.rangeEpoch()==range.rangeEpoch() && Arrays.equals(r.lowerInclusive(),range.lowerInclusive()) && Arrays.equals(r.upperExclusive(),range.upperExclusive())).findFirst().orElseThrow(()->new IOException("CURRENT birth absence has no immutable ordinary bootstrap witness"));if(origin!=null || bootstrap.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)throw new IOException("CURRENT birth differs from trusted ordinary bootstrap");}
        return new QualifiedRecovery(birth.catalogueCut(),view.directory());
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
            identityJournal.write(root.resolve("node.identity"),identity(true),NODE_IDENTITY_BYTES);
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
            if(configuredRecoveries.size()!=0 || needsRecoveryDiscovery())startConfiguredRecoveryService().join();
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
    private static RatisNodeIdentity.Purpose initializationPurpose(Startup startup) {
        return switch(startup){case AUTHORIZED_GENESIS->RatisNodeIdentity.Purpose.GENESIS;case AUTHORIZED_INITIAL_PLACEMENT->RatisNodeIdentity.Purpose.INITIAL_PLACEMENT;case AUTHORIZED_RESTORE_TARGET->RatisNodeIdentity.Purpose.RESTORE_TARGET;case RECOVER->throw new IllegalArgumentException("Recovery cannot create a node identity");};
    }
    private byte[] identity(boolean exposed) {
        nodeIdentity=nodeIdentity.withPhase(exposed?2:nativeExtentBound?1:0);return nodeIdentity.encode();
    }
    private void observeSchemaActivation(SchemaChange.Definition definition,NativeSchemaActivationProof proof) {
        groups.schemaActivated(definition,proof);data(definition.header().domainId()).refreshSchema(definition,proof);
    }
    private boolean identityExposed(byte[] bytes)throws IOException{return readIdentity(bytes).phase()==2;}
    private RatisNodeIdentity readIdentity(byte[] bytes)throws IOException {
        try{return RatisNodeIdentity.decode(bytes,plan,settings.resources().nativeExtents());}
        catch(IllegalArgumentException invalid){throw new IOException("Invalid original node identity",invalid);}
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
            boolean serving=localReadinessStamp(id,machine).discovery()!=RatisNodeReadiness.Discovery.NONE_AT_CUT && !store.topologyRecoveryOnly() && !entry.retired() && local!=null && local.getStartupRole()!=RaftPeerRole.LISTENER;
            sources.add(new RatisNodeReadiness.GroupSource(entry.group(),entry.descriptor(),entry.directory()==null ? null : entry.directory().domainId(),
                    entry.directory()==null ? plan.catalogue().schemaDigest() : entry.directory().schemaDigest(),entry.role(),RaftPeerId.valueOf(plan.peerId()),serving,store,machine,
                    () -> server==null ? null : server.getDivision(id),()->localReadinessStamp(id,machine)));
        }
        return RatisNodeReadiness.evaluate(plan.nodeId(),plan.nodeIncarnation(),this::readinessStamp,sources);
    }
    private RatisNodeReadiness.MachineStamp localReadinessStamp(RaftGroupId id,RatisRangeStateMachine machine) {
        var stamp=machine.readinessStamp();var none=localNoneDiscoveries.get(id.getUuid());
        try {
            if(none!=null && recoveryGraph!=null && server!=null && none.current(recoveryGraph.snapshot(),groups.require(id.getUuid()),stores.get(id.getUuid()),machine,server.getDivision(id),stamp))
                return new RatisNodeReadiness.MachineStamp(stamp.admissionGeneration(),stamp.configurationStamp(),stamp.configurationPending(),RatisNodeReadiness.Discovery.NONE_AT_CUT,stamp.storageFailure(),stamp.observerFailure());
        }catch(IOException unavailable){/* A lost owner remains pending. */}
        return stamp;
    }
    private record ReadinessPlacement(Object groups,Object none,Object graph) {
        @Override public boolean equals(Object other){return other instanceof ReadinessPlacement placement && groups.equals(placement.groups) && none==placement.none && graph==placement.graph;}
        @Override public int hashCode(){return Objects.hash(groups,System.identityHashCode(none),System.identityHashCode(graph));}
    }
    private boolean requiresParticipantRecovery() {
        for(var group:groups.dataGroups()){var id=RaftGroupId.valueOf(group);var machine=machines.get(id);if(machine==null || localReadinessStamp(id,machine).discovery()!=RatisNodeReadiness.Discovery.NONE_AT_CUT)return true;}
        return false;
    }
    private synchronized RatisNodeReadiness.NodeStamp readinessStamp() {
        var registry=participantRecoveries;
        var recovery=registry==null ? new RatisNodeReadiness.Recovery(!requiresParticipantRecovery(),0,0,participantRecoveryFailure())
                : recoveryStamp(registry.recoveryStatus());
        Throwable failure=maintenanceFailure();if(failure==null)failure=retirementFailure();if(failure==null)failure=transactionExpiryFailure();if(failure==null && poolPressure!=null)failure=poolPressure.failure();
        return new RatisNodeReadiness.NodeStamp(state(),installing,new ReadinessPlacement(groups.entries(),localNoneDiscoveries,recoveryGraph==null?null:recoveryGraph.snapshot()),recovery,failure);
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
            if (starting || closing || installing && (configuredRecoveries==null || !configuredRecoveries.running())) throw new IOException("Node lifecycle operation is active"); closeRequested=true; closing=true;
        }
        try {
            if(configuredRecoveryService!=null){try{configuredRecoveryService.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Configured recovery CONTROL drain interrupted",interrupted);}catch(ExecutionException|TimeoutException failed){throw new IOException("Configured recovery CONTROL did not drain",failed);}}
            if(configuredRecoveries!=null)configuredRecoveries.close(settings.shutdownTimeout());
            synchronized(this){if(installing)throw new IOException("External node lifecycle operation is still active");}
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
            for(var owner:pools.values()){
                if(!owner.closed){owner.pool.close();owner.closed=true;}
                if(owner.recoveryMetadata!=null){owner.recoveryMetadata.close();owner.recoveryMetadata=null;owner.recoveryEncodedBytes=0;}
            }poolClosed=true;
            for (var client:homeClients.values()) client.close();
            if(nativeExtentEnv!=null){nativeExtentEnv.close();nativeExtentEnv=null;}
            localNoneDiscoveries=Map.of();if(localNoneCredit!=null){localNoneCredit.close();localNoneCredit=null;}
            if(recoveryGraph!=null){recoveryGraph.close();recoveryGraph=null;}if(recoveryGraphCredit!=null){recoveryGraphCredit.close();recoveryGraphCredit=null;}
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
