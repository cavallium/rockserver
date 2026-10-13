package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.SnapshotFence;

import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import it.cavallium.rockserver.core.common.distributed.NativeTopologySeed;
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
    @FunctionalInterface interface SchemaReplicaClientFactory { RatisSchemaReplicaClient create() throws IOException; }
    @FunctionalInterface interface HomeClientFactory { RatisRoutedClient connect(it.cavallium.rockserver.core.common.distributed.RangeDirectory directory) throws IOException; }
    public record Settings(NativeRangeImage.Limits images,RatisRangeStateMachine.RetentionLimits retention,
                           Duration clockPeriod,int propagationVisits,int maxPropagationRanges,Duration shutdownTimeout,
                           ExportResources exports) {
        public Settings {
            Objects.requireNonNull(images); Objects.requireNonNull(retention); Objects.requireNonNull(clockPeriod); Objects.requireNonNull(shutdownTimeout); Objects.requireNonNull(exports);
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
    private final RatisNodePlan plan; private final Map<UUID,DistributedSchema> logicalSchemas; private final Path root; private final Settings settings;
    private final SchemaReplicaConfig schemaReplicaConfig;
    private final RatisSessionEpochs epochs; private final Map<UUID,RatisRoutedClient> routedClients; private final RatisSnapshotFences snapshotHomes;
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
                                       Map<UUID,String> physicalNames,Set<UUID> mergeColumns,Map<String,byte[]> sourceSchemaRows) {
        public LearnerSourceEvidence { Objects.requireNonNull(image);Objects.requireNonNull(schema);Objects.requireNonNull(sourceOwner);Objects.requireNonNull(configuration);Objects.requireNonNull(completedCut);physicalNames=Map.copyOf(physicalNames);mergeColumns=Set.copyOf(mergeColumns);var rows=new HashMap<String,byte[]>();sourceSchemaRows.forEach((key,value)->rows.put(key,value.clone()));sourceSchemaRows=Map.copyOf(rows); }
        @Override public Map<String,byte[]> sourceSchemaRows(){var copy=new HashMap<String,byte[]>();sourceSchemaRows.forEach((key,value)->copy.put(key,value.clone()));return Map.copyOf(copy);}
    }
    private LearnerSourceReader learnerSource=this::loadLearnerSource;
    private NativeTopologyLearnerRegistration learnerAttempt;
    public synchronized void learnerSource(LearnerSourceReader reader){if(started || starting || installing || closeRequested)throw new IllegalStateException("Learner source policy is fixed before start");learnerSource=Objects.requireNonNull(reader);}

    private record SeedAttempt(TopologyChange.Definition definition,TopologyChange.State authority,ReplicaDescriptor local) {}
    private SeedAttempt seedAttempt;
    private RatisTopologyOwnerClient topologyOwners;private boolean topologyOwnersClosed;
    private long groupManagementCall;
    private final Set<UUID> virginSeedAdds=new HashSet<>();
    private DistributedStoragePool pool; private RaftServer server; private RatisCatalogueClock clock;
    private final Map<UUID,RangeDirectory> startupDirectories=new HashMap<>();
    private final Map<UUID,RatisSessionPropagation> dataPropagations=new HashMap<>(); private RatisSessionClockService service;
    private final List<RatisSessionPropagation> propagations=new ArrayList<>();
    private final Map<RatisNodePlan.Role,RatisRoutedClient> homeClients=new EnumMap<>(RatisNodePlan.Role.class);
    private RatisTransactionExpiryService transactionExpiry;
    private RecoverySettings recoverySettings;
    private RatisParticipantRecoveryRegistry participantRecoveries;
    private RatisTransactionRecoveryService participantRecoveryService;
    private RatisRetirementCoordinator retirementCoordinator;
    private RatisTransactionRecoveryService retirementService;
    private RatisNativeParticipantDiscovery nativeParticipantDiscovery;
    private RatisSnapshotHelpService snapshotHelp;
    private RatisProofQueryService proofQueries;
    private RatisProofQueryService sourceOutcomeQueries;
    private RatisProofQueryService topologyImageQueries,relocationOwnerQueries,importPreflightQueries;private RatisTopologySourceSnapshotArtifacts sourceArtifacts;private RatisTopologySourceSnapshotService sourceSnapshots;private RatisRelocationDataClient relocationClients;
    private RatisProofQueryService authorityQueries;
    private RatisProofQueryService importPublicationQueries;
    private RatisProofQueryService operationRerouteQueries;
    private final Map<UUID,RatisReferencedFiles> importFiles=new LinkedHashMap<>();
    private ThreadPoolExecutor importFileIo;
    private RatisExportSourceService exportSources;
    private RatisSnapshotCohortService snapshotCohorts;
    private final UUID snapshotProcessIncarnation=UUID.randomUUID(),snapshotPoolId=UUID.randomUUID();
    private RatisSchemaReplicaService schemaReplicas;
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
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schemas,snapshotHomes,recoveries,homeFactory,replicas,() -> new RatisSchemaReplicaClient(RatisSchemaReplicaClient.MAX_CALLS));
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,HomeClientFactory homeFactory,SchemaReplicaConfig replicas,SchemaReplicaClientFactory replicaFactory) throws IOException {
        if(factory==DEFAULT_SERVER_FACTORY)RatisStreamingTransport.server(plan,new RaftProperties());
        var node=new RatisNodeRuntime(plan,root,epochs,routed,scheduler,timer,time,settings,schemas,snapshotHomes,recoveries,replicas);
        try { node.initialize(config,startup,factory,homeFactory,replicaFactory); return node; }
        catch (IOException | RuntimeException | Error failure) {
            try { node.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    private RatisNodeRuntime(RatisNodePlan plan,Path root,RatisSessionEpochs epochs,Map<UUID,RatisRoutedClient> routed,
            RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Collection<DistributedSchema> schemas,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,SchemaReplicaConfig replicas) {
        this.plan=Objects.requireNonNull(plan);groups=new RatisRuntimeGroups(plan); this.root=root.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs);
        this.routedClients=Map.copyOf(routed);seedAuthority=epochs.topologyAuthority()::read;joinAuthority=epochs.topologyJoinAuthority()::read; this.scheduler=Objects.requireNonNull(scheduler); this.timer=Objects.requireNonNull(timer);
        this.time=Objects.requireNonNull(time); this.settings=Objects.requireNonNull(settings); schemaReplicaConfig=Objects.requireNonNull(replicas); var qualified=new HashMap<UUID,DistributedSchema>();if(schemas!=null)for(var schema:schemas)if(qualified.putIfAbsent(schema.domainId(),schema)!=null)throw new IllegalArgumentException("Duplicate node schema domain");logicalSchemas=Map.copyOf(qualified); this.snapshotHomes=snapshotHomes; recoverySettings=recoveries;
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
    private void initialize(DatabaseConfig config,Startup startup,ServerFactory factory,HomeClientFactory homeFactory,SchemaReplicaClientFactory replicaFactory) throws IOException {
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
        Path identity=root.resolve("node.identity"), logs=root.resolve("raft"), nativePath=root.resolve("native");
        if (Files.exists(identity)) {
            boolean exposed=identityExposed(SessionJournalFiles.read(identity,128));
            if (!exposed && startup!=Startup.AUTHORIZED_INITIAL_PLACEMENT) throw new IOException("Interrupted initial placement requires explicit authorization to resume");
            if (exposed) {
                if (!Files.isRegularFile(nativePath.resolve("CURRENT"))) throw new IOException("Established native state is missing; refuse empty-voter recovery");
                for (var group:groups.raftGroups()) if (!Files.isRegularFile(logs.resolve(group.getGroupId().getUuid().toString()).resolve("current/raft-meta"))) {
                    throw new IOException("Established Raft state is missing for "+group.getGroupId());
                }
            }
        } else {
            if (startup!=Startup.AUTHORIZED_INITIAL_PLACEMENT) throw new IOException("Node identity is absent; explicit placement initialization is required");
            for (String directory:List.of("native","raft","sessions","recovery","schema-replicas","referenced-files","topology-exposure")) {
                Path path=root.resolve(directory);
                if (Files.exists(path)) try (var entries=Files.list(path)) { if (entries.findAny().isPresent()) throw new IOException("Unidentified node data must not be adopted"); }
            }
            SessionJournalFiles.write(identity,identity(false),128);
        }
        for (String directory:List.of("native","raft","sessions","recovery","schema-replicas")) { Path path=Files.createDirectories(root.resolve(directory)); SessionJournalFiles.force(path); }
        SessionJournalFiles.force(root);
        captureHomes=snapshotHomes;
        var snapshotPlan=plan.home(RatisNodePlan.Role.SNAPSHOT_HOME);
        if (snapshotPlan!=null) {
            if (captureHomes==null) captureHomes=new RatisSnapshotFences(homeClients.get(snapshotPlan.role()),snapshotPlan.column());
        }
        proofQueries=new RatisProofQueryService();importPreflightQueries=RatisProofQueryService.importPreflightLane();sourceOutcomeQueries=RatisProofQueryService.sourceOutcomeLane();topologyImageQueries=RatisProofQueryService.topologyImageLane();relocationOwnerQueries=RatisProofQueryService.relocationOwnerLane();relocationClients=new RatisRelocationDataClient(plan.clusterId(),plan.databaseId(),plan.catalogue().groupId());
        if(groups.raftGroups().stream().anyMatch(group->groups.require(group.getGroupId().getUuid()).role()==RatisNodePlan.Role.TRANSACTION_HOME))
            operationRerouteQueries=RatisProofQueryService.operationRerouteLane();
        if (groups.raftGroups().stream().anyMatch(group -> groups.require(group.getGroupId().getUuid()).role()==RatisNodePlan.Role.CATALOGUE)) {
            authorityQueries=RatisProofQueryService.authorityLane();
            importPublicationQueries=RatisProofQueryService.importPublicationLane();
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
        pool=logicalSchemas.isEmpty() ? DistributedStoragePool.open(nativePath,config,LoggerFactory.getLogger(RatisNodeRuntime.class))
                : DistributedStoragePool.open(nativePath,config,LoggerFactory.getLogger(RatisNodeRuntime.class),logicalSchemas.values(),plan.columns(),metadataBindings);
        pool.configureSnapshotAdmission(settings.exports().pins());
        snapshotCohorts=new RatisSnapshotCohortService(new RatisSnapshotCohortService.Host(){
            public UUID nodeId(){return plan.nodeId();}public UUID nodeIncarnation(){return plan.nodeIncarnation();}public UUID processIncarnation(){return snapshotProcessIncarnation;}public UUID poolId(){return snapshotPoolId;}
            public DistributedStoragePool pool(){return pool;}public PoolSnapshotAdmission.Limits poolLimits(){return settings.exports().pins();}
            public RatisSnapshotCohortService.Member member(UUID group,SnapshotFence fence,boolean ready)throws Exception{
                synchronized(RatisNodeRuntime.this){if(!started || closeRequested || closing || closed)throw new IOException("Cohort node owner unavailable");}
                var entry=groups.require(group);var machine=machines.get(RaftGroupId.valueOf(group));if(entry.role()!=RatisNodePlan.Role.DATA || entry.retired() || machine==null || stores.get(group)==null)throw new IOException("Cohort has no actual local DATA owner");
                return machine.snapshotCohortMember(fence,ready,entry.descriptor(),ownReplica(entry.descriptor()));
            }
        },scheduler,RatisSnapshotCohortService.Limits.defaults(),System::nanoTime);
        sourceArtifacts=new RatisTopologySourceSnapshotArtifacts(Files.createDirectories(root.resolve("topology-source-artifacts")),settings.images(),Math.multiplyExact(8,settings.images().maxImageBytes()),RatisTopologySourceSnapshotService.MAX_METADATA_BYTES,4);
        sourceSnapshots=new RatisTopologySourceSnapshotService(sourceSnapshotHost(),RatisTopologySourceSnapshotService.Limits.defaults(settings.images().maxImageBytes()));

        for(var seed:pool.topologySeeds())groups.register(seed);
        for(var learner:pool.topologyLearners())groups.register(learner);
        loadRelocationIntents();
        var allowed=new HashSet<UUID>(); for (var group:groups.raftGroups()) allowed.add(group.getGroupId().getUuid());
        try (var entries=Files.list(logs)) {
            for (var path:entries.toList()) if (Files.isDirectory(path)) {
                UUID group; try { group=UUID.fromString(path.getFileName().toString()); } catch (IllegalArgumentException other) { continue; }
                if (!path.getFileName().toString().equals(group.toString())) throw new IOException("Noncanonical persisted Raft group path");
                if (!allowed.contains(group)) throw new IOException("Unplanned persisted Raft group "+group);
            }
        }

        if (groups.raftGroups().stream().anyMatch(group -> groups.require(group.getGroupId().getUuid()).role()==RatisNodePlan.Role.DATA))
            schemaReplicas=new RatisSchemaReplicaService(nativePath,root.resolve("schema-replicas"),pool,scheduler,schemaReplicaConfig.policy(),schemaReplicaConfig.limits());
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
                var id=group.getGroupId().getUuid(); RatisRangeStateMachine machine;
                var role=groups.require(id).role();
                if (role==RatisNodePlan.Role.CATALOGUE) {
                    var c=plan.catalogue();
                    var store=pool.openRange(new RangeOwnership.Owner(id,c.epoch()),new RangeOwnership.Interval(id,Set.of(c.directoryColumn(),c.sessionsColumn()),null,null),
                            c.schemaDigest(),Map.of(c.directoryColumn(),"catalogue-directory",c.sessionsColumn(),"catalogue-sessions"),Set.of());
                    stores.put(id,store);
                    var operatorLimits=new RatisOperatorCatalogue.Limits(schemaReplicaConfig.limits().maximumUploads(),schemaReplicaConfig.limits().maximumDiskBytes());
                    var operators=RatisOperatorCatalogue.validator(plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),operatorLimits,this::requireOperatorAdmission,key -> committedCatalogueValue(store,c.directoryColumn(),key));
                    machine=new RatisRangeStateMachine(store,group,settings.images(),RatisCatalogue.validator(plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),c.sessionsColumn(),operators,key -> committedCatalogueValue(store,c.directoryColumn(),key)));
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
                    RatisTopologyCatalogue.OwnerProofReader cdcOwners=(definition,expected)->{var range=(expected.stage()==TopologyChange.Stage.FREEZE ? definition.parents() : definition.targets()).stream().filter(candidate->candidate.groupId().equals(expected.groupId())).findFirst().orElseThrow();var owner=range.replicas().stream().filter(candidate->candidate.replicaId().equals(expected.owner().replicaId())).findFirst().orElseThrow();var evidence=topologyOwners.read(definition,expected.stage(),range,owner);return new RatisTopologyCatalogue.QualifiedOwner(evidence.proof(),evidence.completedCut());};
                    var cdcAuthority=new RatisCdcDeliveryAuthority(routedClients,cdcDirectories,cdcLineages::read,cdcOwners,cdcDefinitions);
                    machine.cdcDeliveryAdmission(cdcAuthority::validate);
                    machine.topologyExecutionAdmission(command->{if(RatisTopologyCatalogue.relocationExecution(command)){RatisTopologyCatalogue.admitRelocation(command,plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),key->committedCatalogueValue(store,c.directoryColumn(),key),this::qualifyRelocationPublication);return;}RatisTopologyCatalogue.admitExecution(command,plan.clusterId(),plan.databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),key->committedCatalogueValue(store,c.directoryColumn(),key),(definition,expected)->{
                        var range=(expected.stage()==TopologyChange.Stage.FREEZE ? definition.parents() : definition.targets()).stream().filter(candidate->candidate.groupId().equals(expected.groupId())).findFirst().orElseThrow();var owner=range.replicas().stream().filter(candidate->candidate.replicaId().equals(expected.owner().replicaId())).findFirst().orElseThrow();var evidence=topologyOwners.read(definition,expected.stage(),range,owner);if(expected.stage()==TopologyChange.Stage.ACTIVATE){var activation=NativeTopologyActivation.decode(evidence.artifact());activation.requireCut(evidence.completedCut());if(!Arrays.equals(activation.proof().encode(),evidence.proof().encode()))throw new IOException("Clock witness differs from genuine owner activation proof");qualifyTopologyActivation(definition,activation);}return new RatisTopologyCatalogue.QualifiedOwner(evidence.proof(),evidence.completedCut());
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
                    if(importDefinition!=null){var manifest=importDefinition.manifest(id);RatisRangeImportRecord.requirePolicy(manifest);if(manifest.columns().size()>settings.images().maxColumns() || (long)manifest.encode().length+17>settings.images().maxRowBytes() || (long)manifest.encode().length+17>settings.images().maxImageBytes())throw new IOException("Imported manifest exceeds configured native image row budget");}
                    var store=registration.learner()!=null ? pool.openTopologyLearner(registration.learner()) : registration.retired() ? pool.openRetiredTopologyRange(id) : registration.seed()!=null ? pool.reopenTopologyRange(registration.seed().birth(id),seedSchema(registration.seed())) : importDefinition==null ? pool.openRange(new RangeOwnership.Owner(id,descriptor.rangeEpoch()),new RangeOwnership.Interval(directory.domainId(),directory.columns(),descriptor.lowerInclusive(),descriptor.upperExclusive()),
                            directory.schemaDigest(),bindings,recipe!=null ? recipe.mergeColumns() : home==null ? plan.domain(directory.domainId()).merges() : Set.of())
                            : startup==Startup.RECOVER ? pool.reopenImportRange(importDefinition.manifest(id),logicalSchemas.get(directory.domainId()))
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
                    // Bind future session controls to the frozen published placement; the native import fence
                    // still rejects DATA/control admission until the exact catalogue activation is applied.
                    machine=RatisRangeStateMachine.withSessions(store,group,settings.images(),validator,epochs,effectiveDirectory,settings.retention());
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
                machines.put(group.getGroupId(),machine);
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
                            var chunk=pool.exportSstChunk(artifact,pin,intent.column(),intent.scopeStart(),
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
        server=factory.create(plan,logs,Collections.unmodifiableMap(machines));
        transactionExpiry=new RatisTransactionExpiryService(scheduler,this::expiryMachines,32);
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
        for(var seed:pool.topologySeeds())for(var target:seed.definition().targets()){var store=stores.get(target.groupId());var activation=store==null ? null : store.topologyActivation();if(activation!=null)observeTopologyActivation(seed.definition(),activation);}
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
    private void configureDataMachine(RatisRangeStateMachine machine,RangeDirectory directory,it.cavallium.rockserver.core.common.distributed.RangeDescriptor descriptor){
        machine.role(RatisNodePlan.Role.DATA);machine.snapshotCohorts(snapshotCohorts);machine.snapshotReadOnlyMaximumTtl(Duration.ofMillis(Math.min(settings.exports().pins().maximumAge().toMillis(),RatisSnapshotCaptureRequest.maximumTtl(RatisSnapshotCaptureRequest.Mode.READ_ONLY).toMillis())));machine.relocationOwnerQueries(relocationOwnerQueries);machine.sourceSnapshots(topologyImageQueries,sourceSnapshots);machine.relocationObserver(new RatisRangeStateMachine.RelocationObserver(){public void persist(RatisTopologyRelocationRecord record,ApplyPosition position)throws IOException{persistRelocation(record,position);}public boolean saved(RatisTopologyRelocationRecord record)throws IOException{return savedRelocation(record);}});machine.relocationAuthority(epochs.topologyJoinAuthority()::read);machine.sourceOutcomeQueries(sourceOutcomeQueries);machine.cdcBinding(directory.clusterId(),directory.databaseId(),descriptor.rangeId());machine.schemaStorage(pool);machine.schemaActivated(this::observeSchemaActivation);
        var authority=epochs.schemaAuthority();machine.schemaAuthority((header,digest)->readSchemaAuthority(authority,header,digest),directory);machine.schemaReplicas(schemaReplicas,ownReplica(descriptor));machine.topologyAuthority(epochs.topologyAuthority()::read,directory,ownReplica(descriptor),pool);machine.topologySeedInstaller(this::seedTopology);machine.topologyActivated(this::observeTopologyActivation);
        if(transactionHomes!=null)machine.sourceOutcomeAdmission(new RatisSourceOutcomeAdmission(new RatisSourceOutcomeAdmission.Authority(){
            public it.cavallium.rockserver.core.common.distributed.TransactionStatus home(it.cavallium.rockserver.core.common.distributed.HomeIdentity original)throws IOException{var configured=plan.home(RatisNodePlan.Role.TRANSACTION_HOME);if(configured==null || !configured.column().equals(original.columnId()))throw new IOException("Source proof HOME column is not configured");var read=transactionHomes.readRegistration(original.transactionId());if(!read.descriptor().groupId().equals(original.groupId()) || read.descriptor().rangeEpoch()!=original.rangeEpoch() || !Arrays.equals(configured.directory().schemaDigest(),original.schemaDigest()))throw new IOException("Source proof changed exact HOME placement");return read.status();}
            public it.cavallium.rockserver.core.common.distributed.ParticipantOutcome outcome(RangeDirectory frozen,RangeDescriptor route,UUID transaction)throws IOException{return data(frozen.domainId()).finalizedParticipant(frozen,route,transaction);}
            public it.cavallium.rockserver.core.common.distributed.ParticipantFenceProof fence(RangeDirectory frozen,RangeDescriptor route,it.cavallium.rockserver.core.common.distributed.ParticipantIdentity identity)throws IOException{return data(frozen.domainId()).participantFence(frozen,route,identity);}
        })::qualify);
        if(captureHomes!=null)machine.snapshotHomes(captureHomes);if(transactionHomes!=null){machine.transactionHomes(transactionHomes);machine.snapshotRecovery(this::recoverParticipant,snapshotHelp);}
    }
    static RatisTopologySourceSnapshotRpc.Request sourceRequest(TopologyChange.Definition definition,UUID catalogue,ReplicaDescriptor receiver){var purpose=definition.parents().getFirst().replicas().stream().anyMatch(r->r.replicaId().equals(receiver.replicaId()))?RatisTopologySourceSnapshotRpc.Purpose.RECOVERY:RatisTopologySourceSnapshotRpc.Purpose.BOOTSTRAP;var bytes=ByteBuffer.allocate(65).put(definition.digest()).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).putLong(receiver.replicaId().getMostSignificantBits()).putLong(receiver.replicaId().getLeastSignificantBits()).put((byte)purpose.ordinal());return new RatisTopologySourceSnapshotRpc.Request(UUID.nameUUIDFromBytes(bytes.array()),definition.header(),catalogue,receiver,purpose);}
    private RatisTopologySourceSnapshotRpc.Prepared prepareLearnerRecovery(RatisTopologySourceSnapshotRpc.Request request)throws Exception{
        synchronized(this){if(!started || closing || closeRequested || installing)throw new IOException("Learner recovery node owner is unavailable");installing=true;}
        try{
            if(request.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.RECOVERY)throw new IOException("Registered image recovery purpose required");var receiver=request.receiver();if(!receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation()) || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address()))throw new IOException("Recovery receiver changed local node identity");
            var view=sourceView(request);var definition=view.definition();var store=stores.get(definition.parents().getFirst().groupId());if(store==null)throw new IOException("No registered local recovery store");var registration=store.topologyLearnerRegistration();if(registration==null || !sameLearnerIdentity(registration.receiver(),receiver))throw new IOException("Recovery lacks original receiver registration");
            var prepared=relocationClients.prepare(request,definition);var names=new HashMap<UUID,String>();for(var local:stores.values())names.putAll(pool.schemaPhysicalNames(local));var permission=recoveryPermission(store,view,prepared,names);
            var saved=store.topologyLearnerImagePermission();if(saved!=null && Arrays.equals(saved.encode(),permission.encode()))return prepared;
            var after=sourceView(request);if(!Arrays.equals(after.definition().encode(),definition.encode()) || !RatisRelocationOwnerRpc.covers(after.cut(),view.cut()))throw new IOException("Recovery catalogue witness changed");pool.prepareTopologyLearnerImage(store,permission);return prepared;
        }finally{synchronized(this){installing=false;}}
    }
    static it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerImagePermission recoveryPermission(ReplicatedRangeStore store,RatisTopologyJoinAuthority.View view,RatisTopologySourceSnapshotRpc.Prepared prepared,Map<UUID,String> receiverBindings)throws IOException{
        var request=prepared.request();request.header().requireDefinition(view.definition());if(request.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.RECOVERY || !request.catalogue().equals(view.catalogue()) || view.state().phase()!=TopologyChange.Phase.JOINING)throw new IOException("Recovery image lacks current irreversible catalogue qualification");var registration=store.topologyLearnerRegistration();if(registration==null || !sameLearnerIdentity(registration.receiver(),request.receiver()))throw new IOException("Recovery changed registered receiver identity");
        var definition=view.definition();RatisRangeStateMachine.requireSchemaMembership(definition.parents().getFirst(),prepared.sourceCfg().data());prepared.schema().requireMatches(definition.sourceDirectory());prepared.schema().requireMatches(definition.targetDirectory());var own=new HashMap<UUID,String>();var merges=new HashSet<UUID>();for(var column:prepared.schema().columns()){own.put(column.columnId(),receiverBindings.getOrDefault(column.columnId(),"relocation-"+column.columnId()));if(column.operator()!=null)merges.add(column.columnId());}
        return new it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerImagePermission(registration,definition,view.catalogue(),view.cut(),prepared.image().topologyOrigin(),prepared.schema(),prepared.image().position(),prepared.image().sha256(),prepared.sourceCfg().data(),own,merges,prepared.schemaRows());
    }
    private static boolean sameLearnerIdentity(ReplicaDescriptor before,ReplicaDescriptor after){return before.nodeId().equals(after.nodeId()) && before.nodeIncarnation().equals(after.nodeIncarnation()) && before.replicaId().equals(after.replicaId()) && before.peerId().equals(after.peerId()) && before.address().equals(after.address());}
    private LearnerSourceEvidence loadLearnerSource(RatisTopologyJoinAuthority.View view,ReplicaDescriptor receiver)throws IOException{if(view.state().phase()!=TopologyChange.Phase.JOINING)throw new IOException("Production learner bootstrap requires irreversible JOINING");var prepared=relocationClients.prepare(sourceRequest(view.definition(),view.catalogue(),receiver),view.definition());var bindings=plan.domain(prepared.schema().domainId());if(!bindings.columns().keySet().equals(prepared.schema().columnIds()))throw new IOException("Receiver has no configured physical bindings for source schema");return new LearnerSourceEvidence(prepared.image(),prepared.schema(),prepared.sourceOwner(),prepared.sourceCfg(),prepared.image().position(),bindings.columns(),bindings.merges(),prepared.schemaRows());}
    private RatisTopologyJoinAuthority.View sourceView(RatisTopologySourceSnapshotRpc.Request request)throws IOException{var query=RatisTopologyJoinAuthority.Request.of(request.header(),request.receiver().replicaId(),request.purpose()==RatisTopologySourceSnapshotRpc.Purpose.BOOTSTRAP?RatisTopologyJoinAuthority.Purpose.REGISTER:RatisTopologyJoinAuthority.Purpose.CONFIGURATION);var view=joinAuthority.read(query);view.require(query);if(!view.catalogue().equals(plan.catalogue().groupId()) || !view.catalogue().equals(request.catalogue()) || view.state().phase()!=TopologyChange.Phase.JOINING)throw new IOException("Source image requires exact irreversible JOINING");return view;}
    private RatisTopologySourceSnapshotService.Host sourceSnapshotHost(){return new RatisTopologySourceSnapshotService.Host(){
        public RatisTopologySourceSnapshotService.Lease capture(RatisTopologySourceSnapshotRpc.Request request)throws Exception{var view=sourceView(request);var machine=machines.get(RaftGroupId.valueOf(view.definition().parents().getFirst().groupId()));if(machine==null)throw new IOException("Source snapshot has no local actual source owner");return machine.captureSourceSnapshot(request,view.definition(),logicalSchemas.get(request.header().domainId()),sourceArtifacts);}
        public void requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{var view=sourceView(lease.prepared().request());var machine=machines.get(RaftGroupId.valueOf(view.definition().parents().getFirst().groupId()));if(machine==null || lease.source().getStateMachine()!=machine)throw new IOException("Source snapshot division changed");var evidence=machine.relocationEvidence(new RatisRelocationOwnerRpc.Request(view.definition(),null,RatisRelocationOwnerRpc.Expected.SOURCE,null,null));if(!evidence.owner().replicaId().equals(lease.prepared().sourceOwner().replicaId()) || !RatisRelocationOwnerRpc.covers(evidence.completedCut(),lease.prepared().image().position()) || !Arrays.equals(evidence.configuration().data(),lease.prepared().sourceCfg().data()))throw new IOException("Retained source image lost actual native owner/configuration");}
        public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{var view=sourceView(lease.prepared().request());var image=lease.prepared().image();var evidence=relocationClients.read(new RatisRelocationOwnerRpc.Request(view.definition(),lease.prepared().request().receiver().replicaId(),RatisRelocationOwnerRpc.Expected.IMAGE,image.position(),image.sha256()));if(!evidence.imageInstalled())throw new IOException("Receiver has not forced the exact native image");}
        public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{sourceArtifacts.released(lease.prepared().request());}
        public boolean isReleased(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return sourceArtifacts.isReleased(request);}
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
        beginSeedLifecycle();try{installTopologySeedOwned(seed);}catch(org.rocksdb.RocksDBException failure){throw new IOException("Native topology children could not register",failure);}finally{endSeedLifecycle();}
    }
    private void beginSeedLifecycle()throws IOException{synchronized(this){if(installing || starting || closing || closeRequested || closed)throw new IOException("Node lifecycle operation prevents topology seed registration");installing=true;}}
    private void endSeedLifecycle(){synchronized(this){installing=false;}}
    private void installTopologySeedOwned(NativeTopologySeed seed)throws IOException,org.rocksdb.RocksDBException{
            Objects.requireNonNull(seed);var forced=pool.topologySeed(seed.definition().intentId());if(forced==null || !forced.equals(seed))throw new IOException("Topology seed is not the exact forced native artifact");groups.register(forced);
            for(var target:forced.definition().targets()){
                var id=RaftGroupId.valueOf(target.groupId());if(machines.containsKey(id))continue;var entry=groups.require(target.groupId());var store=pool.reopenTopologyRange(forced.birth(target.groupId()),seedSchema(forced));
                var directory=store.schemaInstallation()==null ? entry.directory() : store.schemaInstallation().definition().targetDirectory();
                var machine=RatisRangeStateMachine.withSessions(store,entry.group(),settings.images(),command->{},epochs,directory,settings.retention());configureDataMachine(machine,directory,target);machine.learnerInstaller(topologyImageQueries,this::joinTopologyLearner);machine.learnerRecoveryInstaller(this::prepareLearnerRecovery);machine.proofQueries(proofQueries);machine.exportSources(exportSources);stores.put(target.groupId(),store);machines.put(id,machine);
            }
            // Every machine and borrowed node service is configured before any target can vote.
            for(var target:forced.definition().targets())requireSeedStorage(groups.require(target.groupId()));
            for(var target:forced.definition().targets())markSeedExposure(groups.require(target.groupId()));
            if(started){var actual=new HashSet<RaftGroupId>();server.getGroupIds().forEach(actual::add);for(var target:forced.definition().targets()){var group=groups.require(target.groupId()).group();if(!actual.contains(group.getGroupId()))addGroup(group);else verify(group);}}
    }
    /** Forces receiver-local provenance before adding a fenced LISTENER. No source membership change is performed. */
    public NativeTopologyLearnerRegistration joinTopologyLearner(RatisTopologyJoinAuthority.Request request)throws IOException {
        beginSeedLifecycle();try{
            if(!started || server==null)throw new IOException("Learner join requires running node owner");
            if(learnerAttempt!=null){var attempt=learnerAttempt;var expected=RatisTopologyJoinAuthority.Request.of(attempt.definition(),attempt.receiver().replicaId());if(!Arrays.equals(RatisTopologyJoinAuthority.request(request),RatisTopologyJoinAuthority.request(expected)))throw new IOException("Unknown learner force requires exact original request");var authority=new RatisTopologyJoinAuthority.View(attempt.catalogue(),attempt.catalogueCut(),attempt.definition(),TopologyChange.State.begin(attempt.definition().header()));requireLearnerReceiver(authority,authority.require(request));pool.openTopologyLearner(attempt);installTopologyLearnerOwned(attempt);learnerAttempt=null;return attempt;}

            for(var saved:pool.topologyLearners())if(saved.definition().intentId().equals(request.intent())){var view=new RatisTopologyJoinAuthority.View(saved.catalogue(),saved.catalogueCut(),saved.definition(),TopologyChange.State.begin(saved.definition().header()));view.require(request);installTopologyLearnerOwned(saved);return saved;}
            if(learnerSource==null)throw new IOException("Trusted pinned source image reader is not configured");
            var view=joinAuthority.read(request);var receiver=view.require(request);requireLearnerReceiver(view,receiver);
            var evidence=learnerSource.read(view,receiver);var source=view.definition().parents().getFirst();evidence.schema().requireMatches(view.definition().sourceDirectory());
            var image=evidence.image();var cfg=evidence.configuration();var cut=evidence.completedCut();
            if(image.topologyOrigin()==null || source.replicas().stream().noneMatch(evidence.sourceOwner()::equals) || !image.owner().equals(new RangeOwnership.Owner(source.groupId(),source.rangeEpoch()))
                    || !image.interval().equals(new RangeOwnership.Interval(view.definition().sourceDirectory().domainId(),view.definition().sourceDirectory().columns(),source.lowerInclusive(),source.upperExclusive()))
                    || !Arrays.equals(image.schemaDigest(),evidence.schema().digest()) || cut.term()<1 || cut.index()<image.position().index() || cut.term()<image.position().term() || cut.index()==image.position().index() && cut.term()!=image.position().term()
                    || cfg.term()>image.position().term() || cfg.index()>image.position().index() || cfg.index()==image.position().index() && cfg.term()!=image.position().term())throw new IOException("Source image/native owner/configuration evidence differs from relocation");
            RatisRangeStateMachine.requireSchemaMembership(source,cfg.data());
            var current=joinAuthority.read(request);current.require(request);requireLearnerReceiver(current,receiver);if(!Arrays.equals(current.definition().encode(),view.definition().encode()))throw new IOException("Relocation definition changed during source export");
            var target=current.definition().targets().getFirst();var proposed=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder();
            for(var peer:groups.group(target).getPeers().stream().sorted(Comparator.comparing(peer->peer.getId().toString())).toList()){if(peer.getStartupRole()==RaftPeerRole.LISTENER)proposed.addListeners(peer.getRaftPeerProto());else proposed.addPeers(peer.getRaftPeerProto());}
            var registration=new NativeTopologyLearnerRegistration(current.definition(),image.topologyOrigin(),evidence.schema(),receiver,current.catalogue(),current.cut(),image.position(),image.sha256(),cfg.data(),proposed.build().toByteArray(),evidence.physicalNames(),evidence.mergeColumns(),evidence.sourceSchemaRows());
            learnerAttempt=registration;pool.openTopologyLearner(registration);installTopologyLearnerOwned(registration);learnerAttempt=null;return registration;
        }catch(org.rocksdb.RocksDBException|IllegalArgumentException|IllegalStateException failure){throw new IOException("Learner force/register failed",failure);}finally{endSeedLifecycle();}
    }
    private void requireLearnerReceiver(RatisTopologyJoinAuthority.View view,ReplicaDescriptor receiver)throws IOException{
        requirePropagationCoverage(view.definition().header().domainId(),view.definition().targetDirectory());groups.requireCapacity(view.definition().targets().stream().map(RangeDescriptor::groupId).toList());
        var source=view.definition().sourceDirectory();if(!view.catalogue().equals(plan.catalogue().groupId()) || !receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation()) || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address())
                || !source.clusterId().equals(plan.clusterId()) || !source.databaseId().equals(plan.databaseId()) || !plan.directory(source.domainId()).domainId().equals(source.domainId()))throw new IOException("Learner receiver/catalogue/bootstrap identity differs");
    }
    private void installTopologyLearnerOwned(NativeTopologyLearnerRegistration registration)throws IOException,org.rocksdb.RocksDBException{
        var forced=pool.topologyLearners().stream().filter(registration::equals).findFirst().orElseThrow(()->new IOException("Learner is not exact forced native history"));groups.register(forced);var id=RaftGroupId.valueOf(forced.definition().targets().getFirst().groupId());var entry=groups.require(id.getUuid());
        if(!machines.containsKey(id)){if(schemaReplicas==null)schemaReplicas=new RatisSchemaReplicaService(root.resolve("native"),root.resolve("schema-replicas"),pool,scheduler,schemaReplicaConfig.policy(),schemaReplicaConfig.limits());var store=pool.openTopologyLearner(forced);var machine=RatisRangeStateMachine.withSessions(store,entry.group(),settings.images(),command->{},epochs,entry.directory(),settings.retention());configureDataMachine(machine,entry.directory(),entry.descriptor());machine.learnerInstaller(topologyImageQueries,this::joinTopologyLearner);machine.learnerRecoveryInstaller(this::prepareLearnerRecovery);machine.proofQueries(proofQueries);machine.exportSources(exportSources);stores.put(id.getUuid(),store);machines.put(id,machine);}
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
        beginSeedLifecycle();try{
            Objects.requireNonNull(request);requirePropagationCoverage(request.definition().header().domainId(),request.definition().targetDirectory());groups.requireCapacity(request.definition().targets().stream().map(RangeDescriptor::groupId).toList());var saved=pool.topologySeed(request.definition().intentId());var parents=requireSeedParents(request,saved==null);
            if(saved!=null){request.requireSeed(saved);installTopologySeedOwned(saved);return saved;}
            var attempt=seedAttempt;if(attempt!=null && (!Arrays.equals(attempt.definition().encode(),request.definition().encode()) || !attempt.local().equals(request.owner())))throw new IOException("Unknown SEED force requires exact original definition/local retry");
            if(attempt==null){var view=seedAuthority.read(request.definition().header(),TopologyChange.Stage.SEED);view.require(RatisTopologyAuthority.Request.of(request.definition().header(),TopologyChange.Stage.SEED));if(!view.catalogue().equals(request.catalogue()) || !Arrays.equals(view.definition().encode(),request.definition().encode()) || !Arrays.equals(view.admissionDigest(),request.originalAdmissionDigest()))throw new IOException("Fresh SEED authority changed original definition/catalogue");parents=requireSeedParents(request,true);attempt=new SeedAttempt(request.definition(),view.state(),request.owner());seedAttempt=attempt;}
            var seed=pool.seedTopology(attempt.definition(),attempt.authority(),parents,attempt.local());request.requireSeed(seed);seedAttempt=null;installTopologySeedOwned(seed);return seed;
        }catch(org.rocksdb.RocksDBException failure){throw new IOException("Native SEED force/register did not complete",failure);}catch(IllegalArgumentException|IllegalStateException invalid){throw new IOException("Invalid SEED owner/authority/native binding",invalid);}finally{endSeedLifecycle();}
    }
    private void observeTopologyActivation(TopologyChange.Definition definition,NativeTopologyActivation activation){
        var seed=pool.topologySeed(definition.intentId());if(seed==null || !activation.catalogueGroup().equals(plan.catalogue().groupId()))throw new IllegalStateException("Native topology activation has no configured catalogue/forced seed");activation.requireSeed(seed);definition.header().requireDefinition(seed.definition());var store=stores.get(activation.birth().groupId());if(store==null || store.topologyActivation()==null || !Arrays.equals(store.topologyActivation().encode(),activation.encode()))throw new IllegalArgumentException("Topology observer lacks original committed local native activation");activation.requireCut(store.applied());groups.topologyActivated(definition,activation);try{qualifyTopologyActivation(definition,activation);}catch(IOException failure){throw new IllegalStateException("Committed topology clock witness could not persist",failure);}
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
            SessionJournalFiles.write(root.resolve("node.identity"),identity(true),128);
            server.start(); var existing=new HashSet<RaftGroupId>(); server.getGroupIds().forEach(existing::add);
            if (!machines.keySet().containsAll(existing)) throw new IOException("Server recovered an unplanned group");
            for (var group:groups.raftGroups()) if (!existing.contains(group.getGroupId()))addGroup(group);
            var actualIds=new HashSet<RaftGroupId>(); server.getGroupIds().forEach(actualIds::add);
            if (!actualIds.equals(machines.keySet())) throw new IOException("Server division coverage differs from placement");
            for (var group:groups.raftGroups()){verify(group);virginSeedAdds.remove(group.getGroupId().getUuid());}
            if (service!=null) service.start(timer,settings.clockPeriod());
            transactionExpiry.start(timer,Duration.ofMillis(100));
            if (participantRecoveryService!=null) participantRecoveryService.start(timer,recoverySettings.period());
            if (retirementService!=null) retirementService.start(timer,Duration.ofSeconds(1));
            synchronized (this) { started=true; }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) { closeRequested=true; }
            for (var machine:machines.values()) machine.beginShutdown(); throw failure;
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
        return ByteBuffer.allocate(33).put((byte)(exposed ? 1 : 0)).put(plan.identityDigest()).array();
    }
    private void observeSchemaActivation(SchemaChange.Definition definition,NativeSchemaActivationProof proof) {
        groups.schemaActivated(definition,proof);data(definition.header().domainId()).refreshSchema(definition,proof);
    }
    private boolean identityExposed(byte[] bytes) throws IOException {
        if (bytes.length!=33) throw new IOException("Invalid node identity shape");
        var input=ByteBuffer.wrap(bytes);
        int phase=input.get()&255;
        if (phase>1) throw new IOException("Invalid node exposure phase");
        byte[] digest=new byte[32]; input.get(digest);
        if (!Arrays.equals(digest,plan.identityDigest())) throw new IOException("Node identity/schema/placement binding changed");
        return phase==1;
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
            boolean serving=!entry.retired() && local!=null && local.getStartupRole()!=RaftPeerRole.LISTENER;
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
        Throwable failure=maintenanceFailure();if(failure==null)failure=retirementFailure();if(failure==null)failure=transactionExpiryFailure();
        return new RatisNodeReadiness.NodeStamp(state(),installing,groups.entries(),recovery,failure);
    }
    private RatisNodeReadiness.Recovery recoveryStamp(RatisParticipantRecoveryRegistry.RecoveryStatus status) {
        return new RatisNodeReadiness.Recovery(status.scanned(),status.unresolvedDecisions(),status.activeWorkers(),participantRecoveryFailure());
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
            if(topologyImageQueries!=null){try{topologyImageQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Topology image drain interrupted",interrupted);}catch(ExecutionException|TimeoutException failure){throw new IOException("Topology image owners did not drain",failure);}}
            if(sourceSnapshots!=null)sourceSnapshots.close();if(sourceArtifacts!=null)sourceArtifacts.close();
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
            if (schemaReplicas!=null) {
                try { schemaReplicas.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
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
            if(snapshotCohorts!=null)try{snapshotCohorts.close();}catch(Exception failure){throw new IOException("Cohort owners did not drain",failure);}
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
            if (server!=null && !serverClosed) { server.close(); serverClosed=true; }
            for (var machine:machines.values()) machine.awaitNativeReaders(settings.shutdownTimeout());
            for (var files:importFiles.values()) files.close();
            if (importFileIo!=null) {
                importFileIo.shutdown();
                try { if (!importFileIo.awaitTermination(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS)) throw new IOException("Import file executor did not drain"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Import file drain interrupted",interrupted); }
            }
            if (schemaFencingReplicas!=null && !schemaFencingReplicasClosed) { schemaFencingReplicas.close(); schemaFencingReplicasClosed=true; }
            if(topologyOwners!=null && !topologyOwnersClosed){topologyOwners.close();topologyOwnersClosed=true;}
            if (importOwners!=null && !importOwnersClosed) { importOwners.close(); importOwnersClosed=true; }
            if (pool!=null && !poolClosed) { pool.close(); poolClosed=true; }
            for (var client:homeClients.values()) client.close();
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
