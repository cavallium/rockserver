package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import it.cavallium.rockserver.core.common.distributed.NativeSchemaActivationProof;
import it.cavallium.rockserver.core.common.distributed.RangeDirectory;
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

/** Static-placement node owner. Running means verified divisions, not quorum/read/API readiness. */
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
    private final RatisNodePlan plan; private final DistributedSchema logicalSchema; private final Path root; private final Settings settings;
    private final SchemaReplicaConfig schemaReplicaConfig;
    private final RatisSessionEpochs epochs; private final RatisRoutedClient routed; private final RatisSnapshotFences snapshotHomes;
    private final RWScheduler scheduler; private final Scheduler timer; private final LongSupplier time;
    private final Map<RaftGroupId,RatisRangeStateMachine> machines=new LinkedHashMap<>();
    private DistributedStoragePool pool; private RaftServer server; private RatisCatalogueClock clock;
    private RangeDirectory startupDirectory;
    private RatisSessionPropagation propagation; private RatisSessionClockService service;
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
    private RatisProofQueryService authorityQueries;
    private RatisProofQueryService importPublicationQueries;
    private final Map<UUID,RatisReferencedFiles> importFiles=new LinkedHashMap<>();
    private ThreadPoolExecutor importFileIo;
    private RatisExportSourceService exportSources;
    private RatisSchemaReplicaService schemaReplicas;
    private RatisSchemaReplicaClient schemaFencingReplicas;
    private RatisRangeImportOwnerClient importOwners;
    private boolean importOwnersClosed;
    private boolean schemaFencingReplicasClosed;
    private FileChannel lockChannel; private FileLock lock;
    private boolean started,starting,closeRequested,closing,closed,serverClosed,poolClosed;

    /** Fresh placement is an explicit trusted bootstrap assertion, never inferred from the directory. Borrowed clients/schedulers remain caller-owned. */
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,null,null);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,null,null);
    }
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,DistributedSchema schema) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,schema,null);
    }
    /** Operator-qualified home readers are fixed on DATA state machines before the server factory may initialize them. */
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,DistributedSchema schema,RatisSnapshotFences snapshotHomes) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,schema,snapshotHomes);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,DistributedSchema schema) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schema,null);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,DistributedSchema schema,RatisSnapshotFences snapshotHomes) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schema,snapshotHomes,null);
    }
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,schema,snapshotHomes,recoveries);
    }
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,SchemaReplicaConfig replicas) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,DEFAULT_SERVER_FACTORY,schema,snapshotHomes,recoveries,RatisRoutedClient::connect,replicas);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schema,snapshotHomes,recoveries,RatisRoutedClient::connect);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,HomeClientFactory homeFactory) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schema,snapshotHomes,recoveries,homeFactory,SchemaReplicaConfig.defaults());
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,HomeClientFactory homeFactory,SchemaReplicaConfig replicas) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schema,snapshotHomes,recoveries,homeFactory,replicas,() -> new RatisSchemaReplicaClient(RatisSchemaReplicaClient.MAX_CALLS));
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,HomeClientFactory homeFactory,SchemaReplicaConfig replicas,SchemaReplicaClientFactory replicaFactory) throws IOException {
        if(factory==DEFAULT_SERVER_FACTORY)RatisStreamingTransport.server(plan,new RaftProperties());
        var node=new RatisNodeRuntime(plan,root,epochs,routed,scheduler,timer,time,settings,schema,snapshotHomes,recoveries,replicas);
        try { node.initialize(config,startup,factory,homeFactory,replicaFactory); return node; }
        catch (IOException | RuntimeException | Error failure) {
            try { node.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    private RatisNodeRuntime(RatisNodePlan plan,Path root,RatisSessionEpochs epochs,RatisRoutedClient routed,
            RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,SchemaReplicaConfig replicas) {
        this.plan=Objects.requireNonNull(plan); this.root=root.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs);
        this.routed=Objects.requireNonNull(routed); this.scheduler=Objects.requireNonNull(scheduler); this.timer=Objects.requireNonNull(timer);
        this.time=Objects.requireNonNull(time); this.settings=Objects.requireNonNull(settings); schemaReplicaConfig=Objects.requireNonNull(replicas); logicalSchema=schema; this.snapshotHomes=snapshotHomes; recoverySettings=recoveries;
        if (!plan.imports().isEmpty() && schema==null) throw new IllegalArgumentException("Planned native imports require their qualified logical schema");
        if (recoveries!=null) {
            recoveries.homes().requireDatabaseBinding(plan.directory().clusterId(),plan.directory().databaseId());
            var home=plan.home(RatisNodePlan.Role.TRANSACTION_HOME); if (home!=null) recoveries.homes().requireDirectoryBinding(home.directory());
        }
        if (snapshotHomes!=null) {
            snapshotHomes.requireDatabaseBinding(plan.directory().clusterId(),plan.directory().databaseId());
            var home=plan.home(RatisNodePlan.Role.SNAPSHOT_HOME); if (home!=null) snapshotHomes.requireDirectoryBinding(home.directory());
        }
        if (schema!=null) {
            schema.requireMatches(plan.directory());
            var merges=new HashSet<UUID>();
            for (var column:schema.columns()) if (column.operator()!=null) merges.add(column.columnId());
            if (!plan.merges().equals(merges)) throw new IllegalArgumentException("Node merge bindings differ from qualified schema");
        } else if (!plan.merges().isEmpty()) throw new IllegalArgumentException("Node merge bindings require a qualified schema");
        if (plan.localGroups().stream().anyMatch(group -> group.getGroupId().getUuid().equals(plan.catalogue().groupId()))
                && plan.directory().ranges().size()+plan.homes().stream().mapToInt(home -> home.directory().ranges().size()).sum()>settings.maxPropagationRanges()) throw new IllegalArgumentException("Node clock propagation scope exceeds configured range budget");
        var catalogue=plan.catalogue(); epochs.requireCatalogueBinding(plan.directory().clusterId(),plan.directory().databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
        if (!Arrays.equals(routed.bootstrapDirectory().payloadDigest(),plan.directory().payloadDigest())) throw new IllegalArgumentException("Node routed bootstrap directory differs from placement plan");
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
        RatisTransactionStatuses transactionHomes=recoverySettings==null ? null : recoverySettings.homes();
        lockChannel=FileChannel.open(root.resolve("node.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        try { lock=lockChannel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Node root already owned",owned); }
        if (lock==null) throw new IOException("Node root already owned");
        Path identity=root.resolve("node.identity"), logs=root.resolve("raft"), nativePath=root.resolve("native");
        if (Files.exists(identity)) {
            boolean exposed=identityExposed(SessionJournalFiles.read(identity,128));
            if (!exposed && startup!=Startup.AUTHORIZED_INITIAL_PLACEMENT) throw new IOException("Interrupted initial placement requires explicit authorization to resume");
            if (exposed) {
                if (!Files.isRegularFile(nativePath.resolve("CURRENT"))) throw new IOException("Established native state is missing; refuse empty-voter recovery");
                for (var group:plan.localGroups()) if (!Files.isRegularFile(logs.resolve(group.getGroupId().getUuid().toString()).resolve("current/raft-meta"))) {
                    throw new IOException("Established Raft state is missing for "+group.getGroupId());
                }
            }
        } else {
            if (startup!=Startup.AUTHORIZED_INITIAL_PLACEMENT) throw new IOException("Node identity is absent; explicit placement initialization is required");
            for (String directory:List.of("native","raft","sessions","recovery","schema-replicas","referenced-files")) {
                Path path=root.resolve(directory);
                if (Files.exists(path)) try (var entries=Files.list(path)) { if (entries.findAny().isPresent()) throw new IOException("Unidentified node data must not be adopted"); }
            }
            SessionJournalFiles.write(identity,identity(false),128);
        }
        for (String directory:List.of("native","raft","sessions","recovery","schema-replicas")) { Path path=Files.createDirectories(root.resolve(directory)); SessionJournalFiles.force(path); }
        SessionJournalFiles.force(root);
        var allowed=new HashSet<UUID>(); for (var group:plan.localGroups()) allowed.add(group.getGroupId().getUuid());
        try (var entries=Files.list(logs)) {
            for (var path:entries.toList()) if (Files.isDirectory(path)) {
                UUID group; try { group=UUID.fromString(path.getFileName().toString()); } catch (IllegalArgumentException other) { continue; }
                if (!path.getFileName().toString().equals(group.toString())) throw new IOException("Noncanonical persisted Raft group path");
                if (!allowed.contains(group)) throw new IOException("Unplanned persisted Raft group "+group);
            }
        }
        RatisSnapshotFences captureHomes=snapshotHomes;
        var snapshotPlan=plan.home(RatisNodePlan.Role.SNAPSHOT_HOME);
        if (snapshotPlan!=null) {
            if (captureHomes==null) captureHomes=new RatisSnapshotFences(homeClients.get(snapshotPlan.role()),snapshotPlan.column());
        }
        proofQueries=new RatisProofQueryService();
        if (plan.localGroups().stream().anyMatch(group -> plan.role(group.getGroupId().getUuid())==RatisNodePlan.Role.CATALOGUE)) {
            authorityQueries=RatisProofQueryService.authorityLane();
            importPublicationQueries=RatisProofQueryService.importPublicationLane();
        }
        if (transactionHomes!=null) snapshotHelp=new RatisSnapshotHelpService(scheduler);
        var metadataBindings=new HashMap<UUID,String>();
        for (var group:plan.localGroups()) switch (plan.role(group.getGroupId().getUuid())) {
            case DATA -> {}
            case CATALOGUE -> {
                metadataBindings.put(plan.catalogue().directoryColumn(),"catalogue-directory");
                metadataBindings.put(plan.catalogue().sessionsColumn(),"catalogue-sessions");
            }
            case TRANSACTION_HOME, SNAPSHOT_HOME -> {
                var home=plan.home(group.getGroupId().getUuid()); metadataBindings.put(home.column(),home.physicalName());
            }
        }
        pool=logicalSchema==null ? DistributedStoragePool.open(nativePath,config,LoggerFactory.getLogger(RatisNodeRuntime.class))
                : DistributedStoragePool.open(nativePath,config,LoggerFactory.getLogger(RatisNodeRuntime.class),logicalSchema,plan.columns(),metadataBindings);
        pool.configureSnapshotAdmission(settings.exports().pins());
        if (plan.localGroups().stream().anyMatch(group -> plan.role(group.getGroupId().getUuid())==RatisNodePlan.Role.DATA))
            schemaReplicas=new RatisSchemaReplicaService(nativePath,root.resolve("schema-replicas"),pool,scheduler,schemaReplicaConfig.policy(),schemaReplicaConfig.limits());
        if (plan.localGroups().stream().anyMatch(group -> plan.role(group.getGroupId().getUuid())==RatisNodePlan.Role.CATALOGUE)) {
            schemaFencingReplicas=Objects.requireNonNull(replicaFactory.create());
            importOwners=new RatisRangeImportOwnerClient(plan.directory().clusterId(),plan.directory().databaseId(),plan.catalogue().groupId());
        }
        if (plan.localGroups().stream().anyMatch(group -> plan.importDefinition(group.getGroupId().getUuid())!=null))
            importFileIo=new ThreadPoolExecutor(2,2,0L,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(32),
                    Thread.ofPlatform().daemon(true).name("ratis-import-file-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
        try {
            for (var group:plan.localGroups()) {
                var id=group.getGroupId().getUuid(); RatisRangeStateMachine machine;
                var role=plan.role(id);
                if (role==RatisNodePlan.Role.CATALOGUE) {
                    var c=plan.catalogue();
                    var store=pool.openRange(new RangeOwnership.Owner(id,c.epoch()),new RangeOwnership.Interval(id,Set.of(c.directoryColumn(),c.sessionsColumn()),null,null),
                            c.schemaDigest(),Map.of(c.directoryColumn(),"catalogue-directory",c.sessionsColumn(),"catalogue-sessions"),Set.of());
                    var operatorLimits=new RatisOperatorCatalogue.Limits(schemaReplicaConfig.limits().maximumUploads(),schemaReplicaConfig.limits().maximumDiskBytes());
                    var operators=RatisOperatorCatalogue.validator(plan.directory().clusterId(),plan.directory().databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),operatorLimits,this::requireOperatorAdmission,key -> committedCatalogueValue(store,c.directoryColumn(),key));
                    machine=new RatisRangeStateMachine(store,group,settings.images(),RatisCatalogue.validator(plan.directory().clusterId(),plan.directory().databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),c.sessionsColumn(),operators,key -> committedCatalogueValue(store,c.directoryColumn(),key)));
                    machine.role(role);
                    machine.schemaCatalogue(plan.directory().clusterId(),plan.directory().databaseId(),c.directoryColumn());
                    machine.schemaFencingReplicaClient(schemaFencingReplicas);
                    machine.schemaAuthorityQueries(authorityQueries);
                    machine.importPublicationQueries(importPublicationQueries);
                    machine.importPublicationAdmission(command->{
                        if(RatisRangeImportCatalogue.isPublication(command))RatisRangeImportCatalogue.admitPublication(command,plan.directory().clusterId(),plan.directory().databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),key->committedCatalogueValue(store,c.directoryColumn(),key),importOwners::receipt);
                        else RatisRangeImportCatalogue.admitCompletion(command,plan.directory().clusterId(),plan.directory().databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),key->committedCatalogueValue(store,c.directoryColumn(),key),importOwners::activation);
                    });
                } else {
                    var directory=plan.groupDirectory(id); var descriptor=plan.groupDescriptor(id); var home=plan.home(id);
                    var bindings=home==null ? plan.columns() : Map.of(home.column(),home.physicalName());
                    var importDefinition=plan.importDefinition(id);
                    var store=importDefinition==null ? pool.openRange(new RangeOwnership.Owner(id,descriptor.rangeEpoch()),new RangeOwnership.Interval(directory.domainId(),directory.columns(),descriptor.lowerInclusive(),descriptor.upperExclusive()),
                            directory.schemaDigest(),bindings,home==null ? plan.merges() : Set.of())
                            : startup==Startup.RECOVER ? pool.reopenImportRange(importDefinition.manifest(id),logicalSchema)
                            : pool.openNewFencedRange(importDefinition.manifest(id),logicalSchema);
                    java.util.function.Consumer<it.cavallium.rockserver.core.common.distributed.DistributedCommand> validator=command -> {};
                    if(role==RatisNodePlan.Role.DATA) store.enableCdc(descriptor.rangeId());
                    if (role==RatisNodePlan.Role.TRANSACTION_HOME) validator=RatisTransactionStatuses.validator(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),directory.schemaDigest(),home.column());
                    else if (role==RatisNodePlan.Role.SNAPSHOT_HOME) validator=RatisSnapshotFences.validator(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),directory.schemaDigest(),home.column());
                    if (role==RatisNodePlan.Role.DATA && logicalSchema!=null) {
                        var effectiveSchema=store.installedSchema();
                        store.enableLogicalSchema(effectiveSchema==null ? logicalSchema : effectiveSchema);
                    }
                    var effectiveDirectory=role==RatisNodePlan.Role.DATA && store.schemaInstallation()!=null
                            ? store.schemaInstallation().definition().targetDirectory()
                            : importDefinition==null ? directory : importDefinition.targetDirectory();
                    // Bind future session controls to the frozen published placement; the native import fence
                    // still rejects DATA/control admission until the exact catalogue activation is applied.
                    machine=RatisRangeStateMachine.withSessions(store,group,settings.images(),validator,epochs,effectiveDirectory,settings.retention());
                    if (role==RatisNodePlan.Role.TRANSACTION_HOME) {
                        machine.role(role,(key,value) -> RatisTransactionStatuses.validateRecord(directory.clusterId(),directory.databaseId(),key,value));
                        machine.homeRecoveryData(routed); machine.homeRecoveryReservations(directory.clusterId(),directory.databaseId(),home.column());
                    }
                    else if (role==RatisNodePlan.Role.SNAPSHOT_HOME) {
                        machine.role(role,(key,value) -> RatisSnapshotFences.validateRecord(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),key,value));
                        machine.snapshotHomeStorage(directory.clusterId(),directory.databaseId(),home.column()); machine.snapshotHomeData(routed);
                    }
                    else { machine.role(role); machine.schemaStorage(pool); machine.schemaActivated(this::observeSchemaActivation); var authority=epochs.schemaAuthority(); machine.schemaAuthority((header,digest) -> readSchemaAuthority(authority,header,digest),effectiveDirectory); machine.schemaReplicas(schemaReplicas,ownReplica(descriptor)); if (captureHomes!=null) machine.snapshotHomes(captureHomes); if (transactionHomes!=null) { machine.transactionHomes(transactionHomes); machine.snapshotRecovery(this::recoverParticipant,snapshotHelp); } }
                    if (importDefinition!=null) {
                        var limits=new RatisReferencedFiles.Limits(4096,8,32,
                                it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_FILE_BYTES,
                                it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_TOTAL_BYTES
                                        +it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_BYTES);
                        var files=new RatisReferencedFiles(root.resolve("referenced-files").resolve(id.toString()),id,limits,importFileIo,
                                request->RatisRangeImportRpc.requireFile(importDefinition.manifest(id),request));
                        importFiles.put(id,files); machine.referencedFiles(files);
                        machine.rangeImportApply(new RatisRangeImportApply(pool,store,files,importDefinition),epochs.importAuthority(),ownReplica(descriptor));
                    }
                }
                machine.proofQueries(proofQueries);
                machines.put(group.getGroupId(),machine);
            }
            for (var entry:machines.entrySet()) if (plan.role(entry.getKey().getUuid())==RatisNodePlan.Role.DATA)
                for (var schema:entry.getValue().schemaRoutingHistory()) routed.refreshSchema(schema.definition(),schema.proof());
            startupDirectory=routed.directory();
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
        for (var entry:machines.entrySet()) if (plan.role(entry.getKey().getUuid())==RatisNodePlan.Role.DATA)
            entry.getValue().exportSources(exportSources);
        var retirementSources=new ArrayList<RatisRetirementCoordinator.Source>();
        for (var id:machines.keySet().stream().sorted(java.util.Comparator.comparing(RaftGroupId::toString)).toList()) {
            var machine=machines.get(id); var role=plan.role(id.getUuid());
            if (role==RatisNodePlan.Role.TRANSACTION_HOME) {
                retirementSources.add(new RatisRetirementCoordinator.Source(id.getUuid(),RatisRetirementCoordinator.Mode.TRANSACTION,(after,scans) -> machine.retirementCandidates(false,after,scans)));
                retirementSources.add(new RatisRetirementCoordinator.Source(id.getUuid(),RatisRetirementCoordinator.Mode.ORPHAN,(after,scans) -> machine.retirementCandidates(true,after,scans)));
            } else if (role==RatisNodePlan.Role.SNAPSHOT_HOME) retirementSources.add(new RatisRetirementCoordinator.Source(id.getUuid(),RatisRetirementCoordinator.Mode.SNAPSHOT,(after,scans) -> machine.retirementCandidates(false,after,scans)));
        }
        if (!retirementSources.isEmpty()) {
            retirementCoordinator=new RatisRetirementCoordinator(root.resolve("retirement"),transactionHomes,captureHomes,routed,retirementSources,RatisRetirementJournal.Limits.defaults());
            retirementService=new RatisTransactionRecoveryService(scheduler,() -> retirementCoordinator.cycle(4));
        }
        server=factory.create(plan,logs,Collections.unmodifiableMap(machines));
        transactionExpiry=new RatisTransactionExpiryService(scheduler,machines.entrySet().stream().filter(entry -> plan.role(entry.getKey().getUuid())==RatisNodePlan.Role.DATA).map(Map.Entry::getValue).toList(),32);
        var local=new HashSet<UUID>(); for (var id:machines.keySet()) if (plan.role(id.getUuid())==RatisNodePlan.Role.DATA) local.add(id.getUuid());
        if (recoverySettings!=null && !local.isEmpty()) {
            participantRecoveries=new RatisParticipantRecoveryRegistry(root.resolve("recovery"),recoverySettings.homes(),routed,recoverySettings.takeover(),local,recoverySettings.limits());
            var sources=new ArrayList<RatisNativeParticipantDiscovery.Source>();
            for (var group:local.stream().sorted().toList()) sources.add(new RatisNativeParticipantDiscovery.Source(group,machines.get(RaftGroupId.valueOf(group))::preparedRecoveries));
            nativeParticipantDiscovery=new RatisNativeParticipantDiscovery(participantRecoveries,sources);
            participantRecoveryService=new RatisTransactionRecoveryService(scheduler,() -> {
                nativeParticipantDiscovery.cycle(recoverySettings.visits());
            });
        }
        if (!machines.containsKey(RaftGroupId.valueOf(plan.catalogue().groupId()))) return;
        clock=new RatisCatalogueClock(root.resolve("sessions/catalogue-clock.bin"),epochs);
        propagation=new RatisSessionPropagation(root.resolve("sessions"),epochs,routed,settings.maxPropagationRanges()); propagations.add(propagation);
        for (var home:plan.homes()) {
            Path scope=Files.createDirectories(root.resolve("sessions").resolve(home.role().name().toLowerCase(Locale.ROOT))); SessionJournalFiles.force(scope); SessionJournalFiles.force(scope.getParent());
            propagations.add(new RatisSessionPropagation(scope,epochs,homeClients.get(home.role()),settings.maxPropagationRanges()));
        }
        service=new RatisSessionClockService(scheduler,clock,propagations,time,settings.propagationVisits(),this::clockOwnerReady);
    }
    public void start() throws IOException {
        synchronized (this) {
            if (starting || started || closeRequested || closed) throw new IllegalStateException("Node cannot start"); starting=true;
        }
        try {
            if (!Arrays.equals(routed.directory().payloadDigest(),startupDirectory.payloadDigest())) throw new IOException("Node placement changed before start");
            // Persist before the first possible network/voting exposure, including a start attempt that later fails.
            SessionJournalFiles.write(root.resolve("node.identity"),identity(true),128);
            server.start(); var existing=new HashSet<RaftGroupId>(); server.getGroupIds().forEach(existing::add);
            if (!machines.keySet().containsAll(existing)) throw new IOException("Server recovered an unplanned group");
            long call=0;
            for (var group:plan.localGroups()) if (!existing.contains(group.getGroupId())) {
                var reply=server.groupManagement(GroupManagementRequest.newAdd(ClientId.randomId(),server.getId(),++call,group,false));
                if (reply==null || !reply.isSuccess()) throw new IOException("Local group addition failed: "+group.getGroupId(),reply==null ? null : reply.getException());
            }
            var actualIds=new HashSet<RaftGroupId>(); server.getGroupIds().forEach(actualIds::add);
            if (!actualIds.equals(machines.keySet())) throw new IOException("Server division coverage differs from placement");
            for (var group:plan.localGroups()) verify(group);
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
        routed.refreshSchema(definition,proof);
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
        var division=server.getDivision(group.getGroupId()); var configuration=division.getRaftConf();
        for (var role:List.of(RaftPeerRole.FOLLOWER,RaftPeerRole.LISTENER)) {
            if (!configuration.getPreviousPeers(role).isEmpty()) throw new IOException("Joint membership requires topology reconciliation");
            var expected=new TreeMap<String,String>(); var actual=new TreeMap<String,String>();
            for (var peer:group.getPeers()) if (peer.getStartupRole()==role) expected.put(peer.getId().toString(),peer.getAddress()+"|"+Objects.toString(peer.getDataStreamAddress(),""));
            for (var peer:configuration.getCurrentPeers(role)) actual.put(peer.getId().toString(),peer.getAddress()+"|"+Objects.toString(peer.getDataStreamAddress(),""));
            if (!actual.equals(expected)) throw new IOException("Recovered group membership differs from static placement: "+group.getGroupId());
        }
        if (division.getStateMachine()!=machines.get(group.getGroupId())) throw new IOException("Recovered state-machine owner differs from planned group");
    }
    @Override public void close() throws IOException {
        synchronized (this) {
            if (closed) return;
            if (starting || closing) throw new IOException("Node lifecycle operation is active"); closeRequested=true; closing=true;
        }
        try {
            // Ordinary proofs can await both catalogue authority and local replica preparation.
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
            if (importOwners!=null && !importOwnersClosed) { importOwners.close(); importOwnersClosed=true; }
            if (pool!=null && !poolClosed) { pool.close(); poolClosed=true; }
            for (var client:homeClients.values()) client.close();
            if (lock!=null && lock.isValid()) lock.release(); if (lockChannel!=null) lockChannel.close();
            synchronized (this) { closed=true; }
        } finally { synchronized (this) { closing=false; } }
    }
    private static RaftServer buildServer(RatisNodePlan plan,Path logs,Map<RaftGroupId,RatisRangeStateMachine> machines) throws IOException {
        RaftProperties properties=RatisDurability.properties(logs); org.apache.ratis.RaftConfigKeys.Rpc.setType(properties,org.apache.ratis.rpc.SupportedRpcType.GRPC);
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
