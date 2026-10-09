package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
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
    public record RecoverySettings(RatisTransactionStatuses homes,boolean takeover,RatisParticipantRecoveryRegistry.Limits limits,Duration period,int visits) {
        public RecoverySettings {
            Objects.requireNonNull(homes); Objects.requireNonNull(limits); Objects.requireNonNull(period);
            if (period.isNegative() || period.isZero() || period.compareTo(Duration.ofMinutes(1))>0 || visits<1 || visits>32) throw new IllegalArgumentException("Invalid participant recovery settings");
        }
    }
    private final RatisNodePlan plan; private final DistributedSchema logicalSchema; private final Path root; private final Settings settings;
    private final RatisSessionEpochs epochs; private final RatisRoutedClient routed; private final RatisSnapshotFences snapshotHomes;
    private final RWScheduler scheduler; private final Scheduler timer; private final LongSupplier time;
    private final Map<RaftGroupId,RatisRangeStateMachine> machines=new LinkedHashMap<>();
    private DistributedStoragePool pool; private RaftServer server; private RatisCatalogueClock clock;
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
    private RatisExportSourceService exportSources;
    private FileChannel lockChannel; private FileLock lock;
    private boolean started,starting,closeRequested,closing,closed,serverClosed,poolClosed;

    /** Fresh placement is an explicit trusted bootstrap assertion, never inferred from the directory. Borrowed clients/schedulers remain caller-owned. */
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,RatisNodeRuntime::buildServer,null,null);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,null,null);
    }
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,DistributedSchema schema) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,RatisNodeRuntime::buildServer,schema,null);
    }
    /** Operator-qualified home readers are fixed on DATA state machines before the server factory may initialize them. */
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,DistributedSchema schema,RatisSnapshotFences snapshotHomes) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,RatisNodeRuntime::buildServer,schema,snapshotHomes);
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
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,RatisNodeRuntime::buildServer,schema,snapshotHomes,recoveries);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,schema,snapshotHomes,recoveries,RatisRoutedClient::connect);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries,HomeClientFactory homeFactory) throws IOException {
        var node=new RatisNodeRuntime(plan,root,epochs,routed,scheduler,timer,time,settings,schema,snapshotHomes,recoveries);
        try { node.initialize(config,startup,factory,homeFactory); return node; }
        catch (IOException | RuntimeException | Error failure) {
            try { node.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    private RatisNodeRuntime(RatisNodePlan plan,Path root,RatisSessionEpochs epochs,RatisRoutedClient routed,
            RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,DistributedSchema schema,RatisSnapshotFences snapshotHomes,RecoverySettings recoveries) {
        this.plan=Objects.requireNonNull(plan); this.root=root.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs);
        this.routed=Objects.requireNonNull(routed); this.scheduler=Objects.requireNonNull(scheduler); this.timer=Objects.requireNonNull(timer);
        this.time=Objects.requireNonNull(time); this.settings=Objects.requireNonNull(settings); logicalSchema=schema; this.snapshotHomes=snapshotHomes; recoverySettings=recoveries;
        if (recoveries!=null) {
            recoveries.homes().requireDatabaseBinding(plan.directory().clusterId(),plan.directory().databaseId());
            var home=plan.home(RatisNodePlan.Role.TRANSACTION_HOME); if (home!=null) recoveries.homes().requireDirectoryBinding(home.directory());
        }
        if (snapshotHomes!=null) {
            snapshotHomes.requireDatabaseBinding(plan.directory().clusterId(),plan.directory().databaseId());
            var home=plan.home(RatisNodePlan.Role.SNAPSHOT_HOME); if (home!=null) snapshotHomes.requireDirectoryBinding(home.directory());
        }
        if (schema!=null) schema.requireMatches(plan.directory());
        if (plan.localGroups().stream().anyMatch(group -> group.getGroupId().getUuid().equals(plan.catalogue().groupId()))
                && plan.directory().ranges().size()+plan.homes().stream().mapToInt(home -> home.directory().ranges().size()).sum()>settings.maxPropagationRanges()) throw new IllegalArgumentException("Node clock propagation scope exceeds configured range budget");
        var catalogue=plan.catalogue(); epochs.requireCatalogueBinding(plan.directory().clusterId(),plan.directory().databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
        if (!Arrays.equals(routed.directory().payloadDigest(),plan.directory().payloadDigest())) throw new IllegalArgumentException("Node routed directory differs from placement plan");
    }
    private void initialize(DatabaseConfig config,Startup startup,ServerFactory factory,HomeClientFactory homeFactory) throws IOException {
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
            for (String directory:List.of("native","raft","sessions","recovery")) {
                Path path=root.resolve(directory);
                if (Files.exists(path)) try (var entries=Files.list(path)) { if (entries.findAny().isPresent()) throw new IOException("Unidentified node data must not be adopted"); }
            }
            SessionJournalFiles.write(identity,identity(false),128);
        }
        for (String directory:List.of("native","raft","sessions","recovery")) { Path path=Files.createDirectories(root.resolve(directory)); SessionJournalFiles.force(path); }
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
        if (transactionHomes!=null) snapshotHelp=new RatisSnapshotHelpService(scheduler);
        pool=DistributedStoragePool.open(nativePath,config,LoggerFactory.getLogger(RatisNodeRuntime.class));
        pool.configureSnapshotAdmission(settings.exports().pins());
        try {
            for (var group:plan.localGroups()) {
                var id=group.getGroupId().getUuid(); RatisRangeStateMachine machine;
                var role=plan.role(id);
                if (role==RatisNodePlan.Role.CATALOGUE) {
                    var c=plan.catalogue();
                    var store=pool.openRange(new RangeOwnership.Owner(id,c.epoch()),new RangeOwnership.Interval(id,Set.of(c.directoryColumn(),c.sessionsColumn()),null,null),
                            c.schemaDigest(),Map.of(c.directoryColumn(),"catalogue-directory",c.sessionsColumn(),"catalogue-sessions"),Set.of());
                    machine=new RatisRangeStateMachine(store,group,settings.images(),RatisCatalogue.validator(plan.directory().clusterId(),plan.directory().databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),c.sessionsColumn()));
                    machine.role(role);
                    machine.schemaCatalogue(plan.directory().clusterId(),plan.directory().databaseId(),c.directoryColumn());
                } else {
                    var directory=plan.groupDirectory(id); var descriptor=plan.groupDescriptor(id); var home=plan.home(id);
                    var bindings=home==null ? plan.columns() : Map.of(home.column(),home.physicalName());
                    var store=pool.openRange(new RangeOwnership.Owner(id,descriptor.rangeEpoch()),new RangeOwnership.Interval(directory.domainId(),directory.columns(),descriptor.lowerInclusive(),descriptor.upperExclusive()),
                            directory.schemaDigest(),bindings,home==null ? plan.merges() : Set.of());
                    java.util.function.Consumer<it.cavallium.rockserver.core.common.distributed.DistributedCommand> validator=command -> {};
                    if (role==RatisNodePlan.Role.TRANSACTION_HOME) validator=RatisTransactionStatuses.validator(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),directory.schemaDigest(),home.column());
                    else if (role==RatisNodePlan.Role.SNAPSHOT_HOME) validator=RatisSnapshotFences.validator(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),directory.schemaDigest(),home.column());
                    if (role==RatisNodePlan.Role.DATA && logicalSchema!=null) store.enableLogicalSchema(logicalSchema);
                    machine=RatisRangeStateMachine.withSessions(store,group,settings.images(),validator,epochs,directory,settings.retention());
                    if (role==RatisNodePlan.Role.TRANSACTION_HOME) {
                        machine.role(role,(key,value) -> RatisTransactionStatuses.validateRecord(directory.clusterId(),directory.databaseId(),key,value));
                        machine.homeRecoveryData(routed); machine.homeRecoveryReservations(directory.clusterId(),directory.databaseId(),home.column());
                    }
                    else if (role==RatisNodePlan.Role.SNAPSHOT_HOME) {
                        machine.role(role,(key,value) -> RatisSnapshotFences.validateRecord(directory.clusterId(),directory.databaseId(),id,descriptor.rangeEpoch(),key,value));
                        machine.snapshotHomeStorage(directory.clusterId(),directory.databaseId(),home.column()); machine.snapshotHomeData(routed);
                    }
                    else { machine.role(role); var authority=epochs.schemaAuthority(); machine.schemaAuthority(header -> authority.read(header.domainId()),directory); if (captureHomes!=null) machine.snapshotHomes(captureHomes); if (transactionHomes!=null) { machine.transactionHomes(transactionHomes); machine.snapshotRecovery(this::recoverParticipant,snapshotHelp); } }
                }
                machine.proofQueries(proofQueries);
                machines.put(group.getGroupId(),machine);
            }
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
            if (!Arrays.equals(routed.directory().payloadDigest(),plan.directory().payloadDigest())) throw new IOException("Node placement changed before start");
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
    private byte[] identity(boolean exposed) {
        return ByteBuffer.allocate(33).put((byte)(exposed ? 1 : 0)).put(plan.identityDigest()).array();
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
            for (var peer:group.getPeers()) if (peer.getStartupRole()==role) expected.put(peer.getId().toString(),peer.getAddress());
            for (var peer:configuration.getCurrentPeers(role)) actual.put(peer.getId().toString(),peer.getAddress());
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
            if (proofQueries!=null) {
                try { proofQueries.stop().get(settings.shutdownTimeout().toNanos(),TimeUnit.NANOSECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException("Proof query drain interrupted",interrupted); }
                catch (ExecutionException | TimeoutException failure) { throw new IOException("Proof queries did not drain",failure); }
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
        return org.apache.ratis.server.impl.RockserverRaftServers.create(RaftPeerId.valueOf(plan.peerId()),plan.localGroups().getFirst(),
                RaftStorage.StartupOption.RECOVER,id -> {
                    var machine=machines.get(id); if (machine==null) throw new IllegalArgumentException("Unplanned state-machine group"); return machine;
                },properties,RatisSnapshotAckInterceptor.parameters((id,term,index) -> {
                    var machine=machines.get(id); if (machine==null) throw new IOException("Unplanned snapshot group"); machine.syncReceivedSnapshot(term,index);
                }));
    }
}
