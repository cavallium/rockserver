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
    public record Settings(NativeRangeImage.Limits images,RatisRangeStateMachine.RetentionLimits retention,
                           Duration clockPeriod,int propagationVisits,int maxPropagationRanges,Duration shutdownTimeout) {
        public Settings {
            Objects.requireNonNull(images); Objects.requireNonNull(retention); Objects.requireNonNull(clockPeriod); Objects.requireNonNull(shutdownTimeout);
            if (clockPeriod.isNegative() || clockPeriod.isZero() || propagationVisits<1 || propagationVisits>32
                    || maxPropagationRanges<1 || maxPropagationRanges>1024 || shutdownTimeout.isNegative() || shutdownTimeout.isZero()
                    || shutdownTimeout.compareTo(Duration.ofMinutes(1))>0) throw new IllegalArgumentException("Invalid node resource/lifecycle settings");
        }
    }
    private final RatisNodePlan plan; private final DistributedSchema logicalSchema; private final Path root; private final Settings settings;
    private final RatisSessionEpochs epochs; private final RatisRoutedClient routed;
    private final RWScheduler scheduler; private final Scheduler timer; private final LongSupplier time;
    private final Map<RaftGroupId,RatisRangeStateMachine> machines=new LinkedHashMap<>();
    private DistributedStoragePool pool; private RaftServer server; private RatisCatalogueClock clock;
    private RatisSessionPropagation propagation; private RatisSessionClockService service;
    private RatisTransactionExpiryService transactionExpiry;
    private FileChannel lockChannel; private FileLock lock;
    private boolean started,starting,closeRequested,closing,closed,serverClosed,poolClosed;

    /** Fresh placement is an explicit trusted bootstrap assertion, never inferred from the directory. Borrowed clients/schedulers remain caller-owned. */
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,RatisNodeRuntime::buildServer);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,factory,null);
    }
    public static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,DistributedSchema schema) throws IOException {
        return open(plan,root,config,epochs,routed,scheduler,timer,time,settings,startup,RatisNodeRuntime::buildServer,schema);
    }
    static RatisNodeRuntime open(RatisNodePlan plan,Path root,DatabaseConfig config,RatisSessionEpochs epochs,
            RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,Startup startup,ServerFactory factory,DistributedSchema schema) throws IOException {
        var node=new RatisNodeRuntime(plan,root,epochs,routed,scheduler,timer,time,settings,schema);
        try { node.initialize(config,startup,factory); return node; }
        catch (IOException | RuntimeException | Error failure) {
            try { node.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    private RatisNodeRuntime(RatisNodePlan plan,Path root,RatisSessionEpochs epochs,RatisRoutedClient routed,
            RWScheduler scheduler,Scheduler timer,LongSupplier time,Settings settings,DistributedSchema schema) {
        this.plan=Objects.requireNonNull(plan); this.root=root.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs);
        this.routed=Objects.requireNonNull(routed); this.scheduler=Objects.requireNonNull(scheduler); this.timer=Objects.requireNonNull(timer);
        this.time=Objects.requireNonNull(time); this.settings=Objects.requireNonNull(settings); logicalSchema=schema;
        if (schema!=null) schema.requireMatches(plan.directory());
        if (plan.localGroups().stream().anyMatch(group -> group.getGroupId().getUuid().equals(plan.catalogue().groupId()))
                && plan.directory().ranges().size()>settings.maxPropagationRanges()) throw new IllegalArgumentException("Node clock propagation scope exceeds configured range budget");
        var catalogue=plan.catalogue(); epochs.requireCatalogueBinding(plan.directory().clusterId(),plan.directory().databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
        if (!Arrays.equals(routed.directory().payloadDigest(),plan.directory().payloadDigest())) throw new IllegalArgumentException("Node routed directory differs from placement plan");
    }
    private void initialize(DatabaseConfig config,Startup startup,ServerFactory factory) throws IOException {
        Objects.requireNonNull(startup); Objects.requireNonNull(factory);
        if (!Files.isDirectory(root)) throw new IOException("Node root must already exist on durable storage");
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
            for (String directory:List.of("native","raft","sessions")) {
                Path path=root.resolve(directory);
                if (Files.exists(path)) try (var entries=Files.list(path)) { if (entries.findAny().isPresent()) throw new IOException("Unidentified node data must not be adopted"); }
            }
            SessionJournalFiles.write(identity,identity(false),128);
        }
        for (String directory:List.of("native","raft","sessions")) { Path path=Files.createDirectories(root.resolve(directory)); SessionJournalFiles.force(path); }
        SessionJournalFiles.force(root);
        var allowed=new HashSet<UUID>(); for (var group:plan.localGroups()) allowed.add(group.getGroupId().getUuid());
        try (var entries=Files.list(logs)) {
            for (var path:entries.toList()) if (Files.isDirectory(path)) {
                UUID group; try { group=UUID.fromString(path.getFileName().toString()); } catch (IllegalArgumentException other) { continue; }
                if (!path.getFileName().toString().equals(group.toString())) throw new IOException("Noncanonical persisted Raft group path");
                if (!allowed.contains(group)) throw new IOException("Unplanned persisted Raft group "+group);
            }
        }
        pool=DistributedStoragePool.open(nativePath,config,LoggerFactory.getLogger(RatisNodeRuntime.class));
        try {
            for (var group:plan.localGroups()) {
                var id=group.getGroupId().getUuid(); RatisRangeStateMachine machine;
                if (id.equals(plan.catalogue().groupId())) {
                    var c=plan.catalogue();
                    var store=pool.openRange(new RangeOwnership.Owner(id,c.epoch()),new RangeOwnership.Interval(id,Set.of(c.directoryColumn(),c.sessionsColumn()),null,null),
                            c.schemaDigest(),Map.of(c.directoryColumn(),"catalogue-directory",c.sessionsColumn(),"catalogue-sessions"),Set.of());
                    machine=new RatisRangeStateMachine(store,group,settings.images(),RatisCatalogue.validator(plan.directory().clusterId(),plan.directory().databaseId(),id,c.epoch(),c.schemaDigest(),c.directoryColumn(),c.sessionsColumn()));
                } else {
                    var descriptor=plan.directory().ranges().stream().filter(range -> range.groupId().equals(id)).findFirst().orElseThrow();
                    var store=pool.openRange(new RangeOwnership.Owner(id,descriptor.rangeEpoch()),new RangeOwnership.Interval(plan.directory().domainId(),plan.directory().columns(),descriptor.lowerInclusive(),descriptor.upperExclusive()),
                            plan.directory().schemaDigest(),plan.columns(),plan.merges());
                    if (logicalSchema!=null) store.enableLogicalSchema(logicalSchema);
                    machine=RatisRangeStateMachine.withSessions(store,group,settings.images(),command -> {},epochs,plan.directory(),settings.retention());
                }
                machines.put(group.getGroupId(),machine);
            }
        } catch (org.rocksdb.RocksDBException failure) { throw new IOException("Native node groups could not open",failure); }
        server=factory.create(plan,logs,Collections.unmodifiableMap(machines));
        transactionExpiry=new RatisTransactionExpiryService(scheduler,List.copyOf(machines.values()),32);
        if (!machines.containsKey(RaftGroupId.valueOf(plan.catalogue().groupId()))) return;
        clock=new RatisCatalogueClock(root.resolve("sessions/catalogue-clock.bin"),epochs);
        propagation=new RatisSessionPropagation(root.resolve("sessions"),epochs,routed,settings.maxPropagationRanges());
        service=new RatisSessionClockService(scheduler,clock,propagation,time,settings.propagationVisits(),this::clockOwnerReady);
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
            synchronized (this) { started=true; }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) { closeRequested=true; }
            for (var machine:machines.values()) machine.beginShutdown(); throw failure;
        } finally { synchronized (this) { starting=false; } }
    }
    private byte[] identity(boolean exposed) {
        return ByteBuffer.allocate(37).putInt(1).put((byte)(exposed ? 1 : 0)).put(plan.identityDigest()).array();
    }
    private boolean identityExposed(byte[] bytes) throws IOException {
        if (bytes.length!=37) throw new IOException("Invalid node identity shape");
        var input=ByteBuffer.wrap(bytes);
        if (input.getInt()!=1) throw new IOException("Invalid node identity version");
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
            if (propagation!=null) propagation.close(); if (clock!=null) clock.close();
            for (var machine:machines.values()) machine.beginShutdown();
            if (server!=null && !serverClosed) { server.close(); serverClosed=true; }
            for (var machine:machines.values()) machine.awaitNativeReaders(settings.shutdownTimeout());
            if (pool!=null && !poolClosed) { pool.close(); poolClosed=true; }
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
