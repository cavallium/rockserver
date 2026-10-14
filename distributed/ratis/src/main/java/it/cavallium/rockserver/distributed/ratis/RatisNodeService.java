package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.*;
import java.net.URI;
import it.cavallium.rockserver.core.server.Server;
import it.cavallium.rockserver.core.server.ServerBuilder;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.*;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.TimeDuration;
import reactor.core.scheduler.*;

/** Owns one established static node and its shared clients/scheduler. Starting is not catalogue or quorum readiness. */
public final class RatisNodeService implements AutoCloseable {
    static final class Resources implements AutoCloseable {
        final RaftClient catalogue; final Map<UUID,RatisRoutedClient> routed; final RWScheduler scheduler; final Scheduler timer;
        private boolean catalogueClosed,schedulerClosed,timerClosed;
        private final Set<UUID> routedClosed=new HashSet<>();
        Resources(RaftClient catalogue,Map<UUID,RatisRoutedClient> routed,RWScheduler scheduler,Scheduler timer) {
            this.catalogue=Objects.requireNonNull(catalogue); this.routed=Map.copyOf(routed);
            if(this.routed.isEmpty() || this.routed.size()>DatabaseDomainRoster.MAX_DOMAINS)throw new IllegalArgumentException("Invalid routed domain scope");
            for(var entry:this.routed.entrySet())if(!entry.getKey().equals(entry.getValue().directory().domainId()))throw new IllegalArgumentException("Routed domain identity differs");
            this.scheduler=Objects.requireNonNull(scheduler); this.timer=Objects.requireNonNull(timer);
        }
        void abort(Throwable failure) {
            // No owner is returned after failed open: retire every independent allocation.
            for(var entry:routed.entrySet())try { if (!routedClosed.contains(entry.getKey())) { entry.getValue().close(); routedClosed.add(entry.getKey()); } } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (!catalogueClosed) { catalogue.close(); catalogueClosed=true; } } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (!timerClosed) { timer.dispose(); timerClosed=true; } } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (!schedulerClosed) { scheduler.dispose(); schedulerClosed=true; } } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
        }
        @Override public void close() throws IOException {
            // Dependencies are retired only after the node drains. Preserve failed close handles for retry.
            for(var entry:routed.entrySet())if (!routedClosed.contains(entry.getKey())) { entry.getValue().close(); routedClosed.add(entry.getKey()); }
            if (!catalogueClosed) { catalogue.close(); catalogueClosed=true; }
            if (!timerClosed) { timer.dispose(); timerClosed=true; }
            if (!schedulerClosed) { scheduler.dispose(); schedulerClosed=true; }
        }
    }
    private final RatisNodeManifest manifest; private final Resources resources; private final RatisNodeRuntime node;
    private Map<UUID,RangeDirectory> expectedDirectories; private boolean restorePending,restoreOwned;
    private final RatisSchemaCatalogue schemas; private final RatisDatabaseDomainRoster roster;
    private boolean starting,closing,closed,closeRequested; private int checks;
    private RatisDatabaseBackendFactory frontendBackend;
    private Server frontend;
    private boolean frontendOpening,frontendClosed,exportsClosed,backendClosed;
    @FunctionalInterface interface BackendCreator { RatisDatabaseBackendFactory create() throws IOException; }
    @FunctionalInterface interface ListenerCreator { Server create(RatisRocksDBConnection connection,it.cavallium.rockserver.core.common.export.ExportAsyncAPI exports) throws IOException; }
    private RatisNodeService(RatisNodeManifest manifest,Resources resources,RatisNodeRuntime node) {
        this.manifest=manifest; this.resources=resources; this.node=node; var plan=manifest.plan(); var c=plan.catalogue();
        schemas=new RatisSchemaCatalogue(resources.catalogue,plan.clusterId(),plan.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
        roster=new RatisDatabaseDomainRoster(resources.catalogue,plan.clusterId(),plan.databaseId(),c);
        var expected=new HashMap<UUID,RangeDirectory>();plan.domains().forEach(d->expected.put(d.directory().domainId(),d.directory()));expectedDirectories=Map.copyOf(expected);
    }
    /** Never grants placement-creation authority. Root must contain established native and Raft state. */
    public static RatisNodeService recover(RatisNodeManifest manifest,Path root,DatabaseConfig config) throws IOException {
        return recover(manifest,root,config,RatisNodeRuntime.SchemaReplicaConfig.defaults().policy());
    }
    /** Trusted node-local operator admission; manifest budgets persist independently of this borrowed policy. */
    public static RatisNodeService recover(RatisNodeManifest manifest,Path root,DatabaseConfig config,RatisSchemaReplicaService.OperatorPolicy policy) throws IOException {
        Objects.requireNonNull(manifest); Objects.requireNonNull(config); Objects.requireNonNull(policy); preflight(root,manifest.plan());
        return recover(manifest,root,config,connectResources(manifest,config),null,policy);
    }
    static Resources connectResources(RatisNodeManifest manifest,DatabaseConfig config)throws IOException {
        final WorkloadSettings workload;
        try { workload=WorkloadSettings.resolve(config); } catch (Exception invalid) { throw new IOException("Invalid storage workload configuration",invalid); }
        if (workload.readParallelism()!=manifest.readThreads() || workload.writeParallelism()!=manifest.writeThreads()) {
            throw new IllegalArgumentException("Storage workload capacities differ from node manifest");
        }
        RaftClient client=null; var routed=new LinkedHashMap<UUID,RatisRoutedClient>(); RWScheduler scheduler=null; Scheduler timer=null;
        try {
            var properties=new RaftProperties(); RaftConfigKeys.Rpc.setType(properties,SupportedRpcType.GRPC);
            RaftClientConfigKeys.Rpc.setRequestTimeout(properties,TimeDuration.valueOf(3,TimeUnit.SECONDS)); GrpcConfigKeys.Client.setWorkerGroupSize(properties,1);
            client=RaftClient.newBuilder().setRaftGroup(manifest.plan().catalogueGroup()).setProperties(properties).setRetryPolicy(RetryPolicies.noRetry()).build();
            for(var domain:manifest.plan().domains())routed.put(domain.directory().domainId(),RatisRoutedClient.connect(domain.directory()));
            scheduler=new RWScheduler(workload,"distributed-node",null,manifest.plan().databaseId().toString()); timer=Schedulers.newSingle("distributed-node-clock",true);
        } catch (RuntimeException | Error failure) {
            for(var connection:routed.values())try { connection.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (client!=null) client.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (timer!=null) timer.dispose(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (scheduler!=null) scheduler.dispose(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
        return new Resources(client,routed,scheduler,timer);
    }

    static RatisNodeService recover(RatisNodeManifest manifest,Path root,DatabaseConfig config,Resources resources,RatisNodeRuntime.ServerFactory factory) throws IOException {
        return recover(manifest,root,config,resources,factory,RatisNodeRuntime.SchemaReplicaConfig.defaults().policy());
    }
    static RatisNodeService recover(RatisNodeManifest manifest,Path root,DatabaseConfig config,Resources resources,RatisNodeRuntime.ServerFactory factory,RatisSchemaReplicaService.OperatorPolicy policy) throws IOException {
        return open(manifest,root,config,resources,factory,policy,RatisNodeRuntime.Startup.RECOVER,RatisRoutedClient::connect,false,null);
    }
    static RatisNodeService openRestore(RatisNodeManifest manifest,Path root,DatabaseConfig config,Resources resources,
            RatisNodeRuntime.ServerFactory factory,RatisSchemaReplicaService.OperatorPolicy policy,RatisNodeRuntime.HomeClientFactory homes,RatisNodeRuntime.RestoreNativeInitialization staging)throws IOException {
        return open(manifest,root,config,resources,factory,policy,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,homes,true,Objects.requireNonNull(staging));
    }
    private static RatisNodeService open(RatisNodeManifest manifest,Path root,DatabaseConfig config,Resources resources,
            RatisNodeRuntime.ServerFactory factory,RatisSchemaReplicaService.OperatorPolicy policy,RatisNodeRuntime.Startup startup,
            RatisNodeRuntime.HomeClientFactory homes,boolean restoring,RatisNodeRuntime.RestoreNativeInitialization staging)throws IOException {
        try {
            if(!restoring)preflight(root,manifest.plan());
            if(!resources.routed.keySet().equals(manifest.schemas().keySet()))throw new IOException("Recovery routed domains differ from manifest");
            for(var entry:resources.routed.entrySet())manifest.schema(entry.getKey()).requireMatches(entry.getValue().directory());
            var plan=manifest.plan(); var c=plan.catalogue();
            var epochs=new RatisSessionEpochs(resources.catalogue,plan.clusterId(),plan.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            var replicaConfig=new RatisNodeRuntime.SchemaReplicaConfig(manifest.schemaReplicaLimits(),Objects.requireNonNull(policy));
            var node=RatisNodeRuntime.open(plan,root,config,epochs,resources.routed,resources.scheduler,resources.timer,System::currentTimeMillis,
                    manifest.settings(),startup,factory,manifest.schemas().values(),null,null,homes,replicaConfig,
                    ()->new RatisSchemaReplicaClient(RatisSchemaReplicaClient.MAX_CALLS),staging);
            var service=new RatisNodeService(manifest,resources,node);service.restorePending=restoring;service.restoreOwned=restoring;return service;
        } catch (IOException | RuntimeException | Error failure) {
            resources.abort(failure); throw failure;
        }
    }
    void requestRestoreClose()throws IOException {
        synchronized(this){closeRequested=true;if(starting || frontendOpening || checks!=0)throw new IOException("Node service frontend work is active");}
        closeFrontend();
    }
    private void closeFrontend()throws IOException {
        if(frontend!=null && !frontendClosed){frontend.close();frontendClosed=true;}
    }
    synchronized void finishRestoreOwnership(){restoreOwned=false;}
    Resources restoreResources(){return resources;}
    private static void preflight(Path root,RatisNodePlan plan) throws IOException {
        if(Files.exists(root.resolve("restore.owner"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Restore-owned node requires original operation reopen/adoption");
        if (!Files.isRegularFile(root.resolve("node.identity"))) {
            throw new IOException("Recovery launcher requires established node identity/native state");
        }
        for(var spec:plan.pools().values())if(!Files.isRegularFile(spec.nativePath(root).resolve("CURRENT")))throw new IOException("Recovery requires established native pool: "+spec.id());
        for (var group:plan.localGroups()) if (!Files.isRegularFile(root.resolve("raft").resolve(group.getGroupId().getUuid().toString()).resolve("current/raft-meta"))) {
            throw new IOException("Recovery launcher requires established Raft state");
        }
    }
    public void start() throws IOException {
        synchronized (this) { requireOpen(); if (starting || closing || node.state()!=RatisNodeRuntime.State.OPEN) throw new IOException("Node service cannot start in its current state"); starting=true; }
        try { node.start(); }
        catch (IOException | RuntimeException | Error failure) { synchronized (this) { closeRequested=true; } throw failure; }
        finally { synchronized (this) { starting=false; } }
    }
    /** Own the public listener and backend; shared node transports outlive their actual drain. */
    public void startFrontend(Path backendRoot,URI listen,RatisRocksDBConnection.DomainPlacement placement,
            RatisSchemaClient.TrustedOperatorResolver resolver,RatisDatabaseBackendFactory.Policy policy) throws IOException {
        requireListenAddress(listen);
        startFrontend(() -> new RatisDatabaseBackendFactory(backendRoot,manifest.plan(),resources.catalogue,
                resources.routed,resources.scheduler,placement,resolver,policy),(connection,exports) -> {
            var builder=new ServerBuilder();builder.setClient(connection);builder.setExportAPI(exports);
            builder.setHttpAddress(new HostAndPort(listen.getHost(),listen.getPort()));
            builder.setUseThrift(listen.getScheme().equals("thrift"));return builder.build();
        },listen);
    }
    static void requireListenAddress(URI listen) {
        Objects.requireNonNull(listen);
        if (!("grpc".equals(listen.getScheme()) || "thrift".equals(listen.getScheme())) || listen.getHost()==null
                || listen.getPort()<1 || listen.getPort()>65535 || listen.getRawUserInfo()!=null
                || listen.getRawQuery()!=null || listen.getRawFragment()!=null
                || listen.getRawPath()!=null && !listen.getRawPath().isEmpty())
            throw new IllegalArgumentException("Frontend address requires grpc://host:port or thrift://host:port");
    }
    void startFrontend(BackendCreator backend,ListenerCreator listener,URI listen) throws IOException {
        synchronized(this) {
            requireOpen();if(restorePending || frontendOpening || frontendBackend!=null || node.state()!=RatisNodeRuntime.State.RUNNING)
                throw new IOException("Node frontend cannot start in its current state");frontendOpening=true;
        }
        try {
            verifyCatalogue();
            var owner=Objects.requireNonNull(backend.create());synchronized(this){frontendBackend=owner;}
            var connection=owner.open(listen);
            var server=Objects.requireNonNull(listener.create(connection,owner.exports()));synchronized(this){frontend=server;}
            server.start();
        } finally { synchronized(this){frontendOpening=false;} }
    }
    public synchronized RatisNodeRuntime.State state() { return closed ? RatisNodeRuntime.State.CLOSED : closeRequested ? RatisNodeRuntime.State.STOPPING : node.state(); }
    /** Fresh exact schema and directory observations. This does not prove permanent availability or all-replica placement. */
    public void verifyCatalogue() throws IOException {
        synchronized (this) { requireOpen(); if (node.state()!=RatisNodeRuntime.State.RUNNING) throw new IOException("Node service is not running");
            if (checks>=32) throw new IOException("Node service catalogue check budget exhausted"); checks++; }
        try {
            var complete=roster.readComplete();
            if(!complete.directories().keySet().equals(manifest.schemas().keySet()))throw new IOException("Catalogue roster differs from node manifest");
            for(var expected:manifest.schemas().values()) {
                var schema=schemas.read(expected.digest());var directory=complete.directories().get(expected.domainId());
                if(schema==null || !Arrays.equals(DistributedSchemaCodec.encode(expected),DistributedSchemaCodec.encode(schema))
                        || !Arrays.equals(expectedDirectories.get(expected.domainId()).payloadDigest(),directory.payloadDigest()))throw new IOException("Catalogue differs from node manifest");
            }
            roster.requireUnchanged(complete.roster());
        } finally { synchronized (this) { checks--; } }
    }
    /** Original placement identity stays immutable; only proven final serving routes become frontend expectations. */
    void adoptRestore(RatisDatabaseRestorePlanner.Plan original,SessionEpochLedger issued)throws IOException {
        synchronized(this){requireOpen();if(!restorePending || checks!=0 || node.state()!=RatisNodeRuntime.State.RUNNING)throw new IOException("Restore adoption unavailable");checks++;}
        try {
            var authority=original.requireExecutionLedger(issued);var view=roster.readComplete();
            if(!view.roster().equals(original.roster()))throw new IOException("Restored roster differs from frozen plan");
            var expected=new HashMap<UUID,RangeDirectory>();
            for(var domain:original.domains()){
                var schema=schemas.read(domain.schema().digest());var directory=view.directories().get(domain.schema().domainId());
                if(schema==null || !Arrays.equals(DistributedSchemaCodec.encode(schema),DistributedSchemaCodec.encode(domain.schema()))
                        || directory==null || !Arrays.equals(directory.payloadDigest(),domain.finalDirectory().payloadDigest()))throw new IOException("Restored schema/final directory differs");
                var route=resources.routed.get(domain.schema().domainId());
                if(!Arrays.equals(route.directory().payloadDigest(),directory.payloadDigest()))throw new IOException("Restored route not qualified");
                for(var range:directory.ranges())route.stateView(range.groupId()).requireServing(authority);
                expected.put(directory.domainId(),directory);
            }
            roster.requireUnchanged(original.roster());
            synchronized(this){expectedDirectories=Map.copyOf(expected);restorePending=false;}
        }finally{synchronized(this){checks--;}}
    }
    public RatisNodeReadiness.Report readiness() throws IOException {
        synchronized (this) { requireOpen();if(checks>=32)throw new IOException("Node service check budget exhausted");checks++; }
        try { return node.readiness(); }
        finally { synchronized (this) { checks--; } }
    }
    /** Trusted node-local policy; physical observations are replica-local and never logical database totals. */
    public synchronized it.cavallium.rockserver.core.common.ReplicaPhysicalAPI physicalApi(RatisNodeMaintenance.Policy policy)throws IOException {requireOpen();return node.physicalApi(policy);}
    public synchronized RatisNodeMaintenance localMaintenance(RatisNodeMaintenance.Policy policy) throws IOException {
        requireOpen();return node.localMaintenance(policy);
    }
    public RatisNodeMaintenance.Target localMaintenanceTarget(UUID poolId,UUID column) throws IOException {
        synchronized(this){requireOpen();}return node.localMaintenanceTarget(poolId,column);
    }
    private void requireOpen() throws IOException { if (closed || closeRequested || closing) throw new IOException("Node service is closing/closed"); }
    @Override public void close() throws IOException {
        synchronized (this) {
            if (closed) return;
            if (restoreOwned || starting || closing || frontendOpening || checks!=0) throw new IOException("Node service work is active"); closeRequested=true; closing=true;
        }
        try {
            closeFrontend();
            if(frontendBackend!=null && !exportsClosed){frontendBackend.drainExports();exportsClosed=true;}
            if(frontendBackend!=null && !backendClosed){frontendBackend.close();backendClosed=true;}
            node.close(); // On a drain/pin/close failure retain all dependencies and ownership for retry.
            resources.close(); synchronized (this) { closed=true; }
        } finally { synchronized (this) { closing=false; } }
    }
}
