package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.*;
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
        final RaftClient catalogue; final RatisRoutedClient routed; final RWScheduler scheduler; final Scheduler timer;
        private boolean catalogueClosed,routedClosed,schedulerClosed,timerClosed;
        Resources(RaftClient catalogue,RatisRoutedClient routed,RWScheduler scheduler,Scheduler timer) {
            this.catalogue=Objects.requireNonNull(catalogue); this.routed=Objects.requireNonNull(routed);
            this.scheduler=Objects.requireNonNull(scheduler); this.timer=Objects.requireNonNull(timer);
        }
        void abort(Throwable failure) {
            // No owner is returned after failed open: retire every independent allocation.
            try { if (!routedClosed) { routed.close(); routedClosed=true; } } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (!catalogueClosed) { catalogue.close(); catalogueClosed=true; } } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (!timerClosed) { timer.dispose(); timerClosed=true; } } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (!schedulerClosed) { scheduler.dispose(); schedulerClosed=true; } } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
        }
        @Override public void close() throws IOException {
            // Dependencies are retired only after the node drains. Preserve failed close handles for retry.
            if (!routedClosed) { routed.close(); routedClosed=true; }
            if (!catalogueClosed) { catalogue.close(); catalogueClosed=true; }
            if (!timerClosed) { timer.dispose(); timerClosed=true; }
            if (!schedulerClosed) { scheduler.dispose(); schedulerClosed=true; }
        }
    }
    private final RatisNodeManifest manifest; private final Resources resources; private final RatisNodeRuntime node;
    private final RatisSchemaCatalogue schemas; private final RatisCatalogue catalogue;
    private boolean starting,closing,closed,closeRequested; private int checks;
    private RatisNodeService(RatisNodeManifest manifest,Resources resources,RatisNodeRuntime node) {
        this.manifest=manifest; this.resources=resources; this.node=node; var plan=manifest.plan(); var c=plan.catalogue();
        schemas=new RatisSchemaCatalogue(resources.catalogue,plan.directory().clusterId(),plan.directory().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
        catalogue=new RatisCatalogue(resources.catalogue,plan.directory().clusterId(),plan.directory().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
    }
    /** Never grants placement-creation authority. Root must contain established native and Raft state. */
    public static RatisNodeService recover(RatisNodeManifest manifest,Path root,DatabaseConfig config) throws IOException {
        Objects.requireNonNull(manifest); Objects.requireNonNull(config); preflight(root,manifest.plan());
        final WorkloadSettings workload;
        try { workload=WorkloadSettings.resolve(config); } catch (Exception invalid) { throw new IOException("Invalid storage workload configuration",invalid); }
        if (workload.readParallelism()!=manifest.readThreads() || workload.writeParallelism()!=manifest.writeThreads()) {
            throw new IllegalArgumentException("Storage workload capacities differ from node manifest");
        }
        RaftClient client=null; RatisRoutedClient routed=null; RWScheduler scheduler=null; Scheduler timer=null;
        try {
            var properties=new RaftProperties(); RaftConfigKeys.Rpc.setType(properties,SupportedRpcType.GRPC);
            RaftClientConfigKeys.Rpc.setRequestTimeout(properties,TimeDuration.valueOf(3,TimeUnit.SECONDS)); GrpcConfigKeys.Client.setWorkerGroupSize(properties,1);
            client=RaftClient.newBuilder().setRaftGroup(manifest.plan().catalogueGroup()).setProperties(properties).setRetryPolicy(RetryPolicies.noRetry()).build();
            routed=manifest.settings().scopedTransactions() ? RatisRoutedClient.connectFramed(manifest.plan().directory()) : RatisRoutedClient.connect(manifest.plan().directory());
            scheduler=new RWScheduler(workload,"distributed-node",null,manifest.schema().databaseId().toString()); timer=Schedulers.newSingle("distributed-node-clock",true);
        } catch (RuntimeException | Error failure) {
            try { if (routed!=null) routed.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (client!=null) client.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (timer!=null) timer.dispose(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            try { if (scheduler!=null) scheduler.dispose(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
        return recover(manifest,root,config,new Resources(client,routed,scheduler,timer),null);
    }
    static RatisNodeService recover(RatisNodeManifest manifest,Path root,DatabaseConfig config,Resources resources,RatisNodeRuntime.ServerFactory factory) throws IOException {
        try {
            preflight(root,manifest.plan()); manifest.schema().requireMatches(resources.routed.directory());
            if (resources.routed.usesFramedQueries()!=manifest.settings().scopedTransactions()) throw new IOException("Node routed client query protocol differs from manifest");
            var plan=manifest.plan(); var c=plan.catalogue();
            var epochs=new RatisSessionEpochs(resources.catalogue,plan.directory().clusterId(),plan.directory().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            var node=factory==null
                    ? RatisNodeRuntime.open(plan,root,config,epochs,resources.routed,resources.scheduler,resources.timer,System::currentTimeMillis,manifest.settings(),RatisNodeRuntime.Startup.RECOVER,manifest.schema())
                    : RatisNodeRuntime.open(plan,root,config,epochs,resources.routed,resources.scheduler,resources.timer,System::currentTimeMillis,manifest.settings(),RatisNodeRuntime.Startup.RECOVER,factory,manifest.schema());
            return new RatisNodeService(manifest,resources,node);
        } catch (IOException | RuntimeException | Error failure) {
            resources.abort(failure); throw failure;
        }
    }
    private static void preflight(Path root,RatisNodePlan plan) throws IOException {
        if (!Files.isRegularFile(root.resolve("node.identity")) || !Files.isRegularFile(root.resolve("native/CURRENT"))) {
            throw new IOException("Recovery launcher requires established node identity/native state");
        }
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
    public synchronized RatisNodeRuntime.State state() { return closed ? RatisNodeRuntime.State.CLOSED : closeRequested ? RatisNodeRuntime.State.STOPPING : node.state(); }
    /** Fresh exact schema and directory observations. This does not prove permanent availability or all-replica placement. */
    public void verifyCatalogue() throws IOException {
        synchronized (this) { requireOpen(); if (node.state()!=RatisNodeRuntime.State.RUNNING) throw new IOException("Node service is not running");
            if (checks>=32) throw new IOException("Node service catalogue check budget exhausted"); checks++; }
        try {
            var schema=schemas.read(manifest.schema().digest());
            var directory=RatisRoutedClient.rpc(() -> catalogue.read(manifest.schema().domainId()));
            if (schema==null || directory==null || !Arrays.equals(DistributedSchemaCodec.encode(manifest.schema()),DistributedSchemaCodec.encode(schema))
                    || !Arrays.equals(manifest.plan().directory().payloadDigest(),directory.payloadDigest())) throw new IOException("Catalogue differs from node manifest");
        } finally { synchronized (this) { checks--; } }
    }
    private void requireOpen() throws IOException { if (closed || closeRequested || closing) throw new IOException("Node service is closing/closed"); }
    @Override public void close() throws IOException {
        synchronized (this) {
            if (closed) return;
            if (starting || closing || checks!=0) throw new IOException("Node service work is active"); closeRequested=true; closing=true;
        }
        try {
            node.close(); // On a drain/pin/close failure retain all dependencies and ownership for retry.
            resources.close(); synchronized (this) { closed=true; }
        } finally { synchronized (this) { closing=false; } }
    }
}
