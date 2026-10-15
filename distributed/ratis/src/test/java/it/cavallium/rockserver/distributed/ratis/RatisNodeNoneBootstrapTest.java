package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Actual native/core lifecycle with controlled CAT membership replies; no network or quorum claims. */
class RatisNodeNoneBootstrapTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void noneStaysFencedAcrossColdReopenAndZeroJobRediscoveryInvalidatesItForNewObligation()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root,false)) {
            var original=source.firstDirectory;var range=original.ranges().getFirst();var id=RaftGroupId.valueOf(range.groupId());var cut=source.store(range.groupId()).applied();
            for(var machine:source.server.machines.values())assertTrue(machine.takeSnapshot()>=0);
            for(var entry:source.server.storages.entrySet())((org.apache.ratis.server.storage.RaftStorageImpl)entry.getValue()).writeBootstrapConfiguration(source.server.division(entry.getKey()).getRaftConf());
            source.node.close();
            var remote=new ReplicaDescriptor(new UUID(12041,1),new UUID(12041,2),new UUID(12041,3),"replacement","127.0.0.1:9801",ReplicaDescriptor.Role.VOTER);
            var selected=new AtomicReference<>(placement(original,range,List.of(remote),2,1));var calls=new AtomicInteger();var catalogue=membershipCatalogue(source,selected,calls);
            var binding=source.plan.catalogue();var borrowed=source.service.restoreResources();var reference=new AtomicReference<RatisNodeRuntime>();var timer=immediateTimer(borrowed.timer,reference);
            var epochs=new RatisSessionEpochs(catalogue,source.first.clusterId(),source.first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());
            for(int round=0;round<2;round++)try(var node=RatisNodeRuntime.open(source.plan,source.root.resolve("node"),ConfigParser.parseDefault(),epochs,source.routes,borrowed.scheduler,timer,()->20L,source.manifest.settings(),RatisNodeRuntime.Startup.RECOVER,(plan,logs,machines)->{
                machines.get(id).deferRecoveryDiscovery();return RatisLocalMetadataDiscoveryTest.noSocketServer(plan,logs,machines);
            },List.of(source.first,source.second))) {
                reference.set(node);node.relocationTransport(descriptor->{assertEquals(binding.groupId(),descriptor.groupId());return catalogue;},(descriptor,owner)->{throw new AssertionError("NONE needs no DATA host");});node.start();
                var server=field(node,"server",RaftServer.class);var machine=(RatisRangeStateMachine)server.getDivision(id).getStateMachine();var store=field(machine,"store",ReplicatedRangeStore.class);
                var image=machine.getLatestSnapshot().getFiles().getFirst().getPath();byte[] bytes=Files.readAllBytes(image);var modified=Files.getLastModifiedTime(image);
                assertEquals(RatisNodeReadiness.Discovery.PENDING,status(node,id).discovery(),"A cold retained graph grants no NONE disposition before fresh discovery");
                assertEquals(0,field(node,"configuredRecoveries",RatisConfiguredRecoveries.class).size());assertTrue(field(node,"configuredRecoveryServiceStarted",Boolean.class));
                // CONTROL only dispatches onto the actual owned IO executor, even with no recovery slots.
                field(node,"configuredRecoveryService",RatisTransactionRecoveryService.class).tick().get(10,TimeUnit.SECONDS);
                awaitNone(node,id);
                var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot();assertNotNull(graph);assertFalse(graph.activeRequests().containsKey(range.groupId()));
                assertTrue(Files.isRegularFile(source.root.resolve("node/configured-recovery-graph/graph")));assertTrue(calls.get()>0);
                var none=status(node,id);assertFalse(none.servingRequired());assertFalse(none.servingReady());assertTrue(none.recoverySettled(),none.toString());assertNull(none.nativeCut());
                assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,machine.recoveryEligibilityGate().snapshot().mode());
                assertFalse(store.admissionOpen());assertTrue(machine.query(Message.EMPTY).isCompletedExceptionally());assertThrows(IllegalStateException.class,()->store.readCurrentPoint(source.first.columnIds().iterator().next(),new byte[]{7}));
                assertEquals(cut,store.applied());assertEquals(0,store.activeSnapshotPins());assertArrayEquals(bytes,Files.readAllBytes(image));assertEquals(modified,Files.getLastModifiedTime(image));
                var credit=field(node,"localNoneCredit",RatisNodeResourceBudget.Lease.class);long retained=credit.bytes();assertFalse(credit.released());
                int before=calls.get();field(node,"configuredRecoveryService",RatisTransactionRecoveryService.class).tick().get(10,TimeUnit.SECONDS);awaitFresh(node,graph);awaitNone(node,id);
                assertTrue(calls.get()>before);assertEquals(retained,credit.bytes());assertEquals(0,field(node,"configuredRecoveries",RatisConfiguredRecoveries.class).size());
                if(round==0) {
                    // Even an equal graph replacement between group sampling and node resampling
                    // must invalidate the earlier inert certificate.
                    var readStamp=RatisNodeRuntime.class.getDeclaredMethod("readinessStamp");readStamp.setAccessible(true);
                    var beforeStamp=(RatisNodeReadiness.NodeStamp)readStamp.invoke(node);var journal=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class);var previous=journal.snapshot();
                    field(node,"configuredRecoveries",RatisConfiguredRecoveries.class).discover(()->{journal.force(previous.plan(),new ArrayList<>(previous.activeRequests().values()));return null;}).get(15,TimeUnit.SECONDS);
                    assertNotSame(previous,journal.snapshot());assertEquals(previous,journal.snapshot());
                    var afterStamp=(RatisNodeReadiness.NodeStamp)readStamp.invoke(node);assertNotEquals(beforeStamp.placementStamp(),afterStamp.placementStamp());
                    assertEquals(RatisNodeReadiness.Discovery.PENDING,status(node,id).discovery());node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);awaitNone(node,id);
                }
                if(round==1) {
                    var ledger=epochs.read();assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(new OperationId(new UUID(12041,9),0,0),ledger,ledger.advanceTime(ledger.logicalTimeMillis()+1))).status());
                    selected.set(placement(original,range,range.replicas(),3,2));
                    node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);
                    assertTrue(field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot().activeRequests().containsKey(range.groupId()));
                    assertEquals(RatisNodeReadiness.Discovery.PENDING,status(node,id).discovery());assertFalse(status(node,id).recoverySettled());
                    assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());assertEquals(cut,store.applied());assertArrayEquals(bytes,Files.readAllBytes(image));assertFalse(credit.released());
                }
            }
        }
    }
    @Test void allInertDataWithoutHomeRegistrySettlesNodeReadinessWithoutNativeProgress()throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root)) {
            var original=env.routed.directory();var ranges=original.ranges().stream().map(range->new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion()+1,range.lowerInclusive(),range.upperExclusive(),range.topologyState(),range.replicas().stream().filter(owner->!owner.nodeId().equals(env.plan.nodeId())).toList())).toList();
            var directory=new RangeDirectory(original.clusterId(),original.databaseId(),original.domainId(),original.schemaEpoch(),original.schemaDigest(),original.generation()+1,original.columns(),original.routingPrefixBytes(),ranges);
            var cut=new ApplyPosition(1,10);var binding=env.plan.catalogue();
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                assertEquals("sendReadOnly",method.getName());byte[] query=((Message)args[0]).getContent().toByteArray();byte[] wire=switch(ScopedTransactionRpc.kind(query,ScopedTransactionRpc.MAX_REQUEST_BYTES)){
                    case RatisCatalogueViewRpc.KIND->RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(RatisCatalogueViewRpc.request(query),cut,directory,null));
                    case RatisRecoveryLineageRpc.KIND->RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(RatisRecoveryLineageRpc.request(query),cut,null));
                    default->throw new AssertionError("Unexpected inert CAT query");
                };
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("controlled-inert")).setGroupId(RaftGroupId.valueOf(binding.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(wire))).build();
            });
            var catalogue=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(binding.groupId());case "io"->io;case "close"->null;default->throw new AssertionError(method);});
            var inventory=new AtomicReference<Map<RaftGroupId,RatisRangeStateMachine>>();
            try(var node=RatisNodeRuntime.open(env.plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(original.domainId(),env.routed),env.scheduler,env.timer,()->20L,env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,(plan,logs,machines)->{machines.values().forEach(RatisRangeStateMachine::deferRecoveryDiscovery);inventory.set(machines);return RatisLocalMetadataDiscoveryTest.noSocketServer(plan,logs,machines);})) {
                node.relocationTransport(range->catalogue,(range,owner)->{throw new AssertionError("Inert node needs no DATA host");});node.start();assertNull(field(node,"participantRecoveries",RatisParticipantRecoveryRegistry.class));
                assertFalse(node.readiness().ready());node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);var report=node.readiness();assertTrue(report.ready(),report.toString());assertTrue(report.recoveryScanned());
                for(var group:report.groups()){assertEquals(RatisNodeReadiness.Discovery.NONE_AT_CUT,group.discovery());assertNull(group.nativeCut());assertFalse(group.servingRequired());var machine=inventory.get().get(RaftGroupId.valueOf(group.group()));assertNull(field(machine,"store",ReplicatedRangeStore.class).applied());assertTrue(machine.recoveryEligibilityGate().discoveryPending());}
            }
        }
    }
    private static reactor.core.scheduler.Scheduler immediateTimer(reactor.core.scheduler.Scheduler delegate,AtomicReference<RatisNodeRuntime> reference) {
        return new reactor.core.scheduler.Scheduler(){
            public reactor.core.Disposable schedulePeriodically(Runnable task,long initial,long period,TimeUnit unit){
                var scheduled=delegate.schedulePeriodically(task,initial,period,unit);var node=reference.get();
                if(node!=null)try{if(field(node,"configuredRecoveryServiceStarted",Boolean.class)){task.run();field(node,"configuredRecoveryService",RatisTransactionRecoveryService.class).tick().get(10,TimeUnit.SECONDS);}}catch(Exception failure){throw new java.util.concurrent.CompletionException(failure);}
                return scheduled;
            }
            public reactor.core.Disposable schedule(Runnable task){return delegate.schedule(task);}public Worker createWorker(){return delegate.createWorker();}public void dispose(){delegate.dispose();}public boolean isDisposed(){return delegate.isDisposed();}
        };
    }
    private static RatisNodeReadiness.GroupStatus status(RatisNodeRuntime node,RaftGroupId id){return node.readiness().groups().stream().filter(group->group.group().equals(id.getUuid())).findFirst().orElseThrow();}
    private static void awaitNone(RatisNodeRuntime node,RaftGroupId id)throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
        while(System.nanoTime()<deadline){if(!field(node,"configuredRecoveries",RatisConfiguredRecoveries.class).running() && status(node,id).discovery()==RatisNodeReadiness.Discovery.NONE_AT_CUT)return;Thread.sleep(5);}
        fail("Owned NONE discovery did not settle: "+field(node,"configuredRecoveries",RatisConfiguredRecoveries.class).failure());
    }
    private static void awaitFresh(RatisNodeRuntime node,RatisConfiguredRecoveryGraph.Snapshot prior)throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);
        while(System.nanoTime()<deadline){if(field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot()!=prior && !field(node,"configuredRecoveries",RatisConfiguredRecoveries.class).running())return;Thread.sleep(5);}
        fail("Zero-job rediscovery did not refresh its forced graph");
    }
    private static RangeDirectory placement(RangeDirectory d,RangeDescriptor range,List<ReplicaDescriptor> owners,long generation,long configuration) {
        var target=new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),configuration,range.lowerInclusive(),range.upperExclusive(),range.topologyState(),owners);
        return new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),d.schemaEpoch(),d.schemaDigest(),generation,d.columns(),d.routingPrefixBytes(),List.of(target));
    }
    private static RaftClient membershipCatalogue(RatisTwoPoolWorkflowFixture source,AtomicReference<RangeDirectory> selected,AtomicInteger calls) {
        var binding=source.plan.catalogue();
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            if(!method.getName().equals("sendReadOnly"))return method.invoke(source.catalogue.io(),args);
            byte[] query=((Message)args[0]).getContent().toByteArray();var reply=source.catalogue.io().sendReadOnly((Message)args[0]);
            if(ScopedTransactionRpc.kind(query,ScopedTransactionRpc.MAX_REQUEST_BYTES)!=RatisCatalogueViewRpc.KIND)return reply;
            var request=RatisCatalogueViewRpc.request(query);if(!request.domain().equals(source.first.domainId()))return reply;calls.incrementAndGet();
            var current=RatisCatalogueViewRpc.reply(reply.getMessage().getContent().toByteArray(),request);byte[] wire=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(request,current.cut(),selected.get(),current.roster()));
            return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("controlled-membership")).setGroupId(RaftGroupId.valueOf(binding.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(wire))).build();
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){
            case "getGroupId"->RaftGroupId.valueOf(binding.groupId());case "io"->io;case "close"->null;default->throw new AssertionError(method);
        });
    }
}
