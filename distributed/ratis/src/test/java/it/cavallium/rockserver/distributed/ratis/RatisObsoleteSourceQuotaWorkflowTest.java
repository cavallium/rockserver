package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Genuine JNI source capture and FREEZE/CAT, with controlled source transport and actual receiver Raft dispatch. */
class RatisObsoleteSourceQuotaWorkflowTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void lostCurrentPrepareAtOneSlotRetiresWithoutInstallBeforeRealMetaPrepare()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false)) {
            var directory=source.firstDirectory;var range=directory.ranges().getFirst();var id=RaftGroupId.valueOf(range.groupId());var machine=source.server.machines.get(id);var store=source.store(range.groupId());
            var owner=field(source.node,"pools",Map.class).get(source.fast);var artifacts=field(owner,"artifacts",RatisTopologySourceSnapshotArtifacts.class);var oldService=field(owner,"sources",RatisTopologySourceSnapshotService.class);
            oldService.stop().get(5,TimeUnit.SECONDS);var factory=RatisNodeRuntime.class.getDeclaredMethod("sourceSnapshotHost",owner.getClass());factory.setAccessible(true);var host=(RatisTopologySourceSnapshotService.Host)factory.invoke(source.node,owner);
            var limits=source.manifest.settings().images();var admission=field(source.node,"resources",RatisNodeResourceBudget.class);
            var sourcesField=owner.getClass().getDeclaredField("sources");sourcesField.setAccessible(true);var machineField=RatisRangeStateMachine.class.getDeclaredField("sourceSnapshots");machineField.setAccessible(true);
            try(var service=new RatisTopologySourceSnapshotService(host,new RatisTopologySourceSnapshotService.Limits(1,limits.maxImageBytes(),limits.maxImageBytes(),RatisTopologySourceSnapshotService.MAX_METADATA_BYTES),field(source.node,"scheduler",it.cavallium.rockserver.core.impl.RWScheduler.class),(request,bytes,control)->admission.reserve(source.fast,RatisNodeResourceBudget.Kind.PROTECTED,bytes,1)::close)) {
                sourcesField.set(owner,service);machineField.set(machine,service);
                var original=new RatisTopologySourceSnapshotRpc.Request(new UUID(13100,1),null,source.plan.catalogue().groupId(),source.local,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,range.groupId(),directory);var unknown=original.successor(new UUID(13100,2));
                // The source completes PREPARE, but its caller never receives the result or owner identity.
                service.prepareAsync(original).get(15,TimeUnit.SECONDS);assertEquals(1,service.stats().leases());assertTrue(store.activeSnapshotPins()>0);
                var consuming=source.split();var meta=new RatisTopologySourceSnapshotRpc.Request(new UUID(13100,3),consuming.header(),original.catalogue(),source.local,RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,range.groupId(),directory);
                assertThrows(java.util.concurrent.ExecutionException.class,()->service.prepareAsync(meta).get(5,TimeUnit.SECONDS));
                var cut=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),original.catalogue(),source.plan.catalogue().epoch(),source.plan.catalogue().schemaDigest(),source.plan.catalogue().directoryColumn()).readView(directory.domainId()).cut();
                var scope=new NativeRecoverySupersession.Scope(original.catalogue(),cut,consuming.header(),directory,range.groupId(),source.local,hash(meta),hash(original),hash(unknown));
                try(var receiver=new Receiver(source,scope)) {
                    var transportField=RatisNodeRuntime.class.getDeclaredField("relocationClients");transportField.setAccessible(true);var oldTransport=(RatisRelocationDataClient)transportField.get(source.node);
                    try(var transport=new RatisRelocationDataClient(directory.clusterId(),directory.databaseId(),original.catalogue(),r->{throw new AssertionError("Fence proof is owner directed");},(r,peer)->receiver.client())) {
                        transportField.set(source.node,transport);
                        try {
                            var before=service.stats();var wrong=receiver.fence.digest();wrong[0]^=1;
                            var rejected=new RatisRecoveryObsoleteRpc.Request(original,scope,wrong);
                            assertThrows(java.util.concurrent.ExecutionException.class,()->machine.queryStale(RatisRecoverySupersessionQueryTest.message(RatisRecoveryObsoleteRpc.request(rejected)),0).get(15,TimeUnit.SECONDS));
                            assertEquals(before,service.stats());assertTrue(store.activeSnapshotPins()>0);assertFalse(Files.exists(field(artifacts,"root",Path.class).resolve(original.transferId()+".obsolete")));
                            for(var request:List.of(original,unknown)) {
                                var obsolete=new RatisRecoveryObsoleteRpc.Request(request,scope,receiver.fence.digest());
                                var reply=machine.queryStale(RatisRecoverySupersessionQueryTest.message(RatisRecoveryObsoleteRpc.request(obsolete)),0).get(15,TimeUnit.SECONDS);RatisRecoveryObsoleteRpc.reply(reply.getContent().toByteArray(),obsolete);
                                assertTrue(artifacts.isObsolete(obsolete));assertFalse(Files.exists(field(artifacts,"root",Path.class).resolve(request.transferId()+".released")));assertThrows(java.io.IOException.class,()->artifacts.isReleased(request));assertThrows(java.util.concurrent.ExecutionException.class,()->service.prepareAsync(request).get(5,TimeUnit.SECONDS));
                            }
                            assertEquals(0,service.stats().leases());assertEquals(0,service.stats().reservedImageBytes());assertEquals(0,store.activeSnapshotPins());
                            service.stop().get(5,TimeUnit.SECONDS);receiver.available=false;
                            try(var restarted=new RatisTopologySourceSnapshotService(host,new RatisTopologySourceSnapshotService.Limits(1,limits.maxImageBytes(),limits.maxImageBytes(),RatisTopologySourceSnapshotService.MAX_METADATA_BYTES),field(source.node,"scheduler",it.cavallium.rockserver.core.impl.RWScheduler.class),(request,bytes,control)->admission.reserve(source.fast,RatisNodeResourceBudget.Kind.PROTECTED,bytes,1)::close)) {
                                sourcesField.set(owner,restarted);machineField.set(machine,restarted);
                                var replay=new RatisRecoveryObsoleteRpc.Request(original,scope,receiver.fence.digest());var reply=machine.queryStale(RatisRecoverySupersessionQueryTest.message(RatisRecoveryObsoleteRpc.request(replay)),0).get(15,TimeUnit.SECONDS);RatisRecoveryObsoleteRpc.reply(reply.getContent().toByteArray(),replay);
                                var prepared=restarted.prepareAsync(meta).get(15,TimeUnit.SECONDS);assertNotNull(prepared.metadataFreeze());assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(artifacts.lookup(meta).image(),NativeRangeImage.IoAdmission.UNLIMITED));
                                assertNull(receiver.store.applied());assertNull(receiver.store.configuration());assertFalse(receiver.store.metadataRecoveryImageInstalled());
                            }
                        }finally{transportField.set(source.node,oldTransport);}
                    }
                }
            }finally{sourcesField.set(owner,oldService);machineField.set(machine,oldService);}
        }
    }
    static byte[] hash(RatisTopologySourceSnapshotRpc.Request request){return OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request));}
    final class Receiver implements AutoCloseable {
        volatile boolean available=true;
        final DistributedStoragePool pool;final ReplicatedRangeStore store;final NativeRecoverySupersession fence;final RatisRangeStateMachine machine;final RaftServer server;final RaftGroup group;final RatisCommittedApplyDispatcher dispatcher;
        Receiver(RatisTwoPoolWorkflowFixture source,NativeRecoverySupersession.Scope scope)throws Exception {
            pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("receiver"));store=pool.forceRecoverySupersession(scope);fence=store.acknowledgedRecoverySupersession();
            var domain=new RatisNodePlan.Domain(scope.sourceDirectory(),Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());var plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),source.plan.catalogue(),List.of(),List.of(),Map.of(),4,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(source.plan.catalogue(),List.of()));group=plan.localGroups().getFirst();
            machine=new RatisRangeStateMachine(store,group,source.manifest.settings().images());machine.topologyAuthority((header,stage)->{throw new AssertionError("No receiver source IO");},scope.sourceDirectory(),scope.receiver(),pool);machine.deferRecoveryDiscovery();var defaults=RatisNodeRuntime.ResourceLimits.defaults();machine.nodeResources(new RatisNodeResourceBudget(defaults.node(),Map.of(RatisPoolFixtures.ID,defaults.pool())),RatisPoolFixtures.ID);
            var logs=RatisLogResourceBudget.Limits.defaults();machine.logResources(new RatisLogResourceBudget(scope.groupId(),new RatisLogResourceBudget.Shared(logs),new RatisLogResourceBudget.Shared(logs),logs,(bytes,progress)->()->{}));var policy=RatisCommittedApplyDispatcher.Policy.defaults();dispatcher=new RatisCommittedApplyDispatcher(policy.node(),Map.of(RatisPoolFixtures.ID,policy.pool()));machine.committedApply(dispatcher,RatisPoolFixtures.ID,NativeSstImportManifest.MAX_FILE_BYTES+4096);
            server=RatisLocalMetadataDiscoveryTest.noSocketServer(plan,root.resolve("receiver-raft"),Map.of(group.getGroupId(),machine));server.start();
        }
        RaftClient client(){var io=(BlockingApi)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{if(!method.getName().equals("sendStaleRead"))throw new AssertionError(method);if(!available)throw new java.io.IOException("Receiver unavailable after durable terminal force");assertEquals(0L,args[1]);assertEquals(server.getId(),args[2]);return RatisRecoverySupersessionQueryTest.stale(server,group,((Message)args[0]).getContent().toByteArray(),0);});return (RaftClient)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->group.getGroupId();case "io"->io;case "close"->null;default->throw new AssertionError(method);});}
        public void close()throws Exception{machine.beginShutdown();server.close();machine.awaitNativeReaders(Duration.ofSeconds(5));dispatcher.awaitDrained(Duration.ofSeconds(5));dispatcher.close();pool.close();}
    }
}
