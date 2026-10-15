package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Native inventory, actual Node construction and real socket-free Raft dispatch across cold startup. */
class RatisNodeSupersessionBootstrapTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static NativeCurrentRangeRecovery registration(RatisTopologySourceSnapshotRpc.Prepared prepared,ApplyPosition cut){
        var request=prepared.request();var cfg=prepared.sourceCfg();return new NativeCurrentRangeRecovery(request.transferId(),request.catalogue(),cut,request.sourceDirectory(),prepared.schema(),request.sourceGroup(),request.receiver(),prepared.sourceOwner(),prepared.image().position(),prepared.image().sha256(),cfg.term(),cfg.index(),cfg.data(),prepared.image().topologyOrigin(),Map.of(prepared.schema().columnIds().iterator().next(),"data"),prepared.image().deterministicMergeColumns(),prepared.schemaRows());
    }
    @Test void warmFinalImageForceLossUsesOnlyTheFencedDurableReference()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("warm-source"),false);var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("warm-native"))) {
            var directory=source.firstDirectory;var range=directory.ranges().getFirst();var id=RaftGroupId.valueOf(range.groupId());var c=source.plan.catalogue();
            var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(12921,1),null,c.groupId(),source.local,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,range.groupId(),directory);
            var prepared=RatisTopologySourceSnapshotRpc.reply(source.server.machines.get(id).queryLinearizable(RatisRecoverySupersessionQueryTest.message(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request))).get(15,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologySourceSnapshotRpc.PREPARE,request);
            var catalogue=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
            var registered=registration(prepared,catalogue.readView(directory.domainId()).cut());var store=pool.openCurrentRangeRecovery(registered);
            var owner=field(source.node,"pools",Map.class).get(source.fast);var image=field(owner,"artifacts",RatisTopologySourceSnapshotArtifacts.class).lookup(request).image();
            assertThrows(IOException.class,()->store.installSnapshot(image,source.manifest.settings().images(),new ReplicatedRangeStore.InstallationObserver(){public void afterBatch(int batch){}public void afterCompletionForce()throws IOException{throw new IOException("Lost final image force ACK");}}));
            assertTrue(store.pendingInstallation());assertNull(store.pendingInstallationCut());assertNull(store.applied());assertNotNull(store.completedCurrentInstallationReference());
            var helper=new RatisDiscoveryInventoryTest();helper.root=root;
            assertThrows(IOException.class,()->helper.new Adapter(store,pool,directory,source.local,root.resolve("unfenced-warm-inventory"),image,prepared.image().position(),(machine,floor)->{}));
            var consuming=source.split();var digest=OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request));
            var scope=new NativeRecoverySupersession.Scope(c.groupId(),catalogue.readView(directory.domainId()).cut(),consuming.header(),directory,range.groupId(),source.local,new byte[32],List.of(digest));
            pool.forceRecoverySupersession(scope);assertNull(store.recoverySupersession().captured().pendingIntent());assertEquals(prepared.image().position(),store.recoverySupersession().captured().marker());
            try(var adapter=helper.new Adapter(store,pool,directory,source.local,root.resolve("fenced-warm-inventory"),image,prepared.image().position(),(machine,floor)->{})) {
                assertEquals(prepared.image().position().index(),adapter.machine.getLatestSnapshot().getIndex());assertEquals(-1,adapter.machine.getLastAppliedTermIndex().getIndex());assertNull(store.applied());assertTrue(store.pendingInstallation());assertNull(store.pendingInstallationCut());assertTrue(adapter.machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.currentRangeRecoveryReady());assertFalse(store.admissionOpen());assertNull(store.metadataRecoveryRegistration());
                assertThrows(IOException.class,()->store.installSnapshot(image,source.manifest.settings().images()));
            }
        }
    }

    enum Receiver { ABSENT, REGISTERED, CURRENT }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.EnumSource(Receiver.class)
    void receiverFenceMaterializesWithoutSyntheticImageConfigurationOrReadiness(Receiver receiver)throws Exception {
        boolean retainedCurrent=receiver!=Receiver.ABSENT,publishedCurrent=receiver==Receiver.CURRENT;
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false);
            var env=new RatisNodeRuntimeTest.Env(root.resolve("receiver"),false,source.first.digest())) {
            var directory=source.firstDirectory;var parent=directory.ranges().getFirst();var group=parent.groupId();var id=RaftGroupId.valueOf(group);var local=source.local;var c=source.plan.catalogue();var poolId=RatisPoolFixtures.ID;
            // The receiver initially exposes only a local HOME, with no DATA ownership/division.
            var remoteParent=new RangeDescriptor(parent.rangeId(),group,parent.rangeEpoch(),parent.configurationVersion(),null,null,parent.topologyState(),List.of(c.replicas().getFirst()));
            var remoteDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),directory.generation(),directory.columns(),directory.routingPrefixBytes(),List.of(remoteParent));
            var domain=new RatisNodePlan.Domain(remoteDirectory,Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());
            var homeId=new UUID(12920,1);var homeColumn=new UUID(12920,2);var homeReplica=new ReplicaDescriptor(local.nodeId(),local.nodeIncarnation(),new UUID(12920,3),local.peerId(),local.address(),ReplicaDescriptor.Role.VOTER);
            var homeRange=new RangeDescriptor(homeId,homeId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(homeReplica));
            var homeDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),homeId,1,new byte[32],0,Set.of(homeColumn),0,List.of(homeRange));
            var home=new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,homeDirectory,homeColumn,"supersession-home");
            var plan=new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),List.of(domain),c,List.of(home),List.of(),Map.of(),4,List.of(RatisPoolFixtures.spec(poolId,false)),Map.of(directory.domainId(),poolId),Map.of(c.groupId(),poolId,homeId,poolId));
            var epochs=new RatisSessionEpochs(source.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            var settings=new RatisNodeRuntime.Settings(source.manifest.settings().images(),env.settings().retention(),env.settings().clockPeriod(),1,4,java.time.Duration.ofSeconds(10),env.settings().exports(),env.settings().resources());
            try(var route=new RatisRoutedClient(remoteDirectory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID column,byte[] key){throw new AssertionError("No USER access");}public CommandResult command(RangeDescriptor r,DistributedCommand command){throw new AssertionError("No USER command");}})) {
                var original=new RatisTopologySourceSnapshotRpc.Request(new UUID(12920,10),null,c.groupId(),local,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,group,directory);
                RatisRecoverySupersessionRpc.Request expected;NativeCurrentRangeRecovery retained=null;ApplyPosition actual=null;long floor=-1;
                var initial=new RatisNodeRuntimeTest.FakeServer();initial.skipFirstNoop=true;
                try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),route),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,initial::create,List.of(source.first))) {
                    node.relocationTransport(range->RatisPendingCurrentMetadataRegistrationTest.client(source,range),(range,owner)->RatisPendingCurrentMetadataRegistrationTest.client(source,range));node.start();assertFalse(initial.machines.containsKey(id));
                    if(retainedCurrent) {
                        var prepared=RatisTopologySourceSnapshotRpc.reply(source.server.machines.get(id).queryLinearizable(RatisRecoverySupersessionQueryTest.message(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,original))).get(15,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologySourceSnapshotRpc.PREPARE,original);
                        var owner=field(source.node,"pools",Map.class).get(source.fast);var artifacts=field(owner,"artifacts",RatisTopologySourceSnapshotArtifacts.class);var image=artifacts.lookup(original).image();
                        var current=receiver==Receiver.REGISTERED?node.nativePool(poolId).openCurrentRangeRecovery(registration(prepared,new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn()).readView(directory.domainId()).cut())):node.registerRecovery(prepared);retained=current.currentRangeRecoveryRegistration();
                        if(publishedCurrent){current.installSnapshot(image,settings.images());current.completeCurrentRangeRecovery(retained,current.applied(),current.configuration());
                        var machine=initial.machines.get(id);machine.notifyTermIndexUpdated(current.applied().term(),current.applied().index());
                        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(machine.getLastAppliedTermIndex().getIndex()<current.applied().index() && System.nanoTime()<deadline)Thread.sleep(2);
                        assertEquals(current.applied().index(),machine.getLastAppliedTermIndex().getIndex());floor=machine.takeSnapshot();assertEquals(current.applied().index(),floor);actual=current.applied();
                        ((org.apache.ratis.server.storage.RaftStorageImpl)initial.storages.get(id)).writeBootstrapConfiguration(initial.division(id).getRaftConf());}
                    }
                    var consuming=source.split();var cut=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn()).readView(directory.domainId()).cut();
                    var selected=new RatisTopologySourceSnapshotRpc.Request(new UUID(12920,11),consuming.header(),c.groupId(),local,RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,group,directory);
                    var oldDigest=OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,original));
                    var scope=new NativeRecoverySupersession.Scope(c.groupId(),cut,consuming.header(),directory,group,local,OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,selected)),List.of(oldDigest));
                    var store=node.nativePool(poolId).forceRecoverySupersession(scope);assertEquals(actual,store.applied());if(!publishedCurrent)assertNull(store.configuration());expected=RatisRecoverySupersessionRpc.Request.of(store.acknowledgedRecoverySupersession());
                }
                assertEquals(publishedCurrent,Files.exists(env.nodeRoot.resolve("raft").resolve(group.toString()).resolve("current/raft-meta")));
                for(int reopen=0;reopen<2;reopen++) {
                    var machines=new HashMap<RaftGroupId,RatisRangeStateMachine>();
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),route),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.RECOVER,(p,logs,created)->{machines.putAll(created);assertTrue(created.get(id).recoveryEligibilityGate().discoveryPending());return RatisLocalMetadataDiscoveryTest.noSocketServer(p,logs,created);},List.of(source.first))) {
                        node.relocationTransport(range->RatisPendingCurrentMetadataRegistrationTest.client(source,range),(range,owner)->RatisPendingCurrentMetadataRegistrationTest.client(source,range));node.start();
                        var machine=machines.get(id);assertNotNull(machine);var store=field(machine,"store",ReplicatedRangeStore.class);var server=field(node,"server",RaftServer.class);
                        assertEquals(expected,RatisRecoverySupersessionRpc.Request.of(store.acknowledgedRecoverySupersession()));assertEquals(actual,store.applied());if(!publishedCurrent)assertNull(store.configuration());assertTrue(store.userHandlesDetached());assertFalse(store.admissionOpen());assertNull(store.metadataRecoveryRegistration());assertFalse(store.metadataRecoveryImageInstalled());
                        assertTrue(machine.recoveryEligibilityGate().discoveryPending());if(!publishedCurrent)assertNull(machine.getLatestSnapshot());else assertEquals(floor,machine.getLatestSnapshot().getIndex());assertEquals(actual==null?-1:actual.index(),machine.getLastAppliedTermIndex().getIndex());assertEquals(floor,server.getDivision(id).getRaftLog().getSnapshotIndex());
                        var runtime=field(node,"groups",RatisRuntimeGroups.class).require(group);assertTrue(runtime.retired());assertNotNull(runtime.supersession());assertNull(runtime.metadata());assertEquals(retained,runtime.current());
                        for(int retry=0;retry<2;retry++){var reply=RatisRecoverySupersessionQueryTest.stale(server,runtime.group(),RatisRecoverySupersessionRpc.request(expected),0);assertTrue(reply.isSuccess());RatisRecoverySupersessionRpc.reply(reply.getMessage().getContent().toByteArray(),expected);}
                        assertThrows(IllegalStateException.class,machine.recoveryEligibilityGate()::snapshotPermit);assertEquals(-1,machine.takeSnapshot());
                        assertThrows(java.util.concurrent.ExecutionException.class,()->machine.queryStale(RatisRecoverySupersessionQueryTest.message(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0])),0).get(5,TimeUnit.SECONDS));
                        var status=node.readiness().groups().stream().filter(value->value.group().equals(group)).findFirst().orElseThrow();assertFalse(status.servingReady());
                        var owner=field(node,"pools",Map.class).get(poolId);assertEquals(224L,field(owner,"recoveryExposureBytes",Long.class));assertEquals(runtime.supersession().encodedSize()+(retained==null?0:retained.encodedSize()),field(owner,"recoveryEncodedBytes",Long.class));
                        assertTrue(Files.isRegularFile(env.nodeRoot.resolve("topology-exposure/registered").resolve(group.toString())));
                    }
                }
                var raftMeta=env.nodeRoot.resolve("raft").resolve(group.toString()).resolve("current/raft-meta");var hidden=raftMeta.resolveSibling("held-raft-meta");Files.move(raftMeta,hidden);
                try{assertThrows(java.io.IOException.class,()->RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),route),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.RECOVER,(p,logs,created)->{throw new AssertionError("Missing exposed storage must refuse before server construction");},List.of(source.first)));}finally{Files.move(hidden,raftMeta);}
            }
        }
    }
}
