package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Actual warm receiver division, forced graph and native75; no network or quorum claims. */
class RatisWarmSupersessionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @ParameterizedTest @CsvSource({"false,-1","true,-1","false,0","false,1"})
    void pendingAndCompletedCurrentFenceBeforeMetaAndRetryUnknownForce(boolean completed,int lostBoundary)throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false);
            var env=new RatisNodeRuntimeTest.Env(root.resolve("receiver"),false,source.first.digest())) {
            var base=source.firstDirectory;source.createColumn(base.domainId(),"before-recovery");var directory=source.routes.get(base.domainId()).directory();var range=directory.ranges().getFirst();var group=range.groupId();var id=RaftGroupId.valueOf(group);var local=source.local;var c=source.plan.catalogue();var poolId=RatisPoolFixtures.ID;
            var remoteRange=new RangeDescriptor(range.rangeId(),group,range.rangeEpoch(),range.configurationVersion(),null,null,range.topologyState(),List.of(c.replicas().getFirst()));
            var remoteDirectory=new RangeDirectory(base.clusterId(),base.databaseId(),base.domainId(),base.schemaEpoch(),base.schemaDigest(),base.generation(),base.columns(),base.routingPrefixBytes(),List.of(remoteRange));
            var domain=new RatisNodePlan.Domain(remoteDirectory,Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());
            var homeId=new UUID(12960,1);var homeColumn=new UUID(12960,2);var homeReplica=new ReplicaDescriptor(local.nodeId(),local.nodeIncarnation(),new UUID(12960,3),local.peerId(),local.address(),ReplicaDescriptor.Role.VOTER);
            var homeRange=new RangeDescriptor(homeId,homeId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(homeReplica));
            var homeDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),homeId,1,new byte[32],0,Set.of(homeColumn),0,List.of(homeRange));
            var home=new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,homeDirectory,homeColumn,"warm-home");
            var plan=new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),List.of(domain),c,List.of(home),List.of(),Map.of(),8,List.of(RatisPoolFixtures.spec(poolId,false)),Map.of(directory.domainId(),poolId),Map.of(c.groupId(),poolId,homeId,poolId));
            var epochs=new RatisSessionEpochs(source.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            var settings=new RatisNodeRuntime.Settings(source.manifest.settings().images(),env.settings().retention(),env.settings().clockPeriod(),1,8,Duration.ofSeconds(10),env.settings().exports(),env.settings().resources());
            var original=new RatisTopologySourceSnapshotRpc.Request(new UUID(12960,10),null,c.groupId(),local,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,group,directory);
            var prepared=new RatisNodeRecoverySourceTest().prepare(source,original);
            var artifacts=field(field(source.node,"pools",Map.class).get(source.fast),"artifacts",RatisTopologySourceSnapshotArtifacts.class);var image=artifacts.lookup(original).image();
            try(var route=new RatisRoutedClient(remoteDirectory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID column,byte[] key){throw new AssertionError("No USER read");}public CommandResult command(RangeDescriptor r,DistributedCommand command){throw new AssertionError("No USER command");}})) {
                var fake=new RatisNodeRuntimeTest.FakeServer();fake.skipFirstNoop=true;
                try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),route),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,List.of(source.first))) {
                    node.relocationTransport(r->RatisPendingCurrentMetadataRegistrationTest.client(source,r),(r,owner)->RatisPendingCurrentMetadataRegistrationTest.client(source,r));node.start();
                    var catalogue=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
                    var names=new HashMap<UUID,String>();for(var column:prepared.schema().columnIds())names.put(column,base.columns().contains(column)?"data":"recovery-"+column);var cfg=prepared.sourceCfg();
                    var registration=new NativeCurrentRangeRecovery(original.transferId(),original.catalogue(),catalogue.readView(directory.domainId()).cut(),directory,prepared.schema(),group,local,prepared.sourceOwner(),prepared.image().position(),prepared.image().sha256(),cfg.term(),cfg.index(),cfg.data(),prepared.image().topologyOrigin(),names,prepared.image().deterministicMergeColumns(),prepared.schemaRows());
                    var current=completed?node.registerRecovery(prepared):node.nativePool(poolId).openCurrentRangeRecovery(registration);
                    if(completed){current.installSnapshot(image,settings.images());current.completeCurrentRangeRecovery(current.currentRangeRecoveryRegistration(),current.applied(),current.configuration());var machine=fake.machines.get(id);machine.notifyTermIndexUpdated(current.applied().term(),current.applied().index());assertEquals(current.applied().index(),machine.takeSnapshot());((org.apache.ratis.server.storage.RaftStorageImpl)fake.storages.get(id)).writeBootstrapConfiguration(fake.division(id).getRaftConf());}
                    var discovered=RatisConfiguredRecoveryDiscovery.discover(plan,catalogue.recoveryDiscoverySource(),(retained,incoming)->{});field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).force(discovered,List.of(original));
                    source.split();node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);
                }
                NativeRecoverySupersession fence;
                for(int reopen=0;reopen<2;reopen++) {
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),route),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.RECOVER,RatisLocalMetadataDiscoveryTest::noSocketServer,List.of(source.first))) {
                        node.relocationTransport(r->RatisPendingCurrentMetadataRegistrationTest.client(source,r),(r,owner)->RatisPendingCurrentMetadataRegistrationTest.client(source,r));node.start();
                        var server=field(node,"server",RaftServer.class);var machine=(RatisRangeStateMachine)server.getDivision(id).getStateMachine();var store=field(machine,"store",ReplicatedRangeStore.class);var actual=store.applied();var cfg=store.configuration();
                        var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot();var meta=graph.activeRequests().get(group);assertTrue(meta.metadataOnly());
                        var scope=new NativeRecoverySupersession.Scope(c.groupId(),graph.plan().catalogueCut(),meta.header(),meta.sourceDirectory(),group,meta.receiver(),OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,meta)),List.of(OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,original))));
                        if(reopen==0 && lostBoundary>=0){node.nativePool(poolId).recoverySupersessionForceObserver(after->{if(after==(lostBoundary==1))throw new org.rocksdb.RocksDBException("Lost supersession force ACK");});assertThrows(IOException.class,()->node.supersedeRecovery(scope));assertNull(store.acknowledgedRecoverySupersession());assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());node.nativePool(poolId).recoverySupersessionForceObserver(after->{});}
                        fence=node.supersedeRecovery(scope);assertEquals(fence,store.acknowledgedRecoverySupersession());assertEquals(actual,store.applied());if(cfg==null)assertNull(store.configuration());else{assertEquals(cfg.term(),store.configuration().term());assertEquals(cfg.index(),store.configuration().index());assertArrayEquals(cfg.data(),store.configuration().data());}assertNull(store.metadataRecoveryRegistration());assertTrue(store.userHandlesDetached());assertFalse(store.admissionOpen());assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertThrows(IllegalStateException.class,machine.recoveryEligibilityGate()::snapshotPermit);
                        assertThrows(IOException.class,()->store.installSnapshot(image,settings.images()));
                        var request=RatisRecoverySupersessionRpc.Request.of(fence);var runtime=field(node,"groups",RatisRuntimeGroups.class).require(group);var reply=RatisRecoverySupersessionQueryTest.stale(server,runtime.group(),RatisRecoverySupersessionRpc.request(request),0);assertTrue(reply.isSuccess());RatisRecoverySupersessionRpc.reply(reply.getMessage().getContent().toByteArray(),request);
                        assertEquals(fence,node.supersedeRecovery(scope));assertNull(store.metadataRecoveryRegistration());assertEquals(actual,store.applied());
                    }
                }
            }
        }
    }
}
