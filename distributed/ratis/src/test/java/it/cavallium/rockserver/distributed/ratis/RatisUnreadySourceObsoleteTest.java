package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** An actual unready core source can retire cold artifacts using fresh CAT and a distinct receiver's 75. */
class RatisUnreadySourceObsoleteTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void missingUncommittedSourceCleansColdCaptureAndAcknowledgesAbsentWithoutLocalFreezeOrFence()throws Exception {
        var target=new ReplicaDescriptor(new UUID(13310,7),new UUID(13310,8),new UUID(13310,9),"receiver7","127.0.0.1:4597",ReplicaDescriptor.Role.LISTENER);
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false,List.of(target));
            var env=new RatisNodeRuntimeTest.Env(root.resolve("restored-source"),false,source.first.digest())) {
            var directory=source.firstDirectory;var range=directory.ranges().getFirst();var group=RaftGroupId.valueOf(range.groupId());var c=source.plan.catalogue();
            assertEquals(1,range.replicas().stream().filter(peer->peer.role()==ReplicaDescriptor.Role.VOTER).count());assertEquals(source.local,range.replicas().stream().filter(peer->peer.role()==ReplicaDescriptor.Role.VOTER).findFirst().orElseThrow());
            var old=new RatisTopologySourceSnapshotRpc.Request(new UUID(13310,10),null,c.groupId(),target,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,range.groupId(),directory);var absent=old.successor(new UUID(13310,11));
            var sourceMachine=source.server.machines.get(group);var prepared=RatisTopologySourceSnapshotRpc.reply(sourceMachine.queryLinearizable(RatisRecoverySupersessionQueryTest.message(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,old))).get(15,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologySourceSnapshotRpc.PREPARE,old);
            var sourceOwner=field(source.node,"pools",Map.class).get(source.fast);var sourceArtifacts=field(sourceOwner,"artifacts",RatisTopologySourceSnapshotArtifacts.class);assertNotNull(sourceArtifacts.lookup(old));
            var catalogue=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());var originalCut=catalogue.readView(directory.domainId()).cut();
            var consuming=source.publishSplit();var selected=new RatisTopologySourceSnapshotRpc.Request(new UUID(13310,12),consuming.header(),c.groupId(),target,RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,range.groupId(),directory);
            var scope=new NativeRecoverySupersession.Scope(c.groupId(),catalogue.readView(directory.domainId()).cut(),consuming.header(),directory,range.groupId(),target,RatisObsoleteSourceQuotaWorkflowTest.hash(selected),List.of(RatisObsoleteSourceQuotaWorkflowTest.hash(old),RatisObsoleteSourceQuotaWorkflowTest.hash(absent)));
            var helper=new RatisObsoleteSourceQuotaWorkflowTest();helper.root=root;
            try(var receiver=helper.new Receiver(source,scope)) {
                var remoteRange=new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),null,null,range.topologyState(),List.of(c.replicas().getFirst()));
                var bootstrap=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),directory.generation(),directory.columns(),directory.routingPrefixBytes(),List.of(remoteRange));
                var domain=new RatisNodePlan.Domain(bootstrap,Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());var pool=RatisPoolFixtures.ID;
                UUID homeGroup=new UUID(13310,13),homeColumn=new UUID(13310,14);var localHome=new ReplicaDescriptor(source.local.nodeId(),source.local.nodeIncarnation(),new UUID(13310,15),source.local.peerId(),source.local.address(),ReplicaDescriptor.Role.VOTER);
                var homeRange=new RangeDescriptor(homeGroup,homeGroup,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(localHome));
                var homeDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),homeGroup,1,new byte[32],0,Set.of(homeColumn),0,List.of(homeRange));
                var home=new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,homeDirectory,homeColumn,"unready-source-home");
                var plan=new RatisNodePlan(source.local.nodeId(),source.local.nodeIncarnation(),List.of(domain),c,List.of(home),List.of(),Map.of(),4,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(directory.domainId(),pool),Map.of(c.groupId(),pool,homeGroup,pool));
                var epochs=new RatisSessionEpochs(source.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
                var settings=new RatisNodeRuntime.Settings(source.manifest.settings().images(),env.settings().retention(),env.settings().clockPeriod(),1,4,Duration.ofSeconds(10),env.settings().exports(),env.settings().resources());
                try(var route=new RatisRoutedClient(bootstrap,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("No USER read");}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("No USER command");}})) {
                    var cfg=prepared.sourceCfg();var self=new NativeCurrentRangeRecovery(new UUID(13310,16),c.groupId(),originalCut,directory,prepared.schema(),range.groupId(),source.local,prepared.sourceOwner(),prepared.image().position(),prepared.image().sha256(),cfg.term(),cfg.index(),cfg.data(),prepared.image().topologyOrigin(),Map.of(source.first.columnIds().iterator().next(),"unready-current"),prepared.image().deterministicMergeColumns(),prepared.schemaRows());
                    Path destination;var initial=new RatisNodeRuntimeTest.FakeServer();initial.skipFirstNoop=true;
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),route),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,initial::create,List.of(source.first))) {
                        node.start();assertFalse(initial.machines.containsKey(group));var store=node.nativePool(pool).openCurrentRangeRecovery(self);assertNull(store.applied());assertNull(store.configuration());assertFalse(store.pendingInstallation());
                        var restoredOwner=field(node,"pools",Map.class).get(pool);destination=field(field(restoredOwner,"artifacts",RatisTopologySourceSnapshotArtifacts.class),"root",Path.class);
                    }
                    assertNotNull(destination);field(sourceOwner,"sources",RatisTopologySourceSnapshotService.class).stop().get(10,TimeUnit.SECONDS);assertEquals(0,source.store(range.groupId()).activeSnapshotPins());
                    copyCanonicalArtifacts(field(sourceArtifacts,"root",Path.class),destination);
                    var machines=new HashMap<RaftGroupId,RatisRangeStateMachine>();
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),route),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.RECOVER,(p,logs,created)->{machines.putAll(created);return RatisLocalMetadataDiscoveryTest.noSocketServer(p,logs,created);},List.of(source.first))) {
                        node.relocationTransport(descriptor->RatisPendingCurrentMetadataRegistrationTest.client(source,descriptor),(descriptor,owner)->receiver.client());node.start();
                        var server=field(node,"server",RaftServer.class);var division=server.getDivision(group);var machine=(RatisRangeStateMachine)division.getStateMachine();assertSame(machines.get(group),machine);
                        var store=field(machine,"store",ReplicatedRangeStore.class);var actualGroup=field(node,"groups",RatisRuntimeGroups.class).require(range.groupId()).group();
                        var nativeA=store.applied();var nativeCfg=store.configuration();long coreCfg=division.getRaftConf().getLogEntryIndex();boolean discoveryPending=machine.recoveryEligibilityGate().discoveryPending();
                        assertFalse(discoveryPending,"Registered CURRENT is MISSING without an invented discovery defer");assertNull(nativeA);assertNotNull(nativeCfg);assertEquals(0,nativeCfg.term());assertEquals(-1,nativeCfg.index());assertEquals(self,store.currentRangeRecoveryRegistration());assertNull(store.recoverySupersession());assertNull(store.topologyFreeze());assertNull(store.metadataRecoveryRegistration());
                        assertEquals(-1,division.getRaftLog().getLastCommittedIndex());assertEquals(-1,division.getRaftLog().getSnapshotIndex());assertEquals(-1,machine.getLastAppliedTermIndex().getIndex());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,machine.recoveryEligibilityGate().snapshot().mode());
                        var artifacts=field(field(node,"pools",Map.class).get(pool),"artifacts",RatisTopologySourceSnapshotArtifacts.class);assertNotNull(artifacts.lookup(old));long beforeImages=artifacts.reservedImageBytes(),currentImages=2*settings.images().maxImageBytes();assertTrue(beforeImages>currentImages,"The genuine split recovery anchor must remain charged");var unrelated=unrelatedArtifacts(destination,old.transferId());assertTrue(unrelated.keySet().stream().anyMatch(name->name.endsWith(".recovery-reserved")));var resources=field(node,"resources",RatisNodeResourceBudget.class);var before=resources.stats();
                        for(var original:List.of(old,absent)){
                            var obsolete=new RatisRecoveryObsoleteRpc.Request(original,scope,receiver.fence.digest());var reply=RatisRecoverySupersessionQueryTest.stale(server,actualGroup,RatisRecoveryObsoleteRpc.request(obsolete),0);
                            assertTrue(reply.isSuccess(),()->String.valueOf(reply.getException()));RatisRecoveryObsoleteRpc.reply(reply.getMessage().getContent().toByteArray(),obsolete);
                            assertEquals(original.equals(old),artifacts.isObsolete(obsolete));assertEquals(beforeImages-currentImages,artifacts.reservedImageBytes());assertEquals(unrelated,unrelatedArtifacts(destination,old.transferId()));assertFalse(Files.exists(destination.resolve(original.transferId()+".released")));if(original.equals(absent))assertFalse(Files.exists(destination.resolve(original.transferId()+".obsolete")));
                            RatisRecoverySupersessionQueryTest.rejected(server,actualGroup,RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,original),0);assertFalse(Files.exists(destination.resolve(original.transferId()+".reserved")),"Consumed CURRENT must fail before artifact reservation");
                        }
                        assertEquals(beforeImages-currentImages,artifacts.reservedImageBytes());assertEquals(unrelated,unrelatedArtifacts(destination,old.transferId()));assertEquals(nativeA,store.applied());var afterCfg=store.configuration();assertNotNull(afterCfg);assertEquals(nativeCfg.term(),afterCfg.term());assertEquals(nativeCfg.index(),afterCfg.index());assertArrayEquals(nativeCfg.data(),afterCfg.data());assertEquals(coreCfg,division.getRaftConf().getLogEntryIndex());assertEquals(-1,division.getRaftLog().getLastCommittedIndex());assertEquals(-1,machine.getLastAppliedTermIndex().getIndex());
                        assertNull(store.topologyFreeze());assertNull(store.recoverySupersession());assertNull(store.metadataRecoveryRegistration());assertFalse(store.metadataRecoveryImageInstalled());assertFalse(store.admissionOpen());assertFalse(store.currentRangeRecoveryReady());assertEquals(discoveryPending,machine.recoveryEligibilityGate().discoveryPending());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,machine.recoveryEligibilityGate().snapshot().mode());assertThrows(IllegalStateException.class,machine.recoveryEligibilityGate()::snapshotPermit);
                        assertFalse(node.readiness().groups().stream().filter(status->status.group().equals(range.groupId())).findFirst().orElseThrow().servingReady());assertNull(receiver.store.applied());assertNull(receiver.store.configuration());assertEquals(before,resources.stats());
                    }
                }
            }
        }
    }
    static Map<String,String> unrelatedArtifacts(Path root,UUID current)throws Exception {
        var result=new TreeMap<String,String>();try(var files=Files.walk(root)){for(var path:files.filter(path->Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)).toList()){
            var relative=root.relativize(path);String first=relative.getName(0).toString();if(first.equals("artifacts.lock") || first.startsWith(current+"."))continue;
            result.put(relative.toString(),HexFormat.of().formatHex(NativeRangeImage.md5Admitted(path,NativeRangeImage.IoAdmission.UNLIMITED)));
        }}return result;
    }
    static void copyCanonicalArtifacts(Path source,Path destination)throws Exception {
        var paths=new ArrayList<Path>();try(var files=Files.walk(source)){files.forEach(paths::add);}assertTrue(paths.size()<32,"Bounded original capture namespace");
        for(var path:paths){if(path.equals(source)||path.getFileName().toString().equals("artifacts.lock"))continue;assertFalse(path.getFileName().toString().endsWith(".pending"),"Original capture must be fully published");var target=destination.resolve(source.relativize(path));if(Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))Files.createDirectories(target);else{assertTrue(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS));Files.copy(path,target);SessionJournalFiles.force(target);}}
        for(var path:paths.stream().filter(path->Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS)).sorted(Comparator.reverseOrder()).toList())SessionJournalFiles.force(destination.resolve(source.relativize(path)));SessionJournalFiles.force(destination);
    }
}
