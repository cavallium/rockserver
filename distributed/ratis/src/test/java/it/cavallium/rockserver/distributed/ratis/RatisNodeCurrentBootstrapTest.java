package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Fresh CAT role plus actual native USER restoration and socket-free core reload. */
class RatisNodeCurrentBootstrapTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void currentBootstrapRestoresRealUserFloorAndReopensWithoutInventingNativeProgress(boolean behind)throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false)) {
            var directory=source.firstDirectory;var group=directory.ranges().getFirst().groupId();var id=RaftGroupId.valueOf(group);var column=source.first.columnIds().iterator().next();var original=source.store(group).applied();var limits=source.manifest.settings().images();
            var initial=root.resolve("initial.group");NativeRangeImage.write(initial,source.store(group),limits);Path future=root.resolve("future.group");ApplyPosition floor=original;
            if(behind)try(var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("later-native"))) {
                var helper=new RatisDiscoveryInventoryTest();helper.root=root;var copy=helper.open(pool,directory,source.first);copy.fence();copy.installSnapshot(initial,limits);copy.activate();
                floor=new ApplyPosition(original.term(),original.index()+1);var command=new DistributedCommand(group,directory.ranges().getFirst().rangeEpoch(),directory.schemaDigest(),new OperationId(new UUID(12000,1),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{8},new byte[]{77})));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(copy.apply(floor,command)).status());NativeRangeImage.write(future,copy,limits);
            }
            for(var machine:source.server.machines.values())assertTrue(machine.takeSnapshot()>=0);
            for(var entry:source.server.storages.entrySet())((org.apache.ratis.server.storage.RaftStorageImpl)entry.getValue()).writeBootstrapConfiguration(source.server.division(entry.getKey()).getRaftConf());
            var published=field(source.server.machines.get(id),"snapshots",SimpleStateMachineStorage.class).getSnapshotFile(floor.term(),floor.index()).toPath();source.node.close();
            if(behind){Files.copy(future,published);MD5FileUtil.saveMD5File(published.toFile(),MD5FileUtil.computeMd5ForFile(published.toFile()));SessionJournalFiles.force(published);SessionJournalFiles.force(published.getParent());}
            var borrowed=source.service.restoreResources();var c=source.plan.catalogue();var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            for(int reopen=0;reopen<2;reopen++)try(var node=RatisNodeRuntime.open(source.plan,source.root.resolve("node"),ConfigParser.parseDefault(),epochs,source.routes,borrowed.scheduler,borrowed.timer,()->20L,source.manifest.settings(),RatisNodeRuntime.Startup.RECOVER,(plan,logs,machines)->{machines.get(id).deferRecoveryDiscovery();return RatisLocalMetadataDiscoveryTest.noSocketServer(plan,logs,machines);},List.of(source.first,source.second))) {
                node.relocationTransport(range->{assertEquals(c.groupId(),range.groupId());return source.catalogue;},(range,owner)->{throw new AssertionError("Local CURRENT restoration needs no DATA transfer");});node.start();
                var machine=(RatisRangeStateMachine)field(node,"server",RaftServer.class).getDivision(id).getStateMachine();var store=field(machine,"store",ReplicatedRangeStore.class);assertEquals(reopen==0?original:floor,store.applied());assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());assertThrows(IllegalStateException.class,()->store.readCurrentPoint(column,new byte[]{8}));
                node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);assertFalse(machine.recoveryEligibilityGate().discoveryPending());assertEquals(floor,store.applied());assertEquals(floor.index(),machine.getLastAppliedTermIndex().getIndex());assertEquals(floor.index(),field(node,"server",RaftServer.class).getDivision(id).getRaftLog().getSnapshotIndex());assertTrue(store.admissionOpen());assertArrayEquals(new byte[]{11},store.readCurrentPoint(column,new byte[]{7}));assertArrayEquals(behind?new byte[]{77}:null,store.readCurrentPoint(column,new byte[]{8}));assertEquals(0,store.activeSnapshotPins());
                node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);assertEquals(floor,store.applied());assertEquals(0,store.activeSnapshotPins());assertTrue(field(node,"configuredRecoveryServiceStarted",Boolean.class));
            }
        }
    }
}
