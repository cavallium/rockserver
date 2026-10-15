package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Actual JNI images and durable adapter snapshot files; controlled server identity, no network/quorum. */
class RatisRecoverySnapshotLifecycleTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4<<20,32<<20,16);
    final class Adapter implements AutoCloseable {
        final RatisRangeStateMachine machine;final RaftStorage storage;
        Adapter(ReplicatedRangeStore store,DistributedStoragePool pool,RangeDirectory directory,ReplicaDescriptor local,Path path,boolean recover)throws Exception {
            var descriptor=directory.ranges().stream().filter(range->range.groupId().equals(store.ownership().owner().groupId())).findFirst().orElseThrow();
            var peers=descriptor.replicas().stream().map(replica->RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).build()).toList();var group=RaftGroup.valueOf(RaftGroupId.valueOf(descriptor.groupId()),peers);
            machine=new RatisRangeStateMachine(store,group,limits);machine.topologyAuthority((header,stage)->{throw new IOException("Snapshot replay cannot read fresh catalogue authority");},directory,local,pool);
            storage=RaftStorage.newBuilder().setDirectory(path.toFile()).setOption(recover?RaftStorage.StartupOption.RECOVER:RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();
            var server=mock(RaftServer.class);when(server.getId()).thenReturn(RaftPeerId.valueOf(local.peerId()));
            try{machine.initialize(server,group.getGroupId(),storage);}catch(Exception failure){try{machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}throw failure;}
        }
        void install(Path file,NativeRangeImage.Manifest manifest)throws Exception {
            machine.prepareRecoveryReplacement();machine.pause();var target=machine.getStateMachineStorage().getSnapshotFile(manifest.position().term(),manifest.position().index()).toPath();Files.createDirectories(target.getParent());Files.copy(file,target);MD5FileUtil.saveMD5File(target.toFile(),MD5FileUtil.computeMd5ForFile(target.toFile()));SessionJournalFiles.force(target);SessionJournalFiles.force(MD5FileUtil.getDigestFileForFile(target.toFile()).toPath());SessionJournalFiles.force(target.getParent());machine.reinitialize();
        }
        @Override public void close()throws Exception{machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();}
    }
    @Test void currentWithoutImageInitializesFencedThenInstallsOwnSnapshotAndColdReopensStillPending()throws Exception {
        NativeCurrentRangeRecovery registration;Path receiver=root.resolve("current-receiver"),raft=root.resolve("current-raft");
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false)) {
            var split=source.split();var directory=source.routes.get(source.first.domainId()).directory();var range=split.targets().getFirst();var store=source.store(range.groupId());var image=root.resolve("current.image");var manifest=NativeRangeImage.write(image,store,limits);var cfg=store.configuration();var rows=new TreeMap<String,byte[]>();try(var pin=store.capture()){pin.visitState((key,value)->{if(NativeTopologyLearnerSchema.row(key))rows.put(HexFormat.of().formatHex(key),value);});}
            var names=new HashMap<UUID,String>();directory.columns().forEach(column->names.put(column,"recovered-"+column));registration=new NativeCurrentRangeRecovery(new UUID(0,9700),source.plan.catalogue().groupId(),new RatisTopologyAuthority.Client(source.catalogue,source.first.clusterId(),source.first.databaseId(),source.plan.catalogue().groupId()).read(split.header(),TopologyChange.Stage.ACTIVATE).cut(),directory,source.first,range.groupId(),source.local,source.local,manifest.position(),manifest.sha256(),cfg.term(),cfg.index(),cfg.data(),manifest.topologyOrigin(),names,Set.of(),rows);
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(receiver)) {
                var target=pool.openCurrentRangeRecovery(registration);try(var adapter=new Adapter(target,pool,directory,source.local,raft,false)) {
                    assertTrue(target.topologyRecoveryOnly());assertFalse(target.admissionOpen());assertNull(adapter.machine.getLatestSnapshot());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,adapter.machine.recoveryEligibilityGate().snapshot().mode());
                    assertThrows(IllegalStateException.class,target::capture);adapter.install(image,manifest);
                    assertTrue(target.currentRangeRecoveryImageInstalled());assertTrue(target.topologyRecoveryOnly());assertFalse(target.admissionOpen());assertThrows(IllegalStateException.class,target::capture);
                    assertEquals(manifest.position().index(),adapter.machine.getLatestSnapshot().getIndex());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.RECOVERY,adapter.machine.recoveryEligibilityGate().snapshot().mode());assertEquals(-1,adapter.machine.takeSnapshot());
                    long next=manifest.position().index()+1;var configuration=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.parseFrom(cfg.data());var old=adapter.machine.recoveryEligibilityGate().snapshot();
                    var sourceMachine=source.server.machines.get(RaftGroupId.valueOf(range.groupId()));sourceMachine.notifyConfigurationChanged(manifest.position().term(),next,configuration);sourceMachine.notifyTermIndexUpdated(manifest.position().term(),next);
                    adapter.machine.notifyConfigurationChanged(manifest.position().term(),next,configuration);assertEquals(old.configurationIndex(),adapter.machine.recoveryEligibilityGate().snapshot().configurationIndex());
                    adapter.machine.notifyTermIndexUpdated(manifest.position().term(),next);var advanced=adapter.machine.recoveryEligibilityGate().snapshot();assertEquals(next,advanced.configurationIndex());assertEquals(next,advanced.nativeCut().getIndex());assertEquals(configuration,advanced.configuration());
                    sourceMachine.notifyTermIndexUpdated(manifest.position().term(),next+1);adapter.machine.notifyTermIndexUpdated(manifest.position().term(),next+1);assertEquals(store.applied(),target.applied());assertEquals(store.configuration().index(),target.configuration().index());assertEquals(next+1,adapter.machine.recoveryEligibilityGate().snapshot().nativeCut().getIndex());assertTrue(target.topologyRecoveryOnly());assertFalse(target.admissionOpen());assertThrows(IllegalStateException.class,target::capture);
                }
            }
        }
        try(var pool=new RatisTopologyRecoveryAnchorTest().pool(receiver)) {
            var target=pool.openCurrentRangeRecovery(registration);try(var adapter=new Adapter(target,pool,registration.directory(),registration.receiver(),raft,true)) {
                assertTrue(target.topologyRecoveryOnly());assertFalse(target.admissionOpen());assertTrue(target.currentRangeRecoveryImageInstalled());assertEquals(registration.imageCut().index(),adapter.machine.getLatestSnapshot().getIndex());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.RECOVERY,adapter.machine.recoveryEligibilityGate().snapshot().mode());assertThrows(IllegalStateException.class,target::capture);
            }
        }
    }
    @Test void metadataReceiverUsesStrictSemanticSnapshotInstallAndColdReloadWithoutUserAdmission()throws Exception {
        var helper=new RatisSourceOutcomeTest();NativeMetadataRecoveryRegistration registration;Path receiver=root.resolve("metadata-receiver"),raft=root.resolve("metadata-raft");
        try(var f=helper.new Fixture(root.resolve("metadata-source"),false)) {
            var source=f.home.source;var image=root.resolve("metadata.image");var manifest=NativeRangeImage.writeMetadata(image,source.store,limits);var cfg=source.store.configuration();registration=new NativeMetadataRecoveryRegistration(new UUID(0,9701),source.catalogue,new ApplyPosition(1,1),source.directory,source.freeze,source.local,source.local,cfg.term(),cfg.index(),cfg.data(),manifest.position(),manifest.sha256(),Set.of());
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(receiver)) {
                var target=pool.openMetadataRecovery(registration);target.enableSessionControls();target.enableCdc(source.rangeId);try(var adapter=new Adapter(target,pool,source.directory,source.local,raft,false)) {
                    assertTrue(target.metadataRecoveryPending());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,adapter.machine.recoveryEligibilityGate().snapshot().mode());adapter.install(image,manifest);
                    assertTrue(target.metadataRecoveryImageInstalled());assertTrue(target.metadataRecoveryPending());assertFalse(target.admissionOpen());assertThrows(IllegalStateException.class,target::metadataOutcomeState);assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.RECOVERY,adapter.machine.recoveryEligibilityGate().snapshot().mode());
                }
            }
        }
        try(var pool=new RatisTopologyRecoveryAnchorTest().pool(receiver)) {
            var target=pool.openMetadataRecovery(registration);try(var adapter=new Adapter(target,pool,registration.sourceDirectory(),registration.receiver(),raft,true)){assertTrue(target.metadataRecoveryPending());assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.admissionOpen());assertThrows(IllegalStateException.class,target::metadataOutcomeState);assertEquals(registration.imageCut().index(),adapter.machine.getLatestSnapshot().getIndex());}
        }
    }
    @Test void originalRetiredSourceSnapshotsOnlyMetadataAtNewActualOwnCutAndReinitializes()throws Exception {
        var helper=new RatisSourceOutcomeTest();try(var f=helper.new Fixture(root.resolve("retired-source"),false)) {
            var source=f.home.source;assertTrue(source.store.topologyRetired());assertEquals(-1,f.machine.takeSnapshot());f.applyControl(source.clock(995,20));long cut=source.store.applied().index();assertTrue(cut>source.freeze.position().index());assertEquals(cut,f.machine.takeSnapshot());
            var file=f.machine.getLatestSnapshot().getFiles().getFirst().getPath();assertEquals(cut,NativeRangeImage.inspectMetadata(file,limits).position().index());assertThrows(IOException.class,()->NativeRangeImage.inspect(file,limits));
            f.machine.prepareRecoveryReplacement();f.machine.pause();assertNull(f.machine.getLatestSnapshot());f.machine.reinitialize();assertTrue(source.store.topologyRetired());assertFalse(source.store.admissionOpen());assertEquals(cut,f.machine.getLatestSnapshot().getIndex());assertEquals(cut,source.store.applied().index());
        }
    }
}
