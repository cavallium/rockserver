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
            machine=new RatisRangeStateMachine(store,group,limits);machine.cdcBinding(directory.clusterId(),directory.databaseId(),descriptor.rangeId());machine.topologyAuthority((header,stage)->{throw new IOException("Snapshot replay cannot read fresh catalogue authority");},directory,local,pool);
            storage=RaftStorage.newBuilder().setDirectory(path.toFile()).setOption(recover?RaftStorage.StartupOption.RECOVER:RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();
            var server=mock(RaftServer.class);when(server.getId()).thenReturn(RaftPeerId.valueOf(local.peerId()));
            try{machine.initialize(server,group.getGroupId(),storage);}catch(Exception failure){try{machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}throw failure;}
        }
        void install(Path file,NativeRangeImage.Manifest manifest)throws Exception {
            machine.prepareRecoveryReplacement();machine.pause();var target=machine.getStateMachineStorage().getSnapshotFile(manifest.position().term(),manifest.position().index()).toPath();Files.createDirectories(target.getParent());Files.copy(file,target);MD5FileUtil.saveMD5File(target.toFile(),MD5FileUtil.computeMd5ForFile(target.toFile()));SessionJournalFiles.force(target);SessionJournalFiles.force(MD5FileUtil.getDigestFileForFile(target.toFile()).toPath());SessionJournalFiles.force(target.getParent());machine.reinitialize();
        }
        @Override public void close()throws Exception{machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();}
    }
    ReplicatedRangeStore.CdcPage cdc(ReplicatedRangeStore store,RangeDescriptor range)throws Exception {
        var lineage=new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch());
        try{return store.pollCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),256,8<<20);}catch(ReplicatedRangeStore.CdcGap gap){return store.pollCdc(lineage,gap.page().floor(),256,8<<20);}
    }
    Map<String,byte[]> cdcRows(ReplicatedRangeStore store)throws Exception {
        var rows=new TreeMap<String,byte[]>();try(var pin=store.capture()){pin.visitState((key,value)->{if(key.length>16 && key[16]>=0x50 && key[16]<=0x52)rows.put(HexFormat.of().formatHex(key),value.clone());});}return rows;
    }
    @Test void currentInitialImageOwnTailReadinessAndOwnSnapshotSurviveReplacementAndColdReopen()throws Exception {
        NativeCurrentRangeRecovery registration;long completedCut;Path receiver=root.resolve("current-receiver"),raft=root.resolve("current-raft");
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false)) {
            var split=source.split();var directory=source.routes.get(source.first.domainId()).directory();var range=split.targets().getFirst();var store=source.store(range.groupId());var image=root.resolve("current.image");var manifest=NativeRangeImage.write(image,store,limits);var cfg=store.configuration();var rows=new TreeMap<String,byte[]>();try(var pin=store.capture()){pin.visitState((key,value)->{if(NativeTopologyLearnerSchema.row(key))rows.put(HexFormat.of().formatHex(key),value);});}
            var names=new HashMap<UUID,String>();directory.columns().forEach(column->names.put(column,"recovered-"+column));registration=new NativeCurrentRangeRecovery(new UUID(0,9700),source.plan.catalogue().groupId(),new RatisTopologyAuthority.Client(source.catalogue,source.first.clusterId(),source.first.databaseId(),source.plan.catalogue().groupId()).read(split.header(),TopologyChange.Stage.ACTIVATE).cut(),directory,source.first,range.groupId(),source.local,source.local,manifest.position(),manifest.sha256(),cfg.term(),cfg.index(),cfg.data(),manifest.topologyOrigin(),names,Set.of(),rows);
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(receiver)) {
                var target=pool.openCurrentRangeRecovery(registration);target.enableSessionControls();try(var adapter=new Adapter(target,pool,directory,source.local,raft,false)) {
                    assertTrue(target.topologyRecoveryOnly());assertFalse(target.admissionOpen());assertNull(adapter.machine.getLatestSnapshot());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,adapter.machine.recoveryEligibilityGate().snapshot().mode());
                    assertThrows(IllegalStateException.class,target::capture);adapter.install(image,manifest);
                    assertTrue(target.currentRangeRecoveryImageInstalled());assertTrue(target.topologyRecoveryOnly());assertFalse(target.admissionOpen());assertThrows(IllegalStateException.class,target::capture);
                    assertEquals(manifest.position().index(),adapter.machine.getLatestSnapshot().getIndex());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.RECOVERY,adapter.machine.recoveryEligibilityGate().snapshot().mode());assertEquals(-1,adapter.machine.takeSnapshot());
                    long next=manifest.position().index()+1;var configuration=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.parseFrom(cfg.data());var old=adapter.machine.recoveryEligibilityGate().snapshot();
                    var sourceMachine=source.server.machines.get(RaftGroupId.valueOf(range.groupId()));sourceMachine.notifyConfigurationChanged(manifest.position().term(),next,configuration);sourceMachine.notifyTermIndexUpdated(manifest.position().term(),next);
                    adapter.machine.notifyConfigurationChanged(manifest.position().term(),next,configuration);assertEquals(old.configurationIndex(),adapter.machine.recoveryEligibilityGate().snapshot().configurationIndex());
                    adapter.machine.notifyTermIndexUpdated(manifest.position().term(),next);var advanced=adapter.machine.recoveryEligibilityGate().snapshot();assertEquals(next,advanced.configurationIndex());assertEquals(next,advanced.nativeCut().getIndex());assertEquals(configuration,advanced.configuration());
                    sourceMachine.notifyTermIndexUpdated(manifest.position().term(),next+1);adapter.machine.notifyTermIndexUpdated(manifest.position().term(),next+1);assertEquals(store.applied(),target.applied());assertEquals(store.configuration().index(),target.configuration().index());assertEquals(next+1,adapter.machine.recoveryEligibilityGate().snapshot().nativeCut().getIndex());assertTrue(target.topologyRecoveryOnly());assertFalse(target.admissionOpen());assertThrows(IllegalStateException.class,target::capture);
                    var binding=source.plan.catalogue();var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());var ledger=epochs.read();var pending=epochs.prepareGrant(new OperationId(new UUID(0,9702),0,0),ledger,new UUID(0,9703),ledger.logicalTimeMillis()+60000);assertEquals(CommandResult.Status.SUCCESS,epochs.execute(pending.command()).status());ledger=epochs.read();
                    var bind=DistributedCommand.sessionControl(range.groupId(),range.rangeEpoch(),directory.schemaDigest(),new OperationId(new UUID(0,9704),ledger.minimumEpoch(),0),RangeSessionControl.bind(ledger,pending.grant(),16),-1);
                    var bound=source.apply(range.groupId(),DistributedCommandCodec.encode(bind));assertArrayEquals(bound.getContent().toByteArray(),RatisMetadataOutcomeReplayTest.replay(adapter.machine,DistributedCommandCodec.encode(bind),store.applied().index()));
                    UUID column=directory.columns().iterator().next();var put=new DistributedCommand(range.groupId(),range.rangeEpoch(),directory.schemaDigest(),new OperationId(pending.grant().sessionId(),pending.grant().epoch(),0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{9},new byte[]{44})));
                    var result=source.apply(range.groupId(),DistributedCommandCodec.encode(put));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(result.getContent().toByteArray()).status());assertArrayEquals(result.getContent().toByteArray(),RatisMetadataOutcomeReplayTest.replay(adapter.machine,DistributedCommandCodec.encode(put),store.applied().index()));
                    assertEquals(store.applied(),target.applied());assertEquals(store.applied().index(),adapter.machine.recoveryEligibilityGate().snapshot().nativeCut().getIndex());assertTrue(target.topologyRecoveryOnly());assertThrows(IllegalStateException.class,()->target.readCurrentPoint(column,new byte[]{9}));source.service.verifyCatalogue();
                    target.completeCurrentRangeRecovery(registration,store.applied(),store.configuration());adapter.machine.refreshRecoveryEligibility();assertTrue(target.currentRangeRecoveryReady());assertTrue(target.admissionOpen());assertArrayEquals(new byte[]{44},target.readCurrentPoint(column,new byte[]{9}));
                    var sourceCdc=cdc(store,range);var receiverCdc=cdc(target,range);assertEquals(sourceCdc.cut(),receiverCdc.cut());assertEquals(sourceCdc.floor(),receiverCdc.floor());assertEquals(sourceCdc.next(),receiverCdc.next());assertFalse(sourceCdc.records().isEmpty());assertEquals(sourceCdc.records().size(),receiverCdc.records().size());for(int n=0;n<sourceCdc.records().size();n++)assertArrayEquals(sourceCdc.records().get(n).encode(),receiverCdc.records().get(n).encode());
                    var sourceCdcRows=cdcRows(store);var receiverCdcRows=cdcRows(target);assertEquals(sourceCdcRows.keySet(),receiverCdcRows.keySet());for(var row:sourceCdcRows.entrySet())assertArrayEquals(row.getValue(),receiverCdcRows.get(row.getKey()));assertEquals(ledger.logicalTimeMillis(),target.metadataOutcomeState().sessionAuthority().logicalTimeMillis());
                    completedCut=target.applied().index();assertEquals(completedCut,adapter.machine.takeSnapshot());assertEquals(completedCut,adapter.machine.recoveryEligibilityGate().snapshot().snapshotCut().getIndex());var own=adapter.machine.getLatestSnapshot().getFiles().getFirst().getPath();assertEquals(completedCut,target.validateCurrentRangeRecoveryImage(own,limits).position().index());
                    adapter.machine.prepareRecoveryReplacement();adapter.machine.pause();adapter.machine.reinitialize();assertTrue(target.currentRangeRecoveryReady());assertEquals(completedCut,target.applied().index());assertArrayEquals(new byte[]{44},target.readCurrentPoint(column,new byte[]{9}));assertEquals(completedCut,adapter.machine.getLatestSnapshot().getIndex());
                }
            }
        }
        try(var pool=new RatisTopologyRecoveryAnchorTest().pool(receiver)) {
            var target=pool.openCurrentRangeRecovery(registration);target.enableSessionControls();try(var adapter=new Adapter(target,pool,registration.directory(),registration.receiver(),raft,true)) {
                assertFalse(target.topologyRecoveryOnly());assertTrue(target.admissionOpen());assertTrue(target.currentRangeRecoveryImageInstalled());assertEquals(completedCut,target.applied().index());assertEquals(completedCut,adapter.machine.getLatestSnapshot().getIndex());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.RECOVERY,adapter.machine.recoveryEligibilityGate().snapshot().mode());assertArrayEquals(new byte[]{44},target.readCurrentPoint(registration.directory().columns().iterator().next(),new byte[]{9}));
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
    @Test void completedMetadataOwnerPublishesOwnSnapshotWithOnePinAndColdReopens()throws Exception {
        var helper=new RatisSourceOutcomeTest();NativeMetadataRecoveryRegistration registration;long cut;Path receiver=root.resolve("ready-metadata-receiver"),raft=root.resolve("ready-metadata-raft");
        try(var f=helper.new Fixture(root.resolve("ready-metadata-source"),false)) {
            var source=f.home.source;Path initial=root.resolve("ready-metadata.image");var image=NativeRangeImage.writeMetadata(initial,source.store,limits);var cfg=source.store.configuration();registration=new NativeMetadataRecoveryRegistration(new UUID(0,9705),source.catalogue,new ApplyPosition(1,1),source.directory,source.freeze,source.local,source.local,cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of());
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(receiver)) {
                pool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(1,8L<<30,java.time.Duration.ofSeconds(60),0));var target=pool.openMetadataRecovery(registration);target.enableSessionControls();
                try(var adapter=new Adapter(target,pool,source.directory,source.local,raft,false)) {
                    adapter.install(initial,image);var clock=source.clock(997,20);byte[] result=f.applyControl(clock);assertArrayEquals(result,RatisMetadataOutcomeReplayTest.replay(adapter.machine,DistributedCommandCodec.encode(clock),source.store.applied().index()));assertEquals(source.store.applied(),target.applied());assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::metadataOutcomeState);
                    target.completeMetadataRecovery(registration,source.store.applied(),source.store.configuration());adapter.machine.refreshRecoveryEligibility();assertFalse(target.metadataRecoveryPending());assertFalse(target.admissionOpen());assertThrows(IllegalStateException.class,target::capture);
                    cut=target.applied().index();assertEquals(cut,adapter.machine.takeSnapshot());assertEquals(0,pool.snapshotAdmissionStats().pins());assertEquals(cut,adapter.machine.recoveryEligibilityGate().snapshot().snapshotCut().getIndex());var file=adapter.machine.getLatestSnapshot().getFiles().getFirst().getPath();assertEquals(cut,NativeRangeImage.inspectMetadata(file,limits).position().index());assertThrows(IOException.class,()->NativeRangeImage.inspect(file,limits));
                    adapter.machine.prepareRecoveryReplacement();adapter.machine.pause();adapter.machine.reinitialize();assertFalse(target.metadataRecoveryPending());assertEquals(cut,target.metadataOutcomeState().applied().index());assertFalse(target.admissionOpen());assertEquals(0,pool.snapshotAdmissionStats().pins());
                }
            }
        }
        try(var pool=new RatisTopologyRecoveryAnchorTest().pool(receiver)) {
            pool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(1,8L<<30,java.time.Duration.ofSeconds(60),0));var target=pool.openMetadataRecovery(registration);target.enableSessionControls();
            try(var adapter=new Adapter(target,pool,registration.sourceDirectory(),registration.receiver(),raft,true)){assertFalse(target.metadataRecoveryPending());assertFalse(target.admissionOpen());assertEquals(cut,target.metadataOutcomeState().applied().index());assertEquals(cut,adapter.machine.getLatestSnapshot().getIndex());assertEquals(20,target.metadataOutcomeState().sessionAuthority().logicalTimeMillis());assertEquals(0,pool.snapshotAdmissionStats().pins());}
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
