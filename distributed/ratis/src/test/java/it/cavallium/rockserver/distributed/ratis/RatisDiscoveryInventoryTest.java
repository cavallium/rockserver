package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Inventory retains real Raft floors while native USER data and completed markers remain untouched. */
class RatisDiscoveryInventoryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4<<20,32<<20,8);
    @FunctionalInterface interface Setup {void prepare(RatisRangeStateMachine machine,Path floor)throws Exception;}
    final class Adapter implements AutoCloseable {
        final RatisRangeStateMachine machine;final RaftStorage storage;
        Adapter(ReplicatedRangeStore store,DistributedStoragePool pool,RangeDirectory directory,ReplicaDescriptor local,Path path,Path initialImage,ApplyPosition floor,Setup setup)throws Exception {
            var range=directory.ranges().getFirst();var group=RaftGroup.valueOf(RaftGroupId.valueOf(range.groupId()),range.replicas().stream().map(r->RaftPeer.newBuilder().setId(r.peerId()).setAddress(r.address()).build()).toList());
            machine=new RatisRangeStateMachine(store,group,limits);machine.topologyAuthority((header,stage)->{throw new IOException("Inventory has no remote authority IO");},directory,local,pool);machine.deferRecoveryDiscovery();
            storage=RaftStorage.newBuilder().setDirectory(path.toFile()).setOption(initialImage==null?RaftStorage.StartupOption.RECOVER:RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(SizeInBytes.ONE_MB).build();storage.initialize();
            try {
                if(initialImage!=null){var files=new SimpleStateMachineStorage();files.init(storage);var target=files.getSnapshotFile(floor.term(),floor.index()).toPath();Files.createDirectories(target.getParent());Files.copy(initialImage,target);MD5FileUtil.saveMD5File(target.toFile(),MD5FileUtil.computeMd5ForFile(target.toFile()));setup.prepare(machine,target);}
                var server=mock(RaftServer.class);when(server.getId()).thenReturn(RaftPeerId.valueOf(local.peerId()));machine.initialize(server,group.getGroupId(),storage);
            }catch(Exception failure){machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();throw failure;}
        }
        @Override public void close()throws Exception{machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();}
    }
    ReplicatedRangeStore open(DistributedStoragePool pool,RangeDirectory directory,DistributedSchema schema)throws Exception {
        var range=directory.ranges().getFirst();var store=pool.openRange(new RangeOwnership.Owner(range.groupId(),range.rangeEpoch()),new RangeOwnership.Interval(directory.domainId(),directory.columns(),range.lowerInclusive(),range.upperExclusive()),directory.schemaDigest(),Map.of(schema.columnIds().iterator().next(),"data"),Set.of());store.enableLogicalSchema(schema);return store;
    }
    byte[] rawUser(DistributedStoragePool pool,byte[] key)throws Exception {
        var loaded=field(pool,"loaded",RocksDBLoader.LoadedDb.class);var handles=field(pool,"handles",Map.class);return loaded.db().get().get((org.rocksdb.ColumnFamilyHandle)handles.get("data"),key);
    }
    @Test void durableGroupFloorAheadOfActualNativeMarkerRemainsInventoryAcrossColdReopen()throws Exception {
        for(boolean absent:List.of(false,true))try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source-"+absent),false)) {
            var directory=source.firstDirectory;var range=directory.ranges().getFirst();var sourceStore=source.store(range.groupId());var column=source.first.columnIds().iterator().next();assertArrayEquals(new byte[]{11},sourceStore.readCurrentPoint(column,new byte[]{7}));
            var image=root.resolve("floor-"+absent+".group");var manifest=NativeRangeImage.write(image,sourceStore,limits);assertTrue(manifest.position().index()>0);
            Path nativePath=root.resolve("inventory-native-"+absent),raftPath=root.resolve("inventory-raft-"+absent);var pools=new RatisTopologyRecoveryAnchorTest();ApplyPosition actual=absent?null:new ApplyPosition(1,0);
            for(int reopen=0;reopen<2;reopen++)try(var pool=pools.pool(nativePath)) {
                var store=open(pool,directory,source.first);if(reopen==0){store.initializeConfiguration(sourceStore.configuration().data());if(!absent)store.applyNoop(actual);}
                assertEquals(actual,store.applied());assertNull(rawUser(pool,new byte[]{7}));assertThrows(UnsupportedOperationException.class,store::scopedHistoryStats);
                try(var adapter=new Adapter(store,pool,directory,source.local,raftPath,reopen==0?image:null,manifest.position(),(machine,file)->{})) {
                    var machine=adapter.machine;assertEquals(actual,store.applied());assertEquals(actual==null?TermIndex.INITIAL_VALUE:TermIndex.valueOf(actual.term(),actual.index()),machine.getLastAppliedTermIndex());assertNotNull(machine.getLatestSnapshot());assertEquals(manifest.position().index(),machine.getLatestSnapshot().getIndex());assertEquals(NativeRangeImage.Kind.GROUP,NativeRangeImage.kind(machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));
                    assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,machine.recoveryEligibilityGate().snapshot().mode());assertThrows(IllegalStateException.class,()->machine.recoveryEligibilityGate().snapshotPermit());assertFalse(store.admissionOpen());assertThrows(IllegalStateException.class,()->store.readCurrentPoint(column,new byte[]{7}));assertThrows(IllegalStateException.class,store::captureForRecovery);assertEquals(-1,machine.takeSnapshot());assertThrows(UnsupportedOperationException.class,store::scopedHistoryStats);assertNull(rawUser(pool,new byte[]{7}));
                    assertThrows(IOException.class,machine::reinitialize);assertEquals(manifest.position().index(),machine.getLatestSnapshot().getIndex());assertEquals(actual,store.applied());assertThrows(IllegalStateException.class,machine::deferRecoveryDiscovery);
                }
            }
        }
    }
    @Test void corruptedPublishedGroupFloorCannotBecomeInventoryAuthorityOrRestoreUserRows()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("corrupt-source"),false);var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("corrupt-native"))) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());Path image=root.resolve("corrupt-original.group");var manifest=NativeRangeImage.write(image,sourceStore,limits);var store=open(pool,directory,source.first);store.initializeConfiguration(sourceStore.configuration().data());store.applyNoop(new ApplyPosition(1,0));
            assertThrows(IOException.class,()->new Adapter(store,pool,directory,source.local,root.resolve("corrupt-raft"),image,manifest.position(),(machine,file)->{byte[] body=Files.readAllBytes(file);body[body.length-1]^=1;Files.write(file,body);}));assertEquals(new ApplyPosition(1,0),store.applied());assertFalse(store.admissionOpen());assertNull(rawUser(pool,new byte[]{7}));
        }
    }
    @Test void discoveryDeferralRequiresNewDataRole()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("role-source"),false)) {
            var range=source.firstDirectory.ranges().getFirst();var group=RaftGroup.valueOf(RaftGroupId.valueOf(range.groupId()),RaftPeer.newBuilder().setId(source.local.peerId()).setAddress(source.local.address()).build());var machine=new RatisRangeStateMachine(source.store(range.groupId()),group,limits);machine.role(RatisNodePlan.Role.CATALOGUE);assertThrows(IllegalStateException.class,machine::deferRecoveryDiscovery);assertTrue(source.store(range.groupId()).admissionOpen());
        }
    }
    @Test void inventoryRejectsSymlinkNonregularAndMismatchingStoredMd5Objects()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("objects-source"),false)) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());Path image=root.resolve("objects.group");var manifest=NativeRangeImage.write(image,sourceStore,limits);
            for(int kind=0;kind<5;kind++)try(var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("objects-native-"+kind))) {
                var store=open(pool,directory,source.first);store.initializeConfiguration(sourceStore.configuration().data());store.applyNoop(new ApplyPosition(1,0));int selected=kind;
                var failure=assertThrows(IOException.class,()->new Adapter(store,pool,directory,source.local,root.resolve("objects-raft-"+selected),image,manifest.position(),(machine,file)->{
                    var md5=MD5FileUtil.getDigestFileForFile(file.toFile()).toPath();
                    if(selected==0){Files.delete(file);Files.createSymbolicLink(file,image.toAbsolutePath());}
                    else if(selected==1){var saved=root.resolve("linked-md5");Files.copy(md5,saved);Files.delete(md5);Files.createSymbolicLink(md5,saved.toAbsolutePath());}
                    else if(selected==2){Files.delete(file);Files.createDirectory(file);}
                    else if(selected==3){Files.delete(md5);Files.createDirectory(md5);}
                    else MD5FileUtil.saveMD5File(file.toFile(),org.apache.ratis.io.MD5Hash.newInstance(new byte[16]));
                }));
                assertNotNull(failure);assertEquals(new ApplyPosition(1,0),store.applied());assertFalse(store.admissionOpen());assertNull(rawUser(pool,new byte[]{7}));
            }
        }
    }
    @Test void inventoryRejectsMoreThanItsExplicitRetainedFloorEnvelopeBeforeRestoration()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("bounded-source"),false);var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("bounded-native"))) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());var sourceMachine=source.server.machines.get(RaftGroupId.valueOf(sourceStore.ownership().owner().groupId()));var images=new ArrayList<Path>();var cuts=new ArrayList<ApplyPosition>();
            for(int i=0;i<=RatisRangeStateMachine.MAX_DISCOVERY_INVENTORY_FLOORS;i++) {
                if(i>0)sourceMachine.notifyTermIndexUpdated(1,sourceStore.applied().index()+1);
                var image=root.resolve("bounded-"+i+".group");var manifest=NativeRangeImage.write(image,sourceStore,limits);images.add(image);cuts.add(manifest.position());
            }
            var store=open(pool,directory,source.first);store.initializeConfiguration(sourceStore.configuration().data());store.applyNoop(new ApplyPosition(1,0));
            var failure=assertThrows(IOException.class,()->new Adapter(store,pool,directory,source.local,root.resolve("bounded-raft"),images.getFirst(),cuts.getFirst(),(machine,file)->{
                for(int i=0;i<images.size();i++){var cut=cuts.get(i);var target=file.getParent().resolve(SimpleStateMachineStorage.getSnapshotFileName(cut.term(),cut.index()));Files.copy(images.get(i),target,StandardCopyOption.REPLACE_EXISTING);MD5FileUtil.saveMD5File(target.toFile(),MD5FileUtil.computeMd5ForFile(target.toFile()));}
            }));
            assertTrue(causeContains(failure,"Discovery inventory"));assertEquals(new ApplyPosition(1,0),store.applied());assertFalse(store.admissionOpen());assertNull(rawUser(pool,new byte[]{7}));
        }
    }
    @Test void inventoryWorkspaceAdmissionPrecedesStockStorageScanAndNativeImageReads()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("admission-source"),false);var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("admission-native"))) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());Path image=root.resolve("admission.group");var manifest=NativeRangeImage.write(image,sourceStore,limits);var store=open(pool,directory,source.first);store.initializeConfiguration(sourceStore.configuration().data());store.applyNoop(new ApplyPosition(1,0));var reservations=new java.util.concurrent.atomic.AtomicInteger();
            var failure=assertThrows(IOException.class,()->new Adapter(store,pool,directory,source.local,root.resolve("admission-raft"),image,manifest.position(),(machine,file)->{
                // Stock storage.init would parse this malformed sidecar if called before admission.
                Files.writeString(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath(),"invalid digest");
                var admission=new NativeRangeImage.IoAdmission(){public void beforeBytes(int bytes){throw new AssertionError("Image read before inventory workspace admission");}public AutoCloseable reserveMetadata(int bytes)throws IOException{reservations.incrementAndGet();throw new IOException("Controlled inventory workspace denial");}};
                var slot=RatisRangeStateMachine.class.getDeclaredField("snapshotMetadataAdmission");slot.setAccessible(true);slot.set(machine,admission);
            }));
            assertEquals(1,reservations.get());assertTrue(causeContains(failure,"Controlled inventory workspace denial"));assertEquals(new ApplyPosition(1,0),store.applied());assertNull(rawUser(pool,new byte[]{7}));
        }
    }
    private static boolean causeContains(Throwable failure,String text){for(var current=failure;current!=null;current=current.getCause())if(current.getMessage()!=null && current.getMessage().contains(text))return true;return false;}

    NativeCurrentRangeRecovery currentRegistration(RatisTwoPoolWorkflowFixture source,NativeRangeImage.Manifest image)throws Exception {
        var directory=source.firstDirectory;var store=source.store(directory.ranges().getFirst().groupId());var rows=new TreeMap<String,byte[]>();try(var pin=store.captureForRecovery()){pin.visitState((key,value)->{if(NativeTopologyLearnerSchema.row(key))rows.put(HexFormat.of().formatHex(key),value);});}
        var binding=source.plan.catalogue();var cut=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()).readView(directory.domainId()).cut();var cfg=store.configuration();
        return new NativeCurrentRangeRecovery(new UUID(994,1),binding.groupId(),cut,directory,source.first,store.ownership().owner().groupId(),source.local,source.local,image.position(),image.sha256(),cfg.term(),cfg.index(),cfg.data(),image.topologyOrigin(),Map.of(source.first.columnIds().iterator().next(),"data"),Set.of(),rows);
    }
    @Test void interruptedCurrentImportWithPositiveConfigurationInventoriesTruthfulNullNativeMarker()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("partial-source"),false)) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());var sourceMachine=source.server.machines.get(RaftGroupId.valueOf(sourceStore.ownership().owner().groupId()));long next=sourceStore.applied().index()+1;
            sourceMachine.notifyConfigurationChanged(1,next,org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.parseFrom(sourceStore.configuration().data()));sourceMachine.notifyTermIndexUpdated(1,next);
            Path image=root.resolve("partial-current.group");var manifest=NativeRangeImage.write(image,sourceStore,limits);var registration=currentRegistration(source,manifest);Path path=root.resolve("partial-native"),raft=root.resolve("partial-raft");
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)) {
                var store=pool.openCurrentRangeRecovery(registration);assertNull(store.configuration());assertEquals("Imported CFG before final native marker",assertThrows(IOException.class,()->store.installSnapshot(image,limits,batches->{try{var cfg=store.configuration();if(cfg!=null && cfg.index()>0)throw new IOException("Imported CFG before final native marker");}catch(org.rocksdb.RocksDBException failed){throw new IOException(failed);}})).getMessage());
                assertTrue(store.pendingInstallation());assertNull(store.applied());assertTrue(store.configuration().index()>0);assertEquals(manifest.position(),store.pendingInstallationCut());field(pool,"loaded",RocksDBLoader.LoadedDb.class).db().get().flushWal(true);
            }
            for(int reopen=0;reopen<2;reopen++)try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)) {
                var store=pool.openCurrentRangeRecovery(registration);assertTrue(store.pendingInstallation());assertNull(store.applied());var cfg=store.configuration();assertTrue(cfg.index()>0);byte[] before=rawUser(pool,new byte[]{7});
                try(var adapter=new Adapter(store,pool,directory,source.local,raft,reopen==0?image:null,manifest.position(),(machine,file)->{})) {
                    assertNull(store.applied());assertEquals(TermIndex.INITIAL_VALUE,adapter.machine.getLastAppliedTermIndex());assertTrue(store.pendingInstallation());assertEquals(manifest.position(),store.pendingInstallationCut());assertEquals(cfg.index(),store.configuration().index());assertEquals(manifest.position().index(),adapter.machine.getLatestSnapshot().getIndex());assertFalse(store.currentRangeRecoveryImageInstalled());assertFalse(store.currentRangeRecoveryReady());assertFalse(store.admissionOpen());assertArrayEquals(before,rawUser(pool,new byte[]{7}));assertThrows(IOException.class,adapter.machine::reinitialize);assertTrue(adapter.machine.recoveryEligibilityGate().discoveryPending());
                }
            }
        }
    }
    @Test void retainedRecoveryFloorSurvivesCanonicalDirectoryRemovalWithoutLinkCopyOrRestore()throws Exception {
        for(boolean groupFloor:List.of(false,true))try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("retained-source-"+groupFloor),false)) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());Path image=root.resolve("retained-"+groupFloor+".group");var manifest=NativeRangeImage.write(image,sourceStore,limits);Path path=root.resolve("retained-native-"+groupFloor),raft=root.resolve("retained-raft-"+groupFloor);Path retained=raft.resolve("native-snapshot-recovery");if(groupFloor)retained=retained.resolve("group-floor");Path expected=retained.resolve(SimpleStateMachineStorage.getSnapshotFileName(manifest.position().term(),manifest.position().index()));Path visible=groupFloor?raft.resolve("native-snapshot-recovery").resolve(expected.getFileName()):expected;
            for(int reopen=0;reopen<2;reopen++)try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)) {
                var store=open(pool,directory,source.first);if(reopen==0){store.initializeConfiguration(sourceStore.configuration().data());store.applyNoop(new ApplyPosition(1,0));}
                try(var adapter=new Adapter(store,pool,directory,source.local,raft,reopen==0?image:null,manifest.position(),(machine,file)->{
                    Files.createDirectories(expected.getParent());var md5=MD5FileUtil.getDigestFileForFile(file.toFile()).toPath();Files.move(file,expected);Files.move(md5,MD5FileUtil.getDigestFileForFile(expected.toFile()).toPath());if(groupFloor){Files.createLink(visible,expected);Files.createLink(MD5FileUtil.getDigestFileForFile(visible.toFile()).toPath(),MD5FileUtil.getDigestFileForFile(expected.toFile()).toPath());}Files.delete(file.getParent());SessionJournalFiles.force(expected);SessionJournalFiles.force(expected.getParent());SessionJournalFiles.force(raft);
                })) {
                    assertEquals(visible,adapter.machine.getLatestSnapshot().getFiles().getFirst().getPath());assertEquals(manifest.position().index(),adapter.machine.getLatestSnapshot().getIndex());assertEquals(new ApplyPosition(1,0),store.applied());assertNull(rawUser(pool,new byte[]{7}));assertFalse(store.admissionOpen());assertTrue(adapter.machine.recoveryEligibilityGate().discoveryPending());assertFalse(Files.exists(adapter.machine.getStateMachineStorage().getSnapshotFile(manifest.position().term(),manifest.position().index()).toPath()));assertArrayEquals(Files.readAllBytes(image),Files.readAllBytes(expected));
                }
            }
        }
    }
    @Test void sameIndexConflictingGenuineGroupHashesAcrossInventoryRootsAreRejected()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("conflict-source"),false);var alternative=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("alternative-native"));var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("conflict-native"))) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());Path first=root.resolve("first-conflict.group"),second=root.resolve("second-conflict.group");var manifest=NativeRangeImage.write(first,sourceStore,limits);var other=open(alternative,directory,source.first);other.initializeConfiguration(sourceStore.configuration().data());for(long i=0;i<manifest.position().index();i++)other.applyNoop(new ApplyPosition(1,i));
            var command=new DistributedCommand(other.ownership().owner().groupId(),other.ownership().owner().epoch(),other.schemaDigest(),new OperationId(new UUID(994,2),1,0),List.of(new Mutation(Mutation.Kind.PUT,source.first.columnIds().iterator().next(),new byte[]{7},new byte[]{12})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(other.apply(manifest.position(),command)).status());var changed=NativeRangeImage.write(second,other,limits);assertEquals(manifest.position(),changed.position());assertFalse(Arrays.equals(manifest.sha256(),changed.sha256()));
            var store=open(pool,directory,source.first);store.initializeConfiguration(sourceStore.configuration().data());store.applyNoop(new ApplyPosition(1,0));Path raft=root.resolve("conflict-raft");
            var failure=assertThrows(IOException.class,()->new Adapter(store,pool,directory,source.local,raft,first,manifest.position(),(machine,file)->{var retained=Files.createDirectories(raft.resolve("native-snapshot-recovery")).resolve(file.getFileName());Files.copy(second,retained);MD5FileUtil.saveMD5File(retained.toFile(),MD5FileUtil.computeMd5ForFile(retained.toFile()));}));assertTrue(causeContains(failure,"Discovery floors conflict"));assertEquals(new ApplyPosition(1,0),store.applied());assertNull(rawUser(pool,new byte[]{7}));
        }
    }
    @Test void completedCurrentDiscoveryBlocksOrdinaryQueriesBeforeBodyAccessAndSnapshotPin()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("closed-query-source"),false);var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("closed-query-native"))) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());Path image=root.resolve("completed-current.group");var manifest=NativeRangeImage.write(image,sourceStore,limits);var registration=currentRegistration(source,manifest);var store=pool.openCurrentRangeRecovery(registration);store.installSnapshot(image,limits);store.completeCurrentRangeRecovery(registration,manifest.position(),store.configuration());store.fence();try(var pin=store.captureForRecovery()){assertArrayEquals(new byte[]{11},pin.get(source.first.columnIds().iterator().next(),new byte[]{7}));}
            try(var adapter=new Adapter(store,pool,directory,source.local,root.resolve("closed-query-raft"),image,manifest.position(),(machine,file)->{})) {
                var touches=new java.util.concurrent.atomic.AtomicInteger();byte[] state=ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]);Message request=()->{touches.incrementAndGet();return org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(state);};int pins=store.activeSnapshotPins();
                assertFalse(adapter.machine.isRecoveryOwnerQuery(request));assertEquals(1,touches.get(),"Negative-owner classification inspects the bounded frame");touches.set(0);assertThrows(java.util.concurrent.ExecutionException.class,()->adapter.machine.queryStale(request,0).get(5,java.util.concurrent.TimeUnit.SECONDS));assertThrows(java.util.concurrent.ExecutionException.class,()->adapter.machine.query(request).get(5,java.util.concurrent.TimeUnit.SECONDS));assertThrows(java.util.concurrent.ExecutionException.class,()->adapter.machine.queryLinearizable(request).get(5,java.util.concurrent.TimeUnit.SECONDS));assertEquals(0,touches.get());assertEquals(pins,store.activeSnapshotPins());assertTrue(store.currentRangeRecoveryReady());assertFalse(store.admissionOpen());assertEquals(manifest.position(),store.applied());assertThrows(IllegalStateException.class,()->adapter.machine.notifyTermIndexUpdated(1,manifest.position().index()+1));assertEquals(manifest.position(),store.applied());
            }
        }
    }

    NativeTopologySeed sealOriginal(DistributedStoragePool pool,ReplicatedRangeStore store,TopologyChange.Definition split,NativeTopologyFreeze sourceFreeze)throws Exception {
        store.activate();assertTrue(store.admissionOpen());store.enableCdc(split.parents().getFirst().rangeId());var freeze=store.applyTopologyFreeze(sourceFreeze.position(),split,sourceFreeze.admissionDigest());assertEquals(sourceFreeze,freeze);
        var authority=TopologyChange.State.begin(split.header()).beginFencing().frozen(freeze.proof(split.parents().getFirst().replicas().getFirst().replicaId()));
        return pool.seedTopology(split,authority,List.of(store),split.parents().getFirst().replicas().getFirst());
    }
    NativeMetadataRecoveryRegistration metadataRegistration(RatisTwoPoolWorkflowFixture source,NativeRangeImage.Manifest image,NativeCurrentRangeRecovery predecessor)throws Exception {
        var directory=source.firstDirectory;var store=source.store(directory.ranges().getFirst().groupId());var binding=source.plan.catalogue();var cfg=store.configuration();var cut=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()).readView(directory.domainId()).cut();
        return new NativeMetadataRecoveryRegistration(new UUID(996,1),binding.groupId(),cut,directory,store.topologyFreeze(),source.local,source.local,cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of(),predecessor==null?null:NativeMetadataRecoveryRegistration.CurrentPredecessor.of(predecessor));
    }
    @Test void originalSealedMetaFloorAheadOfActualNativePrefixRemainsInertAcrossColdReopen()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("original-meta-source"),false)) {
            var directory=source.firstDirectory;var range=directory.ranges().getFirst();var sourceStore=source.store(range.groupId());Path initial=root.resolve("original-before-freeze.group");var original=NativeRangeImage.write(initial,sourceStore,limits);var split=source.split();var freeze=sourceStore.topologyFreeze();source.server.machines.get(RaftGroupId.valueOf(range.groupId())).notifyTermIndexUpdated(1,sourceStore.applied().index()+1);Path metadata=root.resolve("original-ahead.meta");var image=NativeRangeImage.writeMetadata(metadata,sourceStore,limits);assertTrue(image.position().index()>freeze.position().index());
            Path path=root.resolve("original-meta-native"),raft=root.resolve("original-meta-raft");
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)){var store=open(pool,directory,source.first);store.fence();store.installSnapshot(initial,limits);assertEquals(original.position(),store.applied());sealOriginal(pool,store,split,freeze);assertTrue(store.topologyRetired());}
            for(int reopen=0;reopen<2;reopen++)try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)) {
                var store=open(pool,directory,source.first);assertTrue(store.topologyRetired());assertNull(store.metadataRecoveryRegistration());byte[] before=rawUser(pool,new byte[]{7});
                try(var adapter=new Adapter(store,pool,directory,source.local,raft,reopen==0?metadata:null,image.position(),(machine,file)->{})) {
                    assertEquals(freeze.position(),store.applied());assertEquals(TermIndex.valueOf(freeze.position().term(),freeze.position().index()),adapter.machine.getLastAppliedTermIndex());assertEquals(image.position().index(),adapter.machine.getLatestSnapshot().getIndex());assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(adapter.machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));assertTrue(adapter.machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());assertArrayEquals(before,rawUser(pool,new byte[]{7}));assertThrows(IOException.class,adapter.machine::reinitialize);
                }
            }
        }
    }
    @Test void registeredMetaWithCurrentPredecessorInventoriesGroupAndActualMetaAheadOfAOrNull()throws Exception {
        for(boolean installed:List.of(false,true))try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("registered-meta-source-"+installed),false)) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());Path group=root.resolve("registered-predecessor-"+installed+".group");var old=NativeRangeImage.write(group,sourceStore,limits);var current=currentRegistration(source,old);source.split();Path meta=root.resolve("registered-ahead-"+installed+".meta");var incoming=NativeRangeImage.writeMetadata(meta,sourceStore,limits);var registration=metadataRegistration(source,incoming,current);Path path=root.resolve("registered-meta-native-"+installed),raft=root.resolve("registered-meta-raft-"+installed);
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)){var store=pool.openCurrentRangeRecovery(current);if(installed){store.installSnapshot(group,limits);store.completeCurrentRangeRecovery(current,old.position(),store.configuration());}assertSame(store,pool.openMetadataRecovery(registration));assertNull(store.currentRangeRecoveryRegistration());}
            for(int reopen=0;reopen<2;reopen++)try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)) {
                var store=pool.openMetadataRecovery(registration);byte[] before=rawUser(pool,new byte[]{7});var actual=installed?old.position():null;
                try(var adapter=new Adapter(store,pool,directory,source.local,raft,reopen==0?group:null,old.position(),(machine,file)->{var retained=Files.createDirectories(raft.resolve("native-snapshot-recovery")).resolve(SimpleStateMachineStorage.getSnapshotFileName(incoming.position().term(),incoming.position().index()));Files.copy(meta,retained);MD5FileUtil.saveMD5File(retained.toFile(),MD5FileUtil.computeMd5ForFile(retained.toFile()));})) {
                    assertEquals(actual,store.applied());assertEquals(actual==null?TermIndex.INITIAL_VALUE:TermIndex.valueOf(actual.term(),actual.index()),adapter.machine.getLastAppliedTermIndex());assertEquals(incoming.position().index(),adapter.machine.getLatestSnapshot().getIndex());assertFalse(store.metadataRecoveryImageInstalled());assertTrue(store.metadataRecoveryPending());assertTrue(adapter.machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());assertArrayEquals(before,rawUser(pool,new byte[]{7}));assertThrows(IOException.class,adapter.machine::reinitialize);
                }
            }
        }
    }
    @Test void equalCutOriginalGroupAndMetaProjectionsPreferMetaAndRejectCorruptOrForeignFiles()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("pair-source"),false)) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());Path original=root.resolve("pair-original.group");NativeRangeImage.write(original,sourceStore,limits);var split=source.split();var freeze=sourceStore.topologyFreeze();Path path=root.resolve("pair-native"),raft=root.resolve("pair-raft"),group=root.resolve("pair-frozen.group"),meta=root.resolve("pair-frozen.meta");NativeRangeImage.Manifest metadata;
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)) {
                var store=open(pool,directory,source.first);store.fence();store.installSnapshot(original,limits);store.activate();assertTrue(store.admissionOpen());store.enableCdc(split.parents().getFirst().rangeId());store.applyTopologyFreeze(freeze.position(),split,freeze.admissionDigest());var groupManifest=NativeRangeImage.write(group,store,limits);var authority=TopologyChange.State.begin(split.header()).beginFencing().frozen(freeze.proof(source.local.replicaId()));pool.seedTopology(split,authority,List.of(store),source.local);metadata=NativeRangeImage.writeMetadata(meta,store,limits);assertEquals(groupManifest.position(),metadata.position());assertFalse(Arrays.equals(groupManifest.sha256(),metadata.sha256()));
            }
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)) {var store=open(pool,directory,source.first);try(var adapter=new Adapter(store,pool,directory,source.local,raft,group,metadata.position(),(machine,file)->{var target=Files.createDirectories(raft.resolve("native-snapshot-recovery")).resolve(file.getFileName());Files.copy(meta,target);MD5FileUtil.saveMD5File(target.toFile(),MD5FileUtil.computeMd5ForFile(target.toFile()));})){assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(adapter.machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));assertEquals(metadata.position(),store.applied());assertTrue(adapter.machine.recoveryEligibilityGate().discoveryPending());}}
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)) {var store=open(pool,directory,source.first);var retained=raft.resolve("native-snapshot-recovery").resolve(SimpleStateMachineStorage.getSnapshotFileName(metadata.position().term(),metadata.position().index()));byte[] bytes=Files.readAllBytes(retained);bytes[bytes.length-1]^=1;Files.write(retained,bytes);assertThrows(IOException.class,()->new Adapter(store,pool,directory,source.local,raft,null,metadata.position(),(machine,file)->{}));assertEquals(metadata.position(),store.applied());}
            Path foreign=root.resolve("foreign-child.group");var child=source.store(split.targets().getFirst().groupId());var foreignImage=NativeRangeImage.write(foreign,child,limits);
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("foreign-meta-native"))) {var current=currentRegistrationFromInitial(source,directory,original);var registration=metadataRegistration(source,NativeRangeImage.inspectMetadata(meta,limits),current);var store=pool.openCurrentRangeRecovery(current);pool.openMetadataRecovery(registration);assertThrows(IOException.class,()->new Adapter(store,pool,directory,source.local,root.resolve("foreign-meta-raft"),foreign,foreignImage.position(),(machine,file)->{}));assertNull(store.applied());assertFalse(store.metadataRecoveryImageInstalled());}
        }
    }
    NativeCurrentRangeRecovery currentRegistrationFromInitial(RatisTwoPoolWorkflowFixture source,RangeDirectory directory,Path group)throws Exception {
        var manifest=NativeRangeImage.inspect(group,limits);var rows=new TreeMap<String,byte[]>();var cfg=new ReplicatedRangeStore.Configuration[1];NativeRangeImage.visit(group,limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE){if(key.length==17 && key[16]==1)cfg[0]=ReplicatedRangeStore.Configuration.decode(value);if(NativeTopologyLearnerSchema.row(key))rows.put(HexFormat.of().formatHex(key),value);}});var binding=source.plan.catalogue();var cut=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()).readView(directory.domainId()).cut();return new NativeCurrentRangeRecovery(new UUID(996,2),binding.groupId(),cut,directory,source.first,manifest.owner().groupId(),source.local,source.local,manifest.position(),manifest.sha256(),cfg[0].term(),cfg[0].index(),cfg[0].data(),manifest.topologyOrigin(),Map.of(source.first.columnIds().iterator().next(),"data"),Set.of(),rows);
    }

    @Test void coveredOrdinaryGroupUnderNullAnchorMetaRegistrationRetainsExistingInventoryContract()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("covered-meta-source"),false)) {
            var directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());Path group=root.resolve("covered-original.group");var old=NativeRangeImage.write(group,sourceStore,limits);source.split();Path meta=root.resolve("covered-source.meta");var image=NativeRangeImage.writeMetadata(meta,sourceStore,limits);var registration=metadataRegistration(source,image,null);Path path=root.resolve("covered-meta-native"),raft=root.resolve("covered-meta-raft");
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)){var store=open(pool,directory,source.first);store.fence();store.installSnapshot(group,limits);assertSame(store,pool.openMetadataRecovery(registration));assertNull(store.topologyFreeze());}
            for(int reopen=0;reopen<2;reopen++)try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)){var store=pool.openMetadataRecovery(registration);try(var adapter=new Adapter(store,pool,directory,source.local,raft,reopen==0?group:null,old.position(),(machine,file)->{})){assertEquals(old.position(),store.applied());assertEquals(old.position().index(),adapter.machine.getLatestSnapshot().getIndex());assertTrue(store.metadataRecoveryPending());assertFalse(store.metadataRecoveryImageInstalled());assertTrue(adapter.machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());}}
        }
    }

    @Test void historicalGroupConfigurationMayPrecedeFinalSealedMetadataCohort()throws Exception {
        // Controlled native cohort transition: both FREEZE proofs come from actual JNI owners.
        // This does not claim a network relocation or stale-Ratis catch-up workflow.
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("cohort-source"),false);var remotePool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("cohort-remote"))) {
            var oldDirectory=source.firstDirectory;var oldRange=oldDirectory.ranges().getFirst();var sourceStore=source.store(oldRange.groupId());Path group=root.resolve("cohort-single.group");var original=NativeRangeImage.write(group,sourceStore,limits);var current=currentRegistration(source,original);
            var remote=new ReplicaDescriptor(new UUID(997,1),new UUID(997,2),new UUID(997,3),"cohort-remote","127.0.0.1:32902",ReplicaDescriptor.Role.VOTER);var replicas=List.of(source.local,remote);
            var parent=new RangeDescriptor(oldRange.rangeId(),oldRange.groupId(),oldRange.rangeEpoch(),oldRange.configurationVersion()+1,oldRange.lowerInclusive(),oldRange.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,replicas);
            var finalSource=new RangeDirectory(oldDirectory.clusterId(),oldDirectory.databaseId(),oldDirectory.domainId(),oldDirectory.schemaEpoch(),oldDirectory.schemaDigest(),oldDirectory.generation()+1,oldDirectory.columns(),oldDirectory.routingPrefixBytes(),List.of(parent));
            var left=new RangeDescriptor(new UUID(997,4),new UUID(997,5),1,parent.configurationVersion(),null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,replicas);var right=new RangeDescriptor(new UUID(997,6),new UUID(997,7),1,parent.configurationVersion(),new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,replicas);
            var target=new RangeDirectory(finalSource.clusterId(),finalSource.databaseId(),finalSource.domainId(),finalSource.schemaEpoch(),finalSource.schemaDigest(),finalSource.generation()+1,finalSource.columns(),finalSource.routingPrefixBytes(),List.of(left,right));var split=new TopologyChange.Definition(new UUID(997,8),finalSource,target);
            var configuration=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder().addPeers(RaftPeer.newBuilder().setId(source.local.peerId()).setAddress(source.local.address()).build().getRaftPeerProto()).addPeers(RaftPeer.newBuilder().setId(remote.peerId()).setAddress(remote.address()).build().getRaftPeerProto()).build();
            var configured=new ApplyPosition(1,original.position().index()+1);sourceStore.applyNoop(configured,configuration.toByteArray());
            var remoteStore=open(remotePool,oldDirectory,source.first);remoteStore.fence();remoteStore.installSnapshot(group,limits);remoteStore.activate();remoteStore.applyNoop(configured,configuration.toByteArray());
            var frozenState=TopologyChange.State.begin(split.header()).beginFencing();byte[] encoded=frozenState.encode();byte[] admission=OperatorUpload.sha256(java.nio.ByteBuffer.allocate(16+encoded.length).putLong(current.catalogue().getMostSignificantBits()).putLong(current.catalogue().getLeastSignificantBits()).put(encoded).array());var frozenCut=new ApplyPosition(1,configured.index()+1);
            var localFreeze=sourceStore.applyTopologyFreeze(frozenCut,split,admission);var remoteFreeze=remoteStore.applyTopologyFreeze(frozenCut,split,admission);assertEquals(localFreeze,remoteFreeze);var authority=frozenState.frozen(localFreeze.proof(source.local.replicaId())).frozen(remoteFreeze.proof(remote.replicaId()));
            source.node.nativePool(source.fast).seedTopology(split,authority,List.of(sourceStore),source.local);Path meta=root.resolve("cohort-final.meta");var image=NativeRangeImage.writeMetadata(meta,sourceStore,limits);var cfg=sourceStore.configuration();assertEquals(configured.index(),cfg.index());assertEquals(2,org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.parseFrom(cfg.data()).getPeersCount());
            var registration=new NativeMetadataRecoveryRegistration(new UUID(997,9),current.catalogue(),current.catalogueCut(),finalSource,localFreeze,source.local,source.local,cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of(),NativeMetadataRecoveryRegistration.CurrentPredecessor.of(current));Path path=root.resolve("cohort-receiver"),raft=root.resolve("cohort-raft");
            try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)){var store=pool.openCurrentRangeRecovery(current);store.installSnapshot(group,limits);store.completeCurrentRangeRecovery(current,original.position(),store.configuration());store.applyNoop(configured,configuration.toByteArray());pool.openMetadataRecovery(registration);}
            for(int reopen=0;reopen<2;reopen++)try(var pool=new RatisTopologyRecoveryAnchorTest().pool(path)){var store=pool.openMetadataRecovery(registration);try(var adapter=new Adapter(store,pool,finalSource,source.local,raft,reopen==0?group:null,original.position(),(machine,file)->{var retained=Files.createDirectories(raft.resolve("native-snapshot-recovery")).resolve(SimpleStateMachineStorage.getSnapshotFileName(image.position().term(),image.position().index()));Files.copy(meta,retained);MD5FileUtil.saveMD5File(retained.toFile(),MD5FileUtil.computeMd5ForFile(retained.toFile()));})){assertEquals(configured,store.applied());assertEquals(configured.index(),store.configuration().index());assertEquals(2,org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.parseFrom(store.configuration().data()).getPeersCount());assertEquals(image.position().index(),adapter.machine.getLatestSnapshot().getIndex());assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(adapter.machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));assertTrue(adapter.machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.metadataRecoveryImageInstalled());assertTrue(store.metadataRecoveryPending());assertFalse(store.admissionOpen());}}
        }
    }

}
