package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Genuine JNI GROUP images and actual socket-free updater/log reload; no genesis or quorum inference. */
class RatisLocalCurrentDiscoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final NativeRangeImage.Limits IMAGES=new NativeRangeImage.Limits(4<<20,32<<20,8);
    enum Mode { ABSENT,BEHIND,AHEAD,READY,BORN,EMPTY,UNREADY }
    final class Fixture implements AutoCloseable {
        final RatisTwoPoolWorkflowFixture source;final DistributedStoragePool pool;final RatisNodePlan plan;final RangeDirectory directory;
        final DistributedSchema schema;final ReplicatedRangeStore store;final NativeCurrentRangeRecovery registration;final NativeRangeImage.Manifest floor;final Path image,logs;
        final ApplyPosition graphCut,before;final int expected;final RatisNodeResourceBudget resources;final RatisDiskRunway runway;
        RatisRangeStateMachine machine;RaftServer server;RatisCommittedApplyDispatcher dispatcher;DistributedCommand advancedCommand;
        Fixture(String name,Mode mode)throws Exception {
            source=new RatisTwoPoolWorkflowFixture(root.resolve(name+"-source"),false);pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve(name+"-native"));var helper=new RatisDiscoveryInventoryTest();helper.root=root;schema=source.first;image=root.resolve(name+".group");logs=root.resolve(name+"-logs");
            NativeCurrentRangeRecovery recipe=null;NativeRangeImage.Manifest manifest=null;int value=11;
            if(mode==Mode.BORN) {
                Path parentImage=root.resolve(name+"-parent.group");NativeRangeImage.write(parentImage,source.store(source.firstDirectory.ranges().getFirst().groupId()),IMAGES);var split=source.split();directory=split.targetDirectory();var parent=helper.open(pool,split.sourceDirectory(),schema);parent.fence();parent.installSnapshot(parentImage,IMAGES);parent.activate();parent.enableCdc(split.parents().getFirst().rangeId());var freeze=source.store(split.parents().getFirst().groupId()).topologyFreeze();parent.applyTopologyFreeze(freeze.position(),split,freeze.admissionDigest());var authority=TopologyChange.State.begin(split.header()).beginFencing().frozen(freeze.proof(source.local.replicaId()));var seed=pool.seedTopology(split,authority,List.of(parent),source.local);var leaf=directory.ranges().getFirst();store=pool.reopenTopologyRange(seed.birth(leaf.groupId()),schema);store.initializeConfiguration(source.store(leaf.groupId()).configuration().data());manifest=NativeRangeImage.write(image,source.store(leaf.groupId()),IMAGES);assertNull(store.applied());assertTrue(store.topologyBirthPending());
            } else {
                directory=source.firstDirectory;var sourceStore=source.store(directory.ranges().getFirst().groupId());manifest=mode==Mode.EMPTY?null:NativeRangeImage.write(image,sourceStore,IMAGES);
                if(mode==Mode.READY || mode==Mode.UNREADY) {
                    recipe=helper.currentRegistration(source,manifest);store=pool.openCurrentRangeRecovery(recipe);
                    if(mode==Mode.READY){store.installSnapshot(image,IMAGES);store.completeCurrentRangeRecovery(recipe,manifest.position(),store.configuration());try(var other=new RatisTopologyRecoveryAnchorTest().pool(root.resolve(name+"-checkpoint-native"))){var own=other.openCurrentRangeRecovery(recipe);own.installSnapshot(image,IMAGES);own.completeCurrentRangeRecovery(recipe,manifest.position(),own.configuration());own.apply(new ApplyPosition(1,manifest.position().index()+1),put(own,12));Path checkpoint=root.resolve(name+"-own.group");manifest=NativeRangeImage.write(checkpoint,own,IMAGES);Files.copy(checkpoint,image,StandardCopyOption.REPLACE_EXISTING);}value=12;}
                }else {store=helper.open(pool,directory,schema);store.initializeConfiguration(sourceStore.configuration().data());if(mode==Mode.BEHIND)store.applyNoop(new ApplyPosition(1,0));if(mode==Mode.AHEAD){store.fence();store.installSnapshot(image,IMAGES);store.activate();advancedCommand=put(store,13);store.apply(new ApplyPosition(1,store.applied().index()+1),advancedCommand);value=13;}}
            }
            registration=recipe;floor=manifest;expected=value;before=store.applied();var domain=new RatisNodePlan.Domain(directory,Map.of(schema.columnIds().iterator().next(),"data"),Set.of());plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),source.plan.catalogue(),List.of(),List.of(),Map.of(),4,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(source.plan.catalogue(),List.of()));
            var binding=source.plan.catalogue();graphCut=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()).readView(directory.domainId()).cut();var descriptor=directory.ranges().getFirst();var groupDirectory=logs.resolve(descriptor.groupId().toString());Files.createDirectories(groupDirectory);
            try(var storage=RaftStorage.newBuilder().setDirectory(groupDirectory.toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(SizeInBytes.ONE_MB).build()){storage.initialize();var snapshots=new SimpleStateMachineStorage();snapshots.init(storage);if(floor!=null){var target=snapshots.getSnapshotFile(floor.position().term(),floor.position().index()).toPath();Files.createDirectories(target.getParent());Files.copy(image,target);MD5FileUtil.saveMD5File(target.toFile(),MD5FileUtil.computeMd5ForFile(target.toFile()));SessionJournalFiles.force(target);SessionJournalFiles.force(target.getParent());}}
            var defaults=RatisNodeRuntime.ResourceLimits.defaults();resources=new RatisNodeResourceBudget(defaults.node(),Map.of(RatisPoolFixtures.ID,defaults.pool()));var clock=new AtomicLong(1);var policy=new RatisDiskRunway.Policy(16L<<20,8L<<20,24L<<20,8L<<20,8L<<20,1L<<20,java.time.Duration.ofMinutes(1),16,128,16);runway=new RatisDiskRunway(policy,clock::get);var free=Map.of(1L,new RatisDiskRunway.Filesystem(1,1L<<30));long census=runway.beginCensus();runway.publish(new RatisDiskRunway.Census(1,census,free,Map.of(RatisPoolFixtures.ID,Set.of(1L)),Set.of(1L),0));long observed=runway.beginFreeObservation();runway.observeFree(1,observed,free);try{open();}catch(Exception|Error failure){try{close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}throw failure;}
        }
        DistributedCommand put(ReplicatedRangeStore store,int value){return new DistributedCommand(store.ownership().owner().groupId(),store.ownership().owner().epoch(),store.schemaDigest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,schema.columnIds().iterator().next(),new byte[]{7},new byte[]{(byte)value})));}
        void open()throws Exception {
            var descriptor=directory.ranges().getFirst();var group=plan.localGroups().stream().filter(g->g.getGroupId().getUuid().equals(descriptor.groupId())).findFirst().orElseThrow();machine=new RatisRangeStateMachine(store,group,IMAGES);machine.topologyAuthority((header,stage)->{throw new IOException("No remote authority in local CURRENT permit");},directory,source.local,pool);machine.nodeResources(resources,RatisPoolFixtures.ID);var logLimits=RatisLogResourceBudget.Limits.defaults();machine.logResources(new RatisLogResourceBudget(group.getGroupId().getUuid(),new RatisLogResourceBudget.Shared(logLimits),new RatisLogResourceBudget.Shared(logLimits),logLimits,(bytes,progress)->()->{}));machine.diskRunway(runway,RatisPoolFixtures.ID);var policy=RatisCommittedApplyDispatcher.Policy.defaults();dispatcher=new RatisCommittedApplyDispatcher(policy.node(),Map.of(RatisPoolFixtures.ID,policy.pool()));machine.committedApply(dispatcher,RatisPoolFixtures.ID,NativeSstImportManifest.MAX_FILE_BYTES+4096);machine.deferRecoveryDiscovery();server=RatisLocalMetadataDiscoveryTest.noSocketServer(plan,logs,Map.of(group.getGroupId(),machine));server.start();
        }
        void stop()throws Exception {if(server!=null){machine.beginShutdown();server.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));server=null;}if(dispatcher!=null){dispatcher.awaitDrained(java.time.Duration.ofSeconds(5));dispatcher.close();dispatcher=null;}}
        @Override public void close()throws Exception{try{stop();}finally{pool.close();source.close();}}
    }
    @Test void genuineCurrentRestorationAndExistingNativeBaselineStayFencedUntilActualReloadSettlement()throws Exception {
        for(var mode:List.of(Mode.ABSENT,Mode.BEHIND,Mode.AHEAD,Mode.READY,Mode.BORN))try(var f=new Fixture("actual-"+mode,mode)) {
            assertEquals(f.before,f.store.applied());assertFalse(f.store.admissionOpen());if(f.registration!=null){var old=new ApplyPosition(f.registration.catalogueCut().term(),f.registration.catalogueCut().index()-1);assertThrows(IOException.class,()->f.machine.prepareLocalCurrentDiscovery(old,f.directory));assertEquals(f.before,f.store.applied());}var candidate=f.machine.prepareLocalCurrentDiscovery(f.graphCut,f.directory);var a=f.store.applied();assertNotNull(a);assertEquals(candidate.cut(),a);assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());assertFalse(f.store.admissionOpen());assertThrows(IllegalStateException.class,()->f.store.readCurrentPoint(f.schema.columnIds().iterator().next(),new byte[]{7}));assertThrows(IOException.class,f.machine::reinitialize);
            f.machine.reloadLocalCurrentDiscovery(candidate).get(10,TimeUnit.SECONDS);assertEquals(a,f.store.applied());assertFalse(f.store.admissionOpen());assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());assertEquals(a.index(),f.server.getDivision(f.machine.getGroupId()).getRaftLog().getSnapshotIndex());assertThrows(IOException.class,()->f.machine.settleLocalCurrentDiscovery(candidate,publication->{throw new IOException("Node closing fence");}));assertFalse(f.store.admissionOpen());assertSame(candidate,f.machine.prepareLocalCurrentDiscovery(new ApplyPosition(f.graphCut.term(),f.graphCut.index()+1),f.directory));assertEquals(LifeCycle.State.RUNNING,f.machine.getLifeCycleState());assertTrue(f.machine.settleLocalCurrentDiscovery(candidate,publication->publication.publish()));assertTrue(f.store.admissionOpen());assertArrayEquals(new byte[]{(byte)f.expected},f.store.readCurrentPoint(f.schema.columnIds().iterator().next(),new byte[]{7}));assertEquals(f.registration,f.store.currentRangeRecoveryRegistration());assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.resources.stats().protectedLane().bytes());assertEquals(0,f.runway.stats().owners());
        }
    }
    @Test void retainedGroupFloorCoversGenuineNativeLogPrefixWithoutWritingAnotherUserImage()throws Exception {
        try(var f=new Fixture("covered",Mode.AHEAD)) {
            var division=f.server.getDivision(f.machine.getGroupId());var log=division.getRaftLog();var a=f.store.applied();var s=f.floor.position();assertTrue(a.index()>s.index());
            var before=f.machine.getLatestSnapshot().getFiles().getFirst().getPath();byte[] bytes=Files.readAllBytes(before);var attributes=Files.readAttributes(before,java.nio.file.attribute.BasicFileAttributes.class);
            var next=f.put(f.store,14);long tail=a.index()+1;
            // These are the actual native-applied command and one new committed command. The
            // covered entry must never reenter native apply; only the genuine next entry may run.
            for(var completion:log.append(List.of(logData(a,f.advancedCommand),logData(new ApplyPosition(a.term(),tail),next))))completion.get(5,TimeUnit.SECONDS);
            assertTrue(log.updateCommitIndex(tail,a.term(),false));
            var candidate=f.machine.prepareLocalCurrentDiscovery(f.graphCut,f.directory);assertEquals(a,candidate.cut());assertEquals(LifeCycle.State.RUNNING,f.machine.getLifeCycleState());
            assertEquals(s.index(),f.machine.getLatestSnapshot().getIndex());assertFalse(Files.exists(before.resolveSibling(SimpleStateMachineStorage.getSnapshotFileName(a.term(),a.index()))));
            var canceled=f.machine.reloadLocalCurrentDiscovery(candidate);canceled.cancel(false);f.machine.reloadLocalCurrentDiscovery(candidate).get(10,TimeUnit.SECONDS);
            assertEquals(a,f.store.applied());assertEquals(a.index(),f.machine.getLastAppliedTermIndex().getIndex());assertEquals(s.index(),log.getSnapshotIndex());assertFalse(f.store.admissionOpen());assertTrue(f.store.activeSnapshotPins()>0);
            assertThrows(IOException.class,()->f.machine.settleLocalCurrentDiscovery(candidate,publication->{throw new IOException("Transient Node publication refusal");}));
            assertTrue(f.store.activeSnapshotPins()>0);assertSame(candidate,f.machine.prepareLocalCurrentDiscovery(new ApplyPosition(f.graphCut.term(),f.graphCut.index()+1),f.directory));
            assertEquals(s.index(),f.machine.getLatestSnapshot().getIndex());assertArrayEquals(bytes,Files.readAllBytes(before));assertEquals(attributes.lastModifiedTime(),Files.getLastModifiedTime(before));
            assertTrue(f.machine.settleLocalCurrentDiscovery(candidate,publication->publication.publish()));
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while((f.store.applied()==null || f.store.applied().index()<tail || f.resources.stats().protectedLane().bytes()!=0) && System.nanoTime()<deadline)Thread.sleep(5);
            assertEquals(new ApplyPosition(a.term(),tail),f.store.applied());assertArrayEquals(new byte[]{14},f.store.readCurrentPoint(f.schema.columnIds().iterator().next(),new byte[]{7}));
            assertNotNull(f.store.operationResult(next.operationId(),next.receiptDigest()));assertEquals(s.index(),log.getSnapshotIndex());assertEquals(s.index(),f.machine.getLatestSnapshot().getIndex());assertArrayEquals(bytes,Files.readAllBytes(before));
            assertFalse(Files.exists(before.resolveSibling(SimpleStateMachineStorage.getSnapshotFileName(a.term(),a.index()))));assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.resources.stats().protectedLane().bytes());
        }
    }
    @Test void retainedFloorCoversPositiveNativeAndColdRaftConfigurationWithoutUserCheckpoint()throws Exception {
        try(var f=new Fixture("covered-positive-cfg",Mode.AHEAD)) {
            var dataCut=f.store.applied();var s=f.floor.position();byte[] configuration=f.store.configuration().data();
            f.stop();var a=new ApplyPosition(dataCut.term(),dataCut.index()+1);f.store.applyNoop(a,configuration);
            assertEquals(a,f.store.applied());assertEquals(a.term(),f.store.configuration().term());assertEquals(a.index(),f.store.configuration().index());f.open();
            var next=f.put(f.store,15);long tail=a.index()+1;var full=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.parseFrom(configuration);
            var entries=List.of(logData(dataCut,f.advancedCommand),org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(a.term()).setIndex(a.index()).setConfigurationEntry(full).build(),logData(new ApplyPosition(a.term(),tail),next),org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(a.term()).setIndex(tail+1).setMetadataEntry(org.apache.ratis.proto.RaftProtos.MetadataProto.newBuilder().setCommitIndex(tail)).build());
            for(var completion:f.server.getDivision(f.machine.getGroupId()).getRaftLog().append(entries))completion.get(5,TimeUnit.SECONDS);
            // Load the genuine CFG and persisted committed prefix through actual ServerState/log open.
            f.stop();f.open();var division=f.server.getDivision(f.machine.getGroupId());var log=division.getRaftLog();
            assertEquals(a,f.store.applied());assertEquals(a.index(),division.getRaftConf().getLogEntryIndex());assertTrue(log.getLastCommittedIndex()>=tail);
            assertEquals(full,org.apache.ratis.server.raftlog.LogProtoUtils.toRaftConfigurationProtoBuilder(division.getRaftConf()).build());
            var before=f.machine.getLatestSnapshot().getFiles().getFirst().getPath();byte[] bytes=Files.readAllBytes(before);var unwanted=before.resolveSibling(SimpleStateMachineStorage.getSnapshotFileName(a.term(),a.index()));
            var candidate=f.machine.prepareLocalCurrentDiscovery(f.graphCut,f.directory);assertEquals(a,candidate.cut());assertEquals(LifeCycle.State.RUNNING,f.machine.getLifeCycleState());assertEquals(s.index(),f.machine.getLatestSnapshot().getIndex());assertFalse(Files.exists(unwanted));
            f.machine.reloadLocalCurrentDiscovery(candidate).get(10,TimeUnit.SECONDS);assertEquals(a,f.store.applied());assertEquals(s.index(),log.getSnapshotIndex());assertEquals(a.term(),f.store.configuration().term());assertEquals(a.index(),f.store.configuration().index());assertArrayEquals(configuration,f.store.configuration().data());assertFalse(f.store.admissionOpen());assertTrue(f.store.activeSnapshotPins()>0);
            assertTrue(f.machine.settleLocalCurrentDiscovery(candidate,publication->publication.publish()));
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while((f.store.applied().index()<tail || f.resources.stats().protectedLane().bytes()!=0) && System.nanoTime()<deadline)Thread.sleep(5);
            assertEquals(new ApplyPosition(a.term(),tail),f.store.applied());assertArrayEquals(new byte[]{15},f.store.readCurrentPoint(f.schema.columnIds().iterator().next(),new byte[]{7}));assertNotNull(f.store.operationResult(next.operationId(),next.receiptDigest()));
            assertEquals(a.term(),f.store.configuration().term());assertEquals(a.index(),f.store.configuration().index());assertArrayEquals(configuration,f.store.configuration().data());assertEquals(s.index(),f.machine.getLatestSnapshot().getIndex());assertEquals(s.index(),log.getSnapshotIndex());assertFalse(Files.exists(unwanted));assertArrayEquals(bytes,Files.readAllBytes(before));assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.resources.stats().protectedLane().bytes());
        }
    }
    private static org.apache.ratis.proto.RaftProtos.LogEntryProto logData(ApplyPosition position,DistributedCommand command) {
        return org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(position.term()).setIndex(position.index()).setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder().setClientId(ClientId.valueOf(command.operationId().sessionId()).toByteString()).setCallId(command.operationId().sequence()).setLogData(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build();
    }
    @Test void currentCandidateColdRetryRetainsImageAndUnrelatedPlacementProgressDoesNotRebindLeaf()throws Exception {
        try(var f=new Fixture("cold",Mode.BORN)) {
            var ranges=new ArrayList<>(f.directory.ranges());var other=ranges.removeLast();var middle=new byte[]{(byte)192};ranges.add(new RangeDescriptor(new UUID(999,1),new UUID(999,2),1,other.configurationVersion(),other.lowerInclusive(),middle,RangeDescriptor.TopologyState.ACTIVE,other.replicas()));ranges.add(new RangeDescriptor(new UUID(999,3),new UUID(999,4),1,other.configurationVersion(),middle,other.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,other.replicas()));var fresh=new RangeDirectory(f.directory.clusterId(),f.directory.databaseId(),f.directory.domainId(),f.directory.schemaEpoch(),f.directory.schemaDigest(),f.directory.generation()+1,f.directory.columns(),f.directory.routingPrefixBytes(),ranges);
            var first=f.machine.prepareLocalCurrentDiscovery(f.graphCut,fresh);byte[] hash=first.imageHash();var a=f.store.applied();f.stop();f.open();assertEquals(a,f.store.applied());assertFalse(f.store.admissionOpen());var candidate=f.machine.prepareLocalCurrentDiscovery(f.graphCut,fresh);assertArrayEquals(hash,candidate.imageHash());f.machine.reloadLocalCurrentDiscovery(candidate).get(10,TimeUnit.SECONDS);assertTrue(f.machine.settleLocalCurrentDiscovery(candidate,publication->publication.publish()));assertArrayEquals(new byte[]{11},f.store.readCurrentPoint(f.schema.columnIds().iterator().next(),new byte[]{7}));
        }
    }
    @Test void emptyUnreadyAndChangedScopeNeverBecomeCurrentOrGenesisPermission()throws Exception {
        for(var mode:List.of(Mode.EMPTY,Mode.UNREADY))try(var f=new Fixture("refuse-"+mode,mode)){assertThrows(IOException.class,()->f.machine.prepareLocalCurrentDiscovery(f.graphCut,f.directory));assertNull(f.store.applied());assertFalse(f.store.admissionOpen());assertEquals(0,f.store.activeSnapshotPins());}
        try(var f=new Fixture("foreign",Mode.BEHIND)) {
            var range=f.directory.ranges().getFirst();var moved=new RangeDescriptor(new UUID(999,5),range.groupId(),range.rangeEpoch(),range.configurationVersion(),range.lowerInclusive(),range.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,range.replicas());var wrong=new RangeDirectory(f.directory.clusterId(),f.directory.databaseId(),f.directory.domainId(),f.directory.schemaEpoch(),f.directory.schemaDigest(),f.directory.generation(),f.directory.columns(),f.directory.routingPrefixBytes(),List.of(moved));assertThrows(IOException.class,()->f.machine.prepareLocalCurrentDiscovery(f.graphCut,wrong));assertEquals(f.before,f.store.applied());assertFalse(f.store.admissionOpen());if(f.registration!=null){var old=new ApplyPosition(f.registration.catalogueCut().term(),f.registration.catalogueCut().index()-1);assertThrows(IOException.class,()->f.machine.prepareLocalCurrentDiscovery(old,f.directory));assertEquals(f.before,f.store.applied());}var candidate=f.machine.prepareLocalCurrentDiscovery(f.graphCut,f.directory);f.machine.recoveryEligibilityGate().publish(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence.missing());assertThrows(IOException.class,()->f.machine.reloadLocalCurrentDiscovery(candidate));assertFalse(f.store.admissionOpen());
        }
    }
    @Test void canceledCurrentReloadObserverCannotReleaseNativeOwnershipBeforeUpdaterReturns()throws Exception {
        try(var f=new Fixture("cancel",Mode.ABSENT)) {
            var candidate=f.machine.prepareLocalCurrentDiscovery(f.graphCut,f.directory);var entered=new CountDownLatch(1);var proceed=new CountDownLatch(1);var field=RatisRangeStateMachine.class.getDeclaredField("snapshotMetadataAdmission");field.setAccessible(true);var prior=(NativeRangeImage.IoAdmission)field.get(f.machine);field.set(f.machine,new NativeRangeImage.IoAdmission(){public AutoCloseable reserveMetadata(int bytes)throws IOException{return prior.reserveMetadata(bytes);}public void beforeBytes(int bytes)throws IOException{entered.countDown();try{if(!proceed.await(10,TimeUnit.SECONDS))throw new IOException("Controlled CURRENT reload timeout");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}prior.beforeBytes(bytes);}});
            var observer=f.machine.reloadLocalCurrentDiscovery(candidate);try{assertTrue(entered.await(5,TimeUnit.SECONDS));observer.cancel(false);f.machine.beginShutdown();assertTrue(f.store.activeSnapshotPins()>0);assertEquals(1,f.runway.stats().owners());assertFalse(f.store.admissionOpen());}finally{proceed.countDown();}long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(f.store.activeSnapshotPins()!=0 && System.nanoTime()<end)Thread.sleep(5);assertEquals(0,f.store.activeSnapshotPins());assertFalse(f.store.admissionOpen());assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());
        }
    }
}
