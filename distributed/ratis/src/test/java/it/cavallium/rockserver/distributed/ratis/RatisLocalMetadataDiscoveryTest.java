package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.*;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.*;
import org.apache.ratis.server.*;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual JNI metadata + real division/updater/segmented-log reload; RPC never binds or connects. */
public class RatisLocalMetadataDiscoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final NativeRangeImage.Limits IMAGES=new NativeRangeImage.Limits(4<<20,32<<20,8);
    public static final class NoSocketRpcType implements RpcType {
        public NoSocketRpcType(){}
        public String name(){return getClass().getName();}
        public RpcFactory newFactory(Parameters parameters){return new ServerFactory(){
            public RpcType getRpcType(){return new NoSocketRpcType();}
            public RaftServerRpc newRaftServerRpc(RaftServer server){return (RaftServerRpc)java.lang.reflect.Proxy.newProxyInstance(RaftServerRpc.class.getClassLoader(),new Class<?>[]{RaftServerRpc.class},(proxy,method,args)->switch(method.getName()){
                case "getRpcType"->new NoSocketRpcType();case "getInetSocketAddress"->InetSocketAddress.createUnresolved("no-socket",0);case "start","close","addRaftPeers","handleException"->null;case "toString"->"NoSocketRpc";default->throw new AssertionError("Unexpected socket-free RPC "+method);
            });}
        };}
    }
    /** Borrowed by Node orchestration tests; create initializes groups but does not start transport. */
    static RaftServer noSocketServer(RatisNodePlan plan,Path logs,Map<RaftGroupId,RatisRangeStateMachine> machines)throws IOException {
        var properties=RatisDurability.properties(logs);RaftConfigKeys.Rpc.setType(properties,new NoSocketRpcType());RaftConfigKeys.DataStream.setType(properties,SupportedDataStreamType.DISABLED);
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties,TimeDuration.valueOf(1,TimeUnit.HOURS));RaftServerConfigKeys.Rpc.setTimeoutMax(properties,TimeDuration.valueOf(2,TimeUnit.HOURS));RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties,TimeDuration.valueOf(1,TimeUnit.HOURS));RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties,TimeDuration.valueOf(2,TimeUnit.HOURS));RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties,TimeDuration.ZERO);
        return org.apache.ratis.server.impl.RockserverRaftServers.create(RaftPeerId.valueOf(plan.peerId()),plan.localGroups().getFirst(),RaftStorage.StartupOption.RECOVER,id->{var machine=machines.get(id);if(machine==null)throw new IllegalArgumentException("Unplanned socket-free state machine");return machine;},properties,new Parameters());
    }
    final class Fixture implements AutoCloseable {
        final RatisTwoPoolWorkflowFixture source;final DistributedStoragePool pool;final RatisNodePlan plan;final TopologyChange.Definition consuming;
        final ReplicatedRangeStore store;final Path logs;final ApplyPosition graphCut,floor;final RatisNodeResourceBudget resources;final RatisDiskRunway runway;
        RaftServer server;RatisRangeStateMachine machine;RatisCommittedApplyDispatcher dispatcher;
        Fixture(String name,boolean registered,boolean advance)throws Exception {
            source=new RatisTwoPoolWorkflowFixture(root.resolve(name+"-source"),false);var helper=new RatisDiscoveryInventoryTest();helper.root=root;
            var directory=source.firstDirectory;var descriptor=directory.ranges().getFirst();var sourceStore=source.store(descriptor.groupId());Path initial=root.resolve(name+"-initial.group");NativeRangeImage.write(initial,sourceStore,IMAGES);consuming=source.split();var freeze=sourceStore.topologyFreeze();
            var domain=new RatisNodePlan.Domain(directory,Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),source.plan.catalogue(),List.of(),List.of(),Map.of(),4,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(source.plan.catalogue(),List.of()));
            pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve(name+"-native"));Path group=root.resolve(name+"-floor.group");
            if(registered){Path meta=root.resolve(name+"-initial.meta");var image=NativeRangeImage.writeMetadata(meta,sourceStore,IMAGES);var registration=helper.metadataRegistration(source,image,null);store=pool.openMetadataRecovery(registration);store.installMetadataSnapshot(meta,IMAGES);store.completeMetadataRecovery(registration,image.position(),store.configuration());
                Files.copy(meta,group);
            }else {store=helper.open(pool,directory,source.first);store.fence();store.installSnapshot(initial,IMAGES);store.activate();store.enableCdc(descriptor.rangeId());store.applyTopologyFreeze(freeze.position(),consuming,freeze.admissionDigest());NativeRangeImage.write(group,store,IMAGES);var authority=TopologyChange.State.begin(consuming.header()).beginFencing().frozen(freeze.proof(source.local.replicaId()));pool.seedTopology(consuming,authority,List.of(store),source.local);}
            floor=(registered?NativeRangeImage.inspectMetadata(group,IMAGES):NativeRangeImage.inspect(group,IMAGES)).position();if(advance)store.applyNoop(new ApplyPosition(1,store.applied().index()+1));
            graphCut=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),plan.catalogue().groupId(),plan.catalogue().epoch(),plan.catalogue().schemaDigest(),plan.catalogue().directoryColumn()).readView(directory.domainId()).cut();logs=root.resolve(name+"-raft");var groupDirectory=logs.resolve(descriptor.groupId().toString());Files.createDirectories(groupDirectory);
            try(var storage=RaftStorage.newBuilder().setDirectory(groupDirectory.toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(SizeInBytes.ONE_MB).build()){storage.initialize();var snapshots=new SimpleStateMachineStorage();snapshots.init(storage);var target=snapshots.getSnapshotFile(floor.term(),floor.index()).toPath();Files.createDirectories(target.getParent());Files.copy(group,target);MD5FileUtil.saveMD5File(target.toFile(),MD5FileUtil.computeMd5ForFile(target.toFile()));SessionJournalFiles.force(target);SessionJournalFiles.force(target.getParent());}
            var defaults=RatisNodeRuntime.ResourceLimits.defaults();resources=new RatisNodeResourceBudget(defaults.node(),Map.of(RatisPoolFixtures.ID,defaults.pool()));var clock=new AtomicLong(1);var policy=new RatisDiskRunway.Policy(16L<<20,8L<<20,24L<<20,8L<<20,8L<<20,1L<<20,java.time.Duration.ofMinutes(1),16,128,16);runway=new RatisDiskRunway(policy,clock::get);var free=Map.of(1L,new RatisDiskRunway.Filesystem(1,1L<<30));long cut=runway.beginCensus();runway.publish(new RatisDiskRunway.Census(1,cut,free,Map.of(RatisPoolFixtures.ID,Set.of(1L)),Set.of(1L),0));long observed=runway.beginFreeObservation();runway.observeFree(1,observed,free);try{open();}catch(Exception|Error failure){try{close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}throw failure;}
        }
        void open()throws Exception {
            var group=plan.localGroups().getFirst();machine=new RatisRangeStateMachine(store,group,IMAGES);machine.topologyAuthority((header,stage)->{throw new IOException("No remote IO in local metadata permit");},consuming.sourceDirectory(),source.local,pool);machine.nodeResources(resources,RatisPoolFixtures.ID);var logLimits=RatisLogResourceBudget.Limits.defaults();machine.logResources(new RatisLogResourceBudget(group.getGroupId().getUuid(),new RatisLogResourceBudget.Shared(logLimits),new RatisLogResourceBudget.Shared(logLimits),logLimits,(bytes,progress)->()->{}));machine.diskRunway(runway,RatisPoolFixtures.ID);var policy=RatisCommittedApplyDispatcher.Policy.defaults();dispatcher=new RatisCommittedApplyDispatcher(policy.node(),Map.of(RatisPoolFixtures.ID,policy.pool()));machine.committedApply(dispatcher,RatisPoolFixtures.ID,NativeSstImportManifest.MAX_FILE_BYTES+4096);machine.deferRecoveryDiscovery();server=noSocketServer(plan,logs,Map.of(group.getGroupId(),machine));server.start();
        }
        void stop()throws Exception{if(server!=null){machine.beginShutdown();server.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));server=null;}if(dispatcher!=null){dispatcher.awaitDrained(java.time.Duration.ofSeconds(5));dispatcher.close();dispatcher=null;}}
        @Override public void close()throws Exception{try{stop();}finally{pool.close();source.close();}}
    }
    @Test void genuineLocalMetadataCandidateReloadsActualDivisionWithoutNativeRestoreAtEqualOrLaterCut()throws Exception {
        for(boolean advance:List.of(false,true))try(var f=new Fixture("actual-"+advance,false,advance)) {
            var a=f.store.applied();var candidate=f.machine.prepareLocalMetadataDiscovery(f.graphCut,f.consuming);assertEquals(a,candidate.cut());assertEquals(LifeCycle.State.PAUSED,f.machine.getLifeCycleState());assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());assertTrue(f.store.activeSnapshotPins()>0);assertTrue(f.resources.stats().protectedLane().bytes()>0);assertEquals(1,f.runway.stats().owners());assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(f.machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));assertThrows(IOException.class,f.machine::reinitialize);
            f.machine.reloadLocalMetadataDiscovery(candidate).get(10,TimeUnit.SECONDS);assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());assertEquals(a,f.store.applied());assertEquals(a.index(),f.server.getDivision(f.machine.getGroupId()).getRaftLog().getSnapshotIndex());assertTrue(f.machine.settleLocalMetadataDiscovery(candidate,publication->publication.publish()));assertFalse(f.machine.recoveryEligibilityGate().discoveryPending());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.RECOVERY,f.machine.recoveryEligibilityGate().snapshot().mode());assertFalse(f.store.admissionOpen());assertEquals(a,f.store.applied());assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.runway.stats().owners());assertEquals(0,f.resources.stats().protectedLane().bytes());
        }
    }
    @Test void completedRegisteredMetaCandidateCanColdResumeWithoutRemoteRegistrationCycle()throws Exception {
        try(var f=new Fixture("registered",true,true)) {
            var registration=f.store.metadataRecoveryRegistration();var a=f.store.applied();var first=f.machine.prepareLocalMetadataDiscovery(f.graphCut,f.consuming);byte[] hash=first.imageHash();var canonical=f.machine.getLatestSnapshot().getFiles().getFirst().getPath();Files.delete(MD5FileUtil.getDigestFileForFile(canonical.toFile()).toPath());SessionJournalFiles.force(canonical.getParent());f.stop();assertEquals(0,f.store.activeSnapshotPins());f.open();assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());assertEquals(a,f.store.applied());assertFalse(f.store.metadataRecoveryPending());assertEquals(registration,f.store.metadataRecoveryRegistration());var candidate=f.machine.prepareLocalMetadataDiscovery(f.graphCut,f.consuming);assertArrayEquals(hash,candidate.imageHash());f.machine.reloadLocalMetadataDiscovery(candidate).get(10,TimeUnit.SECONDS);assertTrue(f.machine.settleLocalMetadataDiscovery(candidate,publication->publication.publish()));assertEquals(registration,f.store.metadataRecoveryRegistration());assertEquals(a,f.store.applied());assertFalse(f.store.admissionOpen());
        }
    }
    @Test void candidateBindsExactGraphRevisionAndPublicationOwner()throws Exception {
        try(var f=new Fixture("scope",false,false)) {
            var wrong=new TopologyChange.Definition(new UUID(998,1),f.consuming.sourceDirectory(),f.consuming.targetDirectory());assertThrows(IOException.class,()->f.machine.prepareLocalMetadataDiscovery(f.graphCut,wrong));assertEquals(0,f.store.activeSnapshotPins());var candidate=f.machine.prepareLocalMetadataDiscovery(f.graphCut,f.consuming);assertSame(candidate,f.machine.prepareLocalMetadataDiscovery(f.graphCut,f.consuming));assertSame(candidate,f.machine.prepareLocalMetadataDiscovery(new ApplyPosition(f.graphCut.term(),f.graphCut.index()+1),f.consuming));assertEquals(f.graphCut,candidate.graphCut());f.machine.reloadLocalMetadataDiscovery(candidate).get(10,TimeUnit.SECONDS);assertThrows(IOException.class,()->f.machine.settleLocalMetadataDiscovery(candidate,publication->{throw new IOException("Node closing");}));assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());assertTrue(f.store.activeSnapshotPins()>0);assertSame(candidate,f.machine.prepareLocalMetadataDiscovery(new ApplyPosition(f.graphCut.term(),f.graphCut.index()+2),f.consuming));assertEquals(LifeCycle.State.RUNNING,f.machine.getLifeCycleState());f.machine.reloadLocalMetadataDiscovery(candidate).get(5,TimeUnit.SECONDS);assertTrue(f.machine.settleLocalMetadataDiscovery(candidate,publication->publication.publish()));assertEquals(f.graphCut,candidate.graphCut());assertFalse(f.store.admissionOpen());
        }
    }
    @Test void revokedCandidateCannotReloadOrSettle()throws Exception {
        try(var f=new Fixture("revoked",false,false)){var candidate=f.machine.prepareLocalMetadataDiscovery(f.graphCut,f.consuming);f.machine.recoveryEligibilityGate().publish(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence.missing());assertThrows(IOException.class,()->f.machine.reloadLocalMetadataDiscovery(candidate));assertThrows(IOException.class,()->f.machine.settleLocalMetadataDiscovery(candidate,publication->publication.publish()));assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());assertFalse(f.store.admissionOpen());}
    }
    @Test void canceledObserverAndStopCannotReleaseActualReloadPinBeforeUpdaterReturns()throws Exception {
        try(var f=new Fixture("cancel",false,false)) {
            var candidate=f.machine.prepareLocalMetadataDiscovery(f.graphCut,f.consuming);var entered=new CountDownLatch(1);var proceed=new CountDownLatch(1);var field=RatisRangeStateMachine.class.getDeclaredField("snapshotMetadataAdmission");field.setAccessible(true);var prior=(NativeRangeImage.IoAdmission)field.get(f.machine);field.set(f.machine,new NativeRangeImage.IoAdmission(){public AutoCloseable reserveMetadata(int bytes)throws IOException{return prior.reserveMetadata(bytes);}public void beforeBytes(int bytes)throws IOException{entered.countDown();try{if(!proceed.await(10,TimeUnit.SECONDS))throw new IOException("Controlled updater timeout");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}prior.beforeBytes(bytes);}});
            var observer=f.machine.reloadLocalMetadataDiscovery(candidate);try {assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(IOException.class,()->f.machine.prepareLocalMetadataDiscovery(f.graphCut,f.consuming));observer.cancel(false);f.machine.beginShutdown();assertTrue(f.store.activeSnapshotPins()>0);assertEquals(1,f.runway.stats().owners());assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());}finally{proceed.countDown();}
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(f.store.activeSnapshotPins()!=0 && System.nanoTime()<deadline)Thread.sleep(5);assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.runway.stats().owners());assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());assertFalse(f.store.admissionOpen());
        }
    }
}
