package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native local retirement proves META authority independently of a permanent receiver fence. */
class NativeLocalMetadataDiscoveryTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeRecoverySupersessionWorkflowTest workflow=new NativeRecoverySupersessionWorkflowTest();
    final NativeMetadataRecoveryTest metadata=workflow.metadataTest;
    final NativeTopologyCdcTest sourceTest=workflow.nativeTest;
    final NativeTopologySeedTest helper=workflow.helper;
    NativeRecoverySupersessionWorkflowTest.Fixture fixture(String name)throws Exception{workflow.root=root;return workflow.new Fixture(name,true);}
    private void retire(NativeRecoverySupersessionWorkflowTest.Fixture f)throws Exception {
        f.completeCurrent();var definition=f.source.definition;var freeze=f.target.applyTopologyFreeze(new ApplyPosition(1,2),definition,sourceTest.admission(definition));var frozen=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(f.current.receiver().replicaId()));f.pool.seedTopology(definition,frozen,List.of(f.target),f.current.receiver());
    }
    private byte[] key(UUID group,int tag){return ByteBuffer.allocate(17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)tag).array();}
    private ReplicatedRangeStore.LocalMetadataDiscovery candidate(NativeRecoverySupersessionWorkflowTest.Fixture f)throws Exception{return f.pool.prepareLocalMetadataDiscovery(f.target,f.target.acknowledgedRecoverySupersession(),f.current.imageCut());}
    private NativeRangeImage.Manifest write(ReplicatedRangeStore.LocalMetadataDiscovery candidate,Path path)throws Exception {
        try(var pin=candidate.capture()) {
            assertThrows(IllegalArgumentException.class,()->pin.get(helper.outer.fixed,helper.helper.key(1)));
            return NativeRangeImage.writeMetadataAdmitted(path,pin,pin.interval(),sourceTest.limits,NativeRangeImage.IoAdmission.UNLIMITED);
        }
    }
    private ReplicatedRangeStore.LocalMetadataActivation activation(ReplicatedRangeStore.LocalMetadataDiscovery candidate,Path path)throws Exception{return candidate.prepareActivation(path,sourceTest.limits,true,4096,64L<<20);}
    private void closedUser(NativeRecoverySupersessionWorkflowTest.Fixture f)throws Exception {
        assertFalse(f.target.currentRangeRecoveryReady());assertFalse(f.target.admissionOpen());assertTrue(f.target.userHandlesDetached());assertThrows(IllegalStateException.class,f.target::capture);assertThrows(IllegalStateException.class,f.target::captureForRecovery);assertThrows(IllegalStateException.class,f.target::activate);assertThrows(IOException.class,()->f.target.installSnapshot(f.groupImage,sourceTest.limits));
    }
    @Test void completedCurrentLocalSeedExportsAndActivatesOnlyExactMetadataWithoutNativeWorkAtPublish()throws Exception {
        try(var f=fixture("local-meta")) {
            retire(f);f.pool.forceRecoverySupersession(f.scope);var cut=f.target.applied();var cfg=f.target.configuration();closedUser(f);assertThrows(IllegalStateException.class,f.target::captureMetadataForRecovery);assertThrows(IllegalStateException.class,f.target::metadataOutcomeState);
            var retainedCandidate=ReplicatedRangeStore.class.getDeclaredField("localMetadataCandidate");retainedCandidate.setAccessible(true);
            assertNull(retainedCandidate.get(f.target));assertNotNull(f.pool.qualifyLocalMetadataFreeze(f.target,f.target.acknowledgedRecoverySupersession()));assertNull(retainedCandidate.get(f.target));assertFalse(f.target.localMetadataDiscoveryActive());
            var candidate=candidate(f);assertNotNull(candidate);assertEquals(cut,candidate.cut());assertEquals(f.current.imageCut(),candidate.minimumFloor());assertArrayEquals(cfg.data(),candidate.configuration().data());Path image=root.resolve("local-meta.image");var manifest=write(candidate,image);
            try(var pin=candidate.capture()){assertEquals(candidate.freeze(),f.pool.qualifyLocalMetadataFreeze(f.target,f.target.acknowledgedRecoverySupersession()));assertSame(candidate,retainedCandidate.get(f.target));}
            NativeRangeImage.visitMetadata(image,sourceTest.limits,(section,column,key,value)->{assertNotEquals(NativeRangeImage.Section.USER,section);if(section==NativeRangeImage.Section.STATE)assertFalse(NativeRangeImage.metadataLocalPointer(key));});
            var ready=activation(candidate,image);assertEquals(manifest.position(),ready.manifest().position());assertArrayEquals(manifest.sha256(),ready.manifest().sha256());assertFalse(f.target.localMetadataDiscoveryActive());assertThrows(IllegalStateException.class,f.target::metadataOutcomeState);
            assertEquals(candidate.freeze(),f.pool.qualifyLocalMetadataFreeze(f.target,f.target.acknowledgedRecoverySupersession()));assertSame(candidate,retainedCandidate.get(f.target));
            // Holding the native apply lane on another thread cannot block this cached-only publication.
            var field=ReplicatedRangeStore.class.getDeclaredField("applyLane");field.setAccessible(true);var lane=(ReentrantLock)field.get(f.target);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            try(var executor=Executors.newVirtualThreadPerTaskExecutor()) {
                var holder=executor.submit(()->{lane.lock();try{entered.countDown();release.await();}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new CompletionException(failure);}finally{lane.unlock();}});assertTrue(entered.await(5,TimeUnit.SECONDS));
                try{executor.submit(ready::publish).get(5,TimeUnit.SECONDS);}finally{release.countDown();}holder.get(5,TimeUnit.SECONDS);
            }
            assertNull(retainedCandidate.get(f.target));assertThrows(IllegalStateException.class,candidate::capture);assertThrows(IllegalStateException.class,()->activation(candidate,image));assertThrows(IllegalStateException.class,ready::publish);
            assertTrue(f.target.localMetadataDiscoveryActive());assertFalse(f.target.topologyRecoveryOnly());closedUser(f);assertNull(f.target.metadataRecoveryRegistration());assertFalse(f.target.metadataRecoveryImageInstalled());assertEquals(cut,f.target.metadataOutcomeState().applied());
            try(var pin=f.target.captureMetadataForRecovery()){assertEquals(cut,pin.position());assertThrows(IllegalArgumentException.class,()->pin.get(helper.outer.fixed,helper.helper.key(1)));}
            f.target.applyNoop(new ApplyPosition(1,3));assertEquals(new ApplyPosition(1,3),f.target.metadataOutcomeState().applied());closedUser(f);
            f.target.deferDiscovery();assertFalse(f.target.localMetadataDiscoveryActive());assertThrows(IllegalStateException.class,f.target::metadataOutcomeState);assertThrows(IllegalStateException.class,ready::publish);assertThrows(IllegalStateException.class,candidate::capture);
        }
    }
    @Test void localMetadataCandidateAndPublicationAreRevokedByFenceAndColdReopen()throws Exception {
        try(var f=fixture("local-meta-cold")) {
            retire(f);f.pool.forceRecoverySupersession(f.scope);var expected=f.target.acknowledgedRecoverySupersession();var candidate=candidate(f);Path image=root.resolve("local-meta-cold.image");write(candidate,image);var ready=activation(candidate,image);f.target.fence();assertThrows(IllegalStateException.class,ready::publish);
            candidate=candidate(f);write(candidate,root.resolve("local-meta-next.image"));ready=activation(candidate,root.resolve("local-meta-next.image"));ready.publish();assertTrue(f.target.localMetadataDiscoveryActive());var old=ready;
            f.reopen();f.target=f.pool.forceRecoverySupersession(f.scope);assertEquals(expected,f.target.acknowledgedRecoverySupersession());assertFalse(f.target.localMetadataDiscoveryActive());closedUser(f);assertThrows(IllegalStateException.class,old::publish);assertThrows(IllegalStateException.class,f.target::metadataOutcomeState);
            var fresh=candidate(f);assertNotNull(fresh);Path cold=root.resolve("local-meta-requalified.image");write(fresh,cold);activation(fresh,cold).publish();assertTrue(f.target.localMetadataDiscoveryActive());assertEquals(fresh.cut(),f.target.metadataOutcomeState().applied());closedUser(f);
        }
    }
    @Test void fenceAloneMissingCompletionOrPendingInstallCannotCreateALocalMetadataCandidate()throws Exception {
        try(var f=fixture("local-meta-no-seed")){f.completeCurrent();f.pool.forceRecoverySupersession(f.scope);assertNull(candidate(f));closedUser(f);}
        for(int invalid=0;invalid<3;invalid++)try(var f=fixture("local-meta-invalid-"+invalid)) {
            retire(f);var db=helper.db(f.pool);var state=helper.handles(f.pool).get(DistributedStoragePool.STATE_COLUMN);UUID group=f.current.groupId();
            if(invalid==0)db.delete(state,key(group,0x73));
            if(invalid==1)db.delete(state,key(group,0x62));
            if(invalid==2)db.put(state,key(group,0x7f),ByteBuffer.allocate(48).putLong(1).putLong(2).put(new byte[32]).array());
            f.pool.forceRecoverySupersession(f.scope);
            if(invalid==0)assertNull(candidate(f));else if(invalid==1)assertThrows(IllegalArgumentException.class,()->candidate(f));else assertThrows(IllegalStateException.class,()->candidate(f));assertFalse(f.target.localMetadataDiscoveryActive());assertThrows(IllegalStateException.class,f.target::metadataOutcomeState);
        }
    }
    @Test void changedPortableFileOrUncoveredFloorRefusesActivationWithoutGrantingMetadata()throws Exception {
        try(var f=fixture("local-meta-file")) {
            retire(f);f.pool.forceRecoverySupersession(f.scope);assertNull(f.pool.prepareLocalMetadataDiscovery(f.target,f.target.acknowledgedRecoverySupersession(),new ApplyPosition(1,3)));var candidate=candidate(f);Path good=root.resolve("local-meta-good.image");write(candidate,good);byte[] bytes=Files.readAllBytes(good);bytes[bytes.length-1]^=1;Path corrupt=Files.write(root.resolve("local-meta-corrupt.image"),bytes);assertThrows(IOException.class,()->activation(candidate,corrupt));assertFalse(f.target.localMetadataDiscoveryActive());closedUser(f);assertThrows(IllegalStateException.class,f.target::metadataOutcomeState);
            try(var pin=candidate.capture()){assertNull(candidate(f));assertThrows(IllegalStateException.class,()->activation(candidate,good));}activation(candidate,good).publish();assertTrue(f.target.localMetadataDiscoveryActive());
        }
    }
    @Test void rawCurrentAndSupersessionAdmissionPrecedesBodyCopiesAndDenialLeavesNoCandidateOrPin()throws Exception {
        try(var f=fixture("local-meta-admission")) {
            retire(f);f.pool.forceRecoverySupersession(f.scope);var cut=f.target.applied();var expected=f.target.acknowledgedRecoverySupersession();byte[] original=f.current.encode(),foreign=original.clone();foreign[0]^=1;var db=helper.db(f.pool);var state=helper.handles(f.pool).get(DistributedStoragePool.STATE_COLUMN);var currentKey=key(f.current.groupId(),0x72);db.put(state,currentKey,foreign);
            var field=ReplicatedRangeStore.class.getDeclaredField("localMetadataCandidate");field.setAccessible(true);
            for(int deniedRaw:new int[]{4096+2*expected.encodedSize(),2*f.current.encodedSize()}) {
                var retained=new java.util.concurrent.atomic.AtomicLong();var refused=new java.util.concurrent.atomic.AtomicInteger();
                var admission=new NativeRangeImage.IoAdmission(){public void beforeBytes(int bytes){}public AutoCloseable reserveMetadata(int raw)throws IOException{if(raw==deniedRaw){refused.incrementAndGet();throw new IOException("Denied exact native metadata body");}retained.addAndGet(raw);return ()->retained.addAndGet(-raw);}};
                try(var scope=NativeRangeImage.openMetadataAdmission(admission)){assertEquals("Denied exact native metadata body",assertThrows(IOException.class,()->candidate(f)).getMessage());}
                assertEquals(1,refused.get());assertEquals(0,retained.get());assertNull(field.get(f.target));assertEquals(0,f.target.activeSnapshotPins());assertEquals(cut,f.target.applied());assertFalse(f.target.localMetadataDiscoveryActive());assertArrayEquals(foreign,db.get(state,currentKey));assertNull(f.target.metadataRecoveryRegistration());
            }
            db.put(state,currentKey,original);var charged=new ArrayList<Integer>();var retained=new java.util.concurrent.atomic.AtomicLong();var admitted=new NativeRangeImage.IoAdmission(){public void beforeBytes(int bytes){}public AutoCloseable reserveMetadata(int raw){charged.add(raw);retained.addAndGet(raw);return ()->retained.addAndGet(-raw);}};
            try(var scope=NativeRangeImage.openMetadataAdmission(admitted)){assertNotNull(candidate(f));}
            assertTrue(charged.contains(4096+2*expected.encodedSize()));assertTrue(charged.contains(2*f.current.encodedSize()));assertEquals(0,retained.get());assertEquals(cut,f.target.applied());assertEquals(0,f.target.activeSnapshotPins());assertFalse(f.target.localMetadataDiscoveryActive());
        }
    }

}
