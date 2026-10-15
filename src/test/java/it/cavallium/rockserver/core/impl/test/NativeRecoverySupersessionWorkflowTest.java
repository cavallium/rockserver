package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;
import static org.junit.jupiter.api.Assertions.*;

/** Actual JNI receiver fencing before source META capture, without inventing an installed image. */
class NativeRecoverySupersessionWorkflowTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeMetadataRecoveryTest metadataTest=new NativeMetadataRecoveryTest();
    final NativeTopologyCdcTest nativeTest=metadataTest.nativeTest;
    final NativeTopologySeedTest helper=metadataTest.helper;
    final NativeCurrentRangeRecoveryTest currentHelper=new NativeCurrentRangeRecoveryTest();

    final class Fixture implements AutoCloseable {
        final NativeTopologyCdcTest.Fixture source;
        final Path receiver,groupImage,metaImage;
        final NativeCurrentRangeRecovery current;
        final NativeRecoverySupersession.Scope scope;
        DistributedStoragePool pool;
        ReplicatedRangeStore target;
        Fixture(String name,boolean open)throws Exception {
            receiver=root.resolve(name+"-receiver");groupImage=root.resolve(name+"-group");metaImage=root.resolve(name+"-meta");
            source=nativeTest.new Fixture(root.resolve(name+"-source"));
            try {
                source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));
                var image=NativeRangeImage.write(groupImage,source.parent,nativeTest.limits);var cfg=source.parent.configuration();
                current=new NativeCurrentRangeRecovery(new UUID(0,22001),nativeTest.catalogue,new ApplyPosition(1,10),source.definition.sourceDirectory(),helper.outer.schema(),source.parent.ownership().owner().groupId(),helper.topology.replica,helper.topology.replica,image.position(),image.sha256(),cfg.term(),cfg.index(),cfg.data(),image.topologyOrigin(),currentHelper.names(helper.outer.schema(),"supersession-"),Set.of(),currentHelper.schemaRows(source.parent));
                scope=new NativeRecoverySupersession.Scope(current.catalogue(),new ApplyPosition(1,20),source.definition.header(),source.definition.sourceDirectory(),current.groupId(),current.receiver(),hash(1),hash(2),hash(3));
                pool=helper.pool(receiver);if(open){target=pool.openCurrentRangeRecovery(current);metadataTest.configure(target,source.definition);}
            }catch(Exception|Error failed){try{if(pool!=null)pool.close();}catch(Exception|Error close){failed.addSuppressed(close);}try{source.close();}catch(Exception|Error close){failed.addSuppressed(close);}throw failed;}
        }
        void completeCurrent()throws Exception {target.installSnapshot(groupImage,nativeTest.limits);target.completeCurrentRangeRecovery(current,current.imageCut(),target.configuration());}
        void partialCurrent()throws Exception {assertThrows(IOException.class,()->target.installSnapshot(groupImage,nativeTest.limits,batch->{throw new IOException("Interrupted actual CURRENT clear/import");}));assertTrue(target.pendingInstallation());assertEquals(current.imageCut(),target.pendingInstallationCut());assertNull(target.applied());}
        void reopen()throws Exception {pool.close();pool=helper.pool(receiver);target=null;}
        NativeMetadataRecoveryRegistration prepareMetadata(boolean predecessor)throws Exception {
            source.freeze();source.seed();var image=NativeRangeImage.writeMetadata(metaImage,source.parent,nativeTest.limits);var cfg=source.parent.configuration();
            return new NativeMetadataRecoveryRegistration(new UUID(0,22002),current.catalogue(),new ApplyPosition(1,21),source.definition.sourceDirectory(),source.freeze,current.receiver(),current.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of(),predecessor?NativeMetadataRecoveryRegistration.CurrentPredecessor.of(current):null);
        }
        void installMetadata(boolean predecessor)throws Exception {
            var fence=target.recoverySupersession();assertNotNull(fence);
            var registration=prepareMetadata(predecessor);assertSame(target,pool.openMetadataRecovery(registration));metadataTest.configure(target,source.definition);
            assertFalse(target.metadataRecoveryImageInstalled());assertTrue(target.metadataRecoveryPending());assertFalse(target.admissionOpen());
            target.installMetadataSnapshot(metaImage,nativeTest.limits);target.completeMetadataRecovery(registration,target.applied(),target.configuration());
            assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.metadataRecoveryPending());assertFalse(target.admissionOpen());assertEquals(fence,target.acknowledgedRecoverySupersession());
            var applied=target.applied();var configuration=target.configuration();var outcome=target.metadataOutcomeState();
            assertSame(target,pool.forceRecoverySupersession(scope));
            assertEquals(fence,target.acknowledgedRecoverySupersession());assertEquals(fence.captured(),target.recoverySupersession().captured());
            assertEquals(applied,target.applied());assertEquals(configuration.term(),target.configuration().term());assertEquals(configuration.index(),target.configuration().index());assertArrayEquals(configuration.data(),target.configuration().data());
            assertTrue(target.metadataRecoveryImageInstalled());assertFalse(target.metadataRecoveryPending());assertEquals(outcome.applied(),target.metadataOutcomeState().applied());
            assertArrayEquals(fence.encode(),row(pool,current.groupId(),0x75));
            Path exported=root.resolve(UUID.randomUUID()+"-portable-meta");NativeRangeImage.writeMetadata(exported,target,nativeTest.limits);
            NativeRangeImage.visitMetadata(exported,nativeTest.limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE)assertFalse(key.length==17 && key[16]==0x75,"Receiver supersession must stay local");});
            assertThrows(IllegalStateException.class,()->pool.openCurrentRangeRecovery(current));assertThrows(IllegalStateException.class,target::activate);
        }
        @Override public void close()throws Exception {try{if(pool!=null)pool.close();}finally{source.close();}}
    }
    static byte[] hash(int value){var bytes=new byte[32];Arrays.fill(bytes,(byte)value);return bytes;}
    byte[] row(DistributedStoragePool pool,UUID group,int tag)throws Exception {var key=ByteBuffer.allocate(tag==0?16:17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits());if(tag!=0)key.put((byte)tag);return helper.db(pool).get(helper.handles(pool).get(DistributedStoragePool.STATE_COLUMN),key.array());}
    Map<Integer,byte[]> originalRows(Fixture f)throws Exception {var rows=new HashMap<Integer,byte[]>();for(int tag:new int[]{0,1,0x72,0x73,0x74,0x7f})rows.put(tag,row(f.pool,f.current.groupId(),tag));return rows;}
    void unchangedRows(Fixture f,Map<Integer,byte[]> before)throws Exception {for(var row:before.entrySet())assertArrayEquals(row.getValue(),row(f.pool,f.current.groupId(),row.getKey()),"Native row "+row.getKey()+" must be preserved by fence");}
    void fenced(Fixture f,boolean acknowledged)throws Exception {
        assertNotNull(f.target.recoverySupersession());assertEquals(acknowledged,f.target.acknowledgedRecoverySupersession()!=null);
        assertTrue(f.target.userHandlesDetached());assertFalse(f.target.admissionOpen());assertFalse(f.target.metadataRecoveryImageInstalled());assertNull(f.target.metadataRecoveryRegistration());
        assertThrows(IllegalStateException.class,f.target::capture);assertThrows(IllegalStateException.class,f.target::captureForRecovery);assertThrows(IllegalStateException.class,f.target::metadataOutcomeState);assertThrows(IllegalStateException.class,f.target::activate);
        assertThrows(IllegalStateException.class,()->f.target.applyNoop(new ApplyPosition(1,30)));
        assertThrows(IOException.class,()->f.target.installSnapshot(f.groupImage,nativeTest.limits));
        assertThrows(IllegalStateException.class,()->f.pool.openCurrentRangeRecovery(f.current));
        assertTrue(f.pool.metadataRecoveries().isEmpty());assertEquals(Set.of(f.current.groupId()),f.pool.persistedRangeGroups(8));
        chargedMetadataOwner(f);
    }
    void chargedMetadataOwner(Fixture f)throws Exception {
        var owners=f.pool.retainedMetadataOwners();assertEquals(1,owners.size());var owner=owners.getFirst();
        assertEquals(f.current.groupId(),owner.owner().groupId());assertEquals(f.current.range().rangeEpoch(),owner.owner().epoch());
        assertTrue(owner.retired(),"Supersession owns a permanent metadata role even before force ACK");
        assertTrue(owner.pending(),"No META image/completion has discharged this retained owner");
    }

    @Test void invalidCurrentAuthorityIsRejectedBeforeAnyFenceOrForce()throws Exception {
        try(var f=new Fixture("invalid-authority",true)) {
            f.completeCurrent();var before=originalRows(f);var cfg=f.target.configuration();var applied=f.target.applied();
            var calls=new java.util.concurrent.atomic.AtomicInteger();f.pool.recoverySupersessionForceObserver(after->calls.incrementAndGet());
            for(var invalid:List.of(
                    new NativeRecoverySupersession.Scope(UUID.randomUUID(),f.scope.catalogueCut(),f.scope.header(),f.scope.sourceDirectory(),f.scope.groupId(),f.scope.receiver(),hash(1),hash(2),hash(3)),
                    new NativeRecoverySupersession.Scope(f.scope.catalogue(),new ApplyPosition(1,9),f.scope.header(),f.scope.sourceDirectory(),f.scope.groupId(),f.scope.receiver(),hash(1),hash(2),hash(3)))) {
                assertThrows(IllegalArgumentException.class,()->f.pool.forceRecoverySupersession(invalid));
                assertEquals(0,calls.get());assertNull(f.target.recoverySupersession());assertNull(row(f.pool,f.current.groupId(),0x75));
                assertTrue(f.pool.recoverySupersessions().isEmpty());assertFalse(f.target.userHandlesDetached());assertTrue(f.target.currentRangeRecoveryReady());
                assertEquals(applied,f.target.applied());assertEquals(cfg.index(),f.target.configuration().index());unchangedRows(f,before);
            }
            f.pool.forceRecoverySupersession(f.scope);assertEquals(2,calls.get());fenced(f,true);
        }
    }
    @Test void partialCurrentIsFencedBeforeMetaPreparationThenRealMetadataCompletes()throws Exception {
        try(var f=new Fixture("partial",true)) {
            f.partialCurrent();var before=originalRows(f);var handles=Set.copyOf(helper.handles(f.pool).keySet());var original=f.target;
            assertSame(original,f.pool.forceRecoverySupersession(f.scope));fenced(f,true);unchangedRows(f,before);assertEquals(handles,helper.handles(f.pool).keySet());
            var captured=f.target.recoverySupersession().captured();assertArrayEquals(f.current.digest(),captured.currentRegistrationDigest());assertEquals(f.current.imageCut(),captured.registeredImageCut());assertNotNull(captured.pendingIntent());assertNull(captured.marker());
            assertEquals(List.of(f.target.recoverySupersession()),f.pool.recoverySupersessions());f.installMetadata(true);
        }
    }
    @Test void completedCurrentPreservesActualReceiptAndCfgAcrossFenceAndColdReopen()throws Exception {
        try(var f=new Fixture("completed",true)) {
            f.completeCurrent();var before=originalRows(f);var actual=f.target.applied();var cfg=f.target.configuration();f.pool.forceRecoverySupersession(f.scope);unchangedRows(f,before);fenced(f,true);
            var saved=f.target.recoverySupersession();assertEquals(actual,saved.captured().marker());assertNotNull(saved.captured().localImageReceipt());assertNull(saved.captured().pendingIntent());assertEquals(cfg.index(),saved.captured().configuration().index());
            f.reopen();assertEquals(List.of(saved),f.pool.recoverySupersessions());f.target=f.pool.forceRecoverySupersession(f.scope);assertEquals(saved,f.target.acknowledgedRecoverySupersession());unchangedRows(f,before);fenced(f,true);f.installMetadata(true);
        }
    }
    @Test void beforeForceLostAckRetainsWarmFenceAndOriginalCapturedInputUntilExactRetry()throws Exception {
        try(var f=new Fixture("before",true)) {
            f.partialCurrent();var before=originalRows(f);var once=new java.util.concurrent.atomic.AtomicBoolean(true);
            f.pool.recoverySupersessionForceObserver(after->{if(!after && once.getAndSet(false))throw new RocksDBException("Lost before supersession force");});
            assertThrows(RocksDBException.class,()->f.pool.forceRecoverySupersession(f.scope));assertFalse(once.get());fenced(f,false);assertNull(row(f.pool,f.current.groupId(),0x75));assertTrue(f.pool.recoverySupersessions().isEmpty());unchangedRows(f,before);
            var retained=f.target.recoverySupersession();var changed=new NativeRecoverySupersession.Scope(f.scope.catalogue(),f.scope.catalogueCut(),f.scope.header(),f.scope.sourceDirectory(),f.scope.groupId(),f.scope.receiver(),hash(8),f.scope.originalCurrentRequestDigest(),f.scope.effectiveCurrentRequestDigest());
            assertThrows(IllegalArgumentException.class,()->f.pool.forceRecoverySupersession(changed));assertEquals(retained,f.target.recoverySupersession());
            f.pool.recoverySupersessionForceObserver(after->{});assertSame(f.target,f.pool.forceRecoverySupersession(f.scope));assertEquals(retained,f.target.acknowledgedRecoverySupersession());unchangedRows(f,before);f.installMetadata(true);
        }
    }
    @Test void afterForceLostAckColdInventoryRequiresExactReforceBeforeAcknowledgement()throws Exception {
        try(var f=new Fixture("after",true)) {
            f.partialCurrent();var before=originalRows(f);var once=new java.util.concurrent.atomic.AtomicBoolean(true);
            f.pool.recoverySupersessionForceObserver(after->{if(after && once.getAndSet(false))throw new RocksDBException("Lost after supersession force");});
            assertThrows(RocksDBException.class,()->f.pool.forceRecoverySupersession(f.scope));assertFalse(once.get());fenced(f,false);var saved=f.target.recoverySupersession();assertArrayEquals(saved.encode(),row(f.pool,f.current.groupId(),0x75));assertTrue(f.pool.recoverySupersessions().isEmpty());unchangedRows(f,before);
            f.reopen();assertEquals(List.of(saved),f.pool.recoverySupersessions());assertThrows(IllegalStateException.class,()->f.pool.openCurrentRangeRecovery(f.current));
            f.target=f.pool.forceRecoverySupersession(f.scope);assertEquals(saved,f.target.acknowledgedRecoverySupersession());unchangedRows(f,before);f.installMetadata(true);
        }
    }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void absentGroupUnknownPrepareCreatesOnlyMetadataAndColdRetryThenAcceptsRealMeta(boolean afterForce)throws Exception {
        try(var f=new Fixture("absent-"+afterForce,false)) {
            var handles=Set.copyOf(helper.handles(f.pool).keySet());var once=new java.util.concurrent.atomic.AtomicBoolean(true);
            f.pool.recoverySupersessionForceObserver(after->{if(after==afterForce && once.getAndSet(false))throw new RocksDBException("Lost absent supersession force");});
            assertThrows(RocksDBException.class,()->f.pool.forceRecoverySupersession(f.scope));assertFalse(once.get());assertEquals(handles,helper.handles(f.pool).keySet());assertTrue(f.pool.recoverySupersessions().isEmpty());assertTrue(f.pool.currentRangeRecoveries().isEmpty());assertTrue(f.pool.metadataRecoveries().isEmpty());
            chargedMetadataOwner(f);
            assertNull(row(f.pool,f.current.groupId(),0));assertNull(row(f.pool,f.current.groupId(),1));assertNull(row(f.pool,f.current.groupId(),0x72));assertThrows(IllegalStateException.class,()->f.pool.openCurrentRangeRecovery(f.current));
            f.reopen();assertEquals(afterForce?1:0,f.pool.recoverySupersessions().size());f.target=f.pool.forceRecoverySupersession(f.scope);assertEquals(handles,helper.handles(f.pool).keySet());assertNull(f.target.applied());assertNull(f.target.configuration());fenced(f,true);
            var captured=f.target.recoverySupersession().captured();assertNull(captured.currentRegistrationDigest());assertNull(captured.registeredImageCut());assertNull(captured.marker());assertNull(captured.configuration());assertNull(captured.pendingIntent());assertNull(captured.localImageReceipt());f.installMetadata(false);
        }
    }
    @Test void existingReadyMetadataTailSurvivesFirstFenceColdReforceAndExactOriginalRecipe()throws Exception {
        try(var f=new Fixture("existing-meta",false)) {
            var registration=f.prepareMetadata(false);f.target=f.pool.openMetadataRecovery(registration);metadataTest.configure(f.target,f.source.definition);
            f.target.installMetadataSnapshot(f.metaImage,nativeTest.limits);f.target.completeMetadataRecovery(registration,f.target.applied(),f.target.configuration());
            var tail=new ApplyPosition(1,f.target.applied().index()+1);f.target.applyNoop(tail,new byte[]{2});
            assertTrue(tail.index()>registration.imageCut().index());var cfg=f.target.configuration();assertEquals(tail.index(),cfg.index());
            var scope=new NativeRecoverySupersession.Scope(f.scope.catalogue(),new ApplyPosition(1,30),f.scope.header(),f.scope.sourceDirectory(),f.scope.groupId(),f.scope.receiver(),hash(1),hash(2),hash(3));
            assertSame(f.target,f.pool.forceRecoverySupersession(scope));var saved=f.target.acknowledgedRecoverySupersession();assertNotNull(saved);
            assertEquals(tail,saved.captured().marker());assertEquals(tail.index(),saved.captured().configuration().index());
            saved.requireMetadataScope(registration);assertThrows(IllegalArgumentException.class,()->saved.requireMetadata(registration));
            assertEquals(tail,f.target.applied());assertTrue(f.target.metadataRecoveryImageInstalled());assertFalse(f.target.metadataRecoveryPending());assertEquals(tail,f.target.metadataOutcomeState().applied());
            f.reopen();assertEquals(List.of(saved),f.pool.recoverySupersessions());f.target=f.pool.forceRecoverySupersession(scope);assertSame(f.target,f.pool.openMetadataRecovery(registration));metadataTest.configure(f.target,f.source.definition);
            assertEquals(tail,f.target.applied());assertEquals(cfg.term(),f.target.configuration().term());assertEquals(cfg.index(),f.target.configuration().index());assertArrayEquals(cfg.data(),f.target.configuration().data());
            assertTrue(f.target.metadataRecoveryImageInstalled());assertFalse(f.target.metadataRecoveryPending());assertEquals(tail,f.target.metadataOutcomeState().applied());assertEquals(saved,f.target.acknowledgedRecoverySupersession());assertArrayEquals(saved.encode(),row(f.pool,f.current.groupId(),0x75));
            Path exported=root.resolve("existing-meta-portable");NativeRangeImage.writeMetadata(exported,f.target,nativeTest.limits);
            NativeRangeImage.visitMetadata(exported,nativeTest.limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE)assertFalse(key.length==17 && key[16]==0x75);});
        }
    }
    @Test void portableGroupAndMetadataFramesRejectInjectedReceiverSupersessionRows()throws Exception {
        try(var f=new Fixture("portable-reject",true)) {
            f.completeCurrent();Path own=root.resolve("portable-own-group");NativeRangeImage.write(own,f.target,nativeTest.limits);
            assertDoesNotThrow(()->NativeRangeImage.inspect(own,nativeTest.limits));
            byte[] key72=stateKey(f.current.groupId(),0x72),key75=stateKey(f.current.groupId(),0x75);
            Path injectedGroup=currentHelper.alteredImage(own,root.resolve("injected-group-75"),key72,key75);
            assertTrue(assertThrows(IOException.class,()->NativeRangeImage.inspect(injectedGroup,nativeTest.limits)).getMessage().contains("supersession"));
            f.pool.forceRecoverySupersession(f.scope);f.installMetadata(true);
            Path portable=root.resolve("portable-sealed-meta");NativeRangeImage.writeMetadata(portable,f.target,nativeTest.limits);
            assertDoesNotThrow(()->NativeRangeImage.inspectMetadata(portable,nativeTest.limits));
            Path injectedMeta=currentHelper.alteredImage(portable,root.resolve("injected-meta-75"),stateKey(f.current.groupId(),0x60),key75);
            assertTrue(assertThrows(IOException.class,()->NativeRangeImage.inspectMetadata(injectedMeta,nativeTest.limits)).getMessage().contains("supersession"));
        }
    }
    static byte[] stateKey(UUID group,int tag){return ByteBuffer.allocate(17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)tag).array();}
    @Test void actualSnapshotPinAndInstallerMustDrainBeforeFence()throws Exception {
        try(var f=new Fixture("drain",true);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            f.completeCurrent();try(var pin=f.target.captureForRecovery()){assertThrows(IllegalStateException.class,()->f.pool.forceRecoverySupersession(f.scope));assertNull(f.target.recoverySupersession());assertTrue(f.pool.recoverySupersessions().isEmpty());}
            f.target.fence();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            var installer=workers.submit(()->f.target.installSnapshot(f.groupImage,nativeTest.limits,batch->{entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Installer timeout");}catch(InterruptedException stopped){Thread.currentThread().interrupt();throw new IOException(stopped);}throw new IOException("Interrupted owned installer");}));
            try{assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(IllegalStateException.class,()->f.pool.forceRecoverySupersession(f.scope));assertNull(f.target.recoverySupersession());assertTrue(f.pool.recoverySupersessions().isEmpty());}finally{release.countDown();}
            assertInstanceOf(IOException.class,assertThrows(ExecutionException.class,()->installer.get(5,TimeUnit.SECONDS)).getCause());f.pool.forceRecoverySupersession(f.scope);fenced(f,true);f.installMetadata(true);
        }
    }
}
