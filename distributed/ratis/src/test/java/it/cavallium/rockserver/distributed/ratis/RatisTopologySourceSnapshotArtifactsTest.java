package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.util.concurrent.TimeUnit;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.*;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisTopologySourceSnapshotArtifactsTest {
    @Test void heldColdImageValidationDoesNotOwnTheGlobalArtifactMonitor()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var first=fixture.capture(fixture.request(50),NativeRangeImage.IoAdmission.UNLIMITED);var second=fixture.capture(fixture.request(51),NativeRangeImage.IoAdmission.UNLIMITED);var helper=open(artifacts());var executor=java.util.concurrent.Executors.newSingleThreadExecutor()){
            helper.retain(first.prepared(),managed(first,root.resolve("first-managed")));helper.retain(second.prepared(),managed(second,root.resolve("second-managed")));
            var entered=new java.util.concurrent.CountDownLatch(1);var unblock=new java.util.concurrent.CountDownLatch(1);
            var actual=executor.submit(()->helper.lookupAdmitted(second.prepared().request(),bytes->{assertFalse(Thread.holdsLock(helper));entered.countDown();try{assertTrue(unblock.await(5,TimeUnit.SECONDS));}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}}));
            try{assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(IOException.class,helper::close);helper.released(first.prepared().request());assertTrue(helper.isReleased(first.prepared().request()));}
            finally{unblock.countDown();}
            assertArrayEquals(second.prepared().image().sha256(),actual.get(5,TimeUnit.SECONDS).prepared().image().sha256());
        }
    }
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4*1024*1024,8*1024*1024,16);
    Path artifacts()throws IOException{return Files.createDirectories(root.resolve("artifacts"));}
    RatisTopologySourceSnapshotArtifacts open(Path directory)throws IOException{return new RatisTopologySourceSnapshotArtifacts(directory,limits,64*1024*1024,128*1024*1024,4);}
    @Test void recoveryReservationPendingPromotesAndMatchingDuplicateUsesOneQuotaSlot()throws Exception {
        var owned=artifacts();var id=new UUID(0,901);Path canonical=owned.resolve(id+".recovery-reserved"),pending=owned.resolve(id+".recovery-reserved.pending");
        long maximum=2*limits.maxImageBytes(),metadata=3L*RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES;
        try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,maximum,metadata,1)){ledger.reserveTopologyRecovery(id);assertEquals(maximum,ledger.reservedImageBytes());}
        byte[] original=Files.readAllBytes(canonical);Files.move(canonical,pending);SessionJournalFiles.force(owned);
        try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,maximum,metadata,1)){assertEquals(maximum,ledger.reservedImageBytes());assertArrayEquals(original,Files.readAllBytes(canonical));assertFalse(Files.exists(pending));}
        Files.copy(canonical,pending);SessionJournalFiles.force(pending);SessionJournalFiles.force(owned);
        try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,maximum,metadata,1)){assertEquals(maximum,ledger.reservedImageBytes());assertArrayEquals(original,Files.readAllBytes(canonical));assertFalse(Files.exists(pending));}
        try(var ledger=new RatisTopologySourceSnapshotArtifacts(owned,limits,maximum,metadata,1)){assertEquals(maximum,ledger.reservedImageBytes());}
    }
    @Test void partialOrConflictingRecoveryReservationPendingNeverChangesOriginalFiles()throws Exception {
        for(boolean originalPresent:List.of(false,true))for(boolean partial:List.of(false,true)) {
            var owned=Files.createDirectory(root.resolve("reservation-"+originalPresent+"-"+partial));var id=new UUID(0,902);
            Path canonical=owned.resolve(id+".recovery-reserved"),pending=owned.resolve(id+".recovery-reserved.pending");
            if(originalPresent)SessionJournalFiles.write(canonical,ByteBuffer.allocate(8).putLong(limits.maxImageBytes()).array(),40);
            byte[] original=originalPresent?Files.readAllBytes(canonical):null;
            if(partial)Files.write(pending,new byte[]{1,2,3});else SessionJournalFiles.write(pending,ByteBuffer.allocate(8).putLong(limits.maxImageBytes()+1).array(),40);
            byte[] unsettled=Files.readAllBytes(pending);
            assertThrows(IOException.class,()->open(owned));assertArrayEquals(unsettled,Files.readAllBytes(pending));
            if(originalPresent)assertArrayEquals(original,Files.readAllBytes(canonical));else assertFalse(Files.exists(canonical));
        }
    }
    @Test void recoveryReservationPendingCannotPromoteBeforeSlotImageAndMetadataAdmission()throws Exception {
        var owned=artifacts();var ids=List.of(new UUID(0,903),new UUID(0,904));var originals=new HashMap<UUID,byte[]>();
        for(var id:ids){var pending=owned.resolve(id+".recovery-reserved.pending");SessionJournalFiles.write(pending,ByteBuffer.allocate(8).putLong(limits.maxImageBytes()).array(),40);originals.put(id,Files.readAllBytes(pending));}
        long image=limits.maxImageBytes(),metadata=3L*RatisTopologyRecoveryAnchors.MAX_METADATA_BYTES;
        assertThrows(IOException.class,()->new RatisTopologySourceSnapshotArtifacts(owned,limits,4*image,2*metadata,1));
        assertThrows(IOException.class,()->new RatisTopologySourceSnapshotArtifacts(owned,limits,2*image,2*metadata,2));
        assertThrows(IOException.class,()->new RatisTopologySourceSnapshotArtifacts(owned,limits,4*image,metadata,2));
        for(var id:ids){assertFalse(Files.exists(owned.resolve(id+".recovery-reserved")));assertArrayEquals(originals.get(id),Files.readAllBytes(owned.resolve(id+".recovery-reserved.pending")));}
    }
    SnapshotInfo managed(RatisTopologySourceSnapshotService.Lease lease,Path directory)throws IOException {
        Files.createDirectories(directory);var cut=lease.prepared().image().position();Path standard=directory.resolve(SimpleStateMachineStorage.getSnapshotFileName(cut.term(),cut.index()));
        Files.createLink(standard,lease.snapshot().getFiles().getFirst().getPath());return new SingleFileSnapshotInfo(new FileInfo(standard,MD5FileUtil.computeMd5ForFile(standard.toFile())),cut.term(),cut.index());
    }
    @Test void retainCloseColdLookupPreservesExactNativeCutAndRestoresStandardMd5()throws Exception {
        Path owned=artifacts(),original=root.resolve("source-managed"),restored=Files.createDirectories(root.resolve("restored"));
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=fixture.capture(fixture.request(50),NativeRangeImage.IoAdmission.UNLIMITED)){
            var info=managed(lease,original);Path durable;
            try(var helper=open(owned)){var saved=helper.retain(lease.prepared(),info);durable=saved.image();assertTrue(Files.isRegularFile(durable));assertSame(lease.prepared(),saved.prepared());}
            Files.delete(info.getFiles().getFirst().getPath());
            try(var helper=open(owned)){var saved=helper.lookup(lease.prepared().request());assertEquals(lease.prepared().image().position().index(),saved.snapshot().getIndex());assertArrayEquals(lease.prepared().image().sha256(),saved.prepared().image().sha256());
                Path standard=saved.snapshot().getFiles().getFirst().getPath();assertEquals(SimpleStateMachineStorage.getSnapshotFileName(saved.snapshot().getTerm(),saved.snapshot().getIndex()),standard.getFileName().toString());assertEquals(MD5FileUtil.computeMd5ForFile(standard.toFile()),saved.snapshot().getFiles().getFirst().getFileDigest());assertTrue(standard.startsWith(owned));
                assertArrayEquals(lease.prepared().image().sha256(),NativeRangeImage.inspect(standard,limits).sha256());assertTrue(Files.exists(durable));
                assertNull(helper.lookup(fixture.request(51)));
            }
        }
    }
    @Test void forcedReleaseSurvivesPartialCleanupAndNeverDeletesManagedSnapshot()throws Exception {
        Path owned=artifacts(),sourceDir=root.resolve("managed");
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=fixture.capture(fixture.request(50),NativeRangeImage.IoAdmission.UNLIMITED)){
            var info=managed(lease,sourceDir);UUID id=lease.prepared().request().transferId();var deletes=new AtomicInteger();
            SessionJournalFiles.Removal fail=path->{assertTrue(Files.exists(owned.resolve(id+".released")));if(deletes.incrementAndGet()==2)throw new IOException("controlled partial cleanup");Files.delete(path);};
            try(var helper=new RatisTopologySourceSnapshotArtifacts(owned,limits,64*1024*1024,128*1024*1024,4,fail)){helper.retain(lease.prepared(),info);assertThrows(IOException.class,()->helper.released(lease.prepared().request()));assertTrue(Files.exists(owned.resolve(id+".prepared")));}
            try(var helper=open(owned)){helper.released(lease.prepared().request());helper.released(lease.prepared().request());assertFalse(Files.exists(owned.resolve(id+".image")));assertFalse(Files.exists(owned.resolve(id+".prepared")));assertTrue(Files.exists(info.getFiles().getFirst().getPath()));assertThrows(IOException.class,()->helper.lookup(lease.prepared().request()));assertThrows(IOException.class,()->helper.retain(lease.prepared(),info));}
        }
    }
    @Test void originalMetadataSymlinksAndForeignDataCannotBeOverwritten()throws Exception {
        Path owned=artifacts(),managedDir=root.resolve("managed");
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=fixture.capture(fixture.request(50),NativeRangeImage.IoAdmission.UNLIMITED);var helper=open(owned)){
            var info=managed(lease,managedDir);var saved=helper.retain(lease.prepared(),info);var request=lease.prepared().request();
            var changed=new RatisTopologySourceSnapshotRpc.Request(request.transferId(),request.header(),new UUID(0,99),request.receiver(),request.purpose());assertThrows(IOException.class,()->helper.lookup(changed));
            var old=lease.prepared();var foreignMetadata=new RatisTopologySourceSnapshotRpc.Prepared(request,old.image(),old.schema(),old.sourceOwner(),new it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.Configuration(old.sourceCfg().term(),old.sourceCfg().index(),new byte[]{1}),old.schemaRows(),old.metadataFreeze());
            assertThrows(IOException.class,()->helper.retain(foreignMetadata,info));
            byte[] bytes=Files.readAllBytes(saved.image());Files.delete(saved.image());Files.write(saved.image(),new byte[]{1});assertThrows(IOException.class,()->helper.lookup(request));assertThrows(IOException.class,()->helper.released(request));Files.delete(saved.image());Files.write(saved.image(),bytes);
            Path foreign=root.resolve("foreign");Files.write(foreign,bytes);Files.delete(saved.image());Files.createSymbolicLink(saved.image(),foreign);assertThrows(IOException.class,()->helper.lookup(request));assertThrows(IOException.class,()->helper.released(request));assertTrue(Files.exists(foreign));
        }
    }
    @Test void imageAndMetadataQuotaFailBeforeAnyDurableMutation()throws Exception {
        Path owned=artifacts(),managedDir=root.resolve("managed");
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=fixture.capture(fixture.request(50),NativeRangeImage.IoAdmission.UNLIMITED)){var info=managed(lease,managedDir);
            try(var helper=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*lease.prepared().image().bytes()-1,128*1024*1024,4)){assertThrows(IOException.class,()->helper.retain(lease.prepared(),info));}
            try(var helper=new RatisTopologySourceSnapshotArtifacts(owned,limits,64*1024*1024,1,4)){assertThrows(IOException.class,()->helper.retain(lease.prepared(),info));}
            try(var files=Files.list(owned)){assertEquals(List.of("artifacts.lock"),files.map(path->path.getFileName().toString()).sorted().toList());}
        }
    }
    @Test void forcedMetadataRepairsExactOwnedPendingCopyAndRejectsUncertainPrefix()throws Exception {
        Path owned=artifacts(),managedDir=root.resolve("managed");
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=fixture.capture(fixture.request(50),NativeRangeImage.IoAdmission.UNLIMITED)){var info=managed(lease,managedDir);var request=lease.prepared().request();Path durable;
            try(var helper=open(owned)){durable=helper.retain(lease.prepared(),info).image();}
            byte[] bytes=Files.readAllBytes(durable);Files.delete(durable);Path pending=durable.resolveSibling(durable.getFileName()+".pending");Files.write(pending,Arrays.copyOf(bytes,64));
            try(var helper=open(owned)){var repaired=helper.lookup(request);assertArrayEquals(lease.prepared().image().sha256(),NativeRangeImage.inspect(repaired.image(),limits).sha256());}
            Files.delete(durable);Files.write(pending,new byte[]{99});try(var helper=open(owned)){assertThrows(IOException.class,()->helper.lookup(request));assertArrayEquals(new byte[]{99},Files.readAllBytes(pending));}
        }
    }
    @Test void completeForcedMetadataSidecarRecoversWithoutAChangedCapture()throws Exception {
        Path owned=artifacts(),managedDir=root.resolve("managed");
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("source"));var lease=fixture.capture(fixture.request(50),NativeRangeImage.IoAdmission.UNLIMITED)){var info=managed(lease,managedDir);var request=lease.prepared().request();
            try(var helper=open(owned)){helper.retain(lease.prepared(),info);}
            Path metadata=owned.resolve(request.transferId()+".prepared");Path pending=metadata.resolveSibling(metadata.getFileName()+".pending");Files.move(metadata,pending);
            try(var helper=open(owned)){var saved=helper.lookup(request);assertArrayEquals(lease.prepared().image().sha256(),saved.prepared().image().sha256());assertTrue(Files.exists(metadata));assertFalse(Files.exists(pending));}
        }
    }

    @Test void serviceStopClosesLivePinWithoutDurablyReleasingColdArtifact()throws Exception {
        Path owned=artifacts();
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("source"))){var request=fixture.request(50);
            try(var helper=open(owned)){
                var host=new RatisTopologySourceSnapshotService.Host(){
                    public RatisTopologySourceSnapshotRpc.Abandoned abandoned(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return fixture.abandoned(request);}
                    public RatisTopologySourceSnapshotRpc.Abandoned abandon(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return fixture.abandon(request);}
                    public RatisTopologySourceSnapshotService.Lease capture(RatisTopologySourceSnapshotRpc.Request input,NativeRangeImage.IoAdmission admission)throws Exception{
                        var held=fixture.capture(input,admission);boolean adopted=false;
                        try{var saved=helper.retain(held.prepared(),managed(held,fixture.image.getParent()));var result=new RatisTopologySourceSnapshotService.Lease(saved.prepared(),held.source(),saved.snapshot(),held.retained());adopted=true;return result;}finally{if(!adopted)held.close();}
                    }
                    public void requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireOwner(lease);}
                    public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireReceiverInstalled(lease);}
                    public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.released(lease);helper.released(lease.prepared().request());}
                    public boolean isReleased(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return helper.isReleased(request);}
                    public org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto send(RatisTopologySourceSnapshotRpc.Request scope,org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto request)throws IOException{return fixture.send(scope,request);}
                };
                try(var service=new RatisTopologySourceSnapshotService(host,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){service.prepare(request);service.close();assertEquals(1,fixture.pinCloses.get());assertEquals(0,fixture.durableReleases.get());}
            }
            try(var helper=open(owned)){assertNotNull(helper.lookup(request));assertFalse(Files.exists(owned.resolve(request.transferId()+".released")));}
        }
    }

    @Test void lostReleaseResponseReplaysExistingReceiptAfterHelperAndServiceRestart()throws Exception {
        Path owned=artifacts();
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("source"))){var request=fixture.request(50);
            try(var helper=open(owned)){
                var host=new RatisTopologySourceSnapshotService.Host(){
                    public RatisTopologySourceSnapshotRpc.Abandoned abandoned(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return fixture.abandoned(request);}
                    public RatisTopologySourceSnapshotRpc.Abandoned abandon(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return fixture.abandon(request);}
                    public RatisTopologySourceSnapshotService.Lease capture(RatisTopologySourceSnapshotRpc.Request input,NativeRangeImage.IoAdmission admission)throws Exception{
                        var held=fixture.capture(input,admission);boolean adopted=false;
                        try{var saved=helper.retain(held.prepared(),managed(held,fixture.image.getParent()));var result=new RatisTopologySourceSnapshotService.Lease(saved.prepared(),held.source(),saved.snapshot(),held.retained());adopted=true;return result;}finally{if(!adopted)held.close();}
                    }
                    public void requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireOwner(lease);}
                    public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireReceiverInstalled(lease);}
                    public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.released(lease);helper.released(lease.prepared().request());}
                    public boolean isReleased(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return helper.isReleased(request);}
                    public org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto send(RatisTopologySourceSnapshotRpc.Request scope,org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto request)throws IOException{return fixture.send(scope,request);}
                };
                try(var service=new RatisTopologySourceSnapshotService(host,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){service.prepare(request);service.transfer(request).get(5,TimeUnit.SECONDS);service.release(request);}
            }
            try(var helper=open(owned)){
                var replayOnly=new RatisTopologySourceSnapshotService.Host(){
                    public RatisTopologySourceSnapshotRpc.Abandoned abandoned(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return fixture.abandoned(request);}
                    public RatisTopologySourceSnapshotRpc.Abandoned abandon(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return fixture.abandon(request);}
                    public RatisTopologySourceSnapshotService.Lease capture(RatisTopologySourceSnapshotRpc.Request request,NativeRangeImage.IoAdmission admission){throw new AssertionError("Cold release cannot recapture");}
                    public void requireOwner(RatisTopologySourceSnapshotService.Lease lease){throw new AssertionError("Cold release cannot acquire owner permission");}
                    public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease){throw new AssertionError("Cold release cannot create a new installation proof");}
                    public void released(RatisTopologySourceSnapshotService.Lease lease){throw new AssertionError("Cold release cannot create a receipt");}
                    public boolean isReleased(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return helper.isReleased(request);}
                    public org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto send(RatisTopologySourceSnapshotRpc.Request scope,org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto request){throw new AssertionError("Cold release cannot send chunks");}
                };
                try(var service=new RatisTopologySourceSnapshotService(replayOnly,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){
                    var changed=new RatisTopologySourceSnapshotRpc.Request(request.transferId(),request.header(),new UUID(0,99),request.receiver(),request.purpose());
                    assertThrows(IOException.class,()->service.release(changed));service.release(request);service.release(request);
                    assertThrows(IOException.class,()->service.release(fixture.request(51)));assertFalse(helper.isReleased(fixture.request(51)));
                    assertEquals(1,fixture.captures.get());assertEquals(1,fixture.pinCloses.get());assertEquals(1,fixture.durableReleases.get());assertEquals(0,service.stats().leases());
                }
            }
        }
    }

}
