package it.cavallium.rockserver.distributed.ratis;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.concurrent.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

class RatisTopologySourceSnapshotServiceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisTopologySourceSnapshotService.Limits limits(){return RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024);}
    @Test void abandonedReplayAtFullImageBudgetOwnsOnlyBoundedMetadata()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            var old=fixture.request(49);var next=fixture.request(50);var terminal=new Abandoned(old,next.transferId(),fixture.store.applied(),fixture.donor,new byte[32]);var entered=new CountDownLatch(1);var unblock=new CountDownLatch(1);
            var host=new RatisTopologySourceSnapshotService.Host(){
                public RatisTopologySourceSnapshotService.Lease capture(Request request)throws Exception{return fixture.capture(request);}
                public Abandoned abandoned(Request request)throws Exception{if(request.transferId().equals(old.transferId())){entered.countDown();assertTrue(unblock.await(5,TimeUnit.SECONDS));return terminal;}return null;}
                public Abandoned abandon(Request request)throws Exception{return fixture.abandon(request);}
                public void requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireOwner(lease);}
                public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireReceiverInstalled(lease);}
                public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.released(lease);}
                public boolean isReleased(Request request)throws Exception{return fixture.isReleased(request);}
                public InstallSnapshotReplyProto send(InstallSnapshotRequestProto request)throws IOException{return fixture.send(request);}
            };
            try(var service=new RatisTopologySourceSnapshotService(host,new RatisTopologySourceSnapshotService.Limits(1,8L<<20,8L<<20,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES));var executor=Executors.newSingleThreadExecutor()){
                service.prepare(next);long imageCharge=service.stats().reservedImageBytes(),metadataCharge=service.stats().metadataBytes();var actual=executor.submit(()->service.prepare(old));assertTrue(entered.await(5,TimeUnit.SECONDS));assertEquals(imageCharge,service.stats().reservedImageBytes());assertTrue(service.stats().metadataBytes()>metadataCharge);assertEquals(1,service.stats().active());assertThrows(IOException.class,()->service.prepare(old));unblock.countDown();var failure=assertThrows(ExecutionException.class,()->actual.get(5,TimeUnit.SECONDS));assertInstanceOf(AbandonedException.class,failure.getCause());assertEquals(terminal,((AbandonedException)failure.getCause()).receipt());assertEquals(imageCharge,service.stats().reservedImageBytes());assertEquals(metadataCharge,service.stats().metadataBytes());assertEquals(1,fixture.captures.get());assertThrows(AbandonedException.class,()->service.prepare(old));assertEquals(1,fixture.captures.get());
            }
            assertEquals(1,fixture.pinCloses.get());
        }
    }
    @Test void realNativeImageUsesReleasedChunksAndPinsUntilQualifiedRelease()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits())){
            var request=fixture.request(50);var prepared=service.prepare(request);assertSame(prepared,service.prepare(request));assertEquals(1,fixture.captures.get());
            assertSame(prepared,service.transfer(request));assertTrue(fixture.calls.get()>=2);assertEquals(1,fixture.proofs.get());assertEquals(prepared.image().bytes(),fixture.received.size());
            assertArrayEquals(prepared.image().sha256(),MessageDigest.getInstance("SHA-256").digest(fixture.received.toByteArray()));assertEquals(0,fixture.pinCloses.get());assertEquals(0,fixture.imageDescriptors());
            int sent=fixture.calls.get();assertSame(prepared,service.transfer(request));assertEquals(sent,fixture.calls.get());
            service.release(request);service.release(request);assertEquals(1,fixture.pinCloses.get());assertEquals(0,service.stats().leases());assertEquals(0,service.stats().metadataBytes());assertEquals(0,service.stats().reservedImageBytes());
            assertThrows(IOException.class,()->service.prepare(request));
        }
    }
    @Test void rpcFailureClosesChunkReaderAndRetriesExactHeldImageWithoutRecapture()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits())){
            var request=fixture.request(50);var prepared=service.prepare(request);fixture.failFirst=true;
            assertThrows(IOException.class,()->service.transfer(request));assertEquals(0,fixture.imageDescriptors());assertEquals(0,fixture.pinCloses.get());assertThrows(IOException.class,()->service.release(request));
            assertSame(prepared,service.transfer(request));assertEquals(1,fixture.captures.get());service.release(request);assertEquals(1,fixture.pinCloses.get());
        }
    }
    @Test void unknownAndAlreadyInstalledRepliesRequireExactNativeReceiverQualification()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits())){
            var request=fixture.request(50);service.prepare(request);fixture.unknownReply=true;
            assertThrows(IOException.class,()->service.transfer(request));assertEquals(0,fixture.proofs.get());assertEquals(0,fixture.imageDescriptors());
            fixture.unknownReply=false;fixture.alreadyInstalled=true;fixture.received.writeBytes(Files.readAllBytes(fixture.image));fixture.installed=false;
            assertThrows(IOException.class,()->service.transfer(request));assertThrows(IOException.class,()->service.release(request));assertEquals(0,fixture.pinCloses.get());
            fixture.installed=true;service.transfer(request);assertEquals(1,fixture.captures.get());service.release(request);assertEquals(1,fixture.pinCloses.get());
        }
    }
    @Test void cancelledObserverAndStopRetainPinUntilActualChunkRpcReturns()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits());var executor=Executors.newSingleThreadExecutor()){
            var request=fixture.request(50);service.prepare(request);fixture.entered=new CountDownLatch(1);fixture.unblock=new CountDownLatch(1);
            var actual=new CompletableFuture<Prepared>();executor.execute(()->{try{actual.complete(service.transfer(request));}catch(Throwable error){actual.completeExceptionally(error);}});
            assertTrue(fixture.entered.await(5,TimeUnit.SECONDS));var observer=actual.copy();assertTrue(observer.cancel(true));assertFalse(actual.isCancelled());
            assertThrows(IOException.class,()->service.release(request));assertEquals(0,fixture.durableReleases.get());assertEquals(0,fixture.releaseLookups.get());assertThrows(IOException.class,service::close);assertEquals(0,fixture.durableReleases.get());assertEquals(0,fixture.pinCloses.get());assertEquals(1,service.stats().active());
            fixture.unblock.countDown();assertThrows(ExecutionException.class,()->actual.get(5,TimeUnit.SECONDS));assertEquals(1,fixture.pinCloses.get());assertEquals(0,fixture.imageDescriptors());assertEquals(0,service.stats().leases());service.close();
        }
    }
    @Test void declaredMetadataQuotaRejectsSecondCaptureBeforeHostWorkAndStopDrainsFirst()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var executor=Executors.newSingleThreadExecutor()){
            var entered=new CountDownLatch(1);var unblock=new CountDownLatch(1);
            var host=new RatisTopologySourceSnapshotService.Host(){
                    public RatisTopologySourceSnapshotRpc.Abandoned abandoned(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return fixture.abandoned(request);}
                    public RatisTopologySourceSnapshotRpc.Abandoned abandon(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return fixture.abandon(request);}
                public RatisTopologySourceSnapshotService.Lease capture(Request request)throws Exception{entered.countDown();assertTrue(unblock.await(5,TimeUnit.SECONDS));return fixture.capture(request);}
                public void requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireOwner(lease);}
                public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireReceiverInstalled(lease);}
                public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.released(lease);}
                public boolean isReleased(Request request)throws Exception{return fixture.isReleased(request);}
                public InstallSnapshotReplyProto send(InstallSnapshotRequestProto request)throws IOException{return fixture.send(request);}
            };
            var limits=new RatisTopologySourceSnapshotService.Limits(4,8*1024*1024,32*1024*1024,3L*MAX_REPLY_BYTES);
            try(var service=new RatisTopologySourceSnapshotService(host,limits)){
                var actual=new CompletableFuture<Prepared>();executor.execute(()->{try{actual.complete(service.prepare(fixture.request(50)));}catch(Throwable error){actual.completeExceptionally(error);}});
                assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(IOException.class,()->service.prepare(fixture.request(51)));assertEquals(0,fixture.captures.get());
                assertThrows(IOException.class,service::close);unblock.countDown();assertThrows(ExecutionException.class,()->actual.get(5,TimeUnit.SECONDS));assertEquals(1,fixture.pinCloses.get());assertEquals(0,service.stats().metadataBytes());service.close();
            }
        }
    }
    @Test void countAndCapturedImageLimitsConserveAllCharges()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            try(var service=new RatisTopologySourceSnapshotService(fixture,new RatisTopologySourceSnapshotService.Limits(1,8*1024*1024,8*1024*1024,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES))){
                service.prepare(fixture.request(50));assertThrows(IOException.class,()->service.prepare(fixture.request(51)));assertEquals(1,fixture.captures.get());
            }
            assertEquals(1,fixture.pinCloses.get());
        }
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("small"));var service=new RatisTopologySourceSnapshotService(fixture,new RatisTopologySourceSnapshotService.Limits(1,1,1,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES))){
            assertThrows(IOException.class,()->service.prepare(fixture.request(52)));assertEquals(1,fixture.pinCloses.get());assertEquals(0,service.stats().active());service.close();assertEquals(0,service.stats().reservedImageBytes());
        }
    }
    @Test void failedDurableReleaseKeepsExactPinAndChargesUntilRetrySucceeds()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits())){
            var request=fixture.request(50);service.prepare(request);service.transfer(request);var held=service.stats();fixture.failRelease=true;
            assertThrows(IOException.class,()->service.release(request));assertEquals(1,fixture.durableReleases.get());assertEquals(0,fixture.pinCloses.get());assertEquals(held.metadataBytes(),service.stats().metadataBytes());assertEquals(held.reservedImageBytes(),service.stats().reservedImageBytes());
            fixture.failRelease=false;service.release(request);service.release(request);assertEquals(2,fixture.durableReleases.get());assertEquals(1,fixture.pinCloses.get());assertEquals(0,service.stats().leases());
        }
    }

    @Test void coldReleaseMetadataQuotaRejectsBeforeHostLookup()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,new RatisTopologySourceSnapshotService.Limits(4,8*1024*1024,32*1024*1024,3L*MAX_REPLY_BYTES))){
            assertThrows(IOException.class,()->service.release(fixture.request(50)));assertEquals(0,fixture.releaseLookups.get());assertEquals(0,fixture.captures.get());assertEquals(0,service.stats().active());assertEquals(0,service.stats().metadataBytes());
        }
    }
    @Test void stoppedOwnerRejectsNewColdReleaseLookupWithoutHostWork()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits())){
            service.close();assertThrows(IOException.class,()->service.release(fixture.request(50)));assertEquals(0,fixture.releaseLookups.get());assertEquals(0,fixture.captures.get());assertEquals(0,service.stats().active());assertEquals(0,service.stats().metadataBytes());
        }
    }

}
