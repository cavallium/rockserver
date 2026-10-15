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
    @Test void oneBatchWorkerMakesRecoveryProgressWhileBootstrapHasNoRateCredits()throws Exception {
        var scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,16,16,"one-source-build-worker");
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            var bootstrap=fixture.request(50);var recovery=new Request(new java.util.UUID(0,52),bootstrap.header(),bootstrap.catalogue(),fixture.donor,Purpose.RECOVERY);
            var denied=new CountDownLatch(1);var allow=new java.util.concurrent.atomic.AtomicBoolean();var controller=new RatisMigrationController(RatisMigrationController.Policy.defaults(),java.util.Set.of(new java.util.UUID(0,100)),System::nanoTime,()->RatisDiskRunway.State.EMERGENCY);
            var host=(RatisTopologySourceSnapshotService.Host)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RatisTopologySourceSnapshotService.Host.class},(proxy,method,args)->{
                if(method.getName().equals("chunkReady") && args[0].equals(bootstrap)){denied.countDown();return allow.get();}
                try{return method.invoke(fixture,args);}catch(java.lang.reflect.InvocationTargetException failed){throw failed.getCause();}
            });
            RatisTopologySourceSnapshotService.ResourceAdmission admission=(request,bytes,control)->control?fixture.transferAdmission().reserve(request,bytes,true):request.purpose()==Purpose.RECOVERY?controller.reserveRecovery(new java.util.UUID(0,100),bytes):fixture.transferAdmission().reserve(request,bytes,false);
            try(var service=new RatisTopologySourceSnapshotService(host,limits(),scheduler,admission)){
                var waiting=service.prepareAsync(bootstrap);assertTrue(denied.await(5,TimeUnit.SECONDS));
                try(var saturated=fixture.graph.reserve(new java.util.UUID(0,100),RatisNodeResourceBudget.Kind.ORDINARY,(256L<<20)-fixture.graph.stats().ordinary().bytes(),1)){
                    var completed=service.prepareAsync(recovery).get(5,TimeUnit.SECONDS);assertEquals(Purpose.RECOVERY,completed.request().purpose());assertFalse(waiting.isDone());assertEquals(0,controller.usage().recoveryOwners());
                }
                allow.set(true);waiting.get(5,TimeUnit.SECONDS);service.stop().get(5,TimeUnit.SECONDS);assertEquals(0,fixture.graph.stats().ordinary().owners());
            }finally{allow.set(true);}
        }finally{scheduler.dispose();}
    }
    @Test void heldBatchBuildLeavesReleaseIndependentAndStopWaitsForActualCapture()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            var entered=new CountDownLatch(1);var unblock=new CountDownLatch(1);var first=fixture.request(50);var second=fixture.request(51);
            var host=(RatisTopologySourceSnapshotService.Host)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RatisTopologySourceSnapshotService.Host.class},(proxy,method,args)->{
                if(method.getName().equals("capture") && args[0].equals(second)){entered.countDown();assertTrue(unblock.await(5,TimeUnit.SECONDS));}
                try{return method.invoke(fixture,args);}catch(java.lang.reflect.InvocationTargetException failed){throw failed.getCause();}
            });
            try(var service=new RatisTopologySourceSnapshotService(host,limits(),fixture.scheduler,fixture.transferAdmission())){
                service.prepare(first);service.transfer(first).get(5,TimeUnit.SECONDS);
                var actual=service.prepareAsync(second);assertTrue(entered.await(5,TimeUnit.SECONDS));assertTrue(actual.copy().cancel(true));
                service.release(first);assertEquals(1,fixture.durableReleases.get());assertEquals(1,fixture.pinCloses.get());
                var stopped=service.stop();assertFalse(stopped.isDone());assertEquals(1,fixture.graph.stats().ordinary().owners());
                unblock.countDown();assertThrows(ExecutionException.class,()->actual.get(5,TimeUnit.SECONDS));stopped.get(5,TimeUnit.SECONDS);
                assertEquals(0,fixture.graph.stats().ordinary().owners());assertEquals(0,service.stats().leases());
            }finally{unblock.countDown();}
        }
    }
    @Test void abandonedReplayAtFullImageBudgetOwnsOnlyBoundedMetadata()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            var old=fixture.request(49);var next=fixture.request(50);var terminal=new Abandoned(old,next.transferId(),fixture.store.applied(),fixture.donor,new byte[32]);var entered=new CountDownLatch(1);var unblock=new CountDownLatch(1);
            var host=new RatisTopologySourceSnapshotService.Host(){
                public RatisTopologySourceSnapshotService.Lease capture(Request request,it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.IoAdmission admission)throws Exception{return fixture.capture(request,admission);}
                public Abandoned abandoned(Request request)throws Exception{if(request.transferId().equals(old.transferId())){entered.countDown();assertTrue(unblock.await(5,TimeUnit.SECONDS));return terminal;}return null;}
                public Abandoned abandon(Request request)throws Exception{return fixture.abandon(request);}
                public org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Gate.SnapshotPermit requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{return fixture.requireOwner(lease);}
                public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireReceiverInstalled(lease);}
                public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.released(lease);}
                public boolean isReleased(Request request)throws Exception{return fixture.isReleased(request);}
                public InstallSnapshotReplyProto send(RatisTopologySourceSnapshotRpc.Request scope,InstallSnapshotRequestProto request)throws IOException{return fixture.send(scope,request);}
            };
            try(var service=new RatisTopologySourceSnapshotService(host,new RatisTopologySourceSnapshotService.Limits(1,8L<<20,8L<<20,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES),fixture.scheduler,fixture.transferAdmission());var executor=Executors.newSingleThreadExecutor()){
                service.prepare(next);long imageCharge=service.stats().reservedImageBytes(),metadataCharge=service.stats().metadataBytes();var actual=executor.submit(()->service.prepare(old));assertTrue(entered.await(5,TimeUnit.SECONDS));assertEquals(imageCharge,service.stats().reservedImageBytes());assertTrue(service.stats().metadataBytes()>metadataCharge);assertEquals(1,service.stats().active());assertThrows(IOException.class,()->service.prepare(old));unblock.countDown();var failure=assertThrows(ExecutionException.class,()->actual.get(5,TimeUnit.SECONDS));assertInstanceOf(AbandonedException.class,failure.getCause());assertEquals(terminal,((AbandonedException)failure.getCause()).receipt());assertEquals(imageCharge,service.stats().reservedImageBytes());assertEquals(metadataCharge,service.stats().metadataBytes());assertEquals(1,fixture.captures.get());assertThrows(AbandonedException.class,()->service.prepare(old));assertEquals(1,fixture.captures.get());
            }
            assertEquals(1,fixture.pinCloses.get());
        }
    }
    @Test void realNativeImageUsesReleasedChunksAndPinsUntilQualifiedRelease()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission())){
            var request=fixture.request(50);var prepared=service.prepare(request);assertSame(prepared,service.prepare(request));assertEquals(1,fixture.captures.get());
            assertSame(prepared,service.transfer(request).get(5,TimeUnit.SECONDS));assertTrue(fixture.calls.get()>=2);assertEquals(1,fixture.proofs.get());assertEquals(prepared.image().bytes(),fixture.received.size());
            assertArrayEquals(prepared.image().sha256(),MessageDigest.getInstance("SHA-256").digest(fixture.received.toByteArray()));assertEquals(0,fixture.pinCloses.get());assertEquals(0,fixture.imageDescriptors());
            int sent=fixture.calls.get();assertSame(prepared,service.transfer(request).get(5,TimeUnit.SECONDS));assertEquals(sent,fixture.calls.get());
            service.release(request);service.release(request);assertEquals(1,fixture.pinCloses.get());assertEquals(0,service.stats().leases());assertEquals(0,service.stats().metadataBytes());assertEquals(0,service.stats().reservedImageBytes());
            assertThrows(IOException.class,()->service.prepare(request));
        }
    }
    @Test void rpcFailureClosesChunkReaderAndRetriesExactHeldImageWithoutRecapture()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission())){
            var request=fixture.request(50);var prepared=service.prepare(request);fixture.failFirst=true;
            assertThrows(ExecutionException.class,()->service.transfer(request).get(5,TimeUnit.SECONDS));assertEquals(0,fixture.imageDescriptors());assertEquals(0,fixture.pinCloses.get());assertThrows(IOException.class,()->service.release(request));
            assertSame(prepared,service.transfer(request).get(5,TimeUnit.SECONDS));assertEquals(1,fixture.captures.get());service.release(request);assertEquals(1,fixture.pinCloses.get());
        }
    }
    @Test void unknownAndAlreadyInstalledRepliesRequireExactNativeReceiverQualification()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission())){
            var request=fixture.request(50);service.prepare(request);fixture.unknownReply=true;
            assertThrows(ExecutionException.class,()->service.transfer(request).get(5,TimeUnit.SECONDS));assertEquals(0,fixture.proofs.get());assertEquals(0,fixture.imageDescriptors());
            fixture.unknownReply=false;fixture.alreadyInstalled=true;fixture.received.writeBytes(Files.readAllBytes(fixture.image));fixture.installed=false;
            assertThrows(ExecutionException.class,()->service.transfer(request).get(5,TimeUnit.SECONDS));assertThrows(IOException.class,()->service.release(request));assertEquals(0,fixture.pinCloses.get());
            fixture.installed=true;service.transfer(request).get(5,TimeUnit.SECONDS);assertEquals(1,fixture.captures.get());service.release(request);assertEquals(1,fixture.pinCloses.get());
        }
    }
    @Test void cancelledObserverAndStopRetainPinUntilActualChunkRpcReturns()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission());var executor=Executors.newSingleThreadExecutor()){
            var request=fixture.request(50);service.prepare(request);fixture.entered=new CountDownLatch(1);fixture.unblock=new CountDownLatch(1);
            var actual=service.transfer(request);
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
                public RatisTopologySourceSnapshotService.Lease capture(Request request,it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.IoAdmission admission)throws Exception{entered.countDown();assertTrue(unblock.await(5,TimeUnit.SECONDS));return fixture.capture(request,admission);}
                public org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Gate.SnapshotPermit requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{return fixture.requireOwner(lease);}
                public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.requireReceiverInstalled(lease);}
                public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{fixture.released(lease);}
                public boolean isReleased(Request request)throws Exception{return fixture.isReleased(request);}
                public InstallSnapshotReplyProto send(RatisTopologySourceSnapshotRpc.Request scope,InstallSnapshotRequestProto request)throws IOException{return fixture.send(scope,request);}
            };
            var limits=new RatisTopologySourceSnapshotService.Limits(4,8*1024*1024,32*1024*1024,3L*MAX_REPLY_BYTES);
            try(var service=new RatisTopologySourceSnapshotService(host,limits,fixture.scheduler,fixture.transferAdmission())){
                var actual=new CompletableFuture<Prepared>();executor.execute(()->{try{actual.complete(service.prepare(fixture.request(50)));}catch(Throwable error){actual.completeExceptionally(error);}});
                assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(IOException.class,()->service.prepare(fixture.request(51)));assertEquals(0,fixture.captures.get());
                assertThrows(IOException.class,service::close);unblock.countDown();assertThrows(ExecutionException.class,()->actual.get(5,TimeUnit.SECONDS));assertEquals(1,fixture.pinCloses.get());assertEquals(0,service.stats().metadataBytes());service.close();
            }
        }
    }
    @Test void countAndCapturedImageLimitsConserveAllCharges()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            try(var service=new RatisTopologySourceSnapshotService(fixture,new RatisTopologySourceSnapshotService.Limits(1,8*1024*1024,8*1024*1024,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES),fixture.scheduler,fixture.transferAdmission())){
                service.prepare(fixture.request(50));assertThrows(IOException.class,()->service.prepare(fixture.request(51)));assertEquals(1,fixture.captures.get());
            }
            assertEquals(1,fixture.pinCloses.get());
        }
        try(var fixture=new RatisTopologySourceSnapshotFixture(root.resolve("small"));var service=new RatisTopologySourceSnapshotService(fixture,new RatisTopologySourceSnapshotService.Limits(1,1,1,RatisTopologySourceSnapshotService.MAX_METADATA_BYTES),fixture.scheduler,fixture.transferAdmission())){
            assertThrows(IOException.class,()->service.prepare(fixture.request(52)));assertEquals(1,fixture.pinCloses.get());assertEquals(0,service.stats().active());service.close();assertEquals(0,service.stats().reservedImageBytes());
        }
    }
    @Test void failedDurableReleaseKeepsExactPinAndChargesUntilRetrySucceeds()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission())){
            var request=fixture.request(50);service.prepare(request);service.transfer(request).get(5,TimeUnit.SECONDS);var held=service.stats();fixture.failRelease=true;
            assertThrows(IOException.class,()->service.release(request));assertEquals(1,fixture.durableReleases.get());assertEquals(0,fixture.pinCloses.get());assertEquals(held.metadataBytes(),service.stats().metadataBytes());assertEquals(held.reservedImageBytes(),service.stats().reservedImageBytes());
            fixture.failRelease=false;service.release(request);service.release(request);assertEquals(2,fixture.durableReleases.get());assertEquals(1,fixture.pinCloses.get());assertEquals(0,service.stats().leases());
        }
    }

    @Test void coldReleaseMetadataQuotaRejectsBeforeHostLookup()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,new RatisTopologySourceSnapshotService.Limits(4,8*1024*1024,32*1024*1024,3L*maximumPreparedBytes(fixture.request(50))),fixture.scheduler,fixture.transferAdmission())){
            assertThrows(IOException.class,()->service.release(fixture.request(50)));assertEquals(0,fixture.releaseLookups.get());assertEquals(0,fixture.captures.get());assertEquals(0,service.stats().active());assertEquals(0,service.stats().metadataBytes());
        }
    }
    @Test void stoppedOwnerRejectsNewColdReleaseLookupWithoutHostWork()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission())){
            service.close();assertThrows(IOException.class,()->service.release(fixture.request(50)));assertEquals(0,fixture.releaseLookups.get());assertEquals(0,fixture.captures.get());assertEquals(0,service.stats().active());assertEquals(0,service.stats().metadataBytes());
        }
    }

    @Test void pressuredCancelledTransferKeepsOrdinaryBytesAndReaderWhileSettledReleaseProgresses()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission())){
            var settled=fixture.request(49);service.prepare(settled);service.transfer(settled).get(5,TimeUnit.SECONDS);
            var request=fixture.request(50);var original=service.prepare(request);fixture.scheduler.setStoragePressure(true);fixture.entered=new CountDownLatch(1);fixture.unblock=new CountDownLatch(1);
            var observer=service.transfer(request);assertTrue(fixture.entered.await(5,TimeUnit.SECONDS));
            try {
                assertTrue(fixture.scheduler.isStoragePressure());assertEquals(RatisTopologySourceSnapshotService.transferBytes(request),fixture.graph.stats().ordinary().bytes());assertTrue(fixture.imageDescriptors()>0);assertTrue(observer.cancel(false));
                service.release(settled);assertEquals(1,fixture.durableReleases.get());assertEquals(1,fixture.pinCloses.get());assertEquals(1,service.stats().active());
                var stop=service.stop();assertFalse(stop.isDone());assertEquals(RatisTopologySourceSnapshotService.transferBytes(request),fixture.graph.stats().ordinary().bytes());assertEquals(original.image().bytes(),java.nio.file.Files.size(fixture.image));
                fixture.unblock.countDown();stop.get(5,TimeUnit.SECONDS);assertEquals(0,fixture.graph.stats().ordinary().bytes());assertEquals(0,fixture.imageDescriptors());assertEquals(2,fixture.pinCloses.get());assertEquals(0,service.stats().leases());
            } finally {fixture.unblock.countDown();service.stop().get(5,TimeUnit.SECONDS);}
        }
    }
    @Test void transferAdmissionFailureDoesNotOpenReaderOrRecaptureAndExactImageRetries()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission())){
            var request=fixture.request(50);var original=service.prepare(request);
            try(var occupied=fixture.graph.reserve(new java.util.UUID(0,100),RatisNodeResourceBudget.Kind.ORDINARY,256L<<20,1)){
                assertThrows(ExecutionException.class,()->service.transfer(request).get(5,TimeUnit.SECONDS));assertEquals(0,fixture.calls.get());assertEquals(0,fixture.imageDescriptors());assertEquals(0,service.stats().active());assertEquals(1,fixture.captures.get());
            }
            var retry=service.transfer(request).get(5,TimeUnit.SECONDS);assertArrayEquals(original.image().sha256(),retry.image().sha256());assertEquals(1,fixture.captures.get());service.release(request);
        }
    }

    @Test void asyncTransferDoesNotParkSingleTopologyProofWorkerOrBlockAnotherRelease()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission())){
            var proofs=RatisProofQueryService.topologyImageLane(RatisProofFixtures.admission());var peer=org.apache.ratis.protocol.RaftPeer.newBuilder().setId(fixture.donor.peerId()).setAddress(fixture.donor.address()).build();var group=org.apache.ratis.protocol.RaftGroup.valueOf(org.apache.ratis.protocol.RaftGroupId.valueOf(fixture.group),peer);
            var storage=org.apache.ratis.server.storage.RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();
            var machine=new RatisRangeStateMachine(fixture.store,group,new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.Limits(4<<20,8<<20,1));machine.role(RatisNodePlan.Role.DATA);machine.sourceSnapshots(proofs,service);var server=org.mockito.Mockito.mock(org.apache.ratis.server.RaftServer.class);org.mockito.Mockito.when(server.getId()).thenReturn(peer.getId());machine.initialize(server,group.getGroupId(),storage);
            java.util.function.BiFunction<Integer,Request,java.util.concurrent.CompletableFuture<org.apache.ratis.protocol.Message>> query=(kind,input)->machine.queryLinearizable(org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(request(kind,input))));
            var settled=fixture.request(49);var pending=fixture.request(50);
            try {
                query.apply(PREPARE,settled).get(5,TimeUnit.SECONDS);query.apply(TRANSFER,settled).get(5,TimeUnit.SECONDS);query.apply(PREPARE,pending).get(5,TimeUnit.SECONDS);
                fixture.entered=new CountDownLatch(1);fixture.unblock=new CountDownLatch(1);fixture.scheduler.setStoragePressure(true);var transfer=query.apply(TRANSFER,pending);assertTrue(fixture.entered.await(5,TimeUnit.SECONDS));
                assertEquals(0,proofs.stats().jobs(),"Single proof lane is not owned by the blocked send");query.apply(RELEASE,settled).get(5,TimeUnit.SECONDS);assertEquals(1,fixture.durableReleases.get());assertFalse(transfer.isDone());
                fixture.unblock.countDown();transfer.get(5,TimeUnit.SECONDS);query.apply(RELEASE,pending).get(5,TimeUnit.SECONDS);assertEquals(2,fixture.pinCloses.get());
            } finally {if(fixture.unblock!=null)fixture.unblock.countDown();service.stop().get(5,TimeUnit.SECONDS);proofs.stop().get(5,TimeUnit.SECONDS);machine.close();storage.close();}
        }
    }

    @Test void failedActualReaderCloseRetainsQuotaAndSerializedStopRetryDrainsIt()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,limits(),fixture.scheduler,fixture.transferAdmission())){
            var request=fixture.request(50);service.prepare(request);fixture.entered=new CountDownLatch(1);fixture.unblock=new CountDownLatch(1);var observer=service.transfer(request);assertTrue(fixture.entered.await(5,TimeUnit.SECONDS));var closeCalls=new java.util.concurrent.atomic.AtomicInteger();
            synchronized(service){var tasks=service.getClass().getDeclaredField("transfers");tasks.setAccessible(true);var task=((java.util.Set<?>)tasks.get(service)).iterator().next();var chunks=task.getClass().getDeclaredField("chunks");chunks.setAccessible(true);var actual=(org.apache.ratis.server.leader.RockserverSnapshotRequests.Requests)chunks.get(task);
                chunks.set(task,new org.apache.ratis.server.leader.RockserverSnapshotRequests.Requests(){public boolean hasNext(){return actual.hasNext();}public InstallSnapshotRequestProto next(){return actual.next();}public void close()throws IOException{if(closeCalls.incrementAndGet()==1)throw new IOException("Controlled unknown reader close");actual.close();}});
            }
            var stop=service.stop();
            try {
                assertFalse(stop.isDone());fixture.unblock.countDown();assertThrows(ExecutionException.class,()->observer.get(5,TimeUnit.SECONDS));assertFalse(stop.isDone());assertEquals(1,closeCalls.get());assertEquals(RatisTopologySourceSnapshotService.transferBytes(request),fixture.graph.stats().ordinary().bytes());assertTrue(fixture.imageDescriptors()>0);assertEquals(0,fixture.pinCloses.get());
                service.stop().get(5,TimeUnit.SECONDS);stop.get(5,TimeUnit.SECONDS);assertEquals(2,closeCalls.get());assertEquals(0,fixture.graph.stats().ordinary().bytes());assertEquals(0,fixture.imageDescriptors());assertEquals(1,fixture.pinCloses.get());assertEquals(0,fixture.durableReleases.get());
            } finally {fixture.unblock.countDown();service.stop().get(5,TimeUnit.SECONDS);}
        }
    }

}
