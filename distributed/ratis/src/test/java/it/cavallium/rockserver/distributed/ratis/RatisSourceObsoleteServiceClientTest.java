package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Genuine native image pins with controlled authority/cleanup hooks; no receiver-proof or quorum claim. */
class RatisSourceObsoleteServiceClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static UUID id(int n){return new UUID(12050,n);}
    private static RatisRecoveryObsoleteRpc.Request obsolete(RatisTopologySourceSnapshotFixture f,int n) {
        var directory=f.definition.targetDirectory();var range=directory.ranges().getFirst();
        var original=new RatisTopologySourceSnapshotRpc.Request(id(n),null,id(40),f.receiver,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,f.group,directory);
        var left=new RangeDescriptor(id(100),id(101),1,range.configurationVersion(),null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,range.replicas());var right=new RangeDescriptor(id(102),id(103),1,range.configurationVersion(),new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,range.replicas());
        var target=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),directory.generation()+1,directory.columns(),directory.routingPrefixBytes(),List.of(left,right));var consuming=new TopologyChange.Definition(id(104),directory,target);
        byte[] originalDigest=OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,original));
        var scope=new NativeRecoverySupersession.Scope(original.catalogue(),new ApplyPosition(1,20),consuming.header(),directory,f.group,f.receiver,new byte[32],originalDigest,originalDigest);
        return new RatisRecoveryObsoleteRpc.Request(original,scope,new byte[32]);
    }
    private static final class Host implements InvocationHandler {
        final RatisTopologySourceSnapshotFixture fixture;final AtomicInteger proofs=new AtomicInteger(),forces=new AtomicInteger(),cleanups=new AtomicInteger(),closesAtForce=new AtomicInteger();
        volatile boolean failForce,failCleanup,forced,failProof,releasedConflict;volatile CountDownLatch entered,release,forceEntered,forceRelease,captureEntered,captureRelease,parked;volatile boolean failCapture,unknownCaptureFailure,parkTransfer;final Map<UUID,byte[]> durable=new ConcurrentHashMap<>();final Set<UUID> parkedIds=ConcurrentHashMap.newKeySet();
        Host(RatisTopologySourceSnapshotFixture fixture){this.fixture=fixture;}
        RatisTopologySourceSnapshotService.Host proxy(){return (RatisTopologySourceSnapshotService.Host)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RatisTopologySourceSnapshotService.Host.class},this);}
        public Object invoke(Object proxy,Method method,Object[] args)throws Throwable {
            switch(method.getName()) {
                case "retryObsolete"->{var request=(RatisRecoveryObsoleteRpc.Request)args[0];var saved=durable.get(request.original().transferId());if(saved!=null && !Arrays.equals(saved,request.digest()))throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Conflicting terminal");return saved!=null;}
                case "requireObsolete"->{proofs.incrementAndGet();if(failProof)throw new IOException("Wrong receiver fence proof");if(entered!=null){entered.countDown();assertTrue(release.await(5,TimeUnit.SECONDS));}if(failProof)throw new IOException("Wrong receiver fence proof");return null;}
                case "forceObsolete"->{forces.incrementAndGet();if(releasedConflict)throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Canonical installed RELEASE already forced");var request=(RatisRecoveryObsoleteRpc.Request)args[0];if(forceEntered!=null){forceEntered.countDown();assertTrue(forceRelease.await(5,TimeUnit.SECONDS));}closesAtForce.set(fixture.pinCloses.get());if(failForce)throw new IOException("Unknown obsolete force");forced=true;durable.put(request.original().transferId(),request.digest());return null;}
                case "obsolete"->{assertTrue(forced);cleanups.incrementAndGet();if(failCleanup)throw new IOException("Unknown obsolete cleanup");return null;}
                case "capture"->{if(captureEntered!=null){captureEntered.countDown();assertTrue(captureRelease.await(5,TimeUnit.SECONDS));if(failCapture)throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Controlled natural capture rejection");if(unknownCaptureFailure)throw new IOException("Unclassified capture outcome remains unknown");}try{return method.invoke(fixture,args);}catch(InvocationTargetException failed){throw failed.getCause();}}
                case "chunkReady"->{if(parkTransfer){if(parkedIds.add(((RatisTopologySourceSnapshotRpc.Request)args[0]).transferId()))parked.countDown();return false;}return true;}
                default->{try{return method.invoke(fixture,args);}catch(InvocationTargetException failed){throw failed.getCause();}}
            }
        }
    }
    private static RatisTopologySourceSnapshotService.Limits limits(){return RatisTopologySourceSnapshotService.Limits.defaults(8L<<20);}
    @Test void uninstalledLeaseAndUnknownCaptureCanBecomeObsoleteButNeverInstalledReleased()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var request=obsolete(f,1);service.prepare(request.original());assertEquals(0,f.proofs.get());RatisRecoveryObsoleteRpc.reply(service.obsolete(request).get(5,TimeUnit.SECONDS),request);
                assertEquals(1,host.forces.get());assertEquals(0,host.closesAtForce.get(),"Completed lease must stay pinned until terminal force");assertEquals(1,host.cleanups.get());assertEquals(1,f.pinCloses.get());assertEquals(0,service.stats().leases());assertEquals(0,service.stats().reservedImageBytes());assertEquals(0,f.graph.stats().ordinary().owners());assertEquals(0,f.graph.stats().protectedLane().owners());assertEquals(0,f.durableReleases.get());
                RatisRecoveryObsoleteRpc.reply(service.obsolete(request).get(5,TimeUnit.SECONDS),request);assertEquals(1,host.forces.get());assertThrows(IOException.class,()->service.prepare(request.original()));assertThrows(IOException.class,()->service.release(request.original()));
            }
            var unknownHost=new Host(f);try(var service=new RatisTopologySourceSnapshotService(unknownHost.proxy(),limits(),f.scheduler,f.transferAdmission())){var request=obsolete(f,2);RatisRecoveryObsoleteRpc.reply(service.obsolete(request).get(5,TimeUnit.SECONDS),request);assertEquals(1,unknownHost.cleanups.get());assertEquals(1,f.captures.get());}
        }
    }
    @Test void unknownForceSurvivesStopWithoutOrdinaryLeaseRetirementAndExactRetrySettles()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var request=obsolete(f,3);service.prepare(request.original());host.failForce=true;assertThrows(ExecutionException.class,()->service.obsolete(request).get(5,TimeUnit.SECONDS));var held=service.stats();assertEquals(0,f.pinCloses.get());assertEquals(1,f.graph.stats().protectedLane().owners());
                var stopped=service.stop();long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while((host.forces.get()<2 || service.stats().active()!=0) && System.nanoTime()<deadline)Thread.sleep(1);
                assertTrue(host.forces.get()>=2);assertFalse(stopped.isDone());assertEquals(0,f.pinCloses.get());assertEquals(held.reservedImageBytes(),service.stats().reservedImageBytes());assertEquals(held.metadataBytes(),service.stats().metadataBytes());assertEquals(1,f.graph.stats().protectedLane().owners());
                host.failForce=false;host.failProof=true;RatisRecoveryObsoleteRpc.reply(service.obsolete(request).get(5,TimeUnit.SECONDS),request);stopped.get(5,TimeUnit.SECONDS);assertEquals(1,f.pinCloses.get());assertEquals(0,f.graph.stats().ordinary().owners());assertEquals(0,f.graph.stats().protectedLane().owners());assertEquals(0,service.stats().metadataBytes());
            }
        }
    }
    @Test void failedCleanupRetainsQuotaAfterKnownPinCloseUntilExactRetry()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var request=obsolete(f,7);service.prepare(request.original());host.failCleanup=true;
                assertThrows(ExecutionException.class,()->service.obsolete(request).get(5,TimeUnit.SECONDS));assertTrue(host.forced);assertEquals(1,f.pinCloses.get());assertTrue(service.stats().reservedImageBytes()>0);assertEquals(1,f.graph.stats().protectedLane().owners());
                assertThrows(IOException.class,()->service.prepare(request.original()));assertThrows(IOException.class,()->service.release(request.original()));
                host.failCleanup=false;host.failProof=true;int proofs=host.proofs.get();service.obsolete(request).get(5,TimeUnit.SECONDS);assertEquals(proofs,host.proofs.get());assertEquals(1,f.pinCloses.get());assertEquals(0,f.graph.stats().ordinary().owners());assertEquals(0,f.graph.stats().protectedLane().owners());assertEquals(0,service.stats().reservedImageBytes());assertEquals(0,f.durableReleases.get());
            }
        }
    }
    @Test void canceledObserverAndStopJoinActualObsoleteProofBeforePinAndChargesRelease()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);host.forceEntered=new CountDownLatch(1);host.forceRelease=new CountDownLatch(1);
            try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var request=obsolete(f,4);service.prepare(request.original());var observer=service.obsolete(request);
                try{assertTrue(host.forceEntered.await(5,TimeUnit.SECONDS));observer.cancel(false);var stopped=service.stop();assertFalse(stopped.isDone());assertEquals(0,f.pinCloses.get());assertTrue(service.stats().reservedImageBytes()>0);host.forceRelease.countDown();stopped.get(5,TimeUnit.SECONDS);assertEquals(1,f.pinCloses.get());assertEquals(0,f.graph.stats().ordinary().owners());assertEquals(0,f.graph.stats().protectedLane().owners());}finally{host.forceRelease.countDown();}
            }
        }
    }
    @Test void queuedPreparationWithoutEntryIsCanceledAfterQualificationWithoutDeadlockingBatch()throws Exception {
        var scheduler=RWScheduler.forTesting(1,1,1,16,16,"queued-obsolete");var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);scheduler.executor(RequestContext.batch(),OperationFamily.FULL_SCAN_AGGREGATE).execute(()->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("Held batch queue timed out");}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new AssertionError(failure);}},1);
            assertTrue(entered.await(5,TimeUnit.SECONDS));try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),scheduler,f.transferAdmission())) {
                var request=obsolete(f,5);var preparing=service.prepareAsync(request.original());assertEquals(0,service.stats().leases());var obsolete=service.obsolete(request);
                release.countDown();obsolete.get(5,TimeUnit.SECONDS);assertThrows(ExecutionException.class,()->preparing.get(5,TimeUnit.SECONDS));assertEquals(1,host.cleanups.get());assertEquals(0,f.graph.stats().ordinary().owners());assertEquals(0,f.graph.stats().protectedLane().owners());
            }
        }finally{release.countDown();scheduler.dispose();}
    }
    @Test void badProofDoesNotPoisonCurrentUuidAndQualifiedParkedTransferIsActuallyDrained()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var request=obsolete(f,8);service.prepare(request.original());var before=service.stats();host.failProof=true;
                assertThrows(ExecutionException.class,()->service.obsolete(request).get(5,TimeUnit.SECONDS));assertEquals(before,service.stats());assertEquals(0,f.pinCloses.get());assertEquals(0,f.graph.stats().ordinary().owners());assertEquals(0,f.graph.stats().protectedLane().owners());assertEquals(0,host.forces.get());
                host.failProof=false;host.parkTransfer=true;host.parked=new CountDownLatch(1);var transfer=service.transfer(request.original());assertTrue(host.parked.await(5,TimeUnit.SECONDS));
                service.obsolete(request).get(5,TimeUnit.SECONDS);assertThrows(ExecutionException.class,()->transfer.get(5,TimeUnit.SECONDS));assertEquals(1,f.pinCloses.get());assertEquals(0,service.stats().leases());assertEquals(0,f.graph.stats().ordinary().owners());assertEquals(0,f.graph.stats().protectedLane().owners());
            }
        }
    }
    @Test void readyCurrentExchangesItsPrepaidObsoletePromiseAtAnExactMetadataCeiling()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var request=obsolete(f,9);service.prepare(request.original());long promised=service.stats().metadataBytes();assertTrue(promised>=RatisTopologySourceSnapshotService.obsoletePromiseBytes(request.original()));
                // Tighten only the test's policy boundary after genuine READY; never change accounting.
                var field=RatisTopologySourceSnapshotService.class.getDeclaredField("limits");field.setAccessible(true);field.set(service,new RatisTopologySourceSnapshotService.Limits(4,8L<<20,32L<<20,promised));
                service.obsolete(request).get(5,TimeUnit.SECONDS);assertEquals(1,host.cleanups.get());assertEquals(0,service.stats().leases());assertEquals(0,service.stats().reservedImageBytes());
            }
        }
    }
    @Test void naturalCaptureFailureTransfersBorrowedPromiseUntilUntrustedProofIsRejected()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);host.captureEntered=new CountDownLatch(1);host.captureRelease=new CountDownLatch(1);host.failCapture=true;host.entered=new CountDownLatch(1);host.release=new CountDownLatch(1);host.failProof=true;
            try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var request=obsolete(f,10);var preparation=service.prepareAsync(request.original());assertTrue(host.captureEntered.await(5,TimeUnit.SECONDS));
                // Hold proof before rejecting it, so the old capture can fail without being canceled.
                host.failProof=false;var obsolete=service.obsolete(request);assertTrue(host.entered.await(5,TimeUnit.SECONDS));host.captureRelease.countDown();assertThrows(ExecutionException.class,()->preparation.get(5,TimeUnit.SECONDS));
                assertEquals(0,service.stats().leases());assertTrue(service.stats().metadataBytes()>=RatisTopologySourceSnapshotService.obsoletePromiseBytes(request.original()));assertEquals(1,f.graph.stats().protectedLane().owners());
                host.failProof=true;host.release.countDown();assertThrows(ExecutionException.class,()->obsolete.get(5,TimeUnit.SECONDS));assertEquals(0,service.stats().metadataBytes());assertEquals(0,f.graph.stats().ordinary().owners());assertEquals(0,f.graph.stats().protectedLane().owners());
                host.failProof=false;service.obsolete(request).get(5,TimeUnit.SECONDS);assertEquals(1,host.cleanups.get());
            }finally{host.captureRelease.countDown();host.release.countDown();}
        }
    }
    @Test void fullOrdinaryActivityStillAdmitsSeparatelyBoundedObsoleteDrain()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);var policy=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(1L<<30,16),new RatisNodeResourceBudget.Lane(1L<<30,16));var budget=new RatisNodeResourceBudget(policy,Map.of(id(6000),policy));
            try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,(request,bytes,control)->{var lease=budget.reserve(id(6000),control?RatisNodeResourceBudget.Kind.PROTECTED:RatisNodeResourceBudget.Kind.ORDINARY,bytes,1);return lease::close;})) {
                var requests=new ArrayList<RatisRecoveryObsoleteRpc.Request>();for(int i=0;i<RatisTopologySourceSnapshotService.MAX_LEASES;i++){var request=obsolete(f,20+i);requests.add(request);service.prepare(request.original());}
                host.parkTransfer=true;host.parked=new CountDownLatch(RatisTopologySourceSnapshotService.MAX_LEASES);var transfers=requests.stream().map(request->service.transfer(request.original())).toList();assertTrue(host.parked.await(5,TimeUnit.SECONDS));assertEquals(RatisTopologySourceSnapshotService.MAX_LEASES,service.stats().active());
                service.obsolete(requests.getFirst()).get(5,TimeUnit.SECONDS);assertThrows(ExecutionException.class,()->transfers.getFirst().get(5,TimeUnit.SECONDS));assertEquals(1,host.cleanups.get());assertEquals(1,f.pinCloses.get());
                service.stop().get(5,TimeUnit.SECONDS);assertEquals(RatisTopologySourceSnapshotService.MAX_LEASES,f.pinCloses.get());assertEquals(0,budget.stats().ordinary().owners());
            }
        }
    }
    @Test void unknownTerminalsCannotConsumeTheSlotPromisedToAnAcceptedCurrent()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var accepted=obsolete(f,11);service.prepare(accepted.original());
                for(int i=0;i<RatisTopologySourceSnapshotService.MAX_RELEASED-1;i++)service.obsolete(obsolete(f,2000+i)).get(5,TimeUnit.SECONDS);
                int proofs=host.proofs.get();assertThrows(ExecutionException.class,()->service.obsolete(obsolete(f,5000)).get(5,TimeUnit.SECONDS));assertEquals(proofs,host.proofs.get());assertEquals(0,f.pinCloses.get());
                service.obsolete(accepted).get(5,TimeUnit.SECONDS);assertEquals(1,f.pinCloses.get());assertEquals(0,service.stats().leases());assertEquals(0,f.graph.stats().ordinary().owners());assertEquals(0,f.graph.stats().protectedLane().owners());
            }
        }
    }
    @Test void definitiveCanonicalReleaseConflictUnlatchesObsoleteAndOriginalReleaseRetryProgresses()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var request=obsolete(f,12);service.prepare(request.original());service.transfer(request.original()).get(5,TimeUnit.SECONDS);f.failRelease=true;
                assertThrows(IOException.class,()->service.release(request.original()));assertEquals(1,f.durableReleases.get());assertEquals(0,f.pinCloses.get());var retained=service.stats();
                host.releasedConflict=true;var failure=assertThrows(ExecutionException.class,()->service.obsolete(request).get(5,TimeUnit.SECONDS));assertInstanceOf(RatisTopologySourceSnapshotArtifacts.AdmissionRejected.class,failure.getCause());
                assertEquals(1,host.proofs.get());assertEquals(1,host.forces.get());assertFalse(host.forced);assertEquals(0,host.cleanups.get());assertEquals(retained,service.stats());assertEquals(0,f.pinCloses.get());assertEquals(0,f.graph.stats().protectedLane().owners());
                f.failRelease=false;service.release(request.original());assertEquals(2,f.durableReleases.get());assertEquals(1,f.pinCloses.get());assertEquals(0,service.stats().leases());assertEquals(0,service.stats().reservedImageBytes());
                assertThrows(ExecutionException.class,()->service.obsolete(request).get(5,TimeUnit.SECONDS));assertEquals(0,host.cleanups.get());
            }
        }
    }
    @Test void unclassifiedCaptureFailureRetainsItsOriginalPromiseUntilExactRetry()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var host=new Host(f);host.captureEntered=new CountDownLatch(0);host.captureRelease=new CountDownLatch(0);host.unknownCaptureFailure=true;
            try(var service=new RatisTopologySourceSnapshotService(host.proxy(),limits(),f.scheduler,f.transferAdmission())) {
                var request=obsolete(f,13).original();assertThrows(ExecutionException.class,()->service.prepareAsync(request).get(5,TimeUnit.SECONDS));var retained=service.stats();
                assertEquals(1,retained.leases());assertEquals(limits().maximumImageBytes(),retained.reservedImageBytes());assertTrue(retained.metadataBytes()>=RatisTopologySourceSnapshotService.obsoletePromiseBytes(request));assertEquals(0,retained.active());assertEquals(0,f.captures.get());assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.graph.stats().ordinary().owners());
                host.unknownCaptureFailure=false;var prepared=service.prepareAsync(request).get(5,TimeUnit.SECONDS);assertArrayEquals(prepared.image().sha256(),service.prepareAsync(request).get(5,TimeUnit.SECONDS).image().sha256());assertEquals(retained.reservedImageBytes(),service.stats().reservedImageBytes());assertEquals(1,f.captures.get());assertEquals(1,f.store.activeSnapshotPins());
            }
            assertEquals(1,f.pinCloses.get());assertEquals(0,f.graph.stats().ordinary().owners());
        }
    }
    @Test void clientUsesOriginalVoterAndAdmitsFixedAckBeforeCopy()throws Exception {
        try(var f=new RatisTopologySourceSnapshotFixture(root)) {
            var request=obsolete(f,6);var client=mock(RaftClient.class);var io=mock(BlockingApi.class);when(client.getGroupId()).thenReturn(RaftGroupId.valueOf(f.group));when(client.io()).thenReturn(io);var content=spy(ByteString.copyFrom(RatisRecoveryObsoleteRpc.reply(request)));var peer=RaftPeerId.valueOf(f.donor.peerId());
            var reply=RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(peer).setGroupId(client.getGroupId()).setCallId(1).setSuccess().setMessage(Message.valueOf(content)).build();when(io.sendStaleRead(any(),eq(0L),eq(peer))).thenReturn(reply);clearInvocations(content);
            var denied=new IOException("ACK admission denied");var rejecting=new RatisTopologySourceSnapshotClient(client,bytes->{assertEquals(RatisRecoveryObsoleteRpc.MAX_REPLY_BYTES,bytes);throw denied;});assertSame(denied,assertThrows(IOException.class,()->rejecting.obsolete(request,f.donor)));verify(content,never()).toByteArray();
            var allowed=new RatisTopologySourceSnapshotClient(client,bytes->verify(content,never()).toByteArray());allowed.obsolete(request,f.donor);verify(content,times(1)).toByteArray();
            assertThrows(IOException.class,()->allowed.obsolete(request,f.receiver));verify(io,never()).sendReadOnly(any());verify(client,never()).close();
            byte[] invalid=RatisRecoveryObsoleteRpc.reply(request);invalid[invalid.length-1]^=1;var malformed=RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(peer).setGroupId(client.getGroupId()).setCallId(1).setSuccess().setMessage(Message.valueOf(ByteString.copyFrom(invalid))).build();when(io.sendStaleRead(any(),eq(0L),eq(peer))).thenReturn(malformed);assertThrows(IOException.class,()->new RatisTopologySourceSnapshotClient(client,RatisTopologySourceSnapshotClient.ReplyAdmission.UNLIMITED).obsolete(request,f.donor));
        }
    }
}
