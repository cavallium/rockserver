package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Actual source JNI/image producer; controlled asynchronous receiver ports and journal force outcomes. */
class RatisConfiguredRecoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable,RatisConfiguredRecovery.Host {
        final RatisTopologySourceSnapshotFixture source;
        final RatisTopologySourceSnapshotService images;
        final ExecutorService io=Executors.newSingleThreadExecutor();
        final Request initial;
        final byte[] binding=new byte[32],registration=new byte[32];
        final AtomicInteger releasedMetadata=new AtomicInteger();
        volatile int prepares,registers,completions,releases,heads;
        boolean loseCompletion,loseRelease,notCaughtUp,abandon;
        CompletableFuture<byte[]> blockedRegistration;
        byte[] frozenHead;
        Prepared prepared;
        Request lastPrepared;
        Fixture()throws Exception {
            source=new RatisTopologySourceSnapshotFixture(root.resolve("source"),false,2);var directory=source.definition.sourceDirectory();var range=directory.ranges().getFirst();
            initial=new Request(new UUID(992,1),null,new UUID(992,2),range.replicas().get(1),Purpose.CURRENT_LEAF,range.groupId(),directory);
            images=new RatisTopologySourceSnapshotService(source,RatisTopologySourceSnapshotService.Limits.defaults(8L<<20),source.scheduler,source.transferAdmission());registration[0]=1;
        }
        RatisConfiguredRecovery open(Path path,RatisJournalAccess files)throws Exception {Files.createDirectories(path);return new RatisConfiguredRecovery(path,binding,initial,this,io,files,metadata(releasedMetadata::incrementAndGet),request->requireRequest(initial,request));}
        public CompletableFuture<Prepared> prepare(Request request) {
            prepares++;lastPrepared=request;
            if(abandon){abandon=false;try{return CompletableFuture.failedFuture(new AbandonedException(new Abandoned(request,new UUID(992,3),source.store.applied(),source.donor,OperatorUpload.sha256(source.store.configuration().data()))));}catch(Exception failed){return CompletableFuture.failedFuture(failed);}}
            return images.prepareAsync(request).thenApply(value->{prepared=value;return value;});
        }
        public CompletableFuture<byte[]> register(Prepared value){registers++;return blockedRegistration!=null?blockedRegistration:CompletableFuture.completedFuture(registration.clone());}
        public CompletableFuture<Prepared> transfer(Request request){return images.transfer(request);}
        public CompletableFuture<RatisRecoveryImageRpc.Evidence> probe(RatisRecoveryImageRpc.Request request){try{return CompletableFuture.completedFuture(new RatisRecoveryImageRpc.Evidence(request.digest(),TopologyChange.Owner.of(request.source().receiver()),source.store.applied(),source.store.configuration(),registration,true));}catch(Exception failure){return CompletableFuture.failedFuture(failure);}}
        public CompletableFuture<RatisSourceHeadRpc.Head> head(RatisSourceHeadRpc.Request request){try{assertEquals(0,source.store.activeSnapshotPins(),"Source pin must be released before querying HEAD so schema INSTALL can progress");assertEquals(1,source.durableReleases.get());heads++;return CompletableFuture.completedFuture(new RatisSourceHeadRpc.Head(request,new ApplyPosition(1,20),request.source().sourceDirectory(),source.donor,source.store.applied(),source.store.configuration()));}catch(Exception failure){return CompletableFuture.failedFuture(failure);}}
        public CompletableFuture<Boolean> complete(Prepared value,RatisSourceHeadRpc.Head head) {
            completions++;byte[] bytes=RatisSourceHeadRpc.reply(head);if(frozenHead==null)frozenHead=bytes;else assertArrayEquals(frozenHead,bytes,"Unknown completion must retry the original forced head");
            if(loseCompletion){loseCompletion=false;return CompletableFuture.failedFuture(new IOException("Lost completion force reply"));}
            if(notCaughtUp){notCaughtUp=false;frozenHead=null;return CompletableFuture.completedFuture(false);}return CompletableFuture.completedFuture(true);
        }
        public CompletableFuture<Void> release(Prepared value){releases++;try{images.release(value.request());if(loseRelease){loseRelease=false;throw new IOException("Lost durable release reply");}return CompletableFuture.completedFuture(null);}catch(Exception failed){return CompletableFuture.failedFuture(failed);}}
        public void close()throws Exception {images.close();io.shutdown();assertTrue(io.awaitTermination(10,TimeUnit.SECONDS));source.close();}
    }
    static RatisConfiguredRecovery.Phase step(RatisConfiguredRecovery recovery)throws Exception{return recovery.step().get(10,TimeUnit.SECONDS);}
    @Test void cancelledObserverAndStopRetainActualRegistrationAndColdResumeSkipsSettledPorts()throws Exception {
        try(var fixture=new Fixture()) {
            var path=root.resolve("attempt");var recovery=fixture.open(path,RatisJournalAccess.standalone());assertEquals(RatisConfiguredRecovery.Phase.REGISTER,step(recovery));fixture.blockedRegistration=new CompletableFuture<>();
            var observed=recovery.step();for(int tries=0;tries<100 && fixture.registers==0;tries++)Thread.sleep(10);assertEquals(1,fixture.registers);assertTrue(observed.cancel(false));var stopped=recovery.stop();assertFalse(stopped.isDone());assertEquals(0,fixture.releasedMetadata.get());
            assertThrows(IOException.class,()->fixture.open(path,RatisJournalAccess.standalone()));assertEquals(1,fixture.releasedMetadata.get(),"Only the rejected second opening releases its metadata owner");
            fixture.blockedRegistration.complete(fixture.registration.clone());stopped.get(10,TimeUnit.SECONDS);assertEquals(2,fixture.releasedMetadata.get());fixture.blockedRegistration=null;
            try(var cold=fixture.open(path,RatisJournalAccess.standalone())){assertEquals(RatisConfiguredRecovery.Phase.TRANSFER,cold.phase());while(cold.phase()!=RatisConfiguredRecovery.Phase.DONE)step(cold);}
            assertEquals(1,fixture.prepares);assertEquals(1,fixture.registers);assertEquals(1,fixture.source.pinCloses.get());assertEquals(1,fixture.source.durableReleases.get());
        }
    }
    @Test void forcedJournalUnknownAndColdCompletionReleaseRetriesKeepExactFrozenEvidence()throws Exception {
        try(var fixture=new Fixture()) {
            var path=root.resolve("attempt");var writes=new AtomicInteger();var lost=new Forwarding(){@Override public void write(Path file,byte[] body,int maximum)throws IOException{super.write(file,body,maximum);if(writes.incrementAndGet()==3)throw new IOException("Lost forced phase acknowledgement");}};
            try(var recovery=fixture.open(path,lost)){
                assertEquals(RatisConfiguredRecovery.Phase.REGISTER,step(recovery));assertThrows(ExecutionException.class,()->step(recovery));assertEquals(1,fixture.registers);assertEquals(RatisConfiguredRecovery.Phase.TRANSFER,step(recovery));assertEquals(1,fixture.registers,"Unknown journal force retries its exact candidate before another host action");
                while(recovery.phase()!=RatisConfiguredRecovery.Phase.RELEASE)step(recovery);fixture.loseRelease=true;assertThrows(ExecutionException.class,()->step(recovery));assertEquals(0,fixture.heads);
            }
            try(var cold=fixture.open(path,RatisJournalAccess.standalone())){assertEquals(RatisConfiguredRecovery.Phase.RELEASE,cold.phase());assertEquals(RatisConfiguredRecovery.Phase.HEAD,step(cold));assertEquals(RatisConfiguredRecovery.Phase.COMPLETE,step(cold));fixture.loseCompletion=true;assertThrows(ExecutionException.class,()->step(cold));}
            try(var cold=fixture.open(path,RatisJournalAccess.standalone())){assertEquals(RatisConfiguredRecovery.Phase.COMPLETE,cold.phase());assertEquals(RatisConfiguredRecovery.Phase.DONE,step(cold));assertEquals(1,fixture.heads);}
            assertEquals(2,fixture.completions);assertEquals(2,fixture.releases);assertEquals(1,fixture.prepares);assertEquals(1,fixture.source.durableReleases.get());assertEquals(1,fixture.source.pinCloses.get());
        }
    }
    @Test void explicitAbandonmentIsForcedBeforeSuccessorAndColdReopenCannotChangeInitialScope()throws Exception {
        try(var fixture=new Fixture()) {
            fixture.abandon=true;var path=root.resolve("attempt");try(var recovery=fixture.open(path,RatisJournalAccess.standalone())){assertEquals(RatisConfiguredRecovery.Phase.PREPARE,step(recovery));assertEquals(0,fixture.source.captures.get());}
            try(var cold=fixture.open(path,RatisJournalAccess.standalone())){assertEquals(RatisConfiguredRecovery.Phase.REGISTER,step(cold));assertEquals(new UUID(992,3),fixture.lastPrepared.transferId());fixture.notCaughtUp=true;while(cold.phase()!=RatisConfiguredRecovery.Phase.DONE)step(cold);assertEquals(2,fixture.heads);}
            var reused=new Request(fixture.lastPrepared.transferId(),fixture.source.definition.header(),fixture.initial.catalogue(),fixture.initial.receiver(),Purpose.ANCESTOR_METADATA,fixture.initial.sourceGroup(),fixture.initial.sourceDirectory());byte[] settled=Files.readAllBytes(path.resolve("recovery"));
            assertThrows(IllegalArgumentException.class,()->new RatisConfiguredRecovery(path,fixture.binding,reused,fixture,fixture.io,RatisJournalAccess.standalone(),metadata(()->{}),request->{}));assertArrayEquals(settled,Files.readAllBytes(path.resolve("recovery")),"A released effective capture identity cannot replace DONE");
            byte[] wrong=fixture.binding.clone();wrong[0]=1;assertThrows(IOException.class,()->new RatisConfiguredRecovery(path,wrong,fixture.initial,fixture,fixture.io,RatisJournalAccess.standalone(),metadata(()->{}),request->{}));assertEquals(1,fixture.source.captures.get());
        }
    }
    static RatisConfiguredRecovery.Metadata metadata(Runnable release){return new RatisConfiguredRecovery.Metadata(){public void requireRawBytes(int bytes){}public void close(){release.run();}};}
    static class Forwarding implements RatisJournalAccess {
        final RatisJournalAccess delegate=RatisJournalAccess.standalone();
        public RootLease backendRoot(Path root)throws IOException{return delegate.backendRoot(root);}public void register(Path path,int maximum)throws IOException{delegate.register(path,maximum);}public byte[] read(Path path,int maximum)throws IOException{return delegate.read(path,maximum);}public void write(Path path,byte[] body,int maximum)throws IOException{delegate.write(path,body,maximum);}public void remove(Path path,byte[] body,int maximum,SessionJournalFiles.Removal removal)throws IOException{delegate.remove(path,body,maximum,removal);}public void deleted(Path path)throws IOException{delegate.deleted(path);}public void reclaimedTree(Path path)throws IOException{delegate.reclaimedTree(path);}public void reserve(Reservation reservation)throws IOException{delegate.reserve(reservation);}public void fulfilled(Reservation reservation)throws IOException{delegate.fulfilled(reservation);}
    }
}
