package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisBulkRequestBudgetTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void acknowledgedColdFailureCleanupReusesOpeningCreditUnderCompetingPressure()throws Exception {
        var helper=new RatisRocksDBBatchesTest();helper.root=root;
        try(var x=helper.new Fixture()){
            var token=RatisRocksDBBatchesTest.prepare(x.batches);var once=new java.util.concurrent.atomic.AtomicBoolean();
            x.batches.beforeAcknowledgementMarker(()->{if(!once.compareAndSet(false,true))return;try{var before=x.f.catalogue.epochs.read();var expired=before.advanceTime(x.session.grant().expiresAtMillis());assertEquals(CommandResult.Status.SUCCESS,x.f.catalogue.epochs.execute(x.f.catalogue.epochs.prepare(x.f.outer.operation(3),before,expired)).status());int n=0;for(var group:x.f.groups){var clock=DistributedCommand.sessionControl(group,1,x.f.schema.digest(),new OperationId(x.f.admin,3,n++),RangeSessionControl.advance(expired),-1);assertEquals(CommandResult.Status.SUCCESS,x.f.data.execute(x.f.data.prepareSessionControl(clock)).status());}}catch(java.io.IOException failure){throw new java.io.UncheckedIOException(failure);}});
            var batch=new KVBatch.KVBatchRef(Collections.nCopies(513,RatisAtomicClientTest.key(2)),Collections.nCopies(513,it.cavallium.buffer.Buf.wrap(new byte[1024])));
            var progress=x.batches.append(token,0,batch,RemoteMutationInputs.batch(71,false,batch),RequestContext.batch()).get(10,TimeUnit.SECONDS);assertNotNull(progress.failure());assertTrue(progress.retainedChunks()>0);
            var journal=RatisTopologySeedRuntimeTest.field(x.batches,"journal",RatisBatchRequestJournal.class);long encoded=journal.encodedSize(),cleanup=RatisRequestBudget.bulkRetained(journal.request())+6*encoded;
            var pending=x.journal.resolveSibling(x.journal.getFileName()+".pending");java.nio.file.Files.createDirectory(pending);assertThrows(java.io.IOException.class,()->x.batches.acknowledgeResult(token,x.batches.status(token).resultDigest()));assertTrue(java.nio.file.Files.isRegularFile(x.journal));x.batches.close();x.session.close();java.nio.file.Files.delete(pending);
            long opening=RatisRequestBudget.files(x.journal);var owner=x.budget.owner(RemoteMutationToken.Kind.BULK,RangeDirectoryCodec.encodedSize(x.routed.directory(x.f.schema)),opening);owner.requireOpening(opening);long room=Math.max(0,cleanup-6*opening)+(128L<<10);assertTrue(room<6*encoded,"The old second encoding reservation cannot fit");
            try(owner;var session=RatisDurableSessionClient.reopen(x.window,x.routed);var competing=x.budget.request(RemoteMutationToken.Kind.POINT,x.budget.limits().maximumBytes()-x.budget.chargedBytes()-room);
                    var reopened=new RatisRocksDBBatches(x.f.schema,x.routed,session,x.f.scheduler,x.journal,RatisRocksDBBatches.SessionOwnership.BORROWED,x.budget,owner)){
                assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,reopened.status(token).state());assertEquals(0,x.budget.retainedRequests(RemoteMutationToken.Kind.BULK));assertFalse(java.nio.file.Files.exists(x.journal));
            }
        }
    }
    @Test void deadlineCannotReleaseActualSynchronousIntakeBeforeBorrowedSourceReturns()throws Exception {
        var helper=new RatisRocksDBBatchesTest();helper.root=root;
        try(var x=helper.new Fixture();var caller=Executors.newSingleThreadExecutor()){
            var token=RatisRocksDBBatchesTest.prepare(x.batches);long before=x.budget.chargedBytes();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var key=RatisAtomicClientTest.key(2);var value=RatisAtomicClientTest.value(9);
            KVBatch held=new KVBatch(){public List<Keys> keys(){entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("Input borrower did not return");}catch(InterruptedException failure){throw new AssertionError(failure);}return List.of(key);}public List<it.cavallium.buffer.Buf> values(){return List.of(value);}};
            byte[] digest=RemoteMutationInputs.batch(71,false,new KVBatch.KVBatchRef(List.of(key),List.of(value)));var intake=caller.submit(()->x.batches.append(token,0,held,digest,RequestContext.batch(java.time.Duration.ofMillis(100))));
            try{assertTrue(entered.await(5,TimeUnit.SECONDS));assertThrows(TimeoutException.class,()->intake.get(250,TimeUnit.MILLISECONDS));assertEquals(RatisRocksDBBatches.RecoveryState.ACTIVE,x.batches.recoveryState());assertTrue(x.budget.chargedBytes()>before);assertThrows(java.io.IOException.class,x.batches::close);assertTrue(x.writes.isEmpty());}
            finally{release.countDown();}
            var observed=intake.get(5,TimeUnit.SECONDS);assertThrows(ExecutionException.class,()->observed.get(5,TimeUnit.SECONDS));x.batches.actualCompletion(observed).get(5,TimeUnit.SECONDS);assertEquals(before,x.budget.chargedBytes());assertEquals(0,x.batches.progress().copiedBatches());x.batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);x.batches.acknowledgeResult(token,x.batches.status(token).resultDigest());
        }
    }
    @Test void sharedDenialPrecedesCopyAndCancelledReplyRetainsActualAndRequestCredits()throws Exception {
        var helper=new RatisRocksDBBatchesTest();helper.root=root;
        try(var x=helper.new Fixture()){
            long baseline=x.budget.chargedBytes();var token=RatisRocksDBBatchesTest.prepare(x.batches);long before=x.budget.chargedBytes();var copies=new java.util.concurrent.atomic.AtomicInteger();
            KVBatch unseen=new KVBatch(){public List<Keys> keys(){copies.incrementAndGet();throw new AssertionError("Input copied without shared credit");}public List<it.cavallium.buffer.Buf> values(){throw new AssertionError("Input copied without credit");}};
            try(var point=x.budget.request(RemoteMutationToken.Kind.POINT,x.budget.limits().maximumBytes()-x.budget.chargedBytes()-(80L<<10))){
                var denied=x.batches.append(token,0,unseen,new byte[32],RequestContext.batch());assertThrows(ExecutionException.class,()->denied.get(10,TimeUnit.SECONDS));x.batches.actualCompletion(denied).get(10,TimeUnit.SECONDS);assertEquals(0,copies.get());assertTrue(x.writes.isEmpty());assertEquals(0,x.batches.progress().copiedBatches());
            }
            assertEquals(before,x.budget.chargedBytes());var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(9)));x.entered=new CountDownLatch(1);x.release=new CountDownLatch(1);var observer=x.batches.append(token,0,batch,RemoteMutationInputs.batch(71,false,batch),RequestContext.batch());
            try{assertTrue(x.entered.await(5,TimeUnit.SECONDS));long owned=x.budget.chargedBytes();assertTrue(observer.cancel(false));var actual=x.batches.actualCompletion(observer);assertFalse(actual.isDone());assertEquals(owned,x.budget.chargedBytes());assertEquals(1,x.budget.retainedRequests(RemoteMutationToken.Kind.BULK));assertThrows(java.io.IOException.class,x.batches::close);x.release.countDown();actual.get(10,TimeUnit.SECONDS);}
            finally{x.release.countDown();}
            x.entered=null;x.batches.resumeRetained(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(1,x.budget.retainedRequests(RemoteMutationToken.Kind.BULK));x.batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);x.batches.acknowledgeResult(token,x.batches.status(token).resultDigest());assertEquals(0,x.budget.retainedRequests());assertEquals(baseline,x.budget.chargedBytes());assertFalse(java.nio.file.Files.exists(x.journal));
        }
    }
}
