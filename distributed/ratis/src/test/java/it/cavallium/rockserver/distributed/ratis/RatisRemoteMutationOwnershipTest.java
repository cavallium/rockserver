package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native effects and durable envelopes only; no listener or quorum claims. */
class RatisRemoteMutationOwnershipTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static RatisRangeDeleteClient openRange(RatisAtomicClientTest.Fixture f,RatisRocksDBAdapter.AllocationSource allocations,Path folder,RatisRequestBudget budget)throws IOException {
        var credit=budget.owner(RemoteMutationToken.Kind.RANGE,4L*(DistributedSchemaCodec.encodedSize(f.schema)+RangeDirectoryCodec.encodedSize(f.directory)),RatisRequestBudget.files(folder.resolve("input"),folder.resolve("input.pending"),folder.resolve("atomic"),folder.resolve("atomic.pending")));
        try{return new RatisRangeDeleteClient(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder,budget,credit);}catch(IOException|RuntimeException|Error failure){credit.close();throw failure;}
    }

    @Test void lostPointResultRequiresOriginalTokenAndExplicitAckAcrossReopen() throws Exception {
        var fixtures=new RatisRocksDBDirectTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            var owner=x.direct.owner();var request=UUID.randomUUID();var key=RatisAtomicClientTest.key(2);var value=RatisAtomicClientTest.value(9);
            var token=x.direct.prepareSequence(owner,request,0,71,List.of(key),List.of(value),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,false,RequestContext.batch()).get(10,TimeUnit.SECONDS);
            assertEquals(0,x.writes.size());assertEquals(RemoteMutationStatus.State.PREPARED,x.direct.status(token).state());
            x.lose.set(true);assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS));var original=x.session.pending().getFirst().command().operationId();
            x.abandoned=true;x.direct.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var direct=x.budget.open(x.f.schema,x.routed,session,x.f.scheduler,x.journal,x.window)) {
                assertNull(direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS));assertEquals(original,x.writes.getLast());int effects=x.f.writes();
                assertEquals(RemoteMutationStatus.State.TERMINAL,direct.status(token).state());assertEquals(1,direct.resultPage(token,0,1,CommandResult.MAX_BYTES).size());
                assertThrows(ExecutionException.class,() -> direct.prepareSequence(owner,request,0,71,List.of(key),List.of(RatisAtomicClientTest.value(8)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,false,RequestContext.batch()).get(10,TimeUnit.SECONDS));
                assertThrows(IOException.class,() -> direct.acknowledgeResult(token,new byte[32]));direct.acknowledgeResult(token,direct.status(token).resultDigest());assertEquals(effects,x.f.writes());
            }
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var direct=x.budget.open(x.f.schema,x.routed,session,x.f.scheduler,x.journal,x.window)) {
                assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,direct.status(token).state());direct.acknowledgeResult(token,direct.status(token).resultDigest());assertThrows(ExecutionException.class,() -> direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS));
                var next=direct.prepareSequence(owner,UUID.randomUUID(),1,71,List.of(key),List.of(value),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,false,RequestContext.batch()).get(10,TimeUnit.SECONDS);
                assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,direct.status(token).state());assertArrayEquals(new byte[]{9},((it.cavallium.buffer.Buf)direct.execute(next,RequestContext.batch()).get(10,TimeUnit.SECONDS)).toByteArray());
            }
        }
    }
    @Test void bulkDuplicateOrdinalAndInterruptedTailKeepCopiedInputWithoutInventingFinish() throws Exception {
        var fixtures=new RatisRocksDBBatchesTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            var owner=x.batches.owner();var token=x.batches.prepare(owner,UUID.randomUUID(),0,71,false);var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(9)));byte[] digest=RemoteMutationInputs.batch(71,false,batch);
            assertEquals(0,x.writes.size());x.lose.set(true);assertThrows(ExecutionException.class,() -> x.batches.append(token,0,batch,digest,RequestContext.batch()).get(10,TimeUnit.SECONDS));x.batches.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var batches=x.open(session)) {
                var resumed=batches.resumeRetained(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertFalse(resumed.sourceComplete());assertEquals(1,resumed.copiedBatches());int writes=x.writes.size();
                batches.append(token,0,batch,digest,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(writes,x.writes.size());
                var changed=new KVBatch.KVBatchRef(batch.keys(),List.of(RatisAtomicClientTest.value(8)));assertThrows(ExecutionException.class,() -> batches.append(token,0,changed,RemoteMutationInputs.batch(71,false,changed),RequestContext.batch()).get(10,TimeUnit.SECONDS));
                assertEquals(RemoteMutationStatus.State.PENDING,batches.status(token).state());batches.finishInput(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(RemoteMutationStatus.State.TERMINAL,batches.status(token).state());batches.acknowledgeResult(token,batches.status(token).resultDigest());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,batches.status(token).state());
            }
        }
    }
    @Test void busyObservationReturnsWithoutWaitingForActualBulkRpc() throws Exception {
        var fixtures=new RatisRocksDBBatchesTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            var token=x.batches.prepare(x.batches.owner(),UUID.randomUUID(),0,71,false);var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(9)));
            x.entered=new CountDownLatch(1);x.release=new CountDownLatch(1);var observer=x.batches.append(token,0,batch,RemoteMutationInputs.batch(71,false,batch),RequestContext.batch());assertTrue(x.entered.await(10,TimeUnit.SECONDS));var actual=x.batches.actualCompletion(observer);
            try {assertEquals(RemoteMutationStatus.State.PENDING,assertTimeoutPreemptively(java.time.Duration.ofSeconds(1),() -> x.batches.status(token)).state());assertFalse(actual.isDone());assertThrows(IOException.class,x.batches::close);}finally{x.release.countDown();}
            observer.get(10,TimeUnit.SECONDS);actual.get(10,TimeUnit.SECONDS);assertEquals(1,x.batches.progress().acknowledgedMutations());
        }
    }
    @Test void noncommutativePointMergeLostSubmitResultAndAckRetainOriginalIdentity() throws Exception {
        var fixtures=new RatisMergeFrontendTest();fixtures.root=root;
        try(var x=fixtures.new Fixture(root.resolve("point-merge"))) {
            var owner=x.direct.owner();var request=UUID.randomUUID();var key=RatisMergeFrontendTest.key(2);
            var token=x.direct.prepareSequence(owner,request,0,75,List.of(key,key),List.of(RatisMergeFrontendTest.value(1),RatisMergeFrontendTest.value(2)),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.MERGED,true,RequestContext.batch()).get(15,TimeUnit.SECONDS);
            assertTrue(x.sent.isEmpty());x.lose.set(true);assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(15,TimeUnit.SECONDS));x.direct.close();x.session.close();x.closedDirect=true;
            byte[] acknowledged;
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var direct=x.budget.open(x.schema,x.routed,session,x.f.scheduler,x.journal,x.window)) {
                var result=(List<?>)direct.execute(token,RequestContext.batch()).get(15,TimeUnit.SECONDS);assertArrayEquals(new byte[]{1},((it.cavallium.buffer.Buf)result.get(0)).toByteArray());assertArrayEquals(new byte[]{1,2},((it.cavallium.buffer.Buf)result.get(1)).toByteArray());
                assertEquals(x.sent.get(0).operationId(),x.sent.get(1).operationId());assertEquals(2,direct.status(token).appliedMutations());int sent=x.sent.size();direct.execute(token,RequestContext.batch()).get(15,TimeUnit.SECONDS);assertEquals(sent,x.sent.size());acknowledged=direct.status(token).resultDigest();direct.acknowledgeResult(token,acknowledged);
            }
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var direct=x.budget.open(x.schema,x.routed,session,x.f.scheduler,x.journal,x.window)) {
                assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,direct.status(token).state());direct.acknowledgeResult(token,acknowledged);assertArrayEquals(new byte[]{1,2},x.routed.read(x.schema.column(75).columnId(),new byte[]{2}));
            }
        }
    }
    @Test void acceptedRangeSidecarLostReplyKeepsSameCapturesAndExplicitAck() throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("range-native"),true);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())) {
            RatisRocksDBAdapterTest.seed(f,71,RatisAtomicClientTest.key(2),RatisAtomicClientTest.value(1),1900);RatisRocksDBAdapterTest.seed(f,71,RatisAtomicClientTest.key(200),RatisAtomicClientTest.value(2),1901);
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);var folder=root.resolve("range-owner");RemoteMutationToken token;
            try(var actor=openRange(f,allocations,folder,budget)) {
                token=actor.prepare(actor.owner(),UUID.randomUUID(),0,71,RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(201));assertEquals(0,f.opens.get());assertEquals(0,allocations.count.get());
                f.beforeHomeRead=() -> {throw new java.io.UncheckedIOException(new IOException("Lost original HOME reply"));};assertThrows(ExecutionException.class,() -> actor.execute(token,RequestContext.batch()).observer().get(15,TimeUnit.SECONDS));
            }
            java.nio.file.Files.move(folder.resolve("atomic"),folder.resolve("atomic.pending"));int captures=f.opens.get();assertEquals(2,captures);f.beforeHomeRead=null;
            try(var actor=openRange(f,allocations,folder,budget)) {
                actor.execute(token,RequestContext.batch()).observer().get(15,TimeUnit.SECONDS);assertEquals(captures,f.opens.get());assertEquals(1,allocations.count.get());assertNull(f.get(71,RatisAtomicClientTest.key(2)));assertNull(f.get(71,RatisAtomicClientTest.key(200)));assertEquals(RemoteMutationStatus.State.TERMINAL,actor.status(token).state());assertEquals(CommandResult.Status.SUCCESS,actor.status(token).outcome());assertEquals(2,actor.status(token).appliedMutations());assertThrows(IOException.class,() -> actor.prepare(new RemoteMutationOwner(token.clusterId(),token.databaseId(),token.domainId(),UUID.randomUUID(),token.ownerEpoch(),0,token.kind(),token.schemaDigest(),token.directoryDigest()),token.requestId(),0,71,RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(201)));actor.acknowledgeResult(token,actor.status(token).resultDigest());
            }
            try(var actor=openRange(f,allocations,folder,budget)) {assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,actor.status(token).state());actor.acknowledgeResult(token,actor.status(token).resultDigest());assertEquals(captures,f.opens.get());}
        }
    }
    @Test void thousandNoncommutativeRequestsReuseOneGrantAndEvictedPrepareNeverBecomesFresh() throws Exception {
        var fixtures=new RatisMergeFrontendTest();fixtures.root=root;
        try(var x=fixtures.new Fixture(root.resolve("continuous"))) {
            var owner=x.direct.owner();var key=RatisMergeFrontendTest.key(2);RemoteMutationToken first=null;byte[] expected=new byte[1024];
            for(int n=0;n<expected.length;n++){expected[n]=(byte)n;var token=x.direct.prepareSequence(owner,UUID.randomUUID(),n,75,List.of(key),List.of(it.cavallium.buffer.Buf.wrap(new byte[]{(byte)n})),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(15,TimeUnit.SECONDS);if(first==null)first=token;x.direct.execute(token,RequestContext.batch()).get(15,TimeUnit.SECONDS);x.direct.acknowledgeResult(token,x.direct.status(token).resultDigest());assertEquals(owner.ownerEpoch(),token.ownerEpoch());}
            assertArrayEquals(expected,x.routed.read(x.schema.column(75).columnId(),new byte[]{2}));assertEquals(1024,x.direct.owner().nextRequestSequence());assertEquals(RemoteMutationStatus.State.EXPIRED,x.direct.status(first).state());assertFalse(x.direct.status(first).progressKnown());int sent=x.sent.size();var original=first;
            assertThrows(ExecutionException.class,() -> x.direct.prepareSequence(owner,original.requestId(),0,75,List.of(key),List.of(RatisMergeFrontendTest.value(1)),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(15,TimeUnit.SECONDS));assertThrows(ExecutionException.class,() -> x.direct.execute(original,RequestContext.batch()).get(15,TimeUnit.SECONDS));assertEquals(sent,x.sent.size());
            assertThrows(ExecutionException.class,() -> x.direct.prepareSequence(owner,UUID.randomUUID(),1025,75,List.of(key),List.of(RatisMergeFrontendTest.value(1)),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(15,TimeUnit.SECONDS));
            var request=UUID.randomUUID();var prepared=x.direct.prepareSequence(owner,request,1024,75,List.of(key),List.of(RatisMergeFrontendTest.value(1)),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(15,TimeUnit.SECONDS);assertEquals(prepared,x.direct.prepareSequence(owner,request,1024,75,List.of(key),List.of(RatisMergeFrontendTest.value(1)),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(15,TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,() -> x.direct.prepareSequence(owner,UUID.randomUUID(),1024,75,List.of(key),List.of(RatisMergeFrontendTest.value(1)),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(15,TimeUnit.SECONDS));assertEquals(sent,x.sent.size());
        }
    }
    @Test void unknownRetirementForceFencesLiveOwnerAndColdReopenKeepsLostAck() throws Exception {
        var fixtures=new RatisRocksDBDirectTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            var owner=x.direct.owner();var key=RatisAtomicClientTest.key(2);var token=x.direct.prepareSequence(owner,UUID.randomUUID(),0,71,List.of(key),List.of(RatisAtomicClientTest.value(9)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(10,TimeUnit.SECONDS);x.direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);var digest=x.direct.status(token).resultDigest();int effects=x.f.writes();x.direct.afterRetirementForce(() -> {throw new java.io.UncheckedIOException(new IOException("Lost retirement force reply"));});
            assertThrows(java.io.UncheckedIOException.class,() -> x.direct.acknowledgeResult(token,digest));assertThrows(IOException.class,() -> x.direct.status(token));assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS));x.abandoned=true;x.direct.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var direct=x.budget.open(x.f.schema,x.routed,session,x.f.scheduler,x.journal,x.window)){assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,direct.status(token).state());assertEquals(1,direct.owner().nextRequestSequence());direct.acknowledgeResult(token,digest);assertThrows(ExecutionException.class,() -> direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS));assertEquals(effects,x.f.writes());}
        }
    }
    @Test void unknownBindForceKeepsOriginalSessionIdentityBeforeAnyDataEffect() throws Exception {
        var fixtures=new RatisRocksDBDirectTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            var token=x.direct.prepareSequence(x.direct.owner(),UUID.randomUUID(),0,71,List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(9)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(10,TimeUnit.SECONDS);
            var pending=x.journal.resolveSibling(x.journal.getFileName()+".pending");java.nio.file.Files.createDirectory(pending);assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS));assertTrue(x.writes.isEmpty());var original=x.session.pending().getFirst().command().operationId();assertThrows(IOException.class,() -> x.direct.status(token));x.abandoned=true;x.direct.close();x.session.close();java.nio.file.Files.deleteIfExists(pending);
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var direct=x.budget.open(x.f.schema,x.routed,session,x.f.scheduler,x.journal,x.window)){direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertEquals(1,x.writes.size());assertEquals(original,x.writes.getFirst());}
        }
    }
    @Test void nativeGrantExpiryRetainsAppliedPrefixAndRejectsExpiredRetryAfterAck() throws Exception {
        var fixtures=new RatisRocksDBDirectTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            var token=x.direct.prepareSequence(x.direct.owner(),UUID.randomUUID(),0,71,List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(200)),List.of(RatisAtomicClientTest.value(9),RatisAtomicClientTest.value(8)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,true,RequestContext.batch()).get(10,TimeUnit.SECONDS);
            var once=new java.util.concurrent.atomic.AtomicBoolean();x.direct.afterAcknowledgement(() -> {if(!once.compareAndSet(false,true))return;try {var expired=x.ledger.advanceTime(x.grant.expiresAtMillis());assertEquals(CommandResult.Status.SUCCESS,x.f.catalogue.epochs.execute(x.f.catalogue.epochs.prepare(x.f.outer.operation(3),x.ledger,expired)).status());int n=0;for(var group:x.f.groups){var clock=DistributedCommand.sessionControl(group,1,x.f.schema.digest(),new OperationId(x.f.admin,3,n++),RangeSessionControl.advance(expired),-1);assertEquals(CommandResult.Status.SUCCESS,x.f.data.execute(x.f.data.prepareSessionControl(clock)).status());}}catch(IOException failure){throw new java.io.UncheckedIOException(failure);}});
            assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS));var status=x.direct.status(token);assertEquals(RemoteMutationStatus.State.TERMINAL,status.state());assertEquals(CommandResult.Status.SESSION_EXPIRED,status.outcome());assertTrue(status.progressKnown());assertEquals(1,status.appliedMutations());assertArrayEquals(new byte[]{9},x.f.get(71,RatisAtomicClientTest.key(2)));assertNull(x.f.get(71,RatisAtomicClientTest.key(200)));x.direct.acknowledgeResult(token,status.resultDigest());int writes=x.writes.size();assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(10,TimeUnit.SECONDS));assertEquals(writes,x.writes.size());
        }
    }
    @Test void concurrentPrepareCannotReplaceTheOriginalForcedSequence() throws Exception {
        var fixtures=new RatisRocksDBDirectTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            var owner=x.direct.owner();var request=UUID.randomUUID();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);x.direct.afterRetirementForce(() -> {entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new AssertionError("Owner force barrier timed out");}catch(InterruptedException failure){throw new AssertionError(failure);}});
            var first=x.direct.prepareSequence(owner,request,0,71,List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(9)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch());assertTrue(entered.await(10,TimeUnit.SECONDS));
            try {assertThrows(ExecutionException.class,() -> x.direct.prepareSequence(owner,UUID.randomUUID(),0,71,List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(8)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(10,TimeUnit.SECONDS));assertTrue(x.writes.isEmpty());}finally{release.countDown();}
            var original=first.get(10,TimeUnit.SECONDS);x.direct.afterRetirementForce(null);assertEquals(original,x.direct.prepareSequence(owner,request,0,71,List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(9)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(10,TimeUnit.SECONDS));
        }
    }
    @Test void missingPreparedInputCannotBeAdmittedAsFreshAfterReopen() throws Exception {
        var fixtures=new RatisRocksDBDirectTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            x.direct.prepareSequence(x.direct.owner(),UUID.randomUUID(),0,71,List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(9)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(10,TimeUnit.SECONDS);assertTrue(x.writes.isEmpty());x.abandoned=true;x.direct.close();x.session.close();java.nio.file.Files.delete(x.journal);
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed)){assertThrows(IOException.class,() -> x.budget.open(x.f.schema,x.routed,session,x.f.scheduler,x.journal,x.window));}
        }
    }
    @Test void bulkNativeExpiryExposesTerminalOutcomeAndConfirmedGroupMutationCountAfterAck() throws Exception {
        var fixtures=new RatisRocksDBBatchesTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            var token=x.batches.prepare(x.batches.owner(),UUID.randomUUID(),0,71,false);var batch=new KVBatch.KVBatchRef(List.of(RatisAtomicClientTest.key(2),RatisAtomicClientTest.key(200)),List.of(RatisAtomicClientTest.value(9),RatisAtomicClientTest.value(8)));var once=new java.util.concurrent.atomic.AtomicBoolean();
            x.batches.beforeAcknowledgementMarker(() -> {if(!once.compareAndSet(false,true))return;try {var before=x.f.catalogue.epochs.read();var expired=before.advanceTime(x.session.grant().expiresAtMillis());assertEquals(CommandResult.Status.SUCCESS,x.f.catalogue.epochs.execute(x.f.catalogue.epochs.prepare(x.f.outer.operation(3),before,expired)).status());int n=0;for(var group:x.f.groups){var clock=DistributedCommand.sessionControl(group,1,x.f.schema.digest(),new OperationId(x.f.admin,3,n++),RangeSessionControl.advance(expired),-1);assertEquals(CommandResult.Status.SUCCESS,x.f.data.execute(x.f.data.prepareSessionControl(clock)).status());}}catch(IOException failure){throw new java.io.UncheckedIOException(failure);}});
            x.batches.append(token,0,batch,RemoteMutationInputs.batch(71,false,batch),RequestContext.batch()).get(10,TimeUnit.SECONDS);var status=x.batches.status(token);assertEquals(RemoteMutationStatus.State.TERMINAL,status.state());assertEquals(CommandResult.Status.SESSION_EXPIRED,status.outcome());assertEquals(1,status.appliedMutations());assertArrayEquals(new byte[]{9},x.f.get(71,RatisAtomicClientTest.key(2)));assertNull(x.f.get(71,RatisAtomicClientTest.key(200)));x.batches.acknowledgeResult(token,status.resultDigest());x.batches.close();x.session.close();
            try(var session=RatisDurableSessionClient.reopen(x.window,x.routed);var batches=x.open(session)){var acknowledged=batches.status(token);assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,acknowledged.state());assertEquals(CommandResult.Status.SESSION_EXPIRED,acknowledged.outcome());assertEquals(1,acknowledged.appliedMutations());}
        }
    }
    @Test void unsubmittedPointSuffixExposesStaleSchemaOutcomeAndSavedAppliedPrefix() throws Exception {
        var fixtures=new RatisMergeFrontendTest();fixtures.root=root;
        try(var x=fixtures.new Fixture(root.resolve("unsubmitted-outcome"))) {
            var token=x.direct.prepareSequence(x.direct.owner(),UUID.randomUUID(),0,75,List.of(RatisMergeFrontendTest.key(2),RatisMergeFrontendTest.key(200)),List.of(RatisMergeFrontendTest.value(1),RatisMergeFrontendTest.value(2)),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.MERGED,true,RequestContext.batch()).get(15,TimeUnit.SECONDS);
            x.direct.afterAcknowledgement(() -> {throw new java.io.UncheckedIOException(new IOException("Stop after the forced first prefix"));});assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(15,TimeUnit.SECONDS));x.direct.afterAcknowledgement(null);x.install(x.schema,77,new UUID(310,77),"later-merge",x.schema.column(75).operator(),false);x.routed.refreshSchema(x.lastDefinition,x.lastActivation);int sent=x.sent.size();assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(15,TimeUnit.SECONDS));var status=x.direct.status(token);assertEquals(RemoteMutationStatus.State.TERMINAL,status.state());assertEquals(CommandResult.Status.STALE_SCHEMA,status.outcome());assertEquals(1,status.appliedMutations());assertEquals(1,status.retainedResults());assertEquals(sent,x.sent.size());x.direct.acknowledgeResult(token,status.resultDigest());assertEquals(CommandResult.Status.STALE_SCHEMA,x.direct.status(token).outcome());
        }
    }
    @Test void pointAggregateResultOverflowExposesTerminalOutcomeWithoutLosingAppliedPrefix() throws Exception {
        var fixtures=new RatisRocksDBDirectTest();fixtures.root=root;
        try(var x=fixtures.new Fixture()) {
            var owner=x.direct.owner();var keys=new ArrayList<Keys>();byte[] large=new byte[1024*1024];Arrays.fill(large,(byte)3);
            for(int n=0;n<17;n++){var key=RatisAtomicClientTest.key(n+2);keys.add(key);var seed=x.direct.prepareSequence(owner,UUID.randomUUID(),n,71,List.of(key),List.of(it.cavallium.buffer.Buf.wrap(large)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(15,TimeUnit.SECONDS);x.direct.execute(seed,RequestContext.batch()).get(15,TimeUnit.SECONDS);x.direct.acknowledgeResult(seed,x.direct.status(seed).resultDigest());}
            var values=keys.stream().map(key -> RatisAtomicClientTest.value(9)).toList();var token=x.direct.prepareSequence(owner,UUID.randomUUID(),17,71,keys,values,RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,true,RequestContext.batch()).get(15,TimeUnit.SECONDS);assertThrows(ExecutionException.class,() -> x.direct.execute(token,RequestContext.batch()).get(30,TimeUnit.SECONDS));var status=x.direct.status(token);assertEquals(RemoteMutationStatus.State.TERMINAL,status.state());assertEquals(CommandResult.Status.RESULT_TOO_LARGE,status.outcome());assertEquals(16,status.appliedMutations());assertEquals(16,status.retainedResults());assertArrayEquals(new byte[]{9},x.f.get(71,keys.get(15)));assertArrayEquals(large,x.f.get(71,keys.get(16)));x.direct.acknowledgeResult(token,status.resultDigest());assertEquals(CommandResult.Status.RESULT_TOO_LARGE,x.direct.status(token).outcome());
        }
    }
    @Test void releasedSequenceFloorBoundsMetadataAndNeverReissuesEvictedIdentity() throws Exception {
        UUID owner=UUID.randomUUID(),cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID();var authority=new RemoteMutationOwner(cluster,database,domain,owner,1,0,RemoteMutationToken.Kind.POINT,new byte[32],new byte[32]);var file=root.resolve("receipts");var receipts=new RatisRemoteMutationReceipts(file,authority,false,RatisJournalAccess.standalone());RemoteMutationToken first=null;
        for(int n=0;n<1024;n++){var token=new RemoteMutationToken(cluster,database,domain,owner,new UUID(0,n),1,n,RemoteMutationToken.Kind.POINT,new byte[32],new byte[32],new byte[32]);if(first==null)first=token;receipts.requireNew(token);receipts.bind(token);receipts.acknowledge(new RemoteMutationStatus(token,RemoteMutationStatus.State.ACKNOWLEDGED,1,0,0,CommandResult.Status.SUCCESS,new byte[32]));}
        var reopened=new RatisRemoteMutationReceipts(file,authority,true,RatisJournalAccess.standalone());assertEquals(1024,reopened.nextSequence());assertEquals(RemoteMutationStatus.State.EXPIRED,reopened.find(first).state());assertFalse(reopened.find(first).progressKnown());var original=first;assertThrows(IOException.class,() -> reopened.requireNew(original));
        var next=new RemoteMutationToken(cluster,database,domain,owner,UUID.randomUUID(),1,1024,RemoteMutationToken.Kind.POINT,new byte[32],new byte[32],new byte[32]);reopened.requireNew(next);assertTrue(java.nio.file.Files.size(file)<128*1024);
        var changedOwner=new RemoteMutationToken(cluster,database,domain,UUID.randomUUID(),first.requestId(),1,0,RemoteMutationToken.Kind.POINT,new byte[32],new byte[32],new byte[32]);assertThrows(IOException.class,() -> reopened.find(changedOwner));
        java.nio.file.Files.delete(file);assertThrows(IOException.class,() -> new RatisRemoteMutationReceipts(file,authority,true,RatisJournalAccess.standalone()));
    }
}
