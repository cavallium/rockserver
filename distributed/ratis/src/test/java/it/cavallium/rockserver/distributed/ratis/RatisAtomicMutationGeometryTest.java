package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicMutationOwnerTest.*;
import static it.cavallium.rockserver.distributed.ratis.RatisCrossDomainTransactionTest.*;

/** Derived promises constrain actual encoder writes and independent native operations share one unchanged envelope. */
class RatisAtomicMutationGeometryTest {
    @TempDir(factory=RatisDiskTestDirectory.class)Path root;
    static String observed(CompletableFuture<?> view){if(!view.isDone())return "running";try{view.join();return "success";}catch(CancellationException cancelled){return "cancelled";}catch(CompletionException failed){return failed.getCause().toString();}}
    static String aStatus(RatisAtomicMutationOwner owner,RemoteMutationToken token){try{return owner.status(token).toString();}catch(IOException failure){return failure.toString();}}
    @Test void actualCompactJournalsFitTheirAdmittedBoundsAndColdInventoryRejectsAnUnderstatedHomeBound()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("bounds-native"),true);var budget=spy(new RatisRequestBudget(RemoteMutationOwnerLimits.defaults()))){
            var plans=new ConcurrentHashMap<Path,RatisJournalAccess.Reservation>();
            doAnswer(call->{List<RatisJournalAccess.Reservation> paths=call.getArgument(0);for(var path:paths)plans.put(path.path(),path);return call.callRealMethod();}).when(budget).preflight(anyList());
            doAnswer(call->{Path path=call.getArgument(0);byte[] body=call.getArgument(1);var planned=plans.get(path);if(planned!=null)assertTrue(body.length+32L<=planned.maximum(),"Actual checksum-inclusive journal exceeds its sealed bound: "+path);return call.callRealMethod();}).when(budget).write(any(Path.class),any(byte[].class),anyInt());
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path path=root.resolve("bounds-owner");RemoteMutationToken token;
            try(var actor=singleton(f,allocations,path,budget)){
                var owner=actor.owner();token=actor.prepare(owner,UUID.randomUUID(),0,List.of(new AtomicMutation(71,RatisAtomicClientTest.key(2),Mutation.Kind.PUT,Buf.wrap(new byte[]{9}),Mutation.ReturnMode.CURRENT)),List.of(),30000);
                assertTrue(budget.chargedDiskBytes()<1024*1024,"A tiny atomic request must not reserve fixed multi-megabyte journals");assertTrue(budget.chargedBytes()<32L*1024*1024);
                await(actor.execute(token,CONTEXT).observer());
                for(var name:List.of("input","snapshot","commit","released")){var file=path.resolve(name);var bound=plans.get(file);assertNotNull(bound);assertTrue(Files.size(file)<=bound.maximum());assertEquals(2L*bound.maximum(),bound.peakBytes());}
            }
            byte[] original=SessionJournalFiles.read(path.resolve("input"),RatisAtomicMutationOwner.MAX_BYTES);byte[] understated=original.clone();int tokenBytes=java.nio.ByteBuffer.wrap(understated).getInt(4);java.nio.ByteBuffer.wrap(understated).putLong(26+tokenBytes,0);SessionJournalFiles.write(path.resolve("input"),understated,RatisAtomicMutationOwner.MAX_BYTES);int calls=f.rpcs.get();
            assertThrows(IOException.class,()->singleton(f,allocations,path,budget));assertEquals(calls,f.rpcs.get());assertEquals(1,allocations.count.get());
            SessionJournalFiles.write(path.resolve("input"),original,RatisAtomicMutationOwner.MAX_BYTES);
            try(var cold=singleton(f,allocations,path,budget)){assertEquals(CommandResult.Status.SUCCESS,cold.status(token).outcome());cold.acknowledge(token,cold.status(token).resultDigest());}
        }
    }
    @Test void twoIndependentNativeDomainsRunConcurrentlyUnderOneBudgetAndDeniedThirdInputAllocatesNothing()throws Exception {
        try(var f=new RatisTwoPoolWorkflowFixture(root.resolve("concurrent-native"),false,List.of(),true,2);
            var snapshot=f.transactionHome(RatisNodePlan.Role.SNAPSHOT_HOME);var transaction=f.transactionHome(RatisNodePlan.Role.TRANSACTION_HOME);
            var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            var all=f.transactionBindings();var first=new RatisTransactionBindings(List.of(all.domain(f.first.domainId())));var second=new RatisTransactionBindings(List.of(all.domain(f.second.domainId())));
            var snapshots=new RatisSnapshotFences(snapshot,f.plan.home(RatisNodePlan.Role.SNAPSHOT_HOME).column());var homes=new RatisTransactionStatuses(transaction,f.plan.home(RatisNodePlan.Role.TRANSACTION_HOME).column());
            try(var firstAllocator=f.transactionAllocator(root.resolve("first-allocator"),first,snapshot,transaction,budget);var secondAllocator=f.transactionAllocator(root.resolve("second-allocator"),second,snapshot,transaction,budget);var thirdAllocator=f.transactionAllocator(root.resolve("third-allocator"),first,snapshot,transaction,budget)){
                var firstAllocations=new AtomicInteger();var secondAllocations=new AtomicInteger();var issuedFirst=new AtomicReference<RatisRocksDBAdapter.Allocation>();var issuedSecond=new AtomicReference<RatisRocksDBAdapter.Allocation>();
                RatisRocksDBAdapter.AllocationSource firstSource=(context,targets)->{firstAllocations.incrementAndGet();return firstAllocator.allocate(context,targets).thenApply(issued->{issuedFirst.set(issued);return issued;});};
                RatisRocksDBAdapter.AllocationSource secondSource=(context,targets)->{secondAllocations.incrementAndGet();return secondAllocator.allocate(context,targets).thenApply(issued->{issuedSecond.set(issued);return issued;});};
                var thirdAllocations=new AtomicInteger();RatisRocksDBAdapter.AllocationSource thirdSource=(context,targets)->{thirdAllocations.incrementAndGet();return thirdAllocator.allocate(context,targets);};
                try(var a=new RatisAtomicMutationOwner(first,snapshots,homes,f.transactionScheduler(),firstSource,root.resolve("first-owner"),budget);
                    var b=new RatisAtomicMutationOwner(second,snapshots,homes,f.transactionScheduler(),secondSource,root.resolve("second-owner"),budget);
                    var c=new RatisAtomicMutationOwner(first,snapshots,homes,f.transactionScheduler(),thirdSource,root.resolve("third-owner"),budget)){
                    var ta=a.prepare(a.owner(),UUID.randomUUID(),0,List.of(put(71,firstKey(),51,Mutation.ReturnMode.NONE)),List.of(),30000);var tb=b.prepare(b.owner(),UUID.randomUUID(),0,List.of(put(72,secondKey(),52,Mutation.ReturnMode.NONE)),List.of(),30000);
                    var entered=new CountDownLatch(2);var release=new CountDownLatch(1);var paused=ConcurrentHashMap.<UUID>newKeySet();f.afterSnapshotInstall=gate->{if(paused.add(gate.fence().fenceId())){entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Concurrent native gate timed out");}catch(Exception failure){throw new CompletionException(failure);}}};
                    var oa=a.execute(ta,CONTEXT);var ob=b.execute(tb,CONTEXT);
                    try{
                        assertTrue(entered.await(10,TimeUnit.SECONDS),()->"Both independent native gates must arrive; fences="+paused+", first="+observed(oa.observer())+", second="+observed(ob.observer()));
                        assertEquals(2,budget.retainedRequests(RemoteMutationToken.Kind.ATOMIC),"Both original jobs must remain retained at their native gates");
                        assertTrue(budget.chargedDiskBytes()<2L*1024*1024,()->"Tiny jobs plus actual grant journals exceeded the disk target: "+budget.chargedDiskBytes());
                        assertTrue(budget.chargedBytes()<32L*1024*1024,()->"Tiny jobs plus actual grant journals exceeded the RAM target: "+budget.chargedBytes());
                        assertTrue(oa.observer().cancel(false),"First observer must detach while native work is held");assertFalse(oa.drained().isDone(),"Cancellation must not drain the first held native owner");assertFalse(ob.drained().isDone(),"Second native owner must remain held independently");
                        try(var competing=budget.work(budget.limits().maximumBytes()-budget.chargedBytes()-64*1024)){
                            var refused=assertThrows(RocksDBException.class,()->c.prepare(c.owner(),UUID.randomUUID(),0,List.of(put(71,new Keys(Buf.wrap(new byte[]{8})),53,Mutation.ReturnMode.NONE)),List.of(),30000));assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,refused.getErrorUniqueId());assertEquals(0,thirdAllocations.get());assertTrue(f.transactionCaptures.isEmpty());
                        }
                    }finally{release.countDown();f.afterSnapshotInstall=null;await(oa.drained());await(ob.drained());}
                    var originalResults=Map.copyOf(f.transactionCaptureResults);
                    // Cancellation detaches observation. Any unknown original progress is driven through the same token.
                    try{await(a.execute(ta,CONTEXT).observer());}catch(ExecutionException failure){throw new AssertionError("First original-token resume failed; status="+aStatus(a,ta),failure.getCause());}
                    try{await(b.execute(tb,CONTEXT).observer());}catch(ExecutionException failure){throw new AssertionError("Second original-token resume failed; first observation="+observed(ob.observer()),failure.getCause());}
                    assertEquals(CommandResult.Status.SUCCESS,a.status(ta).outcome(),()->"First original native job did not finish: "+aStatus(a,ta));assertEquals(CommandResult.Status.SUCCESS,b.status(tb).outcome(),()->"Second original native job failed: "+observed(ob.observer()));
                    assertEquals(1,firstAllocations.get(),"Original-token resume must not allocate a replacement first identity");assertEquals(1,secondAllocations.get(),"Original-token resume must not allocate a replacement second identity");
                    var completed=captures(f);assertEquals(2,completed.size());originalResults.forEach((identity,captured)->assertEquals(captured,completed.get(identity),"Original native token/handle changed during resume"));
                    var originalIdentities=Map.of(issuedFirst.get().identity().snapshot().fence(),issuedFirst.get().identity().snapshot(),issuedSecond.get().identity().snapshot().fence(),issuedSecond.get().identity().snapshot());assertEquals(originalIdentities.keySet(),paused);
                    for(var attempt:f.transactionCaptures){var issued=originalIdentities.get(attempt.request().fence().fenceId());assertNotNull(issued,"Capture used an unissued fence identity");assertEquals(issued.business(),attempt.request().fence().operation());}
                    try(var firstJournal=RatisDurableTransaction.reopen(root.resolve("first-owner/commit"),homes,first);var secondJournal=RatisDurableTransaction.reopen(root.resolve("second-owner/commit"),homes,second)){
                        assertEquals(issuedFirst.get().identity().commit().transaction(),firstJournal.snapshot().initial().transactionId());assertEquals(issuedFirst.get().identity().commit().business(),firstJournal.snapshot().initial().operation());assertEquals(issuedSecond.get().identity().commit().transaction(),secondJournal.snapshot().initial().transactionId());assertEquals(issuedSecond.get().identity().commit().business(),secondJournal.snapshot().initial().operation());
                    }
                    a.acknowledge(ta,a.status(ta).resultDigest());b.acknowledge(tb,b.status(tb).resultDigest());assertEquals(0,budget.retainedRequests(RemoteMutationToken.Kind.ATOMIC));
                    var tc=c.prepare(c.owner(),UUID.randomUUID(),0,List.of(put(71,new Keys(Buf.wrap(new byte[]{8})),53,Mutation.ReturnMode.NONE)),List.of(),30000);await(c.execute(tc,CONTEXT).observer());assertEquals(CommandResult.Status.SUCCESS,c.status(tc).outcome());c.acknowledge(tc,c.status(tc).resultDigest());assertEquals(1,thirdAllocations.get());
                }
            }
            for(var pool:List.of(f.fast,f.archive))assertEquals(0,f.node.nativePool(pool).snapshotAdmissionStats().pins());
        }
    }
    @Test void acceptedParticipantEnvelopeOverflowRetainsResultTooLargeWithoutEffectsOrReplacementCapture()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("envelope-native"),true);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            byte[] before=new byte[1100*1024],replacement=new byte[1100*1024];Arrays.fill(before,(byte)7);Arrays.fill(replacement,(byte)9);var key=RatisAtomicClientTest.key(2);RatisRocksDBAdapterTest.seed(f,71,key,Buf.wrap(before),1900);
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path path=root.resolve("envelope-owner");RemoteMutationToken token;Map<RatisAtomicClientTest.Fixture.CaptureIdentity,RatisSnapshotCaptureRequest.Result> original;
            var mutations=List.of(new AtomicMutation(71,key,Mutation.Kind.PUT,Buf.wrap(replacement),Mutation.ReturnMode.PREVIOUS));
            try(var owner=singleton(f,allocations,path,budget)){
                token=owner.prepare(owner.owner(),UUID.randomUUID(),0,mutations,List.of(),30000);await(owner.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.RESULT_TOO_LARGE,owner.status(token).outcome());assertEquals(0,owner.status(token).appliedMutations());assertFalse(Files.exists(path.resolve("commit")));assertArrayEquals(before,f.get(71,key));original=Map.copyOf(f.captureResults);
            }
            int requests=f.captureRequests.get();try(var cold=singleton(f,allocations,path,budget)){
                await(cold.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.RESULT_TOO_LARGE,cold.status(token).outcome());assertEquals(requests,f.captureRequests.get());assertEquals(original,f.captureResults);assertEquals(1,allocations.count.get());cold.acknowledge(token,cold.status(token).resultDigest());
                var authority=cold.owner();var conflict=cold.prepare(authority,UUID.randomUUID(),authority.nextRequestSequence(),mutations,List.of(new AtomicPredicate(71,key,Buf.wrap(new byte[]{0}))),30000);await(cold.execute(conflict,CONTEXT).observer());assertEquals(CommandResult.Status.CONFLICT,cold.status(conflict).outcome(),"A known false original predicate wins over a potential accepted-envelope overflow");cold.acknowledge(conflict,cold.status(conflict).resultDigest());
            }
            assertArrayEquals(before,f.get(71,key));assertTrue(f.stores.values().stream().allMatch(store->store.activeSnapshotPins()==0));
        }
    }

    @Test void qualifiedNewerSnapshotSidecarPromotesWithTheEntireRemainingDiskBudgetAlreadyPromised()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("sidecar-native"),true);var budget=spy(new RatisRequestBudget(RemoteMutationOwnerLimits.defaults()))){
            Path path=root.resolve("sidecar-owner"),snapshot=path.resolve("snapshot");var oldest=new AtomicReference<byte[]>();var latest=new AtomicReference<byte[]>();var fail=new AtomicBoolean(true);
            doAnswer(call->{Path file=call.getArgument(0);byte[] body=call.getArgument(1);Object result=call.callRealMethod();if(file.equals(snapshot)){oldest.compareAndSet(null,body.clone());latest.set(body.clone());}if(file.equals(path.resolve("input"))){int tokenBytes=java.nio.ByteBuffer.wrap(body).getInt(4);if((body[24+tokenBytes]&255)==4 && fail.getAndSet(false))throw new IOException("Stop after the original complete captures, before staging");}return result;}).when(budget).write(any(Path.class),any(byte[].class),anyInt());
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);RemoteMutationToken token;
            try(var actor=singleton(f,allocations,path,budget)){var authority=actor.owner();token=actor.prepare(authority,UUID.randomUUID(),0,List.of(put(71,RatisAtomicClientTest.key(2),81,Mutation.ReturnMode.PREVIOUS)),List.of(),30000);assertThrows(ExecutionException.class,()->await(actor.execute(token,CONTEXT).observer()));assertFalse(fail.get());}
            assertNotNull(oldest.get());assertNotNull(latest.get());assertFalse(Arrays.equals(oldest.get(),latest.get()));
            SessionJournalFiles.write(snapshot,oldest.get(),RatisDurableSnapshot.MAX_BYTES);SessionJournalFiles.write(path.resolve("snapshot.pending"),latest.get(),RatisDurableSnapshot.MAX_BYTES);SessionJournalFiles.force(path);
            var captures=Map.copyOf(f.captureResults);int captureRequests=f.captureRequests.get();
            try(var cold=singleton(f,allocations,path,budget)){
                long available=budget.limits().maximumDiskBytes()-budget.chargedDiskBytes();var competing=new RatisJournalAccess.Reservation(root.resolve("other-promised-journal"),Math.toIntExact((available+1)/2),available);budget.preflight(List.of(competing));assertEquals(budget.limits().maximumDiskBytes(),budget.chargedDiskBytes());
                try{await(cold.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.SUCCESS,cold.status(token).outcome());assertEquals(captureRequests,f.captureRequests.get());assertEquals(captures,f.captureResults);assertEquals(1,allocations.count.get());assertFalse(Files.exists(path.resolve("snapshot.pending")));assertArrayEquals(new byte[]{81},f.get(71,RatisAtomicClientTest.key(2)));cold.acknowledge(token,cold.status(token).resultDigest());}finally{budget.fulfilled(competing);}
            }
            assertTrue(f.stores.values().stream().allMatch(store->store.activeSnapshotPins()==0));
        }
    }

}
