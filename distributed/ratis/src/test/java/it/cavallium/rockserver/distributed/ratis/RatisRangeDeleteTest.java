package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import it.cavallium.rockserver.core.impl.RWScheduler;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

class RatisRangeDeleteTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private final List<RatisRequestBudget> budgets=new ArrayList<>();
    @org.junit.jupiter.api.AfterEach void closeBudgets() throws IOException {for(var budget:budgets)budget.close();}
    private RatisRangeDeleteClient openOwner(DistributedSchema schema,RangeDirectory directory,RatisRoutedClient data,RatisTransactionStatuses homes,RWScheduler scheduler,RatisRocksDBAdapter.AllocationSource allocations,Path folder)throws IOException {
        var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());budgets.add(budget);
        return openOwner(schema,directory,data,homes,scheduler,allocations,folder,budget);
    }
    private RatisRangeDeleteClient openOwner(DistributedSchema schema,RangeDirectory directory,RatisRoutedClient data,RatisTransactionStatuses homes,RWScheduler scheduler,RatisRocksDBAdapter.AllocationSource allocations,Path folder,RatisRequestBudget budget)throws IOException {
        var credit=budget.owner(RemoteMutationToken.Kind.RANGE,RangeDirectoryCodec.encode(directory).length,0);
        try{return new RatisRangeDeleteClient(schema,directory,data,homes,scheduler,allocations,folder,budget,credit);}
        catch(Throwable failure){credit.close();throw failure;}
    }
    static <T>T await(CompletableFuture<T> future)throws Exception{return future.get(15,TimeUnit.SECONDS);}
    static void seed(RatisAtomicClientTest.Fixture f,long column,Keys key,int value,long sequence)throws IOException{RatisRocksDBAdapterTest.seed(f,column,key,value(value),sequence);}
    static RemoteMutationToken prepare(RatisRangeDeleteClient owner,long column,Keys lower,Keys upper)throws IOException{var expected=owner.owner();var id=UUID.randomUUID();long sequence=expected.nextRequestSequence();return owner.prepare(expected,id,sequence,column,lower,upper);}
    static void acknowledge(RatisRangeDeleteClient owner,RemoteMutationToken token)throws IOException{owner.acknowledgeResult(token,owner.status(token).resultDigest());}
    static String stack(Throwable failure){var output=new StringWriter();failure.printStackTrace(new PrintWriter(output));return output.toString();}
    @Test void invalidCandidateBoundsDoNotGrowOriginalPendingRequestCredit()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("candidate-credits"),true)) {
            var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());budgets.add(budget);
            try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,new RatisRocksDBAdapterTest.Allocations(f),root.resolve("candidate-job"),budget)) {
                seed(f,71,key(2),1,1900);var token=prepare(owner,71,key(2),key(3));long charged=budget.chargedBytes();int rpcs=f.rpcs.get();
                var wrong=new Keys(it.cavallium.buffer.Buf.wrap(new byte[128*1024]));
                assertThrows(IllegalArgumentException.class,()->prepare(owner,71,wrong,key(3)));
                assertEquals(charged,budget.chargedBytes());assertEquals(rpcs,f.rpcs.get());assertEquals(0,f.opens.get());
                assertThrows(IOException.class,()->prepare(owner,71,key(2),key(4)));
                assertEquals(charged,budget.chargedBytes());assertEquals(rpcs,f.rpcs.get());
                await(owner.execute(token,RequestContext.batch()).observer());assertNull(f.get(71,key(2)));acknowledge(owner,token);
            }
        }
    }
    @Test void coldConstructorChargesAtomicMainBeforePublishingOwner()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("cold-disk-credits"),true)) {
            var folder=root.resolve("cold-disk-job");var allocations=new RatisRocksDBAdapterTest.Allocations(f);
            try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder)) {
                seed(f,71,key(2),1,1900);await(owner.execute(prepare(owner,71,key(2),key(3)),RequestContext.batch()).observer());
            }
            assertFalse(Files.exists(folder.resolve("atomic.pending")));
            var defaults=RemoteMutationOwnerLimits.defaults();long cap=16L<<20;
            var limits=new RemoteMutationOwnerLimits(defaults.pointSlots(),defaults.maximumPointOwners(),defaults.maximumBulkOwners(),defaults.maximumRangeOwners(),defaults.maximumRequests(),defaults.maximumBytes(),cap,defaults.maximumJournalPaths(),defaults.recoveryBytes(),defaults.renewalBytes(),defaults.recoveryWorkers());
            var budget=new RatisRequestBudget(limits);budgets.add(budget);
            long input=Files.size(folder.resolve("input")),atomic=Files.size(folder.resolve("atomic"));
            var filler=root.resolve("other-retained-journal");long length=cap-input-Math.max(1,atomic/2);
            try(var output=java.nio.channels.FileChannel.open(filler,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) {output.position(length-1);output.write(java.nio.ByteBuffer.wrap(new byte[1]));output.force(true);}
            try(var debt=budget.disk(filler,(int)cap)) {
                int captures=f.opens.get(),rpcs=f.rpcs.get();
                var failure=assertThrows(IOException.class,()->openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder,budget));
                assertTrue(stack(failure).contains("disk envelope"));assertEquals(captures,f.opens.get());assertEquals(rpcs,f.rpcs.get());
                assertEquals(0,budget.retainedOwners(RemoteMutationToken.Kind.RANGE));assertEquals(0,budget.retainedRequests());
            }
        }
    }
    @Test void sharedRequestCapacityRejectsBeforeCaptureAndCanBeReusedAfterClose()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("shared-credits"),true)) {
            var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());budgets.add(budget);
            var points=new ArrayList<RatisRequestBudget.Lease>();
            try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,new RatisRocksDBAdapterTest.Allocations(f),root.resolve("shared-job"),budget)) {
                for(int n=0;n<budget.limits().maximumRequests();n++)points.add(budget.request(RemoteMutationToken.Kind.POINT,1024));
                assertThrows(RocksDBException.class,()->prepare(owner,71,key(2),key(3)));assertEquals(0,f.opens.get());
                points.forEach(RatisRequestBudget.Lease::close);points.clear();
                seed(f,71,key(2),1,1900);var token=prepare(owner,71,key(2),key(3));
                assertEquals(1,budget.retainedRequests(RemoteMutationToken.Kind.RANGE));
                await(owner.execute(token,RequestContext.batch()).observer());assertNull(f.get(71,key(2)));acknowledge(owner,token);
                assertEquals(1,budget.retainedRequests(RemoteMutationToken.Kind.RANGE));
                token=prepare(owner,71,key(2),key(3));await(owner.execute(token,RequestContext.batch()).observer());acknowledge(owner,token);
                assertEquals(1,budget.retainedRequests(RemoteMutationToken.Kind.RANGE));
            } finally {points.forEach(RatisRequestBudget.Lease::close);}
            assertEquals(0,budget.retainedRequests());assertEquals(0,budget.retainedOwners(RemoteMutationToken.Kind.RANGE));assertTrue(budget.chargedDiskBytes()>0);
        }
    }
    @Test void cancelledHomeWorkRetainsCreditsUntilActualReturnAndExactRetry()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("held-credits"),true)) {
            var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());budgets.add(budget);
            var entered=new CountDownLatch(1);var released=new CountDownLatch(1);
            try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,new RatisRocksDBAdapterTest.Allocations(f),root.resolve("held-job"),budget)) {
                seed(f,71,key(2),1,1900);var token=prepare(owner,71,key(2),key(3));
                f.beforeHomeRead=()->{entered.countDown();try{if(!released.await(10,TimeUnit.SECONDS))throw new IllegalStateException("Held HOME timed out");}catch(InterruptedException failure){throw new RuntimeException(failure);}};
                var work=owner.execute(token,RequestContext.batch());
                try {
                    assertTrue(entered.await(10,TimeUnit.SECONDS));work.cancel().run();assertFalse(work.drained().isDone());
                    assertThrows(IOException.class,owner::close);assertEquals(1,budget.retainedRequests(RemoteMutationToken.Kind.RANGE));
                } finally {f.beforeHomeRead=null;released.countDown();}
                await(work.drained());int captures=f.opens.get();
                await(owner.execute(token,RequestContext.batch()).observer());assertEquals(captures,f.opens.get());assertNull(f.get(71,key(2)));acknowledge(owner,token);
            } finally {released.countDown();f.beforeHomeRead=null;}
            assertEquals(0,budget.retainedRequests());
        }
    }
    @Test void coldRangeReopenUsesSameDiskDebtAndOneRetainedRequest()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("cold-credits"),true)) {
            var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());budgets.add(budget);var folder=root.resolve("cold-credit-job");
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);seed(f,71,key(2),1,1900);RemoteMutationToken token;
            try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder,budget)) {token=prepare(owner,71,key(2),key(3));await(owner.execute(token,RequestContext.batch()).observer());}
            long disk=budget.chargedDiskBytes();assertTrue(disk>0);assertEquals(0,budget.retainedRequests());int opened=f.opens.get();
            try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder,budget)) {
                assertEquals(1,budget.retainedRequests(RemoteMutationToken.Kind.RANGE));await(owner.execute(token,RequestContext.batch()).observer());assertEquals(opened,f.opens.get());assertEquals(disk,budget.chargedDiskBytes());acknowledge(owner,token);
            }
            long retained=Files.size(folder.resolve("input"))+Files.size(folder.resolve("atomic"))+Files.size(folder.resolve("released"));
            assertEquals(retained,budget.chargedDiskBytes());assertEquals(0,budget.retainedRequests());assertEquals(1,allocations.count.get());
        }
    }
    @Test void expiredOriginalBusinessBeforeInitialHomeLeavesNoRowOrPrepareAndLocallySettlesExactFailure()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("native-expired"),true)) {
            seed(f,71,key(2),11,1900);seed(f,71,key(200),12,1901);var commit=f.separateIdentity(10);
            var snapshot=new RatisSnapshotClient.Identity(new UUID(990,1),new OperationId(f.session,1,600),new OperationId(f.session,1,601));var allocation=new RatisRocksDBAdapter.Allocation(root.resolve("unused-snapshot"),root.resolve("unused-commit"),new RatisTransactionClient.Identity(snapshot,commit));
            var allocations=new AtomicInteger();RatisRocksDBAdapter.AllocationSource source=(context,dataTargets)->{allocations.incrementAndGet();return CompletableFuture.completedFuture(allocation);};
            var expired=new AtomicBoolean();f.beforeHomeCommand=()->{if(!expired.compareAndSet(false,true))return;assertEquals(2,f.opens.get());try{var before=f.catalogue.epochs.read();var next=before.advanceTime(f.grant.expiresAtMillis());assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(7),before,next)).status());var home=f.homeDirectory.ranges().getFirst();var control=DistributedCommand.sessionControl(home.groupId(),home.rangeEpoch(),f.homeDirectory.schemaDigest(),new OperationId(commit.firstControl().sessionId(),commit.firstControl().epoch(),900),RangeSessionControl.advance(next),-1);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.append(home,DistributedCommandCodec.encode(control))).status());}catch(Exception failure){throw new RuntimeException(failure);}};
            Path folder=root.resolve("expired-range");RemoteMutationToken token;try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,source,folder)) {
                token=prepare(owner,71,key(2),key(201));var failure=assertThrows(ExecutionException.class,()->await(owner.execute(token,RequestContext.batch()).observer()));assertTrue(failure.getCause().getMessage().contains("SESSION_EXPIRED"),()->stack(failure.getCause()));f.beforeHomeCommand=null;
                assertNull(f.homes.read(commit.transaction()));for(var group:f.groups)assertNull(f.stores.get(group).preparedParticipant(commit.transaction()));assertArrayEquals(new byte[]{11},f.get(71,key(2)));assertArrayEquals(new byte[]{12},f.get(71,key(200)));assertEquals(1,allocations.get());assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,owner.recoveryState());assertEquals(CommandResult.Status.SESSION_EXPIRED,owner.status(token).outcome());acknowledge(owner,token);assertEquals(RatisRocksDBDirect.RecoveryState.NONE,owner.recoveryState());
            }
            int opened=f.opens.get();try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,source,folder)) {assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,owner.status(token).state());assertEquals(CommandResult.Status.SESSION_EXPIRED,owner.status(token).outcome());assertThrows(ExecutionException.class,()->await(owner.execute(token,RequestContext.batch()).observer()));assertEquals(opened,f.opens.get());assertEquals(1,allocations.get());assertNull(f.homes.read(commit.transaction()));}
        }
    }
    @Test void fixedBoundsClipBothShardsAndUnboundedSentinelDeletesMaximumCompleteKey()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("fixed"),true);var tx=RatisRocksDBAdapterTest.client(f,f.data);var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,new RatisRocksDBAdapterTest.Allocations(f),root.resolve("fixed-job"));var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(tx).bindings().domain((f.schema).domainId()).client()),tx,(context,dataTargets)->CompletableFuture.failedFuture(new IOException("No second allocator")),null,null).rangeDeletes(owner)) {
            seed(f,71,key(1),1,1900);seed(f,71,key(2),2,1901);seed(f,71,key(128),3,1902);seed(f,71,key(200),4,1903);seed(f,71,key(255),5,1904);seed(f,71,key(0),6,1905);
            assertThrows(ExecutionException.class,()->await(adapter.api(RequestContext.batch()).deleteRangeAsync(71,key(2),key(201))));assertEquals(0,f.opens.get());var first=prepare(owner,71,key(2),key(201));await(owner.execute(first,RequestContext.batch()).observer());assertArrayEquals(new byte[]{1},f.get(71,key(1)));assertNull(f.get(71,key(2)));assertNull(f.get(71,key(128)));assertNull(f.get(71,key(200)));assertArrayEquals(new byte[]{5},f.get(71,key(255)));assertEquals(2,f.opens.get());acknowledge(owner,first);
            var all=prepare(owner,71,null,null);await(owner.execute(all,RequestContext.batch()).observer());assertNull(f.get(71,key(0)));assertNull(f.get(71,key(1)));assertNull(f.get(71,key(255)));assertEquals(4,f.opens.get());acknowledge(owner,all);assertEquals(RatisRocksDBDirect.RecoveryState.NONE,owner.recoveryState());assertTrue(f.stores.values().stream().allMatch(store->store.activeSnapshotPins()==0));
        }
    }
    @Test void hashedRangeDeletesOnlyRequestedCollisionMembersAndKeepsBothNeighbours()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("hashed"),true);var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,new RatisRocksDBAdapterTest.Allocations(f),root.resolve("hashed-job"))) {
            seed(f,74,member(2,"alpha"),1,1900);seed(f,74,member(2,"bravo"),2,1901);seed(f,74,member(2,"charlie"),3,1902);seed(f,74,member(2,"delta"),4,1903);seed(f,74,member(200,"bravo"),5,1904);
            var first=prepare(owner,74,member(2,"bravo"),member(2,"delta"));await(owner.execute(first,RequestContext.batch()).observer());assertArrayEquals(new byte[]{1},f.get(74,member(2,"alpha")));assertNull(f.get(74,member(2,"bravo")));assertNull(f.get(74,member(2,"charlie")));assertArrayEquals(new byte[]{4},f.get(74,member(2,"delta")));assertArrayEquals(new byte[]{5},f.get(74,member(200,"bravo")));
            acknowledge(owner,first);var second=prepare(owner,74,key(2),key(201));await(owner.execute(second,RequestContext.batch()).observer());assertNull(f.get(74,member(2,"alpha")));assertNull(f.get(74,member(2,"delta")));assertNull(f.get(74,member(200,"bravo")));assertEquals(3,f.opens.get());acknowledge(owner,second);
        }
    }
    @Test void liveUnknownInitialHomeReplyResumesTheOwnedJournalWithoutOverlappingItsLock()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("live-unknown"),true);var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,new RatisRocksDBAdapterTest.Allocations(f),root.resolve("live-job"))) {
            seed(f,71,key(2),1,1900);seed(f,71,key(200),2,1901);f.beforeHomeRead=()->{throw new UncheckedIOException(new IOException("HOME proof temporarily unavailable"));};var token=prepare(owner,71,key(2),key(201));assertThrows(ExecutionException.class,()->await(owner.execute(token,RequestContext.batch()).observer()));int opened=f.opens.get();assertEquals(2,opened);assertArrayEquals(new byte[]{1},f.get(71,key(2)));f.beforeHomeRead=null;
            await(owner.execute(token,RequestContext.batch()).observer());assertEquals(opened,f.opens.get());assertNull(f.get(71,key(2)));assertNull(f.get(71,key(200)));acknowledge(owner,token);
        }
    }
    @Test void coldUnknownAcceptedJournalPreservesItsOriginalRemoteCuts()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("cold-unknown"),true)) {
            seed(f,71,key(2),1,1900);seed(f,71,key(200),2,1901);var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path folder=root.resolve("cold-job");RemoteMutationToken token;f.beforeHomeRead=()->{throw new UncheckedIOException(new IOException("Lost original HOME lookup"));};
            try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder)){token=prepare(owner,71,key(2),key(201));assertThrows(ExecutionException.class,()->await(owner.execute(token,RequestContext.batch()).observer()));}
            int opened=f.opens.get();assertEquals(2,opened);assertEquals(0,f.captureCloses.get());f.beforeHomeRead=null;
            try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder)){await(owner.execute(token,RequestContext.batch()).observer());assertEquals(opened,f.opens.get());assertEquals(1,allocations.count.get());assertNull(f.get(71,key(2)));assertNull(f.get(71,key(200)));acknowledge(owner,token);}
        }
    }
    @Test void genuineConcurrentWriteMakesRangeAbortAndConsumeBeforeExposingConflict()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("range-conflict"),true);var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,new RatisRocksDBAdapterTest.Allocations(f),root.resolve("conflict-job"))) {
            seed(f,71,key(2),1,1900);seed(f,71,key(200),2,1901);var once=new AtomicBoolean();f.beforeAuthorize=()->{if(once.compareAndSet(false,true))try{seed(f,71,key(2),9,1902);}catch(IOException failure){throw new UncheckedIOException(failure);}};
            var token=prepare(owner,71,key(2),key(201));var failure=assertThrows(ExecutionException.class,()->await(owner.execute(token,RequestContext.batch()).observer()));assertTrue(failure.getCause().getMessage().contains("CONFLICT"));assertArrayEquals(new byte[]{9},f.get(71,key(2)));assertArrayEquals(new byte[]{2},f.get(71,key(200)));assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,owner.recoveryState());assertEquals(CommandResult.Status.CONFLICT,owner.status(token).outcome());acknowledge(owner,token);assertEquals(RatisRocksDBDirect.RecoveryState.NONE,owner.recoveryState());
            var identity=new UUID(220,2);var home=f.homes.read(identity);assertEquals(TransactionStatus.Phase.ABORT,home.phase());assertTrue(home.fullyFinalized());assertTrue(home.consumed());
        }
    }
    @Test void ownerDeadlineAfterAcceptedCaptureDoesNotAllocateReplacementCuts()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("deadline"),true)) {
            seed(f,71,key(2),1,1900);seed(f,71,key(200),2,1901);var expired=new AtomicBoolean();var scheduler=spy(f.scheduler);
            doReturn(10L).when(scheduler).resolveMonotonicDeadline(any(RequestContext.class));doAnswer(call->expired.get()).when(scheduler).isMonotonicDeadlineExpired(anyLong());doAnswer(call->expired.get()?0L:TimeUnit.SECONDS.toNanos(1)).when(scheduler).remainingMonotonicDeadlineNanos(anyLong());doAnswer(call->f.scheduler.executor(call.getArgument(0),call.getArgument(1),Long.MAX_VALUE)).when(scheduler).executor(any(WorkloadProfile.class),any(OperationFamily.class),anyLong());
            try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,scheduler,new RatisRocksDBAdapterTest.Allocations(f),root.resolve("deadline-job"))) {var token=prepare(owner,71,key(2),key(201));f.beforeHomeRead=()->expired.set(true);assertThrows(ExecutionException.class,()->await(owner.execute(token,RequestContext.batch()).observer()));int opened=f.opens.get();assertEquals(2,opened);f.beforeHomeRead=null;expired.set(false);await(owner.execute(token,RequestContext.batch()).observer());assertEquals(opened,f.opens.get());assertNull(f.get(71,key(2)));assertNull(f.get(71,key(200)));acknowledge(owner,token);}
        }
    }
    @Test void failedConsumptionLiveRetryKeepsSavedResultAndExactEpochZeroRequest()throws Exception {consumptionRetry(false);}
    @Test void failedConsumptionColdRetryKeepsSavedResultAndExactEpochZeroRequest()throws Exception {consumptionRetry(true);}
    private void consumptionRetry(boolean cold)throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve(cold?"cold-consume":"live-consume"),true)) {
            seed(f,71,key(2),1,1900);seed(f,71,key(200),2,1901);var base=f.transport();var blocked=new AtomicBoolean();var lose=new AtomicBoolean(true);var controls=new ArrayList<byte[]>();
            try(var home=new RatisRoutedClient(f.homeDirectory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{if(blocked.get())throw new IOException("Applied consume proof unavailable");return base.read(route,column,key);}public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{return base.command(route,command);}public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh)throws IOException{if(blocked.get())throw new IOException("Retained consume proof unavailable");return base.transaction(route,bytes,fresh);}public byte[] participant(RangeDescriptor route,byte[] bytes)throws IOException{var result=base.participant(route,bytes);if(HomeRecovery.isRecovery(bytes) && HomeRecovery.decode(bytes).action()==HomeRecovery.Action.CONSUME){controls.add(bytes.clone());if(lose.getAndSet(false)){blocked.set(true);throw new IOException("Lost applied protected consume");}}return result;}})) {
                var homes=new RatisTransactionStatuses(home,f.homeColumn);var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path folder=root.resolve(cold?"cold-consume-job":"live-consume-job");var owner=openOwner(f.schema,f.directory,f.data,homes,f.scheduler,allocations,folder);
                try{var token=prepare(owner,71,key(2),key(201));var first=owner;assertThrows(ExecutionException.class,()->await(first.execute(token,RequestContext.batch()).observer()));assertEquals(1,controls.size());assertEquals(0,HomeRecovery.decode(controls.getFirst()).operation().epoch());int opened=f.opens.get();blocked.set(false);if(cold){owner.close();owner=openOwner(f.schema,f.directory,f.data,homes,f.scheduler,allocations,folder);}await(owner.execute(token,RequestContext.batch()).observer());assertEquals(1,controls.size());assertEquals(opened,f.opens.get());assertEquals(1,allocations.count.get());assertTrue(f.homes.read(new UUID(220,2)).consumed());assertNull(f.get(71,key(2)));assertNull(f.get(71,key(200)));acknowledge(owner,token);}
                finally{blocked.set(false);owner.close();}
            }
        }
    }

    @Test void emptyStagingResetPreservesOriginalCutAndCannotClearAFrozenPlan()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("reset"),true)) {
            seed(f,71,key(2),1,1900);var column=f.schema.column(71).columnId();var scope=new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{2})));
            try(var capture=f.data.openTransaction(scope,Duration.ofSeconds(30))) {var cut=capture.snapshot();var id=capture.captureId();var mutation=new Mutation(Mutation.Kind.PUT,column,new byte[]{2},new byte[]{9});capture.stage(List.of(mutation));assertArrayEquals(new byte[]{9},capture.get(column,new byte[]{2}));capture.stage(List.of());assertEquals(cut,capture.snapshot());assertEquals(id,capture.captureId());assertArrayEquals(new byte[]{1},capture.get(column,new byte[]{2}));capture.stage(List.of(mutation));var planned=capture.plan(new DistributedCommand(capture.descriptor().groupId(),capture.descriptor().rangeEpoch(),f.schema.digest(),new OperationId(f.session,1,500),List.of(mutation)));assertNotNull(planned.acceptance());assertThrows(IOException.class,()->capture.stage(List.of()));assertArrayEquals(new byte[]{1},f.get(71,key(2)));}
        }
    }

    @Test void completeInputSidecarWithoutMainRecoversOriginalCompletedOperation()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("input-only"),true)) {seed(f,71,key(2),1,1900);var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path folder=root.resolve("input-only-job");RemoteMutationToken token;try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder)){token=prepare(owner,71,key(2),key(3));await(owner.execute(token,RequestContext.batch()).observer());}Files.move(folder.resolve("input"),folder.resolve("input.pending"));int opened=f.opens.get();try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder)){await(owner.execute(token,RequestContext.batch()).observer());assertTrue(Files.isRegularFile(folder.resolve("input")));assertFalse(Files.exists(folder.resolve("input.pending")));assertEquals(opened,f.opens.get());assertEquals(1,allocations.count.get());assertNull(f.get(71,key(2)));acknowledge(owner,token);}}
    }
    @Test void completeInputSidecarExtendsOlderInputWithoutNewCapture()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("input-older"),true)) {seed(f,71,key(2),1,1900);seed(f,71,key(200),2,1901);Path folder=root.resolve("input-older-job");var original=new java.util.concurrent.atomic.AtomicReference<byte[]>();var base=f.transport();try(var routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] k)throws IOException{return base.read(r,c,k);}public CommandResult command(RangeDescriptor r,DistributedCommand c)throws IOException{return base.command(r,c);}public byte[] participant(RangeDescriptor r,byte[] b)throws IOException{return base.participant(r,b);}public byte[] transaction(RangeDescriptor r,byte[] b,boolean fresh)throws IOException{if(ScopedTransactionRpc.kind(b,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.OPEN && original.get()==null)original.set(Files.readAllBytes(folder.resolve("input")));return base.transaction(r,b,fresh);}})) {var allocations=new RatisRocksDBAdapterTest.Allocations(f);RemoteMutationToken token;f.beforeHomeRead=()->{throw new UncheckedIOException(new IOException("Keep original accepted operation unresolved"));};try(var owner=openOwner(f.schema,f.directory,routed,f.homes,f.scheduler,allocations,folder)){token=prepare(owner,71,key(2),key(201));assertThrows(ExecutionException.class,()->await(owner.execute(token,RequestContext.batch()).observer()));}byte[] latest=Files.readAllBytes(folder.resolve("input"));Files.write(folder.resolve("input.pending"),latest);Files.write(folder.resolve("input"),original.get());int opened=f.opens.get();f.beforeHomeRead=null;try(var owner=openOwner(f.schema,f.directory,routed,f.homes,f.scheduler,allocations,folder)){await(owner.execute(token,RequestContext.batch()).observer());assertEquals(opened,f.opens.get());assertEquals(1,allocations.count.get());assertFalse(Files.exists(folder.resolve("input.pending")));assertNull(f.get(71,key(2)));assertNull(f.get(71,key(200)));acknowledge(owner,token);}}}
    }
    @Test void completeAtomicSidecarColdRecoveryUsesExactAcceptedHandles()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("atomic-side"),true)) {seed(f,71,key(2),1,1900);seed(f,71,key(200),2,1901);var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path folder=root.resolve("atomic-side-job");RemoteMutationToken token;f.beforeHomeRead=()->{throw new UncheckedIOException(new IOException("Accepted HOME response unavailable"));};try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder)){token=prepare(owner,71,key(2),key(201));assertThrows(ExecutionException.class,()->await(owner.execute(token,RequestContext.batch()).observer()));}Files.move(folder.resolve("atomic"),folder.resolve("atomic.pending"));int opened=f.opens.get();f.beforeHomeRead=null;try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder)){await(owner.execute(token,RequestContext.batch()).observer());assertFalse(Files.exists(folder.resolve("atomic.pending")));assertTrue(Files.isRegularFile(folder.resolve("atomic")));assertEquals(opened,f.opens.get());assertEquals(1,allocations.count.get());assertNull(f.get(71,key(2)));assertNull(f.get(71,key(200)));acknowledge(owner,token);}}
    }
    @Test void changedChecksummedInputSidecarAndPartialAtomicSidecarAreRejected()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("bad-side"),true)) {seed(f,71,key(2),1,1900);var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path folder=root.resolve("bad-side-job");try(var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder)){var token=prepare(owner,71,key(2),key(3));await(owner.execute(token,RequestContext.batch()).observer());}byte[] input=SessionJournalFiles.read(folder.resolve("input"),RatisRangeDeleteClient.MAX_BYTES);java.nio.ByteBuffer.wrap(input).putLong(36+RemoteMutationToken.BYTES,72);SessionJournalFiles.write(folder.resolve("input.pending"),input,RatisRangeDeleteClient.MAX_BYTES);int opened=f.opens.get();assertThrows(IOException.class,()->openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder));assertEquals(opened,f.opens.get());Files.delete(folder.resolve("input.pending"));byte[] atomic=Files.readAllBytes(folder.resolve("atomic"));Files.write(folder.resolve("atomic.pending"),Arrays.copyOf(atomic,atomic.length-1));assertThrows(IOException.class,()->openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,allocations,folder));assertEquals(opened,f.opens.get());assertArrayEquals(atomic,Files.readAllBytes(folder.resolve("atomic")));}
    }
    @Test void cancellationAtActualDrainKeepsCompletedOriginalUntilExplicitDelivery()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("drain-cancel"),true);var owner=openOwner(f.schema,f.directory,f.data,f.homes,f.scheduler,new RatisRocksDBAdapterTest.Allocations(f),root.resolve("drain-cancel-job"))) {seed(f,71,key(2),1,1900);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var once=new AtomicBoolean();f.beforeHomeRead=()->{if(once.compareAndSet(false,true)){entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Actual HOME wait did not release");}catch(Exception failure){throw new RuntimeException(failure);}}};var token=prepare(owner,71,key(2),key(3));var owned=owner.execute(token,RequestContext.batch());assertTrue(entered.await(10,TimeUnit.SECONDS));var cancellation=owned.drained().thenRun(owned.cancel());release.countDown();await(cancellation);assertTrue(owned.observer().isCancelled());assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,owner.recoveryState());int opened=f.opens.get();seed(f,71,key(2),9,1901);f.beforeHomeRead=null;await(owner.execute(token,RequestContext.batch()).observer());assertEquals(opened,f.opens.get());assertArrayEquals(new byte[]{9},f.get(71,key(2)));assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,owner.recoveryState());acknowledge(owner,token);assertEquals(RatisRocksDBDirect.RecoveryState.NONE,owner.recoveryState());}
    }

}
