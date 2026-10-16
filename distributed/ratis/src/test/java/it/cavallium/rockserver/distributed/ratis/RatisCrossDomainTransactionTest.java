package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual two-pool DATA/HOME kernels; transport dispatch and interruption barriers are controlled. */
class RatisCrossDomainTransactionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final Duration TTL=Duration.ofSeconds(30);
    static final RequestContext CONTEXT=RequestContext.batch();
    static Keys firstKey(){return new Keys(Buf.wrap(new byte[]{7}));}
    static Keys secondKey(){return new Keys(Buf.wrap(new byte[]{0,7}));}
    static Buf value(int n){return Buf.wrap(new byte[]{(byte)n});}
    static <T>T await(CompletableFuture<T> future)throws Exception{return future.get(20,TimeUnit.SECONDS);}
    static TransactionOpenScope declaration(){return new TransactionOpenScope(List.of(TransactionOpenScope.Span.point(71,firstKey()),TransactionOpenScope.Span.point(72,secondKey())));}
    static void write(RatisDurableSessionClient writer,UUID column,byte[] key,int value)throws IOException{var pending=writer.prepare(List.of(new Mutation(Mutation.Kind.PUT,column,key,new byte[]{(byte)value})));assertEquals(CommandResult.Status.SUCCESS,writer.execute(pending).status());writer.acknowledge(pending);}
    static void bytes(int expected,Buf value){assertNotNull(value);assertArrayEquals(new byte[]{(byte)expected},value.toByteArray());}
    static Map<RatisTwoPoolWorkflowFixture.CaptureIdentity,RatisSnapshotCaptureRequest.Result> captures(RatisTwoPoolWorkflowFixture fixture){
        var requests=new LinkedHashMap<RatisTwoPoolWorkflowFixture.CaptureIdentity,RatisTwoPoolWorkflowFixture.CaptureAttempt>();
        for(var attempt:fixture.transactionCaptures){var previous=requests.putIfAbsent(attempt.identity(),attempt);if(previous!=null){assertArrayEquals(previous.bytes(),attempt.bytes(),"Capture retry changed the exact original request");assertEquals(previous.request(),attempt.request());}}
        assertEquals(requests.keySet(),fixture.transactionCaptureResults.keySet());return Map.copyOf(fixture.transactionCaptureResults);
    }
    @Test void commonOpenCapturesOneCutAcrossDifferentSchemasAndPoolsThenCommitsOneOrderedOutcome()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root.resolve("native"),false,List.of(),true);
            var firstWriter=fixture.transactionWriter(root.resolve("writer-first"),fixture.first.domainId());
            var secondWriter=fixture.transactionWriter(root.resolve("writer-second"),fixture.second.domainId());
            var backend=fixture.transactionBackend(root.resolve("backend"))) {
            assertNotEquals(fixture.first.routingPrefixBytes(),fixture.second.routingPrefixBytes());
            assertNotEquals(fixture.first.column(71).schema().keys(),fixture.second.column(72).schema().keys());
            assertNotSame(fixture.node.nativePool(fixture.fast),fixture.node.nativePool(fixture.archive));
            var connection=backend.open(URI.create("ratis://controlled/cross-domain"));var api=connection.getAsyncApi(CONTEXT);
            var installed=new CountDownLatch(1);var proceed=new CountDownLatch(1);var paused=new AtomicBoolean();
            fixture.afterSnapshotInstall=gate->{assertEquals(2,gate.fence().domains().size());if(paused.compareAndSet(false,true)){installed.countDown();try{if(!proceed.await(10,TimeUnit.SECONDS))throw new IOException("Gate barrier timed out");}catch(Exception failure){throw new CompletionException(failure);}}};
            var opening=api.openTransactionAsync(declaration(),TTL);
            try{assertTrue(installed.await(10,TimeUnit.SECONDS));assertFalse(opening.isDone());write(secondWriter,fixture.second.column(72).columnId(),new byte[]{0,7},23);}finally{proceed.countDown();}
            long reader=await(opening);fixture.afterSnapshotInstall=null;
            assertEquals(2,captures(fixture).size());var fence=fixture.transactionCaptures.getFirst().request().fence();assertEquals(2,fence.domains().size());assertTrue(fixture.transactionCaptures.stream().allMatch(attempt->fence.equals(attempt.request().fence())));assertEquals(2,fixture.transactionCaptures.stream().map(attempt->attempt.identity().group()).distinct().count());
            write(firstWriter,fixture.first.column(71).columnId(),new byte[]{7},40);write(secondWriter,fixture.second.column(72).columnId(),new byte[]{0,7},50);
            bytes(11,await(api.getAsync(reader,71,firstKey(),RequestType.<Buf>current())));bytes(23,await(api.getAsync(reader,72,secondKey(),RequestType.<Buf>current())));assertTrue(await(api.closeTransactionAsync(reader,false)));
            long transaction=await(api.openTransactionAsync(declaration(),TTL));var originalCaptures=captures(fixture);
            bytes(40,await(api.putAsync(transaction,71,firstKey(),value(41),RequestType.<Buf>previous())));
            bytes(50,await(api.putAsync(transaction,72,secondKey(),value(51),RequestType.<Buf>previous())));
            bytes(41,await(api.putAsync(transaction,71,firstKey(),value(42),RequestType.<Buf>previous())));
            bytes(42,await(api.getAsync(transaction,71,firstKey(),RequestType.<Buf>current())));bytes(51,await(api.getAsync(transaction,72,secondKey(),RequestType.<Buf>current())));
            fixture.losePrepareReply=fixture.firstDirectory.ranges().getFirst().groupId();
            assertThrows(ExecutionException.class,()->await(api.closeTransactionAsync(transaction,true)));
            fixture.loseCommitReply=true;assertThrows(ExecutionException.class,()->await(api.closeTransactionAsync(transaction,true)));
            fixture.loseFinalizeReply=fixture.secondDirectory.ranges().getFirst().groupId();assertThrows(ExecutionException.class,()->await(api.closeTransactionAsync(transaction,true)));
            assertTrue(await(api.closeTransactionAsync(transaction,true)));assertEquals(originalCaptures,captures(fixture));
            bytes(42,await(api.getAsync(0,71,firstKey(),RequestType.<Buf>current())));bytes(51,await(api.getAsync(0,72,secondKey(),RequestType.<Buf>current())));
            long conflict=await(api.openTransactionAsync(declaration(),TTL));originalCaptures=captures(fixture);
            bytes(51,await(api.getAsync(conflict,72,secondKey(),RequestType.<Buf>current())));await(api.putAsync(conflict,71,firstKey(),value(99),RequestType.none()));
            write(secondWriter,fixture.second.column(72).columnId(),new byte[]{0,7},52);
            assertFalse(await(api.closeTransactionAsync(conflict,true)));assertFalse(await(api.closeTransactionAsync(conflict,true)));assertEquals(originalCaptures,captures(fixture));assertTrue(await(api.closeTransactionAsync(conflict,false)));
            bytes(42,await(api.getAsync(0,71,firstKey(),RequestType.<Buf>current())));bytes(52,await(api.getAsync(0,72,secondKey(),RequestType.<Buf>current())));
            for(var directory:List.of(fixture.firstDirectory,fixture.secondDirectory))for(var range:directory.ranges())assertEquals(0,fixture.store(range.groupId()).activeSnapshotPins());
        }
    }
    @Test void sixtyFiveParticipantsAcrossDomainsFailBeforeAnyTransactionGrantOrCapture()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root.resolve("bounded-native"),false,List.of(),true);var backend=fixture.transactionBackend(root.resolve("bounded-backend"))){
            var connection=backend.open(URI.create("ratis://controlled/bounded"));long epochs=fixture.transactionEpoch();var original=fixture.firstDirectory;var ranges=new ArrayList<RangeDescriptor>();
            for(int n=0;n<64;n++)ranges.add(new RangeDescriptor(new UUID(994,n+1),new UUID(995,n+1),1,0,n==0?null:new byte[]{(byte)n},n==63?null:new byte[]{(byte)(n+1)},RangeDescriptor.TopologyState.ACTIVE,original.ranges().getFirst().replicas()));
            fixture.routes.get(fixture.first.domainId()).refresh(new RangeDirectory(original.clusterId(),original.databaseId(),original.domainId(),original.schemaEpoch(),original.schemaDigest(),original.generation()+1,original.columns(),original.routingPrefixBytes(),ranges));
            var declaration=new TransactionOpenScope(List.of(TransactionOpenScope.Span.range(71,null,null),TransactionOpenScope.Span.point(72,secondKey())));
            var failure=assertThrows(ExecutionException.class,()->await(connection.getAsyncApi(CONTEXT).openTransactionAsync(declaration,TTL)));assertInstanceOf(IllegalArgumentException.class,failure.getCause());assertTrue(failure.getCause().getMessage().contains("participant"));
            assertEquals(epochs,fixture.transactionEpoch());assertTrue(fixture.transactionCaptures.isEmpty());
        }
    }
    @Test void cancelledOpenDrainsOneGlobalAcquisitionAndReleasesBothNativePools()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root.resolve("cancel-native"),false,List.of(),true);var backend=fixture.transactionBackend(root.resolve("cancel-backend"))){
            var connection=backend.open(URI.create("ratis://controlled/cancel"));var installed=new CountDownLatch(1);var release=new CountDownLatch(1);var paused=new AtomicBoolean();
            fixture.afterSnapshotInstall=gate->{if(paused.compareAndSet(false,true)){installed.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Cancelled gate barrier timed out");}catch(Exception failure){throw new CompletionException(failure);}}};
            var opening=connection.getAsyncApi(CONTEXT).openTransactionAsync(declaration(),TTL);
            try{assertTrue(installed.await(10,TimeUnit.SECONDS));assertTrue(opening.cancel(false));}finally{release.countDown();}
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);boolean closed=false;IOException last=null;
            while(!closed && System.nanoTime()<until){try{connection.close();closed=true;}catch(IOException active){last=active;Thread.sleep(5);}}
            assertTrue(closed,"Actual cancelled owners did not drain: "+lastFailure(last));
            for(var directory:List.of(fixture.firstDirectory,fixture.secondDirectory))for(var range:directory.ranges())assertEquals(0,fixture.store(range.groupId()).activeSnapshotPins());
        }
    }
    static String lastFailure(IOException failure){return failure==null?"none":failure.getMessage();}

    @Test void committedGlobalDecisionColdResumesTheExactJournalAfterBothDataPoolsRestart()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root.resolve("cold-native"),false,List.of(),true)){
            Path allocatorRoot=root.resolve("cold-identities");RatisRocksDBAdapter.Allocation allocation;TransactionStatus definition,decision;
            Map<UUID,UUID> originalHandles;Map<UUID,byte[]> originalPlans;Map<RatisTwoPoolWorkflowFixture.CaptureIdentity,RatisSnapshotCaptureRequest.Result> originalCaptures;
            var snapshotPlacement=fixture.plan.home(RatisNodePlan.Role.SNAPSHOT_HOME);var transactionPlacement=fixture.plan.home(RatisNodePlan.Role.TRANSACTION_HOME);
            try(var snapshotRoute=fixture.transactionHome(RatisNodePlan.Role.SNAPSHOT_HOME);var transactionRoute=fixture.transactionHome(RatisNodePlan.Role.TRANSACTION_HOME)){
                var bindings=fixture.transactionBindings();var snapshots=new RatisSnapshotFences(snapshotRoute,snapshotPlacement.column());var homes=new RatisTransactionStatuses(transactionRoute,transactionPlacement.column());
                try(var allocator=fixture.transactionAllocator(allocatorRoot,bindings,snapshotRoute,transactionRoute);var client=new RatisTransactionClient(bindings,snapshots,homes,fixture.transactionScheduler(),RatisTransactionClient.Limits.defaults())){
                    var scope=bindings.scope(declaration());allocation=await(allocator.allocate(CONTEXT,bindings.targets(scope)));
                    try(var lease=client.open(allocation.snapshot(),allocation.commit(),allocation.identity(),scope,TTL,CONTEXT)){
                        await(lease.completion());await(lease.stage(List.of(RatisLogicalClient.Change.put(72,secondKey(),value(61),Mutation.ReturnMode.PREVIOUS),RatisLogicalClient.Change.put(71,firstKey(),value(62),Mutation.ReturnMode.PREVIOUS)),List.of(),CONTEXT));
                        originalCaptures=captures(fixture);assertEquals(2,originalCaptures.size());fixture.loseCommitReply=true;var operation=lease.commit(CONTEXT);
                        assertThrows(ExecutionException.class,()->await(operation.completion()));var saved=operation.snapshot();definition=saved.initial();assertNull(definition.origin());assertEquals(2,definition.domains().size());assertEquals(allocation.identity().commit().business(),definition.operation());
                        decision=homes.read(definition.transactionId()).terminalDecision();assertEquals(TransactionStatus.Phase.COMMIT,decision.phase());assertTrue(saved.participants().stream().allMatch(participant->participant.vote()!=null && participant.vote().prepared()));
                        originalHandles=new HashMap<>();originalPlans=new HashMap<>();for(var participant:saved.participants()){originalHandles.put(participant.intent().route().groupId(),participant.intent().captureId());originalPlans.put(participant.intent().route().groupId(),DistributedCommandCodec.encode(participant.intent().acceptedBusiness()));}
                    }
                }
            }
            long epochs=fixture.transactionEpoch();var process=fixture.transactionProcess();var fastPool=fixture.node.nativePool(fixture.fast);var archivePool=fixture.node.nativePool(fixture.archive);
            fixture.reopenTransactionDataPools();assertNotEquals(process,fixture.transactionProcess());assertNotSame(fastPool,fixture.node.nativePool(fixture.fast));assertNotSame(archivePool,fixture.node.nativePool(fixture.archive));
            try(var snapshotRoute=fixture.transactionHome(RatisNodePlan.Role.SNAPSHOT_HOME);var transactionRoute=fixture.transactionHome(RatisNodePlan.Role.TRANSACTION_HOME)){
                var bindings=fixture.transactionBindings();var snapshots=new RatisSnapshotFences(snapshotRoute,snapshotPlacement.column());var homes=new RatisTransactionStatuses(transactionRoute,transactionPlacement.column());
                try(var allocator=fixture.transactionAllocator(allocatorRoot,bindings,snapshotRoute,transactionRoute);var client=new RatisTransactionClient(bindings,snapshots,homes,fixture.transactionScheduler(),RatisTransactionClient.Limits.defaults());var operation=client.reopenCommit(allocation.commit(),CONTEXT)){
                    var result=await(operation.completion());assertEquals(CommandResult.Status.SUCCESS,result.status());assertEquals(2,result.valueCount());assertArrayEquals(new byte[]{22},result.value(0));assertArrayEquals(new byte[]{11},result.value(1));
                    var restored=operation.snapshot();assertEquals(definition,restored.initial());assertEquals(decision,homes.read(definition.transactionId()).terminalDecision());
                    for(var participant:restored.participants()){var group=participant.intent().route().groupId();assertEquals(originalHandles.get(group),participant.intent().captureId());assertArrayEquals(originalPlans.get(group),DistributedCommandCodec.encode(participant.intent().acceptedBusiness()));}
                    await(operation.consume(CONTEXT));assertTrue(homes.read(definition.transactionId()).consumed());assertEquals(originalCaptures,captures(fixture));assertEquals(epochs,fixture.transactionEpoch());
                    try(var cleanup=client.reopenForRelease(allocation.snapshot(),CONTEXT)){await(cleanup.completion());}
                    assertArrayEquals(new byte[]{62},fixture.routes.get(fixture.first.domainId()).read(fixture.first.column(71).columnId(),new byte[]{7}));assertArrayEquals(new byte[]{61},fixture.routes.get(fixture.second.domainId()).read(fixture.second.column(72).columnId(),new byte[]{0,7}));
                    for(var pool:List.of(fixture.fast,fixture.archive))assertEquals(0,fixture.node.nativePool(pool).snapshotAdmissionStats().pins());
                }
            }
        }
    }
    @Test void sameSchemaSplitRequiresOriginalWritableHandleToDrainThenFreshCrossDomainOpensUseDescendants()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root.resolve("topology-native"),false,List.of(),true);var backend=fixture.transactionBackend(root.resolve("topology-backend"))){
            var connection=backend.open(URI.create("ratis://controlled/topology-cross-domain"));var api=connection.getAsyncApi(CONTEXT);long old=await(api.openTransactionAsync(declaration(),TTL));
            var originalCaptures=captures(fixture);assertEquals(2,originalCaptures.size());var originalFence=fixture.transactionCaptures.getFirst().request().fence();var parent=fixture.firstDirectory.ranges().getFirst().groupId();
            var split=fixture.beginSplit();var blocked=fixture.prepareSplitFreeze(split);assertFalse(blocked.ready());assertTrue(blocked.snapshotPins()>0);assertNull(blocked.freeze());
            var refusal=fixture.splitFreezeRefusal(split);assertNotNull(refusal);assertTrue(refusal.getMessage().contains("current drained preparation"));assertEquals(originalCaptures,captures(fixture));
            assertTrue(await(api.closeTransactionAsync(old,false)));assertEquals(originalCaptures,captures(fixture));
            fixture.split(split);var current=fixture.routes.get(fixture.first.domainId()).directory();assertEquals(fixture.first.schemaEpoch(),current.schemaEpoch());assertArrayEquals(fixture.first.digest(),current.schemaDigest());assertNotEquals(DomainBinding.of(fixture.firstDirectory),DomainBinding.of(current));assertFalse(current.ranges().stream().anyMatch(range->range.groupId().equals(parent)));
            assertEquals(originalFence.domains(),fixture.frozenTransactionRoutes(originalFence.domains()).authority(),"Frozen HOME proofs must retain exact pre-split directory generations");
            long fresh=await(api.openTransactionAsync(declaration(),TTL));var allCaptures=captures(fixture);assertEquals(4,allCaptures.size());var freshRequests=fixture.transactionCaptures.stream().filter(attempt->!attempt.request().fence().fenceId().equals(originalFence.fenceId())).toList();
            assertFalse(freshRequests.isEmpty());var freshFence=freshRequests.getFirst().request().fence();assertTrue(freshRequests.stream().allMatch(attempt->freshFence.equals(attempt.request().fence())));assertEquals(DomainBinding.of(current),freshFence.domain(fixture.first.domainId()));assertEquals(DomainBinding.of(fixture.secondDirectory),freshFence.domain(fixture.second.domainId()));
            assertFalse(freshRequests.stream().anyMatch(attempt->attempt.identity().group().equals(parent)));assertTrue(freshRequests.stream().anyMatch(attempt->split.targets().stream().anyMatch(child->child.groupId().equals(attempt.identity().group()))));
            await(api.putAsync(fresh,71,firstKey(),value(81),RequestType.none()));await(api.putAsync(fresh,72,secondKey(),value(82),RequestType.none()));assertTrue(await(api.closeTransactionAsync(fresh,true)));
            var closed=assertThrows(ExecutionException.class,()->await(api.getAsync(old,71,firstKey(),RequestType.<Buf>current())));assertInstanceOf(IOException.class,closed.getCause(),"A drained original owner must fail instead of following the current directory");
            assertEquals(allCaptures,captures(fixture));
            bytes(81,await(api.getAsync(0,71,firstKey(),RequestType.<Buf>current())));bytes(82,await(api.getAsync(0,72,secondKey(),RequestType.<Buf>current())));
            for(var pool:List.of(fixture.fast,fixture.archive))assertEquals(0,fixture.node.nativePool(pool).snapshotAdmissionStats().pins());
        }
    }

}
