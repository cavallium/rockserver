package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisCrossDomainTransactionTest.*;

/** Public durable lifecycle against two actual heterogeneous DATA pools and native HOME kernels. */
class RatisAtomicMutationOwnerTest {
    @TempDir(factory=RatisDiskTestDirectory.class)Path root;
    static AtomicMutation put(long column,Keys key,int value,Mutation.ReturnMode returns){return new AtomicMutation(column,key,Mutation.Kind.PUT,value(value),returns);}
    static RemoteMutationOwner discover(RemoteMutationAsyncAPI api)throws Exception{return await(api.atomicOwners(List.of(71L,72L),CONTEXT)).getFirst();}
    static CommandResult result(RemoteMutationAsyncAPI api,RemoteMutationToken token)throws Exception{var page=await(api.results(token,0,1,CommandResult.MAX_BYTES,CONTEXT));assertEquals(1,page.rows().size());return CommandResult.decode(page.rows().getFirst());}

    @Test void publicCrossDomainOrderedReturnsOriginalPredicatesAndAmbiguousDecisionsRecoverOneIdentity()throws Exception {
        try(var f=new RatisTwoPoolWorkflowFixture(root.resolve("native"),false,List.of(),true);
            var firstWriter=f.transactionWriter(root.resolve("writer-one"),f.first.domainId());
            var secondWriter=f.transactionWriter(root.resolve("writer-two"),f.second.domainId());
            var backend=f.transactionBackend(root.resolve("backend"))){
            var connection=backend.open(URI.create("ratis://controlled/public-atomic"));var api=connection.getRemoteMutationApi();var owner=discover(api);assertEquals(2,owner.domains().size());
            var mutations=List.of(put(72,secondKey(),51,Mutation.ReturnMode.PREVIOUS),put(71,firstKey(),41,Mutation.ReturnMode.CURRENT),put(72,secondKey(),52,Mutation.ReturnMode.PREVIOUS));
            var predicates=List.of(new AtomicPredicate(71,firstKey(),value(11)),new AtomicPredicate(72,secondKey(),value(22)));
            var token=await(api.prepareAtomic(owner,UUID.randomUUID(),owner.nextRequestSequence(),mutations,predicates,30000,CONTEXT));assertEquals(owner.domains(),token.domains());
            assertEquals(token,await(api.prepareAtomic(owner,token.requestId(),token.requestSequence(),mutations,predicates,30000,CONTEXT)));
            f.losePrepareReply=f.firstDirectory.ranges().getFirst().groupId();assertThrows(ExecutionException.class,()->await(api.execute(token,CONTEXT)));var original=captures(f);assertEquals(2,original.size());
            f.loseCommitReply=true;assertThrows(ExecutionException.class,()->await(api.resume(token,CONTEXT)));
            f.loseFinalizeReply=f.secondDirectory.ranges().getFirst().groupId();assertThrows(ExecutionException.class,()->await(api.resume(token,CONTEXT)));
            var status=await(api.resume(token,CONTEXT));assertEquals(RemoteMutationStatus.State.TERMINAL,status.state());assertEquals(CommandResult.Status.SUCCESS,status.outcome());assertEquals(3,status.appliedMutations());assertEquals(original,captures(f));
            write(firstWriter,f.first.column(71).columnId(),new byte[]{7},90);write(secondWriter,f.second.column(72).columnId(),new byte[]{0,7},91);
            var saved=result(api,token);assertEquals(3,saved.valueCount());assertArrayEquals(new byte[]{22},saved.value(0));assertArrayEquals(new byte[]{41},saved.value(1));assertArrayEquals(new byte[]{51},saved.value(2));
            assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,await(api.acknowledge(token,status.resultDigest(),CONTEXT)).state());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,await(api.acknowledge(token,status.resultDigest(),CONTEXT)).state());
            for(var pool:List.of(f.fast,f.archive))assertEquals(0,f.node.nativePool(pool).snapshotAdmissionStats().pins());
        }
    }
    @Test void predicateOnlyDomainCertifiesOriginalImageAndConflictHasNoEffects()throws Exception {
        try(var f=new RatisTwoPoolWorkflowFixture(root.resolve("predicates-native"),false,List.of(),true);var backend=f.transactionBackend(root.resolve("predicates-backend"))){
            var api=backend.open(URI.create("ratis://controlled/predicate-only-domain")).getRemoteMutationApi();var owner=discover(api);
            var token=await(api.prepareAtomic(owner,UUID.randomUUID(),owner.nextRequestSequence(),List.of(put(71,firstKey(),99,Mutation.ReturnMode.PREVIOUS)),List.of(new AtomicPredicate(72,secondKey(),value(99))),30000,CONTEXT));
            var status=await(api.execute(token,CONTEXT));assertEquals(CommandResult.Status.CONFLICT,status.outcome());assertEquals(0,status.appliedMutations());assertEquals(2,captures(f).size());assertEquals(0,result(api,token).valueCount());assertArrayEquals(new byte[]{11},f.routes.get(f.first.domainId()).read(f.first.column(71).columnId(),new byte[]{7}));
            await(api.acknowledge(token,status.resultDigest(),CONTEXT));owner=discover(api);
            var onlyPredicates=List.of(new AtomicPredicate(71,firstKey(),value(11)),new AtomicPredicate(72,secondKey(),value(22)));
            var predicateToken=await(api.prepareAtomic(owner,UUID.randomUUID(),owner.nextRequestSequence(),List.of(),onlyPredicates,30000,CONTEXT));var success=await(api.execute(predicateToken,CONTEXT));assertEquals(CommandResult.Status.SUCCESS,success.outcome());assertEquals(0,success.appliedMutations());assertEquals(0,result(api,predicateToken).valueCount());await(api.acknowledge(predicateToken,success.resultDigest(),CONTEXT));
        }
    }
    @Test void callerMutationAfterPrepareCannotChangeForcedInputAndOversizeRefusesBeforeAllocation()throws Exception {
        try(var f=new RatisTwoPoolWorkflowFixture(root.resolve("intake-native"),false,List.of(),true);var backend=f.transactionBackend(root.resolve("intake-backend"))){
            var api=backend.open(URI.create("ratis://controlled/intake")).getRemoteMutationApi();var owner=discover(api);long epoch=f.transactionEpoch();
            assertThrows(ExecutionException.class,()->await(api.prepareAtomic(owner,UUID.randomUUID(),owner.nextRequestSequence(),List.of(new AtomicMutation(71,firstKey(),Mutation.Kind.PUT,Buf.wrap(new byte[2*1024*1024]),Mutation.ReturnMode.NONE)),List.of(),30000,CONTEXT)));assertEquals(epoch,f.transactionEpoch());assertTrue(f.transactionCaptures.isEmpty());
            byte[] key={7},value={61},expected={22};var mutable=new ArrayList<AtomicMutation>();mutable.add(new AtomicMutation(71,new Keys(Buf.wrap(key)),Mutation.Kind.PUT,Buf.wrap(value),Mutation.ReturnMode.CURRENT));
            var preparing=api.prepareAtomic(owner,UUID.randomUUID(),owner.nextRequestSequence(),mutable,List.of(new AtomicPredicate(72,secondKey(),Buf.wrap(expected))),30000,CONTEXT);key[0]=8;value[0]=62;expected[0]=23;mutable.clear();
            var token=await(preparing);var status=await(api.execute(token,CONTEXT));assertEquals(CommandResult.Status.SUCCESS,status.outcome());assertArrayEquals(new byte[]{61},result(api,token).value(0));assertArrayEquals(new byte[]{61},f.routes.get(f.first.domainId()).read(f.first.column(71).columnId(),new byte[]{7}));await(api.acknowledge(token,status.resultDigest(),CONTEXT));
        }
    }
    @Test void noncommutativeMergeOperandsStayOrderedAcrossDomains()throws Exception {
        try(var f=new RatisTwoPoolWorkflowFixture(root.resolve("merge-native"),false,List.of(),true)){
            var merged=f.createOperatorAndMerge();
            // ManualTimer does not perform periodic census during operator provisioning.
            RatisTopologySeedRuntimeTest.field(f.node,"diskCensus",RatisDiskRunwayService.class).initialCensus();
            var original=DomainBinding.of(f.secondDirectory);assertEquals(List.of(original),f.frozenTransactionRoutes(List.of(original)).authority(),"HOME proof resolution must retain the original schema binding after activation");
            try(var backend=f.transactionBackend(root.resolve("merge-backend"))){
                var api=backend.open(URI.create("ratis://controlled/atomic-merge")).getRemoteMutationApi();var owner=await(api.atomicOwners(List.of(71L,merged.api()),CONTEXT)).getFirst();var mergeKey=new Keys(Buf.wrap(new byte[]{9}));
                var mutations=List.of(new AtomicMutation(merged.api(),mergeKey,Mutation.Kind.MERGE,value(44),Mutation.ReturnMode.PREVIOUS),put(71,firstKey(),81,Mutation.ReturnMode.CURRENT),new AtomicMutation(merged.api(),mergeKey,Mutation.Kind.MERGE,value(55),Mutation.ReturnMode.CURRENT));
                var token=await(api.prepareAtomic(owner,UUID.randomUUID(),owner.nextRequestSequence(),mutations,List.of(new AtomicPredicate(merged.api(),mergeKey,Buf.wrap(new byte[]{11,33}))),30000,CONTEXT));
                var status=await(api.execute(token,CONTEXT));assertEquals(CommandResult.Status.SUCCESS,status.outcome());var saved=result(api,token);assertArrayEquals(new byte[]{11,33},saved.value(0));assertArrayEquals(new byte[]{81},saved.value(1));assertArrayEquals(new byte[]{11,33,44,55},saved.value(2));assertArrayEquals(new byte[]{11,33,44,55},f.routes.get(f.second.domainId()).read(merged.column(),new byte[]{9}));await(api.acknowledge(token,status.resultDigest(),CONTEXT));
            }
        }
    }
    @Test void coldCommittedOuterOwnerRecoversSavedOriginalResultAfterBothDataPoolsRestart()throws Exception {
        try(var f=new RatisTwoPoolWorkflowFixture(root.resolve("cold-native"),false,List.of(),true)){
            Path ownerRoot=root.resolve("owner"),allocatorRoot=root.resolve("allocator");RemoteMutationToken token;Map<RatisTwoPoolWorkflowFixture.CaptureIdentity,RatisSnapshotCaptureRequest.Result> original;
            var snapshotPlacement=f.plan.home(RatisNodePlan.Role.SNAPSHOT_HOME);var transactionPlacement=f.plan.home(RatisNodePlan.Role.TRANSACTION_HOME);
            try(var snapshotRoute=f.transactionHome(RatisNodePlan.Role.SNAPSHOT_HOME);var transactionRoute=f.transactionHome(RatisNodePlan.Role.TRANSACTION_HOME);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
                var bindings=f.transactionBindings();var snapshots=new RatisSnapshotFences(snapshotRoute,snapshotPlacement.column());var homes=new RatisTransactionStatuses(transactionRoute,transactionPlacement.column());
                try(var allocator=f.transactionAllocator(allocatorRoot,bindings,snapshotRoute,transactionRoute);var owner=new RatisAtomicMutationOwner(bindings,snapshots,homes,f.transactionScheduler(),allocator,ownerRoot,budget)){
                    var authority=owner.owner();token=owner.prepare(authority,UUID.randomUUID(),authority.nextRequestSequence(),List.of(put(72,secondKey(),61,Mutation.ReturnMode.PREVIOUS),put(71,firstKey(),62,Mutation.ReturnMode.PREVIOUS)),List.of(),30000);
                    f.loseCommitReply=true;assertThrows(ExecutionException.class,()->await(owner.execute(token,CONTEXT).observer()));original=captures(f);assertEquals(2,original.size());
                }
            }
            long epochs=f.transactionEpoch();f.reopenTransactionDataPools();
            try(var snapshotRoute=f.transactionHome(RatisNodePlan.Role.SNAPSHOT_HOME);var transactionRoute=f.transactionHome(RatisNodePlan.Role.TRANSACTION_HOME);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
                var bindings=f.transactionBindings();var snapshots=new RatisSnapshotFences(snapshotRoute,snapshotPlacement.column());var homes=new RatisTransactionStatuses(transactionRoute,transactionPlacement.column());
                try(var allocator=f.transactionAllocator(allocatorRoot,bindings,snapshotRoute,transactionRoute);var owner=new RatisAtomicMutationOwner(bindings,snapshots,homes,f.transactionScheduler(),allocator,ownerRoot,budget)){
                    await(owner.execute(token,CONTEXT).observer());var status=owner.status(token);assertEquals(CommandResult.Status.SUCCESS,status.outcome());var saved=CommandResult.decode(owner.resultPage(token,0,1,CommandResult.MAX_BYTES).getFirst());assertArrayEquals(new byte[]{22},saved.value(0));assertArrayEquals(new byte[]{11},saved.value(1));assertEquals(original,captures(f));assertEquals(epochs,f.transactionEpoch());owner.acknowledge(token,status.resultDigest());
                }
            }
            for(var pool:List.of(f.fast,f.archive))assertEquals(0,f.node.nativePool(pool).snapshotAdmissionStats().pins());
        }
    }
    static RatisAtomicMutationOwner singleton(RatisAtomicClientTest.Fixture f,RatisRocksDBAdapter.AllocationSource allocations,Path path,RatisRequestBudget budget)throws Exception {
        return new RatisAtomicMutationOwner(RatisTransactionBindings.of(f.schema,f.directory,f.data),f.snapshots,f.homes,f.scheduler,allocations,path,budget);
    }
    @Test void falseOriginalPredicateIsForcedConflictWithoutChildAndColdRetryKeepsIt()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("false-native"),true);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path path=root.resolve("false-owner");RemoteMutationToken token;int captures;
            try(var actor=singleton(f,allocations,path,budget)){
                var owner=actor.owner();token=actor.prepare(owner,UUID.randomUUID(),0,List.of(put(71,RatisAtomicClientTest.key(2),99,Mutation.ReturnMode.PREVIOUS)),List.of(new AtomicPredicate(71,RatisAtomicClientTest.key(2),value(33))),30000);
                await(actor.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.CONFLICT,actor.status(token).outcome());assertEquals(0,actor.status(token).appliedMutations());assertFalse(java.nio.file.Files.exists(path.resolve("commit")));assertNull(f.get(71,RatisAtomicClientTest.key(2)));captures=f.captureRequests.get();
            }
            try(var cold=singleton(f,allocations,path,budget)){
                await(cold.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.CONFLICT,cold.status(token).outcome());assertEquals(captures,f.captureRequests.get());assertEquals(1,allocations.count.get());cold.acknowledge(token,cold.status(token).resultDigest());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,cold.status(token).state());
            }
        }
    }
    @Test void swappedValidSameGenerationMainChildIsRejectedBeforeHomeOrConsumption()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("swapped-native"),true)){
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path first=root.resolve("owner-first"),second=root.resolve("owner-second");RemoteMutationToken original;
            try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());var actor=singleton(f,allocations,first,budget)){
                var owner=actor.owner();original=actor.prepare(owner,UUID.randomUUID(),0,List.of(put(71,RatisAtomicClientTest.key(2),81,Mutation.ReturnMode.PREVIOUS)),List.of(),30000);
                f.beforeHomeRead=()->{throw new java.io.UncheckedIOException(new java.io.IOException("Lose original HOME lookup"));};assertThrows(ExecutionException.class,()->await(actor.execute(original,CONTEXT).observer()));f.beforeHomeRead=null;
            }
            try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());var actor=singleton(f,allocations,second,budget)){
                var owner=actor.owner();var other=actor.prepare(owner,UUID.randomUUID(),0,List.of(put(71,RatisAtomicClientTest.key(200),82,Mutation.ReturnMode.CURRENT)),List.of(),30000);await(actor.execute(other,CONTEXT).observer());
            }
            java.nio.file.Files.copy(second.resolve("commit"),first.resolve("commit"),java.nio.file.StandardCopyOption.REPLACE_EXISTING);int captures=f.captureRequests.get();var lookups=new java.util.concurrent.atomic.AtomicInteger();f.beforeHomeRead=lookups::incrementAndGet;
            try(var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults());var cold=singleton(f,allocations,first,budget)){
                var failure=assertThrows(ExecutionException.class,()->await(cold.execute(original,CONTEXT).observer()));assertTrue(failure.getCause().getMessage().contains("original public token"));assertEquals(0,lookups.get());assertEquals(captures,f.captureRequests.get());assertEquals(2,allocations.count.get());assertEquals(RemoteMutationStatus.State.PENDING,cold.status(original).state());
            }
        }
    }

    @Test void aggregatePreviousReturnsOverflowIsSavedBeforeAnyChildAndSurvivesColdRetry()throws Exception {
        try(var f=new RatisTwoPoolWorkflowFixture(root.resolve("overflow-native"),false,List.of(),true);
            var first=f.transactionWriter(root.resolve("overflow-first-writer"),f.first.domainId());
            var second=f.transactionWriter(root.resolve("overflow-second-writer"),f.second.domainId())){
            byte[] large=new byte[1100*1024];Arrays.fill(large,(byte)7);
            for(var entry:List.of(Map.entry(first,f.first.column(71).columnId()),Map.entry(second,f.second.column(72).columnId()))){byte[] key=entry.getKey()==first?new byte[]{7}:new byte[]{0,7};var pending=entry.getKey().prepare(List.of(new Mutation(Mutation.Kind.PUT,entry.getValue(),key,large)));assertEquals(CommandResult.Status.SUCCESS,entry.getKey().execute(pending).status());entry.getKey().acknowledge(pending);}
            Path ownerRoot=root.resolve("overflow-owner"),allocatorRoot=root.resolve("overflow-allocator");var snapshotPlacement=f.plan.home(RatisNodePlan.Role.SNAPSHOT_HOME);var transactionPlacement=f.plan.home(RatisNodePlan.Role.TRANSACTION_HOME);
            try(var snapshotRoute=f.transactionHome(RatisNodePlan.Role.SNAPSHOT_HOME);var transactionRoute=f.transactionHome(RatisNodePlan.Role.TRANSACTION_HOME);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
                var bindings=f.transactionBindings();var snapshots=new RatisSnapshotFences(snapshotRoute,snapshotPlacement.column());var homes=new RatisTransactionStatuses(transactionRoute,transactionPlacement.column());
                try(var allocator=f.transactionAllocator(allocatorRoot,bindings,snapshotRoute,transactionRoute)){
                    RemoteMutationToken token;Map<RatisTwoPoolWorkflowFixture.CaptureIdentity,RatisSnapshotCaptureRequest.Result> original;
                    try(var owner=new RatisAtomicMutationOwner(bindings,snapshots,homes,f.transactionScheduler(),allocator,ownerRoot,budget)){
                        var authority=owner.owner();token=owner.prepare(authority,UUID.randomUUID(),0,List.of(put(71,firstKey(),81,Mutation.ReturnMode.PREVIOUS),put(72,secondKey(),82,Mutation.ReturnMode.PREVIOUS)),List.of(),30000);await(owner.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.RESULT_TOO_LARGE,owner.status(token).outcome());assertEquals(0,owner.status(token).appliedMutations());assertFalse(java.nio.file.Files.exists(ownerRoot.resolve("commit")));original=captures(f);
                    }
                    long epoch=f.transactionEpoch();try(var cold=new RatisAtomicMutationOwner(bindings,snapshots,homes,f.transactionScheduler(),allocator,ownerRoot,budget)){
                        await(cold.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.RESULT_TOO_LARGE,cold.status(token).outcome());assertEquals(original,captures(f));assertEquals(epoch,f.transactionEpoch());cold.acknowledge(token,cold.status(token).resultDigest());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,cold.status(token).state());
                    }
                }
            }
            assertArrayEquals(large,f.routes.get(f.first.domainId()).read(f.first.column(71).columnId(),new byte[]{7}));assertArrayEquals(large,f.routes.get(f.second.domainId()).read(f.second.column(72).columnId(),new byte[]{0,7}));for(var pool:List.of(f.fast,f.archive))assertEquals(0,f.node.nativePool(pool).snapshotAdmissionStats().pins());
        }
    }

}
