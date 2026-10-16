package it.cavallium.rockserver.distributed.ratis;

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

class RatisAtomicMutationFaultTest {
    @TempDir(factory=RatisDiskTestDirectory.class)Path root;
    static RemoteMutationToken prepare(RatisAtomicMutationOwner actor,int key)throws Exception {
        var owner=actor.owner();return actor.prepare(owner,UUID.randomUUID(),owner.nextRequestSequence(),List.of(put(71,RatisAtomicClientTest.key(key),81,Mutation.ReturnMode.PREVIOUS)),List.of(),30000);
    }
    @Test void lostStageReplyColdReplaysOnlyTheOriginalCompletePrefixAndCaptures()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("stage-native"),true);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            var base=f.transport();var lose=new AtomicBoolean(true);var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path path=root.resolve("stage-owner");RemoteMutationToken token;int captures;
            try(var data=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor range,UUID column,byte[] key)throws IOException{return base.read(range,column,key);}
                public CommandResult command(RangeDescriptor range,DistributedCommand command)throws IOException{return base.command(range,command);}
                public byte[] participant(RangeDescriptor range,byte[] body)throws IOException{return base.participant(range,body);}
                public CompletableFuture<byte[]> snapshot(RangeDescriptor range,ReplicaDescriptor replica,byte[] body)throws IOException{return base.snapshot(range,replica,body);}
                public byte[] transaction(RangeDescriptor range,byte[] body,boolean fresh)throws IOException{var result=base.transaction(range,body,fresh);if(ScopedTransactionRpc.kind(body,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.STAGE && lose.getAndSet(false))throw new IOException("Lost exact native STAGE reply");return result;}
            })){
                var bindings=RatisTransactionBindings.of(f.schema,f.directory,data);
                try(var actor=new RatisAtomicMutationOwner(bindings,f.snapshots,f.homes,f.scheduler,allocations,path,budget)){
                    token=prepare(actor,2);assertThrows(ExecutionException.class,()->await(actor.execute(token,CONTEXT).observer()));captures=f.captureRequests.get();assertFalse(Files.exists(path.resolve("commit")));assertNull(f.get(71,RatisAtomicClientTest.key(2)));
                }
                try(var cold=new RatisAtomicMutationOwner(bindings,f.snapshots,f.homes,f.scheduler,allocations,path,budget)){
                    await(cold.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.SUCCESS,cold.status(token).outcome());assertEquals(captures,f.captureRequests.get());assertEquals(1,allocations.count.get());assertArrayEquals(new byte[]{81},f.get(71,RatisAtomicClientTest.key(2)));cold.acknowledge(token,cold.status(token).resultDigest());
                }
            }
        }
    }
    @Test void cancellingObserverRetainsActualOwnershipAndCreditsUntilNativeWorkStops()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("cancel-native"),true);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var once=new AtomicBoolean();
            f.beforeSnapshotCapture=()->{if(once.compareAndSet(false,true)){entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Capture barrier timed out");}catch(Exception failure){throw new CompletionException(failure);}}};
            try(var actor=singleton(f,allocations,root.resolve("cancel-owner"),budget)){
                var token=prepare(actor,2);var executing=actor.execute(token,CONTEXT);
                try{assertTrue(entered.await(10,TimeUnit.SECONDS));long charged=budget.chargedBytes();assertTrue(executing.observer().cancel(false));assertFalse(executing.drained().isDone());assertEquals(charged,budget.chargedBytes());assertEquals(1,budget.retainedRequests(RemoteMutationToken.Kind.ATOMIC));assertThrows(IOException.class,actor::close);}finally{release.countDown();}
                await(executing.drained());assertEquals(CommandResult.Status.SUCCESS,actor.status(token).outcome());actor.acknowledge(token,actor.status(token).resultDigest());assertEquals(0,budget.retainedRequests(RemoteMutationToken.Kind.ATOMIC));
            }
        }
    }
    @Test void acknowledgedDeleteFailureCannotReleaseTheNextRequestsFutureJournalPromises()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("debt-native"),true);var budget=spy(new RatisRequestBudget(RemoteMutationOwnerLimits.defaults()))){
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path path=root.resolve("debt-owner");var failDelete=new AtomicBoolean(true);
            doAnswer(invocation->{Path file=invocation.getArgument(0);if(file.equals(path.resolve("snapshot")) && failDelete.getAndSet(false))throw new IOException("Lost cleanup after forced public ACK");return invocation.callRealMethod();}).when(budget).remove(any(Path.class),any(byte[].class),anyInt(),any(SessionJournalFiles.Removal.class));
            try(var actor=singleton(f,allocations,path,budget)){
                var original=prepare(actor,2);await(actor.execute(original,CONTEXT).observer());var digest=actor.status(original).resultDigest();assertThrows(IOException.class,()->actor.acknowledge(original,digest));assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,actor.status(original).state());
                var next=prepare(actor,200);assertEquals(1,next.requestSequence());assertEquals(1,allocations.count.get());
                long actual;try(var files=Files.walk(path)){actual=files.filter(Files::isRegularFile).mapToLong(file->{try{return Files.size(file);}catch(IOException failure){throw new UncheckedIOException(failure);}}).sum();}
                long promised=budget.chargedDiskBytes();assertTrue(promised>actual+65536,"Prepared request must retain its future main+pending promise");
                long competing=budget.limits().maximumDiskBytes()-actual-8192;assertTrue(actual+competing<budget.limits().maximumDiskBytes());assertTrue(promised+competing>budget.limits().maximumDiskBytes());
                assertThrows(RuntimeException.class,()->budget.preflight(List.of(new RatisJournalAccess.Reservation(root.resolve("competing"),Math.toIntExact((competing+1)/2),competing))));assertEquals(RemoteMutationStatus.State.PREPARED,actor.status(next).state());
            }
        }
    }
    @Test void lostAllocationReplyTerminatesOriginalWithoutIssuingAnotherIdentityOrCapture()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("allocation-native"),true);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            var source=new RatisRocksDBAdapterTest.Allocations(f);RatisRocksDBAdapter.AllocationSource lose=(context,targets)->source.allocate(context,targets).thenCompose(issued->CompletableFuture.failedFuture(new IOException("Lost original allocation reply")));
            try(var actor=singleton(f,lose,root.resolve("allocation-owner"),budget)){
                var token=prepare(actor,2);assertThrows(ExecutionException.class,()->await(actor.execute(token,CONTEXT).observer()));await(actor.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.SESSION_EXPIRED,actor.status(token).outcome());assertEquals(0,actor.status(token).appliedMutations());assertEquals(1,source.count.get());assertEquals(0,f.captureRequests.get());assertNull(f.get(71,RatisAtomicClientTest.key(2)));actor.acknowledge(token,actor.status(token).resultDigest());
            }
        }
    }
    @Test void missingJournalAfterLostFirstCaptureRefusesWithoutNewIdentityUntilOriginalFileRestored()throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("missing-native"),true);var budget=new RatisRequestBudget(RemoteMutationOwnerLimits.defaults())){
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);Path path=root.resolve("missing-owner"),snapshot=path.resolve("snapshot"),saved=path.resolve("saved-original-snapshot");RemoteMutationToken token;UUID fence,capture;
            var bindings=RatisTransactionBindings.of(f.schema,f.directory,f.data);
            try(var actor=singleton(f,allocations,path,budget)){
                token=prepare(actor,2);f.loseCaptureGroup=f.groups.getFirst();assertThrows(ExecutionException.class,()->await(actor.execute(token,CONTEXT).observer()));assertEquals(RemoteMutationStatus.State.PENDING,actor.status(token).state());
            }
            try(var journal=RatisDurableSnapshot.reopen(snapshot,f.snapshots,bindings)){
                var original=journal.snapshot();fence=original.initial().fence().fenceId();var participant=original.participants().getFirst();assertNotNull(participant.capture());assertNull(participant.captureToken());capture=participant.capture().request().snapshotId();
            }
            var home=f.snapshots.read(fence);byte[] grants=f.catalogue.epochs.read().encode();Files.move(snapshot,saved);SessionJournalFiles.force(path);int captures=f.captureRequests.get();assertTrue(f.stores.values().stream().anyMatch(store->store.activeSnapshotPins()>0));
            try(var cold=singleton(f,allocations,path,budget)){
                int rpcs=f.rpcs.get(),submissions=f.submissions.get();long debt=budget.chargedDiskBytes();var failure=assertThrows(ExecutionException.class,()->await(cold.execute(token,CONTEXT).observer()));assertTrue(failure.getCause().getMessage().contains("snapshot journal is missing"));assertEquals(captures,f.captureRequests.get());assertEquals(rpcs,f.rpcs.get());assertEquals(submissions,f.submissions.get());assertEquals(1,allocations.count.get());assertEquals(debt,budget.chargedDiskBytes());assertEquals(RemoteMutationStatus.State.PENDING,cold.status(token).state());assertEquals(home,f.snapshots.read(fence));assertArrayEquals(grants,f.catalogue.epochs.read().encode());
                Files.move(saved,snapshot);SessionJournalFiles.force(path);await(cold.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.SUCCESS,cold.status(token).outcome());assertEquals(1,allocations.count.get());
                try(var journal=RatisDurableSnapshot.reopen(snapshot,f.snapshots,bindings)){assertEquals(capture,journal.snapshot().participants().getFirst().capture().request().snapshotId());}
                cold.acknowledge(token,cold.status(token).resultDigest());assertTrue(f.stores.values().stream().allMatch(store->store.activeSnapshotPins()==0));
            }
        }
    }

    @Test void fenceOnlyAndUnknownSnapshotPublicationForcesRecoverColdBeforeAnyRpc()throws Exception {
        for(boolean marker:new boolean[]{false,true}){
            Path path=root.resolve(marker?"marker-owner":"fence-owner");
            try(var f=new RatisAtomicClientTest.Fixture(root.resolve(marker?"marker-native":"fence-native"),true);var budget=spy(new RatisRequestBudget(RemoteMutationOwnerLimits.defaults()))){
                var fail=new AtomicBoolean(true);var allocations=new RatisRocksDBAdapterTest.Allocations(f);RemoteMutationToken token;SnapshotFence frozen;
                doAnswer(invocation->{
                    Path file=invocation.getArgument(0);byte[] body=invocation.getArgument(1);boolean selected=false;
                    if(file.equals(path.resolve("input"))){int tokenBytes=java.nio.ByteBuffer.wrap(body).getInt(4);int phase=body[24+tokenBytes]&255;boolean published=body[25+tokenBytes]!=0;selected=phase==3 && published==marker;}
                    Object result=invocation.callRealMethod();if(selected && fail.getAndSet(false))throw new IOException(marker?"Lost outer snapshot-publication force reply":"Lost fence-only force reply");return result;
                }).when(budget).write(any(Path.class),any(byte[].class),anyInt());
                try(var actor=singleton(f,allocations,path,budget)){
                    token=prepare(actor,2);assertThrows(ExecutionException.class,()->await(actor.execute(token,CONTEXT).observer()));assertFalse(fail.get());assertEquals(0,f.captureRequests.get());assertEquals(0,f.rpcs.get());assertEquals(1,allocations.count.get());assertEquals(marker,Files.exists(path.resolve("snapshot")));
                    Object job=RatisTopologySeedRuntimeTest.field(actor,"job",Object.class);frozen=RatisTopologySeedRuntimeTest.field(job,"fence",SnapshotFence.class);
                }
                assertNull(f.snapshots.read(frozen.fenceId()));
                try(var cold=singleton(f,allocations,path,budget)){
                    await(cold.execute(token,CONTEXT).observer());assertEquals(CommandResult.Status.SUCCESS,cold.status(token).outcome());assertEquals(1,allocations.count.get());
                    var originals=new LinkedHashMap<RatisAtomicClientTest.Fixture.CaptureIdentity,RatisAtomicClientTest.Fixture.CaptureAttempt>();
                    for(var attempt:f.captureAttempts){assertEquals(frozen,attempt.request().fence());var previous=originals.putIfAbsent(attempt.identity(),attempt);if(previous!=null){assertEquals(previous.request(),attempt.request());assertArrayEquals(previous.bytes(),attempt.bytes());}}
                    assertEquals(1,originals.size(),"Cold recovery must use one original capture identity");assertEquals(originals.keySet(),f.captureResults.keySet());assertEquals(2,f.captureRequests.get(),"Coordinator replays CAPTURED once before reporting the same token to HOME");
                    try(var snapshot=RatisDurableSnapshot.reopen(path.resolve("snapshot"),f.snapshots,RatisTransactionBindings.of(f.schema,f.directory,f.data))){assertEquals(frozen,snapshot.snapshot().initial().fence());}
                    cold.acknowledge(token,cold.status(token).resultDigest());assertTrue(f.stores.values().stream().allMatch(store->store.activeSnapshotPins()==0));
                }
            }
        }
    }

}
