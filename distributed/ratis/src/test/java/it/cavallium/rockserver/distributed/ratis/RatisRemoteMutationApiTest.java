package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class RatisRemoteMutationApiTest {
    final RatisRocksDBConnection connection=mock(RatisRocksDBConnection.class);
    final RWScheduler scheduler=mock(RWScheduler.class);
    final RatisRemoteMutationApi api=new RatisRemoteMutationApi(connection,scheduler);
    final RemoteMutationToken token=new RemoteMutationToken(new UUID(1,1),new UUID(2,2),new UUID(3,3),new UUID(4,4),new UUID(5,5),1,0,RemoteMutationToken.Kind.POINT,new byte[32],new byte[32],new byte[32]);
    RemoteMutationStatus terminal(CommandResult.Status outcome,int count){return new RemoteMutationStatus(token,RemoteMutationStatus.State.TERMINAL,1,0,count,outcome,new byte[32]);}
    void unlimited(){when(scheduler.resolveMonotonicDeadline(any())).thenReturn(Long.MAX_VALUE);}
    @Test void cancellationForwardsToCurrentObserverWithoutStartingStatusOrAcknowledging() {
        unlimited();var actual=new CompletableFuture<Object>();when(connection.executeRemoteMutation(eq(token),any())).thenReturn(actual);var observer=api.execute(token,RequestContext.batch());assertTrue(observer.cancel(false));assertTrue(actual.isCancelled());verify(connection,never()).remoteMutationStatus(any(),any());verify(connection,never()).acknowledgeRemoteMutation(any(),any(),any());
    }
    @Test void executionAndStatusShareOneDeadlineAndTerminalFailureRemainsObservable() throws Exception {
        var context=RequestContext.batch(Duration.ofSeconds(1));when(scheduler.resolveMonotonicDeadline(context)).thenReturn(100L);when(scheduler.remainingMonotonicDeadlineNanos(100L)).thenReturn(70L,25L);var firstContext=new AtomicReference<RequestContext>();var secondContext=new AtomicReference<RequestContext>();var status=terminal(CommandResult.Status.SESSION_EXPIRED,1);
        when(connection.executeRemoteMutation(eq(token),any())).thenAnswer(call -> {firstContext.set(call.getArgument(1));return CompletableFuture.failedFuture(new IllegalStateException("Retained terminal failure"));});when(connection.remoteMutationStatus(eq(token),any())).thenAnswer(call -> {secondContext.set(call.getArgument(1));return CompletableFuture.completedFuture(status);});
        assertSame(status,api.execute(token,context).get());assertEquals(70,firstContext.get().timeoutNanos());assertEquals(25,secondContext.get().timeoutNanos());verify(scheduler,times(1)).resolveMonotonicDeadline(context);verify(connection,never()).acknowledgeRemoteMutation(any(),any(),any());
    }
    @Test void exhaustedCompositeDeadlineDoesNotStartASecondRequest() {
        var context=RequestContext.batch(Duration.ofSeconds(1));when(scheduler.resolveMonotonicDeadline(context)).thenReturn(100L);when(scheduler.remainingMonotonicDeadlineNanos(100L)).thenReturn(70L,0L);when(connection.executeRemoteMutation(eq(token),any())).thenReturn(CompletableFuture.completedFuture(null));assertThrows(CompletionException.class,() -> api.execute(token,context).join());verify(connection,never()).remoteMutationStatus(any(),any());
    }
    @Test void failedAckDoesNotBecomeASuccessfulTerminalStatus() {
        unlimited();when(connection.acknowledgeRemoteMutation(eq(token),any(),any())).thenReturn(CompletableFuture.failedFuture(new IllegalArgumentException("Wrong result digest")));when(connection.remoteMutationStatus(eq(token),any())).thenReturn(CompletableFuture.completedFuture(terminal(CommandResult.Status.SUCCESS,1)));assertThrows(CompletionException.class,() -> api.acknowledge(token,new byte[32],RequestContext.batch()).join());
    }
    @Test void resultPageCarriesStableCountDigestCursorAndCopiesEncodedRows() throws Exception {
        unlimited();var status=terminal(CommandResult.Status.SUCCESS,2);byte[] row=new CommandResult(CommandResult.Status.SUCCESS,Collections.singletonList(new byte[]{9})).encode();when(connection.remoteMutationStatus(eq(token),any())).thenReturn(CompletableFuture.completedFuture(status));when(connection.remoteMutationResults(eq(token),eq(0),eq(1),eq(1024),any())).thenReturn(CompletableFuture.completedFuture(List.of(row)));
        var page=api.results(token,0,1,1024,RequestContext.batch()).get();assertSame(status,page.status());assertEquals(0,page.offset());assertEquals(1,page.nextOffset());assertTrue(page.hasMore());var saved=page.rows().getFirst();row[0]^=1;assertArrayEquals(saved,page.rows().getFirst());var exposed=page.rows().getFirst();exposed[0]^=1;assertArrayEquals(saved,page.rows().getFirst());
    }
    @Test void prepareEntersConnectionAdmissionSynchronouslyWithBorrowedLogicalInput() throws Exception {
        var keys=List.of(new Keys(Buf.wrap(new byte[]{2})));var values=List.of(Buf.wrap(new byte[]{9}));var expected=new RemoteMutationOwner(token.clusterId(),token.databaseId(),token.domainId(),token.ownerId(),token.ownerEpoch(),0,token.kind(),token.schemaDigest(),token.directoryDigest());
        when(connection.preparePointMutation(eq(expected),eq(token.requestId()),eq(0L),eq(71L),same(keys),same(values),eq(RatisRocksDBDirect.WriteKind.PUT),eq(RequestType.RequestTypeId.NOTHING),eq(false),any())).thenReturn(CompletableFuture.completedFuture(token));
        assertSame(token,api.preparePoint(expected,token.requestId(),0,71,keys,values,Mutation.Kind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get());
    }
}
