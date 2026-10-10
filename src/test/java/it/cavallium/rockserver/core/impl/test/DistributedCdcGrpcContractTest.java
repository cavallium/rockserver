package it.cavallium.rockserver.core.impl.test;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.cdc.proto.*;
import it.cavallium.rockserver.core.common.cdc.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.server.DistributedCdcGrpcService;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Direct codec/service calls only; no gRPC server or sockets. */
class DistributedCdcGrpcContractTest {
    static UUID id(int n) { return new UUID(0,n); }
    final UUID column=id(10),subscription=id(6);
    final DistributedCdcCheckpoint.Lineage lineage=new DistributedCdcCheckpoint.Lineage(id(4),id(5),1);
    final OperationId operation=new OperationId(id(50),1,2);
    DistributedCdcSubscription.Definition definition() { return new DistributedCdcSubscription.Definition(id(1),id(2),id(3),subscription,id(7),new byte[32],List.of(column),new DistributedCdcCheckpoint(Map.of(lineage,DistributedCdcCheckpoint.Cursor.beginning()))); }
    DistributedCdcSubscriptions.Status status() { return new DistributedCdcSubscriptions.Status(DistributedCdcSubscription.initial(definition()),DistributedCdcSubscriptions.Phase.ACTIVE); }
    it.cavallium.rockserver.core.common.api.proto.RequestContext context() { return DistributedCdcWire.context(RequestContext.batch(),RequestContext.NO_TIMEOUT); }
    CdcSubscriptionRequest request() { return CdcSubscriptionRequest.newBuilder().setSubscriptionId(DistributedCdcWire.uuid(subscription)).setContext(context()).build(); }
    DistributedCdcSubscriptions.Poll poll() {
        var position=new ApplyPosition(1,3); var from=new DistributedCdcCheckpoint.Cursor(position,1);
        var mutations=List.of(new Mutation(Mutation.Kind.MERGE,column,new byte[]{1},new byte[]{9}),Mutation.bucketDelete(column,new byte[]{2},List.of(new byte[0],new byte[]{3}),Mutation.ReturnMode.NONE),new Mutation(Mutation.Kind.DELETE_RANGE,column,new byte[]{4},new byte[]{5}));
        var events=new ArrayList<DistributedCdcRecord.Event>(); for(int i=0;i<mutations.size();i++) events.add(new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(position,i+2),mutations.get(i)));
        var envelope=new DistributedCdcSubscriptions.Envelope(position,new byte[32],operation,null,events);
        var batch=new DistributedCdcSubscriptions.Batch(id(8),definition(),lineage,from,DistributedCdcCheckpoint.Cursor.after(position),new ApplyPosition(1,5),List.of(envelope));
        return new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.DATA,batch,0,DistributedCdcCheckpoint.Cursor.beginning(),batch.cut());
    }
    @Test void typedRoundTripPreservesVectorsSparseOrdinalsMutationKindsAndNullPresence() throws Exception {
        var definition=definition(); assertEquals(definition,DistributedCdcWire.definition(CdcDefinition.parseFrom(DistributedCdcWire.definition(definition).toByteArray())));
        var delivered=DistributedCdcSubscription.initial(definition).deliver(definition.start().advance(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,5))));
        var acknowledged=delivered.acknowledge(definition.start().advance(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,3))));
        assertEquals(acknowledged,DistributedCdcWire.state(CdcState.parseFrom(DistributedCdcWire.state(acknowledged).toByteArray())));
        var poll=poll(); var encoded=DistributedCdcWire.poll(poll,subscription,16,4096); var decoded=DistributedCdcWire.poll(CdcPoll.parseFrom(encoded.toByteArray()),subscription,1,111);
        assertEquals(poll,decoded); assertEquals(2,decoded.batch().envelopes().getFirst().events().getFirst().cursor().mutationOrdinal());
        assertFalse(encoded.getBatch().getEnvelopes(0).getEvents(1).getMutation().hasValue()); assertEquals(Mutation.Kind.DELETE_RANGE,decoded.batch().envelopes().getFirst().events().getLast().mutation().kind());
        assertEquals(new UUID(Long.MIN_VALUE,-1),DistributedCdcWire.uuid(DistributedCdcWire.uuid(new UUID(Long.MIN_VALUE,-1))));
        for(var descriptor:Cdc.getDescriptor().getMessageTypes()) for(var field:descriptor.getFields()) assertFalse(field.getName().contains("original_command"));
    }
    @Test void typedFinalizationPreservesDecisionPreparedCutsAndRecoveryIdentity() {
        var participant=new TransactionStatus.Participant(lineage.rangeId(),lineage.groupId(),1,new byte[32],new byte[32],null,false);
        var decision=TransactionStatus.begin(id(1),id(2),id(3),id(60),operation,1,new byte[32],new byte[32],List.of(participant)).prepared(lineage.groupId(),new byte[32],new ApplyPosition(1,1)).decide(TransactionStatus.Phase.COMMIT);
        var finalization=new ParticipantFinalization(id(61),1,lineage.groupId(),new OperationId(id(62),0,0),decision);
        assertEquals(finalization,DistributedCdcWire.finalization(DistributedCdcWire.finalization(finalization)));
        assertTrue(DistributedCdcWire.finalization(finalization).getDecision().getParticipants(0).hasPrepared());
    }
    @Test void gapAndRequiredBudgetRetainLineageEvenWhenOriginalBudgetDiffers() {
        var floor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,4)); var cut=new ApplyPosition(1,5);
        var gap=new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.GAP,null,0,floor,cut);
        assertEquals(gap,DistributedCdcWire.poll(DistributedCdcWire.poll(gap,subscription,16,4096),subscription,1,111));
        var large=new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.TOO_LARGE,null,1000,DistributedCdcCheckpoint.Cursor.beginning(),cut);
        assertEquals(large,DistributedCdcWire.poll(DistributedCdcWire.poll(large,subscription,1,111),subscription,256,8192));
    }
    @Test void directoryCanonicalAuthorityAndTypedContextMustAgree() {
        var peer=new ReplicaDescriptor(id(20),id(21),id(22),"fixture","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(lineage.rangeId(),lineage.groupId(),1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var directory=new RangeDirectory(id(1),id(2),id(3),1,new byte[32],2,Set.of(column),0,List.of(range)); var wire=DistributedCdcWire.directory(directory,id(3));
        assertArrayEquals(RangeDirectoryCodec.encode(directory),RangeDirectoryCodec.encode(DistributedCdcWire.directory(wire,id(3))));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcWire.directory(wire.toBuilder().setGeneration(3).build(),id(3)));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcWire.directory(wire,id(99)));
    }
    @Test void malformedCountsFieldsIdentitiesAndMissingWorkloadNeverReachBackend() {
        var backend=mock(DistributedCdcAsyncAPI.class); var service=new DistributedCdcGrpcService(backend);
        assertStatus(Status.Code.INVALID_ARGUMENT,()->service.status(request().toBuilder().clearContext().build()).block());
        assertStatus(Status.Code.INVALID_ARGUMENT,()->service.status(request().toBuilder().setSubscriptionId(ByteString.copyFrom(new byte[17])).build()).block());
        assertStatus(Status.Code.INVALID_ARGUMENT,()->service.status(request().toBuilder().setContext(context().toBuilder().setProfileValue(1)).build()).block());
        assertStatus(Status.Code.INVALID_ARGUMENT,()->service.poll(CdcPollRequest.newBuilder().setSubscriptionId(DistributedCdcWire.uuid(subscription)).setContext(context()).setMaxRecords(257).setMaxBytes(4096).build()).block());
        var duplicate=DistributedCdcWire.definition(definition()).toBuilder().setStart(CdcCheckpoint.newBuilder().addAllEntries(Collections.nCopies(1025,DistributedCdcWire.checkpoint(definition().start()).getEntries(0)))).build();
        assertStatus(Status.Code.INVALID_ARGUMENT,()->service.create(CdcCreateRequest.newBuilder().setDefinition(duplicate).setContext(context()).build()).block()); verifyNoInteractions(backend);
        var batch=DistributedCdcWire.batch(poll().batch()); assertThrows(IllegalArgumentException.class,()->DistributedCdcWire.batch(batch.toBuilder().addAllEnvelopes(Collections.nCopies(256,batch.getEnvelopes(0))).build(),subscription));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcWire.batch(batch,id(99)));
        var malformed=batch.getEnvelopes(0).getEvents(0).getMutation().toBuilder().setKindValue(999).build(); assertThrows(IllegalArgumentException.class,()->DistributedCdcWire.mutation(malformed));
    }
    @Test void cancelledObservationLeavesBackendFutureAndSubscriptionAlive() {
        var backend=mock(DistributedCdcAsyncAPI.class); var pending=new CompletableFuture<DistributedCdcSubscriptions.Status>();
        when(backend.create(any(),any())).thenReturn(pending); when(backend.status(eq(subscription),any())).thenReturn(CompletableFuture.completedFuture(status()));
        var service=new DistributedCdcGrpcService(backend); var replies=new AtomicInteger();
        var observer=service.create(CdcCreateRequest.newBuilder().setDefinition(DistributedCdcWire.definition(definition())).setContext(context()).build()).subscribe(v->replies.incrementAndGet());
        observer.dispose(); assertFalse(pending.isCancelled()); pending.complete(status()); assertEquals(0,replies.get());
        assertEquals(status(),DistributedCdcWire.status(service.status(request()).block(),subscription)); verify(backend,never()).closeSubscription(any(),any());
    }
    @Test void explicitAckIsOnlyAckAndGrpcDeadlineClampsBackendContext() throws Exception {
        var backend=mock(DistributedCdcAsyncAPI.class); when(backend.poll(eq(subscription),eq(16),eq(4096),any())).thenReturn(CompletableFuture.completedFuture(poll()));
        when(backend.acknowledge(eq(subscription),eq(id(8)),any())).thenReturn(CompletableFuture.completedFuture(status())); var service=new DistributedCdcGrpcService(backend);
        var scheduler=Executors.newSingleThreadScheduledExecutor(); var call=io.grpc.Context.current().withDeadlineAfter(5,TimeUnit.SECONDS,scheduler);
        try { call.run(()->service.poll(CdcPollRequest.newBuilder().setSubscriptionId(DistributedCdcWire.uuid(subscription)).setContext(context()).setMaxRecords(16).setMaxBytes(4096).build()).block()); }
        finally { call.cancel(null); scheduler.shutdownNow(); }
        var captured=org.mockito.ArgumentCaptor.forClass(RequestContext.class); verify(backend).poll(eq(subscription),eq(16),eq(4096),captured.capture()); assertTrue(captured.getValue().timeoutNanos()>0 && captured.getValue().timeoutNanos()<TimeUnit.SECONDS.toNanos(5));
        verify(backend,never()).acknowledge(any(),any(),any()); service.acknowledge(CdcAcknowledgeRequest.newBuilder().setSubscriptionId(DistributedCdcWire.uuid(subscription)).setBatchId(DistributedCdcWire.uuid(id(8))).setContext(context()).build()).block(); verify(backend).acknowledge(eq(subscription),eq(id(8)),any());
    }
    @Test void requestUnknownFieldsAreBoundedBeforeParsingAndAllGeneratedMethodsRemainBound() {
        var backend=mock(DistributedCdcAsyncAPI.class); var implementation=new DistributedCdcGrpcService(backend); var bounded=implementation.bindBoundedService(); var generated=implementation.bindService();
        assertEquals(7,bounded.getMethods().size()); assertEquals(generated.getServiceDescriptor().getName(),bounded.getServiceDescriptor().getName());
        for(var method:generated.getMethods()) { var wrapped=bounded.getMethod(method.getMethodDescriptor().getFullMethodName()).getMethodDescriptor(); assertSame(method.getMethodDescriptor().getSchemaDescriptor(),wrapped.getSchemaDescriptor()); assertSame(method.getMethodDescriptor().getResponseMarshaller(),wrapped.getResponseMarshaller()); }
        var unknown=UnknownFieldSet.newBuilder().addField(100,UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFrom(new byte[DistributedCdcWire.MAX_REQUEST_BYTES])).build()).build();
        var oversized=request().toBuilder().setUnknownFields(unknown).build().toByteArray(); var input=new java.io.ByteArrayInputStream(oversized);
        var method=bounded.getMethod(DistributedCdcServiceGrpc.getStatusMethod().getFullMethodName()); assertStatus(Status.Code.RESOURCE_EXHAUSTED,()->method.getMethodDescriptor().getRequestMarshaller().parse(input));
        assertEquals(oversized.length-DistributedCdcWire.MAX_REQUEST_BYTES-1,input.available()); verifyNoInteractions(backend);
        assertStatus(Status.Code.UNIMPLEMENTED,()->new DistributedCdcGrpcService(null).status(request()).block());
    }
    @Test void exactResponseFrameBudgetIncludesUnknownProtobufBytes() {
        var wire=DistributedCdcWire.poll(poll(),subscription,16,4096); var unknown=UnknownFieldSet.newBuilder().addField(100,UnknownFieldSet.Field.newBuilder().addLengthDelimited(ByteString.copyFrom(new byte[DistributedCdcWire.MAX_FRAME_BYTES])).build()).build();
        assertThrows(IllegalArgumentException.class,()->DistributedCdcWire.poll(wire.toBuilder().setUnknownFields(unknown).build(),subscription,16,4096));
        assertSame(wire,DistributedCdcWire.bounded(wire,wire.getSerializedSize())); assertThrows(IllegalArgumentException.class,()->DistributedCdcWire.bounded(wire,wire.getSerializedSize()-1));
    }
    private static void assertStatus(Status.Code expected,Runnable call) { var error=assertThrows(RuntimeException.class,call::run); assertEquals(expected,Status.fromThrowable(error).getCode()); }
}
