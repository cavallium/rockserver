package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.ThriftDistributedCdcClient;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.api.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.cdc.*;
import it.cavallium.rockserver.core.common.cdc.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.server.DistributedCdcThriftService;
import java.nio.ByteBuffer;
import it.cavallium.rockserver.core.common.api.cdc.CdcBatch;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedCdcThriftContractTest {
    private static UUID id(int value) { return new UUID(0,value); }
    private final UUID column=id(10),subscription=id(6),batchId=id(8);
    private final DistributedCdcCheckpoint.Lineage lineage=new DistributedCdcCheckpoint.Lineage(id(4),id(5),1);
    private final OperationId operation=new OperationId(id(50),0,0);
    private final ApplyPosition cut=new ApplyPosition(1,10);
    private final DistributedCdcCheckpoint.Cursor beginning=DistributedCdcCheckpoint.Cursor.beginning();
    private RangeDirectory directory() {
        return new RangeDirectory(id(1),id(2),id(3),1,new byte[32],0,Set.of(column),0,List.of(new RangeDescriptor(
                lineage.rangeId(),lineage.groupId(),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,
                List.of(new ReplicaDescriptor(id(11),id(12),id(13),"peer","localhost:9000",ReplicaDescriptor.Role.VOTER)))));
    }
    private DistributedCdcSubscription.Definition definition() {
        return DistributedCdcSubscription.Definition.create(subscription,id(7),directory(),List.of(column),new DistributedCdcCheckpoint(Map.of(lineage,beginning)));
    }
    private DistributedCdcSubscriptions.Batch batch(List<Mutation> mutations) {
        var position=new ApplyPosition(1,2);
        var participant=new TransactionStatus.Participant(id(3), lineage.rangeId(), lineage.groupId(), 1, new byte[32], new byte[32], null, false);
        var decision=TransactionStatus.begin(id(1), id(2), id(60), operation, List.of(new DomainBinding(id(3), 0, new byte[32], 1, new byte[32])), new byte[32], List.of(participant))
                .prepared(lineage.groupId(),new byte[32],new ApplyPosition(1,1)).decide(TransactionStatus.Phase.COMMIT);
        var transaction=new ParticipantFinalization(id(61),1,lineage.groupId(),new OperationId(id(62),0,0),decision);
        var events=new ArrayList<DistributedCdcRecord.Event>();
        for(int i=0;i<mutations.size();i++) events.add(new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(position,i*2),mutations.get(i)));
        var envelope=new DistributedCdcSubscriptions.Envelope(position,new byte[32],operation,transaction,events);
        return new DistributedCdcSubscriptions.Batch(batchId,definition(),lineage,beginning,DistributedCdcCheckpoint.Cursor.after(position),cut,List.of(envelope));
    }
    private List<Mutation> mutations() {
        return List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{2},Mutation.ReturnMode.PREVIOUS),
                new Mutation(Mutation.Kind.DELETE,column,new byte[]{2},null),new Mutation(Mutation.Kind.MERGE,column,new byte[]{3},new byte[]{4}),
                new Mutation(Mutation.Kind.DELETE_RANGE,column,new byte[]{4},new byte[]{5}),
                Mutation.bucketPut(column,new byte[]{6},List.of(new byte[]{7}),new byte[]{8},Mutation.ReturnMode.CURRENT),
                Mutation.bucketDelete(column,new byte[]{7},List.of(new byte[0],new byte[]{9}),Mutation.ReturnMode.PREVIOUS),
                Mutation.bucketMerge(column,new byte[]{8},List.of(new byte[]{10}),new byte[]{11},Mutation.ReturnMode.NONE));
    }
    private DistributedCdcSubscriptions.Poll data(DistributedCdcSubscriptions.Batch batch) { return new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.DATA,batch,0,beginning,cut); }
    private static DistributedCdcAsyncAPI backend(java.lang.reflect.InvocationHandler handler) {
        return (DistributedCdcAsyncAPI)java.lang.reflect.Proxy.newProxyInstance(DistributedCdcAsyncAPI.class.getClassLoader(),new Class<?>[]{DistributedCdcAsyncAPI.class},handler);
    }
    private static it.cavallium.rockserver.core.common.api.RequestContext wireContext() { return new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.BATCH,Long.MAX_VALUE); }
    @Test void completeWorkflowPersistsDeliveryOnlyAfterExplicitBatchAck() {
        var delivered=batch(mutations()); var state=new AtomicReference<>(DistributedCdcSubscription.initial(definition())); var calls=new ArrayList<String>();
        var api=backend((p,m,a)->{
            calls.add(m.getName()); assertEquals(RequestContext.batch(),a[a.length-1]);
            return CompletableFuture.completedFuture(switch(m.getName()) {
                case "context" -> directory();
                case "poll" -> { state.updateAndGet(v->v.deliver(v.delivered().advance(lineage,delivered.next()))); yield data(delivered); }
                case "acknowledge" -> { assertEquals(batchId,a[1]); state.updateAndGet(v->v.acknowledge(v.checkpoint().advance(lineage,delivered.next()))); yield new DistributedCdcSubscriptions.Status(state.get(),DistributedCdcSubscriptions.Phase.ACTIVE); }
                case "closeSubscription" -> { state.updateAndGet(DistributedCdcSubscription::close); yield new DistributedCdcSubscriptions.Status(state.get(),DistributedCdcSubscriptions.Phase.CLOSED); }
                default -> new DistributedCdcSubscriptions.Status(state.get(),state.get().phase()==DistributedCdcSubscription.Phase.CLOSED?DistributedCdcSubscriptions.Phase.CLOSED:DistributedCdcSubscriptions.Phase.ACTIVE);
            });
        });
        try(var client=new ThriftDistributedCdcClient(new DistributedCdcThriftService(api,32*1024*1024),Runnable::run)) {
            assertArrayEquals(RangeDirectoryCodec.encode(directory()),RangeDirectoryCodec.encode(client.context(id(3),RequestContext.batch()).join()));
            client.create(definition(),RequestContext.batch()).join();
            client.recover(subscription,RequestContext.batch()).join();
            var poll=client.poll(subscription,1,111,RequestContext.batch()).join();
            assertEquals(delivered,poll.batch());
            assertEquals(beginning,client.status(subscription,RequestContext.batch()).join().state().checkpoint().cursors().get(lineage));
            assertEquals(delivered.next(),client.acknowledge(subscription,batchId,RequestContext.batch()).join().state().checkpoint().cursors().get(lineage));
            assertEquals(DistributedCdcSubscriptions.Phase.CLOSED,client.closeSubscription(subscription,RequestContext.batch()).join().phase());
            assertEquals(DistributedCdcSubscriptions.Phase.CLOSED,client.recover(subscription,RequestContext.batch()).join().phase());
        }
        assertEquals(List.of("context","create","recover","poll","status","acknowledge","closeSubscription","recover"),calls);
    }
    @Test void multiplexedMemoryWirePreservesSelectedEnvelopeBucketAndTransactionMetadata() throws Exception {
        var expected=data(batch(mutations()));
        var service=new DistributedCdcThriftService(backend((p,m,a)->CompletableFuture.completedFuture(expected)),32*1024*1024);
        var processor=new org.apache.thrift.TMultiplexedProcessor(); processor.registerProcessor(DistributedCdcThriftService.SERVICE_NAME,service.boundedProcessor());
        var request=new TMemoryBuffer(256); var response=new TMemoryBuffer(256);
        var responseInput=new org.apache.thrift.transport.TMemoryInputTransport(org.apache.thrift.TConfiguration.custom()
                .setMaxFrameSize(32*1024*1024).setMaxMessageSize(32*1024*1024).build());
        var client=new RocksDBDistributedCdc.Client(ThriftDistributedCdcWire.boundedProtocol(new TBinaryProtocol(responseInput),false),
                new TMultiplexedProtocol(new TBinaryProtocol(request),DistributedCdcThriftService.SERVICE_NAME));
        client.send_poll(ThriftDistributedCdcWire.uuid(subscription),1,111,wireContext());
        processor.process(new TBinaryProtocol(request),new TBinaryProtocol(response));
        // TMemoryBuffer's constructor capacity is its initial known-message quota, even after writes grow it.
        // Decode the completed response with its actual message length, as a framed transport does.
        responseInput.reset(response.getArray(),0,response.length());
        responseInput.updateKnownMessageSize(response.length());
        assertEquals(expected,ThriftDistributedCdcWire.poll(client.recv_poll(),subscription,1,111));
        assertEquals(ThriftDistributedCdcWire.serializedSize(ThriftDistributedCdcWire.poll(expected,subscription,1,111)),new org.apache.thrift.TSerializer().serialize(ThriftDistributedCdcWire.poll(expected,subscription,1,111)).length);
    }
    @Test void gapAndTooLargeKeepExactLineageAndNeverCarryBatch() {
        var floor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,3));
        for(var outcome:List.of(DistributedCdcSubscriptions.Outcome.GAP,DistributedCdcSubscriptions.Outcome.TOO_LARGE)) {
            var expected=new DistributedCdcSubscriptions.Poll(lineage,outcome,null,outcome==DistributedCdcSubscriptions.Outcome.GAP?0:111,floor,cut);
            var wire=ThriftDistributedCdcWire.poll(expected,subscription,1,4096);
            assertEquals(expected,ThriftDistributedCdcWire.poll(wire,subscription,1,4096)); assertNull(wire.batch);
        }
        var wire=ThriftDistributedCdcWire.poll(data(batch(mutations())),subscription,1,111);
        wire.lineage.groupId=ThriftDistributedCdcWire.uuid(id(99));
        assertThrows(IllegalArgumentException.class,()->ThriftDistributedCdcWire.poll(wire,subscription,1,111));
    }
    @Test void readerRejectsBinaryAndCollectionDeclarationsBeforeBodyAllocation() throws Exception {
        var bytes=new TMemoryBuffer(32); var output=new TBinaryProtocol(bytes);
        output.writeFieldBegin(new TField("envelopes",TType.LIST,(short)30)); output.writeListBegin(new TList(TType.STRUCT,257));
        var malformedBatchInput=ThriftDistributedCdcWire.boundedProtocol(new TBinaryProtocol(bytes),false);
        assertThrows(TProtocolException.class,()->new CdcBatch().read(malformedBatchInput));
        bytes=new TMemoryBuffer(32); output=new TBinaryProtocol(bytes);
        output.writeFieldBegin(new TField("id",TType.STRING,(short)1)); output.writeI32(17);
        var input=ThriftDistributedCdcWire.boundedProtocol(new TBinaryProtocol(bytes),true); input.readFieldBegin();
        assertThrows(TProtocolException.class,input::readBinary);
        var truncated=new TMemoryBuffer(32); var truncatedOutput=new TBinaryProtocol(truncated);
        truncatedOutput.writeFieldBegin(new TField("key",TType.STRING,(short)21)); truncatedOutput.writeI32(512*1024);
        var truncatedTransport=new org.apache.thrift.transport.TMemoryInputTransport(truncated.getArray(),0,truncated.length());
        var truncatedInput=ThriftDistributedCdcWire.boundedProtocol(new TBinaryProtocol(truncatedTransport),false); truncatedInput.readFieldBegin();
        assertThrows(org.apache.thrift.transport.TTransportException.class,truncatedInput::readBinary);
        var invalid=ThriftDistributedCdcWire.poll(data(batch(mutations())),subscription,1,111);
        invalid.batch.envelopes.getFirst().events.getFirst().mutation.columnId=ThriftDistributedCdcWire.uuid(id(99));
        assertThrows(IllegalArgumentException.class,()->ThriftDistributedCdcWire.poll(invalid,subscription,1,111));
    }
    @Test void retainedReplySurvivesSmallerRetryAndProjectionMayExceedNativeBudget() throws Exception {
        var mutation=new Mutation(Mutation.Kind.PUT,column,new byte[0],new byte[DistributedCommandCodec.MAX_ENCODED_BYTES-DistributedCommandCodec.HEADER_BYTES-26]);
        var envelopes=new ArrayList<DistributedCdcSubscriptions.Envelope>();
        for(int i=1;i<=5;i++) {
            var position=new ApplyPosition(1,i);
            envelopes.add(new DistributedCdcSubscriptions.Envelope(position,new byte[32],operation,null,List.of(new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(position,0),mutation))));
        }
        var retained=new DistributedCdcSubscriptions.Batch(batchId,definition(),lineage,beginning,DistributedCdcCheckpoint.Cursor.after(cut),cut,envelopes);
        assertTrue(retained.budgetBytes()>DistributedCdcSubscriptions.MAX_POLL_BYTES);
        var expected=data(retained); var encoded=ThriftDistributedCdcWire.poll(expected,subscription,1,111);
        assertEquals(expected,ThriftDistributedCdcWire.poll(encoded,subscription,1,111));
        long total=ThriftDistributedCdcWire.serializedSize(encoded)+12+"poll".length()+4;
        assertTrue(total<=DistributedCdcThriftService.MAX_RESPONSE_BYTES); assertTrue(DistributedCdcThriftService.MAX_RESPONSE_BYTES<32*1024*1024);
        var api=backend((p,m,a)->CompletableFuture.completedFuture(expected));
        var service=new DistributedCdcThriftService(api,(int)total);
        assertNotNull(service.poll(ThriftDistributedCdcWire.uuid(subscription),1,111,wireContext()));
        var small=new DistributedCdcThriftService(api,(int)total-1);
        var error=assertThrows(RocksDBThriftException.class,()->small.poll(ThriftDistributedCdcWire.uuid(subscription),1,111,wireContext()));
        assertEquals(RocksDBErrorType.CDC_RESPONSE_TOO_LARGE,error.errorType);
    }
    @Test void cancellationTimeoutAndCloseNeverAckAndQueuedAdmissionIsStopped() throws Exception {
        var pending=new CompletableFuture<DistributedCdcSubscriptions.Status>(); var started=new CountDownLatch(1); var calls=new AtomicInteger();
        var api=backend((p,m,a)->{assertEquals("status",m.getName()); calls.incrementAndGet(); started.countDown(); return pending;});
        try(var executor=Executors.newSingleThreadExecutor(); var client=new ThriftDistributedCdcClient(new DistributedCdcThriftService(api,32*1024*1024),executor)) {
            var observation=client.status(subscription,RequestContext.batch()); assertTrue(started.await(5,TimeUnit.SECONDS));
            assertTrue(observation.cancel(false)); assertFalse(pending.isCancelled());
            var queued=client.status(subscription,RequestContext.batch()); client.close(); assertTrue(queued.isCompletedExceptionally());
            assertTrue(client.status(subscription,RequestContext.batch()).isCompletedExceptionally());
            pending.complete(new DistributedCdcSubscriptions.Status(DistributedCdcSubscription.initial(definition()),DistributedCdcSubscriptions.Phase.ACTIVE));
        }
        assertEquals(1,calls.get());
        var waiting=new CompletableFuture<DistributedCdcSubscriptions.Status>();
        try(var executor=Executors.newSingleThreadExecutor(); var client=new ThriftDistributedCdcClient(new DistributedCdcThriftService(backend((p,m,a)->waiting),32*1024*1024),executor)) {
            var observation=client.status(subscription,RequestContext.batch(java.time.Duration.ofMillis(30)));
            var error=assertThrows(CompletionException.class,observation::join);
            assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,((RocksDBException)error.getCause()).getErrorUniqueId());
            assertFalse(waiting.isCancelled()); waiting.complete(new DistributedCdcSubscriptions.Status(DistributedCdcSubscription.initial(definition()),DistributedCdcSubscriptions.Phase.ACTIVE));
        }
    }
    @Test void pendingQuotaRetainsCancelledWorkAndCompletionCanReenter() throws Exception {
        var blocked=new CompletableFuture<DistributedCdcSubscriptions.Status>(); var started=new CountDownLatch(1);
        var service=new DistributedCdcThriftService(backend((p,m,a)->{started.countDown(); return blocked;}),32*1024*1024);
        try(var executor=Executors.newSingleThreadExecutor(); var client=new ThriftDistributedCdcClient(service,executor)) {
            var observations=new ArrayList<CompletableFuture<DistributedCdcSubscriptions.Status>>();
            observations.add(client.status(subscription,RequestContext.batch())); assertTrue(started.await(5,TimeUnit.SECONDS));
            for(int i=1;i<ThriftDistributedCdcClient.MAX_PENDING;i++) observations.add(client.status(subscription,RequestContext.batch()));
            var rejected=assertThrows(CompletionException.class,()->client.status(subscription,RequestContext.batch()).join());
            assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,((RocksDBException)rejected.getCause()).getErrorUniqueId());
            observations.getLast().cancel(false);
            assertThrows(CompletionException.class,()->client.status(subscription,RequestContext.batch()).join());
            var followup=observations.getFirst().thenCompose(v->client.status(subscription,RequestContext.batch()));
            var state=new DistributedCdcSubscriptions.Status(DistributedCdcSubscription.initial(definition()),DistributedCdcSubscriptions.Phase.ACTIVE);
            blocked.complete(state);
            assertEquals(state,followup.get(5,TimeUnit.SECONDS));
            for(var observation:observations) if(!observation.isCancelled()) assertEquals(state,observation.get(5,TimeUnit.SECONDS));
        }
    }
    @Test void malformedContextAndPollBudgetFailBeforeBackendAndDisabledServiceFailsExplicitly() throws Exception {
        var calls=new AtomicInteger(); var api=backend((p,m,a)->{calls.incrementAndGet(); throw new RejectedExecutionException();});
        var service=new DistributedCdcThriftService(api,32*1024*1024);
        assertThrows(RocksDBThriftException.class,()->service.status(ThriftDistributedCdcWire.uuid(subscription),null));
        assertThrows(RocksDBThriftException.class,()->service.poll(ThriftDistributedCdcWire.uuid(subscription),257,111,wireContext()));
        assertEquals(0,calls.get());
        assertEquals(RocksDBErrorType.SERVER_OVERLOADED,assertThrows(RocksDBThriftException.class,()->service.status(ThriftDistributedCdcWire.uuid(subscription),wireContext())).errorType);
        assertEquals(RocksDBErrorType.NOT_IMPLEMENTED,assertThrows(RocksDBThriftException.class,()->new DistributedCdcThriftService(null,32*1024*1024).status(ThriftDistributedCdcWire.uuid(subscription),wireContext())).errorType);
    }
    @Test void terminalBarrierRoundtripAndPreallocationGuardKeepPayloadExclusive() throws Exception {
        var freeze=DistributedCdcSubscriptionsTest.freeze(id(1),id(2),id(3),lineage,column);
        var envelope=new DistributedCdcSubscriptions.Envelope(freeze.position(),freeze.header().schemaDigest(),null,null,List.of(),freeze);
        var end=DistributedCdcCheckpoint.Cursor.after(freeze.position());
        var batch=new DistributedCdcSubscriptions.Batch(id(90),definition(),lineage,DistributedCdcCheckpoint.Cursor.beginning(),end,new ApplyPosition(1,3),List.of(envelope));
        var poll=new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.DATA,batch,0,DistributedCdcCheckpoint.Cursor.beginning(),new ApplyPosition(1,3));
        var wire=ThriftDistributedCdcWire.poll(poll,subscription,16,4096);
        assertNull(wire.batch.envelopes.getFirst().operation); assertTrue(wire.batch.envelopes.getFirst().events.isEmpty());
        var bytes=new org.apache.thrift.TSerializer().serialize(wire); var copy=new CdcPoll(); new org.apache.thrift.TDeserializer().deserialize(copy,bytes);
        assertEquals(poll,ThriftDistributedCdcWire.poll(copy,subscription,16,4096));
        var mixed=wire.deepCopy(); mixed.batch.envelopes.getFirst().setOperation(new CdcOperation(java.nio.ByteBuffer.wrap(it.cavallium.rockserver.core.common.cdc.DistributedCdcWire.uuid(operation.sessionId()).toByteArray()),operation.epoch(),operation.sequence()));
        assertThrows(IllegalArgumentException.class,()->ThriftDistributedCdcWire.poll(mixed,subscription,16,4096));
        var malformed=wire.deepCopy(); malformed.batch.envelopes.getFirst().setTerminalBarrier(new byte[0]);
        assertThrows(IllegalArgumentException.class,()->ThriftDistributedCdcWire.poll(malformed,subscription,16,4096));
        var oversized=wire.deepCopy(); oversized.batch.envelopes.getFirst().setTerminalBarrier(new byte[NativeTopologyFreeze.MAX_BYTES+1]);
        assertThrows(IllegalArgumentException.class,()->ThriftDistributedCdcWire.poll(oversized,subscription,16,4096));
        var bounded=ThriftDistributedCdcWire.boundedProtocol(new TBinaryProtocol(new org.apache.thrift.transport.TMemoryInputTransport(new org.apache.thrift.TSerializer().serialize(oversized))),false);
        assertThrows(TProtocolException.class,()->new CdcPoll().read(bounded));
    }

    @Test void successorCurrentStartsRoundtripAndFrameGuardPreserveImmutableDefinition() throws Exception {
        var child=new DistributedCdcCheckpoint.Lineage(id(104),id(105),2); var activation=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(2,4));
        var starts=new DistributedCdcCheckpoint(Map.of(child,activation)); var cut=new ApplyPosition(2,5);
        var batch=new DistributedCdcSubscriptions.Batch(id(8),definition(),child,activation,activation,cut,List.of(),starts);
        var poll=new DistributedCdcSubscriptions.Poll(child,DistributedCdcSubscriptions.Outcome.DATA,batch,0,activation,cut);
        var wire=ThriftDistributedCdcWire.poll(poll,subscription,16,4096);
        var copy=new CdcPoll(); new org.apache.thrift.TDeserializer().deserialize(copy,new org.apache.thrift.TSerializer().serialize(wire));
        assertEquals(poll,ThriftDistributedCdcWire.poll(copy,subscription,16,4096)); assertEquals(definition(),poll.batch().definition());
        var crossed=wire.deepCopy(); crossed.batch.setCurrentStarts(definition().start().encode());
        assertThrows(IllegalArgumentException.class,()->ThriftDistributedCdcWire.poll(crossed,subscription,16,4096));
        var oversized=wire.deepCopy(); oversized.batch.setCurrentStarts(new byte[DistributedCdcCheckpoint.MAX_BYTES+1]);
        var input=ThriftDistributedCdcWire.boundedProtocol(new TBinaryProtocol(new org.apache.thrift.transport.TMemoryInputTransport(new org.apache.thrift.TSerializer().serialize(oversized))),false);
        assertThrows(TProtocolException.class,()->new CdcPoll().read(input));
    }

    @Test void rawFinalizationCodecPreservesPromotedRerouteOrigin() throws Exception {
        var finalization=DistributedCdcGrpcContractTest.reroutedFinalization(TransactionStatus.Phase.COMMIT);
        var position=new ApplyPosition(2,5); var event=new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(position,0),new Mutation(Mutation.Kind.MERGE,column,new byte[]{1},new byte[]{9}));
        var envelope=new DistributedCdcSubscriptions.Envelope(position,new byte[32],finalization.decision().operation(),finalization,List.of(event));
        var batch=new DistributedCdcSubscriptions.Batch(batchId,definition(),lineage,beginning,DistributedCdcCheckpoint.Cursor.after(position),position,List.of(envelope));
        var poll=new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.DATA,batch,0,beginning,position);
        var wire=ThriftDistributedCdcWire.poll(poll,subscription,16,4096); var copy=new CdcPoll();
        new org.apache.thrift.TDeserializer().deserialize(copy,new org.apache.thrift.TSerializer().serialize(wire));
        var decoded=ThriftDistributedCdcWire.poll(copy,subscription,16,4096);
        assertEquals(poll,decoded); assertEquals(finalization.decision().origin(),decoded.batch().envelopes().getFirst().transaction().decision().origin());
        assertEquals(ParticipantFinalization.HEADER_BYTES+TransactionStatus.HEADER_BYTES+OperationRerouteOrigin.MAX_BYTES+TransactionStatus.PARTICIPANT_BYTES,finalization.encode().length);
        var oversized=wire.deepCopy(); oversized.batch.envelopes.getFirst().setFinalization(new byte[ParticipantFinalization.MAX_BYTES+1]);
        var input=ThriftDistributedCdcWire.boundedProtocol(new TBinaryProtocol(new org.apache.thrift.transport.TMemoryInputTransport(new org.apache.thrift.TSerializer().serialize(oversized))),false);
        assertThrows(TProtocolException.class,()->new CdcPoll().read(input));
    }

}
