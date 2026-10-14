package it.cavallium.rockserver.core.impl.test;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import it.cavallium.rockserver.core.client.*;
import it.cavallium.rockserver.core.common.ColumnTableProperties;
import it.cavallium.rockserver.core.common.ReplicaPhysicalAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.server.ReplicaPhysicalGrpcService;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ReplicaPhysicalGrpcContractTest {
    static final ReplicaPhysicalAPI.Target TARGET=new ReplicaPhysicalAPI.Target(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),23);
    static final ReplicaPhysicalAPI.Scope SCOPE=new ReplicaPhysicalAPI.Scope(TARGET.pool(),TARGET.column());
    static final ColumnTableProperties TABLES=new ColumnTableProperties(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,Map.of(2L,1L),Map.of(3L,1L),Map.of(1L,1L),Map.of(0L,1L),Map.of(23L,1L),Map.of("filter",1L),Map.of("old-SST-comparator",1L),Map.of("merge",1L),Map.of("prefix",1L),Map.of("collector",1L),Map.of("zstd",1L));
    static SstMaintenance.Metadata metadata(){return new SstMaintenance.Metadata(ReplicaPhysicalWire.session(TARGET),TARGET.nativeColumnId(),"physicalCF",4,1,List.of("/node/pool"),List.of(new SstMaintenance.File("12.sst",1,0,512,"00","ff",false)));}
    static SstMaintenance.Request request(boolean execute){return new SstMaintenance.Request(TARGET.nativeColumnId(),ReplicaPhysicalWire.session(TARGET),List.of("12.sst"),1,0,1024,2048,1,execute);}
    static class Backend implements ReplicaPhysicalAPI {
        int discovers,observes,flushes,compacts,metadataCalls,selective;
        CompletableFuture<Void> active;
        void scope(Scope scope){assertEquals(SCOPE,scope);}
        void target(Target target){if(!TARGET.equals(target))throw new IllegalArgumentException("Stale qualified target");}
        @Override public CompletableFuture<List<Target>> discover(Scope scope){scope(scope);discovers++;return CompletableFuture.completedFuture(List.of(TARGET));}
        @Override public CompletableFuture<List<Observation>> observe(Scope scope){scope(scope);observes++;return CompletableFuture.completedFuture(List.of(new Observation(TARGET,77,"actual-CF-comparator",TABLES)));}
        @Override public CompletableFuture<Void> flush(Scope scope){scope(scope);flushes++;return CompletableFuture.completedFuture(null);}
        @Override public CompletableFuture<Void> compact(Scope scope){scope(scope);compacts++;return active==null?CompletableFuture.completedFuture(null):active;}
        @Override public CompletableFuture<SstMaintenance.Metadata> metadata(Target target,int level){target(target);assertEquals(1,level);metadataCalls++;return CompletableFuture.completedFuture(ReplicaPhysicalGrpcContractTest.metadata());}
        @Override public CompletableFuture<SstMaintenance.Result> compactFiles(Target target,SstMaintenance.Request request){target(target);assertEquals(ReplicaPhysicalGrpcContractTest.request(request.execute()),request);selective++;return CompletableFuture.completedFuture(new SstMaintenance.Result(request.execute(),request.files(),request.execute()?List.of("13.sst"):List.of(),512,request.execute()?400:0,123));}
        int calls(){return discovers+observes+flushes+compacts+metadataCalls+selective;}
    }
    /** Generated future stub/service with full protobuf serialization; no sockets or server lifecycle. */
    static Channel channel(ReplicaPhysicalGrpcService service){return new Channel(){
        @Override public String authority(){return "operator-direct-test";}
        @Override public <Q,R> ClientCall<Q,R> newCall(MethodDescriptor<Q,R> method,CallOptions options){return new ClientCall<>(){
            Listener<R> listener;Q request;final Context.CancellableContext context=Context.current().withCancellation();final java.util.concurrent.atomic.AtomicBoolean closed=new java.util.concurrent.atomic.AtomicBoolean();
            private void close(Status status){if(closed.compareAndSet(false,true))listener.onClose(status,new Metadata());}
            @Override public void start(Listener<R> listener,Metadata headers){this.listener=listener;context.addListener(c->close(c.getDeadline()!=null && c.getDeadline().isExpired()?Status.DEADLINE_EXCEEDED:Status.CANCELLED),Runnable::run);}
            @Override public void request(int n){}
            @Override public void cancel(String message,Throwable cause){context.cancel(cause);close(Status.CANCELLED);}
            @Override public void sendMessage(Q message){request=method.parseRequest(method.streamRequest(message));}
            @Override public void halfClose(){context.run(()->{try{var dispatch=Arrays.stream(service.getClass().getMethods()).filter(m->m.getName().equals(method.getBareMethodName()) && m.getParameterCount()==2).findFirst().orElseThrow();dispatch.invoke(service,request,new StreamObserver<R>(){
                @Override public void onNext(R response){if(!context.isCancelled())listener.onMessage(method.parseResponse(method.streamResponse(response)));}
                @Override public void onError(Throwable error){if(!context.isCancelled())close(Status.fromThrowable(error));}
                @Override public void onCompleted(){if(!context.isCancelled())close(Status.OK);}
            });}catch(Exception error){close(Status.INTERNAL.withCause(error));}});}
        };}
    };}
    @Test void qualifiedWorkflowRetainsAllPropertiesAndSelectiveDryRun(){
        var backend=new Backend();var client=new GrpcReplicaPhysicalClient(channel(new ReplicaPhysicalGrpcService(backend)));assertEquals(List.of(TARGET),client.discover(SCOPE).join());var observed=client.observe(SCOPE).join().getFirst();assertEquals(TARGET,observed.target());assertEquals(77,observed.estimatedKeys());assertEquals("actual-CF-comparator",observed.actualCFComparator());assertEquals(TABLES,observed.tables());assertEquals(29,observed.tables().getClass().getRecordComponents().length);assertEquals(metadata(),client.metadata(TARGET,1).join());
        var dry=client.compactFiles(TARGET,request(false)).join();assertFalse(dry.executed());assertEquals(List.of("12.sst"),dry.inputFiles());assertTrue(dry.outputFiles().isEmpty());assertEquals(0,dry.outputBytes());assertTrue(client.compactFiles(TARGET,request(true)).join().executed());client.flush(SCOPE).join();client.compact(SCOPE).join();assertEquals(7,backend.calls());
    }
    @Test void cancellationAndDeadlineLeaveActualNativeOwnershipIntact()throws Exception{
        var backend=new Backend();backend.active=new CompletableFuture<>();var client=new GrpcReplicaPhysicalClient(channel(new ReplicaPhysicalGrpcService(backend)));var cancelled=client.compact(SCOPE);assertTrue(cancelled.cancel(false));assertFalse(backend.active.isCancelled());backend.active.complete(null);assertTrue(cancelled.isCancelled());
        backend.active=new CompletableFuture<>();var timed=client.withTimeout(Duration.ofMillis(20)).compact(SCOPE);var failure=assertThrows(ExecutionException.class,()->timed.get(5,TimeUnit.SECONDS));assertInstanceOf(RocksDBException.class,failure.getCause());assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,((RocksDBException)failure.getCause()).getErrorUniqueId());assertFalse(backend.active.isDone());backend.active.complete(null);assertEquals(2,backend.compacts);
    }
    @Test void malformedAuthorityContextAndSelectionRejectBeforeBackend(){
        var backend=new Backend();var service=new ReplicaPhysicalGrpcService(backend);var context=ReplicaPhysicalWire.context(Long.MAX_VALUE);var missing=new CompletableFuture<ReplicaPhysicalTargets>();service.discover(ReplicaPhysicalScopeRequest.newBuilder().setScope(ReplicaPhysicalWire.scope(SCOPE)).build(),observer(missing));assertCode(Status.Code.INVALID_ARGUMENT,missing);
        var zero=new CompletableFuture<ReplicaPhysicalTargets>();service.discover(ReplicaPhysicalScopeRequest.newBuilder().setScope(ReplicaPhysicalWire.scope(SCOPE)).setContext(ReplicaPhysicalContext.getDefaultInstance()).build(),observer(zero));assertCode(Status.Code.INVALID_ARGUMENT,zero);
        var malformed=new CompletableFuture<ReplicaPhysicalMetadata>();service.metadata(ReplicaPhysicalMetadataRequest.newBuilder().setContext(context).setLevel(1).setTarget(ReplicaPhysicalWire.target(TARGET).toBuilder().setNode(ByteString.copyFrom(new byte[15]))).build(),observer(malformed));assertCode(Status.Code.INVALID_ARGUMENT,malformed);
        var changedSession=new CompletableFuture<ReplicaPhysicalCompaction>();service.compactFiles(ReplicaPhysicalCompactRequest.newBuilder().setContext(context).setTarget(ReplicaPhysicalWire.target(TARGET)).setRequest(GrpcPortableWire.request(request(false)).toBuilder().setSession("stale")).build(),observer(changedSession));assertCode(Status.Code.INVALID_ARGUMENT,changedSession);
        var traversal=new CompletableFuture<ReplicaPhysicalCompaction>();service.compactFiles(ReplicaPhysicalCompactRequest.newBuilder().setContext(context).setTarget(ReplicaPhysicalWire.target(TARGET)).setRequest(GrpcPortableWire.request(request(false)).toBuilder().setFiles(0,"../12.sst")).build(),observer(traversal));assertCode(Status.Code.INVALID_ARGUMENT,traversal);assertEquals(0,backend.calls());
        var unavailable=new CompletableFuture<ReplicaPhysicalTargets>();new ReplicaPhysicalGrpcService(null).discover(ReplicaPhysicalScopeRequest.newBuilder().setScope(ReplicaPhysicalWire.scope(SCOPE)).setContext(context).build(),observer(unavailable));assertCode(Status.Code.UNIMPLEMENTED,unavailable);
    }
    @Test void crossedTargetsDistributionsAndMetadataBoundsAreRejected(){
        var other=new ReplicaPhysicalAPI.Target(TARGET.node(),UUID.randomUUID(),TARGET.process(),TARGET.pool(),TARGET.session(),TARGET.column(),TARGET.nativeColumnId());assertThrows(IllegalArgumentException.class,()->ReplicaPhysicalWire.metadata(ReplicaPhysicalWire.metadata(metadata(),TARGET,1),other,1));assertThrows(IllegalArgumentException.class,()->ReplicaPhysicalWire.targets(List.of(TARGET,TARGET),SCOPE));assertThrows(IllegalArgumentException.class,()->ReplicaPhysicalWire.targets(List.of(TARGET),new ReplicaPhysicalAPI.Scope(UUID.randomUUID(),null)));
        var observation=ReplicaPhysicalWire.observations(List.of(new ReplicaPhysicalAPI.Observation(TARGET,1,"real",TABLES)),SCOPE);var badMap=observation.toBuilder().setObservations(0,observation.getObservations(0).toBuilder().setTables(GrpcPortableWire.properties(TABLES).toBuilder().putComparators("unexpected",1))).build();assertThrows(IllegalArgumentException.class,()->ReplicaPhysicalWire.observations(badMap,SCOPE));var large=observation.toBuilder().setObservations(0,observation.getObservations(0).toBuilder().setActualCFComparator("x".repeat(ReplicaPhysicalWire.MAX_STRING_BYTES+1))).build();assertThrows(IllegalArgumentException.class,()->ReplicaPhysicalWire.observations(large,SCOPE));
        var wire=ReplicaPhysicalWire.metadata(metadata(),TARGET,1);var badKey=wire.toBuilder().setMetadata(wire.getMetadata().toBuilder().setFiles(0,wire.getMetadata().getFiles(0).toBuilder().setSmallestKeyHex("GG"))).build();assertThrows(IllegalArgumentException.class,()->ReplicaPhysicalWire.metadata(badKey,TARGET,1));var badPath=wire.toBuilder().setMetadata(wire.getMetadata().toBuilder().setFiles(0,wire.getMetadata().getFiles(0).toBuilder().setPathId(1))).build();assertThrows(IllegalArgumentException.class,()->ReplicaPhysicalWire.metadata(badPath,TARGET,1));var badDry=ReplicaPhysicalCompaction.newBuilder().setTarget(ReplicaPhysicalWire.target(TARGET)).setResult(CompactFilesResponse.newBuilder().addInputFiles("12.sst").addOutputFiles("13.sst")).build();assertThrows(IllegalArgumentException.class,()->ReplicaPhysicalWire.result(badDry,TARGET,request(false)));
    }
    @Test void transportDeadlineWinsOverLongerOperatorTimeout()throws Exception{
        var scheduler=Executors.newSingleThreadScheduledExecutor();var backend=new Backend();backend.active=new CompletableFuture<>();var client=new GrpcReplicaPhysicalClient(channel(new ReplicaPhysicalGrpcService(backend))).withTimeout(Duration.ofSeconds(30));var context=Context.current().withDeadlineAfter(200,TimeUnit.MILLISECONDS,scheduler);
        try{var result=context.call(()->client.compact(SCOPE));var failure=assertThrows(ExecutionException.class,()->result.get(5,TimeUnit.SECONDS));assertInstanceOf(RocksDBException.class,failure.getCause());assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,((RocksDBException)failure.getCause()).getErrorUniqueId());assertEquals(1,backend.compacts);assertFalse(backend.active.isDone());backend.active.complete(null);}finally{context.cancel(null);scheduler.shutdownNow();}
    }
    @Test void boundedRequestMarshallerRejectsBeforeParsing(){var method=new ReplicaPhysicalGrpcService(new Backend()).bindBoundedService().getMethods().stream().filter(m->m.getMethodDescriptor().getBareMethodName().equals("discover")).findFirst().orElseThrow();assertThrows(StatusRuntimeException.class,()->method.getMethodDescriptor().getRequestMarshaller().parse(new java.io.ByteArrayInputStream(new byte[ReplicaPhysicalWire.MAX_REQUEST_BYTES+1])));}
    static <T> StreamObserver<T> observer(CompletableFuture<T> result){return new StreamObserver<>(){public void onNext(T value){result.complete(value);}public void onError(Throwable error){result.completeExceptionally(error);}public void onCompleted(){}};}
    static void assertCode(Status.Code expected,CompletableFuture<?> result){var error=assertThrows(CompletionException.class,result::join);assertEquals(expected,Status.fromThrowable(error.getCause()).getCode());}
}
