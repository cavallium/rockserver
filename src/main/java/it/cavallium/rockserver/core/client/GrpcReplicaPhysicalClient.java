package it.cavallium.rockserver.core.client;

import com.google.common.util.concurrent.*;
import io.grpc.Channel;
import it.cavallium.rockserver.core.common.ReplicaPhysicalAPI;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.export.ExportWire;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Explicit operator facade over a borrowed channel; deadlines/cancellation only end observation. */
public final class GrpcReplicaPhysicalClient implements ReplicaPhysicalAPI {
    private final ReplicaPhysicalServiceGrpc.ReplicaPhysicalServiceFutureStub stub;
    private final long timeoutNanos;
    public GrpcReplicaPhysicalClient(Channel channel){this(ReplicaPhysicalServiceGrpc.newFutureStub(Objects.requireNonNull(channel)).withMaxInboundMessageSize(ReplicaPhysicalWire.MAX_FRAME_BYTES),Long.MAX_VALUE);}
    private GrpcReplicaPhysicalClient(ReplicaPhysicalServiceGrpc.ReplicaPhysicalServiceFutureStub stub,long timeout){this.stub=stub;this.timeoutNanos=ReplicaPhysicalWire.timeout(ReplicaPhysicalWire.context(timeout));}
    /** Same borrowed channel, explicit reusable timeout; each method binds one monotonic deadline. */
    public GrpcReplicaPhysicalClient withTimeout(Duration timeout){Objects.requireNonNull(timeout);if(timeout.isNegative() || timeout.isZero())throw new IllegalArgumentException("Positive timeout required");long nanos;try{nanos=timeout.toNanos();}catch(ArithmeticException overflow){nanos=Long.MAX_VALUE-1;}return new GrpcReplicaPhysicalClient(stub,Math.min(nanos,Long.MAX_VALUE-1));}
    private <W,T> CompletableFuture<T> invoke(BiFunction<ReplicaPhysicalServiceGrpc.ReplicaPhysicalServiceFutureStub,ReplicaPhysicalContext,ListenableFuture<W>> call,Function<W,T> mapper){
        try{var active=timeoutNanos==Long.MAX_VALUE?stub:stub.withDeadlineAfter(timeoutNanos,TimeUnit.NANOSECONDS);var source=call.apply(active,ReplicaPhysicalWire.context(timeoutNanos));var result=new CompletableFuture<T>();Futures.addCallback(source,new FutureCallback<>(){public void onSuccess(W value){try{result.complete(mapper.apply(value));}catch(Throwable error){result.completeExceptionally(error);}}public void onFailure(Throwable error){result.completeExceptionally(ExportWire.clientError(error));}},MoreExecutors.directExecutor());result.whenComplete((value,error)->{if(result.isCancelled())source.cancel(false);});return result;}catch(Throwable error){return CompletableFuture.failedFuture(ExportWire.clientError(error));}
    }
    private static ReplicaPhysicalScopeRequest scope(Scope scope,ReplicaPhysicalContext context){return ReplicaPhysicalScopeRequest.newBuilder().setScope(ReplicaPhysicalWire.scope(scope)).setContext(context).build();}
    @Override public CompletableFuture<List<Target>> discover(Scope scope){return invoke((s,c)->s.discover(scope(scope,c)),v->ReplicaPhysicalWire.targets(v,scope));}
    @Override public CompletableFuture<List<Observation>> observe(Scope scope){return invoke((s,c)->s.observe(scope(scope,c)),v->ReplicaPhysicalWire.observations(v,scope));}
    @Override public CompletableFuture<Void> flush(Scope scope){return invoke((s,c)->s.flush(scope(scope,c)),v->null);}
    @Override public CompletableFuture<Void> compact(Scope scope){return invoke((s,c)->s.compact(scope(scope,c)),v->null);}
    @Override public CompletableFuture<SstMaintenance.Metadata> metadata(Target target,int level){return invoke((s,c)->{if(level < -1)throw new IllegalArgumentException("Invalid physical metadata level");return s.metadata(ReplicaPhysicalMetadataRequest.newBuilder().setTarget(ReplicaPhysicalWire.target(target)).setLevel(level).setContext(c).build());},v->ReplicaPhysicalWire.metadata(v,target,level));}
    @Override public CompletableFuture<SstMaintenance.Result> compactFiles(Target target,SstMaintenance.Request request){return invoke((s,c)->{ReplicaPhysicalWire.request(target,request);return s.compactFiles(ReplicaPhysicalWire.bounded(ReplicaPhysicalCompactRequest.newBuilder().setTarget(ReplicaPhysicalWire.target(target)).setRequest(GrpcPortableWire.request(request)).setContext(c).build(),ReplicaPhysicalWire.MAX_REQUEST_BYTES));},v->ReplicaPhysicalWire.result(v,target,request));}
}
