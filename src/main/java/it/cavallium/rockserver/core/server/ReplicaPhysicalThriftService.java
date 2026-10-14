package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.client.ThriftReplicaPhysicalWire;
import it.cavallium.rockserver.core.common.ReplicaPhysicalAPI;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.api.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import org.apache.thrift.*;

/** Explicit borrowed operator API. Timeout or disconnect never cancels an in-flight native operation. */
public final class ReplicaPhysicalThriftService implements RocksDBReplicaPhysical.Iface {
    public static final String SERVICE_NAME="RocksDBReplicaPhysical";
    private final ReplicaPhysicalAPI backend;
    private final int maxFrameSize;
    public ReplicaPhysicalThriftService(ReplicaPhysicalAPI backend,int maxFrameSize){this.backend=Objects.requireNonNull(backend);this.maxFrameSize=Math.min(ThriftReplicaPhysicalWire.MAX_FRAME_BYTES,ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize));}
    public TProcessor boundedProcessor(){var generated=new RocksDBReplicaPhysical.Processor<>(this);return (input,output)->generated.process(ThriftReplicaPhysicalWire.boundedProtocol(input,true),output);}
    private <T,R> R invoke(String method,ThriftPhysicalContext context,Supplier<Supplier<CompletableFuture<T>>> prepare,Function<T,R> mapper)throws TException{
        try{
            long timeout=ThriftReplicaPhysicalWire.timeout(context),start=System.nanoTime();var call=prepare.get();
            long remaining=timeout==Long.MAX_VALUE?Long.MAX_VALUE:timeout-(System.nanoTime()-start);if(remaining<=0)throw new TimeoutException();
            var source=Objects.requireNonNull(call.get(),"Missing physical backend future");T value;
            if(timeout==Long.MAX_VALUE)value=source.get();else{remaining=timeout-(System.nanoTime()-start);if(remaining<=0)throw new TimeoutException();value=source.get(remaining,TimeUnit.NANOSECONDS);}
            var result=mapper.apply(value);if(ThriftReplicaPhysicalWire.size(result)+12+method.length()+4>maxFrameSize)throw new IllegalArgumentException("Physical response exceeds transport frame bound");
            if(timeout!=Long.MAX_VALUE && timeout-(System.nanoTime()-start)<=0)throw new TimeoutException();return result;
        }catch(Exception failure){while((failure instanceof ExecutionException || failure instanceof CompletionException) && failure.getCause() instanceof Exception cause)failure=cause;if(failure instanceof InterruptedException)Thread.currentThread().interrupt();var type=failure instanceof it.cavallium.rockserver.core.common.RocksDBException rocks?RocksDBErrorType.valueOf(rocks.getErrorUniqueId().name()):failure instanceof TimeoutException?RocksDBErrorType.READ_DEADLINE_EXCEEDED:failure instanceof RejectedExecutionException?RocksDBErrorType.SERVER_OVERLOADED:failure instanceof IllegalArgumentException || failure instanceof NullPointerException || failure instanceof ArithmeticException?RocksDBErrorType.PUT_INVALID_REQUEST:RocksDBErrorType.INTERNAL_ERROR;var message=failure.getMessage()==null?failure.getClass().getSimpleName():failure.getMessage();throw new RocksDBThriftException(type,message.substring(0,Math.min(message.length(),4096)));}
    }
    @Override public List<ThriftPhysicalTarget> discover(ThriftPhysicalScope scope,ThriftPhysicalContext context)throws TException{return invoke("discover",context,()->{var selected=ThriftReplicaPhysicalWire.scope(scope);return ()->backend.discover(selected);},values->ThriftReplicaPhysicalWire.targets(values,ThriftReplicaPhysicalWire.scope(scope)));}
    @Override public List<ThriftPhysicalObservation> observe(ThriftPhysicalScope scope,ThriftPhysicalContext context)throws TException{return invoke("observe",context,()->{var selected=ThriftReplicaPhysicalWire.scope(scope);return ()->backend.observe(selected);},values->ThriftReplicaPhysicalWire.observations(values,ThriftReplicaPhysicalWire.scope(scope)));}
    @Override public void flush(ThriftPhysicalScope scope,ThriftPhysicalContext context)throws TException{invoke("flush",context,()->{var selected=ThriftReplicaPhysicalWire.scope(scope);return ()->backend.flush(selected);},value->null);}
    @Override public void compact(ThriftPhysicalScope scope,ThriftPhysicalContext context)throws TException{invoke("compact",context,()->{var selected=ThriftReplicaPhysicalWire.scope(scope);return ()->backend.compact(selected);},value->null);}
    @Override public ThriftQualifiedPhysicalMetadata metadata(ThriftPhysicalTarget target,int level,ThriftPhysicalContext context)throws TException{return invoke("metadata",context,()->{var qualified=ThriftReplicaPhysicalWire.target(target);if(level< -1)throw new IllegalArgumentException("Invalid SST level");return ()->backend.metadata(qualified,level);},value->ThriftReplicaPhysicalWire.metadata(value,ThriftReplicaPhysicalWire.target(target),level));}
    @Override public ThriftQualifiedPhysicalCompaction compactFiles(ThriftPhysicalTarget target,ThriftPhysicalCompactRequest request,ThriftPhysicalContext context)throws TException{return invoke("compactFiles",context,()->{var qualified=ThriftReplicaPhysicalWire.target(target);var selected=ThriftReplicaPhysicalWire.request(request,qualified);return ()->backend.compactFiles(qualified,selected);},value->ThriftReplicaPhysicalWire.result(value,ThriftReplicaPhysicalWire.target(target),ThriftReplicaPhysicalWire.request(request,ThriftReplicaPhysicalWire.target(target))));}
}
