package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.client.ThriftSchemaOperationsWire;
import it.cavallium.rockserver.core.client.ThriftRemoteMutationWire;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.*;
import it.cavallium.rockserver.core.common.schema.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import org.apache.thrift.*;

/** Lazy borrowed schema owner; a deadline or disconnect ends observation only. */
public final class SchemaOperationsThriftService implements RocksDBSchemaOperations.Iface {
    public static final String SERVICE_NAME="RocksDBSchemaOperations";
    private final Supplier<SchemaOperationsAsyncAPI> backend;private final int maxFrame;
    public SchemaOperationsThriftService(Supplier<SchemaOperationsAsyncAPI> backend,int maxFrame){this.backend=Objects.requireNonNull(backend);this.maxFrame=Math.min(SchemaOperations.MAX_FRAME_BYTES,ThriftTransportLimits.validateServerMaxFrameSize(maxFrame));}
    public TProcessor boundedProcessor(){var processor=new RocksDBSchemaOperations.Processor<>(this);return (input,output)->processor.process(ThriftSchemaOperationsWire.boundedProtocol(input,true),output);}
    private <T,R>R invoke(String method,it.cavallium.rockserver.core.common.api.RequestContext wire,Supplier<BiFunction<SchemaOperationsAsyncAPI,RequestContext,CompletableFuture<T>>> prepare,Function<T,R> mapper)throws TException {
        try {
            if(wire==null || wire.profile==null || !wire.isSetTimeoutNanos())throw new IllegalArgumentException("Schema operation context required");var context=new RequestContext(WorkloadProfile.fromWireValue(wire.profile.getValue()),wire.timeoutNanos);long start=System.nanoTime();
            var call=prepare.get();long remaining=context.hasTimeout()?context.timeoutNanos()-(System.nanoTime()-start):RequestContext.NO_TIMEOUT;if(remaining<=0)throw new TimeoutException();
            var source=call.apply(Objects.requireNonNull(backend.get()),new RequestContext(context.profile(),remaining));T value;if(context.hasTimeout()){remaining=context.timeoutNanos()-(System.nanoTime()-start);if(remaining<=0)throw new TimeoutException();value=source.get(remaining,TimeUnit.NANOSECONDS);}else value=source.get();
            var result=mapper.apply(Objects.requireNonNull(value));long size=12+method.length()+4+size(result);if(size>maxFrame)throw new IllegalArgumentException("Schema operation response exceeds frame bound");if(context.hasTimeout() && context.timeoutNanos()-(System.nanoTime()-start)<=0)throw new TimeoutException();return result;
        }catch(Exception error){while((error instanceof ExecutionException || error instanceof CompletionException) && error.getCause() instanceof Exception cause)error=cause;if(error instanceof InterruptedException)Thread.currentThread().interrupt();var type=error instanceof it.cavallium.rockserver.core.common.RocksDBException rocks?RocksDBErrorType.valueOf(rocks.getErrorUniqueId().name()):error instanceof TimeoutException?RocksDBErrorType.READ_DEADLINE_EXCEEDED:error instanceof RejectedExecutionException?RocksDBErrorType.SERVER_OVERLOADED:error instanceof IllegalArgumentException || error instanceof NullPointerException?RocksDBErrorType.PUT_INVALID_REQUEST:RocksDBErrorType.INTERNAL_ERROR;String message=Objects.toString(error.getMessage(),error.getClass().getSimpleName());throw new RocksDBThriftException(type,message.substring(0,Math.min(4096,message.length())));}
    }
    private static long size(Object value)throws TException{if(value instanceof TBase<?,?> dto)return ThriftRemoteMutationWire.serializedSize(dto);if(value instanceof List<?> list){long result=5;for(var entry:list)result+=size(entry);return result;}throw new IllegalArgumentException("Invalid schema response");}
    @Override public List<ThriftSchemaOperationStatus> inventory(it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{return this.<List<SchemaOperations.Status>,List<ThriftSchemaOperationStatus>>invoke("inventory",context,()->SchemaOperationsAsyncAPI::inventory,v->SchemaOperations.inventory(v).stream().map(ThriftSchemaOperationsWire::status).toList());}
    private ThriftSchemaOperationStatus act(String method,ByteBuffer token,it.cavallium.rockserver.core.common.api.RequestContext context,int action)throws TException{return this.<SchemaOperations.Status,ThriftSchemaOperationStatus>invoke(method,context,()->{var id=ThriftRemoteMutationWire.uuid(token);return (api,c)->switch(action){case 0->api.status(id,c);case 1->api.resume(id,c);case 2->api.abort(id,c);default->throw new AssertionError();};},v->ThriftSchemaOperationsWire.status(SchemaOperations.require(v,ThriftRemoteMutationWire.uuid(token))));}
    @Override public ThriftSchemaOperationStatus status(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{return act("status",id,context,0);}
    @Override public ThriftSchemaOperationStatus resume(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{return act("resume",id,context,1);}
    @Override public ThriftSchemaOperationStatus abort(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{return act("abort",id,context,2);}
}
