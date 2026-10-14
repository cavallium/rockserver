package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.client.ThriftRemoteMutationWire;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import org.apache.thrift.*;

/** Borrowed durable mutation backend. Disconnect or timeout stops observation, never acknowledges work. */
public final class RemoteMutationThriftService implements RocksDBRemoteMutations.Iface {
    public static final String SERVICE_NAME="RocksDBRemoteMutations";
    private final RemoteMutationAsyncAPI backend;
    private final int maxFrameSize;
    public RemoteMutationThriftService(RemoteMutationAsyncAPI backend,int maxFrameSize){this.backend=backend;this.maxFrameSize=Math.min(ThriftRemoteMutationWire.MAX_FRAME_BYTES,ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize));}
    public TProcessor boundedProcessor(){var delegate=new RocksDBRemoteMutations.Processor<>(this);return (input,output)->delegate.process(ThriftRemoteMutationWire.boundedProtocol(input,true),output);}
    private <T,R> R invoke(String method,it.cavallium.rockserver.core.common.api.RequestContext wire,Supplier<Function<RequestContext,CompletableFuture<T>>> call,Function<T,R> mapper)throws TException{
        try{
            if(wire==null || wire.profile==null || !wire.isSetTimeoutNanos())throw new IllegalArgumentException("Request context required");
            var context=new RequestContext(WorkloadProfile.fromWireValue(wire.profile.getValue()),wire.timeoutNanos);long start=System.nanoTime();
            var admitted=call.get();if(backend==null)throw it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,"Explicit remote mutations require a distributed backend");
            long remaining=context.hasTimeout()?context.timeoutNanos()-(System.nanoTime()-start):RequestContext.NO_TIMEOUT;if(remaining<=0)throw new TimeoutException();
            var source=admitted.apply(new RequestContext(context.profile(),remaining));
            T result;if(context.hasTimeout()){remaining=context.timeoutNanos()-(System.nanoTime()-start);if(remaining<=0)throw new TimeoutException();result=source.get(remaining,TimeUnit.NANOSECONDS);}else result=source.get();
            var response=mapper.apply(Objects.requireNonNull(result,"Empty mutation backend reply"));long bytes=12+method.length()+4+size(response);
            if(bytes>maxFrameSize)throw new IllegalArgumentException("Mutation response exceeds transport frame bound");
            if(context.hasTimeout() && context.timeoutNanos()-(System.nanoTime()-start)<=0)throw new TimeoutException();return response;
        }catch(Exception error){while((error instanceof ExecutionException || error instanceof CompletionException) && error.getCause() instanceof Exception cause)error=cause;if(error instanceof InterruptedException)Thread.currentThread().interrupt();var type=error instanceof it.cavallium.rockserver.core.common.RocksDBException rocks?RocksDBErrorType.valueOf(rocks.getErrorUniqueId().name()):error instanceof TimeoutException?RocksDBErrorType.READ_DEADLINE_EXCEEDED:error instanceof RejectedExecutionException?RocksDBErrorType.SERVER_OVERLOADED:error instanceof IllegalArgumentException || error instanceof NullPointerException?RocksDBErrorType.PUT_INVALID_REQUEST:RocksDBErrorType.INTERNAL_ERROR;var message=error.getMessage()==null?error.getClass().getSimpleName():error.getMessage();throw new RocksDBThriftException(type,message.substring(0,Math.min(message.length(),4096)));}
    }
    private static long size(Object value)throws TException{if(value instanceof TBase<?,?> dto)return ThriftRemoteMutationWire.serializedSize(dto);if(value instanceof ByteBuffer bytes)return 4L+bytes.remaining();if(value instanceof List<?> list){long result=5;for(var entry:list)result+=size(entry);return result;}throw new IllegalArgumentException("Unknown mutation response");}
    private static ThriftMutationObservation observation(RemoteMutationStatus value,RemoteMutationToken token){var wire=ThriftRemoteMutationWire.status(value);ThriftRemoteMutationWire.status(wire,token);return wire;}
    @Override public List<ThriftMutationOwner> owners(long column,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{return invoke("owners",context,()->c->backend.owners(column,c),values->{if(values.size()>ThriftRemoteMutationWire.MAX_OWNERS)throw new IllegalArgumentException("Too many mutation owners");return values.stream().map(ThriftRemoteMutationWire::owner).toList();});}
    @Override public ByteBuffer preparePoint(ThriftMutationOwner owner,ByteBuffer id,long sequence,long column,List<ThriftMutationEntry> entries,boolean present,ThriftPointMutationKind kind,ThriftMutationCallback callback,boolean multi,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{
        return invoke("preparePoint",context,()->{var authority=ThriftRemoteMutationWire.authority(owner,RemoteMutationToken.Kind.POINT,sequence);var request=ThriftRemoteMutationWire.uuid(id);ThriftRemoteMutationWire.point(entries,present,kind,callback,multi);var input=ThriftRemoteMutationWire.entries(entries,present,RemoteMutationAsyncAPI.MAX_POINT_ITEMS,RemoteMutationAsyncAPI.MAX_POINT_INPUT_BYTES);return c->backend.preparePoint(authority,request,sequence,column,input.keys(),input.values(),Mutation.Kind.valueOf(kind.name()),RequestType.RequestTypeId.valueOf(callback.name()),multi,c);},value->{var input=ThriftRemoteMutationWire.entries(entries,present,RemoteMutationAsyncAPI.MAX_POINT_ITEMS,RemoteMutationAsyncAPI.MAX_POINT_INPUT_BYTES);var result=ThriftRemoteMutationWire.token(value);ThriftRemoteMutationWire.prepared(result,ThriftRemoteMutationWire.owner(owner),ThriftRemoteMutationWire.uuid(id),sequence,RemoteMutationInputs.point(column,Mutation.Kind.valueOf(kind.name()).ordinal(),RequestType.RequestTypeId.valueOf(callback.name()),multi,input.keys(),input.values()));return result;});
    }
    @Override public ByteBuffer prepareRange(ThriftMutationOwner owner,ByteBuffer id,long sequence,long column,List<ByteBuffer> lower,List<ByteBuffer> upper,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{
        return invoke("prepareRange",context,()->{var authority=ThriftRemoteMutationWire.authority(owner,RemoteMutationToken.Kind.RANGE,sequence);var request=ThriftRemoteMutationWire.uuid(id);ThriftRemoteMutationWire.range(lower,upper);var first=ThriftRemoteMutationWire.key(lower);var last=ThriftRemoteMutationWire.key(upper);return c->backend.prepareRange(authority,request,sequence,column,first,last,c);},value->{var result=ThriftRemoteMutationWire.token(value);ThriftRemoteMutationWire.prepared(result,ThriftRemoteMutationWire.owner(owner),ThriftRemoteMutationWire.uuid(id),sequence,RemoteMutationInputs.range(column,ThriftRemoteMutationWire.key(lower),ThriftRemoteMutationWire.key(upper)));return result;});
    }
    @Override public ByteBuffer prepareBulk(ThriftMutationOwner owner,ByteBuffer id,long sequence,long column,boolean merge,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{
        return invoke("prepareBulk",context,()->{var authority=ThriftRemoteMutationWire.authority(owner,RemoteMutationToken.Kind.BULK,sequence);var request=ThriftRemoteMutationWire.uuid(id);return c->backend.prepareBulk(authority,request,sequence,column,merge,c);},value->{var result=ThriftRemoteMutationWire.token(value);ThriftRemoteMutationWire.prepared(result,ThriftRemoteMutationWire.owner(owner),ThriftRemoteMutationWire.uuid(id),sequence,RemoteMutationInputs.bulkHeader(column,merge));return result;});
    }
    private ThriftMutationObservation tokenCall(String method,ByteBuffer wire,it.cavallium.rockserver.core.common.api.RequestContext context,BiFunction<RemoteMutationToken,RequestContext,CompletableFuture<RemoteMutationStatus>> call)throws TException{return invoke(method,context,()->{var token=ThriftRemoteMutationWire.token(wire);if(method.equals("finishInput") && token.kind()!=RemoteMutationToken.Kind.BULK)throw new IllegalArgumentException("Not a bulk token");return c->call.apply(token,c);},value->observation(value,ThriftRemoteMutationWire.token(wire)));}
    @Override public ThriftMutationObservation execute(ByteBuffer token,it.cavallium.rockserver.core.common.api.RequestContext c)throws TException{return tokenCall("execute",token,c,(t,w)->backend.execute(t,w));}
    @Override public ThriftMutationObservation resume(ByteBuffer token,it.cavallium.rockserver.core.common.api.RequestContext c)throws TException{return tokenCall("resume",token,c,(t,w)->backend.resume(t,w));}
    @Override public ThriftMutationObservation status(ByteBuffer token,it.cavallium.rockserver.core.common.api.RequestContext c)throws TException{return tokenCall("status",token,c,(t,w)->backend.status(t,w));}
    @Override public ThriftMutationObservation finishInput(ByteBuffer token,it.cavallium.rockserver.core.common.api.RequestContext c)throws TException{return tokenCall("finishInput",token,c,(t,w)->backend.finishInput(t,w));}
    @Override public ThriftMutationPage results(ByteBuffer wire,int offset,int items,int bytes,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{return invoke("results",context,()->{ThriftRemoteMutationWire.budget(offset,items,bytes);var token=ThriftRemoteMutationWire.token(wire);return c->backend.results(token,offset,items,bytes,c);},value->ThriftRemoteMutationWire.page(value,ThriftRemoteMutationWire.token(wire),offset,items,bytes));}
    @Override public ThriftMutationObservation acknowledge(ByteBuffer wire,ByteBuffer digest,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{return invoke("acknowledge",context,()->{var token=ThriftRemoteMutationWire.token(wire);var expected=ThriftRemoteMutationWire.digest(digest);return c->backend.acknowledge(token,expected,c);},value->observation(value,ThriftRemoteMutationWire.token(wire)));}
    @Override public ThriftMutationObservation appendBatch(ByteBuffer wire,long ordinal,List<ThriftMutationEntry> entries,ByteBuffer digest,it.cavallium.rockserver.core.common.api.RequestContext context)throws TException{return invoke("appendBatch",context,()->{var token=ThriftRemoteMutationWire.token(wire);if(token.kind()!=RemoteMutationToken.Kind.BULK || ordinal<0)throw new IllegalArgumentException("Invalid bulk token/ordinal");var expected=ThriftRemoteMutationWire.digest(digest);var input=ThriftRemoteMutationWire.entries(entries,true,RemoteMutationAsyncAPI.MAX_BATCH_ITEMS,RemoteMutationAsyncAPI.MAX_BATCH_INPUT_BYTES);return c->backend.appendBatch(token,ordinal,new KVBatch.KVBatchRef(input.keys(),input.values()),expected,c);},value->observation(value,ThriftRemoteMutationWire.token(wire)));}
}
