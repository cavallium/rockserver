package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.cdc.*;
import it.cavallium.rockserver.core.common.cdc.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.function.*;
import org.apache.thrift.*;

/** Borrowed vector CDC backend. Disconnects and deadlines never acknowledge or cancel durable delivery. */
public final class DistributedCdcThriftService implements RocksDBDistributedCdc.Iface {
    public static final String SERVICE_NAME="RocksDBDistributedCdc";
    /** Proven upper bound: typed event/member framing is at most three times canonical projected bytes. */
    public static final int MAX_RESPONSE_BYTES=3*DistributedCdcSubscriptions.MAX_BATCH_BYTES+512;
    private final DistributedCdcAsyncAPI backend;
    private final int maxFrameSize;
    public DistributedCdcThriftService(DistributedCdcAsyncAPI backend,int maxFrameSize) {
        this.backend=backend; this.maxFrameSize=ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize);
    }
    /** The mux already read the method header; preserve its StoredMessageProtocol while bounding generated field reads. */
    public TProcessor boundedProcessor() {
        var delegate=new RocksDBDistributedCdc.Processor<>(this);
        return (input,output)->delegate.process(ThriftDistributedCdcWire.boundedProtocol(input,true),output);
    }
    private <T,R extends TBase<?,?>> R invoke(String method,it.cavallium.rockserver.core.common.api.RequestContext wire,
            Supplier<Function<RequestContext,CompletableFuture<T>>> call,Function<T,R> mapper) throws TException {
        if(backend==null) throw new RocksDBThriftException(RocksDBErrorType.NOT_IMPLEMENTED,"Vector CDC route unavailable");
        try {
            if(wire==null || wire.profile==null) throw new IllegalArgumentException("Request context required");
            var context=new RequestContext(WorkloadProfile.fromWireValue(wire.profile.getValue()),wire.timeoutNanos);
            long start=System.nanoTime();
            var request=call.get();
            long remainingForBackend=context.hasTimeout()?context.timeoutNanos()-(System.nanoTime()-start):RequestContext.NO_TIMEOUT;
            if(remainingForBackend<=0) throw new TimeoutException();
            var source=request.apply(new RequestContext(context.profile(),remainingForBackend));
            T result;
            if(context.hasTimeout()) {
                long remaining=context.timeoutNanos()-(System.nanoTime()-start);
                if(remaining<=0) throw new TimeoutException(); result=source.get(remaining,TimeUnit.NANOSECONDS);
            } else result=source.get();
            var mapped=mapper.apply(result);
            long replyBytes=ThriftDistributedCdcWire.serializedSize(mapped)+12+method.length()+4;
            if(replyBytes>maxFrameSize) throw it.cavallium.rockserver.core.common.RocksDBException.of(
                    it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.CDC_RESPONSE_TOO_LARGE,"Projected CDC response exceeds transport frame limit");
            if(context.hasTimeout() && context.timeoutNanos()-(System.nanoTime()-start)<=0) throw new TimeoutException();
            return mapped;
        } catch(Exception error) {
            while((error instanceof ExecutionException || error instanceof CompletionException) && error.getCause() instanceof Exception cause) error=cause;
            if(error instanceof InterruptedException) Thread.currentThread().interrupt();
            var type=error instanceof it.cavallium.rockserver.core.common.RocksDBException rocks?RocksDBErrorType.valueOf(rocks.getErrorUniqueId().name())
                    :error instanceof RejectedExecutionException?RocksDBErrorType.SERVER_OVERLOADED
                    :error instanceof TimeoutException?RocksDBErrorType.READ_DEADLINE_EXCEEDED
                    :error instanceof IllegalArgumentException || error instanceof NullPointerException?RocksDBErrorType.PUT_INVALID_REQUEST:RocksDBErrorType.INTERNAL_ERROR;
            String message=error.getMessage(); if(message==null) message=error.getClass().getSimpleName();
            throw new RocksDBThriftException(type,message.substring(0,Math.min(message.length(),4096)));
        }
    }
    @Override public CdcContext context(ByteBuffer domain,it.cavallium.rockserver.core.common.api.RequestContext c) throws TException {
        return invoke("context",c,()->{var id=ThriftDistributedCdcWire.uuid(domain); return ctx->backend.context(id,ctx);},v->ThriftDistributedCdcWire.directory(v,ThriftDistributedCdcWire.uuid(domain)));
    }
    @Override public CdcStatus create(ByteBuffer definition,it.cavallium.rockserver.core.common.api.RequestContext c) throws TException {
        return invoke("create",c,()->{var request=ThriftDistributedCdcWire.definition(definition); return ctx->backend.create(request,ctx);},v->{
            var expected=ThriftDistributedCdcWire.definition(definition); ThriftDistributedCdcWire.requireDefinition(v,expected);
            return ThriftDistributedCdcWire.status(v,expected.subscriptionId());
        });
    }
    @Override public CdcStatus recover(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext c) throws TException {
        return invoke("recover",c,()->{var subscription=ThriftDistributedCdcWire.uuid(id); return ctx->backend.recover(subscription,ctx);},v->ThriftDistributedCdcWire.status(v,ThriftDistributedCdcWire.uuid(id)));
    }
    @Override public CdcStatus status(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext c) throws TException {
        return invoke("status",c,()->{var subscription=ThriftDistributedCdcWire.uuid(id); return ctx->backend.status(subscription,ctx);},v->ThriftDistributedCdcWire.status(v,ThriftDistributedCdcWire.uuid(id)));
    }
    @Override public CdcPoll poll(ByteBuffer id,int maxRecords,int maxBytes,it.cavallium.rockserver.core.common.api.RequestContext c) throws TException {
        return invoke("poll",c,()->{ThriftDistributedCdcWire.requirePollLimits(maxRecords,maxBytes); var subscription=ThriftDistributedCdcWire.uuid(id); return ctx->backend.poll(subscription,maxRecords,maxBytes,ctx);},
                v->ThriftDistributedCdcWire.poll(v,ThriftDistributedCdcWire.uuid(id),maxRecords,maxBytes));
    }
    @Override public CdcStatus acknowledge(ByteBuffer id,ByteBuffer batch,it.cavallium.rockserver.core.common.api.RequestContext c) throws TException {
        return invoke("acknowledge",c,()->{var subscription=ThriftDistributedCdcWire.uuid(id); var receipt=ThriftDistributedCdcWire.uuid(batch); return ctx->backend.acknowledge(subscription,receipt,ctx);},v->ThriftDistributedCdcWire.status(v,ThriftDistributedCdcWire.uuid(id)));
    }
    @Override public CdcStatus closeSubscription(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext c) throws TException {
        return invoke("closeSubscription",c,()->{var subscription=ThriftDistributedCdcWire.uuid(id); return ctx->backend.closeSubscription(subscription,ctx);},v->ThriftDistributedCdcWire.status(v,ThriftDistributedCdcWire.uuid(id)));
    }
}
