package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.RocksDBRemoteMutations;
import java.nio.ByteBuffer;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.api.ThriftPointMutationKind;
import it.cavallium.rockserver.core.common.api.ThriftMutationCallback;
import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.TFramedTransport;

/** Typed remote mutation client. Closing stops cached API admission and owned IO; the executor remains caller-owned. */
public final class ThriftRemoteMutationClient implements RemoteMutationAsyncAPI,AutoCloseable {
    public static final int MAX_PENDING=16;
    private final Semaphore pending=new Semaphore(MAX_PENDING);
    private final Executor executor;
    private final RocksDBRemoteMutations.Iface borrowed;
    private final String host;
    private final int port,maxFrameSize;
    private final AtomicBoolean closed=new AtomicBoolean();
    private final Set<Ticket<?>> tickets=ConcurrentHashMap.newKeySet();
    private static final class Ticket<T> {
        final CompletableFuture<T> result=new CompletableFuture<>() {
            @Override public boolean completeExceptionally(Throwable error) { return super.completeExceptionally(clientError(error)); }
        };
        volatile TTransport transport;
        volatile java.net.Socket socket;
    }
    public ThriftRemoteMutationClient(RocksDBRemoteMutations.Iface service,Executor executor) {
        this.borrowed=Objects.requireNonNull(service); this.executor=Objects.requireNonNull(executor);
        host=null; port=0; maxFrameSize=ThriftTransportLimits.configuredClientLimits().maxFrameSize();
    }
    public ThriftRemoteMutationClient(String host,int port,Executor executor) {
        this(host,port,executor,ThriftTransportLimits.configuredClientLimits().maxFrameSize());
    }
    public ThriftRemoteMutationClient(String host,int port,Executor executor,int maxFrameSize) {
        this.host=Objects.requireNonNull(host); this.port=port; this.executor=Objects.requireNonNull(executor);
        this.maxFrameSize=Math.min(ThriftRemoteMutationWire.MAX_FRAME_BYTES,ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize)); borrowed=null;
    }
    @FunctionalInterface private interface Call<W> { W call(RocksDBRemoteMutations.Iface client,it.cavallium.rockserver.core.common.api.RequestContext context) throws TException; }
    private <W,T> CompletableFuture<T> invoke(RequestContext context,Call<W> call,Function<W,T> mapper) {
        var ticket=new Ticket<T>();
        if(closed.get()) { ticket.result.completeExceptionally(new IllegalStateException("Remote mutation client is closed")); return ticket.result; }
        if(!pending.tryAcquire()) {
            ticket.result.completeExceptionally(new RejectedExecutionException("Remote mutation pending capacity exhausted"));
            return ticket.result;
        }
        tickets.add(ticket);
        try {
            var bound=BoundRequestContext.bind(context);
            if(closed.get()) throw new IllegalStateException("Remote mutation client is closed");
            if(context.hasTimeout()) ticket.result.orTimeout(bound.remainingNanos(),TimeUnit.NANOSECONDS);
            executor.execute(()->{
                T outcome=null;
                Throwable failure=null;
                try {
                    if(ticket.result.isDone()) return;
                    if(closed.get()) throw new IllegalStateException("Remote mutation client is closed");
                    W value;
                    if(borrowed!=null) {
                        synchronized(borrowed) {
                            if(ticket.result.isDone()) return;
                            if(closed.get()) throw new IllegalStateException("Remote mutation client is closed");
                            value=call.call(borrowed,wire(bound));
                        }
                    } else {
                        long remaining=bound.remainingNanos(); if(remaining<=0) throw new TimeoutException();
                        // Own one non-reopenable socket from admission through IO completion.
                        // TSocket.open can recreate a socket after close, so connect the owned socket directly.
                        try(var socket=new java.net.Socket()) {
                            ticket.socket=socket;
                            if(ticket.result.isDone()) return;
                            if(closed.get()) throw new IllegalStateException("Remote mutation client is closed");
                            socket.setSoTimeout(millis(remaining));
                            socket.connect(new java.net.InetSocketAddress(host,port),millis(remaining));
                            remaining=bound.remainingNanos(); if(remaining<=0) throw new TimeoutException();
                            socket.setSoTimeout(millis(remaining));
                            var underlying=new TSocket(socket);
                            underlying.getConfiguration().setMaxFrameSize(maxFrameSize);
                            underlying.getConfiguration().setMaxMessageSize(maxFrameSize);
                            try(var transport=new TFramedTransport(underlying,maxFrameSize)) {
                                ticket.transport=transport;
                                if(ticket.result.isDone()) return;
                                if(closed.get()) throw new IllegalStateException("Remote mutation client is closed");
                                var input=ThriftRemoteMutationWire.boundedProtocol(new TBinaryProtocol(transport),false);
                                var output=new TMultiplexedProtocol(new TBinaryProtocol(transport),"RocksDBRemoteMutations");
                                value=call.call(new RocksDBRemoteMutations.Client(input,output),wire(bound));
                            }
                        }
                    }
                    outcome=mapper.apply(value);
                } catch(Throwable error) { failure=error; }
                finally { ticket.transport=null; ticket.socket=null; retire(ticket); }
                // Completion callbacks may reenter only after actual IO and its admission charge are retired.
                if(failure==null) ticket.result.complete(outcome); else ticket.result.completeExceptionally(failure);
            });
        } catch(Throwable error) { retire(ticket); ticket.result.completeExceptionally(error); }
        return ticket.result;
    }
    private void retire(Ticket<?> ticket) {
        if(tickets.remove(ticket)) pending.release();
    }
    private static int millis(long remaining) { return remaining==RequestContext.NO_TIMEOUT?0:(int)Math.min(Integer.MAX_VALUE,Math.max(1,remaining/1000000+(remaining%1000000==0?0:1))); }
    private static it.cavallium.rockserver.core.common.api.RequestContext wire(BoundRequestContext bound) {
        long remaining=bound.remainingNanos(); if(remaining<=0) throw RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Remote mutation deadline exceeded");
        return new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.findByValue(bound.value().profile().wireValue()),remaining);
    }
    private static Throwable clientError(Throwable error) {
        if(error instanceof RocksDBThriftException rocks) return RocksDBException.of(RocksDBException.RocksDBErrorType.valueOf(rocks.errorType.name()),rocks.message);
        if(error instanceof TimeoutException) return RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Remote mutation deadline exceeded");
        if(error instanceof RejectedExecutionException) return RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,"Remote mutation client executor rejected admission");
        return error;
    }
    @Override public CompletableFuture<List<RemoteMutationOwner>> owners(long column,RequestContext c){return invoke(c,(s,w)->s.owners(column,w),values->{if(values==null || values.size()>ThriftRemoteMutationWire.MAX_OWNERS)throw new IllegalArgumentException("Too many mutation owners");return values.stream().map(ThriftRemoteMutationWire::owner).toList();});}
    @Override public CompletableFuture<RemoteMutationToken> preparePoint(RemoteMutationOwner owner,UUID request,long sequence,long column,List<Keys> keys,List<Buf> values,Mutation.Kind kind,RequestType.RequestTypeId callback,boolean multi,RequestContext c){
        try{var wireOwner=ThriftRemoteMutationWire.owner(owner);ThriftRemoteMutationWire.authority(wireOwner,RemoteMutationToken.Kind.POINT,sequence);var wireKind=ThriftPointMutationKind.valueOf(kind.name());var wireCallback=ThriftMutationCallback.valueOf(callback.name());var entries=ThriftRemoteMutationWire.entries(keys,values,MAX_POINT_ITEMS,MAX_POINT_INPUT_BYTES);boolean present=!values.isEmpty();ThriftRemoteMutationWire.point(entries,present,wireKind,wireCallback,multi);var input=ThriftRemoteMutationWire.entries(entries,present,MAX_POINT_ITEMS,MAX_POINT_INPUT_BYTES);var digest=RemoteMutationInputs.point(column,kind.ordinal(),callback,multi,input.keys(),input.values());var id=ThriftRemoteMutationWire.uuid(request);return invoke(c,(s,w)->s.preparePoint(wireOwner,id,sequence,column,entries,present,wireKind,wireCallback,multi,w),v->ThriftRemoteMutationWire.prepared(v,owner,request,sequence,digest));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}
    }
    @Override public CompletableFuture<RemoteMutationToken> prepareRange(RemoteMutationOwner owner,UUID request,long sequence,long column,Keys lower,Keys upper,RequestContext c){
        try{var wireOwner=ThriftRemoteMutationWire.owner(owner);ThriftRemoteMutationWire.authority(wireOwner,RemoteMutationToken.Kind.RANGE,sequence);var first=ThriftRemoteMutationWire.key(lower);var last=ThriftRemoteMutationWire.key(upper);ThriftRemoteMutationWire.range(first,last);var digest=RemoteMutationInputs.range(column,ThriftRemoteMutationWire.key(first),ThriftRemoteMutationWire.key(last));var id=ThriftRemoteMutationWire.uuid(request);return invoke(c,(s,w)->s.prepareRange(wireOwner,id,sequence,column,first,last,w),v->ThriftRemoteMutationWire.prepared(v,owner,request,sequence,digest));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}
    }
    @Override public CompletableFuture<RemoteMutationToken> prepareBulk(RemoteMutationOwner owner,UUID request,long sequence,long column,boolean merge,RequestContext c){
        try{var wireOwner=ThriftRemoteMutationWire.owner(owner);ThriftRemoteMutationWire.authority(wireOwner,RemoteMutationToken.Kind.BULK,sequence);var id=ThriftRemoteMutationWire.uuid(request);var digest=RemoteMutationInputs.bulkHeader(column,merge);return invoke(c,(s,w)->s.prepareBulk(wireOwner,id,sequence,column,merge,w),v->ThriftRemoteMutationWire.prepared(v,owner,request,sequence,digest));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}
    }
    @Override public CompletableFuture<RemoteMutationStatus> execute(RemoteMutationToken t,RequestContext c){return invoke(c,(s,w)->s.execute(ThriftRemoteMutationWire.token(t),w),v->ThriftRemoteMutationWire.status(v,t));}
    @Override public CompletableFuture<RemoteMutationStatus> resume(RemoteMutationToken t,RequestContext c){return invoke(c,(s,w)->s.resume(ThriftRemoteMutationWire.token(t),w),v->ThriftRemoteMutationWire.status(v,t));}
    @Override public CompletableFuture<RemoteMutationStatus> status(RemoteMutationToken t,RequestContext c){return invoke(c,(s,w)->s.status(ThriftRemoteMutationWire.token(t),w),v->ThriftRemoteMutationWire.status(v,t));}
    @Override public CompletableFuture<RemoteMutationResultPage> results(RemoteMutationToken t,int offset,int items,int bytes,RequestContext c){return invoke(c,(s,w)->{ThriftRemoteMutationWire.budget(offset,items,bytes);return s.results(ThriftRemoteMutationWire.token(t),offset,items,bytes,w);},v->ThriftRemoteMutationWire.page(v,t,offset,items,bytes));}
    @Override public CompletableFuture<RemoteMutationStatus> acknowledge(RemoteMutationToken t,byte[] digest,RequestContext c){try{var frozen=ByteBuffer.wrap(digest.clone());ThriftRemoteMutationWire.digest(frozen);return invoke(c,(s,w)->s.acknowledge(ThriftRemoteMutationWire.token(t),frozen,w),v->ThriftRemoteMutationWire.status(v,t));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}}
    @Override public CompletableFuture<RemoteMutationStatus> appendBatch(RemoteMutationToken t,long ordinal,KVBatch batch,byte[] digest,RequestContext c){try{if(t.kind()!=RemoteMutationToken.Kind.BULK || ordinal<0 || batch.keys().size()!=batch.values().size())throw new IllegalArgumentException("Invalid bulk input");var entries=ThriftRemoteMutationWire.entries(batch.keys(),batch.values(),MAX_BATCH_ITEMS,MAX_BATCH_INPUT_BYTES);var frozen=ByteBuffer.wrap(digest.clone());ThriftRemoteMutationWire.digest(frozen);return invoke(c,(s,w)->s.appendBatch(ThriftRemoteMutationWire.token(t),ordinal,entries,frozen,w),v->ThriftRemoteMutationWire.status(v,t));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}}
    @Override public CompletableFuture<RemoteMutationStatus> finishInput(RemoteMutationToken t,RequestContext c){return invoke(c,(s,w)->{if(t.kind()!=RemoteMutationToken.Kind.BULK)throw new IllegalArgumentException("Not a bulk token");return s.finishInput(ThriftRemoteMutationWire.token(t),w);},v->ThriftRemoteMutationWire.status(v,t));}
    @Override public void close() {
        if(!closed.compareAndSet(false,true)) return;
        for(var ticket:tickets) {
            ticket.result.completeExceptionally(new IllegalStateException("Remote mutation client is closed"));
            var socket=ticket.socket;
            if(socket!=null) try { socket.close(); } catch(java.io.IOException ignored) { }
            var transport=ticket.transport; if(transport!=null) transport.close();
        }
    }
}
