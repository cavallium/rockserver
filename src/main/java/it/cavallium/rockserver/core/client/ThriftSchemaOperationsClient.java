package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.RocksDBSchemaOperations;
import it.cavallium.rockserver.core.common.schema.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.TFramedTransport;

/** Bounded borrowed backend/channel observation; never reissues a schema submission. */
public final class ThriftSchemaOperationsClient implements SchemaOperationsAsyncAPI,AutoCloseable {
    public static final int MAX_PENDING=16;
    private final Semaphore pending=new Semaphore(MAX_PENDING);
    private final Executor executor;
    private final RocksDBSchemaOperations.Iface borrowed;
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
    public ThriftSchemaOperationsClient(RocksDBSchemaOperations.Iface service,Executor executor) {
        this.borrowed=Objects.requireNonNull(service); this.executor=Objects.requireNonNull(executor);
        host=null; port=0; maxFrameSize=ThriftTransportLimits.configuredClientLimits().maxFrameSize();
    }
    public ThriftSchemaOperationsClient(String host,int port,Executor executor,int maxFrameSize) {
        this.host=Objects.requireNonNull(host); this.port=port; this.executor=Objects.requireNonNull(executor);
        this.maxFrameSize=Math.min(SchemaOperations.MAX_FRAME_BYTES,ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize)); borrowed=null;
    }
    @FunctionalInterface private interface Call<W> { W call(RocksDBSchemaOperations.Iface client,it.cavallium.rockserver.core.common.api.RequestContext context) throws TException; }
    private <W,T> CompletableFuture<T> invoke(RequestContext context,Call<W> call,Function<W,T> mapper) {
        var ticket=new Ticket<T>();
        if(closed.get()) { ticket.result.completeExceptionally(new IllegalStateException("Schema operation client is closed")); return ticket.result; }
        if(!pending.tryAcquire()) {
            ticket.result.completeExceptionally(new RejectedExecutionException("Schema operation pending capacity exhausted"));
            return ticket.result;
        }
        tickets.add(ticket);
        try {
            var bound=BoundRequestContext.bind(context);
            if(closed.get()) throw new IllegalStateException("Schema operation client is closed");
            if(context.hasTimeout()) ticket.result.orTimeout(bound.remainingNanos(),TimeUnit.NANOSECONDS);
            executor.execute(()->{
                T outcome=null;
                Throwable failure=null;
                try {
                    if(ticket.result.isDone()) return;
                    if(closed.get()) throw new IllegalStateException("Schema operation client is closed");
                    W value;
                    if(borrowed!=null) {
                        synchronized(borrowed) {
                            if(ticket.result.isDone()) return;
                            if(closed.get()) throw new IllegalStateException("Schema operation client is closed");
                            value=call.call(borrowed,wire(bound));
                        }
                    } else {
                        long remaining=bound.remainingNanos(); if(remaining<=0) throw new TimeoutException();
                        // Own one non-reopenable socket from admission through IO completion.
                        // TSocket.open can recreate a socket after close, so connect the owned socket directly.
                        try(var socket=new java.net.Socket()) {
                            ticket.socket=socket;
                            if(ticket.result.isDone()) return;
                            if(closed.get()) throw new IllegalStateException("Schema operation client is closed");
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
                                if(closed.get()) throw new IllegalStateException("Schema operation client is closed");
                                var input=ThriftSchemaOperationsWire.boundedProtocol(new TBinaryProtocol(transport),false);
                                var output=new TMultiplexedProtocol(new TBinaryProtocol(transport),"RocksDBSchemaOperations");
                                value=call.call(new RocksDBSchemaOperations.Client(input,output),wire(bound));
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
        long remaining=bound.remainingNanos(); if(remaining<=0) throw RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Schema operation deadline exceeded");
        return new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.findByValue(bound.value().profile().wireValue()),remaining);
    }
    private static Throwable clientError(Throwable error) {
        if(error instanceof RocksDBThriftException rocks) return RocksDBException.of(RocksDBException.RocksDBErrorType.valueOf(rocks.errorType.name()),rocks.message);
        if(error instanceof TimeoutException) return RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Schema operation deadline exceeded");
        if(error instanceof RejectedExecutionException) return RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,"Schema operation client executor rejected admission");
        return error;
    }
    @Override public CompletableFuture<List<SchemaOperations.Status>> inventory(RequestContext context){return invoke(context,(s,c)->s.inventory(c),ThriftSchemaOperationsWire::inventory);}
    @Override public CompletableFuture<SchemaOperations.Status> status(UUID id,RequestContext context){return invoke(context,(s,c)->s.status(ThriftRemoteMutationWire.uuid(Objects.requireNonNull(id)),c),v->ThriftSchemaOperationsWire.status(v,id));}
    @Override public CompletableFuture<SchemaOperations.Status> resume(UUID id,RequestContext context){return invoke(context,(s,c)->s.resume(ThriftRemoteMutationWire.uuid(Objects.requireNonNull(id)),c),v->ThriftSchemaOperationsWire.status(v,id));}
    @Override public CompletableFuture<SchemaOperations.Status> abort(UUID id,RequestContext context){return invoke(context,(s,c)->s.abort(ThriftRemoteMutationWire.uuid(Objects.requireNonNull(id)),c),v->ThriftSchemaOperationsWire.status(v,id));}
    @Override public void close() {
        if(!closed.compareAndSet(false,true)) return;
        for(var ticket:tickets) {
            ticket.result.completeExceptionally(new IllegalStateException("Schema operation client is closed"));
            var socket=ticket.socket;
            if(socket!=null) try { socket.close(); } catch(java.io.IOException ignored) { }
            var transport=ticket.transport; if(transport!=null) transport.close();
        }
    }
}
