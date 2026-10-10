package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.cdc.RocksDBDistributedCdc;
import it.cavallium.rockserver.core.common.cdc.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.TFramedTransport;

/** Typed vector CDC client. Closing stops cached API admission and owned IO; the executor remains caller-owned. */
public final class ThriftDistributedCdcClient implements DistributedCdcAsyncAPI,AutoCloseable {
    public static final int MAX_PENDING=16;
    private final Semaphore pending=new Semaphore(MAX_PENDING);
    private final Executor executor;
    private final RocksDBDistributedCdc.Iface borrowed;
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
    public ThriftDistributedCdcClient(RocksDBDistributedCdc.Iface service,Executor executor) {
        this.borrowed=Objects.requireNonNull(service); this.executor=Objects.requireNonNull(executor);
        host=null; port=0; maxFrameSize=ThriftTransportLimits.configuredClientLimits().maxFrameSize();
    }
    public ThriftDistributedCdcClient(String host,int port,Executor executor) {
        this(host,port,executor,ThriftTransportLimits.configuredClientLimits().maxFrameSize());
    }
    public ThriftDistributedCdcClient(String host,int port,Executor executor,int maxFrameSize) {
        this.host=Objects.requireNonNull(host); this.port=port; this.executor=Objects.requireNonNull(executor);
        this.maxFrameSize=ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize); borrowed=null;
    }
    @FunctionalInterface private interface Call<W> { W call(RocksDBDistributedCdc.Iface client,it.cavallium.rockserver.core.common.api.RequestContext context) throws TException; }
    private <W,T> CompletableFuture<T> invoke(RequestContext context,Call<W> call,Function<W,T> mapper) {
        var ticket=new Ticket<T>();
        if(closed.get()) { ticket.result.completeExceptionally(new IllegalStateException("Vector CDC client is closed")); return ticket.result; }
        if(!pending.tryAcquire()) {
            ticket.result.completeExceptionally(new RejectedExecutionException("Vector CDC pending capacity exhausted"));
            return ticket.result;
        }
        tickets.add(ticket);
        try {
            var bound=BoundRequestContext.bind(context);
            if(closed.get()) throw new IllegalStateException("Vector CDC client is closed");
            if(context.hasTimeout()) ticket.result.orTimeout(bound.remainingNanos(),TimeUnit.NANOSECONDS);
            executor.execute(()->{
                T outcome=null;
                Throwable failure=null;
                try {
                    if(ticket.result.isDone()) return;
                    if(closed.get()) throw new IllegalStateException("Vector CDC client is closed");
                    W value;
                    if(borrowed!=null) {
                        synchronized(borrowed) {
                            if(ticket.result.isDone()) return;
                            if(closed.get()) throw new IllegalStateException("Vector CDC client is closed");
                            value=call.call(borrowed,wire(bound));
                        }
                    } else {
                        long remaining=bound.remainingNanos(); if(remaining<=0) throw new TimeoutException();
                        // Own one non-reopenable socket from admission through IO completion.
                        // TSocket.open can recreate a socket after close, so connect the owned socket directly.
                        try(var socket=new java.net.Socket()) {
                            ticket.socket=socket;
                            if(ticket.result.isDone()) return;
                            if(closed.get()) throw new IllegalStateException("Vector CDC client is closed");
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
                                if(closed.get()) throw new IllegalStateException("Vector CDC client is closed");
                                var input=ThriftDistributedCdcWire.boundedProtocol(new TBinaryProtocol(transport),false);
                                var output=new TMultiplexedProtocol(new TBinaryProtocol(transport),"RocksDBDistributedCdc");
                                value=call.call(new RocksDBDistributedCdc.Client(input,output),wire(bound));
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
        long remaining=bound.remainingNanos(); if(remaining<=0) throw RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Vector CDC deadline exceeded");
        return new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.findByValue(bound.value().profile().wireValue()),remaining);
    }
    private static Throwable clientError(Throwable error) {
        if(error instanceof RocksDBThriftException rocks) return RocksDBException.of(RocksDBException.RocksDBErrorType.valueOf(rocks.errorType.name()),rocks.message);
        if(error instanceof TimeoutException) return RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Vector CDC deadline exceeded");
        if(error instanceof RejectedExecutionException) return RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,"Vector CDC client executor rejected admission");
        return error;
    }
    @Override public CompletableFuture<RangeDirectory> context(UUID domain,RequestContext c) {
        return invoke(c,(s,w)->s.context(ThriftDistributedCdcWire.uuid(domain),w),v->ThriftDistributedCdcWire.directory(v,domain));
    }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> create(DistributedCdcSubscription.Definition definition,RequestContext c) {
        return invoke(c,(s,w)->s.create(ThriftDistributedCdcWire.definition(definition),w),v->{
            var result=ThriftDistributedCdcWire.status(v,definition.subscriptionId()); ThriftDistributedCdcWire.requireDefinition(result,definition); return result;
        });
    }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> recover(UUID id,RequestContext c) { return invoke(c,(s,w)->s.recover(ThriftDistributedCdcWire.uuid(id),w),v->ThriftDistributedCdcWire.status(v,id)); }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> status(UUID id,RequestContext c) { return invoke(c,(s,w)->s.status(ThriftDistributedCdcWire.uuid(id),w),v->ThriftDistributedCdcWire.status(v,id)); }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Poll> poll(UUID id,int maxRecords,int maxBytes,RequestContext c) {
        return invoke(c,(s,w)->{ThriftDistributedCdcWire.requirePollLimits(maxRecords,maxBytes); return s.poll(ThriftDistributedCdcWire.uuid(id),maxRecords,maxBytes,w);},v->ThriftDistributedCdcWire.poll(v,id,maxRecords,maxBytes));
    }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> acknowledge(UUID id,UUID batch,RequestContext c) { return invoke(c,(s,w)->s.acknowledge(ThriftDistributedCdcWire.uuid(id),ThriftDistributedCdcWire.uuid(batch),w),v->ThriftDistributedCdcWire.status(v,id)); }
    @Override public CompletableFuture<DistributedCdcSubscriptions.Status> closeSubscription(UUID id,RequestContext c) { return invoke(c,(s,w)->s.closeSubscription(ThriftDistributedCdcWire.uuid(id),w),v->ThriftDistributedCdcWire.status(v,id)); }
    @Override public void close() {
        if(!closed.compareAndSet(false,true)) return;
        for(var ticket:tickets) {
            ticket.result.completeExceptionally(new IllegalStateException("Vector CDC client is closed"));
            var socket=ticket.socket;
            if(socket!=null) try { socket.close(); } catch(java.io.IOException ignored) { }
            var transport=ticket.transport; if(transport!=null) transport.close();
        }
    }
}
