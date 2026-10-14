package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.ReplicaPhysicalAPI;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.RocksDBReplicaPhysical;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.TFramedTransport;

/** Typed physical operator client. Closing stops cached API admission and owned IO; the executor remains caller-owned. */
public final class ThriftReplicaPhysicalClient implements ReplicaPhysicalAPI,AutoCloseable {
    public static final int MAX_PENDING=16;
    private final Semaphore pending=new Semaphore(MAX_PENDING);
    private final Executor executor;
    private final long timeoutNanos;
    private final RocksDBReplicaPhysical.Iface borrowed;
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
    public ThriftReplicaPhysicalClient(RocksDBReplicaPhysical.Iface service,Executor executor,long timeoutNanos) {
        this.timeoutNanos=ThriftReplicaPhysicalWire.context(timeoutNanos).timeoutNanos;
        this.borrowed=Objects.requireNonNull(service); this.executor=Objects.requireNonNull(executor);
        host=null; port=0; maxFrameSize=ThriftTransportLimits.configuredClientLimits().maxFrameSize();
    }
    public ThriftReplicaPhysicalClient(String host,int port,Executor executor,long timeoutNanos) {
        this(host,port,executor,ThriftTransportLimits.configuredClientLimits().maxFrameSize(),timeoutNanos);
    }
    public ThriftReplicaPhysicalClient(String host,int port,Executor executor,int maxFrameSize,long timeoutNanos) {
        this.timeoutNanos=ThriftReplicaPhysicalWire.context(timeoutNanos).timeoutNanos;
        this.host=Objects.requireNonNull(host); this.port=port; this.executor=Objects.requireNonNull(executor);
        this.maxFrameSize=Math.min(ThriftReplicaPhysicalWire.MAX_FRAME_BYTES,ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize)); borrowed=null;
    }
    @FunctionalInterface private interface Call<W> { W call(RocksDBReplicaPhysical.Iface client,it.cavallium.rockserver.core.common.api.ThriftPhysicalContext context) throws TException; }
    private <W,T> CompletableFuture<T> invoke(Call<W> call,Function<W,T> mapper) {
        var ticket=new Ticket<T>();
        if(closed.get()) { ticket.result.completeExceptionally(new IllegalStateException("Physical operator client is closed")); return ticket.result; }
        if(!pending.tryAcquire()) {
            ticket.result.completeExceptionally(new RejectedExecutionException("Physical operator pending capacity exhausted"));
            return ticket.result;
        }
        tickets.add(ticket);
        try {
            var bound=new Deadline(timeoutNanos);
            if(closed.get()) throw new IllegalStateException("Physical operator client is closed");
            if(timeoutNanos!=Long.MAX_VALUE) ticket.result.orTimeout(bound.remainingNanos(),TimeUnit.NANOSECONDS);
            executor.execute(()->{
                T outcome=null;
                Throwable failure=null;
                try {
                    if(ticket.result.isDone()) return;
                    if(closed.get()) throw new IllegalStateException("Physical operator client is closed");
                    W value;
                    if(borrowed!=null) {
                        synchronized(borrowed) {
                            if(ticket.result.isDone()) return;
                            if(closed.get()) throw new IllegalStateException("Physical operator client is closed");
                            value=call.call(borrowed,wire(bound));
                        }
                    } else {
                        long remaining=bound.remainingNanos(); if(remaining<=0) throw new TimeoutException();
                        // Own one non-reopenable socket from admission through IO completion.
                        // TSocket.open can recreate a socket after close, so connect the owned socket directly.
                        try(var socket=new java.net.Socket()) {
                            ticket.socket=socket;
                            if(ticket.result.isDone()) return;
                            if(closed.get()) throw new IllegalStateException("Physical operator client is closed");
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
                                if(closed.get()) throw new IllegalStateException("Physical operator client is closed");
                                var input=ThriftReplicaPhysicalWire.boundedProtocol(new TBinaryProtocol(transport),false);
                                var output=new TMultiplexedProtocol(new TBinaryProtocol(transport),"RocksDBReplicaPhysical");
                                value=call.call(new RocksDBReplicaPhysical.Client(input,output),wire(bound));
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
    private static int millis(long remaining) { return remaining==Long.MAX_VALUE?0:(int)Math.min(Integer.MAX_VALUE,Math.max(1,remaining/1000000+(remaining%1000000==0?0:1))); }
    private static final class Deadline {
        final long started=System.nanoTime(),timeout;
        Deadline(long timeout){this.timeout=timeout;}
        long remainingNanos(){return timeout==Long.MAX_VALUE?Long.MAX_VALUE:Math.max(0,timeout-(System.nanoTime()-started));}
    }
    private static it.cavallium.rockserver.core.common.api.ThriftPhysicalContext wire(Deadline deadline){long remaining=deadline.remainingNanos();if(remaining<=0)throw RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Physical operator deadline exceeded");return ThriftReplicaPhysicalWire.context(remaining);}
    private static Throwable clientError(Throwable error) {
        if(error instanceof RocksDBThriftException rocks) return RocksDBException.of(RocksDBException.RocksDBErrorType.valueOf(rocks.errorType.name()),rocks.message);
        if(error instanceof TimeoutException) return RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Physical operator deadline exceeded");
        if(error instanceof RejectedExecutionException) return RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,"Physical operator client executor rejected admission");
        return error;
    }
    @Override public CompletableFuture<List<ReplicaPhysicalAPI.Target>> discover(ReplicaPhysicalAPI.Scope scope){try{var wire=ThriftReplicaPhysicalWire.scope(scope);return invoke((s,c)->s.discover(wire,c),values->ThriftReplicaPhysicalWire.targetsWire(values,scope));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}}
    @Override public CompletableFuture<List<ReplicaPhysicalAPI.Observation>> observe(ReplicaPhysicalAPI.Scope scope){try{var wire=ThriftReplicaPhysicalWire.scope(scope);return invoke((s,c)->s.observe(wire,c),values->ThriftReplicaPhysicalWire.observationsWire(values,scope));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}}
    @Override public CompletableFuture<Void> flush(ReplicaPhysicalAPI.Scope scope){return invoke((s,c)->{s.flush(ThriftReplicaPhysicalWire.scope(scope),c);return null;},value->null);}
    @Override public CompletableFuture<Void> compact(ReplicaPhysicalAPI.Scope scope){return invoke((s,c)->{s.compact(ThriftReplicaPhysicalWire.scope(scope),c);return null;},value->null);}
    @Override public CompletableFuture<SstMaintenance.Metadata> metadata(ReplicaPhysicalAPI.Target target,int level){return invoke((s,c)->{if(level< -1)throw new IllegalArgumentException("Invalid SST level");return s.metadata(ThriftReplicaPhysicalWire.target(target),level,c);},value->ThriftReplicaPhysicalWire.metadata(value,target,level));}
    @Override public CompletableFuture<SstMaintenance.Result> compactFiles(ReplicaPhysicalAPI.Target target,SstMaintenance.Request request){try{var wire=ThriftReplicaPhysicalWire.request(request,target);return invoke((s,c)->s.compactFiles(ThriftReplicaPhysicalWire.target(target),wire,c),value->ThriftReplicaPhysicalWire.result(value,target,request));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}}
    @Override public void close() {
        if(!closed.compareAndSet(false,true)) return;
        for(var ticket:tickets) {
            ticket.result.completeExceptionally(new IllegalStateException("Physical operator client is closed"));
            var socket=ticket.socket;
            if(socket!=null) try { socket.close(); } catch(java.io.IOException ignored) { }
            var transport=ticket.transport; if(transport!=null) transport.close();
        }
    }
}
