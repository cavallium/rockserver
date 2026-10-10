package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import static it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.*;

/** Bounded legacy iterator state. Every source remains on one owned or borrowed original cut. */
final class RatisRocksDBIterators {
    static final int MAX_ITERATORS=128,MAX_WAITERS=16,MAX_ADVANCE=4096;
    static final long MAX_BYTES=64L*1024*1024,WORK_BYTES=2L*RangeBudget.DEFAULT_MAX_BYTES;
    interface Resource {
        CompletableFuture<RangePage<KV>> page(Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget,RequestContext context);
        CompletableFuture<Void> release(RequestContext context);
        void closeLocal() throws IOException;
    }
    record Stats(int iterators,int streams,long bytes) {}
    private final DistributedSchema schema;
    private final java.util.function.LongSupplier handleIds;
    private final Map<Long,Entry> entries=new LinkedHashMap<>();
    private final Set<Flow> flows=new HashSet<>();
    private long bytes;
    private boolean closing;
    private static final class Deadline {
        final RequestContext context; final long opened=System.nanoTime();
        Deadline(RequestContext context) { this.context=context; }
        RequestContext remaining() {
            if (context.timeoutNanos()==RequestContext.NO_TIMEOUT) return context;
            long left=context.timeoutNanos()-(System.nanoTime()-opened);
            if (left<=0) throw RocksDBException.of(READ_DEADLINE_EXCEEDED,"Iterator request deadline expired");
            return new RequestContext(context.profile(),left);
        }
    }
    private static final class Task {
        final Object key; final CompletableFuture<Object> result=new CompletableFuture<>(); int views;
        Task(Object key) { this.key=key; }
        synchronized CompletableFuture<Object> view() {
            if (!result.isDone() && views>=MAX_WAITERS) return CompletableFuture.failedFuture(RocksDBException.of(SERVER_OVERLOADED,"Iterator observer capacity exhausted"));
            if (!result.isDone()) views++; return result.thenApply(value -> {
                if (value instanceof List<?> list) { var copy=new ArrayList<Buf>(); for (var item:list) copy.add(Buf.wrap(((Buf)item).toByteArray())); return Collections.unmodifiableList(copy); }
                return value;
            });
        }
    }
    private static final class Entry {
        final long id,transaction,column,opened=System.nanoTime(),ttl,bytes; final Keys lower,upper; final boolean reverse;
        Resource resource; boolean acquiring=true,closed,exhausted,cleaning,disposed; Keys resume,seek; Task active;
        CompletableFuture<Void> drained=new CompletableFuture<>();
        Entry(long id,long transaction,long column,Keys lower,Keys upper,boolean reverse,long ttl,long bytes) { this.id=id; this.transaction=transaction; this.column=column; this.lower=lower; this.upper=upper; this.reverse=reverse; this.ttl=ttl; this.bytes=bytes; }
    }
    private record Advance(long skip,long take,RequestType.RequestTypeId type) {}
    private record Progress(Keys resume,boolean exhausted,List<KV> taken) {}
    RatisRocksDBIterators(DistributedSchema schema) { this(schema,new java.util.concurrent.atomic.AtomicLong(ThreadLocalRandom.current().nextLong(1,Long.MAX_VALUE/2))::incrementAndGet); }
    RatisRocksDBIterators(DistributedSchema schema,java.util.function.LongSupplier handleIds) { this.schema=Objects.requireNonNull(schema); this.handleIds=Objects.requireNonNull(handleIds); }
    synchronized boolean owns(long id) { return entries.containsKey(id); }
    synchronized Stats stats() { return new Stats(entries.size(),flows.size(),bytes); }
    private long metadata(Keys lower,Keys upper) {
        long size=65536; for (var keys:Arrays.asList(lower,upper)) if (keys!=null) for (var part:keys.keys()) size+=2L*part.size()+8;
        if (size>TransactionScope.MAX_BOUND_BYTES*4L+65536) throw new IllegalArgumentException("Iterator bounds exceed byte budget"); return size;
    }
    private synchronized void reserve(long charge) {
        if (closing || entries.size()+flows.size()>=MAX_ITERATORS || charge>MAX_BYTES-bytes) throw RocksDBException.of(SERVER_OVERLOADED,"Iterator entry/byte capacity exhausted"); bytes+=charge;
    }
    CompletableFuture<Long> open(long transaction,long column,Keys lower,Keys upper,boolean reverse,Duration ttl,Supplier<CompletableFuture<Resource>> source,RequestContext context) {
        try {
            expire(); Objects.requireNonNull(ttl); if (ttl.isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofSeconds(60))>0) throw new IllegalArgumentException("Iterator TTL must be positive and at most60seconds");
            Keys low=copy(lower),high=copy(upper); long charge=metadata(low,high); final Entry entry;
            synchronized (this) { long id=handleIds.getAsLong(); if (id<=0 || entries.containsKey(id)) throw new IllegalStateException("Shared handle allocator returned an invalid or reused ID"); reserve(charge); entry=new Entry(id,transaction,column,low,high,reverse,ttl.toNanos(),charge); entries.put(id,entry); }
            var view=new CompletableFuture<Long>(); var deadline=new Deadline(context);
            CompletableFuture<Resource> actual;
            try { actual=source.get(); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            actual.whenComplete((resource,failure) -> {
                synchronized (entry) { entry.resource=resource; entry.acquiring=false; if (view.isCancelled() || failure!=null) entry.closed=true; }
                if (failure!=null) view.completeExceptionally(unwrap(failure));
                else try { deadline.remaining(); if (expired(entry) || entry.closed) { entry.closed=true; view.completeExceptionally(RocksDBException.of(TX_NOT_FOUND,"Iterator expired before publication")); } else if (!view.complete(entry.id)) entry.closed=true; } catch (Throwable expired) { entry.closed=true; view.completeExceptionally(expired); }
                if (entry.closed) cleanup(entry);
            });
            return view;
        } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
    }
    private void expire() {
        List<Entry> owners; synchronized (this) { owners=List.copyOf(entries.values()); }
        for (var entry:owners) { boolean retired; synchronized (entry) { retired=expired(entry); if (retired) entry.closed=true; } if (retired) cleanup(entry); }
    }
    long transaction(long id) { return entry(id).transaction; }
    private Entry entry(long id) {
        final Entry entry; synchronized (this) { entry=entries.get(id); }
        if (entry==null) throw RocksDBException.of(TX_NOT_FOUND,"Unknown iterator");
        boolean retire;
        synchronized (entry) { retire=expired(entry); if (retire) entry.closed=true; if (!retire && (entry.closed || entry.acquiring)) throw RocksDBException.of(TX_NOT_FOUND,"Iterator closed or unexposed"); }
        if (retire) { cleanup(entry); throw RocksDBException.of(TX_NOT_FOUND,"Iterator expired"); }
        return entry;
    }
    private boolean expired(Entry entry) { return System.nanoTime()-entry.opened>=entry.ttl; }
    CompletableFuture<Void> seek(long id,Keys keys,RequestContext context) {
        try {
            Entry entry=entry(id);
            var target=copy(Objects.requireNonNull(keys)); if (target.keys().length!=0) schema.encodeLogicalKey(schema.column(entry.column).columnId(),target);
            synchronized (this) { synchronized (entry) {
                if (entries.get(id)!=entry || entry.closed || entry.disposed || expired(entry)) throw RocksDBException.of(TX_NOT_FOUND,"Iterator closed or expired before seek");
                if (entry.active!=null) throw RocksDBException.of(SERVER_OVERLOADED,"Iterator work active"); new Deadline(context).remaining(); entry.seek=target.keys().length==0 ? null : target; entry.resume=null; entry.exhausted=false;
            } }
            return CompletableFuture.completedFuture(null);
        } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
    }
    CompletableFuture<Object> subsequent(long id,long skip,long take,RequestType.RequestTypeId type,RequestContext context) {
        try {
            if (skip<0 || take<0 || skip>MAX_ADVANCE || take>MAX_ADVANCE || skip+take>MAX_ADVANCE) throw RocksDBException.of(PUT_INVALID_REQUEST,"Iterator advance exceeds bounded quantum");
            if (type!=RequestType.RequestTypeId.NOTHING && type!=RequestType.RequestTypeId.EXISTS && type!=RequestType.RequestTypeId.MULTI) throw RocksDBException.of(NOT_IMPLEMENTED,"Unsupported iterator callback");
            Entry entry=entry(id);
            var request=new Advance(skip,take,type); final Task task; final Keys resume,seek; final boolean exhausted;
            synchronized (this) { synchronized (entry) {
                if (entries.get(id)!=entry || entry.closed || entry.disposed || expired(entry)) throw RocksDBException.of(TX_NOT_FOUND,"Iterator closed or expired before admission");
                if (entry.active!=null) return entry.active.key.equals(request) ? entry.active.view() : CompletableFuture.failedFuture(RocksDBException.of(SERVER_OVERLOADED,"Iterator operation active"));
                if (WORK_BYTES>MAX_BYTES-bytes) throw RocksDBException.of(SERVER_OVERLOADED,"Iterator page byte capacity exhausted"); bytes+=WORK_BYTES;
                entry.active=task=new Task(request); resume=copy(entry.resume); seek=copy(entry.seek); exhausted=entry.exhausted;
            } }
            var deadline=new Deadline(context); CompletableFuture<Progress> actual;
            try { actual=advance(entry,resume,seek,exhausted,(int)skip,(int)take,deadline); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            actual.whenComplete((progress,failure) -> {
                synchronized (entry) {
                    if (failure==null) try {
                        deadline.remaining(); if (entry.closed || expired(entry)) throw RocksDBException.of(TX_NOT_FOUND,"Iterator closed or expired during work");
                        entry.resume=copy(progress.resume()); entry.exhausted=progress.exhausted(); entry.seek=null;
                    } catch (Throwable late) { failure=late; }
                    entry.active=null;
                }
                synchronized (this) { bytes-=WORK_BYTES; }
                if (failure==null) {
                    Object result=type==RequestType.RequestTypeId.NOTHING ? null : type==RequestType.RequestTypeId.EXISTS ? !progress.taken().isEmpty() : progress.taken().stream().map(row -> row.value()==null ? Buf.wrap(new byte[0]) : row.value()).toList(); task.result.complete(result);
                } else task.result.completeExceptionally(unwrap(failure));
                if (entry.closed) cleanup(entry);
            });
            return task.view();
        } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
    }
    private int compare(long column,Keys a,Keys b) {
        var id=schema.column(column).columnId(); byte[] x=RatisLogicalRangePages.bound(schema,id,a),y=RatisLogicalRangePages.bound(schema,id,b); int order=Arrays.compareUnsigned(x,y);
        if (order!=0 || a.keys().length!=schema.column(column).schema().keysCount() || b.keys().length!=a.keys().length) return order;
        var left=schema.encodeLogicalKey(id,a).variableKeys(); var right=schema.encodeLogicalKey(id,b).variableKeys();
        for (int n=0;n<left.size();n++) { order=Arrays.compareUnsigned(left.get(n),right.get(n)); if (order!=0) return order; } return 0;
    }
    private CompletableFuture<Progress> advance(Entry entry,Keys resume,Keys seek,boolean exhausted,int skip,int take,Deadline deadline) {
        if (exhausted || skip+take==0) return CompletableFuture.completedFuture(new Progress(resume,exhausted,List.of()));
        if (seek==null) return collect(entry,resume,skip,take,new ArrayList<>(),0,deadline);
        if (entry.upper!=null && compare(entry.column,seek,entry.upper)>=0) return entry.reverse ? collect(entry,null,skip,take,new ArrayList<>(),0,deadline) : CompletableFuture.completedFuture(new Progress(null,true,List.of()));
        if (entry.lower!=null && compare(entry.column,seek,entry.lower)<0) return entry.reverse ? CompletableFuture.completedFuture(new Progress(null,true,List.of())) : collect(entry,null,skip,take,new ArrayList<>(),0,deadline);
        if (!entry.reverse) return entry.resource.page(seek,entry.upper,false,null,new RangeBudget(skip+take,RangeBudget.DEFAULT_MAX_BYTES),deadline.remaining()).thenCompose(page -> consume(entry,page,skip,take,new ArrayList<>(),0,deadline));
        return entry.resource.page(seek,entry.upper,false,null,new RangeBudget(1,RangeBudget.DEFAULT_MAX_BYTES),deadline.remaining()).thenCompose(page -> {
            var rows=new ArrayList<KV>(); int remainingSkip=skip,remainingTake=take;
            if (!page.items().isEmpty() && compare(entry.column,page.items().getFirst().keys(),seek)==0) {
                if (remainingSkip>0) remainingSkip--; else if (remainingTake>0) { rows.add(page.items().getFirst()); remainingTake--; }
            }
            return collect(entry,seek,remainingSkip,remainingTake,rows,rowBytes(rows),deadline);
        });
    }
    private CompletableFuture<Progress> collect(Entry entry,Keys resume,int skip,int take,List<KV> rows,long bytes,Deadline deadline) {
        if (skip+take==0) return CompletableFuture.completedFuture(new Progress(resume,false,List.copyOf(rows)));
        return entry.resource.page(entry.lower,entry.upper,entry.reverse,resume,new RangeBudget(skip+take,RangeBudget.DEFAULT_MAX_BYTES),deadline.remaining()).thenCompose(page -> consume(entry,page,skip,take,rows,bytes,deadline));
    }
    private CompletableFuture<Progress> consume(Entry entry,RangePage<KV> page,int skip,int take,List<KV> rows,long bytes,Deadline deadline) {
        deadline.remaining(); Keys cursor=page.resumeAfter();
        for (var row:page.items()) { if (skip>0) skip--; else if (take>0) { long next=rowBytes(List.of(row)); if (next>RangeBudget.DEFAULT_MAX_BYTES-bytes) throw RocksDBException.of(RANGE_ITEM_TOO_LARGE,"Iterator result exceeds byte quantum"); bytes+=next; rows.add(row); take--; } }
        if (!page.hasMore() || skip+take==0) return CompletableFuture.completedFuture(new Progress(cursor,!page.hasMore(),List.copyOf(rows)));
        return collect(entry,cursor,skip,take,rows,bytes,deadline);
    }
    private long rowBytes(List<KV> rows) { long bytes=0; for (var row:rows) { for (var key:row.keys().keys()) bytes+=key.size()+4L; bytes+=row.value()==null ? 0 : row.value().size(); } return bytes; }
    CompletableFuture<Void> close(long id,RequestContext context) {
        Entry entry; synchronized (this) { entry=entries.get(id); }
        if (entry==null) return CompletableFuture.completedFuture(null);
        synchronized (entry) { entry.closed=true; }
        return cleanup(entry);
    }
    private CompletableFuture<Void> cleanup(Entry entry) {
        final CompletableFuture<Void> proof;
        synchronized (entry) {
            if (entry.disposed) return entry.drained;
            if (entry.drained.isCompletedExceptionally() && !entry.cleaning) entry.drained=new CompletableFuture<>(); proof=entry.drained;
            if (entry.active!=null || entry.acquiring || entry.cleaning) return proof; entry.cleaning=true;
        }
        CompletableFuture<Void> actual;
        try { actual=entry.resource==null ? CompletableFuture.completedFuture(null) : entry.resource.release(RequestContext.batch()); }
        catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
        actual.whenComplete((ignored,failure) -> {
            if (failure==null) try { if (entry.resource!=null) entry.resource.closeLocal(); } catch (Throwable close) { failure=close; }
            if (failure==null) synchronized (this) { if (entries.remove(entry.id,entry)) bytes-=entry.bytes; }
            synchronized (entry) { entry.cleaning=false; if (failure==null) entry.disposed=true; }
            if (failure==null) proof.complete(null); else proof.completeExceptionally(unwrap(failure));
        });
        return proof;
    }
    CompletableFuture<Void> invalidateTransaction(long transaction,RequestContext context) {
        List<Entry> matching; List<Flow> streaming;
        synchronized (this) { matching=entries.values().stream().filter(entry -> entry.transaction==transaction).toList(); streaming=flows.stream().filter(flow -> flow.transaction==transaction).toList(); }
        var drains=new ArrayList<CompletableFuture<Void>>(); for (var flow:streaming) drains.add(flow.invalidate());
        for (var entry:matching) drains.add(close(entry.id,context));
        return CompletableFuture.allOf(drains.toArray(CompletableFuture[]::new));
    }
    Flux<KV> stream(long transaction,long column,Keys lower,Keys upper,boolean reverse,Supplier<CompletableFuture<Resource>> source,RequestContext context) {
        return Flux.create(sink -> {
            final Flow flow;
            try { flow=new Flow(transaction,column,copy(lower),copy(upper),reverse,new Deadline(context),sink); synchronized (this) { reserve(flow.charge); flows.add(flow); } }
            catch (Throwable failure) { sink.error(failure); return; }
            sink.onRequest(flow::request); sink.onCancel(flow::cancel);
            CompletableFuture<Resource> actual; try { actual=source.get(); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            actual.whenComplete((resource,failure) -> { synchronized (flow) { flow.resource=resource; flow.acquiring=false; } if (failure!=null) flow.fail(unwrap(failure)); else flow.pump(); });
        },FluxSink.OverflowStrategy.ERROR);
    }
    private final class Flow {
        final long transaction,column,charge; final Keys lower,upper; final boolean reverse; final Deadline deadline; final FluxSink<KV> sink;
        final ArrayDeque<KV> buffered=new ArrayDeque<>(); Resource resource; Keys resume; long demand; boolean acquiring=true,busy,stopped,done,cleaning,disposed;
        CompletableFuture<Void> drained=new CompletableFuture<>();
        final java.util.concurrent.atomic.AtomicInteger pumping=new java.util.concurrent.atomic.AtomicInteger();
        Flow(long transaction,long column,Keys lower,Keys upper,boolean reverse,Deadline deadline,FluxSink<KV> sink) { this.transaction=transaction; this.column=column; this.lower=lower; this.upper=upper; this.reverse=reverse; this.deadline=deadline; this.sink=sink; charge=metadata(lower,upper)+RangeBudget.DEFAULT_MAX_BYTES; }
        void request(long count) { synchronized (this) { demand=count>=Long.MAX_VALUE-demand ? Long.MAX_VALUE : demand+count; } pump(); }
        CompletableFuture<Void> cancel() { synchronized (this) { stopped=true; buffered.clear(); } return cleanupFlow(); }
        void fail(Throwable failure) { synchronized (this) { stopped=true; buffered.clear(); } sink.error(failure); cleanupFlow(); }
        CompletableFuture<Void> invalidate() { synchronized (this) { stopped=true; buffered.clear(); } sink.error(RocksDBException.of(TX_NOT_FOUND,"Range owner invalidated or closed")); return cleanupFlow(); }
        void pump() {
            if (pumping.getAndIncrement()!=0) return;
            int missed=1;
            do { drainOnce(); missed=pumping.addAndGet(-missed); } while (missed!=0);
        }
        void drainOnce() {
            while (true) {
                KV row=null; boolean fetch=false,complete=false,cancelled=false;
                synchronized (this) {
                    if (stopped) cancelled=true;
                    else if (acquiring || busy) return;
                    else if (demand>0 && !buffered.isEmpty()) { row=buffered.pollFirst(); if (demand!=Long.MAX_VALUE) demand--; }
                    else if (buffered.isEmpty() && done) { stopped=true; complete=true; }
                    else if (demand>0 && buffered.isEmpty()) { busy=true; fetch=true; }
                    else return;
                }
                if (cancelled) { cleanupFlow(); return; }
                if (row!=null) { try { deadline.remaining(); sink.next(new KV(copy(row.keys()),row.value()==null ? null : Buf.wrap(row.value().toByteArray()))); } catch (Throwable failure) { fail(failure); return; } continue; }
                if (complete) { cleanupFlow().whenComplete((ignored,failure) -> { if (failure==null) sink.complete(); else sink.error(unwrap(failure)); }); return; }
                if (fetch) {
                    CompletableFuture<RangePage<KV>> actual; try { actual=resource.page(lower,upper,reverse,resume,new RangeBudget(128,RangeBudget.DEFAULT_MAX_BYTES),deadline.remaining()); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
                    actual.whenComplete((page,failure) -> {
                        if (failure==null) try { deadline.remaining(); } catch (Throwable expired) { failure=expired; }
                        synchronized (this) { busy=false; if (failure==null && !stopped) { buffered.addAll(page.items()); resume=copy(page.resumeAfter()); done=!page.hasMore(); } }
                        if (failure!=null) fail(unwrap(failure)); else pump();
                    }); return;
                }
            }
        }
        CompletableFuture<Void> cleanupFlow() {
            final CompletableFuture<Void> proof;
            synchronized (this) {
                if (disposed) return drained;
                if (drained.isCompletedExceptionally() && !cleaning) drained=new CompletableFuture<>(); proof=drained;
                if (acquiring || busy || cleaning) return proof; cleaning=true;
            }
            CompletableFuture<Void> actual;
            try { actual=resource==null ? CompletableFuture.completedFuture(null) : resource.release(RequestContext.batch()); }
            catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            actual.whenComplete((ignored,failure) -> {
                if (failure==null) try { if (resource!=null) resource.closeLocal(); } catch (Throwable close) { failure=close; }
                if (failure==null) synchronized (RatisRocksDBIterators.this) { if (flows.remove(this)) bytes-=charge; }
                synchronized (this) { cleaning=false; if (failure==null) disposed=true; }
                if (failure==null) proof.complete(null); else proof.completeExceptionally(unwrap(failure));
            });
            return proof;
        }
    }
    CompletableFuture<Void> closeAll(RequestContext context) {
        List<Entry> owners; List<Flow> streaming; synchronized (this) { closing=true; owners=List.copyOf(entries.values()); streaming=List.copyOf(flows); }
        var drains=new ArrayList<CompletableFuture<Void>>(); for (var flow:streaming) drains.add(flow.invalidate()); for (var entry:owners) drains.add(close(entry.id,context));
        return CompletableFuture.allOf(drains.toArray(CompletableFuture[]::new));
    }
    private static Keys copy(Keys keys) { return RatisLogicalRangePages.copy(keys); }
    private static Throwable unwrap(Throwable failure) { while ((failure instanceof CompletionException || failure instanceof ExecutionException) && failure.getCause()!=null) failure=failure.getCause(); return failure; }
}
