package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import static it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.*;

/** Bounded transaction/update bridge and native direct point operations. */
public final class RatisRocksDBAdapter implements AutoCloseable {
    public static final int MAX_TRANSACTIONS=16,MAX_WAITERS=16;
    public record Allocation(Path snapshot,Path commit,RatisTransactionClient.Identity identity) {
        public Allocation { Objects.requireNonNull(snapshot); Objects.requireNonNull(commit); Objects.requireNonNull(identity); }
    }
    @FunctionalInterface public interface AllocationSource { CompletableFuture<Allocation> allocate(RequestContext context); }
    public record Stats(int transactions,int opening) {}
    private final DistributedSchema schema;
    private final RangeDirectory directory;
    private final RatisTransactionClient transactions;
    private final AllocationSource allocations;
    private final RatisRocksDBDirect direct;
    private final RatisRocksDBRanges ranges;
    private final RatisRocksDBIterators iterators;
    private final java.util.function.LongSupplier handleIds;
    private final Map<Long,Entry> entries=new LinkedHashMap<>();
    private boolean closing;
    private enum Mode { EXPLICIT, UPDATE }
    private static final Duration UPDATE_TTL=Duration.ofSeconds(10);
    private static final class Entry {
        final long id; final Mode mode;
        volatile RatisTransactionClient.Lease lease;
        volatile boolean acquiring=true,orphan;
        Op active;
        WriteCall pending;
        List<RatisLogicalClient.Change> prefix=List.of();
        int returned; long revision;
        RatisAtomicClient.Operation commit;
        CommandResult.Status outcome;
        boolean consumed,needsReread,updateStaged;
        WriteCall updateWrite; Object updateReturn;
        Entry(long id,Mode mode) { this.id=id; this.mode=mode; }
    }
    private static final class Op {
        final Object key;
        final CompletableFuture<Object> result=new CompletableFuture<>();
        int views;
        Op(Object key) { this.key=key; }
        synchronized CompletableFuture<Object> view() {
            if (!result.isDone() && views>=MAX_WAITERS) return CompletableFuture.failedFuture(RocksDBException.of(SERVER_OVERLOADED,"Transaction observer capacity exhausted"));
            if (!result.isDone()) views++; return result.thenApply(RatisRocksDBAdapter::expose);
        }
    }
    private record ReadCall(String kind,long column,List<Keys> keys,RequestType.RequestTypeId type,Object bounds) {}
    private static final class WriteCall {
        final long column; final List<Keys> keys; final List<Buf> values; final RequestType.RequestTypeId type; final RatisRocksDBDirect.WriteKind kind; final boolean multi;
        final List<RatisLogicalClient.Change> prefix,basePrefix; final int firstResult;
        WriteCall(long column,List<Keys> keys,List<Buf> values,RequestType.RequestTypeId type,RatisRocksDBDirect.WriteKind kind,boolean multi,List<RatisLogicalClient.Change> prefix,List<RatisLogicalClient.Change> basePrefix,int firstResult) {
            this.column=column; this.keys=keys; this.values=values; this.type=type; this.kind=kind; this.multi=multi; this.prefix=prefix; this.basePrefix=basePrefix; this.firstResult=firstResult;
        }
        boolean same(WriteCall other) {
            if (column!=other.column || type!=other.type || kind!=other.kind || multi!=other.multi || !keys.equals(other.keys) || values.size()!=other.values.size()) return false;
            for (int n=0;n<values.size();n++) if (!Utils.valueEquals(values.get(n),other.values.get(n))) return false;
            return true;
        }
    }
    private record PageBounds(Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget) {}
    private static final class Deadline {
        final RequestContext context; final long opened=System.nanoTime();
        Deadline(RequestContext context) { this.context=Objects.requireNonNull(context); }
        RequestContext remaining() {
            if (context.timeoutNanos()==RequestContext.NO_TIMEOUT) return context;
            long remaining=context.timeoutNanos()-(System.nanoTime()-opened);
            if (remaining<=0) throw RocksDBException.of(READ_DEADLINE_EXCEEDED,"Request deadline expired; original transaction ownership retained");
            return new RequestContext(context.profile(),remaining);
        }
    }
    public RatisRocksDBAdapter(DistributedSchema schema,RangeDirectory directory,RatisTransactionClient transactions,AllocationSource allocations,RatisRocksDBDirect direct,RatisRocksDBRanges ranges) {
        this(schema,directory,transactions,allocations,direct,ranges,new java.util.concurrent.atomic.AtomicLong(ThreadLocalRandom.current().nextLong(1,Long.MAX_VALUE/2))::incrementAndGet);
    }
    public RatisRocksDBAdapter(DistributedSchema schema,RangeDirectory directory,RatisTransactionClient transactions,AllocationSource allocations,RatisRocksDBDirect direct,RatisRocksDBRanges ranges,java.util.function.LongSupplier handleIds) {
        this.direct=direct; this.ranges=ranges; this.schema=Objects.requireNonNull(schema); this.directory=Objects.requireNonNull(directory); this.transactions=Objects.requireNonNull(transactions); this.allocations=Objects.requireNonNull(allocations); this.handleIds=Objects.requireNonNull(handleIds); schema.requireMatches(directory); iterators=new RatisRocksDBIterators(schema,handleIds);
    }
    synchronized boolean ownsTransactionOrUpdate(long id) { return entries.containsKey(id); }
    boolean ownsIterator(long id) { return iterators.owns(id); }
    record OwnedDispatch(Object observer,CompletableFuture<Void> drained,Runnable cancelObserver) {}
    private static final class Tracking { final List<CompletableFuture<?>> actual=new ArrayList<>(); final List<Runnable> cancel=new ArrayList<>(); }
    private final ThreadLocal<Tracking> tracking=new ThreadLocal<>();
    OwnedDispatch dispatchOwned(RocksDBAPICommand<?,?,?> request,RequestContext context) {
        var tracker=new Tracking(); tracking.set(tracker);
        try {
            Object observer=api(context).requestAsync(request);
            if (tracker.actual.isEmpty() && observer instanceof CompletableFuture<?> future) tracker.actual.add(future.handle((value,failure) -> null));
            var drained=CompletableFuture.allOf(tracker.actual.stream().map(future -> future.handle((value,failure) -> null)).toArray(CompletableFuture[]::new));
            return new OwnedDispatch(observer,drained,() -> tracker.cancel.forEach(Runnable::run));
        } finally { tracking.remove(); }
    }
    private void track(CompletableFuture<?> actual) { var tracker=tracking.get(); if (tracker!=null) tracker.actual.add(actual); }
    private CompletableFuture<Object> directView(CompletableFuture<Object> source,java.util.function.Function<Object,Object> mapper) {
        var actual=direct.actualCompletion(source); track(actual); var view=source.thenApply(mapper); var tracker=tracking.get(); if (tracker!=null) tracker.cancel.add(() -> view.cancel(false));
        view.whenComplete((value,failure) -> { if (view.isCancelled()) source.cancel(false); }); return view;
    }
    public RocksDBAsyncAPI api(RequestContext context) {
        Objects.requireNonNull(context);
        return new RocksDBAsyncAPI() {
            @SuppressWarnings("unchecked") public <R,RS,RA> RA requestAsync(RocksDBAPICommand<R,RS,RA> request) {
                var deadline=new Deadline(context);
                try {
                    WorkloadAdmission.resolve(context,request);
                    if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> range) return (RA)range(range.transactionId(),range.columnId(),range.startKeysInclusive(),range.endKeysExclusive(),range.reverse(),range.requestType().getRequestTypeId(),context);
                    if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream<?>) return (RA)Flux.error(unsupported());
                    return (RA)dispatch(request,deadline);
                } catch (Throwable failure) { return (RA)(request instanceof RocksDBAPICommand.RocksDBAPICommandStream<?> ? Flux.error(failure) : CompletableFuture.failedFuture(failure)); }
            }
            public Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch> cdcPollBatchAsync(String id,Long fromSeq,long maxEvents) { return Mono.error(unsupported()); }
        };
    }
    public synchronized Stats stats() { return new Stats(entries.size(),(int)entries.values().stream().filter(entry -> entry.acquiring).count()); }
    private CompletableFuture<?> dispatch(RocksDBAPICommand<?,?,?> request,Deadline deadline) throws Exception {
        synchronized (this) { if (closing) throw new IOException("Adapter closing"); }
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction open) return open(open.transactionLeaseTtl(),deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction close) return close(close.transactionId(),close.commit(),deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate close) return closeFailedUpdate(close.updateId(),deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.GetColumnId column) return CompletableFuture.completedFuture(schema.column(column.name()).apiId());
        if (request instanceof RocksDBAPICommand.GetAllColumnDefinitions) {
            var definitions=new LinkedHashMap<String,ColumnSchema>(); for (var column:schema.columns()) definitions.put(column.name(),column.schema()); return CompletableFuture.completedFuture(Collections.unmodifiableMap(definitions));
        }
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> get) return get(get.transactionOrUpdateId(),get.columnId(),get.keys(),get.requestType().getRequestTypeId(),deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti exists) return exists(exists.transactionId(),exists.columnId(),exists.keys(),deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> put) return write(put.transactionOrUpdateId(),put.columnId(),List.of(put.keys()),Collections.singletonList(put.value()),put.requestType().getRequestTypeId(),RatisRocksDBDirect.WriteKind.PUT,false,deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> delete) return write(delete.transactionOrUpdateId(),delete.columnId(),List.of(delete.keys()),Collections.singletonList(null),delete.requestType().getRequestTypeId(),RatisRocksDBDirect.WriteKind.DELETE,false,deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> put) return write(put.transactionOrUpdateId(),put.columnId(),put.keys(),put.values(),put.requestType().getRequestTypeId(),RatisRocksDBDirect.WriteKind.PUT,true,deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> delete) return write(delete.transactionOrUpdateId(),delete.columnId(),delete.keys(),Collections.nCopies(delete.keys().size(),null),delete.requestType().getRequestTypeId(),RatisRocksDBDirect.WriteKind.DELETE,true,deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> merge) return write(merge.transactionOrUpdateId(),merge.columnId(),List.of(merge.keys()),List.of(merge.value()),merge.requestType().getRequestTypeId(),RatisRocksDBDirect.WriteKind.MERGE,false,deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> merge) return write(merge.transactionOrUpdateId(),merge.columnId(),merge.keys(),merge.values(),merge.requestType().getRequestTypeId(),RatisRocksDBDirect.WriteKind.MERGE,true,deadline);
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.GetRangePage<?> page) {
            var type=page.requestType().getRequestTypeId(); if (type!=RequestType.RequestTypeId.ALL_IN_RANGE && type!=RequestType.RequestTypeId.ALL_IN_RANGE_NO_CACHE) throw unsupported();
            schema.column(page.columnId()); var bounds=new PageBounds(copy(page.startKeysInclusive()),copy(page.endKeysExclusive()),page.reverse(),copy(page.resumeAfter()),page.budget());
            validateBudget(bounds.budget());
            if (page.transactionId()==0) { if (ranges==null) throw unsupported(); return ranges.page(page.columnId(),bounds.lower(),bounds.upper(),bounds.reverse(),bounds.resume(),bounds.budget(),deadline.remaining()); }
            var entry=entry(page.transactionId());
            return invoke(entry,new ReadCall("page",page.columnId(),List.of(),type,bounds),() -> entry.lease.page(page.columnId(),bounds.lower(),bounds.upper(),bounds.reverse(),bounds.resume(),bounds.budget(),deadline.remaining()).thenApply(value -> { deadline.remaining(); return value; }));
        }
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator iterator) {
            schema.column(iterator.columnId()); Keys lower=copy(iterator.startKeysInclusive()),upper=copy(iterator.endKeysExclusive());
            return iterators.open(iterator.transactionId(),iterator.columnId(),lower,upper,iterator.reverse(),iterator.iteratorLeaseTtl(),() -> resource(iterator.transactionId(),iterator.columnId(),lower,upper,iterator.reverse(),iterator.iteratorLeaseTtl(),deadline,false),deadline.remaining());
        }
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator iterator) return iterators.close(iterator.iteratorId(),deadline.remaining());
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo seek) return iteratorOperation(seek.iterationId(),new ReadCall("seek",seek.iterationId(),List.of(copy(seek.keys())),RequestType.RequestTypeId.NOTHING,null),() -> iterators.seek(seek.iterationId(),seek.keys(),deadline.remaining()));
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<?> next) return iteratorOperation(next.iterationId(),new ReadCall("next",next.iterationId(),List.of(),next.requestType().getRequestTypeId(),List.of(next.skipCount(),next.takeCount())),() -> iterators.subsequent(next.iterationId(),next.skipCount(),next.takeCount(),next.requestType().getRequestTypeId(),deadline.remaining()));
        if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> reduce) {
            var type=reduce.requestType().getRequestTypeId();
            if (type==RequestType.RequestTypeId.FIRST_AND_LAST) {
                Keys lower=copy(reduce.startKeysInclusive()),upper=copy(reduce.endKeysExclusive());
                if (reduce.transactionId()==0) { if (ranges==null) throw unsupported(); return ranges.firstAndLast(reduce.columnId(),lower,upper,reduce.reverse(),deadline.remaining()); }
                var entry=entry(reduce.transactionId()); var budget=new RangeBudget(1,RangeBudget.DEFAULT_MAX_BYTES);
                return invoke(entry,new ReadCall("first-last",reduce.columnId(),List.of(),type,new PageBounds(lower,upper,reduce.reverse(),null,budget)),
                        () -> entry.lease.page(reduce.columnId(),lower,upper,reduce.reverse(),null,budget,deadline.remaining()).thenCompose(first -> {
                            deadline.remaining();
                            return first.items().isEmpty() ? CompletableFuture.completedFuture(new FirstAndLast<KV>(null,null))
                                    : entry.lease.page(reduce.columnId(),lower,upper,!reduce.reverse(),null,budget,deadline.remaining()).thenApply(last -> {
                                        deadline.remaining(); return new FirstAndLast<>(first.items().getFirst(),last.items().getFirst());
                                    });
                        }));
            }
            if (type==RequestType.RequestTypeId.ENTRIES_COUNT) return range(reduce.transactionId(),reduce.columnId(),reduce.startKeysInclusive(),reduce.endKeysExclusive(),reduce.reverse(),RequestType.RequestTypeId.ALL_IN_RANGE,deadline.remaining()).count().toFuture();
            throw unsupported();
        }
        throw unsupported();
    }
    private CompletableFuture<?> iteratorOperation(long iterator,Object key,Supplier<CompletableFuture<?>> work) {
        long transaction=iterators.transaction(iterator); return transaction==0 ? work.get() : invoke(entry(transaction),key,work);
    }
    private static void validateBudget(RangeBudget budget) {
        if (budget.maxItems()>RangeBudget.DEFAULT_MAX_ITEMS || budget.maxBytes()>RangeBudget.DEFAULT_MAX_BYTES) throw RocksDBException.of(PUT_INVALID_REQUEST,"Range budget exceeds server maximum");
    }
    private CompletableFuture<RatisRocksDBIterators.Resource> resource(long transaction,long column,Keys lower,Keys upper,boolean reverse,Duration ttl,Deadline deadline,boolean guard) {
        if (transaction==0) {
            if (ranges==null) return CompletableFuture.failedFuture(unsupported());
            return ranges.open(column,lower,upper,reverse,ttl,deadline.remaining()).thenApply(owner -> new RatisRocksDBIterators.Resource() {
                public CompletableFuture<RangePage<KV>> page(Keys low,Keys high,boolean back,Keys resume,RangeBudget budget,RequestContext context) { return owner.page(low,high,back,resume,budget,context); }
                public CompletableFuture<Void> release(RequestContext context) { return owner.release(context); }
                public void closeLocal() throws IOException { owner.closeLocal(); }
            });
        }
        try {
            var entry=entry(transaction); final long revision;
            synchronized (entry) { if (entry.active!=null || entry.pending!=null || entry.commit!=null || entry.needsReread) throw new IOException("Transaction range ownership busy or unavailable"); revision=entry.revision; }
            RatisLogicalRangePages.validateBounds(RatisSnapshotPlan.scope(entry.lease.snapshot().initial().fence()),schema,column,lower,upper);
            return CompletableFuture.completedFuture(new RatisRocksDBIterators.Resource() {
                public CompletableFuture<RangePage<KV>> page(Keys low,Keys high,boolean back,Keys resume,RangeBudget budget,RequestContext context) {
                    synchronized (entry) { if (entry.revision!=revision) return CompletableFuture.failedFuture(RocksDBException.of(TX_NOT_FOUND,"Iterator staged revision changed")); }
                    Supplier<CompletableFuture<?>> reading=() -> entry.lease.page(column,low,high,back,resume,budget,context);
                    @SuppressWarnings("unchecked") CompletableFuture<RangePage<KV>> result=guard ? (CompletableFuture<RangePage<KV>>)(CompletableFuture<?>)invoke(entry,new ReadCall("stream-page",column,List.of(),RequestType.RequestTypeId.ALL_IN_RANGE,new PageBounds(low,high,back,resume,budget)),reading) : entry.lease.page(column,low,high,back,resume,budget,context);
                    return result.thenApply(value -> { synchronized (entry) { if (entry.revision!=revision) throw RocksDBException.of(TX_NOT_FOUND,"Iterator staged revision changed"); } return value; });
                }
                public CompletableFuture<Void> release(RequestContext context) { return CompletableFuture.completedFuture(null); }
                public void closeLocal() {}
            });
        } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    private Flux<KV> range(long transaction,long column,Keys lower,Keys upper,boolean reverse,RequestType.RequestTypeId type,RequestContext context) {
        if (type!=RequestType.RequestTypeId.ALL_IN_RANGE && type!=RequestType.RequestTypeId.ALL_IN_RANGE_NO_CACHE) return Flux.error(unsupported());
        schema.column(column); Keys low=copy(lower),high=copy(upper);
        return Flux.defer(() -> {
            var deadline=new Deadline(context);
            return iterators.stream(transaction,column,low,high,reverse,() -> resource(transaction,column,low,high,reverse,Duration.ofSeconds(60),deadline,true),deadline.remaining());
        });
    }
    private CompletableFuture<Long> open(Duration ttl,Deadline deadline) { return open(ttl,deadline,Mode.EXPLICIT); }
    private CompletableFuture<Long> open(Duration ttl,Deadline deadline,Mode mode) {
        try {
            if (Objects.requireNonNull(ttl).isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofSeconds(60))>0 || !ttl.equals(Duration.ofMillis(ttl.toMillis()))) throw new IllegalArgumentException("Transaction TTL must be whole milliseconds up to60seconds");
            var scope=new TransactionScope(schema.columns().stream().map(column -> TransactionScope.Span.range(column.columnId(),null,null)).toList()); scope.participants(directory,SnapshotFence.MAX_PARTICIPANTS);
            final Entry entry;
            synchronized (this) { if (closing || entries.size()>=MAX_TRANSACTIONS) throw RocksDBException.of(SERVER_OVERLOADED,"Transaction ID capacity exhausted");
                long id=handleIds.getAsLong(); if (id<=0 || entries.containsKey(id)) throw new IllegalStateException("Shared handle allocator returned an invalid or reused ID"); entry=new Entry(id,mode); entries.put(entry.id,entry); }
            var view=new CompletableFuture<Long>();
            CompletableFuture<Allocation> allocation;
            try { allocation=Objects.requireNonNull(allocations.allocate(deadline.remaining())); }
            catch (Throwable failure) { synchronized (this) { entries.remove(entry.id,entry); } return CompletableFuture.failedFuture(failure); }
            allocation.thenCompose(saved -> {
                try { entry.lease=transactions.open(saved.snapshot(),saved.commit(),saved.identity(),scope,ttl,deadline.remaining()); return entry.lease.completion(); }
                catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
            }).whenComplete((ignored,failure) -> {
                synchronized (entry) { entry.acquiring=false; entry.orphan=view.isCancelled() || failure!=null; }
                if (failure!=null) view.completeExceptionally(unwrap(failure));
                else try { deadline.remaining(); if (!view.complete(entry.id)) entry.orphan=true; } catch (Throwable expired) { entry.orphan=true; view.completeExceptionally(expired); }
                if (view.isCancelled()) entry.orphan=true;
                if (entry.orphan) orphanCleanup(entry);
            });
            return view;
        } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
    }
    private void orphanCleanup(Entry entry) {
        if (entry.lease==null) { synchronized (this) { entries.remove(entry.id,entry); } return; }
        if (entry.commit!=null && entry.outcome==null) {
            close(entry.id,true,new Deadline(RequestContext.batch())).whenComplete((committed,failure) -> {
                if (failure==null && Boolean.FALSE.equals(committed)) close(entry.id,false,new Deadline(RequestContext.batch()));
            });
        } else close(entry.id,false,new Deadline(RequestContext.batch()));
    }
    private synchronized Entry entry(long id) {
        if (id<=0) throw unsupported(); var entry=entries.get(id);
        if (entry==null || entry.acquiring || entry.orphan) throw RocksDBException.of(TX_NOT_FOUND,"Unknown or unexposed transaction ID"); return entry;
    }
    private CompletableFuture<Object> invoke(Entry entry,Object key,Supplier<? extends CompletableFuture<?>> work) {
        final Op operation;
        synchronized (entry) {
            if (entry.active!=null) {
                if (Objects.equals(entry.active.key,key) || entry.active.key instanceof WriteCall old && key instanceof WriteCall next && old.same(next)) return entry.active.view();
                return CompletableFuture.failedFuture(RocksDBException.of(SERVER_OVERLOADED,"Transaction operation already active"));
            }
            if (key instanceof WriteCall write && entry.pending==null && !(entry.mode==Mode.UPDATE && entry.updateWrite==write) && entry.prefix!=write.basePrefix) return CompletableFuture.failedFuture(RocksDBException.of(SERVER_OVERLOADED,"Staged prefix advanced before admission; retry this request"));
            if (entry.pending!=null && (!(key instanceof WriteCall next) || !entry.pending.same(next)) && !key.equals("rollback")) return CompletableFuture.failedFuture(new IOException("Resume the exact uncertain staged request"));
            entry.active=operation=new Op(key);
        }
        CompletableFuture<?> actual;
        try { actual=Objects.requireNonNull(work.get()); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
        actual.whenComplete((value,failure) -> {
            synchronized (entry) { if (entry.active==operation) entry.active=null; }
            if (failure==null) operation.result.complete(value); else operation.result.completeExceptionally(unwrap(failure));
        });
        track(operation.result); return operation.view();
    }
    private CompletableFuture<?> get(long id,long column,Keys key,RequestType.RequestTypeId type,Deadline deadline) {
        if (type!=RequestType.RequestTypeId.CURRENT && type!=RequestType.RequestTypeId.EXISTS && type!=RequestType.RequestTypeId.FOR_UPDATE && type!=RequestType.RequestTypeId.NOTHING) throw unsupported();
        if (id==0 && type==RequestType.RequestTypeId.FOR_UPDATE) return initialUpdate(column,key,deadline);
        if (id==0 && direct!=null) {
            var shape=schema.column(column); schema.encodeLogicalKey(shape.columnId(),Objects.requireNonNull(key));
            return directView(direct.get(column,copy(key),type,deadline.remaining()),value -> { deadline.remaining(); return expose(value); });
        }
        if (id<=0) throw unsupported(); var shape=schema.column(column); schema.encodeLogicalKey(shape.columnId(),Objects.requireNonNull(key)); var frozen=copy(key);
        if (!shape.schema().hasValue() && (type==RequestType.RequestTypeId.CURRENT || type==RequestType.RequestTypeId.FOR_UPDATE)) throw RocksDBException.of(VALUE_MUST_BE_NULL,"This callback requires a value-bearing column");
        var entry=entry(id);
        return invoke(entry,new ReadCall("get",column,List.of(frozen),type,null),() -> {
            if (entry.mode==Mode.UPDATE && (entry.needsReread || entry.outcome==CommandResult.Status.CONFLICT)) {
                if (type!=RequestType.RequestTypeId.FOR_UPDATE) throw new RocksDBRetryException();
                return refreshUpdate(entry,column,frozen,deadline);
            }
            if (entry.commit!=null) throw new CompletionException(new IOException("Transaction commit already frozen"));
            if (type==RequestType.RequestTypeId.NOTHING) { deadline.remaining(); return CompletableFuture.completedFuture(null); }
            return entry.lease.get(column,frozen,deadline.remaining()).thenApply(value -> { deadline.remaining(); return switch (type) { case EXISTS -> value!=null; case FOR_UPDATE -> new UpdateContext<>(value,id); default -> value; }; });
        });
    }
    private CompletableFuture<?> initialUpdate(long column,Keys key,Deadline deadline) {
        var shape=schema.column(column); schema.encodeLogicalKey(shape.columnId(),Objects.requireNonNull(key));
        if (!shape.schema().hasValue()) throw RocksDBException.of(VALUE_MUST_BE_NULL,"FOR_UPDATE requires a value-bearing column");
        var frozen=copy(key); var retained=new java.util.concurrent.atomic.AtomicReference<Entry>(); var view=new CompletableFuture<Object>();
        open(UPDATE_TTL,deadline,Mode.UPDATE).thenCompose(id -> {
            var entry=entry(id); retained.set(entry); return get(id,column,frozen,RequestType.RequestTypeId.FOR_UPDATE,deadline);
        }).whenComplete((value,failure) -> {
            var entry=retained.get();
            if (failure!=null) view.completeExceptionally(unwrap(failure)); else view.complete(expose(value));
            if (entry!=null && (view.isCancelled() || failure!=null)) { entry.orphan=true; orphanCleanup(entry); }
        });
        return view;
    }
    private CompletableFuture<?> refreshUpdate(Entry entry,long column,Keys key,Deadline deadline) {
        CompletableFuture<Void> cleaned=entry.needsReread ? CompletableFuture.completedFuture(null) : cleanupAttempt(entry,deadline,false);
        return cleaned.thenCompose(ignored -> {
            if (entry.lease!=null) return entry.lease.resume(deadline.remaining());
            var scope=new TransactionScope(schema.columns().stream().map(value -> TransactionScope.Span.range(value.columnId(),null,null)).toList()); scope.participants(directory,SnapshotFence.MAX_PARTICIPANTS);
            return allocations.allocate(deadline.remaining()).thenCompose(saved -> {
                try { entry.lease=transactions.open(saved.snapshot(),saved.commit(),saved.identity(),scope,UPDATE_TTL,deadline.remaining()); }
                catch (IOException failure) { return CompletableFuture.failedFuture(failure); }
                return entry.lease.completion();
            });
        }).thenCompose(ignored -> entry.lease.get(column,key,deadline.remaining())).thenApply(value -> {
            deadline.remaining(); synchronized (entry) { entry.needsReread=false; }
            return new UpdateContext<>(value,entry.id);
        });
    }
    private CompletableFuture<?> exists(long id,long column,List<Keys> keys,Deadline deadline) {
        if (id==0 && direct!=null) {
            if (keys.size()>DistributedCommandCodec.MAX_ITEMS) throw new IllegalArgumentException("Existence query exceeds item budget");
            for (var key:keys) schema.encodeLogicalKey(schema.column(column).columnId(),key);
            var frozen=keys.stream().map(RatisRocksDBAdapter::copy).toList(); CompletableFuture<List<Boolean>> result=CompletableFuture.completedFuture(new ArrayList<>());
            for (var key:frozen) result=result.thenCompose(values -> direct.get(column,key,RequestType.RequestTypeId.EXISTS,deadline.remaining()).thenApply(value -> { values.add((Boolean)value); return values; }));
            return result.thenApply(values -> { deadline.remaining(); return List.copyOf(values); });
        }
        var entry=entry(id); if (keys.size()>DistributedCommandCodec.MAX_ITEMS) throw new IllegalArgumentException("Existence query exceeds item budget");
        for (var key:keys) schema.encodeLogicalKey(schema.column(column).columnId(),key);
        var frozen=keys.stream().map(RatisRocksDBAdapter::copy).toList();
        return invoke(entry,new ReadCall("exists",column,frozen,RequestType.RequestTypeId.EXISTS,null),() -> {
            CompletableFuture<List<Boolean>> result=CompletableFuture.completedFuture(new ArrayList<>());
            for (var key:frozen) result=result.thenCompose(values -> entry.lease.get(column,key,deadline.remaining()).thenApply(value -> { values.add(value!=null); return values; }));
            return result.thenApply(values -> { deadline.remaining(); return List.copyOf(values); });
        });
    }
    private CompletableFuture<?> write(long id,long column,List<Keys> keys,List<Buf> values,RequestType.RequestTypeId type,RatisRocksDBDirect.WriteKind kind,boolean multi,Deadline deadline) throws Exception {
        if (kind==RatisRocksDBDirect.WriteKind.MERGE ? type!=RequestType.RequestTypeId.NOTHING && type!=RequestType.RequestTypeId.MERGED
                : !(type==RequestType.RequestTypeId.NOTHING || type==RequestType.RequestTypeId.PREVIOUS || type==RequestType.RequestTypeId.PREVIOUS_PRESENCE || kind==RatisRocksDBDirect.WriteKind.PUT && (type==RequestType.RequestTypeId.DELTA || type==RequestType.RequestTypeId.CHANGED || type==RequestType.RequestTypeId.ENSURE))) throw unsupported();
        var shape=schema.column(column); if (values==null && !shape.schema().hasValue()) values=Collections.nCopies(keys.size(),null); if (keys.size()!=values.size()) throw new IllegalArgumentException("Keys and values differ in count");
        if (keys.size()>DistributedCommandCodec.MAX_ITEMS) throw new IllegalArgumentException("Write exceeds item budget");
        long input=0; for (var key:keys) for (var component:key.keys()) { input+=Objects.requireNonNull(component).size(); if (input>RatisTransactionPlan.MAX_BYTES) throw new IllegalArgumentException("Write input exceeds prefix byte budget"); }
        for (var value:values) { input+=value==null ? 0 : value.size(); if (input>RatisTransactionPlan.MAX_BYTES) throw new IllegalArgumentException("Write input exceeds prefix byte budget"); }
        for (var key:keys) schema.encodeLogicalKey(shape.columnId(),key);
        for (var value:values) if (value!=null && value.size()>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Write value exceeds byte budget");
        var frozenKeys=keys.stream().map(RatisRocksDBAdapter::copy).toList(); var frozenValues=values.stream().map(value -> value==null ? null : Buf.wrap(value.toByteArray())).toList();
        for (int n=0;n<frozenKeys.size();n++) {
            schema.encodeLogicalKey(shape.columnId(),frozenKeys.get(n));
            if (kind==RatisRocksDBDirect.WriteKind.MERGE && (shape.operator()==null || frozenValues.get(n)==null)) throw RocksDBException.of(PUT_INVALID_REQUEST,"MERGE requires a qualified operator and operand");
            if (kind==RatisRocksDBDirect.WriteKind.PUT && (shape.schema().hasValue() ? frozenValues.get(n)==null : frozenValues.get(n)!=null)) throw RocksDBException.of(shape.schema().hasValue() ? UNEXPECTED_NULL_VALUE : VALUE_MUST_BE_NULL,"Value differs from column shape");
        }
        boolean returns=type!=RequestType.RequestTypeId.NOTHING && type!=RequestType.RequestTypeId.ENSURE;
        if (id==0 && direct!=null) return directView(direct.writeSequence(column,frozenKeys,frozenValues,kind,type,multi,deadline.remaining()),RatisRocksDBAdapter::expose);
        var entry=entry(id);
        final WriteCall request;
        synchronized (entry) {
            if (entry.mode==Mode.UPDATE && (kind==RatisRocksDBDirect.WriteKind.MERGE || type!=RequestType.RequestTypeId.NOTHING && type!=RequestType.RequestTypeId.ENSURE || kind==RatisRocksDBDirect.WriteKind.DELETE && type!=RequestType.RequestTypeId.NOTHING)) throw RocksDBException.of(PUT_INVALID_REQUEST,"Implicit updates do not permit previous-value callbacks");
            if (entry.mode==Mode.UPDATE && entry.needsReread) throw new RocksDBRetryException();
            if (entry.commit!=null && entry.mode!=Mode.UPDATE) throw new IOException("Transaction commit already frozen");
            var changes=new ArrayList<>(entry.prefix);
            for (int n=0;n<frozenKeys.size();n++) {
                var mode=!returns ? Mutation.ReturnMode.NONE : kind==RatisRocksDBDirect.WriteKind.MERGE ? Mutation.ReturnMode.CURRENT : Mutation.ReturnMode.PREVIOUS;
                changes.add(switch (kind) {
                    case PUT -> RatisLogicalClient.Change.put(column,frozenKeys.get(n),frozenValues.get(n),mode);
                    case DELETE -> RatisLogicalClient.Change.delete(column,frozenKeys.get(n),mode);
                    case MERGE -> RatisLogicalClient.Change.merge(column,frozenKeys.get(n),frozenValues.get(n),mode);
                });
            }
            var candidate=new WriteCall(column,frozenKeys,frozenValues,type,kind,multi,List.copyOf(changes),entry.prefix,entry.returned);
            if (entry.mode==Mode.UPDATE && entry.updateWrite!=null) {
                if (!entry.updateWrite.same(candidate)) throw new IOException("Resume the exact uncertain update write"); request=entry.updateWrite;
            } else request=candidate;
            RatisLogicalClient.plan(schema,request.prefix,List.of(),RatisTransactionPlan.MAX_BYTES); // reject invalid full prefixes before claiming uncertain native ownership
        }
        return invoke(entry,request,() -> {
            if (request.keys.isEmpty()) {
                deadline.remaining();
                if (entry.mode!=Mode.UPDATE) return CompletableFuture.completedFuture(multi ? List.of() : null);
                synchronized (entry) { entry.updateWrite=request; entry.updateStaged=true; entry.updateReturn=multi ? List.of() : null; }
                return finishUpdate(entry,request,deadline);
            }
            synchronized (entry) { if (entry.mode==Mode.UPDATE) entry.updateWrite=request; }
            if (entry.mode==Mode.UPDATE && entry.updateStaged) return finishUpdate(entry,request,deadline);
            synchronized (entry) { if (entry.pending==null) entry.pending=request; }
            var pending=entry.pending;
            return iterators.invalidateTransaction(entry.id,deadline.remaining()).thenCompose(ignored -> entry.lease.stage(pending.prefix,List.of(),deadline.remaining())).thenApply(results -> {
                deadline.remaining();
                var logical=RatisLogicalClient.plan(schema,pending.prefix,List.of(),RatisTransactionPlan.MAX_BYTES); final RatisTransactionPlan plan;
                try { plan=RatisTransactionPlan.captured(directory,entry.lease.identity().commit().business(),logical.mutations(),logical.predicates(),entry.lease.snapshot()); }
                catch (IOException unavailable) { throw new CompletionException(unavailable); }
                var ordered=new HashMap<Integer,byte[]>();
                for (var part:plan.partitions()) { var result=results.get(part.route().groupId()); if (result==null || result.valueCount()!=part.resultPositions().size()) throw new CompletionException(new IOException("Stage result mapping changed")); for (int n=0;n<result.valueCount();n++) ordered.put(part.resultPositions().get(n),result.value(n)); }
                var returned=new ArrayList<Object>();
                for (int n=0;n<pending.keys.size();n++) {
                    byte[] raw=returns ? ordered.get(pending.firstResult+n) : null; Buf previous=raw==null ? null : Buf.wrap(raw); Buf current=pending.kind==RatisRocksDBDirect.WriteKind.DELETE ? null : shape.schema().hasValue() ? pending.values.get(n) : Buf.wrap(new byte[0]);
                    returned.add(switch (pending.type) { case PREVIOUS,MERGED -> previous; case PREVIOUS_PRESENCE -> raw!=null; case DELTA -> new Delta<>(previous,current); case CHANGED -> !Utils.valueEquals(previous,current); default -> null; });
                }
                deadline.remaining();
                synchronized (entry) { entry.prefix=pending.prefix; entry.returned=pending.firstResult+(returns ? pending.keys.size() : 0); entry.pending=null; entry.revision++; }
                var result=pending.multi ? returns ? Collections.unmodifiableList(returned) : List.of() : returned.isEmpty() ? null : returned.getFirst();
                if (entry.mode==Mode.UPDATE) { synchronized (entry) { entry.updateStaged=true; entry.updateReturn=result; } }
                return result;
            }).thenCompose(result -> entry.mode==Mode.UPDATE ? finishUpdate(entry,request,deadline) : CompletableFuture.completedFuture(result));
        });
    }
    private CompletableFuture<?> close(long id,boolean commit,Deadline deadline) {
        final Entry entry;
        synchronized (this) { entry=entries.get(id); if (entry==null) return commit ? CompletableFuture.failedFuture(RocksDBException.of(TX_NOT_FOUND,"Unknown transaction")) : CompletableFuture.completedFuture(true); }
        if (entry.acquiring) return CompletableFuture.failedFuture(new IOException("Transaction acquisition still active"));
        return invoke(entry,commit ? "commit" : "rollback",() -> {
            deadline.remaining();
            if (entry.needsReread && entry.lease==null) { if (commit) return CompletableFuture.failedFuture(new RocksDBRetryException()); synchronized (this) { entries.remove(entry.id,entry); } return CompletableFuture.completedFuture(true); }
            if (!commit) {
                return iterators.invalidateTransaction(entry.id,deadline.remaining()).thenCompose(ignored -> entry.commit!=null && (entry.consumed || entry.commit.terminalReady()) ? consume(entry,deadline) : CompletableFuture.completedFuture(null)).thenCompose(ignored -> entry.lease.rollback(deadline.remaining())).thenApply(ignored -> { finish(entry); return true; });
            }
            return commitAttempt(entry,deadline);

        });
    }
    private CompletableFuture<Boolean> commitAttempt(Entry entry,Deadline deadline) {
        return iterators.invalidateTransaction(entry.id,deadline.remaining()).thenCompose(ignored -> commitAfterRangeDrain(entry,deadline));
    }
    private CompletableFuture<Boolean> commitAfterRangeDrain(Entry entry,Deadline deadline) {
        if (entry.outcome==CommandResult.Status.CONFLICT) return CompletableFuture.completedFuture(false);
        try { if (entry.commit==null) entry.commit=entry.lease.commit(deadline.remaining()); }
        catch (IOException failure) { return CompletableFuture.failedFuture(failure); }
        CompletableFuture<CommandResult> result=entry.outcome==CommandResult.Status.SUCCESS ? CompletableFuture.completedFuture(new CommandResult(CommandResult.Status.SUCCESS,List.of())) : entry.commit.resume(deadline.remaining());
        return result.thenCompose(outcome -> {
            deadline.remaining(); entry.outcome=outcome.status();
            if (outcome.status()==CommandResult.Status.CONFLICT) return CompletableFuture.completedFuture(false);
            if (outcome.status()!=CommandResult.Status.SUCCESS) return CompletableFuture.failedFuture(RocksDBException.of(COMMIT_FAILED_TRY_AGAIN,"Transaction commit has no successful outcome"));
            return consume(entry,deadline).thenCompose(ignored -> entry.lease.release(deadline.remaining())).thenApply(ignored -> { finish(entry); return true; });
        });
    }
    private CompletableFuture<Object> finishUpdate(Entry entry,WriteCall request,Deadline deadline) {
        return commitAttempt(entry,deadline).thenCompose(committed -> {
            if (committed) return CompletableFuture.completedFuture(entry.updateReturn);
            return cleanupAttempt(entry,deadline,request.multi).thenCompose(ignored -> CompletableFuture.failedFuture(new RocksDBRetryException()));
        });
    }
    private CompletableFuture<Void> cleanupAttempt(Entry entry,Deadline deadline,boolean remove) {
        return iterators.invalidateTransaction(entry.id,deadline.remaining()).thenCompose(ignored -> entry.commit==null ? CompletableFuture.completedFuture(null) : consume(entry,deadline)).thenCompose(ignored -> entry.lease.rollback(deadline.remaining())).thenRun(() -> {
            try { entry.lease.close(); } catch (IOException failure) { throw new CompletionException(failure); }
            if (remove) { synchronized (this) { entries.remove(entry.id,entry); } }
            else synchronized (entry) { entry.lease=null; entry.commit=null; entry.outcome=null; entry.consumed=false; entry.prefix=List.of(); entry.returned=0; entry.pending=null; entry.updateWrite=null; entry.updateReturn=null; entry.updateStaged=false; entry.needsReread=true; }
        });
    }
    private CompletableFuture<?> closeFailedUpdate(long id,Deadline deadline) {
        Entry entry; synchronized (this) { entry=entries.get(id); }
        if (entry==null) return CompletableFuture.completedFuture(null);
        if (entry.commit!=null && entry.outcome==null) return close(id,true,deadline).thenCompose(committed -> Boolean.FALSE.equals(committed) ? close(id,false,deadline).thenApply(ignored -> null) : CompletableFuture.completedFuture(null));
        return close(id,false,deadline).thenApply(ignored -> null);
    }
    private CompletableFuture<Void> consume(Entry entry,Deadline deadline) {
        if (entry.consumed) return CompletableFuture.completedFuture(null);
        return entry.commit.consume(deadline.remaining()).thenRun(() -> entry.consumed=true);
    }
    private void finish(Entry entry) {
        var drained=iterators.invalidateTransaction(entry.id,RequestContext.batch());
        if (!drained.isDone()) throw new CompletionException(new IOException("Transaction range retirement still active")); drained.join();
        try { entry.lease.close(); } catch (IOException failure) { throw new CompletionException(failure); }
        synchronized (this) { entries.remove(entry.id,entry); }
    }
    private static Object expose(Object value) {
        if (value instanceof Buf buf) return Buf.wrap(buf.toByteArray());
        if (value instanceof FirstAndLast<?> ends) return new FirstAndLast<>(expose(ends.first()),expose(ends.last()));
        if (value instanceof Delta<?> delta) return new Delta<>(expose(delta.previous()),expose(delta.current()));
        if (value instanceof UpdateContext<?> update) return new UpdateContext<>(expose(update.previous()),update.updateId());
        if (value instanceof KV row) return new KV(copy(row.keys()),row.value()==null ? null : Buf.wrap(row.value().toByteArray()));
        if (value instanceof RangePage<?> page) return new RangePage<>(page.items().stream().map(RatisRocksDBAdapter::expose).toList(),copy(page.resumeAfter()),page.hasMore());
        if (value instanceof List<?> list) { var owned=new ArrayList<>(); for (var item:list) owned.add(expose(item)); return Collections.unmodifiableList(owned); }
        return value;
    }
    private static Keys copy(Keys keys) { return keys==null ? null : new Keys(Arrays.stream(keys.keys()).map(value -> Buf.wrap(Objects.requireNonNull(value).toByteArray())).toArray(Buf[]::new)); }
    private static Throwable unwrap(Throwable failure) { while ((failure instanceof CompletionException || failure instanceof ExecutionException) && failure.getCause()!=null) failure=failure.getCause(); return failure; }
    private static RocksDBException unsupported() { return RocksDBException.of(NOT_IMPLEMENTED,"Distributed adapter operation is not implemented"); }
    @Override public void close() throws IOException {
        List<Entry> owners; synchronized (this) { closing=true; owners=List.copyOf(entries.values()); }
        var draining=iterators.closeAll(RequestContext.batch()); if (!draining.isDone()) throw new IOException("Range/iterator actual drain still active");
        try { draining.join(); } catch (CompletionException failure) { throw new IOException("Range/iterator cleanup failed",unwrap(failure)); }
        for (var entry:owners) { synchronized (entry) { if (entry.acquiring || entry.active!=null) throw new IOException("Adapter work active; ownership retained"); } orphanCleanup(entry); }
        synchronized (this) { if (!entries.isEmpty()) throw new IOException("Adapter cleanup pending; retry close after actual drain"); }
    }
}
