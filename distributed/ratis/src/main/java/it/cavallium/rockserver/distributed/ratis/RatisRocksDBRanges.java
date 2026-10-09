package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Scoped readonly range ownership. Borrowed clients are never closed by this helper. */
public final class RatisRocksDBRanges implements AutoCloseable {
    public static final int MAX_OWNERS=16,MAX_WAITERS=16;
    private final DistributedSchema schema;
    private final RangeDirectory directory;
    private final RatisSnapshotClient snapshots;
    private final RatisRocksDBAdapter.AllocationSource allocations;
    private final Set<OwnedRead> owners=new HashSet<>();
    private boolean closing;
    public RatisRocksDBRanges(DistributedSchema schema,RangeDirectory directory,RatisSnapshotClient snapshots,RatisRocksDBAdapter.AllocationSource allocations) {
        this.schema=Objects.requireNonNull(schema); this.directory=Objects.requireNonNull(directory); this.snapshots=Objects.requireNonNull(snapshots); this.allocations=Objects.requireNonNull(allocations); schema.requireMatches(directory);
    }
    public CompletableFuture<OwnedRead> open(long column,Keys lower,Keys upper,boolean reverse,Duration ttl,RequestContext context) {
        try {
            var deadline=new Deadline(context); var binding=RatisLogicalClient.supported(schema,column); lower=bound(lower); upper=bound(upper);
            byte[] low=RatisLogicalRangePages.bound(schema,binding.columnId(),lower),high=RatisLogicalRangePages.upperBound(schema,binding.columnId(),upper);
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(binding.columnId(),low,high))); scope.participants(directory,SnapshotFence.MAX_PARTICIPANTS);
            RatisLogicalRangePages.validateBounds(scope,schema,column,lower,upper);
            if (ttl.isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofSeconds(60))>0 || !ttl.equals(Duration.ofMillis(ttl.toMillis()))) throw new IllegalArgumentException("Range TTL must be whole milliseconds up to60seconds");
            var owned=new OwnedRead(column,lower,upper,reverse);
            synchronized (this) { if (closing || owners.size()>=MAX_OWNERS) throw new IOException("Readonly range ownership exhausted"); owners.add(owned); }
            CompletableFuture<RatisRocksDBAdapter.Allocation> allocating;
            try { allocating=Objects.requireNonNull(allocations.allocate(deadline.remaining())); }
            catch (Throwable failure) { owned.acquisition.completeExceptionally(failure); owned.remove(); return CompletableFuture.failedFuture(failure); }
            allocating.thenCompose(allocation -> {
                try { owned.lease=snapshots.open(allocation.snapshot(),allocation.identity().snapshot(),scope,ttl,deadline.remaining()); return owned.lease.completion(); }
                catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
            }).whenComplete((ignored,failure) -> {
                if (failure==null) try { deadline.remaining(); owned.acquisition.complete(owned); } catch (Throwable expired) { failure=expired; }
                if (failure!=null) { owned.acquisition.completeExceptionally(failure); owned.cleanup(); }
            });
            var view=owned.view(); view.whenComplete((ignored,failure) -> { if (view.isCancelled()) owned.cleanup(); }); return view;
        } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    /** One independently consistent page, with cleanup retained through actual completion. */
    public CompletableFuture<RangePage<KV>> page(long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget,RequestContext context) {
        var deadline=new Deadline(context);
        CompletableFuture<RangePage<KV>> actual=open(column,lower,upper,reverse,Duration.ofSeconds(60),deadline.remaining()).thenCompose(owner -> {
            CompletableFuture<RangePage<KV>> reading;
            try { reading=owner.page(resume,budget,deadline.remaining()); }
            catch (Throwable failure) { reading=CompletableFuture.failedFuture(failure); }
            return completeAndRelease(owner,reading);
        });
        return actual.copy();
    }
    public CompletableFuture<FirstAndLast<KV>> firstAndLast(long column,Keys lower,Keys upper,boolean reverse,RequestContext context) {
        var deadline=new Deadline(context);
        var actual=open(column,lower,upper,reverse,Duration.ofSeconds(60),deadline.remaining()).thenCompose(owner -> {
            CompletableFuture<FirstAndLast<KV>> reading;
            try {
                var budget=new RangeBudget(1,RangeBudget.DEFAULT_MAX_BYTES);
                reading=owner.page(null,budget,deadline.remaining()).thenCompose(first -> first.items().isEmpty()
                        ? CompletableFuture.completedFuture(new FirstAndLast<KV>(null,null))
                        : owner.page(owner.lower,owner.upper,!reverse,null,budget,deadline.remaining())
                            .thenApply(last -> new FirstAndLast<>(first.items().getFirst(),last.items().getFirst())));
            } catch (Throwable failure) { reading=CompletableFuture.failedFuture(failure); }
            return completeAndRelease(owner,reading);
        });
        return actual.copy();
    }
    private <T> CompletableFuture<T> completeAndRelease(OwnedRead owner,CompletableFuture<T> reading) {
        var result=new CompletableFuture<T>();
        reading.whenComplete((value,failure) -> owner.release(RequestContext.batch()).whenComplete((ignored,cleanup) -> {
            try { if (cleanup==null) owner.closeLocal(); } catch (Throwable close) { cleanup=close; }
            if (failure!=null) { if (cleanup!=null) failure.addSuppressed(cleanup); result.completeExceptionally(failure); }
            else if (cleanup!=null) result.completeExceptionally(cleanup); else result.complete(value);
        }));
        return result;
    }
    public synchronized int stats() { return owners.size(); }
    public final class OwnedRead {
        private final long column; private final Keys lower,upper; private final boolean reverse;
        private final CompletableFuture<OwnedRead> acquisition=new CompletableFuture<>();
        private volatile RatisSnapshotClient.Lease lease;
        private CompletableFuture<Void> cleanup;
        private CompletableFuture<Void> idle=CompletableFuture.completedFuture(null);
        private int views,readers;
        private boolean removed,releaseRequested,released;
        OwnedRead(long column,Keys lower,Keys upper,boolean reverse) { this.column=column; this.lower=lower; this.upper=upper; this.reverse=reverse; }
        private synchronized CompletableFuture<OwnedRead> view() { if (!acquisition.isDone() && views>=MAX_WAITERS) return CompletableFuture.failedFuture(new IOException("Range observer capacity exhausted")); if (!acquisition.isDone()) views++; return acquisition.copy(); }
        public CompletableFuture<RangePage<KV>> page(Keys resume,RangeBudget budget,RequestContext context) {
            return page(lower,upper,reverse,resume,budget,context);
        }
        CompletableFuture<RangePage<KV>> page(Keys low,Keys high,boolean back,Keys resume,RangeBudget budget,RequestContext context) {
            CompletableFuture<RangePage<KV>> actual;
            try {
                low=bound(low); high=bound(high); resume=bound(resume);
                RatisLogicalRangePages.validateContainedBounds(schema,column,lower,upper,low,high);
                synchronized (this) {
                    if (!acquisition.isDone() || acquisition.isCompletedExceptionally() || lease==null || removed || releaseRequested)
                        throw new IOException("Readonly range unavailable or closing");
                    if (readers>=MAX_WAITERS) throw new IOException("Readonly range reader capacity exhausted");
                    if (readers++==0) idle=new CompletableFuture<>();
                }
            } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
            try { actual=lease.page(column,low,high,back,resume,budget,context); }
            catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            var result=new CompletableFuture<RangePage<KV>>();
            actual.whenComplete((value,failure) -> {
                CompletableFuture<Void> drained=null;
                synchronized (this) { if (--readers==0) drained=idle; }
                if (drained!=null) drained.complete(null);
                if (failure!=null) result.completeExceptionally(failure); else result.complete(value);
            });
            return result.copy();
        }
        public CompletableFuture<Void> release(RequestContext context) {
            CompletableFuture<Void> drained;
            synchronized (this) { if (removed) return CompletableFuture.completedFuture(null); releaseRequested=true; drained=idle; }
            return acquisition.handle((value,failure) -> value).thenCompose(ignored -> drained)
                    .thenCompose(ignored -> lease==null ? CompletableFuture.completedFuture(null) : lease.release(context))
                    .thenRun(() -> { synchronized (this) { released=true; } });
        }
        private void cleanup() {
            CompletableFuture<Void> attempt;
            synchronized (this) { if (removed || cleanup!=null) return; cleanup=attempt=new CompletableFuture<>(); }
            release(RequestContext.batch()).whenComplete((ignored,failure) -> {
                if (failure==null) try { closeLocal(); } catch (Throwable close) { failure=close; }
                synchronized (this) { if (failure!=null && cleanup==attempt) cleanup=null; }
                if (failure==null) attempt.complete(null); else attempt.completeExceptionally(failure);
            });
        }
        public void closeLocal() throws IOException {
            synchronized (this) { if (removed) return; if (!acquisition.isDone() || readers!=0 || !released) throw new IOException("Readonly range work or release unresolved; ownership retained"); releaseRequested=true; }
            if (lease!=null) lease.close(); remove();
        }
        private void remove() { synchronized (this) { removed=true; } synchronized (RatisRocksDBRanges.this) { owners.remove(this); } }
    }

    private static Keys bound(Keys keys) { return keys==null || keys.keys().length==0 ? null : RatisLogicalRangePages.copy(keys); }
    private static final class Deadline {
        final RequestContext context; final long start=System.nanoTime();
        Deadline(RequestContext context) { this.context=Objects.requireNonNull(context); }
        RequestContext remaining() { if (!context.hasTimeout()) return context; long remaining=context.timeoutNanos()-(System.nanoTime()-start); if (remaining<=0) throw new CompletionException(new TimeoutException("Range request deadline expired")); return new RequestContext(context.profile(),remaining); }
    }
    @Override public void close() throws IOException {
        List<OwnedRead> retained; synchronized (this) { closing=true; retained=List.copyOf(owners); }
        for (var owner:retained) owner.cleanup();
        synchronized (this) { if (!owners.isEmpty()) throw new IOException("Readonly range cleanup active/unknown; ownership retained"); }
    }
}
