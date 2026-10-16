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
            var owned=openOwned(column,lower,upper,reverse,ttl,context); var view=owned.view();
            view.whenComplete((ignored,failure) -> { if (view.isCancelled()) owned.cleanup(); }); return view;
        } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    OwnedRead openOwned(long column,Keys lower,Keys upper,boolean reverse,Duration ttl,RequestContext context) throws Exception {
        var deadline=new Deadline(context); var binding=RatisLogicalClient.supported(schema,column); lower=bound(lower); upper=bound(upper);
        byte[] low=RatisLogicalRangePages.bound(schema,binding.columnId(),lower),high=RatisLogicalRangePages.upperBound(schema,binding.columnId(),upper);
        var scope=new TransactionScope(List.of(TransactionScope.Span.range(binding.columnId(),low,high))); scope.participants(directory,SnapshotFence.MAX_PARTICIPANTS);
        RatisLogicalRangePages.validateBounds(scope,schema,column,lower,upper);
        if (ttl.isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofSeconds(60))>0 || !ttl.equals(Duration.ofMillis(ttl.toMillis()))) throw new IllegalArgumentException("Range TTL must be whole milliseconds up to60seconds");
        var owned=new OwnedRead(column,lower,upper,reverse);
        synchronized (this) { if (closing || owners.size()>=MAX_OWNERS) throw new IOException("Readonly range ownership exhausted"); owners.add(owned); }
        CompletableFuture<RatisRocksDBAdapter.Allocation> allocating;
        try { allocating=Objects.requireNonNull(allocations.allocate(deadline.remaining(),snapshots.bindings().targets(scope))); }
        catch (Throwable failure) { owned.acquisition.completeExceptionally(failure); owned.remove(); return owned; }
        allocating.thenCompose(allocation -> {
            try { owned.lease=snapshots.open(allocation.snapshot(),allocation.identity().snapshot(),scope,ttl,deadline.remaining()); return owned.lease.completion(); }
            catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }).whenComplete((ignored,failure) -> {
            if (failure==null) try { deadline.remaining(); owned.acquisition.complete(owned); } catch (Throwable expired) { failure=expired; }
            if (failure!=null) { owned.acquisition.completeExceptionally(failure); owned.cleanup(); }
        });
        return owned;
    }
    record OwnedResult<T>(CompletableFuture<T> observer,CompletableFuture<Void> drained,Runnable cancelObserver) {}
    /** One independently consistent page; failed acquisition retains its hidden owner until cleanup. */
    public CompletableFuture<RangePage<KV>> page(long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget,RequestContext context) {
        return pageOwned(column,lower,upper,reverse,resume,budget,context).observer();
    }
    OwnedResult<RangePage<KV>> pageOwned(long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget,RequestContext context) {
        var deadline=new Deadline(context);
        return readOwned(column,lower,upper,reverse,deadline,owner -> {
            try { return owner.page(resume,budget,deadline.remaining()); }
            catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
        });
    }
    public CompletableFuture<FirstAndLast<KV>> firstAndLast(long column,Keys lower,Keys upper,boolean reverse,RequestContext context) {
        return firstAndLastOwned(column,lower,upper,reverse,context).observer();
    }
    OwnedResult<FirstAndLast<KV>> firstAndLastOwned(long column,Keys lower,Keys upper,boolean reverse,RequestContext context) {
        var deadline=new Deadline(context);
        return readOwned(column,lower,upper,reverse,deadline,owner -> {
            try {
                var budget=new RangeBudget(1,RangeBudget.DEFAULT_MAX_BYTES);
                return owner.page(null,budget,deadline.remaining()).thenCompose(first -> first.items().isEmpty()
                        ? CompletableFuture.completedFuture(new FirstAndLast<KV>(null,null))
                        : owner.page(owner.lower,owner.upper,!reverse,null,budget,deadline.remaining()).thenApply(last -> new FirstAndLast<>(first.items().getFirst(),last.items().getFirst())));
            } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
        });
    }
    private <T> OwnedResult<T> readOwned(long column,Keys lower,Keys upper,boolean reverse,Deadline deadline,java.util.function.Function<OwnedRead,CompletableFuture<T>> reading) {
        try {
            var owner=openOwned(column,lower,upper,reverse,Duration.ofSeconds(60),deadline.remaining());
            var actual=owner.view().thenCompose(ignored -> completeAndRelease(owner,reading.apply(owner))); var observer=actual.copy();
            Runnable cancel=() -> { observer.cancel(false); owner.cleanup(); };
            observer.whenComplete((value,failure) -> { if (observer.isCancelled()) owner.cleanup(); });
            return new OwnedResult<>(observer,owner.drained,cancel);
        } catch (Throwable failure) { return new OwnedResult<>(CompletableFuture.failedFuture(failure),CompletableFuture.completedFuture(null),() -> {}); }
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
        private int views,readers,releaseObservers;
        private CompletableFuture<Void> releaseAttempt;
        private boolean removed,releaseRequested,released,localClosing;
        OwnedRead(long column,Keys lower,Keys upper,boolean reverse) { this.column=column; this.lower=lower; this.upper=upper; this.reverse=reverse; }
        final CompletableFuture<Void> drained=new CompletableFuture<>();
        synchronized CompletableFuture<OwnedRead> view() { if (!acquisition.isDone() && views>=MAX_WAITERS) return CompletableFuture.failedFuture(new IOException("Range observer capacity exhausted")); if (!acquisition.isDone()) views++; return acquisition.copy(); }
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
            final CompletableFuture<Void> attempt,idleAtRelease; final boolean start;
            synchronized (this) {
                if (removed) return CompletableFuture.completedFuture(null);
                if (releaseObservers>=MAX_WAITERS) return CompletableFuture.failedFuture(new IOException("Readonly range release observer capacity exhausted"));
                releaseRequested=true; idleAtRelease=idle; start=releaseAttempt==null;
                if (start) releaseAttempt=new CompletableFuture<>(); attempt=releaseAttempt; releaseObservers++;
            }
            if (start) {
                acquisition.handle((value,failure) -> value).thenCompose(ignored -> idleAtRelease)
                        .thenCompose(ignored -> lease==null ? CompletableFuture.completedFuture(null) : lease.release(context))
                        .whenComplete((ignored,failure) -> {
                            synchronized (this) {
                                if (failure==null) released=true;
                                else if (releaseAttempt==attempt) releaseAttempt=null;
                            }
                            if (failure==null) attempt.complete(null); else attempt.completeExceptionally(failure);
                        });
            }
            var view=attempt.copy();
            attempt.whenComplete((ignored,failure) -> { synchronized (this) { releaseObservers--; } });
            return view;
        }
        void cleanup() {
            CompletableFuture<Void> attempt;
            synchronized (this) { if (removed || cleanup!=null) return; cleanup=attempt=new CompletableFuture<>(); }
            release(RequestContext.batch()).whenComplete((ignored,failure) -> {
                if (failure==null) try { closeLocal(); } catch (Throwable close) { failure=close; }
                synchronized (this) { if (failure!=null && cleanup==attempt) cleanup=null; }
                if (failure==null) attempt.complete(null); else attempt.completeExceptionally(failure);
            });
        }
        public void closeLocal() throws IOException {
            synchronized (this) {
                if (removed) return;
                if (!acquisition.isDone() || readers!=0 || !released || localClosing) throw new IOException("Readonly range work, release or local cleanup unresolved; ownership retained");
                releaseRequested=true; localClosing=true;
            }
            try { if (lease!=null) lease.close(); remove(); }
            finally { synchronized (this) { localClosing=false; } }
        }
        private void remove() { synchronized (this) { removed=true; } synchronized (RatisRocksDBRanges.this) { owners.remove(this); } drained.complete(null); }
    }

    private static Keys bound(Keys keys) { return keys==null || keys.keys().length==0 ? null : RatisLogicalRangePages.copy(keys); }
    private static final class Deadline {
        final RequestContext context; final long start=System.nanoTime();
        Deadline(RequestContext context) { this.context=Objects.requireNonNull(context); }
        RequestContext remaining() { if (!context.hasTimeout()) return context; long remaining=context.timeoutNanos()-(System.nanoTime()-start); if (remaining<=0) throw new CompletionException(new TimeoutException("Range request deadline expired")); return new RequestContext(context.profile(),remaining); }
    }
    CompletableFuture<Void> retryRetiredReadsCleanup() {
        List<OwnedRead> retained; synchronized (this) { retained=List.copyOf(owners); }
        var drains=new ArrayList<CompletableFuture<Void>>();
        for (var owner:retained) {
            boolean retired; synchronized (owner) { retired=!owner.removed && (owner.releaseRequested || owner.acquisition.isCompletedExceptionally()); }
            if (retired) { owner.cleanup(); drains.add(owner.drained); }
        }
        return CompletableFuture.allOf(drains.toArray(CompletableFuture[]::new));
    }
    @Override public void close() throws IOException {
        List<OwnedRead> retained; synchronized (this) { closing=true; retained=List.copyOf(owners); }
        for (var owner:retained) owner.cleanup();
        synchronized (this) { if (!owners.isEmpty()) throw new IOException("Readonly range cleanup active/unknown; ownership retained"); }
    }
}
