package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadCost;
import it.cavallium.rockserver.core.common.distributed.RangeDirectoryCodec;
import it.cavallium.rockserver.core.common.distributed.SessionEpochLedger;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.BooleanSupplier;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/** One queued/running background quantum through the database's existing bounded BATCH write lane. */
public final class RatisSessionClockService {
    public record Snapshot(RatisCatalogueClock.Tick clock,RatisSessionPropagation.Progress propagation) {}
    private final RWScheduler scheduler;
    private final RWScheduler.WorkloadExecutor executor;
    private final RatisCatalogueClock clock;
    private final RatisSessionPropagation propagation;
    private final LongSupplier timeMillis;
    private final BooleanSupplier eligible;
    private final int visits;
    private final long estimatedBytes;
    private Task active;
    private Disposable timer;
    private boolean started,stopping;
    private volatile Snapshot latest;
    private volatile Throwable lastFailure;

    /** All supplied resources remain caller-owned; stop and await its drain before closing journals/clients/scheduler. */
    public RatisSessionClockService(RWScheduler scheduler,RatisCatalogueClock clock,RatisSessionPropagation propagation,
            LongSupplier timeMillis,int visits) {
        this(scheduler,clock,propagation,timeMillis,visits,() -> true);
    }
    /** Eligibility must be a short memory-only ownership/readiness check; rechecked when queued work dispatches. */
    public RatisSessionClockService(RWScheduler scheduler,RatisCatalogueClock clock,RatisSessionPropagation propagation,
            LongSupplier timeMillis,int visits,BooleanSupplier eligible) {
        this.eligible=Objects.requireNonNull(eligible);
        this.scheduler=Objects.requireNonNull(scheduler);
        this.executor=scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION);
        this.clock=Objects.requireNonNull(clock); this.propagation=Objects.requireNonNull(propagation); this.timeMillis=Objects.requireNonNull(timeMillis);
        if (visits<1 || visits>32) throw new IllegalArgumentException("Invalid clock propagation quantum"); this.visits=visits;
        estimatedBytes=Math.min(WorkloadCost.MAX_ESTIMATED_BYTES,2L*SessionEpochLedger.MAX_BYTES+(long)visits*RangeDirectoryCodec.MAX_ENCODED_BYTES);
    }
    /** Timer callbacks only request admission; catalogue/native/file I/O runs on the shared workload executor. */
    public void start(Scheduler timerScheduler,Duration period) {
        Objects.requireNonNull(timerScheduler); long nanos=Objects.requireNonNull(period).toNanos();
        if (nanos<=0) throw new IllegalArgumentException("Clock period must be positive");
        synchronized (this) { if (started || stopping) throw new IllegalStateException("Clock service already started/stopped"); started=true; }
        Disposable scheduled;
        try { scheduled=timerScheduler.schedulePeriodically(() -> request(),0,nanos,TimeUnit.NANOSECONDS); }
        catch (RuntimeException failure) { synchronized (this) { started=false; } throw failure; }
        boolean dispose;
        synchronized (this) { dispose=stopping; if (!dispose) timer=scheduled; }
        if (dispose) scheduled.dispose();
    }
    /** Coalesces callers into one submission. Cancelling a returned view never releases actual work early. */
    public CompletableFuture<Snapshot> tick() {
        var task=request();
        return task==null ? CompletableFuture.failedFuture(new RejectedExecutionException("Clock service stopped or inactive")) : task.result.copy();
    }
    // Timer requests create no discarded dependent futures while an existing quantum is stalled.
    private Task request() {
        synchronized (this) { if (stopping) return null; if (active!=null) return active; }
        if (!eligible.getAsBoolean()) return null;
        final Task task;
        synchronized (this) {
            if (stopping) return null;
            if (active!=null) return active;
            active=task=new Task();
        }
        try { executor.execute(task,estimatedBytes); } catch (RuntimeException rejected) { task.reject(rejected); }
        boolean stopped; synchronized (this) { stopped=stopping; }
        if (stopped) scheduler.removeQueuedTask(executor,task); // stop may have raced the initial submission
        return task;
    }
    public Snapshot latest() { return latest; }
    public Throwable lastFailure() { return lastFailure; }
    /** Removes queued admission and drains running native/journal I/O. User completion callbacks may still be publishing. */
    public CompletableFuture<Void> stop() {
        final Disposable cancelTimer; final Task pending;
        synchronized (this) { stopping=true; cancelTimer=timer; timer=null; pending=active; }
        if (cancelTimer!=null) cancelTimer.dispose();
        if (pending!=null) scheduler.removeQueuedTask(executor,pending);
        return pending==null ? CompletableFuture.completedFuture(null) : pending.drained.copy();
    }
    private Snapshot cycle() throws Exception {
        var progress=propagation.progress(); RatisCatalogueClock.Tick time=null;
        if (progress.target()==null || progress.complete()) {
            time=clock.advance(timeMillis.getAsLong());
            if (time.outcome()==RatisCatalogueClock.Outcome.CONFLICT || time.outcome()==RatisCatalogueClock.Outcome.EXPIRED) return new Snapshot(time,progress);
            if (!propagation.covers(time.authority())) propagation.begin();
        }
        progress=propagation.step(visits); return new Snapshot(time,progress);
    }
    private void finish(Task task,Snapshot snapshot,Throwable failure) {
        synchronized (this) {
            if (active!=task) return;
            if (failure==null) { if (snapshot!=null) latest=snapshot; lastFailure=null; } else lastFailure=failure;
        }
        // All native/journal ownership has settled. Keep admission coalesced through synchronous publication callbacks.
        try {
            task.drained.complete(null);
            if (failure==null) task.result.complete(snapshot); else task.result.completeExceptionally(failure);
        } finally { synchronized (this) { if (active==task) active=null; } }
    }
    private final class Task implements Runnable,RWScheduler.RejectionAwareTask {
        private final CompletableFuture<Snapshot> result=new CompletableFuture<>();
        private final CompletableFuture<Void> drained=new CompletableFuture<>();
        private final AtomicInteger phase=new AtomicInteger(); // queued, running, terminal
        private final AtomicReference<RuntimeException> rejection=new AtomicReference<>();
        @Override public void run() {
            if (!phase.compareAndSet(0,1)) return;
            Snapshot snapshot=null; Throwable failure=null;
            try {
                boolean stopped; synchronized (RatisSessionClockService.this) { stopped=stopping; }
                if (stopped || !eligible.getAsBoolean()) throw new CancellationException("Clock quantum stopped or inactive before execution");
                snapshot=cycle();
            } catch (Throwable problem) { failure=problem; }
            finally {
                phase.set(2); if (failure==null) failure=rejection.get(); finish(this,snapshot,failure);
            }
            if (failure instanceof Error fatal) throw fatal;
            if (failure!=null) throw new CompletionException(failure);
        }
        @Override public void reject(RuntimeException failure) {
            if (phase.compareAndSet(0,2)) finish(this,null,failure);
            else if (phase.get()==1) rejection.compareAndSet(null,failure); // running work owns its eventual drain
        }
    }
}
