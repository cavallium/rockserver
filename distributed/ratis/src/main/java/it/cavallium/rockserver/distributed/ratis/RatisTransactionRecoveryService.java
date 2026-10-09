package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadCost;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/** Node-shared bounded recovery quanta through the existing reserved CONTROL lane. Supplied resources remain caller-owned. */
public final class RatisTransactionRecoveryService {
    public static final int MAX_RECOVERIES=4096,MAX_WAITERS=16;
    @FunctionalInterface public interface Quantum { void run() throws Exception; }
    private final Quantum quantum;
    private final RWScheduler scheduler;
    private final RWScheduler.WorkloadExecutor executor;
    private final int visits;
    private final java.util.ArrayDeque<RatisTransactionRecovery> pending=new java.util.ArrayDeque<>();
    private int remaining;
    private Task active;
    private Disposable timer;
    private boolean started,stopping;
    private volatile Throwable lastFailure;

    public RatisTransactionRecoveryService(RWScheduler scheduler,List<RatisTransactionRecovery> recoveries,int visits) {
        this.scheduler=Objects.requireNonNull(scheduler);
        this.executor=scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT);
        Objects.requireNonNull(recoveries);
        if (recoveries.size()>MAX_RECOVERIES) throw new IllegalArgumentException("Too many transaction recoveries");
        var supplied=List.copyOf(recoveries); pending.addAll(supplied); remaining=supplied.size();
        if (visits<1 || visits>32) throw new IllegalArgumentException("Invalid recovery quantum");
        this.visits=visits; quantum=this::cycle;
    }

    /** Dynamic discovery/recovery uses the same coalesced CONTROL admission and actual-task drain ownership. */
    public RatisTransactionRecoveryService(RWScheduler scheduler,Quantum quantum) {
        this.scheduler=Objects.requireNonNull(scheduler);
        executor=scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT);
        this.quantum=Objects.requireNonNull(quantum); visits=0;
    }

    /** Timer callbacks only admit work; recovery runs on the shared workload executor. */
    public void start(Scheduler timerScheduler,Duration period) {
        Objects.requireNonNull(timerScheduler); long nanos=Objects.requireNonNull(period).toNanos();
        if (nanos<=0) throw new IllegalArgumentException("Recovery period must be positive");
        synchronized (this) { if (started || stopping) throw new IllegalStateException("Recovery service already started/stopped"); started=true; }
        final Disposable scheduled;
        try { scheduled=timerScheduler.schedulePeriodically(() -> request(),0,nanos,TimeUnit.NANOSECONDS); }
        catch (RuntimeException failure) { synchronized (this) { started=false; } throw failure; }
        boolean dispose;
        synchronized (this) { dispose=stopping; if (!dispose) timer=scheduled; }
        if (dispose) scheduled.dispose();
    }

    /** Returned views can be cancelled without releasing the actual quantum's ownership. */
    public CompletableFuture<Void> tick() {
        var task=request();
        if (task==null) return CompletableFuture.failedFuture(new RejectedExecutionException("Recovery service stopped"));
        synchronized (this) {
            if (!task.result.isDone()) {
                if (task.resultViews>=MAX_WAITERS) return CompletableFuture.failedFuture(new RejectedExecutionException("Recovery tick waiter capacity exhausted"));
                task.resultViews++;
            }
            return task.result.copy();
        }
    }

    // Periodic requests allocate no discarded dependent futures while a quantum is already queued/running.
    private Task request() {
        final Task task;
        synchronized (this) {
            if (stopping) return null;
            if (active!=null) return active;
            active=task=new Task();
        }
        try {
            // Each step can encode a bounded durable coordinator file. Charge the capped maximum.
            executor.execute(task,WorkloadCost.MAX_ESTIMATED_BYTES);
        } catch (RuntimeException rejected) { task.reject(rejected); }
        boolean stopped; synchronized (this) { stopped=stopping; }
        if (stopped) scheduler.removeQueuedTask(executor,task);
        return task;
    }

    public Throwable lastFailure() { return lastFailure; }

    /** Remove queued admission and await running recovery before closing borrowed recoveries, journals or clients. */
    public CompletableFuture<Void> stop() {
        final Disposable cancelTimer; final Task pending; final CompletableFuture<Void> view;
        synchronized (this) {
            stopping=true; cancelTimer=timer; timer=null; pending=active;
            if (pending==null) view=CompletableFuture.completedFuture(null);
            else if (!pending.drained.isDone() && pending.drainViews>=MAX_WAITERS) {
                view=CompletableFuture.failedFuture(new RejectedExecutionException("Recovery drain waiter capacity exhausted"));
            } else {
                if (!pending.drained.isDone()) pending.drainViews++;
                view=pending.drained.copy();
            }
        }
        if (cancelTimer!=null) cancelTimer.dispose();
        if (pending!=null) scheduler.removeQueuedTask(executor,pending);
        return view;
    }

    public synchronized int remaining() { if (visits==0) throw new IllegalStateException("Dynamic recovery status belongs to its registry"); return remaining; }
    private void cycle() throws Exception {
        final int count; synchronized (this) { count=Math.min(visits,pending.size()); }
        for (int i=0;i<count;i++) {
            final RatisTransactionRecovery recovery;
            synchronized (this) { if (stopping) return; recovery=pending.pollFirst(); }
            if (recovery==null) return;
            boolean complete=false;
            try { complete=recovery.step().complete(); }
            finally { synchronized (this) { if (complete) remaining--; else pending.addLast(recovery); } }
        }
    }

    private void finish(Task task,Throwable failure) {
        synchronized (this) {
            if (active!=task) return;
            lastFailure=failure;
        }
        // Recovery authority and journal ownership have settled; callbacks run outside our monitor while admission stays coalesced.
        try {
            task.drained.complete(null);
            if (failure==null) task.result.complete(null); else task.result.completeExceptionally(failure);
        } finally { synchronized (this) { if (active==task) active=null; } }
    }

    private final class Task implements Runnable,RWScheduler.RejectionAwareTask {
        private final CompletableFuture<Void> result=new CompletableFuture<>();
        private final CompletableFuture<Void> drained=new CompletableFuture<>();
        private int resultViews,drainViews; // public cancellation never refunds views before actual completion
        private final AtomicInteger phase=new AtomicInteger(); // queued, running, terminal
        private final AtomicReference<RuntimeException> rejection=new AtomicReference<>();
        @Override public void run() {
            if (!phase.compareAndSet(0,1)) return;
            Throwable failure=null;
            try {
                boolean stopped; synchronized (RatisTransactionRecoveryService.this) { stopped=stopping; }
                if (stopped) throw new CancellationException("Recovery stopped before execution");
                quantum.run();
            } catch (Throwable problem) { failure=problem; }
            finally {
                phase.set(2); if (failure==null) failure=rejection.get(); finish(this,failure);
            }
            if (failure instanceof Error fatal) throw fatal;
            if (failure!=null) throw new CompletionException(failure);
        }
        @Override public void reject(RuntimeException failure) {
            if (phase.compareAndSet(0,2)) finish(this,failure);
            else if (phase.get()==1) rejection.compareAndSet(null,failure);
        }
    }
}
