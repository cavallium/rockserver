package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadCost;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.distributed.RangeDirectoryCodec;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/** Node-shared bounded expiry quanta through the existing reserved CONTROL lane. Supplied resources remain caller-owned. */
public final class RatisTransactionExpiryService {
    private final RWScheduler scheduler;
    private final RWScheduler.WorkloadExecutor executor;
    private final List<RatisRangeStateMachine> machines;
    private final int visits;
    private int cursor;
    private Task active;
    private Disposable timer;
    private boolean started,stopping;
    private volatile Throwable lastFailure;

    public RatisTransactionExpiryService(RWScheduler scheduler,List<RatisRangeStateMachine> machines,int visits) {
        this.scheduler=Objects.requireNonNull(scheduler);
        this.executor=scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT);
        Objects.requireNonNull(machines);
        if (machines.size()>RangeDirectoryCodec.MAX_RANGES+1) throw new IllegalArgumentException("Too many expiry groups");
        this.machines=List.copyOf(machines);
        if (visits<1 || visits>32) throw new IllegalArgumentException("Invalid expiry quantum");
        this.visits=visits;
    }

    /** Timer callbacks only admit work; snapshot release runs on the shared workload executor. */
    public void start(Scheduler timerScheduler,Duration period) {
        Objects.requireNonNull(timerScheduler); long nanos=Objects.requireNonNull(period).toNanos();
        if (nanos<=0) throw new IllegalArgumentException("Expiry period must be positive");
        synchronized (this) { if (started || stopping) throw new IllegalStateException("Expiry service already started/stopped"); started=true; }
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
        return task==null ? CompletableFuture.failedFuture(new RejectedExecutionException("Expiry service stopped")) : task.result.copy();
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
            // Each registry permits up to 4096 handles and 256 MiB of scope metadata.
            // Conservatively charge the scheduler's capped maximum, independent of retained row data.
            executor.execute(task,WorkloadCost.MAX_ESTIMATED_BYTES);
        } catch (RuntimeException rejected) { task.reject(rejected); }
        boolean stopped; synchronized (this) { stopped=stopping; }
        if (stopped) scheduler.removeQueuedTask(executor,task);
        return task;
    }

    public Throwable lastFailure() { return lastFailure; }

    /** Remove queued admission and await running snapshot cleanup before closing machines or storage pools. */
    public CompletableFuture<Void> stop() {
        final Disposable cancelTimer; final Task pending;
        synchronized (this) { stopping=true; cancelTimer=timer; timer=null; pending=active; }
        if (cancelTimer!=null) cancelTimer.dispose();
        if (pending!=null) scheduler.removeQueuedTask(executor,pending);
        return pending==null ? CompletableFuture.completedFuture(null) : pending.drained.copy();
    }

    private void cycle() throws Exception {
        for (int i=0,n=Math.min(visits,machines.size());i<n;i++) {
            var machine=machines.get(cursor);
            cursor=(cursor+1)%machines.size(); // a failing group cannot starve the following groups
            machine.expireTransactions();
        }
    }

    private void finish(Task task,Throwable failure) {
        synchronized (this) {
            if (active!=task) return;
            lastFailure=failure;
        }
        // Native ownership has settled; callbacks run outside our monitor while admission stays coalesced.
        try {
            task.drained.complete(null);
            if (failure==null) task.result.complete(null); else task.result.completeExceptionally(failure);
        } finally { synchronized (this) { if (active==task) active=null; } }
    }

    private final class Task implements Runnable,RWScheduler.RejectionAwareTask {
        private final CompletableFuture<Void> result=new CompletableFuture<>();
        private final CompletableFuture<Void> drained=new CompletableFuture<>();
        private final AtomicInteger phase=new AtomicInteger(); // queued, running, terminal
        private final AtomicReference<RuntimeException> rejection=new AtomicReference<>();
        @Override public void run() {
            if (!phase.compareAndSet(0,1)) return;
            Throwable failure=null;
            try {
                boolean stopped; synchronized (RatisTransactionExpiryService.this) { stopped=stopping; }
                if (stopped) throw new CancellationException("Expiry stopped before execution");
                cycle();
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
