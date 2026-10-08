package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Node-bounded HELP ownership on reserved CONTROL workers, outside Ratis read-barrier completion. */
public final class RatisSnapshotHelpService implements RatisRangeStateMachine.SnapshotHelpExecutor {
    public static final int MAX_JOBS=16,MAX_WAITERS=16;
    private record Key(UUID group,ScopedTransactionRpc.SnapshotBinding binding) {}
    private final RWScheduler scheduler;
    private final RWScheduler.WorkloadExecutor executor;
    private final Map<Key,Task> tasks=new HashMap<>();
    private boolean stopping;
    private CompletableFuture<Void> actualStop;
    private int drainViews;
    private boolean sweeping;
    private final List<Publication> deferred=new ArrayList<>();
    private record Publication(Task task,boolean progress,Throwable failure) {}
    public RatisSnapshotHelpService(RWScheduler scheduler) {
        this.scheduler=Objects.requireNonNull(scheduler);
        executor=scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT);
    }
    @Override public CompletableFuture<Boolean> submit(UUID group,ScopedTransactionRpc.SnapshotBinding binding,RatisRangeStateMachine.SnapshotHelpAction action) {
        Objects.requireNonNull(group); Objects.requireNonNull(binding); Objects.requireNonNull(action);
        var key=new Key(group,binding); final Task task;
        synchronized (this) {
            if (stopping) return CompletableFuture.failedFuture(new RejectedExecutionException("Snapshot HELP is stopping"));
            var existing=tasks.get(key);
            if (existing!=null) {
                if (existing.waiters>=MAX_WAITERS) return CompletableFuture.failedFuture(new RejectedExecutionException("Snapshot HELP waiter capacity exhausted"));
                existing.waiters++; return existing.result.copy();
            }
            if (tasks.size()>=MAX_JOBS) return CompletableFuture.failedFuture(new RejectedExecutionException("Snapshot HELP capacity exhausted"));
            task=new Task(key,action); tasks.put(key,task);
        }
        try { executor.execute(task,WorkloadCost.MAX_ESTIMATED_BYTES); }
        catch (RuntimeException rejected) { task.reject(rejected); }
        boolean stopped; synchronized (this) { stopped=stopping; }
        if (stopped) scheduler.removeQueuedTask(executor,task);
        return task.result.copy();
    }
    /** Caller cancellation only cancels this view. Actual queued/running ownership remains until rejection/return. */
    public CompletableFuture<Void> stop() {
        final List<Task> pending; final CompletableFuture<Void> view;
        synchronized (this) {
            stopping=true;
            if (actualStop==null) { actualStop=new CompletableFuture<>(); pending=List.copyOf(tasks.values()); sweeping=true; }
            else pending=null;
            if (!actualStop.isDone() && drainViews>=MAX_WAITERS) view=CompletableFuture.failedFuture(new RejectedExecutionException("Snapshot HELP drain waiter capacity exhausted"));
            else { if (!actualStop.isDone()) drainViews++; view=actualStop.copy(); }
        }
        if (pending!=null) {
            CompletableFuture.allOf(pending.stream().map(task -> task.drained).toArray(CompletableFuture[]::new)).whenComplete((ignored,failure) -> actualStop.complete(null));
            for (var task:pending) scheduler.removeQueuedTask(executor,task);
            final List<Publication> publications;
            synchronized (this) { sweeping=false; publications=List.copyOf(deferred); deferred.clear(); }
            for (var publication:publications) publish(publication);
        }
        return view;
    }
    public synchronized int activeJobs() { return tasks.size(); }
    private void finish(Task task,boolean progress,Throwable failure) {
        var publication=new Publication(task,progress,failure); final boolean defer;
        synchronized (this) { if (!tasks.remove(task.key,task)) return; defer=sweeping; if (defer) deferred.add(publication); }
        task.drained.complete(null);
        if (!defer) publish(publication);
    }
    private static void publish(Publication publication) {
        if (publication.failure()==null) publication.task().result.complete(publication.progress());
        else publication.task().result.completeExceptionally(publication.failure());
    }
    private final class Task implements Runnable,RWScheduler.RejectionAwareTask {
        final Key key;
        RatisRangeStateMachine.SnapshotHelpAction action;
        int waiters=1;
        final CompletableFuture<Boolean> result=new CompletableFuture<>();
        final CompletableFuture<Void> drained=new CompletableFuture<>();
        final AtomicInteger phase=new AtomicInteger();
        final AtomicReference<RuntimeException> rejection=new AtomicReference<>();
        Task(Key key,RatisRangeStateMachine.SnapshotHelpAction action) { this.key=key; this.action=action; }
        @Override public void run() {
            if (!phase.compareAndSet(0,1)) return;
            boolean progress=false; Throwable failure=null;
            try {
                boolean stopped; synchronized (RatisSnapshotHelpService.this) { stopped=stopping; }
                if (stopped) throw new CancellationException("Snapshot HELP stopped before execution");
                progress=action.run();
            } catch (Throwable problem) { failure=problem; }
            finally { phase.set(2); if (failure==null) failure=rejection.get(); action=null; finish(this,progress,failure); }
            if (failure instanceof Error fatal) throw fatal;
            if (failure!=null) throw new CompletionException(failure);
        }
        @Override public void reject(RuntimeException failure) {
            if (phase.compareAndSet(0,2)) { action=null; finish(this,false,failure); }
            else if (phase.get()==1) rejection.compareAndSet(null,failure);
        }
    }
}
