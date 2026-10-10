package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.cdc.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import static it.cavallium.rockserver.core.common.cdc.DistributedCdcSubscriptions.*;

/** Bounded blocking CDC lane. Cancelling an observer never releases actual controller I/O. */
public final class RatisCdcBridge implements DistributedCdcAsyncAPI,AutoCloseable {
    public static final int MAX_PENDING=8;
    @FunctionalInterface public interface DirectoryReader { RangeDirectory read(UUID domain) throws IOException; }
    @FunctionalInterface private interface Work<T> { T run() throws Exception; }
    private final RatisCdcSubscriptionController controller;
    private final RWScheduler clock;
    private final DirectoryReader directories;
    private final ExecutorService lane;
    private int actual;
    private boolean stopping,closed;
    /** The bridge owns the controller; the controller's transports and this scheduler remain borrowed. */
    public RatisCdcBridge(RatisCdcSubscriptionController controller,RWScheduler clock,DirectoryReader directories) {
        this.controller=Objects.requireNonNull(controller); this.clock=Objects.requireNonNull(clock); this.directories=Objects.requireNonNull(directories);
        lane=Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon(true).name("ratis-cdc-consumer").factory());
    }
    @Override public CompletableFuture<RangeDirectory> context(UUID domainId,RequestContext context) {
        return submit(context,OperationFamily.METADATA,()->directory(Objects.requireNonNull(domainId)));
    }
    @Override public CompletableFuture<Status> create(DistributedCdcSubscription.Definition definition,RequestContext context) {
        Objects.requireNonNull(definition);
        return submit(context,OperationFamily.MUTATION,()->status(controller.create(definition,directory(definition.domainId()))));
    }
    @Override public CompletableFuture<Status> recover(UUID id,RequestContext context) { return status(id,context); }
    @Override public CompletableFuture<Status> status(UUID id,RequestContext context) {
        return submit(context,OperationFamily.METADATA,()->status(controller.reopen(Objects.requireNonNull(id))));
    }
    @Override public CompletableFuture<Poll> poll(UUID id,int maxRecords,int maxBytes,RequestContext context) {
        if(maxRecords<1 || maxRecords>MAX_RECORDS || maxBytes<=RatisCdcPollRpc.REPLY_BASE_BYTES || maxBytes>MAX_POLL_BYTES) return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid CDC source page budget"));
        return submit(context,OperationFamily.WAL_PAGE,()->{
            var reply=controller.poll(controller.reopen(Objects.requireNonNull(id)),maxRecords,maxBytes);
            return new Poll(reply.lineage(),Outcome.valueOf(reply.outcome().name()),reply.batch()==null ? null : reply.batch().projected(),reply.requiredBytes(),reply.floor(),reply.cut());
        });
    }
    @Override public CompletableFuture<Status> acknowledge(UUID id,UUID batchId,RequestContext context) {
        return submit(context,OperationFamily.CONTROL,()->{ var h=controller.reopen(Objects.requireNonNull(id)); controller.acknowledge(h,Objects.requireNonNull(batchId)); return status(h); });
    }
    @Override public CompletableFuture<Status> closeSubscription(UUID id,RequestContext context) {
        return submit(context,OperationFamily.CONTROL,()->{ var h=controller.reopen(Objects.requireNonNull(id)); controller.closeSubscription(h); return status(h); });
    }
    private RangeDirectory directory(UUID domain) throws IOException {
        var directory=directories.read(domain);
        if(directory==null || !directory.domainId().equals(domain)) throw new IOException("CDC source directory is absent/foreign");
        return directory;
    }
    private static Status status(RatisCdcSubscriptionController.Handle h) { return new Status(h.state(),Phase.valueOf(h.phase().name())); }
    private <T> CompletableFuture<T> submit(RequestContext context,OperationFamily family,Work<T> work) {
        final long deadline;
        try { WorkloadAdmission.resolve(Objects.requireNonNull(context),family); deadline=clock.resolveMonotonicDeadline(context); }
        catch(Throwable failure) { return CompletableFuture.failedFuture(failure); }
        synchronized(this) { if(stopping || closed || actual>=MAX_PENDING) return CompletableFuture.failedFuture(new RejectedExecutionException("CDC bridge admission exhausted/closing")); actual++; }
        var observer=new CompletableFuture<T>();
        if(deadline!=Long.MAX_VALUE) observer.orTimeout(Math.max(1,clock.remainingMonotonicDeadlineNanos(deadline)),TimeUnit.NANOSECONDS);
        try { lane.execute(()->{
            T value=null; Throwable failure=null;
            try {
                if(observer.isDone()) throw new CancellationException("CDC observer stopped before dispatch");
                synchronized(this) { if(stopping) throw new IOException("CDC bridge closing before dispatch"); }
                if(deadline!=Long.MAX_VALUE && clock.remainingMonotonicDeadlineNanos(deadline)<=0) throw new TimeoutException("CDC request expired before dispatch");
                value=work.run();
                if(deadline!=Long.MAX_VALUE && clock.remainingMonotonicDeadlineNanos(deadline)<=0) throw new TimeoutException("CDC result retained after request deadline");
            } catch(Throwable problem) { failure=problem; }
            finally { synchronized(this) { actual--; } }
            if(failure==null) observer.complete(value); else observer.completeExceptionally(failure);
        }); } catch(RejectedExecutionException failure) { synchronized(this) { actual--; } observer.completeExceptionally(failure); }
        return observer;
    }
    synchronized void stopAdmission() { stopping=true; }
    synchronized int actualOwners() { return actual; }
    @Override public synchronized void close() throws IOException {
        if(closed) return; stopping=true;
        if(actual!=0) throw new IOException("CDC actual I/O remains active; ownership retained");
        controller.close(); lane.shutdown(); closed=true;
    }
}
