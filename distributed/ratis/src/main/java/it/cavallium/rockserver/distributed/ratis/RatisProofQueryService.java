package it.cavallium.rockserver.distributed.ratis;

import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.Message;

/** Bounded outbound-proof work independent from Ratis read barriers and the HELP CONTROL lane. */
public final class RatisProofQueryService implements RatisRangeStateMachine.ProofQueryExecutor {
    public static final int THREADS=4,MAX_JOBS=16,MAX_WAITERS=16;
    public static final long MAX_BYTES=32L*1024*1024;
    public record Stats(int jobs,long bytes,int threads) {}
    private record Key(UUID group,String digest) {}
    private final ThreadPoolExecutor executor;
    private final int maximumJobs;private final long maximumBytes;
    public RatisProofQueryService() { this(THREADS,"ratis-proof-query-"); }
    /** Independent bounded catalogue reads, so DATA proof dependencies never compete for their own response budget. */
    public static RatisProofQueryService authorityLane() { return new RatisProofQueryService(1,"ratis-schema-authority-"); }
    public static final long IMPORT_PUBLICATION_BYTES=64L*1024*1024;
    public static RatisProofQueryService importPublicationLane(){return new RatisProofQueryService(1,"ratis-import-publication-",2,2*IMPORT_PUBLICATION_BYTES);}
    public static RatisProofQueryService operationRerouteLane(){return new RatisProofQueryService(1,"ratis-home-reroute-",2,2*IMPORT_PUBLICATION_BYTES);}
    private RatisProofQueryService(int threads,String name) {this(threads,name,MAX_JOBS,MAX_BYTES);}
    private RatisProofQueryService(int threads,String name,int maximumJobs,long maximumBytes) {
        this.maximumJobs=maximumJobs;this.maximumBytes=maximumBytes;
        executor=new ThreadPoolExecutor(threads,threads,0L,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(maximumJobs),
                Thread.ofPlatform().daemon(true).name(name,0).factory(),new ThreadPoolExecutor.AbortPolicy());
    }
    private final Map<Key,Task> tasks=new HashMap<>();
    private long bytes;
    private boolean stopping;
    private CompletableFuture<Void> actualStop;
    private int drainViews;
    private boolean sweeping;
    private final List<Publication> deferred=new ArrayList<>();
    private record Publication(Task task,Message message,Throwable failure) {}
    @Override public CompletableFuture<Message> submit(UUID group,byte[] encoded,long estimatedBytes,boolean coalesce,RatisRangeStateMachine.ProofQueryAction action) {
        Objects.requireNonNull(group); Objects.requireNonNull(encoded); Objects.requireNonNull(action);
        long weight=Math.max(encoded.length,estimatedBytes);
        if (estimatedBytes<0 || weight>maximumBytes) return CompletableFuture.failedFuture(new RejectedExecutionException("Proof query estimated ownership exceeds byte bound"));
        final Key key;
        try { key=new Key(group,HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(encoded))); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        final Task task;
        synchronized (this) {
            if (stopping) return CompletableFuture.failedFuture(new RejectedExecutionException("Proof queries are stopping"));
            var existing=tasks.get(key);
            if (existing!=null) {
                if (weight>existing.weight) return CompletableFuture.failedFuture(new RejectedExecutionException("Coalesced proof query exceeds original ownership estimate"));
                if (!coalesce) return CompletableFuture.failedFuture(new RejectedExecutionException("Proof query is already active"));
                if (existing.waiters>=MAX_WAITERS) return CompletableFuture.failedFuture(new RejectedExecutionException("Proof query waiter capacity exhausted"));
                existing.waiters++; return existing.result.copy();
            }
            if (tasks.size()>=maximumJobs || weight>maximumBytes-bytes) return CompletableFuture.failedFuture(new RejectedExecutionException("Proof query capacity exhausted"));
            task=new Task(key,encoded.clone(),weight,action); tasks.put(key,task); bytes+=task.weight;
        }
        try { executor.execute(task); } catch (RuntimeException rejected) { task.reject(rejected); }
        boolean stopped; synchronized (this) { stopped=stopping; }
        if (stopped && executor.remove(task)) task.reject(new RejectedExecutionException("Proof query stopped before execution"));
        return task.result.copy();
    }
    /** Cancel queued actions, never interrupt running native/RPC owners, and return a cancellable drain view. */
    public CompletableFuture<Void> stop() {
        final List<Task> pending; final CompletableFuture<Void> view;
        synchronized (this) {
            stopping=true;
            if (actualStop==null) { actualStop=new CompletableFuture<>(); pending=List.copyOf(tasks.values()); sweeping=true; }
            else pending=null;
            if (!actualStop.isDone() && drainViews>=MAX_WAITERS) view=CompletableFuture.failedFuture(new RejectedExecutionException("Proof query drain waiter capacity exhausted"));
            else { if (!actualStop.isDone()) drainViews++; view=actualStop.copy(); }
        }
        if (pending!=null) {
            CompletableFuture.allOf(pending.stream().map(task -> task.drained).toArray(CompletableFuture[]::new)).whenComplete((ignored,failure) -> actualStop.complete(null));
            for (var task:pending) if (executor.remove(task)) task.reject(new RejectedExecutionException("Proof query stopped before execution"));
            executor.shutdown();
            final List<Publication> publications;
            synchronized (this) { sweeping=false; publications=List.copyOf(deferred); deferred.clear(); }
            for (var publication:publications) publish(publication);
        }
        return view;
    }
    public synchronized Stats stats() { return new Stats(tasks.size(),bytes,executor.getPoolSize()); }
    private void finish(Task task,Message message,Throwable failure) {
        var publication=new Publication(task,message,failure); final boolean defer;
        synchronized (this) {
            if (!tasks.remove(task.key,task)) return; bytes-=task.weight;
            defer=sweeping; if (defer) deferred.add(publication);
        }
        task.drained.complete(null);
        if (!defer) publish(publication);
    }
    private static void publish(Publication publication) {
        if (publication.failure()==null) publication.task().result.complete(publication.message());
        else publication.task().result.completeExceptionally(publication.failure());
    }
    private final class Task implements Runnable {
        final Key key;
        byte[] input;
        RatisRangeStateMachine.ProofQueryAction action;
        final long weight;
        int waiters=1;
        final CompletableFuture<Message> result=new CompletableFuture<>();
        final CompletableFuture<Void> drained=new CompletableFuture<>();
        final AtomicInteger phase=new AtomicInteger();
        Task(Key key,byte[] input,long weight,RatisRangeStateMachine.ProofQueryAction action) { this.key=key; this.input=input; this.action=action; this.weight=weight; }
        @Override public void run() {
            if (!phase.compareAndSet(0,1)) return;
            Message message=null; Throwable failure=null;
            try {
                boolean stopped; synchronized (RatisProofQueryService.this) { stopped=stopping; }
                if (stopped) throw new RejectedExecutionException("Proof query stopped before execution");
                message=Objects.requireNonNull(action.run(input));
            } catch (Throwable problem) { failure=problem; }
            finally { phase.set(2); input=null; action=null; finish(this,message,failure); }
            if (failure instanceof Error fatal) throw fatal;
        }
        void reject(RuntimeException failure) { if (phase.compareAndSet(0,2)) { input=null; action=null; finish(this,null,failure); } }
    }
}
