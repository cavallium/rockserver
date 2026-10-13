package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/** Trusted local physical work. Cancellation stops observation, not already running JNI. */
public final class RatisNodeMaintenance implements AutoCloseable {
    public record Target(UUID node,UUID incarnation,UUID process,UUID pool,UUID column) {
        public Target {Objects.requireNonNull(node);Objects.requireNonNull(incarnation);Objects.requireNonNull(process);Objects.requireNonNull(pool);Objects.requireNonNull(column);}
    }
    public record Policy(boolean flush,boolean compact,int maximumPending,int maximumFiles,long maximumMetadataBytes) {
        public Policy {if(maximumPending<1 || maximumPending>64 || maximumFiles<1 || maximumFiles>SstMaintenance.MAX_INPUT_FILES || maximumMetadataBytes<1 || maximumMetadataBytes>SstMaintenance.MAX_METADATA_RESPONSE_BYTES)throw new IllegalArgumentException("Invalid local maintenance policy");}
    }
    private final UUID node,incarnation,process=UUID.randomUUID();
    private final Map<UUID,DistributedStoragePool> pools;
    private final RWScheduler scheduler;
    private final Policy policy;
    private final Set<CompletableFuture<Void>> owners=new HashSet<>();
    private boolean closed;
    public RatisNodeMaintenance(UUID node,UUID incarnation,Collection<DistributedStoragePool> pools,RWScheduler scheduler,Policy policy) {
        this.node=Objects.requireNonNull(node);this.incarnation=Objects.requireNonNull(incarnation);this.scheduler=Objects.requireNonNull(scheduler);this.policy=Objects.requireNonNull(policy);
        var map=new HashMap<UUID,DistributedStoragePool>();for(var pool:pools)if(map.put(pool.maintenanceSession(),pool)!=null)throw new IllegalArgumentException("Duplicate maintenance pool");if(map.isEmpty() || map.size()>RatisNodePlan.MAX_LOCAL_GROUPS)throw new IllegalArgumentException("Invalid local pool count");this.pools=Map.copyOf(map);
    }
    public Target target(DistributedStoragePool pool,UUID column) {
        if(pools.get(pool.maintenanceSession())!=pool)throw new IllegalArgumentException("Pool belongs to another local owner");return new Target(node,incarnation,process,pool.maintenanceSession(),column);
    }
    @FunctionalInterface private interface Action<T> {T run(DistributedStoragePool pool)throws Exception;}
    private <T>CompletableFuture<T> submit(Target target,OperationFamily family,Action<T> action) {
        Objects.requireNonNull(target);var observed=new CompletableFuture<T>();var settled=new CompletableFuture<Void>();DistributedStoragePool pool;
        synchronized(this) {
            if(closed)throw new IllegalStateException("Local maintenance is closed");
            if(!node.equals(target.node()) || !incarnation.equals(target.incarnation()) || !process.equals(target.process()) || (pool=pools.get(target.pool()))==null)throw new IllegalArgumentException("Stale or foreign local maintenance target");
            if(owners.size()>=policy.maximumPending())throw new IllegalStateException("Local maintenance capacity exhausted");owners.add(settled);
        }
        class Job implements Runnable,RWScheduler.RejectionAwareTask {
            private final java.util.concurrent.atomic.AtomicBoolean claimed=new java.util.concurrent.atomic.AtomicBoolean();
            private void release(){synchronized(RatisNodeMaintenance.this){owners.remove(settled);}settled.complete(null);}
            @Override public void run(){if(!claimed.compareAndSet(false,true))return;T result=null;Throwable failure=null;try {if(!observed.isCancelled())result=action.run(pool);}catch(Throwable caught){failure=caught;}finally {release();}if(failure==null)observed.complete(result);else observed.completeExceptionally(failure);}
            @Override public void reject(RuntimeException failure){if(claimed.compareAndSet(false,true))release();observed.completeExceptionally(failure);}
        }
        var job=new Job();
        try {scheduler.executor(WorkloadProfile.PHYSICAL_MAINTENANCE,family,Long.MAX_VALUE).execute(job,policy.maximumMetadataBytes());}
        catch(RuntimeException failure){job.reject(failure);}
        return observed;
    }
    public CompletableFuture<Void> flush(Target target) {
        if(!policy.flush())throw new IllegalStateException("Local flush disabled");return submit(target,OperationFamily.FLUSH,pool->{pool.flushLocal(target.pool(),target.column());return null;});
    }
    public CompletableFuture<Void> compact(Target target) {
        if(!policy.compact())throw new IllegalStateException("Local compaction disabled");return submit(target,OperationFamily.COMPACTION,pool->{pool.compactLocal(target.pool(),target.column());return null;});
    }
    public CompletableFuture<SstMaintenance.Metadata> metadata(Target target,int level) {
        return submit(target,OperationFamily.COMPACTION,pool->pool.sstMetadataLocal(target.pool(),target.column(),level,policy.maximumFiles(),policy.maximumMetadataBytes()));
    }
    public CompletableFuture<SstMaintenance.Result> compactFiles(Target target,SstMaintenance.Request request) {
        Objects.requireNonNull(request);if(request.execute() && !policy.compact())throw new IllegalStateException("Local compaction disabled");
        if(request.files().size()>policy.maximumFiles())throw new IllegalArgumentException("Selective compaction exceeds local file policy");
        return submit(target,OperationFamily.COMPACTION,pool->pool.compactFilesLocal(target.pool(),target.column(),request,policy.maximumFiles(),policy.maximumMetadataBytes()));
    }
    public CompletableFuture<DistributedStoragePool.LocalTableProperties> tableProperties(Target target) {
        return submit(target,OperationFamily.COMPACTION,pool->pool.tablePropertiesLocal(target.pool(),target.column(),policy.maximumFiles()));
    }
    public synchronized int pending(){return owners.size();}
    /** Stop new jobs; the returned future completes only after actual queued/running ownership exits. */
    public synchronized CompletableFuture<Void> stop(){closed=true;return CompletableFuture.allOf(owners.toArray(CompletableFuture[]::new));}
    @Override public void close(){stop();}
}
