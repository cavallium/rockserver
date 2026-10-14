package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.ReplicaPhysicalAPI;
import it.cavallium.rockserver.core.common.ColumnTableProperties;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/** Trusted local physical work. Cancellation stops observation, not already running JNI. */
public final class RatisNodeMaintenance implements AutoCloseable {
    public record Target(UUID node,UUID incarnation,UUID process,UUID pool,UUID column,long nativeColumnId) {
        public Target {Objects.requireNonNull(node);Objects.requireNonNull(incarnation);Objects.requireNonNull(process);Objects.requireNonNull(pool);Objects.requireNonNull(column);if(nativeColumnId<0)throw new IllegalArgumentException("Invalid native CF identity");}
    }
    public record Policy(boolean flush,boolean compact,int maximumPending,int maximumFiles,long maximumMetadataBytes) {
        public Policy {if(maximumPending<1 || maximumPending>64 || maximumFiles<1 || maximumFiles>SstMaintenance.MAX_INPUT_FILES || maximumMetadataBytes<1 || maximumMetadataBytes>SstMaintenance.MAX_METADATA_RESPONSE_BYTES)throw new IllegalArgumentException("Invalid local maintenance policy");}
    }
    private final UUID node,incarnation,process=UUID.randomUUID();
    private final Map<UUID,DistributedStoragePool> pools;
    private final RWScheduler scheduler;
    private final Policy policy;
    private final Set<CompletableFuture<Void>> owners=new HashSet<>();
    private final Map<CompletableFuture<Void>,Runnable> queuedStops=new HashMap<>();
    private boolean closed;
    private long observationBytes;
    public RatisNodeMaintenance(UUID node,UUID incarnation,Collection<DistributedStoragePool> pools,RWScheduler scheduler,Policy policy) {
        this.node=Objects.requireNonNull(node);this.incarnation=Objects.requireNonNull(incarnation);this.scheduler=Objects.requireNonNull(scheduler);this.policy=Objects.requireNonNull(policy);
        var map=new HashMap<UUID,DistributedStoragePool>();for(var pool:pools)if(map.put(pool.maintenanceSession(),pool)!=null)throw new IllegalArgumentException("Duplicate maintenance pool");if(map.isEmpty() || map.size()>RatisNodePlan.MAX_LOCAL_GROUPS)throw new IllegalArgumentException("Invalid local pool count");this.pools=Map.copyOf(map);
    }
    public Target target(DistributedStoragePool pool,UUID column)throws java.io.IOException,org.rocksdb.RocksDBException {
        if(pools.get(pool.maintenanceSession())!=pool)throw new IllegalArgumentException("Pool belongs to another local owner");return new Target(node,incarnation,process,pool.maintenanceSession(),column,pool.physicalColumnId(pool.maintenanceSession(),column));
    }
    @FunctionalInterface private interface Action<T> {T run(DistributedStoragePool pool)throws Exception;}
    private <T>CompletableFuture<T> submit(Target target,OperationFamily family,Action<T> action) {
        Objects.requireNonNull(target);
        if(!node.equals(target.node()) || !incarnation.equals(target.incarnation()) || !process.equals(target.process()))throw new IllegalArgumentException("Stale or foreign local maintenance target");
        return submitPool(target.pool(),family,action);
    }
    private <T>CompletableFuture<T> submitPool(UUID poolSession,OperationFamily family,Action<T> action) {
        return submitPool(poolSession,family,action,false);
    }
    private <T>CompletableFuture<T> submitPool(UUID poolSession,OperationFamily family,Action<T> action,boolean observation) {
        var observed=new CompletableFuture<T>();var settled=new CompletableFuture<Void>();DistributedStoragePool pool;
        synchronized(this) {
            if(closed)throw new IllegalStateException("Local maintenance is closed");
            if((pool=pools.get(poolSession))==null)throw new IllegalArgumentException("Stale or foreign local maintenance target");
            if(owners.size()>=policy.maximumPending())throw new IllegalStateException("Local maintenance capacity exhausted");
            if(observation && observationBytes!=0)throw new IllegalStateException("Physical observation metadata credit exhausted");
            // Reserve the whole policy before discovery copies, JNI, or scheduling. A scope
            // keeps all prior results, so individual CF completion never refunds this credit.
            if(observation)observationBytes=policy.maximumMetadataBytes();
            owners.add(settled);
        }
        class Job implements Runnable,RWScheduler.RejectionAwareTask {
            private final java.util.concurrent.atomic.AtomicBoolean claimed=new java.util.concurrent.atomic.AtomicBoolean();
            private void release(){synchronized(RatisNodeMaintenance.this){owners.remove(settled);queuedStops.remove(settled);if(observation)observationBytes=0;}settled.complete(null);}
            void cancelQueued(){if(claimed.compareAndSet(false,true)){release();observed.completeExceptionally(new IllegalStateException("Physical job stopped before native execution"));}}
            @Override public void run(){if(!claimed.compareAndSet(false,true))return;T result=null;Throwable failure=null;try {if(!observed.isCancelled())result=action.run(pool);}catch(Throwable caught){failure=caught;}finally {release();}if(failure==null)observed.complete(result);else observed.completeExceptionally(failure);}
            @Override public void reject(RuntimeException failure){if(claimed.compareAndSet(false,true))release();observed.completeExceptionally(failure);}
        }
        var job=new Job();boolean stopped;
        synchronized(this){stopped=closed;if(!stopped)queuedStops.put(settled,job::cancelQueued);}
        if(stopped){job.cancelQueued();return observed;}
        observed.whenComplete((value,failure)->{if(observed.isCancelled())job.cancelQueued();});
        try {scheduler.executor(WorkloadProfile.PHYSICAL_MAINTENANCE,family,Long.MAX_VALUE).execute(job,policy.maximumMetadataBytes());}
        catch(RuntimeException failure){job.reject(failure);}
        return observed;
    }
    public CompletableFuture<Void> flush(Target target) {
        if(!policy.flush())throw new IllegalStateException("Local flush disabled");return submit(target,OperationFamily.FLUSH,pool->{pool.flushQualifiedLocal(target.pool(),target.column(),target.nativeColumnId());return null;});
    }
    public CompletableFuture<Void> compact(Target target) {
        if(!policy.compact())throw new IllegalStateException("Local compaction disabled");return submit(target,OperationFamily.COMPACTION,pool->{pool.compactQualifiedLocal(target.pool(),target.column(),target.nativeColumnId());return null;});
    }
    public CompletableFuture<SstMaintenance.Metadata> metadata(Target target,int level) {
        return submit(target,OperationFamily.COMPACTION,pool->pool.metadataQualifiedLocal(target.pool(),target.column(),target.nativeColumnId(),level,policy.maximumFiles(),policy.maximumMetadataBytes()));
    }
    public CompletableFuture<SstMaintenance.Result> compactFiles(Target target,SstMaintenance.Request request) {
        Objects.requireNonNull(request);if(request.execute() && !policy.compact())throw new IllegalStateException("Local compaction disabled");
        if(request.files().size()>policy.maximumFiles())throw new IllegalArgumentException("Selective compaction exceeds local file policy");
        return submit(target,OperationFamily.COMPACTION,pool->pool.compactQualifiedFilesLocal(target.pool(),target.column(),target.nativeColumnId(),request,policy.maximumFiles(),policy.maximumMetadataBytes()));
    }
    public CompletableFuture<ColumnTableProperties> tableProperties(Target target) {return observe(target).thenApply(DistributedStoragePool.LocalObservation::tables);}
    public CompletableFuture<DistributedStoragePool.LocalObservation> observe(Target target) {
        Objects.requireNonNull(target);
        if(!node.equals(target.node()) || !incarnation.equals(target.incarnation()) || !process.equals(target.process()))throw new IllegalArgumentException("Stale or foreign local maintenance target");
        var limits=DistributedStoragePool.TablePropertyLimits.forScope(1,policy.maximumFiles(),policy.maximumMetadataBytes());
        return submitPool(target.pool(),OperationFamily.COMPACTION,
                pool->pool.observeQualifiedLocal(target.pool(),target.column(),target.nativeColumnId(),limits),true);
    }
    /** One owner covers discovery, the whole retained response, and serial JNI/aggregation drain. */
    CompletableFuture<List<ReplicaPhysicalAPI.Observation>> observeScope(
            Map<UUID,DistributedStoragePool> configured,UUID selectedPool,UUID selectedColumn) {
        if(selectedPool!=null && !configured.containsKey(selectedPool))throw new IllegalArgumentException("Foreign configured pool");
        UUID firstSession=configured.entrySet().stream().filter(e->selectedPool==null || selectedPool.equals(e.getKey()))
                .map(e->e.getValue().maintenanceSession()).findFirst().orElseThrow();
        var cancelled=new java.util.concurrent.atomic.AtomicBoolean();
        var future=submitPool(firstSession,OperationFamily.COMPACTION,ignored->{
            // This list and every subsequent retained object are created after credit admission.
            long selectedCount=selectedPool==null?configured.size():1;
            long poolVectorBytes=128*selectedCount;
            if(poolVectorBytes>=policy.maximumMetadataBytes())throw new IllegalArgumentException("Physical pool vector exceeds policy");
            var selected=configured.entrySet().stream().filter(e->selectedPool==null || selectedPool.equals(e.getKey()))
                    .sorted(Map.Entry.comparingByKey()).toList();
            var targets=new ArrayList<ReplicaPhysicalAPI.Target>();
            var seen=new HashSet<String>();
            for(var entry:selected) {
                if(cancelled.get())throw new java.util.concurrent.CancellationException();
                var pool=entry.getValue();
                if(pools.get(pool.maintenanceSession())!=pool)throw new IllegalArgumentException("Pool belongs to another local owner");
                int remaining=(int)Math.min(65536-targets.size(),(policy.maximumMetadataBytes()-poolVectorBytes)/512-targets.size());
                if(remaining<0)throw new IllegalArgumentException("Physical target vector exceeds policy");
                for(var column:pool.physicalColumnsLocal(pool.maintenanceSession(),selectedColumn,remaining,32L*remaining)) {
                    if(seen.add(pool.maintenanceSession()+":"+column.nativeColumnId()))
                        targets.add(new ReplicaPhysicalAPI.Target(node,incarnation,process,entry.getKey(),pool.maintenanceSession(),column.column(),column.nativeColumnId()));
                }
            }
            if(selectedColumn!=null && targets.isEmpty())throw new IllegalArgumentException("Unknown or retired physical column");
            if(targets.isEmpty())return List.<ReplicaPhysicalAPI.Observation>of();
            var limits=DistributedStoragePool.TablePropertyLimits.forScope(targets.size(),policy.maximumFiles(),policy.maximumMetadataBytes()-poolVectorBytes);
            var found=new ArrayList<ReplicaPhysicalAPI.Observation>(targets.size());
            for(var target:targets) {
                if(cancelled.get())throw new java.util.concurrent.CancellationException();
                synchronized(this){if(closed)throw new IllegalStateException("Local maintenance is closed");}
                var pool=pools.get(target.session());
                var value=pool.observeQualifiedLocal(target.session(),target.column(),target.nativeColumnId(),limits);
                if(cancelled.get())throw new java.util.concurrent.CancellationException();
                found.add(new ReplicaPhysicalAPI.Observation(target,value.estimatedKeys(),value.actualCFComparator(),value.tables()));
            }
            return List.copyOf(found);
        },true);
        future.whenComplete((value,failure)->{if(future.isCancelled())cancelled.set(true);});
        return future;
    }
    synchronized long observationBytes(){return observationBytes;}
    CompletableFuture<List<Target>> discover(DistributedStoragePool pool,UUID column,int maximum,long bytes){return submitPool(pool.maintenanceSession(),OperationFamily.COMPACTION,owned->owned.physicalColumnsLocal(owned.maintenanceSession(),column,maximum,bytes).stream().map(c->new Target(node,incarnation,process,owned.maintenanceSession(),c.column(),c.nativeColumnId())).toList());}
    public CompletableFuture<Void> flushPool(UUID session){if(!policy.flush())throw new IllegalStateException("Local flush disabled");return submitPool(session,OperationFamily.FLUSH,pool->{pool.flushWholePoolQualified(session,it.cavallium.rockserver.core.common.SstMaintenance.MAX_INPUT_FILES,policy.maximumMetadataBytes());return null;});}
    public synchronized int pending(){return owners.size();}
    /** Stop new jobs; the returned future completes only after actual queued/running ownership exits. */
    public CompletableFuture<Void> stop(){
        final List<Runnable> cancel;final CompletableFuture<Void> drained;
        synchronized(this){closed=true;cancel=List.copyOf(queuedStops.values());drained=CompletableFuture.allOf(owners.toArray(CompletableFuture[]::new));}
        cancel.forEach(Runnable::run);return drained;
    }
    @Override public void close(){stop();}
}
