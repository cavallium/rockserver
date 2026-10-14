package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/** Borrowed Runtime maintenance owner; its stop/drain retains JNI lifetimes through observer cancellation. */
public final class RatisNodePhysicalApi implements ReplicaPhysicalAPI {
    private static final int MAX_TARGETS=65536,TARGET_BYTES=128;
    private final RatisNodeMaintenance maintenance;private final Map<UUID,DistributedStoragePool> pools;private final RatisNodeMaintenance.Policy policy;
    RatisNodePhysicalApi(RatisNodeMaintenance maintenance,Map<UUID,DistributedStoragePool> pools,RatisNodeMaintenance.Policy policy){this.maintenance=maintenance;this.pools=Map.copyOf(pools);this.policy=policy;}
    private List<Map.Entry<UUID,DistributedStoragePool>> pools(Scope scope){Objects.requireNonNull(scope);if(scope.pool()!=null && !pools.containsKey(scope.pool()))throw new IllegalArgumentException("Foreign configured pool");return pools.entrySet().stream().filter(e->scope.pool()==null || scope.pool().equals(e.getKey())).sorted(Map.Entry.comparingByKey()).toList();}
    @Override public CompletableFuture<List<Target>> discover(Scope scope){
        var result=CompletableFuture.completedFuture(new ArrayList<Target>());
        for(var entry:pools(scope))result=result.thenCompose(found->{int remaining=(int)Math.min(MAX_TARGETS-found.size(),policy.maximumMetadataBytes()/TARGET_BYTES-found.size());if(remaining<0)throw new IllegalArgumentException("Physical target vector exceeds policy");return maintenance.discover(entry.getValue(),scope.column(),remaining,(long)remaining*32).thenApply(targets->{for(var t:targets)found.add(new Target(t.node(),t.incarnation(),t.process(),entry.getKey(),t.pool(),t.column(),t.nativeColumnId()));return found;});});
        return result.thenApply(found->{if(scope.column()!=null && found.isEmpty())throw new IllegalArgumentException("Unknown or retired physical column");return List.copyOf(found);});
    }
    private RatisNodeMaintenance.Target local(Target target){Objects.requireNonNull(target);var pool=pools.get(target.pool());if(pool==null || !pool.maintenanceSession().equals(target.session()))throw new IllegalArgumentException("Stale/foreign physical pool");return new RatisNodeMaintenance.Target(target.node(),target.incarnation(),target.process(),target.session(),target.column(),target.nativeColumnId());}
    @Override public CompletableFuture<List<Observation>> observe(Scope scope){return discover(scope).thenCompose(targets->{if((long)targets.size()*(TARGET_BYTES+72+2*65536)>policy.maximumMetadataBytes())throw new IllegalArgumentException("Physical observation vector exceeds bounded comparator/response envelope");var result=CompletableFuture.completedFuture(new ArrayList<Observation>());for(var target:targets)result=result.thenCompose(found->maintenance.observe(local(target)).thenApply(value->{var t=value.tables();found.add(new Observation(target,value.estimatedKeys(),new Scalars(t.files(),t.entries(),t.deletions(),t.mergeOperands(),t.rangeDeletions(),t.dataBytes(),t.indexBytes(),t.filterBytes(),t.comparator())));return found;}));return result.thenApply(List::copyOf);});}
    @Override public CompletableFuture<Void> flush(Scope scope){if(!policy.flush())throw new IllegalStateException("Local flush disabled");if(scope.column()!=null)return discover(scope).thenCompose(targets->each(targets,true));var result=CompletableFuture.<Void>completedFuture(null);var seen=new HashSet<UUID>();for(var pool:pools(scope))if(seen.add(pool.getValue().maintenanceSession()))result=result.thenCompose(ignored->maintenance.flushPool(pool.getValue().maintenanceSession()));return result;}
    @Override public CompletableFuture<Void> compact(Scope scope){if(!policy.compact())throw new IllegalStateException("Local compact disabled");return discover(scope).thenCompose(targets->each(targets,false));}
    private CompletableFuture<Void> each(List<Target> targets,boolean flush){var result=CompletableFuture.<Void>completedFuture(null);var seen=new HashSet<String>();for(var target:targets)if(seen.add(target.session()+":"+target.nativeColumnId()))result=result.thenCompose(ignored->flush?maintenance.flush(local(target)):maintenance.compact(local(target)));return result;}
    @Override public CompletableFuture<SstMaintenance.Metadata> metadata(Target target,int level){return maintenance.metadata(local(target),level);}
    @Override public CompletableFuture<SstMaintenance.Result> compactFiles(Target target,SstMaintenance.Request request){return maintenance.compactFiles(local(target),request);}
}
