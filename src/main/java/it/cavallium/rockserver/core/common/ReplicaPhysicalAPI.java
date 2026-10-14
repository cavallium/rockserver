package it.cavallium.rockserver.core.common;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/** Node/operator-owned physical observations. No logical totals or replica aggregation are implied. */
public interface ReplicaPhysicalAPI {
    record Target(UUID node,UUID incarnation,UUID process,UUID pool,UUID session,UUID column,long nativeColumnId) {
        public Target {Objects.requireNonNull(node);Objects.requireNonNull(incarnation);Objects.requireNonNull(process);Objects.requireNonNull(pool);Objects.requireNonNull(session);Objects.requireNonNull(column);if(nativeColumnId<0)throw new IllegalArgumentException("Invalid native CF identity");}
    }
    record Observation(Target target,long estimatedKeys,String actualCFComparator,ColumnTableProperties tables) {
        public Observation {Objects.requireNonNull(target);Objects.requireNonNull(actualCFComparator);Objects.requireNonNull(tables);}
    }
    /** Null pool means this node; null column means all bound CFs in the selected scope. */
    record Scope(UUID pool,UUID column) {}
    CompletableFuture<List<Target>> discover(Scope scope);
    CompletableFuture<List<Observation>> observe(Scope scope);
    CompletableFuture<Void> flush(Scope scope);
    CompletableFuture<Void> compact(Scope scope);
    CompletableFuture<SstMaintenance.Metadata> metadata(Target target,int level);
    CompletableFuture<SstMaintenance.Result> compactFiles(Target target,SstMaintenance.Request request);
}
