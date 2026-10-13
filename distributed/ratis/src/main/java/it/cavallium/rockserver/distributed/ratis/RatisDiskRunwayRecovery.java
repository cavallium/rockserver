package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** One actual flush/snapshot owner. Snapshot publication and log purge remain with the pinned updater. */
final class RatisDiskRunwayRecovery {
    record Target(UUID pool,UUID group,long imageBound,Runnable flush,Supplier<CompletableFuture<Void>> snapshot) {}
    private final RWScheduler scheduler;private final RatisDiskRunway runway;private final Supplier<List<Target>> targets;
    private boolean stopping;private CompletableFuture<Void> owner;private int next;
    RatisDiskRunwayRecovery(RWScheduler scheduler,RatisDiskRunway runway,Supplier<List<Target>> targets){this.scheduler=scheduler;this.runway=runway;this.targets=targets;}
    void request(){
        final Target target;final CompletableFuture<Void> actual;
        synchronized(this){if(stopping || owner!=null || runway.state()==RatisDiskRunway.State.UNKNOWN)return;}
        // Qualification may read actual native/division metadata; never invoke it under the owner monitor.
        var values=targets.get();if(values.isEmpty())return;
        synchronized(this){if(stopping || owner!=null || runway.state()==RatisDiskRunway.State.UNKNOWN)return;target=values.get(Math.floorMod(next++,values.size()));owner=actual=new CompletableFuture<>();}
        class Job implements Runnable,RWScheduler.RejectionAwareTask {
            private final java.util.concurrent.atomic.AtomicBoolean claimed=new java.util.concurrent.atomic.AtomicBoolean();private RatisDiskRunway.RecoveryLease disk;
            private void settled(Throwable failure){if(disk!=null)disk.actualFinished();synchronized(RatisDiskRunwayRecovery.this){owner=null;}if(failure==null)actual.complete(null);else actual.completeExceptionally(failure);}
            @Override public void run(){if(!claimed.compareAndSet(false,true))return;try{
                // This is declared recovery headroom, not a proof of native WAL/SST allocation size.
                disk=runway.reserveRecovery(target.pool(),runway.policy().recoveryHeadroomBytes());
                target.flush().run();disk.actualFinished();disk=null;target.snapshot().get().whenComplete((ignored,failure)->settled(failure));
            }catch(Throwable failed){settled(failed);}}
            @Override public void reject(RuntimeException failed){if(claimed.compareAndSet(false,true))settled(failed);}
        }
        var job=new Job();try{scheduler.executor(WorkloadProfile.PHYSICAL_MAINTENANCE,OperationFamily.FLUSH,Long.MAX_VALUE).execute(job,64*1024);}catch(RuntimeException failed){job.reject(failed);}
    }
    CompletableFuture<Void> stop(){synchronized(this){stopping=true;return owner==null?CompletableFuture.completedFuture(null):owner.copy();}}
}
