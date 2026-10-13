package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

/** Fresh filesystem observations and explicit prospective disk growth, never inferred from heap weights. */
public final class RatisDiskRunway {
    public enum State { UNKNOWN,CLEAR,WARNING,EMERGENCY }
    public record Policy(long warningFreeBytes,long emergencyFreeBytes,long resumeFreeBytes,long maximumPendingBytes,
                         long maximumProtectedPendingBytes,long recoveryHeadroomBytes,Duration maximumCensusAge,
                         int maximumScopes,int maximumFiles,int visits) {
        public Policy {
            Objects.requireNonNull(maximumCensusAge);
            if(emergencyFreeBytes<1 || warningFreeBytes<=emergencyFreeBytes || resumeFreeBytes<=warningFreeBytes || resumeFreeBytes>RatisNodeResourceBudget.MAX_BYTES
                    || maximumPendingBytes<1 || maximumProtectedPendingBytes<1 || maximumPendingBytes>RatisNodeResourceBudget.MAX_BYTES || maximumProtectedPendingBytes>RatisNodeResourceBudget.MAX_BYTES
                    || recoveryHeadroomBytes<1 || recoveryHeadroomBytes>=emergencyFreeBytes || maximumCensusAge.isZero() || maximumCensusAge.isNegative() || maximumCensusAge.compareTo(Duration.ofMinutes(10))>0
                    || maximumScopes<1 || maximumScopes>8192 || maximumFiles<1 || maximumFiles>1_000_000 || visits<1 || visits>4096)throw new IllegalArgumentException("Invalid running disk runway policy");
        }
        public static Policy defaults(){long entry=it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_FILE_BYTES+4096;
            long log=Math.addExact(Math.multiplyExact(2,entry),RatisRangeStateMachine.COMMITTED_SEGMENT_BYTES),recovery=1L<<30,emergency=Math.addExact(2*recovery,log);
            return new Policy(Math.addExact(emergency,recovery),emergency,Math.addExact(Math.addExact(emergency,recovery),log),4*log,emergency,recovery,Duration.ofMinutes(5),4096,65536,512);
        }
    }
    public record Filesystem(long id,long usableBytes) {public Filesystem{if(usableBytes<0)throw new IllegalArgumentException("Negative observed free space");}}
    public record Census(long startedAtNanos,long finishedCut,Map<Long,Filesystem> filesystems,Map<UUID,Set<Long>> pools,Set<Long> raftFilesystems,long observedLogicalBytes) {
        public Census {filesystems=Map.copyOf(filesystems);var copy=new HashMap<UUID,Set<Long>>();pools.forEach((id,value)->copy.put(id,Set.copyOf(value)));pools=Map.copyOf(copy);raftFilesystems=Set.copyOf(raftFilesystems);if(filesystems.isEmpty() || observedLogicalBytes<0)throw new IllegalArgumentException("Incomplete disk census");for(var value:pools.values())if(!filesystems.keySet().containsAll(value))throw new IllegalArgumentException("Pool census incomplete");if(!filesystems.keySet().containsAll(raftFilesystems))throw new IllegalArgumentException("Raft census incomplete");}
    }
    public record Stats(State state,long pendingOrdinaryBytes,long pendingProtectedBytes,int owners,long observedLogicalBytes) {}
    private final Policy policy;private final LongSupplier clock;private final Set<AppendLease> pending=new LinkedHashSet<>();
    private Census latest;private State state=State.UNKNOWN;private boolean valid;private long ordinary,protectedBytes;private boolean actualRecovery;
    private long censusCut,observationCut,reconciledCut,freeAt;private Map<Long,Filesystem> free=Map.of();private final Map<Long,Debt> debt=new HashMap<>();
    private static final class Debt {long oldOrdinary,oldProtected,newOrdinary,newProtected;}
    public RatisDiskRunway(Policy policy,LongSupplier clock){this.policy=Objects.requireNonNull(policy);this.clock=Objects.requireNonNull(clock);}
    public Policy policy(){return policy;}
    private boolean fresh(){long age=latest==null?Long.MAX_VALUE:clock.getAsLong()-latest.startedAtNanos();return age>=0 && age<=policy.maximumCensusAge().toNanos();}
    public synchronized void invalidate(){valid=false;}
    public synchronized void invalidateFree(){free=Map.of();}
    /** Cutover is ordered with actual writer exits; at most two aggregated debt epochs are retained per FS. */
    public synchronized long beginCensus(){return censusCut=Math.incrementExact(censusCut);}
    public synchronized long beginFreeObservation(){observationCut=Math.incrementExact(observationCut);for(var value:debt.values()){value.oldOrdinary+=value.newOrdinary;value.oldProtected+=value.newProtected;value.newOrdinary=0;value.newProtected=0;}return observationCut;}
    public synchronized void publish(Census census)throws IOException {
        long age=clock.getAsLong()-census.startedAtNanos();if(age<0 || age>policy.maximumCensusAge().toNanos())throw new IOException("Disk census is not complete and fresh");
        if(latest!=null && census.startedAtNanos()-latest.startedAtNanos()<0 || census.finishedCut()!=censusCut)throw new IOException("Superseded disk census");latest=census;valid=true;if(!free.keySet().equals(census.filesystems().keySet()))free=Map.of();
    }
    public synchronized void observeFree(long startedAt,long cut,Map<Long,Filesystem> observed)throws IOException {
        long age=clock.getAsLong()-startedAt;if(age<0 || age>Duration.ofSeconds(5).toNanos() || latest==null || !latest.filesystems().keySet().equals(observed.keySet()))throw new IOException("Free-space observation incomplete or stale");
        if(cut!=observationCut)throw new IOException("Superseded free-space reconciliation");
        if(!free.isEmpty() && startedAt-freeAt<0)throw new IOException("Superseded free-space observation");
        free=Map.copyOf(observed);freeAt=startedAt;
        for(var value:debt.values()){ordinary-=value.oldOrdinary;protectedBytes-=value.oldProtected;value.oldOrdinary=0;value.oldProtected=0;}reconciledCut=observationCut;
        long minimum=free.values().stream().mapToLong(Filesystem::usableBytes).min().orElse(0);
        if(minimum<policy.emergencyFreeBytes() || state==State.EMERGENCY && minimum<policy.warningFreeBytes())state=State.EMERGENCY;
        else if((state==State.WARNING || state==State.EMERGENCY) && minimum<policy.resumeFreeBytes() || minimum<policy.warningFreeBytes())state=State.WARNING;
        else state=State.CLEAR;
    }
    private boolean freeFresh(){long age=clock.getAsLong()-freeAt;return !free.isEmpty() && age>=0 && age<=Duration.ofSeconds(5).toNanos();}
    public synchronized void requireOrdinary()throws IOException {if(!valid || !fresh() || !freeFresh() || state==State.UNKNOWN || state==State.EMERGENCY)throw new IOException("New ordinary work lacks fresh disk runway");}
    public synchronized State state(){return valid && fresh() && freeFresh()?state:State.UNKNOWN;}
    public synchronized Stats stats(){return new Stats(state(),ordinary,protectedBytes,pending.size(),latest==null?0:latest.observedLogicalBytes());}
    /** Both Raft/external payload and pool WAL/data filesystems are conservatively charged once per unique FS. */
    public synchronized AppendLease reserveRaftAppend(UUID pool,long physicalGrowthUpperBound,boolean protectedProgress)throws IOException {
        if(physicalGrowthUpperBound<0 || physicalGrowthUpperBound>RatisNodeResourceBudget.MAX_BYTES)throw new IllegalArgumentException("Invalid physical append upper bound");
        if(!protectedProgress)requireOrdinary();
        if((!protectedProgress && !fresh()) || latest==null || !latest.pools().containsKey(pool))throw new IOException("Append filesystem census unavailable");
        var filesystems=new HashSet<>(latest.raftFilesystems());filesystems.addAll(latest.pools().get(pool));long charge=Math.multiplyExact(physicalGrowthUpperBound,filesystems.size());
        long used=protectedProgress?protectedBytes:ordinary,limit=protectedProgress?policy.maximumProtectedPendingBytes():policy.maximumPendingBytes();
        if(pending.stream().filter(lease->lease.protectedProgress==protectedProgress).count()>=4096 || charge>limit-used)throw new IOException("Pending physical append runway exhausted");
        if(!protectedProgress)for(long fs:filesystems){long reserved=reserved(fs);if(physicalGrowthUpperBound>Math.max(0,free.get(fs).usableBytes()-policy.emergencyFreeBytes()-reserved))throw new IOException("Filesystem ordinary runway exhausted");}
        var lease=new AppendLease(filesystems,physicalGrowthUpperBound,charge,protectedProgress);pending.add(lease);if(protectedProgress)protectedBytes+=charge;else ordinary+=charge;return lease;
    }
    public synchronized RecoveryLease reserveRecovery(UUID pool,long declaredScratchBytes)throws IOException {
        if(actualRecovery)throw new IOException("Actual recovery snapshot/flush already owns the node");
        if(!valid || !fresh() || !freeFresh() || latest==null)throw new IOException("Recovery filesystem census unavailable");
        var all=new HashSet<>(latest.raftFilesystems());all.addAll(latest.pools().getOrDefault(pool,Set.of()));
        for(long fs:all){long reserved=reserved(fs);if(declaredScratchBytes>Math.max(0,free.get(fs).usableBytes()-reserved))throw new IOException("Declared recovery headroom unavailable");}
        var lease=reserveRaftAppend(pool,declaredScratchBytes,true);actualRecovery=true;return new RecoveryLease(lease);
    }
    public final class RecoveryLease {
        private final AppendLease disk;private boolean finished;
        private RecoveryLease(AppendLease disk){this.disk=disk;}
        public void actualFinished(){synchronized(RatisDiskRunway.this){if(finished)return;finished=true;disk.actualFinished();actualRecovery=false;RatisDiskRunway.this.notifyAll();}}
    }
    public void awaitActualRecovery(java.time.Duration timeout)throws IOException {long budget=timeout.toNanos(),start=clock.getAsLong();synchronized(this){while(actualRecovery){long left=budget-(clock.getAsLong()-start);if(left<=0)throw new IOException("Actual recovery writer remains active");try{java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this,left);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Recovery drain interrupted",interrupted);}}}}
    public final class AppendLease implements AutoCloseable {
        private final Set<Long> filesystems;private final long bound,charge;private final boolean protectedProgress;private boolean finished;private long finishedEpoch;
        private AppendLease(Set<Long> filesystems,long bound,long charge,boolean protectedProgress){this.filesystems=Set.copyOf(filesystems);this.bound=bound;this.charge=charge;this.protectedProgress=protectedProgress;}
        /** Actual writer exit, including a failed/unknown write, is distinct from observer settlement. */
        public void actualFinished(){synchronized(RatisDiskRunway.this){if(!finished){finished=true;finishedEpoch=observationCut;pending.remove(this);for(long fs:filesystems){var value=debt.computeIfAbsent(fs,unused->new Debt());if(protectedProgress)value.newProtected+=bound;else value.newOrdinary+=bound;}}}}
        /** Cancellation/close cannot prove that disk space is free. Later complete census reconciles it. */
        @Override public void close(){}
        public boolean released(){synchronized(RatisDiskRunway.this){return finished && finishedEpoch<reconciledCut;}}
    }
    private long reserved(long fs){long total=0;for(var lease:pending)if(lease.filesystems.contains(fs))total+=lease.bound;var value=debt.get(fs);if(value!=null)total+=value.oldOrdinary+value.oldProtected+value.newOrdinary+value.newProtected;return total;}
}
