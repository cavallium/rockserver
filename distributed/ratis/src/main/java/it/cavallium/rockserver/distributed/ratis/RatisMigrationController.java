package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.util.*;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Finite recovery bulk ownership and shared migration rates. No protected control credits are borrowed. */
public final class RatisMigrationController {
    public record Policy(long nodeBytesPerSecond,long poolBytesPerSecond,long burstBytes,long recoveryBytesPerSecond,
                         long nodeRecoveryBytes,long poolRecoveryBytes,int recoveryOwners,int maximumGroups) {
        public Policy {
            if(nodeBytesPerSecond<1 || poolBytesPerSecond<1 || burstBytes<1 || recoveryBytesPerSecond<1
                    || recoveryBytesPerSecond>Math.min(nodeBytesPerSecond,poolBytesPerSecond) || burstBytes>RatisNodeResourceBudget.MAX_BYTES
                    || nodeBytesPerSecond>RatisNodeResourceBudget.MAX_BYTES || poolBytesPerSecond>RatisNodeResourceBudget.MAX_BYTES
                    || nodeRecoveryBytes<burstBytes || poolRecoveryBytes<burstBytes || nodeRecoveryBytes>RatisNodeResourceBudget.MAX_BYTES
                    || poolRecoveryBytes>nodeRecoveryBytes || recoveryOwners<1 || recoveryOwners>1024 || maximumGroups<1 || maximumGroups>8192)
                throw new IllegalArgumentException("Invalid migration policy");
        }
        /** Explicit administrative ceilings, not measured bandwidth. */
        public static Policy defaults(){return new Policy(128L<<20,64L<<20,65L<<20,1L<<20,512L<<20,256L<<20,8,4096);}
    }
    private static final class Scope {
        double tokens;long updated,bytes,sequence;int owners;final LinkedHashMap<UUID,Flow> flows=new LinkedHashMap<>();
        Scope(long burst,long now){tokens=burst;updated=now;}
    }
    private static final class Flow {final UUID pool;long served,requested,last,sequence;boolean recovery;Flow(UUID pool,long baseline,long now){this.pool=pool;served=baseline;last=now;}}
    public record Usage(long recoveryBytes,int recoveryOwners) {}
    private final Policy policy;private final LongSupplier clock;private final Supplier<RatisDiskRunway.State> pressure;
    private final Scope node;private final Map<UUID,Scope> pools=new HashMap<>();
    public RatisMigrationController(Policy policy,Set<UUID> pools,LongSupplier clock,Supplier<RatisDiskRunway.State> pressure){
        this.policy=Objects.requireNonNull(policy);this.clock=Objects.requireNonNull(clock);this.pressure=Objects.requireNonNull(pressure);
        long now=clock.getAsLong();node=new Scope(policy.burstBytes(),now);for(var id:pools)this.pools.put(id,new Scope(policy.burstBytes(),now));
        if(pools.isEmpty() || pools.size()>RatisNodeResourceBudget.MAX_POOLS)throw new IllegalArgumentException("Invalid migration pools");
    }
    public synchronized Usage usage(){return new Usage(node.bytes,node.owners);}
    public synchronized RatisProofQueryService.ResourceLease reserveRecovery(UUID pool,long bytes)throws IOException{
        var local=pool(pool);if(bytes<0 || bytes>policy.nodeRecoveryBytes()-node.bytes || bytes>policy.poolRecoveryBytes()-local.bytes
                || node.owners>=policy.recoveryOwners() || local.owners>=policy.recoveryOwners())throw new IOException("Recovery bulk ownership exhausted");
        node.bytes+=bytes;local.bytes+=bytes;node.owners++;local.owners++;
        return new RatisProofQueryService.ResourceLease(){boolean closed;public void close(){synchronized(RatisMigrationController.this){if(closed)return;closed=true;node.bytes-=bytes;local.bytes-=bytes;node.owners--;local.owners--;}}};
    }
    /** Nonblocking and before payload materialization. Denial preserves heartbeats and other lanes. */
    public synchronized boolean acquire(UUID group,UUID pool,long bytes,boolean recovery)throws IOException{
        if(bytes<1 || bytes>policy.burstBytes())throw new IOException("Migration quantum exceeds configured burst");
        var local=pool(pool);long now=clock.getAsLong();var state=pressure.get();
        double reduction=state==RatisDiskRunway.State.CLEAR?1:state==RatisDiskRunway.State.WARNING?0.25:0;
        long nodeRate=Math.max(policy.recoveryBytesPerSecond(),(long)(policy.nodeBytesPerSecond()*reduction));
        long poolRate=Math.max(policy.recoveryBytesPerSecond(),(long)(policy.poolBytesPerSecond()*reduction));
        refill(node,nodeRate,now);for(var value:pools.values())refill(value,poolRate,now);
        if(!recovery && reduction==0){forget(group,pool);return false;}
        enqueue(node,group,pool,bytes,recovery,now);
        if(!group.equals(first(reduction>0)) || node.tokens<bytes || local.tokens<bytes)return false;
        node.tokens-=bytes;local.tokens-=bytes;finish(node,group,bytes);return true;
    }
    public synchronized void forget(UUID group,UUID pool){pool(pool);var global=node.flows.get(group);if(global!=null)global.requested=0;}
    private Scope pool(UUID pool){var local=pools.get(pool);if(local==null)throw new IllegalArgumentException("Undeclared migration pool");return local;}
    private void refill(Scope scope,long rate,long now){long elapsed=Math.max(0,now-scope.updated);scope.tokens=Math.min(policy.burstBytes(),scope.tokens+elapsed/1e9*rate);scope.updated=now;}
    private void enqueue(Scope scope,UUID group,UUID pool,long bytes,boolean recovery,long now)throws IOException{
        scope.flows.entrySet().removeIf(entry->now-entry.getValue().last>10_000_000_000L);
        scope.flows.values().forEach(value->{if(now-value.last>1_000_000_000L)value.requested=0;});
        var flow=scope.flows.get(group);
        if(flow==null){if(scope.flows.size()>=policy.maximumGroups())throw new IOException("Migration fairness group bound exhausted");long baseline=scope.flows.values().stream().mapToLong(value->value.served).min().orElse(0);flow=new Flow(pool,baseline,now);scope.flows.put(group,flow);}
        if(!flow.pool.equals(pool))throw new IOException("Migration group changed physical pool");if(flow.requested==0)flow.sequence=++scope.sequence;flow.last=now;flow.requested=bytes;flow.recovery=recovery;
    }
    private UUID first(boolean ordinaryAllowed){return node.flows.entrySet().stream().filter(entry->{var flow=entry.getValue();return flow.requested>0 && (ordinaryAllowed || flow.recovery);}).min(Comparator.<Map.Entry<UUID,Flow>>comparingLong(entry->entry.getValue().served+entry.getValue().requested).thenComparingLong(entry->entry.getValue().sequence)).map(Map.Entry::getKey).orElse(null);}
    /** Refund the unused upper bound only after the immutable payload's actual size is known. */
    public synchronized void refundUnused(UUID group,UUID pool,long bytes){if(bytes<0 || bytes>policy.burstBytes())throw new IllegalArgumentException("Invalid migration refund");node.tokens=Math.min(policy.burstBytes(),node.tokens+bytes);var local=pool(pool);local.tokens=Math.min(policy.burstBytes(),local.tokens+bytes);var flow=node.flows.get(group);if(flow!=null)flow.served=Math.max(0,flow.served-bytes);}
    private void finish(Scope scope,UUID group,long bytes){var flow=scope.flows.get(group);flow.served+=bytes;flow.requested=0;
        // Normalize the virtual clock before overflow; no rate tokens are refunded.
        if(flow.served>Long.MAX_VALUE/2){long minimum=scope.flows.values().stream().mapToLong(value->value.served).min().orElse(0);scope.flows.values().forEach(value->value.served-=minimum);}
    }
}
