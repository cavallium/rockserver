package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.util.*;

/** Volatile ownership across one node and its declared native pools. No native allocation, callback or native work runs here. */
public final class RatisNodeResourceBudget {
    public static final long MAX_BYTES=1L<<40;
    public static final int MAX_OWNERS=1_000_000,MAX_POOLS=64;
    public enum Kind { ORDINARY, PROTECTED }
    public record Lane(long bytes,int owners) {
        public Lane {if(bytes<1 || bytes>MAX_BYTES || owners<1 || owners>MAX_OWNERS)throw new IllegalArgumentException("Invalid node resource lane limits");}
    }
    public record Limits(Lane ordinary,Lane protectedLane) {public Limits{Objects.requireNonNull(ordinary);Objects.requireNonNull(protectedLane);}}
    public record Usage(long bytes,int owners) {}
    public record Stats(Usage ordinary,Usage protectedLane) {}
    private static final class Counter {long bytes;int owners;Usage snapshot(){return new Usage(bytes,owners);}}
    private static final class Pool {final Limits limits;final Counter ordinary=new Counter(),protectedLane=new Counter();Pool(Limits limits){this.limits=limits;}}
    private final Limits limits;private final Map<UUID,Pool> pools;private final Counter ordinary=new Counter(),protectedLane=new Counter();
    public RatisNodeResourceBudget(Limits limits,Map<UUID,Limits> declaredPools) {
        this.limits=Objects.requireNonNull(limits);if(declaredPools.isEmpty() || declaredPools.size()>MAX_POOLS)throw new IllegalArgumentException("Node resource pools must be declared and bounded");var values=new LinkedHashMap<UUID,Pool>();declaredPools.forEach((id,value)->values.put(Objects.requireNonNull(id),new Pool(Objects.requireNonNull(value))));pools=Collections.unmodifiableMap(values);
    }
    public Set<UUID> poolIds(){return pools.keySet();}
    public synchronized Lease reserve(UUID poolId,Kind kind,long bytes,int owners)throws IOException {
        Objects.requireNonNull(kind);if(bytes<0 || bytes>MAX_BYTES || owners<1 || owners>MAX_OWNERS)throw new IllegalArgumentException("Invalid node resource reservation");var pool=pools.get(Objects.requireNonNull(poolId));if(pool==null)throw new IllegalArgumentException("Undeclared native resource pool");var global=counter(kind);var local=counter(pool,kind);var nodeLimit=lane(limits,kind);var poolLimit=lane(pool.limits,kind);
        if(bytes>nodeLimit.bytes-global.bytes || owners>nodeLimit.owners-global.owners || bytes>poolLimit.bytes-local.bytes || owners>poolLimit.owners-local.owners)throw new IOException("Shared node/pool "+kind+" resource admission exhausted");global.bytes+=bytes;global.owners+=owners;local.bytes+=bytes;local.owners+=owners;return new Lease(pool,kind,bytes,owners);
    }
    public synchronized Stats stats(){return new Stats(ordinary.snapshot(),protectedLane.snapshot());}
    public synchronized Stats stats(UUID poolId){var pool=pools.get(poolId);if(pool==null)throw new IllegalArgumentException("Undeclared native resource pool");return new Stats(pool.ordinary.snapshot(),pool.protectedLane.snapshot());}
    public final class Lease implements AutoCloseable {
        private final Pool pool;private final Kind kind;private long bytes;private int owners;private boolean released;
        private Lease(Pool pool,Kind kind,long bytes,int owners){this.pool=pool;this.kind=kind;this.bytes=bytes;this.owners=owners;}
        /** Only known freed storage/owners may shrink; construction reserves its complete upper bound first. */
        public void shrinkTo(long retainedBytes,int retainedOwners){synchronized(RatisNodeResourceBudget.this){if(released)throw new IllegalStateException("Node resource lease already released");if(retainedBytes<0 || retainedBytes>bytes || retainedOwners<0 || retainedOwners>owners)throw new IllegalArgumentException("Node resource lease may only shrink");refund(bytes-retainedBytes,owners-retainedOwners);bytes=retainedBytes;owners=retainedOwners;}}
        public boolean released(){synchronized(RatisNodeResourceBudget.this){return released;}}
        private void refund(long freedBytes,int freedOwners){var global=counter(kind);var local=counter(pool,kind);global.bytes-=freedBytes;global.owners-=freedOwners;local.bytes-=freedBytes;local.owners-=freedOwners;}
        @Override public void close(){synchronized(RatisNodeResourceBudget.this){if(released)return;refund(bytes,owners);bytes=0;owners=0;released=true;}}
    }
    private Counter counter(Kind kind){return kind==Kind.ORDINARY?ordinary:protectedLane;}
    private static Counter counter(Pool pool,Kind kind){return kind==Kind.ORDINARY?pool.ordinary:pool.protectedLane;}
    private static Lane lane(Limits limits,Kind kind){return kind==Kind.ORDINARY?limits.ordinary:limits.protectedLane;}
}
