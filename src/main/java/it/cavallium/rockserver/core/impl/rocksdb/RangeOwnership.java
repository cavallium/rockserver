package it.cavallium.rockserver.core.impl.rocksdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Metadata-only interval ownership guard, with one instance shared by all ranges in a native pool.
 * This is neither durable metadata nor the catalogue/Raft split, merge, or installation protocol.
 * Callers persist the required markers and perform native I/O while retaining logical leases.
 * Domain identities must denote compatible immutable schema and storage policies in this pool.
 */
public final class RangeOwnership {
    public enum State { ACTIVE, FENCED, INSTALLING, RETIRED }

    public record Owner(UUID groupId, long epoch) {
        public Owner {
            Objects.requireNonNull(groupId, "groupId");
            if (epoch < 0) throw new IllegalArgumentException("Owner epoch must be nonnegative");
        }
    }

    /** Immutable physical [lower,upper) bounds; null denotes the respective infinity. */
    public record Interval(UUID domainId, Set<UUID> columns, byte[] lowerInclusive, byte[] upperExclusive) {
        public Interval {
            Objects.requireNonNull(domainId, "domainId");
            columns = Set.copyOf(columns);
            if (lowerInclusive != null && upperExclusive != null
                    && Arrays.compareUnsigned(lowerInclusive, upperExclusive) >= 0) {
                throw new IllegalArgumentException("Interval must have increasing unsigned bounds");
            }
            lowerInclusive = lowerInclusive == null ? null : lowerInclusive.clone();
            upperExclusive = upperExclusive == null ? null : upperExclusive.clone();
        }
        @Override public byte[] lowerInclusive() { return lowerInclusive == null ? null : lowerInclusive.clone(); }
        @Override public byte[] upperExclusive() { return upperExclusive == null ? null : upperExclusive.clone(); }
        @Override public boolean equals(Object other) {
            return other instanceof Interval interval && domainId.equals(interval.domainId)
                    && columns.equals(interval.columns) && Arrays.equals(lowerInclusive, interval.lowerInclusive)
                    && Arrays.equals(upperExclusive, interval.upperExclusive);
        }
        @Override public int hashCode() {
            return 31 * (31 * Objects.hash(domainId, columns) + Arrays.hashCode(lowerInclusive))
                    + Arrays.hashCode(upperExclusive);
        }
    }

    public record Ownership(Owner owner, Interval interval, State state) {}

    /** Schema policy must reject boundaries cutting physical buckets or colocation prefixes. */
    @FunctionalInterface
    public interface BoundaryPolicy {
        boolean permits(Interval parent, byte[] splitKey);
    }

    private final Map<UUID, Ownership> owners = new HashMap<>();
    private final Map<UUID, Lease> leases = new HashMap<>();
    private final Map<UUID, SchemaTransition> schemaTransitions = new HashMap<>();

    /** Trusted native schema intent, permitting only its recorded source and target column sets. */
    public static final class SchemaTransition {
        private final UUID changeId, domainId;
        private final Set<UUID> sourceColumns, targetColumns;
        private SchemaTransition(UUID changeId, UUID domainId, Set<UUID> source, Set<UUID> target) {
            this.changeId=changeId; this.domainId=domainId; sourceColumns=source; targetColumns=target;
        }
    }

    public synchronized SchemaTransition beginSchemaTransition(UUID changeId, UUID domainId,
            Set<UUID> sourceColumns, Set<UUID> targetColumns) {
        Objects.requireNonNull(changeId); Objects.requireNonNull(domainId);
        var source=Set.copyOf(sourceColumns); var target=Set.copyOf(targetColumns);
        var removed=new java.util.HashSet<>(source); removed.removeAll(target);
        var added=new java.util.HashSet<>(target); added.removeAll(source);
        if (removed.size()+added.size()!=1) throw new IllegalArgumentException("Schema transition changes exactly one column");
        var previous=schemaTransitions.get(domainId);
        if (previous!=null) {
            if (!previous.changeId.equals(changeId) || !previous.sourceColumns.equals(source) || !previous.targetColumns.equals(target))
                throw new IllegalStateException("Another schema transition owns this domain");
            return previous;
        }
        for (var other:owners.values()) if (other.state()!=State.RETIRED && other.interval().domainId().equals(domainId)
                && !other.interval().columns().equals(source) && !other.interval().columns().equals(target))
            throw new IllegalArgumentException("Schema transition differs from existing domain columns");
        var transition=new SchemaTransition(changeId,domainId,source,target);
        schemaTransitions.put(domainId,transition); return transition;
    }

    /** Recovery registration uses the same durable intent as live installation. */
    public synchronized Ownership registerSchema(Owner owner, Interval interval, State state, SchemaTransition transition) {
        Objects.requireNonNull(state); Objects.requireNonNull(interval); requireSchemaTransition(transition,interval);
        if (state!=State.FENCED && state!=State.RETIRED) throw new IllegalArgumentException("Schema recovery owner must be fenced");
        requireNew(owner); requireUnleased(interval);
        if (state!=State.RETIRED) requireUnowned(interval,Set.of());
        var result=new Ownership(owner,interval,state); owners.put(owner.groupId(),result); return result;
    }

    public synchronized void validateSchemaReplacement(Owner owner, Interval expectedSource, Interval target, SchemaTransition transition) {
        requireSchemaTransition(transition,expectedSource); requireSchemaTransition(transition,target);
        if (!expectedSource.columns().equals(transition.sourceColumns) || !target.columns().equals(transition.targetColumns)
                || !Arrays.equals(expectedSource.lowerInclusive,target.lowerInclusive) || !Arrays.equals(expectedSource.upperExclusive,target.upperExclusive))
            throw new IllegalArgumentException("Schema install changes bounds or column intent");
        var current=get(owner); requireFenced(current);
        if (!current.interval().equals(expectedSource)) throw new IllegalStateException("Schema source ownership changed");
        requireUnleased(expectedSource); requireUnleased(target); requireUnowned(target,Set.of(owner.groupId()));
    }

    public synchronized Ownership replaceSchema(Owner owner, Interval expectedSource, Interval target, SchemaTransition transition) {
        validateSchemaReplacement(owner,expectedSource,target,transition);
        var result=new Ownership(owner,target,State.FENCED); owners.put(owner.groupId(),result); return result;
    }

    /** Reserve the union of old and new columns through native DDL and durable generation publication. */
    public synchronized SchemaLease claimSchemaReplacement(Owner owner, Interval expectedSource, Interval target, SchemaTransition transition) {
        validateSchemaReplacement(owner,expectedSource,target,transition);
        var protectedColumns=new java.util.HashSet<>(expectedSource.columns()); protectedColumns.addAll(target.columns());
        var installing=new Ownership(owner,expectedSource,State.INSTALLING);
        var reservation=new Lease(installing,true,new Interval(expectedSource.domainId(),protectedColumns,expectedSource.lowerInclusive,expectedSource.upperExclusive));
        owners.put(owner.groupId(),installing); leases.put(owner.groupId(),reservation);
        return new SchemaLease(reservation,target,transition);
    }

    public final class SchemaLease implements AutoCloseable {
        private final Lease reservation;
        private final Interval target;
        private final SchemaTransition transition;
        private boolean published;
        private SchemaLease(Lease reservation, Interval target, SchemaTransition transition) {
            this.reservation=reservation; this.target=target; this.transition=transition;
        }
        /** Invoke only after the target generation and completed marker are durably installed. */
        public Ownership publish() {
            synchronized (RangeOwnership.this) {
                requireSchemaTransition(transition,target);
                var owner=reservation.ownership.owner();
                if (reservation.closed || leases.get(owner.groupId())!=reservation) throw new IllegalStateException("Schema lease is closed");
                var current=get(owner);
                if (current.state()!=State.INSTALLING) throw new IllegalStateException("Schema lease lost install ownership");
                if (published) return current;
                var result=new Ownership(owner,target,State.INSTALLING); owners.put(owner.groupId(),result); published=true; return result;
            }
        }
        @Override public void close() { reservation.close(); }
    }

    public synchronized void completeSchemaTransition(SchemaTransition transition) {
        requireSchemaTransition(transition,null);
        for (var current:owners.values()) if (current.state()!=State.RETIRED && current.interval().domainId().equals(transition.domainId)) {
            if (!current.interval().columns().equals(transition.targetColumns)) throw new IllegalStateException("Schema source owner remains");
            requireUnleased(current.interval());
        }
        schemaTransitions.remove(transition.domainId);
    }

    private void requireSchemaTransition(SchemaTransition transition, Interval interval) {
        Objects.requireNonNull(transition);
        if (schemaTransitions.get(transition.domainId)!=transition) throw new IllegalStateException("Unknown or completed schema transition");
        if (interval!=null && (!interval.domainId().equals(transition.domainId)
                || !interval.columns().equals(transition.sourceColumns) && !interval.columns().equals(transition.targetColumns)))
            throw new IllegalArgumentException("Interval is outside recorded schema transition");
    }

    public synchronized Ownership register(Owner owner, Interval interval, State state) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(interval, "interval");
        Objects.requireNonNull(state, "state");
        if (state == State.INSTALLING) throw new IllegalArgumentException("Installing requires an exclusive lease");
        requireNew(owner);
        requireUnleased(interval);
        requireCompatibleDomain(interval);
        if (state != State.RETIRED) requireUnowned(interval, Set.of());
        var ownership = new Ownership(owner, interval, state);
        owners.put(owner.groupId(), ownership);
        return ownership;
    }

    public synchronized Ownership get(Owner expected) {
        Objects.requireNonNull(expected, "expected");
        Ownership ownership = owners.get(expected.groupId());
        if (ownership == null || !ownership.owner().equals(expected)) {
            throw new IllegalStateException("Unknown or stale owner token");
        }
        return ownership;
    }

    public synchronized Ownership fence(Owner expected) {
        return transition(expected, State.ACTIVE, State.FENCED);
    }

    public synchronized Ownership activate(Owner expected) {
        return transition(expected, State.FENCED, State.ACTIVE);
    }

    public synchronized Ownership retire(Owner expected) {
        return transition(expected, State.FENCED, State.RETIRED);
    }

    private Ownership transition(Owner expected, State from, State to) {
        Ownership previous = get(expected);
        requireUnleased(previous.interval());
        if (previous.state() != from) throw new IllegalStateException("Unexpected owner state");
        var next = new Ownership(expected, previous.interval(), to);
        owners.put(expected.groupId(), next);
        return next;
    }

    /** Atomically retires a fenced parent and seeds two fenced children without touching user keys. */
    public List<Ownership> split(Owner parent, byte[] splitKey, Owner left, Owner right, BoundaryPolicy policy) {
        byte[] key = Objects.requireNonNull(splitKey, "splitKey").clone();
        Objects.requireNonNull(policy, "policy");
        Ownership observed = get(parent);
        // Admission callbacks execute without holding the metadata lock.
        if (!policy.permits(observed.interval(), key.clone())) {
            throw new IllegalArgumentException("Split is not an admitted physical boundary");
        }
        synchronized (this) {
            Ownership previous = get(parent);
            requireFenced(previous);
            requireUnleased(previous.interval());
            requireNew(left);
            requireNew(right);
            if (left.groupId().equals(right.groupId())) throw new IllegalArgumentException("Children need distinct groups");
            Interval interval = previous.interval();
            if ((interval.lowerInclusive != null && Arrays.compareUnsigned(key, interval.lowerInclusive) <= 0)
                    || (interval.upperExclusive != null && Arrays.compareUnsigned(key, interval.upperExclusive) >= 0)) {
                throw new IllegalArgumentException("Split key must lie strictly inside parent");
            }
            var lower = new Interval(interval.domainId(), interval.columns(), interval.lowerInclusive, key);
            var upper = new Interval(interval.domainId(), interval.columns(), key, interval.upperExclusive);
            requireUnowned(lower, Set.of(parent.groupId()));
            requireUnowned(upper, Set.of(parent.groupId()));
            var leftOwnership = new Ownership(left, lower, State.FENCED);
            var rightOwnership = new Ownership(right, upper, State.FENCED);
            owners.put(parent.groupId(), new Ownership(parent, interval, State.RETIRED));
            owners.put(left.groupId(), leftOwnership);
            owners.put(right.groupId(), rightOwnership);
            return List.of(leftOwnership, rightOwnership);
        }
    }

    /** Atomically transfers two adjacent compatible fenced intervals to one new fenced group. */
    public synchronized Ownership merge(Owner left, Owner right, Owner merged) {
        Ownership lower = get(left);
        Ownership upper = get(right);
        requireFenced(lower);
        requireFenced(upper);
        requireUnleased(lower.interval());
        requireUnleased(upper.interval());
        requireNew(merged);
        Interval a = lower.interval();
        Interval b = upper.interval();
        if (!a.domainId().equals(b.domainId()) || !a.columns().equals(b.columns())
                || a.upperExclusive == null || b.lowerInclusive == null
                || !Arrays.equals(a.upperExclusive, b.lowerInclusive)) {
            throw new IllegalArgumentException("Merge requires adjacent compatible intervals in lower/upper order");
        }
        var interval = new Interval(a.domainId(), a.columns(), a.lowerInclusive, b.upperExclusive);
        requireUnowned(interval, Set.of(left.groupId(), right.groupId()));
        var result = new Ownership(merged, interval, State.FENCED);
        owners.put(left.groupId(), new Ownership(left, a, State.RETIRED));
        owners.put(right.groupId(), new Ownership(right, b, State.RETIRED));
        owners.put(merged.groupId(), result);
        return result;
    }

    /** Reserve a fenced interval exclusively; release only after native work and marker durability finish. */
    public synchronized Lease claimInstall(Owner expected) {
        Ownership ownership = get(expected);
        requireFenced(ownership);
        return claim(ownership, true);
    }

    /** Reserve cleanup of an inactive interval, rejecting any successor/live owner of its physical keys. */
    public synchronized Lease claimCleanup(Owner expected) {
        Ownership ownership = get(expected);
        if (ownership.state() != State.FENCED && ownership.state() != State.RETIRED) {
            throw new IllegalStateException("Cleanup requires inactive ownership");
        }
        return claim(ownership, false);
    }

    private Lease claim(Ownership ownership, boolean installing) {
        requireUnleased(ownership.interval());
        requireUnowned(ownership.interval(), Set.of(ownership.owner().groupId()));
        if (installing) {
            ownership = new Ownership(ownership.owner(), ownership.interval(), State.INSTALLING);
            owners.put(ownership.owner().groupId(), ownership);
        }
        var lease = new Lease(ownership, installing);
        leases.put(ownership.owner().groupId(), lease);
        return lease;
    }

    /** Logical reservation, not a held Java lock. Closing leaves install targets fenced, never active. */
    public final class Lease implements AutoCloseable {
        private final Ownership ownership;
        private final boolean installing;
        private final Interval protectedInterval;
        private boolean closed;

        private Lease(Ownership ownership, boolean installing) {
            this(ownership,installing,ownership.interval());
        }

        private Lease(Ownership ownership, boolean installing, Interval protectedInterval) {
            this.ownership=ownership; this.installing=installing; this.protectedInterval=protectedInterval;
        }

        public Ownership ownership() { return ownership; }

        @Override public void close() {
            synchronized (RangeOwnership.this) {
                if (closed) return;
                if (installing) {
                    owners.put(ownership.owner().groupId(),
                            new Ownership(ownership.owner(), get(ownership.owner()).interval(), State.FENCED));
                }
                leases.remove(ownership.owner().groupId());
                closed = true;
            }
        }
    }

    private void requireFenced(Ownership ownership) {
        if (ownership.state() != State.FENCED) throw new IllegalStateException("Owner must be fenced");
    }

    private void requireNew(Owner owner) {
        Objects.requireNonNull(owner, "owner");
        if (owners.containsKey(owner.groupId())) throw new IllegalStateException("Group identity already registered");
    }

    private void requireCompatibleDomain(Interval interval) {
        for (Ownership other : owners.values()) {
            if (other.state()!=State.RETIRED && other.interval().domainId().equals(interval.domainId())
                    && !other.interval().columns().equals(interval.columns())) {
                throw new IllegalArgumentException("Domain columns cannot change within this guard");
            }
        }
    }

    private void requireUnleased(Interval interval) {
        for (Lease lease : leases.values()) {
            if (overlaps(interval, lease.protectedInterval)) {
                throw new IllegalStateException("Interval is exclusively leased");
            }
        }
    }

    private void requireUnowned(Interval interval, Set<UUID> excluded) {
        for (Ownership other : owners.values()) {
            if (!excluded.contains(other.owner().groupId()) && other.state() != State.RETIRED
                    && overlaps(interval, other.interval())) {
                throw new IllegalStateException("Interval overlaps another owner");
            }
        }
    }

    private static boolean overlaps(Interval a, Interval b) {
        boolean sameKeys = a.domainId().equals(b.domainId())
                || a.columns().stream().anyMatch(b.columns()::contains);
        return sameKeys && (a.upperExclusive == null || b.lowerInclusive == null
                || Arrays.compareUnsigned(a.upperExclusive, b.lowerInclusive) > 0)
                && (b.upperExclusive == null || a.lowerInclusive == null
                || Arrays.compareUnsigned(b.upperExclusive, a.lowerInclusive) > 0);
    }
}
