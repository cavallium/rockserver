package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.util.*;

/** Bounded local candidate hints; registry admission separately verifies native proof and qualified home authority. */
public final class RatisNativeParticipantDiscovery {
    /** Null pages mean the local replica is not the ready discovery owner; they perform no admission. */
    @FunctionalInterface public interface Reader { ReplicatedRangeStore.PreparedRecoveryPage read(UUID after,int limit) throws IOException; }
    public record Source(UUID group,Reader reader) { public Source { Objects.requireNonNull(group); Objects.requireNonNull(reader); } }
    private final RatisParticipantRecoveryRegistry registry;
    private final List<Source> sources;
    private final Map<UUID,UUID> cursors=new HashMap<>();
    private int cursor;
    private boolean active,journalsDiscovered;
    public RatisNativeParticipantDiscovery(RatisParticipantRecoveryRegistry registry,List<Source> sources) {
        this.registry=Objects.requireNonNull(registry); this.sources=List.copyOf(sources);
        if (sources.size()>RatisParticipantRecoveryRegistry.MAX_FILES || sources.stream().map(Source::group).distinct().count()!=sources.size()) throw new IllegalArgumentException("Invalid native recovery discovery sources");
    }
    /** At most visits local reads/admissions and visits recovery writes; failures still permit queued recovery to progress. */
    public int cycle(int visits) throws IOException {
        if (visits<1 || visits>RatisParticipantRecoveryRegistry.MAX_VISITS) throw new IllegalArgumentException("Invalid native discovery quantum");
        synchronized (this) { if (active) throw new IOException("Native participant discovery already active"); active=true; }
        try {
            IOException failure=null;
            if (!journalsDiscovered) {
                try { registry.discover(); journalsDiscovered=true; }
                catch (IOException unavailable) { failure=unavailable; }
            }
            for (int n=0;n<Math.min(visits,sources.size());n++) {
                var source=sources.get(cursor); cursor=(cursor+1)%sources.size();
                try {
                    UUID after=cursors.get(source.group()); var page=source.reader().read(after,1);
                    if (page==null) { cursors.remove(source.group()); continue; }
                    if (page.records().size()>1) throw new IOException("Native discovery page exceeded requested bound");
                    if (page.records().isEmpty()) { cursors.remove(source.group()); continue; }
                    var prepared=page.records().getFirst(); UUID transaction=prepared.preparation().transactionId();
                    if (!prepared.preparation().business().groupId().equals(source.group()) || after!=null && transaction.compareTo(after)<=0) throw new IOException("Native discovery candidate binding/cursor changed");
                    cursors.put(source.group(),transaction); // Advance before admission so unavailable candidates cannot starve later IDs.
                    registry.register(source.group(),prepared);
                } catch (IOException unavailable) { if (failure==null) failure=unavailable; else if (failure!=unavailable) failure.addSuppressed(unavailable); }
                catch (RuntimeException invalid) { var unavailable=new IOException("Native participant discovery failed for "+source.group(),invalid); if (failure==null) failure=unavailable; else failure.addSuppressed(unavailable); }
            }
            int steps=0;
            try { steps=registry.cycle(visits); } catch (IOException unavailable) { if (failure==null) failure=unavailable; else if (failure!=unavailable) failure.addSuppressed(unavailable); }
            if (failure!=null) throw failure; return steps;
        } finally { synchronized (this) { active=false; } }
    }
}
