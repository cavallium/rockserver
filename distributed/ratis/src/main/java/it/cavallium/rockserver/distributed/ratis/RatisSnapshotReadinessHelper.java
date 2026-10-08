package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Caller-owned, fair, synchronous recovery discovery; never interprets empty enumeration as READY. */
public final class RatisSnapshotReadinessHelper implements RatisSnapshotCoordinator.ReadinessHelper {
    /** Resolve qualified home authority and force exact requests before performing at most one recovery write. No implicit consumption. */
    @FunctionalInterface public interface Recovery {
        boolean step(UUID group,PreparedParticipant participant) throws IOException;
    }
    private final SnapshotFence fence;
    private final RatisRoutedClient data;
    private final Recovery recovery;
    private final Map<UUID,UUID> cursors=new HashMap<>();
    private boolean active;
    public RatisSnapshotReadinessHelper(SnapshotFence fence,RatisRoutedClient data,Recovery recovery) { this.fence=Objects.requireNonNull(fence); this.data=Objects.requireNonNull(data); this.recovery=Objects.requireNonNull(recovery); }
    @Override public boolean help(SnapshotFence definition,UUID group) throws IOException {
        synchronized (this) { if (active) throw new IOException("Snapshot readiness helper already active"); if (!fence.equals(definition)) throw new IllegalArgumentException("Snapshot helper fence changed"); fence.participant(group); active=true; }
        try {
            UUID after=cursors.get(group); var page=data.snapshotBlockers(fence,group,after,1);
            if (page.records().isEmpty() && after!=null) { cursors.remove(group); page=data.snapshotBlockers(fence,group,null,1); }
            if (page.records().isEmpty()) return false;
            var reference=page.records().getFirst(); cursors.put(group,reference.transaction()); // Advance before fetch/recovery so failures cannot starve later blockers.
            var participant=data.preparedParticipant(group,reference.transaction()); if (participant==null) return false;
            if (!participant.position().equals(reference.prepared()) || !Arrays.equals(participant.preparation().digest(),reference.digest())
                    || !participant.preparation().scope().intersects(fence.participant(group).scope())) throw new IOException("Selected snapshot blocker changed its preparation proof");
            return recovery.step(group,participant);
        } finally { synchronized (this) { active=false; } }
    }
}
