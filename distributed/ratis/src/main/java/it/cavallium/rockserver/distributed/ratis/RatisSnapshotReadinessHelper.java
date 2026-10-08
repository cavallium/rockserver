package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Bound server recovery discovery; never interprets empty enumeration as READY. */
public final class RatisSnapshotReadinessHelper {
    /** Resolve qualified home authority and force exact requests before performing at most one recovery write. No implicit consumption. */
    @FunctionalInterface public interface Recovery {
        boolean step(UUID group,PreparedParticipant participant) throws IOException;
    }
    @FunctionalInterface public interface BlockerReader { RatisSnapshotBlockers read(UUID after,int limit) throws IOException; }
    @FunctionalInterface public interface ParticipantReader { PreparedParticipant read(UUID transaction) throws IOException; }
    private final SnapshotFence fence;
    private final UUID group;
    private final BlockerReader blockers;
    private final ParticipantReader participants;
    private final Recovery recovery;
    private UUID cursor;
    private boolean active;
    public RatisSnapshotReadinessHelper(SnapshotFence fence,UUID group,BlockerReader blockers,ParticipantReader participants,Recovery recovery) {
        this.fence=Objects.requireNonNull(fence); this.group=Objects.requireNonNull(group); fence.participant(group);
        this.blockers=Objects.requireNonNull(blockers); this.participants=Objects.requireNonNull(participants); this.recovery=Objects.requireNonNull(recovery);
    }
    public boolean help() throws IOException {
        synchronized (this) { if (active) throw new IOException("Snapshot readiness helper already active"); active=true; }
        try {
            UUID after=cursor; var page=page(after);
            if (page.records().isEmpty() && after!=null) { cursor=null; page=page(null); }
            if (page.records().isEmpty()) return false;
            var reference=page.records().getFirst(); cursor=reference.transaction(); // Rotate before fetch/recovery so failures cannot starve later blockers.
            var participant=participants.read(reference.transaction()); if (participant==null) return false;
            if (!participant.preparation().business().groupId().equals(group) || !participant.preparation().transactionId().equals(reference.transaction())
                    || !participant.position().equals(reference.prepared()) || !Arrays.equals(participant.preparation().digest(),reference.digest())
                    || !participant.preparation().scope().intersects(fence.participant(group).scope())) throw new IOException("Selected snapshot blocker changed its preparation proof");
            return recovery.step(group,participant);
        } finally { synchronized (this) { active=false; } }
    }
    private RatisSnapshotBlockers page(UUID after) throws IOException {
        var page=blockers.read(after,1);
        if (page==null) throw new IOException("Snapshot blocker reader returned no bound page");
        if (!page.binding().equals(ScopedTransactionRpc.SnapshotBinding.of(fence)) || !page.group().equals(group) || page.records().size()>1
                || after!=null && !page.records().isEmpty() && page.records().getFirst().transaction().compareTo(after)<=0) throw new IOException("Snapshot blocker page changed binding/bound/cursor");
        return page;
    }
}
