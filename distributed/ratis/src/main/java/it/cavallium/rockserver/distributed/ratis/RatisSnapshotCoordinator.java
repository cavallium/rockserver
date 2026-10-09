package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.*;

/** Bounded forward policy over forced snapshot control. COMPLETE never promises a volatile lease remains live. */
public final class RatisSnapshotCoordinator implements AutoCloseable {
    public enum Action { RETRIED_HOME, REGISTERED, WAITING, INSTALLED, HELPED, READY, CAPTURED, CAPTURE_REPORTED, COMPLETED, ABORTED, RELEASED, RETIRED, COMPLETE }
    public record Step(Action action,UUID participant,boolean complete) {}
    private final RatisDurableSnapshot journal;
    private final RatisSnapshotFences homes;
    private final RatisRoutedClient data;
    private final RatisTransactionRecovery.ControlIds controls;
    private final Supplier<UUID> captureIds;
    private final Duration ttl;
    private final List<UUID> groups;
    private int cursor;
    private boolean active,closed,cancelled;

    public RatisSnapshotCoordinator(RatisDurableSnapshot journal,RatisSnapshotFences homes,RatisRoutedClient data,
            RatisTransactionRecovery.ControlIds controls,Supplier<UUID> captureIds,Duration ttl) throws IOException {
        this.journal=Objects.requireNonNull(journal); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data);
        this.controls=Objects.requireNonNull(controls);
        this.captureIds=Objects.requireNonNull(captureIds); this.ttl=Objects.requireNonNull(ttl);
        if (ttl.isNegative() || ttl.isZero() || ttl.compareTo(Duration.ofSeconds(60))>0 || !ttl.equals(Duration.ofMillis(ttl.toMillis()))) throw new IllegalArgumentException("Invalid capture lifetime");
        journal.requireClients(homes,data); groups=journal.snapshot().participants().stream().map(p -> p.route().groupId()).toList();
    }
    /** One active step and at most one state-changing RPC. Rotation happens before participant work. */
    public Step step() throws IOException {
        synchronized (this) { if (closed || active) throw new IOException("Snapshot coordinator closed or step active"); active=true; }
        try { return advance(); } finally { synchronized (this) { active=false; } }
    }
    private Step advance() throws IOException {
        journal.refreshHome(); journal.resolveHomeFromObserved(); var snapshot=journal.snapshot();
        if (snapshot.homeResult()==null && !snapshot.homeSuperseded()) { journal.executeHome(); return result(Action.RETRIED_HOME,null); }
        var home=snapshot.observedHome();
        if (home==null) return transition(snapshot,snapshot.initial(),Action.REGISTERED,null);
        if (home.terminal()) {
            UUID group=select(snapshot,p -> p.retirementProof()==null && (p.releaseReceipt()==null || p.nativeGate()==null || p.nativeGate().released()==null));
            if (group==null) return result(Action.COMPLETE,null);
            if (home.consumed()) { var resolved=journal.resolveRelease(group); if (resolved!=null && resolved.retired()) return result(Action.RETIRED,group); }
            journal.freezeRelease(group);
            var released=journal.executeRelease(group);
            return result(released.retired() ? Action.RETIRED : Action.RELEASED,group);
        }
        for (var progress:snapshot.participants()) {
            UUID group=progress.route().groupId();
            if (progress.captureLost()) return transition(snapshot,home.abort(),Action.ABORTED,group);
            if (home.progress(group).capture()!=null && progress.capture()==null) {
                journal.markCaptureLost(group); return transition(journal.snapshot(),home.abort(),Action.ABORTED,group);
            }
        }
        UUID group=select(snapshot,p -> p.installReceipt()==null || p.nativeGate()==null || p.readyProof()==null || home.progress(p.route().groupId()).ready()==null);
        if (group!=null) {
            var progress=snapshot.participant(group);
            if (progress.installReceipt()==null || progress.nativeGate()==null) {
                journal.freezeInstall(group);
                try { journal.executeInstall(group); }
                catch (RatisSnapshotWaitingException waiting) { return result(Action.WAITING,group); }
                return result(Action.INSTALLED,group);
            }
            if (!journal.verifyReady(group)) return result(data.helpSnapshotFence(home.fence(),group) ? Action.HELPED : Action.WAITING,group);
            var ready=journal.snapshot().participant(group).readyProof(); var reported=home.progress(group).ready();
            if (reported!=null) {
                if (!reported.equals(ready)) throw new IOException("Home READY differs from verified participant cut");
                return result(Action.READY,group);
            }
            return transition(journal.snapshot(),home.ready(group,ready),Action.READY,group);
        }
        group=select(snapshot,p -> p.captureToken()==null || home.progress(p.route().groupId()).capture()==null);
        if (group==null) return transition(snapshot,home.complete(),Action.COMPLETED,null);
        var progress=snapshot.participant(group);
        if (progress.capture()==null) journal.freezeCapture(data.prepareSnapshotCapture(data.snapshotTarget(home.fence(),group),Objects.requireNonNull(captureIds.get()),ttl));
        final SnapshotFenceStatus.Capture token;
        try { token=journal.executeCapture(group); }
        catch (RatisSnapshotLostException lost) { return transition(journal.snapshot(),home.abort(),Action.ABORTED,group); }
        if (progress.captureToken()==null) return result(Action.CAPTURED,group);
        return transition(journal.snapshot(),home.capture(group,token),Action.CAPTURE_REPORTED,group);
    }
    private UUID select(RatisDurableSnapshot.Snapshot snapshot,Predicate<RatisDurableSnapshot.ParticipantProgress> needsWork) {
        for (int i=0;i<groups.size();i++) { UUID group=groups.get(cursor); cursor=(cursor+1)%groups.size(); if (needsWork.test(snapshot.participant(group))) return group; }
        return null;
    }
    private Step transition(RatisDurableSnapshot.Snapshot snapshot,SnapshotFenceStatus next,Action action,UUID group) throws IOException {
        journal.queueHome(homes.prepare(Objects.requireNonNull(controls.nextAfter(snapshot.lastHome())),snapshot.observedHome(),next)); journal.executeHome(); return result(action,group);
    }
    private Step result(Action action,UUID group) throws IOException {
        var snapshot=journal.snapshot(); boolean complete=snapshot.observedHome()!=null && snapshot.observedHome().terminal()
                && snapshot.participants().stream().allMatch(p -> p.releaseReceipt()!=null && p.nativeGate()!=null && p.nativeGate().released()!=null || snapshot.observedHome().consumed() && p.retirementProof()!=null);
        return new Step(action,group,complete);
    }
    /** Borrowed clients, journal and captured pins remain open. Cancel only unreceipted INSTALL authority. */
    @Override public void close() throws IOException {
        synchronized (this) {
            if (active) throw new IOException("Snapshot step/close active; ownership retained");
            if (cancelled) return; closed=true; active=true;
        }
        try {
            var snapshot=journal.snapshot(); IOException failure=null;
            for (var progress:snapshot.participants()) {
                if (!progress.installFrozen() || progress.retirementProof()!=null || progress.installReceipt()!=null || progress.releaseReceipt()!=null || progress.nativeGate()!=null && progress.nativeGate().released()!=null) continue;
                try { journal.cancelInstall(progress.route().groupId()); }
                catch (IOException unavailable) { if (failure==null) failure=unavailable; else failure.addSuppressed(unavailable); }
            }
            if (failure!=null) throw failure;
            synchronized (this) { cancelled=true; }
        } finally { synchronized (this) { active=false; } }
    }
}
