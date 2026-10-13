package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.*;

/** Bounded forward policy over forced snapshot control. COMPLETE never promises a volatile lease remains live. */
public final class RatisSnapshotCoordinator implements AutoCloseable {
    public enum StopAt { GATES_READY, CAPTURES_REPORTED, RELEASED }
    public enum Action { GATES_READY, RETRIED_HOME, REGISTERED, WAITING, INSTALLED, HELPED, READY, CAPTURED, CAPTURE_REPORTED, COMPLETED, ABORTED, RELEASED, RETIRED, HOME_RETIRED, COMPLETE }
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
        RatisSnapshotCaptureRequest.requireTtl(ttl,journal.mode());
        journal.requireClients(homes,data); groups=journal.snapshot().participants().stream().map(p -> p.route().groupId()).toList();
    }
    /** One active step and at most one state-changing RPC. Rotation happens before participant work. */
    public Step step() throws IOException { return step(StopAt.RELEASED); }
    /** Backup barriers stop before capture or HOME completion; COMPLETE would authorize independent gate release. */
    public Step step(StopAt stop) throws IOException {
        Objects.requireNonNull(stop);
        synchronized (this) { if (closed || active) throw new IOException("Snapshot coordinator closed or step active"); active=true; }
        try { if (journal.snapshot().homeRetirementProof()!=null) { if(stop!=StopAt.RELEASED)throw new IOException("Retired snapshot cannot retain backup gates");return new Step(Action.HOME_RETIRED,null,true); } return advance(stop); }
        catch (RatisSnapshotHomeRetiredException retired) { if(stop!=StopAt.RELEASED)throw new IOException("Retired snapshot cannot retain backup gates",retired);return new Step(Action.HOME_RETIRED,null,true); } finally { synchronized (this) { active=false; } }
    }
    private Step advance(StopAt stop) throws IOException {
        journal.refreshHome(); journal.resolveHomeFromObserved(); var snapshot=journal.snapshot();
        if (snapshot.homeResult()==null && !snapshot.homeSuperseded()) { journal.executeHome(); return result(Action.RETRIED_HOME,null); }
        var home=snapshot.observedHome();
        if (home==null) return transition(snapshot,snapshot.initial(),Action.REGISTERED,null);
        if (home.terminal()) {
            if(stop!=StopAt.RELEASED && home.phase()==SnapshotFenceStatus.Phase.COMPLETE)throw new IOException("Snapshot completed before held backup barrier");
            UUID group=select(snapshot,p -> p.retirementProof()==null && (p.releaseReceipt()==null || p.nativeGate()==null || p.nativeGate().released()==null));
            if (group==null) return result(Action.COMPLETE,null);
            if (home.consumed()) { var resolved=journal.resolveRelease(group); if (resolved!=null && resolved.retired()) return result(Action.RETIRED,group); }
            journal.freezeRelease(group);
            var released=journal.executeRelease(group);
            return result(released.retired() ? Action.RETIRED : Action.RELEASED,group);
        }
        if (snapshot.releaseRequested()) return transition(snapshot,home.abort(),Action.ABORTED,null);
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
        if(stop==StopAt.GATES_READY)return result(Action.GATES_READY,null);
        group=select(snapshot,p -> p.captureToken()==null || home.progress(p.route().groupId()).capture()==null);
        if (group==null) return stop==StopAt.CAPTURES_REPORTED ? result(Action.CAPTURE_REPORTED,null) : transition(snapshot,home.complete(),Action.COMPLETED,null);
        var progress=snapshot.participant(group);
        if (progress.capture()==null) {
            var target=data.snapshotTarget(home.fence(),group); var id=Objects.requireNonNull(captureIds.get());
            journal.freezeCapture(journal.mode()==RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED ? data.prepareWritableSnapshotCapture(target,id,ttl) : data.prepareSnapshotCapture(target,id,ttl));
        }
        final SnapshotFenceStatus.Capture token;
        try { token=journal.executeCapture(group); }
        catch (RatisSnapshotLostException lost) { return transition(journal.snapshot(),home.abort(),Action.ABORTED,group); }
        if (progress.captureToken()==null) return result(Action.CAPTURED,group);
        return transition(journal.snapshot(),home.capture(group,token),Action.CAPTURE_REPORTED,group);
    }
    /** Reports only an already forced global cohort vector; never creates or executes a capture. */
    public Step reportExternalCaptures(SnapshotFenceStatus expected) throws IOException {
        Objects.requireNonNull(expected);
        synchronized(this){if(closed || active)throw new IOException("Snapshot coordinator closed or step active");active=true;}
        try {
            journal.refreshHome();journal.resolveHomeFromObserved();var snapshot=journal.snapshot();var home=snapshot.observedHome();
            if(expected.phase()!=SnapshotFenceStatus.Phase.INSTALLING || !expected.allReady() || !expected.fence().equals(snapshot.initial().fence())
                    || expected.progress().stream().anyMatch(p->p.capture()==null) || home==null || home.phase()==SnapshotFenceStatus.Phase.ABORT)
                throw new IOException("External reporting lacks the original global capture vector");
            UUID missing=null;
            for(var progress:expected.progress()) {
                var local=snapshot.participant(progress.groupId());var observed=home.progress(progress.groupId());
                if(!Objects.equals(progress.ready(),observed.ready()) || !Objects.equals(progress.ready(),local.readyProof())
                        || !Objects.equals(progress.capture(),local.captureToken()) || local.captureLost()
                        || observed.capture()!=null && !progress.capture().equals(observed.capture()))throw new IOException("External reporting changed READY/capture vector");
                if(observed.capture()==null && missing==null)missing=progress.groupId();
            }
            if(home.phase()==SnapshotFenceStatus.Phase.COMPLETE) {if(missing!=null)throw new IOException("COMPLETE lacks original capture vector");return result(Action.CAPTURE_REPORTED,null);}
            if(missing==null)return result(Action.CAPTURE_REPORTED,null);
            return transition(snapshot,home.capture(missing,expected.progress(missing).capture()),Action.CAPTURE_REPORTED,missing);
        } finally {synchronized(this){active=false;}}
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
            var snapshot=journal.snapshot(); if (snapshot.homeRetirementProof()!=null) { synchronized (this) { cancelled=true; } return; }
            IOException failure=null;
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
