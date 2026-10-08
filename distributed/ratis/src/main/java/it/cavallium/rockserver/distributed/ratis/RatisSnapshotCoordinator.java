package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.*;

/** Bounded forward policy over forced snapshot control. COMPLETE never promises a volatile lease remains live. */
public final class RatisSnapshotCoordinator implements AutoCloseable {
    @FunctionalInterface public interface GateAuthority {
        Authorization authorize(RatisSnapshotRecord request) throws IOException;
    }
    /** Helpers may perform at most one prepared-transaction recovery write per call. */
    @FunctionalInterface public interface ReadinessHelper {
        boolean help(SnapshotFence fence,UUID group) throws IOException;
    }
    public record Authorization(RatisSnapshotRecord request,BooleanSupplier valid,BooleanSupplier drained,AutoCloseable owner) implements AutoCloseable {
        public Authorization { Objects.requireNonNull(request); Objects.requireNonNull(valid); Objects.requireNonNull(drained); Objects.requireNonNull(owner); }
        @Override public void close() throws Exception { owner.close(); }
    }
    public enum Action { RETRIED_HOME, REGISTERED, WAITING, INSTALLED, HELPED, READY, CAPTURED, CAPTURE_REPORTED, COMPLETED, ABORTED, RELEASED, COMPLETE }
    public record Step(Action action,UUID participant,boolean complete) {}
    private final RatisDurableSnapshot journal;
    private final RatisSnapshotFences homes;
    private final RatisRoutedClient data;
    private final RatisTransactionRecovery.ControlIds controls;
    private final GateAuthority authority;
    private final ReadinessHelper helper;
    private final Supplier<UUID> captureIds;
    private final Duration ttl;
    private final List<UUID> groups;
    private final Map<UUID,Authorization> installs=new HashMap<>();
    private int cursor;
    private boolean active,closed;

    public RatisSnapshotCoordinator(RatisDurableSnapshot journal,RatisSnapshotFences homes,RatisRoutedClient data,
            RatisTransactionRecovery.ControlIds controls,GateAuthority authority,ReadinessHelper helper,Supplier<UUID> captureIds,Duration ttl) throws IOException {
        this.journal=Objects.requireNonNull(journal); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data);
        this.controls=Objects.requireNonNull(controls); this.authority=Objects.requireNonNull(authority); this.helper=Objects.requireNonNull(helper);
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
            closeInstalls();
            UUID group=select(snapshot,p -> p.releaseReceipt()==null || p.nativeGate()==null || p.nativeGate().released()==null);
            if (group==null) return result(Action.COMPLETE,null);
            journal.freezeRelease(group);
            if (snapshot.participant(group).releaseReceipt()!=null) { journal.executeRelease(group); return result(Action.RELEASED,group); }
            try (var grant=authorize(RatisSnapshotRecord.release(home,group))) { journal.executeRelease(group); }
            catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException("Snapshot release authority failed",failure); }
            return result(Action.RELEASED,group);
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
                if (progress.installReceipt()!=null) { journal.executeInstall(group); closeInstall(group); return result(Action.INSTALLED,group); }
                var request=RatisSnapshotRecord.install(home.fence(),group);
                var grant=installs.get(group);
                if (grant!=null && !grant.valid().getAsBoolean()) { closeInstall(group); grant=null; }
                if (grant==null) { grant=authorize(request); installs.put(group,grant); }
                if (!grant.drained().getAsBoolean()) return result(Action.WAITING,group);
                try { journal.executeInstall(group); }
                catch (IOException | RuntimeException failure) { try { closeInstall(group); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
                closeInstall(group); return result(Action.INSTALLED,group);
            }
            if (!journal.verifyReady(group)) return result(helper.help(home.fence(),group) ? Action.HELPED : Action.WAITING,group);
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
    private Authorization authorize(RatisSnapshotRecord request) throws IOException {
        var grant=Objects.requireNonNull(authority.authorize(request));
        if (!grant.request().equals(request)) {
            try { grant.close(); } catch (Exception failure) { throw new IOException("Snapshot authority request changed",failure); }
            throw new IOException("Snapshot authority request changed");
        }
        return grant;
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
                && snapshot.participants().stream().allMatch(p -> p.releaseReceipt()!=null && p.nativeGate()!=null && p.nativeGate().released()!=null);
        return new Step(action,group,complete);
    }
    private void closeInstall(UUID group) throws IOException {
        var grant=installs.get(group); if (grant==null) return;
        try { grant.close(); installs.remove(group); } catch (Exception failure) { throw new IOException("Snapshot install authority close failed",failure); }
    }
    private void closeInstalls() throws IOException { for (var group:List.copyOf(installs.keySet())) closeInstall(group); }
    /** Journal, transports and helpers are borrowed. Undispatched INSTALL reservations remain owned until close or drainage. */
    @Override public synchronized void close() throws IOException {
        if (active) throw new IOException("Snapshot step active; ownership retained"); if (closed) return; closeInstalls(); closed=true;
    }
}
