package org.apache.ratis.server.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;

/** Pinned construction seam: close an allocated proxy when initGroups fails before ownership can be returned. */
public final class RockserverRaftServers {
    private RockserverRaftServers() {}
    public static RaftServer create(RaftPeerId id,RaftGroup seed,RaftStorage.StartupOption option,StateMachine.Registry registry,
            RaftProperties properties,Parameters parameters) throws IOException {
        Objects.requireNonNull(id); Objects.requireNonNull(option); Objects.requireNonNull(registry); Objects.requireNonNull(properties);
        if (seed!=null && !seed.getPeers().isEmpty() && seed.getPeer(id)==null) throw new IllegalArgumentException("Seed group does not contain this node");
        return initialize(new RaftServerProxy(id,registry,properties,parameters,null),server -> server.initGroups(seed,option));
    }
    /** Reload an already forced local checkpoint. Discovery settlement remains the caller's responsibility. */
    public static CompletableFuture<Void> reloadLocalCheckpoint(RaftServer.Division division,
            StateMachine expected, TermIndex cut) throws IOException {
        Objects.requireNonNull(expected); Objects.requireNonNull(cut);
        if (!(division instanceof RaftServerImpl server)) throw new IOException("Local reload requires actual division");
        synchronized (server) {
            if (cut.getTerm() < 1 || cut.getIndex() < 0 || server.getStateMachine() != expected || !server.getInfo().isAlive()
                    || server.getInfo().getLifeCycleState() != LifeCycle.State.RUNNING
                    || expected.getLifeCycleState() != LifeCycle.State.PAUSED
                    || !(expected instanceof RecoveryEligibilityStateMachine recovery)
                    || !recovery.recoveryEligibilityGate().discoveryPending()
                    || expected.getLatestSnapshot() == null
                    || !cut.equals(expected.getLatestSnapshot().getTermIndex())
                    || !cut.equals(expected.getLastAppliedTermIndex())
                    || cut.getIndex() < server.getRaftLog().getSnapshotIndex()
                    || !(server.getRaftLog() instanceof SegmentedRaftLog log)) {
                throw new IOException("Local reload lacks exact paused checkpoint/discovery ownership");
            }
            long retainedIndex = server.getRaftLog().getSnapshotIndex();
            var retainedFloor = retainedIndex < 0 ? null : server.getRaftLog().getTermIndex(retainedIndex);
            if (retainedFloor != null && (cut.getTerm() < retainedFloor.getTerm()
                    || cut.getIndex() == retainedFloor.getIndex() && cut.getTerm() != retainedFloor.getTerm())) {
                throw new IOException("Local reload regressed retained snapshot term/index");
            }
            var knownAtCut = server.getRaftLog().getTermIndex(cut.getIndex());
            if (knownAtCut != null && knownAtCut.getTerm() != cut.getTerm()) {
                throw new IOException("Local checkpoint contradicts retained log entry at its cut");
            }
            var updater = server.getState().getStateMachineUpdater();
            var reinitialized = updater.observeLocalReload(cut);
            CompletableFuture<Long> installed;
            try {
                server.getState().reloadStateMachine(cut);
                installed = log.snapshotInstallationCompletion(cut.getIndex());
            } catch (Throwable failure) {
                // The binary dispatches the updater before invoking the log. Its synchronous
                // exception does not mean that reinitialize has returned.
                try {
                    installed = log.snapshotInstallationCompletion(cut.getIndex())
                        .thenCompose(ignored -> CompletableFuture.failedFuture(failure));
                } catch (IOException absent) {
                    installed = CompletableFuture.failedFuture(failure);
                }
            }
            var actual = awaitLocalReload(reinitialized, installed);
            return actual.whenComplete((ignored, failure) -> updater.finishLocalReload(failure)).copy();
        }
    }
    /** Both actual branches must terminate, including when one has already failed. */
    static CompletableFuture<Void> awaitLocalReload(CompletableFuture<Void> reinitialized,
            CompletableFuture<Long> installed) {
        return CompletableFuture.allOf(reinitialized, installed);
    }
    @FunctionalInterface interface Initializer<T> { void initialize(T resource) throws IOException; }
    static <T extends Closeable> T initialize(T resource,Initializer<T> initialize) throws IOException {
        try { initialize.initialize(resource); return resource; }
        catch (IOException | RuntimeException | Error failure) {
            try { resource.close(); } catch (Throwable cleanup) { if (cleanup!=failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
}
