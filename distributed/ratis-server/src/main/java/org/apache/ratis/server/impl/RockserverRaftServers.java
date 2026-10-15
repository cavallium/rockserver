package org.apache.ratis.server.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.LogProtoUtils;
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
    private static final long MAX_COVERED_HEADERS = 1L << 20;
    /** One updater-owned recovery barrier; observers cannot cancel actual work or retire its owner. */
    public static final class CoveredPrefix {
        final RaftServerImpl server;
        final StateMachine machine;
        final StateMachineUpdater updater;
        final RecoveryEligibilityStateMachine.Gate gate;
        final long revision;
        final TermIndex snapshot, applied, configurationCut;
        final RaftConfigurationProto configuration;
        final CompletableFuture<Void> drained = new CompletableFuture<>(), retired = new CompletableFuture<>();
        volatile boolean started, releaseRequested, abortRequested;
        long precedingTerm;
        private CoveredPrefix(RaftServerImpl server, StateMachine machine, StateMachineUpdater updater,
                RecoveryEligibilityStateMachine.Gate gate, long revision, TermIndex snapshot, TermIndex applied,
                TermIndex configurationCut, RaftConfigurationProto configuration) {
            this.server = server; this.machine = machine; this.updater = updater; this.gate = gate;
            this.revision = revision; this.snapshot = snapshot; this.applied = applied;
            this.configurationCut = configurationCut; this.configuration = configuration; precedingTerm = snapshot.getTerm();
        }
        public CompletableFuture<Void> start() { started = true; updater.notifyUpdater(); return drained(); }
        public CompletableFuture<Void> drained() { return drained.copy(); }
        /** Caller has durably published its exact native/Node role and settled discovery outside core monitors. */
        public CompletableFuture<Void> release() {
            releaseRequested = true; updater.notifyUpdater(); return retired.copy();
        }
        public CompletableFuture<Void> abort() { abortRequested = true; updater.notifyUpdater(); return retired.copy(); }
        boolean sameLocalOwner() {
            return server.getStateMachine() == machine && server.getInfo().isAlive()
                && server.getInfo().getLifeCycleState() == LifeCycle.State.RUNNING
                && machine.getLifeCycleState() == LifeCycle.State.RUNNING
                && applied.equals(machine.getLastAppliedTermIndex()) && machine.getLatestSnapshot() != null
                && snapshot.equals(machine.getLatestSnapshot().getTermIndex())
                && server.getRaftLog().getSnapshotIndex() == snapshot.getIndex();
        }
        boolean published() {
            var evidence = gate.snapshot();
            return sameLocalOwner() && !gate.discoveryPending()
                && evidence.mode() == RecoveryEligibilityStateMachine.Mode.RECOVERY
                && applied.equals(evidence.nativeCut()) && snapshot.equals(evidence.snapshotCut())
                && evidence.configurationTerm() == configurationCut.getTerm()
                && evidence.configurationIndex() == configurationCut.getIndex()
                && configuration.equals(evidence.configuration());
        }
    }

    /** Actual updater progress; Division.Info may report the native-backed SM cut ahead of this cursor. */
    public static boolean hasAppliedPrefix(RaftServer.Division division,StateMachine expected,TermIndex cut) {
        return division instanceof RaftServerImpl server && server.getStateMachine()==expected
            && server.getState().getStateMachineUpdater().getLastAppliedIndex()>=cut.getIndex();
    }

    /** Read-only cold original-creation proof. Missing proof leaves discovery fenced. */
    public static boolean hasGenesisPrefix(RaftServer.Division division, StateMachine expected,
            TermIndex nativeApplied, TermIndex nativeConfiguration, RaftConfigurationProto originalConfiguration) throws IOException {
        Objects.requireNonNull(expected); Objects.requireNonNull(nativeConfiguration); Objects.requireNonNull(originalConfiguration);
        if (!(division instanceof RaftServerImpl server) || !(expected instanceof RecoveryEligibilityStateMachine recovery)
                || !(server.getRaftLog() instanceof SegmentedRaftLog)) throw new IOException("GENESIS requires actual recovery division/log");
        final TermIndex applied = nativeApplied == null ? TermIndex.valueOf(0, -1) : nativeApplied;
        final TermIndex snapshot; final StateMachineUpdater updater; final long revision, cursor, next, start, actualConfigurationIndex;
        synchronized (server) {
            if (server.getStateMachine() != expected || !server.getInfo().isAlive()
                    || server.getInfo().getLifeCycleState() != LifeCycle.State.RUNNING
                    || expected.getLifeCycleState() != LifeCycle.State.RUNNING || !recovery.recoveryEligibilityGate().discoveryPending()
                    || !applied.equals(expected.getLastAppliedTermIndex())) throw new IOException("GENESIS lacks exact native/discovery owner");
            snapshot = expected.getLatestSnapshot() == null ? TermIndex.valueOf(0, -1) : expected.getLatestSnapshot().getTermIndex();
            if (applied.getIndex() < -1 || applied.getIndex() == Long.MAX_VALUE || applied.getTerm() < 0
                    || applied.getIndex() == -1 && applied.getTerm() != 0 || applied.getIndex() >= 0 && applied.getTerm() < 1
                    || snapshot.getIndex() < -1 || snapshot.getTerm() < 0 || snapshot.getIndex() == -1 && snapshot.getTerm() != 0
                    || snapshot.getIndex() >= 0 && snapshot.getTerm() < 1
                    || nativeConfiguration.getIndex() < -1 || nativeConfiguration.getTerm() < 0
                    || nativeConfiguration.getIndex() == -1 && nativeConfiguration.getTerm() != 0
                    || nativeConfiguration.getIndex() > applied.getIndex() || nativeConfiguration.getTerm() > applied.getTerm()
                    || originalConfiguration.getSerializedSize() > 1 << 20) throw new IOException("Invalid GENESIS native/configuration prefix");
            if (applied.getIndex() < snapshot.getIndex()) return false;
            if (applied.getTerm() < snapshot.getTerm() || applied.getIndex() == snapshot.getIndex() && applied.getTerm() != snapshot.getTerm())
                throw new IOException("GENESIS native cut contradicts retained GROUP cut");
            if (server.getRaftLog().getSnapshotIndex() != snapshot.getIndex()) return false;
            updater = server.getState().getStateMachineUpdater(); updater.requireCoveredAvailable();
            cursor = updater.getLastAppliedIndex();
            if (cursor < snapshot.getIndex() || cursor > applied.getIndex()) throw new IOException("GENESIS updater exceeds actual prefix");
            if(server.getRole().getLeaderState().map(LeaderStateImpl::inStagingState).orElse(false))return false;
            var actualConfiguration = server.getRaftConf(); actualConfigurationIndex = actualConfiguration.getLogEntryIndex();
            if (!LogProtoUtils.toRaftConfigurationProtoBuilder(actualConfiguration).build().equals(originalConfiguration))
                throw new IOException("GENESIS original configuration differs from actual membership");
            revision = recovery.recoveryEligibilityGate().revision();
            next = server.getRaftLog().getNextIndex(); start = server.getRaftLog().getStartIndex();
        }
        var log = server.getRaftLog();
        if (next - snapshot.getIndex() - 1 > MAX_COVERED_HEADERS
                || next <= applied.getIndex() || next > snapshot.getIndex() + 1 && start > snapshot.getIndex() + 1) return false;
        long previousTerm = snapshot.getTerm(), lastNativeConfiguration = -1, lastRetainedConfiguration = -1;
        boolean nativeCovered = applied.equals(snapshot);
        for (long index = snapshot.getIndex() + 1; index < next; index++) {
            if (index < log.getStartIndex() || index >= log.getNextIndex()) return false;
            final org.apache.ratis.server.raftlog.LogEntryHeader[] headers;
            try { headers = log.getEntries(index, index + 1); }
            catch (IndexOutOfBoundsException missing) {
                if (index < log.getStartIndex() || index >= log.getNextIndex()) return false;
                throw new IOException("GENESIS cached headers contradict retained bounds", missing);
            }
            if (headers == null || headers.length > 0 && headers[0] == null) throw new IOException("Null GENESIS cached header");
            if (headers.length == 0) return false;
            var header = headers[0];
            if (headers.length != 1 || header.getIndex() != index || header.getTerm() < previousTerm)
                throw new IOException("GENESIS cached prefix is contradictory");
            previousTerm = header.getTerm();
            if (header.getLogEntryBodyCase() == LogEntryProto.LogEntryBodyCase.STATEMACHINELOGENTRY) return false;
            if (header.getLogEntryBodyCase() == LogEntryProto.LogEntryBodyCase.CONFIGURATIONENTRY) {
                lastRetainedConfiguration = index;
                if (index <= applied.getIndex()) lastNativeConfiguration = index;
            } else if (header.getLogEntryBodyCase() != LogEntryProto.LogEntryBodyCase.METADATAENTRY)
                throw new IOException("Unexpected GENESIS log body");
            if (index == applied.getIndex()) {
                if (header.getTerm() != applied.getTerm()) throw new IOException("GENESIS native term contradicts retained log");
                nativeCovered = true;
            }
        }
        if (!nativeCovered) return false;
        if (lastNativeConfiguration >= 0 && lastNativeConfiguration != nativeConfiguration.getIndex())
            throw new IOException("GENESIS native CFG is not latest through native A");
        if (nativeConfiguration.getIndex() >= 0) {
            var entry = log.get(nativeConfiguration.getIndex());
            if (entry == null) return false;
            if (!entry.hasConfigurationEntry() || entry.getTerm() != nativeConfiguration.getTerm()
                    || !entry.getConfigurationEntry().equals(originalConfiguration)) throw new IOException("GENESIS native CFG contradicts retained full entry");
        } else if (actualConfigurationIndex != 0 && actualConfigurationIndex != lastRetainedConfiguration) {
            return false;
        }
        synchronized (server) {
            updater.requireCoveredAvailable();
            return server.getStateMachine() == expected && server.getInfo().isAlive()
                && server.getInfo().getLifeCycleState() == LifeCycle.State.RUNNING && expected.getLifeCycleState() == LifeCycle.State.RUNNING
                && !server.getRole().getLeaderState().map(LeaderStateImpl::inStagingState).orElse(false)
                && recovery.recoveryEligibilityGate().discoveryPending() && recovery.recoveryEligibilityGate().revision() == revision
                && applied.equals(expected.getLastAppliedTermIndex())
                && snapshot.equals(expected.getLatestSnapshot() == null ? TermIndex.valueOf(0, -1) : expected.getLatestSnapshot().getTermIndex())
                && cursor == updater.getLastAppliedIndex() && log.getSnapshotIndex() == snapshot.getIndex()
                && log.getStartIndex() == start && log.getNextIndex() == next
                && server.getRaftConf().getLogEntryIndex() == actualConfigurationIndex
                && LogProtoUtils.toRaftConfigurationProtoBuilder(server.getRaftConf()).build().equals(originalConfiguration);
        }
    }

    /** Missing retained coverage/commit proof selects the snapshot fallback before changing the updater. */
    public static Optional<CoveredPrefix> coverLocalPrefix(RaftServer.Division division, StateMachine expected,
            TermIndex nativeApplied, TermIndex nativeConfiguration, RaftConfigurationProto configuration) throws IOException {
        Objects.requireNonNull(expected); Objects.requireNonNull(nativeApplied);
        Objects.requireNonNull(nativeConfiguration); Objects.requireNonNull(configuration);
        if (!(division instanceof RaftServerImpl server) || !(expected instanceof RecoveryEligibilityStateMachine recovery)
                || !(server.getRaftLog() instanceof SegmentedRaftLog)) throw new IOException("Covered prefix requires actual recovery division/log");
        final StateMachineUpdater updater; final TermIndex snapshot; final long revision;
        synchronized (server) {
            if (server.getStateMachine() != expected || !server.getInfo().isAlive()
                    || server.getInfo().getLifeCycleState() != LifeCycle.State.RUNNING
                    || expected.getLifeCycleState() != LifeCycle.State.RUNNING || !recovery.recoveryEligibilityGate().discoveryPending()
                    || !nativeApplied.equals(expected.getLastAppliedTermIndex()) || expected.getLatestSnapshot() == null)
                throw new IOException("Covered prefix lacks exact native/discovery owner");
            snapshot = expected.getLatestSnapshot().getTermIndex();
            if (snapshot.getTerm() < 1 || snapshot.getIndex() < 0 || nativeApplied.getIndex() <= snapshot.getIndex()
                    || nativeApplied.getTerm() < snapshot.getTerm() || nativeApplied.getIndex() == Long.MAX_VALUE
                    || nativeConfiguration.getIndex() < -1 || nativeConfiguration.getTerm() < 0
                    || nativeConfiguration.getIndex() == -1 && nativeConfiguration.getTerm() != 0
                    || nativeConfiguration.getIndex() > nativeApplied.getIndex() || nativeConfiguration.getTerm() > nativeApplied.getTerm()
                    || configuration.getSerializedSize() > 1 << 20) throw new IOException("Invalid covered prefix geometry/configuration");
            updater = server.getState().getStateMachineUpdater();
            if (server.getRaftLog().getSnapshotIndex() != snapshot.getIndex())
                throw new IOException("Covered prefix snapshot differs from actual log floor");
            updater.requireCoveredAvailable();
            long cursor = updater.getLastAppliedIndex();
            if (cursor < snapshot.getIndex() || cursor > nativeApplied.getIndex())
                throw new IOException("Covered updater cursor exceeds native/snapshot bounds");
            if (cursor > snapshot.getIndex()) return Optional.empty();
            revision = recovery.recoveryEligibilityGate().revision();
        }
        var log = server.getRaftLog();
        if (nativeApplied.getIndex() - snapshot.getIndex() > MAX_COVERED_HEADERS
                || log.getLastCommittedIndex() < nativeApplied.getIndex()
                || log.getStartIndex() > snapshot.getIndex() + 1 || log.getNextIndex() <= nativeApplied.getIndex()) return Optional.empty();
        long previousTerm = snapshot.getTerm(), lastConfiguration = -1;
        for (long index = snapshot.getIndex() + 1; index <= nativeApplied.getIndex(); index++) {
            if (index < log.getStartIndex() || index >= log.getNextIndex()) return Optional.empty();
            final org.apache.ratis.server.raftlog.LogEntryHeader[] headers;
            try { headers = log.getEntries(index, index + 1); }
            catch (IndexOutOfBoundsException missing) {
                if (index < log.getStartIndex() || index >= log.getNextIndex()) return Optional.empty();
                throw new IOException("Cached covered header contradicts retained log bounds", missing);
            }
            if (headers == null) throw new IOException("Cached covered header array is null");
            if (headers.length == 0) return Optional.empty();
            if (headers[0] == null) throw new IOException("Cached covered header is null");
            if (headers.length != 1 || headers[0].getIndex() != index || headers[0].getTerm() < previousTerm)
                throw new IOException("Covered cached log prefix is contradictory");
            previousTerm = headers[0].getTerm();
            if (headers[0].getLogEntryBodyCase() == LogEntryProto.LogEntryBodyCase.CONFIGURATIONENTRY) lastConfiguration = index;
        }
        if (previousTerm != nativeApplied.getTerm()) throw new IOException("Native applied cut contradicts retained log term");
        if (lastConfiguration >= 0 && lastConfiguration != nativeConfiguration.getIndex())
            throw new IOException("Native configuration is not latest in covered log prefix");
        var configurationEntry = nativeConfiguration.getIndex() < 0 ? null : log.get(nativeConfiguration.getIndex());
        if (configurationEntry != null) {
            if (!configurationEntry.hasConfigurationEntry() || configurationEntry.getTerm() != nativeConfiguration.getTerm()
                    || !configurationEntry.getConfigurationEntry().equals(configuration)) throw new IOException("Native configuration contradicts retained full log configuration");
        } else {
            if (nativeConfiguration.getIndex() > snapshot.getIndex()) return Optional.empty();
            var actual = server.getRaftConf();
            boolean bootstrap = nativeConfiguration.getTerm() == 0 && nativeConfiguration.getIndex() == -1
                && actual.getLogEntryIndex() == 0 && log.getTermIndex(0) == null;
            if (actual.getLogEntryIndex() != nativeConfiguration.getIndex() && !bootstrap
                    || !LogProtoUtils.toRaftConfigurationProtoBuilder(actual).build().equals(configuration)) return Optional.empty();
            // A compacted non-bootstrap configuration has no retained term witness here.
            if (nativeConfiguration.getIndex() >= 0) return Optional.empty();
        }
        synchronized (server) {
            var permit = new CoveredPrefix(server, expected, updater, recovery.recoveryEligibilityGate(), revision,
                snapshot, nativeApplied, nativeConfiguration, configuration);
            if (!permit.sameLocalOwner() || !permit.gate.discoveryPending() || permit.gate.revision() != revision
                    || log.getLastCommittedIndex() < nativeApplied.getIndex() || updater.getLastAppliedIndex() != snapshot.getIndex())
                throw new IOException("Covered prefix owner changed during qualification");
            updater.registerCoveredPrefix(permit);
            return Optional.of(permit);
        }
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
