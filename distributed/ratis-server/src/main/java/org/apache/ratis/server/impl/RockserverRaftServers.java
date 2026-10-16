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

    /** Cached Raft frontier only; the adapter must independently bind the native marker/full CFG. */
    public record SourceFrontier(TermIndex cut,long configurationIndex,RaftConfigurationProto configuration){
        public SourceFrontier{Objects.requireNonNull(cut);Objects.requireNonNull(configuration);if(cut.getTerm()<1 || cut.getIndex()<0 || configurationIndex<0 || configurationIndex>cut.getIndex())throw new IllegalArgumentException("Invalid final source frontier");}
    }
    /** Adapter contract for work whose actual lifetime can outlive a timed-out Raft reply.
     * Invoked after sealed ingress and selected applies drain, outside core monitors. It must
     * fence group-specific external admission and join actual query, snapshot and native work.
     * The call must return promptly; its future must acknowledge actual retirement, never an
     * observer timeout/cancellation. Catch-up retry does not reopen ordinary source admission.
     */
    public interface SourceRetirementStateMachine extends StateMachine {
        CompletableFuture<Void> drainSourceRetirement();
    }
    /** Terminal division owner. Observation cancellation does not release admission or actual work. */
    public static final class SourceRetirement {
        final RaftServerImpl server;final StateMachine machine;final StateMachineUpdater updater;final RecoveryEligibilityStateMachine.Gate gate;
        volatile CompletableFuture<Void> protocolsDrained=new CompletableFuture<>();
        volatile CompletableFuture<SourceFrontier> drained=new CompletableFuture<>();
        volatile CompletableFuture<Void> roleStopped, nativeDrained, resumeRequested;volatile SourceFrontier frontier,expected;volatile boolean sealRequested,stopRequested,receiptIssued;volatile long sealGeneration=-1;
        private volatile boolean closed;
        private final java.util.concurrent.atomic.AtomicBoolean closeOwner=new java.util.concurrent.atomic.AtomicBoolean();
        private SourceRetirement(RaftServerImpl server,StateMachine machine,RecoveryEligibilityStateMachine.Gate gate){this.server=server;this.machine=machine;this.gate=gate;updater=server.getState().getStateMachineUpdater();}
        /** Observation only: replication/catch-up remains usable until every cohort owner agrees. */
        public SourceFrontier observe()throws IOException{return server.observeSourceFrontier(this);}
        public CompletableFuture<SourceFrontier> sealAt(SourceFrontier agreed)throws IOException{server.sealSourceRetirement(this,Objects.requireNonNull(agreed));return drained();}
        public CompletableFuture<SourceFrontier> drained(){return drained.copy();}
        /** Explicit pre-publication retry, never observer cancellation. Native closing stays permanent. */
        public CompletableFuture<Void> resumeCatchup()throws IOException{return server.requestSourceCatchup(this);}
        /** Must precede the first native/CAT terminal-receipt force attempt, including an unknown outcome. */
        public void issuingTerminalReceipt(SourceFrontier expected)throws IOException{synchronized(server){requireHeld();if(resumeRequested!=null || !frontier.equals(expected))throw new IOException("Source terminal receipt changed agreed frontier");receiptIssued=true;}}

        /** No callbacks or native IO; terminal admissions prevent this proof from being widened later. */
        public void requireHeld()throws IOException{
            var f=frontier;
            synchronized(server){if(closed || server.getInfo().getLifeCycleState()!=LifeCycle.State.RUNNING || server.getSourceRetirement()!=this || server.getStateMachine()!=machine || !gate.sourceRetirementPending() || gate.revocationGeneration()!=sealGeneration || !server.sourceProtocolsDrained(this) || f==null || !drained.isDone() || drained.isCompletedExceptionally() || roleStopped==null || !roleStopped.isDone() || roleStopped.isCompletedExceptionally() || nativeDrained==null || !nativeDrained.isDone() || nativeDrained.isCompletedExceptionally() || server.getRaftLog().getLastCommittedIndex()!=f.cut().getIndex() || server.getRaftConf().getLogEntryIndex()!=f.configurationIndex() || !LogProtoUtils.toRaftConfigurationProtoBuilder(server.getRaftConf()).build().equals(f.configuration()) || !updater.sourceRetirementReady(this))throw new IOException("Source retirement has not joined its exact committed/native continuation frontier");}
        }
        /** Caller owns the actual blocking action outside Node/division/native monitors. */
        public void closeChecked()throws IOException{
            if(closed)return;if(!closeOwner.compareAndSet(false,true))throw new IOException("Actual source close remains owned");
            try{
                if(!stopRequested)requireHeld();else if(server.getSourceRetirement()!=this || frontier==null || !drained.isDone() || drained.isCompletedExceptionally())throw new IOException("Source close retry changed original owner/frontier");
                if(!receiptIssued)throw new IOException("Source physical close has no issued terminal-receipt owner");stopRequested=true;updater.notifyUpdater();
                updater.stopAndJoin();updater.requireSourceStopped();
                // ServerState.close catches/logs these failures. Terminal removal needs checked acknowledgements.
                server.getRaftLog().close();server.getState().getStorage().close();server.close();updater.requireSourceStopped();closed=true;
            }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Source close interrupted; ownership/deletion debt retained",interrupted);}
            finally{if(!closed)closeOwner.set(false);}
        }
        public boolean closed(){return closed;}
    }
    public static SourceRetirement beginSourceRetirement(RaftServer.Division division,StateMachine expected)throws IOException{
        if(!(division instanceof RaftServerImpl server) || server.getStateMachine()!=expected || !(expected instanceof RecoveryEligibilityStateMachine recovery) || !(expected instanceof SourceRetirementStateMachine))throw new IOException("Source retirement requires actual pinned division/SM owner");
        synchronized(server){var prior=server.getSourceRetirement();if(prior!=null){if(prior.machine!=expected)throw new IOException("Source retirement owner changed");return prior;}}
        var owner=new SourceRetirement(server,expected,recovery.recoveryEligibilityGate());server.beginSourceRetirement(owner);return owner;
    }

    public enum DiscoveryRole { GENESIS, CURRENT }
    /** Stops new selection and joins actual selected callbacks before binding publication evidence. */
    public static final class DiscoveryDrain {
        final RaftServerImpl server;final StateMachine machine;final StateMachineUpdater updater;
        final RecoveryEligibilityStateMachine.Gate gate;final DiscoveryRole role;final boolean initiallyPending;
        final long generation,initialRevision;volatile long revision=-1;
        final CompletableFuture<Void> drained=new CompletableFuture<>(),retired=new CompletableFuture<>();
        volatile boolean releaseRequested,abortRequested;
        private DiscoveryDrain(RaftServerImpl server,StateMachine machine,StateMachineUpdater updater,RecoveryEligibilityStateMachine.Gate gate,DiscoveryRole role) {
            this.server=server;this.machine=machine;this.updater=updater;this.gate=gate;this.role=role;initiallyPending=gate.discoveryPending();generation=gate.revocationGeneration();initialRevision=gate.revision();
        }
        public CompletableFuture<Void> drained(){return drained.copy();}
        public long revision()throws IOException{requireHeld();return revision;}
        public CompletableFuture<Void> release(){releaseRequested=true;updater.notifyUpdater();return retired.copy();}
        public CompletableFuture<Void> abort(){abortRequested=true;updater.notifyUpdater();return retired.copy();}
        /** Cached only: safe under the caller's Node -> SM -> eligibility publication guards. */
        public void requireHeld()throws IOException {
            if(!drained.isDone() || drained.isCompletedExceptionally() || retired.isDone() || !updater.holdsDiscoveryDrain(this) || !sameOwner() || !eligible() || gate.revocationGeneration()!=generation || gate.revision()!=revision)throw new IOException("Discovery updater hold is absent, stale or not drained");
        }
        boolean eligible(){var mode=gate.snapshot().mode();return role==DiscoveryRole.GENESIS?!gate.discoveryPending() && mode==RecoveryEligibilityStateMachine.Mode.GENESIS_SUSPENDED:gate.discoveryPending()==initiallyPending && mode!=RecoveryEligibilityStateMachine.Mode.GENESIS && mode!=RecoveryEligibilityStateMachine.Mode.GENESIS_SUSPENDED;}
        boolean sameOwner(){return server.getStateMachine()==machine && server.getInfo().isAlive() && server.getInfo().getLifeCycleState()==LifeCycle.State.RUNNING && machine.getLifeCycleState()==LifeCycle.State.RUNNING;}
    }
    public static DiscoveryDrain holdDiscovery(RaftServer.Division division,StateMachine expected,DiscoveryRole role)throws IOException {
        Objects.requireNonNull(expected);Objects.requireNonNull(role);
        if(!(division instanceof RaftServerImpl server) || !(expected instanceof RecoveryEligibilityStateMachine recovery))throw new IOException("Discovery hold requires actual recovery division");
        final DiscoveryDrain drain;
        synchronized(server) {
            var gate=recovery.recoveryEligibilityGate();var updater=server.getState().getStateMachineUpdater();
            drain=new DiscoveryDrain(server,expected,updater,gate,role);
            if(!drain.sameOwner() || !drain.eligible())throw new IOException("Discovery hold requires exact role/owner");
            updater.registerDiscoveryDrain(drain);
        }
        drain.updater.notifyUpdater();return drain;
    }

    /** The local checkpoint path needs this core's updater ownership, not only the Division facade. */
    public static boolean ownsLocalUpdater(RaftServer.Division division,StateMachine expected) {
        return division instanceof RaftServerImpl server && server.getStateMachine()==expected;
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
