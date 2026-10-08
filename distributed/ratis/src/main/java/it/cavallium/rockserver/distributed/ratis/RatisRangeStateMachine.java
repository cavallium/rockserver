package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.PreparedParticipant;
import it.cavallium.rockserver.core.common.distributed.ParticipantFinalization;
import it.cavallium.rockserver.core.common.distributed.TransactionStatus;
import it.cavallium.rockserver.core.common.distributed.RangeMutationHistory;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import java.util.function.Consumer;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.common.distributed.RangeDirectory;
import it.cavallium.rockserver.core.common.distributed.RangeStateView;
import it.cavallium.rockserver.core.common.distributed.ProposalFootprints;
import it.cavallium.rockserver.core.common.distributed.TransactionScope;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import java.util.ArrayList;
import java.util.List;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.MD5FileUtil;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collection;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Initial synchronous apply adapter; the pool owner drains Ratis before closing native handles. */
public final class RatisRangeStateMachine extends BaseStateMachine implements org.apache.ratis.server.impl.LinearizableQueryStateMachine {
    private final ReplicatedRangeStore store;
    private final Consumer<DistributedCommand> commandValidator;
    private final Consumer<DistributedCommand> epochFloorAdmission;
    private volatile Throwable storageFailure;
    private final HashMap<OperationId, Admission> admissions = new HashMap<>();
    private long admittedBytes, dataProposalBytes;
    private int dataAdmissions, protectedAdmissions;
    private long reservedReceiptCount, reservedReceiptBytes;
    private Admission maintenanceReservation;
    private final RetentionLimits retentionLimits;
    private final boolean enableSessionControls;
    public record RetentionLimits(long dataCount, long dataBytes, long maintenanceCount, long maintenanceBytes) {
        public RetentionLimits {
            if (dataCount < 1 || dataBytes < 92 || maintenanceCount < 1 || maintenanceBytes < 92) throw new IllegalArgumentException("Invalid retention limits");
            Math.addExact(dataCount,maintenanceCount); Math.addExact(dataBytes,maintenanceBytes);
        }
        public static RetentionLimits defaults() { return new RetentionLimits(4096,16L*1024*1024,16,64*1024); }
    }
    private final java.util.concurrent.atomic.AtomicLong admissionGeneration = new java.util.concurrent.atomic.AtomicLong();
    record Leadership(long term, boolean ready) {}
    @FunctionalInterface interface LeadershipSource { Leadership get() throws IOException; }
    private final LeadershipSource leadershipSource;
    private RatisScopedTransactions scopedTransactions;

    /** Test clock configuration is fixed before this state-machine owner is initialized. */
    void transactionClock(java.util.function.LongSupplier nanos) {
        if (getLifeCycleState()!=LifeCycle.State.NEW) throw new IllegalStateException("Transaction clock is fixed before initialization");
        scopedTransactions=new RatisScopedTransactions(this,RatisScopedTransactions.Limits.defaults(),nanos);
    }
    long transactionGeneration() { return admissionGeneration.get(); }
    long transactionTerm() { var a=store.applied(); return a==null ? -1 : a.term(); }
    UUID transactionOwnerIncarnation() { return transactionIncarnation; }
    void checkTransactionOwner() throws IOException {
        requireHealthy(); var current=leadership(); var a=store.applied();
        if (!current.ready() || a==null || a.term()!=current.term() || !store.admissionOpen()) throw new IOException("Transaction leader changed or is not ready");
    }
    ReplicatedRangeStore.ScopedHistoryPin captureTransaction(TransactionScope scope) throws Exception {
        checkTransactionOwner(); long generation=admissionGeneration.get(); var pin=store.captureTracked(scope); boolean retained=false;
        try {
            checkTransactionOwner();
            synchronized (admissions) {
                var a=store.applied();
                if (generation!=admissionGeneration.get() || !store.admissionOpen() || a==null || pin.position().term()!=a.term()
                        || pin.status()!=it.cavallium.rockserver.core.common.distributed.RangeMutationHistory.Status.ACTIVE) throw new IOException("Transaction capture lost leadership");
            }
            retained=true; return pin;
        } finally { if (!retained) pin.close(); }
    }
    void expireTransactions() throws Exception { scopedTransactions.expire(); }

    private static final class Admission {
        final OperationId id;
        final byte[] digest;
        final int bytes;
        final long term, generation, receiptCount, receiptBytes;
        final boolean maintenance, protectedControl;
        final ProposalFootprints.OrdinaryLease footprint;
        final ProposalFootprints.CandidateLease acceptedScope;
        final ByteString rejectionLogData;
        PreparedParticipant.Preparation participant;
        ParticipantHold participantHold;
        ParticipantFinalization finalization;
        FinalizationHold finalizationHold;
        boolean retentionReleased,maybeAppended,settled;
        int owners = 1;
        Admission(OperationId id, byte[] digest, int bytes, long term, long generation,
                long receiptCount, long receiptBytes, boolean maintenance, boolean protectedControl,ProposalFootprints.OrdinaryLease footprint,ProposalFootprints.CandidateLease acceptedScope,ByteString rejectionLogData) {
            this.rejectionLogData=rejectionLogData; this.acceptedScope=acceptedScope; this.footprint=footprint; this.id = id;
            this.digest = digest;
            this.bytes = bytes; this.term = term; this.generation = generation;
            this.receiptCount=receiptCount; this.receiptBytes=receiptBytes;
            this.maintenance=maintenance; this.protectedControl=protectedControl;
        }
    }

    private final ProposalFootprints proposalFootprints=new ProposalFootprints(ProposalFootprints.Limits.defaults());
    private long reconciledProposalTerm=-1;
    private final UUID transactionIncarnation=UUID.randomUUID();
    private final HashMap<OperationId,ScopedCommit> preparedCommits=new HashMap<>();
    private static final class PreparingCommit {
        final ProposalReservation reservation; int bytes;
        PreparingCommit(ProposalReservation reservation,int bytes) { this.reservation=reservation; this.bytes=bytes; }
    }
    private final HashMap<OperationId,PreparingCommit> preparingCommits=new HashMap<>();
    private static final class ParticipantHold {
        final OperationId control,business; final UUID transaction; final long scopeBytes; long bytes;
        boolean released,capacityReleased; int finalizers; PreparedParticipant.Preparation preparation;
        ParticipantHold(UUID transaction,OperationId control,OperationId business,long bytes,long scopeBytes) { this.transaction=transaction; this.control=control; this.business=business; this.bytes=bytes; this.scopeBytes=scopeBytes; }
    }
    private record PreparingParticipant(ProposalReservation reservation,ParticipantHold hold) {}
    private final HashMap<OperationId,PreparingParticipant> preparingParticipants=new HashMap<>();
    private final HashMap<OperationId,ParticipantProposal> preparedParticipants=new HashMap<>();
    private final HashMap<OperationId,ParticipantHold> participantOwners=new HashMap<>();
    private long reservedParticipantCount,reservedParticipantBytes,reservedParticipantScopeBytes;
    private static final class FinalizationHold {
        final UUID transaction; final OperationId recovery; final long term,generation;
        OperationId business; ParticipantHold shared; boolean capacity,released;
        FinalizationHold(UUID transaction,OperationId recovery,long term,long generation) { this.transaction=transaction; this.recovery=recovery; this.term=term; this.generation=generation; }
    }
    private final HashMap<OperationId,FinalizationHold> finalizationOwners=new HashMap<>();
    private final HashMap<UUID,FinalizationHold> finalizingTransactions=new HashMap<>();
    private final HashMap<OperationId,FinalizationHold> preparingFinalizations=new HashMap<>();
    private final HashMap<OperationId,FinalizationProposal> preparedFinalizations=new HashMap<>();
    private long reservedFinalizationCount,reservedFinalizationBytes;
    private int protectedFinalizationSlots;
    private long preparedCommitBytes;
    private ProposalFootprints.CandidateLease exclusiveMetadataCandidate;
    private final Object gate = new Object();
    private final RaftGroup initialGroup;
    private final NativeRangeImage.Limits limits;
    private final SimpleStateMachineStorage snapshots = new SimpleStateMachineStorage();
    private volatile SingleFileSnapshotInfo durableSnapshot;
    private final TreeMap<Long, Deferred> deferred = new TreeMap<>();
    private final TreeMap<Long, ConfigurationEntry> configurations = new TreeMap<>();
    private final TreeMap<Long, Long> pendingNoops = new TreeMap<>();
    private long deferredBytes;
    private final Object durabilityGate = new Object();
    private Path raftRoot;
    private Path snapshotTmp;
    private Path recoveryDir;
    private record ConfigurationEntry(long term, byte[] bytes) {}
    private record Deferred(long term, byte[] encoded, CompletableFuture<Message> future) {}

    public RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits) {
        this(store, initialGroup, limits, command -> {});
    }

    /** Deterministic envelope validation, repeated on every replica before native apply. */
    public RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator) {
        this(store, initialGroup, limits, commandValidator, command -> {
            throw new IllegalStateException("Session epoch floors require catalogue lifetime authority");
        });
    }

    /** Authority is checked only at proposal admission; committed replay remains deterministic. */
    public RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator, Consumer<DistributedCommand> epochFloorAdmission) {
        this(store, initialGroup, limits, commandValidator, epochFloorAdmission, RetentionLimits.defaults(), null);
    }

    public RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator, Consumer<DistributedCommand> epochFloorAdmission, RetentionLimits retentionLimits) {
        this(store,initialGroup,limits,commandValidator,epochFloorAdmission,retentionLimits,null);
    }

    /** Explicit bootstrap seam: catalogue checks run only for controls, never on ordinary data replay. */
    public static RatisRangeStateMachine withSessions(ReplicatedRangeStore store,RaftGroup group,NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> validator,RatisSessionEpochs epochs,RangeDirectory directory,RetentionLimits retentionLimits) {
        return withSessions(store,group,limits,validator,epochs,directory,retentionLimits,null);
    }
    static RatisRangeStateMachine withSessions(ReplicatedRangeStore store,RaftGroup group,NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> validator,RatisSessionEpochs epochs,RangeDirectory directory,RetentionLimits retentionLimits,
            LeadershipSource leadershipSource) {
        var id=group.getGroupId().getUuid();
        var floor=epochs.floorAdmission(directory,id); var control=epochs.sessionControlAdmission(directory,id);
        var descriptor=directory.ranges().stream().filter(range -> range.groupId().equals(id)).findFirst().orElseThrow();
        var ownership=store.ownership(); var interval=ownership.interval();
        if (!ownership.owner().groupId().equals(id) || ownership.owner().epoch()!=descriptor.rangeEpoch()
                || !interval.domainId().equals(directory.domainId()) || !interval.columns().equals(directory.columns())
                || !Arrays.equals(store.schemaDigest(),directory.schemaDigest())
                || !Arrays.equals(interval.lowerInclusive(),descriptor.lowerInclusive())
                || !Arrays.equals(interval.upperExclusive(),descriptor.upperExclusive())) throw new IllegalArgumentException("Session directory differs from native range");
        return new RatisRangeStateMachine(store,group,limits,validator,
                command -> { if (command.sessionControl()!=null) control.accept(command); else floor.accept(command); },
                retentionLimits,leadershipSource,true);
    }

    // Explicit view injection is only for ordinary in-process fixtures without a running Raft division.
    RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator, Consumer<DistributedCommand> epochFloorAdmission,
            LeadershipSource leadershipSource) {
        this(store,initialGroup,limits,commandValidator,epochFloorAdmission,RetentionLimits.defaults(),leadershipSource);
    }

    RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator, Consumer<DistributedCommand> epochFloorAdmission,
            RetentionLimits retentionLimits, LeadershipSource leadershipSource) {
        this(store,initialGroup,limits,commandValidator,epochFloorAdmission,retentionLimits,leadershipSource,false);
    }
    private RatisRangeStateMachine(ReplicatedRangeStore store,RaftGroup initialGroup,NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator,Consumer<DistributedCommand> epochFloorAdmission,
            RetentionLimits retentionLimits,LeadershipSource leadershipSource,boolean enableSessionControls) {
        this.enableSessionControls=enableSessionControls;
        this.retentionLimits = java.util.Objects.requireNonNull(retentionLimits);
        this.leadershipSource = leadershipSource;
        this.commandValidator = java.util.Objects.requireNonNull(commandValidator);
        this.epochFloorAdmission = java.util.Objects.requireNonNull(epochFloorAdmission);
        this.store = java.util.Objects.requireNonNull(store);
        this.initialGroup = java.util.Objects.requireNonNull(initialGroup);
        this.limits = java.util.Objects.requireNonNull(limits);
        scopedTransactions=new RatisScopedTransactions(this,RatisScopedTransactions.Limits.defaults(),System::nanoTime);
    }

    @Override public SimpleStateMachineStorage getStateMachineStorage() { return snapshots; }
    @Override public SnapshotInfo getLatestSnapshot() { return durableSnapshot; }

    @Override public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException {
        super.initialize(server, groupId, storage);
        getLifeCycle().transition(LifeCycle.State.STARTING);
        try {
            if (!initialGroup.getGroupId().equals(groupId)
                    || !groupId.getUuid().equals(store.ownership().owner().groupId())) {
                throw new IOException("Raft group differs from native ownership");
            }
            store.fence();
            raftRoot = storage.getStorageDir().getRoot().toPath();
            snapshotTmp = storage.getStorageDir().getTmpDir().toPath();
            recoveryDir = raftRoot.resolve("native-snapshot-recovery");
            snapshots.init(storage);
            recover(false);
            store.activate();
            store.enableScopedHistory(it.cavallium.rockserver.core.common.distributed.RangeMutationHistory.Limits.defaults());
            if (enableSessionControls) store.enableSessionControls();
            getLifeCycle().transition(LifeCycle.State.RUNNING);
        } catch (Exception failure) { throw failRecovery(failure); }
    }

    private void recover(boolean installing) throws Exception {
        recoverStagingImages();
        SingleFileSnapshotInfo discovered = snapshots.loadLatestSnapshot();
        if (discovered == null) {
            Path directory = snapshots.getSnapshotFile(0, 0).toPath().getParent();
            try (var files = Files.list(directory)) {
                if (files.anyMatch(path -> SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(path.getFileName().toString()).matches())) {
                    throw new IOException("Snapshot files exist but could not be loaded");
                }
            }
        }
        if (discovered != null) {
            Path image = discovered.getFile().getPath();
            var manifest = validateImage(image, discovered.getTermIndex());
            force(image);
            Path md5 = MD5FileUtil.getDigestFileForFile(image.toFile()).toPath();
            if (Files.exists(md5)) force(md5);
            force(image.getParent());
            force(raftRoot);
            var a = store.applied();
            if (installing || store.pendingInstallation() || a == null || a.index() < manifest.position().index()) {
                store.fence();
                store.installSnapshot(image, limits);
            } else if (a.index() == manifest.position().index() && a.term() != manifest.position().term()) {
                throw new IOException("Native marker conflicts with durable snapshot");
            }
            durableSnapshot = discovered;
        } else if (installing || store.pendingInstallation()) {
            throw new IOException("Snapshot installation has no durable image");
        }
        if (store.configuration() == null) {
            if (store.applied() != null) throw new IOException("Applied native range has no configuration");
            var cfg = RaftConfigurationProto.newBuilder();
            for (var peer : initialGroup.getPeers().stream().sorted(java.util.Comparator.comparing(peer -> peer.getId().toString())).toList()) {
                if (peer.getStartupRole() == RaftPeerRole.LISTENER) cfg.addListeners(peer.getRaftPeerProto());
                else cfg.addPeers(peer.getRaftPeerProto());
            }
            store.initializeConfiguration(cfg.build().toByteArray());
        }
        var cfg = store.configuration();
        var a = store.applied();
        if (a != null && (cfg.index() > a.index() || cfg.term() > a.term())) throw new IOException("Native configuration exceeds completed marker");
        validateConfiguration(cfg.data());
        setLastAppliedTermIndex(a == null ? TermIndex.INITIAL_VALUE : TermIndex.valueOf(a.term(), a.index()));
    }

    private static void validateConfiguration(byte[] bytes) throws IOException {
        if (RaftConfigurationProto.parseFrom(bytes).getPeersCount() == 0) throw new IOException("Configuration has no voters");
    }

    private NativeRangeImage.Manifest validateImage(Path image, TermIndex position) throws IOException {
        final ReplicatedRangeStore.Configuration[] cutConfiguration = {null};
        var manifest = NativeRangeImage.visit(image, limits, (section, column, key, value) -> {
            if (section == NativeRangeImage.Section.STATE && key.length == 17 && key[16] == 1) {
                try { cutConfiguration[0] = ReplicatedRangeStore.Configuration.decode(value); }
                catch (IllegalArgumentException invalid) { throw new IOException("Invalid image configuration", invalid); }
            }
        });
        var cfg = cutConfiguration[0];
        if (cfg == null || cfg.index() > manifest.position().index() || cfg.term() > manifest.position().term()) {
            throw new IOException("Snapshot lacks configuration at its cut");
        }
        validateConfiguration(cfg.data());
        if (!manifest.owner().equals(store.ownership().owner())
                || !manifest.interval().equals(store.ownership().interval())
                || !Arrays.equals(manifest.schemaDigest(), store.schemaDigest())
                || !manifest.deterministicMergeColumns().equals(store.deterministicMergeColumns())
                || manifest.position().term() != position.getTerm() || manifest.position().index() != position.getIndex()) {
            throw new IOException("Snapshot identity does not match native range/filename");
        }
        var saved = MD5FileUtil.readStoredMd5ForFile(image.toFile());
        if (saved != null && !saved.equals(MD5FileUtil.computeMd5ForFile(image.toFile()))) {
            throw new IOException("Snapshot MD5 mismatch");
        }
        return manifest;
    }

    private static void force(Path path) throws IOException {
        try (var channel = FileChannel.open(path, Files.isDirectory(path) ? StandardOpenOption.READ : StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    @Override public long takeSnapshot() throws IOException {
        try (var pin = snapshotPin()) {
            if (pin.position() == null) return -1;
            var position = TermIndex.valueOf(pin.position().term(), pin.position().index());
            Path file = snapshots.getSnapshotFile(position.getTerm(), position.getIndex()).toPath();
            if (!Files.exists(file)) NativeRangeImage.write(file, pin, pin.interval(), limits);
            validateImage(file, position);
            var md5 = MD5FileUtil.computeMd5ForFile(file.toFile());
            MD5FileUtil.saveMD5File(file.toFile(), md5);
            force(file);
            force(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath());
            force(file.getParent());
            force(raftRoot);
            var info = new SingleFileSnapshotInfo(new FileInfo(file, md5), position);
            synchronized (gate) {
                requireHealthy();
                if (durableSnapshot == null || position.getIndex() > durableSnapshot.getIndex()) durableSnapshot = info;
                snapshots.updateLatestSnapshot(info);
            }
            return position.getIndex();
        } catch (Exception failure) { throw new IOException("Could not publish native snapshot", failure); }
    }

    private ReplicatedRangeStore.SnapshotPin snapshotPin() throws Exception {
        synchronized (gate) { requireHealthy(); return store.capture(); }
    }

    private ReplicatedRangeStore.ScopedSnapshot readablePoint(UUID column,byte[] key) throws Exception {
        synchronized (gate) { requireHealthy(); return store.captureReadable(new TransactionScope(java.util.List.of(TransactionScope.Span.point(column,key)))); }
    }

    @Override public void pause() {
        admissionGeneration.incrementAndGet(); invalidateProposalCandidates();
        try {
            synchronized (gate) {
                getLifeCycle().transition(LifeCycle.State.PAUSING);
                store.fence();
                getLifeCycle().transition(LifeCycle.State.PAUSED);
            }
            awaitNativeReaders(java.time.Duration.ofSeconds(10));
            preserveInstallationImages();
        } catch (Exception failure) {
            synchronized (gate) { storageFailure = failure; store.fence(); }
            failDeferred(failure);
            throw new IllegalStateException("Snapshot pause failed; native ownership remains fenced", failure);
        }
    }

    /** Must run before RaftServer.close(), whose updater waits for outstanding apply futures. */
    public void beginShutdown() {
        admissionGeneration.incrementAndGet(); invalidateProposalCandidates();
        try { scopedTransactions.close(); } catch (Exception failure) { storageFailure=failure; }
        IOException failure = new IOException("Range is shutting down");
        synchronized (gate) { storageFailure = failure; store.fence(); }
        failDeferred(failure);
    }

    /** Pool owners must call after shutting Ratis down and before closing any DB/CF handles.
     * Failure leaves readers and native ownership intact; the pool must remain open. */
    public void awaitNativeReaders(java.time.Duration timeout) throws IOException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (store.activeSnapshotPins() != 0) {
            if (System.nanoTime() >= deadline || Thread.currentThread().isInterrupted()) {
                throw new IOException("Native snapshot readers remain active: " + store.activeSnapshotPins());
            }
            java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
        }
    }

    @Override public void close() { beginShutdown(); }

    private record Image(Path file, TermIndex position, NativeRangeImage.Manifest manifest) {}

    private Image image(Path file) throws IOException {
        var position = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(file.toFile());
        return new Image(file, position, validateImage(file, position));
    }

    private void syncImage(Image image) throws IOException {
        force(image.file());
        var md5 = MD5FileUtil.computeMd5ForFile(image.file().toFile());
        MD5FileUtil.saveMD5File(image.file().toFile(), md5);
        force(MD5FileUtil.getDigestFileForFile(image.file().toFile()).toPath());
        force(image.file().getParent());
    }

    private void retainImage(Image image) throws IOException {
        Files.createDirectories(recoveryDir);
        Path target = recoveryDir.resolve(image.file().getFileName());
        if (!Files.exists(target)) Files.createLink(target, image.file());
        else if (!Arrays.equals(image(target).manifest().sha256(), image.manifest().sha256())) {
            throw new IOException("Conflicting snapshot recovery image at " + image.position());
        }
        syncImage(image(target));
        force(recoveryDir);
        force(raftRoot);
    }

    private java.util.List<Image> images(Path directory, boolean staging) throws IOException {
        var result = new java.util.ArrayList<Image>();
        if (!Files.exists(directory)) return result;
        try (var files = Files.list(directory)) {
            for (var file : files.toList()) {
                if (!SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(file.getFileName().toString()).matches()) continue;
                // A sidecar is written only after the last chunk. Partial transfers remain inert.
                if (staging && !Files.exists(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath())) continue;
                try { result.add(image(file)); }
                catch (IOException | RuntimeException invalid) {
                    if (!staging) throw new IOException("Invalid durable snapshot " + file, invalid);
                }
            }
        }
        return result;
    }

    private java.util.List<Image> stagedImages() throws IOException {
        var result = new java.util.ArrayList<Image>();
        if (!Files.exists(snapshotTmp)) return result;
        try (var directories = Files.list(snapshotTmp)) {
            for (var directory : directories.toList()) {
                if (Files.isDirectory(directory) && directory.getFileName().toString().startsWith("snapshot-")) {
                    result.addAll(images(directory, true));
                }
            }
        }
        return result;
    }

    /** Called before stock Ratis moves/deletes the old state-machine directory. */
    private void preserveInstallationImages() throws IOException {
        synchronized (durabilityGate) {
            Path published = snapshots.getSnapshotFile(0, 0).toPath().getParent();
            for (var image : images(published, false)) retainImage(image);
            for (var image : stagedImages()) {
                syncImage(image);
                force(snapshotTmp);
                retainImage(image);
            }
            force(raftRoot);
        }
    }

    private void recoverStagingImages() throws IOException {
        synchronized (durabilityGate) {
            Path published = snapshots.getSnapshotFile(0, 0).toPath().getParent();
            // Corrupt published snapshots are repair failures, never skipped for an older image.
            var candidates = new java.util.ArrayList<>(images(published, false));
            candidates.addAll(images(recoveryDir, false));
            for (var staged : stagedImages()) { syncImage(staged); retainImage(staged); candidates.add(staged); }
            Image latest = null;
            for (var candidate : candidates) {
                if (latest == null || candidate.position().getIndex() > latest.position().getIndex()) latest = candidate;
                else if (candidate.position().getIndex() == latest.position().getIndex()
                        && !Arrays.equals(candidate.manifest().sha256(), latest.manifest().sha256())) {
                    throw new IOException("Snapshot recovery cuts disagree");
                }
            }
            Files.createDirectories(published);
            if (latest != null) {
                Path target = published.resolve(latest.file().getFileName());
                if (!Files.exists(target)) Files.createLink(target, latest.file());
                syncImage(image(target));
            }
            force(published);
            force(raftRoot);
        }
    }

    /** Final InstallSnapshot SUCCESS interceptor barrier; native reload may still be in progress. */
    public void syncReceivedSnapshot(long term, long index) throws IOException {
        try {
            synchronized (durabilityGate) {
                Path received = snapshots.getSnapshotFile(term, index).toPath();
                var image = image(received);
                syncImage(image);
                retainImage(image);
                force(received.getParent());
                force(raftRoot);
            }
        } catch (Exception failure) {
            synchronized (gate) { storageFailure = failure; store.fence(); }
            failDeferred(failure);
            throw failure instanceof IOException io ? io : new IOException("Received snapshot is not durable", failure);
        }
    }

    private record Completion(CompletableFuture<Message> future, Message result) {}

    @Override public void reinitialize() throws IOException {
        admissionGeneration.incrementAndGet(); invalidateProposalCandidates(); store.invalidateScopedSnapshots();
        var completions = new java.util.ArrayList<Completion>();
        var coveredOperations = new java.util.ArrayList<OperationId>();
        IOException failure = null;
        synchronized (gate) {
            getLifeCycle().transition(LifeCycle.State.STARTING);
            try {
                if (storageFailure != null) throw new IOException("Unhealthy range cannot reload", storageFailure);
                recover(true);
                var a = store.applied();
                for (var entry : deferred.headMap(a.index(), true).entrySet()) {
                    var pending = entry.getValue();
                    if (pending.term() > a.term() || entry.getKey() == a.index() && pending.term() != a.term()) {
                        throw new IOException("Deferred term conflicts with installed image");
                    }
                    var recovered=recoveredRecord(pending.encoded());
                    byte[] result=recovered.result();
                    if (result == null) throw new IOException("Installed image omitted deferred receipt");
                    completions.add(new Completion(pending.future(), Message.valueOf(ByteString.copyFrom(result))));
                    coveredOperations.add(recovered.operation());
                }
                for (var entry : pendingNoops.headMap(a.index(), true).entrySet()) {
                    if (entry.getValue() > a.term() || entry.getKey() == a.index() && entry.getValue() != a.term()) {
                        throw new IOException("Deferred metadata term conflicts with installed image");
                    }
                }
                deferred.headMap(a.index(), true).values().forEach(pending -> deferredBytes -= pending.encoded().length);
                deferred.headMap(a.index(), true).clear();
                pendingNoops.headMap(a.index(), true).clear();
                configurations.headMap(a.index(), true).clear();
                store.activate();
            store.enableScopedHistory(it.cavallium.rockserver.core.common.distributed.RangeMutationHistory.Limits.defaults());
                if (enableSessionControls) store.enableSessionControls();
                var verifiedAdmissions=verifiedNativeAdmissionOutcomes();
                synchronized (admissions) {
                    for (var terminal:verifiedAdmissions.terminalScopes()) if (admissions.get(terminal.id)==terminal) retirePreparationScope(terminal);
                    for (var verified:verifiedAdmissions.settled()) if (admissions.get(verified.id)==verified) settleAdmission(verified);
                    for (var id : coveredOperations) {
                        var admission = admissions.get(id);
                        if (admission != null) settleAdmission(admission);
                    }
                }
                getLifeCycle().transition(LifeCycle.State.RUNNING);
            } catch (Exception problem) { failure = failRecovery(problem); }
        }
        if (failure != null) {
            // Nothing succeeds until every covered receipt has been verified.
            failDeferred(failure);
            for (var completion : completions) completion.future().completeExceptionally(failure);
            throw failure;
        }
        for (var completion : completions) completion.future().complete(completion.result());
    }

    private IOException failRecovery(Exception failure) {
        storageFailure = failure;
        store.fence();
        return failure instanceof IOException io ? io : new IOException("Native range recovery failed", failure);
    }

    private void failDeferred(Throwable failure) {
        java.util.List<Deferred> abandoned;
        synchronized (gate) {
            abandoned = java.util.List.copyOf(deferred.values());
            deferred.clear(); deferredBytes = 0;
        }
        for (var pending : abandoned) pending.future().completeExceptionally(failure);
    }

    private void advance(long term, long index) {
        if (index > getLastAppliedTermIndex().getIndex()) updateLastAppliedTermIndex(term, index);
        else if (index == getLastAppliedTermIndex().getIndex() && term != getLastAppliedTermIndex().getTerm()) {
            throw new IllegalStateException("Conflicting applied term");
        }
    }

    @Override public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        var transaction = super.startTransaction(request);
        try {
            requireHealthy();
            long generation = admissionGeneration.get();
            Leadership leadership = leadership();
            if (!leadership.ready()) throw new IllegalStateException("Leader is not ready for admission");
            byte[] encoded = request.getMessage().getContent().toByteArray();
            if (RatisParticipantRecord.isParticipant(encoded)) return RatisParticipantRecord.isFinalization(encoded) ? startFinalization(transaction,encoded,generation,leadership) : startParticipant(transaction,encoded,generation,leadership);
            var command = DistributedCommandCodec.decode(encoded);
            if (command.transactionRejection()) throw new IllegalArgumentException("Internal rejection records cannot be submitted by clients");
            commandValidator.accept(command);
            store.validate(command);
            byte[] digest = command.payloadDigest();
            var rejection=command.acceptance()==null ? null : DistributedCommand.rejectAcceptedTransaction(command);
            var rejectionData=rejection==null ? null : ByteString.copyFrom(DistributedCommandCodec.encode(rejection));
            var spans=new ArrayList<TransactionScope.Span>(command.mutations().size());
            for (var mutation:command.mutations()) spans.add(mutation.kind()==Mutation.Kind.DELETE_RANGE
                    ? TransactionScope.Span.range(mutation.columnId(),mutation.key(),mutation.value()) : TransactionScope.Span.point(mutation.columnId(),mutation.key()));
            var footprint=proposalFootprints.prepare(spans);
            // Native receipt reads stay outside the short admission lock. If apply
            // advanced while reading, retry validation before acquiring ownership.
            for (int attempt = 0; attempt < 3; attempt++) {
                var originalEstimate = store.estimateReceiptRetention(command,enableSessionControls);
                var rejectionEstimate=rejection==null ? originalEstimate : store.estimateReceiptRetention(rejection,enableSessionControls);
                var observed = originalEstimate.usage().appliedPosition();
                if (!java.util.Objects.equals(observed,rejectionEstimate.usage().appliedPosition()) || originalEstimate.newReceipt()!=rejectionEstimate.newReceipt()) continue;
                if (observed == null || observed.term() != leadership.term()) {
                    throw new IllegalStateException("Current-term startup prefix has not completed native apply");
                }
                // Previously committed outcomes and expired identities cannot advance the fence.
                if (originalEstimate.newReceipt() && command.minimumSessionEpoch() >= 0) epochFloorAdmission.accept(command);
                synchronized (admissions) {
                    var estimate=originalEstimate;
                    requireHealthy();
                    if (generation != admissionGeneration.get()) throw new IllegalStateException("Admission leadership generation changed");
                    if (!store.admissionOpen()) throw new IllegalStateException("Native range admission is closed");
                    if (!java.util.Objects.equals(observed, store.applied())) continue;
                    reconcileOlderProposalTerms(leadership.term());
                    if (finalizationOwners.containsKey(command.operationId()) || participantOwners.containsKey(command.operationId())) throw new IllegalStateException("Operation identity belongs to participant preparation");
                    var known=admissions.get(command.operationId());
                    if (known!=null && !estimate.newReceipt()) settleAdmission(known);
                    var admission = admissions.get(command.operationId());
                    if (admission != null) {
                        if (!Arrays.equals(admission.digest, digest)) {
                            throw new IllegalArgumentException("Operation identity reused with different payload");
                        }
                        if (admission.term != leadership.term() || admission.generation != generation) {
                            throw new IllegalStateException("Earlier admission is still settling; retry the same identity");
                        }
                        admission.owners++;
                    } else {
                        if (command.acceptance()==null && (preparingCommits.containsKey(command.operationId()) || preparedCommits.containsKey(command.operationId()))) throw new IllegalStateException("Operation identity is reserved by transaction preparation");
                        var accepted=command.acceptance()==null || !estimate.newReceipt() ? null : preparedCommits.get(command.operationId());
                        boolean reject=false;
                        if (command.acceptance()!=null && estimate.newReceipt()) {
                            if (preparingCommits.containsKey(command.operationId())) throw new IllegalStateException("Operation identity is still being prepared");
                            if (accepted!=null && !Arrays.equals(accepted.digest,digest)) throw new IllegalArgumentException("Operation identity reused with different prepared payload");
                            if (accepted!=null && !accepted.authorized()) { accepted.close(); accepted=null; }
                            reject=accepted==null;
                            if (reject) {
                                estimate=rejectionEstimate;
                            }
                        }
                        if (estimate.newReceipt() && (command.acceptance()==null || reject) && estimate.maintenance() && proposalFootprints.stats().candidates()>0) throw new IllegalStateException("Transaction reservations hold session metadata stable");
                        boolean maintenance = estimate.newReceipt() && estimate.maintenance();
                        boolean protectedControl = estimate.maintenance() && command.mutations().isEmpty() && command.predicates().isEmpty();
                        if (protectedControl ? protectedAdmissions+protectedFinalizationSlots >= 16
                                : dataAdmissions >= 1024 || dataProposalBytes + encoded.length > 16L * 1024 * 1024) {
                            throw new IllegalStateException("Range proposal budget exhausted");
                        }
                        if (maintenance && maintenanceReservation != null) throw new IllegalStateException("Receipt maintenance is already in flight");
                        long countLimit = retentionLimits.dataCount() + (maintenance ? retentionLimits.maintenanceCount() : 0);
                        long byteLimit = retentionLimits.dataBytes() + (maintenance ? retentionLimits.maintenanceBytes() : 0);
                        if (!retentionFits(estimate.usage().retainedCount(),reservedReceiptCount,estimate.additionalCount(),countLimit)
                                || !retentionFits(estimate.usage().retainedBytes(),reservedReceiptBytes,estimate.additionalBytes(),byteLimit)) {
                            throw new IllegalStateException("Retained receipt budget exhausted; acknowledge outcomes or expire epochs");
                        }
                        var lease=!reject && accepted==null && estimate.newReceipt() && !footprint.isEmpty() ? proposalFootprints.registerOrdinary(footprint,leadership.term(),generation) : null;
                        if (!reject && accepted==null && estimate.newReceipt() && !footprint.isEmpty() && lease==null) throw new IllegalStateException("Proposal overlaps a reserved transaction scope");
                        try {
                            admission = new Admission(command.operationId(), digest, encoded.length, leadership.term(), generation,
                                    estimate.additionalCount(),estimate.additionalBytes(),maintenance,protectedControl,lease,accepted==null ? null : accepted.reservation.lease,reject ? rejectionData : null);
                            admissions.put(admission.id, admission);
                        } catch (RuntimeException | Error failure) {
                            if (admission!=null) admissions.remove(admission.id,admission);
                            if (lease!=null) proposalFootprints.release(lease); throw failure;
                        }
                        if (accepted!=null) { proposalFootprints.submit(accepted.reservation.lease); accepted.claimed=true; preparedCommits.remove(admission.id,accepted); preparedCommitBytes-=accepted.bytes; }
                        admittedBytes += admission.bytes;
                        if (protectedControl) protectedAdmissions++; else { dataAdmissions++; dataProposalBytes+=admission.bytes; }
                        reservedReceiptCount+=admission.receiptCount; reservedReceiptBytes+=admission.receiptBytes;
                        if (maintenance) maintenanceReservation=admission;
                    }
                    transaction.setStateMachineContext(admission);
                    if (admission.rejectionLogData==null) return transaction;
                    try {
                        return TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request).setLogData(admission.rejectionLogData).setStateMachineContext(admission).build();
                    } catch (RuntimeException | Error failure) { releaseAdmission(transaction); throw failure; }
                }
            }
            throw new IllegalStateException("Range changed during admission; retry with same identity");
        } catch (Exception rejected) {
            return transaction.setException(rejected);
        }
    }

    private Leadership leadership() throws IOException {
        if (leadershipSource != null) return leadershipSource.get();
        RaftServer server = getServer().getNow(null);
        if (server == null) throw new IOException("Raft division is not initialized");
        var division = server.getDivision(getGroupId());
        synchronized (division) {
            var info = division.getInfo();
            return new Leadership(info.getCurrentTerm(), info.isLeader() && info.isLeaderReady());
        }
    }

    /** Ratis invokes this under its append locks; recheck only in-memory ownership and term. */
    @Override public TransactionContext preAppendTransaction(TransactionContext transaction) throws IOException {
        try {
            requireHealthy();
            Leadership current = leadership();
            var nativePosition = store.applied();
            synchronized (admissions) {
                requireHealthy();
                if (!(transaction.getStateMachineContext() instanceof Admission admission)
                        || admissions.get(admission.id) != admission || admission.owners <= 0
                        || !current.ready() || admission.term != current.term()
                        || admission.generation != admissionGeneration.get()
                        || nativePosition == null || nativePosition.term() != current.term() || !store.admissionOpen()) {
                    throw new IOException("Admission lost its completed current-term owner before append");
                }
            }
            return super.preAppendTransaction(transaction);
        } catch (Exception stale) {
            releaseAdmission(transaction);
            // IOException alone would make Ratis step down the healthy replacement leader.
            throw new org.apache.ratis.protocol.exceptions.StateMachineException("Stale proposal admission", stale, false);
        }
    }

    private record AppliedRecord(OperationId operation,byte[] result) {}
    private AppliedRecord applyRecord(ApplyPosition positionArg,byte[] encoded) throws Exception {
        if (RatisParticipantRecord.isFinalization(encoded)) {
            var finalization=RatisParticipantRecord.decodeFinalization(encoded);
            var position=finalization.decision().phase()==TransactionStatus.Phase.COMMIT ? store.applyCommit(positionArg,finalization).position() : store.applyAbort(positionArg,finalization).position();
            synchronized (admissions) { retireFinalizedPreparations(finalization); }
            return new AppliedRecord(finalization.operation(),RatisParticipantRecord.result(position));
        }
        if (RatisParticipantRecord.isParticipant(encoded)) {
            var preparation=RatisParticipantRecord.decode(encoded); commandValidator.accept(preparation.business());
            var record=store.applyPrepared(positionArg,preparation);
            return new AppliedRecord(preparation.operation(),RatisParticipantRecord.result(record));
        }
        var command=DistributedCommandCodec.decode(encoded); commandValidator.accept(command);
        return new AppliedRecord(command.operationId(),store.apply(positionArg,command));
    }
    private AppliedRecord recoveredRecord(byte[] encoded) throws Exception {
        if (RatisParticipantRecord.isFinalization(encoded)) {
            var finalization=RatisParticipantRecord.decodeFinalization(encoded); var position=store.recoveredFinalization(finalization);
            return new AppliedRecord(finalization.operation(),position==null ? null : RatisParticipantRecord.result(position));
        }
        if (RatisParticipantRecord.isParticipant(encoded)) {
            var preparation=RatisParticipantRecord.decode(encoded); var record=store.recoveredPreparationVote(preparation);
            return new AppliedRecord(preparation.operation(),record==null ? null : RatisParticipantRecord.result(record));
        }
        var command=DistributedCommandCodec.decode(encoded);
        return new AppliedRecord(command.operationId(),store.operationResult(command.operationId(),command.receiptDigest()));
    }
    @Override public CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
        Deferred completion = null;
        Message result = null;
        Throwable failure = null;
        synchronized (gate) {
            try {
                if (storageFailure != null) requireHealthy();
                var entry = transaction.getLogEntry();
                if (pendingNoops.containsKey(entry.getIndex()) || configurations.containsKey(entry.getIndex())) {
                    throw new IllegalStateException("Log entry changed from metadata to data");
                }
                byte[] encoded = entry.getStateMachineLogEntry().getLogData().toByteArray();
                var pending = deferred.get(entry.getIndex());
                if (pending != null && (pending.term() != entry.getTerm() || !Arrays.equals(pending.encoded(), encoded))) {
                    throw new IllegalStateException("Deferred/replayed log identity changed");
                }
                if (getLifeCycleState() != LifeCycle.State.RUNNING) {
                    if (pending == null) {
                        if (deferred.size() >= 1024 || deferredBytes + encoded.length > 16L * 1024 * 1024) {
                            throw new IllegalStateException("Paused apply budget exhausted");
                        }
                        pending = new Deferred(entry.getTerm(), encoded, new CompletableFuture<>());
                        deferred.put(entry.getIndex(), pending); deferredBytes += encoded.length;
                    }
                    return pending.future().whenComplete((message, problem) -> releaseAdmission(transaction));
                }
                var applied=applyRecord(new ApplyPosition(entry.getTerm(),entry.getIndex()),encoded);
                byte[] bytes=applied.result();
                synchronized (admissions) {
                    var admitted = admissions.get(applied.operation());
                    if (admitted != null) settleAdmission(admitted);
                }
                advance(entry.getTerm(), entry.getIndex());
                result = Message.valueOf(ByteString.copyFrom(bytes));
                if (pending != null) {
                    deferred.remove(entry.getIndex()); deferredBytes -= pending.encoded().length;
                    completion = pending;
                }
            } catch (Throwable problem) {
                storageFailure = problem;
                failure = problem;
            }
        }
        releaseAdmission(transaction);
        if (failure != null) {
            failDeferred(failure);
            return CompletableFuture.failedFuture(failure);
        }
        if (completion != null) completion.future().complete(result);
        return CompletableFuture.completedFuture(result);
    }

    @Override public TransactionContext cancelTransaction(TransactionContext transaction) throws IOException {
        releaseAdmission(transaction);
        return super.cancelTransaction(transaction);
    }

    @Override public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
        admissionGeneration.incrementAndGet(); invalidateProposalCandidates(); store.invalidateScopedSnapshots();
        for (var transaction : pendingEntries) releaseAdmission(transaction);
    }

    private void releaseAdmission(TransactionContext transaction) {
        synchronized (admissions) {
            if (transaction.getStateMachineContext() instanceof Admission admission) {
                transaction.setStateMachineContext(null);
                if (transaction.getLogEntry()!=null) admission.maybeAppended=true;
                if (--admission.owners==0 && (!admission.maybeAppended || admission.settled)) finishAdmission(admission);
            }
        }
    }
    // Caller holds admissions. Exact object identity protects a later replacement from old callbacks.
    private void finishAdmission(Admission admission) {
        if (!admissions.remove(admission.id,admission)) return;
        releaseRetention(admission); releaseFootprint(admission);
        admittedBytes-=admission.bytes;
        if (admission.protectedControl) protectedAdmissions--; else { dataAdmissions--; dataProposalBytes-=admission.bytes; }
    }
    private void settleAdmission(Admission admission) {
        admission.settled=true; releaseRetention(admission); releaseFootprint(admission);
        if (admission.owners==0) finishAdmission(admission);
    }
    // Native terminal state prevents every matching PREPARE from acquiring locks again.
    private void retireFinalizedPreparations(ParticipantFinalization finalization) {
        for (var plan:List.copyOf(preparedParticipants.values())) if (matchesFinalization(finalization,plan.preparation)) plan.close();
        for (var preparing:preparingParticipants.values()) {
            var hold=preparing.hold();
            if (hold.transaction.equals(finalization.transactionId()) && hold.business.equals(finalization.decision().operation())
                    && (hold.preparation==null || matchesFinalization(finalization,hold.preparation))) {
                proposalFootprints.release(preparing.reservation().lease); finishParticipantHold(hold,true);
            }
        }
        for (var admission:admissions.values()) if (admission.participant!=null && matchesFinalization(finalization,admission.participant)) retirePreparationScope(admission);
    }
    private static boolean matchesFinalization(ParticipantFinalization finalization,PreparedParticipant.Preparation preparation) {
        try { finalization.requirePreparation(preparation); return true; } catch (IllegalArgumentException different) { return false; }
    }
    private void retirePreparationScope(Admission admission) {
        if (admission.acceptedScope!=null) proposalFootprints.release(admission.acceptedScope);
        if (admission.participantHold!=null) finishParticipantHold(admission.participantHold,true);
    }
    private void releaseFootprint(Admission admission) {
        if (admission.footprint!=null) proposalFootprints.release(admission.footprint);
        if (admission.acceptedScope!=null) proposalFootprints.release(admission.acceptedScope);
        if (admission.participantHold!=null) releaseParticipantHold(admission.participantHold,admission.settled);
        if (admission.finalizationHold!=null) releaseFinalizationHold(admission.finalizationHold,admission.settled);
    }
    // Recovery reads one exact native cut outside the admissions monitor. Unknown suffixes remain reserved.
    private NativeAdmissionProof verifiedNativeAdmissionOutcomes() throws Exception {
        final List<Admission> pending;
        synchronized (admissions) { pending=admissions.values().stream().filter(admission -> !admission.settled).toList(); }
        if (pending.isEmpty()) return new NativeAdmissionProof(List.of(),List.of()); var verified=new ArrayList<Admission>(); var terminalScopes=new ArrayList<Admission>();
        try (var pin=store.capture()) {
            for (var admission:pending) {
                try {
                    if (admission.finalization!=null) { if (pin.finalizationPosition(admission.finalization)!=null) verified.add(admission); }
                    else if (admission.participant==null) { if (pin.operationResult(admission.id,admission.digest)!=null) verified.add(admission); }
                    else { var vote=pin.preparationVote(admission.participant); if (vote!=null) verified.add(admission); else if (pin.finalizedPreparation(admission.participant)) terminalScopes.add(admission); }
                }
                catch (IllegalArgumentException differentPayload) { /* A mismatched speculative request is not verified coverage. */ }
            }
        }
        return new NativeAdmissionProof(verified,terminalScopes);
    }
    private record NativeAdmissionProof(List<Admission> settled,List<Admission> terminalScopes) {}
    private void invalidateProposalCandidates() { synchronized (admissions) { invalidateUnsubmitted(); } }
    private void invalidateUnsubmitted() {
        proposalFootprints.invalidateCandidates();
        for (var plan:preparedCommits.values()) { plan.closed=true; preparedCommitBytes-=plan.bytes; }
        preparedCommits.clear();
        for (var plan:preparedParticipants.values()) { plan.closed=true; preparedCommitBytes-=plan.bytes.length; releaseParticipantHold(plan.hold); }
        preparedParticipants.clear();
        for (var plan:List.copyOf(preparedFinalizations.values())) plan.close();
        // Invalidated native planners still own their input/result payloads until their finally exits.
    }
    /** Only a strictly newer ready native term proves an older term's unknown suffix can no longer interpose. */
    private void reconcileOlderProposalTerms(long term) {
        if (term<=reconciledProposalTerm) return;
        invalidateUnsubmitted();
        for (var admission:List.copyOf(admissions.values())) if (admission.term<term) settleAdmission(admission);
        reconciledProposalTerm=term;
    }
    public ProposalFootprints.Stats proposalFootprintStats() { synchronized (admissions) { return proposalFootprints.stats(); } }
    /** Unsubmitted exclusive scope guard only. Native validation and canonical append acceptance are still separate work. */
    public final class ProposalReservation implements AutoCloseable {
        private final ProposalFootprints.CandidateLease lease;
        private ProposalReservation(ProposalFootprints.CandidateLease lease) { this.lease=lease; }
        public long term() { return lease.term(); }
        public long generation() { return lease.generation(); }
        public boolean active() {
            var applied=store.applied();
            return lease.active() && lease.generation()==admissionGeneration.get() && applied!=null && applied.term()==lease.term()
                    && storageFailure==null && getLifeCycleState()==LifeCycle.State.RUNNING && store.admissionOpen();
        }
        @Override public void close() { synchronized (admissions) { proposalFootprints.release(lease); } }
    }
    /** Frozen bytes must be retained by the caller before its first Ratis RPC. Closing abandons only unsubmitted work. */
    record ProposalDeadline(java.util.function.LongSupplier nanos,long opened,long ttl) {
        ProposalDeadline { java.util.Objects.requireNonNull(nanos); if (ttl<=0) throw new IllegalArgumentException("Invalid proposal deadline"); }
        boolean active() { return nanos.getAsLong()-opened<ttl; }
    }
    public final class ScopedCommit implements AutoCloseable {
        private final ProposalReservation reservation;
        private final DistributedCommand command;
        private final byte[] digest;
        private final int bytes;
        private final ProposalDeadline deadline;
        private boolean claimed,closed;
        private ScopedCommit(ProposalReservation reservation,DistributedCommand command,ProposalDeadline deadline) {
            this.reservation=reservation; this.command=command; this.digest=command.payloadDigest(); this.bytes=DistributedCommandCodec.encodedSize(command);
            this.deadline=deadline;
        }
        private boolean authorized() { return !closed && !claimed && reservation.active() && (deadline==null || deadline.active()); }
        public DistributedCommand command() { return command; }
        public boolean submitted() { synchronized (admissions) { return claimed; } }
        @Override public void close() {
            synchronized (admissions) {
                if (closed || claimed) return; closed=true;
                if (preparedCommits.remove(command.operationId(),this)) preparedCommitBytes-=bytes;
                proposalFootprints.release(reservation.lease);
            }
        }
    }
    public final class FinalizationProposal implements AutoCloseable {
        private final FinalizationHold hold; private final ParticipantFinalization finalization; private final byte[] bytes,digest;
        private boolean claimed,closed;
        private FinalizationProposal(FinalizationHold hold,ParticipantFinalization finalization) { this.hold=hold; this.finalization=finalization; bytes=RatisParticipantRecord.encode(finalization); digest=RatisParticipantRecord.digest(finalization); }
        public ParticipantFinalization finalization() { return finalization; }
        public byte[] bytes() { return bytes.clone(); }
        private boolean authorized() { var a=store.applied(); return !closed && !claimed && hold.generation==admissionGeneration.get() && a!=null && a.term()==hold.term && store.admissionOpen(); }
        @Override public void close() {
            synchronized (admissions) {
                if (closed || claimed) return; closed=true;
                if (preparedFinalizations.remove(hold.recovery,this)) { protectedFinalizationSlots--; releaseFinalizationHold(hold,false); }
            }
        }
    }
    // Reattach exact live acceptance. The view must not close or donate the original owner's reservation.
    private FinalizationProposal borrowedFinalization(FinalizationHold hold,ParticipantFinalization finalization) {
        var proposal=new FinalizationProposal(hold,finalization); proposal.claimed=true; return proposal;
    }
    /** Privileged coordinator seam. Bounded protected capacity is held while the fresh home read is in flight. */
    public FinalizationProposal prepareFinalization(RatisTransactionStatuses homes,UUID transaction,OperationId recovery) throws Exception {
        java.util.Objects.requireNonNull(homes); java.util.Objects.requireNonNull(transaction); java.util.Objects.requireNonNull(recovery);
        requireHealthy(); long generation=admissionGeneration.get(); var leader=leadership(); if (!leader.ready()) throw new IllegalStateException("Finalization leader is not ready");
        FinalizationHold hold;
        synchronized (admissions) {
            requireHealthy(); var a=store.applied();
            if (generation!=admissionGeneration.get() || a==null || a.term()!=leader.term() || !store.admissionOpen()) throw new IllegalStateException("Finalization has no current native owner");
            reconcileOlderProposalTerms(leader.term());
            var ready=preparedFinalizations.get(recovery);
            if (ready!=null && ready.finalization.transactionId().equals(transaction) && ready.authorized()) return borrowedFinalization(ready.hold,ready.finalization);
            var known=admissions.get(recovery);
            if (known!=null && known.finalization!=null && known.finalization.transactionId().equals(transaction) && known.term==leader.term() && known.generation==generation) return borrowedFinalization(known.finalizationHold,known.finalization);
            if (protectedAdmissions+protectedFinalizationSlots>=16) throw new IllegalStateException("Protected proposal budget exhausted");
            if (participantIdentityBusy(recovery) || finalizingTransactions.containsKey(transaction)) throw new IllegalStateException("Finalization identity/transaction is already owned");
            hold=new FinalizationHold(transaction,recovery,leader.term(),generation); preparingFinalizations.put(recovery,hold); finalizingTransactions.put(transaction,hold); finalizationOwners.put(recovery,hold); protectedFinalizationSlots++;
        }
        boolean retained=false;
        try {
            var finalization=homes.readFinalization(transaction,store.ownership().owner().groupId(),recovery);
            for (int attempt=0;attempt<3;attempt++) {
                var estimate=store.estimateFinalization(finalization); var capacity=estimate.capacity();
                synchronized (admissions) {
                    requireHealthy();
                    if (hold.generation!=admissionGeneration.get() || preparingFinalizations.get(recovery)!=hold || capacity.position()==null || capacity.position().term()!=hold.term || !store.admissionOpen()) throw new IllegalStateException("Finalization lost its acceptance owner");
                    if (!java.util.Objects.equals(capacity.position(),store.applied())) continue;
                    var business=finalization.decision().operation();
                    if (finalizationOwners.containsKey(business) || preparedCommits.containsKey(business) || preparingCommits.containsKey(business)) throw new IllegalStateException("Finalization business identity belongs to other work");
                    var pending=admissions.get(business);
                    if (pending!=null && (pending.participant==null || !pending.participant.transactionId().equals(transaction))) throw new IllegalStateException("Business operation is still settling outside this transaction");
                    var shared=participantOwners.get(business);
                    if (shared!=null) {
                        if (!shared.transaction.equals(transaction)) throw new IllegalArgumentException("Finalization shares another transaction's identity");
                        if (shared.preparation!=null) finalization.requirePreparation(shared.preparation);
                    }
                    if (estimate.newFence()) {
                        if (shared!=null && !shared.capacityReleased) { shared.finalizers++; hold.shared=shared; }
                        else {
                            if (capacity.count()+reservedParticipantCount+reservedFinalizationCount>=ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS || ReplicatedRangeStore.FINALIZATION_RESERVATION>ReplicatedRangeStore.MAX_PREPARED_BYTES-capacity.bytes()-reservedParticipantBytes-reservedFinalizationBytes) throw new IllegalStateException("Finalization fence reservation exhausted");
                            hold.capacity=true; reservedFinalizationCount++; reservedFinalizationBytes+=ReplicatedRangeStore.FINALIZATION_RESERVATION;
                        }
                    }
                    hold.business=business; finalizationOwners.put(business,hold);
                    var proposal=new FinalizationProposal(hold,finalization); preparedFinalizations.put(recovery,proposal); preparingFinalizations.remove(recovery,hold); retained=true; return proposal;
                }
            }
            throw new IllegalStateException("Native finalization cut kept changing");
        } finally {
            synchronized (admissions) { if (!retained && preparingFinalizations.remove(recovery,hold)) { protectedFinalizationSlots--; releaseFinalizationHold(hold,false); } }
        }
    }
    private TransactionContext startFinalization(TransactionContext transaction,byte[] encoded,long generation,Leadership leadership) throws Exception {
        var finalization=RatisParticipantRecord.decodeFinalization(encoded); byte[] digest=RatisParticipantRecord.digest(finalization);
        for (int attempt=0;attempt<3;attempt++) {
            var estimate=store.estimateFinalization(finalization); var observed=estimate.capacity().position();
            if (observed==null || observed.term()!=leadership.term()) throw new IllegalStateException("Finalization native prefix is not ready");
            synchronized (admissions) {
                requireHealthy(); if (generation!=admissionGeneration.get() || !store.admissionOpen()) throw new IllegalStateException("Finalization owner changed");
                if (!java.util.Objects.equals(observed,store.applied())) continue; reconcileOlderProposalTerms(leadership.term());
                var known=admissions.get(finalization.operation());
                if (known!=null && !Arrays.equals(known.digest,digest)) throw new IllegalArgumentException("Finalization identity payload changed");
                if (known!=null && estimate.existing()!=null) { settleAdmission(known); known=admissions.get(finalization.operation()); }
                if (known!=null) {
                    if (known.finalization==null || known.term!=leadership.term() || known.generation!=generation) throw new IllegalStateException("Previous finalization is still settling");
                    known.owners++; transaction.setStateMachineContext(known); return transaction;
                }
                var accepted=estimate.existing()==null ? preparedFinalizations.get(finalization.operation()) : null;
                if (estimate.existing()==null && (accepted==null || !Arrays.equals(accepted.digest,digest) || !accepted.authorized())) throw new IllegalStateException("Finalization has no fresh home acceptance authority");
                if (protectedAdmissions+protectedFinalizationSlots-(accepted==null ? 0 : 1)>=16) throw new IllegalStateException("Protected proposal budget exhausted");
                var admission=new Admission(finalization.operation(),digest,encoded.length,leadership.term(),generation,0,0,false,true,null,null,null);
                admission.finalization=finalization; admission.finalizationHold=accepted==null ? null : accepted.hold; admissions.put(admission.id,admission);
                if (accepted!=null) { accepted.claimed=true; preparedFinalizations.remove(admission.id,accepted); protectedFinalizationSlots--; }
                protectedAdmissions++; admittedBytes+=admission.bytes; transaction.setStateMachineContext(admission); return transaction;
            }
        }
        throw new IllegalStateException("Native finalization cut changed during append admission");
    }
    public int protectedFinalizationSlots() { synchronized (admissions) { return protectedFinalizationSlots; } }
    public long reservedFinalizationCount() { synchronized (admissions) { return reservedFinalizationCount; } }
    public final class ParticipantProposal implements AutoCloseable {
        private final ProposalReservation reservation; private final ParticipantHold hold;
        private final PreparedParticipant.Preparation preparation; private final byte[] bytes,digest; private final ProposalDeadline deadline;
        private boolean claimed,closed;
        private ParticipantProposal(ProposalReservation reservation,ParticipantHold hold,PreparedParticipant.Preparation preparation,ProposalDeadline deadline) {
            this.reservation=reservation; this.hold=hold; this.preparation=preparation; this.deadline=deadline;
            bytes=RatisParticipantRecord.encode(preparation); digest=RatisParticipantRecord.digest(preparation);
        }
        private boolean authorized() { return !closed && !claimed && reservation.active() && (deadline==null || deadline.active()); }
        public PreparedParticipant.Preparation preparation() { return preparation; }
        public byte[] bytes() { return bytes.clone(); }
        @Override public void close() {
            synchronized (admissions) {
                if (closed || claimed) return; closed=true;
                if (preparedParticipants.remove(preparation.operation(),this)) preparedCommitBytes-=bytes.length;
                releaseParticipantHold(hold); proposalFootprints.release(reservation.lease);
            }
        }
    }
    private void releaseParticipantHold(ParticipantHold hold) { releaseParticipantHold(hold,false); }
    private void releaseParticipantHold(ParticipantHold hold,boolean covered) {
        hold.released=true; finishParticipantHold(hold,covered);
    }
    private void finishParticipantHold(ParticipantHold hold,boolean covered) {
        if (!hold.capacityReleased && (covered || hold.released && hold.finalizers==0)) {
            hold.capacityReleased=true; reservedParticipantCount--; reservedParticipantBytes-=hold.bytes; reservedParticipantScopeBytes-=hold.scopeBytes;
        }
        if (hold.released && hold.finalizers==0) { participantOwners.remove(hold.control,hold); participantOwners.remove(hold.business,hold); }
    }
    private void releaseFinalizationHold(FinalizationHold hold,boolean covered) {
        if (hold.released) return; hold.released=true;
        finalizationOwners.remove(hold.recovery,hold); if (hold.business!=null) finalizationOwners.remove(hold.business,hold); finalizingTransactions.remove(hold.transaction,hold);
        if (hold.capacity) { reservedFinalizationCount--; reservedFinalizationBytes-=ReplicatedRangeStore.FINALIZATION_RESERVATION; }
        if (hold.shared!=null) { var shared=hold.shared; hold.shared=null; shared.finalizers--; finishParticipantHold(shared,covered); }
    }
    private boolean participantIdentityBusy(OperationId id) {
        return finalizationOwners.containsKey(id) || participantOwners.containsKey(id) || admissions.containsKey(id) || preparedCommits.containsKey(id) || preparingCommits.containsKey(id);
    }
    /** Freeze the original read cut/scope/result before registering the home; it is never rebased during PREPARE. */
    public byte[] participantPlanDigest(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand business) throws Exception { return participantIntent(pin,business).payloadDigest(); }
    /** Canonical planning data for durable registration; this grants no PREPARE append authority. */
    public DistributedCommand participantIntent(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand business) throws Exception {
        checkTransactionOwner(); long generation=transactionGeneration(); commandValidator.accept(business);
        try (var stage=pin.stage(business)) {
            checkTransactionOwner(); if (generation!=transactionGeneration() || pin.position().term()!=transactionTerm()) throw new IllegalStateException("Participant read owner changed");
            var accepted=DistributedCommand.acceptedTransaction(business,new it.cavallium.rockserver.core.common.distributed.TransactionAcceptance(pin.position(),generation,transactionIncarnation,pin.scope(),stage.result().encode()));
            commandValidator.accept(accepted); return accepted;
        }
    }
    /** Reattach exact live PREPARE authority after a lost coordinator reply; never recreate a read cut. */
    public ParticipantProposal reattachParticipant(UUID transaction,OperationId control,DistributedCommand accepted) throws IOException {
        java.util.Objects.requireNonNull(transaction); java.util.Objects.requireNonNull(control); java.util.Objects.requireNonNull(accepted);
        if (accepted.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED) throw new IllegalArgumentException("Expected a frozen accepted participant image"); commandValidator.accept(accepted);
        requireHealthy(); long generation=admissionGeneration.get(); var leader=leadership();
        if (!leader.ready()) throw new RatisPreparationUnavailableException("Participant leader is not ready for original acceptance");
        synchronized (admissions) {
            requireHealthy(); var a=store.applied();
            if (generation!=admissionGeneration.get() || a==null || a.term()!=leader.term() || !store.admissionOpen()) throw new RatisPreparationUnavailableException("Original participant owner is unavailable");
            reconcileOlderProposalTerms(leader.term()); var ready=preparedParticipants.get(control);
            if (ready!=null && ready.authorized()) {
                requireParticipantImage(transaction,accepted,ready.preparation);
                var view=new ParticipantProposal(ready.reservation,ready.hold,ready.preparation,ready.deadline); view.claimed=true; return view;
            }
            var known=admissions.get(control);
            if (known!=null && known.participant!=null && known.term==leader.term() && known.generation==generation) {
                requireParticipantImage(transaction,accepted,known.participant);
                var view=new ParticipantProposal(known.acceptedScope==null ? null : new ProposalReservation(known.acceptedScope),known.participantHold,known.participant,null); view.claimed=true; return view;
            }
            throw new RatisPreparationUnavailableException("No live acceptance for the original participant read cut");
        }
    }
    private static void requireParticipantImage(UUID transaction,DistributedCommand accepted,PreparedParticipant.Preparation preparation) {
        if (!transaction.equals(preparation.transactionId()) || !Arrays.equals(DistributedCommandCodec.encode(accepted),DistributedCommandCodec.encode(preparation.business()))) throw new IllegalArgumentException("Participant image differs from frozen acceptance");
    }
    /** Privileged coordinator seam: verifies the committed home registration, then freezes/reserves one accepted participant plan. */
    public ParticipantProposal prepareParticipant(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand business,OperationId control,
            RatisTransactionStatuses homes,UUID transaction) throws Exception {
        return prepareParticipant(pin,business,control,homes,transaction,null);
    }
    ParticipantProposal prepareParticipant(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand business,OperationId control,
            RatisTransactionStatuses homes,UUID transaction,ProposalDeadline deadline) throws Exception {
        java.util.Objects.requireNonNull(pin); java.util.Objects.requireNonNull(control); commandValidator.accept(business);
        if (control.equals(business.operationId())) throw new IllegalArgumentException("Prepare control identity must differ from business identity");
        var reservation=reserveProposalScope(pin.scope()); if (reservation==null) throw new IllegalStateException("Participant scope overlaps in-flight work");
        PreparingParticipant preparing=null; boolean retained=false;
        try {
            for (int attempt=0;attempt<3 && preparing==null;attempt++) {
                var capacity=store.preparedCapacity();
                synchronized (admissions) {
                    requireHealthy(); if (!reservation.active() || deadline!=null && !deadline.active() || pin.position().term()!=reservation.term()) throw new IllegalStateException("Participant owner expired or changed term");
                    if (!java.util.Objects.equals(capacity.position(),store.applied())) continue;
                    if (participantIdentityBusy(control) || participantIdentityBusy(business.operationId())) throw new IllegalStateException("Participant operation identity is already owned");
                    long scope=RangeMutationHistory.accountedScopeBytes(pin.scope()), maximum=49L+PreparedParticipant.Preparation.MAX_BYTES+ReplicatedRangeStore.FINALIZATION_RESERVATION;
                    if (capacity.count()+reservedParticipantCount+reservedFinalizationCount>=ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS || maximum>ReplicatedRangeStore.MAX_PREPARED_BYTES-capacity.bytes()-reservedParticipantBytes-reservedFinalizationBytes
                            || scope>ReplicatedRangeStore.MAX_PREPARED_BYTES-capacity.scopeBytes()-reservedParticipantScopeBytes
                            || DistributedCommandCodec.MAX_ENCODED_BYTES>16L*1024*1024-preparedCommitBytes) throw new IllegalStateException("Participant staged capacity exhausted");
                    var hold=new ParticipantHold(transaction,control,business.operationId(),maximum,scope); preparing=new PreparingParticipant(reservation,hold);
                    preparingParticipants.put(control,preparing); participantOwners.put(control,hold); participantOwners.put(hold.business,hold);
                    reservedParticipantCount++; reservedParticipantBytes+=maximum; reservedParticipantScopeBytes+=scope; preparedCommitBytes+=DistributedCommandCodec.MAX_ENCODED_BYTES;
                }
            }
            if (preparing==null) throw new IllegalStateException("Native cut kept changing during participant admission");
            if (!store.estimateReceiptRetention(business,enableSessionControls).newReceipt()) throw new IllegalStateException("Business operation already has an outcome");
            var homeRead=homes.readRegistration(transaction); var registration=homeRead.status(); if (registration==null || registration.phase()!=TransactionStatus.Phase.PREPARING) throw new IllegalStateException("Home has no active frozen registration");
            var accepted=store.planScopedCommit(pin,business,reservation.generation(),transactionIncarnation); commandValidator.accept(accepted);
            var home=homeRead.descriptor();
            var preparation=new PreparedParticipant.Preparation(home.groupId(),home.rangeEpoch(),control,registration,accepted);
            store.estimatePreparation(preparation); // Reject an unbound/fenced control owner before closing the caller's read cut.
            var plan=new ParticipantProposal(reservation,preparing.hold(),preparation,deadline);
            synchronized (admissions) { preparing.hold().preparation=preparation; }
            pin.close();
            synchronized (admissions) {
                requireHealthy(); if (!plan.authorized() || preparingParticipants.get(control)!=preparing) throw new IllegalStateException("Participant lost acceptance ownership");
                long actual=49L+preparation.encode().length+ReplicatedRangeStore.FINALIZATION_RESERVATION;
                if (!plan.hold.capacityReleased) reservedParticipantBytes+=actual-plan.hold.bytes; plan.hold.bytes=actual;
                preparedCommitBytes+=plan.bytes.length-DistributedCommandCodec.MAX_ENCODED_BYTES;
                preparedParticipants.put(control,plan); preparingParticipants.remove(control,preparing);
            }
            retained=true; return plan;
        } finally {
            synchronized (admissions) {
                if (preparing!=null && preparingParticipants.remove(control,preparing)) { preparedCommitBytes-=DistributedCommandCodec.MAX_ENCODED_BYTES; releaseParticipantHold(preparing.hold()); }
            }
            if (!retained) reservation.close();
        }
    }
    private TransactionContext startParticipant(TransactionContext transaction,byte[] encoded,long generation,Leadership leadership) throws Exception {
        var preparation=RatisParticipantRecord.decode(encoded); commandValidator.accept(preparation.business()); byte[] digest=RatisParticipantRecord.digest(preparation);
        for (int attempt=0;attempt<3;attempt++) {
            var estimate=store.estimatePreparation(preparation); var observed=estimate.capacity().position();
            if (observed==null || observed.term()!=leadership.term()) throw new IllegalStateException("Participant native prefix is not ready");
            synchronized (admissions) {
                requireHealthy(); if (generation!=admissionGeneration.get() || !store.admissionOpen()) throw new IllegalStateException("Participant owner changed");
                if (!java.util.Objects.equals(observed,store.applied())) continue;
                reconcileOlderProposalTerms(leadership.term());
                var known=admissions.get(preparation.operation());
                if (known!=null && !Arrays.equals(known.digest,digest)) throw new IllegalArgumentException("Prepare operation payload changed");
                if (known!=null && estimate.existing()!=null) { settleAdmission(known); known=admissions.get(preparation.operation()); }
                if (known!=null) {
                    if (known.participant==null || known.term!=leadership.term() || known.generation!=generation) throw new IllegalStateException("Previous prepare admission is still settling");
                    known.owners++; transaction.setStateMachineContext(known); return transaction;
                }
                var accepted=estimate.existing()==null ? preparedParticipants.get(preparation.operation()) : null;
                if (estimate.existing()==null && (accepted==null || !Arrays.equals(accepted.digest,digest) || !accepted.authorized())) throw new IllegalStateException("Prepare has no current acceptance authority; resolve through its home");
                if (dataAdmissions>=1024 || encoded.length>16L*1024*1024-dataProposalBytes) throw new IllegalStateException("Participant input budget exhausted");
                var admission=new Admission(preparation.operation(),digest,encoded.length,leadership.term(),generation,0,0,false,false,null,accepted==null ? null : accepted.reservation.lease,null);
                admission.participant=preparation; admission.participantHold=accepted==null ? null : accepted.hold;
                admissions.put(admission.id,admission);
                if (accepted!=null) {
                    proposalFootprints.submit(accepted.reservation.lease); accepted.claimed=true; preparedParticipants.remove(admission.id,accepted); preparedCommitBytes-=accepted.bytes.length;
                }
                admittedBytes+=admission.bytes; dataAdmissions++; dataProposalBytes+=admission.bytes;
                transaction.setStateMachineContext(admission); return transaction;
            }
        }
        throw new IllegalStateException("Native participant cut changed during submission");
    }
    public long reservedParticipantCount() { synchronized (admissions) { return reservedParticipantCount; } }
    public long reservedParticipantBytes() { synchronized (admissions) { return reservedParticipantBytes; } }
    /** Caller supplies a leader-bound tracked cut obtained after its read barrier; no network work occurs here. */
    public ScopedCommit prepareScopedCommit(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand command) throws Exception {
        return prepareScopedCommit(pin,command,null);
    }
    ScopedCommit prepareScopedCommit(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand command,ProposalDeadline deadline) throws Exception {
        java.util.Objects.requireNonNull(pin); java.util.Objects.requireNonNull(command); commandValidator.accept(command);
        ProposalReservation reservation;
        synchronized (admissions) {
            if (command.acknowledgedThrough()>=0 && proposalFootprints.stats().candidates()>0) throw new IllegalStateException("Acknowledging transaction needs exclusive session metadata ownership");
        }
        reservation=reserveProposalScope(pin.scope());
        if (reservation==null) throw new IllegalStateException("Transaction scope overlaps in-flight proposals");
        // Reserve the largest legal frozen command before native result construction, then shrink to its actual size.
        var preparing=new PreparingCommit(reservation,DistributedCommandCodec.MAX_ENCODED_BYTES);
        boolean retained=false;
        try {
            synchronized (admissions) {
                if (!reservation.active() || deadline!=null && !deadline.active() || pin.position().term()!=reservation.term()) throw new IllegalStateException("Transaction snapshot changed leadership or expired");
                if (finalizationOwners.containsKey(command.operationId()) || participantOwners.containsKey(command.operationId()) || admissions.containsKey(command.operationId()) || preparedCommits.containsKey(command.operationId()) || preparingCommits.containsKey(command.operationId())) throw new IllegalStateException("Operation identity already owns a proposal");
                if (preparingCommits.size()+preparedCommits.size()>=64 || preparing.bytes>16L*1024*1024-preparedCommitBytes) throw new IllegalStateException("Preparing command byte/count budget exhausted");
                if (command.acknowledgedThrough()>=0) {
                    if (proposalFootprints.stats().candidates()!=1) throw new IllegalStateException("Acknowledging transaction lost exclusive session metadata ownership");
                    exclusiveMetadataCandidate=reservation.lease;
                }
                preparingCommits.put(command.operationId(),preparing); preparedCommitBytes+=preparing.bytes;
            }
            if (!store.estimateReceiptRetention(command,enableSessionControls).newReceipt()) throw new IllegalStateException("Transaction identity already has a terminal outcome");
            var accepted=store.planScopedCommit(pin,command,reservation.generation(),transactionIncarnation);
            commandValidator.accept(accepted);
            var plan=new ScopedCommit(reservation,accepted,deadline);
            synchronized (admissions) {
                requireHealthy();
                if (!plan.authorized() || preparingCommits.get(command.operationId())!=preparing) throw new IllegalStateException("Transaction lost its reservation before acceptance");
                if (preparedCommits.size()>=64 || plan.bytes>16L*1024*1024-(preparedCommitBytes-preparing.bytes)) throw new IllegalStateException("Prepared command byte/count budget exhausted");
                preparedCommitBytes+=plan.bytes-preparing.bytes; preparing.bytes=plan.bytes;
            }
            // Closing native ownership can outlive logical invalidation; keep the preparing token charged through it.
            pin.close();
            synchronized (admissions) {
                requireHealthy();
                if (!plan.authorized() || preparingCommits.get(command.operationId())!=preparing) throw new IllegalStateException("Transaction lost its reservation before publication");
                if (admissions.containsKey(command.operationId()) || preparedCommits.containsKey(command.operationId())) throw new IllegalStateException("Operation identity already owns a proposal");
                preparedCommits.put(command.operationId(),plan); preparingCommits.remove(command.operationId(),preparing);
            }
            retained=true; return plan;
        } finally {
            synchronized (admissions) { if (preparingCommits.remove(command.operationId(),preparing)) preparedCommitBytes-=preparing.bytes; }
            if (!retained) reservation.close();
        }
    }
    public ProposalReservation reserveProposalScope(TransactionScope scope) throws IOException {
        requireHealthy(); long generation=admissionGeneration.get(); var current=leadership();
        if (!current.ready()) throw new IOException("Leader is not ready for scope reservation");
        store.validateScope(scope); var prepared=proposalFootprints.prepare(scope);
        synchronized (admissions) {
            requireHealthy(); var applied=store.applied();
            if (generation!=admissionGeneration.get() || applied==null || applied.term()!=current.term()) throw new IOException("Reservation lost its completed current-term owner");
            if (!store.admissionOpen()) throw new IOException("Native range admission is closed"); reconcileOlderProposalTerms(current.term());
            if (exclusiveMetadataCandidate!=null && exclusiveMetadataCandidate.active() || admissions.values().stream().anyMatch(admission -> admission.maintenance && !admission.settled)) throw new IOException("Session metadata is still settling");
            var lease=proposalFootprints.reserveCandidate(prepared,current.term(),generation); return lease==null ? null : new ProposalReservation(lease);
        }
    }

    private static boolean retentionFits(long retained,long reserved,long additional,long ceiling) {
        // No-growth GC/retries remain possible when conservative in-flight accounting exceeds the ceiling.
        return additional == 0 || retained <= ceiling && reserved <= ceiling-retained && additional <= ceiling-retained-reserved;
    }
    // Caller holds admissions. Applied outcomes no longer consume speculative capacity, even with duplicate RPC waiters.
    private void releaseRetention(Admission admission) {
        if (!admission.retentionReleased) {
            admission.retentionReleased=true;
            reservedReceiptCount-=admission.receiptCount; reservedReceiptBytes-=admission.receiptBytes;
            if (maintenanceReservation==admission) maintenanceReservation=null;
        }
    }

    @Override public void notifyConfigurationChanged(long term, long index, RaftConfigurationProto configuration) {
        synchronized (gate) {
            // Snapshot transport's current membership is not an applied configuration at D.
            if (term == 0) return;
            byte[] bytes = configuration.toByteArray();
            if (bytes.length > 65536 || configuration.getPeersCount() == 0 || deferred.containsKey(index)) {
                storageFailure = new IllegalStateException("Invalid configuration callback");
                throw new IllegalStateException("Invalid configuration", storageFailure);
            }
            if (!configurations.containsKey(index) && configurations.size() >= 1024) throw new IllegalStateException("Configuration callback budget exhausted");
            var previous = configurations.putIfAbsent(index, new ConfigurationEntry(term, bytes));
            if (previous != null && (previous.term() != term || !Arrays.equals(previous.bytes(), bytes))) {
                storageFailure = new IllegalStateException("Configuration log identity changed");
                throw new IllegalStateException("Conflicting configuration", storageFailure);
            }
        }
    }

    @Override public void notifyTermIndexUpdated(long term, long index) {
        Throwable failure = null;
        synchronized (gate) {
            try {
                if (storageFailure != null) throw new IllegalStateException("Range requires repair", storageFailure);
                if (deferred.containsKey(index)) throw new IllegalStateException("Log entry changed from data to metadata");
                var pending = pendingNoops.get(index);
                if (pending != null && pending != term) throw new IllegalStateException("Metadata log identity changed");
                if (getLifeCycleState() != LifeCycle.State.RUNNING) {
                    if (pending == null && pendingNoops.size() >= 1024) throw new IllegalStateException("Paused metadata budget exhausted");
                    pendingNoops.put(index, term);
                    return;
                }
                var cfg = configurations.remove(index);
                if (cfg != null && cfg.term() != term) throw new IllegalStateException("Configuration term mismatch");
                if (cfg!=null) { admissionGeneration.incrementAndGet(); invalidateProposalCandidates(); }
                store.applyNoop(new ApplyPosition(term, index), cfg == null ? null : cfg.bytes());
                advance(term, index);
                pendingNoops.remove(index);
            } catch (Throwable problem) { storageFailure = problem; failure = problem; }
        }
        if (failure != null) {
            failDeferred(failure);
            throw new IllegalStateException("Range failed completed apply", failure);
        }
    }

    @Override public CompletableFuture<Message> queryStale(Message request, long minIndex) {
        try {
            if (ScopedTransactionRpc.parse(request.getContent().toByteArray(),ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()>=ScopedTransactionRpc.OPEN) throw new IOException("Stale reads cannot access transaction handles");
            synchronized (gate) {
                requireHealthy();
                if (minIndex > getLastAppliedTermIndex().getIndex()) throw new IllegalStateException("Requested read index has not completed");
            }
            return query(request);
        } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
    }

    @Override public CompletableFuture<Message> query(Message request) {
        return query(request,false);
    }
    @Override public CompletableFuture<Message> queryLinearizable(Message request) { return query(request,true); }
    private CompletableFuture<Message> query(Message request,boolean freshBarrier) {
        try {
            requireHealthy();
            byte[] encoded = request.getContent().toByteArray();
            return CompletableFuture.completedFuture(framedQuery(encoded,freshBarrier));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private Message framedQuery(byte[] encoded,boolean freshBarrier) throws Exception {
        var packet=ScopedTransactionRpc.parse(encoded,ScopedTransactionRpc.MAX_REQUEST_BYTES);
        var input=ByteBuffer.wrap(packet.body()); byte[] reply;
        switch (packet.kind()) {
            case ScopedTransactionRpc.POINT -> {
                var column=ScopedTransactionRpc.uuid(input); var key=ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES-16,false); end(input);
                try (var pin=readablePoint(column,key)) { var value=pin.get(column,key); reply=valueBlob(value); }
            }
            case ScopedTransactionRpc.STATE -> { end(input); try (var pin=snapshotPin()) { reply=pin.stateView().encode(); } }
            case ScopedTransactionRpc.OPEN -> {
                if (!freshBarrier) throw new IOException("Transaction open requires a fresh linearizable read barrier");
                var id=ScopedTransactionRpc.uuid(input); long ttl=input.getLong();
                var handle=scopedTransactions.open(id,ScopedTransactionRpc.decodeScope(input),java.time.Duration.ofMillis(ttl));
                var output=ByteBuffer.allocate(16+ScopedTransactionRpc.HANDLE_BYTES); ScopedTransactionRpc.uuid(output,id); ScopedTransactionRpc.writeHandle(output,handle); reply=output.array();
            }
            case ScopedTransactionRpc.GET -> {
                var handle=ScopedTransactionRpc.readHandle(input); var column=ScopedTransactionRpc.uuid(input);
                var key=ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES-16,false); end(input);
                reply=valueBlob(scopedTransactions.get(handle,column,key));
            }
            case ScopedTransactionRpc.PAGE -> {
                var handle=ScopedTransactionRpc.readHandle(input); var column=ScopedTransactionRpc.uuid(input);
                var lower=ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true); var upper=ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true);
                int direction=input.get()&255; if (direction>1) throw new IllegalArgumentException("Invalid transaction page direction");
                var resume=ScopedTransactionRpc.blob(input,it.cavallium.rockserver.core.common.distributed.PhysicalRangePage.MAX_BYTES,true);
                int items=input.getInt(), bytes=input.getInt(); end(input);
                reply=ScopedTransactionRpc.pageBytes(scopedTransactions.page(handle,column,lower,upper,direction==1,resume,items,bytes));
            }
            case ScopedTransactionRpc.STAGE -> {
                var handle=ScopedTransactionRpc.readHandle(input); var command=DistributedCommandCodec.decode(ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false)); end(input);
                byte[] result=scopedTransactions.stage(handle,command).encode();
                reply=ByteBuffer.allocate(32+result.length).put(command.payloadDigest()).put(result).array();
            }
            case ScopedTransactionRpc.PLAN -> {
                var handle=ScopedTransactionRpc.readHandle(input); var command=DistributedCommandCodec.decode(ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false)); end(input);
                reply=DistributedCommandCodec.encode(scopedTransactions.plan(handle,command));
            }
            case ScopedTransactionRpc.PARTICIPANT_STATE -> {
                if (!freshBarrier) throw new IOException("Prepared state requires a fresh linearizable barrier");
                var id=ScopedTransactionRpc.uuid(input); end(input); checkTransactionOwner(); var record=store.preparedParticipant(id);
                byte[] bytes=record==null ? null : record.encode(); var output=ByteBuffer.allocate(4+(bytes==null ? 0 : bytes.length)); ScopedTransactionRpc.putBlob(output,bytes); reply=output.array();
            }
            case ScopedTransactionRpc.PARTICIPANT_OUTCOME -> {
                if (!freshBarrier) throw new IOException("Participant outcome requires a fresh linearizable barrier");
                var id=ScopedTransactionRpc.uuid(input); end(input); checkTransactionOwner(); var outcome=store.finalizedParticipant(id);
                byte[] bytes=outcome==null ? null : outcome.encode(); var output=ByteBuffer.allocate(4+(bytes==null ? 0 : bytes.length)); ScopedTransactionRpc.putBlob(output,bytes); reply=output.array();
            }
            case ScopedTransactionRpc.CLOSE -> { var handle=ScopedTransactionRpc.readHandle(input); end(input); scopedTransactions.release(handle); reply=new byte[0]; }
            default -> throw new IllegalArgumentException("Unsupported framed query");
        }
        return Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(packet.kind(),reply)));
    }
    private static void end(ByteBuffer input) { if (input.hasRemaining()) throw new IllegalArgumentException("Trailing query bytes"); }
    private static byte[] valueBlob(byte[] value) {
        if (value!=null && value.length>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Point value exceeds reply budget");
        var output=ByteBuffer.allocate(4+(value==null ? 0 : value.length)); ScopedTransactionRpc.putBlob(output,value); return output.array();
    }

    private void requireHealthy() {
        if (storageFailure != null) throw new IllegalStateException("Range requires repair", storageFailure);
        if (getLifeCycleState() != LifeCycle.State.RUNNING) throw new IllegalStateException("Range is paused or starting");
    }
}
