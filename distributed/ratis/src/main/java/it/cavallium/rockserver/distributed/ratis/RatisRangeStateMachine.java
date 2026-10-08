package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import java.util.function.Consumer;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.common.distributed.RangeDirectory;
import it.cavallium.rockserver.core.common.distributed.RangeStateView;
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
public final class RatisRangeStateMachine extends BaseStateMachine {
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

    private static final class Admission {
        final OperationId id;
        final byte[] digest;
        final int bytes;
        final long term, generation, receiptCount, receiptBytes;
        final boolean maintenance, protectedControl;
        boolean retentionReleased;
        int owners = 1;
        Admission(OperationId id, byte[] digest, int bytes, long term, long generation,
                long receiptCount, long receiptBytes, boolean maintenance, boolean protectedControl) {
            this.id = id;
            this.digest = digest;
            this.bytes = bytes; this.term = term; this.generation = generation;
            this.receiptCount=receiptCount; this.receiptBytes=receiptBytes;
            this.maintenance=maintenance; this.protectedControl=protectedControl;
        }
    }

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

    @Override public void pause() {
        admissionGeneration.incrementAndGet();
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
        admissionGeneration.incrementAndGet();
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
        admissionGeneration.incrementAndGet();
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
                    var command = DistributedCommandCodec.decode(pending.encoded());
                    byte[] result = store.operationResult(command.operationId(), command.payloadDigest());
                    if (result == null) throw new IOException("Installed image omitted deferred receipt");
                    completions.add(new Completion(pending.future(), Message.valueOf(ByteString.copyFrom(result))));
                    coveredOperations.add(command.operationId());
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
                if (enableSessionControls) store.enableSessionControls();
                synchronized (admissions) {
                    for (var id : coveredOperations) {
                        var admission = admissions.get(id);
                        if (admission != null) releaseRetention(admission);
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
            var command = DistributedCommandCodec.decode(encoded);
            commandValidator.accept(command);
            store.validate(command);
            byte[] digest = command.payloadDigest();
            // Native receipt reads stay outside the short admission lock. If apply
            // advanced while reading, retry validation before acquiring ownership.
            for (int attempt = 0; attempt < 3; attempt++) {
                var estimate = store.estimateReceiptRetention(command,enableSessionControls);
                var observed = estimate.usage().appliedPosition();
                if (observed == null || observed.term() != leadership.term()) {
                    throw new IllegalStateException("Current-term startup prefix has not completed native apply");
                }
                // Previously committed outcomes and expired identities cannot advance the fence.
                if (estimate.newReceipt() && command.minimumSessionEpoch() >= 0) epochFloorAdmission.accept(command);
                synchronized (admissions) {
                    if (generation != admissionGeneration.get()) throw new IllegalStateException("Admission leadership generation changed");
                    if (!java.util.Objects.equals(observed, store.applied())) continue;
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
                        boolean maintenance = estimate.newReceipt() && estimate.maintenance();
                        boolean protectedControl = estimate.maintenance() && command.mutations().isEmpty() && command.predicates().isEmpty();
                        if (protectedControl ? protectedAdmissions >= 16
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
                        admission = new Admission(command.operationId(), digest, encoded.length, leadership.term(), generation,
                                estimate.additionalCount(),estimate.additionalBytes(),maintenance,protectedControl);
                        admissions.put(admission.id, admission);
                        admittedBytes += admission.bytes;
                        if (protectedControl) protectedAdmissions++; else { dataAdmissions++; dataProposalBytes+=admission.bytes; }
                        reservedReceiptCount+=admission.receiptCount; reservedReceiptBytes+=admission.receiptBytes;
                        if (maintenance) maintenanceReservation=admission;
                    }
                    return transaction.setStateMachineContext(admission);
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
                if (!(transaction.getStateMachineContext() instanceof Admission admission)
                        || admissions.get(admission.id) != admission || admission.owners <= 0
                        || !current.ready() || admission.term != current.term()
                        || admission.generation != admissionGeneration.get()
                        || nativePosition == null || nativePosition.term() != current.term()) {
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
                var command = DistributedCommandCodec.decode(encoded);
                commandValidator.accept(command);
                byte[] bytes = store.apply(new ApplyPosition(entry.getTerm(), entry.getIndex()), command);
                synchronized (admissions) {
                    var admitted = admissions.get(command.operationId());
                    if (admitted != null) releaseRetention(admitted);
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
        admissionGeneration.incrementAndGet();
        for (var transaction : pendingEntries) releaseAdmission(transaction);
    }

    private void releaseAdmission(TransactionContext transaction) {
        synchronized (admissions) {
            if (transaction.getStateMachineContext() instanceof Admission admission) {
                transaction.setStateMachineContext(null);
                if (--admission.owners == 0 && admissions.remove(admission.id, admission)) {
                    admittedBytes -= admission.bytes;
                    if (admission.protectedControl) protectedAdmissions--; else { dataAdmissions--; dataProposalBytes-=admission.bytes; }
                    releaseRetention(admission);
                }
            }
        }
    }

    private static boolean retentionFits(long retained,long reserved,long additional,long ceiling) {
        // No-growth GC/retries remain possible when a legacy database or conservative in-flight accounting exceeds the ceiling.
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
            synchronized (gate) {
                requireHealthy();
                if (minIndex > getLastAppliedTermIndex().getIndex()) throw new IllegalStateException("Requested read index has not completed");
            }
            return query(request);
        } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
    }

    @Override public CompletableFuture<Message> query(Message request) {
        try {
            requireHealthy();
            byte[] encoded = request.getContent().toByteArray();
            if (encoded.length==8) {
                if (!RangeStateView.isRequest(encoded)) throw new IllegalArgumentException("Invalid range-state request");
                try (var pin=snapshotPin()) {
                    return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(pin.stateView().encode())));
                }
            }
            if (encoded.length < 16 || encoded.length > DistributedCommandCodec.MAX_ENCODED_BYTES) {
                throw new IllegalArgumentException("Invalid point read size");
            }
            var input = ByteBuffer.wrap(encoded);
            var column = new UUID(input.getLong(), input.getLong());
            var key = new byte[input.remaining()];
            input.get(key);
            try (var pin = snapshotPin()) {
                byte[] value = pin.get(column, key);
                var result = ByteBuffer.allocate(4 + (value == null ? 0 : value.length));
                result.putInt(value == null ? -1 : value.length);
                if (value != null) result.put(value);
                return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(result.array())));
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private void requireHealthy() {
        if (storageFailure != null) throw new IllegalStateException("Range requires repair", storageFailure);
        if (getLifeCycleState() != LifeCycle.State.RUNNING) throw new IllegalStateException("Range is paused or starting");
    }
}
