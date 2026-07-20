package it.cavallium.rockserver.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.RockserverKeyEncoder;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;
import it.cavallium.rockserver.core.cluster.internal.DurableAtomicFileWriter;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommandValidator;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadResponseCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionStateMetadataCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandValidator;
import it.cavallium.rockserver.core.cluster.command.RocksDBPartitionStateMachineApplier;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport;
import it.cavallium.rockserver.core.replication.ratis.RockserverRatisStateMachine;
import it.cavallium.rockserver.core.common.Keys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MD5FileUtil;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

public final class RockserverRatisClasspathStateMachine extends BaseStateMachine {

    private MetadataView metadataView;
    private final Path dbPath;
    private final SimpleStateMachineStorage snapshotStorage = new SimpleStateMachineStorage();
    private Path stateMachineDirectory;
    private RocksDB db;
    private Options options;
    private RockserverRatisStateMachine stateMachine;

    public RockserverRatisClasspathStateMachine(ClusterMetadata metadata,
                                                PartitionDescriptor partition,
                                                Path dbPath) throws IOException {
        this.metadataView = new MetadataView(metadata, partition);
        Objects.requireNonNull(partition, "partition");
        this.dbPath = Objects.requireNonNull(dbPath, "dbPath").toAbsolutePath().normalize();
        RocksDB.loadLibrary();
    }

    @Override
    public synchronized void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage)
            throws IOException {
        super.initialize(raftServer, raftGroupId, storage);
        snapshotStorage.init(storage);
        stateMachineDirectory = storage.getStorageDir().getStateMachineDir().toPath();
        openFromLatestSnapshot();
    }

    public synchronized void reloadMetadata(ClusterMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");
        var partition = metadata.requirePartition(metadataView.partition().partitionId());
        metadataView = new MetadataView(metadata, partition);
    }

    @Override
    public synchronized TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        try {
            var view = metadataView;
            var command = PartitionWriteCommandCodec.decode(request.getMessage().getContent().toByteArray());
            PartitionWriteCommandValidator.validateForProposal(view.metadata(), view.partition(), command);
        } catch (RuntimeException e) {
            throw new IOException("Invalid Rockserver partition write proposal", e);
        }
        return super.startTransaction(request);
    }

    @Override
    public synchronized CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        var entry = trx.getLogEntry();
        var payload = entry.getStateMachineLogEntry().getLogData().toByteArray();
        try {
            var result = requireStateMachine().applyCommitted(entry.getIndex(), payload);
            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            var encodedResponses = PartitionWriteResponseCodec.encodeResponses(result.responses());
            return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(encodedResponses)));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public synchronized CompletableFuture<Message> query(Message request) {
        try {
            var payload = request.getContent().toByteArray();
            return switch (queryMagic(payload)) {
                case 0x52535052 -> queryPoint(payload);
                case 0x52535252 -> queryRange(payload);
                default -> CompletableFuture.failedFuture(new IllegalArgumentException(
                        "unknown Rockserver partition query magic"));
            };
        } catch (org.rocksdb.RocksDBException e) {
            return CompletableFuture.failedFuture(new IllegalStateException("Failed to read Rockserver partition", e));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<Message> queryPoint(byte[] payload) throws org.rocksdb.RocksDBException {
        var view = metadataView;
        var command = PartitionPointReadCommandCodec.decode(payload);
        var calculatedKey = PartitionPointReadCommandValidator.validateForPartition(
                view.metadata(), view.partition(), command);
        var storageKey = RockserverStorageKeyspace.userData(command.columnId(), calculatedKey);
        var value = requireDb().get(storageKey.toByteArray());
        var encodedResponse = PartitionWriteResponseCodec.encodeResponses(Collections.singletonList(
                value == null ? null : Buf.wrap(value).freeze()
        ));
        return CompletableFuture.completedFuture(Message.valueOf(
                ByteString.copyFrom(encodedResponse)
        ));
    }

    private CompletableFuture<Message> queryRange(byte[] payload) throws org.rocksdb.RocksDBException {
        var view = metadataView;
        var command = PartitionRangeReadCommandCodec.decode(payload);
        var columnSchema = validateRangeForPartition(view.metadata(), view.partition(), command);
        var encodedResponse = PartitionRangeReadResponseCodec.encodeEntries(scanRange(command, columnSchema));
        return CompletableFuture.completedFuture(Message.valueOf(
                ByteString.copyFrom(encodedResponse)
        ));
    }

    private List<KV> scanRange(PartitionRangeReadCommand command, ColumnSchema columnSchema)
            throws org.rocksdb.RocksDBException {
        var lowerBound = rangeLowerBound(command, columnSchema);
        var upperBound = rangeUpperBound(command, columnSchema);
        try (var lowerBoundSlice = new Slice(lowerBound.toByteArray());
             var upperBoundSlice = new Slice(upperBound.toByteArray());
             var readOptions = new ReadOptions()
                     .setIterateLowerBound(lowerBoundSlice)
                     .setIterateUpperBound(upperBoundSlice);
             var iterator = requireDb().newIterator(readOptions)) {
            if (command.reverse()) {
                iterator.seekToLast();
            } else {
                iterator.seekToFirst();
            }
            var entries = new ArrayList<KV>();
            while (iterator.isValid()) {
                if (entries.size() >= command.maxEntries()) {
                    throw new IllegalStateException("partition range read exceeded max entries "
                            + command.maxEntries());
                }
                entries.add(rangeEntry(command, columnSchema, iterator));
                if (command.reverse()) {
                    iterator.prev();
                } else {
                    iterator.next();
                }
            }
            iterator.status();
            return List.copyOf(entries);
        }
    }

    private static KV rangeEntry(PartitionRangeReadCommand command,
                                 ColumnSchema columnSchema,
                                 RocksIterator iterator) {
        var storageKey = Buf.wrap(iterator.key()).freeze();
        var calculatedKey = RockserverStorageKeyspace.userDataCalculatedKey(storageKey, command.columnId());
        var keys = RockserverKeyEncoder.decodeFixedLengthKeys(columnSchema, calculatedKey);
        var value = columnSchema.hasValue() ? Buf.wrap(iterator.value()).freeze() : Buf.wrap(new byte[0]).freeze();
        return new KV(keys, value);
    }

    private static Buf rangeLowerBound(PartitionRangeReadCommand command, ColumnSchema columnSchema) {
        var calculatedStart = calculatedBound(columnSchema, command.startKeysInclusive());
        return calculatedStart == null
                ? RockserverStorageKeyspace.userDataPrefix(command.columnId())
                : RockserverStorageKeyspace.userData(command.columnId(), calculatedStart);
    }

    private static Buf rangeUpperBound(PartitionRangeReadCommand command, ColumnSchema columnSchema) {
        var calculatedEnd = calculatedBound(columnSchema, command.endKeysExclusive());
        return calculatedEnd == null
                ? RockserverStorageKeyspace.userDataColumnEndExclusive(command.columnId())
                : RockserverStorageKeyspace.userData(command.columnId(), calculatedEnd);
    }

    private static ColumnSchema validateRangeForPartition(ClusterMetadata metadata,
                                                          PartitionDescriptor partition,
                                                          PartitionRangeReadCommand command) {
        var column = metadata.requireColumn(command.columnId());
        if (column.schemaEpoch() != command.schemaEpoch()) {
            throw new IllegalArgumentException("range read schema epoch " + command.schemaEpoch()
                    + " does not match column schema epoch " + column.schemaEpoch());
        }
        var schema = column.schema();
        if (RockserverKeyEncoder.hasVariableTailKeys(schema)) {
            throw new IllegalArgumentException("replicated range reads do not support variable-tail columns yet");
        }
        var start = calculatedBound(schema, command.startKeysInclusive());
        var end = calculatedBound(schema, command.endKeysExclusive());
        if (start != null && !partition.containsCalculatedKey(start)) {
            throw new IllegalArgumentException("partition range read start key is outside the target partition");
        }
        if (end != null) {
            if (partition.startKeyInclusive() != null
                    && RockserverKeyEncoder.compareCalculatedKeys(end, partition.startKeyInclusive()) < 0) {
                throw new IllegalArgumentException("partition range read end key is before the target partition");
            }
            if (partition.endKeyExclusive() != null
                    && RockserverKeyEncoder.compareCalculatedKeys(end, partition.endKeyExclusive()) > 0) {
                throw new IllegalArgumentException("partition range read end key is outside the target partition");
            }
        }
        if (start != null
                && end != null
                && RockserverKeyEncoder.compareCalculatedKeys(start, end) > 0) {
            throw new IllegalArgumentException("partition range read start key is after end key");
        }
        return schema;
    }

    private static Buf calculatedBound(ColumnSchema schema, Keys keys) {
        if (keys == null || keys.keys().length == 0) {
            return null;
        }
        return RockserverKeyEncoder.calculateKey(schema, keys);
    }

    private static int queryMagic(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        if (payload.length < 2 * Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Rockserver partition query payload is too short");
        }
        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < Integer.BYTES || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid Rockserver partition query body length: " + bodyLength);
        }
        return buffer.getInt();
    }

    @Override
    public synchronized long takeSnapshot() throws IOException {
        var termIndex = getLastAppliedTermIndex();
        if (termIndex.getIndex() <= 0) {
            return termIndex.getIndex();
        }
        var checkpointDirectory = requireStateMachineDirectory().resolve("rocksdb-checkpoint-"
                + termIndex.getTerm() + "-" + termIndex.getIndex() + "-" + UUID.randomUUID());
        var snapshotFile = snapshotStorage.getSnapshotFile(termIndex.getTerm(), termIndex.getIndex()).toPath();
        try {
            try (var checkpoint = Checkpoint.create(requireDb())) {
                checkpoint.createCheckpoint(checkpointDirectory.toString());
            } catch (org.rocksdb.RocksDBException e) {
                throw new IOException("Failed to create Rockserver Ratis RocksDB checkpoint", e);
            }
            RockserverRatisClasspathSnapshotArchive.write(snapshotFile, checkpointDirectory);
            MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile.toFile());
            snapshotStorage.updateLatestSnapshot(new SingleFileSnapshotInfo(new FileInfo(snapshotFile, md5), termIndex));
            return termIndex.getIndex();
        } finally {
            RockserverRatisClasspathSnapshotArchive.deleteRecursively(checkpointDirectory);
        }
    }

    @Override
    public synchronized void pause() {
        try {
            closeRocksDb();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close Rockserver Ratis state machine RocksDB", e);
        }
    }

    @Override
    public synchronized void reinitialize() throws IOException {
        closeRocksDb();
        openFromLatestSnapshot();
    }

    @Override
    public SimpleStateMachineStorage getStateMachineStorage() {
        return snapshotStorage;
    }

    @Override
    public SingleFileSnapshotInfo getLatestSnapshot() {
        return snapshotStorage.getLatestSnapshot();
    }

    @Override
    public synchronized void close() throws IOException {
        Throwable failure = null;
        try {
            closeRocksDb();
        } catch (IOException | RuntimeException | Error e) {
            failure = merge(failure, e);
        }
        try {
            super.close();
        } catch (IOException | RuntimeException | Error e) {
            failure = merge(failure, e);
        }
        if (failure != null) {
            throwFailure(failure);
        }
    }

    private List<Object> applyCommittedCommand(long logIndex, PartitionWriteCommand command) {
        var view = metadataView;
        return new RocksDBPartitionStateMachineApplier(view.metadata(), view.partition(), requireDb())
                .apply(logIndex, command);
    }

    private void validateCommand(PartitionWriteCommand command) {
        var view = metadataView;
        PartitionWriteCommandValidator.validateForReplay(view.metadata(), view.partition(), command);
    }

    private static long lastAppliedIndex(RocksDB db) throws org.rocksdb.RocksDBException {
        var value = db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray());
        return value == null ? 0 : PartitionStateMetadataCodec.decodeLastAppliedIndex(value);
    }

    private void openFromLatestSnapshot() throws IOException {
        if (db != null || options != null || stateMachine != null) {
            throw new IllegalStateException("Rockserver Ratis state machine RocksDB is already open");
        }
        RocksDBCheckpointSnapshotSupport.recoverInterruptedClosedDatabaseDirectoryInstall(dbPath);
        var snapshot = snapshotStorage.getLatestSnapshot();
        if (snapshot != null) {
            RockserverRatisClasspathSnapshotArchive.install(snapshot.getFile().getPath(), dbPath);
        }
        DurableAtomicFileWriter.createDirectoriesDurably(dbPath);
        Options openedOptions = new Options().setCreateIfMissing(true);
        RocksDB openedDb = null;
        try {
            openedDb = RocksDB.open(openedOptions, dbPath.toString());
            var initialLastAppliedIndex = lastAppliedIndex(openedDb);
            if (snapshot != null && initialLastAppliedIndex != snapshot.getTermIndex().getIndex()) {
                throw new IOException("Rockserver Ratis snapshot index " + snapshot.getTermIndex().getIndex()
                        + " does not match installed RocksDB last-applied index " + initialLastAppliedIndex);
            }
            options = openedOptions;
            db = openedDb;
            stateMachine = new RockserverRatisStateMachine(
                    this::applyCommittedCommand,
                    this::validateCommand,
                    initialLastAppliedIndex
            );
            if (snapshot != null) {
                setLastAppliedTermIndex(snapshot.getTermIndex());
            }
        } catch (org.rocksdb.RocksDBException e) {
            var failure = new IOException("Failed to open Rockserver Ratis state machine RocksDB: " + dbPath, e);
            closeSuppressing(openedDb, openedOptions, failure);
            throw failure;
        } catch (IOException | RuntimeException e) {
            closeSuppressing(openedDb, openedOptions, e);
            throw e;
        }
    }

    private RocksDB requireDb() {
        return Objects.requireNonNull(db, "Rockserver Ratis state machine RocksDB is not open");
    }

    private RockserverRatisStateMachine requireStateMachine() {
        return Objects.requireNonNull(stateMachine, "Rockserver Ratis state machine is not open");
    }

    private Path requireStateMachineDirectory() {
        return Objects.requireNonNull(stateMachineDirectory, "Rockserver Ratis state-machine directory is not open");
    }

    private void closeRocksDb() throws IOException {
        var closingDb = db;
        var closingOptions = options;
        db = null;
        options = null;
        stateMachine = null;
        close(closingDb, closingOptions);
    }

    private static void close(RocksDB db, Options options) throws IOException {
        var scope = new AdapterResourceScope();
        scope.add(options);
        scope.add(db);
        scope.close();
    }

    private static void closeSuppressing(RocksDB db, Options options, Throwable primaryFailure) {
        try {
            close(db, options);
        } catch (IOException | RuntimeException | Error closeFailure) {
            if (primaryFailure != closeFailure) {
                primaryFailure.addSuppressed(closeFailure);
            }
        }
    }

    private static Throwable merge(Throwable failure, Throwable addition) {
        if (addition == null) {
            return failure;
        }
        if (failure == null) {
            return addition;
        }
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
        return failure;
    }

    private static void throwFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException ioException) {
            throw ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IOException("Failed to close Rockserver Ratis state machine", failure);
    }

    private record MetadataView(ClusterMetadata metadata, PartitionDescriptor partition) {

        private MetadataView {
            Objects.requireNonNull(metadata, "metadata");
            Objects.requireNonNull(partition, "partition");
        }
    }
}
