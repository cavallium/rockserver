package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import it.cavallium.rockserver.core.common.distributed.CommandResult;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.WriteBatchWithIndex;

/**
 * Native atomic apply seam for one range in a shared storage pool.
 * The pool owns the database and CF handles and must outlive this store and its
 * snapshot pins. Only committed, canonical plans may enter this seam; admission,
 * consensus, ownership and operator qualification belong to the range adapter.
 */
public final class ReplicatedRangeStore {
    private static final int STATE_VERSION = 1;
    private static final int MAX_RESULT_BYTES = 2 * 1024 * 1024;
    private final RocksDB db;
    private final UUID groupId;
    private final long rangeEpoch;
    private final byte[] schemaDigest;
    private final Map<UUID, ColumnFamilyHandle> columns;
    private final ColumnFamilyHandle state;
    private final ColumnFamilyHandle receipts;
    private final byte[] markerKey;
    private final ReentrantLock applyLane = new ReentrantLock();
    private volatile ApplyPosition applied;

    public ReplicatedRangeStore(RocksDB db, UUID groupId, long rangeEpoch,
            byte[] schemaDigest, Map<UUID, ColumnFamilyHandle> columns,
            ColumnFamilyHandle state, ColumnFamilyHandle receipts) throws RocksDBException {
        this.db = Objects.requireNonNull(db);
        this.groupId = Objects.requireNonNull(groupId);
        if (rangeEpoch < 0 || schemaDigest.length != 32) throw new IllegalArgumentException("Invalid range schema");
        this.rangeEpoch = rangeEpoch;
        this.schemaDigest = schemaDigest.clone();
        this.columns = Map.copyOf(columns);
        this.state = Objects.requireNonNull(state);
        this.receipts = Objects.requireNonNull(receipts);
        if (state.getID() == receipts.getID() || columns.values().stream().anyMatch(
                cf -> cf.getID() == state.getID() || cf.getID() == receipts.getID())) {
            throw new IllegalArgumentException("User, state and receipt CFs must be distinct");
        }
        markerKey = uuidBytes(groupId);
        byte[] recovered = db.get(state, markerKey);
        applied = recovered == null ? null : decodePosition(recovered);
    }

    /** Null means no log entry has yet completed. This is not a durable snapshot watermark. */
    public ApplyPosition applied() { return applied; }

    public void validate(DistributedCommand command) {
        if (!groupId.equals(command.groupId()) || rangeEpoch != command.rangeEpoch()
                || !Arrays.equals(schemaDigest, command.schemaDigest())) {
            throw new IllegalArgumentException("Wrong group, epoch or schema");
        }
        for (var mutation : command.mutations()) {
            if (!columns.containsKey(mutation.columnId())) throw new IllegalArgumentException("Unknown logical column");
        }
        if (command.mutations().stream().anyMatch(m -> m.returnMode() != Mutation.ReturnMode.NONE)
                && command.mutations().stream().anyMatch(m -> m.kind() == Mutation.Kind.DELETE_RANGE)) {
            throw new IllegalArgumentException("Point results with range deletion require a range-aware overlay");
        }
    }

    /** Evaluates autocommit return values in committed order, preserving native merge operands. */
    public byte[] apply(ApplyPosition position, DistributedCommand command) throws RocksDBException {
        return applyInternal(position, command, null);
    }

    /**
     * Installs user effects, the original result and the completed marker in one
     * WAL-enabled cross-CF batch. The result is evaluated by ordered leader
     * planning before consensus, never recomputed during replay or retry.
     */
    public byte[] apply(ApplyPosition position, DistributedCommand command, byte[] result)
            throws RocksDBException {
        return applyInternal(position, command, Objects.requireNonNull(result));
    }

    private byte[] applyInternal(ApplyPosition position, DistributedCommand command, byte[] result)
            throws RocksDBException {
        Objects.requireNonNull(position);
        validate(command);
        if (result != null && result.length > MAX_RESULT_BYTES) throw new IllegalArgumentException("Result exceeds receipt budget");
        byte[] stableResult = result == null ? null : result.clone();
        byte[] digest = command.payloadDigest();
        byte[] receiptKey = receiptKey(command.operationId());
        applyLane.lock();
        try {
            byte[] saved = db.get(receipts, receiptKey);
            byte[] original = saved == null ? null : decodeReceipt(saved, digest);
            if (applied != null && position.index() <= applied.index()) {
                if (saved == null) throw new IllegalStateException("Applied command has no retained receipt");
                return original;
            }
            requireNext(position);
            boolean installUserEffects = true;
            if (saved == null && stableResult == null) {
                var evaluated = evaluate(command);
                stableResult = evaluated.encode();
                installUserEffects = evaluated.status() == CommandResult.Status.SUCCESS;
            }
            try (WriteBatch batch = new WriteBatch();
                    WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                if (saved == null) {
                    if (installUserEffects) for (var mutation : command.mutations()) {
                        var cf = columns.get(mutation.columnId());
                        if (cf == null) throw new IllegalArgumentException("Unknown logical column");
                        switch (mutation.kind()) {
                            case PUT -> batch.put(cf, mutation.key(), mutation.value());
                            case DELETE -> batch.delete(cf, mutation.key());
                            case MERGE -> batch.merge(cf, mutation.key(), mutation.value());
                            case DELETE_RANGE -> batch.deleteRange(cf, mutation.key(), mutation.value());
                        }
                    }
                    batch.put(receipts, receiptKey, encodeReceipt(digest, stableResult));
                }
                batch.put(state, markerKey, encodePosition(position));
                db.write(options, batch);
            }
            applied = position;
            return saved == null ? stableResult : original;
        } finally {
            applyLane.unlock();
        }
    }

    private CommandResult evaluate(DistributedCommand command) throws RocksDBException {
        if (command.mutations().stream().allMatch(m -> m.returnMode() == Mutation.ReturnMode.NONE)) {
            return new CommandResult(CommandResult.Status.SUCCESS, List.of());
        }
        var values = new ArrayList<byte[]>();
        long resultBytes = 8;
        try (var overlay = new WriteBatchWithIndex(true); var options = new ReadOptions()) {
            for (var mutation : command.mutations()) {
                var cf = columns.get(mutation.columnId());
                byte[] key = mutation.key();
                byte[] value = null;
                if (mutation.returnMode() == Mutation.ReturnMode.PREVIOUS) {
                    value = overlay.getFromBatchAndDB(db, cf, options, key);
                }
                switch (mutation.kind()) {
                    case PUT -> overlay.put(cf, key, mutation.value());
                    case DELETE -> overlay.delete(cf, key);
                    case MERGE -> overlay.merge(cf, key, mutation.value());
                    case DELETE_RANGE -> throw new IllegalStateException("Range result plan escaped validation");
                }
                if (mutation.returnMode() == Mutation.ReturnMode.CURRENT) {
                    value = overlay.getFromBatchAndDB(db, cf, options, key);
                }
                if (mutation.returnMode() != Mutation.ReturnMode.NONE) {
                    resultBytes += 4L + (value == null ? 0 : value.length);
                    if (resultBytes > MAX_RESULT_BYTES) return new CommandResult(CommandResult.Status.RESULT_TOO_LARGE, List.of());
                    values.add(value);
                }
            }
        }
        return new CommandResult(CommandResult.Status.SUCCESS, values);
    }

    /** Every committed configuration/no-op entry must be accounted for in log order. */
    public void applyNoop(ApplyPosition position) throws RocksDBException {
        applyLane.lock();
        try {
            if (applied != null && position.index() <= applied.index()) return;
            requireNext(position);
            try (WriteBatch batch = new WriteBatch();
                    WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                batch.put(state, markerKey, encodePosition(position));
                db.write(options, batch);
            }
            applied = position;
        } finally {
            applyLane.unlock();
        }
    }

    public byte[] operationResult(OperationId id, byte[] payloadDigest) throws RocksDBException {
        byte[] saved = db.get(receipts, receiptKey(id));
        return saved == null ? null : decodeReceipt(saved, payloadDigest);
    }

    /**
     * Captures user data, receipts and marker from the same native cut.
     * The adapter must establish the read/fence barrier before invoking this.
     * A native cut alone does not constitute a durable installable Raft snapshot.
     */
    public SnapshotPin capture() throws RocksDBException {
        applyLane.lock();
        try {
            Snapshot snapshot = db.getSnapshot();
            try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                byte[] marker = db.get(state, options, markerKey);
                ApplyPosition cut = marker == null ? null : decodePosition(marker);
                if (!Objects.equals(cut, applied)) throw new IllegalStateException("Snapshot marker differs from completed apply");
                return new SnapshotPin(snapshot, cut);
            } catch (Throwable failure) {
                db.releaseSnapshot(snapshot);
                throw failure;
            }
        } finally {
            applyLane.unlock();
        }
    }

    public final class SnapshotPin implements AutoCloseable {
        private final Snapshot snapshot;
        private final ApplyPosition position;
        private boolean closed;
        private SnapshotPin(Snapshot snapshot, ApplyPosition position) {
            this.snapshot = snapshot;
            this.position = position;
        }
        public ApplyPosition position() { return position; }
        // A pin has one owner; callers must not close it concurrently with use.
        public byte[] get(UUID columnId, byte[] key) throws RocksDBException {
            if (closed) throw new IllegalStateException("Snapshot pin closed");
            var cf = columns.get(columnId);
            if (cf == null) throw new IllegalArgumentException("Unknown logical column");
            try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                return db.get(cf, options, key);
            }
        }
        public byte[] operationResult(OperationId id, byte[] payloadDigest) throws RocksDBException {
            if (closed) throw new IllegalStateException("Snapshot pin closed");
            try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                byte[] saved = db.get(receipts, options, receiptKey(id));
                return saved == null ? null : decodeReceipt(saved, payloadDigest);
            }
        }
        @Override public void close() {
            if (!closed) {
                closed = true;
                db.releaseSnapshot(snapshot);
            }
        }
    }

    private void requireNext(ApplyPosition next) {
        long expected = applied == null ? 0 : Math.addExact(applied.index(), 1);
        if (next.index() != expected || (applied != null && next.term() < applied.term())) {
            throw new IllegalStateException("Noncontiguous or regressing committed apply: " + next);
        }
    }

    private byte[] receiptKey(OperationId id) {
        return ByteBuffer.allocate(48).put(uuidBytes(groupId)).put(uuidBytes(id.sessionId()))
                .putLong(id.epoch()).putLong(id.sequence()).array();
    }
    private static byte[] uuidBytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
    }
    private static byte[] encodePosition(ApplyPosition position) {
        return ByteBuffer.allocate(20).putInt(STATE_VERSION).putLong(position.term()).putLong(position.index()).array();
    }
    private static ApplyPosition decodePosition(byte[] value) {
        if (value.length != 20) throw new IllegalStateException("Invalid applied marker");
        var buffer = ByteBuffer.wrap(value);
        if (buffer.getInt() != STATE_VERSION) throw new IllegalStateException("Unsupported applied marker version");
        return new ApplyPosition(buffer.getLong(), buffer.getLong());
    }
    private static byte[] encodeReceipt(byte[] digest, byte[] result) {
        return ByteBuffer.allocate(36 + result.length).putInt(STATE_VERSION).put(digest).put(result).array();
    }
    private static byte[] decodeReceipt(byte[] value, byte[] digest) {
        if (digest.length != 32 || value.length < 36 || value.length > 36 + MAX_RESULT_BYTES) {
            throw new IllegalStateException("Invalid durable receipt");
        }
        var buffer = ByteBuffer.wrap(value);
        if (buffer.getInt() != STATE_VERSION) throw new IllegalStateException("Unsupported receipt version");
        byte[] originalHash = new byte[32];
        buffer.get(originalHash);
        if (!Arrays.equals(originalHash, digest)) throw new IllegalArgumentException("Operation identity reused with different payload");
        return Arrays.copyOfRange(value, 36, value.length);
    }
}
