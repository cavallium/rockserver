package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

class ReplicatedPoolModeTest {
    static { RocksDB.loadLibrary(); }
    enum Mode { REGULAR, OPTIMISTIC }
    static final UUID GROUP = new UUID(1, 1), A = new UUID(1, 2), B = new UUID(1, 3), SESSION = new UUID(1, 4);
    @TempDir(factory = DiskTestDirectory.class) Path root;

    @ParameterizedTest @EnumSource(Mode.class)
    void atomicRangeBatchAndOriginalReceiptSurviveReopen(Mode mode) throws Exception {
        Path path = root.resolve(mode.name());
        var changes = command(1, new Mutation(Mutation.Kind.MERGE, A, bytes("b"), bytes("tail")),
                new Mutation(Mutation.Kind.DELETE_RANGE, A, bytes("a"), bytes("b")),
                new Mutation(Mutation.Kind.PUT, B, bytes("paired"), bytes("after")));
        byte[] original;
        try (Pool pool = new Pool(path, mode)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, A, bytes("a"), bytes("old")),
                    new Mutation(Mutation.Kind.PUT, A, bytes("b"), bytes("seed")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("paired"), bytes("before"))));
            try (var pin = pool.store.capture()) {
                original = pool.store.apply(new ApplyPosition(1, 2), changes);
                assertArrayEquals(bytes("old"), pin.get(A, bytes("a")));
                assertArrayEquals(bytes("seed"), pin.get(A, bytes("b")));
                assertNull(pin.operationResult(changes.operationId(), changes.payloadDigest()));
            }
            pool.owner.flushWal(true);
        }
        try (Pool pool = new Pool(path, mode)) {
            assertEquals(new ApplyPosition(1, 2), pool.store.applied());
            assertArrayEquals(original, pool.store.apply(new ApplyPosition(2, 3), changes));
            try (var pin = pool.store.capture()) {
                assertNull(pin.get(A, bytes("a")));
                assertArrayEquals(bytes("seed,tail"), pin.get(A, bytes("b")));
                assertArrayEquals(bytes("after"), pin.get(B, bytes("paired")));
                assertArrayEquals(original, pin.operationResult(changes.operationId(), changes.payloadDigest()));
            }
        }
    }

    @ParameterizedTest @EnumSource(Mode.class)
    void portableImportUsesOriginalCfBindingsAndBorrowedBase(Mode mode) throws Exception {
        Path image = root.resolve(mode + "-image");
        var limits = new NativeRangeImage.Limits(1024 * 1024, 8 * 1024 * 1024, 2);
        try (Pool source = new Pool(root.resolve(mode + "-source"), mode)) {
            source.store.applyNoop(new ApplyPosition(1, 0));
            source.store.apply(new ApplyPosition(1, 1), command(0, new Mutation(Mutation.Kind.MERGE, A, bytes("k"), bytes("operand"))));
            try (var pin = source.store.capture()) { NativeRangeImage.write(image, pin, pin.interval(), limits); }
        }
        try (Pool target = new Pool(root.resolve(mode + "-target"), mode)) {
            target.store.fence();
            target.store.installSnapshot(image, limits);
            target.store.activate();
            try (var pin = target.store.capture()) { assertArrayEquals(bytes("operand"), pin.get(A, bytes("k"))); }
            target.store.apply(new ApplyPosition(1, 2), command(1, new Mutation(Mutation.Kind.MERGE, A, bytes("k"), bytes("next"))));
            try (var pin = target.store.capture()) { assertArrayEquals(bytes("operand,next"), pin.get(A, bytes("k"))); }
        }
    }

    @Test void directOptimisticWrapperRejectsMixedRangeBatchWhileBaseIsBorrowed() throws Exception {
        try (Pool pool = new Pool(root.resolve("wrapper"), Mode.OPTIMISTIC)) {
            try (var batch = new WriteBatch(); var options = new WriteOptions()) {
                batch.put(pool.handles.get(1), bytes("key"), bytes("must-not-apply"));
                batch.deleteRange(pool.handles.get(1), bytes("a"), bytes("z"));
                assertThrows(org.rocksdb.RocksDBException.class, () -> pool.owner.write(options, batch));
                assertNull(pool.owner.get(pool.handles.get(1), bytes("key")));
            }
            try (var base = ((OptimisticTransactionDB) pool.owner).getBaseDB()) {
                assertFalse(base.isOwningHandle());
                base.put(pool.handles.get(2), bytes("key"), bytes("borrowed"));
            }
            assertTrue(pool.owner.isOwningHandle());
            assertArrayEquals(bytes("borrowed"), pool.owner.get(pool.handles.get(2), bytes("key")));
        }
    }

    @Test void pessimisticPoolIsRejectedBeforeAnyDistributedMutation() throws Exception {
        var handles = new ArrayList<ColumnFamilyHandle>();
        try (var options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                var transactionOptions = new TransactionDBOptions(); var plain = new ColumnFamilyOptions();
                var owner = TransactionDB.open(options, transactionOptions, root.resolve("pessimistic").toString(),
                        descriptors(plain, plain), handles)) {
            try {
                assertThrows(IllegalArgumentException.class, () -> store(owner, handles));
                assertEquals(0, owner.getLatestSequenceNumber());
            } finally { for (var handle : handles) handle.close(); }
        }
    }

    private static List<ColumnFamilyDescriptor> descriptors(ColumnFamilyOptions plain, ColumnFamilyOptions merged) {
        return List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, plain),
                new ColumnFamilyDescriptor(bytes("a"), merged), new ColumnFamilyDescriptor(bytes("b"), plain),
                new ColumnFamilyDescriptor(bytes("state"), plain), new ColumnFamilyDescriptor(bytes("receipts"), plain));
    }
    private static ReplicatedRangeStore store(RocksDB db, List<ColumnFamilyHandle> handles) throws Exception {
        var guard = new RangeOwnership();
        var owner = new RangeOwnership.Owner(GROUP, 1);
        guard.register(owner, new RangeOwnership.Interval(GROUP, Set.of(A, B), null, null), RangeOwnership.State.ACTIVE);
        return new ReplicatedRangeStore(db, guard, owner, new byte[32], Map.of(A, handles.get(1), B, handles.get(2)),
                handles.get(3), handles.get(4), Set.of(A));
    }
    private static DistributedCommand command(long sequence, Mutation... mutations) {
        return new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, sequence), List.of(mutations));
    }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static final class Pool implements AutoCloseable {
        final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions plain = new ColumnFamilyOptions();
        final ColumnFamilyOptions merged = new ColumnFamilyOptions().setMergeOperator(new StringAppendOperator(","));
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        final RocksDB owner;
        final ReplicatedRangeStore store;
        Pool(Path path, Mode mode) throws Exception {
            owner = mode == Mode.OPTIMISTIC ? OptimisticTransactionDB.open(options, path.toString(), descriptors(plain, merged), handles)
                    : RocksDB.open(options, path.toString(), descriptors(plain, merged), handles);
            store = store(owner, handles);
        }
        @Override public void close() {
            for (var handle : handles) handle.close();
            owner.close(); merged.close(); plain.close(); options.close();
        }
    }
}
