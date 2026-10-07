package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

class NativeRangeInstallTest {
    static { RocksDB.loadLibrary(); }
    private static final UUID GROUP = new UUID(0, 1), OTHER = new UUID(0, 2), DOMAIN = new UUID(0, 3);
    private static final UUID A = new UUID(0, 4), B = new UUID(0, 5), SESSION = new UUID(0, 6);
    private static final RangeOwnership.Interval INTERVAL = new RangeOwnership.Interval(DOMAIN, Set.of(A, B),
            new byte[] {0x10}, new byte[] {(byte) 0x80});
    private static final NativeRangeImage.Limits LIMITS = new NativeRangeImage.Limits(4 * 1024 * 1024, 16 * 1024 * 1024, 2);
    @TempDir(factory = DiskTestDirectory.class) Path root;

    @Test void interruptedImportStaysFencedAcrossReopenAndRecoversWithoutTouchingNeighbor() throws Exception {
        Path image = root.resolve("image");
        var command = command(0, new Mutation(Mutation.Kind.PUT, A, key(0x20), key(7), Mutation.ReturnMode.PREVIOUS),
                new Mutation(Mutation.Kind.PUT, B, key(0x20), key(8)));
        byte[] originalResult;
        try (Pool source = new Pool(root.resolve("source"))) {
            source.store.initializeConfiguration(new byte[] {1});
            source.store.applyNoop(new ApplyPosition(1, 0));
            originalResult = source.store.apply(new ApplyPosition(1, 1), command);
            source.store.applyNoop(new ApplyPosition(1, 2), new byte[] {2, 3});
            try (var pin = source.store.capture()) { NativeRangeImage.write(image, pin, INTERVAL, LIMITS); }
        }
        var neighborCommand = new DistributedCommand(OTHER, 1, new byte[32], new OperationId(SESSION, 1, 77),
                List.of(new Mutation(Mutation.Kind.PUT, A, key(0x90), key(99))));
        Path targetPath = root.resolve("target");
        try (Pool target = new Pool(targetPath)) {
            target.neighbor.applyNoop(new ApplyPosition(1, 0));
            target.neighbor.apply(new ApplyPosition(1, 1), neighborCommand);
            for (int i = 0; i < 300; i++) target.db.put(target.handles.get(1), new byte[] {0x30, (byte) (i >>> 8), (byte) i}, key(1));
            target.store.fence();
            assertThrows(IOException.class, () -> target.store.installSnapshot(image, LIMITS, batches -> {
                assertEquals(RangeOwnership.State.INSTALLING, target.store.ownership().state());
                assertThrows(IllegalStateException.class, target.store::activate);
                assertThrows(IllegalStateException.class,
                        () -> target.store.operationResult(command.operationId(), command.payloadDigest()));
                if (batches == 1) throw new IOException("Injected interruption after cleanup batch");
            }));
            assertTrue(target.store.pendingInstallation());
            assertEquals(RangeOwnership.State.FENCED, target.store.ownership().state());
            assertNull(target.store.applied());
            assertThrows(IllegalStateException.class, target.store::capture);
            assertThrows(IllegalStateException.class, target.store::activate);
            assertArrayEquals(key(99), target.db.get(target.handles.get(1), key(0x90)));
        }
        try (Pool target = new Pool(targetPath)) {
            assertTrue(target.store.pendingInstallation());
            assertEquals(RangeOwnership.State.FENCED, target.store.ownership().state());
            target.store.installSnapshot(image, LIMITS);
            assertFalse(target.store.pendingInstallation());
            assertEquals(new ApplyPosition(1, 2), target.store.applied());
            assertEquals(RangeOwnership.State.FENCED, target.store.ownership().state());
            target.store.activate();
            assertArrayEquals(new byte[] {2, 3}, target.store.configuration().data());
            assertEquals(2, target.store.configuration().index());
            try (var pin = target.store.capture()) {
                assertArrayEquals(key(7), pin.get(A, key(0x20)));
                assertArrayEquals(key(8), pin.get(B, key(0x20)));
                assertNull(pin.get(A, new byte[] {0x30, 0, 0}));
                assertArrayEquals(originalResult, pin.operationResult(command.operationId(), command.payloadDigest()));
            }
            assertArrayEquals(originalResult, target.store.apply(new ApplyPosition(2, 3), command));
            try (var neighbor = target.neighbor.capture()) {
                assertArrayEquals(key(99), neighbor.get(A, key(0x90)));
                assertNotNull(neighbor.operationResult(neighborCommand.operationId(), neighborCommand.payloadDigest()));
            }
            assertEquals(new ApplyPosition(1, 1), target.neighbor.applied());
        }
    }

    @Test void imageMustMatchTargetAndPinsMustDrainBeforeAnyDestructiveEffects() throws Exception {
        Path image = root.resolve("image");
        try (Pool source = new Pool(root.resolve("source"))) {
            source.store.applyNoop(new ApplyPosition(1, 0));
            source.store.apply(new ApplyPosition(1, 1), command(0, new Mutation(Mutation.Kind.PUT, A, key(0x20), key(7))));
            try (var pin = source.store.capture()) { NativeRangeImage.write(image, pin, INTERVAL, LIMITS); }
        }
        try (Pool target = new Pool(root.resolve("target"))) {
            target.store.applyNoop(new ApplyPosition(1, 0));
            target.store.apply(new ApplyPosition(1, 1), command(1, new Mutation(Mutation.Kind.PUT, A, key(0x20), key(9))));
            var pin = target.store.capture();
            target.store.fence();
            assertThrows(IllegalStateException.class, () -> target.store.installSnapshot(image, LIMITS));
            assertFalse(target.store.pendingInstallation());
            assertArrayEquals(key(9), pin.get(A, key(0x20)));
            pin.close();
            target.neighbor.fence();
            assertThrows(IOException.class, () -> target.neighbor.installSnapshot(image, LIMITS));
            assertFalse(target.neighbor.pendingInstallation());
            target.store.installSnapshot(image, LIMITS);
            target.store.activate();
            try (var restored = target.store.capture()) { assertArrayEquals(key(7), restored.get(A, key(0x20))); }
        }
    }

    @Test void ownedBoundsRejectCrossRangeReadsWritesAndTombstonesBeforeEffects() throws Exception {
        try (Pool pool = new Pool(root.resolve("pool"))) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            var outside = command(0, new Mutation(Mutation.Kind.PUT, A, key(0x90), key(1)));
            assertThrows(IllegalArgumentException.class, () -> pool.store.apply(new ApplyPosition(1, 1), outside));
            var crossing = command(1, new Mutation(Mutation.Kind.DELETE_RANGE, A, key(0x20), key(0x90)));
            assertThrows(IllegalArgumentException.class, () -> pool.store.apply(new ApplyPosition(1, 1), crossing));
            assertEquals(new ApplyPosition(1, 0), pool.store.applied());
            try (var pin = pool.store.capture()) {
                assertThrows(IllegalArgumentException.class, () -> pin.get(A, key(0x90)));
                assertThrows(IllegalArgumentException.class, () -> pin.visitColumn(A, key(1), null, (k, v) -> {}));
            }
        }
    }

    @Test void changedImageDuringImportCannotPublishAnAppliedMarker() throws Exception {
        Path image = root.resolve("changing-image");
        try (Pool source = new Pool(root.resolve("source"))) {
            source.store.applyNoop(new ApplyPosition(1, 0));
            source.store.apply(new ApplyPosition(1, 1), command(0, new Mutation(Mutation.Kind.PUT, A, key(0x20), key(7))));
            try (var pin = source.store.capture()) { NativeRangeImage.write(image, pin, INTERVAL, LIMITS); }
        }
        byte[] original = java.nio.file.Files.readAllBytes(image);
        try (Pool target = new Pool(root.resolve("target"))) {
            target.db.put(target.handles.get(1), key(0x30), key(1));
            target.store.fence();
            assertThrows(IOException.class, () -> target.store.installSnapshot(image, LIMITS, batches -> {
                if (batches == 1) {
                    byte[] corrupt = original.clone();
                    corrupt[corrupt.length - 1] ^= 1;
                    java.nio.file.Files.write(image, corrupt);
                }
            }));
            assertTrue(target.store.pendingInstallation());
            assertNull(target.store.applied());
            assertThrows(IllegalStateException.class, target.store::activate);
            assertThrows(IllegalStateException.class, target.store::capture);
            java.nio.file.Files.write(image, original);
            target.store.installSnapshot(image, LIMITS);
            target.store.activate();
            try (var pin = target.store.capture()) { assertArrayEquals(key(7), pin.get(A, key(0x20))); }
        }
    }

    private static byte[] key(int value) { return new byte[] {(byte) value}; }
    private static DistributedCommand command(long sequence, Mutation... mutations) {
        return new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, sequence), List.of(mutations));
    }
    private static final class Pool implements AutoCloseable {
        final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions plain = new ColumnFamilyOptions();
        final ColumnFamilyOptions merged = new ColumnFamilyOptions().setMergeOperator(new StringAppendOperator(","));
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        final RocksDB db;
        final ReplicatedRangeStore store, neighbor;
        Pool(Path path) throws Exception {
            db = RocksDB.open(options, path.toString(), List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, plain),
                    new ColumnFamilyDescriptor(key(1), merged), new ColumnFamilyDescriptor(key(2), plain),
                    new ColumnFamilyDescriptor(key(3), plain), new ColumnFamilyDescriptor(key(4), plain)), handles);
            var guard = new RangeOwnership();
            var owner = new RangeOwnership.Owner(GROUP, 1);
            var other = new RangeOwnership.Owner(OTHER, 1);
            guard.register(owner, INTERVAL, RangeOwnership.State.ACTIVE);
            guard.register(other, new RangeOwnership.Interval(DOMAIN, Set.of(A, B), key(0x80), null), RangeOwnership.State.ACTIVE);
            var columns = Map.of(A, handles.get(1), B, handles.get(2));
            store = new ReplicatedRangeStore(db, guard, owner, new byte[32], columns, handles.get(3), handles.get(4), Set.of(A));
            neighbor = new ReplicatedRangeStore(db, guard, other, new byte[32], columns, handles.get(3), handles.get(4), Set.of(A));
        }
        @Override public void close() {
            for (var handle : handles) handle.close();
            db.close(); merged.close(); plain.close(); options.close();
        }
    }
}
