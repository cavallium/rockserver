package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

class ReceiptUsageAccountingTest {
    static { RocksDB.loadLibrary(); }
    private static final UUID GROUP = new UUID(0, 1), SESSION = new UUID(0, 2), ADMIN = new UUID(0, 3);
    private static final UUID COLUMN = new UUID(0, 4), DOMAIN = new UUID(0, 5);
    private static final NativeRangeImage.Limits LIMITS = new NativeRangeImage.Limits(4 * 1024 * 1024, 16 * 1024 * 1024, 1);
    @TempDir(factory = DiskTestDirectory.class) Path root;

    @Test void exactRetainedBytesDuplicatesPartialAckCoarseFloorAndCleanupSurviveReopen() throws Exception {
        var first = command(1, 0);
        var second = command(1, 1);
        long bytes;
        try (Pool pool = new Pool(root.resolve("pool"))) {
            assertUsage(pool, null, 0, 0);
            pool.store.apply(new ApplyPosition(1, 0), first, new byte[11]);
            pool.store.apply(new ApplyPosition(1, 1), second, new byte[23]);
            assertUsage(pool, new ApplyPosition(1, 1), 2, 2 * 84 + 34);
            assertArrayEquals(new byte[11], pool.store.apply(new ApplyPosition(1, 2), first, new byte[99]));
            assertUsage(pool, new ApplyPosition(1, 2), 2, 2 * 84 + 34);
            var ack = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 2), List.of(), List.of(), 0);
            byte[] result = pool.store.apply(new ApplyPosition(1, 3), ack);
            assertUsage(pool, new ApplyPosition(1, 3), 2, 84 + 23 + 84 + result.length);
            assertEquals(CommandResult.Status.SESSION_EXPIRED,
                    CommandResult.decode(pool.store.apply(new ApplyPosition(1, 4), first)).status());
            assertUsage(pool, new ApplyPosition(1, 4), 2, 84 + 23 + 84 + result.length);
            byte[] live = pool.store.apply(new ApplyPosition(1, 5), command(2, 0));
            var fence = DistributedCommand.advanceSessionEpochFloor(GROUP, 1, new byte[32], new OperationId(ADMIN, 3, 0), 2);
            byte[] fenced = pool.store.apply(new ApplyPosition(1, 6), fence);
            bytes = 168L + live.length + fenced.length;
            assertUsage(pool, new ApplyPosition(1, 6), 2, bytes);
            assertNull(pool.db.get(pool.state(), bucket(SESSION, 1)));
            assertBucket(pool, SESSION, 2, 1, 84 + live.length);
            var page = pool.store.reclaimExpiredSessions(null, 1, 4 * 1024 * 1024);
            while (page.continuation() != null) page = pool.store.reclaimExpiredSessions(page.continuation(), 1, 4 * 1024 * 1024);
            assertUsage(pool, new ApplyPosition(1, 6), 2, bytes);
            pool.db.flushWal(true);
        }
        try (Pool pool = new Pool(root.resolve("pool"))) {
            assertUsage(pool, new ApplyPosition(1, 6), 2, bytes);
        }
    }

    @Test void legacyWriterAdvancingMarkerWithExistingCountersIsRecoveredOnReopenAndAccessor() throws Exception {
        Path path = root.resolve("legacy-writer");
        try (Pool pool = new Pool(path)) {
            pool.store.apply(new ApplyPosition(1, 0), command(1, 0), new byte[7]);
            legacyAppend(pool, new ApplyPosition(1, 1), command(1, 1), new byte[13]);
            pool.db.flushWal(true);
        }
        try (Pool pool = new Pool(path)) {
            assertUsage(pool, new ApplyPosition(1, 1), 2, 188);
            legacyAppend(pool, new ApplyPosition(1, 2), command(1, 2), new byte[19]);
            assertUsage(pool, new ApplyPosition(1, 2), 3, 291);
            pool.store.applyNoop(new ApplyPosition(2, 3), new byte[] {1});
            assertUsage(pool, new ApplyPosition(2, 3), 3, 291);
            // A pre-binding total is never trusted, even with plausible counts and no rebuild flag.
            pool.db.put(pool.state(), metadata((byte) 0x10), usage(1, 91));
            assertUsage(pool, new ApplyPosition(2, 3), 3, 291);
        }
    }

    private static void legacyAppend(Pool pool, ApplyPosition position, DistributedCommand command, byte[] result) throws Exception {
        var id = command.operationId();
        byte[] receipt = ByteBuffer.allocate(48).putLong(0).putLong(1)
                .putLong(id.sessionId().getMostSignificantBits()).putLong(id.sessionId().getLeastSignificantBits())
                .putLong(id.epoch()).putLong(id.sequence()).array();
        try (var batch = new WriteBatch(); var write = new WriteOptions().setDisableWAL(false).setSync(false)) {
            batch.put(pool.handles.get(1), new byte[] {1}, new byte[] {2});
            batch.put(pool.handles.get(3), receipt, ByteBuffer.allocate(36 + result.length).putInt(1).put(command.payloadDigest()).put(result).array());
            batch.put(pool.state(), ByteBuffer.allocate(16).putLong(0).putLong(1).array(),
                    ByteBuffer.allocate(20).putInt(1).putLong(position.term()).putLong(position.index()).array());
            pool.db.write(write, batch);
        }
    }

    @Test void conflictAndOversizedResultPiggybackAckRemoveExactCoveredUsage() throws Exception {
        try (Pool pool = new Pool(root.resolve("results"))) {
            pool.store.apply(new ApplyPosition(1, 0), command(1, 0), new byte[31]);
            var conflict = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 1),
                    List.of(put()), List.of(new KeyPredicate(COLUMN, new byte[] {1}, new byte[] {9})), 0);
            byte[] result = pool.store.apply(new ApplyPosition(1, 1), conflict);
            assertEquals(CommandResult.Status.CONFLICT, CommandResult.decode(result).status());
            assertUsage(pool, new ApplyPosition(1, 1), 1, 84 + result.length);
            pool.db.put(pool.handles.get(1), new byte[] {1}, new byte[2 * 1024 * 1024]);
            var large = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 2),
                    List.of(new Mutation(Mutation.Kind.PUT, COLUMN, new byte[] {1}, new byte[] {3}, Mutation.ReturnMode.PREVIOUS)), List.of(), 1);
            byte[] largeResult = pool.store.apply(new ApplyPosition(1, 2), large);
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE, CommandResult.decode(largeResult).status());
            assertUsage(pool, new ApplyPosition(1, 2), 1, 84 + largeResult.length);
            assertArrayEquals(largeResult, pool.store.apply(new ApplyPosition(1, 3), large));
            assertUsage(pool, new ApplyPosition(1, 3), 1, 84 + largeResult.length);
        }
    }

    @Test void legacyAndInterruptedCounterRecoveryRebuildExactNativeBucketsAndImages() throws Exception {
        Path poolPath = root.resolve("legacy"), image = root.resolve("legacy.image");
        var first = command(1, 0);
        try (Pool pool = new Pool(poolPath)) {
            pool.store.apply(new ApplyPosition(1, 0), first, new byte[7]);
            pool.store.apply(new ApplyPosition(1, 1), command(2, 0), new byte[13]);
            deleteUsage(pool);
            pool.db.flushWal(true);
        }
        try (Pool pool = new Pool(poolPath)) {
            assertUsage(pool, new ApplyPosition(1, 1), 2, 188);
            assertBucket(pool, SESSION, 1, 1, 91);
            assertBucket(pool, SESSION, 2, 1, 97);
            // Simulate a crash after partial rebuild writes: the flag makes partial totals unusable.
            pool.db.put(pool.state(), metadata((byte) 0x12), new byte[] {1});
            pool.db.put(pool.state(), metadata((byte) 0x10), usage(1, 91));
            pool.db.put(pool.state(), bucket(ADMIN, 99), usage(1, 84));
            pool.db.flushWal(true);
        }
        try (Pool pool = new Pool(poolPath)) {
            assertUsage(pool, new ApplyPosition(1, 1), 2, 188);
            assertNull(pool.db.get(pool.state(), bucket(ADMIN, 99)));
            assertNull(pool.db.get(pool.state(), metadata((byte) 0x12)));
            // A pin predating a local legacy-counter removal retains its complete cut.
            try (var pin = pool.store.capture()) {
                deleteUsage(pool);
                NativeRangeImage.write(image, pin, pin.interval(), LIMITS);
            }
            try (Pool target = new Pool(root.resolve("target"))) {
                target.store.fence();
                target.store.installSnapshot(image, LIMITS);
                assertUsage(target, new ApplyPosition(1, 1), 2, 188);
                target.store.activate();
                assertArrayEquals(new byte[7], target.store.apply(new ApplyPosition(1, 2), first));
                assertUsage(target, new ApplyPosition(1, 2), 2, 188);
            }
            assertUsage(pool, new ApplyPosition(1, 1), 2, 188);
        }
    }

    @Test void imagesRejectPartialOrMismatchedUsageAndLegacyImageInstallRebuilds() throws Exception {
        Path image = root.resolve("legacy-cut.image");
        try (Pool pool = new Pool(root.resolve("image-source"))) {
            pool.store.apply(new ApplyPosition(1, 0), command(1, 0), new byte[7]);
            pool.db.put(pool.state(), bucket(SESSION, 1), usage(2, 182));
            try (var pin = pool.store.capture()) {
                assertThrows(java.io.IOException.class, () -> NativeRangeImage.write(root.resolve("bad.image"), pin, pin.interval(), LIMITS));
            }
            deleteUsage(pool);
            // Remove usage rows from a correctly checksummed image to model the historical v1 format.
            assertUsage(pool, new ApplyPosition(1, 0), 1, 91);
            try (var pin = pool.store.capture()) { NativeRangeImage.write(image, pin, pin.interval(), LIMITS); }
        }
        byte[] partial = java.nio.file.Files.readAllBytes(image);
        // Turn the total row into a rebuild flag and reseal the body checksum.
        byte[] totalKey = metadata((byte) 0x10);
        for (int i = 0; i <= partial.length - totalKey.length; i++) {
            if (Arrays.equals(partial, i, i + totalKey.length, totalKey, 0, totalKey.length)) {
                partial[i + 16] = 0x12;
                break;
            }
        }
        byte[] checksum = java.security.MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(partial, partial.length - 32));
        System.arraycopy(checksum, 0, partial, partial.length - 32, 32);
        Path invalid = root.resolve("partial.image");
        java.nio.file.Files.write(invalid, partial);
        assertThrows(java.io.IOException.class, () -> NativeRangeImage.inspect(invalid, LIMITS));
        // A new total with the wrong completed position is rejected even with a valid body checksum.
        byte[] wrongPosition = java.nio.file.Files.readAllBytes(image);
        for (int i = 0; i <= wrongPosition.length - totalKey.length; i++) {
            if (Arrays.equals(wrongPosition, i, i + totalKey.length, totalKey, 0, totalKey.length)) {
                ByteBuffer.wrap(wrongPosition, i + totalKey.length + 20, 8).putLong(99);
                break;
            }
        }
        checksum = java.security.MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(wrongPosition, wrongPosition.length - 32));
        System.arraycopy(checksum, 0, wrongPosition, wrongPosition.length - 32, 32);
        Path wrong = root.resolve("wrong-position.image");
        java.nio.file.Files.write(wrong, wrongPosition);
        assertThrows(java.io.IOException.class, () -> NativeRangeImage.inspect(wrong, LIMITS));
        Path legacyCounters = root.resolve("legacy-counters.image");
        java.nio.file.Files.copy(image, legacyCounters);
        rewriteUsageRows(legacyCounters, true);
        try (Pool target = new Pool(root.resolve("legacy-counter-target"))) {
            target.store.fence(); target.store.installSnapshot(legacyCounters, LIMITS);
            assertUsage(target, new ApplyPosition(1, 0), 1, 91);
        }
        rewriteUsageRows(image, false);
        NativeRangeImage.inspect(image, LIMITS);
        try (Pool target = new Pool(root.resolve("legacy-target"))) {
            target.store.fence(); target.store.installSnapshot(image, LIMITS);
            assertUsage(target, new ApplyPosition(1, 0), 1, 91);
            assertBucket(target, SESSION, 1, 1, 91);
        }
    }

    private static void rewriteUsageRows(Path image, boolean legacyCounters) throws Exception {
        byte[] original = java.nio.file.Files.readAllBytes(image);
        var input = ByteBuffer.wrap(original);
        input.position(96);
        int columns = input.getInt(); input.position(input.position() + columns * 16);
        for (int i = 0; i < 2; i++) { int n = input.getInt(); if (n >= 0) input.position(input.position() + n); }
        int merges = input.getInt(); input.position(input.position() + merges * 16);
        var bytes = new java.io.ByteArrayOutputStream();
        bytes.write(original, 0, input.position());
        long rows = 0;
        while (true) {
            int start = input.position(), tag = input.get() & 255;
            if (tag == 0) break;
            if (tag == 1) input.position(input.position() + 16);
            int keyLength = input.getInt(), valueLength = input.getInt(), keyStart = input.position();
            input.position(keyStart + keyLength + valueLength);
            if (tag == 2 && keyLength > 16 && (original[keyStart + 16] == 0x10 || original[keyStart + 16] == 0x11)) {
                if (!legacyCounters) continue;
                if (original[keyStart + 16] == 0x10) {
                    var row = new java.io.DataOutputStream(bytes);
                    row.writeByte(tag); row.writeInt(keyLength); row.writeInt(20);
                    row.write(original, keyStart, keyLength);
                    row.write(usage(0, 0)); // Legacy counters may be stale; installation must derive them from receipts.
                    rows++;
                    continue;
                }
            }
            bytes.write(original, start, input.position() - start); rows++;
        }
        var output = new java.io.DataOutputStream(bytes);
        output.writeByte(0); output.writeLong(rows); output.flush();
        byte[] body = bytes.toByteArray();
        bytes.write(java.security.MessageDigest.getInstance("SHA-256").digest(body));
        java.nio.file.Files.write(image, bytes.toByteArray());
    }

    private static void assertUsage(Pool pool, ApplyPosition position, long count, long bytes) throws Exception {
        assertEquals(new ReceiptUsage(position, count, bytes), pool.store.receiptUsage());
        assertArrayEquals(ByteBuffer.allocate(36).put(usage(count, bytes))
                .putLong(position == null ? 0 : position.term()).putLong(position == null ? -1 : position.index()).array(),
                pool.db.get(pool.state(), metadata((byte) 0x10)));
    }
    private static void assertBucket(Pool pool, UUID session, long epoch, long count, long bytes) throws Exception {
        assertArrayEquals(usage(count, bytes), pool.db.get(pool.state(), bucket(session, epoch)));
    }
    private static byte[] usage(long count, long bytes) { return ByteBuffer.allocate(20).putInt(1).putLong(count).putLong(bytes).array(); }
    private static byte[] metadata(byte tag) { return ByteBuffer.allocate(17).putLong(0).putLong(1).put(tag).array(); }
    private static byte[] bucket(UUID session, long epoch) {
        return ByteBuffer.allocate(41).put(metadata((byte) 0x11)).putLong(epoch).putLong(session.getMostSignificantBits()).putLong(session.getLeastSignificantBits()).array();
    }
    private static void deleteUsage(Pool pool) throws Exception {
        pool.db.delete(pool.state(), metadata((byte) 0x10));
        pool.db.delete(pool.state(), bucket(SESSION, 1)); pool.db.delete(pool.state(), bucket(SESSION, 2));
    }
    private static Mutation put() { return new Mutation(Mutation.Kind.PUT, COLUMN, new byte[] {1}, new byte[] {2}); }
    private static DistributedCommand command(long epoch, long sequence) {
        return new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, epoch, sequence), List.of(put()));
    }
    private static final class Pool implements AutoCloseable {
        final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        final RocksDB db;
        final ReplicatedRangeStore store;
        Pool(Path path) throws Exception {
            db = RocksDB.open(options, path.toString(), List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                    new ColumnFamilyDescriptor(new byte[] {1}, cfOptions), new ColumnFamilyDescriptor(new byte[] {2}, cfOptions),
                    new ColumnFamilyDescriptor(new byte[] {3}, cfOptions)), handles);
            var ownership = new RangeOwnership();
            var owner = new RangeOwnership.Owner(GROUP, 1);
            ownership.register(owner, new RangeOwnership.Interval(DOMAIN, Set.of(COLUMN), null, null), RangeOwnership.State.ACTIVE);
            store = new ReplicatedRangeStore(db, ownership, owner, new byte[32], Map.of(COLUMN, handles.get(1)), state(), handles.get(3), Set.of());
        }
        ColumnFamilyHandle state() { return handles.get(2); }
        public void close() { handles.forEach(ColumnFamilyHandle::close); db.close(); cfOptions.close(); options.close(); }
    }
}
