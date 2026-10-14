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

    @Test void retentionEstimatesBoundNewResultsAndPreserveNativeStateForRetries() throws Exception {
        try (Pool pool = new Pool(root.resolve("estimates"))) {
            assertEstimate(pool, command(1, 0), true, 1, 88, false);
            var returned = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 99),
                    List.of(new Mutation(Mutation.Kind.PUT, COLUMN, new byte[] {1}, new byte[] {3}, Mutation.ReturnMode.PREVIOUS)));
            assertEstimate(pool, returned, true, 1, 80L + CommandResult.MAX_BYTES, false);
            pool.store.apply(new ApplyPosition(1, 0), command(1, 0), new byte[7]);
            assertEstimate(pool, command(1, 0), false, 0, 0, false);
            long sequence = pool.db.getLatestSequenceNumber();
            var reused = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 0),
                    List.of(new Mutation(Mutation.Kind.PUT, COLUMN, new byte[] {1}, new byte[] {9})));
            assertThrows(IllegalArgumentException.class, () -> pool.store.estimateReceiptRetention(reused));
            var wrongGroup = new DistributedCommand(ADMIN, 1, new byte[32], new OperationId(SESSION, 1, 1), List.of(put()));
            assertThrows(IllegalArgumentException.class, () -> pool.store.estimateReceiptRetention(wrongGroup));
            assertEquals(sequence, pool.db.getLatestSequenceNumber());
            assertUsage(pool, new ApplyPosition(1, 0), 1, 87);
            assertArrayEquals(new byte[] {2}, pool.db.get(pool.handles.get(1), new byte[] {1}));
        }
    }

    @Test void retentionEstimatesSubtractOnlyCurrentAckAndCoarseFloorUsageWithoutNegativeCredits() throws Exception {
        try (Pool pool = new Pool(root.resolve("maintenance-estimates"))) {
            pool.store.apply(new ApplyPosition(1, 0), command(1, 0), new byte[7]);
            pool.store.apply(new ApplyPosition(1, 1), command(1, 1), new byte[13]);
            var partial = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 2), List.of(), List.of(), 0);
            assertEstimate(pool, partial, true, 0, 1, true); // New92 minus covered91; count and bytes clamp independently.
            var returned = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 2),
                    List.of(new Mutation(Mutation.Kind.PUT, COLUMN, new byte[] {1}, new byte[] {3}, Mutation.ReturnMode.PREVIOUS)), List.of(), 0);
            assertEstimate(pool, returned, true, 0, 80L + CommandResult.MAX_BYTES - 87, true);
            pool.store.apply(new ApplyPosition(1, 2), partial);
            assertEstimate(pool, partial, false, 0, 0, true);
            assertEstimate(pool, command(1, 0), false, 0, 0, false);
            var regressive = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 3), List.of(), List.of(), 0);
            assertEstimate(pool, regressive, true, 1, 88, true);
            var advancing = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 3), List.of(), List.of(), 1);
            assertEstimate(pool, advancing, true, 0, 0, true); // Covered97 exceeds the new92; no negative credit escapes.
            pool.store.apply(new ApplyPosition(1, 3), command(2, 0));
            var floor = DistributedCommand.advanceSessionEpochFloor(GROUP, 1, new byte[32], new OperationId(ADMIN, 3, 0), 2);
            assertEstimate(pool, floor, true, 0, 0, true);
            pool.store.apply(new ApplyPosition(1, 4), floor);
            assertEstimate(pool, floor, false, 0, 0, true);
            assertEstimate(pool, command(1, 99), false, 0, 0, false);
            var oldFloor = DistributedCommand.advanceSessionEpochFloor(GROUP, 1, new byte[32], new OperationId(ADMIN, 3, 1), 1);
            assertEstimate(pool, oldFloor, true, 1, 88, true);
            assertUsage(pool, new ApplyPosition(1, 4), 2, 176);
            assertArrayEquals(new byte[] {2}, pool.db.get(pool.handles.get(1), new byte[] {1}));
        }
    }

    private static void assertEstimate(Pool pool, DistributedCommand command, boolean newReceipt,
            long additionalCount, long additionalBytes, boolean maintenance) throws Exception {
        ReceiptUsage usage = pool.store.receiptUsage();
        long sequence = pool.db.getLatestSequenceNumber();
        assertEquals(new ReceiptRetentionEstimate(usage, newReceipt, additionalCount, additionalBytes, maintenance),
                pool.store.estimateReceiptRetention(command));
        assertEquals(sequence, pool.db.getLatestSequenceNumber(), "Estimating current bound state must not write native metadata or effects");
        assertEquals(usage, pool.store.receiptUsage());
    }

    @Test void exactRetainedBytesDuplicatesPartialAckCoarseFloorAndCleanupSurviveReopen() throws Exception {
        var first = command(1, 0);
        var second = command(1, 1);
        long bytes;
        try (Pool pool = new Pool(root.resolve("pool"))) {
            assertUsage(pool, null, 0, 0);
            pool.store.apply(new ApplyPosition(1, 0), first, new byte[11]);
            pool.store.apply(new ApplyPosition(1, 1), second, new byte[23]);
            assertUsage(pool, new ApplyPosition(1, 1), 2, 2 * 80 + 34);
            assertArrayEquals(new byte[11], pool.store.apply(new ApplyPosition(1, 2), first, new byte[99]));
            assertUsage(pool, new ApplyPosition(1, 2), 2, 2 * 80 + 34);
            var ack = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 2), List.of(), List.of(), 0);
            byte[] result = pool.store.apply(new ApplyPosition(1, 3), ack);
            assertUsage(pool, new ApplyPosition(1, 3), 2, 80 + 23 + 80 + result.length);
            assertEquals(CommandResult.Status.SESSION_EXPIRED,
                    CommandResult.decode(pool.store.apply(new ApplyPosition(1, 4), first)).status());
            assertUsage(pool, new ApplyPosition(1, 4), 2, 80 + 23 + 80 + result.length);
            byte[] live = pool.store.apply(new ApplyPosition(1, 5), command(2, 0));
            var fence = DistributedCommand.advanceSessionEpochFloor(GROUP, 1, new byte[32], new OperationId(ADMIN, 3, 0), 2);
            byte[] fenced = pool.store.apply(new ApplyPosition(1, 6), fence);
            bytes = 160L + live.length + fenced.length;
            assertUsage(pool, new ApplyPosition(1, 6), 2, bytes);
            assertNull(pool.db.get(pool.state(), bucket(SESSION, 1)));
            assertBucket(pool, SESSION, 2, 1, 80 + live.length);
            var page = pool.store.reclaimExpiredSessions(null, 1, 4 * 1024 * 1024);
            while (page.continuation() != null) page = pool.store.reclaimExpiredSessions(page.continuation(), 1, 4 * 1024 * 1024);
            assertUsage(pool, new ApplyPosition(1, 6), 2, bytes);
            pool.db.flushWal(true);
        }
        try (Pool pool = new Pool(root.resolve("pool"))) {
            assertUsage(pool, new ApplyPosition(1, 6), 2, bytes);
        }
    }

    @Test void establishedAccountingCorruptionFailsWithoutRepairOnAccessorOrReopen() throws Exception {
        for (String corruption:List.of("missing", "short", "position", "count", "reserved", "bucket-missing", "bucket-count", "total-count", "marker")) {
            Path path=root.resolve("corrupt-"+corruption);
            try (Pool pool=new Pool(path)) {
                pool.store.apply(new ApplyPosition(1,0),command(1,0),new byte[7]);
                switch (corruption) {
                    case "missing" -> pool.db.delete(pool.state(),metadata((byte)0x10));
                    case "short" -> pool.db.put(pool.state(),metadata((byte)0x10),usage(1,87));
                    case "position" -> pool.db.put(pool.state(),metadata((byte)0x10),total(1,87,new ApplyPosition(1,99)));
                    case "count" -> { byte[] bad=total(1,87,new ApplyPosition(1,0)); bad[3]=2; pool.db.put(pool.state(),metadata((byte)0x10),bad); }
                    case "reserved" -> pool.db.put(pool.state(),metadata((byte)0x12),new byte[]{1});
                    case "bucket-missing" -> pool.db.delete(pool.state(),bucket(SESSION,1));
                    case "bucket-count" -> pool.db.put(pool.state(),bucket(SESSION,1),usage(2,182));
                    case "total-count" -> pool.db.put(pool.state(),metadata((byte)0x10),total(2,182,new ApplyPosition(1,0)));
                    case "marker" -> pool.db.put(pool.state(),ByteBuffer.allocate(16).putLong(0).putLong(1).array(),
                            ByteBuffer.allocate(16).putLong(1).putLong(1).array());
                    default -> throw new AssertionError(corruption);
                }
                long sequence=pool.db.getLatestSequenceNumber();
                assertThrows(IllegalStateException.class,pool::openStore,corruption);
                if (List.of("missing","short","position","count","reserved","marker").contains(corruption)) {
                    assertThrows(IllegalStateException.class,pool.store::receiptUsage,corruption);
                }
                assertEquals(sequence,pool.db.getLatestSequenceNumber(),"Rejecting corrupt accounting must not repair metadata");
                assertArrayEquals(new byte[]{2},pool.db.get(pool.handles.get(1),new byte[]{1})); pool.db.flushWal(true);
            }
            assertThrows(IllegalStateException.class,() -> new Pool(path),corruption);
        }
    }

    @Test void conflictAndOversizedResultPiggybackAckRemoveExactCoveredUsage() throws Exception {
        try (Pool pool = new Pool(root.resolve("results"))) {
            pool.store.apply(new ApplyPosition(1, 0), command(1, 0), new byte[31]);
            var conflict = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 1),
                    List.of(put()), List.of(new KeyPredicate(COLUMN, new byte[] {1}, new byte[] {9})), 0);
            byte[] result = pool.store.apply(new ApplyPosition(1, 1), conflict);
            assertEquals(CommandResult.Status.CONFLICT, CommandResult.decode(result).status());
            assertUsage(pool, new ApplyPosition(1, 1), 1, 80 + result.length);
            pool.db.put(pool.handles.get(1), new byte[] {1}, new byte[2 * 1024 * 1024]);
            var large = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 2),
                    List.of(new Mutation(Mutation.Kind.PUT, COLUMN, new byte[] {1}, new byte[] {3}, Mutation.ReturnMode.PREVIOUS)), List.of(), 1);
            byte[] largeResult = pool.store.apply(new ApplyPosition(1, 2), large);
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE, CommandResult.decode(largeResult).status());
            assertUsage(pool, new ApplyPosition(1, 2), 1, 80 + largeResult.length);
            assertArrayEquals(largeResult, pool.store.apply(new ApplyPosition(1, 3), large));
            assertUsage(pool, new ApplyPosition(1, 3), 1, 80 + largeResult.length);
        }
    }

    @Test void onlyFreshOwnedRangesCanInitializeEmptyCurrentAccounting() throws Exception {
        try (Pool pool=new Pool(root.resolve("fresh"))) {
            assertUsage(pool,null,0,0);
            pool.db.delete(pool.state(),metadata((byte)0x10));
            // Shared CFs may contain another group's metadata and receipts without establishing this owned range.
            byte[] foreignTotal=metadata((byte)0x10); foreignTotal[15]=9;
            byte[] foreignReceipt=ByteBuffer.allocate(48).putLong(0).putLong(9).putLong(0).putLong(2).putLong(1).putLong(0).array();
            pool.db.put(pool.state(),foreignTotal,total(0,0,null));
            pool.db.put(pool.handles.get(3),foreignReceipt,new byte[40]);
            assertUsage(pool,null,0,0);
            long sequence=pool.db.getLatestSequenceNumber();
            assertUsage(pool,null,0,0); assertEquals(sequence,pool.db.getLatestSequenceNumber());
            assertArrayEquals(total(0,0,null),pool.db.get(pool.state(),foreignTotal));
            assertNotNull(pool.db.get(pool.handles.get(3),foreignReceipt));
        }
        for (boolean receipt:new boolean[]{false,true}) try (Pool pool=new Pool(root.resolve("orphan-"+receipt))) {
            pool.db.delete(pool.state(),metadata((byte)0x10));
            if (receipt) {
                var command=command(1,0);
                byte[] key=ByteBuffer.allocate(48).putLong(0).putLong(1).putLong(0).putLong(2).putLong(1).putLong(0).array();
                pool.db.put(pool.handles.get(3),key,ByteBuffer.allocate(40).put(command.receiptDigest()).put(new byte[8]).array());
            } else pool.db.put(pool.state(),bucket(SESSION,1),usage(1,88));
            long sequence=pool.db.getLatestSequenceNumber();
            assertThrows(IllegalStateException.class,pool::openStore);
            assertThrows(IllegalStateException.class,pool.store::receiptUsage);
            assertNull(pool.db.get(pool.state(),metadata((byte)0x10))); assertEquals(sequence,pool.db.getLatestSequenceNumber());
        }
    }

    @Test void imagesRequireCurrentBoundTotalsAndInstallCountersWithoutNormalization() throws Exception {
        Path image=root.resolve("current.image"); byte[] expectedTotal,expectedBucket;
        try (Pool pool=new Pool(root.resolve("image-source"))) {
            pool.store.apply(new ApplyPosition(1,0),command(1,0),new byte[7]);
            pool.db.put(pool.state(),bucket(SESSION,1),usage(2,182));
            try (var pin=pool.store.capture()) {
                assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("bad.image"),pin,pin.interval(),LIMITS));
            }
            pool.db.put(pool.state(),bucket(SESSION,1),usage(1,87));
            expectedTotal=pool.db.get(pool.state(),metadata((byte)0x10)); expectedBucket=pool.db.get(pool.state(),bucket(SESSION,1));
            try (var pin=pool.store.capture()) { NativeRangeImage.write(image,pin,pin.interval(),LIMITS); }
        }
        for (boolean omitted:new boolean[]{false,true}) {
            Path malformed=root.resolve("total-"+omitted+".image"); java.nio.file.Files.copy(image,malformed);
            rewriteTotal(malformed,omitted);
            assertThrows(java.io.IOException.class,() -> NativeRangeImage.inspect(malformed,LIMITS));
            try (Pool target=new Pool(root.resolve("reject-image-"+omitted))) {
                target.store.fence(); assertThrows(java.io.IOException.class,() -> target.store.installSnapshot(malformed,LIMITS));
                assertFalse(target.store.pendingInstallation()); assertUsage(target,null,0,0);
            }
        }
        for (boolean reserved:new boolean[]{false,true}) {
            byte[] malformed=java.nio.file.Files.readAllBytes(image), totalKey=metadata((byte)0x10); boolean found=false;
            for (int i=0;i<=malformed.length-totalKey.length;i++) {
                if (Arrays.equals(malformed,i,i+totalKey.length,totalKey,0,totalKey.length)) {
                    if (reserved) malformed[i+16]=0x12;
                    else ByteBuffer.wrap(malformed,i+totalKey.length+16,8).putLong(99);
                    found=true; break;
                }
            }
            assertTrue(found); reseal(malformed); Path invalid=root.resolve("bound-"+reserved+".image"); java.nio.file.Files.write(invalid,malformed);
            assertThrows(java.io.IOException.class,() -> NativeRangeImage.inspect(invalid,LIMITS));
        }
        try (Pool empty=new Pool(root.resolve("empty-image-source"))) {
            empty.store.applyNoop(new ApplyPosition(1,0)); Path missing=root.resolve("empty-missing-total.image");
            try (var pin=empty.store.capture()) { NativeRangeImage.write(missing,pin,pin.interval(),LIMITS); }
            rewriteTotal(missing,true); assertThrows(java.io.IOException.class,() -> NativeRangeImage.inspect(missing,LIMITS));
        }
        try (Pool target=new Pool(root.resolve("current-target"))) {
            target.store.fence(); target.store.installSnapshot(image,LIMITS);
            assertArrayEquals(expectedTotal,target.db.get(target.state(),metadata((byte)0x10)));
            assertArrayEquals(expectedBucket,target.db.get(target.state(),bucket(SESSION,1)));
            assertUsage(target,new ApplyPosition(1,0),1,87); target.store.activate();
            assertArrayEquals(new byte[7],target.store.apply(new ApplyPosition(1,1),command(1,0)));
            assertUsage(target,new ApplyPosition(1,1),1,87);
        }
    }

    @Test void importRejectsPerBucketMismatchWithoutRepairAndRetainsIncompleteInstallationAuthority() throws Exception {
        Path image=root.resolve("swapped-buckets.image");
        try (Pool source=new Pool(root.resolve("swapped-source"))) {
            source.store.apply(new ApplyPosition(1,0),command(1,0),new byte[7]);
            source.store.apply(new ApplyPosition(1,1),command(2,0),new byte[13]);
            source.db.put(source.state(),bucket(SESSION,1),usage(1,93)); source.db.put(source.state(),bucket(SESSION,2),usage(1,87));
            try (var pin=source.store.capture()) { NativeRangeImage.write(image,pin,pin.interval(),LIMITS); }
        }
        // Aggregate image accounting is bounded; the imported native rows permit exact per-bucket validation.
        NativeRangeImage.inspect(image,LIMITS);
        Path targetPath=root.resolve("swapped-target");
        try (Pool target=new Pool(targetPath)) {
            target.store.fence(); assertThrows(IllegalStateException.class,() -> target.store.installSnapshot(image,LIMITS));
            assertTrue(target.store.pendingInstallation()); assertNull(target.store.applied());
            assertEquals(RangeOwnership.State.FENCED,target.store.ownership().state());
            assertThrows(IllegalStateException.class,target.store::activate); assertThrows(IllegalStateException.class,target.store::receiptUsage);
            assertBucket(target,SESSION,1,1,93); assertBucket(target,SESSION,2,1,87);
        }
        try (Pool recovered=new Pool(targetPath)) {
            assertTrue(recovered.store.pendingInstallation()); assertNull(recovered.store.applied());
            assertEquals(RangeOwnership.State.FENCED,recovered.store.ownership().state());
            assertThrows(IllegalStateException.class,recovered.store::activate);
        }
    }

    private static void rewriteTotal(Path image,boolean omitted) throws Exception {
        byte[] original=java.nio.file.Files.readAllBytes(image); var input=ByteBuffer.wrap(original); input.position(92);
        int columns=input.getInt(); input.position(input.position()+columns*16);
        for (int i=0;i<2;i++) { int n=input.getInt(); if (n>=0) input.position(input.position()+n); }
        int merges=input.getInt(); input.position(input.position()+merges*16);
        var bytes=new java.io.ByteArrayOutputStream(); bytes.write(original,0,input.position()); long rows=0;
        while (true) {
            int start=input.position(), tag=input.get()&255; if (tag==0) break;
            if (tag==1) input.position(input.position()+16);
            int keyLength=input.getInt(),valueLength=input.getInt(),keyStart=input.position(); input.position(keyStart+keyLength+valueLength);
            if (tag==2 && keyLength==17 && original[keyStart+16]==0x10) {
                if (omitted) continue;
                var row=new java.io.DataOutputStream(bytes); row.writeByte(tag); row.writeInt(keyLength); row.writeInt(16);
                row.write(original,keyStart,keyLength); row.write(original,keyStart+keyLength,16);
            } else bytes.write(original,start,input.position()-start);
            rows++;
        }
        var output=new java.io.DataOutputStream(bytes); output.writeByte(0); output.writeLong(rows); output.flush();
        bytes.write(java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray())); java.nio.file.Files.write(image,bytes.toByteArray());
    }
    private static void reseal(byte[] encoded) throws Exception {
        byte[] checksum=java.security.MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(encoded,encoded.length-32));
        System.arraycopy(checksum,0,encoded,encoded.length-32,32);
    }
    private static byte[] total(long count,long bytes,ApplyPosition position) {
        return ByteBuffer.allocate(32).put(usage(count,bytes)).putLong(position==null ? 0 : position.term()).putLong(position==null ? -1 : position.index()).array();
    }

    private static void assertUsage(Pool pool, ApplyPosition position, long count, long bytes) throws Exception {
        assertEquals(new ReceiptUsage(position, count, bytes), pool.store.receiptUsage());
        assertArrayEquals(ByteBuffer.allocate(32).put(usage(count, bytes))
                .putLong(position == null ? 0 : position.term()).putLong(position == null ? -1 : position.index()).array(),
                pool.db.get(pool.state(), metadata((byte) 0x10)));
    }
    private static void assertBucket(Pool pool, UUID session, long epoch, long count, long bytes) throws Exception {
        assertArrayEquals(usage(count, bytes), pool.db.get(pool.state(), bucket(session, epoch)));
    }
    private static byte[] usage(long count, long bytes) { return ByteBuffer.allocate(16).putLong(count).putLong(bytes).array(); }
    private static byte[] metadata(byte tag) { return ByteBuffer.allocate(17).putLong(0).putLong(1).put(tag).array(); }
    private static byte[] bucket(UUID session, long epoch) {
        return ByteBuffer.allocate(41).put(metadata((byte) 0x11)).putLong(epoch).putLong(session.getMostSignificantBits()).putLong(session.getLeastSignificantBits()).array();
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
            try { store=openStore(); }
            catch (Exception | Error failure) { close(); throw failure; }
        }
        ReplicatedRangeStore openStore() throws Exception {
            var ownership=new RangeOwnership(); var owner=new RangeOwnership.Owner(GROUP,1);
            ownership.register(owner,new RangeOwnership.Interval(DOMAIN,Set.of(COLUMN),null,null),RangeOwnership.State.ACTIVE);
            return new ReplicatedRangeStore(db,ownership,owner,new byte[32],Map.of(COLUMN,handles.get(1)),state(),handles.get(3),Set.of());
        }
        ColumnFamilyHandle state() { return handles.get(2); }
        public void close() { handles.forEach(ColumnFamilyHandle::close); db.close(); cfOptions.close(); options.close(); }
    }
}
