package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.*;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.distributed.TransactionScope;
import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.test.DiskTestDirectory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class NativePoolSnapshotAdmissionTest {
    @TempDir(factory = DiskTestDirectory.class) Path directory;
    private static final UUID COLUMN = UUID.randomUUID(), DOMAIN = UUID.randomUUID();
    private static final PoolSnapshotAdmission.Limits ONE = new PoolSnapshotAdmission.Limits(1, 1L << 40, Duration.ofHours(1), 0);
    private ReplicatedRangeStore range(DistributedStoragePool pool, UUID group, byte[] lower, byte[] upper) throws Exception {
        return pool.openRange(new RangeOwnership.Owner(group, 1), new RangeOwnership.Interval(DOMAIN, Set.of(COLUMN), lower, upper),
                new byte[32], Map.of(COLUMN, "shared_data"), Set.of());
    }
    @Test void twoRangesShareQuotaAndBusyExportKeepsChargeUntilNativeDrain() throws Exception {
        try (var pool = DistributedStoragePool.open(directory.resolve("pool"), ConfigParser.parseDefault(), LoggerFactory.getLogger(getClass()))) {
            pool.configureSnapshotAdmission(ONE);
            var left = range(pool, UUID.randomUUID(), null, new byte[] {64});
            var right = range(pool, UUID.randomUUID(), new byte[] {64}, null);
            assertThrows(IllegalStateException.class, () -> pool.configureSnapshotAdmission(ONE));
            var scope = new TransactionScope(java.util.List.of(TransactionScope.Span.range(COLUMN, null, new byte[] {64})));
            var snapshot = left.capture(scope);
            var retain = snapshot.getClass().getDeclaredMethod("retainForExport");
            retain.setAccessible(true);
            var use = (AutoCloseable) retain.invoke(snapshot);
            try {
                long estimate = pool.snapshotAdmissionStats().estimatedBytes();
                assertTrue(estimate > 0);
                snapshot.close();
                assertEquals(1, left.activeSnapshotPins());
                assertEquals(1, pool.snapshotAdmissionStats().pins());
                assertEquals(estimate, pool.snapshotAdmissionStats().estimatedBytes());
                var failure = assertThrows(RocksDBException.class, right::capture);
                assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED, failure.getErrorUniqueId());
                assertEquals(0, right.activeSnapshotPins());
                assertFalse(pool.close(Duration.ZERO));
                assertEquals(1, pool.snapshotAdmissionStats().pins());
            } finally { snapshot.close(); use.close(); }
            assertEquals(0, pool.snapshotAdmissionStats().pins());
            assertEquals(0, left.activeSnapshotPins());
            assertTrue(pool.close(Duration.ofSeconds(1)));
        }
    }
    @Test void failedCreatedSnapshotReleasesNativeAndAdmissionExactlyOnce() throws Exception {
        try (var pool = DistributedStoragePool.open(directory.resolve("failure"), ConfigParser.parseDefault(), LoggerFactory.getLogger(getClass()))) {
            pool.configureSnapshotAdmission(ONE);
            UUID group = UUID.randomUUID();
            var range = range(pool, group, null, null);
            var loadedField = DistributedStoragePool.class.getDeclaredField("loaded"); loadedField.setAccessible(true);
            var loaded = (RocksDBLoader.LoadedDb) loadedField.get(pool);
            var state = loaded.db().getStartupColumns().entrySet().stream()
                    .filter(entry -> new String(entry.getKey().getName(), java.nio.charset.StandardCharsets.US_ASCII).equals(DistributedStoragePool.STATE_COLUMN))
                    .findFirst().orElseThrow().getValue();
            byte[] marker = ByteBuffer.allocate(16).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).array();
            loaded.db().get().put(state, marker, new byte[] {1});
            assertThrows(IllegalStateException.class, () -> range.readCurrentPoint(COLUMN, new byte[] {1}));
            assertThrows(IllegalStateException.class, range::capture);
            assertEquals(0, pool.snapshotAdmissionStats().pins());
            assertEquals(0, pool.snapshotAdmissionStats().estimatedBytes());
            assertEquals(0, range.activeSnapshotPins());
            assertEquals(0, loaded.db().get().getLongProperty("rocksdb.num-snapshots"));
            loaded.db().get().delete(state, marker);
            assertNull(range.readCurrentPoint(COLUMN, new byte[] {1}));
            byte[] key = {1}, value = {9};
            range.apply(new ApplyPosition(1, 0),
                    new DistributedCommand(group, 1, new byte[32],
                            new OperationId(UUID.randomUUID(), 1, 0),
                            java.util.List.of(new Mutation(
                                    Mutation.Kind.PUT, COLUMN, key, value))), new byte[0]);
            byte[] completedMarker = loaded.db().get().get(state, marker);
            assertArrayEquals(value, range.readCurrentPoint(COLUMN, key));
            loaded.db().get().delete(state, marker);
            assertThrows(IllegalStateException.class, () -> range.readCurrentPoint(COLUMN, key));
            byte[] mismatchedMarker = completedMarker.clone();
            mismatchedMarker[mismatchedMarker.length - 1]++;
            loaded.db().get().put(state, marker, mismatchedMarker);
            assertThrows(IllegalStateException.class, () -> range.readCurrentPoint(COLUMN, key));
            loaded.db().get().put(state, marker, completedMarker);
            assertArrayEquals(value, range.readCurrentPoint(COLUMN, key));
            try (var pin = range.capture()) { assertEquals(1, pool.snapshotAdmissionStats().pins()); }
            assertEquals(0, pool.snapshotAdmissionStats().pins());
        }
    }
    @Test void currentHomeAndDataPointsRemainReadableAtFullRetentionQuota() throws Exception {
        try (var pool = DistributedStoragePool.open(directory.resolve("points"), ConfigParser.parseDefault(), LoggerFactory.getLogger(getClass()))) {
            pool.configureSnapshotAdmission(ONE);
            var data = range(pool, UUID.randomUUID(), null, new byte[] {64});
            var home = range(pool, UUID.randomUUID(), new byte[] {64}, null);
            home.configureSnapshotHome(UUID.randomUUID(), UUID.randomUUID(), COLUMN);
            try (var pin = data.capture()) {
                assertThrows(RocksDBException.class, home::capture);
                assertNull(home.readCurrentPoint(COLUMN, new byte[] {65}));
                assertNull(data.readCurrentPoint(COLUMN, new byte[] {1}));
                assertEquals(1, pool.snapshotAdmissionStats().pins());
            }
            assertEquals(0, pool.snapshotAdmissionStats().pins());
            try (var pin = home.capture()) { assertEquals(1, pool.snapshotAdmissionStats().pins()); }
        }
    }

    @Test void ambiguousNativeReleaseQuarantinesPoolWithoutRetryOrRefund() throws Exception {
        var pool = DistributedStoragePool.open(directory.resolve("release-failure"), ConfigParser.parseDefault(), LoggerFactory.getLogger(getClass()));
        pool.configureSnapshotAdmission(ONE);
        var range = range(pool, UUID.randomUUID(), null, new byte[] {64});
        var other = range(pool, UUID.randomUUID(), new byte[] {64}, null);
        var pin = range.capture();
        var dbField = ReplicatedRangeStore.class.getDeclaredField("db"); dbField.setAccessible(true);
        var nativeDb = (org.rocksdb.RocksDB) dbField.get(range);
        var snapshotField = pin.getClass().getDeclaredField("snapshot"); snapshotField.setAccessible(true);
        var nativeSnapshot = (org.rocksdb.Snapshot) snapshotField.get(pin);
        var failure = new IllegalStateException("injected failure before JNI");
        var failingDb = org.mockito.Mockito.mock(org.rocksdb.RocksDB.class, org.mockito.AdditionalAnswers.delegatesTo(nativeDb));
        org.mockito.Mockito.doThrow(failure).when(failingDb).releaseSnapshot(nativeSnapshot);
        dbField.set(range, failingDb);
        try {
            assertSame(failure, assertThrows(IllegalStateException.class, pin::close));
            assertSame(failure, range.snapshotReleaseFailure());
            assertEquals(1, pool.snapshotAdmissionStats().pins());
            assertEquals(1, range.activeSnapshotPins());
            assertThrows(IllegalStateException.class, pin::close);
            assertThrows(RuntimeException.class, other::capture);
            assertThrows(RuntimeException.class, () -> other.readCurrentPoint(COLUMN, new byte[] {65}));
            assertThrows(IllegalStateException.class, () -> pool.close(Duration.ZERO));
            org.mockito.Mockito.verify(failingDb, org.mockito.Mockito.times(1)).releaseSnapshot(nativeSnapshot);
        } finally {
            // This injection proved JNI was never entered; only the fixture can establish safe manual cleanup.
            dbField.set(range, nativeDb);
            nativeDb.releaseSnapshot(nativeSnapshot);
            var leaseField = pin.getClass().getDeclaredField("admissionLease"); leaseField.setAccessible(true);
            ((AutoCloseable) leaseField.get(pin)).close();
            var countField = ReplicatedRangeStore.class.getDeclaredField("snapshotPins"); countField.setAccessible(true);
            ((java.util.concurrent.atomic.AtomicInteger) countField.get(range)).decrementAndGet();
            var failureField = ReplicatedRangeStore.class.getDeclaredField("snapshotReleaseFailure"); failureField.setAccessible(true);
            failureField.set(range, null);
            var guardField = DistributedStoragePool.class.getDeclaredField("snapshotAdmission"); guardField.setAccessible(true);
            var guard = guardField.get(pool);
            var guardFailure = guard.getClass().getDeclaredField("releaseFailure"); guardFailure.setAccessible(true);
            guardFailure.set(guard, null);
            pool.close();
        }
    }

    @Test void freeSpaceAndEstimatePressureRejectBeforeNativeCapture() throws Exception {
        for (boolean space : new boolean[] {false, true}) {
            try (var pool = DistributedStoragePool.open(directory.resolve("pressure-" + space), ConfigParser.parseDefault(), LoggerFactory.getLogger(getClass()))) {
                pool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(2, space ? 1L << 40 : 1, Duration.ofSeconds(60), space ? Long.MAX_VALUE : 0));
                var range = range(pool, UUID.randomUUID(), null, null);
                assertThrows(RocksDBException.class, range::capture);
                assertNull(range.readCurrentPoint(COLUMN, new byte[] {1}));
                assertEquals(0, range.activeSnapshotPins());
                assertEquals(0, pool.snapshotAdmissionStats().pins());
            }
        }
    }
}
