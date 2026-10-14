package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.StartupReadCompletions;
import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.ReadTier;

@EnabledIfSystemProperty(named = "rockserver.startup.native.tests", matches = "true")
class StartupNativeReadJfrTest {
    @TempDir Path directory;
    private static final Buf VALUE = Buf.wrap(new byte[]{7});
    private static Keys key(int value) { return new Keys(Buf.wrap(new byte[]{(byte) value})); }
    private static Object field(Object owner, String name) throws Exception {
        var field = owner.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(owner);
    }
    private static ColumnInstance column(EmbeddedDB db, long id) throws Exception {
        var method = EmbeddedDB.class.getDeclaredMethod("getColumn", long.class); method.setAccessible(true);
        return (ColumnInstance) method.invoke(db, id);
    }
    private Path config(boolean startup) throws Exception {
        return Files.writeString(directory.resolve("config-" + startup + ".conf"), """
                database.global.use-direct-io: false
                database.global.enable-fast-get: true
                database.global.maximum-open-files: -1
                database.global.open-files-async: true
                database.global.startup-io: {enabled: %s, default-storage-id: jfr_native_read,
                    storage-groups: [{id: jfr_native_read, maximum-opens: 1}]}
                """.formatted(startup));
    }
    private List<RecordedEvent> events(Recording recording, String name) throws Exception {
        recording.stop(); Path path = directory.resolve(name + ".jfr"); recording.dump(path);
        return RecordingFile.readAllEvents(path).stream().filter(e -> e.getEventType().getName().equals("rockserver.NativeRead")).toList();
    }
    private void readWorkflow(EmbeddedConnection connection, long id) {
        var api = connection.getAsyncApi(RequestContext.batch());
        assertEquals(VALUE, api.getAsync(0, id, key(1), RequestType.current()).join());
        assertNull(api.getAsync(0, id, key(2), RequestType.current()).join());
        assertEquals(List.of(true, false, true), api.existsMultiAsync(0, id, List.of(key(1), key(2), key(1))).join());
    }
    @Test void disabledRecordingIsEmptyAndEnabledEventsWorkWithStartupFeaturesOff() throws Exception {
        try (var connection = new EmbeddedConnection(directory.resolve("off-db"), "native-read-off", config(false))) {
            var api = connection.getSyncApi(RequestContext.batch());
            long id = api.createColumn("column", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            api.put(0, id, key(1), VALUE, RequestType.none());
            readWorkflow(connection, id); // No active recording is required.
            try (var recording = new Recording()) {
                recording.start(); readWorkflow(connection, id);
                assertTrue(events(recording, "disabled").isEmpty());
            }
            try (var recording = new Recording()) {
                recording.enable("rockserver.NativeRead").withThreshold(Duration.ZERO); recording.start();
                readWorkflow(connection, id);
                var events = events(recording, "feature-off"); assertEquals(3, events.size());
                assertEquals(2, events.stream().filter(e -> e.getString("kind").equals("point") && e.getInt("keyCount") == 1).count());
                assertEquals(1, events.stream().filter(e -> e.getString("kind").equals("bulk") && e.getInt("keyCount") == 3).count());
                assertTrue(events.stream().noneMatch(e -> e.getBoolean("failed")), "NotFound is a successful native read outcome");
                assertTrue(events.stream().allMatch(e -> e.getStackTrace() == null));
            }
        }
    }
    private EmbeddedConnection openColdDatabase() throws Exception {
        Path path = directory.resolve("persistent-db"), config = config(true);
        try (var connection = new EmbeddedConnection(path, "native-read-seed", config)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long id = api.createColumn("column", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            api.put(0, id, key(1), VALUE, RequestType.none());
            var db = connection.getInternalDB();
            try (var flush = new FlushOptions().setWaitForFlush(true)) {
                ((TransactionalDB) field(db, "db")).get().flush(flush, List.of(column(db, id).cfh()));
            }
        }
        var connection = new EmbeddedConnection(path, "native-read-reopen", config);
        try {
            ((AutoCloseable) field(connection.getInternalDB(), "startupIoController")).close();
            return connection;
        } catch (Exception | Error failure) {
            connection.close(); throw failure;
        }
    }
    @Test void realBulkStatusFailureAndSuccessesConserveExactlyOneCompletionEach() throws Exception {
        try (var connection = openColdDatabase()) {
            var db = connection.getInternalDB();
            long id = connection.getSyncApi(RequestContext.batch()).getColumnId("column");
            var col = column(db, id);
            @SuppressWarnings("unchecked")
            var groups = (Map<String, StartupReadCompletions>) field(db, "startupReadGroups");
            var reads = groups.get("default"); reads.snapshot(1_000_000);
            try (var recording = new Recording(); var options = new ReadOptions().setReadTier(ReadTier.BLOCK_CACHE_TIER)) {
                recording.enable("rockserver.NativeRead").withThreshold(Duration.ZERO); recording.start();
                var bulk = EmbeddedDB.class.getDeclaredMethod("existsMultiStatusOnly", ColumnInstance.class, ReadOptions.class, List.class, boolean.class);
                bulk.setAccessible(true);
                var bulkFailure = assertThrows(InvocationTargetException.class,
                        () -> bulk.invoke(db, col, options, List.of(key(1).keys()[0]), false));
                assertInstanceOf(RocksDBException.class, bulkFailure.getCause());
                readWorkflow(connection, id);
                var events = events(recording, "conservation"); assertEquals(4, events.size());
                assertEquals(1, events.stream().filter(e -> e.getBoolean("failed")).count());
                var cohort = reads.snapshot(1_000_001);
                assertEquals(2, cohort.point().count()); assertEquals(0, cohort.point().failures());
                assertEquals(2, cohort.bulk().count()); assertEquals(4, cohort.bulk().keys()); assertEquals(1, cohort.bulk().failures());
                assertEquals(cohort.point().count(), events.stream().filter(e -> e.getString("kind").equals("point")).count());
                assertEquals(cohort.bulk().count(), events.stream().filter(e -> e.getString("kind").equals("bulk")).count());
                assertSame(cohort, reads.snapshot(1_000_001));
                assertEquals(0, reads.snapshot(1_000_002).point().count());
                assertEquals(0, reads.snapshot(1_000_002).bulk().count());
            }
        }
    }
    /** Native18 abort reproducer; all JNI errors must propagate as Java exceptions, not escape C++. */
    @Test void realFastPointCacheOnlyFailurePropagatesAndConservesCompletion() throws Exception {
        try (var connection = openColdDatabase(); var recording = new Recording();
             var options = new ReadOptions().setReadTier(ReadTier.BLOCK_CACHE_TIER)) {
            var db = connection.getInternalDB();
            long id = connection.getSyncApi(RequestContext.batch()).getColumnId("column");
            var col = column(db, id);
            @SuppressWarnings("unchecked")
            var groups = (Map<String, StartupReadCompletions>) field(db, "startupReadGroups");
            var reads = groups.get("default"); reads.snapshot(1_000_000);
            recording.enable("rockserver.NativeRead").withThreshold(Duration.ZERO); recording.start();
            var point = EmbeddedDB.class.getDeclaredMethod("dbGetFast", org.rocksdb.ColumnFamilyHandle.class, ReadOptions.class, Buf.class, boolean.class);
            point.setAccessible(true);
            var failure = assertThrows(InvocationTargetException.class,
                    () -> point.invoke(db, col.cfh(), options, key(1).keys()[0], false));
            assertInstanceOf(org.rocksdb.RocksDBException.class, failure.getCause());
            var events = events(recording, "point-failure"); assertEquals(1, events.size());
            assertTrue(events.getFirst().getBoolean("failed")); assertEquals("point", events.getFirst().getString("kind"));
            var cohort = reads.snapshot(1_000_001);
            assertEquals(1, cohort.point().count()); assertEquals(1, cohort.point().failures());
            assertEquals(0, cohort.bulk().count());
        }
    }

}
