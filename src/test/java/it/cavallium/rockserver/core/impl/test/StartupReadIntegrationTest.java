package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.StartupReadCompletions;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(45)
class StartupReadIntegrationTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void readsBelongToTheirGroupAndPinnedOwnershipDoesNotAddCompletions(boolean fast) throws Exception {
        Path config = directory.resolve("startup.conf");
        Files.writeString(config, """
                database.global.enable-fast-get: %s
                database.global.use-direct-io: false
                database.parallelism.workload-groups: [{name: slow, read: 4, write: 4}]
                database.global.column-options: [{name: slow-column, workload-group: slow}]
                """.formatted(fast));
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "startup-read-test", config)) {
            EmbeddedDB db = connection.getInternalDB();
            // Enable only completion instrumentation, independently of the unavailable candidate ABI.
            var enabled = EmbeddedDB.class.getDeclaredField("startupReadsEnabled");
            enabled.setAccessible(true);
            enabled.setBoolean(db, true);
            var api = connection.getSyncApi(RequestContext.batch());
            var schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
            long slow = api.createColumn("slow-column", schema);
            long hot = api.createColumn("hot-column", schema);
            var key = new Keys(Buf.wrap(new byte[] {1}));
            var missing = new Keys(Buf.wrap(new byte[] {2}));
            var value = Buf.wrap(new byte[] {42});
            api.put(0, slow, key, value, RequestType.none());
            api.put(0, hot, key, value, RequestType.none());
            var counters = counters(db);
            counters.get("slow").snapshot(1);
            counters.get("default").snapshot(1);

            assertEquals(value, api.get(0, slow, key, RequestType.current()));
            assertTrue(api.get(0, slow, key, RequestType.exists()));
            assertEquals(List.of(true, false), api.existsMulti(0, slow, List.of(key, missing)));
            var sample = counters.get("slow").snapshot(2);
            assertEquals(2, sample.point().count());
            assertEquals(1, sample.bulk().count());
            assertEquals(2, sample.bulk().keys());
            assertEquals(0, sample.point().failures() + sample.bulk().failures());
            assertSame(sample, counters.get("slow").snapshot(2));
            assertEquals(0, counters.get("default").snapshot(2).point().count());

            if (fast) {
                var pinned = db.tryGetFast(hot, key, EmbeddedDB.FastGetOutput.PINNED);
                assertNotNull(pinned);
                assertTrue(pinned.isPresent());
                var completed = counters.get("default").snapshot(3);
                assertEquals(1, completed.point().count());
                pinned.close();
                var released = counters.get("default").snapshot(4);
                assertEquals(0, released.point().count());
                assertEquals(0, released.point().nanos());
            }
            var registrations = registrations(db);
            assertEquals(2, registrations.size());
            api.deleteColumn(slow);
            assertEquals(1, registrations.size());
            api.deleteColumn(hot);
            assertEquals(0, registrations.size());
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, StartupReadCompletions> counters(EmbeddedDB db) throws Exception {
        var field = EmbeddedDB.class.getDeclaredField("startupReadGroups");
        field.setAccessible(true);
        return (Map<String, StartupReadCompletions>) field.get(db);
    }

    @Test void aUserGroupNamedSharedRetainsItsOwnSchedulerDemand() throws Exception {
        Path config = directory.resolve("shared.conf");
        Files.writeString(config, """
                database.global.use-direct-io: false
                database.parallelism.workload-groups: [{name: __shared, read: 4, write: 4}]
                database.global.column-options: [{name: user-column, workload-group: __shared}]
                """);
        try (var connection = new EmbeddedConnection(directory.resolve("shared-db"), "startup-user-shared", config)) {
            var db = connection.getInternalDB();
            var enabled = EmbeddedDB.class.getDeclaredField("startupReadsEnabled");
            enabled.setAccessible(true);
            enabled.setBoolean(db, true);
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("user-column", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            assertNull(api.get(0, column, new Keys(Buf.wrap(new byte[] {1})), RequestType.current()));
            var method = EmbeddedDB.class.getDeclaredMethod("sampleStartupIo", long.class);
            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            var samples = (Map<String, it.cavallium.rockserver.core.impl.StartupIoPolicy.InputSample>) method.invoke(db, 1L);
            var sample = samples.get("__shared");
            assertNotNull(sample);
            assertEquals(8, sample.workers(), "user group must sample its actual READ and WRITE pools");
            assertEquals(1, sample.reads().point().count());
            assertFalse(samples.containsKey(":shared"));
        }
    }

    @Test void columnRecoveryPressureDoesNotContaminateAnotherWorkload() throws Exception {
        Path config = directory.resolve("pressure.conf");
        Files.writeString(config, """
                database.global.use-direct-io: false
                database.parallelism.workload-groups: [{name: slow, read: 4, write: 4}]
                database.global.column-options: [{name: slow-column, workload-group: slow}]
                """);
        try (var connection = new EmbeddedConnection(directory.resolve("pressure-db"), "startup-pressure", config)) {
            var db = connection.getInternalDB();
            var enabled = EmbeddedDB.class.getDeclaredField("startupReadsEnabled");
            enabled.setAccessible(true);
            enabled.setBoolean(db, true);
            var api = connection.getSyncApi(RequestContext.batch());
            var schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
            long slow = api.createColumn("slow-column", schema);
            api.createColumn("hot-column", schema);
            var field = EmbeddedDB.class.getDeclaredField("storagePressureColumns");
            field.setAccessible(true);
            Object[] columns = ((Object[]) field.get(db)).clone();
            var type = columns.getClass().componentType();
            var components = type.getRecordComponents();
            var parameters = java.util.Arrays.stream(components).map(java.lang.reflect.RecordComponent::getType).toArray(Class<?>[]::new);
            var constructor = type.getDeclaredConstructor(parameters);
            constructor.setAccessible(true);
            for (int i = 0; i < columns.length; i++) {
                Object[] values = new Object[components.length];
                for (int j = 0; j < values.length; j++) {
                    var accessor = components[j].getAccessor();
                    accessor.setAccessible(true);
                    values[j] = accessor.invoke(columns[i]);
                }
                if (((Long) values[0]) == slow) {
                    // Inject this column's recovery threshold while retaining real native properties and leases.
                    for (int j = 0; j < components.length; j++) {
                        if (components[j].getName().equals("effectiveLevel0SlowdownWritesTrigger")) values[j] = 0;
                    }
                    columns[i] = constructor.newInstance(values);
                }
            }
            field.set(db, columns);
            var method = EmbeddedDB.class.getDeclaredMethod("sampleStartupIo", long.class);
            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            var samples = (Map<String, it.cavallium.rockserver.core.impl.StartupIoPolicy.InputSample>) method.invoke(db, 1L);
            assertTrue(samples.get("slow").urgentRecovery());
            assertFalse(samples.get("default").urgentRecovery());
            assertTrue(samples.get("default").complete());
            // Model a sampler's old array after retirement removed the mutable name mapping.
            var namesField = EmbeddedDB.class.getDeclaredField("columnWorkloadNames");
            namesField.setAccessible(true);
            @SuppressWarnings("unchecked")
            var names = (Map<Long, String>) namesField.get(db);
            names.remove(slow);
            var columnsField = EmbeddedDB.class.getDeclaredField("columns");
            columnsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            var registered = (NonBlockingHashMapLong<it.cavallium.rockserver.core.impl.ColumnInstance>) columnsField.get(db);
            registered.get(slow).uses().closeAdmission();
            @SuppressWarnings("unchecked")
            var retiring = (Map<String, it.cavallium.rockserver.core.impl.StartupIoPolicy.InputSample>) method.invoke(db, 2L);
            assertFalse(retiring.get("slow").complete());
            assertTrue(retiring.get("default").complete(), "retired identity must not fall into another storage workload");
        }
    }

    @SuppressWarnings("unchecked")
    private static NonBlockingHashMapLong<StartupReadCompletions> registrations(EmbeddedDB db) throws Exception {
        var field = EmbeddedDB.class.getDeclaredField("startupReadsByHandle");
        field.setAccessible(true);
        return (NonBlockingHashMapLong<StartupReadCompletions>) field.get(db);
    }
}
