package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.StartupWriteCompletions;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

class StartupWriteIntegrationTest {
    @TempDir Path directory;
    private static Object field(Object owner, String name) throws Exception {
        var f = owner.getClass().getDeclaredField(name); f.setAccessible(true); return f.get(owner);
    }
    @SuppressWarnings("unchecked") private static Map<String, StartupWriteCompletions> counters(EmbeddedDB db) throws Exception {
        return (Map<String, StartupWriteCompletions>) field(db, "startupWriteGroups");
    }
    @Test void nativePutBatchCommitIngestAndRollbackRespectScopeAndStaging() throws Exception {
        Path config = Files.writeString(directory.resolve("config.conf"), """
                database.global.use-direct-io: false
                database.global.optimistic: true
                database.parallelism.workload-groups: [{name: slow, read: 4, write: 4}]
                database.global.column-options: [{name: slow-column, workload-group: slow}]
                """);
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "startup-writes", config)) {
            var db = connection.getInternalDB();
            // Isolated real native-call hooks; this does not establish enabled configuration/controller publication.
            var enabled = EmbeddedDB.class.getDeclaredField("startupReadsEnabled"); enabled.setAccessible(true); enabled.setBoolean(db, true);
            var api = connection.getSyncApi(RequestContext.batch());
            var schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
            long slow = api.createColumn("slow-column", schema), hot = api.createColumn("hot-column", schema);
            var key = new Keys(Buf.wrap(new byte[]{1})); var value = Buf.wrap(new byte[]{7});
            var c = counters(db); c.get("slow").snapshot(1); c.get("default").snapshot(1); c.get(":shared").snapshot(1);
            api.put(0, slow, key, value, RequestType.none());
            assertEquals(1, c.get("slow").snapshot(2).calls().count());
            assertEquals(0, c.get("default").snapshot(2).calls().count());
            api.putBatch(slow, Flux.just(new KVBatch.KVBatchRef(List.of(key), List.of(value))), PutBatchMode.WRITE_BATCH);
            assertEquals(1, c.get("slow").snapshot(3).calls().count());
            long tx = api.openTransaction(Duration.ofSeconds(10));
            api.put(tx, slow, key, value, RequestType.none());
            assertEquals(0, c.get("slow").snapshot(4).calls().count(), "transaction staging is not DB completion");
            assertTrue(api.closeTransaction(tx, true));
            assertEquals(1, c.get("slow").snapshot(5).calls().count());
            long rollback = api.openTransaction(Duration.ofSeconds(10));
            api.put(rollback, slow, key, value, RequestType.none());
            assertTrue(api.closeTransaction(rollback, false));
            assertEquals(0, c.get("slow").snapshot(6).calls().count());
            assertTrue(((Map<?, ?>) field(db, "startupTransactionWriteGroups")).isEmpty());
            long mixed = api.openTransaction(Duration.ofSeconds(10));
            api.put(mixed, slow, key, value, RequestType.none()); api.put(mixed, hot, key, value, RequestType.none());
            assertTrue(api.closeTransaction(mixed, true));
            assertEquals(1, c.get(":shared").snapshot(7).calls().count());
            assertEquals(0, c.get("slow").snapshot(7).calls().count());
            try (var sst = db.getSSTWriter(slow, null, false, false)) {
                sst.put(new byte[]{2}, new byte[]{8});
                assertEquals(0, c.get("slow").snapshot(8).calls().count(), "external SST serialization is not DB completion");
                sst.writePending();
            }
            assertEquals(1, c.get("slow").snapshot(9).calls().count());
            long failed = api.openTransaction(Duration.ofSeconds(10));
            api.get(failed, slow, key, RequestType.forUpdate());
            api.put(failed, slow, new Keys(Buf.wrap(new byte[]{3})), value, RequestType.none());
            api.put(0, slow, key, Buf.wrap(new byte[]{9}), RequestType.none());
            c.get("slow").snapshot(10);
            assertFalse(api.closeTransaction(failed, true));
            assertEquals(1, c.get("slow").snapshot(11).calls().failures());
            assertFalse(((Map<?, ?>) field(db, "startupTransactionWriteGroups")).isEmpty(), "retry keeps its resource/owner tag");
            assertTrue(api.closeTransaction(failed, false));
            assertTrue(((Map<?, ?>) field(db, "startupTransactionWriteGroups")).isEmpty());
            long active = c.get("slow").begin();
            try {
                var sampler = EmbeddedDB.class.getDeclaredMethod("sampleStartupIo", long.class);
                sampler.setAccessible(true);
                @SuppressWarnings("unchecked")
                var assembled = (Map<String, it.cavallium.rockserver.core.impl.StartupIoPolicy.InputSample>) sampler.invoke(db, 12L);
                assertEquals(1, assembled.get("slow").writes().active());
                assertEquals(0, assembled.get("default").writes().active());
                var policy = new it.cavallium.rockserver.core.impl.StartupIoPolicy();
                var sample = assembled.get("slow");
                assertFalse(policy.sample(sample, System.nanoTime()).qualifiedHealthy());
            } finally {
                c.get("slow").complete(active, false);
            }
            db.cdcCreate("startup-feedback", null, List.of(slow), false, java.util.OptionalLong.empty());
            assertEquals(1, c.get(":shared").snapshot(13).calls().count());
            db.cdcDelete("startup-feedback");
            var deleted = c.get(":shared").snapshot(14);
            assertEquals(1, deleted.calls().count(), "CDC metadata deletion is shared native write completion");
            assertEquals(0, deleted.calls().failures());
            assertEquals(0, deleted.active());
        }
    }
}
