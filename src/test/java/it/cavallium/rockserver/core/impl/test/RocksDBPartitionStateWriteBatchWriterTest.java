package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeState;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeStateCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionStateMetadataCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionStateWrite;
import it.cavallium.rockserver.core.cluster.command.PartitionStateWriteBatch;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseCodec;
import it.cavallium.rockserver.core.cluster.command.RocksDBPartitionStateWriteBatchWriter;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class RocksDBPartitionStateWriteBatchWriterTest {

    @BeforeAll
    static void loadRocksDb() {
        RocksDBLoader.loadLibrary();
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics(@TempDir Path directory) throws Exception {
        assertIllegalArgument("db must not be null",
                () -> new RocksDBPartitionStateWriteBatchWriter(null));

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            assertIllegalArgument("failureInjector must not be null",
                    () -> new RocksDBPartitionStateWriteBatchWriter(db, null));
            assertIllegalArgument("batch must not be null",
                    () -> new RocksDBPartitionStateWriteBatchWriter(db).write(null));
        }
    }

    @Test
    void writesUserMutationsDedupeAndLastAppliedInOneBatch(@TempDir Path directory) throws Exception {
        var userKey = text("user-key");
        var deletedKey = text("deleted-key");
        var requestId = new ClientRequestId(new ClientId(new UUID(0, 1)), 7);
        var batch = new PartitionStateWriteBatch(
                List.of(
                        PartitionStateWrite.put(userKey, text("value")),
                        PartitionStateWrite.delete(deletedKey)
                ),
                RockserverStorageKeyspace.dedupeResponse(requestId),
                PartitionWriteResponseCodec.encodeResponses(List.of(text("response"))),
                RockserverStorageKeyspace.dedupeClientState(requestId.clientId()),
                ClientDedupeStateCodec.encode(ClientDedupeState.acknowledged(requestId)),
                RockserverStorageKeyspace.lastAppliedIndex(),
                PartitionStateMetadataCodec.encodeLastAppliedIndex(15),
                15
        );

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(deletedKey.toByteArray(), text("stale").toByteArray());

            new RocksDBPartitionStateWriteBatchWriter(db).write(batch);

            assertArrayEquals(text("value").toByteArray(), db.get(userKey.toByteArray()));
            assertNull(db.get(deletedKey.toByteArray()));
            var decodedResponses = PartitionWriteResponseCodec.decodeResponses(
                    db.get(RockserverStorageKeyspace.dedupeResponse(requestId).toByteArray()));
            assertEquals(1, decodedResponses.size());
            assertArrayEquals(text("response").toByteArray(), ((Buf) decodedResponses.getFirst()).toByteArray());
            assertEquals(ClientDedupeState.acknowledged(requestId), ClientDedupeStateCodec.decode(
                    db.get(RockserverStorageKeyspace.dedupeClientState(requestId.clientId()).toByteArray())));
            assertEquals(15, PartitionStateMetadataCodec.decodeLastAppliedIndex(
                    db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray())));
        }
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }

    private static Buf text(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
