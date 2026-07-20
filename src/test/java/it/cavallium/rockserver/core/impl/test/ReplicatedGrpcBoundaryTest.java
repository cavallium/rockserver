package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class ReplicatedGrpcBoundaryTest {

    @Test
    void grpcClientReceivesNotImplementedForReplicatedLocalHandles() throws Exception {
        var logClient = new RecordingLogClient();
        var connection = new ReplicatedTestConnection(new LegacyReplicatedWriteSyncFixture(logClient));

        try (var server = new GrpcServer(connection, new InetSocketAddress("127.0.0.1", 0))) {
            server.start();
            try (var client = GrpcConnection.forHostAndPort("replicated-grpc-boundary",
                    new HostAndPort("127.0.0.1", server.getPort()))) {
                var api = client.getSyncApi();

                assertNotImplemented(() -> api.openTransaction(1000));
                assertNotImplemented(() -> api.closeTransaction(99, true));
                assertNotImplemented(() -> api.closeFailedUpdate(99));
                assertNotImplemented(() -> api.put(99, 4, new Keys(buf("key")), buf("value"), RequestType.none()));
                assertNotImplemented(() -> api.delete(99, 4, new Keys(buf("key")), RequestType.none()));
                assertNotImplemented(() -> api.deleteMulti(99, 4, List.of(new Keys(buf("key"))), RequestType.none()));
                assertNotImplemented(() -> api.get(0, 4, new Keys(buf("key")), RequestType.forUpdate()));
                assertNotImplemented(() -> api.openIterator(0, 4, new Keys(buf("start")), null, false, 1000));
                assertNotImplemented(() -> api.closeIterator(99));
                assertNotImplemented(() -> api.seekTo(99, new Keys(buf("seek"))));
                assertNotImplemented(() -> api.subsequent(99, 0, 1, RequestType.none()));
                assertNotImplemented(() -> api.put(0, 4, new Keys(buf("key")), buf("value"), RequestType.previous()));
                assertNotImplemented(() -> api.delete(0, 4, new Keys(buf("key")), RequestType.previous()));
                assertNotImplemented(() -> api.deleteMulti(0, 4, List.of(new Keys(buf("key"))), RequestType.previousPresence()));
                assertNotImplemented(() -> api.putMulti(0, 4, List.of(new Keys(buf("key"))), List.of(buf("value")), RequestType.previous()));
                assertNotImplemented(() -> api.merge(0, 4, new Keys(buf("key")), buf("delta"), RequestType.merged()));
                assertNotImplemented(() -> api.mergeMulti(0, 4, List.of(new Keys(buf("key"))), List.of(buf("delta")), RequestType.merged()));

                var async = client.getAsyncApi();
                assertAsyncNotImplemented(async.deleteAsync(99, 4, new Keys(buf("key")), RequestType.none()));
                assertAsyncNotImplemented(async.deleteMultiAsync(99, 4, List.of(new Keys(buf("key"))), RequestType.none()));
                assertAsyncNotImplemented(async.putAsync(0, 4, new Keys(buf("key")), buf("value"), RequestType.previous()));
                assertAsyncNotImplemented(async.deleteAsync(0, 4, new Keys(buf("key")), RequestType.previous()));
                assertAsyncNotImplemented(async.deleteMultiAsync(0, 4, List.of(new Keys(buf("key"))), RequestType.previousPresence()));
                assertAsyncNotImplemented(async.putMultiAsync(0, 4, List.of(new Keys(buf("key"))), List.of(buf("value")), RequestType.previous()));
                assertAsyncNotImplemented(async.mergeAsync(0, 4, new Keys(buf("key")), buf("delta"), RequestType.merged()));
                assertAsyncNotImplemented(async.mergeMultiAsync(0, 4, List.of(new Keys(buf("key"))), List.of(buf("delta")), RequestType.merged()));
            }
        }

        assertEquals(0, logClient.payloads.size());
    }

    private static void assertNotImplemented(Executable executable) {
        var ex = assertThrows(RocksDBException.class, executable);
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, ex.getErrorUniqueId());
    }

    private static void assertAsyncNotImplemented(CompletableFuture<?> future) {
        var ex = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                ((RocksDBException) ex.getCause()).getErrorUniqueId());
    }

    private static Buf buf(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static final class ReplicatedTestConnection implements RocksDBConnection {

        private final RocksDBSyncAPI syncApi;
        private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
        };

        private ReplicatedTestConnection(RocksDBSyncAPI syncApi) {
            this.syncApi = syncApi;
        }

        @Override
        public URI getUrl() {
            return URI.create("memory://replicated-grpc-boundary");
        }

        @Override
        public RocksDBSyncAPI getSyncApi() {
            return syncApi;
        }

        @Override
        public RocksDBAsyncAPI getAsyncApi() {
            return asyncApi;
        }

        @Override
        public void close() {
        }
    }

    private static final class RecordingLogClient implements ReplicatedLogClient {

        private final List<byte[]> payloads = new ArrayList<>();

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            payloads.add(payload.clone());
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(payloads.size(), true));
        }
    }
}
