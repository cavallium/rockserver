package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.server.ThriftServer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

class ReplicatedThriftBoundaryTest {

    @Test
    void thriftClientReceivesNotImplementedForReplicatedLocalHandles() throws Exception {
        var logClient = new RecordingLogClient();
        var connection = new ReplicatedTestConnection(new LegacyReplicatedWriteSyncFixture(logClient));
        var port = freePort();

        try (var server = new ThriftServer(connection, "127.0.0.1", port)) {
            server.start();
            try (var client = connect(port)) {
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

    @Test
    void canceledQueuedAsyncTaskDoesNotRunOnThriftClientWorker() throws Exception {
        var firstStarted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var secondStarted = new CountDownLatch(1);
        var connection = new ReplicatedTestConnection(new RocksDBSyncAPI() {
        });
        var port = freePort();

        try (var server = new ThriftServer(connection, "127.0.0.1", port)) {
            server.start();
            try (var client = connect(port)) {
                var first = submitAsync(client, () -> {
                    firstStarted.countDown();
                    await(releaseFirst);
                });
                assertTrue(firstStarted.await(5, TimeUnit.SECONDS));

                var second = submitAsync(client, secondStarted::countDown);
                assertFalse(second.isDone());
                assertTrue(second.cancel(true));
                releaseFirst.countDown();

                first.get(5, TimeUnit.SECONDS);
                assertFalse(secondStarted.await(200, TimeUnit.MILLISECONDS));
            } finally {
                releaseFirst.countDown();
            }
        }
    }

    @Test
    void cancelingRunningAsyncBatchInterruptsPublisherBeforeRemoteWrite() throws Exception {
        var publisherStarted = new CountDownLatch(1);
        var publisherInterrupted = new CountDownLatch(1);
        var releasePublisher = new CountDownLatch(1);
        var remoteWrites = new AtomicInteger();
        var connection = new ReplicatedTestConnection(new RocksDBSyncAPI() {
            @Override
            public void putBatch(long columnId, Publisher<KVBatch> batchPublisher, PutBatchMode mode) {
                remoteWrites.incrementAndGet();
            }
        });
        var port = freePort();

        try (var server = new ThriftServer(connection, "127.0.0.1", port)) {
            server.start();
            try (var client = connect(port)) {
                var pending = client.getAsyncApi().putBatchAsync(4, blockingPublisher(
                        publisherStarted,
                        publisherInterrupted,
                        releasePublisher
                ), PutBatchMode.WRITE_BATCH);
                assertTrue(publisherStarted.await(5, TimeUnit.SECONDS));

                assertTrue(pending.cancel(true));
                assertTrue(publisherInterrupted.await(5, TimeUnit.SECONDS));
                assertEquals(0, remoteWrites.get());
            } finally {
                releasePublisher.countDown();
            }
        }
    }

    private static ThriftConnection connect(int port) throws Exception {
        TException last = null;
        for (int attempt = 0; attempt < 50; attempt++) {
            try {
                return new ThriftConnection("replicated-thrift-boundary", "127.0.0.1", port);
            } catch (TException ex) {
                last = ex;
                Thread.sleep(20);
            }
        }
        throw last;
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
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

    private static Publisher<KVBatch> blockingPublisher(CountDownLatch started,
                                                         CountDownLatch interrupted,
                                                         CountDownLatch release) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                started.countDown();
                try {
                    release.await();
                    subscriber.onComplete();
                } catch (InterruptedException e) {
                    interrupted.countDown();
                    subscriber.onError(e);
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void cancel() {
                release.countDown();
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<Void> submitAsync(ThriftConnection connection, Runnable runnable) {
        try {
            var method = ThriftConnection.class.getDeclaredMethod("submitAsync", Runnable.class);
            method.setAccessible(true);
            return (CompletableFuture<Void>) method.invoke(connection, runnable);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (e.getCause() instanceof Error error) {
                throw error;
            }
            throw new AssertionError(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
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
            return URI.create("memory://replicated-thrift-boundary");
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
