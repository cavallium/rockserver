package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.ClusterConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

class ClusterConnectionTest {

    @Test
    @SuppressWarnings("resource")
    void constructorRejectsNullInputsWithExplicitDiagnostics() throws Exception {
        try (var endpoint = new TestConnection("endpoint", CompletableFuture.completedFuture(Map.of()))) {

            assertIllegalArgument("Cluster connection requires at least one endpoint", () -> new ClusterConnection(
                    "cluster-test",
                    null,
                    0,
                    ClusterConnection.ReadPolicy.ROUND_ROBIN_REPLICAS,
                    Duration.ZERO
            ));
            assertIllegalArgument("Cluster connection requires at least one endpoint", () -> new ClusterConnection(
                    "cluster-test",
                    List.of(),
                    0,
                    ClusterConnection.ReadPolicy.ROUND_ROBIN_REPLICAS,
                    Duration.ZERO
            ));
            assertIllegalArgument("readPolicy must not be null", () -> new ClusterConnection(
                    "cluster-test",
                    List.of(endpoint),
                    0,
                    null,
                    Duration.ZERO
            ));
            assertIllegalArgument("unhealthyCooldown must not be null", () -> new ClusterConnection(
                    "cluster-test",
                    List.of(endpoint),
                    0,
                    ClusterConnection.ReadPolicy.ROUND_ROBIN_REPLICAS,
                    null
            ));
            assertIllegalArgument("connection must not be null", () -> new ClusterConnection(
                    "cluster-test",
                    Collections.singletonList(null),
                    0,
                    ClusterConnection.ReadPolicy.ROUND_ROBIN_REPLICAS,
                    Duration.ZERO
            ));
            assertIllegalArgument("primary must not be null", () -> ClusterConnection.primaryWithReplicas(
                    "cluster-test",
                    null,
                    List.of(endpoint),
                    ClusterConnection.ReadPolicy.ROUND_ROBIN_REPLICAS
            ));
            assertIllegalArgument("replicas must not be null", () -> ClusterConnection.primaryWithReplicas(
                    "cluster-test",
                    endpoint,
                    null,
                    ClusterConnection.ReadPolicy.ROUND_ROBIN_REPLICAS
            ));
            assertIllegalArgument("readPolicy must not be null", () -> ClusterConnection.primaryWithReplicas(
                    "cluster-test",
                    endpoint,
                    List.of(),
                    null
            ));
        }
    }

    @Test
    void syncRequestRejectsNullInputWithExplicitDiagnostic() throws Exception {
        var endpoint = new TestConnection("endpoint", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(endpoint)) {
            assertIllegalArgument("req must not be null", () -> connection.getSyncApi().requestSync(null));
        }

        assertEquals(0, endpoint.invocations.get());
    }

    @Test
    void cancelingReplicaReadCancelsActiveEndpointAndDoesNotFailOver() throws Exception {
        var active = new CompletableFuture<Map<String, ColumnSchema>>();
        var first = new TestConnection("first", active);
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second)) {
            var result = connection.getAsyncApi().getAllColumnDefinitionsAsync();

            assertTrue(result.cancel(true));

            assertTrue(active.isCancelled());
            assertEquals(1, first.invocations.get());
            assertEquals(0, second.invocations.get());
        }
    }

    @Test
    void cancelingReplicaReadAfterFailoverCancelsCurrentAttemptAndStopsLaterRetries() throws Exception {
        var active = new CompletableFuture<Map<String, ColumnSchema>>();
        var first = new TestConnection("first",
                CompletableFuture.failedFuture(new IllegalStateException("first failed")));
        var second = new TestConnection("second", active);
        var third = new TestConnection("third", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second, third)) {
            var result = connection.getAsyncApi().getAllColumnDefinitionsAsync();

            assertEquals(1, first.invocations.get());
            assertEquals(1, second.invocations.get());
            assertTrue(result.cancel(true));

            assertTrue(active.isCancelled());
            assertEquals(0, third.invocations.get());
        }
    }

    @Test
    void replicaReadStillFailsOverAfterEndpointFailure() throws Exception {
        var expected = Map.<String, ColumnSchema>of();
        var first = new TestConnection("first",
                CompletableFuture.failedFuture(new IllegalStateException("first failed")));
        var second = new TestConnection("second", CompletableFuture.completedFuture(expected));
        try (var connection = connection(first, second)) {
            assertSame(expected, connection.getAsyncApi().getAllColumnDefinitionsAsync().join());

            assertEquals(1, first.invocations.get());
            assertEquals(1, second.invocations.get());
        }
    }

    @Test
    void syncReplicaReadWrapsCheckedEndpointFailureWithDiagnostic() throws Exception {
        var checkedFailure = new IOException("checked sync endpoint failure");
        var endpoint = new TestConnection(
                "endpoint",
                () -> {
                    throw new CompletionException(new CompletionException(checkedFailure));
                },
                () -> CompletableFuture.completedFuture(Map.of()),
                null
        );
        try (var connection = connection(endpoint)) {
            var failure = assertThrows(CompletionException.class,
                    () -> connection.getSyncApi().getAllColumnDefinitions());

            assertEquals("cluster endpoint request failed", failure.getMessage());
            assertSame(checkedFailure, failure.getCause());
            assertEquals(1, endpoint.invocations.get());
        }
    }

    @Test
    void asyncReplicaReadWrapsCheckedEndpointFailureWithDiagnostic() throws Exception {
        var checkedFailure = new IOException("checked async endpoint failure");
        var endpoint = new TestConnection("endpoint", CompletableFuture.failedFuture(checkedFailure));
        try (var connection = connection(endpoint)) {
            var failure = assertThrows(CompletionException.class,
                    () -> connection.getAsyncApi().getAllColumnDefinitionsAsync().join());

            assertEquals("cluster endpoint request failed", failure.getMessage());
            assertSame(checkedFailure, failure.getCause());
            assertEquals(1, endpoint.invocations.get());
        }
    }

    @Test
    void syncReplicaReadDoesNotFailOverAfterSemanticEndpointFailure() throws Exception {
        var semanticFailure = RocksDBException.of(
                RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND,
                "missing column"
        );
        var first = new TestConnection(
                "first",
                () -> {
                    throw semanticFailure;
                },
                () -> CompletableFuture.completedFuture(Map.of()),
                null
        );
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second)) {
            var failure = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().getAllColumnDefinitions());

            assertSame(semanticFailure, failure);
            assertEquals(1, first.invocations.get());
            assertEquals(0, second.invocations.get());
        }
    }

    @Test
    void asyncReplicaReadDoesNotFailOverAfterSemanticEndpointFailure() throws Exception {
        var semanticFailure = RocksDBException.of(
                RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND,
                "missing column"
        );
        var first = new TestConnection("first", CompletableFuture.failedFuture(semanticFailure));
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second)) {
            var failure = assertThrows(CompletionException.class,
                    () -> connection.getAsyncApi().getAllColumnDefinitionsAsync().join());

            assertSame(semanticFailure, failure.getCause());
            assertEquals(1, first.invocations.get());
            assertEquals(0, second.invocations.get());
        }
    }

    @Test
    void syncReplicaReadPropagatesWrappedFatalEndpointFailureWithoutFailover() throws Exception {
        var fatalFailure = new AssertionError("fatal sync replica failure");
        var first = new TestConnection(
                "first",
                () -> {
                    throw new CompletionException(fatalFailure);
                },
                () -> CompletableFuture.completedFuture(Map.of()),
                null
        );
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second)) {
            var failure = assertThrows(AssertionError.class,
                    () -> connection.getSyncApi().getAllColumnDefinitions());

            assertSame(fatalFailure, failure);
            assertEquals(1, first.invocations.get());
            assertEquals(0, second.invocations.get());
        }
    }

    @Test
    void syncReplicaReadPropagatesNestedWrappedFatalEndpointFailureWithoutFailover() throws Exception {
        var fatalFailure = new AssertionError("nested fatal sync replica failure");
        var first = new TestConnection(
                "first",
                () -> {
                    throw new CompletionException(new CompletionException(fatalFailure));
                },
                () -> CompletableFuture.completedFuture(Map.of()),
                null
        );
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second)) {
            var failure = assertThrows(AssertionError.class,
                    () -> connection.getSyncApi().getAllColumnDefinitions());

            assertSame(fatalFailure, failure);
            assertEquals(1, first.invocations.get());
            assertEquals(0, second.invocations.get());
        }
    }

    @Test
    void asyncReplicaReadPropagatesFatalEndpointFailureWithoutFailover() throws Exception {
        var fatalFailure = new AssertionError("fatal async replica failure");
        var first = new TestConnection("first", CompletableFuture.failedFuture(fatalFailure));
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second)) {
            var failure = assertThrows(CompletionException.class,
                    () -> connection.getAsyncApi().getAllColumnDefinitionsAsync().join());

            assertSame(fatalFailure, failure.getCause());
            assertEquals(1, first.invocations.get());
            assertEquals(0, second.invocations.get());
        }
    }

    @Test
    void asyncReplicaReadPropagatesNestedWrappedFatalEndpointFailureWithoutFailover() throws Exception {
        var fatalFailure = new AssertionError("nested fatal async replica failure");
        var first = new TestConnection("first", CompletableFuture.failedFuture(
                new CompletionException(new CompletionException(fatalFailure))
        ));
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second)) {
            var failure = assertThrows(CompletionException.class,
                    () -> connection.getAsyncApi().getAllColumnDefinitionsAsync().join());

            assertSame(fatalFailure, failure.getCause());
            assertEquals(1, first.invocations.get());
            assertEquals(0, second.invocations.get());
        }
    }

    @Test
    void asyncReplicaReadDoesNotFailOverAfterWrappedSemanticEndpointFailure() throws Exception {
        var semanticFailure = RocksDBException.of(
                RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND,
                "missing column"
        );
        var first = new TestConnection("first", CompletableFuture.failedFuture(
                new CompletionException(new CompletionException(semanticFailure))
        ));
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second)) {
            var failure = assertThrows(CompletionException.class,
                    () -> connection.getAsyncApi().getAllColumnDefinitionsAsync().join());

            assertSame(semanticFailure, failure.getCause());
            assertEquals(1, first.invocations.get());
            assertEquals(0, second.invocations.get());
        }
    }

    @Test
    void cancelingReplicaReadCancelsLatePublishedFailoverAttempt() throws Exception {
        var firstResponse = new CompletableFuture<Map<String, ColumnSchema>>();
        var lateResponse = new CompletableFuture<Map<String, ColumnSchema>>();
        var secondStarted = new CountDownLatch(1);
        var releaseSecond = new CountDownLatch(1);
        var first = new TestConnection("first", firstResponse);
        var second = new TestConnection("second", () -> {
            secondStarted.countDown();
            await(releaseSecond);
            return lateResponse;
        });
        var third = new TestConnection("third", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second, third)) {
            var result = connection.getAsyncApi().getAllColumnDefinitionsAsync();
            var completer = new Thread(() ->
                    firstResponse.completeExceptionally(new IllegalStateException("first failed")));
            completer.start();
            assertTrue(secondStarted.await(5, TimeUnit.SECONDS));

            assertTrue(result.cancel(true));
            releaseSecond.countDown();
            completer.join(TimeUnit.SECONDS.toMillis(5));

            assertFalse(completer.isAlive());
            assertTrue(lateResponse.isCancelled());
            assertEquals(0, third.invocations.get());
        }
    }

    @Test
    void replicaReadCancellationPropagatesFatalEndpointCleanupFault() throws Exception {
        var cleanupFailure = new AssertionError("cleanup failed");
        var active = new CompletableFuture<Map<String, ColumnSchema>>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                super.cancel(mayInterruptIfRunning);
                throw cleanupFailure;
            }
        };
        var first = new TestConnection("first", active);
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()));
        try (var connection = connection(first, second)) {
            var result = connection.getAsyncApi().getAllColumnDefinitionsAsync();

            assertSame(cleanupFailure, assertThrows(AssertionError.class, () -> result.cancel(true)));

            assertTrue(result.isCancelled());
            assertTrue(active.isCancelled());
            assertEquals(0, second.invocations.get());
        }
    }

    @Test
    void closeContinuesAcrossFatalEndpointFailureAndSuppressesLaterCloseFailures() {
        var fatalFailure = new AssertionError("first close failed");
        var checkedFailure = new IOException("second close failed");
        var first = new TestConnection("first", CompletableFuture.completedFuture(Map.of()), fatalFailure);
        var second = new TestConnection("second", CompletableFuture.completedFuture(Map.of()), checkedFailure);
        var connection = connection(first, second);

        var failure = assertThrows(AssertionError.class, connection::close);

        assertSame(fatalFailure, failure);
        assertEquals(1, first.closes.get());
        assertEquals(1, second.closes.get());
        assertEquals(1, failure.getSuppressed().length);
        assertSame(checkedFailure, failure.getSuppressed()[0]);
    }

    @Test
    void closeIsIdempotentAndRejectsNewRequestsBeforeEndpointInvocation() throws Exception {
        var endpoint = new TestConnection("endpoint", CompletableFuture.completedFuture(Map.of()));
        var connection = connection(endpoint);

        connection.close();
        connection.close();

        var syncFailure = assertThrows(IllegalStateException.class,
                () -> connection.getSyncApi().getAllColumnDefinitions());
        var asyncFailure = assertThrows(CompletionException.class,
                () -> connection.getAsyncApi().getAllColumnDefinitionsAsync().join());

        assertEquals("Cluster connection is closed", syncFailure.getMessage());
        assertEquals("Cluster connection is closed", asyncFailure.getCause().getMessage());
        assertEquals(1, endpoint.closes.get());
        assertEquals(0, endpoint.invocations.get());
    }

    private static ClusterConnection connection(TestConnection... endpoints) {
        return new ClusterConnection(
                "cluster-test",
                List.of(endpoints),
                0,
                ClusterConnection.ReadPolicy.ROUND_ROBIN_REPLICAS,
                Duration.ZERO
        );
    }

    private static void assertIllegalArgument(String expectedMessage, Runnable runnable) {
        var error = assertThrows(IllegalArgumentException.class, runnable::run);
        assertTrue(error.getMessage().contains(expectedMessage));
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        }
    }

    private static final class TestConnection implements RocksDBConnection {
        private final String name;
        private final Supplier<Map<String, ColumnSchema>> syncResponseSupplier;
        private final Supplier<CompletableFuture<Map<String, ColumnSchema>>> responseSupplier;
        private final Throwable closeFailure;
        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
            @Override
            public Map<String, ColumnSchema> getAllColumnDefinitions() {
                invocations.incrementAndGet();
                return syncResponseSupplier.get();
            }
        };
        private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
            @Override
            public CompletableFuture<Map<String, ColumnSchema>> getAllColumnDefinitionsAsync() {
                invocations.incrementAndGet();
                return responseSupplier.get();
            }
        };

        private TestConnection(String name, CompletableFuture<Map<String, ColumnSchema>> response) {
            this(name, response::join, () -> response, null);
        }

        private TestConnection(String name,
                                CompletableFuture<Map<String, ColumnSchema>> response,
                                Throwable closeFailure) {
            this(name, response::join, () -> response, closeFailure);
        }

        private TestConnection(String name, Supplier<CompletableFuture<Map<String, ColumnSchema>>> responseSupplier) {
            this(name, () -> responseSupplier.get().join(), responseSupplier, null);
        }

        private TestConnection(String name,
                                Supplier<Map<String, ColumnSchema>> syncResponseSupplier,
                                Supplier<CompletableFuture<Map<String, ColumnSchema>>> responseSupplier,
                                Throwable closeFailure) {
            this.name = name;
            this.syncResponseSupplier = syncResponseSupplier;
            this.responseSupplier = responseSupplier;
            this.closeFailure = closeFailure;
        }

        @Override
        public URI getUrl() {
            return URI.create("memory://" + name);
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
        public void close() throws IOException {
            closes.incrementAndGet();
            if (closeFailure instanceof IOException ioException) {
                throw ioException;
            }
            if (closeFailure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (closeFailure instanceof Error error) {
                throw error;
            }
        }
    }
}
