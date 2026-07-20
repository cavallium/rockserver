package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class ThriftConnectionLifecycleTest {

    @Test
    void unsupportedMergeOperatorChecksReturnPublicNotImplementedErrors() throws Exception {
        try (var connection = newConnection(new TrackingTransport(), new TerminatingExecutor())) {
            var syncError = assertThrows(RocksDBException.class,
                    () -> connection.checkMergeOperator("merge", new byte[]{1}));
            assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, syncError.getErrorUniqueId());
            assertTrue(syncError.getMessage().contains(
                    "checkMergeOperator is not exposed by the legacy Thrift transport"));

            var asyncFailure = assertThrows(ExecutionException.class,
                    () -> connection.checkMergeOperatorAsync("merge", new byte[]{1})
                            .get(5, TimeUnit.SECONDS));
            var asyncError = assertInstanceOf(RocksDBException.class, asyncFailure.getCause());
            assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, asyncError.getErrorUniqueId());
            assertTrue(asyncError.getMessage().contains(
                    "checkMergeOperator is not exposed by the legacy Thrift transport"));
        }
    }

    @Test
    void unsupportedRequestTypesReturnPublicNotImplementedErrors() throws Exception {
        try (var connection = newConnection(new TrackingTransport(), new TerminatingExecutor())) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.get(0, 4, new Keys(Buf.wrap(new byte[]{1})), RequestType.none()));

            assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("get request type"));
            assertTrue(error.getMessage().contains("is not exposed by the legacy Thrift transport"));
        }
    }

    @Test
    void closeStillShutsDownAsyncExecutorWhenTransportCloseFailsFatally() throws Exception {
        var transportFailure = new AssertionError("transport close failed");
        var executorFailure = new IllegalStateException("executor shutdown failed");
        var transport = new CloseFailingTransport(transportFailure);
        var executor = new CloseFailingExecutor(executorFailure);
        var connection = newConnection(transport, executor);

        var failure = assertThrows(AssertionError.class, connection::close);

        assertSame(transportFailure, failure);
        assertEquals(1, transport.closeCalls.get());
        assertEquals(1, executor.shutdownNowCalls.get());
        assertEquals(1, failure.getSuppressed().length);
        assertSame(executorFailure, failure.getSuppressed()[0]);
    }

    @Test
    void closeReportsUnterminatedAsyncExecutorAfterShutdown() throws Exception {
        var transport = new TrackingTransport();
        var executor = new NonTerminatingExecutor();
        var connection = newConnection(transport, executor);

        var failure = assertThrows(IOException.class, connection::close);

        assertEquals("Thrift client async requests did not stop within 5 seconds", failure.getMessage());
        assertEquals(1, transport.closeCalls.get());
        assertEquals(1, executor.shutdownNowCalls.get());
        assertEquals(1, executor.awaitTerminationCalls.get());
    }

    @Test
    void closeWaitsAgainAndRestoresInterruptWhenAsyncExecutorAwaitIsInterrupted() throws Exception {
        var transport = new TrackingTransport();
        var executor = new InterruptingAwaitExecutor();
        var connection = newConnection(transport, executor);

        try {
            connection.close();

            assertTrue(Thread.interrupted());
            assertEquals(1, transport.closeCalls.get());
            assertEquals(1, executor.shutdownNowCalls.get());
            assertEquals(2, executor.awaitTerminationCalls.get());
        } finally {
            clearInterruptedStatus();
        }
    }

    @Test
    void closeIsIdempotentAndRejectsNewRequestsBeforeTransportOrExecutorUse() throws Exception {
        var transport = new TrackingTransport();
        var executor = new TerminatingExecutor();
        var connection = newConnection(transport, executor);

        connection.close();
        connection.close();

        var syncError = assertThrows(IllegalStateException.class, connection::getAllColumnDefinitions);
        var unsupportedSyncError = assertThrows(IllegalStateException.class,
                () -> connection.checkMergeOperator("merge", new byte[]{1}));
        var asyncFailure = assertThrows(ExecutionException.class,
                () -> connection.getAllColumnDefinitionsAsync().get(5, TimeUnit.SECONDS));
        var unsupportedAsyncFailure = assertThrows(ExecutionException.class,
                () -> connection.checkMergeOperatorAsync("merge", new byte[]{1}).get(5, TimeUnit.SECONDS));
        var streamError = assertThrows(IllegalStateException.class,
                () -> Flux.from(connection.getRangeAsync(
                        0,
                        4,
                        null,
                        null,
                        false,
                        RequestType.allInRange(),
                        1000
                )).blockLast());

        assertEquals("Thrift connection is closed", syncError.getMessage());
        assertEquals("Thrift connection is closed", unsupportedSyncError.getMessage());
        assertEquals("Thrift connection is closed", asyncFailure.getCause().getMessage());
        assertEquals("Thrift connection is closed", unsupportedAsyncFailure.getCause().getMessage());
        assertEquals("Thrift connection is closed", streamError.getMessage());
        assertEquals(1, transport.closeCalls.get());
        assertEquals(1, executor.shutdownNowCalls.get());
        assertEquals(1, executor.awaitTerminationCalls.get());
    }

    @Test
    void getRangeAsyncMapsExecutorRejectionToSaturationFailure() throws Exception {
        try (var connection = newConnection(new TrackingTransport(), new RejectingExecutor())) {
            var failure = assertThrows(IllegalStateException.class,
                    () -> Flux.from(connection.getRangeAsync(
                            0,
                            4,
                            null,
                            null,
                            false,
                            RequestType.allInRange(),
                            1000
                    )).blockLast());

            assertEquals("Thrift client async request executor is saturated", failure.getMessage());
            assertInstanceOf(RejectedExecutionException.class, failure.getCause());
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptedStatus() {
        Thread.interrupted();
    }

    private static ThriftConnection newConnection(TTransport transport, ExecutorService executor) throws Exception {
        Constructor<ThriftConnection> constructor = ThriftConnection.class.getDeclaredConstructor(
                String.class,
                URI.class,
                TTransport.class,
                ExecutorService.class
        );
        constructor.setAccessible(true);
        return constructor.newInstance(
                "thrift-lifecycle-test",
                URI.create("memory://thrift-lifecycle-test"),
                transport,
                executor
        );
    }

    private static class TrackingTransport extends TTransport {

        private final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void open() {
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws TTransportException {
            throw new TTransportException("read is not used by this test");
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws TTransportException {
            throw new TTransportException("write is not used by this test");
        }

        @Override
        public TConfiguration getConfiguration() {
            return TConfiguration.DEFAULT;
        }

        @Override
        public void updateKnownMessageSize(long size) {
        }

        @Override
        public void checkReadBytesAvailable(long numBytes) {
        }
    }

    private static final class CloseFailingTransport extends TTransport {

        private final Error closeFailure;
        private final AtomicInteger closeCalls = new AtomicInteger();

        private CloseFailingTransport(Error closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void open() {
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            throw closeFailure;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws TTransportException {
            throw new TTransportException("read is not used by this test");
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws TTransportException {
            throw new TTransportException("write is not used by this test");
        }

        @Override
        public TConfiguration getConfiguration() {
            return TConfiguration.DEFAULT;
        }

        @Override
        public void updateKnownMessageSize(long size) {
        }

        @Override
        public void checkReadBytesAvailable(long numBytes) {
        }
    }

    private abstract static class TrackingExecutor extends AbstractExecutorService {

        protected final AtomicInteger shutdownNowCalls = new AtomicInteger();
        protected final AtomicInteger awaitTerminationCalls = new AtomicInteger();
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public @NotNull List<Runnable> shutdownNow() {
            shutdownNowCalls.incrementAndGet();
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public void execute(@NotNull Runnable command) {
            throw new IllegalStateException("executor is closed");
        }
    }

    private static final class TerminatingExecutor extends TrackingExecutor {

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) {
            awaitTerminationCalls.incrementAndGet();
            return true;
        }
    }

    private static final class CloseFailingExecutor extends TrackingExecutor {

        private final RuntimeException closeFailure;

        private CloseFailingExecutor(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public @NotNull List<Runnable> shutdownNow() {
            super.shutdownNow();
            throw closeFailure;
        }

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) {
            awaitTerminationCalls.incrementAndGet();
            return true;
        }
    }

    private static final class NonTerminatingExecutor extends TrackingExecutor {

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) {
            awaitTerminationCalls.incrementAndGet();
            return false;
        }
    }

    private static final class RejectingExecutor extends TrackingExecutor {

        @Override
        public void execute(@NotNull Runnable command) {
            throw new RejectedExecutionException("executor saturated");
        }

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) {
            awaitTerminationCalls.incrementAndGet();
            return true;
        }
    }

    private static final class InterruptingAwaitExecutor extends TrackingExecutor {

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
            if (awaitTerminationCalls.incrementAndGet() == 1) {
                throw new InterruptedException("await interrupted");
            }
            return true;
        }
    }
}
