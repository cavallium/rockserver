package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterBoundaryMode;
import it.cavallium.rockserver.core.replication.ratis.RatisFeatureUnavailableException;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterClientFactory;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealth;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealthStatus;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcCodec;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcException;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcHandler;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcLimits;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcRequest;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcResponse;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcTransport;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterSocketRpcServer;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterSocketRpcTransport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class RatisProcessAdapterRpcBridgeTest {

    @Test
    void processClientFactoryDispatchesAllPartitionOperationsThroughRpcHandler() {
        var descriptor = descriptor();
        var backingClient = new RecordingPartitionClient(descriptor);
        var reloaded = new AtomicBoolean();
        var handler = new RatisProcessAdapterRpcHandler(partitionId -> {
            assertEquals(descriptor.partitionId(), partitionId);
            return backingClient;
        }, () -> {
            reloaded.set(true);
            return CompletableFuture.completedFuture(null);
        });
        var factory = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                new HandlerTransport(handler)
        );

        var client = factory.openPartition(descriptor);
        var proposePayload = new byte[]{1, 2, 3};
        var proposed = client.propose(proposePayload).join();
        proposePayload[0] = 9;

        assertEquals(RatisAdapterBoundaryMode.EXTERNAL_PROCESS, factory.boundary().mode());
        assertTrue(factory.boundary().keepsRatisOffProductionModulePath());
        assertEquals(new ReplicatedLogCommit(11, true, new byte[]{13, 14}), proposed);
        assertArrayEquals(new byte[]{13, 14}, proposed.responsePayload());
        assertArrayEquals(new byte[]{1, 2, 3}, backingClient.proposedPayload);

        assertEquals(new ReplicatedLogCommit(27, true), client.waitForCommit(27).join());
        assertEquals(27, backingClient.waitedIndex);

        assertArrayEquals(new byte[]{4, 5}, client.queryLinearizable(new byte[]{8}).join());
        assertArrayEquals(new byte[]{8}, backingClient.linearizablePayload);

        assertArrayEquals(new byte[]{6, 7}, client.queryStale(new byte[]{9}, 42).join());
        assertArrayEquals(new byte[]{9}, backingClient.stalePayload);
        assertEquals(42, backingClient.minAppliedIndex);

        assertArrayEquals(new byte[]{10}, client.queryLocal(new byte[]{12}).join());
        assertArrayEquals(new byte[]{12}, backingClient.localPayload);

        factory.reloadMetadata().join();
        assertTrue(reloaded.get());

        client.close();
        assertThrows(IllegalStateException.class, () -> client.propose(new byte[]{1}));
    }

    @Test
    void processClientFactoryMapsAdapterErrorsToRpcException() {
        var factory = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                request -> CompletableFuture.completedFuture(RatisProcessAdapterRpcResponse.error(
                        request.requestId(),
                        "NOT_LEADER",
                        "leader moved"
                ))
        );
        var failure = joinFailure(factory.openPartition(descriptor()).propose(new byte[]{1}));

        var rpcFailure = assertInstanceOf(RatisProcessAdapterRpcException.class, failure);
        assertEquals("NOT_LEADER", rpcFailure.errorType());
        assertEquals("leader moved", rpcFailure.getMessage());
    }

    @Test
    void processClientFactoryMapsNullTransportFutureToRpcException() {
        var factory = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                _ -> null
        );

        var failure = joinFailure(factory.openPartition(descriptor()).queryLocal(new byte[]{1}));

        var rpcFailure = assertInstanceOf(RatisProcessAdapterRpcException.class, failure);
        assertEquals("ADAPTER_ERROR", rpcFailure.errorType());
        assertEquals("Ratis adapter RPC transport returned null response future", rpcFailure.getMessage());
    }

    @Test
    void processClientFactoryMapsNullTransportResponseToRpcException() {
        var factory = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                _ -> CompletableFuture.completedFuture(null)
        );

        var failure = joinFailure(factory.openPartition(descriptor()).queryLocal(new byte[]{1}));

        var rpcFailure = assertInstanceOf(RatisProcessAdapterRpcException.class, failure);
        assertEquals("ADAPTER_ERROR", rpcFailure.errorType());
        assertEquals("Ratis adapter RPC transport returned null response", rpcFailure.getMessage());
    }

    @Test
    void processClientFactoryRejectsNullInputsWithExplicitDiagnostics() {
        var factory = new RatisProcessAdapterClientFactory("ratis-process-test", _ -> CompletableFuture.completedFuture(
                RatisProcessAdapterRpcResponse.payload("unused", new byte[0])
        ));

        assertRejectsNull("transport must not be null",
                () -> new RatisProcessAdapterClientFactory("ratis-process-test", null));
        assertRejectsNull("descriptor must not be null", () -> factory.openPartition(null));
    }

    @Test
    void processClientFactoryPublishesClosedClientStateAcrossThreads() {
        var factory = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                request -> CompletableFuture.completedFuture(RatisProcessAdapterRpcResponse.commit(
                        request.requestId(),
                        1,
                        true
                ))
        );
        var client = factory.openPartition(descriptor());
        client.close();

        var failure = joinFailure(CompletableFuture.runAsync(() -> client.propose(new byte[]{1})));

        assertInstanceOf(IllegalStateException.class, failure);
    }

    @Test
    void processClientFactoryClosesRpcTransport() {
        var closed = new AtomicBoolean();
        var factory = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                new RatisProcessAdapterRpcTransport() {
                    @Override
                    public CompletableFuture<RatisProcessAdapterRpcResponse> send(
                            RatisProcessAdapterRpcRequest request) {
                        return CompletableFuture.completedFuture(RatisProcessAdapterRpcResponse.commit(
                                request.requestId(),
                                1,
                                true
                        ));
                    }

                    @Override
                    public void close() {
                        closed.set(true);
                    }
                }
        );

        factory.close();

        assertTrue(closed.get());
    }

    @Test
    void processClientFactoryRejectsNewWorkAfterCloseWithoutDependingOnTransport() {
        var sendCount = new AtomicInteger();
        var factory = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                request -> {
                    sendCount.incrementAndGet();
                    return CompletableFuture.completedFuture(RatisProcessAdapterRpcResponse.commit(
                            request.requestId(),
                            1,
                            true
                    ));
                }
        );
        var client = factory.openPartition(descriptor());

        factory.close();

        assertThrows(IllegalStateException.class, () -> factory.openPartition(descriptor()));
        assertThrows(IllegalStateException.class, factory::reloadMetadata);
        assertThrows(IllegalStateException.class, () -> client.propose(new byte[]{1}));
        assertEquals(0, sendCount.get());
    }

    @Test
    void processClientFactoryPropagatesPublicOperationCancellationToRpcTransport() {
        var transportResponses = new ArrayList<CompletableFuture<RatisProcessAdapterRpcResponse>>();
        var factory = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                _ -> {
                    var response = new CompletableFuture<RatisProcessAdapterRpcResponse>();
                    transportResponses.add(response);
                    return response;
                }
        );
        var client = factory.openPartition(descriptor());
        var publicOperations = List.of(
                client.propose(new byte[]{1}),
                client.waitForCommit(2),
                client.queryLinearizable(new byte[]{3}),
                client.queryStale(new byte[]{4}, 5),
                client.queryLocal(new byte[]{6}),
                factory.reloadMetadata()
        );

        publicOperations.forEach(operation -> assertTrue(operation.cancel(true)));

        assertEquals(publicOperations.size(), transportResponses.size());
        assertTrue(transportResponses.stream().allMatch(CompletableFuture::isCancelled));
    }

    @Test
    void rpcExceptionRejectsMultiLineErrorType() {
        assertThrows(IllegalArgumentException.class,
                () -> new RatisProcessAdapterRpcException("BAD\nTYPE", "message"));
    }

    @Test
    void processClientFactoryRejectsMismatchedResponseIds() {
        var factory = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                _ -> CompletableFuture.completedFuture(RatisProcessAdapterRpcResponse.commit(
                        "other-request",
                        1,
                        true
                ))
        );
        var failure = joinFailure(factory.openPartition(descriptor()).propose(new byte[]{1}));

        var rpcFailure = assertInstanceOf(RatisProcessAdapterRpcException.class, failure);
        assertEquals("ADAPTER_ERROR", rpcFailure.errorType());
        assertTrue(rpcFailure.getMessage().contains("does not match request id"));
    }

    @Test
    void processClientFactoryRejectsSuccessfulResponsesWithWrongShape() {
        var proposeWithPayload = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                request -> CompletableFuture.completedFuture(RatisProcessAdapterRpcResponse.commit(
                        request.requestId(),
                        7,
                        true,
                        new byte[]{1}
                ))
        );
        var waitWithPayload = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                request -> CompletableFuture.completedFuture(RatisProcessAdapterRpcResponse.commit(
                        request.requestId(),
                        7,
                        true,
                        new byte[]{1}
                ))
        );
        var queryWithCommit = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                request -> CompletableFuture.completedFuture(RatisProcessAdapterRpcResponse.commit(
                        request.requestId(),
                        7,
                        true
                ))
        );
        var reloadWithCommit = new RatisProcessAdapterClientFactory(
                "ratis-process-test",
                request -> CompletableFuture.completedFuture(RatisProcessAdapterRpcResponse.commit(
                        request.requestId(),
                        7,
                        true
                ))
        );

        var commit = proposeWithPayload.openPartition(descriptor()).propose(new byte[]{1}).join();
        assertArrayEquals(new byte[]{1}, commit.responsePayload());
        var waitFailure = assertInstanceOf(RatisProcessAdapterRpcException.class,
                joinFailure(waitWithPayload.openPartition(descriptor()).waitForCommit(7)));
        var queryFailure = assertInstanceOf(RatisProcessAdapterRpcException.class,
                joinFailure(queryWithCommit.openPartition(descriptor()).queryLocal(new byte[]{1})));
        var reloadFailure = assertInstanceOf(RatisProcessAdapterRpcException.class,
                joinFailure(reloadWithCommit.reloadMetadata()));
        assertEquals("ADAPTER_ERROR", waitFailure.errorType());
        assertTrue(waitFailure.getMessage().contains("WAIT_FOR_COMMIT response must not include a payload"));
        assertEquals("ADAPTER_ERROR", queryFailure.errorType());
        assertTrue(queryFailure.getMessage().contains("QUERY_LOCAL response must not include commit fields"));
        assertEquals("ADAPTER_ERROR", reloadFailure.errorType());
        assertTrue(reloadFailure.getMessage().contains("RELOAD_METADATA response must be empty"));
    }

    @Test
    void rpcHandlerConvertsPartitionFailuresToSingleLineErrors() {
        var descriptor = descriptor();
        var unsupportedClient = new UnsupportedPartitionClient(descriptor);
        var handler = new RatisProcessAdapterRpcHandler(partitionId -> {
            if ("missing-partition".equals(partitionId)) {
                throw new IllegalArgumentException("unknown\u2028partition");
            }
            return unsupportedClient;
        });

        var unsupported = handler.handle(RatisProcessAdapterRpcRequest.queryLocal(
                "request-1",
                descriptor.partitionId(),
                new byte[]{1}
        )).join();
        var missing = handler.handle(RatisProcessAdapterRpcRequest.propose(
                "request-2",
                "missing-partition",
                new byte[]{1}
        )).join();

        assertFalse(unsupported.success());
        assertEquals("UNSUPPORTED", unsupported.errorType());
        assertEquals("local disabled", unsupported.message());
        assertFalse(missing.success());
        assertEquals("BAD_REQUEST", missing.errorType());
        assertEquals("unknown partition", missing.message());
    }

    @Test
    void rpcHandlerTruncatesOversizedPartitionFailureMessages() {
        var oversizedMessage = "x".repeat(RatisProcessAdapterRpcLimits.MAX_ERROR_MESSAGE_CHARACTERS + 1);
        var handler = new RatisProcessAdapterRpcHandler(_ -> {
            throw new IllegalArgumentException(oversizedMessage);
        });

        var response = handler.handle(RatisProcessAdapterRpcRequest.propose(
                "request-1",
                "partition-a",
                new byte[0]
        )).join();

        assertFalse(response.success());
        assertEquals("BAD_REQUEST", response.errorType());
        assertEquals(RatisProcessAdapterRpcLimits.MAX_ERROR_MESSAGE_CHARACTERS, response.message().length());
    }

    @Test
    void rpcHandlerPropagatesSynchronousFatalContractFailures() {
        var partitionFailure = new AssertionError("fatal partition resolver failure");
        var reloadFailure = new AssertionError("fatal metadata reload failure");
        var partitionHandler = new RatisProcessAdapterRpcHandler(_ -> {
            throw partitionFailure;
        });
        var reloadHandler = new RatisProcessAdapterRpcHandler(
                _ -> new RecordingPartitionClient(descriptor()),
                () -> {
                    throw reloadFailure;
                }
        );

        var partition = assertThrows(AssertionError.class, () -> partitionHandler.handle(RatisProcessAdapterRpcRequest.queryLocal(
                "request-1",
                "partition-a",
                new byte[0]
        )));
        var reload = assertThrows(
                AssertionError.class,
                () -> reloadHandler.handle(RatisProcessAdapterRpcRequest.reloadMetadata("reload-1"))
        );

        assertSame(partitionFailure, partition);
        assertSame(reloadFailure, reload);
    }

    @Test
    void rpcHandlerPropagatesWrappedSynchronousFatalContractFailures() {
        var partitionFailure = new AssertionError("fatal wrapped partition resolver failure");
        var handler = new RatisProcessAdapterRpcHandler(_ -> {
            throw new CompletionException(partitionFailure);
        });

        var failure = assertThrows(AssertionError.class, () -> handler.handle(RatisProcessAdapterRpcRequest.queryLocal(
                "request-1",
                "partition-a",
                new byte[0]
        )));

        assertSame(partitionFailure, failure);
    }

    @Test
    void rpcHandlerPropagatesNestedWrappedSynchronousFatalContractFailures() {
        var partitionFailure = new AssertionError("fatal nested wrapped partition resolver failure");
        var handler = new RatisProcessAdapterRpcHandler(_ -> {
            throw new CompletionException(new CompletionException(partitionFailure));
        });

        var failure = assertThrows(AssertionError.class, () -> handler.handle(RatisProcessAdapterRpcRequest.queryLocal(
                "request-1",
                "partition-a",
                new byte[0]
        )));

        assertSame(partitionFailure, failure);
    }

    @Test
    void rpcHandlerPropagatesAsynchronousFatalOperationFailures() {
        var operationFailure = new AssertionError("fatal partition operation failure");
        var handler = new RatisProcessAdapterRpcHandler(
                _ -> new FatalOperationPartitionClient(descriptor(), operationFailure)
        );

        var failure = joinFailure(handler.handle(RatisProcessAdapterRpcRequest.queryLocal(
                "request-1",
                "partition-a",
                new byte[0]
        )));

        assertSame(operationFailure, failure);
    }

    @Test
    void rpcHandlerPropagatesNestedAsynchronousFatalOperationFailures() {
        var operationFailure = new AssertionError("fatal nested partition operation failure");
        var handler = new RatisProcessAdapterRpcHandler(
                _ -> new FatalOperationPartitionClient(
                        descriptor(),
                        new CompletionException(new CompletionException(operationFailure))
                )
        );

        var failure = joinFailure(handler.handle(RatisProcessAdapterRpcRequest.queryLocal(
                "request-1",
                "partition-a",
                new byte[0]
        )));

        assertSame(operationFailure, failure);
    }

    @Test
    void rpcHandlerTreatsNullAdapterContractResultsAsAdapterErrors() {
        var nullClientHandler = new RatisProcessAdapterRpcHandler(_ -> null);
        var nullReloadHandler = new RatisProcessAdapterRpcHandler(
                _ -> new RecordingPartitionClient(descriptor()),
                () -> null
        );

        var nullClient = nullClientHandler.handle(RatisProcessAdapterRpcRequest.queryLocal(
                "request-1",
                "partition-a",
                new byte[0]
        )).join();
        var nullReload = nullReloadHandler.handle(RatisProcessAdapterRpcRequest.reloadMetadata("reload-1")).join();

        assertFalse(nullClient.success());
        assertEquals("ADAPTER_ERROR", nullClient.errorType());
        assertEquals("resolved partition client must not be null", nullClient.message());
        assertFalse(nullReload.success());
        assertEquals("ADAPTER_ERROR", nullReload.errorType());
    }

    @Test
    void rpcHandlerTreatsNullPartitionOperationResultsAsAdapterErrors() {
        var descriptor = descriptor();
        var handler = new RatisProcessAdapterRpcHandler(_ -> new NullResultPartitionClient(descriptor));

        var propose = handler.handle(RatisProcessAdapterRpcRequest.propose(
                "request-1",
                descriptor.partitionId(),
                new byte[0]
        )).join();
        var wait = handler.handle(RatisProcessAdapterRpcRequest.waitForCommit(
                "request-2",
                descriptor.partitionId(),
                13
        )).join();
        var linearizable = handler.handle(RatisProcessAdapterRpcRequest.queryLinearizable(
                "request-3",
                descriptor.partitionId(),
                new byte[0]
        )).join();
        var stale = handler.handle(RatisProcessAdapterRpcRequest.queryStale(
                "request-4",
                descriptor.partitionId(),
                new byte[0],
                7
        )).join();
        var local = handler.handle(RatisProcessAdapterRpcRequest.queryLocal(
                "request-5",
                descriptor.partitionId(),
                new byte[0]
        )).join();

        assertAdapterError(propose, "request-1", "commit must not be null");
        assertAdapterError(wait, "request-2", "commit must not be null");
        assertAdapterError(linearizable, "request-3", "payload must not be null");
        assertAdapterError(stale, "request-4", "payload must not be null");
        assertAdapterError(local, "request-5", "payload must not be null");
    }

    @Test
    void rpcHandlerRejectsNullInputsWithExplicitDiagnostics() {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));

        assertRejectsNull("partitionClientResolver must not be null",
                () -> new RatisProcessAdapterRpcHandler(null));
        assertRejectsNull("partitionClientResolver must not be null",
                () -> new RatisProcessAdapterRpcHandler(null, () -> CompletableFuture.completedFuture(null)));
        assertRejectsNull("metadataReloader must not be null",
                () -> new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()), null));
        assertRejectsNull("request must not be null", () -> handler.handle(null));
    }

    @Test
    void rpcHandlerReturnsUnsupportedWhenMetadataReloadIsNotConfigured() {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));

        var response = handler.handle(RatisProcessAdapterRpcRequest.reloadMetadata("reload-1")).join();

        assertFalse(response.success());
        assertEquals("UNSUPPORTED", response.errorType());
        assertEquals("metadata reload is not configured", response.message());
    }

    @Test
    void rpcHandlerReturnsUnsupportedWhenMetadataReloadFeatureIsUnavailable() {
        var handler = new RatisProcessAdapterRpcHandler(
                _ -> new RecordingPartitionClient(descriptor()),
                () -> CompletableFuture.failedFuture(
                        new RatisFeatureUnavailableException("adapter metadata reload requires config and metadata paths")
                )
        );

        var response = handler.handle(RatisProcessAdapterRpcRequest.reloadMetadata("reload-1")).join();

        assertFalse(response.success());
        assertEquals("UNSUPPORTED", response.errorType());
        assertEquals("adapter metadata reload requires config and metadata paths", response.message());
    }

    @Test
    void socketRpcTransportSendsEncodedRequestAndDecodesResponse() throws Exception {
        try (var server = new ServerSocket(0);
             var transport = new RatisProcessAdapterSocketRpcTransport(
                     new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                     Duration.ofSeconds(5)
             )) {
            var served = CompletableFuture.supplyAsync(() -> serveOneRpcRequest(server));
            var request = RatisProcessAdapterRpcRequest.queryLinearizable(
                    "request-1",
                    "partition-a",
                    new byte[]{1, 2, 3}
            );

            var response = transport.send(request).get(5, TimeUnit.SECONDS);

            assertEquals(RatisProcessAdapterRpcResponse.payload("request-1", new byte[]{7, 8}), response);
            assertEquals(request, served.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void socketRpcTransportRejectsOversizedOutboundRequestBeforeConnecting() {
        try (var transport = new RatisProcessAdapterSocketRpcTransport(
                new InetSocketAddress("127.0.0.1", 1),
                Duration.ofSeconds(5),
                128
        )) {

            var failure = joinFailure(transport.send(RatisProcessAdapterRpcRequest.propose(
                    "request-1",
                    "partition-a",
                    new byte[256]
            )));

            assertInstanceOf(IOException.class, failure);
            assertTrue(failure.getMessage().contains("exceeds max size"));
        }
    }

    @Test
    void socketRpcTransportRejectsMalformedOutboundUnicodeBeforeConnecting() {
        try (var transport = new RatisProcessAdapterSocketRpcTransport(
                new InetSocketAddress("127.0.0.1", 1),
                Duration.ofSeconds(5)
        )) {

            var failure = joinFailure(transport.send(RatisProcessAdapterRpcRequest.propose(
                    "request-\uD800",
                    "partition-a",
                    new byte[0]
            )));

            assertInstanceOf(IOException.class, failure);
            assertTrue(failure.getMessage().contains("cannot be encoded as UTF-8"));
        }
    }

    @Test
    void socketRpcTransportRejectsOversizedMultibyteOutboundRequestBeforeConnecting() {
        try (var transport = new RatisProcessAdapterSocketRpcTransport(
                new InetSocketAddress("127.0.0.1", 1),
                Duration.ofSeconds(5),
                180
        )) {

            var failure = joinFailure(transport.send(RatisProcessAdapterRpcRequest.propose(
                    "\u00E9".repeat(64),
                    "partition-a",
                    new byte[0]
            )));

            assertInstanceOf(IOException.class, failure);
            assertTrue(failure.getMessage().contains("exceeds max size"));
        }
    }

    @Test
    void socketRpcTransportRejectsOversizedResponse() throws Exception {
        try (var server = new ServerSocket(0);
             var transport = new RatisProcessAdapterSocketRpcTransport(
                     new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                     Duration.ofSeconds(5),
                     256
             )) {
            var served = CompletableFuture.runAsync(() -> serveOversizedResponse(server));

            var failure = joinFailure(transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                    "request-1",
                    "partition-a",
                    new byte[0]
            )));

            assertInstanceOf(IOException.class, failure);
            assertTrue(failure.getMessage().contains("exceeds max size"));
            served.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void socketRpcTransportReportsMalformedResponseAsTransportFailure() throws Exception {
        try (var server = new ServerSocket(0);
             var transport = new RatisProcessAdapterSocketRpcTransport(
                     new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                     Duration.ofSeconds(5)
             )) {
            var served = CompletableFuture.runAsync(() -> serveMalformedResponse(server));

            var failure = joinFailure(transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                    "request-1",
                    "partition-a",
                    new byte[0]
            )));

            assertInstanceOf(IOException.class, failure);
            assertTrue(failure.getMessage().contains("Invalid Ratis process adapter RPC response"));
            served.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void socketRpcTransportRejectsMalformedUtf8Response() throws Exception {
        try (var server = new ServerSocket(0);
             var transport = new RatisProcessAdapterSocketRpcTransport(
                     new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                     Duration.ofSeconds(5)
             )) {
            var served = CompletableFuture.runAsync(() -> serveMalformedUtf8Response(server));

            var failure = joinFailure(transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                    "request-1",
                    "partition-a",
                    new byte[0]
            )));

            assertInstanceOf(IOException.class, failure);
            assertTrue(failure.getMessage().contains("not valid UTF-8"));
            served.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void socketRpcTransportAppliesOneTimeoutBudgetToDripResponse() throws Exception {
        try (var server = new ServerSocket(0);
             var transport = new RatisProcessAdapterSocketRpcTransport(
                     new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                     Duration.ofMillis(50),
                     256
             )) {
            var served = CompletableFuture.runAsync(() -> serveDripResponse(server));

            var failure = assertTimeoutPreemptively(Duration.ofMillis(500), () -> joinFailure(transport.send(
                    RatisProcessAdapterRpcRequest.queryLocal("request-1", "partition-a", new byte[0])
            )));

            assertInstanceOf(SocketTimeoutException.class, failure);
            served.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void socketRpcTransportTimesOutBlockedRequestWrite() throws Exception {
        try (var server = new ServerSocket(0)) {
            server.setReceiveBufferSize(1024);
            var accepted = new CountDownLatch(1);
            var releaseConnection = new CountDownLatch(1);
            var served = CompletableFuture.runAsync(
                    () -> holdConnectionWithoutReading(server, accepted, releaseConnection)
            );
            try (var transport = new RatisProcessAdapterSocketRpcTransport(
                    new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                    Duration.ofMillis(100),
                    16 * 1024 * 1024
            )) {
                try {
                    var failure = assertTimeoutPreemptively(Duration.ofSeconds(1), () -> joinFailure(transport.send(
                            RatisProcessAdapterRpcRequest.propose(
                                    "request-1",
                                    "partition-a",
                                    new byte[8 * 1024 * 1024]
                            )
                    )));

                    assertTrue(accepted.await(1, TimeUnit.SECONDS));
                    assertInstanceOf(SocketTimeoutException.class, failure);
                } finally {
                    releaseConnection.countDown();
                    served.get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Test
    void socketRpcTransportReturnsBeforeResponseAndRejectsSaturatedClientExecutor() throws Exception {
        try (var server = new ServerSocket(0)) {
            var accepted = new CountDownLatch(1);
            var releaseConnection = new CountDownLatch(1);
            var served = CompletableFuture.runAsync(
                    () -> holdConnectionWithoutReading(server, accepted, releaseConnection)
            );
            try (var transport = new RatisProcessAdapterSocketRpcTransport(
                    new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                    Duration.ofSeconds(5),
                    4096,
                    1
            )) {
                CompletableFuture<RatisProcessAdapterRpcResponse> pending = null;
                try {
                    pending = assertTimeoutPreemptively(Duration.ofMillis(250), () -> transport.send(
                            RatisProcessAdapterRpcRequest.queryLocal("request-1", "partition-a", new byte[0])
                    ));
                    assertTrue(accepted.await(1, TimeUnit.SECONDS));
                    assertFalse(pending.isDone());

                    var failure = joinFailure(assertTimeoutPreemptively(Duration.ofMillis(250), () -> transport.send(
                            RatisProcessAdapterRpcRequest.queryLocal("request-2", "partition-a", new byte[0])
                    )));

                    assertInstanceOf(IOException.class, failure);
                    assertTrue(failure.getMessage().contains("client executor is saturated"));
                } finally {
                    releaseConnection.countDown();
                    served.get(5, TimeUnit.SECONDS);
                    if (pending != null) {
                        assertInstanceOf(IOException.class, joinFailure(pending));
                    }
                }
            }
        }
    }

    @Test
    void socketRpcTransportCancellationReleasesClientCapacityPromptly() throws Exception {
        try (var server = new ServerSocket(0);
             var transport = new RatisProcessAdapterSocketRpcTransport(
                     new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                     Duration.ofSeconds(5),
                     4096,
                     1
             )) {
            var firstAccepted = new CountDownLatch(1);
            var served = CompletableFuture.supplyAsync(() -> serveOneRpcRequestAfterHoldingFirst(server, firstAccepted));
            var pending = transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                    "request-1",
                    "partition-a",
                    new byte[0]
            ));
            assertTrue(firstAccepted.await(1, TimeUnit.SECONDS));

            assertTrue(pending.cancel(true));
            var request = RatisProcessAdapterRpcRequest.queryLocal("request-2", "partition-a", new byte[0]);
            var response = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> sendWhenClientCapacityIsAvailable(transport, request)
            );

            assertEquals(RatisProcessAdapterRpcResponse.payload("request-2", new byte[]{7, 8}), response);
            assertEquals(request, served.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void socketRpcTransportCancellationRetainsUncheckedSocketCloseFailureForShutdownRetry() throws Exception {
        var transport = new RatisProcessAdapterSocketRpcTransport(
                new InetSocketAddress("127.0.0.1", 1),
                Duration.ofSeconds(5)
        );
        var socket = new CloseOnceErrorSocket("injected cancellation socket close failure");
        try {
            var response = cancellationAwareResponse(transport, socket);

            var failure = assertThrows(AssertionError.class, () -> response.cancel(true));

            assertEquals("injected cancellation socket close failure", failure.getMessage());
            assertTrue(response.isCancelled());
            assertTrue(pendingSocketCleanupSet(transport).contains(socket));

            transport.close();

            assertTrue(pendingSocketCleanupSet(transport).isEmpty());
            assertEquals(2, socket.closeCalls());
        } finally {
            transport.close();
        }
    }

    @Test
    void socketRpcTransportCancellationRetainsCheckedSocketCloseFailureForShutdownRetry() throws Exception {
        var transport = new RatisProcessAdapterSocketRpcTransport(
                new InetSocketAddress("127.0.0.1", 1),
                Duration.ofSeconds(5)
        );
        var socket = new CloseOnceIOExceptionSocket("injected cancellation socket close failure");
        try {
            var response = cancellationAwareResponse(transport, socket);

            assertTrue(response.cancel(true));

            assertTrue(response.isCancelled());
            assertTrue(pendingSocketCleanupSet(transport).contains(socket));

            transport.close();

            assertTrue(pendingSocketCleanupSet(transport).isEmpty());
            assertEquals(2, socket.closeCalls());
        } finally {
            transport.close();
        }
    }

    @Test
    void socketRpcTransportCloseReportsIncompleteCheckedSocketCleanup() throws Exception {
        var transport = new RatisProcessAdapterSocketRpcTransport(
                new InetSocketAddress("127.0.0.1", 1),
                Duration.ofSeconds(5)
        );
        var socket = new ErroringIOExceptionSocket("injected persistent socket close failure");
        try {
            var response = cancellationAwareResponse(transport, socket);
            assertTrue(response.cancel(true));

            var failure = assertThrows(IllegalStateException.class, transport::close);

            assertTrue(failure.getMessage().contains("socket cleanup remains incomplete"));
            assertTrue(pendingSocketCleanupSet(transport).contains(socket));
            assertEquals(3, socket.closeCalls());
        } finally {
            pendingSocketCleanupSet(transport).clear();
            transport.close();
        }
    }

    @Test
    void socketRpcTransportCloseFailsActiveAndLaterRequestsPromptly() throws Exception {
        try (var server = new ServerSocket(0)) {
            var accepted = new CountDownLatch(1);
            var releaseConnection = new CountDownLatch(1);
            var served = CompletableFuture.runAsync(
                    () -> holdConnectionWithoutReading(server, accepted, releaseConnection)
            );
            try (var transport = new RatisProcessAdapterSocketRpcTransport(
                    new InetSocketAddress("127.0.0.1", server.getLocalPort()),
                    Duration.ofSeconds(5),
                    4096,
                    1
            )) {
                var pending = transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                        "request-1",
                        "partition-a",
                        new byte[0]
                ));
                assertTrue(accepted.await(1, TimeUnit.SECONDS));
                assertFalse(pending.isDone());

                transport.close();

                assertTrue(requestExecutor(transport).isTerminated());
                var pendingFailure = assertTimeoutPreemptively(
                        Duration.ofSeconds(1),
                        () -> joinFailure(pending)
                );
                assertInstanceOf(IOException.class, pendingFailure);
                var laterFailure = joinFailure(transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                        "request-2",
                        "partition-a",
                        new byte[0]
                )));
                assertInstanceOf(IOException.class, laterFailure);
                assertTrue(laterFailure.getMessage().contains("transport is closed"));
            } finally {
                releaseConnection.countDown();
                served.get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void socketRpcTransportCloseWaitsAcrossInterruptionAndRestoresInterrupt() throws Exception {
        var transport = new RatisProcessAdapterSocketRpcTransport(
                new InetSocketAddress("127.0.0.1", 1),
                Duration.ofSeconds(5)
        );
        var workerEntered = new CountDownLatch(1);
        var releaseWorker = new CountDownLatch(1);
        var closeEntered = new CountDownLatch(1);
        var closeFailure = new AtomicReference<Throwable>();
        var interruptRestored = new AtomicInteger();
        try {
            requestExecutor(transport).execute(() -> {
                workerEntered.countDown();
                awaitUninterruptibly(releaseWorker);
            });
            assertTrue(workerEntered.await(1, TimeUnit.SECONDS));

            var closeThread = new Thread(() -> {
                closeEntered.countDown();
                try {
                    transport.close();
                } catch (Throwable e) {
                    closeFailure.set(e);
                }
                if (Thread.currentThread().isInterrupted()) {
                    interruptRestored.incrementAndGet();
                }
            }, "ratis-rpc-transport-close-test");
            closeThread.start();
            try {
                assertTrue(closeEntered.await(1, TimeUnit.SECONDS));
                TimeUnit.MILLISECONDS.sleep(50);
                assertTrue(closeThread.isAlive());
                closeThread.interrupt();
                TimeUnit.MILLISECONDS.sleep(50);
                assertTrue(closeThread.isAlive(), "close returned before interrupted worker shutdown completed");
            } finally {
                releaseWorker.countDown();
            }
            closeThread.join(1_000);

            assertFalse(closeThread.isAlive());
            assertNull(closeFailure.get());
            assertEquals(1, interruptRestored.get());
            assertTrue(requestExecutor(transport).isTerminated());
        } finally {
            releaseWorker.countDown();
            Thread.interrupted();
            transport.close();
        }
    }

    @Test
    void socketRpcTransportCloseStillFailsSiblingRequestsWhenSocketCloseThrowsError() throws Exception {
        var transport = new RatisProcessAdapterSocketRpcTransport(
                new InetSocketAddress("127.0.0.1", 1),
                Duration.ofSeconds(5)
        );
        var sockets = List.of(
                new ErroringCloseSocket("first close failed"),
                new ErroringCloseSocket("second close failed"),
                new ErroringCloseSocket("third close failed")
        );
        var responses = List.of(
                new CompletableFuture<RatisProcessAdapterRpcResponse>(),
                new CompletableFuture<RatisProcessAdapterRpcResponse>(),
                new CompletableFuture<RatisProcessAdapterRpcResponse>()
        );
        try {
            for (var i = 0; i < sockets.size(); i++) {
                pendingRequestSet(transport).add(newPendingRequest(responses.get(i), sockets.get(i)));
            }

            assertThrows(AssertionError.class, transport::close);

            assertTrue(sockets.stream().allMatch(ErroringCloseSocket::closeCalled));
            assertTrue(responses.stream().allMatch(CompletableFuture::isCompletedExceptionally));
            assertTrue(pendingSocketCleanupSet(transport).containsAll(sockets));
            assertTrue(requestExecutor(transport).isTerminated());
        } finally {
            pendingRequestSet(transport).clear();
            pendingSocketCleanupSet(transport).clear();
            transport.close();
        }
    }

    @Test
    void socketWireWriteTimeoutSuppressesUncheckedSocketCloseFailure() {
        var closeFailure = new AssertionError("injected write-timeout socket close failure");
        var failure = assertTimeoutPreemptively(
                Duration.ofSeconds(1),
                () -> assertInstanceOf(
                        SocketTimeoutException.class,
                        invokeSocketWireWriteUtf8(new TimeoutCloseErrorSocket(closeFailure))
                )
        );

        assertTrue(failure.getMessage().contains("timed out"));
        assertEquals(List.of(closeFailure), List.of(failure.getSuppressed()));
    }

    @Test
    void socketWireWriteTimeoutDoesNotLetSlowCloseBlockLaterTimeouts() throws Exception {
        var firstCloseFailure = new AssertionError("injected slow write-timeout socket close failure");
        var firstSocket = new BlockingTimeoutCloseErrorSocket(firstCloseFailure);
        var firstWrite = CompletableFuture.supplyAsync(() -> {
            try {
                return invokeSocketWireWriteUtf8(firstSocket);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
        try {
            assertTrue(firstSocket.closeAttempted.await(1, TimeUnit.SECONDS));
            assertFalse(firstWrite.isDone());

            var secondCloseFailure = new AssertionError("injected later write-timeout socket close failure");
            var secondFailure = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> assertInstanceOf(
                            SocketTimeoutException.class,
                            invokeSocketWireWriteUtf8(new TimeoutCloseErrorSocket(secondCloseFailure))
                    )
            );

            assertEquals(List.of(secondCloseFailure), List.of(secondFailure.getSuppressed()));
        } finally {
            firstSocket.releaseClose.countDown();
        }
        var firstFailure = assertTimeoutPreemptively(Duration.ofSeconds(1), firstWrite::join);
        assertInstanceOf(SocketTimeoutException.class, firstFailure);
        assertEquals(List.of(firstCloseFailure), List.of(firstFailure.getSuppressed()));
    }

    @Test
    void socketWireWriteTimeoutDoesNotWaitIndefinitelyForSlowClose() throws Exception {
        var socket = new BlockingTimeoutCloseErrorSocket(new AssertionError(
                "injected slow write-timeout socket close failure"
        ));
        try {
            var failure = assertTimeoutPreemptively(
                    Duration.ofSeconds(1),
                    () -> assertInstanceOf(SocketTimeoutException.class, invokeSocketWireWriteUtf8(socket))
            );

            var closeFailure = assertInstanceOf(IOException.class, failure.getSuppressed()[0]);
            assertTrue(closeFailure.getMessage().contains("did not complete within 100 milliseconds"));
        } finally {
            socket.releaseClose.countDown();
            assertTrue(socket.closeFinished.await(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void socketWireListenerStartupRollbackSuppressesUncheckedCloseFailure() throws Exception {
        var startFailure = new AssertionError("injected listener startup failure");
        var closeFailure = new AssertionError("injected listener rollback close failure");
        var listener = new ClosingErrorServerSocket(closeFailure);

        assertSame(startFailure, invokeSocketWireBindAndStartListener(listener, _ -> {
            throw startFailure;
        }));

        assertEquals(List.of(closeFailure), List.of(startFailure.getSuppressed()));
        assertTrue(listener.closeCalled());
    }

    @Test
    void socketRpcTransportSendCleansPendingRequestWhenExecutorThrowsError() throws Exception {
        var transport = new RatisProcessAdapterSocketRpcTransport(
                new InetSocketAddress("127.0.0.1", 1),
                Duration.ofSeconds(5)
        );
        var handoffFailure = new AssertionError("request executor failed");
        var failingExecutor = new ErroringThreadPoolExecutor(handoffFailure);
        var requestExecutor = replaceTransportRequestExecutor(transport, failingExecutor);
        try {
            var failure = assertThrows(AssertionError.class, () -> transport.send(
                    RatisProcessAdapterRpcRequest.queryLocal(
                            "request-1",
                            descriptor().partitionId(),
                            new byte[]{1}
                    )
            ));

            assertSame(handoffFailure, failure);
            assertTrue(pendingRequestSet(transport).isEmpty());
        } finally {
            replaceTransportRequestExecutor(transport, requestExecutor);
            failingExecutor.shutdownNow();
            transport.close();
        }
    }

    @Test
    void socketRpcServerServesRequestsThroughHandler() throws Exception {
        var descriptor = descriptor();
        var backingClient = new RecordingPartitionClient(descriptor);
        var handler = new RatisProcessAdapterRpcHandler(_ -> backingClient);
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            server.start();
            try (var transport = new RatisProcessAdapterSocketRpcTransport(
                    server.boundAddress(),
                    Duration.ofSeconds(5)
            )) {
                var response = transport.send(RatisProcessAdapterRpcRequest.propose(
                        "request-1",
                        descriptor.partitionId(),
                        new byte[]{1, 2, 3}
                )).get(5, TimeUnit.SECONDS);

                assertEquals(RatisProcessAdapterRpcResponse.commit("request-1", 11, true, new byte[]{13, 14}),
                        response);
                assertArrayEquals(new byte[]{1, 2, 3}, backingClient.proposedPayload);
            }
        }
    }

    @Test
    void socketRpcServerCloseDisconnectsAcceptedIdleRequestPromptly() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            server.start();
            try (var socket = new Socket()) {
                socket.connect(server.boundAddress(), 1_000);
                socket.setSoTimeout(1_000);
                socket.getOutputStream().write('x');
                socket.getOutputStream().flush();
                awaitAcceptedSocket(server);

                assertTimeoutPreemptively(Duration.ofSeconds(1), server::close);

                assertPeerDisconnected(socket);
            }
        }
    }

    @Test
    void socketRpcEndpointsRejectTimeoutsWithoutMillisecondRepresentation() {
        var address = new InetSocketAddress("127.0.0.1", 1);
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        for (var timeout : List.of(
                Duration.ofMillis(-1),
                Duration.ZERO,
                Duration.ofNanos(1),
                Duration.ofMillis((long) Integer.MAX_VALUE + 1),
                Duration.ofSeconds(Long.MAX_VALUE)
        )) {
            assertThrows(IllegalArgumentException.class,
                    () -> {
                        try (var ignored = new RatisProcessAdapterSocketRpcTransport(address, timeout)) {
                            throw new AssertionError("expected constructor rejection");
                        }
                    });
            assertThrows(IllegalArgumentException.class,
                    () -> {
                        try (var ignored = new RatisProcessAdapterSocketRpcServer(address, handler, timeout)) {
                            throw new AssertionError("expected constructor rejection");
                        }
                    });
        }
    }

    @Test
    void socketRpcEndpointsRejectNullInputsWithExplicitDiagnostics() throws Exception {
        var address = new InetSocketAddress("127.0.0.1", 1);
        var timeout = Duration.ofMillis(1);
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));

        assertRejectsNull("address must not be null",
                () -> new RatisProcessAdapterSocketRpcTransport(null, timeout));
        assertRejectsNull("timeout must not be null",
                () -> new RatisProcessAdapterSocketRpcTransport(address, null));
        try (var transport = new RatisProcessAdapterSocketRpcTransport(address, timeout)) {
            assertRejectsNull("request must not be null", () -> transport.send(null));
        }
        assertRejectsNull("bindAddress must not be null",
                () -> new RatisProcessAdapterSocketRpcServer(null, handler, timeout));
        assertRejectsNull("handler must not be null",
                () -> new RatisProcessAdapterSocketRpcServer(address, null, timeout));
        assertRejectsNull("stopTimeout must not be null",
                () -> new RatisProcessAdapterSocketRpcServer(address, handler, null));
        try (var server = new RatisProcessAdapterSocketRpcServer(address, handler, timeout)) {
            var failure = invokeRpcStartListener(server, null);

            assertInstanceOf(IllegalArgumentException.class, failure);
            assertEquals("listener must not be null", failure.getMessage());
        }
    }

    @Test
    void socketRpcTransportRejectsNonPositiveConcurrencyLimit() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (var ignored = new RatisProcessAdapterSocketRpcTransport(
                    new InetSocketAddress("127.0.0.1", 1),
                    Duration.ofSeconds(1),
                    4096,
                    0
            )) {
                throw new AssertionError("expected constructor rejection");
            }
        });
    }

    @Test
    void socketRpcServerCanRetryAfterBindFailure() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var blocker = new ServerSocket()) {
            blocker.bind(new InetSocketAddress("127.0.0.1", 0));
            var address = new InetSocketAddress("127.0.0.1", blocker.getLocalPort());
            try (var server = new RatisProcessAdapterSocketRpcServer(address, handler, Duration.ofSeconds(5))) {
                assertThrows(IOException.class, server::start);
                assertThrows(IllegalStateException.class, server::boundAddress);

                blocker.close();
                server.start();

                assertEquals(address, server.boundAddress());
            }
        }
    }

    @Test
    void socketRpcServerCanRetryAfterAcceptExecutorThrowsError() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var blocker = new ServerSocket()) {
            blocker.bind(new InetSocketAddress("127.0.0.1", 0));
            var address = new InetSocketAddress("127.0.0.1", blocker.getLocalPort());
            blocker.close();
            try (var server = new RatisProcessAdapterSocketRpcServer(address, handler, Duration.ofSeconds(5))) {
                var handoffFailure = new AssertionError("accept executor failed");
                var acceptExecutor = replaceRpcAcceptExecutor(server, new ErroringExecutorService(handoffFailure));
                try {
                    assertSame(handoffFailure, assertThrows(AssertionError.class, server::start));
                    assertFalse(server.isRunning());
                } finally {
                    replaceRpcAcceptExecutor(server, acceptExecutor);
                }

                server.start();

                assertEquals(address, server.boundAddress());
            }
        }
    }

    @Test
    void socketRpcServerRetainsStartupListenerWhenRollbackCloseThrowsError() throws Exception {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var handoffFailure = new AssertionError("accept executor failed");
        var closeFailure = new AssertionError("startup listener rollback close failed");
        var listener = new CloseOnceErrorServerSocket(closeFailure);
        var acceptExecutor = replaceRpcAcceptExecutor(server, new ErroringExecutorService(handoffFailure));
        try {
            assertSame(handoffFailure, invokeRpcStartListener(server, listener));
            assertEquals(List.of(closeFailure), List.of(handoffFailure.getSuppressed()));
            assertSame(listener, listener(server));
            assertThrows(IllegalStateException.class, server::start);

            replaceRpcAcceptExecutor(server, acceptExecutor);
            server.close();
            assertEquals(2, listener.closeCalls());
            assertNull(listener(server));
        } finally {
            replaceRpcAcceptExecutor(server, acceptExecutor);
            clearRpcListener(server);
            server.close();
        }
    }

    @Test
    void socketRpcServerReportsUnexpectedAcceptFailureAsDegradedHealth() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            server.start();
            assertTrue(server.health().isReady());

            listener(server).close();
            var health = awaitDegradedHealth(server);

            assertEquals(RatisProcessAdapterHealthStatus.DEGRADED, health.status());
            assertTrue(health.message().contains("RPC listener failed"));
            assertThrows(IllegalStateException.class, server::start);
        }
    }

    @Test
    void socketRpcServerClosesAcceptedSocketAndDegradesWhenRequestExecutorThrowsError() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var blocker = new ServerSocket()) {
            blocker.bind(new InetSocketAddress("127.0.0.1", 0));
            var address = new InetSocketAddress("127.0.0.1", blocker.getLocalPort());
            blocker.close();
            try (var server = new RatisProcessAdapterSocketRpcServer(address, handler, Duration.ofSeconds(5))) {
                server.start();
                var handoffFailure = new AssertionError("request executor failed");
                var requestExecutor = replaceRpcRequestExecutor(server, new ErroringExecutorService(handoffFailure));
                try (var socket = new Socket()) {
                    socket.connect(address, 5_000);
                    socket.setSoTimeout(1_000);

                    assertPeerDisconnected(socket);
                    var health = awaitDegradedHealth(server);

                    assertTrue(health.message().contains("RPC listener failed"));
                    assertThrows(IllegalStateException.class, server::start);
                    try (var rebound = new ServerSocket()) {
                        rebound.bind(address);
                    }
                } finally {
                    replaceRpcRequestExecutor(server, requestExecutor);
                }
            }
        }
    }

    @Test
    void socketRpcServerStopsListenerAndReportsRequestWorkerErrorAsDegradedHealth() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            server.start();
            var address = server.boundAddress();
            var requestFailure = new AssertionError("injected RPC request worker failure");

            assertSame(requestFailure, invokeRpcServe(server, new ErroringSocket(requestFailure)));

            var health = awaitDegradedHealth(server);
            assertFalse(server.isRunning());
            assertTrue(health.message().contains("RPC request worker failed"));
            assertThrows(IllegalStateException.class, server::boundAddress);
            assertThrows(IllegalStateException.class, server::start);
            try (var rebound = new ServerSocket()) {
                rebound.bind(address);
            }
        }
    }

    @Test
    void socketRpcServerPropagatesFatalHandlerFailureFromOperationExecutor() throws Throwable {
        var fatalFailure = new AssertionError("fatal handler failure");
        var handler = new RatisProcessAdapterRpcHandler(_ -> {
            throw fatalFailure;
        });
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            var failure = assertThrows(AssertionError.class, () -> invokeRpcHandle(
                    server,
                    RatisProcessAdapterRpcRequest.queryLocal("request-1", "partition-a", new byte[0])
            ));

            assertSame(fatalFailure, failure);
        }
    }

    @Test
    void socketRpcServerPropagatesFatalResponseFailureFromOperationFuture() throws Throwable {
        var fatalFailure = new AssertionError("fatal operation failure");
        var handler = new RatisProcessAdapterRpcHandler(_ -> new FatalOperationPartitionClient(
                descriptor(),
                fatalFailure
        ));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            var failure = assertThrows(AssertionError.class, () -> invokeRpcHandle(
                    server,
                    RatisProcessAdapterRpcRequest.queryLocal("request-1", "partition-a", new byte[0])
            ));

            assertSame(fatalFailure, failure);
        }
    }

    @Test
    void socketRpcServerPropagatesNestedWrappedFatalOperationExecutorFailures() throws Throwable {
        var fatalFailure = new AssertionError("fatal nested operation executor failure");
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            var previous = replaceRpcOperationExecutor(
                    server,
                    new FailingSubmitThreadPoolExecutor(new CompletionException(new CompletionException(fatalFailure)))
            );
            previous.shutdownNow();

            var failure = assertThrows(AssertionError.class, () -> invokeRpcHandle(
                    server,
                    RatisProcessAdapterRpcRequest.queryLocal("request-1", "partition-a", new byte[0])
            ));

            assertSame(fatalFailure, failure);
        }
    }

    @Test
    void socketRpcServerRetainsRequestSocketWhenWorkerCloseThrowsError() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            server.start();
            var closeFailure = new AssertionError("injected RPC request socket close failure");
            var socket = new WorkerCloseErrorSocket(closeFailure);
            acceptedSocketSet(server).add(socket);
            try {
                assertSame(closeFailure, invokeRpcServe(server, socket));

                assertTrue(acceptedSocketSet(server).contains(socket));
                assertFalse(server.isRunning());
                assertTrue(server.health().message().contains("RPC request worker failed"));
            } finally {
                acceptedSocketSet(server).remove(socket);
            }
        }
    }

    @Test
    void socketRpcServerRetainsRejectedSocketWhenCloseThrowsError() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            var closeFailure = new AssertionError("injected rejected RPC socket close failure");
            var socket = new WorkerCloseErrorSocket(closeFailure);
            acceptedSocketSet(server).add(socket);
            try {
                assertSame(closeFailure, invokeRpcRejectBusy(server, socket));

                assertTrue(acceptedSocketSet(server).contains(socket));
            } finally {
                acceptedSocketSet(server).remove(socket);
            }
        }
    }

    @Test
    void socketRpcServerRetainsHandoffSocketWhenCloseThrowsError() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            var handoffFailure = new AssertionError("injected RPC handoff failure");
            var closeFailure = new AssertionError("injected RPC handoff socket close failure");
            var socket = new WorkerCloseErrorSocket(closeFailure);
            acceptedSocketSet(server).add(socket);
            try {
                invokeRpcCloseAfterFailedHandoff(server, socket, handoffFailure);

                assertEquals(List.of(closeFailure), List.of(handoffFailure.getSuppressed()));
                assertTrue(acceptedSocketSet(server).contains(socket));
            } finally {
                acceptedSocketSet(server).remove(socket);
            }
        }
    }

    @Test
    void socketRpcServerPreservesRequestWorkerFailureWhenListenerCloseThrowsError() throws Exception {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var workerFailure = new IOException("injected RPC request worker failure");
        var closeFailure = new AssertionError("injected RPC listener close failure");
        try {
            invokeRpcFailRequestWorker(server, workerFailure, new ErroringCloseServerSocket(closeFailure));

            assertFalse(server.isRunning());
            assertEquals(List.of(closeFailure), List.of(workerFailure.getSuppressed()));
            assertTrue(server.health().message().contains("RPC request worker failed"));
            assertThrows(IllegalStateException.class, server::start);
        } finally {
            clearRpcListener(server);
            server.close();
        }
    }

    @Test
    void socketRpcServerPreservesAcceptLoopFailureWhenListenerCloseThrowsError() throws Exception {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var acceptFailure = new IOException("injected RPC accept-loop failure");
        var closeFailure = new AssertionError("injected RPC listener close failure");
        try {
            invokeRpcFailAcceptLoop(server, acceptFailure, new ErroringCloseServerSocket(closeFailure));

            assertFalse(server.isRunning());
            assertEquals(List.of(closeFailure), List.of(acceptFailure.getSuppressed()));
            assertTrue(server.health().message().contains("RPC listener failed"));
            assertThrows(IllegalStateException.class, server::start);
        } finally {
            clearRpcListener(server);
            server.close();
        }
    }

    @Test
    void socketRpcServerReturnsBadRequestForOversizedPayload() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5),
                256
        )) {
            server.start();
            try (var socket = new Socket()) {
                socket.connect(server.boundAddress(), 5_000);
                socket.getOutputStream().write("x".repeat(512).getBytes(StandardCharsets.UTF_8));
                socket.shutdownOutput();

                var response = RatisProcessAdapterRpcCodec.decodeResponse(new String(
                        socket.getInputStream().readAllBytes(),
                        StandardCharsets.UTF_8
                ));

                assertFalse(response.success());
                assertEquals("invalid", response.requestId());
                assertEquals("BAD_REQUEST", response.errorType());
                assertTrue(response.message().contains("exceeds max size"));
            }
        }
    }

    @Test
    void socketRpcServerRejectsRequestsWhenConcurrencyLimitIsSaturated() throws Exception {
        var descriptor = descriptor();
        var firstRequestEntered = new CountDownLatch(1);
        var blockedCommit = new CompletableFuture<ReplicatedLogCommit>();
        var handler = new RatisProcessAdapterRpcHandler(_ ->
                new BlockingPartitionClient(descriptor, firstRequestEntered, blockedCommit));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5),
                4096,
                1
        )) {
            server.start();
            try (var transport = new RatisProcessAdapterSocketRpcTransport(
                    server.boundAddress(),
                    Duration.ofSeconds(5)
            )) {
                var firstResponse = CompletableFuture.supplyAsync(() -> transport.send(
                        RatisProcessAdapterRpcRequest.propose(
                                "request-1",
                                descriptor.partitionId(),
                                new byte[]{1}
                        )).join());
                assertTrue(firstRequestEntered.await(5, TimeUnit.SECONDS));

                var busy = transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                        "request-2",
                        descriptor.partitionId(),
                        new byte[]{2}
                )).get(5, TimeUnit.SECONDS);

                assertFalse(busy.success());
                assertEquals("invalid", busy.requestId());
                assertEquals("BUSY", busy.errorType());

                blockedCommit.complete(new ReplicatedLogCommit(21, true));
                assertEquals(RatisProcessAdapterRpcResponse.commit("request-1", 21, true),
                        firstResponse.get(5, TimeUnit.SECONDS));
            }
        } finally {
            blockedCommit.complete(new ReplicatedLogCommit(21, true));
        }
    }

    @Test
    void socketRpcServerTimesOutBlockedHandler() throws Exception {
        var descriptor = descriptor();
        var firstRequestEntered = new CountDownLatch(1);
        var blockedCommit = new CompletableFuture<ReplicatedLogCommit>();
        var handler = new RatisProcessAdapterRpcHandler(_ ->
                new BlockingPartitionClient(descriptor, firstRequestEntered, blockedCommit));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofMillis(25)
        )) {
            server.start();
            try (var transport = new RatisProcessAdapterSocketRpcTransport(
                    server.boundAddress(),
                    Duration.ofSeconds(1)
            )) {
                var response = transport.send(RatisProcessAdapterRpcRequest.propose(
                        "request-1",
                        descriptor.partitionId(),
                        new byte[]{1}
                )).get(1, TimeUnit.SECONDS);

                assertTrue(firstRequestEntered.await(1, TimeUnit.SECONDS));
                assertFalse(response.success());
                assertEquals("request-1", response.requestId());
                assertEquals("ADAPTER_TIMEOUT", response.errorType());
                assertTrue(response.message().contains("timed out"));
            }
        } finally {
            blockedCommit.complete(new ReplicatedLogCommit(21, true));
        }
    }

    @Test
    void socketRpcServerCancelsLatePublishedPartitionOperationAfterTimeout() throws Exception {
        var descriptor = descriptor();
        var resolverEntered = new CountDownLatch(1);
        var releaseResolver = new CountDownLatch(1);
        var operationEntered = new CountDownLatch(1);
        var blockedCommit = new CompletableFuture<ReplicatedLogCommit>();
        var handler = new RatisProcessAdapterRpcHandler(_ -> {
            resolverEntered.countDown();
            awaitUninterruptibly(releaseResolver);
            return new BlockingPartitionClient(descriptor, operationEntered, blockedCommit);
        });
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofMillis(25)
        )) {
            server.start();
            try (var transport = new RatisProcessAdapterSocketRpcTransport(
                    server.boundAddress(),
                    Duration.ofSeconds(1)
            )) {
                var response = transport.send(RatisProcessAdapterRpcRequest.propose(
                        "request-1",
                        descriptor.partitionId(),
                        new byte[]{1}
                )).get(1, TimeUnit.SECONDS);

                assertTrue(resolverEntered.await(1, TimeUnit.SECONDS));
                assertFalse(response.success());
                assertEquals("request-1", response.requestId());
                assertEquals("ADAPTER_TIMEOUT", response.errorType());

                releaseResolver.countDown();
                assertTrue(operationEntered.await(1, TimeUnit.SECONDS));
                awaitCancellation(blockedCommit);
            }
        } finally {
            releaseResolver.countDown();
            blockedCommit.complete(new ReplicatedLogCommit(21, true));
        }
    }

    @Test
    void socketRpcServerFailsClosedWhenLatePublishedPartitionCancellationThrowsError() throws Exception {
        var descriptor = descriptor();
        var resolverEntered = new CountDownLatch(1);
        var releaseResolver = new CountDownLatch(1);
        var operationEntered = new CountDownLatch(1);
        var cancellationFailure = new AssertionError("injected partition cancellation failure");
        var blockedCommit = new ErroringCancellationFuture<ReplicatedLogCommit>(cancellationFailure);
        var handler = new RatisProcessAdapterRpcHandler(_ -> {
            resolverEntered.countDown();
            awaitUninterruptibly(releaseResolver);
            return new BlockingPartitionClient(descriptor, operationEntered, blockedCommit);
        });
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofMillis(25)
        )) {
            server.start();
            try (var transport = new RatisProcessAdapterSocketRpcTransport(
                    server.boundAddress(),
                    Duration.ofSeconds(1)
            )) {
                var response = transport.send(RatisProcessAdapterRpcRequest.propose(
                        "request-1",
                        descriptor.partitionId(),
                        new byte[]{1}
                )).get(1, TimeUnit.SECONDS);

                assertTrue(resolverEntered.await(1, TimeUnit.SECONDS));
                assertFalse(response.success());
                assertEquals("request-1", response.requestId());
                assertEquals("ADAPTER_TIMEOUT", response.errorType());

                releaseResolver.countDown();
                assertTrue(operationEntered.await(1, TimeUnit.SECONDS));
                var degraded = awaitDegradedHealth(server);
                assertTrue(blockedCommit.cancelCalled());
                assertFalse(server.isRunning());
                assertTrue(degraded.message().contains("response cancellation failed"));
            }
        } finally {
            releaseResolver.countDown();
            blockedCommit.complete(new ReplicatedLogCommit(21, true));
        }
    }

    @Test
    void socketRpcServerAppliesOneTimeoutBudgetAcrossReadAndHandlerInvocation() throws Exception {
        var descriptor = descriptor();
        var resolverEntered = new CountDownLatch(1);
        var releaseResolver = new CountDownLatch(1);
        var handler = new RatisProcessAdapterRpcHandler(_ -> {
            resolverEntered.countDown();
            awaitUninterruptibly(releaseResolver);
            return new RecordingPartitionClient(descriptor);
        });
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofMillis(500),
                4096,
                1
        )) {
            server.start();
            try (var socket = new Socket()) {
                socket.connect(server.boundAddress(), 1_000);
                socket.setSoTimeout(1_000);
                var requestBytes = RatisProcessAdapterRpcCodec.encodeRequest(
                        RatisProcessAdapterRpcRequest.queryLocal(
                                "request-1",
                                descriptor.partitionId(),
                                new byte[]{1}
                        )
                ).getBytes(StandardCharsets.UTF_8);
                var startNanos = System.nanoTime();
                socket.getOutputStream().write(requestBytes);
                socket.getOutputStream().flush();
                TimeUnit.MILLISECONDS.sleep(300);
                socket.shutdownOutput();

                var response = RatisProcessAdapterRpcCodec.decodeResponse(new String(
                        socket.getInputStream().readAllBytes(),
                        StandardCharsets.UTF_8
                ));
                var elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                assertTrue(resolverEntered.await(1, TimeUnit.SECONDS));
                assertFalse(response.success());
                assertEquals("request-1", response.requestId());
                assertEquals("ADAPTER_TIMEOUT", response.errorType());
                assertTrue(elapsedMillis < 700, "request retained capacity for " + elapsedMillis + " milliseconds");
            } finally {
                releaseResolver.countDown();
            }
        }
    }

    @Test
    void socketRpcServerCloseUsesOneTimeoutBudgetAcrossExecutorDrains() throws Exception {
        var descriptor = descriptor();
        var resolverEntered = new CountDownLatch(1);
        var releaseResolver = new CountDownLatch(1);
        var handler = new RatisProcessAdapterRpcHandler(_ -> {
            resolverEntered.countDown();
            awaitUninterruptibly(releaseResolver);
            return new RecordingPartitionClient(descriptor);
        });
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofMillis(200),
                4096,
                1
        );
        server.start();
        var transport = new RatisProcessAdapterSocketRpcTransport(server.boundAddress(), Duration.ofSeconds(1));
        var response = CompletableFuture.supplyAsync(() -> transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                "request-1",
                descriptor.partitionId(),
                new byte[]{1}
        )).join());
        try {
            assertTrue(resolverEntered.await(1, TimeUnit.SECONDS));

            var failure = assertTimeoutPreemptively(
                    Duration.ofMillis(450),
                    () -> assertThrows(IOException.class, server::close)
            );

            assertTrue(failure.getMessage().contains("operation executor"));
            assertInstanceOf(IOException.class, assertThrows(
                    ExecutionException.class,
                    () -> response.get(1, TimeUnit.SECONDS)
            ).getCause());
        } finally {
            releaseResolver.countDown();
            server.close();
        }
    }

    @Test
    void socketRpcServerCloseWaitsAcrossInterruptionAndRestoresInterrupt() throws Exception {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var workerEntered = new CountDownLatch(1);
        var releaseWorker = new CountDownLatch(1);
        var closeEntered = new CountDownLatch(1);
        var closeFailure = new AtomicReference<Throwable>();
        var interruptRestored = new AtomicBoolean();
        try {
            rpcOperationExecutor(server).execute(() -> {
                workerEntered.countDown();
                awaitUninterruptibly(releaseWorker);
            });
            assertTrue(workerEntered.await(1, TimeUnit.SECONDS));

            var closeThread = new Thread(() -> {
                closeEntered.countDown();
                try {
                    server.close();
                } catch (Throwable e) {
                    closeFailure.set(e);
                }
                if (Thread.currentThread().isInterrupted()) {
                    interruptRestored.set(true);
                }
            }, "ratis-rpc-server-close-test");
            closeThread.start();
            try {
                assertTrue(closeEntered.await(1, TimeUnit.SECONDS));
                TimeUnit.MILLISECONDS.sleep(50);
                assertTrue(closeThread.isAlive());
                closeThread.interrupt();
                TimeUnit.MILLISECONDS.sleep(50);
                assertTrue(closeThread.isAlive(), "close returned before interrupted worker shutdown completed");
            } finally {
                releaseWorker.countDown();
            }
            closeThread.join(1_000);

            assertFalse(closeThread.isAlive());
            assertNull(closeFailure.get());
            assertTrue(interruptRestored.get());
            assertTrue(rpcOperationExecutor(server).isTerminated());
        } finally {
            releaseWorker.countDown();
            Thread.interrupted();
            server.close();
        }
    }

    @Test
    void socketRpcServerCloseStillDrainsLaterExecutorsWhenAcceptShutdownThrowsError() throws Exception {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var shutdownFailure = new AssertionError("accept shutdown failed");
        var acceptExecutor = replaceRpcAcceptExecutor(server, new ShutdownErrorExecutorService(shutdownFailure));
        try {
            assertSame(shutdownFailure, assertThrows(AssertionError.class, server::close));
            assertTrue(rpcRequestExecutor(server).isShutdown());
            assertTrue(rpcOperationExecutor(server).isShutdown());
        } finally {
            replaceRpcAcceptExecutor(server, acceptExecutor);
            server.close();
        }
    }

    @Test
    void socketRpcServerCloseRetriesListenerAfterCloseThrowsError() throws Exception {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var closeFailure = new AssertionError("first listener close failed");
        var listener = new CloseOnceErrorServerSocket(closeFailure);
        try {
            installRpcListener(server, listener);

            assertSame(closeFailure, assertThrows(AssertionError.class, server::close));

            assertSame(listener, listener(server));
            server.close();
            assertEquals(2, listener.closeCalls());
            assertNull(listener(server));
        } finally {
            clearRpcListener(server);
            server.close();
        }
    }

    @Test
    void socketRpcServerCloseStillClosesSiblingAcceptedSocketsWhenSocketCloseThrowsError() throws Exception {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var sockets = List.of(
                new ErroringCloseSocket("first close failed"),
                new ErroringCloseSocket("second close failed"),
                new ErroringCloseSocket("third close failed")
        );
        try {
            acceptedSocketSet(server).addAll(sockets);

            assertThrows(AssertionError.class, server::close);

            assertTrue(sockets.stream().allMatch(ErroringCloseSocket::closeCalled));
            assertTrue(acceptedSocketSet(server).containsAll(sockets));
        } finally {
            acceptedSocketSet(server).removeAll(sockets);
            server.close();
        }
    }

    @Test
    void socketRpcServerCloseRetriesAcceptedSocketAfterCloseThrowsError() throws Exception {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var socket = new CloseOnceErrorSocket("first close failed");
        try {
            acceptedSocketSet(server).add(socket);

            assertThrows(AssertionError.class, server::close);

            assertTrue(acceptedSocketSet(server).contains(socket));
            server.close();
            assertEquals(2, socket.closeCalls());
            assertFalse(acceptedSocketSet(server).contains(socket));
        } finally {
            acceptedSocketSet(server).remove(socket);
            server.close();
        }
    }

    @Test
    void socketRpcServerCloseRetriesAcceptedSocketAfterCloseThrowsIOException() throws Exception {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var socket = new CloseOnceIOExceptionSocket("first close failed");
        try {
            acceptedSocketSet(server).add(socket);

            assertThrows(IOException.class, server::close);

            assertTrue(acceptedSocketSet(server).contains(socket));
            server.close();
            assertEquals(2, socket.closeCalls());
            assertFalse(acceptedSocketSet(server).contains(socket));
        } finally {
            acceptedSocketSet(server).remove(socket);
            server.close();
        }
    }

    @Test
    void socketRpcServerClosedRegistryRetriesRejectedSocketAfterCloseThrowsError() throws Throwable {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var socket = new CloseOnceErrorSocket("first rejected close failed");
        try {
            server.close();

            assertThrows(AssertionError.class, () -> rejectAcceptedSocketIfClosed(server, socket));

            assertTrue(acceptedSocketSet(server).contains(socket));
            server.close();
            assertEquals(2, socket.closeCalls());
            assertFalse(acceptedSocketSet(server).contains(socket));
        } finally {
            acceptedSocketSet(server).remove(socket);
            server.close();
        }
    }

    @Test
    void socketRpcServerClosedRegistryRetriesRejectedSocketAfterCloseThrowsIOException() throws Throwable {
        var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor())),
                Duration.ofSeconds(5)
        );
        var socket = new CloseOnceIOExceptionSocket("first rejected close failed");
        try {
            server.close();

            assertThrows(IOException.class, () -> rejectAcceptedSocketIfClosed(server, socket));

            assertTrue(acceptedSocketSet(server).contains(socket));
            server.close();
            assertEquals(2, socket.closeCalls());
            assertFalse(acceptedSocketSet(server).contains(socket));
        } finally {
            acceptedSocketSet(server).remove(socket);
            server.close();
        }
    }

    @Test
    void socketRpcServerTimesOutBlockedSynchronousHandlerInvocation() throws Exception {
        var descriptor = descriptor();
        var resolverEntered = new CountDownLatch(1);
        var releaseResolver = new CountDownLatch(1);
        var handler = new RatisProcessAdapterRpcHandler(_ -> {
            resolverEntered.countDown();
            awaitUninterruptibly(releaseResolver);
            return new RecordingPartitionClient(descriptor);
        });
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofMillis(25),
                4096,
                1
        )) {
            server.start();
            try (var transport = new RatisProcessAdapterSocketRpcTransport(
                    server.boundAddress(),
                    Duration.ofSeconds(1)
            )) {
                var response = transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                        "request-1",
                        descriptor.partitionId(),
                        new byte[]{1}
                )).get(1, TimeUnit.SECONDS);

                assertTrue(resolverEntered.await(1, TimeUnit.SECONDS));
                assertFalse(response.success());
                assertEquals("request-1", response.requestId());
                assertEquals("ADAPTER_TIMEOUT", response.errorType());
                assertTrue(response.message().contains("timed out"));
                var degraded = awaitDegradedHealth(server);
                assertTrue(degraded.message().contains("operation executor is saturated"));

                var busy = transport.send(RatisProcessAdapterRpcRequest.queryLocal(
                        "request-2",
                        descriptor.partitionId(),
                        new byte[]{2}
                )).get(1, TimeUnit.SECONDS);

                assertFalse(busy.success());
                assertEquals("request-2", busy.requestId());
                assertEquals("BUSY", busy.errorType());

                releaseResolver.countDown();
                assertTrue(awaitReadyHealth(server).isReady());
            } finally {
                releaseResolver.countDown();
            }
        }
    }

    @Test
    void socketRpcServerReturnsBadRequestForMalformedPayload() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            server.start();
            try (var socket = new Socket()) {
                socket.connect(server.boundAddress(), 5_000);
                socket.getOutputStream().write("malformed\n".getBytes(StandardCharsets.UTF_8));
                socket.shutdownOutput();

                var response = RatisProcessAdapterRpcCodec.decodeResponse(new String(
                        socket.getInputStream().readAllBytes(),
                        StandardCharsets.UTF_8
                ));

                assertFalse(response.success());
                assertEquals("invalid", response.requestId());
                assertEquals("BAD_REQUEST", response.errorType());
            }
        }
    }

    @Test
    void socketRpcServerReturnsBadRequestForMalformedUtf8() throws Exception {
        var handler = new RatisProcessAdapterRpcHandler(_ -> new RecordingPartitionClient(descriptor()));
        try (var server = new RatisProcessAdapterSocketRpcServer(
                new InetSocketAddress("127.0.0.1", 0),
                handler,
                Duration.ofSeconds(5)
        )) {
            server.start();
            try (var socket = new Socket()) {
                socket.connect(server.boundAddress(), 5_000);
                socket.getOutputStream().write(new byte[]{(byte) 0xC3});
                socket.shutdownOutput();

                var response = RatisProcessAdapterRpcCodec.decodeResponse(new String(
                        socket.getInputStream().readAllBytes(),
                        StandardCharsets.UTF_8
                ));

                assertFalse(response.success());
                assertEquals("invalid", response.requestId());
                assertEquals("BAD_REQUEST", response.errorType());
                assertTrue(response.message().contains("not valid UTF-8"));
            }
        }
    }

    private static final class BlockingPartitionClient implements RatisPartitionClient {

        private final RatisPartitionDescriptor descriptor;
        private final CountDownLatch firstRequestEntered;
        private final CompletableFuture<ReplicatedLogCommit> blockedCommit;

        private BlockingPartitionClient(RatisPartitionDescriptor descriptor,
                                        CountDownLatch firstRequestEntered,
                                        CompletableFuture<ReplicatedLogCommit> blockedCommit) {
            this.descriptor = descriptor;
            this.firstRequestEntered = firstRequestEntered;
            this.blockedCommit = blockedCommit;
        }

        @Override
        public RatisPartitionDescriptor descriptor() {
            return descriptor;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            firstRequestEntered.countDown();
            return blockedCommit;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            return CompletableFuture.completedFuture(request.clone());
        }

        @Override
        public CompletableFuture<byte[]> queryLocal(byte[] request) {
            return CompletableFuture.completedFuture(request.clone());
        }

        @Override
        public void close() {
        }
    }

    private static final class ErroringCancellationFuture<T> extends CompletableFuture<T> {

        private final Error cancellationFailure;
        private final AtomicBoolean cancelCalled = new AtomicBoolean();

        private ErroringCancellationFuture(Error cancellationFailure) {
            this.cancellationFailure = cancellationFailure;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelCalled.set(true);
            throw cancellationFailure;
        }

        private boolean cancelCalled() {
            return cancelCalled.get();
        }
    }

    private static void serveOversizedResponse(ServerSocket server) {
        try (var socket = server.accept()) {
            socket.getInputStream().readAllBytes();
            socket.getOutputStream()
                    .write(RatisProcessAdapterRpcCodec.encodeResponse(RatisProcessAdapterRpcResponse.payload(
                            "request-1",
                            new byte[512]
                    )).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void serveMalformedResponse(ServerSocket server) {
        try (var socket = server.accept()) {
            socket.getInputStream().readAllBytes();
            socket.getOutputStream().write("malformed".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void serveMalformedUtf8Response(ServerSocket server) {
        try (var socket = server.accept()) {
            socket.getInputStream().readAllBytes();
            socket.getOutputStream().write(0xC3);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void serveDripResponse(ServerSocket server) {
        try (var socket = server.accept()) {
            socket.getInputStream().readAllBytes();
            for (var next : "version=1\nstatus=".getBytes(StandardCharsets.UTF_8)) {
                socket.getOutputStream().write(next);
                socket.getOutputStream().flush();
                TimeUnit.MILLISECONDS.sleep(20);
            }
        } catch (IOException ignored) {
            // The transport closes its socket when the total response-read deadline expires.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void holdConnectionWithoutReading(ServerSocket server,
                                                     CountDownLatch accepted,
                                                     CountDownLatch releaseConnection) {
        try (var socket = server.accept()) {
            socket.setReceiveBufferSize(1024);
            accepted.countDown();
            if (!releaseConnection.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to release blocked request connection");
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static RatisProcessAdapterRpcRequest serveOneRpcRequestAfterHoldingFirst(ServerSocket server,
                                                                                     CountDownLatch firstAccepted) {
        try (var ignored = server.accept()) {
            firstAccepted.countDown();
            return serveOneRpcRequest(server);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("BusyWait")
    private static RatisProcessAdapterRpcResponse sendWhenClientCapacityIsAvailable(
            RatisProcessAdapterSocketRpcTransport transport,
            RatisProcessAdapterRpcRequest request) throws Exception {
        var timeoutNanos = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < timeoutNanos) {
            try {
                return transport.send(request).get(1, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof IOException failure)
                        || !failure.getMessage().contains("client executor is saturated")) {
                    throw e;
                }
                Thread.sleep(1);
            }
        }
        throw new AssertionError("Canceled RPC request did not release client executor capacity");
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        var interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("BusyWait")
    private static void awaitCancellation(CompletableFuture<?> future) throws InterruptedException {
        var timeoutNanos = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < timeoutNanos) {
            if (future.isCancelled()) {
                return;
            }
            Thread.sleep(1);
        }
        throw new AssertionError("Timed-out RPC request did not cancel late-published partition operation");
    }

    private static ServerSocket listener(RatisProcessAdapterSocketRpcServer server) throws Exception {
        var field = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("serverSocket");
        field.setAccessible(true);
        return (ServerSocket) field.get(server);
    }

    private static Throwable invokeRpcServe(RatisProcessAdapterSocketRpcServer server, Socket socket) throws Exception {
        var method = RatisProcessAdapterSocketRpcServer.class.getDeclaredMethod("serve", Socket.class, long.class);
        method.setAccessible(true);
        return assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(server, socket, System.nanoTime())
        ).getCause();
    }

    private static RatisProcessAdapterRpcResponse invokeRpcHandle(RatisProcessAdapterSocketRpcServer server,
                                                                  RatisProcessAdapterRpcRequest request)
            throws Throwable {
        var method = RatisProcessAdapterSocketRpcServer.class.getDeclaredMethod("handle", String.class, long.class);
        method.setAccessible(true);
        try {
            return (RatisProcessAdapterRpcResponse) method.invoke(
                    server,
                    RatisProcessAdapterRpcCodec.encodeRequest(request),
                    System.nanoTime()
            );
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static Throwable invokeRpcRejectBusy(RatisProcessAdapterSocketRpcServer server, Socket socket)
            throws Exception {
        var method = RatisProcessAdapterSocketRpcServer.class.getDeclaredMethod("rejectBusy", Socket.class);
        method.setAccessible(true);
        return assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(server, socket)
        ).getCause();
    }

    private static void invokeRpcCloseAfterFailedHandoff(RatisProcessAdapterSocketRpcServer server,
                                                         Socket socket,
                                                         Throwable failure) throws Exception {
        var method = RatisProcessAdapterSocketRpcServer.class.getDeclaredMethod(
                "closeAfterFailedHandoff",
                Socket.class,
                Throwable.class
        );
        method.setAccessible(true);
        method.invoke(server, socket, failure);
    }

    private static void invokeRpcFailRequestWorker(RatisProcessAdapterSocketRpcServer server,
                                                   IOException failure,
                                                   ServerSocket listener) throws Exception {
        installRpcListener(server, listener);
        var method = RatisProcessAdapterSocketRpcServer.class.getDeclaredMethod("failRequestWorker", IOException.class);
        method.setAccessible(true);
        method.invoke(server, failure);
    }

    private static void invokeRpcFailAcceptLoop(RatisProcessAdapterSocketRpcServer server,
                                                IOException failure,
                                                ServerSocket listener) throws Exception {
        installRpcListener(server, listener);
        var method = RatisProcessAdapterSocketRpcServer.class.getDeclaredMethod(
                "failAcceptLoop",
                ServerSocket.class,
                IOException.class
        );
        method.setAccessible(true);
        method.invoke(server, listener, failure);
    }

    private static Throwable invokeRpcStartListener(RatisProcessAdapterSocketRpcServer server,
                                                    ServerSocket listener) throws Exception {
        var method = RatisProcessAdapterSocketRpcServer.class.getDeclaredMethod("startListener", ServerSocket.class);
        method.setAccessible(true);
        return assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(server, listener)
        ).getCause();
    }

    private static void installRpcListener(RatisProcessAdapterSocketRpcServer server, ServerSocket listener)
            throws Exception {
        var listenerField = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("serverSocket");
        listenerField.setAccessible(true);
        listenerField.set(server, listener);
        var runningField = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.setBoolean(server, true);
    }

    private static void clearRpcListener(RatisProcessAdapterSocketRpcServer server) throws Exception {
        var field = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("serverSocket");
        field.setAccessible(true);
        field.set(server, null);
    }

    private static Throwable invokeSocketWireWriteUtf8(Socket socket) throws Exception {
        var wire = Class.forName("it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterSocketWire");
        var method = wire.getDeclaredMethod(
                "writeUtf8",
                Socket.class,
                byte[].class,
                long.class,
                int.class,
                String.class
        );
        method.setAccessible(true);
        return assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(null, socket, new byte[]{1}, System.nanoTime(), 10, "test socket write")
        ).getCause();
    }

    private static Throwable invokeSocketWireBindAndStartListener(ServerSocket listener,
                                                                  Consumer<ServerSocket> startListener)
            throws Exception {
        var wire = Class.forName("it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterSocketWire");
        var method = wire.getDeclaredMethod(
                "bindAndStartListener",
                ServerSocket.class,
                InetSocketAddress.class,
                Consumer.class
        );
        method.setAccessible(true);
        return assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(null, listener, new InetSocketAddress("127.0.0.1", 0), startListener)
        ).getCause();
    }

    private static ThreadPoolExecutor requestExecutor(RatisProcessAdapterSocketRpcTransport transport)
            throws Exception {
        var field = RatisProcessAdapterSocketRpcTransport.class.getDeclaredField("requestExecutor");
        field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(transport);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<RatisProcessAdapterRpcResponse> cancellationAwareResponse(
            RatisProcessAdapterSocketRpcTransport transport,
            Socket socket)
            throws Exception {
        var method = RatisProcessAdapterSocketRpcTransport.class.getDeclaredMethod(
                "cancellationAwareResponse",
                AtomicReference.class
        );
        method.setAccessible(true);
        return (CompletableFuture<RatisProcessAdapterRpcResponse>) method.invoke(transport, new AtomicReference<>(socket));
    }

    @SuppressWarnings("unchecked")
    private static Set<Socket> pendingSocketCleanupSet(RatisProcessAdapterSocketRpcTransport transport)
            throws Exception {
        var field = RatisProcessAdapterSocketRpcTransport.class.getDeclaredField("pendingSocketCleanup");
        field.setAccessible(true);
        return (Set<Socket>) field.get(transport);
    }

    private static ThreadPoolExecutor replaceTransportRequestExecutor(RatisProcessAdapterSocketRpcTransport transport,
                                                                      ThreadPoolExecutor replacement) throws Exception {
        var field = RatisProcessAdapterSocketRpcTransport.class.getDeclaredField("requestExecutor");
        field.setAccessible(true);
        var previous = (ThreadPoolExecutor) field.get(transport);
        field.set(transport, replacement);
        return previous;
    }

    @SuppressWarnings("unchecked")
    private static Set<Object> pendingRequestSet(RatisProcessAdapterSocketRpcTransport transport) throws Exception {
        var field = RatisProcessAdapterSocketRpcTransport.class.getDeclaredField("pendingRequests");
        field.setAccessible(true);
        return (Set<Object>) field.get(transport);
    }

    private static Object newPendingRequest(CompletableFuture<RatisProcessAdapterRpcResponse> response,
                                            Socket activeSocket) throws Exception {
        var type = Class.forName(RatisProcessAdapterSocketRpcTransport.class.getName() + "$PendingRequest");
        var constructor = type.getDeclaredConstructor(CompletableFuture.class, AtomicReference.class);
        constructor.setAccessible(true);
        return constructor.newInstance(response, new AtomicReference<>(activeSocket));
    }

    private static ThreadPoolExecutor rpcOperationExecutor(RatisProcessAdapterSocketRpcServer server)
            throws Exception {
        var field = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("operationExecutor");
        field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(server);
    }

    private static ThreadPoolExecutor replaceRpcOperationExecutor(RatisProcessAdapterSocketRpcServer server,
                                                                  ThreadPoolExecutor replacement) throws Exception {
        var field = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("operationExecutor");
        field.setAccessible(true);
        var previous = (ThreadPoolExecutor) field.get(server);
        field.set(server, replacement);
        return previous;
    }

    private static ThreadPoolExecutor rpcRequestExecutor(RatisProcessAdapterSocketRpcServer server)
            throws Exception {
        var field = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("requestExecutor");
        field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(server);
    }

    private static ExecutorService replaceRpcRequestExecutor(RatisProcessAdapterSocketRpcServer server,
                                                             ExecutorService replacement) throws Exception {
        var field = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("requestExecutor");
        field.setAccessible(true);
        var previous = (ExecutorService) field.get(server);
        field.set(server, replacement);
        return previous;
    }

    private static ExecutorService replaceRpcAcceptExecutor(RatisProcessAdapterSocketRpcServer server,
                                                            ExecutorService replacement) throws Exception {
        var field = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("acceptExecutor");
        field.setAccessible(true);
        var previous = (ExecutorService) field.get(server);
        field.set(server, replacement);
        return previous;
    }

    @SuppressWarnings("unchecked")
    private static Set<Socket> acceptedSocketSet(RatisProcessAdapterSocketRpcServer server) throws Exception {
        var acceptedSockets = acceptedSockets(server);
        var sockets = acceptedSockets.getClass().getDeclaredField("sockets");
        sockets.setAccessible(true);
        return (Set<Socket>) sockets.get(acceptedSockets);
    }

    private static Object acceptedSockets(RatisProcessAdapterSocketRpcServer server) throws Exception {
        var field = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("acceptedSockets");
        field.setAccessible(true);
        return field.get(server);
    }

    private static boolean rejectAcceptedSocketIfClosed(RatisProcessAdapterSocketRpcServer server,
                                                        Socket socket) throws Throwable {
        var acceptedSockets = acceptedSockets(server);
        var method = acceptedSockets.getClass().getDeclaredMethod("rejectIfClosed", Socket.class);
        method.setAccessible(true);
        try {
            return (boolean) method.invoke(acceptedSockets, socket);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @SuppressWarnings("BusyWait")
    private static void awaitAcceptedSocket(RatisProcessAdapterSocketRpcServer server) throws Exception {
        var field = RatisProcessAdapterSocketRpcServer.class.getDeclaredField("acceptedSockets");
        field.setAccessible(true);
        var acceptedSockets = field.get(server);
        var size = acceptedSockets.getClass().getDeclaredMethod("size");
        size.setAccessible(true);
        var timeoutNanos = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (System.nanoTime() < timeoutNanos) {
            if ((int) size.invoke(acceptedSockets) > 0) {
                return;
            }
            Thread.sleep(1);
        }
        throw new AssertionError("RPC server did not track the accepted socket");
    }

    private static void assertPeerDisconnected(Socket socket) {
        assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
            try {
                assertEquals(-1, socket.getInputStream().read());
            } catch (SocketTimeoutException e) {
                throw e;
            } catch (IOException ignored) {
                // A reset is also a prompt peer disconnect.
            }
        });
    }

    @SuppressWarnings("BusyWait")
    private static RatisProcessAdapterHealth awaitDegradedHealth(
            RatisProcessAdapterSocketRpcServer server) throws InterruptedException {
        var timeoutNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < timeoutNanos) {
            var health = server.health();
            if (!health.isReady()) {
                return health;
            }
            Thread.sleep(1);
        }
        throw new AssertionError("RPC server health did not become degraded");
    }

    @SuppressWarnings("BusyWait")
    private static RatisProcessAdapterHealth awaitReadyHealth(
            RatisProcessAdapterSocketRpcServer server) throws InterruptedException {
        var timeoutNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < timeoutNanos) {
            var health = server.health();
            if (health.isReady()) {
                return health;
            }
            Thread.sleep(1);
        }
        throw new AssertionError("RPC server health did not become ready");
    }

    private static RatisProcessAdapterRpcRequest serveOneRpcRequest(ServerSocket server) {
        try (var socket = server.accept()) {
            var request = RatisProcessAdapterRpcCodec.decodeRequest(new String(
                    socket.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8
            ));
            socket.getOutputStream()
                    .write(RatisProcessAdapterRpcCodec.encodeResponse(RatisProcessAdapterRpcResponse.payload(
                            request.requestId(),
                            new byte[]{7, 8}
                    )).getBytes(StandardCharsets.UTF_8));
            return request;
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static Throwable joinFailure(CompletableFuture<?> future) {
        return assertThrows(CompletionException.class, future::join).getCause();
    }

    private static final class ErroringSocket extends Socket {

        private final AssertionError failure;

        private ErroringSocket(AssertionError failure) {
            this.failure = failure;
        }

        @Override
        public InputStream getInputStream() {
            throw failure;
        }
    }

    private static final class WorkerCloseErrorSocket extends Socket {

        private final AssertionError closeFailure;

        private WorkerCloseErrorSocket(AssertionError closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public OutputStream getOutputStream() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public void close() {
            throw closeFailure;
        }
    }

    private static RatisPartitionDescriptor descriptor() {
        return new RatisPartitionDescriptor(
                "partition-a",
                "group-a",
                "node-a",
                List.of(new RatisPeerDescriptor(
                        "node-a",
                        "grpc://127.0.0.1:19001",
                        "rack-a"
                )),
                Path.of("/tmp/rockserver-partition-a")
        );
    }

    private static void assertAdapterError(RatisProcessAdapterRpcResponse response,
                                           String requestId,
                                           String message) {
        assertFalse(response.success());
        assertEquals(requestId, response.requestId());
        assertEquals("ADAPTER_ERROR", response.errorType());
        assertEquals(message, response.message());
    }

    private static void assertRejectsNull(String expectedMessage, Executable executable) {
        var error = assertThrows(IllegalArgumentException.class, executable);
        assertEquals(expectedMessage, error.getMessage());
    }

    private record HandlerTransport(RatisProcessAdapterRpcHandler handler)
            implements RatisProcessAdapterRpcTransport {

        private HandlerTransport {
            if (handler == null) {
                throw new IllegalArgumentException("handler must not be null");
            }
        }

        @Override
        public CompletableFuture<RatisProcessAdapterRpcResponse> send(RatisProcessAdapterRpcRequest request) {
            return handler.handle(request);
        }
    }

    private static final class RecordingPartitionClient implements RatisPartitionClient {

        private final RatisPartitionDescriptor descriptor;
        private byte[] proposedPayload;
        private long waitedIndex;
        private byte[] linearizablePayload;
        private byte[] stalePayload;
        private long minAppliedIndex;
        private byte[] localPayload;

        private RecordingPartitionClient(RatisPartitionDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public RatisPartitionDescriptor descriptor() {
            return descriptor;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            proposedPayload = payload.clone();
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(11, true, new byte[]{13, 14}));
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            waitedIndex = index;
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            linearizablePayload = request.clone();
            return CompletableFuture.completedFuture(new byte[]{4, 5});
        }

        @Override
        public CompletableFuture<byte[]> queryStale(byte[] request, long minAppliedIndex) {
            stalePayload = request.clone();
            this.minAppliedIndex = minAppliedIndex;
            return CompletableFuture.completedFuture(new byte[]{6, 7});
        }

        @Override
        public CompletableFuture<byte[]> queryLocal(byte[] request) {
            localPayload = request.clone();
            return CompletableFuture.completedFuture(new byte[]{10});
        }

        @Override
        public void close() {
        }
    }

    private static final class NullResultPartitionClient implements RatisPartitionClient {

        private final RatisPartitionDescriptor descriptor;

        private NullResultPartitionClient(RatisPartitionDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public RatisPartitionDescriptor descriptor() {
            return descriptor;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<byte[]> queryStale(byte[] request, long minAppliedIndex) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<byte[]> queryLocal(byte[] request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
        }
    }

    private static final class FatalOperationPartitionClient implements RatisPartitionClient {

        private final RatisPartitionDescriptor descriptor;
        private final Throwable failure;

        private FatalOperationPartitionClient(RatisPartitionDescriptor descriptor, Throwable failure) {
            this.descriptor = descriptor;
            this.failure = failure;
        }

        @Override
        public RatisPartitionDescriptor descriptor() {
            return descriptor;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            return failed();
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return failed();
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            return failed();
        }

        @Override
        public CompletableFuture<byte[]> queryStale(byte[] request, long minAppliedIndex) {
            return failed();
        }

        @Override
        public CompletableFuture<byte[]> queryLocal(byte[] request) {
            return failed();
        }

        @Override
        public void close() {
        }

        private <T> CompletableFuture<T> failed() {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private static final class FailingSubmitThreadPoolExecutor extends ThreadPoolExecutor {

        private final Throwable failure;

        private FailingSubmitThreadPoolExecutor(Throwable failure) {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
            this.failure = failure;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            var response = new CompletableFuture<T>();
            response.completeExceptionally(failure);
            return response;
        }
    }

    private static final class UnsupportedPartitionClient implements RatisPartitionClient {

        private final RatisPartitionDescriptor descriptor;

        private UnsupportedPartitionClient(RatisPartitionDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public RatisPartitionDescriptor descriptor() {
            return descriptor;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            return CompletableFuture.failedFuture(new RatisFeatureUnavailableException("propose disabled"));
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.failedFuture(new RatisFeatureUnavailableException("wait disabled"));
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            return CompletableFuture.failedFuture(new RatisFeatureUnavailableException("linearizable disabled"));
        }

        @Override
        public CompletableFuture<byte[]> queryLocal(byte[] request) {
            return CompletableFuture.failedFuture(new RatisFeatureUnavailableException("local disabled"));
        }

        @Override
        public void close() {
        }
    }

    private static final class ErroringExecutorService extends AbstractExecutorService {

        private final Error failure;

        private ErroringExecutorService(Error failure) {
            this.failure = failure;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public void execute(Runnable command) {
            throw failure;
        }
    }

    private static final class ErroringThreadPoolExecutor extends ThreadPoolExecutor {

        private final Error failure;

        private ErroringThreadPoolExecutor(Error failure) {
            super(0, 1, 1, TimeUnit.SECONDS, new SynchronousQueue<>());
            this.failure = failure;
        }

        @Override
        public void execute(Runnable command) {
            throw failure;
        }
    }

    private static final class ShutdownErrorExecutorService extends AbstractExecutorService {

        private final Error failure;

        private ShutdownErrorExecutorService(Error failure) {
            this.failure = failure;
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw failure;
        }

        @Override
        public boolean isShutdown() {
            return true;
        }

        @Override
        public boolean isTerminated() {
            return true;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(Runnable command) {
            throw new AssertionError("unexpected execute");
        }
    }

    private static final class ErroringCloseSocket extends Socket {

        private final String message;
        private boolean closeCalled;

        private ErroringCloseSocket(String message) {
            this.message = message;
        }

        @Override
        public void close() {
            closeCalled = true;
            throw new AssertionError(message);
        }

        private boolean closeCalled() {
            return closeCalled;
        }
    }

    private static final class TimeoutCloseErrorSocket extends Socket {

        private final AssertionError closeFailure;
        private final CountDownLatch closeAttempted = new CountDownLatch(1);

        private TimeoutCloseErrorSocket(AssertionError closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public OutputStream getOutputStream() {
            return new OutputStream() {
                @Override
                public void write(int ignored) {
                    awaitUninterruptibly(closeAttempted);
                    try {
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            };
        }

        @Override
        public void close() {
            closeAttempted.countDown();
            throw closeFailure;
        }
    }

    private static final class BlockingTimeoutCloseErrorSocket extends Socket {

        private final AssertionError closeFailure;
        private final CountDownLatch closeAttempted = new CountDownLatch(1);
        private final CountDownLatch releaseClose = new CountDownLatch(1);
        private final CountDownLatch closeFinished = new CountDownLatch(1);

        private BlockingTimeoutCloseErrorSocket(AssertionError closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public OutputStream getOutputStream() {
            return new OutputStream() {
                @Override
                public void write(int ignored) {
                    awaitUninterruptibly(closeAttempted);
                }
            };
        }

        @Override
        public void close() {
            closeAttempted.countDown();
            awaitUninterruptibly(releaseClose);
            closeFinished.countDown();
            throw closeFailure;
        }
    }

    private static final class ErroringCloseServerSocket extends ServerSocket {

        private final AssertionError closeFailure;

        private ErroringCloseServerSocket(AssertionError closeFailure) throws IOException {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            throw closeFailure;
        }
    }

    private static final class ClosingErrorServerSocket extends ServerSocket {

        private final AssertionError closeFailure;
        private boolean closeCalled;

        private ClosingErrorServerSocket(AssertionError closeFailure) throws IOException {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() throws IOException {
            closeCalled = true;
            super.close();
            throw closeFailure;
        }

        private boolean closeCalled() {
            return closeCalled;
        }
    }

    private static final class CloseOnceErrorSocket extends Socket {

        private final String message;
        private int closeCalls;

        private CloseOnceErrorSocket(String message) {
            this.message = message;
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            if (closeCalls == 1) {
                throw new AssertionError(message);
            }
            super.close();
        }

        private int closeCalls() {
            return closeCalls;
        }
    }

    private static final class CloseOnceIOExceptionSocket extends Socket {

        private final String message;
        private int closeCalls;

        private CloseOnceIOExceptionSocket(String message) {
            this.message = message;
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            if (closeCalls == 1) {
                throw new IOException(message);
            }
            super.close();
        }

        private int closeCalls() {
            return closeCalls;
        }
    }

    private static final class ErroringIOExceptionSocket extends Socket {

        private final String message;
        private int closeCalls;

        private ErroringIOExceptionSocket(String message) {
            this.message = message;
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            throw new IOException(message);
        }

        private int closeCalls() {
            return closeCalls;
        }
    }

    private static final class CloseOnceErrorServerSocket extends ServerSocket {

        private final AssertionError closeFailure;
        private int closeCalls;

        private CloseOnceErrorServerSocket(AssertionError closeFailure) throws IOException {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            if (closeCalls == 1) {
                throw closeFailure;
            }
            super.close();
        }

        private int closeCalls() {
            return closeCalls;
        }
    }
}
