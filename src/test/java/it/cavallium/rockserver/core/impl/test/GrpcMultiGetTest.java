package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.api.proto.GetMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.KeyTuple;
import it.cavallium.rockserver.core.common.api.proto.RangeBudget;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@Timeout(180)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class GrpcMultiGetTest {

    @TempDir
    Path directory;

    @Test
    void stockClientDecodesPinnedPagesIncludingWithLegacySingleGetStrategy() throws Exception {
        String property = "rockserver.grpc.fast-get.strategy";
        String previous = System.getProperty(property);
        try (var embedded = openEmbedded()) {
            var api = embedded.getSyncApi(RequestContext.batch());
            long column = api.createColumn("messages", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            byte[] value = new byte[]{3, 4, 5, 6, 7, 8, 9, 10};
            api.put(0, column, key(1), Buf.wrap(value), RequestType.none());
            api.put(0, column, key(3), Buf.wrap(new byte[0]), RequestType.none());
            var expected = Arrays.asList(Buf.wrap(value), null, Buf.wrap(new byte[0]), Buf.wrap(value));
            var keys = List.of(1, 2, 3, 1);
            for (String strategy : List.of("legacy", "pinned")) {
                System.setProperty(property, strategy);
                try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
                    server.start();
                    var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                            .usePlaintext().build();
                    try {
                        var stub = RocksDBServiceGrpc.newBlockingStub(channel).withDeadlineAfter(20, TimeUnit.SECONDS);
                        var complete = stub.getMulti(request(column, keys, 256));
                        assertEquals(keys.size(), complete.getValuesCount());
                        assertFalse(complete.getHasMore());
                        assertTrue(complete.getValues(2).hasValue(), "empty values must remain present");
                        assertFalse(complete.getValues(1).hasValue(), "missing keys must retain their slot");
                        assertEquals(expected, values(complete));

                        var actual = new ArrayList<Buf>();
                        do {
                            var remaining = keys.subList(actual.size(), keys.size());
                            var response = stub.getMulti(request(column, remaining, 56));
                            var page = new MultiGetPage(values(response), response.getHasMore());
                            page.validateResponse(remaining.size(),
                                    new it.cavallium.rockserver.core.common.RangeBudget(remaining.size(), 56));
                            assertFalse(page.values().isEmpty());
                            assertEquals(expected.subList(actual.size(), actual.size() + page.values().size()), page.values());
                            actual.addAll(page.values());
                            assertEquals(actual.size() < keys.size(), page.hasMore());
                        } while (actual.size() < keys.size());
                        assertEquals(expected, actual);
                        // A second full request exercises fresh ownership after all page owners closed.
                        assertEquals(expected, values(stub.getMulti(request(column, keys, 256))));
                    } finally {
                        channel.shutdownNow();
                        assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
                    }
                }
            }
            assertEquals(0, embedded.getInternalDB().getPendingOpsCount());
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    @Test
    void cancellationAtResponseHeadersReleasesPinnedPage() throws Exception {
        try (var embedded = openEmbedded()) {
            var api = embedded.getSyncApi(RequestContext.batch());
            long column = api.createColumn("messages", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            api.put(0, column, key(1), Buf.wrap(new byte[1024 * 1024]), RequestType.none());
            try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
                server.start();
                var channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                        .usePlaintext().maxInboundMessageSize(2 * 1024 * 1024).build();
                try {
                    CountDownLatch headers = new CountDownLatch(1);
                    CountDownLatch closed = new CountDownLatch(1);
                    AtomicReference<Status> status = new AtomicReference<>();
                    var call = channel.newCall(RocksDBServiceGrpc.getGetMultiMethod(),
                            CallOptions.DEFAULT.withDeadlineAfter(20, TimeUnit.SECONDS));
                    call.start(new ClientCall.Listener<>() {
                        @Override
                        public void onHeaders(Metadata metadata) {
                            headers.countDown();
                            call.cancel("cancel pinned MultiGet response", null);
                        }

                        @Override
                        public void onClose(Status result, Metadata trailers) {
                            status.set(result);
                            closed.countDown();
                        }
                    }, new Metadata());
                    call.request(1);
                    call.sendMessage(request(column, List.of(1), 2 * 1024 * 1024));
                    call.halfClose();
                    assertTrue(headers.await(10, TimeUnit.SECONDS));
                    assertTrue(closed.await(10, TimeUnit.SECONDS));
                    assertEquals(Status.Code.CANCELLED, status.get().getCode());
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (embedded.getInternalDB().getPendingOpsCount() != 0 && System.nanoTime() < deadline) {
                        Thread.sleep(10);
                    }
                    assertEquals(0, embedded.getInternalDB().getPendingOpsCount());
                    api.deleteColumn(column);
                } finally {
                    channel.shutdownNow();
                    assertTrue(channel.awaitTermination(10, TimeUnit.SECONDS));
                }
            }
        }
    }

    private EmbeddedConnection openEmbedded() throws Exception {
        Path config = directory.resolve("multi-get.conf");
        Files.writeString(config, """
                database: {
                  global: {
                    enable-fast-get: true
                    ingest-behind: false
                    optimistic: false
                    use-direct-io: false
                    maximum-open-files: -1
                    fallback-column-options: { cache-index-and-filter-blocks: true }
                  }
                }
                """);
        return new EmbeddedConnection(directory.resolve("db"), "grpc-multi-get", config);
    }

    private static List<Buf> values(it.cavallium.rockserver.core.common.api.proto.MultiGetPage page) {
        var values = new ArrayList<Buf>();
        for (var slot : page.getValuesList()) {
            values.add(slot.hasValue() ? Buf.wrap(slot.getValue().toByteArray()) : null);
        }
        return values;
    }

    private static GetMultiRequest request(long column, List<Integer> keys, long maxBytes) {
        var request = GetMultiRequest.newBuilder().setColumnId(column)
                .setBudget(RangeBudget.newBuilder().setMaxItems(keys.size()).setMaxBytes(maxBytes))
                .setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
                        .setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
                        .setWorkloadContractVersion(3).setTimeoutNanos(Long.MAX_VALUE));
        for (int key : keys) {
            request.addKeysMulti(KeyTuple.newBuilder().addKeys(ByteString.copyFrom(new byte[]{(byte) key})));
        }
        return request.build();
    }

    private static Keys key(int value) {
        return new Keys(Buf.wrap(new byte[]{(byte) value}));
    }
}
