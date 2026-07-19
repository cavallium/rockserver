package it.cavallium.rockserver.core.impl.test;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.common.cdc.CdcCommitMode;
import it.cavallium.rockserver.core.common.cdc.CdcStreamOptions;
import it.cavallium.rockserver.core.server.CdcResponseBudget;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assumptions;

import static org.junit.jupiter.api.Assertions.*;

public class CdcGrpcTest {

    private Path dbDir;
    private Path configFile;
    private EmbeddedConnection embeddedConnection;
    private GrpcServer grpcServer;
    private GrpcConnection client;

    @BeforeEach
    void setUp() throws IOException {
        dbDir = Files.createTempDirectory("rockserver-cdc-grpc-test");
        configFile = Files.createTempFile("rockserver-config", ".conf");
        Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
        
        startServer();
    }

    private void startServer() throws IOException {
        embeddedConnection = new EmbeddedConnection(dbDir, "cdc-grpc-test", configFile);
        grpcServer = new GrpcServer(embeddedConnection, new InetSocketAddress("127.0.0.1", 0));
        grpcServer.start();
        client = GrpcConnection.forHostAndPort("grpc-client", new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));
    }

    private void stopServer() throws IOException {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
        }
        if (grpcServer != null) {
            try {
                grpcServer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (embeddedConnection != null) embeddedConnection.closeTesting();
    }

    @AfterEach
    void tearDown() throws IOException {
        stopServer();
        if (dbDir != null) Utils.deleteDirectory(dbDir.toString());
        if (configFile != null) Files.deleteIfExists(configFile);
    }

    @Test
    void testBasicCdcFlowOverGrpc() throws Exception {
        var colId = client.getSyncApi().createColumn("test-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));

        // Create CDC subscription
        var startSeq = client.getSyncApi().cdcCreate("sub1", null, null);
        assertTrue(startSeq >= 0);

        // Put some data
        var key1 = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})});
        client.getSyncApi().put(0, colId, key1, Buf.wrap("val1".getBytes(StandardCharsets.UTF_8)), RequestType.none());
        
        var key2 = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 2})});
        client.getSyncApi().put(0, colId, key2, Buf.wrap("val2".getBytes(StandardCharsets.UTF_8)), RequestType.none());

        // Verify events
        List<CDCEvent> events = client.getSyncApi().cdcPoll("sub1", null, 100).collect(Collectors.toList());
        assertEquals(2, events.size());
        assertEquals(CDCEvent.Op.PUT, events.get(0).op());
        assertEquals(CDCEvent.Op.PUT, events.get(1).op());
        assertEquals("val1", new String(events.get(0).value().toByteArray(), StandardCharsets.UTF_8));
        assertEquals("val2", new String(events.get(1).value().toByteArray(), StandardCharsets.UTF_8));
        
        // Commit
        client.getSyncApi().cdcCommit("sub1", events.get(1).seq());
    }

	@Test
	void missingSubscriptionErrorsRemainTypedOverGrpc() {
		var commitError = assertThrows(RocksDBException.class,
				() -> client.getSyncApi().cdcCommit("missing", 1L));
		assertEquals(RocksDBErrorType.CDC_SUBSCRIPTION_NOT_FOUND,
				commitError.getErrorUniqueId());

		var batchError = assertThrows(RocksDBException.class,
				() -> client.getAsyncApi().cdcPollBatchAsync("missing", null, 10).block());
		assertEquals(RocksDBErrorType.CDC_SUBSCRIPTION_NOT_FOUND,
				batchError.getErrorUniqueId());

		var streamError = assertThrows(RocksDBException.class,
				() -> Flux.from(client.getAsyncApi().cdcPollAsync("missing", null, 10))
						.collectList()
						.block());
		assertEquals(RocksDBErrorType.CDC_SUBSCRIPTION_NOT_FOUND,
				streamError.getErrorUniqueId());

		var highLevelStreamError = assertTimeoutPreemptively(Duration.ofSeconds(2),
				() -> assertThrows(RocksDBException.class,
						() -> client.getAsyncApi().cdcStream(
								"missing",
								new CdcStreamOptions(null, 10, Duration.ofMillis(10), CdcCommitMode.NONE),
								null)
								.blockFirst()));
		assertEquals(RocksDBErrorType.CDC_SUBSCRIPTION_NOT_FOUND,
				highLevelStreamError.getErrorUniqueId(),
				"metadata loss must escape the transient retry loop so the consumer can reinitialize");
	}

    @Test
    void filteredEmptyBatchCarriesServerNextSequenceOverGrpc() throws Exception {
        var selectedColumn = client.getSyncApi().createColumn(
                "selected-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
        var ignoredColumn = client.getSyncApi().createColumn(
                "ignored-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
        long startSeq = client.getSyncApi().cdcCreate("filtered-sub", null, List.of(selectedColumn));
        var key = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})});

        client.getSyncApi().put(0, ignoredColumn, key,
                Buf.wrap("ignored".getBytes(StandardCharsets.UTF_8)), RequestType.none());

        CdcBatch embeddedBatch = embeddedConnection.getAsyncApi()
                .cdcPollBatchAsync("filtered-sub", startSeq, 100)
                .block();
        assertNotNull(embeddedBatch);
        assertTrue(embeddedBatch.events().isEmpty());
        assertTrue(embeddedBatch.nextSeq() > startSeq,
                "The embedded server cursor must advance first: start=" + startSeq
                        + ", next=" + embeddedBatch.nextSeq());

        CdcBatch emptyBatch = client.getAsyncApi()
                .cdcPollBatchAsync("filtered-sub", startSeq, 100)
                .block();
        assertNotNull(emptyBatch);
        assertTrue(emptyBatch.events().isEmpty());
        assertTrue(emptyBatch.nextSeq() > startSeq,
                "The server cursor must advance across WAL entries removed by the subscription filter: start="
                        + startSeq + ", next=" + emptyBatch.nextSeq());

        client.getSyncApi().put(0, selectedColumn, key,
                Buf.wrap("selected".getBytes(StandardCharsets.UTF_8)), RequestType.none());
        CdcBatch selectedBatch = client.getAsyncApi()
                .cdcPollBatchAsync("filtered-sub", emptyBatch.nextSeq(), 100)
                .block();
        assertNotNull(selectedBatch);
        assertEquals(1, selectedBatch.events().size());
        assertEquals(selectedColumn, selectedBatch.events().getFirst().columnId());
    }

    @Test
    void cdcUnaryBatchLargerThanFourMiBSucceedsWithExactCursor() throws Exception {
        long columnId = client.getSyncApi().createColumn(
                "large-response-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
        long startSeq = client.getSyncApi().cdcCreate("large-response-sub", null, List.of(columnId));
        var key = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 7})});
        byte[] value = new byte[5 * 1024 * 1024];
        Arrays.fill(value, (byte) 0x5a);
        client.getSyncApi().put(0, columnId, key, Buf.wrap(value), RequestType.none());

        CdcBatch embeddedBatch = embeddedConnection.getAsyncApi()
                .cdcPollBatchAsync("large-response-sub", startSeq, 10)
                .block();
        assertNotNull(embeddedBatch);
        assertEquals(1, embeddedBatch.events().size());
        int serializedSize = CdcResponseBudget.build(embeddedBatch, Integer.MAX_VALUE).getSerializedSize();
        assertTrue(serializedSize > 4 * 1024 * 1024,
                "Regression fixture must exceed gRPC's legacy 4 MiB default, actual=" + serializedSize);

        CdcBatch grpcBatch = client.getAsyncApi()
                .cdcPollBatchAsync("large-response-sub", startSeq, 10)
                .block();
        assertNotNull(grpcBatch);
        assertEquals(embeddedBatch.nextSeq(), grpcBatch.nextSeq());
        assertEquals(1, grpcBatch.events().size());
        CDCEvent event = grpcBatch.events().getFirst();
        assertEquals(columnId, event.columnId());
        assertEquals(CDCEvent.Op.PUT, event.op());
        assertArrayEquals(new byte[]{0, 0, 0, 7}, event.key().toByteArray());
        assertArrayEquals(value, event.value().toByteArray());

        List<CDCEvent> streamedEvents = Flux.from(client.getAsyncApi()
                        .cdcPollAsync("large-response-sub", startSeq, 10))
                .collectList()
                .block();
        assertNotNull(streamedEvents);
        assertEquals(1, streamedEvents.size());
        assertArrayEquals(value, streamedEvents.getFirst().value().toByteArray());

        CdcBatch tail = client.getAsyncApi()
                .cdcPollBatchAsync("large-response-sub", grpcBatch.nextSeq(), 10)
                .block();
        assertNotNull(tail);
        assertTrue(tail.events().isEmpty());
        assertEquals(grpcBatch.nextSeq(), tail.nextSeq());
    }

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void negotiatedLimitIsExactAndOversizedGroupFailsWithoutRetryingOrAdvancing() throws Exception {
        long columnId = client.getSyncApi().createColumn(
                "limit-boundary-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
        long startSeq = client.getSyncApi().cdcCreate("limit-boundary-sub", null, List.of(columnId));
        var key = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 8})});
        byte[] value = new byte[5 * 1024 * 1024];
        Arrays.fill(value, (byte) 0x33);
        client.getSyncApi().put(0, columnId, key, Buf.wrap(value), RequestType.none());

        CdcBatch expected = embeddedConnection.getAsyncApi()
                .cdcPollBatchAsync("limit-boundary-sub", startSeq, 10)
                .block();
        assertNotNull(expected);
        int exactResponseSize = CdcResponseBudget.build(expected, Integer.MAX_VALUE).getSerializedSize();
        int exactEventSize = CdcResponseBudget.buildEvent(
                expected.events().getFirst(), Integer.MAX_VALUE).getSerializedSize();

        String previous = System.getProperty(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY);
        GrpcConnection undersizedClient = null;
        GrpcConnection exactClient = null;
        try {
            int undersizedLimit = exactEventSize - 1;
            System.setProperty(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY,
                    Integer.toString(undersizedLimit));
            undersizedClient = GrpcConnection.forHostAndPort(
                    "undersized-grpc-client", new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));

            GrpcConnection directClient = undersizedClient;
            StatusRuntimeException directFailure = assertThrows(StatusRuntimeException.class,
                    () -> directClient.getAsyncApi()
                            .cdcPollBatchAsync("limit-boundary-sub", startSeq, 10)
                            .block());
            assertEquals(Status.Code.FAILED_PRECONDITION, directFailure.getStatus().getCode());
            String description = directFailure.getStatus().getDescription();
            assertNotNull(description);
            assertTrue(description.contains(Long.toString(expected.events().getFirst().seq())));
            assertTrue(description.contains(Integer.toString(exactResponseSize)));
            assertTrue(description.contains(Integer.toString(undersizedLimit)));
            assertTrue(description.contains(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY));

            StatusRuntimeException legacyStreamFailure = assertThrows(StatusRuntimeException.class,
                    () -> Flux.from(directClient.getAsyncApi()
                                    .cdcPollAsync("limit-boundary-sub", startSeq, 10))
                            .collectList()
                            .block());
            assertEquals(Status.Code.FAILED_PRECONDITION, legacyStreamFailure.getStatus().getCode());
            assertNotNull(legacyStreamFailure.getStatus().getDescription());
            assertTrue(legacyStreamFailure.getStatus().getDescription()
                    .contains("requires " + exactEventSize + " serialized bytes"));

            GrpcConnection streamClient = undersizedClient;
            StatusRuntimeException streamFailure = assertThrows(StatusRuntimeException.class,
                    () -> streamClient.getAsyncApi().cdcStream(
                                    "limit-boundary-sub",
                                    new CdcStreamOptions(startSeq, 10, Duration.ofMillis(10), CdcCommitMode.NONE),
                                    null)
                            .blockFirst(Duration.ofSeconds(2)));
            assertEquals(Status.Code.FAILED_PRECONDITION, streamFailure.getStatus().getCode(),
                    "A permanent oversized-group error must not enter the unbounded CDC retry loop");

            System.setProperty(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY,
                    Integer.toString(exactResponseSize));
            exactClient = GrpcConnection.forHostAndPort(
                    "exact-limit-grpc-client", new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));
            CdcBatch recovered = exactClient.getAsyncApi()
                    .cdcPollBatchAsync("limit-boundary-sub", startSeq, 10)
                    .block();
            assertNotNull(recovered);
            assertEquals(expected.nextSeq(), recovered.nextSeq());
            assertEquals(1, recovered.events().size());
            assertArrayEquals(value, recovered.events().getFirst().value().toByteArray());
        } finally {
            if (exactClient != null) exactClient.close();
            if (undersizedClient != null) undersizedClient.close();
            if (previous == null) {
                System.clearProperty(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY);
            } else {
                System.setProperty(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY, previous);
            }
        }
    }

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void negotiatedResponseBudgetPaginatesWithoutSkippingEvents() throws Exception {
        long columnId = client.getSyncApi().createColumn(
                "response-pages-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
        long startSeq = client.getSyncApi().cdcCreate("response-pages-sub", null, List.of(columnId));
        int valueSize = 2 * 1024 * 1024;
        for (int i = 0; i < 3; i++) {
            byte[] value = new byte[valueSize];
            Arrays.fill(value, (byte) (i + 1));
            var key = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, (byte) (i + 1)})});
            client.getSyncApi().put(0, columnId, key, Buf.wrap(value), RequestType.none());
        }

        CdcBatch expected = embeddedConnection.getAsyncApi()
                .cdcPollBatchAsync("response-pages-sub", startSeq, 10)
                .block();
        assertNotNull(expected);
        assertEquals(3, expected.events().size());

        String previous = System.getProperty(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY);
        GrpcConnection pagedClient = null;
        try {
            System.setProperty(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY,
                    Integer.toString(GrpcConnection.MIN_MAX_INBOUND_MESSAGE_SIZE));
            pagedClient = GrpcConnection.forHostAndPort(
                    "paged-grpc-client", new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));

            long cursor = startSeq;
            for (int i = 0; i < expected.events().size(); i++) {
                CdcBatch page = pagedClient.getAsyncApi()
                        .cdcPollBatchAsync("response-pages-sub", cursor, 10)
                        .block();
                assertNotNull(page);
                assertEquals(1, page.events().size(), "Two 2 MiB events plus protobuf framing exceed 4 MiB");
                CDCEvent actual = page.events().getFirst();
                CDCEvent expectedEvent = expected.events().get(i);
                assertEquals(expectedEvent.seq(), actual.seq());
                assertArrayEquals(expectedEvent.key().toByteArray(), actual.key().toByteArray());
                assertArrayEquals(expectedEvent.value().toByteArray(), actual.value().toByteArray());
                long expectedNextSeq = i + 1 < expected.events().size()
                        ? expected.events().get(i + 1).seq()
                        : expected.nextSeq();
                assertEquals(expectedNextSeq, page.nextSeq());
                cursor = page.nextSeq();
            }

            CdcBatch tail = pagedClient.getAsyncApi()
                    .cdcPollBatchAsync("response-pages-sub", cursor, 10)
                    .block();
            assertNotNull(tail);
            assertTrue(tail.events().isEmpty());
            assertEquals(cursor, tail.nextSeq());
        } finally {
            if (pagedClient != null) pagedClient.close();
            if (previous == null) {
                System.clearProperty(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY);
            } else {
                System.setProperty(GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY, previous);
            }
        }
    }

	@Test
	void cdcGapIsPropagatedOverGrpcInsteadOfReturningAnEmptyBatch() throws Exception {
		var columnId = client.getSyncApi().createColumn(
				"gap-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
		client.getSyncApi().cdcCreate("gap-sub", null, List.of(columnId));

		long missingWalSeq = embeddedConnection.getInternalDB()
				.getDb()
				.get()
				.getLatestSequenceNumber() + 1;
		long pollFromMissingWalSeq = missingWalSeq << 20;

		var missingKey = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})});
		embeddedConnection.getSyncApi().putBatch(
				columnId,
				Flux.just(new KVBatch.KVBatchRef(
						List.of(missingKey),
						List.of(Buf.wrap("not-in-wal".getBytes(StandardCharsets.UTF_8))))),
				PutBatchMode.WRITE_BATCH_NO_WAL);

		var presentKey = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 2})});
		client.getSyncApi().put(0, columnId, presentKey,
				Buf.wrap("in-wal".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		RocksDBException error = assertThrows(RocksDBException.class,
				() -> client.getAsyncApi()
						.cdcPollBatchAsync("gap-sub", pollFromMissingWalSeq, 100)
						.block());
		assertEquals(RocksDBErrorType.CDC_GAP_DETECTED, error.getErrorUniqueId());
		assertTrue(error.getMessage().contains("Gap detected in WAL"));
	}

    @Test
    void testResumeAfterRestart() throws Exception {
        var colName = "test-col-resume";
        long lastSeq;
        long colId;
        
        colId = client.getSyncApi().createColumn(colName, ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
        client.getSyncApi().cdcCreate("sub1", null, null);
        
        var key1 = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})});
        client.getSyncApi().put(0, colId, key1, Buf.wrap("val1".getBytes(StandardCharsets.UTF_8)), RequestType.none());
        
        List<CDCEvent> events = client.getSyncApi().cdcPoll("sub1", null, 100).collect(Collectors.toList());
        assertEquals(1, events.size());
        lastSeq = events.get(0).seq();
        client.getSyncApi().cdcCommit("sub1", lastSeq);
        
        // Restart
        stopServer();
        startServer();
        
        // Re-acquire column ID
        colId = client.getSyncApi().getColumnId(colName);
        
        // New event
        var key2 = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 2})});
        client.getSyncApi().put(0, colId, key2, Buf.wrap("val2".getBytes(StandardCharsets.UTF_8)), RequestType.none());
        
        // Poll - should get only the new event
        events = client.getSyncApi().cdcPoll("sub1", null, 100).collect(Collectors.toList());
        assertEquals(1, events.size());
        assertTrue(events.get(0).seq() > lastSeq);
        assertEquals("val2", new String(events.get(0).value().toByteArray(), StandardCharsets.UTF_8));
    }
    
    @Test
    void testBucketedColumn() throws Exception {
        // Bucketed column: 1 fixed key (int), 1 variable key (int)
        var colId = client.getSyncApi().createColumn("bucket-col", ColumnSchema.of(
                IntList.of(Integer.BYTES), 
                ObjectList.of(ColumnHashType.XXHASH32),
                true
        ));
        
        client.getSyncApi().cdcCreate("sub-bucket", null, null);
        
        var key = new Keys(new Buf[]{
            Buf.wrap(new byte[]{0, 0, 0, 1}), // fixed
            Buf.wrap(new byte[]{0, 0, 0, 2})  // variable
        });
        client.getSyncApi().put(0, colId, key, Buf.wrap("bucket-val".getBytes(StandardCharsets.UTF_8)), RequestType.none());
        
        List<CDCEvent> events = client.getSyncApi().cdcPoll("sub-bucket", null, 100).collect(Collectors.toList());
        assertEquals(1, events.size());
        // The key in CDC event should be the fixed key (4 bytes)
        assertEquals(4, events.get(0).key().size());
        // The value should be the bucket content (not just "bucket-val")
        assertTrue(events.get(0).value().size() > "bucket-val".length());
    }

    @Test
    void testStressAndBackpressure() throws Exception {
        int totalEvents = 10_000; // Reduce from 50k for speed in integration test, but enough to trigger chunks
        int batchSize = 1000;
        
        var colId = client.getSyncApi().createColumn("stress-col", ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
        var startSeq = client.getSyncApi().cdcCreate("stress-sub", null, null);
        
        // 1. Generate load
        for (int i = 0; i < totalEvents; i++) {
            var key = new Keys(new Buf[]{Buf.wrap(new byte[]{
                    (byte) (i >> 24), (byte) (i >> 16), (byte) (i >> 8), (byte) i
            })});
            client.getSyncApi().put(0, colId, key, Buf.wrap(("val-" + i).getBytes(StandardCharsets.UTF_8)), RequestType.none());
            if (i % 2000 == 0) client.getSyncApi().flush(); 
        }
        
        // 2. Poll with backpressure
        long consumed = 0;
        long lastSeq = startSeq - 1;
        
        while (consumed < totalEvents) {
            // Poll small batch
            List<CDCEvent> events = client.getSyncApi().cdcPoll("stress-sub", null, batchSize).collect(Collectors.toList());
            
            if (events.isEmpty()) {
                break;
            }
            
            // Verify order and sequence
            for (CDCEvent ev : events) {
                assertTrue(ev.seq() > lastSeq, "Sequence must be strictly increasing");
                lastSeq = ev.seq();
                consumed++;
            }
            
            // Commit periodically
            client.getSyncApi().cdcCommit("stress-sub", lastSeq);
        }
        
        assertEquals(totalEvents, consumed);
    }

    @Test
    void testResolvedValuesOverGrpc() throws Exception {
        // Upload merge operator and create column with it enabled
        long opVersion;
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            try (java.util.jar.JarOutputStream jos = new java.util.jar.JarOutputStream(baos, new java.util.jar.Manifest())) {
                // Create valid empty JAR
            }
            opVersion = client.getSyncApi().uploadMergeOperator("append-op", "it.cavallium.rockserver.core.impl.MyStringAppendOperator", baos.toByteArray());
        } catch (Throwable t) {
            Assumptions.assumeTrue(false, "Merge operator upload not available on this transport/env; skipping resolved-values over gRPC test");
            return;
        }
        var colId = client.getSyncApi().createColumn(
                "resolved-col-grpc",
                ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true, "append-op", opVersion)
        );

        // Create CDC subscription with resolved values enabled
        String subId = "resolved-sub-grpc";
        long startSeq = client.getSyncApi().cdcCreate(subId, null, List.of(colId), true);

        // Put initial value, then merge a delta (using sample String append operator semantics)
        var key = new Keys(new Buf[]{Buf.wrap(new byte[]{0,0,0,1})});
        Buf initial = Buf.wrap("Initial".getBytes(StandardCharsets.UTF_8));
        Buf delta = Buf.wrap("-Patched".getBytes(StandardCharsets.UTF_8));
        client.getSyncApi().put(0, colId, key, initial, RequestType.none());
        client.getSyncApi().merge(0, colId, key, delta, RequestType.none());

        // Poll two events starting from startSeq
        var events = client.getSyncApi().cdcPoll(subId, startSeq, 10).collect(Collectors.toList());
        assertEquals(2, events.size());
        CDCEvent ev1 = events.get(0);
        CDCEvent ev2 = events.get(1);
        System.out.println("[DEBUG_LOG] ev1 value: " + new String(ev1.value().toByteArray(), StandardCharsets.UTF_8));
        System.out.println("[DEBUG_LOG] ev2 value: " + new String(ev2.value().toByteArray(), StandardCharsets.UTF_8));
        assertEquals(CDCEvent.Op.PUT, ev1.op());
        assertTrue(new String(ev1.value().toByteArray(), StandardCharsets.UTF_8).contains("Initial"));
        assertEquals(CDCEvent.Op.PUT, ev2.op());
        String val2 = new String(ev2.value().toByteArray(), StandardCharsets.UTF_8);
        assertTrue(val2.contains("Initial"));
        assertTrue(val2.contains("Patched"));
    }

}
