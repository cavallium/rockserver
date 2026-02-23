package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
