package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaUpdateTest {

    @TempDir
    Path tempDir;

    @Test
    void testMergeOperatorUpdate() throws Exception {
        Path configV1 = Files.createTempFile("config-v1", ".conf");
        Files.writeString(configV1, """
            database: {
              global: {
                fallback-column-options: {
                  merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
                }
              }
            }
            """);

        Path configV2 = Files.createTempFile("config-v2", ".conf");
        Files.writeString(configV2, """
            database: {
              global: {
                fallback-column-options: {
                  merge-operator-class: "it.cavallium.rockserver.core.impl.test.MyStringAppendOperatorV2"
                }
              }
            }
            """);

        String dbName = "schema-update-test";
        String colName = "test-col";
        long colId;
        String subId = "test-sub";

        // --- STEP 1: V1 ---
        try (EmbeddedConnection db = new EmbeddedConnection(tempDir, dbName, configV1)) {
            // Create column with V1 schema (explicit class name)
            colId = db.createColumn(colName, ColumnSchema.of(IntList.of(4), ObjectList.of(), true, null, null, "it.cavallium.rockserver.core.impl.MyStringAppendOperator"));
            
            // Create CDC subscription (Resolved=true)
            db.getSyncApi().cdcCreate(subId, null, List.of(colId), true);

            // Put "A", Merge "B" -> "A,B"
            db.put(0, colId, new Keys(Buf.wrap(intToBytes(1))), Buf.wrap("A".getBytes()), RequestType.none());
            db.merge(0, colId, new Keys(Buf.wrap(intToBytes(1))), Buf.wrap("B".getBytes()), RequestType.none());

            // Verify
            Buf val = db.get(0, colId, new Keys(Buf.wrap(intToBytes(1))), RequestType.current());
            assertEquals("A,B", new String(val.toByteArray()));
        }

        // --- STEP 2: V2 ---
        try (EmbeddedConnection db = new EmbeddedConnection(tempDir, dbName, configV2)) {
            // Call createColumn with V2 schema (New Class Name)
            // This exercises the fix: schema update on disk
            colId = db.createColumn(colName, ColumnSchema.of(IntList.of(4), ObjectList.of(), true, null, null, "it.cavallium.rockserver.core.impl.test.MyStringAppendOperatorV2"));
            
            // Merge "C" -> Should use V2 (semicolon) -> "A,B;C"
            db.merge(0, colId, new Keys(Buf.wrap(intToBytes(1))), Buf.wrap("C".getBytes()), RequestType.none());

            Buf val = db.get(0, colId, new Keys(Buf.wrap(intToBytes(1))), RequestType.current());
            String valStr = new String(val.toByteArray());
            System.out.println("[DEBUG_LOG] Result value: " + valStr);
            assertTrue(valStr.contains(";"), "Result " + valStr + " should contain semicolon, proving V2 was used");

            // --- CDC Verification ---
            // Poll all events. Resolved=true.
            // All events should resolve to LATEST state ("A,B;C" or "A;B;C")
            List<CDCEvent> events = db.getSyncApi().cdcPoll(subId, null, 100).collect(Collectors.toList());
            assertEquals(3, events.size());
            
            String latestState = valStr;
            assertEquals(latestState, new String(events.get(0).value().toByteArray()));
            assertEquals(latestState, new String(events.get(1).value().toByteArray()));
            assertEquals(latestState, new String(events.get(2).value().toByteArray()));
        }
    }

    private byte[] intToBytes(int x) {
        return java.nio.ByteBuffer.allocate(4).putInt(x).array();
    }
}
