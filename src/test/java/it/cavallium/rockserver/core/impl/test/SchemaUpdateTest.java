package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

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

        // --- STEP 1: V1 ---
        try (EmbeddedConnection db = new EmbeddedConnection(tempDir, dbName, configV1)) {
            // Create column with V1 schema (explicit class name)
            colId = db.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn(colName, ColumnSchema.of(IntList.of(4), ObjectList.of(), true, null, null, "it.cavallium.rockserver.core.impl.MyStringAppendOperator"));

            // Put "A", Merge "B" -> "A,B"
            db.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).put(0, colId, new Keys(Buf.wrap(intToBytes(1))), Buf.wrap("A".getBytes()), RequestType.none());
            db.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).merge(0, colId, new Keys(Buf.wrap(intToBytes(1))), Buf.wrap("B".getBytes()), RequestType.none());

            // Verify
            Buf val = db.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).get(0, colId, new Keys(Buf.wrap(intToBytes(1))), RequestType.current());
            assertEquals("A,B", new String(val.toByteArray()));
        }

        // --- STEP 2: V2 ---
        try (EmbeddedConnection db = new EmbeddedConnection(tempDir, dbName, configV2)) {
            // Call createColumn with V2 schema (New Class Name)
            // This exercises the fix: schema update on disk
            colId = db.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn(colName, ColumnSchema.of(IntList.of(4), ObjectList.of(), true, null, null, "it.cavallium.rockserver.core.impl.test.MyStringAppendOperatorV2"));

            // Merge "C" -> Should use V2 (semicolon) -> "A,B;C"
            db.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).merge(0, colId, new Keys(Buf.wrap(intToBytes(1))), Buf.wrap("C".getBytes()), RequestType.none());

            Buf val = db.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).get(0, colId, new Keys(Buf.wrap(intToBytes(1))), RequestType.current());
            String valStr = new String(val.toByteArray());
            System.out.println("[DEBUG_LOG] Result value: " + valStr);
            assertTrue(valStr.contains(";"), "Result " + valStr + " should contain semicolon, proving V2 was used");
        }
    }

    private byte[] intToBytes(int x) {
        return java.nio.ByteBuffer.allocate(4).putInt(x).array();
    }
}
