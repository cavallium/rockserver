package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HotSwapMergeOperatorTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;

    @BeforeEach
    void setUp() throws Exception {
        db = new EmbeddedDB(tempDir, "hotswap-db", null);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) db.close();
    }

    public static class OperatorA extends FFMByteArrayMergeOperator {
        public OperatorA() { super("OperatorA"); }
        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            return "A".getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class OperatorB extends FFMByteArrayMergeOperator {
        public OperatorB() { super("OperatorB"); }
        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            return "B".getBytes(StandardCharsets.UTF_8);
        }
    }

    private byte[] createEmptyJar() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JarOutputStream jos = new JarOutputStream(baos, new Manifest())) {
            // empty
        }
        return baos.toByteArray();
    }

    @Test
    void testHotSwap() throws Exception {
        // 1. Upload Operator A
        long verA = db.uploadMergeOperator("op-a", OperatorA.class.getName(), createEmptyJar());
        
        // 2. Create Column with Operator A
        var schemaA = ColumnSchema.of(
                IntList.of(4), 
                ObjectList.of(), 
                true, 
                "op-a", 
                verA, 
                null
        );
        long colId = db.createColumn("test-col", schemaA);

        // 3. Verify A logic
        var key = new it.cavallium.rockserver.core.common.Keys(new Buf[]{Buf.wrap(new byte[]{0,0,0,1})});
        db.put(0, colId, key, Buf.wrap("init".getBytes()), RequestType.none());
        db.merge(0, colId, key, Buf.wrap("delta".getBytes()), RequestType.none());
        
        Buf valA = db.get(0, colId, key, RequestType.current());
        assertEquals("A", new String(valA.toByteArray()));

        // 4. Upload Operator B
        long verB = db.uploadMergeOperator("op-b", OperatorB.class.getName(), createEmptyJar());

        // 5. Update Column to Operator B
        var schemaB = ColumnSchema.of(
                IntList.of(4), 
                ObjectList.of(), 
                true, 
                "op-b", 
                verB, 
                null
        );
        // This should trigger the hot swap
        db.createColumn("test-col", schemaB);

        // 6. Verify B logic (on new merge)
        db.merge(0, colId, key, Buf.wrap("delta".getBytes()), RequestType.none());
        
        Buf valB = db.get(0, colId, key, RequestType.current());
        assertEquals("B", new String(valB.toByteArray()));
    }
}
