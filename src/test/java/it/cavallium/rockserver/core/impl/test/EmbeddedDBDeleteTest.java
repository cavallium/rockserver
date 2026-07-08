package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class EmbeddedDBDeleteTest {

    private EmbeddedDB db;
    private Path tempDir;
    private long colId;

    private static Buf longToBuf(long l) {
        return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(l).array());
    }

    @BeforeEach
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("rockserver-test-delete");
        db = new EmbeddedDB(tempDir, "test_delete_db", null);
        colId = db.createColumn("test_col", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (db != null) {
            db.closeTesting();
        }
        if (tempDir != null) {
            Utils.deleteDirectory(tempDir.toAbsolutePath().toString());
        }
    }

    @Test
    public void testDeleteSimple() {
        Keys key = new Keys(longToBuf(1L));
        Buf value = Utils.toBufSimple(10);

        db.put(0, colId, key, value, RequestType.none());

        // Verify exists
        Assertions.assertTrue(db.get(0, colId, key, RequestType.exists()));

        // Delete
        db.delete(0, colId, key, RequestType.none());

        // Verify deleted
        Assertions.assertFalse(db.get(0, colId, key, RequestType.exists()));
    }

    @Test
    public void testDeletePreviousValue() {
        Keys key = new Keys(longToBuf(2L));
        Buf value = Utils.toBufSimple(20);

        db.put(0, colId, key, value, RequestType.none());

        // Delete and get previous
        Buf prev = db.delete(0, colId, key, RequestType.previous());

        Assertions.assertNotNull(prev);
        Assertions.assertEquals(value, prev);
        Assertions.assertFalse(db.get(0, colId, key, RequestType.exists()));

        // Delete again, should be null
        Buf prev2 = db.delete(0, colId, key, RequestType.previous());
        Assertions.assertNull(prev2);
    }

    @Test
    public void testDeletePreviousPresence() {
        Keys key = new Keys(longToBuf(3L));
        Buf value = Utils.toBufSimple(30);

        db.put(0, colId, key, value, RequestType.none());

        // Delete and get presence
        boolean existed = db.delete(0, colId, key, RequestType.previousPresence());

        Assertions.assertTrue(existed);
        Assertions.assertFalse(db.get(0, colId, key, RequestType.exists()));

        // Delete again, should be false
        boolean existed2 = db.delete(0, colId, key, RequestType.previousPresence());
        Assertions.assertFalse(existed2);
    }

    @Test
    public void testDeleteNonExistent() {
        Keys key = new Keys(longToBuf(4L));

        // Delete non-existent
        db.delete(0, colId, key, RequestType.none());

        // Verify still non-existent
        Assertions.assertFalse(db.get(0, colId, key, RequestType.exists()));
    }

    @Test
    public void testDeleteRangeRemovesOnlyKeysInsideBounds() {
        Keys key1 = new Keys(longToBuf(1L));
        Keys key2 = new Keys(longToBuf(2L));
        Keys key3 = new Keys(longToBuf(3L));
        Keys key4 = new Keys(longToBuf(4L));

        db.put(0, colId, key1, Utils.toBufSimple(10), RequestType.none());
        db.put(0, colId, key2, Utils.toBufSimple(20), RequestType.none());
        db.put(0, colId, key3, Utils.toBufSimple(30), RequestType.none());
        db.put(0, colId, key4, Utils.toBufSimple(40), RequestType.none());

        db.deleteRange(colId, key2, key4);

        Assertions.assertTrue(db.get(0, colId, key1, RequestType.exists()));
        Assertions.assertFalse(db.get(0, colId, key2, RequestType.exists()));
        Assertions.assertFalse(db.get(0, colId, key3, RequestType.exists()));
        Assertions.assertTrue(db.get(0, colId, key4, RequestType.exists()));
    }

    @Test
    public void testDeleteRangeSupportsOpenEndedUpperBound() {
        Keys key1 = new Keys(longToBuf(1L));
        Keys key2 = new Keys(longToBuf(2L));
        Keys key3 = new Keys(longToBuf(3L));

        db.put(0, colId, key1, Utils.toBufSimple(10), RequestType.none());
        db.put(0, colId, key2, Utils.toBufSimple(20), RequestType.none());
        db.put(0, colId, key3, Utils.toBufSimple(30), RequestType.none());

        db.deleteRange(colId, key2, null);

        Assertions.assertTrue(db.get(0, colId, key1, RequestType.exists()));
        Assertions.assertFalse(db.get(0, colId, key2, RequestType.exists()));
        Assertions.assertFalse(db.get(0, colId, key3, RequestType.exists()));
    }
}
