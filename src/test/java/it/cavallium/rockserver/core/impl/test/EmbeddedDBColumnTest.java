 package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;

public class EmbeddedDBColumnTest {

    private EmbeddedDB db;

    @BeforeEach
    public void setUp() throws IOException {
        db = new EmbeddedDB(null, "test_cols", null);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (db != null) {
            db.closeTesting();
        }
    }

    @Test
    public void testColumnRegistrationAndCollision() throws IOException {
        // "Aa" and "BB" have the same hashCode 2112
        String col1Name = "Aa";
        String col2Name = "BB";
        
        Assertions.assertEquals(col1Name.hashCode(), col2Name.hashCode());

        long id1 = db.createColumn(col1Name, ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
        Assertions.assertTrue(id1 != 0);

        long id2 = db.createColumn(col2Name, ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
        Assertions.assertTrue(id2 != 0);
        
        Assertions.assertNotEquals(id1, id2, "IDs must be different even with hash collision");

        // Verify retrieval
        Assertions.assertEquals(id1, db.getColumnId(col1Name));
        Assertions.assertEquals(id2, db.getColumnId(col2Name));

        // Delete columns
        db.deleteColumn(id1);
        
        // After deletion, getting ID might fail or return null depending on API (it throws or returns null?)
        // getColumnId throws if not found usually, or returns -1? 
        // Based on code: getColumnId throws RocksDBException or RuntimeException?
        // Let's check getColumnId implementation.
        // It uses columnNamesIndex.
        
        try {
            db.getColumnId(col1Name);
            Assertions.fail("Should throw exception for deleted column");
        } catch (Exception e) {
            // Expected
        }

        db.deleteColumn(id2);
    }
}
