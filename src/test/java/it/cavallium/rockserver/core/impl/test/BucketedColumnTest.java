package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import org.rocksdb.ColumnFamilyHandle;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class BucketedColumnTest {

    @Test
    void testDecodeBucketedKeys() {
        // Schema: 1 Fixed Key (4 bytes int), 1 Variable Key (hashed with XXHASH32)
        var schema = ColumnSchema.of(
            IntArrayList.of(4),
            ObjectArrayList.of(ColumnHashType.XXHASH32),
            true
        );

        ColumnFamilyHandle cfh = mock(ColumnFamilyHandle.class);
        
        try (var col = new ColumnInstance(cfh, schema)) {
            // 1. Create original keys
            Buf fixedKey = Buf.wrap(new byte[]{0, 0, 0, 1}); // 1
            Buf varKey = Buf.wrap("variable-key-content".getBytes(StandardCharsets.UTF_8));
            Buf[] originalKeys = new Buf[]{fixedKey, varKey};

            // 2. Calculate RocksDB Key (Fixed + Hash)
            Buf calculatedKey = col.calculateKey(originalKeys);
            
            // Verify calculated key size = 4 (fixed) + 4 (hash) = 8
            assertEquals(8, calculatedKey.size());

            // 3. Compute Bucket Element Key (Encoded Var Keys)
            // We need to extract the var keys part.
            Buf[] varKeysOnly = new Buf[]{varKey};
            Buf bucketElementKey = col.computeBucketElementKey(varKeysOnly);

            // 4. Decode keys
            Buf[] decodedKeys = col.decodeKeys(calculatedKey, bucketElementKey);
            
            // 5. Verify
            assertEquals(2, decodedKeys.length);
            assertEquals(fixedKey, decodedKeys[0], "Fixed key mismatch");
            assertEquals(varKey, decodedKeys[1], "Variable key mismatch");
        }
    }

    @Test
    void testDecodeBucketedKeysMultipleVariable() {
        // Schema: 1 Fixed Key (4 bytes), 2 Variable Keys
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH32),
                true
        );

        ColumnFamilyHandle cfh = mock(ColumnFamilyHandle.class);

        try (var col = new ColumnInstance(cfh, schema)) {
            Buf fixedKey = Buf.wrap(new byte[]{1, 2, 3, 4});
            Buf varKey1 = Buf.wrap("var1".getBytes(StandardCharsets.UTF_8));
            Buf varKey2 = Buf.wrap("variable2".getBytes(StandardCharsets.UTF_8));
            Buf[] originalKeys = new Buf[]{fixedKey, varKey1, varKey2};

            Buf calculatedKey = col.calculateKey(originalKeys);
            // 4 (fixed) + 4 (hash1) + 4 (hash2) = 12
            assertEquals(12, calculatedKey.size());

            Buf[] varKeysOnly = new Buf[]{varKey1, varKey2};
            Buf bucketElementKey = col.computeBucketElementKey(varKeysOnly);

            Buf[] decodedKeys = col.decodeKeys(calculatedKey, bucketElementKey);

            assertEquals(3, decodedKeys.length);
            assertEquals(fixedKey, decodedKeys[0]);
            assertEquals(varKey1, decodedKeys[1]);
            assertEquals(varKey2, decodedKeys[2]);
        }
    }

    @Test
    void testDecodeBucketedKeysWithEmptyVariableKey() {
        // Schema: 1 Fixed Key, 1 Variable Key
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );

        ColumnFamilyHandle cfh = mock(ColumnFamilyHandle.class);

        try (var col = new ColumnInstance(cfh, schema)) {
            Buf fixedKey = Buf.wrap(new byte[]{0, 0, 0, 1});
            Buf varKey = Buf.wrap(new byte[0]); // Empty key
            Buf[] originalKeys = new Buf[]{fixedKey, varKey};

            Buf calculatedKey = col.calculateKey(originalKeys);
            Buf bucketElementKey = col.computeBucketElementKey(new Buf[]{varKey});

            Buf[] decodedKeys = col.decodeKeys(calculatedKey, bucketElementKey);

            assertEquals(2, decodedKeys.length);
            assertEquals(fixedKey, decodedKeys[0]);
            assertEquals(varKey, decodedKeys[1]);
            assertEquals(0, decodedKeys[1].size());
        }
    }

    @Test
    void testDecodeBucketedKeysNullBucketValue() {
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );

        ColumnFamilyHandle cfh = mock(ColumnFamilyHandle.class);

        try (var col = new ColumnInstance(cfh, schema)) {
            Buf calculatedKey = Buf.createZeroes(8);
            
            RocksDBException ex = assertThrows(RocksDBException.class, () -> {
                col.decodeKeys(calculatedKey, null);
            });
            assertEquals(RocksDBException.RocksDBErrorType.NULL_ARGUMENT, ex.getErrorUniqueId());
        }
    }

    @Test
    void testDecodeBucketedKeysCorruptedBucketValueShortLength() {
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );

        ColumnFamilyHandle cfh = mock(ColumnFamilyHandle.class);

        try (var col = new ColumnInstance(cfh, schema)) {
            Buf calculatedKey = Buf.createZeroes(8);
            Buf bucketValue = Buf.wrap(new byte[]{0}); // Too short for length char (needs 2 bytes)

            RocksDBException ex = assertThrows(RocksDBException.class, () -> {
                col.decodeKeys(calculatedKey, bucketValue);
            });
            assertEquals(RocksDBException.RocksDBErrorType.INTERNAL_ERROR, ex.getErrorUniqueId());
            assertTrue(ex.getMessage().contains("too short for key length"));
        }
    }

    @Test
    void testDecodeBucketedKeysCorruptedBucketValueShortContent() {
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );

        ColumnFamilyHandle cfh = mock(ColumnFamilyHandle.class);

        try (var col = new ColumnInstance(cfh, schema)) {
            Buf calculatedKey = Buf.createZeroes(8);
            // Length says 5 bytes, but buffer has only 2 bytes for length + 1 byte for content
            // 0, 5 (length 5)
            // 1 (content)
            Buf bucketValue = Buf.wrap(new byte[]{0, 5, 1}); 

            RocksDBException ex = assertThrows(RocksDBException.class, () -> {
                col.decodeKeys(calculatedKey, bucketValue);
            });
            assertEquals(RocksDBException.RocksDBErrorType.INTERNAL_ERROR, ex.getErrorUniqueId());
            assertTrue(ex.getMessage().contains("too short for key content"));
        }
    }
    
    @Test
    void testMixedFixedAndVariableKeys() {
        // 2 Fixed, 2 Variable
         var schema = ColumnSchema.of(
                IntArrayList.of(4, 2),
                ObjectArrayList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH32),
                true
        );

        ColumnFamilyHandle cfh = mock(ColumnFamilyHandle.class);

        try (var col = new ColumnInstance(cfh, schema)) {
            Buf f1 = Buf.wrap(new byte[]{1, 1, 1, 1});
            Buf f2 = Buf.wrap(new byte[]{2, 2});
            Buf v1 = Buf.wrap("v1".getBytes(StandardCharsets.UTF_8));
            Buf v2 = Buf.wrap("v2".getBytes(StandardCharsets.UTF_8));
            Buf[] originalKeys = new Buf[]{f1, f2, v1, v2};

            Buf calculatedKey = col.calculateKey(originalKeys);
            Buf bucketElementKey = col.computeBucketElementKey(new Buf[]{v1, v2});

            Buf[] decodedKeys = col.decodeKeys(calculatedKey, bucketElementKey);

            assertEquals(4, decodedKeys.length);
            assertEquals(f1, decodedKeys[0]);
            assertEquals(f2, decodedKeys[1]);
            assertEquals(v1, decodedKeys[2]);
            assertEquals(v2, decodedKeys[3]);
        }
    }
}
