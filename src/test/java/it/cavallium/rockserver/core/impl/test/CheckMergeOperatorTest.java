package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.MyStringAppendOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.*;

class CheckMergeOperatorTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;

    @BeforeEach
    void setUp() throws Exception {
        db = new EmbeddedDB(tempDir, "test-db", null);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.close();
        }
    }

    @Test
    void testCheckAndEnsureMergeOperator() throws Exception {
        String name = "my-op";
        String className = MyStringAppendOperator.class.getName();
        byte[] jarData = createJar();

        // 1. Check - should not exist
        byte[] hash = java.security.MessageDigest.getInstance("SHA-256").digest(jarData);
        Long version = db.checkMergeOperator(name, hash);
        assertNull(version, "Operator should not exist yet");

        // 2. Ensure - should create version 1
        long v1 = db.ensureMergeOperator(name, className, jarData);
        assertTrue(v1 >= 0);

        // 3. Check - should exist now
        Long vCheck = db.checkMergeOperator(name, hash);
        assertNotNull(vCheck);
        assertEquals(v1, vCheck);

        // 4. Ensure again - should return same version
        long v2 = db.ensureMergeOperator(name, className, jarData);
        assertEquals(v1, v2, "Should return existing version");

        // 5. Upload explicitly - should return same version (due to deduplication in upload logic I added)
        long v3 = db.uploadMergeOperator(name, className, jarData);
        assertEquals(v1, v3, "Upload should also deduplicate");
        
        // 6. Ensure with DIFFERENT content
        byte[] jarData2 = createJarWithEntry();
        long v4 = db.ensureMergeOperator(name, className, jarData2);
        assertNotEquals(v1, v4, "Different content should produce new version");
        assertTrue(v4 > v1);
    }

    private byte[] createJar() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JarOutputStream jos = new JarOutputStream(baos, new Manifest())) {
            // empty jar
        }
        return baos.toByteArray();
    }
    
    private byte[] createJarWithEntry() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JarOutputStream jos = new JarOutputStream(baos, new Manifest())) {
            jos.putNextEntry(new java.util.zip.ZipEntry("test.txt"));
            jos.write("test".getBytes());
            jos.closeEntry();
        }
        return baos.toByteArray();
    }
}
