package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.MyStringAppendOperator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static it.cavallium.rockserver.core.common.Utils.toBuf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RemoteMergeOperatorTest {

    @Test
    public void testRemoteMergeOperator() throws Exception {
        Path tempDir = Files.createTempDirectory("rockserver-test");
        try (EmbeddedConnection db = new EmbeddedConnection(tempDir, "test", null)) {
            // Create a JAR containing MyStringAppendOperator
            byte[] jarData = createJar(MyStringAppendOperator.class);
            
            // Upload
            long version = db.getSyncApi().uploadMergeOperator("remote-append", MyStringAppendOperator.class.getName(), jarData);
            
            // Create column using the operator
            ColumnSchema schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true, "remote-append", version);
            long colId = db.getSyncApi().createColumn("remote-col", schema);
            
            // Put and Merge
            var key = new it.cavallium.rockserver.core.common.Keys(toBuf(new byte[]{1}));
            db.getSyncApi().put(0, colId, key, toBuf("Hello".getBytes()), RequestType.none());
            db.getSyncApi().merge(0, colId, key, toBuf("World".getBytes()), RequestType.none());
            
            // Get
            var result = db.getSyncApi().get(0, colId, key, RequestType.current());
            assertNotNull(result);
            assertEquals("Hello,World", new String(result.toByteArray()));
            System.out.println("Test passed!");
        }
    }

    public static void main(String[] args) throws Exception {
        new RemoteMergeOperatorTest().testRemoteMergeOperator();
    }

    private byte[] createJar(Class<?>... classes) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JarOutputStream jos = new JarOutputStream(baos)) {
            for (Class<?> clazz : classes) {
                String entryName = clazz.getName().replace('.', '/') + ".class";
                jos.putNextEntry(new JarEntry(entryName));
                try (InputStream is = clazz.getClassLoader().getResourceAsStream(entryName)) {
                    if (is == null) throw new RuntimeException("Class file not found: " + entryName);
                    is.transferTo(jos);
                }
                jos.closeEntry();
            }
        }
        return baos.toByteArray();
    }
}
