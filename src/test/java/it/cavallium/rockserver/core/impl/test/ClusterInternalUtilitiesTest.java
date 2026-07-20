package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.internal.BoundedByteArrayOutputStream;
import it.cavallium.rockserver.core.cluster.internal.BoundedInputStream;
import it.cavallium.rockserver.core.cluster.internal.BoundedOutputStream;
import it.cavallium.rockserver.core.cluster.internal.DurableAtomicFileWriter;
import it.cavallium.rockserver.core.cluster.internal.StrictProperties;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ClusterInternalUtilitiesTest {

    @Test
    void durableAtomicFileWriterRejectsNullInputsWithExplicitDiagnostics(@TempDir Path tempDir) {
        assertIllegalArgument("path must not be null",
                () -> DurableAtomicFileWriter.write(null, tempPath -> { }));
        assertIllegalArgument("writer must not be null",
                () -> DurableAtomicFileWriter.write(tempDir.resolve("value.txt"), null));
        assertIllegalArgument("path must not be null",
                () -> DurableAtomicFileWriter.createDirectoriesDurably(null));
        assertIllegalArgument("path must not be null",
                () -> DurableAtomicFileWriter.forceDirectory(null));
    }

    @Test
    void durableAtomicFileWriterPublishesCompleteFile(@TempDir Path tempDir) throws Exception {
        var target = tempDir.resolve("nested").resolve("value.txt");

        DurableAtomicFileWriter.write(target,
                tempPath -> Files.writeString(tempPath, "published", StandardCharsets.UTF_8));

        assertEquals("published", Files.readString(target, StandardCharsets.UTF_8));
    }

    @Test
    void boundedInputStreamRejectsNullInputsWithExplicitDiagnostics() throws Exception {
        assertIllegalArgument("input must not be null",
                () -> new BoundedInputStream(null, 8, "payload"));
        assertIllegalArgument("description must not be null",
                () -> new BoundedInputStream(new ByteArrayInputStream(new byte[0]), 8, null));
        assertIllegalArgument("value must not be null",
                () -> new BoundedInputStream(new ByteArrayInputStream(new byte[0]), 8, "payload")
                        .read(null, 0, 0));
    }

    @Test
    void boundedInputStreamRejectsOversizedReadsBeforeReturningExtraByte() throws Exception {
        var input = new BoundedInputStream(new ByteArrayInputStream(new byte[]{1, 2, 3}), 2, "payload");

        var error = assertThrows(IllegalArgumentException.class, () -> input.read(new byte[8], 0, 8));

        assertEquals("Invalid payload length: 3", error.getMessage());
    }

    @Test
    void boundedOutputStreamRejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("output must not be null",
                () -> new BoundedOutputStream(null, 8, "payload"));
        assertIllegalArgument("description must not be null",
                () -> new BoundedOutputStream(new ByteArrayOutputStream(), 8, null));
        assertIllegalArgument("value must not be null",
                () -> new BoundedOutputStream(new ByteArrayOutputStream(), 8, "payload")
                        .write(null, 0, 0));
    }

    @Test
    void boundedOutputStreamRejectsOversizedWritesBeforePublishingBytes() {
        var output = new ByteArrayOutputStream();
        var bounded = new BoundedOutputStream(output, 2, "payload");

        var error = assertThrows(IllegalArgumentException.class, () -> bounded.write(new byte[]{1, 2, 3}));

        assertEquals("Invalid payload length: 3", error.getMessage());
        assertArrayEquals(new byte[0], output.toByteArray());
    }

    @Test
    void boundedByteArrayOutputStreamRejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("description must not be null",
                () -> new BoundedByteArrayOutputStream(8, null));
        assertIllegalArgument("value must not be null",
                () -> new BoundedByteArrayOutputStream(8, "payload").write(null, 0, 0));
    }

    @Test
    void boundedByteArrayOutputStreamRejectsOversizedWritesBeforePublishingBytes() {
        var output = new BoundedByteArrayOutputStream(2, "payload");

        var error = assertThrows(IllegalArgumentException.class, () -> output.write(new byte[]{1, 2, 3}));

        assertEquals("Invalid payload length: 3", error.getMessage());
        assertArrayEquals(new byte[0], output.toByteArray());
    }

    @Test
    void strictPropertiesRejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("reader must not be null",
                () -> StrictProperties.load(null, "test properties"));
        assertIllegalArgument("description must not be null",
                () -> StrictProperties.load(new StringReader("key=value\n"), null));
    }

    @Test
    void strictPropertiesRejectsDuplicateKeysAfterEscapes() {
        var error = assertThrows(IllegalArgumentException.class,
                () -> StrictProperties.load(new StringReader("key=first\n\\u006bey=second\n"), "test properties"));

        assertEquals("Duplicate test properties key: key", error.getMessage());
    }

    private static void assertIllegalArgument(String message, ThrowingOperation operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }

    @FunctionalInterface
    private interface ThrowingOperation {

        void run() throws IOException;
    }
}
