package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterProperties;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RatisProcessAdapterPropertiesTest {

    @Test
    void helpersRejectNullArgumentsWithExplicitDiagnostics() {
        var nullPath = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.read((Path) null, 1024, "test properties"));
        var nullInput = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.read((InputStream) null, 1024, "test properties"));
        var nullPayload = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.fromBytes(null, 1024, "test properties"));
        var nullToBytesProperties = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.toBytes(null, "test", 1024, "test properties"));
        var nullDigestProperties = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.semanticSha256(null));
        var nullOutput = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.write(null, new Properties(), "test", 1024, "test properties"));
        var nullWriteProperties = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.write(
                        new ByteArrayOutputStream(),
                        null,
                        "test",
                        1024,
                        "test properties"
                ));
        var nullDescription = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.fromBytes(new byte[0], 1024, null));

        assertEquals("path must not be null", nullPath.getMessage());
        assertEquals("input must not be null", nullInput.getMessage());
        assertEquals("payload must not be null", nullPayload.getMessage());
        assertEquals("properties must not be null", nullToBytesProperties.getMessage());
        assertEquals("properties must not be null", nullDigestProperties.getMessage());
        assertEquals("output must not be null", nullOutput.getMessage());
        assertEquals("properties must not be null", nullWriteProperties.getMessage());
        assertEquals("description must not be null", nullDescription.getMessage());
    }

    @Test
    void readRejectsOversizedFileBeforeLoading(@TempDir Path tempDir) throws Exception {
        var path = tempDir.resolve("adapter.properties");
        Files.writeString(path, "key=value\n");

        assertEquals("Invalid test properties length: 10",
                assertThrows(IllegalArgumentException.class,
                        () -> RatisProcessAdapterProperties.read(path, 9, "test properties"))
                        .getMessage());
    }

    @Test
    void fromBytesRejectsOversizedPayloadBeforeLoading() {
        assertEquals("Invalid test properties length: 2",
                assertThrows(IllegalArgumentException.class,
                        () -> RatisProcessAdapterProperties.fromBytes(new byte[]{'x', '='}, 1, "test properties"))
                        .getMessage());
    }

    @Test
    void readStreamRejectsOversizedPayloadWhileLoading() {
        var payload = "key=value\n".getBytes(StandardCharsets.UTF_8);

        assertEquals("Invalid test properties length: 10",
                assertThrows(IllegalArgumentException.class,
                        () -> RatisProcessAdapterProperties.read(
                                new ByteArrayInputStream(payload),
                                9,
                                "test properties"
                        ))
                        .getMessage());
    }

    @Test
    void readStreamRejectsMalformedUtf8PayloadWhileLoading() {
        assertThrows(MalformedInputException.class,
                () -> RatisProcessAdapterProperties.read(
                        new ByteArrayInputStream(malformedUtf8Properties()),
                        1024,
                        "test properties"
                ));
    }

    @Test
    void readFileRejectsMalformedUtf8PayloadWhileLoading(@TempDir Path tempDir) throws Exception {
        var path = tempDir.resolve("adapter.properties");
        Files.write(path, malformedUtf8Properties());

        assertThrows(MalformedInputException.class,
                () -> RatisProcessAdapterProperties.read(path, 1024, "test properties"));
    }

    @Test
    void fromBytesRejectsMalformedUtf8PayloadWhileLoading() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.fromBytes(
                        malformedUtf8Properties(),
                        1024,
                        "test properties"
                ));

        assertEquals("Malformed test properties", thrown.getMessage());
        assertInstanceOf(MalformedInputException.class, thrown.getCause());
    }

    @Test
    void readersRejectDuplicateKeys(@TempDir Path tempDir) throws Exception {
        assertEquals("Duplicate test properties key: key",
                assertThrows(IllegalArgumentException.class,
                        () -> RatisProcessAdapterProperties.fromBytes(
                                "key=first\nkey=second\n".getBytes(StandardCharsets.UTF_8),
                                1024,
                                "test properties"
                        ))
                        .getMessage());

        var path = tempDir.resolve("adapter.properties");
        Files.writeString(path, "escaped=first\n\\u0065scaped=second\n", StandardCharsets.UTF_8);
        assertEquals("Duplicate test properties key: escaped",
                assertThrows(IllegalArgumentException.class,
                        () -> RatisProcessAdapterProperties.read(path, 1024, "test properties"))
                        .getMessage());
    }

    @Test
    void readRejectsSymbolicLinkPath(@TempDir Path tempDir) throws Exception {
        var target = tempDir.resolve("target.properties");
        Files.writeString(target, "key=value\n");
        var path = tempDir.resolve("adapter.properties");
        Files.createSymbolicLink(path, target.getFileName());

        var error = assertThrows(IOException.class,
                () -> RatisProcessAdapterProperties.read(path, 1024, "test properties"));

        assertTrue(error.getMessage().contains("path must be a regular file"));
    }

    @Test
    void toBytesRejectsOversizedOutput() {
        var properties = new Properties();
        properties.setProperty("key", "value");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.toBytes(properties, "test", 8, "test properties"));
    }

    @Test
    void writeRejectsOversizedOutput() {
        var properties = new Properties();
        properties.setProperty("key", "value");

        assertThrows(IllegalArgumentException.class,
                () -> RatisProcessAdapterProperties.write(
                        new ByteArrayOutputStream(),
                        properties,
                        "test",
                        8,
                        "test properties"
                ));
    }

    @Test
    void roundTripsUtf8Properties() {
        var properties = new Properties();
        properties.setProperty("key", "value");
        var encoded = RatisProcessAdapterProperties.toBytes(properties, "test", 1024, "test properties");

        var decoded = RatisProcessAdapterProperties.fromBytes(encoded, 1024, "test properties");

        assertEquals("value", decoded.getProperty("key"));
    }

    @Test
    void writeStreamsUtf8Properties() throws Exception {
        var properties = new Properties();
        properties.setProperty("key", "value");
        var output = new ByteArrayOutputStream();

        RatisProcessAdapterProperties.write(output, properties, "test", 1024, "test properties");

        var decoded = RatisProcessAdapterProperties.fromBytes(output.toByteArray(), 1024, "test properties");
        assertEquals("value", decoded.getProperty("key"));
    }

    @Test
    void semanticSha256IsStableAcrossPropertyInsertionOrderAndChangesWithValues() {
        var first = new Properties();
        first.setProperty("b", "two");
        first.setProperty("a", "one");
        var reordered = new Properties();
        reordered.setProperty("a", "one");
        reordered.setProperty("b", "two");
        var changed = new Properties();
        changed.setProperty("a", "one");
        changed.setProperty("b", "three");

        assertEquals(RatisProcessAdapterProperties.semanticSha256(first),
                RatisProcessAdapterProperties.semanticSha256(reordered));
        assertNotEquals(RatisProcessAdapterProperties.semanticSha256(first),
                RatisProcessAdapterProperties.semanticSha256(changed));
    }

    private static byte[] malformedUtf8Properties() {
        return new byte[]{'k', 'e', 'y', '=', (byte) 0xC3, 0x28, '\n'};
    }
}
