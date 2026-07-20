package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.internal.BoundedByteArrayOutputStream;
import it.cavallium.rockserver.core.cluster.internal.BoundedInputStream;
import it.cavallium.rockserver.core.cluster.internal.BoundedOutputStream;
import it.cavallium.rockserver.core.cluster.internal.StrictProperties;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Properties;
import java.util.TreeMap;

public final class RatisProcessAdapterProperties {

    private RatisProcessAdapterProperties() {
    }

    public static Properties read(Path path, int maxBytes, String description) throws IOException {
        requireNonNullArgument(path, "path");
        var requiredDescription = requireNonNullArgument(description, "description");
        requireMaxBytes(maxBytes);
        var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) {
            throw new IOException(requiredDescription + " path must be a regular file: " + path);
        }
        requirePayloadLength(attributes.size(), maxBytes, requiredDescription);
        try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            return read(input, maxBytes, requiredDescription);
        }
    }

    public static Properties read(InputStream input, int maxBytes, String description) throws IOException {
        requireNonNullArgument(input, "input");
        var requiredDescription = requireNonNullArgument(description, "description");
        requireMaxBytes(maxBytes);
        return StrictProperties.load(
                strictUtf8Reader(new BoundedInputStream(input, maxBytes, requiredDescription)),
                requiredDescription
        );
    }

    public static Properties fromBytes(byte[] payload, int maxBytes, String description) {
        requireNonNullArgument(payload, "payload");
        var requiredDescription = requireNonNullArgument(description, "description");
        requireMaxBytes(maxBytes);
        requirePayloadLength(payload.length, maxBytes, requiredDescription);
        try (var reader = strictUtf8Reader(new ByteArrayInputStream(payload))) {
            return StrictProperties.load(reader, requiredDescription);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed " + requiredDescription, e);
        }
    }

    public static byte[] toBytes(Properties properties, String comments, int maxBytes, String description) {
        requireNonNullArgument(properties, "properties");
        var requiredDescription = requireNonNullArgument(description, "description");
        requireMaxBytes(maxBytes);
        var bytes = new BoundedByteArrayOutputStream(maxBytes, requiredDescription);
        try {
            write(bytes, properties, comments, maxBytes, requiredDescription);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode " + requiredDescription, e);
        }
        return bytes.toByteArray();
    }

    public static String semanticSha256(Properties properties) {
        requireNonNullArgument(properties, "properties");
        var sortedProperties = new TreeMap<String, String>();
        for (var key : properties.stringPropertyNames()) {
            var value = properties.getProperty(key);
            if (value == null) {
                throw new IllegalArgumentException("property value must not be null: " + key);
            }
            sortedProperties.put(key, value);
        }
        var digest = sha256();
        updateDigest(digest, Integer.toString(sortedProperties.size()));
        for (var entry : sortedProperties.entrySet()) {
            updateDigest(digest, entry.getKey());
            updateDigest(digest, entry.getValue());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    public static void write(OutputStream output,
                             Properties properties,
                             String comments,
                             int maxBytes,
                             String description) throws IOException {
        requireNonNullArgument(output, "output");
        requireNonNullArgument(properties, "properties");
        var requiredDescription = requireNonNullArgument(description, "description");
        requireMaxBytes(maxBytes);
        var writer = new OutputStreamWriter(
                new BoundedOutputStream(output, maxBytes, requiredDescription),
                StandardCharsets.UTF_8
        );
        properties.store(writer, comments);
        writer.flush();
    }

    private static void requirePayloadLength(long length, int maxBytes, String description) {
        if (length > maxBytes) {
            throw new IllegalArgumentException("Invalid " + description + " length: " + length);
        }
    }

    private static void requireMaxBytes(int maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must not be negative: " + maxBytes);
        }
    }

    private static <T> T requireNonNullArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static InputStreamReader strictUtf8Reader(InputStream input) {
        return new InputStreamReader(input, StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT));
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 message digest is unavailable", e);
        }
    }

    static void updateDigest(MessageDigest digest, String value) {
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }
}
