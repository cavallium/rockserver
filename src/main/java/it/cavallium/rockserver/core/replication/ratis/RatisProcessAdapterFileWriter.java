package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.internal.BoundedOutputStream;
import it.cavallium.rockserver.core.cluster.internal.DurableAtomicFileWriter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

final class RatisProcessAdapterFileWriter {

    private RatisProcessAdapterFileWriter() {
    }

    static void writeProperties(Path path,
                                Properties properties,
                                String comments,
                                int maxBytes,
                                String description) throws IOException {
        var resolvedPath = requireNonNull(path, "path");
        var resolvedProperties = requireNonNull(properties, "properties");
        var resolvedDescription = requireNonNull(description, "description");
        write(resolvedPath, tempPath -> {
            try (var output = Files.newOutputStream(tempPath)) {
                RatisProcessAdapterProperties.write(output, resolvedProperties, comments, maxBytes, resolvedDescription);
            }
        });
    }

    static void writeString(Path path,
                            String value,
                            int maxBytes,
                            String description) throws IOException {
        var resolvedPath = requireNonNull(path, "path");
        var resolvedValue = requireNonNull(value, "value");
        var resolvedDescription = requireNonNull(description, "description");
        write(resolvedPath, tempPath -> {
            try (var writer = new OutputStreamWriter(
                    new BoundedOutputStream(Files.newOutputStream(tempPath), maxBytes, resolvedDescription),
                    StandardCharsets.UTF_8
            )) {
                writer.write(resolvedValue);
            }
        });
    }

    private static void write(Path path, TempFileWriter writer) throws IOException {
        DurableAtomicFileWriter.write(requireNonNull(path, "path"), requireNonNull(writer, "writer")::write);
    }

    static void createDirectoriesDurably(Path path) throws IOException {
        DurableAtomicFileWriter.createDirectoriesDurably(requireNonNull(path, "path"));
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    @FunctionalInterface
    private interface TempFileWriter {

        void write(Path tempPath) throws IOException;
    }
}
