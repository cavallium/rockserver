package it.cavallium.rockserver.core.cluster.internal;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;

public final class DurableAtomicFileWriter {

    private DurableAtomicFileWriter() {
    }

    public static void write(Path path, TempFileWriter writer) throws IOException {
        var targetPath = requireArgument(path, "path").toAbsolutePath().normalize();
        var parent = targetPath.getParent();
        if (parent == null) {
            throw new IOException("Durable file must have a parent directory: " + targetPath);
        }
        createDirectoriesDurably(parent);
        var tempPath = tempPath(targetPath);
        try {
            requireArgument(writer, "writer").write(tempPath);
            forceFile(tempPath);
            Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(parent);
        } finally {
            Files.deleteIfExists(tempPath);
        }
    }

    public static void createDirectoriesDurably(Path path) throws IOException {
        var directory = requireArgument(path, "path").toAbsolutePath().normalize();
        var existingAncestor = existingAncestor(directory);
        Files.createDirectories(directory);
        if (directory.equals(existingAncestor)) {
            return;
        }
        var current = directory;
        while (current != null) {
            forceDirectory(current);
            if (current.equals(existingAncestor)) {
                return;
            }
            current = current.getParent();
        }
        throw new IOException("Directory ancestry changed while preparing " + directory);
    }

    private static Path existingAncestor(Path path) throws IOException {
        var current = path;
        while (current != null) {
            try {
                Files.readAttributes(current, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                return current;
            } catch (NoSuchFileException _) {
                current = current.getParent();
            }
        }
        throw new IOException("Directory must have an existing ancestor: " + path);
    }

    private static Path tempPath(Path target) throws IOException {
        var parent = target.getParent();
        var fileName = target.getFileName();
        var prefix = fileName == null ? "durable-file" : fileName.toString();
        if (prefix.length() < 3) {
            prefix = "durable-file-" + prefix;
        }
        if (parent == null) {
            return Files.createTempFile(prefix + ".", ".tmp");
        }
        return Files.createTempFile(parent, prefix + ".", ".tmp");
    }

    private static void forceFile(Path path) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    public static void forceDirectory(Path path) throws IOException {
        var directory = requireArgument(path, "path").toAbsolutePath().normalize();
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    @FunctionalInterface
    public interface TempFileWriter {

        void write(Path tempPath) throws IOException;
    }
}
