package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;

public final class RatisProcessAdapterPaths {

    private RatisProcessAdapterPaths() {
    }

    public static boolean resolvesInside(Path directory, Path path) throws IOException {
        if (directory == null) {
            throw new IllegalArgumentException("directory must not be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path must not be null");
        }
        var resolvedDirectory = resolveExistingAncestor(directory);
        var resolvedPath = resolveExistingAncestor(path);
        return resolvedPath.equals(resolvedDirectory) || resolvedPath.startsWith(resolvedDirectory);
    }

    public static Path resolveExistingAncestor(Path path) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("path must not be null");
        }
        var normalized = normalize(path);
        var suffix = new ArrayDeque<Path>();
        var current = normalized;
        while (current != null && !existsNoFollow(current)) {
            var fileName = current.getFileName();
            if (fileName != null) {
                suffix.addFirst(fileName);
            }
            current = current.getParent();
        }
        if (current == null) {
            throw new IOException("Path must have an existing ancestor: " + normalized);
        }
        var resolved = current.toRealPath();
        for (var part : suffix) {
            resolved = resolved.resolve(part);
        }
        return resolved.normalize();
    }

    private static boolean existsNoFollow(Path path) throws IOException {
        try {
            Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            return true;
        } catch (NoSuchFileException _) {
            return false;
        }
    }

    private static Path normalize(Path path) {
        return path.toAbsolutePath().normalize();
    }
}
