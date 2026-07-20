package it.cavallium.rockserver.ratis;

import it.cavallium.rockserver.core.cluster.internal.DurableAtomicFileWriter;
import it.cavallium.rockserver.core.cluster.snapshot.RocksDBCheckpointSnapshotSupport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

import org.jetbrains.annotations.NotNull;

public final class RockserverRatisClasspathSnapshotArchive {

    private static final int MAGIC = 0x5253_4350;
    private static final int FORMAT_VERSION = 1;
    private static final int MAX_RELATIVE_PATH_BYTES = 4 * 1024;
    private static final int COPY_BUFFER_SIZE_BYTES = 64 * 1024;

    private RockserverRatisClasspathSnapshotArchive() {
    }

    public static void write(Path snapshotFile, Path checkpointDirectory) throws IOException {
        write(snapshotFile, checkpointDirectory, RocksDBCheckpointSnapshotSupport.DEFAULT_LIMITS);
    }

    static void write(Path snapshotFile, Path checkpointDirectory,
                      RocksDBCheckpointSnapshotSupport.SnapshotLimits limits) throws IOException {
        Objects.requireNonNull(snapshotFile, "snapshotFile");
        var normalizedCheckpoint = requireDirectory(checkpointDirectory, "checkpointDirectory");
        var admittedFiles = listFiles(normalizedCheckpoint, limits);
        var admittedEntries = metadata(admittedFiles);
        var totalSizeBytes = totalSizeBytes(admittedEntries, limits);
        DurableAtomicFileWriter.write(snapshotFile, temporaryFile -> {
            try (var output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                    temporaryFile,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    LinkOption.NOFOLLOW_LINKS
            )))) {
                output.writeInt(MAGIC);
                output.writeInt(FORMAT_VERSION);
                output.writeInt(admittedFiles.size());
                output.writeLong(totalSizeBytes);
                for (var file : admittedFiles) {
                    output.writeInt(file.relativePathBytes().length);
                    output.write(file.relativePathBytes());
                    output.writeLong(file.sizeBytes());
                    output.writeLong(copyExact(file.path(), file.sizeBytes(), output));
                }
            }
            if (!admittedEntries.equals(metadata(listFiles(normalizedCheckpoint, limits)))) {
                throw new IOException("RocksDB checkpoint directory changed while archiving: " + normalizedCheckpoint);
            }
        });
    }

    public static void install(Path snapshotFile, Path targetDirectory) throws IOException {
        install(snapshotFile, targetDirectory, RocksDBCheckpointSnapshotSupport.DEFAULT_LIMITS);
    }

    static void install(Path snapshotFile, Path targetDirectory,
                        RocksDBCheckpointSnapshotSupport.SnapshotLimits limits) throws IOException {
        Objects.requireNonNull(snapshotFile, "snapshotFile");
        Objects.requireNonNull(limits, "limits");
        var normalizedTarget = Objects.requireNonNull(targetDirectory, "targetDirectory").toAbsolutePath().normalize();
        var parent = normalizedTarget.getParent();
        if (parent == null) {
            throw new IOException("RocksDB snapshot target must have a parent directory: " + normalizedTarget);
        }
        DurableAtomicFileWriter.createDirectoriesDurably(parent);
        var prefix = normalizedTarget.getFileName() + ".snapshot-install-";
        Path installDirectory = Files.createTempDirectory(parent, prefix);
        DurableAtomicFileWriter.forceDirectory(parent);
        Throwable failure = null;
        try {
            extract(snapshotFile, installDirectory, limits);
            RocksDBCheckpointSnapshotSupport.replacingClosedDatabaseDirectory(normalizedTarget)
                    .installSnapshot(installDirectory);
            installDirectory = null;
        } catch (IOException | RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            cleanupInstallDirectory(installDirectory, failure);
        }
    }

    private static void extract(Path snapshotFile, Path installDirectory,
                                RocksDBCheckpointSnapshotSupport.SnapshotLimits limits) throws IOException {
        var normalizedInstall = requireEmptyDirectory(installDirectory);
        var paths = new HashSet<Path>();
        try (var input = new DataInputStream(new BufferedInputStream(Files.newInputStream(
                snapshotFile,
                LinkOption.NOFOLLOW_LINKS
        )))) {
            if (input.readInt() != MAGIC) {
                throw new IOException("Unsupported Rockserver Ratis snapshot archive magic");
            }
            if (input.readInt() != FORMAT_VERSION) {
                throw new IOException("Unsupported Rockserver Ratis snapshot archive version");
            }
            var fileCount = input.readInt();
            if (fileCount < 0 || fileCount > limits.maxFileCount()) {
                throw new IOException("Rockserver Ratis snapshot archive file count exceeds supported limit: "
                        + fileCount);
            }
            var expectedTotalSizeBytes = input.readLong();
            validateTotalSizeBytes(expectedTotalSizeBytes, limits);

            long totalSizeBytes = 0;
            for (int i = 0; i < fileCount; i++) {
                var relativePath = readRelativePath(input);
                if (!paths.add(relativePath)) {
                    throw new IOException("Duplicate Rockserver Ratis snapshot archive path: " + relativePath);
                }
                var sizeBytes = input.readLong();
                if (sizeBytes < 0) {
                    throw new IOException("Negative Rockserver Ratis snapshot archive file size: " + sizeBytes);
                }
                totalSizeBytes = addExact(totalSizeBytes, sizeBytes, "snapshot archive extracted size");
                validateTotalSizeBytes(totalSizeBytes, limits);
                if (totalSizeBytes > expectedTotalSizeBytes) {
                    throw new IOException("Rockserver Ratis snapshot archive files exceed declared total size");
                }

                var target = normalizedInstall.resolve(relativePath).normalize();
                if (!target.startsWith(normalizedInstall)) {
                    throw new IOException("Rockserver Ratis snapshot archive path escapes target: " + relativePath);
                }
                DurableAtomicFileWriter.createDirectoriesDurably(
                        Objects.requireNonNull(target.getParent(), "snapshot archive target parent")
                );
                var checksum = copyExact(input, target, sizeBytes);
                DurableAtomicFileWriter.forceDirectory(
                        Objects.requireNonNull(target.getParent(), "snapshot archive target parent")
                );
                var expectedChecksum = input.readLong();
                if (expectedChecksum < 0 || expectedChecksum > 0xFFFF_FFFFL || expectedChecksum != checksum) {
                    throw new IOException("Rockserver Ratis snapshot archive checksum mismatch: " + relativePath);
                }
            }
            if (totalSizeBytes != expectedTotalSizeBytes) {
                throw new IOException("Rockserver Ratis snapshot archive expected " + expectedTotalSizeBytes
                        + " bytes but extracted " + totalSizeBytes);
            }
            if (input.read() != -1) {
                throw new IOException("Rockserver Ratis snapshot archive contains trailing bytes");
            }
        } catch (EOFException e) {
            throw new IOException("Truncated Rockserver Ratis snapshot archive", e);
        }
    }

    private static long copyExact(Path source, long sizeBytes, DataOutputStream output) throws IOException {
        var checksum = new CRC32C();
        var buffer = new byte[COPY_BUFFER_SIZE_BYTES];
        long copiedBytes = 0;
        try (var input = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS)) {
            while (copiedBytes < sizeBytes) {
                var expectedBytes = (int) Math.min(buffer.length, sizeBytes - copiedBytes);
                var readBytes = input.read(buffer, 0, expectedBytes);
                if (readBytes < 0) {
                    throw new EOFException("RocksDB checkpoint file shrank while archiving: " + source);
                }
                output.write(buffer, 0, readBytes);
                checksum.update(buffer, 0, readBytes);
                copiedBytes += readBytes;
            }
            if (input.read() != -1) {
                throw new IOException("RocksDB checkpoint file grew while archiving: " + source);
            }
        }
        return checksum.getValue();
    }

    private static long copyExact(DataInputStream input, Path target, long sizeBytes) throws IOException {
        var checksum = new CRC32C();
        var buffer = new byte[COPY_BUFFER_SIZE_BYTES];
        long copiedBytes = 0;
        try (var channel = FileChannel.open(
                target,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS
        ); var output = Channels.newOutputStream(channel)) {
            while (copiedBytes < sizeBytes) {
                var expectedBytes = (int) Math.min(buffer.length, sizeBytes - copiedBytes);
                var readBytes = input.read(buffer, 0, expectedBytes);
                if (readBytes < 0) {
                    throw new EOFException("Rockserver Ratis snapshot archive file is truncated: " + target);
                }
                output.write(buffer, 0, readBytes);
                checksum.update(buffer, 0, readBytes);
                copiedBytes += readBytes;
            }
            output.flush();
            channel.force(true);
        }
        return checksum.getValue();
    }

    private static Path readRelativePath(DataInputStream input) throws IOException {
        var pathSizeBytes = input.readInt();
        if (pathSizeBytes <= 0 || pathSizeBytes > MAX_RELATIVE_PATH_BYTES) {
            throw new IOException("Rockserver Ratis snapshot archive path length exceeds supported limit: "
                    + pathSizeBytes);
        }
        var pathBytes = input.readNBytes(pathSizeBytes);
        if (pathBytes.length != pathSizeBytes) {
            throw new EOFException("Rockserver Ratis snapshot archive path is truncated");
        }
        final String encodedPath;
        try {
            encodedPath = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(pathBytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new IOException("Rockserver Ratis snapshot archive path is not valid UTF-8", e);
        }
        var components = encodedPath.split("/", -1);
        for (var component : components) {
            if (component.isBlank() || component.equals(".") || component.equals("..")
                    || component.indexOf('\\') >= 0 || component.indexOf('\0') >= 0) {
                throw new IOException("Invalid Rockserver Ratis snapshot archive path: " + encodedPath);
            }
        }
        var relativePath = Path.of(components[0], Arrays.copyOfRange(components, 1, components.length));
        if (relativePath.isAbsolute()) {
            throw new IOException("Absolute Rockserver Ratis snapshot archive path: " + encodedPath);
        }
        return relativePath;
    }

    private static List<ArchiveFile> listFiles(Path checkpointDirectory,
                                               RocksDBCheckpointSnapshotSupport.SnapshotLimits limits)
            throws IOException {
        Objects.requireNonNull(limits, "limits");
        var files = new ArrayList<ArchiveFile>();
        Files.walkFileTree(checkpointDirectory, new SimpleFileVisitor<>() {
            @Override
            public @NotNull FileVisitResult visitFile(@NotNull Path file,
                                                       @NotNull BasicFileAttributes attributes) throws IOException {
                if (!attributes.isRegularFile()) {
                    throw new IOException("RocksDB checkpoint contains unsupported non-regular file: " + file);
                }
                if (files.size() >= limits.maxFileCount()) {
                    throw new IOException("RocksDB checkpoint file count exceeds supported limit: "
                            + limits.maxFileCount());
                }
                var relativePath = relativePath(checkpointDirectory, file);
                var relativePathBytes = relativePath.getBytes(StandardCharsets.UTF_8);
                if (relativePathBytes.length > MAX_RELATIVE_PATH_BYTES) {
                    throw new IOException("RocksDB checkpoint relative path exceeds supported limit: " + relativePath);
                }
                files.add(new ArchiveFile(file, relativePath, relativePathBytes, attributes.size()));
                return FileVisitResult.CONTINUE;
            }
        });
        files.sort(Comparator.comparing(ArchiveFile::relativePath));
        totalSizeBytes(metadata(files), limits);
        return List.copyOf(files);
    }

    private static String relativePath(Path checkpointDirectory, Path file) throws IOException {
        var relative = checkpointDirectory.relativize(file);
        var components = new ArrayList<String>(relative.getNameCount());
        for (var component : relative) {
            var value = component.toString();
            if (value.isBlank() || value.equals(".") || value.equals("..") || value.indexOf('\\') >= 0
                    || value.indexOf('\0') >= 0) {
                throw new IOException("Invalid RocksDB checkpoint relative path: " + relative);
            }
            components.add(value);
        }
        if (components.isEmpty()) {
            throw new IOException("RocksDB checkpoint file path must not be empty: " + file);
        }
        return String.join("/", components);
    }

    private static List<ArchiveEntry> metadata(List<ArchiveFile> files) {
        return files.stream()
                .map(file -> new ArchiveEntry(file.relativePath(), file.sizeBytes()))
                .toList();
    }

    private static long totalSizeBytes(List<ArchiveEntry> entries,
                                       RocksDBCheckpointSnapshotSupport.SnapshotLimits limits) throws IOException {
        long totalSizeBytes = 0;
        for (var file : entries) {
            totalSizeBytes = addExact(totalSizeBytes, file.sizeBytes(), "snapshot archive admitted size");
            validateTotalSizeBytes(totalSizeBytes, limits);
        }
        return totalSizeBytes;
    }

    private static void validateTotalSizeBytes(long sizeBytes,
                                               RocksDBCheckpointSnapshotSupport.SnapshotLimits limits)
            throws IOException {
        if (sizeBytes < 0 || sizeBytes > limits.maxTotalSizeBytes()) {
            throw new IOException("Rockserver Ratis snapshot archive size exceeds supported limit: " + sizeBytes);
        }
    }

    private static long addExact(long first, long second, String description) throws IOException {
        try {
            return Math.addExact(first, second);
        } catch (ArithmeticException e) {
            throw new IOException(description + " exceeds supported range", e);
        }
    }

    private static Path requireDirectory(Path directory, String description) throws IOException {
        var normalized = Objects.requireNonNull(directory, description).toAbsolutePath().normalize();
        var attributes = Files.readAttributes(normalized, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isDirectory()) {
            throw new IOException(description + " must be a directory: " + normalized);
        }
        return normalized;
    }

    private static Path requireEmptyDirectory(Path directory) throws IOException {
        var normalized = requireDirectory(directory, "snapshot install directory");
        try (var children = Files.list(normalized)) {
            if (children.findAny().isPresent()) {
                throw new IOException("Snapshot install directory must be empty: " + normalized);
            }
        }
        return normalized;
    }

    static void deleteRecursively(Path path) throws IOException {
        if (path == null || readAttributesIfExists(path) == null) {
            return;
        }
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public @NotNull FileVisitResult visitFile(@NotNull Path file,
                                                       @NotNull BasicFileAttributes attributes) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public @NotNull FileVisitResult postVisitDirectory(@NotNull Path directory,
                                                               IOException failure) throws IOException {
                if (failure != null) {
                    throw failure;
                }
                Files.deleteIfExists(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static BasicFileAttributes readAttributesIfExists(Path path) throws IOException {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException _) {
            return null;
        }
    }

    private static void cleanupInstallDirectory(Path path, Throwable failure) throws IOException {
        if (path == null) {
            return;
        }
        try {
            deleteRecursively(path);
        } catch (IOException e) {
            var cleanupFailure = new IOException(
                    "Failed to delete temporary Rockserver Ratis snapshot install directory: " + path,
                    e
            );
            if (failure != null) {
                failure.addSuppressed(cleanupFailure);
                return;
            }
            throw cleanupFailure;
        }
    }

    private record ArchiveFile(Path path, String relativePath, byte[] relativePathBytes, long sizeBytes) {
    }

    private record ArchiveEntry(String relativePath, long sizeBytes) {
    }
}
