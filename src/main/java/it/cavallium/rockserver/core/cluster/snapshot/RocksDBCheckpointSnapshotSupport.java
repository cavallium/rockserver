package it.cavallium.rockserver.core.cluster.snapshot;

import it.cavallium.rockserver.core.cluster.internal.DurableAtomicFileWriter;
import it.cavallium.rockserver.core.common.RocksDBException;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RocksDBCheckpointSnapshotSupport {

    public static final int DEFAULT_CHUNK_SIZE_BYTES = 1024 * 1024;
    public static final int MAX_CHUNK_SIZE_BYTES = 64 * 1024 * 1024;
    public static final SnapshotLimits DEFAULT_LIMITS = new SnapshotLimits(100_000, 1L << 40, 1_000_000);
    public static final String SNAPSHOT_BACKUP_SUFFIX = ".snapshot-backup";

    private static final int FORMAT_VERSION = 1;
    private static final String SNAPSHOT_INSTALL_LOCK_SUFFIX = ".snapshot-install.lock";
    private static final Object SNAPSHOT_INSTALL_LOCK = new Object();
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBCheckpointSnapshotSupport.class);

    private final CheckpointWriter checkpointWriter;
    private final Path workDirectory;
    private final SnapshotInstaller snapshotInstaller;
    private final int chunkSizeBytes;
    private final SnapshotLimits limits;

    public RocksDBCheckpointSnapshotSupport(
            CheckpointWriter checkpointWriter,
            Path workDirectory,
            SnapshotInstaller snapshotInstaller
    ) {
        this(checkpointWriter, workDirectory, snapshotInstaller, DEFAULT_CHUNK_SIZE_BYTES);
    }

    public RocksDBCheckpointSnapshotSupport(
            CheckpointWriter checkpointWriter,
            Path workDirectory,
            SnapshotInstaller snapshotInstaller,
            int chunkSizeBytes
    ) {
        this(checkpointWriter, workDirectory, snapshotInstaller, chunkSizeBytes, DEFAULT_LIMITS);
    }

    public RocksDBCheckpointSnapshotSupport(
            CheckpointWriter checkpointWriter,
            Path workDirectory,
            SnapshotInstaller snapshotInstaller,
            int chunkSizeBytes,
            SnapshotLimits limits
    ) {
        this.checkpointWriter = requireArgument(checkpointWriter, "checkpointWriter");
        this.workDirectory = requireArgument(workDirectory, "workDirectory").toAbsolutePath().normalize();
        this.snapshotInstaller = requireArgument(snapshotInstaller, "snapshotInstaller");
        this.chunkSizeBytes = requireChunkSizeBytes(chunkSizeBytes);
        this.limits = requireArgument(limits, "limits");
    }

    public static RocksDBCheckpointSnapshotSupport forRocksDB(
            RocksDB db,
            Path workDirectory,
            SnapshotInstaller snapshotInstaller
    ) {
        return forRocksDB(db, workDirectory, snapshotInstaller, DEFAULT_CHUNK_SIZE_BYTES);
    }

    public static RocksDBCheckpointSnapshotSupport forRocksDB(
            RocksDB db,
            Path workDirectory,
            SnapshotInstaller snapshotInstaller,
            int chunkSizeBytes
    ) {
        return forRocksDB(db, workDirectory, snapshotInstaller, chunkSizeBytes, DEFAULT_LIMITS);
    }

    public static RocksDBCheckpointSnapshotSupport forRocksDB(
            RocksDB db,
            Path workDirectory,
            SnapshotInstaller snapshotInstaller,
            int chunkSizeBytes,
            SnapshotLimits limits
    ) {
        var requiredDb = requireArgument(db, "db");
        return new RocksDBCheckpointSnapshotSupport(checkpointDirectory -> {
            try (var checkpoint = Checkpoint.create(requiredDb)) {
                checkpoint.createCheckpoint(checkpointDirectory.toString());
            } catch (org.rocksdb.RocksDBException e) {
                throw new IOException("Failed to create RocksDB checkpoint", e);
            }
        }, workDirectory, snapshotInstaller, chunkSizeBytes, limits);
    }

    public static SnapshotInstaller replacingClosedDatabaseDirectory(Path targetDirectory) {
        var normalizedTarget = requireArgument(targetDirectory, "targetDirectory").toAbsolutePath().normalize();
        var parent = normalizedTarget.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("targetDirectory must have a parent");
        }
        return checkpointDirectory -> withSnapshotInstallLock(normalizedTarget, () -> {
            recoverInterruptedClosedDatabaseDirectoryInstallLocked(normalizedTarget);
            var backup = snapshotBackupDirectory(normalizedTarget);
            boolean backedUp = false;
            boolean installed = false;
            try {
                var targetAttributes = readAttributesIfExists(normalizedTarget);
                if (targetAttributes != null) {
                    if (!targetAttributes.isDirectory()) {
                        throw new IOException("RocksDB target must be a directory: " + normalizedTarget);
                    }
                    moveDirectoryAtomically(normalizedTarget, backup);
                    backedUp = true;
                    forceRenameDirectories(normalizedTarget, backup);
                }
                moveDirectoryAtomically(checkpointDirectory, normalizedTarget);
                installed = true;
                forceRenameDirectories(checkpointDirectory, normalizedTarget);
                if (backedUp) {
                    deleteBackupQuietly(backup);
                }
            } catch (IOException e) {
                if (installed) {
                    try {
                        deleteRecursive(normalizedTarget);
                    } catch (IOException restoreFailure) {
                        e.addSuppressed(restoreFailure);
                    }
                }
                if (backedUp && readAttributesIfExists(normalizedTarget) == null) {
                    try {
                        moveDirectoryAtomically(backup, normalizedTarget);
                        forceRenameDirectories(backup, normalizedTarget);
                    } catch (IOException restoreFailure) {
                        e.addSuppressed(restoreFailure);
                    }
                }
                throw e;
            }
        });
    }

    public static void recoverInterruptedClosedDatabaseDirectoryInstall(Path targetDirectory) throws IOException {
        var normalizedTarget = requireArgument(targetDirectory, "targetDirectory").toAbsolutePath().normalize();
        var parent = normalizedTarget.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("targetDirectory must have a parent");
        }
        withSnapshotInstallLock(normalizedTarget,
                () -> recoverInterruptedClosedDatabaseDirectoryInstallLocked(normalizedTarget));
    }

    private static void recoverInterruptedClosedDatabaseDirectoryInstallLocked(Path normalizedTarget)
            throws IOException {
        var parent = requireParent(normalizedTarget, "targetDirectory");
        var backup = snapshotBackupDirectory(normalizedTarget);
        var backupAttributes = readAttributesIfExists(backup);
        if (backupAttributes == null) {
            return;
        }
        if (!backupAttributes.isDirectory()) {
            throw new IOException("RocksDB snapshot backup must be a directory: " + backup);
        }
        var targetAttributes = readAttributesIfExists(normalizedTarget);
        if (targetAttributes == null) {
            moveDirectoryAtomically(backup, normalizedTarget);
            forceRenameDirectories(backup, normalizedTarget);
            return;
        }
        if (!targetAttributes.isDirectory()) {
            throw new IOException("RocksDB target must be a directory: " + normalizedTarget);
        }
        deleteRecursive(backup);
        DurableAtomicFileWriter.forceDirectory(parent);
    }

    private static void withSnapshotInstallLock(Path normalizedTarget, IOAction action) throws IOException {
        var parent = requireParent(normalizedTarget, "targetDirectory");
        DurableAtomicFileWriter.createDirectoriesDurably(parent);
        var lockFile = normalizedTarget.resolveSibling(normalizedTarget.getFileName() + SNAPSHOT_INSTALL_LOCK_SUFFIX);
        synchronized (SNAPSHOT_INSTALL_LOCK) {
            // The monitor prevents overlapping JVM locks; the file lock coordinates distinct server processes.
            try (var lockChannel = FileChannel.open(lockFile,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS);
                 var _ = lockChannel.lock()) {
                action.run();
            }
        }
    }

    public void takeSnapshot(long commitIndex, Consumer<Object> snapshotChunkConsumer) {
        requireCommitIndex(commitIndex);
        var requiredSnapshotChunkConsumer = requireArgument(snapshotChunkConsumer, "snapshotChunkConsumer");
        Path checkpointDirectory = null;
        try {
            Files.createDirectories(workDirectory);
            checkpointDirectory = workDirectory.resolve("rocksdb-checkpoint-" + commitIndex + "-" + UUID.randomUUID());
            checkpointWriter.createCheckpoint(checkpointDirectory);
            var files = listCheckpointFiles(checkpointDirectory, limits.maxFileCount());
            long totalSizeBytes = 0;
            long totalChunkCount = 0;
            for (var file : files) {
                totalSizeBytes = addExact(totalSizeBytes, file.sizeBytes(), "checkpoint snapshot total size");
                totalChunkCount = addExact(
                        totalChunkCount,
                        file.chunkCount(),
                        "checkpoint snapshot chunk count"
                );
                limits.validateTotalSizeBytes(totalSizeBytes);
                limits.validateChunkCount(totalChunkCount);
            }
            requiredSnapshotChunkConsumer.accept(new CheckpointSnapshotManifest(
                    FORMAT_VERSION,
                    commitIndex,
                    files.size(),
                    totalSizeBytes,
                    chunkSizeBytes
            ));
            for (var file : files) {
                emitFileChunks(checkpointDirectory, file, requiredSnapshotChunkConsumer);
            }
            if (!files.equals(listCheckpointFiles(checkpointDirectory, limits.maxFileCount()))) {
                throw new IOException("RocksDB checkpoint directory changed while reading: " + checkpointDirectory);
            }
        } catch (IOException e) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                    "Failed to take RocksDB checkpoint snapshot at commit index " + commitIndex,
                    e
            );
        } finally {
            deleteQuietly(checkpointDirectory);
        }
    }

    public void installSnapshot(long commitIndex, List<Object> snapshotChunks) {
        requireCommitIndex(commitIndex);
        var requiredSnapshotChunks = requireArgument(snapshotChunks, "snapshotChunks");
        var manifest = manifest(commitIndex, requiredSnapshotChunks);
        limits.validateManifest(manifest);
        limits.validateChunkCount(requiredSnapshotChunks.size() - 1L);
        var chunksByPath = fileChunks(requiredSnapshotChunks);
        if (chunksByPath.size() != manifest.fileCount()) {
            throw new IllegalArgumentException("RocksDB checkpoint snapshot expected " + manifest.fileCount()
                    + " files but found " + chunksByPath.size());
        }
        long receivedTotalSizeBytes = snapshotSizeBytes(chunksByPath);
        limits.validateTotalSizeBytes(receivedTotalSizeBytes);
        if (receivedTotalSizeBytes != manifest.totalSizeBytes()) {
            throw new IllegalArgumentException("RocksDB checkpoint snapshot expected " + manifest.totalSizeBytes()
                    + " bytes but received " + receivedTotalSizeBytes);
        }
        validateFileChunks(chunksByPath, manifest.chunkSizeBytes());

        Path installDirectory = null;
        try {
            Files.createDirectories(workDirectory);
            installDirectory = Files.createTempDirectory(workDirectory, "rocksdb-install-" + commitIndex + "-");
            long totalSizeBytes = 0;
            for (var entry : chunksByPath.entrySet()) {
                totalSizeBytes = addExact(totalSizeBytes, writeFileChunks(
                        installDirectory,
                        entry.getKey(),
                        entry.getValue()
                ), "RocksDB checkpoint snapshot installed size");
            }
            if (totalSizeBytes != manifest.totalSizeBytes()) {
                throw new IllegalArgumentException("RocksDB checkpoint snapshot expected " + manifest.totalSizeBytes()
                        + " bytes but installed " + totalSizeBytes);
            }
            snapshotInstaller.installSnapshot(installDirectory);
        } catch (IOException e) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                    "Failed to install RocksDB checkpoint snapshot at commit index " + commitIndex,
                    e
            );
        } finally {
            deleteQuietly(installDirectory);
        }
    }

    private void emitFileChunks(Path checkpointDirectory, CheckpointFile file, Consumer<Object> snapshotChunkConsumer)
            throws IOException {
        var relativePath = checkpointDirectory.relativize(file.path()).toString().replace('\\', '/');
        long remainingBytes = file.sizeBytes();
        int chunkCount = Math.toIntExact(file.chunkCount());
        try (var input = Files.newInputStream(file.path(), LinkOption.NOFOLLOW_LINKS)) {
            for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
                int expectedBytes = (int) Math.min(remainingBytes, chunkSizeBytes);
                byte[] bytes = input.readNBytes(expectedBytes);
                if (bytes.length != expectedBytes) {
                    throw new EOFException("RocksDB checkpoint file changed while reading: " + file.path());
                }
                remainingBytes -= bytes.length;
                snapshotChunkConsumer.accept(new CheckpointSnapshotFileChunk(
                        relativePath,
                        chunkIndex,
                        chunkCount,
                        bytes,
                        checksum(bytes)
                ));
            }
            if (input.read() != -1) {
                throw new IOException("RocksDB checkpoint file grew while reading: " + file.path());
            }
        }
    }

    private static CheckpointSnapshotManifest manifest(long commitIndex, List<Object> snapshotChunks) {
        if (snapshotChunks.isEmpty()) {
            throw new IllegalArgumentException("RocksDB checkpoint snapshot is missing its manifest");
        }
        if (!(snapshotChunks.getFirst() instanceof CheckpointSnapshotManifest manifest)) {
            throw new IllegalArgumentException("RocksDB checkpoint snapshot first chunk is not a manifest");
        }
        if (manifest.formatVersion() != FORMAT_VERSION) {
            throw new IllegalArgumentException("Unsupported RocksDB checkpoint snapshot format version: "
                    + manifest.formatVersion());
        }
        if (manifest.commitIndex() != commitIndex) {
            throw new IllegalArgumentException("RocksDB checkpoint snapshot commit index " + manifest.commitIndex()
                    + " does not match install index " + commitIndex);
        }
        return manifest;
    }

    private static Map<Path, List<CheckpointSnapshotFileChunk>> fileChunks(List<Object> snapshotChunks) {
        var chunksByPath = new TreeMap<Path, List<CheckpointSnapshotFileChunk>>();
        for (int i = 1; i < snapshotChunks.size(); i++) {
            var snapshotChunk = snapshotChunks.get(i);
            if (!(snapshotChunk instanceof CheckpointSnapshotFileChunk chunk)) {
                var type = snapshotChunk == null ? "null" : snapshotChunk.getClass().getName();
                throw new IllegalArgumentException("RocksDB checkpoint snapshot chunk " + i
                        + " has unsupported type: " + type);
            }
            chunksByPath.computeIfAbsent(validateRelativePath(chunk.relativePath()), ignored -> new ArrayList<>())
                    .add(chunk);
        }
        return chunksByPath;
    }

    private static long writeFileChunks(Path installDirectory,
                                        Path relativePath,
                                        List<CheckpointSnapshotFileChunk> chunks)
            throws IOException {
        var target = installDirectory.resolve(relativePath).normalize();
        if (!target.startsWith(installDirectory)) {
            throw new IllegalArgumentException("RocksDB checkpoint snapshot file escapes install directory: "
                    + relativePath);
        }
        var parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        long totalSize = 0;
        try (var output = Files.newOutputStream(target)) {
            for (var chunk : chunks) {
                output.write(chunk.bytes);
                totalSize = addExact(totalSize, chunk.sizeBytes(),
                        "RocksDB checkpoint snapshot file " + relativePath + " size");
            }
        }
        return totalSize;
    }

    private static long snapshotSizeBytes(Map<Path, List<CheckpointSnapshotFileChunk>> chunksByPath) {
        long totalSizeBytes = 0;
        for (var chunks : chunksByPath.values()) {
            for (var chunk : chunks) {
                totalSizeBytes = addExact(
                        totalSizeBytes,
                        chunk.sizeBytes(),
                        "RocksDB checkpoint snapshot received size"
                );
            }
        }
        return totalSizeBytes;
    }

    private static void validateFileChunks(Map<Path, List<CheckpointSnapshotFileChunk>> chunksByPath,
                                           int chunkSizeBytes) {
        for (var entry : chunksByPath.entrySet()) {
            var relativePath = entry.getKey();
            var chunks = entry.getValue();
            chunks.sort(Comparator.comparingInt(CheckpointSnapshotFileChunk::chunkIndex));
            int chunkCount = chunks.getFirst().chunkCount();
            if (chunkCount != chunks.size()) {
                throw new IllegalArgumentException("RocksDB checkpoint snapshot file " + relativePath
                        + " expected " + chunkCount + " chunks but found " + chunks.size());
            }
            for (int i = 0; i < chunks.size(); i++) {
                var chunk = chunks.get(i);
                if (chunk.chunkIndex() != i) {
                    throw new IllegalArgumentException("RocksDB checkpoint snapshot file " + relativePath
                            + " is missing chunk " + i);
                }
                if (chunk.chunkCount() != chunkCount) {
                    throw new IllegalArgumentException("RocksDB checkpoint snapshot file " + relativePath
                            + " has inconsistent chunk count");
                }
                validateChunkSize(relativePath, chunk, chunk.sizeBytes(), chunkSizeBytes);
                if (checksum(chunk.bytes) != chunk.crc32c()) {
                    throw new IllegalArgumentException("RocksDB checkpoint snapshot file " + relativePath
                            + " chunk " + i + " failed checksum validation");
                }
            }
        }
    }

    private static Path validateRelativePath(String relativePath) {
        var requiredRelativePath = requireArgument(relativePath, "relativePath");
        var path = Path.of(requiredRelativePath);
        if (path.isAbsolute() || path.getNameCount() == 0) {
            throw new IllegalArgumentException("Invalid RocksDB checkpoint snapshot file path: " + requiredRelativePath);
        }
        for (Path part : path) {
            if ("..".equals(part.toString())) {
                throw new IllegalArgumentException("Invalid RocksDB checkpoint snapshot file path: " + requiredRelativePath);
            }
        }
        var normalized = path.normalize();
        if (normalized.toString().isEmpty()) {
            throw new IllegalArgumentException("Invalid RocksDB checkpoint snapshot file path: " + requiredRelativePath);
        }
        return normalized;
    }

    private static long chunkCount(long fileSize, int chunkSizeBytes) {
        return Math.max(1, Math.ceilDiv(fileSize, (long) chunkSizeBytes));
    }

    private List<CheckpointFile> listCheckpointFiles(Path root, int maxFileCount) throws IOException {
        var rootAttributes = Files.readAttributes(root, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!rootAttributes.isDirectory()) {
            throw new IOException("RocksDB checkpoint root must be a directory: " + root);
        }
        var files = new ArrayList<CheckpointFile>();
        try (var stream = Files.walk(root)) {
            var paths = stream.iterator();
            while (paths.hasNext()) {
                var path = paths.next();
                if (path.equals(root)) {
                    continue;
                }
                var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (attributes.isDirectory()) {
                    continue;
                }
                if (!attributes.isRegularFile()) {
                    throw new IOException("RocksDB checkpoint contains a non-regular path: " + path);
                }
                files.add(new CheckpointFile(path, attributes.size(), chunkCount(attributes.size(), chunkSizeBytes)));
                if (files.size() > maxFileCount) {
                    throw new IllegalArgumentException("checkpoint snapshot file count " + files.size()
                            + " exceeds limit " + maxFileCount);
                }
            }
        }
        files.sort(Comparator.comparing(file -> root.relativize(file.path()).toString()));
        return List.copyOf(files);
    }

    private static long checksum(byte[] bytes) {
        var checksum = new CRC32C();
        checksum.update(bytes);
        return checksum.getValue();
    }

    private static void validateChunkSize(Path relativePath,
                                          CheckpointSnapshotFileChunk chunk,
                                          int bytesLength,
                                          int chunkSizeBytes) {
        if (bytesLength > chunkSizeBytes) {
            throw new IllegalArgumentException("RocksDB checkpoint snapshot file " + relativePath
                    + " chunk " + chunk.chunkIndex() + " exceeds declared chunk size " + chunkSizeBytes);
        }
        if (chunk.chunkIndex() < chunk.chunkCount() - 1 && bytesLength != chunkSizeBytes) {
            throw new IllegalArgumentException("RocksDB checkpoint snapshot file " + relativePath
                    + " chunk " + chunk.chunkIndex() + " must contain " + chunkSizeBytes + " bytes");
        }
        if (chunk.chunkCount() > 1 && bytesLength == 0) {
            throw new IllegalArgumentException("RocksDB checkpoint snapshot file " + relativePath
                    + " final chunk must not be empty");
        }
    }

    private static int requireChunkSizeBytes(int value) {
        if (value <= 0 || value > MAX_CHUNK_SIZE_BYTES) {
            throw new IllegalArgumentException("chunkSizeBytes must be between 1 and " + MAX_CHUNK_SIZE_BYTES);
        }
        return value;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static Path requireParent(Path path, String name) {
        var parent = path.getParent();
        if (parent == null) {
            throw new IllegalArgumentException(name + " must have a parent");
        }
        return parent;
    }

    private static long addExact(long first, long second, String description) {
        try {
            return Math.addExact(first, second);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(description + " exceeds supported range", e);
        }
    }

    private static void requireCommitIndex(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("commitIndex must be non-negative");
        }
    }

    private static void requireCrc32c(long value) {
        if (value < 0 || value > 0xFFFF_FFFFL) {
            throw new IllegalArgumentException("crc32c must be a CRC32C value");
        }
    }

    private static BasicFileAttributes readAttributesIfExists(Path path) throws IOException {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException _) {
            return null;
        }
    }

    private static void moveDirectoryAtomically(Path source, Path target) throws IOException {
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void forceRenameDirectories(Path source, Path target) throws IOException {
        var sourceParent = requireParent(source, "source");
        var targetParent = requireParent(target, "target");
        DurableAtomicFileWriter.forceDirectory(sourceParent);
        if (!sourceParent.equals(targetParent)) {
            DurableAtomicFileWriter.forceDirectory(targetParent);
        }
    }

    private static Path snapshotBackupDirectory(Path targetDirectory) {
        return targetDirectory.resolveSibling(targetDirectory.getFileName() + SNAPSHOT_BACKUP_SUFFIX);
    }

    private static void deleteBackupQuietly(Path backup) {
        try {
            deleteRecursive(backup);
            DurableAtomicFileWriter.forceDirectory(requireParent(backup, "backup"));
        } catch (IOException e) {
            LOG.warn("Failed to delete RocksDB snapshot backup directory {}", backup, e);
        }
    }

    private static void deleteQuietly(Path path) {
        if (path == null) {
            return;
        }
        try {
            deleteRecursive(path);
        } catch (IOException e) {
            LOG.warn("Failed to delete temporary RocksDB checkpoint snapshot directory {}", path, e);
        }
    }

    private static void deleteRecursive(Path path) throws IOException {
        if (readAttributesIfExists(path) == null) {
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

    @FunctionalInterface
    private interface IOAction {

        void run() throws IOException;
    }

    @FunctionalInterface
    public interface CheckpointWriter {

        void createCheckpoint(Path checkpointDirectory) throws IOException;
    }

    private record CheckpointFile(Path path, long sizeBytes, long chunkCount) {
    }

    @FunctionalInterface
    public interface SnapshotInstaller {

        void installSnapshot(Path checkpointDirectory) throws IOException;
    }

    public record CheckpointSnapshotManifest(
            int formatVersion,
            long commitIndex,
            int fileCount,
            long totalSizeBytes,
            int chunkSizeBytes
    ) implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        public CheckpointSnapshotManifest {
            if (formatVersion != FORMAT_VERSION) {
                throw new IllegalArgumentException("formatVersion must be " + FORMAT_VERSION);
            }
            requireCommitIndex(commitIndex);
            if (fileCount < 0) {
                throw new IllegalArgumentException("fileCount must be non-negative");
            }
            if (totalSizeBytes < 0) {
                throw new IllegalArgumentException("totalSizeBytes must be non-negative");
            }
            requireChunkSizeBytes(chunkSizeBytes);
        }
    }

    public record SnapshotLimits(int maxFileCount, long maxTotalSizeBytes, int maxChunkCount) {

        public SnapshotLimits {
            if (maxFileCount < 0) {
                throw new IllegalArgumentException("maxFileCount must be non-negative");
            }
            if (maxTotalSizeBytes < 0) {
                throw new IllegalArgumentException("maxTotalSizeBytes must be non-negative");
            }
            if (maxChunkCount < 0) {
                throw new IllegalArgumentException("maxChunkCount must be non-negative");
            }
        }

        private void validateManifest(CheckpointSnapshotManifest manifest) {
            if (manifest.fileCount() > maxFileCount) {
                throw new IllegalArgumentException("checkpoint snapshot file count " + manifest.fileCount()
                        + " exceeds limit " + maxFileCount);
            }
            validateTotalSizeBytes(manifest.totalSizeBytes());
        }

        private void validateTotalSizeBytes(long totalSizeBytes) {
            if (totalSizeBytes > maxTotalSizeBytes) {
                throw new IllegalArgumentException("checkpoint snapshot total size " + totalSizeBytes
                        + " exceeds limit " + maxTotalSizeBytes);
            }
        }

        private void validateChunkCount(long chunkCount) {
            if (chunkCount > maxChunkCount) {
                throw new IllegalArgumentException("checkpoint snapshot chunk count " + chunkCount
                        + " exceeds limit " + maxChunkCount);
            }
        }
    }

    public record CheckpointSnapshotFileChunk(
            @NotNull String relativePath,
            int chunkIndex,
            int chunkCount,
            byte @NotNull [] bytes,
            long crc32c
    ) implements Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        public CheckpointSnapshotFileChunk {
            var requiredRelativePath = requireArgument(relativePath, "relativePath");
            var requiredBytes = requireArgument(bytes, "bytes");
            bytes = requiredBytes.clone();
            if (requiredRelativePath.isBlank()) {
                throw new IllegalArgumentException("relativePath must not be blank");
            }
            if (chunkIndex < 0) {
                throw new IllegalArgumentException("chunkIndex must be non-negative");
            }
            if (chunkCount <= 0) {
                throw new IllegalArgumentException("chunkCount must be positive");
            }
            if (chunkIndex >= chunkCount) {
                throw new IllegalArgumentException("chunkIndex must be less than chunkCount");
            }
            requireCrc32c(crc32c);
        }

        @Override
        public byte @NotNull [] bytes() {
            return bytes.clone();
        }

        public int sizeBytes() {
            return bytes.length;
        }
    }
}
