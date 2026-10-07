package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.rocksdb.*;

/** Materialized user-column SST from one retained cut. This is not a complete replicated range image. */
public final class NativeRangeSstExport {
    public record Limits(long maximumRows, long maximumPayloadBytes, long maximumFileBytes, Duration maximumDuration) {
        public Limits {
            Objects.requireNonNull(maximumDuration);
            if (maximumRows < 1 || maximumPayloadBytes < 1 || maximumFileBytes < 1
                    || maximumDuration.isZero() || maximumDuration.isNegative()) throw new IllegalArgumentException("Invalid SST export limits");
            maximumDuration.toNanos();
        }
    }
    public record Manifest(UUID groupId, long rangeEpoch, UUID columnId, byte[] schemaDigest, ApplyPosition position,
                           byte[] lowerInclusive, byte[] upperExclusive, long rows, long payloadBytes,
                           long fileBytes, byte[] sha256, Path file) {
        public Manifest {
            Objects.requireNonNull(groupId); Objects.requireNonNull(columnId); Objects.requireNonNull(position);
            if (rangeEpoch < 0 || rows < 0 || payloadBytes < 0 || fileBytes < 0 || schemaDigest.length != 32
                    || (lowerInclusive != null && upperExclusive != null && java.util.Arrays.compareUnsigned(lowerInclusive, upperExclusive) >= 0)
                    || (rows == 0 ? file != null || payloadBytes != 0 || fileBytes != 0 || sha256.length != 0 : file == null || !file.isAbsolute() || fileBytes == 0 || sha256.length != 32)) {
                throw new IllegalArgumentException("Invalid SST export manifest");
            }
            schemaDigest=schemaDigest.clone(); sha256=sha256.clone();
            lowerInclusive=lowerInclusive == null ? null : lowerInclusive.clone();
            upperExclusive=upperExclusive == null ? null : upperExclusive.clone();
        }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
        @Override public byte[] sha256() { return sha256.clone(); }
        @Override public byte[] lowerInclusive() { return lowerInclusive == null ? null : lowerInclusive.clone(); }
        @Override public byte[] upperExclusive() { return upperExclusive == null ? null : upperExclusive.clone(); }
    }
    private NativeRangeSstExport() {}

    /** Source options must belong to this column's retained pool. Caller retains the scoped snapshot. */
    public static Manifest write(Path destination, ReplicatedRangeStore.ScopedSnapshot snapshot, UUID column,
                                 byte[] lowerInclusive, byte[] upperExclusive, DBOptions databaseOptions,
                                 ColumnFamilyOptions columnOptions, Limits limits) throws IOException, RocksDBException {
        Objects.requireNonNull(snapshot); Objects.requireNonNull(column); Objects.requireNonNull(limits);
        Objects.requireNonNull(databaseOptions); Objects.requireNonNull(columnOptions);
        if (snapshot.position() == null) throw new IllegalArgumentException("SST export requires a completed applied cut");
        if ((long)(lowerInclusive == null ? 0 : lowerInclusive.length) + (upperExclusive == null ? 0 : upperExclusive.length) > TransactionScope.MAX_BOUND_BYTES) {
            throw new IllegalArgumentException("SST export bounds exceed budget");
        }
        byte[] lower=lowerInclusive == null ? snapshot.interval().lowerInclusive() : lowerInclusive.clone();
        byte[] upper=upperExclusive == null ? snapshot.interval().upperExclusive() : upperExclusive.clone();
        long started=System.nanoTime(), duration=limits.maximumDuration().toNanos();
        Path target=Objects.requireNonNull(destination).toAbsolutePath();
        Path directory=target.getParent();
        if (Files.exists(target,LinkOption.NOFOLLOW_LINKS)) throw new FileAlreadyExistsException(target.toString());
        try (var nativeUse=snapshot.retainForExport(); var cursor=snapshot.openCursor(column,lower,upper,false)) {
            Path temporary=Files.createTempFile(directory,".range-sst-building-",".tmp");
            long rows=0, payload=0;
            try {
                try (var env=new EnvOptions(databaseOptions); var options=new Options(databaseOptions,columnOptions);
                        var writer=new SstFileWriter(env,options)) {
                    writer.open(temporary.toString());
                    while (true) {
                        deadline(started,duration);
                        var page=cursor.nextPage(PhysicalRangePage.MAX_ITEMS,PhysicalRangePage.MAX_BYTES);
                        for (var row : page.rows()) {
                            byte[] key=row.key(), value=row.value();
                            long bytes=(long)key.length+value.length;
                            if (rows >= limits.maximumRows() || bytes > limits.maximumPayloadBytes()-payload) throw new IOException("SST export row/payload limit exceeded");
                            deadline(started,duration);
                            writer.put(key,value);
                            rows++; payload+=bytes;
                        }
                        if (writer.fileSize() > limits.maximumFileBytes()) throw new IOException("SST export file limit exceeded");
                        if (!page.hasMore()) break;
                    }
                    if (rows != 0) writer.finish();
                }
                if (rows == 0) return manifest(snapshot,column,lower,upper,0,0,0,new byte[0],null);
                deadline(started,duration);
                long fileBytes=Files.size(temporary);
                if (fileBytes > limits.maximumFileBytes()) throw new IOException("SST export file limit exceeded");
                force(temporary);
                byte[] checksum=sha256(temporary,started,duration);
                deadline(started,duration);
                Files.createLink(target,temporary);
                force(directory);
                Files.delete(temporary);
                force(directory);
                return manifest(snapshot,column,lower,upper,rows,payload,fileBytes,checksum,target);
            } finally { Files.deleteIfExists(temporary); }
        }
    }
    private static Manifest manifest(ReplicatedRangeStore.ScopedSnapshot snapshot, UUID column, byte[] lower, byte[] upper,
                                     long rows, long payload, long bytes, byte[] hash, Path file) {
        return new Manifest(snapshot.groupId(),snapshot.rangeEpoch(),column,snapshot.schemaDigest(),snapshot.position(),
                lower,upper,rows,payload,bytes,hash,file);
    }
    private static void deadline(long started,long duration) throws IOException {
        if (System.nanoTime()-started >= duration) throw new IOException("SST export deadline exceeded");
    }
    private static void force(Path path) throws IOException {
        try (var channel=FileChannel.open(path,StandardOpenOption.READ)) { channel.force(true); }
    }
    private static byte[] sha256(Path file,long started,long duration) throws IOException {
        MessageDigest digest;
        try { digest=MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        try (var input=Files.newInputStream(file)) {
            byte[] buffer=new byte[64*1024];
            int read;
            while ((read=input.read(buffer)) != -1) { deadline(started,duration); digest.update(buffer,0,read); }
        }
        return digest.digest();
    }
}
