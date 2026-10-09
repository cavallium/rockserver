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

    public record Chunk(Manifest manifest,boolean hasMore) {
        public Chunk { Objects.requireNonNull(manifest); if (hasMore && (manifest.rows()==0 || manifest.upperExclusive()==null)) throw new IllegalArgumentException("Chunk boundary needs progress and an exact next key"); }
    }
    static final class Prepared {
        final Manifest manifest; final boolean more; final Path temporary,target; final long started,duration;
        Prepared(Manifest manifest,boolean more,Path temporary,Path target,long started,long duration) { this.manifest=manifest; this.more=more; this.temporary=temporary; this.target=target; this.started=started; this.duration=duration; }
    }
    /** Source options must belong to this column's retained pool. Caller retains the scoped snapshot. */
    public static Manifest write(Path destination,ReplicatedRangeStore.ScopedSnapshot snapshot,UUID column,
            byte[] lowerInclusive,byte[] upperExclusive,DBOptions databaseOptions,ColumnFamilyOptions columnOptions,Limits limits) throws IOException,RocksDBException {
        return writeRetained(destination,snapshot,column,lowerInclusive,upperExclusive,databaseOptions,columnOptions,limits,false).manifest();
    }
    /** Emit one bounded exact coverage interval, ending at the first unconsumed physical key. */
    public static Chunk writeChunk(Path destination,ReplicatedRangeStore.ScopedSnapshot snapshot,UUID column,
            byte[] lowerInclusive,byte[] upperExclusive,DBOptions databaseOptions,ColumnFamilyOptions columnOptions,Limits limits) throws IOException,RocksDBException {
        return writeRetained(destination,snapshot,column,lowerInclusive,upperExclusive,databaseOptions,columnOptions,limits,true);
    }
    private static Chunk writeRetained(Path destination,ReplicatedRangeStore.ScopedSnapshot snapshot,UUID column,
            byte[] lower,byte[] upper,DBOptions databaseOptions,ColumnFamilyOptions columnOptions,Limits limits,boolean chunk) throws IOException,RocksDBException {
        Prepared prepared=null;
        try {
            try (var nativeUse=Objects.requireNonNull(snapshot).retainForExport()) { prepared=prepare(destination,snapshot,column,lower,upper,databaseOptions,columnOptions,limits,chunk); }
            return publish(prepared);
        } finally { discard(prepared); }
    }
    /** The pool retains options/native ownership around this build, then closes that retain before publish. */
    static Prepared prepare(Path destination,ReplicatedRangeStore.ScopedSnapshot snapshot,UUID column,
            byte[] lowerInclusive,byte[] upperExclusive,DBOptions databaseOptions,ColumnFamilyOptions columnOptions,Limits limits,boolean chunk) throws IOException,RocksDBException {
        Objects.requireNonNull(snapshot); Objects.requireNonNull(column); Objects.requireNonNull(limits); Objects.requireNonNull(databaseOptions); Objects.requireNonNull(columnOptions);
        if (snapshot.position()==null) throw new IllegalArgumentException("SST export requires a completed applied cut");
        if ((long)(lowerInclusive==null ? 0 : lowerInclusive.length)+(upperExclusive==null ? 0 : upperExclusive.length)>TransactionScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("SST export bounds exceed budget");
        byte[] lower=lowerInclusive==null ? snapshot.interval().lowerInclusive() : lowerInclusive.clone(),upper=upperExclusive==null ? snapshot.interval().upperExclusive() : upperExclusive.clone();
        long started=System.nanoTime(),duration=limits.maximumDuration().toNanos(); Path target=Objects.requireNonNull(destination).toAbsolutePath();
        if (Files.exists(target,LinkOption.NOFOLLOW_LINKS)) throw new FileAlreadyExistsException(target.toString());
        Path temporary=null; boolean complete=false; Prepared result;
        try {
            try (var cursor=snapshot.openCursor(column,lower,upper,false)) {
                temporary=Files.createTempFile(target.getParent(),".range-sst-building-",".tmp"); long rows=0,payload=0; boolean more=false; byte[] boundary=upper;
                try (var env=new EnvOptions(databaseOptions); var options=new Options(databaseOptions,columnOptions); var writer=new SstFileWriter(env,options)) {
                    writer.open(temporary.toString());
                    scan: while (true) {
                        deadline(started,duration);
                        long remaining=limits.maximumRows()-rows;
                        int count=chunk ? (int)(remaining>=PhysicalRangePage.MAX_ITEMS ? PhysicalRangePage.MAX_ITEMS : remaining+1) : PhysicalRangePage.MAX_ITEMS;
                        final PhysicalRangePage page;
                        try { page=cursor.nextPage(count,PhysicalRangePage.MAX_BYTES); }
                        catch (PhysicalRangePage.LimitExceededException oversized) { throw new IOException("SST export physical row exceeds bounded materialization",oversized); }
                        for (var row:page.rows()) {
                            byte[] key=row.key(),value=row.value(); long bytes=(long)key.length+value.length;
                            if (rows>=limits.maximumRows() || bytes>limits.maximumPayloadBytes()-payload) {
                                if (!chunk || rows==0) throw new IOException("SST export row/payload limit exceeded");
                                boundary=key; more=true; break scan;
                            }
                            deadline(started,duration); writer.put(key,value); rows++; payload+=bytes;
                            if (writer.fileSize()>limits.maximumFileBytes()) throw new IOException("SST export file limit exceeded");
                        }
                        if (!page.hasMore()) break;
                    }
                    if (rows!=0) writer.finish();
                }
                deadline(started,duration); long fileBytes=rows==0 ? 0 : Files.size(temporary);
                if (fileBytes>limits.maximumFileBytes()) throw new IOException("SST export file limit exceeded");
                result=new Prepared(manifest(snapshot,column,lower,boundary,rows,payload,fileBytes,new byte[rows==0 ? 0 : 32],rows==0 ? null : target),more,temporary,target,started,duration);
            }
            complete=true; return result;
        } finally { if (!complete && temporary!=null) Files.deleteIfExists(temporary); }
    }
    /** No native owner remains here; the link is published only after writer/cursor/retains close. */
    static Chunk publish(Prepared prepared) throws IOException {
        Objects.requireNonNull(prepared); var built=prepared.manifest;
        deadline(prepared.started,prepared.duration);
        if (built.rows()==0) return new Chunk(built,false);
        force(prepared.temporary); byte[] checksum=sha256(prepared.temporary,prepared.started,prepared.duration); deadline(prepared.started,prepared.duration);
        boolean linked=false,complete=false;
        try {
            Files.createLink(prepared.target,prepared.temporary); linked=true; force(prepared.target.getParent());
            Files.delete(prepared.temporary); force(prepared.target.getParent()); deadline(prepared.started,prepared.duration);
            var result=new Manifest(built.groupId(),built.rangeEpoch(),built.columnId(),built.schemaDigest(),built.position(),built.lowerInclusive(),built.upperExclusive(),built.rows(),built.payloadBytes(),built.fileBytes(),checksum,prepared.target);
            complete=true; return new Chunk(result,prepared.more);
        } finally { if (linked && !complete) { Files.deleteIfExists(prepared.target); force(prepared.target.getParent()); } }
    }
    static void discard(Prepared prepared) throws IOException { if (prepared!=null) Files.deleteIfExists(prepared.temporary); }
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
