package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Bounded atomic files; caller holds the stable journal lock and fences on any write failure. */
final class SessionJournalFiles {
    private SessionJournalFiles() {}
    @FunctionalInterface interface Removal {
        void delete(Path path) throws IOException;
        default void force(Path directory) throws IOException { SessionJournalFiles.force(directory); }
    }
    static final Removal FILESYSTEM=Files::delete;
    /** Caller retains its stable lock and has established that discarding the saved outcome is allowed. */
    static void remove(Path path,byte[] exactBody,int maximum) throws IOException { remove(path,exactBody,maximum,FILESYSTEM); }
    static void remove(Path path,byte[] exactBody,int maximum,Removal io) throws IOException {
        if ((long)exactBody.length+32>maximum || !Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid owned journal for reclamation");
        Path pending=path.resolveSibling(path.getFileName()+".pending"); boolean sidecar=Files.exists(pending,LinkOption.NOFOLLOW_LINKS);
        if (sidecar && (!Files.isRegularFile(pending,LinkOption.NOFOLLOW_LINKS) || Files.size(pending)>maximum)) throw new IOException("Invalid unpublished journal sidecar");
        // Compare without materializing a second full journal; large accepted plans may still be retained.
        try (var file=FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)) {
            if (file.size()!=(long)exactBody.length+32) throw new IOException("Owned journal changed before reclamation");
            var buffer=ByteBuffer.allocate(Math.min(64*1024,Math.max(32,exactBody.length))); int offset=0;
            while (offset<exactBody.length) {
                int count=Math.min(buffer.capacity(),exactBody.length-offset); buffer.clear().limit(count);
                while (buffer.hasRemaining()) if (file.read(buffer)<0) throw new IOException("Truncated owned journal");
                if (Arrays.mismatch(buffer.array(),0,count,exactBody,offset,offset+count)!=-1) throw new IOException("Owned journal contents changed before reclamation");
                offset+=count;
            }
            buffer.clear().limit(32); while (buffer.hasRemaining()) if (file.read(buffer)<0) throw new IOException("Truncated owned journal checksum");
            if (!MessageDigest.isEqual(digest(exactBody),Arrays.copyOf(buffer.array(),32)) || file.size()!=(long)exactBody.length+32) throw new IOException("Owned journal checksum changed before reclamation");
        }
        if (sidecar) io.delete(pending);
        io.delete(path); io.force(path.getParent());
    }
    static void write(Path path,byte[] body,int maximum) throws IOException {
        if (body.length+32>maximum) throw new IOException("Session journal exceeds bound");
        Path temporary=path.resolveSibling(path.getFileName()+".pending");
        try {
            try (var file=FileChannel.open(temporary,StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE)) {
                var buffer=ByteBuffer.allocate(body.length+32).put(body).put(digest(body)).flip();
                while (buffer.hasRemaining()) file.write(buffer); file.force(true);
            }
            Files.move(temporary,path,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING); temporary=null;
            force(path.getParent());
        } finally { if (temporary!=null) Files.deleteIfExists(temporary); }
    }
    static byte[] read(Path path,int maximum) throws IOException {
        long size=Files.size(path); if (size<32 || size>maximum) throw new IOException("Invalid session journal size");
        byte[] encoded=Files.readAllBytes(path), body=Arrays.copyOf(encoded,encoded.length-32);
        if (!MessageDigest.isEqual(digest(body),Arrays.copyOfRange(encoded,body.length,encoded.length))) throw new IOException("Session journal checksum mismatch");
        return body;
    }
    static void force(Path path) throws IOException { try (var file=FileChannel.open(path,StandardOpenOption.READ)) { file.force(true); } }
    private static byte[] digest(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
}
