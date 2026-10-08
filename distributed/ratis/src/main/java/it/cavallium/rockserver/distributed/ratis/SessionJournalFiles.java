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
