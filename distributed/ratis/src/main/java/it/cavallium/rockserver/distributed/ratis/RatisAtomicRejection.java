package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.OperationId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Forced local no-effects CONFLICT, independent of accepted plans and transaction-HOME state. */
public final class RatisAtomicRejection implements AutoCloseable {
    public static final int BODY_BYTES=149,MAX_BYTES=BODY_BYTES+32;
    private static final int MAGIC=0x5241524a,TRANSACTION_MAGIC=0x54584a4c;
    @FunctionalInterface interface Persistence { void write(Path path,byte[] body,int maximum) throws IOException; }
    private record Saved(byte[] binding,RatisAtomicClient.Identity identity,byte[] requestDigest,boolean consumed) {}
    private final Path path;
    private final byte[] binding,requestDigest;
    private final RatisAtomicClient.Identity identity;
    private final FileChannel channel;
    private final FileLock lock;
    private boolean consumed,active,closed,fenced;

    public static RatisAtomicRejection create(Path path,byte[] binding32,RatisAtomicClient.Identity identity,byte[] requestDigest) throws IOException {
        return create(path,binding32,identity,requestDigest,SessionJournalFiles::write);
    }
    static RatisAtomicRejection create(Path path,byte[] binding32,RatisAtomicClient.Identity identity,byte[] requestDigest,Persistence io) throws IOException {
        Objects.requireNonNull(identity); requireDigest(requestDigest,"request digest");
        return new RatisAtomicRejection(path,binding32,identity,requestDigest,Objects.requireNonNull(io));
    }
    public static RatisAtomicRejection reopen(Path path,byte[] binding32) throws IOException {
        return new RatisAtomicRejection(path,binding32,null,null,null);
    }
    private RatisAtomicRejection(Path file,byte[] binding32,RatisAtomicClient.Identity requestedIdentity,byte[] digest,Persistence io) throws IOException {
        path=Objects.requireNonNull(file).toAbsolutePath().normalize(); requireDigest(binding32,"binding"); binding=binding32.clone();
        if (path.getParent()==null || !Files.isDirectory(path.getParent(),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Atomic rejection parent is absent");
        var lockPath=path.resolveSibling(path.getFileName()+".lock");
        if (Files.exists(lockPath,LinkOption.NOFOLLOW_LINKS) && (!Files.isRegularFile(lockPath,LinkOption.NOFOLLOW_LINKS) || Files.size(lockPath)!=0)) throw new IOException("Atomic rejection lock must be an empty regular file");
        channel=FileChannel.open(lockPath,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS); FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Atomic operation journal already owned",owned); }
            if (acquired==null) throw new IOException("Atomic operation journal already owned"); lock=acquired;
            boolean create=requestedIdentity!=null;
            if (create==Files.exists(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException(create ? "Atomic operation journal already exists" : "Atomic rejection is absent");
            if (create) {
                identity=requestedIdentity; requestDigest=digest.clone(); requireNoSidecar();
                try { io.write(path,encode(false),MAX_BYTES); }
                catch (IOException | RuntimeException failure) { fenced=true; throw failure; }
            } else {
                if (!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Atomic rejection must be a regular file");
                byte[] body=SessionJournalFiles.read(path,MAX_BYTES); var saved=decode(body);
                if (!Arrays.equals(binding,saved.binding())) throw new IOException("Atomic rejection binding changed");
                identity=saved.identity(); requestDigest=saved.requestDigest(); consumed=saved.consumed();
                if (!Arrays.equals(encode(consumed),body)) throw new IOException("Noncanonical atomic rejection");
                SessionJournalFiles.force(path); discardOwnedSidecar(); SessionJournalFiles.force(path.getParent());
            }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure;
        }
    }
    /** Routing probe only. Accepted transaction bodies are never allocated or interpreted here. */
    public static boolean isRejection(Path path) throws IOException {
        if (!Files.isRegularFile(Objects.requireNonNull(path),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Atomic operation journal is absent or not a regular file");
        try (var file=FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)) {
            long size=file.size(); if (size<36 || size>RatisDurableTransaction.MAX_BYTES) throw new IOException("Invalid atomic journal size");
            var header=ByteBuffer.allocate(4); read(file,header); int magic=header.getInt(0);
            if (magic==TRANSACTION_MAGIC) return false;
            if (magic!=MAGIC || size!=MAX_BYTES) throw new IOException("Unknown atomic journal kind or size");
            var encoded=ByteBuffer.allocate(MAX_BYTES).putInt(magic); read(file,encoded);
            if (file.size()!=MAX_BYTES) throw new IOException("Atomic rejection changed during probe");
            byte[] bytes=encoded.array(),body=Arrays.copyOf(bytes,BODY_BYTES);
            if (!MessageDigest.isEqual(checksum(body),Arrays.copyOfRange(bytes,BODY_BYTES,MAX_BYTES))) throw new IOException("Atomic rejection checksum mismatch");
            decode(body); return true;
        }
    }
    public RatisAtomicClient.Identity identity() { return identity; }
    public byte[] requestDigest() { return requestDigest.clone(); }
    public synchronized boolean consumed() throws IOException { requireOpen(); return consumed; }
    public void consume() throws IOException { consume(SessionJournalFiles::write); }
    void consume(Persistence io) throws IOException {
        Objects.requireNonNull(io);
        synchronized (this) { requireOpen(); if (active) throw new IOException("Atomic rejection file IO active"); if (consumed) return; active=true; }
        try {
            if (!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || !Arrays.equals(encode(false),SessionJournalFiles.read(path,MAX_BYTES))) throw new IOException("Atomic rejection changed before consumption");
            discardOwnedSidecar();
            io.write(path,encode(true),MAX_BYTES); synchronized (this) { consumed=true; }
        } catch (IOException | RuntimeException failure) {
            synchronized (this) { fenced=true; }
            if (failure instanceof IOException ioFailure) throw ioFailure; throw new IOException("Atomic rejection persistence failed; reopen it",failure);
        } finally { synchronized (this) { active=false; } }
    }
    /** Only a verified published record grants ownership of its never-dispatched crash sidecar. */
    private void discardOwnedSidecar() throws IOException {
        var sidecar=path.resolveSibling(path.getFileName()+".pending"); if (!Files.exists(sidecar,LinkOption.NOFOLLOW_LINKS)) return;
        if (!Files.isRegularFile(sidecar,LinkOption.NOFOLLOW_LINKS) || Files.size(sidecar)>MAX_BYTES) throw new IOException("Invalid unpublished atomic rejection sidecar");
        Files.delete(sidecar); SessionJournalFiles.force(path.getParent());
    }
    private void requireNoSidecar() throws IOException { if (Files.exists(path.resolveSibling(path.getFileName()+".pending"),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Unpublished atomic operation data requires inspection"); }
    private void requireOpen() throws IOException { if (closed || fenced || !lock.isValid()) throw new IOException("Atomic rejection closed/fenced; reopen it"); }
    private byte[] encode(boolean consumed) {
        var output=ByteBuffer.allocate(BODY_BYTES).putInt(MAGIC).put(binding); uuid(output,identity.transaction()); operation(output,identity.business()); operation(output,identity.firstControl());
        return output.put(requestDigest).put((byte)(consumed ? 1 : 0)).array();
    }
    private static Saved decode(byte[] body) throws IOException {
        if (body.length!=BODY_BYTES) throw new IOException("Invalid atomic rejection body size");
        try {
            var input=ByteBuffer.wrap(body); if (input.getInt()!=MAGIC) throw new IOException("Wrong atomic rejection kind");
            byte[] binding=new byte[32]; input.get(binding); var transaction=uuid(input); var business=operation(input); var control=operation(input); byte[] digest=new byte[32]; input.get(digest);
            int consumed=input.get()&255; if (consumed>1) throw new IOException("Invalid atomic rejection consumption flag");
            return new Saved(binding,new RatisAtomicClient.Identity(transaction,business,control),digest,consumed==1);
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid atomic rejection identity",invalid); }
    }
    private static void requireDigest(byte[] bytes,String name) { if (Objects.requireNonNull(bytes).length!=32) throw new IllegalArgumentException("Atomic rejection "+name+" must contain 32 bytes"); }
    private static void read(FileChannel file,ByteBuffer buffer) throws IOException { while (buffer.hasRemaining()) if (file.read(buffer)<0) throw new IOException("Truncated atomic operation journal"); }
    private static byte[] checksum(byte[] body) { try { return MessageDigest.getInstance("SHA-256").digest(body); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    private static void uuid(ByteBuffer output,UUID value) { output.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer input) { return new UUID(input.getLong(),input.getLong()); }
    private static void operation(ByteBuffer output,OperationId value) { uuid(output,value.sessionId()); output.putLong(value.epoch()).putLong(value.sequence()); }
    private static OperationId operation(ByteBuffer input) { return new OperationId(uuid(input),input.getLong(),input.getLong()); }
    @Override public synchronized void close() throws IOException {
        if (active) throw new IOException("Atomic rejection file IO active; ownership retained");
        if (closed && !channel.isOpen()) return; closed=true; try { if (lock.isValid()) lock.release(); } finally { channel.close(); }
    }
}
