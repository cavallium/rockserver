package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/** Forced cleanup intents: one unresolved frame per group, independently of business cardinality. */
public final class RatisRetirementJournal implements AutoCloseable {
    public static final int MAX_PENDING=16, MAX_BYTES=32*1024*1024;
    private static final int BINDING_BYTES=32, CHECKSUM_BYTES=32, MAX_FILE_BYTES=BINDING_BYTES+RatisRetirementOperation.MAX_BYTES+CHECKSUM_BYTES;
    private static final int MAX_SCAN_FILES=2*MAX_PENDING+1;
    private static final long MAX_SIDECAR_BYTES=MAX_BYTES+(long)MAX_PENDING*CHECKSUM_BYTES;
    private static final String LOCK="retirement.lock";
    @FunctionalInterface public interface Action { void run() throws IOException; }
    public record Limits(int maxPending,long maxBytes) {
        public Limits { if (maxPending<1 || maxPending>MAX_PENDING || maxBytes<1 || maxBytes>MAX_BYTES) throw new IllegalArgumentException("Invalid retirement journal limits"); }
        public static Limits defaults() { return new Limits(MAX_PENDING,MAX_BYTES); }
    }
    /** Bytes count binding plus canonical native frame, excluding the checksum. */
    public record Stats(int count,long bytes) {}
    private final Path directory;
    private final byte[] binding;
    private final Limits limits;
    private final FileChannel channel;
    private final FileLock lock;
    private final Map<UUID,RatisRetirementOperation> operations=new LinkedHashMap<>();
    private long chargedBytes;
    private boolean active,rpcActive,closed,fenced;

    public static RatisRetirementJournal open(Path directory,byte[] binding32,Limits limits) throws IOException {
        return new RatisRetirementJournal(directory,binding32,limits);
    }
    private RatisRetirementJournal(Path directory,byte[] binding32,Limits limits) throws IOException {
        this.directory=Objects.requireNonNull(directory).toAbsolutePath().normalize(); this.limits=Objects.requireNonNull(limits);
        if (Objects.requireNonNull(binding32).length!=BINDING_BYTES) throw new IllegalArgumentException("Retirement binding must contain 32 bytes"); binding=binding32.clone();
        var parent=this.directory.getParent();
        if (parent==null || !Files.isDirectory(parent,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Retirement journal parent must already exist");
        if (!Files.exists(this.directory,LinkOption.NOFOLLOW_LINKS)) {
            try { Files.createDirectory(this.directory); SessionJournalFiles.force(parent); }
            catch (FileAlreadyExistsException raced) { /* Verify the resulting directory below. */ }
        }
        if (!Files.isDirectory(this.directory,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Retirement journal requires a dedicated regular directory");
        var lockPath=this.directory.resolve(LOCK); if (Files.exists(lockPath,LinkOption.NOFOLLOW_LINKS)) requireLock(lockPath);
        channel=FileChannel.open(lockPath,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS); FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Retirement journal already owned",owned); }
            if (acquired==null) throw new IOException("Retirement journal already owned"); lock=acquired;
            channel.force(true); scan(); SessionJournalFiles.force(this.directory);
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure;
        }
    }
    private void scan() throws IOException {
        int visited=0; long sidecarBytes=0; var sidecars=new ArrayList<Path>();
        try (var paths=Files.newDirectoryStream(directory)) {
            for (var path:paths) {
                if (++visited>MAX_SCAN_FILES) throw new IOException("Retirement journal directory exceeds file bound");
                var name=path.getFileName().toString(); if (name.equals(LOCK)) { requireLock(path); continue; }
                if (name.endsWith(".bin.pending")) {
                    parse(name.substring(0,name.length()-8));
                    if (!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Unpublished retirement sidecar must be a regular file");
                    long size=Files.size(path);
                    if (size>MAX_FILE_BYTES || sidecars.size()>=MAX_PENDING || size>MAX_SIDECAR_BYTES-sidecarBytes) throw new IOException("Unpublished retirement sidecars exceed bound");
                    sidecars.add(path); sidecarBytes+=size; continue;
                }
                var group=parse(name); var operation=read(path,group); long bytes=payloadBytes(operation);
                if (operations.size()>=limits.maxPending() || bytes>limits.maxBytes()-chargedBytes) throw new IOException("Retirement journal exceeds pending intent quota");
                if (operations.putIfAbsent(group,operation)!=null) throw new IOException("Duplicate retirement group"); chargedBytes+=bytes;
                SessionJournalFiles.force(path);
            }
        }
        // No callback could have seen these files: atomic publication and directory force had not returned.
        // Validate the whole directory first, including every published frame, before discarding sidecars.
        for (var sidecar:sidecars) Files.delete(sidecar);
        var ordered=new ArrayList<>(operations.values()); ordered.sort(Comparator.comparing(RatisRetirementOperation::targetGroup)); operations.clear();
        for (var operation:ordered) operations.put(operation.targetGroup(),operation);
    }
    /** True only after the exact frame and its directory entry have been forced. */
    public boolean reserve(RatisRetirementOperation operation) throws IOException {
        Objects.requireNonNull(operation); final Path path; final long bytes=payloadBytes(operation);
        synchronized (this) {
            writable(); var existing=operations.get(operation.targetGroup()); if (existing!=null) return existing.equals(operation);
            if (operations.size()>=limits.maxPending() || bytes>limits.maxBytes()-chargedBytes) return false;
            path=path(operation.targetGroup()); operations.put(operation.targetGroup(),operation); chargedBytes+=bytes; active=true;
        }
        try {
            if (Files.exists(path,LinkOption.NOFOLLOW_LINKS) || Files.exists(path.resolveSibling(path.getFileName()+".pending"),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Unowned retirement file already exists");
            var record=operation.bytes(); var body=ByteBuffer.allocate(BINDING_BYTES+record.length).put(binding).put(record).array();
            SessionJournalFiles.write(path,body,MAX_FILE_BYTES); return true;
        } catch (IOException | RuntimeException failure) { throw fence(failure); }
        finally { synchronized (this) { active=false; } }
    }
    /** Fenced contents remain inspectable; reopen must settle them before any replay RPC. */
    public synchronized List<RatisRetirementOperation> pending() throws IOException { requireOpen(); if (active) throw new IOException("Retirement file mutation active"); return List.copyOf(operations.values()); }
    public synchronized boolean fenced() { return fenced; }
    /** Own the exact durable frame until the synchronous RPC callback actually returns. */
    public void invoke(RatisRetirementOperation operation,Action action) throws IOException {
        Objects.requireNonNull(operation); Objects.requireNonNull(action); final Path path;
        synchronized (this) {
            writable(); if (!operation.equals(operations.get(operation.targetGroup()))) throw new IOException("RPC requires the exact pending retirement intent");
            path=path(operation.targetGroup()); rpcActive=true;
        }
        try {
            try { if (!operation.equals(read(path,operation.targetGroup()))) throw new IOException("Retirement file changed before RPC"); }
            catch (IOException | RuntimeException failure) { throw fence(failure); }
            action.run();
        } finally { synchronized (this) { rpcActive=false; } }
    }
    /** Delete only the exact owned frame after its caller has established semantic success. */
    public boolean remove(RatisRetirementOperation operation) throws IOException {
        Objects.requireNonNull(operation); final Path path;
        synchronized (this) {
            writable(); if (!operation.equals(operations.get(operation.targetGroup()))) return false;
            path=path(operation.targetGroup()); active=true;
        }
        try {
            if (!operation.equals(read(path,operation.targetGroup()))) throw new IOException("Retirement file changed before removal");
            Files.delete(path); SessionJournalFiles.force(directory);
            synchronized (this) { operations.remove(operation.targetGroup()); chargedBytes-=payloadBytes(operation); } return true;
        } catch (IOException | RuntimeException failure) { throw fence(failure); }
        finally { synchronized (this) { active=false; } }
    }
    public synchronized Stats stats() throws IOException { requireOpen(); return new Stats(operations.size(),chargedBytes); }
    private RatisRetirementOperation read(Path path,UUID group) throws IOException {
        if (!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Retirement intent must be a regular file");
        byte[] body=SessionJournalFiles.read(path,MAX_FILE_BYTES); if (body.length<=BINDING_BYTES) throw new IOException("Truncated retirement intent");
        if (!Arrays.equals(binding,Arrays.copyOf(body,BINDING_BYTES))) throw new IOException("Retirement journal binding changed");
        final RatisRetirementOperation operation;
        try { operation=RatisRetirementOperation.decode(Arrays.copyOfRange(body,BINDING_BYTES,body.length)); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid canonical retirement intent",invalid); }
        if (!group.equals(operation.targetGroup())) throw new IOException("Retirement filename differs from target group"); return operation;
    }
    private static UUID parse(String name) throws IOException {
        if (!name.endsWith(".bin")) throw new IOException("Unknown retirement journal file");
        try { var group=UUID.fromString(name.substring(0,name.length()-4)); if (!name.equals(group+".bin")) throw new IllegalArgumentException("Noncanonical UUID"); return group; }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid retirement group filename",invalid); }
    }
    private Path path(UUID group) { return directory.resolve(group+".bin"); }
    private static long payloadBytes(RatisRetirementOperation operation) { return BINDING_BYTES+(long)operation.encodedSize(); }
    private static void requireLock(Path path) throws IOException { if (!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)!=0) throw new IOException("Retirement lock must be an empty regular file"); }
    private void requireOpen() throws IOException { if (closed || !lock.isValid()) throw new IOException("Retirement journal is closed"); }
    private void writable() throws IOException { requireOpen(); if (fenced) throw new IOException("Retirement file mutation failed; close and reopen journal"); if (active || rpcActive) throw new IOException("Retirement file mutation or RPC active"); }
    private synchronized IOException fence(Exception failure) { fenced=true; return failure instanceof IOException io ? io : new IOException("Retirement file mutation failed",failure); }
    @Override public synchronized void close() throws IOException {
        if (active || rpcActive) throw new IOException("Retirement file mutation or RPC active; ownership retained");
        if (closed && !channel.isOpen()) return; closed=true; try { if (lock.isValid()) lock.release(); } finally { channel.close(); }
    }
}
