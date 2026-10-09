package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.OperatorUpload;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import static java.nio.file.StandardOpenOption.*;
import static it.cavallium.rockserver.distributed.ratis.RatisSchemaReplicaRpc.*;

/** Forced, bounded upload progress. The service serializes each owner's actual I/O. */
final class RatisSchemaArtifactRegistry implements AutoCloseable {
    record Progress(BitSet chunks,boolean ready) {}
    private record Entry(Request begin,Path directory,long charge) {}
    @FunctionalInterface interface Force { void force(Path path) throws IOException; }
    private final Force forcing;
    private final Path root;
    private final RatisSchemaReplicaService.Limits limits;
    private final FileChannel lockChannel;
    private final java.nio.channels.FileLock lock;
    private final Map<String,Entry> entries=new java.util.concurrent.ConcurrentHashMap<>();
    private long diskBytes;
    private final Map<String,Long> orphans=new HashMap<>();
    RatisSchemaArtifactRegistry(Path root,RatisSchemaReplicaService.Limits limits) throws IOException { this(root,limits,RatisSchemaArtifactRegistry::diskForce); }
    RatisSchemaArtifactRegistry(Path root,RatisSchemaReplicaService.Limits limits,Force forcing) throws IOException {
        this.forcing=forcing; this.root=root.toAbsolutePath().normalize(); this.limits=limits;
        Files.createDirectories(this.root);
        if (Files.isSymbolicLink(this.root) || !Files.isDirectory(this.root,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid schema staging directory");
        lockChannel=FileChannel.open(this.root.resolve("lock"),CREATE,WRITE);
        java.nio.channels.FileLock acquired=null;
        try {
            try { acquired=lockChannel.tryLock(); } catch (java.nio.channels.OverlappingFileLockException owned) { throw new IOException("Schema staging already has an owner",owned); }
            if (acquired==null) throw new IOException("Schema staging already has an owner");
            try (var children=Files.newDirectoryStream(this.root)) {
                int count=0;
                for (var directory:children) {
                    if (directory.getFileName().toString().equals("lock")) continue;
                    if (++count>limits.maximumUploads() || Files.isSymbolicLink(directory) || !Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Schema staging inventory exceeds bounds");
                    if (!Files.exists(directory.resolve("manifest"),LinkOption.NOFOLLOW_LINKS)) {
                        String name=directory.getFileName().toString();
                        if (!name.matches("[0-9a-f-]{147}")) throw new IOException("Invalid interrupted schema upload owner");
                        long charge=0; int files=0;
                        try (var abandoned=Files.newDirectoryStream(directory)) {
                            for (var file:abandoned) {
                                if (++files>1 || !(file.getFileName().toString().equals("manifest.tmp") || file.getFileName().toString().equals("released")) || Files.isSymbolicLink(file)
                                        || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS) || Files.size(file)>MAX_BYTES) throw new IOException("Invalid interrupted schema manifest");
                                charge+=Files.size(file);
                            }
                        }
                        if (files==0) { Files.delete(directory); force(this.root); count--; continue; }
                        diskBytes+=charge; if (diskBytes>limits.maximumDiskBytes()) throw new IOException("Interrupted schema staging exceeds budget");
                        orphans.put(name,charge); continue;
                    }
                    var begin=RatisSchemaReplicaRpc.request(read(directory.resolve("manifest"),RatisSchemaReplicaRpc.MAX_BYTES));
                    if (begin.kind()!=ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN || !directory.getFileName().toString().equals(key(begin))) throw new IOException("Schema staging owner changed");
                    long charge=charge(begin); diskBytes=Math.addExact(diskBytes,charge);
                    if (diskBytes>limits.maximumDiskBytes()) throw new IOException("Schema staging disk budget exceeded");
                    var entry=new Entry(begin,directory,charge); validateFiles(entry); entries.put(key(begin),entry);
                }
            }
            lock=acquired;
        } catch (Throwable failure) {
            if (acquired!=null) acquired.close(); lockChannel.close();
            throw failure;
        }
    }
    static String key(Request request) {
        var replica=request.target().replica();
        return request.upload().uploadId()+"-"+request.target().binding().groupId()+"-"+replica.replicaId()+"-"+replica.nodeIncarnation();
    }
    private static long charge(Request request) { return request.upload().artifactLength()+2L*request.encode().length+2L*OperatorUpload.CHUNK_BYTES+4096; }
    private Entry entry(Request request) throws IOException {
        var entry=entries.get(key(request));
        if (entry!=null && (!entry.begin().target().equals(request.target()) || !Arrays.equals(entry.begin().upload().encode(),request.upload().encode()))) throw new IOException("Original operator upload owner changed");
        return entry;
    }
    synchronized Progress begin(Request request) throws IOException {
        var entry=entry(request);
        if (entry==null) {
            long charge=charge(request);
            if (entries.size()+orphans.size()-(orphans.containsKey(key(request)) ? 1 : 0)>=limits.maximumUploads() || charge>limits.maximumDiskBytes()-diskBytes+(orphans.getOrDefault(key(request),0L))) throw new IOException("Schema staging capacity unavailable");
            long free=Files.getFileStore(root).getUsableSpace();
            if (free<limits.minimumFreeBytes() || charge>free-limits.minimumFreeBytes()) throw new IOException("Schema staging disk headroom unavailable");
            Path directory=root.resolve(key(request));
            if (Files.exists(directory.resolve("released"),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Operator upload staging was released");
            if (orphans.containsKey(key(request))) diskBytes-=orphans.remove(key(request)); else Files.createDirectory(directory);
            force(root);
            entry=new Entry(request,directory,charge); entries.put(key(request),entry); diskBytes+=charge;
            immutable(directory.resolve("manifest"),request.encode());
        } else immutable(entry.directory().resolve("manifest"),entry.begin().encode());
        return progress(request);
    }
    Progress chunk(Request request) throws IOException {
        var entry=entry(request); if (entry==null) throw new IOException("Operator upload has not begun");
        if (!Arrays.equals(read(entry.directory().resolve("manifest"),MAX_BYTES),entry.begin().encode())) throw new IOException("Operator upload manifest was not forced");
        byte[] bytes=request.chunk(); request.upload().requireChunk(request.chunkIndex(),bytes);
        immutable(entry.directory().resolve("chunk-"+request.chunkIndex()),bytes); return progress(request);
    }
    byte[] artifact(Request request) throws IOException {
        var entry=entry(request); if (entry==null) throw new IOException("Operator upload has not begun");
        byte[] result=new byte[request.upload().artifactLength()]; int offset=0;
        var digest=sha();
        for (int i=0;i<request.upload().chunks().size();i++) {
            byte[] chunk=read(entry.directory().resolve("chunk-"+i),OperatorUpload.CHUNK_BYTES); request.upload().requireChunk(i,chunk);
            digest.update(chunk); System.arraycopy(chunk,0,result,offset,chunk.length); offset+=chunk.length;
        }
        if (!Arrays.equals(digest.digest(),request.upload().operator().artifactSha256())) throw new IOException("Complete operator artifact SHA changed"); return result;
    }
    Progress ready(Request request) throws IOException {
        var entry=entry(request); if (entry==null) throw new IOException("Operator upload has not begun");
        immutable(entry.directory().resolve("ready"),entry.begin().upload().digest()); return progress(request);
    }
    Progress progress(Request request) throws IOException {
        var entry=entry(request); var bits=new BitSet(); if (entry==null || Files.exists(entry.directory().resolve("released"),LinkOption.NOFOLLOW_LINKS)) return new Progress(bits,false);
        for (int i=0;i<request.upload().chunks().size();i++) {
            Path chunk=entry.directory().resolve("chunk-"+i);
            if (Files.exists(chunk,LinkOption.NOFOLLOW_LINKS)) { request.upload().requireChunk(i,read(chunk,OperatorUpload.CHUNK_BYTES)); bits.set(i); }
        }
        boolean ready=Files.exists(entry.directory().resolve("ready"),LinkOption.NOFOLLOW_LINKS);
        if (ready && (bits.cardinality()!=request.upload().chunks().size() || !Arrays.equals(read(entry.directory().resolve("ready"),32),request.upload().digest()))) throw new IOException("Forced operator readiness is inconsistent");
        if (!Arrays.equals(read(entry.directory().resolve("manifest"),MAX_BYTES),entry.begin().encode())) throw new IOException("Original upload manifest changed");
        force(entry.directory().resolve("manifest"));
        for (int i=bits.nextSetBit(0);i>=0;i=bits.nextSetBit(i+1)) force(entry.directory().resolve("chunk-"+i));
        if (ready) force(entry.directory().resolve("ready"));
        force(entry.directory()); force(root);
        return new Progress(bits,ready);
    }
    synchronized void release(Target target) throws IOException {
        byte[] identity=OperatorUpload.sha256(target.encode());
        for (var entry:List.copyOf(entries.values())) if (entry.begin().target().equals(target)) {
            immutable(entry.directory().resolve("released"),identity);
            try (var files=Files.newDirectoryStream(entry.directory())) {
                for (var file:files) if (!file.getFileName().toString().equals("manifest") && !file.getFileName().toString().equals("released")) Files.delete(file);
            }
            force(entry.directory()); Files.deleteIfExists(entry.directory().resolve("manifest")); force(entry.directory());
            Files.delete(entry.directory().resolve("released")); force(entry.directory()); Files.delete(entry.directory()); force(root);
            entries.remove(key(entry.begin())); diskBytes-=entry.charge();
        }
        String suffix="-"+target.binding().groupId()+"-"+target.replica().replicaId()+"-"+target.replica().nodeIncarnation();
        for (var name:List.copyOf(orphans.keySet())) if (name.endsWith(suffix)) {
            Path directory=root.resolve(name),released=directory.resolve("released");
            if (Files.exists(released,LinkOption.NOFOLLOW_LINKS) && Arrays.equals(read(released,32),identity)) {
                Files.delete(released); force(directory); Files.delete(directory); force(root); diskBytes-=orphans.remove(name);
            }
        }
    }
    private static void validateFiles(Entry entry) throws IOException {
        int count=0; long bytes=0;
        try (var files=Files.newDirectoryStream(entry.directory())) {
            for (var file:files) {
                String name=file.getFileName().toString();
                if (++count>20 || Files.isSymbolicLink(file) || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)
                        || !name.matches("manifest|ready|released|chunk-[0-7]|(?:manifest|ready|released|chunk-[0-7])\\.tmp")) throw new IOException("Invalid schema staging file");
                bytes=Math.addExact(bytes,Files.size(file)); if (bytes>entry.charge()) throw new IOException("Schema staging files exceed reservation");
            }
        }
    }
    private static byte[] read(Path file,int maximum) throws IOException {
        if (Files.isSymbolicLink(file) || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS) || Files.size(file)>maximum) throw new IOException("Invalid bounded schema staging file");
        byte[] result=Files.readAllBytes(file); if (result.length>maximum) throw new IOException("Schema staging file grew beyond bound"); return result;
    }
    private void immutable(Path path,byte[] bytes) throws IOException {
        if (Files.exists(path,LinkOption.NOFOLLOW_LINKS)) {
            if (!Arrays.equals(read(path,bytes.length),bytes)) throw new IOException("Original forced upload bytes changed"); force(path); force(path.getParent()); return;
        }
        Path temporary=path.resolveSibling(path.getFileName()+".tmp");
        try (var channel=FileChannel.open(temporary,CREATE,TRUNCATE_EXISTING,WRITE,LinkOption.NOFOLLOW_LINKS)) {
            var buffer=java.nio.ByteBuffer.wrap(bytes); while (buffer.hasRemaining()) channel.write(buffer); channel.force(true);
        }
        Files.move(temporary,path,StandardCopyOption.ATOMIC_MOVE); force(path.getParent());
    }
    private static java.security.MessageDigest sha() {
        try { return java.security.MessageDigest.getInstance("SHA-256"); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private void force(Path path) throws IOException { forcing.force(path); }
    private static void diskForce(Path path) throws IOException { try (var channel=FileChannel.open(path,READ)) { channel.force(true); } }
    @Override public void close() throws IOException { try { if (lock.isValid()) lock.close(); } finally { if (lockChannel.isOpen()) lockChannel.close(); } }
}
