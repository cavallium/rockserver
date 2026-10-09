package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.export.ExportJobs;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Bounded nonce ledger. Pending identity is forced before allocation; binding precedes source RPC. */
final class RatisExportRequests {
    record Entry(ExportJobs.Create request,UUID job,byte[] schema,byte[] source,SnapshotFence fence,Path snapshot,boolean initialized) {
        Entry { schema=schema.clone(); source=source.clone(); if (initialized && fence==null || (fence==null)!=(snapshot==null)) throw new IllegalArgumentException("Partial export binding"); }
        Entry bind(SnapshotFence cut,Path path) { return new Entry(request,job,schema,source,cut,path.toAbsolutePath().normalize(),false); }
        RatisExportJournal.Definition definition(List<UUID> columns) {
            var l=request.limits();
            return new RatisExportJournal.Definition(job,fence,columns,snapshot,source,new RatisExportJournal.Limits(l.maximumChunks(),l.maximumJournalBytes(),l.maximumDiskBytes(),new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport.Limits(l.maximumRows(),l.maximumPayloadBytes(),l.maximumFileBytes(),l.maximumChunkDuration())),l.snapshotTtl());
        }
    }
    private final Path file; private final int count,bytes;
    private final Map<UUID,Entry> entries=new LinkedHashMap<>();
    private boolean fenced;
    RatisExportRequests(Path root,int count,int bytes) throws IOException {
        file=root.resolve("requests"); this.count=count; this.bytes=bytes;
        Path sibling=root.resolve("requests.pending");
        if (Files.exists(file,LinkOption.NOFOLLOW_LINKS)) {
            read(); // Only the forced authoritative catalog grants allocation or source-dispatch authority.
            if (Files.exists(sibling,LinkOption.NOFOLLOW_LINKS)) {
                if (!Files.isRegularFile(sibling,LinkOption.NOFOLLOW_LINKS) || Files.size(sibling)>bytes) throw new IOException("Invalid unpublished export catalog sibling");
                Files.delete(sibling); SessionJournalFiles.force(root);
            }
        } else {
            if (Files.exists(sibling,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Original export catalog absent; unpublished sibling grants no recovery authority");
            save();
        }
    }
    Collection<Entry> entries() { return List.copyOf(entries.values()); }
    Entry nonce(UUID nonce) throws IOException { check(); return entries.get(nonce); }
    Entry job(UUID job) throws IOException { check(); return entries.values().stream().filter(e -> e.job.equals(job)).findFirst().orElseThrow(() -> new IOException("Unknown export job")); }
    Entry pending(ExportJobs.Create request,byte[] schema,byte[] source) throws IOException {
        check(); var existing=entries.get(request.requestId());
        if (existing!=null) { if (!existing.request.equals(request)) throw new IllegalArgumentException("Export nonce payload changed"); return existing; }
        if (entries.size()>=count) throw new IOException("Export durable nonce capacity exhausted; authority-qualified reclamation required");
        var created=new Entry(request,UUID.randomUUID(),schema,source,null,null,false); entries.put(request.requestId(),created); save(); return created;
    }
    Entry bind(Entry pending,SnapshotFence fence,Path source) throws IOException {
        check(); if (pending.fence!=null || entries.get(pending.request.requestId())!=pending) throw new IOException("Export binding changed");
        var bound=pending.bind(fence,source); entries.put(pending.request.requestId(),bound); save(); return bound;
    }
    Entry initialized(Entry entry) throws IOException {
        check(); var current=entries.get(entry.request.requestId());
        if (current.initialized) return current;
        var marked=new Entry(current.request,current.job,current.schema,current.source,current.fence,current.snapshot,true);
        entries.put(current.request.requestId(),marked); save(); return marked;
    }
    private void check() throws IOException { if (fenced) throw new IOException("Export request catalog fenced after mutation failure; reopen original root"); }
    private void save() throws IOException {
        try {
            var buffer=new ByteArrayOutputStream(); var out=new DataOutputStream(buffer); out.writeInt(entries.size());
            for (var entry:entries.values()) {
                uuid(out,entry.request.requestId()); uuid(out,entry.job); out.write(entry.schema); out.write(entry.source);
                out.writeInt(entry.request.columns().size()); for (long column:entry.request.columns()) out.writeLong(column);
                var l=entry.request.limits(); out.writeInt(l.maximumChunks()); out.writeLong(l.maximumJournalBytes()); out.writeLong(l.maximumDiskBytes()); out.writeInt(l.maximumRows()); out.writeLong(l.maximumPayloadBytes()); out.writeLong(l.maximumFileBytes()); out.writeLong(l.maximumChunkDuration().toNanos()); out.writeLong(l.snapshotTtl().toNanos());
                out.writeBoolean(entry.initialized); out.writeBoolean(entry.fence!=null); if (entry.fence!=null) { var cut=entry.fence.encode(); out.writeInt(cut.length); out.write(cut); out.writeUTF(entry.snapshot.toString()); }
                if (buffer.size()+32>bytes) throw new IOException("Export request byte capacity exhausted");
            }
            SessionJournalFiles.write(file,buffer.toByteArray(),bytes);
        } catch (IOException | RuntimeException failure) { fenced=true; throw failure; }
    }
    private void read() throws IOException {
        if (!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid export request catalog");
        try (var in=new DataInputStream(new ByteArrayInputStream(SessionJournalFiles.read(file,bytes)))) {
            int size=in.readInt(); if (size<0 || size>count) throw new IOException("Invalid export request count");
            var jobs=new HashSet<UUID>();
            for (int i=0;i<size;i++) {
                UUID nonce=uuid(in),job=uuid(in); byte[] schema=in.readNBytes(32),source=in.readNBytes(32); if (schema.length!=32 || source.length!=32) throw new EOFException();
                int n=in.readInt(); if (n<1 || n>ExportJobs.MAX_COLUMNS) throw new IOException("Invalid selected columns"); var columns=new ArrayList<Long>(n); for (int c=0;c<n;c++) columns.add(in.readLong());
                var limits=new ExportJobs.Limits(in.readInt(),in.readLong(),in.readLong(),in.readInt(),in.readLong(),in.readLong(),Duration.ofNanos(in.readLong()),Duration.ofNanos(in.readLong()));
                boolean initialized=in.readBoolean(); SnapshotFence fence=null; Path path=null; if (in.readBoolean()) { int length=in.readInt(); if (length<1 || length>SnapshotFence.MAX_BYTES) throw new IOException("Invalid saved export cut"); byte[] cut=in.readNBytes(length); if (cut.length!=length) throw new EOFException(); fence=SnapshotFence.decode(cut); path=Path.of(in.readUTF()); if (!path.isAbsolute() || !path.equals(path.normalize())) throw new IOException("Noncanonical source path"); }
                var entry=new Entry(new ExportJobs.Create(nonce,columns,limits),job,schema,source,fence,path,initialized);
                if (entries.putIfAbsent(nonce,entry)!=null || !jobs.add(job)) throw new IOException("Repeated export identity");
            }
            if (in.read()!=-1) throw new IOException("Trailing export request bytes");
        } catch (RuntimeException invalid) { throw new IOException("Invalid export request catalog",invalid); }
    }
    private static void uuid(DataOutputStream out,UUID id) throws IOException { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(),in.readLong()); }
}
