package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.time.Duration;
import java.util.*;

/** Append-only export control; immutable artifacts remain independently usable after source loss. */
public final class RatisExportJournal implements AutoCloseable {
    private static final int MAGIC=0x5245584a,MAX_RECORD=4*1024*1024+4096,FRAME=36;
    public enum Phase { CAPTURING, EXPORTING, MATERIALIZED, READY, SNAPSHOT_LOST, CANCELLED, RELEASED }
    public record Limits(int maximumChunks,long maximumJournalBytes,long maximumDiskBytes,NativeRangeSstExport.Limits chunk) {
        public Limits { Objects.requireNonNull(chunk); if (maximumChunks<1 || maximumChunks>65536 || maximumJournalBytes<2L*MAX_RECORD+1024 || maximumJournalBytes>64L*1024*1024 || maximumDiskBytes<chunk.maximumFileBytes()) throw new IllegalArgumentException("Invalid export budgets"); }
        public static Limits defaults() { return new Limits(4096,64L*1024*1024,1024L*1024*1024,new NativeRangeSstExport.Limits(65536,16L*1024*1024,32L*1024*1024,Duration.ofSeconds(30))); }
    }
    public record Definition(UUID jobId,SnapshotFence fence,UUID column,Path sourceJournal,byte[] sourceIdentity,Limits limits) {
        public Definition {
            Objects.requireNonNull(jobId); Objects.requireNonNull(fence); Objects.requireNonNull(column); Objects.requireNonNull(sourceJournal); Objects.requireNonNull(sourceIdentity); Objects.requireNonNull(limits);
            sourceJournal=sourceJournal.toAbsolutePath().normalize(); if (sourceIdentity.length!=32 || sourceJournal.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length>4096) throw new IllegalArgumentException("Invalid export source binding"); sourceIdentity=sourceIdentity.clone();
            long future=2L*(MAX_RECORD+FRAME)+fence.encode().length+sourceJournal.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length+8192;
            if (future>limits.maximumJournalBytes()) throw new IllegalArgumentException("Export lacks reserved first-chunk/capture/READY capacity");
            var projections=new ArrayList<TransactionScope.Span>(); for (var participant:fence.participants()) projections.add(projection(participant,column));
            projections.sort(Comparator.comparing(TransactionScope.Span::lowerInclusive,RatisExportJournal::lowerOrder));
            for (int n=1;n<projections.size();n++) if (projections.get(n-1).upperExclusive()==null || projections.get(n).lowerInclusive()==null || !Arrays.equals(projections.get(n-1).upperExclusive(),projections.get(n).lowerInclusive())) throw new IllegalArgumentException("Export projection has overlapping/gapped participant coverage");
        }
        @Override public byte[] sourceIdentity() { return sourceIdentity.clone(); }
    }
    public record ChunkIntent(UUID group,UUID column,ApplyPosition cut,byte[] scopeStart,long ordinal,NativeRangeSstExport.Limits limits) {
        public ChunkIntent { Objects.requireNonNull(group); Objects.requireNonNull(column); Objects.requireNonNull(cut); Objects.requireNonNull(limits); if (ordinal<0 || cut.term()<1 || scopeStart!=null && scopeStart.length>TransactionScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("Invalid export chunk intent"); scopeStart=copy(scopeStart); }
        @Override public byte[] scopeStart() { return copy(scopeStart); }
    }
    public record Chunk(UUID jobId,UUID group,long rangeEpoch,ApplyPosition cut,long ordinal,UUID column,byte[] lowerInclusive,byte[] upperExclusive,byte[] sha256,long rows,long payloadBytes,long fileBytes,Path file,boolean hasMore) {
        public Chunk {
            Objects.requireNonNull(jobId); Objects.requireNonNull(group); Objects.requireNonNull(cut); Objects.requireNonNull(column); Objects.requireNonNull(sha256);
            if (lowerInclusive!=null && lowerInclusive.length>TransactionScope.MAX_BOUND_BYTES || upperExclusive!=null && upperExclusive.length>TransactionScope.MAX_BOUND_BYTES || rangeEpoch<0 || ordinal<0 || cut.term()<1 || rows<0 || payloadBytes<0 || fileBytes<0 || sha256.length!=32 || rows==0 && (file!=null || payloadBytes!=0 || fileBytes!=0 || hasMore) || rows>0 && (file==null || !file.isAbsolute() || fileBytes<1) || lowerInclusive!=null && upperExclusive!=null && Arrays.compareUnsigned(lowerInclusive,upperExclusive)>=0) throw new IllegalArgumentException("Invalid published export chunk");
            lowerInclusive=copy(lowerInclusive); upperExclusive=copy(upperExclusive); sha256=sha256.clone();
        }
        @Override public byte[] lowerInclusive() { return copy(lowerInclusive); } @Override public byte[] upperExclusive() { return copy(upperExclusive); } @Override public byte[] sha256() { return sha256.clone(); }
    }
    public record Snapshot(Definition definition,Phase phase,Map<UUID,SnapshotFenceStatus.Capture> captures,ChunkIntent pending,List<Chunk> chunks,Set<String> acknowledged,long pendingArtifactBytes,boolean reclaiming,boolean artifactsReclaimed) { public Snapshot { captures=Map.copyOf(captures); chunks=List.copyOf(chunks); acknowledged=Set.copyOf(acknowledged); } }
    @FunctionalInterface public interface ChunkWriter { NativeRangeSstExport.Chunk write(Path artifact,ChunkIntent intent) throws Exception; }
    private static final class Progress { byte[] next; long ordinal; boolean complete; Progress(byte[] lower) { next=copy(lower); } }
    private final Path path; private final FileChannel ownership,log; private final FileLock lock;
    private Definition definition; private final Map<UUID,SnapshotFenceStatus.Capture> captures=new LinkedHashMap<>(); private final Map<UUID,ApplyPosition> sourceReleases=new LinkedHashMap<>(); private byte[] sourceDecision; private final Map<UUID,Progress> progress=new LinkedHashMap<>(); private final List<Chunk> chunks=new ArrayList<>(); private final Set<String> acknowledged=new HashSet<>();
    private ChunkIntent pending; private byte[] chain=new byte[32]; private long disk,orphanBytes; private Phase phase=Phase.CAPTURING; private boolean active,closed,fenced,reclaiming,artifactsReclaimed;
    public static RatisExportJournal create(Path path,Definition definition) throws IOException { return new RatisExportJournal(path,Objects.requireNonNull(definition)); }
    public static RatisExportJournal reopen(Path path) throws IOException { return new RatisExportJournal(path,null); }
    private RatisExportJournal(Path path,Definition initial) throws IOException {
        this.path=path.toAbsolutePath().normalize(); if (!Files.isDirectory(this.path.getParent())) throw new IOException("Export journal parent absent");
        ownership=FileChannel.open(this.path.resolveSibling(this.path.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE); FileLock acquired=null; FileChannel opened=null;
        try {
            try { acquired=ownership.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Export journal already owned",owned); } if (acquired==null) throw new IOException("Export journal already owned"); lock=acquired;
            opened=FileChannel.open(this.path,initial==null ? new StandardOpenOption[]{StandardOpenOption.READ,StandardOpenOption.WRITE} : new StandardOpenOption[]{StandardOpenOption.CREATE_NEW,StandardOpenOption.READ,StandardOpenOption.WRITE}); log=opened;
            if (initial!=null) { define(initial); append(1,definitionBytes(initial)); SessionJournalFiles.force(this.path.getParent()); }
            else { replay(); if (!reclaiming && !artifactsReclaimed) for (var chunk:chunks) verifyArtifact(chunk); log.force(true); }
        } catch (IOException | RuntimeException | Error failure) { if (opened!=null) try { opened.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } if (acquired!=null) try { acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } try { ownership.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    private void define(Definition value) { definition=value; for (var participant:value.fence().participants()) progress.put(participant.groupId(),new Progress(scope(participant.groupId()).lowerInclusive())); }
    public synchronized Snapshot snapshot() throws IOException { open(); if (active) throw new IOException("Export journal work active"); return new Snapshot(definition,phase,captures,pending,chunks,acknowledged,orphanBytes,reclaiming,artifactsReclaimed); }
    /** Freeze the original complete read-only cut and released native gates before any chunk build. */
    public void bindSource(RatisDurableSnapshot.Snapshot source) throws IOException {
        Objects.requireNonNull(source); admit();
        try {
            if (!definition.fence().equals(source.initial().fence()) || source.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || source.observedHome()==null || source.observedHome().phase()!=SnapshotFenceStatus.Phase.COMPLETE) throw new IllegalArgumentException("Export source lacks matching read-only COMPLETE snapshot");
            var vector=new LinkedHashMap<UUID,SnapshotFenceStatus.Capture>(); var releases=new LinkedHashMap<UUID,ApplyPosition>();
            for (var group:progress.keySet()) {
                var participant=source.participant(group); var token=participant.captureToken();
                if (participant.capture()==null || participant.capture().request().mode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || token==null || !participant.capture().request().owner().equals(token.ownerIncarnation()) || !participant.capture().request().snapshotId().equals(token.snapshotId()) || !token.equals(source.observedHome().progress(group).capture())) throw new IllegalArgumentException("Export source lacks original capture vector");
                if (captures.isEmpty() && (source.releaseRequested() || participant.captureReleased() || participant.captureReleaseLost())) throw new IllegalArgumentException("Original source pin already released before export binding");
                vector.put(group,token); releases.put(group,sourceRelease(source,group,token));
            }
            byte[] decision=source.observedHome().terminalDecision().digest();
            if (!captures.isEmpty()) {
                if (!captures.equals(vector) || !Arrays.equals(sourceDecision,decision)) throw new IllegalArgumentException("Original export source cut changed");
                for (var group:progress.keySet()) if (!atLeast(releases.get(group),sourceReleases.get(group))) throw new IllegalArgumentException("Original released gate proof regressed");
                return;
            }
            if (phase!=Phase.CAPTURING) throw new IOException("Export source capture is unavailable");
            var out=output(); out.write(decision); out.writeInt(vector.size());
            for (var group:progress.keySet()) { uuid(out,group); var token=vector.get(group); uuid(out,token.ownerIncarnation()); uuid(out,token.snapshotId()); position(out,token.position()); position(out,releases.get(group)); }
            append(2,bytes(out)); sourceDecision=decision; captures.putAll(vector); sourceReleases.putAll(releases); phase=Phase.EXPORTING;
        } finally { finish(); }
    }
    private ApplyPosition sourceRelease(RatisDurableSnapshot.Snapshot source,UUID group,SnapshotFenceStatus.Capture token) {
        var participant=source.participant(group); ApplyPosition release=participant.releaseReceipt();
        if (release==null || participant.nativeGate()==null || !release.equals(participant.nativeGate().released()) || !participant.nativeGate().fence().equals(definition.fence()) || !Arrays.equals(participant.nativeGate().decisionDigest(),source.observedHome().terminalDecision().digest())) {
            var retired=participant.retirementProof(); if (retired==null || !retired.fence().equals(definition.fence()) || !retired.participantGroup().equals(group) || !source.observedHome().consumed()) throw new IllegalArgumentException("Export source lacks exact native gate release proof"); release=retired.position();
        }
        if (release.index()<=token.position().index() || release.term()<token.position().term()) throw new IllegalArgumentException("Released gate proof precedes original capture"); return release;
    }
    private static boolean atLeast(ApplyPosition next,ApplyPosition previous) { return next.term()>=previous.term() && next.index()>=previous.index() && (next.index()!=previous.index() || next.term()==previous.term()); }
    public ChunkIntent freezeChunk(UUID group) throws IOException { admit(); try { return freeze(group); } finally { finish(); } }
    private ChunkIntent freeze(UUID group) throws IOException {
        if (phase!=Phase.EXPORTING) throw new IOException("Export is not accepting chunks");
        if (pending!=null) { if (!pending.group().equals(group)) throw new IOException("Original pending export chunk must be resolved first"); return pending; }
        var state=progress.get(group); if (state==null || state.complete) throw new IllegalArgumentException("Unknown or complete export participant");
        byte[] end=scope(group).upperExclusive(); if ((state.next==null ? 0 : state.next.length)+(long)(end==null ? 0 : end.length)>TransactionScope.MAX_BOUND_BYTES) throw new IOException("Export continuation exceeds current native lower/upper bound budget; projection remains incomplete");
        if (chunks.size()>=definition.limits().maximumChunks() || definition.limits().chunk().maximumFileBytes()>definition.limits().maximumDiskBytes()-disk || log.size()+2L*(MAX_RECORD+FRAME)+101L*(chunks.size()+1-acknowledged.size())+4096>definition.limits().maximumJournalBytes()) throw new IOException("Export future artifact/manifest capacity unavailable");
        var intent=new ChunkIntent(group,definition.column(),captures.get(group).position(),state.next,state.ordinal,definition.limits().chunk()); var artifact=artifactPath(intent);
        if (Files.exists(artifact,LinkOption.NOFOLLOW_LINKS) || Files.exists(sidecar(artifact),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Unbound export artifact already exists"); append(3,intentBytes(intent)); pending=intent; return intent;
    }
    public Path artifactPath(ChunkIntent intent) { return path.resolveSibling(hex(hash(path.getFileName().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)))+"-"+definition.jobId()+"-"+intent.group()+"-"+intent.ordinal()+".sst"); }
    public Chunk export(UUID group,ChunkWriter writer) throws Exception {
        Objects.requireNonNull(writer); admit(); try { var intent=freeze(group); var recovered=recover(); if (recovered!=null) return recovered; return publishInternal(intent,writer.write(artifactPath(intent),intent)); } finally { finish(); }
    }
    public Chunk publish(ChunkIntent intent,NativeRangeSstExport.Chunk nativeChunk) throws IOException { admit(); try { return publishInternal(intent,nativeChunk); } finally { finish(); } }
    private Chunk publishInternal(ChunkIntent intent,NativeRangeSstExport.Chunk result) throws IOException {
        if (phase!=Phase.EXPORTING) throw new IOException("Terminal export cannot publish a late native chunk");
        requireIntent(intent); var manifest=result.manifest(); var participant=definition.fence().participant(intent.group()); byte[] end=manifest.upperExclusive(),finalEnd=scope(intent.group()).upperExclusive();
        if (!manifest.groupId().equals(intent.group()) || manifest.rangeEpoch()!=participant.rangeEpoch() || !manifest.columnId().equals(definition.column()) || !Arrays.equals(manifest.schemaDigest(),participant.schemaDigest()) || !manifest.position().equals(intent.cut()) || !Arrays.equals(manifest.lowerInclusive(),intent.scopeStart()) || (result.hasMore() ? end==null || finalEnd!=null && Arrays.compareUnsigned(end,finalEnd)>=0 : !Arrays.equals(end,finalEnd)) || manifest.rows()>intent.limits().maximumRows() || manifest.payloadBytes()>intent.limits().maximumPayloadBytes() || manifest.fileBytes()>intent.limits().maximumFileBytes()) throw new IOException("Native chunk changed original source/coverage/limits");
        Path file=manifest.file(); if (file!=null && !file.normalize().equals(artifactPath(intent))) throw new IOException("Native chunk artifact path changed");
        byte[] hash=manifest.rows()==0 ? emptyHash(intent,end) : manifest.sha256(); var chunk=new Chunk(definition.jobId(),intent.group(),participant.rangeEpoch(),intent.cut(),intent.ordinal(),definition.column(),intent.scopeStart(),end,hash,manifest.rows(),manifest.payloadBytes(),manifest.fileBytes(),file,result.hasMore()); verifyArtifact(chunk);
        if (disk+chunk.fileBytes()>definition.limits().maximumDiskBytes()) throw new IOException("Export disk limit exceeded"); byte[] descriptor=chunkBytes(chunk);
        SessionJournalFiles.write(sidecar(artifactPath(intent)),ByteBuffer.allocate(32+descriptor.length).put(hash(intentBytes(intent))).put(descriptor).array(),MAX_RECORD);
        commitChunk(chunk,descriptor); return chunk;
    }
    public Chunk recoverChunk() throws IOException { admit(); try { return recover(); } finally { finish(); } }
    private Chunk recover() throws IOException {
        if (pending==null) return null; if (phase!=Phase.EXPORTING) throw new IOException("Terminal export cannot recover a new chunk"); Path artifact=artifactPath(pending),sidecar=sidecar(artifact);
        if (!Files.exists(sidecar,LinkOption.NOFOLLOW_LINKS)) { if (Files.exists(artifact,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Published export artifact lacks exact durable sidecar; cannot rerun blindly"); return null; }
        byte[] body=SessionJournalFiles.read(sidecar,MAX_RECORD); if (body.length<32 || !Arrays.equals(Arrays.copyOf(body,32),hash(intentBytes(pending)))) throw new IOException("Orphan export sidecar changed exact intent"); var descriptor=ByteBuffer.wrap(body,32,body.length-32); var chunk=decodeChunk(descriptor); if (descriptor.hasRemaining() || !Arrays.equals(chunkBytes(chunk),Arrays.copyOfRange(body,32,body.length))) throw new IOException("Noncanonical orphan export sidecar"); requireChunk(chunk); verifyArtifact(chunk); commitChunk(chunk,Arrays.copyOfRange(body,32,body.length)); return chunk;
    }
    private void commitChunk(Chunk chunk,byte[] descriptor) throws IOException { requireChunk(chunk); append(4,descriptor); applyChunk(chunk); }
    private void requireChunk(Chunk chunk) throws IOException { if (chunk.rows()>definition.limits().chunk().maximumRows() || chunk.payloadBytes()>definition.limits().chunk().maximumPayloadBytes() || chunk.fileBytes()>definition.limits().chunk().maximumFileBytes()) throw new IOException("Chunk exceeds frozen native budget"); if (pending==null || !chunk.jobId().equals(definition.jobId()) || !chunk.group().equals(pending.group()) || !chunk.column().equals(pending.column()) || !chunk.cut().equals(pending.cut()) || chunk.ordinal()!=pending.ordinal() || !Arrays.equals(chunk.lowerInclusive(),pending.scopeStart()) || chunk.rangeEpoch()!=definition.fence().participant(chunk.group()).rangeEpoch() || (chunk.hasMore() ? chunk.upperExclusive()==null || scope(chunk.group()).upperExclusive()!=null && Arrays.compareUnsigned(chunk.upperExclusive(),scope(chunk.group()).upperExclusive())>=0 : !Arrays.equals(chunk.upperExclusive(),scope(chunk.group()).upperExclusive()))) throw new IOException("Published export chunk differs from exact pending intent"); }
    private void applyChunk(Chunk chunk) { chunks.add(chunk); disk+=chunk.fileBytes(); var state=progress.get(chunk.group()); state.next=chunk.upperExclusive(); state.ordinal++; state.complete=!chunk.hasMore(); pending=null; if (progress.values().stream().allMatch(p -> p.complete)) phase=Phase.MATERIALIZED; }
    /** READY additionally certifies original gate release and explicit owner-pin settlement. */
    public void markReady(RatisDurableSnapshot.Snapshot source) throws IOException {
        Objects.requireNonNull(source); admit();
        try {
            if (phase!=Phase.READY && phase!=Phase.MATERIALIZED || pending!=null || !definition.fence().equals(source.initial().fence()) || source.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !source.releaseRequested() || source.observedHome()==null || source.observedHome().phase()!=SnapshotFenceStatus.Phase.COMPLETE || !Arrays.equals(sourceDecision,source.observedHome().terminalDecision().digest())) throw new IllegalArgumentException("READY requires complete coverage and released original snapshot");
            for (var chunk:chunks) verifyArtifact(chunk);
            var out=output(); out.write(source.observedHome().terminalDecision().digest()); out.writeInt(progress.size());
            for (var group:progress.keySet()) {
                var participant=source.participant(group); var token=captures.get(group);
                if (participant.capture()==null || participant.capture().request().mode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !token.equals(participant.captureToken()) || !participant.captureReleased() && !participant.captureReleaseLost()) throw new IllegalArgumentException("READY lacks exact original released capture");
                ApplyPosition released=sourceRelease(source,group,token); if (!atLeast(released,sourceReleases.get(group))) throw new IllegalArgumentException("Original gate release proof regressed");
                uuid(out,group); out.writeBoolean(participant.captureReleaseLost()); position(out,released);
            }
            if (phase!=Phase.READY) { append(8,bytes(out)); phase=Phase.READY; }
        } finally { finish(); }
    }
    public void acknowledge(Chunk chunk) throws IOException { admit(); try { if (phase!=Phase.READY) throw new IllegalStateException("Export ACK requires READY"); String id=hex(hash(chunkBytes(chunk))); if (chunks.stream().noneMatch(saved -> Arrays.equals(uncheckedChunkBytes(saved),uncheckedChunkBytes(chunk)))) throw new IllegalArgumentException("ACK lacks exact published export descriptor"); if (!acknowledged.contains(id)) { if (acknowledged.size()>=chunks.size() || !Arrays.equals(chunkBytes(chunks.get(acknowledged.size())),chunkBytes(chunk))) throw new IllegalStateException("Export acknowledgement must advance the exact published prefix"); append(5,hash(chunkBytes(chunk))); acknowledged.add(id); } } finally { finish(); } }
    public void failSnapshotLost() throws IOException {
        admit();
        try {
            if (phase==Phase.READY || phase==Phase.MATERIALIZED || phase==Phase.RELEASED) throw new IllegalStateException("Retained complete export remains independent of source loss");
            if (phase==Phase.SNAPSHOT_LOST || phase==Phase.CANCELLED) return;
            try { recover(); } catch (IOException invalidOrBareArtifact) { if (fenced) throw invalidOrBareArtifact; }
            if (phase==Phase.MATERIALIZED) return; // An exact durable sidecar rescued the complete immutable coverage.
            long bytes=0;
            if (pending!=null) { var artifact=artifactPath(pending); if (Files.isRegularFile(artifact,LinkOption.NOFOLLOW_LINKS)) bytes=Files.size(artifact); }
            append(6,ByteBuffer.allocate(8).putLong(bytes).array()); orphanBytes=bytes; phase=Phase.SNAPSHOT_LOST;
        } finally { finish(); }
    }
    public void release() throws IOException { admit(); try { if (phase==Phase.RELEASED) return; if (phase!=Phase.READY && phase!=Phase.SNAPSHOT_LOST && phase!=Phase.CANCELLED || phase==Phase.READY && acknowledged.size()!=chunks.size()) throw new IllegalStateException("Release requires all published chunks explicitly acknowledged"); append(7,new byte[0]); phase=Phase.RELEASED; } finally { finish(); } }
    /** Explicit abandonment is distinct from evidence that the original source was lost. */
    public void cancel() throws IOException {
        admit();
        try {
            if (phase==Phase.CANCELLED || phase==Phase.RELEASED || phase==Phase.SNAPSHOT_LOST) return;
            long bytes=0; if (pending!=null) { var artifact=artifactPath(pending); if (Files.isRegularFile(artifact,LinkOption.NOFOLLOW_LINKS)) bytes=Files.size(artifact); }
            append(9,ByteBuffer.allocate(8).putLong(bytes).array()); orphanBytes=bytes; phase=Phase.CANCELLED;
        }
        finally { finish(); }
    }
    /** Caller must first drain all source writers and artifact readers; the journal retains its lock inode. */
    public void reclaimArtifacts() throws IOException {
        admit();
        try {
            if (phase!=Phase.RELEASED) throw new IllegalStateException("Export artifacts remain retained until explicit release");
            if (artifactsReclaimed) return;
            if (!reclaiming) {
                for (var chunk:chunks) verifyArtifact(chunk);
                append(10,new byte[0]); reclaiming=true;
            }
            for (var chunk:chunks) {
                var artifact=artifactPath(new ChunkIntent(chunk.group(),chunk.column(),chunk.cut(),chunk.lowerInclusive(),chunk.ordinal(),definition.limits().chunk()));
                Files.deleteIfExists(artifact); Files.deleteIfExists(NativeRangeSstExport.buildingPath(artifact)); Files.deleteIfExists(sidecar(artifact)); Files.deleteIfExists(sidecar(artifact).resolveSibling(sidecar(artifact).getFileName()+".pending"));
            }
            if (pending!=null) { var artifact=artifactPath(pending); Files.deleteIfExists(artifact); Files.deleteIfExists(NativeRangeSstExport.buildingPath(artifact)); Files.deleteIfExists(sidecar(artifact)); Files.deleteIfExists(sidecar(artifact).resolveSibling(sidecar(artifact).getFileName()+".pending")); }
            SessionJournalFiles.force(path.getParent());
            append(11,new byte[0]); reclaiming=false; artifactsReclaimed=true; disk=orphanBytes=0;
        } finally { finish(); }
    }
    private void requireIntent(ChunkIntent value) throws IOException { if (pending==null || !Arrays.equals(intentBytes(value),intentBytes(pending))) throw new IOException("Original export chunk intent changed"); }
    private TransactionScope.Span scope(UUID group) { return projection(definition.fence().participant(group),definition.column()); }
    private static int lowerOrder(byte[] a,byte[] b) { return a==null ? b==null ? 0 : -1 : b==null ? 1 : Arrays.compareUnsigned(a,b); }
    private static TransactionScope.Span projection(SnapshotFence.Participant participant,UUID column) {
        var spans=participant.scope().spans().stream().filter(s -> s.columnId().equals(column)).sorted(Comparator.comparing(TransactionScope.Span::lowerInclusive,RatisExportJournal::lowerOrder)).toList();
        if (spans.isEmpty() || spans.stream().anyMatch(TransactionScope.Span::isPoint)) throw new IllegalArgumentException("Export projection requires contiguous ranges");
        for (int n=1;n<spans.size();n++) if (spans.get(n-1).upperExclusive()==null || spans.get(n).lowerInclusive()==null || !Arrays.equals(spans.get(n-1).upperExclusive(),spans.get(n).lowerInclusive())) throw new IllegalArgumentException("Export projection overlaps or has a gap");
        return TransactionScope.Span.range(column,spans.getFirst().lowerInclusive(),spans.getLast().upperExclusive());
    }
    private synchronized void admit() throws IOException { open(); if (active) throw new IOException("Export journal work active"); active=true; }
    private synchronized void finish() { active=false; }
    private void open() throws IOException { if (closed || fenced || !lock.isValid()) throw new IOException("Export journal closed/fenced; reopen it"); }
    @Override public synchronized void close() throws IOException { if (active) throw new IOException("Export work active; ownership retained"); closed=true; try { log.close(); } finally { try { lock.release(); } finally { ownership.close(); } } }
    private void append(int kind,byte[] data) throws IOException {
        try { long bound=definition.limits().maximumJournalBytes(); int size=33+data.length; if (size>MAX_RECORD || log.size()+FRAME+size>bound) throw new IOException("Export append exceeds manifest budget"); byte[] payload=ByteBuffer.allocate(size).put((byte)kind).put(chain).put(data).array(),digest=hash(payload); var frame=ByteBuffer.allocate(FRAME+size).putInt(size).put(payload).put(digest).flip(); log.position(log.size()); while (frame.hasRemaining()) log.write(frame); log.force(true); chain=digest; }
        catch (IOException | RuntimeException failure) { fenced=true; if (failure instanceof IOException io) throw io; throw new IOException("Export journal persistence failed",failure); }
    }
    private void replay() throws IOException {
        long size=log.size(); if (size<FRAME+33 || size>64L*1024*1024) throw new IOException("Invalid export journal size");
        long validated=0; boolean torn=false;
        try (var in=new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            long remaining=size;
            while (remaining>0) {
                if (remaining<4) { torn=true; break; }
                int length=in.readInt(); if (length<33 || length>MAX_RECORD) throw new IOException("Invalid export frame length");
                if (length+FRAME>remaining) { torn=true; break; }
                byte[] payload=in.readNBytes(length),digest=in.readNBytes(32);
                if (payload.length!=length || digest.length!=32 || !Arrays.equals(hash(payload),digest)) throw new IOException("Export frame checksum differs");
                var record=ByteBuffer.wrap(payload); int kind=record.get() & 255; byte[] previous=new byte[32]; record.get(previous);
                if (!Arrays.equals(previous,chain)) throw new IOException("Export record chain differs"); applyRecord(kind,record); if (record.hasRemaining()) throw new IOException("Trailing export record bytes");
                chain=digest; remaining-=length+FRAME; validated+=length+FRAME;
            }
        } catch (RuntimeException invalid) { throw new IOException("Invalid export journal",invalid); }
        if (definition==null || size>definition.limits().maximumJournalBytes()) throw new IOException("Export definition/budget absent");
        if (torn) { if (validated==0) throw new IOException("Initial export definition was not durably published"); log.truncate(validated); log.force(true); SessionJournalFiles.force(path.getParent()); }
    }
    private void applyRecord(int kind,ByteBuffer in) throws IOException {
        if (kind==1) { if (definition!=null) throw new IOException("Repeated export definition"); define(decodeDefinition(in)); return; } if (definition==null) throw new IOException("Export record precedes definition");
        switch (kind) {
            case 2 -> { if (!captures.isEmpty() || phase!=Phase.CAPTURING) throw new IOException("Invalid export source binding"); sourceDecision=new byte[32]; in.get(sourceDecision); if (in.getInt()!=progress.size()) throw new IOException("Invalid export capture count"); for (var group:progress.keySet()) { if (!group.equals(uuid(in))) throw new IOException("Export capture order changed"); var token=new SnapshotFenceStatus.Capture(uuid(in),uuid(in),position(in)); var release=position(in); if (release.index()<=token.position().index() || release.term()<token.position().term()) throw new IOException("Export source gate was not released after capture"); captures.put(group,token); sourceReleases.put(group,release); } phase=Phase.EXPORTING; }
            case 3 -> { if (phase!=Phase.EXPORTING || pending!=null) throw new IOException("Invalid export pending transition"); var intent=decodeIntent(in); var state=progress.get(intent.group()); if (state==null || state.complete || intent.ordinal()!=state.ordinal || !Arrays.equals(intent.scopeStart(),state.next) || !intent.cut().equals(captures.get(intent.group()).position()) || !intent.column().equals(definition.column()) || !intent.limits().equals(definition.limits().chunk())) throw new IOException("Export pending cursor/cut changed"); pending=intent; }
            case 4 -> { if (phase!=Phase.EXPORTING) throw new IOException("Export publication after terminal state"); var chunk=decodeChunk(in); requireChunk(chunk); if (chunks.size()>=definition.limits().maximumChunks() || disk+chunk.fileBytes()>definition.limits().maximumDiskBytes()) throw new IOException("Export recovered capacity exceeded"); applyChunk(chunk); }
            case 5 -> { if (phase!=Phase.READY) throw new IOException("Export ACK precedes READY"); byte[] digest=new byte[32]; in.get(digest); String id=hex(digest); if (acknowledged.size()>=chunks.size() || !hex(hash(uncheckedChunkBytes(chunks.get(acknowledged.size())))).equals(id) || !acknowledged.add(id)) throw new IOException("Invalid export acknowledgement"); }
            case 6 -> { if (phase!=Phase.CAPTURING && phase!=Phase.EXPORTING) throw new IOException("Invalid export source loss transition"); orphanBytes=in.getLong(); if (orphanBytes<0 || pending==null && orphanBytes!=0) throw new IOException("Invalid retained orphan artifact bytes"); phase=Phase.SNAPSHOT_LOST; }
            case 7 -> { if (phase!=Phase.READY && phase!=Phase.SNAPSHOT_LOST && phase!=Phase.CANCELLED || phase==Phase.READY && acknowledged.size()!=chunks.size()) throw new IOException("Invalid export release transition"); phase=Phase.RELEASED; }
            case 8 -> {
                if (phase!=Phase.MATERIALIZED || pending!=null) throw new IOException("Export READY precedes complete materialization"); byte[] decision=new byte[32]; in.get(decision); if (!Arrays.equals(decision,sourceDecision) || in.getInt()!=progress.size()) throw new IOException("Export READY participant count differs");
                for (var group:progress.keySet()) { if (!group.equals(uuid(in))) throw new IOException("Export READY participant changed"); int lost=in.get() & 255; var release=position(in); var token=captures.get(group); if (lost>1 || !atLeast(release,sourceReleases.get(group)) || release.index()<=token.position().index() || release.term()<token.position().term()) throw new IOException("Export READY release/capture proof differs"); } phase=Phase.READY;
            }
            case 9 -> { if (phase==Phase.RELEASED || phase==Phase.CANCELLED || phase==Phase.SNAPSHOT_LOST) throw new IOException("Invalid export cancellation transition"); orphanBytes=in.getLong(); if (orphanBytes<0 || pending==null && orphanBytes!=0) throw new IOException("Invalid cancelled artifact accounting"); phase=Phase.CANCELLED; }
            case 10 -> { if (phase!=Phase.RELEASED || reclaiming || artifactsReclaimed) throw new IOException("Invalid export reclamation intent"); reclaiming=true; }
            case 11 -> { if (phase!=Phase.RELEASED || !reclaiming || artifactsReclaimed) throw new IOException("Invalid export reclamation completion"); reclaiming=false; artifactsReclaimed=true; disk=orphanBytes=0; }
            default -> throw new IOException("Unknown export record kind");
        }
    }
    private static Path sidecar(Path artifact) { return artifact.resolveSibling(artifact.getFileName()+".manifest"); }
    private void verifyArtifact(Chunk chunk) throws IOException {
        if (chunk.rows()==0) { var intent=new ChunkIntent(chunk.group(),chunk.column(),chunk.cut(),chunk.lowerInclusive(),chunk.ordinal(),definition.limits().chunk()); if (!Arrays.equals(chunk.sha256(),emptyHash(intent,chunk.upperExclusive()))) throw new IOException("Empty export identity differs"); return; }
        Path expected=artifactPath(new ChunkIntent(chunk.group(),chunk.column(),chunk.cut(),chunk.lowerInclusive(),chunk.ordinal(),definition.limits().chunk())); if (!expected.equals(chunk.file()) || !Files.isRegularFile(expected,LinkOption.NOFOLLOW_LINKS) || Files.size(expected)!=chunk.fileBytes()) throw new IOException("Retained export artifact missing/changed");
        if (!Arrays.equals(fileHash(expected),chunk.sha256())) throw new IOException("Retained export artifact checksum differs"); SessionJournalFiles.force(expected); SessionJournalFiles.force(expected.getParent());
    }
    private byte[] emptyHash(ChunkIntent intent,byte[] end) throws IOException { var out=output(); uuid(out,definition.jobId()); out.write(intentBytes(intent)); bound(out,end); return hash(bytes(out)); }
    private static byte[] fileHash(Path file) throws IOException { var digest=digest(); try (var in=Files.newInputStream(file)) { byte[] buffer=new byte[65536]; for (int n;(n=in.read(buffer))>=0;) digest.update(buffer,0,n); } return digest.digest(); }
    private static MessageDigest digest() { try { return MessageDigest.getInstance("SHA-256"); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    private static byte[] hash(byte[] body) { return digest().digest(body); } private static String hex(byte[] digest) { return HexFormat.of().formatHex(digest); }
    private static byte[] copy(byte[] value) { return value==null ? null : value.clone(); }
    private static final class Encoding extends DataOutputStream {
        final ByteArrayOutputStream buffer;
        Encoding() { this(new ByteArrayOutputStream()); }
        private Encoding(ByteArrayOutputStream buffer) { super(buffer); this.buffer=buffer; }
    }
    private static Encoding output() { return new Encoding(); }
    private static byte[] bytes(DataOutputStream out) throws IOException { out.flush(); return ((Encoding)out).buffer.toByteArray(); }
    private static void uuid(DataOutputStream out,UUID id) throws IOException { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); } private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
    private static void position(DataOutputStream out,ApplyPosition position) throws IOException { out.writeLong(position.term()); out.writeLong(position.index()); } private static ApplyPosition position(ByteBuffer in) { return new ApplyPosition(in.getLong(),in.getLong()); }
    private static void bound(DataOutputStream out,byte[] bound) throws IOException { out.writeInt(bound==null ? -1 : bound.length); if (bound!=null) out.write(bound); }
    private static byte[] bound(ByteBuffer in) throws IOException { int size=in.getInt(); if (size== -1) return null; if (size<0 || size>TransactionScope.MAX_BOUND_BYTES || size>in.remaining()) throw new IOException("Invalid export bound/blob"); byte[] value=new byte[size]; in.get(value); return value; }
    private static byte[] blob(ByteBuffer in,int maximum) throws IOException { int size=in.getInt(); if (size<0 || size>maximum || size>in.remaining()) throw new IOException("Invalid typed export blob"); byte[] bytes=new byte[size]; in.get(bytes); return bytes; }
    private static void limits(DataOutputStream out,NativeRangeSstExport.Limits limits) throws IOException { out.writeLong(limits.maximumRows()); out.writeLong(limits.maximumPayloadBytes()); out.writeLong(limits.maximumFileBytes()); out.writeLong(limits.maximumDuration().toNanos()); }
    private static NativeRangeSstExport.Limits limits(ByteBuffer in) { return new NativeRangeSstExport.Limits(in.getLong(),in.getLong(),in.getLong(),Duration.ofNanos(in.getLong())); }
    private byte[] intentBytes(ChunkIntent intent) throws IOException { var out=output(); uuid(out,intent.group()); uuid(out,intent.column()); position(out,intent.cut()); bound(out,intent.scopeStart()); out.writeLong(intent.ordinal()); limits(out,intent.limits()); return bytes(out); }
    private ChunkIntent decodeIntent(ByteBuffer in) throws IOException { return new ChunkIntent(uuid(in),uuid(in),position(in),bound(in),in.getLong(),limits(in)); }
    private static byte[] definitionBytes(Definition definition) throws IOException { var out=output(); out.writeInt(MAGIC); uuid(out,definition.jobId()); bound(out,definition.fence().encode()); uuid(out,definition.column()); bound(out,definition.sourceJournal().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)); out.write(definition.sourceIdentity()); out.writeInt(definition.limits().maximumChunks()); out.writeLong(definition.limits().maximumJournalBytes()); out.writeLong(definition.limits().maximumDiskBytes()); limits(out,definition.limits().chunk()); return bytes(out); }
    private static Definition decodeDefinition(ByteBuffer in) throws IOException { if (in.getInt()!=MAGIC) throw new IOException("Invalid export definition header"); UUID job=uuid(in); var fence=SnapshotFence.decode(blob(in,SnapshotFence.MAX_BYTES)); UUID column=uuid(in); String source=new String(blob(in,4096),java.nio.charset.StandardCharsets.UTF_8); byte[] identity=new byte[32]; in.get(identity); return new Definition(job,fence,column,Path.of(source),identity,new Limits(in.getInt(),in.getLong(),in.getLong(),limits(in))); }
    private byte[] chunkBytes(Chunk chunk) throws IOException { var out=output(); uuid(out,chunk.jobId()); uuid(out,chunk.group()); out.writeLong(chunk.rangeEpoch()); position(out,chunk.cut()); out.writeLong(chunk.ordinal()); uuid(out,chunk.column()); bound(out,chunk.lowerInclusive()); bound(out,chunk.upperExclusive()); out.write(chunk.sha256()); out.writeLong(chunk.rows()); out.writeLong(chunk.payloadBytes()); out.writeLong(chunk.fileBytes()); out.writeBoolean(chunk.hasMore()); return bytes(out); }
    private byte[] uncheckedChunkBytes(Chunk chunk) { try { return chunkBytes(chunk); } catch (IOException impossible) { throw new java.io.UncheckedIOException(impossible); } }
    private Chunk decodeChunk(ByteBuffer in) throws IOException { UUID job=uuid(in),group=uuid(in); long epoch=in.getLong(); var cut=position(in); long ordinal=in.getLong(); UUID column=uuid(in); byte[] lower=bound(in),upper=bound(in),hash=new byte[32]; in.get(hash); long rows=in.getLong(),payload=in.getLong(),file=in.getLong(); int more=in.get() & 255; if (more>1) throw new IOException("Invalid export continuation"); var intent=new ChunkIntent(group,column,cut,lower,ordinal,definition.limits().chunk()); return new Chunk(job,group,epoch,cut,ordinal,column,lower,upper,hash,rows,payload,file,rows==0 ? null : artifactPath(intent),more==1); }
}
