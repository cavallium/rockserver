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
    public record Definition(UUID jobId,SnapshotFence fence,List<UUID> columns,Path sourceJournal,byte[] sourceIdentity,Limits limits,Duration snapshotTtl) {
        public Definition {
            Objects.requireNonNull(jobId); Objects.requireNonNull(fence); columns=selectedColumns(fence,columns); Objects.requireNonNull(sourceJournal); Objects.requireNonNull(sourceIdentity); Objects.requireNonNull(limits); Objects.requireNonNull(snapshotTtl);
            if (snapshotTtl.isZero() || snapshotTtl.isNegative() || snapshotTtl.compareTo(Duration.ofSeconds(60))>0 || !snapshotTtl.equals(Duration.ofMillis(snapshotTtl.toMillis()))) throw new IllegalArgumentException("Export snapshot TTL must be 1..60000 whole milliseconds");
            sourceJournal=sourceJournal.toAbsolutePath().normalize(); if (sourceIdentity.length!=32 || sourceJournal.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length>4096) throw new IllegalArgumentException("Invalid export source binding"); sourceIdentity=sourceIdentity.clone();
            long future=2L*(MAX_RECORD+FRAME)+fence.encode().length+sourceJournal.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length+8192+16L*columns.size();
            if (future>limits.maximumJournalBytes()) throw new IllegalArgumentException("Export lacks reserved first-chunk/capture/READY capacity");
            long pairs=0; for (var participant:fence.participants()) pairs+=RatisExportJournal.columnsForGroup(fence,columns,participant.groupId()).size();
            if (limits.maximumChunks()<pairs) throw new IllegalArgumentException("Export chunk budget cannot cover selected column/group pairs");
        }
        public List<UUID> columnsForGroup(UUID group) { return RatisExportJournal.columnsForGroup(fence,columns,group); }
        public TransactionScope.Span groupScope(UUID group,UUID column) { if (!columnsForGroup(group).contains(column)) throw new IllegalArgumentException("Unselected export group/column"); return RatisExportJournal.groupScope(fence,group,column); }
        public List<UUID> groups() { return fence.participants().stream().map(SnapshotFence.Participant::groupId).filter(group -> !columnsForGroup(group).isEmpty()).toList(); }
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
    public record SourceOwner(RangeDescriptor route,ReplicaDescriptor replica) {
        public SourceOwner { Objects.requireNonNull(route); Objects.requireNonNull(replica); if (!route.replicas().contains(replica)) throw new IllegalArgumentException("Export owner absent from original route"); }
    }
    public record Snapshot(Definition definition,Phase phase,Map<UUID,SnapshotFenceStatus.Capture> captures,Map<UUID,SourceOwner> owners,Set<UUID> sourceCleanup,ChunkIntent pending,List<Chunk> chunks,Set<String> acknowledged,long pendingArtifactBytes,boolean reclaiming,boolean artifactsReclaimed) { public Snapshot { captures=Map.copyOf(captures); owners=Map.copyOf(owners); sourceCleanup=Set.copyOf(sourceCleanup); chunks=List.copyOf(chunks); acknowledged=Set.copyOf(acknowledged); } }
    @FunctionalInterface public interface ChunkWriter { NativeRangeSstExport.Chunk write(Path artifact,ChunkIntent intent) throws Exception; }
    private static final class Progress { final List<UUID> columns; int columnIndex; byte[] next; long ordinal; boolean complete; Progress(List<UUID> columns,byte[] lower) { this.columns=columns; next=copy(lower); } UUID column() { return columns.get(columnIndex); } }
    private final Path path; private final FileChannel ownership,log; private final FileLock lock;
    private Definition definition; private byte[] frozenFenceDigest; private final Map<UUID,SnapshotFenceStatus.Capture> captures=new LinkedHashMap<>(); private final Map<UUID,ApplyPosition> sourceReleases=new LinkedHashMap<>(); private final Map<UUID,SourceOwner> owners=new LinkedHashMap<>(); private final Set<UUID> sourceCleanup=new LinkedHashSet<>(); private byte[] sourceDecision; private final Map<UUID,Progress> progress=new LinkedHashMap<>(); private final List<Chunk> chunks=new ArrayList<>(); private final Set<String> acknowledged=new HashSet<>();
    private record ChunkKey(UUID group,long ordinal) {}
    private record Published(Chunk chunk,byte[] digest,String id) {}
    private final Map<ChunkKey,Published> chunkIndex=new HashMap<>();
    private ChunkIntent pending; private byte[] chain=new byte[32]; private long disk,orphanBytes; private Phase phase=Phase.CAPTURING; private boolean active,closed,fenced,reclaiming,artifactsReclaimed,sourceSettled;
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
    private void define(Definition value) { definition=value; frozenFenceDigest=hash(value.fence().encode()); for (var group:value.groups()) { var columns=value.columnsForGroup(group); progress.put(group,new Progress(columns,value.groupScope(group,columns.getFirst()).lowerInclusive())); } }
    public synchronized Snapshot snapshot() throws IOException { open(); if (active) throw new IOException("Export journal work active"); return new Snapshot(definition,phase,captures,owners,sourceCleanup,pending,chunks,acknowledged,orphanBytes,reclaiming,artifactsReclaimed); }
    public record Summary(Definition definition,Phase phase,int chunks,int acknowledged,long fileBytes,boolean cleanupPending) {}
    /** Constant-size status view; does not copy retained manifests or capture vectors. */
    public synchronized Summary summary() throws IOException {
        open(); if (active) throw new IOException("Export journal work active");
        return new Summary(definition,phase,chunks.size(),acknowledged.size(),disk,!sourceCleanup.isEmpty() || !sourceSettled && !captures.isEmpty() && (phase==Phase.CANCELLED || phase==Phase.SNAPSHOT_LOST || phase==Phase.RELEASED) || phase==Phase.CAPTURING || phase==Phase.EXPORTING || phase==Phase.MATERIALIZED);
    }
    /** Qualify one retained descriptor without copying its manifest or reading artifact bytes. */
    public synchronized Chunk chunk(UUID group,long ordinal) throws IOException {
        open(); if (active) throw new IOException("Export journal work active");
        if (phase!=Phase.READY) throw new IOException("Export delivery requires READY immutable artifacts");
        var chunk=chunkIndex.get(new ChunkKey(Objects.requireNonNull(group),ordinal));
        if (chunk==null) throw new IllegalArgumentException("Unknown original export chunk");
        return chunk.chunk();
    }
    /** Freeze the original complete read-only cut and released native gates before any chunk build. */
    public void bindSource(RatisDurableSnapshot.Snapshot source) throws IOException {
        Objects.requireNonNull(source); admit();
        try {
            if (!definition.fence().equals(source.initial().fence()) || source.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !definition.snapshotTtl().equals(source.requestedTtl()) || source.observedHome()==null || source.observedHome().phase()!=SnapshotFenceStatus.Phase.COMPLETE) throw new IllegalArgumentException("Export source lacks matching read-only COMPLETE snapshot");
            var bindings=new LinkedHashMap<UUID,SourceOwner>(); var vector=new LinkedHashMap<UUID,SnapshotFenceStatus.Capture>(); var releases=new LinkedHashMap<UUID,ApplyPosition>();
            byte[] decision=source.observedHome().terminalDecision().digest();
            for (var group:sourceGroups()) {
                var participant=source.participant(group); var token=participant.captureToken();
                if (participant.capture()==null || participant.capture().request().mode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !definition.snapshotTtl().equals(participant.capture().request().ttl()) || token==null || !participant.capture().request().owner().equals(token.ownerIncarnation()) || !participant.capture().request().snapshotId().equals(token.snapshotId()) || !token.equals(source.observedHome().progress(group).capture())) throw new IllegalArgumentException("Export source lacks original capture vector");
                if (captures.isEmpty() && (source.releaseRequested() || participant.captureReleased() || participant.captureReleaseLost())) throw new IllegalArgumentException("Original source pin already released before export binding");
                var target=participant.capture().target(); bindings.put(group,new SourceOwner(target.descriptor(),target.replica()));
                vector.put(group,token); releases.put(group,sourceRelease(source,group,token,decision));
            }
            if (!captures.isEmpty()) {
                if (!captures.equals(vector) || !owners.equals(bindings) || !Arrays.equals(sourceDecision,decision)) throw new IllegalArgumentException("Original export source cut changed");
                for (var group:sourceGroups()) if (!atLeast(releases.get(group),sourceReleases.get(group))) throw new IllegalArgumentException("Original released gate proof regressed");
                return;
            }
            if (phase!=Phase.CAPTURING) throw new IOException("Export source capture is unavailable");
            var out=output(); out.write(decision); out.writeInt(vector.size());
            for (var group:sourceGroups()) { uuid(out,group); var token=vector.get(group); uuid(out,token.ownerIncarnation()); uuid(out,token.snapshotId()); position(out,token.position()); position(out,releases.get(group)); var owner=bindings.get(group); route(out,owner.route()); out.writeInt(owner.route().replicas().indexOf(owner.replica())); }
            append(2,bytes(out)); sourceDecision=decision; captures.putAll(vector); owners.putAll(bindings); sourceCleanup.addAll(progress.keySet()); sourceReleases.putAll(releases); phase=Phase.EXPORTING;
        } finally { finish(); }
    }
    private ApplyPosition sourceRelease(RatisDurableSnapshot.Snapshot source,UUID group,SnapshotFenceStatus.Capture token,byte[] decision) {
        var participant=source.participant(group); ApplyPosition release=participant.releaseReceipt();
        if (release==null || participant.nativeGate()==null || !release.equals(participant.nativeGate().released()) || !participant.nativeGate().fence().equals(definition.fence()) || !Arrays.equals(participant.nativeGate().decisionDigest(),decision)) {
            var retired=participant.retirementProof(); if (retired==null || !retired.fence().equals(definition.fence()) || !retired.participantGroup().equals(group) || !source.observedHome().consumed()) throw new IllegalArgumentException("Export source lacks exact native gate release proof"); release=retired.position();
        }
        if (token!=null && (release.index()<=token.position().index() || release.term()<token.position().term())) throw new IllegalArgumentException("Released gate proof precedes original capture"); return release;
    }
    private static boolean atLeast(ApplyPosition next,ApplyPosition previous) { return next.term()>=previous.term() && next.index()>=previous.index() && (next.index()!=previous.index() || next.term()==previous.term()); }
    public ChunkIntent freezeChunk(UUID group) throws IOException { admit(); try { return freeze(group); } finally { finish(); } }
    private ChunkIntent freeze(UUID group) throws IOException {
        if (phase!=Phase.EXPORTING) throw new IOException("Export is not accepting chunks");
        if (pending!=null) { if (!pending.group().equals(group)) throw new IOException("Original pending export chunk must be resolved first"); return pending; }
        var state=progress.get(group); if (state==null || state.complete) throw new IllegalArgumentException("Unknown or complete export participant");
        byte[] end=definition.groupScope(group,state.column()).upperExclusive(); if ((state.next==null ? 0 : state.next.length)+(long)(end==null ? 0 : end.length)>TransactionScope.MAX_BOUND_BYTES) throw new IOException("Export continuation exceeds current native lower/upper bound budget; projection remains incomplete");
        if (chunks.size()>=definition.limits().maximumChunks() || definition.limits().chunk().maximumFileBytes()>definition.limits().maximumDiskBytes()-disk || log.size()+2L*(MAX_RECORD+FRAME)+101L*(chunks.size()+1-acknowledged.size())+4096>definition.limits().maximumJournalBytes()) throw new IOException("Export future artifact/manifest capacity unavailable");
        var intent=new ChunkIntent(group,state.column(),captures.get(group).position(),state.next,state.ordinal,definition.limits().chunk()); var artifact=artifactPath(intent);
        if (Files.exists(artifact,LinkOption.NOFOLLOW_LINKS) || Files.exists(sidecar(artifact),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Unbound export artifact already exists"); append(3,intentBytes(intent)); pending=intent; return intent;
    }
    public Path artifactPath(ChunkIntent intent) { return path.resolveSibling(hex(hash(path.getFileName().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)))+"-"+definition.jobId()+"-"+intent.group()+"-"+intent.ordinal()+".sst"); }
    public Chunk export(UUID group,ChunkWriter writer) throws Exception {
        Objects.requireNonNull(writer); admit(); try { var intent=freeze(group); var recovered=recover(); if (recovered!=null) return recovered; return publishInternal(intent,writer.write(artifactPath(intent),intent)); } finally { finish(); }
    }
    public Chunk publish(ChunkIntent intent,NativeRangeSstExport.Chunk nativeChunk) throws IOException { admit(); try { return publishInternal(intent,nativeChunk); } finally { finish(); } }
    private Chunk publishInternal(ChunkIntent intent,NativeRangeSstExport.Chunk result) throws IOException {
        if (phase!=Phase.EXPORTING) throw new IOException("Terminal export cannot publish a late native chunk");
        requireIntent(intent); var manifest=result.manifest(); var participant=definition.fence().participant(intent.group()); byte[] end=manifest.upperExclusive(),finalEnd=definition.groupScope(intent.group(),intent.column()).upperExclusive();
        if (!manifest.groupId().equals(intent.group()) || manifest.rangeEpoch()!=participant.rangeEpoch() || !manifest.columnId().equals(intent.column()) || !Arrays.equals(manifest.schemaDigest(),participant.schemaDigest()) || !manifest.position().equals(intent.cut()) || !Arrays.equals(manifest.lowerInclusive(),intent.scopeStart()) || (result.hasMore() ? end==null || finalEnd!=null && Arrays.compareUnsigned(end,finalEnd)>=0 : !Arrays.equals(end,finalEnd)) || manifest.rows()>intent.limits().maximumRows() || manifest.payloadBytes()>intent.limits().maximumPayloadBytes() || manifest.fileBytes()>intent.limits().maximumFileBytes()) throw new IOException("Native chunk changed original source/coverage/limits");
        Path file=manifest.file(); if (file!=null && !file.normalize().equals(artifactPath(intent))) throw new IOException("Native chunk artifact path changed");
        byte[] hash=manifest.rows()==0 ? emptyHash(intent,end) : manifest.sha256(); var chunk=new Chunk(definition.jobId(),intent.group(),participant.rangeEpoch(),intent.cut(),intent.ordinal(),intent.column(),intent.scopeStart(),end,hash,manifest.rows(),manifest.payloadBytes(),manifest.fileBytes(),file,result.hasMore()); verifyArtifact(chunk);
        if (disk+chunk.fileBytes()>definition.limits().maximumDiskBytes()) throw new IOException("Export disk limit exceeded"); byte[] descriptor=chunkBytes(chunk);
        SessionJournalFiles.write(sidecar(artifactPath(intent)),ByteBuffer.allocate(32+descriptor.length).put(hash(intentBytes(intent))).put(descriptor).array(),MAX_RECORD);
        commitChunk(chunk,descriptor); return chunk;
    }
    public Chunk recoverChunk() throws IOException { admit(); try { return recover(); } finally { finish(); } }
    private Chunk recover() throws IOException {
        if (pending==null) return null; if (phase!=Phase.EXPORTING) throw new IOException("Terminal export cannot recover a new chunk"); Path artifact=artifactPath(pending),sidecar=sidecar(artifact);
        if (!Files.exists(sidecar,LinkOption.NOFOLLOW_LINKS)) { if (Files.exists(artifact,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Published export artifact lacks exact durable sidecar; cannot rerun blindly"); return null; }
        byte[] body=SessionJournalFiles.read(sidecar,MAX_RECORD); if (body.length<32 || !Arrays.equals(Arrays.copyOf(body,32),hash(intentBytes(pending)))) throw new IOException("Orphan export sidecar changed exact intent"); var descriptor=ByteBuffer.wrap(body,32,body.length-32); var chunk=decodeChunk(descriptor); if (descriptor.hasRemaining() || !Arrays.equals(chunkBytes(chunk),Arrays.copyOfRange(body,32,body.length))) throw new IOException("Noncanonical orphan export sidecar"); requireChunk(chunk); if (chunk.file()!=null && !Files.exists(chunk.file(),LinkOption.NOFOLLOW_LINKS)) { var downloaded=download(chunk.file()); if (!Files.isRegularFile(downloaded,LinkOption.NOFOLLOW_LINKS) || Files.size(downloaded)!=chunk.fileBytes() || !Arrays.equals(fileHash(downloaded),chunk.sha256())) throw new IOException("Incomplete downloaded artifact"); SessionJournalFiles.force(downloaded); Files.move(downloaded,chunk.file(),StandardCopyOption.ATOMIC_MOVE); SessionJournalFiles.force(chunk.file().getParent()); } verifyArtifact(chunk); commitChunk(chunk,Arrays.copyOfRange(body,32,body.length)); return chunk;
    }
    private void commitChunk(Chunk chunk,byte[] descriptor) throws IOException { requireChunk(chunk); append(4,descriptor); applyChunk(chunk); }
    private void requireChunk(Chunk chunk) throws IOException { if (chunk.rows()>definition.limits().chunk().maximumRows() || chunk.payloadBytes()>definition.limits().chunk().maximumPayloadBytes() || chunk.fileBytes()>definition.limits().chunk().maximumFileBytes()) throw new IOException("Chunk exceeds frozen native budget"); if (pending==null || !chunk.jobId().equals(definition.jobId()) || !chunk.group().equals(pending.group()) || !chunk.column().equals(pending.column()) || !chunk.cut().equals(pending.cut()) || chunk.ordinal()!=pending.ordinal() || !Arrays.equals(chunk.lowerInclusive(),pending.scopeStart()) || chunk.rangeEpoch()!=definition.fence().participant(chunk.group()).rangeEpoch() || (chunk.hasMore() ? chunk.upperExclusive()==null || definition.groupScope(chunk.group(),chunk.column()).upperExclusive()!=null && Arrays.compareUnsigned(chunk.upperExclusive(),definition.groupScope(chunk.group(),chunk.column()).upperExclusive())>=0 : !Arrays.equals(chunk.upperExclusive(),definition.groupScope(chunk.group(),chunk.column()).upperExclusive()))) throw new IOException("Published export chunk differs from exact pending intent"); }
    private void applyChunk(Chunk chunk) { chunks.add(chunk); byte[] digest=hash(uncheckedChunkBytes(chunk)); chunkIndex.put(new ChunkKey(chunk.group(),chunk.ordinal()),new Published(chunk,digest,hex(digest))); disk+=chunk.fileBytes(); var state=progress.get(chunk.group()); state.ordinal++; if (chunk.hasMore()) state.next=chunk.upperExclusive(); else if (++state.columnIndex==state.columns.size()) state.complete=true; else state.next=definition.groupScope(chunk.group(),state.column()).lowerInclusive(); pending=null; if (progress.values().stream().allMatch(p -> p.complete)) phase=Phase.MATERIALIZED; }
    /** READY additionally certifies original gate release and explicit owner-pin settlement. */
    public void markReady(RatisDurableSnapshot.Snapshot source) throws IOException {
        Objects.requireNonNull(source); admit();
        try {
            if (phase!=Phase.READY && phase!=Phase.MATERIALIZED || !sourceCleanup.isEmpty() || pending!=null || !definition.fence().equals(source.initial().fence()) || source.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !definition.snapshotTtl().equals(source.requestedTtl()) || !source.releaseRequested() || source.observedHome()==null || source.observedHome().phase()!=SnapshotFenceStatus.Phase.COMPLETE || !Arrays.equals(sourceDecision,source.observedHome().terminalDecision().digest())) throw new IllegalArgumentException("READY requires complete coverage and released original snapshot");
            for (var chunk:chunks) verifyArtifact(chunk);
            var out=output(); out.write(sourceDecision); out.writeInt(sourceGroups().size());
            for (var group:sourceGroups()) {
                var participant=source.participant(group); var token=captures.get(group);
                if (participant.capture()==null || participant.capture().request().mode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !definition.snapshotTtl().equals(participant.capture().request().ttl()) || !token.equals(participant.captureToken()) || !participant.captureReleased() && !participant.captureReleaseLost()) throw new IllegalArgumentException("READY lacks exact original released capture");
                ApplyPosition released=sourceRelease(source,group,token,sourceDecision); if (!atLeast(released,sourceReleases.get(group))) throw new IllegalArgumentException("Original gate release proof regressed");
                uuid(out,group); out.writeBoolean(participant.captureReleaseLost()); position(out,released);
            }
            if (phase!=Phase.READY) { append(8,bytes(out)); phase=Phase.READY; } sourceSettled=true;
        } finally { finish(); }
    }
    public synchronized boolean sourceSettled() throws IOException { open(); return sourceSettled; }
    /** Force compact proof after the original read-only source has actually released every gate and pin. */
    public void confirmSourceSettlement(RatisDurableSnapshot.Snapshot source) throws IOException {
        Objects.requireNonNull(source); admit();
        try {
            if (sourceSettled) return;
            if (phase!=Phase.CANCELLED && phase!=Phase.SNAPSHOT_LOST && phase!=Phase.RELEASED) throw new IllegalStateException("Source settlement requires terminal export cleanup");
            var home=source.observedHome();
            if (!definition.fence().equals(source.initial().fence()) || source.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !definition.snapshotTtl().equals(source.requestedTtl()) || !source.releaseRequested() || home==null || !home.terminal() || !home.consumed() || !home.fence().equals(definition.fence())) throw new IllegalArgumentException("Source settlement lacks original consumed read-only snapshot");
            byte[] decision=home.terminalDecision().digest(); if (sourceDecision!=null && !Arrays.equals(sourceDecision,decision)) throw new IllegalArgumentException("Source settlement decision changed");
            var out=output(); out.write(frozenFenceDigest); out.write(definition.sourceIdentity()); out.writeLong(definition.snapshotTtl().toNanos()); out.write(decision); out.writeBoolean(home.phase()==SnapshotFenceStatus.Phase.COMPLETE); out.writeInt(definition.fence().participants().size());
            for (var frozen:definition.fence().participants()) {
                var participant=source.participant(frozen.groupId()); var token=participant.captureToken(); var expected=captures.get(frozen.groupId());
                if (expected!=null && !expected.equals(token)) throw new IllegalArgumentException("Settled original capture changed");
                if (participant.capture()!=null) {
                    var request=participant.capture().request();
                    if (request.mode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !definition.snapshotTtl().equals(request.ttl()) || !request.fence().equals(definition.fence()) || token!=null && (!request.owner().equals(token.ownerIncarnation()) || !request.snapshotId().equals(token.snapshotId()))) throw new IllegalArgumentException("Settled capture identity changed");
                }
                boolean existed=participant.capture()!=null || token!=null || home.progress(frozen.groupId()).capture()!=null;
                if (existed && !participant.captureReleased() && !participant.captureReleaseLost()) throw new IllegalArgumentException("Original source pin settlement remains unresolved");
                var released=sourceRelease(source,frozen.groupId(),token,decision); var previous=sourceReleases.get(frozen.groupId()); if (previous!=null && !atLeast(released,previous)) throw new IllegalArgumentException("Original gate release proof regressed");
                uuid(out,frozen.groupId()); out.writeBoolean(token!=null); if (token!=null) { uuid(out,token.ownerIncarnation()); uuid(out,token.snapshotId()); position(out,token.position()); }
                out.writeByte(!existed ? 0 : participant.captureReleaseLost() ? 2 : 1); position(out,released);
            }
            byte[] proof=bytes(out); validateSettlement(ByteBuffer.wrap(proof)); append(13,proof); sourceSettled=true;
        } finally { finish(); }
    }
    private void validateSettlement(ByteBuffer in) throws IOException {
        byte[] cut=new byte[32],identity=new byte[32],decision=new byte[32]; in.get(cut); in.get(identity); long ttl=in.getLong(); in.get(decision); int complete=in.get()&255;
        if (!Arrays.equals(cut,frozenFenceDigest) || !Arrays.equals(identity,definition.sourceIdentity()) || ttl!=definition.snapshotTtl().toNanos() || complete>1 || sourceDecision!=null && (complete!=1 || !Arrays.equals(sourceDecision,decision)) || in.getInt()!=definition.fence().participants().size()) throw new IOException("Source settlement definition/decision changed");
        for (var frozen:definition.fence().participants()) {
            if (!uuid(in).equals(frozen.groupId())) throw new IOException("Source settlement participant changed"); int present=in.get()&255; if (present>1) throw new IOException("Invalid settled capture flag");
            var token=present==0 ? null : new SnapshotFenceStatus.Capture(uuid(in),uuid(in),position(in)); int disposition=in.get()&255; var released=position(in); var expected=captures.get(frozen.groupId()); var previous=sourceReleases.get(frozen.groupId());
            if (disposition>2 || disposition==0 && token!=null || disposition==1 && token==null || expected!=null && !expected.equals(token) || released.term()<1 || released.index()<1 || token!=null && (released.index()<=token.position().index() || released.term()<token.position().term()) || previous!=null && !atLeast(released,previous)) throw new IOException("Source settlement gate/pin proof changed");
        }
    }
    public void acknowledge(Chunk chunk) throws IOException {
        admit();
        try {
            if (phase!=Phase.READY) throw new IllegalStateException("Export ACK requires READY");
            var published=chunkIndex.get(new ChunkKey(chunk.group(),chunk.ordinal()));
            if (published==null || !sameChunk(published.chunk(),chunk)) throw new IllegalArgumentException("ACK lacks exact published export descriptor");
            if (!acknowledged.contains(published.id())) {
                if (acknowledged.size()>=chunks.size() || !sameChunk(chunks.get(acknowledged.size()),chunk)) throw new IllegalStateException("Export acknowledgement must advance the exact published prefix");
                append(5,published.digest()); acknowledged.add(published.id());
            }
        } finally { finish(); }
    }
    private static boolean sameChunk(Chunk a,Chunk b) {
        return a==b || a.jobId.equals(b.jobId) && a.group.equals(b.group) && a.rangeEpoch==b.rangeEpoch && a.cut.equals(b.cut) && a.ordinal==b.ordinal && a.column.equals(b.column)
                && Arrays.equals(a.lowerInclusive,b.lowerInclusive) && Arrays.equals(a.upperExclusive,b.upperExclusive) && Arrays.equals(a.sha256,b.sha256)
                && a.rows==b.rows && a.payloadBytes==b.payloadBytes && a.fileBytes==b.fileBytes && Objects.equals(a.file,b.file) && a.hasMore==b.hasMore;
    }
    public void failSnapshotLost() throws IOException {
        admit();
        try {
            if (phase==Phase.READY || phase==Phase.MATERIALIZED || phase==Phase.RELEASED) throw new IllegalStateException("Retained complete export remains independent of source loss");
            if (phase==Phase.SNAPSHOT_LOST || phase==Phase.CANCELLED) return;
            try { recover(); } catch (IOException invalidOrBareArtifact) { if (fenced) throw invalidOrBareArtifact; }
            if (phase==Phase.MATERIALIZED) return; // An exact durable sidecar rescued the complete immutable coverage.
            long bytes=retainedPendingBytes();
            append(6,ByteBuffer.allocate(8).putLong(bytes).array()); orphanBytes=bytes; phase=Phase.SNAPSHOT_LOST;
        } finally { finish(); }
    }
    public void release() throws IOException { admit(); try { if (phase==Phase.RELEASED) return; if (phase!=Phase.READY && phase!=Phase.SNAPSHOT_LOST && phase!=Phase.CANCELLED || phase==Phase.READY && acknowledged.size()!=chunks.size()) throw new IllegalStateException("Release requires all published chunks explicitly acknowledged"); append(7,new byte[0]); phase=Phase.RELEASED; } finally { finish(); } }
    /** Explicit abandonment is distinct from evidence that the original source was lost. */
    public void cancel() throws IOException {
        admit();
        try {
            if (phase==Phase.CANCELLED || phase==Phase.RELEASED || phase==Phase.SNAPSHOT_LOST) return;
            long bytes=retainedPendingBytes();
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
                Files.deleteIfExists(artifact); Files.deleteIfExists(download(artifact)); Files.deleteIfExists(NativeRangeSstExport.buildingPath(artifact)); Files.deleteIfExists(sidecar(artifact)); Files.deleteIfExists(sidecar(artifact).resolveSibling(sidecar(artifact).getFileName()+".pending"));
            }
            if (pending!=null) { var artifact=artifactPath(pending); Files.deleteIfExists(artifact); Files.deleteIfExists(download(artifact)); Files.deleteIfExists(NativeRangeSstExport.buildingPath(artifact)); Files.deleteIfExists(sidecar(artifact)); Files.deleteIfExists(sidecar(artifact).resolveSibling(sidecar(artifact).getFileName()+".pending")); }
            SessionJournalFiles.force(path.getParent());
            append(11,new byte[0]); reclaiming=false; artifactsReclaimed=true; disk=orphanBytes=0;
        } finally { finish(); }
    }
    public void sourceReleased(UUID group) throws IOException { admit(); try { if (!sourceCleanup.contains(group)) return; if (phase==Phase.CAPTURING || phase==Phase.EXPORTING) throw new IOException("Source artifacts still needed"); var out=output(); uuid(out,group); append(12,bytes(out)); sourceCleanup.remove(group); } finally { finish(); } }
    public static RatisExportSourceRpc.JobDefinition sourceDefinition(Snapshot saved,UUID group) { var d=saved.definition(); if (d.columnsForGroup(group).isEmpty()) throw new IllegalArgumentException("Group has no selected export column"); return new RatisExportSourceRpc.JobDefinition(d.jobId(),d.fence(),group,d.columns(),saved.captures().get(group),d.limits()); }
    /** Force the exact local descriptor before renaming a complete download into the artifact namespace. */
    static void publishDownload(RatisExportSourceRpc.BuildRequest request,RatisExportSourceRpc.Descriptor descriptor,Path artifact) throws IOException {
        var i=request.intent(); var d=request.definition(); var hash=descriptor.rows()==0 ? emptyHash(d.jobId(),i,descriptor.upperExclusive()) : descriptor.contentHash();
        var chunk=new Chunk(d.jobId(),i.group(),descriptor.rangeEpoch(),i.cut(),i.ordinal(),i.column(),i.scopeStart(),descriptor.upperExclusive(),hash,descriptor.rows(),descriptor.payloadBytes(),descriptor.fileBytes(),descriptor.rows()==0 ? null : artifact,descriptor.hasMore());
        var bytes=chunkBytes(chunk); SessionJournalFiles.write(sidecar(artifact),ByteBuffer.allocate(32+bytes.length).put(hash(intentBytes(i))).put(bytes).array(),MAX_RECORD);
        if (chunk.rows()>0) { Files.move(download(artifact),artifact,StandardCopyOption.ATOMIC_MOVE); SessionJournalFiles.force(artifact.getParent()); }
    }
    static Path download(Path artifact) { return artifact.resolveSibling(artifact.getFileName()+".download"); }
    private static void route(DataOutputStream out,RangeDescriptor route) throws IOException { var bytes=ByteBuffer.allocate(Math.toIntExact(SessionWindowJournal.routeSize(route))); SessionWindowJournal.route(bytes,route); out.write(bytes.array()); }
    private long retainedPendingBytes() throws IOException {
        if (pending==null) return 0;
        var artifact=artifactPath(pending); long bytes=0;
        for (var file:List.of(artifact,download(artifact))) if (Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)) {
            try { bytes=Math.addExact(bytes,Files.size(file)); } catch (ArithmeticException overflow) { throw new IOException("Retained export artifact byte count overflow",overflow); }
        }
        return bytes;
    }
    private void requireIntent(ChunkIntent value) throws IOException { if (pending==null || !Arrays.equals(intentBytes(value),intentBytes(pending))) throw new IOException("Original export chunk intent changed"); }
    private List<UUID> sourceGroups() { return definition.fence().participants().stream().map(SnapshotFence.Participant::groupId).toList(); }
    public synchronized UUID unfinishedGroup() throws IOException { open(); return progress.entrySet().stream().filter(entry -> !entry.getValue().complete).map(Map.Entry::getKey).findFirst().orElseThrow(() -> new IOException("Export has no unfinished participant")); }
    public static List<UUID> selectedColumns(SnapshotFence fence,List<UUID> columns) { var selected=RatisExportSourceRpc.canonicalColumns(columns); RatisExportSourceRpc.validateColumns(fence,selected); return selected; }
    public static List<UUID> columnsForGroup(SnapshotFence fence,List<UUID> columns,UUID group) { return RatisExportSourceRpc.columnsForGroup(fence,group,columns); }
    public static TransactionScope.Span groupScope(SnapshotFence fence,UUID group,UUID column) { return RatisExportSourceRpc.projection(fence.participant(group),column); }
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
            case 2 -> { if (!captures.isEmpty() || phase!=Phase.CAPTURING) throw new IOException("Invalid export source binding"); sourceDecision=new byte[32]; in.get(sourceDecision); if (in.getInt()!=sourceGroups().size()) throw new IOException("Invalid export capture count"); for (var group:sourceGroups()) { if (!group.equals(uuid(in))) throw new IOException("Export capture order changed"); var token=new SnapshotFenceStatus.Capture(uuid(in),uuid(in),position(in)); var release=position(in); if (release.index()<=token.position().index() || release.term()<token.position().term()) throw new IOException("Export source gate was not released after capture"); var route=SessionWindowJournal.route(in); int member=in.getInt(); if (!route.groupId().equals(group) || member<0 || member>=route.replicas().size()) throw new IOException("Invalid original export owner"); owners.put(group,new SourceOwner(route,route.replicas().get(member))); if (progress.containsKey(group)) sourceCleanup.add(group); captures.put(group,token); sourceReleases.put(group,release); } phase=Phase.EXPORTING; }
            case 3 -> { if (phase!=Phase.EXPORTING || pending!=null) throw new IOException("Invalid export pending transition"); var intent=decodeIntent(in); var state=progress.get(intent.group()); if (state==null || state.complete || intent.ordinal()!=state.ordinal || !Arrays.equals(intent.scopeStart(),state.next) || !intent.cut().equals(captures.get(intent.group()).position()) || !intent.column().equals(state.column()) || !intent.limits().equals(definition.limits().chunk())) throw new IOException("Export pending cursor/cut changed"); pending=intent; }
            case 4 -> { if (phase!=Phase.EXPORTING) throw new IOException("Export publication after terminal state"); var chunk=decodeChunk(in); requireChunk(chunk); if (chunks.size()>=definition.limits().maximumChunks() || disk+chunk.fileBytes()>definition.limits().maximumDiskBytes()) throw new IOException("Export recovered capacity exceeded"); applyChunk(chunk); }
            case 5 -> { if (phase!=Phase.READY) throw new IOException("Export ACK precedes READY"); byte[] digest=new byte[32]; in.get(digest); String id=hex(digest); if (acknowledged.size()>=chunks.size() || !hex(hash(uncheckedChunkBytes(chunks.get(acknowledged.size())))).equals(id) || !acknowledged.add(id)) throw new IOException("Invalid export acknowledgement"); }
            case 6 -> { if (phase!=Phase.CAPTURING && phase!=Phase.EXPORTING) throw new IOException("Invalid export source loss transition"); orphanBytes=in.getLong(); if (orphanBytes<0 || pending==null && orphanBytes!=0) throw new IOException("Invalid retained orphan artifact bytes"); phase=Phase.SNAPSHOT_LOST; }
            case 7 -> { if (phase!=Phase.READY && phase!=Phase.SNAPSHOT_LOST && phase!=Phase.CANCELLED || phase==Phase.READY && acknowledged.size()!=chunks.size()) throw new IOException("Invalid export release transition"); phase=Phase.RELEASED; }
            case 8 -> {
                if (phase!=Phase.MATERIALIZED || !sourceCleanup.isEmpty() || pending!=null) throw new IOException("Export READY precedes complete materialization"); byte[] decision=new byte[32]; in.get(decision); if (!Arrays.equals(decision,sourceDecision) || in.getInt()!=sourceGroups().size()) throw new IOException("Export READY participant count differs");
                for (var group:sourceGroups()) { if (!group.equals(uuid(in))) throw new IOException("Export READY participant changed"); int lost=in.get() & 255; var release=position(in); var token=captures.get(group); if (lost>1 || !atLeast(release,sourceReleases.get(group)) || release.index()<=token.position().index() || release.term()<token.position().term()) throw new IOException("Export READY release/capture proof differs"); } phase=Phase.READY; sourceSettled=true;
            }
            case 9 -> { if (phase==Phase.RELEASED || phase==Phase.CANCELLED || phase==Phase.SNAPSHOT_LOST) throw new IOException("Invalid export cancellation transition"); orphanBytes=in.getLong(); if (orphanBytes<0 || pending==null && orphanBytes!=0) throw new IOException("Invalid cancelled artifact accounting"); phase=Phase.CANCELLED; }
            case 10 -> { if (phase!=Phase.RELEASED || reclaiming || artifactsReclaimed) throw new IOException("Invalid export reclamation intent"); reclaiming=true; }
            case 11 -> { if (phase!=Phase.RELEASED || !reclaiming || artifactsReclaimed) throw new IOException("Invalid export reclamation completion"); reclaiming=false; artifactsReclaimed=true; disk=orphanBytes=0; }
            case 12 -> { if (phase==Phase.CAPTURING || phase==Phase.EXPORTING) throw new IOException("Source cleanup precedes materialization"); var group=uuid(in); if (!sourceCleanup.remove(group)) throw new IOException("Invalid export source cleanup receipt"); }
            case 13 -> { if (sourceSettled || phase!=Phase.CANCELLED && phase!=Phase.SNAPSHOT_LOST && phase!=Phase.RELEASED) throw new IOException("Invalid source settlement transition"); validateSettlement(in); sourceSettled=true; }
            default -> throw new IOException("Unknown export record kind");
        }
    }
    private static Path sidecar(Path artifact) { return artifact.resolveSibling(artifact.getFileName()+".manifest"); }
    private void verifyArtifact(Chunk chunk) throws IOException {
        if (chunk.rows()==0) { var intent=new ChunkIntent(chunk.group(),chunk.column(),chunk.cut(),chunk.lowerInclusive(),chunk.ordinal(),definition.limits().chunk()); if (!Arrays.equals(chunk.sha256(),emptyHash(intent,chunk.upperExclusive()))) throw new IOException("Empty export identity differs"); return; }
        Path expected=artifactPath(new ChunkIntent(chunk.group(),chunk.column(),chunk.cut(),chunk.lowerInclusive(),chunk.ordinal(),definition.limits().chunk())); if (!expected.equals(chunk.file()) || !Files.isRegularFile(expected,LinkOption.NOFOLLOW_LINKS) || Files.size(expected)!=chunk.fileBytes()) throw new IOException("Retained export artifact missing/changed");
        if (!Arrays.equals(fileHash(expected),chunk.sha256())) throw new IOException("Retained export artifact checksum differs"); SessionJournalFiles.force(expected); SessionJournalFiles.force(expected.getParent());
    }
    private byte[] emptyHash(ChunkIntent intent,byte[] end) throws IOException { return emptyHash(definition.jobId(),intent,end); }
    private static byte[] emptyHash(UUID job,ChunkIntent intent,byte[] end) throws IOException { var out=output(); uuid(out,job); out.write(intentBytes(intent)); bound(out,end); return hash(bytes(out)); }
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
    private static byte[] intentBytes(ChunkIntent intent) throws IOException { var out=output(); uuid(out,intent.group()); uuid(out,intent.column()); position(out,intent.cut()); bound(out,intent.scopeStart()); out.writeLong(intent.ordinal()); limits(out,intent.limits()); return bytes(out); }
    private ChunkIntent decodeIntent(ByteBuffer in) throws IOException { return new ChunkIntent(uuid(in),uuid(in),position(in),bound(in),in.getLong(),limits(in)); }
    private static byte[] definitionBytes(Definition definition) throws IOException { var out=output(); out.writeInt(MAGIC); uuid(out,definition.jobId()); bound(out,definition.fence().encode()); out.writeInt(definition.columns().size()); for (var column:definition.columns()) uuid(out,column); bound(out,definition.sourceJournal().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)); out.write(definition.sourceIdentity()); out.writeInt(definition.limits().maximumChunks()); out.writeLong(definition.limits().maximumJournalBytes()); out.writeLong(definition.limits().maximumDiskBytes()); limits(out,definition.limits().chunk()); out.writeLong(definition.snapshotTtl().toNanos()); return bytes(out); }
    private static Definition decodeDefinition(ByteBuffer in) throws IOException { if (in.getInt()!=MAGIC) throw new IOException("Invalid export definition header"); UUID job=uuid(in); var fence=SnapshotFence.decode(blob(in,SnapshotFence.MAX_BYTES)); int count=in.getInt(); if (count<1 || count>RangeDirectoryCodec.MAX_COLUMNS) throw new IOException("Invalid export column count"); var columns=new ArrayList<UUID>(count); for (int n=0;n<count;n++) columns.add(uuid(in)); if (!columns.equals(selectedColumns(fence,columns))) throw new IOException("Noncanonical export columns"); String source=new String(blob(in,4096),java.nio.charset.StandardCharsets.UTF_8); byte[] identity=new byte[32]; in.get(identity); return new Definition(job,fence,columns,Path.of(source),identity,new Limits(in.getInt(),in.getLong(),in.getLong(),limits(in)),Duration.ofNanos(in.getLong())); }
    private static byte[] chunkBytes(Chunk chunk) throws IOException { var out=output(); uuid(out,chunk.jobId()); uuid(out,chunk.group()); out.writeLong(chunk.rangeEpoch()); position(out,chunk.cut()); out.writeLong(chunk.ordinal()); uuid(out,chunk.column()); bound(out,chunk.lowerInclusive()); bound(out,chunk.upperExclusive()); out.write(chunk.sha256()); out.writeLong(chunk.rows()); out.writeLong(chunk.payloadBytes()); out.writeLong(chunk.fileBytes()); out.writeBoolean(chunk.hasMore()); return bytes(out); }
    private byte[] uncheckedChunkBytes(Chunk chunk) { try { return chunkBytes(chunk); } catch (IOException impossible) { throw new java.io.UncheckedIOException(impossible); } }
    private Chunk decodeChunk(ByteBuffer in) throws IOException { UUID job=uuid(in),group=uuid(in); long epoch=in.getLong(); var cut=position(in); long ordinal=in.getLong(); UUID column=uuid(in); byte[] lower=bound(in),upper=bound(in),hash=new byte[32]; in.get(hash); long rows=in.getLong(),payload=in.getLong(),file=in.getLong(); int more=in.get() & 255; if (more>1) throw new IOException("Invalid export continuation"); var intent=new ChunkIntent(group,column,cut,lower,ordinal,definition.limits().chunk()); return new Chunk(job,group,epoch,cut,ordinal,column,lower,upper,hash,rows,payload,file,rows==0 ? null : artifactPath(intent),more==1); }
}
