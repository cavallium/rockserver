package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/** Exclusive bounded current input and group-chunk progress; never retains or recreates an unseen publisher tail. */
final class RatisBatchRequestJournal implements AutoCloseable {
    static final int MAX_BYTES=16*1024*1024,COMPLETED_WINDOW=256;
    static final class Request {
        RemoteMutationToken token; boolean resultAcknowledged; long column; final Map<Long,byte[]> inputDigests=new LinkedHashMap<>(); final UUID id; final boolean merge; final RangeDirectory directory;
        long retiredInputThrough=-1,copiedBatches,acknowledgedChunks,acknowledgedMutations;
        List<DistributedCommand> chunks=List.of(); int cursor; OperationId pending; CommandResult result,failure; boolean eof;
        Request(UUID id,boolean merge,RangeDirectory directory) { this.id=id; this.merge=merge; this.directory=directory; }
        boolean complete() { return eof && chunks.isEmpty() && pending==null && failure==null; }
    }
    private final Path path; private final byte[] binding; private final FileChannel channel; private final FileLock lock;
    private Request request; private boolean closed,fenced;
    RatisBatchRequestJournal(Path path,byte[] binding) throws IOException {
        this.path=path.toAbsolutePath(); this.binding=binding.clone();
        if(binding.length!=32 || !Files.isDirectory(this.path.getParent())) throw new IOException("Invalid batch journal binding/parent");
        channel=FileChannel.open(this.path.resolveSibling(this.path.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE); FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch(OverlappingFileLockException held) { throw new IOException("Batch journal already owned",held); }
            if(acquired==null) throw new IOException("Batch journal already owned"); lock=acquired;
            if(Files.exists(this.path)) {
                byte[] body=SessionJournalFiles.read(this.path,MAX_BYTES); request=decode(body);
                if(!Arrays.equals(body,encode())) throw new IOException("Noncanonical batch journal");
                try(var file=FileChannel.open(this.path,StandardOpenOption.READ)) { file.force(true); }
                SessionJournalFiles.force(this.path.getParent());
            }
            else if(Files.deleteIfExists(this.path.resolveSibling(this.path.getFileName()+".pending"))) SessionJournalFiles.force(this.path.getParent());
        } catch(IOException|RuntimeException|Error failure) { if(acquired!=null) try { acquired.release(); } catch(IOException cleanup) { failure.addSuppressed(cleanup); } try { channel.close(); } catch(IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    Request request() throws IOException { requireOpen(); return request; }
    void begin(RemoteMutationToken token,long column,boolean merge,RangeDirectory directory) throws IOException { requireOpen(); request=new Request(token.requestId(),merge,directory); request.token=token; request.column=column; persist(); }
    boolean retainedInput(long ordinal,byte[] digest)throws IOException {
        requireOpen();if(ordinal<0)throw new IOException("Negative input ordinal");if(ordinal<=request.retiredInputThrough)throw new RatisBulkInputExpiredException(ordinal,request.retiredInputThrough);
        var original=request.inputDigests.get(ordinal);if(original==null)return false;if(!Arrays.equals(original,digest))throw new IOException("Copied ordinal input changed");return true;
    }
    boolean unresolvedInput(){return !request.chunks.isEmpty() || request.pending!=null || request.failure!=null;}
    void remoteInput(long ordinal,byte[] digest,List<DistributedCommand> chunks)throws IOException {
        requireOpen();if(digest==null || digest.length!=32)throw new IOException("Invalid input digest");if(retainedInput(ordinal,digest))return;
        if(ordinal!=request.copiedBatches || unresolvedInput() || request.eof)throw new IOException("Input ordinal unavailable or previous input unresolved");
        long next=Math.incrementExact(request.copiedBatches);var frozen=List.copyOf(chunks);
        request.inputDigests.put(ordinal,digest.clone());request.chunks=frozen;request.cursor=0;request.copiedBatches=next;
        if(chunks.isEmpty())trimCompleted();persist();
    }
    private void trimCompleted(){
        if(unresolvedInput())return;
        long floor=Math.max(-1,request.copiedBatches-COMPLETED_WINDOW-1);
        while(!request.inputDigests.isEmpty() && request.inputDigests.keySet().iterator().next()<=floor)request.inputDigests.remove(request.inputDigests.keySet().iterator().next());
        request.retiredInputThrough=floor;
    }
    static CommandResult.Status terminalOutcome(Request request){if(request.failure!=null)return request.failure.status();if(!request.complete())throw new IllegalArgumentException("Bulk outcome is not terminal");return CommandResult.Status.SUCCESS;}
    static byte[] resultDigest(Request request) { var out=java.nio.ByteBuffer.allocate(33).putLong(request.copiedBatches).putLong(request.acknowledgedChunks).putLong(request.acknowledgedMutations).putLong(request.eof?1:0).put((byte)(request.failure==null?0:request.failure.status().ordinal()+1)); return RemoteMutationToken.digest(out.array()); }
    void eof() throws IOException { requireOpen(); request.eof=true; persist(); }
    void bind(OperationId id) throws IOException { requireOpen(); if(request.pending!=null || request.cursor>=request.chunks.size()) throw new IOException("Batch identity binding changed"); request.pending=id; persist(); }
    void result(CommandResult result) throws IOException { requireOpen(); if(request.pending==null || request.result!=null) throw new IOException("Batch result lacks original identity"); request.result=result; persist(); }
    void acknowledged() throws IOException {
        requireOpen(); if(request.pending==null || request.result==null) throw new IOException("Batch ACK lacks forced result");
        var result=request.result; request.acknowledgedChunks=Math.incrementExact(request.acknowledgedChunks);
        if(result.status()==CommandResult.Status.SUCCESS) request.acknowledgedMutations=Math.addExact(request.acknowledgedMutations,request.chunks.get(request.cursor).mutations().size());
        if(result.status()!=CommandResult.Status.SUCCESS) request.failure=result;
        request.pending=null; request.result=null; request.cursor++; if(request.cursor==request.chunks.size()) { request.chunks=List.of(); request.cursor=0;trimCompleted(); } persist();
    }
    private void requireOpen() throws IOException { if(closed || fenced) throw new IOException("Batch journal unavailable; reopen exact retained input"); }
    private void persist() throws IOException { try { SessionJournalFiles.write(path,encode(),MAX_BYTES); } catch(IOException|RuntimeException failure) { fenced=true; if(failure instanceof IOException io) throw io; throw new IOException("Batch journal force failed",failure); } }
    private byte[] encode() throws IOException {
        validate(request); var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(0x52424154); out.write(binding); uuid(out,request.id); out.writeBoolean(request.merge); blob(out,RangeDirectoryCodec.encode(request.directory));
        out.writeLong(request.retiredInputThrough);out.writeLong(request.copiedBatches); out.writeLong(request.acknowledgedChunks); out.writeLong(request.acknowledgedMutations); out.writeBoolean(request.eof); out.writeInt(request.cursor); out.writeInt(request.chunks.size());
        for(var chunk:request.chunks) blob(out,DistributedCommandCodec.encode(chunk)); out.writeBoolean(request.pending!=null);
        if(request.pending!=null) { uuid(out,request.pending.sessionId()); out.writeLong(request.pending.epoch()); out.writeLong(request.pending.sequence()); }
        out.writeBoolean(request.result!=null); if(request.result!=null) blob(out,request.result.encode()); out.writeBoolean(request.failure!=null); if(request.failure!=null) blob(out,request.failure.encode()); out.write(request.token.encode()); out.writeLong(request.column); out.writeInt(request.inputDigests.size()); for(var entry:request.inputDigests.entrySet()) { out.writeLong(entry.getKey()); out.write(entry.getValue()); } out.flush(); if(bytes.size()+32>MAX_BYTES) throw new IOException("Batch journal exceeds bound"); return bytes.toByteArray();
    }
    private Request decode(byte[] bytes) throws IOException {
        try {
            var in=new DataInputStream(new ByteArrayInputStream(bytes)); if(in.readInt()!=0x52424154 || !Arrays.equals(binding,in.readNBytes(32))) throw new IOException("Batch journal identity changed");
            var result=new Request(uuid(in),in.readBoolean(),RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES)));
            result.retiredInputThrough=in.readLong();result.copiedBatches=in.readLong(); result.acknowledgedChunks=in.readLong(); result.acknowledgedMutations=in.readLong(); result.eof=in.readBoolean(); result.cursor=in.readInt(); int count=in.readInt();
            if(count<0 || count>RatisRocksDBBatches.MAX_INPUT_ITEMS || 4L*count>in.available()) throw new IOException("Invalid retained batch chunk count"); var chunks=new ArrayList<DistributedCommand>(count);
            for(int i=0;i<count;i++) chunks.add(DistributedCommandCodec.decode(blob(in,RatisRocksDBBatches.MAX_CHUNK_BYTES))); result.chunks=List.copyOf(chunks);
            if(in.readBoolean()) result.pending=new OperationId(uuid(in),in.readLong(),in.readLong()); if(in.readBoolean()) result.result=CommandResult.decode(blob(in,CommandResult.MAX_BYTES)); if(in.readBoolean()) result.failure=CommandResult.decode(blob(in,CommandResult.MAX_BYTES));
            result.token=RemoteMutationToken.decode(in.readNBytes(RemoteMutationToken.BYTES)); result.column=in.readLong(); int receipts=in.readInt(); if(receipts<0 || receipts>COMPLETED_WINDOW+1)throw new IOException("Invalid copied ordinal receipts"); for(int n=0;n<receipts;n++) { long ordinal=in.readLong();byte[] digest=in.readNBytes(32);if(ordinal!=result.retiredInputThrough+1+n || digest.length!=32 || result.inputDigests.put(ordinal,digest)!=null)throw new IOException("Invalid ordinal receipt"); } if(in.available()!=0) throw new IOException("Trailing batch journal input"); validate(result); return result;
        } catch(RuntimeException invalid) { throw new IOException("Invalid batch journal",invalid); }
    }
    private static void validate(Request request) throws IOException {
        if(request==null || request.token==null || request.token.kind()!=RemoteMutationToken.Kind.BULK || !request.token.requestId().equals(request.id) || request.copiedBatches<0 || request.acknowledgedChunks<0 || request.acknowledgedMutations<0 || request.cursor<0 || request.cursor>=Math.max(1,request.chunks.size()) || request.result!=null && request.pending==null || request.pending!=null && request.chunks.isEmpty()) throw new IOException("Invalid retained batch progress");
        boolean unresolved=!request.chunks.isEmpty() || request.pending!=null || request.failure!=null;
        long completed=request.copiedBatches-(unresolved?1:0);long expectedFloor=Math.max(-1,completed-COMPLETED_WINDOW-1);
        if(unresolved && request.copiedBatches==0 || request.retiredInputThrough!=expectedFloor || request.inputDigests.size()!=request.copiedBatches-request.retiredInputThrough-1 || request.inputDigests.size()>COMPLETED_WINDOW+1)throw new IOException("Invalid completed input floor/window");
        long ordinal=request.retiredInputThrough+1;for(var entry:request.inputDigests.entrySet())if(entry.getKey()!=ordinal++ || entry.getValue()==null || entry.getValue().length!=32)throw new IOException("Noncontiguous input digest window");
        long bytes=0; int items=0;
        for(var chunk:request.chunks) {
            bytes+=DistributedCommandCodec.encodedSize(chunk); items+=chunk.mutations().size();
            if(chunk.kind()!=DistributedCommand.Kind.DATA || !chunk.predicates().isEmpty() || chunk.mutations().isEmpty() || chunk.mutations().size()>RatisRocksDBBatches.MAX_CHUNK_ITEMS || DistributedCommandCodec.encodedSize(chunk)>RatisRocksDBBatches.MAX_CHUNK_BYTES || !Arrays.equals(chunk.schemaDigest(),request.directory.schemaDigest())) throw new IOException("Invalid retained native batch chunk");
            for(var mutation:chunk.mutations()) {
                boolean kind=request.merge ? mutation.kind()==Mutation.Kind.MERGE || mutation.kind()==Mutation.Kind.BUCKET_MERGE : mutation.kind()==Mutation.Kind.PUT || mutation.kind()==Mutation.Kind.BUCKET_PUT;
                var route=request.directory.route(mutation.columnId(),mutation.key()); if(!kind || mutation.returnMode()!=Mutation.ReturnMode.NONE || !route.groupId().equals(chunk.groupId()) || route.rangeEpoch()!=chunk.rangeEpoch()) throw new IOException("Retained chunk effect/route changed");
            }
        }
        if(items>RatisRocksDBBatches.MAX_INPUT_ITEMS || bytes>RatisRocksDBBatches.MAX_INPUT_BYTES) throw new IOException("Retained copied batch exceeds bound");
        if(request.failure!=null && (request.failure.status()==CommandResult.Status.SUCCESS || request.failure.valueCount()!=0)) throw new IOException("Invalid terminal batch rejection");
        if(request.result!=null && request.result.valueCount()!=0) throw new IOException("Batch receipt unexpectedly returns values");
    }
    private static void blob(DataOutputStream out,byte[] bytes) throws IOException { out.writeInt(bytes.length); out.write(bytes); }
    private static byte[] blob(DataInputStream in,int max) throws IOException { int size=in.readInt(); if(size<0 || size>max || size>in.available()) throw new IOException("Batch journal field exceeds bound"); return in.readNBytes(size); }
    private static void uuid(DataOutputStream out,UUID id) throws IOException { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(),in.readLong()); }
    @Override public void close() throws IOException { if(closed) return; IOException failure=null; try { lock.release(); } catch(IOException ex) { failure=ex; } try { channel.close(); } catch(IOException ex) { if(failure==null) failure=ex; else failure.addSuppressed(ex); } if(failure!=null) throw failure; closed=true; }
}
