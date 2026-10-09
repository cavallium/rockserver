package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.*;
import java.time.Duration;
import java.util.*;

/** Portable bounded requests and replies for node-owned immutable export artifacts. */
public final class RatisExportSourceRpc {
    public static final int MAX_READ_BYTES=1024*1024, MAX_BYTES=ScopedTransactionRpc.MAX_REPLY_BYTES-ScopedTransactionRpc.HEADER_BYTES;
    private RatisExportSourceRpc() {}
    public record JobDefinition(UUID jobId,SnapshotFence fence,UUID group,UUID column,SnapshotFenceStatus.Capture capture,RatisExportJournal.Limits limits) {
        public JobDefinition {
            Objects.requireNonNull(jobId); Objects.requireNonNull(fence); Objects.requireNonNull(group); Objects.requireNonNull(column); Objects.requireNonNull(capture); Objects.requireNonNull(limits);
            var projections=new ArrayList<TransactionScope.Span>(); for (var p:fence.participants()) projections.add(RatisExportSourceRpc.projection(p,column));
            projections.sort(Comparator.comparing(TransactionScope.Span::lowerInclusive,RatisExportSourceRpc::lowerOrder));
            for(int i=1;i<projections.size();i++) if(projections.get(i-1).upperExclusive()==null || projections.get(i).lowerInclusive()==null || !Arrays.equals(projections.get(i-1).upperExclusive(),projections.get(i).lowerInclusive())) throw new IllegalArgumentException("Gapped export projection");
            fence.participant(group);
        }
        public TransactionScope.Span scope() { return RatisExportSourceRpc.projection(fence.participant(group),column); }
        public byte[] encode() { return pack(out->{ uuid(out,jobId); blob(out,fence.encode()); uuid(out,group); uuid(out,column); uuid(out,capture.ownerIncarnation()); uuid(out,capture.snapshotId()); position(out,capture.position()); out.writeInt(limits.maximumChunks()); out.writeLong(limits.maximumJournalBytes()); out.writeLong(limits.maximumDiskBytes()); RatisExportSourceRpc.limits(out,limits.chunk()); }); }
        public static JobDefinition decode(byte[] bytes) { return unpack(bytes,in->new JobDefinition(uuid(in),SnapshotFence.decode(blob(in,SnapshotFence.MAX_BYTES)),uuid(in),uuid(in),new SnapshotFenceStatus.Capture(uuid(in),uuid(in),position(in)),new RatisExportJournal.Limits(in.readInt(),in.readLong(),in.readLong(),RatisExportSourceRpc.limits(in)))); }
        public byte[] digest() { return hash(encode()); }
    }
    public record BuildRequest(JobDefinition definition,RatisExportJournal.ChunkIntent intent) {
        public BuildRequest { Objects.requireNonNull(definition); Objects.requireNonNull(intent); if(!intent.group().equals(definition.group()) || !intent.column().equals(definition.column()) || !intent.cut().equals(definition.capture().position()) || !intent.limits().equals(definition.limits().chunk()) || intent.ordinal()>=definition.limits().maximumChunks()) throw new IllegalArgumentException("Export intent changed binding"); }
        public byte[] encode() { return pack(out->{blob(out,definition.encode()); RatisExportSourceRpc.intent(out,intent);}); }
        public static BuildRequest decode(byte[] bytes) { return unpack(bytes,in->new BuildRequest(JobDefinition.decode(blob(in,MAX_BYTES)),RatisExportSourceRpc.intent(in))); }
    }
    public record ReadRequest(JobDefinition definition,long ordinal,byte[] descriptorDigest,long offset,int maxBytes) {
        public ReadRequest { Objects.requireNonNull(definition); descriptorDigest=digestBytes(descriptorDigest); if(ordinal<0 || offset<0 || maxBytes<1 || maxBytes>MAX_READ_BYTES) throw new IllegalArgumentException("Invalid export read bounds"); }
        @Override public byte[] descriptorDigest() { return descriptorDigest.clone(); }
        public byte[] encode() { return pack(out->{blob(out,definition.encode());out.writeLong(ordinal);out.write(descriptorDigest);out.writeLong(offset);out.writeInt(maxBytes);}); }
        public static ReadRequest decode(byte[] bytes) { return unpack(bytes,in->new ReadRequest(JobDefinition.decode(blob(in,MAX_BYTES)),in.readLong(),fixed(in),in.readLong(),in.readInt())); }
    }
    public record ReleaseRequest(JobDefinition definition) {
        public ReleaseRequest { Objects.requireNonNull(definition); }
        public byte[] encode() { return definition.encode(); }
        public static ReleaseRequest decode(byte[] bytes) { return new ReleaseRequest(JobDefinition.decode(bytes)); }
    }
    public record Descriptor(byte[] bindingDigest,long ordinal,UUID group,long rangeEpoch,byte[] schemaDigest,UUID column,ApplyPosition cut,byte[] lowerInclusive,byte[] upperExclusive,byte[] contentHash,long rows,long payloadBytes,long fileBytes,boolean hasMore) {
        public Descriptor { bindingDigest=digestBytes(bindingDigest); schemaDigest=digestBytes(schemaDigest);contentHash=digestBytes(contentHash);Objects.requireNonNull(group);Objects.requireNonNull(column);Objects.requireNonNull(cut);lowerInclusive=copy(lowerInclusive);upperExclusive=copy(upperExclusive); if((long)(lowerInclusive==null?0:lowerInclusive.length)+(upperExclusive==null?0:upperExclusive.length)>TransactionScope.MAX_BOUND_BYTES || ordinal<0 || rangeEpoch<0 || rows<0 || payloadBytes<0 || fileBytes<0 || rows==0 && (payloadBytes!=0 || fileBytes!=0 || hasMore) || rows>0 && fileBytes==0 || hasMore && upperExclusive==null || lowerInclusive!=null && upperExclusive!=null && Arrays.compareUnsigned(lowerInclusive,upperExclusive)>=0) throw new IllegalArgumentException("Invalid export descriptor"); }
        @Override public byte[] bindingDigest(){return bindingDigest.clone();} @Override public byte[] schemaDigest(){return schemaDigest.clone();} @Override public byte[] contentHash(){return contentHash.clone();} @Override public byte[] lowerInclusive(){return copy(lowerInclusive);} @Override public byte[] upperExclusive(){return copy(upperExclusive);}
        public byte[] encode(){return pack(out->{out.write(bindingDigest);out.writeLong(ordinal);uuid(out,group);out.writeLong(rangeEpoch);out.write(schemaDigest);uuid(out,column);position(out,cut);blob(out,lowerInclusive);blob(out,upperExclusive);out.write(contentHash);out.writeLong(rows);out.writeLong(payloadBytes);out.writeLong(fileBytes);out.writeBoolean(hasMore);});}
        public static Descriptor decode(byte[] bytes){return unpack(bytes,in->new Descriptor(fixed(in),in.readLong(),uuid(in),in.readLong(),fixed(in),uuid(in),position(in),blob(in,TransactionScope.MAX_BOUND_BYTES),blob(in,TransactionScope.MAX_BOUND_BYTES),fixed(in),in.readLong(),in.readLong(),in.readLong(),bool(in)));}
        public byte[] digest(){return hash(encode());}
        public NativeRangeSstExport.Chunk nativeChunk(Path file){return new NativeRangeSstExport.Chunk(new NativeRangeSstExport.Manifest(group,rangeEpoch,column,schemaDigest,cut,lowerInclusive,upperExclusive,rows,payloadBytes,fileBytes,rows==0?new byte[0]:contentHash,rows==0?null:file),hasMore);}
    }
    public record BuildReply(BuildRequest request,Descriptor descriptor){public BuildReply{Objects.requireNonNull(request);if(descriptor!=null)requireDescriptor(request,descriptor);} public byte[] encode(){return pack(out->{blob(out,request.encode());out.writeBoolean(descriptor!=null);if(descriptor!=null)blob(out,descriptor.encode());});}public static BuildReply decode(byte[] bytes){return unpack(bytes,in->new BuildReply(BuildRequest.decode(blob(in,MAX_BYTES)),bool(in)?Descriptor.decode(blob(in,MAX_BYTES)):null));}}
    public record ReadReply(ReadRequest request,byte[] bytes){public ReadReply{Objects.requireNonNull(request);Objects.requireNonNull(bytes);if(bytes.length>request.maxBytes())throw new IllegalArgumentException("Oversized export reply");bytes=bytes.clone();}@Override public byte[] bytes(){return bytes.clone();}public byte[] encode(){return pack(out->{blob(out,request.encode());blob(out,bytes);});}public static ReadReply decode(byte[] bytes){return unpack(bytes,in->new ReadReply(ReadRequest.decode(blob(in,MAX_BYTES)),blob(in,MAX_READ_BYTES)));}}
    public record ReleaseReply(ReleaseRequest request){public ReleaseReply{Objects.requireNonNull(request);}public byte[] encode(){return request.encode();}public static ReleaseReply decode(byte[] bytes){return new ReleaseReply(ReleaseRequest.decode(bytes));}}
    public static void requireDescriptor(BuildRequest r,Descriptor d) {
        var p=r.definition().fence().participant(r.definition().group());byte[] finalUpper=r.definition().scope().upperExclusive();
        if(d.rows()==0 && !Arrays.equals(d.contentHash(),hash(new byte[0])) || !Arrays.equals(d.bindingDigest(),r.definition().digest()) || d.ordinal()!=r.intent().ordinal() || !d.group().equals(r.intent().group()) || !d.column().equals(r.intent().column()) || !d.cut().equals(r.intent().cut()) || d.rangeEpoch()!=p.rangeEpoch() || !Arrays.equals(d.schemaDigest(),p.schemaDigest()) || !Arrays.equals(d.lowerInclusive(),r.intent().scopeStart()) || (d.hasMore()?d.upperExclusive()==null || finalUpper!=null && Arrays.compareUnsigned(d.upperExclusive(),finalUpper)>=0:!Arrays.equals(d.upperExclusive(),finalUpper)) || d.rows()>r.intent().limits().maximumRows() || d.payloadBytes()>r.intent().limits().maximumPayloadBytes() || d.fileBytes()>r.intent().limits().maximumFileBytes())throw new IllegalArgumentException("Descriptor changed original source/coverage/budget");
    }
    static TransactionScope.Span projection(SnapshotFence.Participant p,UUID column){var spans=p.scope().spans().stream().filter(s->s.columnId().equals(column)).sorted(Comparator.comparing(TransactionScope.Span::lowerInclusive,RatisExportSourceRpc::lowerOrder)).toList();if(spans.isEmpty() || spans.stream().anyMatch(TransactionScope.Span::isPoint))throw new IllegalArgumentException("Export requires contiguous ranges");for(int i=1;i<spans.size();i++)if(spans.get(i-1).upperExclusive()==null || spans.get(i).lowerInclusive()==null || !Arrays.equals(spans.get(i-1).upperExclusive(),spans.get(i).lowerInclusive()))throw new IllegalArgumentException("Gapped export scope");return TransactionScope.Span.range(column,spans.getFirst().lowerInclusive(),spans.getLast().upperExclusive());}
    private static int lowerOrder(byte[] a,byte[] b){return a==null?b==null?0:-1:b==null?1:Arrays.compareUnsigned(a,b);}
    static byte[] hash(byte[] b){try{return MessageDigest.getInstance("SHA-256").digest(b);}catch(NoSuchAlgorithmException e){throw new AssertionError(e);}}
    private static byte[] digestBytes(byte[] b){if(b==null || b.length!=32)throw new IllegalArgumentException("Invalid digest");return b.clone();}
    private static byte[] copy(byte[] b){if(b!=null && b.length>TransactionScope.MAX_BOUND_BYTES)throw new IllegalArgumentException("Oversized bound");return b==null?null:b.clone();}
    interface Writer{void write(DataOutputStream out)throws IOException;} interface Reader<T>{T read(DataInputStream in)throws IOException;}
    static byte[] pack(Writer writer){try{var bytes=new ByteArrayOutputStream();writer.write(new DataOutputStream(bytes));if(bytes.size()>MAX_BYTES)throw new IllegalArgumentException("Oversized export RPC");return bytes.toByteArray();}catch(IOException e){throw new UncheckedIOException(e);}}
    static <T>T unpack(byte[] bytes,Reader<T> reader){if(bytes==null || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Oversized export RPC");try{var in=new DataInputStream(new ByteArrayInputStream(bytes));T value=reader.read(in);if(in.available()!=0)throw new IllegalArgumentException("Trailing export RPC bytes");return value;}catch(IOException e){throw new IllegalArgumentException("Malformed export RPC",e);}}
    static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
    static void position(DataOutputStream out,ApplyPosition p)throws IOException{out.writeLong(p.term());out.writeLong(p.index());}static ApplyPosition position(DataInputStream in)throws IOException{return new ApplyPosition(in.readLong(),in.readLong());}
    static void blob(DataOutputStream out,byte[] b)throws IOException{out.writeInt(b==null?-1:b.length);if(b!=null)out.write(b);}static byte[] blob(DataInputStream in,int max)throws IOException{int n=in.readInt();if(n==-1)return null;if(n<0 || n>max || n>in.available())throw new IOException("Invalid blob");return in.readNBytes(n);}static byte[] fixed(DataInputStream in)throws IOException{byte[] b=new byte[32];in.readFully(b);return b;}
    static boolean bool(DataInputStream in)throws IOException{int v=in.readUnsignedByte();if(v>1)throw new IOException("Invalid boolean");return v==1;}
    static void limits(DataOutputStream out,NativeRangeSstExport.Limits l)throws IOException{out.writeLong(l.maximumRows());out.writeLong(l.maximumPayloadBytes());out.writeLong(l.maximumFileBytes());out.writeLong(l.maximumDuration().toNanos());}static NativeRangeSstExport.Limits limits(DataInputStream in)throws IOException{return new NativeRangeSstExport.Limits(in.readLong(),in.readLong(),in.readLong(),Duration.ofNanos(in.readLong()));}
    static void intent(DataOutputStream out,RatisExportJournal.ChunkIntent i)throws IOException{uuid(out,i.group());uuid(out,i.column());position(out,i.cut());blob(out,i.scopeStart());out.writeLong(i.ordinal());limits(out,i.limits());}static RatisExportJournal.ChunkIntent intent(DataInputStream in)throws IOException{return new RatisExportJournal.ChunkIntent(uuid(in),uuid(in),position(in),blob(in,TransactionScope.MAX_BOUND_BYTES),in.readLong(),limits(in));}
}
