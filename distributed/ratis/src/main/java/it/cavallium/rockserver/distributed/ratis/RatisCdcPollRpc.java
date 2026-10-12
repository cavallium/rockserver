package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.nio.ByteBuffer;
import java.util.*;

/** Current whole-envelope CDC poll, bound to namespace, lineage and original cursor. */
public final class RatisCdcPollRpc {
    public static final int KIND=81,REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+116;
    public static final int REPLY_BASE_BYTES=ScopedTransactionRpc.HEADER_BYTES+98,MAX_REPLY_BYTES=8*1024*1024,MAX_RECORDS=256;
    public enum Outcome { DATA, GAP, TOO_LARGE }
    private RatisCdcPollRpc() {}
    public record Binding(UUID clusterId,UUID databaseId,UUID domainId,DistributedCdcCheckpoint.Lineage lineage) {
        public Binding { Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(domainId); Objects.requireNonNull(lineage); }
    }
    public record Request(Binding binding,DistributedCdcCheckpoint.Cursor from,int maxRecords,int maxBytes) {
        public Request {
            Objects.requireNonNull(binding); Objects.requireNonNull(from);
            if(maxRecords<1 || maxRecords>MAX_RECORDS || maxBytes<=REPLY_BASE_BYTES || maxBytes>MAX_REPLY_BYTES) throw new IllegalArgumentException("Invalid CDC poll response budget");
        }
    }
    public record Result(Request request,Outcome outcome,ApplyPosition cut,DistributedCdcCheckpoint.Cursor floor,
            DistributedCdcCheckpoint.Cursor next,boolean fromVerified,int requiredBytes,List<DistributedCdcRecord> records) {
        public Result {
            Objects.requireNonNull(request); Objects.requireNonNull(outcome); Objects.requireNonNull(cut); Objects.requireNonNull(floor); Objects.requireNonNull(next); records=List.copyOf(records);
            if(cut.term()==0 || records.size()>request.maxRecords()) throw new IllegalArgumentException("Invalid CDC reply cut/count");
            var end=DistributedCdcCheckpoint.Cursor.after(cut);
            if(!end.covers(floor) || !end.covers(next) || !end.covers(request.from()) || !next.covers(request.from())) throw new IllegalArgumentException("CDC reply cursor exceeds cut or regresses");
            if(outcome==Outcome.GAP) {
                if(fromVerified || request.from().covers(floor) || !next.equals(request.from()) || requiredBytes!=0 || !records.isEmpty()) throw new IllegalArgumentException("Invalid CDC gap reply");
            } else {
                if(!request.from().covers(floor)) throw new IllegalArgumentException("CDC data/budget reply crosses a retention gap");
                if(outcome==Outcome.TOO_LARGE) {
                    if(!next.equals(request.from()) || requiredBytes<=request.maxBytes() || requiredBytes>REPLY_BASE_BYTES+4+DistributedCdcRecord.MAX_BYTES || !records.isEmpty()) throw new IllegalArgumentException("Invalid CDC required-budget reply");
                } else if(requiredBytes!=0) throw new IllegalArgumentException("CDC data reply has a required-budget field");
            }
            long bytes=REPLY_BASE_BYTES,previous=-1;
            for(var record:records) {
                if(!record.lineage().equals(request.binding().lineage()) || record.position().index()<=previous || !end.covers(record.end())
                        || request.from().covers(record.end()) || !next.covers(record.end())) throw new IllegalArgumentException("CDC reply record differs from lineage/cursor/cut");
                previous=record.position().index(); bytes+=4L+record.encodedSize();
            }
            if(outcome==Outcome.DATA && !next.equals(records.isEmpty() ? request.from() : records.getLast().end())) throw new IllegalArgumentException("CDC next cursor lacks a returned event boundary");
            if(bytes>request.maxBytes()) throw new IllegalArgumentException("CDC reply exceeds requested wire budget");
        }
    }
    public static byte[] request(Request request) {
        var binding=request.binding(); var lineage=binding.lineage(); var bytes=ByteBuffer.allocate(116);
        uuid(bytes,binding.clusterId()); uuid(bytes,binding.databaseId()); uuid(bytes,binding.domainId());
        uuid(bytes,lineage.rangeId()); uuid(bytes,lineage.groupId()); bytes.putLong(lineage.rangeEpoch()); cursor(bytes,request.from());
        bytes.putInt(request.maxRecords()).putInt(request.maxBytes()); return ScopedTransactionRpc.frame(KIND,bytes.array());
    }
    public static Request decodeRequest(byte[] encoded) {
        var packet=ScopedTransactionRpc.parse(encoded,REQUEST_BYTES);
        if(packet.kind()!=KIND || packet.body().length!=116) throw new IllegalArgumentException("Invalid CDC request shape");
        var bytes=ByteBuffer.wrap(packet.body()); var binding=new Binding(uuid(bytes),uuid(bytes),uuid(bytes),new DistributedCdcCheckpoint.Lineage(uuid(bytes),uuid(bytes),bytes.getLong()));
        return new Request(binding,cursor(bytes),bytes.getInt(),bytes.getInt());
    }
    public static byte[] reply(Request request,ReplicatedRangeStore.CdcPage page) {
        if(!request.binding().lineage().equals(page.lineage())) throw new IllegalArgumentException("CDC native page lineage differs");
        return reply(new Result(request,Outcome.DATA,page.cut(),page.floor(),page.next(),page.fromVerified(),0,page.records()));
    }
    public static byte[] reply(Result result) {
        int size=REPLY_BASE_BYTES-ScopedTransactionRpc.HEADER_BYTES;
        for(var record:result.records()) size+=4+record.encodedSize();
        var bytes=ByteBuffer.allocate(size).put(RangeImportAdmission.sha256(request(result.request())));
        bytes.put((byte)result.outcome().ordinal()).putLong(result.cut().term()).putLong(result.cut().index()); cursor(bytes,result.floor()); cursor(bytes,result.next()); bytes.put((byte)(result.fromVerified()?1:0)).putInt(result.requiredBytes()).putInt(result.records().size());
        for(var record:result.records()) { byte[] encoded=record.encode(); bytes.putInt(encoded.length).put(encoded); }
        return ScopedTransactionRpc.frame(KIND,bytes.array());
    }
    public static Result decodeReply(byte[] encoded,Request request) {
        var packet=ScopedTransactionRpc.parse(encoded,request.maxBytes());
        if(packet.kind()!=KIND || encoded.length<REPLY_BASE_BYTES) throw new IllegalArgumentException("Invalid CDC reply shape");
        var bytes=ByteBuffer.wrap(packet.body()); byte[] digest=new byte[32]; bytes.get(digest);
        if(!Arrays.equals(digest,RangeImportAdmission.sha256(request(request)))) throw new IllegalArgumentException("CDC reply crossed request binding");
        int tag=bytes.get()&255; if(tag>=Outcome.values().length) throw new IllegalArgumentException("Invalid CDC reply outcome"); var outcome=Outcome.values()[tag];
        var cut=new ApplyPosition(bytes.getLong(),bytes.getLong()); var floor=cursor(bytes); var next=cursor(bytes); int verified=bytes.get()&255;
        if(verified>1) throw new IllegalArgumentException("Invalid CDC cursor witness"); int required=bytes.getInt(),count=bytes.getInt();
        if(count<0 || count>request.maxRecords() || count>bytes.remaining()/(4+DistributedCdcRecord.MIN_BYTES)) throw new IllegalArgumentException("Invalid CDC reply record count");
        var records=new ArrayList<DistributedCdcRecord>(count);
        for(int i=0;i<count;i++) {
            if(bytes.remaining()<4) throw new IllegalArgumentException("Truncated CDC record size"); int size=bytes.getInt();
            if(size<DistributedCdcRecord.MIN_BYTES || size>DistributedCdcRecord.MAX_BYTES || size>bytes.remaining()) throw new IllegalArgumentException("Invalid CDC record length");
            byte[] record=new byte[size]; bytes.get(record); records.add(DistributedCdcRecord.decode(record));
        }
        if(bytes.hasRemaining()) throw new IllegalArgumentException("Trailing CDC reply bytes"); return new Result(request,outcome,cut,floor,next,verified==1,required,records);
    }
    private static void cursor(ByteBuffer bytes,DistributedCdcCheckpoint.Cursor cursor) { bytes.putLong(cursor.position().term()).putLong(cursor.position().index()).putInt(cursor.mutationOrdinal()); }
    private static DistributedCdcCheckpoint.Cursor cursor(ByteBuffer bytes) { return new DistributedCdcCheckpoint.Cursor(new ApplyPosition(bytes.getLong(),bytes.getLong()),bytes.getInt()); }
    private static void uuid(ByteBuffer bytes,UUID id) { bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer bytes) { return new UUID(bytes.getLong(),bytes.getLong()); }
}
