package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Compact bounded references at one completed native cut; fetch a selected plan separately. */
public record RatisSnapshotBlockers(ScopedTransactionRpc.SnapshotBinding binding,UUID group,ApplyPosition installed,ApplyPosition position,List<Reference> records,boolean more) {
    public static final int MAX_ITEMS=16,HEADER_BYTES=101,MAX_BYTES=HEADER_BYTES+64*MAX_ITEMS;
    public record Reference(UUID transaction,byte[] digest,ApplyPosition prepared) {
        public Reference { Objects.requireNonNull(transaction); Objects.requireNonNull(prepared); if (digest.length!=32 || prepared.term()<=0 || prepared.index()<=0) throw new IllegalArgumentException("Invalid blocker reference"); digest=digest.clone(); }
        @Override public byte[] digest() { return digest.clone(); }
        @Override public boolean equals(Object other) { return other instanceof Reference r && transaction.equals(r.transaction) && prepared.equals(r.prepared) && Arrays.equals(digest,r.digest); }
        @Override public int hashCode() { return Objects.hash(transaction,prepared,Arrays.hashCode(digest)); }
    }
    public RatisSnapshotBlockers {
        Objects.requireNonNull(binding); Objects.requireNonNull(group); Objects.requireNonNull(installed); Objects.requireNonNull(position); records=List.copyOf(records);
        if (installed.term()<=0 || installed.index()<=0 || position.term()<installed.term() || position.index()<installed.index() || position.index()==installed.index() && position.term()!=installed.term()
                || records.size()>MAX_ITEMS || more && records.isEmpty()) throw new IllegalArgumentException("Invalid blocker page cut/bound");
        UUID previous=null;
        for (var record:records) { if (previous!=null && previous.compareTo(record.transaction())>=0 || record.prepared().term()>installed.term() || record.prepared().index()>=installed.index()) throw new IllegalArgumentException("Invalid blocker order/position"); previous=record.transaction(); }
    }
    public byte[] encode() {
        var out=ByteBuffer.allocate(HEADER_BYTES+records.size()*64); ScopedTransactionRpc.snapshotBinding(out,binding); ScopedTransactionRpc.uuid(out,group);
        out.putLong(installed.term()).putLong(installed.index()).putLong(position.term()).putLong(position.index()).putInt(records.size()).put((byte)(more ? 1 : 0));
        for (var r:records) { ScopedTransactionRpc.uuid(out,r.transaction()); out.put(r.digest()).putLong(r.prepared().term()).putLong(r.prepared().index()); } return out.array();
    }
    public static RatisSnapshotBlockers decode(byte[] bytes) {
        if (bytes.length<HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid blocker page length");
        var in=ByteBuffer.wrap(bytes); var binding=ScopedTransactionRpc.snapshotBinding(in); var group=ScopedTransactionRpc.uuid(in);
        var installed=new ApplyPosition(in.getLong(),in.getLong()); var position=new ApplyPosition(in.getLong(),in.getLong()); int count=in.getInt(),more=in.get() & 255;
        if (count<0 || count>MAX_ITEMS || more>1 || in.remaining()!=count*64) throw new IllegalArgumentException("Invalid blocker page count");
        var records=new ArrayList<Reference>(); for (int n=0;n<count;n++) { var tx=ScopedTransactionRpc.uuid(in); byte[] digest=new byte[32]; in.get(digest); records.add(new Reference(tx,digest,new ApplyPosition(in.getLong(),in.getLong()))); }
        return new RatisSnapshotBlockers(binding,group,installed,position,records,more==1);
    }
    public record Request(SnapshotFence fence,UUID after,int limit) {
        public Request { Objects.requireNonNull(fence); if (limit<1 || limit>MAX_ITEMS) throw new IllegalArgumentException("Invalid blocker page limit"); }
        public byte[] encode() { byte[] definition=fence.encode(); var out=ByteBuffer.allocate(definition.length+9+(after==null ? 0 : 16)); out.putInt(definition.length).put(definition).put((byte)(after==null ? 0 : 1)); if (after!=null) ScopedTransactionRpc.uuid(out,after); return out.putInt(limit).array(); }
        public static Request decode(byte[] bytes) {
            if (bytes.length<SnapshotFence.HEADER_BYTES+9 || bytes.length>SnapshotFence.MAX_BYTES+25) throw new IllegalArgumentException("Invalid blocker request length");
            var in=ByteBuffer.wrap(bytes); int length=in.getInt(); if (length<SnapshotFence.HEADER_BYTES || length>SnapshotFence.MAX_BYTES || length>in.remaining()-5) throw new IllegalArgumentException("Invalid blocker definition length");
            byte[] definition=new byte[length]; in.get(definition); int flag=in.get() & 255; if (flag>1 || in.remaining()!=(flag==0 ? 4 : 20)) throw new IllegalArgumentException("Invalid blocker cursor");
            UUID after=flag==0 ? null : ScopedTransactionRpc.uuid(in); return new Request(SnapshotFence.decode(definition),after,in.getInt());
        }
    }
}
