package it.cavallium.rockserver.core.common.distributed;

import java.nio.*;
import java.util.*;

/** Immutable selected-member operation, distinct from stored insertion-ordered bucket bytes. */
public final class BucketMergeOperand {
    public static final int FORMAT_ID=0x424d4f50;
    public static final int MAX_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES;
    public enum Type { PUT, DELETE, MERGE }
    private final Type type;
    private final List<byte[]> memberKeys;
    private final byte[] value;
    public BucketMergeOperand(Type type,List<byte[]> memberKeys,byte[] value) {
        this.type=Objects.requireNonNull(type); int members=Mutation.memberBytes(memberKeys);
        if(type==Type.DELETE ? value!=null : value==null) throw new IllegalArgumentException("Only member DELETE has null payload");
        if(9L+members+(value==null ? 0 : value.length)>MAX_BYTES) throw new IllegalArgumentException("Bucket merge operand exceeds byte budget");
        this.memberKeys=Mutation.copyMembers(memberKeys); this.value=value==null ? null : value.clone();
    }
    public Type type() { return type; }
    public List<byte[]> memberKeys() { return Mutation.copyMembers(memberKeys); }
    public byte[] value() { return value==null ? null : value.clone(); }
    public byte[] encode() {
        var out=ByteBuffer.allocate(9+Mutation.memberBytes(memberKeys)+(value==null ? 0 : value.length)).putInt(FORMAT_ID).put((byte)(type.ordinal()+1)).putInt(memberKeys.size());
        for(var member:memberKeys) out.putChar((char)member.length).put(member);
        out.putInt(value==null ? -1 : value.length); if(value!=null) out.put(value); return out.array();
    }
    public static byte[] encode(Type type,List<byte[]> members,byte[] value) { return new BucketMergeOperand(type,members,value).encode(); }
    public static BucketMergeOperand decode(byte[] bytes) {
        if(bytes==null || bytes.length<15 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid bucket merge operand size");
        try {
            var in=ByteBuffer.wrap(bytes); if(in.getInt()!=FORMAT_ID) throw new IllegalArgumentException("Invalid bucket merge operand tag");
            int tag=in.get()&255; if(tag<1 || tag>Type.values().length) throw new IllegalArgumentException("Invalid bucket merge operation"); var type=Type.values()[tag-1]; int count=in.getInt();
            if(count<1 || count>DistributedSchemaCodec.MAX_KEY_COMPONENTS || 2L*count+4>in.remaining()) throw new IllegalArgumentException("Invalid bucket merge member count");
            var scan=in.duplicate(); for(int i=0;i<count;i++) { int length=scan.getChar(); if(length>scan.remaining()-4) throw new IllegalArgumentException("Truncated bucket merge member"); scan.position(scan.position()+length); }
            int payload=scan.getInt(); if(type==Type.DELETE ? payload!=-1 || scan.hasRemaining() : payload<0 || payload!=scan.remaining()) throw new IllegalArgumentException("Invalid bucket merge payload");
            var members=new ArrayList<byte[]>(count); for(int i=0;i<count;i++) { byte[] member=new byte[in.getChar()]; in.get(member); members.add(member); } in.getInt(); byte[] value=payload<0 ? null : new byte[payload]; if(value!=null) in.get(value);
            var result=new BucketMergeOperand(type,members,value); if(!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical bucket merge operand"); return result;
        } catch(BufferUnderflowException malformed) { throw new IllegalArgumentException("Truncated bucket merge operand",malformed); }
    }
    @Override public boolean equals(Object other) { return other instanceof BucketMergeOperand operand && type==operand.type && Mutation.sameMembers(memberKeys,operand.memberKeys) && Arrays.equals(value,operand.value); }
    @Override public int hashCode() { return Objects.hash(type,Mutation.membersHash(memberKeys),Arrays.hashCode(value)); }
}
