package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.util.*;

/** Immutable path-free published reference; request metadata precedes Ratis's assigned term/index. */
public record RatisReferencedFile(UUID groupId,long term,long index,UUID clientId,long callId,long length,byte[] sha256) {
    public static final int BYTES=100;
    public RatisReferencedFile { Objects.requireNonNull(groupId); Objects.requireNonNull(clientId); if(term<0 || index<0 || callId<0 || length<0 || sha256.length!=32) throw new IllegalArgumentException("Invalid referenced file identity/bounds"); sha256=sha256.clone(); }
    @Override public byte[] sha256() { return sha256.clone(); }
    public record Request(UUID groupId,UUID clientId,long callId,long length,byte[] sha256) {
        public static final int BYTES=84,MAGIC=0x52465251;
        public Request { Objects.requireNonNull(groupId); Objects.requireNonNull(clientId); if(callId<0 || length<0 || sha256.length!=32) throw new IllegalArgumentException("Invalid referenced request"); sha256=sha256.clone(); }
        @Override public byte[] sha256() { return sha256.clone(); }
        public static boolean isRequest(byte[] bytes) { return bytes!=null && bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
        public byte[] encode() { var out=ByteBuffer.allocate(BYTES).putInt(MAGIC); uuid(out,groupId); uuid(out,clientId); out.putLong(callId).putLong(length).put(sha256); return out.array(); }
        public static Request decode(byte[] bytes) { if(bytes.length!=BYTES) throw new IllegalArgumentException("Invalid referenced request size"); var in=ByteBuffer.wrap(bytes); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid referenced request tag"); UUID group=uuid(in),client=uuid(in); long call=in.getLong(),length=in.getLong(); byte[] sha=new byte[32]; in.get(sha); return new Request(group,client,call,length,sha); }
        @Override public boolean equals(Object other) { return other instanceof Request request && Arrays.equals(encode(),request.encode()); }
        @Override public int hashCode() { return Arrays.hashCode(encode()); }
    }
    public Request request() { return new Request(groupId,clientId,callId,length,sha256); }
    public byte[] encode() { var out=ByteBuffer.allocate(BYTES).putInt(0x52465246); uuid(out,groupId); out.putLong(term).putLong(index); uuid(out,clientId); out.putLong(callId).putLong(length).put(sha256); return out.array(); }
    public static RatisReferencedFile decode(byte[] bytes) { if(bytes.length!=BYTES) throw new IllegalArgumentException("Invalid reference size"); var in=ByteBuffer.wrap(bytes); if(in.getInt()!=0x52465246) throw new IllegalArgumentException("Invalid reference tag"); UUID group=uuid(in); long term=in.getLong(),index=in.getLong(); UUID client=uuid(in); long call=in.getLong(),length=in.getLong(); byte[] sha=new byte[32]; in.get(sha); return new RatisReferencedFile(group,term,index,client,call,length,sha); }
    private static void uuid(ByteBuffer out,UUID value) { out.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
    @Override public boolean equals(Object other) { return other instanceof RatisReferencedFile reference && Arrays.equals(encode(),reference.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
}
