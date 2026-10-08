package it.cavallium.rockserver.core.common.distributed;

import it.cavallium.rockserver.core.common.ColumnSchema;
import java.nio.ByteBuffer;
import java.util.*;

/** Immutable collision bucket using the existing standalone insertion-ordered byte format. */
public final class DistributedBucket {
    public static final int MAX_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES-4, MAX_MEMBERS=65536;
    /** Policy refusal before any native mutation; it does not certify stored bytes as well formed. */
    public static final class LimitExceededException extends IllegalArgumentException {
        public LimitExceededException(String message) { super(message); }
    }
    public record Member(List<byte[]> tails,byte[] value) {
        public Member {
            memberSize(tails,value); tails=copyTails(tails); value=value==null ? new byte[0] : value.clone();
        }
        @Override public List<byte[]> tails() { return copyTails(tails); }
        @Override public byte[] value() { return value.clone(); }
        @Override public boolean equals(Object other) { return other instanceof Member member && same(tails,member.tails) && Arrays.equals(value,member.value); }
        @Override public int hashCode() { return 31*tailHash(tails)+Arrays.hashCode(value); }
    }
    private final int tailCount;
    private final boolean hasValue;
    private final List<Member> members;
    private final int encodedBytes;

    private DistributedBucket(int tailCount,boolean hasValue,List<Member> members) {
        if (members.size()>MAX_MEMBERS) throw new LimitExceededException("Bucket member count exceeds bound");
        long size=members.isEmpty() ? 0 : 4;
        for (var member:members) size+=4L+memberSize(member.tails,member.value);
        if (size>MAX_BYTES) throw new LimitExceededException("Bucket exceeds byte budget");
        this.tailCount=tailCount; this.hasValue=hasValue; this.members=List.copyOf(members); encodedBytes=(int)size;
    }
    public static DistributedBucket empty(ColumnSchema schema) {
        int tails=tailCount(schema); return new DistributedBucket(tails,schema.hasValue(),List.of());
    }
    public static DistributedBucket decode(ColumnSchema schema,byte[] encoded) {
        var empty=empty(schema);
        if (encoded==null || encoded.length==0) return empty;
        if (encoded.length>MAX_BYTES) throw new LimitExceededException("Stored bucket exceeds byte budget");
        if (encoded.length<4) throw new IllegalArgumentException("Invalid stored bucket byte count");
        var input=ByteBuffer.wrap(encoded); int count=input.getInt();
        if (count<0 || (long)count*(4L+2L*empty.tailCount)>input.remaining()) {
            throw new IllegalArgumentException("Invalid stored bucket member count");
        }
        if (count>MAX_MEMBERS) throw new LimitExceededException("Stored bucket member count exceeds bound");
        var members=new ArrayList<Member>(count); var unique=new HashSet<TailKey>();
        for (int i=0;i<count;i++) {
            if (input.remaining()<4) throw new IllegalArgumentException("Truncated stored bucket member");
            int length=input.getInt(), start=input.position();
            if (length<2L*empty.tailCount || length>input.remaining()) throw new IllegalArgumentException("Invalid stored bucket member length");
            int end=start+length;
            // Validate every component and key-only remainder before allocating any member payload.
            for (int k=0;k<empty.tailCount;k++) {
                if (end-input.position()<2) throw new IllegalArgumentException("Truncated stored bucket tail length");
                int tail=input.getChar();
                if (tail>end-input.position()) throw new IllegalArgumentException("Truncated stored bucket tail bytes");
                input.position(input.position()+tail);
            }
            int valueStart=input.position();
            if (!empty.hasValue && valueStart!=end) throw new IllegalArgumentException("Key-only bucket contains value bytes");
            var keys=new ArrayList<byte[]>(empty.tailCount); var memberInput=input.duplicate(); memberInput.position(start);
            for (int k=0;k<empty.tailCount;k++) { int size=memberInput.getChar(); byte[] key=new byte[size]; memberInput.get(key); keys.add(key); }
            var member=new Member(keys,Arrays.copyOfRange(encoded,valueStart,end));
            if (!unique.add(new TailKey(member.tails))) throw new IllegalArgumentException("Duplicate stored bucket member keys");
            members.add(member); input.position(end);
        }
        if (input.hasRemaining()) throw new IllegalArgumentException("Trailing stored bucket bytes");
        return count==0 ? empty : new DistributedBucket(empty.tailCount,empty.hasValue,members);
    }
    public byte[] encode() {
        if (members.isEmpty()) return new byte[0];
        var output=ByteBuffer.allocate(encodedBytes).putInt(members.size());
        for (var member:members) {
            output.putInt((int)memberSize(member.tails,member.value));
            for (byte[] tail:member.tails) output.putChar((char)tail.length).put(tail);
            output.put(member.value);
        }
        return output.array();
    }
    public List<Member> members() { return members; }
    /** Null means absent; an empty value also represents presence for key-only schemas. */
    public byte[] get(List<byte[]> tails) {
        validateTails(tails); int index=indexOf(tails); return index<0 ? null : members.get(index).value();
    }
    public DistributedBucket put(List<byte[]> tails,byte[] value) {
        validateTails(tails);
        if (!hasValue && value!=null && value.length!=0) throw new IllegalArgumentException("Key-only bucket cannot contain a value");
        long memberBytes=memberSize(tails,value); int index=indexOf(tails);
        if (index<0 && members.size()==MAX_MEMBERS) throw new LimitExceededException("Bucket member count exceeds bound");
        long size=(members.isEmpty() ? 4 : encodedBytes)+(index<0 ? 4L+memberBytes : memberBytes-memberSize(members.get(index).tails,members.get(index).value));
        if (size>MAX_BYTES) throw new LimitExceededException("Bucket exceeds byte budget");
        var member=new Member(tails,value);
        if (index>=0 && member.equals(members.get(index))) return this;
        var next=new ArrayList<>(members); if (index<0) next.add(member); else next.set(index,member);
        return new DistributedBucket(tailCount,hasValue,next);
    }
    public DistributedBucket remove(List<byte[]> tails) {
        validateTails(tails); int index=indexOf(tails); if (index<0) return this;
        var next=new ArrayList<>(members); next.remove(index); return new DistributedBucket(tailCount,hasValue,next);
    }
    private int indexOf(List<byte[]> tails) {
        for (int i=0;i<members.size();i++) if (same(members.get(i).tails,tails)) return i; return -1;
    }
    private void validateTails(List<byte[]> tails) {
        if (tails==null || tails.size()!=tailCount) throw new IllegalArgumentException("Bucket variable-tail count differs from schema");
        memberSize(tails,null);
    }
    private static int tailCount(ColumnSchema schema) {
        if (schema==null || schema.variableLengthKeysCount()<1 || schema.variableLengthKeysCount()>schema.keysCount()
                || schema.variableLengthKeysCount()>DistributedSchemaCodec.MAX_KEY_COMPONENTS) throw new IllegalArgumentException("Bucket requires a bounded variable-tail schema");
        return schema.variableLengthKeysCount();
    }
    private static long memberSize(List<byte[]> tails,byte[] value) {
        if (tails==null || tails.isEmpty()) throw new IllegalArgumentException("Invalid bucket tail count");
        if (tails.size()>DistributedSchemaCodec.MAX_KEY_COMPONENTS) throw new LimitExceededException("Bucket tail count exceeds bound");
        long size=value==null ? 0 : value.length;
        for (byte[] tail:tails) {
            if (tail==null) throw new IllegalArgumentException("Null bucket tail");
            if (tail.length>65535) throw new LimitExceededException("Bucket tail exceeds unsigned-short bound");
            size+=2L+tail.length;
            if (size>MAX_BYTES-8) throw new LimitExceededException("Bucket member exceeds byte budget");
        }
        return size;
    }
    private static List<byte[]> copyTails(List<byte[]> tails) { return tails.stream().map(byte[]::clone).toList(); }
    private static boolean same(List<byte[]> first,List<byte[]> second) {
        if (first.size()!=second.size()) return false;
        for (int i=0;i<first.size();i++) if (!Arrays.equals(first.get(i),second.get(i))) return false; return true;
    }
    private static int tailHash(List<byte[]> tails) { int hash=1; for (byte[] tail:tails) hash=31*hash+Arrays.hashCode(tail); return hash; }
    private static final class TailKey implements Comparable<TailKey> {
        private final List<byte[]> tails; private final int hash;
        TailKey(List<byte[]> tails) { this.tails=tails; hash=tailHash(tails); }
        @Override public int hashCode() { return hash; }
        @Override public int compareTo(TailKey other) {
            for (int i=0;i<Math.min(tails.size(),other.tails.size());i++) {
                int order=Arrays.compareUnsigned(tails.get(i),other.tails.get(i)); if (order!=0) return order;
            }
            return Integer.compare(tails.size(),other.tails.size());
        }
        @Override public boolean equals(Object other) { return other instanceof TailKey key && same(tails,key.tails); }
    }
}
