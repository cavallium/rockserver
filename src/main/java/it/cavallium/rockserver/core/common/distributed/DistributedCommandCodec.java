package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Single current canonical command format, with explicit semantic kind and item tags. */
public final class DistributedCommandCodec {
    public static final int FORMAT_VERSION=1, MAX_ITEMS=2048, MAX_MUTATIONS=MAX_ITEMS, MAX_ENCODED_BYTES=2*1024*1024;
    public static final int HEADER_BYTES=109;
    private DistributedCommandCodec() {}
    public static int encodedSize(DistributedCommand command) {
        Objects.requireNonNull(command); int size=HEADER_BYTES;
        for (var mutation:command.mutations()) size+=mutation.encodedSize();
        for (var predicate:command.predicates()) size+=predicate.encodedSize();
        return size+switch(command.kind()) {
            case DATA -> 0;
            case SESSION_FLOOR -> 8;
            case SESSION_CONTROL -> 4+command.sessionControl().encodedSize();
            case COMMIT_ACCEPTED -> 4+command.acceptance().encodedSize();
            case COMMIT_REJECTED -> 32;
        };
    }
    public static byte[] encode(DistributedCommand command) {
        var output=ByteBuffer.allocate(encodedSize(command)).putInt(FORMAT_VERSION).put((byte)kind(command.kind()));
        uuid(output,command.groupId()); output.putLong(command.rangeEpoch()).put(command.schemaDigest());
        uuid(output,command.operationId().sessionId()); output.putLong(command.operationId().epoch()).putLong(command.operationId().sequence());
        output.putLong(command.acknowledgedThrough()).putInt(command.mutations().size()).putInt(command.predicates().size());
        for (var mutation:command.mutations()) {
            output.put((byte)switch(mutation.kind()) { case PUT -> 1; case DELETE -> 2; case MERGE -> 3; case DELETE_RANGE -> 4; case BUCKET_PUT -> 5; case BUCKET_DELETE -> 6; case BUCKET_MERGE -> 7; });
            output.put((byte)switch(mutation.returnMode()) { case NONE -> 0; case PREVIOUS -> 1; case CURRENT -> 2; });
            uuid(output,mutation.columnId()); bytes(output,mutation.key()); bytes(output,mutation.value());
            if (mutation.bucket()) members(output,mutation.memberKeys());
        }
        for (var predicate:command.predicates()) {
            output.put((byte)(predicate.bucket() ? 2 : 1)); uuid(output,predicate.columnId()); bytes(output,predicate.key()); bytes(output,predicate.expectedValue());
            if (predicate.bucket()) members(output,predicate.memberKeys());
        }
        switch(command.kind()) {
            case DATA -> {}
            case SESSION_FLOOR -> output.putLong(command.minimumSessionEpoch());
            case SESSION_CONTROL -> { output.putInt(command.sessionControl().encodedSize()); output.put(command.sessionControl().encode()); }
            case COMMIT_ACCEPTED -> { output.putInt(command.acceptance().encodedSize()); command.acceptance().encode(output); }
            case COMMIT_REJECTED -> output.put(command.rejectionDigest());
        }
        return output.array();
    }
    public static DistributedCommand decode(byte[] encoded) {
        Objects.requireNonNull(encoded);
        if (encoded.length<HEADER_BYTES || encoded.length>MAX_ENCODED_BYTES) throw new IllegalArgumentException("Invalid command byte length");
        var input=ByteBuffer.wrap(encoded);
        if (input.getInt()!=FORMAT_VERSION) throw new IllegalArgumentException("Unsupported command format");
        var kind=kind(input.get() & 255); UUID group=uuid(input); long epoch=input.getLong(); byte[] schema=new byte[32]; input.get(schema);
        var operation=new OperationId(uuid(input),input.getLong(),input.getLong()); long ack=input.getLong(); int count=input.getInt(), predicates=input.getInt();
        boolean data=kind==DistributedCommand.Kind.DATA || kind==DistributedCommand.Kind.COMMIT_ACCEPTED;
        if (epoch<0 || ack< -1 || ack>=operation.sequence() || kind==DistributedCommand.Kind.SESSION_FLOOR && ack!=-1
                || kind==DistributedCommand.Kind.DATA && count==0 && predicates==0 && ack<0
                || count<0 || predicates<0 || (long)count+predicates>MAX_ITEMS || !data && (count!=0 || predicates!=0)
                || 26L*count+25L*predicates>input.remaining()) throw new IllegalArgumentException("Invalid command kind/item counts");
        var mutations=new ArrayList<Mutation>(count);
        for (int i=0;i<count;i++) {
            require(input,26);
            var mutation=switch(input.get()) {
                case 1 -> Mutation.Kind.PUT; case 2 -> Mutation.Kind.DELETE; case 3 -> Mutation.Kind.MERGE;
                case 4 -> Mutation.Kind.DELETE_RANGE; case 5 -> Mutation.Kind.BUCKET_PUT; case 6 -> Mutation.Kind.BUCKET_DELETE; case 7 -> Mutation.Kind.BUCKET_MERGE;
                default -> throw new IllegalArgumentException("Unknown mutation kind");
            };
            var mode=switch(input.get()) { case 0 -> Mutation.ReturnMode.NONE; case 1 -> Mutation.ReturnMode.PREVIOUS; case 2 -> Mutation.ReturnMode.CURRENT; default -> throw new IllegalArgumentException("Unknown return mode"); };
            UUID column=uuid(input); int keyLength=input.getInt();
            if (keyLength<0 || keyLength>input.remaining()-4) throw new IllegalArgumentException("Invalid mutation key length");
            int valueLength=input.getInt(input.position()+keyLength), available=input.remaining()-keyLength-4;
            boolean deletion=mutation==Mutation.Kind.DELETE || mutation==Mutation.Kind.BUCKET_DELETE;
            if (deletion ? valueLength!=-1 : valueLength<0 || valueLength>available) throw new IllegalArgumentException("Invalid mutation value length");
            boolean bucket=mutation==Mutation.Kind.BUCKET_PUT || mutation==Mutation.Kind.BUCKET_DELETE || mutation==Mutation.Kind.BUCKET_MERGE;
            ByteBuffer tails=null; List<byte[]> memberKeys=List.of();
            if (bucket) { tails=input.duplicate(); tails.position(input.position()+keyLength+4+Math.max(0,valueLength)); memberKeys=members(tails); }
            byte[] key=new byte[keyLength]; input.get(key); input.getInt(); byte[] value=valueLength<0 ? null : new byte[valueLength]; if (value!=null) input.get(value);
            if (bucket) { input.position(tails.position()); mutations.add(switch(mutation) { case BUCKET_PUT -> Mutation.bucketPut(column,key,memberKeys,value,mode); case BUCKET_DELETE -> Mutation.bucketDelete(column,key,memberKeys,mode); case BUCKET_MERGE -> Mutation.bucketMerge(column,key,memberKeys,value,mode); default -> throw new IllegalStateException(); }); }
            else mutations.add(new Mutation(mutation,column,key,value,mode));
        }
        var conditions=new ArrayList<KeyPredicate>(predicates);
        for (int i=0;i<predicates;i++) {
            require(input,25); int tag=input.get() & 255; if (tag!=1 && tag!=2) throw new IllegalArgumentException("Unknown predicate kind");
            UUID column=uuid(input); int keyLength=input.getInt();
            if (keyLength<0 || keyLength>input.remaining()-4) throw new IllegalArgumentException("Invalid predicate key length");
            int expectedLength=input.getInt(input.position()+keyLength);
            if (expectedLength< -1 || expectedLength>input.remaining()-keyLength-4) throw new IllegalArgumentException("Invalid predicate value length");
            ByteBuffer tails=null; List<byte[]> memberKeys=List.of();
            if (tag==2) { tails=input.duplicate(); tails.position(input.position()+keyLength+4+Math.max(0,expectedLength)); memberKeys=members(tails); }
            byte[] key=new byte[keyLength]; input.get(key); input.getInt(); byte[] expected=expectedLength<0 ? null : new byte[expectedLength]; if (expected!=null) input.get(expected);
            if (tag==2) { input.position(tails.position()); conditions.add(KeyPredicate.bucketMember(column,key,memberKeys,expected)); }
            else conditions.add(new KeyPredicate(column,key,expected));
        }
        long floor=-1; RangeSessionControl control=null; TransactionAcceptance acceptance=null; byte[] rejected=null;
        switch(kind) {
            case DATA -> {}
            case SESSION_FLOOR -> { if (input.remaining()!=8) throw new IllegalArgumentException("Invalid session floor body"); floor=input.getLong(); }
            case SESSION_CONTROL -> {
                require(input,4); int length=input.getInt();
                if ((length!=RangeSessionControl.ADVANCE_BYTES && length!=RangeSessionControl.BIND_BYTES) || length!=input.remaining()) throw new IllegalArgumentException("Invalid session control body");
                byte[] body=new byte[length]; input.get(body); control=RangeSessionControl.decode(body); floor=control.authority().minimumEpoch();
            }
            case COMMIT_ACCEPTED -> {
                require(input,4); int length=input.getInt(); if (length<1 || length!=input.remaining()) throw new IllegalArgumentException("Invalid acceptance audit body");
                acceptance=TransactionAcceptance.decode(input.slice()); input.position(input.limit());
            }
            case COMMIT_REJECTED -> { if (input.remaining()!=32) throw new IllegalArgumentException("Invalid transaction rejection body"); rejected=new byte[32]; input.get(rejected); }
        }
        if (input.hasRemaining()) throw new IllegalArgumentException("Trailing command bytes");
        return new DistributedCommand(kind,group,epoch,schema,operation,mutations,conditions,ack,floor,control,acceptance,rejected);
    }
    static byte[] digest(DistributedCommand command) {
        try { return MessageDigest.getInstance("SHA-256").digest(encode(command)); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static int kind(DistributedCommand.Kind kind) { return switch(kind) { case DATA -> 1; case SESSION_FLOOR -> 2; case SESSION_CONTROL -> 3; case COMMIT_ACCEPTED -> 4; case COMMIT_REJECTED -> 5; }; }
    private static DistributedCommand.Kind kind(int tag) { return switch(tag) { case 1 -> DistributedCommand.Kind.DATA; case 2 -> DistributedCommand.Kind.SESSION_FLOOR; case 3 -> DistributedCommand.Kind.SESSION_CONTROL; case 4 -> DistributedCommand.Kind.COMMIT_ACCEPTED; case 5 -> DistributedCommand.Kind.COMMIT_REJECTED; default -> throw new IllegalArgumentException("Unknown command kind"); }; }
    private static void members(ByteBuffer output,List<byte[]> members) { output.putInt(members.size()); for (byte[] key:members) output.putChar((char)key.length).put(key); }
    private static List<byte[]> members(ByteBuffer input) {
        require(input,4); int count=input.getInt(); if (count<1 || count>DistributedSchemaCodec.MAX_KEY_COMPONENTS || 2L*count>input.remaining()) throw new IllegalArgumentException("Invalid bucket member count");
        int start=input.position();
        for (int i=0;i<count;i++) { require(input,2); int length=input.getChar(); require(input,length); input.position(input.position()+length); }
        var copy=input.duplicate(); copy.position(start); var members=new ArrayList<byte[]>(count);
        for (int i=0;i<count;i++) { int length=copy.getChar(); byte[] key=new byte[length]; copy.get(key); members.add(key); }
        return List.copyOf(members);
    }
    private static void require(ByteBuffer input,int count) { if (input.remaining()<count) throw new IllegalArgumentException("Truncated command body"); }
    private static void bytes(ByteBuffer output,byte[] bytes) { output.putInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.put(bytes); }
    private static void uuid(ByteBuffer output,UUID id) { output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer input) { return new UUID(input.getLong(),input.getLong()); }
}
