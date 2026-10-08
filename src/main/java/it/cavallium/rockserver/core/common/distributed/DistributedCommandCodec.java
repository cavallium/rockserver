package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.List;
import java.util.UUID;

/** Canonical big-endian v1/v2/v3/v4/v5/v6/v7 encoding; decoded legacy commands retain their original bytes/hash. */
public final class DistributedCommandCodec {
    public static final int FORMAT_VERSION = 2;
    public static final int MAX_ITEMS = 256;
    public static final int MAX_MUTATIONS = MAX_ITEMS;
    public static final int MAX_ENCODED_BYTES = 2 * 1024 * 1024;
    // version, group UUID, range epoch, schema digest, session UUID/epoch/sequence, count
    static final int HEADER_BYTES = 4 + 16 + 8 + 32 + 16 + 8 + 8 + 4;

    private DistributedCommandCodec() {}
    static int headerBytes(int version) { return HEADER_BYTES + (version >= 2 ? 4 : 0) + (version >= 3 ? 8 : 0) + (version == 5 ? 4 : 0); }

    /** Exact canonical size without allocating or copying command operands. */
    public static int encodedSize(DistributedCommand command) {
        Objects.requireNonNull(command, "command");
        int size = headerBytes(command.formatVersion()) + (command.sessionControl()==null ? 0 : command.sessionControl().encodedSize());
        for (Mutation mutation : command.mutations()) size += mutation.encodedSize();
        for (KeyPredicate predicate : command.predicates()) size += predicate.encodedSize();
        if (command.acceptance()!=null) size+=4+command.acceptance().encodedSize();
        return size;
    }

    public static byte[] encode(DistributedCommand command) {
        ByteBuffer output = ByteBuffer.allocate(encodedSize(command));
        output.putInt(command.formatVersion());
        putUuid(output, command.groupId());
        output.putLong(command.rangeEpoch()).put(command.schemaDigest());
        putUuid(output, command.operationId().sessionId());
        output.putLong(command.operationId().epoch()).putLong(command.operationId().sequence());
        output.putInt(command.mutations().size());
        if (command.formatVersion() >= 2) output.putInt(command.predicates().size());
        if (command.formatVersion() == 3 || command.formatVersion() == 6 || command.formatVersion() == 7) output.putLong(command.acknowledgedThrough());
        if (command.formatVersion() == 4) output.putLong(command.minimumSessionEpoch());
        if (command.formatVersion() == 5) {
            output.putLong(command.acknowledgedThrough()).putInt(command.sessionControl().encodedSize()).put(command.sessionControl().encode());
        }
        for (Mutation mutation : command.mutations()) {
            output.put((byte) switch (mutation.kind()) {
                case PUT -> 1; case DELETE -> 2; case MERGE -> 3; case DELETE_RANGE -> 4; case BUCKET_PUT -> 5; case BUCKET_DELETE -> 6;
            });
            output.put((byte) switch (mutation.returnMode()) {
                case NONE -> 0; case PREVIOUS -> 1; case CURRENT -> 2;
            });
            putUuid(output, mutation.columnId());
            byte[] key = mutation.key();
            byte[] value = mutation.value();
            output.putInt(key.length).put(key);
            output.putInt(value == null ? -1 : value.length);
            if (value != null) output.put(value);
            if (mutation.bucket()) putMembers(output,mutation.memberKeys());
        }
        for (KeyPredicate predicate : command.predicates()) {
            output.put((byte) (predicate.bucket() ? 2 : 1)); // v2 physical/v6 member equality or absence
            putUuid(output, predicate.columnId());
            byte[] key = predicate.key();
            byte[] expected = predicate.expectedValue();
            output.putInt(key.length).put(key).putInt(expected == null ? -1 : expected.length);
            if (expected != null) output.put(expected);
            if (predicate.bucket()) putMembers(output,predicate.memberKeys());
        }
        if (command.acceptance()!=null) { output.putInt(command.acceptance().encodedSize()); command.acceptance().encode(output); }
        return output.array();
    }

    public static DistributedCommand decode(byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length < HEADER_BYTES || encoded.length > MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("Invalid command byte length");
        }
        ByteBuffer input = ByteBuffer.wrap(encoded);
        int version = input.getInt();
        if (version < 1 || version > 7) throw new IllegalArgumentException("Unsupported command version");
        if (encoded.length < headerBytes(version)) throw new IllegalArgumentException("Truncated command header");
        UUID group = getUuid(input);
        long rangeEpoch = input.getLong();
        byte[] schema = new byte[32];
        input.get(schema);
        OperationId operation = new OperationId(getUuid(input), input.getLong(), input.getLong());
        int count = input.getInt();
        int predicateCount = version >= 2 ? input.getInt() : 0;
        long acknowledgedThrough = version == 3 || version == 5 || version == 6 || version == 7 ? input.getLong() : -1;
        long minimumEpoch = version == 4 ? input.getLong() : -1;
        RangeSessionControl control=null;
        if (version == 5) {
            int length=input.getInt();
            if ((length!=RangeSessionControl.ADVANCE_BYTES && length!=RangeSessionControl.BIND_BYTES) || length!=input.remaining() || count!=0 || predicateCount!=0) throw new IllegalArgumentException("Invalid session control envelope");
            byte[] bytes=new byte[length]; input.get(bytes); control=RangeSessionControl.decode(bytes);
            minimumEpoch=control.authority().minimumEpoch();
        }
        if (count < 0 || predicateCount < 0 || (long) count + predicateCount > MAX_ITEMS
                || 26L * count + 25L * predicateCount > input.remaining()) {
            throw new IllegalArgumentException("Invalid command item counts");
        }
        var mutations = new ArrayList<Mutation>(count);
        for (int i = 0; i < count; i++) {
            require(input, 26);
            Mutation.Kind kind = switch (input.get()) {
                case 1 -> Mutation.Kind.PUT; case 2 -> Mutation.Kind.DELETE;
                case 3 -> Mutation.Kind.MERGE; case 4 -> Mutation.Kind.DELETE_RANGE;
                case 5 -> { if (version!=6 && version!=7) throw new IllegalArgumentException("Bucket mutation requires v6 or v7"); yield Mutation.Kind.BUCKET_PUT; }
                case 6 -> { if (version!=6 && version!=7) throw new IllegalArgumentException("Bucket mutation requires v6 or v7"); yield Mutation.Kind.BUCKET_DELETE; }
                default -> throw new IllegalArgumentException("Unknown mutation kind");
            };
            Mutation.ReturnMode mode = switch (input.get()) {
                case 0 -> Mutation.ReturnMode.NONE; case 1 -> Mutation.ReturnMode.PREVIOUS;
                case 2 -> Mutation.ReturnMode.CURRENT;
                default -> throw new IllegalArgumentException("Unknown return mode");
            };
            UUID column = getUuid(input);
            int keyLength = input.getInt();
            if (keyLength < 0 || keyLength > input.remaining() - 4) {
                throw new IllegalArgumentException("Invalid key length");
            }
            // Check both lengths and the operation shape before allocating either array.
            int valueLength = input.getInt(input.position() + keyLength);
            int available = input.remaining() - keyLength - 4;
            if (kind == Mutation.Kind.DELETE || kind == Mutation.Kind.BUCKET_DELETE ? valueLength != -1 : valueLength < 0 || valueLength > available) {
                throw new IllegalArgumentException("Invalid value length");
            }
            boolean bucket=kind==Mutation.Kind.BUCKET_PUT || kind==Mutation.Kind.BUCKET_DELETE;
            ByteBuffer memberInput=null; List<byte[]> members=List.of();
            if (bucket) {
                memberInput=input.duplicate(); memberInput.position(input.position()+keyLength+4+Math.max(0,valueLength)); members=getMembers(memberInput);
            }
            byte[] key = new byte[keyLength];
            input.get(key);
            input.getInt();
            byte[] value = valueLength == -1 ? null : new byte[valueLength];
            if (value != null) input.get(value);
            if (bucket) {
                input.position(memberInput.position());
                mutations.add(kind==Mutation.Kind.BUCKET_PUT ? Mutation.bucketPut(column,key,members,value,mode) : Mutation.bucketDelete(column,key,members,mode));
            } else mutations.add(new Mutation(kind, column, key, value, mode));
        }
        var predicates = new ArrayList<KeyPredicate>(predicateCount);
        for (int i = 0; i < predicateCount; i++) {
            require(input, 25);
            int predicateKind=input.get() & 255;
            if (predicateKind!=1 && (predicateKind!=2 || version!=6 && version!=7)) throw new IllegalArgumentException("Unknown predicate kind");
            UUID column = getUuid(input);
            int keyLength = input.getInt();
            if (keyLength < 0 || keyLength > input.remaining() - 4) {
                throw new IllegalArgumentException("Invalid predicate key length");
            }
            int expectedLength = input.getInt(input.position() + keyLength);
            if (expectedLength < -1 || expectedLength > input.remaining() - keyLength - 4) {
                throw new IllegalArgumentException("Invalid predicate value length");
            }
            ByteBuffer memberInput=null; List<byte[]> members=List.of();
            if (predicateKind==2) {
                memberInput=input.duplicate(); memberInput.position(input.position()+keyLength+4+Math.max(0,expectedLength)); members=getMembers(memberInput);
            }
            byte[] key = new byte[keyLength];
            input.get(key);
            input.getInt();
            byte[] expected = expectedLength == -1 ? null : new byte[expectedLength];
            if (expected != null) input.get(expected);
            if (predicateKind==2) {
                input.position(memberInput.position()); predicates.add(KeyPredicate.bucketMember(column,key,members,expected));
            } else predicates.add(new KeyPredicate(column, key, expected));
        }
        TransactionAcceptance acceptance=null;
        if (version==7) {
            require(input,4); int length=input.getInt();
            if (length<1 || length>MAX_ENCODED_BYTES || length!=input.remaining()) throw new IllegalArgumentException("Invalid transaction acceptance trailer length");
            acceptance=TransactionAcceptance.decode(input.slice()); input.position(input.limit());
        }
        if (input.hasRemaining()) throw new IllegalArgumentException("Trailing command bytes");
        return new DistributedCommand(version, group, rangeEpoch, schema, operation, mutations, predicates, acknowledgedThrough, minimumEpoch, control,acceptance);
    }

    static byte[] digest(DistributedCommand command) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(encode(command));
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError("SHA-256 unavailable", impossible);
        }
    }

    private static void putMembers(ByteBuffer output,List<byte[]> members) {
        output.putInt(members.size()); for (byte[] key:members) output.putChar((char)key.length).put(key);
    }
    private static List<byte[]> getMembers(ByteBuffer input) {
        require(input,4); int count=input.getInt();
        if (count<1 || count>DistributedSchemaCodec.MAX_KEY_COMPONENTS || 2L*count>input.remaining()) throw new IllegalArgumentException("Invalid bucket member component count");
        int start=input.position();
        for (int i=0;i<count;i++) {
            require(input,2); int length=input.getChar(); require(input,length); input.position(input.position()+length);
        }
        int end=input.position(); var copy=input.duplicate(); copy.position(start); var keys=new ArrayList<byte[]>(count);
        for (int i=0;i<count;i++) { int length=copy.getChar(); byte[] key=new byte[length]; copy.get(key); keys.add(key); }
        input.position(end); return List.copyOf(keys);
    }

    private static void require(ByteBuffer input, int count) {
        if (input.remaining() < count) throw new IllegalArgumentException("Truncated mutation");
    }

    private static void putUuid(ByteBuffer output, UUID uuid) {
        output.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
    }

    private static UUID getUuid(ByteBuffer input) {
        return new UUID(input.getLong(), input.getLong());
    }
}
