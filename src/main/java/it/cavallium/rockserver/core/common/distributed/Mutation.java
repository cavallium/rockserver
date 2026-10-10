package it.cavallium.rockserver.core.common.distributed;

import java.util.Arrays;
import java.util.Objects;
import java.util.List;
import java.util.UUID;

/** A physical or bucket-member mutation. DELETE_RANGE uses key/value as inclusive/exclusive bounds. */
public final class Mutation {
    public enum Kind { PUT, DELETE, MERGE, DELETE_RANGE, BUCKET_PUT, BUCKET_DELETE, BUCKET_MERGE }
    public enum ReturnMode { NONE, PREVIOUS, CURRENT }

    private final Kind kind;
    private final UUID columnId;
    private final byte[] key;
    private final byte[] value;
    private final ReturnMode returnMode;
    private final List<byte[]> memberKeys;

    public Mutation(Kind kind, UUID columnId, byte[] key, byte[] value) {
        this(kind, columnId, key, value, ReturnMode.NONE);
    }

    public Mutation(Kind kind, UUID columnId, byte[] key, byte[] value, ReturnMode returnMode) {
        this(kind,columnId,key,value,returnMode,null);
    }

    public static Mutation bucketPut(UUID column,byte[] physical,List<byte[]> memberKeys,byte[] value,ReturnMode mode) {
        return new Mutation(Kind.BUCKET_PUT,column,physical,value,mode,Objects.requireNonNull(memberKeys));
    }
    public static Mutation bucketDelete(UUID column,byte[] physical,List<byte[]> memberKeys,ReturnMode mode) {
        return new Mutation(Kind.BUCKET_DELETE,column,physical,null,mode,Objects.requireNonNull(memberKeys));
    }
    public static Mutation bucketMerge(UUID column,byte[] physical,List<byte[]> memberKeys,byte[] operand,ReturnMode mode) {
        return new Mutation(Kind.BUCKET_MERGE,column,physical,operand,mode,Objects.requireNonNull(memberKeys));
    }
    private Mutation(Kind kind,UUID columnId,byte[] key,byte[] value,ReturnMode returnMode,List<byte[]> members) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.columnId = Objects.requireNonNull(columnId, "columnId");
        this.returnMode = Objects.requireNonNull(returnMode, "returnMode");
        if (kind == Kind.DELETE_RANGE && returnMode != ReturnMode.NONE) {
            throw new IllegalArgumentException("Range deletion has no point return value");
        }
        Objects.requireNonNull(key, "key");
        boolean bucket=kind==Kind.BUCKET_PUT || kind==Kind.BUCKET_DELETE || kind==Kind.BUCKET_MERGE;
        if (bucket!=(members!=null)) throw new IllegalArgumentException("Bucket mutations require explicit member keys");
        boolean deletion=kind==Kind.DELETE || kind==Kind.BUCKET_DELETE;
        if (deletion ? value != null : value == null) {
            throw new IllegalArgumentException("Only DELETE and BUCKET_DELETE have a null value");
        }
        int memberBytes=bucket ? memberBytes(members) : 0;
        if ((long) key.length + (value == null ? 0 : value.length) + memberBytes > DistributedCommandCodec.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("Mutation exceeds command byte limit");
        }
        if (kind == Kind.DELETE_RANGE && Arrays.compareUnsigned(key, value) >= 0) {
            throw new IllegalArgumentException("Range lower bound must precede upper bound");
        }
        this.key = key.clone();
        this.value = value == null ? null : value.clone();
        this.memberKeys=bucket ? copyMembers(members) : List.of();
    }

    public Kind kind() { return kind; }
    public UUID columnId() { return columnId; }
    public byte[] key() { return key.clone(); }
    public byte[] value() { return value == null ? null : value.clone(); }
    public ReturnMode returnMode() { return returnMode; }

    public boolean bucket() { return kind==Kind.BUCKET_PUT || kind==Kind.BUCKET_DELETE || kind==Kind.BUCKET_MERGE; }
    public List<byte[]> memberKeys() { return copyMembers(memberKeys); }
    /** Canonical size without copying keys, values, or bucket members. */
    public int encodedSize() { return 26 + key.length + (value == null ? 0 : value.length) + (bucket() ? memberBytes(memberKeys) : 0); }

    static int memberBytes(List<byte[]> members) {
        if (members==null || members.isEmpty() || members.size()>DistributedSchemaCodec.MAX_KEY_COMPONENTS) throw new IllegalArgumentException("Invalid bucket member component count");
        long size=4;
        for (byte[] key:members) {
            if (key==null || key.length>65535) throw new IllegalArgumentException("Invalid bucket member component length");
            size+=2L+key.length;
            if (size>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Bucket member keys exceed command budget");
        }
        return (int)size;
    }
    static List<byte[]> copyMembers(List<byte[]> members) { return members.stream().map(byte[]::clone).toList(); }
    static boolean sameMembers(List<byte[]> first,List<byte[]> second) {
        if (first.size()!=second.size()) return false;
        for (int i=0;i<first.size();i++) if (!Arrays.equals(first.get(i),second.get(i))) return false;
        return true;
    }
    static int membersHash(List<byte[]> members) { int hash=1; for (byte[] key:members) hash=31*hash+Arrays.hashCode(key); return hash; }

    @Override
    public boolean equals(Object other) {
        return other instanceof Mutation mutation && kind == mutation.kind
                && returnMode == mutation.returnMode && columnId.equals(mutation.columnId) && Arrays.equals(key, mutation.key)
                && Arrays.equals(value, mutation.value) && sameMembers(memberKeys,mutation.memberKeys);
    }

    @Override
    public int hashCode() {
        int hash=31 * (31 * Objects.hash(kind, columnId, returnMode) + Arrays.hashCode(key)) + Arrays.hashCode(value);
        return bucket() ? 31*hash+membersHash(memberKeys) : hash;
    }
}
