package it.cavallium.rockserver.core.common.distributed;

import java.util.Arrays;
import java.util.Objects;
import java.util.List;
import java.util.UUID;

/** Physical-row or bucket-member equality: null expects absence, empty bytes expect a present empty value. */
public final class KeyPredicate {
    private final UUID columnId;
    private final byte[] key;
    private final byte[] expectedValue;
    private final List<byte[]> memberKeys;
    private final boolean bucket;

    public KeyPredicate(UUID columnId, byte[] key, byte[] expectedValue) {
        this(columnId,key,expectedValue,null);
    }
    public static KeyPredicate bucketMember(UUID column,byte[] physical,List<byte[]> memberKeys,byte[] expected) {
        return new KeyPredicate(column,physical,expected,Objects.requireNonNull(memberKeys));
    }
    private KeyPredicate(UUID columnId,byte[] key,byte[] expectedValue,List<byte[]> members) {
        this.columnId = Objects.requireNonNull(columnId, "columnId");
        Objects.requireNonNull(key, "key");
        int memberBytes=members==null ? 0 : Mutation.memberBytes(members);
        if ((long) key.length + (expectedValue == null ? 0 : expectedValue.length) + memberBytes
                > DistributedCommandCodec.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("Predicate exceeds command byte limit");
        }
        this.key = key.clone();
        this.expectedValue = expectedValue == null ? null : expectedValue.clone();
        this.bucket=members!=null; this.memberKeys=bucket ? Mutation.copyMembers(members) : List.of();
    }

    public UUID columnId() { return columnId; }
    public byte[] key() { return key.clone(); }
    public byte[] expectedValue() { return expectedValue == null ? null : expectedValue.clone(); }
    public boolean bucket() { return bucket; }
    public List<byte[]> memberKeys() { return Mutation.copyMembers(memberKeys); }
    public int encodedSize() { return 25 + key.length + (expectedValue == null ? 0 : expectedValue.length) + (bucket ? Mutation.memberBytes(memberKeys) : 0); }

    @Override public boolean equals(Object other) {
        return other instanceof KeyPredicate predicate && columnId.equals(predicate.columnId)
                && Arrays.equals(key, predicate.key) && Arrays.equals(expectedValue, predicate.expectedValue)
                && bucket==predicate.bucket && Mutation.sameMembers(memberKeys,predicate.memberKeys);
    }
    @Override public int hashCode() {
        int hash=31 * (31 * columnId.hashCode() + Arrays.hashCode(key)) + Arrays.hashCode(expectedValue);
        return bucket ? 31*hash+Mutation.membersHash(memberKeys) : hash;
    }
}
