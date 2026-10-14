package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Caller-selected identity bound to canonical input and its original generation. */
public record RemoteMutationToken(UUID clusterId, UUID databaseId, UUID domainId, UUID ownerId, UUID requestId, long ownerEpoch, long requestSequence, Kind kind, byte[] schemaDigest, byte[] directoryDigest, byte[] inputDigest) {
    public enum Kind { POINT, RANGE, BULK }
    public static final int BYTES = 193;
    public RemoteMutationToken {
        if(requestSequence<0 || ownerEpoch<0)throw new IllegalArgumentException("Negative owner epoch"); Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(domainId); Objects.requireNonNull(ownerId); Objects.requireNonNull(requestId); Objects.requireNonNull(kind);
        schemaDigest=checked(schemaDigest); directoryDigest=checked(directoryDigest); inputDigest=checked(inputDigest);
    }
    private static byte[] checked(byte[] value) { if(value.length!=32) throw new IllegalArgumentException("Digest must contain 32 bytes"); return value.clone(); }
    @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
    @Override public byte[] directoryDigest() { return directoryDigest.clone(); }
    @Override public byte[] inputDigest() { return inputDigest.clone(); }
    public byte[] encode() { return ByteBuffer.allocate(BYTES).putLong(clusterId.getMostSignificantBits()).putLong(clusterId.getLeastSignificantBits()).putLong(databaseId.getMostSignificantBits()).putLong(databaseId.getLeastSignificantBits()).putLong(domainId.getMostSignificantBits()).putLong(domainId.getLeastSignificantBits()).putLong(ownerId.getMostSignificantBits()).putLong(ownerId.getLeastSignificantBits()).putLong(requestId.getMostSignificantBits()).putLong(requestId.getLeastSignificantBits()).putLong(ownerEpoch).putLong(requestSequence).put((byte)kind.ordinal()).put(schemaDigest).put(directoryDigest).put(inputDigest).array(); }
    public static RemoteMutationToken decode(byte[] bytes) { if(bytes.length!=BYTES)throw new IllegalArgumentException("Invalid token size"); var in=ByteBuffer.wrap(bytes); var cluster=new UUID(in.getLong(),in.getLong()); var database=new UUID(in.getLong(),in.getLong()); var domain=new UUID(in.getLong(),in.getLong()); var owner=new UUID(in.getLong(),in.getLong()); var id=new UUID(in.getLong(),in.getLong()); long epoch=in.getLong(),sequence=in.getLong(); int kind=in.get()&255; if(kind>=Kind.values().length)throw new IllegalArgumentException("Invalid mutation kind"); byte[] schema=new byte[32],directory=new byte[32],input=new byte[32]; in.get(schema).get(directory).get(input); return new RemoteMutationToken(cluster,database,domain,owner,id,epoch,sequence,Kind.values()[kind],schema,directory,input); }
    public static byte[] digest(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    @Override public boolean equals(Object other) { return other instanceof RemoteMutationToken token && Arrays.equals(encode(),token.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
}
