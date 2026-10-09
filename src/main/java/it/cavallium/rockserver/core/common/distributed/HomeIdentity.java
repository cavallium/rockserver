package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Original transaction definition in its dedicated HOME placement. */
public record HomeIdentity(UUID groupId,long rangeEpoch,byte[] schemaDigest,UUID columnId,TransactionStatus definition) {
    public static final int MAX_BYTES=76+TransactionStatus.MAX_BYTES;
    public HomeIdentity {
        Objects.requireNonNull(groupId); Objects.requireNonNull(columnId); Objects.requireNonNull(schemaDigest);
        if (rangeEpoch<0 || schemaDigest.length!=32) throw new IllegalArgumentException("Invalid HOME placement identity");
        schemaDigest=schemaDigest.clone(); definition=Objects.requireNonNull(definition).asInitial();
    }
    @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
    public OperationId business() { return definition.operation(); }
    public UUID transactionId() { return definition.transactionId(); }
    public byte[] encode() {
        byte[] status=definition.encode(); return ByteBuffer.allocate(76+status.length).putLong(groupId.getMostSignificantBits()).putLong(groupId.getLeastSignificantBits())
                .putLong(rangeEpoch).put(schemaDigest).putLong(columnId.getMostSignificantBits()).putLong(columnId.getLeastSignificantBits()).putInt(status.length).put(status).array();
    }
    public static HomeIdentity decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<76+TransactionStatus.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid HOME identity size");
        var b=ByteBuffer.wrap(bytes); UUID group=new UUID(b.getLong(),b.getLong()); long epoch=b.getLong(); byte[] schema=new byte[32]; b.get(schema); UUID column=new UUID(b.getLong(),b.getLong());
        int n=b.getInt(); if (n!=b.remaining()) throw new IllegalArgumentException("Invalid HOME definition length"); byte[] status=new byte[n]; b.get(status);
        var identity=new HomeIdentity(group,epoch,schema,column,TransactionStatus.decode(status)); if (!Arrays.equals(bytes,identity.encode())) throw new IllegalArgumentException("Noncanonical HOME identity"); return identity;
    }
    public byte[] digest() { try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    @Override public boolean equals(Object other) { return other instanceof HomeIdentity id && groupId.equals(id.groupId) && rangeEpoch==id.rangeEpoch && columnId.equals(id.columnId) && definition.equals(id.definition) && Arrays.equals(schemaDigest,id.schemaDigest); }
    @Override public int hashCode() { return Objects.hash(groupId,rangeEpoch,columnId,definition,Arrays.hashCode(schemaDigest)); }
}
