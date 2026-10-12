package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Immutable source-evidenced reroute identity. Digests bind admitted evidence; this model does not authenticate it. */
public record OperationRerouteOrigin(UUID clusterId,UUID databaseId,UUID domainId,DistributedCdcCheckpoint.Lineage source,OperationId operation,
        byte[] originalReceiptDigest,byte[] absenceCommitment,ApplyPosition absenceCut,byte[] targetDirectoryDigest,byte[] planDigest,int targetParticipantCount,OperationId firstControl) {
    public static final int MAX_BYTES=300,CONTROL_SPAN=256;
    public OperationRerouteOrigin {
        Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(domainId); Objects.requireNonNull(source); Objects.requireNonNull(operation); Objects.requireNonNull(absenceCut); Objects.requireNonNull(firstControl);
        if(absenceCut.term()<1 || targetParticipantCount<1 || targetParticipantCount>TransactionStatus.MAX_PARTICIPANTS || firstControl.epoch()<1 || firstControl.sequence()>Long.MAX_VALUE-(CONTROL_SPAN-1) || firstControl.equals(operation)) throw new IllegalArgumentException("Invalid bounded reroute origin/cut/control reservation");
        originalReceiptDigest=copy(originalReceiptDigest); absenceCommitment=copy(absenceCommitment); targetDirectoryDigest=copy(targetDirectoryDigest); planDigest=copy(planDigest);
    }
    private static byte[] copy(byte[] digest) { if(digest==null || digest.length!=32) throw new IllegalArgumentException("Invalid reroute origin digest"); return digest.clone(); }
    @Override public byte[] originalReceiptDigest() { return originalReceiptDigest.clone(); }
    @Override public byte[] absenceCommitment() { return absenceCommitment.clone(); }
    @Override public byte[] targetDirectoryDigest() { return targetDirectoryDigest.clone(); }
    @Override public byte[] planDigest() { return planDigest.clone(); }
    /** Same original operation has one HOME key even when a caller changes payload, target plan, evidence or control allocation. */
    public UUID transactionId() {
        byte[] domain="rockserver-operation-reroute".getBytes(StandardCharsets.US_ASCII); var out=ByteBuffer.allocate(domain.length+120).put(domain);
        uuid(out,clusterId); uuid(out,databaseId); uuid(out,domainId); uuid(out,source.rangeId()); uuid(out,source.groupId()); out.putLong(source.rangeEpoch()); operation(out,operation);
        byte[] digest=hash(out.array()); var in=ByteBuffer.wrap(digest); return new UUID(in.getLong(),in.getLong());
    }
    public byte[] encode() {
        var out=ByteBuffer.allocate(MAX_BYTES); uuid(out,clusterId); uuid(out,databaseId); uuid(out,domainId); uuid(out,source.rangeId()); uuid(out,source.groupId()); out.putLong(source.rangeEpoch()); operation(out,operation);
        out.put(originalReceiptDigest).put(absenceCommitment).putLong(absenceCut.term()).putLong(absenceCut.index()).put(targetDirectoryDigest).put(planDigest).putInt(targetParticipantCount); operation(out,firstControl); return out.array();
    }
    public static OperationRerouteOrigin decode(byte[] bytes) {
        if(bytes==null || bytes.length!=MAX_BYTES) throw new IllegalArgumentException("Invalid reroute origin length"); var in=ByteBuffer.wrap(bytes);
        UUID cluster=uuid(in),database=uuid(in),domain=uuid(in); var source=new DistributedCdcCheckpoint.Lineage(uuid(in),uuid(in),in.getLong()); var operation=operation(in); byte[] receipt=new byte[32],absence=new byte[32],target=new byte[32],plan=new byte[32]; in.get(receipt).get(absence); var cut=new ApplyPosition(in.getLong(),in.getLong()); in.get(target).get(plan); int participants=in.getInt(); var first=operation(in);
        return new OperationRerouteOrigin(cluster,database,domain,source,operation,receipt,absence,cut,target,plan,participants,first);
    }
    public byte[] digest() { return hash(encode()); }
    @Override public boolean equals(Object other) { return other instanceof OperationRerouteOrigin origin && Arrays.equals(encode(),origin.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
    private static void operation(ByteBuffer out,OperationId operation) { uuid(out,operation.sessionId()); out.putLong(operation.epoch()).putLong(operation.sequence()); }
    private static OperationId operation(ByteBuffer in) { return new OperationId(uuid(in),in.getLong(),in.getLong()); }
    private static byte[] hash(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
