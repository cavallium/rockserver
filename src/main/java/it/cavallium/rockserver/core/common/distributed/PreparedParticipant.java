package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Native durable staged plan and prepare receipt. Home registration/vote provenance must be verified by the coordinator. */
public record PreparedParticipant(Preparation preparation,ApplyPosition position) {
    public PreparedParticipant {
        Objects.requireNonNull(preparation); Objects.requireNonNull(position);
        var snapshot=preparation.business().acceptance().snapshot();
        if (position.term()!=snapshot.term() || position.index()<=snapshot.index()) throw new IllegalArgumentException("Prepared position does not follow its accepted cut");
    }
    public byte[] encode() { byte[] bytes=preparation.encode(); return ByteBuffer.allocate(16+bytes.length).putLong(position.term()).putLong(position.index()).put(bytes).array(); }
    public static PreparedParticipant decode(byte[] bytes) {
        if (bytes.length<16 || bytes.length>16+Preparation.MAX_BYTES) throw new IllegalArgumentException("Invalid prepared record length");
        var b=ByteBuffer.wrap(bytes); var position=new ApplyPosition(b.getLong(),b.getLong()); byte[] body=new byte[b.remaining()]; b.get(body); return new PreparedParticipant(Preparation.decode(body),position);
    }
    /** Immutable accepted business plan; the separate prepare identity never becomes a business receipt. */
    public static final class Preparation {
        public static final int MAX_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES-8;
        private final UUID homeGroup;
        private final long homeEpoch;
        private final OperationId operation;
        private final TransactionStatus registration;
        private final DistributedCommand business;
        private volatile byte[] digest;
        public Preparation(UUID homeGroup,long homeEpoch,OperationId operation,TransactionStatus registration,DistributedCommand business) {
            this.homeGroup=Objects.requireNonNull(homeGroup); this.homeEpoch=homeEpoch; this.operation=Objects.requireNonNull(operation); this.registration=Objects.requireNonNull(registration); this.business=Objects.requireNonNull(business);
            if (homeEpoch<0 || registration.phase()!=TransactionStatus.Phase.PREPARING || business.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED
                    || !business.operationId().equals(registration.operation()) || operation.equals(business.operationId())) throw new IllegalArgumentException("Invalid accepted participant preparation");
            var participant=registration.participants().stream().filter(p -> p.groupId().equals(business.groupId())).findFirst().orElseThrow(() -> new IllegalArgumentException("Unregistered participant group"));
            if (participant.prepared()!=null || participant.rangeEpoch()!=business.rangeEpoch() || !Arrays.equals(participant.schemaDigest(),business.schemaDigest())
                    || !Arrays.equals(participant.planDigest(),planDigest(business))) throw new IllegalArgumentException("Participant plan differs from frozen registration");
            if ((long)64+registration.encode().length+DistributedCommandCodec.encodedSize(business)>MAX_BYTES) throw new IllegalArgumentException("Prepared plan exceeds byte budget");
        }
        public UUID homeGroup() { return homeGroup; } public long homeEpoch() { return homeEpoch; } public OperationId operation() { return operation; }
        public TransactionStatus registration() { return registration; } public DistributedCommand business() { return business; }
        public UUID transactionId() { return registration.transactionId(); } public TransactionScope scope() { return business.acceptance().scope(); }
        public boolean ownsOperation(OperationId id) { return operation.equals(id) || business.operationId().equals(id); }
        public boolean sharesOperation(Preparation other) { return ownsOperation(other.operation) || ownsOperation(other.business.operationId()); }
        public static byte[] planDigest(DistributedCommand business) {
            if (business.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED) throw new IllegalArgumentException("Participant plan must bind its original cut, scope, owner and result");
            return business.payloadDigest();
        }
        @Override public boolean equals(Object other) { return other instanceof Preparation preparation && Arrays.equals(encode(),preparation.encode()); }
        @Override public int hashCode() { return Arrays.hashCode(encode()); }
        public byte[] digest() {
            byte[] value=digest;
            if (value==null) {
                try { value=MessageDigest.getInstance("SHA-256").digest(encode()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
                digest=value;
            }
            return value.clone();
        }
        public byte[] encode() {
            byte[] home=registration.encode(),plan=DistributedCommandCodec.encode(business);
            var b=ByteBuffer.allocate(64+home.length+plan.length); uuid(b,homeGroup); b.putLong(homeEpoch); uuid(b,operation.sessionId()); b.putLong(operation.epoch()).putLong(operation.sequence());
            b.putInt(home.length).put(home).putInt(plan.length).put(plan); return b.array();
        }
        public static Preparation decode(byte[] bytes) {
            if (bytes.length<64 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid preparation length"); var b=ByteBuffer.wrap(bytes);
            UUID group=uuid(b); long epoch=b.getLong(); var operation=new OperationId(uuid(b),b.getLong(),b.getLong());
            int homeSize=b.getInt(); if (homeSize<TransactionStatus.HEADER_BYTES || homeSize>TransactionStatus.MAX_BYTES || homeSize>b.remaining()-4) throw new IllegalArgumentException("Invalid registration length");
            byte[] home=new byte[homeSize]; b.get(home); int planSize=b.getInt(); if (planSize!=b.remaining()) throw new IllegalArgumentException("Invalid staged plan length"); byte[] plan=new byte[planSize]; b.get(plan);
            var value=new Preparation(group,epoch,operation,TransactionStatus.decode(home),DistributedCommandCodec.decode(plan)); if (!Arrays.equals(bytes,value.encode())) throw new IllegalArgumentException("Noncanonical preparation"); return value;
        }
        private static void uuid(ByteBuffer b,UUID id) { b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
        private static UUID uuid(ByteBuffer b) { return new UUID(b.getLong(),b.getLong()); }
    }
}
