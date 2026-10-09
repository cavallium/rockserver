package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Reclaims a consumed HOME after DATA retirement; null consumed only reclaims fenced conflict receipts for an absent HOME. */
public record HomeRetirement(HomeIdentity identity,TransactionStatus consumed,List<ParticipantFenceProof> participants) {
    public static final int MAX_BYTES=81+TransactionStatus.MAX_BYTES+40*TransactionStatus.MAX_PARTICIPANTS;
    public HomeRetirement {
        Objects.requireNonNull(identity); Objects.requireNonNull(participants);
        if (consumed!=null && (!consumed.gcEligible() || !identity.definition().sameDefinition(consumed))) throw new IllegalArgumentException("HOME retirement requires matching consumed decision");
        if (consumed==null ? !participants.isEmpty() : participants.size()!=identity.definition().participants().size()) throw new IllegalArgumentException("Consumed HOME requires every DATA fence; orphan conflicts require none");
        var byGroup=new HashMap<UUID,ParticipantFenceProof>();
        for (var proof:participants) {
            if (byGroup.putIfAbsent(proof.identity().participantGroup(),proof)!=null || !proof.identity().equals(new ParticipantIdentity(identity.groupId(),identity.rangeEpoch(),proof.identity().participantGroup(),identity.definition())))
                throw new IllegalArgumentException("HOME retirement DATA fence changed definition");
        }
        participants=consumed==null ? List.of() : identity.definition().participants().stream().map(p -> Objects.requireNonNull(byGroup.get(p.groupId()),"Missing HOME retirement participant")).toList();
        if (consumed!=null) for (var participant:consumed.participants()) {
            var vote=participant.prepared(); var proof=byGroup.get(participant.groupId());
            if (vote!=null && (proof.position().index()<=vote.index() || proof.position().term()<vote.term())) throw new IllegalArgumentException("DATA retirement fence must follow the recorded positive vote");
        }
    }
    public byte[] encode() {
        byte[] status=(consumed==null ? identity.definition() : consumed).encode(); var b=ByteBuffer.allocate(81+status.length+40*participants.size());
        b.putLong(identity.groupId().getMostSignificantBits()).putLong(identity.groupId().getLeastSignificantBits()).putLong(identity.rangeEpoch()).put(identity.schemaDigest())
                .putLong(identity.columnId().getMostSignificantBits()).putLong(identity.columnId().getLeastSignificantBits()).put((byte)(consumed==null ? 0 : 1)).putInt(status.length).put(status).putInt(participants.size());
        for (var proof:participants) b.putLong(proof.identity().participantGroup().getMostSignificantBits()).putLong(proof.identity().participantGroup().getLeastSignificantBits())
                .putLong(proof.minimumEpoch()).putLong(proof.position().term()).putLong(proof.position().index());
        return b.array();
    }
    public static HomeRetirement decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<81+TransactionStatus.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid HOME retirement size");
        var b=ByteBuffer.wrap(bytes); UUID group=new UUID(b.getLong(),b.getLong()); long epoch=b.getLong(); byte[] schema=new byte[32]; b.get(schema); UUID column=new UUID(b.getLong(),b.getLong()); int mode=b.get()&255,n=b.getInt();
        if (mode>1 || n<TransactionStatus.HEADER_BYTES || n>TransactionStatus.MAX_BYTES || n>b.remaining()-4) throw new IllegalArgumentException("Invalid HOME retirement status");
        byte[] statusBytes=new byte[n]; b.get(statusBytes); var status=TransactionStatus.decode(statusBytes); if (mode==0) status.requireInitial();
        var identity=new HomeIdentity(group,epoch,schema,column,status); n=b.getInt(); if (n<0 || n>TransactionStatus.MAX_PARTICIPANTS || b.remaining()!=40*n) throw new IllegalArgumentException("Invalid HOME retirement participant count");
        var proofs=new ArrayList<ParticipantFenceProof>(n);
        for (int i=0;i<n;i++) { UUID participant=new UUID(b.getLong(),b.getLong()); proofs.add(new ParticipantFenceProof(new ParticipantIdentity(group,epoch,participant,status),b.getLong(),new ApplyPosition(b.getLong(),b.getLong()))); }
        var result=new HomeRetirement(identity,mode==0 ? null : status,proofs); if (!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical HOME retirement"); return result;
    }
    public byte[] digest() { try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
