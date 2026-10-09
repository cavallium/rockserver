package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Consumed snapshot HOME and every participant's fenced gate absence. The wire stores the fence only once. */
public record SnapshotHomeRetirement(SnapshotHomeIdentity identity,SnapshotFenceStatus consumed,List<SnapshotFenceProof> participants) {
    public static final int MAX_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES-4;
    public SnapshotHomeRetirement {
        Objects.requireNonNull(identity); Objects.requireNonNull(consumed); Objects.requireNonNull(participants);
        if (!consumed.consumed() || !consumed.terminal() || !identity.fence().equals(consumed.fence()) || participants.size()!=identity.fence().participants().size()) throw new IllegalArgumentException("Snapshot HOME retirement requires matching consumption and every DATA fence");
        var byGroup=new HashMap<UUID,SnapshotFenceProof>();
        for (var proof:participants) {
            if (!proof.fence().equals(identity.fence()) || byGroup.putIfAbsent(proof.participantGroup(),proof)!=null) throw new IllegalArgumentException("Snapshot HOME retirement DATA identity changed");
            var progress=consumed.progress(proof.participantGroup()); var before=progress.capture()==null ? progress.ready() : progress.capture().position();
            if (before!=null && (proof.position().index()<=before.index() || proof.position().term()<before.term())) throw new IllegalArgumentException("Snapshot DATA fence precedes its READY/capture proof");
        }
        var canonical=new ArrayList<SnapshotFenceProof>(participants.size());
        for (var participant:identity.fence().participants()) {
            var proof=byGroup.get(participant.groupId()); if (proof==null) throw new IllegalArgumentException("Missing snapshot HOME participant");
            canonical.add(new SnapshotFenceProof(identity.fence(),participant.groupId(),proof.minimumEpoch(),proof.position()));
        }
        participants=List.copyOf(canonical);
        if (80L+consumed.encode().length+40L*participants.size()>MAX_BYTES) throw new IllegalArgumentException("Snapshot HOME retirement exceeds command budget");
    }
    public byte[] encode() {
        byte[] status=consumed.encode(); var b=ByteBuffer.allocate(80+status.length+40*participants.size());
        b.putLong(identity.groupId().getMostSignificantBits()).putLong(identity.groupId().getLeastSignificantBits()).putLong(identity.rangeEpoch()).put(identity.schemaDigest())
                .putLong(identity.columnId().getMostSignificantBits()).putLong(identity.columnId().getLeastSignificantBits()).putInt(status.length).put(status).putInt(participants.size());
        for (var proof:participants) b.putLong(proof.participantGroup().getMostSignificantBits()).putLong(proof.participantGroup().getLeastSignificantBits()).putLong(proof.minimumEpoch()).putLong(proof.position().term()).putLong(proof.position().index());
        return b.array();
    }
    public static SnapshotHomeRetirement decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<80+SnapshotFenceStatus.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot HOME retirement size");
        var b=ByteBuffer.wrap(bytes); UUID group=new UUID(b.getLong(),b.getLong()); long epoch=b.getLong(); byte[] schema=new byte[32]; b.get(schema); UUID column=new UUID(b.getLong(),b.getLong());
        int n=b.getInt(); if (n<SnapshotFenceStatus.HEADER_BYTES || n>SnapshotFenceStatus.MAX_BYTES || n>b.remaining()-4) throw new IllegalArgumentException("Invalid snapshot HOME consumed size");
        byte[] status=new byte[n]; b.get(status); var consumed=SnapshotFenceStatus.decode(status); var identity=new SnapshotHomeIdentity(group,epoch,schema,column,consumed.fence());
        n=b.getInt(); if (n<1 || n>SnapshotFence.MAX_PARTICIPANTS || b.remaining()!=40*n) throw new IllegalArgumentException("Invalid snapshot HOME participant count");
        var participants=new ArrayList<SnapshotFenceProof>(n);
        for (int i=0;i<n;i++) participants.add(new SnapshotFenceProof(identity.fence(),new UUID(b.getLong(),b.getLong()),b.getLong(),new ApplyPosition(b.getLong(),b.getLong())));
        var result=new SnapshotHomeRetirement(identity,consumed,participants); if (!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical snapshot HOME retirement"); return result;
    }
    public byte[] digest() { try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
