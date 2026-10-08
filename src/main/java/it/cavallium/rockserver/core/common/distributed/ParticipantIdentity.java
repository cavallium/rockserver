package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Original business identity; recovery operation IDs do not control retention. */
public record ParticipantIdentity(UUID homeGroup,long homeEpoch,UUID participantGroup,TransactionStatus definition) {
    public static final int MAX_BYTES=44+TransactionStatus.MAX_BYTES;
    public ParticipantIdentity {
        Objects.requireNonNull(homeGroup); Objects.requireNonNull(participantGroup);
        definition=Objects.requireNonNull(definition).asInitial();
        if (homeEpoch<0 || definition.participants().stream().noneMatch(p -> p.groupId().equals(participantGroup)))
            throw new IllegalArgumentException("Invalid participant identity");
    }
    public OperationId business() { return definition.operation(); }
    public UUID transactionId() { return definition.transactionId(); }
    public TransactionStatus.Participant participant() { return definition.participants().stream().filter(p -> p.groupId().equals(participantGroup)).findFirst().orElseThrow(); }
    public byte[] encode() {
        byte[] status=definition.encode(); var b=ByteBuffer.allocate(44+status.length);
        b.putLong(homeGroup.getMostSignificantBits()).putLong(homeGroup.getLeastSignificantBits()).putLong(homeEpoch)
                .putLong(participantGroup.getMostSignificantBits()).putLong(participantGroup.getLeastSignificantBits()).putInt(status.length).put(status);
        return b.array();
    }
    public static ParticipantIdentity decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<44+TransactionStatus.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid participant identity size");
        var b=ByteBuffer.wrap(bytes); var home=new UUID(b.getLong(),b.getLong()); long epoch=b.getLong(); var group=new UUID(b.getLong(),b.getLong());
        int n=b.getInt(); if (n!=b.remaining()) throw new IllegalArgumentException("Invalid participant definition size");
        byte[] status=new byte[n]; b.get(status); var identity=new ParticipantIdentity(home,epoch,group,TransactionStatus.decode(status));
        if (!Arrays.equals(bytes,identity.encode())) throw new IllegalArgumentException("Noncanonical participant identity"); return identity;
    }
    public byte[] digest() {
        try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
}
