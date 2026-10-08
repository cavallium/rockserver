package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Current canonical participant log record. PREPARE is separate from the accepted business effects it stages. */
public final class RatisParticipantRecord {
    private static final int MAGIC=0x52535054, PREPARE=1, FINALIZE=2, PREPARE_NEGATIVE=3, RETIRE=4;
    private RatisParticipantRecord() {}
    public static boolean isParticipant(byte[] bytes) { return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static boolean isNegative(byte[] bytes) { return bytes.length>=8 && ByteBuffer.wrap(bytes).getInt()==MAGIC && ByteBuffer.wrap(bytes).getInt(4)==PREPARE_NEGATIVE; }
    public static boolean isFinalization(byte[] bytes) { return bytes.length>=8 && ByteBuffer.wrap(bytes).getInt()==MAGIC && ByteBuffer.wrap(bytes).getInt(4)==FINALIZE; }
    public static boolean isRetirement(byte[] bytes) { return bytes.length>=8 && ByteBuffer.wrap(bytes).getInt()==MAGIC && ByteBuffer.wrap(bytes).getInt(4)==RETIRE; }
    public static byte[] encode(ParticipantRetirement retirement) { byte[] body=retirement.encode(); return ByteBuffer.allocate(8+body.length).putInt(MAGIC).putInt(RETIRE).put(body).array(); }
    public static ParticipantRetirement decodeRetirement(byte[] bytes) {
        if (!isRetirement(bytes) || bytes.length>8+ParticipantRetirement.MAX_BYTES) throw new IllegalArgumentException("Invalid retirement record");
        return ParticipantRetirement.decode(Arrays.copyOfRange(bytes,8,bytes.length));
    }
    public static byte[] digest(ParticipantRetirement retirement) { return hash(encode(retirement)); }
    public static OperationId operation(ParticipantRetirement retirement) { return new OperationId(UUID.nameUUIDFromBytes(digest(retirement)),retirement.identity().business().epoch(),0); }
    public static byte[] encode(ParticipantFinalization finalization) { byte[] body=finalization.encode(); return ByteBuffer.allocate(8+body.length).putInt(MAGIC).putInt(FINALIZE).put(body).array(); }
    public static ParticipantFinalization decodeFinalization(byte[] bytes) {
        if (bytes.length<8 || bytes.length>8+ParticipantFinalization.MAX_BYTES) throw new IllegalArgumentException("Invalid FINALIZE record size");
        var b=ByteBuffer.wrap(bytes); if (b.getInt()!=MAGIC || b.getInt()!=FINALIZE) throw new IllegalArgumentException("Expected FINALIZE record");
        byte[] body=new byte[b.remaining()]; b.get(body); return ParticipantFinalization.decode(body);
    }
    public static byte[] digest(ParticipantFinalization finalization) { return hash(encode(finalization)); }
    public static ParticipantFinalizationResult finalizationResult(byte[] bytes) { var vote=result(bytes); if (vote.retired()) return ParticipantFinalizationResult.retired(vote.retirementProof()); if (!vote.prepared()) throw new IllegalArgumentException("FINALIZE requires an applied receipt"); return ParticipantFinalizationResult.applied(vote.position()); }
    public static byte[] encode(PreparedParticipant.Preparation preparation) { return encode(preparation,false); }
    public static byte[] encode(PreparedParticipant.Preparation preparation,boolean negative) {
        byte[] body=preparation.encode(); return ByteBuffer.allocate(8+body.length).putInt(MAGIC).putInt(negative ? PREPARE_NEGATIVE : PREPARE).put(body).array();
    }
    public static PreparedParticipant.Preparation decode(byte[] bytes) {
        if (bytes.length<8 || bytes.length>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Invalid participant record size");
        var b=ByteBuffer.wrap(bytes); int magic=b.getInt(),kind=b.getInt(); if (magic!=MAGIC || kind!=PREPARE && kind!=PREPARE_NEGATIVE) throw new IllegalArgumentException("Unknown participant record kind");
        byte[] body=new byte[b.remaining()]; b.get(body); return PreparedParticipant.Preparation.decode(body);
    }
    public static byte[] digest(PreparedParticipant.Preparation preparation) { return digest(preparation,false); }
    public static byte[] digest(PreparedParticipant.Preparation preparation,boolean negative) { return hash(encode(preparation,negative)); }
    private static byte[] hash(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    public static byte[] result(ParticipantFinalizationResult result) { return result.retired() ? result(ParticipantVote.retired(result.retirement())) : result(result.position()); }
    public static byte[] result(ApplyPosition position) { return result(ParticipantVote.prepared(position)); }
    public static byte[] result(ParticipantVote vote) {
        if (vote.retired()) { byte[] proof=vote.retirementProof().encode(); return ByteBuffer.allocate(1+proof.length).put((byte)2).put(proof).array(); }
        if (!vote.prepared()) return new byte[]{0};
        return ByteBuffer.allocate(17).put((byte)1).putLong(vote.position().term()).putLong(vote.position().index()).array();
    }
    public static ParticipantVote result(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<1 || bytes.length>1+ParticipantFenceProof.MAX_BYTES) throw new IllegalArgumentException("Invalid participant reply size");
        var b=ByteBuffer.wrap(bytes); int kind=b.get()&255;
        if (kind==0 && !b.hasRemaining()) return ParticipantVote.negative();
        if (kind==1 && b.remaining()==16) return ParticipantVote.prepared(new ApplyPosition(b.getLong(),b.getLong()));
        if (kind!=2) throw new IllegalArgumentException("Invalid participant reply kind/size");
        byte[] proof=new byte[b.remaining()]; b.get(proof); return ParticipantVote.retired(ParticipantFenceProof.decode(proof));
    }
}
