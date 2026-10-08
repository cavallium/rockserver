package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Current canonical participant log record. PREPARE is separate from the accepted business effects it stages. */
public final class RatisParticipantRecord {
    private static final int MAGIC=0x52535054, PREPARE=1, FINALIZE=2;
    private RatisParticipantRecord() {}
    public static boolean isParticipant(byte[] bytes) { return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static boolean isFinalization(byte[] bytes) { return bytes.length>=8 && ByteBuffer.wrap(bytes).getInt()==MAGIC && ByteBuffer.wrap(bytes).getInt(4)==FINALIZE; }
    public static byte[] encode(ParticipantFinalization finalization) { byte[] body=finalization.encode(); return ByteBuffer.allocate(8+body.length).putInt(MAGIC).putInt(FINALIZE).put(body).array(); }
    public static ParticipantFinalization decodeFinalization(byte[] bytes) {
        if (bytes.length<8 || bytes.length>8+ParticipantFinalization.MAX_BYTES) throw new IllegalArgumentException("Invalid FINALIZE record size");
        var b=ByteBuffer.wrap(bytes); if (b.getInt()!=MAGIC || b.getInt()!=FINALIZE) throw new IllegalArgumentException("Expected FINALIZE record");
        byte[] body=new byte[b.remaining()]; b.get(body); return ParticipantFinalization.decode(body);
    }
    public static byte[] digest(ParticipantFinalization finalization) { return hash(encode(finalization)); }
    public static ApplyPosition finalizationResult(byte[] bytes) { var vote=result(bytes); if (!vote.prepared()) throw new IllegalArgumentException("FINALIZE requires an applied receipt"); return vote.position(); }
    public static byte[] encode(PreparedParticipant.Preparation preparation) {
        byte[] body=preparation.encode(); return ByteBuffer.allocate(8+body.length).putInt(MAGIC).putInt(PREPARE).put(body).array();
    }
    public static PreparedParticipant.Preparation decode(byte[] bytes) {
        if (bytes.length<8 || bytes.length>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Invalid participant record size");
        var b=ByteBuffer.wrap(bytes); if (b.getInt()!=MAGIC || b.getInt()!=PREPARE) throw new IllegalArgumentException("Unknown participant record kind");
        byte[] body=new byte[b.remaining()]; b.get(body); return PreparedParticipant.Preparation.decode(body);
    }
    public static byte[] digest(PreparedParticipant.Preparation preparation) {
        return hash(encode(preparation));
    }
    private static byte[] hash(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    public static byte[] result(ApplyPosition position) { return result(ParticipantVote.prepared(position)); }
    public static byte[] result(ParticipantVote vote) { if (!vote.prepared()) return new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode(); var position=vote.position(); return new CommandResult(CommandResult.Status.SUCCESS,List.of(ByteBuffer.allocate(16).putLong(position.term()).putLong(position.index()).array())).encode(); }
    public static ParticipantVote result(byte[] bytes) {
        var result=CommandResult.decode(bytes); if (result.status()==CommandResult.Status.CONFLICT && result.valueCount()==0) return ParticipantVote.negative(); if (result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=1 || result.value(0)==null || result.value(0).length!=16) throw new IllegalArgumentException("Invalid prepared vote receipt");
        var b=ByteBuffer.wrap(result.value(0)); var position=new ApplyPosition(b.getLong(),b.getLong()); if (position.term()<=0 || position.index()<=0) throw new IllegalArgumentException("Invalid prepared position"); return ParticipantVote.prepared(position);
    }
}
