package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Current canonical participant log record. PREPARE is separate from the accepted business effects it stages. */
public final class RatisParticipantRecord {
    private static final int MAGIC=0x52535054, PREPARE=1;
    private RatisParticipantRecord() {}
    public static boolean isParticipant(byte[] bytes) { return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static byte[] encode(PreparedParticipant.Preparation preparation) {
        byte[] body=preparation.encode(); return ByteBuffer.allocate(8+body.length).putInt(MAGIC).putInt(PREPARE).put(body).array();
    }
    public static PreparedParticipant.Preparation decode(byte[] bytes) {
        if (bytes.length<8 || bytes.length>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Invalid participant record size");
        var b=ByteBuffer.wrap(bytes); if (b.getInt()!=MAGIC || b.getInt()!=PREPARE) throw new IllegalArgumentException("Unknown participant record kind");
        byte[] body=new byte[b.remaining()]; b.get(body); return PreparedParticipant.Preparation.decode(body);
    }
    public static byte[] digest(PreparedParticipant.Preparation preparation) {
        try { return MessageDigest.getInstance("SHA-256").digest(encode(preparation)); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    public static byte[] result(ApplyPosition position) { return new CommandResult(CommandResult.Status.SUCCESS,List.of(ByteBuffer.allocate(16).putLong(position.term()).putLong(position.index()).array())).encode(); }
    public static ApplyPosition result(byte[] bytes) {
        var result=CommandResult.decode(bytes); if (result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=1 || result.value(0)==null || result.value(0).length!=16) throw new IllegalArgumentException("Invalid prepared vote receipt");
        var b=ByteBuffer.wrap(result.value(0)); var position=new ApplyPosition(b.getLong(),b.getLong()); if (position.term()<=0 || position.index()<=0) throw new IllegalArgumentException("Invalid prepared position"); return position;
    }
}
