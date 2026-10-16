package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeHomeSourceClosing;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Committed HOME reference-creation fence. */
public record RatisHomeSourceClosingRecord(NativeHomeSourceClosing.Request request) {
    public static final int MAGIC=0x52485343,MAX_BYTES=8+NativeHomeSourceClosing.Request.MAX_BYTES;
    public RatisHomeSourceClosingRecord {Objects.requireNonNull(request);}
    public byte[] encode(){byte[] bytes=request.encode();return ByteBuffer.allocate(8+bytes.length).putInt(MAGIC).putInt(bytes.length).put(bytes).array();}
    public static boolean isRecord(byte[] bytes){return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC;}
    public static RatisHomeSourceClosingRecord decode(byte[] bytes){if(bytes.length<8 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid HOME closing record bound");var b=ByteBuffer.wrap(bytes);if(b.getInt()!=MAGIC || b.getInt()!=b.remaining())throw new IllegalArgumentException("Invalid HOME closing record");byte[] body=new byte[b.remaining()];b.get(body);return new RatisHomeSourceClosingRecord(NativeHomeSourceClosing.Request.decode(body));}
}
