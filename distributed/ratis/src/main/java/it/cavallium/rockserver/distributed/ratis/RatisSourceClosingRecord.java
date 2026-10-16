package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeSourceClosing;
import java.nio.ByteBuffer;
import java.util.*;

/** One committed admission barrier; independent terminal reclamation authority is still required. */
public record RatisSourceClosingRecord(NativeSourceClosing.Request request) {
    private static final int MAGIC=0x5253434c;
    public static final int MAX_BYTES=8+NativeSourceClosing.Request.MAX_BYTES;
    public RatisSourceClosingRecord{Objects.requireNonNull(request);}
    public byte[] encode(){byte[] body=request.encode();return ByteBuffer.allocate(8+body.length).putInt(MAGIC).putInt(body.length).put(body).array();}
    public static boolean isRecord(byte[] bytes){return bytes!=null && bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC;}
    public static RatisSourceClosingRecord decode(byte[] bytes){if(bytes==null || bytes.length<8 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid source CLOSE record bound");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=MAGIC || in.getInt()!=in.remaining())throw new IllegalArgumentException("Invalid source CLOSE record");byte[] body=new byte[in.remaining()];in.get(body);var value=new RatisSourceClosingRecord(NativeSourceClosing.Request.decode(body));if(!Arrays.equals(bytes,value.encode()))throw new IllegalArgumentException("Noncanonical source CLOSE record");return value;}
}
