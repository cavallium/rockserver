package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Replicated sealed-source metadata; proof payloads stay outside the committed record. */
public record RatisSourceOutcomeRecord(Kind kind,NativeSourceOutcomeCredit.Request request,byte[] result) {
    public enum Kind { CREDIT,PUBLISH }
    private static final int MAGIC=0x53524f55;
    public static final int MAX_BYTES=16+NativeSourceOutcomeCredit.Request.MAX_BYTES+CommandResult.MAX_BYTES;
    public RatisSourceOutcomeRecord {
        Objects.requireNonNull(kind);Objects.requireNonNull(request);
        if(kind==Kind.CREDIT ? result!=null : result==null || result.length>request.maximumResultBytes())throw new IllegalArgumentException("Invalid source record result");
        if(result!=null){var decoded=CommandResult.decode(result);if(decoded.status()!=CommandResult.Status.SUCCESS && decoded.status()!=CommandResult.Status.CONFLICT && decoded.status()!=CommandResult.Status.SESSION_EXPIRED || !Arrays.equals(result,decoded.encode()))throw new IllegalArgumentException("Invalid source publication business result");result=result.clone();}
    }
    @Override public byte[] result(){return result==null?null:result.clone();}
    public byte[] encode(){byte[] input=request.encode();return ByteBuffer.allocate(16+input.length+(result==null?0:result.length)).putInt(MAGIC).putInt(kind.ordinal()).putInt(input.length).put(input).putInt(result==null?-1:result.length).put(result==null?new byte[0]:result).array();}
    public byte[] digest(){return OperatorUpload.sha256(encode());}
    public static boolean isRecord(byte[] bytes){return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC;}
    public static RatisSourceOutcomeRecord decode(byte[] bytes){if(bytes.length<16 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid source record bound");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=MAGIC)throw new IllegalArgumentException("Invalid source record tag");int kind=in.getInt();if(kind<0 || kind>=Kind.values().length)throw new IllegalArgumentException("Invalid source record kind");byte[] request=blob(in,NativeSourceOutcomeCredit.Request.MAX_BYTES,false);byte[] result=blob(in,CommandResult.MAX_BYTES,true);if(in.hasRemaining())throw new IllegalArgumentException("Trailing source record");var record=new RatisSourceOutcomeRecord(Kind.values()[kind],NativeSourceOutcomeCredit.Request.decode(request),result);if(!Arrays.equals(bytes,record.encode()))throw new IllegalArgumentException("Noncanonical source record");return record;}
    static byte[] blob(ByteBuffer in,int maximum,boolean nullable){if(in.remaining()<4)throw new IllegalArgumentException("Truncated source blob");int length=in.getInt();if(nullable && length==-1)return null;if(length<0 || length>maximum || length>in.remaining())throw new IllegalArgumentException("Invalid source blob bound");byte[] bytes=new byte[length];in.get(bytes);return bytes;}
    @Override public boolean equals(Object other){return other instanceof RatisSourceOutcomeRecord record && Arrays.equals(encode(),record.encode());}
    @Override public int hashCode(){return Arrays.hashCode(digest());}
}
