package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Exact control-only HOME/source closure proofs; there is no public ABSENT or result publication. */
public final class RatisHomeSourceClosingRpc {
    public static final int PREPARE=123,STATE=124,SOURCE_STATE=125;
    public static final int MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+NativeHomeSourceClosing.MAX_BYTES+OperationRerouteOrigin.MAX_BYTES+8;
    public static boolean kind(int kind){return kind>=PREPARE && kind<=SOURCE_STATE;}
    public static byte[] request(int kind,NativeHomeSourceClosing.Request request){if(kind!=PREPARE)throw new IllegalArgumentException("Invalid HOME closing action");return ScopedTransactionRpc.frame(kind,request.encode());}
    public static byte[] lookup(HomeIdentity identity){if(!identity.definition().reserved())throw new IllegalArgumentException("CLOSED lookup requires original reservation");return ScopedTransactionRpc.frame(STATE,identity.encode());}
    public record SourceQuery(NativeSourceClosing closing,OperationRerouteOrigin noCredit){
        public SourceQuery{Objects.requireNonNull(closing);}
        byte[] encode(){byte[] body=closing.encode();byte[] origin=noCredit==null?new byte[0]:noCredit.encode();return ByteBuffer.allocate(8+body.length+origin.length).putInt(body.length).put(body).putInt(origin.length).put(origin).array();}
    }
    public static byte[] source(NativeSourceClosing closing,OperationRerouteOrigin noCredit){return ScopedTransactionRpc.frame(SOURCE_STATE,new SourceQuery(closing,noCredit).encode());}
    public static SourceQuery sourceQuery(byte[] body){try{var b=ByteBuffer.wrap(body);int n=b.getInt();if(n<0 || n>NativeSourceClosing.MAX_BYTES || n>b.remaining()-4)throw new IllegalArgumentException("Invalid source CLOSE query");byte[] bytes=new byte[n];b.get(bytes);int m=b.getInt();if(m!=b.remaining() || m!=0 && m!=OperationRerouteOrigin.MAX_BYTES)throw new IllegalArgumentException("Invalid source CLOSED origin");byte[] origin=new byte[m];b.get(origin);return new SourceQuery(NativeSourceClosing.decode(bytes),m==0?null:OperationRerouteOrigin.decode(origin));}catch(java.nio.BufferUnderflowException malformed){throw new IllegalArgumentException("Truncated source CLOSE query",malformed);}}
    public static byte[] sourceAck(SourceQuery query){return OperatorUpload.sha256(query.encode());}
    public static void requireSourceAck(byte[] bytes,NativeSourceClosing closing,OperationRerouteOrigin noCredit){if(!Arrays.equals(bytes,sourceAck(new SourceQuery(closing,noCredit))))throw new IllegalArgumentException("Source CLOSED proof changed exact request");}
}
