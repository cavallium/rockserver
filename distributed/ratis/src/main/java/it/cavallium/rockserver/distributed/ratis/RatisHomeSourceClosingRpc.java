package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Exact control-only HOME/source closure proofs; there is no public ABSENT or result publication. */
public final class RatisHomeSourceClosingRpc {
    public static final int PREPARE=123,STATE=124,SOURCE_STATE=125;
    public static final int MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+NativeHomeSourceClosing.MAX_BYTES+HomeRecoveryReceipt.MAX_BYTES+OperationRerouteOrigin.MAX_BYTES+24;
    public static boolean kind(int kind){return kind>=PREPARE && kind<=SOURCE_STATE;}
    public static byte[] request(int kind,NativeHomeSourceClosing.Request request){if(kind!=PREPARE)throw new IllegalArgumentException("Invalid HOME closing action");return ScopedTransactionRpc.frame(kind,request.encode());}
    public static byte[] lookup(HomeIdentity identity){if(!identity.definition().reserved())throw new IllegalArgumentException("CLOSED lookup requires original reservation");return ScopedTransactionRpc.frame(STATE,identity.encode());}
    /** A fresh closed/no-credit qualification, optionally accompanied by the actual retained terminal HOME receipt. */
    public record ClosedReservation(NativeHomeSourceClosing closing,ApplyPosition cut,HomeRecoveryReceipt terminal){
        public static final int MAX_BYTES=24+NativeHomeSourceClosing.MAX_BYTES+HomeRecoveryReceipt.MAX_BYTES;
        public ClosedReservation{Objects.requireNonNull(closing);Objects.requireNonNull(cut);closing.requireCut(cut);if(terminal!=null){terminal.requireCut(cut);var r=terminal.recovery();if(terminal.result().status()!=CommandResult.Status.SUCCESS || r.action()!=HomeRecovery.Action.ABORT || !r.before().reserved() || r.next().phase()!=TransactionStatus.Phase.ABORT || !r.next().participants().isEmpty() || r.next().noEffectOutcome()==TransactionStatus.NoEffectOutcome.NONE)throw new IllegalArgumentException("CLOSED result is not a successful original no-effect HOME abort");}}
        public void requireIdentity(HomeIdentity identity){var request=closing.request();if(!identity.definition().reserved() || !request.home().equals(identity.groupId()) || request.homeEpoch()!=identity.rangeEpoch() || !Arrays.equals(request.schema(),identity.schemaDigest()) || !request.references(identity.definition().origin()))throw new IllegalArgumentException("CLOSED proof changed original HOME/source identity");if(terminal!=null){var r=terminal.recovery();var command=r.command();if(!r.before().equals(identity.definition()) || !command.groupId().equals(identity.groupId()) || command.rangeEpoch()!=identity.rangeEpoch() || !Arrays.equals(command.schemaDigest(),identity.schemaDigest()) || !command.mutations().getFirst().columnId().equals(identity.columnId()))throw new IllegalArgumentException("CLOSED terminal changed original HOME identity");}}
        public byte[] encode(){byte[] fence=closing.encode(),receipt=terminal==null?new byte[0]:terminal.encode();return ByteBuffer.allocate(24+fence.length+receipt.length).putLong(cut.term()).putLong(cut.index()).putInt(fence.length).put(fence).putInt(receipt.length).put(receipt).array();}
        public static ClosedReservation decode(byte[] bytes){try{if(bytes.length<24 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid CLOSED reservation proof bound");var b=ByteBuffer.wrap(bytes);var cut=new ApplyPosition(b.getLong(),b.getLong());int n=b.getInt();if(n<0 || n>NativeHomeSourceClosing.MAX_BYTES || n>b.remaining()-4)throw new IllegalArgumentException("Invalid CLOSED fence length");byte[] fence=new byte[n];b.get(fence);int m=b.getInt();if(m!=b.remaining() || m>HomeRecoveryReceipt.MAX_BYTES)throw new IllegalArgumentException("Invalid CLOSED terminal receipt length");byte[] receipt=new byte[m];b.get(receipt);return new ClosedReservation(NativeHomeSourceClosing.decode(fence),cut,m==0?null:HomeRecoveryReceipt.decode(receipt));}catch(java.nio.BufferUnderflowException malformed){throw new IllegalArgumentException("Truncated CLOSED proof",malformed);}}
    }
    public record SourceQuery(NativeSourceClosing closing,OperationRerouteOrigin noCredit){
        public SourceQuery{Objects.requireNonNull(closing);}
        byte[] encode(){byte[] body=closing.encode();byte[] origin=noCredit==null?new byte[0]:noCredit.encode();return ByteBuffer.allocate(8+body.length+origin.length).putInt(body.length).put(body).putInt(origin.length).put(origin).array();}
    }
    public static byte[] source(NativeSourceClosing closing,OperationRerouteOrigin noCredit){return ScopedTransactionRpc.frame(SOURCE_STATE,new SourceQuery(closing,noCredit).encode());}
    public static SourceQuery sourceQuery(byte[] body){try{var b=ByteBuffer.wrap(body);int n=b.getInt();if(n<0 || n>NativeSourceClosing.MAX_BYTES || n>b.remaining()-4)throw new IllegalArgumentException("Invalid source CLOSE query");byte[] bytes=new byte[n];b.get(bytes);int m=b.getInt();if(m!=b.remaining() || m!=0 && m!=OperationRerouteOrigin.MAX_BYTES)throw new IllegalArgumentException("Invalid source CLOSED origin");byte[] origin=new byte[m];b.get(origin);return new SourceQuery(NativeSourceClosing.decode(bytes),m==0?null:OperationRerouteOrigin.decode(origin));}catch(java.nio.BufferUnderflowException malformed){throw new IllegalArgumentException("Truncated source CLOSE query",malformed);}}
    public static byte[] sourceAck(SourceQuery query){return OperatorUpload.sha256(query.encode());}
    public static void requireSourceAck(byte[] bytes,NativeSourceClosing closing,OperationRerouteOrigin noCredit){if(!Arrays.equals(bytes,sourceAck(new SourceQuery(closing,noCredit))))throw new IllegalArgumentException("Source CLOSED proof changed exact request");}
}
