package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RejectedTransactionCodecTest {
    private static final UUID GROUP=new UUID(0,1), COLUMN=new UUID(0,2), SESSION=new UUID(0,3), OWNER=new UUID(0,4);
    private static final OperationId OPERATION=new OperationId(SESSION,2,3);
    private static TransactionAcceptance acceptance() {
        return new TransactionAcceptance(new ApplyPosition(1,7),2,OWNER,new TransactionScope(List.of(TransactionScope.Span.range(COLUMN,null,null))),new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode());
    }
    private static DistributedCommand accepted(boolean bucket,long ack) {
        var mutation=bucket ? Mutation.bucketPut(COLUMN,new byte[] {7},List.of(new byte[] {(byte)255},new byte[0]),new byte[] {9},Mutation.ReturnMode.NONE)
                : new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {7},new byte[] {9});
        var base=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(mutation),List.of(),ack);
        return DistributedCommand.acceptedTransaction(base,acceptance());
    }
    @Test void compactRejectionKeepsOriginalReceiptIdentityAndHashesItsOwnCurrentEncoding() throws Exception {
        for (boolean bucket:new boolean[] {false,true}) for (long ack:new long[] {-1,0,2}) {
            var original=accepted(bucket,ack); var rejected=DistributedCommand.rejectAcceptedTransaction(original); byte[] bytes=DistributedCommandCodec.encode(rejected);
            assertEquals(DistributedCommand.Kind.COMMIT_REJECTED,rejected.kind()); assertEquals(1,ByteBuffer.wrap(bytes).getInt()); assertEquals(141,bytes.length);
            assertEquals(original.groupId(),rejected.groupId()); assertEquals(original.rangeEpoch(),rejected.rangeEpoch()); assertEquals(original.operationId(),rejected.operationId());
            assertArrayEquals(original.schemaDigest(),rejected.schemaDigest()); assertEquals(ack,rejected.acknowledgedThrough()); assertFalse(rejected.hasBucketData());
            assertTrue(rejected.mutations().isEmpty()); assertTrue(rejected.predicates().isEmpty()); assertNull(rejected.acceptance()); assertNull(rejected.sessionControl()); assertEquals(-1,rejected.minimumSessionEpoch());
            assertArrayEquals(original.payloadDigest(),rejected.receiptDigest()); assertArrayEquals(original.payloadDigest(),rejected.rejectionDigest());
            assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(bytes),rejected.payloadDigest()); assertFalse(Arrays.equals(rejected.payloadDigest(),rejected.receiptDigest()));
            var copy=DistributedCommandCodec.decode(bytes); assertArrayEquals(bytes,DistributedCommandCodec.encode(copy)); assertArrayEquals(rejected.receiptDigest(),copy.receiptDigest());
        }
    }
    @Test void digestOwnershipAndSemanticSourceRestrictionsAreStrict() {
        var original=accepted(true,1); var rejected=DistributedCommand.rejectAcceptedTransaction(original); byte[] before=DistributedCommandCodec.encode(rejected);
        rejected.receiptDigest()[0]^=1; rejected.rejectionDigest()[0]^=1; rejected.schemaDigest()[0]^=1; original.acceptance().result()[0]^=1;
        assertArrayEquals(before,DistributedCommandCodec.encode(rejected)); assertArrayEquals(original.payloadDigest(),rejected.receiptDigest());
        assertNull(original.rejectionDigest()); assertFalse(original.transactionRejection()); assertArrayEquals(original.payloadDigest(),original.receiptDigest());
        var raw=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[0])));
        var floor=DistributedCommand.advanceSessionEpochFloor(GROUP,1,new byte[32],OPERATION,1);
        var control=DistributedCommand.sessionControl(GROUP,1,new byte[32],OPERATION,new RangeSessionControl(new SessionAuthority(new UUID(1,1),new UUID(1,2),1,10,1),null,0),-1);
        for (var source:List.of(raw,floor,control,rejected)) assertThrows(IllegalArgumentException.class,() -> DistributedCommand.rejectAcceptedTransaction(source));
    }
    @Test void strictRejectionShapeRejectsTruncationExtraItemsAndMalformedAck() {
        byte[] valid=DistributedCommandCodec.encode(DistributedCommand.rejectAcceptedTransaction(accepted(false,0)));
        for (int length=0;length<valid.length;length++) { byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(Arrays.copyOf(valid,valid.length+1)));
        for (int offset:new int[] {101,105}) for (int count:new int[] {-1,1,Integer.MAX_VALUE}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,count); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        for (long ack:new long[] {-2,3,Long.MAX_VALUE}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putLong(93,ack); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        for (int offset:new int[] {21,77,85}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putLong(offset,-1); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        byte[] wrongKind=valid.clone(); wrongKind[4]=1; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(wrongKind));
        byte[] business=DistributedCommandCodec.encode(accepted(false,0)); business[4]=5; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(business));
    }
    @Test void maximumAcceptedBodyIsDiscardedWithoutGrowingTheRejectionEnvelope() {
        byte[] value=new byte[DistributedCommandCodec.MAX_ENCODED_BYTES-224];
        var base=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],value)));
        var original=DistributedCommand.acceptedTransaction(base,acceptance()); assertEquals(DistributedCommandCodec.MAX_ENCODED_BYTES,DistributedCommandCodec.encodedSize(original));
        var rejected=DistributedCommand.rejectAcceptedTransaction(original); assertEquals(141,DistributedCommandCodec.encode(rejected).length); assertArrayEquals(original.payloadDigest(),rejected.receiptDigest());
    }
}
