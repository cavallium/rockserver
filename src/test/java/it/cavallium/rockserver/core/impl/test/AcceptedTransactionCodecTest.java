package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AcceptedTransactionCodecTest {
    private static final UUID GROUP=new UUID(0,1), COLUMN=new UUID(0,2), SESSION=new UUID(0,3), OWNER=new UUID(0,4), OTHER=new UUID(0,5);
    private static final OperationId OPERATION=new OperationId(SESSION,2,3);
    private static TransactionScope scope(TransactionScope.Span... spans) { return new TransactionScope(List.of(spans)); }
    private static TransactionScope all() { return scope(TransactionScope.Span.range(COLUMN,null,null)); }
    private static byte[] success() { return new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode(); }
    private static TransactionAcceptance acceptance(TransactionScope scope,byte[] result) { return new TransactionAcceptance(new ApplyPosition(2,7),11,OWNER,scope,result); }
    private static DistributedCommand simple() {
        var base=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {7},new byte[] {9})));
        return DistributedCommand.acceptedTransaction(base,acceptance(scope(TransactionScope.Span.point(COLUMN,new byte[] {7})),success()));
    }

    @Test void deterministicAcceptedRoundTripPreservesOrderedMergeBucketOperandsAndAudit() throws Exception {
        var mutations=List.of(new Mutation(Mutation.Kind.MERGE,COLUMN,new byte[] {1},new byte[] {2},Mutation.ReturnMode.CURRENT),
                Mutation.bucketPut(COLUMN,new byte[] {9},List.of(new byte[] {(byte)255},new byte[0]),new byte[] {3},Mutation.ReturnMode.PREVIOUS),
                Mutation.bucketDelete(COLUMN,new byte[] {9},List.of(new byte[] {4}),Mutation.ReturnMode.NONE));
        var predicates=List.of(new KeyPredicate(COLUMN,new byte[] {1},new byte[] {5}),KeyPredicate.bucketMember(COLUMN,new byte[] {9},List.of(new byte[] {(byte)255},new byte[0]),null));
        var base=DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,mutations,predicates,1);
        byte[] result=new CommandResult(CommandResult.Status.SUCCESS,Arrays.asList(new byte[] {8},null)).encode();
        var audit=acceptance(all(),result); var accepted=DistributedCommand.acceptedTransaction(base,audit); byte[] encoded=DistributedCommandCodec.encode(accepted);
        var decoded=DistributedCommandCodec.decode(encoded); assertEquals(DistributedCommand.Kind.COMMIT_ACCEPTED,decoded.kind()); assertEquals(1,decoded.acknowledgedThrough());
        assertTrue(decoded.hasBucketData()); assertEquals(mutations,decoded.mutations()); assertEquals(predicates,decoded.predicates());
        assertEquals(audit,decoded.acceptance()); assertEquals(audit.hashCode(),decoded.acceptance().hashCode());
        assertEquals(encoded.length,DistributedCommandCodec.encodedSize(accepted)); assertArrayEquals(encoded,DistributedCommandCodec.encode(decoded));
        assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(encoded),decoded.payloadDigest());
        assertEquals(new ApplyPosition(2,7),decoded.acceptance().snapshot(),"Snapshot term binds origin, independently of later replay term");
        var onlyPredicate=DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {1},new byte[0])),
                List.of(KeyPredicate.bucketMember(COLUMN,new byte[] {9},List.of(new byte[0]),null)),-1);
        assertTrue(DistributedCommand.acceptedTransaction(onlyPredicate,acceptance(all(),success())).hasBucketData());
    }

    @Test void auditOwnsResultAndCanonicalScopeOrderInTheCurrentFormat() {
        byte[] key={7}, result=success(); var audit=acceptance(scope(TransactionScope.Span.point(COLUMN,key)),result);
        var base=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,key,new byte[] {9})));
        var accepted=DistributedCommand.acceptedTransaction(base,audit); byte[] wire=DistributedCommandCodec.encode(accepted), digest=accepted.payloadDigest();
        key[0]=0; result[0]=99; audit.result()[0]=99; audit.scope().spans().getFirst().lowerInclusive()[0]=0;
        assertArrayEquals(wire,DistributedCommandCodec.encode(accepted)); assertArrayEquals(digest,accepted.payloadDigest());
        assertThrows(UnsupportedOperationException.class,() -> audit.scope().spans().clear());
        var first=acceptance(scope(TransactionScope.Span.range(OTHER,null,null),TransactionScope.Span.point(COLUMN,new byte[] {7}),TransactionScope.Span.range(COLUMN,null,new byte[] {7})),success());
        var second=acceptance(scope(TransactionScope.Span.range(COLUMN,null,new byte[] {7}),TransactionScope.Span.point(COLUMN,new byte[] {7}),TransactionScope.Span.range(OTHER,null,null)),success());
        assertEquals(first,second); assertArrayEquals(DistributedCommandCodec.encode(DistributedCommand.acceptedTransaction(base,first)),DistributedCommandCodec.encode(DistributedCommand.acceptedTransaction(base,second)));
    }

    @Test void scopeMustCoverAllMutationsTombstonesAndPredicatesAndResultShapeMustMatch() {
        var point=new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {7},new byte[] {9});
        var wrongScope=acceptance(scope(TransactionScope.Span.point(COLUMN,new byte[] {8})),success());
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.acceptedTransaction(new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(point)),wrongScope));
        var predicate=new KeyPredicate(OTHER,new byte[] {1},null);
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.acceptedTransaction(new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(point),List.of(predicate)),acceptance(all(),success())));
        var deletion=new Mutation(Mutation.Kind.DELETE_RANGE,COLUMN,new byte[] {1},new byte[] {9});
        var base=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(deletion));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.acceptedTransaction(base,acceptance(scope(TransactionScope.Span.point(COLUMN,new byte[] {1})),success())));
        var accepted=DistributedCommand.acceptedTransaction(base,acceptance(scope(TransactionScope.Span.range(COLUMN,new byte[] {1},new byte[] {9})),success()));
        assertFalse(accepted.hasBucketData()); assertArrayEquals(DistributedCommandCodec.encode(accepted),DistributedCommandCodec.encode(DistributedCommandCodec.decode(DistributedCommandCodec.encode(accepted))));
        var returning=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.MERGE,COLUMN,new byte[] {7},new byte[] {9},Mutation.ReturnMode.CURRENT)));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.acceptedTransaction(returning,acceptance(all(),success())));
        assertThrows(IllegalArgumentException.class,() -> acceptance(all(),new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode()));
        assertThrows(IllegalArgumentException.class,() -> new TransactionAcceptance(new ApplyPosition(0,7),0,OWNER,all(),success()));
        assertThrows(IllegalArgumentException.class,() -> new TransactionAcceptance(new ApplyPosition(1,7),-1,OWNER,all(),success()));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.acceptedTransaction(DistributedCommand.advanceSessionEpochFloor(GROUP,1,new byte[32],OPERATION,1),acceptance(all(),success())));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.acceptedTransaction(new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(),List.of(),1),acceptance(all(),success())));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.acceptedTransaction(simple(),acceptance(all(),success())));
    }

    @Test void malformedAuditLengthsFlagsOrderResultAndEveryTruncationAreRejected() {
        byte[] valid=DistributedCommandCodec.encode(simple()); assertEquals(227,valid.length);
        for (int length=0;length<valid.length;length++) { byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(Arrays.copyOf(valid,valid.length+1)));
        for (int offset:new int[] {0,137,141,185,206,211,215,219,223}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad));
        }
        for (int offset:new int[] {145,153,161}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putLong(offset,-1); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        byte[] kind=valid.clone(); kind[189]=3; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(kind));
        byte[] missingPoint=valid.clone(); ByteBuffer.wrap(missingPoint).putInt(206,-1); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(missingPoint));
        byte[] foreign=valid.clone(); foreign[210]=8; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(foreign));
        byte[] conflict=valid.clone(); ByteBuffer.wrap(conflict).putInt(219,2); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(conflict));
        var base=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {7},new byte[] {9})));
        byte[] two=DistributedCommandCodec.encode(DistributedCommand.acceptedTransaction(base,acceptance(scope(TransactionScope.Span.point(COLUMN,new byte[] {7}),TransactionScope.Span.point(OTHER,new byte[] {7})),success())));
        byte[] first=Arrays.copyOfRange(two,189,215); System.arraycopy(two,215,two,189,26); System.arraycopy(first,0,two,215,26);
        assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(two));
    }

    @Test void fullEnvelopeBudgetIncludesScopeAndFrozenResult() {
        byte[] large=new byte[DistributedCommandCodec.MAX_ENCODED_BYTES-300];
        var result=new CommandResult(CommandResult.Status.SUCCESS,List.of(large)).encode();
        var audit=acceptance(all(),result);
        var small=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[0],Mutation.ReturnMode.PREVIOUS)));
        assertTrue(DistributedCommandCodec.encode(DistributedCommand.acceptedTransaction(small,audit)).length<=DistributedCommandCodec.MAX_ENCODED_BYTES);
        var oversized=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[300],Mutation.ReturnMode.PREVIOUS)));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.acceptedTransaction(oversized,audit));
        assertThrows(IllegalArgumentException.class,() -> acceptance(scope(TransactionScope.Span.point(COLUMN,new byte[TransactionScope.MAX_BOUND_BYTES])),success()));
    }
}
