package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BucketCommandCodecTest {
    private static final UUID GROUP=new UUID(0,1), COLUMN=new UUID(0,2), SESSION=new UUID(0,3);
    private static final OperationId OPERATION=new OperationId(SESSION,2,3);
    private static Mutation bucket() { return Mutation.bucketPut(COLUMN,new byte[] {9},List.of(new byte[] {(byte)255},new byte[0]),new byte[] {7},Mutation.ReturnMode.PREVIOUS); }
    private static DistributedCommand command(long ack) {
        return new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(bucket(),new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {1},new byte[] {2}),
                Mutation.bucketDelete(COLUMN,new byte[] {9},List.of(new byte[] {3}),Mutation.ReturnMode.CURRENT)),
                List.of(new KeyPredicate(COLUMN,new byte[] {1},null),KeyPredicate.bucketMember(COLUMN,new byte[] {9},List.of(new byte[] {(byte)255},new byte[0]),new byte[] {7})),ack);
    }
    @Test void unifiedDataRoundTripsMixedItemsOptionalAckExactSizeAndDigest() throws Exception {
        for (long ack:new long[] {-1,0,2}) {
            var original=command(ack); byte[] encoded=DistributedCommandCodec.encode(original); var decoded=DistributedCommandCodec.decode(encoded);
            assertEquals(DistributedCommand.Kind.DATA,decoded.kind()); assertEquals(ack,decoded.acknowledgedThrough()); assertEquals(-1,decoded.minimumSessionEpoch());
            assertTrue(decoded.hasBucketData()); assertEquals(original.mutations(),decoded.mutations()); assertEquals(original.predicates(),decoded.predicates());
            assertArrayEquals(encoded,DistributedCommandCodec.encode(decoded)); assertEquals(encoded.length,DistributedCommandCodec.encodedSize(original));
            assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(encoded),original.payloadDigest()); assertEquals(5,encoded[109]);
        }
        var predicateOnlyBucket=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[0])),
                List.of(KeyPredicate.bucketMember(COLUMN,new byte[0],List.of(new byte[0]),null)));
        assertTrue(DistributedCommandCodec.decode(DistributedCommandCodec.encode(predicateOnlyBucket)).hasBucketData());
    }
    @Test void deepMemberOwnershipAndEqualityPreserveFrozenIdentity() {
        byte[] physical={9}, tail={(byte)255}, value={7}, expected={8}; var members=new ArrayList<>(List.of(tail,new byte[0]));
        var mutation=Mutation.bucketPut(COLUMN,physical,members,value,Mutation.ReturnMode.CURRENT); var predicate=KeyPredicate.bucketMember(COLUMN,physical,members,expected);
        var command=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(mutation),List.of(predicate)); byte[] wire=DistributedCommandCodec.encode(command);
        physical[0]=0; tail[0]=0; value[0]=0; expected[0]=0; members.clear(); mutation.memberKeys().getFirst()[0]=1; predicate.memberKeys().getFirst()[0]=1;
        assertThrows(UnsupportedOperationException.class,() -> mutation.memberKeys().clear()); assertArrayEquals(wire,DistributedCommandCodec.encode(command));
        var copy=DistributedCommandCodec.decode(wire); assertEquals(mutation,copy.mutations().getFirst()); assertEquals(mutation.hashCode(),copy.mutations().getFirst().hashCode());
        assertEquals(predicate,copy.predicates().getFirst()); assertEquals(predicate.hashCode(),copy.predicates().getFirst().hashCode());
        assertNotEquals(mutation,Mutation.bucketPut(COLUMN,new byte[] {9},List.of(new byte[] {1},new byte[0]),new byte[] {7},Mutation.ReturnMode.CURRENT));
        assertNotEquals(predicate,new KeyPredicate(COLUMN,new byte[] {9},new byte[] {8}));
    }
    @Test void malformedTagsCountsLengthsAndEveryTruncationRejectBeforeUse() {
        byte[] valid=DistributedCommandCodec.encode(new DistributedCommand(GROUP,1,new byte[32],OPERATION,
                List.of(Mutation.bucketPut(COLUMN,new byte[] {9},List.of(new byte[] {1}),new byte[] {7},Mutation.ReturnMode.NONE))));
        for (int length=0;length<valid.length;length++) { byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(Arrays.copyOf(valid,valid.length+1)));
        for (int count:new int[] {-1,0,257,Integer.MAX_VALUE}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(137,count); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        byte[] tooLong=valid.clone(); ByteBuffer.wrap(tooLong).putChar(141,(char)65535); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(tooLong));
        for (int offset:new int[] {101,105,127,132}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        for (int offset:new int[] {109,110}) { byte[] bad=valid.clone(); bad[offset]=99; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        byte[] nullPut=valid.clone(); ByteBuffer.wrap(nullPut).putInt(132,-1); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(nullPut));
        byte[] deletion=valid.clone(); deletion[109]=6; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(deletion));
        byte[] predicate=DistributedCommandCodec.encode(new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(bucket()),List.of(KeyPredicate.bucketMember(COLUMN,new byte[0],List.of(new byte[0]),null))));
        predicate[146]=3; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(predicate));
    }
    @Test void componentBudgetsAndExplicitFactoryShapesAreBounded() {
        for (long ack:new long[] {-2,3,Long.MAX_VALUE}) assertThrows(IllegalArgumentException.class,() -> command(ack));
        for (List<byte[]> members:List.of(List.<byte[]>of(),Collections.nCopies(257,new byte[0]),List.of(new byte[65536]))) {
            assertThrows(IllegalArgumentException.class,() -> Mutation.bucketPut(COLUMN,new byte[0],members,new byte[0],Mutation.ReturnMode.NONE));
            assertThrows(IllegalArgumentException.class,() -> KeyPredicate.bucketMember(COLUMN,new byte[0],members,null));
        }
        assertThrows(IllegalArgumentException.class,() -> new Mutation(Mutation.Kind.BUCKET_PUT,COLUMN,new byte[0],new byte[0]));
        assertThrows(IllegalArgumentException.class,() -> Mutation.bucketPut(COLUMN,new byte[0],List.of(new byte[0]),null,Mutation.ReturnMode.NONE));
        assertThrows(IllegalArgumentException.class,() -> Mutation.bucketDelete(COLUMN,new byte[0],Collections.nCopies(33,new byte[65535]),Mutation.ReturnMode.NONE));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[0])),List.of(),-1));
        var maximum=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(Mutation.bucketDelete(COLUMN,new byte[0],List.of(new byte[65535]),Mutation.ReturnMode.NONE)));
        assertArrayEquals(DistributedCommandCodec.encode(maximum),DistributedCommandCodec.encode(DistributedCommandCodec.decode(DistributedCommandCodec.encode(maximum))));
    }
}
