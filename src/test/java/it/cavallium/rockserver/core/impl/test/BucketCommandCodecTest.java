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
    private static Mutation bucket() { return Mutation.bucketPut(COLUMN,new byte[] {9},List.of(new byte[] {(byte)0xff},new byte[0]),new byte[] {7},Mutation.ReturnMode.PREVIOUS); }
    private static DistributedCommand command(long ack) {
        return DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,
                List.of(bucket(),new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {1},new byte[] {2}),
                        Mutation.bucketDelete(COLUMN,new byte[] {9},List.of(new byte[] {3}),Mutation.ReturnMode.CURRENT)),
                List.of(new KeyPredicate(COLUMN,new byte[] {1},null),KeyPredicate.bucketMember(COLUMN,new byte[] {9},List.of(new byte[] {(byte)0xff},new byte[0]),new byte[] {7})),ack);
    }

    @Test void mixedV6RoundTripsWithOptionalAckExactSizeAndFrozenDigest() throws Exception {
        for (long ack:new long[] {-1,0,2}) {
            var original=command(ack); byte[] encoded=DistributedCommandCodec.encode(original); var decoded=DistributedCommandCodec.decode(encoded);
            assertEquals(6,decoded.formatVersion()); assertEquals(ack,decoded.acknowledgedThrough()); assertEquals(-1,decoded.minimumSessionEpoch()); assertNull(decoded.sessionControl());
            assertEquals(original.mutations(),decoded.mutations()); assertEquals(original.predicates(),decoded.predicates());
            assertArrayEquals(encoded,DistributedCommandCodec.encode(decoded)); assertEquals(encoded.length,DistributedCommandCodec.encodedSize(original));
            assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(encoded),original.payloadDigest()); assertArrayEquals(original.payloadDigest(),decoded.payloadDigest());
            assertEquals(5,encoded[108]); assertTrue(decoded.mutations().getFirst().bucket()); assertFalse(decoded.mutations().get(1).bucket());
            assertNull(decoded.mutations().getLast().value()); assertTrue(decoded.predicates().getLast().bucket());
        }
        var onlyPredicate=DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[0])),
                List.of(KeyPredicate.bucketMember(COLUMN,new byte[0],List.of(new byte[0]),null)),-1);
        assertArrayEquals(DistributedCommandCodec.encode(onlyPredicate),DistributedCommandCodec.encode(DistributedCommandCodec.decode(DistributedCommandCodec.encode(onlyPredicate))));
    }

    @Test void memberInputsAndAccessorsCannotChangeCommandIdentityOrEquality() {
        byte[] physical={9}, tail={(byte)0xff}, value={7}, expected={8}, schema=new byte[32]; var members=new ArrayList<>(List.of(tail,new byte[0]));
        var mutation=Mutation.bucketPut(COLUMN,physical,members,value,Mutation.ReturnMode.CURRENT);
        var predicate=KeyPredicate.bucketMember(COLUMN,physical,members,expected);
        var command=DistributedCommand.bucketData(GROUP,1,schema,OPERATION,List.of(mutation),List.of(predicate),-1);
        byte[] frozen=DistributedCommandCodec.encode(command), digest=command.payloadDigest();
        physical[0]=0; tail[0]=0; value[0]=0; expected[0]=0; schema[0]=1; members.clear();
        mutation.memberKeys().getFirst()[0]=1; predicate.memberKeys().getFirst()[0]=1;
        assertThrows(UnsupportedOperationException.class,() -> mutation.memberKeys().clear()); assertThrows(UnsupportedOperationException.class,() -> predicate.memberKeys().clear());
        assertArrayEquals(frozen,DistributedCommandCodec.encode(command)); assertArrayEquals(digest,command.payloadDigest());
        var copy=DistributedCommandCodec.decode(frozen); assertEquals(mutation,copy.mutations().getFirst()); assertEquals(mutation.hashCode(),copy.mutations().getFirst().hashCode());
        assertEquals(predicate,copy.predicates().getFirst()); assertEquals(predicate.hashCode(),copy.predicates().getFirst().hashCode());
        assertNotEquals(mutation,Mutation.bucketPut(COLUMN,new byte[] {9},List.of(new byte[] {1},new byte[0]),new byte[] {7},Mutation.ReturnMode.CURRENT));
        assertNotEquals(predicate,new KeyPredicate(COLUMN,new byte[] {9},new byte[] {8}));
        assertTrue(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[0]).memberKeys().isEmpty());
    }

    @Test void legacyVersionsRemainByteStableAndCannotAdmitBucketItems() {
        var raw=new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {1},new byte[] {2});
        var v2=new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(raw));
        byte[] oldV2=DistributedCommandCodec.encode(v2), v1=new byte[oldV2.length-4];
        System.arraycopy(oldV2,0,v1,0,96); ByteBuffer.wrap(v1).putInt(1); System.arraycopy(oldV2,100,v1,96,oldV2.length-100);
        var authority=new SessionAuthority(new UUID(0,4),new UUID(0,5),1,10,1);
        var oldVersions=new ArrayList<byte[]>(); oldVersions.add(v1); oldVersions.add(oldV2);
        oldVersions.add(DistributedCommandCodec.encode(new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(raw),List.of(),0)));
        oldVersions.add(DistributedCommandCodec.encode(DistributedCommand.advanceSessionEpochFloor(GROUP,1,new byte[32],OPERATION,1)));
        oldVersions.add(DistributedCommandCodec.encode(DistributedCommand.sessionControl(GROUP,1,new byte[32],OPERATION,new RangeSessionControl(authority,null,0),0)));
        for (byte[] encoded:oldVersions) assertArrayEquals(encoded,DistributedCommandCodec.encode(DistributedCommandCodec.decode(encoded)));
        for (byte[] encoded:oldVersions.subList(0,3)) {
            byte[] invalid=encoded.clone(); int version=ByteBuffer.wrap(invalid).getInt(); invalid[version==1 ? 96 : version==2 ? 100 : 108]=5;
            assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(invalid));
        }
        assertThrows(IllegalArgumentException.class,() -> new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(bucket())));
        assertThrows(IllegalArgumentException.class,() -> new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(bucket()),List.of(),0));
        assertThrows(IllegalArgumentException.class,() -> new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(raw),List.of(KeyPredicate.bucketMember(COLUMN,new byte[0],List.of(new byte[0]),null))));
        assertThrows(IllegalArgumentException.class,() -> new Mutation(Mutation.Kind.BUCKET_PUT,COLUMN,new byte[0],new byte[0]));
        assertThrows(IllegalArgumentException.class,() -> new Mutation(Mutation.Kind.BUCKET_DELETE,COLUMN,new byte[0],null));
        byte[] physicalPredicate=DistributedCommandCodec.encode(new DistributedCommand(GROUP,1,new byte[32],OPERATION,List.of(raw),List.of(new KeyPredicate(COLUMN,new byte[0],null))));
        physicalPredicate[128]=2; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(physicalPredicate));
    }

    @Test void malformedVersionsTagsMembersLengthsAndEveryTruncationAreRejected() {
        var single=DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,List.of(Mutation.bucketPut(COLUMN,new byte[] {9},List.of(new byte[] {1}),new byte[] {7},Mutation.ReturnMode.NONE)),List.of(),-1);
        byte[] valid=DistributedCommandCodec.encode(single);
        for (int length=0;length<valid.length;length++) { byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(Arrays.copyOf(valid,valid.length+1)));
        // Header108, mutation26 + one physical key + one value, then member count/ushort/raw key.
        int memberCount=136;
        for (int count:new int[] {-1,0,257,Integer.MAX_VALUE}) {
            byte[] invalid=valid.clone(); ByteBuffer.wrap(invalid).putInt(memberCount,count); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(invalid));
        }
        byte[] tooLong=valid.clone(); ByteBuffer.wrap(tooLong).putChar(memberCount+4,(char)65535); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(tooLong));
        for (int offset:new int[] {0,92,96,126,131}) {
            byte[] invalid=valid.clone(); ByteBuffer.wrap(invalid).putInt(offset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(invalid));
        }
        for (int offset:new int[] {108,109}) { byte[] invalid=valid.clone(); invalid[offset]=99; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(invalid)); }
        byte[] nullPut=valid.clone(); ByteBuffer.wrap(nullPut).putInt(131,-1); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(nullPut));
        byte[] deletion=valid.clone(); deletion[108]=6; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(deletion));
        byte[] badPredicate=DistributedCommandCodec.encode(DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,List.of(bucket()),
                List.of(KeyPredicate.bucketMember(COLUMN,new byte[0],List.of(new byte[0]),null)),-1));
        int predicateOffset=108+26+1+1+4+2+1+2; badPredicate[predicateOffset]=3;
        assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(badPredicate));
    }

    @Test void factoriesEnforceOptionalAckBucketPresenceAndComponentBudgets() {
        for (long ack:new long[] {-2,3,Long.MAX_VALUE}) assertThrows(IllegalArgumentException.class,() -> command(ack));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,List.of(),List.of(),-1));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,
                List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[0])),List.of(),-1));
        for (List<byte[]> members:List.of(List.<byte[]>of(),Collections.nCopies(257,new byte[0]),List.of(new byte[65536]))) {
            assertThrows(IllegalArgumentException.class,() -> Mutation.bucketPut(COLUMN,new byte[0],members,new byte[0],Mutation.ReturnMode.NONE));
            assertThrows(IllegalArgumentException.class,() -> KeyPredicate.bucketMember(COLUMN,new byte[0],members,null));
        }
        assertThrows(IllegalArgumentException.class,() -> Mutation.bucketPut(COLUMN,new byte[0],List.of(new byte[0]),null,Mutation.ReturnMode.NONE));
        var members=Collections.nCopies(33,new byte[65535]);
        assertThrows(IllegalArgumentException.class,() -> Mutation.bucketDelete(COLUMN,new byte[0],members,Mutation.ReturnMode.NONE));
        byte[] maximumTail=new byte[65535]; var maximum=DistributedCommand.bucketData(GROUP,1,new byte[32],OPERATION,
                List.of(Mutation.bucketDelete(COLUMN,new byte[0],List.of(maximumTail),Mutation.ReturnMode.NONE)),List.of(),-1);
        assertArrayEquals(DistributedCommandCodec.encode(maximum),DistributedCommandCodec.encode(DistributedCommandCodec.decode(DistributedCommandCodec.encode(maximum))));
    }
}
