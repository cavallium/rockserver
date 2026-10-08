package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RangeStateViewTest {
    private static final UUID GROUP=new UUID(0,1), DOMAIN=new UUID(0,2), CLUSTER=new UUID(0,3), DATABASE=new UUID(0,4);
    private static final UUID COLUMN=new UUID(0,7), HIGH_COLUMN=new UUID(Long.MIN_VALUE,0);
    private static final SessionAuthority AUTHORITY=new SessionAuthority(CLUSTER,DATABASE,2,20,1);
    private static RangeStateView view(ApplyPosition position,SessionAuthority authority) {
        return new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN,HIGH_COLUMN),new byte[] {1,2},new byte[] {3,4},position,authority);
    }
    private static RangeStateView minimal() {
        return new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),null,null,null,null);
    }

    @Test void roundTripAndCanonicalUnsignedColumnsRetainDefensiveCopies() {
        byte[] schema=new byte[32], lower={1,2}, upper={3,4}; var columns=new HashSet<>(Set.of(HIGH_COLUMN,COLUMN));
        var original=new RangeStateView(GROUP,1,DOMAIN,schema,columns,lower,upper,new ApplyPosition(2,3),AUTHORITY);
        schema[0]=99; lower[0]=99; upper[0]=99; columns.clear();
        original.schemaDigest()[0]=55; original.lowerInclusive()[0]=55; original.upperExclusive()[0]=55;
        assertEquals(view(new ApplyPosition(2,3),AUTHORITY),original);
        assertEquals(0,original.schemaDigest()[0]); assertArrayEquals(new byte[] {1,2},original.lowerInclusive());
        assertArrayEquals(new byte[] {3,4},original.upperExclusive());
        assertThrows(UnsupportedOperationException.class,() -> original.columns().clear());
        byte[] encoded=original.encode();
        assertEquals(COLUMN,new UUID(ByteBuffer.wrap(encoded).getLong(84),ByteBuffer.wrap(encoded).getLong(92)));
        var decoded=RangeStateView.decode(encoded); assertEquals(original,decoded); assertEquals(original.hashCode(),decoded.hashCode());
        assertArrayEquals(encoded,decoded.encode()); encoded[0]=0; assertEquals(original,decoded);
        for (ApplyPosition position:new ApplyPosition[] {null,new ApplyPosition(0,0),new ApplyPosition(2,3)}) {
            for (SessionAuthority authority:new SessionAuthority[] {null,AUTHORITY}) {
                var value=view(position,authority); assertEquals(value,RangeStateView.decode(value.encode()));
            }
        }
        assertEquals(minimal(),RangeStateView.decode(minimal().encode()));
    }

    @Test void queryHeaderIsExactlyEightBytesAndRejectsUnrecognizedVersions() {
        byte[] request=RangeStateView.request(); assertEquals(8,request.length); assertTrue(RangeStateView.isRequest(request));
        assertFalse(RangeStateView.isRequest(null)); assertFalse(RangeStateView.isRequest(new byte[8]));
        assertFalse(RangeStateView.isRequest(Arrays.copyOf(request,7))); assertFalse(RangeStateView.isRequest(Arrays.copyOf(request,9)));
        request[7]=2; assertFalse(RangeStateView.isRequest(request)); assertTrue(RangeStateView.isRequest(RangeStateView.request()));
        assertFalse(RangeStateView.isRequest(minimal().encode()));
        assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(RangeStateView.request()));
    }

    @Test void malformedTruncatedNoncanonicalAndOversizedViewsAreRejected() {
        byte[] valid=view(new ApplyPosition(2,3),AUTHORITY).encode();
        for (int length=0;length<valid.length;length++) {
            byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(truncated));
        }
        assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(Arrays.copyOf(valid,valid.length+1)));
        for (int offset:new int[] {0,4,80,116,122}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,Integer.MAX_VALUE);
            assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(bad));
        }
        for (int count:new int[] {0,-1,257}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(80,count); assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(bad));
        }
        for (int offset:new int[] {116,122}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,-2); assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(bad));
        }
        byte[] duplicate=valid.clone(); System.arraycopy(duplicate,84,duplicate,100,16);
        assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(duplicate));
        byte[] unsorted=valid.clone(); System.arraycopy(valid,84,unsorted,100,16); System.arraycopy(valid,100,unsorted,84,16);
        assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(unsorted));
        for (int offset:new int[] {128,145}) {
            byte[] bad=valid.clone(); bad[offset]=2; assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(bad));
        }
        for (int offset:new int[] {24,129,137}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putLong(offset,-1); assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(bad));
        }
        byte[] binding=valid.clone(); ByteBuffer.wrap(binding).putInt(150,1);
        assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(binding));
        assertThrows(IllegalArgumentException.class,() -> RangeStateView.decode(new byte[RangeStateView.MAX_BYTES+1]));
        byte[] large=new byte[RangeStateView.MAX_BYTES-110];
        var maximum=new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),large,null,null,null);
        assertEquals(RangeStateView.MAX_BYTES,maximum.encode().length);
        assertEquals(maximum,RangeStateView.decode(maximum.encode()));
        assertThrows(IllegalArgumentException.class,() -> new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),large,new byte[] {1},null,null));
        assertThrows(IllegalArgumentException.class,() -> new RangeStateView(GROUP,-1,DOMAIN,new byte[32],Set.of(COLUMN),null,null,null,null));
        assertThrows(IllegalArgumentException.class,() -> new RangeStateView(GROUP,1,DOMAIN,new byte[31],Set.of(COLUMN),null,null,null,null));
        assertThrows(IllegalArgumentException.class,() -> new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(),null,null,null,null));
        assertThrows(IllegalArgumentException.class,() -> new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),new byte[] {(byte)255},new byte[] {1},null,null));
        assertThrows(IllegalArgumentException.class,() -> new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),new byte[] {1},new byte[] {1},null,null));
    }

    @Test void matchingScopeAndServingRequireARealAppliedTermAndDominatingFullAuthority() {
        var replica=new ReplicaDescriptor(new UUID(0,10),new UUID(0,11),new UUID(0,12),"peer","127.0.0.1:9000",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(new UUID(0,13),GROUP,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory=new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,new byte[32],0,Set.of(COLUMN),0,List.of(range));
        var serving=new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),null,null,new ApplyPosition(2,3),AUTHORITY);
        assertDoesNotThrow(() -> serving.requireMatches(directory,range)); assertDoesNotThrow(() -> serving.requireServing(AUTHORITY));
        assertDoesNotThrow(() -> serving.requireServing(new SessionAuthority(CLUSTER,DATABASE,1,10,1)));
        for (var target:List.of(new SessionAuthority(CLUSTER,DATABASE,3,30,1),new SessionAuthority(CLUSTER,DATABASE,2,21,1),
                new SessionAuthority(CLUSTER,DATABASE,2,20,2),new SessionAuthority(new UUID(9,9),DATABASE,1,10,1))) {
            assertThrows(IllegalStateException.class,() -> serving.requireServing(target));
        }
        for (var notServing:List.of(minimal(),new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),null,null,new ApplyPosition(0,3),AUTHORITY),
                new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),null,null,new ApplyPosition(2,3),null))) {
            assertThrows(IllegalStateException.class,() -> notServing.requireServing(AUTHORITY));
        }
        for (var mismatch:List.of(
                new RangeStateView(new UUID(9,9),1,DOMAIN,new byte[32],Set.of(COLUMN),null,null,null,null),
                new RangeStateView(GROUP,2,DOMAIN,new byte[32],Set.of(COLUMN),null,null,null,null),
                new RangeStateView(GROUP,1,new UUID(9,9),new byte[32],Set.of(COLUMN),null,null,null,null),
                new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(HIGH_COLUMN),null,null,null,null),
                new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),new byte[] {1},null,null,null),
                new RangeStateView(GROUP,1,DOMAIN,new byte[32],Set.of(COLUMN),null,null,null,new SessionAuthority(CLUSTER,new UUID(9,9),2,20,1)))) {
            assertThrows(IllegalArgumentException.class,() -> mismatch.requireMatches(directory,range));
        }
        byte[] differentSchema=new byte[32]; differentSchema[0]=1;
        assertThrows(IllegalArgumentException.class,() -> new RangeStateView(GROUP,1,DOMAIN,differentSchema,Set.of(COLUMN),null,null,null,null).requireMatches(directory,range));
    }
}
