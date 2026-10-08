package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class RangeSessionControlTest {
    final UUID cluster=new UUID(1,1), database=new UUID(1,2), session=new UUID(1,3), group=new UUID(1,4);
    final SessionEpochLedger issued=SessionEpochLedger.initial(cluster,database,10).issue(session,100);
    @Test void controlRoundTripsPreserveIdentityAndRejectNoncanonicalHeaders() {
        for (var control:List.of(RangeSessionControl.advance(issued),RangeSessionControl.bind(issued,issued.grants().getFirst(),32))) {
            assertEquals(control,RangeSessionControl.decode(control.encode()));
            var command=DistributedCommand.sessionControl(group,1,new byte[32],new OperationId(new UUID(9,9),2,8),control,7);
            var bytes=DistributedCommandCodec.encode(command); var copy=DistributedCommandCodec.decode(bytes);
            assertEquals(DistributedCommand.Kind.SESSION_CONTROL,copy.kind()); assertEquals(7,copy.acknowledgedThrough()); assertEquals(1,copy.minimumSessionEpoch());
            assertEquals(control,copy.sessionControl()); assertEquals(bytes.length,DistributedCommandCodec.encodedSize(command));
            assertArrayEquals(bytes,DistributedCommandCodec.encode(copy)); assertArrayEquals(command.payloadDigest(),copy.payloadDigest());
            for (int i=0;i<bytes.length;i++) {
                byte[] shortBytes=Arrays.copyOf(bytes,i);
                assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(shortBytes));
            }
            assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(Arrays.copyOf(bytes,bytes.length+1)));
            for (int offset:new int[]{92,96,108,112,116}) {
                byte[] malformed=bytes.clone(); ByteBuffer.wrap(malformed).putInt(offset,Integer.MAX_VALUE);
                assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(malformed));
            }
            var noAck=DistributedCommand.sessionControl(group,1,new byte[32],new OperationId(session,1,0),control,-1);
            assertEquals(-1,DistributedCommandCodec.decode(DistributedCommandCodec.encode(noAck)).acknowledgedThrough());
            for (long ack:new long[]{-2,8,Long.MAX_VALUE}) {
                assertThrows(IllegalArgumentException.class,() -> DistributedCommand.sessionControl(group,1,new byte[32],command.operationId(),control,ack));
            }
        }
    }
    @Test void authorityCutsRequireMonotonicIdentityTimeFloorAndGeneration() {
        var before=SessionAuthority.from(issued); var later=SessionAuthority.from(issued.advanceTime(100));
        assertDoesNotThrow(() -> later.requireAtLeast(before)); assertDoesNotThrow(() -> before.requireAtLeast(before));
        assertThrows(IllegalArgumentException.class,() -> before.requireAtLeast(later));
        assertThrows(IllegalArgumentException.class,() -> new SessionAuthority(cluster,database,before.generation(),11,1).requireAtLeast(before));
        assertThrows(IllegalArgumentException.class,() -> new SessionAuthority(cluster,database,10,9,1).requireAtLeast(before));
        assertThrows(IllegalArgumentException.class,() -> new SessionAuthority(UUID.randomUUID(),database,10,100,2).requireAtLeast(before));
        assertFalse(later.permits(issued.grants().getFirst()));
        assertThrows(IllegalArgumentException.class,() -> new RangeSessionControl(later,issued.grants().getFirst(),32));
        assertDoesNotThrow(() -> RangeSessionControl.advance(issued.advanceTime(100)));
    }
    @Test void bindingsRequireExactIssuedLiveGrantAndBoundedDeclaredWindow() {
        var grant=issued.grants().getFirst();
        for (int window:new int[]{1,RangeSessionControl.MAX_OUTSTANDING}) assertNotNull(RangeSessionControl.bind(issued,grant,window));
        for (int window:new int[]{-1,0,RangeSessionControl.MAX_OUTSTANDING+1}) {
            assertThrows(IllegalArgumentException.class,() -> RangeSessionControl.bind(issued,grant,window));
        }
        assertThrows(IllegalArgumentException.class,() -> RangeSessionControl.bind(issued,new SessionEpochLedger.Grant(1,session,10,99),1));
        assertThrows(IllegalArgumentException.class,() -> RangeSessionControl.bind(issued.advanceTime(100),grant,1));
        assertThrows(IllegalArgumentException.class,() -> new RangeSessionControl(SessionAuthority.from(issued),null,1));
        assertThrows(IllegalArgumentException.class,() -> DistributedCommand.sessionControl(group,1,new byte[32],new OperationId(session,0,0),RangeSessionControl.advance(issued),-1));
    }
}
