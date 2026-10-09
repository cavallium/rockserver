package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotFenceStatusTest {
    final SnapshotFenceTest helper=new SnapshotFenceTest();
    final ApplyPosition aReady=new ApplyPosition(1,2),bReady=new ApplyPosition(2,4);
    SnapshotFenceStatus.Capture capture(int n,ApplyPosition position) { return new SnapshotFenceStatus.Capture(new UUID(0,50+n),new UUID(0,60+n),position); }
    SnapshotFenceStatus ready() { return SnapshotFenceStatus.begin(helper.fence()).ready(helper.a,aReady).ready(helper.b,bReady); }
    SnapshotFenceStatus captured() { return ready().capture(helper.a,capture(1,new ApplyPosition(1,3))).capture(helper.b,capture(2,new ApplyPosition(2,5))); }
    @Test void everyReadyPrecedesAnyCaptureAndCompleteRequiresEveryCapture() {
        var initial=SnapshotFenceStatus.begin(helper.fence()); assertFalse(initial.allReady()); assertEquals(0,initial.generation());
        var one=initial.ready(helper.a,aReady); one.requireSuccessorOf(initial); assertSame(one,one.ready(helper.a,aReady));
        assertThrows(IllegalArgumentException.class,() -> one.capture(helper.a,capture(1,aReady)));
        assertThrows(IllegalArgumentException.class,one::complete);
        var ready=one.ready(helper.b,bReady); ready.requireSuccessorOf(one); assertTrue(ready.allReady()); assertEquals(2,ready.generation());
        assertThrows(IllegalArgumentException.class,() -> ready.capture(helper.a,capture(1,new ApplyPosition(1,1))));
        assertThrows(IllegalArgumentException.class,() -> ready.capture(helper.b,capture(2,new ApplyPosition(1,5))));
        var first=ready.capture(helper.a,capture(1,new ApplyPosition(1,3))); first.requireSuccessorOf(ready); assertSame(first,first.capture(helper.a,first.progress(helper.a).capture()));
        assertThrows(IllegalArgumentException.class,first::complete); assertThrows(IllegalArgumentException.class,() -> first.capture(helper.a,capture(9,new ApplyPosition(1,3))));
        var both=first.capture(helper.b,capture(2,new ApplyPosition(2,5))); both.requireSuccessorOf(first);
        var complete=both.complete(); complete.requireSuccessorOf(both); assertTrue(complete.terminal()); assertEquals(5,complete.generation());
        assertSame(complete,complete.complete()); assertSame(complete,complete.ready(helper.a,aReady)); assertSame(complete,complete.capture(helper.a,first.progress(helper.a).capture()));
        assertThrows(IllegalArgumentException.class,complete::abort); assertThrows(IllegalArgumentException.class,() -> ready.requireSuccessorOf(initial));
        assertThrows(IllegalArgumentException.class,() -> complete.requireSuccessorOf(complete));
    }
    @Test void abortFreezesPartialProgressAndTerminalDecisionsCannotBeOverturned() {
        var one=SnapshotFenceStatus.begin(helper.fence()).ready(helper.a,aReady); var abort=one.abort(); abort.requireSuccessorOf(one);
        assertTrue(abort.terminal()); assertSame(abort,abort.abort()); assertSame(abort,abort.ready(helper.a,aReady));
        assertThrows(IllegalArgumentException.class,() -> abort.ready(helper.b,bReady)); assertThrows(IllegalArgumentException.class,abort::complete);
        assertThrows(IllegalArgumentException.class,() -> abort.capture(helper.a,capture(1,new ApplyPosition(1,3))));
        assertThrows(IllegalArgumentException.class,() -> one.ready(helper.a,new ApplyPosition(1,3)));
        assertThrows(IllegalArgumentException.class,() -> one.ready(helper.b,new ApplyPosition(0,1)));
    }
    @Test void currentStatusCodecBindsDefinitionProofsAndDecisionDigest() {
        for (var status:List.of(SnapshotFenceStatus.begin(helper.fence()),ready(),captured(),captured().complete(),ready().abort())) {
            byte[] bytes=status.encode(); var decoded=SnapshotFenceStatus.decode(bytes); assertEquals(status,decoded); assertEquals(status.hashCode(),decoded.hashCode()); assertArrayEquals(status.digest(),decoded.digest());
            Arrays.fill(bytes,(byte)0); assertEquals(status,decoded); byte[] digest=status.digest(); digest[0]^=1; assertArrayEquals(status.digest(),decoded.digest());
        }
        assertFalse(Arrays.equals(captured().complete().digest(),captured().abort().digest()));
        byte[] bytes=captured().complete().encode();
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> SnapshotFenceStatus.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> SnapshotFenceStatus.decode(Arrays.copyOf(bytes,bytes.length+1)));
        byte[] phase=bytes.clone(); phase[4+helper.fence().encode().length]=3; assertThrows(IllegalArgumentException.class,() -> SnapshotFenceStatus.decode(phase));
        byte[] flags=bytes.clone(); flags[helper.fence().encode().length+SnapshotFenceStatus.HEADER_BYTES+16]=2; assertThrows(IllegalArgumentException.class,() -> SnapshotFenceStatus.decode(flags));
        byte[] length=bytes.clone(); ByteBuffer.wrap(length).putInt(0,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> SnapshotFenceStatus.decode(length));
    }
}
