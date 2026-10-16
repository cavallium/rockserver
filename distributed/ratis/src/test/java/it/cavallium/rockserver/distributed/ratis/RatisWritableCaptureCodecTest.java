package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisWritableCaptureCodecTest {
    final UUID owner=new UUID(0,10),snapshot=new UUID(0,11);
    final ApplyPosition position=new ApplyPosition(3,7);
    final SnapshotFence fence=new SnapshotFence(new UUID(0,1), new UUID(0,2), new UUID(0,4), new UUID(0,5), 1, new OperationId(new UUID(0,6),1,0), List.of(new DomainBinding(new UUID(0,3), 1, new byte[32], 1, new byte[32])), List.of(
            new SnapshotFence.Participant(new UUID(0,3), new UUID(0,7), new UUID(0,8), 1, new byte[32], new OperationId(new UUID(0,9),1,0), new OperationId(new UUID(0,9),1,1), new TransactionScope(List.of(TransactionScope.Span.point(new UUID(0,12),new byte[]{1}))))));
    RatisSnapshotCaptureRequest request(RatisSnapshotCaptureRequest.Mode mode) { return new RatisSnapshotCaptureRequest(fence,owner,snapshot,Duration.ofSeconds(30),mode); }
    SnapshotFenceStatus.Capture token() { return new SnapshotFenceStatus.Capture(owner,snapshot,position); }
    ScopedTransactionRpc.Handle handle() { return new ScopedTransactionRpc.Handle(owner,new UUID(0,13),4,position); }

    @Test void currentRequestFreezesSemanticModeAndCanonicalWritableReplyBindsOriginalHandle() throws Exception {
        var readonly=request(RatisSnapshotCaptureRequest.Mode.READ_ONLY); var writable=request(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED);
        assertEquals(readonly,new RatisSnapshotCaptureRequest(fence,owner,snapshot,Duration.ofSeconds(30))); assertFalse(Arrays.equals(readonly.encode(),writable.encode()));
        for (var request:List.of(readonly,writable)) {
            byte[] bytes=request.encode(); assertEquals(request,RatisSnapshotCaptureRequest.decode(bytes)); assertArrayEquals(bytes,RatisSnapshotCaptureRequest.decode(bytes).encode());
            byte[] invalid=bytes.clone(); invalid[40]=2; assertThrows(IllegalArgumentException.class,() -> RatisSnapshotCaptureRequest.decode(invalid));
            for (int n=0;n<bytes.length;n++) { byte[] shortRequest=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotCaptureRequest.decode(shortRequest)); }
        }
        byte[] reply=writable.reply(token(),handle()); assertEquals(157,reply.length); assertEquals(2,reply[80]);
        var decoded=writable.certifiedResult(reply); assertEquals(token(),decoded.token()); assertEquals(handle(),decoded.handle()); assertEquals(decoded,writable.resultBinding(reply));
        assertEquals(97,readonly.reply(token()).length); assertEquals(token(),readonly.result(readonly.reply(token()))); assertNull(readonly.resultBinding(readonly.reply(token())).handle());
        assertThrows(IllegalArgumentException.class,() -> writable.result(reply)); assertThrows(IllegalArgumentException.class,() -> readonly.certifiedResult(readonly.reply(token())));
    }
    @Test void modeCrossingLossAndChangedOwnerOrCutAreRejectedWithoutDroppingCertification() throws Exception {
        var readonly=request(RatisSnapshotCaptureRequest.Mode.READ_ONLY); var writable=request(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED);
        assertThrows(IllegalArgumentException.class,() -> writable.reply(token())); assertThrows(IllegalArgumentException.class,() -> readonly.reply(token(),handle()));
        assertThrows(IllegalArgumentException.class,() -> readonly.result(writable.reply(token(),handle()))); assertThrows(IllegalArgumentException.class,() -> writable.resultBinding(readonly.reply(token())));
        byte[] lost=writable.reply(null); assertEquals(81,lost.length); assertEquals(3,lost[80]); assertThrows(RatisSnapshotLostException.class,() -> writable.certifiedResult(lost)); assertThrows(IllegalArgumentException.class,() -> readonly.result(lost));
        assertEquals(1,readonly.reply(null)[80]); assertThrows(RatisSnapshotLostException.class,() -> readonly.result(readonly.reply(null)));
        assertThrows(IllegalArgumentException.class,() -> writable.reply(token(),new ScopedTransactionRpc.Handle(UUID.randomUUID(),handle().id(),4,position)));
        assertThrows(IllegalArgumentException.class,() -> writable.reply(token(),new ScopedTransactionRpc.Handle(owner,handle().id(),4,new ApplyPosition(4,7))));
        assertThrows(IllegalArgumentException.class,() -> writable.reply(null,handle())); assertThrows(NullPointerException.class,() -> new RatisSnapshotCaptureRequest.Result(null,handle()));
    }
    @Test void malformedFlagsLengthsBindingsAndHandleMetadataCannotCrossTheWire() {
        var request=request(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED); byte[] reply=request.reply(token(),handle());
        for (int n=0;n<reply.length;n++) { byte[] truncated=Arrays.copyOf(reply,n); assertThrows(IllegalArgumentException.class,() -> request.resultBinding(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> request.resultBinding(Arrays.copyOf(reply,reply.length+1)));
        for (int offset:new int[]{0,48,64,101}) { byte[] changed=reply.clone(); changed[offset]^=1; assertThrows(IllegalArgumentException.class,() -> request.resultBinding(changed)); }
        for (int flag:new int[]{0,1,3,4,255}) { byte[] changed=reply.clone(); changed[80]=(byte)flag; assertThrows(IllegalArgumentException.class,() -> request.resultBinding(changed)); }
        byte[] length=reply.clone(); ByteBuffer.wrap(length).putInt(97,ScopedTransactionRpc.HANDLE_BYTES-1); assertThrows(IllegalArgumentException.class,() -> request.resultBinding(length));
        byte[] generation=reply.clone(); ByteBuffer.wrap(generation).putLong(133,-1); assertThrows(IllegalArgumentException.class,() -> request.resultBinding(generation));
        byte[] changedCut=reply.clone(); ByteBuffer.wrap(changedCut).putLong(141,4); assertThrows(IllegalArgumentException.class,() -> request.resultBinding(changedCut));
    }
}
