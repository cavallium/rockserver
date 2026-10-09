package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeSchemaFence;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Bounded prepare/cancel/state queries; every reply echoes its complete qualified request. */
public final class RatisSchemaFenceRpc {
    public static final int MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+4+RatisSchemaFenceRecord.MAX_BYTES+4+NativeSchemaFence.BYTES+25;
    private RatisSchemaFenceRpc() {}
    public record State(RatisSchemaFenceRecord record,NativeSchemaFence fence,long snapshotPins,long preparedParticipants,long activeSnapshotGates,boolean ready) {
        public State {
            Objects.requireNonNull(record);
            if (snapshotPins<0 || preparedParticipants<0 || activeSnapshotGates<0) throw new IllegalArgumentException("Negative schema readiness count");
            if (ready && (fence==null || snapshotPins!=0 || preparedParticipants!=0 || activeSnapshotGates!=0)) throw new IllegalArgumentException("Schema readiness contradicts blockers");
            if (fence!=null) {
                fence.requireHeader(record.header());
                var expected=record.header().participants().stream().filter(p -> p.groupId().equals(record.groupId())).findFirst().orElseThrow();
                if (!expected.equals(fence.participant()) || !new NativeSchemaFence(record.header(),record.groupId(),fence.position()).equals(fence)) throw new IllegalArgumentException("Schema fence proof participant changed");
            }
        }
    }
    public static byte[] prepareRequest(RatisSchemaFenceRecord record) { return request(ScopedTransactionRpc.SCHEMA_FENCE_PREPARE,record); }
    public static byte[] cancelRequest(RatisSchemaFenceRecord record) { return request(ScopedTransactionRpc.SCHEMA_FENCE_CANCEL,record); }
    public static byte[] stateRequest(RatisSchemaFenceRecord record) { return request(ScopedTransactionRpc.SCHEMA_FENCE_STATE,record); }
    private static byte[] request(int kind,RatisSchemaFenceRecord record) { return ScopedTransactionRpc.frame(kind,record.encode()); }
    public static RatisSchemaFenceRecord request(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        requireKind(packet.kind()); return RatisSchemaFenceRecord.decode(packet.body());
    }
    public static byte[] preparedReply(int kind,RatisSchemaFenceRecord record) {
        requirePreparedKind(kind); return ScopedTransactionRpc.frame(kind,record.encode());
    }
    public static RatisSchemaFenceRecord decodePreparedReply(byte[] bytes,int expectedKind,RatisSchemaFenceRecord expected) {
        requirePreparedKind(expectedKind); var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (packet.kind()!=expectedKind) throw new IllegalArgumentException("Crossed schema preparation reply");
        var record=RatisSchemaFenceRecord.decode(packet.body());
        if (!record.equals(expected)) throw new IllegalArgumentException("Schema preparation reply identity changed");
        return record;
    }
    private static void requirePreparedKind(int kind) {
        if (kind!=ScopedTransactionRpc.SCHEMA_FENCE_PREPARE && kind!=ScopedTransactionRpc.SCHEMA_FENCE_CANCEL) throw new IllegalArgumentException("Expected schema preparation query");
    }
    public static byte[] reply(int kind,State state) {
        if (kind!=ScopedTransactionRpc.SCHEMA_FENCE_STATE) throw new IllegalArgumentException("Expected schema state reply"); byte[] record=state.record().encode(),proof=state.fence()==null ? null : state.fence().encode();
        var out=ByteBuffer.allocate(4+record.length+4+(proof==null ? 0 : proof.length)+25);
        ScopedTransactionRpc.putBlob(out,record); ScopedTransactionRpc.putBlob(out,proof);
        out.putLong(state.snapshotPins()).putLong(state.preparedParticipants()).putLong(state.activeSnapshotGates()).put((byte)(state.ready() ? 1 : 0));
        return ScopedTransactionRpc.frame(kind,out.array());
    }
    public static State stateReply(byte[] bytes,int expectedKind,RatisSchemaFenceRecord expected) {
        if (expectedKind!=ScopedTransactionRpc.SCHEMA_FENCE_STATE) throw new IllegalArgumentException("Expected schema state reply"); var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (packet.kind()!=expectedKind) throw new IllegalArgumentException("Crossed schema query reply");
        var in=ByteBuffer.wrap(packet.body()); var record=RatisSchemaFenceRecord.decode(ScopedTransactionRpc.blob(in,RatisSchemaFenceRecord.MAX_BYTES,false));
        if (!record.equals(expected)) throw new IllegalArgumentException("Schema query reply identity changed");
        byte[] proof=ScopedTransactionRpc.blob(in,NativeSchemaFence.BYTES,true);
        if (in.remaining()!=25) throw new IllegalArgumentException("Invalid schema query reply tail");
        long pins=in.getLong(),participants=in.getLong(),gates=in.getLong(); int ready=in.get() & 255;
        if (ready>1) throw new IllegalArgumentException("Invalid schema readiness flag");
        return new State(record,proof==null ? null : NativeSchemaFence.decode(proof),pins,participants,gates,ready==1);
    }
    private static void requireKind(int kind) { if (!ScopedTransactionRpc.schemaFence(kind)) throw new IllegalArgumentException("Expected schema fence query"); }
}
