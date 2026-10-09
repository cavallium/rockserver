package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.Arrays;

/** Exact journaled catalogue fencing input; neither preparation nor an echoed reply is an admission proof. */
public final class RatisSchemaFencingRpc {
    public static final int MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+DistributedCommandCodec.MAX_ENCODED_BYTES;
    private RatisSchemaFencingRpc() {}
    public static byte[] prepareRequest(DistributedCommand command) { return frame(ScopedTransactionRpc.SCHEMA_FENCING_PREPARE,command); }
    public static byte[] cancelRequest(DistributedCommand command) { return frame(ScopedTransactionRpc.SCHEMA_FENCING_CANCEL,command); }
    public static DistributedCommand request(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        requireKind(packet.kind()); var command=DistributedCommandCodec.decode(packet.body());
        if (!RatisSchemaAdministration.isFencing(command) || !Arrays.equals(packet.body(),DistributedCommandCodec.encode(command))) throw new IllegalArgumentException("Expected canonical exact fencing command");
        return command;
    }
    public static byte[] preparedReply(int kind,DistributedCommand command) { return frame(kind,command); }
    public static DistributedCommand decodePreparedReply(byte[] bytes,int expectedKind,DistributedCommand expected) {
        requireKind(expectedKind); var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (packet.kind()!=expectedKind) throw new IllegalArgumentException("Crossed fencing preparation reply");
        var command=request(bytes);
        if (!Arrays.equals(DistributedCommandCodec.encode(command),DistributedCommandCodec.encode(expected))) throw new IllegalArgumentException("Fencing preparation changed original command"); return command;
    }
    private static byte[] frame(int kind,DistributedCommand command) {
        requireKind(kind); if (!RatisSchemaAdministration.isFencing(command)) throw new IllegalArgumentException("Expected fencing command");
        return ScopedTransactionRpc.frame(kind,DistributedCommandCodec.encode(command));
    }
    private static void requireKind(int kind) {
        if (kind!=ScopedTransactionRpc.SCHEMA_FENCING_PREPARE && kind!=ScopedTransactionRpc.SCHEMA_FENCING_CANCEL) throw new IllegalArgumentException("Expected fencing preparation/cancellation");
    }
}
