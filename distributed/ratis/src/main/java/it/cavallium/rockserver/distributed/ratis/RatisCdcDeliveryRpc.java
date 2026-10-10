package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.Arrays;

/** Current exact delivery preparation envelope. The echo cannot authorize another command or leader. */
public final class RatisCdcDeliveryRpc {
    public static final int PREPARE=82,CANCEL=83,MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+DistributedCommandCodec.MAX_ENCODED_BYTES;
    private RatisCdcDeliveryRpc() {}
    public static boolean kind(int kind) { return kind==PREPARE || kind==CANCEL; }
    public static byte[] request(int kind,DistributedCommand command) {
        if(!kind(kind) || !RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command)) throw new IllegalArgumentException("Expected CDC delivery preparation");
        return ScopedTransactionRpc.frame(kind,DistributedCommandCodec.encode(command));
    }
    public static DistributedCommand decode(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES); var command=DistributedCommandCodec.decode(packet.body());
        if(!kind(packet.kind()) || !RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command) || !Arrays.equals(packet.body(),DistributedCommandCodec.encode(command))) throw new IllegalArgumentException("Expected canonical CDC delivery command");
        return command;
    }
    public static void requireEcho(byte[] reply,int kind,DistributedCommand command) {
        if(!Arrays.equals(reply,request(kind,command))) throw new IllegalArgumentException("CDC preparation reply changed original command"); decode(reply);
    }
}
