package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.Arrays;

/** Exact HOME reservation/promotion preparation; echoed command bytes are not caller-supplied authority. */
public final class RatisOperationRerouteAdmissionRpc {
    public static final int PREPARE=96,CANCEL=97,MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+DistributedCommandCodec.MAX_ENCODED_BYTES;
    private RatisOperationRerouteAdmissionRpc() {}
    public static boolean kind(int kind){return kind==PREPARE || kind==CANCEL;}
    public static byte[] prepareRequest(DistributedCommand command){return frame(PREPARE,command);}
    public static byte[] cancelRequest(DistributedCommand command){return frame(CANCEL,command);}
    private static byte[] frame(int kind,DistributedCommand command){if(!kind(kind) || !RatisTransactionStatuses.requiresRerouteAdmission(command))throw new IllegalArgumentException("Expected HOME reroute reservation/promotion");return ScopedTransactionRpc.frame(kind,DistributedCommandCodec.encode(command));}
    public static DistributedCommand decode(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);if(!kind(packet.kind()))throw new IllegalArgumentException("Wrong reroute admission kind");var command=DistributedCommandCodec.decode(packet.body());if(!RatisTransactionStatuses.requiresRerouteAdmission(command))throw new IllegalArgumentException("Expected HOME reroute command");return command;}
    public static void requireEcho(byte[] bytes,int kind,DistributedCommand expected){var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);if(packet.kind()!=kind || !Arrays.equals(DistributedCommandCodec.encode(decode(bytes)),DistributedCommandCodec.encode(expected)))throw new IllegalArgumentException("Reroute admission changed original command/kind");}
}
