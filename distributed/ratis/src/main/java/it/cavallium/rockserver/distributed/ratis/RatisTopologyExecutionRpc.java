package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.Arrays;

/** Exact catalogue execution preparation; the echo is local ticket acknowledgement, not transferable proof. */
public final class RatisTopologyExecutionRpc {
    public static final int PREPARE=93,CANCEL=94,MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+DistributedCommandCodec.MAX_ENCODED_BYTES;
    private RatisTopologyExecutionRpc(){}
    public static boolean kind(int kind){return kind==PREPARE || kind==CANCEL;}
    public static byte[] prepareRequest(DistributedCommand command){return request(PREPARE,command);}
    public static byte[] cancelRequest(DistributedCommand command){return request(CANCEL,command);}
    public static byte[] request(int kind,DistributedCommand command){if(!kind(kind) || !RatisTopologyCatalogue.requiresOwnerAdmission(command))throw new IllegalArgumentException("Expected topology execution preparation");return ScopedTransactionRpc.frame(kind,DistributedCommandCodec.encode(command));}
    public static DistributedCommand decode(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);var command=DistributedCommandCodec.decode(packet.body());if(!kind(packet.kind()) || !RatisTopologyCatalogue.requiresOwnerAdmission(command) || !Arrays.equals(packet.body(),DistributedCommandCodec.encode(command)))throw new IllegalArgumentException("Expected canonical topology execution command");return command;}
    public static void requireEcho(byte[] reply,int kind,DistributedCommand command){if(!Arrays.equals(reply,request(kind,command)))throw new IllegalArgumentException("Topology preparation reply changed original command");decode(reply);}
}
