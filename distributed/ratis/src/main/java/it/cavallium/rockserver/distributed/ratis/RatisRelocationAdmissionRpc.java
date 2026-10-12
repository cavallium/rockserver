package it.cavallium.rockserver.distributed.ratis;

import java.util.Arrays;

public final class RatisRelocationAdmissionRpc {
    public static final int PREPARE=110,CANCEL=111,MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+RatisTopologyRelocationRecord.MAX_BYTES;
    private RatisRelocationAdmissionRpc(){}
    public static boolean kind(int kind){return kind==PREPARE || kind==CANCEL;}
    public static byte[] request(int kind,RatisTopologyRelocationRecord record){if(!kind(kind))throw new IllegalArgumentException("Invalid relocation admission action");return ScopedTransactionRpc.frame(kind,record.encode());}
    public static RatisTopologyRelocationRecord decode(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);if(!kind(packet.kind()))throw new IllegalArgumentException("Invalid relocation admission frame");return RatisTopologyRelocationRecord.decode(packet.body());}
    public static void requireEcho(byte[] bytes,int kind,RatisTopologyRelocationRecord expected){if(!Arrays.equals(bytes,request(kind,expected)))throw new IllegalArgumentException("Relocation admission echo changed record");decode(bytes);}
}
