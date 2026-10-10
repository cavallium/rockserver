package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.Arrays;

/** Exact publication envelope. An echoed acknowledgement is not a transferable owner proof. */
public final class RatisImportPublicationRpc {
    public static final int MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+DistributedCommandCodec.MAX_ENCODED_BYTES;
    private RatisImportPublicationRpc() {}
    public static byte[] prepareRequest(DistributedCommand command){return frame(ScopedTransactionRpc.IMPORT_PUBLICATION_PREPARE,command);}
    public static byte[] cancelRequest(DistributedCommand command){return frame(ScopedTransactionRpc.IMPORT_PUBLICATION_CANCEL,command);}
    public static DistributedCommand request(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);requireKind(packet.kind());var command=DistributedCommandCodec.decode(packet.body());if(!RatisRangeImportCatalogue.requiresOwnerAdmission(command) || !Arrays.equals(packet.body(),DistributedCommandCodec.encode(command)))throw new IllegalArgumentException("Expected canonical import publication");return command;}
    public static byte[] preparedReply(int kind,DistributedCommand command){return frame(kind,command);}
    public static DistributedCommand decodePreparedReply(byte[] bytes,int kind,DistributedCommand expected){requireKind(kind);var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);if(packet.kind()!=kind)throw new IllegalArgumentException("Crossed import publication reply");var command=request(bytes);if(!Arrays.equals(DistributedCommandCodec.encode(command),DistributedCommandCodec.encode(expected)))throw new IllegalArgumentException("Import publication reply changed original command");return command;}
    private static byte[] frame(int kind,DistributedCommand command){requireKind(kind);if(!RatisRangeImportCatalogue.requiresOwnerAdmission(command))throw new IllegalArgumentException("Expected import publication");return ScopedTransactionRpc.frame(kind,DistributedCommandCodec.encode(command));}
    private static void requireKind(int kind){if(!ScopedTransactionRpc.importPublication(kind))throw new IllegalArgumentException("Expected import preparation/cancellation");}
}
