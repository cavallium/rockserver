package org.apache.ratis.server.leader;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Objects;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.SnapshotInfo;

/** Exposes the current Ratis snapshot producer with deterministic file-reader ownership. */
public final class RockserverSnapshotRequests {
    private RockserverSnapshotRequests() {}
    public interface Requests extends Iterator<InstallSnapshotRequestProto>,Closeable {}
    public static Requests open(RaftServer.Division source,RaftPeerId receiver,String requestId,SnapshotInfo snapshot,int chunkBytes){
        Objects.requireNonNull(source);Objects.requireNonNull(receiver);Objects.requireNonNull(requestId);Objects.requireNonNull(snapshot);
        if(requestId.isEmpty() || requestId.length()>128 || chunkBytes<1 || chunkBytes>1024*1024)throw new IllegalArgumentException("Snapshot transfer request/chunk bound exceeded");
        return new InstallSnapshotRequests(source,receiver,requestId,snapshot,chunkBytes).iterator();
    }
}
