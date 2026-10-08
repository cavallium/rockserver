package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import java.nio.ByteBuffer;
import java.util.*;

/** Fresh participant proof: an installed gate is READY only after all intersecting old work has drained. */
public record RatisSnapshotReadiness(ScopedTransactionRpc.SnapshotBinding binding,UUID group,ApplyPosition installed,boolean ready) {
    public RatisSnapshotReadiness {
        Objects.requireNonNull(binding); Objects.requireNonNull(group);
        if (ready && installed==null || installed!=null && (installed.term()<=0 || installed.index()<=0)) throw new IllegalArgumentException("Invalid snapshot ready proof");
    }
    public byte[] encode() {
        var output=ByteBuffer.allocate(installed==null ? 65 : 81); ScopedTransactionRpc.snapshotBinding(output,binding); ScopedTransactionRpc.uuid(output,group);
        output.put((byte)((installed==null ? 0 : 1)|(ready ? 2 : 0))); if (installed!=null) output.putLong(installed.term()).putLong(installed.index()); return output.array();
    }
    public static RatisSnapshotReadiness decode(byte[] bytes) {
        if (bytes.length!=65 && bytes.length!=81) throw new IllegalArgumentException("Invalid snapshot readiness length");
        var input=ByteBuffer.wrap(bytes); var binding=ScopedTransactionRpc.snapshotBinding(input); UUID group=ScopedTransactionRpc.uuid(input); int flags=input.get() & 255;
        if (flags>3 || flags==2 || ((flags & 1)==0 ? input.hasRemaining() : input.remaining()!=16)) throw new IllegalArgumentException("Invalid snapshot readiness flags");
        return new RatisSnapshotReadiness(binding,group,(flags & 1)==0 ? null : new ApplyPosition(input.getLong(),input.getLong()),(flags & 2)!=0);
    }
}
