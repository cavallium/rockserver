package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.SnapshotFenceProof;
import java.nio.ByteBuffer;
import java.util.*;

/** Fresh participant proof: an installed gate is READY only after all intersecting old work has drained. */
public record RatisSnapshotReadiness(ScopedTransactionRpc.SnapshotBinding binding,UUID group,ApplyPosition installed,boolean ready,SnapshotFenceProof retirement) {
    public RatisSnapshotReadiness {
        Objects.requireNonNull(binding); Objects.requireNonNull(group);
        if (retirement!=null && (installed!=null || ready || !retirement.participantGroup().equals(group) || !ScopedTransactionRpc.SnapshotBinding.of(retirement.fence()).equals(binding))) throw new IllegalArgumentException("Invalid retired snapshot readiness");
        if (ready && installed==null || installed!=null && (installed.term()<=0 || installed.index()<=0)) throw new IllegalArgumentException("Invalid snapshot ready proof");
    }
    public RatisSnapshotReadiness(ScopedTransactionRpc.SnapshotBinding binding,UUID group,ApplyPosition installed,boolean ready) { this(binding,group,installed,ready,null); }
    public static RatisSnapshotReadiness retired(SnapshotFenceProof proof) { return new RatisSnapshotReadiness(ScopedTransactionRpc.SnapshotBinding.of(proof.fence()),proof.participantGroup(),null,false,proof); }
    public boolean retired() { return retirement!=null; }
    public byte[] encode() {
        if (retired()) {
            byte[] proof=retirement.encode(); var b=ByteBuffer.allocate(69+proof.length); ScopedTransactionRpc.snapshotBinding(b,binding); ScopedTransactionRpc.uuid(b,group); b.put((byte)4); ScopedTransactionRpc.putBlob(b,proof); return b.array();
        }
        var output=ByteBuffer.allocate(installed==null ? 65 : 81); ScopedTransactionRpc.snapshotBinding(output,binding); ScopedTransactionRpc.uuid(output,group);
        output.put((byte)((installed==null ? 0 : 1)|(ready ? 2 : 0))); if (installed!=null) output.putLong(installed.term()).putLong(installed.index()); return output.array();
    }
    public static RatisSnapshotReadiness decode(byte[] bytes) {
        if (bytes.length<65 || bytes.length>69+SnapshotFenceProof.MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot readiness length");
        var input=ByteBuffer.wrap(bytes); var binding=ScopedTransactionRpc.snapshotBinding(input); UUID group=ScopedTransactionRpc.uuid(input); int flags=input.get() & 255;
        if (flags==4) { byte[] proof=ScopedTransactionRpc.blob(input,SnapshotFenceProof.MAX_BYTES,false); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing retired readiness"); return new RatisSnapshotReadiness(binding,group,null,false,SnapshotFenceProof.decode(proof)); }
        if (flags>3 || flags==2 || ((flags & 1)==0 ? input.hasRemaining() : input.remaining()!=16)) throw new IllegalArgumentException("Invalid snapshot readiness flags");
        return new RatisSnapshotReadiness(binding,group,(flags & 1)==0 ? null : new ApplyPosition(input.getLong(),input.getLong()),(flags & 2)!=0);
    }
}
