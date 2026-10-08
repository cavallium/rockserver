package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Durable physical gate; only a bound terminal home decision releases it. No timer or session expiry can unlock it. */
public record SnapshotGate(SnapshotFence fence,UUID participantGroup,ApplyPosition installed,ApplyPosition released,byte[] decisionDigest) {
    public static final int HEADER_BYTES=21, MAX_BYTES=SnapshotFence.MAX_BYTES+85;
    public SnapshotGate {
        Objects.requireNonNull(fence); Objects.requireNonNull(participantGroup); fence.participant(participantGroup);
        if (installed!=null) SnapshotFenceStatus.positive(installed);
        if (released!=null) SnapshotFenceStatus.positive(released);
        if (installed==null && released==null || (released==null)!=(decisionDigest==null) || decisionDigest!=null && decisionDigest.length!=32
                || installed!=null && released!=null && (!SnapshotFenceStatus.atLeast(released,installed) || released.index()<=installed.index())) throw SnapshotFence.invalid("Invalid snapshot gate state");
        decisionDigest=decisionDigest==null ? null : decisionDigest.clone();
    }
    @Override public byte[] decisionDigest() { return decisionDigest==null ? null : decisionDigest.clone(); }
    public static SnapshotGate install(SnapshotFence fence,UUID group,ApplyPosition position) { return new SnapshotGate(fence,group,position,null,null); }
    public static SnapshotGate released(SnapshotFenceStatus terminal,UUID group,ApplyPosition position) {
        Objects.requireNonNull(terminal);
        if (terminal.phase()!=SnapshotFenceStatus.Phase.ABORT || terminal.progress(group).ready()!=null) throw SnapshotFence.invalid("Release before install requires unready ABORT");
        return new SnapshotGate(terminal.fence(),group,null,position,terminal.digest());
    }
    public boolean active() { return installed!=null && released==null; }
    public boolean ownsOperation(OperationId operation) { var participant=fence.participant(participantGroup); return participant.installOperation().equals(operation) || participant.releaseOperation().equals(operation); }
    public SnapshotGate release(SnapshotFenceStatus terminal,ApplyPosition position) {
        Objects.requireNonNull(terminal); SnapshotFenceStatus.positive(position);
        if (!terminal.terminal() || !fence.equals(terminal.fence())) throw SnapshotFence.invalid("Snapshot release lacks bound terminal decision");
        var progress=terminal.progress(participantGroup);
        if (progress.ready()!=null && !progress.ready().equals(installed)) throw SnapshotFence.invalid("Home ready proof differs from native gate");
        if (released!=null) { if (!Arrays.equals(decisionDigest,terminal.digest())) throw SnapshotFence.invalid("Snapshot release decision changed"); return this; }
        if (terminal.phase()==SnapshotFenceStatus.Phase.COMPLETE
                && (installed==null || !installed.equals(progress.ready()) || progress.capture()==null || !SnapshotFenceStatus.atLeast(progress.capture().position(),installed)
                || !SnapshotFenceStatus.atLeast(position,progress.capture().position()) || position.index()<=progress.capture().position().index())) throw SnapshotFence.invalid("Completed snapshot release lacks ordered capture proof");
        return new SnapshotGate(fence,participantGroup,installed,position,terminal.digest());
    }
    public byte[] encode() {
        byte[] definition=fence.encode(); int size=HEADER_BYTES+definition.length+(installed==null ? 0 : 16)+(released==null ? 0 : 48);
        var b=ByteBuffer.allocate(size).putInt(definition.length).put(definition); SnapshotFence.uuid(b,participantGroup);
        b.put((byte)((installed==null ? 0 : 1)|(released==null ? 0 : 2)));
        if (installed!=null) SnapshotFenceStatus.position(b,installed);
        if (released!=null) { SnapshotFenceStatus.position(b,released); b.put(decisionDigest); }
        return b.array();
    }
    public static SnapshotGate decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<HEADER_BYTES || bytes.length>MAX_BYTES) throw SnapshotFence.invalid("Invalid snapshot gate length");
        try {
            var b=ByteBuffer.wrap(bytes); int length=b.getInt();
            if (length<SnapshotFence.HEADER_BYTES || length>SnapshotFence.MAX_BYTES || length>b.remaining()-17) throw SnapshotFence.invalid("Invalid snapshot gate definition length");
            byte[] definition=new byte[length]; b.get(definition); var fence=SnapshotFence.decode(definition); UUID group=SnapshotFence.uuid(b); int flags=b.get() & 255;
            if (flags<1 || flags>3) throw SnapshotFence.invalid("Invalid snapshot gate flags");
            ApplyPosition installed=(flags & 1)==0 ? null : SnapshotFenceStatus.position(b),released=(flags & 2)==0 ? null : SnapshotFenceStatus.position(b); byte[] digest=null;
            if (released!=null) { SnapshotFence.require(b,32); digest=new byte[32]; b.get(digest); }
            if (b.hasRemaining()) throw SnapshotFence.invalid("Trailing snapshot gate bytes");
            var result=new SnapshotGate(fence,group,installed,released,digest);
            if (!Arrays.equals(bytes,result.encode())) throw SnapshotFence.invalid("Noncanonical snapshot gate"); return result;
        } catch (RuntimeException malformed) { if (malformed instanceof IllegalArgumentException invalid) throw invalid; throw new IllegalArgumentException("Invalid snapshot gate",malformed); }
    }
    @Override public boolean equals(Object other) { return other instanceof SnapshotGate gate && fence.equals(gate.fence) && participantGroup.equals(gate.participantGroup) && Objects.equals(installed,gate.installed) && Objects.equals(released,gate.released) && Arrays.equals(decisionDigest,gate.decisionDigest); }
    @Override public int hashCode() { return Objects.hash(fence,participantGroup,installed,released,Arrays.hashCode(decisionDigest)); }
}
