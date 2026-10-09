package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Monotone home proofs: every participant is gated before any snapshot capture is admitted. */
public final class SnapshotFenceStatus {
    public static final int HEADER_BYTES=10, MAX_BYTES=SnapshotFence.MAX_BYTES+8192;
    public enum Phase { INSTALLING, COMPLETE, ABORT }
    public record Capture(UUID ownerIncarnation,UUID snapshotId,ApplyPosition position) {
        public Capture { Objects.requireNonNull(ownerIncarnation); Objects.requireNonNull(snapshotId); positive(position); }
    }
    public record Progress(UUID groupId,ApplyPosition ready,Capture capture) {
        public Progress {
            Objects.requireNonNull(groupId); if (ready!=null) positive(ready);
            if (capture!=null && (ready==null || !atLeast(capture.position(),ready))) throw SnapshotFence.invalid("Capture precedes local ready proof");
        }
    }
    private final SnapshotFence fence;
    private final Phase phase;
    private final List<Progress> progress;
    private final boolean consumed;
    private SnapshotFenceStatus(SnapshotFence fence,Phase phase,List<Progress> progress,boolean consumed) {
        this.fence=Objects.requireNonNull(fence); this.phase=Objects.requireNonNull(phase); this.progress=List.copyOf(progress); this.consumed=consumed;
        if (consumed && phase==Phase.INSTALLING) throw SnapshotFence.invalid("Only a terminal snapshot may be consumed");
        if (this.progress.size()!=fence.participants().size()) throw SnapshotFence.invalid("Incomplete snapshot progress set");
        for (int i=0;i<this.progress.size();i++) if (!this.progress.get(i).groupId().equals(fence.participants().get(i).groupId())) throw SnapshotFence.invalid("Snapshot progress changed participant order/identity");
        if (!allReady() && this.progress.stream().anyMatch(p -> p.capture()!=null) || phase==Phase.COMPLETE && this.progress.stream().anyMatch(p -> p.capture()==null)) throw SnapshotFence.invalid("Invalid snapshot capture/decision shape");
    }
    public static SnapshotFenceStatus begin(SnapshotFence fence) { return new SnapshotFenceStatus(fence,Phase.INSTALLING,fence.participants().stream().map(p -> new Progress(p.groupId(),null,null)).toList(),false); }
    public SnapshotFence fence() { return fence; }
    public Phase phase() { return phase; }
    public List<Progress> progress() { return progress; }
    public Progress progress(UUID group) { return progress.get(index(group)); }
    public boolean allReady() { return progress.stream().allMatch(p -> p.ready()!=null); }
    public boolean consumed() { return consumed; }
    public boolean terminal() { return phase!=Phase.INSTALLING; }
    public long generation() { return progress.stream().mapToLong(p -> (p.ready()==null ? 0 : 1)+(p.capture()==null ? 0 : 1)).sum()+(terminal() ? 1 : 0)+(consumed ? 1 : 0); }
    public SnapshotFenceStatus ready(UUID group,ApplyPosition position) {
        positive(position); int index=index(group); var old=progress.get(index);
        if (old.ready()!=null) { if (!old.ready().equals(position)) throw SnapshotFence.invalid("Ready proof changed"); return this; }
        requireInstalling(); var next=new ArrayList<>(progress); next.set(index,new Progress(group,position,null)); return new SnapshotFenceStatus(fence,phase,next,false);
    }
    public SnapshotFenceStatus capture(UUID group,Capture capture) {
        Objects.requireNonNull(capture); int index=index(group); var old=progress.get(index);
        if (old.capture()!=null) { if (!old.capture().equals(capture)) throw SnapshotFence.invalid("Snapshot capture changed"); return this; }
        requireInstalling(); if (!allReady()) throw SnapshotFence.invalid("Snapshot capture requires every ready proof");
        var next=new ArrayList<>(progress); next.set(index,new Progress(group,old.ready(),capture)); return new SnapshotFenceStatus(fence,phase,next,false);
    }
    public SnapshotFenceStatus complete() {
        if (phase==Phase.COMPLETE) return this;
        requireInstalling(); return new SnapshotFenceStatus(fence,Phase.COMPLETE,progress,false);
    }
    public SnapshotFenceStatus abort() {
        if (phase==Phase.ABORT) return this;
        requireInstalling(); return new SnapshotFenceStatus(fence,Phase.ABORT,progress,false);
    }
    /** Explicit client retention permission; this does not certify release or snapshot delivery. */
    public SnapshotFenceStatus consume() {
        if (!terminal()) throw SnapshotFence.invalid("Snapshot consumption requires a terminal decision");
        return consumed ? this : new SnapshotFenceStatus(fence,phase,progress,true);
    }
    public SnapshotFenceStatus terminalDecision() {
        if (!terminal()) throw SnapshotFence.invalid("Snapshot has no terminal decision");
        return consumed ? new SnapshotFenceStatus(fence,phase,progress,false) : this;
    }
    public void requireSuccessorOf(SnapshotFenceStatus previous) {
        Objects.requireNonNull(previous);
        if (!fence.equals(previous.fence) || generation()!=previous.generation()+1) throw SnapshotFence.invalid("Snapshot fence/decision/generation changed");
        if (previous.terminal()) {
            if (previous.consumed || !consumed || phase!=previous.phase || !progress.equals(previous.progress)) throw SnapshotFence.invalid("Consumption changed the terminal snapshot decision");
            return;
        }
        if (consumed) throw SnapshotFence.invalid("Consumption requires a terminal predecessor");
        int additions=0;
        for (int i=0;i<progress.size();i++) {
            var old=previous.progress.get(i); var next=progress.get(i);
            if (!Objects.equals(old.ready(),next.ready())) { if (old.ready()!=null || next.ready()==null) throw SnapshotFence.invalid("Snapshot ready proof changed"); additions++; }
            if (!Objects.equals(old.capture(),next.capture())) { if (old.capture()!=null || next.capture()==null) throw SnapshotFence.invalid("Snapshot capture proof changed"); additions++; }
        }
        if (phase==Phase.INSTALLING ? additions!=1 : additions!=0) throw SnapshotFence.invalid("Snapshot status must advance one proof or decision");
    }
    public byte[] encode() {
        byte[] definition=fence.encode(); int size=HEADER_BYTES+definition.length;
        for (var p:progress) size+=17+(p.ready()==null ? 0 : 16)+(p.capture()==null ? 0 : 48);
        if (size>MAX_BYTES) throw SnapshotFence.invalid("Snapshot status exceeds bound");
        var b=ByteBuffer.allocate(size).putInt(definition.length).put(definition).put((byte)phase.ordinal()).put((byte)(consumed ? 1 : 0)).putInt(progress.size());
        for (var p:progress) {
            SnapshotFence.uuid(b,p.groupId()); b.put((byte)((p.ready()==null ? 0 : 1)|(p.capture()==null ? 0 : 2)));
            if (p.ready()!=null) position(b,p.ready());
            if (p.capture()!=null) { SnapshotFence.uuid(b,p.capture().ownerIncarnation()); SnapshotFence.uuid(b,p.capture().snapshotId()); position(b,p.capture().position()); }
        }
        return b.array();
    }
    public static SnapshotFenceStatus decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<HEADER_BYTES || bytes.length>MAX_BYTES) throw SnapshotFence.invalid("Invalid snapshot status length");
        try {
            var b=ByteBuffer.wrap(bytes); int length=b.getInt();
            if (length<SnapshotFence.HEADER_BYTES || length>SnapshotFence.MAX_BYTES || length>b.remaining()-6) throw SnapshotFence.invalid("Invalid snapshot definition length");
            byte[] definition=new byte[length]; b.get(definition); var fence=SnapshotFence.decode(definition); int phase=b.get() & 255,consumed=b.get() & 255,count=b.getInt();
            if (phase>=Phase.values().length || consumed>1 || count!=fence.participants().size() || 17L*count>b.remaining()) throw SnapshotFence.invalid("Invalid snapshot status phase/count");
            var progress=new ArrayList<Progress>(count);
            for (int i=0;i<count;i++) {
                SnapshotFence.require(b,17); UUID group=SnapshotFence.uuid(b); int flags=b.get() & 255;
                if (flags>3 || flags==2) throw SnapshotFence.invalid("Invalid snapshot progress flags");
                ApplyPosition ready=(flags & 1)==0 ? null : position(b); Capture capture=null;
                if ((flags & 2)!=0) { SnapshotFence.require(b,48); capture=new Capture(SnapshotFence.uuid(b),SnapshotFence.uuid(b),position(b)); }
                progress.add(new Progress(group,ready,capture));
            }
            if (b.hasRemaining()) throw SnapshotFence.invalid("Trailing snapshot status bytes");
            var result=new SnapshotFenceStatus(fence,Phase.values()[phase],progress,consumed==1);
            if (!Arrays.equals(bytes,result.encode())) throw SnapshotFence.invalid("Noncanonical snapshot status"); return result;
        } catch (RuntimeException malformed) { if (malformed instanceof IllegalArgumentException invalid) throw invalid; throw new IllegalArgumentException("Invalid snapshot status",malformed); }
    }
    public byte[] digest() {
        try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    @Override public boolean equals(Object other) { return other instanceof SnapshotFenceStatus status && Arrays.equals(encode(),status.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
    private int index(UUID group) { for (int i=0;i<progress.size();i++) if (progress.get(i).groupId().equals(group)) return i; throw SnapshotFence.invalid("Unregistered snapshot participant"); }
    private void requireInstalling() { if (terminal()) throw SnapshotFence.invalid("Snapshot decision is immutable"); }
    static void positive(ApplyPosition position) { Objects.requireNonNull(position); if (position.term()<=0 || position.index()<=0) throw SnapshotFence.invalid("Snapshot proof position must be positive"); }
    static boolean atLeast(ApplyPosition after,ApplyPosition before) { return after.term()>=before.term() && after.index()>=before.index() && (after.index()!=before.index() || after.term()==before.term()); }
    static void position(ByteBuffer b,ApplyPosition position) { b.putLong(position.term()).putLong(position.index()); }
    static ApplyPosition position(ByteBuffer b) { SnapshotFence.require(b,16); return new ApplyPosition(b.getLong(),b.getLong()); }
}
