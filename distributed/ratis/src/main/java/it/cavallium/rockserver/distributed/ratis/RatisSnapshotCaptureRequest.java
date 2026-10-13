package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;

/** Frozen owner-directed volatile capture request. Retain it before the first RPC. */
public record RatisSnapshotCaptureRequest(SnapshotFence fence,UUID owner,UUID snapshotId,Duration ttl,Mode mode) {
    public enum Mode { READ_ONLY, WRITE_CERTIFIED }
    public record Result(SnapshotFenceStatus.Capture token,ScopedTransactionRpc.Handle handle) {
        public Result {
            Objects.requireNonNull(token);
            if (handle!=null && (!handle.owner().equals(token.ownerIncarnation()) || !handle.position().equals(token.position()))) throw new IllegalArgumentException("Certified handle differs from capture owner/cut");
        }
    }
    public static final int HEADER_BYTES=45, MAX_BYTES=HEADER_BYTES+SnapshotFence.MAX_BYTES;
    public static Duration maximumTtl(Mode mode) {return Objects.requireNonNull(mode)==Mode.READ_ONLY ? it.cavallium.rockserver.core.common.export.ExportJobs.MAX_SNAPSHOT_TTL : Duration.ofSeconds(60);}
    /** Validate before converting; both current wire milliseconds and monotonic nanoseconds remain bounded. */
    public static void requireTtl(Duration ttl,Mode mode) {
        Objects.requireNonNull(ttl);var maximum=maximumTtl(mode);
        if(ttl.isNegative() || ttl.isZero() || ttl.compareTo(maximum)>0)throw new IllegalArgumentException("Snapshot capture TTL exceeds mode bound");
        long millis=ttl.toMillis();if(millis<1 || !ttl.equals(Duration.ofMillis(millis)))throw new IllegalArgumentException("Snapshot capture TTL must use positive whole milliseconds");
    }
    public RatisSnapshotCaptureRequest(SnapshotFence fence,UUID owner,UUID snapshotId,Duration ttl) { this(fence,owner,snapshotId,ttl,Mode.READ_ONLY); }
    public RatisSnapshotCaptureRequest {
        Objects.requireNonNull(fence); Objects.requireNonNull(owner); Objects.requireNonNull(snapshotId); Objects.requireNonNull(ttl); Objects.requireNonNull(mode);
        requireTtl(ttl,mode);
    }
    public ScopedTransactionRpc.SnapshotBinding binding() { return ScopedTransactionRpc.SnapshotBinding.of(fence); }
    public byte[] encode() {
        byte[] definition=fence.encode(); var output=ByteBuffer.allocate(HEADER_BYTES+definition.length);
        ScopedTransactionRpc.uuid(output,owner); ScopedTransactionRpc.uuid(output,snapshotId);
        return output.putLong(ttl.toMillis()).put((byte)(mode==Mode.READ_ONLY ? 0 : 1)).putInt(definition.length).put(definition).array();
    }
    public static RatisSnapshotCaptureRequest decode(byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.length<HEADER_BYTES+SnapshotFence.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid capture request length");
        var input=ByteBuffer.wrap(bytes); UUID owner=ScopedTransactionRpc.uuid(input),snapshot=ScopedTransactionRpc.uuid(input); long ttl=input.getLong(); int tag=input.get() & 255,size=input.getInt();
        if (tag>1 || size!=input.remaining()) throw new IllegalArgumentException("Invalid capture mode/fence length"); byte[] definition=new byte[size]; input.get(definition);
        return new RatisSnapshotCaptureRequest(SnapshotFence.decode(definition),owner,snapshot,Duration.ofMillis(ttl),tag==0 ? Mode.READ_ONLY : Mode.WRITE_CERTIFIED);
    }
    /** Null token is a mode-bound loss reply; a writable success requires its original tracked handle. */
    public byte[] reply(SnapshotFenceStatus.Capture token) { return reply(token,null); }
    public byte[] reply(SnapshotFenceStatus.Capture token,ScopedTransactionRpc.Handle handle) {
        if (token==null && handle!=null || token!=null && (!token.ownerIncarnation().equals(owner) || !token.snapshotId().equals(snapshotId))) throw new IllegalArgumentException("Capture result identity changed");
        if (token!=null && (mode==Mode.WRITE_CERTIFIED)!=(handle!=null)) throw new IllegalArgumentException("Capture result mode changed");
        if (token!=null) new Result(token,handle);
        int flags=(token==null ? 1 : 0)|(mode==Mode.WRITE_CERTIFIED ? 2 : 0);
        var output=ByteBuffer.allocate(token==null ? 81 : handle==null ? 97 : 101+ScopedTransactionRpc.HANDLE_BYTES);
        ScopedTransactionRpc.snapshotBinding(output,binding()); ScopedTransactionRpc.uuid(output,owner); ScopedTransactionRpc.uuid(output,snapshotId); output.put((byte)flags);
        if (token!=null) { output.putLong(token.position().term()).putLong(token.position().index()); if (handle!=null) { output.putInt(ScopedTransactionRpc.HANDLE_BYTES); ScopedTransactionRpc.writeHandle(output,handle); } }
        return output.array();
    }
    public SnapshotFenceStatus.Capture result(byte[] bytes) throws RatisSnapshotLostException {
        if (mode!=Mode.READ_ONLY) throw new IllegalArgumentException("Writable capture requires explicit certified result handling");
        return resultBinding(bytes).token();
    }
    public Result certifiedResult(byte[] bytes) throws RatisSnapshotLostException {
        if (mode!=Mode.WRITE_CERTIFIED) throw new IllegalArgumentException("Expected a write-certified capture request");
        return resultBinding(bytes);
    }
    public Result resultBinding(byte[] bytes) throws RatisSnapshotLostException {
        Objects.requireNonNull(bytes);
        if (bytes.length!=81 && bytes.length!=97 && bytes.length!=101+ScopedTransactionRpc.HANDLE_BYTES) throw new IllegalArgumentException("Invalid capture reply length");
        var input=ByteBuffer.wrap(bytes); var binding=ScopedTransactionRpc.snapshotBinding(input); UUID actualOwner=ScopedTransactionRpc.uuid(input),actualSnapshot=ScopedTransactionRpc.uuid(input); int flags=input.get() & 255;
        boolean lost=(flags&1)!=0,writable=(flags&2)!=0;
        if (flags>3 || !binding.equals(this.binding()) || !actualOwner.equals(owner) || !actualSnapshot.equals(snapshotId) || writable!=(mode==Mode.WRITE_CERTIFIED)
                || (lost ? input.hasRemaining() : input.remaining()!=(writable ? 20+ScopedTransactionRpc.HANDLE_BYTES : 16))) throw new IllegalArgumentException("Capture reply binding/mode changed");
        if (lost) throw new RatisSnapshotLostException("Capture unavailable on its frozen owner");
        var token=new SnapshotFenceStatus.Capture(owner,snapshotId,new ApplyPosition(input.getLong(),input.getLong())); ScopedTransactionRpc.Handle handle=null;
        if (writable) { if (input.getInt()!=ScopedTransactionRpc.HANDLE_BYTES) throw new IllegalArgumentException("Invalid certified handle length"); handle=ScopedTransactionRpc.readHandle(input); }
        if (input.hasRemaining()) throw new IllegalArgumentException("Trailing capture reply bytes"); return new Result(token,handle);
    }
}
