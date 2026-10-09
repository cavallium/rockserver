package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Initial bounded point/same-range client. Cross-group atomicity and automatic ambiguity retry are
 * unsupported. Range clients are reused within a bounded idle-evicting cache; measured density is pending.
 */
public final class RatisRoutedClient implements AutoCloseable {
    static final int MAX_CONCURRENT_CALLS = 32;

    public interface Transport extends AutoCloseable {
        byte[] read(RangeDescriptor descriptor, UUID columnId, byte[] physicalKey) throws IOException;
        CommandResult command(RangeDescriptor descriptor, DistributedCommand command) throws IOException;
        default byte[] participant(RangeDescriptor descriptor,byte[] record) throws IOException { throw new IOException("Participant prepare unsupported by this transport"); }
        default byte[] snapshot(RangeDescriptor descriptor,ReplicaDescriptor owner,byte[] request) throws IOException { throw new IOException("Owner-specific snapshots unsupported by this transport"); }
        default RangeStateView stateView(RangeDescriptor descriptor) throws IOException { throw new IOException("Range-state reads unsupported by this transport"); }
        default byte[] transaction(RangeDescriptor descriptor,byte[] request,boolean freshBarrier) throws IOException { throw new IOException("Scoped transactions unsupported by this transport"); }
        @Override default void close() throws IOException {}
    }

    /** Keeps the original command identity and route for explicit retries, including after refresh. */
    public static final class PreparedCommand {
        private final RangeDirectory source;
        private final RangeDescriptor descriptor;
        private final DistributedCommand command;
        private PreparedCommand(RangeDirectory source, RangeDescriptor descriptor, DistributedCommand command) {
            this.source = source;
            this.descriptor = descriptor;
            this.command = command;
        }
        public RangeDescriptor descriptor() { return descriptor; }
        public DistributedCommand command() { return command; }
    }

    /** Frozen original replica and process incarnation; reads never reselect the current leader. */
    public static final class SnapshotTarget {
        private final RangeDirectory source; private final SnapshotFence fence; private final RangeDescriptor descriptor; private final ReplicaDescriptor replica; private final UUID incarnation; private final ScopedTransactionRpc.SnapshotBinding binding;
        private SnapshotTarget(RangeDirectory source,SnapshotFence fence,RangeDescriptor descriptor,ReplicaDescriptor replica,UUID incarnation) { this.source=source; this.fence=fence; this.descriptor=descriptor; this.replica=replica; this.incarnation=incarnation; binding=ScopedTransactionRpc.SnapshotBinding.of(fence); }
        public SnapshotFence fence() { return fence; }
        public RangeDescriptor descriptor() { return descriptor; }
        public ReplicaDescriptor replica() { return replica; }
        public UUID incarnation() { return incarnation; }
    }
    public RatisSnapshotReadiness snapshotReadiness(SnapshotFence fence,UUID group) throws IOException {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent"));
        requireSnapshotRoute(observed,descriptor,fence,group);
        try {
            var proof=RatisSnapshotReadiness.decode(transaction(descriptor,ScopedTransactionRpc.SNAPSHOT_READY,fence.encode(),true));
            if (!proof.group().equals(group) || !proof.binding().equals(ScopedTransactionRpc.SnapshotBinding.of(fence)) || proof.retired() && !proof.retirement().fence().equals(fence)) throw new IllegalArgumentException("Snapshot readiness binding changed"); return proof;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot readiness reply",malformed); }
    }
    public RatisSnapshotBlockers snapshotBlockers(SnapshotFence fence,UUID group,UUID after,int limit) throws IOException {
        var observed=current(); var route=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent"));
        requireSnapshotRoute(observed,route,fence,group); var request=new RatisSnapshotBlockers.Request(fence,after,limit);
        try {
            var page=RatisSnapshotBlockers.decode(transaction(route,ScopedTransactionRpc.SNAPSHOT_BLOCKERS,request.encode(),true));
            if (!page.binding().equals(ScopedTransactionRpc.SnapshotBinding.of(fence)) || !page.group().equals(group) || page.records().size()>limit
                    || after!=null && page.records().stream().anyMatch(r -> r.transaction().compareTo(after)<=0)) throw new IllegalArgumentException("Snapshot blocker page binding/cursor changed"); return page;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot blocker page",malformed); }
    }
    public SnapshotTarget snapshotTarget(SnapshotFence fence,UUID group) throws IOException {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent"));
        requireSnapshotRoute(observed,descriptor,fence,group);
        try {
            var owner=RatisSnapshotOwner.decode(transaction(descriptor,ScopedTransactionRpc.SNAPSHOT_OWNER,new byte[0],true));
            if (!owner.groupId().equals(group) || owner.rangeEpoch()!=descriptor.rangeEpoch() || !Arrays.equals(owner.schemaDigest(),observed.schemaDigest())) throw new IllegalArgumentException("Snapshot owner reply changed range/schema");
            var replica=descriptor.replicas().stream().filter(r -> r.peerId().equals(owner.peerId()) && r.role()==ReplicaDescriptor.Role.VOTER).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot owner is not a frozen voter"));
            return new SnapshotTarget(identity,fence,descriptor,replica,owner.incarnation());
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot owner reply",malformed); }
    }
    public SnapshotTarget restoreSnapshotTarget(SnapshotFence fence,RangeDescriptor descriptor,ReplicaDescriptor replica,UUID incarnation) {
        Objects.requireNonNull(incarnation); requireSnapshotRoute(current(),descriptor,fence,descriptor.groupId());
        if (!descriptor.replicas().contains(replica) || replica.role()!=ReplicaDescriptor.Role.VOTER) throw new IllegalArgumentException("Snapshot replica differs from frozen route");
        return new SnapshotTarget(identity,fence,descriptor,replica,incarnation);
    }
    public byte[] snapshotGet(SnapshotTarget target,SnapshotFenceStatus.Capture token,UUID column,byte[] key) throws IOException {
        Objects.requireNonNull(key); if (key.length>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Snapshot key exceeds bound");
        var body=ByteBuffer.allocate(116+key.length); ScopedTransactionRpc.snapshotBinding(body,target.binding); ScopedTransactionRpc.snapshotToken(body,token); ScopedTransactionRpc.uuid(body,column); ScopedTransactionRpc.putBlob(body,key);
        try { var input=ByteBuffer.wrap(snapshotQuery(target,token,ScopedTransactionRpc.SNAPSHOT_GET,body.array())); var value=ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing snapshot point reply"); return value; }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot point reply",malformed); }
    }
    public PhysicalRangePage snapshotPage(SnapshotTarget target,SnapshotFenceStatus.Capture token,UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws IOException {
        long size=133L+(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length)+(resume==null ? 0 : resume.length);
        if (size>ScopedTransactionRpc.MAX_REQUEST_BYTES-ScopedTransactionRpc.HEADER_BYTES) throw new IllegalArgumentException("Snapshot page request exceeds bound");
        var body=ByteBuffer.allocate((int)size); ScopedTransactionRpc.snapshotBinding(body,target.binding); ScopedTransactionRpc.snapshotToken(body,token); ScopedTransactionRpc.uuid(body,column);
        ScopedTransactionRpc.putBlob(body,lower); ScopedTransactionRpc.putBlob(body,upper); body.put((byte)(reverse ? 1 : 0)); ScopedTransactionRpc.putBlob(body,resume); body.putInt(items).putInt(bytes);
        try { return ScopedTransactionRpc.decodePage(snapshotQuery(target,token,ScopedTransactionRpc.SNAPSHOT_PAGE,body.array())); }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot page reply",malformed); }
    }
    public void releaseSnapshot(SnapshotTarget target,SnapshotFenceStatus.Capture token) throws IOException {
        var body=ByteBuffer.allocate(48+ScopedTransactionRpc.SNAPSHOT_TOKEN_BYTES); ScopedTransactionRpc.snapshotBinding(body,target.binding); ScopedTransactionRpc.snapshotToken(body,token);
        if (snapshotQuery(target,token,ScopedTransactionRpc.SNAPSHOT_RELEASE,body.array()).length!=0) throw new IOException("Invalid snapshot release reply");
    }
    private byte[] snapshotQuery(SnapshotTarget target,SnapshotFenceStatus.Capture token,int kind,byte[] body) throws IOException {
        Objects.requireNonNull(target); Objects.requireNonNull(token); requireIdentity(target.source);
        if (!target.incarnation.equals(token.ownerIncarnation())) throw new RatisSnapshotLostException("Token belongs to another owner incarnation");
        byte[] request=ScopedTransactionRpc.frame(kind,body); beginCall();
        try {
            var reply=ScopedTransactionRpc.parse(transport.snapshot(target.descriptor,target.replica,request),ScopedTransactionRpc.MAX_REPLY_BYTES);
            if (reply.kind()!=kind) throw new IllegalArgumentException("Snapshot reply opcode changed");
            return ScopedTransactionRpc.snapshotPayload(reply.body(),target.binding,token);
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid owner-specific snapshot reply",malformed); }
        finally { endCall(); }
    }

    public static final class PreparedSnapshotCapture {
        private final SnapshotTarget target; private final RatisSnapshotCaptureRequest request; private final byte[] bytes;
        private PreparedSnapshotCapture(SnapshotTarget target,RatisSnapshotCaptureRequest request) { this.target=target; this.request=request; bytes=ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_CAPTURE,request.encode()); }
        public SnapshotTarget target() { return target; }
        public RatisSnapshotCaptureRequest request() { return request; }
        public byte[] bytes() { return bytes.clone(); }
    }
    public PreparedSnapshotCapture prepareSnapshotCapture(SnapshotTarget target,UUID snapshotId,java.time.Duration ttl) {
        return restoreSnapshotCapture(target,new RatisSnapshotCaptureRequest(target.fence,target.incarnation,snapshotId,ttl));
    }
    public PreparedSnapshotCapture restoreSnapshotCapture(SnapshotTarget target,RatisSnapshotCaptureRequest request) {
        Objects.requireNonNull(target); Objects.requireNonNull(request); requireIdentity(target.source);
        if (!target.fence.equals(request.fence()) || !target.incarnation.equals(request.owner())) throw new IllegalArgumentException("Capture request differs from frozen target");
        return new PreparedSnapshotCapture(target,request);
    }
    public SnapshotFenceStatus.Capture executeSnapshotCapture(PreparedSnapshotCapture operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.target.source); beginCall();
        try {
            var response=ScopedTransactionRpc.parse(transport.snapshot(operation.target.descriptor,operation.target.replica,operation.bytes),ScopedTransactionRpc.MAX_REPLY_BYTES);
            if (response.kind()!=ScopedTransactionRpc.SNAPSHOT_CAPTURE) throw new IllegalArgumentException("Capture reply opcode changed");
            return operation.request.result(response.body());
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid capture reply",malformed); }
        finally { endCall(); }
    }

    public static final class PreparedSnapshotOperation {
        private final RangeDirectory source; private final RangeDescriptor descriptor; private final RatisSnapshotRecord request; private final byte[] bytes;
        private PreparedSnapshotOperation(RangeDirectory source,RangeDescriptor descriptor,RatisSnapshotRecord request) { this.source=source; this.descriptor=descriptor; this.request=request; bytes=request.encode(); }
        public RangeDescriptor descriptor() { return descriptor; }
        public RatisSnapshotRecord request() { return request; }
        public byte[] bytes() { return bytes.clone(); }
    }
    /** Retain this exact command and original route before dispatch; encoding does not grant leader admission authority. */
    public PreparedSnapshotOperation prepareSnapshotGate(RatisSnapshotRecord request) {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(request.participantGroup())).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent"));
        return restoreSnapshotGate(request,descriptor);
    }
    public PreparedSnapshotOperation restoreSnapshotGate(RatisSnapshotRecord request,RangeDescriptor descriptor) {
        var observed=current(); requireSnapshotRoute(observed,descriptor,request.fence(),request.participantGroup());
        return new PreparedSnapshotOperation(identity,descriptor,request);
    }
    private static void requireSnapshotRoute(RangeDirectory directory,RangeDescriptor descriptor,SnapshotFence fence,UUID group) {
        var participant=fence.participant(group);
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || !descriptor.groupId().equals(group) || !descriptor.rangeId().equals(participant.rangeId()) || descriptor.rangeEpoch()!=participant.rangeEpoch()
                || !Arrays.equals(directory.schemaDigest(),participant.schemaDigest()) || !Arrays.equals(directory.schemaDigest(),fence.schemaDigest()) || directory.schemaEpoch()!=fence.schemaEpoch()
                || !directory.clusterId().equals(fence.clusterId()) || !directory.databaseId().equals(fence.databaseId()) || !directory.domainId().equals(fence.domainId())) throw new IllegalArgumentException("Snapshot route differs from frozen participant");
    }
    public record SnapshotGatePreparation(boolean drained,RatisSnapshotRecord.Result receipt) {
        public SnapshotGatePreparation { if (!drained && receipt!=null) throw new IllegalArgumentException("Retained snapshot proof cannot be waiting"); }
    }
    public SnapshotGatePreparation authorizeSnapshotGate(PreparedSnapshotOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try { return authorizeSnapshotGateCall(operation); } finally { endCall(); }
    }
    private SnapshotGatePreparation authorizeSnapshotGateCall(PreparedSnapshotOperation operation) throws IOException {
        try {
            var input=ByteBuffer.wrap(transactionReply(operation.descriptor,ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE,operation.bytes,true));
            byte[] digest=ScopedTransactionRpc.blob(input,32,false);
            if (input.remaining()<5) throw new IllegalArgumentException("Truncated snapshot preparation reply");
            int state=input.get()&255; byte[] bytes=ScopedTransactionRpc.blob(input,RatisSnapshotRecord.Result.MAX_BYTES,true);
            if (!Arrays.equals(digest,operation.request.digest()) || input.hasRemaining() || state>2 || (state==2)!=(bytes!=null)) throw new IllegalArgumentException("Snapshot authority changed its frozen request");
            var receipt=bytes==null ? null : RatisSnapshotRecord.Result.decode(bytes); if (receipt!=null) requireSnapshotResult(operation,receipt);
            return new SnapshotGatePreparation(state!=0,receipt);
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot gate authority reply",malformed); }
    }
    public RatisSnapshotRecord.Result executeSnapshotGate(PreparedSnapshotOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try {
            var preparation=authorizeSnapshotGateCall(operation);
            if (!preparation.drained()) throw new RatisSnapshotWaitingException();
            var result=preparation.receipt()==null ? RatisSnapshotRecord.Result.decode(transport.participant(operation.descriptor,operation.bytes)) : preparation.receipt();
            requireSnapshotResult(operation,result); return result;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot gate applied reply",malformed); }
        finally { endCall(); }
    }
    private static void requireSnapshotResult(PreparedSnapshotOperation operation,RatisSnapshotRecord.Result result) {
        if (result.retired()) {
            if (!result.retirement().fence().equals(operation.request.fence()) || !result.retirement().participantGroup().equals(operation.request.participantGroup())) throw new IllegalArgumentException("Retired snapshot reply binding changed");
            return;
        }
        if (operation.request.isRelease()) {
            if (result.suppressed()) throw new IllegalArgumentException("Snapshot RELEASE requires an applied receipt");
            var progress=operation.request.terminal().progress(operation.request.participantGroup());
            var before=progress.capture()==null ? progress.ready() : progress.capture().position();
            if (before!=null && (result.position().term()<before.term() || result.position().index()<=before.index())) throw new IllegalArgumentException("Snapshot RELEASE reply precedes frozen proof");
        }
    }
    public void cancelSnapshotGate(PreparedSnapshotOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try {
            byte[] reply=transactionReply(operation.descriptor,ScopedTransactionRpc.SNAPSHOT_GATE_CANCEL,operation.bytes,true);
            if (!Arrays.equals(reply,operation.request.digest())) throw new IOException("Snapshot cancellation identity changed");
        } finally { endCall(); }
    }
    /** One bounded server-side recovery quantum; false or an empty blocker page never proves READY. */
    public boolean helpSnapshotFence(SnapshotFence fence,UUID group) throws IOException {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot helper route absent"));
        requireSnapshotRoute(observed,descriptor,fence,group);
        try {
            var binding=ScopedTransactionRpc.SnapshotBinding.of(fence); var body=ByteBuffer.allocate(48); ScopedTransactionRpc.snapshotBinding(body,binding);
            var input=ByteBuffer.wrap(transaction(descriptor,ScopedTransactionRpc.SNAPSHOT_HELP,body.array(),true));
            var actual=ScopedTransactionRpc.snapshotBinding(input); var actualGroup=ScopedTransactionRpc.uuid(input);
            if (!binding.equals(actual) || !group.equals(actualGroup) || input.remaining()!=1) throw new IllegalArgumentException("Snapshot helper reply binding changed");
            int helped=input.get()&255; if (helped>1) throw new IllegalArgumentException("Invalid snapshot helper progress"); return helped==1;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot helper reply",malformed); }
    }
    /** Fresh retained gate/terminal state remains queryable independently of control grants. */
    public SnapshotGate snapshotGate(SnapshotFence fence,UUID group) throws IOException {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent"));
        requireSnapshotRoute(observed,descriptor,fence,group); var body=ByteBuffer.allocate(16); ScopedTransactionRpc.uuid(body,fence.fenceId());
        try {
            var input=ByteBuffer.wrap(transaction(descriptor,ScopedTransactionRpc.SNAPSHOT_GATE_STATE,body.array(),true));
            byte[] bytes=ScopedTransactionRpc.blob(input,SnapshotGate.MAX_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing snapshot gate state");
            if (bytes==null) return null; var record=SnapshotGate.decode(bytes);
            if (!record.participantGroup().equals(group) || !record.fence().equals(fence)) throw new IllegalArgumentException("Snapshot gate reply binding changed");
            return record;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot gate state reply",malformed); }
    }
    public static final class PreparedHomeRecoveryOperation {
        private final RangeDirectory source;
        private final RangeDescriptor descriptor;
        private final HomeRecovery recovery;
        private final byte[] bytes;
        private PreparedHomeRecoveryOperation(RangeDirectory source,RangeDescriptor descriptor,HomeRecovery recovery) {
            this.source=source; this.descriptor=descriptor; this.recovery=recovery; bytes=recovery.encode();
        }
        public RangeDescriptor descriptor() { return descriptor; }
        public HomeRecovery recovery() { return recovery; }
        PreparedCommand command() { return new PreparedCommand(source,descriptor,recovery.command()); }
        public byte[] bytes() { return bytes.clone(); }
    }
    /** Freeze this request and its original HOME route durably before preparation or append RPCs. */
    public PreparedHomeRecoveryOperation prepareHomeRecovery(HomeRecovery recovery) {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(recovery.command().groupId())).findFirst().orElseThrow(() -> new IllegalArgumentException("HOME recovery route absent"));
        return restoreHomeRecovery(recovery,descriptor);
    }
    public PreparedHomeRecoveryOperation restoreHomeRecovery(HomeRecovery recovery,RangeDescriptor descriptor) {
        Objects.requireNonNull(recovery); var observed=current();
        if (!observed.clusterId().equals(recovery.before().clusterId()) || !observed.databaseId().equals(recovery.before().databaseId())) throw new IllegalArgumentException("HOME recovery database changed");
        restoreSessionCommand(recovery.command(),descriptor);
        return new PreparedHomeRecoveryOperation(identity,descriptor,recovery);
    }
    public void authorizeHomeRecovery(PreparedHomeRecoveryOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try { authorizeHomeRecoveryCall(operation); } finally { endCall(); }
    }
    private HomeRecoveryReceipt authorizeHomeRecoveryCall(PreparedHomeRecoveryOperation operation) throws IOException {
        try {
            var input=ByteBuffer.wrap(transactionReply(operation.descriptor,ScopedTransactionRpc.HOME_RECOVERY_PREPARE,operation.bytes,true));
            byte[] digest=ScopedTransactionRpc.blob(input,32,false),receipt=ScopedTransactionRpc.blob(input,HomeRecoveryResult.MAX_BYTES,true);
            if (!Arrays.equals(digest,operation.recovery.digest()) || input.hasRemaining()) throw new IllegalArgumentException("HOME recovery preparation changed its frozen request");
            return receipt==null ? null : homeReceipt(operation,receipt);
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid HOME recovery preparation reply",malformed); }
    }
    public HomeRecoveryReceipt executeHomeRecovery(PreparedHomeRecoveryOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try {
            var retained=authorizeHomeRecoveryCall(operation);
            return retained==null ? homeReceipt(operation,transport.participant(operation.descriptor,operation.bytes)) : retained;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid HOME recovery applied receipt",malformed); }
        finally { endCall(); }
    }
    public HomeRecoveryReceipt homeRecoveryResult(PreparedHomeRecoveryOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try {
            var input=ByteBuffer.wrap(transactionReply(operation.descriptor,ScopedTransactionRpc.HOME_RECOVERY_RESULT,operation.bytes,true));
            var bytes=ScopedTransactionRpc.blob(input,HomeRecoveryResult.MAX_BYTES,true);
            if (input.hasRemaining()) throw new IllegalArgumentException("Trailing HOME recovery receipt");
            return bytes==null ? null : homeReceipt(operation,bytes);
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid retained HOME recovery receipt",malformed); }
        finally { endCall(); }
    }
    /** Abandon only unsubmitted authority; a submitted unknown request retains its original owner. */
    public void cancelHomeRecovery(PreparedHomeRecoveryOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try {
            byte[] reply=transactionReply(operation.descriptor,ScopedTransactionRpc.HOME_RECOVERY_CANCEL,operation.bytes,true);
            if (!Arrays.equals(reply,operation.recovery.digest())) throw new IOException("HOME recovery cancellation identity changed");
        } finally { endCall(); }
    }
    private static HomeRecoveryReceipt homeReceipt(PreparedHomeRecoveryOperation operation,byte[] bytes) throws RatisHomeRetiredException {
        if (bytes==null) throw new IllegalArgumentException("Missing HOME recovery receipt");
        var outcome=HomeRecoveryResult.decode(bytes);
        if (outcome.retired()) {
            var command=operation.recovery.command(); var proof=outcome.retirement();
            var expected=new HomeIdentity(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.mutations().getFirst().columnId(),operation.recovery.before());
            if (!proof.identity().equals(expected)) throw new IllegalArgumentException("Retired HOME recovery identity changed");
            throw new RatisHomeRetiredException(proof);
        }
        var receipt=outcome.receipt();
        if (!receipt.recovery().equals(operation.recovery)) throw new IllegalArgumentException("HOME recovery receipt belongs to another frozen request");
        return receipt;
    }

    public static final class PreparedFinalizationOperation {
        private final RangeDirectory source; private final RangeDescriptor descriptor; private final ParticipantFinalization finalization; private final byte[] bytes;
        private PreparedFinalizationOperation(RangeDirectory source,RangeDescriptor descriptor,ParticipantFinalization finalization) { this.source=source; this.descriptor=descriptor; this.finalization=finalization; bytes=RatisParticipantRecord.encode(finalization); }
        public RangeDescriptor descriptor() { return descriptor; }
        public ParticipantFinalization finalization() { return finalization; }
        public byte[] bytes() { return bytes.clone(); }
    }
    /** Retain exact bytes and original route durably before the first FINALIZE write RPC. */
    public PreparedFinalizationOperation prepareFinalization(ParticipantFinalization finalization) {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(finalization.participantGroup())).findFirst().orElseThrow(() -> new IllegalArgumentException("Finalization route absent"));
        return restoreFinalization(finalization,descriptor);
    }
    public PreparedFinalizationOperation restoreFinalization(ParticipantFinalization finalization,RangeDescriptor descriptor) {
        var observed=current(); var registered=finalization.participant(); var decision=finalization.decision();
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || !descriptor.groupId().equals(registered.groupId()) || !descriptor.rangeId().equals(registered.rangeId()) || descriptor.rangeEpoch()!=registered.rangeEpoch()
                || !Arrays.equals(observed.schemaDigest(),registered.schemaDigest()) || !observed.domainId().equals(decision.domainId()) || !observed.clusterId().equals(decision.clusterId()) || !observed.databaseId().equals(decision.databaseId())) throw new IllegalArgumentException("Finalization route differs from frozen participant");
        return new PreparedFinalizationOperation(identity,descriptor,finalization);
    }
    /** Obtain ready authority from a fixed server-side HOME reader; frozen bytes never choose that reader. */
    public void authorizeFinalization(PreparedFinalizationOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try { authorizeFinalizationCall(operation); } finally { endCall(); }
    }
    private ParticipantFinalizationResult authorizeFinalizationCall(PreparedFinalizationOperation operation) throws IOException {
        try {
            var input=ByteBuffer.wrap(transactionReply(operation.descriptor,ScopedTransactionRpc.FINALIZATION_PREPARE,operation.bytes,true));
            byte[] digest=ScopedTransactionRpc.blob(input,32,false),bytes=ScopedTransactionRpc.blob(input,ParticipantFinalizationResult.MAX_BYTES,true);
            if (!Arrays.equals(digest,RatisParticipantRecord.digest(operation.finalization)) || input.hasRemaining()) throw new IllegalArgumentException("Finalization authority changed its frozen request");
            if (bytes==null) return null;
            var result=ParticipantFinalizationResult.decode(bytes); requireFinalizationResult(operation,result); return result;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid participant finalization authority reply",malformed); }
    }
    /** Retained proof finishes an exact retry without another append or another HOME read. */
    public ApplyPosition executeFinalization(PreparedFinalizationOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try {
            var retained=authorizeFinalizationCall(operation);
            var position=retained==null ? RatisParticipantRecord.finalizationResult(transport.participant(operation.descriptor,operation.bytes)) : retained;
            requireFinalizationResult(operation,position);
            if (position.retired()) throw new RatisParticipantRetiredException(position.retirement()); return position.position();
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid finalization applied reply",malformed); }
        finally { endCall(); }
    }
    private static void requireFinalizationResult(PreparedFinalizationOperation operation,ParticipantFinalizationResult result) {
        if (result.retired()) {
            var f=operation.finalization;
            if (!result.retirement().identity().equals(new ParticipantIdentity(f.homeGroup(),f.homeEpoch(),f.participantGroup(),f.decision()))) throw new IllegalArgumentException("Retired finalization identity changed");
        } else requireFinalizationPosition(operation,result.position());
    }
    private static void requireFinalizationPosition(PreparedFinalizationOperation operation,ApplyPosition position) {
        var vote=operation.finalization.participant().prepared();
        if (position.term()<=0 || position.index()<=0 || vote!=null && (position.term()<vote.term() || position.index()<=vote.index())) throw new IllegalArgumentException("Finalization reply does not follow its prepared vote");
    }
    /** Releases ready unsubmitted authority; active preparations and submitted unknown requests retain ownership. */
    public void cancelFinalization(PreparedFinalizationOperation operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try {
            byte[] reply=transactionReply(operation.descriptor,ScopedTransactionRpc.FINALIZATION_CANCEL,operation.bytes,true);
            if (!Arrays.equals(reply,RatisParticipantRecord.digest(operation.finalization))) throw new IOException("Finalization cancellation request changed");
        } finally { endCall(); }
    }
    public static final class PreparedParticipantOperation {
        private final RangeDirectory source; private final RangeDescriptor descriptor; private final PreparedParticipant.Preparation preparation; private final boolean negative; private final byte[] bytes;
        private PreparedParticipantOperation(RangeDirectory source,RangeDescriptor descriptor,PreparedParticipant.Preparation preparation,boolean negative) { this.source=source; this.descriptor=descriptor; this.preparation=preparation; this.negative=negative; bytes=RatisParticipantRecord.encode(preparation,negative); }
        public RangeDescriptor descriptor() { return descriptor; } public PreparedParticipant.Preparation preparation() { return preparation; } public boolean negative() { return negative; } public byte[] bytes() { return bytes.clone(); }
    }
    /** Freeze and durably retain this operation before the first participant write RPC. No route or identity is regenerated on retry. */
    public PreparedParticipantOperation prepareParticipant(PreparedParticipant.Preparation preparation) { return prepareParticipant(preparation,false); }
    public PreparedParticipantOperation prepareParticipant(PreparedParticipant.Preparation preparation,boolean negative) {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(preparation.business().groupId())).findFirst().orElseThrow(() -> new IllegalArgumentException("Participant route is absent"));
        return restoreParticipant(preparation,descriptor,negative);
    }
    public PreparedParticipantOperation restoreParticipant(PreparedParticipant.Preparation preparation,RangeDescriptor descriptor) { return restoreParticipant(preparation,descriptor,false); }
    public PreparedParticipantOperation restoreParticipant(PreparedParticipant.Preparation preparation,RangeDescriptor descriptor,boolean negative) {
        var observed=current();
        var registered=preparation.registration().participants().stream().filter(p -> p.groupId().equals(descriptor.groupId())).findFirst().orElseThrow();
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || descriptor.rangeEpoch()!=preparation.business().rangeEpoch() || !registered.rangeId().equals(descriptor.rangeId())
                || !Arrays.equals(observed.schemaDigest(),preparation.business().schemaDigest()) || !observed.domainId().equals(preparation.registration().domainId())
                || !observed.clusterId().equals(preparation.registration().clusterId()) || !observed.databaseId().equals(preparation.registration().databaseId())) throw new IllegalArgumentException("Participant route differs from frozen registration");
        restoreSessionCommand(preparation.business(),descriptor);
        return new PreparedParticipantOperation(identity,descriptor,preparation,negative);
    }
    public ParticipantVote executeParticipant(PreparedParticipantOperation operation) throws IOException {
        java.util.Objects.requireNonNull(operation); requireIdentity(operation.source); beginCall();
        try {
            var vote=RatisParticipantRecord.result(transport.participant(operation.descriptor,operation.bytes));
            if (vote.retired()) {
                var p=operation.preparation;
                if (!vote.retirementProof().identity().equals(new ParticipantIdentity(p.homeGroup(),p.homeEpoch(),p.business().groupId(),p.registration()))) throw new IllegalArgumentException("Retired PREPARE identity changed");
            }
            if (operation.negative && vote.prepared()) throw new IllegalArgumentException("Negative PREPARE returned a positive vote"); return vote;
        }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid participant vote reply",invalid); }
        finally { endCall(); }
    }
    /** Fresh independently queryable state survives prepare-response loss and control-session receipt GC. */
    public PreparedParticipant preparedParticipant(UUID group,UUID transactionId) throws IOException {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Participant route absent"));
        var body=ByteBuffer.allocate(16); ScopedTransactionRpc.uuid(body,transactionId);
        try {
            var input=ByteBuffer.wrap(transaction(descriptor,ScopedTransactionRpc.PARTICIPANT_STATE,body.array(),true));
            byte[] bytes=ScopedTransactionRpc.blob(input,16+PreparedParticipant.Preparation.MAX_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing prepared state");
            if (bytes==null) return null; var record=PreparedParticipant.decode(bytes);
            if (!record.preparation().transactionId().equals(transactionId) || !record.preparation().business().groupId().equals(group)) throw new IllegalArgumentException("Prepared reply identity changed");
            prepareParticipant(record.preparation()); return record;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid prepared state reply",malformed); }
    }
    /** Fresh terminal result lookup does not depend on either business or recovery grant eligibility. */
    public ParticipantOutcome finalizedParticipant(UUID group,UUID transactionId) throws IOException {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Participant route absent"));
        var body=ByteBuffer.allocate(16); ScopedTransactionRpc.uuid(body,transactionId);
        try {
            var input=ByteBuffer.wrap(transaction(descriptor,ScopedTransactionRpc.PARTICIPANT_OUTCOME,body.array(),true));
            byte[] bytes=ScopedTransactionRpc.blob(input,ParticipantOutcome.MAX_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing participant outcome");
            if (bytes==null) return null; var outcome=ParticipantOutcome.decode(bytes); var decision=outcome.finalization().decision(); var participant=outcome.finalization().participant();
            if (!decision.transactionId().equals(transactionId) || !participant.groupId().equals(group) || !participant.rangeId().equals(descriptor.rangeId()) || participant.rangeEpoch()!=descriptor.rangeEpoch()
                    || !Arrays.equals(participant.schemaDigest(),observed.schemaDigest()) || !decision.clusterId().equals(observed.clusterId()) || !decision.databaseId().equals(observed.databaseId()) || !decision.domainId().equals(observed.domainId())) throw new IllegalArgumentException("Participant outcome route/identity changed");
            return outcome;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid participant outcome reply",invalid); }
    }
    public SnapshotFenceProof snapshotFence(SnapshotFence fence,UUID group) throws IOException {
        var observed=current(); var route=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent")); requireSnapshotRoute(observed,route,fence,group); beginCall();
        try {
            var input=ByteBuffer.wrap(transactionReply(route,ScopedTransactionRpc.SNAPSHOT_FENCE,fence.encode(),true));
            byte[] binding=new byte[48]; if (input.remaining()<52) throw new IllegalArgumentException("Truncated snapshot fence reply"); input.get(binding);
            if (!ScopedTransactionRpc.snapshotBinding(ByteBuffer.wrap(binding)).equals(ScopedTransactionRpc.SnapshotBinding.of(fence))) throw new IllegalArgumentException("Snapshot fence binding changed");
            byte[] bytes=ScopedTransactionRpc.blob(input,SnapshotFenceProof.MAX_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing snapshot fence reply");
            if (bytes==null) return null; var proof=SnapshotFenceProof.decode(bytes); if (!proof.fence().equals(fence) || !proof.participantGroup().equals(group)) throw new IllegalArgumentException("Snapshot fence identity changed"); return proof;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot fence reply",malformed); } finally { endCall(); }
    }
    public void retireSnapshotGate(SnapshotGateRetirement retirement) throws IOException {
        var fence=retirement.fence(); var observed=current(); var route=observed.ranges().stream().filter(r -> r.groupId().equals(retirement.participantGroup())).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent")); requireSnapshotRoute(observed,route,fence,retirement.participantGroup()); beginCall();
        try {
            byte[] reply=transactionReply(route,ScopedTransactionRpc.SNAPSHOT_RETIRE_PREPARE,RatisSnapshotRetirementRecord.encode(retirement),true);
            if (reply.length!=33 || !Arrays.equals(Arrays.copyOf(reply,32),retirement.digest()) || (reply[32]&255)>1) throw new IOException("Invalid snapshot retirement authority reply"); if (reply[32]==1) return;
            var result=CommandResult.decode(transport.participant(route,RatisSnapshotRetirementRecord.encode(retirement))); if (result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=0) throw new IOException("Invalid snapshot retirement applied reply");
        } finally { endCall(); }
    }
    public void cancelSnapshotRetirement(SnapshotGateRetirement retirement) throws IOException {
        var observed=current(); var route=observed.ranges().stream().filter(r -> r.groupId().equals(retirement.participantGroup())).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent")); requireSnapshotRoute(observed,route,retirement.fence(),retirement.participantGroup()); beginCall();
        try { byte[] reply=transactionReply(route,ScopedTransactionRpc.SNAPSHOT_RETIRE_CANCEL,RatisSnapshotRetirementRecord.encode(retirement),true);
            if (reply.length!=33 || !Arrays.equals(Arrays.copyOf(reply,32),retirement.digest()) || reply[32]!=0) throw new IOException("Invalid snapshot retirement cancellation reply");
        } finally { endCall(); }
    }
    private RangeDescriptor homeRoute(HomeIdentity identity) {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(identity.groupId())).findFirst().orElseThrow(() -> new IllegalArgumentException("HOME route absent"));
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || descriptor.rangeEpoch()!=identity.rangeEpoch() || !observed.columns().equals(java.util.Set.of(identity.columnId()))
                || !observed.clusterId().equals(identity.definition().clusterId()) || !observed.databaseId().equals(identity.definition().databaseId()) || !Arrays.equals(observed.schemaDigest(),identity.schemaDigest())) throw new IllegalArgumentException("HOME identity differs from route");
        return descriptor;
    }
    public HomeFenceProof homeFence(HomeIdentity identity) throws IOException {
        Objects.requireNonNull(identity); var descriptor=homeRoute(identity); beginCall();
        try {
            var input=ByteBuffer.wrap(transactionReply(descriptor,ScopedTransactionRpc.HOME_FENCE,identity.encode(),true));
            if (input.remaining()<36) throw new IllegalArgumentException("Truncated HOME fence reply"); byte[] digest=new byte[32]; input.get(digest);
            byte[] bytes=ScopedTransactionRpc.blob(input,HomeFenceProof.MAX_BYTES,true);
            if (!Arrays.equals(digest,identity.digest()) || input.hasRemaining()) throw new IllegalArgumentException("HOME fence binding changed");
            if (bytes==null) return null; var proof=HomeFenceProof.decode(bytes); if (!proof.identity().equals(identity)) throw new IllegalArgumentException("HOME fence identity changed"); return proof;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid HOME fence reply",malformed); }
        finally { endCall(); }
    }
    public void retireHome(HomeRetirement retirement) throws IOException {
        Objects.requireNonNull(retirement); var descriptor=homeRoute(retirement.identity()); beginCall();
        try {
            byte[] record=RatisHomeRetirementRecord.encode(retirement),digest=retirement.digest();
            byte[] reply=transactionReply(descriptor,ScopedTransactionRpc.HOME_RETIRE_PREPARE,record,true);
            if (reply.length!=33 || !Arrays.equals(Arrays.copyOf(reply,32),digest) || (reply[32]&255)>1) throw new IOException("Invalid HOME retirement authority reply");
            if (reply[32]==1) return;
            var result=CommandResult.decode(transport.participant(descriptor,record));
            if (result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=0) throw new IOException("Invalid HOME retirement applied reply");
        } finally { endCall(); }
    }
    public void cancelHomeRetirement(HomeRetirement retirement) throws IOException {
        Objects.requireNonNull(retirement); var descriptor=homeRoute(retirement.identity()); beginCall();
        try {
            byte[] reply=transactionReply(descriptor,ScopedTransactionRpc.HOME_RETIRE_CANCEL,RatisHomeRetirementRecord.encode(retirement),true);
            if (reply.length!=33 || !Arrays.equals(Arrays.copyOf(reply,32),retirement.digest()) || reply[32]!=0) throw new IOException("Invalid HOME retirement cancellation reply");
        } finally { endCall(); }
    }
    /** Reclaim only the exact terminal family after qualified server-side consumed HOME validation. */
    public void retireParticipant(ParticipantRetirement retirement) throws IOException {
        Objects.requireNonNull(retirement); var route=prepareFinalization(retirement.finalization()); beginCall();
        try {
            byte[] record=RatisParticipantRecord.encode(retirement),digest=RatisParticipantRecord.digest(retirement);
            byte[] reply=transactionReply(route.descriptor,ScopedTransactionRpc.PARTICIPANT_RETIRE_PREPARE,record,true);
            if (reply.length!=33 || !Arrays.equals(Arrays.copyOf(reply,32),digest) || (reply[32]&255)>1) throw new IOException("Invalid participant retirement authority reply");
            if (reply[32]==1) return;
            var result=CommandResult.decode(transport.participant(route.descriptor,record));
            if (result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=0) throw new IOException("Invalid participant retirement applied reply");
        } finally { endCall(); }
    }
    public void cancelParticipantRetirement(ParticipantRetirement retirement) throws IOException {
        Objects.requireNonNull(retirement); var route=prepareFinalization(retirement.finalization()); beginCall();
        try {
            byte[] reply=transactionReply(route.descriptor,ScopedTransactionRpc.PARTICIPANT_RETIRE_CANCEL,RatisParticipantRecord.encode(retirement),true);
            if (reply.length!=33 || !Arrays.equals(Arrays.copyOf(reply,32),RatisParticipantRecord.digest(retirement)) || reply[32]!=0) throw new IOException("Invalid participant retirement cancellation reply");
        } finally { endCall(); }
    }
    /** A single fresh native cut binds absence and the durable aggregate business fence. */
    public ParticipantFenceProof participantFence(ParticipantIdentity identity) throws IOException {
        Objects.requireNonNull(identity); var observed=current(); var participant=identity.participant();
        var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(identity.participantGroup())).findFirst().orElseThrow(() -> new IllegalArgumentException("Participant route absent"));
        if (!observed.clusterId().equals(identity.definition().clusterId()) || !observed.databaseId().equals(identity.definition().databaseId()) || !observed.domainId().equals(identity.definition().domainId())
                || !descriptor.rangeId().equals(participant.rangeId()) || descriptor.rangeEpoch()!=participant.rangeEpoch() || !Arrays.equals(observed.schemaDigest(),participant.schemaDigest())) throw new IllegalArgumentException("Participant fence route differs");
        try {
            var input=ByteBuffer.wrap(transaction(descriptor,ScopedTransactionRpc.PARTICIPANT_FENCE,identity.encode(),true));
            if (input.remaining()<36) throw new IllegalArgumentException("Truncated participant fence reply"); byte[] digest=new byte[32]; input.get(digest);
            byte[] bytes=ScopedTransactionRpc.blob(input,ParticipantFenceProof.MAX_BYTES,true);
            if (!Arrays.equals(digest,identity.digest()) || input.hasRemaining()) throw new IllegalArgumentException("Participant fence binding changed");
            if (bytes==null) return null; var proof=ParticipantFenceProof.decode(bytes);
            if (!proof.identity().equals(identity)) throw new IllegalArgumentException("Participant fence identity changed"); return proof;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid participant fence reply",malformed); }
    }
    private final RangeDirectory identity;
    private final Transport transport;
    private RangeDirectory directory;
    private int activeCalls;
    private boolean closing;
    private CompletableFuture<Void> closeAttempt;

    public RatisRoutedClient(RangeDirectory directory, Transport transport) {
        this.identity = this.directory = Objects.requireNonNull(directory, "directory");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    public static RatisRoutedClient connect(RangeDirectory directory) {
        return new RatisRoutedClient(directory, new RaftTransport());
    }
    public ScopedTransaction openTransaction(TransactionScope scope,java.time.Duration ttl) throws IOException {
        Objects.requireNonNull(scope); long millis=Objects.requireNonNull(ttl).toMillis();
        if (millis<1 || millis>60000 || !ttl.equals(java.time.Duration.ofMillis(millis))) throw new IllegalArgumentException("Transaction TTL must be 1..60000 whole milliseconds");
        var observed=current(); var participants=scope.participants(observed,1); if (participants.size()!=1) throw new IllegalArgumentException("Transaction requires one complete participant");
        var descriptor=participants.getFirst(); var nonce=UUID.randomUUID(); byte[] encodedScope=ScopedTransactionRpc.scopeBytes(scope);
        var input=ByteBuffer.allocate(24+encodedScope.length); ScopedTransactionRpc.uuid(input,nonce); input.putLong(millis).put(encodedScope);
        byte[] reply=transaction(descriptor,ScopedTransactionRpc.OPEN,input.array(),true);
        try {
            var output=ByteBuffer.wrap(reply); var echoed=ScopedTransactionRpc.uuid(output); var handle=ScopedTransactionRpc.readHandle(output);
            if (output.hasRemaining() || !echoed.equals(nonce)) throw new IOException("Transaction open reply differs from request identity");
            return new ScopedTransaction(descriptor,handle,scope,observed.schemaDigest());
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid transaction open reply",malformed); }
    }
    public final class ScopedTransaction implements AutoCloseable {
        private final RangeDescriptor descriptor; private final ScopedTransactionRpc.Handle handle;
        private final TransactionScope scope;
        private final byte[] schemaDigest;
        private boolean closed;
        private ScopedTransaction(RangeDescriptor descriptor,ScopedTransactionRpc.Handle handle,TransactionScope scope,byte[] schemaDigest) { this.descriptor=descriptor; this.handle=handle; this.scope=scope; this.schemaDigest=schemaDigest.clone(); }
        public ApplyPosition snapshot() { return handle.position(); }
        public RangeDescriptor descriptor() { return descriptor; }
        public byte[] get(UUID column,byte[] key) throws IOException {
            Objects.requireNonNull(key); if (key.length>DistributedCommandCodec.MAX_ENCODED_BYTES-16) throw new IllegalArgumentException("Transaction key exceeds budget");
            var body=ByteBuffer.allocate(76+key.length); ScopedTransactionRpc.writeHandle(body,handle); ScopedTransactionRpc.uuid(body,column); ScopedTransactionRpc.putBlob(body,key);
            var reply=ByteBuffer.wrap(call(ScopedTransactionRpc.GET,body.array()));
            try {
                var value=ScopedTransactionRpc.blob(reply,DistributedCommandCodec.MAX_ENCODED_BYTES,true);
                if (reply.hasRemaining()) throw new IOException("Trailing transaction point reply"); return value;
            } catch (IllegalArgumentException malformed) { throw new IOException("Invalid transaction point reply",malformed); }
        }
        public PhysicalRangePage page(UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws IOException {
            long size=93L+(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length)+(resume==null ? 0 : resume.length);
            if (size>ScopedTransactionRpc.MAX_REQUEST_BYTES-ScopedTransactionRpc.HEADER_BYTES) throw new IllegalArgumentException("Transaction page request exceeds budget");
            var body=ByteBuffer.allocate((int)size); ScopedTransactionRpc.writeHandle(body,handle); ScopedTransactionRpc.uuid(body,column);
            ScopedTransactionRpc.putBlob(body,lower); ScopedTransactionRpc.putBlob(body,upper); body.put((byte)(reverse ? 1 : 0)); ScopedTransactionRpc.putBlob(body,resume); body.putInt(items).putInt(bytes);
            byte[] reply=call(ScopedTransactionRpc.PAGE,body.array());
            try { return ScopedTransactionRpc.decodePage(reply); } catch (IllegalArgumentException malformed) { throw new IOException("Invalid transaction page reply",malformed); }
        }
        /** Replaces an ephemeral staged prefix with an equal or extended one. No mutation is submitted to consensus. */
        public CommandResult stage(List<Mutation> mutations,List<KeyPredicate> predicates) throws IOException {
            var prefix=new DistributedCommand(descriptor.groupId(),descriptor.rangeEpoch(),schemaDigest,new OperationId(handle.id(),0,0),mutations,predicates);
            byte[] encoded=DistributedCommandCodec.encode(prefix); var body=ByteBuffer.allocate(60+encoded.length); ScopedTransactionRpc.writeHandle(body,handle); ScopedTransactionRpc.putBlob(body,encoded);
            byte[] reply=call(ScopedTransactionRpc.STAGE,body.array());
            if (reply.length<32 || !Arrays.equals(prefix.payloadDigest(),Arrays.copyOf(reply,32))) throw new IOException("Staging reply differs from requested prefix");
            try {
                var result=CommandResult.decode(Arrays.copyOfRange(reply,32,reply.length));
                long returns=prefix.mutations().stream().filter(m -> m.returnMode()!=Mutation.ReturnMode.NONE).count();
                if (result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=returns) throw new IOException("Staging reply differs from requested result plan");
                return result;
            } catch (IllegalArgumentException malformed) { throw new IOException("Invalid staging reply",malformed); }
        }
        public CommandResult stage(List<Mutation> mutations) throws IOException { return stage(mutations,List.of()); }
        public byte[] participantPlanDigest(DistributedCommand intent,CommandResult stagedResult) throws IOException { return participantIntent(intent,stagedResult).payloadDigest(); }
        /** Freeze an accepted-plan image for registration/journaling; server PREPARE authority is still required. */
        public DistributedCommand participantIntent(DistributedCommand intent,CommandResult stagedResult) throws IOException {
            synchronized (this) { if (closed) throw new IOException("Transaction handle is closed"); }
            if (!intent.groupId().equals(descriptor.groupId()) || intent.rangeEpoch()!=descriptor.rangeEpoch() || !Arrays.equals(intent.schemaDigest(),schemaDigest)) throw new IllegalArgumentException("Participant intent differs from frozen route");
            var audit=new TransactionAcceptance(handle.position(),handle.generation(),handle.owner(),scope,stagedResult.encode());
            return DistributedCommand.acceptedTransaction(intent,audit);
        }
        public DistributedCommand plan(DistributedCommand intent) throws IOException {
            byte[] encoded=DistributedCommandCodec.encode(intent); var body=ByteBuffer.allocate(60+encoded.length); ScopedTransactionRpc.writeHandle(body,handle); ScopedTransactionRpc.putBlob(body,encoded);
            byte[] reply=call(ScopedTransactionRpc.PLAN,body.array());
            try {
                var accepted=DistributedCommandCodec.decode(reply);
                if (accepted.acceptance()==null || !Arrays.equals(DistributedCommandCodec.encode(accepted),DistributedCommandCodec.encode(DistributedCommand.acceptedTransaction(intent,accepted.acceptance())))) throw new IOException("Accepted reply differs from original transaction intent");
                var audit=accepted.acceptance();
                if (!audit.snapshot().equals(handle.position()) || audit.leaderGeneration()!=handle.generation() || !audit.ownerIncarnation().equals(handle.owner())
                        || !Arrays.equals(ScopedTransactionRpc.scopeBytes(scope),ScopedTransactionRpc.scopeBytes(audit.scope()))) throw new IOException("Accepted reply changed transaction capture identity/scope");
                return accepted;
            } catch (IllegalArgumentException malformed) { throw new IOException("Invalid accepted transaction reply",malformed); }
        }
        private byte[] call(int kind,byte[] body) throws IOException {
            synchronized (this) { if (closed) throw new IOException("Transaction handle is closed"); }
            return transaction(descriptor,kind,body,false);
        }
        @Override public void close() throws IOException {
            synchronized (this) { if (closed) return; }
            var body=ByteBuffer.allocate(ScopedTransactionRpc.HANDLE_BYTES); ScopedTransactionRpc.writeHandle(body,handle);
            if (transaction(descriptor,ScopedTransactionRpc.CLOSE,body.array(),false).length!=0) throw new IOException("Invalid transaction close reply");
            synchronized (this) { closed=true; }
        }
    }
    private byte[] transaction(RangeDescriptor descriptor,int kind,byte[] body,boolean freshBarrier) throws IOException {
        beginCall();
        try { return transactionReply(descriptor,kind,body,freshBarrier); } finally { endCall(); }
    }
    private byte[] transactionReply(RangeDescriptor descriptor,int kind,byte[] body,boolean freshBarrier) throws IOException {
        byte[] request=ScopedTransactionRpc.frame(kind,body); if (request.length>ScopedTransactionRpc.MAX_REQUEST_BYTES) throw new IllegalArgumentException("Transaction request exceeds byte budget");
        try {
            var packet=ScopedTransactionRpc.parse(transport.transaction(descriptor,request,freshBarrier),ScopedTransactionRpc.MAX_REPLY_BYTES);
            if (packet.kind()!=kind) throw new IOException("Transaction reply opcode differs from request"); return packet.body();
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid scoped transaction reply",malformed); }
    }

    public synchronized RangeDirectory directory() { return directory; }

    public synchronized void refresh(RangeDirectory next) {
        requireOpen();
        Objects.requireNonNull(next, "next");
        requireIdentity(next);
        if (next.generation() < directory.generation()
                || (next.generation() == directory.generation() && !Arrays.equals(next.payloadDigest(), directory.payloadDigest()))) {
            throw new IllegalArgumentException("Stale or conflicting directory generation");
        }
        directory = next;
    }

    record RoutedRead(RangeDescriptor descriptor,byte[] value) {}
    public byte[] read(UUID columnId,byte[] physicalKey) throws IOException { return readWithRoute(columnId,physicalKey).value(); }
    RoutedRead readWithRoute(UUID columnId,byte[] physicalKey) throws IOException {
        Objects.requireNonNull(physicalKey, "physicalKey");
        if (physicalKey.length > DistributedCommandCodec.MAX_ENCODED_BYTES - 16) throw new IllegalArgumentException("Point read key exceeds budget");
        byte[] key = physicalKey.clone();
        RangeDirectory observed = current();
        RangeDescriptor descriptor = observed.route(columnId, key);
        beginCall();
        try {
            byte[] value = transport.read(descriptor, columnId, key);
            if (value != null && value.length > DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IOException("Point value exceeds budget");
            return new RoutedRead(descriptor,value==null ? null : value.clone());
        } finally { endCall(); }
    }

    /** Linearizable metadata read through the same pooled Ratis transport; scope is verified against the captured directory. */
    public RangeStateView stateView(UUID groupId) throws IOException {
        Objects.requireNonNull(groupId); var observed=current();
        var descriptor=observed.ranges().stream().filter(range -> range.groupId().equals(groupId)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Range-state group is absent from directory"));
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Range-state group is inactive");
        beginCall();
        try {
            var view=transport.stateView(descriptor); if (view==null) throw new IOException("Missing range-state reply");
            try { view.requireMatches(observed,descriptor); }
            catch (IllegalArgumentException mismatch) { throw new IOException("Range-state reply differs from routed identity",mismatch); }
            return view;
        } finally { endCall(); }
    }

    /** Freeze a complete multi-group business plan before snapshots, allocation or any network dispatch. */
    public RatisTransactionPlan transactionPlan(OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates) {
        return RatisTransactionPlan.create(current(),operation,mutations,predicates);
    }

    public PreparedCommand prepare(OperationId operation, List<Mutation> mutations) {
        return prepare(operation, mutations, List.of());
    }

    public PreparedCommand prepare(OperationId operation, List<Mutation> mutations, List<KeyPredicate> predicates) {
        return prepareInternal(operation, mutations, predicates, -1);
    }

    /** Piggyback a contiguous acknowledgement from this operation's own session/epoch. */
    public PreparedCommand prepare(OperationId operation, List<Mutation> mutations, List<KeyPredicate> predicates,
                                   long acknowledgedThrough) {
        if (acknowledgedThrough < 0) throw new IllegalArgumentException("Negative acknowledgement");
        return prepareInternal(operation, mutations, predicates, acknowledgedThrough);
    }

    private PreparedCommand prepareInternal(OperationId operation, List<Mutation> mutations,
                                             List<KeyPredicate> predicates, long acknowledgedThrough) {
        RangeDirectory observed = current();
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(mutations, "mutations");
        Objects.requireNonNull(predicates, "predicates");
        if (mutations.isEmpty() && predicates.isEmpty() || (long) mutations.size() + predicates.size() > DistributedCommandCodec.MAX_ITEMS) {
            throw new IllegalArgumentException("Invalid atomic command item count");
        }
        mutations = List.copyOf(mutations);
        predicates = List.copyOf(predicates);
        RangeDescriptor target = null;
        for (Mutation mutation : mutations) {
            RangeDescriptor routed = observed.route(mutation.columnId(), mutation.key());
            target = sameRange(target, routed);
            if (mutation.kind() == Mutation.Kind.DELETE_RANGE) {
                for (RangeDescriptor covered : observed.intersect(mutation.columnId(), mutation.key(), mutation.value(), false)) {
                    target = sameRange(target, covered);
                }
            }
        }
        for (KeyPredicate predicate : predicates) target = sameRange(target, observed.route(predicate.columnId(), predicate.key()));
        boolean bucket=mutations.stream().anyMatch(Mutation::bucket) || predicates.stream().anyMatch(KeyPredicate::bucket);
        var command = bucket ? DistributedCommand.bucketData(target.groupId(),target.rangeEpoch(),observed.schemaDigest(),operation,mutations,predicates,acknowledgedThrough)
                : acknowledgedThrough < 0
                ? new DistributedCommand(target.groupId(), target.rangeEpoch(), observed.schemaDigest(), operation, mutations, predicates)
                : new DistributedCommand(target.groupId(), target.rangeEpoch(), observed.schemaDigest(), operation, mutations, predicates, acknowledgedThrough);
        return new PreparedCommand(observed, target, command);
    }

    /** The routing key selects one range; acknowledgement does not mutate that user key. */
    public PreparedCommand prepareAcknowledgement(OperationId operation, UUID columnId, byte[] physicalKey,
                                                  long acknowledgedThrough) {
        Objects.requireNonNull(physicalKey);
        if (physicalKey.length > DistributedCommandCodec.MAX_ENCODED_BYTES - 16) throw new IllegalArgumentException("Routing key exceeds budget");
        var observed = current();
        var descriptor = observed.route(columnId, physicalKey.clone());
        var command = new DistributedCommand(descriptor.groupId(), descriptor.rangeEpoch(), observed.schemaDigest(),
                operation, List.of(), List.of(), acknowledgedThrough);
        return new PreparedCommand(observed, descriptor, command);
    }

    /** Keep a catalogue-prepared control immutable and route it to its exact active range incarnation. */
    public PreparedCommand prepareSessionControl(DistributedCommand command) {
        Objects.requireNonNull(command);
        var observed=current(); var control=command.sessionControl();
        if (control==null || !control.authority().clusterId().equals(observed.clusterId())
                || !control.authority().databaseId().equals(observed.databaseId())
                || !Arrays.equals(command.schemaDigest(),observed.schemaDigest())) throw new IllegalArgumentException("Wrong session control identity/schema");
        var descriptor=observed.ranges().stream().filter(range -> range.groupId().equals(command.groupId())).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Control range is absent from directory"));
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || descriptor.rangeEpoch()!=command.rangeEpoch()) {
            throw new IllegalArgumentException("Control range is inactive or changed incarnation");
        }
        return new PreparedCommand(observed,descriptor,command);
    }

    // Journal recovery preserves the exact original peer route even after a directory refresh.
    PreparedCommand restoreSessionControl(DistributedCommand command,RangeDescriptor descriptor) {
        var observed=current(); var control=command.sessionControl();
        if (control==null || !control.authority().clusterId().equals(observed.clusterId())
                || !control.authority().databaseId().equals(observed.databaseId())
                || !Arrays.equals(command.schemaDigest(),observed.schemaDigest())
                || !command.groupId().equals(descriptor.groupId()) || command.rangeEpoch()!=descriptor.rangeEpoch()
                || descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Invalid recovered control route");
        return new PreparedCommand(identity,descriptor,command);
    }
    /** Restore an immutable user retry route without rebasing it onto the current topology. */
    PreparedCommand restoreSessionCommand(DistributedCommand command,RangeDescriptor descriptor) {
        Objects.requireNonNull(command); Objects.requireNonNull(descriptor); var observed=current();
        if (command.kind()!=DistributedCommand.Kind.DATA && command.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED || command.sessionControl()!=null || command.minimumSessionEpoch()!=-1 || !Arrays.equals(command.schemaDigest(),observed.schemaDigest())
                || !command.groupId().equals(descriptor.groupId()) || command.rangeEpoch()!=descriptor.rangeEpoch()
                || descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE
                || command.mutations().isEmpty() && command.predicates().isEmpty() && command.acknowledgedThrough()<0) {
            throw new IllegalArgumentException("Invalid recovered user command route");
        }
        int prefix=observed.routingPrefixBytes();
        if (prefix>0 && (descriptor.lowerInclusive()!=null && descriptor.lowerInclusive().length!=prefix
                || descriptor.upperExclusive()!=null && descriptor.upperExclusive().length!=prefix)) throw new IllegalArgumentException("Recovered bounds differ from projection");
        for (var mutation:command.mutations()) {
            recoveredKey(observed,descriptor,mutation.columnId(),mutation.key());
            if (mutation.kind()==Mutation.Kind.DELETE_RANGE) {
                byte[] upper=projectRecovered(observed,mutation.value()); int order=descriptor.upperExclusive()==null ? -1 : Arrays.compareUnsigned(upper,descriptor.upperExclusive());
                if (order>0 || order==0 && observed.routingPrefixBytes()>0 && mutation.value().length>observed.routingPrefixBytes()) {
                    throw new IllegalArgumentException("Recovered range deletion crosses frozen route");
                }
            }
        }
        for (var predicate:command.predicates()) recoveredKey(observed,descriptor,predicate.columnId(),predicate.key());
        if (command.acceptance()!=null) for (var span:command.acceptance().scope().spans()) {
            if (span.isPoint()) { recoveredKey(observed,descriptor,span.columnId(),span.lowerInclusive()); continue; }
            if (!observed.columns().contains(span.columnId())) throw new IllegalArgumentException("Recovered transaction scope belongs to another column");
            byte[] lower=span.lowerInclusive(), upper=span.upperExclusive(), routeLower=descriptor.lowerInclusive(), routeUpper=descriptor.upperExclusive();
            if (routeLower!=null && (lower==null || Arrays.compareUnsigned(projectRecovered(observed,lower),routeLower)<0)
                    || routeUpper!=null && (upper==null || Arrays.compareUnsigned(projectRecovered(observed,upper),routeUpper)>0
                    || Arrays.equals(projectRecovered(observed,upper),routeUpper) && observed.routingPrefixBytes()>0 && upper.length>observed.routingPrefixBytes())) {
                throw new IllegalArgumentException("Recovered transaction scope crosses frozen route");
            }
        }
        return new PreparedCommand(identity,descriptor,command);
    }
    private static byte[] projectRecovered(RangeDirectory directory,byte[] key) {
        int prefix=directory.routingPrefixBytes();
        if (prefix>key.length) throw new IllegalArgumentException("Recovered key is shorter than routing prefix");
        return prefix==0 ? key : Arrays.copyOf(key,prefix);
    }
    private static void recoveredKey(RangeDirectory directory,RangeDescriptor descriptor,UUID column,byte[] key) {
        if (!directory.columns().contains(column)) throw new IllegalArgumentException("Recovered key belongs to another column");
        byte[] projected=projectRecovered(directory,key), lower=descriptor.lowerInclusive(), upper=descriptor.upperExclusive();
        if (lower!=null && Arrays.compareUnsigned(projected,lower)<0 || upper!=null && Arrays.compareUnsigned(projected,upper)>=0) {
            throw new IllegalArgumentException("Recovered key is outside frozen route");
        }
    }
    byte[] sessionIdentityDigest() {
        var output=ByteBuffer.allocate(96+16*identity.columns().size());
        output.putLong(identity.clusterId().getMostSignificantBits()).putLong(identity.clusterId().getLeastSignificantBits());
        output.putLong(identity.databaseId().getMostSignificantBits()).putLong(identity.databaseId().getLeastSignificantBits());
        output.putLong(identity.domainId().getMostSignificantBits()).putLong(identity.domainId().getLeastSignificantBits());
        output.putLong(identity.schemaEpoch()).put(identity.schemaDigest()).putInt(identity.routingPrefixBytes()).putInt(identity.columns().size());
        for (var id:identity.columns().stream().sorted().toList()) output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
        try { return java.security.MessageDigest.getInstance("SHA-256").digest(output.array()); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    public CommandResult execute(PreparedCommand prepared) throws IOException {
        Objects.requireNonNull(prepared, "prepared");
        requireIdentity(prepared.source);
        beginCall();
        try {
            CommandResult result = transport.command(prepared.descriptor(), prepared.command());
            if (result == null) throw new IOException("Missing command result");
            return result;
        } finally { endCall(); }
    }

    private synchronized RangeDirectory current() { requireOpen(); return directory; }
    private void requireOpen() { if (closing) throw new IllegalStateException("Routed client is closing"); }
    private void requireIdentity(RangeDirectory other) {
        if (!identity.clusterId().equals(other.clusterId()) || !identity.databaseId().equals(other.databaseId())
                || !identity.domainId().equals(other.domainId()) || identity.schemaEpoch() != other.schemaEpoch()
                || !Arrays.equals(identity.schemaDigest(), other.schemaDigest()) || !identity.columns().equals(other.columns())
                || identity.routingPrefixBytes() != other.routingPrefixBytes()) {
            throw new IllegalArgumentException("Directory belongs to another cluster/database/domain/schema");
        }
    }
    private static RangeDescriptor sameRange(RangeDescriptor target, RangeDescriptor next) {
        if (target != null && (!target.groupId().equals(next.groupId()) || target.rangeEpoch() != next.rangeEpoch())) {
            throw new IllegalArgumentException("Cross-group atomic commands require an unsupported coordination protocol");
        }
        return target == null ? next : target;
    }
    private synchronized void beginCall() throws IOException {
        requireOpen();
        if (activeCalls >= MAX_CONCURRENT_CALLS) throw new IOException("Routed client call budget exhausted");
        activeCalls++;
    }
    private synchronized void endCall() { activeCalls--; notifyAll(); }

    /** Stops admission and drains existing calls before releasing transport resources; it does not cancel committed work. */
    @Override public void close() throws IOException {
        boolean owner;
        boolean interrupted = false;
        CompletableFuture<Void> attempt;
        synchronized (this) {
            closing = true;
            owner = closeAttempt == null || closeAttempt.isCompletedExceptionally();
            if (owner) closeAttempt = new CompletableFuture<>();
            attempt = closeAttempt;
            if (owner) {
                while (activeCalls != 0) {
                    try { wait(); }
                    catch (InterruptedException interruption) { interrupted = true; }
                }
            }
        }
        if (owner) {
            try { transport.close(); attempt.complete(null); }
            catch (IOException | RuntimeException | Error failure) { attempt.completeExceptionally(failure); }
        }
        try { attempt.join(); }
        catch (CompletionException failure) { throw new IOException("Failed to close routed transport", failure.getCause()); }
        finally { if (interrupted) Thread.currentThread().interrupt(); }
    }

    /** Addresses the frozen peer directly; capture creation additionally verifies trusted home authority on that participant. */
    static byte[] snapshotOnOwner(RaftClient client,RangeDescriptor descriptor,ReplicaDescriptor owner,byte[] request) throws IOException {
        if (!descriptor.replicas().contains(owner)) throw new IllegalArgumentException("Snapshot owner is absent from frozen route");
        var packet=ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);
        if (!ScopedTransactionRpc.ownerSnapshot(packet.kind())) throw new IllegalArgumentException("Owner transport accepts only bound snapshot operations");
        var reply=client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(request)),0,RaftPeerId.valueOf(owner.peerId()));
        if (reply!=null && (!reply.getServerId().equals(RaftPeerId.valueOf(owner.peerId())) || !reply.getRaftGroupId().equals(RaftGroupId.valueOf(descriptor.groupId())))) throw new IOException("Snapshot reply came from another peer/group");
        return RaftTransport.content(reply,ScopedTransactionRpc.MAX_REPLY_BYTES);
    }

    private static final class RaftTransport implements Transport {
        private final RatisClientPool clients = new RatisClientPool(MAX_CONCURRENT_CALLS, RaftTransport::connected,RaftTransport::connectedOwner);

        @Override public void close() throws IOException { clients.close(); }
        @Override public byte[] read(RangeDescriptor descriptor, UUID column, byte[] key) throws IOException {
            byte[] reply=transaction(descriptor,ScopedTransactionRpc.pointRequest(column,key),true);
            try { return ScopedTransactionRpc.pointValue(reply,DistributedCommandCodec.MAX_ENCODED_BYTES); }
            catch (IllegalArgumentException malformed) { throw new IOException("Invalid point reply",malformed); }
        }
        @Override public RangeStateView stateView(RangeDescriptor descriptor) throws IOException {
            byte[] reply=transaction(descriptor,ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]),true);
            try {
                var packet=ScopedTransactionRpc.parse(reply,ScopedTransactionRpc.MAX_REPLY_BYTES);
                if (packet.kind()!=ScopedTransactionRpc.STATE) throw new IOException("Unexpected state reply"); return RangeStateView.decode(packet.body());
            } catch (IllegalArgumentException malformed) { throw new IOException("Invalid state reply",malformed); }
        }

        @Override public CommandResult command(RangeDescriptor descriptor, DistributedCommand command) throws IOException {
            return rpc(() -> { try (var lease = clients.acquire(descriptor)) {
                RaftClient client = lease.client();
                byte[] encoded = content(client.io().send(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))));
                try { return CommandResult.decode(encoded); }
                catch (IllegalArgumentException invalid) { throw new IOException("Invalid command reply", invalid); }
            } });
        }
        @Override public byte[] transaction(RangeDescriptor descriptor,byte[] request,boolean freshBarrier) throws IOException {
            return rpc(() -> { try (var lease=clients.acquire(descriptor)) {
                var message=Message.valueOf(ByteString.copyFrom(request));
                return content(freshBarrier ? lease.client().io().sendReadOnly(message) : lease.client().io().sendReadOnlyNonLinearizable(message),ScopedTransactionRpc.MAX_REPLY_BYTES);
            } });
        }

        @Override public byte[] snapshot(RangeDescriptor descriptor,ReplicaDescriptor owner,byte[] request) throws IOException {
            return rpc(() -> { try (var lease=clients.acquireOwner(descriptor,owner)) { return snapshotOnOwner(lease.client(),descriptor,owner,request); } });
        }
        @Override public byte[] participant(RangeDescriptor descriptor,byte[] record) throws IOException {
            return rpc(() -> { try (var lease=clients.acquire(descriptor)) { return content(lease.client().io().send(Message.valueOf(ByteString.copyFrom(record)))); } });
        }

        private static RaftClient connected(RangeDescriptor descriptor) throws IOException {
            var group=group(descriptor); var properties=properties();
            RaftPeerId leader = null;
            try (RaftClient probe = build(group, properties, null)) {
                for (ReplicaDescriptor replica : descriptor.replicas()) {
                    if (replica.role() != ReplicaDescriptor.Role.VOTER) continue;
                    try {
                        var info = rpc(() -> probe.getGroupManagementApi(RaftPeerId.valueOf(replica.peerId())).info(group.getGroupId()));
                        if (info.isSuccess() && info.isRaftStorageHealthy() && info.getGroup() != null
                                && info.getGroup().getGroupId().equals(group.getGroupId())
                                && info.getRoleInfoProto().getRole() == RaftPeerRole.LEADER) {
                            leader = RaftPeerId.valueOf(replica.peerId()); break;
                        }
                    } catch (IOException unavailable) {
                        // A bounded read-only probe can inspect another voter; no user mutation was submitted.
                    }
                }
            }
            if (leader == null) throw new IOException("No healthy range leader discovered");
            return build(group, properties, leader);
        }

        private static RaftClient connectedOwner(RangeDescriptor descriptor,ReplicaDescriptor owner) {
            return build(group(descriptor),properties(),RaftPeerId.valueOf(owner.peerId()));
        }
        private static RaftGroup group(RangeDescriptor descriptor) {
            return RaftGroup.valueOf(RaftGroupId.valueOf(descriptor.groupId()),descriptor.replicas().stream().map(replica -> RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address())
                    .setStartupRole(replica.role()==ReplicaDescriptor.Role.VOTER ? RaftPeerRole.FOLLOWER : RaftPeerRole.LISTENER).build()).toList());
        }
        private static RaftProperties properties() {
            var properties=new RaftProperties(); RaftConfigKeys.Rpc.setType(properties,SupportedRpcType.GRPC);
            RaftClientConfigKeys.Rpc.setRequestTimeout(properties,TimeDuration.valueOf(3,TimeUnit.SECONDS)); GrpcConfigKeys.Client.setWorkerGroupSize(properties,1); return properties;
        }
        private static RaftClient build(RaftGroup group, RaftProperties properties, RaftPeerId leader) {
            var builder = RaftClient.newBuilder().setRaftGroup(group).setProperties(properties).setRetryPolicy(RetryPolicies.noRetry());
            if (leader != null) builder.setLeaderId(leader);
            return builder.build();
        }

        private static byte[] content(RaftClientReply reply) throws IOException {
            return content(reply,DistributedCommandCodec.MAX_ENCODED_BYTES);
        }
        private static byte[] content(RaftClientReply reply,int maximum) throws IOException {
            if (reply == null || !reply.isSuccess() || reply.getMessage() == null) {
                throw new IOException("Raft operation did not return a successful application reply", reply == null ? null : reply.getException());
            }
            var content = reply.getMessage().getContent();
            if (content == null || content.size() > maximum) throw new IOException("Application reply exceeds budget");
            return content.toByteArray();
        }
    }

    @FunctionalInterface
    interface RpcOperation<T> { T call() throws IOException; }

    // Released shaded gRPC can throw unchecked status failures from admin/send/close calls.
    // Normalize only that transport failure; caller argument/programming errors remain visible.
    static <T> T rpc(RpcOperation<T> operation) throws IOException {
        try { return operation.call(); }
        catch (StatusRuntimeException unavailable) { throw new IOException("Raft gRPC transport failed", unavailable); }
    }
}
