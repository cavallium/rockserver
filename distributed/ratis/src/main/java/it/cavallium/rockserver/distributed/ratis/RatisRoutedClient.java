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
import java.util.Map;
import java.util.HashMap;
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
        /** Null owner requests fresh SNAPSHOT_OWNER discovery; other requests use the exact frozen voter. */
        default CompletableFuture<byte[]> snapshot(RangeDescriptor descriptor,ReplicaDescriptor owner,byte[] request) throws IOException { return CompletableFuture.failedFuture(new IOException("Owner-specific snapshots unsupported by this transport")); }
        default RangeStateView stateView(RangeDescriptor descriptor) throws IOException { throw new IOException("Range-state reads unsupported by this transport"); }
        default byte[] transaction(RangeDescriptor descriptor,byte[] request,boolean freshBarrier) throws IOException { throw new IOException("Scoped transactions unsupported by this transport"); }
        default CompletableFuture<byte[]> readAsync(RangeDescriptor descriptor,UUID column,byte[] key) throws IOException { return CompletableFuture.failedFuture(new IOException("Asynchronous point reads unsupported by this transport")); }
        default CompletableFuture<CommandResult> commandAsync(RangeDescriptor descriptor,DistributedCommand command) throws IOException { return CompletableFuture.failedFuture(new IOException("Asynchronous commands unsupported by this transport")); }
        default CompletableFuture<byte[]> transactionAsync(RangeDescriptor descriptor,byte[] request,boolean freshBarrier) throws IOException { return CompletableFuture.failedFuture(new IOException("Asynchronous scoped transactions unsupported by this transport")); }
        default CompletableFuture<byte[]> participantAsync(RangeDescriptor descriptor,byte[] record) throws IOException { return CompletableFuture.failedFuture(new IOException("Asynchronous participant commands unsupported by this transport")); }
        @Override default void close() throws IOException {}
    }

    /** Returns whole envelopes; consumers skip event cursors covered by from. GAP requires snapshot resynchronization. */
    public RatisCdcPollRpc.Result pollCdc(it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage lineage,
            it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Cursor from,int maxRecords,int maxBytes) throws IOException {
        return pollCdcDirectory(current(),lineage,from,maxRecords,maxBytes);
    }
    /** Trusted catalogue authority routes other domains in this database using native directory witnesses. */
    RatisCdcPollRpc.Result pollCdcDirectory(RangeDirectory directory,it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage lineage,
            it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Cursor from,int maxRecords,int maxBytes) throws IOException {
        Objects.requireNonNull(lineage); Objects.requireNonNull(directory);
        if(!identity.clusterId().equals(directory.clusterId()) || !identity.databaseId().equals(directory.databaseId())) throw new IllegalArgumentException("Foreign CDC database directory");
        var descriptor=directory.ranges().stream().filter(range->range.groupId().equals(lineage.groupId()) && range.rangeId().equals(lineage.rangeId())
                && range.rangeEpoch()==lineage.rangeEpoch() && range.topologyState()==RangeDescriptor.TopologyState.ACTIVE).findFirst()
                .orElseThrow(()->new IllegalArgumentException("CDC lineage is outside the current active directory"));
        var request=new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(directory.clusterId(),directory.databaseId(),directory.domainId(),lineage),from,maxRecords,maxBytes);
        beginCall();
        try { return RatisCdcPollRpc.decodeReply(transport.transaction(descriptor,RatisCdcPollRpc.request(request),true),request); }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid CDC poll reply",invalid); }
        finally { endCall(); }
    }

    /** Original administrative route; retries never rebase a pending change onto a new schema. */
    public static final class SchemaTarget {
        private final RangeDescriptor descriptor;
        private final RatisSchemaFenceRecord record;
        private final SchemaChange.Definition definition;
        private SchemaTarget(RangeDescriptor descriptor,RatisSchemaFenceRecord record,SchemaChange.Definition definition) { this.descriptor=descriptor; this.record=record; this.definition=definition; }
        public RangeDescriptor descriptor() { return descriptor; }
        public RatisSchemaFenceRecord record() { return record; }
    }
    public SchemaTarget schemaTarget(SchemaChange.Definition definition,UUID group) {
        return schemaTarget(definition,group,null);
    }
    public SchemaTarget schemaTarget(SchemaChange.Definition definition,UUID group,SchemaAdmissionManifest admissions) {
        Objects.requireNonNull(definition); var observed=current(); var original=definition.sourceDirectory();
        requireNamespace(original);
        if (!observed.clusterId().equals(original.clusterId()) || !observed.databaseId().equals(original.databaseId()) || !observed.domainId().equals(original.domainId()))
            throw new IllegalArgumentException("Schema change belongs to another routed database/domain");
        var descriptor=original.ranges().stream().filter(range -> range.groupId().equals(group)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Schema group is outside original participant set"));
        if (admissions!=null) admissions.requireDefinition(definition);
        return new SchemaTarget(descriptor,new RatisSchemaFenceRecord(definition.header(),group,admissions==null ? null : admissions.encode()),definition);
    }
    public NativeSchemaFence fenceSchema(SchemaTarget target) throws IOException {
        Objects.requireNonNull(target); requireNamespace(target.definition.sourceDirectory()); beginCall();
        try {
            RatisSchemaFenceRpc.decodePreparedReply(transport.transaction(target.descriptor,RatisSchemaFenceRpc.prepareRequest(target.record),true),ScopedTransactionRpc.SCHEMA_FENCE_PREPARE,target.record);
            var proof=NativeSchemaFence.decode(transport.participant(target.descriptor,target.record.encode()));
            proof.requireHeader(target.record.header());
            proof.requireAdmissionDigest(target.record.admissionDigest());
            if (!proof.participant().groupId().equals(target.descriptor.groupId())) throw new IllegalArgumentException("Schema FENCE reply names another participant");
            return proof;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema FENCE reply",invalid); }
        finally { endCall(); }
    }
    public RatisSchemaFenceRpc.State schemaFenceState(SchemaTarget target) throws IOException {
        Objects.requireNonNull(target); requireNamespace(target.definition.sourceDirectory()); beginCall();
        try { return RatisSchemaFenceRpc.stateReply(transport.transaction(target.descriptor,RatisSchemaFenceRpc.stateRequest(target.record),true),ScopedTransactionRpc.SCHEMA_FENCE_STATE,target.record); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema FENCE state reply",invalid); }
        finally { endCall(); }
    }
    public void cancelSchemaPreparation(SchemaTarget target) throws IOException {
        Objects.requireNonNull(target); requireNamespace(target.definition.sourceDirectory()); beginCall();
        try { RatisSchemaFenceRpc.decodePreparedReply(transport.transaction(target.descriptor,RatisSchemaFenceRpc.cancelRequest(target.record),true),ScopedTransactionRpc.SCHEMA_FENCE_CANCEL,target.record); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema preparation cancellation reply",invalid); }
        finally { endCall(); }
    }

    public static final class SchemaInstallTarget {
        private final RangeDescriptor descriptor;
        private final RatisSchemaInstallRecord record;
        private final NativeSchemaFence fence;
        private SchemaInstallTarget(RangeDescriptor descriptor,RatisSchemaInstallRecord record,NativeSchemaFence fence) { this.descriptor=descriptor; this.record=record; this.fence=fence; }
        public RangeDescriptor descriptor() { return descriptor; }
        public RatisSchemaInstallRecord record() { return record; }
    }
    public SchemaInstallTarget schemaInstallTarget(SchemaTarget target,NativeSchemaFence fence,String physicalName,boolean mergeAdmission) {
        Objects.requireNonNull(target); requireNamespace(target.definition.sourceDirectory()); Objects.requireNonNull(fence); fence.requireHeader(target.record.header());
        if (!fence.participant().groupId().equals(target.descriptor.groupId())) throw new IllegalArgumentException("Schema fence belongs to another group");
        fence.requireAdmissionDigest(target.record.admissionDigest());
        return new SchemaInstallTarget(target.descriptor,new RatisSchemaInstallRecord(target.definition,target.descriptor.groupId(),fence.position(),physicalName,mergeAdmission),fence);
    }
    public NativeSchemaInstallProof installSchema(SchemaInstallTarget target) throws IOException {
        Objects.requireNonNull(target); requireNamespace(target.record.definition().sourceDirectory()); beginCall();
        try {
            RatisSchemaInstallRpc.decodePreparedReply(transport.transaction(target.descriptor,RatisSchemaInstallRpc.prepareRequest(target.record),true),ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE,target.record);
            var proof=NativeSchemaInstallProof.decode(transport.participant(target.descriptor,target.record.encode()));
            requireInstallProof(target,proof); return proof;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema INSTALL reply",invalid); }
        finally { endCall(); }
    }
    private static void requireInstallProof(SchemaInstallTarget target,NativeSchemaInstallProof proof) {
        proof.requireHeader(target.record.header());
        if (!proof.fence().equals(target.fence))
            throw new IllegalArgumentException("Schema installation differs from original fence");
    }
    public RatisSchemaInstallRpc.State schemaInstallationState(SchemaInstallTarget target) throws IOException {
        Objects.requireNonNull(target); requireNamespace(target.record.definition().sourceDirectory()); beginCall();
        try { var state=RatisSchemaInstallRpc.stateReply(transport.transaction(target.descriptor,RatisSchemaInstallRpc.stateRequest(target.record),true),ScopedTransactionRpc.SCHEMA_INSTALL_STATE,target.record); if (state.installation()!=null) requireInstallProof(target,state.installation()); return state; }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema installation state",invalid); }
        finally { endCall(); }
    }
    public void cancelSchemaInstallation(SchemaInstallTarget target) throws IOException {
        Objects.requireNonNull(target); requireNamespace(target.record.definition().sourceDirectory()); beginCall();
        try { RatisSchemaInstallRpc.decodePreparedReply(transport.transaction(target.descriptor,RatisSchemaInstallRpc.cancelRequest(target.record),true),ScopedTransactionRpc.SCHEMA_INSTALL_CANCEL,target.record); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema installation cancellation",invalid); }
        finally { endCall(); }
    }

    public static final class SchemaActivationTarget {
        private final RangeDescriptor descriptor;
        private final RatisSchemaActivateRpc.Request request;
        private final RangeDirectory source;
        private final NativeSchemaInstallProof installation;
        private SchemaActivationTarget(RangeDescriptor descriptor,RatisSchemaActivateRpc.Request request,RangeDirectory source,NativeSchemaInstallProof installation) { this.descriptor=descriptor; this.request=request; this.source=source; this.installation=installation; }
        public RangeDescriptor descriptor() { return descriptor; }
        public RatisSchemaActivateRpc.Request request() { return request; }
    }
    public SchemaActivationTarget schemaActivationTarget(SchemaTarget target,NativeSchemaInstallProof installation) {
        Objects.requireNonNull(target); requireNamespace(target.definition.sourceDirectory()); Objects.requireNonNull(installation); installation.requireHeader(target.record.header());
        if (!installation.fence().participant().groupId().equals(target.descriptor.groupId())) throw new IllegalArgumentException("Schema installation belongs to another group");
        installation.fence().requireAdmissionDigest(target.record.admissionDigest());
        return new SchemaActivationTarget(target.descriptor,new RatisSchemaActivateRpc.Request(target.record.header(),target.descriptor.groupId(),installation.position()),target.definition.sourceDirectory(),installation);
    }
    public RatisSchemaActivateRecord prepareSchemaActivation(SchemaActivationTarget target) throws IOException {
        Objects.requireNonNull(target); requireNamespace(target.source); beginCall();
        try { return RatisSchemaActivateRpc.decodePreparedReply(transport.transaction(target.descriptor,RatisSchemaActivateRpc.prepareRequest(target.request),true),ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,target.request); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema ACTIVATE preparation",invalid); }
        finally { endCall(); }
    }
    public NativeSchemaActivationProof appendSchemaActivation(SchemaActivationTarget target,RatisSchemaActivateRecord record) throws IOException {
        Objects.requireNonNull(target); Objects.requireNonNull(record); requireNamespace(target.source); target.request.requireRecord(record); beginCall();
        try {
            var frozen=new RatisSchemaActivateRpc.Request(target.request.header(),target.request.groupId(),target.request.expectedInstallPosition(),record);
            RatisSchemaActivateRpc.decodePreparedReply(transport.transaction(target.descriptor,RatisSchemaActivateRpc.prepareRequest(frozen),true),ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,frozen);
            var proof=NativeSchemaActivationProof.decode(transport.participant(target.descriptor,record.encode()));
            proof.requireHeader(target.request.header());
            if (!proof.installation().equals(target.installation)
                    || !proof.catalogueGroup().equals(record.catalogueGroup()) || !proof.cataloguePosition().equals(record.cataloguePosition())) throw new IllegalArgumentException("Schema activation differs from frozen publication qualification");
            return proof;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema ACTIVATE reply",invalid); }
        finally { endCall(); }
    }
    public NativeSchemaActivationProof activateSchema(SchemaActivationTarget target) throws IOException {
        return appendSchemaActivation(target,prepareSchemaActivation(target));
    }
    public RatisSchemaActivateRpc.State schemaActivationState(SchemaActivationTarget target) throws IOException {
        Objects.requireNonNull(target); requireNamespace(target.source); beginCall();
        try { var state=RatisSchemaActivateRpc.stateReply(transport.transaction(target.descriptor,RatisSchemaActivateRpc.stateRequest(target.request),true),ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE,target.request); if (state.activation()!=null && !state.activation().installation().equals(target.installation)) throw new IllegalArgumentException("Schema activation original installation changed"); return state; }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema activation state",invalid); }
        finally { endCall(); }
    }
    public void cancelSchemaActivation(SchemaActivationTarget target) throws IOException {
        Objects.requireNonNull(target); requireNamespace(target.source); beginCall();
        try { RatisSchemaActivateRpc.decodePreparedReply(transport.transaction(target.descriptor,RatisSchemaActivateRpc.cancelRequest(target.request),true),ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL,target.request); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema activation cancellation",invalid); }
        finally { endCall(); }
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
        RangeDirectory sourceDirectory(){return source;}
    }

    /** Frozen original replica and process incarnation; reads never reselect the current leader. */
    public static final class SnapshotTarget {
        private final RangeDirectory source; private final SnapshotFence fence; private final RangeDescriptor descriptor; private final ReplicaDescriptor replica; private final UUID incarnation;private final RatisSnapshotCohortRpc.Capabilities cohort; private final ScopedTransactionRpc.SnapshotBinding binding;
        private SnapshotTarget(RangeDirectory source,SnapshotFence fence,RangeDescriptor descriptor,ReplicaDescriptor replica,UUID incarnation,RatisSnapshotCohortRpc.Capabilities cohort) { this.cohort=cohort;this.source=source; this.fence=fence; this.descriptor=descriptor; this.replica=replica; this.incarnation=incarnation; binding=ScopedTransactionRpc.SnapshotBinding.of(fence); }
        public SnapshotFence fence() { return fence; }
        public RangeDescriptor descriptor() { return descriptor; }
        public ReplicaDescriptor replica() { return replica; }
        public UUID incarnation() { return incarnation; }
        public RatisSnapshotCohortRpc.Capabilities cohort(){return cohort;}
    }
    public CompletableFuture<RatisSnapshotReadiness> snapshotReadinessAsync(SnapshotFence fence,UUID group)  {return snapshotReadinessAsyncAt(current(),fence,group);}
    CompletableFuture<RatisSnapshotReadiness> snapshotReadinessAsyncAt(RangeDirectory observed,SnapshotFence fence,UUID group)  {

         var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent"));
        requireSnapshotRoute(observed,descriptor,fence,group);
        return snapshotAsync(()->transactionReplyAsync(descriptor,ScopedTransactionRpc.SNAPSHOT_READY,fence.encode(),true).thenApply(reply->{
        try {
            var proof=RatisSnapshotReadiness.decode(reply);
            if (!proof.group().equals(group) || !proof.binding().equals(ScopedTransactionRpc.SnapshotBinding.of(fence)) || proof.retired() && !proof.retirement().fence().equals(fence)) throw new IllegalArgumentException("Snapshot readiness binding changed"); return proof;
        } catch (IllegalArgumentException malformed) { throw new CompletionException(new IOException("Invalid snapshot readiness reply",malformed)); }

        }));
    }

    public RatisSnapshotReadiness snapshotReadiness(SnapshotFence fence,UUID group) throws IOException  {return snapshotReadinessAt(current(),fence,group);}
    RatisSnapshotReadiness snapshotReadinessAt(RangeDirectory observed,SnapshotFence fence,UUID group) throws IOException  {
         var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent"));
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
    public SnapshotTarget snapshotTarget(SnapshotFence fence,UUID group)throws IOException{return awaitSnapshot(snapshotTargetAsync(fence,group));}
    SnapshotTarget snapshotTargetAt(RangeDirectory directory,SnapshotFence fence,UUID group)throws IOException{return awaitSnapshot(snapshotTargetAsyncAt(directory,fence,group));}
    public CompletableFuture<SnapshotTarget> snapshotTargetAsync(SnapshotFence fence,UUID group)  {return snapshotTargetAsyncAt(current(),fence,group);}
    CompletableFuture<SnapshotTarget> snapshotTargetAsyncAt(RangeDirectory observed,SnapshotFence fence,UUID group)  {
        try{var descriptor=observed.ranges().stream().filter(r->r.groupId().equals(group)).findFirst().orElseThrow(()->new IllegalArgumentException("Snapshot participant route absent"));requireSnapshotRoute(observed,descriptor,fence,group);
            return snapshotAsync(()->transport.snapshot(descriptor,null,ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_OWNER,new byte[0])).thenApply(bytes->{try{var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REPLY_BYTES);if(packet.kind()!=ScopedTransactionRpc.SNAPSHOT_OWNER)throw new IllegalArgumentException("Snapshot owner opcode changed");var owner=RatisSnapshotOwner.decode(packet.body());if(!owner.groupId().equals(group) || owner.rangeEpoch()!=descriptor.rangeEpoch() || !Arrays.equals(owner.schemaDigest(),observed.schemaDigest()))throw new IllegalArgumentException("Snapshot owner reply changed range/schema");var replica=descriptor.replicas().stream().filter(r->r.peerId().equals(owner.peerId())&&r.role()==ReplicaDescriptor.Role.VOTER).findFirst().orElseThrow(()->new IllegalArgumentException("Snapshot owner is not a frozen voter"));return new SnapshotTarget(identity,fence,descriptor,replica,owner.incarnation(),owner.cohort());}catch(IllegalArgumentException invalid){throw new CompletionException(new IOException("Invalid snapshot owner reply",invalid));}}));
        }catch(Exception failure){return CompletableFuture.failedFuture(failure);}
    }

    public SnapshotTarget restoreSnapshotTarget(SnapshotFence fence,RangeDescriptor descriptor,ReplicaDescriptor replica,UUID incarnation)  {return restoreSnapshotTargetAt(current(),fence,descriptor,replica,incarnation);}
    SnapshotTarget restoreSnapshotTargetAt(RangeDirectory observed,SnapshotFence fence,RangeDescriptor descriptor,ReplicaDescriptor replica,UUID incarnation)  {
        Objects.requireNonNull(incarnation); requireSnapshotRoute(observed,descriptor,fence,descriptor.groupId());
        if (!descriptor.replicas().contains(replica) || replica.role()!=ReplicaDescriptor.Role.VOTER) throw new IllegalArgumentException("Snapshot replica differs from frozen route");
        return new SnapshotTarget(identity,fence,descriptor,replica,incarnation,null);
    }

    /** Exact frozen voter route; no current-leader or arbitrary-endpoint fallback. */
    public CompletableFuture<byte[]> cohortQuery(SnapshotTarget anchor,int kind,byte[] body) {
        try{Objects.requireNonNull(anchor);requireIdentity(anchor.source);if(kind<RatisSnapshotCohortRpc.OWNER || kind>RatisSnapshotCohortRpc.CANCEL)throw new IllegalArgumentException("Invalid cohort operation");return snapshotAsync(()->transport.snapshot(anchor.descriptor,anchor.replica,ScopedTransactionRpc.frame(kind,body)).thenApply(bytes->{var packet=ScopedTransactionRpc.parse(bytes,RatisSnapshotCohortRpc.MAX_REPLY_BYTES);if(packet.kind()!=kind)throw new CompletionException(new IOException("Cohort reply operation changed"));return packet.body();}));}catch(Exception failure){return CompletableFuture.failedFuture(failure);}
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
    public CompletableFuture<Void> releaseSnapshotAsync(SnapshotTarget target,SnapshotFenceStatus.Capture token)throws IOException{
        Objects.requireNonNull(target);Objects.requireNonNull(token);requireIdentity(target.source);
        if(!target.incarnation.equals(token.ownerIncarnation()))throw new RatisSnapshotLostException("Token belongs to another owner incarnation");
        var body=ByteBuffer.allocate(48+ScopedTransactionRpc.SNAPSHOT_TOKEN_BYTES);ScopedTransactionRpc.snapshotBinding(body,target.binding);ScopedTransactionRpc.snapshotToken(body,token);
        var request=ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_RELEASE,body.array());
        return snapshotAsync(()->transport.snapshot(target.descriptor,target.replica,request).thenApply(bytes->{try{
            var reply=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REPLY_BYTES);if(reply.kind()!=ScopedTransactionRpc.SNAPSHOT_RELEASE)throw new IOException("Snapshot reply opcode changed");
            if(ScopedTransactionRpc.snapshotPayload(reply.body(),target.binding,token).length!=0)throw new IOException("Invalid snapshot release reply");return null;
        }catch(IOException|IllegalArgumentException failure){throw new CompletionException(failure);}}));
    }
    public void releaseSnapshot(SnapshotTarget target,SnapshotFenceStatus.Capture token) throws IOException {
        var body=ByteBuffer.allocate(48+ScopedTransactionRpc.SNAPSHOT_TOKEN_BYTES); ScopedTransactionRpc.snapshotBinding(body,target.binding); ScopedTransactionRpc.snapshotToken(body,token);
        if (snapshotQuery(target,token,ScopedTransactionRpc.SNAPSHOT_RELEASE,body.array()).length!=0) throw new IOException("Invalid snapshot release reply");
    }
    public RatisExportSourceRpc.Descriptor exportBuild(SnapshotTarget target,RatisExportSourceRpc.BuildRequest request) throws IOException {
        try {
            var reply=RatisExportSourceRpc.BuildReply.decode(exportQuery(target,request.definition(),ScopedTransactionRpc.EXPORT_BUILD,request.encode()));
            if (!Arrays.equals(request.encode(),reply.request().encode())) throw new IllegalArgumentException("Export BUILD echo changed");
            if (reply.descriptor()==null) throw new RatisSnapshotLostException("Original export pin is lost and no durable chunk exists");
            RatisExportSourceRpc.requireDescriptor(request,reply.descriptor()); return reply.descriptor();
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid export BUILD reply",malformed); }
    }
    public byte[] exportRead(SnapshotTarget target,RatisExportSourceRpc.ReadRequest request) throws IOException {
        try {
            var reply=RatisExportSourceRpc.ReadReply.decode(exportQuery(target,request.definition(),ScopedTransactionRpc.EXPORT_READ,request.encode()));
            if (!Arrays.equals(request.encode(),reply.request().encode())) throw new IllegalArgumentException("Export READ echo changed");
            return reply.bytes();
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid export READ reply",malformed); }
    }
    public void releaseExport(SnapshotTarget target,RatisExportSourceRpc.ReleaseRequest request) throws IOException {
        try {
            var reply=RatisExportSourceRpc.ReleaseReply.decode(exportQuery(target,request.definition(),ScopedTransactionRpc.EXPORT_RELEASE,request.encode()));
            if (!Arrays.equals(request.encode(),reply.request().encode())) throw new IllegalArgumentException("Export RELEASE echo changed");
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid export RELEASE reply",malformed); }
    }
    private byte[] exportQuery(SnapshotTarget target,RatisExportSourceRpc.JobDefinition definition,int kind,byte[] body) throws IOException {
        Objects.requireNonNull(target); Objects.requireNonNull(definition); requireIdentity(target.source);
        if (!target.fence.equals(definition.fence()) || !target.descriptor.groupId().equals(definition.group())
                || !target.incarnation.equals(definition.capture().ownerIncarnation()))
            throw new IllegalArgumentException("Export differs from original frozen owner");
        byte[] request=ScopedTransactionRpc.frame(kind,body);
        return awaitSnapshot(snapshotAsync(()->transport.snapshot(target.descriptor,target.replica,request).thenApply(bytes->{var reply=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REPLY_BYTES);if(reply.kind()!=kind)throw new IllegalArgumentException("Export reply opcode changed");return reply.body();})));
    }
    private byte[] snapshotQuery(SnapshotTarget target,SnapshotFenceStatus.Capture token,int kind,byte[] body) throws IOException {
        Objects.requireNonNull(target); Objects.requireNonNull(token); requireIdentity(target.source);
        if (!target.incarnation.equals(token.ownerIncarnation())) throw new RatisSnapshotLostException("Token belongs to another owner incarnation");
        byte[] request=ScopedTransactionRpc.frame(kind,body);
        try{return awaitSnapshot(snapshotAsync(()->transport.snapshot(target.descriptor,target.replica,request).thenApply(bytes->{var reply=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REPLY_BYTES);if(reply.kind()!=kind)throw new IllegalArgumentException("Snapshot reply opcode changed");try{return ScopedTransactionRpc.snapshotPayload(reply.body(),target.binding,token);}catch(IOException lost){throw new CompletionException(lost);}})));}catch(IllegalArgumentException invalid){throw new IOException("Invalid owner-specific snapshot reply",invalid);}
    }

    public static final class PreparedSnapshotCapture {
        private final SnapshotTarget target; private final RatisSnapshotCaptureRequest request;
        private PreparedSnapshotCapture(SnapshotTarget target,RatisSnapshotCaptureRequest request) { this.target=target; this.request=request; }
        public SnapshotTarget target() { return target; }
        public RatisSnapshotCaptureRequest request() { return request; }
        public byte[] bytes() { return ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_CAPTURE,request.encode()); }
        boolean sameRequest(PreparedSnapshotCapture other){return other!=null && target.descriptor().equals(other.target.descriptor()) && target.replica().equals(other.target.replica()) && request.equals(other.request); }
    }
    public PreparedSnapshotCapture prepareSnapshotCapture(SnapshotTarget target,UUID snapshotId,java.time.Duration ttl) {
        if(snapshotCleanup!=null)throw new IllegalStateException("Frozen snapshot cleanup view cannot create captures");
        return restoreSnapshotCapture(target,new RatisSnapshotCaptureRequest(target.fence,target.incarnation,snapshotId,ttl));
    }
    public PreparedSnapshotCapture prepareWritableSnapshotCapture(SnapshotTarget target,UUID snapshotId,java.time.Duration ttl) {
        if(snapshotCleanup!=null)throw new IllegalStateException("Frozen snapshot cleanup view cannot create captures");
        return restoreSnapshotCapture(target,new RatisSnapshotCaptureRequest(target.fence,target.incarnation,snapshotId,ttl,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED));
    }
    public PreparedSnapshotCapture restoreSnapshotCapture(SnapshotTarget target,RatisSnapshotCaptureRequest request) {
        Objects.requireNonNull(target); Objects.requireNonNull(request); requireIdentity(target.source);
        if (!target.fence.equals(request.fence()) || !target.incarnation.equals(request.owner())) throw new IllegalArgumentException("Capture request differs from frozen target");
        return new PreparedSnapshotCapture(target,request);
    }
    public SnapshotFenceStatus.Capture executeSnapshotCapture(PreparedSnapshotCapture operation) throws IOException {
        if (Objects.requireNonNull(operation).request.mode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY) throw new IllegalArgumentException("Expected read-only snapshot capture");
        return executeCapture(operation).token();
    }
    public RatisSnapshotCaptureRequest.Result executeWritableSnapshotCapture(PreparedSnapshotCapture operation) throws IOException {
        if (Objects.requireNonNull(operation).request.mode()!=RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED) throw new IllegalArgumentException("Expected writable snapshot capture");
        return executeCapture(operation);
    }
    /** Reuse the already captured native owner; this method never opens or renews a snapshot. */
    public ScopedTransaction capturedTransaction(PreparedSnapshotCapture operation,RatisSnapshotCaptureRequest.Result result) throws IOException {
        Objects.requireNonNull(operation); Objects.requireNonNull(result); requireIdentity(operation.target.source);
        // Revalidate the entire response binding, including mode and the native cut.
        var certified=operation.request.certifiedResult(operation.request.reply(result.token(),result.handle()));
        var participant=operation.request.fence().participant(operation.target.descriptor.groupId());
        return new ScopedTransaction(operation.target.descriptor,certified.handle(),participant.scope(),participant.schemaDigest());
    }
    CompletableFuture<RatisSnapshotCaptureRequest.Result> executeCaptureAsync(PreparedSnapshotCapture operation){
        Objects.requireNonNull(operation);requireIdentity(operation.target.source);
        return snapshotAsync(()->transport.snapshot(operation.target.descriptor,operation.target.replica,operation.bytes()).thenApply(bytes->{try{
            var response=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REPLY_BYTES);if(response.kind()!=ScopedTransactionRpc.SNAPSHOT_CAPTURE)throw new IllegalArgumentException("Capture reply opcode changed");return operation.request.resultBinding(response.body());
        }catch(IllegalArgumentException malformed){throw new CompletionException(new IOException("Invalid capture reply",malformed));}
        catch(IOException failure){throw new CompletionException(failure);}}));
    }
    private RatisSnapshotCaptureRequest.Result executeCapture(PreparedSnapshotCapture operation) throws IOException {
        Objects.requireNonNull(operation); requireIdentity(operation.target.source);
        try{return awaitSnapshot(snapshotAsync(()->transport.snapshot(operation.target.descriptor,operation.target.replica,operation.bytes()).thenApply(bytes->{var response=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REPLY_BYTES);if(response.kind()!=ScopedTransactionRpc.SNAPSHOT_CAPTURE)throw new IllegalArgumentException("Capture reply opcode changed");try{return operation.request.resultBinding(response.body());}catch(IOException lost){throw new CompletionException(lost);}})));}catch(IllegalArgumentException invalid){throw new IOException("Invalid capture reply",invalid);}
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
    public PreparedSnapshotOperation restoreSnapshotGate(RatisSnapshotRecord request,RangeDescriptor descriptor)  {return restoreSnapshotGateAt(current(),request,descriptor);}
    PreparedSnapshotOperation restoreSnapshotGateAt(RangeDirectory observed,RatisSnapshotRecord request,RangeDescriptor descriptor)  {
         requireSnapshotRoute(observed,descriptor,request.fence(),request.participantGroup());
        return new PreparedSnapshotOperation(identity,descriptor,request);
    }

    private static void requireSnapshotRoute(RangeDirectory directory,RangeDescriptor descriptor,SnapshotFence fence,UUID group) {
        var participant=fence.participant(group);
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || !descriptor.groupId().equals(group) || !descriptor.rangeId().equals(participant.rangeId()) || descriptor.rangeEpoch()!=participant.rangeEpoch()
                || !Arrays.equals(directory.schemaDigest(),participant.schemaDigest()) || !Arrays.equals(directory.schemaDigest(),fence.participantDomain(participant.groupId()).schemaDigest()) || directory.schemaEpoch()!=fence.participantDomain(participant.groupId()).schemaEpoch()
                || !directory.clusterId().equals(fence.clusterId()) || !directory.databaseId().equals(fence.databaseId()) || !directory.domainId().equals(participant.domainId())) throw new IllegalArgumentException("Snapshot route differs from frozen participant");
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
    public CompletableFuture<RatisSnapshotRecord.Result> executeSnapshotGateAsync(PreparedSnapshotOperation operation) {
        Objects.requireNonNull(operation);requireIdentity(operation.source);
        return snapshotAsync(()->transactionReplyAsync(operation.descriptor,ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE,operation.bytes,true).thenApply(reply->{
        try {
            var input=ByteBuffer.wrap(reply);
            byte[] digest=ScopedTransactionRpc.blob(input,32,false);
            if (input.remaining()<5) throw new IllegalArgumentException("Truncated snapshot preparation reply");
            int state=input.get()&255; byte[] bytes=ScopedTransactionRpc.blob(input,RatisSnapshotRecord.Result.MAX_BYTES,true);
            if (!Arrays.equals(digest,operation.request.digest()) || input.hasRemaining() || state>2 || (state==2)!=(bytes!=null)) throw new IllegalArgumentException("Snapshot authority changed its frozen request");
            var receipt=bytes==null ? null : RatisSnapshotRecord.Result.decode(bytes); if (receipt!=null) requireSnapshotResult(operation,receipt);
            return new SnapshotGatePreparation(state!=0,receipt);
        } catch (IllegalArgumentException malformed) { throw new CompletionException(new IOException("Invalid snapshot gate authority reply",malformed)); }
        }).thenCompose(preparation->{
            if(!preparation.drained())return CompletableFuture.failedFuture(new RatisSnapshotWaitingException());
            if(preparation.receipt()!=null)return CompletableFuture.completedFuture(preparation.receipt());
            try{return transport.participantAsync(operation.descriptor,operation.bytes).thenApply(bytes->{
                try{var result=RatisSnapshotRecord.Result.decode(bytes);requireSnapshotResult(operation,result);return result;}
                catch(IllegalArgumentException invalid){throw new CompletionException(new IOException("Invalid snapshot gate applied reply",invalid));}
            });}catch(IOException failure){return CompletableFuture.failedFuture(failure);}
        }));
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
    public CompletableFuture<Boolean> helpSnapshotFenceAsync(SnapshotFence fence,UUID group)  {return helpSnapshotFenceAsyncAt(current(),fence,group);}
    CompletableFuture<Boolean> helpSnapshotFenceAsyncAt(RangeDirectory observed,SnapshotFence fence,UUID group)  {

         var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot helper route absent"));
        requireSnapshotRoute(observed,descriptor,fence,group);
        var binding=ScopedTransactionRpc.SnapshotBinding.of(fence); var body=ByteBuffer.allocate(48); ScopedTransactionRpc.snapshotBinding(body,binding);
        return snapshotAsync(()->transactionReplyAsync(descriptor,ScopedTransactionRpc.SNAPSHOT_HELP,body.array(),true).thenApply(reply->{
        try {
            var input=ByteBuffer.wrap(reply);
            var actual=ScopedTransactionRpc.snapshotBinding(input); var actualGroup=ScopedTransactionRpc.uuid(input);
            if (!binding.equals(actual) || !group.equals(actualGroup) || input.remaining()!=1) throw new IllegalArgumentException("Snapshot helper reply binding changed");
            int helped=input.get()&255; if (helped>1) throw new IllegalArgumentException("Invalid snapshot helper progress"); return helped==1;
        } catch (IllegalArgumentException malformed) { throw new CompletionException(new IOException("Invalid snapshot helper reply",malformed)); }

        }));
    }

    public boolean helpSnapshotFence(SnapshotFence fence,UUID group) throws IOException  {return helpSnapshotFenceAt(current(),fence,group);}
    boolean helpSnapshotFenceAt(RangeDirectory observed,SnapshotFence fence,UUID group) throws IOException  {
         var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot helper route absent"));
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
    public CompletableFuture<SnapshotGate> snapshotGateAsync(SnapshotFence fence,UUID group)  {return snapshotGateAsyncAt(current(),fence,group);}
    CompletableFuture<SnapshotGate> snapshotGateAsyncAt(RangeDirectory observed,SnapshotFence fence,UUID group)  {

         var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent"));
        requireSnapshotRoute(observed,descriptor,fence,group); var body=ByteBuffer.allocate(16); ScopedTransactionRpc.uuid(body,fence.fenceId());
        return snapshotAsync(()->transactionReplyAsync(descriptor,ScopedTransactionRpc.SNAPSHOT_GATE_STATE,body.array(),true).thenApply(reply->{
        try {
            var input=ByteBuffer.wrap(reply);
            byte[] bytes=ScopedTransactionRpc.blob(input,SnapshotGate.MAX_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing snapshot gate state");
            if (bytes==null) return null; var record=SnapshotGate.decode(bytes);
            if (!record.participantGroup().equals(group) || !record.fence().equals(fence)) throw new IllegalArgumentException("Snapshot gate reply binding changed");
            return record;
        } catch (IllegalArgumentException malformed) { throw new CompletionException(new IOException("Invalid snapshot gate state reply",malformed)); }

        }));
    }

    public SnapshotGate snapshotGate(SnapshotFence fence,UUID group) throws IOException  {return snapshotGateAt(current(),fence,group);}
    SnapshotGate snapshotGateAt(RangeDirectory observed,SnapshotFence fence,UUID group) throws IOException  {
         var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent"));
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
    public PreparedFinalizationOperation restoreFinalization(ParticipantFinalization finalization,RangeDescriptor descriptor)  {return restoreFinalizationAt(current(),finalization,descriptor);}
    PreparedFinalizationOperation restoreFinalizationAt(RangeDirectory observed,ParticipantFinalization finalization,RangeDescriptor descriptor)  {
         var registered=finalization.participant(); var decision=finalization.decision();
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || !descriptor.groupId().equals(registered.groupId()) || !descriptor.rangeId().equals(registered.rangeId()) || descriptor.rangeEpoch()!=registered.rangeEpoch()
                || !Arrays.equals(observed.schemaDigest(),registered.schemaDigest()) || !observed.domainId().equals(registered.domainId()) || !observed.clusterId().equals(decision.clusterId()) || !observed.databaseId().equals(decision.databaseId())) throw new IllegalArgumentException("Finalization route differs from frozen participant");
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
    public PreparedParticipantOperation restoreParticipant(PreparedParticipant.Preparation preparation,RangeDescriptor descriptor,boolean negative)  {return restoreParticipantAt(current(),preparation,descriptor,negative);}
    PreparedParticipantOperation restoreParticipantAt(RangeDirectory observed,PreparedParticipant.Preparation preparation,RangeDescriptor descriptor,boolean negative)  {

        var registered=preparation.registration().participants().stream().filter(p -> p.groupId().equals(descriptor.groupId())).findFirst().orElseThrow();
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || descriptor.rangeEpoch()!=preparation.business().rangeEpoch() || !registered.rangeId().equals(descriptor.rangeId())
                || !Arrays.equals(observed.schemaDigest(),preparation.business().schemaDigest()) || !observed.domainId().equals(preparation.domainBinding().domainId())
                || !observed.clusterId().equals(preparation.registration().clusterId()) || !observed.databaseId().equals(preparation.registration().databaseId())) throw new IllegalArgumentException("Participant route differs from frozen registration");
        restoreSessionCommand(observed,preparation.business(),descriptor);
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
    public PreparedParticipant preparedParticipant(UUID group,UUID transactionId) throws IOException  {return preparedParticipantAt(current(),group,transactionId);}
    PreparedParticipant preparedParticipantAt(RangeDirectory observed,UUID group,UUID transactionId) throws IOException  {
         var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Participant route absent"));
        var body=ByteBuffer.allocate(16); ScopedTransactionRpc.uuid(body,transactionId);
        try {
            var input=ByteBuffer.wrap(transaction(descriptor,ScopedTransactionRpc.PARTICIPANT_STATE,body.array(),true));
            byte[] bytes=ScopedTransactionRpc.blob(input,16+PreparedParticipant.Preparation.MAX_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing prepared state");
            if (bytes==null) return null; var record=PreparedParticipant.decode(bytes);
            if (!record.preparation().transactionId().equals(transactionId) || !record.preparation().business().groupId().equals(group)) throw new IllegalArgumentException("Prepared reply identity changed");
            restoreParticipantAt(observed,record.preparation(),descriptor,false); return record;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid prepared state reply",malformed); }
    }

    /** Fresh terminal result lookup does not depend on either business or recovery grant eligibility. */
    public ParticipantOutcome finalizedParticipant(UUID group,UUID transactionId) throws IOException {
        var observed=current(); var descriptor=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Participant route absent"));
        return finalizedParticipant(observed,descriptor,transactionId);
    }
    ParticipantOutcome finalizedParticipant(RangeDirectory observed,RangeDescriptor descriptor,UUID transactionId)throws IOException {
        requireNamespace(observed);if(observed.ranges().stream().noneMatch(descriptor::equals))throw new IllegalArgumentException("Frozen participant descriptor absent");UUID group=descriptor.groupId();
        var body=ByteBuffer.allocate(16); ScopedTransactionRpc.uuid(body,transactionId);
        try {
            var input=ByteBuffer.wrap(transaction(descriptor,ScopedTransactionRpc.PARTICIPANT_OUTCOME,body.array(),true));
            byte[] bytes=ScopedTransactionRpc.blob(input,ParticipantOutcome.MAX_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing participant outcome");
            if (bytes==null) return null; var outcome=ParticipantOutcome.decode(bytes); var decision=outcome.finalization().decision(); var participant=outcome.finalization().participant();
            if (!decision.transactionId().equals(transactionId) || !participant.groupId().equals(group) || !participant.rangeId().equals(descriptor.rangeId()) || participant.rangeEpoch()!=descriptor.rangeEpoch()
                    || !Arrays.equals(participant.schemaDigest(),observed.schemaDigest()) || !decision.clusterId().equals(observed.clusterId()) || !decision.databaseId().equals(observed.databaseId()) || !participant.domainId().equals(observed.domainId())) throw new IllegalArgumentException("Participant outcome route/identity changed");
            return outcome;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid participant outcome reply",invalid); }
    }
    public CompletableFuture<SnapshotFenceProof> snapshotFenceAsync(SnapshotFence fence,UUID group)  {return snapshotFenceAsyncAt(current(),fence,group);}
    CompletableFuture<SnapshotFenceProof> snapshotFenceAsyncAt(RangeDirectory observed,SnapshotFence fence,UUID group)  {

         var route=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent")); requireSnapshotRoute(observed,route,fence,group);
        return snapshotAsync(()->transactionReplyAsync(route,ScopedTransactionRpc.SNAPSHOT_FENCE,fence.encode(),true).thenApply(reply->{
        try {
            var input=ByteBuffer.wrap(reply);
            byte[] binding=new byte[48]; if (input.remaining()<52) throw new IllegalArgumentException("Truncated snapshot fence reply"); input.get(binding);
            if (!ScopedTransactionRpc.snapshotBinding(ByteBuffer.wrap(binding)).equals(ScopedTransactionRpc.SnapshotBinding.of(fence))) throw new IllegalArgumentException("Snapshot fence binding changed");
            byte[] bytes=ScopedTransactionRpc.blob(input,SnapshotFenceProof.MAX_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing snapshot fence reply");
            if (bytes==null) return null; var proof=SnapshotFenceProof.decode(bytes); if (!proof.fence().equals(fence) || !proof.participantGroup().equals(group)) throw new IllegalArgumentException("Snapshot fence identity changed"); return new SnapshotFenceProof(fence,group,proof.minimumEpoch(),proof.position());
        } catch (IllegalArgumentException malformed) { throw new CompletionException(new IOException("Invalid snapshot fence reply",malformed)); }

        }));
    }

    public SnapshotFenceProof snapshotFence(SnapshotFence fence,UUID group) throws IOException  {return snapshotFenceAt(current(),fence,group);}
    SnapshotFenceProof snapshotFenceAt(RangeDirectory observed,SnapshotFence fence,UUID group) throws IOException  {
         var route=observed.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot participant route absent")); requireSnapshotRoute(observed,route,fence,group); beginCall();
        try {
            var input=ByteBuffer.wrap(transactionReply(route,ScopedTransactionRpc.SNAPSHOT_FENCE,fence.encode(),true));
            byte[] binding=new byte[48]; if (input.remaining()<52) throw new IllegalArgumentException("Truncated snapshot fence reply"); input.get(binding);
            if (!ScopedTransactionRpc.snapshotBinding(ByteBuffer.wrap(binding)).equals(ScopedTransactionRpc.SnapshotBinding.of(fence))) throw new IllegalArgumentException("Snapshot fence binding changed");
            byte[] bytes=ScopedTransactionRpc.blob(input,SnapshotFenceProof.MAX_BYTES,true); if (input.hasRemaining()) throw new IllegalArgumentException("Trailing snapshot fence reply");
            if (bytes==null) return null; var proof=SnapshotFenceProof.decode(bytes); if (!proof.fence().equals(fence) || !proof.participantGroup().equals(group)) throw new IllegalArgumentException("Snapshot fence identity changed"); return new SnapshotFenceProof(fence,group,proof.minimumEpoch(),proof.position());
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
    private RangeDescriptor snapshotHomeRoute(SnapshotHomeIdentity identity) {
        var observed=current(); var route=observed.ranges().stream().filter(r -> r.groupId().equals(identity.groupId())).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot HOME route absent"));
        if (route.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || route.rangeEpoch()!=identity.rangeEpoch() || !observed.columns().equals(java.util.Set.of(identity.columnId()))
                || !observed.clusterId().equals(identity.fence().clusterId()) || !observed.databaseId().equals(identity.fence().databaseId()) || !Arrays.equals(observed.schemaDigest(),identity.schemaDigest())) throw new IllegalArgumentException("Snapshot HOME route differs from frozen identity"); return route;
    }
    public CompletableFuture<SnapshotHomeFenceProof> snapshotHomeFenceAsync(SnapshotHomeIdentity identity) {

        Objects.requireNonNull(identity); var route=snapshotHomeRoute(identity);
        return snapshotAsync(()->transactionReplyAsync(route,ScopedTransactionRpc.SNAPSHOT_HOME_FENCE,identity.encode(),true).thenApply(reply->{
        try {
            var input=ByteBuffer.wrap(reply);
            if (input.remaining()<36) throw new IllegalArgumentException("Truncated snapshot HOME proof"); byte[] digest=new byte[32]; input.get(digest); byte[] bytes=ScopedTransactionRpc.blob(input,SnapshotHomeFenceProof.MAX_BYTES,true);
            if (!Arrays.equals(digest,identity.digest()) || input.hasRemaining()) throw new IllegalArgumentException("Snapshot HOME proof binding changed"); if (bytes==null) return null;
            var proof=SnapshotHomeFenceProof.decode(bytes); if (!proof.identity().equals(identity)) throw new IllegalArgumentException("Snapshot HOME proof identity changed"); return new SnapshotHomeFenceProof(identity,proof.minimumEpoch(),proof.position());
        } catch (IllegalArgumentException malformed) { throw new CompletionException(new IOException("Invalid snapshot HOME proof reply",malformed)); }

        }));
    }
    public SnapshotHomeFenceProof snapshotHomeFence(SnapshotHomeIdentity identity) throws IOException {
        Objects.requireNonNull(identity); var route=snapshotHomeRoute(identity); beginCall();
        try {
            var input=ByteBuffer.wrap(transactionReply(route,ScopedTransactionRpc.SNAPSHOT_HOME_FENCE,identity.encode(),true));
            if (input.remaining()<36) throw new IllegalArgumentException("Truncated snapshot HOME proof"); byte[] digest=new byte[32]; input.get(digest); byte[] bytes=ScopedTransactionRpc.blob(input,SnapshotHomeFenceProof.MAX_BYTES,true);
            if (!Arrays.equals(digest,identity.digest()) || input.hasRemaining()) throw new IllegalArgumentException("Snapshot HOME proof binding changed"); if (bytes==null) return null;
            var proof=SnapshotHomeFenceProof.decode(bytes); if (!proof.identity().equals(identity)) throw new IllegalArgumentException("Snapshot HOME proof identity changed"); return new SnapshotHomeFenceProof(identity,proof.minimumEpoch(),proof.position());
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot HOME proof reply",malformed); } finally { endCall(); }
    }
    public void retireSnapshotHome(SnapshotHomeRetirement retirement) throws IOException {
        Objects.requireNonNull(retirement); var route=snapshotHomeRoute(retirement.identity()); beginCall();
        try {
            byte[] record=RatisSnapshotHomeRetirementRecord.encode(retirement); byte[] reply=transactionReply(route,ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_PREPARE,record,true);
            if (reply.length!=33 || !Arrays.equals(Arrays.copyOf(reply,32),retirement.digest()) || (reply[32]&255)>1) throw new IOException("Invalid snapshot HOME retirement authority reply"); if (reply[32]==1) return;
            var result=CommandResult.decode(transport.participant(route,record)); if (result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=0) throw new IOException("Invalid snapshot HOME retirement applied reply");
        } finally { endCall(); }
    }
    public void cancelSnapshotHomeRetirement(SnapshotHomeRetirement retirement) throws IOException {
        Objects.requireNonNull(retirement); var route=snapshotHomeRoute(retirement.identity()); beginCall();
        try { byte[] reply=transactionReply(route,ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_CANCEL,RatisSnapshotHomeRetirementRecord.encode(retirement),true);
            if (reply.length!=33 || !Arrays.equals(Arrays.copyOf(reply,32),retirement.digest()) || reply[32]!=0) throw new IOException("Invalid snapshot HOME retirement cancellation reply");
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
        return participantFence(observed,descriptor,identity);
    }
    ParticipantFenceProof participantFence(RangeDirectory observed,RangeDescriptor descriptor,ParticipantIdentity identity)throws IOException {
        requireNamespace(observed);if(observed.ranges().stream().noneMatch(descriptor::equals))throw new IllegalArgumentException("Frozen participant fence descriptor absent");var participant=identity.participant();
        if (!observed.clusterId().equals(identity.definition().clusterId()) || !observed.databaseId().equals(identity.definition().databaseId()) || !observed.domainId().equals(participant.domainId())
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
    private volatile List<RangeDirectory> acceptedSchemas;
    private final Transport transport;
    private final SnapshotCleanupTransport snapshotCleanup;
    private int snapshotCleanupBorrowers;
    private RangeDirectory directory;
    private int activeCalls;
    private boolean closing;
    private CompletableFuture<Void> closeAttempt;

    public RatisRoutedClient(RangeDirectory directory, Transport transport) {
        this.identity = this.directory = Objects.requireNonNull(directory, "directory");
        acceptedSchemas=List.of(directory);
        this.transport = Objects.requireNonNull(transport, "transport");snapshotCleanup=null;
    }
    private RatisRoutedClient(RangeDirectory identity,RangeDirectory frozen,SnapshotCleanupTransport transport){this.identity=identity;directory=frozen;acceptedSchemas=List.of(frozen);this.transport=transport;snapshotCleanup=transport;}
    /** Trusted forced snapshot metadata only. No independent client pool or new capture authority is created. */
    synchronized RatisRoutedClient borrowedSnapshotCleanupView(RangeDirectory frozen,SnapshotFence fence)throws IOException {
        requireOpen();requireNamespace(frozen);RatisSnapshotPlan.requireDomain(frozen,fence);if(snapshotCleanup!=null || snapshotCleanupBorrowers>=8192)throw new IOException("Snapshot cleanup view capacity exhausted");var borrowed=new SnapshotCleanupTransport(frozen,fence);snapshotCleanupBorrowers++;return new RatisRoutedClient(identity,frozen,borrowed);
    }
    /** Called only after the original durable journal has been decoded, bound and forced. */
    void restoreSnapshotCleanupCaptures(RatisDurableSnapshot.Snapshot saved)throws IOException {
        if(snapshotCleanup==null || !snapshotCleanup.fence.equals(saved.initial().fence()))throw new IOException("Snapshot cleanup journal differs from forced definition");for(var participant:saved.participants())if(participant.capture()!=null)snapshotCleanup.restore(participant.capture());
    }
    private final class SnapshotCleanupTransport implements Transport {
        final RangeDirectory frozen;final SnapshotFence fence;private final java.util.Map<UUID,PreparedSnapshotCapture> captures=new java.util.HashMap<>();private boolean released;
        SnapshotCleanupTransport(RangeDirectory frozen,SnapshotFence fence){this.frozen=frozen;this.fence=fence;}
        synchronized void restore(PreparedSnapshotCapture original)throws IOException{route(original.target.descriptor);if(!fence.equals(original.request.fence()) || original.request.mode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY)throw denied();var previous=captures.putIfAbsent(original.target.descriptor.groupId(),original);if(previous!=null&&!previous.sameRequest(original))throw denied();}
        private IOException denied(){return new IOException("Frozen snapshot cleanup view cannot create captures or perform USER work");}
        private void route(RangeDescriptor route)throws IOException{if(frozen.ranges().stream().noneMatch(route::equals) || fence.participants().stream().noneMatch(p->p.groupId().equals(route.groupId())))throw denied();}
        private void record(RangeDescriptor route,byte[] bytes,boolean release)throws IOException{route(route);var record=RatisSnapshotRecord.decode(bytes);if(!fence.equals(record.fence()) || !route.groupId().equals(record.participantGroup()) || release&&!record.isRelease())throw denied();}
        public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{throw denied();}
        public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{throw denied();}
        public CompletableFuture<byte[]> readAsync(RangeDescriptor route,UUID column,byte[] key)throws IOException{throw denied();}
        public CompletableFuture<CommandResult> commandAsync(RangeDescriptor route,DistributedCommand command)throws IOException{throw denied();}
        public byte[] participant(RangeDescriptor route,byte[] bytes)throws IOException{record(route,bytes,true);return transport.participant(route,bytes);}
        public CompletableFuture<byte[]> participantAsync(RangeDescriptor route,byte[] bytes)throws IOException{record(route,bytes,true);return transport.participantAsync(route,bytes);}
        public RangeStateView stateView(RangeDescriptor route)throws IOException{route(route);return transport.stateView(route);}
        private void transactionRequest(RangeDescriptor route,byte[] bytes)throws IOException{route(route);var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);switch(packet.kind()){
            case ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE -> record(route,packet.body(),true);
            case ScopedTransactionRpc.SNAPSHOT_GATE_CANCEL -> record(route,packet.body(),false);
            case ScopedTransactionRpc.SNAPSHOT_GATE_STATE -> {if(!Arrays.equals(packet.body(),ByteBuffer.allocate(16).putLong(fence.fenceId().getMostSignificantBits()).putLong(fence.fenceId().getLeastSignificantBits()).array()))throw denied();}
            case ScopedTransactionRpc.SNAPSHOT_FENCE,ScopedTransactionRpc.SNAPSHOT_READY -> {if(!Arrays.equals(packet.body(),fence.encode()))throw denied();}
            default -> throw denied();
        }}

        public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh)throws IOException{transactionRequest(route,bytes);return transport.transaction(route,bytes,fresh);}
        public CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] bytes,boolean fresh)throws IOException{transactionRequest(route,bytes);return transport.transactionAsync(route,bytes,fresh);}
        public CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] bytes)throws IOException{route(route);var packet=ScopedTransactionRpc.parse(bytes,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES);switch(packet.kind()){
            case ScopedTransactionRpc.SNAPSHOT_CAPTURE -> {PreparedSnapshotCapture original;synchronized(this){original=captures.get(route.groupId());}if(original==null || !original.target.replica.equals(owner) || !Arrays.equals(original.bytes(),bytes))throw denied();}
            case ScopedTransactionRpc.SNAPSHOT_RELEASE -> {var input=ByteBuffer.wrap(packet.body());if(!ScopedTransactionRpc.snapshotBinding(input).equals(ScopedTransactionRpc.SnapshotBinding.of(fence)))throw denied();var token=ScopedTransactionRpc.snapshotToken(input);PreparedSnapshotCapture original;synchronized(this){original=captures.get(route.groupId());}if(original==null || !original.target.replica.equals(owner) || !original.request.snapshotId().equals(token.snapshotId()) || !original.request.owner().equals(token.ownerIncarnation()))throw denied();}
            case ScopedTransactionRpc.EXPORT_RELEASE -> {var request=RatisExportSourceRpc.ReleaseRequest.decode(packet.body());PreparedSnapshotCapture original;synchronized(this){original=captures.get(route.groupId());}if(!fence.equals(request.definition().fence()) || !route.groupId().equals(request.definition().group()) || original==null || !original.target.replica.equals(owner) || !original.request.snapshotId().equals(request.definition().capture().snapshotId()) || !original.request.owner().equals(request.definition().capture().ownerIncarnation()))throw denied();}
            case RatisSnapshotCohortRpc.STATE,RatisSnapshotCohortRpc.CANCEL -> {}
            default -> throw denied();
        }return transport.snapshot(route,owner,bytes);}
        public void close(){synchronized(RatisRoutedClient.this){if(!released){released=true;snapshotCleanupBorrowers--;RatisRoutedClient.this.notifyAll();}}}
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
    /** Reattach only the original server handle recorded with an accepted intent; never opens a new cut. */
    public ScopedTransaction restoreTransaction(RatisDurableTransaction.Intent intent) throws IOException  {return restoreTransactionAt(current(),intent);}
    ScopedTransaction restoreTransactionAt(RangeDirectory observed,RatisDurableTransaction.Intent intent) throws IOException  {
        Objects.requireNonNull(intent); if (intent.captureId()==null) throw new RatisPreparationUnavailableException("Intent has no original remote capture handle");
        var directory=observed; var accepted=intent.acceptedBusiness(); var audit=Objects.requireNonNull(accepted.acceptance(),"Intent has no accepted cut"); var route=intent.route();
        restoreSessionCommand(observed,accepted,route);
        var participants=audit.scope().participants(directory,1);
        if (participants.size()!=1) throw new IllegalArgumentException("Original capture scope changed participant");
        var current=participants.getFirst();
        if (!current.rangeId().equals(route.rangeId()) || !current.groupId().equals(route.groupId()) || current.rangeEpoch()!=route.rangeEpoch() || !Arrays.equals(current.lowerInclusive(),route.lowerInclusive()) || !Arrays.equals(current.upperExclusive(),route.upperExclusive())) throw new IllegalArgumentException("Original capture route changed ownership/bounds");
        return new ScopedTransaction(route,new ScopedTransactionRpc.Handle(audit.ownerIncarnation(),intent.captureId(),audit.leaderGeneration(),audit.snapshot()),audit.scope(),accepted.schemaDigest());
    }

    public final class ScopedTransaction implements AutoCloseable {
        private final RangeDescriptor descriptor; private final ScopedTransactionRpc.Handle handle;
        private final TransactionScope scope;
        private final byte[] schemaDigest;
        private boolean closed;
        private ScopedTransaction(RangeDescriptor descriptor,ScopedTransactionRpc.Handle handle,TransactionScope scope,byte[] schemaDigest) { this.descriptor=descriptor; this.handle=handle; this.scope=scope; this.schemaDigest=schemaDigest.clone(); }
        public ApplyPosition snapshot() { return handle.position(); }
        public UUID captureId() { return handle.id(); }
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
        /** Equal/extended prefixes are immutable; an empty prefix explicitly clears only unfrozen ephemeral staging. */
        public CommandResult stage(List<Mutation> mutations,List<KeyPredicate> predicates) throws IOException {
            boolean reset=mutations.isEmpty() && predicates.isEmpty();
            var prefix=reset?null:new DistributedCommand(descriptor.groupId(),descriptor.rangeEpoch(),schemaDigest,new OperationId(handle.id(),0,0),mutations,predicates);
            byte[] encoded=reset?new byte[0]:DistributedCommandCodec.encode(prefix),digest=reset?OperatorUpload.sha256(encoded):prefix.payloadDigest();var body=ByteBuffer.allocate(60+encoded.length);ScopedTransactionRpc.writeHandle(body,handle);ScopedTransactionRpc.putBlob(body,encoded);
            byte[] reply=call(ScopedTransactionRpc.STAGE,body.array());
            if(reply.length<32 || !Arrays.equals(digest,Arrays.copyOf(reply,32)))throw new IOException("Staging reply differs from requested prefix");
            try{var result=CommandResult.decode(Arrays.copyOfRange(reply,32,reply.length));long returns=mutations.stream().filter(m->m.returnMode()!=Mutation.ReturnMode.NONE).count();if(result.status()==CommandResult.Status.SUCCESS && result.valueCount()!=returns || result.status()!=CommandResult.Status.SUCCESS && result.valueCount()!=0)throw new IOException("Staging reply differs from requested result plan");return result;}
            catch(IllegalArgumentException malformed){throw new IOException("Invalid staging reply",malformed);}
        }
        public CommandResult stage(List<Mutation> mutations) throws IOException { return stage(mutations,List.of()); }
        public byte[] participantPlanDigest(DistributedCommand intent,CommandResult stagedResult) throws IOException { return participantIntent(intent,stagedResult).payloadDigest(); }
        /** Freeze the original complete read scope as a participant with no user effects. */
        public DistributedCommand readCertification(OperationId operation) throws IOException {
            synchronized (this) { if (closed) throw new IOException("Transaction handle is closed"); }
            var audit=new TransactionAcceptance(handle.position(),handle.generation(),handle.owner(),scope,new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode());
            return DistributedCommand.readCertification(descriptor.groupId(),descriptor.rangeEpoch(),schemaDigest,operation,audit);
        }
        /** Freeze an accepted-plan image for registration/journaling; server PREPARE authority is still required. */
        public DistributedCommand participantIntent(DistributedCommand intent,CommandResult stagedResult) throws IOException {
            synchronized (this) { if (closed) throw new IOException("Transaction handle is closed"); }
            if (!intent.groupId().equals(descriptor.groupId()) || intent.rangeEpoch()!=descriptor.rangeEpoch() || !Arrays.equals(intent.schemaDigest(),schemaDigest)) throw new IllegalArgumentException("Participant intent differs from frozen route");
            var audit=new TransactionAcceptance(handle.position(),handle.generation(),handle.owner(),scope,stagedResult.encode());
            return DistributedCommand.acceptedTransaction(intent,audit);
        }
        /** Authorize this handle's accepted writes or complete read scope without rebasing its original pin. */
        public PreparedParticipantOperation authorizeParticipant(DistributedCommand acceptedBusiness,OperationId control,UUID transactionId) throws IOException {
            Objects.requireNonNull(control); Objects.requireNonNull(transactionId);
            synchronized (this) { if (closed) throw new IOException("Transaction handle is closed"); }
            var audit=Objects.requireNonNull(acceptedBusiness.acceptance(),"Expected accepted participant intent");
            if (!acceptedBusiness.groupId().equals(descriptor.groupId()) || acceptedBusiness.rangeEpoch()!=descriptor.rangeEpoch() || !Arrays.equals(acceptedBusiness.schemaDigest(),schemaDigest)
                    || !audit.snapshot().equals(handle.position()) || audit.leaderGeneration()!=handle.generation() || !audit.ownerIncarnation().equals(handle.owner()) || !Arrays.equals(ScopedTransactionRpc.scopeBytes(scope),ScopedTransactionRpc.scopeBytes(audit.scope()))) throw new IllegalArgumentException("Participant authorization differs from original handle");
            byte[] encoded=DistributedCommandCodec.encode(acceptedBusiness); var body=ByteBuffer.allocate(108+encoded.length); ScopedTransactionRpc.writeHandle(body,handle);
            ScopedTransactionRpc.uuid(body,control.sessionId()); body.putLong(control.epoch()).putLong(control.sequence()); ScopedTransactionRpc.uuid(body,transactionId); ScopedTransactionRpc.putBlob(body,encoded);
            byte[] response=transaction(descriptor,ScopedTransactionRpc.PARTICIPANT_AUTHORIZE,body.array(),true);
            if (response.length==1 && response[0]==1) throw new RatisPreparationUnavailableException("Original participant handle is no longer available");
            if (response.length<2 || response[0]!=0) throw new IOException("Invalid participant authorization disposition");
            byte[] reply=Arrays.copyOfRange(response,1,response.length);
            try {
                var preparation=RatisParticipantRecord.decode(reply); boolean negative=RatisParticipantRecord.isNegative(reply);
                if (!preparation.transactionId().equals(transactionId) || !preparation.operation().equals(control) || !Arrays.equals(DistributedCommandCodec.encode(preparation.business()),encoded)) throw new IOException("Participant authority reply changed original request");
                var operation=restoreParticipant(preparation,descriptor,negative); if (!Arrays.equals(operation.bytes(),reply)) throw new IOException("Noncanonical participant authority reply"); return operation;
            } catch (IllegalArgumentException invalid) { throw new IOException("Invalid participant authority reply",invalid); }
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

    private CompletableFuture<byte[]> transactionReplyAsync(RangeDescriptor descriptor,int kind,byte[] body,boolean freshBarrier) throws IOException {
        byte[] request=ScopedTransactionRpc.frame(kind,body);
        if(request.length>ScopedTransactionRpc.MAX_REQUEST_BYTES)throw new IllegalArgumentException("Transaction request exceeds byte budget");
        return transport.transactionAsync(descriptor,request,freshBarrier).thenApply(bytes->{
            try{var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REPLY_BYTES);
                if(packet.kind()!=kind)throw new IOException("Transaction reply opcode differs from request");return packet.body();
            }catch(IOException|IllegalArgumentException failure){throw new CompletionException(new IOException("Invalid scoped transaction reply",failure));}
        });
    }

    public synchronized RangeDirectory directory() { return directory; }
    /** Select only a current or ACTIVATE-qualified historical schema, never a caller-selected route. */
    public synchronized RangeDirectory directory(DistributedSchema schema) {
        requireOpen(); Objects.requireNonNull(schema);
        RangeDirectory known=Arrays.equals(directory.schemaDigest(),schema.digest()) ? directory : schemaView(schema.digest());
        schema.requireMatches(known); return known;
    }
    public void requireKnownSchema(DistributedSchema schema) { directory(schema); }
    RangeDirectory bootstrapDirectory() { return identity; }

    public synchronized void refresh(RangeDirectory next) {
        requireOpen();
        Objects.requireNonNull(next, "next");
        requireIdentity(next);
        if (!sameSchema(directory,next)) throw new IllegalArgumentException("Schema refresh requires a native activation proof");
        if (next.generation() < directory.generation()
                || (next.generation() == directory.generation() && !Arrays.equals(next.payloadDigest(), directory.payloadDigest()))) {
            throw new IllegalArgumentException("Stale or conflicting directory generation");
        }
        directory = next;
    }

    /** Published directory qualified by an original native ACTIVATE result; owned retries retain their schemas. */
    public synchronized void refreshSchema(SchemaChange.Definition definition,NativeSchemaActivationProof activation) {
        requireOpen(); Objects.requireNonNull(definition); Objects.requireNonNull(activation);
        requireNamespace(definition.sourceDirectory()); activation.requireHeader(definition.header());
        var target=definition.targetDirectory();
        if (acceptedSchemas.stream().anyMatch(known -> sameSchema(known,target)) && directory.generation()>=target.generation()) {
            if (acceptedSchemas.stream().noneMatch(known -> sameSchema(known,definition.sourceDirectory()))) {
                if (acceptedSchemas.size()>=65) throw new IllegalStateException("Routed schema history budget exhausted");
                var schemas=new java.util.ArrayList<>(acceptedSchemas); schemas.add(definition.sourceDirectory()); acceptedSchemas=List.copyOf(schemas);
            }
            return;
        }
        if (!sameSchema(directory,definition.sourceDirectory()) || directory.generation()>definition.sourceDirectory().generation()
                || target.generation()<=directory.generation()) throw new IllegalArgumentException("Schema activation does not follow current routing");
        if (acceptedSchemas.size()>=65) throw new IllegalStateException("Routed schema history budget exhausted");
        var schemas=new java.util.ArrayList<>(acceptedSchemas); schemas.add(target); acceptedSchemas=List.copyOf(schemas);
        directory=target;
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

    CompletableFuture<RoutedRead> readWithRouteAsync(UUID columnId,byte[] physicalKey) {
        Objects.requireNonNull(physicalKey,"physicalKey");
        if(physicalKey.length>DistributedCommandCodec.MAX_ENCODED_BYTES-16)throw new IllegalArgumentException("Point read key exceeds budget");
        var descriptor=current().route(columnId,physicalKey);
        return snapshotAsync(()->transport.readAsync(descriptor,columnId,physicalKey.clone()).thenApply(value->{
            if(value!=null&&value.length>DistributedCommandCodec.MAX_ENCODED_BYTES)throw new CompletionException(new IOException("Point value exceeds budget"));
            return new RoutedRead(descriptor,value==null?null:value.clone());
        }));
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
        return RatisTransactionPlan.create(List.of(current()),operation,mutations,predicates);
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
        return prepareInternal(current(),operation,mutations,predicates,acknowledgedThrough);
    }
    interface OwnerDirectoryLease extends AutoCloseable {@Override void close();}
    private static final class OwnerDirectory {final RangeDirectory directory;int owners;OwnerDirectory(RangeDirectory directory){this.directory=directory;}}
    private final Map<String,OwnerDirectory> ownerDirectories=new HashMap<>();
    /** Trusted forced owner-root restoration only; never changes current routing or accepts a caller-selected schema. */
    synchronized OwnerDirectoryLease retainOwnerDirectory(RatisSessionBindingPlan plan){
        requireOpen();var frozen=plan.directory(identity.domainId());requireIdentity(frozen);var digest=java.util.HexFormat.of().formatHex(frozen.payloadDigest());var retained=ownerDirectories.get(digest);
        if(retained==null){if(ownerDirectories.size()>=64)throw new IllegalStateException("Retained owner directory capacity exhausted");retained=new OwnerDirectory(frozen);ownerDirectories.put(digest,retained);}retained.owners=Math.addExact(retained.owners,1);var owner=retained;
        return new OwnerDirectoryLease(){boolean released;public void close(){synchronized(RatisRoutedClient.this){if(released)return;released=true;if(--owner.owners==0)ownerDirectories.remove(digest,owner);}}};
    }
    /** Resume a forced complete request using its exact locally qualified original directory. */
    public synchronized PreparedCommand prepareAt(RangeDirectory original,OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates,long acknowledgedThrough) {
        requireOpen(); requireIdentity(Objects.requireNonNull(original));
        if (!Arrays.equals(original.payloadDigest(),directory.payloadDigest()) && acceptedSchemas.stream().noneMatch(known -> Arrays.equals(known.payloadDigest(),original.payloadDigest())) && !ownerDirectories.containsKey(java.util.HexFormat.of().formatHex(original.payloadDigest()))) throw new IllegalArgumentException("Original directory was not qualified for this client");
        return prepareInternal(original,operation,mutations,predicates,acknowledgedThrough);
    }
    private PreparedCommand prepareInternal(RangeDirectory observed,OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates,long acknowledgedThrough) {
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
    @FunctionalInterface public interface SourceAckDirectoryReader { RangeDirectory read(TopologyChange.Header header)throws IOException; }
    public record QualifiedAcknowledgementSource(RangeDirectory directory,RangeDescriptor parent,RatisTopologyOperationOutcomeRpc.Reply evidence){public QualifiedAcknowledgementSource{Objects.requireNonNull(directory);Objects.requireNonNull(parent);Objects.requireNonNull(evidence);}}
    /** Qualify the metadata header before a durable window allocates a fresh ACK identity. */
    public QualifiedAcknowledgementSource qualifyAcknowledgementSource(PreparedCommand original,SourceAckDirectoryReader reader)throws IOException {
        Objects.requireNonNull(original);Objects.requireNonNull(reader);var evidence=operationOutcome(original);var header=evidence.freeze().header();var directory=reader.read(header);
        if(directory==null || !Arrays.equals(directory.payloadDigest(),header.sourceDirectoryDigest()) || directory.schemaEpoch()!=header.schemaEpoch() || !Arrays.equals(directory.schemaDigest(),header.schemaDigest()) || directory.generation()!=header.sourceGeneration())throw new IOException("ACK source directory differs from actual native sealed header");
        try{requireNamespace(directory);var lineage=evidence.request().lineage();var parent=directory.ranges().stream().filter(r->r.rangeId().equals(lineage.rangeId()) && r.groupId().equals(lineage.groupId()) && r.rangeEpoch()==lineage.rangeEpoch()).findFirst().orElseThrow(()->new IllegalArgumentException("ACK source lineage absent from immutable directory"));
            if(!Arrays.equals(parent.lowerInclusive(),original.descriptor.lowerInclusive()) || !Arrays.equals(parent.upperExclusive(),original.descriptor.upperExclusive()))throw new IllegalArgumentException("ACK source ownership changed original lineage bounds");
            synchronized(this){requireOpen();if(acceptedSchemas.stream().noneMatch(known->sameSchema(known,directory))){if(acceptedSchemas.size()>=65)throw new IllegalArgumentException("ACK source schema history capacity exhausted");var schemas=new java.util.ArrayList<>(acceptedSchemas);schemas.add(directory);acceptedSchemas=List.copyOf(schemas);}}
            return new QualifiedAcknowledgementSource(directory,parent,evidence);
        }catch(IllegalArgumentException invalid){throw new IOException("ACK source namespace/projection changed",invalid);}
    }
    /** Original-parent receipt GC uses the caller's fresh durable DATA identity and frozen source placement. */
    public PreparedCommand prepareAcknowledgementAt(OperationId fresh,RangeDirectory frozenSource,RangeDescriptor originalParent,long acknowledgedThrough){
        Objects.requireNonNull(fresh);Objects.requireNonNull(frozenSource);Objects.requireNonNull(originalParent);
        return restoreSessionCommand(frozenSource,new DistributedCommand(originalParent.groupId(),originalParent.rangeEpoch(),frozenSource.schemaDigest(),fresh,List.of(),List.of(),acknowledgedThrough),originalParent);
    }
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

    /** Trusted clock archive path only: no USER payload or session binding can use this frozen metadata route. */
    PreparedCommand prepareMetadataSessionControl(DistributedCommand command,RangeDirectory frozen,RangeDescriptor descriptor){
        Objects.requireNonNull(command);Objects.requireNonNull(frozen);Objects.requireNonNull(descriptor);requireNamespace(frozen);var control=command.sessionControl();
        if(control==null || control.binding() || command.kind()!=DistributedCommand.Kind.SESSION_CONTROL || !control.authority().clusterId().equals(frozen.clusterId()) || !control.authority().databaseId().equals(frozen.databaseId()) || !Arrays.equals(command.schemaDigest(),frozen.schemaDigest()) || !command.groupId().equals(descriptor.groupId()) || command.rangeEpoch()!=descriptor.rangeEpoch() || frozen.ranges().stream().noneMatch(descriptor::equals))throw new IllegalArgumentException("Invalid frozen metadata clock route");
        return new PreparedCommand(frozen,descriptor,command);
    }
    PreparedCommand restoreMetadataSessionControl(DistributedCommand command,RangeDirectory frozen,RangeDescriptor descriptor){return prepareMetadataSessionControl(command,frozen,descriptor);}
    CommandResult executeMetadataSessionControl(PreparedCommand prepared)throws IOException{
        prepareMetadataSessionControl(prepared.command(),prepared.source,prepared.descriptor());beginCall();try{var result=transport.command(prepared.descriptor(),prepared.command());if(result==null)throw new IOException("Missing metadata clock result");return result;}finally{endCall();}
    }
    // Journal recovery preserves the exact original peer route even after a directory refresh.
    PreparedCommand restoreSessionControl(DistributedCommand command,RangeDescriptor descriptor) {
        var observed=schemaView(command.schemaDigest()); var control=command.sessionControl();
        if (control==null || !control.authority().clusterId().equals(observed.clusterId())
                || !control.authority().databaseId().equals(observed.databaseId())
                || !Arrays.equals(command.schemaDigest(),observed.schemaDigest())
                || !command.groupId().equals(descriptor.groupId()) || command.rangeEpoch()!=descriptor.rangeEpoch()
                || descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Invalid recovered control route");
        return new PreparedCommand(observed,descriptor,command);
    }
    /** Only journal recovery uses this exact binding source; fresh binding still requires the current placement. */
    PreparedCommand restoreBindingControl(DistributedCommand command,RangeDirectory frozen,RangeDescriptor descriptor){
        Objects.requireNonNull(frozen);requireIdentity(frozen);var control=command.sessionControl();
        if(control==null || !control.binding() || command.kind()!=DistributedCommand.Kind.SESSION_CONTROL || !control.authority().clusterId().equals(frozen.clusterId()) || !control.authority().databaseId().equals(frozen.databaseId()) || !Arrays.equals(command.schemaDigest(),frozen.schemaDigest()) || !command.groupId().equals(descriptor.groupId()) || command.rangeEpoch()!=descriptor.rangeEpoch() || descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || frozen.ranges().stream().noneMatch(descriptor::equals))throw new IllegalArgumentException("Retained binding differs from its frozen source");
        return new PreparedCommand(frozen,descriptor,command);
    }
    /** Restore an immutable user retry route without rebasing it onto the current topology. */
    PreparedCommand restoreSessionCommand(DistributedCommand command,RangeDescriptor descriptor) {
        return restoreSessionCommandValidated(schemaView(command.schemaDigest()),command,descriptor);
    }
    PreparedCommand restoreSessionCommand(RangeDirectory observed,DistributedCommand command,RangeDescriptor descriptor) {
        Objects.requireNonNull(command);Objects.requireNonNull(descriptor);Objects.requireNonNull(observed);requireIdentity(observed);
        if(observed.ranges().stream().noneMatch(descriptor::equals))throw new IllegalArgumentException("Recovered route is outside original directory");
        return restoreSessionCommandValidated(observed,command,descriptor);
    }
    private PreparedCommand restoreSessionCommandValidated(RangeDirectory observed,DistributedCommand command,RangeDescriptor descriptor) {
        Objects.requireNonNull(command);Objects.requireNonNull(descriptor);
        if (command.kind()!=DistributedCommand.Kind.DATA && command.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED || command.sessionControl()!=null || command.minimumSessionEpoch()!=-1 || !Arrays.equals(command.schemaDigest(),observed.schemaDigest())
                || !command.groupId().equals(descriptor.groupId()) || command.rangeEpoch()!=descriptor.rangeEpoch()
                || descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE
                || command.mutations().isEmpty() && command.predicates().isEmpty() && command.acknowledgedThrough()<0 && !command.readCertification()) {
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
        return new PreparedCommand(observed,descriptor,command);
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
    /** Stable READ_ONLY snapshot namespace; the forced fence separately binds schema, routes and native tokens. */
    byte[] snapshotNamespaceDigest() {
        var bytes=ByteBuffer.allocate(48);for(var id:List.of(identity.clusterId(),identity.databaseId(),identity.domainId()))bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());try{return java.security.MessageDigest.getInstance("SHA-256").digest(bytes.array());}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}
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

    byte[] maintenanceIdentityDigest() {
        var output=ByteBuffer.allocate(52);
        for (var id:List.of(identity.clusterId(),identity.databaseId(),identity.domainId())) output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
        output.putInt(identity.routingPrefixBytes());
        try { return java.security.MessageDigest.getInstance("SHA-256").digest(output.array()); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    boolean registeredSchema(long epoch,byte[] digest) {
        return acceptedSchemas.stream().anyMatch(known -> known.schemaEpoch()==epoch && Arrays.equals(known.schemaDigest(),digest));
    }
    private RangeDirectory schemaView(byte[] digest) {
        current(); var schemas=acceptedSchemas;
        for (int i=schemas.size()-1;i>=0;i--) if (Arrays.equals(schemas.get(i).schemaDigest(),digest)) return schemas.get(i);
        throw new IllegalArgumentException("Recovered schema has not been qualified for this client");
    }

    /** A sealed source proved no original effect; the complete request still needs atomic rerouting. */
    public static final class OriginalOperationAbsent extends IOException {
        private final PreparedCommand original;
        private final RatisTopologyOperationOutcomeRpc.Reply evidence;
        private OriginalOperationAbsent(PreparedCommand original,RatisTopologyOperationOutcomeRpc.Reply evidence) {
            super("Original operation is absent at its sealed parent; whole-operation coordination is required");
            this.original=original;this.evidence=evidence;
        }
        public PreparedCommand original() { return original; }
        public RangeDirectory originalDirectory() { return original.source; }
        public RatisTopologyOperationOutcomeRpc.Reply evidence() { return evidence; }
    }

    /** Query the original receipt prefix, preserving the original group and complete payload digest. */
    public RatisTopologyOperationOutcomeRpc.Reply operationOutcome(PreparedCommand prepared) throws IOException {
        Objects.requireNonNull(prepared);requireNamespace(prepared.source);beginCall();
        try { return readOriginalOutcome(prepared); }
        finally { endCall(); }
    }
    /** Re-establish the exact forced CLOSE (and optionally its no-credit fact), never ABSENT. */
    public void requireSourceClosing(RangeDirectory source,NativeSourceClosing closing,OperationRerouteOrigin noCredit)throws IOException {
        requireNamespace(source);var header=closing.request().header();var parent=header.participant(TopologyChange.Stage.FREEZE,closing.request().group());var route=source.ranges().stream().filter(r->r.groupId().equals(parent.groupId()) && r.rangeId().equals(parent.rangeId()) && r.rangeEpoch()==parent.rangeEpoch()).findFirst().orElseThrow(()->new IOException("Source closing original route unavailable"));
        if(!source.domainId().equals(header.domainId()) || !Arrays.equals(source.payloadDigest(),header.sourceDirectoryDigest()))throw new IOException("Source closing immutable directory changed");
        beginCall();try{RatisHomeSourceClosingRpc.requireSourceAck(transport.transaction(route,RatisHomeSourceClosingRpc.source(closing,noCredit),true),closing,noCredit);}catch(IllegalArgumentException invalid){throw new IOException("Invalid source CLOSED proof",invalid);}finally{endCall();}
    }
    /** Null means no acknowledged HOME fence, never fresh source absence. A non-null result includes fresh no-credit qualification. */
    public RatisHomeSourceClosingRpc.ClosedReservation closedReservation(HomeIdentity identity)throws IOException {
        var descriptor=directory.ranges().stream().filter(r->r.groupId().equals(identity.groupId())).findFirst().orElseThrow(()->new IllegalArgumentException("HOME closing group absent"));if(!identity.definition().reserved())throw new IllegalArgumentException("CLOSED lookup requires original RESERVED");beginCall();try{byte[] bytes=transport.transaction(descriptor,RatisHomeSourceClosingRpc.lookup(identity),true);if(bytes.length==0)return null;var value=RatisHomeSourceClosingRpc.ClosedReservation.decode(bytes);value.requireIdentity(identity);return value;}catch(IllegalArgumentException invalid){throw new IOException("Invalid CLOSED reservation proof",invalid);}finally{endCall();}
    }
    /** Caller pays the bounded request/reply/decoder workspace before any transport copies. */
    public RatisHomeSourceReferencesRpc.Reply homeSourceReferences(RatisHomeSourceReferencesRpc.Request request,RatisSourceReferenceClosure.Admission admission)throws IOException{
        var home=directory.ranges().stream().filter(range->range.groupId().equals(request.closing().request().home())).findFirst().orElseThrow(()->new IllegalArgumentException("HOME reference group absent"));
        if(home.rangeEpoch()!=request.closing().request().homeEpoch() || !Arrays.equals(directory.schemaDigest(),request.closing().request().schema()))throw new IllegalArgumentException("HOME reference route/schema changed");
        try(var workspace=admission.reserve(RatisHomeSourceReferencesRpc.WORKSPACE_BYTES)){
            beginCall();try{byte[] response=transport.transaction(home,RatisHomeSourceReferencesRpc.request(request),true);return RatisHomeSourceReferencesRpc.reply(response,request);}catch(IllegalArgumentException invalid){throw new IOException("Invalid HOME reference proof",invalid);}finally{endCall();}
        }
    }
    public NativeHomeSourceClosing closeHomeSource(RangeDescriptor home,NativeHomeSourceClosing.Request request)throws IOException {
        if(!home.groupId().equals(request.home()) || home.rangeEpoch()!=request.homeEpoch())throw new IllegalArgumentException("HOME closing route changed");
        beginCall();try{
            var prepared=transport.transaction(home,RatisHomeSourceClosingRpc.request(RatisHomeSourceClosingRpc.PREPARE,request),true);if(!Arrays.equals(prepared,request.digest()))throw new IOException("HOME closing preparation changed identity");
            var value=NativeHomeSourceClosing.decode(transport.participant(home,new RatisHomeSourceClosingRecord(request).encode()));if(!value.request().equals(request))throw new IOException("HOME closing result changed identity");return value;
        }finally{endCall();}
    }
    public RatisSourceOutcomeRpc.Prepared prepareSourceOutcome(RangeDirectory source,RatisSourceOutcomeRpc.Proposal proposal)throws IOException {
        var route=sourceOutcomeRoute(source,proposal.request());beginCall();try{return RatisSourceOutcomeRpc.prepared(transport.transaction(route,RatisSourceOutcomeRpc.prepare(proposal),true),proposal);}catch(IllegalArgumentException invalid){throw new IOException("Invalid source preparation reply",invalid);}finally{endCall();}
    }
    public RatisSourceOutcomeRpc.Applied appendSourceOutcome(RangeDirectory source,RatisSourceOutcomeRecord record,List<RatisDurableTransaction.Intent> intents)throws IOException {
        var route=sourceOutcomeRoute(source,record.request());var proposal=new RatisSourceOutcomeRpc.Proposal(record.kind(),record.request(),intents,record);var prepared=prepareSourceOutcome(source,proposal);if(prepared.fenced())return new RatisSourceOutcomeRpc.Applied(record.digest(),NativeSourceOutcomePublication.Outcome.FENCED,null);if(!record.equals(prepared.record()))throw new IOException("Source append changed frozen record");beginCall();try{return RatisSourceOutcomeRpc.applied(transport.participant(route,record.encode()),record);}catch(IllegalArgumentException invalid){throw new IOException("Invalid source applied proof",invalid);}finally{endCall();}
    }
    public void cancelSourceOutcome(RangeDirectory source,RatisSourceOutcomeRecord.Kind kind,NativeSourceOutcomeCredit.Request request)throws IOException {
        var route=sourceOutcomeRoute(source,request);beginCall();try{RatisSourceOutcomeRpc.requireCancelled(transport.transaction(route,RatisSourceOutcomeRpc.cancel(kind,request),true),kind,request);}catch(IllegalArgumentException invalid){throw new IOException("Invalid source cancellation reply",invalid);}finally{endCall();}
    }
    private RangeDescriptor sourceOutcomeRoute(RangeDirectory source,NativeSourceOutcomeCredit.Request request){requireNamespace(source);var parent=request.freeze().header().participant(TopologyChange.Stage.FREEZE,request.original().groupId());var route=source.ranges().stream().filter(r->r.groupId().equals(parent.groupId()) && r.rangeId().equals(parent.rangeId()) && r.rangeEpoch()==parent.rangeEpoch()).findFirst().orElseThrow(()->new IllegalArgumentException("Original source route absent"));if(!source.domainId().equals(request.origin().domainId()) || !Arrays.equals(source.schemaDigest(),request.original().schemaDigest()))throw new IllegalArgumentException("Source request changed original ownership/schema");return route;}
    public void authorizeOperationReroute(PreparedCommand prepared) throws IOException {
        Objects.requireNonNull(prepared);requireIdentity(prepared.source);beginCall();
        try { RatisOperationRerouteAdmissionRpc.requireEcho(transport.transaction(prepared.descriptor,
                RatisOperationRerouteAdmissionRpc.prepareRequest(prepared.command),true),RatisOperationRerouteAdmissionRpc.PREPARE,prepared.command); }
        catch(IllegalArgumentException malformed) { throw new IOException("Invalid reroute admission reply",malformed); }
        finally { endCall(); }
    }
    public void authorizeOperationReroute(PreparedCommand prepared,RatisOperationRerouteAdmissionRpc.Evidence evidence)throws IOException {
        Objects.requireNonNull(prepared);Objects.requireNonNull(evidence);requireIdentity(prepared.source);beginCall();
        try {RatisOperationRerouteAdmissionRpc.requireEcho(transport.transaction(prepared.descriptor,RatisOperationRerouteAdmissionRpc.prepareRequest(prepared.command,evidence),true),RatisOperationRerouteAdmissionRpc.PREPARE,prepared.command);}
        catch(IllegalArgumentException malformed){throw new IOException("Invalid reroute evidence reply",malformed);}
        finally {endCall();}
    }
    public void cancelOperationReroute(PreparedCommand prepared) throws IOException {
        Objects.requireNonNull(prepared);requireIdentity(prepared.source);beginCall();
        try { RatisOperationRerouteAdmissionRpc.requireEcho(transport.transaction(prepared.descriptor,
                RatisOperationRerouteAdmissionRpc.cancelRequest(prepared.command),true),RatisOperationRerouteAdmissionRpc.CANCEL,prepared.command); }
        catch(IllegalArgumentException malformed) { throw new IOException("Invalid reroute cancellation reply",malformed); }
        finally { endCall(); }
    }
    private RatisTopologyOperationOutcomeRpc.Reply readOriginalOutcome(PreparedCommand prepared) throws IOException {
        if(Thread.currentThread().isInterrupted())throw new java.io.InterruptedIOException("Original outcome lookup interrupted");
        var source=prepared.source;var descriptor=prepared.descriptor;
        var lineage=new it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage(descriptor.rangeId(),descriptor.groupId(),descriptor.rangeEpoch());
        var request=new RatisTopologyOperationOutcomeRpc.Request(source.clusterId(),source.databaseId(),source.domainId(),lineage,
                prepared.command.operationId(),prepared.command.receiptDigest());
        try { return RatisTopologyOperationOutcomeRpc.decodeReply(transport.transaction(descriptor,RatisTopologyOperationOutcomeRpc.request(request),true),request); }
        catch(IllegalArgumentException malformed) { throw new IOException("Invalid original operation outcome",malformed); }
    }
    private CommandResult resolvedOriginalOutcome(PreparedCommand prepared,RatisTopologyOperationOutcomeRpc.Reply reply) throws IOException {
        return switch(reply.outcome()) {
            case FOUND -> CommandResult.decode(reply.result());
            case EXPIRED -> new CommandResult(CommandResult.Status.SESSION_EXPIRED,List.of());
            case ABSENT -> {
                if(!RatisOperationRerouteAdmissionRpc.acknowledgementOnly(prepared.command))throw new OriginalOperationAbsent(prepared,reply);
                // Receipt cleanup belongs to this sealed lineage, using its exact original identity.
                var result=transport.command(prepared.descriptor,prepared.command);
                if(result==null || result.valueCount()!=0)throw new IOException("Invalid original-parent acknowledgement result");
                yield result;
            }
        };
    }
    public CommandResult execute(PreparedCommand prepared) throws IOException {
        Objects.requireNonNull(prepared, "prepared");
        var observed=current();var descriptor=prepared.descriptor;
        boolean moved=observed.ranges().stream().noneMatch(range->range.rangeId().equals(descriptor.rangeId())
                && range.groupId().equals(descriptor.groupId()) && range.rangeEpoch()==descriptor.rangeEpoch());
        if(moved)requireNamespace(prepared.source);else requireIdentity(prepared.source);
        beginCall();
        try {
            if(moved)return resolvedOriginalOutcome(prepared,readOriginalOutcome(prepared));
            try {
                CommandResult result=transport.command(prepared.descriptor(),prepared.command());
                if(result==null)throw new IOException("Missing command result");
                return result;
            } catch(IOException unresolved) {
                if(unresolved instanceof java.io.InterruptedIOException || Thread.currentThread().isInterrupted())throw unresolved;
                try { return resolvedOriginalOutcome(prepared,readOriginalOutcome(prepared)); }
                catch(OriginalOperationAbsent absent) { absent.addSuppressed(unresolved);throw absent; }
                catch(IOException unavailable) { unresolved.addSuppressed(unavailable);throw unresolved; }
            }
        } finally { endCall(); }
    }

    public CompletableFuture<CommandResult> executeAsync(PreparedCommand prepared) {
        Objects.requireNonNull(prepared,"prepared");var observed=current();var descriptor=prepared.descriptor;
        boolean moved=observed.ranges().stream().noneMatch(range->range.rangeId().equals(descriptor.rangeId())
                &&range.groupId().equals(descriptor.groupId())&&range.rangeEpoch()==descriptor.rangeEpoch());
        if(moved)requireNamespace(prepared.source);else requireIdentity(prepared.source);
        return snapshotAsync(()->{
            if(moved)return readOriginalOutcomeAsync(prepared).thenCompose(reply->resolvedOriginalOutcomeAsync(prepared,reply));
            CompletableFuture<CommandResult> submitted;
            try{submitted=transport.commandAsync(descriptor,prepared.command);}catch(IOException failure){submitted=CompletableFuture.failedFuture(failure);}
            return submitted.handle((result,failure)->{
                if(failure==null)return result==null?CompletableFuture.<CommandResult>failedFuture(new IOException("Missing command result")):CompletableFuture.completedFuture(result);
                Throwable cause=unwrap(failure);
                if(!(cause instanceof IOException unresolved)||cause instanceof java.io.InterruptedIOException)return CompletableFuture.<CommandResult>failedFuture(cause);
                return readOriginalOutcomeAsync(prepared).thenCompose(reply->resolvedOriginalOutcomeAsync(prepared,reply)).handle((resolved,lookupFailure)->{
                    if(lookupFailure==null)return resolved;
                    Throwable lookup=unwrap(lookupFailure);
                    if(lookup instanceof OriginalOperationAbsent){lookup.addSuppressed(unresolved);throw new CompletionException(lookup);}
                    unresolved.addSuppressed(lookup);throw new CompletionException(unresolved);
                });
            }).thenCompose(future->future);
        });
    }
    private CompletableFuture<RatisTopologyOperationOutcomeRpc.Reply> readOriginalOutcomeAsync(PreparedCommand prepared) {
        try {
        var source=prepared.source;var descriptor=prepared.descriptor;
        var lineage=new DistributedCdcCheckpoint.Lineage(descriptor.rangeId(),descriptor.groupId(),descriptor.rangeEpoch());
        var request=new RatisTopologyOperationOutcomeRpc.Request(source.clusterId(),source.databaseId(),source.domainId(),lineage,prepared.command.operationId(),prepared.command.receiptDigest());
        return transport.transactionAsync(descriptor,RatisTopologyOperationOutcomeRpc.request(request),true).thenApply(bytes->{
            try{return RatisTopologyOperationOutcomeRpc.decodeReply(bytes,request);}catch(IllegalArgumentException malformed){throw new CompletionException(new IOException("Invalid original operation outcome",malformed));}
        });
        } catch(IOException failure) {return CompletableFuture.failedFuture(failure);}
    }
    private CompletableFuture<CommandResult> resolvedOriginalOutcomeAsync(PreparedCommand prepared,RatisTopologyOperationOutcomeRpc.Reply reply) {
        try{return switch(reply.outcome()){
            case FOUND -> CompletableFuture.completedFuture(CommandResult.decode(reply.result()));
            case EXPIRED -> CompletableFuture.completedFuture(new CommandResult(CommandResult.Status.SESSION_EXPIRED,List.of()));
            case ABSENT -> {
                if(!RatisOperationRerouteAdmissionRpc.acknowledgementOnly(prepared.command))yield CompletableFuture.failedFuture(new OriginalOperationAbsent(prepared,reply));
                yield transport.commandAsync(prepared.descriptor,prepared.command).thenApply(result->{
                    if(result==null||result.valueCount()!=0)throw new CompletionException(new IOException("Invalid original-parent acknowledgement result"));return result;
                });
            }
        };}catch(IOException|RuntimeException failure){return CompletableFuture.failedFuture(failure);}
    }
    private static Throwable unwrap(Throwable failure){while(failure instanceof CompletionException&&failure.getCause()!=null)failure=failure.getCause();return failure;}

    private synchronized RangeDirectory current() { requireOpen(); return directory; }
    private void requireOpen() { if (closing) throw new IllegalStateException("Routed client is closing"); }
    private void requireIdentity(RangeDirectory other) {
        requireNamespace(other);
        var schemas=acceptedSchemas;
        for (int i=schemas.size()-1;i>=0;i--) if (sameSchema(schemas.get(i),other)) return;
        throw new IllegalArgumentException("Directory schema has not been qualified for this client");
    }
    private void requireNamespace(RangeDirectory other) {
        if (!identity.clusterId().equals(other.clusterId()) || !identity.databaseId().equals(other.databaseId())
                || !identity.domainId().equals(other.domainId()) || identity.routingPrefixBytes()!=other.routingPrefixBytes()) {
            throw new IllegalArgumentException("Directory belongs to another cluster/database/domain/projection");
        }
    }
    private static boolean sameSchema(RangeDirectory first,RangeDirectory second) {
        return first.schemaEpoch()==second.schemaEpoch() && Arrays.equals(first.schemaDigest(),second.schemaDigest())
                && first.columns().equals(second.columns()) && first.routingPrefixBytes()==second.routingPrefixBytes();
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
            if(snapshotCleanupBorrowers!=0)throw new IOException("Frozen snapshot cleanup views remain owned");
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
    static CompletableFuture<byte[]> snapshotOnOwner(RaftClient client,RangeDescriptor descriptor,ReplicaDescriptor owner,byte[] request) throws IOException {
        if(owner!=null && !descriptor.replicas().contains(owner))throw new IllegalArgumentException("Snapshot owner is absent from frozen route");
        int kind=ScopedTransactionRpc.kind(request,Math.max(ScopedTransactionRpc.MAX_REQUEST_BYTES,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES));boolean cohort=kind>=RatisSnapshotCohortRpc.OWNER&&kind<=RatisSnapshotCohortRpc.CANCEL;
        if(owner==null&&kind!=ScopedTransactionRpc.SNAPSHOT_OWNER || owner!=null&&!cohort&&!ScopedTransactionRpc.ownerSnapshot(kind)&&!ScopedTransactionRpc.ownerExport(kind))throw new IllegalArgumentException("Owner transport accepts only bound snapshot/export operations");
        CompletableFuture<RaftClientReply> actual;try{var message=Message.valueOf(ByteString.copyFrom(request));actual=owner==null ? client.async().sendReadOnly(message) : kind==RatisSnapshotCohortRpc.OWNER ? client.async().sendReadOnly(message) : client.async().sendStaleRead(message,0,RaftPeerId.valueOf(owner.peerId()));}catch(StatusRuntimeException failure){throw new IOException("Raft gRPC transport failed",failure);}
        return actual.thenApply(reply->{try{if(reply!=null&&(!reply.getRaftGroupId().equals(RaftGroupId.valueOf(descriptor.groupId())) || owner!=null&&!reply.getServerId().equals(RaftPeerId.valueOf(owner.peerId()))))throw new IOException("Snapshot reply came from another peer/group");return RaftTransport.content(reply,cohort?RatisSnapshotCohortRpc.MAX_REPLY_BYTES:ScopedTransactionRpc.MAX_REPLY_BYTES);}catch(IOException failure){throw new CompletionException(failure);}});
    }
    /** Only READ and Ratis' exact zero-index window handshake discover voters; mutations never retry. */
    static org.apache.ratis.retry.RetryPolicy snapshotRetryPolicy(RangeDescriptor descriptor){
        var voters=descriptor.replicas().stream().filter(replica->replica.role()==ReplicaDescriptor.Role.VOTER)
                .map(ReplicaDescriptor::peerId).collect(java.util.stream.Collectors.toSet());
        return event->{
            if(!(event instanceof org.apache.ratis.client.retry.ClientRetryEvent client)||event.getAttemptCount()>=5
                    ||!(event.getCause() instanceof org.apache.ratis.protocol.exceptions.NotLeaderException notLeader)
                    ||notLeader.getSuggestedLeader()==null||!voters.contains(notLeader.getSuggestedLeader().getId().toString()))
                return org.apache.ratis.retry.RetryPolicy.NO_RETRY_ACTION;
            var type=client.getRequest().getType();
            boolean read=type.is(org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.READ);
            // OrderedAsync initializes a new window with its no-effect DUMMY WATCH(0).
            // Retrying an arbitrary WATCH, or an already-appended WRITE on step-down, is forbidden.
            boolean handshake=type.is(org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.WATCH)
                    &&type.getWatch().getIndex()==0
                    &&type.getWatch().getReplication()==org.apache.ratis.proto.RaftProtos.ReplicationLevel.MAJORITY
                    &&client.getRequest().getMessage()!=null
                    &&client.getRequest().getMessage().getContent().equals(ByteString.copyFromUtf8("DUMMY"));
            if(!read&&!handshake)return org.apache.ratis.retry.RetryPolicy.NO_RETRY_ACTION;
            return org.apache.ratis.retry.RetryPolicy.RETRY_WITHOUT_SLEEP_ACTION;
        };
    }
    /** Discover the leader asynchronously before dispatch, without retrying a possibly appended WRITE. */
    static CompletableFuture<RaftClientReply> metadataWriteAfterLeaderBarrier(RaftClient client,RangeDescriptor descriptor,Message message){
        var barrier=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0])));
        return client.async().sendReadOnly(barrier).thenCompose(reply->{
            try{if(reply==null||!reply.getRaftGroupId().equals(RaftGroupId.valueOf(descriptor.groupId())))throw new IOException("Metadata barrier came from another group");
                RaftTransport.content(reply,ScopedTransactionRpc.MAX_REPLY_BYTES);
                return client.async().send(message);
            }catch(IOException failure){return CompletableFuture.failedFuture(failure);}
        });
    }
    @FunctionalInterface private interface SnapshotAsync<T>{CompletableFuture<T> run()throws IOException;}
    private <T> CompletableFuture<T> snapshotAsync(SnapshotAsync<T> work) {
        try{beginCall();}catch(Exception failure){return CompletableFuture.failedFuture(failure);}final CompletableFuture<T> actual;
        try{actual=Objects.requireNonNull(work.run());}catch(Throwable failure){endCall();return CompletableFuture.failedFuture(failure);}var result=new CompletableFuture<T>();actual.whenComplete((value,failure)->{endCall();if(failure==null)result.complete(value);else{while(failure instanceof CompletionException && failure.getCause()!=null)failure=failure.getCause();if(failure instanceof StatusRuntimeException)failure=new IOException("Raft gRPC transport failed",failure);result.completeExceptionally(failure);}});return result.copy();
    }
    static <T>T awaitSnapshot(CompletableFuture<T> future)throws IOException {
        try{return future.get();}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new java.io.InterruptedIOException("Snapshot caller interrupted");}catch(java.util.concurrent.ExecutionException failure){Throwable cause=failure.getCause();while(cause instanceof CompletionException&&cause.getCause()!=null)cause=cause.getCause();if(cause instanceof IOException io)throw io;if(cause instanceof RuntimeException runtime)throw runtime;throw new IOException("Snapshot request failed",cause);}
    }

    private static final class RaftTransport implements Transport {
        private final RatisClientPool clients = new RatisClientPool(MAX_CONCURRENT_CALLS,RaftTransport::connected,RaftTransport::connectedOwner,null,RaftTransport::connectedSnapshot,RaftTransport::connectedSnapshotOwner);

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
        private static int transactionReplyLimit(byte[] request){return request.length>=ScopedTransactionRpc.HEADER_BYTES && ByteBuffer.wrap(request).getInt()==ScopedTransactionRpc.MAGIC && ByteBuffer.wrap(request).getInt(4)==RatisHomeSourceReferencesRpc.KIND?RatisHomeSourceReferencesRpc.MAX_REPLY_BYTES:ScopedTransactionRpc.MAX_REPLY_BYTES;}
        @Override public byte[] transaction(RangeDescriptor descriptor,byte[] request,boolean freshBarrier) throws IOException {
            return rpc(() -> { try (var lease=clients.acquire(descriptor)) {
                var message=Message.valueOf(ByteString.copyFrom(request));
                return content(freshBarrier ? lease.client().io().sendReadOnly(message) : lease.client().io().sendReadOnlyNonLinearizable(message),transactionReplyLimit(request));
            } });
        }

        @Override public CompletableFuture<byte[]> snapshot(RangeDescriptor descriptor,ReplicaDescriptor owner,byte[] request) throws IOException {
            var flow=clients.acquireSnapshotAsync(descriptor,owner).thenCompose(lease->{final CompletableFuture<byte[]> actual;try{actual=snapshotOnOwner(lease.client(),descriptor,owner,request);}catch(Throwable failure){lease.invalidate();lease.close();return CompletableFuture.failedFuture(failure);}var result=new CompletableFuture<byte[]>();actual.whenComplete((value,failure)->{if(failure!=null)lease.invalidate();lease.close();if(failure==null)result.complete(value);else result.completeExceptionally(failure);});return result;});return flow.copy();
        }
        @Override public byte[] participant(RangeDescriptor descriptor,byte[] record) throws IOException {
            return rpc(() -> { try (var lease=clients.acquire(descriptor)) { return content(lease.client().io().send(Message.valueOf(ByteString.copyFrom(record))),RatisSourceOutcomeRecord.isRecord(record)?37+NativeSourceOutcomeCredit.MAX_BYTES:CommandResult.MAX_BYTES); } });
        }

        @FunctionalInterface private interface AsyncCall { CompletableFuture<RaftClientReply> send(RaftClient client); }
        private CompletableFuture<byte[]> callAsync(RangeDescriptor descriptor,int maximum,AsyncCall call) {
            var flow=clients.acquireSnapshotAsync(descriptor,null).thenCompose(lease->{
                final CompletableFuture<RaftClientReply> actual;
                try{actual=Objects.requireNonNull(call.send(lease.client()));}
                catch(Throwable failure){lease.invalidate();lease.close();return CompletableFuture.failedFuture(failure);}
                var result=new CompletableFuture<byte[]>();
                actual.whenComplete((reply,failure)->{
                    try{if(failure!=null)throw new CompletionException(failure);
                        if(reply!=null&&!reply.getRaftGroupId().equals(RaftGroupId.valueOf(descriptor.groupId())))throw new IOException("Reply came from another group");
                        byte[] value=content(reply,maximum);lease.close();result.complete(value);
                    }catch(Throwable invalid){lease.invalidate();lease.close();result.completeExceptionally(invalid);}
                });return result;
            });return flow.copy();
        }
        @Override public CompletableFuture<byte[]> readAsync(RangeDescriptor descriptor,UUID column,byte[] key) {
            return transactionAsync(descriptor,ScopedTransactionRpc.pointRequest(column,key),true).thenApply(bytes->{
                try{return ScopedTransactionRpc.pointValue(bytes,DistributedCommandCodec.MAX_ENCODED_BYTES);}
                catch(IllegalArgumentException invalid){throw new CompletionException(new IOException("Invalid point reply",invalid));}
            });
        }
        @Override public CompletableFuture<CommandResult> commandAsync(RangeDescriptor descriptor,DistributedCommand command) {
            var message=Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)));
            return callAsync(descriptor,CommandResult.MAX_BYTES,client->metadataWriteAfterLeaderBarrier(client,descriptor,message)).thenApply(bytes->{
                try{return CommandResult.decode(bytes);}catch(IllegalArgumentException invalid){throw new CompletionException(new IOException("Invalid command reply",invalid));}
            });
        }
        @Override public CompletableFuture<byte[]> transactionAsync(RangeDescriptor descriptor,byte[] request,boolean freshBarrier) {
            var message=Message.valueOf(ByteString.copyFrom(request));
            return callAsync(descriptor,transactionReplyLimit(request),client->freshBarrier?client.async().sendReadOnly(message):client.async().sendReadOnlyNonLinearizable(message));
        }
        @Override public CompletableFuture<byte[]> participantAsync(RangeDescriptor descriptor,byte[] record) {
            var message=Message.valueOf(ByteString.copyFrom(record));
            return callAsync(descriptor,RatisSourceOutcomeRecord.isRecord(record)?37+NativeSourceOutcomeCredit.MAX_BYTES:CommandResult.MAX_BYTES,client->metadataWriteAfterLeaderBarrier(client,descriptor,message));
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

        private static RaftClient connectedSnapshot(RangeDescriptor descriptor){return snapshotBuilder(descriptor,null);}
        private static RaftClient connectedSnapshotOwner(RangeDescriptor descriptor,ReplicaDescriptor owner){return snapshotBuilder(descriptor,RaftPeerId.valueOf(owner.peerId()));}
        private static RaftClient snapshotBuilder(RangeDescriptor descriptor,RaftPeerId leader){var builder=RaftClient.newBuilder().setRaftGroup(group(descriptor)).setProperties(properties()).setRetryPolicy(snapshotRetryPolicy(descriptor));if(leader!=null)builder.setLeaderId(leader);return builder.build();}
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
