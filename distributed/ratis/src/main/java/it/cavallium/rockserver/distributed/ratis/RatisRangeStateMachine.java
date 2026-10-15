package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeSchemaInstallation;
import it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest;
import it.cavallium.rockserver.core.common.distributed.RangeImportAdmission;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.TopologyChange;
import it.cavallium.rockserver.core.common.distributed.OperatorUpload;
import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyFreeze;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyActivation;
import it.cavallium.rockserver.core.common.distributed.NativeTopologySeed;
import it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor;
import it.cavallium.rockserver.core.common.distributed.RangeDescriptor;
import it.cavallium.rockserver.core.common.distributed.SchemaChange;
import it.cavallium.rockserver.core.common.distributed.SchemaAdmissionManifest;
import it.cavallium.rockserver.core.common.distributed.NativeSchemaFence;
import it.cavallium.rockserver.core.common.distributed.NativeSchemaInstallProof;
import it.cavallium.rockserver.core.common.distributed.NativeSchemaActivationProof;
import it.cavallium.rockserver.core.common.distributed.NativeSourceOutcomeCredit;
import it.cavallium.rockserver.core.common.distributed.NativeSourceOutcomePublication;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import it.cavallium.rockserver.core.common.distributed.RangeDirectoryCodec;
import it.cavallium.rockserver.core.common.distributed.RangeSessionControl;
import it.cavallium.rockserver.core.common.distributed.HomeRecovery;
import it.cavallium.rockserver.core.common.distributed.HomeIdentity;
import it.cavallium.rockserver.core.common.distributed.HomeFenceProof;
import it.cavallium.rockserver.core.common.distributed.HomeRetirement;
import it.cavallium.rockserver.core.common.distributed.PreparedParticipant;
import it.cavallium.rockserver.core.common.distributed.ParticipantFinalization;
import it.cavallium.rockserver.core.common.distributed.ParticipantIdentity;
import it.cavallium.rockserver.core.common.distributed.ParticipantRetirement;
import it.cavallium.rockserver.core.common.distributed.ParticipantFenceProof;
import it.cavallium.rockserver.core.common.distributed.TransactionStatus;
import it.cavallium.rockserver.core.common.distributed.RangeMutationHistory;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import java.util.function.Consumer;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.common.distributed.RangeDirectory;
import it.cavallium.rockserver.core.common.distributed.RangeStateView;
import it.cavallium.rockserver.core.common.distributed.ProposalFootprints;
import it.cavallium.rockserver.core.common.distributed.SnapshotFence;
import it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus;
import it.cavallium.rockserver.core.common.distributed.SnapshotGate;
import it.cavallium.rockserver.core.common.distributed.SnapshotGateResult;
import it.cavallium.rockserver.core.common.distributed.SnapshotFenceProof;
import it.cavallium.rockserver.core.common.distributed.SnapshotGateRetirement;
import it.cavallium.rockserver.core.common.distributed.SnapshotHomeIdentity;
import it.cavallium.rockserver.core.common.distributed.SnapshotHomeRetirement;
import it.cavallium.rockserver.core.common.distributed.TransactionScope;
import it.cavallium.rockserver.core.common.distributed.TransactionAcceptance;
import it.cavallium.rockserver.core.common.distributed.CommandResult;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.MD5FileUtil;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collection;
import java.io.IOException;
import it.cavallium.rockserver.core.common.distributed.OperatorUpload;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Initial synchronous apply adapter; the pool owner drains Ratis before closing native handles. */
public final class RatisRangeStateMachine extends BaseStateMachine implements org.apache.ratis.server.leader.MigrationLogStateMachine,org.apache.ratis.server.impl.LinearizableQueryStateMachine, org.apache.ratis.server.impl.CommittedApplyStateMachine,org.apache.ratis.server.impl.LogResourceStateMachine,org.apache.ratis.server.impl.RecoveryEligibilityStateMachine {
    private RatisReferencedFiles referencedFiles;
    private static final org.apache.ratis.statemachine.StateMachine.DataApi NO_REFERENCED_DATA=new org.apache.ratis.statemachine.StateMachine.DataApi() {
        @Override public CompletableFuture<?> write(org.apache.ratis.proto.RaftProtos.LogEntryProto entry) {
            var data=entry.getStateMachineLogEntry();
            if(data.getType()==org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.Type.DATASTREAM
                    || !data.getStateMachineEntry().getStateMachineData().isEmpty()
                    || data.getStateMachineEntry().getLogEntryProtoSerializedSize()!=0)
                return CompletableFuture.failedFuture(new IOException("Referenced data owner is not configured"));
            return CompletableFuture.completedFuture(null);
        }
        @Override public CompletableFuture<ByteString> read(org.apache.ratis.proto.RaftProtos.LogEntryProto entry) {
            return CompletableFuture.failedFuture(new IOException("Referenced data owner is not configured"));
        }
        @Override public CompletableFuture<org.apache.ratis.statemachine.StateMachine.DataStream> stream(RaftClientRequest request) {
            return CompletableFuture.failedFuture(new IOException("Referenced data owner is not configured"));
        }
        @Override public CompletableFuture<?> link(org.apache.ratis.statemachine.StateMachine.DataStream stream,org.apache.ratis.proto.RaftProtos.LogEntryProto entry) {
            // Released Ratis nests the returned link future; a synchronous failure must reach its outer future.
            throw new UnsupportedOperationException("Referenced data owner is not configured");
        }
        @Override public long transferTo(Message message,java.nio.channels.WritableByteChannel destination) throws IOException {
            throw new IOException("Referenced data owner is not configured");
        }
    };
    /** Fixed before initialization. The enclosing owner closes files only after Ratis has drained its log I/O. */
    public void referencedFiles(RatisReferencedFiles files) {
        if(getLifeCycleState()!=LifeCycle.State.NEW || referencedFiles!=null) throw new IllegalStateException("Referenced data owner is fixed before initialization");
        java.util.Objects.requireNonNull(files).requireGroup(initialGroup.getGroupId().getUuid());
        referencedFiles=files;
    }
    @Override public org.apache.ratis.statemachine.StateMachine.DataApi data() {
        return referencedFiles==null ? NO_REFERENCED_DATA : referencedFiles;
    }
    private final ReplicatedRangeStore store;
    private RatisCdcPollRpc.Binding cdcBinding;
    /** Trusted namespace setup, fixed before the DATA state machine starts. */
    void cdcBinding(UUID cluster,UUID database,UUID rangeId) {
        requireDataRole();
        if(getLifeCycleState()!=LifeCycle.State.NEW || cdcBinding!=null) throw new IllegalStateException("CDC binding is fixed before initialization");
        var ownership=store.ownership(); var owner=ownership.owner();
        var binding=new RatisCdcPollRpc.Binding(cluster,database,ownership.interval().domainId(),
                new it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage(rangeId,owner.groupId(),owner.epoch()));
        try { store.enableCdc(rangeId); } catch(org.rocksdb.RocksDBException failure) { throw new IllegalStateException("Cannot configure native CDC",failure); }
        cdcBinding=binding;
    }
    private final Consumer<DistributedCommand> commandValidator;
    @FunctionalInterface public interface ImportPublicationAdmission { void validate(DistributedCommand command) throws IOException; }
    private ImportPublicationAdmission importPublicationAdmission;
    private ImportPublicationAdmission cdcDeliveryAdmission;
    private ImportPublicationAdmission topologyExecutionAdmission;
    @FunctionalInterface public interface OperationRerouteAdmission { void validate(DistributedCommand command) throws IOException; }
    private OperationRerouteAdmission rerouteReservationAdmission,reroutePromotionAdmission;
    @FunctionalInterface interface OperationRerouteProofAdmission { void validate(DistributedCommand command,RatisOperationRerouteAdmissionRpc.Evidence evidence)throws IOException; }
    private OperationRerouteProofAdmission rerouteProofAdmission;
    void operationRerouteProofAdmission(OperationRerouteProofAdmission admission){if(role!=RatisNodePlan.Role.TRANSACTION_HOME || getLifeCycleState()!=LifeCycle.State.NEW || rerouteProofAdmission!=null || rerouteReservationAdmission!=null)throw new IllegalStateException("Reroute proof admission is fixed before initialization");rerouteProofAdmission=java.util.Objects.requireNonNull(admission);}
    private ProofQueryExecutor operationRerouteQueries;
    void operationRerouteQueries(ProofQueryExecutor executor) {
        if(role!=RatisNodePlan.Role.TRANSACTION_HOME || getLifeCycleState()!=LifeCycle.State.NEW || operationRerouteQueries!=null)throw new IllegalStateException("Reroute preparation lane is fixed before HOME initialization");
        operationRerouteQueries=java.util.Objects.requireNonNull(executor);
    }
    void operationRerouteAdmission(OperationRerouteAdmission reservation,OperationRerouteAdmission promotion) {
        if(role!=RatisNodePlan.Role.TRANSACTION_HOME || getLifeCycleState()!=LifeCycle.State.NEW || !homeReservations || rerouteReservationAdmission!=null || reroutePromotionAdmission!=null || rerouteProofAdmission!=null) throw new IllegalStateException("Reroute HOME admission is fixed before initialization");
        rerouteReservationAdmission=java.util.Objects.requireNonNull(reservation);reroutePromotionAdmission=promotion;
    }
    void topologyExecutionAdmission(ImportPublicationAdmission admission) {
        if(role!=RatisNodePlan.Role.CATALOGUE || getLifeCycleState()!=LifeCycle.State.NEW || topologyExecutionAdmission!=null)throw new IllegalStateException("Topology execution admission is fixed before initialization");topologyExecutionAdmission=java.util.Objects.requireNonNull(admission);
    }
    void cdcDeliveryAdmission(ImportPublicationAdmission admission) {
        if(role!=RatisNodePlan.Role.CATALOGUE || getLifeCycleState()!=LifeCycle.State.NEW || cdcDeliveryAdmission!=null) throw new IllegalStateException("CDC delivery admission is fixed before initialization");
        cdcDeliveryAdmission=java.util.Objects.requireNonNull(admission);
    }
    private boolean requiresCataloguePreparation(DistributedCommand command) {
        return role==RatisNodePlan.Role.TRANSACTION_HOME && RatisTransactionStatuses.requiresRerouteAdmission(command)
                || role==RatisNodePlan.Role.CATALOGUE && (RatisTopologyCatalogue.requiresOwnerAdmission(command) || RatisRangeImportCatalogue.requiresOwnerAdmission(command) || RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command));
    }
    /** Trusted fresh owner checks for new publications only. Committed replay never invokes this callback. */
    public void importPublicationAdmission(ImportPublicationAdmission admission) {
        if(getLifeCycleState()!=LifeCycle.State.NEW || importPublicationAdmission!=null) throw new IllegalStateException("Import publication admission is fixed before initialization");
        importPublicationAdmission=java.util.Objects.requireNonNull(admission);
    }
    private ProofQueryExecutor importPublicationQueries;
    void importPublicationQueries(ProofQueryExecutor executor){
        if(role!=RatisNodePlan.Role.CATALOGUE || getLifeCycleState()!=LifeCycle.State.NEW || importPublicationQueries!=null)throw new IllegalStateException("Import publication dispatcher is fixed before initialization");
        importPublicationQueries=java.util.Objects.requireNonNull(executor);
    }
    private final HashMap<OperationId,ImportPublicationHold> importPublicationHolds=new HashMap<>();
    private long importPublicationBytes;
    private static class RetainedProof {
        final RatisNodeResourceBudget.Lease retainedGraph;
        final java.util.List<RatisNodeResourceBudget.Lease> inputGraphs=new java.util.ArrayList<>();
        int nativeOwners;
        RetainedProof(RatisNodeResourceBudget.Lease graph){retainedGraph=graph;}
        void closeGraph(){if(retainedGraph!=null)retainedGraph.close();inputGraphs.forEach(RatisNodeResourceBudget.Lease::close);inputGraphs.clear();}
    }
    private record SavedAppendStamp(RetainedProof owner,Object record,byte[] digest,long term,long generation,long configurationStamp) {}
    private final java.util.IdentityHashMap<TransactionContext,SavedAppendStamp> savedAppendStamps=new java.util.IdentityHashMap<>();
    private byte[] appendInputDigest(TransactionContext transaction) {
        java.security.MessageDigest digest;
        try{digest=java.security.MessageDigest.getInstance("SHA-256");}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}
        digest.update(transaction.getClientRequest().getMessage().getContent().asReadOnlyByteBuffer());return digest.digest();
    }
    /** Exact native originals replay without borrowing a new fresh-admission capability. */
    private void savedAppend(TransactionContext transaction,RetainedProof owner,Object record,Leadership current,long generation) throws IOException {
        var cut=store.applied();
        if(!current.ready() || cut==null || cut.term()!=current.term() || generation!=admissionGeneration.get() || schemaConfigurationPending)
            throw new IOException("Saved append lost completed current-term owner");
        savedAppendStamps.put(transaction,new SavedAppendStamp(owner,record,appendInputDigest(transaction),current.term(),generation,schemaConfigurationStamp));
    }
    private RatisNodeResourceBudget.Lease retainedProof(long wireBound,ProposalFootprints.GateLease scope)throws IOException {
        try{return reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+4L*wireBound,1);}
        catch(IOException failure){if(scope!=null)proposalFootprints.release(scope);throw failure;}
    }
    private long schemaRetainedBound(Object record) {
        if(record instanceof DistributedCommand)return DistributedCommandCodec.MAX_ENCODED_BYTES;
        if(record instanceof RatisSchemaInstallRecord)return (long)RatisSchemaInstallRecord.MAX_BYTES+NativeSchemaInstallation.MAX_BYTES;
        if(record instanceof RatisSchemaActivateRpc.Request)return (long)RatisSchemaActivateRpc.MAX_REQUEST_BYTES+RatisSchemaActivateRecord.MAX_BYTES;
        if(record instanceof RatisSchemaActivateRecord)return RatisSchemaActivateRecord.MAX_BYTES;
        return RatisSchemaFenceRecord.MAX_BYTES;
    }
    private static final class ImportPublicationHold extends RetainedProof {
        final DistributedCommand command;final byte[] digest;final long term,generation,configurationStamp;
        final ProposalFootprints.GateLease lease;ApplyPosition cut;
        boolean reading=true,qualified,submitted,invalidated,released;int owners;
        ImportPublicationHold(DistributedCommand command,long term,long generation,long configurationStamp,ProposalFootprints.GateLease lease,RatisNodeResourceBudget.Lease graph){super(graph);this.command=command;this.digest=command.payloadDigest();this.term=term;this.generation=generation;this.configurationStamp=configurationStamp;this.lease=lease;}
    }
    record ImportPublicationStats(int holds,long bytes,int reading) {}
    ImportPublicationStats importPublicationStats(){synchronized(admissions){return new ImportPublicationStats(importPublicationHolds.size(),importPublicationBytes,(int)importPublicationHolds.values().stream().filter(h->h.reading).count());}}
    private void releaseImportPublication(ImportPublicationHold hold){
        hold.released=true;
        // An invalidated lookup still owns its gate and metadata until its actual finally exits.
        if(hold.reading || hold.owners!=0 || hold.nativeOwners!=0)return;
        proposalFootprints.release(hold.lease);
        if(importPublicationHolds.remove(hold.command.operationId(),hold)){importPublicationBytes-=RatisProofQueryService.IMPORT_PUBLICATION_BYTES;hold.closeGraph();}
    }
    private static boolean preparedCutMatches(ImportPublicationHold hold,ApplyPosition current) {
        if(hold.cut==null || current==null)return false;
        // The drained key gate protects the witnesses while unrelated same-term metadata advances.
        return RatisTopologyCatalogue.requiresOwnerAdmission(hold.command) || RatisTransactionStatuses.requiresRerouteAdmission(hold.command)
                ? current.term()==hold.term && current.index()>=hold.cut.index()
                : hold.cut.equals(current);
    }
    private void requireImportPublication(ImportPublicationHold hold,DistributedCommand command,Leadership owner,long generation)throws IOException{
        if(hold==null || importPublicationHolds.get(command.operationId())!=hold || !Arrays.equals(hold.digest,command.payloadDigest()) || hold.reading || !hold.qualified || hold.invalidated || hold.released
                || !owner.ready() || hold.term!=owner.term() || hold.generation!=generation || generation!=admissionGeneration.get() || getLifeCycleState()!=LifeCycle.State.RUNNING
                || schemaConfigurationPending || hold.configurationStamp!=schemaConfigurationStamp || !preparedCutMatches(hold,store.applied()) || !store.admissionOpen() || !proposalFootprints.drained(hold.lease))throw new IOException("Import publication lacks current prepared owner");
    }
    private void prepareImportPublication(DistributedCommand command)throws Exception{prepareImportPublication(command,null);}
    private void prepareImportPublication(DistributedCommand command,RatisOperationRerouteAdmissionRpc.Evidence evidence)throws Exception{
        boolean reroute=role==RatisNodePlan.Role.TRANSACTION_HOME && RatisTransactionStatuses.requiresRerouteAdmission(command);
        ImportPublicationAdmission admission=reroute && rerouteProofAdmission!=null ? prepared->rerouteProofAdmission.validate(prepared,evidence) : reroute ? (RatisTransactionStatuses.isReservation(command)
                ? rerouteReservationAdmission==null ? null : rerouteReservationAdmission::validate
                : reroutePromotionAdmission==null ? null : reroutePromotionAdmission::validate)
                : RatisTopologyCatalogue.requiresOwnerAdmission(command) ? topologyExecutionAdmission : RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command) ? cdcDeliveryAdmission : importPublicationAdmission;
        if(!requiresCataloguePreparation(command))throw new IOException("Expected protected HOME/catalogue preparation");
        if(reroute)checkHomeOwner();else checkCatalogueOwner();var owner=leadership();long generation=admissionGeneration.get();
        if(!store.estimateReceiptRetention(command).newReceipt())return;
        if(admission==null) throw new IOException("Fresh catalogue delivery/publication admission is not configured");
        final ImportPublicationHold hold;
        synchronized(admissions){
            reconcileOlderProposalTerms(owner.term());
            if(schemaConfigurationPending || generation!=admissionGeneration.get())throw new IOException("Import publication configuration/owner changed");
            var existing=importPublicationHolds.get(command.operationId());
            if(existing!=null) {
                if(!Arrays.equals(existing.digest,command.payloadDigest()))throw new IOException("Import operation identity reused with different command");
                try { requireImportPublication(existing,command,owner,generation); return; }
                catch(IOException stale) {
                    if(existing.reading || existing.submitted || admissions.containsKey(command.operationId()))throw stale;
                    existing.invalidated=true;releaseImportPublication(existing);
                }
            }
            if(admissions.containsKey(command.operationId()))throw new IOException("Import operation already owns a proposal");
            if(importPublicationHolds.size()>=2 || RatisProofQueryService.IMPORT_PUBLICATION_BYTES>2*RatisProofQueryService.IMPORT_PUBLICATION_BYTES-importPublicationBytes)throw new IOException("Import publication metadata budget exhausted");
            var lease=proposalFootprints.reserveGate(proposalFootprints.prepare(store.commandAccessScope(command)),owner.term(),generation);
            if(lease==null)throw new IOException("Import publication overlaps a reserved proposal");
            hold=new ImportPublicationHold(command,owner.term(),generation,schemaConfigurationStamp,lease,retainedProof(DistributedCommandCodec.MAX_ENCODED_BYTES,lease));importPublicationHolds.put(command.operationId(),hold);importPublicationBytes+=RatisProofQueryService.IMPORT_PUBLICATION_BYTES;
        }
        boolean qualified=false;
        try{
            synchronized(admissions){if(!proposalFootprints.drained(hold.lease))throw new IOException("Import publication predecessors are still settling");}
            // Ordered native witnesses and slow configured owner reads never run under admission/Raft monitors.
            commandValidator.accept(command);store.validate(command);
            var points=new ArrayList<ReplicatedRangeStore.PhysicalPoint>();for(var predicate:command.predicates())points.add(new ReplicatedRangeStore.PhysicalPoint(predicate.columnId(),predicate.key()));
            ApplyPosition cut=null;int checked=0;
            for(int offset=0;offset<points.size();offset+=4){var keys=points.subList(offset,Math.min(offset+4,points.size()));int[] bounds=new int[keys.size()];for(int i=0;i<bounds.length;i++){byte[] expected=command.predicates().get(offset+i).expectedValue();bounds[i]=expected==null ? 1 : Math.max(1,expected.length);}var witnesses=RatisTopologyCatalogue.requiresOwnerAdmission(command) ? store.readSchemaAuthorityPoints(keys,bounds) : store.readCurrentPoints(keys);if(cut!=null && (RatisTopologyCatalogue.requiresOwnerAdmission(command)
                    ? witnesses.position()==null || witnesses.position().term()!=hold.term || witnesses.position().index()<cut.index()
                    : !cut.equals(witnesses.position())))throw new IOException("Import publication witness cut advanced");cut=witnesses.position();var values=witnesses.values();for(var value:values){int index=checked++;if(!Arrays.equals(value,command.predicates().get(index).expectedValue()) && !(role==RatisNodePlan.Role.CATALOGUE && RatisCdcSubscriptionCatalogue.requalifiesTopologyGuard(command,index,value)))throw new IOException("Import publication current native witness changed");}}
            hold.cut=cut;admission.validate(command);
            var current=leadership();
            synchronized(admissions){hold.reading=false;requireImportPublicationForQualification(hold,current);hold.qualified=true;qualified=true;}
        }finally{synchronized(admissions){hold.reading=false;if(!qualified || hold.invalidated || hold.released)releaseImportPublication(hold);}}
    }
    private void requireImportPublicationForQualification(ImportPublicationHold hold,Leadership current)throws IOException{
        if(importPublicationHolds.get(hold.command.operationId())!=hold || hold.invalidated || hold.released || !current.ready() || current.term()!=hold.term || hold.generation!=admissionGeneration.get()
                || getLifeCycleState()!=LifeCycle.State.RUNNING || schemaConfigurationPending || hold.configurationStamp!=schemaConfigurationStamp || hold.cut==null || hold.cut.term()!=current.term()
                || !preparedCutMatches(hold,store.applied()) || !store.admissionOpen() || !proposalFootprints.drained(hold.lease))throw new IOException("Import publication preparation lost its cut/owner");
    }
    private void cancelImportPublication(DistributedCommand command){synchronized(admissions){var hold=importPublicationHolds.get(command.operationId());if(hold!=null && Arrays.equals(hold.digest,command.payloadDigest()) && !hold.submitted){hold.invalidated=true;releaseImportPublication(hold);}}}
    private static byte[] importProposalBytes(Object record) {
        return record instanceof RatisReferencedFile.Request request ? request.encode() : ((RatisRangeImportRecord)record).encode();
    }
    private void requireImportProposal(Object record) {
        if(record instanceof RatisRangeImportRecord importing){importing.requireDefinition(rangeImportApply.definition());return;}
        var request=(RatisReferencedFile.Request)record; referencedFiles.requireGroup(request.groupId());
        RatisRangeImportRpc.requireFile(rangeImportApply.definition().manifest(request.groupId()),request);
    }
    private ImportApplyHold importApplyHold;
    private static final class ImportApplyHold extends RetainedProof {
        final Object record; final byte[] bytes; final long term,generation,configurationStamp; final ApplyPosition cut;
        boolean reading=true,qualified,submitted,invalidated,released,maybeAppended; int owners;
        ImportApplyHold(Object record,Leadership leader,long generation,long stamp,ApplyPosition cut,RatisNodeResourceBudget.Lease graph) {
            super(graph);this.record=record; bytes=importProposalBytes(record); term=leader.term(); this.generation=generation; configurationStamp=stamp; this.cut=cut;
        }
    }
    private void releaseImportApply(ImportApplyHold hold) { hold.released=true; if(!hold.reading && hold.owners==0 && hold.nativeOwners==0 && importApplyHold==hold){importApplyHold=null;hold.closeGraph();} }
    private ApplyPosition requireImportOwner(Leadership leader) throws IOException {
        requireHealthy(); requireDataRole(); var cut=store.applied();
        if(rangeImportApply==null || getLifeCycleState()!=LifeCycle.State.RUNNING || !leader.ready() || cut==null || cut.term()!=leader.term()
                || schemaConfigurationPending || !(store.importPending() || store.admissionOpen()))throw new IOException("Native import lacks completed current-term owner");
        return cut;
    }
    private void requireImportApply(ImportApplyHold hold,Object record,Leadership leader,long generation) throws IOException {
        var cut=requireImportOwner(leader);
        if(hold==null || importApplyHold!=hold || hold.reading || !hold.qualified || hold.invalidated || hold.released
                || !Arrays.equals(hold.bytes,importProposalBytes(record)) || hold.term!=leader.term() || hold.generation!=generation || generation!=admissionGeneration.get()
                || hold.configurationStamp!=schemaConfigurationStamp || !hold.cut.equals(cut))throw new IOException("Native import lacks exact prepared append owner");
    }
    private ReplicatedRangeStore.Configuration requireImportMembership() throws Exception {
        var configuration=store.configuration();
        if(configuration==null)throw new IOException("Native import lacks completed membership");
        var descriptor=rangeImportApply.definition().sourceDirectory().ranges().stream()
                .filter(range -> range.groupId().equals(rangeImportApply.groupId())).findFirst().orElseThrow();
        requireSchemaMembership(descriptor,configuration.data());
        if(initialGroup.getPeers().stream().anyMatch(peer->peer.getDataStreamAddress()!=null && !peer.getDataStreamAddress().isEmpty())) {
            var expected=new java.util.TreeMap<String,String>(); var actual=new java.util.TreeMap<String,String>();
            for(var peer:initialGroup.getPeers())expected.put(peer.getId().toString(),java.util.Objects.toString(peer.getDataStreamAddress(),""));
            var nativeConfiguration=RaftConfigurationProto.parseFrom(configuration.data());
            for(var peer:nativeConfiguration.getPeersList())actual.put(peer.getId().toStringUtf8(),peer.getDataStreamAddress());
            for(var peer:nativeConfiguration.getListenersList())actual.put(peer.getId().toStringUtf8(),peer.getDataStreamAddress());
            if(!expected.equals(actual))throw new IOException("Native import streaming membership differs from frozen endpoints");
        }
        return configuration;
    }
    private byte[] recoveredImportInput(Object record) throws Exception {
        return record instanceof RatisReferencedFile.Request file ? referenceProof(file) : rangeImportApply.recoveredResult((RatisRangeImportRecord)record);
    }
    private void prepareImportApply(Object record,long generation) throws Exception {
        requireImportProposal(record); var leader=leadership(); var cut=requireImportOwner(leader); final ImportApplyHold hold;
        synchronized(admissions) {
            if(generation!=admissionGeneration.get())throw new IOException("Native import read-barrier owner changed");
            reconcileOlderProposalTerms(leader.term());
            if(importApplyHold!=null) { requireImportApply(importApplyHold,record,leader,generation); return; }
            hold=new ImportApplyHold(record,leader,generation,schemaConfigurationStamp,cut,retainedProof(Math.max(RatisRangeImportRecord.MAX_BYTES,RatisReferencedFile.Request.BYTES),null)); importApplyHold=hold;
        }
        boolean qualified=false;
        try {
            requireImportMembership();
            if(record instanceof RatisRangeImportRecord.Install install)rangeImportApply.validateInstallProposal(install);
            else if(record instanceof RatisRangeImportRecord.Activate activate)rangeImportApply.validateActivationProposal(activate,rangeImportPublication);
            else if(record instanceof RatisReferencedFile.Request file)referenceProof(file);
            var current=leadership();
            synchronized(admissions) { hold.reading=false; hold.qualified=true; requireImportApply(hold,record,current,generation); qualified=true; }
        } finally { synchronized(admissions) { hold.reading=false; if(!qualified || hold.invalidated || hold.released)releaseImportApply(hold); } }
    }
    private TransactionContext startImportApply(TransactionContext transaction,Object record,long generation,Leadership leader) throws Exception {
        synchronized(admissions){if(importApplyHold!=null && importApplyHold.reading)throw new IOException("Native import preparation is still reading");}
        var saved=recoveredImportInput(record);var cut=requireImportOwner(leader);
        synchronized(admissions) {
            if(saved!=null && importApplyHold==null){
                var retained=new ImportApplyHold(record,leader,generation,schemaConfigurationStamp,cut,retainedProof(Math.max(RatisRangeImportRecord.MAX_BYTES,RatisReferencedFile.Request.BYTES),null));
                retained.reading=false;retained.qualified=true;importApplyHold=retained;
            }
            var hold=importApplyHold;
            if(saved==null)requireImportApply(hold,record,leader,generation);
            else {if(hold==null || hold.reading || !Arrays.equals(hold.bytes,importProposalBytes(record)))throw new IOException("Saved import input differs from retained owner");if(hold.owners>=16)throw new IOException("Native import proposal waiter bound exhausted");savedAppend(transaction,hold,record,leader,generation);}
            if(hold.owners>=16)throw new IOException("Native import proposal waiter bound exhausted");
            hold.submitted=true; hold.owners++; transaction.setStateMachineContext(hold); return transaction;
        }
    }
    private it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor importLocalReplica;
    private RatisRangeImportApply rangeImportApply;
    private RatisRangeImportApply.PublicationReader rangeImportPublication;
    public void rangeImportApply(RatisRangeImportApply adapter,RatisRangeImportApply.PublicationReader reader){rangeImportApply(adapter,reader,null);}
    public void rangeImportApply(RatisRangeImportApply adapter,RatisRangeImportApply.PublicationReader reader,it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor local){
        if(getLifeCycleState()!=LifeCycle.State.NEW || rangeImportApply!=null || role!=RatisNodePlan.Role.DATA)throw new IllegalStateException("Native import adapter is fixed before initialization");
        java.util.Objects.requireNonNull(adapter).requireGroup(store.ownership().owner().groupId());adapter.requireGroup(initialGroup.getGroupId().getUuid());java.util.Objects.requireNonNull(reader);
        if(local!=null){
            RatisRangeImportStatusRpc.Target.of(adapter.definition(),adapter.groupId(),local);
            if(initialGroup.getPeers().stream().noneMatch(peer->peer.getId().toString().equals(local.peerId()) && peer.getAddress().equals(local.address())))throw new IllegalArgumentException("Import local peer differs from initial group");
            importLocalReplica=local;
        }
        rangeImportApply=adapter;rangeImportPublication=reader;
    }
    private final org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Gate recoveryEligibility=new org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Gate();
    private volatile boolean recoveryReplacement=true;
    @Override public org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Gate recoveryEligibilityGate(){return recoveryEligibility;}
    /** Caller must revoke outside division/native/apply locks before starting a live replacement. */
    void prepareRecoveryReplacement(){recoveryEligibility.publish(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence.missing());}
    /** Native reads finish before the immutable Gate assignment; no division callback runs under Gate. */
    void refreshRecoveryEligibility()throws IOException {
        long revision=recoveryEligibility.revision(),generation=admissionGeneration.get();
        if(recoveryReplacement || getLifeCycleState()!=LifeCycle.State.RUNNING)return;
        org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence evidence;
        try {
            var current=store.currentRangeRecoveryRegistration();var metadata=store.metadataRecoveryRegistration();
            if(current!=null && store.currentRangeRecoveryReady() && store.admissionOpen()) {
                store.enableScopedHistory(RangeMutationHistory.Limits.defaults());if(enableSessionControls)store.enableSessionControls();
            }
            if(current==null && metadata==null)evidence=org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence.normal();
            else {
                var image=current==null?metadata.imageCut():current.imageCut();boolean installed=current==null?store.metadataRecoveryImageInstalled():store.currentRangeRecoveryImageInstalled();
                var cut=store.applied();var cfg=store.configuration();var snapshot=durableSnapshot;
                if(!installed || store.pendingInstallation() || cut==null || cfg==null || snapshot==null || !RatisRelocationOwnerRpc.covers(new ApplyPosition(snapshot.getTerm(),snapshot.getIndex()),image))evidence=org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence.missing();
                else evidence=org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence.recovered(TermIndex.valueOf(image.term(),image.index()),TermIndex.valueOf(cut.term(),cut.index()),snapshot.getTermIndex(),cfg.term(),cfg.index(),RaftConfigurationProto.parseFrom(cfg.data()));
            }
        }catch(Exception failure){recoveryEligibility.publish(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence.missing());throw new IOException("Cannot qualify native/Ratis recovery baseline",failure);}
        if(!recoveryReplacement && generation==admissionGeneration.get() && getLifeCycleState()==LifeCycle.State.RUNNING)recoveryEligibility.publishIfCurrent(revision,evidence);
    }
    private void refreshRecoveryAfterNativeCompletion()throws IOException {
        if(store.currentRangeRecoveryRegistration()!=null || store.metadataRecoveryRegistration()!=null)refreshRecoveryEligibility();
    }
    private void activateRecoveredOwner()throws IOException{
        if(store.ownership().state()==it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.State.RETIRED || store.topologyBirthPending() || store.topologyRecoveryOnly()){validateRoleNativeState();return;}
        if(store.importPending()){if(rangeImportApply==null)throw new IOException("Unpublished native import lacks configured adapter");rangeImportApply.requireGroup(store.ownership().owner().groupId());store.fence();return;}
        else store.activate();
        validateRoleNativeState();
    }
    private volatile Consumer<DistributedCommand> epochFloorAdmission;
    private RatisSessionEpochs sessionEpochs;
    private java.util.function.BiConsumer<SchemaChange.Definition,NativeSchemaActivationProof> schemaActivationObserver;
    private volatile Throwable schemaActivationObserverFailure;
    private volatile Throwable storageFailure;
    private final HashMap<OperationId, Admission> admissions = new HashMap<>();
    private long admittedBytes, dataProposalBytes;
    private int dataAdmissions, protectedAdmissions;
    private long reservedReceiptCount, reservedReceiptBytes;
    private Admission maintenanceReservation;
    private final RetentionLimits retentionLimits;
    private final boolean enableSessionControls;
    public record RetentionLimits(long dataCount, long dataBytes, long maintenanceCount, long maintenanceBytes) {
        public RetentionLimits {
            if (dataCount < 1 || dataBytes < 88 || maintenanceCount < 1 || maintenanceBytes < 88) throw new IllegalArgumentException("Invalid retention limits");
            Math.addExact(dataCount,maintenanceCount); Math.addExact(dataBytes,maintenanceBytes);
        }
        public static RetentionLimits defaults() { return new RetentionLimits(4096,16L*1024*1024,16,64*1024); }
    }
    private final java.util.concurrent.atomic.AtomicLong admissionGeneration = new java.util.concurrent.atomic.AtomicLong();
    record Leadership(long term, boolean ready) {}
    @FunctionalInterface interface LeadershipSource { Leadership get() throws IOException; }
    private final LeadershipSource leadershipSource;
    @FunctionalInterface interface TopologyAuthorityReader { RatisTopologyAuthority.View read(TopologyChange.Header header,TopologyChange.Stage stage)throws IOException; }
    private TopologyAuthorityReader topologyAuthority;
    private RangeDescriptor topologyDescriptor;private ReplicaDescriptor topologyLocalReplica;private DistributedStoragePool topologyStorage;
    void topologyAuthority(TopologyAuthorityReader reader,RangeDirectory directory,ReplicaDescriptor local,DistributedStoragePool pool){
        requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || topologyAuthority!=null)throw new IllegalStateException("Topology authority is fixed before initialization");
        var descriptor=directory.ranges().stream().filter(r->r.groupId().equals(store.ownership().owner().groupId())).findFirst().orElseThrow();
        if(!descriptor.replicas().contains(local) || !initialGroup.getGroupId().getUuid().equals(descriptor.groupId()))throw new IllegalArgumentException("Topology local owner differs from frozen placement");
        topologyAuthority=java.util.Objects.requireNonNull(reader);topologyDescriptor=descriptor;topologyLocalReplica=java.util.Objects.requireNonNull(local);topologyStorage=java.util.Objects.requireNonNull(pool);
    }
    private TopologyHold topologyHold;private int topologySlots;private long topologyBytes;
    private CompletableFuture<Void> topologyApplyCleanup=CompletableFuture.completedFuture(null);
    private static final class TopologyHold extends RetainedProof {
        final RatisTopologyFreezeRecord record;final byte[] bytes;final long term,generation,stamp;final ProposalFootprints.GateLease lease;
        ReplicatedRangeStore.TopologyFreezeGate nativeGate;
        boolean reading=true,qualified,nativeReady,submitted,maybeAppended,invalidated,released,cleaning;int owners;
        final CompletableFuture<Void> cleanup=new CompletableFuture<>();
        TopologyHold(RatisTopologyFreezeRecord record,Leadership owner,long generation,long stamp,ProposalFootprints.GateLease lease,RatisNodeResourceBudget.Lease graph){super(graph);this.record=record;bytes=record.encode();term=owner.term();this.generation=generation;this.stamp=stamp;this.lease=lease;}
    }
    record TopologyFreezeStats(int holds,long bytes,int reading) {}
    TopologyFreezeStats topologyFreezeStats(){synchronized(admissions){return new TopologyFreezeStats(topologySlots,topologyBytes,topologyHold!=null && topologyHold.reading ? 1 : 0);}}
    CompletableFuture<Void> topologyCleanup(){synchronized(admissions){return CompletableFuture.allOf(topologyApplyCleanup,topologyActivationHold==null ? CompletableFuture.completedFuture(null) : topologyActivationHold.cleanup,topologyHold==null ? CompletableFuture.completedFuture(null) : topologyHold.cleanup).copy();}}
    private void finishTopologyHold(TopologyHold hold){if(topologyHold==hold && !hold.reading && hold.owners==0 && hold.nativeOwners==0){topologyHold=null;topologySlots--;topologyBytes-=hold.bytes.length;hold.closeGraph();}}
    private void releaseTopologyHold(TopologyHold hold){
        hold.released=true;if(hold.reading || hold.owners!=0 || hold.nativeOwners!=0 || hold.cleaning)return;
        if(hold.lease!=null)proposalFootprints.release(hold.lease);
        var capture=hold.nativeGate;if(capture==null){finishTopologyHold(hold);hold.cleanup.complete(null);return;}
        hold.cleaning=true;hold.nativeGate=null;
        topologyStorage.submitTopologyCleanup(capture).whenComplete((unused,failure)->{synchronized(admissions){if(failure==null){hold.cleaning=false;finishTopologyHold(hold);}else storageFailure=failure;}if(failure==null)hold.cleanup.complete(null);else hold.cleanup.completeExceptionally(failure);});
    }
    private void requireTopologyRecord(RatisTopologyFreezeRecord record)throws IOException{
        requireDataRole();if(schemaConfigurationPending)throw new IOException("Topology FREEZE waits for queued configuration apply");if(topologyAuthority==null || topologyDescriptor==null || topologyLocalReplica==null || topologyStorage==null || !record.groupId().equals(store.ownership().owner().groupId()))throw new IOException("Topology FREEZE owner is not configured");
        var definition=record.definition();var descriptor=definition.parents().stream().filter(r->r.groupId().equals(record.groupId())).findFirst().orElseThrow();var ownership=store.ownership();var interval=ownership.interval();var schema=store.installedSchema();
        if(schema==null)throw new IOException("Topology FREEZE native schema unavailable");schema.requireMatches(definition.sourceDirectory());
        if(definition.kind()==TopologyChange.Kind.RELOCATE || descriptor.rangeEpoch()!=ownership.owner().epoch() || !descriptor.rangeId().equals(topologyDescriptor.rangeId()) || descriptor.configurationVersion()!=topologyDescriptor.configurationVersion()
                || !descriptor.replicas().equals(topologyDescriptor.replicas()) || !descriptor.replicas().contains(topologyLocalReplica) || !definition.header().domainId().equals(interval.domainId())
                || !Arrays.equals(descriptor.lowerInclusive(),interval.lowerInclusive()) || !Arrays.equals(descriptor.upperExclusive(),interval.upperExclusive()))throw new IOException("Topology FREEZE native placement/schema changed");
        final ReplicatedRangeStore.Configuration configuration;
        try { configuration=store.configuration(); } catch(org.rocksdb.RocksDBException failure) { throw new IOException("Cannot read topology FREEZE native membership",failure); }
        if(configuration==null)throw new IOException("Topology FREEZE native membership missing");requireSchemaMembership(descriptor,configuration.data());
        var server=getServer().getNow(null);if(server==null || !server.getId().toString().equals(topologyLocalReplica.peerId()))throw new IOException("Topology FREEZE local peer identity changed");
    }
    private void requireTopologyOwner(TopologyHold hold,RatisTopologyFreezeRecord record,Leadership current,long generation,boolean ready)throws IOException{
        var applied=store.applied();
        if(topologyHold!=hold || hold==null || !hold.record.equals(record) || hold.reading || !hold.qualified || hold.invalidated || hold.released || !current.ready() || hold.term!=current.term() || hold.generation!=generation || generation!=admissionGeneration.get()
                || schemaConfigurationPending || hold.stamp!=schemaConfigurationStamp || getLifeCycleState()!=LifeCycle.State.RUNNING || applied==null || applied.term()!=current.term() || !store.admissionOpen()
                || ready && (!hold.nativeReady || hold.lease!=null && !proposalFootprints.drained(hold.lease)))throw new IOException("Topology FREEZE lacks current drained preparation");
    }
    private void prepareTopologyFreeze(RatisTopologyFreezeRecord record,long generation)throws Exception{
        requireTopologyRecord(record);requireHealthy();var owner=leadership();var applied=store.applied();
        if(!owner.ready() || applied==null || applied.term()!=owner.term() || getLifeCycleState()!=LifeCycle.State.RUNNING || !store.admissionOpen())throw new IOException("Topology FREEZE leader is not ready");
        var saved=store.lookupTopologyFreeze(record.header(),record.admissionDigest());if(saved!=null){saved.requireCut(applied);return;}
        final TopologyHold hold;
        synchronized(admissions){reconcileOlderProposalTerms(owner.term());if(schemaConfigurationPending || generation!=admissionGeneration.get())throw new IOException("Topology FREEZE configuration changed");
            if(topologyHold!=null){hold=topologyHold;requireTopologyOwner(hold,record,owner,generation,false);if(hold.nativeGate!=null)return;hold.reading=true;}
            else {
            if(sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16)throw new IOException("Protected topology proposal budget exhausted");
            var spans=record.definition().sourceDirectory().columns().stream().map(column->TransactionScope.Span.range(column,null,null)).toList();var lease=proposalFootprints.reserveGate(proposalFootprints.prepare(spans),owner.term(),generation);
            if(lease==null)throw new IOException("Topology FREEZE scope already reserved");hold=new TopologyHold(record,owner,generation,schemaConfigurationStamp,lease,retainedProof(RatisTopologyFreezeRecord.MAX_BYTES,lease));topologyHold=hold;topologySlots++;topologyBytes+=hold.bytes.length;}
        }
        boolean qualified=false;
        try{
            var view=topologyAuthority.read(record.header(),TopologyChange.Stage.FREEZE);view.require(RatisTopologyAuthority.Request.of(record.header(),TopologyChange.Stage.FREEZE));
            if(!Arrays.equals(view.definition().encode(),record.definition().encode()) || !Arrays.equals(view.admissionDigest(),record.admissionDigest()))throw new IOException("Topology FREEZE catalogue admission changed");
            requireTopologyRecord(record);boolean drained;synchronized(admissions){if(hold.invalidated || hold.released || generation!=admissionGeneration.get() || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Topology FREEZE preparation lost replacement owner");drained=proposalFootprints.drained(hold.lease);}
            var capture=drained ? store.prepareTopologyFreeze(record.definition(),record.admissionDigest()) : null;var state=capture==null ? null : capture.state();var current=leadership();
            synchronized(admissions){hold.nativeGate=capture;hold.reading=false;hold.qualified=true;hold.nativeReady=state!=null && state.ready();requireTopologyOwner(hold,record,current,generation,false);qualified=true;}
            if(capture!=null)capture.whenReady().whenComplete((unused,failure)->{synchronized(admissions){if(topologyHold!=hold || hold.released || hold.invalidated)return;if(failure!=null){hold.invalidated=true;if(!hold.submitted)releaseTopologyHold(hold);}else hold.nativeReady=true;}});
        }finally{synchronized(admissions){hold.reading=false;if(!qualified || hold.invalidated || hold.released)releaseTopologyHold(hold);}}
    }
    private RatisTopologyFreezeRpc.State topologyState(RatisTopologyFreezeRecord record)throws Exception{
        final long stamp;synchronized(admissions){stamp=schemaConfigurationStamp;}
        requireTopologyRecord(record);var freeze=store.lookupTopologyFreeze(record.header(),record.admissionDigest());if(freeze!=null){record.requireFreeze(freeze);freeze.requireCut(store.applied());}
        final TopologyHold hold;final ReplicatedRangeStore.TopologyFreezeGate capture;
        synchronized(admissions){hold=topologyHold;capture=hold!=null && hold.record.equals(record) && !hold.reading && !hold.released ? hold.nativeGate : null;}
        var state=capture==null ? store.topologyFreezeState() : capture.state();boolean ready=freeze!=null && state.ready();
        synchronized(admissions){if(capture!=null && topologyHold==hold && !hold.invalidated && !hold.released && !schemaConfigurationPending && hold.stamp==schemaConfigurationStamp && hold.generation==admissionGeneration.get()){hold.nativeReady=state.ready();ready=state.ready() && hold.qualified && proposalFootprints.drained(hold.lease);}}
        requireTopologyRecord(record);synchronized(admissions){if(stamp!=schemaConfigurationStamp || schemaConfigurationPending)throw new IOException("Topology FREEZE state configuration changed");}
        return new RatisTopologyFreezeRpc.State(record,freeze,state.writablePins(),state.preparedParticipants(),state.snapshotGates(),ready);
    }
    TopologyChange.Proof topologyFreezeProof(RatisTopologyFreezeRecord record)throws Exception{
        requireTopologyRecord(record);var freeze=store.lookupTopologyFreeze(record.header(),record.admissionDigest());if(freeze==null)throw new IOException("Topology FREEZE has no committed native artifact");record.requireFreeze(freeze);freeze.requireCut(store.applied());return freeze.proof(topologyLocalReplica.replicaId());
    }
    private TransactionContext startTopologyFreeze(TransactionContext transaction,RatisTopologyFreezeRecord record,long generation,Leadership owner)throws Exception{
        requireTopologyRecord(record);var saved=store.lookupTopologyFreeze(record.header(),record.admissionDigest());
        synchronized(admissions){reconcileOlderProposalTerms(owner.term());
            if(saved!=null && topologyHold==null){if(sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16)throw new IOException("Protected topology retry budget exhausted");topologyHold=new TopologyHold(record,owner,generation,schemaConfigurationStamp,null,retainedProof(RatisTopologyFreezeRecord.MAX_BYTES,null));topologyHold.reading=false;topologyHold.qualified=topologyHold.nativeReady=true;topologySlots++;topologyBytes+=topologyHold.bytes.length;}
            var hold=topologyHold;if(saved==null)requireTopologyOwner(hold,record,owner,generation,true);else {if(hold==null || hold.reading || !hold.record.equals(record))throw new IOException("Saved topology input differs from retained owner");savedAppend(transaction,hold,record,owner,generation);}hold.submitted=true;hold.owners++;transaction.setStateMachineContext(hold);return transaction;}
    }
    private byte[] topologyRecovered(RatisTopologyFreezeRecord record){var freeze=store.lookupTopologyFreeze(record.header(),record.admissionDigest());if(freeze==null)return null;freeze.requireCut(store.applied());synchronized(admissions){if(topologyHold!=null && topologyHold.record.equals(record))releaseTopologyHold(topologyHold);}return freeze.encode();}
    @FunctionalInterface interface TopologySeedInstaller { NativeTopologySeed install(RatisTopologySeedRpc.Request request)throws IOException; }
    private TopologySeedInstaller topologySeedInstaller;
    void topologySeedInstaller(TopologySeedInstaller installer) { requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || topologySeedInstaller!=null)throw new IllegalStateException("Topology seed installer is fixed before initialization");topologySeedInstaller=java.util.Objects.requireNonNull(installer); }
    private ActivationHold topologyActivationHold;
    private java.util.function.BiConsumer<TopologyChange.Definition,NativeTopologyActivation> topologyActivationObserver;
    private volatile Throwable topologyActivationObserverFailure;
    void topologyActivated(java.util.function.BiConsumer<TopologyChange.Definition,NativeTopologyActivation> observer) {
        requireDataRole(); if(getLifeCycleState()!=LifeCycle.State.NEW || topologyActivationObserver!=null)throw new IllegalStateException("Topology activation observer is fixed before initialization"); topologyActivationObserver=java.util.Objects.requireNonNull(observer);
    }
    Throwable topologyActivationObserverFailure() { return topologyActivationObserverFailure; }
    private static final class ActivationHold extends RetainedProof {
        final RatisTopologyActivationRecord record; final long term,generation,stamp; final ApplyPosition cut; final ProposalFootprints.GateLease lease;
        final CompletableFuture<Void> cleanup=new CompletableFuture<>();
        boolean reading=true,qualified,submitted,maybeAppended,invalidated,released; int owners;
        ActivationHold(RatisTopologyActivationRecord record,Leadership leader,long generation,long stamp,ApplyPosition cut,ProposalFootprints.GateLease lease,RatisNodeResourceBudget.Lease graph) { super(graph);this.record=record; term=leader.term();this.generation=generation;this.stamp=stamp;this.cut=cut;this.lease=lease; }
    }
    record TopologyActivationStats(int holds,long bytes,int reading) {}
    TopologyActivationStats topologyActivationStats() { synchronized(admissions) { var h=topologyActivationHold; return new TopologyActivationStats(h==null?0:1,h==null?0:h.record.encode().length,h!=null && h.reading?1:0); } }
    private void releaseTopologyActivation(ActivationHold hold) {
        hold.released=true; if(hold.reading || hold.owners!=0 || hold.nativeOwners!=0)return;
        if(hold.lease!=null)proposalFootprints.release(hold.lease);
        if(topologyActivationHold==hold) { topologyActivationHold=null;topologySlots--;topologyBytes-=hold.record.encode().length;hold.closeGraph(); }
        hold.cleanup.complete(null);
    }
    private NativeTopologyActivation savedTopologyActivation(RatisTopologyActivationRecord record) {
        var saved=store.topologyActivation(); if(saved==null)return null;
        RatisTopologyActivationRpc.requireActivation(record,saved); var seed=topologyStorage.topologySeed(saved.birth().intentId()); if(seed==null)throw new IllegalArgumentException("Topology activation original seed missing"); saved.requireSeed(seed); saved.requireCut(store.applied()); return saved;
    }
    private void requireTopologyActivationRecord(RatisTopologyActivationRecord record)throws Exception {
        requireDataRole(); if(topologyAuthority==null || topologyStorage==null || topologyLocalReplica==null || topologyDescriptor==null || !record.groupId().equals(store.ownership().owner().groupId()) || schemaConfigurationPending)throw new IOException("Topology activation local owner is not configured");
        var seed=topologyStorage.topologySeed(record.header().intentId()); var birth=store.topologyBirth(); if(seed==null || birth==null || store.topologyRetired())throw new IOException("Topology activation requires retained live local child");
        birth.requireSeed(seed); seed.header().requireDefinition(record.definition()); if(!seed.localReplica().equals(topologyLocalReplica) || !Arrays.equals(seed.authority().freezeDigest(),record.freezeDigest()))throw new IOException("Topology activation local seed/source changed");
        var target=birth.resolveDescriptor(seed);var interval=store.ownership().interval();
        if(!target.equals(topologyDescriptor) || target.rangeEpoch()!=store.ownership().owner().epoch() || !record.header().domainId().equals(interval.domainId()) || !Arrays.equals(target.lowerInclusive(),interval.lowerInclusive()) || !Arrays.equals(target.upperExclusive(),interval.upperExclusive()))throw new IOException("Topology activation frozen placement changed");
        if(store.topologyActivation()==null) { if(!store.topologyBirthPending() || store.ownership().state()!=RangeOwnership.State.FENCED)throw new IOException("Topology activation requires fenced pending birth"); var schema=store.installedSchema();if(schema==null)throw new IOException("Topology activation schema unavailable"); schema.requireMatches(record.definition().targetDirectory()); }
        var cfg=store.configuration();if(cfg==null)throw new IOException("Topology activation own native configuration absent");requireSchemaMembership(target,cfg.data());
        var cut=store.applied(); if(cut==null || cut.term()<=0 || cut.index()<0 || cfg.term()>cut.term() || cfg.index()>cut.index())throw new IOException("Topology activation own prefix/configuration incomplete");
        var server=getServer().getNow(null);if(server==null || !server.getId().toString().equals(topologyLocalReplica.peerId()))throw new IOException("Topology activation local server identity changed");
    }
    private void requireTopologyActivationOwner(ActivationHold hold,RatisTopologyActivationRecord record,Leadership leader,long generation)throws IOException {
        var cut=store.applied(); if(hold==null || topologyActivationHold!=hold || !hold.record.equals(record) || hold.reading || !hold.qualified || hold.released || hold.invalidated || !leader.ready() || leader.term()!=hold.term || hold.generation!=generation || generation!=admissionGeneration.get() || hold.stamp!=schemaConfigurationStamp || schemaConfigurationPending || getLifeCycleState()!=LifeCycle.State.RUNNING || cut==null || cut.term()!=leader.term() || !cut.equals(hold.cut) || store.topologyRetired() || !(store.topologyBirthPending() || store.admissionOpen()) || hold.lease!=null && !proposalFootprints.drained(hold.lease))throw new IOException("Topology activation lacks current prepared owner");
    }
    private void prepareTopologyActivation(RatisTopologyActivationRecord record,long generation)throws Exception {
        requireTopologyActivationRecord(record); var leader=leadership(); var cut=store.applied(); if(!leader.ready() || cut.term()!=leader.term() || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Topology activation leader lacks completed current term");
        if(savedTopologyActivation(record)!=null)return; final ActivationHold hold;
        synchronized(admissions) {
            reconcileOlderProposalTerms(leader.term()); if(schemaConfigurationPending || generation!=admissionGeneration.get())throw new IOException("Topology activation owner changed");
            if(topologyActivationHold!=null) {
                var existing=topologyActivationHold;
                if(!existing.record.equals(record))throw new IOException("Topology activation identity already reserved");
                try { requireTopologyActivationOwner(existing,record,leader,generation);return; }
                catch(IOException stale) { if(existing.reading || existing.submitted || existing.owners!=0)throw stale;existing.invalidated=true;releaseTopologyActivation(existing); }
            }
            if(sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16)throw new IOException("Protected activation proposal budget exhausted");
            var spans=record.definition().targetDirectory().columns().stream().map(c->TransactionScope.Span.range(c,null,null)).toList();var lease=proposalFootprints.reserveGate(proposalFootprints.prepare(spans),leader.term(),generation);if(lease==null)throw new IOException("Topology activation scope already reserved");
            hold=new ActivationHold(record,leader,generation,schemaConfigurationStamp,cut,lease,retainedProof(RatisTopologyActivationRecord.MAX_BYTES,lease));topologyActivationHold=hold;topologySlots++;topologyBytes+=record.encode().length;
        }
        boolean qualified=false;
        try {
            synchronized(admissions) { if(!proposalFootprints.drained(hold.lease))throw new IOException("Topology activation predecessors still settling"); }
            var view=topologyAuthority.read(record.header(),TopologyChange.Stage.ACTIVATE);record.requireAuthority(view);requireTopologyActivationRecord(record);var current=leadership();
            synchronized(admissions) { hold.reading=false;hold.qualified=true;requireTopologyActivationOwner(hold,record,current,generation);qualified=true; }
        } finally { synchronized(admissions) { hold.reading=false;if(!qualified || hold.released || hold.invalidated)releaseTopologyActivation(hold); } }
    }
    private RatisTopologyActivationRpc.State topologyActivationState(RatisTopologyActivationRecord record)throws Exception {
        requireTopologyActivationRecord(record);var saved=savedTopologyActivation(record);boolean ready=saved!=null;
        var current=leadership();
        synchronized(admissions) { if(saved==null && topologyActivationHold!=null) { try { requireTopologyActivationOwner(topologyActivationHold,record,current,admissionGeneration.get());ready=true; } catch(IOException stale) { ready=false; } } }
        return new RatisTopologyActivationRpc.State(record,saved,ready);
    }
    private TransactionContext startTopologyActivation(TransactionContext transaction,RatisTopologyActivationRecord record,long generation,Leadership leader)throws Exception {
        requireTopologyActivationRecord(record);var saved=savedTopologyActivation(record);
        synchronized(admissions) { reconcileOlderProposalTerms(leader.term());
            if(saved!=null && topologyActivationHold==null) { if(sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16)throw new IOException("Topology activation retry budget exhausted");var hold=new ActivationHold(record,leader,generation,schemaConfigurationStamp,store.applied(),null,retainedProof(RatisTopologyActivationRecord.MAX_BYTES,null));hold.reading=false;hold.qualified=true;topologyActivationHold=hold;topologySlots++;topologyBytes+=record.encode().length; }
            var hold=topologyActivationHold;if(saved==null)requireTopologyActivationOwner(hold,record,leader,generation);else {if(hold==null || hold.reading || !hold.record.equals(record))throw new IOException("Saved activation input differs from retained owner");savedAppend(transaction,hold,record,leader,generation);}hold.submitted=true;hold.owners++;transaction.setStateMachineContext(hold);return transaction;
        }
    }
    private byte[] topologyActivationRecovered(RatisTopologyActivationRecord record) {
        var saved=savedTopologyActivation(record);if(saved==null)return null; synchronized(admissions) { if(topologyActivationHold!=null && topologyActivationHold.record.equals(record))releaseTopologyActivation(topologyActivationHold); } return saved.encode();
    }
    private RatisTopologyOwnerRpc.Evidence topologyOwnerEvidence(RatisTopologyOwnerRpc.Target target)throws Exception {
        requireHealthy();requireDataRole();if(getLifeCycleState()!=LifeCycle.State.RUNNING || topologyStorage==null || topologyDescriptor==null || topologyLocalReplica==null || !target.group().equals(store.ownership().owner().groupId()) || !target.replica().equals(topologyLocalReplica.replicaId()) || !target.owner().equals(topologyLocalReplica) || !target.range().equals(topologyDescriptor))throw new IOException("Topology proof does not address actual initialized local owner");
        var server=getServer().getNow(null);if(server==null || !server.getId().toString().equals(topologyLocalReplica.peerId()))throw new IOException("Topology proof local server changed");
        var division=server.getDivision(getGroupId());final org.apache.ratis.server.RaftConfiguration raftConfiguration;final long term;
        synchronized(division) { if(division.getStateMachine()!=this || division.getInfo().getLifeCycleState()!=LifeCycle.State.RUNNING || !division.getInfo().isAlive())throw new IOException("Topology proof division is not the running state-machine owner");term=division.getInfo().getCurrentTerm();raftConfiguration=division.getRaftConf(); }
        final long generation=admissionGeneration.get(),stamp;
        synchronized(admissions) { if(schemaConfigurationPending)throw new IOException("Topology proof waits for configuration completion");stamp=schemaConfigurationStamp; }
        var cut=store.applied();var cfg=store.configuration();if(cut==null || cut.term()<=0 || cut.index()<0 || cut.term()!=term || cfg==null || cfg.term()>cut.term() || cfg.index()>cut.index() || cfg.index()==cut.index() && cfg.term()!=cut.term())throw new IOException("Topology proof lacks actual current-term completed prefix/configuration");
        requireSchemaMembership(target.range(),cfg.data());
        for(var role:List.of(RaftPeerRole.FOLLOWER,RaftPeerRole.LISTENER)) {
            if(!raftConfiguration.getPreviousPeers(role).isEmpty())throw new IOException("Topology proof waits for stable actual Raft membership");
            var expected=new java.util.TreeMap<String,String>();var actual=new java.util.TreeMap<String,String>();
            for(var replica:target.range().replicas())if((replica.role()==ReplicaDescriptor.Role.LISTENER)==(role==RaftPeerRole.LISTENER))expected.put(replica.peerId(),replica.address());
            for(var peer:raftConfiguration.getCurrentPeers(role))actual.put(peer.getId().toString(),peer.getAddress());if(!actual.equals(expected))throw new IOException("Topology proof actual division address/role configuration changed");
        }
        if(raftConfiguration.getLogEntryIndex()>cut.index() || cfg.index()>=0 && raftConfiguration.getLogEntryIndex()!=cfg.index())throw new IOException("Topology proof actual/native configuration positions differ");
        final TopologyChange.Proof proof;final byte[] artifact;
        switch(target.stage()) {
            case FREEZE -> {
                var view=new RatisTopologyAuthority.View(target.catalogue(),cut,target.definition(),TopologyChange.State.begin(target.definition().header()).beginFencing());
                var freeze=store.lookupTopologyFreeze(target.definition().header(),view.admissionDigest());if(freeze==null)throw new IOException("Topology proof has no original native FREEZE");freeze.requireCut(cut);proof=freeze.proof(topologyLocalReplica.replicaId());artifact=freeze.encode();
            }
            case SEED -> {
                var seed=topologyStorage.topologySeed(target.definition().intentId());var birth=store.topologyBirth();if(seed==null || birth==null || !seed.localReplica().equals(topologyLocalReplica))throw new IOException("Topology proof has no actual local SEED");seed.header().requireDefinition(target.definition());birth.requireSeed(seed);proof=birth.proof(seed);artifact=seed.encode();
            }
            case ACTIVATE -> {
                var seed=topologyStorage.topologySeed(target.definition().intentId());var receipt=store.topologyActivation();if(seed==null || receipt==null || !receipt.catalogueGroup().equals(target.catalogue()))throw new IOException("Topology proof has no actual local ACTIVATE");receipt.requireSeed(seed);receipt.header().requireDefinition(target.definition());receipt.requireCut(cut);proof=receipt.proof();artifact=receipt.encode();
            }
            default -> throw new IOException("Unknown topology proof stage");
        }
        var evidence=new RatisTopologyOwnerRpc.Evidence(target,topologyLocalReplica,proof,cut,cfg.term(),cfg.index(),cfg.data(),term,artifact);
        synchronized(division) { if(division.getStateMachine()!=this || division.getInfo().getLifeCycleState()!=LifeCycle.State.RUNNING || !division.getInfo().isAlive() || division.getInfo().getCurrentTerm()!=term || division.getRaftConf()!=raftConfiguration)throw new IOException("Topology proof actual division changed during native lookup"); }
        synchronized(admissions) { requireHealthy();if(getLifeCycleState()!=LifeCycle.State.RUNNING || generation!=admissionGeneration.get() || stamp!=schemaConfigurationStamp || schemaConfigurationPending || !cut.equals(store.applied()))throw new IOException("Topology proof native owner/cut changed"); }
        var current=store.configuration();if(current==null || current.term()!=cfg.term() || current.index()!=cfg.index() || !Arrays.equals(current.data(),cfg.data()))throw new IOException("Topology proof native membership changed");
        return evidence;
    }
    private RatisScopedTransactions scopedTransactions;
    private RatisSnapshotPins capturedSnapshots;
    private RatisSnapshotCohortService snapshotCohorts;
    void snapshotCohorts(RatisSnapshotCohortService service){requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || snapshotCohorts!=null)throw new IllegalStateException("Cohort owner fixed before initialization");snapshotCohorts=java.util.Objects.requireNonNull(service);}
    private RatisSnapshotPins.Limits snapshotPinLimits=RatisSnapshotPins.Limits.defaults();
    private java.util.function.LongSupplier snapshotPinClock=System::nanoTime;
    private boolean snapshotTtlConfigured;
    void snapshotReadOnlyMaximumTtl(java.time.Duration maximum){requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || snapshotTtlConfigured)throw new IllegalStateException("Snapshot read-only TTL fixed before initialization");RatisSnapshotCaptureRequest.requireTtl(maximum,RatisSnapshotCaptureRequest.Mode.READ_ONLY);var base=RatisSnapshotPins.Limits.defaults();snapshotPinLimits=new RatisSnapshotPins.Limits(base.captures(),base.bytes(),maximum);snapshotTtlConfigured=true;capturedSnapshots=newSnapshotPins(snapshotPinClock);}
    @FunctionalInterface interface SnapshotHelpAction { boolean run() throws Exception; }
    @FunctionalInterface interface SnapshotHelpExecutor { CompletableFuture<Boolean> submit(UUID group,ScopedTransactionRpc.SnapshotBinding binding,SnapshotHelpAction action); }
    @FunctionalInterface interface ProofQueryAction { Message run(byte[] ownedRequest) throws Exception; }
    @FunctionalInterface interface ProofQueryExecutor { CompletableFuture<Message> submit(UUID group,byte[] encoded,long estimatedBytes,boolean coalesce,ProofQueryAction action); }
    private SnapshotHelpExecutor snapshotHelpExecutor;
    private ProofQueryExecutor proofQueries;
    private ProofQueryExecutor schemaAuthorityQueries;
    private RatisExportSourceService exportSources;
    private RatisSnapshotReadinessHelper.Recovery snapshotRecovery;
    private final HashMap<UUID,RatisSnapshotReadinessHelper> snapshotHelpers=new HashMap<>();
    private RatisSnapshotFences snapshotHomes;
    private RatisTransactionStatuses transactionHomes;
    @FunctionalInterface interface HomeRecoveryProofReader { it.cavallium.rockserver.core.common.distributed.ParticipantOutcome read(HomeRecovery recovery) throws IOException; }
    private HomeRecoveryProofReader homeRecoveryProofs;
    @FunctionalInterface interface HomeRecoveryFenceReader { ParticipantFenceProof read(ParticipantIdentity identity) throws IOException; }
    private HomeRecoveryFenceReader homeRecoveryFences;
    @FunctionalInterface interface SnapshotHomeProofReader { SnapshotFenceProof read(SnapshotFence fence,UUID group) throws IOException; }
    private SnapshotHomeProofReader snapshotHomeProofs;
    private boolean snapshotHomeConfigured;
    private boolean homeReservations;
    private UUID homeCluster,homeDatabase,homeColumn;
    /** Trusted node configuration, installed before transport exposure; request bytes never select a home connector. */
    @FunctionalInterface interface HomeValueValidator { void validate(byte[] key,byte[] value); }
    private RatisNodePlan.Role role=RatisNodePlan.Role.DATA;
    private boolean roleConfigured;
    private HomeValueValidator homeValues;
    void role(RatisNodePlan.Role role) { role(role,null); }
    void role(RatisNodePlan.Role role,HomeValueValidator values) {
        if (getLifeCycleState()!=LifeCycle.State.NEW || roleConfigured) throw new IllegalStateException("Range role is fixed before initialization");
        java.util.Objects.requireNonNull(role);
        boolean home=role==RatisNodePlan.Role.TRANSACTION_HOME || role==RatisNodePlan.Role.SNAPSHOT_HOME;
        if (home!=(values!=null) || role!=RatisNodePlan.Role.DATA && (snapshotHomes!=null || transactionHomes!=null || snapshotRecovery!=null)) throw new IllegalArgumentException("Invalid range role capabilities");
        this.role=role; homeValues=values; roleConfigured=true;
    }
    @FunctionalInterface interface SchemaAuthorityReader { RatisSchemaAuthority.View read(SchemaChange.Header header,byte[] admissionDigest) throws IOException; }
    private SchemaAuthorityReader schemaAuthority;
    private it.cavallium.rockserver.core.common.distributed.RangeDescriptor schemaDescriptor;
    private UUID schemaCluster,schemaDatabase,schemaColumn;
    private SchemaHold schemaHold;
    private DistributedStoragePool schemaStorage;
    private RatisSchemaReplicaService schemaReplicas;
    private it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor schemaLocalReplica;
    private RatisSchemaReplicaClient schemaFencingReplicas;
    private int schemaSlots;
    // Queue-time schema authority invalidation; ordinary DATA generation changes only on native apply.
    private long schemaConfigurationStamp; // guarded by admissions
    private volatile boolean schemaConfigurationPending;
    private volatile boolean currentDataVoter=true;
    private void updateDataVoter(byte[] configuration)throws IOException{if(role!=RatisNodePlan.Role.DATA || topologyLocalReplica==null)return;var cfg=RaftConfigurationProto.parseFrom(configuration);String peer=topologyLocalReplica.peerId();currentDataVoter=java.util.stream.Stream.concat(cfg.getPeersList().stream(),cfg.getOldPeersList().stream()).anyMatch(p->p.getId().toStringUtf8().equals(peer));}
    private void requireUserReplica()throws IOException{if(role==RatisNodePlan.Role.DATA && !currentDataVoter)throw new IOException("Local replica is no longer a current voting USER owner");}
    private static final class SchemaHold extends RetainedProof {
        final Object record;
        ReplicatedRangeStore.SchemaInstallCaptureGate captureGate;
        DistributedStoragePool.SchemaInstallPlan installPlan;
        RatisSchemaActivateRecord activation;
        it.cavallium.rockserver.core.common.distributed.NativeSchemaCapacityReservation capacity;
        final long term,generation,configurationStamp;
        final ProposalFootprints.GateLease lease;
        boolean reading=true,qualified,submitted,maybeAppended,invalidated,released,nativeReady,cleaning;
        final CompletableFuture<Void> cleanup=new CompletableFuture<>();
        int owners;
        SchemaHold(Object record,long term,long generation,long configurationStamp,ProposalFootprints.GateLease lease,RatisNodeResourceBudget.Lease graph) {
            super(graph);this.record=record; this.term=term; this.generation=generation; this.configurationStamp=configurationStamp; this.lease=lease;
        }
    }
    void schemaReplicas(RatisSchemaReplicaService service,it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor replica) {
        requireDataRole();
        if (getLifeCycleState()!=LifeCycle.State.NEW || schemaReplicas!=null || schemaAuthority==null || schemaDescriptor==null
                || !schemaDescriptor.replicas().contains(replica)) throw new IllegalStateException("Schema replica owner is fixed before initialization");
        schemaReplicas=java.util.Objects.requireNonNull(service); schemaLocalReplica=replica;
        service.register(store.ownership().owner().groupId(),store,replica,header -> schemaAuthority.read(header,null),this::validateSchemaReplicaTarget);
    }
    private long validateSchemaReplicaTarget(RatisSchemaReplicaRpc.Target target,boolean currentSource) throws Exception {
        requireSchemaGroup(target.binding().groupId()); requireHealthy();
        if (getLifeCycleState()!=LifeCycle.State.RUNNING || schemaConfigurationPending || !store.admissionOpen() || store.applied()==null) throw new IOException("Schema replica lacks a completed running configuration");
        long stamp; synchronized (admissions) { stamp=schemaConfigurationStamp; }
        var descriptor=target.binding().source(); var ownership=store.ownership(); var interval=ownership.interval();
        if (schemaDescriptor==null || !descriptor.rangeId().equals(schemaDescriptor.rangeId()) || descriptor.rangeEpoch()!=ownership.owner().epoch()
                || descriptor.configurationVersion()!=schemaDescriptor.configurationVersion() || !descriptor.replicas().equals(schemaDescriptor.replicas())
                || !interval.domainId().equals(target.binding().header().domainId()) || !Arrays.equals(interval.lowerInclusive(),descriptor.lowerInclusive())
                || !Arrays.equals(interval.upperExclusive(),descriptor.upperExclusive())) throw new IOException("Schema replica source placement changed");
        var cfg=store.configuration(); if (cfg==null) throw new IOException("Schema replica has no native configuration");
        requireSchemaMembership(descriptor,cfg.data());
        if (currentSource) requireSchemaOwnership(target.binding().definition().sourceDirectory(),target.binding().groupId());
        synchronized (admissions) { if (schemaConfigurationPending || stamp!=schemaConfigurationStamp) throw new IOException("Schema replica configuration changed during validation"); }
        return stamp;
    }
    /** Actual listener division and completed native tail evidence; this confers no USER authority. */
    public ReplicatedRangeStore.TopologyLearnerReadiness topologyLearnerReadiness(ApplyPosition requiredCut)throws IOException {
        requireDataRole();requireHealthy();var registration=store.topologyLearnerRegistration();if(registration==null)throw new IOException("No receiver-local learner registration");
        long generation=admissionGeneration.get();final long stamp;synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Learner configuration apply pending");stamp=schemaConfigurationStamp;}
        var server=getServer().getNow(null);if(server==null || !server.getId().toString().equals(registration.receiver().peerId()))throw new IOException("Learner server identity differs");var division=server.getDivision(initialGroup.getGroupId());var info=division.getInfo();var term=info.getCurrentTerm();var configuration=division.getRaftConf();
        if(getLifeCycleState()!=LifeCycle.State.RUNNING || division.getStateMachine()!=this || !info.isAlive() || info.getLifeCycleState()!=LifeCycle.State.RUNNING || term<1)throw new IOException("Learner division is not actual running owner");
        for(var role:List.of(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER,org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)){
            if(!configuration.getPreviousPeers(role).isEmpty())throw new IOException("Learner readiness requires stable configuration");var actual=new java.util.TreeMap<String,String>();for(var peer:configuration.getCurrentPeers(role))actual.put(peer.getId().toString(),peer.getAddress());var expected=new java.util.TreeMap<String,String>();for(var replica:registration.definition().targets().getFirst().replicas())if((replica.role()==it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor.Role.LISTENER)==(role==org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER))expected.put(replica.peerId(),replica.address());if(!actual.equals(expected))throw new IOException("Learner actual configuration differs from proposed membership");
        }
        final ReplicatedRangeStore.TopologyLearnerReadiness result;try{result=store.topologyLearnerReadiness(requiredCut);}catch(org.rocksdb.RocksDBException|IllegalStateException invalid){throw new IOException("Learner native readiness unavailable",invalid);}
        if(result.ready() && (result.completedCut().term()!=term || result.configuration().index()!=configuration.getLogEntryIndex()))throw new IOException("Learner tail/configuration does not cover actual division");
        synchronized(admissions){if(generation!=admissionGeneration.get() || schemaConfigurationPending || stamp!=schemaConfigurationStamp || getLifeCycleState()!=LifeCycle.State.RUNNING || division.getInfo().getCurrentTerm()!=term || !java.util.Objects.equals(result.completedCut(),store.applied()))throw new IOException("Learner readiness owner changed");}return result;
    }
    @FunctionalInterface interface RelocationAuthorityReader { RatisTopologyJoinAuthority.View read(RatisTopologyJoinAuthority.Request request)throws IOException; }
    private RelocationAuthorityReader relocationAuthority;
    private ProofQueryExecutor relocationOwnerQueries;
    void relocationOwnerQueries(ProofQueryExecutor executor){requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || relocationOwnerQueries!=null)throw new IllegalStateException("Relocation owner lane fixed before initialization");relocationOwnerQueries=java.util.Objects.requireNonNull(executor);}
    void relocationAuthority(RelocationAuthorityReader reader){requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || relocationAuthority!=null)throw new IllegalStateException("Relocation authority is fixed before initialization");relocationAuthority=java.util.Objects.requireNonNull(reader);}
    RatisRelocationOwnerRpc.Evidence relocationEvidence(RatisRelocationOwnerRpc.Request request)throws Exception{
        requireDataRole();requireHealthy();if(relocationAuthority==null || topologyLocalReplica==null || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Actual relocation owner is not configured");
        var definition=request.definition();var source=definition.parents().getFirst();var target=definition.targets().getFirst();var sourceExpected=request.expected()==RatisRelocationOwnerRpc.Expected.SOURCE || request.expected()==RatisRelocationOwnerRpc.Expected.CAUGHT_UP && source.replicas().stream().anyMatch(r->r.replicaId().equals(request.replica()));var expected=sourceExpected?source:target;
        var local=expected.replicas().stream().filter(r->sameReplicaIdentity(r,topologyLocalReplica)).findFirst().orElseThrow(()->new IOException("Relocation does not name actual local replica"));if(request.replica()!=null && !request.replica().equals(local.replicaId()) || !expected.groupId().equals(store.ownership().owner().groupId()))throw new IOException("Relocation request crossed owner/group");
        var authorityRequest=RatisTopologyJoinAuthority.Request.of(definition.header(),local.replicaId(),sourceExpected?RatisTopologyJoinAuthority.Purpose.SOURCE_CONFIGURATION:RatisTopologyJoinAuthority.Purpose.CONFIGURATION);var authority=relocationAuthority.read(authorityRequest);authority.require(authorityRequest);if(!java.util.Arrays.equals(authority.definition().encode(),definition.encode()))throw new IOException("Relocation definition changed");
        var server=getServer().getNow(null);if(server==null || !server.getId().toString().equals(local.peerId()))throw new IOException("Relocation server identity differs");var division=server.getDivision(getGroupId());var info=division.getInfo();var term=info.getCurrentTerm();boolean leaderRequired=request.replica()==null && (request.expected()==RatisRelocationOwnerRpc.Expected.SOURCE || request.expected()==RatisRelocationOwnerRpc.Expected.TARGET);
        if(division.getStateMachine()!=this || !info.isAlive() || info.getLifeCycleState()!=LifeCycle.State.RUNNING || term<1 || leaderRequired && (!info.isLeader() || !info.isLeaderReady()))throw new IOException("Relocation lacks actual running owner/leader");long generation=admissionGeneration.get();final long stamp;synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Relocation configuration apply pending");stamp=schemaConfigurationStamp;}
        var cut=store.applied();var cfg=store.configuration();var ownership=store.ownership();if(cut==null || (request.expected()==RatisRelocationOwnerRpc.Expected.IMAGE?cut.term()>term:cut.term()!=term) || cfg==null || cfg.term()>cut.term() || cfg.index()>cut.index() || cfg.index()==cut.index() && cfg.term()!=cut.term() || ownership.owner().epoch()!=expected.rangeEpoch() || !ownership.interval().equals(new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(definition.sourceDirectory().domainId(),definition.sourceDirectory().columns(),expected.lowerInclusive(),expected.upperExclusive())) || !java.util.Arrays.equals(store.schemaDigest(),definition.sourceDirectory().schemaDigest()))throw new IOException("Relocation native namespace/configuration/prefix differs");
        var actual=division.getRaftConf();boolean imageOnly=request.expected()==RatisRelocationOwnerRpc.Expected.IMAGE;
        if(imageOnly){try{requireSchemaMembership(source,cfg.data());}catch(IOException notSource){requireSchemaMembership(target,cfg.data());}try{requireActualMembership(actual,source);}catch(IOException notSource){requireActualMembership(actual,target);}}else{requireSchemaMembership(expected,cfg.data());requireActualMembership(actual,expected);if(actual.getLogEntryIndex()!=cfg.index() && !(cfg.term()==0 && cfg.index()==-1 && actual.getLogEntryIndex()<=0))throw new IOException("Relocation actual/native configuration positions differ");}
        var registration=store.topologyLearnerRegistration();if(imageOnly && registration==null)throw new IOException("Relocation image has no receiver-local registration");boolean installed=registration==null?store.admissionOpen():imageOnly?store.topologyLearnerImageInstalled(request.requiredCut(),request.imageHash()):store.topologyLearnerImageInstalled();boolean caught=!imageOnly && installed && (request.requiredCut()==null || RatisRelocationOwnerRpc.covers(cut,request.requiredCut()));if(registration!=null && !imageOnly && caught)caught=store.topologyLearnerReadiness(request.requiredCut()==null?cut:request.requiredCut(),cfg.data()).ready();
        synchronized(admissions){if(generation!=admissionGeneration.get() || schemaConfigurationPending || stamp!=schemaConfigurationStamp || getLifeCycleState()!=LifeCycle.State.RUNNING || division.getInfo().getCurrentTerm()!=term || !java.util.Objects.equals(cut,store.applied()))throw new IOException("Relocation owner/configuration changed during native proof");}
        var evidence=new RatisRelocationOwnerRpc.Evidence(request.digest(),TopologyChange.Owner.of(local),cut,cfg,registration==null?null:registration.digest(),installed,caught);evidence.require(request);return evidence;
    }
    private static boolean sameReplicaIdentity(ReplicaDescriptor left,ReplicaDescriptor right){return left.nodeId().equals(right.nodeId()) && left.nodeIncarnation().equals(right.nodeIncarnation()) && left.replicaId().equals(right.replicaId()) && left.peerId().equals(right.peerId()) && left.address().equals(right.address());}
    static void requireActualMembership(org.apache.ratis.server.RaftConfiguration actual,RangeDescriptor expected)throws IOException{
        for(var role:List.of(RaftPeerRole.FOLLOWER,RaftPeerRole.LISTENER)){if(!actual.getPreviousPeers(role).isEmpty())throw new IOException("Relocation requires stable actual configuration");var peers=new java.util.TreeMap<String,String>();for(var peer:actual.getCurrentPeers(role))peers.put(peer.getId().toString(),peer.getAddress());var wanted=new java.util.TreeMap<String,String>();for(var replica:expected.replicas())if((replica.role()==ReplicaDescriptor.Role.LISTENER)==(role==RaftPeerRole.LISTENER))wanted.put(replica.peerId(),replica.address());if(!peers.equals(wanted))throw new IOException("Relocation actual membership differs");}
    }
    static void requireActualRelocationMembership(org.apache.ratis.server.RaftConfiguration actual,RatisTopologyRelocationRecord record,boolean joint)throws IOException{
        var cfg=RaftConfigurationProto.newBuilder();actual.getCurrentPeers(RaftPeerRole.FOLLOWER).forEach(p->cfg.addPeers(p.getRaftPeerProto()));actual.getCurrentPeers(RaftPeerRole.LISTENER).forEach(p->cfg.addListeners(p.getRaftPeerProto()));actual.getPreviousPeers(RaftPeerRole.FOLLOWER).forEach(p->cfg.addOldPeers(p.getRaftPeerProto()));actual.getPreviousPeers(RaftPeerRole.LISTENER).forEach(p->cfg.addOldListeners(p.getRaftPeerProto()));if(record.requireNativeConfiguration(cfg.build().toByteArray())!=joint)throw new IOException("Actual relocation joint phase differs from native commitment");
    }
    interface RelocationObserver {void persist(RatisTopologyRelocationRecord record,ApplyPosition position)throws IOException;boolean saved(RatisTopologyRelocationRecord record)throws IOException;}
    private RelocationObserver relocationObserver;
    void relocationObserver(RelocationObserver observer){requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || relocationObserver!=null)throw new IllegalStateException("Relocation journal owner fixed before initialization");relocationObserver=java.util.Objects.requireNonNull(observer);}
    private static final class RelocationHold extends RetainedProof {final RatisTopologyRelocationRecord record;final long term,generation,stamp;boolean reading=true,qualified,submitted,invalidated,maybeAppended,released;int owners;RelocationHold(RatisTopologyRelocationRecord record,long term,long generation,long stamp,RatisNodeResourceBudget.Lease graph){super(graph);this.record=record;this.term=term;this.generation=generation;this.stamp=stamp;}}
    private RelocationHold relocationHold;
    private void releaseRelocation(RelocationHold hold){hold.invalidated=true;if(hold.reading || hold.owners>0 || hold.nativeOwners!=0)return;hold.released=true;if(relocationHold==hold){relocationHold=null;topologySlots--;topologyBytes-=hold.record.encode().length;hold.closeGraph();}}
    private void requireRelocationHold(RelocationHold hold,Leadership leader)throws IOException{if(hold==null || relocationHold!=hold || hold.released || hold.invalidated || hold.reading || !hold.qualified || hold.term!=leader.term() || !leader.ready() || hold.generation!=admissionGeneration.get() || hold.stamp!=schemaConfigurationStamp || schemaConfigurationPending || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Relocation record lacks exact current prepared owner");}
    private void prepareRelocation(RatisTopologyRelocationRecord record)throws Exception{
        requireDataRole();requireHealthy();if(relocationObserver==null || relocationAuthority==null)throw new IOException("Relocation durable owner is not configured");var leader=leadership();if(!leader.ready())throw new IOException("Relocation leader unavailable");final RelocationHold hold;
        synchronized(admissions){if(relocationHold!=null){if(!relocationHold.record.equals(record))throw new IOException("Another relocation proposal owns the group");requireRelocationHold(relocationHold,leader);return;}if(topologySlots>=16 || topologyBytes+record.encode().length>16L*1024*1024 || schemaConfigurationPending)throw new IOException("Relocation metadata budget/configuration unavailable");hold=new RelocationHold(record,leader.term(),admissionGeneration.get(),schemaConfigurationStamp,retainedProof(RatisTopologyRelocationRecord.MAX_BYTES,null));relocationHold=hold;topologySlots++;topologyBytes+=record.encode().length;}
        boolean qualified=false;try{if(relocationObserver.saved(record)){synchronized(admissions){hold.reading=false;hold.qualified=true;requireRelocationHold(hold,leadership());qualified=true;}return;}var purpose=record.action()==RatisTopologyRelocationRecord.Action.AUTHORIZE?RatisTopologyJoinAuthority.Purpose.SOURCE_CONFIGURATION:RatisTopologyJoinAuthority.Purpose.PUBLICATION;var request=RatisTopologyJoinAuthority.Request.of(record.definition().header(),topologyLocalReplica.replicaId(),purpose);var view=relocationAuthority.read(request);view.require(request);if(!view.catalogue().equals(record.catalogue()) || !java.util.Arrays.equals(view.definition().encode(),record.definition().encode()) || !RatisRelocationOwnerRpc.covers(view.cut(),record.catalogueCut()))throw new IOException("Relocation catalogue witness differs");var cfg=store.configuration();var cut=store.applied();byte[] expected=record.action()==RatisTopologyRelocationRecord.Action.AUTHORIZE?record.sourceConfiguration():record.targetConfiguration();if(cfg==null || cut==null || cut.term()!=leader.term() || !java.util.Arrays.equals(cfg.data(),expected))throw new IOException("Relocation native configuration changed");requireActualMembership(getServer().getNow(null).getDivision(getGroupId()).getRaftConf(),record.action()==RatisTopologyRelocationRecord.Action.AUTHORIZE?record.definition().parents().getFirst():record.definition().targets().getFirst());if(record.action()==RatisTopologyRelocationRecord.Action.COMPLETE && !java.util.Arrays.equals(view.state().relocation().configurationDigest(),OperatorUpload.sha256(expected)))throw new IOException("Publication committed another configuration");synchronized(admissions){hold.reading=false;hold.qualified=true;requireRelocationHold(hold,leadership());qualified=true;}}finally{synchronized(admissions){hold.reading=false;if(!qualified)releaseRelocation(hold);}}
    }
    private TransactionContext startRelocation(TransactionContext transaction,RatisTopologyRelocationRecord record)throws IOException{
        boolean saved=relocationObserver!=null && relocationObserver.saved(record);var current=leadership();
        synchronized(admissions){
            if(saved && relocationHold==null){
                if(topologySlots>=16 || topologyBytes+record.encode().length>16L*1024*1024)throw new IOException("Saved relocation input budget exhausted");
                var retained=new RelocationHold(record,current.term(),admissionGeneration.get(),schemaConfigurationStamp,retainedProof(RatisTopologyRelocationRecord.MAX_BYTES,null));
                retained.reading=false;retained.qualified=true;relocationHold=retained;topologySlots++;topologyBytes+=record.encode().length;
            }
            var hold=relocationHold;
            if(!saved)requireRelocationHold(hold,current);
            else {if(hold==null || hold.reading || !hold.record.equals(record))throw new IOException("Saved relocation input differs from retained owner");savedAppend(transaction,hold,record,current,admissionGeneration.get());}
            if(!hold.record.equals(record))throw new IOException("Relocation frozen record changed");hold.submitted=true;hold.owners++;transaction.setStateMachineContext(hold);return transaction;
        }
    }
    private ProofQueryExecutor importPreflightQueries;
    void importPreflightQueries(ProofQueryExecutor executor){requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || importPreflightQueries!=null)throw new IllegalStateException("Import preflight lane fixed before initialization");importPreflightQueries=java.util.Objects.requireNonNull(executor);}
    private int qualifiedImportManifestBytes(Object record){var definition=rangeImportApply.definition();if(record instanceof RatisRangeImportRecord proposal)proposal.requireDefinition(definition);else if(!(record instanceof RatisReferencedFile.Request))throw new IllegalArgumentException("Unknown import preparation subject");var manifest=definition.manifest(store.ownership().owner().groupId());RatisRangeImportRecord.requirePolicy(manifest);return manifest.limits().maximumEncodedBytes();}
    private void refreshPublishedReplicaRole(RangeDescriptor descriptor){
        if(topologyLocalReplica==null)return;var local=descriptor.replicas().stream().filter(replica->sameReplicaIdentity(replica,topologyLocalReplica)).findFirst().orElse(null);if(local==null)return;topologyLocalReplica=local;if(schemaLocalReplica!=null){schemaLocalReplica=local;if(schemaReplicas!=null)schemaReplicas.rebindReplica(store.ownership().owner().groupId(),store,local);}
    }
    private byte[] applyRelocation(ApplyPosition position,RatisTopologyRelocationRecord record)throws Exception{
        if(relocationObserver==null)throw new IOException("Committed relocation lacks durable node owner");relocationObserver.persist(record,position);byte[] result;
        if(record.action()==RatisTopologyRelocationRecord.Action.COMPLETE && store.topologyLearnerRegistration()!=null)result=store.applyTopologyRelocationCompletion(position,record.definition(),record.targetConfiguration(),record.catalogue(),record.catalogueCut()).encode();else{store.applyNoop(position,null);result=new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode();}
        if(record.action()==RatisTopologyRelocationRecord.Action.COMPLETE){var directory=record.definition().targetDirectory();var descriptor=record.definition().targets().getFirst();if((effectiveSessionDirectory==null || directory.generation()>=effectiveSessionDirectory.generation()) && (topologyDescriptor==null || descriptor.configurationVersion()>=topologyDescriptor.configurationVersion())){schemaDescriptor=descriptor;topologyDescriptor=descriptor;refreshPublishedReplicaRole(descriptor);refreshSessionAdmission(directory);if(store.admissionOpen()){if(enableSessionControls)store.enableSessionControls();store.enableScopedHistory(it.cavallium.rockserver.core.common.distributed.RangeMutationHistory.Limits.defaults());}}}
        synchronized(admissions){if(relocationHold!=null && relocationHold.record.equals(record))releaseRelocation(relocationHold);}return result;
    }
    private java.util.function.LongFunction<org.apache.ratis.server.leader.MigrationLogStateMachine.Lease> migrationTail;
    void migrationTail(java.util.function.LongFunction<org.apache.ratis.server.leader.MigrationLogStateMachine.Lease> admission){if(getLifeCycleState()!=LifeCycle.State.NEW || migrationTail!=null)throw new IllegalStateException("Migration tail fixed before initialization");migrationTail=java.util.Objects.requireNonNull(admission);}
    @Override public org.apache.ratis.server.leader.MigrationLogStateMachine.Lease reserveListenerTail(long bytes)throws IOException{return migrationTail==null?null:migrationTail.apply(bytes);}
    private final java.util.concurrent.ConcurrentHashMap<java.util.UUID,ReplicatedRangeStore.SnapshotPin> failedSourceCapturePins=new java.util.concurrent.ConcurrentHashMap<>();
    void drainSourceCapture(java.util.UUID transfer)throws Exception{var pin=failedSourceCapturePins.get(transfer);if(pin!=null){pin.close();failedSourceCapturePins.remove(transfer,pin);}}
    private void closeSourceCapture(java.util.UUID transfer,ReplicatedRangeStore.SnapshotPin pin)throws Exception{try{pin.close();failedSourceCapturePins.remove(transfer,pin);}catch(Exception failed){failedSourceCapturePins.put(transfer,pin);throw failed;}}
    Path sourceSnapshotDirectory(){return snapshots.getSnapshotFile(0,0).toPath().getParent();}
    record SourceSnapshotEvidence(ReplicaDescriptor owner,ApplyPosition completedCut,ReplicatedRangeStore.Configuration configuration) {}
    SourceSnapshotEvidence sourceSnapshotEvidence(RatisTopologySourceSnapshotRpc.Request request,TopologyChange.Definition definition,RangeDirectory qualifiedDirectory)throws Exception {
        return sourceSnapshotEvidence(request,definition,qualifiedDirectory,false);
    }
    SourceSnapshotEvidence sourceSnapshotEvidence(RatisTopologySourceSnapshotRpc.Request request,TopologyChange.Definition definition,RangeDirectory qualifiedDirectory,boolean continuity)throws Exception {
        return sourceSnapshotEvidence(request,definition,qualifiedDirectory,continuity,false);
    }
    SourceSnapshotEvidence sourceSnapshotEvidence(RatisTopologySourceSnapshotRpc.Request request,TopologyChange.Definition definition,RangeDirectory qualifiedDirectory,boolean continuity,boolean retained)throws Exception {
        java.util.Objects.requireNonNull(qualifiedDirectory);
        if(request.relocation()) {
            if(definition==null)throw new IOException("Relocation export requires immutable definition");
            request.header().requireDefinition(definition);
            if(!java.util.Arrays.equals(qualifiedDirectory.payloadDigest(),definition.sourceDirectory().payloadDigest()))throw new IOException("Relocation export directory differs");
            var evidence=relocationEvidence(new RatisRelocationOwnerRpc.Request(definition,null,RatisRelocationOwnerRpc.Expected.SOURCE,null,null));
            var owner=definition.parents().getFirst().replicas().stream().filter(replica->replica.replicaId().equals(evidence.owner().replicaId())).findFirst().orElseThrow();
            return new SourceSnapshotEvidence(owner,evidence.completedCut(),evidence.configuration());
        }
        requireDataRole();requireHealthy();
        if(definition!=null || request.sourceDirectory()==null || (!continuity || request.metadataOnly()) && !java.util.Arrays.equals(request.sourceDirectory().payloadDigest(),qualifiedDirectory.payloadDigest()) || topologyLocalReplica==null || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Recovery export lacks exact qualified source directory");
        var expected=continuity && !request.metadataOnly()?RatisSourceHeadRpc.requireContinuity(request,qualifiedDirectory):request.range();var local=expected.replicas().stream().filter(replica->sameReplicaIdentity(replica,topologyLocalReplica)).findFirst().orElseThrow(()->new IOException("Recovery export has no configured local source"));
        if(local.role()!=ReplicaDescriptor.Role.VOTER || (!continuity || request.metadataOnly()) && !request.sourceOwner(local) || !request.sourceGroup().equals(store.ownership().owner().groupId()))throw new IOException("Recovery export crossed actual source group/owner");
        var server=getServer().getNow(null);if(server==null || !server.getId().toString().equals(local.peerId()))throw new IOException("Recovery export server identity differs");
        var division=server.getDivision(getGroupId());var info=division.getInfo();long term=info.getCurrentTerm();
        if(division.getStateMachine()!=this || !info.isAlive() || info.getLifeCycleState()!=LifeCycle.State.RUNNING || term<1 || !info.isLeader() || !info.isLeaderReady())throw new IOException("Recovery export requires actual live ready leader");
        long generation=admissionGeneration.get();final long stamp;synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Recovery export configuration apply pending");stamp=schemaConfigurationStamp;}
        var cut=store.applied();var cfg=store.configuration();var ownership=store.ownership();
        if(cut==null || cut.term()!=term || cfg==null || cfg.term()>cut.term() || cfg.index()>cut.index() || cfg.index()==cut.index() && cfg.term()!=cut.term() || ownership.owner().epoch()!=expected.rangeEpoch()
                || !ownership.interval().equals(new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(qualifiedDirectory.domainId(),qualifiedDirectory.columns(),expected.lowerInclusive(),expected.upperExclusive())) || !java.util.Arrays.equals(store.schemaDigest(),qualifiedDirectory.schemaDigest())
                || store.pendingInstallation() || store.importPending() || store.topologyBirthPending() || store.topologyRecoveryOnly() || store.metadataRecoveryPending())throw new IOException("Recovery export native geometry/schema/completed prefix differs");
        if(request.metadataOnly()) {
            var freeze=store.topologyFreeze();if(freeze==null || !java.util.Arrays.equals(freeze.header().encode(),request.header().encode()))throw new IOException("Metadata export lacks original sealed source");
            freeze.requireParent(expected.groupId(),expected.rangeEpoch(),qualifiedDirectory.domainId(),qualifiedDirectory.schemaDigest(),expected.lowerInclusive(),expected.upperExclusive());freeze.requireCut(cut);
            var metadata=store.metadataOutcomeState();if(!java.util.Objects.equals(metadata.applied(),cut))throw new IOException("Metadata export is not ready at native cut");
        } else {
            var fence=store.schemaFence();boolean schemaFenced=retained && fence!=null && fence.clusterId().equals(qualifiedDirectory.clusterId()) && fence.databaseId().equals(qualifiedDirectory.databaseId()) && fence.domainId().equals(qualifiedDirectory.domainId()) && fence.participant().groupId().equals(expected.groupId()) && fence.participant().rangeEpoch()==expected.rangeEpoch() && java.util.Arrays.equals(fence.sourceDirectoryDigest(),qualifiedDirectory.payloadDigest());
            if(schemaFenced)fence.requireCut(cut);
            if(store.topologyRetired() || store.topologyFreeze()!=null || (!store.admissionOpen() || fence!=null) && !schemaFenced)throw new IOException("Current leaf export lacks suitable native source authority");
        }
        requireSchemaMembership(expected,cfg.data());var actual=division.getRaftConf();requireActualMembership(actual,expected);
        if(actual.getLogEntryIndex()!=cfg.index() && !(cfg.term()==0 && cfg.index()==-1 && actual.getLogEntryIndex()<=0))throw new IOException("Recovery export actual/native configuration positions differ");
        synchronized(admissions){if(generation!=admissionGeneration.get() || schemaConfigurationPending || stamp!=schemaConfigurationStamp || getLifeCycleState()!=LifeCycle.State.RUNNING || info.getCurrentTerm()!=term || !info.isAlive() || !info.isLeader() || !info.isLeaderReady() || !java.util.Objects.equals(cut,store.applied()))throw new IOException("Recovery export owner/configuration changed during proof");}
        return new SourceSnapshotEvidence(local,cut,cfg);
    }
    RatisTopologySourceSnapshotService.Lease captureSourceSnapshot(RatisTopologySourceSnapshotRpc.Request request,TopologyChange.Definition definition,RangeDirectory qualifiedDirectory,DistributedSchema qualifiedSchema,RatisTopologySourceSnapshotArtifacts artifacts,NativeRangeImage.IoAdmission imageAdmission)throws Exception{
        imageAdmission.beforeBytes(1);qualifiedSchema.requireMatches(qualifiedDirectory);
        var saved=artifacts.lookupAdmitted(request,imageAdmission);var evidence=sourceSnapshotEvidence(request,definition,qualifiedDirectory,saved!=null,saved!=null);var division=getServer().getNow(null).getDivision(getGroupId());
        if(saved!=null){var prepared=saved.prepared();var after=sourceSnapshotEvidence(request,definition,qualifiedDirectory,true,true);if(!RatisRelocationOwnerRpc.covers(after.completedCut(),prepared.image().position()) || !prepared.sourceOwner().equals(after.owner()) || !after.owner().equals(evidence.owner()))throw new IOException("Cold source image lacks its original actual owner/prefix");return new RatisTopologySourceSnapshotService.Lease(prepared,division,saved.snapshot(),()->{});}
        ReplicatedRangeStore.SnapshotPin pin=request.metadataOnly()?store.captureMetadataForRecovery():request.relocation()?store.captureForRecovery():store.capture();boolean returned=false;
        try {
            var position=pin.position();if(position==null || position.term()<1 || !java.util.Arrays.equals(qualifiedSchema.digest(),pin.schemaDigest()))throw new IOException("Source image lacks qualified committed schema/prefix");
            var rows=new java.util.HashMap<String,byte[]>();var metadataFreeze=new NativeTopologyFreeze[1];pin.visitState((key,value)->{if(it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerSchema.row(key))rows.put(java.util.HexFormat.of().formatHex(key),value.clone());if(request.metadataOnly() && RatisTopologySourceSnapshotRpc.metadataFreezeKey(pin.groupId(),key)){if(metadataFreeze[0]!=null)throw new IllegalArgumentException("Duplicate snapshot metadata FREEZE");metadataFreeze[0]=NativeTopologyFreeze.decode(value);}});
            RatisTopologySourceSnapshotRpc.requireMetadataFreeze(request,metadataFreeze[0],position,pin.interval().lowerInclusive(),pin.interval().upperExclusive());
            if(request.relocation())artifacts.capture(request,definition,pin,qualifiedSchema,evidence.owner());else artifacts.capture(request,pin,qualifiedSchema,evidence.owner());
            var file=artifacts.originalImagePath(request,position);NativeRangeImage.Manifest image;
            if(!Files.exists(file))image=request.metadataOnly()?NativeRangeImage.writeMetadataAdmitted(file,pin,pin.interval(),limits,imageAdmission):NativeRangeImage.writeAdmitted(file,pin,pin.interval(),limits,imageAdmission);
            else image=request.metadataOnly()?NativeRangeImage.visitMetadataAdmitted(file,limits,(section,column,key,value)->{},imageAdmission):NativeRangeImage.inspectAdmitted(file,limits,imageAdmission);
            if(!position.equals(image.position()))throw new IOException("Published source snapshot cut changed");
            var md5=org.apache.ratis.io.MD5Hash.newInstance(NativeRangeImage.md5Admitted(file,imageAdmission));var snapshot=new SingleFileSnapshotInfo(new FileInfo(file,md5),TermIndex.valueOf(position.term(),position.index()));
            var prepared=new RatisTopologySourceSnapshotRpc.Prepared(request,image,qualifiedSchema,evidence.owner(),pin.configuration(),rows,metadataFreeze[0]);var retained=artifacts.retainAdmitted(prepared,snapshot,imageAdmission);
            var after=sourceSnapshotEvidence(request,definition,qualifiedDirectory);if(!after.owner().equals(evidence.owner()) || !RatisRelocationOwnerRpc.covers(after.completedCut(),position))throw new IOException("Source lost completed image owner");
            var lease=new RatisTopologySourceSnapshotService.Lease(prepared,division,retained.snapshot(),()->closeSourceCapture(request.transferId(),pin));returned=true;return lease;
        }finally{if(!returned)closeSourceCapture(request.transferId(),pin);}
    }
    RatisRecoveryImageRpc.Evidence recoveryImageEvidence(RatisRecoveryImageRpc.Request request)throws Exception {
        requireDataRole();requireHealthy();var scope=request.source();
        if(topologyLocalReplica==null || !sameReplicaIdentity(scope.receiver(),topologyLocalReplica) || !scope.sourceGroup().equals(store.ownership().owner().groupId()) || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Image receipt does not address actual local recovery owner");
        var server=getServer().getNow(null);if(server==null || !server.getId().toString().equals(scope.receiver().peerId()))throw new IOException("Image receipt server identity differs");
        var division=server.getDivision(getGroupId());var info=division.getInfo();
        if(division.getStateMachine()!=this || !info.isAlive() || info.getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Image receipt requires initialized actual local division");
        long generation=admissionGeneration.get();final long stamp;synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Image receipt configuration apply pending");stamp=schemaConfigurationStamp;}
        byte[] registrationDigest=null;boolean installed=false;
        if(scope.metadataOnly()) {
            var registration=store.metadataRecoveryRegistration();
            if(registration!=null) {
                if(!registration.transferId().equals(scope.transferId()) || !registration.catalogue().equals(scope.catalogue()) || !registration.receiver().equals(scope.receiver()) || !java.util.Arrays.equals(registration.sourceDirectory().payloadDigest(),scope.sourceDirectory().payloadDigest())
                        || !java.util.Arrays.equals(registration.freeze().header().encode(),scope.header().encode()) || !registration.imageCut().equals(request.imageCut()) || !java.util.Arrays.equals(registration.imageHash(),request.imageHash()))throw new IOException("Metadata image receipt changed original registration");
                registrationDigest=registration.digest();installed=store.metadataRecoveryImageInstalled();
            }
        } else {
            var registration=store.currentRangeRecoveryRegistration();
            if(registration!=null) {
                if(!registration.transferId().equals(scope.transferId()) || !registration.catalogue().equals(scope.catalogue()) || !registration.receiver().equals(scope.receiver()) || !registration.groupId().equals(scope.sourceGroup()) || !java.util.Arrays.equals(registration.directory().payloadDigest(),scope.sourceDirectory().payloadDigest())
                        || !java.util.Arrays.equals(registration.schema().digest(),scope.schemaDigest()) || !registration.imageCut().equals(request.imageCut()) || !java.util.Arrays.equals(registration.imageHash(),request.imageHash()))throw new IOException("Current leaf image receipt changed original registration");
                registrationDigest=registration.digest();installed=store.currentRangeRecoveryImageInstalled();
            }
        }
        installed=installed && !store.pendingInstallation();var cut=installed?store.applied():null;var cfg=installed?store.configuration():null;
        if(cut==null || cfg==null){if(installed)throw new IOException("Completed image receipt has no native cut/configuration");cut=null;cfg=null;}
        if(installed){requireSchemaMembership(scope.range(),cfg.data());var actual=division.getRaftConf();requireActualMembership(actual,scope.range());if(actual.getLogEntryIndex()!=cfg.index() && !(cfg.term()==0 && cfg.index()==-1 && actual.getLogEntryIndex()<=0))throw new IOException("Image receipt actual/native configuration differs");}
        synchronized(admissions){if(generation!=admissionGeneration.get() || schemaConfigurationPending || stamp!=schemaConfigurationStamp || !java.util.Objects.equals(cut,store.applied()) && cut!=null || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Image receipt local owner changed during proof");}
        var result=new RatisRecoveryImageRpc.Evidence(request.digest(),TopologyChange.Owner.of(scope.receiver()),cut,cfg,registrationDigest,installed);result.require(request);return result;
    }
    @FunctionalInterface interface LearnerInstaller {it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerRegistration install(RatisTopologyJoinAuthority.Request request)throws IOException;}
    @FunctionalInterface interface LearnerRecoveryInstaller {RatisTopologySourceSnapshotRpc.Prepared prepare(RatisTopologySourceSnapshotRpc.Request request)throws Exception;}
    private LearnerRecoveryInstaller learnerRecoveryInstaller;
    void learnerRecoveryInstaller(LearnerRecoveryInstaller installer){if(getLifeCycleState()!=LifeCycle.State.NEW || learnerRecoveryInstaller!=null)throw new IllegalStateException("Learner recovery owner fixed before initialization");learnerRecoveryInstaller=java.util.Objects.requireNonNull(installer);}
    private LearnerInstaller learnerInstaller;
    void learnerInstaller(ProofQueryExecutor executor,LearnerInstaller installer){if(getLifeCycleState()!=LifeCycle.State.NEW || learnerInstaller!=null)throw new IllegalStateException("Learner bootstrap fixed before initialization");if(topologyImageQueries!=null && topologyImageQueries!=executor)throw new IllegalStateException("Conflicting topology image dispatcher");topologyImageQueries=java.util.Objects.requireNonNull(executor);learnerInstaller=java.util.Objects.requireNonNull(installer);}
    private ProofQueryExecutor topologyImageQueries;private RatisTopologySourceSnapshotService sourceSnapshots;
    void sourceSnapshots(ProofQueryExecutor executor,RatisTopologySourceSnapshotService source){if(getLifeCycleState()!=LifeCycle.State.NEW || sourceSnapshots!=null || topologyImageQueries!=null && topologyImageQueries!=executor)throw new IllegalStateException("Topology image resources fixed before initialization");topologyImageQueries=java.util.Objects.requireNonNull(executor);sourceSnapshots=java.util.Objects.requireNonNull(source);}
    static void requireSchemaMembership(it.cavallium.rockserver.core.common.distributed.RangeDescriptor descriptor,byte[] configuration) throws IOException {
        var nativeConfiguration=RaftConfigurationProto.parseFrom(configuration);
        var actual=new java.util.HashMap<String,String>();
        if (nativeConfiguration.getOldPeersCount()!=0 || nativeConfiguration.getOldListenersCount()!=0) throw new IOException("Schema replica waits for stable native membership");
        for (var peer:nativeConfiguration.getPeersList()) if (actual.put(peer.getId().toStringUtf8(),peer.getAddress()+"|VOTER")!=null) throw new IOException("Invalid native voter identity");
        for (var peer:nativeConfiguration.getListenersList()) if (actual.put(peer.getId().toStringUtf8(),peer.getAddress()+"|LISTENER")!=null) throw new IOException("Invalid native listener identity");
        var expected=new java.util.HashMap<String,String>();
        for (var peer:descriptor.replicas()) if (expected.put(peer.peerId(),peer.address()+"|"+peer.role())!=null) throw new IOException("Duplicate frozen replica identity");
        if (!actual.equals(expected)) throw new IOException("Schema replica native peer address/role configuration changed");
    }
    List<ReplicatedRangeStore.ActivatedSchema> schemaRoutingHistory() throws org.rocksdb.RocksDBException {
        requireDataRole(); return store.activatedSchemas();
    }
    void schemaActivated(java.util.function.BiConsumer<SchemaChange.Definition,NativeSchemaActivationProof> observer) {
        requireDataRole();
        if (getLifeCycleState()!=LifeCycle.State.NEW || schemaActivationObserver!=null) throw new IllegalStateException("Schema observer is fixed before initialization");
        schemaActivationObserver=java.util.Objects.requireNonNull(observer);
    }
    Throwable schemaActivationObserverFailure() { return schemaActivationObserverFailure; }
    private volatile RangeDirectory effectiveSessionDirectory;
    private void refreshSessionAdmission(RangeDirectory directory) {
        if(effectiveSessionDirectory!=null && directory.generation()<effectiveSessionDirectory.generation())return;effectiveSessionDirectory=directory;
        if (sessionEpochs==null) return;
        UUID group=store.ownership().owner().groupId();
        var floor=sessionEpochs.floorAdmission(directory,group); var control=sessionEpochs.sessionControlAdmission(directory,group);
        var installed=store.schemaInstallation();
        var sourceControl=installed==null ? null : sessionEpochs.sessionControlAdmission(installed.definition().sourceDirectory(),group);
        epochFloorAdmission=command -> {
            if (command.sessionControl()==null) { floor.accept(command); return; }
            if (sourceControl!=null && store.pendingSchemaControl(command)) sourceControl.accept(command);
            else control.accept(command);
        };
    }
    private void publishSchemaActivation(AppliedRecord applied) {
        if(applied!=null && applied.topologyActivation()!=null && topologyActivationObserver!=null) {
            try { topologyActivationObserver.accept(applied.topologyDefinition(),applied.topologyActivation()); }
            catch(Throwable failure) { topologyActivationObserverFailure=failure; }
        }
        if (applied==null || applied.activation()==null || schemaActivationObserver==null) return;
        try { schemaActivationObserver.accept(applied.definition(),applied.activation()); }
        catch (Throwable failure) { schemaActivationObserverFailure=failure; }
    }
    void schemaStorage(DistributedStoragePool pool) {
        requireDataRole();
        if (getLifeCycleState()!=LifeCycle.State.NEW || schemaStorage!=null) throw new IllegalStateException("Schema storage is fixed before initialization");
        schemaStorage=java.util.Objects.requireNonNull(pool);
    }
    private void requireSchemaGroup(UUID group) {
        requireDataRole();
        if (!group.equals(store.ownership().owner().groupId())) throw new IllegalArgumentException("Schema operation addresses another DATA group");
    }
    void schemaAuthority(SchemaAuthorityReader reader) {
        requireDataRole();
        if (getLifeCycleState()!=LifeCycle.State.NEW || schemaAuthority!=null) throw new IllegalStateException("Schema authority is fixed before initialization");
        schemaAuthority=java.util.Objects.requireNonNull(reader);
    }
    void schemaAuthority(SchemaAuthorityReader reader,RangeDirectory directory) {
        schemaAuthority(reader);
        var descriptor=directory.ranges().stream().filter(r -> r.groupId().equals(store.ownership().owner().groupId())).findFirst().orElseThrow();
        schemaDescriptor=descriptor;
    }
    void schemaCatalogue(UUID cluster,UUID database,UUID column) {
        if (role!=RatisNodePlan.Role.CATALOGUE || getLifeCycleState()!=LifeCycle.State.NEW || schemaColumn!=null) throw new IllegalStateException("Schema catalogue context is fixed before initialization");
        schemaCluster=java.util.Objects.requireNonNull(cluster); schemaDatabase=java.util.Objects.requireNonNull(database); schemaColumn=java.util.Objects.requireNonNull(column);
    }
    void schemaAuthorityQueries(ProofQueryExecutor executor) {
        if (role!=RatisNodePlan.Role.CATALOGUE || getLifeCycleState()!=LifeCycle.State.NEW || schemaAuthorityQueries!=null) throw new IllegalStateException("Catalogue authority dispatcher is fixed before initialization");
        schemaAuthorityQueries=java.util.Objects.requireNonNull(executor);
    }
    void schemaFencingReplicaClient(RatisSchemaReplicaClient client) {
        if (role!=RatisNodePlan.Role.CATALOGUE || getLifeCycleState()!=LifeCycle.State.NEW || schemaFencingReplicas!=null) throw new IllegalStateException("Schema fencing client is fixed before initialization");
        schemaFencingReplicas=java.util.Objects.requireNonNull(client);
    }
    private record SchemaAuthorityCut(RatisSchemaAuthority.View view,SchemaChange.Definition definition) {}
    private SchemaAuthorityCut readCatalogueAuthorityCut(RatisSchemaAuthority.Request request) throws Exception {
        if (role!=RatisNodePlan.Role.CATALOGUE || schemaColumn==null || !request.clusterId().equals(schemaCluster) || !request.databaseId().equals(schemaDatabase)) throw new IOException("Schema authority requires configured catalogue namespace");
        var owner=leadership(); long generation=admissionGeneration.get(); var cut=store.applied();
        if (!owner.ready() || cut==null || cut.term()!=owner.term() || !store.admissionOpen()) throw new IOException("Catalogue authority lacks completed leader prefix");
        var points=new ArrayList<ReplicatedRangeStore.PhysicalPoint>();
        points.add(new ReplicatedRangeStore.PhysicalPoint(schemaColumn,ByteBuffer.allocate(16).putLong(request.domainId().getMostSignificantBits()).putLong(request.domainId().getLeastSignificantBits()).array()));
        var expected=request.expectedHeader();
        points.add(new ReplicatedRangeStore.PhysicalPoint(schemaColumn,request.outcome() ? RatisSchemaAdministration.outcomeKey(expected.definitionDigest()) : RatisSchemaAdministration.headKey(request.domainId())));
        if (expected!=null) points.add(new ReplicatedRangeStore.PhysicalPoint(schemaColumn,RatisSchemaAdministration.definitionKey(expected.definitionDigest())));
        if (request.admissionDigest()!=null) points.add(new ReplicatedRangeStore.PhysicalPoint(schemaColumn,RatisSchemaAdministration.admissionKey(request.admissionDigest())));
        var read=store.readSchemaAuthorityPoints(points); var values=read.values();
        var directory=values.get(0)==null ? null : RangeDirectoryCodec.decode(values.get(0));
        var head=values.get(1)==null ? null : RatisSchemaAdministration.Head.decode(values.get(1));
        SchemaChange.Definition definition=null; byte[] admissionsBytes=null;
        if (expected!=null) {
            if (values.get(2)==null) throw new IOException("Original committed schema definition missing");
            definition=SchemaChange.Definition.decode(values.get(2));
            if (!Arrays.equals(definition.header().encode(),expected.encode()) || head==null || !Arrays.equals(head.status().header().encode(),expected.encode())) throw new IOException("Original schema authority identity changed");
        }
        if (request.admissionDigest()!=null) {
            admissionsBytes=values.get(3);
            if (admissionsBytes==null || !Arrays.equals(OperatorUpload.sha256(admissionsBytes),request.admissionDigest())) throw new IOException("Original committed schema admissions missing");
            SchemaAdmissionManifest.decode(admissionsBytes,definition);
            if (head.status().phase()!=SchemaChange.Phase.PREPARED && !Arrays.equals(head.status().admissionDigest(),request.admissionDigest())) throw new IOException("Schema HEAD admission commitment changed");
        }
        var current=leadership();
        if (!current.ready() || current.term()!=owner.term() || read.position().term()!=current.term() || generation!=admissionGeneration.get() || schemaConfigurationPending) throw new IOException("Catalogue authority lost barrier owner");
        return new SchemaAuthorityCut(new RatisSchemaAuthority.View(request,store.ownership().owner().groupId(),read.position(),directory,head,admissionsBytes),definition);
    }
    private static boolean coversSchemaPosition(ApplyPosition current,ApplyPosition frozen) {
        return current.index()>=frozen.index() && current.term()>=frozen.term() && (current.index()!=frozen.index() || current.term()==frozen.term());
    }
    private static boolean matchesFencing(SchemaHold hold,DistributedCommand command) {
        return hold!=null && hold.record instanceof DistributedCommand original && Arrays.equals(original.payloadDigest(),command.payloadDigest());
    }
    private void requireFencingOwner(SchemaHold hold,DistributedCommand command,Leadership owner,long generation) throws IOException {
        if (schemaHold!=hold || !matchesFencing(hold,command) || hold.reading || !hold.qualified || hold.invalidated || hold.released
                || !owner.ready() || hold.term!=owner.term() || hold.generation!=generation || generation!=admissionGeneration.get()
                || schemaConfigurationPending || hold.configurationStamp!=schemaConfigurationStamp || !proposalFootprints.drained(hold.lease)) throw new IOException("FENCING lacks drained current-owner all-replica preparation");
    }
    private void checkCatalogueOwner() throws IOException {
        requireHealthy(); var owner=leadership(); var applied=store.applied();
        if (role!=RatisNodePlan.Role.CATALOGUE || getLifeCycleState()!=LifeCycle.State.RUNNING || !owner.ready() || applied==null || applied.term()!=owner.term() || !store.admissionOpen()) throw new IOException("Catalogue leader changed or is not ready");
    }
    private void prepareSchemaFencing(DistributedCommand command) throws Exception {
        if (role!=RatisNodePlan.Role.CATALOGUE || schemaColumn==null || schemaFencingReplicas==null || !RatisSchemaAdministration.isFencing(command)) throw new IOException("Schema fencing preparation is not configured");
        checkCatalogueOwner(); commandValidator.accept(command); store.validate(command);
        if (!store.estimateReceiptRetention(command).newReceipt()) return;
        var next=RatisSchemaAdministration.Head.decode(command.mutations().getFirst().value()); var header=next.status().header();
        var owner=leadership(); long generation=admissionGeneration.get(); final SchemaHold hold;
        synchronized (admissions) {
            reconcileOlderProposalTerms(owner.term());
            if (schemaConfigurationPending) throw new IOException("FENCING waits for queued configuration apply");
            if (schemaHold!=null) { requireFencingOwner(schemaHold,command,owner,generation); return; }
            if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16) throw new IOException("Protected schema proposal budget exhausted");
            var lease=proposalFootprints.reserveGate(proposalFootprints.prepare(store.commandAccessScope(command)),owner.term(),generation);
            if (lease==null) throw new IOException("Conflicting catalogue gate owns FENCING");
            hold=new SchemaHold(command,owner.term(),generation,schemaConfigurationStamp,lease,retainedProof(schemaRetainedBound(command),lease)); schemaHold=hold; schemaSlots++;
        }
        boolean qualified=false;
        try {
            var cut=readCatalogueAuthorityCut(new RatisSchemaAuthority.Request(header.clusterId(),header.databaseId(),header.domainId(),header,next.status().admissionDigest(),false));
            var view=cut.view(); var definition=cut.definition();
            if (view.head().status().phase()!=SchemaChange.Phase.PREPARED || !Arrays.equals(view.head().encode(),command.predicates().getFirst().expectedValue())
                    || view.directory()==null || !Arrays.equals(RangeDirectoryCodec.encode(view.directory()),command.predicates().get(1).expectedValue())) throw new IOException("FENCING exact HEAD/source directory CAS changed");
            var manifest=SchemaAdmissionManifest.decode(view.admissions(),definition);
            // Sequential fresh direct owner calls keep the expanded payload bounded and never hold Raft/native/admission monitors.
            for (var admission:manifest.replicas()) {
                var binding=manifest.binding(admission.groupId());
                var target=new RatisSchemaReplicaRpc.Target(new RatisSchemaReplicaRpc.Binding(definition,binding.groupId(),binding.changedPhysicalName(),binding.merge()),manifest.owner(admission.groupId(),admission.replicaId()));
                var request=RatisSchemaReplicaClient.capacity(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE,target);
                var reply=schemaFencingReplicas.execute(request);
                if (!reply.target().equals(target) || reply.kind()!=request.kind() || !Arrays.equals(reply.requestDigest(),request.digest())
                        || !reply.capacityActive() || !admission.reservation().equals(reply.capacity())) throw new IOException("Frozen voter/listener lacks exact forced ACTIVE capacity admission");
            }
            checkCatalogueOwner(); var current=leadership();
            synchronized (admissions) {
                if (schemaHold!=hold || hold.invalidated || hold.released || generation!=admissionGeneration.get() || current.term()!=hold.term || schemaConfigurationPending || hold.configurationStamp!=schemaConfigurationStamp) throw new IOException("FENCING preparation lost authority owner");
                hold.qualified=true; qualified=true;
                if (!proposalFootprints.drained(hold.lease)) throw new IOException("FENCING metadata predecessors have not drained");
            }
        } finally { synchronized (admissions) { hold.reading=false; if (!qualified || hold.invalidated || hold.released) releaseSchemaHold(hold); } }
    }
    private void cancelSchemaFencing(DistributedCommand command) {
        synchronized (admissions) { if (matchesFencing(schemaHold,command) && !schemaHold.submitted) { schemaHold.invalidated=true; releaseSchemaHold(schemaHold); } }
    }
    private void requireSchemaRecord(RatisSchemaFenceRecord record) {
        requireSchemaGroup(record.groupId());
        if (schemaReplicas!=null && record.admissions()==null) throw new IllegalArgumentException("Configured DATA FENCE requires immutable admissions");
    }
    private NativeSchemaFence schemaProof(RatisSchemaFenceRecord record) {
        requireSchemaRecord(record); var proof=store.lookupSchemaFence(record.header());
        if (proof!=null) {
            proof.requireHeader(record.header()); proof.requireAdmissionDigest(record.admissionDigest());
            if (!proof.participant().groupId().equals(record.groupId())) throw new IllegalArgumentException("Schema proof group changed");
            synchronized (admissions) { if (schemaHold!=null && schemaHold.record.equals(record)) releaseSchemaHold(schemaHold); }
        }
        return proof;
    }
    // A pending authority lookup keeps its bounded input charged until the lookup exits.
    private void releaseSchemaHold(SchemaHold hold) {
        hold.released=true; if (hold.lease!=null) proposalFootprints.release(hold.lease);
        if (hold.reading || hold.owners!=0 || hold.nativeOwners!=0 || hold.cleaning) return;
        var plan=hold.installPlan; var capture=hold.captureGate;
        if (plan==null && capture==null) {
            if (schemaHold==hold) { schemaHold=null; schemaSlots--;hold.closeGraph(); }
            hold.cleanup.complete(null); return;
        }
        hold.cleaning=true; hold.installPlan=null; hold.captureGate=null;
        // Enqueue only: pool-owned actual cleanup never acquires native mutation while Raft monitors are held.
        schemaStorage.submitSchemaCleanup(plan,capture).whenComplete((unused,failure) -> {
            synchronized (admissions) {
                if (failure==null) { hold.cleaning=false;if (schemaHold==hold && !hold.reading && hold.owners==0 && hold.nativeOwners==0) { schemaHold=null; schemaSlots--;hold.closeGraph(); } }
                else storageFailure=failure;
            }
            if (failure==null) hold.cleanup.complete(null); else hold.cleanup.completeExceptionally(failure);
        });
    }
    CompletableFuture<Void> schemaCleanup() {
        synchronized (admissions) { return schemaHold!=null && schemaHold.released ? schemaHold.cleanup.copy() : CompletableFuture.completedFuture(null); }
    }
    private void requireSchemaSource(RatisSchemaFenceRecord record,RatisSchemaAuthority.View view) throws Exception {
        var header=record.header(); var directory=view.directory(); var head=view.head();
        if (!sameSchemaNamespace(view.request(),header) || directory==null || head==null
                || !Arrays.equals(head.status().header().encode(),header.encode())
                || head.status().phase()!=SchemaChange.Phase.FENCING && head.status().phase()!=SchemaChange.Phase.FENCED && head.status().phase()!=SchemaChange.Phase.INSTALLED
                || !Arrays.equals(directory.payloadDigest(),header.sourceDirectoryDigest())) throw new IllegalArgumentException("Schema FENCE lacks exact irreversible source authority");
        var lineage=directory.ranges().stream().map(r -> new SchemaChange.Participant(r.rangeId(),r.groupId(),r.rangeEpoch(),r.configurationVersion())).sorted(java.util.Comparator.comparing(SchemaChange.Participant::groupId,(UUID first,UUID second) -> {
            int high=Long.compareUnsigned(first.getMostSignificantBits(),second.getMostSignificantBits());
            return high!=0 ? high : Long.compareUnsigned(first.getLeastSignificantBits(),second.getLeastSignificantBits());
        })).toList();
        if (!lineage.equals(header.participants())) throw new IllegalArgumentException("Schema source participant lineage changed");
        if (schemaReplicas!=null || record.admissions()!=null) {
            if (record.admissions()==null || view.admissions()==null || !Arrays.equals(record.admissions(),view.admissions())
                    || !Arrays.equals(head.status().admissionDigest(),record.admissionDigest())) throw new IllegalArgumentException("Schema FENCE immutable admissions changed");
            SchemaAdmissionManifest.decode(record.admissions(),header,directory);
        }
        requireSchemaOwnership(directory,record.groupId());
    }
    private static boolean sameSchemaNamespace(RatisSchemaAuthority.Request request,SchemaChange.Header header) {
        return request.clusterId().equals(header.clusterId()) && request.databaseId().equals(header.databaseId()) && request.domainId().equals(header.domainId());
    }
    private void requireSchemaOwnership(RangeDirectory directory,UUID group) throws Exception {
        var descriptor=directory.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow();
        var ownership=store.ownership(); var interval=ownership.interval(); var cfg=store.configuration();
        if (cfg!=null) requireSchemaMembership(descriptor,cfg.data());
        if (descriptor.rangeEpoch()!=ownership.owner().epoch() || cfg==null || schemaDescriptor==null
                || !descriptor.rangeId().equals(schemaDescriptor.rangeId()) || descriptor.configurationVersion()!=schemaDescriptor.configurationVersion()
                || !descriptor.replicas().equals(schemaDescriptor.replicas())
                || !interval.domainId().equals(directory.domainId()) || !interval.columns().equals(directory.columns())
                || !Arrays.equals(interval.lowerInclusive(),descriptor.lowerInclusive()) || !Arrays.equals(interval.upperExclusive(),descriptor.upperExclusive())
                || !Arrays.equals(store.schemaDigest(),directory.schemaDigest())) throw new IllegalArgumentException("Schema source differs from current DATA ownership");
    }
    private void prepareSchemaFence(RatisSchemaFenceRecord record) throws Exception {
        requireSchemaRecord(record); checkTransactionOwner(); if (schemaProof(record)!=null) return;
        var reader=schemaAuthority; if (reader==null) throw new IOException("Schema authority reader is not configured");
        var owner=leadership(); long generation=admissionGeneration.get(); SchemaHold hold;
        synchronized (admissions) {
            reconcileOlderProposalTerms(owner.term());
            if (schemaConfigurationPending) throw new IOException("Schema authority waits for queued configuration apply");
            if (schemaHold!=null) {
                hold=schemaHold;
                if (!hold.record.equals(record) || hold.invalidated || hold.term!=owner.term() || hold.generation!=generation || hold.configurationStamp!=schemaConfigurationStamp || hold.reading || !hold.qualified)
                    throw new IOException("Schema preparation already owns protected input");
                if (!proposalFootprints.drained(hold.lease)) throw new IOException("Schema predecessors have not drained");
                return;
            }
            if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16)
                throw new IOException("Protected schema proposal budget exhausted");
            var interval=store.ownership().interval();
            var spans=interval.columns().stream().map(c -> TransactionScope.Span.range(c,null,null)).toList();
            var lease=proposalFootprints.reserveGate(proposalFootprints.prepare(spans),owner.term(),generation);
            if (lease==null) throw new IOException("Conflicting DATA gate owns schema admission");
            hold=new SchemaHold(record,owner.term(),generation,schemaConfigurationStamp,lease,retainedProof(schemaRetainedBound(record),lease)); schemaHold=hold; schemaSlots++;
        }
        boolean qualified=false;
        try {
            var authority=reader.read(record.header(),record.admissionDigest()); // Network work never owns admissions or the native gate monitor.
            requireSchemaSource(record,authority);
            var capacity=schemaReplicas==null ? null : schemaStorage.requireSchemaFencingAdmission(store,record.header(),record.admissions(),schemaLocalReplica);
            checkTransactionOwner(); var current=leadership();
            synchronized (admissions) {
                if (schemaHold!=hold || hold.invalidated || hold.released || generation!=admissionGeneration.get() || current.term()!=hold.term || schemaConfigurationPending || hold.configurationStamp!=schemaConfigurationStamp)
                    throw new IOException("Schema preparation lost its authority owner");
                hold.capacity=capacity; hold.qualified=true; qualified=true;
                if (!proposalFootprints.drained(hold.lease)) throw new IOException("Schema predecessors have not drained");
            }
        } finally {
            synchronized (admissions) { hold.reading=false; if (!qualified || hold.invalidated || hold.released) releaseSchemaHold(hold); }
        }
    }
    private void cancelSchemaFence(RatisSchemaFenceRecord record) {
        requireSchemaRecord(record);
        synchronized (admissions) { var hold=schemaHold; if (hold!=null && (hold.record.equals(record) || sameActivationRequest(hold.record,record)) && !hold.submitted) { hold.invalidated=true; releaseSchemaHold(hold); } }
    }
    private TransactionContext startSchemaFence(TransactionContext transaction,RatisSchemaFenceRecord record,long generation,Leadership leader) throws Exception {
        requireSchemaRecord(record); checkTransactionOwner(); var proof=schemaProof(record);
        synchronized (admissions) {
            if (schemaConfigurationPending) throw new IOException("Schema append waits for queued configuration apply");
            if (proof!=null) {
                // A native retry needs no new source capture or volatile gate, but still owns a bounded input slot.
                if (schemaHold==null) {
                    if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16) throw new IOException("Protected schema proposal budget exhausted");
                    schemaHold=new SchemaHold(record,leader.term(),generation,schemaConfigurationStamp,null,retainedProof(schemaRetainedBound(record),null)); schemaHold.reading=false; schemaHold.qualified=true; schemaSlots++;
                }
                var hold=schemaHold;
                if (!hold.record.equals(record) || hold.reading) throw new IOException("Schema retry lost exact retained input");
                savedAppend(transaction,hold,record,leader,generation);hold.submitted=true; hold.owners++; transaction.setStateMachineContext(hold);
            } else {
                var hold=schemaHold;
                if (hold==null || !hold.record.equals(record) || hold.reading || !hold.qualified || hold.invalidated || hold.released
                        || hold.term!=leader.term() || hold.generation!=generation || hold.configurationStamp!=schemaConfigurationStamp || generation!=admissionGeneration.get() || !proposalFootprints.drained(hold.lease)
                        || schemaReplicas!=null && hold.capacity==null)
                    throw new IOException("Schema FENCE lacks drained current-owner preparation");
                hold.submitted=true; hold.owners++; transaction.setStateMachineContext(hold);
            }
            return transaction;
        }
    }
    private NativeSchemaInstallProof schemaInstallProof(RatisSchemaInstallRecord record) throws org.rocksdb.RocksDBException {
        requireSchemaGroup(record.groupId());
        var retained=store.lookupSchemaInstallationRecord(record.header());
        if (retained==null) return null;
        retained.requireDefinition(record.definition());
        if (!retained.fence().position().equals(record.expectedFencePosition())
                || record.header().kind()==SchemaChange.Kind.CREATE_COLUMN
                && (!record.changedPhysicalName().equals(retained.targetNames().get(record.header().changedColumnId()))
                || record.mergeAdmission()!=retained.targetMerges().contains(record.header().changedColumnId())))
            throw new IllegalArgumentException("Original installation binding changed");
        return retained.proof();
    }
    private NativeSchemaActivationProof schemaActivationProof(RatisSchemaActivateRpc.Request request) {
        requireSchemaGroup(request.groupId());
        var proof=store.lookupSchemaActivation(request.header());
        if (proof!=null) {
            if (!proof.installation().position().equals(request.expectedInstallPosition())) throw new IllegalArgumentException("Original activation installation changed");
            request.requireRecord(new RatisSchemaActivateRecord(request.header(),request.groupId(),request.expectedInstallPosition(),proof.catalogueGroup(),proof.cataloguePosition()));
        }
        return proof;
    }
    private byte[] schemaRecovered(Object record) throws org.rocksdb.RocksDBException {
        byte[] result;
        if (record instanceof RatisSchemaFenceRecord fence) { var proof=schemaProof(fence); result=proof==null ? null : proof.encode(); }
        else if (record instanceof RatisSchemaInstallRecord install) { var proof=schemaInstallProof(install); result=proof==null ? null : proof.encode(); }
        else {
            var activate=record instanceof RatisSchemaActivateRecord a ? a : null;
            var request=activate==null ? (RatisSchemaActivateRpc.Request)record : new RatisSchemaActivateRpc.Request(activate.header(),activate.groupId(),activate.expectedInstallPosition(),activate);
            var proof=schemaActivationProof(request);
            if (proof!=null && activate!=null && (!proof.catalogueGroup().equals(activate.catalogueGroup()) || !proof.cataloguePosition().equals(activate.cataloguePosition()))) throw new IllegalArgumentException("Original activation publication changed");
            result=proof==null ? null : proof.encode();
        }
        if (result!=null) synchronized (admissions) {
            if (schemaHold!=null && (schemaHold.record.equals(record) || schemaHold.activation!=null && schemaHold.activation.equals(record))) releaseSchemaHold(schemaHold);
        }
        return result;
    }
    private static boolean sameActivationRequest(Object first,Object second) {
        return first instanceof RatisSchemaActivateRpc.Request a && second instanceof RatisSchemaActivateRpc.Request b
                && a.groupId().equals(b.groupId()) && a.expectedInstallPosition().equals(b.expectedInstallPosition()) && Arrays.equals(a.header().encode(),b.header().encode());
    }
    private boolean matchesSchemaLifecycle(SchemaHold hold,Object record) {
        if (hold.record.equals(record)) return true;
        if (!sameActivationRequest(hold.record,record) || hold.activation==null) return false;
        ((RatisSchemaActivateRpc.Request)record).requireRecord(hold.activation); return true;
    }
    private SchemaHold reserveSchemaLifecycle(Object record,Leadership owner,long generation) throws IOException {
        synchronized (admissions) {
            reconcileOlderProposalTerms(owner.term());
            if (schemaConfigurationPending) throw new IOException("Schema operation waits for queued configuration apply");
            if (schemaHold!=null) {
                var hold=schemaHold;
                if (!matchesSchemaLifecycle(hold,record) || hold.invalidated || hold.released || hold.term!=owner.term() || hold.generation!=generation || hold.configurationStamp!=schemaConfigurationStamp || hold.reading || !hold.qualified)
                    throw new IOException("Schema preparation already owns protected input");
                if (hold.lease!=null && !proposalFootprints.drained(hold.lease) || hold.captureGate!=null && !hold.captureGate.ready()) throw new IOException("Schema predecessors have not drained");
                hold.nativeReady=true; return hold;
            }
            if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16) throw new IOException("Protected schema proposal budget exhausted");
            var spans=store.ownership().interval().columns().stream().map(c -> TransactionScope.Span.range(c,null,null)).toList();
            var lease=proposalFootprints.reserveGate(proposalFootprints.prepare(spans),owner.term(),generation);
            if (lease==null) throw new IOException("Conflicting DATA gate owns schema admission");
            var hold=new SchemaHold(record,owner.term(),generation,schemaConfigurationStamp,lease,retainedProof(schemaRetainedBound(record),lease)); schemaHold=hold; schemaSlots++; return hold;
        }
    }
    private void requireSchemaLifecycleOwner(SchemaHold hold) throws IOException {
        if (schemaHold!=hold || hold.invalidated || hold.released || hold.generation!=admissionGeneration.get() || schemaConfigurationPending || hold.configurationStamp!=schemaConfigurationStamp)
            throw new IOException("Schema preparation lost its authority owner");
    }
    private void prepareSchemaInstall(RatisSchemaInstallRecord record) throws Exception {
        requireSchemaGroup(record.groupId()); checkTransactionOwner(); if (schemaRecovered(record)!=null) return;
        if (schemaStorage==null || schemaAuthority==null) throw new IOException("Schema installation is not configured");
        var hold=reserveSchemaLifecycle(record,leadership(),admissionGeneration.get());
        if (!hold.reading) return;
        boolean qualified=false;
        try {
            hold.captureGate=store.prepareSchemaInstallation(record.header());
            var names=new java.util.HashMap<>(schemaStorage.schemaPhysicalNames(store));
            var merges=new java.util.HashSet<>(store.deterministicMergeColumns());
            if (record.header().kind()==SchemaChange.Kind.CREATE_COLUMN) {
                names.put(record.header().changedColumnId(),record.changedPhysicalName());
                if (record.mergeAdmission()) merges.add(record.header().changedColumnId());
            } else { names.remove(record.header().changedColumnId()); merges.remove(record.header().changedColumnId()); }
            hold.installPlan=schemaStorage.prepareSchemaInstallation(store,record.definition(),names,merges);
            var retainedFence=store.lookupSchemaFence(record.header());
            var authority=schemaAuthority.read(record.header(),retainedFence==null ? null : retainedFence.admissionDigest());
            if (authority.head()==null || authority.head().status().phase()!=SchemaChange.Phase.FENCED && authority.head().status().phase()!=SchemaChange.Phase.INSTALLED) throw new IllegalArgumentException("INSTALL lacks fenced catalogue authority");
            requireSchemaSource(new RatisSchemaFenceRecord(record.header(),record.groupId(),authority.admissions()),authority);
            var fence=store.lookupSchemaFence(record.header());
            if (fence==null || !fence.position().equals(record.expectedFencePosition())) throw new IllegalArgumentException("INSTALL original native fence changed");
            checkTransactionOwner();
            synchronized (admissions) {
                requireSchemaLifecycleOwner(hold); hold.qualified=true; qualified=true;
                if (!proposalFootprints.drained(hold.lease) || !hold.captureGate.ready()) throw new IOException("Schema installation predecessors have not drained");
                hold.nativeReady=true;
            }
        } finally { synchronized (admissions) { hold.reading=false; if (!qualified || hold.invalidated || hold.released) releaseSchemaHold(hold); } }
    }
    private RatisSchemaActivateRecord prepareSchemaActivate(RatisSchemaActivateRpc.Request request) throws Exception {
        requireSchemaGroup(request.groupId()); checkTransactionOwner();
        var saved=schemaActivationProof(request);
        if (saved!=null) return new RatisSchemaActivateRecord(request.header(),request.groupId(),request.expectedInstallPosition(),saved.catalogueGroup(),saved.cataloguePosition());
        if (schemaAuthority==null) throw new IOException("Schema activation is not configured");
        var hold=reserveSchemaLifecycle(request,leadership(),admissionGeneration.get());
        if (!hold.reading) return hold.activation;
        boolean qualified=false;
        try {
            var install=store.lookupSchemaInstallationRecord(request.header());
            if (install==null || !install.position().equals(request.expectedInstallPosition())) throw new IllegalArgumentException("ACTIVATE original installation changed");
            byte[] admissionDigest=install.proof().fence().admissionDigest();
            var authority=schemaAuthority.read(request.header(),admissionDigest); var directory=authority.directory(); var head=authority.head();
            if (!sameSchemaNamespace(authority.request(),request.header())
                    || head==null || head.status().phase()!=SchemaChange.Phase.PUBLISHED
                    || !Arrays.equals(head.status().header().encode(),request.header().encode()) || directory==null
                    || !Arrays.equals(directory.payloadDigest(),request.header().targetDirectoryDigest())
                    || !Arrays.equals(directory.payloadDigest(),install.definition().targetDirectory().payloadDigest())) throw new IllegalArgumentException("ACTIVATE lacks exact published catalogue authority");
            requireSchemaOwnership(directory,request.groupId());
            if (schemaReplicas!=null && (authority.admissions()==null || !Arrays.equals(head.status().admissionDigest(),admissionDigest))) throw new IllegalArgumentException("ACTIVATE immutable admissions changed");
            if (authority.admissions()!=null) SchemaAdmissionManifest.decode(authority.admissions(),install.definition());
            var frozen=request.frozenRecord();
            if (frozen!=null && (!frozen.catalogueGroup().equals(authority.catalogueGroup()) || !coversSchemaPosition(authority.position(),frozen.cataloguePosition()))) throw new IllegalArgumentException("ACTIVATE authority no longer covers frozen publication");
            var record=frozen==null ? new RatisSchemaActivateRecord(request.header(),request.groupId(),request.expectedInstallPosition(),authority.catalogueGroup(),authority.position()) : frozen;
            checkTransactionOwner();
            synchronized (admissions) { requireSchemaLifecycleOwner(hold); hold.activation=record; hold.qualified=true; qualified=true; }
            return record;
        } finally { synchronized (admissions) { hold.reading=false; if (!qualified || hold.invalidated || hold.released) releaseSchemaHold(hold); } }
    }
    private void cancelSchemaLifecycle(Object record) {
        synchronized (admissions) { var hold=schemaHold; if (hold!=null && (hold.record.equals(record) || sameActivationRequest(hold.record,record)) && !hold.submitted) { hold.invalidated=true; releaseSchemaHold(hold); } }
    }
    private TransactionContext startSchemaLifecycle(TransactionContext transaction,Object record,long generation,Leadership leader) throws Exception {
        var saved=schemaRecovered(record);
        synchronized (admissions) {
            if (schemaConfigurationPending) throw new IOException("Schema append waits for queued configuration apply");
            if (saved!=null && schemaHold==null) {
                if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16) throw new IOException("Protected schema proposal budget exhausted");
                schemaHold=new SchemaHold(record,leader.term(),generation,schemaConfigurationStamp,null,retainedProof(schemaRetainedBound(record),null)); schemaHold.reading=false; schemaHold.qualified=true; schemaSlots++;
            }
            var hold=schemaHold;
            if(saved!=null){
                if(hold==null || !(hold.record.equals(record) || hold.activation!=null && hold.activation.equals(record)) || hold.reading)throw new IOException("Saved schema input differs from retained owner");
                savedAppend(transaction,hold,record,leader,generation);
            }else if (hold==null || !(hold.record.equals(record) || hold.activation!=null && hold.activation.equals(record)) || hold.reading || !hold.qualified || hold.invalidated || hold.released
                    || hold.term!=leader.term() || hold.generation!=generation || hold.configurationStamp!=schemaConfigurationStamp || generation!=admissionGeneration.get()
                    || hold.lease!=null && !proposalFootprints.drained(hold.lease) || hold.captureGate!=null && !hold.nativeReady) throw new IOException("Schema operation lacks drained current-owner preparation");
            hold.submitted=true; hold.owners++; transaction.setStateMachineContext(hold); return transaction;
        }
    }
    private void requireDataRole() { if (role!=RatisNodePlan.Role.DATA) throw new IllegalStateException("Range role does not support DATA transaction/snapshot operations"); }
    void homeRecoveryData(RatisRoutedClient data) {
        java.util.Objects.requireNonNull(data);
        homeRecoveryProofs(recovery -> {
            if (!data.directory().clusterId().equals(recovery.before().clusterId()) || !data.directory().databaseId().equals(recovery.before().databaseId())) throw new IOException("HOME recovery DATA reader belongs to another database");
            return data.finalizedParticipant(recovery.proof().participant(),recovery.before().transactionId());
        });
        homeRecoveryFences(data::participantFence);
    }
    void homeRecoveryProofs(HomeRecoveryProofReader reader) {
        if (role!=RatisNodePlan.Role.TRANSACTION_HOME || getLifeCycleState()!=LifeCycle.State.NEW || homeRecoveryProofs!=null) throw new IllegalStateException("HOME data proof reader is fixed before initialization");
        homeRecoveryProofs=java.util.Objects.requireNonNull(reader);
    }
    void homeRecoveryFences(HomeRecoveryFenceReader reader) {
        if (role!=RatisNodePlan.Role.TRANSACTION_HOME || getLifeCycleState()!=LifeCycle.State.NEW || homeRecoveryFences!=null) throw new IllegalStateException("HOME fence reader is fixed before initialization");
        homeRecoveryFences=java.util.Objects.requireNonNull(reader);
    }
    void homeRecoveryReservations(UUID cluster,UUID database,UUID column) {
        if (role!=RatisNodePlan.Role.TRANSACTION_HOME || getLifeCycleState()!=LifeCycle.State.NEW || homeReservations) throw new IllegalStateException("HOME reservations are fixed before initialization");
        store.configureHomeRecoveryReservations(cluster,database,column); homeCluster=cluster; homeDatabase=database; homeColumn=column; homeReservations=true;
    }
    void snapshotHomeStorage(UUID cluster,UUID database,UUID column) {
        if (role!=RatisNodePlan.Role.SNAPSHOT_HOME || getLifeCycleState()!=LifeCycle.State.NEW || snapshotHomeConfigured || homeReservations) throw new IllegalStateException("Snapshot HOME storage is fixed before initialization");
        store.configureSnapshotHome(cluster,database,column); homeCluster=cluster; homeDatabase=database; homeColumn=column; snapshotHomeConfigured=true;
    }
    void snapshotHomeProofs(SnapshotHomeProofReader reader) {
        if (role!=RatisNodePlan.Role.SNAPSHOT_HOME || getLifeCycleState()!=LifeCycle.State.NEW || snapshotHomeProofs!=null) throw new IllegalStateException("Snapshot HOME proof reader is fixed before initialization"); snapshotHomeProofs=java.util.Objects.requireNonNull(reader);
    }
    void snapshotHomeData(RatisRoutedClient data) { java.util.Objects.requireNonNull(data); snapshotHomeProofs(data::snapshotFence); }
    private void requireSnapshotHomeIdentity(SnapshotHomeIdentity identity) {
        if (role!=RatisNodePlan.Role.SNAPSHOT_HOME || !snapshotHomeConfigured || !identity.fence().clusterId().equals(homeCluster) || !identity.fence().databaseId().equals(homeDatabase) || !identity.columnId().equals(homeColumn)) throw new IllegalArgumentException("Snapshot HOME identity differs from configured context");
    }
    private void requireHomeRecovery(HomeRecovery recovery) {
        if (role!=RatisNodePlan.Role.TRANSACTION_HOME) throw new IllegalArgumentException("Only transaction HOME accepts protected recovery");
        commandValidator.accept(recovery.command());
    }
    private void requireHomeRetirement(HomeRetirement retirement) {
        var identity=retirement.identity();
        if (role!=RatisNodePlan.Role.TRANSACTION_HOME || !homeReservations || !identity.definition().clusterId().equals(homeCluster) || !identity.definition().databaseId().equals(homeDatabase)
                || !identity.columnId().equals(homeColumn)) throw new IllegalArgumentException("HOME retirement requires the configured transaction HOME context");
    }
    private void requireRoleRecord(byte[] encoded) {
        if(RatisTopologyRelocationRecord.isRecord(encoded)){requireDataRole();var record=RatisTopologyRelocationRecord.decode(encoded);if(!record.definition().parents().getFirst().groupId().equals(store.ownership().owner().groupId()))throw new IllegalArgumentException("Foreign relocation group");return;}
        if(RatisSourceOutcomeRecord.isRecord(encoded)){requireDataRole();if(!RatisSourceOutcomeRecord.decode(encoded).request().original().groupId().equals(store.ownership().owner().groupId()))throw new IllegalArgumentException("Foreign source outcome record");return;}
        if(RatisTopologyActivationRecord.isActivation(encoded)){requireDataRole();if(!RatisTopologyActivationRecord.decode(encoded).groupId().equals(store.ownership().owner().groupId()))throw new IllegalArgumentException("Foreign topology ACTIVATE group");return;}
        if(RatisTopologyFreezeRecord.isFreeze(encoded)){requireDataRole();if(!RatisTopologyFreezeRecord.decode(encoded).groupId().equals(store.ownership().owner().groupId()))throw new IllegalArgumentException("Foreign topology FREEZE group");return;}
        if (RatisReferencedFile.Request.isRequest(encoded)) { requireDataRole(); if(referencedFiles==null || rangeImportApply==null)throw new IllegalArgumentException("Native import file owner is not configured"); referencedFiles.requireGroup(RatisReferencedFile.Request.decode(encoded).groupId()); return; }
        if (RatisRangeImportRecord.isImport(encoded)) { requireDataRole(); if(rangeImportApply==null)throw new IllegalArgumentException("Native import adapter is not configured"); RatisRangeImportRecord.decode(encoded).requireDefinition(rangeImportApply.definition()); return; }
        if (RatisSchemaFenceRecord.isFence(encoded)) { requireSchemaRecord(RatisSchemaFenceRecord.decode(encoded)); return; }
        if (RatisSchemaInstallRecord.isInstall(encoded)) { requireSchemaGroup(RatisSchemaInstallRecord.decode(encoded).groupId()); return; }
        if (RatisSchemaActivateRecord.isActivate(encoded)) { requireSchemaGroup(RatisSchemaActivateRecord.decode(encoded).groupId()); return; }
        if (RatisSnapshotHomeRetirementRecord.isRetirement(encoded)) { requireSnapshotHomeIdentity(RatisSnapshotHomeRetirementRecord.decode(encoded).identity()); return; }
        if (RatisSnapshotRetirementRecord.isRetirement(encoded)) { requireDataRole(); return; }
        if (RatisHomeRetirementRecord.isRetirement(encoded)) { requireHomeRetirement(RatisHomeRetirementRecord.decode(encoded)); return; }
        if (HomeRecovery.isRecovery(encoded)) { requireHomeRecovery(HomeRecovery.decode(encoded)); return; }
        if (role==RatisNodePlan.Role.DATA) return;
        if (RatisSnapshotRecord.isSnapshot(encoded) || RatisParticipantRecord.isParticipant(encoded)) throw new IllegalArgumentException("Range role rejects participant/snapshot records");
        var command=DistributedCommandCodec.decode(encoded);
        if (command.kind()!=DistributedCommand.Kind.DATA && command.kind()!=DistributedCommand.Kind.SESSION_FLOOR && command.kind()!=DistributedCommand.Kind.SESSION_CONTROL) throw new IllegalArgumentException("Range role rejects transaction business records");
    }
    private void validateRoleNativeState() throws IOException {
        if(role!=RatisNodePlan.Role.DATA && store.topologyFreeze()!=null)throw new IOException("Non-DATA range contains topology FREEZE state");
        if (role!=RatisNodePlan.Role.DATA && (store.schemaFence()!=null || store.schemaInstallation()!=null)) throw new IOException("Non-DATA range contains schema transition state");
        if (role!=RatisNodePlan.Role.TRANSACTION_HOME && store.recoveredHomeRecoveryCapacity().effectiveCount()!=0) throw new IOException("Range role contains protected transaction-HOME receipts");
        if (role!=RatisNodePlan.Role.DATA && (store.preparedCapacity().count()!=0 || store.gateCapacity().count()!=0)) throw new IOException("Non-DATA range contains participant/snapshot state");
    }
    void proofQueries(ProofQueryExecutor executor) {
        if (getLifeCycleState()!=LifeCycle.State.NEW || proofQueries!=null) throw new IllegalStateException("Proof query dispatcher is fixed before initialization");
        proofQueries=java.util.Objects.requireNonNull(executor);
    }
    void exportSources(RatisExportSourceService service) {
        requireDataRole();
        if (getLifeCycleState()!=LifeCycle.State.NEW || exportSources!=null)
            throw new IllegalStateException("Export source dispatcher is fixed before initialization");
        exportSources=java.util.Objects.requireNonNull(service);
    }
    void requireExportPin(RatisExportSourceRpc.JobDefinition definition) throws Exception {
        requireDataRole();
        var participant=definition.fence().participant(definition.group());
        if (!store.ownership().owner().groupId().equals(definition.group())
                || store.ownership().owner().epoch()!=participant.rangeEpoch()
                || !java.util.Arrays.equals(store.schemaDigest(),participant.schemaDigest()))
            throw new RatisSnapshotLostException("Original export range or schema is unavailable");
        capturedSnapshots.requireExport(definition.capture(),definition.fence());
    }
    void snapshotRecovery(RatisSnapshotReadinessHelper.Recovery recovery,SnapshotHelpExecutor executor) {
        requireDataRole();
        if (getLifeCycleState()!=LifeCycle.State.NEW || snapshotRecovery!=null) throw new IllegalStateException("Snapshot recovery is fixed before initialization");
        snapshotRecovery=java.util.Objects.requireNonNull(recovery); snapshotHelpExecutor=java.util.Objects.requireNonNull(executor);
    }
    void transactionHomes(RatisTransactionStatuses homes) {
        requireDataRole();
        if (getLifeCycleState()!=LifeCycle.State.NEW || transactionHomes!=null) throw new IllegalStateException("Transaction HOME reader is fixed before initialization");
        transactionHomes=java.util.Objects.requireNonNull(homes);
    }
    void snapshotHomes(RatisSnapshotFences homes) {
        requireDataRole();
        if (getLifeCycleState()!=LifeCycle.State.NEW) throw new IllegalStateException("Snapshot home reader is fixed before initialization");
        if (snapshotHomes!=null) throw new IllegalStateException("Snapshot home reader already configured"); snapshotHomes=java.util.Objects.requireNonNull(homes);
    }

    private RatisLogResourceBudget logResources;
    public void logResources(RatisLogResourceBudget resources){if(getLifeCycleState()!=LifeCycle.State.NEW || logResources!=null)throw new IllegalStateException("Log resources fixed before initialization");logResources=java.util.Objects.requireNonNull(resources);}
    @Override public org.apache.ratis.server.impl.LogResourceStateMachine.Resources logResources(){return logResources;}
    /** Structural completion classification only; native apply still proves the exact predecessor/authority. */
    @Override public boolean isProtectedLogEntry(org.apache.ratis.proto.RaftProtos.LogEntryProto entry) throws IOException {
        if(entry==null || !entry.hasStateMachineLogEntry() || entry.getSerializedSize()>NativeSstImportManifest.MAX_FILE_BYTES+4096L)return false;
        var data=entry.getStateMachineLogEntry();
        if(data.hasStateMachineEntry())return false; // External payloads are fresh bulk work.
        var wire=data.getLogData();
        var prefix=new byte[Math.min(wire.size(),DistributedCommandCodec.HEADER_BYTES)];
        for(int i=0;i<prefix.length;i++)prefix[i]=wire.byteAt(i);
        long maximum=protectedLogMaximum(wire,prefix);
        if(maximum==0 || wire.size()>maximum)return false;
        if(logResources==null)throw new IOException("Protected log classification requires physical resource admission");
        // Encoded copy, nested decoder clones/canonical encoders and bounded model graphs coexist.
        try(var scratch=logResources.reserveClassification(Math.addExact(65536,Math.multiplyExact(16,maximum)))) {
            byte[] encoded=wire.toByteArray();UUID group=initialGroup.getGroupId().getUuid();
            if(RatisParticipantRecord.isParticipant(encoded)) {
                if(role!=RatisNodePlan.Role.DATA)return false;
                if(RatisParticipantRecord.isFinalization(encoded))return group.equals(RatisParticipantRecord.decodeFinalization(encoded).participantGroup());
                if(RatisParticipantRecord.isRetirement(encoded))return group.equals(RatisParticipantRecord.decodeRetirement(encoded).identity().participantGroup());
                return RatisParticipantRecord.isNegative(encoded) && group.equals(RatisParticipantRecord.decode(encoded).business().groupId());
            }
            if(HomeRecovery.isRecovery(encoded))return role==RatisNodePlan.Role.TRANSACTION_HOME && group.equals(HomeRecovery.decode(encoded).command().groupId());
            if(RatisHomeRetirementRecord.isRetirement(encoded)) {
                var retirement=RatisHomeRetirementRecord.decode(encoded);requireHomeRetirement(retirement);return group.equals(retirement.identity().groupId());
            }
            if(RatisSnapshotHomeRetirementRecord.isRetirement(encoded)) {
                var retirement=RatisSnapshotHomeRetirementRecord.decode(encoded);requireSnapshotHomeIdentity(retirement.identity());return group.equals(retirement.identity().groupId());
            }
            if(RatisSnapshotRetirementRecord.isRetirement(encoded))return role==RatisNodePlan.Role.DATA && group.equals(RatisSnapshotRetirementRecord.decode(encoded).participantGroup());
            if(RatisSnapshotRecord.isSnapshot(encoded)) {
                var snapshot=RatisSnapshotRecord.decode(encoded);return role==RatisNodePlan.Role.DATA && snapshot.isRelease() && group.equals(snapshot.participantGroup());
            }
            if(RatisSourceOutcomeRecord.isRecord(encoded)) {
                var outcome=RatisSourceOutcomeRecord.decode(encoded);return role==RatisNodePlan.Role.DATA && outcome.kind()==RatisSourceOutcomeRecord.Kind.PUBLISH && group.equals(outcome.request().original().groupId());
            }
            if(RatisTopologyActivationRecord.isActivation(encoded))return role==RatisNodePlan.Role.DATA && group.equals(RatisTopologyActivationRecord.decode(encoded).groupId());
            if(RatisSchemaActivateRecord.isActivate(encoded))return role==RatisNodePlan.Role.DATA && group.equals(RatisSchemaActivateRecord.decode(encoded).groupId());
            if(RatisTopologyRelocationRecord.isRecord(encoded)) {
                var relocation=RatisTopologyRelocationRecord.decode(encoded);return role==RatisNodePlan.Role.DATA && relocation.action()==RatisTopologyRelocationRecord.Action.COMPLETE && group.equals(relocation.definition().parents().getFirst().groupId());
            }
            var command=DistributedCommandCodec.decode(encoded);
            if(!group.equals(command.groupId()) || !Arrays.equals(encoded,DistributedCommandCodec.encode(command)))return false;
            return switch(command.kind()) {
                case SESSION_FLOOR,COMMIT_REJECTED -> true;
                case SESSION_CONTROL -> !command.sessionControl().binding();
                case COMMIT_ACCEPTED -> false;
                case DATA -> command.mutations().isEmpty() && command.predicates().isEmpty()
                        || protectedSnapshotHomeSuccessor(command,encoded);
            };
        } catch(IllegalArgumentException | java.nio.BufferUnderflowException malformed) {return false;}
    }
    private long protectedLogMaximum(ByteString wire,byte[] prefix) {
        if(RatisParticipantRecord.isParticipant(prefix)) {
            if(role!=RatisNodePlan.Role.DATA)return 0;
            if(RatisParticipantRecord.isFinalization(prefix))return 8L+ParticipantFinalization.MAX_BYTES;
            if(RatisParticipantRecord.isRetirement(prefix))return 8L+ParticipantRetirement.MAX_BYTES;
            return RatisParticipantRecord.isNegative(prefix)?8L+PreparedParticipant.Preparation.MAX_BYTES:0;
        }
        if(HomeRecovery.isRecovery(prefix))return role==RatisNodePlan.Role.TRANSACTION_HOME?HomeRecovery.MAX_BYTES:0;
        if(RatisHomeRetirementRecord.isRetirement(prefix))return role==RatisNodePlan.Role.TRANSACTION_HOME?4L+HomeRetirement.MAX_BYTES:0;
        if(RatisSnapshotHomeRetirementRecord.isRetirement(prefix))return role==RatisNodePlan.Role.SNAPSHOT_HOME?4L+SnapshotHomeRetirement.MAX_BYTES:0;
        if(RatisSnapshotRetirementRecord.isRetirement(prefix))return role==RatisNodePlan.Role.DATA?4L+SnapshotGateRetirement.MAX_BYTES:0;
        if(RatisSnapshotRecord.isSnapshot(prefix))return role==RatisNodePlan.Role.DATA && prefix.length>=8 && ByteBuffer.wrap(prefix).getInt(4)==2?DistributedCommandCodec.MAX_ENCODED_BYTES:0;
        if(RatisSourceOutcomeRecord.isRecord(prefix))return role==RatisNodePlan.Role.DATA && prefix.length>=8 && ByteBuffer.wrap(prefix).getInt(4)==RatisSourceOutcomeRecord.Kind.PUBLISH.ordinal()?RatisSourceOutcomeRecord.MAX_BYTES:0;
        if(RatisTopologyActivationRecord.isActivation(prefix))return role==RatisNodePlan.Role.DATA?RatisTopologyActivationRecord.MAX_BYTES:0;
        if(RatisSchemaActivateRecord.isActivate(prefix))return role==RatisNodePlan.Role.DATA?RatisSchemaActivateRecord.MAX_BYTES:0;
        if(RatisTopologyRelocationRecord.isRecord(prefix))return role==RatisNodePlan.Role.DATA && prefix.length>=5 && prefix[4]==RatisTopologyRelocationRecord.Action.COMPLETE.ordinal()?RatisTopologyRelocationRecord.MAX_BYTES:0;
        if(prefix.length<DistributedCommandCodec.HEADER_BYTES)return 0;
        return switch(prefix[DistributedCommandCodec.KIND_OFFSET]&255) {
            case 1 -> ByteBuffer.wrap(prefix).getInt(DistributedCommandCodec.MUTATION_COUNT_OFFSET)==0 && ByteBuffer.wrap(prefix).getInt(DistributedCommandCodec.PREDICATE_COUNT_OFFSET)==0?DistributedCommandCodec.HEADER_BYTES
                    : snapshotHomeProgressShape(wire)?DistributedCommandCodec.MAX_ENCODED_BYTES:0;
            case 2 -> DistributedCommandCodec.HEADER_BYTES+8L;
            case 3 -> wire.size()>=DistributedCommandCodec.HEADER_BYTES+4 && logWireInt(wire,DistributedCommandCodec.HEADER_BYTES)==RangeSessionControl.ADVANCE_BYTES
                    ?DistributedCommandCodec.HEADER_BYTES+4L+RangeSessionControl.ADVANCE_BYTES:0;
            case 5 -> DistributedCommandCodec.HEADER_BYTES+32L;
            default -> 0;
        };
    }
    /** Project an existing snapshot HOME CAS without coalescing a possibly segmented ByteString. */
    private boolean snapshotHomeProgressShape(ByteString wire) {
        if(!snapshotHomeConfigured || role!=RatisNodePlan.Role.SNAPSHOT_HOME || wire.size()>DistributedCommandCodec.MAX_ENCODED_BYTES)return false;
        try {
            if(logWireLong(wire,DistributedCommandCodec.ACK_OFFSET)!=-1 || logWireInt(wire,DistributedCommandCodec.MUTATION_COUNT_OFFSET)!=1 || logWireInt(wire,DistributedCommandCodec.PREDICATE_COUNT_OFFSET)!=1)return false;
            int p=DistributedCommandCodec.HEADER_BYTES;
            if(wire.byteAt(p++)!=1 || wire.byteAt(p++)!=0 || logWireLong(wire,p)!=homeColumn.getMostSignificantBits() || logWireLong(wire,p+8)!=homeColumn.getLeastSignificantBits() || logWireInt(wire,p+16)!=16)return false;
            p+=20;long keyHigh=logWireLong(wire,p),keyLow=logWireLong(wire,p+8);p+=16;int bytes=logWireInt(wire,p);p+=4;
            if(bytes<SnapshotFenceStatus.HEADER_BYTES || bytes>SnapshotFenceStatus.MAX_BYTES || bytes>wire.size()-p)return false;p+=bytes;
            if(wire.byteAt(p++)!=1 || logWireLong(wire,p)!=homeColumn.getMostSignificantBits() || logWireLong(wire,p+8)!=homeColumn.getLeastSignificantBits() || logWireInt(wire,p+16)!=16)return false;
            p+=20;if(logWireLong(wire,p)!=keyHigh || logWireLong(wire,p+8)!=keyLow)return false;p+=16;int expected=logWireInt(wire,p);p+=4;
            return expected>=SnapshotFenceStatus.HEADER_BYTES && expected<=SnapshotFenceStatus.MAX_BYTES && expected==wire.size()-p;
        } catch(IndexOutOfBoundsException malformed) {return false;}
    }
    private static int logWireInt(ByteString wire,int offset) {int value=0;for(int i=0;i<4;i++)value=(value<<8)|(wire.byteAt(offset+i)&255);return value;}
    private static long logWireLong(ByteString wire,int offset) {long value=0;for(int i=0;i<8;i++)value=(value<<8)|(wire.byteAt(offset+i)&255);return value;}
    private boolean protectedSnapshotHomeSuccessor(DistributedCommand command,byte[] encoded) {
        if(!snapshotHomeProgressInput(ByteBuffer.wrap(encoded)))return false;
        var before=SnapshotFenceStatus.decode(command.predicates().getFirst().expectedValue());
        var next=SnapshotFenceStatus.decode(command.mutations().getFirst().value());next.requireSuccessorOf(before);
        requireSnapshotHomeIdentity(new SnapshotHomeIdentity(command.groupId(),command.rangeEpoch(),command.schemaDigest(),homeColumn,before.fence()));
        byte[] key=ByteBuffer.allocate(16).putLong(before.fence().fenceId().getMostSignificantBits()).putLong(before.fence().fenceId().getLeastSignificantBits()).array();
        return Arrays.equals(key,command.mutations().getFirst().key());
    }
    private RatisDiskRunway diskRunway;private UUID diskPool;
    public void diskRunway(RatisDiskRunway runway,UUID pool){if(getLifeCycleState()!=LifeCycle.State.NEW || diskRunway!=null)throw new IllegalStateException("Disk runway fixed before initialization");diskRunway=java.util.Objects.requireNonNull(runway);diskPool=java.util.Objects.requireNonNull(pool);}
    private RatisNodeResourceBudget nodeResourceBudget;
    private UUID nodeResourcePool;
    /** Shared volatile limits are bound once before any proposal or native handle is admitted. */
    public void nodeResources(RatisNodeResourceBudget budget,UUID pool) {
        if (getLifeCycleState()!=LifeCycle.State.NEW || nodeResourceBudget!=null) throw new IllegalStateException("Node resources are fixed before initialization");
        java.util.Objects.requireNonNull(budget); java.util.Objects.requireNonNull(pool);
        if (!budget.poolIds().contains(pool)) throw new IllegalArgumentException("Undeclared native resource pool");
        nodeResourcePool=pool; nodeResourceBudget=budget;snapshotMetadataAdmission=budget.imageMetadataAdmission(pool,NativeRangeImage.IoAdmission.UNLIMITED);
    }
    RatisNodeResourceBudget.Lease reserveNodeResources(RatisNodeResourceBudget.Kind kind,long bytes,int owners) throws IOException {
        if(kind==RatisNodeResourceBudget.Kind.ORDINARY && diskRunway!=null)diskRunway.requireOrdinary();
        return nodeResourceBudget==null ? null : nodeResourceBudget.reserve(nodeResourcePool,kind,bytes,owners);
    }

    /** Test clock configuration is fixed before this state-machine owner is initialized. */
    void transactionClock(java.util.function.LongSupplier nanos) {
        if (getLifeCycleState()!=LifeCycle.State.NEW) throw new IllegalStateException("Transaction clock is fixed before initialization");
        scopedTransactions=new RatisScopedTransactions(this,RatisScopedTransactions.Limits.defaults(),nanos);
        snapshotPinClock=nanos;capturedSnapshots=newSnapshotPins(nanos);
    }
    long transactionGeneration() { return admissionGeneration.get(); }
    RatisNodeReadiness.MachineStamp readinessStamp(){
        synchronized(admissions){return new RatisNodeReadiness.MachineStamp(admissionGeneration.get(),schemaConfigurationStamp,schemaConfigurationPending,storageFailure,schemaActivationObserverFailure!=null?schemaActivationObserverFailure:topologyActivationObserverFailure);}
    }

    long transactionTerm() { var a=store.applied(); return a==null ? -1 : a.term(); }
    UUID transactionOwnerIncarnation() { return transactionIncarnation; }
    void checkTransactionOwner() throws IOException {
        requireDataRole(); requireHealthy(); requireUserReplica();var current=leadership(); var a=store.applied();
        if (!current.ready() || a==null || a.term()!=current.term() || !store.admissionOpen()) throw new IOException("Transaction leader changed or is not ready");
    }
    private ProofQueryExecutor sourceOutcomeQueries;
    void sourceOutcomeQueries(ProofQueryExecutor executor){requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || sourceOutcomeQueries!=null)throw new IllegalStateException("Source outcome lane is fixed before initialization");sourceOutcomeQueries=java.util.Objects.requireNonNull(executor);}
    @FunctionalInterface interface SourceOutcomeAdmission { RatisSourceOutcomeRecord qualify(RatisSourceOutcomeRpc.Proposal proposal)throws IOException; }
    private SourceOutcomeAdmission sourceOutcomeAdmission;
    void sourceOutcomeAdmission(SourceOutcomeAdmission admission){requireDataRole();if(getLifeCycleState()!=LifeCycle.State.NEW || sourceOutcomeAdmission!=null)throw new IllegalStateException("Source outcome proof is fixed before initialization");sourceOutcomeAdmission=java.util.Objects.requireNonNull(admission);}
    private record SourceKey(OperationId operation,RatisSourceOutcomeRecord.Kind kind){}
    private final Map<SourceKey,SourceHold> sourceHolds=new HashMap<>();
    private static final class SourceHold extends RetainedProof {
        final SourceKey key;final NativeSourceOutcomeCredit.Request request;final long term,generation,stamp;final ApplyPosition cut;final long count,bytes,inputBytes,metadataBytes;
        SessionNamespace namespaceReservation; RatisSourceOutcomeRecord record;boolean reading=true,qualified,submitted,invalidated,released,maybeAppended;int owners;
        SourceHold(SourceKey key,NativeSourceOutcomeCredit.Request request,CdcMetadataOwner owner,long count,long bytes,long metadataBytes,RatisNodeResourceBudget.Lease graph){super(graph);this.key=key;this.request=request;this.term=owner.term();this.generation=owner.generation();this.stamp=owner.stamp();this.cut=owner.cut();this.count=count;this.bytes=bytes;this.metadataBytes=metadataBytes;this.inputBytes=16L+request.encode().length+(key.kind()==RatisSourceOutcomeRecord.Kind.PUBLISH?request.maximumResultBytes():0);}
    }
    record SourceOutcomeStats(int holds,int reading,long count,long bytes){}
    SourceOutcomeStats sourceOutcomeStats(){synchronized(admissions){return new SourceOutcomeStats(sourceHolds.size(),(int)sourceHolds.values().stream().filter(h->h.reading).count(),sourceHolds.values().stream().mapToLong(h->h.count).sum(),sourceHolds.values().stream().mapToLong(h->h.bytes).sum());}}
    private void releaseSourceHold(SourceHold hold){hold.released=true;if(hold.reading || hold.owners>0 || hold.nativeOwners!=0)return;if(sourceHolds.remove(hold.key,hold)){hold.closeGraph();releaseSourceNamespace(hold);reservedReceiptCount-=hold.count;reservedReceiptBytes-=hold.bytes;}}
    private void releaseSourceNamespace(SourceHold hold) {
        if (hold.namespaceReservation!=null) { releaseSessionNamespace(hold.namespaceReservation); hold.namespaceReservation=null; }
    }
    private static boolean sourceCutCovered(ApplyPosition original,ApplyPosition current){return current!=null && original.term()==current.term() && current.index()>=original.index();}
    private void settleUnsubmittedSource(NativeSourceOutcomeCredit.Request request,RatisSourceOutcomeRecord.Kind kind){synchronized(admissions){var hold=sourceHolds.get(new SourceKey(request.original().operationId(),kind));if(hold!=null && hold.request.equals(request) && !hold.reading && !hold.submitted){hold.invalidated=true;releaseSourceHold(hold);}}}
    private void requireSourceOwner(SourceHold hold,Leadership current)throws IOException{
        if(sourceHolds.get(hold.key)!=hold || hold.invalidated || hold.released || !current.ready() || current.term()!=hold.term || hold.generation!=admissionGeneration.get() || schemaConfigurationPending || hold.stamp!=schemaConfigurationStamp || getLifeCycleState()!=LifeCycle.State.RUNNING || !sourceCutCovered(hold.cut,store.applied()) || store.pendingInstallation() || store.topologyBirthPending() || store.importPending() || !hold.request.freeze().equals(store.topologyFreeze()))throw new IOException("Source outcome lost sealed metadata owner");
    }
    private byte[] recoveredSourceRecord(RatisSourceOutcomeRecord record)throws Exception{var saved=committedSourceRecord(record);if(saved!=null)synchronized(admissions){var hold=sourceHolds.get(new SourceKey(record.request().original().operationId(),record.kind()));if(hold!=null && record.equals(hold.record)){releaseSourceNamespace(hold);releaseSourceHold(hold);}}return saved;}
    /** Committed replay/duplicate lookup must not cross the fresh metadata-readiness admission gate. */
    private byte[] committedSourceRecord(RatisSourceOutcomeRecord record)throws Exception {
        var saved=store.readCommittedSourceOutcome(record.request());
        if(record.kind()==RatisSourceOutcomeRecord.Kind.CREDIT) {
            if(saved.credit()!=null)return RatisSourceOutcomeRpc.applied(record,saved.fenced()?NativeSourceOutcomePublication.Outcome.FENCED:NativeSourceOutcomePublication.Outcome.RETAINED,saved.credit().encode());
            if(saved.fenced())return RatisSourceOutcomeRpc.applied(record,NativeSourceOutcomePublication.Outcome.FENCED,null);
            if(saved.publication()!=null)return RatisSourceOutcomeRpc.applied(record,NativeSourceOutcomePublication.Outcome.RETAINED,new NativeSourceOutcomeCredit(record.request(),saved.publication().creditPosition()).encode());
            return null;
        }
        if(saved.publication()!=null){saved.publication().requireRequest(record.request(),record.result());return RatisSourceOutcomeRpc.applied(record,saved.fenced()?NativeSourceOutcomePublication.Outcome.FENCED:NativeSourceOutcomePublication.Outcome.RETAINED,saved.publication().encode());}
        return saved.fenced()?RatisSourceOutcomeRpc.applied(record,NativeSourceOutcomePublication.Outcome.FENCED,null):null;
    }
    private byte[] savedSourceRecord(RatisSourceOutcomeRecord record)throws Exception{
        if(record.kind()==RatisSourceOutcomeRecord.Kind.CREDIT){var estimate=store.estimateSourceOutcomeCredit(record.request());var credit=store.sourceOutcomeCredit(record.request().original().operationId(),record.request().original().receiptDigest());if(credit!=null){if(!credit.request().equals(record.request()))throw new IOException("Saved source credit changed input");return RatisSourceOutcomeRpc.applied(record,estimate.fenced()?NativeSourceOutcomePublication.Outcome.FENCED:NativeSourceOutcomePublication.Outcome.RETAINED,credit.encode());}if(estimate.fenced())return RatisSourceOutcomeRpc.applied(record,NativeSourceOutcomePublication.Outcome.FENCED,null);if(estimate.existing()){var request=record.request();var origin=request.origin();var original=store.readTopologyOperationOutcome(origin.clusterId(),origin.databaseId(),origin.domainId(),origin.source(),request.original().operationId(),request.original().receiptDigest());if(original.result()==null)throw new IOException("Published source credit has no original retained result");var publication=store.sourceOutcomePublication(request,original.result());if(publication==null || publication.publication()==null)throw new IOException("Published source credit lost original proof");var historical=new NativeSourceOutcomeCredit(request,publication.publication().creditPosition());return RatisSourceOutcomeRpc.applied(record,publication.outcome(),historical.encode());}return null;}
        var saved=store.sourceOutcomePublication(record.request(),record.result());return saved==null?null:RatisSourceOutcomeRpc.applied(record,saved.outcome(),saved.publication()==null?null:saved.publication().encode());
    }
    private RatisSourceOutcomeRpc.Prepared prepareSourceOutcome(RatisSourceOutcomeRpc.Proposal proposal)throws Exception{
        var owner=checkCdcMetadataOwner();if(!owner.sealed())throw new IOException("Source outcome requires original sealed parent");
        if(proposal.frozenRecord()!=null){var saved=savedSourceRecord(proposal.frozenRecord());if(saved!=null){settleUnsubmittedSource(proposal.request(),proposal.kind());var applied=RatisSourceOutcomeRpc.applied(saved,proposal.frozenRecord());return new RatisSourceOutcomeRpc.Prepared(proposal.request().digest(),applied.outcome()==NativeSourceOutcomePublication.Outcome.FENCED?null:proposal.frozenRecord(),applied.outcome()==NativeSourceOutcomePublication.Outcome.FENCED);}}
        var estimate=store.estimateSourceOutcomeCredit(proposal.request());var pendingCredit=proposal.kind()==RatisSourceOutcomeRecord.Kind.PUBLISH?store.sourceOutcomeCredit(proposal.request().original().operationId(),proposal.request().original().receiptDigest()):null;if(estimate.fenced() && (proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT || pendingCredit==null)){settleUnsubmittedSource(proposal.request(),proposal.kind());return new RatisSourceOutcomeRpc.Prepared(proposal.request().digest(),null,true);}
        boolean savedCredit=proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT && estimate.existing();
        if(savedCredit)return new RatisSourceOutcomeRpc.Prepared(proposal.request().digest(),new RatisSourceOutcomeRecord(proposal.kind(),proposal.request(),null),false);
        if(proposal.kind()==RatisSourceOutcomeRecord.Kind.PUBLISH && pendingCredit==null)throw new IOException("Source publication requires original forced credit");
        if(!savedCredit && sourceOutcomeAdmission==null)throw new IOException("Source outcome trusted proof reader is not configured");
        if(!owner.cut().equals(estimate.usage().appliedPosition()))throw new IOException("Source outcome estimate lost metadata cut");
        var namespaceEstimate=proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT && proposal.request().original().acknowledgedThrough()>=0 ? store.estimateSourceSessionMetadata(proposal.request()) : null;
        if(namespaceEstimate!=null && !owner.cut().equals(namespaceEstimate.position()))throw new IOException("Source namespace estimate lost metadata cut");
        final SourceHold hold;var key=new SourceKey(proposal.request().original().operationId(),proposal.kind());
        synchronized(admissions){reconcileOlderProposalTerms(owner.term());if(nonSourceIdentityBusy(key.operation()))throw new IOException("Source original identity already owns ordinary/protected work");var prior=sourceHolds.get(key);if(prior!=null){if(!prior.request.equals(proposal.request()))throw new IOException("Source operation reused different input");try{requireSourceOwner(prior,leadership());if(prior.reading || !prior.qualified)throw new IOException("Source proof already owns input");if(proposal.frozenRecord()!=null && !prior.record.equals(proposal.frozenRecord()))throw new IOException("Source frozen result changed");return new RatisSourceOutcomeRpc.Prepared(proposal.request().digest(),prior.record,false);}catch(IOException stale){if(prior.reading || prior.submitted || prior.owners>0)throw stale;prior.invalidated=true;releaseSourceHold(prior);}}
            if(sourceHolds.values().stream().mapToLong(h->h.inputBytes).sum()+16L+proposal.request().encode().length+(proposal.kind()==RatisSourceOutcomeRecord.Kind.PUBLISH?proposal.request().maximumResultBytes():0)>NativeSourceOutcomeCredit.MAX_METADATA_BYTES || sourceHolds.size()>=NativeSourceOutcomeCredit.MAX_CREDITS || sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16 || owner.generation()!=admissionGeneration.get() || schemaConfigurationPending || owner.stamp()!=schemaConfigurationStamp || !owner.cut().equals(store.applied()))throw new IOException("Source outcome protected capacity/owner unavailable");
            long count=proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT?estimate.additionalCount():0,bytes=proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT?estimate.additionalBytes():0;
            if(!retentionFits(estimate.usage().retainedCount(),reservedReceiptCount,count,retentionLimits.dataCount()+(proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT?0:retentionLimits.maintenanceCount())) || !retentionFits(estimate.usage().retainedBytes(),reservedReceiptBytes,bytes,retentionLimits.dataBytes()+(proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT?0:retentionLimits.maintenanceBytes())))throw new IOException("Source promises overlap pending ordinary receipt capacity");
            if(proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT && !retentionFits(estimate.metadataUsageBytes(),sourceHolds.values().stream().mapToLong(h->h.metadataBytes).sum(),estimate.metadataBytes(),NativeSourceOutcomeCredit.MAX_METADATA_BYTES))throw new IOException("Source metadata promises exceed durable/volatile budget");
            var namespace=reserveSessionNamespace(proposal.request().original(),namespaceEstimate);
            try { hold=new SourceHold(key,proposal.request(),owner,count,bytes,proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT?estimate.metadataBytes():0,retainedProof(RatisSourceOutcomeRecord.MAX_BYTES,null)); }
            catch (IOException | RuntimeException | Error failure) { if(namespace!=null)releaseSessionNamespace(namespace); throw failure; }
            hold.namespaceReservation=namespace;sourceHolds.put(key,hold);reservedReceiptCount+=count;reservedReceiptBytes+=bytes;
        }
        boolean qualified=false;
        try{var record=savedCredit?new RatisSourceOutcomeRecord(proposal.kind(),proposal.request(),null):sourceOutcomeAdmission.qualify(proposal);if(record.kind()!=proposal.kind() || !record.request().equals(proposal.request()) || proposal.frozenRecord()!=null && !record.equals(proposal.frozenRecord()))throw new IOException("Source proof changed frozen record/result");var after=checkCdcMetadataOwner();if(!sameCdcConfiguration(owner,after) || !sourceCutCovered(owner.cut(),after.cut()))throw new IOException("Source proof lost original native metadata cut");synchronized(admissions){requireSourceOwner(hold,leadership());hold.record=record;hold.reading=false;hold.qualified=true;qualified=true;}return new RatisSourceOutcomeRpc.Prepared(proposal.request().digest(),record,false);
        }finally{synchronized(admissions){hold.reading=false;if(!qualified || hold.invalidated || hold.released)releaseSourceHold(hold);}}
    }
    private void cancelSourceOutcome(RatisSourceOutcomeRpc.Proposal proposal){synchronized(admissions){var hold=sourceHolds.get(new SourceKey(proposal.request().original().operationId(),proposal.kind()));if(hold!=null && hold.request.equals(proposal.request()) && !hold.submitted){hold.invalidated=true;releaseSourceHold(hold);}}}
    private TransactionContext startSourceOutcome(TransactionContext transaction,RatisSourceOutcomeRecord record)throws Exception{
        var owner=checkCdcMetadataOwner();if(!owner.sealed())throw new IOException("Source record requires sealed native owner");var saved=savedSourceRecord(record);var key=new SourceKey(record.request().original().operationId(),record.kind());
        synchronized(admissions){reconcileOlderProposalTerms(owner.term());if(nonSourceIdentityBusy(key.operation()))throw new IOException("Source record original identity already owns another proposal");var hold=sourceHolds.get(key);if(saved!=null && hold==null){if(sourceHolds.size()>=NativeSourceOutcomeCredit.MAX_CREDITS || sourceHolds.values().stream().mapToLong(h->h.inputBytes).sum()+record.encode().length>NativeSourceOutcomeCredit.MAX_METADATA_BYTES)throw new IOException("Saved source retry slots/input exhausted");hold=new SourceHold(key,record.request(),owner,0,0,0,retainedProof(RatisSourceOutcomeRecord.MAX_BYTES,null));hold.record=record;hold.reading=false;hold.qualified=true;sourceHolds.put(key,hold);}if(hold==null || hold.reading || !hold.qualified || !record.equals(hold.record))throw new IOException("Source record has no exact prepared proof");if(saved==null)requireSourceOwner(hold,leadership());else savedAppend(transaction,hold,record,leadership(),admissionGeneration.get());hold.submitted=true;hold.owners++;transaction.setStateMachineContext(hold);return transaction;}
    }
    private byte[] applySourceOutcome(ApplyPosition position,RatisSourceOutcomeRecord record)throws Exception{
        byte[] result;if(record.kind()==RatisSourceOutcomeRecord.Kind.CREDIT){var credit=store.applySourceOutcomeCredit(position,record.request());var replay=store.readCommittedSourceOutcome(record.request());result=RatisSourceOutcomeRpc.applied(record,replay.fenced()?NativeSourceOutcomePublication.Outcome.FENCED:NativeSourceOutcomePublication.Outcome.RETAINED,credit==null?null:credit.encode());}else{var published=store.publishSourceOutcome(position,record.request(),record.result());result=RatisSourceOutcomeRpc.applied(record,published.outcome(),published.publication()==null?null:published.publication().encode());}synchronized(admissions){var hold=sourceHolds.get(new SourceKey(record.request().original().operationId(),record.kind()));if(hold!=null && record.equals(hold.record)){releaseSourceNamespace(hold);releaseSourceHold(hold);}}return result;
    }
    private record CdcMetadataOwner(ApplyPosition cut,long term,long generation,long stamp,boolean sealed,ReplicatedRangeStore.Configuration configuration) {}
    /** Metadata authority only: this never opens a USER snapshot or changes DATA admission. */
    private CdcMetadataOwner checkCdcMetadataOwner() throws IOException {
        requireDataRole(); requireHealthy(); var leader=leadership(); var cut=store.applied(); long generation=admissionGeneration.get(); final long stamp;
        synchronized(admissions) { if(schemaConfigurationPending) throw new IOException("CDC metadata waits for configuration apply"); stamp=schemaConfigurationStamp; }
        if(getLifeCycleState()!=LifeCycle.State.RUNNING || !leader.ready() || cut==null || cut.term()!=leader.term() || store.topologyBirthPending()) throw new IOException("CDC metadata lacks completed current-term owner");
        var freeze=store.topologyFreeze(); boolean sealed=freeze!=null;
        if(!store.admissionOpen() && !sealed) throw new IOException("CDC metadata has no qualified active/sealed producer");
        if(sealed) {
            var ownership=store.ownership(); var interval=ownership.interval();
            try {
                freeze.requireParent(ownership.owner().groupId(),ownership.owner().epoch(),interval.domainId(),store.schemaDigest(),interval.lowerInclusive(),interval.upperExclusive()); freeze.requireCut(cut);
                if(cdcBinding!=null) {
                    var parent=freeze.header().participant(it.cavallium.rockserver.core.common.distributed.TopologyChange.Stage.FREEZE,freeze.groupId());
                    if(!cdcBinding.clusterId().equals(freeze.header().clusterId()) || !cdcBinding.databaseId().equals(freeze.header().databaseId()) || !cdcBinding.domainId().equals(freeze.header().domainId())
                            || !cdcBinding.lineage().rangeId().equals(parent.rangeId()) || !cdcBinding.lineage().groupId().equals(parent.groupId()) || cdcBinding.lineage().rangeEpoch()!=parent.rangeEpoch()) throw new IllegalArgumentException("CDC metadata original lineage differs");
                }
            } catch(IllegalArgumentException invalid) { throw new IOException("CDC metadata parent proof differs",invalid); }
        }
        final ReplicatedRangeStore.Configuration configuration;
        try { configuration=store.configuration(); if(configuration==null) throw new IOException("CDC metadata native configuration missing"); requireCdcMembership(configuration.data()); }
        catch(org.rocksdb.RocksDBException invalid) { throw new IOException("CDC metadata configuration read failed",invalid); }
        if(configuration.term()>cut.term() || configuration.index()>cut.index() || configuration.index()==cut.index() && configuration.term()!=cut.term()) throw new IOException("CDC metadata configuration exceeds completed prefix");
        synchronized(admissions) { if(generation!=admissionGeneration.get() || schemaConfigurationPending || stamp!=schemaConfigurationStamp || !java.util.Objects.equals(cut,store.applied())) throw new IOException("CDC metadata owner changed during qualification"); }
        return new CdcMetadataOwner(cut,leader.term(),generation,stamp,sealed,configuration);
    }
    private void requireCdcMembership(byte[] encoded) throws IOException {
        if(topologyDescriptor!=null) { requireSchemaMembership(topologyDescriptor,encoded); return; }
        var configuration=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.parseFrom(encoded);
        if(configuration.getOldPeersCount()!=0 || configuration.getOldListenersCount()!=0) throw new IOException("CDC metadata requires stable membership");
        var actual=new java.util.HashSet<String>();
        for(var peer:configuration.getPeersList()) actual.add(peer.getId().toStringUtf8()+"/"+peer.getAddress()+"/VOTER");
        for(var peer:configuration.getListenersList()) actual.add(peer.getId().toStringUtf8()+"/"+peer.getAddress()+"/LISTENER");
        var expected=new java.util.HashSet<String>();
        for(var peer:initialGroup.getPeers()) expected.add(peer.getId()+"/"+(peer.getAddress()==null ? "" : peer.getAddress())+"/"+(peer.getStartupRole()==org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER ? "LISTENER" : "VOTER"));
        if(!actual.equals(expected)) throw new IOException("CDC metadata original membership changed");
    }
    private static boolean sameCdcConfiguration(CdcMetadataOwner first,CdcMetadataOwner next) {
        return first.term()==next.term() && first.generation()==next.generation() && first.stamp()==next.stamp() && first.sealed()==next.sealed()
                && first.configuration().term()==next.configuration().term() && first.configuration().index()==next.configuration().index() && Arrays.equals(first.configuration().data(),next.configuration().data());
    }
    /** Bounded local retirement hints. Mutation admission independently revalidates qualified HOME/DATA proofs. */
    ReplicatedRangeStore.RetirementPage retirementCandidates(boolean orphans,byte[] after,int scans) throws IOException {
        if (role!=RatisNodePlan.Role.TRANSACTION_HOME && role!=RatisNodePlan.Role.SNAPSHOT_HOME) return null;
        if (storageFailure!=null) throw new IOException("Native retirement discovery requires repair",storageFailure);
        if (getLifeCycleState()!=LifeCycle.State.RUNNING) return null;
        var owner=leadership(); var applied=store.applied();
        if (!owner.ready() || applied==null || applied.term()!=owner.term() || !store.admissionOpen()) return null;
        long generation=admissionGeneration.get();
        final ReplicatedRangeStore.RetirementPage page;
        try { page=store.retirementCandidates(orphans,after,scans); }
        catch (org.rocksdb.RocksDBException | RuntimeException changed) {
            if (generation!=admissionGeneration.get() || getLifeCycleState()!=LifeCycle.State.RUNNING) return null;
            throw new IOException("Native retirement discovery unavailable",changed);
        }
        var current=leadership();
        if (storageFailure!=null) throw new IOException("Native retirement discovery requires repair",storageFailure);
        if (getLifeCycleState()!=LifeCycle.State.RUNNING || generation!=admissionGeneration.get() || !current.ready() || page.position().term()!=current.term() || !store.admissionOpen()) return null;
        return page;
    }
    /** Null means this replica has no qualified current-term discovery owner. */
    ReplicatedRangeStore.PreparedRecoveryPage preparedRecoveries(UUID after,int limit) throws IOException {
        if (role!=RatisNodePlan.Role.DATA) return null;
        if (storageFailure!=null) throw new IOException("Native recovery discovery requires repair",storageFailure);
        if (getLifeCycleState()!=LifeCycle.State.RUNNING) return null;
        var owner=leadership(); var applied=store.applied();
        if (!owner.ready() || applied==null || applied.term()!=owner.term() || !store.admissionOpen()) return null;
        long generation=admissionGeneration.get();
        final ReplicatedRangeStore.PreparedRecoveryPage page;
        try { page=store.preparedRecoveries(after,limit); }
        catch (RuntimeException changed) { if (generation!=admissionGeneration.get() || getLifeCycleState()!=LifeCycle.State.RUNNING) return null; throw new IOException("Native recovery discovery unavailable",changed); }
        var current=leadership();
        if (storageFailure!=null) throw new IOException("Native recovery discovery requires repair",storageFailure);
        if (getLifeCycleState()!=LifeCycle.State.RUNNING || generation!=admissionGeneration.get() || !current.ready() || page.position().term()!=current.term() || !store.admissionOpen()) return null;
        return page;
    }
    ReplicatedRangeStore.ScopedHistoryPin captureTransaction(TransactionScope scope) throws Exception {
        checkTransactionOwner(); long generation=admissionGeneration.get(); var pin=store.captureTracked(scope); boolean retained=false;
        try {
            checkTransactionOwner();
            synchronized (admissions) {
                var a=store.applied();
                if (generation!=admissionGeneration.get() || !store.admissionOpen() || a==null || pin.position().term()!=a.term()
                        || pin.status()!=it.cavallium.rockserver.core.common.distributed.RangeMutationHistory.Status.ACTIVE) throw new IOException("Transaction capture lost leadership");
            }
            retained=true; return pin;
        } finally { if (!retained) pin.close(); }
    }
    void expireTransactions() throws Exception {
        Exception failure=null;
        try { scopedTransactions.expire(); } catch (Exception problem) { failure=problem; }
        try { capturedSnapshots.retireBefore(store.snapshotRetentionFloor()); capturedSnapshots.expire(); } catch (Exception problem) { if (failure==null) failure=problem; else if (failure!=problem) failure.addSuppressed(problem); }
        if (failure!=null) throw failure;
    }
    private RatisSnapshotPins newSnapshotPins(java.util.function.LongSupplier nanos) {
        return new RatisSnapshotPins(transactionIncarnation,store.ownership().owner().groupId(),snapshotPinLimits,nanos,this::captureSnapshotGate,
                ReplicatedRangeStore.ScopedSnapshot::get,ReplicatedRangeStore.ScopedSnapshot::close,scopedTransactions::captureSnapshot,scopedTransactions::releaseAndDrain,scopedTransactions::snapshotLive);
    }
    /** Qualified coordinator seam; the caller freezes this incarnation before its first capture request. */
    public UUID snapshotCaptureOwner() throws IOException { checkTransactionOwner(); return transactionIncarnation; }
    public SnapshotFenceStatus.Capture captureSnapshotPin(RatisSnapshotFences homes,SnapshotFence fence,UUID owner,UUID snapshot,java.time.Duration ttl) throws Exception { requireDataRole();
        return capturedSnapshots.capture(homes,fence,owner,snapshot,ttl,this::snapshotRetentionFloor);
    }
    private long snapshotRetentionFloor() {
        try { return store.snapshotRetentionFloor(); } catch (org.rocksdb.RocksDBException unavailable) { throw new IllegalStateException("Cannot read snapshot retention floor",unavailable); }
    }
    /** Immutable pin reads do not acquire new leader authority or substitute a current native cut. */
    public byte[] snapshotPinGet(SnapshotFenceStatus.Capture token,UUID column,byte[] key) throws Exception { requireDataRole(); return capturedSnapshots.get(token,column,key); }
    public it.cavallium.rockserver.core.common.distributed.PhysicalRangePage snapshotPinPage(SnapshotFenceStatus.Capture token,UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws Exception {
        requireDataRole(); return capturedSnapshots.page(token,column,lower,upper,reverse,resume,items,bytes);
    }
    @FunctionalInterface public interface SnapshotChunkWriter {
        it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport.Chunk write(ReplicatedRangeStore.ScopedSnapshot pin) throws Exception;
    }
    /** Owner-directed export keeps the original native pin through actual exporter/descriptor completion. */
    public it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport.Chunk exportSnapshotPinChunk(SnapshotFenceStatus.Capture token,SnapshotChunkWriter writer) throws Exception {
        requireDataRole(); return capturedSnapshots.export(token,writer::write);
    }
    public void releaseSnapshotPin(SnapshotFenceStatus.Capture token) throws Exception { requireDataRole(); capturedSnapshots.release(token); }
    RatisSnapshotPins.Stats snapshotPinStats() { return capturedSnapshots.stats(); }
    RatisSnapshotCohortService.Member snapshotCohortMember(SnapshotFence fence,boolean ready,RangeDescriptor descriptor,ReplicaDescriptor replica)throws Exception {
        checkTransactionOwner();long generation=admissionGeneration.get();final long stamp;synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Cohort configuration apply pending");stamp=schemaConfigurationStamp;}
        var server=getServer().getNow(null);if(server==null || !server.getId().toString().equals(replica.peerId()))throw new IOException("Cohort peer identity differs");var division=server.getDivision(getGroupId());var info=division.getInfo();long term=info.getCurrentTerm();
        if(division.getStateMachine()!=this || getLifeCycleState()!=LifeCycle.State.RUNNING || !info.isAlive() || info.getLifeCycleState()!=LifeCycle.State.RUNNING || !info.isLeader() || !info.isLeaderReady() || replica.role()!=ReplicaDescriptor.Role.VOTER)throw new IOException("Cohort lacks actual running leader");
        var view=store.metadataOutcomeState();var cfg=store.configuration();var cut=view.applied();var log=division.getRaftLog();
        if(cut==null || cut.term()!=term || cfg==null || cfg.term()>cut.term() || cfg.index()>cut.index() || cfg.index()==cut.index() && cfg.term()!=cut.term() || info.getLastAppliedIndex()>cut.index() || log==null || log.getLastCommittedIndex()>cut.index())throw new IOException("Cohort native cut does not cover actual committed prefix");
        requireSchemaMembership(descriptor,cfg.data());requireActualMembership(division.getRaftConf(),descriptor);
        if(division.getRaftConf().getLogEntryIndex()!=cfg.index() && !(cfg.term()==0 && cfg.index()==-1 && division.getRaftConf().getLogEntryIndex()<=0))throw new IOException("Cohort actual/native configuration positions differ");
        if(fence!=null){var participant=fence.participant(view.groupId());if(participant.rangeEpoch()!=view.rangeEpoch() || !java.util.Arrays.equals(participant.schemaDigest(),view.schemaDigest()) || !fence.domainId().equals(view.domainId()))throw new IOException("Cohort fence changed native schema/epoch/domain");}
        if(ready){if(fence==null || snapshotHomes==null)throw new IOException("Cohort HOME readiness is unavailable");var status=snapshotHomes.readRegistration(fence.fenceId()).status();var gate=store.snapshotGate(fence.fenceId());if(status==null || !status.fence().equals(fence) || status.phase()!=SnapshotFenceStatus.Phase.INSTALLING || !status.allReady() || gate==null || !gate.active() || !gate.fence().equals(fence) || !gate.installed().equals(status.progress(view.groupId()).ready()) || !snapshotGateReady(fence))throw new IOException("Cohort HOME/native gate is not exactly READY");}
        checkTransactionOwner();synchronized(admissions){if(generation!=admissionGeneration.get() || stamp!=schemaConfigurationStamp || schemaConfigurationPending || division.getInfo().getCurrentTerm()!=term)throw new IOException("Cohort qualification changed owner/configuration");}
        var owner=new RatisSnapshotOwner(view.groupId(),view.rangeEpoch(),view.schemaDigest(),transactionIncarnation,replica.peerId(),null);
        return new RatisSnapshotCohortService.Member(store,capturedSnapshots,owner,replica,generation,term,OperatorUpload.sha256(cfg.data()),cut);
    }


    private static final class Admission {
        final OperationId id;
        final byte[] digest;
        final int bytes;
        final long term, generation, receiptCount, receiptBytes;
        final boolean maintenance, protectedControl;
        final ProposalFootprints.OrdinaryLease footprint;
        final ProposalFootprints.CandidateLease acceptedScope;
        final ByteString rejectionLogData;
        final java.util.List<RatisNodeResourceBudget.Lease> resourceLeases=new java.util.ArrayList<>();
        PreparedParticipant.Preparation participant;
        ParticipantHold participantHold;
        boolean participantNegative;
        NegativePayload negativePayload;
        RatisSnapshotRecord snapshot;
        SnapshotHold snapshotHold;
        ParticipantRetirement retirement;
        ParticipantFinalization finalization;
        FinalizationHold finalizationHold;
        SnapshotHomeRetirement snapshotHomeRetirement;
        SnapshotGateRetirement snapshotRetirement;
        HomeRetirement homeRetirement;
        HomeRecovery homeRecovery;
        HomeHold homeHold;
        long homeCreditCount,homeCreditBytes;
        SchemaHold fencingHold;
        ImportPublicationHold importPublicationHold;
        boolean sessionBinding;
        SessionNamespace namespaceReservation;
        DistributedCommand topologyMetadataControl;
        CdcMetadataOwner topologyMetadataOwner;
        boolean retentionReleased,maybeAppended,settled;
        int owners = 1;
        Admission(OperationId id, byte[] digest, int bytes, long term, long generation,
                long receiptCount, long receiptBytes, boolean maintenance, boolean protectedControl,ProposalFootprints.OrdinaryLease footprint,ProposalFootprints.CandidateLease acceptedScope,ByteString rejectionLogData) {
            this.rejectionLogData=rejectionLogData; this.acceptedScope=acceptedScope; this.footprint=footprint; this.id = id;
            this.digest = digest;
            this.bytes = bytes; this.term = term; this.generation = generation;
            this.receiptCount=receiptCount; this.receiptBytes=receiptBytes;
            this.maintenance=maintenance; this.protectedControl=protectedControl;
        }
    }

    private final ProposalFootprints proposalFootprints=new ProposalFootprints(ProposalFootprints.Limits.defaults());
    private long reconciledProposalTerm=-1;
    private record SessionNamespace(long epoch,UUID session) {}
    private final HashMap<SessionNamespace,Integer> pendingSessionNamespaces=new HashMap<>();
    private SessionNamespace reserveSessionNamespace(DistributedCommand command,ReplicatedRangeStore.SessionMetadataEstimate estimate) throws IOException {
        if (estimate==null || estimate.existingReceipt() || estimate.additionalNamespaces()==0) return null;
        var namespace=new SessionNamespace(command.operationId().epoch(),command.operationId().sessionId());
        boolean alreadyPending=pendingSessionNamespaces.containsKey(namespace);
        long projected=(long)estimate.retainedNamespaces()+estimate.reservedNamespaces()-estimate.removedNamespaces()+pendingSessionNamespaces.size()+(alreadyPending ? 0 : estimate.additionalNamespaces());
        if (projected>estimate.maximumNamespaces()) throw new IOException("Session ACK namespace admission exhausted");
        pendingSessionNamespaces.merge(namespace,1,Integer::sum); return namespace;
    }
    private void releaseSessionNamespace(Admission admission) {
        var namespace=admission.namespaceReservation;
        if (namespace!=null) { releaseSessionNamespace(namespace); admission.namespaceReservation=null; }
    }
    private void releaseSessionNamespace(SessionNamespace namespace) {
        pendingSessionNamespaces.compute(namespace,(key,references)->{
            if (references==null) throw new IllegalStateException("Session namespace reservation lost ownership");
            return references==1 ? null : references-1;
        });
    }

    private final UUID transactionIncarnation=UUID.randomUUID();
    private final HashMap<OperationId,ScopedCommit> preparedCommits=new HashMap<>();
    private static final class PreparingCommit {
        final ProposalReservation reservation; int bytes;
        PreparingCommit(ProposalReservation reservation,int bytes) { this.reservation=reservation; this.bytes=bytes; }
    }
    private final HashMap<OperationId,PreparingCommit> preparingCommits=new HashMap<>();
    private static final class NegativePayload {
        int bytes,references=1;
        NegativePayload(int bytes) { this.bytes=bytes; }
    }
    private int protectedNegativeSlots;
    private long negativeInputBytes;
    private static final class ParticipantHold {
        final OperationId control,business; final UUID transaction; final long scopeBytes,term,generation; long bytes;
        final NegativePayload payload;
        RatisNodeResourceBudget.Lease resourceLease;
        boolean released,capacityReleased,negativeSlot,transferred,invalidated; int finalizers; PreparedParticipant.Preparation preparation;
        ParticipantHold(UUID transaction,OperationId control,OperationId business,long bytes,long scopeBytes,long term,long generation,NegativePayload payload,boolean reserved) {
            this.transaction=transaction; this.control=control; this.business=business; this.bytes=bytes; this.scopeBytes=scopeBytes; this.term=term; this.generation=generation;
            this.payload=payload; negativeSlot=payload!=null; capacityReleased=!reserved;
        }
    }
    private record PreparingParticipant(ProposalReservation reservation,ParticipantHold hold) {}
    private final HashMap<OperationId,PreparingParticipant> preparingParticipants=new HashMap<>();
    private final HashMap<OperationId,ParticipantProposal> preparedParticipants=new HashMap<>();
    private final HashMap<OperationId,ParticipantHold> participantOwners=new HashMap<>();
    private long reservedParticipantCount,reservedParticipantBytes,reservedParticipantScopeBytes;
    private static final class FinalizationHold {
        final UUID transaction; final OperationId recovery; final long term,generation;
        OperationId business; ParticipantHold shared; RatisNodeResourceBudget.Lease resourceLease; boolean capacity,released;
        FinalizationHold(UUID transaction,OperationId recovery,long term,long generation) { this.transaction=transaction; this.recovery=recovery; this.term=term; this.generation=generation; }
    }
    private final HashMap<OperationId,FinalizationHold> finalizationOwners=new HashMap<>();
    private final HashMap<UUID,FinalizationHold> finalizingTransactions=new HashMap<>();
    private final HashMap<OperationId,FinalizationHold> preparingFinalizations=new HashMap<>();
    private final HashMap<OperationId,FinalizationProposal> preparedFinalizations=new HashMap<>();
    private record RetirementHold(ParticipantRetirement retirement,long term,long generation,int bytes,RatisNodeResourceBudget.Lease resources) {}
    private final HashMap<OperationId,RetirementHold> preparingRetirements=new HashMap<>(),preparedRetirements=new HashMap<>();
    private static final class HomeRetirementHold {
        final HomeRetirement retirement; final long term,generation; final int bytes; final ProposalFootprints.CandidateLease scope;
        RatisNodeResourceBudget.Lease resourceLease;
        boolean invalidated;
        HomeRetirementHold(HomeRetirement retirement,long term,long generation,ProposalFootprints.CandidateLease scope) { this.retirement=retirement; this.term=term; this.generation=generation; this.scope=scope; bytes=RatisHomeRetirementRecord.encode(retirement).length; }
    }
    private final HashMap<OperationId,HomeRetirementHold> preparingHomeRetirements=new HashMap<>(),preparedHomeRetirements=new HashMap<>();
    private int homeRetirementSlots;
    private long homeRetirementInputBytes;
    private record SnapshotRetirementHold(SnapshotGateRetirement retirement,long term,long generation,int bytes,RatisNodeResourceBudget.Lease resources) {}
    private final HashMap<OperationId,SnapshotRetirementHold> preparingSnapshotRetirements=new HashMap<>(),preparedSnapshotRetirements=new HashMap<>();
    private int snapshotRetirementSlots;
    private long snapshotRetirementInputBytes;
    private static final class SnapshotHomeRetirementHold {
        final SnapshotHomeRetirement retirement; final long term,generation; final int bytes; final ProposalFootprints.CandidateLease scope; RatisNodeResourceBudget.Lease resources; boolean invalidated;
        SnapshotHomeRetirementHold(SnapshotHomeRetirement retirement,long term,long generation,ProposalFootprints.CandidateLease scope) { this.retirement=retirement; this.term=term; this.generation=generation; this.scope=scope; bytes=RatisSnapshotHomeRetirementRecord.encode(retirement).length; }
    }
    private final HashMap<OperationId,SnapshotHomeRetirementHold> preparingSnapshotHomeRetirements=new HashMap<>(),preparedSnapshotHomeRetirements=new HashMap<>();
    private int snapshotHomeRetirementSlots;
    private long snapshotHomeRetirementInputBytes;
    private int retirementSlots;
    private long retirementInputBytes;
    private long reservedFinalizationCount,reservedFinalizationBytes;
    private int protectedFinalizationSlots;
    private static final class HomeHold {
        final HomeRecovery recovery; final long term,generation; final int bytes;
        final ProposalFootprints.CandidateLease scope;
        RatisNodeResourceBudget.Lease resourceLease;
        long count,capacityBytes;
        boolean invalidated,released,transferred;
        HomeHold(HomeRecovery recovery,long term,long generation,ProposalFootprints.CandidateLease scope) {
            this.recovery=recovery; this.term=term; this.generation=generation; this.scope=scope; bytes=recovery.encode().length;
        }
    }
    private final HashMap<OperationId,HomeHold> homeOwners=new HashMap<>(),preparingHomes=new HashMap<>();
    private final HashMap<OperationId,HomeRecoveryProposal> preparedHomes=new HashMap<>();
    private int homeRecoverySlots;
    private long homeRecoveryInputBytes,reservedHomeCount,reservedHomeBytes;
    private static final class SnapshotCharge {
        final SnapshotFence fence; final long bytes; int references; boolean reserved;
        SnapshotCharge(SnapshotFence fence,long bytes,boolean reserved) { this.fence=fence; this.bytes=bytes; this.reserved=reserved; }
    }
    private static final class SnapshotHold {
        final SnapshotCharge charge; final boolean install; final long term,generation;
        final OperationId operation; final ProposalFootprints.GateLease lease;
        RatisNodeResourceBudget.Lease resourceLease;
        int bytes=DistributedCommandCodec.MAX_ENCODED_BYTES; boolean slot=true,input=true,installCount,invalidated,released;
        SnapshotHold(SnapshotCharge charge,boolean install,long term,long generation,OperationId operation,ProposalFootprints.GateLease lease) {
            this.charge=charge; this.install=install; this.installCount=install; this.term=term; this.generation=generation; this.operation=operation; this.lease=lease;
        }
    }
    private final HashMap<UUID,SnapshotCharge> snapshotCharges=new HashMap<>();
    private final HashMap<OperationId,SnapshotCharge> snapshotControlCharges=new HashMap<>();
    private final HashMap<OperationId,SnapshotHold> preparingSnapshots=new HashMap<>();
    private final HashMap<OperationId,SnapshotProposal> preparedSnapshots=new HashMap<>();
    private long reservedSnapshotCount,reservedSnapshotBytes,snapshotProposalBytes,snapshotInstallBytes;
    private int snapshotSlots,snapshotInstallOwners,snapshotAdmittedInstalls;
    private long snapshotAdmittedBytes,snapshotInstallAdmittedBytes;
    private long preparedCommitBytes;
    private ProposalFootprints.CandidateLease exclusiveMetadataCandidate;
    private final Object gate = new Object();
    private final RaftGroup initialGroup;
    private RatisCommittedApplyDispatcher.Group committedGroup;
    private long committedMaximumEntryBytes;
    public static final long COMMITTED_SEGMENT_BYTES=8L*1024*1024;
    public static long committedReadWeight(long maximumEntryBytes){return Math.addExact(256,Math.multiplyExact(4,Math.addExact(Math.multiplyExact(2,maximumEntryBytes),COMMITTED_SEGMENT_BYTES)));}
    public static long committedExecutionWeight(long wireBytes){return Math.addExact(256,Math.multiplyExact(4,Math.addExact(Math.multiplyExact(2,wireBytes),COMMITTED_SEGMENT_BYTES+2L*1024*1024+RatisRangeImportRecord.MAX_MANIFEST_BYTES+NativeSchemaInstallation.MAX_BYTES)));}
    /** Production binding is fixed before any group initialization or committed materialization. */
    public void committedApply(RatisCommittedApplyDispatcher dispatcher,UUID pool,long maximumEntryBytes){
        if(getLifeCycleState()!=LifeCycle.State.NEW || committedGroup!=null || maximumEntryBytes<RatisSourceOutcomeRecord.MAX_BYTES)throw new IllegalStateException("Invalid committed apply binding");
        committedMaximumEntryBytes=maximumEntryBytes;
        committedGroup=dispatcher.register(store.ownership().owner().groupId(),pool,committedReadWeight(maximumEntryBytes),committedExecutionWeight(maximumEntryBytes),COMMITTED_SEGMENT_BYTES,this::describeCommitted);
    }
    @Override public org.apache.ratis.server.impl.CommittedApplyStateMachine.Admission committedApplyAdmission(){
        if(committedGroup==null)throw new IllegalStateException("Production committed apply admission is not configured");return committedGroup;
    }
    private org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor describeCommitted(org.apache.ratis.proto.RaftProtos.LogEntryProto entry){
        long size=entry.getSerializedSize();
        var lane=org.apache.ratis.server.impl.CommittedApplyStateMachine.Lane.PROTECTED;
        if(entry.hasStateMachineLogEntry()){
            var data=entry.getStateMachineLogEntry();
            if(data.hasStateMachineEntry())size=Math.max(size,data.getStateMachineEntry().getLogEntryProtoSerializedSize());
            if(size>committedMaximumEntryBytes)throw new IllegalStateException("Committed entry exceeds declared producer limit");
            byte[] encoded=data.getLogData().toByteArray();
            if(RatisSchemaInstallRecord.isInstall(encoded) || RatisReferencedFile.Request.isRequest(encoded) || RatisRangeImportRecord.isImport(encoded) || RatisTopologyFreezeRecord.isFreeze(encoded) || RatisParticipantRecord.isParticipant(encoded) && !RatisParticipantRecord.isFinalization(encoded) && !RatisParticipantRecord.isNegative(encoded) && !RatisParticipantRecord.isRetirement(encoded) || RatisSourceOutcomeRecord.isRecord(encoded) && encoded.length>=8 && ByteBuffer.wrap(encoded).getInt(4)==RatisSourceOutcomeRecord.Kind.CREDIT.ordinal())lane=org.apache.ratis.server.impl.CommittedApplyStateMachine.Lane.ORDINARY;
            else{
                try {var command=DistributedCommandCodec.decode(encoded);if((command.kind()==DistributedCommand.Kind.DATA && !(command.mutations().isEmpty() && command.predicates().isEmpty()) || command.kind()==DistributedCommand.Kind.COMMIT_ACCEPTED) && !snapshotHomeProgressInput(ByteBuffer.wrap(encoded)))lane=org.apache.ratis.server.impl.CommittedApplyStateMachine.Lane.ORDINARY;}
                catch(IllegalArgumentException customRecord){/* Current custom completion records use the independent protected lane. */}
            }
        }
        if(size>committedMaximumEntryBytes)throw new IllegalStateException("Committed entry exceeds declared producer limit");
        try {return new org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor(entry.getTerm(),entry.getIndex(),entry.getLogEntryBodyCase().getNumber(),lane,Math.max(committedReadWeight(committedMaximumEntryBytes),committedExecutionWeight(size)),java.security.MessageDigest.getInstance("SHA-256").digest(entry.toByteArray()),0);}
        catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}
    }
    private final NativeRangeImage.Limits limits;
    private final SimpleStateMachineStorage snapshots = new SimpleStateMachineStorage();
    private volatile SingleFileSnapshotInfo durableSnapshot;
    private final TreeMap<Long, Deferred> deferred = new TreeMap<>();
    private final TreeMap<Long, ConfigurationEntry> configurations = new TreeMap<>();
    private final TreeMap<Long, Long> pendingNoops = new TreeMap<>();
    private long deferredBytes;
    private NativeApply nativeApply;
    private final class NativeApply {
        final long index; final Deferred deferred; final RatisSchemaInstallRecord record; final RatisRangeImportRecord importRecord; final RatisReferencedFile.Request referenceRequest;final RatisTopologyFreezeRecord topologyRecord;
        ReplicatedRangeStore.TopologyFreezeGate topologyGate;DistributedStoragePool topologyPool;
        long topologyGeneration;boolean topologyReplacing;CompletableFuture<Void> topologyActions=CompletableFuture.completedFuture(null);
        ReplicatedRangeStore.SchemaInstallCaptureGate captureGate;
        DistributedStoragePool.SchemaInstallPlan plan; volatile boolean running; boolean suspended,stageReady,cancelled;
        byte[] coveredResult;
        RetainedProof retainedOwner;volatile boolean cleanupSettled,nativeCleaning;
        NativeApply(long index,Deferred deferred) {
            this.index=index; this.deferred=deferred; deferred.nativeOwner=true;
            byte[] bytes=deferred.encoded(); boolean importing=RatisRangeImportRecord.isImport(bytes),referencing=RatisReferencedFile.Request.isRequest(bytes),freezing=RatisTopologyFreezeRecord.isFreeze(bytes);
            topologyRecord=freezing ? RatisTopologyFreezeRecord.decode(bytes) : null;
            record=importing || referencing || freezing ? null : RatisSchemaInstallRecord.decode(bytes);
            importRecord=importing ? RatisRangeImportRecord.decode(bytes) : null;
            referenceRequest=referencing ? RatisReferencedFile.Request.decode(bytes) : null;
        }
        void ownRetainedProof() {
            synchronized(admissions){
                if(retainedOwner!=null)return;
                RetainedProof found=null;
                if(topologyRecord!=null && topologyHold!=null && topologyHold.record.equals(topologyRecord))found=topologyHold;
                else if(record!=null && schemaHold!=null && schemaHold.record.equals(record))found=schemaHold;
                else if(importApplyHold!=null && Arrays.equals(importApplyHold.bytes,deferred.encoded()))found=importApplyHold;
                if(found!=null){retainedOwner=found;found.nativeOwners++;}
            }
        }
        void releaseRetainedIfDrained(){
            synchronized(admissions){
                if(running || !cleanupSettled)return;
                deferred.releaseCommitted();
                if(retainedOwner==null)return;
                var owner=retainedOwner;retainedOwner=null;owner.nativeOwners--;
                if(owner instanceof TopologyHold h && h.released)releaseTopologyHold(h);
                else if(owner instanceof SchemaHold h && h.released)releaseSchemaHold(h);
                else if(owner instanceof ImportApplyHold h && h.released)releaseImportApply(h);
            }
        }
        void close() {
            if(nativeCleaning)return;
            if(topologyGate!=null){
                nativeCleaning=true;var capture=topologyGate;topologyGate=null;
                var cleanup=topologyPool.submitTopologyCleanup(capture).toCompletableFuture();
                synchronized(admissions){topologyApplyCleanup=CompletableFuture.allOf(topologyApplyCleanup,cleanup);}
                cleanup.whenComplete((unused,failure)->{if(failure==null){cleanupSettled=true;releaseRetainedIfDrained();}});
                return;
            }
            if(plan!=null)plan.close();if(captureGate!=null)captureGate.close();cleanupSettled=true;releaseRetainedIfDrained();
        }
    }
    private final Object durabilityGate = new Object();
    private Path raftRoot;
    private Path snapshotTmp;
    private Path recoveryDir;
    private record ConfigurationEntry(long term, byte[] bytes) {}
    private static final class Deferred {
        private final long term;private final byte[] encoded;private final CompletableFuture<Message> future;
        private final org.apache.ratis.server.impl.CommittedApplyStateMachine.ApplyPermit committed;boolean nativeOwner;
        Deferred(long term,byte[] encoded,CompletableFuture<Message> future,org.apache.ratis.server.impl.CommittedApplyStateMachine.ApplyPermit committed){this.term=term;this.encoded=encoded;this.future=future;this.committed=committed;}
        long term(){return term;}byte[] encoded(){return encoded;}CompletableFuture<Message> future(){return future;}
        void releaseCommitted(){if(committed!=null)committed.close();}
    }

    public RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits) {
        this(store, initialGroup, limits, command -> {});
    }

    /** Deterministic envelope validation, repeated on every replica before native apply. */
    public RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator) {
        this(store, initialGroup, limits, commandValidator, command -> {
            throw new IllegalStateException("Session epoch floors require catalogue lifetime authority");
        });
    }

    /** Authority is checked only at proposal admission; committed replay remains deterministic. */
    public RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator, Consumer<DistributedCommand> epochFloorAdmission) {
        this(store, initialGroup, limits, commandValidator, epochFloorAdmission, RetentionLimits.defaults(), null);
    }

    public RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator, Consumer<DistributedCommand> epochFloorAdmission, RetentionLimits retentionLimits) {
        this(store,initialGroup,limits,commandValidator,epochFloorAdmission,retentionLimits,null);
    }

    /** Explicit bootstrap seam: catalogue checks run only for controls, never on ordinary data replay. */
    public static RatisRangeStateMachine withSessions(ReplicatedRangeStore store,RaftGroup group,NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> validator,RatisSessionEpochs epochs,RangeDirectory directory,RetentionLimits retentionLimits) {
        return withSessions(store,group,limits,validator,epochs,directory,retentionLimits,null);
    }
    static RatisRangeStateMachine withSessions(ReplicatedRangeStore store,RaftGroup group,NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> validator,RatisSessionEpochs epochs,RangeDirectory directory,RetentionLimits retentionLimits,
            LeadershipSource leadershipSource) {
        var id=group.getGroupId().getUuid();
        var floor=epochs.floorAdmission(directory,id); var control=epochs.sessionControlAdmission(directory,id);
        var descriptor=directory.ranges().stream().filter(range -> range.groupId().equals(id)).findFirst().orElseThrow();
        var ownership=store.ownership(); var interval=ownership.interval();
        if (!ownership.owner().groupId().equals(id) || ownership.owner().epoch()!=descriptor.rangeEpoch()
                || !interval.domainId().equals(directory.domainId()) || !interval.columns().equals(directory.columns())
                || !Arrays.equals(store.schemaDigest(),directory.schemaDigest())
                || !Arrays.equals(interval.lowerInclusive(),descriptor.lowerInclusive())
                || !Arrays.equals(interval.upperExclusive(),descriptor.upperExclusive())) throw new IllegalArgumentException("Session directory differs from native range");
        var machine=new RatisRangeStateMachine(store,group,limits,validator,
                command -> { if (command.sessionControl()!=null) control.accept(command); else floor.accept(command); },
                retentionLimits,leadershipSource,true);
        machine.sessionEpochs=epochs; machine.refreshSessionAdmission(directory); return machine;
    }

    // Explicit view injection is only for ordinary in-process fixtures without a running Raft division.
    RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator, Consumer<DistributedCommand> epochFloorAdmission,
            LeadershipSource leadershipSource) {
        this(store,initialGroup,limits,commandValidator,epochFloorAdmission,RetentionLimits.defaults(),leadershipSource);
    }

    RatisRangeStateMachine(ReplicatedRangeStore store, RaftGroup initialGroup, NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator, Consumer<DistributedCommand> epochFloorAdmission,
            RetentionLimits retentionLimits, LeadershipSource leadershipSource) {
        this(store,initialGroup,limits,commandValidator,epochFloorAdmission,retentionLimits,leadershipSource,false);
    }
    private RatisRangeStateMachine(ReplicatedRangeStore store,RaftGroup initialGroup,NativeRangeImage.Limits limits,
            Consumer<DistributedCommand> commandValidator,Consumer<DistributedCommand> epochFloorAdmission,
            RetentionLimits retentionLimits,LeadershipSource leadershipSource,boolean enableSessionControls) {
        this.enableSessionControls=enableSessionControls;
        this.retentionLimits = java.util.Objects.requireNonNull(retentionLimits);
        this.leadershipSource = leadershipSource;
        this.commandValidator = java.util.Objects.requireNonNull(commandValidator);
        this.epochFloorAdmission = java.util.Objects.requireNonNull(epochFloorAdmission);
        this.store = java.util.Objects.requireNonNull(store);
        this.initialGroup = java.util.Objects.requireNonNull(initialGroup);
        this.limits = java.util.Objects.requireNonNull(limits);
        scopedTransactions=new RatisScopedTransactions(this,RatisScopedTransactions.Limits.defaults(),System::nanoTime);
        capturedSnapshots=newSnapshotPins(System::nanoTime);
    }

    @Override public SimpleStateMachineStorage getStateMachineStorage() { return snapshots; }
    @Override public SnapshotInfo getLatestSnapshot() { return recoveryReplacement?null:durableSnapshot; }

    @Override public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException {
        super.initialize(server, groupId, storage);
        getLifeCycle().transition(LifeCycle.State.STARTING);
        try(var imageMetadata=NativeRangeImage.openMetadataAdmission(snapshotMetadataAdmission)) {
            if (!initialGroup.getGroupId().equals(groupId)
                    || !groupId.getUuid().equals(store.ownership().owner().groupId())) {
                throw new IOException("Raft group differs from native ownership");
            }
            store.configureSourceOutcomeCapacity(retentionLimits.dataCount()+retentionLimits.maintenanceCount(),retentionLimits.dataBytes()+retentionLimits.maintenanceBytes());
            store.fence();
            raftRoot = storage.getStorageDir().getRoot().toPath();
            snapshotTmp = storage.getStorageDir().getTmpDir().toPath();
            recoveryDir = raftRoot.resolve("native-snapshot-recovery");
            snapshots.init(storage);
            recover(false);
            activateRecoveredOwner();
            if (role==RatisNodePlan.Role.DATA && !store.importPending() && !store.topologyBirthPending() && !store.topologyRecoveryOnly() && store.ownership().state()!=it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.State.RETIRED) store.enableScopedHistory(it.cavallium.rockserver.core.common.distributed.RangeMutationHistory.Limits.defaults());
            else if (role==RatisNodePlan.Role.CATALOGUE) store.enableCatalogueScopeCompaction();
            if (enableSessionControls && !store.importPending() && !store.topologyBirthPending()) store.enableSessionControls();
            getLifeCycle().transition(LifeCycle.State.RUNNING);recoveryReplacement=false;refreshRecoveryEligibility();
        } catch (Exception failure) { throw failRecovery(failure); }
    }

    private void recover(boolean installing) throws Exception {
        recoverStagingImages();
        SingleFileSnapshotInfo discovered = snapshots.loadLatestSnapshot();
        if (discovered == null) {
            Path directory = snapshots.getSnapshotFile(0, 0).toPath().getParent();
            try (var files = Files.list(directory)) {
                if (files.anyMatch(path -> SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(path.getFileName().toString()).matches())) {
                    throw new IOException("Snapshot files exist but could not be loaded");
                }
            }
        }
        if (discovered != null) {
            Path image = discovered.getFile().getPath();
            var manifest = validateImage(image, discovered.getTermIndex());
            force(image);
            Path md5 = MD5FileUtil.getDigestFileForFile(image.toFile()).toPath();
            if (Files.exists(md5)) force(md5);
            force(image.getParent());
            force(raftRoot);
            var a = store.applied();
            if (installing || store.pendingInstallation() || a == null || a.index() < manifest.position().index()) {
                store.fence();
                if(metadataSnapshotOwner())store.installMetadataSnapshotAdmitted(image,limits,batches->{},snapshotMetadataAdmission);else store.installSnapshotAdmitted(image,limits,batches->{},snapshotMetadataAdmission);
            } else if (a.index() == manifest.position().index() && a.term() != manifest.position().term()) {
                throw new IOException("Native marker conflicts with durable snapshot");
            }
            durableSnapshot = discovered;
        } else if (installing || store.pendingInstallation()) {
            throw new IOException("Snapshot installation has no durable image");
        }
        if (store.configuration() == null) {
            if (store.applied() != null) throw new IOException("Applied native range has no configuration");
            var cfg = RaftConfigurationProto.newBuilder();
            for (var peer : initialGroup.getPeers().stream().sorted(java.util.Comparator.comparing(peer -> peer.getId().toString())).toList()) {
                if (peer.getStartupRole() == RaftPeerRole.LISTENER) cfg.addListeners(peer.getRaftPeerProto());
                else cfg.addPeers(peer.getRaftPeerProto());
            }
            store.initializeConfiguration(cfg.build().toByteArray());
        }
        var cfg = store.configuration();
        var a = store.applied();
        if (a != null && (cfg.index() > a.index() || cfg.term() > a.term())) throw new IOException("Native configuration exceeds completed marker");
        validateConfiguration(cfg.data());updateDataVoter(cfg.data());
        if (schemaHold!=null) schemaRecovered(schemaHold.record);
        if(topologyHold!=null)topologyRecovered(topologyHold.record);
        if(topologyActivationHold!=null)topologyActivationRecovered(topologyActivationHold.record);
        setLastAppliedTermIndex(a == null ? TermIndex.INITIAL_VALUE : TermIndex.valueOf(a.term(), a.index()));
    }

    private static void validateConfiguration(byte[] bytes) throws IOException {
        if (RaftConfigurationProto.parseFrom(bytes).getPeersCount() == 0) throw new IOException("Configuration has no voters");
    }

    private NativeRangeImage.IoAdmission snapshotMetadataAdmission=NativeRangeImage.IoAdmission.UNLIMITED;
    private boolean metadataSnapshotOwner() {
        return role==RatisNodePlan.Role.DATA && (store.metadataRecoveryRegistration()!=null || store.topologyRetired() && store.topologyFreeze()!=null);
    }
    private NativeRangeImage.Manifest validateImage(Path image,TermIndex position)throws IOException{return validateImage(image,position,null);}
    private NativeRangeImage.Manifest validateImage(Path image,TermIndex position,ReplicatedRangeStore.SnapshotPin pin)throws IOException {
        try(var imageMetadata=NativeRangeImage.openMetadataAdmission(snapshotMetadataAdmission)) {
        final ReplicatedRangeStore.Configuration[] cutConfiguration = {null};
        final NativeTopologyFreeze[] cutFreeze={null};
        boolean metadata=metadataSnapshotOwner();
        NativeRangeImage.RowVisitor visitor = (section, column, key, value) -> {
            if (role!=RatisNodePlan.Role.DATA && section==NativeRangeImage.Section.STATE && key.length>=17 && (key[16]&255)>=0x27 && ((key[16]&255)<=0x2b || (key[16]&255)==0x60)) throw new IllegalArgumentException("Non-DATA image contains schema transition state");
            if (role!=RatisNodePlan.Role.DATA && section==NativeRangeImage.Section.STATE && key.length>=17 && (key[16]&255)>=0x20 && (key[16]&255)<=0x24) throw new IllegalArgumentException("Non-DATA image contains participant/snapshot state");
            if (role!=RatisNodePlan.Role.TRANSACTION_HOME && section==NativeRangeImage.Section.STATE && key.length>=17 && ((key[16]&255)==0x25 || (key[16]&255)==0x26)) throw new IllegalArgumentException("Image role rejects protected HOME recovery receipts");
            if (role==RatisNodePlan.Role.TRANSACTION_HOME && section==NativeRangeImage.Section.STATE && key.length>=17 && (key[16]&255)==0x25) {
                var receipt=it.cavallium.rockserver.core.common.distributed.HomeRecoveryReceipt.decode(value); var recovery=receipt.recovery(); var homeKey=recovery.command().mutations().getFirst().key();
                homeValues.validate(homeKey,recovery.before().encode()); homeValues.validate(homeKey,recovery.next().encode());
            }
            if (role==RatisNodePlan.Role.TRANSACTION_HOME && section==NativeRangeImage.Section.STATE && key.length>=17 && (key[16]&255)==0x26) {
                var credit=ReplicatedRangeStore.HomeRecoveryCredit.decode(value);
                var homeKey=ByteBuffer.allocate(16).putLong(credit.current().transactionId().getMostSignificantBits()).putLong(credit.current().transactionId().getLeastSignificantBits()).array();
                homeValues.validate(homeKey,credit.initial().encode()); homeValues.validate(homeKey,credit.current().encode());
            }
            if (homeValues!=null && section==NativeRangeImage.Section.USER) homeValues.validate(key,value);
            if(section==NativeRangeImage.Section.STATE && key.length==17 && key[16]==0x60)cutFreeze[0]=NativeTopologyFreeze.decode(value);
            if (section == NativeRangeImage.Section.STATE && key.length == 17 && key[16] == 1) {
                try { cutConfiguration[0] = ReplicatedRangeStore.Configuration.decode(value); }
                catch (IllegalArgumentException invalid) { throw new IOException("Invalid image configuration", invalid); }
            }
        };
        var manifest=metadata?NativeRangeImage.visitMetadata(image,limits,visitor):homeReservations ? NativeRangeImage.visitHomeRecoveryReservations(image,limits,homeCluster,homeDatabase,homeColumn,visitor) : NativeRangeImage.visit(image,limits,visitor);
        if(manifest.topologyOrigin()!=null && role!=RatisNodePlan.Role.DATA)throw new IOException("Non-DATA image contains topology provenance");
        var current=store.currentRangeRecoveryRegistration();
        if(metadata) {
            var registration=store.metadataRecoveryRegistration();var freeze=registration==null?store.topologyFreeze():registration.freeze();
            if(freeze==null || !freeze.equals(cutFreeze[0]))throw new IOException("Metadata snapshot changed qualified original source FREEZE");
            if(registration!=null) {
                var server=getServer().getNow(null);if(topologyLocalReplica==null || !sameReplicaIdentity(registration.receiver(),topologyLocalReplica) || server==null || !server.getId().toString().equals(registration.receiver().peerId()))throw new IOException("META snapshot receiver identity differs");
                try{var qualified=pin==null?store.validateMetadataRecoveryImage(image,limits):pin.validateMetadataRecoveryImage(image,limits);if(!qualified.position().equals(manifest.position()) || !Arrays.equals(qualified.sha256(),manifest.sha256()))throw new IOException("META image changed during native qualification");}
                catch(org.rocksdb.RocksDBException invalid){throw new IOException("Cannot qualify META image provenance",invalid);}
            }
        } else if(current!=null) {
            var server=getServer().getNow(null);
            if(topologyLocalReplica==null || !sameReplicaIdentity(current.receiver(),topologyLocalReplica) || server==null || !server.getId().toString().equals(current.receiver().peerId()))throw new IOException("CURRENT snapshot receiver identity differs");
            try {
                var qualified=store.validateCurrentRangeRecoveryImage(image,limits);
                if(!qualified.position().equals(manifest.position()) || !Arrays.equals(qualified.sha256(),manifest.sha256()))throw new IOException("CURRENT image changed during native qualification");
            }catch(org.rocksdb.RocksDBException invalid){throw new IOException("Cannot qualify CURRENT image provenance",invalid);}
        }
        var learner=store.topologyLearnerRegistration();
        if(!metadata && current==null && learner!=null){
            var server=getServer().getNow(null);if(topologyStorage==null || topologyLocalReplica==null || !sameReplicaIdentity(learner.receiver(),topologyLocalReplica) || server==null || !server.getId().toString().equals(learner.receiver().peerId()))throw new IOException("Learner image target identity changed");
            var permission=store.topologyLearnerImagePermission();boolean initial=manifest.position().equals(learner.imageCut()) && Arrays.equals(manifest.sha256(),learner.imageHash()) && cutConfiguration[0]!=null && Arrays.equals(cutConfiguration[0].data(),learner.sourceConfiguration());boolean authorized=permission!=null && manifest.position().equals(permission.imageCut()) && Arrays.equals(manifest.sha256(),permission.imageHash()) && cutConfiguration[0]!=null && Arrays.equals(cutConfiguration[0].data(),permission.sourceConfiguration());
            if(!initial && !authorized)throw new IOException("Learner image lacks exact forced registration/recovery permission");
            try{store.validateTopologyImageOrigin(manifest.topologyOrigin());}catch(org.rocksdb.RocksDBException invalid){throw new IOException("Cannot qualify admitted learner provenance",invalid);}
        }else if(!metadata && current==null && (manifest.topologyOrigin()!=null || store.topologyBirth()!=null)) {
            var birth=store.topologyBirth();
            if(birth==null || topologyStorage==null || topologyLocalReplica==null)throw new IOException("Topology snapshot target has no configured local birth");
            var seed=topologyStorage.topologySeed(birth.intentId());
            var server=getServer().getNow(null);
            if(seed==null || !seed.localReplica().equals(topologyLocalReplica) || server==null || !server.getId().toString().equals(topologyLocalReplica.peerId()))throw new IOException("Topology snapshot target local replica changed");
            try { store.validateTopologyImageOrigin(manifest.topologyOrigin()); }
            catch(org.rocksdb.RocksDBException invalid) { throw new IOException("Cannot qualify local topology image provenance",invalid); }
        }
        var cfg = cutConfiguration[0];
        if (cfg == null || cfg.index() > manifest.position().index() || cfg.term() > manifest.position().term()) {
            throw new IOException("Snapshot lacks configuration at its cut");
        }
        validateConfiguration(cfg.data());updateDataVoter(cfg.data());
        boolean currentSchema=manifest.interval().equals(store.ownership().interval())
                && Arrays.equals(manifest.schemaDigest(),store.schemaDigest())
                && manifest.deterministicMergeColumns().equals(store.deterministicMergeColumns());
        boolean precedingSchema=!currentSchema && !metadata && store.qualifiedHistoricalImage(manifest);
        if (!manifest.owner().equals(store.ownership().owner())
                || !currentSchema && !precedingSchema
                || manifest.position().term() != position.getTerm() || manifest.position().index() != position.getIndex()) {
            throw new IOException("Snapshot identity does not match native range/filename");
        }
        var saved = MD5FileUtil.readStoredMd5ForFile(image.toFile());
        if (saved != null && !saved.equals(MD5FileUtil.computeMd5ForFile(image.toFile()))) {
            throw new IOException("Snapshot MD5 mismatch");
        }
        return manifest;
        }
    }

    private static void force(Path path) throws IOException {
        try (var channel = FileChannel.open(path, Files.isDirectory(path) ? StandardOpenOption.READ : StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    @Override public long takeSnapshot() throws IOException {
        // A retired parent's shared USER rows may already belong to its children.
        boolean metadata=metadataSnapshotOwner();
        if(store.topologyBirthPending() || store.topologyRecoveryOnly() || store.metadataRecoveryPending())return -1;
        if(metadata){var freeze=store.topologyFreeze();var cut=store.applied();if(freeze==null || cut==null || cut.index()<=freeze.position().index())return -1;}
        var recovery=diskRunway==null?null:diskRunway.reserveRecovery(diskPool,Math.addExact(2*limits.maxImageBytes(),65536));
        long published;
        try (var imageMetadata=NativeRangeImage.openMetadataAdmission(snapshotMetadataAdmission);var pin = metadata?store.captureMetadataForRecovery():snapshotPin()) {
            if (pin.position() == null) return -1;
            var position = TermIndex.valueOf(pin.position().term(), pin.position().index());
            Path file = snapshots.getSnapshotFile(position.getTerm(), position.getIndex()).toPath();
            if (!Files.exists(file)){if(metadata)NativeRangeImage.writeMetadataAdmitted(file,pin,pin.interval(),limits,NativeRangeImage.IoAdmission.UNLIMITED);else NativeRangeImage.write(file, pin, pin.interval(), limits);}
            validateImage(file,position,pin);
            var md5 = MD5FileUtil.computeMd5ForFile(file.toFile());
            MD5FileUtil.saveMD5File(file.toFile(), md5);
            force(file);
            force(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath());
            force(file.getParent());
            force(raftRoot);
            var info = new SingleFileSnapshotInfo(new FileInfo(file, md5), position);
            synchronized (gate) {
                requireHealthy();
                if (durableSnapshot == null || position.getIndex() > durableSnapshot.getIndex()) durableSnapshot = info;
                snapshots.updateLatestSnapshot(info);
            }
            published=position.getIndex();
        } catch (Exception failure) { throw new IOException("Could not publish native snapshot", failure); }
        finally {if(recovery!=null)recovery.actualFinished();}
        refreshRecoveryAfterNativeCompletion();return published;
    }

    private ReplicatedRangeStore.SnapshotPin snapshotPin() throws Exception {
        synchronized (gate) { requireHealthy(); return store.captureForRecovery(); }
    }

    private byte[] readablePoint(UUID column,byte[] key) throws Exception {
        synchronized (gate) { requireHealthy();requireUserReplica(); return store.readCurrentPoint(column,key); }
    }

    @Override public void pause() {
        recoveryReplacement=true;
        admissionGeneration.incrementAndGet(); invalidateProposalCandidates();
        try(var imageMetadata=NativeRangeImage.openMetadataAdmission(snapshotMetadataAdmission)) {
            synchronized (gate) {
                getLifeCycle().transition(LifeCycle.State.PAUSING);
                store.fence();
                getLifeCycle().transition(LifeCycle.State.PAUSED);
                if(committedGroup!=null && nativeApply!=null){nativeApply.suspended=true;nativeApply.stageReady=false;}
            }
            if(committedGroup!=null)committedGroup.wakeup();
            capturedSnapshots.invalidate();
            awaitNativeReaders(java.time.Duration.ofSeconds(10));
            preserveInstallationImages();
        } catch (Exception failure) {
            synchronized (gate) { storageFailure = failure; store.fence(); }
            failDeferred(failure);
            throw new IllegalStateException("Snapshot pause failed; native ownership remains fenced", failure);
        }
    }

    /** Must run before RaftServer.close(), whose updater waits for outstanding apply futures. */
    public void beginShutdown() {
        prepareRecoveryReplacement();recoveryReplacement=true;
        if(committedGroup!=null)committedGroup.stop();
        admissionGeneration.incrementAndGet(); invalidateProposalCandidates();
        try { scopedTransactions.close(); } catch (Exception failure) { storageFailure=failure; }
        try { capturedSnapshots.close(); } catch (Exception failure) { storageFailure=failure; }
        IOException failure = new IOException("Range is shutting down");
        NativeApply abandoned=null;
        synchronized (gate) {
            storageFailure = failure; store.fence();
            if (nativeApply!=null) { nativeApply.cancelled=true; if (!nativeApply.running) abandoned=nativeApply; nativeApply=null; }
        }
        if (abandoned!=null) abandoned.close();
        failDeferred(failure);
    }

    /** Pool owners must call after shutting Ratis down and before closing any DB/CF handles.
     * Failure leaves readers and native ownership intact; the pool must remain open. */
    public void awaitNativeReaders(java.time.Duration timeout) throws IOException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (store.activeSnapshotPins() != 0) {
            if (System.nanoTime() >= deadline || Thread.currentThread().isInterrupted()) {
                throw new IOException("Native snapshot readers remain active: " + store.activeSnapshotPins());
            }
            java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
        }
    }

    @Override public void close() { beginShutdown(); }

    private record Image(Path file, TermIndex position, NativeRangeImage.Manifest manifest) {}

    private Image image(Path file) throws IOException {
        var position = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(file.toFile());
        return new Image(file, position, validateImage(file, position));
    }

    private void syncImage(Image image) throws IOException {
        force(image.file());
        var md5 = MD5FileUtil.computeMd5ForFile(image.file().toFile());
        MD5FileUtil.saveMD5File(image.file().toFile(), md5);
        force(MD5FileUtil.getDigestFileForFile(image.file().toFile()).toPath());
        force(image.file().getParent());
    }

    private void retainImage(Image image) throws IOException {
        Files.createDirectories(recoveryDir);
        Path target = recoveryDir.resolve(image.file().getFileName());
        if (!Files.exists(target)) Files.createLink(target, image.file());
        else if (!Arrays.equals(image(target).manifest().sha256(), image.manifest().sha256())) {
            throw new IOException("Conflicting snapshot recovery image at " + image.position());
        }
        syncImage(image(target));
        force(recoveryDir);
        force(raftRoot);
    }

    private java.util.List<Image> images(Path directory, boolean staging) throws IOException {
        var result = new java.util.ArrayList<Image>();
        if (!Files.exists(directory)) return result;
        try (var files = Files.list(directory)) {
            for (var file : files.toList()) {
                if (!SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(file.getFileName().toString()).matches()) continue;
                // A sidecar is written only after the last chunk. Partial transfers remain inert.
                if (staging && !Files.exists(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath())) continue;
                try { result.add(image(file)); }
                catch (IOException | RuntimeException invalid) {
                    if (!staging) throw new IOException("Invalid durable snapshot " + file, invalid);
                }
            }
        }
        return result;
    }

    private java.util.List<Image> stagedImages() throws IOException {
        var result = new java.util.ArrayList<Image>();
        if (!Files.exists(snapshotTmp)) return result;
        try (var directories = Files.list(snapshotTmp)) {
            for (var directory : directories.toList()) {
                if (Files.isDirectory(directory) && directory.getFileName().toString().startsWith("snapshot-")) {
                    result.addAll(images(directory, true));
                }
            }
        }
        return result;
    }

    /** Called before stock Ratis moves/deletes the old state-machine directory. */
    private void preserveInstallationImages() throws IOException {
        synchronized (durabilityGate) {
            Path published = snapshots.getSnapshotFile(0, 0).toPath().getParent();
            for (var image : images(published, false)) retainImage(image);
            for (var image : stagedImages()) {
                syncImage(image);
                force(snapshotTmp);
                retainImage(image);
            }
            force(raftRoot);
        }
    }

    private void recoverStagingImages() throws IOException {
        synchronized (durabilityGate) {
            Path published = snapshots.getSnapshotFile(0, 0).toPath().getParent();
            // Corrupt published snapshots are repair failures, never skipped for an older image.
            var candidates = new java.util.ArrayList<>(images(published, false));
            candidates.addAll(images(recoveryDir, false));
            for (var staged : stagedImages()) { syncImage(staged); retainImage(staged); candidates.add(staged); }
            Image latest = null;
            for (var candidate : candidates) {
                if (latest == null || candidate.position().getIndex() > latest.position().getIndex()) latest = candidate;
                else if (candidate.position().getIndex() == latest.position().getIndex()
                        && !Arrays.equals(candidate.manifest().sha256(), latest.manifest().sha256())) {
                    throw new IOException("Snapshot recovery cuts disagree");
                }
            }
            Files.createDirectories(published);
            if (latest != null) {
                Path target = published.resolve(latest.file().getFileName());
                if (!Files.exists(target)) Files.createLink(target, latest.file());
                syncImage(image(target));
            }
            force(published);
            force(raftRoot);
        }
    }

    /** Final InstallSnapshot SUCCESS interceptor barrier; native reload may still be in progress. */
    public void syncReceivedSnapshot(long term, long index) throws IOException {
        try {
            synchronized (durabilityGate) {
                Path received = snapshots.getSnapshotFile(term, index).toPath();
                var image = image(received);
                syncImage(image);
                retainImage(image);
                force(received.getParent());
                force(raftRoot);
            }
        } catch (Exception failure) {
            synchronized (gate) { storageFailure = failure; store.fence(); }
            failDeferred(failure);
            throw failure instanceof IOException io ? io : new IOException("Received snapshot is not durable", failure);
        }
    }

    private record Completion(CompletableFuture<Message> future, Message result) {}

    /** Drop only volatile native admission before image replacement; committed suffix bytes remain owned. */
    private void quiesceTopologyReplacement()throws IOException{
        final NativeApply work;final CompletableFuture<Void> actions;final CompletableFuture<Void> abandoned;
        synchronized(gate){getLifeCycle().transition(LifeCycle.State.STARTING);work=nativeApply!=null && nativeApply.topologyRecord!=null ? nativeApply : null;if(work!=null){work.topologyReplacing=true;work.topologyGeneration++;work.suspended=true;work.stageReady=false;}actions=work==null ? CompletableFuture.completedFuture(null) : work.topologyActions;}
        synchronized(admissions){var hold=topologyHold;abandoned=hold!=null && !hold.submitted ? hold.cleanup.copy() : CompletableFuture.completedFuture(null);}
        try{
            // Neither native mutation nor readiness callbacks may race replacement. No monitor is held while waiting.
            CompletableFuture.allOf(actions,abandoned).get(10,java.util.concurrent.TimeUnit.SECONDS);
            final ReplicatedRangeStore.TopologyFreezeGate leaderGate;
            synchronized(admissions){var hold=topologyHold;leaderGate=hold!=null && hold.submitted ? hold.nativeGate : null;if(leaderGate!=null){hold.nativeGate=null;hold.nativeReady=false;hold.invalidated=true;}}
            final ReplicatedRangeStore.TopologyFreezeGate followerGate;
            synchronized(gate){followerGate=work==null ? null : work.topologyGate;if(work!=null)work.topologyGate=null;}
            var cleanups=new ArrayList<CompletableFuture<Void>>();
            if(leaderGate!=null)cleanups.add(topologyStorage.submitTopologyCleanup(leaderGate).toCompletableFuture());
            if(followerGate!=null)cleanups.add(topologyStorage.submitTopologyCleanup(followerGate).toCompletableFuture());
            synchronized(admissions){cleanups.add(topologyApplyCleanup);}
            CompletableFuture.allOf(cleanups.toArray(CompletableFuture[]::new)).get(10,java.util.concurrent.TimeUnit.SECONDS);
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Topology replacement preparation interrupted",interrupted);}
        catch(java.util.concurrent.ExecutionException|java.util.concurrent.TimeoutException failure){throw new IOException("Topology replacement preparation did not drain",failure);}
    }
    @Override public void reinitialize() throws IOException {
        try(var imageMetadata=NativeRangeImage.openMetadataAdmission(snapshotMetadataAdmission)) {
        if(recoveryEligibility.snapshot().mode()!=org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING)prepareRecoveryReplacement();
        recoveryReplacement=true;
        admissionGeneration.incrementAndGet(); invalidateProposalCandidates(); store.invalidateScopedSnapshots();
        quiesceTopologyReplacement();
        var completions = new java.util.ArrayList<Completion>();
        var coveredOperations = new java.util.ArrayList<OperationId>();
        IOException failure = null; NativeApply covered=null; boolean closeCovered=false;
        synchronized (gate) {
            try {
                if (storageFailure != null) throw new IOException("Unhealthy range cannot reload", storageFailure);
                recover(true);
                var a = store.applied();
                for (var entry : deferred.headMap(a.index(), true).entrySet()) {
                    var pending = entry.getValue();
                    if (pending.term() > a.term() || entry.getKey() == a.index() && pending.term() != a.term()) {
                        throw new IOException("Deferred term conflicts with installed image");
                    }
                    var recovered=RatisReferencedFile.Request.isRequest(pending.encoded())
                            ? new AppliedRecord(null,referenceProof(RatisReferencedFile.Request.decode(pending.encoded())))
                            : recoveredRecord(pending.encoded());
                    byte[] result=recovered.result();
                    if (result == null) throw new IOException("Installed image omitted deferred receipt");
                    synchronized(admissions) {
                        if(importApplyHold!=null && Arrays.equals(importApplyHold.bytes,pending.encoded()))releaseImportApply(importApplyHold);
                    }
                    if(!pending.nativeOwner)pending.releaseCommitted();
                    completions.add(new Completion(pending.future(), Message.valueOf(ByteString.copyFrom(result))));
                    coveredOperations.add(recovered.operation());
                }
                for (var entry : pendingNoops.headMap(a.index(), true).entrySet()) {
                    if (entry.getValue() > a.term() || entry.getKey() == a.index() && entry.getValue() != a.term()) {
                        throw new IOException("Deferred metadata term conflicts with installed image");
                    }
                }
                deferred.headMap(a.index(), true).values().forEach(pending -> deferredBytes -= pending.encoded().length);
                deferred.headMap(a.index(), true).clear();
                pendingNoops.headMap(a.index(), true).clear();
                configurations.headMap(a.index(), true).clear(); schemaConfigurationPending=!configurations.isEmpty();
                activateRecoveredOwner();
                if (role==RatisNodePlan.Role.DATA && !store.importPending() && !store.topologyBirthPending() && !store.topologyRecoveryOnly() && store.ownership().state()!=it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.State.RETIRED) store.enableScopedHistory(it.cavallium.rockserver.core.common.distributed.RangeMutationHistory.Limits.defaults());
                else if (role==RatisNodePlan.Role.CATALOGUE) store.enableCatalogueScopeCompaction();
                if (enableSessionControls && !store.importPending() && !store.topologyBirthPending()) store.enableSessionControls();
                var verifiedAdmissions=verifiedNativeAdmissionOutcomes();
                synchronized (admissions) {
                    for (var terminal:verifiedAdmissions.terminalScopes()) if (admissions.get(terminal.id)==terminal) retirePreparationScope(terminal);
                    for (var verified:verifiedAdmissions.settled()) if (admissions.get(verified.id)==verified) {
                        if (verified.snapshot!=null) materializeSnapshotCharge(verified.snapshot.fence());
                        settleAdmission(verified);
                    }
                    for (var id : coveredOperations) {
                        var admission = admissions.get(id);
                        if (admission != null) settleAdmission(admission);
                    }
                }
                var installed=store.schemaInstallation();
                if (installed!=null) refreshSessionAdmission(installed.definition().targetDirectory());
                if (nativeApply!=null && nativeApply.index<=a.index()) {
                    var original=nativeApplyProof(nativeApply);
                    if (original==null) throw new IOException("Covered INSTALL lacks original native proof");
                    covered=nativeApply; covered.coveredResult=original; closeCovered=!covered.running; nativeApply=null;
                }
                if(nativeApply!=null && nativeApply.topologyRecord!=null)nativeApply.topologyReplacing=false;
                getLifeCycle().transition(LifeCycle.State.RUNNING);
                if(committedGroup!=null)committedGroup.resume();
            } catch (Exception problem) { failure = failRecovery(problem); }
        }
        if (failure != null) {
            // Nothing succeeds until every covered receipt has been verified.
            failDeferred(failure);
            for (var completion : completions) completion.future().completeExceptionally(failure);
            throw failure;
        }
        recoveryReplacement=false;refreshRecoveryEligibility();
        for (var completion : completions) completion.future().complete(completion.result());
        NativeApply resume;
        synchronized (gate) { resume=nativeApply; }
        if (covered!=null && closeCovered) covered.close();
        if (resume!=null && resume.suspended) scheduleNativeApply(resume,resume.stageReady);
        }
    }

    private IOException failRecovery(Exception failure) {
        storageFailure = failure;
        store.fence();
        return failure instanceof IOException io ? io : new IOException("Native range recovery failed", failure);
    }

    private void failDeferred(Throwable failure) {
        java.util.List<Deferred> abandoned;
        synchronized (gate) {
            abandoned = java.util.List.copyOf(deferred.values());
            deferred.clear(); deferredBytes = 0;
        }
        for (var pending : abandoned) {if(!pending.nativeOwner)pending.releaseCommitted();pending.future().completeExceptionally(failure);}
    }

    private void advance(long term, long index) {
        if (index > getLastAppliedTermIndex().getIndex()) updateLastAppliedTermIndex(term, index);
        else if (index == getLastAppliedTermIndex().getIndex() && term != getLastAppliedTermIndex().getTerm()) {
            throw new IllegalStateException("Conflicting applied term");
        }
    }

    /** Classify only the configured snapshot HOME's existing-row CAS, without copying status payloads. */
    private boolean snapshotHomeProgressInput(ByteBuffer wire) {
        if (!snapshotHomeConfigured || role!=RatisNodePlan.Role.SNAPSHOT_HOME) return false;
        try {
            if (wire.remaining()<DistributedCommandCodec.HEADER_BYTES
                    || wire.get(DistributedCommandCodec.KIND_OFFSET)!=1 || wire.getLong(DistributedCommandCodec.ACK_OFFSET)!=-1 || wire.getInt(DistributedCommandCodec.MUTATION_COUNT_OFFSET)!=1 || wire.getInt(DistributedCommandCodec.PREDICATE_COUNT_OFFSET)!=1) return false;
            wire.position(DistributedCommandCodec.HEADER_BYTES);
            if (wire.get()!=1 || wire.get()!=0 || wire.getLong()!=homeColumn.getMostSignificantBits() || wire.getLong()!=homeColumn.getLeastSignificantBits() || wire.getInt()!=16) return false;
            long keyHigh=wire.getLong(),keyLow=wire.getLong(); int valueBytes=wire.getInt();
            if (valueBytes<SnapshotFenceStatus.HEADER_BYTES || valueBytes>SnapshotFenceStatus.MAX_BYTES || valueBytes>wire.remaining()) return false;
            wire.position(wire.position()+valueBytes);
            if (wire.get()!=1 || wire.getLong()!=homeColumn.getMostSignificantBits() || wire.getLong()!=homeColumn.getLeastSignificantBits() || wire.getInt()!=16
                    || wire.getLong()!=keyHigh || wire.getLong()!=keyLow) return false;
            int expected=wire.getInt();
            // NULL is fresh registration and must consume ordinary capacity. Full native validation
            // still checks immutable fence, exact predecessor and authoritative current-row CAS.
            return expected>=SnapshotFenceStatus.HEADER_BYTES && expected<=SnapshotFenceStatus.MAX_BYTES && expected==wire.remaining();
        } catch (java.nio.BufferUnderflowException | IllegalArgumentException malformed) { return false; }
    }
    @Override public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        var content=request.getMessage().getContent();
        // The bounded prefix classifies ordinary command/participant inputs before their full copy/decode.
        var prefix=content.substring(0,Math.min(content.size(),DistributedCommandCodec.HEADER_BYTES)).toByteArray();
        RatisNodeResourceBudget.Kind kind=null;
        if (RatisParticipantRecord.isParticipant(prefix)) {
            kind=RatisParticipantRecord.isFinalization(prefix) || RatisParticipantRecord.isNegative(prefix) || RatisParticipantRecord.isRetirement(prefix)
                    ? RatisNodeResourceBudget.Kind.PROTECTED : RatisNodeResourceBudget.Kind.ORDINARY;
        } else if (RatisSnapshotRecord.isSnapshot(prefix) || HomeRecovery.isRecovery(prefix) || RatisHomeRetirementRecord.isRetirement(prefix)
                || RatisSnapshotRetirementRecord.isRetirement(prefix) || RatisSnapshotHomeRetirementRecord.isRetirement(prefix)) {
            kind=RatisNodeResourceBudget.Kind.PROTECTED;
        } else if (prefix.length>=DistributedCommandCodec.HEADER_BYTES && (prefix[DistributedCommandCodec.KIND_OFFSET]&255)>=1 && (prefix[DistributedCommandCodec.KIND_OFFSET]&255)<=5) {
            int semantic=prefix[DistributedCommandCodec.KIND_OFFSET]&255;
            boolean maintenance=semantic==2 || semantic==3 || semantic==1 && java.nio.ByteBuffer.wrap(prefix).getInt(DistributedCommandCodec.MUTATION_COUNT_OFFSET)==0 && java.nio.ByteBuffer.wrap(prefix).getInt(DistributedCommandCodec.PREDICATE_COUNT_OFFSET)==0;
            kind=maintenance || snapshotHomeProgressInput(content.asReadOnlyByteBuffer()) ? RatisNodeResourceBudget.Kind.PROTECTED : RatisNodeResourceBudget.Kind.ORDINARY;
        }
        if(kind==null && (RatisRangeImportRecord.isImport(prefix) || RatisReferencedFile.Request.isRequest(prefix) || RatisTopologyFreezeRecord.isFreeze(prefix)
                || RatisTopologyActivationRecord.isActivation(prefix) || prefix.length>=4 && ByteBuffer.wrap(prefix).getInt()==RatisTopologyRelocationRecord.MAGIC
                || RatisSchemaFenceRecord.isFence(prefix) || RatisSchemaInstallRecord.isInstall(prefix) || RatisSchemaActivateRecord.isActivate(prefix) || RatisSourceOutcomeRecord.isRecord(prefix)))kind=RatisNodeResourceBudget.Kind.PROTECTED;
        var lease=kind==null ? null : reserveNodeResources(kind,256L+4L*content.size(),1);
        boolean retained=false;
        try {
            var transaction=startTransactionOwned(request);
            if (lease!=null && transaction.getStateMachineContext() instanceof Admission admission) {
                synchronized(admissions) { admission.resourceLeases.add(lease); }
                retained=true;
            } else if(lease!=null && transaction.getStateMachineContext() instanceof RetainedProof hold) {
                synchronized(admissions){hold.inputGraphs.add(lease);}retained=true;
            }
            return transaction;
        } finally { if (!retained && lease!=null) lease.close(); }
    }
    private TransactionContext startTransactionOwned(RaftClientRequest request) throws IOException {
        var transaction = super.startTransaction(request);
        try {
            requireHealthy();
            if(store.topologyRecoveryOnly())throw new IOException("Topology historical owner is replay-only");
            long generation = admissionGeneration.get();
            Leadership leadership = leadership();
            if (!leadership.ready()) throw new IllegalStateException("Leader is not ready for admission");
            byte[] encoded = request.getMessage().getContent().toByteArray(); requireRoleRecord(encoded);
            if(RatisTopologyRelocationRecord.isRecord(encoded))return startRelocation(transaction,RatisTopologyRelocationRecord.decode(encoded));
            if(RatisSourceOutcomeRecord.isRecord(encoded)){if(!initialGroup.getGroupId().equals(request.getRaftGroupId()))throw new IOException("Source record crossed Raft group");return startSourceOutcome(transaction,RatisSourceOutcomeRecord.decode(encoded));}
            if(RatisTopologyActivationRecord.isActivation(encoded)){if(!initialGroup.getGroupId().equals(request.getRaftGroupId()))throw new IOException("Topology activation request crossed group");return startTopologyActivation(transaction,RatisTopologyActivationRecord.decode(encoded),generation,leadership);}
            if(RatisTopologyFreezeRecord.isFreeze(encoded))return startTopologyFreeze(transaction,RatisTopologyFreezeRecord.decode(encoded),generation,leadership);
            if (RatisRangeImportRecord.isImport(encoded)) {
                if(!initialGroup.getGroupId().equals(request.getRaftGroupId()))throw new IOException("Native import request addresses another group");
                return startImportApply(transaction,RatisRangeImportRecord.decode(encoded),generation,leadership);
            }
            if (RatisReferencedFile.Request.isRequest(encoded)) {
                var referenced=RatisReferencedFile.Request.decode(encoded);
                if(!initialGroup.getGroupId().equals(request.getRaftGroupId()) || !request.getClientId().getUuid().equals(referenced.clientId()) || request.getCallId()!=referenced.callId())throw new IOException("Reference proposal client/call/group binding differs");
                requireImportProposal(referenced); return startImportApply(transaction,referenced,generation,leadership);
            }
            if (RatisSchemaFenceRecord.isFence(encoded)) return startSchemaFence(transaction,RatisSchemaFenceRecord.decode(encoded),generation,leadership);
            if (RatisSchemaInstallRecord.isInstall(encoded)) return startSchemaLifecycle(transaction,RatisSchemaInstallRecord.decode(encoded),generation,leadership);
            if (RatisSchemaActivateRecord.isActivate(encoded)) return startSchemaLifecycle(transaction,RatisSchemaActivateRecord.decode(encoded),generation,leadership);
            if (RatisSnapshotHomeRetirementRecord.isRetirement(encoded)) return startSnapshotHomeRetirement(transaction,RatisSnapshotHomeRetirementRecord.decode(encoded),encoded,generation,leadership);
            if (RatisSnapshotRetirementRecord.isRetirement(encoded)) return startSnapshotRetirement(transaction,RatisSnapshotRetirementRecord.decode(encoded),encoded,generation,leadership);
            if (RatisHomeRetirementRecord.isRetirement(encoded)) return startHomeRetirement(transaction,RatisHomeRetirementRecord.decode(encoded),encoded,generation,leadership);
            if (HomeRecovery.isRecovery(encoded)) return startHomeRecovery(transaction,encoded,generation,leadership);
            if (RatisSnapshotRecord.isSnapshot(encoded)) return startSnapshot(transaction,encoded,generation,leadership);
            if (RatisParticipantRecord.isRetirement(encoded)) return startRetirement(transaction,RatisParticipantRecord.decodeRetirement(encoded),encoded,generation,leadership);
            if (RatisParticipantRecord.isParticipant(encoded)) return RatisParticipantRecord.isFinalization(encoded) ? startFinalization(transaction,encoded,generation,leadership) : startParticipant(transaction,encoded,generation,leadership);
            var command = DistributedCommandCodec.decode(encoded);
            if (command.transactionRejection()) throw new IllegalArgumentException("Internal rejection records cannot be submitted by clients");
            // Fresh publication metadata/owner reads belong to the independently bounded preparation lane.
            if(!requiresCataloguePreparation(command))commandValidator.accept(command);
            if(role==RatisNodePlan.Role.DATA && store.topologyFreeze()!=null && command.sessionControl()!=null && command.sessionControl().binding()) throw new IOException("Sealed topology parent cannot acquire session grants");
            store.validate(command);
            var metadataOwner=role==RatisNodePlan.Role.DATA && store.metadataTopologyControl(command) ? checkCdcMetadataOwner() : null;
            byte[] digest = command.payloadDigest();
            boolean fencing=role==RatisNodePlan.Role.CATALOGUE && RatisSchemaAdministration.isFencing(command);
            var rejection=command.acceptance()==null ? null : DistributedCommand.rejectAcceptedTransaction(command);
            var rejectionData=rejection==null ? null : ByteString.copyFrom(DistributedCommandCodec.encode(rejection));
            var spans=new ArrayList<TransactionScope.Span>(command.mutations().size());
            if (role==RatisNodePlan.Role.CATALOGUE) {
                var scope=store.commandAccessScope(command);
                if (scope!=null) spans.addAll(scope.spans());
            } else for (var mutation:command.mutations()) spans.add(mutation.kind()==Mutation.Kind.DELETE_RANGE
                    ? TransactionScope.Span.range(mutation.columnId(),mutation.key(),mutation.value()) : TransactionScope.Span.point(mutation.columnId(),mutation.key()));
            var footprint=proposalFootprints.prepare(spans);
            // Native receipt reads stay outside the short admission lock. If apply
            // advanced while reading, retry validation before acquiring ownership.
            for (int attempt = 0; attempt < 3; attempt++) {
                var originalEstimate = store.estimateReceiptRetention(command,enableSessionControls);
                var rejectionEstimate=rejection==null ? originalEstimate : store.estimateReceiptRetention(rejection,enableSessionControls);
                var namespaceEstimate=command.acknowledgedThrough()>=0 || command.minimumSessionEpoch()>=0 ? store.estimateSessionMetadata(command) : null;
                var homeCredit=homeReservations && originalEstimate.newReceipt() && !command.mutations().isEmpty() ? store.estimateHomeRegistration(command) : null;
                var observed = originalEstimate.usage().appliedPosition();
                if (namespaceEstimate!=null && !java.util.Objects.equals(observed,namespaceEstimate.position())) continue;
                if (homeCredit!=null && !java.util.Objects.equals(observed,homeCredit.capacity().position())) continue;
                if (!java.util.Objects.equals(observed,rejectionEstimate.usage().appliedPosition()) || originalEstimate.newReceipt()!=rejectionEstimate.newReceipt()) continue;
                if (observed == null || observed.term() != leadership.term()) {
                    throw new IllegalStateException("Current-term startup prefix has not completed native apply");
                }
                // Previously committed outcomes and expired identities cannot advance the fence.
                if (originalEstimate.newReceipt() && command.minimumSessionEpoch() >= 0 && !store.canonicalStaleSchemaControl(command)) epochFloorAdmission.accept(command);
                synchronized (admissions) {
                    var estimate=originalEstimate;
                    requireHealthy();
                    if(role==RatisNodePlan.Role.DATA && estimate.newReceipt() && (!command.mutations().isEmpty() || !command.predicates().isEmpty() || command.acceptance()!=null || command.sessionControl()!=null && command.sessionControl().binding()))requireUserReplica();
                    if (generation != admissionGeneration.get()) throw new IllegalStateException("Admission leadership generation changed");
                    if (!store.admissionOpen() && metadataOwner==null) throw new IllegalStateException("Native range admission is closed");
                    if(metadataOwner!=null && (getLifeCycleState()!=LifeCycle.State.RUNNING || schemaConfigurationPending || metadataOwner.stamp()!=schemaConfigurationStamp || metadataOwner.generation()!=generation || metadataOwner.term()!=leadership.term())) throw new IOException("Topology metadata control lost configuration/owner");
                    if (!java.util.Objects.equals(observed, store.applied())) continue;
                    reconcileOlderProposalTerms(leadership.term());
                    if (sourceIdentityBusy(command.operationId()) || retirementIdentityBusy(command.operationId()) || snapshotControlCharges.containsKey(command.operationId()) || finalizationOwners.containsKey(command.operationId()) || participantOwners.containsKey(command.operationId())) throw new IllegalStateException("Operation identity belongs to participant preparation");
                    var known=admissions.get(command.operationId());
                    if (known!=null && !estimate.newReceipt()) settleAdmission(known);
                    var admission = admissions.get(command.operationId());
                    var importOwner=estimate.newReceipt() && requiresCataloguePreparation(command) ? importPublicationHolds.get(command.operationId()) : null;
                    if(estimate.newReceipt() && requiresCataloguePreparation(command))requireImportPublication(importOwner,command,leadership,generation);
                    var fencingOwner=fencing && estimate.newReceipt() ? schemaHold : null;
                    if (fencing && estimate.newReceipt()) requireFencingOwner(fencingOwner,command,leadership,generation);
                    if (admission != null) {
                        if (!Arrays.equals(admission.digest, digest)) {
                            throw new IllegalArgumentException("Operation identity reused with different payload");
                        }
                        if (admission.term != leadership.term() || admission.generation != generation) {
                            throw new IllegalStateException("Earlier admission is still settling; retry the same identity");
                        }
                        admission.owners++;
                    } else {
                        if (homeOwners.containsKey(command.operationId())) throw new IllegalStateException("Identity belongs to protected HOME recovery");
                        if (command.acceptance()==null && (preparingCommits.containsKey(command.operationId()) || preparedCommits.containsKey(command.operationId()))) throw new IllegalStateException("Operation identity is reserved by transaction preparation");
                        var accepted=command.acceptance()==null || !estimate.newReceipt() ? null : preparedCommits.get(command.operationId());
                        boolean reject=false;
                        if (command.acceptance()!=null && estimate.newReceipt()) {
                            if (preparingCommits.containsKey(command.operationId())) throw new IllegalStateException("Operation identity is still being prepared");
                            if (accepted!=null && !Arrays.equals(accepted.digest,digest)) throw new IllegalArgumentException("Operation identity reused with different prepared payload");
                            if (accepted!=null && !accepted.authorized()) { accepted.close(); accepted=null; }
                            reject=accepted==null;
                            if (reject) {
                                estimate=rejectionEstimate;
                            }
                        }
                        if (estimate.newReceipt() && (command.acceptance()==null || reject) && estimate.maintenance() && proposalFootprints.stats().candidates()>0) throw new IllegalStateException("Transaction reservations hold session metadata stable");
                        boolean maintenance = estimate.newReceipt() && estimate.maintenance();
                        boolean protectedControl = estimate.maintenance() && command.mutations().isEmpty() && command.predicates().isEmpty()
                                || snapshotHomeProgressInput(ByteBuffer.wrap(encoded));
                        if (protectedControl ? sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots >= 16
                                : dataAdmissions >= 1024 || dataProposalBytes + encoded.length > 16L * 1024 * 1024) {
                            throw new IllegalStateException("Range proposal budget exhausted");
                        }
                        if (homeCredit!=null && (!retentionFits(homeCredit.capacity().effectiveCount(),reservedHomeCount,homeCredit.additionalCount(),ReplicatedRangeStore.MAX_HOME_RECOVERIES)
                                || !retentionFits(homeCredit.capacity().effectiveBytes(),reservedHomeBytes,homeCredit.additionalBytes(),ReplicatedRangeStore.MAX_HOME_RECOVERY_BYTES))) throw new IllegalStateException("HOME registration future recovery capacity exhausted");
                        if (maintenance && maintenanceReservation != null) throw new IllegalStateException("Receipt maintenance is already in flight");
                        long countLimit = retentionLimits.dataCount() + (maintenance ? retentionLimits.maintenanceCount() : 0);
                        long byteLimit = retentionLimits.dataBytes() + (maintenance ? retentionLimits.maintenanceBytes() : 0);
                        if (!retentionFits(estimate.usage().retainedCount(),reservedReceiptCount,estimate.additionalCount(),countLimit)
                                || !retentionFits(estimate.usage().retainedBytes(),reservedReceiptBytes,estimate.additionalBytes(),byteLimit)) {
                            throw new IllegalStateException("Retained receipt budget exhausted; acknowledge outcomes or expire epochs");
                        }
                        var namespace=reject ? null : reserveSessionNamespace(command,namespaceEstimate);
                        var lease=importOwner==null && fencingOwner==null && !reject && accepted==null && estimate.newReceipt() && !footprint.isEmpty() ? proposalFootprints.registerOrdinary(footprint,leadership.term(),generation) : null;
                        if (importOwner==null && fencingOwner==null && !reject && accepted==null && estimate.newReceipt() && !footprint.isEmpty() && lease==null) {
                            if (namespace!=null) releaseSessionNamespace(namespace);
                            throw new IllegalStateException("Proposal overlaps a reserved transaction scope");
                        }
                        try {
                            admission = new Admission(command.operationId(), digest, encoded.length, leadership.term(), generation,
                                    estimate.additionalCount(),estimate.additionalBytes(),maintenance,protectedControl,lease,accepted==null ? null : accepted.reservation.lease,reject ? rejectionData : null);
                            admission.namespaceReservation=namespace;
                            admission.importPublicationHold=importOwner;if(importOwner!=null){importOwner.submitted=true;importOwner.owners++;}
                            admission.sessionBinding=command.sessionControl()!=null && command.sessionControl().binding();
                            if(metadataOwner!=null) { admission.topologyMetadataControl=command; admission.topologyMetadataOwner=metadataOwner; }
                            admission.fencingHold=fencingOwner;
                            if (fencingOwner!=null) { fencingOwner.submitted=true;fencingOwner.owners++; }
                            if (homeCredit!=null) { admission.homeCreditCount=homeCredit.additionalCount(); admission.homeCreditBytes=homeCredit.additionalBytes(); }
                            admissions.put(admission.id, admission);
                        } catch (RuntimeException | Error failure) {
                            if (admission!=null) admissions.remove(admission.id,admission);
                            if (namespace!=null) releaseSessionNamespace(namespace);
                            if (lease!=null) proposalFootprints.release(lease); throw failure;
                        }
                        if (accepted!=null) { if (accepted.resourceLease!=null) { admission.resourceLeases.add(accepted.resourceLease); accepted.resourceLease=null; } proposalFootprints.submit(accepted.reservation.lease); accepted.claimed=true; preparedCommits.remove(admission.id,accepted); preparedCommitBytes-=accepted.bytes; }
                        admittedBytes += admission.bytes;
                        if (protectedControl) protectedAdmissions++; else { dataAdmissions++; dataProposalBytes+=admission.bytes; }
                        reservedHomeCount+=admission.homeCreditCount; reservedHomeBytes+=admission.homeCreditBytes;
                        reservedReceiptCount+=admission.receiptCount; reservedReceiptBytes+=admission.receiptBytes;
                        if (maintenance) maintenanceReservation=admission;
                    }
                    transaction.setStateMachineContext(admission);
                    if (admission.rejectionLogData==null) return transaction;
                    try {
                        return TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request).setLogData(admission.rejectionLogData).setStateMachineContext(admission).build();
                    } catch (RuntimeException | Error failure) { releaseAdmission(transaction); throw failure; }
                }
            }
            throw new IllegalStateException("Range changed during admission; retry with same identity");
        } catch (Exception rejected) {
            return transaction.setException(rejected);
        }
    }

    public final class SnapshotProposal implements AutoCloseable {
        private final SnapshotHold hold; private final RatisSnapshotRecord request; private final byte[] bytes,digest;
        private boolean claimed,closed;
        private SnapshotProposal(SnapshotHold hold,RatisSnapshotRecord request) { this.hold=hold; this.request=request; bytes=request.encode(); digest=request.digest(); }
        public RatisSnapshotRecord request() { return request; }
        public byte[] bytes() { return bytes.clone(); }
        private boolean authorized() { var a=store.applied(); return !closed && !claimed && hold!=null && !hold.invalidated && !hold.released && hold.generation==admissionGeneration.get() && a!=null && a.term()==hold.term && store.admissionOpen(); }
        public boolean valid() {
            final Leadership leader;
            try { leader=leadership(); } catch (IOException unavailable) { return false; }
            synchronized (admissions) {
                if (closed || !leader.ready() || !store.admissionOpen()) return false;
                return hold==null || !hold.invalidated && !hold.released && hold.generation==admissionGeneration.get() && leader.term()==hold.term;
            }
        }
        public boolean drained() { synchronized (admissions) { if (hold==null) return true; return hold.lease==null || !hold.lease.active() ? !hold.invalidated && !hold.released : proposalFootprints.drained(hold.lease); } }
        @Override public void close() {
            synchronized (admissions) { if (closed || claimed) return; closed=true; if (preparedSnapshots.remove(hold.operation,this)) releaseSnapshotHold(hold); }
        }
    }
    private SnapshotProposal borrowedSnapshot(SnapshotHold hold,RatisSnapshotRecord request) { var view=new SnapshotProposal(hold,request); view.claimed=true; return view; }
    public SnapshotProposal prepareSnapshotInstall(RatisSnapshotFences homes,SnapshotFence fence) throws Exception { return prepareSnapshot(homes,fence,true); }
    public SnapshotProposal prepareSnapshotRelease(RatisSnapshotFences homes,SnapshotFence fence) throws Exception { return prepareSnapshot(homes,fence,false); }
    private SnapshotProposal prepareSnapshot(RatisSnapshotFences homes,SnapshotFence fence,boolean install) throws Exception { requireDataRole();
        java.util.Objects.requireNonNull(homes); java.util.Objects.requireNonNull(fence); requireHealthy(); long generation=admissionGeneration.get(); var leader=leadership();
        if (!leader.ready()) throw new IllegalStateException("Snapshot gate leader is not ready");
        var group=store.ownership().owner().groupId(); var participant=fence.participant(group); var operation=install ? participant.installOperation() : participant.releaseOperation();
        var footprint=install ? proposalFootprints.prepare(participant.scope()) : null; long storageBytes=ReplicatedRangeStore.snapshotGateBytes(fence);
        SnapshotHold hold=null;
        for (int attempt=0;attempt<3 && hold==null;attempt++) {
            var capacity=store.gateCapacity(); var outcome=store.snapshotGateResult(fence); if (outcome!=null && outcome.retired()) throw new RatisSnapshotRetiredException(outcome.retirement()); var existing=outcome==null ? null : outcome.gate();
            synchronized (admissions) {
                requireHealthy(); var a=store.applied();
                if (generation!=admissionGeneration.get() || a==null || a.term()!=leader.term() || !store.admissionOpen()) throw new IllegalStateException("Snapshot gate has no current native owner");
                if (!java.util.Objects.equals(a,capacity.position())) continue; reconcileOlderProposalTerms(leader.term());
                var ready=preparedSnapshots.get(operation);
                if (ready!=null && ready.authorized()) { if (!ready.request.fence().equals(fence)) throw new IllegalArgumentException("Snapshot fence definition changed"); return borrowedSnapshot(ready.hold,ready.request); }
                var known=admissions.get(operation);
                if (known!=null && known.snapshot!=null && known.term==leader.term() && known.generation==generation) {
                    if (!known.snapshot.fence().equals(fence)) throw new IllegalArgumentException("Snapshot fence definition changed");
                    return borrowedSnapshot(known.snapshotHold,known.snapshot);
                }
                if (known!=null) throw new IllegalStateException("Previous snapshot control is still settling");
                if (preparingSnapshots.containsKey(operation)) throw new IllegalStateException("Snapshot control is still preparing");
                if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16 || DistributedCommandCodec.MAX_ENCODED_BYTES>16L*1024*1024-snapshotProposalBytes-snapshotAdmittedBytes
                        || install && (snapshotInstallOwners+snapshotAdmittedInstalls>=8 || DistributedCommandCodec.MAX_ENCODED_BYTES>8L*1024*1024-snapshotInstallBytes-snapshotInstallAdmittedBytes)) throw new IllegalStateException("Snapshot protected input budget exhausted");
                var charge=snapshotCharges.get(fence.fenceId());
                if (charge!=null && !charge.fence.equals(fence)) throw new IllegalArgumentException("Snapshot fence definition changed");
                if (charge==null) {
                    for (var id:List.of(fence.operation(),participant.installOperation(),participant.releaseOperation())) if (participantIdentityBusy(id)) {
                        var other=admissions.get(id);
                        if (other==null || other.snapshot==null || !other.snapshot.fence().equals(fence)) throw new IllegalStateException("Snapshot control identity belongs to other work");
                    }
                    long bytes=storageBytes; boolean reserve=existing==null;
                    if (reserve && (capacity.count()+reservedSnapshotCount>=ReplicatedRangeStore.MAX_SNAPSHOT_GATES || bytes>ReplicatedRangeStore.MAX_SNAPSHOT_GATE_BYTES-capacity.bytes()-reservedSnapshotBytes)) throw new IllegalStateException("Snapshot gate storage reservation exhausted");
                    charge=new SnapshotCharge(fence,bytes,reserve);
                }
                ProposalFootprints.GateLease lease=null;
                if (install && existing==null) {
                    lease=proposalFootprints.reserveGate(footprint,leader.term(),generation);
                    if (lease==null) throw new IllegalStateException("Snapshot gate overlaps another reservation");
                }
                RatisNodeResourceBudget.Lease resources;
                try { resources=reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+8L*DistributedCommandCodec.MAX_ENCODED_BYTES,1); }
                catch (IOException failure) { if (lease!=null) proposalFootprints.release(lease); throw failure; }
                if (!snapshotCharges.containsKey(fence.fenceId())) {
                    snapshotCharges.put(fence.fenceId(),charge); snapshotControlCharges.put(fence.operation(),charge); snapshotControlCharges.put(participant.installOperation(),charge); snapshotControlCharges.put(participant.releaseOperation(),charge);
                    if (charge.reserved) { reservedSnapshotCount++; reservedSnapshotBytes+=charge.bytes; }
                }
                if (existing!=null) materializeSnapshotCharge(fence);
                charge.references++; hold=new SnapshotHold(charge,install,leader.term(),generation,operation,lease); hold.resourceLease=resources;
                preparingSnapshots.put(operation,hold); snapshotSlots++; snapshotProposalBytes+=hold.bytes;
                if (install) { snapshotInstallOwners++; snapshotInstallBytes+=hold.bytes; }
            }
        }
        if (hold==null) throw new IllegalStateException("Native snapshot gate cut kept changing"); boolean retained=false;
        try {
            var read=homes.readRegistration(fence.fenceId()); var status=read.status();
            if (status==null || !status.fence().equals(fence) || !read.descriptor().groupId().equals(fence.homeGroup()) || read.descriptor().rangeEpoch()!=fence.homeEpoch()
                    || (install ? status.phase()!=SnapshotFenceStatus.Phase.INSTALLING : !status.terminal())) throw new IllegalStateException("Snapshot home has no matching gate authority");
            var request=install ? RatisSnapshotRecord.install(fence,group) : RatisSnapshotRecord.release(status,group);
            var proposal=new SnapshotProposal(hold,request);
            for (int attempt=0;attempt<3;attempt++) {
                var estimate=install ? store.estimateGateInstall(fence) : store.estimateGateRelease(status); var capacity=estimate.capacity();
                synchronized (admissions) {
                    requireHealthy();
                    if (hold.invalidated || hold.generation!=admissionGeneration.get() || preparingSnapshots.get(operation)!=hold || capacity.position()==null || capacity.position().term()!=hold.term || !store.admissionOpen()) throw new IllegalStateException("Snapshot gate lost acceptance ownership");
                    if (!java.util.Objects.equals(capacity.position(),store.applied())) continue;
                    if (install && status.progress(group).ready()!=null && (estimate.existing()==null || !status.progress(group).ready().equals(estimate.existing().installed()))) throw new IllegalStateException("Home ready proof lacks matching native gate");
                    if (estimate.existing()!=null) materializeSnapshotCharge(fence);
                    snapshotProposalBytes+=proposal.bytes.length-hold.bytes;
                    if (install) snapshotInstallBytes+=proposal.bytes.length-hold.bytes; hold.bytes=proposal.bytes.length;
                    preparedSnapshots.put(operation,proposal); preparingSnapshots.remove(operation,hold); retained=true; return proposal;
                }
            }
            throw new IllegalStateException("Native snapshot gate cut kept changing during acceptance");
        } finally { synchronized (admissions) { if (!retained && preparingSnapshots.remove(operation,hold)) releaseSnapshotHold(hold); } }
    }
    private void transferSnapshotInput(SnapshotHold hold) {
        if (hold.slot) { hold.slot=false; snapshotSlots--; }
        if (hold.installCount) { hold.installCount=false; snapshotInstallOwners--; }
        if (hold.input) { hold.input=false; snapshotProposalBytes-=hold.bytes; if (hold.install) snapshotInstallBytes-=hold.bytes; }
    }
    private void materializeSnapshotCharge(SnapshotFence fence) {
        var charge=snapshotCharges.get(fence.fenceId());
        if (charge!=null && charge.fence.equals(fence) && charge.reserved) { charge.reserved=false; reservedSnapshotCount--; reservedSnapshotBytes-=charge.bytes; }
    }
    private void releaseSnapshotHold(SnapshotHold hold) {
        if (hold==null || hold.released) return; hold.released=true; transferSnapshotInput(hold);
        if (hold.resourceLease!=null) { hold.resourceLease.close(); hold.resourceLease=null; }
        if (hold.lease!=null) proposalFootprints.release(hold.lease);
        var charge=hold.charge;
        if (--charge.references==0) {
            if (charge.reserved) { charge.reserved=false; reservedSnapshotCount--; reservedSnapshotBytes-=charge.bytes; }
            snapshotCharges.remove(charge.fence.fenceId(),charge); var participant=charge.fence.participant(store.ownership().owner().groupId());
            snapshotControlCharges.remove(charge.fence.operation(),charge); snapshotControlCharges.remove(participant.installOperation(),charge); snapshotControlCharges.remove(participant.releaseOperation(),charge);
        }
    }
    private void retireSnapshotOwners(SnapshotGateResult result) {
        if (!result.retired()) { retireSnapshotOwners(result.gate()); return; }
        var fence=result.retirement().fence(); snapshotHelpers.remove(fence.fenceId()); materializeSnapshotCharge(fence);
        for (var plan:List.copyOf(preparedSnapshots.values())) if (plan.request.fence().equals(fence)) plan.close();
        for (var hold:preparingSnapshots.values()) if (hold.charge.fence.equals(fence)) { hold.invalidated=true; if (hold.lease!=null) proposalFootprints.release(hold.lease); }
        for (var admission:List.copyOf(admissions.values())) if (admission.snapshot!=null && admission.snapshot.fence().equals(fence)) settleAdmission(admission);
    }
    private void retireSnapshotOwners(SnapshotGate record) {
        if (record.released()!=null) snapshotHelpers.remove(record.fence().fenceId());
        materializeSnapshotCharge(record.fence());
        for (var plan:List.copyOf(preparedSnapshots.values())) {
            try { if (plan.request.receipt(record)!=null) plan.close(); } catch (IllegalArgumentException different) { /* Another speculative definition is not proof. */ }
        }
        for (var hold:preparingSnapshots.values()) if (hold.charge.fence.equals(record.fence()) && (hold.install || record.released()!=null)) {
            hold.invalidated=true; if (hold.lease!=null) proposalFootprints.release(hold.lease);
        }
        for (var admission:List.copyOf(admissions.values())) if (admission.snapshot!=null) {
            try { if (admission.snapshot.receipt(record)!=null) settleAdmission(admission); } catch (IllegalArgumentException different) { /* Unrelated outcome. */ }
        }
    }
    public int snapshotPreparationSlots() { synchronized (admissions) { return snapshotSlots; } }
    public long reservedSnapshotGateCount() { synchronized (admissions) { return reservedSnapshotCount; } }
    public long reservedSnapshotGateBytes() { synchronized (admissions) { return reservedSnapshotBytes; } }
    public ProposalFootprints.GateStats proposalGateStats() { synchronized (admissions) { return proposalFootprints.gateStats(); } }
    public boolean snapshotGateReady(SnapshotFence fence) throws IOException { requireDataRole();
        checkTransactionOwner(); var record=store.snapshotGate(fence.fenceId());
        if (record==null || !record.active()) return false;
        if (!record.fence().equals(fence)) throw new IllegalArgumentException("Snapshot fence definition changed");
        boolean ready=store.gateBlockers(fence).isEmpty(); checkTransactionOwner();
        synchronized (admissions) {
            for (var hold:preparingSnapshots.values()) if (hold.charge.fence.equals(fence) && hold.lease!=null && hold.lease.active() && !proposalFootprints.drained(hold.lease)) return false;
            for (var plan:preparedSnapshots.values()) if (plan.request.fence().equals(fence) && !plan.drained()) return false;
        }
        return ready;
    }
    public ReplicatedRangeStore.ScopedSnapshot captureSnapshotGate(RatisSnapshotFences homes,SnapshotFence fence) throws Exception {
        return captureSnapshotGate(homes,fence,() -> store.captureGate(fence));
    }
    /** The snapshot gate cut also retains complete scoped history for later read certification. */
    public ReplicatedRangeStore.ScopedHistoryPin captureTrackedSnapshotGate(RatisSnapshotFences homes,SnapshotFence fence) throws Exception {
        return captureSnapshotGate(homes,fence,() -> store.captureTrackedGate(fence));
    }
    private <T extends AutoCloseable> T captureSnapshotGate(RatisSnapshotFences homes,SnapshotFence fence,java.util.concurrent.Callable<T> capture) throws Exception {
        requireDataRole(); java.util.Objects.requireNonNull(homes); java.util.Objects.requireNonNull(fence);
        checkTransactionOwner(); long generation=admissionGeneration.get();
        if (store.snapshotFence(fence)!=null) throw new RatisSnapshotLostException("Snapshot gate has retired");
        var read=homes.readRegistration(fence.fenceId()); var status=read.status();
        if (status!=null && status.fence().equals(fence) && status.consumed()) throw new RatisSnapshotLostException("Snapshot was consumed");
        try {
            if (status==null || !status.fence().equals(fence) || status.phase()!=SnapshotFenceStatus.Phase.INSTALLING || !status.allReady()) throw new IllegalStateException("Snapshot capture requires every home ready proof");
            var gate=store.snapshotGate(fence.fenceId());
            if (gate==null || !gate.active() || !gate.fence().equals(fence) || !gate.installed().equals(status.progress(store.ownership().owner().groupId()).ready())) throw new IllegalStateException("Home ready proof differs from native gate");
            if (!snapshotGateReady(fence)) throw new IllegalStateException("Snapshot gate is not ready");
        } catch (IllegalStateException | IllegalArgumentException unavailable) {
            if (store.snapshotFence(fence)!=null) throw new RatisSnapshotLostException("Snapshot gate retired during capture",unavailable);
            throw unavailable;
        }
        final T pin;
        try { pin=capture.call(); } catch (it.cavallium.rockserver.core.common.distributed.SnapshotRetentionExpiredException expired) { throw new RatisSnapshotLostException("Snapshot business epoch expired",expired); }
        boolean retained=false;
        try { checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot capture lost leadership"); retained=true; return pin; }
        finally { if (!retained) pin.close(); }
    }

    private boolean snapshotRetryIdentityBusy(OperationId operation,SnapshotFence fence) {
        var charge=snapshotControlCharges.get(operation);
        return charge!=null && !charge.fence.equals(fence) || retirementIdentityBusy(operation) || homeOwners.containsKey(operation) || finalizationOwners.containsKey(operation) || participantOwners.containsKey(operation)
                || admissions.containsKey(operation) || preparedCommits.containsKey(operation) || preparingCommits.containsKey(operation);
    }
    private void requireSnapshotParticipant(RatisSnapshotRecord request) {
        if (!request.participantGroup().equals(store.ownership().owner().groupId())) throw new IllegalArgumentException("Snapshot gate command addressed to another participant");
    }
    /** Exact retained outcomes may retry without fresh home authority; new gate admission is a separate privileged seam. */
    private TransactionContext startSnapshot(TransactionContext transaction,byte[] encoded,long generation,Leadership leader) throws Exception {
        var request=RatisSnapshotRecord.decode(encoded); requireSnapshotParticipant(request); byte[] digest=request.digest();
        for (int attempt=0;attempt<3;attempt++) {
            var estimate=request.isRelease() ? store.estimateGateRelease(request.terminal()) : store.estimateGateInstall(request.fence());
            var observed=estimate.capacity().position(); var receipt=estimate.retirement()==null ? request.receipt(estimate.existing()) : RatisSnapshotRecord.Result.retired(estimate.retirement());
            if (observed==null || observed.term()!=leader.term()) throw new IllegalStateException("Snapshot gate native prefix is not ready");
            synchronized (admissions) {
                requireHealthy(); if (generation!=admissionGeneration.get() || !store.admissionOpen()) throw new IllegalStateException("Snapshot gate retry owner changed");
                if (!java.util.Objects.equals(observed,store.applied())) continue; reconcileOlderProposalTerms(leader.term());
                var known=admissions.get(request.operation());
                if (known!=null && !Arrays.equals(known.digest,digest)) throw new IllegalArgumentException("Snapshot control identity payload changed");
                if (known!=null && receipt!=null) { materializeSnapshotCharge(request.fence()); settleAdmission(known); known=admissions.get(request.operation()); }
                if (known!=null) {
                    if (known.snapshot==null || known.term!=leader.term() || known.generation!=generation) throw new IllegalStateException("Snapshot control is still settling");
                    known.owners++; transaction.setStateMachineContext(known); return transaction;
                }
                var accepted=receipt==null ? preparedSnapshots.get(request.operation()) : null;
                if (receipt==null && (accepted==null || !Arrays.equals(accepted.digest,digest) || !accepted.authorized())) throw new IllegalStateException("Snapshot gate has no fresh home admission authority");
                if (accepted!=null && !accepted.drained()) throw new IllegalStateException("Snapshot gate predecessors are still settling");
                if (accepted==null && snapshotRetryIdentityBusy(request.operation(),request.fence())) throw new IllegalStateException("Snapshot control identity belongs to other work");
                if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots-(accepted==null ? 0 : 1)>=16) throw new IllegalStateException("Protected proposal budget exhausted");
                long transferred=accepted==null ? 0 : accepted.hold.bytes;
                if (encoded.length>16L*1024*1024-snapshotProposalBytes-snapshotAdmittedBytes+transferred
                        || !request.isRelease() && (snapshotInstallOwners+snapshotAdmittedInstalls-(accepted==null ? 0 : 1)>=8
                        || encoded.length>8L*1024*1024-snapshotInstallBytes-snapshotInstallAdmittedBytes+transferred)) throw new IllegalStateException("Snapshot submitted input budget exhausted");
                var admission=new Admission(request.operation(),digest,encoded.length,leader.term(),generation,0,0,false,true,null,null,null);
                admission.snapshot=request; admission.snapshotHold=accepted==null ? null : accepted.hold;
                if (accepted!=null && accepted.hold.resourceLease!=null) { admission.resourceLeases.add(accepted.hold.resourceLease); accepted.hold.resourceLease=null; }
                if (accepted!=null) { accepted.claimed=true; preparedSnapshots.remove(request.operation(),accepted); transferSnapshotInput(accepted.hold); }
                else if (receipt!=null) materializeSnapshotCharge(request.fence());
                admissions.put(admission.id,admission); protectedAdmissions++; admittedBytes+=admission.bytes; snapshotAdmittedBytes+=admission.bytes;
                if (!request.isRelease()) { snapshotAdmittedInstalls++; snapshotInstallAdmittedBytes+=admission.bytes; }
                transaction.setStateMachineContext(admission); return transaction;
            }
        }
        throw new IllegalStateException("Native snapshot gate cut changed during retry admission");
    }

    private Leadership leadership() throws IOException {
        if (leadershipSource != null) return leadershipSource.get();
        RaftServer server = getServer().getNow(null);
        if (server == null) throw new IOException("Raft division is not initialized");
        var division = server.getDivision(getGroupId());
        synchronized (division) {
            var info = division.getInfo();
            return new Leadership(info.getCurrentTerm(), info.isLeader() && info.isLeaderReady());
        }
    }

    /** Ratis invokes this under its append locks; recheck only in-memory ownership and term. */
    @Override public TransactionContext preAppendTransaction(TransactionContext transaction) throws IOException {
        try {
            requireHealthy();
            // This callback owns Raft append locks: native proof/configuration was qualified at start/estimate.
            boolean metadataQualified=transaction.getStateMachineContext() instanceof Admission input && input.topologyMetadataControl!=null
                    && role==RatisNodePlan.Role.DATA && store.metadataTopologyControl(input.topologyMetadataControl);
            Leadership current = leadership();
            var nativePosition = store.applied();
            var replayDigest=transaction.getStateMachineContext() instanceof RetainedProof ? appendInputDigest(transaction) : null;
            synchronized (admissions) {
                requireHealthy();
                var saved=savedAppendStamps.get(transaction);
                if(saved!=null){
                    if(transaction.getStateMachineContext()!=saved.owner() || !Arrays.equals(saved.digest(),replayDigest) || !current.ready() || nativePosition==null || nativePosition.term()!=current.term()
                            || saved.term()!=current.term() || saved.generation()!=admissionGeneration.get() || saved.configurationStamp()!=schemaConfigurationStamp || schemaConfigurationPending || getLifeCycleState()!=LifeCycle.State.RUNNING)
                        throw new IOException("Saved exact append lost current context owner");
                    return super.preAppendTransaction(transaction);
                }
                if(transaction.getStateMachineContext() instanceof RelocationHold hold){requireRelocationHold(hold,current);if(hold.owners<=0)throw new IOException("Relocation owner released");return super.preAppendTransaction(transaction);}
                if(transaction.getStateMachineContext() instanceof SourceHold hold){requireSourceOwner(hold,current);if(hold.owners<=0 || !hold.qualified || hold.reading)throw new IOException("Source record owner released");return super.preAppendTransaction(transaction);}
                if(transaction.getStateMachineContext() instanceof ActivationHold hold){requireTopologyActivationOwner(hold,hold.record,current,admissionGeneration.get());if(hold.owners<=0)throw new IOException("Topology activation proposal owner released");return super.preAppendTransaction(transaction);}
                if(transaction.getStateMachineContext() instanceof TopologyHold hold){requireTopologyOwner(hold,hold.record,current,admissionGeneration.get(),true);return super.preAppendTransaction(transaction);}
                if (transaction.getStateMachineContext() instanceof ImportApplyHold hold) {
                    requireImportApply(hold,hold.record,current,admissionGeneration.get());
                    if(hold.owners<=0)throw new IOException("Native import proposal owner released");
                    return super.preAppendTransaction(transaction);
                }
                if (transaction.getStateMachineContext() instanceof SchemaHold hold) {
                    if (schemaHold!=hold || hold.released || hold.invalidated || hold.owners<=0 || !hold.qualified
                            || !current.ready() || hold.term!=current.term() || hold.generation!=admissionGeneration.get()
                            || schemaConfigurationPending || hold.configurationStamp!=schemaConfigurationStamp
                            || nativePosition==null || nativePosition.term()!=current.term() || !store.admissionOpen()
                            || hold.lease!=null && !proposalFootprints.drained(hold.lease)
                            || hold.captureGate!=null && !hold.nativeReady) throw new IOException("Schema operation lost append owner");
                    return super.preAppendTransaction(transaction);
                }
                if (!(transaction.getStateMachineContext() instanceof Admission admission)
                        || admissions.get(admission.id) != admission || admission.owners <= 0
                        || !current.ready() || admission.term != current.term()
                        || admission.generation != admissionGeneration.get()
                        || nativePosition == null || nativePosition.term() != current.term() || !store.admissionOpen() && !metadataQualified) {
                    throw new IOException("Admission lost its completed current-term owner before append");
                }
                if(!admission.protectedControl && diskRunway!=null)diskRunway.requireOrdinary();
                if(admission.sessionBinding && store.topologyFreeze()!=null) throw new IOException("Sealed topology parent cannot append session grants");
                if(admission.topologyMetadataControl!=null && (!metadataQualified || getLifeCycleState()!=LifeCycle.State.RUNNING || schemaConfigurationPending || store.topologyBirthPending()
                        || admission.topologyMetadataOwner==null || admission.topologyMetadataOwner.stamp()!=schemaConfigurationStamp || admission.topologyMetadataOwner.term()!=current.term()
                        || admission.topologyMetadataOwner.generation()!=admissionGeneration.get() || !admission.id.equals(admission.topologyMetadataControl.operationId()) || !Arrays.equals(admission.digest,admission.topologyMetadataControl.payloadDigest()))) throw new IOException("Topology metadata control lost its original configuration/generation");
                if(admission.importPublicationHold!=null && !admission.settled)requireImportPublication(admission.importPublicationHold,admission.importPublicationHold.command,current,admissionGeneration.get());
                if (admission.fencingHold!=null && !admission.settled) requireFencingOwner(admission.fencingHold,(DistributedCommand)admission.fencingHold.record,current,admissionGeneration.get());
                if (admission.snapshotHold!=null && admission.snapshotHold.install && admission.snapshotHold.lease!=null
                        && admission.snapshotHold.lease.active() && !proposalFootprints.drained(admission.snapshotHold.lease)) throw new IOException("Snapshot gate predecessors are still settling");
            }
            return super.preAppendTransaction(transaction);
        } catch (Exception stale) {
            releaseAdmission(transaction);
            // IOException alone would make Ratis step down the healthy replacement leader.
            throw new org.apache.ratis.protocol.exceptions.StateMachineException("Stale proposal admission", stale, false);
        }
    }

    private record AppliedRecord(OperationId operation,byte[] result,SchemaChange.Definition definition,NativeSchemaActivationProof activation,TopologyChange.Definition topologyDefinition,NativeTopologyActivation topologyActivation) {
        AppliedRecord(OperationId operation,byte[] result) { this(operation,result,null,null,null,null); }
        AppliedRecord(OperationId operation,byte[] result,SchemaChange.Definition definition,NativeSchemaActivationProof activation) { this(operation,result,definition,activation,null,null); }
    }
    private AppliedRecord applyRecord(ApplyPosition positionArg,byte[] encoded) throws Exception {
        if(RatisTopologyRelocationRecord.isRecord(encoded))return new AppliedRecord(null,applyRelocation(positionArg,RatisTopologyRelocationRecord.decode(encoded)));
        requireRoleRecord(encoded);
        if(RatisSourceOutcomeRecord.isRecord(encoded))return new AppliedRecord(null,applySourceOutcome(positionArg,RatisSourceOutcomeRecord.decode(encoded)));
        if(RatisTopologyActivationRecord.isActivation(encoded)) {
            var record=RatisTopologyActivationRecord.decode(encoded);boolean fresh=store.topologyActivation()==null;
            var receipt=store.applyTopologyActivation(positionArg,record.definition(),record.freezeDigest(),record.catalogueGroup(),record.catalogueCut(),record.publicationCommitment());
            if(fresh)refreshSessionAdmission(record.definition().targetDirectory());if(enableSessionControls)store.enableSessionControls();store.enableScopedHistory(RangeMutationHistory.Limits.defaults());
            synchronized(admissions) { if(topologyActivationHold!=null && topologyActivationHold.record.equals(record))releaseTopologyActivation(topologyActivationHold); }
            return new AppliedRecord(null,receipt.encode(),null,null,fresh?record.definition():null,fresh?receipt:null);
        }
        if (RatisSchemaFenceRecord.isFence(encoded)) {
            var record=RatisSchemaFenceRecord.decode(encoded);
            var proof=schemaReplicas==null ? store.applySchemaFence(positionArg,record.header(),record.admissionDigest())
                    : schemaStorage.applySchemaFence(store,positionArg,record.header(),schemaStorage.requireSchemaFencingAdmission(store,record.header(),record.admissions(),schemaLocalReplica),record.admissionDigest());
            schemaProof(record);
            return new AppliedRecord(null,proof.encode());
        }
        if (RatisSchemaActivateRecord.isActivate(encoded)) {
            var record=RatisSchemaActivateRecord.decode(encoded);
            var install=store.lookupSchemaInstallation(record.header());
            if (install==null || !install.position().equals(record.expectedInstallPosition())) throw new IllegalArgumentException("Activation original installation changed");
            boolean fresh=store.lookupSchemaActivation(record.header())==null;
            var proof=store.applySchemaActivation(positionArg,install,record.catalogueGroup(),record.cataloguePosition());
            var current=store.schemaInstallation();
            if (current!=null && current.definition().changeId().equals(record.header().changeId())) {
                var directory=current.definition().targetDirectory();
                schemaDescriptor=directory.ranges().stream().filter(r -> r.groupId().equals(record.groupId())).findFirst().orElseThrow();
                refreshSessionAdmission(directory);
            }
            boolean notify=fresh && current!=null && current.definition().changeId().equals(record.header().changeId());
            schemaRecovered(record); return new AppliedRecord(null,proof.encode(),notify ? current.definition() : null,notify ? proof : null);
        }
        if (RatisSnapshotHomeRetirementRecord.isRetirement(encoded)) {
            var retirement=RatisSnapshotHomeRetirementRecord.decode(encoded); store.applySnapshotHomeRetirement(positionArg,retirement);
            return new AppliedRecord(RatisSnapshotHomeRetirementRecord.operation(retirement),new it.cavallium.rockserver.core.common.distributed.CommandResult(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,List.of()).encode());
        }
        if (RatisSnapshotRetirementRecord.isRetirement(encoded)) {
            var retirement=RatisSnapshotRetirementRecord.decode(encoded); store.applySnapshotGateRetirement(positionArg,retirement);
            synchronized (admissions) { retireSnapshotOwners(SnapshotGateResult.retired(store.snapshotFence(retirement.fence()))); }
            return new AppliedRecord(RatisSnapshotRetirementRecord.operation(retirement),new it.cavallium.rockserver.core.common.distributed.CommandResult(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,List.of()).encode());
        }
        if (RatisHomeRetirementRecord.isRetirement(encoded)) {
            var retirement=RatisHomeRetirementRecord.decode(encoded); store.applyHomeRetirement(positionArg,retirement);
            return new AppliedRecord(RatisHomeRetirementRecord.operation(retirement),new it.cavallium.rockserver.core.common.distributed.CommandResult(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,List.of()).encode());
        }
        if (HomeRecovery.isRecovery(encoded)) {
            var recovery=HomeRecovery.decode(encoded); var receipt=store.applyHomeRecovery(positionArg,recovery);
            return new AppliedRecord(recovery.operation(),receipt.encode());
        }
        if (RatisSnapshotRecord.isSnapshot(encoded)) {
            var request=RatisSnapshotRecord.decode(encoded); requireSnapshotParticipant(request);
            var record=request.isRelease() ? store.applyGateRelease(positionArg,request.terminal()) : store.applyGateInstall(positionArg,request.fence());
            synchronized (admissions) { retireSnapshotOwners(record); }
            return new AppliedRecord(request.operation(),java.util.Objects.requireNonNull(request.receipt(record)).encode());
        }
        if (RatisParticipantRecord.isRetirement(encoded)) {
            var retirement=RatisParticipantRecord.decodeRetirement(encoded); store.applyParticipantRetirement(positionArg,retirement);
            synchronized (admissions) { retireFinalizedPreparations(retirement.finalization()); }
            return new AppliedRecord(RatisParticipantRecord.operation(retirement),new it.cavallium.rockserver.core.common.distributed.CommandResult(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,List.of()).encode());
        }
        if (RatisParticipantRecord.isFinalization(encoded)) {
            var finalization=RatisParticipantRecord.decodeFinalization(encoded);
            var position=store.applyFinalization(positionArg,finalization);
            synchronized (admissions) { retireFinalizedPreparations(finalization); }
            return new AppliedRecord(finalization.operation(),RatisParticipantRecord.result(position));
        }
        if (RatisParticipantRecord.isParticipant(encoded)) {
            var preparation=RatisParticipantRecord.decode(encoded); commandValidator.accept(preparation.business());
            var record=RatisParticipantRecord.isNegative(encoded) ? store.applyRejected(positionArg,preparation) : store.applyPrepared(positionArg,preparation);
            return new AppliedRecord(preparation.operation(),RatisParticipantRecord.result(record));
        }
        var command=DistributedCommandCodec.decode(encoded); commandValidator.accept(command);
        return new AppliedRecord(command.operationId(),store.currentRangeRecoveryRegistration()!=null ? store.applyCurrentRangeRecoveryTail(positionArg,command) : store.topologyLearnerRegistration()==null ? store.apply(positionArg,command) : store.applyTopologyLearnerTail(positionArg,command));
    }
    private AppliedRecord recoveredRecord(byte[] encoded) throws Exception {
        if(RatisTopologyRelocationRecord.isRecord(encoded))return new AppliedRecord(null,new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode());
        if(RatisSourceOutcomeRecord.isRecord(encoded))return new AppliedRecord(null,recoveredSourceRecord(RatisSourceOutcomeRecord.decode(encoded)));
        requireRoleRecord(encoded);
        if(RatisTopologyActivationRecord.isActivation(encoded))return new AppliedRecord(null,topologyActivationRecovered(RatisTopologyActivationRecord.decode(encoded)));
        if(RatisTopologyFreezeRecord.isFreeze(encoded))return new AppliedRecord(null,topologyRecovered(RatisTopologyFreezeRecord.decode(encoded)));
        if (RatisRangeImportRecord.isImport(encoded)) return new AppliedRecord(null,rangeImportApply.recoveredResult(RatisRangeImportRecord.decode(encoded)));
        if (RatisReferencedFile.Request.isRequest(encoded)) throw new IOException("Reference recovery requires exact original log position");
        if (RatisSchemaFenceRecord.isFence(encoded)) { var proof=schemaProof(RatisSchemaFenceRecord.decode(encoded)); return new AppliedRecord(null,proof==null ? null : proof.encode()); }
        if (RatisSchemaInstallRecord.isInstall(encoded)) return new AppliedRecord(null,schemaRecovered(RatisSchemaInstallRecord.decode(encoded)));
        if (RatisSchemaActivateRecord.isActivate(encoded)) return new AppliedRecord(null,schemaRecovered(RatisSchemaActivateRecord.decode(encoded)));
        if (RatisSnapshotHomeRetirementRecord.isRetirement(encoded)) {
            var retirement=RatisSnapshotHomeRetirementRecord.decode(encoded);
            return new AppliedRecord(RatisSnapshotHomeRetirementRecord.operation(retirement),store.snapshotHomeRetired(retirement) ? new it.cavallium.rockserver.core.common.distributed.CommandResult(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,List.of()).encode() : null);
        }
        if (RatisSnapshotRetirementRecord.isRetirement(encoded)) {
            var retirement=RatisSnapshotRetirementRecord.decode(encoded);
            return new AppliedRecord(RatisSnapshotRetirementRecord.operation(retirement),store.snapshotFence(retirement.fence())!=null ? new it.cavallium.rockserver.core.common.distributed.CommandResult(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,List.of()).encode() : null);
        }
        if (RatisHomeRetirementRecord.isRetirement(encoded)) {
            var retirement=RatisHomeRetirementRecord.decode(encoded);
            return new AppliedRecord(RatisHomeRetirementRecord.operation(retirement),store.homeRetired(retirement) ? new it.cavallium.rockserver.core.common.distributed.CommandResult(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,List.of()).encode() : null);
        }
        if (HomeRecovery.isRecovery(encoded)) {
            var recovery=HomeRecovery.decode(encoded); var receipt=store.homeRecoveryResult(recovery);
            return new AppliedRecord(recovery.operation(),receipt==null ? null : receipt.encode());
        }
        if (RatisSnapshotRecord.isSnapshot(encoded)) {
            var request=RatisSnapshotRecord.decode(encoded); requireSnapshotParticipant(request);
            var receipt=request.receipt(store.snapshotGateResult(request.fence()));
            return new AppliedRecord(request.operation(),receipt==null ? null : receipt.encode());
        }
        if (RatisParticipantRecord.isRetirement(encoded)) {
            var retirement=RatisParticipantRecord.decodeRetirement(encoded);
            return new AppliedRecord(RatisParticipantRecord.operation(retirement),store.participantFence(retirement.identity())==null ? null : new it.cavallium.rockserver.core.common.distributed.CommandResult(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,List.of()).encode());
        }
        if (RatisParticipantRecord.isFinalization(encoded)) {
            var finalization=RatisParticipantRecord.decodeFinalization(encoded); var position=store.recoveredFinalization(finalization);
            return new AppliedRecord(finalization.operation(),position==null ? null : RatisParticipantRecord.result(position));
        }
        if (RatisParticipantRecord.isParticipant(encoded)) {
            var preparation=RatisParticipantRecord.decode(encoded); var record=store.recoveredPreparationVote(preparation);
            if (RatisParticipantRecord.isNegative(encoded) && record!=null && record.prepared()) throw new IOException("Negative preparation has contradictory positive native coverage");
            return new AppliedRecord(preparation.operation(),record==null ? null : RatisParticipantRecord.result(record));
        }
        var command=DistributedCommandCodec.decode(encoded);
        return new AppliedRecord(command.operationId(),store.operationResult(command.operationId(),command.receiptDigest()));
    }
    @Override public CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
        Deferred completion = null;
        var logEntry=transaction.getLogEntry();
        org.apache.ratis.server.impl.CommittedApplyStateMachine.ApplyPermit committed=null;
        boolean committedDeferred=false;
        Message result = null;
        Throwable failure = null;
        NativeApply schedule=null; CompletableFuture<Message> waiting=null; AppliedRecord appliedRecord=null;
        synchronized (gate) {
            try {
                committed=committedGroup==null ? null : committedGroup.claim(logEntry.getTerm(),logEntry.getIndex(),getLifeCycleState()==LifeCycle.State.RUNNING);
                if (storageFailure != null) requireHealthy();
                var entry = transaction.getLogEntry();
                if (pendingNoops.containsKey(entry.getIndex()) || configurations.containsKey(entry.getIndex())) {
                    throw new IllegalStateException("Log entry changed from metadata to data");
                }
                byte[] encoded = entry.getStateMachineLogEntry().getLogData().toByteArray();
                if (RatisReferencedFile.Request.isRequest(encoded)) { requireRoleRecord(encoded); referencedFiles.validateEntry(entry); }
                var pending = deferred.get(entry.getIndex());
                if (pending != null && (pending.term() != entry.getTerm() || !Arrays.equals(pending.encoded(), encoded))) {
                    throw new IllegalStateException("Deferred/replayed log identity changed");
                }
                if (getLifeCycleState() != LifeCycle.State.RUNNING || nativeApply!=null || asyncNativeRecord(encoded)) {
                    if (pending == null) {
                        if (committed==null && (deferred.size() >= 1024 || deferredBytes + encoded.length > 16L * 1024 * 1024)) throw new IllegalStateException("Deferred apply budget exhausted");
                        pending = new Deferred(entry.getTerm(), encoded, new CompletableFuture<>(),committed);
                        deferred.put(entry.getIndex(), pending); deferredBytes += encoded.length;
                    }
                    committedDeferred=true;
                    if(committed!=null && pending.committed!=committed)committed.close();
                    waiting=pending.future().whenComplete((message, problem) -> releaseAdmission(transaction));
                    if (nativeApply==null && getLifeCycleState()==LifeCycle.State.RUNNING && deferred.firstKey()==entry.getIndex()) {
                        nativeApply=new NativeApply(entry.getIndex(),pending); schedule=nativeApply;
                    }
                } else {
                var applied=applyRecord(new ApplyPosition(entry.getTerm(),entry.getIndex()),encoded); appliedRecord=applied;
                byte[] bytes=applied.result();
                synchronized (admissions) {
                    var admitted = admissions.get(applied.operation());
                    if (admitted != null) settleAdmission(admitted);
                }
                advance(entry.getTerm(), entry.getIndex());
                result = Message.valueOf(ByteString.copyFrom(bytes));
                if (pending != null) {
                    deferred.remove(entry.getIndex()); deferredBytes -= pending.encoded().length;
                    completion = pending;
                }
                }
            } catch (Throwable problem) {
                if(!(problem instanceof org.apache.ratis.server.impl.CommittedApplyStateMachine.Retry) && !(problem instanceof org.apache.ratis.server.impl.CommittedApplyStateMachine.Stopped))storageFailure = problem;
                failure = problem;
            }
        }
        if (waiting!=null && failure==null) { if (schedule!=null) scheduleNativeApply(schedule,false); return waiting; }
        if(committed!=null && !committedDeferred)committed.close();
        releaseAdmission(transaction);
        if(failure instanceof org.apache.ratis.server.impl.CommittedApplyStateMachine.Retry || failure instanceof org.apache.ratis.server.impl.CommittedApplyStateMachine.Stopped)return CompletableFuture.failedFuture(failure);
        if (failure != null) {
            failDeferred(failure);
            return CompletableFuture.failedFuture(failure);
        }
        try{refreshRecoveryAfterNativeCompletion();}catch(IOException problem){synchronized(gate){storageFailure=problem;}failDeferred(problem);return CompletableFuture.failedFuture(problem);}
        publishSchemaActivation(appliedRecord);
        if (completion != null) completion.future().complete(result);
        return CompletableFuture.completedFuture(result);
    }

    private static boolean asyncNativeRecord(byte[] bytes) { return RatisTopologyFreezeRecord.isFreeze(bytes) || RatisSchemaInstallRecord.isInstall(bytes) || RatisRangeImportRecord.isImport(bytes) || RatisReferencedFile.Request.isRequest(bytes); }
    private byte[] referenceProof(RatisReferencedFile.Request request) throws Exception {
        var result=store.importFileReceipt(request.clientId(),request.callId(),RangeImportAdmission.sha256(request.encode()));
        if(result==null)return null;
        var reference=RatisReferencedFile.decode(result.result());
        if(!reference.request().equals(request) || reference.term()!=result.position().term() || reference.index()!=result.position().index())throw new IOException("Original file result differs from native receipt");
        return reference.encode();
    }
    private byte[] nativeApplyProof(NativeApply work) throws Exception {
        if(work.topologyRecord!=null)return topologyRecovered(work.topologyRecord);
        if (work.referenceRequest!=null) return referenceProof(work.referenceRequest);
        if (work.importRecord!=null) return rangeImportApply.recoveredResult(work.importRecord);
        var proof=schemaInstallProof(work.record); return proof==null ? null : proof.encode();
    }
    private void scheduleNativeApply(NativeApply work,boolean ready) { scheduleNativeApply(work,ready,-1); }
    private void scheduleNativeApply(NativeApply work,boolean ready,long expectedTopologyGeneration) {
        ProofQueryExecutor executor=committedGroup==null ? proofQueries : (group,input,weight,coalesce,action)->committedGroup.executeNative(()->action.run(input));
        if (executor==null) { failNativeApply(work,new IOException("Bounded schema apply dispatcher is not configured"),expectedTopologyGeneration); return; }
        byte[] input=ByteBuffer.allocate(17+work.deferred.encoded().length).putLong(work.deferred.term()).putLong(work.index).put((byte)(ready ? 1 : 0)).put(work.deferred.encoded()).array();
        var topologyContinuation=new CompletableFuture<Void>();var topologyWaiting=new java.util.concurrent.atomic.AtomicBoolean();
        final long topologyGeneration;var topologyAction=new CompletableFuture<Void>();
        synchronized(gate){topologyGeneration=work.topologyGeneration;if(work.topologyRecord!=null){if(nativeApply!=work || work.topologyReplacing || expectedTopologyGeneration>=0 && expectedTopologyGeneration!=topologyGeneration)return;work.topologyActions=CompletableFuture.allOf(work.topologyActions,topologyAction);}}
        executor.submit(store.ownership().owner().groupId(),input,input.length,false,ignored -> {
            synchronized (gate) {
                if (nativeApply!=work || work.cancelled || work.topologyRecord!=null && (work.topologyReplacing || topologyGeneration!=work.topologyGeneration)) return Message.EMPTY;
                if (storageFailure!=null) requireHealthy();
                if (getLifeCycleState()!=LifeCycle.State.RUNNING) { work.suspended=true; work.stageReady=ready; return Message.EMPTY; }
                work.running=true; work.suspended=false;
            }
            try {
            work.ownRetainedProof();
            if(work.topologyRecord!=null){
                var record=work.topologyRecord;var saved=store.lookupTopologyFreeze(record.header(),record.admissionDigest());
                if(saved!=null){store.applyTopologyFreeze(new ApplyPosition(work.deferred.term(),work.index),record.definition(),record.admissionDigest());return finishNativeApply(work,saved.encode());}
                if(topologyStorage==null)throw new IOException("Topology FREEZE storage is not configured");work.topologyPool=topologyStorage;
                if(work.topologyGate==null){synchronized(admissions){if(topologyHold!=null && topologyHold.record.equals(record) && !topologyHold.reading && topologyHold.nativeGate!=null){work.topologyGate=topologyHold.nativeGate;topologyHold.nativeGate=null;}}if(work.topologyGate==null)work.topologyGate=store.prepareTopologyFreeze(record.definition(),record.admissionDigest());}
                if(!ready && !work.topologyGate.state().ready()){topologyWaiting.set(true);work.topologyGate.whenReady().whenComplete((unused,failure)->{if(failure==null)topologyContinuation.complete(null);else topologyContinuation.completeExceptionally(failure);});return Message.EMPTY;}
                var freeze=store.applyTopologyFreeze(new ApplyPosition(work.deferred.term(),work.index),record.definition(),record.admissionDigest());return finishNativeApply(work,freeze.encode());
            }
            if (work.referenceRequest!=null) {
                byte[] saved=referenceProof(work.referenceRequest);
                if(saved!=null){store.applyNoop(new ApplyPosition(work.deferred.term(),work.index));return finishNativeApply(work,saved);}
                referencedFiles.flush(work.index).get();
                var reference=referencedFiles.publishedReference(work.deferred.term(),work.index,work.referenceRequest);
                try(var retained=referencedFiles.pin(reference)) {
                    var original=store.applyImportFileReceipt(new ApplyPosition(work.deferred.term(),work.index),work.referenceRequest.clientId(),work.referenceRequest.callId(),RangeImportAdmission.sha256(work.referenceRequest.encode()),reference.encode());
                    reference=RatisReferencedFile.decode(original.result());
                }
                return finishNativeApply(work,reference.encode());
            }
            if (work.importRecord!=null) {
                var position=new ApplyPosition(work.deferred.term(),work.index);
                byte[] result=work.importRecord instanceof RatisRangeImportRecord.Install install
                        ? rangeImportApply.applyInstall(position,install).encode()
                        : rangeImportApply.applyActivate(position,(RatisRangeImportRecord.Activate)work.importRecord).encode();
                return finishNativeApply(work,result);
            }
            if (!ready) {
                var saved=schemaInstallProof(work.record);
                if (saved!=null) { store.applyNoop(new ApplyPosition(work.deferred.term(),work.index)); return finishNativeApply(work,saved.encode()); }
                if (schemaStorage==null) throw new IOException("Schema installation storage is not configured");
                synchronized (admissions) {
                    if (schemaHold!=null && schemaHold.record.equals(work.record) && schemaHold.installPlan!=null) {
                        work.plan=schemaHold.installPlan; work.captureGate=schemaHold.captureGate;
                        schemaHold.installPlan=null; schemaHold.captureGate=null;
                    }
                }
                if (work.captureGate==null) work.captureGate=store.prepareSchemaInstallation(work.record.header());
                if (work.plan==null) {
                    var names=new java.util.HashMap<>(schemaStorage.schemaPhysicalNames(store)); var merges=new java.util.HashSet<>(store.deterministicMergeColumns());
                    if (work.record.header().kind()==SchemaChange.Kind.CREATE_COLUMN) {
                        names.put(work.record.header().changedColumnId(),work.record.changedPhysicalName());
                        if (work.record.mergeAdmission()) merges.add(work.record.header().changedColumnId());
                    } else { names.remove(work.record.header().changedColumnId()); merges.remove(work.record.header().changedColumnId()); }
                    work.plan=schemaStorage.prepareSchemaInstallation(store,work.record.definition(),names,merges);
                }
                var fence=store.lookupSchemaFence(work.record.header());
                if (fence==null || !fence.position().equals(work.record.expectedFencePosition())) throw new IllegalArgumentException("Committed installation original fence changed");
                if (!work.captureGate.ready()) {
                    topologyWaiting.set(true);work.captureGate.readyFuture().whenComplete((unused,failure) -> { if (failure!=null) topologyContinuation.completeExceptionally(failure); else topologyContinuation.complete(null); });
                    return Message.EMPTY;
                }
            }
            // No Ratis gate or admission monitor owns JNI schema mutation or waits for retained pins.
            var installed=schemaStorage.installSchema(store,new ApplyPosition(work.deferred.term(),work.index),work.plan);
            return finishNativeApply(work,installed.proof().encode());
            } finally { boolean close; synchronized (gate) { work.running=false; close=work.cancelled || work.coveredResult!=null; } if (close) work.close(); work.releaseRetainedIfDrained(); }
        }).whenComplete((ignored,failure) -> {
            topologyAction.complete(null);
            synchronized(gate){if(work.topologyRecord!=null && (work.topologyReplacing || topologyGeneration!=work.topologyGeneration))return;}
            if(failure!=null)failNativeApply(work,failure,topologyGeneration);
            else if(topologyWaiting.get())topologyContinuation.whenComplete((unused,problem)->{
                if(problem!=null)failNativeApply(work,problem,topologyGeneration);else scheduleNativeApply(work,true,topologyGeneration);
            });
        });
    }
    private Message finishNativeApply(NativeApply work,byte[] bytes) throws Exception {
        var completions=new java.util.ArrayList<Completion>(); var activations=new java.util.ArrayList<AppliedRecord>(); NativeApply next=null;
        var result=Message.valueOf(ByteString.copyFrom(bytes));
        try {
        synchronized (gate) {
            if (nativeApply!=work || deferred.get(work.index)!=work.deferred) {
                var cut=store.applied(); var original=nativeApplyProof(work);
                if (work.coveredResult!=null && original!=null && Arrays.equals(work.coveredResult,bytes) && Arrays.equals(original,bytes)
                        && cut!=null && cut.index()>=work.index && cut.term()>=work.deferred.term()
                        && (cut.index()!=work.index || cut.term()==work.deferred.term())) return result;
                throw new IOException("Schema apply lost native ordered owner");
            }
            var current=store.schemaInstallation();
            if (work.record!=null) {
                if (current!=null && current.definition().changeId().equals(work.record.header().changeId())) refreshSessionAdmission(current.definition().targetDirectory());
                schemaRecovered(work.record);
            } else if (work.importRecord instanceof RatisRangeImportRecord.Activate) {
                store.enableScopedHistory(it.cavallium.rockserver.core.common.distributed.RangeMutationHistory.Limits.defaults());
                if (enableSessionControls) store.enableSessionControls();
                refreshSessionAdmission(current==null ? rangeImportApply.definition().targetDirectory() : current.definition().targetDirectory());
            }
            if(work.topologyRecord!=null)topologyRecovered(work.topologyRecord);
            synchronized(admissions){if(importApplyHold!=null && Arrays.equals(importApplyHold.bytes,work.deferred.encoded()))releaseImportApply(importApplyHold);}
            advance(work.deferred.term(),work.index);
            deferred.remove(work.index); deferredBytes-=work.deferred.encoded().length; nativeApply=null;
            completions.add(new Completion(work.deferred.future(),result));
            while (getLifeCycleState()==LifeCycle.State.RUNNING && (!deferred.isEmpty() || !pendingNoops.isEmpty())) {
                long data=deferred.isEmpty() ? Long.MAX_VALUE : deferred.firstKey(); long metadata=pendingNoops.isEmpty() ? Long.MAX_VALUE : pendingNoops.firstKey();
                if (metadata<data) {
                    long term=pendingNoops.remove(metadata); var cfg=configurations.remove(metadata);
                    if (cfg!=null && cfg.term()!=term) throw new IOException("Deferred configuration term changed");
                    if (cfg!=null) { admissionGeneration.incrementAndGet(); invalidateProposalCandidates(); }
                    store.applyNoop(new ApplyPosition(term,metadata),cfg==null ? null : cfg.bytes()); schemaConfigurationPending=!configurations.isEmpty(); advance(term,metadata); continue;
                }
                var pending=deferred.get(data);
                if (asyncNativeRecord(pending.encoded())) { next=new NativeApply(data,pending); nativeApply=next; break; }
                var applied=applyRecord(new ApplyPosition(pending.term(),data),pending.encoded());
                if (applied.activation()!=null || applied.topologyActivation()!=null) activations.add(applied);
                synchronized (admissions) { var admission=admissions.get(applied.operation()); if (admission!=null) settleAdmission(admission); }
                advance(pending.term(),data); deferred.remove(data); deferredBytes-=pending.encoded().length; pending.releaseCommitted();
                completions.add(new Completion(pending.future(),Message.valueOf(ByteString.copyFrom(applied.result()))));
            }
        }
        work.close();refreshRecoveryAfterNativeCompletion(); activations.forEach(this::publishSchemaActivation); for (var completion:completions) completion.future().complete(completion.result());
        if (next!=null) scheduleNativeApply(next,false); return result;
        } catch (Exception failure) {
            synchronized (gate) { storageFailure=failure; }
            work.close();
            // Native INSTALL and any successfully drained records retain their original receipts.
            activations.forEach(this::publishSchemaActivation);
            for (var completion:completions) completion.future().complete(completion.result());
            work.deferred.future().completeExceptionally(failure); failDeferred(failure); throw failure;
        }
    }
    private void failNativeApply(NativeApply work,Throwable failure) { failNativeApply(work,failure,-1); }
    private void failNativeApply(NativeApply work,Throwable failure,long expectedTopologyGeneration) {
        synchronized (gate) { if (nativeApply!=work || work.topologyRecord!=null && expectedTopologyGeneration>=0 && (work.topologyReplacing || expectedTopologyGeneration!=work.topologyGeneration)) return; storageFailure=failure; nativeApply=null; }
        work.close(); failDeferred(failure);
    }

    @Override public TransactionContext cancelTransaction(TransactionContext transaction) throws IOException {
        releaseAdmission(transaction);
        return super.cancelTransaction(transaction);
    }

    @Override public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
        admissionGeneration.incrementAndGet(); invalidateProposalCandidates(); store.invalidateScopedSnapshots();
        for (var transaction : pendingEntries) releaseAdmission(transaction);
    }

    private void releaseAdmission(TransactionContext transaction) {
        synchronized (admissions) {
            savedAppendStamps.remove(transaction);
            if(transaction.getStateMachineContext() instanceof RelocationHold hold){transaction.setStateMachineContext(null);hold.owners--;if(transaction.getLogEntry()!=null)hold.maybeAppended=true;if(hold.released || hold.invalidated || !hold.maybeAppended && hold.owners==0)releaseRelocation(hold);}
            if(transaction.getStateMachineContext() instanceof SourceHold hold){transaction.setStateMachineContext(null);hold.owners--;if(transaction.getLogEntry()!=null)hold.maybeAppended=true;if(hold.released)releaseSourceHold(hold);else if(!hold.maybeAppended && hold.owners==0){hold.submitted=false;releaseSourceHold(hold);}}
            if(transaction.getStateMachineContext() instanceof ActivationHold hold){transaction.setStateMachineContext(null);hold.owners--;if(transaction.getLogEntry()!=null)hold.maybeAppended=true;if(hold.released)releaseTopologyActivation(hold);else if(!hold.maybeAppended && hold.owners==0){hold.submitted=false;releaseTopologyActivation(hold);}}
            if(transaction.getStateMachineContext() instanceof TopologyHold hold){transaction.setStateMachineContext(null);hold.owners--;if(transaction.getLogEntry()!=null)hold.maybeAppended=true;if(hold.released || !hold.maybeAppended && hold.owners==0){hold.submitted=false;releaseTopologyHold(hold);}}
            if (transaction.getStateMachineContext() instanceof ImportApplyHold hold) {
                transaction.setStateMachineContext(null); hold.owners--; if(transaction.getLogEntry()!=null)hold.maybeAppended=true;
                if(hold.released || !hold.maybeAppended && hold.owners==0){hold.submitted=false;releaseImportApply(hold);}
            }
            if (transaction.getStateMachineContext() instanceof SchemaHold hold) {
                transaction.setStateMachineContext(null); hold.owners--; if (transaction.getLogEntry()!=null) hold.maybeAppended=true;
                if (hold.released || !hold.maybeAppended && hold.owners==0) { hold.submitted=false; releaseSchemaHold(hold); }
            }
            if (transaction.getStateMachineContext() instanceof Admission admission) {
                transaction.setStateMachineContext(null);
                if (transaction.getLogEntry()!=null) admission.maybeAppended=true;
                if (--admission.owners==0 && (!admission.maybeAppended || admission.settled)) finishAdmission(admission);
            }
        }
    }
    // Caller holds admissions. Exact object identity protects a later replacement from old callbacks.
    private void finishAdmission(Admission admission) {
        if (!admissions.remove(admission.id,admission)) return;
        releaseRetention(admission); releaseFootprint(admission);
        if(admission.importPublicationHold!=null){admission.importPublicationHold.owners--;releaseImportPublication(admission.importPublicationHold);}
        if(admission.fencingHold!=null){admission.fencingHold.owners--;releaseSchemaHold(admission.fencingHold);}
        admission.resourceLeases.forEach(RatisNodeResourceBudget.Lease::close); admission.resourceLeases.clear();
        admittedBytes-=admission.bytes;
        if (admission.homeRecovery!=null) homeRecoveryInputBytes-=admission.bytes;
        if (admission.homeRetirement!=null) homeRetirementInputBytes-=admission.bytes;
        if (admission.snapshotRetirement!=null) snapshotRetirementInputBytes-=admission.bytes;
        if (admission.snapshotHomeRetirement!=null) snapshotHomeRetirementInputBytes-=admission.bytes;
        if (admission.retirement!=null) retirementInputBytes-=admission.bytes;
        if (admission.negativePayload!=null) releaseNegativePayload(admission.negativePayload);
        if (admission.snapshot!=null) { snapshotAdmittedBytes-=admission.bytes; if (!admission.snapshot.isRelease()) { snapshotAdmittedInstalls--; snapshotInstallAdmittedBytes-=admission.bytes; } }
        if (admission.protectedControl) protectedAdmissions--; else { dataAdmissions--; dataProposalBytes-=admission.bytes; }
    }
    private void settleAdmission(Admission admission) {
        admission.settled=true; releaseRetention(admission); releaseFootprint(admission);
        if (admission.owners==0) finishAdmission(admission);
    }
    // Native terminal state prevents every matching PREPARE from acquiring locks again.
    private void retireFinalizedPreparations(ParticipantFinalization finalization) {
        for (var plan:List.copyOf(preparedParticipants.values())) if (matchesFinalization(finalization,plan.preparation)) plan.close();
        for (var preparing:preparingParticipants.values()) {
            var hold=preparing.hold();
            if (hold.transaction.equals(finalization.transactionId()) && hold.business.equals(finalization.decision().operation())
                    && (hold.preparation==null || matchesFinalization(finalization,hold.preparation))) {
                hold.invalidated=true; if (preparing.reservation()!=null) proposalFootprints.release(preparing.reservation().lease); finishParticipantHold(hold,true);
            }
        }
        for (var admission:admissions.values()) if (admission.participant!=null && matchesFinalization(finalization,admission.participant)) retirePreparationScope(admission);
    }
    private static boolean matchesFinalization(ParticipantFinalization finalization,PreparedParticipant.Preparation preparation) {
        try { finalization.requirePreparation(preparation); return true; } catch (IllegalArgumentException different) { return false; }
    }
    private void retirePreparationScope(Admission admission) {
        if (admission.acceptedScope!=null) proposalFootprints.release(admission.acceptedScope);
        if (admission.participantHold!=null) finishParticipantHold(admission.participantHold,true);
    }
    private void releaseFootprint(Admission admission) {
        if(admission.importPublicationHold!=null)releaseImportPublication(admission.importPublicationHold);
        if (admission.fencingHold!=null) releaseSchemaHold(admission.fencingHold);
        if (admission.homeHold!=null) releaseHomeHold(admission.homeHold);
        if (admission.snapshotHold!=null) releaseSnapshotHold(admission.snapshotHold);
        if (admission.footprint!=null) proposalFootprints.release(admission.footprint);
        if (admission.acceptedScope!=null) proposalFootprints.release(admission.acceptedScope);
        if (admission.participantHold!=null) releaseParticipantHold(admission.participantHold,admission.settled);
        if (admission.finalizationHold!=null) releaseFinalizationHold(admission.finalizationHold,admission.settled);
    }
    // Recovery reads one exact native cut outside the admissions monitor. Unknown suffixes remain reserved.
    private NativeAdmissionProof verifiedNativeAdmissionOutcomes() throws Exception {
        final List<Admission> pending;
        synchronized (admissions) { pending=admissions.values().stream().filter(admission -> !admission.settled).toList(); }
        if (pending.isEmpty()) return new NativeAdmissionProof(List.of(),List.of()); var verified=new ArrayList<Admission>(); var terminalScopes=new ArrayList<Admission>();
        try (var pin=store.captureForRecovery()) {
            for (var admission:pending) {
                try {
                    if (admission.snapshotHomeRetirement!=null) { if (pin.snapshotHomeRetired(admission.snapshotHomeRetirement)) verified.add(admission); }
                    else if (admission.snapshotRetirement!=null) { if (pin.snapshotFence(admission.snapshotRetirement.fence())!=null) verified.add(admission); }
                    else if (admission.homeRetirement!=null) { if (pin.homeRetired(admission.homeRetirement)) verified.add(admission); }
                    else if (admission.retirement!=null) { if (pin.participantFence(admission.retirement.identity())!=null) verified.add(admission); }
                    else if (admission.homeRecovery!=null) { if (pin.homeRecoveryResult(admission.homeRecovery)!=null) verified.add(admission); }
                    else if (admission.snapshot!=null) { if (admission.snapshot.receipt(pin.snapshotGateResult(admission.snapshot.fence()))!=null) verified.add(admission); }
                    else if (admission.finalization!=null) { if (pin.finalizationResult(admission.finalization)!=null) verified.add(admission); }
                    else if (admission.participant==null) { if (pin.operationResult(admission.id,admission.digest)!=null) verified.add(admission); }
                    else { var vote=pin.preparationVote(admission.participant); if (admission.participantNegative && vote!=null && vote.prepared()) throw new IllegalArgumentException("Negative preparation contradicts positive proof"); if (vote!=null) verified.add(admission); else if (pin.finalizedPreparation(admission.participant)) terminalScopes.add(admission); }
                }
                catch (IllegalArgumentException differentPayload) { /* A mismatched speculative request is not verified coverage. */ }
            }
        }
        return new NativeAdmissionProof(verified,terminalScopes);
    }
    private record NativeAdmissionProof(List<Admission> settled,List<Admission> terminalScopes) {}
    private void invalidateProposalCandidates() {
        synchronized(admissions){if(relocationHold!=null)releaseRelocation(relocationHold);} synchronized (admissions) { invalidateUnsubmitted(); } }
    private void invalidateUnsubmitted() {
        for(var hold:List.copyOf(sourceHolds.values())){hold.invalidated=true;if(!hold.submitted)releaseSourceHold(hold);}
        if(topologyActivationHold!=null){topologyActivationHold.invalidated=true;if(!topologyActivationHold.submitted)releaseTopologyActivation(topologyActivationHold);}
        if(topologyHold!=null){topologyHold.invalidated=true;if(!topologyHold.submitted)releaseTopologyHold(topologyHold);}
        if(importApplyHold!=null){importApplyHold.invalidated=true;if(!importApplyHold.submitted)releaseImportApply(importApplyHold);}
        for(var hold:List.copyOf(importPublicationHolds.values())){hold.invalidated=true;if(!hold.submitted)releaseImportPublication(hold);}
        if (schemaHold!=null) { schemaHold.invalidated=true; if (!schemaHold.submitted) releaseSchemaHold(schemaHold); }
        snapshotHelpers.clear();
        for (var hold:preparedSnapshotHomeRetirements.values()) { snapshotHomeRetirementSlots--; snapshotHomeRetirementInputBytes-=hold.bytes; proposalFootprints.release(hold.scope); if (hold.resources!=null) { hold.resources.close(); hold.resources=null; } }
        preparedSnapshotHomeRetirements.clear();
        for (var hold:preparingSnapshotHomeRetirements.values()) { hold.invalidated=true; proposalFootprints.release(hold.scope); }
        for (var hold:preparedSnapshotRetirements.values()) { snapshotRetirementSlots--; snapshotRetirementInputBytes-=hold.bytes(); if (hold.resources()!=null) hold.resources().close(); }
        preparedSnapshotRetirements.clear();
        for (var hold:preparedHomeRetirements.values()) { homeRetirementSlots--; homeRetirementInputBytes-=hold.bytes; proposalFootprints.release(hold.scope); if (hold.resourceLease!=null) { hold.resourceLease.close(); hold.resourceLease=null; } }
        preparedHomeRetirements.clear();
        for (var hold:preparingHomeRetirements.values()) { hold.invalidated=true; proposalFootprints.release(hold.scope); }
        for (var hold:preparedRetirements.values()) { retirementSlots--; retirementInputBytes-=hold.bytes(); if (hold.resources()!=null) hold.resources().close(); }
        preparedRetirements.clear();
        proposalFootprints.invalidateCandidates();
        for (var plan:preparedCommits.values()) { plan.closed=true; preparedCommitBytes-=plan.bytes; if (plan.resourceLease!=null) { plan.resourceLease.close(); plan.resourceLease=null; } }
        preparedCommits.clear();
        for (var plan:preparedParticipants.values()) { plan.closed=true; if (!plan.negative) preparedCommitBytes-=plan.bytes.length; releaseParticipantHold(plan.hold); }
        preparedParticipants.clear();
        for (var preparing:preparingParticipants.values()) preparing.hold().invalidated=true;
        for (var plan:List.copyOf(preparedHomes.values())) plan.close();
        for (var hold:preparingHomes.values()) { hold.invalidated=true; if (hold.scope!=null) proposalFootprints.release(hold.scope); }
        for (var plan:List.copyOf(preparedFinalizations.values())) plan.close();
        for (var plan:List.copyOf(preparedSnapshots.values())) plan.close();
        for (var hold:preparingSnapshots.values()) { hold.invalidated=true; if (hold.lease!=null) proposalFootprints.release(hold.lease); }
        // Invalidated native planners still own their input/result payloads until their finally exits.
    }
    /** Only a strictly newer ready native term proves an older term's unknown suffix can no longer interpose. */
    private void reconcileOlderProposalTerms(long term) {
        // Leadership alone does not exclude a possibly appended activation. A newer completed
        // native term does; retained TransactionContexts still own the charged record until release.
        var nativeCut=store.applied();
        if(topologyActivationHold!=null && topologyActivationHold.term<term && nativeCut!=null && nativeCut.term()>=term)releaseTopologyActivation(topologyActivationHold);
        if (term<=reconciledProposalTerm) return;
        invalidateUnsubmitted();
        for(var hold:List.copyOf(sourceHolds.values()))if(hold.term<term)releaseSourceHold(hold);
        for(var hold:List.copyOf(importPublicationHolds.values()))if(hold.term<term)releaseImportPublication(hold);
        if(topologyHold!=null && topologyHold.term<term)releaseTopologyHold(topologyHold);
        if (importApplyHold!=null && importApplyHold.term<term)releaseImportApply(importApplyHold);
        if (schemaHold!=null && schemaHold.term<term) releaseSchemaHold(schemaHold);
        for (var admission:List.copyOf(admissions.values())) if (admission.term<term) settleAdmission(admission);
        reconciledProposalTerm=term;
    }
    public ProposalFootprints.Stats proposalFootprintStats() { synchronized (admissions) { return proposalFootprints.stats(); } }
    /** Unsubmitted exclusive scope guard only. Native validation and canonical append acceptance are still separate work. */
    public final class ProposalReservation implements AutoCloseable {
        private final ProposalFootprints.CandidateLease lease;
        private ProposalReservation(ProposalFootprints.CandidateLease lease) { this.lease=lease; }
        public long term() { return lease.term(); }
        public long generation() { return lease.generation(); }
        public boolean active() {
            var applied=store.applied();
            return lease.active() && lease.generation()==admissionGeneration.get() && applied!=null && applied.term()==lease.term()
                    && storageFailure==null && getLifeCycleState()==LifeCycle.State.RUNNING && store.admissionOpen();
        }
        @Override public void close() { synchronized (admissions) { proposalFootprints.release(lease); } }
    }
    /** Frozen bytes must be retained by the caller before its first Ratis RPC. Closing abandons only unsubmitted work. */
    record ProposalDeadline(java.util.function.LongSupplier nanos,long opened,long ttl) {
        ProposalDeadline { java.util.Objects.requireNonNull(nanos); if (ttl<=0) throw new IllegalArgumentException("Invalid proposal deadline"); }
        boolean active() { return nanos.getAsLong()-opened<ttl; }
    }
    public final class ScopedCommit implements AutoCloseable {
        private final ProposalReservation reservation;
        private final DistributedCommand command;
        private final byte[] digest;
        private final int bytes;
        private final ProposalDeadline deadline;
        private RatisNodeResourceBudget.Lease resourceLease;
        private boolean claimed,closed;
        private ScopedCommit(ProposalReservation reservation,DistributedCommand command,ProposalDeadline deadline) {
            this.reservation=reservation; this.command=command; this.digest=command.payloadDigest(); this.bytes=DistributedCommandCodec.encodedSize(command);
            this.deadline=deadline;
        }
        private boolean authorized() { return !closed && !claimed && reservation.active() && (deadline==null || deadline.active()); }
        public DistributedCommand command() { return command; }
        public boolean submitted() { synchronized (admissions) { return claimed; } }
        @Override public void close() {
            synchronized (admissions) {
                if (closed || claimed) return; closed=true;
                if (preparedCommits.remove(command.operationId(),this)) preparedCommitBytes-=bytes;
                proposalFootprints.release(reservation.lease);
                if (resourceLease!=null) { resourceLease.close(); resourceLease=null; }
            }
        }
    }
    public final class HomeRecoveryProposal implements AutoCloseable {
        private final HomeHold hold;
        private final HomeRecovery recovery;
        private boolean claimed,closed;
        private HomeRecoveryProposal(HomeHold hold) { this.hold=hold; recovery=hold.recovery; }
        private HomeRecoveryProposal(HomeRecovery recovery) { this.hold=null; this.recovery=recovery; claimed=true; }
        public HomeRecovery recovery() { return recovery; }
        public byte[] bytes() { return recovery.encode(); }
        private boolean authorized() {
            var cut=store.applied();
            return !closed && !claimed && !hold.invalidated && !hold.released && hold.generation==admissionGeneration.get()
                    && cut!=null && cut.term()==hold.term && store.admissionOpen() && (hold.scope==null || hold.scope.active());
        }
        @Override public void close() {
            synchronized (admissions) {
                if (closed || claimed) return; closed=true;
                if (preparedHomes.remove(hold.recovery.operation(),this)) releaseHomeHold(hold);
            }
        }
    }
    private HomeRecoveryProposal borrowedHome(HomeHold hold) {
        var view=new HomeRecoveryProposal(hold); view.claimed=true; return view;
    }
    /** Reserve the HOME row and protected input before the qualified DATA proof read. */
    public HomeRecoveryProposal prepareHomeRecovery(HomeRecovery recovery) throws Exception {
        java.util.Objects.requireNonNull(recovery); requireHomeRecovery(recovery); requireHealthy();
        long generation=admissionGeneration.get(); var leader=leadership();
        if (!leader.ready()) throw new IllegalStateException("HOME recovery leader is not ready");
        var mutation=recovery.command().mutations().getFirst();
        var footprint=proposalFootprints.prepare(List.of(TransactionScope.Span.point(mutation.columnId(),mutation.key())));
        var observed=store.estimateHomeRecovery(recovery);
        HomeHold hold;
        synchronized (admissions) {
            requireHealthy(); var cut=store.applied();
            if (generation!=admissionGeneration.get() || cut==null || cut.term()!=leader.term() || !store.admissionOpen()) throw new IllegalStateException("HOME recovery has no current native owner");
            if (!java.util.Objects.equals(observed.capacity().position(),cut)) throw new IllegalStateException("Native HOME cut changed during recovery preparation");
            reconcileOlderProposalTerms(leader.term());
            // Immutable native proof needs no append authority or new protected input reservation.
            if (observed.existing()!=null || observed.retirement()!=null) return new HomeRecoveryProposal(recovery);
            var ready=preparedHomes.get(recovery.operation());
            if (ready!=null) {
                if (!ready.hold.recovery.equals(recovery)) throw new IllegalArgumentException("HOME recovery identity payload changed");
                if (ready.authorized()) return borrowedHome(ready.hold);
            }
            var known=admissions.get(recovery.operation());
            if (known!=null) {
                if (known.homeRecovery==null || !known.homeRecovery.equals(recovery)) throw new IllegalArgumentException("HOME recovery identity payload changed");
                if (known.term==leader.term() && known.generation==generation) {
                    if (known.homeHold!=null) return borrowedHome(known.homeHold);
                }
                throw new IllegalStateException("Previous HOME recovery is still settling");
            }
            if (participantIdentityBusy(recovery.operation())) throw new IllegalStateException("HOME recovery identity is already owned");
            requireHomeInput(recovery.encode().length);
            var scope=observed.existing()==null ? proposalFootprints.reserveCandidate(footprint,leader.term(),generation) : null;
            if (observed.existing()==null && scope==null) throw new IllegalStateException("HOME recovery row overlaps in-flight work");
            RatisNodeResourceBudget.Lease resources;
            try { resources=reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+4L*HomeRecovery.MAX_BYTES,1); }
            catch (IOException failure) { if (scope!=null) proposalFootprints.release(scope); throw failure; }
            hold=new HomeHold(recovery,leader.term(),generation,scope); hold.resourceLease=resources;
            preparingHomes.put(recovery.operation(),hold); homeOwners.put(recovery.operation(),hold);
            homeRecoverySlots++; homeRecoveryInputBytes+=hold.bytes;
        }
        boolean retained=false;
        try {
            boolean existing=reserveHomeCapacity(hold);
            if (!existing && recovery.action()==HomeRecovery.Action.ACK) {
                if (recovery.proof().fenced()) {
                    var reader=homeRecoveryFences; if (reader==null) throw new IllegalStateException("HOME recovery has no qualified DATA fence reader");
                    recovery.requireFence(reader.read(new ParticipantIdentity(recovery.command().groupId(),recovery.command().rangeEpoch(),recovery.proof().participant(),recovery.before())));
                } else {
                    var reader=homeRecoveryProofs; if (reader==null) throw new IllegalStateException("HOME recovery has no qualified DATA reader");
                    recovery.requireOutcome(reader.read(recovery));
                }
            }
            reserveHomeCapacity(hold);
            synchronized (admissions) {
                requireHomeHold(hold);
                var proposal=new HomeRecoveryProposal(hold); preparedHomes.put(recovery.operation(),proposal);
                preparingHomes.remove(recovery.operation(),hold); retained=true; return proposal;
            }
        } finally {
            synchronized (admissions) {
                if (!retained && preparingHomes.remove(recovery.operation(),hold)) releaseHomeHold(hold);
            }
        }
    }
    private void requireHomeInput(int bytes) {
        if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16
                || bytes>16L*1024*1024-homeRecoveryInputBytes) throw new IllegalStateException("Protected HOME input budget exhausted");
    }
    private void requireHomeHold(HomeHold hold) throws IOException {
        requireHealthy(); var cut=store.applied();
        if (hold.invalidated || hold.released || hold.generation!=admissionGeneration.get() || homeOwners.get(hold.recovery.operation())!=hold
                || cut==null || cut.term()!=hold.term || !store.admissionOpen() || hold.scope!=null && !hold.scope.active()) throw new IllegalStateException("HOME recovery lost its acceptance owner");
    }
    private boolean reserveHomeCapacity(HomeHold hold) throws Exception {
        for (int attempt=0;attempt<3;attempt++) {
            var estimate=store.estimateHomeRecovery(hold.recovery); var capacity=estimate.capacity();
            synchronized (admissions) {
                requireHomeHold(hold);
                if (!java.util.Objects.equals(capacity.position(),store.applied())) continue;
                reserveHomeCapacity(hold,estimate); return estimate.existing()!=null || estimate.retirement()!=null;
            }
        }
        throw new IllegalStateException("Native HOME cut kept changing");
    }
    private void reserveHomeCapacity(HomeHold hold,ReplicatedRangeStore.HomeRecoveryEstimate estimate) {
        var capacity=estimate.capacity(); long count=estimate.additionalCount(),bytes=estimate.additionalBytes();
        if (!retentionFits(capacity.effectiveCount(),reservedHomeCount-hold.count,count,ReplicatedRangeStore.MAX_HOME_RECOVERIES)
                || !retentionFits(capacity.effectiveBytes(),reservedHomeBytes-hold.capacityBytes,bytes,ReplicatedRangeStore.MAX_HOME_RECOVERY_BYTES)) throw new IllegalStateException("Protected HOME recovery capacity exhausted");
        reservedHomeCount+=count-hold.count; reservedHomeBytes+=bytes-hold.capacityBytes; hold.count=count; hold.capacityBytes=bytes;
    }
    private void releaseHomeHold(HomeHold hold) {
        if (hold.released) return; hold.released=true;
        if (hold.resourceLease!=null) { hold.resourceLease.close(); hold.resourceLease=null; }
        homeOwners.remove(hold.recovery.operation(),hold);
        reservedHomeCount-=hold.count; reservedHomeBytes-=hold.capacityBytes;
        if (hold.scope!=null) proposalFootprints.release(hold.scope);
        if (!hold.transferred) { homeRecoverySlots--; homeRecoveryInputBytes-=hold.bytes; }
    }
    private TransactionContext startHomeRecovery(TransactionContext transaction,byte[] encoded,long generation,Leadership leader) throws Exception {
        var recovery=HomeRecovery.decode(encoded); byte[] digest=recovery.digest();
        for (int attempt=0;attempt<3;attempt++) {
            var estimate=store.estimateHomeRecovery(recovery); var cut=estimate.capacity().position();
            if (cut==null || cut.term()!=leader.term()) throw new IllegalStateException("HOME recovery native prefix is not ready");
            synchronized (admissions) {
                requireHealthy();
                if (generation!=admissionGeneration.get() || !store.admissionOpen()) throw new IllegalStateException("HOME recovery owner changed");
                if (!java.util.Objects.equals(cut,store.applied())) continue;
                reconcileOlderProposalTerms(leader.term());
                var known=admissions.get(recovery.operation());
                if (known!=null) {
                    if (known.homeRecovery==null || !Arrays.equals(known.digest,digest)) throw new IllegalArgumentException("HOME recovery identity payload changed");
                    if (estimate.existing()!=null || estimate.retirement()!=null) settleAdmission(known);
                    known=admissions.get(recovery.operation());
                    if (known!=null) {
                        if (known.term!=leader.term() || known.generation!=generation) throw new IllegalStateException("Previous HOME recovery is still settling");
                        known.owners++; transaction.setStateMachineContext(known); return transaction;
                    }
                }
                var accepted=preparedHomes.get(recovery.operation());
                if (accepted!=null && (!accepted.hold.recovery.equals(recovery) || !accepted.authorized())) throw new IllegalStateException("HOME recovery preparation changed or expired");
                if (estimate.existing()==null && estimate.retirement()==null && accepted==null) throw new IllegalStateException("HOME recovery requires privileged proof admission");
                if (accepted==null && participantIdentityBusy(recovery.operation())) throw new IllegalStateException("HOME recovery identity is still owned");
                if (accepted==null) requireHomeInput(encoded.length);
                else { requireHomeHold(accepted.hold); reserveHomeCapacity(accepted.hold,estimate); }
                var hold=accepted==null ? null : accepted.hold;
                var admission=new Admission(recovery.operation(),digest,encoded.length,leader.term(),generation,0,0,false,true,null,hold==null ? null : hold.scope,null);
                admission.homeRecovery=recovery; admission.homeHold=hold;
                if (hold!=null && hold.resourceLease!=null) { admission.resourceLeases.add(hold.resourceLease); hold.resourceLease=null; }
                if (hold!=null) {
                    if (hold.scope!=null) proposalFootprints.submit(hold.scope); hold.transferred=true; accepted.claimed=true;
                    preparedHomes.remove(recovery.operation(),accepted); homeRecoverySlots--;
                } else homeRecoveryInputBytes+=encoded.length;
                admissions.put(admission.id,admission); protectedAdmissions++; admittedBytes+=admission.bytes;
                transaction.setStateMachineContext(admission); return transaction;
            }
        }
        throw new IllegalStateException("Native HOME cut changed during append admission");
    }
    private void checkHomeOwner() throws IOException {
        requireHealthy(); var owner=leadership(); var cut=store.applied();
        if ((role!=RatisNodePlan.Role.TRANSACTION_HOME && role!=RatisNodePlan.Role.SNAPSHOT_HOME) || !owner.ready() || cut==null || cut.term()!=owner.term() || !store.admissionOpen()) throw new IOException("HOME operation requires a current completed leader prefix");
    }
    public void cancelHomeRecovery(HomeRecovery recovery) throws IOException {
        requireHomeRecovery(recovery); checkHomeOwner();
        synchronized (admissions) {
            var hold=homeOwners.get(recovery.operation());
            if (hold==null) return;
            if (!hold.recovery.equals(recovery)) throw new IllegalArgumentException("HOME cancellation identity payload changed");
            var ready=preparedHomes.get(recovery.operation());
            if (ready!=null) ready.close();
            else if (preparingHomes.get(recovery.operation())==hold) { hold.invalidated=true; if (hold.scope!=null) proposalFootprints.release(hold.scope); }
            // Claimed/uncertain append ownership is never donated by cancellation.
        }
    }
    public long homeRecoveryInputBytes() { synchronized (admissions) { return homeRecoveryInputBytes; } }
    public int homeRecoverySlots() { synchronized (admissions) { return homeRecoverySlots; } }
    public long reservedHomeRecoveryCount() { synchronized (admissions) { return reservedHomeCount; } }

    public final class FinalizationProposal implements AutoCloseable {
        private final FinalizationHold hold; private final ParticipantFinalization finalization; private final byte[] bytes,digest;
        private boolean claimed,closed;
        private FinalizationProposal(FinalizationHold hold,ParticipantFinalization finalization) { this.hold=hold; this.finalization=finalization; bytes=RatisParticipantRecord.encode(finalization); digest=RatisParticipantRecord.digest(finalization); }
        public ParticipantFinalization finalization() { return finalization; }
        public byte[] bytes() { return bytes.clone(); }
        private boolean authorized() { var a=store.applied(); return !closed && !claimed && hold.generation==admissionGeneration.get() && a!=null && a.term()==hold.term && store.admissionOpen(); }
        @Override public void close() {
            synchronized (admissions) {
                if (closed || claimed) return; closed=true;
                if (preparedFinalizations.remove(hold.recovery,this)) { protectedFinalizationSlots--; releaseFinalizationHold(hold,false); }
            }
        }
    }
    // Reattach exact live acceptance. The view must not close or donate the original owner's reservation.
    private FinalizationProposal borrowedFinalization(FinalizationHold hold,ParticipantFinalization finalization) {
        var proposal=new FinalizationProposal(hold,finalization); proposal.claimed=true; return proposal;
    }
    /** Privileged coordinator seam. Bounded protected capacity is held while the fresh home read is in flight. */
    public FinalizationProposal prepareFinalization(RatisTransactionStatuses homes,UUID transaction,OperationId recovery) throws Exception { requireDataRole();
        java.util.Objects.requireNonNull(homes); java.util.Objects.requireNonNull(transaction); java.util.Objects.requireNonNull(recovery);
        requireHealthy(); long generation=admissionGeneration.get(); var leader=leadership(); if (!leader.ready()) throw new IllegalStateException("Finalization leader is not ready");
        FinalizationHold hold;
        synchronized (admissions) {
            requireHealthy(); var a=store.applied();
            if (generation!=admissionGeneration.get() || a==null || a.term()!=leader.term() || !store.admissionOpen()) throw new IllegalStateException("Finalization has no current native owner");
            reconcileOlderProposalTerms(leader.term());
            var ready=preparedFinalizations.get(recovery);
            if (ready!=null && ready.finalization.transactionId().equals(transaction) && ready.authorized()) return borrowedFinalization(ready.hold,ready.finalization);
            var known=admissions.get(recovery);
            if (known!=null && known.finalization!=null && known.finalization.transactionId().equals(transaction) && known.term==leader.term() && known.generation==generation) return borrowedFinalization(known.finalizationHold,known.finalization);
            if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16) throw new IllegalStateException("Protected proposal budget exhausted");
            if (participantIdentityBusy(recovery) || finalizingTransactions.containsKey(transaction)) throw new IllegalStateException("Finalization identity/transaction is already owned");
            var resources=reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+4L*ParticipantFinalization.MAX_BYTES,1);
            hold=new FinalizationHold(transaction,recovery,leader.term(),generation); hold.resourceLease=resources; preparingFinalizations.put(recovery,hold); finalizingTransactions.put(transaction,hold); finalizationOwners.put(recovery,hold); protectedFinalizationSlots++;
        }
        boolean retained=false;
        try {
            var finalization=homes.readFinalization(transaction,store.ownership().owner().groupId(),recovery);
            for (int attempt=0;attempt<3;attempt++) {
                var estimate=store.estimateFinalization(finalization); var capacity=estimate.capacity();
                synchronized (admissions) {
                    requireHealthy();
                    if (hold.generation!=admissionGeneration.get() || preparingFinalizations.get(recovery)!=hold || capacity.position()==null || capacity.position().term()!=hold.term || !store.admissionOpen()) throw new IllegalStateException("Finalization lost its acceptance owner");
                    if (!java.util.Objects.equals(capacity.position(),store.applied())) continue;
                    var business=finalization.decision().operation();
                    if (finalizationOwners.containsKey(business) || preparedCommits.containsKey(business) || preparingCommits.containsKey(business)) throw new IllegalStateException("Finalization business identity belongs to other work");
                    var pending=admissions.get(business);
                    if (pending!=null && (pending.participant==null || !pending.participant.transactionId().equals(transaction))) throw new IllegalStateException("Business operation is still settling outside this transaction");
                    var shared=participantOwners.get(business);
                    if (shared!=null) {
                        if (!shared.transaction.equals(transaction)) throw new IllegalArgumentException("Finalization shares another transaction's identity");
                        if (shared.preparation!=null) finalization.requirePreparation(shared.preparation);
                    }
                    if (estimate.newFence()) {
                        if (shared!=null && !shared.capacityReleased) { shared.finalizers++; if (shared.payload!=null) shared.payload.references++; hold.shared=shared; }
                        else {
                            if (capacity.count()+reservedParticipantCount+reservedFinalizationCount>=ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS || ReplicatedRangeStore.FINALIZATION_RESERVATION>ReplicatedRangeStore.MAX_PREPARED_BYTES-capacity.bytes()-reservedParticipantBytes-reservedFinalizationBytes) throw new IllegalStateException("Finalization fence reservation exhausted");
                            hold.capacity=true; reservedFinalizationCount++; reservedFinalizationBytes+=ReplicatedRangeStore.FINALIZATION_RESERVATION;
                        }
                    }
                    hold.business=business; finalizationOwners.put(business,hold);
                    var proposal=new FinalizationProposal(hold,finalization); preparedFinalizations.put(recovery,proposal); preparingFinalizations.remove(recovery,hold); retained=true; return proposal;
                }
            }
            throw new IllegalStateException("Native finalization cut kept changing");
        } finally {
            synchronized (admissions) { if (!retained && preparingFinalizations.remove(recovery,hold)) { protectedFinalizationSlots--; releaseFinalizationHold(hold,false); } }
        }
    }
    private TransactionContext startFinalization(TransactionContext transaction,byte[] encoded,long generation,Leadership leadership) throws Exception {
        var finalization=RatisParticipantRecord.decodeFinalization(encoded); byte[] digest=RatisParticipantRecord.digest(finalization);
        for (int attempt=0;attempt<3;attempt++) {
            var estimate=store.estimateFinalization(finalization); var observed=estimate.capacity().position();
            if (observed==null || observed.term()!=leadership.term()) throw new IllegalStateException("Finalization native prefix is not ready");
            synchronized (admissions) {
                requireHealthy(); if (generation!=admissionGeneration.get() || !store.admissionOpen()) throw new IllegalStateException("Finalization owner changed");
                if (!java.util.Objects.equals(observed,store.applied())) continue; reconcileOlderProposalTerms(leadership.term());
                var known=admissions.get(finalization.operation());
                if (known!=null && !Arrays.equals(known.digest,digest)) throw new IllegalArgumentException("Finalization identity payload changed");
                if (known!=null && (estimate.existing()!=null || estimate.retired())) { settleAdmission(known); known=admissions.get(finalization.operation()); }
                if (known!=null) {
                    if (known.finalization==null || known.term!=leadership.term() || known.generation!=generation) throw new IllegalStateException("Previous finalization is still settling");
                    known.owners++; transaction.setStateMachineContext(known); return transaction;
                }
                var accepted=estimate.existing()==null && !estimate.retired() ? preparedFinalizations.get(finalization.operation()) : null;
                if (estimate.existing()==null && !estimate.retired() && (accepted==null || !Arrays.equals(accepted.digest,digest) || !accepted.authorized())) throw new IllegalStateException("Finalization has no fresh home acceptance authority");
                if (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots-(accepted==null ? 0 : 1)>=16) throw new IllegalStateException("Protected proposal budget exhausted");
                var admission=new Admission(finalization.operation(),digest,encoded.length,leadership.term(),generation,0,0,false,true,null,null,null);
                admission.finalization=finalization; admission.finalizationHold=accepted==null ? null : accepted.hold; if (accepted!=null && accepted.hold.resourceLease!=null) { admission.resourceLeases.add(accepted.hold.resourceLease); accepted.hold.resourceLease=null; } admissions.put(admission.id,admission);
                if (accepted!=null) { accepted.claimed=true; preparedFinalizations.remove(admission.id,accepted); protectedFinalizationSlots--; }
                protectedAdmissions++; admittedBytes+=admission.bytes; transaction.setStateMachineContext(admission); return transaction;
            }
        }
        throw new IllegalStateException("Native finalization cut changed during append admission");
    }
    public void cancelFinalization(ParticipantFinalization finalization) throws IOException {
        checkTransactionOwner();
        synchronized (admissions) {
            var ready=preparedFinalizations.get(finalization.operation());
            if (ready!=null) {
                if (!ready.finalization.equals(finalization)) throw new IllegalArgumentException("Finalization cancellation payload changed");
                ready.close();
            }
            var preparing=preparingFinalizations.get(finalization.operation());
            if (preparing!=null) throw new IllegalStateException("Finalization HOME read is still active");
            var admitted=admissions.get(finalization.operation());
            if (admitted!=null && (admitted.finalization==null || !admitted.finalization.equals(finalization))) throw new IllegalArgumentException("Finalization cancellation identity changed");
            // Submitted unknown work retains scope, input and capacity until verified native settlement.
        }
    }
    private boolean prepareSnapshotHomeRetirement(SnapshotHomeRetirement retirement) throws Exception {
        requireSnapshotHomeIdentity(retirement.identity()); checkHomeOwner(); long generation=admissionGeneration.get(); var leader=leadership();
        if (!store.estimateSnapshotHomeRetirement(retirement)) return true;
        var operation=RatisSnapshotHomeRetirementRecord.operation(retirement); SnapshotHomeRetirementHold hold;
        synchronized (admissions) {
            checkHomeOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot HOME retirement owner changed"); reconcileOlderProposalTerms(leader.term());
            var ready=preparedSnapshotHomeRetirements.get(operation); if (ready!=null && !ready.invalidated && ready.term==leader.term() && ready.generation==generation && ready.retirement.equals(retirement)) return false;
            var known=admissions.get(operation); if (known!=null) { if (retirement.equals(known.snapshotHomeRetirement) && known.term==leader.term() && known.generation==generation) return false; throw new IOException("Snapshot HOME retirement identity is still owned"); }
            int bytes=RatisSnapshotHomeRetirementRecord.encode(retirement).length;
            if (participantIdentityBusy(operation) || sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16 || bytes>16L*1024*1024-snapshotHomeRetirementInputBytes) throw new IOException("Protected snapshot HOME retirement input exhausted");
            var id=retirement.identity(); byte[] key=ByteBuffer.allocate(16).putLong(id.snapshotId().getMostSignificantBits()).putLong(id.snapshotId().getLeastSignificantBits()).array();
            var scope=proposalFootprints.reserveCandidate(proposalFootprints.prepare(List.of(TransactionScope.Span.point(id.columnId(),key))),leader.term(),generation); if (scope==null) throw new IOException("Snapshot HOME retirement row overlaps in-flight work");
            RatisNodeResourceBudget.Lease resources;
            try { resources=reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+4L*SnapshotHomeRetirement.MAX_BYTES,1); }
            catch (IOException failure) { proposalFootprints.release(scope); throw failure; }
            hold=new SnapshotHomeRetirementHold(retirement,leader.term(),generation,scope); hold.resources=resources; preparingSnapshotHomeRetirements.put(operation,hold); snapshotHomeRetirementSlots++; snapshotHomeRetirementInputBytes+=hold.bytes;
        }
        boolean retained=false;
        try {
            var reader=snapshotHomeProofs; if (reader==null) throw new IOException("Snapshot HOME retirement lacks a qualified DATA reader");
            for (var frozen:retirement.participants()) { var actual=reader.read(frozen.fence(),frozen.participantGroup()); if (actual==null || !actual.covers(frozen)) throw new IOException("Snapshot HOME retirement lacks fresh covering DATA evidence"); }
            boolean needed=store.estimateSnapshotHomeRetirement(retirement);
            synchronized (admissions) { checkHomeOwner(); if (hold.invalidated || generation!=admissionGeneration.get() || leadership().term()!=hold.term || !hold.scope.active()) throw new IOException("Snapshot HOME retirement lost its owner"); if (!needed) return true; preparingSnapshotHomeRetirements.remove(operation,hold); preparedSnapshotHomeRetirements.put(operation,hold); retained=true; return false; }
        } finally { synchronized (admissions) { if (!retained && preparingSnapshotHomeRetirements.remove(operation,hold)) { snapshotHomeRetirementSlots--; snapshotHomeRetirementInputBytes-=hold.bytes; proposalFootprints.release(hold.scope); if (hold.resources!=null) { hold.resources.close(); hold.resources=null; } } } }
    }
    private void cancelSnapshotHomeRetirement(SnapshotHomeRetirement retirement) throws IOException {
        var operation=RatisSnapshotHomeRetirementRecord.operation(retirement);
        synchronized (admissions) {
            if (preparingSnapshotHomeRetirements.containsKey(operation)) throw new IOException("Snapshot HOME retirement DATA read is still active"); var hold=preparedSnapshotHomeRetirements.get(operation);
            if (hold!=null) { if (!hold.retirement.equals(retirement)) throw new IOException("Snapshot HOME retirement identity changed"); preparedSnapshotHomeRetirements.remove(operation,hold); snapshotHomeRetirementSlots--; snapshotHomeRetirementInputBytes-=hold.bytes; proposalFootprints.release(hold.scope); if (hold.resources!=null) { hold.resources.close(); hold.resources=null; } }
            var admitted=admissions.get(operation); if (admitted!=null && !retirement.equals(admitted.snapshotHomeRetirement)) throw new IOException("Snapshot HOME retirement identity changed");
        }
    }
    private TransactionContext startSnapshotHomeRetirement(TransactionContext transaction,SnapshotHomeRetirement retirement,byte[] encoded,long generation,Leadership leader) throws Exception {
        requireSnapshotHomeIdentity(retirement.identity()); boolean needed=store.estimateSnapshotHomeRetirement(retirement); var operation=RatisSnapshotHomeRetirementRecord.operation(retirement);
        synchronized (admissions) {
            checkHomeOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot HOME retirement owner changed"); reconcileOlderProposalTerms(leader.term()); var known=admissions.get(operation);
            if (known!=null) { if (!retirement.equals(known.snapshotHomeRetirement) || known.term!=leader.term() || known.generation!=generation) throw new IOException("Snapshot HOME retirement is still settling"); known.owners++; transaction.setStateMachineContext(known); return transaction; }
            var accepted=preparedSnapshotHomeRetirements.get(operation); if (needed && (accepted==null || accepted.invalidated || accepted.term!=leader.term() || accepted.generation!=generation || !accepted.retirement.equals(retirement) || !accepted.scope.active())) throw new IOException("Snapshot HOME retirement lacks fresh DATA authority");
            if (accepted==null && (participantIdentityBusy(operation) || sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16 || encoded.length>16L*1024*1024-snapshotHomeRetirementInputBytes)) throw new IOException("Protected snapshot HOME retirement input exhausted");
            var admission=new Admission(operation,retirement.digest(),encoded.length,leader.term(),generation,0,0,false,true,null,accepted==null ? null : accepted.scope,null); admission.snapshotHomeRetirement=retirement;
            if (accepted!=null) { if (accepted.resources!=null) { admission.resourceLeases.add(accepted.resources); accepted.resources=null; } proposalFootprints.submit(accepted.scope); preparedSnapshotHomeRetirements.remove(operation,accepted); snapshotHomeRetirementSlots--; } else snapshotHomeRetirementInputBytes+=encoded.length;
            admissions.put(operation,admission); protectedAdmissions++; admittedBytes+=encoded.length; transaction.setStateMachineContext(admission); return transaction;
        }
    }
    private boolean prepareSnapshotRetirement(SnapshotGateRetirement retirement) throws Exception {
        requireDataRole(); checkTransactionOwner(); long generation=admissionGeneration.get(); var leader=leadership();
        if (!store.estimateSnapshotGateRetirement(retirement)) return true;
        var operation=RatisSnapshotRetirementRecord.operation(retirement); SnapshotRetirementHold hold; int inputBytes=4+SnapshotGateRetirement.MAX_BYTES;
        synchronized (admissions) {
            checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot retirement owner changed"); reconcileOlderProposalTerms(leader.term());
            var ready=preparedSnapshotRetirements.get(operation); if (ready!=null && ready.term()==leader.term() && ready.generation()==generation && ready.retirement().equals(retirement)) return false;
            var known=admissions.get(operation); if (known!=null) { if (retirement.equals(known.snapshotRetirement) && known.term==leader.term() && known.generation==generation) return false; throw new IOException("Snapshot retirement identity is still owned"); }
            if (participantIdentityBusy(operation) || snapshotRetirementSlots>=8 || sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16 || inputBytes>16L*1024*1024-snapshotRetirementInputBytes) throw new IOException("Protected snapshot retirement input budget exhausted");
            var resources=reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+4L*SnapshotGateRetirement.MAX_BYTES,1);
            try { hold=new SnapshotRetirementHold(retirement,leader.term(),generation,RatisSnapshotRetirementRecord.encode(retirement).length,resources); }
            catch (RuntimeException | Error failure) { if (resources!=null) resources.close(); throw failure; }
            preparingSnapshotRetirements.put(operation,hold); snapshotRetirementSlots++; snapshotRetirementInputBytes+=hold.bytes();
        }
        boolean retained=false;
        try {
            var homes=snapshotHomes; if (homes==null) throw new IOException("Qualified snapshot HOME reader is not configured"); var read=homes.readRegistration(retirement.fence().fenceId());
            if (!read.descriptor().groupId().equals(retirement.fence().homeGroup()) || read.descriptor().rangeEpoch()!=retirement.fence().homeEpoch() || !retirement.consumed().equals(read.status())) throw new IOException("Snapshot retirement differs from fresh consumed HOME authority");
            boolean needed=store.estimateSnapshotGateRetirement(retirement);
            synchronized (admissions) { checkTransactionOwner(); if (generation!=admissionGeneration.get() || leadership().term()!=hold.term()) throw new IOException("Snapshot retirement lost its owner"); if (!needed) return true; preparingSnapshotRetirements.remove(operation,hold); preparedSnapshotRetirements.put(operation,hold); retained=true; return false; }
        } finally { synchronized (admissions) { if (!retained && preparingSnapshotRetirements.remove(operation,hold)) { snapshotRetirementSlots--; snapshotRetirementInputBytes-=hold.bytes(); if (hold.resources()!=null) hold.resources().close(); } } }
    }
    private void cancelSnapshotRetirement(SnapshotGateRetirement retirement) throws IOException {
        var operation=RatisSnapshotRetirementRecord.operation(retirement);
        synchronized (admissions) {
            if (preparingSnapshotRetirements.containsKey(operation)) throw new IOException("Snapshot retirement HOME read is still active");
            var hold=preparedSnapshotRetirements.get(operation); if (hold!=null) { if (!hold.retirement().equals(retirement)) throw new IOException("Snapshot retirement identity changed"); preparedSnapshotRetirements.remove(operation,hold); snapshotRetirementSlots--; snapshotRetirementInputBytes-=hold.bytes(); if (hold.resources()!=null) hold.resources().close(); }
            var admitted=admissions.get(operation); if (admitted!=null && !retirement.equals(admitted.snapshotRetirement)) throw new IOException("Snapshot retirement identity changed");
        }
    }
    private TransactionContext startSnapshotRetirement(TransactionContext transaction,SnapshotGateRetirement retirement,byte[] encoded,long generation,Leadership leader) throws Exception {
        requireDataRole(); boolean needed=store.estimateSnapshotGateRetirement(retirement); var operation=RatisSnapshotRetirementRecord.operation(retirement);
        synchronized (admissions) {
            checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot retirement owner changed"); reconcileOlderProposalTerms(leader.term()); var known=admissions.get(operation);
            if (known!=null) { if (!retirement.equals(known.snapshotRetirement) || known.term!=leader.term() || known.generation!=generation) throw new IOException("Snapshot retirement is still settling"); known.owners++; transaction.setStateMachineContext(known); return transaction; }
            var accepted=preparedSnapshotRetirements.get(operation); if (needed && (accepted==null || accepted.term()!=leader.term() || accepted.generation()!=generation || !accepted.retirement().equals(retirement))) throw new IOException("Snapshot retirement lacks fresh HOME authority");
            if (accepted==null && (participantIdentityBusy(operation) || sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16 || encoded.length>16L*1024*1024-snapshotRetirementInputBytes)) throw new IOException("Protected snapshot retirement input budget exhausted");
            var admission=new Admission(operation,retirement.digest(),encoded.length,leader.term(),generation,0,0,false,true,null,null,null); admission.snapshotRetirement=retirement;
            if (accepted!=null) { if (accepted.resources()!=null) admission.resourceLeases.add(accepted.resources()); preparedSnapshotRetirements.remove(operation,accepted); snapshotRetirementSlots--; } else snapshotRetirementInputBytes+=encoded.length;
            admissions.put(operation,admission); protectedAdmissions++; admittedBytes+=encoded.length; transaction.setStateMachineContext(admission); return transaction;
        }
    }
    private boolean prepareHomeRetirement(HomeRetirement retirement) throws Exception {
        requireHomeRetirement(retirement); checkHomeOwner(); long generation=admissionGeneration.get(); var leader=leadership();
        if (!store.estimateHomeRetirement(retirement)) return true;
        var operation=RatisHomeRetirementRecord.operation(retirement); HomeRetirementHold hold;
        synchronized (admissions) {
            checkHomeOwner(); if (generation!=admissionGeneration.get()) throw new IOException("HOME retirement owner changed"); reconcileOlderProposalTerms(leader.term());
            var ready=preparedHomeRetirements.get(operation);
            if (ready!=null && !ready.invalidated && ready.term==leader.term() && ready.generation==generation && ready.retirement.equals(retirement)) return false;
            var known=admissions.get(operation);
            if (known!=null) {
                if (retirement.equals(known.homeRetirement) && known.term==leader.term() && known.generation==generation) return false;
                throw new IOException("HOME retirement identity is still owned");
            }
            int bytes=RatisHomeRetirementRecord.encode(retirement).length;
            if (participantIdentityBusy(operation) || sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16
                    || bytes>1024L*1024-homeRetirementInputBytes) throw new IOException("Protected HOME retirement input budget exhausted");
            var id=retirement.identity(); byte[] key=ByteBuffer.allocate(16).putLong(id.transactionId().getMostSignificantBits()).putLong(id.transactionId().getLeastSignificantBits()).array();
            var scope=proposalFootprints.reserveCandidate(proposalFootprints.prepare(List.of(TransactionScope.Span.point(id.columnId(),key))),leader.term(),generation);
            if (scope==null) throw new IOException("HOME retirement row overlaps in-flight work");
            RatisNodeResourceBudget.Lease resources;
            try { resources=reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+4L*HomeRetirement.MAX_BYTES,1); }
            catch (IOException failure) { proposalFootprints.release(scope); throw failure; }
            hold=new HomeRetirementHold(retirement,leader.term(),generation,scope); hold.resourceLease=resources; preparingHomeRetirements.put(operation,hold); homeRetirementSlots++; homeRetirementInputBytes+=hold.bytes;
        }
        boolean retained=false;
        try {
            if (retirement.consumed()!=null) {
                var reader=homeRecoveryFences; if (reader==null) throw new IOException("HOME retirement has no qualified DATA reader");
                for (var frozen:retirement.participants()) { var actual=reader.read(frozen.identity()); if (actual==null || !actual.covers(frozen)) throw new IOException("HOME retirement lacks a fresh covering DATA fence"); }
            }
            boolean needed=store.estimateHomeRetirement(retirement);
            synchronized (admissions) {
                checkHomeOwner(); if (hold.invalidated || generation!=admissionGeneration.get() || leadership().term()!=hold.term || !hold.scope.active()) throw new IOException("HOME retirement lost its owner");
                if (!needed) return true;
                preparingHomeRetirements.remove(operation,hold); preparedHomeRetirements.put(operation,hold); retained=true; return false;
            }
        } finally { synchronized (admissions) { if (!retained && preparingHomeRetirements.remove(operation,hold)) { homeRetirementSlots--; homeRetirementInputBytes-=hold.bytes; proposalFootprints.release(hold.scope); if (hold.resourceLease!=null) { hold.resourceLease.close(); hold.resourceLease=null; } } } }
    }
    private void cancelHomeRetirement(HomeRetirement retirement) throws IOException {
        var operation=RatisHomeRetirementRecord.operation(retirement);
        synchronized (admissions) {
            if (preparingHomeRetirements.containsKey(operation)) throw new IOException("HOME retirement DATA read is still active");
            var hold=preparedHomeRetirements.get(operation);
            if (hold!=null) { if (!hold.retirement.equals(retirement)) throw new IOException("HOME retirement identity changed"); preparedHomeRetirements.remove(operation,hold); homeRetirementSlots--; homeRetirementInputBytes-=hold.bytes; proposalFootprints.release(hold.scope); if (hold.resourceLease!=null) { hold.resourceLease.close(); hold.resourceLease=null; } }
            var admitted=admissions.get(operation); if (admitted!=null && !retirement.equals(admitted.homeRetirement)) throw new IOException("HOME retirement identity changed");
        }
    }
    private TransactionContext startHomeRetirement(TransactionContext transaction,HomeRetirement retirement,byte[] encoded,long generation,Leadership leader) throws Exception {
        requireHomeRetirement(retirement); boolean needed=store.estimateHomeRetirement(retirement); var operation=RatisHomeRetirementRecord.operation(retirement);
        synchronized (admissions) {
            checkHomeOwner(); if (generation!=admissionGeneration.get()) throw new IOException("HOME retirement owner changed"); reconcileOlderProposalTerms(leader.term());
            var known=admissions.get(operation);
            if (known!=null) {
                if (!retirement.equals(known.homeRetirement) || known.term!=leader.term() || known.generation!=generation) throw new IOException("HOME retirement is still settling");
                known.owners++; transaction.setStateMachineContext(known); return transaction;
            }
            var accepted=preparedHomeRetirements.get(operation);
            if (needed && (accepted==null || accepted.invalidated || accepted.term!=leader.term() || accepted.generation!=generation || !accepted.retirement.equals(retirement) || !accepted.scope.active())) throw new IOException("HOME retirement lacks fresh DATA authority");
            if (accepted==null && (participantIdentityBusy(operation) || sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16 || encoded.length>1024L*1024-homeRetirementInputBytes)) throw new IOException("Protected HOME retirement input budget exhausted");
            var admission=new Admission(operation,retirement.digest(),encoded.length,leader.term(),generation,0,0,false,true,null,accepted==null ? null : accepted.scope,null); admission.homeRetirement=retirement;
            if (accepted!=null) { if (accepted.resourceLease!=null) { admission.resourceLeases.add(accepted.resourceLease); accepted.resourceLease=null; } proposalFootprints.submit(accepted.scope); preparedHomeRetirements.remove(operation,accepted); homeRetirementSlots--; } else homeRetirementInputBytes+=encoded.length;
            admissions.put(operation,admission); protectedAdmissions++; admittedBytes+=encoded.length; transaction.setStateMachineContext(admission); return transaction;
        }
    }
    private boolean prepareRetirement(ParticipantRetirement retirement) throws Exception {
        requireDataRole(); requireHealthy(); var leader=leadership(); long generation=admissionGeneration.get();
        if (!leader.ready()) throw new IOException("Retirement leader is not ready");
        if (!store.estimateParticipantRetirement(retirement)) return true;
        var operation=RatisParticipantRecord.operation(retirement); RetirementHold hold; int inputBytes=8+ParticipantRetirement.MAX_BYTES;
        synchronized (admissions) {
            requireHealthy(); if (generation!=admissionGeneration.get() || !store.admissionOpen()) throw new IOException("Retirement owner changed");
            var cut=store.applied(); if (cut==null || cut.term()!=leader.term()) throw new IOException("Retirement native prefix is not ready");
            reconcileOlderProposalTerms(leader.term());
            var ready=preparedRetirements.get(operation);
            if (ready!=null && ready.term()==leader.term() && ready.generation()==generation && ready.retirement().equals(retirement)) return false;
            var known=admissions.get(operation);
            if (known!=null) {
                if (retirement.equals(known.retirement) && known.term==leader.term() && known.generation==generation) return false;
                throw new IOException("Retirement identity is still owned");
            }
            if (participantIdentityBusy(operation) || sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16
                    || inputBytes>1024L*1024-retirementInputBytes) throw new IOException("Protected retirement input budget exhausted");
            var resources=reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+4L*ParticipantRetirement.MAX_BYTES,1);
            try { hold=new RetirementHold(retirement,leader.term(),generation,RatisParticipantRecord.encode(retirement).length,resources); }
            catch (RuntimeException | Error failure) { if (resources!=null) resources.close(); throw failure; }
            preparingRetirements.put(operation,hold); retirementSlots++; retirementInputBytes+=hold.bytes();
        }
        boolean retained=false;
        try {
            var homes=transactionHomes; if (homes==null) throw new IOException("Qualified transaction HOME reader is not configured");
            var home=homes.readRegistration(retirement.identity().transactionId());
            if (!home.descriptor().groupId().equals(retirement.finalization().homeGroup()) || home.descriptor().rangeEpoch()!=retirement.finalization().homeEpoch()
                    || !retirement.consumed().equals(home.status())) throw new IOException("Retirement differs from fresh consumed HOME authority");
            boolean needed=store.estimateParticipantRetirement(retirement);
            synchronized (admissions) {
                requireHealthy(); if (generation!=admissionGeneration.get() || !store.admissionOpen() || !leadership().ready() || leadership().term()!=hold.term()) throw new IOException("Retirement lost its owner");
                if (!needed) return true;
                preparingRetirements.remove(operation,hold); preparedRetirements.put(operation,hold); retained=true; return false;
            }
        } finally {
            synchronized (admissions) { if (!retained && preparingRetirements.remove(operation,hold)) { retirementSlots--; retirementInputBytes-=hold.bytes(); if (hold.resources()!=null) hold.resources().close(); } }
        }
    }
    private void cancelRetirement(ParticipantRetirement retirement) throws IOException {
        var operation=RatisParticipantRecord.operation(retirement);
        synchronized (admissions) {
            if (preparingRetirements.containsKey(operation)) throw new IOException("Retirement HOME read is still active");
            var hold=preparedRetirements.get(operation);
            if (hold!=null) { if (!hold.retirement().equals(retirement)) throw new IOException("Retirement identity changed"); preparedRetirements.remove(operation,hold); retirementSlots--; retirementInputBytes-=hold.bytes(); if (hold.resources()!=null) hold.resources().close(); }
            var admitted=admissions.get(operation); if (admitted!=null && !retirement.equals(admitted.retirement)) throw new IOException("Retirement identity changed");
        }
    }
    private TransactionContext startRetirement(TransactionContext transaction,ParticipantRetirement retirement,byte[] encoded,long generation,Leadership leader) throws Exception {
        var operation=RatisParticipantRecord.operation(retirement); boolean needed=store.estimateParticipantRetirement(retirement);
        synchronized (admissions) {
            requireHealthy(); if (generation!=admissionGeneration.get() || !store.admissionOpen()) throw new IOException("Retirement owner changed");
            var cut=store.applied(); if (cut==null || cut.term()!=leader.term()) throw new IOException("Retirement native prefix is not ready");
            reconcileOlderProposalTerms(leader.term()); var known=admissions.get(operation);
            if (known!=null) {
                if (!retirement.equals(known.retirement) || known.term!=leader.term() || known.generation!=generation) throw new IOException("Retirement is still settling");
                known.owners++; transaction.setStateMachineContext(known); return transaction;
            }
            var accepted=preparedRetirements.get(operation);
            if (needed && (accepted==null || accepted.term()!=leader.term() || accepted.generation()!=generation || !accepted.retirement().equals(retirement))) throw new IOException("Retirement has no fresh HOME authority");
            if (accepted==null && (sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16 || encoded.length>1024L*1024-retirementInputBytes)) throw new IOException("Protected retirement input budget exhausted");
            var admission=new Admission(operation,RatisParticipantRecord.digest(retirement),encoded.length,leader.term(),generation,0,0,false,true,null,null,null);
            admission.retirement=retirement;
            if (accepted!=null) { if (accepted.resources()!=null) admission.resourceLeases.add(accepted.resources()); preparedRetirements.remove(operation,accepted); retirementSlots--; } else retirementInputBytes+=encoded.length;
            admissions.put(operation,admission); protectedAdmissions++; admittedBytes+=encoded.length; transaction.setStateMachineContext(admission); return transaction;
        }
    }
    public int protectedFinalizationSlots() { synchronized (admissions) { return protectedFinalizationSlots; } }
    public long reservedFinalizationCount() { synchronized (admissions) { return reservedFinalizationCount; } }
    public final class ParticipantProposal implements AutoCloseable {
        private final ProposalReservation reservation; private final ParticipantHold hold;
        private final PreparedParticipant.Preparation preparation; private final byte[] bytes,digest; private final ProposalDeadline deadline;
        private final boolean negative;
        private boolean claimed,closed;
        private ParticipantProposal(ProposalReservation reservation,ParticipantHold hold,PreparedParticipant.Preparation preparation,ProposalDeadline deadline,boolean negative) {
            this.reservation=reservation; this.hold=hold; this.preparation=preparation; this.deadline=deadline; this.negative=negative;
            bytes=RatisParticipantRecord.encode(preparation,negative); digest=RatisParticipantRecord.digest(preparation,negative);
        }
        private boolean authorized() {
            var a=store.applied();
            return !closed && !claimed && hold!=null && !hold.invalidated && !hold.released && (negative ? hold.generation==admissionGeneration.get() && a!=null && a.term()==hold.term && store.admissionOpen() : reservation!=null && reservation.active()) && (deadline==null || deadline.active());
        }
        public boolean negative() { return negative; }
        public PreparedParticipant.Preparation preparation() { return preparation; }
        public byte[] bytes() { return bytes.clone(); }
        @Override public void close() {
            synchronized (admissions) {
                if (closed || claimed) return; closed=true;
                if (preparedParticipants.remove(preparation.operation(),this) && !negative) preparedCommitBytes-=bytes.length;
                releaseParticipantHold(hold); if (reservation!=null) proposalFootprints.release(reservation.lease);
            }
        }
    }
    private void releaseNegativePayload(NegativePayload payload) {
        if (--payload.references==0) negativeInputBytes-=payload.bytes;
    }
    public long negativeParticipantInputBytes() { synchronized (admissions) { return negativeInputBytes; } }
    public int negativeParticipantSlots() { synchronized (admissions) { return protectedNegativeSlots; } }
    private void releaseParticipantHold(ParticipantHold hold) { releaseParticipantHold(hold,false); }
    private void releaseParticipantHold(ParticipantHold hold,boolean covered) {
        if (hold.released) return; hold.released=true;
        if (hold.negativeSlot) { hold.negativeSlot=false; protectedNegativeSlots--; }
        if (hold.payload!=null && !hold.transferred) releaseNegativePayload(hold.payload);
        finishParticipantHold(hold,covered);
    }
    private void finishParticipantHold(ParticipantHold hold,boolean covered) {
        if (!hold.capacityReleased && (covered || hold.released && hold.finalizers==0)) {
            hold.capacityReleased=true; reservedParticipantCount--; reservedParticipantBytes-=hold.bytes; reservedParticipantScopeBytes-=hold.scopeBytes;
        }
        if (hold.released && hold.finalizers==0) { if (hold.resourceLease!=null) { hold.resourceLease.close(); hold.resourceLease=null; } participantOwners.remove(hold.control,hold); participantOwners.remove(hold.business,hold); }
    }
    private void releaseFinalizationHold(FinalizationHold hold,boolean covered) {
        if (hold.released) return; hold.released=true;
        if (hold.resourceLease!=null) { hold.resourceLease.close(); hold.resourceLease=null; }
        finalizationOwners.remove(hold.recovery,hold); if (hold.business!=null) finalizationOwners.remove(hold.business,hold); finalizingTransactions.remove(hold.transaction,hold);
        if (hold.capacity) { reservedFinalizationCount--; reservedFinalizationBytes-=ReplicatedRangeStore.FINALIZATION_RESERVATION; }
        if (hold.shared!=null) { var shared=hold.shared; hold.shared=null; shared.finalizers--; if (shared.payload!=null) releaseNegativePayload(shared.payload); finishParticipantHold(shared,covered); }
    }
    private boolean retirementIdentityBusy(OperationId id) { return preparedSnapshotHomeRetirements.containsKey(id) || preparingSnapshotHomeRetirements.containsKey(id) || preparedSnapshotRetirements.containsKey(id) || preparingSnapshotRetirements.containsKey(id) || preparedRetirements.containsKey(id) || preparingRetirements.containsKey(id) || preparedHomeRetirements.containsKey(id) || preparingHomeRetirements.containsKey(id); }
    private boolean sourceIdentityBusy(OperationId id){return sourceHolds.values().stream().anyMatch(hold->hold.key.operation().equals(id));}
    private boolean participantIdentityBusy(OperationId id) {return sourceIdentityBusy(id) || nonSourceIdentityBusy(id);}
    private boolean nonSourceIdentityBusy(OperationId id) {
        return retirementIdentityBusy(id) || homeOwners.containsKey(id) || snapshotControlCharges.containsKey(id) || finalizationOwners.containsKey(id) || participantOwners.containsKey(id) || admissions.containsKey(id) || preparedCommits.containsKey(id) || preparingCommits.containsKey(id);
    }
    /** Freeze the original read cut/scope/result before registering the home; it is never rebased during PREPARE. */
    public byte[] participantPlanDigest(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand business) throws Exception { return participantIntent(pin,business).payloadDigest(); }
    /** Canonical planning data for durable registration; this grants no PREPARE append authority. */
    public DistributedCommand participantIntent(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand business) throws Exception { requireDataRole();
        java.util.Objects.requireNonNull(pin);
        if (!pin.belongsTo(store)) throw new IllegalArgumentException("Tracked snapshot belongs to another range");
        checkTransactionOwner(); long generation=transactionGeneration(); commandValidator.accept(business);
        try (var stage=pin.stage(business)) {
            checkTransactionOwner(); if (generation!=transactionGeneration() || pin.position().term()!=transactionTerm()) throw new IllegalStateException("Participant read owner changed");
            var accepted=DistributedCommand.acceptedTransaction(business,new it.cavallium.rockserver.core.common.distributed.TransactionAcceptance(pin.position(),generation,transactionIncarnation,pin.scope(),stage.result().encode()));
            commandValidator.accept(accepted); return accepted;
        }
    }
    /** Freeze a read dependency at its original complete scope without inventing DATA operands. */
    public DistributedCommand participantReadIntent(ReplicatedRangeStore.ScopedHistoryPin pin,OperationId operation) throws Exception { requireDataRole();
        java.util.Objects.requireNonNull(pin); java.util.Objects.requireNonNull(operation);
        checkTransactionOwner(); long generation=transactionGeneration();
        if (!pin.belongsTo(store)) throw new IllegalArgumentException("Tracked snapshot belongs to another range");
        if (pin.status()!=RangeMutationHistory.Status.ACTIVE || pin.position().term()!=transactionTerm()) throw new IllegalStateException("Participant read owner changed");
        var accepted=DistributedCommand.readCertification(store.ownership().owner().groupId(),store.ownership().owner().epoch(),store.schemaDigest(),operation,
                new TransactionAcceptance(pin.position(),generation,transactionIncarnation,pin.scope(),new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
        commandValidator.accept(accepted); store.validate(accepted);
        checkTransactionOwner(); if (generation!=transactionGeneration() || pin.status()!=RangeMutationHistory.Status.ACTIVE || pin.position().term()!=transactionTerm()) throw new IllegalStateException("Participant read owner changed");
        return accepted;
    }
    /** Reattach exact live PREPARE authority after a lost coordinator reply; never recreate a read cut. */
    public ParticipantProposal reattachParticipant(UUID transaction,OperationId control,DistributedCommand accepted) throws IOException { requireDataRole();
        java.util.Objects.requireNonNull(transaction); java.util.Objects.requireNonNull(control); java.util.Objects.requireNonNull(accepted);
        if (accepted.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED) throw new IllegalArgumentException("Expected a frozen accepted participant image"); commandValidator.accept(accepted);
        requireHealthy(); long generation=admissionGeneration.get(); var leader=leadership();
        if (!leader.ready()) throw new RatisPreparationUnavailableException("Participant leader is not ready for original acceptance");
        synchronized (admissions) {
            requireHealthy(); var a=store.applied();
            if (generation!=admissionGeneration.get() || a==null || a.term()!=leader.term() || !store.admissionOpen()) throw new RatisPreparationUnavailableException("Original participant owner is unavailable");
            reconcileOlderProposalTerms(leader.term()); var ready=preparedParticipants.get(control);
            if (ready!=null && ready.authorized()) {
                requireParticipantImage(transaction,accepted,ready.preparation);
                var view=new ParticipantProposal(ready.reservation,ready.hold,ready.preparation,ready.deadline,ready.negative); view.claimed=true; return view;
            }
            var known=admissions.get(control);
            if (known!=null && known.participant!=null && known.term==leader.term() && known.generation==generation) {
                requireParticipantImage(transaction,accepted,known.participant);
                var view=new ParticipantProposal(known.acceptedScope==null ? null : new ProposalReservation(known.acceptedScope),known.participantHold,known.participant,null,known.participantNegative); view.claimed=true; return view;
            }
            throw new RatisPreparationUnavailableException("No live acceptance for the original participant read cut");
        }
    }
    private static void requireParticipantImage(UUID transaction,DistributedCommand accepted,PreparedParticipant.Preparation preparation) {
        if (!transaction.equals(preparation.transactionId()) || !Arrays.equals(DistributedCommandCodec.encode(accepted),DistributedCommandCodec.encode(preparation.business()))) throw new IllegalArgumentException("Participant image differs from frozen acceptance");
    }
    /** Claims scope/identities at one cut. Gate rejection has protected input and no candidate lease. */
    private PreparingParticipant reserveParticipant(TransactionScope scope,OperationId business,OperationId control,UUID transaction,boolean rejectionOnly,ProposalDeadline deadline) throws Exception {
        requireHealthy(); long generation=admissionGeneration.get(); var leader=leadership();
        if (!leader.ready()) throw new IllegalStateException("Participant leader is not ready");
        var footprint=proposalFootprints.prepare(scope); long scopeBytes=RangeMutationHistory.accountedScopeBytes(scope);
        for (int attempt=0;attempt<3;attempt++) {
            var observed=store.rejectionCapacity(transaction); var capacity=observed.capacity(); var gateCut=store.gateConflict(scope);
            synchronized (admissions) {
                requireHealthy(); var a=store.applied();
                if (generation!=admissionGeneration.get() || a==null || a.term()!=leader.term() || !store.admissionOpen() || deadline!=null && !deadline.active()) throw new IllegalStateException("Participant owner changed");
                if (!java.util.Objects.equals(a,capacity.position()) || !java.util.Objects.equals(a,gateCut.position())) continue;
                reconcileOlderProposalTerms(leader.term());
                if (participantIdentityBusy(control) || participantIdentityBusy(business)) throw new IllegalStateException("Participant operation identity is already owned");
                boolean negative=gateCut.blocked() || proposalFootprints.gated(footprint);
                if (rejectionOnly && !negative) throw new RatisPreparationUnavailableException("No gate authorizes this negative preparation");
                boolean reserve=!negative || observed.newParticipant();
                long bytes=negative ? ReplicatedRangeStore.FINALIZATION_RESERVATION : 49L+PreparedParticipant.Preparation.MAX_BYTES+ReplicatedRangeStore.FINALIZATION_RESERVATION;
                long ownedScope=negative ? 0 : scopeBytes;
                if (reserve && (capacity.count()+reservedParticipantCount+reservedFinalizationCount>=ReplicatedRangeStore.MAX_PREPARED_PARTICIPANTS || bytes>ReplicatedRangeStore.MAX_PREPARED_BYTES-capacity.bytes()-reservedParticipantBytes-reservedFinalizationBytes)
                        || ownedScope>ReplicatedRangeStore.MAX_PREPARED_BYTES-capacity.scopeBytes()-reservedParticipantScopeBytes) throw new IllegalStateException("Participant staged capacity exhausted");
                if (negative ? sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots>=16 || DistributedCommandCodec.MAX_ENCODED_BYTES>8L*1024*1024-negativeInputBytes
                        : DistributedCommandCodec.MAX_ENCODED_BYTES>16L*1024*1024-preparedCommitBytes) throw new IllegalStateException("Participant input budget exhausted");
                var resources=reserveNodeResources(negative ? RatisNodeResourceBudget.Kind.PROTECTED : RatisNodeResourceBudget.Kind.ORDINARY,
                        256L+ownedScope+4L*PreparedParticipant.Preparation.MAX_BYTES,1);
                ProposalReservation reservation=null;
                if (!negative) { var lease=proposalFootprints.reserveCandidate(footprint,leader.term(),generation); if (lease==null) { if (resources!=null) resources.close(); throw new IllegalStateException("Participant scope overlaps in-flight work"); } reservation=new ProposalReservation(lease); }
                var payload=negative ? new NegativePayload(DistributedCommandCodec.MAX_ENCODED_BYTES) : null;
                var hold=new ParticipantHold(transaction,control,business,reserve ? bytes : 0,ownedScope,leader.term(),generation,payload,reserve);
                hold.resourceLease=resources;
                var preparing=new PreparingParticipant(reservation,hold); preparingParticipants.put(control,preparing); participantOwners.put(control,hold); participantOwners.put(business,hold);
                if (reserve) { reservedParticipantCount++; reservedParticipantBytes+=bytes; reservedParticipantScopeBytes+=ownedScope; }
                if (negative) { protectedNegativeSlots++; negativeInputBytes+=payload.bytes; } else preparedCommitBytes+=DistributedCommandCodec.MAX_ENCODED_BYTES;
                return preparing;
            }
        }
        throw new IllegalStateException("Native cut kept changing during participant admission");
    }
    ParticipantProposal prepareHandleParticipant(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand accepted,OperationId control,UUID transaction,ProposalDeadline deadline) throws Exception {
        var homes=transactionHomes; if (homes==null) throw new IOException("Qualified transaction HOME reader is not configured");
        if (accepted.readCertification()) return prepareReadParticipant(pin,accepted,control,homes,transaction,deadline);
        var business=new DistributedCommand(accepted.groupId(),accepted.rangeEpoch(),accepted.schemaDigest(),accepted.operationId(),accepted.mutations(),accepted.predicates(),accepted.acknowledgedThrough());
        var proposal=prepareParticipant(pin,business,control,homes,transaction,deadline);
        if (!Arrays.equals(proposal.preparation().business().payloadDigest(),accepted.payloadDigest())) { proposal.close(); throw new IllegalArgumentException("Participant preparation changed original staged cut/result"); }
        return proposal;
    }
    public ParticipantProposal prepareParticipant(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand business,OperationId control,RatisTransactionStatuses homes,UUID transaction) throws Exception {
        return prepareParticipant(pin,business,control,homes,transaction,null);
    }
    ParticipantProposal prepareParticipant(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand business,OperationId control,RatisTransactionStatuses homes,UUID transaction,ProposalDeadline deadline) throws Exception { requireDataRole();
        if (business.readCertification()) return prepareReadParticipant(pin,business,control,homes,transaction,deadline);
        java.util.Objects.requireNonNull(pin); java.util.Objects.requireNonNull(control); commandValidator.accept(business);
        if (control.equals(business.operationId())) throw new IllegalArgumentException("Prepare control identity must differ from business identity");
        var preparing=reserveParticipant(pin.scope(),business.operationId(),control,transaction,false,deadline);
        return finishParticipantPreparation(preparing,homes,transaction,pin,deadline,() -> {
            if (pin.position().term()!=preparing.hold().term) throw new IllegalStateException("Original participant pin changed term");
            if (preparing.hold().payload!=null) return participantIntent(pin,business);
            if (!store.estimateReceiptRetention(business,enableSessionControls).newReceipt()) throw new IllegalStateException("Business operation already has an outcome");
            return store.planScopedCommit(pin,business,preparing.hold().generation,transactionIncarnation);
        });
    }
    private ParticipantProposal prepareReadParticipant(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand accepted,OperationId control,RatisTransactionStatuses homes,UUID transaction,ProposalDeadline deadline) throws Exception {
        java.util.Objects.requireNonNull(pin); java.util.Objects.requireNonNull(control); checkTransactionOwner();
        if (!pin.belongsTo(store)) throw new IllegalArgumentException("Tracked snapshot belongs to another range");
        if (pin.status()!=RangeMutationHistory.Status.ACTIVE || pin.position().term()!=transactionTerm()) throw new IllegalStateException("Original participant read owner changed");
        var audit=accepted.acceptance();
        if (!accepted.readCertification() || control.equals(accepted.operationId()) || !audit.snapshot().equals(pin.position())
                || audit.leaderGeneration()!=transactionGeneration() || !audit.ownerIncarnation().equals(transactionIncarnation)
                || !Arrays.equals(ScopedTransactionRpc.scopeBytes(audit.scope()),ScopedTransactionRpc.scopeBytes(pin.scope()))) throw new IllegalArgumentException("Read certification differs from original pin/owner");
        commandValidator.accept(accepted); store.validate(accepted);
        var preparing=reserveParticipant(pin.scope(),accepted.operationId(),control,transaction,false,deadline);
        return finishParticipantPreparation(preparing,homes,transaction,pin,deadline,() -> {
            if (pin.position().term()!=preparing.hold().term) throw new IllegalStateException("Original participant pin changed term");
            if (preparing.hold().payload!=null) return accepted;
            var certified=store.planReadCertification(pin,accepted.operationId(),preparing.hold().generation,transactionIncarnation);
            if (!Arrays.equals(certified.payloadDigest(),accepted.payloadDigest())) throw new IllegalArgumentException("Read certification changed original cut/scope/result");
            return certified;
        });
    }
    /** Reject a frozen original plan under current gate authority without reopening or rebasing its read cut. */
    public ParticipantProposal prepareParticipantRejection(DistributedCommand accepted,OperationId control,RatisTransactionStatuses homes,UUID transaction) throws Exception { requireDataRole();
        java.util.Objects.requireNonNull(accepted); java.util.Objects.requireNonNull(control);
        if (accepted.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED || control.equals(accepted.operationId())) throw new IllegalArgumentException("Expected a frozen accepted plan and separate control");
        commandValidator.accept(accepted); store.validate(accepted);
        var preparing=reserveParticipant(accepted.acceptance().scope(),accepted.operationId(),control,transaction,true,null);
        return finishParticipantPreparation(preparing,homes,transaction,null,null,() -> accepted);
    }
    private ParticipantProposal finishParticipantPreparation(PreparingParticipant preparing,RatisTransactionStatuses homes,UUID transaction,ReplicatedRangeStore.ScopedHistoryPin pin,ProposalDeadline deadline,java.util.concurrent.Callable<DistributedCommand> plan) throws Exception {
        var hold=preparing.hold(); boolean negative=hold.payload!=null,retained=false;
        try {
            var homeRead=homes.readRegistration(transaction); var registration=homeRead.status();
            if (registration==null || registration.phase()!=TransactionStatus.Phase.PREPARING) throw new IllegalStateException("Home has no active frozen registration");
            var accepted=plan.call(); commandValidator.accept(accepted); var home=homeRead.descriptor();
            var preparation=new PreparedParticipant.Preparation(home.groupId(),home.rangeEpoch(),hold.control,registration,accepted);
            if (negative) { var estimate=store.estimateRejection(preparation); if (estimate.existing()!=null && estimate.existing().prepared()) throw new IllegalStateException("Positive preparation cannot become a negative vote"); }
            else store.estimatePreparation(preparation);
            var retired=store.participantFence(new ParticipantIdentity(preparation.homeGroup(),preparation.homeEpoch(),preparation.business().groupId(),preparation.registration()));
            if (retired!=null) throw new RatisParticipantRetiredException(retired);
            var proposal=new ParticipantProposal(preparing.reservation(),hold,preparation,deadline,negative);
            synchronized (admissions) { hold.preparation=preparation; }
            if (pin!=null) pin.close();
            synchronized (admissions) {
                requireHealthy(); if (!proposal.authorized() || preparingParticipants.get(hold.control)!=preparing) throw new IllegalStateException("Participant lost acceptance ownership");
                if (negative) { negativeInputBytes+=proposal.bytes.length-hold.payload.bytes; hold.payload.bytes=proposal.bytes.length; }
                else {
                    long actual=49L+preparation.encode().length+ReplicatedRangeStore.FINALIZATION_RESERVATION;
                    if (!hold.capacityReleased) reservedParticipantBytes+=actual-hold.bytes; hold.bytes=actual;
                    preparedCommitBytes+=proposal.bytes.length-DistributedCommandCodec.MAX_ENCODED_BYTES;
                }
                preparedParticipants.put(hold.control,proposal); preparingParticipants.remove(hold.control,preparing); retained=true; return proposal;
            }
        } finally {
            synchronized (admissions) {
                if (!retained && preparingParticipants.remove(hold.control,preparing)) {
                    if (!negative) preparedCommitBytes-=DistributedCommandCodec.MAX_ENCODED_BYTES; releaseParticipantHold(hold);
                }
            }
            if (!retained && preparing.reservation()!=null) preparing.reservation().close();
        }
    }
    private TransactionContext startParticipant(TransactionContext transaction,byte[] encoded,long generation,Leadership leadership) throws Exception {
        var preparation=RatisParticipantRecord.decode(encoded); boolean negative=RatisParticipantRecord.isNegative(encoded); commandValidator.accept(preparation.business()); byte[] digest=RatisParticipantRecord.digest(preparation,negative);
        for (int attempt=0;attempt<3;attempt++) {
            var estimate=negative ? store.estimateRejection(preparation) : null;
            var positive=negative ? null : store.estimatePreparation(preparation);
            var observed=negative ? estimate.capacity().position() : positive.capacity().position();
            var existing=negative ? estimate.existing() : positive.existing();
            if (negative && existing!=null && existing.prepared()) throw new IllegalStateException("Positive preparation cannot become a negative vote");
            if (observed==null || observed.term()!=leadership.term()) throw new IllegalStateException("Participant native prefix is not ready");
            synchronized (admissions) {
                requireHealthy(); if (generation!=admissionGeneration.get() || !store.admissionOpen()) throw new IllegalStateException("Participant owner changed");
                if (!java.util.Objects.equals(observed,store.applied())) continue;
                reconcileOlderProposalTerms(leadership.term());
                var known=admissions.get(preparation.operation());
                if (known!=null && !Arrays.equals(known.digest,digest)) throw new IllegalArgumentException("Prepare operation payload changed");
                if (known!=null && existing!=null) { settleAdmission(known); known=admissions.get(preparation.operation()); }
                if (known!=null) {
                    if (known.participant==null || known.term!=leadership.term() || known.generation!=generation) throw new IllegalStateException("Previous prepare admission is still settling");
                    known.owners++; transaction.setStateMachineContext(known); return transaction;
                }
                var accepted=existing==null ? preparedParticipants.get(preparation.operation()) : null;
                if (existing==null && (accepted==null || !Arrays.equals(accepted.digest,digest) || !accepted.authorized())) throw new IllegalStateException("Prepare has no current acceptance authority; resolve through its home");
                if (negative ? sourceHolds.size()+schemaSlots+topologySlots+protectedAdmissions+protectedFinalizationSlots+snapshotSlots+protectedNegativeSlots+homeRecoverySlots+retirementSlots+homeRetirementSlots+snapshotRetirementSlots+snapshotHomeRetirementSlots-(accepted==null ? 0 : 1)>=16 || accepted==null && encoded.length>8L*1024*1024-negativeInputBytes
                        : dataAdmissions>=1024 || encoded.length>16L*1024*1024-dataProposalBytes) throw new IllegalStateException("Participant input budget exhausted");
                var admission=new Admission(preparation.operation(),digest,encoded.length,leadership.term(),generation,0,0,false,negative,null,accepted==null || accepted.reservation==null ? null : accepted.reservation.lease,null);
                admission.participant=preparation; admission.participantNegative=negative; admission.participantHold=accepted==null ? null : accepted.hold;
                if (negative) {
                    admission.negativePayload=accepted==null ? new NegativePayload(encoded.length) : accepted.hold.payload;
                    if (accepted==null) negativeInputBytes+=encoded.length;
                    else { accepted.hold.transferred=true; accepted.hold.negativeSlot=false; protectedNegativeSlots--; }
                }
                admissions.put(admission.id,admission);
                if (accepted!=null) {
                    if (accepted.reservation!=null) proposalFootprints.submit(accepted.reservation.lease); accepted.claimed=true; preparedParticipants.remove(admission.id,accepted); if (!negative) preparedCommitBytes-=accepted.bytes.length;
                }
                admittedBytes+=admission.bytes; if (negative) protectedAdmissions++; else { dataAdmissions++; dataProposalBytes+=admission.bytes; }
                transaction.setStateMachineContext(admission); return transaction;
            }
        }
        throw new IllegalStateException("Native participant cut changed during submission");
    }
    public long reservedParticipantCount() { synchronized (admissions) { return reservedParticipantCount; } }
    public long reservedParticipantBytes() { synchronized (admissions) { return reservedParticipantBytes; } }
    /** Caller supplies a leader-bound tracked cut obtained after its read barrier; no network work occurs here. */
    public ScopedCommit prepareScopedCommit(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand command) throws Exception { requireDataRole();
        return prepareScopedCommit(pin,command,null);
    }
    ScopedCommit prepareScopedCommit(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand command,ProposalDeadline deadline) throws Exception {
        java.util.Objects.requireNonNull(pin); java.util.Objects.requireNonNull(command); commandValidator.accept(command);
        ProposalReservation reservation;
        synchronized (admissions) {
            if (command.acknowledgedThrough()>=0 && proposalFootprints.stats().candidates()>0) throw new IllegalStateException("Acknowledging transaction needs exclusive session metadata ownership");
        }
        reservation=reserveProposalScope(pin.scope());
        if (reservation==null) throw new IllegalStateException("Transaction scope overlaps in-flight proposals");
        // Reserve the largest legal frozen command before native result construction, then shrink to its actual size.
        var preparing=new PreparingCommit(reservation,DistributedCommandCodec.MAX_ENCODED_BYTES);
        boolean retained=false; RatisNodeResourceBudget.Lease resources=null;
        try {
            resources=reserveNodeResources(RatisNodeResourceBudget.Kind.ORDINARY,256L+4L*DistributedCommandCodec.MAX_ENCODED_BYTES,1);
            synchronized (admissions) {
                if (!reservation.active() || deadline!=null && !deadline.active() || pin.position().term()!=reservation.term()) throw new IllegalStateException("Transaction snapshot changed leadership or expired");
                if (snapshotControlCharges.containsKey(command.operationId()) || finalizationOwners.containsKey(command.operationId()) || participantOwners.containsKey(command.operationId()) || admissions.containsKey(command.operationId()) || preparedCommits.containsKey(command.operationId()) || preparingCommits.containsKey(command.operationId())) throw new IllegalStateException("Operation identity already owns a proposal");
                if (preparingCommits.size()+preparedCommits.size()>=64 || preparing.bytes>16L*1024*1024-preparedCommitBytes) throw new IllegalStateException("Preparing command byte/count budget exhausted");
                if (command.acknowledgedThrough()>=0) {
                    if (proposalFootprints.stats().candidates()!=1) throw new IllegalStateException("Acknowledging transaction lost exclusive session metadata ownership");
                    exclusiveMetadataCandidate=reservation.lease;
                }
                preparingCommits.put(command.operationId(),preparing); preparedCommitBytes+=preparing.bytes;
            }
            if (!store.estimateReceiptRetention(command,enableSessionControls).newReceipt()) throw new IllegalStateException("Transaction identity already has a terminal outcome");
            var accepted=store.planScopedCommit(pin,command,reservation.generation(),transactionIncarnation);
            commandValidator.accept(accepted);
            var plan=new ScopedCommit(reservation,accepted,deadline);
            plan.resourceLease=resources;
            synchronized (admissions) {
                requireHealthy();
                if (!plan.authorized() || preparingCommits.get(command.operationId())!=preparing) throw new IllegalStateException("Transaction lost its reservation before acceptance");
                if (preparedCommits.size()>=64 || plan.bytes>16L*1024*1024-(preparedCommitBytes-preparing.bytes)) throw new IllegalStateException("Prepared command byte/count budget exhausted");
                preparedCommitBytes+=plan.bytes-preparing.bytes; preparing.bytes=plan.bytes;
            }
            // Closing native ownership can outlive logical invalidation; keep the preparing token charged through it.
            pin.close();
            synchronized (admissions) {
                requireHealthy();
                if (!plan.authorized() || preparingCommits.get(command.operationId())!=preparing) throw new IllegalStateException("Transaction lost its reservation before publication");
                if (admissions.containsKey(command.operationId()) || preparedCommits.containsKey(command.operationId())) throw new IllegalStateException("Operation identity already owns a proposal");
                preparedCommits.put(command.operationId(),plan); preparingCommits.remove(command.operationId(),preparing);
            }
            retained=true; return plan;
        } finally {
            synchronized (admissions) { if (preparingCommits.remove(command.operationId(),preparing)) preparedCommitBytes-=preparing.bytes; }
            if (!retained) { reservation.close(); if (resources!=null) resources.close(); }
        }
    }
    public ProposalReservation reserveProposalScope(TransactionScope scope) throws IOException { requireDataRole();
        requireHealthy(); long generation=admissionGeneration.get(); var current=leadership();
        if (!current.ready()) throw new IOException("Leader is not ready for scope reservation");
        store.validateScope(scope); var prepared=proposalFootprints.prepare(scope);
        synchronized (admissions) {
            requireHealthy(); var applied=store.applied();
            if (generation!=admissionGeneration.get() || applied==null || applied.term()!=current.term()) throw new IOException("Reservation lost its completed current-term owner");
            if (!store.admissionOpen()) throw new IOException("Native range admission is closed"); reconcileOlderProposalTerms(current.term());
            if (exclusiveMetadataCandidate!=null && exclusiveMetadataCandidate.active() || admissions.values().stream().anyMatch(admission -> admission.maintenance && !admission.settled)) throw new IOException("Session metadata is still settling");
            var lease=proposalFootprints.reserveCandidate(prepared,current.term(),generation); return lease==null ? null : new ProposalReservation(lease);
        }
    }

    private static boolean retentionFits(long retained,long reserved,long additional,long ceiling) {
        // No-growth GC/retries remain possible when conservative in-flight accounting exceeds the ceiling.
        return additional == 0 || retained <= ceiling && reserved <= ceiling-retained && additional <= ceiling-retained-reserved;
    }
    // Caller holds admissions. Applied outcomes no longer consume speculative capacity, even with duplicate RPC waiters.
    private void releaseRetention(Admission admission) {
        if (!admission.retentionReleased) {
            admission.retentionReleased=true;
            releaseSessionNamespace(admission);
            reservedHomeCount-=admission.homeCreditCount; reservedHomeBytes-=admission.homeCreditBytes;
            reservedReceiptCount-=admission.receiptCount; reservedReceiptBytes-=admission.receiptBytes;
            if (maintenanceReservation==admission) maintenanceReservation=null;
        }
    }

    @Override public void notifyConfigurationChanged(long term, long index, RaftConfigurationProto configuration) {
        synchronized (gate) {
            // Snapshot transport's current membership is not an applied configuration at D.
            if (term == 0) return;
            byte[] bytes = configuration.toByteArray();
            if (bytes.length > 65536 || configuration.getPeersCount() == 0 || deferred.containsKey(index)) {
                storageFailure = new IllegalStateException("Invalid configuration callback");
                throw new IllegalStateException("Invalid configuration", storageFailure);
            }
            if (!configurations.containsKey(index) && configurations.size() >= 1024) throw new IllegalStateException("Configuration callback budget exhausted");
            synchronized (admissions) {
                var previous = configurations.putIfAbsent(index, new ConfigurationEntry(term, bytes));
                if (previous != null && (previous.term() != term || !Arrays.equals(previous.bytes(), bytes))) {
                    storageFailure = new IllegalStateException("Configuration log identity changed");
                    throw new IllegalStateException("Conflicting configuration", storageFailure);
                }
                if (previous==null) {
                    schemaConfigurationStamp++; schemaConfigurationPending=true;
                    try{updateDataVoter(bytes);}catch(IOException invalid){throw new IllegalStateException("Invalid local voting configuration",invalid);}
                    if(relocationHold!=null){relocationHold.invalidated=true;if(!relocationHold.submitted)releaseRelocation(relocationHold);}
                    for(var hold:List.copyOf(sourceHolds.values())){hold.invalidated=true;if(!hold.submitted)releaseSourceHold(hold);}
        if(topologyActivationHold!=null){topologyActivationHold.invalidated=true;if(!topologyActivationHold.submitted)releaseTopologyActivation(topologyActivationHold);}
        if(topologyHold!=null){topologyHold.invalidated=true;if(!topologyHold.submitted)releaseTopologyHold(topologyHold);}
                    if(importApplyHold!=null){importApplyHold.invalidated=true;if(!importApplyHold.submitted)releaseImportApply(importApplyHold);}
                    for(var hold:List.copyOf(importPublicationHolds.values())){hold.invalidated=true;if(!hold.submitted)releaseImportPublication(hold);}
                    if (schemaHold!=null) { schemaHold.invalidated=true; if (!schemaHold.submitted) releaseSchemaHold(schemaHold); }
                }
            }
        }
    }

    @Override public void notifyTermIndexUpdated(long term, long index) {
        Throwable failure = null;
        synchronized (gate) {
            try {
                if (storageFailure != null) throw new IllegalStateException("Range requires repair", storageFailure);
                if (deferred.containsKey(index)) throw new IllegalStateException("Log entry changed from data to metadata");
                var pending = pendingNoops.get(index);
                if (pending != null && pending != term) throw new IllegalStateException("Metadata log identity changed");
                if (getLifeCycleState() != LifeCycle.State.RUNNING || nativeApply!=null) {
                    if (pending == null && pendingNoops.size() >= 1024) throw new IllegalStateException("Paused metadata budget exhausted");
                    pendingNoops.put(index, term);
                    return;
                }
                var cfg = configurations.remove(index);
                if (cfg != null && cfg.term() != term) throw new IllegalStateException("Configuration term mismatch");
                if (cfg!=null) { admissionGeneration.incrementAndGet(); invalidateProposalCandidates(); }
                store.applyNoop(new ApplyPosition(term, index), cfg == null ? null : cfg.bytes());
                schemaConfigurationPending=!configurations.isEmpty();
                advance(term, index);
                pendingNoops.remove(index);
            } catch (Throwable problem) { storageFailure = problem; failure = problem; }
        }
        if (failure != null) {
            failDeferred(failure);
            throw new IllegalStateException("Range failed completed apply", failure);
        }
        try{refreshRecoveryAfterNativeCompletion();}catch(IOException problem){synchronized(gate){storageFailure=problem;}failDeferred(problem);throw new IllegalStateException("Recovery evidence did not follow completed native metadata",problem);}
    }

    @Override public CompletableFuture<Message> queryStale(Message request, long minIndex) {
        try {
            int kind=ScopedTransactionRpc.kind(request.getContent().asReadOnlyByteBuffer(),RatisSnapshotCohortRpc.MAX_REQUEST_BYTES);
            if (ScopedTransactionRpc.ownerSnapshot(kind) || ScopedTransactionRpc.ownerExport(kind) || kind>=RatisSnapshotCohortRpc.CAPTURE && kind<=RatisSnapshotCohortRpc.CANCEL) {
                if (minIndex!=0) throw new IOException("Pinned reads do not accept current-index constraints");
                return query(request);
            }
            if(kind==RatisTopologySourceSnapshotRpc.RELEASE || kind==RatisRecoveryImageRpc.KIND || ScopedTransactionRpc.schemaReplica(kind) || kind==ScopedTransactionRpc.TOPOLOGY_OWNER || kind==ScopedTransactionRpc.TOPOLOGY_SEED || kind==RatisRangeImportStatusRpc.KIND){if(minIndex!=0)throw new IOException("Owner-local requests have no requested index override");return query(request);}
            if (kind>=ScopedTransactionRpc.OPEN) throw new IOException("Stale reads cannot access transaction handles");
            synchronized (gate) {
                requireHealthy();
                if (minIndex > getLastAppliedTermIndex().getIndex()) throw new IllegalStateException("Requested read index has not completed");
            }
            return query(request);
        } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
    }

    @Override public CompletableFuture<Message> query(Message request) {
        return query(request,false);
    }
    @Override public CompletableFuture<Message> queryLinearizable(Message request) { return query(request,true); }
    private CompletableFuture<Message> query(Message request,boolean freshBarrier) {
        try {
            int kind=ScopedTransactionRpc.kind(request.getContent().asReadOnlyByteBuffer(),Math.max(RatisSnapshotCohortRpc.MAX_REQUEST_BYTES,Math.max(RatisOperationRerouteAdmissionRpc.MAX_BYTES,RatisSourceOutcomeRpc.MAX_REQUEST_BYTES)));
            try (var construction=readConstructionLease(kind,request.getContent().asReadOnlyByteBuffer())) {
            byte[] encoded = request.getContent().toByteArray();
            if (!ScopedTransactionRpc.ownerSnapshot(kind) && !ScopedTransactionRpc.ownerExport(kind) && !(kind>=RatisSnapshotCohortRpc.OWNER && kind<=RatisSnapshotCohortRpc.CANCEL)) requireHealthy();
            if (ScopedTransactionRpc.schemaReplica(kind)) {
                requireDataRole(); var replicas=schemaReplicas;
                if (replicas==null) throw new IOException("Schema replica preparation is not configured");
                return replicas.submit(store.ownership().owner().groupId(),encoded).thenApply(bytes -> Message.valueOf(ByteString.copyFrom(bytes)));
            }
            if (ScopedTransactionRpc.ownerExport(kind)) {
                requireDataRole(); var dispatcher=exportSources;
                if (dispatcher==null) throw new IOException("Durable export sources are not configured");
                var packet=ScopedTransactionRpc.parse(encoded,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                return dispatcher.submit(store.ownership().owner().groupId(),kind,packet.body()).thenApply(payload ->
                        Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(kind,payload))));
            }
            if(RatisSourceOutcomeRpc.kind(kind)){
                if(!freshBarrier || sourceOutcomeQueries==null)throw new IOException("Source outcome preparation requires dedicated fresh lane");requireDataRole();var owner=checkCdcMetadataOwner();if(!owner.sealed())throw new IOException("Source preparation requires original sealed parent");
                if(kind==RatisSourceOutcomeRpc.CANCEL){var proposal=RatisSourceOutcomeRpc.proposal(encoded);cancelSourceOutcome(proposal);return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(RatisSourceOutcomeRpc.cancelled(proposal))));}
                return sourceOutcomeQueries.submit(store.ownership().owner().groupId(),encoded,2*RatisProofQueryService.IMPORT_PUBLICATION_BYTES,false,owned->{var before=checkCdcMetadataOwner();if(!sameCdcConfiguration(owner,before))throw new IOException("Source preparation lost read barrier");var prepared=prepareSourceOutcome(RatisSourceOutcomeRpc.proposal(owned));var after=checkCdcMetadataOwner();if(!sameCdcConfiguration(before,after) || !sourceCutCovered(before.cut(),after.cut()))throw new IOException("Source preparation lost completed metadata cut");return Message.valueOf(ByteString.copyFrom(RatisSourceOutcomeRpc.prepared(prepared)));});
            }
            if(RatisOperationRerouteAdmissionRpc.kind(kind)) {
                if(!freshBarrier || role!=RatisNodePlan.Role.TRANSACTION_HOME || !homeReservations || operationRerouteQueries==null)throw new IOException("Reroute admission requires configured fresh HOME barrier");
                if(kind==RatisOperationRerouteAdmissionRpc.CANCEL) { var command=RatisOperationRerouteAdmissionRpc.decode(encoded);cancelImportPublication(command);return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(encoded))); }
                long generation=admissionGeneration.get();
                return operationRerouteQueries.submit(store.ownership().owner().groupId(),encoded,2*RatisProofQueryService.IMPORT_PUBLICATION_BYTES,false,owned->{
                    if(generation!=admissionGeneration.get())throw new IOException("Reroute preparation lost read-barrier owner");
                    var prepared=RatisOperationRerouteAdmissionRpc.decodeRequest(owned);prepareImportPublication(prepared.command(),prepared.evidence());return Message.valueOf(ByteString.copyFrom(RatisOperationRerouteAdmissionRpc.preparedReply(RatisOperationRerouteAdmissionRpc.PREPARE,prepared.command())));
                });
            }
            if(kind==RatisTopologyOperationOutcomeRpc.KIND) {
                requireDataRole();
                if(!freshBarrier || proofQueries==null)throw new IOException("Topology operation outcome requires configured fresh metadata barrier");
                var lookup=RatisTopologyOperationOutcomeRpc.decodeRequest(encoded);var owner=checkCdcMetadataOwner();
                if(!owner.sealed())throw new IOException("Operation outcome parent is not sealed");
                return (sourceOutcomeQueries==null ? proofQueries : sourceOutcomeQueries).submit(store.ownership().owner().groupId(),encoded,8L*RatisTopologyOperationOutcomeRpc.MAX_REPLY_BYTES,false,owned->{
                    var before=checkCdcMetadataOwner();if(!before.sealed() || !sameCdcConfiguration(owner,before))throw new IOException("Topology outcome lost read-barrier owner");
                    var original=RatisTopologyOperationOutcomeRpc.decodeRequest(owned);
                    var result=store.readTopologyOperationOutcome(original.clusterId(),original.databaseId(),original.domainId(),original.lineage(),original.operation(),original.receiptDigest());
                    var after=checkCdcMetadataOwner();
                    if(!after.sealed() || !sameCdcConfiguration(owner,after) || !java.util.Objects.equals(result.cut(),after.cut())
                            || result.configuration().term()!=after.configuration().term() || result.configuration().index()!=after.configuration().index() || !Arrays.equals(result.configuration().data(),after.configuration().data()))throw new IOException("Topology outcome native cut/configuration changed");
                    return Message.valueOf(ByteString.copyFrom(RatisTopologyOperationOutcomeRpc.reply(original,result)));
                });
            }
            if(kind==RatisCdcPollRpc.KIND) {
                requireDataRole();
                if(!freshBarrier || cdcBinding==null || proofQueries==null) throw new IOException("CDC poll requires configured fresh DATA barrier");
                var poll=RatisCdcPollRpc.decodeRequest(encoded);
                if(!cdcBinding.equals(poll.binding())) throw new IOException("CDC poll namespace/lineage differs");
                var owner=checkCdcMetadataOwner();
                return proofQueries.submit(store.ownership().owner().groupId(),encoded,Math.max(4096,4L*poll.maxBytes()),false,owned->{
                    var before=checkCdcMetadataOwner();
                    if(!sameCdcConfiguration(owner,before)) throw new IOException("CDC poll lost read-barrier owner");
                    var ownedPoll=RatisCdcPollRpc.decodeRequest(owned);
                    if(before.sealed()) {
                        var terminal=store.topologyFreeze().position(); var from=ownedPoll.from().position();
                        if(from.index()>terminal.index() || from.index()==terminal.index() && from.term()!=terminal.term()) throw new IOException("CDC parent cursor exceeds original terminal boundary");
                    }
                    ReplicatedRangeStore.CdcPage page; byte[] reply;
                    try {
                        var data=before.sealed() ? store.pollRetiredCdc(ownedPoll.binding().lineage(),ownedPoll.from(),ownedPoll.maxRecords(),ownedPoll.maxBytes()-RatisCdcPollRpc.REPLY_BASE_BYTES)
                                : store.pollCdc(ownedPoll.binding().lineage(),ownedPoll.from(),ownedPoll.maxRecords(),ownedPoll.maxBytes()-RatisCdcPollRpc.REPLY_BASE_BYTES);
                        page=data; reply=RatisCdcPollRpc.reply(ownedPoll,data);
                    } catch(ReplicatedRangeStore.CdcGap gap) {
                        var status=gap.page(); page=status;
                        reply=RatisCdcPollRpc.reply(new RatisCdcPollRpc.Result(ownedPoll,RatisCdcPollRpc.Outcome.GAP,status.cut(),status.floor(),status.next(),false,0,List.of()));
                    } catch(ReplicatedRangeStore.CdcTooLarge large) {
                        var status=large.page(); page=status;
                        reply=RatisCdcPollRpc.reply(new RatisCdcPollRpc.Result(ownedPoll,RatisCdcPollRpc.Outcome.TOO_LARGE,status.cut(),status.floor(),status.next(),status.fromVerified(),RatisCdcPollRpc.REPLY_BASE_BYTES+large.requiredPayloadBytes(),List.of()));
                    }
                    var after=checkCdcMetadataOwner();
                    if(!sameCdcConfiguration(owner,after) || !java.util.Objects.equals(page.cut(),after.cut())) throw new IOException("CDC poll lost native cut/configuration");
                    return Message.valueOf(ByteString.copyFrom(reply));
                });
            }
            if(kind==RatisRangeImportFileResultRpc.KIND) {
                if(!freshBarrier || rangeImportApply==null || proofQueries==null)throw new IOException("File result lookup requires configured fresh DATA barrier");
                var file=RatisRangeImportFileResultRpc.decodeRequest(encoded);requireImportProposal(file);long generation=admissionGeneration.get();
                return proofQueries.submit(store.ownership().owner().groupId(),encoded,4L*RatisRangeImportFileResultRpc.MAX_REPLY_BYTES,false,owned->{
                    var leader=leadership();var cut=requireImportOwner(leader);final long stamp;synchronized(admissions){stamp=schemaConfigurationStamp;}
                    requireImportMembership();byte[] original=referenceProof(file);var current=leadership();
                    synchronized(admissions){requireImportOwner(current);if(generation!=admissionGeneration.get() || current.term()!=leader.term() || stamp!=schemaConfigurationStamp || !java.util.Objects.equals(cut,store.applied()))throw new IOException("File result lookup lost native owner/cut");}
                    return Message.valueOf(ByteString.copyFrom(RatisRangeImportFileResultRpc.reply(file,cut,original==null?null:RatisReferencedFile.decode(original))));
                });
            }
            if(kind==ScopedTransactionRpc.TOPOLOGY_SEED) {
                requireDataRole();if(proofQueries==null || topologySeedInstaller==null)throw new IOException("Topology SEED actual owner installer is not configured");var requested=RatisTopologySeedRpc.decodeRequest(encoded);if(!requested.anchorGroup().equals(store.ownership().owner().groupId()))throw new IOException("Topology SEED crossed parent group");
                return proofQueries.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.MAX_BYTES,false,owned->{ var seedRequest=RatisTopologySeedRpc.decodeRequest(owned);var target=new RatisTopologyOwnerRpc.Target(seedRequest.definition(),TopologyChange.Stage.FREEZE,seedRequest.catalogue(),seedRequest.anchorGroup(),seedRequest.replica());topologyOwnerEvidence(target);var seed=topologySeedInstaller.install(seedRequest);seedRequest.requireSeed(seed);topologyOwnerEvidence(target);return Message.valueOf(ByteString.copyFrom(RatisTopologySeedRpc.reply(new RatisTopologySeedRpc.Reply(seedRequest,seed)))); });
            }
            if(kind==ScopedTransactionRpc.TOPOLOGY_OWNER) {
                requireDataRole();if(proofQueries==null)throw new IOException("Topology owner proof dispatcher is not configured");var target=RatisTopologyOwnerRpc.decodeRequest(encoded);if(!target.group().equals(store.ownership().owner().groupId()))throw new IOException("Topology proof crossed native group");
                return proofQueries.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.MAX_BYTES,false,owned->Message.valueOf(ByteString.copyFrom(RatisTopologyOwnerRpc.reply(topologyOwnerEvidence(RatisTopologyOwnerRpc.decodeRequest(owned))))));
            }
            if(kind==RatisRangeImportStatusRpc.KIND) {
                requireDataRole(); if(rangeImportApply==null || importLocalReplica==null || proofQueries==null)throw new IOException("Import status owner is not configured");
                var target=RatisRangeImportStatusRpc.decodeRequest(encoded); target.requireBinding(rangeImportApply.definition(),importLocalReplica);
                var server=getServer().getNow(null); if(server==null || !server.getId().toString().equals(importLocalReplica.peerId()))throw new IOException("Import status server identity differs");
                long generation=admissionGeneration.get(); final long stamp;
                synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Import status waits for configuration apply");stamp=schemaConfigurationStamp;}
                return proofQueries.submit(store.ownership().owner().groupId(),encoded,4L*RatisRangeImportStatusRpc.MAX_REPLY_BYTES,false,owned->{
                    var before=store.applied(); var configuration=requireImportMembership(); var status=rangeImportApply.status(); var cut=status.applied();
                    if(cut==null || configuration.term()>cut.term() || configuration.index()>cut.index() || configuration.index()==cut.index() && configuration.term()!=cut.term())throw new IOException("Import status configuration exceeds completed cut");
                    synchronized(admissions){requireHealthy();if(generation!=admissionGeneration.get() || schemaConfigurationPending || stamp!=schemaConfigurationStamp || !java.util.Objects.equals(before,cut) || !java.util.Objects.equals(cut,store.applied()))throw new IOException("Import status native cut/configuration changed");}
                    return Message.valueOf(ByteString.copyFrom(RatisRangeImportStatusRpc.reply(target,status)));
                });
            }
            if(ScopedTransactionRpc.importApply(kind)) {
                if(!freshBarrier || role!=RatisNodePlan.Role.DATA || rangeImportApply==null || importPreflightQueries==null)throw new IOException("Native import preparation requires configured fresh DATA barrier");
                var record=RatisRangeImportRpc.proposal(encoded); requireImportProposal(record);
                if(kind==ScopedTransactionRpc.IMPORT_APPLY_CANCEL){
                    synchronized(admissions){var hold=importApplyHold;if(hold!=null && Arrays.equals(hold.bytes,importProposalBytes(record)) && !hold.submitted){hold.invalidated=true;releaseImportApply(hold);}}
                    return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(encoded)));
                }
                long generation=admissionGeneration.get();
                long weight=record instanceof RatisRangeImportRecord.Activate ? 4L*RatisRangeImportAuthority.MAX_REPLY_BYTES
                        : 4L*qualifiedImportManifestBytes(record);
                return importPreflightQueries.submit(store.ownership().owner().groupId(),encoded,weight,false,owned->{
                    prepareImportApply(RatisRangeImportRpc.proposal(owned),generation);return Message.valueOf(ByteString.copyFrom(owned));
                });
            }
            if(kind>=RatisSnapshotCohortRpc.OWNER && kind<=RatisSnapshotCohortRpc.CANCEL){requireDataRole();if(snapshotCohorts==null)throw new IOException("Cohort capture is not configured");if(kind==RatisSnapshotCohortRpc.OWNER && !freshBarrier)throw new IOException("Cohort owner qualification requires fresh barrier");var packet=ScopedTransactionRpc.parse(encoded,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES);return snapshotCohorts.query(store.ownership().owner().groupId(),kind,packet.body()).thenApply(bytes->Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(kind,bytes))));}
            if(kind==RatisTopologyLearnerRecoveryRpc.KIND){if(topologyImageQueries==null || learnerRecoveryInstaller==null || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Learner recovery dispatcher unavailable");return topologyImageQueries.submit(store.ownership().owner().groupId(),encoded,8L*RatisTopologyLearnerRecoveryRpc.MAX_REPLY_BYTES,false,owned->{try{return Message.valueOf(ByteString.copyFrom(RatisTopologyLearnerRecoveryRpc.reply(learnerRecoveryInstaller.prepare(RatisTopologyLearnerRecoveryRpc.decode(owned)))));}catch(RatisTopologySourceSnapshotRpc.AbandonedException terminal){return Message.valueOf(ByteString.copyFrom(RatisTopologyLearnerRecoveryRpc.reply(terminal.receipt())));}});}
            if(kind==RatisTopologyLearnerJoinRpc.KIND){if(topologyImageQueries==null || learnerInstaller==null || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Node learner bootstrap unavailable");return topologyImageQueries.submit(store.ownership().owner().groupId(),encoded,8L*RatisTopologyLearnerJoinRpc.MAX_REPLY_BYTES,false,owned->{var join=RatisTopologyLearnerJoinRpc.decode(owned);var registration=learnerInstaller.install(join);return Message.valueOf(ByteString.copyFrom(RatisTopologyLearnerJoinRpc.reply(join,registration)));});}
            if(kind==RatisSourceHeadRpc.KIND){
                requireDataRole();if(!freshBarrier || sourceSnapshots==null || topologyImageQueries==null)throw new IOException("Source head requires fresh DATA dispatcher");var head=RatisSourceHeadRpc.request(encoded);if(!head.source().sourceGroup().equals(store.ownership().owner().groupId()))throw new IOException("Source head crossed group");
                return topologyImageQueries.submit(store.ownership().owner().groupId(),encoded,8L*(encoded.length+RatisSourceHeadRpc.MAX_REPLY_BYTES),false,owned->Message.valueOf(ByteString.copyFrom(RatisSourceHeadRpc.reply(sourceSnapshots.head(RatisSourceHeadRpc.request(owned))))));
            }
            if(kind==RatisRecoveryLineageRpc.KIND){
                if(!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE || schemaColumn==null || schemaAuthorityQueries==null)throw new IOException("Recovery lineage requires fresh catalogue dispatcher");
                checkCatalogueOwner();long generation=admissionGeneration.get();var owner=leadership();final long configurationStamp;
                synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Recovery lineage waits for configuration apply");configurationStamp=schemaConfigurationStamp;}
                return schemaAuthorityQueries.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.MAX_BYTES,false,owned->{
                    var query=RatisRecoveryLineageRpc.request(owned);
                    if(!query.cluster().equals(schemaCluster) || !query.database().equals(schemaDatabase) || !query.catalogue().equals(store.ownership().owner().groupId()) || !query.column().equals(schemaColumn))throw new IOException("Recovery lineage crossed catalogue namespace");
                    checkCatalogueOwner();if(generation!=admissionGeneration.get() || leadership().term()!=owner.term())throw new IOException("Recovery lineage lost read-barrier owner");
                    var lookup=new RatisRecoveryLineage(schemaCluster,schemaDatabase,query.catalogue(),store,schemaColumn).lookup(new RatisRecoveryLineage.Request(query.domain(),query.range(),query.group(),query.epoch()),query.minimumCatalogueCut());
                    checkCatalogueOwner();var current=leadership();
                    synchronized(admissions){if(generation!=admissionGeneration.get() || current.term()!=owner.term() || lookup.catalogueCut().term()!=current.term() || getLifeCycleState()!=LifeCycle.State.RUNNING || schemaConfigurationPending || configurationStamp!=schemaConfigurationStamp || !lookup.catalogueCut().equals(store.applied()))throw new IOException("Recovery lineage lost completed cut/configuration");}
                    return Message.valueOf(ByteString.copyFrom(RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(query,lookup.catalogueCut(),lookup.birth()==null?null:lookup.birth().definition()))));
                });
            }
            if(kind==RatisCatalogueViewRpc.KIND){
                if(!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE || schemaColumn==null || schemaAuthorityQueries==null)throw new IOException("Catalogue view requires fresh catalogue dispatcher");checkCatalogueOwner();long generation=admissionGeneration.get();var leader=leadership();
                return schemaAuthorityQueries.submit(store.ownership().owner().groupId(),encoded,4L*RatisCatalogueViewRpc.MAX_REPLY_BYTES,false,owned->{
                    var query=RatisCatalogueViewRpc.request(owned);if(!query.cluster().equals(schemaCluster) || !query.database().equals(schemaDatabase) || !query.catalogue().equals(store.ownership().owner().groupId()) || !query.column().equals(schemaColumn))throw new IOException("Catalogue view crossed namespace");
                    checkCatalogueOwner();var point=new ReplicatedRangeStore.PhysicalPoint(schemaColumn,ByteBuffer.allocate(16).putLong(query.domain().getMostSignificantBits()).putLong(query.domain().getLeastSignificantBits()).array());var read=store.readSchemaAuthorityPoints(List.of(point),new int[]{RatisCatalogue.MAX_DIRECTORY_BYTES});var bytes=read.values().getFirst();var directory=bytes==null?null:RangeDirectoryCodec.decode(bytes);
                    checkCatalogueOwner();synchronized(admissions){if(generation!=admissionGeneration.get() || schemaConfigurationPending || leadership().term()!=leader.term() || read.position().term()!=leader.term() || !read.position().equals(store.applied()))throw new IOException("Catalogue view lost completed barrier owner");}
                    return Message.valueOf(ByteString.copyFrom(RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(query,read.position(),directory))));
                });
            }
            if(RatisTopologySourceSnapshotRpc.kind(kind)){
                requireDataRole();if((kind!=RatisTopologySourceSnapshotRpc.RELEASE && !freshBarrier) || sourceSnapshots==null || topologyImageQueries==null || getLifeCycleState()!=LifeCycle.State.RUNNING)throw new IOException("Source snapshot dispatcher unavailable");var sourceRequest=RatisTopologySourceSnapshotRpc.request(encoded);if(!sourceRequest.sourceGroup().equals(store.ownership().owner().groupId()))throw new IOException("Source snapshot crossed group");
                if(kind==RatisTopologySourceSnapshotRpc.PREPARE)return sourceSnapshots.prepareAsync(sourceRequest).handle((prepared,failed)->{
                    Throwable problem=failed;while(problem instanceof java.util.concurrent.CompletionException && problem.getCause()!=null)problem=problem.getCause();
                    if(problem instanceof RatisTopologySourceSnapshotRpc.AbandonedException terminal)return Message.valueOf(ByteString.copyFrom(RatisTopologySourceSnapshotRpc.reply(kind,terminal.receipt())));
                    if(problem!=null)throw new java.util.concurrent.CompletionException(problem);return Message.valueOf(ByteString.copyFrom(RatisTopologySourceSnapshotRpc.reply(kind,prepared)));
                });
                if(kind==RatisTopologySourceSnapshotRpc.TRANSFER)return sourceSnapshots.transfer(sourceRequest).handle((prepared,failed)->{
                    Throwable problem=failed;while(problem instanceof java.util.concurrent.CompletionException && problem.getCause()!=null)problem=problem.getCause();
                    if(problem instanceof RatisTopologySourceSnapshotRpc.AbandonedException terminal)return Message.valueOf(ByteString.copyFrom(RatisTopologySourceSnapshotRpc.reply(kind,terminal.receipt())));
                    if(problem!=null)throw new java.util.concurrent.CompletionException(problem);return Message.valueOf(ByteString.copyFrom(RatisTopologySourceSnapshotRpc.reply(kind,prepared)));
                });
                return topologyImageQueries.submit(store.ownership().owner().groupId(),encoded,8L*RatisTopologySourceSnapshotRpc.maximumPreparedBytes(sourceRequest),false,owned->{sourceSnapshots.release(RatisTopologySourceSnapshotRpc.request(owned));return Message.valueOf(ByteString.copyFrom(owned));});
            }
            if(RatisRelocationAdmissionRpc.kind(kind)){if(role!=RatisNodePlan.Role.DATA || proofQueries==null)throw new IOException("Relocation dispatcher unavailable");var record=RatisRelocationAdmissionRpc.decode(encoded);if(kind==RatisRelocationAdmissionRpc.CANCEL){synchronized(admissions){if(relocationHold!=null && relocationHold.record.equals(record) && !relocationHold.submitted)releaseRelocation(relocationHold);}return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(encoded)));}if(!freshBarrier)throw new IOException("Relocation preparation needs fresh leader barrier");return proofQueries.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.MAX_BYTES,false,owned->{prepareRelocation(RatisRelocationAdmissionRpc.decode(owned));return Message.valueOf(ByteString.copyFrom(owned));});}
            if(kind==RatisRecoveryImageRpc.KIND){
                requireDataRole();if(proofQueries==null)throw new IOException("Recovery image receipt dispatcher unavailable");var probe=RatisRecoveryImageRpc.decode(encoded);if(!probe.source().sourceGroup().equals(store.ownership().owner().groupId()))throw new IOException("Recovery receipt crossed native group");
                return proofQueries.submit(store.ownership().owner().groupId(),encoded,256L+4L*encoded.length+4L*RatisRecoveryImageRpc.MAX_REPLY_BYTES,false,owned->Message.valueOf(ByteString.copyFrom(RatisRecoveryImageRpc.reply(recoveryImageEvidence(RatisRecoveryImageRpc.decode(owned))))));
            }
            if(kind==RatisRelocationOwnerRpc.KIND){
                if(role!=RatisNodePlan.Role.DATA || relocationOwnerQueries==null || relocationAuthority==null)throw new IOException("Relocation owner dispatcher is not configured");var ownerRequest=RatisRelocationOwnerRpc.decode(encoded);if(!freshBarrier && ownerRequest.replica()==null && (ownerRequest.expected()==RatisRelocationOwnerRpc.Expected.SOURCE || ownerRequest.expected()==RatisRelocationOwnerRpc.Expected.TARGET))throw new IOException("Relocation leader proof requires fresh barrier");return relocationOwnerQueries.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.RELOCATION_OWNER_BYTES,false,owned->Message.valueOf(ByteString.copyFrom(RatisRelocationOwnerRpc.reply(relocationEvidence(RatisRelocationOwnerRpc.decode(owned))))));
            }
            if(kind==RatisTopologyJoinAuthority.KIND){
                if(!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE || schemaColumn==null || schemaAuthorityQueries==null)throw new IOException("Topology join authority requires configured fresh catalogue dispatcher");
                checkCatalogueOwner();long generation=admissionGeneration.get();var owner=leadership();final long configurationStamp;
                synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Topology join authority waits for configuration apply");configurationStamp=schemaConfigurationStamp;}
                return schemaAuthorityQueries.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.MAX_BYTES,false,owned->{
                    checkCatalogueOwner();if(generation!=admissionGeneration.get() || leadership().term()!=owner.term())throw new IOException("Topology join authority lost read-barrier owner");
                    var view=RatisTopologyJoinAuthority.readNative(store,schemaColumn,schemaCluster,schemaDatabase,RatisTopologyJoinAuthority.decodeRequest(owned));
                    checkCatalogueOwner();var current=leadership();
                    synchronized(admissions){if(generation!=admissionGeneration.get() || current.term()!=owner.term() || view.cut().term()!=current.term()
                            || getLifeCycleState()!=LifeCycle.State.RUNNING || schemaConfigurationPending || configurationStamp!=schemaConfigurationStamp || !java.util.Objects.equals(view.cut(),store.applied()))throw new IOException("Topology join authority cut/configuration changed");}
                    return Message.valueOf(ByteString.copyFrom(RatisTopologyJoinAuthority.reply(view)));
                });
            }
            if(kind==RatisTopologyAuthority.KIND){
                if(!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE || schemaColumn==null || schemaAuthorityQueries==null)throw new IOException("Topology authority requires configured fresh catalogue dispatcher");
                checkCatalogueOwner();long generation=admissionGeneration.get();var owner=leadership();final long configurationStamp;
                synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Topology authority waits for configuration apply");configurationStamp=schemaConfigurationStamp;}
                return schemaAuthorityQueries.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.MAX_BYTES,false,owned->{
                    checkCatalogueOwner();if(generation!=admissionGeneration.get() || leadership().term()!=owner.term())throw new IOException("Topology authority lost read-barrier owner");
                    var view=RatisTopologyAuthority.readNative(store,schemaColumn,schemaCluster,schemaDatabase,RatisTopologyAuthority.decodeRequest(owned));
                    checkCatalogueOwner();var current=leadership();
                    synchronized(admissions){if(generation!=admissionGeneration.get() || current.term()!=owner.term() || view.cut().term()!=current.term()
                            || getLifeCycleState()!=LifeCycle.State.RUNNING || schemaConfigurationPending || configurationStamp!=schemaConfigurationStamp || !java.util.Objects.equals(view.cut(),store.applied()))throw new IOException("Topology authority cut/configuration changed");}
                    return Message.valueOf(ByteString.copyFrom(RatisTopologyAuthority.reply(view)));
                });
            }
            if(ScopedTransactionRpc.topologyActivate(kind)) {
                if(role!=RatisNodePlan.Role.DATA || topologyAuthority==null || proofQueries==null)throw new IOException("Topology activation requires configured DATA dispatcher");
                var record=RatisTopologyActivationRpc.request(encoded);
                if(kind==RatisTopologyActivationRpc.CANCEL) { synchronized(admissions) { var hold=topologyActivationHold;if(hold!=null && hold.record.equals(record) && !hold.submitted) { hold.invalidated=true;releaseTopologyActivation(hold); } } return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(RatisTopologyActivationRpc.cancelledReply(record)))); }
                if(kind==RatisTopologyActivationRpc.PREPARE && !freshBarrier)throw new IOException("Topology activation preparation requires fresh leader barrier");
                long generation=admissionGeneration.get();
                return proofQueries.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.MAX_BYTES,false,owned->{ var requested=RatisTopologyActivationRpc.request(owned);if(kind==RatisTopologyActivationRpc.PREPARE) { if(generation!=admissionGeneration.get())throw new IOException("Topology activation lost barrier generation");prepareTopologyActivation(requested,generation); } return Message.valueOf(ByteString.copyFrom(RatisTopologyActivationRpc.reply(kind,topologyActivationState(requested)))); });
            }
            if(ScopedTransactionRpc.topologyFreeze(kind)){
                requireDataRole();if(topologyAuthority==null || proofQueries==null)throw new IOException("Topology FREEZE dispatcher is not configured");
                var record=RatisTopologyFreezeRpc.request(encoded);
                if(kind==ScopedTransactionRpc.TOPOLOGY_FREEZE_CANCEL){synchronized(admissions){var hold=topologyHold;if(hold!=null && hold.record.equals(record) && !hold.submitted){hold.invalidated=true;releaseTopologyHold(hold);}}return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(RatisTopologyFreezeRpc.cancelledReply(record))));}
                if(kind==ScopedTransactionRpc.TOPOLOGY_FREEZE_PREPARE && !freshBarrier)throw new IOException("Topology FREEZE preparation requires a fresh barrier");long generation=admissionGeneration.get();
                return proofQueries.submit(store.ownership().owner().groupId(),encoded,4L*encoded.length+RatisTopologyFreezeRpc.MAX_REPLY_BYTES,false,owned->{var requested=RatisTopologyFreezeRpc.request(owned);if(kind==ScopedTransactionRpc.TOPOLOGY_FREEZE_PREPARE){if(generation!=admissionGeneration.get())throw new IOException("Topology FREEZE lost barrier owner");prepareTopologyFreeze(requested,generation);}return Message.valueOf(ByteString.copyFrom(RatisTopologyFreezeRpc.reply(kind,topologyState(requested))));});
            }
            if(kind==RatisRangeImportAuthority.KIND){
                if(!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE || schemaColumn==null || schemaAuthorityQueries==null)throw new IOException("Import authority requires configured fresh catalogue dispatcher");
                checkCatalogueOwner();long generation=admissionGeneration.get();var owner=leadership();final long configurationStamp;
                synchronized(admissions){if(schemaConfigurationPending)throw new IOException("Import authority waits for configuration apply");configurationStamp=schemaConfigurationStamp;}
                return schemaAuthorityQueries.submit(store.ownership().owner().groupId(),encoded,4L*RatisRangeImportAuthority.MAX_REPLY_BYTES,false,owned->{
                    checkCatalogueOwner();if(generation!=admissionGeneration.get() || leadership().term()!=owner.term())throw new IOException("Import authority lost read-barrier owner");
                    var view=RatisRangeImportAuthority.readNative(store,schemaColumn,schemaCluster,schemaDatabase,RatisRangeImportAuthority.decodeRequest(owned));
                    checkCatalogueOwner();var current=leadership();
                    synchronized(admissions){if(generation!=admissionGeneration.get() || current.term()!=owner.term() || view.publication().completedCatalogueCut().term()!=current.term()
                            || getLifeCycleState()!=LifeCycle.State.RUNNING || schemaConfigurationPending || configurationStamp!=schemaConfigurationStamp || !java.util.Objects.equals(view.publication().completedCatalogueCut(),store.applied()))throw new IOException("Import authority cut/configuration changed");}
                    return Message.valueOf(ByteString.copyFrom(RatisRangeImportAuthority.reply(view)));
                });
            }
            if(ScopedTransactionRpc.topologyExecution(kind)) {
                if(!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE)throw new IOException("Topology execution preparation requires fresh catalogue barrier");
                var command=RatisTopologyExecutionRpc.decode(encoded);
                if(kind==RatisTopologyExecutionRpc.CANCEL) { cancelImportPublication(command);return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(encoded))); }
                var executor=importPublicationQueries;if(executor==null)throw new IOException("Bounded topology execution dispatcher is not configured");long generation=admissionGeneration.get();
                return executor.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.IMPORT_PUBLICATION_BYTES,false,owned->{if(generation!=admissionGeneration.get())throw new IOException("Topology execution lost read-barrier owner");prepareImportPublication(RatisTopologyExecutionRpc.decode(owned));return Message.valueOf(ByteString.copyFrom(owned));});
            }
            if(ScopedTransactionRpc.importPublication(kind)){
                if(!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE)throw new IOException("Import publication requires a fresh catalogue barrier");
                if(kind==ScopedTransactionRpc.IMPORT_PUBLICATION_CANCEL){var command=RatisImportPublicationRpc.request(encoded);cancelImportPublication(command);return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(RatisImportPublicationRpc.preparedReply(kind,command))));}
                var executor=importPublicationQueries;if(executor==null)throw new IOException("Bounded import publication dispatcher is not configured");long generation=admissionGeneration.get();
                return executor.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.IMPORT_PUBLICATION_BYTES,false,owned->{if(generation!=admissionGeneration.get())throw new IOException("Import publication lost read-barrier owner");var command=RatisImportPublicationRpc.request(owned);prepareImportPublication(command);return Message.valueOf(ByteString.copyFrom(RatisImportPublicationRpc.preparedReply(kind,command)));});
            }
            if(RatisCdcDeliveryRpc.kind(kind)) {
                if(!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE) throw new IOException("CDC delivery requires a fresh catalogue barrier");
                var command=RatisCdcDeliveryRpc.decode(encoded);
                if(kind==RatisCdcDeliveryRpc.CANCEL) { cancelImportPublication(command); return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(encoded))); }
                var executor=importPublicationQueries; if(executor==null) throw new IOException("Bounded catalogue preparation dispatcher is not configured");
                long generation=admissionGeneration.get();
                return executor.submit(store.ownership().owner().groupId(),encoded,RatisProofQueryService.IMPORT_PUBLICATION_BYTES,false,owned->{
                    if(generation!=admissionGeneration.get()) throw new IOException("CDC delivery lost read-barrier owner");
                    prepareImportPublication(RatisCdcDeliveryRpc.decode(owned)); return Message.valueOf(ByteString.copyFrom(owned));
                });
            }
            if (kind==ScopedTransactionRpc.SNAPSHOT_HELP) return snapshotHelpQuery(encoded,freshBarrier);
            if (kind==ScopedTransactionRpc.SCHEMA_FENCING_PREPARE || kind==ScopedTransactionRpc.SCHEMA_FENCE_PREPARE || kind==ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE || kind==ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE || kind==ScopedTransactionRpc.PARTICIPANT_AUTHORIZE || kind==ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_PREPARE || kind==ScopedTransactionRpc.SNAPSHOT_RETIRE_PREPARE || kind==ScopedTransactionRpc.HOME_RETIRE_PREPARE || kind==ScopedTransactionRpc.PARTICIPANT_RETIRE_PREPARE || kind==ScopedTransactionRpc.FINALIZATION_PREPARE || kind==ScopedTransactionRpc.HOME_RECOVERY_PREPARE || kind==ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE) {
                // Reject before coalescing: a stale request must never borrow a fresh request's authority.
                if (!freshBarrier) throw new IOException("Proof preparation requires a fresh linearizable barrier");
                if (kind==ScopedTransactionRpc.SCHEMA_FENCING_PREPARE) { if (role!=RatisNodePlan.Role.CATALOGUE) throw new IOException("Only CATALOGUE prepares FENCING"); }
                else if (kind==ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_PREPARE) { if (role!=RatisNodePlan.Role.SNAPSHOT_HOME) throw new IOException("Only snapshot HOME prepares retirement"); }
                else if (kind==ScopedTransactionRpc.HOME_RECOVERY_PREPARE || kind==ScopedTransactionRpc.HOME_RETIRE_PREPARE) {
                    if (role!=RatisNodePlan.Role.TRANSACTION_HOME) throw new IOException("Only transaction HOME prepares protected recovery");
                } else requireDataRole();
            }
            if (kind==ScopedTransactionRpc.SNAPSHOT_CAPTURE) requireDataRole();
            if (kind==ScopedTransactionRpc.SCHEMA_AUTHORITY || kind==ScopedTransactionRpc.SCHEMA_FENCING_PREPARE || kind==ScopedTransactionRpc.SCHEMA_FENCE_PREPARE || kind==ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE || kind==ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE || kind==ScopedTransactionRpc.PARTICIPANT_AUTHORIZE || kind==ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_PREPARE || kind==ScopedTransactionRpc.SNAPSHOT_RETIRE_PREPARE || kind==ScopedTransactionRpc.HOME_RETIRE_PREPARE || kind==ScopedTransactionRpc.PARTICIPANT_RETIRE_PREPARE || kind==ScopedTransactionRpc.FINALIZATION_PREPARE || kind==ScopedTransactionRpc.HOME_RECOVERY_PREPARE || kind==ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE || kind==ScopedTransactionRpc.SNAPSHOT_CAPTURE) {
                // Only trusted unconfigured catalogue fixtures may borrow the ordinary lane.
                if (kind==ScopedTransactionRpc.SCHEMA_AUTHORITY && schemaAuthorityQueries==null && schemaFencingReplicas!=null) throw new IOException("Catalogue authority dispatcher is not configured");
                var executor=kind==ScopedTransactionRpc.SCHEMA_AUTHORITY && schemaAuthorityQueries!=null ? schemaAuthorityQueries : proofQueries;
                if (executor==null) throw new IOException("Bounded proof query dispatcher is not configured");
                long generation=admissionGeneration.get();
                return executor.submit(store.ownership().owner().groupId(),encoded,
                        kind==ScopedTransactionRpc.SCHEMA_AUTHORITY || kind==ScopedTransactionRpc.SCHEMA_FENCING_PREPARE ? RatisProofQueryService.MAX_BYTES : encoded.length,
                        kind!=ScopedTransactionRpc.SCHEMA_FENCING_PREPARE && kind!=ScopedTransactionRpc.SNAPSHOT_CAPTURE && kind!=ScopedTransactionRpc.SCHEMA_FENCE_PREPARE && kind!=ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE && kind!=ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,ownedRequest -> {
                    // Owner-specific retained captures deliberately survive leader changes.
                    if (kind!=ScopedTransactionRpc.SNAPSHOT_CAPTURE && generation!=admissionGeneration.get()) throw new IOException("Proof query lost its read-barrier owner");
                    return framedQuery(ownedRequest,freshBarrier);
                });
            }
            return CompletableFuture.completedFuture(framedQuery(encoded,freshBarrier));
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private RatisNodeResourceBudget.Lease readConstructionLease(int kind,ByteBuffer request) throws IOException {
        long responseBytes;
        switch (kind) {
            case RatisRecoveryImageRpc.KIND -> {return reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+4L*request.remaining()+4L*RatisRecoveryImageRpc.MAX_REPLY_BYTES,1);}
            case ScopedTransactionRpc.POINT, ScopedTransactionRpc.GET, ScopedTransactionRpc.SNAPSHOT_GET ->
                    responseBytes=4L*DistributedCommandCodec.MAX_ENCODED_BYTES;
            case ScopedTransactionRpc.PAGE, ScopedTransactionRpc.SNAPSHOT_PAGE -> {
                if (request.remaining()<ScopedTransactionRpc.HEADER_BYTES+8) throw new IOException("Truncated page request");
                int end=request.limit(),items=request.getInt(end-8),bytes=request.getInt(end-4);
                if (items<1 || items>it.cavallium.rockserver.core.common.distributed.PhysicalRangePage.MAX_ITEMS
                        || bytes<it.cavallium.rockserver.core.common.distributed.PhysicalRangePage.HEADER_BYTES
                        || bytes>it.cavallium.rockserver.core.common.distributed.PhysicalRangePage.MAX_BYTES)
                    throw new IOException("Invalid page construction budget");
                // Native rows, immutable row copies, serialization and response framing overlap.
                responseBytes=8L*bytes;
            }
            default -> { return reserveNodeResources(RatisNodeResourceBudget.Kind.PROTECTED,256L+4L*request.remaining(),1); }
        }
        var lane=RatisNodeResourceBudget.Kind.ORDINARY;
        // Exact configured HOME status reads are required by protected completion protocols.
        if(kind==ScopedTransactionRpc.POINT && (homeReservations || snapshotHomeConfigured)
                && request.remaining()==ScopedTransactionRpc.HEADER_BYTES+36
                && request.getLong(ScopedTransactionRpc.HEADER_BYTES)==homeColumn.getMostSignificantBits()
                && request.getLong(ScopedTransactionRpc.HEADER_BYTES+8)==homeColumn.getLeastSignificantBits()
                && request.getInt(ScopedTransactionRpc.HEADER_BYTES+16)==16)
            lane=RatisNodeResourceBudget.Kind.PROTECTED;
        return reserveNodeResources(lane,256L+4L*request.remaining()+responseBytes,1);
    }

    private CompletableFuture<Message> snapshotHelpQuery(byte[] encoded,boolean freshBarrier) throws Exception {
        if (!freshBarrier) throw new IOException("Snapshot helping requires a fresh linearizable barrier"); requireDataRole();
        var input=ByteBuffer.wrap(ScopedTransactionRpc.parse(encoded,ScopedTransactionRpc.MAX_REQUEST_BYTES).body());
        var binding=ScopedTransactionRpc.snapshotBinding(input); end(input); var group=store.ownership().owner().groupId();
        var executor=snapshotHelpExecutor; if (executor==null || snapshotRecovery==null) throw new IOException("Bounded snapshot recovery is not configured");
        long generation=admissionGeneration.get();
        return executor.submit(group,binding,() -> {
            checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot helper lost its read-barrier owner");
            var record=store.snapshotGate(binding.fenceId()); boolean helped=false;
            if (record!=null) {
                var fence=record.fence();
                if (!binding.equals(ScopedTransactionRpc.SnapshotBinding.of(fence)) || !group.equals(record.participantGroup())) throw new IOException("Snapshot helper native binding changed");
                if (record.active()) {
                    final RatisSnapshotReadinessHelper helper;
                    synchronized (admissions) {
                        if (generation!=admissionGeneration.get()) throw new IOException("Snapshot helper owner changed");
                        var known=snapshotHelpers.get(fence.fenceId());
                        if (known==null) {
                            if (snapshotHelpers.size()>=ReplicatedRangeStore.MAX_SNAPSHOT_GATES) throw new IOException("Snapshot helper context budget exhausted");
                            known=new RatisSnapshotReadinessHelper(fence,group,(after,limit) -> snapshotBlockerPage(fence,after,limit),store::preparedParticipant,snapshotRecovery);
                            snapshotHelpers.put(fence.fenceId(),known);
                        }
                        helper=known;
                    }
                    try { helped=helper.help(); }
                    finally {
                        boolean remove=generation!=admissionGeneration.get();
                        if (!remove) {
                            try { var current=store.recoveredSnapshotGate(fence); remove=current==null || !current.active(); }
                            catch (RuntimeException | org.rocksdb.RocksDBException unavailable) { remove=true; }
                        }
                        // Context cleanup never settles the actual recovery task owned by the dispatcher.
                        if (remove) synchronized (admissions) { snapshotHelpers.remove(fence.fenceId(),helper); }
                    }
                }
            }
            checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot helper reply lost its owner");
            if (record!=null && record.active()) {
                var after=store.recoveredSnapshotGate(record.fence());
                if (after==null || !after.active()) throw new IOException("Snapshot gate retired while helping");
            }
            return helped;
        }).thenApply(helped -> {
            var body=ByteBuffer.allocate(65); ScopedTransactionRpc.snapshotBinding(body,binding); ScopedTransactionRpc.uuid(body,group); body.put((byte)(helped ? 1 : 0));
            return Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_HELP,body.array())));
        });
    }
    private RatisSnapshotBlockers snapshotBlockerPage(SnapshotFence fence,UUID after,int limit) throws IOException {
        checkTransactionOwner(); var page=store.snapshotBlockers(fence,after,limit); checkTransactionOwner();
        var records=page.records().stream().map(p -> new RatisSnapshotBlockers.Reference(p.preparation().transactionId(),p.preparation().digest(),p.position())).toList();
        return new RatisSnapshotBlockers(ScopedTransactionRpc.SnapshotBinding.of(fence),page.gate().participantGroup(),page.gate().installed(),page.position(),records,page.more());
    }
    public void cancelSnapshotGate(RatisSnapshotRecord request) throws IOException {
        checkTransactionOwner(); requireSnapshotParticipant(request);
        synchronized (admissions) {
            var ready=preparedSnapshots.get(request.operation());
            if (ready!=null) { if (!ready.request.equals(request)) throw new IllegalArgumentException("Snapshot cancellation request changed"); ready.close(); }
            if (preparingSnapshots.containsKey(request.operation())) throw new IOException("Snapshot HOME read is still active");
            var admitted=admissions.get(request.operation());
            if (admitted!=null && (admitted.snapshot==null || !admitted.snapshot.equals(request))) throw new IllegalArgumentException("Snapshot cancellation identity changed");
        }
    }
    private Message framedQuery(byte[] encoded,boolean freshBarrier) throws Exception {
        var packet=ScopedTransactionRpc.parse(encoded,ScopedTransactionRpc.MAX_REQUEST_BYTES);
        if (role!=RatisNodePlan.Role.DATA && !(role==RatisNodePlan.Role.CATALOGUE && schemaColumn!=null && (packet.kind()==ScopedTransactionRpc.SCHEMA_AUTHORITY || ScopedTransactionRpc.schemaFencing(packet.kind()))) && packet.kind()!=ScopedTransactionRpc.POINT && packet.kind()!=ScopedTransactionRpc.STATE && !(role==RatisNodePlan.Role.TRANSACTION_HOME && (ScopedTransactionRpc.homeRecovery(packet.kind()) || packet.kind()==ScopedTransactionRpc.HOME_FENCE || packet.kind()==ScopedTransactionRpc.HOME_RETIRE_PREPARE || packet.kind()==ScopedTransactionRpc.HOME_RETIRE_CANCEL)) && !(role==RatisNodePlan.Role.SNAPSHOT_HOME && (packet.kind()==ScopedTransactionRpc.SNAPSHOT_HOME_FENCE || packet.kind()==ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_PREPARE || packet.kind()==ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_CANCEL))) throw new IOException("Range role rejects transaction/snapshot queries");
        var input=ByteBuffer.wrap(packet.body()); byte[] reply;
        switch (packet.kind()) {
            case ScopedTransactionRpc.SNAPSHOT_GATE_PREPARE, ScopedTransactionRpc.SNAPSHOT_GATE_CANCEL -> {
                if (!freshBarrier) throw new IOException("Snapshot gate authority requires a fresh linearizable barrier");
                long generation=admissionGeneration.get(); checkTransactionOwner();
                var request=RatisSnapshotRecord.decode(packet.body()); requireSnapshotParticipant(request);
                if (packet.kind()==ScopedTransactionRpc.SNAPSHOT_GATE_CANCEL) { cancelSnapshotGate(request); reply=request.digest(); }
                else {
                    var receipt=request.receipt(store.snapshotGateResult(request.fence())); boolean drained=true;
                    if (receipt==null) {
                        if (snapshotHomes==null) throw new IOException("Qualified snapshot HOME reader is not configured");
                        var proposal=request.isRelease() ? prepareSnapshotRelease(snapshotHomes,request.fence()) : prepareSnapshotInstall(snapshotHomes,request.fence());
                        if (!proposal.request().equals(request)) { proposal.close(); throw new IOException("Fresh snapshot HOME authority differs from frozen request"); }
                        receipt=request.receipt(store.snapshotGateResult(request.fence()));
                        if (receipt!=null) proposal.close(); else drained=proposal.drained();
                    }
                    byte[] retained=receipt==null ? null : receipt.encode(); var output=ByteBuffer.allocate(41+(retained==null ? 0 : retained.length));
                    ScopedTransactionRpc.putBlob(output,request.digest()); output.put((byte)(receipt!=null ? 2 : drained ? 1 : 0)); ScopedTransactionRpc.putBlob(output,retained); reply=output.array();
                }
                checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot authority query lost its owner");
            }
            case ScopedTransactionRpc.FINALIZATION_PREPARE, ScopedTransactionRpc.FINALIZATION_CANCEL -> {
                if (!freshBarrier) throw new IOException("Participant finalization requires a fresh linearizable barrier");
                long generation=admissionGeneration.get(); checkTransactionOwner();
                var finalization=RatisParticipantRecord.decodeFinalization(packet.body());
                // Native validation binds the participant owner, schema and the exact finalization control.
                var retained=store.recoveredFinalization(finalization);
                if (packet.kind()==ScopedTransactionRpc.FINALIZATION_CANCEL) {
                    cancelFinalization(finalization); reply=RatisParticipantRecord.digest(finalization);
                } else {
                    if (retained==null) {
                        if (transactionHomes==null) throw new IOException("Qualified transaction HOME reader is not configured");
                        var proposal=prepareFinalization(transactionHomes,finalization.transactionId(),finalization.operation());
                        if (!proposal.finalization().equals(finalization)) { proposal.close(); throw new IOException("Fresh HOME authority differs from frozen finalization"); }
                        retained=store.recoveredFinalization(finalization); if (retained!=null) proposal.close();
                    }
                    byte[] position=retained==null ? null : retained.encode();
                    var output=ByteBuffer.allocate(40+(position==null ? 0 : position.length));
                    ScopedTransactionRpc.putBlob(output,RatisParticipantRecord.digest(finalization)); ScopedTransactionRpc.putBlob(output,position); reply=output.array();
                }
                checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Finalization query lost its owner");
            }
            case ScopedTransactionRpc.SNAPSHOT_HOME_FENCE -> {
                if (!freshBarrier) throw new IOException("Snapshot HOME proof requires a fresh linearizable barrier"); var identity=SnapshotHomeIdentity.decode(packet.body()); requireSnapshotHomeIdentity(identity); checkHomeOwner();
                var proof=store.snapshotHomeFence(identity); byte[] bytes=proof==null ? null : proof.encode(); var output=ByteBuffer.allocate(36+(bytes==null ? 0 : bytes.length)); output.put(identity.digest()); ScopedTransactionRpc.putBlob(output,bytes); reply=output.array();
            }
            case ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_PREPARE, ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_CANCEL -> {
                if (!freshBarrier) throw new IOException("Snapshot HOME retirement requires a fresh linearizable barrier"); var retirement=RatisSnapshotHomeRetirementRecord.decode(packet.body()); requireSnapshotHomeIdentity(retirement.identity()); checkHomeOwner(); long generation=admissionGeneration.get();
                boolean retired; if (packet.kind()==ScopedTransactionRpc.SNAPSHOT_HOME_RETIRE_CANCEL) { cancelSnapshotHomeRetirement(retirement); retired=false; } else retired=prepareSnapshotHomeRetirement(retirement);
                checkHomeOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot HOME retirement query lost its owner"); reply=ByteBuffer.allocate(33).put(retirement.digest()).put((byte)(retired ? 1 : 0)).array();
            }
            case ScopedTransactionRpc.HOME_FENCE -> {
                if (!freshBarrier) throw new IOException("HOME fence requires a fresh linearizable barrier");
                var identity=HomeIdentity.decode(packet.body());
                if (role!=RatisNodePlan.Role.TRANSACTION_HOME || !homeReservations) throw new IOException("HOME fence requires transaction HOME context"); checkHomeOwner();
                var proof=store.homeFence(identity); byte[] bytes=proof==null ? null : proof.encode(); var output=ByteBuffer.allocate(36+(bytes==null ? 0 : bytes.length));
                output.put(identity.digest()); ScopedTransactionRpc.putBlob(output,bytes); reply=output.array();
            }
            case ScopedTransactionRpc.HOME_RETIRE_PREPARE, ScopedTransactionRpc.HOME_RETIRE_CANCEL -> {
                if (!freshBarrier) throw new IOException("HOME retirement requires a fresh linearizable barrier");
                var retirement=RatisHomeRetirementRecord.decode(packet.body()); requireHomeRetirement(retirement); checkHomeOwner(); long generation=admissionGeneration.get();
                boolean retired;
                if (packet.kind()==ScopedTransactionRpc.HOME_RETIRE_CANCEL) { cancelHomeRetirement(retirement); retired=false; }
                else retired=prepareHomeRetirement(retirement);
                checkHomeOwner(); if (generation!=admissionGeneration.get()) throw new IOException("HOME retirement query lost its owner");
                reply=ByteBuffer.allocate(33).put(retirement.digest()).put((byte)(retired ? 1 : 0)).array();
            }
            case ScopedTransactionRpc.HOME_RECOVERY_PREPARE, ScopedTransactionRpc.HOME_RECOVERY_RESULT, ScopedTransactionRpc.HOME_RECOVERY_CANCEL -> {
                if (!freshBarrier) throw new IOException("HOME recovery requires a fresh linearizable barrier");
                long generation=admissionGeneration.get();
                var recovery=HomeRecovery.decode(packet.body()); requireHomeRecovery(recovery); checkHomeOwner();
                if (packet.kind()==ScopedTransactionRpc.HOME_RECOVERY_PREPARE) {
                    // Existing proof finishes a retry without another log append. New work keeps its ready token.
                    var proposal=prepareHomeRecovery(recovery); var receipt=store.homeRecoveryResult(recovery);
                    if (receipt!=null) proposal.close();
                    checkHomeOwner();
                    if (generation!=admissionGeneration.get()) throw new IOException("HOME preparation reply lost its owner");
                    byte[] retained=receipt==null ? null : receipt.encode();
                    var output=ByteBuffer.allocate(40+(retained==null ? 0 : retained.length));
                    ScopedTransactionRpc.putBlob(output,recovery.digest()); ScopedTransactionRpc.putBlob(output,retained); reply=output.array();
                } else if (packet.kind()==ScopedTransactionRpc.HOME_RECOVERY_CANCEL) {
                    cancelHomeRecovery(recovery); reply=recovery.digest();
                } else {
                    var receipt=store.homeRecoveryResult(recovery); checkHomeOwner();
                    if (generation!=admissionGeneration.get()) throw new IOException("HOME recovery receipt query lost its owner");
                    reply=valueBlob(receipt==null ? null : receipt.encode());
                }
            }
            case ScopedTransactionRpc.SCHEMA_AUTHORITY -> {
                if (!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE || schemaColumn==null) throw new IOException("Schema authority requires configured CATALOGUE and fresh barrier");
                return Message.valueOf(ByteString.copyFrom(RatisSchemaAuthority.reply(readCatalogueAuthorityCut(RatisSchemaAuthority.request(encoded)).view())));
            }
            case ScopedTransactionRpc.SCHEMA_FENCING_PREPARE, ScopedTransactionRpc.SCHEMA_FENCING_CANCEL -> {
                if (!freshBarrier || role!=RatisNodePlan.Role.CATALOGUE) throw new IOException("FENCING requires fresh CATALOGUE authority");
                var command=RatisSchemaFencingRpc.request(encoded);
                if (packet.kind()==ScopedTransactionRpc.SCHEMA_FENCING_PREPARE) prepareSchemaFencing(command); else cancelSchemaFencing(command);
                return Message.valueOf(ByteString.copyFrom(RatisSchemaFencingRpc.preparedReply(packet.kind(),command)));
            }
            case ScopedTransactionRpc.SCHEMA_FENCE_PREPARE, ScopedTransactionRpc.SCHEMA_FENCE_CANCEL, ScopedTransactionRpc.SCHEMA_FENCE_STATE -> {
                if (!freshBarrier) throw new IOException("Schema fence query requires a fresh linearizable barrier");
                var record=RatisSchemaFenceRecord.decode(packet.body()); requireSchemaRecord(record); checkTransactionOwner();
                long generation=admissionGeneration.get();
                if (packet.kind()==ScopedTransactionRpc.SCHEMA_FENCE_PREPARE) prepareSchemaFence(record);
                else if (packet.kind()==ScopedTransactionRpc.SCHEMA_FENCE_CANCEL) cancelSchemaFence(record);
                checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Schema query lost its owner");
                if (packet.kind()!=ScopedTransactionRpc.SCHEMA_FENCE_STATE) return Message.valueOf(ByteString.copyFrom(RatisSchemaFenceRpc.preparedReply(packet.kind(),record)));
                var proof=schemaProof(record); var state=store.schemaFenceReady();
                boolean historical=proof!=null && store.lookupSchemaInstallation(record.header())!=null;
                checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Schema readiness lost its owner");
                return Message.valueOf(ByteString.copyFrom(RatisSchemaFenceRpc.reply(packet.kind(),new RatisSchemaFenceRpc.State(record,proof,historical ? 0 : state.snapshotPins(),historical ? 0 : state.preparedParticipants(),historical ? 0 : state.snapshotGates(),historical || state.ready()))));
            }
            case ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE, ScopedTransactionRpc.SCHEMA_INSTALL_CANCEL, ScopedTransactionRpc.SCHEMA_INSTALL_STATE -> {
                if (!freshBarrier) throw new IOException("Schema installation requires fresh linearizable barrier");
                var record=RatisSchemaInstallRecord.decode(packet.body()); requireSchemaGroup(record.groupId()); checkTransactionOwner(); long generation=admissionGeneration.get();
                if (packet.kind()==ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE) prepareSchemaInstall(record);
                else if (packet.kind()==ScopedTransactionRpc.SCHEMA_INSTALL_CANCEL) cancelSchemaLifecycle(record);
                checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Installation query lost its owner");
                if (packet.kind()!=ScopedTransactionRpc.SCHEMA_INSTALL_STATE) return Message.valueOf(ByteString.copyFrom(RatisSchemaInstallRpc.preparedReply(packet.kind(),record)));
                var proof=schemaInstallProof(record);
                return Message.valueOf(ByteString.copyFrom(RatisSchemaInstallRpc.reply(packet.kind(),new RatisSchemaInstallRpc.State(record,proof,proof!=null))));
            }
            case ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE, ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL, ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE -> {
                if (!freshBarrier) throw new IOException("Schema activation requires fresh linearizable barrier");
                var request=RatisSchemaActivateRpc.Request.decode(packet.body()); requireSchemaGroup(request.groupId()); checkTransactionOwner(); long generation=admissionGeneration.get();
                RatisSchemaActivateRecord record=null;
                if (packet.kind()==ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE) record=prepareSchemaActivate(request);
                else if (packet.kind()==ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL) cancelSchemaLifecycle(request);
                checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Activation query lost its owner");
                if (packet.kind()!=ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE) return Message.valueOf(ByteString.copyFrom(RatisSchemaActivateRpc.preparedReply(packet.kind(),request,record)));
                var proof=schemaActivationProof(request);
                return Message.valueOf(ByteString.copyFrom(RatisSchemaActivateRpc.reply(packet.kind(),new RatisSchemaActivateRpc.State(request,proof,proof!=null))));
            }
            case ScopedTransactionRpc.POINT -> {
                var column=ScopedTransactionRpc.uuid(input); var key=ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES-16,false); end(input);
                reply=valueBlob(readablePoint(column,key));
            }
            case ScopedTransactionRpc.STATE -> {
                end(input);
                if(store.ownership().state()==it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.State.RETIRED || store.topologyBirthPending())reply=store.retiredOutcomeState().encode();
                else if(role==RatisNodePlan.Role.DATA && !currentDataVoter)reply=store.metadataOutcomeState().encode();
                else try(var pin=snapshotPin()){reply=pin.stateView().encode();}
            }
            case ScopedTransactionRpc.OPEN -> {
                if (!freshBarrier) throw new IOException("Transaction open requires a fresh linearizable read barrier");
                var id=ScopedTransactionRpc.uuid(input); long ttl=input.getLong();
                var handle=scopedTransactions.open(id,ScopedTransactionRpc.decodeScope(input),java.time.Duration.ofMillis(ttl));
                var output=ByteBuffer.allocate(16+ScopedTransactionRpc.HANDLE_BYTES); ScopedTransactionRpc.uuid(output,id); ScopedTransactionRpc.writeHandle(output,handle); reply=output.array();
            }
            case ScopedTransactionRpc.GET -> {
                var handle=ScopedTransactionRpc.readHandle(input); var column=ScopedTransactionRpc.uuid(input);
                var key=ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES-16,false); end(input);
                reply=valueBlob(scopedTransactions.get(handle,column,key));
            }
            case ScopedTransactionRpc.PAGE -> {
                var handle=ScopedTransactionRpc.readHandle(input); var column=ScopedTransactionRpc.uuid(input);
                var lower=ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true); var upper=ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true);
                int direction=input.get()&255; if (direction>1) throw new IllegalArgumentException("Invalid transaction page direction");
                var resume=ScopedTransactionRpc.blob(input,it.cavallium.rockserver.core.common.distributed.PhysicalRangePage.MAX_BYTES,true);
                int items=input.getInt(), bytes=input.getInt(); end(input);
                reply=ScopedTransactionRpc.pageBytes(scopedTransactions.page(handle,column,lower,upper,direction==1,resume,items,bytes));
            }
            case ScopedTransactionRpc.PARTICIPANT_AUTHORIZE -> {
                if (!freshBarrier) throw new IOException("Participant authority requires a fresh linearizable barrier"); requireDataRole(); long generation=admissionGeneration.get(); checkTransactionOwner();
                var handle=ScopedTransactionRpc.readHandle(input); var control=new OperationId(ScopedTransactionRpc.uuid(input),input.getLong(),input.getLong()); var transaction=ScopedTransactionRpc.uuid(input);
                var accepted=DistributedCommandCodec.decode(ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false)); end(input);
                try { byte[] canonical=scopedTransactions.authorizeParticipant(handle,accepted,control,transaction); reply=ByteBuffer.allocate(1+canonical.length).put((byte)0).put(canonical).array(); }
                catch (RatisPreparationUnavailableException unavailable) { reply=new byte[]{1}; }
                if (generation!=admissionGeneration.get()) throw new IOException("Participant authorization lost leader generation"); checkTransactionOwner();
            }
            case ScopedTransactionRpc.STAGE -> {
                var handle=ScopedTransactionRpc.readHandle(input);var prefix=ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false);end(input);
                var command=prefix.length==0?null:DistributedCommandCodec.decode(prefix);
                byte[] result=(command==null?scopedTransactions.reset(handle):scopedTransactions.stage(handle,command)).encode();
                reply=ByteBuffer.allocate(32+result.length).put(command==null?OperatorUpload.sha256(prefix):command.payloadDigest()).put(result).array();
            }
            case ScopedTransactionRpc.PLAN -> {
                var handle=ScopedTransactionRpc.readHandle(input); var command=DistributedCommandCodec.decode(ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false)); end(input);
                reply=DistributedCommandCodec.encode(scopedTransactions.plan(handle,command));
            }
            case ScopedTransactionRpc.PARTICIPANT_RETIRE_PREPARE, ScopedTransactionRpc.PARTICIPANT_RETIRE_CANCEL -> {
                if (!freshBarrier) throw new IOException("Participant retirement requires a fresh linearizable barrier"); requireDataRole(); checkTransactionOwner();
                long generation=admissionGeneration.get(); var retirement=RatisParticipantRecord.decodeRetirement(packet.body());
                boolean retired;
                if (packet.kind()==ScopedTransactionRpc.PARTICIPANT_RETIRE_CANCEL) { cancelRetirement(retirement); retired=false; }
                else retired=prepareRetirement(retirement);
                checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Retirement query lost its owner");
                reply=ByteBuffer.allocate(33).put(RatisParticipantRecord.digest(retirement)).put((byte)(retired ? 1 : 0)).array();
            }
            case ScopedTransactionRpc.PARTICIPANT_FENCE -> {
                if (!freshBarrier) throw new IOException("Participant fence requires a fresh linearizable barrier"); requireDataRole(); checkTransactionOwner();
                var identity=ParticipantIdentity.decode(packet.body()); var proof=store.participantFence(identity);
                byte[] bytes=proof==null ? null : proof.encode(); var output=ByteBuffer.allocate(36+(bytes==null ? 0 : bytes.length));
                output.put(identity.digest()); ScopedTransactionRpc.putBlob(output,bytes); reply=output.array();
            }
            case ScopedTransactionRpc.PARTICIPANT_STATE -> {
                if (!freshBarrier) throw new IOException("Prepared state requires a fresh linearizable barrier");
                var id=ScopedTransactionRpc.uuid(input); end(input); checkTransactionOwner(); var record=store.preparedParticipant(id);
                byte[] bytes=record==null ? null : record.encode(); var output=ByteBuffer.allocate(4+(bytes==null ? 0 : bytes.length)); ScopedTransactionRpc.putBlob(output,bytes); reply=output.array();
            }
            case ScopedTransactionRpc.PARTICIPANT_OUTCOME -> {
                if (!freshBarrier) throw new IOException("Participant outcome requires a fresh linearizable barrier");
                var id=ScopedTransactionRpc.uuid(input); end(input); checkTransactionOwner(); var outcome=store.finalizedParticipant(id);
                byte[] bytes=outcome==null ? null : outcome.encode(); var output=ByteBuffer.allocate(4+(bytes==null ? 0 : bytes.length)); ScopedTransactionRpc.putBlob(output,bytes); reply=output.array();
            }
            case ScopedTransactionRpc.SNAPSHOT_FENCE -> {
                if (!freshBarrier) throw new IOException("Snapshot retirement proof requires a fresh linearizable barrier"); requireDataRole(); checkTransactionOwner();
                var fence=SnapshotFence.decode(packet.body()); var proof=store.snapshotFence(fence);
                if (proof!=null) synchronized (admissions) { retireSnapshotOwners(SnapshotGateResult.retired(proof)); }
                byte[] bytes=proof==null ? null : proof.encode(); var output=ByteBuffer.allocate(52+(bytes==null ? 0 : bytes.length));
                ScopedTransactionRpc.snapshotBinding(output,ScopedTransactionRpc.SnapshotBinding.of(fence)); ScopedTransactionRpc.putBlob(output,bytes); reply=output.array();
            }
            case ScopedTransactionRpc.SNAPSHOT_RETIRE_PREPARE, ScopedTransactionRpc.SNAPSHOT_RETIRE_CANCEL -> {
                if (!freshBarrier) throw new IOException("Snapshot retirement requires a fresh linearizable barrier"); requireDataRole(); checkTransactionOwner(); long generation=admissionGeneration.get(); var retirement=RatisSnapshotRetirementRecord.decode(packet.body());
                boolean retired; if (packet.kind()==ScopedTransactionRpc.SNAPSHOT_RETIRE_CANCEL) { cancelSnapshotRetirement(retirement); retired=false; } else retired=prepareSnapshotRetirement(retirement);
                checkTransactionOwner(); if (generation!=admissionGeneration.get()) throw new IOException("Snapshot retirement query lost its owner"); reply=ByteBuffer.allocate(33).put(retirement.digest()).put((byte)(retired ? 1 : 0)).array();
            }
            case ScopedTransactionRpc.SNAPSHOT_GATE_STATE -> {
                if (!freshBarrier) throw new IOException("Snapshot gate state requires a fresh linearizable barrier");
                var id=ScopedTransactionRpc.uuid(input); end(input); checkTransactionOwner(); var record=store.snapshotGate(id);
                byte[] bytes=record==null ? null : record.encode(); var output=ByteBuffer.allocate(4+(bytes==null ? 0 : bytes.length)); ScopedTransactionRpc.putBlob(output,bytes); reply=output.array();
            }
            case ScopedTransactionRpc.SNAPSHOT_OWNER -> {
                if (!freshBarrier) throw new IOException("Snapshot owner discovery requires a fresh linearizable barrier");
                end(input); checkTransactionOwner(); var server=getServer().getNow(null);
                if (server==null) throw new IOException("Snapshot peer is not initialized");
                var view=store.metadataOutcomeState();reply=new RatisSnapshotOwner(view.groupId(),view.rangeEpoch(),view.schemaDigest(),snapshotCaptureOwner(),server.getId().toString(),snapshotCohorts==null ? null : snapshotCohorts.capabilities(view.groupId())).encode();
            }
            case ScopedTransactionRpc.SNAPSHOT_READY -> {
                if (!freshBarrier) throw new IOException("Snapshot readiness requires a fresh linearizable barrier");
                byte[] definition=new byte[input.remaining()]; input.get(definition); var fence=SnapshotFence.decode(definition); checkTransactionOwner();
                var outcome=store.snapshotGateResult(fence); var record=outcome==null ? null : outcome.gate(); boolean ready=record!=null && record.active() && snapshotGateReady(fence);
                var binding=ScopedTransactionRpc.SnapshotBinding.of(fence);
                reply=outcome!=null && outcome.retired() ? RatisSnapshotReadiness.retired(outcome.retirement()).encode() : new RatisSnapshotReadiness(binding,store.ownership().owner().groupId(),record==null ? null : record.installed(),ready).encode();
            }
            case ScopedTransactionRpc.SNAPSHOT_BLOCKERS -> {
                if (!freshBarrier) throw new IOException("Snapshot blockers require a fresh linearizable barrier");
                var request=RatisSnapshotBlockers.Request.decode(packet.body()); checkTransactionOwner();
                var page=store.snapshotBlockers(request.fence(),request.after(),request.limit()); checkTransactionOwner();
                var records=page.records().stream().map(p -> new RatisSnapshotBlockers.Reference(p.preparation().transactionId(),p.preparation().digest(),p.position())).toList();
                reply=new RatisSnapshotBlockers(ScopedTransactionRpc.SnapshotBinding.of(request.fence()),page.gate().participantGroup(),page.gate().installed(),page.position(),records,page.more()).encode();
            }
            case ScopedTransactionRpc.SNAPSHOT_CAPTURE -> {
                var capture=RatisSnapshotCaptureRequest.decode(packet.body()); SnapshotFenceStatus.Capture token=null;
                ScopedTransactionRpc.Handle handle=null;
                if (capture.owner().equals(transactionIncarnation)) {
                    if (snapshotHomes==null) throw new IOException("Snapshot home reader is not configured");
                    snapshotHomes.requireDatabaseBinding(capture.fence().clusterId(),capture.fence().databaseId());
                    try {
                        var captured=capturedSnapshots.capture(snapshotHomes,capture.fence(),capture.owner(),capture.snapshotId(),capture.ttl(),this::snapshotRetentionFloor,capture.mode());
                        token=captured.token(); handle=captured.handle();
                    }
                    catch (RatisSnapshotLostException lost) { /* Explicit loss cannot create a replacement pin. */ }
                }
                reply=capture.reply(token,handle);
            }
            case ScopedTransactionRpc.SNAPSHOT_GET, ScopedTransactionRpc.SNAPSHOT_PAGE, ScopedTransactionRpc.SNAPSHOT_RELEASE -> {
                var binding=ScopedTransactionRpc.snapshotBinding(input); var token=ScopedTransactionRpc.snapshotToken(input); byte[] payload;
                try {
                    capturedSnapshots.requireBinding(token,binding);
                    if (packet.kind()==ScopedTransactionRpc.SNAPSHOT_GET) {
                        var column=ScopedTransactionRpc.uuid(input); var key=ScopedTransactionRpc.blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false); end(input);
                        payload=valueBlob(snapshotPinGet(token,column,key));
                    } else if (packet.kind()==ScopedTransactionRpc.SNAPSHOT_PAGE) {
                        var column=ScopedTransactionRpc.uuid(input); var lower=ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true); var upper=ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true);
                        int reverse=input.get() & 255; if (reverse>1) throw new IllegalArgumentException("Invalid snapshot direction");
                        var resume=ScopedTransactionRpc.blob(input,it.cavallium.rockserver.core.common.distributed.PhysicalRangePage.MAX_BYTES,true); int items=input.getInt(),bytes=input.getInt(); end(input);
                        payload=ScopedTransactionRpc.pageBytes(snapshotPinPage(token,column,lower,upper,reverse==1,resume,items,bytes));
                    } else { end(input); releaseSnapshotPin(token); payload=new byte[0]; }
                } catch (RatisSnapshotLostException lost) { payload=null; }
                reply=ScopedTransactionRpc.snapshotReply(binding,token,payload);
            }
            case ScopedTransactionRpc.CLOSE -> { var handle=ScopedTransactionRpc.readHandle(input); end(input); scopedTransactions.release(handle); reply=new byte[0]; }
            default -> throw new IllegalArgumentException("Unsupported framed query");
        }
        return Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(packet.kind(),reply)));
    }
    private static void end(ByteBuffer input) { if (input.hasRemaining()) throw new IllegalArgumentException("Trailing query bytes"); }
    private static byte[] valueBlob(byte[] value) {
        if (value!=null && value.length>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Point value exceeds reply budget");
        var output=ByteBuffer.allocate(4+(value==null ? 0 : value.length)); ScopedTransactionRpc.putBlob(output,value); return output.array();
    }

    private void requireHealthy() {
        if (storageFailure != null) throw new IllegalStateException("Range requires repair", storageFailure);
        if (getLifeCycleState() != LifeCycle.State.RUNNING) throw new IllegalStateException("Range is paused or starting");
    }
}
