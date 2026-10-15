package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.BucketMergeOperand;
import it.cavallium.rockserver.core.common.distributed.DatabaseDomainRoster;
import it.cavallium.rockserver.core.common.distributed.CatalogueHistory;
import it.cavallium.rockserver.core.common.distributed.RangeDirectoryCodec;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.NativeSchemaFence;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyFreeze;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyRecoveryFence;
import it.cavallium.rockserver.core.common.distributed.NativeMetadataRecoveryRegistration;
import it.cavallium.rockserver.core.common.distributed.NativeCurrentRangeRecovery;
import it.cavallium.rockserver.core.common.distributed.NativeSourceOutcomeCredit;
import it.cavallium.rockserver.core.common.distributed.NativeSourceOutcomePublication;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyBirth;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyActivation;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyImageOrigin;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerRegistration;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerImagePermission;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyLearnerSchema;
import it.cavallium.rockserver.core.common.distributed.NativeTopologyRelocationCompletion;
import it.cavallium.rockserver.core.common.distributed.NativeTopologySeed;
import it.cavallium.rockserver.core.common.distributed.TopologyChange;
import it.cavallium.rockserver.core.common.distributed.NativeSchemaInstallation;
import it.cavallium.rockserver.core.common.distributed.NativeSchemaInstallProof;
import it.cavallium.rockserver.core.common.distributed.NativeSchemaActivationProof;
import it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest;
import it.cavallium.rockserver.core.common.distributed.SchemaChange;
import it.cavallium.rockserver.core.common.distributed.HomeRecovery;
import it.cavallium.rockserver.core.common.distributed.HomeRecoveryReceipt;
import it.cavallium.rockserver.core.common.distributed.HomeRecoveryResult;
import it.cavallium.rockserver.core.common.distributed.HomeIdentity;
import it.cavallium.rockserver.core.common.distributed.HomeFenceProof;
import it.cavallium.rockserver.core.common.distributed.HomeRetirement;
import it.cavallium.rockserver.core.common.distributed.PreparedParticipant;
import it.cavallium.rockserver.core.common.distributed.ParticipantFinalization;
import it.cavallium.rockserver.core.common.distributed.ParticipantVote;
import it.cavallium.rockserver.core.common.distributed.ParticipantIdentity;
import it.cavallium.rockserver.core.common.distributed.ParticipantFenceProof;
import it.cavallium.rockserver.core.common.distributed.ParticipantRetirement;
import it.cavallium.rockserver.core.common.distributed.ParticipantFinalizationResult;
import it.cavallium.rockserver.core.common.distributed.AbortedParticipant;
import it.cavallium.rockserver.core.common.distributed.RejectedParticipant;
import it.cavallium.rockserver.core.common.distributed.SnapshotFence;
import it.cavallium.rockserver.core.common.distributed.TransactionAcceptance;
import it.cavallium.rockserver.core.common.distributed.SnapshotHomeIdentity;
import it.cavallium.rockserver.core.common.distributed.SnapshotHomeFenceProof;
import it.cavallium.rockserver.core.common.distributed.SnapshotHomeRetirement;
import it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus;
import it.cavallium.rockserver.core.common.distributed.SnapshotGate;
import it.cavallium.rockserver.core.common.distributed.SnapshotGateResult;
import it.cavallium.rockserver.core.common.distributed.SnapshotFenceProof;
import it.cavallium.rockserver.core.common.distributed.SnapshotGateRetirement;
import it.cavallium.rockserver.core.common.distributed.SnapshotRetentionExpiredException;
import it.cavallium.rockserver.core.common.distributed.CommittedParticipant;
import it.cavallium.rockserver.core.common.distributed.ParticipantOutcome;
import it.cavallium.rockserver.core.common.distributed.TransactionStatus;
import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import it.cavallium.rockserver.core.common.distributed.DistributedBucket;
import it.cavallium.rockserver.core.common.distributed.KeyPredicate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import it.cavallium.rockserver.core.common.distributed.RangeMutationHistory;
import it.cavallium.rockserver.core.common.distributed.RangeSessionControl;
import it.cavallium.rockserver.core.common.distributed.SessionAuthority;
import it.cavallium.rockserver.core.common.distributed.SessionEpochLedger;
import it.cavallium.rockserver.core.common.distributed.RangeStateView;
import it.cavallium.rockserver.core.common.distributed.ReceiptUsage;
import it.cavallium.rockserver.core.common.distributed.ReceiptRetentionEstimate;
import it.cavallium.rockserver.core.common.distributed.DistributedCommand;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import it.cavallium.rockserver.core.common.distributed.OperationId;
import it.cavallium.rockserver.core.common.distributed.Mutation;
import it.cavallium.rockserver.core.common.distributed.CommandResult;
import it.cavallium.rockserver.core.common.distributed.TransactionScope;
import it.cavallium.rockserver.core.common.distributed.PhysicalRangePage;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.TimeUnit;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.TransactionDB;

/**
 * Native atomic apply seam for one range in a shared storage pool.
 * The pool owns the database and CF handles and must outlive this store and its
 * snapshot pins. Only committed, canonical plans may enter this seam; admission,
 * consensus, ownership and operator qualification belong to the range adapter.
 */
public final class ReplicatedRangeStore {
    private NativeRangeCdc cdc;
    /** Trusted DATA-role setup. Existing data without CDC history starts at a gap floor, never a false empty tail. */
    public void enableCdc(UUID rangeId) throws RocksDBException {
        Objects.requireNonNull(rangeId); enterNative(); applyLane.lock();
        try {
            var lineage=new it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage(rangeId,groupId,rangeEpoch);
            if(cdc!=null) { if(!cdc.lineage().equals(lineage)) throw new IllegalStateException("CDC lineage changed"); return; }
            var candidate=new NativeRangeCdc(databaseOwner,state,lineage);
            if(pendingInstallation==null) candidate.recover(applied,cdcAuthorityTime());
            cdc=candidate;
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public record CdcPage(it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage lineage,ApplyPosition cut,
            it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Cursor floor,
            it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Cursor next,
            boolean fromVerified,List<it.cavallium.rockserver.core.common.distributed.DistributedCdcRecord> records) {
        public CdcPage { records=List.copyOf(records); }
    }
    public static final class CdcGap extends it.cavallium.rockserver.core.common.CdcGapDetectedException {
        private final CdcPage page;
        CdcGap(CdcPage page) { super("Distributed CDC retention gap"); this.page=page; }
        public CdcPage page() { return page; }
    }
    public static final class CdcTooLarge extends it.cavallium.rockserver.core.common.RocksDBException {
        private final CdcPage page; private final int requiredPayloadBytes;
        CdcTooLarge(CdcPage page,int requiredPayloadBytes) {
            super(RocksDBErrorType.CDC_RESPONSE_TOO_LARGE,"CDC response budget cannot fit next mutation envelope"); this.page=page; this.requiredPayloadBytes=requiredPayloadBytes;
        }
        public CdcPage page() { return page; }
        public int requiredPayloadBytes() { return requiredPayloadBytes; }
    }
    /** One completed native cut. Fresh-read/owner authority is supplied by the range adapter. */
    public CdcPage pollCdc(it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage lineage,
            it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Cursor from,int maxRecords,int maxBytes) throws RocksDBException {
        Objects.requireNonNull(lineage); Objects.requireNonNull(from);
        if(maxRecords<1 || maxRecords>256 || maxBytes<1 || maxBytes>8*1024*1024) throw new IllegalArgumentException("Invalid CDC poll budget");
        enterNative(); applyLane.lock();
        try {
            requireActive();
            if(cdc==null || !cdc.lineage().equals(lineage)) throw new IllegalArgumentException("CDC lineage is not configured");
            return cdc.poll(from,maxRecords,maxBytes,applied);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Retired/sealed producer metadata only; never acquires a USER snapshot or reads child rows. */
    public CdcPage pollRetiredCdc(it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage lineage,
            it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Cursor from,int maxRecords,int maxBytes) throws RocksDBException {
        Objects.requireNonNull(lineage);Objects.requireNonNull(from);if(maxRecords<1 || maxRecords>256 || maxBytes<1 || maxBytes>8*1024*1024)throw new IllegalArgumentException("Invalid CDC poll budget");
        enterNative();applyLane.lock();try {
            requireTopologyMetadataOwner();if(cdc==null || !cdc.lineage().equals(lineage))throw new IllegalArgumentException("CDC retired lineage is not configured");return cdc.poll(from,maxRecords,maxBytes,applied);
        } finally { applyLane.unlock();leaveNative(); }
    }
    private void requireTopologyMetadataOwner() throws RocksDBException {
        requireTopologyMetadataOwner(true);
    }
    private void requireTopologyMetadataOwner(boolean ready) throws RocksDBException {
        if(ready)requireMetadataRecoveryReady();
        if(metadataRecoveryRegistration!=null && !metadataImageInstalledUnderLane())throw new IllegalStateException("Source metadata has no completed registered image");
        var ownershipState=ownership.get(owner).state();var freeze=activeTopologyFreeze;
        if(closing || pendingInstallation!=null || importPending() || freeze==null || applied==null || applied.term()<=0 || applied.index()<0
                || ownershipState!=RangeOwnership.State.ACTIVE && ownershipState!=RangeOwnership.State.FENCED && ownershipState!=RangeOwnership.State.RETIRED
                || ownershipState==RangeOwnership.State.RETIRED && !topologyRetired())throw new IllegalStateException("Topology metadata lacks a completed sealed native owner");
        var original=db.get(state,topologyFreezeKey());if(original==null || !freeze.equals(NativeTopologyFreeze.decode(original)) || !groupId.equals(freeze.groupId()) || !Arrays.equals(freeze.header().schemaDigest(),schemaDigest))throw new IllegalStateException("Topology metadata original FREEZE changed");freeze.requireCut(applied);
        if(topologyRetired() && !Arrays.equals(topologyRetirement,db.get(state,topologyOwnershipKey(0x62))))throw new IllegalStateException("Topology metadata retirement proof changed");
        if(metadataRecoveryRegistration!=null && (!metadataRecoveryRegistration.freeze().equals(freeze) || !Arrays.equals(metadataRecoveryRegistration.digest(),db.get(state,topologyOwnershipKey(0x6f)))))throw new IllegalStateException("Source metadata registration changed");
        byte[] marker=db.get(state,markerKey);if(marker==null || !applied.equals(decodePosition(marker)))throw new IllegalStateException("Topology metadata completed marker changed");
        var cfg=configurationUnderAdmission();if(cfg==null || cfg.term()>applied.term() || cfg.index()>applied.index() || cfg.index()==applied.index() && cfg.term()!=applied.term())throw new IllegalStateException("Topology metadata native configuration exceeds completed prefix");
        var authority=sessionAuthority();if(authority!=null && (!authority.clusterId().equals(freeze.header().clusterId()) || !authority.databaseId().equals(freeze.header().databaseId())))throw new IllegalStateException("Topology metadata session authority namespace changed");
    }
    public record TopologyOperationOutcome(ApplyPosition cut,NativeTopologyFreeze freeze,Configuration configuration,NativeSourceOutcomeCredit credit,byte[] result) {
        public TopologyOperationOutcome { Objects.requireNonNull(cut);Objects.requireNonNull(freeze);Objects.requireNonNull(configuration);result=result==null ? null : result.clone(); }
        @Override public byte[] result() { return result==null ? null : result.clone(); }
    }
    /** Definitive original-parent outcome at a sealed prefix; null never authorizes a child retry. */
    public TopologyOperationOutcome readTopologyOperationOutcome(UUID cluster,UUID database,UUID domain,
            it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage lineage,OperationId operation,byte[] originalReceiptDigest) throws RocksDBException {
        Objects.requireNonNull(cluster);Objects.requireNonNull(database);Objects.requireNonNull(domain);Objects.requireNonNull(lineage);Objects.requireNonNull(operation);
        Objects.requireNonNull(originalReceiptDigest);if(originalReceiptDigest.length!=32)throw new IllegalArgumentException("Invalid original receipt digest");byte[] digest=originalReceiptDigest.clone();
        enterNative();applyLane.lock();
        try {
            if(topologyBirthPending())throw new IllegalStateException("Pending topology birth has no sealed original-parent outcome");
            requireTopologyMetadataOwner();var freeze=activeTopologyFreeze;var header=freeze.header();var parent=header.participant(TopologyChange.Stage.FREEZE,groupId);
            if(!cluster.equals(header.clusterId()) || !database.equals(header.databaseId()) || !domain.equals(header.domainId()) || !lineage.rangeId().equals(parent.rangeId()) || !lineage.groupId().equals(groupId) || lineage.rangeEpoch()!=rangeEpoch || parent.rangeEpoch()!=rangeEpoch)throw new IllegalArgumentException("Topology operation outcome namespace/parent lineage differs");
            var credit=sourceOutcomeCredits.get(operation);if(credit!=null && !Arrays.equals(credit.request().original().receiptDigest(),digest))throw new IllegalArgumentException("Source original credit digest changed");
            return new TopologyOperationOutcome(applied,freeze,configurationUnderAdmission(),credit,operationResultUnderAdmission(operation,digest));
        } finally { applyLane.unlock();leaveNative(); }
    }
    private volatile Map<OperationId,NativeSourceOutcomeCredit> sourceOutcomeCredits=Map.of();
    private volatile Map<OperationId,NativeSourceOutcomePublication> sourceOutcomePublications=Map.of();
    private long sourceOutcomeCountLimit,sourceOutcomeByteLimit;
    /** Trusted adapter bootstrap; ordinary standalone admission is unchanged without promises. */
    public void configureSourceOutcomeCapacity(long count,long bytes) {
        if(count<1 || bytes<88)throw new IllegalArgumentException("Invalid source receipt capacity");enterNative();applyLane.lock();
        try { if(sourceOutcomeCountLimit!=0 && (sourceOutcomeCountLimit!=count || sourceOutcomeByteLimit!=bytes))throw new IllegalStateException("Source receipt capacity already configured");ensureReceiptUsage();var effective=readUsage(usageKey).plus(sourceOutcomePromises(null));if(effective.count()>count || effective.bytes()>bytes)throw new IllegalStateException("Retained receipts/promises exceed proposed source capacity");sourceOutcomeCountLimit=count;sourceOutcomeByteLimit=bytes; }
        catch(RocksDBException failure) { throw new IllegalStateException("Cannot qualify source receipt capacity",failure); }
        finally { applyLane.unlock();leaveNative(); }
    }
    private byte[] sourceOutcomeKey(int tag,OperationId operation) { return ByteBuffer.allocate(49).put(markerKey).put((byte)tag).put(uuidBytes(operation.sessionId())).putLong(operation.epoch()).putLong(operation.sequence()).array(); }
    private Usage sourceOutcomePromises(OperationId consuming) {
        Usage promises=new Usage(0,0);for(var entry:sourceOutcomeCredits.entrySet())if(!entry.getKey().equals(consuming))promises=promises.plus(new Usage(1,entry.getValue().request().reservedBytes()));return promises;
    }
    private void checkSourceOutcomeUsage(Usage total,OperationId consuming) {
        if(sourceOutcomeCredits.isEmpty())return;
        if(sourceOutcomeCountLimit==0)throw new IllegalStateException("Source receipt promises require configured capacity");var effective=total.plus(sourceOutcomePromises(consuming));if(effective.count()>sourceOutcomeCountLimit || effective.bytes()>sourceOutcomeByteLimit)throw new IllegalStateException("Ordinary receipts would consume promised source outcome capacity");
    }
    private long sourceOutcomeMetadataBytes() { long bytes=0;for(var credit:sourceOutcomeCredits.values())bytes+=49L+credit.encode().length+49+NativeSourceOutcomePublication.BYTES;return bytes+sourceOutcomePublications.size()*(49L+NativeSourceOutcomePublication.BYTES); }
    private void requireSourceCreditOriginal(DistributedCommand command) {
        var credit=sourceOutcomeCredits.get(command.operationId());
        if(credit!=null && !Arrays.equals(credit.request().original().payloadDigest(),command.payloadDigest()))throw new IllegalArgumentException("Operation identity is reserved for the exact source original payload");
    }
    private boolean sourceOperationFenced(OperationId operation) throws RocksDBException { return operation.epoch()<minimumSessionEpoch(null) || operation.sequence()<=acknowledgedThrough(operation,null); }
    private void requireSourceOutcomeRequest(NativeSourceOutcomeCredit.Request request,boolean ready) throws RocksDBException {
        requireTopologyMetadataOwner(ready);if(!activeTopologyFreeze.equals(request.freeze()) || !request.original().groupId().equals(groupId) || request.original().rangeEpoch()!=rangeEpoch)throw new IllegalArgumentException("Source outcome differs from sealed native owner");
        if(!request.targetDirectory().columns().equals(logicalColumns()) || (logicalSchema!=null || retiredRoutingPrefix>=0) && request.targetDirectory().routingPrefixBytes()!=(logicalSchema!=null ? logicalSchema.routingPrefixBytes() : retiredRoutingPrefix))throw new IllegalArgumentException("Source recovery target projection changed");
        request.freeze().requireCut(request.origin().absenceCut());if(request.origin().absenceCut().index()>applied.index() || request.origin().absenceCut().term()>applied.term())throw new IllegalArgumentException("Source credit absence cut exceeds native prefix");
    }
    public NativeSourceOutcomeCredit sourceOutcomeCredit(OperationId operation,byte[] originalReceiptDigest) throws RocksDBException {
        Objects.requireNonNull(operation);Objects.requireNonNull(originalReceiptDigest);if(originalReceiptDigest.length!=32)throw new IllegalArgumentException("Invalid source original receipt digest");
        enterNative();applyLane.lock();try { requireTopologyMetadataOwner();var credit=sourceOutcomeCredits.get(operation);if(credit!=null && !Arrays.equals(credit.request().original().receiptDigest(),originalReceiptDigest))throw new IllegalArgumentException("Source original credit digest changed");return credit; } finally { applyLane.unlock();leaveNative(); }
    }
    public List<NativeSourceOutcomeCredit> sourceOutcomeCredits() { enterNative();applyLane.lock();try { requireMetadataRecoveryReady();return List.copyOf(sourceOutcomeCredits.values()); } finally { applyLane.unlock();leaveNative(); } }
    public record SourceOutcomeReplayState(NativeSourceOutcomeCredit credit,NativeSourceOutcomePublication publication,boolean fenced,byte[] result) {
        public SourceOutcomeReplayState {result=result==null?null:result.clone();}
        @Override public byte[] result(){return result==null?null:result.clone();}
    }
    /** Trusted committed-log replay only. This state cannot qualify a fresh proposal or authoritative ABSENT. */
    public SourceOutcomeReplayState readCommittedSourceOutcome(NativeSourceOutcomeCredit.Request request)throws RocksDBException {
        Objects.requireNonNull(request);enterNative();applyLane.lock();try {
            requireSourceOutcomeRequest(request,false);var id=request.original().operationId();var credit=sourceOutcomeCredits.get(id);var publication=sourceOutcomePublications.get(id);
            if(credit!=null && !credit.request().equals(request) || publication!=null && !Arrays.equals(publication.requestDigest(),request.digest()))throw new IllegalArgumentException("Committed source replay changed original input");
            boolean fenced=sourceOperationFenced(id);byte[] result=publication!=null && !fenced?operationResultUnderAdmission(id,request.original().receiptDigest()):null;
            if(publication!=null){publication.requireCut(applied);if(!fenced){if(result==null)throw new IllegalStateException("Source publication lost original retained result");publication.requireRequest(request,result);}}
            return new SourceOutcomeReplayState(credit,publication,fenced,result);
        }finally{applyLane.unlock();leaveNative();}
    }
    public record SourceOutcomeCreditEstimate(ReceiptUsage usage,boolean existing,boolean fenced,long additionalCount,long additionalBytes,long metadataBytes,long metadataUsageBytes) {}
    public SourceOutcomeCreditEstimate estimateSourceOutcomeCredit(NativeSourceOutcomeCredit.Request request) throws RocksDBException {
        return estimateSourceOutcomeCredit(request,true);
    }
    private SourceOutcomeCreditEstimate estimateSourceOutcomeCredit(NativeSourceOutcomeCredit.Request request,boolean ready) throws RocksDBException {
        Objects.requireNonNull(request);enterNative();applyLane.lock();try {
            requireSourceOutcomeRequest(request,ready);ensureReceiptUsage();var id=request.original().operationId();var total=readUsage(usageKey).plus(sourceOutcomePromises(null));var usage=new ReceiptUsage(applied,total.count(),total.bytes());
            var saved=sourceOutcomeCredits.get(id);if(saved!=null) { if(!saved.request().equals(request))throw new IllegalArgumentException("Source credit original input changed");return new SourceOutcomeCreditEstimate(usage,true,sourceOperationFenced(id),0,0,0,sourceOutcomeMetadataBytes()); }
            var published=sourceOutcomePublications.get(id);if(published!=null) { if(!Arrays.equals(published.requestDigest(),request.digest()))throw new IllegalArgumentException("Source publication original input changed");return new SourceOutcomeCreditEstimate(usage,true,sourceOperationFenced(id),0,0,0,sourceOutcomeMetadataBytes()); }
            if(sourceOperationFenced(id))return new SourceOutcomeCreditEstimate(usage,false,true,0,0,0,sourceOutcomeMetadataBytes());
            if(operationResultUnderAdmission(id,request.original().receiptDigest())!=null)throw new IllegalStateException("Source original operation already has an ordinary outcome");
            if(sessionAckEpochs==null)validateSessionMetadata(null);
            if(sessionAckCount+sourceSessionMetadataPromises(minimumSessionEpoch(null),null,request.original())>sessionAckLimit)throw new IllegalStateException("Source outcome ACK namespace promise capacity exhausted");
            long metadata=49L+24+request.encode().length+49+NativeSourceOutcomePublication.BYTES;
            if(sourceOutcomeCountLimit==0 || total.count()+1>sourceOutcomeCountLimit || total.bytes()+request.reservedBytes()>sourceOutcomeByteLimit || sourceOutcomeCredits.size()>=NativeSourceOutcomeCredit.MAX_CREDITS || sourceOutcomePublications.size()+sourceOutcomeCredits.size()>=NativeSourceOutcomeCredit.MAX_PUBLICATIONS || sourceOutcomeMetadataBytes()+metadata>NativeSourceOutcomeCredit.MAX_METADATA_BYTES)throw new IllegalStateException("Source outcome receipt/metadata capacity exhausted");
            return new SourceOutcomeCreditEstimate(usage,false,false,1,request.reservedBytes(),metadata,sourceOutcomeMetadataBytes());
        } finally { applyLane.unlock();leaveNative(); }
    }
    /** Trusted committed reservation after HOME RESERVED; fresh HOME/source authority is adapter-owned. */
    public NativeSourceOutcomeCredit applySourceOutcomeCredit(ApplyPosition position,NativeSourceOutcomeCredit.Request request) throws RocksDBException {
        enterNative();applyLane.lock();try {
            var estimate=estimateSourceOutcomeCredit(request,false);var id=request.original().operationId();var existing=sourceOutcomeCredits.get(id);if(existing!=null) { advanceCompletedMarker(position);return existing; }
            var publication=sourceOutcomePublications.get(id);if(publication!=null) { advanceCompletedMarker(position);return new NativeSourceOutcomeCredit(request,publication.creditPosition()); }
            if(estimate.fenced()) { advanceCompletedMarker(position);return null; }
            requireNext(position);var credit=new NativeSourceOutcomeCredit(request,position);var next=new HashMap<>(sourceOutcomeCredits);next.put(id,credit);
            try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(false)) { batch.put(state,sourceOutcomeKey(0x65,id),credit.encode());batch.put(state,markerKey,encodePosition(position));batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position));db.write(options,batch); }
            sourceOutcomeCredits=Map.copyOf(next);applied=position;return credit;
        } finally { applyLane.unlock();leaveNative(); }
    }
    public SourceOutcomePublicationResult sourceOutcomePublication(NativeSourceOutcomeCredit.Request request,byte[] result) throws RocksDBException {
        Objects.requireNonNull(request);Objects.requireNonNull(result);if(result.length>request.maximumResultBytes())throw new IllegalArgumentException("Source publication result exceeds promise");
        enterNative();applyLane.lock();try { requireSourceOutcomeRequest(request,true);var id=request.original().operationId();var saved=sourceOutcomePublications.get(id);if(saved!=null){saved.requireRequest(request,result);saved.requireCut(applied);return new SourceOutcomePublicationResult(sourceOperationFenced(id)?NativeSourceOutcomePublication.Outcome.FENCED:saved.outcome(),saved);}return sourceOperationFenced(id) && !sourceOutcomeCredits.containsKey(id)?new SourceOutcomePublicationResult(NativeSourceOutcomePublication.Outcome.FENCED,null):null; } finally { applyLane.unlock();leaveNative(); }
    }
    public record SourceOutcomePublicationResult(NativeSourceOutcomePublication.Outcome outcome,NativeSourceOutcomePublication publication) {}
    /** Trusted verified terminal HOME result; no USER access or original grant revalidation. */
    public SourceOutcomePublicationResult publishSourceOutcome(ApplyPosition position,NativeSourceOutcomeCredit.Request request,byte[] result) throws RocksDBException {
        Objects.requireNonNull(position);Objects.requireNonNull(request);Objects.requireNonNull(result);if(result.length>request.maximumResultBytes())throw new IllegalArgumentException("Source publication result exceeds promise");byte[] stable=result.clone();CommandResult.decode(stable);
        enterNative();applyLane.lock();try {
            requireSourceOutcomeRequest(request,false);ensureReceiptUsage();var id=request.original().operationId();boolean fenced=sourceOperationFenced(id);var saved=sourceOutcomePublications.get(id);
            if(saved!=null) { saved.requireRequest(request,stable);saved.requireCut(applied);advanceCompletedMarker(position);return new SourceOutcomePublicationResult(fenced?NativeSourceOutcomePublication.Outcome.FENCED:saved.outcome(),saved); }
            var credit=sourceOutcomeCredits.get(id);if(credit==null) { if(fenced) { advanceCompletedMarker(position);return new SourceOutcomePublicationResult(NativeSourceOutcomePublication.Outcome.FENCED,null); }throw new IllegalStateException("Source publication has no durable exact receipt credit"); }
            if(!credit.request().equals(request))throw new IllegalArgumentException("Source publication changed credited input");requireNext(position);
            byte[] old=operationResultUnderAdmission(id,request.original().receiptDigest());if(!fenced && old!=null && !Arrays.equals(old,stable))throw new IllegalArgumentException("Source original outcome already differs");
            var outcome=fenced?NativeSourceOutcomePublication.Outcome.FENCED:NativeSourceOutcomePublication.Outcome.RETAINED;var publication=new NativeSourceOutcomePublication(groupId,id,request.digest(),sourceOutcomeHash(stable),credit.position(),position,outcome);
            var next=new HashMap<>(sourceOutcomeCredits);next.remove(id);var published=new HashMap<>(sourceOutcomePublications);
            var sessionMetadata=!fenced && old==null ? sessionMetadataChange(request.original(),-1,request.original().acknowledgedThrough()) : null;
            try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(!fenced && old==null) {
                    var command=request.original();long minimum=minimumSessionEpoch(null),floor=acknowledgedThrough(id,null);long ack=command.acknowledgedThrough();
                    if(sessionMetadata!=null)batch.put(state,sessionAckCountKey(),ByteBuffer.allocate(8).putLong(sessionMetadata.count()).array());
                    if(ack>floor) { batch.put(state,sessionFloorKey(id),ByteBuffer.allocate(8).putLong(ack).array());batch.deleteRange(receipts,receiptKey(new OperationId(id.sessionId(),id.epoch(),0)),receiptKey(new OperationId(id.sessionId(),id.epoch(),ack+1))); }
                    for(var prior:List.copyOf(published.keySet()))if(prior.epoch()<minimum || prior.sessionId().equals(id.sessionId()) && prior.epoch()==id.epoch() && prior.sequence()<=ack) { batch.delete(state,sourceOutcomeKey(0x66,prior));published.remove(prior); }
                    byte[] receipt=encodeReceipt(command.receiptDigest(),stable);updateReceiptUsage(batch,command,-1,ack,minimum,floor,receipt.length,position,id);batch.put(receipts,receiptKey(id),receipt);
                } else batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position));
                published.put(id,publication);batch.delete(state,sourceOutcomeKey(0x65,id));batch.put(state,sourceOutcomeKey(0x66,id),publication.encode());batch.put(state,markerKey,encodePosition(position));db.write(options,batch);
            }
            publishSessionMetadata(sessionMetadata);sourceOutcomeCredits=Map.copyOf(next);sourceOutcomePublications=Map.copyOf(published);applied=position;return new SourceOutcomePublicationResult(outcome,publication);
        } finally { applyLane.unlock();leaveNative(); }
    }
    private static byte[] sourceOutcomeHash(byte[] value) { try { return java.security.MessageDigest.getInstance("SHA-256").digest(value); } catch(java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    private void recoverSourceOutcomes(ApplyPosition cut) throws RocksDBException {
        var credits=new HashMap<OperationId,NativeSourceOutcomeCredit>();var publications=new HashMap<OperationId,NativeSourceOutcomePublication>();long bytes=0;
        for(int tag:List.of(0x65,0x66))try(var iterator=databaseOwner.newIterator(state)) {
            byte[] prefix=ByteBuffer.allocate(17).put(markerKey).put((byte)tag).array();iterator.seek(prefix);byte[] sizing=new byte[0];
            while(iterator.isValid()) { byte[] key=iterator.key();if(key.length<17 || !Arrays.equals(key,0,17,prefix,0,17))break;int size=iterator.value(sizing);if(key.length!=49 || size>(tag==0x65?NativeSourceOutcomeCredit.MAX_BYTES:NativeSourceOutcomePublication.BYTES) || (bytes+=49L+size)>NativeSourceOutcomeCredit.MAX_METADATA_BYTES)throw new IllegalStateException("Source outcome metadata exceeds bound");
                if(tag==0x65) { var credit=NativeSourceOutcomeCredit.decode(iterator.value());credit.requireCut(cut);var id=credit.request().original().operationId();if(!groupId.equals(credit.request().original().groupId()) || !Arrays.equals(key,sourceOutcomeKey(tag,id)) || credits.size()>=NativeSourceOutcomeCredit.MAX_CREDITS || credits.put(id,credit)!=null)throw new IllegalStateException("Source credit identity/capacity changed"); }
                else { var publication=NativeSourceOutcomePublication.decode(iterator.value());publication.requireCut(cut);if(!groupId.equals(publication.group()) || !Arrays.equals(key,sourceOutcomeKey(tag,publication.operation())) || publications.size()>=NativeSourceOutcomeCredit.MAX_PUBLICATIONS || publications.put(publication.operation(),publication)!=null)throw new IllegalStateException("Source publication identity/capacity changed"); }
                iterator.next();
            }iterator.status();
        }
        if(bytes+credits.size()*(49L+NativeSourceOutcomePublication.BYTES)>NativeSourceOutcomeCredit.MAX_METADATA_BYTES || credits.keySet().stream().anyMatch(publications::containsKey) || publications.size()+credits.size()>NativeSourceOutcomeCredit.MAX_PUBLICATIONS)throw new IllegalStateException("Source credit/publication overlap");
        sourceOutcomeCredits=Map.copyOf(credits);sourceOutcomePublications=Map.copyOf(publications);
        if(sessionAckEpochs==null)validateSessionMetadata(null);
        if(sessionAckCount+sourceSessionMetadataPromises(minimumSessionEpoch(null),null,null)>sessionAckLimit)throw new IllegalStateException("Source ACK namespace promises exceed native capacity");
        if((!credits.isEmpty() || !publications.isEmpty()) && activeTopologyFreeze==null)throw new IllegalStateException("Source outcomes lack sealed parent");for(var credit:credits.values())if(!credit.request().freeze().equals(activeTopologyFreeze))throw new IllegalStateException("Source credit original seal changed");
    }
    private Runnable sourcePublicationGc(WriteBatch batch,long minimum,long ack,OperationId operation) throws RocksDBException {
        if(sourceOutcomePublications.isEmpty())return ()->{};var next=new HashMap<>(sourceOutcomePublications);
        for(var id:sourceOutcomePublications.keySet())if(id.epoch()<minimum || id.sessionId().equals(operation.sessionId()) && id.epoch()==operation.epoch() && id.sequence()<=ack) { batch.delete(state,sourceOutcomeKey(0x66,id));next.remove(id); }
        return ()->sourceOutcomePublications=Map.copyOf(next);
    }
    /** Cached shape classification for the adapter; validate/estimate/apply separately verify native evidence. */
    public boolean metadataTopologyControl(DistributedCommand command) {
        var freeze=activeTopologyFreeze;var control=command.sessionControl();
        if(freeze==null || !groupId.equals(command.groupId()) || rangeEpoch!=command.rangeEpoch()
                || !Arrays.equals(schemaDigest,command.schemaDigest()) || !command.mutations().isEmpty() || !command.predicates().isEmpty() || command.acceptance()!=null || command.transactionRejection())return false;
        return command.kind()==DistributedCommand.Kind.DATA && command.acknowledgedThrough()>=0
                || control!=null && !control.binding() && freeze.header().clusterId().equals(control.authority().clusterId()) && freeze.header().databaseId().equals(control.authority().databaseId());
    }
    private static final int MAX_RESULT_BYTES = 2 * 1024 * 1024;
    public static final int MAX_SCHEMA_AUTHORITY_BYTES=5*1024*1024;
    /** Consensus bound for original bucket bytes plus retained staged bucket replacements. */
    public static final long MAX_BUCKET_PLAN_BYTES=16L*1024*1024;
    public static final int MAX_PREPARED_PARTICIPANTS=64;
    public static final long MAX_PREPARED_BYTES=16L*1024*1024;
    private record PreparedIndex(List<PreparedParticipant> records,long bytes,long scopeBytes) {
        static PreparedIndex empty() { return new PreparedIndex(List.of(),0,0); }
    }
    private volatile PreparedIndex preparedIndex=PreparedIndex.empty();
    private volatile Map<UUID,AbortedParticipant> abortedIndex=Map.of();
    private volatile Map<UUID,CommittedParticipant> committedIndex=Map.of();
    private volatile Map<UUID,RejectedParticipant> rejectedIndex=Map.of();
    // Reserve both negative vote and terminal decision before either is appended.
    public static final long FINALIZATION_RESERVATION=66L+AbortedParticipant.MAX_BYTES+RejectedParticipant.MAX_BYTES;
    public static final int MAX_SNAPSHOT_GATES=16;
    public static final long MAX_SNAPSHOT_GATE_BYTES=16L*1024*1024;
    public static final int MAX_HOME_STATUS_ROWS=1024;
    public static final long MAX_HOME_STATUS_BYTES=16L*1024*1024;
    public static final long HOME_TRANSACTION_STATUS_BYTES=16L+TransactionStatus.MAX_BYTES;
    public static long homeSnapshotStatusBytes(SnapshotFence fence) {return 16L+SnapshotFenceStatus.HEADER_BYTES+fence.encode().length+81L*fence.participants().size();}
    public record HomeStatusCapacity(ApplyPosition position,int rows,long reservedBytes) {}
    @FunctionalInterface public interface HomeStatusWriteObserver {void afterWrite() throws RocksDBException;}
    private volatile HomeStatusWriteObserver homeStatusWriteObserver=()->{};
    public void homeStatusWriteObserver(HomeStatusWriteObserver observer){homeStatusWriteObserver=Objects.requireNonNull(observer);}
    private boolean homeStatusCapacityKnown;
    private int homeStatusRows;
    private long homeStatusBytes;
    private HomeStatusCapacity currentHomeStatusCapacity(HomeRecoveryContext context,boolean snapshots) throws RocksDBException {
        if(!homeStatusCapacityKnown){var recovered=scanHomeStatusCapacity(context,snapshots,null);homeStatusRows=recovered.rows();homeStatusBytes=recovered.reservedBytes();homeStatusCapacityKnown=true;}
        return new HomeStatusCapacity(applied,homeStatusRows,homeStatusBytes);
    }
    private record HomeStatusDelta(int rows,long bytes,boolean invalidate) {}
    private HomeStatusDelta homeStatusDelta(List<Mutation> effects) throws RocksDBException {
        var context=homeRecoveryContext!=null ? homeRecoveryContext : snapshotHomeContext;if(context==null || effects.isEmpty())return null;
        currentHomeStatusCapacity(context,snapshotHomeContext!=null);int rows=0;long bytes=0;boolean invalidate=false;var touched=new java.util.HashSet<String>();
        for(var mutation:effects)if(mutation.columnId().equals(context.column())) {
            if(!touched.add(java.util.HexFormat.of().formatHex(mutation.key())))throw new IllegalArgumentException("Duplicate HOME status mutation key");
            if(mutation.kind()==Mutation.Kind.DELETE_RANGE){invalidate=true;continue;}
            byte[] old=db.get(columns.get(context.column()),mutation.key());
            if(mutation.kind()==Mutation.Kind.PUT){if(old==null){rows++;bytes=Math.addExact(bytes,snapshotHomeContext!=null ? homeSnapshotStatusBytes(SnapshotFenceStatus.decode(mutation.value()).fence()) : HOME_TRANSACTION_STATUS_BYTES);}else if(snapshotHomeContext!=null && !SnapshotFenceStatus.decode(old).fence().equals(SnapshotFenceStatus.decode(mutation.value()).fence()))throw new IllegalArgumentException("Snapshot HOME immutable definition changed");}
            else if(mutation.kind()==Mutation.Kind.DELETE && old!=null){rows--;bytes-=snapshotHomeContext!=null ? homeSnapshotStatusBytes(SnapshotFenceStatus.decode(old).fence()) : HOME_TRANSACTION_STATUS_BYTES;}
        }
        return new HomeStatusDelta(rows,bytes,invalidate);
    }
    private boolean homeStatusFits(HomeStatusDelta change){return change==null || homeStatusRows+change.rows()<=MAX_HOME_STATUS_ROWS && homeStatusBytes+change.bytes()<=MAX_HOME_STATUS_BYTES;}
    private void publishHomeStatus(HomeStatusDelta change){if(change!=null){homeStatusRows+=change.rows();homeStatusBytes+=change.bytes();if(change.invalidate())homeStatusCapacityKnown=false;}}

    public HomeStatusCapacity homeStatusCapacity() throws RocksDBException {
        enterNative();applyLane.lock();try {var context=homeRecoveryContext!=null ? homeRecoveryContext : snapshotHomeContext;if(context==null)throw new IllegalStateException("HOME status role is not configured");return currentHomeStatusCapacity(context,snapshotHomeContext!=null);}finally {applyLane.unlock();leaveNative();}
    }
    private static void requireHomeStatusCapacity(int rows,long bytes) {
        if(rows>MAX_HOME_STATUS_ROWS || bytes>MAX_HOME_STATUS_BYTES)throw new IllegalStateException("HOME USER status reservation capacity exhausted");
    }
    private HomeStatusCapacity scanHomeStatusCapacity(HomeRecoveryContext context,boolean snapshots,Snapshot snapshot) throws RocksDBException {
        int rows=0;long bytes=0;
        try(var options=new ReadOptions().setSnapshot(snapshot).setFillCache(false);var iterator=databaseOwner.newIterator(columns.get(context.column()),options)) {
            if(interval.lowerInclusive()==null)iterator.seekToFirst();else iterator.seek(interval.lowerInclusive());var sizing=new byte[0];
            while(iterator.isValid() && contains(iterator.key())) {
                if(iterator.key(sizing)!=16 || iterator.value(sizing)>(snapshots ? SnapshotFenceStatus.MAX_BYTES : TransactionStatus.MAX_BYTES))throw new IllegalArgumentException("Invalid HOME USER status row shape");
                if(snapshots){var status=SnapshotFenceStatus.decode(iterator.value());requireSnapshotHomeUser(context,iterator.key(),status);bytes=Math.addExact(bytes,homeSnapshotStatusBytes(status.fence()));}
                else {var status=TransactionStatus.decode(iterator.value());if(!status.clusterId().equals(context.cluster()) || !status.databaseId().equals(context.database()) || !Arrays.equals(iterator.key(),uuidBytes(status.transactionId())))throw new IllegalStateException("HOME USER status identity differs");bytes=Math.addExact(bytes,HOME_TRANSACTION_STATUS_BYTES);}
                requireHomeStatusCapacity(++rows,bytes);iterator.next();
            }
            iterator.status();
        }
        return new HomeStatusCapacity(applied,rows,bytes);
    }
    private void requireHomeStatusAdmission(DistributedCommand command) {
        var context=homeRecoveryContext!=null ? homeRecoveryContext : snapshotHomeContext;if(context==null || command.mutations().isEmpty())return;
        enterNative();applyLane.lock();try {
            if(db.get(receipts,receiptKey(command.operationId()))!=null || expiredHomeRegistration(command))return;
            int additions=0;long bytes=0;var keys=new java.util.HashSet<String>();
            for(var mutation:command.mutations())if(mutation.kind()==Mutation.Kind.PUT && mutation.columnId().equals(context.column())) {
                byte[] old=db.get(columns.get(context.column()),mutation.key());
                if(snapshotHomeContext!=null){var next=SnapshotFenceStatus.decode(mutation.value());if(old!=null){if(!SnapshotFenceStatus.decode(old).fence().equals(next.fence()))throw new IllegalArgumentException("Snapshot HOME immutable definition changed");continue;}}
                if(old!=null)continue;
                var predicate=command.predicates().stream().filter(p->p.columnId().equals(mutation.columnId()) && Arrays.equals(p.key(),mutation.key())).findFirst().orElse(null);
                if(predicate!=null && predicate.expectedValue()!=null)continue;
                if(!keys.add(java.util.HexFormat.of().formatHex(mutation.key())))throw new IllegalArgumentException("Duplicate HOME status registration key");
                additions++;bytes=Math.addExact(bytes,snapshotHomeContext!=null ? homeSnapshotStatusBytes(SnapshotFenceStatus.decode(mutation.value()).fence()) : HOME_TRANSACTION_STATUS_BYTES);
            }
            if(additions!=0){var current=currentHomeStatusCapacity(context,snapshotHomeContext!=null);requireHomeStatusCapacity(current.rows()+additions,Math.addExact(current.reservedBytes(),bytes));}
        }catch(RocksDBException failure){throw new IllegalStateException("Cannot qualify HOME USER status capacity",failure);}finally {applyLane.unlock();leaveNative();}
    }
    public static final int MAX_HOME_RECOVERIES=1024;
    public static final long MAX_HOME_RECOVERY_BYTES=16L*1024*1024;
    private volatile Map<OperationId,HomeRecoveryReceipt> homeRecoveries=Map.of();
    private long retainedHomeRecoveryBytes,reservedHomeRecoveryBytes;
    private int reservedHomeRecoveryCount;
    private volatile Map<UUID,HomeRecoveryCredit> homeRecoveryCredits=Map.of();
    private volatile HomeRecoveryContext homeRecoveryContext;
    private volatile HomeRecoveryContext snapshotHomeContext;
    private record HomeRecoveryContext(UUID cluster,UUID database,UUID column) {}
    public record HomeRecoveryCredit(UUID group,long rangeEpoch,byte[] schema,UUID cluster,UUID database,UUID column,TransactionStatus initial,TransactionStatus current,ApplyPosition position) {
        public static final int MAX_BYTES=128+2*TransactionStatus.MAX_BYTES;
        public static final long TRANSITION_BYTES=49L+HomeRecoveryReceipt.MAX_BYTES;
        public HomeRecoveryCredit {
            Objects.requireNonNull(group); Objects.requireNonNull(schema); Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(column); Objects.requireNonNull(initial); Objects.requireNonNull(current); Objects.requireNonNull(position);
            initial.requireInitial();
            if (rangeEpoch<0 || schema.length!=32 || position.term()<=0 || !cluster.equals(initial.clusterId()) || !database.equals(initial.databaseId()) || !initial.sameDefinition(current) || remaining(current)==0) throw new IllegalArgumentException("Invalid HOME recovery promise");
            var rebuilt=initial;
            for (var p:current.participants()) if (p.prepared()!=null) rebuilt=rebuilt.prepared(p.groupId(),p.planDigest(),p.prepared());
            if (current.terminal()) rebuilt=rebuilt.reserved() ? rebuilt.abortReservation(current.noEffectOutcome()) : rebuilt.decide(current.phase());
            for (var p:current.participants()) if (p.finalized()) rebuilt=rebuilt.finalized(p.groupId());
            if (current.consumed()) rebuilt=rebuilt.consume();
            if (!rebuilt.equals(current)) throw new IllegalArgumentException("HOME promise progress/generation changed"); schema=schema.clone();
        }
        @Override public byte[] schema() { return schema.clone(); }
        public static int remaining(TransactionStatus status) { return status.reserved() ? 2+status.origin().targetParticipantCount() : (status.consumed() ? 0 : 1)+(status.phase()==TransactionStatus.Phase.PREPARING ? 1 : 0)+(int)status.participants().stream().filter(p -> !p.finalized()).count(); }
        public int reservedCount() { return remaining(current); }
        public long reservedBytes() { return reservedCount()*TRANSITION_BYTES+33L+(initial.origin()==null ? encode().length : 128L+2L*TransactionStatus.MAX_BYTES); }
        public byte[] userKey() { return uuidBytes(current.transactionId()); }
        public void requireCut(ApplyPosition cut) { if (cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("HOME promise differs from completed cut"); }
        public void requireOwner(RangeOwnership.Owner owner,RangeOwnership.Interval interval,byte[] digest) {
            var key=userKey(); if (!group.equals(owner.groupId()) || rangeEpoch!=owner.epoch() || !Arrays.equals(schema,digest) || !interval.columns().equals(Set.of(column))
                    || interval.lowerInclusive()!=null && Arrays.compareUnsigned(key,interval.lowerInclusive())<0 || interval.upperExclusive()!=null && Arrays.compareUnsigned(key,interval.upperExclusive())>=0) throw new IllegalArgumentException("HOME promise owner/schema/column differs");
        }
        public byte[] encode() {
            byte[] first=initial.encode(),next=current.encode(); var b=ByteBuffer.allocate(128+first.length+next.length);
            b.putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).putLong(rangeEpoch).put(schema);
            for (var id:List.of(cluster,database,column)) b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
            return b.putLong(position.term()).putLong(position.index()).putInt(first.length).put(first).putInt(next.length).put(next).array();
        }
        @Override public boolean equals(Object other) { return other instanceof HomeRecoveryCredit credit && Arrays.equals(encode(),credit.encode()); }
        @Override public int hashCode() { return Arrays.hashCode(encode()); }
        public static HomeRecoveryCredit decode(byte[] bytes) {
            if (bytes.length<128 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid HOME promise length");
            try {
                var b=ByteBuffer.wrap(bytes); UUID group=new UUID(b.getLong(),b.getLong()); long epoch=b.getLong(); byte[] schema=new byte[32]; b.get(schema);
                UUID cluster=new UUID(b.getLong(),b.getLong()),database=new UUID(b.getLong(),b.getLong()),column=new UUID(b.getLong(),b.getLong()); var position=new ApplyPosition(b.getLong(),b.getLong());
                int first=b.getInt(); if (first<0 || first>TransactionStatus.MAX_BYTES || first>b.remaining()-4) throw new IllegalArgumentException("Invalid HOME promise definition"); byte[] initial=new byte[first]; b.get(initial);
                int next=b.getInt(); if (next<0 || next>TransactionStatus.MAX_BYTES || next!=b.remaining()) throw new IllegalArgumentException("Invalid HOME promise progress"); byte[] current=new byte[next]; b.get(current);
                var result=new HomeRecoveryCredit(group,epoch,schema,cluster,database,column,TransactionStatus.decode(initial),TransactionStatus.decode(current),position);
                if (!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical HOME promise"); return result;
            } catch (RuntimeException invalid) { throw new IllegalArgumentException("Invalid HOME promise",invalid); }
        }
    }
    private volatile Map<UUID,SnapshotGate> snapshotGates=Map.of();
    private long retainedGateBytes;
    private volatile DistributedSchema logicalSchema;
    private volatile NativeSchemaFence activeSchemaFence;
    private volatile NativeTopologyFreeze activeTopologyFreeze;
    private volatile NativeTopologyBirth topologyBirth;
    private volatile NativeTopologyLearnerRegistration topologyLearner;
    private volatile NativeTopologyLearnerImagePermission learnerImagePermission;
    private NativeTopologyLearnerImagePermission pendingLearnerImagePermission;
    private volatile NativeTopologyLearnerImagePermission learnerSchemaPreparation;
    private RangeOwnership.LearnerSchemaLease learnerSchemaLease;
    @FunctionalInterface public interface LearnerImageForceObserver { void boundary(boolean afterPersistence) throws RocksDBException; }
    private volatile LearnerImageForceObserver learnerImageForceObserver=after -> {};
    public void topologyLearnerImageForceObserver(LearnerImageForceObserver observer) { learnerImageForceObserver=Objects.requireNonNull(observer); }
    private volatile Map<UUID,NativeTopologyRelocationCompletion> relocationCompletions=Map.of();
    private volatile NativeTopologyRelocationCompletion relocationCompletion;
    private volatile boolean learnerServing;
    public boolean topologyLearnerServing() { return learnerServing; }
    public NativeTopologyRelocationCompletion topologyRelocationCompletion() { return relocationCompletion; }
    public NativeTopologyLearnerRegistration topologyLearnerRegistration() { return topologyLearner; }
    public NativeTopologyLearnerImagePermission topologyLearnerImagePermission() { return learnerImagePermission; }
    NativeTopologyLearnerImagePermission learnerSchemaPreparation() { return learnerSchemaPreparation; }
    private volatile NativeTopologyActivation topologyActivation;
    private volatile byte[] topologyRetirement;
    private volatile NativeTopologyRecoveryFence topologyRecoveryFence;
    private volatile NativeMetadataRecoveryRegistration metadataRecoveryRegistration;
    private volatile NativeCurrentRangeRecovery currentRangeRecovery;
    private volatile boolean currentRangeRecoveryReady;
    private byte[] pendingCurrentCompletion;
    @FunctionalInterface public interface CurrentRecoveryForceObserver {void boundary(boolean afterPersistence)throws RocksDBException;}
    private volatile CurrentRecoveryForceObserver currentRecoveryCompletionForceObserver=after->{};
    public void currentRecoveryCompletionForceObserver(CurrentRecoveryForceObserver observer){currentRecoveryCompletionForceObserver=Objects.requireNonNull(observer);}
    private volatile boolean metadataRecoveryReady;
    private volatile TopologyPreparation topologyPreparation;
    private volatile NativeSchemaInstallation activeSchemaInstallation;
    private volatile NativeSchemaActivationProof activeSchemaActivation;
    private volatile Map<UUID,NativeSchemaInstallProof> schemaInstallProofs=Map.of();
    private volatile Map<UUID,NativeSchemaActivationProof> schemaActivationProofs=Map.of();
    private volatile Map<String,Set<UUID>> schemaMergeHistory=Map.of();
    private volatile Map<String,ApplyPosition> schemaSuperseded=Map.of();
    public NativeSchemaActivationProof schemaActivation() { return activeSchemaActivation; }
    private record SchemaGeneration(DistributedSchema schema,RangeOwnership.Interval interval,Map<UUID,ColumnFamilyHandle> columns,Set<UUID> deterministicMergeColumns) {
        public SchemaGeneration { columns=Map.copyOf(columns); deterministicMergeColumns=Set.copyOf(deterministicMergeColumns); }
    }
    private volatile SchemaGeneration installedGeneration;
    public DistributedSchema installedSchema() { var generation=installedGeneration; return generation==null ? logicalSchema : generation.schema(); }
    private volatile Map<String,DistributedSchema> schemaHistory=Map.of();
    public NativeSchemaInstallation schemaInstallation() { return activeSchemaInstallation; }
    private static String schemaIdentity(byte[] digest) { return java.util.HexFormat.of().formatHex(digest); }
    private boolean knownSchema(byte[] digest) { return Arrays.equals(schemaDigest,digest) || schemaHistory.containsKey(schemaIdentity(digest)); }
    private byte[] schemaStateKey(int tag) { return ByteBuffer.allocate(17).put(markerKey).put((byte)tag).array(); }
    private byte[] schemaHistoryKey(UUID change) { return schemaChangeKey(0x2a,change); }
    byte[] schemaCapacityInstalledRow(UUID change) throws RocksDBException { return db.get(state,schemaHistoryKey(change)); }
    private byte[] schemaChangeKey(int tag,UUID change) { return ByteBuffer.allocate(33).put(markerKey).put((byte)tag).putLong(change.getMostSignificantBits()).putLong(change.getLeastSignificantBits()).array(); }
    @FunctionalInterface interface SchemaMetadataReader<T> { T read() throws RocksDBException; }
    <T> T readSchemaMetadata(SchemaMetadataReader<T> reader) throws RocksDBException {
        enterNative(); applyLane.lock(); try { return reader.read(); } finally { applyLane.unlock(); leaveNative(); }
    }
    private void recoverSchemaMetadata() throws RocksDBException { recoverSchemaMetadata(applied); }
    private void recoverSchemaMetadata(ApplyPosition cut) throws RocksDBException {
        byte[] currentBytes=db.get(state,schemaStateKey(0x28)),activationBytes=db.get(state,schemaStateKey(0x2c)),pendingBytes=db.get(state,schemaFenceKey());
        var current=currentBytes==null ? null : NativeSchemaInstallation.decode(currentBytes);
        var activation=activationBytes==null ? null : NativeSchemaActivationProof.decode(activationBytes);
        var pending=pendingBytes==null ? null : NativeSchemaFence.decode(pendingBytes);
        var records=new ArrayList<NativeSchemaInstallation>(); var activations=new HashMap<UUID,NativeSchemaActivationProof>();
        long bytes=(currentBytes==null ? 0 : currentBytes.length)+(activationBytes==null ? 0 : activationBytes.length);
        for(int tag:List.of(0x2a,0x2b)) try(var iterator=databaseOwner.newIterator(state)) {
            var prefix=schemaStateKey(tag); iterator.seek(prefix);
            while(iterator.isValid() && iterator.key().length>=prefix.length && Arrays.equals(iterator.key(),0,prefix.length,prefix,0,prefix.length)) {
                byte[] value=iterator.value(); if((bytes+=value.length)>16L*1024*1024) throw new IllegalStateException("Schema lifecycle metadata exceeds retention budget");
                if(tag==0x2a) {
                    var record=NativeSchemaInstallation.decode(value); record.requireRange(groupId,rangeEpoch,interval.domainId(),interval.lowerInclusive(),interval.upperExclusive());
                    if(records.size()>=64 || !Arrays.equals(iterator.key(),schemaHistoryKey(record.definition().changeId()))) throw new IllegalStateException("Invalid schema history identity/count"); records.add(record);
                } else {
                    var proof=NativeSchemaActivationProof.decode(value); var change=proof.installation().fence().changeId();
                    if(activations.size()>=64 || !Arrays.equals(iterator.key(),schemaChangeKey(tag,change)) || activations.put(change,proof)!=null) throw new IllegalStateException("Invalid schema activation history identity/count");
                }
                iterator.next();
            }
            iterator.status();
        }
        var ordered=NativeSchemaInstallation.validateLifecycle(current,records,activation,activations,pending,cut);
        var schemas=new HashMap<String,DistributedSchema>(); var merges=new HashMap<String,Set<UUID>>(); var superseded=new HashMap<String,ApplyPosition>(); var proofs=new HashMap<UUID,NativeSchemaInstallProof>();
        for(var record:ordered) {
            var source=record.definition().sourceSchema(); var target=record.definition().targetSchema();
            schemas.put(schemaIdentity(source.digest()),source); schemas.put(schemaIdentity(target.digest()),target);
            merges.put(schemaIdentity(source.digest()),record.sourceMerges()); merges.put(schemaIdentity(target.digest()),record.targetMerges());
            superseded.put(schemaIdentity(source.digest()),record.position()); proofs.put(record.definition().changeId(),record.proof());
        }
        if(current!=null && (!Arrays.equals(current.definition().targetSchema().digest(),schemaDigest) || !current.definition().targetSchema().columnIds().equals(logicalColumns()) || !current.targetMerges().equals(deterministicMergeColumns))) throw new IllegalStateException("Current generation differs from opened handles");
        schemaHistory=Map.copyOf(schemas); schemaMergeHistory=Map.copyOf(merges); schemaSuperseded=Map.copyOf(superseded); schemaInstallProofs=Map.copyOf(proofs); schemaActivationProofs=Map.copyOf(activations);
        activeSchemaInstallation=current; activeSchemaActivation=activation;
        if(current!=null) { logicalSchema=current.definition().targetSchema(); installedGeneration=new SchemaGeneration(logicalSchema,interval,columns,deterministicMergeColumns); }
    }
    public NativeSchemaInstallation lookupSchemaInstallationRecord(SchemaChange.Header header) throws RocksDBException {
        Objects.requireNonNull(header); enterNative(); applyLane.lock();
        try { return savedSchemaInstallation(header); } finally { applyLane.unlock(); leaveNative(); }
    }
    private NativeSchemaInstallation savedSchemaInstallation(SchemaChange.Header header) throws RocksDBException {
        byte[] value=db.get(state,schemaHistoryKey(header.changeId())); if(value==null) return null;
        var record=NativeSchemaInstallation.decode(value); record.fence().requireHeader(header); record.requireCut(applied);
        record.requireRange(groupId,rangeEpoch,interval.domainId(),interval.lowerInclusive(),interval.upperExclusive()); return record;
    }
    public NativeSchemaInstallProof lookupSchemaInstallation(SchemaChange.Header header) throws RocksDBException {
        var record=lookupSchemaInstallationRecord(header); return record==null ? null : record.proof();
    }
    public NativeSchemaActivationProof lookupSchemaActivation(SchemaChange.Header header) {
        Objects.requireNonNull(header); enterNative(); applyLane.lock();
        try { var proof=schemaActivationProofs.get(header.changeId()); if(proof!=null) { proof.requireHeader(header); proof.requireCut(applied); } return proof; }
        finally { applyLane.unlock(); leaveNative(); }
    }
    public NativeSchemaFence lookupSchemaFence(SchemaChange.Header header) {
        Objects.requireNonNull(header); enterNative(); applyLane.lock();
        try {
            var saved=schemaInstallProofs.get(header.changeId());
            var proof=saved!=null ? saved.fence() : activeSchemaFence!=null && activeSchemaFence.changeId().equals(header.changeId()) ? activeSchemaFence : null;
            if(proof!=null) { proof.requireHeader(header); proof.requireCut(applied); } return proof;
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public record ActivatedSchema(SchemaChange.Definition definition,NativeSchemaActivationProof proof) {}
    public List<ActivatedSchema> activatedSchemas() throws RocksDBException {
        enterNative(); applyLane.lock();
        try {
            var result=new ArrayList<ActivatedSchema>(); long bytes=0;
            for(var proof:schemaInstallProofs.values().stream().sorted(Comparator.comparingLong(p -> p.position().index())).toList()) {
                var activation=schemaActivationProofs.get(proof.fence().changeId()); if(activation==null) continue;
                byte[] encoded=db.get(state,schemaHistoryKey(proof.fence().changeId()));
                if(encoded==null || (bytes+=encoded.length)>16L*1024*1024) throw new IllegalStateException("Activated schema history missing or exceeds budget");
                var record=NativeSchemaInstallation.decode(encoded); if(!record.proof().equals(proof) || !activation.installation().equals(proof)) throw new IllegalStateException("Activated definition differs from original native proof");
                record.requireRange(groupId,rangeEpoch,interval.domainId(),interval.lowerInclusive(),interval.upperExclusive()); result.add(new ActivatedSchema(record.definition(),activation));
            }
            return List.copyOf(result);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public boolean qualifiedHistoricalImage(NativeRangeImage.Manifest image) {
        Objects.requireNonNull(image); enterNative(); applyLane.lock();
        try {
            if(Arrays.equals(image.schemaDigest(),schemaDigest)) return false;
            var identity=schemaIdentity(image.schemaDigest()); var schema=schemaHistory.get(identity); if(schema==null) return false;
            var superseded=schemaSuperseded.get(identity);
            if(!image.owner().equals(owner) || !image.interval().domainId().equals(interval.domainId()) || !image.interval().columns().equals(schema.columnIds())
                    || !Arrays.equals(image.interval().lowerInclusive(),interval.lowerInclusive()) || !Arrays.equals(image.interval().upperExclusive(),interval.upperExclusive())
                    || !image.deterministicMergeColumns().equals(schemaMergeHistory.get(identity)) || superseded==null || image.position().index()>=superseded.index() || image.position().term()>superseded.term()) throw new IllegalArgumentException("Historical image differs from exact generation/cut");
            return true;
        } finally { applyLane.unlock(); leaveNative(); }
    }

    private volatile HomeStatusWriteObserver catalogueHistoryWriteObserver=()->{};
    public void catalogueHistoryWriteObserver(HomeStatusWriteObserver observer){catalogueHistoryWriteObserver=Objects.requireNonNull(observer);}
    private int catalogueDirectorySlots=-1;
    private volatile UUID catalogueHistoryColumn;
    private UUID catalogueLedgerColumn;
    private java.util.function.Predicate<byte[]> catalogueHistoryKeys;
    private java.util.function.Function<DistributedCommand,CatalogueHistory.Directive> catalogueHistoryPolicy;
    private CatalogueHistory.State catalogueHistoryCache;
    public void configureCatalogueHistory(UUID column,UUID ledgerColumn,java.util.function.Predicate<byte[]> keys,java.util.function.Function<DistributedCommand,CatalogueHistory.Directive> policy) throws RocksDBException {
        Objects.requireNonNull(column);Objects.requireNonNull(ledgerColumn);Objects.requireNonNull(keys);Objects.requireNonNull(policy);enterNative();applyLane.lock();try {
            if(column.equals(ledgerColumn) || !catalogueScopeCompaction || homeRecoveryContext!=null || snapshotHomeContext!=null || !columns.keySet().equals(Set.of(column,ledgerColumn)) || catalogueHistoryColumn!=null && (!catalogueHistoryColumn.equals(column) || !catalogueLedgerColumn.equals(ledgerColumn)))throw new IllegalArgumentException("Catalogue history role/column changed");
            catalogueHistoryColumn=column;catalogueLedgerColumn=ledgerColumn;catalogueHistoryKeys=keys;catalogueHistoryPolicy=policy;catalogueHistoryCache=null;catalogueDirectorySlots=-1;catalogueHistoryCache=verifyCatalogueHistory(null);
        }finally {applyLane.unlock();leaveNative();}
    }
    private byte[] catalogueHistoryKey(byte[] suffix){return ByteBuffer.allocate(16+suffix.length).put(markerKey).put(suffix).array();}
    public CatalogueHistory.State catalogueHistoryCapacity() throws RocksDBException {
        enterNative();applyLane.lock();try {if(catalogueHistoryColumn==null)throw new IllegalStateException("Catalogue history is not configured");if(catalogueHistoryCache==null)catalogueHistoryCache=verifyCatalogueHistory(null);return catalogueHistoryCache;}finally {applyLane.unlock();leaveNative();}
    }
    private CatalogueHistory.State verifyCatalogueHistory(Snapshot snapshot) throws RocksDBException {
        long actualBytes=0,actualRows=0,promisedBytes=0,promisedRows=0;int directorySlots=0;byte[] stored;
        try(var options=new ReadOptions().setSnapshot(snapshot).setFillCache(false)) {
            stored=db.get(state,options,catalogueHistoryKey(CatalogueHistory.stateKey()));
            if(stored!=null){actualBytes=17L+stored.length;actualRows=1;}
            var sizing=new byte[0];try(var rows=databaseOwner.newIterator(columns.get(catalogueHistoryColumn),options)) {
                if(interval.lowerInclusive()==null)rows.seekToFirst();else rows.seek(interval.lowerInclusive());
                for(;rows.isValid() && contains(rows.key());rows.next()){byte[] key=rows.key();if(key.length==16){if(++directorySlots>DatabaseDomainRoster.MAX_DOMAINS || rows.value(sizing)>RangeDirectoryCodec.MAX_ENCODED_BYTES)throw new IllegalStateException("Catalogue directory reservation bound exceeded");requireCatalogueDirectory(key,rows.value());}if(!catalogueHistoryKeys.test(key))continue;actualBytes=Math.addExact(actualBytes,key.length+(long)rows.value(sizing));actualRows++;if(actualBytes>CatalogueHistory.MAX_BYTES || actualRows>CatalogueHistory.MAX_ROWS)throw new IllegalStateException("Catalogue history native inventory exceeds capacity");}rows.status();
            }
            byte[] prefix=catalogueHistoryKey(new byte[]{0x79});try(var rows=databaseOwner.newIterator(state,options)) {
                rows.seek(prefix);for(;rows.isValid() && rows.key().length>=prefix.length && Arrays.equals(rows.key(),0,prefix.length,prefix,0,prefix.length);rows.next()){if(rows.key(sizing)!=49 || rows.value(sizing)!=CatalogueHistory.CREDIT_BYTES)throw new IllegalStateException("Invalid catalogue history credit shape");var credit=CatalogueHistory.Credit.decode(rows.value());if(!Arrays.equals(Arrays.copyOfRange(rows.key(),17,49),credit.definition()))throw new IllegalStateException("Catalogue history credit identity changed");actualBytes=Math.addExact(actualBytes,49L+CatalogueHistory.CREDIT_BYTES);actualRows++;promisedBytes=Math.addExact(promisedBytes,credit.bytes());promisedRows=Math.addExact(promisedRows,credit.rows());}rows.status();
            }
        }
        if(snapshot==null)catalogueDirectorySlots=directorySlots;
        if(stored==null){if(actualRows!=0)throw new IllegalStateException("Catalogue history inventory lacks accounting");return new CatalogueHistory.State(17L+CatalogueHistory.STATE_BYTES,1,0,0);}
        var actual=new CatalogueHistory.State(actualBytes,actualRows,promisedBytes,promisedRows);if(!actual.equals(CatalogueHistory.State.decode(stored)))throw new IllegalStateException("Catalogue history accounting differs from actual inventory");return actual;
    }
    private static void requireCatalogueDirectory(byte[] key,byte[] value){if(value.length>RangeDirectoryCodec.MAX_ENCODED_BYTES)throw new IllegalArgumentException("Catalogue directory value exceeds pre-reserved slot");var directory=RangeDirectoryCodec.decode(value);if(!Arrays.equals(key,uuidBytes(directory.domainId())))throw new IllegalArgumentException("Catalogue directory key/domain differs");}
    private record CatalogueChange(CatalogueHistory.Directive directive,CatalogueHistory.Update update,int directoryDelta) {}
    private CatalogueChange catalogueHistoryChange(DistributedCommand command,List<Mutation> effects) throws RocksDBException {
        if(catalogueHistoryColumn==null || effects.stream().noneMatch(effect->effect.columnId().equals(catalogueHistoryColumn)))return null;
        var directive=Objects.requireNonNull(catalogueHistoryPolicy.apply(command));
        for(var effect:effects)if(effect.columnId().equals(catalogueHistoryColumn) && effect.kind()!=Mutation.Kind.PUT && effect.kind()!=Mutation.Kind.DELETE)throw new IllegalArgumentException("Catalogue accounting requires bounded point mutations");
        var delta=new CatalogueHistory.Delta(0,0);var touched=new java.util.HashSet<String>();int directoryDelta=0;
        for(var effect:effects)if(effect.columnId().equals(catalogueHistoryColumn) && effect.key().length==16){if(!touched.add(java.util.HexFormat.of().formatHex(effect.key())))throw new IllegalArgumentException("Duplicate catalogue directory mutation key");byte[] previous=db.get(columns.get(catalogueHistoryColumn),effect.key());if(effect.kind()==Mutation.Kind.PUT){requireCatalogueDirectory(effect.key(),effect.value());if(previous==null)directoryDelta++;}else if(effect.kind()==Mutation.Kind.DELETE && previous!=null)directoryDelta--;else if(effect.kind()!=Mutation.Kind.DELETE)throw new IllegalArgumentException("Catalogue directory requires point mutation");}
        for(var effect:effects)if(effect.columnId().equals(catalogueHistoryColumn) && catalogueHistoryKeys.test(effect.key())) {
            if(effect.kind()!=Mutation.Kind.PUT && effect.kind()!=Mutation.Kind.DELETE)throw new IllegalArgumentException("Catalogue history requires bounded point mutations");
            if(!touched.add(java.util.HexFormat.of().formatHex(effect.key())))throw new IllegalArgumentException("Duplicate catalogue history mutation key");
            delta=delta.plus(CatalogueHistory.Delta.replacement(effect.key(),db.get(columns.get(catalogueHistoryColumn),effect.key()),effect.kind()==Mutation.Kind.DELETE ? null : effect.value()));
        }
        if(directoryDelta==0 && delta.bytes()==0 && delta.rows()==0 && directive.action()==CatalogueHistory.Action.UNRESERVED)return null;
        if(catalogueHistoryCache==null)catalogueHistoryCache=verifyCatalogueHistory(null);
        if(catalogueDirectorySlots+directoryDelta>DatabaseDomainRoster.MAX_DOMAINS)return new CatalogueChange(directive,null,0);
        var definition=directive.definition();byte[] value=definition==null ? null : db.get(state,catalogueHistoryKey(CatalogueHistory.creditKey(definition)));var credit=value==null ? null : CatalogueHistory.Credit.decode(value);
        try {
            var updated=CatalogueHistory.apply(catalogueHistoryCache,credit,directive,delta);
            // The current common model charges suffixes; native STATE keys also carry group UUID16.
            long prefix=directive.action()==CatalogueHistory.Action.BEGIN ? 16 : directive.action()==CatalogueHistory.Action.TERMINAL ? -16 : 0;
            var qualified=new CatalogueHistory.State(Math.addExact(updated.state().actualBytes(),prefix),updated.state().actualRows(),updated.state().promisedBytes(),updated.state().promisedRows());
            return new CatalogueChange(directive,new CatalogueHistory.Update(qualified,updated.credit()),directoryDelta);
        }
        catch(IllegalArgumentException full){if(directive.action()==CatalogueHistory.Action.BEGIN || directive.action()==CatalogueHistory.Action.UNRESERVED)return new CatalogueChange(directive,null,0);throw full;}
    }
    private void writeCatalogueHistory(WriteBatch batch,CatalogueChange change) throws RocksDBException {
        if(change==null)return;batch.put(state,catalogueHistoryKey(CatalogueHistory.stateKey()),change.update().state().encode());
        if(change.directive().definition()!=null){byte[] key=catalogueHistoryKey(CatalogueHistory.creditKey(change.directive().definition()));if(change.update().credit()==null)batch.delete(state,key);else batch.put(state,key,change.update().credit().encode());}
    }
    private volatile boolean catalogueScopeCompaction;
    private RangeMutationHistory scopedHistory;
    private RangeMutationHistory.Limits scopedLimits;
    private final Map<RangeMutationHistory.Pin,ScopedHistoryPin> scopedPins=new IdentityHashMap<>();
    private final ConcurrentLinkedQueue<ScopedSnapshot> scopedReleases=new ConcurrentLinkedQueue<>();
    private volatile Object scopedGeneration=new Object();
    // Atomic DELETE_RANGE batches require the base DB; iterator parents must
    // be the owning wrapper so JNI disposes them while pool leases are held.
    private final RocksDB db;
    // getBaseDB() is borrowed and does not retain its original Java/native owner.
    private final RocksDB databaseOwner;
    private final UUID groupId;
    private final long rangeEpoch;
    private volatile byte[] schemaDigest;
    private volatile Map<UUID, ColumnFamilyHandle> columns;
    private volatile boolean retiredMetadataOnly;
    private volatile int retiredRoutingPrefix=-1;
    void configureRetiredTopologyDirectory(it.cavallium.rockserver.core.common.distributed.RangeDirectory directory) {
        if(!retiredMetadataOnly || !directory.domainId().equals(interval.domainId()) || !Arrays.equals(directory.schemaDigest(),schemaDigest) || !directory.columns().equals(interval.columns()))throw new IllegalArgumentException("Retired directory differs from original projection");retiredRoutingPrefix=directory.routingPrefixBytes();
    }
    private Set<UUID> logicalColumns() { return retiredMetadataOnly ? interval.columns() : columns.keySet(); }
    public boolean userHandlesDetached() { return retiredMetadataOnly; }
    private volatile Set<UUID> deterministicMergeColumns;
    private final ColumnFamilyHandle state;
    private final ColumnFamilyHandle receipts;
    private final byte[] markerKey;
    private volatile NativeSstImportManifest importBirth;
    private volatile NativeSstImportManifest.Intent importIntent;
    private volatile NativeSstImportManifest.Receipt importReceipt;
    private volatile NativeRangeSstImport.Activation importActivation;
    public boolean importPending() { return importBirth!=null && importActivation==null; }
    public NativeSstImportManifest.Receipt importReceipt() { return importReceipt; }
    private byte[] importFileKey(UUID client,long call){return ByteBuffer.allocate(41).put(markerKey).put((byte)0x44).putLong(client.getMostSignificantBits()).putLong(client.getLeastSignificantBits()).putLong(call).array();}
    public NativeRangeSstImport.FileReceipt importFileReceipt(UUID client,long call,byte[] digest)throws RocksDBException {
        return readSchemaMetadata(()->{
            if(call<0 || digest.length!=32)throw new IllegalArgumentException("Invalid file result identity");
            byte[] value=db.get(state,importFileKey(client,call));if(value==null)return null;var result=NativeRangeSstImport.FileReceipt.decode(value);
            if(!client.equals(result.clientId()) || call!=result.callId() || !Arrays.equals(digest,result.requestDigest()))throw new IllegalArgumentException("File request identity reused with different payload");
            result.requireCut(applied);return result;
        });
    }
    public NativeRangeSstImport.FileReceipt applyImportFileReceipt(ApplyPosition position,UUID client,long call,byte[] digest,byte[] result)throws RocksDBException {
        enterNative();applyLane.lock();
        try {
            if(importBirth==null || pendingInstallation!=null || importIntent!=null)throw new IllegalStateException("File publication requires its original import owner");
            var stateNow=ownership.get(owner).state();if(stateNow!=RangeOwnership.State.ACTIVE && stateNow!=RangeOwnership.State.FENCED)throw new IllegalStateException("Import file owner unavailable");
            var saved=importFileReceipt(client,call,digest);if(saved!=null){applyNoopUnderAdmission(position,null);return saved;}
            requireNext(position);ensureReceiptUsage();var receipt=new NativeRangeSstImport.FileReceipt(client,call,digest,position,result);
            byte[] encodedCount=db.get(state,importKey(0x45));if(encodedCount!=null && encodedCount.length!=8)throw new IllegalStateException("Invalid import file receipt count");long count=encodedCount==null?0:ByteBuffer.wrap(encodedCount).getLong();
            if(count<0 || count>=NativeRangeSstImport.FileReceipt.MAX_RECEIPTS)throw new IllegalStateException("Import file receipt capacity exhausted");
            var writes=prepareScopedWrites(false,List.of(),Map.of());
            try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(true)) {
                if(cdc!=null) cdc.initialize(batch);
                batch.put(state,importFileKey(client,call),receipt.encode());batch.put(state,importKey(0x45),ByteBuffer.allocate(8).putLong(count+1).array());
                batch.put(state,markerKey,encodePosition(position));batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position));db.write(options,batch);
            }
            publishScoped(position,writes,false);applied=position;return receipt;
        }catch(RocksDBException|RuntimeException|Error failure){resetScopedUnderLane();throw failure;}
        finally{applyLane.unlock();try{drainScopedReleases();}finally{leaveNative();}}
    }
    public NativeRangeSstImport.Activation importActivation() throws RocksDBException { return readSchemaMetadata(() -> { recoverImportMetadata(); return importActivation; }); }
    private byte[] importKey(int tag) { return ByteBuffer.allocate(17).put(markerKey).put((byte)tag).array(); }
    private void recoverImportMetadata() throws RocksDBException {
        int[] tags={0x40,0x41,0x42,0x43},bounds={NativeSstImportManifest.MAX_BYTES,NativeSstImportManifest.MAX_BYTES+24,NativeSstImportManifest.Receipt.BYTES,168};
        for(int n=0;n<tags.length;n++){int size=db.get(state,importKey(tags[n]),new byte[0]);if(size!=RocksDB.NOT_FOUND && size>bounds[n])throw new IllegalArgumentException("Native import metadata exceeds accepted bound");}
        byte[] birth=db.get(state,importKey(0x40)),intent=db.get(state,importKey(0x41)),receipt=db.get(state,importKey(0x42)),activation=db.get(state,importKey(0x43));
        if(birth==null) { if(intent!=null || receipt!=null || activation!=null)throw new IllegalStateException("Import metadata lacks virgin birth"); return; }
        var manifest=NativeSstImportManifest.decode(birth);
        if(!manifest.groupId().equals(groupId) || manifest.rangeEpoch()!=rangeEpoch || !manifest.domainId().equals(interval.domainId()) || !Arrays.equals(manifest.lowerInclusive(),interval.lowerInclusive()) || !Arrays.equals(manifest.upperExclusive(),interval.upperExclusive()))throw new IllegalStateException("Import birth differs from store geometry");
        if(intent!=null) { importIntent=NativeSstImportManifest.Intent.decode(intent); if(!Arrays.equals(importIntent.manifest().encode(),birth))throw new IllegalStateException("Import intent differs from birth"); requireNext(importIntent.position()); }
        importBirth=manifest;
        if(receipt!=null) { importReceipt=NativeSstImportManifest.Receipt.decode(receipt); importReceipt.requireManifest(manifest); if(intent!=null)throw new IllegalStateException("Installed import retains intent"); importReceipt.requireCut(applied); }
        if(activation!=null) { importActivation=NativeRangeSstImport.Activation.decode(activation); if(importReceipt==null || !Arrays.equals(importActivation.receipt().encode(),importReceipt.encode()))throw new IllegalStateException("Import activation differs from receipt"); importActivation.requireCut(applied); }
        boolean current=Arrays.equals(manifest.schemaDigest(),schemaDigest) && manifest.physicalNames().keySet().equals(logicalColumns()) && manifest.mergeColumns().equals(deterministicMergeColumns);
        if(!current) {
            if(importActivation==null)throw new IllegalStateException("Unpublished import schema changed");
            boolean qualified=false;
            for(var change:schemaInstallProofs.keySet()) { var record=NativeSchemaInstallation.decode(db.get(state,schemaHistoryKey(change)));
                if(Arrays.equals(record.definition().sourceSchema().digest(),manifest.schemaDigest()) && record.sourceNames().equals(manifest.physicalNames()) && record.sourceMerges().equals(manifest.mergeColumns()) && record.fence().position().index()>importActivation.position().index() && record.fence().position().term()>=importActivation.position().term()) { qualified=true; break; }
            }
            if(!qualified)throw new IllegalStateException("Historical import birth lacks subsequent qualified schema lifecycle");
        }
        if(importPending() && ownership.get(owner).state()==RangeOwnership.State.ACTIVE)ownership.fence(owner);
    }
    @FunctionalInterface interface ImportQualification { void run() throws IOException,RocksDBException; }
    @FunctionalInterface interface ImportIngestion { void run(RocksDB database,Map<UUID,ColumnFamilyHandle> columns) throws IOException,RocksDBException; }
    NativeSstImportManifest.Receipt installSstUnderPool(NativeSstImportManifest manifest,ApplyPosition position,ImportQualification qualification,ImportIngestion ingestion,NativeRangeSstImport.Observer observer) throws IOException,RocksDBException {
        nativeLifecycle.writeLock().lock(); applyLane.lock();
        try {
            if(closing || pendingInstallation!=null || importBirth==null || !Arrays.equals(importBirth.encode(),manifest.encode()))throw new IllegalStateException("Import requires exact virgin native birth");
            if(importReceipt==null && importIntent!=null) {
                // A sync final write may have completed before throwing. Re-force its atomic proof before retrying,
                // rather than writing a new intent beside an already completed receipt.
                byte[] completed=db.get(state,importKey(0x42));
                if(completed!=null) {
                    var saved=NativeSstImportManifest.Receipt.decode(completed); saved.requireManifest(manifest);
                    byte[] marker=db.get(state,markerKey),usage=db.get(state,usageKey); if(marker==null || usage==null || db.get(state,importKey(0x41))!=null)throw new IllegalStateException("Import completion lacks atomic native marker");
                    var cut=decodePosition(marker); saved.requireCut(cut);
                    if(!saved.position().equals(importIntent.position()) || !cut.equals(saved.position()))throw new IllegalStateException("Unknown import completion changed original position");
                    try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(true)) {
                        if(cdc!=null) cdc.initialize(batch); batch.put(state,importKey(0x42),completed); batch.put(state,markerKey,marker); batch.put(state,usageKey,usage); db.write(options,batch); }
                    importReceipt=saved; importIntent=null; applied=cut;
                }
            }
            if(importReceipt!=null) { importReceipt.requireManifest(manifest); importReceipt.requireCut(position); if(applied==null || position.index()>applied.index())applyNoopUnderAdmission(position,null); return importReceipt; }
            if(ownership.get(owner).state()!=RangeOwnership.State.FENCED || snapshotPins.get()!=0 || !preparedIndex.records().isEmpty() || !snapshotGates.isEmpty())throw new IllegalStateException("Import target must be unpinned NEW fenced owner");
            requireNext(position); if(importIntent!=null && !importIntent.position().equals(position))throw new IllegalArgumentException("Import retry changes original pending apply position"); qualification.run();
            try(var lease=ownership.claimInstall(owner)) {
                importIntent=new NativeSstImportManifest.Intent(manifest,position);
                try(var options=new WriteOptions().setDisableWAL(false).setSync(true)) { db.put(state,options,importKey(0x41),importIntent.encode()); }
                observer.at(NativeRangeSstImport.Stage.INTENT_FORCED);
                try(var writer=new InstallWriter(batches -> {})) { for(var cf:columns.values())clearForInstall(cf,interval.lowerInclusive(),interval.upperExclusive(),null,writer,false); writer.flush(); }
                db.flushWal(true); observer.at(NativeRangeSstImport.Stage.INTERVAL_RESET);
                ingestion.run(db,columns);
                try(var flush=new org.rocksdb.FlushOptions().setWaitForFlush(true)) { if(!columns.isEmpty())db.flush(flush,new ArrayList<>(columns.values())); }
                db.flushWal(true); observer.at(NativeRangeSstImport.Stage.DATA_DURABLE);
                var receipt=new NativeSstImportManifest.Receipt(manifest.importId(),groupId,rangeEpoch,manifest.digest(),position);
                try(var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(true)) {
                    if(cdc!=null) cdc.initialize(batch); batch.put(state,importKey(0x42),receipt.encode()); batch.delete(state,importKey(0x41)); batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch); }
                importIntent=null; importReceipt=receipt; applied=position; observer.at(NativeRangeSstImport.Stage.RECEIPT_FORCED); return receipt;
            }
        } finally { applyLane.unlock(); nativeLifecycle.writeLock().unlock(); }
    }
    void activateImportUnderPool(NativeRangeSstImport.Activation authority) throws RocksDBException {
        nativeLifecycle.writeLock().lock(); applyLane.lock();
        try {
            requireSnapshotReleaseHealthy(); if(closing || pendingInstallation!=null)throw new IllegalStateException("Import activation native admission is closed");
            if(importReceipt==null || !Arrays.equals(importReceipt.encode(),authority.receipt().encode()))throw new IllegalArgumentException("Import activation requires original installed receipt");
            if(importActivation!=null) { if(!Arrays.equals(importActivation.encode(),authority.encode()))throw new IllegalArgumentException("Import activation changed"); return; }
            if(ownership.get(owner).state()!=RangeOwnership.State.FENCED || snapshotPins.get()!=0)throw new IllegalStateException("Import activation requires fenced owner"); requireNext(authority.position());
            Runnable publishCdc=()->{};
            try(var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(true)) {
                if(cdc!=null) cdc.initialize(batch);
                if(cdc!=null) publishCdc=cdc.importedBaseline(batch,authority.position(),cdcAuthorityTime());
                batch.put(state,importKey(0x43),authority.encode()); batch.put(state,markerKey,encodePosition(authority.position())); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),authority.position())); db.write(options,batch);
            }
            publishCdc.run();
            importActivation=authority; applied=authority.position(); ownership.activate(owner);
        } finally { applyLane.unlock(); nativeLifecycle.writeLock().unlock(); }
    }
    private final byte[] installingKey;
    private final byte[] configurationKey;
    private final byte[] usageKey;
    private final byte[] usageBucketsPrefix;
    private final RangeOwnership ownership;
    private final RangeOwnership.Owner owner;
    private volatile RangeOwnership.Interval interval;
    private final ReentrantLock applyLane = new ReentrantLock();
    private final ReentrantReadWriteLock nativeLifecycle = new ReentrantReadWriteLock();
    private volatile boolean closing;
    private volatile ApplyPosition applied;
    private PoolSnapshotAdmission snapshotAdmission;
    private volatile Throwable snapshotReleaseFailure;
    private final AtomicInteger snapshotPins = new AtomicInteger();
    private final AtomicInteger trackedSnapshotPins=new AtomicInteger();
    private final AtomicLong trackedSnapshotScopeBytes=new AtomicLong();
    private volatile byte[] pendingInstallation;
    private volatile long installationGeneration;

    public ReplicatedRangeStore(RocksDB db, RangeOwnership ownership, RangeOwnership.Owner owner,
            byte[] schemaDigest, Map<UUID, ColumnFamilyHandle> columns,
            ColumnFamilyHandle state, ColumnFamilyHandle receipts,
            Set<UUID> deterministicMergeColumns) throws RocksDBException {
        this(db,ownership,owner,schemaDigest,columns,state,receipts,deterministicMergeColumns,false,null);
    }
    ReplicatedRangeStore(RocksDB db,RangeOwnership ownership,RangeOwnership.Owner owner,
            byte[] schemaDigest,Map<UUID,ColumnFamilyHandle> columns,ColumnFamilyHandle state,
            ColumnFamilyHandle receipts,Set<UUID> deterministicMergeColumns,boolean retiredMetadataOnly,NativeMetadataRecoveryRegistration metadataRegistration) throws RocksDBException {
        metadataRecoveryRegistration=metadataRegistration;
        this.retiredMetadataOnly=retiredMetadataOnly;
        if(retiredMetadataOnly && (ownership.get(owner).state()!=RangeOwnership.State.RETIRED || !columns.isEmpty()))throw new IllegalArgumentException("Metadata-only range requires retired ownership without USER handles");
        this.databaseOwner = Objects.requireNonNull(db);
        if (db instanceof TransactionDB) {
            throw new IllegalArgumentException("Distributed pools require regular or optimistic-base RocksDB for atomic range deletion");
        }
        this.db = db instanceof OptimisticTransactionDB optimistic ? optimistic.getBaseDB() : db;
        this.ownership = Objects.requireNonNull(ownership);
        this.owner = Objects.requireNonNull(owner);
        this.interval = ownership.get(owner).interval();
        this.groupId = owner.groupId();
        this.rangeEpoch = owner.epoch();
        if (schemaDigest.length != 32 || !retiredMetadataOnly && !interval.columns().equals(columns.keySet())) {
            throw new IllegalArgumentException("Invalid range schema or physical column coverage");
        }
        this.schemaDigest = schemaDigest.clone();
        this.columns = Map.copyOf(columns);
        this.deterministicMergeColumns = Set.copyOf(deterministicMergeColumns);
        if (!logicalColumns().containsAll(this.deterministicMergeColumns)) {
            throw new IllegalArgumentException("Merge admission refers to an unknown column");
        }
        this.state = Objects.requireNonNull(state);
        this.receipts = Objects.requireNonNull(receipts);
        if (state.getID() == receipts.getID() || columns.values().stream().anyMatch(
                cf -> cf.getID() == state.getID() || cf.getID() == receipts.getID())) {
            throw new IllegalArgumentException("User, state and receipt CFs must be distinct");
        }
        if (columns.values().stream().map(ColumnFamilyHandle::getID).distinct().count() != columns.size()) {
            throw new IllegalArgumentException("Logical columns must have distinct physical column families");
        }
        markerKey = uuidBytes(groupId);
        installingKey = ByteBuffer.allocate(17).put(markerKey).put((byte) 0x7f).array();
        configurationKey = ByteBuffer.allocate(17).put(markerKey).put((byte) 1).array();
        usageKey = ByteBuffer.allocate(17).put(markerKey).put((byte) 0x10).array();
        usageBucketsPrefix = ByteBuffer.allocate(17).put(markerKey).put((byte) 0x11).array();
        byte[] recovered = this.db.get(state, markerKey);
        applied = recovered == null ? null : decodePosition(recovered);
        pendingInstallation = this.db.get(state, installingKey);
        if (pendingInstallation != null) {
            if (pendingInstallation.length != 48) {
                throw new IllegalStateException("Invalid pending installation marker");
            }
            if (ownership.get(owner).state() == RangeOwnership.State.ACTIVE) ownership.fence(owner);
        }
        recoverTopologyOwnership();
        if(metadataRecoveryReady && !metadataImageInstalledUnderLane())throw new IllegalStateException("Ready metadata recovery lost its installed source image");
        if(retiredMetadataOnly && (!topologyRetired() || pendingInstallation!=null && metadataRecoveryRegistration==null))throw new IllegalArgumentException("Metadata-only owner lacks completed retirement/recovery authority");
        if(pendingInstallation==null && (metadataRecoveryRegistration==null || metadataImageInstalledUnderLane())) {
            recoverSchemaMetadata();
            recoverRelocationCompletions();
            recoverImportMetadata();
            ensureReceiptUsage();
            validateReceiptUsage(applied);
            homeRecoveries=loadHomeRecoveries(); homeRecoveryCredits=loadHomeCredits(applied); validateHomeCreditCapacity(); abortedIndex=loadAbortedIndex(); committedIndex=loadCommittedIndex(); rejectedIndex=loadRejectedIndex(); snapshotGates=loadSnapshotGates(); preparedIndex=loadPreparedIndex(); validateAbortedIndex(); validateCommittedIndex(); validateRejectedIndex(); validateSnapshotGates(); validateHomeRecoveries(); activeSchemaFence=loadSchemaFence(applied); activeTopologyFreeze=loadTopologyFreeze(applied);recoverSourceOutcomes(applied);
        }
    }

    // Called by the pool factory before publishing the store; standalone stores remain unconfigured.
    void configureSnapshotAdmission(PoolSnapshotAdmission guard) { this.snapshotAdmission = guard; }

    /** Null means no log entry has yet completed. This is not a durable snapshot watermark. */
    static byte[] topologyVirginUsage() { return ByteBuffer.allocate(32).putLong(0).putLong(0).putLong(0).putLong(-1).array(); }
    public ApplyPosition applied() { return applied; }
    /** Process-only failure; native release outcome is unknown and requires pool replacement. */
    public Throwable snapshotReleaseFailure() { return snapshotReleaseFailure; }
    public int activeSnapshotPins() { return snapshotPins.get(); }
    public int activeTrackedSnapshotPins() { return trackedSnapshotPins.get(); }
    public long trackedSnapshotScopeBytes() { return trackedSnapshotScopeBytes.get(); }
    public boolean pendingInstallation() { return pendingInstallation != null; }
    public RangeOwnership.Ownership ownership() { nativeLifecycle.readLock().lock(); try { return ownership.get(owner); } finally { nativeLifecycle.readLock().unlock(); } }
    public byte[] schemaDigest() { var generation=installedGeneration; return generation==null ? schemaDigest.clone() : generation.schema().digest(); }
    public Set<UUID> deterministicMergeColumns() { var generation=installedGeneration; return generation==null ? deterministicMergeColumns : generation.deterministicMergeColumns(); }

    public record Configuration(long term, long index, byte[] data) {
        public Configuration {
            if (term < 0 || index < -1 || (index == -1 && term != 0) || data.length == 0 || data.length > 65536) {
                throw new IllegalArgumentException("Invalid cut configuration");
            }
            data = data.clone();
        }
        @Override public byte[] data() { return data.clone(); }
        public static Configuration decode(byte[] value) { return decodeConfiguration(value); }
    }

    public Configuration configuration() throws RocksDBException {
        enterNative();
        try { return configurationUnderAdmission(); }
        finally { leaveNative(); }
    }

    private Configuration configurationUnderAdmission() throws RocksDBException {
        byte[] value = db.get(state, configurationKey);
        return value == null ? null : decodeConfiguration(value);
    }

    public void initializeConfiguration(byte[] initialConfiguration) throws RocksDBException {
        enterNative();
        try { initializeConfigurationUnderAdmission(initialConfiguration); }
        finally { leaveNative(); }
    }

    private void initializeConfigurationUnderAdmission(byte[] initialConfiguration) throws RocksDBException {
        applyLane.lock();
        try {
            if (configurationUnderAdmission() != null) return;
            if (applied != null || pendingInstallation != null) throw new IllegalStateException("Recovered range has no cut configuration");
            var bootstrap = new Configuration(0, -1, initialConfiguration);
            try (var options = new WriteOptions().setDisableWAL(false).setSync(true)) {
                db.put(state, options, configurationKey, encodeConfiguration(bootstrap));
            }
        } finally { applyLane.unlock(); }
    }

    public void fence() {
        applyLane.lock();
        try {
            var current = ownership.get(owner);
            if (current.state() == RangeOwnership.State.ACTIVE) ownership.fence(owner);
            else if (current.state() != RangeOwnership.State.FENCED && current.state()!=RangeOwnership.State.RETIRED && current.state()!=RangeOwnership.State.INSTALLING) throw new IllegalStateException("Range cannot be fenced");
            // INSTALLING already denies USER work; shutdown must preserve its recovery ownership.
            invalidateScopedUnderLane(RangeMutationHistory.Status.INVALIDATED);
        } finally { applyLane.unlock(); drainScopedReleases(); }
    }

    /** Local activation only; the range adapter must supply the committed ownership authority. */
    public void activate() {
        enterNative();
        try { activateUnderAdmission(); }
        finally { leaveNative(); }
    }

    private void activateUnderAdmission() {
        applyLane.lock();
        try {
            if(currentRangeRecovery!=null && !currentRangeRecoveryReady || topologyRetired() || topologyBirthPending()) throw new IllegalStateException("Topology owner requires qualified future activation");
            if (pendingInstallation != null || importPending()) throw new IllegalStateException("Installation remains incomplete or unpublished");
            try { validateTopologyActivationRecovery(); recoverSchemaMetadata(); } catch(RocksDBException failure) { throw new IllegalStateException("Cannot recover schema generation",failure); }
            homeRecoveries=loadHomeRecoveries(); homeRecoveryCredits=loadHomeCredits(applied); validateHomeCreditCapacity(); abortedIndex=loadAbortedIndex(); committedIndex=loadCommittedIndex(); rejectedIndex=loadRejectedIndex(); snapshotGates=loadSnapshotGates(); preparedIndex=loadPreparedIndex(); validateAbortedIndex(); validateCommittedIndex(); validateRejectedIndex(); validateSnapshotGates(); validateHomeRecoveries(); activeSchemaFence=loadSchemaFence(applied); activeTopologyFreeze=loadTopologyFreeze(applied);try { recoverSourceOutcomes(applied); } catch(RocksDBException failure) { throw new IllegalStateException("Cannot recover source outcome promises",failure); } validateSnapshotHomeUsersUnchecked();
            ownership.activate(owner);
        } finally { applyLane.unlock(); }
    }

    private void requireActive() {
        if (currentRangeRecovery!=null && !currentRangeRecoveryReady || pendingInstallation != null || importPending() || topologyBirthPending() || topologyRetired() || ownership.get(owner).state() != RangeOwnership.State.ACTIVE) {
            throw new IllegalStateException("Range is fenced");
        }
    }

    private byte[] topologyOwnershipKey(int tag) { return ByteBuffer.allocate(17).put(markerKey).put((byte)tag).array(); }
    private void recoverTopologyOwnership() throws RocksDBException {
        int currentSize=db.get(state,topologyOwnershipKey(0x72),new byte[0]);if(currentSize>NativeCurrentRangeRecovery.MAX_BYTES)throw new IllegalArgumentException("CURRENT registration exceeds bound");currentRangeRecovery=currentSize==RocksDB.NOT_FOUND?null:NativeCurrentRangeRecovery.decode(db.get(state,topologyOwnershipKey(0x72)));
        if(currentRangeRecovery!=null){var range=currentRangeRecovery.range();if(!groupId.equals(range.groupId()) || rangeEpoch!=range.rangeEpoch() || !interval.domainId().equals(currentRangeRecovery.schema().domainId()) || !Arrays.equals(interval.lowerInclusive(),range.lowerInclusive()) || !Arrays.equals(interval.upperExclusive(),range.upperExclusive()))throw new IllegalArgumentException("CURRENT registration changed exact receiver geometry");}
        byte[] currentReady=db.get(state,topologyOwnershipKey(0x73));currentRangeRecoveryReady=currentRangeRecovery!=null && currentReady!=null;
        if(currentRangeRecoveryReady)NativeCurrentRangeRecovery.requireCompletion(currentRangeRecovery,currentReady,applied);
        if(currentRangeRecovery==null && (currentReady!=null || db.get(state,topologyOwnershipKey(0x74))!=null))throw new IllegalArgumentException("CURRENT receipts lost registration");

        int metadataSize=db.get(state,topologyOwnershipKey(0x6f),new byte[0]);if(metadataSize!=RocksDB.NOT_FOUND && (metadataSize!=32 || metadataRecoveryRegistration==null || !Arrays.equals(metadataRecoveryRegistration.digest(),db.get(state,topologyOwnershipKey(0x6f)))))throw new IllegalArgumentException("Metadata recovery lacks exact registry context");
        if(metadataSize==RocksDB.NOT_FOUND && metadataRecoveryRegistration!=null)throw new IllegalArgumentException("Metadata recovery lost its durable pointer");
        if(metadataRecoveryRegistration!=null && (!metadataRecoveryRegistration.groupId().equals(groupId) || metadataRecoveryRegistration.range().rangeEpoch()!=rangeEpoch))throw new IllegalArgumentException("Metadata registration changed native group identity");
        metadataRecoveryReady=metadataRecoveryRegistration!=null && Arrays.equals(db.get(state,topologyOwnershipKey(0x70)),metadataRecoveryRegistration.digest());
        int recoverySize=db.get(state,topologyOwnershipKey(0x6d),new byte[0]);if(recoverySize>NativeTopologyRecoveryFence.MAX_BYTES)throw new IllegalArgumentException("Recovery fence exceeds bound");byte[] recovery=recoverySize==RocksDB.NOT_FOUND?null:db.get(state,topologyOwnershipKey(0x6d));topologyRecoveryFence=recovery==null?null:NativeTopologyRecoveryFence.decode(recovery);if(topologyRecoveryFence!=null)topologyRecoveryFence.requireGroup(groupId,rangeEpoch);
        byte[] learner=db.get(state,topologyOwnershipKey(0x67));topologyLearner=learner==null ? null : NativeTopologyLearnerRegistration.decode(learner);
        if(topologyLearner!=null) { var range=topologyLearner.definition().targets().getFirst();if(!range.groupId().equals(groupId) || range.rangeEpoch()!=rangeEpoch || !topologyLearner.schema().domainId().equals(interval.domainId()) || !Arrays.equals(range.lowerInclusive(),interval.lowerInclusive()) || !Arrays.equals(range.upperExclusive(),interval.upperExclusive()))throw new IllegalArgumentException("Learner registration differs from receiver ownership"); }
        byte[] permissionKey=topologyOwnershipKey(0x6a);int permissionSize=db.get(state,permissionKey,new byte[0]);
        if(permissionSize!=RocksDB.NOT_FOUND && (topologyLearner==null || permissionSize>NativeTopologyLearnerImagePermission.MAX_BYTES))throw new IllegalArgumentException("Unqualified later learner image permission");
        learnerImagePermission=permissionSize==RocksDB.NOT_FOUND ? null : NativeTopologyLearnerImagePermission.decode(db.get(state,permissionKey),topologyLearner);
        byte[] adoptionKey=topologyOwnershipKey(0x6c);int adoptionSize=db.get(state,adoptionKey,new byte[0]);if(adoptionSize!=RocksDB.NOT_FOUND && (topologyLearner==null || adoptionSize>NativeTopologyLearnerImagePermission.MAX_BYTES))throw new IllegalArgumentException("Unqualified learner schema adoption");learnerSchemaPreparation=adoptionSize==RocksDB.NOT_FOUND ? null : NativeTopologyLearnerImagePermission.decode(db.get(state,adoptionKey),topologyLearner);if(learnerSchemaPreparation!=null && !learnerSchemaPreparation.equals(learnerImagePermission))throw new IllegalArgumentException("Learner schema intent differs from exact image permission");
        byte[] birth=db.get(state,topologyOwnershipKey(0x61)),retirement=db.get(state,topologyOwnershipKey(0x62));
        if(birth!=null) { topologyBirth=NativeTopologyBirth.decode(birth); if(!topologyBirth.groupId().equals(groupId)) throw new IllegalArgumentException("Topology birth group differs from store"); }
        if(retirement!=null && retirement.length!=32) throw new IllegalArgumentException("Invalid topology retirement pointer"); topologyRetirement=retirement;
        byte[] activation=db.get(state,topologyOwnershipKey(0x63));
        if(activation!=null) { topologyActivation=NativeTopologyActivation.decode(activation); if(topologyBirth==null || !topologyBirth.equals(topologyActivation.birth())) throw new IllegalArgumentException("Topology activation lacks matching live birth"); topologyActivation.requireCut(topologyRecoveryCut()); }
    }
    private ApplyPosition topologyRecoveryCut() {
        if(applied!=null || pendingInstallation==null)return applied;
        return snapshotInstallationCut(pendingInstallation);
    }
    static ApplyPosition snapshotInstallationCut(byte[] pending) {
        if(pending==null)return null;
        if(pending.length!=48)throw new IllegalArgumentException("Invalid pending snapshot installation intent");
        var in=ByteBuffer.wrap(pending);return new ApplyPosition(in.getLong(),in.getLong());
    }
    private Map<String,byte[]> learnerSchemaRows(NativeTopologyLearnerImagePermission permission) throws RocksDBException {
        var aliases=new HashMap<>(topologyLearner.physicalNames());
        for(var row:topologySchemaRows().entrySet()){byte[] key=HexFormat.of().parseHex(row.getKey());if(key[16]==0x28 || key[16]==0x2a){var record=NativeSchemaInstallation.decode(row.getValue());aliases.putAll(record.sourceNames());aliases.putAll(record.targetNames());}}
        for(var column:columns.entrySet())aliases.put(column.getKey(),new String(column.getValue().getName(),java.nio.charset.StandardCharsets.UTF_8));
        return NativeTopologyLearnerSchema.localRows(permission.origin(),permission.definition().parents().getFirst(),permission.schema(),permission.imageCut(),permission.physicalNames(),permission.mergeColumns(),permission.sourceSchemaRows(),aliases);
    }
    /** Called only by this pool's qualified local learner preparation; the completed Raft marker stays unchanged. */
    void prepareLearnerImageSchema(NativeTopologyLearnerImagePermission permission,SchemaHandles create) throws RocksDBException {
        nativeLifecycle.writeLock().lock();applyLane.lock();try {
            requireSnapshotReleaseHealthy();if(closing)throw new IllegalStateException("Range is closing");permission.requireRegistration(Objects.requireNonNull(topologyLearner,"No learner registration"));
            if(pendingInstallation!=null || snapshotPins.get()!=0 || preparedIndex.records().stream().anyMatch(p->!isFinalized(p.preparation().transactionId())) || snapshotGates.values().stream().anyMatch(SnapshotGate::active) || activeSchemaFence!=null || activeTopologyFreeze!=null)throw new IllegalStateException("Learner schema native owners have not drained");
            if(learnerSchemaPreparation!=null && !learnerSchemaPreparation.equals(permission) || pendingLearnerImagePermission!=null && !pendingLearnerImagePermission.equals(permission))throw new IllegalArgumentException("Unknown learner adoption requires exact original retry");
            if(applied==null || permission.imageCut().index()<applied.index() || permission.imageCut().term()<applied.term() || permission.imageCut().index()==applied.index() && permission.imageCut().term()!=applied.term())throw new IllegalArgumentException("Learner adoption regresses completed prefix");
            var normalized=learnerSchemaRows(permission);var local=topologySchemaRows();
            for(var row:local.entrySet()){byte[] key=HexFormat.of().parseHex(row.getKey());if((key[16]==0x2a || key[16]==0x2b) && !Arrays.equals(row.getValue(),normalized.get(row.getKey())))throw new IllegalArgumentException("Learner adoption changed existing immutable schema history");}
            if(!Arrays.equals(schemaDigest,permission.schema().digest()) && normalized.entrySet().stream().noneMatch(row->{byte[] key=HexFormat.of().parseHex(row.getKey());return key[16]==0x2a && Arrays.equals(NativeSchemaInstallation.decode(row.getValue()).definition().sourceSchema().digest(),schemaDigest);}))throw new IllegalArgumentException("Learner adoption history does not extend current local schema");
            var target=new RangeOwnership.Interval(interval.domainId(),permission.schema().columnIds(),interval.lowerInclusive(),interval.upperExclusive());
            if(learnerSchemaLease==null)learnerSchemaLease=ownership.claimLearnerSchema(owner,interval,target);
            pendingLearnerImagePermission=permission;learnerImageForceObserver.boundary(false);
            try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(true)){batch.put(state,topologyOwnershipKey(0x6c),permission.encode());batch.put(state,topologyOwnershipKey(0x6a),permission.encode());batch.delete(state,topologyOwnershipKey(0x6b));db.write(options,batch);}learnerImageForceObserver.boundary(true);
            learnerSchemaPreparation=permission;learnerImagePermission=permission;pendingLearnerImagePermission=null;
            var next=Map.copyOf(create.create());if(!next.keySet().equals(permission.schema().columnIds()))throw new IllegalArgumentException("Prepared learner handles differ from qualified schema");db.flushWal(true);
            learnerSchemaLease.publish();learnerSchemaLease.close();learnerSchemaLease=null;
            interval=target;columns=next;deterministicMergeColumns=permission.mergeColumns();schemaDigest=permission.schema().digest();logicalSchema=permission.schema();installedGeneration=new SchemaGeneration(logicalSchema,target,next,deterministicMergeColumns);learnerServing=false;resetScopedUnderLane();
        } finally {applyLane.unlock();nativeLifecycle.writeLock().unlock();}
    }
    /** Trusted fresh catalogue/source qualification; no authority lookup occurs under this native lane. */
    public void authorizeTopologyLearnerImage(NativeTopologyLearnerImagePermission permission) throws RocksDBException {
        Objects.requireNonNull(permission);enterNative();applyLane.lock();try {
            permission.requireRegistration(Objects.requireNonNull(topologyLearner,"No learner registration"));
            if(pendingLearnerImagePermission!=null && !pendingLearnerImagePermission.equals(permission))throw new IllegalArgumentException("Unknown later image force requires exact permission retry");
            if(learnerImagePermission!=null && learnerImagePermission.equals(permission))return;
            if(pendingInstallation!=null || ownership.get(owner).state()!=RangeOwnership.State.FENCED || !topologyLearnerImageInstalledUnderLane())throw new IllegalStateException("Later image needs an installed fenced learner");
            if(!Arrays.equals(permission.schema().digest(),schemaDigest) || !permission.physicalNames().keySet().equals(columns.keySet()) || !permission.mergeColumns().equals(deterministicMergeColumns) || schemaPreparationPool==null)throw new IllegalArgumentException("Later image requires verified current local CF/schema preparation");
            for(var column:columns.entrySet())if(!permission.physicalNames().get(column.getKey()).equals(new String(column.getValue().getName(),java.nio.charset.StandardCharsets.UTF_8)))throw new IllegalArgumentException("Later image changed actual local CF alias");
            var local=topologySchemaRows();var incoming=learnerSchemaRows(permission);if(!local.keySet().equals(incoming.keySet()) || local.entrySet().stream().anyMatch(row->!Arrays.equals(row.getValue(),incoming.get(row.getKey()))))throw new IllegalArgumentException("Later image requires equivalent verified local schema history");
            if(permission.imageCut().index()<applied.index() || permission.imageCut().term()<applied.term() || permission.imageCut().index()==applied.index() && permission.imageCut().term()!=applied.term() || learnerImagePermission!=null && (permission.imageCut().index()<=learnerImagePermission.imageCut().index() || permission.catalogueCut().index()<learnerImagePermission.catalogueCut().index() || permission.catalogueCut().term()<learnerImagePermission.catalogueCut().term()))throw new IllegalArgumentException("Later image permission regresses retained authority/cut");
            pendingLearnerImagePermission=permission;learnerImageForceObserver.boundary(false);try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(true)){batch.put(state,topologyOwnershipKey(0x6a),permission.encode());batch.delete(state,topologyOwnershipKey(0x6b));db.write(options,batch);}learnerImageForceObserver.boundary(true);learnerImagePermission=permission;pendingLearnerImagePermission=null;
        } finally {applyLane.unlock();leaveNative();}
    }
    /** Read-only target qualification; source image validation remains NativeRangeImage's responsibility. */
    public void validateTopologyImageOrigin(NativeTopologyImageOrigin origin) throws IOException,RocksDBException {
        enterNative();applyLane.lock();try { qualifyTopologyImageOrigin(origin); } finally { applyLane.unlock();leaveNative(); }
    }
    private NativeTopologyActivation qualifyTopologyImageOrigin(NativeTopologyImageOrigin origin) throws IOException,RocksDBException {
        if(topologyRetired())throw new IOException("Retired topology parent cannot restore USER image");
        if(currentRangeRecovery!=null){if(!Objects.equals(origin,currentRangeRecovery.origin()))throw new IOException("CURRENT image changed qualified source provenance");return origin==null?null:origin.activation();}
        if(topologyLearner!=null) { if(learnerImagePermission!=null && Objects.equals(origin,learnerImagePermission.origin()))return topologyLearner.origin()==null ? null : topologyLearner.origin().activation();if(topologyLearner.origin()==null) { if(origin!=null)throw new IOException("Ordinary learner source acquired foreign birth provenance");return null; }if(origin==null)throw new IOException("Learner image lacks original topology provenance");try { origin.requireLearner(topologyLearner);return origin.activation(); } catch(IllegalArgumentException invalid) { throw new IOException("Learner image changed qualified source origin",invalid); } }
        if(topologyBirth==null) { if(origin!=null)throw new IOException("Topology image requires an existing qualified original member birth");return null; }
        if(origin==null || schemaPreparationPool==null)throw new IOException("Born-child image requires self-contained qualified topology origin");
        try {
            origin.requireImage(groupId,rangeEpoch,interval.domainId(),interval.lowerInclusive(),interval.upperExclusive(),origin.activation().position());
            var seed=schemaPreparationPool.topologySeed(topologyBirth.intentId());if(seed==null)throw new IllegalArgumentException("Target original local seed missing");topologyBirth.requireSeed(seed);var local=origin.localActivation(seed);
            if(topologyActivation!=null && !Arrays.equals(topologyActivation.encode(),local.encode()))throw new IllegalArgumentException("Topology image changes target original activation/cut/publication");
            var cut=topologyRecoveryCut();if(configurationUnderAdmission()==null || cut==null || cut.term()<=0 || cut.index()<0)throw new IllegalArgumentException("Topology image requires configured positive original local child prefix");
            return local;
        } catch(IllegalArgumentException invalid) { throw new IOException("Topology image differs from retained local birth",invalid); }
    }
    public NativeTopologyBirth topologyBirth() { return topologyBirth; }
    public NativeTopologyActivation topologyActivation() { return topologyActivation; }
    public boolean topologyBirthPending() { return topologyLearner!=null && (learnerSchemaPreparation!=null || !learnerServing) || topologyBirth!=null && topologyRetirement==null && topologyActivation==null; }
    public boolean topologyRecoveryOnly(){return topologyRecoveryFence!=null || currentRangeRecovery!=null && !currentRangeRecoveryReady;}
    public NativeTopologyRecoveryFence topologyRecoveryFence(){return topologyRecoveryFence;}
    private void requireServing(){if(topologyRecoveryOnly())throw new IllegalStateException("Topology lineage recovery is replay-only");}
    /** Local trusted authority has qualified current catalogue placement and actual completed configuration. */
    public void completeTopologyRecovery(NativeTopologyRecoveryFence expected,TopologyChange.Definition definition,ApplyPosition requiredCut,Configuration expectedConfiguration) throws RocksDBException {
        Objects.requireNonNull(expected);enterNative();applyLane.lock();try {
            if(topologyRecoveryFence==null && Arrays.equals(db.get(state,topologyOwnershipKey(0x6e)),expected.encode()))return;
            if(!expected.equals(topologyRecoveryFence))throw new IllegalArgumentException("Recovery completion changed original fence");
            expected.header().requireDefinition(definition);expected.requireGroup(groupId,rangeEpoch);
            if(definition.targets().stream().noneMatch(r->r.groupId().equals(groupId)) || topologyRetired() || topologyActivation==null || topologyBirthPending())throw new IllegalStateException("Only the current activated leaf may serve");
            topologyActivation.header().requireDefinition(definition);if(logicalSchema==null)throw new IllegalStateException("Recovery leaf lacks native schema");logicalSchema.requireMatches(definition.targetDirectory());
            requireActive();if(applied==null || applied.term()<requiredCut.term() || applied.index()<requiredCut.index() || applied.index()==requiredCut.index() && applied.term()!=requiredCut.term())throw new IllegalArgumentException("Recovery lacks covering native prefix");
            var cfg=configurationUnderAdmission();if(cfg==null || expectedConfiguration==null || cfg.term()!=expectedConfiguration.term() || cfg.index()!=expectedConfiguration.index() || !Arrays.equals(cfg.data(),expectedConfiguration.data()))throw new IllegalArgumentException("Recovery configuration changed");
            try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(true)){batch.put(state,topologyOwnershipKey(0x6e),expected.encode());batch.delete(state,topologyOwnershipKey(0x6d));db.write(options,batch);}topologyRecoveryFence=null;
        }finally{applyLane.unlock();leaveNative();}
    }
    public boolean topologyRetired() { return topologyRetirement!=null || metadataRecoveryRegistration!=null; }
    public NativeMetadataRecoveryRegistration metadataRecoveryRegistration(){return metadataRecoveryRegistration;}
    public boolean metadataRecoveryImageInstalled()throws RocksDBException{return readSchemaMetadata(this::metadataImageInstalledUnderLane);}
    public boolean metadataRecoveryPending(){return metadataRecoveryRegistration!=null && !metadataRecoveryReady;}
    private void requireMetadataRecoveryReady(){var registration=metadataRecoveryRegistration;if(registration!=null && (!metadataRecoveryReady || pendingInstallation!=null || applied==null || !NativeMetadataRecoveryRegistration.covers(applied,registration.imageCut())))throw new IllegalStateException("Source metadata recovery has not qualified its current completed prefix");}
    private static byte[] metadataImageReceipt(NativeMetadataRecoveryRegistration registration){return ByteBuffer.allocate(48).putLong(registration.imageCut().term()).putLong(registration.imageCut().index()).put(registration.imageHash()).array();}
    private boolean metadataImageInstalledUnderLane()throws RocksDBException {
        var registration=metadataRecoveryRegistration;return registration!=null && pendingInstallation==null && applied!=null && NativeMetadataRecoveryRegistration.covers(applied,registration.imageCut())
                && Arrays.equals(db.get(state,topologyOwnershipKey(0x71)),metadataImageReceipt(registration));
    }
    @FunctionalInterface interface MetadataRecoveryForce {void force()throws RocksDBException;}
    /** Pool mutation ownership supplies the one durable registry/registration force before local publication. */
    void prepareMetadataRecovery(NativeMetadataRecoveryRegistration registration,MetadataRecoveryForce force)throws RocksDBException {
        Objects.requireNonNull(registration);Objects.requireNonNull(force);requireSnapshotReleaseHealthy();
        if(!nativeLifecycle.writeLock().tryLock())throw new IllegalStateException("Actual original native users have not drained");
        applyLane.lock();try {
            if(closing || snapshotPins.get()!=0)throw new IllegalStateException("Actual original native snapshots have not drained");
            var previous=metadataRecoveryRegistration;if(previous!=null)registration.requireSuccessorOf(previous);
            if(pendingInstallation!=null && !registration.equals(previous))throw new IllegalStateException("Uncertain metadata install requires original image retry");
            if(applied!=null && !NativeMetadataRecoveryRegistration.covers(registration.imageCut(),applied))throw new IllegalArgumentException("Metadata image regresses completed native prefix");
            var descriptor=registration.range();var target=new RangeOwnership.Interval(registration.sourceDirectory().domainId(),registration.sourceDirectory().columns(),descriptor.lowerInclusive(),descriptor.upperExclusive());
            if(!groupId.equals(registration.groupId()) || rangeEpoch!=descriptor.rangeEpoch())throw new IllegalArgumentException("Metadata registration changed source owner");
            ownership.validateMetadataRecovery(owner,target);byte[] digest=registration.sourceDirectory().schemaDigest();
            // Fence first. A failed/unknown force must never reopen the previous USER owner.
            metadataRecoveryRegistration=registration;metadataRecoveryReady=false;
            force.force();ownership.recoverMetadata(owner,target);interval=target;schemaDigest=digest;columns=Map.of();deterministicMergeColumns=registration.mergeColumns();topologyRecoveryFence=null;
            retiredMetadataOnly=true;installedGeneration=null;logicalSchema=null;retiredRoutingPrefix=registration.sourceDirectory().routingPrefixBytes();
        }finally{applyLane.unlock();nativeLifecycle.writeLock().unlock();}
    }
    /** Fresh source-group quorum/configuration qualification is supplied by the trusted adapter. */
    public void completeMetadataRecovery(NativeMetadataRecoveryRegistration expected,ApplyPosition requiredCut,Configuration requiredConfiguration)throws RocksDBException {
        Objects.requireNonNull(expected);Objects.requireNonNull(requiredCut);Objects.requireNonNull(requiredConfiguration);enterNative();applyLane.lock();try {
            if(!expected.equals(metadataRecoveryRegistration))throw new IllegalArgumentException("Metadata completion changed original recovery registration");
            requireTopologyMetadataOwner(false);if(!metadataImageInstalledUnderLane() || !NativeMetadataRecoveryRegistration.covers(requiredCut,expected.imageCut()) || !NativeMetadataRecoveryRegistration.covers(applied,requiredCut))throw new IllegalStateException("Metadata recovery lacks actual current completed prefix");
            var configuration=configurationUnderAdmission();if(configuration==null || configuration.term()!=requiredConfiguration.term() || configuration.index()!=requiredConfiguration.index() || !Arrays.equals(configuration.data(),requiredConfiguration.data()))throw new IllegalArgumentException("Metadata source configuration changed");
            byte[] receipt=expected.digest();try(var options=new WriteOptions().setDisableWAL(false).setSync(true)){db.put(state,options,topologyOwnershipKey(0x70),receipt);}metadataRecoveryReady=true;
        }finally{applyLane.unlock();leaveNative();}
    }
    void publishTopologyRetirement(NativeTopologySeed seed) { topologyRetirement=seed.digest(); }
    /** Metadata-only parent/birth status at its own completed prefix; no USER snapshot is acquired. */
    public RangeStateView retiredOutcomeState() throws RocksDBException {
        return readSchemaMetadata(() -> {
            if(!topologyRetired() && !topologyBirthPending()) throw new IllegalStateException("Range is not an inactive topology owner");
            return metadataOutcomeStateUnderLane();
        });
    }
    /** Trusted outcome/status metadata only; does not grant USER access or acquire a native snapshot. */
    public RangeStateView metadataOutcomeState() throws RocksDBException { return readSchemaMetadata(this::metadataOutcomeStateUnderLane); }
    private RangeStateView metadataOutcomeStateUnderLane() throws RocksDBException {
        requireMetadataRecoveryReady();
        byte[] marker=db.get(state,markerKey);if(!Objects.equals(marker==null ? null : decodePosition(marker),applied))throw new IllegalStateException("Metadata marker differs from completed prefix");
        return new RangeStateView(groupId,rangeEpoch,interval.domainId(),schemaDigest,logicalColumns(),interval.lowerInclusive(),interval.upperExclusive(),applied,minimumSessionEpoch(null),sessionAuthority(null));
    }
    public long retiredAcknowledgedThrough(OperationId operation) throws RocksDBException { return readSchemaMetadata(() -> { requireMetadataRecoveryReady();if(!topologyRetired()) throw new IllegalStateException("Parent is not retired"); return acknowledgedThrough(operation,null); }); }

    private NativeTopologySeed requireTopologyActivationSeed(boolean fresh) throws RocksDBException {
        if(topologyBirth==null || schemaPreparationPool==null) throw new IllegalStateException("Topology activation requires retained local child birth");
        var seed=schemaPreparationPool.topologySeed(topologyBirth.intentId());
        if(seed==null) throw new IllegalStateException("Topology activation seed is absent");
        var target=topologyBirth.resolveDescriptor(seed);
        if(target.rangeEpoch()!=rangeEpoch || !target.groupId().equals(groupId) || !seed.header().domainId().equals(interval.domainId()) || fresh && (!Arrays.equals(seed.header().schemaDigest(),schemaDigest) || !seed.definition().targetDirectory().columns().equals(columns.keySet())) || !Arrays.equals(target.lowerInclusive(),interval.lowerInclusive()) || !Arrays.equals(target.upperExclusive(),interval.upperExclusive())) throw new IllegalArgumentException("Topology activation child geometry/schema differs");
        var cut=fresh ? applied : topologyRecoveryCut();
        if(cut==null || cut.term()<=0 || cut.index()<0 || configurationUnderAdmission()==null) throw new IllegalStateException("Topology activation requires own committed Raft prefix/configuration");
        return seed;
    }
    void validateTopologyActivationRecovery() throws RocksDBException {
        if(retiredMetadataOnly){if(!topologyRetired())throw new IllegalArgumentException("Metadata-only owner lost durable retirement/recovery authority");return;}
        if(currentRangeRecovery!=null){
            if(topologyLearner!=null || metadataRecoveryRegistration!=null)throw new IllegalArgumentException("CURRENT recovery acquired foreign ownership");var origin=currentRangeRecovery.origin();
            if(origin==null ? topologyBirth!=null || topologyActivation!=null : topologyBirth!=null && !topologyBirth.equals(origin.activation().birth()) || topologyActivation!=null && !Arrays.equals(topologyActivation.encode(),origin.activation().encode()))throw new IllegalArgumentException("CURRENT recovery changed original source activation");
            if(currentRangeRecoveryReady && !currentImageInstalledUnderLane())throw new IllegalArgumentException("CURRENT completion lost actual image");return;
        }
        if(topologyLearner!=null) {
            if(topologyRetired())throw new IllegalArgumentException("Learner cannot adopt source retirement");
            if(topologyLearner.origin()==null) { if(topologyBirth!=null || topologyActivation!=null)throw new IllegalArgumentException("Ordinary learner acquired foreign birth/activation"); }
            else if(topologyBirth!=null && !topologyBirth.equals(topologyLearner.origin().activation().birth()) || topologyActivation!=null && !Arrays.equals(topologyActivation.encode(),topologyLearner.origin().activation().encode()))throw new IllegalArgumentException("Learner changed original birth/activation owner evidence");
            var cut=topologyRecoveryCut();if(topologyActivation!=null)topologyActivation.requireCut(cut);if(cut!=null && (topologyActivation!=null || db.get(state,topologyOwnershipKey(0x69))!=null)) { var imageCut=topologyLearner.imageCut();if(imageCut.index()>cut.index() || imageCut.term()>cut.term() || imageCut.index()==cut.index() && imageCut.term()!=cut.term())throw new IllegalArgumentException("Learner completed prefix precedes registered image"); }return;
        }
        if(topologyActivation!=null) { topologyActivation.requireSeed(requireTopologyActivationSeed(false)); topologyActivation.requireCut(topologyRecoveryCut()); }
    }
    private byte[] relocationCompletionKey(UUID intent) {return ByteBuffer.allocate(33).put(markerKey).put((byte)0x68).putLong(intent.getMostSignificantBits()).putLong(intent.getLeastSignificantBits()).array();}
    private void recoverRelocationCompletions() throws RocksDBException {
        var found=new HashMap<UUID,NativeTopologyRelocationCompletion>();NativeTopologyRelocationCompletion latest=null;long bytes=0;
        try(var iterator=databaseOwner.newIterator(state)){byte[] prefix=ByteBuffer.allocate(17).put(markerKey).put((byte)0x68).array();iterator.seek(prefix);while(iterator.isValid()){byte[] key=iterator.key();if(key.length<17 || !Arrays.equals(key,0,17,prefix,0,17))break;int size=iterator.value(new byte[0]);if(key.length!=33 || size>NativeTopologyRelocationCompletion.MAX_BYTES || (bytes+=size)>16*1024*1024 || found.size()>=64 || topologyLearner==null)throw new IllegalStateException("Relocation completion history exceeds qualified bounds");var receipt=NativeTopologyRelocationCompletion.decode(iterator.value());receipt.requireRegistration(topologyLearner);receipt.requireCut(topologyRecoveryCut());if(!Arrays.equals(key,relocationCompletionKey(receipt.definition().intentId())) || found.put(receipt.definition().intentId(),receipt)!=null)throw new IllegalStateException("Relocation completion history identity changed");if(latest==null || receipt.position().index()>latest.position().index())latest=receipt;iterator.next();}iterator.status();}
        relocationCompletions=Map.copyOf(found);relocationCompletion=latest;qualifyLearnerServing();
    }
    private void qualifyLearnerServing() throws RocksDBException {
        var cfg=configurationUnderAdmission();learnerServing=learnerSchemaPreparation==null && topologyLearnerImageInstalledUnderLane() && relocationCompletion!=null && relocationCompletion.writable() && cfg!=null && Arrays.equals(cfg.data(),relocationCompletion.configuration()) && applied!=null && relocationCompletion.position().index()<=applied.index() && relocationCompletion.position().term()<=applied.term();
        if(topologyLearner!=null && !learnerServing && ownership.get(owner).state()==RangeOwnership.State.ACTIVE)ownership.fence(owner);
    }
    /** Trusted committed publication, after actual stable configuration and fresh catalogue qualification. */
    public NativeTopologyRelocationCompletion applyTopologyRelocationCompletion(ApplyPosition position,TopologyChange.Definition definition,byte[] targetConfiguration,UUID catalogue,ApplyPosition publicationCut) throws RocksDBException {
        enterNative();applyLane.lock();try {
            if(topologyLearner==null)throw new IllegalStateException("Relocation receiver has no original learner registration");var saved=relocationCompletions.get(definition.intentId());if(saved!=null){saved.requireRequest(definition,targetConfiguration,catalogue,publicationCut);advanceCompletedMarker(position);return saved;}
            requireTopologyLearnerImage();var cfg=configurationUnderAdmission();if(cfg.term()<1 || cfg.index()<topologyLearner.imageCut().index() || !Arrays.equals(cfg.data(),targetConfiguration) || !Arrays.equals(definition.header().schemaDigest(),schemaDigest) || definition.header().schemaEpoch()!=installedSchema().schemaEpoch())throw new IllegalArgumentException("Relocation publication differs from native current config/schema");requireNext(position);var receipt=new NativeTopologyRelocationCompletion(topologyLearner,definition,catalogue,publicationCut,position,new ApplyPosition(cfg.term(),cfg.index()),cfg.data());
            if(relocationCompletion!=null && definition.sourceDirectory().generation()<relocationCompletion.definition().targetDirectory().generation())throw new IllegalArgumentException("Relocation completion regresses published directory");long bytes=receipt.encode().length;for(var existing:relocationCompletions.values())bytes+=existing.encode().length;if(relocationCompletions.size()>=64 || bytes>16*1024*1024)throw new IllegalStateException("Relocation completion history capacity exhausted");
            try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(false)){batch.put(state,relocationCompletionKey(definition.intentId()),receipt.encode());batch.put(state,markerKey,encodePosition(position));batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position));db.write(options,batch);}var next=new HashMap<>(relocationCompletions);next.put(definition.intentId(),receipt);relocationCompletions=Map.copyOf(next);relocationCompletion=receipt;applied=position;qualifyLearnerServing();
            if(learnerServing)ownership.activate(owner);else if(ownership.get(owner).state()==RangeOwnership.State.ACTIVE)ownership.fence(owner);return receipt;
        } finally {applyLane.unlock();leaveNative();}
    }
    /** Trusted committed activation; fresh catalogue/membership authority is qualified by the adapter. */
    public NativeTopologyActivation applyTopologyActivation(ApplyPosition position,TopologyChange.Definition definition,byte[] commonFreezeDigest,UUID catalogueGroup,ApplyPosition catalogueCut,byte[] publicationCommitment) throws RocksDBException {
        Objects.requireNonNull(position); enterNative(); applyLane.lock();
        try {
            if(topologyRetired()) throw new IllegalStateException("Retired topology source cannot activate");
            var seed=requireTopologyActivationSeed(topologyActivation==null); seed.header().requireDefinition(definition);
            if(!Arrays.equals(seed.authority().freezeDigest(),commonFreezeDigest)) throw new IllegalArgumentException("Topology activation changes common parent freeze");
            if(topologyActivation!=null) {
                topologyActivation.requireSeed(seed); topologyActivation.requireRequest(definition,commonFreezeDigest,catalogueGroup,catalogueCut,publicationCommitment);
                if(position.index()<topologyActivation.position().index() || position.index()==topologyActivation.position().index() && position.term()!=topologyActivation.position().term()) throw new IllegalArgumentException("Topology activation changes original position");
                advanceCompletedMarker(position); return topologyActivation;
            }
            synchronized(ownership) {
            if(ownership.get(owner).state()!=RangeOwnership.State.FENCED) throw new IllegalStateException("Topology activation requires fenced child");
            requireNext(position); ensureReceiptUsage(); var receipt=new NativeTopologyActivation(seed,topologyBirth,position,catalogueGroup,catalogueCut,publicationCommitment); var history=prepareScopedWrites(false,List.of(),Map.of());
            Runnable publishCdc=()->{};
            try(var lease=ownership.claimInstall(owner);var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(true)) {
                if(cdc!=null)publishCdc=cdc.importedBaseline(batch,receipt.position(),cdcAuthorityTime()); batch.put(state,topologyOwnershipKey(0x63),receipt.encode()); batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            publishCdc.run();topologyActivation=receipt; publishScoped(position,history,false); applied=position; ownership.activate(owner); return receipt;
            }
        } finally { applyLane.unlock(); leaveNative(); }
    }

    private byte[] topologyFreezeKey() { return ByteBuffer.allocate(17).put(markerKey).put((byte)0x60).array(); }
    private void requireTopologyWritable() { if(activeTopologyFreeze!=null || topologyPreparation!=null) throw new IllegalStateException("Topology freeze blocks new writable admission"); }
    private void requireTopologyDefinition(TopologyChange.Definition definition,byte[] admission) {
        if(admission==null || admission.length!=32 || definition.kind()==TopologyChange.Kind.RELOCATE) throw new IllegalArgumentException("Topology FREEZE requires split/merge and exact admission");
        var header=definition.header(); var schema=logicalSchema;
        if(schema==null || catalogueScopeCompaction || homeRecoveryContext!=null || snapshotHomeContext!=null || activeSchemaFence!=null || schemaCaptureGate!=null || activeSchemaInstallation!=null && activeSchemaActivation==null || importPending()) throw new IllegalStateException("Topology FREEZE requires active qualified DATA generation");
        schema.requireMatches(definition.sourceDirectory());
        var parent=definition.parents().stream().filter(p -> p.groupId().equals(groupId)).findFirst().orElseThrow(() -> new IllegalArgumentException("Store is not a topology parent"));
        if(parent.rangeEpoch()!=rangeEpoch || !header.domainId().equals(interval.domainId()) || !Arrays.equals(header.schemaDigest(),schemaDigest)
                || !Arrays.equals(parent.lowerInclusive(),interval.lowerInclusive()) || !Arrays.equals(parent.upperExclusive(),interval.upperExclusive())) throw new IllegalArgumentException("Topology parent differs from native ownership/schema/bounds");
    }
    private NativeTopologyFreeze loadTopologyFreeze(ApplyPosition cut) { return loadTopologyFreeze(cut,true); }
    private NativeTopologyFreeze loadTopologyFreeze(ApplyPosition cut,boolean checkWork) {
        NativeTopologyFreeze freeze=null; byte[] high=topologyFreezeKey(); high[16]=0x61;
        try(var lower=new org.rocksdb.Slice(topologyFreezeKey());var upper=new org.rocksdb.Slice(high);var options=new ReadOptions().setFillCache(false).setIterateLowerBound(lower).setIterateUpperBound(upper);var iterator=databaseOwner.newIterator(state,options)) {
            iterator.seekToFirst(); var sizing=new byte[0];
            while(iterator.isValid()) { if(iterator.key(sizing)!=17 || iterator.value(sizing)>NativeTopologyFreeze.MAX_BYTES || freeze!=null) throw new IllegalArgumentException("Invalid native topology FREEZE namespace/count/budget"); freeze=NativeTopologyFreeze.decode(iterator.value()); iterator.next(); }
            iterator.status();
        } catch(RocksDBException failure) { throw new IllegalStateException("Cannot recover native topology FREEZE",failure); }
        if(freeze==null) return null; freeze.requireCut(cut);
        freeze.requireParent(groupId,rangeEpoch,interval.domainId(),schemaDigest,interval.lowerInclusive(),interval.upperExclusive());
        if(activeSchemaFence!=null || catalogueScopeCompaction || homeRecoveryContext!=null || snapshotHomeContext!=null || checkWork && (preparedIndex.records().stream().anyMatch(p -> !isFinalized(p.preparation().transactionId())) || snapshotGates.values().stream().anyMatch(SnapshotGate::active))) throw new IllegalArgumentException("Frozen topology retains incompatible native work/role");
        if(logicalSchema!=null && (!freeze.header().clusterId().equals(logicalSchema.clusterId()) || !freeze.header().databaseId().equals(logicalSchema.databaseId()) || freeze.header().schemaEpoch()!=logicalSchema.schemaEpoch())) throw new IllegalArgumentException("Frozen topology namespace differs from qualified schema");
        return freeze;
    }
    private static final class TopologyPreparation {
        final TopologyChange.Header header; final byte[] admission; int observers;
        final java.util.concurrent.CompletableFuture<Void> ready=new java.util.concurrent.CompletableFuture<>();
        TopologyPreparation(TopologyChange.Header header,byte[] admission) { this.header=header; this.admission=admission.clone(); }
        void requireRequest(TopologyChange.Header header,byte[] admission) { if(!Arrays.equals(this.header.encode(),header.encode()) || !Arrays.equals(this.admission,admission)) throw new IllegalArgumentException("Topology preparation changed original admission"); }
    }
    public record TopologyFreezeState(NativeTopologyFreeze freeze,int writablePins,int preparedParticipants,int snapshotGates,boolean ready) {}
    private TopologyFreezeState topologyFreezeStateUnderLane() {
        int pins=trackedSnapshotPins.get(),prepared=(int)preparedIndex.records().stream().filter(p -> !isFinalized(p.preparation().transactionId())).count(),gates=(int)snapshotGates.values().stream().filter(SnapshotGate::active).count();
        return new TopologyFreezeState(activeTopologyFreeze,pins,prepared,gates,(activeTopologyFreeze!=null || topologyPreparation!=null) && pendingInstallation==null && !importPending() && pins==0 && prepared==0 && gates==0);
    }
    public TopologyFreezeState topologyFreezeState() { enterNative(); applyLane.lock(); try { return topologyFreezeStateUnderLane(); } finally { applyLane.unlock(); leaveNative(); } }
    public NativeTopologyFreeze topologyFreeze() { return activeTopologyFreeze; }
    public NativeTopologyFreeze lookupTopologyFreeze(TopologyChange.Header header,byte[] admission) { var freeze=activeTopologyFreeze; if(freeze!=null) freeze.requireRequest(header,admission); return freeze; }
    public final class TopologyFreezeGate implements AutoCloseable {
        private final TopologyPreparation preparation; private volatile boolean closed;
        private TopologyFreezeGate(TopologyPreparation preparation) { this.preparation=preparation; }
        boolean belongsToPool(DistributedStoragePool pool){return schemaPreparationPool==pool;}
        boolean closed(){return closed;}
        public TopologyFreezeState state() { enterNative(); applyLane.lock(); try { if(closed || preparation!=null && topologyPreparation!=preparation && activeTopologyFreeze==null) throw new IllegalStateException("Topology preparation released"); return topologyFreezeStateUnderLane(); } finally { applyLane.unlock(); leaveNative(); } }
        public boolean ready() { return state().ready(); }
        public java.util.concurrent.CompletionStage<Void> whenReady() {
            applyLane.lock(); try { if(closed) throw new IllegalStateException("Topology preparation released"); return (preparation==null ? java.util.concurrent.CompletableFuture.<Void>completedFuture(null) : preparation.ready).minimalCompletionStage(); } finally { applyLane.unlock(); }
        }
        @Override public void close() {
            boolean cancel=false; applyLane.lock();
            try { if(closed) return; closed=true; if(preparation!=null && --preparation.observers==0 && topologyPreparation==preparation) { topologyPreparation=null; cancel=activeTopologyFreeze==null; } } finally { applyLane.unlock(); }
            if(cancel) preparation.ready.completeExceptionally(new java.util.concurrent.CancellationException("Topology preparation released"));
        }
    }
    private void signalTopologyDrain() {
        var preparation=topologyPreparation;
        if(preparation==null || preparation.ready.isDone() || applyLane.isHeldByCurrentThread() || nativeLifecycle.getReadHoldCount()!=0 || nativeLifecycle.isWriteLockedByCurrentThread()) return;
        boolean ready; applyLane.lock();
        try { ready=topologyPreparation==preparation && topologyFreezeStateUnderLane().ready(); } finally { applyLane.unlock(); }
        if(ready) preparation.ready.complete(null);
    }
    /** Local preparation only. The adapter reserves/drains accepted proposal predecessors before calling this seam. */
    public TopologyFreezeGate prepareTopologyFreeze(TopologyChange.Definition definition,byte[] admission) {
        Objects.requireNonNull(definition); enterNative(); applyLane.lock();
        try {
            requireActive(); requireTopologyDefinition(definition,admission);
            if(activeTopologyFreeze!=null) { activeTopologyFreeze.requireRequest(definition.header(),admission); return new TopologyFreezeGate(null); }
            if(topologyPreparation==null) topologyPreparation=new TopologyPreparation(definition.header(),admission);
            topologyPreparation.requireRequest(definition.header(),admission); if(topologyPreparation.observers>=16) throw new IllegalStateException("Topology preparation observer budget exhausted");
            topologyPreparation.observers++; return new TopologyFreezeGate(topologyPreparation);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Trusted committed parent boundary; catalogue/membership authority is supplied by the adapter, never request bytes. */
    public NativeTopologyFreeze applyTopologyFreeze(ApplyPosition position,TopologyChange.Definition definition,byte[] admission) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(definition); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage();
            if(activeTopologyFreeze!=null) { activeTopologyFreeze.requireRequest(definition.header(),admission); if(position.index()<activeTopologyFreeze.position().index() || position.index()==activeTopologyFreeze.position().index() && position.term()!=activeTopologyFreeze.position().term()) throw new IllegalArgumentException("Topology callback changes original cut"); advanceCompletedMarker(position); return activeTopologyFreeze; }
            requireTopologyDefinition(definition,admission);
            // Followers replay the trusted committed seam without a volatile leader preparation object.
            if(trackedSnapshotPins.get()!=0 || preparedIndex.records().stream().anyMatch(p -> !isFinalized(p.preparation().transactionId())) || snapshotGates.values().stream().anyMatch(SnapshotGate::active)) throw new IllegalStateException("Topology parent writable/native work has not drained");
            if(topologyPreparation!=null) topologyPreparation.requireRequest(definition.header(),admission);
            requireNext(position); var freeze=new NativeTopologyFreeze(definition.header(),groupId,position,NativeTopologyFreeze.boundsDigest(interval.lowerInclusive(),interval.upperExclusive()),admission); var history=prepareScopedWrites(false,List.of(),Map.of());
            Runnable publishCdc=()->{};
            try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null)publishCdc=cdc.append(batch,it.cavallium.rockserver.core.common.distributed.DistributedCdcRecord.terminalBarrier(freeze)); batch.put(state,topologyFreezeKey(),freeze.encode()); batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            publishCdc.run();activeTopologyFreeze=freeze; publishScoped(position,history,false); applied=position; return freeze;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }

    private byte[] schemaFenceKey() { return ByteBuffer.allocate(17).put(markerKey).put((byte)0x27).array(); }
    private void requireSchemaAdmission() {
        if (activeSchemaFence!=null) throw new IllegalStateException("Schema change blocks new DATA admission");
    }
    private void requireSchemaFenceSchema(NativeSchemaFence fence,DistributedSchema schema) {
        if (!fence.clusterId().equals(schema.clusterId()) || !fence.databaseId().equals(schema.databaseId())
                || !fence.domainId().equals(schema.domainId()) || fence.sourceSchemaEpoch()!=schema.schemaEpoch()
                || !Arrays.equals(fence.sourceSchemaDigest(),schema.digest())) throw new IllegalArgumentException("Schema fence differs from qualified logical schema");
    }
    private NativeSchemaFence loadSchemaFence(ApplyPosition cut) {
        byte[] bytes;
        try { bytes=db.get(state,schemaFenceKey()); } catch (RocksDBException failure) { throw new IllegalStateException("Cannot recover native schema fence",failure); }
        if (bytes==null) return null;
        var fence=NativeSchemaFence.decode(bytes); fence.requireCut(cut);
        if (!fence.participant().groupId().equals(groupId) || fence.participant().rangeEpoch()!=rangeEpoch
                || !fence.domainId().equals(interval.domainId()) || !knownSchema(fence.sourceSchemaDigest())
                || homeRecoveryContext!=null || snapshotHomeContext!=null) throw new IllegalArgumentException("Recovered schema fence differs from DATA ownership");
        if(logicalSchema!=null && (activeSchemaInstallation==null || activeSchemaActivation!=null)) requireSchemaFenceSchema(fence,logicalSchema);
        return fence;
    }
    public NativeSchemaFence schemaFence() { return activeSchemaFence; }
    public record SchemaFenceReadiness(NativeSchemaFence fence,boolean ready,int snapshotPins,int preparedParticipants,int snapshotGates) {}
    /** Exact native blockers at one ordered cut; ownership is never forcibly released. */
    public SchemaFenceReadiness schemaFenceReady() {
        enterNative(); applyLane.lock();
        try {
            int pins=snapshotPins.get(),prepared=(int)preparedIndex.records().stream().filter(p -> !isFinalized(p.preparation().transactionId())).count();
            int gates=(int)snapshotGates.values().stream().filter(SnapshotGate::active).count();
            return new SchemaFenceReadiness(activeSchemaFence,activeSchemaFence!=null && pendingInstallation==null && pins==0 && prepared==0 && gates==0,pins,prepared,gates);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Explicit trusted fixture path without qualified replica admission. */
    public NativeSchemaFence applySchemaFence(ApplyPosition position,SchemaChange.Header header) throws RocksDBException { return applySchemaFence(position,header,null); }
    /** Trusted committed seam; the adapter validates manifest/source authority and drains accepted predecessors before append. */
    public NativeSchemaFence applySchemaFence(ApplyPosition position,SchemaChange.Header header,byte[] admissionDigest) throws RocksDBException {return applySchemaFence(position,header,admissionDigest,false);}
    /** Trusted committed CURRENT own-tail control; foreground admission remains closed. */
    public NativeSchemaFence applyCurrentRangeRecoverySchemaFence(ApplyPosition position,SchemaChange.Header header,byte[] admissionDigest)throws RocksDBException {return applySchemaFence(position,header,admissionDigest,true);}
    private NativeSchemaFence applySchemaFence(ApplyPosition position,SchemaChange.Header header,byte[] admissionDigest,boolean recoveryTail) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(header); enterNative(); applyLane.lock();
        try {
            if(recoveryTail)requireCurrentSchemaTail();else requireActive(); ensureReceiptUsage();
            var historical=schemaInstallProofs.get(header.changeId());
            if(historical!=null) { historical.requireHeader(header); historical.fence().requireAdmissionDigest(admissionDigest); historical.requireCut(applied); advanceCompletedMarker(position); return historical.fence(); }
            var proposed=new NativeSchemaFence(header,groupId,position,admissionDigest);
            if (homeRecoveryContext!=null || snapshotHomeContext!=null || logicalSchema==null) throw new IllegalStateException("Schema fence requires qualified DATA schema");
            if (proposed.participant().rangeEpoch()!=rangeEpoch) throw new IllegalArgumentException("Schema fence participant epoch differs from ownership");
            if (activeSchemaFence!=null) {
                activeSchemaFence.requireHeader(header); activeSchemaFence.requireAdmissionDigest(admissionDigest);
                if (position.index()<activeSchemaFence.position().index() || position.index()==activeSchemaFence.position().index() && position.term()!=activeSchemaFence.position().term()) throw new IllegalArgumentException("Schema fence callback regresses or changes original position");
                advanceCompletedMarker(position); return activeSchemaFence;
            }
            requireTopologyWritable();
            if(schemaPreparationPool!=null) schemaPreparationPool.requireCapacityFence(groupId,header);
            requireSchemaFenceSchema(proposed,logicalSchema);
            requireNext(position); var history=prepareScopedWrites(false,List.of(),Map.of());
            try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) cdc.initialize(batch);
                batch.put(state,schemaFenceKey(),proposed.encode()); batch.put(state,markerKey,encodePosition(position));
                batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            activeSchemaFence=proposed; publishScoped(position,history,false); applied=position; return proposed;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }

    void preflightSchemaInstallation(NativeSchemaInstallation record) throws RocksDBException {
        enterNative(); applyLane.lock();
        try {
            var saved=savedSchemaInstallation(record.definition().header());
            if(saved!=null) {
                saved.requireDefinition(record.definition());
                if(!saved.sourceNames().equals(record.sourceNames()) || !saved.targetNames().equals(record.targetNames()) || !saved.sourceMerges().equals(record.sourceMerges()) || !saved.targetMerges().equals(record.targetMerges())) throw new IllegalArgumentException("Original INSTALL binding changed"); return;
            }
            if(activeSchemaFence==null || !Arrays.equals(schemaDigest,record.definition().sourceSchema().digest()) || activeSchemaInstallation!=null && activeSchemaActivation==null) throw new IllegalStateException("INSTALL source is not pending from an activated generation");
            activeSchemaFence.requireHeader(record.definition().header()); record.requireRange(groupId,rangeEpoch,interval.domainId(),interval.lowerInclusive(),interval.upperExclusive());
            schemaInstallationBudget(record.encode().length,record.definition().changeId());
        } finally { applyLane.unlock(); leaveNative(); }
    }
    void preflightSchemaCapacity(SchemaChange.Definition definition,int installBytes) throws RocksDBException {
        requireTopologyWritable();
        if(importPending() || ownership.get(owner).state()!=RangeOwnership.State.ACTIVE || logicalSchema==null || homeRecoveryContext!=null || snapshotHomeContext!=null || activeSchemaFence!=null || activeSchemaInstallation!=null && activeSchemaActivation==null || !Arrays.equals(schemaDigest,definition.sourceSchema().digest())) throw new IllegalStateException("Capacity admission requires active qualified source DATA");
        var participant=definition.header().participants().stream().filter(p -> p.groupId().equals(groupId)).findFirst().orElseThrow(() -> new IllegalArgumentException("Group outside original capacity membership"));
        var descriptor=definition.sourceDirectory().ranges().stream().filter(p -> p.groupId().equals(groupId)).findFirst().orElseThrow();
        if(participant.rangeEpoch()!=rangeEpoch || !interval.domainId().equals(definition.sourceSchema().domainId()) || !interval.columns().equals(definition.sourceSchema().columnIds()) || !Arrays.equals(interval.lowerInclusive(),descriptor.lowerInclusive()) || !Arrays.equals(interval.upperExclusive(),descriptor.upperExclusive())) throw new IllegalArgumentException("Capacity admission differs from native source lineage/bounds");
        schemaInstallationBudget(installBytes,definition.changeId());
    }
    private void schemaInstallationBudget(int encodedBytes,UUID except) throws RocksDBException {
        int count=0; long bytes=3L*encodedBytes+2L*NativeSchemaActivationProof.BYTES;
        if(schemaPreparationPool!=null) for(var reserved:schemaPreparationPool.outstandingCapacity(groupId,except)) { count++; bytes=Math.addExact(bytes,reserved.historyBytes()); }
        var current=db.get(state,schemaStateKey(0x28)); if(current!=null) bytes+=current.length;
        var activation=db.get(state,schemaStateKey(0x2c)); if(activation!=null) bytes+=activation.length;
        for(int tag:List.of(0x2a,0x2b)) try(var iterator=databaseOwner.newIterator(state)) {
            var prefix=schemaStateKey(tag); iterator.seek(prefix);
            while(iterator.isValid() && iterator.key().length>=prefix.length && Arrays.equals(iterator.key(),0,prefix.length,prefix,0,prefix.length)) {
                if(tag==0x2a) count++; bytes=Math.addExact(bytes,iterator.value().length); iterator.next();
            }
            iterator.status();
        }
        if(count>=64 || bytes>16L*1024*1024) throw new IllegalStateException("Retained schema lifecycle metadata budget exhausted");
    }

    @FunctionalInterface interface SchemaHandles { Map<UUID,ColumnFamilyHandle> create() throws RocksDBException; }
    NativeSchemaInstallation installSchema(NativeSchemaInstallation record,RangeOwnership.SchemaTransition transition,DistributedStoragePool.SchemaInstallObserver observer,SchemaHandles create,DistributedStoragePool.CapacityTransfer capacityTransfer) throws RocksDBException {
        nativeLifecycle.writeLock().lock(); applyLane.lock();
        try {
            requireSnapshotReleaseHealthy(); if(closing) throw new IllegalStateException("Range is closing");
            if(activeSchemaInstallation!=null && activeSchemaInstallation.definition().changeId().equals(record.definition().changeId())) {
                activeSchemaInstallation.requireDefinition(record.definition());
                if(!Arrays.equals(activeSchemaInstallation.encode(),record.encode())) throw new IllegalArgumentException("Original install identity changed");
                return activeSchemaInstallation;
            }
            if(activeSchemaFence==null) throw new IllegalStateException("Installation requires native schema fence");
            activeSchemaFence.requireHeader(record.definition().header());
            if(activeSchemaInstallation!=null && activeSchemaActivation==null) throw new IllegalStateException("Previous installed generation is not activated");
            if(!Arrays.equals(schemaDigest,record.definition().sourceSchema().digest())) throw new IllegalArgumentException("Install source schema differs");
            record.requireRange(groupId,rangeEpoch,interval.domainId(),interval.lowerInclusive(),interval.upperExclusive());
            if(snapshotPins.get()!=0 || preparedIndex.records().stream().anyMatch(p -> !isFinalized(p.preparation().transactionId()))
                    || snapshotGates.values().stream().anyMatch(SnapshotGate::active)) throw new IllegalStateException("Schema owners have not drained");
            var target=new RangeOwnership.Interval(interval.domainId(),record.targetNames().keySet(),interval.lowerInclusive(),interval.upperExclusive());
            var prior=interval;
            if(ownership.get(owner).state()==RangeOwnership.State.ACTIVE) ownership.fence(owner);
            ownership.validateSchemaReplacement(owner,prior,target,transition);
            try(var lease=ownership.claimSchemaReplacement(owner,prior,target,transition)) {
            byte[] encoded=record.encode(), pending=db.get(state,schemaStateKey(0x29));
            if(pending!=null && !Arrays.equals(pending,encoded)) throw new IllegalArgumentException("Different pending installation");
            if(pending==null) {
                requireNext(record.position()); schemaInstallationBudget(encoded.length,record.definition().changeId());
                try(var options=new WriteOptions().setDisableWAL(false).setSync(true)) { db.put(state,options,schemaStateKey(0x29),encoded); }
            }
            observer.after(DistributedStoragePool.SchemaInstallStage.INTENT);
            var nextColumns=Map.copyOf(create.create());
            observer.after(DistributedStoragePool.SchemaInstallStage.COLUMN_FAMILIES);
            if(!nextColumns.keySet().equals(record.targetNames().keySet())) throw new IllegalArgumentException("Target handles differ");
            try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(true)) {
                if(cdc!=null) cdc.initialize(batch);
                batch.put(state,schemaStateKey(0x28),encoded); batch.put(state,schemaHistoryKey(record.definition().changeId()),encoded);
                batch.delete(state,schemaStateKey(0x29)); batch.delete(state,schemaStateKey(0x2c)); batch.put(state,markerKey,encodePosition(record.position()));
                batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),record.position())); if(capacityTransfer!=null) capacityTransfer.append(batch); db.write(options,batch);
                if(capacityTransfer!=null) capacityTransfer.committed();
            }
            observer.after(DistributedStoragePool.SchemaInstallStage.COMMITTED);
            lease.publish();
            interval=target; columns=nextColumns; deterministicMergeColumns=record.targetMerges(); schemaDigest=record.definition().targetSchema().digest();
            logicalSchema=record.definition().targetSchema();
            installedGeneration=new SchemaGeneration(logicalSchema,target,nextColumns,record.targetMerges()); activeSchemaInstallation=record;
            var history=new HashMap<>(schemaHistory); history.put(schemaIdentity(record.definition().sourceSchema().digest()),record.definition().sourceSchema());
            history.put(schemaIdentity(schemaDigest),logicalSchema); schemaHistory=Map.copyOf(history);
            resetScopedUnderLane(); applied=record.position(); recoverSchemaMetadata();
            }
            if(currentRangeRecovery==null || currentRangeRecoveryReady)ownership.activate(owner); return record;
        } finally { applyLane.unlock(); nativeLifecycle.writeLock().unlock(); }
    }

    public NativeSchemaActivationProof applySchemaActivation(ApplyPosition position,NativeSchemaInstallProof expected,UUID catalogueGroup,ApplyPosition catalogueCut) throws RocksDBException {return applySchemaActivation(position,expected,catalogueGroup,catalogueCut,false);}
    public NativeSchemaActivationProof applyCurrentRangeRecoverySchemaActivation(ApplyPosition position,NativeSchemaInstallProof expected,UUID catalogueGroup,ApplyPosition catalogueCut)throws RocksDBException {return applySchemaActivation(position,expected,catalogueGroup,catalogueCut,true);}
    private NativeSchemaActivationProof applySchemaActivation(ApplyPosition position,NativeSchemaInstallProof expected,UUID catalogueGroup,ApplyPosition catalogueCut,boolean recoveryTail) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(expected); enterNative(); applyLane.lock();
        try {
            if(recoveryTail)requireCurrentSchemaTail();else requireActive(); ensureReceiptUsage(); var change=expected.fence().changeId(); var saved=schemaActivationProofs.get(change);
            if(saved!=null) {
                if(!saved.installation().equals(expected) || !saved.catalogueGroup().equals(catalogueGroup) || !saved.cataloguePosition().equals(catalogueCut)) throw new IllegalArgumentException("Original activation identity changed");
                saved.requireCut(applied); advanceCompletedMarker(position); return saved;
            }
            if(activeSchemaInstallation==null || !activeSchemaInstallation.proof().equals(expected) || activeSchemaActivation!=null
                    || activeSchemaFence==null || !activeSchemaFence.equals(expected.fence())) throw new IllegalArgumentException("Activation differs from current pending INSTALL");
            requireNext(position); var proof=new NativeSchemaActivationProof(expected,catalogueGroup,catalogueCut,position);
            var history=prepareScopedWrites(false,List.of(),Map.of());
            try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) cdc.initialize(batch);
                batch.put(state,schemaChangeKey(0x2b,change),proof.encode()); batch.put(state,schemaStateKey(0x2c),proof.encode()); batch.delete(state,schemaFenceKey());
                batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            var activations=new HashMap<>(schemaActivationProofs); activations.put(change,proof); schemaActivationProofs=Map.copyOf(activations);
            activeSchemaActivation=proof; activeSchemaFence=null; publishScoped(position,history,false); applied=position; return proof;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }

    private static final class SchemaCaptureState {
        final SchemaChange.Header header; final java.util.concurrent.CompletableFuture<Void> ready=new java.util.concurrent.CompletableFuture<>();
        int observers; boolean cancelled;
        SchemaCaptureState(SchemaChange.Header header) { this.header=header; }
    }
    private volatile SchemaCaptureState schemaCaptureGate;
    private volatile DistributedStoragePool schemaPreparationPool;
    boolean schemaPreparationBelongsTo(DistributedStoragePool pool) { return schemaPreparationPool==pool; }
    void configureSchemaPreparationPool(DistributedStoragePool pool) { schemaPreparationPool=pool; }
    public final class SchemaInstallCaptureGate implements AutoCloseable {
        private final SchemaCaptureState captured; private volatile boolean closed;
        private SchemaInstallCaptureGate(SchemaCaptureState captured) { this.captured=captured; }
        boolean belongsToPool(DistributedStoragePool pool) { return schemaPreparationPool==pool; }
        boolean closed() { return closed; }
        public boolean ready() {
            enterNative(); applyLane.lock();
            try {
                if(closed) throw new IllegalStateException("Schema capture gate is closed");
                if(captured==null || schemaInstallProofs.containsKey(captured.header.changeId())) return true;
                if(schemaCaptureGate!=captured || captured.cancelled) throw new IllegalStateException("Schema capture gate was cancelled");
                return snapshotPins.get()==0 && preparedIndex.records().stream().noneMatch(p -> !isFinalized(p.preparation().transactionId())) && snapshotGates.values().stream().noneMatch(SnapshotGate::active);
            } finally { applyLane.unlock(); leaveNative(); }
        }
        public java.util.concurrent.CompletionStage<Void> readyFuture() {
            applyLane.lock();
            try { if(closed) throw new IllegalStateException("Schema capture gate is closed"); return (captured==null ? java.util.concurrent.CompletableFuture.<Void>completedFuture(null) : captured.ready).minimalCompletionStage(); }
            finally { applyLane.unlock(); }
        }
        @Override public void close() {
            boolean cancel=false; applyLane.lock();
            try {
                if(closed) return; closed=true;
                if(captured!=null && --captured.observers==0 && schemaCaptureGate==captured) { schemaCaptureGate=null; captured.cancelled=true; cancel=true; }
            } finally { applyLane.unlock(); }
            if(schemaPreparationPool!=null) schemaPreparationPool.unregisterSchemaCapture(this);
            if(cancel) captured.ready.completeExceptionally(new java.util.concurrent.CancellationException("Schema capture gate released"));
        }
    }
    /** Reserve before qualification/append; observers never close existing native snapshot owners. */
    public SchemaInstallCaptureGate prepareSchemaInstallation(SchemaChange.Header header) throws RocksDBException {
        Objects.requireNonNull(header); enterNative(); applyLane.lock();
        try {
            requireActive(); var saved=savedSchemaInstallation(header);
            if(saved!=null) { var token=new SchemaInstallCaptureGate(null); if(schemaPreparationPool!=null) schemaPreparationPool.registerSchemaCapture(token); return token; }
            if(activeSchemaFence==null) throw new IllegalStateException("INSTALL capture admission requires native fence"); activeSchemaFence.requireHeader(header);
            if(schemaCaptureGate==null) schemaCaptureGate=new SchemaCaptureState(header);
            else if(!Arrays.equals(schemaCaptureGate.header.encode(),header.encode())) throw new IllegalArgumentException("Another schema change owns capture admission");
            if(schemaCaptureGate.observers>=16) throw new IllegalStateException("Schema capture observer capacity exhausted");
            schemaCaptureGate.observers++; var token=new SchemaInstallCaptureGate(schemaCaptureGate);
            try { if(schemaPreparationPool!=null) schemaPreparationPool.registerSchemaCapture(token); return token; }
            catch(RuntimeException failure) { token.close(); throw failure; }
        } finally { applyLane.unlock(); leaveNative(); }
    }
    private void signalSchemaCaptureDrain() {
        var capture=schemaCaptureGate;
        if(capture==null || capture.ready.isDone() || applyLane.isHeldByCurrentThread() || nativeLifecycle.getReadHoldCount()!=0 || nativeLifecycle.isWriteLockedByCurrentThread()) return;
        Throwable failure; boolean ready;
        applyLane.lock();
        try {
            if(schemaCaptureGate!=capture || capture.cancelled) return;
            failure=snapshotReleaseFailure;
            ready=snapshotPins.get()==0 && preparedIndex.records().stream().noneMatch(p -> !isFinalized(p.preparation().transactionId())) && snapshotGates.values().stream().noneMatch(SnapshotGate::active);
        } finally { applyLane.unlock(); }
        if(failure!=null) capture.ready.completeExceptionally(failure);
        else if(ready) capture.ready.complete(null);
    }

    private boolean contains(byte[] key) {
        byte[] lower = interval.lowerInclusive();
        byte[] upper = interval.upperExclusive();
        return (lower == null || Arrays.compareUnsigned(key, lower) >= 0)
                && (upper == null || Arrays.compareUnsigned(key, upper) < 0);
    }

    private volatile boolean sessionControlsEnabled;

    /** Bootstrap must install catalogue proposal authority before enabling new controls. Persisted data enforcement is unconditional. */
    public void enableSessionControls() {
        enterNative(); applyLane.lock();
        try { if(metadataRecoveryRegistration!=null){try{if(metadataImageInstalledUnderLane())requireTopologyMetadataOwner(false);}catch(RocksDBException failure){throw new IllegalStateException("Cannot configure recovering metadata controls",failure);}}else if(topologyRetired()) { try { requireTopologyMetadataOwner(); } catch(RocksDBException failure) { throw new IllegalStateException("Cannot configure sealed topology controls",failure); } } else requireActive(); sessionControlsEnabled=true; }
        finally { applyLane.unlock(); leaveNative(); }
    }

    /** Trusted CATALOGUE role configuration; request bytes never enable metadata scope compaction. */
    public void enableCatalogueScopeCompaction() {
        enterNative(); applyLane.lock();
        try {
            requireActive();
            if(activeTopologyFreeze!=null) throw new IllegalStateException("Frozen DATA parent cannot become CATALOGUE");
            if (scopedHistory!=null) throw new IllegalStateException("DATA scoped history excludes catalogue scope compaction");
            catalogueScopeCompaction=true;
        } finally { applyLane.unlock(); leaveNative(); }
    }

    /** Group-local ephemeral limits, configured once; ordinary read/export pins remain independent. */
    public void enableScopedHistory(RangeMutationHistory.Limits limits) {
        Objects.requireNonNull(limits); enterNative(); applyLane.lock();
        try {
            requireActive();
            if (catalogueScopeCompaction) throw new IllegalStateException("Catalogue scope compaction excludes DATA scoped history");
            if (scopedHistory!=null) {
                if (!scopedLimits.equals(limits)) throw new IllegalArgumentException("Scoped history limits already configured"); return;
            }
            scopedHistory=new RangeMutationHistory(limits); scopedLimits=limits;
            if (applied!=null && applied.term()>0) scopedHistory.advance(applied,List.of());
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public RangeMutationHistory.Stats scopedHistoryStats() {
        applyLane.lock(); try { if (scopedHistory==null) throw new UnsupportedOperationException("Scoped history is not enabled"); return scopedHistory.stats(); }
        finally { applyLane.unlock(); }
    }
    /** Leader-loss seam: invalidates writable-history observations without fencing follower apply. */
    public void invalidateScopedSnapshots() {
        applyLane.lock(); try { invalidateScopedUnderLane(RangeMutationHistory.Status.INVALIDATED); }
        finally { applyLane.unlock(); drainScopedReleases(); }
    }
    private void invalidateScopedUnderLane(RangeMutationHistory.Status reason) {
        if (scopedHistory==null) return;
        scopedGeneration=new Object(); scopedHistory.invalidateAll(reason);
        for (var pin:scopedPins.values()) scopedReleases.add(pin.snapshot); scopedPins.clear();
    }
    private void resetScopedUnderLane() {
        catalogueHistoryCache=null;catalogueDirectorySlots=-1;sessionAckEpochs=null;
        homeStatusCapacityKnown=false;
        if (scopedHistory==null) return;
        scopedGeneration=new Object(); scopedHistory.reset();
        for (var pin:scopedPins.values()) scopedReleases.add(pin.snapshot); scopedPins.clear();
    }
    private void retireScoped(List<RangeMutationHistory.Pin> pins) {
        for (var pin:pins) { var owned=scopedPins.remove(pin); if (owned!=null) scopedReleases.add(owned.snapshot); }
    }
    private void retireScoped(RangeMutationHistory.Pin pin) {
        var owned=scopedPins.remove(pin); if (owned!=null) scopedReleases.add(owned.snapshot);
    }
    private void drainScopedReleases() { ScopedSnapshot pin; while ((pin=scopedReleases.poll())!=null) pin.close(); }
    private RangeMutationHistory.PreparedWrites prepareScopedWrites(boolean business,List<Mutation> effects,Map<BucketKey,DistributedBucket> buckets) {
        if (scopedHistory==null) return null;
        if (!business || scopedPins.isEmpty()) return scopedHistory.prepare(List.of());
        var spans=new ArrayList<TransactionScope.Span>(effects.size()+buckets.size());
        for (var mutation:effects) spans.add(mutation.kind()==Mutation.Kind.DELETE_RANGE
                ? TransactionScope.Span.range(mutation.columnId(),mutation.key(),mutation.value()) : TransactionScope.Span.point(mutation.columnId(),mutation.key()));
        for (var key:buckets.keySet()) spans.add(TransactionScope.Span.point(key.column,key.key));
        return scopedHistory.prepare(spans);
    }
    private void publishScoped(ApplyPosition position,RangeMutationHistory.PreparedWrites writes,boolean configurationChanged) {
        if (scopedHistory==null) return;
        try {
            if (configurationChanged || closing) invalidateScopedUnderLane(RangeMutationHistory.Status.INVALIDATED);
            retireScoped(scopedHistory.advance(position,writes));
        } catch (RuntimeException failure) {
            // Native data is already committed: no old handle may certify against a partial publication.
            resetScopedUnderLane();
        }
    }
    /** Install exact immutable decoding material before accepting/replaying bucket operations. This grants no schema activation authority. */
    public void enableLogicalSchema(DistributedSchema schema) {
        Objects.requireNonNull(schema); enterNative(); applyLane.lock();
        try {
            var current=ownership.get(owner).state();
            if (current!=RangeOwnership.State.ACTIVE && current!=RangeOwnership.State.FENCED && !(current==RangeOwnership.State.RETIRED && topologyRetired())) throw new IllegalStateException("Range cannot install decoding schema");
            int prefix=schema.routingPrefixBytes();
            if (!Arrays.equals(schemaDigest,schema.digest()) || !interval.domainId().equals(schema.domainId()) || !interval.columns().equals(schema.columnIds())
                    || logicalSchema!=null && !Arrays.equals(logicalSchema.digest(),schema.digest())
                    || prefix>0 && (interval.lowerInclusive()!=null && interval.lowerInclusive().length!=prefix
                            || interval.upperExclusive()!=null && interval.upperExclusive().length!=prefix)) throw new IllegalArgumentException("Logical schema differs from native ownership");
            if (activeSchemaFence!=null && activeSchemaInstallation==null) requireSchemaFenceSchema(activeSchemaFence,schema);
            if(activeTopologyFreeze!=null && (!activeTopologyFreeze.header().clusterId().equals(schema.clusterId()) || !activeTopologyFreeze.header().databaseId().equals(schema.databaseId()) || activeTopologyFreeze.header().schemaEpoch()!=schema.schemaEpoch())) throw new IllegalArgumentException("Topology FREEZE qualified schema changed");
            logicalSchema=schema; installedGeneration=new SchemaGeneration(schema,interval,columns,deterministicMergeColumns);
            for (var prepared:preparedIndex.records()) requirePreparation(prepared.preparation(),true);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    private void validateLogical(DistributedCommand command) { validateLogical(command,logicalSchema); }
    private void validateLogical(DistributedCommand command,DistributedSchema schema) {
         if (schema==null) throw new UnsupportedOperationException("Bucket decoding schema is not installed");
        for (var mutation:command.mutations()) {
            if (mutation.kind()==Mutation.Kind.DELETE_RANGE) continue; // Bounds address whole physical rows, not member keys or stored values.
            var shape=schema.column(mutation.columnId()).schema(); boolean bucket=shape.variableLengthKeysCount()!=0;
            if (bucket!=mutation.bucket()) throw new IllegalArgumentException("Bucket mutation differs from qualified column kind");
            if(mutation.kind()==Mutation.Kind.BUCKET_MERGE && schema.column(mutation.columnId()).operator()==null) throw new IllegalArgumentException("BUCKET_MERGE requires activated logical operator");
            if (bucket) schema.requireBucketKey(mutation.columnId(),mutation.key(),mutation.memberKeys());
            else schema.requireFixedKey(mutation.columnId(),mutation.key());
            if (!shape.hasValue() && mutation.value()!=null && mutation.value().length!=0) throw new IllegalArgumentException("Key-only mutation has value bytes");
        }
        for (var predicate:command.predicates()) {
            var shape=schema.column(predicate.columnId()).schema(); boolean bucket=shape.variableLengthKeysCount()!=0;
            if (bucket!=predicate.bucket()) throw new IllegalArgumentException("Bucket predicate differs from qualified column kind");
            if (bucket) schema.requireBucketKey(predicate.columnId(),predicate.key(),predicate.memberKeys());
            else schema.requireFixedKey(predicate.columnId(),predicate.key());
            if (!shape.hasValue() && predicate.expectedValue()!=null && predicate.expectedValue().length!=0) throw new IllegalArgumentException("Key-only predicate has value bytes");
        }
    }
    public boolean currentSchemaControl(DistributedCommand command) {
        return command.sessionControl()!=null && groupId.equals(command.groupId()) && rangeEpoch==command.rangeEpoch() && Arrays.equals(schemaDigest,command.schemaDigest());
    }
    public boolean pendingSchemaControl(DistributedCommand command) {
        var fence=activeSchemaFence;
        return command.sessionControl()!=null && groupId.equals(command.groupId()) && rangeEpoch==command.rangeEpoch() && fence!=null && Arrays.equals(fence.sourceSchemaDigest(),command.schemaDigest());
    }
    public boolean metadataSchemaControl(DistributedCommand command) { return currentSchemaControl(command) || pendingSchemaControl(command); }
    public boolean canonicalStaleSchemaControl(DistributedCommand command) {
        Objects.requireNonNull(command);
        if(command.sessionControl()==null || !groupId.equals(command.groupId()) || rangeEpoch!=command.rangeEpoch() || metadataSchemaControl(command)) return false;
        var digest=command.schemaDigest(); return !Arrays.equals(schemaDigest,digest) && schemaSuperseded.containsKey(schemaIdentity(digest));
    }
    private byte[] applyStaleSchemaControl(ApplyPosition position,DistributedCommand command) throws RocksDBException {
        var id=command.operationId(); long minimum=minimumSessionEpoch(null),floor=id.epoch()<minimum ? -1 : acknowledgedThrough(id,null);
        if(id.epoch()<minimum || id.sequence()<=floor) { advanceCompletedMarker(position); return expiredResult(); }
        requireUnlocked(command); requireNext(position);
        byte[] result=new CommandResult(CommandResult.Status.STALE_SCHEMA,List.of()).encode(),encoded=encodeReceipt(command.receiptDigest(),result);
        var history=prepareScopedWrites(false,List.of(),Map.of());
        try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
            if(cdc!=null) cdc.initialize(batch);
            updateReceiptUsage(batch,command,-1,-1,minimum,floor,encoded.length,position);
            batch.put(receipts,receiptKey(id),encoded); batch.put(state,markerKey,encodePosition(position)); db.write(options,batch);
        }
        publishScoped(position,history,false); applied=position; return result;
    }
    private boolean topologyAccountingControl(DistributedCommand command) {
        return groupId.equals(command.groupId()) && rangeEpoch==command.rangeEpoch() && Arrays.equals(schemaDigest,command.schemaDigest())
                && command.mutations().isEmpty() && command.predicates().isEmpty() && command.acceptance()==null && !command.transactionRejection()
                && (command.kind()==DistributedCommand.Kind.DATA && command.acknowledgedThrough()>=0 || command.kind()==DistributedCommand.Kind.SESSION_FLOOR && command.minimumSessionEpoch()>=0);
    }
    public void validate(DistributedCommand command) {
        requireServing();
        if(metadataTopologyControl(command)) {
            enterNative();applyLane.lock();try { requireTopologyMetadataOwner();validateCommand(command,false); }
            catch(RocksDBException failure) { throw new IllegalStateException("Cannot validate sealed topology control",failure); }
            finally { applyLane.unlock();leaveNative(); }return;
        }
        if(metadataSchemaControl(command) || topologyAccountingControl(command) && activeSchemaFence==null || activeTopologyFreeze==null && topologyPreparation==null && activeSchemaFence==null && Arrays.equals(schemaDigest,command.schemaDigest())) { validateCommand(command,true); return; }
        enterNative(); applyLane.lock();
        try {
            requireActive(); validateCommand(command,false);
            byte[] retained=db.get(receipts,receiptKey(command.operationId()));
            if(retained==null) {
                requireTopologyWritable();
                if(canonicalStaleSchemaControl(command)) { requireUnlocked(command); return; }
                if(!Arrays.equals(command.schemaDigest(),schemaDigest)) throw new IllegalArgumentException("Historical schema has no saved DATA outcome");
                throw new IllegalStateException("Schema change blocks new DATA admission");
            }
            decodeReceipt(retained,command.receiptDigest());
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot validate retained schema-fenced command",failure); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    private void validateCommand(DistributedCommand command,boolean active) { validateCommand(command,active,true); }
    private void validateCommand(DistributedCommand command,boolean active,boolean logical) {
        requireSourceCreditOriginal(command);
        requireHomeRecoveryFree(command.operationId());
        if (command.sessionControl()!=null && !sessionControlsEnabled) throw new UnsupportedOperationException("Replicated session binding is not activated");
        if (closing) throw new IllegalStateException("Range admission is closed");
        if(active) { requireActive(); if(!metadataSchemaControl(command)) requireSchemaAdmission(); }
        var commandSchemaDigest=command.schemaDigest(); boolean currentSchema=Arrays.equals(schemaDigest,commandSchemaDigest);
        var historical=(!active || pendingSchemaControl(command)) && !currentSchema ? schemaHistory.get(schemaIdentity(commandSchemaDigest)) : null;
        var commandColumns=historical==null ? logicalColumns() : historical.columnIds();
        if (!groupId.equals(command.groupId()) || rangeEpoch != command.rangeEpoch()
                || !currentSchema && historical==null) {
            throw new IllegalArgumentException("Wrong group, epoch or schema");
        }
        for (var mutation : command.mutations()) {
            if (snapshotHomeContext!=null && mutation.kind()==Mutation.Kind.PUT) requireSnapshotHomeUser(snapshotHomeContext,mutation.key(),SnapshotFenceStatus.decode(mutation.value()));
            if (!commandColumns.contains(mutation.columnId())) throw new IllegalArgumentException("Unknown logical column");
            var selectedSchema=historical==null ? logicalSchema : historical;
            if(mutation.kind()==Mutation.Kind.MERGE && selectedSchema!=null && selectedSchema.column(mutation.columnId()).schema().variableLengthKeysCount()!=0) throw new IllegalArgumentException("Bucket MERGE requires explicit member operand");
            if ((mutation.kind() == Mutation.Kind.MERGE || mutation.kind()==Mutation.Kind.BUCKET_MERGE) && !(historical==null ? deterministicMergeColumns.contains(mutation.columnId()) : historical.column(mutation.columnId()).operator()!=null)) {
                throw new IllegalArgumentException("Column has no admitted deterministic merge operator");
            }
            if (!contains(mutation.key())) throw new IllegalArgumentException("Mutation outside owned interval");
            if (mutation.kind() == Mutation.Kind.DELETE_RANGE) {
                byte[] upper = interval.upperExclusive();
                if (upper != null && Arrays.compareUnsigned(mutation.value(), upper) > 0) {
                    throw new IllegalArgumentException("Range deletion outside owned interval");
                }
            }
        }
        for (var predicate : command.predicates()) {
            if (!commandColumns.contains(predicate.columnId()) || !contains(predicate.key())) {
                throw new IllegalArgumentException("Predicate outside owned column/interval");
            }
        }
        if(active)requireHomeStatusAdmission(command);
        if (logical && (command.hasBucketData() || command.acceptance()!=null && logicalSchema!=null)) validateLogical(command,historical==null ? logicalSchema : historical);
    }

    public record RetirementCandidate(TransactionStatus transaction,SnapshotFenceStatus snapshot,boolean orphan) {
        public RetirementCandidate {
            if ((transaction==null)==(snapshot==null) || orphan && transaction==null) throw new IllegalArgumentException("Retirement candidate requires one status");
            if (transaction!=null) { if (orphan) transaction.requireInitial(); else if (!transaction.gcEligible()) throw new IllegalArgumentException("Transaction is not consumed"); }
            if (snapshot!=null && !snapshot.consumed()) throw new IllegalArgumentException("Snapshot is not consumed");
        }
        public UUID id() { return transaction==null ? snapshot.fence().fenceId() : transaction.transactionId(); }
        public OperationId business() { return transaction==null ? snapshot.fence().operation() : transaction.operation(); }
    }
    public record RetirementPage(UUID groupId,long rangeEpoch,byte[] schemaDigest,UUID columnId,ApplyPosition position,long minimumEpoch,byte[] next,boolean more,RetirementCandidate candidate) {
        public RetirementPage { Objects.requireNonNull(groupId); Objects.requireNonNull(columnId); Objects.requireNonNull(position); if (rangeEpoch<0 || schemaDigest.length!=32 || minimumEpoch<0) throw new IllegalArgumentException("Invalid retirement page"); schemaDigest=schemaDigest.clone(); next=next==null ? null : next.clone(); }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
        @Override public byte[] next() { return next==null ? null : next.clone(); }
    }
    /** Bounded, non-mutating discovery at one completed native cut. Cursor advances over ineligible rows. */
    public RetirementPage retirementCandidates(boolean orphans,byte[] after,int scans) throws RocksDBException {
        if (scans<1 || scans>16 || after!=null && after.length!=(orphans ? 32 : 16)) throw new IllegalArgumentException("Invalid retirement cursor/scan bound");
        byte[] cursor=after==null ? null : after.clone(); enterNative(); applyLane.lock();
        try {
            requireActive(); var context=homeRecoveryContext!=null ? homeRecoveryContext : snapshotHomeContext;
            if (context==null || orphans && homeRecoveryContext==null) throw new IllegalStateException("Retirement discovery requires a qualified HOME role");
            if (applied==null) throw new IllegalStateException("Retirement discovery requires a completed cut");
            long floor=minimumSessionEpoch(null); byte[] lower=orphans ? ByteBuffer.allocate(17).put(markerKey).put((byte)0x25).array() : interval.lowerInclusive();
            byte[] upper=orphans ? ByteBuffer.allocate(17).put(markerKey).put((byte)0x26).array() : interval.upperExclusive();
            try (var low=lower==null ? null : new org.rocksdb.Slice(lower); var high=upper==null ? null : new org.rocksdb.Slice(upper);
                 var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high); var iterator=databaseOwner.newIterator(orphans ? state : columns.get(context.column()),options)) {
                byte[] seek=cursor==null ? lower : orphans ? ByteBuffer.allocate(49).put(lower).put(cursor).array() : cursor;
                if (seek==null) iterator.seekToFirst(); else { iterator.seek(seek); if (cursor!=null && iterator.isValid() && Arrays.equals(iterator.key(),seek)) iterator.next(); }
                byte[] next=null; RetirementCandidate candidate=null; long bytes=0; int visited=0; var sizing=new byte[0];
                while (iterator.isValid() && visited<scans) {
                    int size=iterator.value(sizing); if (size>2*1024*1024) throw new IllegalStateException("Retirement row exceeds bounded discovery payload");
                    if (size>2*1024*1024-bytes) break;
                    byte[] key=iterator.key(); if (key.length!=(orphans ? 49 : 16)) throw new IllegalArgumentException("Retirement row key is malformed");
                    byte[] value=iterator.value(); bytes+=size; visited++; next=orphans ? Arrays.copyOfRange(key,17,49) : key;
                    if (orphans) {
                        var receipt=HomeRecoveryReceipt.decode(value); var recovery=receipt.recovery(); requireHomeRecovery(recovery); receipt.requireCut(applied);
                        if (!Arrays.equals(key,homeRecoveryKey(recovery.operation()))) throw new IllegalArgumentException("HOME recovery row key changed");
                        requireHomeSuccessor(receipt,null);
                        if (receipt.result().status()==CommandResult.Status.CONFLICT && recovery.before().operation().epoch()<floor) {
                            var original=homeIdentity(recovery); var current=readHomeState(original,null,applied);
                            if (!current.matching() && (current.credit()==null || !current.credit().current().operation().equals(original.business()))) candidate=new RetirementCandidate(recovery.before().asInitial(),null,true);
                        }
                    } else if (homeRecoveryContext!=null) {
                        var transaction=TransactionStatus.decode(value); var original=new HomeIdentity(groupId,rangeEpoch,schemaDigest,context.column(),transaction);
                        var current=readHomeState(original,null,applied);
                        if (!Arrays.equals(key,uuidBytes(transaction.transactionId())) || !current.matching() || !transaction.equals(current.row())) throw new IllegalArgumentException("HOME discovery USER identity changed");
                        if (transaction.gcEligible()) candidate=new RetirementCandidate(transaction,null,false);
                    } else {
                        var snapshot=SnapshotFenceStatus.decode(value); requireSnapshotHomeUser(context,key,snapshot);
                        if (snapshot.consumed()) candidate=new RetirementCandidate(null,snapshot,false);
                    }
                    iterator.next(); if (candidate!=null) break;
                }
                iterator.status(); return new RetirementPage(groupId,rangeEpoch,schemaDigest,context.column(),applied,floor,next,iterator.isValid(),candidate);
            }
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public record HomeRecoveryCapacity(ApplyPosition position,int count,long bytes,int reservedCount,long reservedBytes) {
        public int effectiveCount() { return count+reservedCount; }
        public long effectiveBytes() { return bytes+reservedBytes; }
    }
    public record HomeRecoveryEstimate(HomeRecoveryCapacity capacity,boolean newReceipt,HomeRecoveryReceipt existing,boolean creditCovered,int additionalCount,long additionalBytes,HomeFenceProof retirement) {}
    public record HomeCreditEstimate(HomeRecoveryCapacity capacity,int additionalCount,long additionalBytes) {}
    private record HomeCreditChange(Map<UUID,HomeRecoveryCredit> published,UUID transaction,HomeRecoveryCredit credit,int count,long bytes) {}
    public void configureHomeRecoveryReservations(UUID cluster,UUID database,UUID column) {
        Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(column); enterNative(); applyLane.lock();
        try {
            var context=new HomeRecoveryContext(cluster,database,column);
            if (activeTopologyFreeze!=null || activeSchemaFence!=null || snapshotHomeContext!=null || !columns.keySet().equals(Set.of(column)) || homeRecoveryContext!=null && !homeRecoveryContext.equals(context)) throw new IllegalArgumentException("HOME reservation context changed or column is not dedicated");
            for (var credit:homeRecoveryCredits.values()) if (!credit.cluster().equals(cluster) || !credit.database().equals(database) || !credit.column().equals(column)) throw new IllegalArgumentException("HOME reservation context differs from stored promises");
            for (var receipt:homeRecoveries.values()) {
                var recovery=receipt.recovery(); if (!recovery.before().clusterId().equals(cluster) || !recovery.before().databaseId().equals(database) || !recovery.command().mutations().getFirst().columnId().equals(column)) throw new IllegalArgumentException("HOME receipt differs from configured context");
            }
            var recovered=loadHomeCredits(applied,context); homeRecoveryCredits=recovered; validateHomeCreditCapacity(); homeRecoveryContext=context; homeStatusCapacityKnown=false;
        } finally { applyLane.unlock(); leaveNative(); }
    }
    private byte[] homeCreditKey(UUID transaction) { return ByteBuffer.allocate(33).put(markerKey).put((byte)0x26).putLong(transaction.getMostSignificantBits()).putLong(transaction.getLeastSignificantBits()).array(); }
    private HomeRecoveryContext creditContext() {
        if (homeRecoveryContext!=null) return homeRecoveryContext;
        var credit=homeRecoveryCredits.values().stream().findFirst().orElse(null); return credit==null ? null : new HomeRecoveryContext(credit.cluster(),credit.database(),credit.column());
    }
    private HomeCreditChange homeCreditChange(DistributedCommand command,ApplyPosition position,boolean successful) throws RocksDBException { return homeCreditChange(command,position,successful,false); }
    private HomeCreditChange homeCreditChange(DistributedCommand command,ApplyPosition position,boolean successful,boolean protectedRecovery) throws RocksDBException {
        if (!successful || command.mutations().isEmpty()) return null;
        if (!protectedRecovery && homeRecoveryContext==null && !homeRecoveryCredits.isEmpty()) throw new IllegalStateException("HOME reservations must be configured before ordinary changes");
        var context=creditContext(); if (context==null) return null;
        if (command.kind()!=DistributedCommand.Kind.DATA || command.hasBucketData() || command.mutations().size()!=1 || command.predicates().size()!=1) throw new IllegalArgumentException("Promised HOME changes require one physical CAS");
        var mutation=command.mutations().getFirst(); var predicate=command.predicates().getFirst();
        if (mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE || !mutation.columnId().equals(context.column()) || !predicate.columnId().equals(context.column()) || mutation.key().length!=16 || !Arrays.equals(mutation.key(),predicate.key())) throw new IllegalArgumentException("Invalid promised HOME CAS envelope");
        var next=TransactionStatus.decode(mutation.value());
        if (!next.clusterId().equals(context.cluster()) || !next.databaseId().equals(context.database()) || !Arrays.equals(mutation.key(),uuidBytes(next.transactionId()))) throw new IllegalArgumentException("Promised HOME identity changed");
        var old=homeRecoveryCredits.get(next.transactionId()); TransactionStatus initial;
        if (predicate.expectedValue()==null) { next.requireInitial(); if(next.origin()!=null && !next.reserved())throw new IllegalArgumentException("Reroute HOME must reserve before installing accepted participants"); if (old!=null) throw new IllegalStateException("HOME registration replaced a promise"); initial=next; }
        else {
            var before=TransactionStatus.decode(predicate.expectedValue()); next.requireSuccessorOf(before);requireHomeNoEffectOutcome(before,next);
            if (old==null && HomeRecoveryCredit.remaining(before)!=0) throw new IllegalStateException("Existing HOME has no future recovery promise");
            if (old!=null && !old.current().equals(before)) throw new IllegalStateException("HOME row and promise differ"); initial=before.reserved() && next.phase()==TransactionStatus.Phase.PREPARING ? next.asInitial() : old==null ? before : old.initial();
        }
        var credit=HomeRecoveryCredit.remaining(next)==0 ? null : new HomeRecoveryCredit(groupId,rangeEpoch,schemaDigest,context.cluster(),context.database(),context.column(),initial,next,position);
        int count=(credit==null ? 0 : credit.reservedCount())-(old==null ? 0 : old.reservedCount()); long bytes=(credit==null ? 0 : credit.reservedBytes())-(old==null ? 0 : old.reservedBytes());
        var published=new HashMap<>(homeRecoveryCredits); if (credit==null) published.remove(next.transactionId()); else published.put(next.transactionId(),credit);
        return new HomeCreditChange(Map.copyOf(published),next.transactionId(),credit,count,bytes);
    }
    private void checkHomeCapacity(int count,long bytes) { var capacity=homeRecoveryCapacityUnderLane(); if (count>MAX_HOME_RECOVERIES-capacity.effectiveCount() || bytes>MAX_HOME_RECOVERY_BYTES-capacity.effectiveBytes()) throw new IllegalStateException("HOME receipt/future promise capacity exhausted"); }
    private void writeHomeCredit(WriteBatch batch,HomeCreditChange change) throws RocksDBException { if (change!=null) { if (change.credit()==null) batch.delete(state,homeCreditKey(change.transaction())); else batch.put(state,homeCreditKey(change.transaction()),change.credit().encode()); } }
    private void publishHomeCredit(HomeCreditChange change) { if (change!=null) { homeRecoveryCredits=change.published(); reservedHomeRecoveryCount+=change.count(); reservedHomeRecoveryBytes+=change.bytes(); } }
    public HomeCreditEstimate estimateHomeRegistration(DistributedCommand command) throws RocksDBException {
        Objects.requireNonNull(command); enterNative(); applyLane.lock();
        try {
            requireActive(); if (homeRecoveryContext==null) throw new IllegalStateException("HOME registration reservations are not configured"); validate(command);
            var capacity=homeRecoveryCapacityUnderLane(); if (db.get(receipts,receiptKey(command.operationId()))!=null) return new HomeCreditEstimate(capacity,0,0);
            if (expiredHomeRegistration(command)) return new HomeCreditEstimate(capacity,0,0);
            boolean successful=evaluate(command).result().status()==CommandResult.Status.SUCCESS;
            var change=homeCreditChange(command,new ApplyPosition(Math.max(1,applied==null ? 1 : applied.term()),applied==null ? 0 : applied.index()+1),successful);
            int count=change==null ? 0 : Math.max(0,change.count()); long bytes=change==null ? 0 : Math.max(0,change.bytes()); checkHomeCapacity(count,bytes); return new HomeCreditEstimate(capacity,count,bytes);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    private byte[] homeRecoveryKey(OperationId id) { return ByteBuffer.allocate(49).put(markerKey).put((byte)0x25).putLong(id.sessionId().getMostSignificantBits()).putLong(id.sessionId().getLeastSignificantBits()).putLong(id.epoch()).putLong(id.sequence()).array(); }
    public static long homeRecoveryBytes(HomeRecovery recovery) { return 49L+28+recovery.encode().length; }
    private void requireHomeRecovery(HomeRecovery recovery) {
        var command=recovery.command(); var mutation=command.mutations().getFirst();
        if (!command.groupId().equals(groupId) || command.rangeEpoch()!=rangeEpoch || !Arrays.equals(command.schemaDigest(),schemaDigest)
                || columns.size()!=1 || !columns.containsKey(mutation.columnId()) || !contains(mutation.key())) throw new IllegalArgumentException("HOME recovery differs from native home ownership/schema");
        var context=homeRecoveryContext;
        if (context!=null && (!context.column().equals(mutation.columnId()) || !context.cluster().equals(recovery.before().clusterId()) || !context.database().equals(recovery.before().databaseId()))) throw new IllegalArgumentException("HOME recovery differs from configured database");
    }
    private void requireHomeRecoveryFree(OperationId id) { if (homeRecoveries.containsKey(id)) throw new IllegalArgumentException("Operation identity belongs to protected HOME recovery"); }
    private void requireHomeRecoveryIdentity(HomeRecovery recovery) throws RocksDBException {
        var id=recovery.operation();
        for (var p:preparedIndex.records()) if (p.preparation().ownsOperation(id)) throw new IllegalArgumentException("HOME recovery identity belongs to preparation");
        for (var r:rejectedIndex.values()) if (r.ownsOperation(id)) throw new IllegalArgumentException("HOME recovery identity belongs to rejection");
        for (var r:abortedIndex.values()) if (r.ownsOperation(id)) throw new IllegalArgumentException("HOME recovery identity belongs to ABORT");
        for (var r:committedIndex.values()) if (r.ownsOperation(id)) throw new IllegalArgumentException("HOME recovery identity belongs to COMMIT");
        for (var r:snapshotGates.values()) if (r.ownsOperation(id)) throw new IllegalArgumentException("HOME recovery identity belongs to snapshot gate");
        if (db.get(receipts,receiptKey(id))!=null) throw new IllegalArgumentException("HOME recovery identity has an ordinary receipt");
    }
    private HomeRecoveryCapacity homeRecoveryCapacityUnderLane() { return new HomeRecoveryCapacity(applied,homeRecoveries.size(),retainedHomeRecoveryBytes,reservedHomeRecoveryCount,reservedHomeRecoveryBytes); }
    public HomeRecoveryCapacity homeRecoveryCapacity() {
        enterNative(); applyLane.lock(); try { requireActive(); return homeRecoveryCapacityUnderLane(); } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Metadata-only role validation after recovery; it grants no writable ownership. */
    public HomeRecoveryCapacity recoveredHomeRecoveryCapacity() {
        enterNative(); applyLane.lock();
        try { if(pendingInstallation!=null)throw new IllegalStateException("Range metadata is being installed"); return homeRecoveryCapacityUnderLane(); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    public HomeRecoveryEstimate estimateHomeRecovery(HomeRecovery recovery) throws RocksDBException {
        Objects.requireNonNull(recovery); enterNative(); applyLane.lock();
        try {
            requireActive(); requireHomeRecovery(recovery); var existing=homeRecoveries.get(recovery.operation());
            if (existing!=null) { requireSameHomeRecovery(existing,recovery,applied); requireHomeSuccessor(existing,null); return new HomeRecoveryEstimate(homeRecoveryCapacityUnderLane(),false,existing,false,0,0,null); }
            var retired=homeRecoveryContext==null ? null : readHomeFence(homeIdentity(recovery),null,applied);
            if (retired!=null) return new HomeRecoveryEstimate(homeRecoveryCapacityUnderLane(),false,null,false,0,0,retired);
            requireHomeRecoveryIdentity(recovery); var command=recovery.command(); requireUnlocked(command); requireUngated(commandScope(command));
            var capacity=homeRecoveryCapacityUnderLane();
            boolean successful=evaluate(command).result().status()==CommandResult.Status.SUCCESS;if(successful)requireHomeNoEffectOutcome(recovery.before(),recovery.next());
            var credit=homeRecoveryCredits.get(recovery.before().transactionId()); boolean covered=credit!=null && credit.current().equals(recovery.before()) && successful;
            int count=covered ? 0 : 1; long bytes=covered ? 0 : homeRecoveryBytes(recovery); checkHomeCapacity(count,bytes);
            return new HomeRecoveryEstimate(capacity,true,null,covered,count,bytes,null);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public HomeRecoveryResult applyHomeRecovery(ApplyPosition position,HomeRecovery recovery) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(recovery); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); var estimate=estimateHomeRecovery(recovery); var saved=estimate.existing();
            if (estimate.retirement()!=null) { advanceCompletedMarker(position); return HomeRecoveryResult.retired(readHomeFence(homeIdentity(recovery),null,applied)); }
            if (applied!=null && position.index()<=applied.index()) { if (saved==null) throw new IllegalStateException("Completed HOME recovery has no retained receipt"); return HomeRecoveryResult.applied(saved); }
            requireNext(position); var command=recovery.command();
            var result=saved==null ? evaluate(command).result() : saved.result();
            var receipt=saved==null ? new HomeRecoveryReceipt(recovery,position,result) : saved;
            var published=homeRecoveries; long bytes=retainedHomeRecoveryBytes;
            if (saved==null) { var next=new HashMap<>(homeRecoveries); next.put(recovery.operation(),receipt); published=Map.copyOf(next); bytes+=homeRecoveryBytes(recovery); }
            boolean effects=saved==null && result.status()==CommandResult.Status.SUCCESS;
            var homeStatusChange=effects ? homeStatusDelta(command.mutations()) : null;
            if(!homeStatusFits(homeStatusChange))throw new IllegalStateException("Protected HOME recovery cannot create an unreserved status row");
            var creditChange=homeCreditChange(command,position,effects,true);
            if (saved==null) checkHomeCapacity(1+(creditChange==null ? 0 : creditChange.count()),homeRecoveryBytes(recovery)+(creditChange==null ? 0 : creditChange.bytes()));
            var writes=prepareScopedWrites(effects,command.mutations(),Map.of());
            try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) cdc.initialize(batch);
                if (effects) { var mutation=command.mutations().getFirst(); batch.put(columns.get(mutation.columnId()),mutation.key(),mutation.value()); }
                if (saved==null) batch.put(state,homeRecoveryKey(recovery.operation()),receipt.encode()); writeHomeCredit(batch,creditChange);
                batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            if(homeStatusChange!=null)homeStatusWriteObserver.afterWrite();
            homeRecoveries=published; retainedHomeRecoveryBytes=bytes; publishHomeCredit(creditChange); publishHomeStatus(homeStatusChange); publishScoped(position,writes,false); applied=position; return HomeRecoveryResult.applied(receipt);
        } catch (RocksDBException | RuntimeException | Error failure) { resetScopedUnderLane(); throw failure; }
        finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private void requireSameHomeRecovery(HomeRecoveryReceipt receipt,HomeRecovery expected,ApplyPosition cut) {
        receipt.requireCut(cut); if (!Arrays.equals(receipt.recovery().digest(),expected.digest())) throw new IllegalArgumentException("Protected HOME recovery identity payload changed");
    }
    private HomeRecoveryResult readHomeRecovery(HomeRecovery recovery,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        requireHomeRecovery(recovery); byte[] bytes=options==null ? db.get(state,homeRecoveryKey(recovery.operation())) : db.get(state,options,homeRecoveryKey(recovery.operation()));
        if (bytes!=null) { var receipt=HomeRecoveryReceipt.decode(bytes); requireSameHomeRecovery(receipt,recovery,cut); requireHomeSuccessor(receipt,options); return HomeRecoveryResult.applied(receipt); }
        var retired=homeRecoveryContext==null ? null : readHomeFence(homeIdentity(recovery),options,cut);
        return retired==null ? null : HomeRecoveryResult.retired(retired);
    }
    public HomeRecoveryResult homeRecoveryResult(HomeRecovery recovery) throws RocksDBException {
        Objects.requireNonNull(recovery); enterNative(); applyLane.lock();
        try { if (pendingInstallation!=null) throw new IllegalStateException("HOME recovery metadata is being installed"); return readHomeRecovery(recovery,null,applied); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    static void requireHomeRecoverySuccessor(HomeRecoveryReceipt receipt,TransactionStatus current) {
        if (receipt.result().status()!=CommandResult.Status.SUCCESS) return;
        if (current==null) throw new IllegalStateException("Successful HOME recovery has no USER row");
        var next=receipt.recovery().next();
        if (!next.sameDefinition(current) || next.noEffectOutcome()!=current.noEffectOutcome() || next.phase()!=current.phase() || current.generation()<next.generation() || next.consumed() && !current.consumed()) throw new IllegalArgumentException("Successful HOME recovery USER definition/decision regressed");
        long steps=next.consumed()==current.consumed() ? 0 : 1;
        for (int i=0;i<next.participants().size();i++) {
            var before=next.participants().get(i); var after=current.participants().get(i);
            if (!Objects.equals(before.prepared(),after.prepared()) || before.finalized() && !after.finalized()) throw new IllegalArgumentException("Successful HOME recovery USER proofs changed");
            if (!before.finalized() && after.finalized()) steps++;
        }
        if (current.generation()-next.generation()!=steps) throw new IllegalArgumentException("Successful HOME recovery USER generation changed");
    }
    private void requireHomeSuccessor(HomeRecoveryReceipt receipt,ReadOptions options) throws RocksDBException {
        if (receipt.result().status()!=CommandResult.Status.SUCCESS) return;
        var mutation=receipt.recovery().command().mutations().getFirst(); var column=columns.get(mutation.columnId());
        byte[] bytes=options==null ? db.get(column,mutation.key()) : db.get(column,options,mutation.key());
        requireHomeRecoverySuccessor(receipt,bytes==null ? null : TransactionStatus.decode(bytes));
    }
    private void requireHomeNoEffectOutcome(TransactionStatus before,TransactionStatus next) throws RocksDBException {
        if(!before.reserved() || next.noEffectOutcome()!=TransactionStatus.NoEffectOutcome.SESSION_EXPIRED)return;
        var origin=before.origin();var first=origin.firstControl();var promotion=new OperationId(first.sessionId(),first.epoch(),first.sequence()+128);
        if(!homeIdentityExpired(before.operation(),false) && !homeIdentityExpired(promotion,true))throw new IllegalArgumentException("HOME expired no-effect outcome lacks native business/control expiry evidence");
    }
    private boolean homeIdentityExpired(OperationId operation,boolean acknowledgement) throws RocksDBException {
        if(operation.epoch()<minimumSessionEpoch(null) || acknowledgement && operation.sequence()<=acknowledgedThrough(operation,null))return true;
        var authority=sessionAuthority();if(authority==null)return false;
        if(operation.epoch()<authority.minimumEpoch())return true;
        var binding=sessionBinding(operation.epoch());
        return binding!=null && binding.grant().sessionId().equals(operation.sessionId()) && binding.grant().expiresAtMillis()<=authority.logicalTimeMillis();
    }
    private boolean expiredHomeRegistration(DistributedCommand command) throws RocksDBException {
        var context=homeRecoveryContext!=null ? homeRecoveryContext : snapshotHomeContext;
        if (context==null || command.kind()!=DistributedCommand.Kind.DATA || command.mutations().size()!=1 || command.predicates().size()!=1) return false;
        var mutation=command.mutations().getFirst();
        if (!mutation.columnId().equals(context.column()) || mutation.kind()!=Mutation.Kind.PUT) return false;
        if (snapshotHomeContext!=null) {
            if(command.predicates().getFirst().expectedValue()!=null)return false;
            var initial=SnapshotFenceStatus.decode(mutation.value());
            requireSnapshotHomeUser(context,mutation.key(),initial);
            if (initial.phase()!=SnapshotFenceStatus.Phase.INSTALLING || initial.generation()!=0) throw new IllegalArgumentException("Snapshot HOME registration is not initial");
            return homeIdentityExpired(initial.fence().operation(),false);
        }
        var initial=TransactionStatus.decode(mutation.value());
        var expected=command.predicates().getFirst().expectedValue();
        if(expected==null)initial.requireInitial();
        else {
            var before=TransactionStatus.decode(expected);
            if(!before.reserved() || initial.phase()!=TransactionStatus.Phase.PREPARING)return false;
            initial.requireSuccessorOf(before);
        }
        if(homeIdentityExpired(initial.operation(),false))return true;
        if(initial.origin()!=null) {
            var authority=sessionAuthority();
            if(authority!=null) {
                var binding=sessionBinding(initial.operation().epoch());
                if(binding==null || !binding.grant().sessionId().equals(initial.operation().sessionId()))throw new IllegalStateException("Reroute HOME business has no exact native session binding");
                return !authority.permits(binding.grant());
            }
        }
        return false;
    }
    public void configureSnapshotHome(UUID cluster,UUID database,UUID column) {
        var context=new HomeRecoveryContext(Objects.requireNonNull(cluster),Objects.requireNonNull(database),Objects.requireNonNull(column));
        enterNative(); applyLane.lock();
        try {
            if (activeTopologyFreeze!=null || activeSchemaFence!=null || homeRecoveryContext!=null || !homeRecoveries.isEmpty() || !homeRecoveryCredits.isEmpty() || !columns.keySet().equals(Set.of(column)) || snapshotHomeContext!=null && !snapshotHomeContext.equals(context)) throw new IllegalArgumentException("Snapshot HOME role/column changed");
            var authority=sessionAuthority(null); if (authority!=null && (!authority.clusterId().equals(cluster) || !authority.databaseId().equals(database))) throw new IllegalArgumentException("Snapshot HOME session authority differs");
            validateSnapshotHomeUsers(null,context); snapshotHomeContext=context; homeStatusCapacityKnown=false;
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot validate snapshot HOME",failure); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    private void requireSnapshotHomeUser(HomeRecoveryContext context,byte[] key,SnapshotFenceStatus status) {
        var fence=status.fence();
        if (key.length!=16 || !Arrays.equals(key,uuidBytes(fence.fenceId())) || !contains(key) || !fence.clusterId().equals(context.cluster()) || !fence.databaseId().equals(context.database()) || !fence.homeGroup().equals(groupId) || fence.homeEpoch()!=rangeEpoch) throw new IllegalArgumentException("Snapshot HOME USER identity differs from native role");
    }
    private void validateSnapshotHomeUsersUnchecked() {
        try { validateSnapshotHomeUsers(null,snapshotHomeContext); } catch (RocksDBException failure) { throw new IllegalStateException("Cannot validate snapshot HOME USER",failure); }
    }
    private void validateSnapshotHomeUsers(Snapshot snapshot,HomeRecoveryContext context) throws RocksDBException {
        if (context==null) return;
        scanHomeStatusCapacity(context,true,snapshot);
        try (var options=new ReadOptions().setSnapshot(snapshot).setFillCache(false); var iterator=databaseOwner.newIterator(columns.get(context.column()),options)) {
            var authority=sessionAuthority(options);
            if (authority!=null && (!authority.clusterId().equals(context.cluster()) || !authority.databaseId().equals(context.database()))) throw new IllegalArgumentException("Snapshot HOME session authority differs");
            if (interval.lowerInclusive()==null) iterator.seekToFirst(); else iterator.seek(interval.lowerInclusive());
            for (;iterator.isValid() && contains(iterator.key());iterator.next()) {
                byte[] value=iterator.value(); if (value.length>SnapshotFenceStatus.MAX_BYTES) throw new IllegalArgumentException("Snapshot HOME row too large");
                requireSnapshotHomeUser(context,iterator.key(),SnapshotFenceStatus.decode(value));
            }
            iterator.status();
        }
    }
    private SnapshotFenceStatus readSnapshotHome(SnapshotHomeIdentity identity,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        var context=snapshotHomeContext; if (context==null) throw new IllegalStateException("Snapshot HOME capability is not configured");
        if (!identity.groupId().equals(groupId) || identity.rangeEpoch()!=rangeEpoch || !Arrays.equals(identity.schemaDigest(),schemaDigest) || !identity.columnId().equals(context.column())) throw new IllegalArgumentException("Snapshot HOME identity differs from native ownership/schema");
        requireSnapshotHomeUser(context,uuidBytes(identity.snapshotId()),SnapshotFenceStatus.begin(identity.fence()));
        byte[] marker=stateRow(markerKey,options); if (marker==null || !Objects.equals(decodePosition(marker),cut)) throw new IllegalStateException("Snapshot HOME cut differs from completed marker");
        var authority=sessionAuthority(options); if (authority!=null && (!authority.clusterId().equals(context.cluster()) || !authority.databaseId().equals(context.database()))) throw new IllegalArgumentException("Snapshot HOME session authority differs");
        byte[] key=uuidBytes(identity.snapshotId()); byte[] value=options==null ? db.get(columns.get(context.column()),key) : db.get(columns.get(context.column()),options,key);
        if (value==null) return null;
        var status=SnapshotFenceStatus.decode(value); requireSnapshotHomeUser(context,key,status);
        if (status.fence().operation().equals(identity.business())) {
            if (!status.fence().equals(identity.fence())) throw new IllegalArgumentException("Retained snapshot HOME definition changed");
            return status;
        }
        if (identity.business().epoch()>=minimumSessionEpoch(options)) throw new IllegalArgumentException("Snapshot UUID belongs to another live business");
        return null;
    }
    private SnapshotHomeFenceProof readSnapshotHomeFence(SnapshotHomeIdentity identity,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        return readSnapshotHome(identity,options,cut)==null && identity.business().epoch()<minimumSessionEpoch(options) ? new SnapshotHomeFenceProof(identity,minimumSessionEpoch(options),cut) : null;
    }
    public SnapshotHomeFenceProof snapshotHomeFence(SnapshotHomeIdentity identity) throws RocksDBException {
        enterNative(); applyLane.lock(); try { if (pendingInstallation!=null) throw new IllegalStateException("Snapshot HOME installation pending"); return readSnapshotHomeFence(identity,null,applied); } finally { applyLane.unlock(); leaveNative(); }
    }
    public boolean snapshotHomeRetired(SnapshotHomeRetirement retirement) throws RocksDBException { return snapshotHomeFence(retirement.identity())!=null; }
    private boolean snapshotHomeRetirementNeeded(SnapshotHomeRetirement retirement) throws RocksDBException {
        var status=readSnapshotHome(retirement.identity(),null,applied);
        if (retirement.identity().business().epoch()>=minimumSessionEpoch(null)) throw new IllegalStateException("Snapshot HOME retirement requires a newer business floor");
        if (status==null) return false;
        if (!status.consumed() || !status.equals(retirement.consumed())) throw new IllegalArgumentException("Snapshot HOME retirement requires exact consumed USER");
        return true;
    }
    public boolean estimateSnapshotHomeRetirement(SnapshotHomeRetirement retirement) throws RocksDBException {
        enterNative(); applyLane.lock(); try { requireActive(); return snapshotHomeRetirementNeeded(retirement); } finally { applyLane.unlock(); leaveNative(); }
    }
    public void applySnapshotHomeRetirement(ApplyPosition position,SnapshotHomeRetirement retirement) throws RocksDBException {
        enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); if (!snapshotHomeRetirementNeeded(retirement)) { advanceCompletedMarker(position); return; }
            if (applied!=null && position.index()<=applied.index()) throw new IllegalStateException("Covered snapshot HOME retirement retains USER");
            requireNext(position); var identity=retirement.identity(); byte[] key=uuidBytes(identity.snapshotId());
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(identity.columnId(),key))); requireUnlocked(scope); requireUngated(scope);
            var effects=List.of(new Mutation(Mutation.Kind.DELETE,identity.columnId(),key,null)); var history=prepareScopedWrites(true,effects,Map.of());
            try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) cdc.initialize(batch);
                batch.delete(columns.get(identity.columnId()),key); batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            publishScoped(position,history,false); homeStatusCapacityKnown=false; applied=position;
        } catch (RocksDBException | RuntimeException | Error failure) { resetScopedUnderLane(); throw failure; }
        finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private static HomeIdentity homeIdentity(HomeRecovery recovery) {
        var command=recovery.command(); return new HomeIdentity(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.mutations().getFirst().columnId(),recovery.before());
    }
    private void requireHomeIdentity(HomeIdentity identity,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        var context=homeRecoveryContext; if (context==null) throw new IllegalStateException("HOME retirement capability is not configured");
        if (!identity.groupId().equals(groupId) || identity.rangeEpoch()!=rangeEpoch || !Arrays.equals(identity.schemaDigest(),schemaDigest) || !identity.columnId().equals(context.column())
                || !identity.definition().clusterId().equals(context.cluster()) || !identity.definition().databaseId().equals(context.database()) || !columns.keySet().equals(Set.of(context.column())) || !contains(uuidBytes(identity.transactionId()))) throw new IllegalArgumentException("HOME identity differs from native role/ownership");
        byte[] marker=stateRow(markerKey,options); var actual=marker==null ? null : decodePosition(marker);
        if (!Objects.equals(actual,cut)) throw new IllegalStateException("HOME proof cut differs from completed native marker");
        var authority=sessionAuthority(options);
        if (authority!=null && (!authority.clusterId().equals(context.cluster()) || !authority.databaseId().equals(context.database()))) throw new IllegalArgumentException("HOME identity differs from native session authority");
    }
    private record HomeState(TransactionStatus row,HomeRecoveryCredit credit,boolean matching) {}
    private static boolean sameHomeReservationDefinition(TransactionStatus left,TransactionStatus right) {
        if(left.sameDefinition(right))return true;
        var reserved=left.reserved() ? left : right.reserved() ? right : null;var installed=reserved==left ? right : left;
        if(reserved==null || installed.origin()==null || !Objects.equals(reserved.origin(),installed.origin()) || installed.participants().isEmpty())return false;
        var initial=installed.asInitial();
        try { return reserved.promote(TransactionStatus.begin(initial.clusterId(),initial.databaseId(),initial.domainId(),initial.transactionId(),initial.operation(),initial.schemaEpoch(),initial.schemaDigest(),initial.planDigest(),initial.participants())).sameDefinition(installed); }
        catch(IllegalArgumentException different) { return false; }
    }
    private static boolean sameHomeReservationIdentity(HomeIdentity left,HomeIdentity right) {
        return left.groupId().equals(right.groupId()) && left.rangeEpoch()==right.rangeEpoch() && Arrays.equals(left.schemaDigest(),right.schemaDigest()) && left.columnId().equals(right.columnId()) && sameHomeReservationDefinition(left.definition(),right.definition());
    }
    private HomeState readHomeState(HomeIdentity identity,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        requireHomeIdentity(identity,options,cut); byte[] key=uuidBytes(identity.transactionId()); var column=columns.get(identity.columnId());
        byte[] bytes=options==null ? db.get(column,key) : db.get(column,options,key); var row=bytes==null ? null : TransactionStatus.decode(bytes);
        if (row!=null && (!row.transactionId().equals(identity.transactionId()) || !row.clusterId().equals(homeRecoveryContext.cluster()) || !row.databaseId().equals(homeRecoveryContext.database()))) throw new IllegalArgumentException("HOME USER row identity changed");
        bytes=stateRow(homeCreditKey(identity.transactionId()),options); var credit=bytes==null ? null : HomeRecoveryCredit.decode(bytes);
        if (credit!=null) {
            credit.requireCut(cut); credit.requireOwner(owner,interval,schemaDigest);
            if (!credit.column().equals(identity.columnId()) || !credit.cluster().equals(homeRecoveryContext.cluster()) || !credit.database().equals(homeRecoveryContext.database()) || row==null || !credit.current().equals(row)) throw new IllegalStateException("HOME USER row and recovery promise differ");
        }
        if (row!=null && HomeRecoveryCredit.remaining(row)>0 && credit==null) throw new IllegalStateException("Unfinished HOME USER row lacks recovery promise");
        boolean matching=row!=null && row.operation().equals(identity.business());
        if (matching && !sameHomeReservationDefinition(row,identity.definition())) throw new IllegalArgumentException("Retained HOME definition changed");
        return new HomeState(row,credit,matching);
    }
    private List<HomeRecoveryReceipt> matchingHomeReceipts(HomeIdentity identity,ReadOptions source,ApplyPosition cut,TransactionStatus row) throws RocksDBException {
        var matched=new ArrayList<HomeRecoveryReceipt>();
        if (source==null) {
            for (var cached:homeRecoveries.values()) {
                var recovery=cached.recovery();
                if (!recovery.before().transactionId().equals(identity.transactionId()) || !recovery.before().operation().equals(identity.business())) continue;
                byte[] bytes=db.get(state,homeRecoveryKey(recovery.operation()));
                if (bytes==null) throw new IllegalStateException("HOME recovery index lost its native row");
                var actual=HomeRecoveryReceipt.decode(bytes);
                if (!actual.equals(cached)) throw new IllegalStateException("HOME recovery index payload changed");
                requireHomeRecovery(recovery); actual.requireCut(cut);
                if (!sameHomeReservationIdentity(homeIdentity(recovery),identity)) throw new IllegalArgumentException("Retained HOME recovery definition changed");
                requireHomeRecoverySuccessor(actual,row); matched.add(actual);
            }
            return List.copyOf(matched);
        }
        int count=0; long bytes=0;
        try (var low=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x25).array()); var high=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x26).array());
             var options=(source==null ? new ReadOptions() : new ReadOptions(source)).setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high); var iterator=databaseOwner.newIterator(state,options)) {
            iterator.seekToFirst(); var sizing=new byte[0];
            while (iterator.isValid()) {
                if (++count>MAX_HOME_RECOVERIES || iterator.key(sizing)!=49 || iterator.value(sizing)>HomeRecoveryReceipt.MAX_BYTES) throw new IllegalStateException("HOME recovery scan bound exceeded");
                bytes+=49L+iterator.value(sizing); if (bytes>MAX_HOME_RECOVERY_BYTES) throw new IllegalStateException("HOME recovery scan byte bound exceeded");
                var receipt=HomeRecoveryReceipt.decode(iterator.value()); var recovery=receipt.recovery(); requireHomeRecovery(recovery); receipt.requireCut(cut);
                if (!Arrays.equals(iterator.key(),homeRecoveryKey(recovery.operation()))) throw new IllegalArgumentException("HOME recovery key changed");
                if (recovery.before().transactionId().equals(identity.transactionId()) && recovery.before().operation().equals(identity.business())) {
                    if (!sameHomeReservationIdentity(homeIdentity(recovery),identity)) throw new IllegalArgumentException("Retained HOME recovery definition changed");
                    requireHomeRecoverySuccessor(receipt,row); matched.add(receipt);
                }
                iterator.next();
            }
            iterator.status(); return List.copyOf(matched);
        }
    }
    private HomeFenceProof readHomeFence(HomeIdentity identity,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        var state=readHomeState(identity,options,cut); if (state.matching() || identity.business().epoch()>=minimumSessionEpoch(options)) return null;
        matchingHomeReceipts(identity,options,cut,state.row()); // SUCCESS orphans are corruption, never retirement evidence.
        return new HomeFenceProof(identity,minimumSessionEpoch(options),cut);
    }
    public HomeFenceProof homeFence(HomeIdentity identity) throws RocksDBException {
        Objects.requireNonNull(identity); enterNative(); applyLane.lock();
        try { if (pendingInstallation!=null) throw new IllegalStateException("HOME metadata is being installed"); return readHomeFence(identity,null,applied); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    private boolean readHomeRetired(HomeRetirement retirement,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        var identity=retirement.identity(); var state=readHomeState(identity,options,cut);
        return !state.matching() && identity.business().epoch()<minimumSessionEpoch(options) && matchingHomeReceipts(identity,options,cut,state.row()).isEmpty();
    }
    public boolean homeRetired(HomeRetirement retirement) throws RocksDBException {
        Objects.requireNonNull(retirement); enterNative(); applyLane.lock();
        try { if (pendingInstallation!=null) throw new IllegalStateException("HOME metadata is being installed"); return readHomeRetired(retirement,null,applied); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    private record HomeRetirementPlan(HomeState state,List<HomeRecoveryReceipt> receipts) { boolean needed() { return state.matching() || !receipts.isEmpty(); } }
    private HomeRetirementPlan homeRetirementPlan(HomeRetirement retirement) throws RocksDBException {
        var identity=retirement.identity(); var state=readHomeState(identity,null,applied);
        if (identity.business().epoch()>=minimumSessionEpoch(null)) throw new IllegalStateException("HOME retirement requires a strictly newer native business floor");
        if (state.matching() && (retirement.consumed()==null || !state.row().gcEligible() || !state.row().equals(retirement.consumed()))) throw new IllegalArgumentException("HOME retirement lacks exact consumed USER image");
        if (state.matching() && state.credit()!=null) throw new IllegalStateException("Consumed HOME still has a recovery promise");
        return new HomeRetirementPlan(state,matchingHomeReceipts(identity,null,applied,state.row()));
    }
    public boolean estimateHomeRetirement(HomeRetirement retirement) throws RocksDBException {
        Objects.requireNonNull(retirement); enterNative(); applyLane.lock();
        try { requireActive(); return homeRetirementPlan(retirement).needed(); } finally { applyLane.unlock(); leaveNative(); }
    }
    public void applyHomeRetirement(ApplyPosition position,HomeRetirement retirement) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(retirement); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); var plan=homeRetirementPlan(retirement);
            if (!plan.needed()) { advanceCompletedMarker(position); return; }
            if (applied!=null && position.index()<=applied.index()) throw new IllegalStateException("Covered HOME retirement still retains its original family");
            requireNext(position); var identity=retirement.identity(); var key=uuidBytes(identity.transactionId());
            var effects=plan.state().matching() ? List.of(new Mutation(Mutation.Kind.DELETE,identity.columnId(),key,null)) : List.<Mutation>of();
            if (!effects.isEmpty()) { var scope=new TransactionScope(List.of(TransactionScope.Span.point(identity.columnId(),key))); requireUnlocked(scope); requireUngated(scope); }
            var next=new HashMap<>(homeRecoveries); long bytes=retainedHomeRecoveryBytes;
            for (var receipt:plan.receipts()) {
                if (!receipt.equals(next.remove(receipt.recovery().operation()))) throw new IllegalStateException("HOME recovery index differs from native family");
                bytes-=homeRecoveryBytes(receipt.recovery());
            }
            var published=Map.copyOf(next); var history=prepareScopedWrites(!effects.isEmpty(),effects,Map.of());
            try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) cdc.initialize(batch);
                if (plan.state().matching()) { batch.delete(columns.get(identity.columnId()),key); batch.delete(state,homeCreditKey(identity.transactionId())); }
                for (var receipt:plan.receipts()) batch.delete(state,homeRecoveryKey(receipt.recovery().operation()));
                batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            homeRecoveries=published; retainedHomeRecoveryBytes=bytes; homeStatusCapacityKnown=false; publishScoped(position,history,false); applied=position;
        } catch (RocksDBException | RuntimeException | Error failure) { resetScopedUnderLane(); throw failure; }
        finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private Map<OperationId,HomeRecoveryReceipt> loadHomeRecoveries() { return loadHomeRecoveries(applied); }
    private Map<OperationId,HomeRecoveryReceipt> loadHomeRecoveries(ApplyPosition cut) {
        var records=new HashMap<OperationId,HomeRecoveryReceipt>(); long bytes=0;
        try (var low=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x25).array());
             var high=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x26).array());
             var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high); var iterator=databaseOwner.newIterator(state,options)) {
            iterator.seekToFirst(); var sizing=new byte[0];
            while (iterator.isValid()) {
                if (records.size()>=MAX_HOME_RECOVERIES || iterator.key(sizing)!=49 || iterator.value(sizing)>HomeRecoveryReceipt.MAX_BYTES) throw new IllegalStateException("HOME recovery receipt bound exceeded");
                var receipt=HomeRecoveryReceipt.decode(iterator.value()); requireHomeRecovery(receipt.recovery()); receipt.requireCut(cut);
                if (!Arrays.equals(iterator.key(),homeRecoveryKey(receipt.recovery().operation()))) throw new IllegalArgumentException("HOME recovery receipt key changed");
                bytes+=49L+iterator.value(sizing); if (bytes>MAX_HOME_RECOVERY_BYTES || records.putIfAbsent(receipt.recovery().operation(),receipt)!=null) throw new IllegalStateException("HOME recovery receipt capacity/identity exceeded"); iterator.next();
            }
            iterator.status(); retainedHomeRecoveryBytes=bytes; return Map.copyOf(records);
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot recover protected HOME receipts",failure); }
    }
    private Map<UUID,HomeRecoveryCredit> loadHomeCredits(ApplyPosition cut) { return loadHomeCredits(cut,homeRecoveryContext); }
    private Map<UUID,HomeRecoveryCredit> loadHomeCredits(ApplyPosition cut,HomeRecoveryContext required) {
        var records=new HashMap<UUID,HomeRecoveryCredit>(); int count=0; long bytes=0; HomeRecoveryContext context=null;
        try (var low=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x26).array()); var high=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x27).array());
             var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high); var iterator=databaseOwner.newIterator(state,options)) {
            iterator.seekToFirst(); var sizing=new byte[0];
            while (iterator.isValid()) {
                if (records.size()>=MAX_HOME_RECOVERIES || iterator.key(sizing)!=33 || iterator.value(sizing)>HomeRecoveryCredit.MAX_BYTES) throw new IllegalStateException("HOME promise recovery bound exceeded");
                var credit=HomeRecoveryCredit.decode(iterator.value()); credit.requireCut(cut); credit.requireOwner(owner,interval,schemaDigest);
                if (!Arrays.equals(iterator.key(),homeCreditKey(credit.current().transactionId())) || !Arrays.equals(db.get(columns.get(credit.column()),credit.userKey()),credit.current().encode())) throw new IllegalStateException("HOME promise differs from user row");
                var nextContext=new HomeRecoveryContext(credit.cluster(),credit.database(),credit.column()); if (context!=null && !context.equals(nextContext) || required!=null && !required.equals(nextContext)) throw new IllegalStateException("HOME promise database context differs"); context=nextContext;
                count+=credit.reservedCount(); bytes+=credit.reservedBytes(); if (count>MAX_HOME_RECOVERIES || bytes>MAX_HOME_RECOVERY_BYTES) throw new IllegalStateException("HOME future promise capacity exceeded"); records.put(credit.current().transactionId(),credit); iterator.next();
            }
            iterator.status(); validateHomeUserCredits(records,required==null ? context : required); reservedHomeRecoveryCount=count; reservedHomeRecoveryBytes=bytes; return Map.copyOf(records);
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot recover HOME promises",failure); }
    }
    /** Streaming reverse proof, including the empty-credit case when the role supplies its context. */
    private void validateHomeUserCredits(Map<UUID,HomeRecoveryCredit> credits,HomeRecoveryContext context) throws RocksDBException { validateHomeUserCredits(credits,context,null); }
    private void validateHomeUserCredits(Map<UUID,HomeRecoveryCredit> credits,HomeRecoveryContext context,Snapshot snapshot) throws RocksDBException {
        if (context==null) return;
        if (!columns.keySet().equals(Set.of(context.column()))) throw new IllegalStateException("HOME promise context column differs from ownership");
        scanHomeStatusCapacity(context,false,snapshot);
        try (var low=interval.lowerInclusive()==null ? null : new org.rocksdb.Slice(interval.lowerInclusive());
             var high=interval.upperExclusive()==null ? null : new org.rocksdb.Slice(interval.upperExclusive()); var options=new ReadOptions().setFillCache(false)) {
            if (snapshot!=null) options.setSnapshot(snapshot);
            if (low!=null) options.setIterateLowerBound(low); if (high!=null) options.setIterateUpperBound(high);
            try (var iterator=databaseOwner.newIterator(columns.get(context.column()),options)) {
                iterator.seekToFirst(); var sizing=new byte[0];
                while (iterator.isValid()) {
                    if (iterator.key(sizing)!=16 || iterator.value(sizing)>TransactionStatus.MAX_BYTES) throw new IllegalStateException("Invalid HOME user row shape");
                    var status=TransactionStatus.decode(iterator.value());
                    if (!status.clusterId().equals(context.cluster()) || !status.databaseId().equals(context.database()) || !Arrays.equals(iterator.key(),uuidBytes(status.transactionId()))) throw new IllegalStateException("HOME user row database/key differs");
                    var credit=credits.get(status.transactionId());
                    if (HomeRecoveryCredit.remaining(status)>0 && (credit==null || !credit.current().equals(status)) || HomeRecoveryCredit.remaining(status)==0 && credit!=null) throw new IllegalStateException("Unfinished HOME user row has no exact future recovery promise");
                    iterator.next();
                }
                iterator.status();
            }
        }
    }
    private void validateHomeCreditCapacity() { if (homeRecoveryCapacityUnderLane().effectiveCount()>MAX_HOME_RECOVERIES || homeRecoveryCapacityUnderLane().effectiveBytes()>MAX_HOME_RECOVERY_BYTES) throw new IllegalStateException("Combined HOME receipt/promise capacity exceeded"); }
    private void validateHomeRecoveries() {
        try { for (var receipt:homeRecoveries.values()) { requireHomeRecoveryIdentity(receipt.recovery()); requireHomeSuccessor(receipt,null); } }
        catch (RocksDBException failure) { throw new IllegalStateException("Cannot validate HOME recovery identities",failure); }
    }

    private record ParticipantAnchor(UUID home,long epoch,UUID group,TransactionStatus definition) {}
    private void requireParticipantDefinition(UUID participantGroup,TransactionStatus definition,ReadOptions options) throws RocksDBException {
        var participant=definition.participants().stream().filter(p -> p.groupId().equals(participantGroup)).findFirst().orElseThrow(() -> new IllegalArgumentException("Unregistered participant identity"));
        if (!participantGroup.equals(groupId) || participant.rangeEpoch()!=rangeEpoch || !knownSchema(participant.schemaDigest())
                || !definition.domainId().equals(interval.domainId())) throw new IllegalArgumentException("Participant identity differs from native owner/schema");
        if (logicalSchema!=null && (!logicalSchema.clusterId().equals(definition.clusterId()) || !logicalSchema.databaseId().equals(definition.databaseId()))) throw new IllegalArgumentException("Participant identity database differs from schema");
        var authority=sessionAuthority(options);
        if (authority!=null && (!authority.clusterId().equals(definition.clusterId()) || !authority.databaseId().equals(definition.databaseId()))) throw new IllegalArgumentException("Participant identity database differs from native session authority");
    }
    private byte[] stateRow(byte[] key,ReadOptions options) throws RocksDBException { return options==null ? db.get(state,key) : db.get(state,options,key); }
    /** A floor proves retirement only when no retained row belongs to this original business anchor. */
    private boolean retiredParticipant(UUID home,long epoch,UUID participantGroup,TransactionStatus definition,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        requireParticipantDefinition(participantGroup,definition,options); var anchors=new ArrayList<ParticipantAnchor>(4); UUID transaction=definition.transactionId();
        byte[] bytes=stateRow(preparedKey(transaction),options);
        if (bytes!=null) { var record=PreparedParticipant.decode(bytes); requireParticipantCut(record.position(),cut); var preparation=record.preparation(); anchors.add(new ParticipantAnchor(preparation.homeGroup(),preparation.homeEpoch(),preparation.business().groupId(),preparation.registration())); }
        bytes=stateRow(abortedKey(transaction),options);
        if (bytes!=null) { var record=AbortedParticipant.decode(bytes); requireParticipantCut(record.position(),cut); var value=record.finalization(); anchors.add(new ParticipantAnchor(value.homeGroup(),value.homeEpoch(),value.participantGroup(),value.decision())); }
        bytes=stateRow(committedKey(transaction),options);
        if (bytes!=null) { var record=CommittedParticipant.decode(bytes); requireParticipantCut(record.position(),cut); var value=record.finalization(); anchors.add(new ParticipantAnchor(value.homeGroup(),value.homeEpoch(),value.participantGroup(),value.decision())); }
        bytes=stateRow(rejectedKey(transaction),options);
        if (bytes!=null) { var value=RejectedParticipant.decode(bytes); requireParticipantCut(value.position(),cut); anchors.add(new ParticipantAnchor(value.homeGroup(),value.homeEpoch(),value.participantGroup(),value.registration())); }
        ParticipantAnchor first=null;
        for (var anchor:anchors) {
            requireParticipantDefinition(anchor.group(),anchor.definition(),options);
            if (first==null) first=anchor;
            else if (!first.home().equals(anchor.home()) || first.epoch()!=anchor.epoch() || !first.group().equals(anchor.group()) || !first.definition().sameDefinition(anchor.definition())) throw new IllegalArgumentException("Native participant family has crossed definitions");
        }
        if (first!=null && first.definition().operation().equals(definition.operation())) {
            if (!first.home().equals(home) || first.epoch()!=epoch || !first.group().equals(participantGroup) || !first.definition().sameDefinition(definition)) throw new IllegalArgumentException("Retained participant definition changed");
            return false;
        }
        if (definition.operation().epoch()<minimumSessionEpoch(options)) return true;
        if (first!=null) throw new IllegalArgumentException("Transaction UUID belongs to another live business anchor");
        return false;
    }
    private static void requireParticipantCut(ApplyPosition position,ApplyPosition cut) {
        if (cut==null || position.term()>cut.term() || position.index()>cut.index() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("Participant record differs from completed native cut");
    }
    private void advanceCompletedMarker(ApplyPosition position) throws RocksDBException {
        if (applied!=null && position.index()<=applied.index()) {
            if (position.index()==applied.index() && position.term()!=applied.term()) throw new IllegalArgumentException("Covered participant callback changed completed term");
            return;
        }
        requireNext(position); var history=prepareScopedWrites(false,List.of(),Map.of());
        try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
            if(cdc!=null) cdc.initialize(batch);
            batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
        }
        publishScoped(position,history,false); applied=position;
    }

    public record FinalizationEstimate(PreparedCapacity capacity,boolean newFence,ApplyPosition existing,boolean retired) {}
    /** Admission observes terminal ownership/capacity at one completed cut; no old business grant is required. */
    public FinalizationEstimate estimateFinalization(ParticipantFinalization finalization) throws RocksDBException {
        Objects.requireNonNull(finalization); enterNative(); applyLane.lock();
        try {
            requireActive();
            if (readParticipantFence(identity(finalization),null,applied)!=null) return new FinalizationEstimate(preparedCapacityUnderLane(),false,null,true);
            requireFinalization(finalization);
            var commit=committedIndex.get(finalization.transactionId()); var abort=abortedIndex.get(finalization.transactionId());
            if (commit!=null || abort!=null) {
                var saved=commit!=null ? commit.finalization() : abort.finalization();
                if (!saved.equals(finalization)) throw new IllegalArgumentException("Finalization intent changed");
                return new FinalizationEstimate(preparedCapacityUnderLane(),false,commit!=null ? commit.position() : abort.position(),false);
            }
            if(activeTopologyFreeze!=null) throw new IllegalStateException("Frozen parent has no fresh finalization authority");
            var prepared=preparedIndex.records().stream().filter(p -> p.preparation().transactionId().equals(finalization.transactionId())).findFirst().orElse(null);
            if (prepared!=null) finalization.requirePrepared(prepared);
            else if (finalization.decision().phase()==TransactionStatus.Phase.COMMIT || finalization.participant().prepared()!=null) throw new IllegalStateException("Finalization requires missing native preparation");
            for (var p:preparedIndex.records()) if (p!=prepared && (p.preparation().ownsOperation(finalization.operation()) || p.preparation().ownsOperation(finalization.decision().operation()))) throw new IllegalArgumentException("Finalization identity belongs to another preparation");
            for (var other:abortedIndex.values()) if (other.ownsOperation(finalization.operation()) || other.ownsOperation(finalization.decision().operation())) throw new IllegalArgumentException("Finalization identity belongs to another ABORT");
            for (var other:committedIndex.values()) if (other.ownsOperation(finalization.operation()) || other.ownsOperation(finalization.decision().operation())) throw new IllegalArgumentException("Finalization identity belongs to another COMMIT");
            if (db.get(receipts,receiptKey(finalization.operation()))!=null || db.get(receipts,receiptKey(finalization.decision().operation()))!=null) throw new IllegalArgumentException("Finalization identity already has an ordinary receipt");
            var rejected=rejectedIndex.get(finalization.transactionId()); if (rejected!=null) rejected.requireFinalization(finalization);
            requireFinalizationRejectionIdentity(finalization);
            var capacity=preparedCapacityUnderLane(); boolean newFence=prepared==null && rejected==null;
            if (newFence) { requireTopologyWritable(); requireSchemaAdmission(); }
            if (newFence && (capacity.count()>=MAX_PREPARED_PARTICIPANTS || FINALIZATION_RESERVATION>MAX_PREPARED_BYTES-capacity.bytes())) throw new IllegalStateException("Finalization capacity exhausted");
            return new FinalizationEstimate(capacity,newFence,null,false);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Exact committed-native proof is available while fenced, before activation after image install. */
    public ParticipantFinalizationResult recoveredFinalization(ParticipantFinalization expected) throws RocksDBException {
        Objects.requireNonNull(expected); enterNative(); applyLane.lock();
        try { return readFinalizationResult(expected,null,applied); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    private ParticipantFinalizationResult readFinalizationResult(ParticipantFinalization expected,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        var retirement=readParticipantFence(identity(expected),options,cut); if (retirement!=null) return ParticipantFinalizationResult.retired(retirement);
        byte[] committed=options==null ? db.get(state,committedKey(expected.transactionId())) : db.get(state,options,committedKey(expected.transactionId()));
        byte[] aborted=options==null ? db.get(state,abortedKey(expected.transactionId())) : db.get(state,options,abortedKey(expected.transactionId()));
        if (committed!=null && aborted!=null) throw new IllegalArgumentException("Conflicting native finalization decisions");
        if (committed==null && aborted==null) return null;
        ParticipantFinalization actual; ApplyPosition position;
        if (committed!=null) { var receipt=CommittedParticipant.decode(committed); actual=receipt.finalization(); position=receipt.position(); }
        else { var receipt=AbortedParticipant.decode(aborted); actual=receipt.finalization(); position=receipt.position(); }
        requireFinalization(actual);
        byte[] prepared=stateRow(preparedKey(actual.transactionId()),options);
        if (prepared==null && (actual.decision().phase()==TransactionStatus.Phase.COMMIT || actual.participant().prepared()!=null)) throw new IllegalArgumentException("Native finalization lacks required positive preparation");
        if (prepared!=null) requireTerminalAfterPreparation(actual,position,PreparedParticipant.decode(prepared));
        if (!actual.equals(expected) || cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("Native finalization proof differs");
        return ParticipantFinalizationResult.applied(position);
    }
    private static void requireTerminalAfterPreparation(ParticipantFinalization terminal,ApplyPosition position,PreparedParticipant prepared) {
        terminal.requirePrepared(prepared);
        if (position.index()<=prepared.position().index() || position.term()<prepared.position().term()) throw new IllegalArgumentException("Participant finalization does not follow native preparation");
    }
    private static ParticipantIdentity identity(ParticipantFinalization value) { return new ParticipantIdentity(value.homeGroup(),value.homeEpoch(),value.participantGroup(),value.decision()); }
    private static ParticipantIdentity identity(PreparedParticipant.Preparation value) { return new ParticipantIdentity(value.homeGroup(),value.homeEpoch(),value.business().groupId(),value.registration()); }
    private ParticipantFenceProof readParticipantFence(ParticipantIdentity identity,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        byte[] marker=stateRow(markerKey,options); var observed=marker==null ? null : decodePosition(marker);
        if (!Objects.equals(cut,observed)) throw new IllegalStateException("Participant proof cut differs from completed native marker");
        if (!retiredParticipant(identity.homeGroup(),identity.homeEpoch(),identity.participantGroup(),identity.definition(),options,cut)) return null;
        return new ParticipantFenceProof(identity,minimumSessionEpoch(options),cut);
    }
    /** Atomic current-cut absence/floor evidence; never invents a terminal effect or an original APOS. */
    public ParticipantFenceProof participantFence(ParticipantIdentity identity) throws RocksDBException {
        Objects.requireNonNull(identity); enterNative(); applyLane.lock();
        try { return readParticipantFence(identity,null,applied); } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Trusted committed FINALIZE seam. A retired original business can only advance the completed marker. */
    public ParticipantFinalizationResult applyFinalization(ApplyPosition position,ParticipantFinalization finalization) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(finalization); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); var identity=identity(finalization);
            if (readParticipantFence(identity,null,applied)!=null) {
                advanceCompletedMarker(position); return ParticipantFinalizationResult.retired(readParticipantFence(identity,null,applied));
            }
            var original=finalization.decision().phase()==TransactionStatus.Phase.COMMIT ? applyCommit(position,finalization).position() : applyAbort(position,finalization).position();
            return ParticipantFinalizationResult.applied(original);
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    /** Preflight at one native cut. False means the original family is already fenced absent. */
    public boolean estimateParticipantRetirement(ParticipantRetirement retirement) throws RocksDBException {
        Objects.requireNonNull(retirement); enterNative(); applyLane.lock();
        try { requireActive(); return retirementNeeded(retirement); } finally { applyLane.unlock(); leaveNative(); }
    }
    private boolean retirementNeeded(ParticipantRetirement retirement) throws RocksDBException {
        var identity=retirement.identity();
        if (readParticipantFence(identity,null,applied)!=null) return false;
        if (identity.business().epoch()>=minimumSessionEpoch(null)) throw new IllegalStateException("Participant retirement requires a strictly newer native session epoch floor");
        var terminal=readFinalizationResult(retirement.finalization(),null,applied);
        if (terminal==null || terminal.retired() || !terminal.position().equals(retirement.position())) throw new IllegalArgumentException("Participant retirement lacks exact native terminal proof");
        byte[] bytes=db.get(state,preparedKey(identity.transactionId()));
        if (bytes!=null) requireTerminalAfterPreparation(retirement.finalization(),retirement.position(),PreparedParticipant.decode(bytes));
        bytes=db.get(state,rejectedKey(identity.transactionId()));
        if (bytes!=null) RejectedParticipant.decode(bytes).requireFinalization(retirement.finalization());
        return true;
    }
    /** Qualified consumed HOME authority is supplied by the adapter. All four family rows disappear in one native batch. */
    public void applyParticipantRetirement(ApplyPosition position,ParticipantRetirement retirement) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(retirement); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); var identity=retirement.identity();
            if (!retirementNeeded(retirement)) { advanceCompletedMarker(position); return; }
            if (applied!=null && position.index()<=applied.index()) throw new IllegalStateException("Covered retirement callback still has a retained family");
            requireNext(position); var transaction=identity.transactionId();
            var remaining=preparedIndex.records().stream().filter(record -> !record.preparation().transactionId().equals(transaction)).toList();
            long bytes=0,scopeBytes=0;
            for (var record:remaining) { bytes+=33L+record.encode().length+FINALIZATION_RESERVATION; scopeBytes+=RangeMutationHistory.accountedScopeBytes(record.preparation().scope()); }
            var nextPrepared=new PreparedIndex(remaining,bytes,scopeBytes);
            var nextAborted=new HashMap<>(abortedIndex); nextAborted.remove(transaction);
            var nextCommitted=new HashMap<>(committedIndex); nextCommitted.remove(transaction);
            var nextRejected=new HashMap<>(rejectedIndex); nextRejected.remove(transaction);
            var publishedAborted=Map.copyOf(nextAborted); var publishedCommitted=Map.copyOf(nextCommitted); var publishedRejected=Map.copyOf(nextRejected);
            var history=prepareScopedWrites(false,List.of(),Map.of());
            try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) cdc.initialize(batch);
                batch.delete(state,preparedKey(transaction)); batch.delete(state,abortedKey(transaction)); batch.delete(state,committedKey(transaction)); batch.delete(state,rejectedKey(transaction));
                batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            preparedIndex=nextPrepared; abortedIndex=publishedAborted; committedIndex=publishedCommitted; rejectedIndex=publishedRejected;
            publishScoped(position,history,false); applied=position;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private boolean isFinalized(UUID transaction) { return abortedIndex.containsKey(transaction) || committedIndex.containsKey(transaction); }
    private byte[] committedKey(UUID transaction) { return ByteBuffer.allocate(33).put(markerKey).put((byte)0x22).putLong(transaction.getMostSignificantBits()).putLong(transaction.getLeastSignificantBits()).array(); }
    private Map<UUID,CommittedParticipant> loadCommittedIndex() {
        var records=new HashMap<UUID,CommittedParticipant>();
        try (var low=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x22).array());
             var high=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x23).array());
             var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high); var iterator=databaseOwner.newIterator(state,options)) {
            iterator.seekToFirst(); byte[] sizing=new byte[0];
            while (iterator.isValid()) {
                if (records.size()>=MAX_PREPARED_PARTICIPANTS || iterator.key(sizing)!=33 || iterator.value(sizing)>CommittedParticipant.MAX_BYTES) throw new IllegalStateException("COMMIT recovery bound exceeded");
                var record=CommittedParticipant.decode(iterator.value()); requireFinalization(record.finalization()); var id=record.finalization().transactionId();
                if (!Arrays.equals(iterator.key(),committedKey(id)) || applied==null || record.position().index()>applied.index() || record.position().term()>applied.term() || record.position().index()==applied.index() && record.position().term()!=applied.term()) throw new IllegalStateException("COMMIT receipt differs from completed cut");
                records.put(id,record); iterator.next();
            }
            iterator.status(); return Map.copyOf(records);
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot recover COMMIT receipts",failure); }
    }
    private void validateCommittedIndex() {
        for (var commit:committedIndex.values()) {
            var id=commit.finalization().transactionId(); if (abortedIndex.containsKey(id)) throw new IllegalArgumentException("COMMIT and ABORT coexist");
            var prepared=preparedIndex.records().stream().filter(p -> p.preparation().transactionId().equals(id)).findFirst().orElseThrow(() -> new IllegalArgumentException("COMMIT has no retained preparation"));
            commit.finalization().requirePrepared(prepared);
            for (var p:preparedIndex.records()) if (p!=prepared && (commit.ownsOperation(p.preparation().operation()) || commit.ownsOperation(p.preparation().business().operationId()))) throw new IllegalArgumentException("COMMIT/preparation identity reused");
            for (var abort:abortedIndex.values()) if (abort.ownsOperation(commit.finalization().operation()) || abort.ownsOperation(commit.finalization().decision().operation())) throw new IllegalArgumentException("COMMIT/ABORT identity reused");
            for (var other:committedIndex.values()) if (other!=commit && (other.ownsOperation(commit.finalization().operation()) || other.ownsOperation(commit.finalization().decision().operation()))) throw new IllegalArgumentException("COMMIT identity reused");
        }
    }
    /** Trusted committed COMMIT seam. Recovery does not depend on the original business grant remaining live. */
    private CommittedParticipant applyCommit(ApplyPosition position,ParticipantFinalization finalization) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(finalization); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); var candidate=new CommittedParticipant(finalization,position); requireFinalization(finalization);
            if (abortedIndex.containsKey(finalization.transactionId())) throw new IllegalArgumentException("ABORT decision cannot become COMMIT");
            var saved=committedIndex.get(finalization.transactionId());
            if(activeTopologyFreeze!=null && saved==null) throw new IllegalStateException("Frozen parent cannot accept fresh COMMIT");
            if (saved!=null && !saved.finalization().equals(finalization)) throw new IllegalArgumentException("COMMIT intent changed");
            requireFinalizationRejectionIdentity(finalization);
            var prepared=preparedIndex.records().stream().filter(p -> p.preparation().transactionId().equals(finalization.transactionId())).findFirst().orElseThrow(() -> new IllegalStateException("COMMIT requires native preparation"));
            finalization.requirePrepared(prepared);
            if (applied!=null && position.index()<=applied.index()) { if (saved==null) throw new IllegalStateException("Applied COMMIT lacks its receipt"); return saved; }
            requireNext(position);
            Evaluation evaluation=null;
            if (saved==null) {
                for (var p:preparedIndex.records()) if (p!=prepared && p.preparation().ownsOperation(finalization.operation())) throw new IllegalArgumentException("COMMIT recovery identity belongs to another prepare");
                for (var abort:abortedIndex.values()) if (abort.ownsOperation(finalization.operation())) throw new IllegalArgumentException("COMMIT recovery identity belongs to ABORT");
                for (var other:committedIndex.values()) if (other.ownsOperation(finalization.operation())) throw new IllegalArgumentException("COMMIT recovery identity reused");
                if (db.get(receipts,receiptKey(finalization.operation()))!=null || db.get(receipts,receiptKey(prepared.preparation().business().operationId()))!=null) throw new IllegalArgumentException("COMMIT identity already has an ordinary receipt");
                evaluation=evaluate(prepared.preparation().business());
                if (!Arrays.equals(evaluation.result().encode(),prepared.preparation().business().acceptance().result()) || evaluation.result().status()!=CommandResult.Status.SUCCESS) throw new IllegalStateException("COMMIT effects disagree with prepared result");
            }
            var effects=evaluation==null ? List.<Mutation>of() : evaluation.effects(); var buckets=evaluation==null ? Map.<BucketKey,DistributedBucket>of() : evaluation.buckets();
            var history=prepareScopedWrites(saved==null,effects,buckets); var result=saved==null ? candidate : saved;
            var next=new HashMap<>(committedIndex); next.put(finalization.transactionId(),result); var published=Map.copyOf(next);
            Runnable publishCdc=()->{};
            try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) cdc.initialize(batch);
                for (var mutation:effects) {
                    var cf=columns.get(mutation.columnId());
                    switch (mutation.kind()) {
                        case PUT -> batch.put(cf,mutation.key(),mutation.value()); case DELETE -> batch.delete(cf,mutation.key());
                        case MERGE -> batch.merge(cf,mutation.key(),mutation.value()); case DELETE_RANGE -> batch.deleteRange(cf,mutation.key(),mutation.value());
                        case BUCKET_PUT, BUCKET_DELETE, BUCKET_MERGE -> throw new IllegalStateException("Unlowered bucket finalization effect");
                    }
                }
                for (var entry:buckets.entrySet()) batch.put(columns.get(entry.getKey().column),entry.getKey().key,entry.getValue().encode());
                if(cdc!=null && saved==null && !prepared.preparation().business().mutations().isEmpty()) publishCdc=cdc.append(batch,
                        new it.cavallium.rockserver.core.common.distributed.DistributedCdcRecord(cdc.lineage(),position,prepared.preparation().business(),finalization));
                batch.put(state,committedKey(finalization.transactionId()),result.encode()); batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            publishCdc.run();
            committedIndex=published; publishScoped(position,history,false); applied=position; return result;
        } catch (RocksDBException | RuntimeException | Error failure) { resetScopedUnderLane(); throw failure; }
        finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    public CommittedParticipant committedParticipant(UUID transaction) { enterNative(); applyLane.lock(); try { requireActive(); return committedIndex.get(transaction); } finally { applyLane.unlock(); leaveNative(); } }
    /** One completed native cut; the adapter supplies a fresh linearizable barrier and owner check. */
    public ParticipantOutcome finalizedParticipant(UUID transaction) {
        Objects.requireNonNull(transaction); enterNative(); applyLane.lock();
        try {
            requireActive(); var commit=committedIndex.get(transaction); var abort=abortedIndex.get(transaction);
            if (commit==null && abort==null) return null;
            var prepared=preparedIndex.records().stream().filter(p -> p.preparation().transactionId().equals(transaction)).findFirst().orElse(null);
            if (commit!=null) return new ParticipantOutcome(commit.finalization(),commit.position(),ParticipantVote.prepared(Objects.requireNonNull(prepared,"COMMIT preparation missing").position()),commit.result(prepared));
            var vote=prepared!=null ? ParticipantVote.prepared(prepared.position()) : rejectedIndex.containsKey(transaction) ? ParticipantVote.negative() : null;
            return new ParticipantOutcome(abort.finalization(),abort.position(),vote,null);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Retained business result is independent of ordinary receipt/session GC. */
    public byte[] committedResult(UUID transaction,byte[] acceptedPlanDigest) {
        enterNative(); applyLane.lock();
        try {
            requireActive(); var commit=committedIndex.get(transaction); if (commit==null) return null;
            var prepared=preparedIndex.records().stream().filter(p -> p.preparation().transactionId().equals(transaction)).findFirst().orElseThrow(() -> new IllegalStateException("COMMIT plan missing"));
            if (!Arrays.equals(prepared.preparation().business().payloadDigest(),acceptedPlanDigest)) throw new IllegalArgumentException("COMMIT result plan changed"); return commit.result(prepared);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public record GateCapacity(ApplyPosition position,int count,long bytes) {}
    public record GateEstimate(GateCapacity capacity,SnapshotGate existing,SnapshotFenceProof retirement) {}
    private byte[] snapshotGateKey(UUID fence) { return ByteBuffer.allocate(33).put(markerKey).put((byte)0x24).putLong(fence.getMostSignificantBits()).putLong(fence.getLeastSignificantBits()).array(); }
    private void requireSnapshotFence(SnapshotFence fence) { requireSnapshotFence(fence,false); }
    private void requireSnapshotFence(SnapshotFence fence,boolean terminal) {
        var participant=fence.participant(groupId);
        if (participant.rangeEpoch()!=rangeEpoch || !knownSchema(participant.schemaDigest()) || !fence.domainId().equals(interval.domainId())) throw new IllegalArgumentException("Snapshot fence differs from range");
        if (logicalSchema!=null && (!fence.clusterId().equals(logicalSchema.clusterId()) || !fence.databaseId().equals(logicalSchema.databaseId()))) throw new IllegalArgumentException("Snapshot fence database differs from schema");
        if(Arrays.equals(participant.schemaDigest(),schemaDigest)) validateSnapshotScope(participant.scope());
        else {
            var historical=schemaHistory.get(schemaIdentity(participant.schemaDigest()));
            if(!terminal || historical==null) throw new IllegalArgumentException("Historical snapshot fence cannot acquire admission");
            validateSnapshotScope(participant.scope(),historical.columnIds());
        }
    }
    private Map<UUID,SnapshotGate> loadSnapshotGates() {
        var records=new HashMap<UUID,SnapshotGate>();
        try (var low=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x24).array());
             var high=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x25).array());
             var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high); var iterator=databaseOwner.newIterator(state,options)) {
            iterator.seekToFirst(); byte[] sizing=new byte[0];
            while (iterator.isValid()) {
                if (records.size()>=MAX_SNAPSHOT_GATES || iterator.key(sizing)!=33 || iterator.value(sizing)>SnapshotGate.MAX_BYTES) throw new IllegalStateException("Snapshot gate recovery bound exceeded");
                var record=SnapshotGate.decode(iterator.value()); requireSnapshotFence(record.fence(),!record.active());
                try { record.requireCompletedCut(applied); } catch (IllegalArgumentException invalid) { throw new IllegalStateException("Snapshot gate differs from completed cut",invalid); }
                if (!record.participantGroup().equals(groupId) || !Arrays.equals(iterator.key(),snapshotGateKey(record.fence().fenceId()))) throw new IllegalStateException("Snapshot gate recovery identity differs");
                records.put(record.fence().fenceId(),record); iterator.next();
            }
            iterator.status(); long bytes=0; for (var record:records.values()) bytes+=snapshotGateBytes(record.fence());
            retainedGateBytes=bytes; return Map.copyOf(records);
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot recover snapshot gates",failure); }
    }
    public static long snapshotGateBytes(SnapshotFence fence) { return 33L+fence.encode().length+128; }
    private GateCapacity gateCapacityUnderLane() {
        return new GateCapacity(applied,snapshotGates.size(),retainedGateBytes);
    }
    private void requireGateCapacity(SnapshotFence fence) {
        var capacity=gateCapacityUnderLane();
        if (capacity.count()>=MAX_SNAPSHOT_GATES || snapshotGateBytes(fence)>MAX_SNAPSHOT_GATE_BYTES-capacity.bytes()) throw new IllegalStateException("Snapshot gate capacity exhausted");
    }
    private void requireGateIdentity(SnapshotFence fence) throws RocksDBException {
        var participant=fence.participant(groupId);
        for (var id:List.of(fence.operation(),participant.installOperation(),participant.releaseOperation())) {
            requireHomeRecoveryFree(id);
            for (var prepared:preparedIndex.records()) if (prepared.preparation().ownsOperation(id)) throw new IllegalArgumentException("Gate identity belongs to preparation");
            for (var rejected:rejectedIndex.values()) if (rejected.ownsOperation(id)) throw new IllegalArgumentException("Gate identity belongs to rejection");
            for (var abort:abortedIndex.values()) if (abort.ownsOperation(id)) throw new IllegalArgumentException("Gate identity belongs to ABORT");
            for (var commit:committedIndex.values()) if (commit.ownsOperation(id)) throw new IllegalArgumentException("Gate identity belongs to COMMIT");
            for (var other:snapshotGates.values()) if (!other.fence().fenceId().equals(fence.fenceId()) && other.ownsOperation(id)) throw new IllegalArgumentException("Gate identity reused");
            if (db.get(receipts,receiptKey(id))!=null) throw new IllegalArgumentException("Gate identity has an ordinary receipt");
        }
    }
    private void validateSnapshotGates() {
        for (var gate:snapshotGates.values()) {
            try { requireGateIdentity(gate.fence()); } catch (RocksDBException failure) { throw new IllegalStateException("Cannot validate gate identities",failure); }
            if (gate.active()) for (var other:snapshotGates.values()) if (other!=gate && other.active() && gate.fence().participant(groupId).scope().intersects(other.fence().participant(groupId).scope())) throw new IllegalArgumentException("Active snapshot gates overlap");
        }
        if (gateCapacityUnderLane().bytes()>MAX_SNAPSHOT_GATE_BYTES) throw new IllegalStateException("Snapshot gate recovery capacity exceeded");
    }
    private SnapshotGate requireActiveGate(SnapshotFence fence) {
        requireSnapshotFence(fence); var gate=snapshotGates.get(fence.fenceId());
        if (gate==null || !gate.fence().equals(fence) || !gate.active()) throw new IllegalStateException("Snapshot gate is not active for this fence"); return gate;
    }
    public GateCapacity gateCapacity() { enterNative(); applyLane.lock(); try { requireActive(); return gateCapacityUnderLane(); } finally { applyLane.unlock(); leaveNative(); } }
    public SnapshotGate snapshotGate(UUID fence) { enterNative(); applyLane.lock(); try { requireActive(); return snapshotGates.get(fence); } finally { applyLane.unlock(); leaveNative(); } }
    /** Exact native receipt lookup while fenced; it grants no new gate admission authority. */
    public SnapshotGate recoveredSnapshotGate(SnapshotFence fence) throws RocksDBException {
        Objects.requireNonNull(fence); enterNative(); applyLane.lock();
        try { return readSnapshotGate(fence,null,applied); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    private SnapshotGate readSnapshotGate(SnapshotFence fence,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        byte[] key=snapshotGateKey(fence.fenceId());
        byte[] bytes=options==null ? db.get(state,key) : db.get(state,options,key); if (bytes==null) { requireSnapshotFence(fence); return null; }
        var record=SnapshotGate.decode(bytes); requireSnapshotFence(fence,!record.active());
        record.requireCompletedCut(cut);
        if (!record.fence().equals(fence) || !record.participantGroup().equals(groupId)) throw new IllegalArgumentException("Recovered snapshot gate binding changed");
        return record;
    }
    /** Snapshot retention uses the same durable aggregate business epoch floor as native sessions. */
    public long snapshotRetentionFloor() throws RocksDBException {
        enterNative(); applyLane.lock(); try { requireActive(); return minimumSessionEpoch(null); } finally { applyLane.unlock(); leaveNative(); }
    }
    private SnapshotGateResult readSnapshotGateResult(SnapshotFence fence,ReadOptions options,ApplyPosition cut) throws RocksDBException {
        if (options==null && pendingInstallation!=null) throw new IllegalStateException("Snapshot gate metadata is being installed");
        byte[] marker=stateRow(markerKey,options); var actual=marker==null ? null : decodePosition(marker);
        if (!Objects.equals(cut,actual)) throw new IllegalStateException("Snapshot gate proof cut differs from completed native marker");
        var authority=sessionAuthority(options);
        if (authority!=null && (!authority.clusterId().equals(fence.clusterId()) || !authority.databaseId().equals(fence.databaseId()))) throw new IllegalArgumentException("Snapshot fence differs from native database authority");
        byte[] bytes=stateRow(snapshotGateKey(fence.fenceId()),options);
        if (bytes!=null) {
            var retained=SnapshotGate.decode(bytes); retained.requireCompletedCut(cut); requireSnapshotFence(retained.fence(),!retained.active());
            if (!retained.participantGroup().equals(groupId)) throw new IllegalArgumentException("Snapshot gate participant changed");
            if (retained.fence().operation().equals(fence.operation())) {
                if (!retained.fence().equals(fence)) throw new IllegalArgumentException("Retained snapshot business definition changed");
                return SnapshotGateResult.retained(retained);
            }
            if (fence.operation().epoch()>=minimumSessionEpoch(options)) throw new IllegalArgumentException("Snapshot UUID belongs to another live business anchor");
        }
        requireSnapshotFence(fence);
        long floor=minimumSessionEpoch(options);
        return fence.operation().epoch()<floor ? SnapshotGateResult.retired(new SnapshotFenceProof(fence,groupId,floor,cut)) : null;
    }
    public SnapshotGateResult snapshotGateResult(SnapshotFence fence) throws RocksDBException {
        Objects.requireNonNull(fence); enterNative(); applyLane.lock();
        try { return readSnapshotGateResult(fence,null,applied); } finally { applyLane.unlock(); leaveNative(); }
    }
    public SnapshotFenceProof snapshotFence(SnapshotFence fence) throws RocksDBException {
        var result=snapshotGateResult(fence); return result!=null && result.retired() ? result.retirement() : null;
    }
    private boolean snapshotGateRetirementNeeded(SnapshotGateRetirement retirement) throws RocksDBException {
        if (!retirement.participantGroup().equals(groupId)) throw new IllegalArgumentException("Snapshot retirement belongs to another participant");
        var result=readSnapshotGateResult(retirement.fence(),null,applied);
        if (result!=null && result.retired()) return false;
        if (retirement.fence().operation().epoch()>=minimumSessionEpoch(null)) throw new IllegalStateException("Snapshot retirement requires a strictly newer native business floor");
        var gate=result==null ? null : result.gate();
        if (gate==null || gate.released()==null || !gate.released().equals(retirement.released()) || !Arrays.equals(gate.decisionDigest(),retirement.consumed().terminalDecision().digest())) throw new IllegalArgumentException("Snapshot retirement lacks exact released native proof");
        return true;
    }
    public boolean estimateSnapshotGateRetirement(SnapshotGateRetirement retirement) throws RocksDBException {
        Objects.requireNonNull(retirement); enterNative(); applyLane.lock();
        try { requireActive(); return snapshotGateRetirementNeeded(retirement); } finally { applyLane.unlock(); leaveNative(); }
    }
    public void applySnapshotGateRetirement(ApplyPosition position,SnapshotGateRetirement retirement) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(retirement); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage();
            if (!snapshotGateRetirementNeeded(retirement)) { advanceCompletedMarker(position); return; }
            if (applied!=null && position.index()<=applied.index()) throw new IllegalStateException("Covered snapshot retirement still retains its original gate");
            requireNext(position); var saved=snapshotGates.get(retirement.fence().fenceId());
            if (saved==null || !saved.fence().equals(retirement.fence())) throw new IllegalStateException("Snapshot gate index differs from native retirement family");
            var next=new HashMap<>(snapshotGates); next.remove(retirement.fence().fenceId()); var published=Map.copyOf(next); long bytes=retainedGateBytes-snapshotGateBytes(saved.fence());
            var history=prepareScopedWrites(false,List.of(),Map.of());
            try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) cdc.initialize(batch);
                batch.delete(state,snapshotGateKey(retirement.fence().fenceId())); batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            snapshotGates=published; retainedGateBytes=bytes; publishScoped(position,history,false); applied=position;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    public GateEstimate estimateGateInstall(SnapshotFence fence) throws RocksDBException {
        Objects.requireNonNull(fence); enterNative(); applyLane.lock();
        try {
            requireActive(); var result=readSnapshotGateResult(fence,null,applied);
            if (result!=null && result.retired()) return new GateEstimate(gateCapacityUnderLane(),null,result.retirement());
            var saved=result==null ? null : result.gate();
            if (saved==null) { requireTopologyWritable(); requireSchemaAdmission(); requireGateIdentity(fence); requireUngated(fence.participant(groupId).scope()); requireGateCapacity(fence); }
            return new GateEstimate(gateCapacityUnderLane(),saved,null);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Trusted committed gate seam; the adapter must drain its pre-gate proposal owners before append. */
    public SnapshotGateResult applyGateInstall(ApplyPosition position,SnapshotFence fence) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(fence); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); var estimate=estimateGateInstall(fence); var saved=estimate.existing();
            if (estimate.retirement()!=null) { advanceCompletedMarker(position); return readSnapshotGateResult(fence,null,applied); }
            if (applied!=null && position.index()<=applied.index()) { if (saved==null) throw new IllegalStateException("Applied gate installation lacks receipt"); return SnapshotGateResult.retained(saved); }
            requireNext(position); var result=saved==null ? SnapshotGate.install(fence,groupId,position) : saved;
            publishGate(position,result); return SnapshotGateResult.retained(result);
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    public GateEstimate estimateGateRelease(SnapshotFenceStatus terminal) throws RocksDBException {
        Objects.requireNonNull(terminal); enterNative(); applyLane.lock();
        try {
            requireActive();
            if (!terminal.terminal()) throw new IllegalArgumentException("Snapshot gate release requires a terminal home decision");
            var result=readSnapshotGateResult(terminal.fence(),null,applied);
            if (result!=null && result.retired()) return new GateEstimate(gateCapacityUnderLane(),null,result.retirement());
            var saved=result==null ? null : result.gate();
            if (saved==null) { requireGateIdentity(terminal.fence()); requireGateCapacity(terminal.fence()); }
            if (applied==null || applied.index()==Long.MAX_VALUE) throw new IllegalStateException("Completed prefix unavailable/exhausted");
            var next=new ApplyPosition(Math.max(1,applied.term()),applied.index()+1);
            if (saved==null) SnapshotGate.released(terminal,groupId,next); else saved.release(terminal,next);
            return new GateEstimate(gateCapacityUnderLane(),saved,null);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Home read provenance is verified by the adapter; timers never authorize this release. */
    public SnapshotGateResult applyGateRelease(ApplyPosition position,SnapshotFenceStatus terminal) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(terminal); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); var estimate=estimateGateRelease(terminal); var saved=estimate.existing();
            if (estimate.retirement()!=null) { advanceCompletedMarker(position); return readSnapshotGateResult(terminal.fence(),null,applied); }
            if (applied!=null && position.index()<=applied.index()) { if (saved==null || saved.released()==null) throw new IllegalStateException("Applied gate release lacks receipt"); return SnapshotGateResult.retained(saved); }
            requireNext(position); var result=saved==null ? SnapshotGate.released(terminal,groupId,position) : saved.release(terminal,position);
            publishGate(position,result); return SnapshotGateResult.retained(result);
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private void publishGate(ApplyPosition position,SnapshotGate gate) throws RocksDBException {
        long bytes=retainedGateBytes+(snapshotGates.containsKey(gate.fence().fenceId()) ? 0 : snapshotGateBytes(gate.fence()));
        var next=new HashMap<>(snapshotGates); next.put(gate.fence().fenceId(),gate); var published=Map.copyOf(next);
        var history=prepareScopedWrites(false,List.of(),Map.of());
        try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
            if(cdc!=null) cdc.initialize(batch);
            batch.put(state,snapshotGateKey(gate.fence().fenceId()),gate.encode()); batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
        }
        snapshotGates=published; retainedGateBytes=bytes; publishScoped(position,history,false); applied=position;
    }
    /** Native readiness is necessary; the adapter must separately prove volatile predecessor drainage. */
    public List<PreparedParticipant> gateBlockers(SnapshotFence fence) {
        Objects.requireNonNull(fence); enterNative(); applyLane.lock();
        try { requireActive(); requireActiveGate(fence); var scope=fence.participant(groupId).scope(); return preparedIndex.records().stream().filter(p -> !isFinalized(p.preparation().transactionId()) && p.preparation().scope().intersects(scope)).toList(); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    public record SnapshotBlockerPage(SnapshotGate gate,ApplyPosition position,List<PreparedParticipant> records,boolean more) {
        public SnapshotBlockerPage { records=List.copyOf(records); }
    }
    /** Exclusive transaction-ID pagination at one native cut; an active gate prevents new positive blockers. */
    public SnapshotBlockerPage snapshotBlockers(SnapshotFence fence,UUID after,int limit) {
        Objects.requireNonNull(fence); if (limit<1 || limit>16) throw new IllegalArgumentException("Invalid snapshot blocker page limit");
        enterNative(); applyLane.lock();
        try {
            requireActive(); requireActiveGate(fence); var scope=fence.participant(groupId).scope();
            var records=preparedIndex.records().stream().filter(p -> !isFinalized(p.preparation().transactionId()) && p.preparation().scope().intersects(scope)
                    && (after==null || p.preparation().transactionId().compareTo(after)>0)).sorted(java.util.Comparator.comparing(p -> p.preparation().transactionId())).limit(limit+1L).toList();
            boolean more=records.size()>limit;
            return new SnapshotBlockerPage(snapshotGates.get(fence.fenceId()),applied,more ? records.subList(0,limit) : records,more);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public record PreparedRecoveryPage(ApplyPosition position,List<PreparedParticipant> records,boolean more) {
        public PreparedRecoveryPage {
            java.util.Objects.requireNonNull(position); records=List.copyOf(records);
            if (records.size()>16 || more && records.isEmpty()) throw new IllegalArgumentException("Invalid prepared recovery page bound");
            UUID previous=null;
            for (var record:records) {
                var id=record.preparation().transactionId(); var prepared=record.position();
                if (previous!=null && previous.compareTo(id)>=0 || prepared.term()>position.term() || prepared.index()>position.index()
                        || prepared.index()==position.index() && prepared.term()!=position.term()) throw new IllegalArgumentException("Invalid prepared recovery page order/cut");
                previous=id;
            }
        }
    }
    /** Retained positive votes include finalized participants whose home ACK may still need recovery. */
    public PreparedRecoveryPage preparedRecoveries(UUID after,int limit) {
        if (limit<1 || limit>16) throw new IllegalArgumentException("Invalid prepared recovery page limit");
        enterNative(); applyLane.lock();
        try {
            requireActive(); var records=preparedIndex.records().stream().filter(p -> after==null || p.preparation().transactionId().compareTo(after)>0)
                    .sorted(java.util.Comparator.comparing(p -> p.preparation().transactionId())).limit(limit+1L).toList();
            boolean more=records.size()>limit; return new PreparedRecoveryPage(applied,more ? records.subList(0,limit) : records,more);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    void enterCohortNative() { enterNative(); }
    void leaveCohortNative() { leaveNative(); }
    void lockCohortApply() { applyLane.lock(); }
    void unlockCohortApply() { applyLane.unlock(); }
    void requireCohortGate(SnapshotFence fence) throws RocksDBException {
        requireActive();requireSchemaAdmission();requireSnapshotFence(fence);
        if(schemaCaptureGate!=null)throw new IllegalStateException("INSTALL preparation blocks cohort capture");
        if(fence.operation().epoch()<minimumSessionEpoch(null))throw new SnapshotRetentionExpiredException();requireActiveGate(fence);
        var scope=fence.participant(groupId).scope();
        if(scope.spans().size()!=columns.size() || columns.keySet().stream().anyMatch(column->scope.spans().stream().noneMatch(span->span.columnId().equals(column) && !span.isPoint() && Arrays.equals(span.lowerInclusive(),interval.lowerInclusive()) && Arrays.equals(span.upperExclusive(),interval.upperExclusive()))))throw new IllegalArgumentException("Cohort requires exact full-column full-interval gates");
        requireUnlocked(scope);ensureReceiptUsage();
    }
    SnapshotPin cohortPin(Snapshot snapshot,PoolSnapshotAdmission.Lease lease,PhysicalSnapshotOwner physical,SnapshotFence fence) throws RocksDBException {
        try(var options=new ReadOptions().setSnapshot(snapshot)){
            byte[] marker=db.get(state,options,markerKey);var cut=marker==null ? null : decodePosition(marker);
            if(!Objects.equals(cut,applied))throw new IllegalStateException("Cohort snapshot marker differs from completed apply");
            var gate=readSnapshotGate(fence,options,cut);if(gate==null || !gate.active() || !Arrays.equals(gate.encode(),requireActiveGate(fence).encode()))throw new IllegalStateException("Cohort snapshot gate differs from completed state");
            if(fence.operation().epoch()<minimumSessionEpoch(options))throw new SnapshotRetentionExpiredException();
            return new SnapshotPin(snapshot,cut,lease,physical,false);
        }
    }
    /** A physical snapshot holds all member charges until the final alias's actual native users drain. */
    static final class PhysicalSnapshotOwner {
        private final RocksDB db;private Snapshot snapshot;private final PoolSnapshotAdmission.Lease lease;private final List<ReplicatedRangeStore> members;private final Runnable beforeRelease;
        private int remaining;private boolean releaseStarted;private volatile boolean releaseSucceeded;
        PhysicalSnapshotOwner(RocksDB db,PoolSnapshotAdmission.Lease lease,List<ReplicatedRangeStore> members,Runnable beforeRelease){this.db=db;this.lease=lease;this.members=List.copyOf(members);this.beforeRelease=beforeRelease;remaining=members.size();}
        void attach(Snapshot snapshot){this.snapshot=Objects.requireNonNull(snapshot);for(var member:members)member.snapshotPins.incrementAndGet();}
        void dropAlias(){boolean release; synchronized(this){release=--remaining==0 && !releaseStarted;if(release)releaseStarted=true;}if(release)releaseNative();}
        void abandon(){synchronized(this){if(releaseStarted)return;releaseStarted=true;}releaseNative();}
        private void releaseNative(){if(snapshot==null){if(lease!=null)lease.close();return;}try {beforeRelease.run();db.releaseSnapshot(snapshot);}catch(Throwable failure){for(var member:members)member.recordSnapshotReleaseFailure(failure);throw failure;}
            if(lease!=null)lease.close();for(var member:members)member.snapshotPins.decrementAndGet();for(var member:members){member.signalSchemaCaptureDrain();member.signalTopologyDrain();}releaseSucceeded=true;
        }
    }

    /** Transfers this existing cohort alias into a scoped owner; both views close the same pin exactly once. */
    public ScopedSnapshot scopeCohortPin(SnapshotPin owned,SnapshotFence fence) throws RocksDBException {
        Objects.requireNonNull(owned);Objects.requireNonNull(fence);
        if(!owned.belongsTo(this) || owned.physicalOwner==null)throw new IllegalArgumentException("Snapshot is not this store's owned cohort alias");
        owned.acquireUse();
        try(var options=new ReadOptions().setSnapshot(owned.snapshot)) {
            var participant=fence.participant(groupId);var coverage=owned.capturedInterval;var scope=participant.scope();
            if(participant.rangeEpoch()!=rangeEpoch || !Arrays.equals(participant.schemaDigest(),owned.capturedSchemaDigest) || !Arrays.equals(fence.schemaDigest(),owned.capturedSchemaDigest) || !fence.domainId().equals(coverage.domainId()) || scope.spans().size()!=owned.capturedColumns.size() || owned.capturedColumns.stream().anyMatch(column->scope.spans().stream().noneMatch(span->span.columnId().equals(column) && !span.isPoint() && Arrays.equals(span.lowerInclusive(),coverage.lowerInclusive()) && Arrays.equals(span.upperExclusive(),coverage.upperExclusive()))))throw new IllegalArgumentException("Scoped cohort fence changed captured schema/epoch/full interval");
            byte[] marker=db.get(state,options,markerKey);if(!Objects.equals(marker==null ? null : decodePosition(marker),owned.position))throw new IllegalStateException("Scoped cohort marker differs from its original cut");
            byte[] encoded=db.get(state,options,snapshotGateKey(fence.fenceId()));if(encoded==null)throw new IllegalArgumentException("Cohort cut has no requested native gate");var gate=SnapshotGate.decode(encoded);gate.requireCompletedCut(owned.position);
            if(!gate.active() || !gate.participantGroup().equals(groupId) || !gate.fence().equals(fence))throw new IllegalArgumentException("Cohort cut lacks exact active captured gate");
            if(fence.operation().epoch()<minimumSessionEpoch(options))throw new SnapshotRetentionExpiredException();owned.requireOpen();return new ScopedSnapshot(scope,owned);
        } finally {owned.releaseUse();}
    }

    public ScopedSnapshot captureGate(SnapshotFence fence) throws RocksDBException {
        Objects.requireNonNull(fence); enterNative(); applyLane.lock();
        try { requireActive(); requireSnapshotFence(fence); if (fence.operation().epoch()<minimumSessionEpoch(null)) throw new SnapshotRetentionExpiredException(); requireActiveGate(fence); var scope=fence.participant(groupId).scope(); requireUnlocked(scope); return new ScopedSnapshot(scope,captureUnderAdmission()); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    public record GateConflict(ApplyPosition position,boolean blocked) {}
    public GateConflict gateConflict(TransactionScope scope) {
        Objects.requireNonNull(scope); enterNative(); applyLane.lock();
        try { requireActive(); validateSnapshotScope(scope); return new GateConflict(applied,gated(scope)); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    private boolean gated(TransactionScope scope) { return snapshotGates.values().stream().anyMatch(gate -> gate.active() && gate.fence().participant(groupId).scope().intersects(scope)); }
    private void requireUngated(TransactionScope scope) { if (gated(scope)) throw new IllegalStateException("Write scope intersects an active snapshot gate"); }
    private void requireGateOperation(OperationId id) { for (var gate:snapshotGates.values()) if (gate.ownsOperation(id)) throw new IllegalArgumentException("Operation identity belongs to snapshot gate"); }
    private byte[] rejectedKey(UUID transaction) { return ByteBuffer.allocate(33).put(markerKey).put((byte)0x23).putLong(transaction.getMostSignificantBits()).putLong(transaction.getLeastSignificantBits()).array(); }
    private Set<UUID> unpreparedTransactions(PreparedIndex index) {
        var ids=new java.util.HashSet<>(abortedIndex.keySet()); ids.addAll(rejectedIndex.keySet());
        for (var record:index.records()) ids.remove(record.preparation().transactionId()); return ids;
    }
    private void requireRejected(RejectedParticipant record) {
        requireHomeRecoveryFree(record.operation()); requireHomeRecoveryFree(record.registration().operation());
        var participant=record.registration().participants().stream().filter(p -> p.groupId().equals(record.participantGroup())).findFirst().orElseThrow();
        if (!record.participantGroup().equals(groupId) || participant.rangeEpoch()!=rangeEpoch || !knownSchema(participant.schemaDigest())
                || !record.registration().domainId().equals(interval.domainId())) throw new IllegalArgumentException("Rejected preparation differs from range");
        if (logicalSchema!=null && (!logicalSchema.clusterId().equals(record.registration().clusterId()) || !logicalSchema.databaseId().equals(record.registration().databaseId()))) throw new IllegalArgumentException("Rejected preparation database differs from schema");
    }
    private Map<UUID,RejectedParticipant> loadRejectedIndex() {
        var records=new HashMap<UUID,RejectedParticipant>();
        try (var low=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x23).array());
             var high=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x24).array());
             var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high); var iterator=databaseOwner.newIterator(state,options)) {
            iterator.seekToFirst(); byte[] sizing=new byte[0];
            while (iterator.isValid()) {
                if (records.size()>=MAX_PREPARED_PARTICIPANTS || iterator.key(sizing)!=33 || iterator.value(sizing)>RejectedParticipant.MAX_BYTES) throw new IllegalStateException("Rejected preparation recovery bound exceeded");
                var record=RejectedParticipant.decode(iterator.value()); requireRejected(record);
                if (!Arrays.equals(iterator.key(),rejectedKey(record.transactionId())) || applied==null || record.position().index()>applied.index() || record.position().term()>applied.term() || record.position().index()==applied.index() && record.position().term()!=applied.term()) throw new IllegalStateException("Rejected preparation differs from completed cut");
                records.put(record.transactionId(),record); iterator.next();
            }
            iterator.status(); return Map.copyOf(records);
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot recover rejected preparations",failure); }
    }
    private void requireRejectionIdentity(PreparedParticipant.Preparation preparation) {
        requireGateOperation(preparation.operation()); requireGateOperation(preparation.business().operationId());
        for (var rejected:rejectedIndex.values()) if (rejected.transactionId().equals(preparation.transactionId())
                || rejected.ownsOperation(preparation.operation()) || rejected.ownsOperation(preparation.business().operationId())) throw new IllegalArgumentException("Identity belongs to rejected preparation");
    }
    private void requireFinalizationRejectionIdentity(ParticipantFinalization finalization) {
        requireGateOperation(finalization.operation()); requireGateOperation(finalization.decision().operation());
        for (var rejected:rejectedIndex.values()) {
            if (rejected.transactionId().equals(finalization.transactionId())) rejected.requireFinalization(finalization);
            else if (rejected.ownsOperation(finalization.operation()) || rejected.ownsOperation(finalization.decision().operation())) throw new IllegalArgumentException("Finalization identity belongs to another rejection");
        }
    }
    private void validateRejectedIndex() {
        for (var rejected:rejectedIndex.values()) {
            var id=rejected.transactionId();
            if (committedIndex.containsKey(id)) throw new IllegalArgumentException("COMMIT and rejection coexist");
            for (var prepared:preparedIndex.records()) if (prepared.preparation().transactionId().equals(id) || rejected.ownsOperation(prepared.preparation().operation()) || rejected.ownsOperation(prepared.preparation().business().operationId())) throw new IllegalArgumentException("Positive/negative preparation ownership reused");
            for (var other:rejectedIndex.values()) if (other!=rejected && (other.ownsOperation(rejected.operation()) || other.ownsOperation(rejected.registration().operation()))) throw new IllegalArgumentException("Rejected operation identity reused");
            for (var abort:abortedIndex.values()) {
                if (abort.finalization().transactionId().equals(id)) rejected.requireFinalization(abort.finalization());
                else if (abort.ownsOperation(rejected.operation()) || abort.ownsOperation(rejected.registration().operation())) throw new IllegalArgumentException("ABORT/rejection identity reused");
            }
            for (var commit:committedIndex.values()) if (commit.ownsOperation(rejected.operation()) || commit.ownsOperation(rejected.registration().operation())) throw new IllegalArgumentException("COMMIT/rejection identity reused");
            try {
                if (db.get(receipts,receiptKey(rejected.operation()))!=null || db.get(receipts,receiptKey(rejected.registration().operation()))!=null) throw new IllegalArgumentException("Rejected identity has an ordinary receipt");
            } catch (RocksDBException failure) { throw new IllegalStateException("Cannot validate rejected receipt ownership",failure); }
        }
    }
    public RejectedParticipant rejectedParticipant(UUID transaction) { enterNative(); applyLane.lock(); try { requireActive(); return rejectedIndex.get(transaction); } finally { applyLane.unlock(); leaveNative(); } }
    /** Trusted committed negative-vote seam. The adapter supplies gate/ordering authority; no home ABORT is implied. */
    public ParticipantVote applyRejected(ApplyPosition position,PreparedParticipant.Preparation preparation) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(preparation); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); var identity=identity(preparation);
            if (readParticipantFence(identity,null,applied)!=null) { advanceCompletedMarker(position); return ParticipantVote.retired(readParticipantFence(identity,null,applied)); }
            requirePreparation(preparation,true); return applyRejectedUnderLane(position,preparation);
        }
        finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private ParticipantVote applyRejectedUnderLane(ApplyPosition position,PreparedParticipant.Preparation preparation) throws RocksDBException {
        var saved=rejectedIndex.get(preparation.transactionId());
        if (saved!=null) saved.requirePreparation(preparation);
        else {
            requireNegativeIdentity(preparation);
            if (committedIndex.containsKey(preparation.transactionId())) throw new IllegalArgumentException("COMMIT cannot become a negative vote");
            var abort=abortedIndex.get(preparation.transactionId()); if (abort!=null) abort.finalization().requirePreparation(preparation);
        }
        if (applied!=null && position.index()<=applied.index()) { if (saved==null) throw new IllegalStateException("Applied rejection lacks its receipt"); return ParticipantVote.negative(); }
        requireNext(position);
        if (saved==null && !abortedIndex.containsKey(preparation.transactionId())) {
            requireTopologyWritable(); requireSchemaAdmission();
            var capacity=preparedCapacityUnderLane();
            if (capacity.count()>=MAX_PREPARED_PARTICIPANTS || FINALIZATION_RESERVATION>MAX_PREPARED_BYTES-capacity.bytes()) throw new IllegalStateException("Rejected preparation capacity exhausted");
        }
        var result=saved==null ? RejectedParticipant.from(preparation,position) : saved;
        var next=new HashMap<>(rejectedIndex); next.put(preparation.transactionId(),result); var published=Map.copyOf(next);
        var history=prepareScopedWrites(false,List.of(),Map.of());
        try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
            if(cdc!=null) cdc.initialize(batch);
            batch.put(state,rejectedKey(preparation.transactionId()),result.encode()); batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
        }
        rejectedIndex=published; publishScoped(position,history,false); applied=position; return ParticipantVote.negative();
    }
    private byte[] abortedKey(UUID transaction) { return ByteBuffer.allocate(33).put(markerKey).put((byte)0x21).putLong(transaction.getMostSignificantBits()).putLong(transaction.getLeastSignificantBits()).array(); }
    private void requireAbort(AbortedParticipant record) { requireFinalization(record.finalization()); }
    private void requireFinalization(ParticipantFinalization decision) {
        requireHomeRecoveryFree(decision.operation()); requireHomeRecoveryFree(decision.decision().operation());
        var participant=decision.participant();
        if (!decision.participantGroup().equals(owner.groupId()) || participant.rangeEpoch()!=owner.epoch() || !knownSchema(participant.schemaDigest())
                || !decision.decision().domainId().equals(interval.domainId())) throw new IllegalArgumentException("Finalization differs from range");
        if (logicalSchema!=null && (!logicalSchema.clusterId().equals(decision.decision().clusterId()) || !logicalSchema.databaseId().equals(decision.decision().databaseId()))) throw new IllegalArgumentException("Finalization database differs from schema");
    }
    private Map<UUID,AbortedParticipant> loadAbortedIndex() {
        var records=new HashMap<UUID,AbortedParticipant>();
        try (var low=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x21).array());
             var high=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x22).array());
             var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high); var iterator=databaseOwner.newIterator(state,options)) {
            iterator.seekToFirst(); byte[] sizing=new byte[0];
            while (iterator.isValid()) {
                if (records.size()>=MAX_PREPARED_PARTICIPANTS || iterator.key(sizing)!=33 || iterator.value(sizing)>AbortedParticipant.MAX_BYTES) throw new IllegalStateException("ABORT recovery bound exceeded");
                var record=AbortedParticipant.decode(iterator.value()); requireAbort(record); var id=record.finalization().transactionId();
                if (!Arrays.equals(iterator.key(),abortedKey(id)) || applied==null || record.position().index()>applied.index() || record.position().term()>applied.term() || record.position().index()==applied.index() && record.position().term()!=applied.term()) throw new IllegalStateException("ABORT fence differs from completed cut");
                records.put(id,record); iterator.next();
            }
            iterator.status(); return Map.copyOf(records);
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot recover ABORT fences",failure); }
    }
    private void requireNegativeIdentity(PreparedParticipant.Preparation preparation) throws RocksDBException {
        requireRejectionIdentity(preparation);
        for (var old:preparedIndex.records()) if (old.preparation().transactionId().equals(preparation.transactionId()) || old.preparation().sharesOperation(preparation)) throw new IllegalArgumentException("Negative prepare identity belongs to an existing prepare");
        for (var commit:committedIndex.values()) if (!commit.finalization().transactionId().equals(preparation.transactionId()) && (commit.ownsOperation(preparation.operation()) || commit.ownsOperation(preparation.business().operationId()))) throw new IllegalArgumentException("Prepared identity belongs to COMMIT receipt");
        for (var abort:abortedIndex.values()) if (!abort.finalization().transactionId().equals(preparation.transactionId()) && (abortOwns(abort,preparation.operation()) || abortOwns(abort,preparation.business().operationId()))) throw new IllegalArgumentException("Negative prepare identity belongs to another ABORT");
        if (db.get(receipts,receiptKey(preparation.operation()))!=null || db.get(receipts,receiptKey(preparation.business().operationId()))!=null) throw new IllegalArgumentException("Negative prepare identity already has a receipt");
    }
    private boolean abortOwns(AbortedParticipant record,OperationId id) {
        return record.ownsOperation(id);
    }
    private void validateAbortedIndex() {
        for (var abort:abortedIndex.values()) {
            if (abort.finalization().participant().prepared()!=null && preparedIndex.records().stream().noneMatch(p -> p.preparation().transactionId().equals(abort.finalization().transactionId()))) throw new IllegalArgumentException("ABORT home vote lacks native evidence");
            for (var preparation:preparedIndex.records()) {
                if (preparation.preparation().transactionId().equals(abort.finalization().transactionId())) { requireTerminalAfterPreparation(abort.finalization(),abort.position(),preparation); }
                else if (abortOwns(abort,preparation.preparation().operation()) || abortOwns(abort,preparation.preparation().business().operationId())) throw new IllegalArgumentException("ABORT/prepare operation identity reused");
            }
            for (var other:abortedIndex.values()) if (other!=abort && (abortOwns(other,abort.finalization().operation()) || abortOwns(other,abort.finalization().decision().operation()))) throw new IllegalArgumentException("ABORT operation identity reused");
        }
        var capacity=preparedCapacityUnderLane(); if (capacity.count()>MAX_PREPARED_PARTICIPANTS || capacity.bytes()>MAX_PREPARED_BYTES) throw new IllegalStateException("Participant recovery capacity exceeded");
    }
    private PreparedCapacity preparedCapacityUnderLane() {
        int count=preparedIndex.records().size(); long bytes=preparedIndex.bytes();
        for (var id:unpreparedTransactions(preparedIndex)) { count++; bytes+=FINALIZATION_RESERVATION; }
        return new PreparedCapacity(applied,count,bytes,preparedIndex.scopeBytes());
    }
    /** Trusted committed ABORT seam. Its adapter must verify fresh home authority and retain proposal ownership through return. */
    private AbortedParticipant applyAbort(ApplyPosition position,ParticipantFinalization finalization) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(finalization); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); if (committedIndex.containsKey(finalization.transactionId())) throw new IllegalArgumentException("COMMIT decision cannot become ABORT"); var candidate=new AbortedParticipant(finalization,position); requireAbort(candidate);
            var saved=abortedIndex.get(finalization.transactionId());
            if(activeTopologyFreeze!=null && saved==null) throw new IllegalStateException("Frozen parent cannot accept fresh ABORT");
            if (saved!=null && (!saved.finalization().operation().equals(finalization.operation()) || !Arrays.equals(saved.finalization().decisionDigest(),finalization.decisionDigest()))) throw new IllegalArgumentException("Participant decision changed");
            if (applied!=null && position.index()<=applied.index()) { if (saved==null) throw new IllegalStateException("Applied ABORT lacks its fence"); return saved; }
            requireNext(position);
            if (saved==null) {
                boolean prepared=false;
                for (var record:preparedIndex.records()) {
                    if (record.preparation().transactionId().equals(finalization.transactionId())) { finalization.requirePrepared(record); prepared=true; }
                    else if (record.preparation().ownsOperation(finalization.operation()) || record.preparation().ownsOperation(finalization.decision().operation())) throw new IllegalArgumentException("ABORT identity belongs to another prepare");
                }
                for (var commit:committedIndex.values()) if (commit.ownsOperation(finalization.operation()) || commit.ownsOperation(finalization.decision().operation())) throw new IllegalArgumentException("ABORT identity belongs to COMMIT receipt");
                for (var abort:abortedIndex.values()) if (abortOwns(abort,finalization.operation()) || abortOwns(abort,finalization.decision().operation())) throw new IllegalArgumentException("ABORT operation identity reused");
                if (db.get(receipts,receiptKey(finalization.operation()))!=null || db.get(receipts,receiptKey(finalization.decision().operation()))!=null) throw new IllegalArgumentException("ABORT identity already has a receipt");
                var rejected=rejectedIndex.get(finalization.transactionId()); if (rejected!=null) rejected.requireFinalization(finalization);
                requireFinalizationRejectionIdentity(finalization);
                if (!prepared && finalization.participant().prepared()!=null) throw new IllegalStateException("Home reports missing native prepare evidence");
                var capacity=preparedCapacityUnderLane();
                if (!prepared && rejected==null) { requireTopologyWritable(); requireSchemaAdmission(); }
                if (!prepared && rejected==null && (capacity.count()>=MAX_PREPARED_PARTICIPANTS || FINALIZATION_RESERVATION>MAX_PREPARED_BYTES-capacity.bytes())) throw new IllegalStateException("ABORT fence capacity exhausted");
            }
            var result=saved==null ? candidate : saved; var next=new HashMap<>(abortedIndex); next.put(finalization.transactionId(),result);
            publishAbort(position,result); abortedIndex=Map.copyOf(next); return result;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private void publishAbort(ApplyPosition position,AbortedParticipant abort) throws RocksDBException {
        var history=prepareScopedWrites(false,List.of(),Map.of());
        try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
            if(cdc!=null) cdc.initialize(batch);
            batch.put(state,abortedKey(abort.finalization().transactionId()),abort.encode()); batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
        }
        publishScoped(position,history,false); applied=position;
    }
    public AbortedParticipant abortedParticipant(UUID transaction) { enterNative(); applyLane.lock(); try { requireActive(); return abortedIndex.get(transaction); } finally { applyLane.unlock(); leaveNative(); } }
    private byte[] preparedKey(UUID transaction) { return ByteBuffer.allocate(33).put(markerKey).put((byte)0x20).putLong(transaction.getMostSignificantBits()).putLong(transaction.getLeastSignificantBits()).array(); }
    private void requirePreparation(PreparedParticipant.Preparation preparation,boolean material) {
        requireHomeRecoveryFree(preparation.operation()); requireHomeRecoveryFree(preparation.business().operationId());
        var business=preparation.business(); validateCommand(business,false,material || logicalSchema!=null);
        if(Arrays.equals(business.schemaDigest(),schemaDigest)) validateSnapshotScope(preparation.scope());
        else {
            if(!isFinalized(preparation.transactionId())) throw new IllegalArgumentException("Unresolved preparation from historical schema");
            validateSnapshotScope(preparation.scope(),schemaHistory.get(schemaIdentity(business.schemaDigest())).columnIds());
        }
        if (!preparation.registration().domainId().equals(interval.domainId())) throw new IllegalArgumentException("Prepared domain differs from range");
        if (logicalSchema!=null && (!logicalSchema.clusterId().equals(preparation.registration().clusterId()) || !logicalSchema.databaseId().equals(preparation.registration().databaseId()))) throw new IllegalArgumentException("Prepared database differs from schema");
    }
    private PreparedIndex addPrepared(PreparedIndex index,PreparedParticipant value) {
        var preparation=value.preparation();
        if (rejectedIndex.containsKey(preparation.transactionId())) throw new IllegalArgumentException("Positive and negative prepare evidence coexist");
        requireRejectionIdentity(preparation);
        for (var old:index.records()) {
            if (old.preparation().transactionId().equals(preparation.transactionId()) || old.preparation().sharesOperation(preparation)) throw new IllegalArgumentException("Prepared transaction or operation identity reused");
            if (!isFinalized(preparation.transactionId()) && !isFinalized(old.preparation().transactionId()) && old.preparation().scope().intersects(preparation.scope())) throw new IllegalStateException("Transaction overlaps durable prepared scope");
        }
        for (var commit:committedIndex.values()) if (!commit.finalization().transactionId().equals(preparation.transactionId()) && (commit.ownsOperation(preparation.operation()) || commit.ownsOperation(preparation.business().operationId()))) throw new IllegalArgumentException("Prepared identity belongs to COMMIT receipt");
        for (var abort:abortedIndex.values()) if (!abort.finalization().transactionId().equals(preparation.transactionId()) && (abortOwns(abort,preparation.operation()) || abortOwns(abort,preparation.business().operationId()))) throw new IllegalArgumentException("Prepared identity belongs to ABORT fence");
        long terminalOnly=unpreparedTransactions(index).stream().filter(id -> !id.equals(preparation.transactionId())).count();
        long bytes=index.bytes()+33L+value.encode().length+FINALIZATION_RESERVATION, scope=index.scopeBytes()+RangeMutationHistory.accountedScopeBytes(preparation.scope());
        if (index.records().size()+terminalOnly>=MAX_PREPARED_PARTICIPANTS || bytes+terminalOnly*FINALIZATION_RESERVATION>MAX_PREPARED_BYTES || scope>MAX_PREPARED_BYTES) throw new IllegalStateException("Durable prepared capacity exhausted");
        var records=new ArrayList<>(index.records()); records.add(value); return new PreparedIndex(List.copyOf(records),bytes,scope);
    }
    private PreparedIndex loadPreparedIndex() {
        var index=PreparedIndex.empty();
        try (var low=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x20).array());
             var high=new org.rocksdb.Slice(ByteBuffer.allocate(17).put(markerKey).put((byte)0x21).array());
             var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high);
             var iterator=databaseOwner.newIterator(state,options)) {
            iterator.seekToFirst(); var sizing=new byte[0];
            while (iterator.isValid()) {
                if (iterator.key(sizing)!=33 || iterator.value(sizing)>16+PreparedParticipant.Preparation.MAX_BYTES) throw new IllegalStateException("Prepared record exceeds native recovery bounds");
                byte[] key=iterator.key(); var value=PreparedParticipant.decode(iterator.value()); requirePreparation(value.preparation(),false);
                if (!Arrays.equals(key,preparedKey(value.preparation().transactionId())) || applied==null || value.position().index()>applied.index() || value.position().term()>applied.term()) throw new IllegalStateException("Prepared record differs from completed cut");
                index=addPrepared(index,value); iterator.next();
            }
            iterator.status(); return index;
        } catch (RocksDBException failure) { throw new IllegalStateException("Cannot recover durable prepared scopes",failure); }
    }
    private void requireUnlocked(TransactionScope scope) {
        for (var record:preparedIndex.records()) if (!isFinalized(record.preparation().transactionId()) && record.preparation().scope().intersects(scope)) throw new IllegalStateException("Access overlaps durable prepared transaction");
    }
    private void requireUnlocked(DistributedCommand command) {
        requireGateOperation(command.operationId());
        for (var rejected:rejectedIndex.values()) if (rejected.ownsOperation(command.operationId())) throw new IllegalArgumentException("Operation identity belongs to rejected preparation");
        for (var commit:committedIndex.values()) if (commit.ownsOperation(command.operationId())) throw new IllegalArgumentException("Operation identity belongs to COMMIT receipt");
        for (var abort:abortedIndex.values()) if (abortOwns(abort,command.operationId())) throw new IllegalArgumentException("Operation identity belongs to durable ABORT fence");
        for (var record:preparedIndex.records()) if (record.preparation().ownsOperation(command.operationId())) throw new IllegalArgumentException("Operation identity is owned by durable preparation");
        var scope=commandScope(command); if (scope!=null) requireUnlocked(scope);
    }
    private TransactionScope commandScope(DistributedCommand command) { return commandAccessScope(command); }

    /** Pure physical access scope, conservatively compacted only for trusted catalogue stores. */
    public TransactionScope commandAccessScope(DistributedCommand command) {
        Objects.requireNonNull(command);
        if (command.acceptance()!=null) return command.acceptance().scope();
        if (catalogueScopeCompaction && (long)command.mutations().size()+command.predicates().size()>TransactionScope.MAX_SPANS) {
            var columns=new LinkedHashSet<UUID>();
            for (var mutation:command.mutations()) columns.add(mutation.columnId());
            for (var predicate:command.predicates()) columns.add(predicate.columnId());
            return new TransactionScope(columns.stream().map(column -> TransactionScope.Span.range(column,null,null)).toList());
        }
        var spans=new ArrayList<TransactionScope.Span>();
        for (var m:command.mutations()) spans.add(m.kind()==Mutation.Kind.DELETE_RANGE ? TransactionScope.Span.range(m.columnId(),m.key(),m.value()) : TransactionScope.Span.point(m.columnId(),m.key()));
        for (var p:command.predicates()) spans.add(TransactionScope.Span.point(p.columnId(),p.key()));
        return spans.isEmpty() ? null : new TransactionScope(spans);
    }
    public record PreparedCapacity(ApplyPosition position,int count,long bytes,long scopeBytes) {}
    public record PreparationEstimate(PreparedCapacity capacity,ParticipantVote existing) {}
    public PreparedCapacity preparedCapacity() {
        enterNative(); applyLane.lock();
        try { requireActive(); return preparedCapacityUnderLane(); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    public record RejectionCapacity(PreparedCapacity capacity,boolean newParticipant) {}
    public record RejectionEstimate(PreparedCapacity capacity,ParticipantVote existing,boolean newParticipant) {}
    private boolean newParticipant(UUID transaction) {
        return !abortedIndex.containsKey(transaction) && !rejectedIndex.containsKey(transaction) && !committedIndex.containsKey(transaction)
                && preparedIndex.records().stream().noneMatch(p -> p.preparation().transactionId().equals(transaction));
    }
    public RejectionCapacity rejectionCapacity(UUID transaction) {
        Objects.requireNonNull(transaction); enterNative(); applyLane.lock();
        try { requireActive(); return new RejectionCapacity(preparedCapacityUnderLane(),newParticipant(transaction)); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    /** Negative votes retain compact metadata, never acquire a scope, and cannot replace positive evidence. */
    public RejectionEstimate estimateRejection(PreparedParticipant.Preparation preparation) throws RocksDBException {
        Objects.requireNonNull(preparation); enterNative(); applyLane.lock();
        try {
            requireActive(); var retirement=readParticipantFence(identity(preparation),null,applied); var capacity=preparedCapacityUnderLane();
            if (retirement!=null) return new RejectionEstimate(capacity,ParticipantVote.retired(retirement),false);
            requirePreparation(preparation,true);
            for (var prepared:preparedIndex.records()) if (prepared.preparation().transactionId().equals(preparation.transactionId())) {
                if (!prepared.preparation().equals(preparation)) throw new IllegalArgumentException("Prepared transaction payload changed");
                return new RejectionEstimate(capacity,ParticipantVote.prepared(prepared.position()),false);
            }
            var saved=rejectedIndex.get(preparation.transactionId());
            if (saved!=null) { saved.requirePreparation(preparation); return new RejectionEstimate(capacity,ParticipantVote.negative(),false); }
            requireNegativeIdentity(preparation); var abort=abortedIndex.get(preparation.transactionId());
            if (abort!=null) abort.finalization().requirePreparation(preparation);
            if (applied==null || applied.index()==Long.MAX_VALUE || preparation.business().acceptance().snapshot().index()>applied.index()
                    || preparation.business().acceptance().snapshot().term()>applied.term()) throw new IllegalStateException("Rejection cannot follow its original cut");
            boolean additional=newParticipant(preparation.transactionId());
            if (additional) { requireTopologyWritable(); requireSchemaAdmission(); }
            if (additional && (capacity.count()>=MAX_PREPARED_PARTICIPANTS || FINALIZATION_RESERVATION>MAX_PREPARED_BYTES-capacity.bytes())) throw new IllegalStateException("Rejected preparation capacity exhausted");
            return new RejectionEstimate(capacity,null,additional);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Native checks and quota observation from one completed cut, before entering the adapter admission monitor. */
    public PreparationEstimate estimatePreparation(PreparedParticipant.Preparation preparation) throws RocksDBException {
        enterNative(); applyLane.lock();
        try {
            requireActive(); var retirement=readParticipantFence(identity(preparation),null,applied);
            if (retirement!=null) return new PreparationEstimate(preparedCapacityUnderLane(),ParticipantVote.retired(retirement));
            requirePreparation(preparation,true);
            var control=preparation.operation(); long minimumControl=minimumSessionEpoch(null); long controlFloor=control.epoch()<minimumControl ? -1 : acknowledgedThrough(control,null);
            var business=preparation.business();
            var controlCommand=business.readCertification() ? DistributedCommand.readCertification(business.groupId(),business.rangeEpoch(),business.schemaDigest(),control,business.acceptance()) : new DistributedCommand(business.groupId(),business.rangeEpoch(),business.schemaDigest(),control,business.mutations(),business.predicates());
            if (control.epoch()<minimumControl || control.sequence()<=controlFloor || sessionDecision(controlCommand,controlFloor,sessionAuthority())!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Prepare control session is fenced; query its durable state");
            for (var record:preparedIndex.records()) if (record.preparation().transactionId().equals(preparation.transactionId())) {
                if (!record.preparation().equals(preparation)) throw new IllegalArgumentException("Prepared transaction payload changed");
                return new PreparationEstimate(preparedCapacity(),ParticipantVote.prepared(record.position()));
            }
            var rejected=rejectedIndex.get(preparation.transactionId());
            if (rejected!=null) { rejected.requirePreparation(preparation); return new PreparationEstimate(preparedCapacityUnderLane(),ParticipantVote.negative()); }
            var abort=abortedIndex.get(preparation.transactionId());
            if (abort!=null) { requireNegativeIdentity(preparation); abort.finalization().requirePreparation(preparation); return new PreparationEstimate(preparedCapacityUnderLane(),null); }
            requireTopologyWritable();
            var id=business.operationId(); long minimum=minimumSessionEpoch(null); long floor=id.epoch()<minimum ? -1 : acknowledgedThrough(id,null);
            if (id.epoch()<minimum || id.sequence()<=floor || db.get(receipts,receiptKey(id))!=null || db.get(receipts,receiptKey(preparation.operation()))!=null
                    || sessionDecision(business,floor,sessionAuthority())!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Prepared identity is no longer eligible");
            if (applied==null || applied.index()==Long.MAX_VALUE || business.acceptance().snapshot().index()==Long.MAX_VALUE) throw new IllegalStateException("Participant completed prefix unavailable/exhausted");
            if (gated(preparation.scope())) {
                requireNegativeIdentity(preparation); var capacity=preparedCapacityUnderLane();
                if (capacity.count()>=MAX_PREPARED_PARTICIPANTS || FINALIZATION_RESERVATION>MAX_PREPARED_BYTES-capacity.bytes()) throw new IllegalStateException("Rejected preparation capacity exhausted");
            } else addPrepared(preparedIndex,new PreparedParticipant(preparation,new ApplyPosition(business.acceptance().snapshot().term(),Math.max(applied.index()+1,business.acceptance().snapshot().index()+1))));
            return new PreparationEstimate(preparedCapacity(),null);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Exact native recovery lookup is permitted while fenced; it grants no user admission authority. */
    public PreparedParticipant recoveredParticipant(PreparedParticipant.Preparation expected) throws RocksDBException {
        enterNative(); applyLane.lock();
        try {
            byte[] bytes=db.get(state,preparedKey(expected.transactionId())); if (bytes==null) return null;
            var record=PreparedParticipant.decode(bytes); requirePreparation(record.preparation(),false);
            if (!record.preparation().equals(expected) || applied==null || record.position().index()>applied.index() || record.position().term()>applied.term() || record.position().index()==applied.index() && record.position().term()!=applied.term()) throw new IllegalArgumentException("Recovered prepared receipt differs");
            return record;
        } finally { applyLane.unlock(); leaveNative(); }
    }
    public ParticipantVote recoveredPreparationVote(PreparedParticipant.Preparation expected) throws RocksDBException {
        enterNative(); applyLane.lock();
        try {
            var retirement=readParticipantFence(identity(expected),null,applied); if (retirement!=null) return ParticipantVote.retired(retirement);
            var prepared=recoveredParticipant(expected); if (prepared!=null) return ParticipantVote.prepared(prepared.position());
            byte[] bytes=db.get(state,rejectedKey(expected.transactionId())); if (bytes==null) return null;
            var rejected=RejectedParticipant.decode(bytes); requireRejected(rejected); rejected.requirePreparation(expected);
            if (applied==null || rejected.position().index()>applied.index() || rejected.position().term()>applied.term()) throw new IllegalArgumentException("Recovered negative receipt differs from completed cut"); return ParticipantVote.negative();
        } finally { applyLane.unlock(); leaveNative(); }
    }
    /** Only committed accepted preparations may enter. The adapter must retain its complete proposal reservation through this return. */
    public ParticipantVote applyPrepared(ApplyPosition position,PreparedParticipant.Preparation preparation) throws RocksDBException {
        Objects.requireNonNull(position); Objects.requireNonNull(preparation); enterNative(); applyLane.lock();
        try {
            requireActive(); ensureReceiptUsage(); var identity=identity(preparation);
            if (readParticipantFence(identity,null,applied)!=null) { advanceCompletedMarker(position); return ParticipantVote.retired(readParticipantFence(identity,null,applied)); }
            requirePreparation(preparation,true);
            PreparedParticipant saved=null;
            for (var old:preparedIndex.records()) if (old.preparation().transactionId().equals(preparation.transactionId())) {
                if (!Arrays.equals(old.preparation().digest(),preparation.digest())) throw new IllegalArgumentException("Prepared transaction payload changed"); saved=old;
            }
            if (applied!=null && position.index()<=applied.index()) {
                if (saved==null) { var rejected=rejectedIndex.get(preparation.transactionId()); if (rejected!=null) { rejected.requirePreparation(preparation); return ParticipantVote.negative(); } throw new IllegalStateException("Completed prepare has no durable receipt"); } return ParticipantVote.prepared(saved.position());
            }
            requireNext(position);
            var abort=abortedIndex.get(preparation.transactionId());
            if (saved==null && abort==null && !rejectedIndex.containsKey(preparation.transactionId())) { requireTopologyWritable(); requireSchemaAdmission(); }
            if (saved==null && (abort!=null || rejectedIndex.containsKey(preparation.transactionId()) || gated(preparation.scope()))) {
                return applyRejectedUnderLane(position,preparation);
            }
            if (saved==null) {
                requireTopologyWritable(); requireSchemaAdmission();
                var business=preparation.business(); var id=business.operationId(); long minimum=minimumSessionEpoch(null); long floor=id.epoch()<minimum ? -1 : acknowledgedThrough(id,null);
                if (id.epoch()<minimum || id.sequence()<=floor || db.get(receipts,receiptKey(id))!=null || db.get(receipts,receiptKey(preparation.operation()))!=null
                        || sessionDecision(business,floor,sessionAuthority())!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Prepared business/control identity is no longer eligible");
            }
            var value=saved==null ? new PreparedParticipant(preparation,position) : saved;
            var next=saved==null ? addPrepared(preparedIndex,value) : preparedIndex;
            if (saved==null && !Arrays.equals(evaluate(preparation.business()).result().encode(),preparation.business().acceptance().result())) throw new IllegalStateException("Prepared result disagrees with ordered native state");
            // Canonical acceptance was certified before append; replicas do not consult volatile history or expose effects.
            var history=prepareScopedWrites(false,List.of(),Map.of());
            try (var batch=new WriteBatch(); var options=new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) cdc.initialize(batch);
                if (saved==null) batch.put(state,preparedKey(preparation.transactionId()),value.encode());
                batch.put(state,markerKey,encodePosition(position)); batch.put(state,usageKey,encodeTotalUsage(readUsage(usageKey),position)); db.write(options,batch);
            }
            preparedIndex=next; publishScoped(position,history,false); applied=position; return ParticipantVote.prepared(value.position());
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    /** Barrier/query ownership is supplied by the adapter; this record survives prepare-receipt/session GC. */
    public PreparedParticipant preparedParticipant(UUID transaction) {
        enterNative(); applyLane.lock();
        try { requireActive(); return preparedIndex.records().stream().filter(p -> p.preparation().transactionId().equals(transaction)).findFirst().orElse(null); }
        finally { applyLane.unlock(); leaveNative(); }
    }
    public int preparedParticipantCount() { return preparedIndex.records().size(); }
    public long preparedParticipantBytes() { return preparedIndex.bytes(); }
    /** Fresh user reads must reject unresolved prepared scopes; administrative capture remains available for export/recovery. */
    public ScopedSnapshot captureReadable(TransactionScope scope) throws RocksDBException {
        Objects.requireNonNull(scope); enterNative(); applyLane.lock();
        try { requireServing(); requireActive(); validateSnapshotScope(scope); requireUnlocked(scope); return new ScopedSnapshot(scope,captureUnderAdmission()); }
        finally { applyLane.unlock(); leaveNative(); }
    }

    public record PhysicalPoint(UUID column,byte[] key) {
        public PhysicalPoint { Objects.requireNonNull(column); key=Objects.requireNonNull(key).clone(); }
        @Override public byte[] key() { return key.clone(); }
    }
    public record CurrentPoints(ApplyPosition position,List<byte[]> values) {
        public CurrentPoints { values=copyPointValues(values); }
        @Override public List<byte[]> values() { return copyPointValues(values); }
        private static List<byte[]> copyPointValues(List<byte[]> values) {
            var copy=new ArrayList<byte[]>(values.size()); for (var value:values) copy.add(value==null ? null : value.clone());
            return java.util.Collections.unmodifiableList(copy);
        }
    }
    public record MetadataEntry(byte[] key,byte[] value) {
        public MetadataEntry { key=Objects.requireNonNull(key).clone();value=Objects.requireNonNull(value).clone(); }
        @Override public byte[] key() { return key.clone(); }
        @Override public byte[] value() { return value.clone(); }
    }
    public record CurrentMetadataPrefix(ApplyPosition position,List<MetadataEntry> entries) {
        public CurrentMetadataPrefix { entries=List.copyOf(entries); }
    }
    /** Bounded server-owned topology status inventory, exclusively for active CATALOGUE metadata. */
    public CurrentMetadataPrefix readCurrentMetadataPrefix(UUID column,byte[] prefix,int exactKeyBytes,int maxRows,int maxBytes) throws RocksDBException {
        Objects.requireNonNull(column);byte[] low=Objects.requireNonNull(prefix).clone();
        if(low.length!=1 || low[0]!=(byte)0x82 || exactKeyBytes!=17 || maxRows<1 || maxRows>128 || maxBytes<1 || maxBytes>MAX_SCHEMA_AUTHORITY_BYTES)throw new IllegalArgumentException("Invalid topology metadata prefix/budget");
        enterNative();applyLane.lock();
        try {
            requireActive();if(!catalogueScopeCompaction || !columns.containsKey(column))throw new IllegalStateException("Topology prefix inventory requires active CATALOGUE column");ensureReceiptUsage();
            byte[] marker=db.get(state,markerKey);var cut=marker==null ? null : decodePosition(marker);if(cut==null || !cut.equals(applied))throw new IllegalStateException("Catalogue prefix marker differs from completed apply");
            var entries=new ArrayList<MetadataEntry>();long bytes=0;byte[] sizing=new byte[0],high={(byte)0x83};
            try(var lower=new org.rocksdb.Slice(low);var upper=new org.rocksdb.Slice(high);var options=new ReadOptions().setFillCache(false).setIterateLowerBound(lower).setIterateUpperBound(upper);var iterator=databaseOwner.newIterator(columns.get(column),options)) {
                iterator.seekToFirst();
                while(iterator.isValid()) {
                    int keyBytes=iterator.key(sizing);
                    if(keyBytes==16) { iterator.next();continue; } // Shared-column directory UUIDs are not status rows.
                    if(keyBytes!=exactKeyBytes)throw new IllegalStateException("Topology metadata status key shape changed");
                    int valueBytes=iterator.value(sizing);
                    if(entries.size()>=maxRows || keyBytes+(long)valueBytes>maxBytes-bytes)throw new IllegalStateException("Topology metadata prefix exceeds inventory budget");
                    byte[] key=iterator.key(),value=iterator.value();if(key.length!=keyBytes || value.length!=valueBytes || key[0]!=(byte)0x82)throw new IllegalStateException("Topology metadata iterator cut changed");entries.add(new MetadataEntry(key,value));bytes+=keyBytes+(long)valueBytes;iterator.next();
                }
                iterator.status();
            }
            marker=db.get(state,markerKey);if(marker==null || !cut.equals(decodePosition(marker)) || !cut.equals(applied))throw new IllegalStateException("Catalogue prefix completed cut changed");return new CurrentMetadataPrefix(cut,entries);
        } finally { applyLane.unlock();leaveNative(); }
    }
    /** Small atomic metadata/user point read at one completed cut, without acquiring a native snapshot. */
    public CurrentPoints readCurrentPoints(List<PhysicalPoint> points) throws RocksDBException { return readCurrentPoints(points,MAX_RESULT_BYTES); }
    /** Explicit bounded schema-authority control read; ordinary foreground reads retain their smaller reply budget. */
    public CurrentPoints readSchemaAuthorityPoints(List<PhysicalPoint> points) throws RocksDBException { return readCurrentPoints(points,MAX_SCHEMA_AUTHORITY_BYTES); }
    public CurrentPoints readSchemaAuthorityPoints(List<PhysicalPoint> points,int[] maximumValueBytes) throws RocksDBException { return readCurrentPoints(points,MAX_SCHEMA_AUTHORITY_BYTES,Objects.requireNonNull(maximumValueBytes).clone()); }
    private CurrentPoints readCurrentPoints(List<PhysicalPoint> points,int maximumBytes) throws RocksDBException { return readCurrentPoints(points,maximumBytes,null); }
    private CurrentPoints readCurrentPoints(List<PhysicalPoint> points,int maximumBytes,int[] maximumValueBytes) throws RocksDBException {
        points=List.copyOf(points);
        if(maximumValueBytes!=null && (maximumValueBytes.length!=points.size() || Arrays.stream(maximumValueBytes).anyMatch(limit -> limit<=0))) throw new IllegalArgumentException("Atomic point value bounds differ from request"); if (points.isEmpty() || points.size()>4) throw new IllegalArgumentException("Atomic point read requires 1..4 points");
        long keyBytes=0; var spans=new ArrayList<TransactionScope.Span>(points.size());
        for (var point:points) { keyBytes+=point.key.length; if (keyBytes>TransactionScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("Atomic point keys exceed budget"); spans.add(TransactionScope.Span.point(point.column,point.key)); }
        var scope=new TransactionScope(spans); enterNative(); applyLane.lock();
        try {
            requireServing(); requireActive(); requireSchemaAdmission(); validateSnapshotScope(scope); requireUnlocked(scope); ensureReceiptUsage();
            byte[] marker=db.get(state,markerKey); ApplyPosition cut=marker==null ? null : decodePosition(marker);
            if (!Objects.equals(cut,applied)) throw new IllegalStateException("Atomic point marker differs from completed apply");
            var values=new ArrayList<byte[]>(points.size()); long valueBytes=0;
            for (int i=0;i<points.size();i++) {
                var point=points.get(i);
                // Read size before allocating an arbitrary native value into Java memory.
                var cf=columns.get(point.column); int length=db.get(cf,point.key,new byte[0]);
                if (length!=RocksDB.NOT_FOUND) { if(maximumValueBytes!=null && length>maximumValueBytes[i]) throw new IllegalStateException("Atomic point value exceeds its control bound"); valueBytes+=length; if (valueBytes>maximumBytes) throw new IllegalStateException("Atomic point values exceed budget"); }
                values.add(length==RocksDB.NOT_FOUND ? null : db.get(cf,point.key));
            }
            return new CurrentPoints(cut,values);
        } finally { applyLane.unlock(); leaveNative(); }
    }

    /** Fresh atomic point read after completed apply; no retained snapshot or retention quota is acquired. */
    public byte[] readCurrentPoint(UUID column, byte[] key) throws RocksDBException {
        byte[] physicalKey = Objects.requireNonNull(key).clone();
        var scope = new TransactionScope(List.of(TransactionScope.Span.point(column, physicalKey)));
        enterNative(); applyLane.lock();
        try {
            requireServing(); requireActive(); requireSchemaAdmission(); validateSnapshotScope(scope); requireUnlocked(scope);
            ensureReceiptUsage();
            byte[] marker = db.get(state, markerKey);
            ApplyPosition cut = marker == null ? null : decodePosition(marker);
            if (!Objects.equals(cut, applied)) throw new IllegalStateException("Current point marker differs from completed apply");
            return db.get(columns.get(column), physicalKey);
        } finally { applyLane.unlock(); leaveNative(); }
    }

    /** Evaluates autocommit return values in committed order, preserving native merge operands. */
    public byte[] apply(ApplyPosition position, DistributedCommand command) throws RocksDBException {
        return applyInternal(position, command, null);
    }

    /**
     * Installs user effects, the original result and the completed marker in one
     * WAL-enabled cross-CF batch. The result is evaluated by ordered leader
     * planning before consensus, never recomputed during replay or retry.
     */
    public byte[] apply(ApplyPosition position, DistributedCommand command, byte[] result)
            throws RocksDBException {
        return applyInternal(position, command, Objects.requireNonNull(result));
    }

    private byte[] applyInternal(ApplyPosition position, DistributedCommand command, byte[] result)
            throws RocksDBException {
        enterNative();
        try { return applyUnderAdmission(position, command, result); }
        finally { try { drainScopedReleases(); } finally { leaveNative(); } }
    }

    private byte[] applyUnderAdmission(ApplyPosition position, DistributedCommand command, byte[] result) throws RocksDBException { return applyUnderAdmission(position,command,result,false); }
    /** Trusted ordered committed follower replay; registration never grants ordinary USER admission. */
    public byte[] applyTopologyLearnerTail(ApplyPosition position,DistributedCommand command) throws RocksDBException {
        enterNative();try { return applyUnderAdmission(position,command,null,true); } finally { try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private byte[] learnerImageReceipt() {
        return ByteBuffer.allocate(48).putLong(topologyLearner.imageCut().term()).putLong(topologyLearner.imageCut().index()).put(topologyLearner.imageHash()).array();
    }
    /** Forced import evidence at the original registered cut, rather than inference from later NOOPs. */
    public boolean topologyLearnerImageInstalled() throws RocksDBException {
        return readSchemaMetadata(this::topologyLearnerImageInstalledUnderLane);
    }
    public boolean topologyLearnerImageInstalled(ApplyPosition cut,byte[] hash) throws RocksDBException {
        Objects.requireNonNull(cut);if(hash==null || hash.length!=32)throw new IllegalArgumentException("Invalid learner image digest");return readSchemaMetadata(()->{
            if(!topologyLearnerImageInstalledUnderLane())return false;
            if(cut.equals(topologyLearner.imageCut()) && Arrays.equals(hash,topologyLearner.imageHash()))return true;
            if(learnerImagePermission==null || !cut.equals(learnerImagePermission.imageCut()) || !Arrays.equals(hash,learnerImagePermission.imageHash()))return false;
            byte[] key=topologyOwnershipKey(0x6b);int size=db.get(state,key,new byte[0]);if(size==RocksDB.NOT_FOUND)return false;if(size!=48)throw new IllegalArgumentException("Invalid later image receipt");
            byte[] expected=ByteBuffer.allocate(48).putLong(cut.term()).putLong(cut.index()).put(hash).array();if(!Arrays.equals(db.get(state,key),expected))throw new IllegalArgumentException("Later image receipt changed permission");return applied.index()>=cut.index() && applied.term()>=cut.term() && (applied.index()!=cut.index() || applied.term()==cut.term());
        });
    }
    private boolean topologyLearnerImageInstalledUnderLane() throws RocksDBException {
        if(topologyLearner==null || pendingInstallation!=null || applied==null)return false;
        byte[] key=topologyOwnershipKey(0x69);int size=db.get(state,key,new byte[0]);if(size==RocksDB.NOT_FOUND)return false;if(size!=48)throw new IllegalArgumentException("Invalid learner image receipt length");byte[] receipt=db.get(state,key);
        if(!Arrays.equals(receipt,learnerImageReceipt()))throw new IllegalArgumentException("Learner image receipt differs from original registration");
        var cut=topologyLearner.imageCut();return applied.index()>=cut.index() && applied.term()>=cut.term() && (applied.index()!=cut.index() || applied.term()==cut.term());
    }
    private void requireCurrentSchemaTail()throws RocksDBException {if(currentRangeRecovery==null || pendingCurrentCompletion!=null)throw new IllegalStateException("CURRENT own-tail control lacks settled registration");requireTopologyLearnerImage();}
    public NativeCurrentRangeRecovery currentRangeRecoveryRegistration(){return currentRangeRecovery;}
    public boolean currentRangeRecoveryReady(){return currentRangeRecovery!=null && currentRangeRecoveryReady;}
    private byte[] currentImageReceipt(){return ByteBuffer.allocate(48).putLong(currentRangeRecovery.imageCut().term()).putLong(currentRangeRecovery.imageCut().index()).put(currentRangeRecovery.imageHash()).array();}
    private boolean currentImageInstalledUnderLane()throws RocksDBException {
        if(currentRangeRecovery==null || pendingInstallation!=null || applied==null)return false;byte[] receipt=db.get(state,topologyOwnershipKey(0x74));if(receipt==null)return false;if(!Arrays.equals(receipt,currentImageReceipt()))throw new IllegalArgumentException("CURRENT receipt changed registered image");return NativeMetadataRecoveryRegistration.covers(applied,currentRangeRecovery.imageCut());
    }
    public boolean currentRangeRecoveryImageInstalled()throws RocksDBException{return readSchemaMetadata(this::currentImageInstalledUnderLane);}
    /** Committed own-group replay after a genuine current image; no intermediate historical USER replay is needed. */
    public byte[] applyCurrentRangeRecoveryTail(ApplyPosition position,DistributedCommand command)throws RocksDBException {
        if(currentRangeRecovery==null)throw new IllegalStateException("No CURRENT recovery registration");return applyTopologyLearnerTail(position,command);
    }
    /** Caller supplies fresh current placement/quorum proof; this method forces native image/tail/configuration evidence first. */
    public void completeCurrentRangeRecovery(NativeCurrentRangeRecovery expected,ApplyPosition requiredCut,Configuration requiredConfiguration)throws RocksDBException {
        Objects.requireNonNull(expected);Objects.requireNonNull(requiredCut);Objects.requireNonNull(requiredConfiguration);enterNative();applyLane.lock();try {
            if(!expected.equals(currentRangeRecovery))throw new IllegalArgumentException("CURRENT completion changed original registration");
            byte[] cfg=requiredConfiguration.data();byte[] receipt=ByteBuffer.allocate(84+cfg.length).put(expected.digest()).putLong(requiredCut.term()).putLong(requiredCut.index()).putLong(requiredConfiguration.term()).putLong(requiredConfiguration.index()).putInt(cfg.length).put(cfg).putLong(applied==null?0:applied.term()).putLong(applied==null?-1:applied.index()).array();
            if(pendingCurrentCompletion!=null && !Arrays.equals(pendingCurrentCompletion,receipt))throw new IllegalArgumentException("Unknown CURRENT completion force requires original retry");
            if(currentRangeRecoveryReady){byte[] original=db.get(state,topologyOwnershipKey(0x73));if(original==null || !Arrays.equals(receipt,0,receipt.length-16,original,0,original.length-16))throw new IllegalArgumentException("CURRENT completion changed original evidence");return;}
            requireTopologyLearnerImage();var actual=configurationUnderAdmission();
            if(requiredCut.term()<1 || !NativeMetadataRecoveryRegistration.covers(applied,requiredCut) || actual==null || actual.term()!=requiredConfiguration.term() || actual.index()!=requiredConfiguration.index() || !Arrays.equals(actual.data(),cfg) || actual.index()>applied.index() || actual.term()>applied.term() || logicalSchema==null || !Arrays.equals(schemaDigest,logicalSchema.digest()) || !Arrays.equals(schemaDigest,expected.schema().digest()) && !schemaHistory.containsKey(schemaIdentity(expected.schema().digest())) || activeSchemaFence!=null || activeSchemaInstallation!=null && activeSchemaActivation==null)throw new IllegalStateException("CURRENT completion lacks actual image, own tail, configuration or schema");
            pendingCurrentCompletion=receipt;currentRecoveryCompletionForceObserver.boundary(false);try(var batch=new WriteBatch();var options=new WriteOptions().setDisableWAL(false).setSync(true)){batch.put(state,topologyOwnershipKey(0x73),receipt);db.write(options,batch);}currentRecoveryCompletionForceObserver.boundary(true);currentRangeRecoveryReady=true;pendingCurrentCompletion=null;ownership.activate(owner);
        }finally{applyLane.unlock();leaveNative();}
    }
    private void requireTopologyLearnerImage() throws RocksDBException {
        if(currentRangeRecovery!=null){if(!currentImageInstalledUnderLane() || ownership.get(owner).state()!=RangeOwnership.State.FENCED && ownership.get(owner).state()!=RangeOwnership.State.ACTIVE)throw new IllegalStateException("CURRENT tail requires its actual completed fenced image");validateTopologyActivationRecovery();ensureReceiptUsage();var cfg=configurationUnderAdmission();if(cfg==null || cfg.index()>applied.index() || cfg.term()>applied.term() || cfg.index()==applied.index() && cfg.term()!=applied.term())throw new IllegalStateException("CURRENT configuration exceeds own completed prefix");return;}

        if(learnerSchemaPreparation!=null || !topologyLearnerImageInstalledUnderLane() || topologyLearner.origin()!=null && (topologyActivation==null || topologyBirth==null) || applied==null || ownership.get(owner).state()!=RangeOwnership.State.FENCED && ownership.get(owner).state()!=RangeOwnership.State.ACTIVE)throw new IllegalStateException("Learner has no completed image at its fenced receiver");
        validateTopologyActivationRecovery();ensureReceiptUsage();var cfg=configurationUnderAdmission();if(cfg==null || cfg.index()>applied.index() || cfg.term()>applied.term() || cfg.index()==applied.index() && cfg.term()!=applied.term())throw new IllegalStateException("Learner configuration exceeds genuine completed prefix");
    }
    public record TopologyLearnerReadiness(NativeTopologyLearnerRegistration registration,ApplyPosition completedCut,Configuration configuration,boolean ready) {}
    /** Native cut/config evidence only; the adapter must qualify actual division/member/tail authority. */
    public TopologyLearnerReadiness topologyLearnerReadiness(ApplyPosition requiredCut) throws RocksDBException {return topologyLearnerReadiness(requiredCut,Objects.requireNonNull(topologyLearner,"No learner registration").proposedConfiguration());}
    public TopologyLearnerReadiness topologyLearnerReadiness(ApplyPosition requiredCut,byte[] expectedConfiguration) throws RocksDBException {
        Objects.requireNonNull(expectedConfiguration);if(expectedConfiguration.length<1 || expectedConfiguration.length>65536)throw new IllegalArgumentException("Invalid expected learner configuration");Objects.requireNonNull(requiredCut);if(requiredCut.term()<1)throw new IllegalArgumentException("Learner readiness requires positive committed tail cut");enterNative();applyLane.lock();try {
            if(topologyLearner==null)throw new IllegalStateException("Range has no learner registration");var cfg=configurationUnderAdmission();boolean ready=false;
            if(topologyLearnerImageInstalledUnderLane()) { requireTopologyLearnerImage();ready=applied.index()>=requiredCut.index() && applied.term()>=requiredCut.term() && (applied.index()!=requiredCut.index() || applied.term()==requiredCut.term()) && cfg.term()>0 && cfg.index()>=topologyLearner.imageCut().index() && cfg.index()<=applied.index() && cfg.term()<=applied.term() && (cfg.index()!=applied.index() || cfg.term()==applied.term()) && Arrays.equals(cfg.data(),expectedConfiguration); }
            return new TopologyLearnerReadiness(topologyLearner,applied,cfg,ready);
        } finally { applyLane.unlock();leaveNative(); }
    }
    private byte[] applyUnderAdmission(ApplyPosition position, DistributedCommand command, byte[] result,boolean learnerTail)
            throws RocksDBException {
        Objects.requireNonNull(position);
        validateCommand(command,false);
        if (result!=null && (command.sessionControl()!=null || command.hasBucketData() || command.acceptance()!=null || command.transactionRejection())) throw new IllegalArgumentException("Controls, buckets and transaction plans require ordered native evaluation");
        if (result != null && !command.predicates().isEmpty()) {
            throw new IllegalArgumentException("Precomputed results require a fully evaluated plan without live predicates");
        }
        if (result != null && result.length > MAX_RESULT_BYTES) throw new IllegalArgumentException("Result exceeds receipt budget");
        byte[] stableResult = result == null ? null : result.clone();
        byte[] digest = command.receiptDigest();
        byte[] receiptKey = receiptKey(command.operationId());
        applyLane.lock();
        try {
            if(pendingCurrentCompletion!=null)throw new IllegalStateException("Unknown CURRENT completion force requires original retry");
            if(learnerTail)requireTopologyLearnerImage();else if(metadataTopologyControl(command))requireTopologyMetadataOwner(false);else requireActive();
            ensureReceiptUsage(); requireSourceCreditOriginal(command);requireHomeRecoveryFree(command.operationId());
            if(!metadataSchemaControl(command) && (activeTopologyFreeze!=null && !topologyAccountingControl(command) || activeSchemaFence!=null || !Arrays.equals(schemaDigest,command.schemaDigest()))) {
                byte[] retained=db.get(receipts,receiptKey);
                if(retained==null) {
                    if(activeTopologyFreeze!=null) throw new IllegalStateException("Topology FREEZE blocks fresh DATA effects");
                    if(canonicalStaleSchemaControl(command)) return applyStaleSchemaControl(position,command);
                    if(!Arrays.equals(schemaDigest,command.schemaDigest())) throw new IllegalArgumentException("Historical schema has no saved DATA outcome");
                    throw new IllegalStateException("Schema change blocks new DATA operations");
                }
                byte[] original=decodeReceipt(retained,digest);
                if(activeSchemaFence!=null && (position.index()==activeSchemaFence.position().index() || position.index()<activeSchemaFence.position().index() && position.term()>activeSchemaFence.position().term())) throw new IllegalArgumentException("Operation callback conflicts with schema fence position");
                var id=command.operationId();
                if(id.epoch()<minimumSessionEpoch(null) || id.sequence()<=acknowledgedThrough(id,null)) original=expiredResult();
                advanceCompletedMarker(position); return original;
            }
            long currentEpochFloor = minimumSessionEpoch(null);
            long currentFloor = command.operationId().epoch() < currentEpochFloor ? -1 : acknowledgedThrough(command.operationId(), null);
            if (command.operationId().epoch() < currentEpochFloor || command.operationId().sequence() <= currentFloor) {
                if (applied == null || position.index() > applied.index()) {
                    requireNext(position);
                    var writes=prepareScopedWrites(false,List.of(),Map.of());
                    try (var batch = new WriteBatch(); var options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                        if(cdc!=null) cdc.initialize(batch);
                        batch.put(state, markerKey, encodePosition(position));
                        batch.put(state, usageKey, encodeTotalUsage(readUsage(usageKey), position));
                        db.write(options, batch);
                    }
                    publishScoped(position,writes,false); applied = position;
                }
                return expiredResult();
            }
            byte[] saved = db.get(receipts, receiptKey);
            byte[] original = saved == null ? null : decodeReceipt(saved, digest);
            if (applied != null && position.index() <= applied.index()) {
                if (saved == null) throw new IllegalStateException("Applied command has no retained receipt");
                return original;
            }
            requireNext(position);
            if (saved==null) requireUnlocked(command);
            if (saved==null && command.acceptance()!=null && (command.acceptance().snapshot().index()>=position.index()
                    || command.acceptance().snapshot().term()!=position.term())) throw new IllegalArgumentException("Accepted transaction does not follow its audited leadership cut");
            var authority=saved==null ? sessionAuthority() : null;
            var sessionStatus = saved==null ? (expiredHomeRegistration(command) ? CommandResult.Status.SESSION_EXPIRED : sessionDecision(command,currentFloor,authority)) : CommandResult.Status.SUCCESS;
            boolean terminalExpiry = sessionStatus==CommandResult.Status.SESSION_EXPIRED && expiredBoundOutcome(command,currentFloor,authority);
            boolean acceptMaintenance = sessionStatus==CommandResult.Status.SUCCESS || terminalExpiry;
            long effectiveMinimum = acceptMaintenance ? command.minimumSessionEpoch() : -1;
            long effectiveAcknowledgement = acceptMaintenance ? command.acknowledgedThrough() : -1;
            boolean installUserEffects = true;
            var scope=saved==null ? commandScope(command) : null;
            if (saved==null && scope!=null && gated(scope)) {
                stableResult=new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode(); installUserEffects=false;
            }
            if (saved==null && sessionStatus!=CommandResult.Status.SUCCESS) {
                stableResult=new CommandResult(sessionStatus,List.of()).encode(); installUserEffects=false;
            }
            var sessionMetadata=saved==null && acceptMaintenance ? sessionMetadataChange(command,effectiveMinimum,effectiveAcknowledgement) : null;
            if(sessionMetadata!=null && sessionMetadata.count()+sourceSessionMetadataPromises(Math.max(currentEpochFloor,effectiveMinimum),null,null)>sessionAckLimit){stableResult=new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode();installUserEffects=false;acceptMaintenance=false;effectiveMinimum=-1;effectiveAcknowledgement=-1;sessionMetadata=null;}
            List<Mutation> effects=command.mutations(); Map<BucketKey,DistributedBucket> bucketEffects=Map.of();
            if (saved==null && stableResult==null && command.transactionRejection()) {
                stableResult=new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode(); installUserEffects=false;
            }
            if (saved == null && stableResult == null) {
                var evaluated = evaluate(command);
                stableResult = evaluated.result().encode(); effects=evaluated.effects(); bucketEffects=evaluated.buckets();
                if (command.acceptance()!=null && !Arrays.equals(stableResult,command.acceptance().result())) {
                    throw new IllegalStateException("Accepted transaction result disagrees with ordered native effects");
                }
                installUserEffects = evaluated.result().status() == CommandResult.Status.SUCCESS;
            }
            var catalogueChange=saved==null && installUserEffects ? catalogueHistoryChange(command,effects) : null;
            if(catalogueChange!=null && catalogueChange.update()==null){stableResult=new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode();installUserEffects=false;catalogueChange=null;}
            var homeStatusChange=saved==null && installUserEffects ? homeStatusDelta(effects) : null;
            if(!homeStatusFits(homeStatusChange)){stableResult=new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode();installUserEffects=false;homeStatusChange=null;}
            var creditChange=homeCreditChange(command,position,saved==null && installUserEffects);
            if (creditChange!=null) checkHomeCapacity(creditChange.count(),creditChange.bytes());
            var writes=prepareScopedWrites(saved==null && installUserEffects,effects,bucketEffects);
            Runnable publishCdc=()->{},publishSourceGc=()->{};
            try (WriteBatch batch = new WriteBatch();
                    WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) publishCdc=cdc.initialize(batch);
                if (saved == null) {
                    if(installUserEffects)writeCatalogueHistory(batch,catalogueChange);
                    if (installUserEffects) for (var mutation : effects) {
                        var cf = columns.get(mutation.columnId());
                        if (cf == null) throw new IllegalArgumentException("Unknown logical column");
                        switch (mutation.kind()) {
                            case PUT -> batch.put(cf, mutation.key(), mutation.value());
                            case DELETE -> batch.delete(cf, mutation.key());
                            case MERGE -> batch.merge(cf, mutation.key(), mutation.value());
                            case DELETE_RANGE -> batch.deleteRange(cf, mutation.key(), mutation.value());
                            case BUCKET_PUT, BUCKET_DELETE, BUCKET_MERGE -> throw new IllegalStateException("Bucket effect was not lowered into a physical batch");
                        }
                    }
                    if (installUserEffects) for (var entry:bucketEffects.entrySet()) batch.put(columns.get(entry.getKey().column),entry.getKey().key,entry.getValue().encode());
                    if(cdc!=null && installUserEffects && !command.mutations().isEmpty()) publishCdc=cdc.append(batch,
                            new it.cavallium.rockserver.core.common.distributed.DistributedCdcRecord(cdc.lineage(),position,command,null));
                    if (acceptMaintenance && command.sessionControl()!=null) {
                        var control=command.sessionControl();
                        var clock=effectiveAuthority(control,authority).logicalTimeMillis();
                        installSessionControl(batch,control);
                        if(cdc!=null) publishCdc=cdc.advanceClock(batch,clock);
                    }
                    if (effectiveMinimum > currentEpochFloor) {
                        batch.put(state, epochFloorKey(), ByteBuffer.allocate(8).putLong(effectiveMinimum).array());
                        batch.deleteRange(state, bindingPrefix(), bindingKey(effectiveMinimum));
                        batch.deleteRange(state,sessionFloorPrefix(),sessionFloorEpoch(effectiveMinimum));
                        batch.deleteRange(receipts,markerKey,receiptEpoch(effectiveMinimum));
                    }
                    if (effectiveAcknowledgement > currentFloor) {
                        long floor = effectiveAcknowledgement;
                        batch.put(state, sessionFloorKey(command.operationId()),
                                ByteBuffer.allocate(8).putLong(floor).array());
                        batch.deleteRange(receipts, receiptKey(new OperationId(command.operationId().sessionId(), command.operationId().epoch(), 0)),
                                receiptKey(new OperationId(command.operationId().sessionId(), command.operationId().epoch(), floor + 1)));
                    }
                    if(sessionMetadata!=null)batch.put(state,sessionAckCountKey(),ByteBuffer.allocate(8).putLong(sessionMetadata.count()).array());
                    byte[] encodedReceipt = encodeReceipt(digest, stableResult);
                    updateReceiptUsage(batch, command, effectiveMinimum, effectiveAcknowledgement, currentEpochFloor, currentFloor, encodedReceipt.length, position);
                    publishSourceGc=sourcePublicationGc(batch,Math.max(currentEpochFloor,effectiveMinimum),effectiveAcknowledgement,command.operationId());
                    batch.put(receipts, receiptKey, encodedReceipt);
                }
                writeHomeCredit(batch,creditChange); batch.put(state, markerKey, encodePosition(position));
                if (saved != null) batch.put(state, usageKey, encodeTotalUsage(readUsage(usageKey), position));
                db.write(options, batch);
            }
            publishCdc.run();publishSourceGc.run();publishSessionMetadata(sessionMetadata);
            if(catalogueChange!=null){catalogueHistoryWriteObserver.afterWrite();catalogueHistoryCache=catalogueChange.update().state();catalogueDirectorySlots+=catalogueChange.directoryDelta();}
            if(homeStatusChange!=null)homeStatusWriteObserver.afterWrite();
            publishHomeCredit(creditChange); publishHomeStatus(homeStatusChange); publishScoped(position,writes,false); applied = position;
            return saved == null ? stableResult : original;
        } catch (RocksDBException | RuntimeException | Error failure) { if (activeSchemaFence==null && activeTopologyFreeze==null && topologyPreparation==null) resetScopedUnderLane(); throw failure; }
        finally { applyLane.unlock(); }
    }

    private byte[] authorityKey() { return ByteBuffer.allocate(17).put(markerKey).put((byte)0x13).array(); }
    private byte[] bindingPrefix() { return ByteBuffer.allocate(17).put(markerKey).put((byte)0x14).array(); }
    private byte[] bindingKey(long epoch) { return ByteBuffer.allocate(25).put(bindingPrefix()).putLong(epoch).array(); }
    private SessionAuthority sessionAuthority() throws RocksDBException { return sessionAuthority(null); }
    private long cdcAuthorityTime() throws RocksDBException { var authority=sessionAuthority(); return authority==null ? -1 : authority.logicalTimeMillis(); }
    private SessionAuthority sessionAuthority(ReadOptions options) throws RocksDBException {
        byte[] bytes=options==null ? db.get(state,authorityKey()) : db.get(state,options,authorityKey());
        if (bytes==null) return null;
        var control=RangeSessionControl.decode(bytes);
        if (control.binding()) throw new IllegalStateException("Invalid persisted session authority");
        return control.authority();
    }
    private RangeSessionControl sessionBinding(long epoch) throws RocksDBException {
        byte[] bytes=db.get(state,bindingKey(epoch));
        if (bytes==null) return null;
        var control=RangeSessionControl.decode(bytes);
        if (!control.binding() || control.grant().epoch()!=epoch) throw new IllegalStateException("Invalid persisted session binding");
        return control;
    }
    private SessionAuthority effectiveAuthority(RangeSessionControl control,SessionAuthority current) {
        if (current==null) return control.authority();
        try { control.authority().requireAtLeast(current); return control.authority(); }
        catch (IllegalArgumentException older) {
            current.requireAtLeast(control.authority()); return current;
        }
    }
    private CommandResult.Status sessionDecision(DistributedCommand command,long acknowledged,SessionAuthority authority) throws RocksDBException {
        var control=command.sessionControl();
        if (control!=null) {
            final SessionAuthority effective;
            try { effective=effectiveAuthority(control,authority); }
            catch (IllegalArgumentException stale) { return CommandResult.Status.CONFLICT; }
            if (control.binding()) {
                if (!effective.permits(control.grant()) || control.grant().epoch()<minimumSessionEpoch(null)) return CommandResult.Status.SESSION_EXPIRED;
                var previous=sessionBinding(control.grant().epoch());
                if (previous!=null && (!previous.grant().equals(control.grant()) || previous.outstandingWindow()!=control.outstandingWindow())) return CommandResult.Status.CONFLICT;
                if (previous==null) {
                    int count=0;
                    byte[] prefix=bindingPrefix(), end=prefixEnd(prefix);
                    try (var lower=new org.rocksdb.Slice(prefix); var upper=new org.rocksdb.Slice(end);
                            var options=new ReadOptions().setFillCache(false).setIterateLowerBound(lower).setIterateUpperBound(upper);
                            var iterator=databaseOwner.newIterator(state,options)) {
                        iterator.seek(bindingKey(Math.max(minimumSessionEpoch(null),control.authority().minimumEpoch())));
                        while (iterator.isValid()) {
                            if (++count>=SessionEpochLedger.MAX_GRANTS) return CommandResult.Status.CONFLICT;
                            iterator.next();
                        }
                        iterator.status();
                    }
                }
            }
            return CommandResult.Status.SUCCESS;
        }
        if (authority==null || command.minimumSessionEpoch()>=0) return CommandResult.Status.SUCCESS;
        var id=command.operationId(); var binding=sessionBinding(id.epoch());
        if (binding==null || !binding.grant().sessionId().equals(id.sessionId())) return CommandResult.Status.SESSION_EXPIRED;
        boolean acknowledgementOnly=!command.transactionRejection() && command.acknowledgedThrough()>=0 && command.mutations().isEmpty() && command.predicates().isEmpty();
        if (!acknowledgementOnly && !authority.permits(binding.grant())) return CommandResult.Status.SESSION_EXPIRED;
        return withinSessionWindow(command,acknowledged,binding.outstandingWindow()) ? CommandResult.Status.SUCCESS : CommandResult.Status.CONFLICT;
    }
    private static boolean withinSessionWindow(DistributedCommand command,long acknowledged,int window) {
        long floor=Math.max(acknowledged,command.acknowledgedThrough());
        return floor>Long.MAX_VALUE-window || command.operationId().sequence()<=floor+window;
    }
    // Only an installed immutable grant proves individual expiry. Unbound requests remain retryable until binding.
    private boolean expiredBoundOutcome(DistributedCommand command,long acknowledged,SessionAuthority authority) throws RocksDBException {
        if (authority==null || command.minimumSessionEpoch()>=0) return false;
        var id=command.operationId(); var binding=sessionBinding(id.epoch());
        return binding!=null && binding.grant().sessionId().equals(id.sessionId())
                && binding.grant().expiresAtMillis()<=authority.logicalTimeMillis()
                && withinSessionWindow(command,acknowledged,binding.outstandingWindow());
    }
    private void installSessionControl(WriteBatch batch,RangeSessionControl control) throws RocksDBException {
        var effective=effectiveAuthority(control,sessionAuthority());
        batch.put(state,authorityKey(),new RangeSessionControl(effective,null,0).encode());
        if (control.binding()) batch.put(state,bindingKey(control.grant().epoch()),control.encode());
    }

    private record Evaluation(CommandResult result,List<Mutation> effects,Map<BucketKey,DistributedBucket> buckets) {
        Evaluation(CommandResult result,List<Mutation> effects) { this(result,effects,Map.of()); }
    }
    private Evaluation evaluate(DistributedCommand command) throws RocksDBException {
        if (expiredHomeRegistration(command)) return new Evaluation(new CommandResult(CommandResult.Status.SESSION_EXPIRED,List.of()),List.of());
        if (!command.hasBucketData()) return new Evaluation(evaluateRaw(command),command.mutations());
        try { return evaluateBuckets(command); }
        catch (DistributedBucket.LimitExceededException limit) { return new Evaluation(new CommandResult(CommandResult.Status.RESULT_TOO_LARGE,List.of()),List.of()); }
    }
    private static final class BucketKey {
        final UUID column; final byte[] key; final int hash;
        BucketKey(UUID column,byte[] key) { this.column=column; this.key=key.clone(); hash=31*column.hashCode()+Arrays.hashCode(key); }
        @Override public int hashCode() { return hash; }
        @Override public boolean equals(Object other) { return other instanceof BucketKey bucket && column.equals(bucket.column) && Arrays.equals(key,bucket.key); }
    }
    /** At most one entry per command point/range; no stored key is enumerated to represent a deletion. */
    private static final class RangeOverlay {
        private record Deletion(UUID column,byte[] lower,byte[] upper,int order) {}
        final boolean enabled;
        private final List<Deletion> ranges=new ArrayList<>();
        private final Map<UUID,java.util.NavigableMap<BucketKey,Integer>> points=new HashMap<>();
        RangeOverlay(DistributedCommand command) { enabled=command.mutations().stream().anyMatch(m -> m.kind()==Mutation.Kind.DELETE_RANGE); }
        void delete(Mutation mutation,int order) { ranges.add(new Deletion(mutation.columnId(),mutation.key(),mutation.value(),order)); }
        void point(UUID column,byte[] key,int order) {
            if (enabled) points.computeIfAbsent(column,ignored -> new java.util.TreeMap<>((a,b) -> Arrays.compareUnsigned(a.key,b.key))).put(new BucketKey(column,key),order);
        }
        Deletion covering(UUID column,byte[] key) {
            for (int i=ranges.size()-1;i>=0;i--) {
                var range=ranges.get(i);
                if (range.column().equals(column) && Arrays.compareUnsigned(key,range.lower())>=0 && Arrays.compareUnsigned(key,range.upper())<0) return range;
            }
            return null;
        }
        boolean deleted(UUID column,byte[] key) {
            var range=covering(column,key); if (range==null) return false;
            var writes=points.get(column);
            return range.order()>(writes==null ? -1 : writes.getOrDefault(new BucketKey(column,key),-1));
        }
        void hideDeleted(WriteBatchWithIndex batch,Map<UUID,ColumnFamilyHandle> columns) throws RocksDBException {
            for (var column:points.entrySet()) for (var point:column.getValue().keySet()) {
                if (deleted(column.getKey(),point.key)) batch.delete(columns.get(column.getKey()),point.key);
            }
        }
        /** Seek past an erased interval, stopping at a later staged point inside it if necessary. */
        void skipDeleted(org.rocksdb.RocksIterator iterator,UUID column,boolean reverse,byte[] lower,byte[] upper) {
            while (enabled && withinPageBounds(iterator,lower,upper)) {
                byte[] key=iterator.key(); var range=covering(column,key);
                if (range==null || !deleted(column,key)) return;
                var writes=points.get(column); byte[] target=reverse ? range.lower() : range.upper(); boolean resurrection=false;
                if (writes!=null) {
                    var point=reverse ? writes.lowerEntry(new BucketKey(column,key)) : writes.higherEntry(new BucketKey(column,key));
                    while (point!=null && Arrays.compareUnsigned(point.getKey().key,range.lower())>=0 && Arrays.compareUnsigned(point.getKey().key,range.upper())<0) {
                        if (!deleted(column,point.getKey().key)) { target=point.getKey().key; resurrection=true; break; }
                        point=reverse ? writes.lowerEntry(point.getKey()) : writes.higherEntry(point.getKey());
                    }
                }
                if (reverse) {
                    iterator.seekForPrev(target);
                    if (!resurrection && iterator.isValid() && Arrays.equals(iterator.key(),target)) iterator.prev();
                } else iterator.seek(target);
            }
        }
    }
    private static final class BucketBudget {
        long bytes;
        void replace(long previous,long next) {
            long total=bytes-previous+next;
            if (total>MAX_BUCKET_PLAN_BYTES) throw new DistributedBucket.LimitExceededException("Bucket apply plan exceeds protocol byte budget"); bytes=total;
        }
    }
    private DistributedBucket originalBucket(BucketKey key,Map<BucketKey,DistributedBucket> originals,BucketBudget budget,ReadOptions options) throws RocksDBException {
        var bucket=originals.get(key); if (bucket!=null) return bucket;
        byte[] raw=db.get(columns.get(key.column),options,key.key); budget.replace(0,raw==null ? 0 : raw.length);
        bucket=logicalSchema.decodeBucket(key.column,key.key,raw);
        originals.put(key,bucket); return bucket;
    }
    private Evaluation evaluateBuckets(DistributedCommand command) throws RocksDBException {
        try (var overlay=new WriteBatchWithIndex(true); var options=new ReadOptions()) {
            return evaluatePoints(command,overlay,options);
        }
    }
    private Evaluation evaluatePoints(DistributedCommand command,WriteBatchWithIndex overlay,ReadOptions options) throws RocksDBException {
        return evaluatePoints(command,overlay,options,new RangeOverlay(command));
    }
    private Evaluation evaluatePoints(DistributedCommand command,WriteBatchWithIndex overlay,ReadOptions options,RangeOverlay visibility) throws RocksDBException {
        var budget=new BucketBudget(); var originals=new HashMap<BucketKey,DistributedBucket>();
        if (command.acceptance()==null) for (var predicate:command.predicates()) {
            byte[] current=predicate.bucket() ? originalBucket(new BucketKey(predicate.columnId(),predicate.key()),originals,budget,options).get(predicate.memberKeys())
                    : db.get(columns.get(predicate.columnId()),options,predicate.key());
            if (!Arrays.equals(current,predicate.expectedValue())) return new Evaluation(new CommandResult(CommandResult.Status.CONFLICT,List.of()),List.of());
        }
        var staged=new LinkedHashMap<BucketKey,DistributedBucket>(); var operatorReads=new HashMap<BucketKey,Integer>(); var effects=new ArrayList<Mutation>(); var values=new ArrayList<byte[]>(); long resultBytes=8;
        int order=0;
        for (var mutation:command.mutations()) {
            if (mutation.kind()==Mutation.Kind.DELETE_RANGE) { visibility.delete(mutation,order++); effects.add(mutation); continue; }
            byte[] value=null;
            if (mutation.bucket()) {
                var key=new BucketKey(mutation.columnId(),mutation.key());
                if(logicalSchema.column(mutation.columnId()).operator()!=null) {
                    var cf=columns.get(mutation.columnId());
                    if(visibility.deleted(mutation.columnId(),mutation.key())) overlay.delete(cf,mutation.key());
                    if(mutation.returnMode()==Mutation.ReturnMode.PREVIOUS) value=operatorBucketMember(mutation,overlay,options,operatorReads,budget);
                    var type=switch(mutation.kind()) { case BUCKET_PUT -> BucketMergeOperand.Type.PUT; case BUCKET_DELETE -> BucketMergeOperand.Type.DELETE; case BUCKET_MERGE -> BucketMergeOperand.Type.MERGE; default -> throw new IllegalStateException("Invalid member operation"); };
                    byte[] operand=BucketMergeOperand.encode(type,mutation.memberKeys(),mutation.value()); budget.replace(0,operand.length);
                    overlay.merge(cf,mutation.key(),operand); effects.add(new Mutation(Mutation.Kind.MERGE,mutation.columnId(),mutation.key(),operand));
                    if(mutation.returnMode()==Mutation.ReturnMode.CURRENT) value=operatorBucketMember(mutation,overlay,options,operatorReads,budget);
                } else {
                    var prior=staged.get(key);
                    var bucket=visibility.deleted(mutation.columnId(),mutation.key()) ? logicalSchema.decodeBucket(mutation.columnId(),mutation.key(),null)
                            : prior==null ? originalBucket(key,originals,budget,options) : prior;
                    if (mutation.returnMode()==Mutation.ReturnMode.PREVIOUS) value=bucket.get(mutation.memberKeys());
                    if(mutation.kind()==Mutation.Kind.BUCKET_MERGE) throw new IllegalArgumentException("Bucket has no activated operator");
                    var next=mutation.kind()==Mutation.Kind.BUCKET_PUT ? bucket.put(mutation.memberKeys(),mutation.value()) : bucket.remove(mutation.memberKeys());
                    budget.replace(prior==null ? 0 : prior.encodedSize(),next.encodedSize()); staged.put(key,next);
                    if (mutation.returnMode()==Mutation.ReturnMode.CURRENT) value=next.get(mutation.memberKeys());
                }
            } else {
                var cf=columns.get(mutation.columnId()); byte[] key=mutation.key();
                if (visibility.deleted(mutation.columnId(),key)) overlay.delete(cf,key);
                if (mutation.returnMode()==Mutation.ReturnMode.PREVIOUS) value=overlay.getFromBatchAndDB(db,cf,options,key);
                switch (mutation.kind()) {
                    case PUT -> overlay.put(cf,key,mutation.value()); case DELETE -> overlay.delete(cf,key); case MERGE -> overlay.merge(cf,key,mutation.value());
                    default -> throw new IllegalStateException("Non-point effect escaped bucket validation");
                }
                effects.add(mutation);
                if (mutation.returnMode()==Mutation.ReturnMode.CURRENT) value=overlay.getFromBatchAndDB(db,cf,options,key);
            }
            visibility.point(mutation.columnId(),mutation.key(),order++);
            if (mutation.returnMode()!=Mutation.ReturnMode.NONE) {
                resultBytes+=4L+(value==null ? 0 : value.length);
                if (resultBytes>MAX_RESULT_BYTES) return new Evaluation(new CommandResult(CommandResult.Status.RESULT_TOO_LARGE,List.of()),List.of());
                values.add(value);
            }
        }
        visibility.hideDeleted(overlay,columns);
        staged.entrySet().removeIf(entry -> visibility.deleted(entry.getKey().column,entry.getKey().key));
        return new Evaluation(new CommandResult(CommandResult.Status.SUCCESS,values),List.copyOf(effects),staged);
    }
    private byte[] operatorBucketMember(Mutation mutation,WriteBatchWithIndex overlay,ReadOptions options,Map<BucketKey,Integer> reads,BucketBudget budget) throws RocksDBException {
        byte[] raw=overlay.getFromBatchAndDB(db,columns.get(mutation.columnId()),options,mutation.key());
        var key=new BucketKey(mutation.columnId(),mutation.key()); int size=raw==null ? 0 : raw.length; int previous=reads.getOrDefault(key,0);
        if(size>previous) { budget.replace(previous,size); reads.put(key,size); }
        return logicalSchema.decodeBucket(mutation.columnId(),mutation.key(),raw).get(mutation.memberKeys());
    }
    private CommandResult evaluateRaw(DistributedCommand command) throws RocksDBException {
        if (command.mutations().stream().allMatch(m -> m.returnMode()==Mutation.ReturnMode.NONE)) {
            if (command.acceptance()==null) for (var predicate:command.predicates()) {
                if (!Arrays.equals(db.get(columns.get(predicate.columnId()),predicate.key()),predicate.expectedValue())) {
                    return new CommandResult(CommandResult.Status.CONFLICT,List.of());
                }
            }
            return new CommandResult(CommandResult.Status.SUCCESS,List.of());
        }
        try (var overlay=new WriteBatchWithIndex(true); var options=new ReadOptions()) {
            return evaluatePoints(command,overlay,options).result();
        }
    }

    /** Every committed configuration/no-op entry must be accounted for in log order. */
    public void applyNoop(ApplyPosition position) throws RocksDBException {
        applyNoop(position, null);
    }

    public void applyNoop(ApplyPosition position, byte[] configuration) throws RocksDBException {
        enterNative();
        try { applyNoopUnderAdmission(position, configuration); }
        finally { try { drainScopedReleases(); } finally { leaveNative(); } }
    }

    private void applyNoopUnderAdmission(ApplyPosition position, byte[] configuration) throws RocksDBException {
        applyLane.lock();
        try {
            if(pendingCurrentCompletion!=null)throw new IllegalStateException("Unknown CURRENT completion force requires original retry");
            if(currentRangeRecovery!=null && !currentImageInstalledUnderLane())throw new IllegalStateException("CURRENT log replay requires registered image first");
            if(metadataRecoveryRegistration!=null && !metadataImageInstalledUnderLane())throw new IllegalStateException("Metadata log replay requires its registered source image first");
            if(importIntent!=null && position.index()>=importIntent.position().index())throw new IllegalStateException("Pending native import owns its original committed apply position");
            var current = ownership.get(owner).state();
            if (pendingInstallation != null || (current != RangeOwnership.State.ACTIVE && current != RangeOwnership.State.FENCED && !(current==RangeOwnership.State.RETIRED && topologyRetired()))) {
                throw new IllegalStateException("Range cannot complete Raft accounting");
            }
            ensureReceiptUsage();
            if (applied != null && position.index() <= applied.index()) return;
            requireNext(position);
            var writes=prepareScopedWrites(false,List.of(),Map.of());
            Runnable publishCdc=()->{};
            try (WriteBatch batch = new WriteBatch();
                    WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false)) {
                if(cdc!=null) publishCdc=cdc.initialize(batch);
                batch.put(state, markerKey, encodePosition(position));
                batch.put(state, usageKey, encodeTotalUsage(readUsage(usageKey), position));
                if (configuration != null) {
                    batch.put(state, configurationKey, encodeConfiguration(new Configuration(position.term(), position.index(), configuration)));
                }
                db.write(options, batch);
            }
            publishCdc.run();
            publishScoped(position,writes,configuration!=null); applied = position;if(configuration!=null && topologyLearner!=null)qualifyLearnerServing();
        } catch (RocksDBException | RuntimeException | Error failure) { resetScopedUnderLane(); throw failure; }
        finally { applyLane.unlock(); }
    }

    /**
     * Replaces only this fenced interval, retaining a durable INSTALLING marker
     * until all imported data and metadata have been flushed through the WAL.
     * The caller supplies consensus/ownership authority and keeps the pool open.
     * Successful installation leaves ownership fenced for explicit activation.
     */
    public NativeRangeImage.Manifest installSnapshot(Path image, NativeRangeImage.Limits limits)
            throws IOException, RocksDBException {
        return installSnapshot(image, limits, batches -> {});
    }

    public NativeRangeImage.Manifest installSnapshot(Path image, NativeRangeImage.Limits limits,
            InstallationObserver observer) throws IOException, RocksDBException {
        return installSnapshotAdmitted(image,limits,observer,NativeRangeImage.IoAdmission.UNLIMITED);
    }
    public NativeRangeImage.Manifest installSnapshotAdmitted(Path image,NativeRangeImage.Limits limits,
            InstallationObserver observer,NativeRangeImage.IoAdmission admission)throws IOException,RocksDBException {
        enterNative();
        try(var metadataScope=NativeRangeImage.openMetadataAdmission(admission);var metadataPass=metadataScope.pass()) { metadataPass.reserve(recoveryRegistrationBytes(null,limits));return installUnderAdmission(image, limits, observer,false); }
        finally { try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    public NativeRangeImage.Manifest installMetadataSnapshot(Path image,NativeRangeImage.Limits limits)throws IOException,RocksDBException{return installMetadataSnapshot(image,limits,batches->{});}
    /** Trusted sealed-source snapshot replacement. Descendant USER CFs and intervals are never visited. */
    public NativeRangeImage.Manifest installMetadataSnapshot(Path image,NativeRangeImage.Limits limits,InstallationObserver observer)throws IOException,RocksDBException {
        return installMetadataSnapshotAdmitted(image,limits,observer,NativeRangeImage.IoAdmission.UNLIMITED);
    }
    public NativeRangeImage.Manifest installMetadataSnapshotAdmitted(Path image,NativeRangeImage.Limits limits,
            InstallationObserver observer,NativeRangeImage.IoAdmission admission)throws IOException,RocksDBException {
        enterNative();try(var metadataScope=NativeRangeImage.openMetadataAdmission(admission)){if(!topologyRetired())throw new IllegalStateException("Metadata install requires durable source retirement/recovery registration");return installUnderAdmission(image,limits,observer,true);}
        finally{try{drainScopedReleases();}finally{leaveNative();}}
    }

    private int recoveryRegistrationBytes(ReadOptions options,NativeRangeImage.Limits limits)throws RocksDBException,IOException {
        int total=0;
        int[] tags={0x67,0x6a,0x72};
        int[] bounds={NativeTopologyLearnerRegistration.MAX_BYTES,NativeTopologyLearnerImagePermission.MAX_BYTES,NativeCurrentRangeRecovery.MAX_BYTES};
        for(int i=0;i<tags.length;i++) {
            byte[] key=topologyOwnershipKey(tags[i]);
            int size=options==null?db.get(state,key,new byte[0]):db.get(state,options,key,new byte[0]);
            if(size==RocksDB.NOT_FOUND)continue;
            if(size<0 || size>bounds[i] || (long)size+17>limits.maxImageBytes())throw new IOException("Recovery registration exceeds image metadata bound");
            total=Math.addExact(total,size);
        }
        return total;
    }

    private static boolean topologySchemaRow(byte[] key) { return key.length>16 && (key[16]==0x27 || key[16]==0x28 || key[16]==0x2a || key[16]==0x2b || key[16]==0x2c); }
    private Map<String,byte[]> topologySchemaRows() throws RocksDBException {
        var rows=new HashMap<String,byte[]>();long bytes=0;
        try(var iterator=databaseOwner.newIterator(state)) {
            iterator.seek(markerKey);
            while(iterator.isValid()) {
                byte[] key=iterator.key();if(key.length<16 || !Arrays.equals(key,0,16,markerKey,0,16))break;
                if(topologySchemaRow(key)) { byte[] value=iterator.value();if((bytes+=value.length)>16L*1024*1024 || rows.size()>=131)throw new IllegalStateException("Topology local schema evidence exceeds retention budget");rows.put(HexFormat.of().formatHex(key),value); }
                iterator.next();
            }
            iterator.status();
        }
        return Map.copyOf(rows);
    }
    private Map<String,byte[]> validateTopologySchemaImage(Path image,NativeRangeImage.Limits limits,NativeTopologyLearnerImagePermission permission) throws IOException,RocksDBException {
        if(currentRangeRecovery!=null){var source=currentRangeRecovery.sourceSchemaRows();var seen=new HashSet<String>();NativeRangeImage.visit(image,limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE && topologySchemaRow(key)){String identity=HexFormat.of().formatHex(key);if(!Arrays.equals(source.get(identity),value))throw new IOException("CURRENT image changed qualified source schema rows");seen.add(identity);}});if(!seen.equals(source.keySet()))throw new IOException("CURRENT image lost qualified source schema rows");return currentRangeRecovery.localSchemaRows();}
        if(topologyLearner!=null) {var source=permission==null ? topologyLearner.sourceSchemaRows() : permission.sourceSchemaRows();var seen=new HashSet<String>();NativeRangeImage.visit(image,limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE && topologySchemaRow(key)){String identity=HexFormat.of().formatHex(key);if(!Arrays.equals(source.get(identity),value))throw new IOException("Learner schema changed admitted original source evidence");seen.add(identity);}});if(!seen.equals(source.keySet()))throw new IOException("Learner image lost admitted source schema evidence");return permission==null ? topologyLearner.localSchemaRows() : learnerSchemaRows(permission);}
        var local=topologySchemaRows();var seen=new HashSet<String>();
        NativeRangeImage.visit(image,limits,(section,column,key,value)->{
            if(section!=NativeRangeImage.Section.STATE || !topologySchemaRow(key))return;
            var identity=HexFormat.of().formatHex(key);byte[] original=local.get(identity);
            if(original==null)throw new IOException("Topology image schema requires qualified local schema preparation/INSTALL/ACTIVATE first");
            try {
                if(key[16]==0x28 || key[16]==0x2a) {
                    var incoming=NativeSchemaInstallation.decode(value);var retained=NativeSchemaInstallation.decode(original);retained.requireDefinition(incoming.definition());
                    if(!retained.fence().equals(incoming.fence()) || !retained.position().equals(incoming.position()) || !retained.sourceMerges().equals(incoming.sourceMerges()) || !retained.targetMerges().equals(incoming.targetMerges()))throw new IllegalArgumentException("Topology image schema changed common original native evidence");
                } else if(!Arrays.equals(original,value))throw new IllegalArgumentException("Topology image schema changed original fence/activation evidence");
            } catch(IllegalArgumentException invalid) { throw new IOException("Topology image schema origin differs",invalid); }
            seen.add(identity);
        });
        if(!seen.equals(local.keySet()))throw new IOException("Topology image discards retained original local schema evidence");
        return local;
    }
    static byte[] topologySchemaValue(Map<String,byte[]> rows,byte[] key,byte[] value) {
        return rows==null || !topologySchemaRow(key) ? value : Objects.requireNonNull(rows.get(HexFormat.of().formatHex(key)),"Missing prevalidated local schema image evidence");
    }

    private NativeRangeImage.Manifest installUnderAdmission(Path image, NativeRangeImage.Limits limits,
            InstallationObserver observer,boolean metadata) throws IOException, RocksDBException {
        if(importPending()) throw new IllegalStateException("Unpublished native import cannot be replaced by snapshot replay");
        var context=homeRecoveryContext;
        var snapshotContext=snapshotHomeContext;
        var expected = metadata?NativeRangeImage.inspectMetadata(image,limits):snapshotContext!=null ? NativeRangeImage.inspectSnapshotHome(image,limits,snapshotContext.cluster(),snapshotContext.database(),snapshotContext.column()) : context==null ? NativeRangeImage.inspect(image, limits) : NativeRangeImage.inspectHomeRecoveryReservations(image,limits,context.cluster(),context.database(),context.column());
        if (!expected.owner().equals(owner) || !expected.interval().equals(interval)
                || !Arrays.equals(expected.schemaDigest(), schemaDigest)
                || !expected.deterministicMergeColumns().equals(deterministicMergeColumns)) {
            throw new IOException("Snapshot target identity/schema/coverage mismatch");
        }
        Objects.requireNonNull(observer);
        applyLane.lock();
        try {
            if(metadata) {
                if(applied!=null && !NativeMetadataRecoveryRegistration.covers(expected.position(),applied))throw new IOException("Metadata snapshot regresses actual completed source prefix");
                var registration=metadataRecoveryRegistration;
                if(registration!=null) {
                    if(!expected.position().equals(registration.imageCut()) || !Arrays.equals(expected.sha256(),registration.imageHash()))throw new IOException("Metadata snapshot differs from exact registered image");
                    Configuration[] configuration={null};NativeTopologyFreeze[] freeze={null};NativeRangeImage.visitMetadata(image,limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE){if(Arrays.equals(key,configurationKey))configuration[0]=Configuration.decode(value);if(Arrays.equals(key,topologyFreezeKey()))freeze[0]=NativeTopologyFreeze.decode(value);}});
                    if(!registration.freeze().equals(freeze[0]))throw new IOException("Metadata image changed registered original FREEZE");
                    if(configuration[0]==null || configuration[0].term()!=registration.configurationTerm() || configuration[0].index()!=registration.configurationIndex() || !Arrays.equals(configuration[0].data(),registration.sourceConfiguration()))throw new IOException("Metadata image changed registered source configuration");
                }
            }
            if(pendingLearnerImagePermission!=null)throw new IllegalStateException("Unknown later image force requires exact permission retry");
            final NativeTopologyLearnerImagePermission permission=topologyLearner!=null && learnerImagePermission!=null && expected.position().equals(learnerImagePermission.imageCut()) && Arrays.equals(expected.sha256(),learnerImagePermission.imageHash()) ? learnerImagePermission : null;
            final NativeTopologyActivation localActivation=metadata?null:qualifyTopologyImageOrigin(expected.topologyOrigin());
            if(!metadata && currentRangeRecovery!=null){var registration=currentRangeRecovery;
                if(!expected.position().equals(registration.imageCut()) || !Arrays.equals(expected.sha256(),registration.imageHash()) || applied!=null && !NativeMetadataRecoveryRegistration.covers(expected.position(),applied))throw new IOException("CURRENT image changed registered hash/cut or regressed actual tail");
                var cfg=new Configuration[1];NativeRangeImage.visit(image,limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE && Arrays.equals(key,configurationKey))cfg[0]=Configuration.decode(value);});
                if(cfg[0]==null || cfg[0].term()!=registration.configurationTerm() || cfg[0].index()!=registration.configurationIndex() || !Arrays.equals(cfg[0].data(),registration.sourceConfiguration()))throw new IOException("CURRENT image changed registered full configuration tuple");
            }
            if(!metadata && topologyLearner!=null) {
                if(permission==null && (!expected.position().equals(topologyLearner.imageCut()) || !Arrays.equals(expected.sha256(),topologyLearner.imageHash())))throw new IOException("Learner image lacks exact original or freshly qualified later permission");
                var configuration=new Configuration[1];NativeRangeImage.visit(image,limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE && key.length==17 && key[16]==1)configuration[0]=Configuration.decode(value);});
                if(configuration[0]==null || !Arrays.equals(configuration[0].data(),(permission==null ? topologyLearner.sourceConfiguration() : permission.sourceConfiguration())))throw new IOException("Learner image changed admitted source configuration");
            }
            final Map<String,byte[]> localSchemaRows=metadata || localActivation==null && topologyLearner==null && currentRangeRecovery==null ? null : validateTopologySchemaImage(image,limits,permission);
            if(!metadata && !sourceOutcomeCredits.isEmpty()) {
                var incomingCredits=new HashMap<OperationId,NativeSourceOutcomeCredit>();var incomingPublications=new HashMap<OperationId,NativeSourceOutcomePublication>();
                NativeRangeImage.visit(image,limits,(section,column,key,value)->{ if(section==NativeRangeImage.Section.STATE && key.length==49) { if(key[16]==0x65){var credit=NativeSourceOutcomeCredit.decode(value);incomingCredits.put(credit.request().original().operationId(),credit);}if(key[16]==0x66){var publication=NativeSourceOutcomePublication.decode(value);incomingPublications.put(publication.operation(),publication);} } });
                for(var entry:sourceOutcomeCredits.entrySet()) { var retained=incomingCredits.get(entry.getKey());var terminal=incomingPublications.get(entry.getKey());if(retained!=null ? !retained.equals(entry.getValue()) : terminal==null || !Arrays.equals(terminal.requestDigest(),entry.getValue().request().digest()) || !terminal.creditPosition().equals(entry.getValue().position()))throw new IOException("Snapshot discards/changes durable source outcome credit"); }
            }
            if(topologyPreparation!=null) throw new IllegalStateException("Topology preparation owns native replacement admission");
            if(activeTopologyFreeze!=null) {
                var incoming=new NativeTopologyFreeze[1]; NativeRangeImage.visitKind(image,limits,(section,column,key,value) -> { if(section==NativeRangeImage.Section.STATE && key.length==17 && key[16]==0x60) incoming[0]=NativeTopologyFreeze.decode(value); },metadata);
                if(!activeTopologyFreeze.equals(incoming[0])) throw new IOException("Snapshot cannot discard/change native topology FREEZE");
            }
            if(catalogueHistoryColumn!=null)NativeRangeImage.inspectCatalogueHistory(image,limits,catalogueHistoryColumn,catalogueHistoryKeys);
            if(sourceOutcomeCountLimit!=0)NativeRangeImage.inspectReceiptCapacity(image,limits,sourceOutcomeCountLimit,sourceOutcomeByteLimit,metadata);
            NativeRangeImage.inspectSessionMetadata(image,limits,sessionAckLimit,metadata);
            if(cdc!=null) cdc.validateImage(image,limits,expected.position(),metadata);
            if(!metadata && importBirth!=null) {
                byte[][] incoming=new byte[3][];
                NativeRangeImage.visit(image,limits,(section,column,key,value)->{ if(section==NativeRangeImage.Section.STATE && key.length==17) { if(key[16]==0x40)incoming[0]=value; if(key[16]==0x42)incoming[1]=value; if(key[16]==0x43)incoming[2]=value; } });
                if(!Arrays.equals(importBirth.encode(),incoming[0]) || importReceipt==null || !Arrays.equals(importReceipt.encode(),incoming[1]) || importActivation==null || !Arrays.equals(importActivation.encode(),incoming[2]))throw new IOException("Snapshot cannot discard or change native import publication evidence");
            }
            if(!metadata && schemaPreparationPool!=null) schemaPreparationPool.validateSnapshotCapacity(this,image,limits,localSchemaRows);
            final boolean preparingLearner=permission!=null && permission.equals(learnerSchemaPreparation);
            if(!metadata && (activeSchemaFence!=null || activeSchemaInstallation!=null || activeSchemaActivation!=null)) {
                var incoming=new NativeSchemaFence[1]; var incomingInstall=new NativeSchemaInstallation[1]; var incomingActivation=new NativeSchemaActivationProof[1];
                NativeRangeImage.visit(image,limits,(section,column,key,value) -> {
                    if (section==NativeRangeImage.Section.STATE && key.length==17 && key[16]==0x27) incoming[0]=NativeSchemaFence.decode(value);
                    if (section==NativeRangeImage.Section.STATE && key.length==17 && key[16]==0x28) incomingInstall[0]=NativeSchemaInstallation.decode(topologySchemaValue(localSchemaRows,key,value));
                    if(section==NativeRangeImage.Section.STATE && key.length==17 && key[16]==0x2c) incomingActivation[0]=NativeSchemaActivationProof.decode(value);
                });
                if(activeSchemaFence!=null && !activeSchemaFence.equals(incoming[0])) throw new IOException("Snapshot cannot discard or change a committed native schema fence");
                if(!preparingLearner && activeSchemaInstallation!=null && (incomingInstall[0]==null || !Arrays.equals(activeSchemaInstallation.encode(),incomingInstall[0].encode()))) throw new IOException("Snapshot cannot discard or change an installed schema generation");
                if(!preparingLearner && activeSchemaActivation!=null && !activeSchemaActivation.equals(incomingActivation[0])) throw new IOException("Snapshot cannot discard or change native activation proof");
            }
            if (snapshotPins.get() != 0) throw new IllegalStateException("Native snapshot readers have not drained");
            if (ownership.get(owner).state() != (metadata?RangeOwnership.State.RETIRED:RangeOwnership.State.FENCED)) {
                throw new IllegalStateException("Snapshot installation requires a fenced interval");
            }
            if (pendingInstallation != null) {
                var pending = ByteBuffer.wrap(pendingInstallation);
                long pendingTerm = pending.getLong();
                long pendingIndex = pending.getLong();
                byte[] hash = new byte[32];
                pending.get(hash);
                if (expected.position().index() < pendingIndex || expected.position().term() < pendingTerm
                        || (expected.position().index() == pendingIndex && !Arrays.equals(hash, expected.sha256()))) {
                    throw new IOException("Snapshot regresses or disagrees with pending installation");
                }
            }
            if(topologyLearner!=null && applied!=null && (expected.position().index()<applied.index() || expected.position().term()<applied.term() || expected.position().index()==applied.index() && expected.position().term()!=applied.term()))throw new IOException("Learner image regresses actual completed tail");
            try (var lease = metadata?null:ownership.claimInstall(owner)) {
                if(metadata){retiredMetadataOnly=true;columns=Map.of();if(installedGeneration!=null)installedGeneration=new SchemaGeneration(installedGeneration.schema(),interval,columns,deterministicMergeColumns);}
                resetScopedUnderLane(); installationGeneration++; homeStatusCapacityKnown=false;
                pendingInstallation = ByteBuffer.allocate(48)
                        .putLong(expected.position().term()).putLong(expected.position().index()).put(expected.sha256()).array();
                // Publish recovery authority before the first destructive interval change.
                try (var batch = new WriteBatch(); var options = new WriteOptions().setDisableWAL(false).setSync(true)) {
                    batch.put(state, installingKey, pendingInstallation);
                    if(metadata && metadataRecoveryRegistration!=null){metadataRecoveryReady=false;batch.delete(state,topologyOwnershipKey(0x70));}
                    if(currentRangeRecovery!=null){currentRangeRecoveryReady=false;batch.delete(state,topologyOwnershipKey(0x73));for(var row:localSchemaRows.entrySet())batch.put(state,HexFormat.of().parseHex(row.getKey()),row.getValue());}
                    if(learnerSchemaPreparation!=null){for(var row:localSchemaRows.entrySet())batch.put(state,HexFormat.of().parseHex(row.getKey()),row.getValue());}
                    batch.delete(state, markerKey);
                    db.write(options, batch);
                }
                applied = null;
                try (var writer = new InstallWriter(observer)) {
                    if(!metadata)for (var cf : columns.values()) clearForInstall(cf, interval.lowerInclusive(), interval.upperExclusive(), null, writer,false);
                    clearForInstall(state, markerKey, null, markerKey, writer,metadata);
                    clearForInstall(receipts, markerKey, null, markerKey, writer,metadata);
                    writer.flush();
                    NativeRangeImage.RowVisitor importer = (section, column, key, value) -> {
                        try {
                            switch (section) {
                                case USER -> {if(metadata)throw new IOException("Metadata installer cannot write USER rows");writer.put(columns.get(column), key, value);}
                                case RECEIPTS -> writer.put(receipts, key, value);
                                case STATE -> {
                                    if(key.length==17 && key[16]>=0x6d && key[16]<=0x71)throw new IOException("Source image cannot replace local recovery fence");
                                    if (Arrays.equals(key, installingKey)) throw new IOException("Image contains incomplete-install metadata");
                                    if(key.length==17 && key[16]>=0x72 && key[16]<=0x74)break;
                                    if(key.length==33 && key[16]==0x68 && currentRangeRecovery!=null)break;
                                    if(key.length==17 && (key[16]==0x67 || key[16]==0x69 || key[16]==0x6a || key[16]==0x6b || key[16]==0x6c) || topologyLearner!=null && key.length==33 && key[16]==0x68)break;
                                    if(localActivation!=null && key.length==17 && (key[16]==0x61 || key[16]==0x63 || key[16]==0x64))break;
                                    if (!Arrays.equals(key, markerKey)) writer.put(state, key, topologySchemaValue(localSchemaRows,key,value));
                                }
                            }
                        } catch (RocksDBException storage) { throw new IOException("Native snapshot import failed", storage); }
                    };
                    var actual=metadata?NativeRangeImage.visitMetadata(image,limits,importer):snapshotContext==null ? NativeRangeImage.visit(image,limits,importer) : NativeRangeImage.visitSnapshotHome(image,limits,snapshotContext.cluster(),snapshotContext.database(),snapshotContext.column(),importer);
                    if (!Arrays.equals(expected.sha256(), actual.sha256())) throw new IOException("Snapshot changed during installation");
                    writer.flush();
                    // Ordinary counters remain bound to the source cut; only local ownership/alias evidence is normalized.
                    recoverSchemaMetadata(expected.position());
                    if(cdc!=null) cdc.recover(expected.position(),cdcAuthorityTime());
                    validateReceiptUsage(expected.position());
                    activeSchemaFence=loadSchemaFence(expected.position()); activeTopologyFreeze=loadTopologyFreeze(expected.position(),false);recoverSourceOutcomes(expected.position());
                    homeRecoveries=loadHomeRecoveries(expected.position()); homeRecoveryCredits=loadHomeCredits(expected.position()); validateHomeCreditCapacity();
                    // Flush every prior CF mutation before publishing the final applied marker.
                    db.flushWal(true);
                    try (var batch = new WriteBatch(); var options = new WriteOptions().setDisableWAL(false).setSync(true)) {
                        if(cdc!=null) cdc.initialize(batch);
                        if(metadata && metadataRecoveryRegistration!=null)batch.put(state,topologyOwnershipKey(0x71),metadataImageReceipt(metadataRecoveryRegistration));
                        if(currentRangeRecovery!=null)batch.put(state,topologyOwnershipKey(0x74),currentImageReceipt());
                        if(topologyLearner!=null)batch.put(state,topologyOwnershipKey(permission==null ? 0x69 : 0x6b),permission==null ? learnerImageReceipt() : ByteBuffer.allocate(48).putLong(permission.imageCut().term()).putLong(permission.imageCut().index()).put(permission.imageHash()).array());
                        if(localActivation!=null) { batch.put(state,topologyOwnershipKey(0x61),localActivation.birth().encode());batch.put(state,topologyOwnershipKey(0x63),localActivation.encode()); }
                        batch.put(state, markerKey, encodePosition(expected.position()));
                        batch.delete(state, installingKey);
                        if(topologyLearner!=null)batch.delete(state,topologyOwnershipKey(0x6c));
                        db.write(options, batch);
                    }
                    publishScoped(expected.position(),prepareScopedWrites(false,List.of(),Map.of()),false); applied = expected.position();
                    recoverImportMetadata();recoverTopologyOwnership();recoverRelocationCompletions();validateTopologyActivationRecovery();
                    installationGeneration++;
                    pendingInstallation = null;qualifyLearnerServing();
                    return actual;
                }
            }
        } finally { applyLane.unlock(); }
    }

    @FunctionalInterface
    public interface InstallationObserver {
        /** Fault/measurement hook; the interval remains exclusively leased and unservable. */
        void afterBatch(int completedBatches) throws IOException;
    }

    private final class InstallWriter implements AutoCloseable {
        private final InstallationObserver observer;
        private final WriteBatch batch = new WriteBatch();
        private final WriteOptions options = new WriteOptions().setDisableWAL(false).setSync(false);
        private long bytes;
        private int batches;
        InstallWriter(InstallationObserver observer) { this.observer = observer; }
        void put(ColumnFamilyHandle cf, byte[] key, byte[] value) throws IOException, RocksDBException {
            reserve(key.length + (long) value.length);
            batch.put(cf, key, value);
            bytes += key.length + (long) value.length + 32;
        }
        void delete(ColumnFamilyHandle cf, byte[] key) throws IOException, RocksDBException {
            reserve(key.length);
            batch.delete(cf, key);
            bytes += key.length + 32L;
        }
        private void reserve(long added) throws IOException, RocksDBException {
            if (batch.count() >= 256 || (batch.count() != 0 && bytes + added + 32 > 2L * 1024 * 1024)) flush();
        }
        void flush() throws IOException, RocksDBException {
            if (batch.count() == 0) return;
            db.write(options, batch);
            batch.clear();
            bytes = 0;
            observer.afterBatch(++batches);
        }
        @Override public void close() { batch.close(); options.close(); }
    }

    private void clearForInstall(ColumnFamilyHandle cf, byte[] lower, byte[] upper, byte[] prefix, InstallWriter writer,boolean metadata)
            throws IOException, RocksDBException {
        try (var options = new ReadOptions().setFillCache(false); var iterator = databaseOwner.newIterator(cf, options)) {
            if (lower == null) iterator.seekToFirst(); else iterator.seek(lower);
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (upper != null && Arrays.compareUnsigned(key, upper) >= 0) break;
                if (prefix != null && (key.length < prefix.length
                        || !Arrays.equals(prefix, 0, prefix.length, key, 0, prefix.length))) break;
                if(metadata){if(cf.getID()!=state.getID() || !Arrays.equals(key,installingKey) && !NativeRangeImage.metadataLocalPointer(key))writer.delete(cf,key);}
                else if (cf.getID() != state.getID() || !Arrays.equals(key, installingKey) && !(key.length==17 && key[16]>=0x6d && key[16]<=0x74) && !(key.length==17 && (key[16]==0x67 || topologyLearner!=null && (key[16]==0x69 || key[16]==0x6a || key[16]==0x6c))) && !(topologyLearner!=null && key.length==33 && key[16]==0x68) && !((topologyBirth!=null || topologyLearner!=null || currentRangeRecovery!=null) && (topologySchemaRow(key) || key.length==17 && (key[16]==0x61 || key[16]==0x63 || key[16]==1)))) writer.delete(cf, key);
                iterator.next();
            }
            iterator.status();
        }
    }

    public byte[] operationResult(OperationId id, byte[] payloadDigest) throws RocksDBException {
        enterNative();
        try { requireMetadataRecoveryReady();return operationResultUnderAdmission(id, payloadDigest); }
        finally { leaveNative(); }
    }

    private byte[] operationResultUnderAdmission(OperationId id, byte[] payloadDigest) throws RocksDBException {
        if (payloadDigest.length != 32) throw new IllegalArgumentException("Invalid payload digest");
        applyLane.lock();
        try {
            if (pendingInstallation != null) throw new IllegalStateException("Range metadata is being installed");
            requireHomeRecoveryFree(id);
            if (id.epoch() < minimumSessionEpoch(null) || id.sequence() <= acknowledgedThrough(id, null)) return expiredResult();
            byte[] key=receiptKey(id);int size=db.get(receipts,key,new byte[0]);
            if(size==org.rocksdb.RocksDB.NOT_FOUND)return null;
            if(size<32 || size>32+MAX_RESULT_BYTES)throw new IllegalStateException("Invalid durable receipt size");
            byte[] saved=new byte[size];if(db.get(receipts,key,saved)!=size)throw new IllegalStateException("Durable receipt changed during bounded outcome read");
            return decodeReceipt(saved,payloadDigest);
        } finally { applyLane.unlock(); }
    }

    /**
     * Captures user data, receipts and marker from the same native cut.
     * The adapter must establish the read/fence barrier before invoking this.
     * A native cut alone does not constitute a durable installable Raft snapshot.
     */
    public SnapshotPin capture() throws RocksDBException {
        enterNative();
        try { return captureUnderAdmission(); }
        finally { leaveNative(); }
    }

    /** One local cut restricted to the complete declared scope; the adapter supplies the read barrier. */
    public ScopedSnapshot capture(TransactionScope scope) throws RocksDBException {
        Objects.requireNonNull(scope);
        enterNative();
        try {
            requireActive();
            validateSnapshotScope(scope);
            return new ScopedSnapshot(scope, captureUnderAdmission());
        } finally { leaveNative(); }
    }

    /** Memory-only admission state; no native reads or snapshot acquisition. */
    public boolean admissionOpen() { return !topologyRecoveryOnly() && !closing && pendingInstallation==null && !importPending() && !topologyBirthPending() && !topologyRetired() && ownership.get(owner).state()==RangeOwnership.State.ACTIVE; }
    /** Memory-only scope validation before entering the short proposal coordination lane. */
    public void validateScope(TransactionScope scope) {
        Objects.requireNonNull(scope); requireSchemaAdmission(); if (!admissionOpen()) throw new IllegalStateException("Range admission is closed"); validateSnapshotScope(scope);
    }
    private void validateSnapshotScope(TransactionScope scope) { validateSnapshotScope(scope,logicalColumns()); }
    private void validateSnapshotScope(TransactionScope scope,Set<UUID> scopeColumns) {
        for (var span : scope.spans()) {
            if (!scopeColumns.contains(span.columnId())) throw new IllegalArgumentException("Scope column is outside this range");
            byte[] lower = span.lowerInclusive(), upper = span.upperExclusive();
            if (span.isPoint()) {
                if (!contains(lower)) throw new IllegalArgumentException("Scope key is outside this range");
            } else if ((interval.lowerInclusive() != null && (lower == null || Arrays.compareUnsigned(lower, interval.lowerInclusive()) < 0))
                    || (interval.upperExclusive() != null && (upper == null || Arrays.compareUnsigned(upper, interval.upperExclusive()) > 0))) {
                throw new IllegalArgumentException("Scope interval is outside this range");
            }
        }
    }
    /** The adapter supplies a fresh leader/read barrier; validation alone confers no commit authority. */
    public ScopedHistoryPin captureTracked(TransactionScope scope) throws RocksDBException { return captureTracked(scope,null); }
    /** Active-gate capture plus history registration is one atomic native cut; release does not replace this pin. */
    public ScopedHistoryPin captureTrackedGate(SnapshotFence fence) throws RocksDBException {
        Objects.requireNonNull(fence); return captureTracked(fence.participant(groupId).scope(),fence);
    }
    private ScopedHistoryPin captureTracked(TransactionScope scope,SnapshotFence fence) throws RocksDBException {
        Objects.requireNonNull(scope); enterNative(); applyLane.lock(); ScopedSnapshot snapshot=null; RangeMutationHistory.Pin registration=null;
        try {
            requireActive(); requireTopologyWritable(); validateSnapshotScope(scope); ensureReceiptUsage(); requireUnlocked(scope);
            if (fence!=null) { requireSnapshotFence(fence); if (fence.operation().epoch()<minimumSessionEpoch(null)) throw new SnapshotRetentionExpiredException(); requireActiveGate(fence); }
            if (scopedHistory==null) throw new UnsupportedOperationException("Scoped mutation history is not enabled");
            if (applied==null || applied.term()==0) throw new IllegalStateException("Tracked snapshot requires a positive completed term");
            scopedHistory.checkOpenCapacity(scope); long scopeBytes=RangeMutationHistory.accountedScopeBytes(scope);
            if (trackedSnapshotPins.get()>=scopedLimits.maxPins() || scopeBytes>scopedLimits.maxScopeBytes()-trackedSnapshotScopeBytes.get()) throw new IllegalStateException("Retired native readers retain scoped admission");
            snapshot=new ScopedSnapshot(scope,captureUnderAdmission());
            snapshot.pin.historyScopeBytes=scopeBytes; trackedSnapshotScopeBytes.addAndGet(scopeBytes); trackedSnapshotPins.incrementAndGet(); snapshot.pin.historyTracked=true;
            if (closing) throw new IllegalStateException("Range admission is closing");
            try { registration=scopedHistory.open(snapshot.position(),scope); }
            catch (RuntimeException | Error failure) { resetScopedUnderLane(); throw failure; }
            var owned=new ScopedHistoryPin(snapshot,scopedHistory,registration,scopedGeneration,installationGeneration); scopedPins.put(registration,owned); snapshot=null; return owned;
        } finally {
            if (snapshot!=null) { if (registration!=null) { scopedHistory.close(registration); scopedPins.remove(registration); } scopedReleases.add(snapshot); }
            applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); }
        }
    }
    /**
     * Caller must already own the complete exclusive proposal scope. This only certifies
     * and freezes the ordered plan; it neither writes native state nor confers append authority.
     */
    public DistributedCommand planScopedCommit(ScopedHistoryPin pin,DistributedCommand command,long generation,UUID incarnation) throws RocksDBException {
        Objects.requireNonNull(pin); Objects.requireNonNull(command);
        if (pin.owner()!=this) throw new IllegalArgumentException("Tracked snapshot belongs to another range");
        if (command.acceptance()!=null || command.sessionControl()!=null || command.minimumSessionEpoch()>=0 || command.mutations().isEmpty() && command.predicates().isEmpty()) throw new IllegalArgumentException("Expected an unaccepted business command");
        validate(command);
        validateScopedCommand(pin,command);
        enterNative(); applyLane.lock();
        try {
            requireActive(); requireTopologyWritable();
            requireUnlocked(pin.scope()); requireUngated(pin.scope());
            if (pin.validateUnderLane()!=RangeMutationHistory.Status.ACTIVE) throw new IllegalStateException("Transaction snapshot failed serializable validation: "+pin.status());
            var id=command.operationId(); long minimum=minimumSessionEpoch(null);
            long floor=id.epoch()<minimum ? -1 : acknowledgedThrough(id,null);
            if (id.epoch()<minimum || id.sequence()<=floor || db.get(receipts,receiptKey(id))!=null
                    || sessionDecision(command,floor,sessionAuthority())!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Transaction operation/session is no longer eligible");
            var evaluation=evaluate(command);
            if (evaluation.result().status()!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Transaction plan rejected: "+evaluation.result().status());
            // Close/fence can begin without this lane; it must not race certification unnoticed.
            requireActive(); if (pin.status()!=RangeMutationHistory.Status.ACTIVE) throw new IllegalStateException("Transaction snapshot lost certification ownership");
            var accepted=DistributedCommand.acceptedTransaction(command,new it.cavallium.rockserver.core.common.distributed.TransactionAcceptance(pin.position(),generation,incarnation,pin.scope(),evaluation.result().encode()));
            validate(accepted); return accepted;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    /** Certify the complete original read scope with no fabricated predicates or user mutations. */
    public DistributedCommand planReadCertification(ScopedHistoryPin pin,OperationId operation,long generation,UUID incarnation) throws RocksDBException {
        Objects.requireNonNull(pin); Objects.requireNonNull(operation); Objects.requireNonNull(incarnation);
        if (pin.owner()!=this) throw new IllegalArgumentException("Tracked snapshot belongs to another range");
        enterNative(); applyLane.lock();
        try {
            requireActive(); requireTopologyWritable(); requireUnlocked(pin.scope()); requireUngated(pin.scope());
            if (pin.validateUnderLane()!=RangeMutationHistory.Status.ACTIVE) throw new IllegalStateException("Transaction snapshot failed serializable validation: "+pin.status());
            var accepted=DistributedCommand.readCertification(groupId,rangeEpoch,schemaDigest,operation,new TransactionAcceptance(pin.position(),generation,incarnation,pin.scope(),new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
            validate(accepted); long minimum=minimumSessionEpoch(null); long floor=operation.epoch()<minimum ? -1 : acknowledgedThrough(operation,null);
            if (operation.epoch()<minimum || operation.sequence()<=floor || db.get(receipts,receiptKey(operation))!=null || sessionDecision(accepted,floor,sessionAuthority())!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Read certification operation/session is no longer eligible");
            requireActive(); if (pin.status()!=RangeMutationHistory.Status.ACTIVE) throw new IllegalStateException("Transaction snapshot lost certification ownership");
            return accepted;
        } finally { applyLane.unlock(); try { drainScopedReleases(); } finally { leaveNative(); } }
    }
    private void validateScopedCommand(ScopedHistoryPin pin,DistributedCommand command) {
        for (var mutation:command.mutations()) {
            if (mutation.kind()==Mutation.Kind.DELETE_RANGE ? !pin.scope().containsRange(mutation.columnId(),mutation.key(),mutation.value())
                    : !pin.scope().contains(mutation.columnId(),mutation.key())) throw new IllegalArgumentException("Mutation outside transaction scope");
        }
        for (var predicate:command.predicates()) if (!pin.scope().contains(predicate.columnId(),predicate.key())) throw new IllegalArgumentException("Predicate outside transaction scope");
    }
    /** Unprepared physical-scope snapshot. All member accesses in a collision bucket share its one physical stamp. */
    public final class ScopedHistoryPin implements AutoCloseable {
        private final ScopedSnapshot snapshot; private final RangeMutationHistory history; private final RangeMutationHistory.Pin registration;
        private final Object generation; private final long installation; private final AtomicBoolean closed=new AtomicBoolean();
        private ScopedHistoryPin(ScopedSnapshot snapshot,RangeMutationHistory history,RangeMutationHistory.Pin registration,Object generation,long installation) {
            this.snapshot=snapshot; this.history=history; this.registration=registration; this.generation=generation; this.installation=installation;
        }
        public TransactionScope scope() { return snapshot.scope(); }
        private ReplicatedRangeStore owner() { return ReplicatedRangeStore.this; }
        public boolean belongsTo(ReplicatedRangeStore store) { return owner()==store; }
        public ApplyPosition position() { return snapshot.position(); }
        public RangeMutationHistory.Status status() {
            var status=registration.status(); if (status!=RangeMutationHistory.Status.ACTIVE) return status;
            if (closed.get()) return RangeMutationHistory.Status.CLOSED;
            return closing || generation!=scopedGeneration || installation!=installationGeneration ? RangeMutationHistory.Status.INVALIDATED : status;
        }
        private void requireReadable() { if (status()!=RangeMutationHistory.Status.ACTIVE) throw new IllegalStateException("Tracked snapshot is no longer valid: "+status()); }
        public byte[] get(UUID column,byte[] key) throws RocksDBException { requireReadable(); return snapshot.get(column,key); }
        public PhysicalRangePage page(UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws RocksDBException {
            requireReadable(); return snapshot.page(column,lower,upper,reverse,resume,items,bytes);
        }
        public void visitColumn(UUID column,byte[] lower,byte[] upper,SnapshotVisitor visitor) throws RocksDBException,IOException {
            requireReadable(); snapshot.visitColumn(column,lower,upper,visitor);
        }
        /** Builds a bounded read-your-writes view at this exact cut, without applying or accepting anything. */
        public ScopedStaging stage(DistributedCommand command) throws RocksDBException {
            requireReadable(); Objects.requireNonNull(command); requireTopologyWritable();
            if (command.kind()!=DistributedCommand.Kind.DATA || command.mutations().isEmpty() && command.predicates().isEmpty()) throw new IllegalArgumentException("Expected business DATA for staging");
            ReplicatedRangeStore.this.validate(command); validateScopedCommand(this,command);
            if (logicalSchema!=null) validateLogical(command);
            return new ScopedStaging(this,command);
        }
        /** Observes complete declared-scope history under the apply lane; a later proposal still needs reservations/revalidation. */
        public RangeMutationHistory.Status validate() {
            applyLane.lock();
            try { return validateUnderLane(); }
            finally { applyLane.unlock(); drainScopedReleases(); }
        }
        private RangeMutationHistory.Status validateUnderLane() {
            var result=status();
            if (result==RangeMutationHistory.Status.ACTIVE) {
                try { requireActive(); result=history.validate(registration); }
                catch (RuntimeException failure) { resetScopedUnderLane(); result=RangeMutationHistory.Status.INVALIDATED; }
            } else if (registration.status()==RangeMutationHistory.Status.ACTIVE) {
                if (result==RangeMutationHistory.Status.CLOSED) history.close(registration);
                else invalidateScopedUnderLane(RangeMutationHistory.Status.INVALIDATED);
            }
            if (result!=RangeMutationHistory.Status.ACTIVE) retireScoped(registration); return result;
        }
        @Override public void close() {
            if (!closed.compareAndSet(false,true)) return;
            try { snapshot.close(); } // Existing JNI users own their deferral; never wait for them under the apply lane.
            finally {
                applyLane.lock(); try { history.close(registration); scopedPins.remove(registration); }
                finally { applyLane.unlock(); drainScopedReleases(); }
            }
        }
    }

    /** Immutable staged prefix. Owns its indexed batch and a native use until close; its parent owns certification. */
    public final class ScopedStaging implements AutoCloseable {
        private final ScopedHistoryPin parent;
        private final DistributedCommand command;
        private final WriteBatchWithIndex overlay;
        private final RangeOverlay visibility;
        private final CommandResult result;
        private final long accountedBytes;
        private boolean closed;
        private ScopedStaging(ScopedHistoryPin parent,DistributedCommand command) throws RocksDBException {
            this.parent=parent; this.command=command; this.visibility=new RangeOverlay(command);
            var pin=parent.snapshot.pin; pin.acquireUse();
            WriteBatchWithIndex building=null; boolean success=false;
            try {
                building=new WriteBatchWithIndex(true);
                building.setMaxBytes(MAX_BUCKET_PLAN_BYTES+3L*DistributedCommandCodec.MAX_ENCODED_BYTES);
                try (var options=new ReadOptions().setSnapshot(pin.snapshot)) {
                    var evaluation=evaluatePoints(command,building,options,visibility);
                    if (evaluation.result().status()!=CommandResult.Status.SUCCESS) throw new IllegalStateException("Transaction staging rejected: "+evaluation.result().status());
                    for (var entry:evaluation.buckets().entrySet()) building.put(columns.get(entry.getKey().column),entry.getKey().key,entry.getValue().encode());
                    result=evaluation.result();
                    // Original operands, raw indexed effects, materialized buckets, result and bounded entry overhead.
                    long bytes=(visibility.enabled ? 5L : 2L)*DistributedCommandCodec.encodedSize(command)+result.encode().length+4096L+128L*(command.mutations().size()+command.predicates().size());
                    for (var bucket:evaluation.buckets().values()) bytes+=bucket.encodedSize();
                    accountedBytes=bytes;
                }
                parent.requireReadable(); overlay=building; building=null; success=true;
            } finally {
                if (!success) { try { if (building!=null) building.close(); } finally { pin.releaseUse(); } }
            }
        }
        public DistributedCommand command() { return command; }
        public CommandResult result() { return result; }
        public long accountedBytes() { return accountedBytes; }
        private void requireReadable() {
            if (closed) throw new IllegalStateException("Staged transaction view is closed");
            parent.requireReadable();
        }
        public synchronized byte[] get(UUID column,byte[] key) throws RocksDBException {
            requireReadable(); key=Objects.requireNonNull(key).clone();
            if (!parent.scope().contains(column,key)) throw new IllegalArgumentException("Read outside declared scope");
            try (var options=new ReadOptions().setSnapshot(parent.snapshot.pin.snapshot)) {
                byte[] value=visibility.deleted(column,key) ? null : overlay.getFromBatchAndDB(db,columns.get(column),options,key);
                if (value!=null && value.length>MAX_RESULT_BYTES) throw new IllegalStateException("Staged value exceeds reply budget");
                return value;
            }
        }
        public synchronized byte[] getMember(UUID column,byte[] key,List<byte[]> members) throws RocksDBException {
            requireReadable();
            if (logicalSchema==null) throw new UnsupportedOperationException("Logical schema is not qualified");
            key=Objects.requireNonNull(key).clone(); members=members.stream().map(byte[]::clone).toList();
            logicalSchema.requireBucketKey(column,key,members);
            // Physical bucket bytes have their own budget; only the selected member is a point reply.
            if (!parent.scope().contains(column,key)) throw new IllegalArgumentException("Read outside declared scope");
            try (var options=new ReadOptions().setSnapshot(parent.snapshot.pin.snapshot)) {
                byte[] raw=visibility.deleted(column,key) ? null : overlay.getFromBatchAndDB(db,columns.get(column),options,key);
                if (raw!=null && raw.length>MAX_BUCKET_PLAN_BYTES) throw new IllegalStateException("Staged bucket exceeds byte budget");
                byte[] value=logicalSchema.decodeBucket(column,key,raw).get(members);
                if (value!=null && value.length>MAX_RESULT_BYTES) throw new IllegalStateException("Staged member exceeds reply budget");
                return value;
            }
        }
        public synchronized PhysicalRangePage page(UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws RocksDBException {
            requireReadable(); validatePageBudget(items,bytes);
            if ((long)(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length)>TransactionScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("Page bounds exceed budget");
            lower=lower==null ? interval.lowerInclusive() : lower.clone(); upper=upper==null ? interval.upperExclusive() : upper.clone();
            if (!parent.scope().containsRange(column,lower,upper)) throw new IllegalArgumentException("Page outside declared scope");
            resume=resume==null ? null : resume.clone();
            if (resume!=null && (resume.length>PhysicalRangePage.MAX_BYTES || lower!=null && Arrays.compareUnsigned(resume,lower)<0 || upper!=null && Arrays.compareUnsigned(resume,upper)>=0)) throw new IllegalArgumentException("Continuation outside page bounds");
            try (var low=lower==null ? null : new org.rocksdb.Slice(lower);
                 var high=upper==null ? null : new org.rocksdb.Slice(upper);
                 var options=new ReadOptions().setSnapshot(parent.snapshot.pin.snapshot).setFillCache(false)) {
                if (low!=null) options.setIterateLowerBound(low); if (high!=null) options.setIterateUpperBound(high);
                try (var base=databaseOwner.newIterator(columns.get(column),options);
                     var iterator=overlay.newIteratorWithBase(columns.get(column),base,options)) {
                    return readPage(iterator,lower,upper,reverse,resume,items,bytes,true,visibility,column);
                }
            }
        }
        @Override public synchronized void close() {
            if (closed) return; closed=true;
            try { overlay.close(); } finally { parent.snapshot.pin.releaseUse(); }
        }
    }

    /** Scope cannot expand; native reader ownership and close/drain semantics are retained by the pin. */
    public final class ScopedSnapshot implements AutoCloseable {
        private final TransactionScope scope;
        private final SnapshotPin pin;
        private ScopedSnapshot(TransactionScope scope, SnapshotPin pin) { this.scope = scope; this.pin = pin; }
        boolean belongsTo(ReplicatedRangeStore store) { return ReplicatedRangeStore.this == store; }
        /** Checks the existing owner without reading values or acquiring a new capture. */
        public void requireLive(){pin.acquireUse();try {} finally {pin.releaseUse();}}
        /** Closed registry entries remain charged until actual native ownership has settled. */
        public boolean releaseSettled(){return pin.releaseSettled();}
        public TransactionScope scope() { return scope; }
        public RangeOwnership.Interval interval() { return pin.interval(); }
        NativeUse retainForExport() {
            var released = new AtomicBoolean();
            NativeUse use = () -> { if (released.compareAndSet(false, true)) pin.releaseUse(); };
            pin.acquireUse();
            return use;
        }
        public ApplyPosition position() { return pin.position(); }
        public UUID groupId() { return pin.groupId(); }
        public long rangeEpoch() { return pin.rangeEpoch(); }
        public byte[] schemaDigest() { return pin.schemaDigest(); }
        public byte[] get(UUID columnId, byte[] key) throws RocksDBException {
            byte[] physicalKey = Objects.requireNonNull(key).clone();
            if (!scope.contains(columnId, physicalKey)) throw new IllegalArgumentException("Read outside declared scope");
            return pin.get(columnId, physicalKey);
        }
        /** Null bounds request the entire owned interval, rather than silently clipping to the declared scope. */
        public void visitColumn(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive, SnapshotVisitor visitor)
                throws RocksDBException, IOException {
            byte[] lower = lowerInclusive == null ? pin.interval().lowerInclusive() : lowerInclusive.clone();
            byte[] upper = upperExclusive == null ? pin.interval().upperExclusive() : upperExclusive.clone();
            if (!scope.containsRange(columnId, lower, upper)) throw new IllegalArgumentException("Scan outside declared scope");
            pin.visitColumn(columnId, lower, upper, Objects.requireNonNull(visitor));
        }
        /** Exclusive full-physical-key continuation, in either direction, at this retained cut. */
        public PhysicalRangePage page(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive,
                                      boolean reverse, byte[] resumeAfter, int itemLimit, int byteLimit) throws RocksDBException {
            validatePageBudget(itemLimit, byteLimit);
            if ((long)(lowerInclusive == null ? 0 : lowerInclusive.length) + (upperExclusive == null ? 0 : upperExclusive.length) > TransactionScope.MAX_BOUND_BYTES
                    || resumeAfter != null && resumeAfter.length > PhysicalRangePage.MAX_BYTES) {
                throw new IllegalArgumentException("Page request exceeds bound budget");
            }
            byte[] lower = lowerInclusive == null ? pin.interval().lowerInclusive() : lowerInclusive.clone();
            byte[] upper = upperExclusive == null ? pin.interval().upperExclusive() : upperExclusive.clone();
            if (!scope.containsRange(columnId, lower, upper)) throw new IllegalArgumentException("Page outside declared scope");
            byte[] resume = resumeAfter == null ? null : resumeAfter.clone();
            if (resume != null && (resume.length > PhysicalRangePage.MAX_BYTES
                    || lower != null && Arrays.compareUnsigned(resume, lower) < 0
                    || upper != null && Arrays.compareUnsigned(resume, upper) >= 0)) {
                throw new IllegalArgumentException("Continuation outside page bounds");
            }
            return pin.page(columnId, lower, upper, reverse, resume, itemLimit, byteLimit);
        }
        /** Bounds/direction remain fixed; this cursor borrows its parent snapshot and never reopens at latest. */
        public PageCursor openCursor(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive, boolean reverse) {
            if ((long)(lowerInclusive == null ? 0 : lowerInclusive.length) + (upperExclusive == null ? 0 : upperExclusive.length) > TransactionScope.MAX_BOUND_BYTES) {
                throw new IllegalArgumentException("Cursor bounds exceed budget");
            }
            byte[] lower = lowerInclusive == null ? pin.interval().lowerInclusive() : lowerInclusive.clone();
            byte[] upper = upperExclusive == null ? pin.interval().upperExclusive() : upperExclusive.clone();
            if (!scope.containsRange(columnId, lower, upper)) throw new IllegalArgumentException("Cursor outside declared scope");
            pin.requireOpen();
            return new PageCursor(columnId, lower, upper, reverse);
        }

        public final class PageCursor implements AutoCloseable {
            private final UUID columnId;
            private final byte[] lower, upper;
            private final boolean reverse;
            private final AtomicBoolean cursorClosed = new AtomicBoolean();
            private byte[] resume;
            private boolean exhausted;
            private PageCursor(UUID columnId, byte[] lower, byte[] upper, boolean reverse) {
                this.columnId = columnId; this.lower = lower; this.upper = upper; this.reverse = reverse;
            }
            /** Calls serialize continuation changes; closing allows an already-started page to finish. */
            public synchronized PhysicalRangePage nextPage(int itemLimit, int byteLimit) throws RocksDBException {
                if (cursorClosed.get()) throw new IllegalStateException("Page cursor closed");
                validatePageBudget(itemLimit, byteLimit);
                pin.requireOpen();
                if (exhausted) return new PhysicalRangePage(List.of(), false);
                var result = pin.page(columnId, lower, upper, reverse, resume, itemLimit, byteLimit);
                resume = result.resumeAfter();
                exhausted = !result.hasMore();
                return result;
            }
            /** Stops this borrowed cursor; the parent snapshot continues to own its native pin. */
            @Override public void close() { cursorClosed.set(true); }
        }
        @Override public void close() { pin.close(); }
    }

    private static void validatePageBudget(int itemLimit, int byteLimit) {
        if (itemLimit < 1 || itemLimit > PhysicalRangePage.MAX_ITEMS
                || byteLimit < PhysicalRangePage.HEADER_BYTES || byteLimit > PhysicalRangePage.MAX_BYTES) {
            throw new IllegalArgumentException("Invalid page budget");
        }
    }

    private SnapshotPin captureUnderAdmission() throws RocksDBException { return captureUnderAdmission(false); }
    SnapshotPin captureForImage() throws RocksDBException { if(topologyBirthPending() || topologyRetired() || topologyRecoveryOnly()) throw new IllegalStateException("Inactive topology owner cannot export USER image"); return captureForRecovery(); }
    /**
     * Trusted Raft metadata recovery and image ownership only, never fresh DATA admission.
     * Bypasses the schema business fence while retaining physical activation, completed-marker
     * validation and normal native snapshot quota/drain accounting. The caller owns the pin
     * through all native reads and file I/O and must release it afterward.
     */
    public SnapshotPin captureForRecovery() throws RocksDBException {
        enterNative(); try { return captureUnderAdmission(true); } finally { leaveNative(); }
    }
    /** Sealed source metadata from its own completed cut, with no borrowed USER handles. */
    public SnapshotPin captureMetadataForRecovery()throws RocksDBException {
        enterNative();try{return captureUnderAdmission(true,true);}finally{leaveNative();}
    }
    private SnapshotPin captureUnderAdmission(boolean recovery) throws RocksDBException {
        return captureUnderAdmission(recovery,false);
    }
    private SnapshotPin captureUnderAdmission(boolean recovery,boolean metadata) throws RocksDBException {
        if(metadata)requireTopologyMetadataOwner();else {if (!recovery) { requireServing(); requireSchemaAdmission(); } if(!recovery || topologyLearner==null && currentRangeRecovery==null)requireActive();}
        applyLane.lock();
        try {
            if(metadata)requireTopologyMetadataOwner();else {if(!recovery) { requireServing(); requireSchemaAdmission(); } if(recovery && (topologyLearner!=null || currentRangeRecovery!=null))requireTopologyLearnerImage();else requireActive();}
            if(schemaCaptureGate!=null) throw new IllegalStateException("INSTALL preparation blocks new native captures");
            ensureReceiptUsage();
            PoolSnapshotAdmission.Lease lease = snapshotAdmission == null ? null : snapshotAdmission.reserve();
            Snapshot snapshot;
            try { snapshot = Objects.requireNonNull(db.getSnapshot(), "Native snapshot creation failed"); }
            catch (Throwable failure) { if (lease != null) lease.close(); throw failure; }
            try {
                ApplyPosition cut;
                try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                    byte[] marker = db.get(state, options, markerKey);
                    cut = marker == null ? null : decodePosition(marker);
                    if (!Objects.equals(cut, applied)) throw new IllegalStateException("Snapshot marker differs from completed apply");
                }
                var pin = new SnapshotPin(snapshot, cut, lease,null,metadata);
                snapshotPins.incrementAndGet();
                return pin;
            } catch (Throwable failure) {
                try { db.releaseSnapshot(snapshot); }
                catch (Throwable releaseFailure) {
                    recordSnapshotReleaseFailure(releaseFailure);
                    // No published pin exists, but the uncertain native owner must still block pool close.
                    snapshotPins.incrementAndGet();
                    if (releaseFailure != failure) releaseFailure.addSuppressed(failure);
                    throw releaseFailure;
                }
                if (lease != null) lease.close();
                throw failure;
            }
        } finally {
            applyLane.unlock();
        }
    }

    private void enterNative() {
        requireSnapshotReleaseHealthy();
        if (closing) throw new IllegalStateException("Range admission is closed");
        nativeLifecycle.readLock().lock();
        try { requireSnapshotReleaseHealthy(); }
        catch (RuntimeException failure) { nativeLifecycle.readLock().unlock(); throw failure; }
        if (closing) {
            nativeLifecycle.readLock().unlock();
            throw new IllegalStateException("Range admission is closed");
        }
    }

    private void leaveNative() { nativeLifecycle.readLock().unlock(); signalSchemaCaptureDrain(); signalTopologyDrain(); }

    /** Called under pool mutation ownership; never closes shared CFs or busy snapshots. */
    boolean detachRetiredUserHandles() throws RocksDBException {
        requireSnapshotReleaseHealthy();if(!nativeLifecycle.writeLock().tryLock())return false;
        try { applyLane.lock();try {
            if(closing)throw new IllegalStateException("Retired range is closing");requireTopologyMetadataOwner();
            if(!topologyRetired() || ownership.get(owner).state()!=RangeOwnership.State.RETIRED)throw new IllegalStateException("Only durable retired topology owners can detach USER handles");
            if(snapshotPins.get()!=0)return false;
            if(retiredMetadataOnly)return true;
            columns=Map.of();retiredMetadataOnly=true;
            if(installedGeneration!=null)installedGeneration=new SchemaGeneration(installedGeneration.schema(),interval,columns,deterministicMergeColumns);
            return true;
        } finally { applyLane.unlock(); } } finally { nativeLifecycle.writeLock().unlock(); }
    }

    /**
     * Permanently closes borrowed native admission. Pool ownership stays intact
     * until true is returned. Existing pins retain their exact cut and may finish;
     * this method never closes their database or forcibly releases a native user.
     */
    public boolean close(Duration timeout) throws InterruptedException {
        requireSnapshotReleaseHealthy();
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative close timeout");
        long nanos = timeout.toNanos();
        closing = true;
        if (!nativeLifecycle.writeLock().tryLock(nanos, TimeUnit.NANOSECONDS)) return false;
        try {
            fence();
            return snapshotPins.get() == 0;
        } finally { nativeLifecycle.writeLock().unlock(); }
    }

    private void recordSnapshotReleaseFailure(Throwable failure) {
        snapshotReleaseFailure = failure;
        if (snapshotAdmission != null) snapshotAdmission.failRelease(failure);
    }

    private void requireSnapshotReleaseHealthy() {
        Throwable failure = snapshotReleaseFailure;
        if (failure == null && snapshotAdmission != null) failure = snapshotAdmission.releaseFailure();
        if (failure != null) throw new IllegalStateException("Native snapshot release outcome unknown; pool retained", failure);
    }

    public final class SnapshotPin implements AutoCloseable {
        private final Snapshot snapshot;
        private final ApplyPosition position;
        private final PoolSnapshotAdmission.Lease admissionLease;
        private final byte[] capturedSchemaDigest;
        private final RangeOwnership.Interval capturedInterval;
        private final Map<UUID,ColumnFamilyHandle> capturedHandles;
        private final PhysicalSnapshotOwner physicalOwner;
        private final Set<UUID> capturedColumns,capturedMergeColumns;
        private final boolean metadataOnly;
        private boolean closed;
        private int nativeUsers;
        private volatile boolean nativeReleaseSucceeded;
        private long historyScopeBytes; private volatile boolean historyTracked;
        private SnapshotPin(Snapshot snapshot, ApplyPosition position, PoolSnapshotAdmission.Lease lease,PhysicalSnapshotOwner physicalOwner,boolean metadataOnly) {
            this.snapshot = snapshot;
            this.position = position;
            this.admissionLease = lease;this.physicalOwner=physicalOwner;this.metadataOnly=metadataOnly;capturedHandles=metadataOnly?Map.of():Map.copyOf(columns);
            capturedSchemaDigest=schemaDigest.clone(); capturedInterval=interval; capturedColumns=Set.copyOf(metadataOnly?logicalColumns():columns.keySet()); capturedMergeColumns=Set.copyOf(deterministicMergeColumns);
        }
        private boolean belongsTo(ReplicatedRangeStore store){return ReplicatedRangeStore.this==store;}
        private boolean capturedContains(byte[] key){return (capturedInterval.lowerInclusive()==null || Arrays.compareUnsigned(key,capturedInterval.lowerInclusive())>=0) && (capturedInterval.upperExclusive()==null || Arrays.compareUnsigned(key,capturedInterval.upperExclusive())<0);}
        public boolean releaseSettled(){synchronized(this){if(!closed || nativeUsers!=0)return false;}return physicalOwner==null ? nativeReleaseSucceeded : physicalOwner.releaseSucceeded;}
        public ApplyPosition position() { return position; }
        boolean metadataOnly(){return metadataOnly;}
        public UUID groupId() { return groupId; }
        public long rangeEpoch() { return rangeEpoch; }
        public byte[] schemaDigest() { return capturedSchemaDigest.clone(); }
        public Set<UUID> columns() { return capturedColumns; }
        public Set<UUID> deterministicMergeColumns() { return capturedMergeColumns; }
        public RangeOwnership.Interval interval() { return capturedInterval; }
        /** Immutable identity and authority from this same completed native snapshot cut; no user keys are accessed. */
        public RangeStateView stateView() throws RocksDBException {
            acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) {
                return new RangeStateView(groupId,rangeEpoch,capturedInterval.domainId(),capturedSchemaDigest,capturedColumns,capturedInterval.lowerInclusive(),capturedInterval.upperExclusive(),position,minimumSessionEpoch(options),sessionAuthority(options));
            } finally { releaseUse(); }
        }
        /** Size-only native reads precede every registration allocation in image export. */
        int recoveryRegistrationBytes(NativeRangeImage.Limits limits)throws RocksDBException,IOException {
            if(metadataOnly)return 0;
            acquireUse();
            try(var options=new ReadOptions().setSnapshot(snapshot)) { return ReplicatedRangeStore.this.recoveryRegistrationBytes(options,limits); }
            finally { releaseUse(); }
        }
        /** Local topology evidence is read from this owned native cut, never the live store cache. */
        public NativeTopologyImageOrigin topologyImageOrigin() throws RocksDBException {
            if(metadataOnly)return null;
            acquireUse();
            try(var options=new ReadOptions().setSnapshot(snapshot)) {
                byte[] retired=db.get(state,options,topologyOwnershipKey(0x62));if(retired!=null)throw new IllegalStateException("Retired topology parent cannot export USER image");
                byte[] current=db.get(state,options,topologyOwnershipKey(0x72));if(current!=null){var registration=NativeCurrentRangeRecovery.decode(current);if(db.get(state,options,topologyOwnershipKey(0x73))==null || !Arrays.equals(db.get(state,options,topologyOwnershipKey(0x74)),ByteBuffer.allocate(48).putLong(registration.imageCut().term()).putLong(registration.imageCut().index()).put(registration.imageHash()).array()))throw new IllegalStateException("CURRENT snapshot lacks actual completed recovery");if(registration.origin()==null)return null;var origin=new NativeTopologyImageOrigin(registration.origin().seed(),registration.origin().activation(),registration.digest(),snapshotSchemaAdoptionAnchor(options));origin.requireImage(groupId,rangeEpoch,capturedInterval.domainId(),capturedInterval.lowerInclusive(),capturedInterval.upperExclusive(),position);return origin;}
                byte[] learner=db.get(state,options,topologyOwnershipKey(0x67));if(learner!=null){var registration=NativeTopologyLearnerRegistration.decode(learner);byte[] activation=db.get(state,options,topologyOwnershipKey(0x63));if(registration.origin()==null){if(activation!=null || db.get(state,options,topologyOwnershipKey(0x61))!=null)throw new IllegalStateException("Ordinary learner snapshot acquired birth provenance");return null;}if(activation==null || !Arrays.equals(activation,registration.origin().activation().encode()))throw new IllegalStateException("Learner snapshot lost original activation");var origin=new NativeTopologyImageOrigin(registration.origin().seed(),registration.origin().activation(),registration.digest(),snapshotSchemaAdoptionAnchor(options));origin.requireImage(groupId,rangeEpoch,capturedInterval.domainId(),capturedInterval.lowerInclusive(),capturedInterval.upperExclusive(),position);return origin;}
                byte[] birth=db.get(state,options,topologyOwnershipKey(0x61)),activation=db.get(state,options,topologyOwnershipKey(0x63));
                if(birth==null && activation==null)return null;
                if(birth==null || activation==null || schemaPreparationPool==null)throw new IllegalStateException("Topology image lacks completed local birth/activation/original seed");
                var pointer=NativeTopologyBirth.decode(birth);var seed=schemaPreparationPool.topologySeed(pointer.intentId());if(seed==null)throw new IllegalStateException("Topology image original seed missing");pointer.requireSeed(seed);
                var receipt=NativeTopologyActivation.decode(activation);if(!pointer.equals(receipt.birth()))throw new IllegalArgumentException("Topology image birth/activation differ");var origin=new NativeTopologyImageOrigin(seed,receipt);origin.requireImage(groupId,rangeEpoch,capturedInterval.domainId(),capturedInterval.lowerInclusive(),capturedInterval.upperExclusive(),position);return origin;
            } finally { releaseUse(); }
        }
        private byte[] snapshotSchemaAdoptionAnchor(ReadOptions options)throws RocksDBException {
            NativeSchemaInstallation first=null;long bytes=0;int count=0;byte[] prefix=schemaStateKey(0x2a);
            try(var iterator=databaseOwner.newIterator(state,options)){iterator.seek(prefix);while(iterator.isValid()){byte[] key=iterator.key();if(key.length<17 || !Arrays.equals(key,0,17,prefix,0,17))break;int size=iterator.value(new byte[0]);if(key.length!=33 || ++count>64 || size>NativeSchemaInstallation.MAX_BYTES || (bytes+=size)>NativeTopologyLearnerSchema.MAX_BYTES)throw new IllegalStateException("Snapshot schema anchor exceeds retained history bound");var record=NativeSchemaInstallation.decode(iterator.value());record.requireRange(groupId,rangeEpoch,capturedInterval.domainId(),capturedInterval.lowerInclusive(),capturedInterval.upperExclusive());record.requireCut(position);if(first==null || record.position().index()<first.position().index())first=record;iterator.next();}iterator.status();}
            return first==null?null:it.cavallium.rockserver.core.common.distributed.RangeImportAdmission.sha256(first.encode());
        }
        public Configuration configuration() throws RocksDBException {
            acquireUse();
            try (var options = new ReadOptions().setSnapshot(snapshot)) {
                byte[] value = db.get(state, options, configurationKey);
                return value == null ? null : decodeConfiguration(value);
            } finally { releaseUse(); }
        }
        public byte[] get(UUID columnId, byte[] key) throws RocksDBException {
            acquireUse();
            try {
                var cf = capturedHandles.get(columnId);
                if (cf == null) throw new IllegalArgumentException("Unknown logical column");
                if (!capturedContains(key)) throw new IllegalArgumentException("Read outside owned interval");
                try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                    return db.get(cf, options, key);
                }
            } finally {
                releaseUse();
            }
        }
        public ParticipantFenceProof participantFence(ParticipantIdentity identity) throws RocksDBException {
            Objects.requireNonNull(identity); acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) { return readParticipantFence(identity,options,position); }
            finally { releaseUse(); }
        }
        public ParticipantFinalizationResult finalizationResult(ParticipantFinalization expected) throws RocksDBException {
            Objects.requireNonNull(expected); acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) { return readFinalizationResult(expected,options,position); }
            finally { releaseUse(); }
        }
        void validateHomeCreditImage(Map<UUID,HomeRecoveryCredit> credits) throws RocksDBException {
            var context=homeRecoveryContext;
            if (context==null && !credits.isEmpty()) { var credit=credits.values().iterator().next(); context=new HomeRecoveryContext(credit.cluster(),credit.database(),credit.column()); }
            acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) {
                for (var credit:credits.values()) {
                    var column=capturedHandles.get(credit.column()); if (column==null) throw new IllegalStateException("HOME promise column differs from snapshot owner");
                    byte[] user=db.get(column,options,uuidBytes(credit.current().transactionId()));
                    if (user==null || !credit.current().equals(TransactionStatus.decode(user))) throw new IllegalStateException("HOME image promise has no exact USER row");
                }
                validateHomeUserCredits(credits,context,snapshot);
            } finally { releaseUse(); }
        }
        void validateSessionMetadataImage() throws RocksDBException {acquireUse();try{validateSessionMetadata(snapshot);}finally{releaseUse();}}
        void validateCatalogueHistoryImage() throws RocksDBException {if(catalogueHistoryColumn==null)return;acquireUse();try {verifyCatalogueHistory(snapshot);}finally {releaseUse();}}
        void validateSnapshotHomeImage() throws RocksDBException { acquireUse(); try { validateSnapshotHomeUsers(snapshot,snapshotHomeContext); } finally { releaseUse(); } }
        public SnapshotHomeFenceProof snapshotHomeFence(SnapshotHomeIdentity identity) throws RocksDBException {
            acquireUse(); try (var options=new ReadOptions().setSnapshot(snapshot)) { return readSnapshotHomeFence(identity,options,position); } finally { releaseUse(); }
        }
        public boolean snapshotHomeRetired(SnapshotHomeRetirement retirement) throws RocksDBException { return snapshotHomeFence(retirement.identity())!=null; }
        public HomeFenceProof homeFence(HomeIdentity identity) throws RocksDBException {
            Objects.requireNonNull(identity); acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) { return readHomeFence(identity,options,position); } finally { releaseUse(); }
        }
        public boolean homeRetired(HomeRetirement retirement) throws RocksDBException {
            Objects.requireNonNull(retirement); acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) { return readHomeRetired(retirement,options,position); } finally { releaseUse(); }
        }
        void validateHomeRecoveryImage(Map<OperationId,HomeRecoveryReceipt> records) throws RocksDBException {
            acquireUse(); try (var options=new ReadOptions().setSnapshot(snapshot)) { for (var receipt:records.values()) requireHomeSuccessor(receipt,options); } finally { releaseUse(); }
        }
        public HomeRecoveryResult homeRecoveryResult(HomeRecovery recovery) throws RocksDBException {
            Objects.requireNonNull(recovery); acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) { return readHomeRecovery(recovery,options,position); } finally { releaseUse(); }
        }
        public SnapshotGateResult snapshotGateResult(SnapshotFence fence) throws RocksDBException {
            Objects.requireNonNull(fence); acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) { return readSnapshotGateResult(fence,options,position); } finally { releaseUse(); }
        }
        public SnapshotFenceProof snapshotFence(SnapshotFence fence) throws RocksDBException {
            var result=snapshotGateResult(fence); return result!=null && result.retired() ? result.retirement() : null;
        }
        public SnapshotGate snapshotGate(SnapshotFence fence) throws RocksDBException {
            Objects.requireNonNull(fence); acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) { return readSnapshotGate(fence,options,position); }
            finally { releaseUse(); }
        }
        public boolean finalizedPreparation(PreparedParticipant.Preparation expected) throws RocksDBException {
            Objects.requireNonNull(expected); acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) {
                byte[] commit=db.get(state,options,committedKey(expected.transactionId())),abort=db.get(state,options,abortedKey(expected.transactionId()));
                if (commit!=null && abort!=null) throw new IllegalArgumentException("Conflicting native finalization decisions");
                if (commit==null && abort==null) return false;
                ParticipantFinalization finalization; ApplyPosition finalized;
                if (commit!=null) { var receipt=CommittedParticipant.decode(commit); finalization=receipt.finalization(); finalized=receipt.position(); }
                else { var receipt=AbortedParticipant.decode(abort); finalization=receipt.finalization(); finalized=receipt.position(); }
                requireFinalization(finalization); finalization.requirePreparation(expected);
                if (position==null || finalized.index()>position.index() || finalized.term()>position.term()) throw new IllegalArgumentException("Terminal preparation proof differs from snapshot cut"); return true;
            } finally { releaseUse(); }
        }
        public ParticipantVote preparationVote(PreparedParticipant.Preparation expected) throws RocksDBException {
            acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) { var retirement=readParticipantFence(identity(expected),options,position); if (retirement!=null) return ParticipantVote.retired(retirement); }
            finally { releaseUse(); }
            var prepared=preparedParticipant(expected.transactionId()); if (prepared!=null) { if (!prepared.preparation().equals(expected)) throw new IllegalArgumentException("Prepared snapshot payload changed"); return ParticipantVote.prepared(prepared.position()); }
            acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) {
                byte[] bytes=db.get(state,options,rejectedKey(expected.transactionId())); if (bytes==null) return null; var rejected=RejectedParticipant.decode(bytes);
                requireRejected(rejected); rejected.requirePreparation(expected);
                if (position==null || rejected.position().index()>position.index() || rejected.position().term()>position.term()) throw new IllegalStateException("Negative receipt differs from snapshot cut"); return ParticipantVote.negative();
            } finally { releaseUse(); }
        }
        public PreparedParticipant preparedParticipant(UUID transaction) throws RocksDBException {
            acquireUse();
            try (var options=new ReadOptions().setSnapshot(snapshot)) {
                byte[] bytes=db.get(state,options,preparedKey(transaction)); if (bytes==null) return null;
                var record=PreparedParticipant.decode(bytes);
                if (!record.preparation().transactionId().equals(transaction) || position==null || record.position().index()>position.index() || record.position().term()>position.term() || record.position().index()==position.index() && record.position().term()!=position.term()) throw new IllegalStateException("Prepared receipt differs from snapshot cut");
                return record;
            } finally { releaseUse(); }
        }
        public byte[] operationResult(OperationId id, byte[] payloadDigest) throws RocksDBException {
            acquireUse();
            try {
                try (ReadOptions options = new ReadOptions().setSnapshot(snapshot)) {
                    if (payloadDigest.length != 32) throw new IllegalArgumentException("Invalid payload digest");
                    if (db.get(state,options,homeRecoveryKey(id))!=null) throw new IllegalArgumentException("Operation identity belongs to protected HOME recovery");
                    if (id.epoch() < minimumSessionEpoch(options) || id.sequence() <= acknowledgedThrough(id, options)) return expiredResult();
                    byte[] saved = db.get(receipts, options, receiptKey(id));
                    return saved == null ? null : decodeReceipt(saved, payloadDigest);
                }
            } finally {
                releaseUse();
            }
        }
        /** Visits copied key/value rows from this exact native cut in physical comparator order. */
        public void visitColumn(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive, SnapshotVisitor visitor)
                throws RocksDBException, IOException {
            var cf = capturedHandles.get(columnId);
            if (cf == null) throw new IllegalArgumentException("Unknown logical column");
            byte[] lower = lowerInclusive == null ? capturedInterval.lowerInclusive() : lowerInclusive.clone();
            byte[] upper = upperExclusive == null ? capturedInterval.upperExclusive() : upperExclusive.clone();
            if ((capturedInterval.lowerInclusive() != null && (lower == null || Arrays.compareUnsigned(lower, capturedInterval.lowerInclusive()) < 0))
                    || (capturedInterval.upperExclusive() != null && (upper == null || Arrays.compareUnsigned(upper, capturedInterval.upperExclusive()) > 0))) {
                throw new IllegalArgumentException("Export outside owned interval");
            }
            visit(cf, lower, upper, null, visitor);
        }
        private PhysicalRangePage page(UUID columnId, byte[] lower, byte[] upper, boolean reverse,
                                       byte[] resume, int itemLimit, int byteLimit) throws RocksDBException {
            var cf = capturedHandles.get(columnId);
            if (cf == null) throw new IllegalArgumentException("Unknown logical column");
            acquireUse();
            try (var lowerBound = lower == null ? null : new org.rocksdb.Slice(lower);
                    var upperBound = upper == null ? null : new org.rocksdb.Slice(upper);
                    var options = new ReadOptions().setSnapshot(snapshot).setFillCache(false)) {
                if (lowerBound != null) options.setIterateLowerBound(lowerBound);
                if (upperBound != null) options.setIterateUpperBound(upperBound);
                try (var iterator = databaseOwner.newIterator(cf, options)) {
                    return readPage(iterator,lower,upper,reverse,resume,itemLimit,byteLimit,false,null,null);
                }
            } finally { releaseUse(); }
        }
        public void visitState(SnapshotVisitor visitor) throws RocksDBException, IOException {
            acquireUse();
            try (var options = new ReadOptions().setSnapshot(snapshot)) {
                long minimum = minimumSessionEpoch(options);
                visit(state, markerKey, null, markerKey, (key, value) -> {
                    if (key.length == 41 && key[16] == 2) {
                        long epoch = ByteBuffer.wrap(key,17,8).getLong();
                        if (epoch >= 0 && epoch < minimum) return;
                    }
                    visitor.accept(key,value);
                });
            } finally { releaseUse(); }
        }
        public void visitReceipts(SnapshotVisitor visitor) throws RocksDBException, IOException {
            acquireUse();
            try (var options = new ReadOptions().setSnapshot(snapshot)) {
                long minimum = minimumSessionEpoch(options);
                visit(receipts, markerKey, null, markerKey, (key, value) -> {
                    if (key.length == 48) {
                        long epoch = ByteBuffer.wrap(key,16,8).getLong();
                        if (epoch >= 0 && epoch < minimum) return;
                    }
                    visitor.accept(key,value);
                });
            } finally { releaseUse(); }
        }
        private void visit(ColumnFamilyHandle cf, byte[] lower, byte[] upper, byte[] prefix, SnapshotVisitor visitor)
                throws RocksDBException, IOException {
            if (lower != null && upper != null && Arrays.compareUnsigned(lower, upper) >= 0) {
                throw new IllegalArgumentException("Invalid export interval");
            }
            acquireUse();
            try (var options = new ReadOptions().setSnapshot(snapshot).setFillCache(false);
                    var iterator = databaseOwner.newIterator(cf, options)) {
                if (lower == null) iterator.seekToFirst(); else iterator.seek(lower);
                while (iterator.isValid()) {
                    byte[] key = iterator.key();
                    if (upper != null && Arrays.compareUnsigned(key, upper) >= 0) break;
                    if (prefix != null && (key.length < prefix.length
                            || !Arrays.equals(prefix, 0, prefix.length, key, 0, prefix.length))) break;
                    visitor.accept(key, iterator.value());
                    iterator.next();
                }
                iterator.status();
            } finally {
                releaseUse();
            }
        }
        private synchronized void requireOpen() {
            requireSnapshotReleaseHealthy();
            if (closed) throw new IllegalStateException("Snapshot pin closed");
            if (admissionLease != null) admissionLease.checkAge();
        }
        private void acquireUse() {
            synchronized (this) {
                requireOpen();
                nativeUsers++;
            }
            // The borrowed use keeps DB/CF owners live while pressure metrics are sampled.
            try { if (admissionLease != null) admissionLease.checkUse(); }
            catch (Throwable failure) { releaseUse(); throw failure; }
        }
        private void releaseUse() {
            boolean release;
            synchronized (this) { release = --nativeUsers == 0 && closed; }
            if (release) releaseNative();
        }
        @Override public void close() {
            boolean release;
            synchronized (this) {
                if (closed) { requireSnapshotReleaseHealthy(); return; }
                closed = true;
                release = nativeUsers == 0;
            }
            if (release) releaseNative();
        }
        private void releaseNative() {
            if(physicalOwner!=null){physicalOwner.dropAlias();return;}
            try { db.releaseSnapshot(snapshot); }
            catch (Throwable failure) { recordSnapshotReleaseFailure(failure); throw failure; }
            if (admissionLease != null) admissionLease.close();
            if (historyTracked) { trackedSnapshotScopeBytes.addAndGet(-historyScopeBytes); trackedSnapshotPins.decrementAndGet(); }
            snapshotPins.decrementAndGet(); signalSchemaCaptureDrain(); signalTopologyDrain();nativeReleaseSucceeded=true;
        }
    }

    private static boolean withinPageBounds(org.rocksdb.RocksIterator iterator,byte[] lower,byte[] upper) {
        if (!iterator.isValid()) return false;
        byte[] key=iterator.key();
        return (lower==null || Arrays.compareUnsigned(key,lower)>=0) && (upper==null || Arrays.compareUnsigned(key,upper)<0);
    }
    private static PhysicalRangePage readPage(org.rocksdb.RocksIterator iterator,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int itemLimit,int byteLimit,boolean checkBounds,RangeOverlay visibility,UUID column) throws RocksDBException {
        byte[] sizingBuffer = new byte[0];
        if (resume != null) {
            if (reverse) iterator.seekForPrev(resume); else iterator.seek(resume);
            if (iterator.isValid() && iterator.key(sizingBuffer) == resume.length && Arrays.equals(iterator.key(), resume)) {
                if (reverse) iterator.prev(); else iterator.next();
            }
        } else if (reverse && upper!=null) {
            iterator.seekForPrev(upper);
            if (iterator.isValid() && Arrays.equals(iterator.key(),upper)) iterator.prev();
        } else if (reverse) iterator.seekToLast();
        else if (lower == null) iterator.seekToFirst(); else iterator.seek(lower);
        if (visibility!=null) visibility.skipDeleted(iterator,column,reverse,lower,upper);
        var rows = new ArrayList<PhysicalRangePage.Row>();
        int bytes = PhysicalRangePage.HEADER_BYTES, lastKeyBytes = 0;
        while (iterator.isValid() && (!checkBounds || withinPageBounds(iterator,lower,upper)) && rows.size() < itemLimit) {
            int keyBytes = iterator.key(sizingBuffer), valueBytes = iterator.value(sizingBuffer);
            if (keyBytes < 0 || valueBytes < 0) throw new IllegalStateException("Invalid native row length");
            long required = (long)bytes - lastKeyBytes + 8L + 2L * keyBytes + valueBytes;
            if (required > byteLimit) {
                iterator.status();
                if (rows.isEmpty()) throw new PhysicalRangePage.LimitExceededException(required);
                break;
            }
            rows.add(new PhysicalRangePage.Row(iterator.key(), iterator.value()));
            bytes = (int)required; lastKeyBytes = keyBytes;
            if (reverse) iterator.prev(); else iterator.next();
            if (visibility!=null) visibility.skipDeleted(iterator,column,reverse,lower,upper);
        }
        iterator.status();
        return new PhysicalRangePage(rows, iterator.isValid() && (!checkBounds || withinPageBounds(iterator,lower,upper)));
    }

    @FunctionalInterface
    interface NativeUse extends AutoCloseable {
        @Override void close();
    }

    @FunctionalInterface
    public interface SnapshotVisitor {
        void accept(byte[] key, byte[] value) throws IOException;
    }

    private void requireNext(ApplyPosition next) {
        long expected = applied == null ? 0 : Math.addExact(applied.index(), 1);
        if (next.index() != expected || (applied != null && next.term() < applied.term())) {
            throw new IllegalStateException("Noncontiguous or regressing committed apply: " + next);
        }
    }

    /** Immutable local progress; no native pin/iterator survives between reclamation pages. */
    public final class ReclamationCursor {
        private final long cutoff, generation;
        private final int phase;
        private final byte[] after;
        private ReclamationCursor(long cutoff, long generation, int phase, byte[] after) {
            this.cutoff = cutoff; this.generation = generation; this.phase = phase;
            this.after = after == null ? null : after.clone();
        }
        public long minimumEpoch() { return cutoff; }
        private boolean belongsTo(ReplicatedRangeStore store) { return ReplicatedRangeStore.this == store; }
    }
    public record ReclamationPage(long minimumEpoch, ReclamationCursor continuation,
                                  int examined, int deleted, long scannedBytes) {}

    /** Physical garbage collection after a committed epoch fence; never advances A or changes user data. */
    public ReclamationPage reclaimExpiredSessions(ReclamationCursor cursor, int rowLimit, int byteLimit) throws RocksDBException {
        if (rowLimit < 1 || rowLimit > 4096 || byteLimit < 1 || byteLimit > 8 * 1024 * 1024) {
            throw new IllegalArgumentException("Invalid session reclamation budget");
        }
        enterNative();
        applyLane.lock();
        try {
            requireActive();
            long minimum = minimumSessionEpoch(null);
            if (cursor != null && (!cursor.belongsTo(this) || cursor.generation != installationGeneration || cursor.cutoff > minimum)) {
                throw new IllegalArgumentException("Session reclamation cursor lost its floor or owner");
            }
            long cutoff = cursor == null ? minimum : cursor.cutoff;
            long generation = installationGeneration;
            if (cutoff == 0) return new ReclamationPage(0,null,0,0,0);
            int phase = cursor == null ? 0 : cursor.phase;
            byte[] after = cursor == null ? null : cursor.after;
            // Persist the fence before any local deletion can become durable without it.
            if (cursor == null) db.flushWal(true);
            int examined = 0, deleted = 0;
            long bytes = 0;
            try (var batch = new WriteBatch(); var write = new WriteOptions().setDisableWAL(false).setSync(false)) {
                while (phase < 2 && examined < rowLimit) {
                    byte[] prefix = phase == 0 ? ByteBuffer.allocate(17).put(markerKey).put((byte)2).array() : markerKey;
                    byte[] end = prefixEnd(prefix);
                    var cf = phase == 0 ? state : receipts;
                    try (var lower = new org.rocksdb.Slice(prefix); var upper = end == null ? null : new org.rocksdb.Slice(end);
                            var options = new ReadOptions().setFillCache(false).setIterateLowerBound(lower)) {
                        if (upper != null) options.setIterateUpperBound(upper);
                        try (var iterator = databaseOwner.newIterator(cf,options)) {
                            byte[] sizing = new byte[0];
                            if (after == null) iterator.seek(prefix);
                            else {
                                iterator.seek(after);
                                if (iterator.isValid() && iterator.key(sizing) == after.length && Arrays.equals(iterator.key(),after)) iterator.next();
                            }
                            while (iterator.isValid() && examined < rowLimit) {
                                int keyBytes = iterator.key(sizing), valueBytes = iterator.value(sizing);
                                if (keyBytes != (phase == 0 ? 41 : 48) || valueBytes < 0) {
                                    throw new IllegalStateException("Invalid session reclamation metadata shape");
                                }
                                long needed = (long)keyBytes + valueBytes;
                                if (needed > byteLimit - bytes) {
                                    if (examined == 0) throw new IllegalArgumentException("Session reclamation needs at least " + needed + " bytes");
                                    break;
                                }
                                byte[] key = iterator.key();
                                long epoch = ByteBuffer.wrap(key,phase == 0 ? 17 : 16,8).getLong();
                                if (epoch < 0) throw new IllegalStateException("Invalid reclamation session epoch");
                                if (epoch < cutoff) { batch.delete(cf,key); deleted++; }
                                examined++; bytes += needed; after = key;
                                iterator.next();
                            }
                            iterator.status();
                            if (iterator.isValid()) break; // row/byte budget exhausted; continue this phase later
                        }
                    }
                    phase++; after = null;
                }
                if (deleted != 0) db.write(write,batch);
            }
            return new ReclamationPage(cutoff,phase == 2 ? null : new ReclamationCursor(cutoff,generation,phase,after),
                    examined,deleted,bytes);
        } finally { applyLane.unlock(); leaveNative(); }
    }
    private static byte[] prefixEnd(byte[] prefix) {
        byte[] end = prefix.clone();
        int i = end.length - 1;
        while (i >= 0 && Byte.toUnsignedInt(end[i]) == 255) i--;
        if (i < 0) return null;
        end[i]++;
        return Arrays.copyOf(end,i+1);
    }

    /** Totals and completed position from one ordered apply cut. */
    public ReceiptUsage receiptUsage() throws RocksDBException {
        enterNative();
        applyLane.lock();
        try {
            if (pendingInstallation != null) throw new IllegalStateException("Range metadata is being installed");
            ensureReceiptUsage();
            var totals = readUsage(usageKey);
            return new ReceiptUsage(applied, totals.count, totals.bytes);
        } finally { applyLane.unlock(); leaveNative(); }
    }

    /**
     * Worst net retention growth at one completed apply cut; this reserves nothing and changes no command state.
     * Callers must serialize outstanding maintenance proposals to avoid spending overlapping ACK/floor removal twice.
     */
    public ReceiptRetentionEstimate estimateReceiptRetention(DistributedCommand command) throws RocksDBException {
        return estimateReceiptRetention(command,false);
    }

    /** Session-enabled adapters stop new data admission until their first replicated authority cut is installed. */
    public ReceiptRetentionEstimate estimateReceiptRetention(DistributedCommand command,boolean requireSessionAuthority) throws RocksDBException {
        Objects.requireNonNull(command);
        enterNative();
        applyLane.lock();
        try {
            validate(command);
            ensureReceiptUsage();
            Usage total = readUsage(usageKey).plus(sourceOutcomePromises(null));
            var usage = new ReceiptUsage(applied, total.count, total.bytes);
            boolean staleControl=canonicalStaleSchemaControl(command);
            boolean maintenance=!staleControl && (command.acknowledgedThrough()>=0 || command.minimumSessionEpoch()>=0);
            var id = command.operationId();
            long minimum = minimumSessionEpoch(null);
            long floor = id.epoch() < minimum ? -1 : acknowledgedThrough(id, null);
            if (id.epoch() < minimum || id.sequence() <= floor) {
                return new ReceiptRetentionEstimate(usage, false, 0, 0, maintenance);
            }
            byte[] saved = db.get(receipts, receiptKey(id));
            if (saved != null) {
                decodeReceipt(saved, command.receiptDigest());
                return new ReceiptRetentionEstimate(usage, false, 0, 0, maintenance);
            }
            requireUnlocked(command);
            if(staleControl) return new ReceiptRetentionEstimate(usage,true,1,88,false);
            var scope=commandScope(command); if(scope!=null) requireUngated(scope);
            var authority=sessionAuthority();
            if (requireSessionAuthority && command.sessionControl()==null && command.minimumSessionEpoch()<0 && authority==null) {
                throw new IllegalStateException("Replicated session authority has not been initialized");
            }
            var sessionStatus=expiredHomeRegistration(command) ? CommandResult.Status.SESSION_EXPIRED : sessionDecision(command,floor,authority);
            boolean terminalExpiry=sessionStatus==CommandResult.Status.SESSION_EXPIRED && expiredBoundOutcome(command,floor,authority);
            if (sessionStatus!=CommandResult.Status.SUCCESS && !terminalExpiry && !expiredHomeRegistration(command)) throw new IllegalStateException("Session admission rejected: " + sessionStatus);
            Usage removed = new Usage(0, 0);
            if (command.minimumSessionEpoch() > minimum) {
                byte[] end = ByteBuffer.allocate(25).put(usageBucketsPrefix).putLong(command.minimumSessionEpoch()).array();
                removed = scanUsage(state, usageBucketsPrefix, end, false);
            }
            if (command.acknowledgedThrough() > floor) {
                removed = removed.plus(scanUsage(receipts, receiptKey(new OperationId(id.sessionId(), id.epoch(), 0)),
                        receiptKey(new OperationId(id.sessionId(), id.epoch(), command.acknowledgedThrough() + 1)), true));
            }
            long maximumBytes = 80L + (terminalExpiry || command.mutations().stream().allMatch(m -> m.returnMode() == Mutation.ReturnMode.NONE)
                    ? 8 : CommandResult.MAX_BYTES);
            return new ReceiptRetentionEstimate(usage, true, Math.max(0, 1 - removed.count),
                    Math.max(0, maximumBytes - removed.bytes), maintenance);
        } finally { applyLane.unlock(); leaveNative(); }
    }

    private record Usage(long count, long bytes) {
        Usage {
            if (count < 0 || bytes < 0 || (count == 0 ? bytes != 0 : bytes / count < 80)) {
                throw new IllegalStateException("Invalid receipt usage");
            }
        }
        Usage plus(Usage other) { return new Usage(Math.addExact(count, other.count), Math.addExact(bytes, other.bytes)); }
        Usage minus(Usage other) { return new Usage(Math.subtractExact(count, other.count), Math.subtractExact(bytes, other.bytes)); }
        byte[] encode() { return ByteBuffer.allocate(16).putLong(count).putLong(bytes).array(); }
    }

    private static Usage decodeUsage(byte[] value) {
        if (value == null) return new Usage(0, 0);
        if (value.length != 16) {
            throw new IllegalStateException("Invalid receipt usage metadata");
        }
        return new Usage(ByteBuffer.wrap(value, 0, 16).getLong(), ByteBuffer.wrap(value, 8, 8).getLong());
    }

    // Current distinct records: total/count/bytes/term/index (32 bytes), epoch bucket/count/bytes (16 bytes).
    private static byte[] encodeTotalUsage(Usage usage, ApplyPosition position) {
        return ByteBuffer.allocate(32).put(usage.encode()).putLong(position == null ? 0 : position.term())
                .putLong(position == null ? -1 : position.index()).array();
    }

    private Usage readUsage(byte[] key) throws RocksDBException {
        byte[] value = db.get(state, key);
        return Arrays.equals(key, usageKey) ? decodeTotalUsage(value,applied) : decodeUsage(value);
    }

    // Epoch comes first so a coarse fence deletes all obsolete buckets in one range tombstone.
    private byte[] usageBucketKey(OperationId id) {
        return ByteBuffer.allocate(41).put(usageBucketsPrefix).putLong(id.epoch()).put(uuidBytes(id.sessionId())).array();
    }

    private Usage scanUsage(ColumnFamilyHandle cf, byte[] lower, byte[] upper, boolean receiptRows) throws RocksDBException {
        Usage total = new Usage(0, 0);
        try (var options = new ReadOptions().setFillCache(false); var iterator = databaseOwner.newIterator(cf, options)) {
            for (iterator.seek(lower); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (upper != null && Arrays.compareUnsigned(key, upper) >= 0) break;
                if (upper == null && (key.length < markerKey.length || !Arrays.equals(key, 0, markerKey.length, markerKey, 0, markerKey.length))) break;
                if (receiptRows) {
                    byte[] value = iterator.value();
                    validateReceiptRow(key, value);
                    total = total.plus(new Usage(1, (long) key.length + value.length));
                } else {
                    if (key.length != 41) throw new IllegalStateException("Invalid usage bucket key");
                    total = total.plus(decodeUsage(iterator.value()));
                }
            }
            iterator.status();
        }
        return total;
    }

    private void updateReceiptUsage(WriteBatch batch, DistributedCommand command, long minimumEpoch, long acknowledgement, long epochFloor,
            long acknowledged, int valueBytes, ApplyPosition position) throws RocksDBException {
        updateReceiptUsage(batch,command,minimumEpoch,acknowledgement,epochFloor,acknowledged,valueBytes,position,null);
    }
    private void updateReceiptUsage(WriteBatch batch,DistributedCommand command,long minimumEpoch,long acknowledgement,long epochFloor,long acknowledged,int valueBytes,ApplyPosition position,OperationId consuming) throws RocksDBException {
        Usage total = readUsage(usageKey);
        byte[] bucketKey = usageBucketKey(command.operationId());
        Usage bucket = readUsage(bucketKey);
        if (minimumEpoch > epochFloor) {
            byte[] end = ByteBuffer.allocate(25).put(usageBucketsPrefix).putLong(minimumEpoch).array();
            total = total.minus(scanUsage(state, usageBucketsPrefix, end, false));
            batch.deleteRange(state, usageBucketsPrefix, end);
        }
        if (acknowledgement > acknowledged) {
            var id = command.operationId();
            Usage removed = scanUsage(receipts, receiptKey(new OperationId(id.sessionId(), id.epoch(), 0)),
                    receiptKey(new OperationId(id.sessionId(), id.epoch(), acknowledgement + 1)), true);
            total = total.minus(removed);
            bucket = bucket.minus(removed);
        }
        Usage added = new Usage(1, 48L + valueBytes);checkSourceOutcomeUsage(total.plus(added),consuming);
        batch.put(state, bucketKey, bucket.plus(added).encode());
        batch.put(state, usageKey, encodeTotalUsage(total.plus(added), position));
    }

    private static void validateReceiptRow(byte[] key, byte[] value) {
        if (key.length != 48 || ByteBuffer.wrap(key, 16, 8).getLong() < 0
                || ByteBuffer.wrap(key, 40, 8).getLong() < 0 || value.length < 32
                || value.length > 32 + MAX_RESULT_BYTES) {
            throw new IllegalStateException("Invalid durable receipt row");
        }
    }

    private static Usage decodeTotalUsage(byte[] value,ApplyPosition position) {
        if (value==null || value.length!=32) throw new IllegalStateException("Missing or malformed total receipt usage");
        var usage=decodeUsage(Arrays.copyOf(value,16));
        var input=ByteBuffer.wrap(value,16,16);
        if (input.getLong()!=(position==null ? 0 : position.term()) || input.getLong()!=(position==null ? -1 : position.index())) {
            throw new IllegalStateException("Receipt usage differs from completed apply position");
        }
        return usage;
    }

    private void ensureReceiptUsage() throws RocksDBException {
        byte[] marker=db.get(state,markerKey);
        var observed=marker==null ? null : decodePosition(marker);
        if (!Objects.equals(applied,observed)) throw new IllegalStateException("Completed apply marker changed outside ordered apply");
        if (db.get(state,ByteBuffer.allocate(17).put(markerKey).put((byte)0x12).array())!=null) {
            throw new IllegalStateException("Unsupported receipt accounting metadata");
        }
        byte[] total=db.get(state,usageKey);
        if (total==null) {
            if (applied!=null || hasRows(receipts,markerKey) || hasRows(state,usageBucketsPrefix)) {
                throw new IllegalStateException("Established range has no total receipt usage");
            }
            // Only a genuinely fresh owned range can create the current empty accounting record.
            total=encodeTotalUsage(new Usage(0,0),null);
            try (var write=new WriteOptions().setDisableWAL(false).setSync(true)) { db.put(state,write,usageKey,total); }
        }
        decodeTotalUsage(total,applied);
    }

    private boolean hasRows(ColumnFamilyHandle cf,byte[] prefix) throws RocksDBException {
        try (var options=new ReadOptions().setFillCache(false); var iterator=databaseOwner.newIterator(cf,options)) {
            iterator.seek(prefix);
            byte[] key=iterator.isValid() ? iterator.key() : null;
            boolean present=key!=null && key.length>=prefix.length && Arrays.equals(key,0,prefix.length,prefix,0,prefix.length);
            iterator.status(); return present;
        }
    }

    /** Read-only validation at open/import, with one receipt and one session/epoch accumulator in memory. */
    private void validateReceiptUsage(ApplyPosition position) throws RocksDBException {
        Usage expected=decodeTotalUsage(db.get(state,usageKey),position), total=new Usage(0,0), bucket=new Usage(0,0);
        long minimum=minimumSessionEpoch(null), floor=-1, validatedBuckets=0;
        byte[] previousBucket=null;
        try (var options=new ReadOptions().setFillCache(false); var iterator=databaseOwner.newIterator(receipts,options)) {
            byte[] end=prefixEnd(markerKey);
            for (iterator.seek(markerKey);iterator.isValid();iterator.next()) {
                byte[] key=iterator.key();
                if (end!=null && Arrays.compareUnsigned(key,end)>=0) break;
                byte[] value=iterator.value(); validateReceiptRow(key,value);
                var input=ByteBuffer.wrap(key,16,32);
                long epoch=input.getLong();var id=new OperationId(new UUID(input.getLong(),input.getLong()),epoch,input.getLong());
                if (id.epoch()<minimum) throw new IllegalStateException("Obsolete physical receipt survived epoch GC");
                byte[] currentBucket=usageBucketKey(id);
                if (!Arrays.equals(previousBucket,currentBucket)) {
                    if (previousBucket!=null && bucket.count!=0) { requireUsageBucket(previousBucket,bucket); validatedBuckets++; }
                    previousBucket=currentBucket; bucket=new Usage(0,0); floor=acknowledgedThrough(id,null);
                }
                if (id.sequence()<=floor) continue;
                Usage row=new Usage(1,(long)key.length+value.length); bucket=bucket.plus(row); total=total.plus(row);
            }
            iterator.status();
        }
        if (previousBucket!=null && bucket.count!=0) { requireUsageBucket(previousBucket,bucket); validatedBuckets++; }
        Usage bucketTotal=new Usage(0,0); long buckets=0;
        try (var options=new ReadOptions().setFillCache(false); var iterator=databaseOwner.newIterator(state,options)) {
            byte[] end=prefixEnd(usageBucketsPrefix);
            for (iterator.seek(usageBucketsPrefix);iterator.isValid();iterator.next()) {
                byte[] key=iterator.key(); if (end!=null && Arrays.compareUnsigned(key,end)>=0) break;
                if (key.length!=41 || ByteBuffer.wrap(key,17,8).getLong()<minimum) throw new IllegalStateException("Invalid receipt usage bucket identity");
                Usage usage=decodeUsage(iterator.value());
                if (usage.count==0) throw new IllegalStateException("Empty receipt usage bucket");
                bucketTotal=bucketTotal.plus(usage); buckets++;
            }
            iterator.status();
        }
        if (!expected.equals(total) || !expected.equals(bucketTotal) || buckets!=validatedBuckets) {
            throw new IllegalStateException("Receipt usage differs from retained receipts/buckets");
        }
    }

    private void requireUsageBucket(byte[] key,Usage expected) throws RocksDBException {
        if (expected.count==0 || !expected.equals(decodeUsage(db.get(state,key)))) {
            throw new IllegalStateException("Receipt usage bucket differs from retained receipts");
        }
        validateSessionMetadata(null);
    }

    public static final int MAX_SESSION_ACK_NAMESPACES=65_536;
    public static final long MAX_SESSION_ACK_BYTES=25L+49L*MAX_SESSION_ACK_NAMESPACES;
    private int sessionAckLimit=MAX_SESSION_ACK_NAMESPACES;
    private java.util.TreeMap<Long,Integer> sessionAckEpochs;
    private int sessionAckCount;
    private byte[] sessionAckCountKey(){return ByteBuffer.allocate(17).put(markerKey).put((byte)0x15).array();}
    private int validateSessionMetadata(Snapshot snapshot) throws RocksDBException {
        int count=0;var epochs=new java.util.TreeMap<Long,Integer>();
        try(var options=new ReadOptions().setSnapshot(snapshot).setFillCache(false);var iterator=databaseOwner.newIterator(state,options)) {
            long minimum=minimumSessionEpoch(options);byte[] prefix=sessionFloorPrefix();var sizing=new byte[0];iterator.seek(prefix);
            for(;iterator.isValid() && iterator.key().length>=prefix.length && Arrays.equals(iterator.key(),0,prefix.length,prefix,0,prefix.length);iterator.next()) {
                byte[] key=iterator.key();if(key.length!=41 || iterator.value(sizing)!=8)throw new IllegalStateException("Invalid session ACK namespace shape");long epoch=ByteBuffer.wrap(key,17,8).getLong();
                var value=ByteBuffer.wrap(iterator.value());long floor=value.getLong();if(epoch<minimum || floor<0 || floor==Long.MAX_VALUE || ++count>MAX_SESSION_ACK_NAMESPACES)throw new IllegalStateException("Session ACK namespace inventory corrupt or unbounded");epochs.merge(epoch,1,Integer::sum);
            }
            iterator.status();byte[] total=db.get(state,options,sessionAckCountKey());if(total==null ? count!=0 : total.length!=8 || ByteBuffer.wrap(total).getLong()!=count)throw new IllegalStateException("Session ACK namespace count differs from inventory");
        }
        if(snapshot==null){sessionAckCount=count;sessionAckEpochs=epochs;}return count;
    }
    public void configureSessionMetadataCapacity(int maximumNamespaces) throws RocksDBException {
        if(maximumNamespaces<1 || maximumNamespaces>MAX_SESSION_ACK_NAMESPACES)throw new IllegalArgumentException("Invalid session ACK namespace capacity");enterNative();applyLane.lock();try {if(validateSessionMetadata(null)+sourceSessionMetadataPromises(minimumSessionEpoch(null),null,null)>maximumNamespaces)throw new IllegalStateException("Existing session ACK namespaces/promises exceed configured capacity");sessionAckLimit=maximumNamespaces;}finally {applyLane.unlock();leaveNative();}
    }
    public record SessionMetadataEstimate(ApplyPosition position,int retainedNamespaces,int reservedNamespaces,int maximumNamespaces,int removedNamespaces,int additionalNamespaces,boolean existingReceipt) {}
    private record SessionMetadataChange(int count,long minimumEpoch,long newEpoch,boolean additional) {}
    private SessionMetadataChange sessionMetadataChange(DistributedCommand command,long minimum,long acknowledgement) throws RocksDBException {
        if(sessionAckEpochs==null)validateSessionMetadata(null);int removed=0;
        if(minimum>=0)for(var value:sessionAckEpochs.headMap(minimum,false).values())removed+=value;
        var id=command.operationId();boolean add=acknowledgement>=0 && id.epoch()>=Math.max(0,minimum) && db.get(state,sessionFloorKey(id))==null;
        return new SessionMetadataChange(sessionAckCount-removed+(add?1:0),minimum,id.epoch(),add);
    }
    private SessionMetadataEstimate sessionMetadataEstimate(SessionMetadataChange change,boolean saved,DistributedCommand candidate) throws RocksDBException {
        long current=minimumSessionEpoch(null),minimum=Math.max(current,change.minimumEpoch());
        int reserved=sourceSessionMetadataPromises(minimum,null,null);
        if(change.additional()){var key=sessionFloorKey(candidate.operationId());for(var credit:sourceOutcomeCredits.values()){var original=credit.request().original();if(original.acknowledgedThrough()>=0 && original.operationId().epoch()>=minimum && Arrays.equals(key,sessionFloorKey(original.operationId()))){reserved--;break;}}}
        return new SessionMetadataEstimate(applied,sessionAckCount,reserved,sessionAckLimit,sessionAckCount+(change.additional()?1:0)-change.count(),change.additional()?1:0,saved);
    }
    public SessionMetadataEstimate estimateSessionMetadata(DistributedCommand command) throws RocksDBException {
        Objects.requireNonNull(command);enterNative();applyLane.lock();try {
            validateCommand(command,false);if(sessionAckEpochs==null)validateSessionMetadata(null);boolean saved=db.get(receipts,receiptKey(command.operationId()))!=null;
            long minimum=minimumSessionEpoch(null);var id=command.operationId();long floor=id.epoch()<minimum ? -1 : acknowledgedThrough(id,null);
            boolean eligible=!saved && id.epoch()>=minimum && id.sequence()>floor;
            if(eligible){var authority=sessionAuthority();var status=expiredHomeRegistration(command) ? CommandResult.Status.SESSION_EXPIRED : sessionDecision(command,floor,authority);eligible=status==CommandResult.Status.SUCCESS || status==CommandResult.Status.SESSION_EXPIRED && expiredBoundOutcome(command,floor,authority);}
            var change=eligible ? sessionMetadataChange(command,command.minimumSessionEpoch(),command.acknowledgedThrough()) : new SessionMetadataChange(sessionAckCount,-1,0,false);
            return sessionMetadataEstimate(change,saved,command);
        }finally {applyLane.unlock();leaveNative();}
    }
    /** Protected publication promise estimate: no original USER read or old-grant revalidation. */
    public SessionMetadataEstimate estimateSourceSessionMetadata(NativeSourceOutcomeCredit.Request request) throws RocksDBException {
        Objects.requireNonNull(request);enterNative();applyLane.lock();try {
            requireSourceOutcomeRequest(request,true);if(sessionAckEpochs==null)validateSessionMetadata(null);var original=request.original();var id=original.operationId();
            boolean saved=sourceOutcomeCredits.containsKey(id) || sourceOutcomePublications.containsKey(id);
            var change=!saved && !sourceOperationFenced(id) ? sessionMetadataChange(original,-1,original.acknowledgedThrough()) : new SessionMetadataChange(sessionAckCount,-1,0,false);
            return sessionMetadataEstimate(change,saved,original);
        }finally {applyLane.unlock();leaveNative();}
    }
    private int sourceSessionMetadataPromises(long minimum,OperationId consuming,DistributedCommand additional) throws RocksDBException {
        var keys=new java.util.HashSet<String>();
        for(var credit:sourceOutcomeCredits.values()){var original=credit.request().original();if(consuming!=null && consuming.equals(original.operationId()))continue;var id=original.operationId();if(original.acknowledgedThrough()>=0 && id.epoch()>=minimum && db.get(state,sessionFloorKey(id))==null)keys.add(java.util.HexFormat.of().formatHex(sessionFloorKey(id)));}
        if(additional!=null){var id=additional.operationId();if(additional.acknowledgedThrough()>=0 && id.epoch()>=minimum && db.get(state,sessionFloorKey(id))==null)keys.add(java.util.HexFormat.of().formatHex(sessionFloorKey(id)));}return keys.size();
    }
    private void publishSessionMetadata(SessionMetadataChange change){if(change==null)return;if(change.minimumEpoch()>=0)sessionAckEpochs.headMap(change.minimumEpoch(),false).clear();if(change.additional())sessionAckEpochs.merge(change.newEpoch(),1,Integer::sum);sessionAckCount=change.count();}
    private byte[] sessionFloorPrefix(){return ByteBuffer.allocate(17).put(markerKey).put((byte)2).array();}
    private byte[] sessionFloorEpoch(long epoch){return ByteBuffer.allocate(25).put(sessionFloorPrefix()).putLong(epoch).array();}
    private byte[] receiptEpoch(long epoch){return ByteBuffer.allocate(24).put(markerKey).putLong(epoch).array();}
    private byte[] epochFloorKey() { return ByteBuffer.allocate(17).put(uuidBytes(groupId)).put((byte) 3).array(); }
    private long minimumSessionEpoch(ReadOptions options) throws RocksDBException {
        byte[] value = options == null ? db.get(state, epochFloorKey()) : db.get(state, options, epochFloorKey());
        if (value == null) return 0;
        if (value.length != 8) throw new IllegalStateException("Invalid session epoch floor");
        var input = ByteBuffer.wrap(value);
        long floor = input.getLong();
        if (floor < 0) throw new IllegalStateException("Invalid session epoch floor");
        return floor;
    }

    private static byte[] expiredResult() {
        return new CommandResult(CommandResult.Status.SESSION_EXPIRED, List.of()).encode();
    }
    private byte[] sessionFloorKey(OperationId id) {
        return ByteBuffer.allocate(41).put(uuidBytes(groupId)).put((byte) 2).putLong(id.epoch()).put(uuidBytes(id.sessionId())).array();
    }
    private long acknowledgedThrough(OperationId id, ReadOptions options) throws RocksDBException {
        byte[] value = options == null ? db.get(state, sessionFloorKey(id)) : db.get(state, options, sessionFloorKey(id));
        if (value == null) return -1;
        if (value.length != 8) throw new IllegalStateException("Invalid session floor");
        var input = ByteBuffer.wrap(value);
        long floor = input.getLong();
        if (floor < 0 || floor == Long.MAX_VALUE) throw new IllegalStateException("Invalid session floor sequence");
        return floor;
    }

    private byte[] receiptKey(OperationId id) {
        return ByteBuffer.allocate(48).put(uuidBytes(groupId)).putLong(id.epoch()).put(uuidBytes(id.sessionId())).putLong(id.sequence()).array();
    }
    private static byte[] uuidBytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
    }
    private static byte[] encodePosition(ApplyPosition position) {
        return ByteBuffer.allocate(16).putLong(position.term()).putLong(position.index()).array();
    }
    private static byte[] encodeConfiguration(Configuration configuration) {
        return ByteBuffer.allocate(16 + configuration.data.length)
                .putLong(configuration.term()).putLong(configuration.index()).put(configuration.data).array();
    }
    private static Configuration decodeConfiguration(byte[] value) {
        if (value.length <= 16 || value.length > 16 + 65536) throw new IllegalStateException("Invalid cut configuration bytes");
        var input = ByteBuffer.wrap(value);
        long term = input.getLong();
        long index = input.getLong();
        byte[] data = new byte[input.remaining()];
        input.get(data);
        return new Configuration(term, index, data);
    }
    static ApplyPosition decodePosition(byte[] value) {
        if (value.length != 16) throw new IllegalStateException("Invalid applied marker");
        var buffer = ByteBuffer.wrap(value);
        return new ApplyPosition(buffer.getLong(), buffer.getLong());
    }
    private static byte[] encodeReceipt(byte[] digest, byte[] result) {
        return ByteBuffer.allocate(32 + result.length).put(digest).put(result).array();
    }
    private static byte[] decodeReceipt(byte[] value, byte[] digest) {
        if (digest.length != 32 || value.length < 32 || value.length > 32 + MAX_RESULT_BYTES) {
            throw new IllegalStateException("Invalid durable receipt");
        }
        var buffer = ByteBuffer.wrap(value);
        byte[] originalHash = new byte[32];
        buffer.get(originalHash);
        if (!Arrays.equals(originalHash, digest)) throw new IllegalArgumentException("Operation identity reused with different payload");
        return Arrays.copyOfRange(value, 32, value.length);
    }
}
