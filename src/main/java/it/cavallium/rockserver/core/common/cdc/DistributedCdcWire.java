package it.cavallium.rockserver.core.common.cdc;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.cdc.proto.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import it.cavallium.rockserver.core.common.api.cdc.proto.CdcBatch;
import java.util.*;

/** One current typed vector CDC contract. Check frame/count/byte bounds before materializing payloads. */
public final class DistributedCdcWire {
    public static final int MAX_REQUEST_BYTES=128*1024,MAX_FRAME_BYTES=32*1024*1024,MIN_POLL_BYTES=111;
    private DistributedCdcWire() {}
    public static <T extends MessageLite> T bounded(T value,int maximum) { if(value.getSerializedSize()>maximum) throw new IllegalArgumentException("CDC protobuf frame exceeds byte bound"); return value; }
    public static ByteString uuid(UUID value) { return ByteString.copyFrom(ByteBuffer.allocate(16).putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()).array()); }
    public static UUID uuid(ByteString value) { exact(value,16); var bytes=value.asReadOnlyByteBuffer(); return new UUID(bytes.getLong(),bytes.getLong()); }
    private static void exact(ByteString value,int bytes) { if(value.size()!=bytes) throw new IllegalArgumentException("Invalid CDC identity/digest length"); }
    private static void count(int count,int maximum) { if(count<1 || count>maximum) throw new IllegalArgumentException("Invalid CDC collection count"); }
    public static RequestContext context(it.cavallium.rockserver.core.common.api.proto.RequestContext value) { return new RequestContext(WorkloadProfile.fromWireValue(value.getProfileValue()),value.getTimeoutNanos()); }
    public static it.cavallium.rockserver.core.common.api.proto.RequestContext context(RequestContext value,long remaining) {
        Objects.requireNonNull(value); if(remaining<=0) throw Status.DEADLINE_EXCEEDED.asRuntimeException();
        return it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder().setProfileValue(value.profile().wireValue()).setTimeoutNanos(remaining).build();
    }
    public static void pollBudget(int records,int bytes) { if(records<1 || records>DistributedCdcSubscriptions.MAX_RECORDS || bytes<MIN_POLL_BYTES || bytes>DistributedCdcSubscriptions.MAX_POLL_BYTES) throw new IllegalArgumentException("Invalid vector CDC poll budget"); }
    public static CdcPosition position(ApplyPosition value) { return CdcPosition.newBuilder().setTerm(value.term()).setIndex(value.index()).build(); }
    public static ApplyPosition position(CdcPosition value) { return new ApplyPosition(value.getTerm(),value.getIndex()); }
    public static CdcCursor cursor(DistributedCdcCheckpoint.Cursor value) { return CdcCursor.newBuilder().setPosition(position(value.position())).setMutationOrdinal(value.mutationOrdinal()).build(); }
    public static DistributedCdcCheckpoint.Cursor cursor(CdcCursor value) { if(!value.hasPosition()) throw new IllegalArgumentException("Missing CDC cursor position"); return new DistributedCdcCheckpoint.Cursor(position(value.getPosition()),value.getMutationOrdinal()); }
    public static CdcLineage lineage(DistributedCdcCheckpoint.Lineage value) { return CdcLineage.newBuilder().setRangeId(uuid(value.rangeId())).setGroupId(uuid(value.groupId())).setRangeEpoch(value.rangeEpoch()).build(); }
    public static DistributedCdcCheckpoint.Lineage lineage(CdcLineage value) { return new DistributedCdcCheckpoint.Lineage(uuid(value.getRangeId()),uuid(value.getGroupId()),value.getRangeEpoch()); }
    public static CdcOperationId operation(OperationId value) { return CdcOperationId.newBuilder().setSessionId(uuid(value.sessionId())).setEpoch(value.epoch()).setSequence(value.sequence()).build(); }
    public static OperationId operation(CdcOperationId value) { return new OperationId(uuid(value.getSessionId()),value.getEpoch(),value.getSequence()); }
    public static CdcCheckpoint checkpoint(DistributedCdcCheckpoint value) {
        var builder=CdcCheckpoint.newBuilder(); value.cursors().forEach((l,c)->builder.addEntries(CdcCheckpointEntry.newBuilder().setLineage(lineage(l)).setCursor(cursor(c)))); return builder.build();
    }
    public static DistributedCdcCheckpoint checkpoint(CdcCheckpoint value) {
        bounded(value,MAX_REQUEST_BYTES); count(value.getEntriesCount(),DistributedCdcCheckpoint.MAX_LINEAGES); var entries=new TreeMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>();
        for(var entry:value.getEntriesList()) { if(!entry.hasLineage() || !entry.hasCursor() || entries.put(lineage(entry.getLineage()),cursor(entry.getCursor()))!=null) throw new IllegalArgumentException("Missing/duplicate CDC checkpoint lineage"); }
        return new DistributedCdcCheckpoint(entries);
    }
    public static CdcDefinition definition(DistributedCdcSubscription.Definition value) {
        var builder=CdcDefinition.newBuilder().setClusterId(uuid(value.clusterId())).setDatabaseId(uuid(value.databaseId())).setDomainId(uuid(value.domainId())).setSubscriptionId(uuid(value.subscriptionId())).setIncarnationId(uuid(value.incarnationId())).setDirectoryDigest(ByteString.copyFrom(value.directoryDigest())).setStart(checkpoint(value.start()));
        value.columns().forEach(c->builder.addColumns(uuid(c))); return bounded(builder.build(),MAX_REQUEST_BYTES);
    }
    public static DistributedCdcSubscription.Definition definition(CdcDefinition value) {
        bounded(value,MAX_REQUEST_BYTES); count(value.getColumnsCount(),DistributedCdcSubscription.MAX_COLUMNS); exact(value.getDirectoryDigest(),32); if(!value.hasStart()) throw new IllegalArgumentException("Missing CDC initial checkpoint");
        return new DistributedCdcSubscription.Definition(uuid(value.getClusterId()),uuid(value.getDatabaseId()),uuid(value.getDomainId()),uuid(value.getSubscriptionId()),uuid(value.getIncarnationId()),value.getDirectoryDigest().toByteArray(),value.getColumnsList().stream().map(DistributedCdcWire::uuid).toList(),checkpoint(value.getStart()));
    }
    public static CdcState state(DistributedCdcSubscription value) {
        var builder=CdcState.newBuilder().setDefinition(definition(value.definition())).setGeneration(value.generation()).setPhase(value.phase()==DistributedCdcSubscription.Phase.ACTIVE ? CdcStatePhase.SUBSCRIPTION_ACTIVE : CdcStatePhase.SUBSCRIPTION_CLOSED).setCheckpoint(checkpoint(value.checkpoint())).setDelivered(checkpoint(value.delivered())).setCurrentStarts(checkpoint(value.currentStarts()));
        if(value.latestHandoff()!=null) builder.setLatestHandoff(ByteString.copyFrom(value.latestHandoff().encode()));
        return builder.build();
    }
    public static DistributedCdcSubscription state(CdcState value) {
        bounded(value,MAX_FRAME_BYTES); if(!value.hasDefinition() || !value.hasCheckpoint() || !value.hasDelivered() || !value.hasCurrentStarts() || value.getPhaseValue()<0 || value.getPhaseValue()>1) throw new IllegalArgumentException("Missing/invalid CDC state fields");
        byte[] definition=definition(value.getDefinition()).encode(),ack=checkpoint(value.getCheckpoint()).encode(),delivered=checkpoint(value.getDelivered()).encode(),starts=checkpoint(value.getCurrentStarts()).encode();
        byte[] handoff=new byte[0];
        if(value.hasLatestHandoff()) {
            if(value.getLatestHandoff().size()>DistributedCdcBarrierHandoff.MAX_BYTES) throw new IllegalArgumentException("Oversized CDC handoff");
            handoff=DistributedCdcBarrierHandoff.decode(value.getLatestHandoff().toByteArray()).encode();
        }
        var encoded=ByteBuffer.allocate(DistributedCdcSubscription.HEADER_BYTES+definition.length+ack.length+delivered.length+starts.length+handoff.length).putLong(value.getGeneration()).put((byte)value.getPhaseValue()).putInt(definition.length).put(definition).putInt(ack.length).put(ack).putInt(delivered.length).put(delivered).putInt(starts.length).put(starts).putInt(handoff.length).put(handoff).array();
        return DistributedCdcSubscription.decode(encoded);
    }
    public static CdcStatus status(DistributedCdcSubscriptions.Status value,UUID expected) {
        if(!value.state().definition().subscriptionId().equals(expected)) throw new IllegalArgumentException("Crossed CDC status identity"); return bounded(CdcStatus.newBuilder().setState(state(value.state())).setPhase(CdcLocalPhase.valueOf(value.phase().name())).build(),MAX_FRAME_BYTES);
    }
    public static DistributedCdcSubscriptions.Status status(CdcStatus value,UUID expected) {
        bounded(value,MAX_FRAME_BYTES); if(!value.hasState() || value.getPhase()==CdcLocalPhase.UNRECOGNIZED) throw new IllegalArgumentException("Missing/unknown CDC status");
        var state=state(value.getState()); if(!state.definition().subscriptionId().equals(expected)) throw new IllegalArgumentException("Crossed CDC status identity"); return new DistributedCdcSubscriptions.Status(state,DistributedCdcSubscriptions.Phase.valueOf(value.getPhase().name()));
    }
    public static CdcStatus status(DistributedCdcSubscriptions.Status value,DistributedCdcSubscription.Definition expected) {
        if(!value.state().definition().equals(expected)) throw new IllegalArgumentException("CDC create changed immutable definition/incarnation"); return status(value,expected.subscriptionId());
    }
    public static DistributedCdcSubscriptions.Status status(CdcStatus value,DistributedCdcSubscription.Definition expected) {
        var result=status(value,expected.subscriptionId()); if(!result.state().definition().equals(expected)) throw new IllegalArgumentException("CDC create changed immutable definition/incarnation"); return result;
    }
    public static CdcMutation mutation(Mutation value) {
        var builder=CdcMutation.newBuilder().setKindValue(value.kind().ordinal()).setColumnId(uuid(value.columnId())).setKey(ByteString.copyFrom(value.key())).setReturnModeValue(value.returnMode().ordinal());
        byte[] payload=value.value(); if(payload!=null) builder.setValue(ByteString.copyFrom(payload)); value.memberKeys().forEach(k->builder.addMemberKeys(ByteString.copyFrom(k))); return builder.build();
    }
    public static Mutation mutation(CdcMutation value) {
        bounded(value,MAX_FRAME_BYTES);
        if(value.getKindValue()<0 || value.getKindValue()>=Mutation.Kind.values().length || value.getReturnModeValue()<0 || value.getReturnModeValue()>=Mutation.ReturnMode.values().length || value.getMemberKeysCount()>DistributedSchemaCodec.MAX_KEY_COMPONENTS) throw new IllegalArgumentException("Invalid CDC mutation tags/components");
        long bytes=26L+value.getKey().size()+value.getValue().size(); for(var member:value.getMemberKeysList()) { if(member.size()>65535) throw new IllegalArgumentException("Oversized CDC member key"); bytes+=2L+member.size(); }
        var kind=Mutation.Kind.values()[value.getKindValue()]; boolean bucket=kind==Mutation.Kind.BUCKET_PUT || kind==Mutation.Kind.BUCKET_DELETE || kind==Mutation.Kind.BUCKET_MERGE;
        if(bytes+(bucket ? 4 : 0)>DistributedCommandCodec.MAX_ENCODED_BYTES || bucket!=(value.getMemberKeysCount()>0)) throw new IllegalArgumentException("Invalid CDC mutation body bound");
        boolean deletion=kind==Mutation.Kind.DELETE || kind==Mutation.Kind.BUCKET_DELETE;
        if(deletion ? value.hasValue() : !value.hasValue()) throw new IllegalArgumentException("Invalid CDC mutation value presence");
        var mode=Mutation.ReturnMode.values()[value.getReturnModeValue()]; UUID column=uuid(value.getColumnId()); byte[] key=value.getKey().toByteArray(),payload=value.hasValue() ? value.getValue().toByteArray() : null;
        if(bucket) {
            var members=value.getMemberKeysList().stream().map(ByteString::toByteArray).toList();
            return switch(kind) { case BUCKET_PUT -> Mutation.bucketPut(column,key,members,Objects.requireNonNull(payload),mode); case BUCKET_DELETE -> { if(payload!=null) throw new IllegalArgumentException("CDC delete has a value"); yield Mutation.bucketDelete(column,key,members,mode); } case BUCKET_MERGE -> Mutation.bucketMerge(column,key,members,Objects.requireNonNull(payload),mode); default -> throw new AssertionError(); };
        }
        return new Mutation(kind,column,key,payload,mode);
    }
    public static CdcFinalization finalization(ParticipantFinalization value) {
        var decision=value.decision();
        if(decision.phase()!=TransactionStatus.Phase.COMMIT && decision.phase()!=TransactionStatus.Phase.ABORT) throw new IllegalArgumentException("CDC finalization requires a terminal decision");
        var builder=CdcDecision.newBuilder().setClusterId(uuid(decision.clusterId())).setDatabaseId(uuid(decision.databaseId())).setDomainId(uuid(decision.domainId())).setTransactionId(uuid(decision.transactionId())).setOperation(operation(decision.operation())).setSchemaEpoch(decision.schemaEpoch()).setSchemaDigest(ByteString.copyFrom(decision.schemaDigest())).setPlanDigest(ByteString.copyFrom(decision.planDigest())).setPhase(decision.phase()==TransactionStatus.Phase.COMMIT ? CdcDecisionPhase.DECISION_COMMIT : CdcDecisionPhase.DECISION_ABORT);
        if(decision.origin()!=null) builder.setOrigin(ByteString.copyFrom(decision.origin().encode()));
        for(var participant:decision.participants()) { var p=CdcParticipant.newBuilder().setLineage(lineage(new DistributedCdcCheckpoint.Lineage(participant.rangeId(),participant.groupId(),participant.rangeEpoch()))).setSchemaDigest(ByteString.copyFrom(participant.schemaDigest())).setPlanDigest(ByteString.copyFrom(participant.planDigest())); if(participant.prepared()!=null) p.setPrepared(position(participant.prepared())); builder.addParticipants(p); }
        return CdcFinalization.newBuilder().setHomeGroup(uuid(value.homeGroup())).setHomeEpoch(value.homeEpoch()).setParticipantGroup(uuid(value.participantGroup())).setRecoveryOperation(operation(value.operation())).setDecision(builder).build();
    }
    public static ParticipantFinalization finalization(CdcFinalization value) {
        bounded(value,MAX_REQUEST_BYTES); if(!value.hasDecision() || !value.hasRecoveryOperation()) throw new IllegalArgumentException("Missing CDC finalization"); var d=value.getDecision();
        count(d.getParticipantsCount(),TransactionStatus.MAX_PARTICIPANTS); exact(d.getSchemaDigest(),32); exact(d.getPlanDigest(),32); if(!d.hasOperation() || d.getPhase()==CdcDecisionPhase.UNRECOGNIZED) throw new IllegalArgumentException("Invalid CDC decision");
        var participants=new ArrayList<TransactionStatus.Participant>();
        for(var p:d.getParticipantsList()) { if(!p.hasLineage()) throw new IllegalArgumentException("Missing CDC participant lineage"); exact(p.getSchemaDigest(),32); exact(p.getPlanDigest(),32); var l=lineage(p.getLineage()); participants.add(new TransactionStatus.Participant(l.rangeId(),l.groupId(),l.rangeEpoch(),p.getSchemaDigest().toByteArray(),p.getPlanDigest().toByteArray(),null,false)); }
        var decision=TransactionStatus.begin(uuid(d.getClusterId()),uuid(d.getDatabaseId()),uuid(d.getDomainId()),uuid(d.getTransactionId()),operation(d.getOperation()),d.getSchemaEpoch(),d.getSchemaDigest().toByteArray(),d.getPlanDigest().toByteArray(),participants);
        if(d.hasOrigin()) {
            exact(d.getOrigin(),OperationRerouteOrigin.MAX_BYTES);
            var origin=OperationRerouteOrigin.decode(d.getOrigin().toByteArray());
            decision=TransactionStatus.reserve(origin,d.getSchemaEpoch(),d.getSchemaDigest().toByteArray()).promote(decision);
        }
        for(var p:d.getParticipantsList()) if(p.hasPrepared()) decision=decision.prepared(uuid(p.getLineage().getGroupId()),p.getPlanDigest().toByteArray(),position(p.getPrepared()));
        decision=decision.decide(d.getPhase()==CdcDecisionPhase.DECISION_COMMIT ? TransactionStatus.Phase.COMMIT : TransactionStatus.Phase.ABORT);
        return new ParticipantFinalization(uuid(value.getHomeGroup()),value.getHomeEpoch(),uuid(value.getParticipantGroup()),operation(value.getRecoveryOperation()),decision);
    }
    public static CdcEnvelope envelope(DistributedCdcSubscriptions.Envelope value) {
        var builder=CdcEnvelope.newBuilder().setPosition(position(value.position())).setSchemaDigest(ByteString.copyFrom(value.schemaDigest())); if(value.operation()!=null) builder.setOperation(operation(value.operation())); if(value.terminalBarrier()!=null) builder.setTerminalBarrier(ByteString.copyFrom(value.terminalBarrier().encode())); if(value.transaction()!=null) builder.setTransaction(finalization(value.transaction()));
        value.events().forEach(e->builder.addEvents(CdcEvent.newBuilder().setMutationOrdinal(e.cursor().mutationOrdinal()).setMutation(mutation(e.mutation())))); return builder.build();
    }
    public static DistributedCdcSubscriptions.Envelope envelope(CdcEnvelope value) {
        bounded(value,MAX_FRAME_BYTES); if(value.getEventsCount()>DistributedCdcSubscriptions.MAX_EVENTS_PER_ENVELOPE) throw new IllegalArgumentException("CDC envelope event count exceeds bound"); exact(value.getSchemaDigest(),32); if(!value.hasPosition()) throw new IllegalArgumentException("Missing CDC envelope position");
        if(value.hasTerminalBarrier()) {
            if(value.hasOperation() || value.hasTransaction() || value.getEventsCount()!=0 || value.getTerminalBarrier().size()>NativeTopologyFreeze.MAX_BYTES) throw new IllegalArgumentException("Mixed or oversized CDC barrier payload");
            return new DistributedCdcSubscriptions.Envelope(position(value.getPosition()),value.getSchemaDigest().toByteArray(),null,null,List.of(),NativeTopologyFreeze.decode(value.getTerminalBarrier().toByteArray()));
        }
        if(!value.hasOperation()) throw new IllegalArgumentException("Missing CDC mutation operation");
        count(value.getEventsCount(),DistributedCdcSubscriptions.MAX_EVENTS_PER_ENVELOPE);
        long payload=0;
        for(var event:value.getEventsList()) {
            var mutation=event.getMutation(); payload+=26L+mutation.getKey().size()+mutation.getValue().size();
            if(mutation.getMemberKeysCount()>0) { payload+=4; for(var member:mutation.getMemberKeysList()) payload+=2L+member.size(); }
            if(payload>DistributedCommandCodec.MAX_ENCODED_BYTES-DistributedCommandCodec.HEADER_BYTES) throw new IllegalArgumentException("Oversized projected CDC envelope payload");
        }
        var position=position(value.getPosition()); var events=new ArrayList<DistributedCdcRecord.Event>();
        for(var event:value.getEventsList()) { if(!event.hasMutation()) throw new IllegalArgumentException("Missing CDC mutation"); events.add(new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(position,event.getMutationOrdinal()),mutation(event.getMutation()))); }
        return new DistributedCdcSubscriptions.Envelope(position,value.getSchemaDigest().toByteArray(),operation(value.getOperation()),value.hasTransaction() ? finalization(value.getTransaction()) : null,events);
    }
    public static CdcBatch batch(DistributedCdcSubscriptions.Batch value) {
        var builder=CdcBatch.newBuilder().setId(uuid(value.id())).setDefinition(definition(value.definition())).setCurrentStarts(checkpoint(value.currentStarts())).setLineage(lineage(value.lineage())).setFrom(cursor(value.from())).setNext(cursor(value.next())).setCut(position(value.cut())); value.envelopes().forEach(e->builder.addEnvelopes(envelope(e))); return bounded(builder.build(),MAX_FRAME_BYTES);
    }
    public static DistributedCdcSubscriptions.Batch batch(CdcBatch value,UUID expected) {
        bounded(value,MAX_FRAME_BYTES); if(value.getEnvelopesCount()>DistributedCdcSubscriptions.MAX_RECORDS || !value.hasDefinition() || !value.hasLineage() || !value.hasFrom() || !value.hasNext() || !value.hasCut()) throw new IllegalArgumentException("Missing/oversized CDC batch");
        var definition=definition(value.getDefinition()); if(!definition.subscriptionId().equals(expected)) throw new IllegalArgumentException("Crossed CDC batch identity");
        if(!value.hasCurrentStarts()) throw new IllegalArgumentException("Missing CDC current start frontier");
        var starts=checkpoint(value.getCurrentStarts());
        return new DistributedCdcSubscriptions.Batch(uuid(value.getId()),definition,lineage(value.getLineage()),cursor(value.getFrom()),cursor(value.getNext()),position(value.getCut()),value.getEnvelopesList().stream().map(DistributedCdcWire::envelope).toList(),starts);
    }
    /** New-page limits never replace the identity/budget of an already retained observation. */
    public static CdcPoll poll(DistributedCdcSubscriptions.Poll value,UUID expected,int maxRecords,int maxBytes) {
        pollBudget(maxRecords,maxBytes); if(value.batch()!=null && (!value.batch().definition().subscriptionId().equals(expected))) throw new IllegalArgumentException("Crossed/oversized CDC poll");
        var builder=CdcPoll.newBuilder().setLineage(lineage(value.lineage())).setOutcome(CdcOutcome.valueOf(value.outcome().name())).setRequiredBytes(value.requiredBytes()).setFloor(cursor(value.floor())).setCut(position(value.cut())); if(value.batch()!=null) builder.setBatch(batch(value.batch())); return bounded(builder.build(),MAX_FRAME_BYTES);
    }
    public static DistributedCdcSubscriptions.Poll poll(CdcPoll value,UUID expected,int maxRecords,int maxBytes) {
        bounded(value,MAX_FRAME_BYTES); pollBudget(maxRecords,maxBytes); if(!value.hasLineage() || !value.hasFloor() || !value.hasCut() || value.getOutcome()==CdcOutcome.UNRECOGNIZED) throw new IllegalArgumentException("Missing/invalid CDC poll");
        return new DistributedCdcSubscriptions.Poll(lineage(value.getLineage()),DistributedCdcSubscriptions.Outcome.valueOf(value.getOutcome().name()),value.hasBatch() ? batch(value.getBatch(),expected) : null,value.getRequiredBytes(),cursor(value.getFloor()),position(value.getCut()));
    }
    public static CdcDirectory directory(RangeDirectory value,UUID expected) {
        if(!value.domainId().equals(expected)) throw new IllegalArgumentException("Crossed CDC context domain");
        var builder=CdcDirectory.newBuilder().setCanonical(ByteString.copyFrom(RangeDirectoryCodec.encode(value))).setClusterId(uuid(value.clusterId())).setDatabaseId(uuid(value.databaseId())).setDomainId(uuid(value.domainId())).setSchemaEpoch(value.schemaEpoch()).setSchemaDigest(ByteString.copyFrom(value.schemaDigest())).setGeneration(value.generation()).setRoutingPrefixBytes(value.routingPrefixBytes()); value.columns().stream().sorted().forEach(c->builder.addColumns(uuid(c)));
        for(var range:value.ranges()) { var r=CdcRange.newBuilder().setLineage(lineage(new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()))).setConfigurationVersion(range.configurationVersion()).setTopologyValue(range.topologyState().ordinal()); if(range.lowerInclusive()!=null) r.setLowerInclusive(ByteString.copyFrom(range.lowerInclusive())); if(range.upperExclusive()!=null) r.setUpperExclusive(ByteString.copyFrom(range.upperExclusive())); builder.addRanges(r); } return bounded(builder.build(),MAX_FRAME_BYTES);
    }
    public static RangeDirectory directory(CdcDirectory value,UUID expected) {
        bounded(value,MAX_FRAME_BYTES); count(value.getColumnsCount(),RangeDirectoryCodec.MAX_COLUMNS); count(value.getRangesCount(),RangeDirectoryCodec.MAX_RANGES); if(value.getCanonical().size()>RangeDirectoryCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Oversized CDC context directory");
        var decoded=RangeDirectoryCodec.decode(value.getCanonical().toByteArray()); if(!directory(decoded,expected).equals(value)) throw new IllegalArgumentException("CDC typed context differs from canonical authority"); return decoded;
    }
}
