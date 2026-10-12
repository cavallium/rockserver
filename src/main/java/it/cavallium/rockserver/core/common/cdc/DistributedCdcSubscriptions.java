package it.cavallium.rockserver.core.common.cdc;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;

/** Bounded immutable vector CDC results. Projected envelopes contain selected mutations or an unfiltered terminal lineage barrier. */
public final class DistributedCdcSubscriptions {
    public static final int MAX_RECORDS=256,MAX_POLL_BYTES=8*1024*1024;
    public static final int MAX_EVENTS_PER_ENVELOPE=DistributedCommandCodec.MAX_MUTATIONS,MAX_EVENTS=MAX_RECORDS*MAX_EVENTS_PER_ENVELOPE;
    /** Fixed projected metadata, including body/count length fields; definition and events are additional. */
    public static final int BATCH_HEADER_BYTES=120,ENVELOPE_HEADER_BYTES=92,EVENT_ORDINAL_BYTES=4;
    public static final int MAX_BATCH_BYTES=MAX_POLL_BYTES+DistributedCdcSubscription.Definition.MAX_BYTES+DistributedCdcCheckpoint.MAX_BYTES+EVENT_ORDINAL_BYTES*MAX_EVENTS+BATCH_HEADER_BYTES+4*MAX_RECORDS;
    private DistributedCdcSubscriptions() {}
    public enum Phase { CREATE, ACTIVE, CLOSING, CLOSED, CONFLICT }
    public enum Outcome { DATA, GAP, TOO_LARGE }
    public record Status(DistributedCdcSubscription state,Phase phase) {
        public Status {
            Objects.requireNonNull(state); Objects.requireNonNull(phase);
            if(phase==Phase.CREATE && !state.equals(DistributedCdcSubscription.initial(state.definition()))
                    || phase==Phase.CLOSED && state.phase()!=DistributedCdcSubscription.Phase.CLOSED
                    || phase==Phase.ACTIVE && state.phase()!=DistributedCdcSubscription.Phase.ACTIVE) throw new IllegalArgumentException("CDC local phase differs from subscription state");
        }
    }
    public record Poll(DistributedCdcCheckpoint.Lineage lineage,Outcome outcome,Batch batch,int requiredBytes,DistributedCdcCheckpoint.Cursor floor,ApplyPosition cut) {
        public Poll {
            Objects.requireNonNull(lineage); Objects.requireNonNull(outcome); Objects.requireNonNull(floor); requireCommitted(cut);
            if(!DistributedCdcCheckpoint.Cursor.after(cut).covers(floor)) throw new IllegalArgumentException("CDC floor exceeds completed cut");
            if(outcome==Outcome.DATA) {
                if(batch==null || !batch.lineage().equals(lineage) || requiredBytes!=0 || !batch.cut().equals(cut) || !batch.from().covers(floor)) throw new IllegalArgumentException("Invalid CDC DATA outcome");
            } else if(batch!=null || outcome==Outcome.GAP && requiredBytes!=0 || outcome==Outcome.TOO_LARGE && (requiredBytes<=0 || requiredBytes>MAX_POLL_BYTES)) throw new IllegalArgumentException("Invalid CDC gap/budget outcome");
        }
    }
    public record Batch(UUID id,DistributedCdcSubscription.Definition definition,DistributedCdcCheckpoint.Lineage lineage,
                        DistributedCdcCheckpoint.Cursor from,DistributedCdcCheckpoint.Cursor next,ApplyPosition cut,List<Envelope> envelopes,DistributedCdcCheckpoint currentStarts) {
        public Batch(UUID id,DistributedCdcSubscription.Definition definition,DistributedCdcCheckpoint.Lineage lineage,
                     DistributedCdcCheckpoint.Cursor from,DistributedCdcCheckpoint.Cursor next,ApplyPosition cut,List<Envelope> envelopes) {
            this(id,definition,lineage,from,next,cut,envelopes,definition.start());
        }
        public Batch {
            Objects.requireNonNull(id); Objects.requireNonNull(definition); Objects.requireNonNull(lineage); Objects.requireNonNull(from); Objects.requireNonNull(next); requireCommitted(cut);
            Objects.requireNonNull(currentStarts); Objects.requireNonNull(envelopes); if(envelopes.size()>MAX_RECORDS) throw new IllegalArgumentException("CDC envelope count exceeds bound"); envelopes=List.copyOf(envelopes);
            var start=currentStarts.cursors().get(lineage); var end=DistributedCdcCheckpoint.Cursor.after(cut);
            if(start==null || !from.covers(start) || !next.covers(from) || !end.covers(next)) throw new IllegalArgumentException("CDC batch changed lineage or cursor bounds");
            var selected=new HashSet<>(definition.columns()); long bytes=BATCH_HEADER_BYTES+definition.encode().length+currentStarts.encode().length; long previous=-1; int events=0;
            for(var envelope:envelopes) {
                if(envelope.position().index()<=previous || !end.covers(DistributedCdcCheckpoint.Cursor.after(envelope.position())) || !next.covers(DistributedCdcCheckpoint.Cursor.after(envelope.position()))) throw new IllegalArgumentException("CDC projected envelopes changed order/cut"); previous=envelope.position().index();
                for(var event:envelope.events()) if(!selected.contains(event.mutation().columnId()) || from.covers(event.cursor()) || !next.covers(event.cursor())) throw new IllegalArgumentException("CDC event is unselected or outside batch cursor bounds");
                if(envelope.terminalBarrier()!=null) {
                    var freeze=envelope.terminalBarrier(); freeze.requireCut(cut); var header=freeze.header(); var parent=header.participant(TopologyChange.Stage.FREEZE,freeze.groupId());
                    if(!freeze.groupId().equals(lineage.groupId()) || !parent.rangeId().equals(lineage.rangeId()) || parent.rangeEpoch()!=lineage.rangeEpoch()
                            || !header.clusterId().equals(definition.clusterId()) || !header.databaseId().equals(definition.databaseId()) || !header.domainId().equals(definition.domainId())
                            || from.covers(DistributedCdcCheckpoint.Cursor.after(freeze.position()))) throw new IllegalArgumentException("CDC barrier differs from subscription lineage/namespace/frontier");
                }
                if(envelope.transaction()!=null) {
                    var transaction=envelope.transaction(); var participant=transaction.participant(); var decision=transaction.decision();
                    if(!transaction.participantGroup().equals(lineage.groupId()) || !participant.rangeId().equals(lineage.rangeId()) || participant.rangeEpoch()!=lineage.rangeEpoch()
                            || !decision.clusterId().equals(definition.clusterId()) || !decision.databaseId().equals(definition.databaseId()) || !decision.domainId().equals(definition.domainId())) throw new IllegalArgumentException("CDC transaction differs from subscription lineage/namespace");
                }
                events+=envelope.events().size(); bytes+=envelope.budgetBytes(); if(events>MAX_EVENTS || bytes>MAX_BATCH_BYTES) throw new IllegalArgumentException("CDC projected batch exceeds byte/event bound");
            }
            if(bytes>MAX_BATCH_BYTES) throw new IllegalArgumentException("CDC definition exceeds batch budget");
        }
        public int budgetBytes() { long bytes=BATCH_HEADER_BYTES+definition.encode().length+currentStarts.encode().length; for(var envelope:envelopes) bytes+=envelope.budgetBytes(); return Math.toIntExact(bytes); }
    }
    public record Envelope(ApplyPosition position,byte[] schemaDigest,OperationId operation,ParticipantFinalization transaction,List<DistributedCdcRecord.Event> events,NativeTopologyFreeze terminalBarrier) {
        public Envelope(ApplyPosition position,byte[] schemaDigest,OperationId operation,ParticipantFinalization transaction,List<DistributedCdcRecord.Event> events) {
            this(position,schemaDigest,operation,transaction,events,null);
        }
        public Envelope {
            requireCommitted(position); Objects.requireNonNull(schemaDigest); Objects.requireNonNull(events);
            if(schemaDigest.length!=32 || events.size()>MAX_EVENTS_PER_ENVELOPE) throw new IllegalArgumentException("Invalid CDC projected envelope bounds");
            if(terminalBarrier==null ? operation==null || events.isEmpty() : operation!=null || transaction!=null || !events.isEmpty()
                    || !position.equals(terminalBarrier.position()) || !Arrays.equals(schemaDigest,terminalBarrier.header().schemaDigest())
                    || terminalBarrier.encode().length>NativeTopologyFreeze.MAX_BYTES) throw new IllegalArgumentException("CDC envelope must contain exactly one qualified payload");
            schemaDigest=schemaDigest.clone(); events=List.copyOf(events); int ordinal=-1; long mutations=0;
            for(var event:events) {
                Objects.requireNonNull(event.cursor()); Objects.requireNonNull(event.mutation()); int current=event.cursor().mutationOrdinal();
                if(!event.cursor().position().equals(position) || current<=ordinal || current>=MAX_EVENTS_PER_ENVELOPE) throw new IllegalArgumentException("CDC event changed envelope position/ordinal"); ordinal=current;
                mutations+=mutationBytes(event.mutation()); if(mutations>DistributedCommandCodec.MAX_ENCODED_BYTES-DistributedCommandCodec.HEADER_BYTES) throw new IllegalArgumentException("CDC projected mutations exceed command byte bound");
            }
            if(transaction!=null) {
                var participant=transaction.participant(); var prepared=participant.prepared();
                if(transaction.decision().phase()!=TransactionStatus.Phase.COMMIT || !operation.equals(transaction.decision().operation()) || !Arrays.equals(schemaDigest,participant.schemaDigest())
                        || prepared==null || prepared.index()>=position.index() || prepared.term()>position.term()) throw new IllegalArgumentException("CDC transaction differs from committed envelope");
            }
        }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
        public int budgetBytes() { long bytes=ENVELOPE_HEADER_BYTES+(transaction==null ? 0 : transaction.encode().length)+(terminalBarrier==null ? 0 : terminalBarrier.encode().length); for(var event:events) bytes+=EVENT_ORDINAL_BYTES+mutationBytes(event.mutation()); return Math.toIntExact(bytes); }
        @Override public boolean equals(Object other) { return other instanceof Envelope envelope && position.equals(envelope.position) && Arrays.equals(schemaDigest,envelope.schemaDigest) && Objects.equals(operation,envelope.operation) && Objects.equals(transaction,envelope.transaction) && events.equals(envelope.events) && Objects.equals(terminalBarrier,envelope.terminalBarrier); }
        @Override public int hashCode() { return 31*Objects.hash(position,operation,transaction,events,terminalBarrier)+Arrays.hashCode(schemaDigest); }
    }
    private static void requireCommitted(ApplyPosition position) { Objects.requireNonNull(position); if(position.term()==0) throw new IllegalArgumentException("CDC evidence requires a committed cut"); }
    private static long mutationBytes(Mutation mutation) { return mutation.encodedSize(); }
}
