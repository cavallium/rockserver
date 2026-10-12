package it.cavallium.rockserver.core.common.cdc;

import it.cavallium.rockserver.core.common.api.cdc.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import it.cavallium.rockserver.core.common.api.cdc.CdcBatch;
import java.util.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TMemoryBuffer;

/** Current projected CDC transport; no original command, predicates, or unselected events are sent. */
public final class ThriftDistributedCdcWire {
    public static final int MAX_RECORDS=DistributedCdcSubscriptions.MAX_RECORDS;
    public static final int MIN_SOURCE_BYTES=111;
    public static final int MAX_SOURCE_BYTES=DistributedCdcSubscriptions.MAX_POLL_BYTES;
    public static final int MAX_PROJECTED_BYTES=DistributedCdcSubscriptions.MAX_BATCH_BYTES;
    private ThriftDistributedCdcWire() {}
    public static ByteBuffer uuid(UUID id) { return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).flip(); }
    public static UUID uuid(ByteBuffer id) { if(id==null || id.remaining()!=16) throw new IllegalArgumentException("UUID must contain 16 bytes"); var b=id.duplicate(); return new UUID(b.getLong(),b.getLong()); }
    private static byte[] bytes(ByteBuffer value,int maximum) {
        if(value==null || value.remaining()>maximum) throw new IllegalArgumentException("CDC binary exceeds bound");
        var result=new byte[value.remaining()]; value.duplicate().get(result); return result;
    }
    public static ByteBuffer definition(DistributedCdcSubscription.Definition value) { return ByteBuffer.wrap(value.encode()); }
    public static DistributedCdcSubscription.Definition definition(ByteBuffer value) { return DistributedCdcSubscription.Definition.decode(bytes(value,DistributedCdcSubscription.Definition.MAX_BYTES)); }
    public static CdcContext directory(RangeDirectory value,UUID domain) {
        if(!value.domainId().equals(domain)) throw new IllegalArgumentException("Wrong CDC directory domain");
        return new CdcContext(ByteBuffer.wrap(RangeDirectoryCodec.encode(value)));
    }
    public static RangeDirectory directory(CdcContext value,UUID domain) {
        var result=RangeDirectoryCodec.decode(bytes(value.directory,RangeDirectoryCodec.MAX_ENCODED_BYTES));
        if(!result.domainId().equals(domain)) throw new IllegalArgumentException("Wrong CDC directory domain"); return result;
    }
    private static CdcPosition position(ApplyPosition v) { return new CdcPosition(v.term(),v.index()); }
    private static ApplyPosition position(CdcPosition v) { Objects.requireNonNull(v); return new ApplyPosition(v.term,v.index); }
    private static CdcCursor cursor(DistributedCdcCheckpoint.Cursor v) { return new CdcCursor(position(v.position()),v.mutationOrdinal()); }
    private static DistributedCdcCheckpoint.Cursor cursor(CdcCursor v) { Objects.requireNonNull(v); return new DistributedCdcCheckpoint.Cursor(position(v.position),v.ordinal); }
    private static CdcLineage lineage(DistributedCdcCheckpoint.Lineage v) { return new CdcLineage(uuid(v.rangeId()),uuid(v.groupId()),v.rangeEpoch()); }
    private static DistributedCdcCheckpoint.Lineage lineage(CdcLineage v) { Objects.requireNonNull(v); return new DistributedCdcCheckpoint.Lineage(uuid(v.rangeId),uuid(v.groupId),v.rangeEpoch); }
    private static CdcOperation operation(OperationId v) { return new CdcOperation(uuid(v.sessionId()),v.epoch(),v.sequence()); }
    private static OperationId operation(CdcOperation v) { Objects.requireNonNull(v); return new OperationId(uuid(v.sessionId),v.epoch,v.sequence); }
    public static CdcStatus status(DistributedCdcSubscriptions.Status value,UUID subscription) {
        requireSubscription(value.state().definition(),subscription);
        return new CdcStatus(ByteBuffer.wrap(value.state().encode()),CdcPhase.valueOf(value.phase().name()));
    }
    public static DistributedCdcSubscriptions.Status status(CdcStatus value,UUID subscription) {
        var state=DistributedCdcSubscription.decode(bytes(value.state,DistributedCdcSubscription.MAX_BYTES));
        requireSubscription(state.definition(),subscription);
        return new DistributedCdcSubscriptions.Status(state,DistributedCdcSubscriptions.Phase.valueOf(value.phase.name()));
    }
    public static void requireDefinition(DistributedCdcSubscriptions.Status value,DistributedCdcSubscription.Definition definition) {
        if(!value.state().definition().equals(definition)) throw new IllegalArgumentException("CDC create changed immutable definition");
    }
    private static void requireSubscription(DistributedCdcSubscription.Definition value,UUID subscription) {
        if(!value.subscriptionId().equals(subscription)) throw new IllegalArgumentException("Wrong CDC subscription identity");
    }
    private static CdcMutation mutation(Mutation value) {
        var result=new CdcMutation().setColumnId(uuid(value.columnId())).setKind(CdcMutationKind.valueOf(value.kind().name()))
                .setReturnMode(CdcReturnMode.valueOf(value.returnMode().name())).setKey(value.key());
        if(value.value()!=null) result.setValue(value.value());
        if(value.bucket()) result.setMemberKeys(value.memberKeys().stream().map(ByteBuffer::wrap).toList());
        return result;
    }
    private static long mutationBudget(CdcMutation value) {
        Objects.requireNonNull(value); Objects.requireNonNull(value.kind); Objects.requireNonNull(value.returnMode); Objects.requireNonNull(value.key);
        uuid(value.columnId);
        boolean bucket=switch(value.kind) { case BUCKET_PUT,BUCKET_DELETE,BUCKET_MERGE -> true; default -> false; };
        boolean deletion=value.kind==CdcMutationKind.DELETE || value.kind==CdcMutationKind.BUCKET_DELETE;
        if(deletion ? value.value!=null : value.value==null) throw new IllegalArgumentException("Invalid CDC mutation value presence");
        if(bucket!=(value.memberKeys!=null)) throw new IllegalArgumentException("Invalid CDC bucket member presence");
        long size=26L+value.key.remaining()+(value.value==null?0:value.value.remaining());
        if(bucket) {
            if(value.memberKeys.isEmpty() || value.memberKeys.size()>DistributedSchemaCodec.MAX_KEY_COMPONENTS) throw new IllegalArgumentException("Invalid CDC bucket component count");
            size+=4;
            for(var member:value.memberKeys) {
                if(member==null || member.remaining()>65535) throw new IllegalArgumentException("Invalid CDC bucket component length"); size+=2L+member.remaining();
            }
        }
        if(size>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("CDC mutation exceeds bound"); return size;
    }
    private static Mutation mutation(CdcMutation value) {
        mutationBudget(value);
        var kind=Mutation.Kind.valueOf(value.kind.name()); var mode=Mutation.ReturnMode.valueOf(value.returnMode.name());
        var column=uuid(value.columnId); var key=bytes(value.key,DistributedCommandCodec.MAX_ENCODED_BYTES);
        var data=value.value==null?null:bytes(value.value,DistributedCommandCodec.MAX_ENCODED_BYTES);
        if(value.memberKeys==null) return new Mutation(kind,column,key,data,mode);
        var members=value.memberKeys.stream().map(member->bytes(member,65535)).toList();
        return switch(kind) {
            case BUCKET_PUT -> Mutation.bucketPut(column,key,members,data,mode);
            case BUCKET_DELETE -> Mutation.bucketDelete(column,key,members,mode);
            case BUCKET_MERGE -> Mutation.bucketMerge(column,key,members,data,mode);
            default -> throw new IllegalArgumentException("Invalid CDC bucket mutation");
        };
    }
    public static CdcPoll poll(DistributedCdcSubscriptions.Poll value,UUID subscription,int maxRecords,int maxBytes) {
        requirePollLimits(maxRecords,maxBytes);
        var result=new CdcPoll().setLineage(lineage(value.lineage())).setOutcome(CdcOutcome.valueOf(value.outcome().name()))
                .setRequiredBytes(value.requiredBytes()).setFloor(cursor(value.floor())).setCut(position(value.cut()));
        if(value.batch()!=null) {
            var batch=value.batch(); requireSubscription(batch.definition(),subscription);
            if(batch.envelopes().size()>MAX_RECORDS) throw new IllegalArgumentException("CDC response exceeds global record bound");
            var envelopes=new ArrayList<CdcEnvelope>(batch.envelopes().size());
            for(var envelope:batch.envelopes()) {
                var projected=new CdcEnvelope().setPosition(position(envelope.position())).setSchemaDigest(envelope.schemaDigest())
                        .setEvents(envelope.events().stream().map(e->new CdcEvent(e.cursor().mutationOrdinal(),mutation(e.mutation()))).toList());
                if(envelope.operation()!=null) projected.setOperation(operation(envelope.operation()));
                if(envelope.terminalBarrier()!=null) projected.setTerminalBarrier(envelope.terminalBarrier().encode());
                if(envelope.transaction()!=null) projected.setFinalization(envelope.transaction().encode()); envelopes.add(projected);
            }
            result.setBatch(new CdcBatch(uuid(batch.id()),definition(batch.definition()),lineage(batch.lineage()),cursor(batch.from()),cursor(batch.next()),position(batch.cut()),envelopes,ByteBuffer.wrap(batch.currentStarts().encode())));
        }
        return result;
    }
    public static DistributedCdcSubscriptions.Poll poll(CdcPoll value,UUID subscription,int maxRecords,int maxBytes) {
        requirePollLimits(maxRecords,maxBytes); Objects.requireNonNull(value.outcome);
        var identity=lineage(value.lineage); var floor=cursor(value.floor); var cut=position(value.cut);
        DistributedCdcSubscriptions.Batch batch=null;
        if(value.batch!=null) {
            var wire=value.batch;
            if(value.outcome!=CdcOutcome.DATA) throw new IllegalArgumentException("Non-DATA response carried CDC batch");
            var definition=definition(wire.definition); requireSubscription(definition,subscription);
            if(wire.envelopes==null || wire.envelopes.size()>MAX_RECORDS) throw new IllegalArgumentException("CDC envelope count exceeds global bound");
            var starts=DistributedCdcCheckpoint.decode(bytes(wire.currentStarts,DistributedCdcCheckpoint.MAX_BYTES));
            var selected=new HashSet<>(definition.columns()); long budget=DistributedCdcSubscriptions.BATCH_HEADER_BYTES+wire.definition.remaining()+wire.currentStarts.remaining(); long totalEvents=0;
            // Check the entire projected body before copying binary mutation data or allocating event lists.
            for(var envelope:wire.envelopes) {
                if(envelope.events==null || envelope.events.size()>DistributedCdcSubscriptions.MAX_EVENTS_PER_ENVELOPE
                        || envelope.schemaDigest==null || envelope.schemaDigest.remaining()!=32
                        || envelope.finalization!=null && envelope.finalization.remaining()>ParticipantFinalization.MAX_BYTES)
                    throw new IllegalArgumentException("Invalid CDC envelope bounds");
                if(envelope.terminalBarrier==null ? envelope.operation==null || envelope.events.isEmpty() : envelope.operation!=null || envelope.finalization!=null || !envelope.events.isEmpty()
                        || envelope.terminalBarrier.remaining()>NativeTopologyFreeze.MAX_BYTES) throw new IllegalArgumentException("Mixed or missing CDC envelope payload");
                totalEvents+=envelope.events.size(); long mutations=0;
                budget+=DistributedCdcSubscriptions.ENVELOPE_HEADER_BYTES+(envelope.finalization==null?0:envelope.finalization.remaining())+(envelope.terminalBarrier==null?0:envelope.terminalBarrier.remaining());
                for(var event:envelope.events) {
                    if(!selected.contains(uuid(event.mutation.columnId))) throw new IllegalArgumentException("Unselected CDC column");
                    long size=mutationBudget(event.mutation); mutations+=size; budget+=DistributedCdcSubscriptions.EVENT_ORDINAL_BYTES+size;
                }
                if(mutations>DistributedCommandCodec.MAX_ENCODED_BYTES-DistributedCommandCodec.HEADER_BYTES || totalEvents>DistributedCdcSubscriptions.MAX_EVENTS || budget>MAX_PROJECTED_BYTES)
                    throw new IllegalArgumentException("CDC projected batch exceeds bounds");
            }
            var envelopes=new ArrayList<DistributedCdcSubscriptions.Envelope>(wire.envelopes.size());
            for(var envelope:wire.envelopes) {
                var position=position(envelope.position);
                var events=envelope.events.stream().map(e->new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(position,e.ordinal),mutation(e.mutation))).toList();
                var transaction=envelope.finalization==null?null:ParticipantFinalization.decode(bytes(envelope.finalization,ParticipantFinalization.MAX_BYTES));
                var barrier=envelope.terminalBarrier==null?null:NativeTopologyFreeze.decode(bytes(envelope.terminalBarrier,NativeTopologyFreeze.MAX_BYTES));
                envelopes.add(new DistributedCdcSubscriptions.Envelope(position,bytes(envelope.schemaDigest,32),envelope.operation==null?null:operation(envelope.operation),transaction,events,barrier));
            }
            batch=new DistributedCdcSubscriptions.Batch(uuid(wire.batchId),definition,lineage(wire.lineage),cursor(wire.fromCursor),cursor(wire.nextCursor),position(wire.cut),envelopes,starts);
        }
        return new DistributedCdcSubscriptions.Poll(identity,DistributedCdcSubscriptions.Outcome.valueOf(value.outcome.name()),batch,value.requiredBytes,floor,cut);
    }
    public static void requirePollLimits(int records,int bytes) {
        if(records<1 || records>MAX_RECORDS || bytes<MIN_SOURCE_BYTES || bytes>MAX_SOURCE_BYTES) throw new IllegalArgumentException("Invalid CDC poll limits");
    }
    /** Exact binary DTO size without allocating a serialized response frame. */
    public static long serializedSize(org.apache.thrift.TBase<?,?> value) throws TException {
        class Counter extends TMemoryBuffer {
            long count;
            Counter() throws org.apache.thrift.transport.TTransportException { super(0); }
            @Override public void write(byte[] bytes,int offset,int length) { count=Math.addExact(count,length); }
        }
        var counter=new Counter(); value.write(new TBinaryProtocol(counter)); return counter.count;
    }
    /** Bound binary fields and collection counts before generated readers allocate their contents. */
    public static TProtocol boundedProtocol(TProtocol protocol,boolean request) {
        return new TProtocolDecorator(protocol) {
            private short field;
            @Override public TField readFieldBegin() throws TException { var value=super.readFieldBegin(); field=value.id; return value; }
            @Override public ByteBuffer readBinary() throws TException {
                int length=readI32();
                int maximum=request?switch(field) { case 1,2,3,4,5 -> 16; case 24 -> DistributedCdcSubscription.Definition.MAX_BYTES; default -> 0; }:switch(field) {
                    case 1,2,3,4,5 -> 16;
                    case 9 -> NativeTopologyFreeze.MAX_BYTES;
                    case 20 -> 32;
                    case 21,22 -> DistributedCommandCodec.MAX_ENCODED_BYTES;
                    case 23 -> DistributedCdcSubscription.MAX_BYTES;
                    case 24 -> DistributedCdcSubscription.Definition.MAX_BYTES;
                    case 25 -> RangeDirectoryCodec.MAX_ENCODED_BYTES;
                    case 26 -> ParticipantFinalization.MAX_BYTES;
                    case 27 -> DistributedCdcCheckpoint.MAX_BYTES;
                    case 32 -> 65535;
                    default -> DistributedCdcSubscription.Definition.MAX_BYTES;
                };
                if(length<0 || length>maximum) throw new TProtocolException(TProtocolException.SIZE_LIMIT,"CDC binary exceeds field bound");
                getTransport().checkReadBytesAvailable(length);
                byte[] bytes=new byte[length]; getTransport().readAll(bytes,0,length); return ByteBuffer.wrap(bytes);
            }
            @Override public TList readListBegin() throws TException {
                var list=super.readListBegin();
                int maximum=request?0:switch(field) { case 30 -> MAX_RECORDS; case 31 -> DistributedCdcSubscriptions.MAX_EVENTS_PER_ENVELOPE; case 32 -> DistributedSchemaCodec.MAX_KEY_COMPONENTS; default -> 0; };
                if(list.size<0 || list.size>maximum) throw new TProtocolException(TProtocolException.SIZE_LIMIT,"CDC collection exceeds bound"); return list;
            }
            @Override public TMap readMapBegin() throws TException { throw new TProtocolException(TProtocolException.INVALID_DATA,"CDC has no map fields"); }
            @Override public TSet readSetBegin() throws TException { throw new TProtocolException(TProtocolException.INVALID_DATA,"CDC has no set fields"); }
        };
    }
}
