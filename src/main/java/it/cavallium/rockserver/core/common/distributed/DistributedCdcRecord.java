package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** One committed mutation envelope or original parent terminal boundary.
 * MERGE values are operands; mutation records exist only when the business effects succeed. */
public final class DistributedCdcRecord {
    public enum Kind { MUTATIONS, TERMINAL_BARRIER }
    public static final int HEADER_BYTES=65, MIN_BYTES=HEADER_BYTES;
    public static final int MAX_BYTES=HEADER_BYTES+Math.max(DistributedCommandCodec.MAX_ENCODED_BYTES+ParticipantFinalization.MAX_BYTES,NativeTopologyFreeze.MAX_BYTES);
    public record Event(DistributedCdcCheckpoint.Cursor cursor,Mutation mutation) {}
    private final Kind kind;
    private final DistributedCdcCheckpoint.Lineage lineage;
    private final ApplyPosition position;
    private final DistributedCommand command;
    private final ParticipantFinalization finalization;
    private final NativeTopologyFreeze barrier;

    public DistributedCdcRecord(DistributedCdcCheckpoint.Lineage lineage,ApplyPosition position,
            DistributedCommand command,ParticipantFinalization finalization) {
        kind=Kind.MUTATIONS; barrier=null;
        this.lineage=Objects.requireNonNull(lineage); this.position=Objects.requireNonNull(position);
        this.command=Objects.requireNonNull(command); this.finalization=finalization;
        if(position.term()==0 || !lineage.groupId().equals(command.groupId()) || lineage.rangeEpoch()!=command.rangeEpoch()
                || command.mutations().isEmpty() || command.kind()!=DistributedCommand.Kind.DATA && command.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED)
            throw new IllegalArgumentException("Invalid committed CDC command identity");
        if(finalization!=null) {
            var decision=finalization.decision(); var participant=finalization.participant(); var prepared=participant.prepared();
            if(command.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED || decision.phase()!=TransactionStatus.Phase.COMMIT
                    || !finalization.participantGroup().equals(lineage.groupId()) || !participant.rangeId().equals(lineage.rangeId())
                    || participant.rangeEpoch()!=lineage.rangeEpoch() || !command.operationId().equals(decision.operation())
                    || !Arrays.equals(participant.schemaDigest(),command.schemaDigest()) || !Arrays.equals(participant.planDigest(),command.payloadDigest())
                    || prepared==null || prepared.index()>=position.index() || prepared.term()>position.term())
                throw new IllegalArgumentException("CDC finalization differs from committed business plan");
        }
    }
    private DistributedCdcRecord(NativeTopologyFreeze freeze) {
        barrier=Objects.requireNonNull(freeze); kind=Kind.TERMINAL_BARRIER; command=null; finalization=null;
        var parent=freeze.header().participant(TopologyChange.Stage.FREEZE,freeze.groupId());
        lineage=new DistributedCdcCheckpoint.Lineage(parent.rangeId(),parent.groupId(),parent.rangeEpoch()); position=freeze.position();
    }
    public static DistributedCdcRecord terminalBarrier(NativeTopologyFreeze freeze) { return new DistributedCdcRecord(freeze); }
    public Kind kind() { return kind; }
    public DistributedCdcCheckpoint.Lineage lineage() { return lineage; }
    public ApplyPosition position() { return position; }
    public DistributedCommand command() { return command; }
    public ParticipantFinalization finalization() { return finalization; }
    public NativeTopologyFreeze barrier() { return barrier; }
    public byte[] schemaDigest() { return command==null ? barrier.header().schemaDigest() : command.schemaDigest(); }
    public int mutationCount() { return command==null ? 0 : command.mutations().size(); }
    public DistributedCdcCheckpoint.Cursor end() { return DistributedCdcCheckpoint.Cursor.after(position); }
    public List<Event> events() {
        if(kind==Kind.TERMINAL_BARRIER) return List.of();
        return java.util.stream.IntStream.range(0,command.mutations().size())
                .mapToObj(i->new Event(new DistributedCdcCheckpoint.Cursor(position,i),command.mutations().get(i))).toList();
    }
    public int encodedSize() { return HEADER_BYTES+(barrier==null ? DistributedCommandCodec.encodedSize(command)+(finalization==null ? 0 : finalization.encode().length) : barrier.encode().length); }
    public byte[] encode() {
        byte[] payload=barrier==null ? DistributedCommandCodec.encode(command) : barrier.encode(),decision=finalization==null ? new byte[0] : finalization.encode();
        var bytes=ByteBuffer.allocate(HEADER_BYTES+payload.length+decision.length).put((byte)kind.ordinal());
        uuid(bytes,lineage.rangeId()); uuid(bytes,lineage.groupId()); bytes.putLong(lineage.rangeEpoch());
        bytes.putLong(position.term()).putLong(position.index()).putInt(payload.length).putInt(decision.length).put(payload).put(decision);
        return bytes.array();
    }
    public static DistributedCdcRecord decode(byte[] encoded) {
        Objects.requireNonNull(encoded);
        if(encoded.length<MIN_BYTES || encoded.length>MAX_BYTES) throw new IllegalArgumentException("Invalid CDC record length");
        var bytes=ByteBuffer.wrap(encoded); int tag=bytes.get()&255;
        if(tag>=Kind.values().length) throw new IllegalArgumentException("Unknown CDC record kind"); var kind=Kind.values()[tag];
        var lineage=new DistributedCdcCheckpoint.Lineage(uuid(bytes),uuid(bytes),bytes.getLong());
        var position=new ApplyPosition(bytes.getLong(),bytes.getLong()); int payloadSize=bytes.getInt(),decisionSize=bytes.getInt();
        int minimum=kind==Kind.MUTATIONS ? DistributedCommandCodec.HEADER_BYTES : 1;
        int maximum=kind==Kind.MUTATIONS ? DistributedCommandCodec.MAX_ENCODED_BYTES : NativeTopologyFreeze.MAX_BYTES;
        if(payloadSize<minimum || payloadSize>maximum || decisionSize<0 || decisionSize>ParticipantFinalization.MAX_BYTES
                || kind==Kind.TERMINAL_BARRIER && decisionSize!=0 || (long)payloadSize+decisionSize!=bytes.remaining())
            throw new IllegalArgumentException("Invalid CDC record body lengths");
        byte[] payload=new byte[payloadSize]; bytes.get(payload); byte[] decision=new byte[decisionSize]; bytes.get(decision);
        final DistributedCdcRecord result;
        if(kind==Kind.MUTATIONS) result=new DistributedCdcRecord(lineage,position,DistributedCommandCodec.decode(payload),decisionSize==0 ? null : ParticipantFinalization.decode(decision));
        else {
            result=terminalBarrier(NativeTopologyFreeze.decode(payload));
            if(!lineage.equals(result.lineage) || !position.equals(result.position)) throw new IllegalArgumentException("CDC terminal barrier differs from original parent lineage/cut");
        }
        if(!Arrays.equals(encoded,result.encode())) throw new IllegalArgumentException("Noncanonical CDC record"); return result;
    }
    private static void uuid(ByteBuffer bytes,UUID id) { bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer bytes) { return new UUID(bytes.getLong(),bytes.getLong()); }
}
