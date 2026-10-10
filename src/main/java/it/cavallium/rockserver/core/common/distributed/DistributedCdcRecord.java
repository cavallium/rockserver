package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** One committed mutation envelope. MERGE values are operands, including bucket-member operands.
 * The native apply owner creates records only when the business effects succeed. */
public final class DistributedCdcRecord {
    public static final int HEADER_BYTES=64;
    public static final int MAX_BYTES=HEADER_BYTES+DistributedCommandCodec.MAX_ENCODED_BYTES+ParticipantFinalization.MAX_BYTES;
    public record Event(DistributedCdcCheckpoint.Cursor cursor,Mutation mutation) {}
    private final DistributedCdcCheckpoint.Lineage lineage;
    private final ApplyPosition position;
    private final DistributedCommand command;
    private final ParticipantFinalization finalization;

    public DistributedCdcRecord(DistributedCdcCheckpoint.Lineage lineage,ApplyPosition position,
            DistributedCommand command,ParticipantFinalization finalization) {
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
    public DistributedCdcCheckpoint.Lineage lineage() { return lineage; }
    public ApplyPosition position() { return position; }
    public DistributedCommand command() { return command; }
    public ParticipantFinalization finalization() { return finalization; }
    public DistributedCdcCheckpoint.Cursor end() { return DistributedCdcCheckpoint.Cursor.after(position); }
    public List<Event> events() {
        return java.util.stream.IntStream.range(0,command.mutations().size())
                .mapToObj(i->new Event(new DistributedCdcCheckpoint.Cursor(position,i),command.mutations().get(i))).toList();
    }
    public int encodedSize() { return HEADER_BYTES+DistributedCommandCodec.encodedSize(command)+(finalization==null ? 0 : finalization.encode().length); }
    public byte[] encode() {
        byte[] business=DistributedCommandCodec.encode(command),decision=finalization==null ? new byte[0] : finalization.encode();
        var bytes=ByteBuffer.allocate(HEADER_BYTES+business.length+decision.length);
        uuid(bytes,lineage.rangeId()); uuid(bytes,lineage.groupId()); bytes.putLong(lineage.rangeEpoch());
        bytes.putLong(position.term()).putLong(position.index()).putInt(business.length).putInt(decision.length).put(business).put(decision);
        return bytes.array();
    }
    public static DistributedCdcRecord decode(byte[] encoded) {
        Objects.requireNonNull(encoded);
        if(encoded.length<HEADER_BYTES+DistributedCommandCodec.HEADER_BYTES || encoded.length>MAX_BYTES) throw new IllegalArgumentException("Invalid CDC record length");
        var bytes=ByteBuffer.wrap(encoded);
        var lineage=new DistributedCdcCheckpoint.Lineage(uuid(bytes),uuid(bytes),bytes.getLong());
        var position=new ApplyPosition(bytes.getLong(),bytes.getLong()); int businessSize=bytes.getInt(),decisionSize=bytes.getInt();
        if(businessSize<DistributedCommandCodec.HEADER_BYTES || businessSize>DistributedCommandCodec.MAX_ENCODED_BYTES
                || decisionSize<0 || decisionSize>ParticipantFinalization.MAX_BYTES || (long)businessSize+decisionSize!=bytes.remaining())
            throw new IllegalArgumentException("Invalid CDC record body lengths");
        byte[] business=new byte[businessSize]; bytes.get(business); byte[] decision=new byte[decisionSize]; bytes.get(decision);
        return new DistributedCdcRecord(lineage,position,DistributedCommandCodec.decode(business),decisionSize==0 ? null : ParticipantFinalization.decode(decision));
    }
    private static void uuid(ByteBuffer bytes,UUID id) { bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer bytes) { return new UUID(bytes.getLong(),bytes.getLong()); }
}
