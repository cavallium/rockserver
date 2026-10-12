package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Current protected transaction-HOME CAS. Serialized ACK evidence requires a qualified native read at admission. */
public final class HomeRecovery {
    public enum Action { ABORT, ACK }
    public static final int MAGIC=0x52485243,MAX_BYTES=2*TransactionStatus.MAX_BYTES+512;
    public record Proof(UUID participant,ApplyPosition position,byte[] digest,boolean fenced,long minimumEpoch) {
        public Proof {
            Objects.requireNonNull(participant); Objects.requireNonNull(position); Objects.requireNonNull(digest);
            if (position.term()<=0 || (fenced ? minimumEpoch<=0 : position.index()<=0 || minimumEpoch!=0) || digest.length!=32) throw new IllegalArgumentException("Invalid HOME recovery proof"); digest=digest.clone();
        }
        public Proof(UUID participant,ApplyPosition position,byte[] digest) { this(participant,position,digest,false,0); }
        public static Proof from(ParticipantFenceProof proof) { return new Proof(proof.identity().participantGroup(),proof.position(),proof.identity().digest(),true,proof.minimumEpoch()); }
        @Override public byte[] digest() { return digest.clone(); }
        @Override public boolean equals(Object other) { return other instanceof Proof p && participant.equals(p.participant) && position.equals(p.position) && fenced==p.fenced && minimumEpoch==p.minimumEpoch && Arrays.equals(digest,p.digest); }
        @Override public int hashCode() { return Objects.hash(participant,position,fenced,minimumEpoch,Arrays.hashCode(digest)); }
        public static Proof from(ParticipantOutcome outcome) {
            // An ABORT may learn a late negative vote without changing its terminal effects or receipt.
            var vote=outcome.vote()!=null && outcome.vote().prepared() ? outcome.vote() : null;
            var stable=new ParticipantOutcome(outcome.finalization(),outcome.position(),vote,outcome.result());
            return new Proof(outcome.finalization().participantGroup(),outcome.position(),hash(stable.encode()));
        }
    }
    private final Action action;
    private final DistributedCommand command;
    private final TransactionStatus before,next;
    private final Proof proof;
    public HomeRecovery(Action action,DistributedCommand command,Proof proof) {
        this.action=Objects.requireNonNull(action); this.command=Objects.requireNonNull(command); this.proof=proof;
        if (command.kind()!=DistributedCommand.Kind.DATA || command.operationId().epoch()!=0 || command.acknowledgedThrough()!=-1 || command.acceptance()!=null || command.hasBucketData()
                || command.mutations().size()!=1 || command.predicates().size()!=1) throw new IllegalArgumentException("HOME recovery requires one protected existing-image CAS");
        var mutation=command.mutations().getFirst(); var predicate=command.predicates().getFirst();
        if (mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE || !mutation.columnId().equals(predicate.columnId()) || mutation.key().length!=16
                || !Arrays.equals(mutation.key(),predicate.key()) || predicate.expectedValue()==null) throw new IllegalArgumentException("Invalid HOME recovery CAS envelope");
        before=TransactionStatus.decode(predicate.expectedValue()); next=TransactionStatus.decode(mutation.value()); next.requireSuccessorOf(before);
        var key=ByteBuffer.allocate(16).putLong(before.transactionId().getMostSignificantBits()).putLong(before.transactionId().getLeastSignificantBits()).array();
        if (!Arrays.equals(key,mutation.key()) || command.operationId().equals(before.operation())) throw new IllegalArgumentException("HOME recovery key/business identity changed");
        if (action==Action.ABORT) {
            if (proof!=null || (before.phase()!=TransactionStatus.Phase.PREPARING && !before.reserved()) || !(before.reserved() ? before.abortReservation(next.noEffectOutcome()) : before.decide(TransactionStatus.Phase.ABORT)).equals(next)) throw new IllegalArgumentException("HOME recovery ABORT changed its transition");
        } else {
            if (proof==null || !before.terminal() || proof.fenced() && before.phase()!=TransactionStatus.Phase.ABORT || !before.finalized(proof.participant()).equals(next)) throw new IllegalArgumentException("HOME recovery ACK changed its participant/transition");
        }
        if (proof!=null && proof.fenced()) requireFence(new ParticipantFenceProof(new ParticipantIdentity(command.groupId(),command.rangeEpoch(),proof.participant(),before),proof.minimumEpoch(),proof.position()));
        if ((long)9+DistributedCommandCodec.encodedSize(command)+(proof==null ? 0 : 73)>MAX_BYTES) throw new IllegalArgumentException("HOME recovery exceeds byte bound");
    }
    public Action action() { return action; }
    public DistributedCommand command() { return command; }
    public TransactionStatus before() { return before; }
    public TransactionStatus next() { return next; }
    public Proof proof() { return proof; }
    public OperationId operation() { return command.operationId(); }
    public void requireOutcome(ParticipantOutcome actual) {
        if (action!=Action.ACK || proof.fenced() || actual==null || !proof.equals(Proof.from(actual)) || !actual.finalization().homeGroup().equals(command.groupId())
                || actual.finalization().homeEpoch()!=command.rangeEpoch() || !actual.finalization().decision().equals(before.terminalDecision())) throw new IllegalArgumentException("HOME ACK lacks its exact bound native terminal proof");
    }
    public void requireFence(ParticipantFenceProof actual) {
        if (action!=Action.ACK || !proof.fenced() || before.phase()!=TransactionStatus.Phase.ABORT || actual==null)
            throw new IllegalArgumentException("HOME ACK lacks qualified fenced absence");
        var identity=new ParticipantIdentity(command.groupId(),command.rangeEpoch(),proof.participant(),before);
        var frozen=new ParticipantFenceProof(identity,proof.minimumEpoch(),proof.position());
        if (!Arrays.equals(identity.digest(),proof.digest()) || !actual.covers(frozen)) throw new IllegalArgumentException("HOME ACK fence identity or cut changed");
    }
    public byte[] digest() { return hash(encode()); }
    public byte[] encode() {
        byte[] body=DistributedCommandCodec.encode(command); var out=ByteBuffer.allocate(9+body.length+(proof==null ? 0 : 73)).putInt(MAGIC).put((byte)(action==Action.ABORT ? 1 : 2)).putInt(body.length).put(body);
        if (proof!=null) out.put((byte)(proof.fenced() ? 1 : 0)).putLong(proof.participant().getMostSignificantBits()).putLong(proof.participant().getLeastSignificantBits()).putLong(proof.position().term()).putLong(proof.position().index()).put(proof.digest()).putLong(proof.minimumEpoch()); return out.array();
    }
    public static boolean isRecovery(byte[] bytes) { return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static HomeRecovery decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<9+DistributedCommandCodec.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid HOME recovery length");
        var in=ByteBuffer.wrap(bytes); if (in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid HOME recovery kind"); int action=in.get()&255,size=in.getInt();
        if (action<1 || action>2 || size<DistributedCommandCodec.HEADER_BYTES || size>DistributedCommandCodec.MAX_ENCODED_BYTES || in.remaining()!=size+(action==2 ? 73 : 0)) throw new IllegalArgumentException("Invalid HOME recovery body/proof size");
        byte[] command=new byte[size]; in.get(command); Proof proof=null;
        if (action==2) { int source=in.get()&255; if (source>1) throw new IllegalArgumentException("Invalid HOME proof source"); UUID participant=new UUID(in.getLong(),in.getLong()); var position=new ApplyPosition(in.getLong(),in.getLong()); byte[] digest=new byte[32]; in.get(digest); proof=new Proof(participant,position,digest,source==1,in.getLong()); }
        return new HomeRecovery(action==1 ? Action.ABORT : Action.ACK,DistributedCommandCodec.decode(command),proof);
    }
    @Override public boolean equals(Object other) { return other instanceof HomeRecovery r && Arrays.equals(encode(),r.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
    private static byte[] hash(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
