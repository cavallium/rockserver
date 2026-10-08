package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Immutable home record. Vote positions are durable participant evidence supplied by the coordinator, not self-verifying proofs. */
public final class TransactionStatus {
    public enum Phase { PREPARING, COMMIT, ABORT }
    public static final int MAX_PARTICIPANTS=64, HEADER_BYTES=182, PARTICIPANT_BYTES=121;
    public static final int MAX_BYTES=HEADER_BYTES+MAX_PARTICIPANTS*PARTICIPANT_BYTES;
    public record Participant(UUID rangeId,UUID groupId,long rangeEpoch,byte[] schemaDigest,byte[] planDigest,ApplyPosition prepared,boolean finalized) {
        public Participant {
            Objects.requireNonNull(rangeId); Objects.requireNonNull(groupId);
            if (rangeEpoch<0 || schemaDigest.length!=32 || planDigest.length!=32 || prepared!=null && (prepared.term()<=0 || prepared.index()<=0)) throw invalid("Invalid participant identity/vote");
            schemaDigest=schemaDigest.clone(); planDigest=planDigest.clone();
        }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
        @Override public byte[] planDigest() { return planDigest.clone(); }
        @Override public boolean equals(Object other) { return other instanceof Participant p && rangeId.equals(p.rangeId) && groupId.equals(p.groupId) && rangeEpoch==p.rangeEpoch && Arrays.equals(schemaDigest,p.schemaDigest) && Arrays.equals(planDigest,p.planDigest) && Objects.equals(prepared,p.prepared) && finalized==p.finalized; }
        @Override public int hashCode() { return Objects.hash(rangeId,groupId,rangeEpoch,Arrays.hashCode(schemaDigest),Arrays.hashCode(planDigest),prepared,finalized); }
        boolean sameDefinition(Participant p) { return rangeId.equals(p.rangeId) && groupId.equals(p.groupId) && rangeEpoch==p.rangeEpoch && Arrays.equals(schemaDigest,p.schemaDigest) && Arrays.equals(planDigest,p.planDigest); }
    }
    private final UUID clusterId,databaseId,domainId,transactionId;
    private final OperationId operation;
    private final long schemaEpoch,generation;
    private final byte[] schemaDigest,planDigest;
    private final Phase phase;
    private final List<Participant> participants;
    private final boolean consumed;
    public static TransactionStatus begin(UUID cluster,UUID database,UUID domain,UUID transaction,OperationId operation,long schemaEpoch,byte[] schemaDigest,byte[] planDigest,List<Participant> participants) {
        var status=new TransactionStatus(cluster,database,domain,transaction,operation,schemaEpoch,schemaDigest,planDigest,0,Phase.PREPARING,participants,false);
        status.requireInitial(); return status;
    }
    private TransactionStatus(UUID cluster,UUID database,UUID domain,UUID transaction,OperationId operation,long schemaEpoch,byte[] schemaDigest,byte[] planDigest,long generation,Phase phase,List<Participant> participants,boolean consumed) {
        clusterId=Objects.requireNonNull(cluster); databaseId=Objects.requireNonNull(database); domainId=Objects.requireNonNull(domain); transactionId=Objects.requireNonNull(transaction); this.operation=Objects.requireNonNull(operation);
        if (schemaEpoch<0 || generation<0 || schemaDigest.length!=32 || planDigest.length!=32 || participants.isEmpty() || participants.size()>MAX_PARTICIPANTS) throw invalid("Invalid transaction status identity/bounds");
        this.schemaEpoch=schemaEpoch; this.schemaDigest=schemaDigest.clone(); this.planDigest=planDigest.clone(); this.generation=generation; this.phase=Objects.requireNonNull(phase); this.consumed=consumed;
        this.participants=participants.stream().sorted((a,b) -> compare(a.groupId(),b.groupId())).toList();
        var groups=new HashSet<UUID>(); var ranges=new HashSet<UUID>();
        for (var p:this.participants) if (!groups.add(p.groupId()) || !ranges.add(p.rangeId())) throw invalid("Duplicate transaction participant");
        if (phase==Phase.PREPARING && (consumed || this.participants.stream().anyMatch(Participant::finalized))
                || phase==Phase.COMMIT && this.participants.stream().anyMatch(p -> p.prepared()==null)
                || consumed && !fullyFinalized()) throw invalid("Invalid decision/finalization shape");
    }
    public UUID clusterId() { return clusterId; } public UUID databaseId() { return databaseId; } public UUID domainId() { return domainId; }
    public UUID transactionId() { return transactionId; } public OperationId operation() { return operation; }
    public long schemaEpoch() { return schemaEpoch; } public byte[] schemaDigest() { return schemaDigest.clone(); } public byte[] planDigest() { return planDigest.clone(); }
    public long generation() { return generation; } public Phase phase() { return phase; } public List<Participant> participants() { return participants; } public boolean consumed() { return consumed; }
    /** Frozen definition comparison ignores votes, decision, generation, ACKs and consumption. */
    public boolean sameDefinition(TransactionStatus other) {
        if (other==null || !clusterId.equals(other.clusterId) || !databaseId.equals(other.databaseId) || !domainId.equals(other.domainId)
                || !transactionId.equals(other.transactionId) || !operation.equals(other.operation) || schemaEpoch!=other.schemaEpoch
                || !Arrays.equals(schemaDigest,other.schemaDigest) || !Arrays.equals(planDigest,other.planDigest) || participants.size()!=other.participants.size()) return false;
        for (int i=0;i<participants.size();i++) if (!participants.get(i).sameDefinition(other.participants.get(i))) return false;
        return true;
    }
    /** Canonical frozen definition, excluding votes and recovery progress. */
    public TransactionStatus asInitial() {
        return begin(clusterId,databaseId,domainId,transactionId,operation,schemaEpoch,schemaDigest,planDigest,
                participants.stream().map(p -> new Participant(p.rangeId(),p.groupId(),p.rangeEpoch(),p.schemaDigest(),p.planDigest(),null,false)).toList());
    }
    /** Stable chosen decision. Mutable recovery progress is not part of a participant's finalization identity. */
    public TransactionStatus terminalDecision() {
        if (phase==Phase.PREPARING) throw invalid("Home has no terminal decision");
        var frozen=participants.stream().map(p -> new Participant(p.rangeId(),p.groupId(),p.rangeEpoch(),p.schemaDigest(),p.planDigest(),p.prepared(),false)).toList();
        return new TransactionStatus(clusterId,databaseId,domainId,transactionId,operation,schemaEpoch,schemaDigest,planDigest,0,phase,frozen,false);
    }
    public boolean fullyFinalized() { return phase!=Phase.PREPARING && participants.stream().allMatch(Participant::finalized); }
    public boolean gcEligible() { return fullyFinalized() && consumed; }
    public void requireInitial() { if (generation!=0 || phase!=Phase.PREPARING || consumed || participants.stream().anyMatch(p -> p.prepared()!=null || p.finalized())) throw invalid("Initial status must freeze an unvoted participant set"); }
    public TransactionStatus prepared(UUID group,byte[] participantPlan,ApplyPosition position) {
        if (phase!=Phase.PREPARING) throw invalid("Decision already chosen"); Objects.requireNonNull(position);
        var list=new ArrayList<>(participants); int i=index(group); var p=list.get(i);
        if (!Arrays.equals(p.planDigest(),participantPlan)) throw invalid("Prepared vote changed participant plan");
        if (p.prepared()!=null) { if (!p.prepared().equals(position)) throw invalid("Prepared evidence changed"); return this; }
        list.set(i,new Participant(p.rangeId(),p.groupId(),p.rangeEpoch(),p.schemaDigest(),p.planDigest(),position,false)); return next(phase,list,false);
    }
    public TransactionStatus decide(Phase decision) {
        if (decision==Phase.PREPARING) throw invalid("Expected terminal decision");
        if (phase!=Phase.PREPARING) { if (phase!=decision) throw invalid("Transaction decision is immutable"); return this; }
        return next(decision,participants,false);
    }
    public TransactionStatus finalized(UUID group) {
        if (phase==Phase.PREPARING) throw invalid("Finalization requires a decision");
        var list=new ArrayList<>(participants); int i=index(group); var p=list.get(i); if (p.finalized()) return this;
        list.set(i,new Participant(p.rangeId(),p.groupId(),p.rangeEpoch(),p.schemaDigest(),p.planDigest(),p.prepared(),true)); return next(phase,list,consumed);
    }
    public TransactionStatus consume() { if (!fullyFinalized()) throw invalid("Consumption requires every participant finalized"); return consumed ? this : next(phase,participants,true); }
    private int index(UUID group) { for (int i=0;i<participants.size();i++) if (participants.get(i).groupId().equals(group)) return i; throw invalid("Unregistered participant"); }
    private TransactionStatus next(Phase next,List<Participant> participants,boolean consumed) {
        if (generation==Long.MAX_VALUE) throw invalid("Transaction status generation exhausted");
        return new TransactionStatus(clusterId,databaseId,domainId,transactionId,operation,schemaEpoch,schemaDigest,planDigest,generation+1,next,participants,consumed);
    }
    /** Validate a single monotonic transition before ordered expected-image CAS. */
    public void requireSuccessorOf(TransactionStatus old) {
        Objects.requireNonNull(old);
        if (!sameDefinition(old) || old.generation==Long.MAX_VALUE || generation!=old.generation+1) throw invalid("Transaction definition/generation changed");
        int votes=0,acks=0;
        for (int i=0;i<participants.size();i++) {
            var p=participants.get(i); var before=old.participants.get(i);
            if (!Objects.equals(p.prepared(),before.prepared())) { if (before.prepared()!=null || p.prepared()==null) throw invalid("Prepared evidence changed"); votes++; }
            if (p.finalized()!=before.finalized()) { if (before.finalized()) throw invalid("Finalization regressed"); acks++; }
        }
        boolean valid=old.phase==Phase.PREPARING
                ? consumed==old.consumed && acks==0 && (phase==Phase.PREPARING ? votes==1 : votes==0)
                : phase==old.phase && votes==0 && (consumed==old.consumed ? acks==1 : !old.consumed && consumed && acks==0 && fullyFinalized());
        if (!valid) throw invalid("Illegal transaction status transition");
    }
    public byte[] encode() {
        var b=ByteBuffer.allocate(HEADER_BYTES+participants.size()*PARTICIPANT_BYTES);
        uuid(b,clusterId); uuid(b,databaseId); uuid(b,domainId); uuid(b,transactionId); uuid(b,operation.sessionId());
        b.putLong(operation.epoch()).putLong(operation.sequence()).putLong(schemaEpoch).put(schemaDigest).put(planDigest).putLong(generation).put((byte)phase.ordinal()).put((byte)(consumed ? 1 : 0)).putInt(participants.size());
        for (var p:participants) { uuid(b,p.rangeId()); uuid(b,p.groupId()); b.putLong(p.rangeEpoch()).put(p.schemaDigest()).put(p.planDigest()).putLong(p.prepared()==null ? 0 : p.prepared().term()).putLong(p.prepared()==null ? -1 : p.prepared().index()).put((byte)(p.finalized() ? 1 : 0)); }
        return b.array();
    }
    public static TransactionStatus decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<HEADER_BYTES || bytes.length>MAX_BYTES) throw invalid("Invalid transaction status byte length");
        var b=ByteBuffer.wrap(bytes);
        UUID cluster=uuid(b),database=uuid(b),domain=uuid(b),transaction=uuid(b); var operation=new OperationId(uuid(b),b.getLong(),b.getLong()); long epoch=b.getLong();
        byte[] schema=new byte[32],plan=new byte[32]; b.get(schema).get(plan); long generation=b.getLong(); int phase=b.get() & 255; boolean consumed=flag(b); int count=b.getInt();
        if (phase>=Phase.values().length || count<1 || count>MAX_PARTICIPANTS || b.remaining()!=count*PARTICIPANT_BYTES) throw invalid("Invalid transaction status phase/count");
        var participants=new ArrayList<Participant>(count);
        for (int i=0;i<count;i++) {
            UUID range=uuid(b),group=uuid(b); long rangeEpoch=b.getLong(); byte[] digest=new byte[32],hash=new byte[32]; b.get(digest).get(hash); long term=b.getLong(),index=b.getLong();
            if (term==0 ? index!=-1 : term<0 || index<=0) throw invalid("Invalid prepared position");
            participants.add(new Participant(range,group,rangeEpoch,digest,hash,term==0 ? null : new ApplyPosition(term,index),flag(b)));
        }
        var status=new TransactionStatus(cluster,database,domain,transaction,operation,epoch,schema,plan,generation,Phase.values()[phase],participants,consumed);
        if (!Arrays.equals(bytes,status.encode())) throw invalid("Noncanonical transaction status"); return status;
    }
    @Override public boolean equals(Object other) { return other instanceof TransactionStatus status && Arrays.equals(encode(),status.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
    private static int compare(UUID a,UUID b) { int order=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits()); return order!=0 ? order : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits()); }
    private static void uuid(ByteBuffer b,UUID id) { b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer b) { return new UUID(b.getLong(),b.getLong()); }
    private static boolean flag(ByteBuffer b) { int value=b.get() & 255; if (value>1) throw invalid("Invalid transaction status flag"); return value==1; }
    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}
