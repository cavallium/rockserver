package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Durable terminal participant outcome, independent of session receipt retention. A missing vote means ABORT preceded PREPARE. */
public record ParticipantOutcome(ParticipantFinalization finalization,ApplyPosition position,ParticipantVote vote,byte[] result) {
    public static final int MAX_RESULT_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES;
    public static final int MAX_BYTES=41+ParticipantFinalization.MAX_BYTES+MAX_RESULT_BYTES;
    public ParticipantOutcome {
        Objects.requireNonNull(finalization); Objects.requireNonNull(position);
        if (position.term()<=0 || position.index()<=0) throw new IllegalArgumentException("Invalid finalization position");
        var homeVote=finalization.participant().prepared();
        if (vote!=null && vote.prepared() && (vote.position().index()>=position.index() || vote.position().term()>position.term()
                || homeVote!=null && !homeVote.equals(vote.position()))) throw new IllegalArgumentException("Finalization vote/cut differs");
        if (homeVote!=null && (vote==null || !vote.prepared())) throw new IllegalArgumentException("Recorded home vote has no positive native evidence");
        if (finalization.decision().phase()==TransactionStatus.Phase.COMMIT) {
            if (vote==null || !vote.prepared() || result==null || result.length>MAX_RESULT_BYTES) throw new IllegalArgumentException("COMMIT outcome needs its prepared vote and result");
            var decoded=CommandResult.decode(result);
            if (decoded.status()!=CommandResult.Status.SUCCESS || !Arrays.equals(result,decoded.encode())) throw new IllegalArgumentException("Invalid COMMIT result");
        } else if (result!=null) throw new IllegalArgumentException("ABORT has no committed business result");
        result=result==null ? null : result.clone();
    }
    @Override public byte[] result() { return result==null ? null : result.clone(); }
    @Override public boolean equals(Object other) { return other instanceof ParticipantOutcome o && finalization.equals(o.finalization) && position.equals(o.position) && Objects.equals(vote,o.vote) && Arrays.equals(result,o.result); }
    @Override public int hashCode() { return Objects.hash(finalization,position,vote,Arrays.hashCode(result)); }
    public byte[] encode() {
        byte[] decision=finalization.encode(); int voteBytes=vote!=null && vote.prepared() ? 16 : 0;
        var b=ByteBuffer.allocate(25+voteBytes+decision.length+(result==null ? 0 : result.length)).putLong(position.term()).putLong(position.index()).put((byte)(vote==null ? 0 : vote.prepared() ? 1 : 2));
        if (voteBytes!=0) b.putLong(vote.position().term()).putLong(vote.position().index());
        b.putInt(decision.length).put(decision).putInt(result==null ? -1 : result.length); if (result!=null) b.put(result); return b.array();
    }
    public static ParticipantOutcome decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<25 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid participant outcome length");
        var b=ByteBuffer.wrap(bytes); var position=new ApplyPosition(b.getLong(),b.getLong()); int kind=b.get() & 255;
        if (kind>2 || kind==1 && b.remaining()<24) throw new IllegalArgumentException("Invalid prepare outcome kind/length");
        var vote=kind==0 ? null : kind==2 ? ParticipantVote.negative() : ParticipantVote.prepared(new ApplyPosition(b.getLong(),b.getLong()));
        int length=b.getInt(); if (length<0 || length>ParticipantFinalization.MAX_BYTES || length>b.remaining()-4) throw new IllegalArgumentException("Invalid finalization length"); byte[] decision=new byte[length]; b.get(decision);
        int resultBytes=b.getInt(); if (resultBytes < -1 || resultBytes>MAX_RESULT_BYTES || (resultBytes==-1 ? b.hasRemaining() : resultBytes!=b.remaining())) throw new IllegalArgumentException("Invalid finalization result length");
        byte[] result=resultBytes==-1 ? null : new byte[resultBytes]; if (result!=null) b.get(result);
        var outcome=new ParticipantOutcome(ParticipantFinalization.decode(decision),position,vote,result);
        if (!Arrays.equals(bytes,outcome.encode())) throw new IllegalArgumentException("Noncanonical participant outcome"); return outcome;
    }
}
