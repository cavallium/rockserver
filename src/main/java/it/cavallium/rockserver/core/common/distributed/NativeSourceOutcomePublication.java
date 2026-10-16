package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Compact terminal publication proof; original receipt fences remain authoritative. */
public record NativeSourceOutcomePublication(UUID group,OperationId operation,byte[] requestDigest,byte[] resultDigest,ApplyPosition creditPosition,ApplyPosition position,Outcome outcome) {
    public enum Outcome { RETAINED,FENCED,CLOSED }
    public static final int BYTES=149;
    public NativeSourceOutcomePublication { Objects.requireNonNull(group);Objects.requireNonNull(operation);Objects.requireNonNull(creditPosition);Objects.requireNonNull(position);Objects.requireNonNull(outcome);if(outcome==Outcome.CLOSED || requestDigest==null || requestDigest.length!=32 || resultDigest==null || resultDigest.length!=32 || creditPosition.term()<1 || creditPosition.index()<0 || position.term()<creditPosition.term() || position.index()<=creditPosition.index())throw new IllegalArgumentException("Invalid source publication proof");requestDigest=requestDigest.clone();resultDigest=resultDigest.clone(); }
    @Override public byte[] requestDigest() { return requestDigest.clone(); }
    @Override public byte[] resultDigest() { return resultDigest.clone(); }
    public void requireRequest(NativeSourceOutcomeCredit.Request request,byte[] result) { if(!group.equals(request.original().groupId()) || !operation.equals(request.original().operationId()) || !Arrays.equals(requestDigest,request.digest()) || !Arrays.equals(resultDigest,NativeSourceOutcomeCredit.hash(result)))throw new IllegalArgumentException("Source publication identity/result changed"); }
    public void requireCut(ApplyPosition cut) { if(cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term())throw new IllegalArgumentException("Source publication exceeds completed prefix"); }
    public byte[] encode() { return ByteBuffer.allocate(BYTES).putInt(0x534f5055).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).putLong(operation.sessionId().getMostSignificantBits()).putLong(operation.sessionId().getLeastSignificantBits()).putLong(operation.epoch()).putLong(operation.sequence()).put(requestDigest).put(resultDigest).putLong(creditPosition.term()).putLong(creditPosition.index()).putLong(position.term()).putLong(position.index()).put((byte)outcome.ordinal()).array(); }
    @Override public boolean equals(Object other) { return other instanceof NativeSourceOutcomePublication publication && Arrays.equals(encode(),publication.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
    public static NativeSourceOutcomePublication decode(byte[] bytes) { if(bytes==null || bytes.length!=BYTES)throw new IllegalArgumentException("Invalid source publication size");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=0x534f5055)throw new IllegalArgumentException("Invalid source publication tag");UUID group=new UUID(in.getLong(),in.getLong());var operation=new OperationId(new UUID(in.getLong(),in.getLong()),in.getLong(),in.getLong());byte[] request=new byte[32],result=new byte[32];in.get(request).get(result);var credit=new ApplyPosition(in.getLong(),in.getLong());var position=new ApplyPosition(in.getLong(),in.getLong());int kind=Byte.toUnsignedInt(in.get());if(kind>=Outcome.values().length)throw new IllegalArgumentException("Invalid source publication outcome");return new NativeSourceOutcomePublication(group,operation,request,result,credit,position,Outcome.values()[kind]); }
}
