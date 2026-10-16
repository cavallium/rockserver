package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Immutable origin audit for an accepted plan; the snapshot term is not a replay-term constraint. */
public record TransactionAcceptance(ApplyPosition snapshot,long leaderGeneration,UUID ownerIncarnation,
                                    TransactionScope scope,byte[] result) {
    private static final int FIXED_BYTES=48;
    private static final Comparator<UUID> UUID_ORDER=(a,b) -> {
        int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits());
        return first!=0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits());
    };
    private static final Comparator<TransactionScope.Span> SPAN_ORDER=Comparator.comparing(TransactionScope.Span::columnId,UUID_ORDER)
            .thenComparing(TransactionScope.Span::lowerInclusive,TransactionAcceptance::lowerOrder)
            .thenComparingInt(span -> span.isPoint() ? 1 : 2)
            .thenComparing(TransactionScope.Span::upperExclusive,TransactionAcceptance::upperOrder);
    public TransactionAcceptance {
        if (snapshot==null || snapshot.term()<1 || leaderGeneration<0 || ownerIncarnation==null || scope==null || result==null) {
            throw new IllegalArgumentException("Invalid transaction acceptance audit");
        }
        if (encodedSize(scope,result.length)>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Transaction acceptance exceeds command budget");
        var decoded=CommandResult.decode(result);
        if (decoded.status()!=CommandResult.Status.SUCCESS || !Arrays.equals(result,decoded.encode())) throw new IllegalArgumentException("Acceptance requires a canonical SUCCESS result");
        scope=new TransactionScope(ordered(scope)); result=result.clone();
    }
    @Override public byte[] result() { return result.clone(); }
    int encodedSize() { return (int)encodedSize(scope,result.length); }
    void encode(ByteBuffer output) {
        output.putLong(snapshot.term()).putLong(snapshot.index()).putLong(leaderGeneration);
        uuid(output,ownerIncarnation); var spans=ordered(scope); output.putInt(spans.size());
        for (var span:spans) {
            output.put((byte)(span.isPoint() ? 1 : 2)); uuid(output,span.columnId());
            bound(output,span.lowerInclusive()); bound(output,span.upperExclusive());
        }
        output.putInt(result.length).put(result);
    }
    static TransactionAcceptance decode(ByteBuffer input) {
        if (input.remaining()<FIXED_BYTES+25+8 || input.remaining()>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Invalid transaction acceptance length");
        var snapshot=new ApplyPosition(input.getLong(),input.getLong()); long generation=input.getLong(); UUID owner=uuid(input);
        int count=input.getInt();
        if (snapshot.term()<1 || generation<0 || count<1 || count>TransactionScope.MAX_SPANS || 25L*count>input.remaining()-12) {
            throw new IllegalArgumentException("Invalid transaction acceptance scope bounds");
        }
        var spans=new ArrayList<TransactionScope.Span>(count); TransactionScope.Span previous=null; long bounds=0;
        for (int i=0;i<count;i++) {
            require(input,25); int kind=input.get() & 255;
            if (kind!=1 && kind!=2) throw new IllegalArgumentException("Unknown acceptance scope span kind");
            UUID column=uuid(input); int lowerLength=input.getInt(); int reserved=12+25*(count-i-1);
            if (lowerLength< -1 || kind==1 && lowerLength<0 || lowerLength>input.remaining()-4-reserved) throw new IllegalArgumentException("Invalid acceptance lower bound length");
            int upperLength=input.getInt(input.position()+Math.max(0,lowerLength));
            if (upperLength< -1 || kind==1 && upperLength!=-1 || upperLength>input.remaining()-Math.max(0,lowerLength)-4-reserved) {
                throw new IllegalArgumentException("Invalid acceptance upper bound length");
            }
            bounds+=Math.max(0,lowerLength)+(long)Math.max(0,upperLength);
            if (bounds>TransactionScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("Acceptance scope bound budget exceeded");
            byte[] lower=lowerLength<0 ? null : new byte[lowerLength]; if (lower!=null) input.get(lower);
            input.getInt(); byte[] upper=upperLength<0 ? null : new byte[upperLength]; if (upper!=null) input.get(upper);
            var span=kind==1 ? TransactionScope.Span.point(column,lower) : TransactionScope.Span.range(column,lower,upper);
            if (previous!=null && SPAN_ORDER.compare(previous,span)>0) throw new IllegalArgumentException("Noncanonical acceptance scope order");
            spans.add(span); previous=span;
        }
        require(input,4); int resultLength=input.getInt();
        if (resultLength<8 || resultLength>CommandResult.MAX_BYTES || resultLength!=input.remaining()) throw new IllegalArgumentException("Invalid accepted result byte count");
        byte[] result=new byte[resultLength]; input.get(result);
        return new TransactionAcceptance(snapshot,generation,owner,new TransactionScope(spans),result);
    }
    void requireCovers(List<Mutation> mutations,List<KeyPredicate> predicates) {
        int results=0;
        for (var mutation:mutations) {
            boolean covered=mutation.kind()==Mutation.Kind.DELETE_RANGE ? scope.containsRange(mutation.columnId(),mutation.key(),mutation.value())
                    : scope.contains(mutation.columnId(),mutation.key());
            if (!covered) throw new IllegalArgumentException("Acceptance scope does not cover mutation");
            if (mutation.returnMode()!=Mutation.ReturnMode.NONE) results++;
        }
        for (var predicate:predicates) if (!scope.contains(predicate.columnId(),predicate.key())) throw new IllegalArgumentException("Acceptance scope does not cover predicate");
        if (ByteBuffer.wrap(result).getInt(4)!=results) throw new IllegalArgumentException("Accepted result shape differs from ordered return modes");
    }
    @Override public boolean equals(Object other) {
        if (!(other instanceof TransactionAcceptance audit) || !snapshot.equals(audit.snapshot) || leaderGeneration!=audit.leaderGeneration
                || !ownerIncarnation.equals(audit.ownerIncarnation) || !Arrays.equals(result,audit.result) || scope.spans().size()!=audit.scope.spans().size()) return false;
        for (int i=0;i<scope.spans().size();i++) if (SPAN_ORDER.compare(scope.spans().get(i),audit.scope.spans().get(i))!=0) return false;
        return true;
    }
    @Override public int hashCode() {
        int hash=Objects.hash(snapshot,leaderGeneration,ownerIncarnation,Arrays.hashCode(result));
        for (var span:scope.spans()) hash=31*hash+Objects.hash(span.columnId(),span.isPoint(),Arrays.hashCode(span.lowerInclusive()),Arrays.hashCode(span.upperExclusive()));
        return hash;
    }
    /** Exact audit bytes without constructing any provisional snapshot/owner identity. */
    public static long encodedSize(TransactionScope scope,int result) {
        Objects.requireNonNull(scope);if(result<8 || result>CommandResult.MAX_BYTES)throw new IllegalArgumentException("Invalid accepted result byte bound");
        long size=FIXED_BYTES+(long)result;
        for (var span:scope.spans()) { byte[] lower=span.lowerInclusive(), upper=span.upperExclusive(); size+=25L+(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length); }
        return size;
    }
    private static List<TransactionScope.Span> ordered(TransactionScope scope) { return scope.spans().stream().sorted(SPAN_ORDER).toList(); }
    private static int lowerOrder(byte[] a,byte[] b) { return a==null ? b==null ? 0 : -1 : b==null ? 1 : Arrays.compareUnsigned(a,b); }
    private static int upperOrder(byte[] a,byte[] b) { return a==null ? b==null ? 0 : 1 : b==null ? -1 : Arrays.compareUnsigned(a,b); }
    private static void require(ByteBuffer input,int count) { if (input.remaining()<count) throw new IllegalArgumentException("Truncated acceptance audit"); }
    private static void bound(ByteBuffer output,byte[] bound) { output.putInt(bound==null ? -1 : bound.length); if (bound!=null) output.put(bound); }
    private static void uuid(ByteBuffer output,UUID id) { output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer input) { return new UUID(input.getLong(),input.getLong()); }
}
