package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** One native range metadata cut; serving authority additionally requires a linearizable transport read. */
public record RangeStateView(UUID groupId,long rangeEpoch,UUID domainId,byte[] schemaDigest,Set<UUID> columns,
                             byte[] lowerInclusive,byte[] upperExclusive,ApplyPosition applied,SessionAuthority sessionAuthority) {
    public static final int MAX_BYTES=2*1024*1024;
    private static final int MAGIC=0x52535657, VERSION=1, MIN_BYTES=110;
    private static final Comparator<UUID> UUID_ORDER=(a,b) -> {
        int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits());
        return first!=0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits());
    };
    public RangeStateView {
        Objects.requireNonNull(groupId); Objects.requireNonNull(domainId);
        if (rangeEpoch<0 || schemaDigest.length!=32 || columns.isEmpty() || columns.size()>RangeDirectoryCodec.MAX_COLUMNS
                || (lowerInclusive!=null && upperExclusive!=null && Arrays.compareUnsigned(lowerInclusive,upperExclusive)>=0)
                || encodedSize(columns.size(),lowerInclusive,upperExclusive,applied,sessionAuthority)>MAX_BYTES) {
            throw new IllegalArgumentException("Invalid bounded range state view");
        }
        schemaDigest=schemaDigest.clone(); columns=Set.copyOf(columns);
        lowerInclusive=lowerInclusive==null ? null : lowerInclusive.clone(); upperExclusive=upperExclusive==null ? null : upperExclusive.clone();
    }
    @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
    @Override public byte[] lowerInclusive() { return lowerInclusive==null ? null : lowerInclusive.clone(); }
    @Override public byte[] upperExclusive() { return upperExclusive==null ? null : upperExclusive.clone(); }
    public byte[] encode() {
        var output=ByteBuffer.allocate((int)encodedSize(columns.size(),lowerInclusive,upperExclusive,applied,sessionAuthority));
        output.putInt(MAGIC).putInt(VERSION); uuid(output,groupId); output.putLong(rangeEpoch); uuid(output,domainId); output.put(schemaDigest);
        output.putInt(columns.size()); columns.stream().sorted(UUID_ORDER).forEach(column -> uuid(output,column));
        bound(output,lowerInclusive); bound(output,upperExclusive); output.put((byte)(applied==null ? 0 : 1));
        if (applied!=null) output.putLong(applied.term()).putLong(applied.index());
        output.put((byte)(sessionAuthority==null ? 0 : 1));
        if (sessionAuthority!=null) output.put(new RangeSessionControl(sessionAuthority,null,0).encode());
        return output.array();
    }
    public static RangeStateView decode(byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.length<MIN_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid range state view size");
        var input=ByteBuffer.wrap(bytes);
        if (input.getInt()!=MAGIC || input.getInt()!=VERSION) throw new IllegalArgumentException("Unsupported range state view header");
        UUID group=uuid(input); long epoch=input.getLong(); UUID domain=uuid(input); byte[] schema=new byte[32]; input.get(schema);
        int count=input.getInt();
        if (epoch<0 || count<1 || count>RangeDirectoryCodec.MAX_COLUMNS || (long)count*16>input.remaining()-10) {
            throw new IllegalArgumentException("Invalid range state view epoch/columns");
        }
        var columns=new HashSet<UUID>(); UUID previous=null;
        for (int i=0;i<count;i++) {
            UUID column=uuid(input);
            if (previous!=null && UUID_ORDER.compare(previous,column)>=0) throw new IllegalArgumentException("Noncanonical range state view columns");
            columns.add(column); previous=column;
        }
        byte[] lower=bound(input,6), upper=bound(input,2);
        ApplyPosition applied=null;
        if (present(input)) { require(input,17); applied=new ApplyPosition(input.getLong(),input.getLong()); }
        SessionAuthority authority=null;
        if (present(input)) {
            require(input,RangeSessionControl.ADVANCE_BYTES);
            byte[] encoded=new byte[RangeSessionControl.ADVANCE_BYTES]; input.get(encoded);
            var control=RangeSessionControl.decode(encoded);
            if (control.binding()) throw new IllegalArgumentException("Range state view authority cannot bind a grant");
            authority=control.authority();
        }
        if (input.hasRemaining()) throw new IllegalArgumentException("Trailing range state view bytes");
        return new RangeStateView(group,epoch,domain,schema,columns,lower,upper,applied,authority);
    }
    public void requireMatches(RangeDirectory directory,RangeDescriptor range) {
        Objects.requireNonNull(directory); Objects.requireNonNull(range);
        if (!groupId.equals(range.groupId()) || rangeEpoch!=range.rangeEpoch() || !domainId.equals(directory.domainId())
                || !Arrays.equals(schemaDigest,directory.schemaDigest()) || !columns.equals(directory.columns())
                || !Arrays.equals(lowerInclusive,range.lowerInclusive()) || !Arrays.equals(upperExclusive,range.upperExclusive())
                || (sessionAuthority!=null && (!sessionAuthority.clusterId().equals(directory.clusterId())
                        || !sessionAuthority.databaseId().equals(directory.databaseId())))) {
            throw new IllegalArgumentException("Range state view differs from directory scope");
        }
    }
    /** This check proves metadata readiness only; it cannot establish quorum freshness on its own. */
    public void requireServing(SessionAuthority target) {
        Objects.requireNonNull(target);
        if (applied==null || applied.term()==0 || sessionAuthority==null) throw new IllegalStateException("Range state view is not serving");
        try { sessionAuthority.requireAtLeast(target); }
        catch (IllegalArgumentException stale) { throw new IllegalStateException("Range state view has not reached target authority",stale); }
    }
    @Override public boolean equals(Object other) {
        return other instanceof RangeStateView view && groupId.equals(view.groupId) && rangeEpoch==view.rangeEpoch && domainId.equals(view.domainId)
                && Arrays.equals(schemaDigest,view.schemaDigest) && columns.equals(view.columns) && Arrays.equals(lowerInclusive,view.lowerInclusive)
                && Arrays.equals(upperExclusive,view.upperExclusive) && Objects.equals(applied,view.applied) && Objects.equals(sessionAuthority,view.sessionAuthority);
    }
    @Override public int hashCode() {
        return Objects.hash(groupId,rangeEpoch,domainId,Arrays.hashCode(schemaDigest),columns,Arrays.hashCode(lowerInclusive),
                Arrays.hashCode(upperExclusive),applied,sessionAuthority);
    }
    private static long encodedSize(int columns,byte[] lower,byte[] upper,ApplyPosition applied,SessionAuthority authority) {
        return 94L+16L*columns+(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length)
                +(applied==null ? 0 : 16)+(authority==null ? 0 : RangeSessionControl.ADVANCE_BYTES);
    }
    private static boolean present(ByteBuffer input) {
        require(input,1); byte value=input.get();
        if (value!=0 && value!=1) throw new IllegalArgumentException("Invalid range state view presence flag");
        return value==1;
    }
    private static byte[] bound(ByteBuffer input,int reserved) {
        require(input,4); int size=input.getInt(); if (size== -1) return null;
        if (size<0 || size>input.remaining()-reserved) throw new IllegalArgumentException("Invalid range state view bound size");
        byte[] bytes=new byte[size]; input.get(bytes); return bytes;
    }
    private static void bound(ByteBuffer output,byte[] bytes) { output.putInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.put(bytes); }
    private static void require(ByteBuffer input,int count) { if (input.remaining()<count) throw new IllegalArgumentException("Truncated range state view"); }
    private static UUID uuid(ByteBuffer input) { return new UUID(input.getLong(),input.getLong()); }
    private static void uuid(ByteBuffer output,UUID id) { output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
}
