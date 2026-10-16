package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Frozen complete snapshot participant set and physical scopes, independent of mutable capture progress. */
public record SnapshotFence(UUID clusterId,UUID databaseId,UUID fenceId,UUID homeGroup,
                            long homeEpoch,OperationId operation,List<DomainBinding> domains,List<Participant> participants) {
    public static final int MAX_PARTICIPANTS=64, HEADER_BYTES=112, MAX_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES-8192;
    private static final int PARTICIPANT_HEADER_BYTES=156;
    static final Comparator<UUID> UUID_ORDER=(a,b) -> { int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits()); return first!=0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits()); };
    private static final Comparator<TransactionScope.Span> SPAN_ORDER=Comparator.comparing(TransactionScope.Span::columnId,UUID_ORDER)
            .thenComparing(TransactionScope.Span::lowerInclusive,SnapshotFence::lowerOrder).thenComparingInt(s -> s.isPoint() ? 1 : 2)
            .thenComparing(TransactionScope.Span::upperExclusive,SnapshotFence::upperOrder);
    public record Participant(UUID domainId,UUID rangeId,UUID groupId,long rangeEpoch,byte[] schemaDigest,OperationId installOperation,OperationId releaseOperation,TransactionScope scope) {
        public Participant {
            Objects.requireNonNull(domainId); Objects.requireNonNull(rangeId); Objects.requireNonNull(groupId); Objects.requireNonNull(schemaDigest);
            Objects.requireNonNull(installOperation); Objects.requireNonNull(releaseOperation); Objects.requireNonNull(scope);
            if (rangeEpoch<0 || schemaDigest.length!=32 || installOperation.equals(releaseOperation)) throw invalid("Invalid snapshot participant definition");
            schemaDigest=schemaDigest.clone(); scope=new TransactionScope(ordered(scope));
        }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
        @Override public boolean equals(Object other) { return this==other || other instanceof Participant p
                && domainId.equals(p.domainId) && rangeId.equals(p.rangeId) && groupId.equals(p.groupId) && rangeEpoch==p.rangeEpoch
                && Arrays.equals(schemaDigest,p.schemaDigest) && installOperation.equals(p.installOperation)
                && releaseOperation.equals(p.releaseOperation) && sameScope(scope,p.scope); }
        @Override public int hashCode() {
            int hash=31*(31*domainId.hashCode()+rangeId.hashCode())+groupId.hashCode(); hash=31*hash+Long.hashCode(rangeEpoch);
            hash=31*hash+Arrays.hashCode(schemaDigest); hash=31*hash+installOperation.hashCode();
            return 31*(31*hash+releaseOperation.hashCode())+scopeHash(scope);
        }
    }
    public SnapshotFence {
        Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(fenceId); Objects.requireNonNull(homeGroup);
        Objects.requireNonNull(operation); Objects.requireNonNull(participants);
        if (homeEpoch<0 || participants.isEmpty() || participants.size()>MAX_PARTICIPANTS) throw invalid("Invalid snapshot fence identity/bounds");
        domains=DomainBinding.canonical(domains); participants=participants.stream().sorted(Comparator.comparing(Participant::groupId,UUID_ORDER)).toList();
        var ranges=new HashSet<UUID>(); var groups=new HashSet<UUID>(); var operations=new HashSet<OperationId>(); operations.add(operation);var usedDomains=new HashSet<UUID>();long bytes=HEADER_BYTES+(long)domains.size()*DomainBinding.BYTES;
        for (var p:participants) {
            if(!Arrays.equals(DomainBinding.require(domains,p.domainId()).schemaDigest(),p.schemaDigest()))throw invalid("Snapshot participant schema differs from domain binding");usedDomains.add(p.domainId());
            if (!ranges.add(p.rangeId()) || !groups.add(p.groupId()) || !operations.add(p.installOperation()) || !operations.add(p.releaseOperation())) throw invalid("Duplicate snapshot participant/control identity");
            bytes+=participantSize(p); if (bytes>MAX_BYTES) throw invalid("Snapshot fence exceeds aggregate byte bound");
        }
        if(usedDomains.size()!=domains.size())throw invalid("Snapshot binding has no participant");
    }
    public DomainBinding domain(UUID domain){return DomainBinding.require(domains,domain);}
    public DomainBinding participantDomain(UUID group){return domain(participant(group).domainId());}
    public Participant participant(UUID group) { return participants.stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow(() -> invalid("Unregistered snapshot participant")); }
    public byte[] encode() {
        long size=HEADER_BYTES+(long)domains.size()*DomainBinding.BYTES; for (var p:participants) size+=participantSize(p);
        if (size>MAX_BYTES) throw invalid("Snapshot fence exceeds aggregate byte bound");
        var b=ByteBuffer.allocate((int)size); uuid(b,clusterId); uuid(b,databaseId); uuid(b,fenceId); uuid(b,homeGroup);
        b.putLong(homeEpoch); operation(b,operation); b.putInt(domains.size()).putInt(participants.size());for(var domain:domains)DomainBinding.write(b,domain);
        for (var p:participants) participant(b,p); return b.array();
    }
    public static SnapshotFence decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<HEADER_BYTES || bytes.length>MAX_BYTES) throw invalid("Invalid snapshot fence length");
        try {
            var b=ByteBuffer.wrap(bytes); UUID cluster=uuid(b),database=uuid(b),fence=uuid(b),home=uuid(b);
            long homeEpoch=b.getLong();var operation=operation(b);int domainCount=b.getInt(),count=b.getInt();
            if(domainCount<1 || domainCount>DomainBinding.MAX_DOMAINS || (long)domainCount*DomainBinding.BYTES>b.remaining())throw invalid("Invalid snapshot domain count");
            var domains=new ArrayList<DomainBinding>(domainCount);for(int n=0;n<domainCount;n++)domains.add(DomainBinding.read(b));
            if (count<1 || count>MAX_PARTICIPANTS || (long)count*(PARTICIPANT_HEADER_BYTES+25)>b.remaining()) throw invalid("Invalid snapshot participant count");
            var participants=new ArrayList<Participant>(count);
            for (int i=0;i<count;i++) {
                require(b,PARTICIPANT_HEADER_BYTES); UUID domain=uuid(b),range=uuid(b),group=uuid(b); long epoch=b.getLong(); byte[] digest=new byte[32]; b.get(digest);
                var install=operation(b); var release=operation(b); int spans=b.getInt();
                if (spans<1 || spans>TransactionScope.MAX_SPANS || 25L*spans>b.remaining()) throw invalid("Invalid snapshot scope span count");
                var scope=new ArrayList<TransactionScope.Span>(spans); long bounds=0;
                for (int j=0;j<spans;j++) {
                    require(b,25); int kind=b.get() & 255; if (kind!=1 && kind!=2) throw invalid("Invalid snapshot scope kind"); UUID column=uuid(b);
                    byte[] lower=bound(b),upper=bound(b); bounds+=(lower==null ? 0 : lower.length)+(long)(upper==null ? 0 : upper.length);
                    if (bounds>TransactionScope.MAX_BOUND_BYTES || kind==1 && (lower==null || upper!=null)) throw invalid("Invalid snapshot scope bounds");
                    scope.add(kind==1 ? TransactionScope.Span.point(column,lower) : TransactionScope.Span.range(column,lower,upper));
                }
                participants.add(new Participant(domain,range,group,epoch,digest,install,release,new TransactionScope(scope)));
            }
            if (b.hasRemaining()) throw invalid("Trailing snapshot fence bytes");
            var result=new SnapshotFence(cluster,database,fence,home,homeEpoch,operation,domains,participants);
            if (!Arrays.equals(bytes,result.encode())) throw invalid("Noncanonical snapshot fence"); return result;
        } catch (RuntimeException malformed) { if (malformed instanceof IllegalArgumentException invalid) throw invalid; throw new IllegalArgumentException("Invalid snapshot fence",malformed); }
    }
    @Override public boolean equals(Object other) { return this==other || other instanceof SnapshotFence fence
            && clusterId.equals(fence.clusterId) && databaseId.equals(fence.databaseId) && domains.equals(fence.domains)
            && fenceId.equals(fence.fenceId) && homeGroup.equals(fence.homeGroup) && homeEpoch==fence.homeEpoch
            && operation.equals(fence.operation) && participants.equals(fence.participants); }
    @Override public int hashCode(){return Objects.hash(clusterId,databaseId,fenceId,homeGroup,homeEpoch,operation,domains,participants);}
    private static boolean sameScope(TransactionScope a,TransactionScope b) {
        var first=a.spans(); var second=b.spans(); if (first.size()!=second.size()) return false;
        for (int i=0;i<first.size();i++) if (!first.get(i).sameValue(second.get(i))) return false;
        return true;
    }
    private static int scopeHash(TransactionScope scope) {
        int hash=1; var spans=scope.spans(); for (int i=0;i<spans.size();i++) hash=31*hash+spans.get(i).valueHash(); return hash;
    }
    private static long participantSize(Participant p) {
        long size=PARTICIPANT_HEADER_BYTES; for (var span:p.scope().spans()) size+=25L+(span.lowerInclusive()==null ? 0 : span.lowerInclusive().length)+(span.upperExclusive()==null ? 0 : span.upperExclusive().length); return size;
    }
    private static void participant(ByteBuffer b,Participant p) {
        uuid(b,p.domainId());uuid(b,p.rangeId()); uuid(b,p.groupId()); b.putLong(p.rangeEpoch()).put(p.schemaDigest()); operation(b,p.installOperation()); operation(b,p.releaseOperation());
        var spans=ordered(p.scope()); b.putInt(spans.size());
        for (var span:spans) { b.put((byte)(span.isPoint() ? 1 : 2)); uuid(b,span.columnId()); bound(b,span.lowerInclusive()); bound(b,span.upperExclusive()); }
    }
    private static List<TransactionScope.Span> ordered(TransactionScope scope) { return scope.spans().stream().sorted(SPAN_ORDER).toList(); }
    private static int lowerOrder(byte[] a,byte[] b) { return a==null ? b==null ? 0 : -1 : b==null ? 1 : Arrays.compareUnsigned(a,b); }
    private static int upperOrder(byte[] a,byte[] b) { return a==null ? b==null ? 0 : 1 : b==null ? -1 : Arrays.compareUnsigned(a,b); }
    private static void bound(ByteBuffer b,byte[] value) { b.putInt(value==null ? -1 : value.length); if (value!=null) b.put(value); }
    private static byte[] bound(ByteBuffer b) { require(b,4); int size=b.getInt(); if (size== -1) return null; if (size<0 || size>TransactionScope.MAX_BOUND_BYTES || size>b.remaining()) throw invalid("Invalid snapshot bound length"); byte[] value=new byte[size]; b.get(value); return value; }
    static void uuid(ByteBuffer b,UUID value) { b.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()); }
    static UUID uuid(ByteBuffer b) { return new UUID(b.getLong(),b.getLong()); }
    private static void operation(ByteBuffer b,OperationId value) { uuid(b,value.sessionId()); b.putLong(value.epoch()).putLong(value.sequence()); }
    private static OperationId operation(ByteBuffer b) { return new OperationId(uuid(b),b.getLong(),b.getLong()); }
    static void require(ByteBuffer b,int count) { if (b.remaining()<count) throw invalid("Truncated snapshot record"); }
    static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}
