package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;

/** Pure frozen snapshot routing and control allocation; acquires no native or remote resources. */
public final class RatisSnapshotPlan {
    public static final int CONTROL_SLOTS=512;
    private static final Comparator<UUID> UUID_ORDER=(a,b) -> { int n=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits()); return n!=0 ? n : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits()); };
    private final SnapshotFence fence;
    private final List<RangeDescriptor> routes;
    private final TransactionScope scope;
    private RatisSnapshotPlan(SnapshotFence fence,List<RangeDescriptor> routes,TransactionScope scope) { this.fence=fence; this.routes=List.copyOf(routes); this.scope=scope; }
    public SnapshotFence fence() { return fence; }
    public List<RangeDescriptor> routes() { return routes; }
    public TransactionScope scope() { return scope; }
    static RangeDirectory column(List<RangeDirectory> directories,UUID column){
        RangeDirectory selected=null;for(var directory:directories)if(directory.columns().contains(column)){if(selected!=null)throw new IllegalArgumentException("Ambiguous transaction column");selected=directory;}if(selected==null)throw new IllegalArgumentException("Unknown transaction column");return selected;
    }
    static RangeDirectory group(List<RangeDirectory> directories,UUID group){return directories.stream().filter(d->d.ranges().stream().anyMatch(r->r.groupId().equals(group))).findFirst().orElseThrow(()->new IllegalArgumentException("Unknown transaction group"));}
    public static List<RangeDescriptor> participants(List<RangeDirectory> directories,TransactionScope scope){
        if(directories.isEmpty() || directories.size()>DomainBinding.MAX_DOMAINS)throw new IllegalArgumentException("Invalid snapshot domain count");
        var namespace=directories.getFirst();var columns=new HashSet<UUID>();var groups=new HashSet<UUID>();var domains=new HashSet<UUID>();
        for(var directory:directories){if(!namespace.clusterId().equals(directory.clusterId()) || !namespace.databaseId().equals(directory.databaseId()) || !domains.add(directory.domainId()))throw new IllegalArgumentException("Foreign or duplicate snapshot namespace");for(var column:directory.columns())if(!columns.add(column))throw new IllegalArgumentException("Ambiguous snapshot column");for(var range:directory.ranges())if(!groups.add(range.groupId()))throw new IllegalArgumentException("Ambiguous snapshot group");}
        var selected=new TreeMap<UUID,RangeDescriptor>(UUID_ORDER);
        for(var span:scope.spans()){var directory=column(directories,span.columnId());var ranges=span.isPoint()?List.of(directory.route(span.columnId(),span.lowerInclusive())):directory.intersect(span.columnId(),span.lowerInclusive(),span.upperExclusive(),false);for(var range:ranges){selected.putIfAbsent(range.groupId(),range);if(selected.size()>SnapshotFence.MAX_PARTICIPANTS)throw new IllegalArgumentException("Aggregate transaction participant budget exceeded");}}
        return List.copyOf(selected.values());
    }
    private record Projection(List<RangeDescriptor> routes,Map<UUID,List<TransactionScope.Span>> scopes,List<DomainBinding> authority) {}
    /** Exact clipped wire envelope validation, without allocating a transaction or any operation identity. */
    public static void preflight(List<RangeDirectory> directories,TransactionScope scope){project(directories,scope);}
    private static Projection project(List<RangeDirectory> directories,TransactionScope scope){
        var routes=participants(directories,scope);var used=new HashSet<UUID>();for(var route:routes)used.add(group(directories,route.groupId()).domainId());
        var authority=DomainBinding.canonical(directories.stream().filter(directory->used.contains(directory.domainId())).map(DomainBinding::of).toList());
        var scopes=new HashMap<UUID,List<TransactionScope.Span>>();long bytes=SnapshotFence.HEADER_BYTES+(long)authority.size()*DomainBinding.BYTES,bounds=0;
        for(var span:scope.spans()){
            var directory=column(directories,span.columnId());byte[] originalLower=span.lowerInclusive(),originalUpper=span.upperExclusive();
            var selected=span.isPoint()?List.of(directory.route(span.columnId(),originalLower)):directory.intersect(span.columnId(),originalLower,originalUpper,false);
            for(var route:selected){
                byte[] lower=span.isPoint()?originalLower:maximum(originalLower,route.lowerInclusive()),upper=span.isPoint()?null:minimum(originalUpper,route.upperExclusive());
                if(!span.isPoint() && lower!=null && upper!=null && Arrays.compareUnsigned(lower,upper)>=0)continue;
                long size=(lower==null?0:lower.length)+(long)(upper==null?0:upper.length);bytes+=25+size+(scopes.containsKey(route.groupId())?0:156);bounds+=size;
                if(bytes>SnapshotFence.MAX_BYTES || bounds>TransactionScope.MAX_BOUND_BYTES)throw new IllegalArgumentException("Snapshot scope exceeds aggregate frozen byte budget");
                var spans=scopes.computeIfAbsent(route.groupId(),ignored->new ArrayList<>());if(spans.size()>=TransactionScope.MAX_SPANS)throw new IllegalArgumentException("Participant scope exceeds span budget");
                spans.add(span.isPoint()?TransactionScope.Span.point(span.columnId(),lower):TransactionScope.Span.range(span.columnId(),lower,upper));
            }
        }
        return new Projection(routes,Map.copyOf(scopes),authority);
    }
    public static RatisSnapshotPlan create(List<RangeDirectory> directories,TransactionScope scope,UUID fenceId,OperationId business,UUID homeGroup,long homeEpoch,OperationId firstControl) {
        Objects.requireNonNull(scope);Objects.requireNonNull(fenceId);Objects.requireNonNull(business);Objects.requireNonNull(homeGroup);Objects.requireNonNull(firstControl);
        if(homeEpoch<0 || business.epoch()<1 || !business.sessionId().equals(firstControl.sessionId()) || business.epoch()!=firstControl.epoch() || firstControl.sequence()<=business.sequence() || firstControl.sequence()>Long.MAX_VALUE-CONTROL_SLOTS)throw new IllegalArgumentException("Invalid reserved snapshot identities");
        var projection=project(directories,scope);var participants=new ArrayList<SnapshotFence.Participant>();int index=0;
        for(var route:projection.routes()){
            var directory=group(directories,route.groupId());participants.add(new SnapshotFence.Participant(directory.domainId(),route.rangeId(),route.groupId(),route.rangeEpoch(),directory.schemaDigest(),control(firstControl,index),control(firstControl,64+index),new TransactionScope(projection.scopes().get(route.groupId()))));index++;
        }
        var namespace=directories.getFirst();return new RatisSnapshotPlan(new SnapshotFence(namespace.clusterId(),namespace.databaseId(),fenceId,homeGroup,homeEpoch,business,projection.authority(),participants),projection.routes(),scope);
    }
    static void requireDomain(RangeDirectory directory,SnapshotFence fence){fence.domain(directory.domainId()).require(directory);for(var participant:fence.participants())if(participant.domainId().equals(directory.domainId())){var route=directory.ranges().stream().filter(r->r.groupId().equals(participant.groupId())).findFirst().orElseThrow();if(!route.rangeId().equals(participant.rangeId()) || route.rangeEpoch()!=participant.rangeEpoch() || !Arrays.equals(directory.schemaDigest(),participant.schemaDigest()))throw new IllegalArgumentException("Frozen participant route changed");var covered=participant.scope().participants(directory,SnapshotFence.MAX_PARTICIPANTS);if(covered.size()!=1 || !covered.getFirst().equals(route))throw new IllegalArgumentException("Frozen participant scope changed");}}
    /** Qualify the original fence without rewriting equivalent adjacent scope spans or its binding digest. */
    public static RatisSnapshotPlan frozen(List<RangeDirectory> directories,SnapshotFence expected) {
        Objects.requireNonNull(expected);if(!DomainBinding.canonical(directories.stream().map(DomainBinding::of).toList()).equals(expected.domains()))throw new IllegalArgumentException("Snapshot authority vector changed");
        var first=expected.participants().stream().map(SnapshotFence.Participant::installOperation)
                .min(Comparator.comparingLong(OperationId::sequence)).orElseThrow();
        var plan=create(directories,scope(expected),expected.fenceId(),expected.operation(),expected.homeGroup(),expected.homeEpoch(),first);
        var participants=expected.participants().stream().map(p -> new SnapshotFence.Participant(p.domainId(), p.rangeId(), p.groupId(), p.rangeEpoch(), p.schemaDigest(), p.installOperation(), p.releaseOperation(), normalize(p.scope().spans()))).toList();
        var qualified=new SnapshotFence(expected.clusterId(), expected.databaseId(), expected.fenceId(), expected.homeGroup(), expected.homeEpoch(), expected.operation(), expected.domains(), participants);
        if (!qualified.equals(plan.fence())) throw new IllegalArgumentException("Frozen source definition differs from cached projection");
        return new RatisSnapshotPlan(expected,plan.routes(),plan.scope());
    }
    private static OperationId control(OperationId first,int offset) { return new OperationId(first.sessionId(),first.epoch(),first.sequence()+offset); }
    private static byte[] maximum(byte[] a,byte[] b) { return a==null ? b : b==null || Arrays.compareUnsigned(a,b)>=0 ? a : b; }
    private static byte[] minimum(byte[] a,byte[] b) { return a==null ? b : b==null || Arrays.compareUnsigned(a,b)<=0 ? a : b; }
    /** Reconstruct the physical union without multiplying the original span count at partition boundaries. */
    public static TransactionScope scope(SnapshotFence fence) {
        Objects.requireNonNull(fence); var spans=new ArrayList<TransactionScope.Span>(); fence.participants().forEach(p -> spans.addAll(p.scope().spans()));
        return normalize(spans);
    }
    private static TransactionScope normalize(List<TransactionScope.Span> source) {
        var spans=new ArrayList<>(source);
        spans.sort(Comparator.comparing(TransactionScope.Span::columnId,UUID_ORDER).thenComparing(TransactionScope.Span::lowerInclusive,(a,b) -> a==null ? b==null ? 0 : -1 : b==null ? 1 : Arrays.compareUnsigned(a,b)).thenComparing(TransactionScope.Span::isPoint));
        var union=new ArrayList<TransactionScope.Span>();
        for (var span:spans) {
            if (!union.isEmpty()) {
                var previous=union.getLast();
                if (previous.columnId().equals(span.columnId())) {
                    byte[] upper=previous.upperExclusive(),lower=span.lowerInclusive();
                    if (!previous.isPoint() && (upper==null || lower==null || Arrays.compareUnsigned(lower,upper)<=0)) {
                        if (span.isPoint()) { if (upper==null || Arrays.compareUnsigned(lower,upper)<0) continue; }
                        else { byte[] end=span.upperExclusive(); byte[] merged=upper==null || end==null ? null : maximum(upper,end); union.set(union.size()-1,TransactionScope.Span.range(span.columnId(),previous.lowerInclusive(),merged)); continue; }
                    }
                    if (previous.isPoint() && span.isPoint() && Arrays.equals(previous.lowerInclusive(),lower)) continue;
                }
            }
            union.add(span);
        }
        return new TransactionScope(union);
    }
}
