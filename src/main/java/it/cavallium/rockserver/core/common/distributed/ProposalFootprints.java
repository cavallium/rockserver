package it.cavallium.rockserver.core.common.distributed;

import java.util.*;

/** Ephemeral proposal ownership; caller serializes admissions/releases, while preparation needs no index lock. */
public final class ProposalFootprints {
    public record Limits(int maxOrdinary,int maxCandidates,int maxSpans,long maxBytes) {
        public Limits {
            if (maxOrdinary<1 || maxOrdinary>65536 || maxCandidates<1 || maxCandidates>4096 || maxSpans<1 || maxSpans>1_048_576
                    || maxBytes<1 || maxBytes>256L*1024*1024) throw new IllegalArgumentException("Invalid proposal footprint limits");
        }
        public static Limits defaults() { return new Limits(1024,64,16384,16L*1024*1024); }
    }
    public static final class PreparedFootprint {
        private final ProposalFootprints owner;
        private final List<Piece> pieces;
        private final long bytes;
        private PreparedFootprint(ProposalFootprints owner,List<Piece> pieces,long bytes) {
            this.owner=owner; this.pieces=List.copyOf(pieces); this.bytes=bytes;
        }
        public boolean isEmpty() { return pieces.isEmpty(); }
        public List<TransactionScope.Span> spans() {
            return pieces.stream().map(piece -> piece.point() ? TransactionScope.Span.point(piece.column(),piece.lower().value)
                    : TransactionScope.Span.range(piece.column(),piece.lower()==null ? null : piece.lower().value,piece.upper()==null ? null : piece.upper().value)).toList();
        }
    }
    private abstract static class Lease {
        private final ProposalFootprints owner;
        private final PreparedFootprint footprint;
        private final long term,generation;
        private volatile boolean active=true;
        private Lease(ProposalFootprints owner,PreparedFootprint footprint,long term,long generation) {
            this.owner=owner; this.footprint=footprint; this.term=term; this.generation=generation;
        }
        public long term() { return term; }
        public long generation() { return generation; }
        public PreparedFootprint footprint() { return footprint; }
        public boolean active() { return active; }
    }
    public static final class OrdinaryLease extends Lease {
        private OrdinaryLease(ProposalFootprints owner,PreparedFootprint footprint,long term,long generation) { super(owner,footprint,term,generation); }
        @Override public long term() { return super.term(); }
        @Override public long generation() { return super.generation(); }
        @Override public PreparedFootprint footprint() { return super.footprint(); }
        @Override public boolean active() { return super.active(); }
    }
    public static final class CandidateLease extends Lease {
        private CandidateLease(ProposalFootprints owner,PreparedFootprint footprint,long term,long generation) { super(owner,footprint,term,generation); }
        @Override public long term() { return super.term(); }
        @Override public long generation() { return super.generation(); }
        @Override public PreparedFootprint footprint() { return super.footprint(); }
        @Override public boolean active() { return super.active(); }
    }
    public record Stats(int ordinary,int candidates,int spans,long bytes) {}
    // Accounted weights: 128/lease +64/normalized span +bound bytes. Bounds are owned once and shared by indexes;
    // JVM object overhead varies, while independent lease/span caps also bound index metadata.
    private static final int LEASE_BYTES=128, SPAN_BYTES=64;
    private static final class Bytes implements Comparable<Bytes> {
        private final byte[] value;
        Bytes(byte[] value) { this.value=value; }
        @Override public int compareTo(Bytes other) { return Arrays.compareUnsigned(value,other.value); }
    }
    private record Piece(UUID column,Bytes lower,Bytes upper,boolean point) {}
    private static final Comparator<Bytes> START=(a,b) -> a==null ? b==null ? 0 : -1 : b==null ? 1 : a.compareTo(b);
    private static final Comparator<Bytes> END=(a,b) -> a==null ? b==null ? 0 : 1 : b==null ? -1 : a.compareTo(b);
    private final Limits limits;
    private final Index ordinaryIndex=new Index(), candidateIndex=new Index();
    private final Set<OrdinaryLease> ordinary=new HashSet<>();
    private final Set<CandidateLease> candidates=new HashSet<>();
    private int spans;
    private long bytes;

    public ProposalFootprints(Limits limits) { this.limits=Objects.requireNonNull(limits); }
    public PreparedFootprint prepare(TransactionScope scope) {
        if (scope==null) throw new IllegalArgumentException("Missing proposal scope"); return prepare(scope.spans());
    }
    public PreparedFootprint prepare(List<TransactionScope.Span> source) {
        if (source==null || source.size()>TransactionScope.MAX_SPANS) throw new IllegalArgumentException("Invalid proposal span count");
        var byColumn=new TreeMap<UUID,List<Piece>>(); long inputBytes=0;
        for (var span:source) {
            if (span==null) throw new IllegalArgumentException("Null proposal span");
            byte[] lower=span.lowerInclusive(), upper=span.upperExclusive();
            inputBytes+=(long)(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length);
            if (inputBytes>TransactionScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("Proposal span bounds exceed budget");
            // Empty bytes are the minimum physical key; (-infinity, empty) contains no keys.
            if (!span.isPoint() && upper!=null && upper.length==0) continue;
            byColumn.computeIfAbsent(span.columnId(),ignored -> new ArrayList<>()).add(new Piece(span.columnId(),
                    lower==null ? null : new Bytes(lower),upper==null ? null : new Bytes(upper),span.isPoint()));
        }
        var pieces=new ArrayList<Piece>();
        for (var entry:byColumn.entrySet()) {
            var ranges=new ArrayList<Piece>(); var points=new TreeSet<Bytes>();
            for (var piece:entry.getValue()) { if (piece.point()) points.add(piece.lower()); else ranges.add(piece); }
            ranges.sort(Comparator.comparing(Piece::lower,START)); var merged=new ArrayList<Piece>();
            for (var range:ranges) {
                if (!merged.isEmpty()) {
                    var previous=merged.getLast();
                    if (previous.upper()==null || range.lower()==null || range.lower().compareTo(previous.upper())<=0) {
                        merged.set(merged.size()-1,new Piece(entry.getKey(),previous.lower(),maxEnd(previous.upper(),range.upper()),false)); continue;
                    }
                }
                merged.add(range);
            }
            pieces.addAll(merged); int cursor=0;
            for (var point:points) {
                while (cursor<merged.size() && !endsAfter(merged.get(cursor).upper(),point)) cursor++;
                if (cursor<merged.size() && START.compare(merged.get(cursor).lower(),point)<=0) continue;
                pieces.add(new Piece(entry.getKey(),point,null,true));
            }
        }
        long size=LEASE_BYTES;
        for (var piece:pieces) {
            byte[] lower=piece.lower()==null ? null : piece.lower().value, upper=piece.upper()==null ? null : piece.upper().value;
            size+=SPAN_BYTES+(long)(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length);
        }
        return new PreparedFootprint(this,pieces,size);
    }
    /** Ordinary proposals may overlap each other; only exclusive candidates can refuse admission. */
    public OrdinaryLease registerOrdinary(PreparedFootprint footprint,long term,long generation) {
        require(footprint,term,generation);
        if (candidateIndex.overlaps(footprint)) return null;
        capacity(footprint,false); var lease=new OrdinaryLease(this,footprint,term,generation);
        ordinaryIndex.add(footprint); ordinary.add(lease); account(footprint,1); return lease;
    }
    public CandidateLease reserveCandidate(PreparedFootprint footprint,long term,long generation) {
        require(footprint,term,generation);
        if (ordinaryIndex.overlaps(footprint) || candidateIndex.overlaps(footprint)) return null;
        capacity(footprint,true); var lease=new CandidateLease(this,footprint,term,generation);
        candidateIndex.add(footprint); candidates.add(lease); account(footprint,1); return lease;
    }
    public void release(OrdinaryLease lease) {
        requireOwner(lease); if (!lease.active()) return;
        ordinaryIndex.remove(lease.footprint()); ordinary.remove(lease); account(lease.footprint(),-1); ((Lease)lease).active=false;
    }
    public void release(CandidateLease lease) {
        requireOwner(lease); if (!lease.active()) return;
        candidateIndex.remove(lease.footprint()); candidates.remove(lease); account(lease.footprint(),-1); ((Lease)lease).active=false;
    }
    public void invalidateCandidates() { for (var lease:List.copyOf(candidates)) release(lease); }
    public Stats stats() { return new Stats(ordinary.size(),candidates.size(),spans,bytes); }
    private void require(PreparedFootprint footprint,long term,long generation) {
        if (footprint==null || footprint.owner!=this || term<1 || generation<0) throw new IllegalArgumentException("Foreign footprint or invalid proposal term/generation");
    }
    private void requireOwner(Lease lease) { if (lease==null || lease.owner!=this) throw new IllegalArgumentException("Lease belongs to another proposal index"); }
    private void capacity(PreparedFootprint footprint,boolean candidate) {
        if ((candidate ? candidates.size()>=limits.maxCandidates() : ordinary.size()>=limits.maxOrdinary())
                || footprint.pieces.size()>limits.maxSpans()-spans || footprint.bytes>limits.maxBytes()-bytes) {
            throw new IllegalStateException("Proposal footprint quota exhausted");
        }
    }
    private void account(PreparedFootprint footprint,int sign) { spans+=sign*footprint.pieces.size(); bytes+=sign*footprint.bytes; }
    private static boolean endsAfter(Bytes upper,Bytes lower) { return lower==null || upper==null || upper.compareTo(lower)>0; }
    private static boolean startsBefore(Bytes lower,Bytes upper) { return lower==null || upper==null || lower.compareTo(upper)<0; }
    private static Bytes maxEnd(Bytes first,Bytes second) { return END.compare(first,second)>=0 ? first : second; }

    private static final class ColumnIndex {
        final NavigableMap<Bytes,Integer> points=new TreeMap<>(); RangeNode ranges;
        boolean empty() { return points.isEmpty() && ranges==null; }
    }
    private static final class Index {
        final Map<UUID,ColumnIndex> columns=new HashMap<>();
        boolean overlaps(PreparedFootprint footprint) {
            for (var piece:footprint.pieces) {
                var index=columns.get(piece.column()); if (index==null) continue;
                if (piece.point()) { if (index.points.containsKey(piece.lower())) return true; }
                else {
                    var point=piece.lower()==null ? index.points.firstEntry() : index.points.ceilingEntry(piece.lower());
                    if (point!=null && startsBefore(point.getKey(),piece.upper())) return true;
                }
                if (overlap(index.ranges,piece)) return true;
            }
            return false;
        }
        void add(PreparedFootprint footprint) {
            for (var piece:footprint.pieces) {
                var index=columns.computeIfAbsent(piece.column(),ignored -> new ColumnIndex());
                if (piece.point()) index.points.merge(piece.lower(),1,Integer::sum);
                else index.ranges=insert(index.ranges,piece.lower(),piece.upper());
            }
        }
        void remove(PreparedFootprint footprint) {
            for (var piece:footprint.pieces) {
                var index=columns.get(piece.column());
                if (piece.point()) { int count=index.points.get(piece.lower()); if (count==1) index.points.remove(piece.lower()); else index.points.put(piece.lower(),count-1); }
                else index.ranges=removeRange(index.ranges,piece.lower(),piece.upper());
                if (index.empty()) columns.remove(piece.column());
            }
        }
    }
    /** AVL range-start index augmented by the furthest upper endpoint, including unbounded ranges. */
    private static final class RangeNode {
        Bytes lower,maxUpper; NavigableMap<Bytes,Integer> ends=new TreeMap<>(END); RangeNode left,right; int height=1;
        RangeNode(Bytes lower,Bytes upper) { this.lower=lower; ends.put(upper,1); maxUpper=upper; }
    }
    private static boolean overlap(RangeNode node,Piece query) {
        if (node==null || !endsAfter(node.maxUpper,query.lower())) return false;
        if (overlap(node.left,query)) return true;
        if (query.point() ? START.compare(node.lower,query.lower())>0 : !startsBefore(node.lower,query.upper())) return false;
        if (endsAfter(node.ends.lastKey(),query.lower())) return true;
        return overlap(node.right,query);
    }
    private static RangeNode insert(RangeNode node,Bytes lower,Bytes upper) {
        if (node==null) return new RangeNode(lower,upper);
        int order=START.compare(lower,node.lower);
        if (order<0) node.left=insert(node.left,lower,upper); else if (order>0) node.right=insert(node.right,lower,upper);
        else node.ends.merge(upper,1,Integer::sum);
        return balance(node);
    }
    private static RangeNode removeRange(RangeNode node,Bytes lower,Bytes upper) {
        int order=START.compare(lower,node.lower);
        if (order<0) node.left=removeRange(node.left,lower,upper); else if (order>0) node.right=removeRange(node.right,lower,upper);
        else {
            int count=node.ends.get(upper);
            if (count>1) node.ends.put(upper,count-1); else node.ends.remove(upper);
            if (node.ends.isEmpty()) {
                if (node.left==null) return node.right; if (node.right==null) return node.left;
                var next=node.right; while (next.left!=null) next=next.left;
                node.lower=next.lower; node.ends=next.ends; node.right=removeFirst(node.right);
            }
        }
        return balance(node);
    }
    private static RangeNode removeFirst(RangeNode node) { if (node.left==null) return node.right; node.left=removeFirst(node.left); return balance(node); }
    private static int height(RangeNode node) { return node==null ? 0 : node.height; }
    private static RangeNode update(RangeNode node) {
        node.height=1+Math.max(height(node.left),height(node.right)); node.maxUpper=node.ends.lastKey();
        if (node.left!=null) node.maxUpper=maxEnd(node.maxUpper,node.left.maxUpper);
        if (node.right!=null) node.maxUpper=maxEnd(node.maxUpper,node.right.maxUpper); return node;
    }
    private static RangeNode balance(RangeNode node) {
        update(node); int difference=height(node.left)-height(node.right);
        if (difference>1) { if (height(node.left.left)<height(node.left.right)) node.left=rotateLeft(node.left); return rotateRight(node); }
        if (difference< -1) { if (height(node.right.right)<height(node.right.left)) node.right=rotateRight(node.right); return rotateLeft(node); }
        return node;
    }
    private static RangeNode rotateLeft(RangeNode node) { var root=node.right; node.right=root.left; root.left=node; update(node); return update(root); }
    private static RangeNode rotateRight(RangeNode node) { var root=node.left; node.left=root.right; root.right=node; update(node); return update(root); }
}
