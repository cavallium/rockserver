package it.cavallium.rockserver.core.common.distributed;

import java.util.*;

/** Ephemeral range history; caller serializes mutations and validation under its native apply lane. */
public final class RangeMutationHistory {
    public record Limits(int maxPins,int maxEntries,long maxHistoryBytes,long maxScopeBytes) {
        public Limits {
            if (maxPins<1 || maxPins>4096 || maxEntries<1 || maxEntries>1_048_576
                    || maxHistoryBytes<1 || maxHistoryBytes>256L*1024*1024 || maxScopeBytes<1 || maxScopeBytes>256L*1024*1024) {
                throw new IllegalArgumentException("Invalid bounded mutation history limits");
            }
        }
        public static Limits defaults() { return new Limits(128,65536,16L*1024*1024,16L*1024*1024); }
    }
    public enum Status { ACTIVE, CONFLICT, EXPIRED, INVALIDATED, CLOSED }
    public static final class Pin {
        private final RangeMutationHistory owner;
        private final TransactionScope scope;
        private final ApplyPosition position;
        private final long scopeBytes;
        private volatile Status status=Status.ACTIVE;
        private Pin(RangeMutationHistory owner,TransactionScope scope,ApplyPosition position,long scopeBytes) {
            this.owner=owner; this.scope=scope; this.position=position; this.scopeBytes=scopeBytes;
        }
        public TransactionScope scope() { return scope; }
        public ApplyPosition position() { return position; }
        public Status status() { return status; }
    }
    public record Stats(int activePins,int entries,long historyBytes,long scopeBytes) {}
    // Fixed accounting, independent of pin count: 128 per point plus key bytes; 128 per pin +64/span +bound bytes.
    private static final int POINT_BYTES=128, PIN_BYTES=128, SPAN_BYTES=64;
    private static final class Bytes implements Comparable<Bytes> {
        private final byte[] value;
        Bytes(byte[] value) { this.value=value; }
        @Override public int compareTo(Bytes other) { return Arrays.compareUnsigned(value,other.value); }
        @Override public boolean equals(Object other) { return other instanceof Bytes bytes && Arrays.equals(value,bytes.value); }
        @Override public int hashCode() { return Arrays.hashCode(value); }
    }
    private record Point(UUID column,Bytes key) {}
    private static final Comparator<Point> POINT_ORDER=Comparator.comparing(Point::column).thenComparing(Point::key);
    private record Stamp(Point point,long index) {}
    /** Immutable pre-write footprint; an oversized batch retains only an expire-all marker. */
    public static final class PreparedWrites {
        private final RangeMutationHistory owner;
        private final List<Point> points;
        private final boolean range,oversized;
        private PreparedWrites(RangeMutationHistory owner,List<Point> points,boolean range,boolean oversized) {
            this.owner=owner; this.points=points; this.range=range; this.oversized=oversized;
        }
    }
    private final Limits limits;
    private final LinkedHashSet<Pin> active=new LinkedHashSet<>();
    private final Map<UUID,NavigableMap<Bytes,Stamp>> columns=new HashMap<>();
    private final NavigableMap<Long,Set<Point>> byIndex=new TreeMap<>();
    private ApplyPosition observed;
    private int entries;
    private long historyBytes,scopeBytes;

    public RangeMutationHistory() { this(Limits.defaults()); }
    public RangeMutationHistory(Limits limits) { this.limits=Objects.requireNonNull(limits); }
    /** Must run before native snapshot acquisition; refusal does not evict existing owners. */
    public void checkOpenCapacity(TransactionScope scope) {
        long bytes=scopeSize(scope);
        if (active.size()>=limits.maxPins() || bytes>limits.maxScopeBytes()-scopeBytes) throw new IllegalStateException("Mutation history pin/scope admission exhausted");
    }
    public Pin open(ApplyPosition cut,TransactionScope scope) {
        requirePosition(cut);
        if (observed!=null && (cut.index()<observed.index() || cut.term()<observed.term()
                || !active.isEmpty() && cut.term()!=observed.term())) throw new IllegalArgumentException("Pin cut regressed or changed term before explicit advance");
        checkOpenCapacity(scope);
        long bytes=scopeSize(scope); var pin=new Pin(this,scope,cut,bytes);
        observed=cut; active.add(pin); scopeBytes+=bytes; prune(); return pin;
    }
    /** Every completed marker advances the watermark, even when it produced no actual writes. */
    public List<Pin> advance(ApplyPosition position,List<TransactionScope.Span> actualWrites) {
        return advance(position,prepare(actualWrites));
    }
    /** Publishes a footprint prepared before the native batch; committed effects are never quota-rejected. */
    public List<Pin> advance(ApplyPosition position,PreparedWrites writes) {
        requirePosition(position);
        if (observed!=null && (position.index()<=observed.index() || position.term()<observed.term())) throw new IllegalArgumentException("Mutation history requires a new monotonic apply position");
        if (writes==null || writes.owner!=this) throw new IllegalArgumentException("Prepared writes belong to another mutation history");
        var retired=new ArrayList<Pin>();
        if (observed!=null && position.term()!=observed.term()) retired.addAll(invalidateAll(Status.INVALIDATED));
        observed=position;
        if (writes.range) retired.addAll(invalidateAll(Status.CONFLICT));
        else if (writes.oversized) retired.addAll(invalidateAll(Status.EXPIRED));
        else if (!active.isEmpty()) {
            for (var point:writes.points) stamp(point,position.index());
            prune();
            while (entries>limits.maxEntries() || historyBytes>limits.maxHistoryBytes()) {
                var oldest=active.iterator().next(); retire(oldest,Status.EXPIRED); retired.add(oldest); prune();
            }
        }
        if (active.isEmpty()) clearHistory();
        return List.copyOf(retired);
    }
    /** Checks all declared points/ranges, including keys that were absent or unchanged at the cut. */
    public Status validate(Pin pin) {
        requireOwner(pin);
        if (pin.status!=Status.ACTIVE) return pin.status;
        for (var span:pin.scope.spans()) {
            var points=columns.get(span.columnId()); if (points==null) continue;
            byte[] lower=span.lowerInclusive(), upper=span.upperExclusive();
            if (span.isPoint()) {
                var stamp=points.get(new Bytes(lower));
                if (stamp!=null && stamp.index()>pin.position.index()) { retire(pin,Status.CONFLICT); prune(); return pin.status; }
            } else {
                NavigableMap<Bytes,Stamp> selected=lower==null ? points : points.tailMap(new Bytes(lower),true);
                if (upper!=null) selected=selected.headMap(new Bytes(upper),false);
                for (var stamp:selected.values()) if (stamp.index()>pin.position.index()) { retire(pin,Status.CONFLICT); prune(); return pin.status; }
            }
        }
        return Status.ACTIVE;
    }
    public void close(Pin pin) { requireOwner(pin); if (pin.status==Status.ACTIVE) { retire(pin,Status.CLOSED); prune(); } }
    public List<Pin> invalidateAll(Status status) {
        if (status==null || status==Status.ACTIVE) throw new IllegalArgumentException("Invalid mutation history retirement status");
        var retired=List.copyOf(active); for (var pin:retired) pin.status=status;
        active.clear(); scopeBytes=0; clearHistory(); return retired;
    }
    public List<Pin> reset() { var retired=invalidateAll(Status.INVALIDATED); observed=null; return retired; }
    public static long accountedScopeBytes(TransactionScope scope) { return scopeSize(scope); }
    public Stats stats() { return new Stats(active.size(),entries,historyBytes,scopeBytes); }

    /** Validates and owns actual-key bytes without publishing history or changing pin state. */
    public PreparedWrites prepare(List<TransactionScope.Span> writes) {
        if (writes==null) throw new IllegalArgumentException("Missing actual writes");
        var points=new TreeSet<Point>(POINT_ORDER); long bytes=0; boolean range=false,oversized=false;
        for (var span:writes) {
            if (span==null) throw new IllegalArgumentException("Null actual write span");
            if (!span.isPoint()) { range=true; points.clear(); continue; }
            if (range || oversized) continue;
            var point=new Point(span.columnId(),new Bytes(span.lowerInclusive()));
            if (points.add(point)) {
                bytes+=POINT_BYTES+(long)point.key.value.length;
                if (points.size()>limits.maxEntries() || bytes>limits.maxHistoryBytes()) { oversized=true; points.clear(); }
            }
        }
        return new PreparedWrites(this,List.copyOf(points),range,oversized);
    }
    private void stamp(Point candidate,long index) {
        var points=columns.computeIfAbsent(candidate.column(),ignored -> new TreeMap<>()); var previous=points.get(candidate.key());
        Point point=candidate;
        if (previous!=null) {
            point=previous.point(); var old=byIndex.get(previous.index()); old.remove(point); if (old.isEmpty()) byIndex.remove(previous.index());
        } else { entries++; historyBytes+=POINT_BYTES+(long)candidate.key.value.length; }
        points.put(point.key(),new Stamp(point,index)); byIndex.computeIfAbsent(index,ignored -> new TreeSet<>(POINT_ORDER)).add(point);
    }
    /** The second index removes only obsolete latest stamps; it never scans all point maps to collect history. */
    private void prune() {
        if (active.isEmpty()) { clearHistory(); return; }
        long cutoff=active.iterator().next().position.index();
        while (!byIndex.isEmpty() && byIndex.firstKey()<=cutoff) {
            var expired=byIndex.pollFirstEntry();
            for (var point:expired.getValue()) {
                var points=columns.get(point.column()); points.remove(point.key()); if (points.isEmpty()) columns.remove(point.column());
                entries--; historyBytes-=POINT_BYTES+(long)point.key.value.length;
            }
        }
    }
    private void retire(Pin pin,Status status) { active.remove(pin); scopeBytes-=pin.scopeBytes; pin.status=status; }
    private void clearHistory() { columns.clear(); byIndex.clear(); entries=0; historyBytes=0; }
    private void requireOwner(Pin pin) { if (pin==null || pin.owner!=this) throw new IllegalArgumentException("Pin belongs to another mutation history"); }
    private static void requirePosition(ApplyPosition position) { if (position==null || position.term()<1) throw new IllegalArgumentException("Mutation history requires a positive-term apply cut"); }
    private static long scopeSize(TransactionScope scope) {
        if (scope==null) throw new IllegalArgumentException("Missing transaction scope");
        long bytes=PIN_BYTES;
        for (var span:scope.spans()) { byte[] lower=span.lowerInclusive(), upper=span.upperExclusive(); bytes+=SPAN_BYTES+(long)(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length); }
        return bytes;
    }
}
