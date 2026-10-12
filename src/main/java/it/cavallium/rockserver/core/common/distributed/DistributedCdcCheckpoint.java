package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;

/** Bounded per-lineage progress. There is deliberately no ordering between different lineages.
 * A topology barrier, rather than an epoch substitution, introduces a successor lineage. */
public final class DistributedCdcCheckpoint {
    public static final int MAX_LINEAGES=1024;
    public static final int ENTRY_BYTES=60, MAX_BYTES=4+MAX_LINEAGES*ENTRY_BYTES;

    public record Lineage(UUID rangeId,UUID groupId,long rangeEpoch) implements Comparable<Lineage> {
        public Lineage {
            Objects.requireNonNull(rangeId); Objects.requireNonNull(groupId);
            if(rangeEpoch<0) throw new IllegalArgumentException("Negative CDC range epoch");
        }
        @Override public int compareTo(Lineage other) {
            int order=rangeId.compareTo(other.rangeId);
            if(order==0) order=groupId.compareTo(other.groupId);
            return order==0 ? Long.compare(rangeEpoch,other.rangeEpoch) : order;
        }
    }

    /** Inclusive position of the last consumed mutation. Term binds the committed log entry.
     * Ordinal MAX_MUTATIONS denotes the end of an entry, including a snapshot/no-effects cut. */
    public record Cursor(ApplyPosition position,int mutationOrdinal) {
        public Cursor {
            Objects.requireNonNull(position);
            boolean beginning=position.term()==0 && position.index()==0 && mutationOrdinal==-1;
            if(!beginning && (position.term()==0 || mutationOrdinal<0 || mutationOrdinal>DistributedCommandCodec.MAX_MUTATIONS))
                throw new IllegalArgumentException("Invalid CDC mutation cursor");
        }
        public static Cursor beginning() { return new Cursor(new ApplyPosition(0,0),-1); }
        public static Cursor after(ApplyPosition cut) { return new Cursor(cut,DistributedCommandCodec.MAX_MUTATIONS); }
        public boolean covers(Cursor earlier) {
            Objects.requireNonNull(earlier);
            if(earlier.equals(beginning())) return true;
            if(equals(beginning())) return false;
            if(position.index()==earlier.position.index()) {
                if(position.term()!=earlier.position.term()) throw new IllegalArgumentException("CDC entry term changed");
                return mutationOrdinal>=earlier.mutationOrdinal;
            }
            if(position.index()>earlier.position.index()) {
                if(position.term()<earlier.position.term()) throw new IllegalArgumentException("CDC term regressed with log progress");
                return true;
            }
            if(position.term()>earlier.position.term()) throw new IllegalArgumentException("CDC term increased before an earlier entry");
            return false;
        }
    }

    private final Map<Lineage,Cursor> cursors;
    public DistributedCdcCheckpoint(Map<Lineage,Cursor> cursors) {
        Objects.requireNonNull(cursors);
        if(cursors.size()>MAX_LINEAGES) throw new IllegalArgumentException("CDC checkpoint lineage capacity exceeded");
        var copy=new TreeMap<Lineage,Cursor>();
        cursors.forEach((lineage,cursor)->copy.put(Objects.requireNonNull(lineage),Objects.requireNonNull(cursor)));
        this.cursors=Collections.unmodifiableMap(copy);
    }
    public Map<Lineage,Cursor> cursors() { return cursors; }

    /** Advance one existing lineage without discarding progress for the others.
     * Polling cannot implicitly add a lineage; subscription/topology admission owns that decision. */
    public DistributedCdcCheckpoint advance(Lineage lineage,Cursor next) {
        Objects.requireNonNull(lineage); Objects.requireNonNull(next);
        var previous=cursors.get(lineage);
        if(previous==null) throw new IllegalArgumentException("CDC lineage is not in this checkpoint");
        if(!next.covers(previous)) throw new IllegalArgumentException("CDC acknowledgement regressed");
        if(next.equals(previous)) return this;
        var copy=new TreeMap<>(cursors); copy.put(lineage,next); return new DistributedCdcCheckpoint(copy);
    }

    /** Replace every affected parent together only after explicit consumer ACK of its terminal boundary. */
    public DistributedCdcCheckpoint handoff(DistributedCdcBarrierHandoff handoff) {
        Objects.requireNonNull(handoff); var copy=new TreeMap<>(cursors);
        for(var entry:handoff.parentTerminal().cursors().entrySet()) {
            var cursor=copy.get(entry.getKey()); if(cursor==null || !cursor.covers(entry.getValue())) throw new IllegalArgumentException("CDC parent terminal boundary has not been acknowledged");
        }
        for(var lineage:handoff.childStarts().cursors().keySet()) if(copy.containsKey(lineage)) throw new IllegalArgumentException("CDC child lineage already present");
        handoff.parentTerminal().cursors().keySet().forEach(copy::remove); copy.putAll(handoff.childStarts().cursors()); return new DistributedCdcCheckpoint(copy);
    }

    public byte[] encode() {
        var bytes=ByteBuffer.allocate(4+cursors.size()*ENTRY_BYTES).putInt(cursors.size());
        cursors.forEach((lineage,cursor)->{
            uuid(bytes,lineage.rangeId()); uuid(bytes,lineage.groupId()); bytes.putLong(lineage.rangeEpoch());
            bytes.putLong(cursor.position().term()).putLong(cursor.position().index()).putInt(cursor.mutationOrdinal());
        });
        return bytes.array();
    }
    public static DistributedCdcCheckpoint decode(byte[] encoded) {
        Objects.requireNonNull(encoded);
        if(encoded.length<4 || encoded.length>MAX_BYTES) throw new IllegalArgumentException("Invalid CDC checkpoint length");
        var bytes=ByteBuffer.wrap(encoded); int count=bytes.getInt();
        if(count<0 || count>MAX_LINEAGES || bytes.remaining()!=count*ENTRY_BYTES) throw new IllegalArgumentException("Invalid CDC checkpoint count");
        var cursors=new TreeMap<Lineage,Cursor>();
        for(int i=0;i<count;i++) {
            var lineage=new Lineage(uuid(bytes),uuid(bytes),bytes.getLong());
            var cursor=new Cursor(new ApplyPosition(bytes.getLong(),bytes.getLong()),bytes.getInt());
            if(cursors.put(lineage,cursor)!=null) throw new IllegalArgumentException("Duplicate CDC lineage");
        }
        var result=new DistributedCdcCheckpoint(cursors);
        if(!Arrays.equals(encoded,result.encode())) throw new IllegalArgumentException("Noncanonical CDC checkpoint order");
        return result;
    }
    @Override public boolean equals(Object other) { return other instanceof DistributedCdcCheckpoint checkpoint && cursors.equals(checkpoint.cursors); }
    @Override public int hashCode() { return cursors.hashCode(); }
    private static void uuid(ByteBuffer bytes,UUID id) { bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer bytes) { return new UUID(bytes.getLong(),bytes.getLong()); }
}
