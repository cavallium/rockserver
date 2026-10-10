package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Immutable subscription and monotone consumer progress. Delivered bounds are validated here;
 * their provenance and durable CAS/receipt ownership belong to the subscription service. */
public final class DistributedCdcSubscription {
    public static final int MAX_COLUMNS=256,HEADER_BYTES=21;
    public static final int MAX_BYTES=HEADER_BYTES+Definition.MAX_BYTES+2*DistributedCdcCheckpoint.MAX_BYTES;
    public enum Phase { ACTIVE, CLOSED }

    public record Definition(UUID clusterId,UUID databaseId,UUID domainId,UUID subscriptionId,UUID incarnationId,
            byte[] directoryDigest,List<UUID> columns,DistributedCdcCheckpoint start) {
        public static final int HEADER_BYTES=120,MAX_BYTES=HEADER_BYTES+16*MAX_COLUMNS+DistributedCdcCheckpoint.MAX_BYTES;
        public Definition {
            Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(domainId);
            Objects.requireNonNull(subscriptionId); Objects.requireNonNull(incarnationId); Objects.requireNonNull(directoryDigest); Objects.requireNonNull(start);
            if(directoryDigest.length!=32 || columns.isEmpty() || columns.size()>MAX_COLUMNS || start.cursors().isEmpty()) throw new IllegalArgumentException("Invalid CDC subscription definition");
            var selected=new TreeSet<UUID>(); for(var column:columns) if(!selected.add(Objects.requireNonNull(column))) throw new IllegalArgumentException("Duplicate CDC selected column");
            columns=List.copyOf(selected); directoryDigest=directoryDigest.clone();
        }
        /** Bind all current ACTIVE lineages. A successor topology needs an explicit barrier transition. */
        public static Definition create(UUID subscription,UUID incarnation,RangeDirectory directory,Collection<UUID> columns,DistributedCdcCheckpoint start) {
            Objects.requireNonNull(directory); Objects.requireNonNull(start);
            if(!directory.columns().containsAll(columns)) throw new IllegalArgumentException("CDC selection names a foreign column");
            var lineages=new HashSet<DistributedCdcCheckpoint.Lineage>();
            for(var range:directory.ranges()) {
                if(range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("CDC definition requires active topology");
                lineages.add(new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()));
            }
            if(!lineages.equals(start.cursors().keySet())) throw new IllegalArgumentException("CDC initial checkpoint coverage differs from directory");
            byte[] digest;
            try { digest=MessageDigest.getInstance("SHA-256").digest(RangeDirectoryCodec.encode(directory)); }
            catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
            return new Definition(directory.clusterId(),directory.databaseId(),directory.domainId(),subscription,incarnation,digest,List.copyOf(columns),start);
        }
        @Override public byte[] directoryDigest() { return directoryDigest.clone(); }
        public byte[] encode() {
            byte[] checkpoint=start.encode(); var bytes=ByteBuffer.allocate(HEADER_BYTES+16*columns.size()+checkpoint.length);
            uuid(bytes,clusterId); uuid(bytes,databaseId); uuid(bytes,domainId); uuid(bytes,subscriptionId); uuid(bytes,incarnationId);
            bytes.put(directoryDigest).putInt(columns.size()); for(var column:columns) uuid(bytes,column);
            return bytes.putInt(checkpoint.length).put(checkpoint).array();
        }
        public static Definition decode(byte[] encoded) {
            Objects.requireNonNull(encoded);
            if(encoded.length<HEADER_BYTES+16+4 || encoded.length>MAX_BYTES) throw new IllegalArgumentException("Invalid CDC definition length");
            var bytes=ByteBuffer.wrap(encoded); UUID cluster=uuid(bytes),database=uuid(bytes),domain=uuid(bytes),subscription=uuid(bytes),incarnation=uuid(bytes);
            byte[] digest=new byte[32]; bytes.get(digest); int count=bytes.getInt();
            if(count<1 || count>MAX_COLUMNS || count*16L>bytes.remaining()-4L) throw new IllegalArgumentException("Invalid CDC column count");
            var columns=new ArrayList<UUID>(count); for(int i=0;i<count;i++) columns.add(uuid(bytes));
            int size=bytes.getInt(); if(size<4 || size>DistributedCdcCheckpoint.MAX_BYTES || size!=bytes.remaining()) throw new IllegalArgumentException("Invalid CDC definition checkpoint length");
            byte[] checkpoint=new byte[size]; bytes.get(checkpoint);
            var result=new Definition(cluster,database,domain,subscription,incarnation,digest,columns,DistributedCdcCheckpoint.decode(checkpoint));
            if(!Arrays.equals(encoded,result.encode())) throw new IllegalArgumentException("Noncanonical CDC definition"); return result;
        }
        @Override public boolean equals(Object other) {
            return other instanceof Definition definition && clusterId.equals(definition.clusterId) && databaseId.equals(definition.databaseId) && domainId.equals(definition.domainId)
                    && subscriptionId.equals(definition.subscriptionId) && incarnationId.equals(definition.incarnationId) && Arrays.equals(directoryDigest,definition.directoryDigest)
                    && columns.equals(definition.columns) && start.equals(definition.start);
        }
        @Override public int hashCode() { return 31*Objects.hash(clusterId,databaseId,domainId,subscriptionId,incarnationId,columns,start)+Arrays.hashCode(directoryDigest); }
    }

    private final Definition definition;
    private final Phase phase;
    private final long generation;
    private final DistributedCdcCheckpoint checkpoint,delivered;
    private DistributedCdcSubscription(Definition definition,Phase phase,long generation,DistributedCdcCheckpoint checkpoint,DistributedCdcCheckpoint delivered) {
        this.definition=Objects.requireNonNull(definition); this.phase=Objects.requireNonNull(phase); this.generation=generation;
        this.checkpoint=Objects.requireNonNull(checkpoint); this.delivered=Objects.requireNonNull(delivered);
        if(generation<0 || !checkpoint.cursors().keySet().equals(definition.start().cursors().keySet()) || !delivered.cursors().keySet().equals(checkpoint.cursors().keySet())) throw new IllegalArgumentException("CDC subscription generation/lineages changed");
        for(var entry:checkpoint.cursors().entrySet()) {
            if(!entry.getValue().covers(definition.start().cursors().get(entry.getKey()))) throw new IllegalArgumentException("CDC ACK precedes initial checkpoint");
            if(!delivered.cursors().get(entry.getKey()).covers(entry.getValue())) throw new IllegalArgumentException("CDC ACK exceeds persisted delivery");
        }
        boolean initial=delivered.equals(definition.start());
        long minimum=(checkpoint.equals(definition.start()) ? 1 : 2)+(phase==Phase.CLOSED ? 1 : 0);
        if(initial ? generation!=(phase==Phase.ACTIVE ? 0 : 1) : generation<minimum) throw new IllegalArgumentException("CDC generation differs from delivery/ACK/closure");
    }
    public static DistributedCdcSubscription initial(Definition definition) { return new DistributedCdcSubscription(definition,Phase.ACTIVE,0,definition.start(),definition.start()); }
    public Definition definition() { return definition; }
    public Phase phase() { return phase; }
    public long generation() { return generation; }
    public DistributedCdcCheckpoint checkpoint() { return checkpoint; }
    public DistributedCdcCheckpoint delivered() { return delivered; }

    /** Persist complete lineage coverage before a consumer can acknowledge new progress. */
    public DistributedCdcSubscription deliver(DistributedCdcCheckpoint next) {
        requireActive(); Objects.requireNonNull(next);
        if(!next.cursors().keySet().equals(delivered.cursors().keySet())) throw new IllegalArgumentException("CDC delivery changed lineage coverage");
        if(next.equals(delivered)) return this;
        for(var entry:next.cursors().entrySet()) if(!entry.getValue().covers(delivered.cursors().get(entry.getKey()))) throw new IllegalArgumentException("CDC delivery regressed");
        return new DistributedCdcSubscription(definition,phase,Math.incrementExact(generation),checkpoint,next);
    }
    public DistributedCdcSubscription acknowledge(DistributedCdcCheckpoint next) {
        requireActive(); Objects.requireNonNull(next);
        if(!next.cursors().keySet().equals(checkpoint.cursors().keySet())) throw new IllegalArgumentException("CDC ACK changed lineage coverage");
        if(next.equals(checkpoint)) return this;
        for(var entry:next.cursors().entrySet()) {
            if(!entry.getValue().covers(checkpoint.cursors().get(entry.getKey()))) throw new IllegalArgumentException("CDC ACK regressed");
            if(!delivered.cursors().get(entry.getKey()).covers(entry.getValue())) throw new IllegalArgumentException("CDC ACK exceeds persisted delivery");
        }
        return new DistributedCdcSubscription(definition,phase,Math.incrementExact(generation),next,delivered);
    }
    public DistributedCdcSubscription close() { return phase==Phase.CLOSED ? this : new DistributedCdcSubscription(definition,Phase.CLOSED,Math.incrementExact(generation),checkpoint,delivered); }
    public void requireSuccessorOf(DistributedCdcSubscription previous) {
        Objects.requireNonNull(previous);
        if(!definition.equals(previous.definition) || previous.generation==Long.MAX_VALUE || generation!=previous.generation+1) throw new IllegalArgumentException("CDC successor changed definition/incarnation/generation");
        previous.requireActive(); final DistributedCdcSubscription expected;
        if(phase==Phase.CLOSED) expected=previous.close();
        else {
            boolean deliveryChanged=!delivered.equals(previous.delivered),ackChanged=!checkpoint.equals(previous.checkpoint);
            if(deliveryChanged==ackChanged) throw new IllegalArgumentException("CDC successor requires exactly one delivery or ACK");
            expected=deliveryChanged ? previous.deliver(delivered) : previous.acknowledge(checkpoint);
        }
        if(!equals(expected)) throw new IllegalArgumentException("CDC successor changed closure/delivery/ACK");
    }
    private void requireActive() { if(phase!=Phase.ACTIVE) throw new IllegalStateException("CDC subscription is closed"); }
    public byte[] encode() {
        byte[] definition=this.definition.encode(),checkpoint=this.checkpoint.encode(),delivered=this.delivered.encode();
        return ByteBuffer.allocate(HEADER_BYTES+definition.length+checkpoint.length+delivered.length).putLong(generation).put((byte)phase.ordinal()).putInt(definition.length).put(definition).putInt(checkpoint.length).put(checkpoint).putInt(delivered.length).put(delivered).array();
    }
    public static DistributedCdcSubscription decode(byte[] encoded) {
        Objects.requireNonNull(encoded);
        if(encoded.length<HEADER_BYTES+Definition.HEADER_BYTES+16+12 || encoded.length>MAX_BYTES) throw new IllegalArgumentException("Invalid CDC subscription length");
        var bytes=ByteBuffer.wrap(encoded); long generation=bytes.getLong(); int phase=bytes.get()&255;
        if(phase>=Phase.values().length) throw new IllegalArgumentException("Invalid CDC subscription phase");
        int size=bytes.getInt(); if(size<Definition.HEADER_BYTES+16+4 || size>Definition.MAX_BYTES || size>bytes.remaining()-8) throw new IllegalArgumentException("Invalid CDC subscription definition length");
        byte[] definition=new byte[size]; bytes.get(definition); size=bytes.getInt();
        if(size<4 || size>DistributedCdcCheckpoint.MAX_BYTES || size>bytes.remaining()-4) throw new IllegalArgumentException("Invalid CDC ACK checkpoint length");
        byte[] checkpoint=new byte[size]; bytes.get(checkpoint); size=bytes.getInt();
        if(size<4 || size>DistributedCdcCheckpoint.MAX_BYTES || size!=bytes.remaining()) throw new IllegalArgumentException("Invalid CDC delivered checkpoint length");
        byte[] delivered=new byte[size]; bytes.get(delivered);
        return new DistributedCdcSubscription(Definition.decode(definition),Phase.values()[phase],generation,DistributedCdcCheckpoint.decode(checkpoint),DistributedCdcCheckpoint.decode(delivered));
    }
    @Override public boolean equals(Object other) { return other instanceof DistributedCdcSubscription subscription && definition.equals(subscription.definition) && phase==subscription.phase && generation==subscription.generation && checkpoint.equals(subscription.checkpoint) && delivered.equals(subscription.delivered); }
    @Override public int hashCode() { return Objects.hash(definition,phase,generation,checkpoint,delivered); }
    private static UUID uuid(ByteBuffer bytes) { return new UUID(bytes.getLong(),bytes.getLong()); }
    private static void uuid(ByteBuffer bytes,UUID id) { bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
}
