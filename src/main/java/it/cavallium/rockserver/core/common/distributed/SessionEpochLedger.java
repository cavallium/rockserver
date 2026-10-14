package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Catalogue lifetime state. Time advances only through committed, explicit monotonic transitions. */
public record SessionEpochLedger(UUID clusterId, UUID databaseId, long generation, long logicalTimeMillis,
                                 long minimumEpoch, long nextEpoch, List<Grant> grants) {
    public static final int MAX_GRANTS = 1024;
    public static final long MAX_LIFETIME_MILLIS = 86_400_000;
    public static final int MAX_BYTES = 72 + MAX_GRANTS * 40;
    private static final int MAGIC = 0x5345504c;
    public record Grant(long epoch, UUID sessionId, long issuedAtMillis, long expiresAtMillis) {
        public Grant {
            Objects.requireNonNull(sessionId);
            if (epoch < 1 || issuedAtMillis < 0 || expiresAtMillis <= issuedAtMillis
                    || expiresAtMillis - issuedAtMillis > MAX_LIFETIME_MILLIS) throw new IllegalArgumentException("Invalid session grant");
        }
    }
    public SessionEpochLedger {
        Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId);
        grants = List.copyOf(grants);
        if (generation < 0 || logicalTimeMillis < 0 || minimumEpoch < 1 || nextEpoch < minimumEpoch
                || nextEpoch - minimumEpoch != grants.size() || grants.size() > MAX_GRANTS) {
            throw new IllegalArgumentException("Invalid epoch ledger bounds");
        }
        for (int i = 0; i < grants.size(); i++) {
            Grant grant = grants.get(i);
            if (grant.epoch() != minimumEpoch + i || grant.issuedAtMillis() > logicalTimeMillis
                    || i == 0 && grant.expiresAtMillis() <= logicalTimeMillis) throw new IllegalArgumentException("Noncanonical session grants");
        }
    }
    public static SessionEpochLedger initial(UUID cluster, UUID database, long logicalTimeMillis) {
        return new SessionEpochLedger(cluster,database,0,logicalTimeMillis,1,1,List.of());
    }
    public SessionEpochLedger issue(UUID session, long expiresAtMillis) {
        if (nextEpoch == Long.MAX_VALUE || grants.size() == MAX_GRANTS) throw new IllegalStateException("Session issuance capacity exhausted");
        var next = new java.util.ArrayList<>(grants);
        next.add(new Grant(nextEpoch,session,logicalTimeMillis,expiresAtMillis));
        return new SessionEpochLedger(clusterId,databaseId,Math.addExact(generation,1),logicalTimeMillis,minimumEpoch,nextEpoch+1,next);
    }
    public SessionEpochLedger advanceTime(long proposedTimeMillis) {
        if (proposedTimeMillis < 0) throw new IllegalArgumentException("Negative catalogue time");
        long time = Math.max(logicalTimeMillis,proposedTimeMillis);
        int removed = 0;
        while (removed < grants.size() && grants.get(removed).expiresAtMillis() <= time) removed++;
        return new SessionEpochLedger(clusterId,databaseId,Math.addExact(generation,1),time,minimumEpoch+removed,nextEpoch,
                grants.subList(removed,grants.size()));
    }
    public boolean isLive(OperationId operation) {
        long epoch = operation.epoch();
        if (epoch < minimumEpoch || epoch >= nextEpoch) return false;
        Grant grant = grants.get((int)(epoch-minimumEpoch));
        return grant.sessionId().equals(operation.sessionId()) && grant.expiresAtMillis() > logicalTimeMillis;
    }
    /** A CAS can create the initial empty image, append one grant, or advance time/prune its expired prefix. */
    public void requireSuccessorOf(SessionEpochLedger previous) {
        if (previous == null) {
            if (generation != 0 || minimumEpoch != 1 || nextEpoch != 1 || !grants.isEmpty()) throw new IllegalArgumentException("Invalid initial epoch ledger");
            return;
        }
        if (!clusterId.equals(previous.clusterId) || !databaseId.equals(previous.databaseId)
                || previous.generation == Long.MAX_VALUE || generation != previous.generation+1) throw new IllegalArgumentException("Invalid ledger identity/generation");
        if (nextEpoch == previous.nextEpoch) {
            if (!equals(previous.advanceTime(logicalTimeMillis))) throw new IllegalArgumentException("Invalid lifetime transition");
        } else {
            if (previous.nextEpoch == Long.MAX_VALUE || nextEpoch != previous.nextEpoch+1 || grants.isEmpty()
                    || !equals(previous.issue(grants.getLast().sessionId(),grants.getLast().expiresAtMillis()))) {
                throw new IllegalArgumentException("Invalid issuance transition");
            }
        }
    }
    public byte[] encode() {
        var output = ByteBuffer.allocate(72+grants.size()*40).putInt(MAGIC);
        uuid(output,clusterId); uuid(output,databaseId);
        output.putLong(generation).putLong(logicalTimeMillis).putLong(minimumEpoch).putLong(nextEpoch).putInt(grants.size());
        for (Grant grant : grants) {
            output.putLong(grant.epoch()); uuid(output,grant.sessionId()); output.putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis());
        }
        return output.array();
    }
    public static SessionEpochLedger decode(byte[] bytes) {
        if (bytes.length < 72 || bytes.length > MAX_BYTES) throw new IllegalArgumentException("Invalid ledger byte size");
        var input = ByteBuffer.wrap(bytes);
        if (input.getInt() != MAGIC) throw new IllegalArgumentException("Unsupported epoch ledger");
        UUID cluster = uuid(input), database = uuid(input);
        long generation = input.getLong(), time = input.getLong(), minimum = input.getLong(), next = input.getLong();
        int count = input.getInt();
        if (count < 0 || count > MAX_GRANTS || input.remaining() != count*40) throw new IllegalArgumentException("Invalid ledger grant count");
        var grants = new java.util.ArrayList<Grant>(count);
        for (int i=0;i<count;i++) grants.add(new Grant(input.getLong(),uuid(input),input.getLong(),input.getLong()));
        return new SessionEpochLedger(cluster,database,generation,time,minimum,next,grants);
    }
    private static void uuid(ByteBuffer output, UUID uuid) { output.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer input) { return new UUID(input.getLong(),input.getLong()); }
}
