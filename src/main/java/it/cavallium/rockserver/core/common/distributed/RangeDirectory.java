package it.cavallium.rockserver.core.common.distributed;

import java.util.*;

/**
 * Immutable complete routing-domain image; generation is catalogue CAS data, not local consensus.
 * Schema topology policy must authorize physical-bucket boundaries separately from this routing model.
 */
public final class RangeDirectory {
    private final UUID clusterId, databaseId, domainId;
    private final long schemaEpoch, generation;
    private final byte[] schemaDigest, payloadDigest;
    private final Set<UUID> columns;
    private final int routingPrefixBytes;
    private final List<RangeDescriptor> ranges;

    public RangeDirectory(UUID clusterId, UUID databaseId, UUID domainId, long schemaEpoch, byte[] schemaDigest,
                          long generation, Set<UUID> columns, int routingPrefixBytes, List<RangeDescriptor> ranges) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.databaseId = Objects.requireNonNull(databaseId, "databaseId");
        this.domainId = Objects.requireNonNull(domainId, "domainId");
        if (schemaEpoch < 0 || generation < 0 || routingPrefixBytes < 0
                || routingPrefixBytes > RangeDirectoryCodec.MAX_ENCODED_BYTES || schemaDigest.length != 32) {
            throw new IllegalArgumentException("Invalid directory schema/generation/projection");
        }
        Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(ranges, "ranges");
        if (columns.size() > RangeDirectoryCodec.MAX_COLUMNS
                || ranges.isEmpty() || ranges.size() > RangeDirectoryCodec.MAX_RANGES) {
            throw new IllegalArgumentException("Invalid directory column/range counts");
        }
        this.schemaEpoch = schemaEpoch;
        this.generation = generation;
        this.schemaDigest = schemaDigest.clone();
        this.columns = Set.copyOf(columns);
        this.routingPrefixBytes = routingPrefixBytes;
        this.ranges = ranges.stream().sorted((a, b) -> compareLower(a.lowerInclusive(), b.lowerInclusive())).toList();
        var ids = new HashSet<UUID>();
        var groups = new HashSet<UUID>();
        byte[] previous = null;
        for (int i = 0; i < this.ranges.size(); i++) {
            RangeDescriptor range = this.ranges.get(i);
            byte[] lower = range.lowerInclusive();
            byte[] upper = range.upperExclusive();
            if (!ids.add(range.rangeId()) || !groups.add(range.groupId())
                    || (i == 0 ? lower != null : previous == null || !Arrays.equals(previous, lower))
                    || (i == this.ranges.size() - 1 ? upper != null : upper == null)) {
                throw new IllegalArgumentException("Directory requires unique ranges with complete gap-free coverage");
            }
            if (routingPrefixBytes > 0 && ((lower != null && lower.length != routingPrefixBytes)
                    || (upper != null && upper.length != routingPrefixBytes))) {
                throw new IllegalArgumentException("Range boundary width differs from routing prefix");
            }
            previous = upper;
        }
        if (RangeDirectoryCodec.encodedSize(this) > RangeDirectoryCodec.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("Directory exceeds byte limit");
        }
        payloadDigest = RangeDirectoryCodec.digest(this);
    }

    public UUID clusterId() { return clusterId; }
    public UUID databaseId() { return databaseId; }
    public UUID domainId() { return domainId; }
    public long schemaEpoch() { return schemaEpoch; }
    public byte[] schemaDigest() { return schemaDigest.clone(); }
    public long generation() { return generation; }
    public Set<UUID> columns() { return columns; }
    public int routingPrefixBytes() { return routingPrefixBytes; }
    public List<RangeDescriptor> ranges() { return ranges; }
    public byte[] payloadDigest() { return payloadDigest.clone(); }

    public RangeDescriptor route(UUID columnId, byte[] physicalKey) {
        requireColumn(columnId);
        RangeDescriptor result = ranges.get(find(project(physicalKey)));
        requireActive(result);
        return result;
    }

    /** Ordered descriptors for physical [lower,upper), including a partly intersected colocation prefix. */
    public List<RangeDescriptor> intersect(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive, boolean reverse) {
        requireColumn(columnId);
        byte[] lower = lowerInclusive == null ? null : project(lowerInclusive);
        byte[] upper = upperExclusive == null ? null : project(upperExclusive);
        if (lowerInclusive != null && upperExclusive != null) {
            int order = Arrays.compareUnsigned(lowerInclusive, upperExclusive);
            if (order > 0) throw new IllegalArgumentException("Invalid scan bounds");
            if (order == 0) return List.of();
        }
        boolean upperPrefixIncluded = routingPrefixBytes > 0 && upperExclusive != null && upperExclusive.length > routingPrefixBytes;
        var result = new ArrayList<RangeDescriptor>();
        for (int i = lower == null ? 0 : find(lower); i < ranges.size(); i++) {
            RangeDescriptor range = ranges.get(i);
            byte[] start = range.lowerInclusive();
            if (upper != null && start != null) {
                int order = Arrays.compareUnsigned(start, upper);
                if (order > 0 || (order == 0 && !upperPrefixIncluded)) break;
            }
            requireActive(range);
            result.add(range);
        }
        if (reverse) Collections.reverse(result);
        return List.copyOf(result);
    }

    private byte[] project(byte[] physicalKey) {
        Objects.requireNonNull(physicalKey, "physicalKey");
        if (routingPrefixBytes == 0) return physicalKey.clone();
        if (physicalKey.length < routingPrefixBytes) throw new IllegalArgumentException("Physical key shorter than routing prefix");
        return Arrays.copyOf(physicalKey, routingPrefixBytes);
    }

    private int find(byte[] key) {
        int low = 0, high = ranges.size();
        while (low + 1 < high) {
            int middle = (low + high) >>> 1;
            byte[] boundary = ranges.get(middle).lowerInclusive();
            if (Arrays.compareUnsigned(boundary, key) <= 0) low = middle; else high = middle;
        }
        return low;
    }

    private void requireColumn(UUID column) {
        if (!columns.contains(column)) throw new IllegalArgumentException("Column is outside routing domain");
    }
    private static void requireActive(RangeDescriptor range) {
        if (range.topologyState() != RangeDescriptor.TopologyState.ACTIVE) throw new IllegalStateException("Range is not active");
    }
    static int compareLower(byte[] a, byte[] b) {
        return a == null ? (b == null ? 0 : -1) : b == null ? 1 : Arrays.compareUnsigned(a, b);
    }
}
