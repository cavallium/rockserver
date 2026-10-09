package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Canonical bounded big-endian directory encoding; this blob carries catalogue CAS generation. */
public final class RangeDirectoryCodec {
    public static final int FORMAT_VERSION = 1;
    public static final int MAX_RANGES = 1024;
    public static final int MAX_REPLICAS = 16;
    public static final int MAX_COLUMNS = 256;
    public static final int MAX_ENCODED_BYTES = 1024 * 1024;
    static final Comparator<UUID> UUID_ORDER = (a, b) -> {
        int first = Long.compareUnsigned(a.getMostSignificantBits(), b.getMostSignificantBits());
        return first != 0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(), b.getLeastSignificantBits());
    };
    private RangeDirectoryCodec() {}

    public static long encodedSize(RangeDirectory directory) {
        long bytes = 112 + 16L * directory.columns().size();
        for (RangeDescriptor range : directory.ranges()) {
            byte[] lower = range.lowerInclusive(), upper = range.upperExclusive();
            bytes += 61L + (lower == null ? 0 : lower.length) + (upper == null ? 0 : upper.length);
            for (ReplicaDescriptor replica : range.replicas()) bytes += 57L + replica.peerId().length() + replica.address().length();
        }
        return bytes;
    }

    public static byte[] encode(RangeDirectory directory) {
        Objects.requireNonNull(directory, "directory");
        var output = ByteBuffer.allocate((int) encodedSize(directory));
        output.putInt(FORMAT_VERSION);
        putUuid(output, directory.clusterId());
        putUuid(output, directory.databaseId());
        putUuid(output, directory.domainId());
        output.putLong(directory.schemaEpoch()).put(directory.schemaDigest()).putLong(directory.generation());
        output.putInt(directory.routingPrefixBytes()).putInt(directory.columns().size());
        for (UUID column : directory.columns().stream().sorted(UUID_ORDER).toList()) putUuid(output, column);
        output.putInt(directory.ranges().size());
        for (RangeDescriptor range : directory.ranges()) {
            putUuid(output, range.rangeId());
            putUuid(output, range.groupId());
            output.putLong(range.rangeEpoch()).putLong(range.configurationVersion());
            putBound(output, range.lowerInclusive());
            putBound(output, range.upperExclusive());
            output.put((byte) switch (range.topologyState()) { case ACTIVE -> 1; case FENCED -> 2; case RETIRED -> 3; });
            output.putInt(range.replicas().size());
            for (ReplicaDescriptor replica : range.replicas()) {
                putUuid(output, replica.nodeId());
                putUuid(output, replica.nodeIncarnation());
                putUuid(output, replica.replicaId());
                output.put((byte) switch (replica.role()) { case VOTER -> 1; case LISTENER -> 2; });
                putAscii(output, replica.peerId());
                putAscii(output, replica.address());
            }
        }
        return output.array();
    }

    public static RangeDirectory decode(byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length < 112 || encoded.length > MAX_ENCODED_BYTES) throw new IllegalArgumentException("Invalid directory byte size");
        var input = ByteBuffer.wrap(encoded);
        if (input.getInt() != FORMAT_VERSION) throw new IllegalArgumentException("Unknown directory version");
        UUID cluster = getUuid(input), database = getUuid(input), domain = getUuid(input);
        long schemaEpoch = input.getLong();
        byte[] schema = new byte[32];
        input.get(schema);
        long generation = input.getLong();
        int prefix = input.getInt();
        int columnCount = count(input, MAX_COLUMNS, 16, 0);
        var columns = new HashSet<UUID>();
        UUID previous = null;
        for (int i = 0; i < columnCount; i++) {
            UUID column = getUuid(input);
            if (previous != null && UUID_ORDER.compare(previous, column) >= 0) throw new IllegalArgumentException("Noncanonical column order");
            columns.add(column);
            previous = column;
        }
        int rangeCount = count(input, MAX_RANGES, 61, 1);
        var ranges = new ArrayList<RangeDescriptor>(rangeCount);
        byte[] previousLower = null;
        for (int i = 0; i < rangeCount; i++) {
            require(input, 61);
            UUID rangeId = getUuid(input), group = getUuid(input);
            long epoch = input.getLong(), configuration = input.getLong();
            byte[] lower = bound(input), upper = bound(input);
            if (i > 0 && RangeDirectory.compareLower(previousLower, lower) >= 0) throw new IllegalArgumentException("Noncanonical range order");
            previousLower = lower;
            require(input, 5);
            var state = switch (input.get()) {
                case 1 -> RangeDescriptor.TopologyState.ACTIVE; case 2 -> RangeDescriptor.TopologyState.FENCED;
                case 3 -> RangeDescriptor.TopologyState.RETIRED;
                default -> throw new IllegalArgumentException("Unknown range topology state");
            };
            int replicaCount = count(input, MAX_REPLICAS, 57, 1);
            var replicas = new ArrayList<ReplicaDescriptor>(replicaCount);
            UUID previousReplica = null;
            for (int j = 0; j < replicaCount; j++) {
                require(input, 57);
                UUID node = getUuid(input), incarnation = getUuid(input), replica = getUuid(input);
                if (previousReplica != null && UUID_ORDER.compare(previousReplica, replica) >= 0) throw new IllegalArgumentException("Noncanonical replica order");
                previousReplica = replica;
                var role = switch (input.get()) {
                    case 1 -> ReplicaDescriptor.Role.VOTER; case 2 -> ReplicaDescriptor.Role.LISTENER;
                    default -> throw new IllegalArgumentException("Unknown replica role");
                };
                String peer = ascii(input, ReplicaDescriptor.MAX_PEER_BYTES);
                String address = ascii(input, ReplicaDescriptor.MAX_ADDRESS_BYTES);
                replicas.add(new ReplicaDescriptor(node, incarnation, replica, peer, address, role));
            }
            ranges.add(new RangeDescriptor(rangeId, group, epoch, configuration, lower, upper, state, replicas));
        }
        if (input.hasRemaining()) throw new IllegalArgumentException("Trailing directory bytes");
        return new RangeDirectory(cluster, database, domain, schemaEpoch, schema, generation, columns, prefix, ranges);
    }

    static byte[] digest(RangeDirectory directory) {
        try { return MessageDigest.getInstance("SHA-256").digest(encode(directory)); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static int count(ByteBuffer input, int max, int minimumBytes, int minimumCount) {
        require(input, 4);
        int count = input.getInt();
        if (count < minimumCount || count > max || (long) count * minimumBytes > input.remaining()) {
            throw new IllegalArgumentException("Invalid directory collection count");
        }
        return count;
    }
    private static void putBound(ByteBuffer output, byte[] bound) {
        output.putInt(bound == null ? -1 : bound.length);
        if (bound != null) output.put(bound);
    }
    private static byte[] bound(ByteBuffer input) {
        require(input, 4);
        int size = input.getInt();
        if (size == -1) return null;
        if (size < 0 || size > input.remaining()) throw new IllegalArgumentException("Invalid directory bound length");
        byte[] bound = new byte[size];
        input.get(bound);
        return bound;
    }
    private static void putAscii(ByteBuffer output, String string) {
        byte[] bytes = string.getBytes(StandardCharsets.US_ASCII);
        output.putInt(bytes.length).put(bytes);
    }
    private static String ascii(ByteBuffer input, int maximum) {
        require(input, 4);
        int size = input.getInt();
        if (size <= 0 || size > maximum || size > input.remaining()) throw new IllegalArgumentException("Invalid directory string length");
        for (int i = 0; i < size; i++) {
            if (input.get(input.position() + i) < 0) throw new IllegalArgumentException("Non-ASCII directory string");
        }
        byte[] bytes = new byte[size];
        input.get(bytes);
        return new String(bytes, StandardCharsets.US_ASCII);
    }
    private static void require(ByteBuffer input, int bytes) {
        if (input.remaining() < bytes) throw new IllegalArgumentException("Truncated directory");
    }
    private static void putUuid(ByteBuffer output, UUID uuid) { output.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()); }
    private static UUID getUuid(ByteBuffer input) { return new UUID(input.getLong(), input.getLong()); }
}
