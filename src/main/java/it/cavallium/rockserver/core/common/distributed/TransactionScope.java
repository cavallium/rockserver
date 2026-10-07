package it.cavallium.rockserver.core.common.distributed;

import java.util.*;

/** Immutable physical access declaration. This model does not allocate or coordinate snapshots. */
public final class TransactionScope {
    public static final int MAX_SPANS = 256;
    public static final int MAX_BOUND_BYTES = 2 * 1024 * 1024;

    public static final class Span {
        private final UUID columnId;
        private final byte[] lower, upper;
        private final boolean point;

        private Span(UUID columnId, byte[] lower, byte[] upper, boolean point) {
            this.columnId = Objects.requireNonNull(columnId);
            if (point && lower == null) throw new IllegalArgumentException("Point scope requires a key");
            if (!point && lower != null && upper != null && Arrays.compareUnsigned(lower, upper) >= 0) {
                throw new IllegalArgumentException("Scope bounds must increase");
            }
            if ((long)(lower == null ? 0 : lower.length) + (upper == null ? 0 : upper.length) > MAX_BOUND_BYTES) {
                throw new IllegalArgumentException("Scope bound budget exceeded");
            }
            this.lower = lower == null ? null : lower.clone();
            this.upper = upper == null ? null : upper.clone();
            this.point = point;
        }
        public static Span point(UUID columnId, byte[] physicalKey) { return new Span(columnId, Objects.requireNonNull(physicalKey), null, true); }
        public static Span range(UUID columnId, byte[] lowerInclusive, byte[] upperExclusive) { return new Span(columnId, lowerInclusive, upperExclusive, false); }
        public static Span prefix(UUID columnId, byte[] physicalPrefix) {
            Objects.requireNonNull(physicalPrefix);
            byte[] upper = physicalPrefix.clone();
            int i = upper.length - 1;
            while (i >= 0 && Byte.toUnsignedInt(upper[i]) == 255) i--;
            upper = i < 0 ? null : Arrays.copyOf(upper, i + 1);
            if (upper != null) upper[i]++;
            return range(columnId, physicalPrefix, upper);
        }
        public UUID columnId() { return columnId; }
        public boolean isPoint() { return point; }
        public byte[] lowerInclusive() { return lower == null ? null : lower.clone(); }
        public byte[] upperExclusive() { return upper == null ? null : upper.clone(); }
        private boolean contains(byte[] key) {
            return point ? Arrays.equals(lower, key) : (lower == null || Arrays.compareUnsigned(lower, key) <= 0)
                    && (upper == null || Arrays.compareUnsigned(key, upper) < 0);
        }
    }

    private final List<Span> spans;
    public TransactionScope(List<Span> spans) {
        Objects.requireNonNull(spans);
        if (spans.isEmpty() || spans.size() > MAX_SPANS) throw new IllegalArgumentException("Invalid scope span count");
        long bytes = 0;
        for (Span span : spans) {
            Objects.requireNonNull(span);
            bytes += (span.lower == null ? 0 : span.lower.length) + (span.upper == null ? 0 : span.upper.length);
        }
        if (bytes > MAX_BOUND_BYTES) throw new IllegalArgumentException("Scope bound budget exceeded");
        this.spans = spans.stream().sorted(Comparator.comparing(Span::columnId).thenComparing(
                (a, b) -> RangeDirectory.compareLower(a.lower, b.lower))).toList();
    }
    public List<Span> spans() { return spans; }

    public boolean contains(UUID column, byte[] physicalKey) {
        Objects.requireNonNull(column); Objects.requireNonNull(physicalKey);
        return spans.stream().anyMatch(span -> span.columnId.equals(column) && span.contains(physicalKey));
    }

    /** A scan/tombstone must be completely covered by declared ranges; individual points grant no scan access. */
    public boolean containsRange(UUID column, byte[] lowerInclusive, byte[] upperExclusive) {
        Objects.requireNonNull(column);
        if (lowerInclusive != null && upperExclusive != null && Arrays.compareUnsigned(lowerInclusive, upperExclusive) >= 0) {
            throw new IllegalArgumentException("Access range bounds must increase");
        }
        byte[] cursor = lowerInclusive;
        boolean covered = false;
        for (Span span : spans) {
            if (!span.columnId.equals(column) || span.point) continue;
            if (span.lower != null && (cursor == null || Arrays.compareUnsigned(span.lower, cursor) > 0)) return false;
            if (span.upper != null && cursor != null && Arrays.compareUnsigned(span.upper, cursor) <= 0) continue;
            covered = true;
            if (span.upper == null || upperExclusive != null && Arrays.compareUnsigned(span.upper, upperExclusive) >= 0) return true;
            cursor = span.upper;
        }
        return covered && upperExclusive != null && cursor != null && Arrays.compareUnsigned(cursor, upperExclusive) >= 0;
    }

    /** Validate the entire declaration and participant budget before any native snapshot acquisition. */
    public List<RangeDescriptor> participants(RangeDirectory directory, int maximumParticipants) {
        Objects.requireNonNull(directory);
        if (maximumParticipants < 1 || maximumParticipants > RangeDirectoryCodec.MAX_RANGES) {
            throw new IllegalArgumentException("Invalid participant budget");
        }
        var participants = new LinkedHashMap<UUID, RangeDescriptor>();
        for (Span span : spans) {
            var ranges = span.point ? List.of(directory.route(span.columnId, span.lower))
                    : directory.intersect(span.columnId, span.lower, span.upper, false);
            for (var range : ranges) {
                participants.putIfAbsent(range.groupId(), range);
                if (participants.size() > maximumParticipants) throw new IllegalArgumentException("Transaction scope exceeds participant budget");
            }
        }
        return List.copyOf(participants.values());
    }
}
