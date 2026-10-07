package it.cavallium.rockserver.core.common.distributed;

import it.cavallium.rockserver.core.common.RangeBudget;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Bounded physical rows from a retained native cut; logical bucket/member decoding is a separate layer. */
public final class PhysicalRangePage {
    public static final int MAX_ITEMS = RangeBudget.DEFAULT_MAX_ITEMS;
    public static final int MAX_BYTES = (int)RangeBudget.DEFAULT_MAX_BYTES;
    public static final int HEADER_BYTES = 9;
    public record Row(byte[] key, byte[] value) {
        public Row {
            Objects.requireNonNull(key); Objects.requireNonNull(value);
            if (8L + key.length + value.length > MAX_BYTES) throw new IllegalArgumentException("Physical row exceeds page budget");
            key = key.clone(); value = value.clone();
        }
        @Override public byte[] key() { return key.clone(); }
        @Override public byte[] value() { return value.clone(); }
        @Override public boolean equals(Object other) { return other instanceof Row row && Arrays.equals(key,row.key) && Arrays.equals(value,row.value); }
        @Override public int hashCode() { return 31 * Arrays.hashCode(key) + Arrays.hashCode(value); }
    }
    private final List<Row> rows;
    private final boolean hasMore;
    private final int payloadBytes;
    public PhysicalRangePage(List<Row> rows, boolean hasMore) {
        this.rows = List.copyOf(Objects.requireNonNull(rows));
        if (this.rows.size() > MAX_ITEMS || this.rows.isEmpty() && hasMore) throw new IllegalArgumentException("Invalid physical page cardinality");
        long bytes = HEADER_BYTES;
        for (Row row : this.rows) bytes += 8L + row.key.length + row.value.length;
        if (!this.rows.isEmpty()) bytes += this.rows.getLast().key.length;
        if (bytes > MAX_BYTES) throw new IllegalArgumentException("Physical page exceeds byte budget");
        this.payloadBytes = (int)bytes;
        this.hasMore = hasMore;
    }
    public List<Row> rows() { return rows; }
    public boolean hasMore() { return hasMore; }
    public byte[] resumeAfter() { return rows.isEmpty() ? null : rows.getLast().key(); }
    /** Row and continuation framing bytes; transport identity envelopes are not included. */
    public int payloadBytes() { return payloadBytes; }

    public static final class LimitExceededException extends IllegalStateException {
        private final long minimumBytes;
        public LimitExceededException(long minimumBytes) {
            super("First physical row exceeds page byte budget; requires " + minimumBytes + " bytes");
            this.minimumBytes = minimumBytes;
        }
        public long minimumBytes() { return minimumBytes; }
    }
}
