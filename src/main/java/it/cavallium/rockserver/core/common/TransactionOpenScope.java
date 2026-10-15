package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import java.util.*;

/** Immutable bounded declaration using API column IDs and ordinary logical keys.
 * Points cover their complete physical collision bucket. Ranges use the same
 * half-open key/prefix bounds as range operations; null bounds are unbounded.
 */
public record TransactionOpenScope(List<Span> spans) {
    public static final int MAX_SPANS = 256, MAX_BOUND_BYTES = 2 * 1024 * 1024, MAX_KEY_COMPONENTS = 256;
    public record Span(long columnId, boolean point, Keys lowerInclusive, Keys upperExclusive) {
        public Span {
            if (columnId <= 0 || point && (lowerInclusive == null || upperExclusive != null))
                throw new IllegalArgumentException("Invalid transaction scope span");
            lowerInclusive = copy(lowerInclusive); upperExclusive = copy(upperExclusive);
        }
        public static Span point(long columnId, Keys key) { return new Span(columnId, true, key, null); }
        public static Span range(long columnId, Keys lower, Keys upper) { return new Span(columnId, false, lower, upper); }
        @Override public Keys lowerInclusive() { return copy(lowerInclusive); }
        @Override public Keys upperExclusive() { return copy(upperExclusive); }
    }
    public TransactionOpenScope {
        Objects.requireNonNull(spans);
        if (spans.isEmpty() || spans.size() > MAX_SPANS) throw new IllegalArgumentException("Invalid transaction scope span count");
        long bytes = 0;
        for (var span : spans) {
            Objects.requireNonNull(span);
            for (var keys : Arrays.asList(span.lowerInclusive, span.upperExclusive)) if (keys != null)
                for (var key : keys.keys()) bytes += key.size();
        }
        if (bytes > MAX_BOUND_BYTES) throw new IllegalArgumentException("Transaction scope exceeds byte budget");
        spans = List.copyOf(spans);
    }
    private static Keys copy(Keys keys) {
        if (keys == null) return null;
        if (keys.keys().length > MAX_KEY_COMPONENTS) throw new IllegalArgumentException("Too many scope key components");
        long bytes = 0;
        for (var part : keys.keys()) { bytes += Objects.requireNonNull(part).size(); if (bytes > MAX_BOUND_BYTES) throw new IllegalArgumentException("Scope key exceeds byte budget"); }
        return new Keys(Arrays.stream(keys.keys()).map(part -> Buf.wrap(part.toByteArray())).toArray(Buf[]::new));
    }
}
