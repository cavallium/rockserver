package it.cavallium.rockserver.core.common.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import java.util.Objects;

/** Equality against the original captured image. Null is absent; empty is present empty. */
public record AtomicPredicate(long column, Keys key, Buf expected) {
    public AtomicPredicate { Objects.requireNonNull(key); }
}
