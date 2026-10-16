package it.cavallium.rockserver.core.common.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import java.util.Objects;

/** One ordered logical atomic write. Null PUT input supports key-only columns; DELETE has no input. */
public record AtomicMutation(long column, Keys key, Mutation.Kind kind, Buf value, Mutation.ReturnMode returns) {
    public AtomicMutation {
        Objects.requireNonNull(key); Objects.requireNonNull(kind); Objects.requireNonNull(returns);
        if (kind != Mutation.Kind.PUT && kind != Mutation.Kind.DELETE && kind != Mutation.Kind.MERGE
                || kind == Mutation.Kind.DELETE && value != null || kind == Mutation.Kind.MERGE && value == null) throw new IllegalArgumentException("Invalid atomic mutation");
    }
}
