package it.cavallium.rockserver.core.common;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import it.cavallium.buffer.Buf;
import java.util.Objects;

public record KV(@NotNull Keys keys, @Nullable Buf value) {
    @Override
    public String toString() {
        return "KV{" + keys + "=" + (value != null ? Utils.toPrettyString(value) : "null") + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KV kv = (KV) o;
        return Objects.equals(keys, kv.keys) && Utils.valueEquals(value, kv.value);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + Objects.hashCode(keys);
        hash = 31 * hash + Utils.valueHash(value);
        return hash;
    }
}
