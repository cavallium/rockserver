package it.cavallium.rockserver.core.common;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;

public record KVBatch(@NotNull List<@NotNull Keys> keys, @NotNull List<@NotNull MemorySegment> values) {
}
