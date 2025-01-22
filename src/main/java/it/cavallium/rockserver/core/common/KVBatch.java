package it.cavallium.rockserver.core.common;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public interface KVBatch {

	List<@NotNull Keys> keys();

	List<@NotNull MemorySegment> values();

	record KVBatchRef(@NotNull List<@NotNull Keys> keys, @NotNull List<@NotNull MemorySegment> values) implements
			KVBatch {
	}
}
