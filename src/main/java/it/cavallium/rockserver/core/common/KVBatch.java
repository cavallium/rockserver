package it.cavallium.rockserver.core.common;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public interface KVBatch extends AutoCloseable {

	List<@NotNull Keys> keys();

	List<@NotNull MemorySegment> values();

	@Override
	void close();

	record KVBatchRef(@NotNull List<@NotNull Keys> keys, @NotNull List<@NotNull MemorySegment> values) implements
			KVBatch {

		@Override
		public void close() {

		}
	}

	record KVBatchOwned(Arena arena, @NotNull List<@NotNull Keys> keys,
											@NotNull List<@NotNull MemorySegment> values) implements KVBatch, AutoCloseable {

		@Override
		public void close() {
			arena.close();
		}
	}
}
