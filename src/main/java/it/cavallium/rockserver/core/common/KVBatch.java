package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public interface KVBatch {

	List<@NotNull Keys> keys();

	List<@NotNull Buf> values();

	record KVBatchRef(@NotNull List<@NotNull Keys> keys, @NotNull List<@NotNull Buf> values) implements
			KVBatch {
	}
}
