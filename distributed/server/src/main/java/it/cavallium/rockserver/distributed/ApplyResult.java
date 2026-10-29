package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/** Outcome of applying one log entry, identical on every replica. */
public sealed interface ApplyResult {

	/**
	 * @param value null, a {@code Buf}, {@code Boolean}, {@code Long}, {@code Delta<Buf>}, or a {@code List} of these
	 */
	record Ok(@Nullable Object value) implements ApplyResult {}

	/** A deterministic rejection, such as a missing column. The log moves on. */
	record Failed(RocksDBErrorType type, String message) implements ApplyResult {
		public Failed {
			Objects.requireNonNull(type, "type");
			Objects.requireNonNull(message, "message");
		}

		public RocksDBException toException() {
			return RocksDBException.of(type, message);
		}
	}

	ApplyResult OK_NULL = new Ok(null);
}
