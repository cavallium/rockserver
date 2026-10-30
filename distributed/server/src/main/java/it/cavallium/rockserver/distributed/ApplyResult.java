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

	/**
	 * Only in a reply, never applied nor recorded: the request's result did not fit in the reply of its batch (see
	 * {@link ResultCodec#encodeAll}). The request took effect and its result is recorded; a copy of it sent alone gets
	 * that result.
	 */
	record ResendAlone() implements ApplyResult {}

	ApplyResult OK_NULL = new Ok(null);

	ApplyResult RESEND_ALONE = new ResendAlone();
}
