package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/** Event emitted by a resumable raw-SST scan. */
public sealed interface RawScanEvent {

	/**
	 * One raw key/value batch. The optional completion token is present only on
	 * the final batch of an SST and only after that SST's native reader has been
	 * closed successfully.
	 */
	record Batch(Buf serialized, @Nullable RawSstToken completedSstToken)
			implements RawScanEvent, SerializedKVBatch {

		public Batch(Buf serialized) {
			this(serialized, null);
		}

		public Batch {
			Objects.requireNonNull(serialized, "serialized");
		}
	}

	/**
	 * All batches from one SST have been emitted successfully. This standalone
	 * form is used for an SST with no point-data batch and by legacy transports.
	 *
	 * <p>The client must persist this token only after every earlier batch it
	 * received has been durably applied. Returning a prematurely acknowledged
	 * token can make a later scan skip unapplied data.</p>
	 */
	record SstCompleted(RawSstToken token) implements RawScanEvent {
		public SstCompleted {
			Objects.requireNonNull(token, "token");
		}
	}
}
