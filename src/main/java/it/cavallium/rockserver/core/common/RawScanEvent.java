package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import java.util.Objects;

/** Event emitted by a resumable raw-SST scan. */
public sealed interface RawScanEvent {

	/** One ordinary raw key/value batch. */
	record Batch(Buf serialized) implements RawScanEvent, SerializedKVBatch {
		public Batch {
			Objects.requireNonNull(serialized, "serialized");
		}
	}

	/**
	 * All batches from one SST have been emitted successfully.
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
