package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.ClientRequestId;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * A log entry as proposed, with the identity that makes it take effect at most once.
 *
 * <p>A proposer (one per node process) numbers its requests. A retried request keeps its number, so when an
 * earlier attempt was already applied (perhaps under another leader, before a restart), applying the retry returns
 * the result recorded the first time instead of executing the entry again. {@code completedBelow} tells every
 * replica that the proposer will never retry requests numbered below it, so their records can be deleted.</p>
 *
 * <p>A write of a client session carries the client's own numbering in {@code client}: then that numbering, not the
 * proposer's, identifies it at apply. The client resends it, through any node, with the same id, so a copy proposed by
 * another node process is still recognized.</p>
 *
 * @param proposerId random id of the proposing process
 * @param sequence number of this request within its proposer
 * @param completedBelow every request of this proposer numbered below this has its final answer
 * @param entry the command
 * @param client the client session's id of this write, null for a request that has none
 */
record Request(long proposerId, long sequence, long completedBelow, LogEntry entry, @Nullable ClientRequestId client) {

	Request {
		Objects.requireNonNull(entry, "entry");
		if (sequence < 0 || completedBelow < 0 || completedBelow > sequence) {
			throw new IllegalArgumentException("Invalid request numbering: " + sequence + ", completed below "
					+ completedBelow);
		}
	}

	/** A request of the proposer alone, with no client session. */
	Request(long proposerId, long sequence, long completedBelow, LogEntry entry) {
		this(proposerId, sequence, completedBelow, entry, null);
	}
}
