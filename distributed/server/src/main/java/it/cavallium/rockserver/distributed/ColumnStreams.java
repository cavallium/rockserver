package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * The streams of this node that read a column (range streams and raw scans), so that the apply of the column's
 * deletion can end them instead of waiting for them (DD-7a).
 *
 * <p>A column deletion waits until every use of the column ended ({@code ColumnInstance.stopNewUsesAndWait}), and a
 * stream holds a use from its start to its end, which a slow client or a large scan can make minutes. The deletion runs
 * on the apply thread, so until then the replica applies nothing; on the leader, which serves every stream, that holds
 * every write of the cluster. So the apply ends them first, like the iterators of the column: each one stops reading
 * (its subscription is cancelled, which releases its use of the column as a client's cancel does) and ends with
 * {@code COLUMN_NOT_FOUND}, never as if it had reached the end of the range. A stream that starts while the deletion
 * runs fails at once.</p>
 */
final class ColumnStreams {

	private final Map<Long, Set<Tracked>> byColumn = new HashMap<>();
	private final Set<Long> deleting = new HashSet<>();

	private static final class Tracked {
		final Sinks.Empty<Void> stop = Sinks.empty();
		final AtomicBoolean stopped = new AtomicBoolean();
	}

	/** {@code stream}, ended with {@code COLUMN_NOT_FOUND} if column {@code columnId} is deleted while it runs. */
	<T> Flux<T> track(long columnId, Flux<T> stream) {
		return Flux.defer(() -> {
			var tracked = new Tracked();
			synchronized (this) {
				if (deleting.contains(columnId)) {
					return Flux.error(deletedError(columnId));
				}
				byColumn.computeIfAbsent(columnId, _ -> new HashSet<>()).add(tracked);
			}
			// Not a merge with an error: that would prefetch from the stream. takeUntilOther passes the demand through.
			return stream
					.takeUntilOther(tracked.stop.asMono())
					.concatWith(Mono.defer(() -> tracked.stopped.get() ? Mono.error(deletedError(columnId)) : Mono.empty()))
					.doFinally(_ -> forget(columnId, tracked));
		});
	}

	private synchronized void forget(long columnId, Tracked tracked) {
		var streams = byColumn.get(columnId);
		if (streams != null && streams.remove(tracked) && streams.isEmpty()) {
			byColumn.remove(columnId);
		}
	}

	/**
	 * The apply is about to delete column {@code columnId}: ends its streams, and fails those that start until
	 * {@link #deleted}. Returns how many it ended.
	 */
	int deleting(long columnId) {
		List<Tracked> streams;
		synchronized (this) {
			deleting.add(columnId);
			var tracked = byColumn.remove(columnId);
			streams = tracked == null ? List.of() : List.copyOf(tracked);
		}
		for (var stream : streams) {
			stream.stopped.set(true);
		}
		if (!streams.isEmpty()) {
			// Off the caller, the apply thread: the cancel and the error run the streams' own handlers. The deletion then
			// waits until their uses of the column ended.
			Thread.ofVirtual().name("rockserver-column-streams-stop").start(() -> streams.forEach(s -> s.stop.tryEmitEmpty()));
		}
		return streams.size();
	}

	/** The deletion of column {@code columnId} ended (applied or failed). */
	synchronized void deleted(long columnId) {
		deleting.remove(columnId);
	}

	/** Tests only. */
	synchronized int running(long columnId) {
		var streams = byColumn.get(columnId);
		return streams == null ? 0 : streams.size();
	}

	private static RocksDBException deletedError(long columnId) {
		return RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND,
				"Column " + columnId + " was deleted while this stream read it");
	}
}
