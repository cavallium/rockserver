package it.cavallium.rockserver.distributed;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

/** The highest log index whose effects are visible in the local database, with waiters. */
final class AppliedIndex {

	private volatile long value;
	private final ConcurrentSkipListMap<Long, CompletableFuture<Void>> waiters = new ConcurrentSkipListMap<>();

	AppliedIndex(long initial) {
		this.value = initial;
	}

	long get() {
		return value;
	}

	/** Called by the apply thread only, with non-decreasing values. */
	void advance(long index) {
		if (index < value) {
			throw new IllegalStateException("Applied index moved backwards: " + value + " -> " + index);
		}
		value = index;
		completeUpTo(index);
	}

	/**
	 * Completes once the applied index reaches {@code index}. Each caller gets a future of its own: waiters of one
	 * index share an internal one, which a caller's timeout or cancellation must not complete for the others.
	 */
	CompletableFuture<Void> await(long index) {
		if (value >= index) {
			return CompletableFuture.completedFuture(null);
		}
		var future = waiters.computeIfAbsent(index, _ -> new CompletableFuture<>());
		// advance() may have run between the check and the registration.
		if (value >= index) {
			completeUpTo(value);
		}
		return future.copy();
	}

	private void completeUpTo(long index) {
		var head = waiters.headMap(index, true);
		for (Map.Entry<Long, CompletableFuture<Void>> entry : head.entrySet()) {
			if (head.remove(entry.getKey(), entry.getValue())) {
				entry.getValue().complete(null);
			}
		}
	}
}
