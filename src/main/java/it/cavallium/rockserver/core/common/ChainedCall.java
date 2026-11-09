package it.cavallium.rockserver.core.common;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * The future of two calls made one after the other, the second from the first one's result, whose {@link #cancel}
 * reaches the call in flight, as a single call's does (R48-3, {@link RocksDBAsyncAPI#ensureMergeOperatorAsync}). A
 * {@code thenCompose} future does not: cancelling it completes it, "cancelled", which reads "not applied", while the
 * calls run on.
 *
 * <ul>
 *   <li>Before the second call is made, a cancel stops the first one, the second is never made, and this future is
 *   cancelled. The first call must be one that changes nothing (a read, such as a check).</li>
 *   <li>After, it is the second call's own cancel, and this future ends as that call does: a numbered write that sent a
 *   copy refuses the cancel and ends OUTCOME_UNKNOWN (SessionWrites), a call that already ended refuses it and its
 *   outcome comes through.</li>
 *   <li>A cancel from the thread making the second call (a dependent stage run synchronously) cannot be granted then:
 *   it returns false and is passed to the call once made, as {@code DeferredCall} does.</li>
 * </ul>
 */
final class ChainedCall<T> extends CompletableFuture<T> {

	private final Object lock = new Object();
	private final CompletableFuture<?> first;
	/** Guarded by {@link #lock}: the second call, once made. */
	private CompletableFuture<T> second;
	/** Guarded by {@link #lock}: cancelled before the second call was made, which then never is. */
	private boolean cancelledBeforeSecond;
	/** Guarded by {@link #lock}: the second call is being made (only its own thread can see this). */
	private boolean making;
	/** Guarded by {@link #lock}: a cancel came while the second call was made; passed to it once made. */
	private boolean cancelWhileMaking;
	/** Guarded by {@link #lock}: that cancel's argument. */
	private boolean interruptWhileMaking;

	private ChainedCall(CompletableFuture<?> first) {
		this.first = first;
	}

	/** {@code first}, then the call that {@code then} makes from its result. */
	static <A, T> CompletableFuture<T> of(CompletableFuture<A> first,
			Function<? super A, ? extends CompletableFuture<T>> then) {
		var result = new ChainedCall<T>(first);
		first.whenComplete((value, error) -> result.firstEnded(value, error, then));
		return result;
	}

	private <A> void firstEnded(A value, Throwable error, Function<? super A, ? extends CompletableFuture<T>> then) {
		CompletableFuture<T> made;
		boolean cancelMade;
		boolean interrupt;
		synchronized (lock) {
			if (cancelledBeforeSecond) return;
			if (error != null) {
				made = null;
			} else {
				// Made under the lock, so that a cancel from another thread either comes first (and nothing more is sent) or
				// reaches the call.
				making = true;
				try {
					made = Objects.requireNonNull(then.apply(value), "call");
				} catch (Throwable failure) {
					made = CompletableFuture.failedFuture(failure);
				} finally {
					making = false;
				}
				second = made;
			}
			cancelMade = cancelWhileMaking;
			interrupt = interruptWhileMaking;
		}
		if (made == null) {
			if (first.isCancelled() && unwrap(error) instanceof CancellationException) {
				cancelOnly();
			} else {
				completeExceptionally(unwrap(error));
			}
			return;
		}
		var source = made;
		made.whenComplete((result, failure) -> {
			if (failure == null) {
				complete(result);
			} else if (source.isCancelled() && unwrap(failure) instanceof CancellationException) {
				cancelOnly();
			} else {
				completeExceptionally(unwrap(failure));
			}
		});
		if (cancelMade) made.cancel(interrupt);
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		if (isDone()) return isCancelled();
		CompletableFuture<T> made;
		synchronized (lock) {
			made = second;
			if (made == null && making) {
				cancelWhileMaking = true;
				interruptWhileMaking |= mayInterruptIfRunning;
				return false;
			}
			if (made == null) cancelledBeforeSecond = true;
		}
		if (made == null) {
			// Nothing but the first call was sent, which changes nothing: it is stopped, and the second is never made.
			first.cancel(mayInterruptIfRunning);
		} else if (!made.cancel(mayInterruptIfRunning) && !made.isCancelled()) {
			// The second call refused the cancel (it ended, or a copy of a numbered write was sent): its outcome completes
			// this future.
			return false;
		}
		super.cancel(mayInterruptIfRunning);
		return isCancelled();
	}

	private void cancelOnly() {
		super.cancel(false);
	}

	private static Throwable unwrap(Throwable error) {
		return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
	}
}
