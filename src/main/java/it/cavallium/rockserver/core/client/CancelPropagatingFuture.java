package it.cavallium.rockserver.core.client;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;

/**
 * A future derived from another one (by {@code thenApply}, {@code whenComplete}, {@code exceptionallyCompose}, ...)
 * whose {@link #cancel} also cancels the future it is derived from. A future that {@link CompletableFuture} derives
 * does not: cancelling it completes it, and the source, the gRPC call, runs on. A caller that cancels a
 * {@code getForUpdate} must reach the server, which then releases the key at once, instead of when the update's
 * lease ends (R46-4, design.md "Single-key updates queue").
 *
 * <p>A cancel that comes after the source ended does nothing and returns false, as for the source
 * ({@code GrpcResponseFuture}): its outcome, a result or a failure, reaches the caller.</p>
 */
final class CancelPropagatingFuture<T> extends CompletableFuture<T> {

	private final Future<?> source;

	private CancelPropagatingFuture(Future<?> source) {
		this.source = source;
	}

	/** {@code derived}, as a future whose {@link #cancel} also cancels {@code source}, the future it is derived from. */
	static <T> CompletableFuture<T> of(Future<?> source, CompletableFuture<T> derived) {
		var result = new CancelPropagatingFuture<T>(source);
		derived.whenComplete((value, error) -> {
			if (error == null) {
				result.complete(value);
			} else if (source.isCancelled() && unwrap(error) instanceof CancellationException) {
				result.cancelOnly();
			} else {
				result.completeExceptionally(error);
			}
		});
		return result;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		if (isDone()) {
			return isCancelled();
		}
		if (!source.cancel(mayInterruptIfRunning) && !source.isCancelled()) {
			// The source ended first: its outcome completes this future.
			return false;
		}
		// The source's cancel usually completed this future already, through the derived one.
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
