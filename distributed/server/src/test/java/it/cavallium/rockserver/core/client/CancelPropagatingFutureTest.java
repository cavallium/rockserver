package it.cavallium.rockserver.core.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

/**
 * R46-4: a future that the client derives from a call's future ({@code thenApply}, {@code whenComplete},
 * {@code exceptionallyCompose}) passes a cancel on to it, as the call's own future does. In the core client's package
 * on the class path, as {@link LeaderHintRouterTest}.
 */
class CancelPropagatingFutureTest {

	@Test
	void cancellingTheDerivedFutureCancelsTheSource() {
		var source = new CompletableFuture<Integer>();
		var derived = CancelPropagatingFuture.of(source, source.thenApply(value -> value + 1));
		assertTrue(derived.cancel(true));
		assertTrue(source.isCancelled(), "the call is cancelled");
		assertTrue(derived.isCancelled());
	}

	@Test
	void aCancelAfterTheSourceEndedLetsItsOutcomeThrough() throws Exception {
		var source = new CompletableFuture<Integer>();
		var gate = new CompletableFuture<Void>();
		// The source answered, the derived step has not run yet (as on the callback executor).
		var derived = CancelPropagatingFuture.of(source, source.thenCombine(gate, (value, _) -> value + 1));
		source.complete(1);
		assertFalse(derived.cancel(true), "too late: the answer came");
		gate.complete(null);
		assertEquals(2, derived.get());
	}

	@Test
	void resultsAndFailuresPassAsWithThenApply() {
		var source = new CompletableFuture<Integer>();
		var derived = CancelPropagatingFuture.of(source, source.thenApply(value -> value + 1));
		source.complete(41);
		assertEquals(42, derived.join());

		var failing = new CompletableFuture<Integer>();
		var failure = new IllegalStateException("boom");
		var derivedFailure = CancelPropagatingFuture.of(failing, failing.thenApply(value -> value + 1));
		failing.completeExceptionally(failure);
		var thrown = assertThrows(ExecutionException.class, derivedFailure::get);
		assertSame(failure, thrown.getCause());
		assertFalse(derivedFailure.isCancelled());
	}

	@Test
	void aSourceCancelledElsewhereCancelsTheDerivedFuture() {
		var source = new CompletableFuture<Integer>();
		var derived = CancelPropagatingFuture.of(source, source.whenComplete((_, _) -> {}));
		source.cancel(false);
		assertTrue(derived.isCancelled());
	}
}
