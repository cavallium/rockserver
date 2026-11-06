package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/** The idle timeout of forwarded streams: a stuck source fails, a slow consumer does not. */
class ForwardIdleTimeoutTest {

	private static final Duration IDLE = Duration.ofMillis(200);

	/** Requests one element at a time, {@code pauseMillis} after the previous one. */
	private static final class SlowSubscriber extends BaseSubscriber<Integer> {

		final CompletableFuture<Throwable> end = new CompletableFuture<>();
		final AtomicInteger received = new AtomicInteger();
		private final long pauseMillis;

		SlowSubscriber(long pauseMillis) {
			this.pauseMillis = pauseMillis;
		}

		@Override
		protected void hookOnSubscribe(Subscription subscription) {
			request(1);
		}

		@Override
		protected void hookOnNext(Integer value) {
			received.incrementAndGet();
			CompletableFuture.delayedExecutor(pauseMillis, TimeUnit.MILLISECONDS).execute(() -> request(1));
		}

		@Override
		protected void hookOnComplete() {
			end.complete(null);
		}

		@Override
		protected void hookOnError(Throwable error) {
			end.complete(error);
		}
	}

	@Test
	void sourceThatStopsDeliveringFails() throws Exception {
		var subscriber = new SlowSubscriber(0);
		ReplicatedConnection.idleTimeout(Flux.concat(Flux.range(0, 3), Flux.never()), IDLE).subscribe(subscriber);
		var error = subscriber.end.get(10, TimeUnit.SECONDS);
		assertInstanceOf(TimeoutException.class, error);
		assertEquals(3, subscriber.received.get());
	}

	@Test
	void slowConsumerIsNotTimedOut() throws Exception {
		// The consumer waits longer than the idle timeout before asking for each next element.
		var subscriber = new SlowSubscriber(IDLE.toMillis() * 2);
		ReplicatedConnection.idleTimeout(Flux.range(0, 4), IDLE).subscribe(subscriber);
		assertNull(subscriber.end.get(10, TimeUnit.SECONDS));
		assertEquals(4, subscriber.received.get());
	}
}
