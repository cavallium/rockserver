package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * R6-6a: the collect budget of numbered multi writes ({@code GrpcServerImpl.CollectHold}, through reflection). A cancel ends the collection
 * while a late item may still arrive on another thread: whatever the order, every byte taken is given back. The
 * Reactor wiring ({@code GrpcServerImpl.collectNumbered}, which {@code collectMulti} uses outside LATENCY) is tested too:
 * a cancel gives the bytes back, and an item that arrives after it takes none.
 */
class CollectHoldTest {

	/** {@code GrpcServerImpl.CollectHold}, package-private in another package. */
	private static final class Hold {

		private static final Constructor<?> CONSTRUCTOR;
		private static final Method TAKE;
		private static final Method END;

		static {
			try {
				var type = Class.forName("it.cavallium.rockserver.core.server.GrpcServerImpl$CollectHold");
				CONSTRUCTOR = type.getDeclaredConstructor(AtomicLong.class, long.class);
				CONSTRUCTOR.setAccessible(true);
				TAKE = type.getDeclaredMethod("take", long.class);
				TAKE.setAccessible(true);
				END = type.getDeclaredMethod("end");
				END.setAccessible(true);
			} catch (ReflectiveOperationException ex) {
				throw new ExceptionInInitializerError(ex);
			}
		}

		private final Object target;

		Hold(AtomicLong budget, long limit) {
			try {
				target = CONSTRUCTOR.newInstance(budget, limit);
			} catch (ReflectiveOperationException ex) {
				throw new IllegalStateException(ex);
			}
		}

		boolean take(long bytes) {
			return (Boolean) call(TAKE, bytes);
		}

		void end() {
			call(END);
		}

		private Object call(Method method, Object... args) {
			try {
				return method.invoke(target, args);
			} catch (InvocationTargetException ex) {
				throw new IllegalStateException(ex.getCause());
			} catch (IllegalAccessException ex) {
				throw new IllegalStateException(ex);
			}
		}
	}

	@Test
	void theBytesOfACollectionGoBackWhenItEnds() {
		var budget = new AtomicLong();
		var hold = new Hold(budget, 100);
		assertTrue(hold.take(60));
		assertTrue(hold.take(40));
		assertFalse(hold.take(1), "the budget is full");
		assertEquals(100, budget.get());
		var other = new Hold(budget, 100);
		assertFalse(other.take(1), "another collection finds the budget full");
		hold.end();
		assertEquals(0, budget.get());
		assertTrue(other.take(100));
		other.end();
		assertEquals(0, budget.get());
	}

	/** The late item after the cancel's end takes nothing: it would never be given back. */
	@Test
	void anItemAfterTheEndTakesNothing() {
		var budget = new AtomicLong();
		var hold = new Hold(budget, 100);
		assertTrue(hold.take(10));
		hold.end();
		assertFalse(hold.take(10));
		assertEquals(0, budget.get());
		hold.end();
		assertEquals(0, budget.get(), "a second end gives nothing back twice");
	}

	/** Items on several threads race the end of their collection, many times: the budget always ends at zero. */
	@Test
	void itemsRacingTheEndLeaveNothingTaken() throws Exception {
		var budget = new AtomicLong();
		for (int round = 0; round < 2_000; round++) {
			var hold = new Hold(budget, Long.MAX_VALUE / 2);
			var start = new CountDownLatch(1);
			var threads = new ArrayList<Thread>();
			for (int t = 0; t < 3; t++) {
				threads.add(Thread.ofPlatform().start(() -> {
					await(start);
					for (int i = 0; i < 50; i++) {
						hold.take(1 + ThreadLocalRandom.current().nextInt(100));
					}
				}));
			}
			threads.add(Thread.ofPlatform().start(() -> {
				await(start);
				if (ThreadLocalRandom.current().nextBoolean()) {
					Thread.onSpinWait();
				}
				hold.end();
			}));
			start.countDown();
			for (var thread : threads) {
				thread.join();
			}
			assertEquals(0, budget.get(), "round " + round);
		}
	}

	/** {@code GrpcServerImpl.collectNumbered}: the collection of a numbered streamed multi write. */
	@SuppressWarnings("unchecked")
	private static Mono<List<Long>> collectNumbered(Flux<Long> items, AtomicLong budget, long limit) {
		try {
			var type = Class.forName("it.cavallium.rockserver.core.server.GrpcServerImpl");
			Method method = type.getDeclaredMethod("collectNumbered", Flux.class, AtomicLong.class, long.class,
					Duration.class, ToLongFunction.class, String.class);
			method.setAccessible(true);
			ToLongFunction<Long> bytes = _ -> 100;
			return (Mono<List<Long>>) method.invoke(null, items, budget, limit, Duration.ofSeconds(30), bytes, "putMulti");
		} catch (InvocationTargetException ex) {
			throw new IllegalStateException(ex.getCause());
		} catch (ReflectiveOperationException ex) {
			throw new IllegalStateException(ex);
		}
	}

	/** A collection that completes, or fails, gives its bytes back. */
	@Test
	void aCollectionThatEndsGivesItsBytesBack() {
		var budget = new AtomicLong();
		var values = collectNumbered(Flux.range(0, 10).map(Long::valueOf), budget, Long.MAX_VALUE / 2)
				.block(Duration.ofSeconds(10));
		assertEquals(10, values.size());
		assertEquals(0, budget.get());
		var failing = Flux.concat(Flux.range(0, 10).map(Long::valueOf), Flux.<Long>error(new IllegalStateException("x")));
		try {
			collectNumbered(failing, budget, Long.MAX_VALUE / 2).block(Duration.ofSeconds(10));
		} catch (IllegalStateException expected) {
			// the stream's own failure
		}
		assertEquals(0, budget.get());
	}

	/**
	 * A stream that keeps sending items on its own thread while its call is cancelled, as a gRPC inbound stream can:
	 * the cancel gives back what the collection took, and the late items that reach the collection after it take
	 * nothing, many times over.
	 */
	@Test
	void aCancelWhileItemsArriveLeavesNothingTaken() throws Exception {
		var budget = new AtomicLong();
		for (int round = 0; round < 500; round++) {
			var cancelled = new AtomicBoolean();
			var emitted = new CountDownLatch(1);
			var started = new CountDownLatch(1);
			// Ignores demand and keeps sending after the cancel: a few more items arrive late.
			Publisher<Long> stream = (Subscriber<? super Long> subscriber) -> {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
					}

					@Override
					public void cancel() {
						cancelled.set(true);
					}
				});
				Thread.ofPlatform().start(() -> {
					try {
						long i = 0;
						int late = 0;
						while (late < 5) {
							subscriber.onNext(i++);
							if (i == 3) {
								started.countDown();
							}
							if (cancelled.get()) {
								late++;
							}
						}
					} finally {
						started.countDown();
						emitted.countDown();
					}
				});
			};
			var subscription = collectNumbered(Flux.from(stream), budget, Long.MAX_VALUE / 2).subscribe(_ -> {
			}, _ -> {
			});
			assertTrue(started.await(10, TimeUnit.SECONDS));
			if (ThreadLocalRandom.current().nextBoolean()) {
				Thread.onSpinWait();
			}
			subscription.dispose();
			assertTrue(emitted.await(10, TimeUnit.SECONDS));
			assertEquals(0, budget.get(), "round " + round);
		}
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
