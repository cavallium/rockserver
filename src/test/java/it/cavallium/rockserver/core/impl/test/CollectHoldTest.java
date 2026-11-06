package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * R6-6a: the collect budget of numbered multi writes ({@code GrpcServerImpl.CollectHold}, through reflection). A cancel ends the collection
 * while a late item may still arrive on another thread: whatever the order, every byte taken is given back.
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

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}
