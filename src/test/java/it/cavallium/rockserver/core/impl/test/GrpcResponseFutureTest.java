package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class GrpcResponseFutureTest {

	private static final int RACE_ITERATIONS = 1_000;

	@Test
	void completedDelegateCannotBeCancelledWhileSuccessCallbackIsQueued() throws Exception {
		var callbacks = new QueuedExecutor();
		var delegate = ListenableFutureTask.create(() -> "result");
		var mapperCalls = new AtomicInteger();
		var response = bridge(delegate, callbacks, value -> {
			mapperCalls.incrementAndGet();
			return value.length();
		}, Function.identity());

		delegate.run();
		assertEquals(1, callbacks.size());
		assertFalse(response.cancel(false), "completed delegate cannot be cancelled");
		assertFalse(response.isDone(), "queued completion must not be replaced by cancellation");

		callbacks.runAll();

		assertEquals(6, response.get(1, SECONDS));
		assertEquals(1, mapperCalls.get());
		assertFalse(response.isCancelled());
	}

	@Test
	void completedDelegateCannotBeCancelledWhileFailureCallbackIsQueued() {
		var callbacks = new QueuedExecutor();
		var sourceFailure = new IllegalStateException("source");
		var mappedFailure = new IllegalArgumentException("mapped", sourceFailure);
		var delegate = ListenableFutureTask.<String>create(() -> {
			throw sourceFailure;
		});
		var errorMapperCalls = new AtomicInteger();
		var response = bridge(delegate, callbacks, Function.identity(), failure -> {
			errorMapperCalls.incrementAndGet();
			assertEquals(sourceFailure, failure);
			return mappedFailure;
		});

		delegate.run();
		assertEquals(1, callbacks.size());
		assertFalse(response.cancel(false), "failed delegate cannot be cancelled");
		assertFalse(response.isDone(), "queued failure must not be replaced by cancellation");

		callbacks.runAll();

		var thrown = assertThrows(ExecutionException.class, () -> response.get(1, SECONDS));
		assertEquals(mappedFailure, thrown.getCause());
		assertEquals(1, errorMapperCalls.get());
		assertFalse(response.isCancelled());
	}

	@Test
	void cancellationExceptionFromCallableRemainsAnExceptionalResult() {
		var callbacks = new QueuedExecutor();
		var sourceFailure = new CancellationException("application failure");
		var mappedFailure = new IllegalArgumentException("mapped", sourceFailure);
		var delegate = ListenableFutureTask.<String>create(() -> {
			throw sourceFailure;
		});
		var response = bridge(delegate, callbacks, Function.identity(), failure -> {
			assertEquals(sourceFailure, failure);
			return mappedFailure;
		});

		delegate.run();
		callbacks.runAll();

		assertFalse(delegate.isCancelled());
		assertFalse(response.isCancelled());
		var thrown = assertThrows(ExecutionException.class, () -> response.get(1, SECONDS));
		assertEquals(mappedFailure, thrown.getCause());
	}

	@Test
	void successfulCancellationCancelsBothFuturesWithoutRunningMappers() {
		var callbacks = new QueuedExecutor();
		var delegate = ListenableFutureTask.<String>create(() -> "unused");
		var mapperCalls = new AtomicInteger();
		var errorMapperCalls = new AtomicInteger();
		var response = bridge(delegate, callbacks, value -> {
			mapperCalls.incrementAndGet();
			return value;
		}, failure -> {
			errorMapperCalls.incrementAndGet();
			return failure;
		});

		assertTrue(response.cancel(false));
		assertTrue(delegate.isCancelled());
		assertTrue(response.isCancelled());
		assertThrows(CancellationException.class, response::join);

		callbacks.runAll();

		assertTrue(response.isCancelled());
		assertEquals(0, mapperCalls.get());
		assertEquals(0, errorMapperCalls.get());
	}

	@Test
	void successAndCancellationRaceHasOneConsistentWinner() throws Exception {
		try (var racers = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory())) {
			for (int iteration = 0; iteration < RACE_ITERATIONS; iteration++) {
				var callbacks = new QueuedExecutor();
				var start = new CountDownLatch(1);
				int expected = iteration;
				var delegate = ListenableFutureTask.create(() -> {
					start.await();
					return expected;
				});
				var mapperCalls = new AtomicInteger();
				var response = bridge(delegate, callbacks, value -> {
					mapperCalls.incrementAndGet();
					return value + 1;
				}, Function.identity());
				Future<?> completed = racers.submit(delegate);
				Future<Boolean> cancelled = racers.submit(() -> {
					start.await();
					return response.cancel(false);
				});
				Future<?> callback = racers.submit(callbacks::runWhenAvailable);

				start.countDown();
				completed.get(1, SECONDS);
				boolean cancellationWon = cancelled.get(1, SECONDS);
				callback.get(1, SECONDS);
				boolean completionWon = !delegate.isCancelled();

				assertTrue(completionWon ^ cancellationWon,
						"delegate must have exactly one terminal winner at iteration " + iteration);
				if (completionWon) {
					assertFalse(response.isCancelled());
					assertEquals(iteration + 1, response.get(1, SECONDS));
					assertEquals(1, mapperCalls.get());
				} else {
					assertTrue(response.isCancelled());
					assertThrows(CancellationException.class, response::join);
					assertEquals(0, mapperCalls.get());
				}
			}
		}
	}

	@Test
	void failureAndCancellationRaceHasOneConsistentWinner() throws Exception {
		try (var racers = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory())) {
			for (int iteration = 0; iteration < RACE_ITERATIONS; iteration++) {
				var callbacks = new QueuedExecutor();
				var start = new CountDownLatch(1);
				var errorMapperCalls = new AtomicInteger();
				var sourceFailure = new IllegalStateException("failure-" + iteration);
				var mappedFailure = new IllegalArgumentException("mapped-" + iteration, sourceFailure);
				var delegate = ListenableFutureTask.<String>create(() -> {
					start.await();
					throw sourceFailure;
				});
				var response = bridge(delegate, callbacks, Function.identity(), failure -> {
					errorMapperCalls.incrementAndGet();
					assertEquals(sourceFailure, failure);
					return mappedFailure;
				});
				Future<?> failed = racers.submit(delegate);
				Future<Boolean> cancelled = racers.submit(() -> {
					start.await();
					return response.cancel(false);
				});
				Future<?> callback = racers.submit(callbacks::runWhenAvailable);

				start.countDown();
				failed.get(1, SECONDS);
				boolean cancellationWon = cancelled.get(1, SECONDS);
				callback.get(1, SECONDS);
				boolean failureWon = !delegate.isCancelled();

				assertTrue(failureWon ^ cancellationWon,
						"delegate must have exactly one terminal winner at iteration " + iteration);
				if (failureWon) {
					assertFalse(response.isCancelled());
					var thrown = assertThrows(ExecutionException.class, () -> response.get(1, SECONDS));
					assertEquals(mappedFailure, thrown.getCause());
					assertEquals(1, errorMapperCalls.get());
				} else {
					assertTrue(response.isCancelled());
					assertThrows(CancellationException.class, response::join);
					assertEquals(0, errorMapperCalls.get());
				}
			}
		}
	}

	private static final Method BRIDGE_RESPONSE = findBridgeResponse();

	private static Method findBridgeResponse() {
		try {
			Class<?> delegate = Class.forName("it.cavallium.rockserver.core.client.GrpcConnectionDelegate");
			Method method = delegate.getDeclaredMethod("bridgeResponse",
					ListenableFuture.class,
					Function.class,
					Function.class,
					Executor.class);
			method.setAccessible(true);
			return method;
		} catch (ReflectiveOperationException failure) {
			throw new ExceptionInInitializerError(failure);
		}
	}

	@SuppressWarnings("unchecked")
	private static <T, U> CompletableFuture<U> bridge(ListenableFuture<T> delegate,
			Executor callbackExecutor,
			Function<T, U> mapper,
			Function<Throwable, Throwable> errorMapper) {
		try {
			return (CompletableFuture<U>) BRIDGE_RESPONSE.invoke(null,
					delegate,
					mapper,
					errorMapper,
					callbackExecutor);
		} catch (IllegalAccessException failure) {
			throw new AssertionError(failure);
		} catch (InvocationTargetException failure) {
			throw new AssertionError(failure.getCause());
		}
	}

	private static final class QueuedExecutor implements Executor {
		private final Queue<Runnable> tasks = new ArrayDeque<>();
		private final CountDownLatch taskSubmitted = new CountDownLatch(1);

		@Override
		public synchronized void execute(Runnable command) {
			tasks.add(command);
			taskSubmitted.countDown();
		}

		synchronized int size() {
			return tasks.size();
		}

		void runAll() {
			while (true) {
				final Runnable task;
				synchronized (this) {
					task = tasks.poll();
				}
				if (task == null) {
					return;
				}
				task.run();
			}
		}

		void runWhenAvailable() {
			try {
				assertTrue(taskSubmitted.await(1, SECONDS), "delegate did not submit its terminal callback");
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
				throw new AssertionError(interrupted);
			}
			runAll();
		}
	}
}
