package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import java.lang.reflect.Constructor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class GrpcResponseFutureMapperTest {

	@Test
	void throwingSuccessMapperCompletesPromptlyWithTheMapperFailure() throws Exception {
		var source = new TestListenableFuture<String>();
		var mapperFailure = new IllegalStateException("broken success mapper");
		var bridge = newBridge(source, ignored -> {
			throw mapperFailure;
		}, Function.identity());

		assertDoesNotThrow(() -> bridge.callback().onSuccess("response"));

		var completionFailure = assertThrows(ExecutionException.class,
				() -> bridge.future().get(1, SECONDS));
		assertSame(mapperFailure, completionFailure.getCause());
	}

	@Test
	void throwingErrorMapperCompletesPromptlyAndPreservesTheTransportFailure() throws Exception {
		var source = new TestListenableFuture<String>();
		var transportFailure = new IllegalArgumentException("transport failure");
		var mapperFailure = new IllegalStateException("broken error mapper");
		var bridge = newBridge(source, Function.identity(), ignored -> {
			throw mapperFailure;
		});

		assertDoesNotThrow(() -> bridge.callback().onFailure(transportFailure));

		var completionFailure = assertThrows(ExecutionException.class,
				() -> bridge.future().get(1, SECONDS));
		assertSame(mapperFailure, completionFailure.getCause());
		assertEquals(1, mapperFailure.getSuppressed().length);
		assertSame(transportFailure, mapperFailure.getSuppressed()[0]);
	}

	@Test
	void nullSuccessResultIsAValidCompletedResponse() throws Exception {
		var bridge = newBridge(new TestListenableFuture<String>(), ignored -> null, Function.identity());

		bridge.callback().onSuccess("response");

		assertNull(bridge.future().get(1, SECONDS));
		assertTrue(bridge.future().isDone());
		assertFalse(bridge.future().isCompletedExceptionally());
	}

	@Test
	void nullMappedFailureCompletesPromptlyAndPreservesTheTransportFailure() throws Exception {
		var transportFailure = new IllegalArgumentException("transport failure");
		var bridge = newBridge(new TestListenableFuture<String>(), Function.identity(), ignored -> null);

		assertDoesNotThrow(() -> bridge.callback().onFailure(transportFailure));

		var completionFailure = assertThrows(ExecutionException.class,
				() -> bridge.future().get(1, SECONDS));
		assertTrue(completionFailure.getCause() instanceof NullPointerException);
		assertEquals(1, completionFailure.getCause().getSuppressed().length);
		assertSame(transportFailure, completionFailure.getCause().getSuppressed()[0]);
	}

	@Test
	void duplicateCallbacksInvokeExactlyOneMapper() throws Exception {
		var successCalls = new AtomicInteger();
		var failureCalls = new AtomicInteger();
		var bridge = newBridge(new TestListenableFuture<String>(), response -> {
			successCalls.incrementAndGet();
			return response.length();
		}, failure -> {
			failureCalls.incrementAndGet();
			return failure;
		});

		bridge.callback().onSuccess("first");
		bridge.callback().onSuccess("second");
		bridge.callback().onFailure(new IllegalStateException("late failure"));

		assertEquals(5, bridge.future().get(1, SECONDS));
		assertEquals(1, successCalls.get());
		assertEquals(0, failureCalls.get());
	}

	@Test
	void concurrentLosingCallbackReturnsWithoutInvokingItsMapper() throws Exception {
		var firstMapperEntered = new CountDownLatch(1);
		var releaseFirstMapper = new CountDownLatch(1);
		var successCalls = new AtomicInteger();
		var failureCalls = new AtomicInteger();
		var bridge = newBridge(new TestListenableFuture<String>(), response -> {
			successCalls.incrementAndGet();
			firstMapperEntered.countDown();
			await(releaseFirstMapper);
			return response.length();
		}, failure -> {
			failureCalls.incrementAndGet();
			return failure;
		});

		ExecutorService callbacks = Executors.newFixedThreadPool(2);
		try {
			Future<?> winner = callbacks.submit(() -> bridge.callback().onSuccess("winner"));
			assertTrue(firstMapperEntered.await(1, SECONDS), "winning mapper never started");

			Future<?> loser = callbacks.submit(() ->
					bridge.callback().onFailure(new IllegalStateException("loser")));
			loser.get(1, SECONDS);
			assertEquals(1, successCalls.get());
			assertEquals(0, failureCalls.get());

			releaseFirstMapper.countDown();
			winner.get(1, SECONDS);
			assertEquals(6, bridge.future().get(1, SECONDS));
		} finally {
			releaseFirstMapper.countDown();
			callbacks.shutdownNow();
		}
	}

	@Test
	void callbacksAfterAnExternalTerminalWinnerDoNotInvokeMappers() throws Exception {
		var mapperCalls = new AtomicInteger();
		var bridge = newBridge(new TestListenableFuture<String>(), response -> {
			mapperCalls.incrementAndGet();
			return response;
		}, failure -> {
			mapperCalls.incrementAndGet();
			return failure;
		});

		assertTrue(bridge.future().complete("external winner"));
		bridge.callback().onSuccess("late success");
		bridge.callback().onFailure(new IllegalStateException("late failure"));

		assertEquals("external winner", bridge.future().get(1, SECONDS));
		assertEquals(0, mapperCalls.get());
	}

	@Test
	void callbacksAfterCancellationDoNotInvokeMappers() {
		var mapperCalls = new AtomicInteger();
		var source = new TestListenableFuture<String>();
		var bridge = newBridge(source, response -> {
			mapperCalls.incrementAndGet();
			return response;
		}, failure -> {
			mapperCalls.incrementAndGet();
			return failure;
		});

		assertTrue(bridge.future().cancel(true));
		assertTrue(source.isCancelled());
		bridge.callback().onSuccess("late success");
		bridge.callback().onFailure(new IllegalStateException("late failure"));

		assertTrue(bridge.future().isCancelled());
		assertEquals(0, mapperCalls.get());
	}

	@Test
	void virtualMachineErrorCompletesTheFutureBeforeItIsRethrown() throws Exception {
		var fatal = new TestVirtualMachineError("fatal mapper failure");
		var bridge = newBridge(new TestListenableFuture<String>(), ignored -> {
			throw fatal;
		}, Function.identity());

		assertSame(fatal, assertThrows(TestVirtualMachineError.class,
				() -> bridge.callback().onSuccess("response")));

		var completionFailure = assertThrows(ExecutionException.class,
				() -> bridge.future().get(1, SECONDS));
		assertSame(fatal, completionFailure.getCause());
	}

	@Test
	void virtualMachineErrorFromErrorMapperPreservesTheTransportFailureBeforeRethrow() throws Exception {
		var transportFailure = new IllegalArgumentException("transport failure");
		var fatal = new TestVirtualMachineError("fatal error mapper failure");
		var bridge = newBridge(new TestListenableFuture<String>(), Function.identity(), ignored -> {
			throw fatal;
		});

		assertSame(fatal, assertThrows(TestVirtualMachineError.class,
				() -> bridge.callback().onFailure(transportFailure)));

		var completionFailure = assertThrows(ExecutionException.class,
				() -> bridge.future().get(1, SECONDS));
		assertSame(fatal, completionFailure.getCause());
		assertEquals(1, fatal.getSuppressed().length);
		assertSame(transportFailure, fatal.getSuppressed()[0]);
	}

	@SuppressWarnings("unchecked")
	private static <T, U> Bridge<T, U> newBridge(ListenableFuture<T> source,
			Function<T, U> mapper,
			Function<Throwable, Throwable> errorMapper) {
		try {
			Class<?> bridgeClass = Class.forName(
					"it.cavallium.rockserver.core.client.GrpcConnectionDelegate$GrpcResponseFuture");
			Constructor<?> constructor = bridgeClass.getDeclaredConstructor(
					ListenableFuture.class, Function.class, Function.class);
			constructor.setAccessible(true);
			Object bridge = constructor.newInstance(source, mapper, errorMapper);
			return new Bridge<>((CompletableFuture<U>) bridge, (FutureCallback<T>) bridge);
		} catch (ReflectiveOperationException e) {
			throw new AssertionError("Unable to construct the response bridge", e);
		}
	}

	private static void await(CountDownLatch latch) {
		try {
			assertTrue(latch.await(5, SECONDS), "test coordination timed out");
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError("test interrupted", e);
		}
	}

	private record Bridge<T, U>(CompletableFuture<U> future, FutureCallback<T> callback) {}

	private static final class TestListenableFuture<T> implements ListenableFuture<T> {

		private final FutureTask<T> delegate = new FutureTask<>(() -> {
			throw new AssertionError("test future must be completed through its callback");
		});

		@Override
		public void addListener(Runnable listener, Executor executor) {
			// Callback delivery is controlled directly by each test.
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return delegate.cancel(mayInterruptIfRunning);
		}

		@Override
		public boolean isCancelled() {
			return delegate.isCancelled();
		}

		@Override
		public boolean isDone() {
			return delegate.isDone();
		}

		@Override
		public T get() throws InterruptedException, ExecutionException {
			return delegate.get();
		}

		@Override
		public T get(long timeout, java.util.concurrent.TimeUnit unit)
				throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
			return delegate.get(timeout, unit);
		}
	}

	private static final class TestVirtualMachineError extends VirtualMachineError {

		private TestVirtualMachineError(String message) {
			super(message);
		}
	}
}
