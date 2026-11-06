package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.UpdateContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** Which writes the core client's {@code SessionWrites} numbers, and how its calls end when the connection closes. */
class SessionWritesTest {

	private static final Keys KEY = new Keys(Buf.wrap(new byte[] {1}));

	/** Reflective access to the package-private {@code SessionWrites}. */
	private static final class Writes {
		private static final Class<?> TYPE = type("it.cavallium.rockserver.core.client.SessionWrites");
		private static final Class<?> BOUND = type("it.cavallium.rockserver.core.client.BoundRequestContext");
		private final Object target;

		Writes(Function<RequestContext, CompletableFuture<Long>> opener,
				BiFunction<Object, Object, CompletableFuture<?>> sender, Executor executor) throws Exception {
			var constructor = TYPE.getDeclaredConstructor(Function.class, BiFunction.class, Executor.class);
			constructor.setAccessible(true);
			target = constructor.newInstance(opener, sender, executor);
		}

		static boolean observes(RocksDBAPICommand<?, ?, ?> command) {
			return (Boolean) call(null, "observes", new Class<?>[] {RocksDBAPICommand.class}, command);
		}

		void observe(RocksDBAPICommand<?, ?, ?> command, Object result) {
			call(target, "observe", new Class<?>[] {RocksDBAPICommand.class, Object.class}, command, result);
		}

		boolean numbers(RocksDBAPICommand<?, ?, ?> command) {
			return (Boolean) call(target, "numbers", new Class<?>[] {RocksDBAPICommand.class}, command);
		}

		@SuppressWarnings("unchecked")
		CompletableFuture<Object> send(RequestContext context, RocksDBAPICommand<?, ?, ?> command) {
			Object bound = callOn(BOUND, null, "bind", new Class<?>[] {RequestContext.class}, context);
			return (CompletableFuture<Object>) call(target, "send",
					new Class<?>[] {BOUND, RocksDBAPICommand.RocksDBAPICommandSingle.class}, bound, command);
		}

		void close() {
			call(target, "close", new Class<?>[0]);
		}

		private static Object call(Object target, String name, Class<?>[] types, Object... args) {
			return callOn(TYPE, target, name, types, args);
		}

		private static Object callOn(Class<?> type, Object target, String name, Class<?>[] types, Object... args) {
			try {
				Method method = type.getDeclaredMethod(name, types);
				method.setAccessible(true);
				return method.invoke(target, args);
			} catch (InvocationTargetException ex) {
				throw new RuntimeException(ex.getCause());
			} catch (ReflectiveOperationException ex) {
				throw new IllegalStateException(ex);
			}
		}

		private static Class<?> type(String name) {
			try {
				return Class.forName(name);
			} catch (ClassNotFoundException ex) {
				throw new IllegalStateException(ex);
			}
		}
	}

	private static Put<Void> put(long id) {
		return new Put<>(id, 1, KEY, Buf.wrap(new byte[] {2}), RequestType.none());
	}

	/** A getForUpdate inside a transaction answers with the transaction's id: its later writes are staged. */
	@Test
	void onlyAGetForUpdateThatOpensAnUpdateMakesItsIdNumbered() throws Exception {
		var writes = new Writes(_ -> new CompletableFuture<>(), (_, _) -> new CompletableFuture<>(), Runnable::run);
		long transaction = -7;
		var inTransaction = new Get<>(transaction, 1, KEY, RequestType.forUpdate());
		assertFalse(Writes.observes(inTransaction));
		writes.observe(inTransaction, new UpdateContext<>(null, transaction));
		assertFalse(writes.numbers(put(transaction)), "a write staged in a transaction must not be numbered");

		long update = -9;
		var opening = new Get<>(0, 1, KEY, RequestType.forUpdate());
		assertTrue(Writes.observes(opening));
		writes.observe(opening, new UpdateContext<>(null, update));
		assertTrue(writes.numbers(put(update)), "the write that ends an update is numbered");
		assertTrue(writes.numbers(put(0)));
	}

	/**
	 * A write whose copies keep failing waits between resends: closing the connection, which also shuts its executor,
	 * ends it as unknown (a copy was sent) instead of leaving it waiting forever.
	 */
	@Test
	void closingEndsTheWritesWaitingForAResend() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var attempts = new AtomicInteger();
		var writes = new Writes(_ -> CompletableFuture.completedFuture(5L), (_, _) -> {
			attempts.incrementAndGet();
			return CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException());
		}, executor);
		var context = RequestContext.latency(Duration.ofSeconds(60));
		var result = writes.send(context, put(0));
		while (attempts.get() < 2) {
			Thread.sleep(10);
		}
		executor.shutdownNow();
		writes.close();
		var error = assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
		var cause = assertInstanceOf(RocksDBException.class, error.getCause());
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, cause.getErrorUniqueId(), cause.getMessage());
		// A new write is refused as not sent.
		var later = writes.send(context, put(0));
		var refused = assertThrows(ExecutionException.class, () -> later.get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED,
				((RocksDBException) refused.getCause()).getErrorUniqueId());
	}
}
