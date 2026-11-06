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
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatchChunks;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

/**
 * Which writes the core client's {@code SessionWrites} numbers, how it resends them, re-issues them under a new session
 * and retries opening one, how it numbers batch chunks, and how its calls end when the connection closes.
 */
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
	/** The request id a copy was sent with. */
	private static ClientRequestId idOf(Object bound) {
		try {
			var value = bound.getClass().getDeclaredMethod("value");
			value.setAccessible(true);
			return ((RequestContext) value.invoke(bound)).requestId();
		} catch (ReflectiveOperationException ex) {
			throw new IllegalStateException(ex);
		}
	}

	private static RocksDBException rocks(Throwable error) {
		Throwable cause = error;
		while (cause.getCause() != null && !(cause instanceof RocksDBException)) {
			cause = cause.getCause();
		}
		return assertInstanceOf(RocksDBException.class, cause);
	}

	private static RocksDBException expired() {
		return RocksDBException.of(RocksDBErrorType.SESSION_EXPIRED,
				"The client session is unknown or expired; this copy of the write was not applied");
	}

	/** Schema changes and batch writes are numbered: their copies must not take effect twice. */
	@Test
	void schemaChangesAndBatchesAreNumbered() throws Exception {
		var writes = new Writes(_ -> new CompletableFuture<>(), (_, _) -> new CompletableFuture<>(), Runnable::run);
		var schema = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.CreateColumn("c", schema)));
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.DeleteColumn(3)));
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.DeleteColumnIfExists("c")));
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.UploadMergeOperator("m", "a.B", new byte[] {1})));
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.PutBatch(1, Flux.empty(), PutBatchMode.WRITE_BATCH)));
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.DeleteRange(1, KEY, KEY)));
		assertFalse(writes.numbers(new RocksDBAPICommandSingle.CloseTransaction(5, false)));
	}

	/**
	 * The write that ends an update is numbered while the update is open, and forgotten once that write ended (or the
	 * update was closed as failed): the id may be reused later by a transaction, whose staged writes are not numbered.
	 */
	@Test
	void anUpdateIdIsNumberedUntilTheUpdateEnds() throws Exception {
		var writes = new Writes(_ -> CompletableFuture.completedFuture(5L),
				(_, _) -> CompletableFuture.completedFuture(null), Runnable::run);
		var opening = new Get<>(0, 1, KEY, RequestType.forUpdate());
		writes.observe(opening, new UpdateContext<>(null, -11L));
		assertTrue(writes.numbers(put(-11)));
		writes.send(RequestContext.latency(Duration.ofSeconds(5)), put(-11)).get(5, TimeUnit.SECONDS);
		assertFalse(writes.numbers(put(-11)), "forgotten once the write that ended the update ended");

		writes.observe(opening, new UpdateContext<>(null, -12L));
		assertTrue(writes.numbers(put(-12)));
		writes.observe(new RocksDBAPICommandSingle.CloseFailedUpdate(-12), null);
		assertFalse(writes.numbers(put(-12)), "forgotten once the update was closed as failed");
	}

	/**
	 * A first copy that meets an expired session was not applied: the write is numbered again in a new session and
	 * sent; a later write uses the new session too.
	 */
	@Test
	void aFirstCopyRefusedByAnExpiredSessionIsReissuedUnderANewSession() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var sessions = new AtomicLong(5);
		var ids = Collections.synchronizedList(new ArrayList<ClientRequestId>());
		var writes = new Writes(_ -> CompletableFuture.completedFuture(sessions.getAndIncrement()), (bound, _) -> {
			var id = idOf(bound);
			ids.add(id);
			return id.sessionId() == 5 ? CompletableFuture.failedFuture(expired())
					: CompletableFuture.completedFuture("ok");
		}, executor);
		var context = RequestContext.latency(Duration.ofSeconds(10));
		assertEquals("ok", writes.send(context, put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(List.of(new ClientRequestId(5, 0, 0), new ClientRequestId(6, 0, 0)), ids);
		assertEquals("ok", writes.send(context, put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(new ClientRequestId(6, 1, 1), ids.getLast());
		assertEquals(7, sessions.get(), "two sessions opened");
		executor.shutdownNow();
	}

	/**
	 * A copy sent again after an unknown outcome that meets an expired session ends "unknown": the earlier copy may have
	 * been applied. The next write opens a new session.
	 */
	@Test
	void anExpiredSessionAfterAnUnknownOutcomeEndsUnknown() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var sessions = new AtomicLong(5);
		var attempts = new AtomicInteger();
		var ids = Collections.synchronizedList(new ArrayList<ClientRequestId>());
		var writes = new Writes(_ -> CompletableFuture.completedFuture(sessions.getAndIncrement()), (bound, _) -> {
			ids.add(idOf(bound));
			return switch (attempts.getAndIncrement()) {
				case 0 -> CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException());
				case 1 -> CompletableFuture.failedFuture(expired());
				default -> CompletableFuture.completedFuture("ok");
			};
		}, executor);
		var context = RequestContext.latency(Duration.ofSeconds(10));
		var error = assertThrows(ExecutionException.class, () -> writes.send(context, put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId());
		assertEquals(List.of(new ClientRequestId(5, 0, 0), new ClientRequestId(5, 0, 0)), ids,
				"the same number twice, never a new one");
		assertEquals("ok", writes.send(context, put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(6, ids.getLast().sessionId(), "the expired session is not used again");
		executor.shutdownNow();
	}

	/** Sessions that expire at once do not make a tight loop: the write ends, not applied, at its deadline. */
	@Test
	void sessionsThatKeepExpiringEndTheWriteAtItsDeadline() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var sessions = new AtomicLong(1);
		var attempts = new AtomicInteger();
		var writes = new Writes(_ -> CompletableFuture.completedFuture(sessions.getAndIncrement()), (_, _) -> {
			attempts.incrementAndGet();
			return CompletableFuture.failedFuture(expired());
		}, executor);
		var error = assertThrows(ExecutionException.class,
				() -> writes.send(RequestContext.latency(Duration.ofMillis(600)), put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.SESSION_EXPIRED, rocks(error).getErrorUniqueId(), "not applied");
		assertTrue(attempts.get() >= 2 && attempts.get() < 20, "attempts: " + attempts.get());
		executor.shutdownNow();
	}

	/** Opening a session is retried after an unknown outcome: opening twice only leaves an unused session. */
	@Test
	void openingASessionIsRetriedAfterAnUnknownOutcome() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var opens = new AtomicInteger();
		var ids = Collections.synchronizedList(new ArrayList<ClientRequestId>());
		var writes = new Writes(_ -> opens.incrementAndGet() < 3
				? CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException())
				: CompletableFuture.completedFuture(9L), (bound, _) -> {
			ids.add(idOf(bound));
			return CompletableFuture.completedFuture("ok");
		}, executor);
		assertEquals("ok", writes.send(RequestContext.latency(Duration.ofSeconds(10)), put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(3, opens.get());
		assertEquals(List.of(new ClientRequestId(9, 0, 0)), ids);

		// An opening that cannot succeed (not an unknown outcome) ends the write as not sent.
		var refusing = new Writes(_ -> CompletableFuture.failedFuture(
				RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED, "no sessions")),
				(_, _) -> CompletableFuture.completedFuture("ok"), executor);
		var error = assertThrows(ExecutionException.class,
				() -> refusing.send(RequestContext.latency(Duration.ofSeconds(10)), put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.NOT_IMPLEMENTED, rocks(error).getErrorUniqueId());
		executor.shutdownNow();
	}

	/**
	 * A write waiting for an opening that another write started ends with that write's deadline: it opens again in its
	 * own time instead of failing with the other's.
	 */
	@Test
	void aWriteWaitingForASharedOpeningOpensAgainInItsOwnTime() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		long unavailableUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(700);
		var writes = new Writes(_ -> System.nanoTime() < unavailableUntil
				? CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException())
				: CompletableFuture.completedFuture(4L), (_, _) -> CompletableFuture.completedFuture("ok"), executor);
		var shortWrite = writes.send(RequestContext.latency(Duration.ofMillis(300)), put(0));
		var longWrite = writes.send(RequestContext.latency(Duration.ofSeconds(10)), put(0));
		assertEquals("ok", longWrite.get(5, TimeUnit.SECONDS));
		// The short write ended too (its copy, sent late, would be refused by the server as expired before dispatch).
		try {
			shortWrite.get(5, TimeUnit.SECONDS);
		} catch (ExecutionException expected) {
			// Ended at its own deadline.
		}
		executor.shutdownNow();
	}

	private static KVBatch batch(int items, int valueBytes) {
		var keys = new ArrayList<Keys>();
		var values = new ArrayList<Buf>();
		for (int i = 0; i < items; i++) {
			keys.add(new Keys(Buf.wrap(new byte[] {(byte) i})));
			values.add(Buf.wrap(new byte[valueBytes]));
		}
		return new KVBatch.KVBatchRef(keys, values);
	}

	/**
	 * A batch write is sent as numbered chunks, in order, one per replicated entry; a chunk whose outcome is unknown is
	 * sent again with its number. A chunk split again on the server is that one chunk.
	 */
	@Test
	void batchesAreSentAsNumberedChunksResentAfterAnUnknownOutcome() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var sent = Collections.synchronizedList(new ArrayList<String>());
		var attempts = new AtomicInteger();
		var writes = new Writes(_ -> CompletableFuture.completedFuture(3L), (bound, command) -> {
			var chunks = Flux.from(((RocksDBAPICommandSingle.PutBatch) command).batchPublisher()).collectList().block();
			assertEquals(1, chunks.size());
			assertEquals(1, KVBatchChunks.split(chunks.getFirst()).size(), "one chunk on the server too");
			sent.add(idOf(bound).sequence() + ":" + chunks.getFirst().keys().size());
			return attempts.getAndIncrement() == 0
					? CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException())
					: CompletableFuture.completedFuture(null);
		}, executor);
		int big = (int) (KVBatchChunks.CHUNK_BYTES / 3);
		// 5 items of a third of a chunk: chunks of 2, 2 and 1 items; then an empty batch, then a small one.
		var command = new RocksDBAPICommandSingle.PutBatch(1,
				Flux.just(batch(5, big), batch(0, 0), batch(3, 10)), PutBatchMode.WRITE_BATCH);
		writes.send(RequestContext.batch(Duration.ofSeconds(20)), command).get(10, TimeUnit.SECONDS);
		assertEquals(List.of("0:2", "0:2", "1:2", "2:1", "3:3"), sent);
		executor.shutdownNow();
	}

	/**
	 * A chunk that fails for good after an earlier one took effect leaves the batch partly applied: INVALID_REQUEST,
	 * which callers do not resend. If the first chunk fails, the batch fails with its error.
	 */
	@Test
	void aBatchThatFailsAfterAChunkTookEffectIsPartlyApplied() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var attempts = new AtomicInteger();
		var writes = new Writes(_ -> CompletableFuture.completedFuture(3L), (_, _) -> attempts.getAndIncrement() == 0
				? CompletableFuture.completedFuture(null)
				: CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "busy")),
				executor);
		int big = (int) (KVBatchChunks.CHUNK_BYTES / 2);
		var partly = assertThrows(ExecutionException.class, () -> writes.send(RequestContext.batch(Duration.ofSeconds(10)),
				new RocksDBAPICommandSingle.PutBatch(1, Flux.just(batch(3, big)), PutBatchMode.WRITE_BATCH))
				.get(10, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.INVALID_REQUEST, rocks(partly).getErrorUniqueId(), rocks(partly).getMessage());
		assertTrue(rocks(partly).getMessage().contains("partly applied"), rocks(partly).getMessage());

		var first = assertThrows(ExecutionException.class, () -> writes.send(RequestContext.batch(Duration.ofSeconds(10)),
				new RocksDBAPICommandSingle.PutBatch(1, Flux.just(batch(1, 1)), PutBatchMode.WRITE_BATCH))
				.get(10, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, rocks(first).getErrorUniqueId());
		executor.shutdownNow();
	}
}
