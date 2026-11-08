package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
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
import java.time.temporal.ChronoUnit;
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
import reactor.core.publisher.Mono;

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
			this(opener, sender, executor, true);
		}

		Writes(Function<RequestContext, CompletableFuture<Long>> opener,
				BiFunction<Object, Object, CompletableFuture<?>> sender, Executor executor, boolean numberedBatches)
				throws Exception {
			var constructor = TYPE.getDeclaredConstructor(Function.class, BiFunction.class, Executor.class,
					boolean.class);
			constructor.setAccessible(true);
			target = constructor.newInstance(opener, sender, executor, numberedBatches);
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

		boolean endsUpdateUnnumbered(RocksDBAPICommand<?, ?, ?> command) {
			return (Boolean) call(target, "endsUpdateUnnumbered", new Class<?>[] {RocksDBAPICommand.class}, command);
		}

		void forgetUpdate(RocksDBAPICommand<?, ?, ?> command) {
			call(target, "forgetUpdate", new Class<?>[] {RocksDBAPICommand.class}, command);
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

		long forgottenUpdates() {
			return (Long) call(target, "forgottenUpdates", new Class<?>[0]);
		}

		/** {@code SessionWrites.await}: a synchronous caller's bounded wait. */
		static Object await(CompletableFuture<Object> answer, long waitNanos) {
			return call(null, "await", new Class<?>[] {CompletableFuture.class, long.class}, answer, waitNanos);
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
				if (ex.getCause() instanceof RuntimeException runtime) {
					throw runtime;
				}
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
		assertTrue(((RocksDBException) refused.getCause()).isNotApplied(), "not sent: flagged not applied");
	}

	/**
	 * R26-3 review: a write that never got a session and ends because the connection closed was not sent, so it ends
	 * flagged not applied (as the Rust client's {@code not_sent}), both while it waits between openings and when the
	 * connection is already closed. Before, both ended {@code SERVER_OVERLOADED} without the flag.
	 */
	@Test
	void aWriteEndedByCloseBeforeItWasSentIsNotApplied() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var opens = new AtomicInteger();
		var sent = new AtomicInteger();
		var writes = new Writes(_ -> {
			opens.incrementAndGet();
			return CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException());
		}, (_, _) -> {
			sent.incrementAndGet();
			return CompletableFuture.completedFuture("ok");
		}, executor);
		var context = RequestContext.latency(Duration.ofSeconds(60));
		var waiting = writes.send(context, put(0));
		while (opens.get() < 2) {
			Thread.sleep(10);
		}
		writes.close();
		var error = assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, rocks(error).getErrorUniqueId(), rocks(error).getMessage());
		assertTrue(rocks(error).isNotApplied(), "closed between openings, nothing sent: not applied");
		var later = writes.send(context, put(0));
		error = assertThrows(ExecutionException.class, () -> later.get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, rocks(error).getErrorUniqueId());
		assertTrue(rocks(error).isNotApplied(), "sent on a closed connection, nothing sent: not applied");
		assertEquals(0, sent.get());
		executor.shutdownNow();
	}

	/**
	 * R26-4 review: nothing is opened once the deadline has passed, as the Rust client's {@code remaining} before every
	 * opening. A write whose deadline passed before its session was opened ends {@code SERVER_OVERLOADED} flagged not
	 * applied, without an opening. Before, the opening was sent with a 1 ns gRPC deadline.
	 */
	@Test
	void noSessionIsOpenedOnceTheDeadlineHasPassed() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var opens = new AtomicInteger();
		var writes = new Writes(_ -> {
			opens.incrementAndGet();
			return CompletableFuture.completedFuture(3L);
		}, (_, _) -> CompletableFuture.completedFuture("ok"), executor);
		var context = RequestContext.latency(Duration.ofNanos(1));
		Thread.sleep(1);
		var error = assertThrows(ExecutionException.class,
				() -> writes.send(context, put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, rocks(error).getErrorUniqueId(), rocks(error).getMessage());
		assertTrue(rocks(error).isNotApplied());
		assertEquals(0, opens.get(), "no opening after the deadline");
		executor.shutdownNow();
	}

	/**
	 * R26-4 review: an opening of a caller without a deadline, sent with no gRPC deadline, is ended at the window by a
	 * local timer, as the Rust client's {@code within_window}: the write ends {@code SERVER_OVERLOADED} flagged not
	 * applied, and so does a caller without a deadline that waits for that opening. Before, only keepalive bounded it
	 * (here: never, the opening does not answer).
	 */
	@Test
	void anOpeningThatNeverAnswersEndsACallerWithoutDeadlineUnsentAtTheWindow() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var property = "it.cavallium.rockserver.grpc.client.write-retry-window-ms";
		var previous = System.getProperty(property);
		System.setProperty(property, "200");
		Writes writes;
		try {
			writes = new Writes(_ -> new CompletableFuture<>(), (_, _) -> CompletableFuture.completedFuture("ok"),
					executor);
		} finally {
			if (previous == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, previous);
			}
		}
		var opener = writes.send(RequestContext.batch(), put(0));
		var joined = writes.send(RequestContext.batch(), put(0));
		for (var write : List.of(opener, joined)) {
			var error = assertThrows(ExecutionException.class, () -> write.get(5, TimeUnit.SECONDS));
			assertEquals(RocksDBErrorType.SERVER_OVERLOADED, rocks(error).getErrorUniqueId(), rocks(error).getMessage());
			assertTrue(rocks(error).isNotApplied(), "not sent: not applied");
		}
		executor.shutdownNow();
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

		// A server that does not number batches (it ignores their id) gets them unnumbered, never resent.
		var older = new Writes(_ -> new CompletableFuture<>(), (_, _) -> new CompletableFuture<>(), Runnable::run, false);
		assertFalse(older.numbers(new RocksDBAPICommandSingle.PutBatch(1, Flux.empty(), PutBatchMode.WRITE_BATCH)));
		assertFalse(older.numbers(new RocksDBAPICommandSingle.MergeBatch(1, Flux.empty(),
				MergeBatchMode.MERGE_WRITE_BATCH)));
		assertTrue(older.numbers(new RocksDBAPICommandSingle.DeleteRange(1, KEY, KEY)));
	}

	/**
	 * R6-3: the server collects a numbered streamed multi write whole and refuses one over 16 MiB counted (or its item
	 * limit) as invalid. A streamed putMulti, deleteMulti or mergeMulti over 4 MiB counted is therefore sent unnumbered,
	 * as the Rust client does; a putMulti without results that goes as one unary putMultiList stays numbered.
	 */
	@Test
	void largeStreamedMultiWritesAreNotNumbered() throws Exception {
		var writes = new Writes(_ -> new CompletableFuture<>(), (_, _) -> new CompletableFuture<>(), Runnable::run);
		var smallKeys = List.of(KEY, KEY);
		var smallValues = List.of(Buf.wrap(new byte[10]), Buf.wrap(new byte[10]));
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.PutMulti<>(0, 1, smallKeys, smallValues,
				RequestType.previous())));
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.MergeMulti<>(0, 1, smallKeys, smallValues,
				RequestType.none())));
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.DeleteMulti<>(0, 1, smallKeys, RequestType.none())));

		// 9 values of 1 MiB: over 4 MiB counted, and over the unary putMultiList budget (8 MiB).
		var bigKeys = new ArrayList<Keys>();
		var bigValues = new ArrayList<Buf>();
		for (int i = 0; i < 9; i++) {
			bigKeys.add(KEY);
			bigValues.add(Buf.wrap(new byte[1024 * 1024]));
		}
		assertFalse(writes.numbers(new RocksDBAPICommandSingle.PutMulti<>(0, 1, bigKeys, bigValues,
				RequestType.previous())));
		assertFalse(writes.numbers(new RocksDBAPICommandSingle.PutMulti<>(0, 1, bigKeys, bigValues,
				RequestType.none())));
		assertFalse(writes.numbers(new RocksDBAPICommandSingle.MergeMulti<>(0, 1, bigKeys, bigValues,
				RequestType.none())));

		// Many small items count their per-item costs: 40 000 one-byte keys count over 4 MiB.
		var manyKeys = new ArrayList<Keys>();
		var manyValues = new ArrayList<Buf>();
		for (int i = 0; i < 40_000; i++) {
			manyKeys.add(KEY);
			manyValues.add(Buf.wrap(new byte[1]));
		}
		assertFalse(writes.numbers(new RocksDBAPICommandSingle.DeleteMulti<>(0, 1, manyKeys, RequestType.none())));
		assertFalse(writes.numbers(new RocksDBAPICommandSingle.PutMulti<>(0, 1, manyKeys, manyValues,
				RequestType.changed())));
		// Within putMultiList's 65 536 items and 8 MiB: one unary call, which the server does not collect.
		assertTrue(writes.numbers(new RocksDBAPICommandSingle.PutMulti<>(0, 1, manyKeys, manyValues,
				RequestType.none())));
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
	 * R6-3 review: a multi write that ends an update but goes unnumbered, over the numbering limit, is recognized as
	 * ending a remembered update (the connection then forgets the update when it ended); one of an update this client
	 * does not remember, or with no id, is not.
	 */
	@Test
	void anUnnumberedMultiWriteThatEndsAnUpdateIsRecognized() throws Exception {
		var writes = new Writes(_ -> new CompletableFuture<>(), (_, _) -> new CompletableFuture<>(), Runnable::run);
		var bigKeys = new ArrayList<Keys>();
		var bigValues = new ArrayList<Buf>();
		for (int i = 0; i < 9; i++) {
			bigKeys.add(KEY);
			bigValues.add(Buf.wrap(new byte[1024 * 1024]));
		}
		writes.observe(new Get<>(0, 1, KEY, RequestType.forUpdate()), new UpdateContext<>(null, -21L));
		var ending = new RocksDBAPICommandSingle.PutMulti<>(-21, 1, bigKeys, bigValues, RequestType.previous());
		assertFalse(writes.numbers(ending), "over the numbering limit");
		assertTrue(writes.endsUpdateUnnumbered(ending));
		assertTrue(writes.endsUpdateUnnumbered(new RocksDBAPICommandSingle.MergeMulti<>(-21, 1, bigKeys, bigValues,
				RequestType.none())));
		assertFalse(writes.endsUpdateUnnumbered(new RocksDBAPICommandSingle.PutMulti<>(-22, 1, bigKeys, bigValues,
				RequestType.previous())), "an update this client did not open");
		assertFalse(writes.endsUpdateUnnumbered(new RocksDBAPICommandSingle.PutMulti<>(0, 1, bigKeys, bigValues,
				RequestType.previous())), "no update");
		writes.forgetUpdate(ending);
		assertFalse(writes.numbers(put(-21)), "forgotten");
		assertFalse(writes.endsUpdateUnnumbered(ending));
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
				// Flagged, as a server sends it: only this copy was not applied, the first may have been.
				case 1 -> CompletableFuture.failedFuture(RocksDBException.notApplied(RocksDBErrorType.SESSION_EXPIRED,
						expired().getMessage()));
				default -> CompletableFuture.completedFuture("ok");
			};
		}, executor);
		var context = RequestContext.latency(Duration.ofSeconds(10));
		var error = assertThrows(ExecutionException.class, () -> writes.send(context, put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId());
		assertFalse(rocks(error).isNotApplied());
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
		assertTrue(rocks(error).isNotApplied(), "flagged, though the server's failures were not");
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

	/**
	 * R27-3: a write that joins an opening another write started (with a longer deadline) is bounded by its own
	 * deadline: it ends then, {@code SERVER_OVERLOADED} flagged not applied (nothing was sent, as the Rust client's
	 * {@code not_sent} for a joiner whose deadline came while it waited for the session). The opener goes on, and a
	 * session opened later serves it. Before, the joiner waited for the opener's deadline.
	 */
	@Test
	void aWriteJoiningAnOpeningEndsAtItsOwnDeadlineNotApplied() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var opening = new CompletableFuture<Long>();
		var sent = new AtomicInteger();
		var writes = new Writes(_ -> opening, (_, _) -> {
			sent.incrementAndGet();
			return CompletableFuture.completedFuture("ok");
		}, executor);
		var opener = writes.send(RequestContext.latency(Duration.ofSeconds(30)), put(0));
		long start = System.nanoTime();
		var joiner = writes.send(RequestContext.latency(Duration.ofMillis(300)), put(0));
		var error = assertThrows(ExecutionException.class, () -> joiner.get(5, TimeUnit.SECONDS));
		long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		assertTrue(tookMs < 2_000, "the joiner waited " + tookMs + " ms");
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, rocks(error).getErrorUniqueId(), rocks(error).getMessage());
		assertTrue(rocks(error).isNotApplied(), "not sent: not applied");
		assertFalse(opener.isDone(), "the opener has its own deadline");
		opening.complete(3L);
		assertEquals("ok", opener.get(5, TimeUnit.SECONDS));
		assertEquals(1, sent.get(), "only the opener's copy");
		executor.shutdownNow();
	}

	/**
	 * R27-3: a synchronous wait that ends before any copy was sent (the session is still opening) cancels the write,
	 * which was not applied: the error is flagged so. Before, it was READ_DEADLINE_EXCEEDED without the flag, which reads
	 * as an unknown outcome.
	 */
	@Test
	void aSynchronousWaitThatEndsBeforeACopyWasSentIsNotApplied() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var sent = new AtomicInteger();
		var writes = new Writes(_ -> new CompletableFuture<>(), (_, _) -> {
			sent.incrementAndGet();
			return CompletableFuture.completedFuture("ok");
		}, executor);
		var result = writes.send(RequestContext.latency(Duration.ofSeconds(30)), put(0));
		var error = assertThrows(RocksDBException.class, () -> Writes.await(result, TimeUnit.MILLISECONDS.toNanos(200)));
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
		assertTrue(error.isNotApplied(), error.getMessage());
		assertEquals(0, sent.get());
		// A plain future (not a numbered write) says nothing about what was sent: no flag.
		var plain = assertThrows(RocksDBException.class,
				() -> Writes.await(new CompletableFuture<>(), TimeUnit.MILLISECONDS.toNanos(50)));
		assertFalse(plain.isNotApplied());
		executor.shutdownNow();
	}

	/**
	 * An opening that ended "not applied" at the opener's deadline (the proposer still had it queued), or refused as
	 * overloaded, does not end a write waiting for it with more time: that write opens again in its own time.
	 */
	@Test
	void aSharedOpeningEndedByTheOpenersDeadlineOrOverloadIsOpenedAgain() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var opens = new AtomicInteger();
		var firstOpening = new CompletableFuture<Long>();
		var writes = new Writes(_ -> switch (opens.incrementAndGet()) {
			case 1 -> firstOpening;
			case 2 -> CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "busy"));
			default -> CompletableFuture.completedFuture(4L);
		}, (_, _) -> CompletableFuture.completedFuture("ok"), executor);
		var shortWrite = writes.send(RequestContext.latency(Duration.ofMillis(200)), put(0));
		var longWrite = writes.send(RequestContext.latency(Duration.ofSeconds(10)), put(0));
		Thread.sleep(250);
		firstOpening.completeExceptionally(RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				"The request deadline came before the write was sent; it was not applied"));
		assertEquals("ok", longWrite.get(5, TimeUnit.SECONDS));
		var error = assertThrows(ExecutionException.class, () -> shortWrite.get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, rocks(error).getErrorUniqueId(),
				"the opener, out of time, ends with its own deadline");
		assertEquals(3, opens.get(), "opened again after the deadline error, and again after the overload");
		executor.shutdownNow();
	}

	/**
	 * R26-3: a write that could not get a session was never sent, so it ends flagged not applied, whatever the opening
	 * answered. Before, the opening's own error came back unflagged: gRPC's deadline on the opening (mapped to
	 * {@code READ_DEADLINE_EXCEEDED}) then read as an unknown outcome, and a refusal such as {@code NOT_IMPLEMENTED}
	 * did not say that nothing was applied. As in the Rust client.
	 */
	@Test
	void aWriteThatGotNoSessionEndsNotApplied() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var sent = new AtomicInteger();
		// gRPC's own deadline on every opening, as GrpcConnection maps it: unflagged.
		var timingOut = new Writes(_ -> CompletableFuture.failedFuture(RocksDBException.of(
				RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded")), (_, _) -> {
			sent.incrementAndGet();
			return CompletableFuture.completedFuture("ok");
		}, executor);
		var error = assertThrows(ExecutionException.class,
				() -> timingOut.send(RequestContext.latency(Duration.ofMillis(300)), put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, rocks(error).getErrorUniqueId());
		assertTrue(rocks(error).isNotApplied(), "nothing was sent: not applied");

		var refusing = new Writes(_ -> CompletableFuture.failedFuture(
				RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED, "no sessions")), (_, _) -> {
			sent.incrementAndGet();
			return CompletableFuture.completedFuture("ok");
		}, executor);
		error = assertThrows(ExecutionException.class,
				() -> refusing.send(RequestContext.latency(Duration.ofSeconds(10)), put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.NOT_IMPLEMENTED, rocks(error).getErrorUniqueId());
		assertTrue(rocks(error).isNotApplied(), "refused before anything was sent: not applied");

		var unknown = new Writes(_ -> CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException()),
				(_, _) -> {
					sent.incrementAndGet();
					return CompletableFuture.completedFuture("ok");
				}, executor);
		error = assertThrows(ExecutionException.class,
				() -> unknown.send(RequestContext.latency(Duration.ofMillis(300)), put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, rocks(error).getErrorUniqueId());
		assertTrue(rocks(error).isNotApplied(), "an opening of unknown outcome sent no write: not applied");
		assertEquals(0, sent.get(), "no copy was sent");
		executor.shutdownNow();
	}

	/**
	 * R26-4: for a caller without a deadline the retry window bounds the copies too, not only the resends. A session
	 * that opens after the window sends no copy: the write ends {@code READ_DEADLINE_EXCEEDED} flagged not applied,
	 * where a copy sent then and lost would end {@code OUTCOME_UNKNOWN}. As in the Rust client (R20-1).
	 */
	@Test
	void aSessionOpenedAfterTheWindowSendsNoCopyForACallerWithoutDeadline() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var property = "it.cavallium.rockserver.grpc.client.write-retry-window-ms";
		var previous = System.getProperty(property);
		System.setProperty(property, "200");
		Writes writes;
		var sent = new AtomicInteger();
		try {
			writes = new Writes(_ -> {
				var late = new CompletableFuture<Long>();
				CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS).execute(() -> late.complete(7L));
				return late;
			}, (_, _) -> {
				sent.incrementAndGet();
				return CompletableFuture.completedFuture("ok");
			}, executor);
		} finally {
			if (previous == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, previous);
			}
		}
		// The opening is started by a caller with a deadline, so the window's timer on an opening (which a caller
		// without one would start) does not end it: the caller without a deadline waits for it and gets the session
		// after its window.
		var timed = writes.send(RequestContext.latency(Duration.ofSeconds(10)), put(0));
		var error = assertThrows(ExecutionException.class,
				() -> writes.send(RequestContext.batch(), put(0)).get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, rocks(error).getErrorUniqueId());
		assertTrue(rocks(error).isNotApplied(), "not sent: not applied");
		assertEquals("ok", timed.get(5, TimeUnit.SECONDS));
		assertEquals(1, sent.get(), "no copy after the window, only the timed caller's");
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

	/** An answer that the fake sender throws instead of returning a failed future. */
	private record Thrown(RuntimeException error) {}

	private static RocksDBException notApplied(RocksDBErrorType type) {
		return RocksDBException.notApplied(type, "The request deadline came before the write was sent");
	}

	/** Runs one write against a sender that answers each copy in turn from {@code answers}; returns the copies sent. */
	private static List<ClientRequestId> sendAgainst(Duration deadline, CompletableFuture<Object>[] result,
			Object... answers) throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var ids = Collections.synchronizedList(new ArrayList<ClientRequestId>());
		var writes = new Writes(_ -> CompletableFuture.completedFuture(3L), (bound, _) -> {
			ids.add(idOf(bound));
			var answer = answers[Math.min(ids.size() - 1, answers.length - 1)];
			if (answer instanceof Thrown thrown) {
				throw thrown.error;
			}
			return answer instanceof Throwable error ? CompletableFuture.failedFuture(error)
					: CompletableFuture.completedFuture(answer);
		}, executor);
		result[0] = writes.send(RequestContext.latency(deadline), put(0));
		try {
			result[0].get(10, TimeUnit.SECONDS);
		} catch (ExecutionException ignored) {
			// Checked by the caller.
		} finally {
			executor.shutdownNow();
		}
		return ids;
	}

	@SuppressWarnings("unchecked")
	private static CompletableFuture<Object>[] holder() {
		return new CompletableFuture[1];
	}

	/**
	 * Each branch of a copy's answer (CR-4): a value or a failure that is the write's answer ends it at once; an unknown
	 * outcome sends the same number again, and after it a "not applied" answer does too; a sender that throws counts
	 * as a lost copy.
	 */
	@Test
	void eachAnswerOfACopyEndsOrResendsTheWrite() throws Exception {
		var result = holder();
		assertEquals(1, sendAgainst(Duration.ofSeconds(5), result, "ok").size());
		assertEquals("ok", result[0].get());

		// The write's own answer: a recorded failure, overload or a flagged "not applied" deadline on the first copy.
		for (var answer : List.of(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, "bad"),
				RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "busy"),
				notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED))) {
			assertEquals(1, sendAgainst(Duration.ofSeconds(5), result, answer).size(), answer.getMessage());
			var error = assertThrows(ExecutionException.class, () -> result[0].get());
			assertEquals(answer.getErrorUniqueId(), rocks(error).getErrorUniqueId());
		}

		// Unknown, then "not applied" (says nothing about the first copy), then the answer: same number each time.
		var ids = sendAgainst(Duration.ofSeconds(5), result, Status.UNAVAILABLE.asRuntimeException(),
				RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "busy"),
				notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED),
				RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN, "lost"), "ok");
		assertEquals("ok", result[0].get());
		assertEquals(5, ids.size());
		assertEquals(1, ids.stream().distinct().count(), "one number: " + ids);

		// A sender that throws instead of failing its future: the copy is lost, its outcome unknown.
		ids = sendAgainst(Duration.ofSeconds(5), result, new Thrown(Status.UNAVAILABLE.asRuntimeException()), "ok");
		assertEquals("ok", result[0].get());
		assertEquals(2, ids.size());
	}

	/**
	 * Y-3: a resent copy that ends with gRPC's DEADLINE_EXCEEDED (mapped by the client to READ_DEADLINE_EXCEEDED without
	 * the server's "not applied" flag, or raw) ends the write OUTCOME_UNKNOWN, never as a plain deadline error, which a
	 * caller would take for "not applied". On a first copy an unflagged deadline is unknown too: it is sent again.
	 */
	@Test
	void aDeadlineWithoutTheNotAppliedFlagIsAnUnknownOutcome() throws Exception {
		var result = holder();
		for (Object deadline : List.of(RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded"),
				Status.DEADLINE_EXCEEDED.asRuntimeException())) {
			var ids = sendAgainst(Duration.ofMillis(400), result, Status.UNAVAILABLE.asRuntimeException(), deadline);
			var error = assertThrows(ExecutionException.class, () -> result[0].get());
			assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId(), String.valueOf(deadline));
			assertTrue(ids.size() >= 2, "resent: " + ids);

			ids = sendAgainst(Duration.ofMillis(400), result, deadline);
			error = assertThrows(ExecutionException.class, () -> result[0].get());
			assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId(), String.valueOf(deadline));
			assertTrue(ids.size() >= 2, "a first copy's unflagged deadline is unknown, so it is sent again: " + ids);
		}
		// The message alone does not make it "not applied": only the flag does.
		var ids = sendAgainst(Duration.ofMillis(400), result,
				RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "it was not applied"));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN,
				rocks(assertThrows(ExecutionException.class, () -> result[0].get())).getErrorUniqueId());
		assertTrue(ids.size() >= 2);
	}

	/**
	 * WS-12: a caller that cancels a write stops its resends and cancels the copy in flight. A copy was sent, so the
	 * future ends OUTCOME_UNKNOWN instead of cancelled.
	 */
	@Test
	void cancellingAWriteAfterACopyWasSentStopsItUnknown() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var copies = Collections.synchronizedList(new ArrayList<CompletableFuture<Object>>());
		var writes = new Writes(_ -> CompletableFuture.completedFuture(3L), (_, _) -> {
			if (copies.isEmpty()) {
				copies.add(CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException()));
			} else {
				copies.add(new CompletableFuture<>());
			}
			return copies.getLast();
		}, executor);
		var result = writes.send(RequestContext.latency(Duration.ofSeconds(30)), put(0));
		while (copies.size() < 2) {
			Thread.sleep(5);
		}
		assertFalse(result.cancel(true), "not cancelled: it ended unknown");
		assertFalse(result.isCancelled());
		var error = assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId());
		assertTrue(copies.getLast().isCancelled(), "the copy in flight is cancelled");
		Thread.sleep(1_200);
		assertEquals(2, copies.size(), "no copy sent after the cancel");
		executor.shutdownNow();
	}

	/**
	 * A reactive caller ({@code Mono.fromFuture}, as Yotsuba wraps the async API) that disposes its subscription, or
	 * whose {@code timeout} fires, cancels the future: the resends stop and the copy in flight is cancelled.
	 */
	@Test
	void disposingAMonoOverAWriteStopsItsResends() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var copies = Collections.synchronizedList(new ArrayList<CompletableFuture<Object>>());
		var writes = new Writes(_ -> CompletableFuture.completedFuture(3L), (_, _) -> {
			if (copies.size() < 2) {
				copies.add(CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException()));
			} else {
				copies.add(new CompletableFuture<>());
			}
			return copies.getLast();
		}, executor);
		var result = writes.send(RequestContext.latency(Duration.ofSeconds(30)), put(0));
		var subscription = Mono.fromFuture(result).subscribe(_ -> {}, _ -> {});
		while (copies.size() < 3) {
			Thread.sleep(5);
		}
		subscription.dispose();
		var error = assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId());
		assertTrue(copies.getLast().isCancelled(), "the copy in flight is cancelled");
		Thread.sleep(1_200);
		assertEquals(3, copies.size(), "no copy sent after the dispose");

		// The same through an operator timeout: the caller's Mono ends, the write stops.
		var lost = Collections.synchronizedList(new ArrayList<CompletableFuture<Object>>());
		var timedOut = new Writes(_ -> CompletableFuture.completedFuture(4L), (_, _) -> {
			lost.add(CompletableFuture.failedFuture(Status.UNAVAILABLE.asRuntimeException()));
			return lost.getLast();
		}, executor);
		var write = timedOut.send(RequestContext.latency(Duration.ofSeconds(30)), put(0));
		assertThrows(RuntimeException.class, () -> Mono.fromFuture(write)
				.timeout(Duration.ofMillis(300)).block());
		assertTrue(write.isDone());
		int sent = lost.size();
		Thread.sleep(1_200);
		assertEquals(sent, lost.size(), "no copy sent after the timeout");
		executor.shutdownNow();
	}

	/** A write cancelled before any copy was sent (its session is still opening) is cancelled, and never sent. */
	@Test
	void cancellingAWriteBeforeItWasSentCancelsIt() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var opening = new CompletableFuture<Long>();
		var sent = new AtomicInteger();
		var writes = new Writes(_ -> opening, (_, _) -> {
			sent.incrementAndGet();
			return CompletableFuture.completedFuture("ok");
		}, executor);
		var result = writes.send(RequestContext.latency(Duration.ofSeconds(30)), put(0));
		assertTrue(result.cancel(true));
		assertTrue(result.isCancelled());
		opening.complete(3L);
		Thread.sleep(100);
		assertEquals(0, sent.get(), "never sent");
		assertEquals("ok", writes.send(RequestContext.latency(Duration.ofSeconds(5)), put(0)).get(5, TimeUnit.SECONDS));
		executor.shutdownNow();
	}

	/** Cancelling a batch after a chunk was sent ends it unknown and cancels the chunk in flight. */
	@Test
	void cancellingABatchAfterAChunkWasSentEndsItUnknown() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var copies = Collections.synchronizedList(new ArrayList<CompletableFuture<Object>>());
		var writes = new Writes(_ -> CompletableFuture.completedFuture(3L), (_, _) -> {
			copies.add(new CompletableFuture<>());
			return copies.getLast();
		}, executor);
		var result = writes.send(RequestContext.batch(Duration.ofSeconds(30)),
				new RocksDBAPICommandSingle.PutBatch(1, Flux.just(batch(2, 1)), PutBatchMode.WRITE_BATCH));
		while (copies.isEmpty()) {
			Thread.sleep(5);
		}
		assertFalse(result.cancel(true));
		var error = assertThrows(ExecutionException.class, () -> result.get(1, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId());
		assertTrue(copies.getFirst().isCancelled(), "the chunk in flight is cancelled");
		executor.shutdownNow();
	}

	/**
	 * F19: a synchronous caller waits at most the time given: then the write is cancelled, and ends unknown because a
	 * copy was sent.
	 */
	@Test
	void aSynchronousWaitIsBoundedAndEndsUnknown() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var writes = new Writes(_ -> CompletableFuture.completedFuture(3L), (_, _) -> new CompletableFuture<>(), executor);
		var result = writes.send(RequestContext.latency(Duration.ofSeconds(30)), put(0));
		Thread.sleep(50);
		long start = System.nanoTime();
		var error = assertThrows(RocksDBException.class, () -> Writes.await(result, TimeUnit.MILLISECONDS.toNanos(200)));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, error.getErrorUniqueId());
		assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5));
		executor.shutdownNow();
	}

	/** {@code SessionWrites.syncWaitNanos} of a write sent with {@code context}. */
	private static long syncWaitNanos(RequestContext context) {
		Object bound = Writes.callOn(Writes.BOUND, null, "bind", new Class<?>[] {RequestContext.class}, context);
		return (Long) Writes.call(null, "syncWaitNanos", new Class<?>[] {Writes.BOUND}, bound);
	}

	/**
	 * A caller without a deadline is not bounded (a long ingest batch through the sync API must not be cut and end
	 * unknown); a huge finite timeout ("forever", clamped by RequestContext) saturates instead of overflowing to a
	 * negative wait; a finite one waits its deadline plus the margin.
	 */
	@Test
	void theSynchronousWaitIsUnboundedWithoutADeadlineAndNeverOverflows() throws Exception {
		assertEquals(Long.MAX_VALUE, syncWaitNanos(RequestContext.ingest()));
		assertEquals(Long.MAX_VALUE, syncWaitNanos(RequestContext.batch()));
		assertEquals(Long.MAX_VALUE, syncWaitNanos(RequestContext.latency(ChronoUnit.FOREVER.getDuration())));
		assertEquals(Long.MAX_VALUE, syncWaitNanos(RequestContext.latency(Duration.ofSeconds(Long.MAX_VALUE))));
		long wait = syncWaitNanos(RequestContext.latency(Duration.ofSeconds(1)));
		assertTrue(wait > TimeUnit.SECONDS.toNanos(5) && wait <= TimeUnit.SECONDS.toNanos(6), String.valueOf(wait));

		// The unbounded wait still returns the answer.
		var answer = new CompletableFuture<Object>();
		CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS).execute(() -> answer.complete("ok"));
		assertEquals("ok", Writes.await(answer, Long.MAX_VALUE));
	}

	/**
	 * The client's own "Request deadline already expired" check (thrown before anything is sent) is flagged not
	 * applied: a first copy that meets it ends the write with the deadline error, not OUTCOME_UNKNOWN.
	 */
	@Test
	void theClientsOwnExpiredDeadlineOnAFirstCopyIsNotApplied() throws Exception {
		var result = holder();
		var expired = RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				"Request deadline already expired");
		assertEquals(1, sendAgainst(Duration.ofSeconds(5), result, new Thrown(expired)).size());
		var error = assertThrows(ExecutionException.class, () -> result[0].get());
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, rocks(error).getErrorUniqueId());
	}

	/**
	 * A batch cancelled while its session is still opening sent no chunk: it is cancelled (not applied), and no chunk
	 * is sent afterwards. Before, the batch counted a chunk as sent as soon as it was handed to the session, sent or not.
	 */
	@Test
	void cancellingABatchBeforeAChunkWasSentCancelsIt() throws Exception {
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var opening = new CompletableFuture<Long>();
		var sent = new AtomicInteger();
		var writes = new Writes(_ -> opening, (_, _) -> {
			sent.incrementAndGet();
			return CompletableFuture.completedFuture(null);
		}, executor);
		var result = writes.send(RequestContext.batch(Duration.ofSeconds(30)),
				new RocksDBAPICommandSingle.PutBatch(1, Flux.just(batch(2, 1)), PutBatchMode.WRITE_BATCH));
		Thread.sleep(50);
		assertTrue(result.cancel(true));
		assertTrue(result.isCancelled());
		opening.complete(3L);
		Thread.sleep(100);
		assertEquals(0, sent.get(), "never sent");
		executor.shutdownNow();
	}

	/**
	 * CR-3: past the limit of remembered updates the oldest is forgotten, counted, and the write that ends it is not
	 * numbered; the newest still are.
	 */
	@Test
	void pastTheLimitTheOldestUpdateIsForgottenAndCounted() throws Exception {
		var writes = new Writes(_ -> new CompletableFuture<>(), (_, _) -> new CompletableFuture<>(), Runnable::run);
		var opening = new Get<>(0, 1, KEY, RequestType.forUpdate());
		int limit = 1 << 16;
		for (int i = 1; i <= limit + 1; i++) {
			writes.observe(opening, new UpdateContext<>(null, -i));
		}
		assertEquals(1, writes.forgottenUpdates());
		assertFalse(writes.numbers(put(-1)), "the oldest is forgotten");
		assertTrue(writes.numbers(put(-2)));
		assertTrue(writes.numbers(put(-(limit + 1))), "the newest is remembered");
	}
}
