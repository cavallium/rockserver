package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.rocksdb.REntry;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.rocksdb.RocksIterator;

@Timeout(10)
class IteratorCloseConcurrencyTest {

	@TempDir
	Path tempDir;

	@Test
	void activeAndQueuedPagingSerializeBeforeCloseClaimsTheNativeIterator() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("iterator-active-close"),
				"iterator-active-close", null)) {
			var api = connection.getSyncApi();
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = api.createColumn(
					"column",
					ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
			for (int entry : new int[] {10, 20, 30, 40, 50, 60}) {
				api.put(0, columnId, key(entry), value(entry), RequestType.none());
			}

			long iteratorId = api.openIterator(0, columnId, key(10), null, false, 10_000);
			Map<Long, REntry<RocksIterator>> iterators = getIterators(internal);
			REntry<RocksIterator> iteratorEntry = iterators.get(iteratorId);
			assertNotNull(iteratorEntry);
			Object iteratorState = iteratorEntry.objs();

			var firstPageEnteredNativeRead = new CountDownLatch(1);
			var releaseFirstPage = new CountDownLatch(1);
			var closeRaceEnteredNativeRead = new CountDownLatch(1);
			var releaseCloseRacePage = new CountDownLatch(1);
			var nativeValueCalls = new AtomicInteger();
			RocksIterator controlledIterator = delegateWithReadGates(
					iteratorEntry.val(),
					nativeValueCalls,
					firstPageEnteredNativeRead,
					releaseFirstPage,
					closeRaceEnteredNativeRead,
					releaseCloseRacePage);
			replaceIterator(iteratorState, controlledIterator);

			var threads = new ArrayList<Thread>();
			try {
				FutureTask<List<Buf>> firstPage = pageTask(api, iteratorId, 2);
				Thread firstPageThread = start(threads, "iterator-first-page", firstPage);
				assertTrue(firstPageEnteredNativeRead.await(2, TimeUnit.SECONDS),
						"the first page never reached the controlled native read");

				FutureTask<List<Buf>> secondPage = pageTask(api, iteratorId, 2);
				Thread secondPageThread = start(threads, "iterator-second-page", secondPage);
				awaitBlockedAt(secondPageThread, "withIterator");
				releaseFirstPage.countDown();

				List<Buf> firstValues = firstPage.get(3, TimeUnit.SECONDS);
				List<Buf> secondValues = secondPage.get(3, TimeUnit.SECONDS);
				assertIterableEquals(values(10, 20), firstValues);
				assertIterableEquals(values(30, 40), secondValues,
						"concurrent paging calls must serialize without returning duplicate values");

				FutureTask<List<Buf>> activeCloseRacePage = pageTask(api, iteratorId, 2);
				start(threads, "iterator-active-close-page", activeCloseRacePage);
				assertTrue(closeRaceEnteredNativeRead.await(2, TimeUnit.SECONDS),
						"the close-race page never reached the controlled native read");

				FutureTask<List<Buf>> queuedCloseRacePage = pageTask(api, iteratorId, 1);
				Thread queuedPageThread = start(threads, "iterator-queued-close-page", queuedCloseRacePage);
				awaitBlockedAt(queuedPageThread, "withIterator");

				FutureTask<Void> close = new FutureTask<>(() -> {
					api.closeIterator(iteratorId);
					return null;
				});
				Thread closeThread = start(threads, "iterator-close", close);
				awaitIteratorClaimed(internal);
				awaitBlockedAt(closeThread, "closeIteratorInternal");

				releaseCloseRacePage.countDown();
				List<Buf> closeRaceValues = activeCloseRacePage.get(3, TimeUnit.SECONDS);
				assertIterableEquals(values(50, 60), closeRaceValues,
						"an active page must finish before close releases its native iterator");
				close.get(3, TimeUnit.SECONDS);

				ExecutionException queuedFailure = assertThrows(ExecutionException.class,
						() -> queuedCloseRacePage.get(3, TimeUnit.SECONDS));
				assertTrue(queuedFailure.getCause() instanceof RocksDBException,
						"a page queued behind a claimed iterator must fail through the API, not touch a closed native handle");

				assertIterableEquals(values(10, 20, 30, 40, 50, 60),
						java.util.stream.Stream.of(firstValues, secondValues, closeRaceValues)
								.flatMap(List::stream)
								.toList(),
						"serialized successful pages must contain every value exactly once");
				assertEquals(6, nativeValueCalls.get());

				api.closeIterator(iteratorId);
				assertEquals(0, internal.getOpenIteratorsCount());
				assertEquals(0, internal.getPendingOpsCount());
			} finally {
				releaseFirstPage.countDown();
				releaseCloseRacePage.countDown();
				api.closeIterator(iteratorId);
				for (Thread thread : threads) {
					thread.join(TimeUnit.SECONDS.toMillis(2));
					assertFalse(thread.isAlive(), () -> thread.getName() + " did not terminate");
				}
			}
		}
	}

	@Test
	void forcedAndExplicitCloseClaimIteratorLifetimeExactlyOnce() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("iterator-close"), "iterator-close", null)) {
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = connection.getSyncApi().createColumn(
					"column",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			long iteratorId = connection.getSyncApi().openIterator(
					0,
					columnId,
					new Keys(new Buf[] {Buf.wrap(new byte[Integer.BYTES])}),
					null,
					false,
					10_000);

			Map<Long, REntry<RocksIterator>> iterators = getIterators(internal);
			REntry<RocksIterator> nativeEntry = iterators.remove(iteratorId);
			assertNotNull(nativeEntry);
			nativeEntry.close();

			@SuppressWarnings("unchecked")
			REntry<RocksIterator> blockingEntry = Mockito.mock(REntry.class);
			iterators.put(iteratorId, blockingEntry);
			var closeCalls = new AtomicInteger();
			var forcedCloseStarted = new CountDownLatch(1);
			var releaseForcedClose = new CountDownLatch(1);
			Mockito.doAnswer(invocation -> {
				if (closeCalls.incrementAndGet() == 1) {
					forcedCloseStarted.countDown();
					assertTrue(releaseForcedClose.await(1, TimeUnit.SECONDS));
				}
				return null;
			}).when(blockingEntry).close();

			try (var executor = Executors.newSingleThreadExecutor()) {
				var forcedCleanup = executor.submit(() -> invokeForcedCleanup(internal));
				assertTrue(forcedCloseStarted.await(1, TimeUnit.SECONDS));
				try {
					connection.getSyncApi().closeIterator(iteratorId);
				} finally {
					releaseForcedClose.countDown();
				}
				forcedCleanup.get(3, TimeUnit.SECONDS);
			}

			assertEquals(1, closeCalls.get(), "only the thread that removed the iterator may close it");
			assertEquals(0, internal.getOpenIteratorsCount());
			assertEquals(0, internal.getPendingOpsCount());
		}
	}

	@Test
	void throwingNativeCloseStillClaimsIteratorAndBalancesLifetimeExactlyOnce() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("iterator-close-failure"),
				"iterator-close-failure", null)) {
			EmbeddedDB internal = connection.getInternalDB();
			long columnId = connection.getSyncApi().createColumn(
					"column",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			long iteratorId = connection.getSyncApi().openIterator(
					0,
					columnId,
					new Keys(new Buf[] {Buf.wrap(new byte[Integer.BYTES])}),
					null,
					false,
					10_000);

			Map<Long, REntry<RocksIterator>> iterators = getIterators(internal);
			REntry<RocksIterator> nativeEntry = iterators.remove(iteratorId);
			assertNotNull(nativeEntry);
			nativeEntry.close();

			@SuppressWarnings("unchecked")
			REntry<RocksIterator> throwingEntry = Mockito.mock(REntry.class);
			RuntimeException failure = new RuntimeException("synthetic iterator close failure");
			Mockito.doThrow(failure).when(throwingEntry).close();
			iterators.put(iteratorId, throwingEntry);

			RuntimeException actual = assertThrows(RuntimeException.class,
					() -> connection.getSyncApi().closeIterator(iteratorId));
			assertSame(failure, actual);
			assertEquals(0, internal.getOpenIteratorsCount());
			assertEquals(0, internal.getPendingOpsCount());

			connection.getSyncApi().closeIterator(iteratorId);
			invokeForcedCleanup(internal);
			Mockito.verify(throwingEntry, Mockito.times(1)).close();
			assertEquals(0, internal.getPendingOpsCount());
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<Long, REntry<RocksIterator>> getIterators(EmbeddedDB db) throws Exception {
		Field iteratorsField = EmbeddedDB.class.getDeclaredField("its");
		iteratorsField.setAccessible(true);
		return (Map<Long, REntry<RocksIterator>>) iteratorsField.get(db);
	}

	private static void invokeForcedCleanup(EmbeddedDB db) {
		try {
			Method cleanup = EmbeddedDB.class.getDeclaredMethod("forceCloseLeakedResources");
			cleanup.setAccessible(true);
			cleanup.invoke(db);
		} catch (ReflectiveOperationException exception) {
			throw new RuntimeException(exception);
		}
	}

	private static RocksIterator delegateWithReadGates(RocksIterator nativeIterator,
			AtomicInteger valueCalls,
			CountDownLatch firstReadEntered,
			CountDownLatch releaseFirstRead,
			CountDownLatch closeRaceReadEntered,
			CountDownLatch releaseCloseRaceRead) throws org.rocksdb.RocksDBException {
		RocksIterator delegate = Mockito.mock(RocksIterator.class);
		Mockito.when(delegate.isValid()).thenAnswer(_ -> nativeIterator.isValid());
		Mockito.when(delegate.value()).thenAnswer(_ -> {
			int call = valueCalls.incrementAndGet();
			if (call == 1) {
				firstReadEntered.countDown();
				assertTrue(releaseFirstRead.await(2, TimeUnit.SECONDS));
			} else if (call == 5) {
				closeRaceReadEntered.countDown();
				assertTrue(releaseCloseRaceRead.await(2, TimeUnit.SECONDS));
			}
			return nativeIterator.value();
		});
		Mockito.doAnswer(_ -> {
			nativeIterator.next();
			return null;
		}).when(delegate).next();
		Mockito.doAnswer(_ -> {
			nativeIterator.status();
			return null;
		}).when(delegate).status();
		return delegate;
	}

	private static void replaceIterator(Object iteratorState, RocksIterator iterator) throws Exception {
		Field iteratorField = iteratorState.getClass().getDeclaredField("iterator");
		iteratorField.setAccessible(true);
		iteratorField.set(iteratorState, iterator);
	}

	private static FutureTask<List<Buf>> pageTask(it.cavallium.rockserver.core.common.RocksDBSyncAPI api,
			long iteratorId,
			long takeCount) {
		return new FutureTask<>(() -> api.subsequent(iteratorId, 0, takeCount, RequestType.multi()));
	}

	private static Thread start(List<Thread> threads, String name, Runnable task) {
		Thread thread = Thread.ofPlatform().name(name).unstarted(task);
		threads.add(thread);
		thread.start();
		return thread;
	}

	private static void awaitBlockedAt(Thread thread, String methodName) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		do {
			if (thread.getState() == Thread.State.BLOCKED
					&& Arrays.stream(thread.getStackTrace())
							.anyMatch(frame -> frame.getClassName().equals(EmbeddedDB.class.getName())
									&& frame.getMethodName().equals(methodName))) {
				return;
			}
			Thread.onSpinWait();
		} while (System.nanoTime() < deadline);
		throw new AssertionError(thread.getName() + " did not block in " + methodName + ": "
				+ Arrays.toString(thread.getStackTrace()));
	}

	private static void awaitIteratorClaimed(EmbeddedDB db) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		while (db.getOpenIteratorsCount() != 0 && System.nanoTime() < deadline) {
			Thread.onSpinWait();
		}
		assertEquals(0, db.getOpenIteratorsCount(), "close never claimed the iterator registration");
	}

	private static Keys key(int value) {
		return new Keys(it.cavallium.rockserver.core.common.Utils.toBufSimple(value));
	}

	private static Buf value(int value) {
		return it.cavallium.rockserver.core.common.Utils.toBufSimple(value);
	}

	private static List<Buf> values(int... values) {
		return Arrays.stream(values).mapToObj(IteratorCloseConcurrencyTest::value).toList();
	}
}
