package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class CdcMetadataConcurrencyTest {

	private static final String SUBSCRIPTION_ID = "metadata-concurrency";

	@TempDir
	Path tempDir;

	private final List<CountDownLatch> blockedLoadReleases = new ArrayList<>();
	private EmbeddedDB db;
	private ExecutorService executor;

	@BeforeEach
	void setUp() throws IOException {
		db = new EmbeddedDB(tempDir, "cdc-metadata-concurrency", null);
		executor = Executors.newFixedThreadPool(2);
	}

	@AfterEach
	void tearDown() throws Exception {
		for (var release : blockedLoadReleases) {
			release.countDown();
		}
		if (db != null) {
			db.setCdcMetadataOperationObserverForTesting(null);
			db.setCdcMetadataLoadedObserverForTesting(null);
		}
		if (executor != null) {
			executor.shutdownNow();
			assertTrue(executor.awaitTermination(10, SECONDS), "CDC metadata workers did not stop");
		}
		if (db != null) {
			db.closeTesting();
		}
	}

	@Test
	void concurrentCommitsCannotRegressCheckpoint() throws Exception {
		db.cdcCreate(SUBSCRIPTION_ID, 1L, null, false);
		var firstCommit = blockNextMetadataLoad("commit");

		Future<?> lowerCommit = executor.submit(() -> db.cdcCommit(SUBSCRIPTION_ID, 10L));
		assertLoaded(firstCommit);

		var higherCommitAttempted = observeNextOperation("commit");
		Future<?> higherCommit = executor.submit(() -> db.cdcCommit(SUBSCRIPTION_ID, 20L));
		assertAttempted(higherCommitAttempted, "higher commit");
		assertBlocked(higherCommit, "higher commit");

		firstCommit.release().countDown();
		lowerCommit.get(10, SECONDS);
		higherCommit.get(10, SECONDS);

		assertEquals(OptionalLong.of(20L), db.cdcGetLastCommittedSequence(SUBSCRIPTION_ID));
	}

	@Test
	void periodicCreateCannotOverwriteConcurrentCommit() throws Exception {
		db.cdcCreate(SUBSCRIPTION_ID, 1L, null, false);
		db.cdcCommit(SUBSCRIPTION_ID, 10L);
		var periodicCreate = blockNextMetadataLoad("create");

		Future<Long> create = executor.submit(() -> db.cdcCreate(SUBSCRIPTION_ID, 0L, null, false));
		assertLoaded(periodicCreate);

		var commitAttempted = observeNextOperation("commit");
		Future<?> commit = executor.submit(() -> db.cdcCommit(SUBSCRIPTION_ID, 42L));
		assertAttempted(commitAttempted, "commit during periodic create");
		assertBlocked(commit, "commit during periodic create");

		periodicCreate.release().countDown();
		create.get(10, SECONDS);
		commit.get(10, SECONDS);

		assertEquals(OptionalLong.of(42L), db.cdcGetLastCommittedSequence(SUBSCRIPTION_ID));
	}

	@Test
	void deleteAfterInFlightCommitCannotBeResurrected() throws Exception {
		db.cdcCreate(SUBSCRIPTION_ID, 1L, null, false);
		var commitLoad = blockNextMetadataLoad("commit");

		Future<?> commit = executor.submit(() -> db.cdcCommit(SUBSCRIPTION_ID, 42L));
		assertLoaded(commitLoad);

		var deleteAttempted = observeNextOperation("delete");
		Future<?> delete = executor.submit(() -> db.cdcDelete(SUBSCRIPTION_ID));
		assertAttempted(deleteAttempted, "delete during commit");
		assertBlocked(delete, "delete during commit");

		commitLoad.release().countDown();
		commit.get(10, SECONDS);
		delete.get(10, SECONDS);

		assertEquals(OptionalLong.empty(), db.cdcGetLastCommittedSequence(SUBSCRIPTION_ID));
	}

	@Test
	void deleteAfterInFlightCreateCannotBeResurrected() throws Exception {
		var createLoad = blockNextMetadataLoad("create");

		Future<Long> create = executor.submit(() -> db.cdcCreate(SUBSCRIPTION_ID, 0L, null, false));
		assertLoaded(createLoad);

		var deleteAttempted = observeNextOperation("delete");
		Future<?> delete = executor.submit(() -> db.cdcDelete(SUBSCRIPTION_ID));
		assertAttempted(deleteAttempted, "delete during create");
		assertBlocked(delete, "delete during create");

		createLoad.release().countDown();
		create.get(10, SECONDS);
		delete.get(10, SECONDS);

		assertEquals(OptionalLong.empty(), db.cdcGetLastCommittedSequence(SUBSCRIPTION_ID));
	}

	@Test
	void getterLinearizesAfterInFlightCommit() throws Exception {
		db.cdcCreate(SUBSCRIPTION_ID, 1L, null, false);
		var commitLoad = blockNextMetadataLoad("commit");

		Future<?> commit = executor.submit(() -> db.cdcCommit(SUBSCRIPTION_ID, 77L));
		assertLoaded(commitLoad);

		var getterAttempted = observeNextOperation("get");
		Future<OptionalLong> getter = executor.submit(() -> db.cdcGetLastCommittedSequence(SUBSCRIPTION_ID));
		assertAttempted(getterAttempted, "getter during commit");
		assertBlocked(getter, "getter during commit");

		commitLoad.release().countDown();
		commit.get(10, SECONDS);
		assertEquals(OptionalLong.of(77L), getter.get(10, SECONDS));
	}

	@Test
	void conditionalCreateRejectsCommitThatWinsTheMetadataLock() throws Exception {
		db.cdcCreate(SUBSCRIPTION_ID, 1L, null, false);
		db.cdcCommit(SUBSCRIPTION_ID, 10L);
		var commitLoad = blockNextMetadataLoad("commit");

		Future<?> commit = executor.submit(() -> db.cdcCommit(SUBSCRIPTION_ID, 42L));
		assertLoaded(commitLoad);

		var createAttempted = observeNextOperation("create");
		Future<Long> create = executor.submit(() -> db.cdcCreate(
				SUBSCRIPTION_ID,
				null,
				List.of(99L),
				true,
				OptionalLong.of(10L)));
		assertAttempted(createAttempted, "conditional create during commit");
		assertBlocked(create, "conditional create during commit");

		commitLoad.release().countDown();
		commit.get(10, SECONDS);
		var failure = assertThrows(ExecutionException.class, () -> create.get(10, SECONDS));
		var rocksError = assertInstanceOf(RocksDBException.class, failure.getCause());
		assertEquals(RocksDBException.RocksDBErrorType.CDC_SUBSCRIPTION_CHANGED,
				rocksError.getErrorUniqueId());
		assertEquals(OptionalLong.of(42L), db.cdcGetLastCommittedSequence(SUBSCRIPTION_ID),
				"a failed conditional create must not regress the winning checkpoint");
	}

	private BlockedLoad blockNextMetadataLoad(String operation) {
		var loaded = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var firstMatch = new AtomicBoolean(true);
		blockedLoadReleases.add(release);
		db.setCdcMetadataLoadedObserverForTesting((observedOperation, observedId) -> {
			if (operation.equals(observedOperation)
					&& SUBSCRIPTION_ID.equals(observedId)
					&& firstMatch.compareAndSet(true, false)) {
				loaded.countDown();
				awaitRelease(release);
			}
		});
		return new BlockedLoad(loaded, release);
	}

	private CountDownLatch observeNextOperation(String operation) {
		var attempted = new CountDownLatch(1);
		db.setCdcMetadataOperationObserverForTesting((observedOperation, observedId) -> {
			if (operation.equals(observedOperation) && SUBSCRIPTION_ID.equals(observedId)) {
				attempted.countDown();
			}
		});
		return attempted;
	}

	private static void assertLoaded(BlockedLoad blockedLoad) throws InterruptedException {
		assertTrue(blockedLoad.loaded().await(10, SECONDS), "metadata load was not reached");
	}

	private static void assertAttempted(CountDownLatch attempted, String operation) throws InterruptedException {
		assertTrue(attempted.await(10, SECONDS), operation + " was not attempted");
	}

	private static void assertBlocked(Future<?> future, String operation) {
		assertThrows(TimeoutException.class,
				() -> future.get(100, TimeUnit.MILLISECONDS),
				operation + " completed before the in-flight metadata operation released its lock");
	}

	private static void awaitRelease(CountDownLatch release) {
		try {
			if (!release.await(10, SECONDS)) {
				throw new AssertionError("timed out while holding the CDC metadata lock");
			}
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new AssertionError("interrupted while holding the CDC metadata lock", exception);
		}
	}

	private record BlockedLoad(CountDownLatch loaded, CountDownLatch release) {}
}
