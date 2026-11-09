package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * R48-3: {@link RocksDBAsyncAPI#ensureMergeOperatorAsync} makes two calls, a check and, if the operator is missing, an
 * upload. Its cancel reaches the call in flight, as a single call's does: before the upload is made it stops the check
 * and nothing is uploaded; after, it is the upload's own cancel, so an upload that refuses it (a numbered write that sent
 * a copy, SessionWrites' OUTCOME_UNKNOWN) makes the cancel return false, and its outcome comes through. A derived
 * {@code thenCompose} future answered true and ended cancelled, which reads "not applied", while the upload went on.
 */
class EnsureMergeOperatorCancelTest {

	private static final byte[] JAR = {1, 2, 3};

	/** The two calls, as a client makes them: each returns the future the test gives it. */
	private static class TwoCalls implements RocksDBAsyncAPI {
		final CompletableFuture<Long> check;
		final CompletableFuture<Long> upload;
		final AtomicInteger uploads = new AtomicInteger();

		TwoCalls(CompletableFuture<Long> check, CompletableFuture<Long> upload) {
			this.check = check;
			this.upload = upload;
		}

		@Override
		public CompletableFuture<Long> checkMergeOperatorAsync(String name, byte[] hash) {
			return check;
		}

		@Override
		public CompletableFuture<Long> uploadMergeOperatorAsync(String name, String className, byte[] jarData) {
			uploads.incrementAndGet();
			return upload;
		}
	}

	/** A numbered write whose copy was sent: it refuses a cancel and ends OUTCOME_UNKNOWN (SessionWrites' CallResult). */
	private static final class SentNumberedWrite extends CompletableFuture<Long> {
		final AtomicInteger cancels = new AtomicInteger();

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			cancels.incrementAndGet();
			completeExceptionally(RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
					"Cancelled after a copy was sent: the write may have been applied"));
			return false;
		}
	}

	@Test
	void cancelAfterTheUploadWasSentEndsAsTheUpload() throws Exception {
		var upload = new SentNumberedWrite();
		var calls = new TwoCalls(CompletableFuture.completedFuture(null), upload);
		var ensure = calls.ensureMergeOperatorAsync("op", "Op", JAR);
		assertEquals(1, calls.uploads.get());

		assertFalse(ensure.cancel(true), "the upload refused the cancel");

		assertEquals(1, upload.cancels.get(), "the cancel reached the upload");
		assertFalse(ensure.isCancelled());
		var failure = assertThrows(ExecutionException.class, () -> ensure.get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN,
				assertInstanceOf(RocksDBException.class, failure.getCause()).getErrorUniqueId());
	}

	@Test
	void cancelReachesTheUpload() {
		var upload = new CompletableFuture<Long>();
		var calls = new TwoCalls(CompletableFuture.completedFuture(null), upload);
		var ensure = calls.ensureMergeOperatorAsync("op", "Op", JAR);

		assertTrue(ensure.cancel(true));

		assertTrue(upload.isCancelled(), "the upload is cancelled");
		assertTrue(ensure.isCancelled());
	}

	@Test
	void cancelDuringTheCheckStopsItAndUploadsNothing() {
		var check = new CompletableFuture<Long>();
		var calls = new TwoCalls(check, new CompletableFuture<>());
		var ensure = calls.ensureMergeOperatorAsync("op", "Op", JAR);

		assertTrue(ensure.cancel(true));

		assertTrue(check.isCancelled(), "the check is cancelled");
		assertTrue(ensure.isCancelled());
		assertEquals(0, calls.uploads.get(), "nothing uploaded");
	}

	/** The check answered "missing" but the upload is not made yet: nothing more is sent, and the cancel is granted. */
	@Test
	void cancelBetweenTheCheckAndTheUploadUploadsNothing() {
		var answered = new AtomicReference<Runnable>();
		var check = new CompletableFuture<Long>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				// It answered a moment ago, and its answer is still on its way: a call that answered refuses a cancel.
				answered.set(() -> complete(null));
				return false;
			}
		};
		var calls = new TwoCalls(check, new CompletableFuture<>());
		var ensure = calls.ensureMergeOperatorAsync("op", "Op", JAR);

		assertTrue(ensure.cancel(true));
		answered.get().run();

		assertTrue(ensure.isCancelled());
		assertEquals(0, calls.uploads.get(), "nothing uploaded");
		assertThrows(CancellationException.class, ensure::join);
	}

	/** A cancel from a stage that runs while the upload is being made reaches the upload once made. */
	@Test
	void cancelWhileTheUploadIsBeingMadeReachesIt() throws Exception {
		var upload = new SentNumberedWrite();
		var check = new CompletableFuture<Long>();
		var ensureRef = new AtomicReference<CompletableFuture<Long>>();
		var nested = new AtomicReference<Boolean>();
		var calls = new TwoCalls(check, upload) {
			@Override
			public CompletableFuture<Long> uploadMergeOperatorAsync(String name, String className, byte[] jarData) {
				var made = super.uploadMergeOperatorAsync(name, className, jarData);
				// The copy is out: a cancel now must not read "not applied".
				nested.set(ensureRef.get().cancel(true));
				return made;
			}
		};
		ensureRef.set(calls.ensureMergeOperatorAsync("op", "Op", JAR));
		check.complete(null);

		assertEquals(Boolean.FALSE, nested.get(), "the upload is being made: the cancel cannot be granted yet");
		assertEquals(1, upload.cancels.get(), "the cancel reached the upload once made");
		var failure = assertThrows(ExecutionException.class, () -> ensureRef.get().get(5, TimeUnit.SECONDS));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN,
				assertInstanceOf(RocksDBException.class, failure.getCause()).getErrorUniqueId());
	}

	@Test
	void existingOperatorIsTheAnswerAndACancelAfterItReturnsFalse() throws Exception {
		var calls = new TwoCalls(CompletableFuture.completedFuture(7L), new CompletableFuture<>());
		var ensure = calls.ensureMergeOperatorAsync("op", "Op", JAR);

		assertFalse(ensure.cancel(true));
		assertEquals(7L, ensure.get(5, TimeUnit.SECONDS));
		assertEquals(0, calls.uploads.get());
	}

	@Test
	void failuresComeThrough() {
		var check = new CompletableFuture<Long>();
		var ensure = new TwoCalls(check, new CompletableFuture<>()).ensureMergeOperatorAsync("op", "Op", JAR);
		var lost = RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN, "lost");
		check.completeExceptionally(lost);
		var failure = assertThrows(ExecutionException.class, () -> ensure.get(5, TimeUnit.SECONDS));
		assertEquals(lost, failure.getCause());
	}
}
