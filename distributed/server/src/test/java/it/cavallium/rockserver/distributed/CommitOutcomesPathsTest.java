package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The paths of {@link CommitOutcomes} that {@code CommitOutcomesTest} leaves out: marking a second copy, settling
 * with each kind of outcome, the pruning of marks, every kind of record, a session that is gone, and barrier failures.
 */
class CommitOutcomesPathsTest {

	private final ExecutorService inline = Executors.newVirtualThreadPerTaskExecutor();
	private final AtomicLong term = new AtomicLong(5);
	private final AtomicReference<CompletableFuture<Void>> barrier =
			new AtomicReference<>(CompletableFuture.completedFuture(null));
	private final ConcurrentHashMap<ClientRequestId, ApplyResult> records = new ConcurrentHashMap<>();
	private final AtomicBoolean sessionExists = new AtomicBoolean(true);
	private final CommitOutcomes outcomes = new CommitOutcomes(() -> barrier.get(), term::get,
			id -> records.get(new ClientRequestId(id.sessionId(), id.sequence(), 0)), _ -> sessionExists.get(), inline);
	private final AppliedIndex applied = new AppliedIndex(0);

	@AfterEach
	void tearDown() {
		inline.shutdownNow();
	}

	private static RequestContext numbered(long sequence) {
		return RequestContext.batch(Duration.ofSeconds(10)).withRequestId(new ClientRequestId(7, sequence, 0));
	}

	private static Throwable failure(CompletableFuture<?> answer) {
		var error = assertThrows(ExecutionException.class, () -> answer.get(5, TimeUnit.SECONDS));
		return error.getCause();
	}

	private static RocksDBErrorType type(Throwable error) {
		return assertInstanceOf(RocksDBException.class, error).getErrorUniqueId();
	}

	@Test
	void aCommitWithoutARequestIdIsNotMarked() {
		assertNull(outcomes.markUnsettled(RequestContext.batch(Duration.ofSeconds(1)), new LeaderSession(5, applied)));
		outcomes.settle(null, null);
		assertEquals(0, outcomes.unsettledCommitsForTesting());
	}

	@Test
	void aSecondCopyMarkedInTheSameTermIsUnknownAndKeepsTheFirstMark() {
		var session = new LeaderSession(5, applied);
		var first = outcomes.markUnsettled(numbered(1), session);
		var error = assertThrows(RocksDBException.class, () -> outcomes.markUnsettled(numbered(1), session));
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, error.getErrorUniqueId());
		assertEquals(1, outcomes.unsettledCommitsForTesting());
		outcomes.settle(first, null);
		assertEquals(0, outcomes.unsettledCommitsForTesting());
		// A copy in a later term marks again: the earlier term's entries are all applied by then.
		outcomes.markUnsettled(numbered(2), new LeaderSession(4, applied));
		outcomes.markUnsettled(numbered(2), session);
		assertEquals(1, outcomes.unsettledCommitsForTesting());
	}

	@Test
	void anUnknownOutcomeKeepsTheMarkAndAnyOtherEndRemovesIt() {
		var session = new LeaderSession(5, applied);
		var unknown = outcomes.markUnsettled(numbered(1), session);
		outcomes.settle(unknown, new java.util.concurrent.CompletionException(
				RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN, "no answer")));
		assertEquals(1, outcomes.unsettledCommitsForTesting());
		var refused = outcomes.markUnsettled(numbered(2), session);
		outcomes.settle(refused, new RocksDBRetryException());
		var failed = outcomes.markUnsettled(numbered(3), session);
		outcomes.settle(failed, RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, "bad"));
		assertEquals(1, outcomes.unsettledCommitsForTesting());
		// The kept mark answers "unknown" to a copy, until a record exists.
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, type(failure(outcomes.settledCommit(new ClientRequestId(7, 1, 0)))));
	}

	/**
	 * Marks of earlier terms go at the first mark of a newer term; marks with a record go once the map reached the
	 * pruning size.
	 */
	@Test
	void pruningDropsMarksOfEarlierTermsAndMarksWithARecord() throws Exception {
		var old = new LeaderSession(4, applied);
		for (int i = 0; i < 1000; i++) {
			outcomes.markUnsettled(numbered(i), old);
		}
		assertEquals(1000, outcomes.unsettledCommitsForTesting());
		var current = new LeaderSession(5, applied);
		outcomes.markUnsettled(numbered(1000), current);
		assertEquals(1, outcomes.unsettledCommitsForTesting());
		for (int i = 1001; i < 2023; i++) {
			outcomes.markUnsettled(numbered(i), current);
		}
		records.put(new ClientRequestId(7, 1000, 0), new ApplyResult.Ok(true));
		// The 1024th mark starts the prune, on a virtual thread.
		outcomes.markUnsettled(numbered(2023), current);
		InProcessCluster.await(Duration.ofSeconds(10), () -> outcomes.unsettledCommitsForTesting() == 1023);
	}

	@Test
	void everyKindOfRecordAnswersACopy() throws Exception {
		records.put(new ClientRequestId(7, 1, 0), new ApplyResult.Ok(42L));
		records.put(new ClientRequestId(7, 2, 0), new ApplyResult.Failed(RocksDBErrorType.UPDATE_RETRY, "conflict"));
		records.put(new ClientRequestId(7, 3, 0), new ApplyResult.Failed(RocksDBErrorType.INVALID_REQUEST, "bad"));
		records.put(new ClientRequestId(7, 4, 0), ApplyResult.RESEND_ALONE);
		assertEquals(42L, outcomes.settledCommit(new ClientRequestId(7, 1, 0)).get(5, TimeUnit.SECONDS));
		assertInstanceOf(RocksDBRetryException.class, failure(outcomes.settledCommit(new ClientRequestId(7, 2, 0))));
		assertEquals(RocksDBErrorType.INVALID_REQUEST, type(failure(outcomes.settledCommit(new ClientRequestId(7, 3, 0)))));
		assertEquals(RocksDBErrorType.INTERNAL_ERROR, type(failure(outcomes.settledCommit(new ClientRequestId(7, 4, 0)))));
	}

	@Test
	void aCopyOfAGoneSessionIsExpiredNotRunAgain() {
		sessionExists.set(false);
		assertEquals(RocksDBErrorType.SESSION_EXPIRED, type(failure(outcomes.settledCommit(new ClientRequestId(7, 1, 0)))));
	}

	@Test
	void barrierFailuresBecomeRetryableAnswers() {
		barrier.set(CompletableFuture.failedFuture(new ReadBarrier.NotLeaderException("deposed")));
		assertInstanceOf(RocksDBRetryException.class, failure(outcomes.settledCommit(new ClientRequestId(7, 1, 0))));
		barrier.set(CompletableFuture.failedFuture(new TimeoutException()));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, type(failure(outcomes.settledCommit(new ClientRequestId(7, 1, 0)))));
		barrier.set(CompletableFuture.failedFuture(new ReadBarrier.ApplyLagException(10, 5)));
		var lag = failure(outcomes.settledCommit(new ClientRequestId(7, 1, 0)));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, type(lag));
		assertTrue(lag.getMessage().contains("applied 5 of 10"), lag.getMessage());
	}
}
