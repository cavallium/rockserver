package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The answer to a copy of a numbered commit whose transaction is gone ({@link CommitOutcomes#settledCommit}), against
 * the in-flight marks, across term changes while the leadership barrier runs. A copy must never be told "not applied"
 * ({@code UPDATE_RETRY}) while an earlier copy may still apply: the client would run the transaction a second time.
 */
class CommitOutcomesTest {

	private final ExecutorService inline = Executors.newVirtualThreadPerTaskExecutor();
	private final AtomicLong term = new AtomicLong(5);
	private final AtomicReference<CompletableFuture<Void>> barrier =
			new AtomicReference<>(CompletableFuture.completedFuture(null));
	private final ConcurrentHashMap<ClientRequestId, ApplyResult> records = new ConcurrentHashMap<>();
	private final CommitOutcomes outcomes = new CommitOutcomes(() -> barrier.get(), term::get,
			id -> records.get(new ClientRequestId(id.sessionId(), id.sequence(), 0)), _ -> true, inline);
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

	private static void assertUnknown(CompletableFuture<?> answer) {
		var cause = assertInstanceOf(RocksDBException.class, failure(answer));
		assertFalse(cause instanceof RocksDBRetryException, "must not answer \"not applied\"");
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, cause.getErrorUniqueId(), cause.getMessage());
	}

	/** A copy unsettled in this term: unknown. Without a mark and a record: never applied, run it again. */
	@Test
	void aMarkOfThisTermAnswersUnknownAndNoMarkAnswersRetry() {
		var marked = outcomes.markUnsettled(numbered(1), new LeaderSession(5, applied));
		assertUnknown(outcomes.settledCommit(new ClientRequestId(7, 1, 0)));
		outcomes.settle(marked, null);
		assertInstanceOf(RocksDBRetryException.class, failure(outcomes.settledCommit(new ClientRequestId(7, 1, 0))));
	}

	/**
	 * The node lost leadership and won it again while the barrier of a copy ran: a copy marked in the new term is in
	 * flight. The term read before the barrier is the old one, so the mark is newer than it: still unknown.
	 */
	@Test
	void aMarkFromATermThatStartedDuringTheBarrierAnswersUnknown() {
		var pending = new CompletableFuture<Void>();
		barrier.set(pending);
		var answer = outcomes.settledCommit(new ClientRequestId(7, 2, 0));
		term.set(8);
		outcomes.markUnsettled(numbered(2), new LeaderSession(8, applied));
		pending.complete(null);
		assertUnknown(answer);
	}

	/** A mark of an earlier term says nothing: every entry of that term was applied before this term served. */
	@Test
	void aMarkOfAnEarlierTermIsIgnored() {
		outcomes.markUnsettled(numbered(3), new LeaderSession(4, applied));
		assertInstanceOf(RocksDBRetryException.class, failure(outcomes.settledCommit(new ClientRequestId(7, 3, 0))));
	}

	/** A record answers, whatever the marks, and ends the mark. */
	@Test
	void aRecordAnswersAndEndsTheMark() throws Exception {
		outcomes.markUnsettled(numbered(4), new LeaderSession(5, applied));
		records.put(new ClientRequestId(7, 4, 0), new ApplyResult.Ok(true));
		assertEquals(true, outcomes.settledCommit(new ClientRequestId(7, 4, 0)).get(5, TimeUnit.SECONDS));
		assertEquals(0, outcomes.unsettledCommitsForTesting());
	}

	/**
	 * A mark left by an unknown outcome stays until its term ends: the first mark of a newer term drops the older ones,
	 * without waiting for the map to grow (F15).
	 */
	@Test
	void theFirstMarkOfANewTermDropsTheMarksOfEarlierTerms() {
		var unknown = outcomes.markUnsettled(numbered(10), new LeaderSession(5, applied));
		outcomes.settle(unknown, RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN, "lost"));
		var other = outcomes.markUnsettled(numbered(11), new LeaderSession(5, applied));
		outcomes.settle(other, RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN, "lost"));
		assertEquals(2, outcomes.unsettledCommitsForTesting(), "kept in their term");
		term.set(6);
		outcomes.markUnsettled(numbered(12), new LeaderSession(6, applied));
		assertEquals(1, outcomes.unsettledCommitsForTesting(), "only the new term's mark");
		assertInstanceOf(RocksDBRetryException.class, failure(outcomes.settledCommit(new ClientRequestId(7, 10, 0))));
	}
}
