package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

import static it.cavallium.rockserver.distributed.ReplicatedConnection.unwrap;
import static it.cavallium.rockserver.distributed.LocalCommands.barrierFailure;

/**
 * The outcome of the commits of client session writes on this leader: which are unsettled in the current term
 * ({@link #markUnsettled}, {@link #settle}), and the answer to a copy of one whose transaction or update is gone
 * ({@link #settledCommit}).
 */
final class CommitOutcomes {

	/** A write of a client session, without its {@code completedBelow}, which differs between copies. */
	private record SessionWrite(long sessionId, long sequence) {
		static @Nullable SessionWrite of(RequestContext context) {
			var id = context.requestId();
			return id == null ? null : new SessionWrite(id.sessionId(), id.sequence());
		}
	}

	/**
	 * Commits of client session writes that this node proposed as the leader of the term they map to, and whose
	 * outcome is not settled: still in flight, or ended without an answer. Their entry may still be applied in that
	 * term, so a copy of the same write that finds no record must not be told that it was not applied. Once the term
	 * is over, every entry of it this node's log holds is applied before the next term serves (see the read barrier),
	 * so its markers say nothing more.
	 */
	private final java.util.concurrent.ConcurrentHashMap<SessionWrite, Long> unsettledCommits =
			new java.util.concurrent.ConcurrentHashMap<>();
	/** Size of {@link #unsettledCommits} at which {@link #pruneUnsettled} runs next. */
	private final AtomicLong pruneUnsettledAt = new AtomicLong(MIN_PRUNE_UNSETTLED);
	private static final long MIN_PRUNE_UNSETTLED = 1024;
	/** The newest term a commit was marked in: the first mark of a newer term drops every older mark. */
	private final AtomicLong markedTerm = new AtomicLong();

	private final java.util.function.Supplier<CompletableFuture<Void>> leadership;
	private final java.util.function.LongSupplier currentTerm;
	private final java.util.function.Function<it.cavallium.rockserver.core.common.ClientRequestId, ApplyResult> recorded;
	private final java.util.function.LongPredicate sessionExists;
	private final java.util.concurrent.ExecutorService inline;

	CommitOutcomes(RockserverStateMachine stateMachine, LocalCommands localCommands,
			java.util.concurrent.ExecutorService inline) {
		this(localCommands::leadership, localCommands::currentTerm, stateMachine::recordedResult,
				sessionId -> sessionLive(stateMachine.store(), sessionId), inline);
	}

	/**
	 * True if client session {@code sessionId} exists and is not expired at the replicated clock, as the applier sees
	 * it: a session expired at the clock is gone even before the sweep deletes its row. The row is read before the
	 * clock and both only move forward, so the answer can only err toward "expired" (a write of the session applied in
	 * between), never toward "live": either way the copy asking was not applied, and the client is told so.
	 */
	static boolean sessionLive(NodeStore store, long sessionId) {
		var db = store.db();
		Buf row = db.get(0L, store.metaColumnId(), NodeStore.sessionKey(sessionId), RequestType.<Buf>current());
		if (row == null) {
			return false;
		}
		var fields = NodeStore.decodeSession(row);
		long clock = NodeStore.decodeClock(db.get(0L, store.metaColumnId(), NodeStore.key(NodeStore.CLOCK),
				RequestType.<Buf>current()));
		return !NodeStore.sessionExpired(fields[0], fields[1], clock);
	}

	/**
	 * Over its sources: the leadership barrier, the current term, the record of a session write (null if none), and
	 * whether a session exists. Tests give fakes.
	 */
	CommitOutcomes(java.util.function.Supplier<CompletableFuture<Void>> leadership,
			java.util.function.LongSupplier currentTerm,
			java.util.function.Function<it.cavallium.rockserver.core.common.ClientRequestId, ApplyResult> recorded,
			java.util.function.LongPredicate sessionExists, java.util.concurrent.ExecutorService inline) {
		this.leadership = leadership;
		this.currentTerm = currentTerm;
		this.recorded = recorded;
		this.sessionExists = sessionExists;
		this.inline = inline;
	}

	private CompletableFuture<Void> leadership() {
		return leadership.get();
	}

	private long currentTerm() {
		return currentTerm.getAsLong();
	}

	/** A commit of a client session write, marked unsettled in a term. */
	record Marked(SessionWrite write, long term) {}

	/**
	 * Marks the commit of the client session write of {@code context} unsettled in {@code session}'s term, until
	 * {@link #settle}: null for a commit without a request id. Throws {@code OUTCOME_UNKNOWN} if a copy of it is
	 * unsettled in this term already, leaving that copy's transaction as it is.
	 */
	@Nullable Marked markUnsettled(RequestContext context, LeaderSession session) {
		var write = SessionWrite.of(context);
		if (write == null) {
			return null;
		}
		long term = session.term();
		if (markedTerm.getAndAccumulate(term, Math::max) < term) {
			// A new term: every entry of earlier terms is applied before it serves, so their marks say nothing more.
			unsettledCommits.values().removeIf(markTerm -> markTerm < term);
		}
		var previous = unsettledCommits.put(write, term);
		if (previous != null && previous == term) {
			throw earlierCopyInFlight();
		}
		if (unsettledCommits.size() >= pruneUnsettledAt.get()) {
			pruneUnsettled(term);
		}
		return new Marked(write, term);
	}

	/**
	 * Ends the mark of a commit once its transaction or update left the session: unless its outcome is unknown, in
	 * which case its entry may still be applied in this term, and the mark stays until its record exists
	 * ({@link #settledCommit}, {@link #pruneUnsettled}) or the term ends.
	 */
	void settle(@Nullable Marked marked, @Nullable Throwable error) {
		if (marked != null && (error == null || !isOutcomeUnknown(unwrap(error)))) {
			unsettledCommits.remove(marked.write(), marked.term());
		}
	}

	/**
	 * Drops the marks of earlier terms and those whose record exists, on a virtual thread (it reads the database).
	 * Runs once the map doubled since the last run, so a commit does not scan it each time.
	 */
	private void pruneUnsettled(long term) {
		long size = unsettledCommits.size();
		long at = pruneUnsettledAt.get();
		if (size < at || !pruneUnsettledAt.compareAndSet(at, Long.MAX_VALUE)) {
			return;
		}
		try {
			inline.execute(() -> {
				try {
					unsettledCommits.entrySet().removeIf(mark -> mark.getValue() < term
							|| recorded.apply(new it.cavallium.rockserver.core.common.ClientRequestId(
									mark.getKey().sessionId(), mark.getKey().sequence(), 0)) != null);
				} finally {
					pruneUnsettledAt.set(Math.max(MIN_PRUNE_UNSETTLED, 2L * unsettledCommits.size()));
				}
			});
		} catch (java.util.concurrent.RejectedExecutionException stopping) {
			pruneUnsettledAt.set(at);
		}
	}

	/** Test hook: the commits of session writes marked unsettled. */
	int unsettledCommitsForTesting() {
		return unsettledCommits.size();
	}

	private static boolean isOutcomeUnknown(Throwable error) {
		return error instanceof RocksDBException rocks && rocks.getErrorUniqueId() == RocksDBErrorType.OUTCOME_UNKNOWN;
	}

	private static RocksDBException earlierCopyInFlight() {
		return RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN,
				"An earlier copy of this commit is in flight or unsettled; its outcome is unknown yet");
	}

	/**
	 * The answer to a copy of a commit (or of the write that ends an update) of a client session whose transaction or
	 * update is not on this leader: the transaction ended, or another leader had it. After the read barrier every
	 * entry of earlier leaders is applied, so the record of an earlier copy that took effect is there, and is the
	 * answer. Without a record, and with no copy unsettled in this term, the commit was never applied: the client
	 * runs the transaction again ({@code UPDATE_RETRY}), as without sessions. A session that is gone, or expired at the
	 * replicated clock, which the applier treats as gone, has no records any more: {@code SESSION_EXPIRED}.
	 */
	CompletableFuture<?> settledCommit(it.cavallium.rockserver.core.common.ClientRequestId id) {
		long term = currentTerm();
		var write = new SessionWrite(id.sessionId(), id.sequence());
		return leadership().handle((_, error) -> {
			if (error != null) {
				throw new CompletionException(barrierFailure(error));
			}
			// The mark first: a commit removes it only after its record is in the database, so no mark and then no record
			// proves that no copy applied in this term.
			Long unsettledTerm = unsettledCommits.get(write);
			var recorded = this.recorded.apply(id);
			if (recorded != null && unsettledTerm != null) {
				unsettledCommits.remove(write, unsettledTerm);
			}
			if (recorded != null) {
				return switch (recorded) {
					case ApplyResult.Ok ok -> ok.value();
					case ApplyResult.Failed failed when failed.type() == RocksDBErrorType.UPDATE_RETRY ->
							throw new CompletionException(new RocksDBRetryException());
					case ApplyResult.Failed failed -> throw new CompletionException(failed.toException());
					case ApplyResult.ResendAlone _ -> throw new CompletionException(
							RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "Invalid record"));
				};
			}
			// A mark of this term, or of a later one (this node lost and won leadership again while the barrier ran, and
			// a copy was marked in the new term): its entry may still be applied. Earlier terms' entries are all applied.
			if (unsettledTerm != null && unsettledTerm >= term) {
				throw new CompletionException(earlierCopyInFlight());
			}
			if (!sessionExists.test(id.sessionId())) {
				// This copy is refused before it runs: flagged not applied. An earlier copy may have applied, which only
				// the client knows (it sent it): it ends the write unknown then.
				var gone = (ApplyResult.Failed) Applier.SESSION_GONE;
				throw new CompletionException(RocksDBException.notApplied(gone.type(), gone.message()));
			}
			throw new CompletionException(new RocksDBRetryException());
		});
	}
}
