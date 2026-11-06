package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Delete;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Merge;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.UpdateContext;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.jetbrains.annotations.Nullable;

import static it.cavallium.rockserver.distributed.ReplicatedConnection.remainingContext;
import static it.cavallium.rockserver.distributed.ReplicatedConnection.remainingNanos;
import static it.cavallium.rockserver.distributed.ReplicatedConnection.unwrap;
import static it.cavallium.rockserver.distributed.LocalCommands.barrierFailure;

/**
 * Transactions and updates, which live on the leader ({@link LeaderSession}): opening them, staged writes, commits,
 * and chained updates, which read the value of a predecessor's commit that may not be applied yet.
 */
final class UpdateProtocol {

	private static final Logger LOG = LoggerFactory.getLogger(ReplicatedConnection.class);

	/**
	 * Default lease of {@code getForUpdate} updates, as on a single node ({@code EmbeddedDB}); the native update
	 * transaction expires on its own. An abandoned update keeps its key locked this long.
	 */
	private static final Duration UPDATE_LEASE = Duration.ofSeconds(10);
	/**
	 * How long an update waits for earlier updates of the same key before the client is told to retry: longer than
	 * the lease plus the expiry cadence, so that an update queued behind an abandoned one still gets the key.
	 */
	static final Duration UPDATE_LOCK_WAIT = Duration.ofSeconds(12);
	/**
	 * How long the close of a chained update waits for the commit it read from. The bound lives in the future:
	 * {@code requestSync} joins a close without a timeout. {@code GrpcServer} takes the asynchronous path, so that no
	 * worker waits this long; how many closes can wait for one commit is {@link LeaderSession#MAX_LINK_READERS}.
	 */
	private static final Duration CHAINED_CLOSE_WAIT = Duration.ofSeconds(2);
	/** Tests only: replaces {@link #CHAINED_CLOSE_WAIT}. */
	volatile Duration chainedCloseWaitForTesting = CHAINED_CLOSE_WAIT;

	private final NodeConfig config;
	private final RockserverStateMachine stateMachine;
	private final Proposer proposer;
	private final LocalCommands localCommands;
	private final CommitOutcomes outcomes;
	private final NodeMetrics.Series barrierTime;
	private final NodeMetrics.Series updateOpenTime;
	private final NodeMetrics.Series commitTime;
	private final NodeMetrics.Series lockWaitTime;
	private final NodeMetrics.Series chainedUpdates;
	private final NodeMetrics.Series chainConflicts;
	private final NodeMetrics.Series closeWaitTime;
	private final NodeMetrics.Series closeRetries;
	private final NodeMetrics.Series transactionConflicts;
	private final NodeMetrics.Series transactionChainConflicts;

	UpdateProtocol(NodeConfig config,
			RockserverStateMachine stateMachine,
			Proposer proposer,
			LocalCommands localCommands,
			CommitOutcomes outcomes,
			NodeMetrics metrics) {
		this.config = config;
		this.stateMachine = stateMachine;
		this.proposer = proposer;
		this.localCommands = localCommands;
		this.outcomes = outcomes;
		this.barrierTime = metrics.duration("read.barrier");
		this.updateOpenTime = metrics.duration("update.open");
		this.commitTime = metrics.duration("commit");
		this.lockWaitTime = metrics.duration("update.lockWait");
		this.chainedUpdates = metrics.amount("update.chained");
		this.chainConflicts = metrics.amount("update.chainConflict");
		this.closeWaitTime = metrics.duration("update.closeWait");
		this.closeRetries = metrics.amount("update.closeRetry");
		this.transactionConflicts = metrics.amount("transaction.conflict");
		this.transactionChainConflicts = metrics.amount("transaction.chainConflict");
	}

	private CompletableFuture<?> local(RequestContext context, RocksDBAPICommandSingle<?> command) {
		return localCommands.local(context, command);
	}

	private CompletableFuture<Void> leadership() {
		return localCommands.leadership();
	}

	private long currentTerm() {
		return localCommands.currentTerm();
	}

	private LeaderSession currentSession() {
		return localCommands.currentSession();
	}

	CompletableFuture<?> openTransaction(RequestContext context, OpenTransaction command) {
		long start = System.nanoTime();
		long term = currentTerm();
		// A transaction that opens after its caller's deadline is rolled back: nobody would learn its id, and it
		// would keep its snapshot and write history for the whole lease.
		BooleanSupplier callerGone = () -> remainingNanos(context, start) <= 0;
		// The lease is capped: an open transaction keeps the write history after its snapshot on the leader. The
		// native transaction gets the same lease, so that both expire together.
		var lease = transactionLease(command.transactionLeaseTtl(), config.maxTransactionLease());
		var open = lease.equals(command.transactionLeaseTtl()) ? command : new OpenTransaction(lease);
		return leadership().handle((_, error) -> {
			if (error != null) {
				throw new CompletionException(barrierFailure(unwrap(error)));
			}
			return openNative(remainingContext(context, start), open, term, false, lease,
					result -> (Long) result, null, null, null, callerGone);
		}).thenCompose(f -> f);
	}

	/** The lease a replicated transaction gets: the requested one, at most {@code max}. */
	static Duration transactionLease(Duration requested, Duration max) {
		return requested.compareTo(max) > 0 ? max : requested;
	}

	CompletableFuture<?> getForUpdate(RequestContext context, Get<?> command) {
		if (command.transactionOrUpdateId() != 0) {
			var transaction = requireLeaderTransaction(command.transactionOrUpdateId());
			transaction.reserveBytes(LogEntryCodec.CONFLICT_KEY_BYTES);
			return stageLocally(context, command, transaction, LogEntryCodec.CONFLICT_KEY_BYTES).thenApply(result -> {
				transaction.addConflict(command.columnId(), command.keys());
				return result;
			});
		}
		// Updates of one key queue up here instead of conflicting: each one reads what the previous one committed.
		var session = currentSession();
		long start = System.nanoTime();
		// Leadership is confirmed while the update waits for its key: any confirmation after the request arrived will
		// do, and the session's term is checked again when the update opens.
		long term = currentTerm();
		var leadership = leadership().whenComplete((_, _) -> barrierTime.since(start));
		long hash = ConflictTracker.keyHash(command.columnId(), command.keys());
		var waiter = new CompletableFuture<LeaderSession.KeyLock>();
		if (session.pipelined()) {
			// The key is asked for once leadership is confirmed: its next holder may be running one round trip behind.
			leadership.whenComplete((_, error) -> {
				if (error == null) {
					session.lockKey(hash, command.columnId(), waiter);
				} else {
					waiter.completeExceptionally(barrierFailure(error));
				}
			});
		} else {
			session.lockKey(hash, command.columnId(), waiter);
		}
		var result = new CompletableFuture<Object>();
		// The caller is gone once its deadline passed or it cancelled: an update it cannot learn the id of would keep
		// the key locked for the whole lease.
		BooleanSupplier callerGone = () -> result.isCancelled() || remainingNanos(context, start) <= 0;
		waiter.orTimeout(Math.min(UPDATE_LOCK_WAIT.toNanos(), remainingNanos(context, start)), TimeUnit.NANOSECONDS)
				.handleAsync((lock, lockError) -> {
					if (lockError != null) {
						var cause = unwrap(lockError);
						boolean gaveUp = cause instanceof TimeoutException || cause instanceof CancellationException;
						throw new CompletionException(gaveUp ? new RocksDBRetryException() : cause);
					}
					lockWaitTime.since(start);
					return leadership.handle((_, error) -> {
						if (error != null) {
							session.unlockKey(lock);
							throw new CompletionException(barrierFailure(unwrap(error)));
						}
						if (callerGone.getAsBoolean()) {
							session.unlockKey(lock);
							throw new CompletionException(new RocksDBRetryException());
						}
						var base = lock.base;
						// With another key, it is a hash collision: that update reads the database.
						if (base != null && base.columnId == command.columnId() && base.keys.equals(command.keys())) {
							return openChained(command, term, session, lock);
						}
						return openNative(remainingContext(context, start), command, term, true, UPDATE_LEASE,
								r -> ((UpdateContext<?>) r).updateId(), command, session, lock, callerGone);
					}).thenCompose(f -> f);
				})
				.thenCompose(f -> f)
				.whenComplete((value, error) -> {
					updateOpenTime.since(start);
					if (error != null) {
						result.completeExceptionally(unwrap(error));
					} else if (!result.complete(value)) {
						// The caller cancelled after the update registered: nobody learns its id, so close it here
						// instead of keeping the key locked for the whole lease.
						closeAbandonedUpdate(session, ((UpdateContext<?>) value).updateId());
					}
				});
		// A queued update whose caller cancelled leaves the queue.
		result.whenComplete((_, _) -> {
			if (result.isCancelled()) {
				waiter.cancel(false);
			}
		});
		return result;
	}

	/**
	 * Opens a native transaction or update on the local database and registers it in the current leader session.
	 * The snapshot index is reserved first, so that no history it needs can be pruned meanwhile. The session must
	 * belong to the term the read barrier confirmed: a later term of this same node may not have applied, nor
	 * tracked, the entries another leader committed in between. If {@code callerGone} turns true before the
	 * registration, the native transaction is rolled back instead, since nobody would ever close it.
	 */
	private CompletableFuture<?> openNative(RequestContext context,
			RocksDBAPICommandSingle<?> command,
			long confirmedTerm,
			boolean update,
			Duration lease,
			java.util.function.Function<Object, Long> idOf,
			@Nullable Get<?> forUpdate,
			@Nullable LeaderSession lockSession,
			@Nullable LeaderSession.KeyLock keyLock,
			@Nullable BooleanSupplier callerGone) {
		LeaderSession session;
		try {
			session = currentSession();
			if (session.term() != confirmedTerm || (lockSession != null && lockSession != session)) {
				throw new RocksDBRetryException();
			}
		} catch (RuntimeException ex) {
			if (keyLock != null) {
				lockSession.unlockKey(keyLock);
			}
			throw ex;
		}
		var reservation = session.reserve(stateMachine.appliedIndex());
		CompletableFuture<?> opened;
		try {
			opened = local(context, command);
		} catch (RuntimeException ex) {
			session.cancel(reservation);
			if (keyLock != null) {
				session.unlockKey(keyLock);
			}
			throw ex;
		}
		return opened.handle((result, openError) -> {
			if (openError != null) {
				session.cancel(reservation);
				if (keyLock != null) {
					session.unlockKey(keyLock);
				}
				throw new CompletionException(unwrap(openError));
			}
			long id = idOf.apply(result);
			if (callerGone != null && callerGone.getAsBoolean()) {
				session.cancel(reservation);
				if (keyLock != null) {
					session.unlockKey(keyLock);
				}
				rollBackNativeQuietly(id, update);
				throw new CompletionException(new RocksDBRetryException());
			}
			LeaderSession.OpenTransaction transaction;
			try {
				transaction = session.register(reservation, id, update, keyLock, lease.toNanos());
			} catch (IllegalStateException leadershipEnded) {
				if (keyLock != null) {
					session.unlockKey(keyLock);
				}
				rollBackNativeQuietly(id, update);
				throw new CompletionException(new RocksDBRetryException());
			}
			if (forUpdate != null) {
				transaction.addConflict(forUpdate.columnId(), forUpdate.keys());
			}
			return result;
		});
	}

	/**
	 * Opens an update that reads the value its predecessor's commit writes. That commit was admitted but may not be
	 * applied, so the value is provisional until this update ends well: nothing is opened on the database, its commit
	 * carries a guard, and its close waits for the predecessor. Leadership was confirmed before the key was granted.
	 */
	private CompletableFuture<?> openChained(Get<?> command, long confirmedTerm, LeaderSession session,
			LeaderSession.KeyLock lock) {
		LeaderSession.OpenTransaction transaction;
		try {
			if (currentSession() != session || session.term() != confirmedTerm) {
				throw new RocksDBRetryException();
			}
			transaction = session.register(session.reserve(stateMachine.appliedIndex()), session.nextChainedId(), true, lock,
					lock.base, UPDATE_LEASE.toNanos());
		} catch (RuntimeException ex) {
			session.unlockKey(lock);
			throw ex instanceof IllegalStateException ? new RocksDBRetryException() : ex;
		}
		chainedUpdates.record(1);
		// Its commit conflicts on what follows the predecessor.
		transaction.addConflict(command.columnId(), command.keys());
		return CompletableFuture.completedFuture(new UpdateContext<>(lock.base.value, transaction.id));
	}

	/** A write with a transaction id: staged in an open transaction, or the final write of an update. */
	CompletableFuture<?> stagedWrite(RequestContext context,
			RocksDBAPICommandSingle<?> command,
			long transactionId,
			LogEntry.Write write,
			List<Keys> keys) {
		var session = currentSession();
		var transaction = session.get(transactionId);
		if (transaction == null) {
			if (context.requestId() != null) {
				// A copy of the write that ended an update may have been applied already.
				return outcomes.settledCommit(context.requestId());
			}
			throw new RocksDBRetryException();
		}
		if (transaction.update) {
			// Marked before anything can refuse it, and until the update left the session: a copy of this write that
			// arrives meanwhile answers "unknown", never "not applied" nor a second commit.
			var marked = outcomes.markUnsettled(context, session);
			try {
				if (session.pipelined() && !transaction.startCommit()) {
					// A second write on this id: the first commit stays in flight, and the update with it.
					throw new RocksDBRetryException();
				}
				if (transaction.chained() && transaction.base.failed()) {
					// It read a value that was never written: its guard would reject it.
					session.remove(transaction);
					throw new RocksDBRetryException();
				}
			} catch (RuntimeException refused) {
				outcomes.settle(marked, null);
				throw refused;
			}
			var entry = transaction.updateEntry(session.term(), write, keys, session.pipelined());
			return commit(context, session, transaction, entry).handle((result, error) -> {
				session.remove(transaction);
				rollBackNativeQuietly(transaction.id, true);
				outcomes.settle(marked, error);
				// After remove: the commit can be admitted up to the moment its key is released. The link can only be this
				// commit's own, see startCommit.
				var link = transaction.link();
				if (link != null && error != null) {
					session.linkFailed(link);
				}
				if (error == null) {
					return result;
				}
				var cause = unwrap(error);
				if (cause instanceof Proposer.ConflictException && transaction.chained()) {
					chainConflicts.record(1);
				}
				// On a conflict the update is closed and must be retried, as on a single node.
				throw new CompletionException(cause instanceof Proposer.ConflictException
						? new RocksDBRetryException() : cause);
			});
		}
		if (context.requestId() != null) {
			// A client resends a numbered write after an unknown outcome: a staged write sent twice would be staged twice.
			throw RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"A write staged in a transaction cannot carry a client request id");
		}
		var staged = stagedOnly(write);
		// Refused before it is staged if the commit could not carry it, instead of failing the commit at the end.
		long bytes = LeaderSession.OpenTransaction.stagedBytes(staged, keys.size());
		transaction.reserveBytes(bytes);
		return stageLocally(context, command, transaction, bytes).thenApply(result -> {
			transaction.addWrite(staged, keys);
			return result;
		});
	}

	/** Runs a request staged in {@code transaction}, which reserved {@code bytes} for it, releasing them on failure. */
	private CompletableFuture<?> stageLocally(RequestContext context,
			RocksDBAPICommandSingle<?> command,
			LeaderSession.OpenTransaction transaction,
			long bytes) {
		CompletableFuture<?> result;
		try {
			result = local(context, command);
		} catch (RuntimeException ex) {
			transaction.releaseBytes(bytes);
			throw ex;
		}
		return result.whenComplete((_, error) -> {
			if (error != null) {
				transaction.releaseBytes(bytes);
			}
		});
	}

	CompletableFuture<?> closeTransaction(RequestContext context, CloseTransaction command) {
		var session = currentSession();
		var transaction = session.get(command.transactionId());
		if (transaction == null && command.commit() && context.requestId() != null) {
			// A copy of a commit that may have been applied already.
			return outcomes.settledCommit(context.requestId());
		}
		if (transaction == null) {
			if (command.commit() || command.transactionId() > 0) {
				// Expired, or opened by a previous leader: the client must run the transaction again. A positive id is a
				// chained update, and a clean end of one would confirm a value that may never be written.
				throw new RocksDBRetryException();
			}
			return CompletableFuture.completedFuture(true);
		}
		if (transaction.chained()) {
			throw refuseChained(session, transaction);
		}
		// Rollbacks run here, at once: an optimistic rollback does no I/O, and once the transaction left the session
		// nothing else would close it if a scheduler refused or expired the rollback.
		if (!command.commit()) {
			session.remove(transaction);
			rollBackNativeQuietly(transaction.id, false);
			return CompletableFuture.completedFuture(true);
		}
		// Marked until the transaction left the session (see stagedWrite).
		var marked = outcomes.markUnsettled(context, session);
		LogEntry.Guarded entry;
		try {
			entry = transaction.commitEntry(session.term());
		} catch (RuntimeException refused) {
			outcomes.settle(marked, null);
			throw refused;
		}
		if (entry.writes().isEmpty() && entry.conflictHashes().length == 0) {
			session.remove(transaction);
			rollBackNativeQuietly(transaction.id, false);
			outcomes.settle(marked, null);
			return CompletableFuture.completedFuture(true);
		}
		return commit(context, session, transaction, entry).handle((_, error) -> {
			if (error != null && unwrap(error) instanceof Proposer.ConflictException) {
				// Same contract as a single node: false, and the transaction stays open.
				transactionConflicts.record(1);
				if (session.yieldChainsTo(entry)) {
					transactionChainConflicts.record(1);
				}
				outcomes.settle(marked, null);
				return false;
			}
			session.remove(transaction);
			rollBackNativeQuietly(transaction.id, false);
			outcomes.settle(marked, error);
			if (error != null) {
				throw new CompletionException(unwrap(error));
			}
			return true;
		});
	}

	/**
	 * Proposes a commit entry, keeping its snapshot's write history pinned until the leader validated it, whatever
	 * happens to the transaction meanwhile. A commit still queued at its caller's deadline is not sent.
	 */
	CompletableFuture<Object> commit(RequestContext context,
			LeaderSession session,
			LeaderSession.OpenTransaction transaction,
			LogEntry.Guarded entry) {
		var pin = session.pin(transaction.snapshotIndex);
		long start = System.nanoTime();
		transaction.commitProposed();
		CompletableFuture<Object> proposed;
		try {
			proposed = proposer.proposeOnce(entry, remainingNanos(context, start), context.requestId());
		} catch (RuntimeException ex) {
			transaction.commitEnded();
			session.unpin(pin);
			throw ex;
		}
		return proposed.whenComplete((_, error) -> {
			commitTime.since(start);
			session.unpin(pin);
			transaction.commitEnded();
		});
	}

	CompletableFuture<?> closeFailedUpdate(RequestContext context, CloseFailedUpdate command) {
		long start = System.nanoTime();
		var session = currentSession();
		long id = command.updateId();
		var transaction = session.get(id);
		if (id > 0) {
			return closeChained(context, start, session, transaction);
		}
		if (transaction != null) {
			session.remove(transaction);
		}
		// At once, like the rollbacks of closeTransaction.
		rollBackNativeQuietly(id, true);
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * The close of a chained update, which wrote nothing, so no log entry is needed: it hands the key on at once, then
	 * tells whether what the update read was real. Its read counts only if the commit it read from applied: the read
	 * then linearizes right after that commit (see {@link LeaderSession#admitted}, and the leadership confirmed before
	 * the grant). Otherwise, or if that is not known in time, or the update is unknown here (expired, or opened by a
	 * previous leader), the client must run the update again. The answer goes on a pool: {@code settled} completes on
	 * the thread that completes every applied entry.
	 */
	private CompletableFuture<?> closeChained(RequestContext context, long start, LeaderSession session,
			LeaderSession.@Nullable OpenTransaction transaction) {
		if (transaction == null) {
			return CompletableFuture.failedFuture(new RocksDBRetryException());
		}
		session.remove(transaction);
		long wait = Math.min(chainedCloseWaitForTesting.toNanos(), remainingNanos(context, start));
		// A copy: the timeout would otherwise complete the future every waiter shares.
		return transaction.base.settled.copy().orTimeout(wait, TimeUnit.NANOSECONDS).handleAsync((ok, error) -> {
			closeWaitTime.since(start);
			if (error == null && ok) {
				return null;
			}
			closeRetries.record(1);
			throw new CompletionException(new RocksDBRetryException());
		});
	}

	CompletableFuture<?> inTransaction(RequestContext context, RocksDBAPICommandSingle<?> command, long id) {
		requireLeaderTransaction(id);
		return local(context, command);
	}

	LeaderSession.OpenTransaction requireLeaderTransaction(long id) {
		var session = currentSession();
		var transaction = session.get(id);
		if (transaction == null) {
			// Unknown here: expired, or opened by a previous leader. Either way the client must start over.
			throw new RocksDBRetryException();
		}
		if (transaction.chained()) {
			throw refuseChained(session, transaction);
		}
		return transaction;
	}

	/**
	 * A native operation on a chained update: there is no snapshot to serve it from, and reading the database would
	 * hide a value the update already read. The update ends, and the next one of the key is a head.
	 */
	private static RocksDBRetryException refuseChained(LeaderSession session, LeaderSession.OpenTransaction transaction) {
		session.breakChain(transaction.keyLock.key);
		session.remove(transaction);
		return new RocksDBRetryException();
	}

	/** Closes an update that registered in {@code session} but whose caller is gone: releases its key and rolls it back. */
	private void closeAbandonedUpdate(LeaderSession session, long updateId) {
		var transaction = session.get(updateId);
		if (transaction != null) {
			session.remove(transaction);
		}
		rollBackNativeQuietly(updateId, true);
	}

	private void rollBackNativeQuietly(long id, boolean update) {
		if (id > 0) {
			// A chained update has no native transaction.
			return;
		}
		try {
			var db = stateMachine.store().db();
			if (update) {
				db.closeFailedUpdate(id);
			} else {
				db.closeTransaction(id, false);
			}
		} catch (RocksDBException ex) {
			if (ex.getErrorUniqueId() != RocksDBErrorType.TX_NOT_FOUND
					&& ex.getErrorUniqueId() != RocksDBErrorType.TRANSACTION_NOT_FOUND) {
				LOG.warn("Failed to roll back native {} {}", update ? "update" : "transaction", id, ex);
			}
			// Otherwise already expired or closed.
		} catch (RuntimeException ex) {
			// Such as a database that is closing. Nothing else would close it: say so.
			LOG.warn("Failed to roll back native {} {}", update ? "update" : "transaction", id, ex);
		}
	}

	/** The write as recorded for the commit entry: staged writes return nothing at commit time. */
	private static LogEntry.Write stagedOnly(LogEntry.Write write) {
		return switch (write) {
			case LogEntry.Put put -> new LogEntry.Put(put.columnId(), put.keys(), put.value(), RequestTypeId.NOTHING);
			case LogEntry.PutMulti put ->
					new LogEntry.PutMulti(put.columnId(), put.keys(), put.values(), RequestTypeId.NOTHING);
			case LogEntry.Delete delete -> new LogEntry.Delete(delete.columnId(), delete.keys(), RequestTypeId.NOTHING);
			case LogEntry.DeleteMulti delete ->
					new LogEntry.DeleteMulti(delete.columnId(), delete.keys(), RequestTypeId.NOTHING);
			case LogEntry.Merge merge -> new LogEntry.Merge(merge.columnId(), merge.keys(), merge.value(), RequestTypeId.NOTHING);
			case LogEntry.MergeMulti merge ->
					new LogEntry.MergeMulti(merge.columnId(), merge.keys(), merge.values(), RequestTypeId.NOTHING);
			case LogEntry.DeleteRange _ -> throw new IllegalArgumentException("deleteRange cannot be transactional");
		};
	}
}
