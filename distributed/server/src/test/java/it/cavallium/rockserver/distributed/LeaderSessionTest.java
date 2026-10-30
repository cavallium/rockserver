package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/** Key locks of single-key updates, the snapshot bookkeeping and the commit size of the leader session. */
class LeaderSessionTest {

	private static final long KEY = 1234;
	private static final long LEASE_NANOS = TimeUnit.MINUTES.toNanos(1);

	private final AppliedIndex applied = new AppliedIndex(100);
	private final LeaderSession session = new LeaderSession(3, applied);

	@Test
	void updatesOfOneKeyRunOneAfterTheOtherInArrivalOrder() {
		var first = session.lockKey(KEY);
		var second = session.lockKey(KEY);
		var third = session.lockKey(KEY);
		var otherKey = session.lockKey(KEY + 1);
		assertTrue(first.isDone());
		assertTrue(otherKey.isDone(), "another key is not held");
		assertFalse(second.isDone());

		session.unlockKey(first.join());
		assertTrue(second.isDone());
		assertFalse(third.isDone());
		// Releasing twice does not hand the key to a second waiter.
		session.unlockKey(first.join());
		assertFalse(third.isDone());

		session.unlockKey(second.join());
		assertTrue(third.isDone());
		session.unlockKey(third.join());
		assertTrue(session.lockKey(KEY).isDone(), "free again once every update finished");
	}

	@Test
	void aWaiterThatGaveUpIsSkipped() {
		var holder = session.lockKey(KEY);
		var gaveUp = session.lockKey(KEY);
		var next = session.lockKey(KEY);
		gaveUp.completeExceptionally(new TimeoutException());
		session.unlockKey(holder.join());
		assertTrue(next.isDone());
		assertFalse(next.isCompletedExceptionally());
	}

	@Test
	void removingAnUpdateReleasesItsKey() {
		var lock = session.lockKey(KEY).join();
		var waiter = session.lockKey(KEY);
		var update = session.register(session.reserve(applied), 77, true, lock, LEASE_NANOS);
		assertFalse(waiter.isDone());
		session.remove(update);
		assertTrue(waiter.isDone());
	}

	@Test
	void anExpiredUpdateReleasesItsKey() throws Exception {
		var lock = session.lockKey(KEY).join();
		var waiter = session.lockKey(KEY);
		session.register(session.reserve(applied), 78, true, lock, 1);
		Thread.sleep(5);
		session.expire();
		assertTrue(waiter.isDone());
	}

	@Test
	void aQueuedUpdateGetsTheKeyWhenTheAbandonedOneExpiresWithoutOtherTraffic() throws Exception {
		var lock = session.lockKey(KEY).join();
		session.register(session.reserve(applied), 79, true, lock, TimeUnit.MILLISECONDS.toNanos(200));
		var waiter = session.lockKey(KEY);
		// Nothing calls expire(): the session must do it on its own once the lease ends.
		waiter.get(2, TimeUnit.SECONDS);
		session.unlockKey(waiter.join());
		assertEquals(0, session.lockedKeys());
	}

	@Test
	void endOfLeadershipFailsTheWaitersWithARetry() {
		session.lockKey(KEY).join();
		var waiter = session.lockKey(KEY);
		session.close(null);
		var error = assertThrows(CompletionException.class, waiter::join);
		assertInstanceOf(RocksDBRetryException.class, error.getCause());
		assertTrue(session.lockKey(KEY + 5).isCompletedExceptionally(), "no lock after the end of leadership");
	}

	@Test
	void anOpenTransactionKeepsTheHistoryItsCommitNeeds() {
		// A transaction opens at snapshot 100; then key k is written at 101 and the apply goes on.
		var transaction = session.register(session.reserve(applied), 9, false, null, LEASE_NANOS);
		var token = session.tracker().register(put(key("k")));
		session.tracker().resolve(token, 101);
		applied.advance(500);
		session.expire();
		var unrelated = session.register(session.reserve(applied), 10, false, null, LEASE_NANOS);
		session.remove(unrelated);
		// Other transactions came and went: the write at 101 must still be known, so the commit conflicts.
		assertTrue(session.tracker().conflicts(commit(transaction.snapshotIndex, key("k"))));
		session.remove(transaction);
		session.expire();
		assertEquals(0, session.tracker().size(), "pruned once no transaction needs it");
	}

	@Test
	void aPinnedCommitKeepsItsHistoryAfterItsTransactionIsGone() {
		var transaction = session.register(session.reserve(applied), 11, false, null, LEASE_NANOS);
		var pin = session.pin(transaction.snapshotIndex);
		session.remove(transaction);
		var token = session.tracker().register(put(key("k")));
		session.tracker().resolve(token, 101);
		applied.advance(500);
		session.expire();
		assertTrue(session.tracker().conflicts(commit(100, key("k"))));
		session.unpin(pin);
		assertEquals(0, session.tracker().size());
	}

	@Test
	void aLongTransactionDoesNotKeepMoreHistoryThanTheCap() {
		var capped = new LeaderSession(3, applied, new ConflictTracker(4));
		var transaction = capped.register(capped.reserve(applied), 12, false, null, LEASE_NANOS);
		for (int i = 0; i < 10; i++) {
			var token = capped.tracker().register(put(key("k" + i)));
			applied.advance(101 + i);
			capped.tracker().resolve(token, 101 + i);
			capped.afterApply();
		}
		assertEquals(4, capped.tracker().resolvedKeys(), "the open transaction does not pin more than the cap");
		// Its commit cannot be validated any more: it conflicts, even on a key nobody wrote.
		assertTrue(capped.tracker().conflicts(commit(transaction.snapshotIndex, key("unrelated"))));
		// A transaction opened now is validated as usual.
		var recent = capped.register(capped.reserve(applied), 13, false, null, LEASE_NANOS);
		assertFalse(capped.tracker().conflicts(commit(recent.snapshotIndex, key("k9"))));
		var token = capped.tracker().register(put(key("k9")));
		assertTrue(capped.tracker().conflicts(commit(recent.snapshotIndex, key("k9"))));
		capped.tracker().resolve(token, 111);
	}

	@Test
	void aWriteThatWouldMakeTheCommitTooLargeIsRefused() {
		var transaction = session.register(session.reserve(applied), 14, false, null, LEASE_NANOS);
		var large = new LogEntry.Put(1, key("k"), Buf.wrap(new byte[4 * 1024 * 1024]), RequestTypeId.NOTHING);
		long bytes = LeaderSession.OpenTransaction.stagedBytes(large, 1);
		assertEquals(LogEntryCodec.writeSize(large) + 16, bytes);
		for (int i = 0; i < 3; i++) {
			transaction.reserveBytes(bytes);
		}
		var error = assertThrows(RocksDBException.class, () -> transaction.reserveBytes(bytes));
		assertEquals(RocksDBErrorType.INVALID_REQUEST, error.getErrorUniqueId());
		// A staged write that failed gives its room back.
		transaction.releaseBytes(bytes);
		transaction.reserveBytes(bytes);
		transaction.reserveBytes(3 * 1024 * 1024);
		assertThrows(RocksDBException.class, () -> transaction.reserveBytes(1024 * 1024));
		transaction.reserveBytes(1024);
	}

	private static Keys key(String value) {
		return new Keys(Buf.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
	}

	private static LogEntry.Put put(Keys keys) {
		return new LogEntry.Put(1, keys, Buf.wrap(new byte[] {1}), RequestTypeId.NOTHING);
	}

	private static LogEntry.Guarded commit(long snapshotIndex, Keys keys) {
		return new LogEntry.Guarded(3, snapshotIndex, new long[] {1}, new long[] {ConflictTracker.keyHash(1, keys)},
				List.of(), false);
	}
}
