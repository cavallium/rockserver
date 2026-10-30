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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
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

	// Chained updates. A pipelined session on column 1, key "k".

	private static final Keys CHAIN_KEY = key("k");
	private static final long HASH = ConflictTracker.keyHash(1, CHAIN_KEY);

	private final LeaderSession chain = new LeaderSession(3, applied, new ConflictTracker(), true);
	private long nextId = -2;

	private CompletableFuture<LeaderSession.KeyLock> lock() {
		var waiter = new CompletableFuture<LeaderSession.KeyLock>();
		chain.lockKey(HASH, 1, waiter);
		return waiter;
	}

	/** Opens the update that holds {@code lock}, chained if the lock says so. */
	private LeaderSession.OpenTransaction open(LeaderSession.KeyLock lock, long leaseNanos) {
		long id = lock.base != null ? chain.nextChainedId() : nextId--;
		return chain.register(chain.reserve(applied), id, true, lock, lock.base, leaseNanos);
	}

	private LeaderSession.OpenTransaction open(LeaderSession.KeyLock lock) {
		return open(lock, LEASE_NANOS);
	}

	/** Admits the commit of {@code update} that writes {@code write} and returns its link, null if it gave none. */
	private LeaderSession.ChainLink admit(LeaderSession.OpenTransaction update, LogEntry.Write write) {
		var entry = update.updateEntry(3, write, List.of(CHAIN_KEY), true);
		var token = chain.tracker().register(entry);
		var links = chain.admitted(List.of(new Request(1, 1, 0, entry)), token);
		return links == null ? null : links.getFirst();
	}

	private LeaderSession.ChainLink admit(LeaderSession.OpenTransaction update, String value) {
		return admit(update, new LogEntry.Put(1, CHAIN_KEY, text(value), RequestTypeId.NOTHING));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
	}

	private static List<ApplyResult> ok() {
		return List.of(ApplyResult.OK_NULL);
	}

	/** Settles {@code link} as applied at {@code index}, the way the apply does. */
	private void applied(LeaderSession.ChainLink link, long index) {
		chain.tracker().resolve(link.token, index);
		applied.advance(index);
		chain.settle(List.of(link), ok());
	}

	private void rejected(LeaderSession.ChainLink link, long index) {
		chain.tracker().resolve(link.token, index);
		applied.advance(index);
		chain.settle(List.of(link),
				List.of(new ApplyResult.Failed(RocksDBErrorType.UPDATE_RETRY, "Please, retry the transaction")));
	}

	@Test
	void aKeyPassesAtAdmissionInArrivalOrderAndTheNextUpdateReadsTheAdmittedValue() {
		var head = lock().join();
		assertEquals(null, head.base, "nothing to read from: a head");
		var second = lock();
		var third = lock();
		assertFalse(second.isDone(), "held until the commit is admitted");

		var firstLink = admit(open(head), "one");
		assertTrue(second.isDone());
		assertFalse(third.isDone());
		assertEquals(firstLink, second.join().base);
		assertEquals("one", firstLink.value.toString(java.nio.charset.StandardCharsets.UTF_8));

		var secondLink = admit(open(second.join()), "two");
		assertEquals(secondLink, third.join().base);
		assertEquals(2, chain.chainLinks());
		assertEquals(1, chain.lockedKeys());
	}

	@Test
	void aLateArrivalChainsOnTheNewestLink() {
		var link = admit(open(lock().join()), "one");
		assertEquals(link, lock().join().base);
	}

	@Test
	void aSettledChainLeavesNoState() {
		var link = admit(open(lock().join()), "one");
		applied(link, 101);
		assertEquals(0, chain.chainLinks());
		assertEquals(0, chain.lockedKeys());
		assertEquals(null, lock().join().base, "a head again");
	}

	@Test
	void theNextUpdateAfterASettledChainIsAHead() {
		var head = open(lock().join());
		var next = lock();
		var link = admit(head, "one");
		var chained = open(next.join());
		var last = lock();
		applied(link, 101);
		assertFalse(last.isDone(), "the key is held");
		chain.remove(chained);
		assertEquals(null, last.join().base, "no unsettled link");
	}

	@Test
	void aDeleteIsAPredecessorWithAnAbsentValue() {
		var update = open(lock().join());
		var link = admit(update, new LogEntry.Delete(1, CHAIN_KEY, RequestTypeId.NOTHING));
		assertEquals(null, link.value);
		assertEquals(-1, link.guard().length());
		assertEquals(link, lock().join().base);
	}

	@Test
	void writesNobodyCanReadKeepTheKeyUntilTheUpdateEnds() {
		for (LogEntry.Write write : List.of(
				new LogEntry.Merge(1, CHAIN_KEY, text("m"), RequestTypeId.NOTHING),
				new LogEntry.PutMulti(1, List.of(CHAIN_KEY), List.of(text("m")), RequestTypeId.NOTHING),
				new LogEntry.Put(1, key("another key"), text("m"), RequestTypeId.NOTHING),
				new LogEntry.Put(1, CHAIN_KEY, Buf.wrap(new byte[LeaderSession.MAX_LINK_VALUE_BYTES + 1]),
						RequestTypeId.NOTHING))) {
			var update = open(lock().join());
			var next = lock();
			assertEquals(null, admit(update, write), write.getClass().getSimpleName());
			assertFalse(next.isDone());
			chain.remove(update);
			assertTrue(next.isDone());
			chain.remove(open(next.join()));
		}
		assertEquals(0, chain.lockedKeys());
	}

	@Test
	void aChainHasACap() {
		var waiter = lock();
		var links = new ArrayList<LeaderSession.ChainLink>();
		for (int i = 0; i < LeaderSession.MAX_CHAIN_LINKS; i++) {
			var update = open(waiter.join());
			waiter = lock();
			links.add(admit(update, "v" + i));
		}
		assertEquals(LeaderSession.MAX_CHAIN_LINKS, chain.chainLinks());
		assertFalse(waiter.isDone(), "the chain is full");
		applied(links.getFirst(), 101);
		assertEquals(links.getLast(), waiter.join().base, "room again, and still the newest link");
	}

	@Test
	void aConflictWithoutLinksDoesNotWedgeTheKey() {
		var update = open(lock().join());
		var next = lock();
		// Its commit conflicted: nothing was admitted.
		chain.remove(update);
		assertEquals(null, next.join().base);
	}

	@Test
	void aFailedLinkStopsChainingUntilEveryLinkIsSettled() {
		var head = admit(open(lock().join()), "one");
		var second = admit(open(lock().join()), "two");
		var holder = open(lock().join());
		var third = lock();

		rejected(head, 101);
		chain.remove(holder);
		assertFalse(third.isDone(), "the chain is broken while a link is unsettled");
		assertEquals(1, chain.chainLinks());
		rejected(second, 102);
		assertEquals(null, third.join().base, "broken clears at zero links, and the next update is a head");
		assertEquals(0, chain.chainLinks());
	}

	@Test
	void aLinkAdmittedAfterABreakIsNotChainedOn() {
		var head = admit(open(lock().join()), "one");
		var second = admit(open(lock().join()), "two");
		var chained = open(lock().join());
		var next = lock();
		chain.linkFailed(head);
		var third = admit(chained, "three");
		assertEquals(2, chain.chainLinks());
		assertFalse(next.isDone(), "the next update must not read a broken chain");
		applied(second, 101);
		assertFalse(next.isDone());
		applied(third, 102);
		assertEquals(null, next.join().base);
	}

	@Test
	void anUnknownOutcomeCountsAsFailedAndALaterSettleChangesNothing() {
		var link = admit(open(lock().join()), "one");
		chain.linkFailed(link);
		assertFalse(link.settled.join());
		assertEquals(0, chain.chainLinks());
		applied(link, 101);
		assertFalse(link.settled.join(), "the outcome stays failed");
		assertEquals(0, chain.chainLinks(), "a link is counted once");
		chain.linkFailed(link);
		assertEquals(0, chain.chainLinks());
	}

	@Test
	void releasingTwiceDoesNotGrantTwice() {
		var update = open(lock().join());
		var next = lock();
		var last = lock();
		admit(update, "one");
		assertTrue(next.isDone());
		// The update ends after admission: its key already moved on.
		chain.remove(update);
		chain.unlockKey(update.keyLock);
		assertFalse(last.isDone());
	}

	@Test
	void anAdmissionAfterTheUpdateEndedGivesNoLink() {
		var update = open(lock().join());
		var next = lock();
		chain.remove(update);
		assertTrue(next.isDone());
		assertEquals(null, admit(update, "one"));
		assertEquals(0, chain.chainLinks());
	}

	@Test
	void aSecondCommitOfOneUpdateGivesNoSecondLink() {
		var update = open(lock().join());
		assertTrue(admit(update, "one") != null);
		assertEquals(null, admit(update, "two"));
		assertEquals(1, chain.chainLinks());
	}

	@Test
	void anExpiredHolderReleasesItsKeyKeepingTheChain() throws Exception {
		var link = admit(open(lock().join()), "one");
		var chained = open(lock().join(), 1);
		var next = lock();
		Thread.sleep(5);
		chain.expire();
		assertEquals(link, next.join().base, "the expired update wrote nothing");
		assertEquals(null, chain.get(chained.id));
	}

	@Test
	void aPredecessorExpiringAfterAdmissionChangesNothing() throws Exception {
		var update = open(lock().join(), 1);
		var link = admit(update, "one");
		var next = lock();
		Thread.sleep(5);
		chain.expire();
		assertEquals(link, next.join().base);
		assertEquals(1, chain.chainLinks());
		applied(link, 101);
		assertTrue(link.settled.join());
	}

	@Test
	void anInterveningWriterStopsChaining() {
		var link = admit(open(lock().join()), "one");
		var writer = chain.tracker().register(put(CHAIN_KEY));
		var next = lock();
		assertFalse(next.isDone(), "the write after the link hides it");
		applied(link, 101);
		assertEquals(null, next.join().base);
		chain.tracker().resolve(writer, 102);
	}

	/**
	 * A write after the link, applied and visible, while the link is not settled yet: an update that starts now must
	 * not read the link, because its close would then hide a write that came before it.
	 */
	@Test
	void aWriteAppliedBeforeItsLinkSettledIsNotHidden() {
		var link = admit(open(lock().join()), "one");
		var writer = chain.tracker().register(put(CHAIN_KEY));
		chain.tracker().resolve(link.token, 101);
		chain.tracker().resolve(writer, 102);
		applied.advance(102);
		var next = lock();
		assertFalse(next.isDone());
		chain.settle(List.of(link), ok());
		assertEquals(null, next.join().base);
	}

	@Test
	void aColumnWriteStopsChaining() {
		var link = admit(open(lock().join()), "one");
		var writer = chain.tracker().register(new LogEntry.DeleteRange(1, null, null));
		var next = lock();
		assertFalse(next.isDone());
		applied(link, 101);
		assertEquals(null, next.join().base);
		chain.tracker().resolve(writer, 102);
	}

	@Test
	void breakingAChainMakesTheNextUpdateAHead() {
		var link = admit(open(lock().join()), "one");
		chain.breakChain(HASH);
		var next = lock();
		assertFalse(next.isDone());
		applied(link, 101);
		assertEquals(null, next.join().base);
		chain.breakChain(HASH);
		chain.remove(open(next.join()));
		chain.breakChain(HASH + 1);
		assertEquals(null, lock().join().base, "a key without links is not broken");
	}

	@Test
	void theCheckOfAChainedCommitDependsOnItsBase() {
		var link = admit(open(lock().join()), "one");
		var chained = open(lock().join());
		var entry = chained.updateEntry(3, new LogEntry.Put(1, CHAIN_KEY, text("two"), RequestTypeId.NOTHING),
				List.of(CHAIN_KEY), true);
		assertEquals(link.guard().length(), entry.guard().length());
		assertEquals(new ConflictTracker.ChainCheck(1, HASH, link.token), chain.chainCheck(entry));
		assertTrue(chain.tracker().conflicts(List.of((LogEntry) entry), chain::chainCheck).isEmpty());

		chain.linkFailed(link);
		assertEquals(null, chain.chainCheck(entry), "its base failed");
		assertEquals(it.unimi.dsi.fastutil.ints.IntList.of(0),
				chain.tracker().conflicts(List.of((LogEntry) entry), chain::chainCheck));
	}

	@Test
	void aGoneUpdateFailsItsChainCheck() {
		admit(open(lock().join()), "one");
		var chained = open(lock().join());
		var entry = chained.updateEntry(3, new LogEntry.Put(1, CHAIN_KEY, text("two"), RequestTypeId.NOTHING),
				List.of(CHAIN_KEY), true);
		chain.remove(chained);
		assertEquals(null, chain.chainCheck(entry));
	}

	@Test
	void chainedIdsArePositiveAndDifferAcrossTerms() {
		long id = chain.nextChainedId();
		assertTrue(id > 0);
		assertTrue(chain.nextChainedId() > id);
		assertEquals(3, id >>> 32);
		var later = new LeaderSession(Integer.MAX_VALUE + 5L, applied, new ConflictTracker(), true);
		assertTrue(later.nextChainedId() > 0);
	}

	@Test
	void theEndOfLeadershipSettlesLinksAndFailsWaiters() {
		var link = admit(open(lock().join()), "one");
		var chained = open(lock().join());
		var waiter = lock();
		// A chained update has no native transaction: closing it never reaches the database.
		chain.close(null);
		assertFalse(link.settled.join());
		var error = assertThrows(CompletionException.class, waiter::join);
		assertInstanceOf(RocksDBRetryException.class, error.getCause());
		assertEquals(0, chain.chainLinks());
		assertEquals(0, chain.lockedKeys());
		assertEquals(null, admit(chained, "two"), "nothing is admitted after the end of leadership");
		applied(link, 101);
		assertFalse(link.settled.join());
	}

	@Test
	void aCancelledWaiterIsSkipped() {
		var link = admit(open(lock().join()), "one");
		var chained = open(lock().join());
		var cancelled = lock();
		var next = lock();
		cancelled.cancel(false);
		chain.remove(chained);
		assertEquals(link, next.join().base);
		var cancelledFirst = new CompletableFuture<LeaderSession.KeyLock>();
		cancelledFirst.cancel(false);
		chain.lockKey(HASH + 7, 1, cancelledFirst);
		assertEquals(1, chain.lockedKeys(), "a waiter that is already done never queues");
	}

	/** A slow dependent of a waiter would stall appends: the grant must not wait for it. */
	@Test
	void theGrantOnlyQueuesWhatDependsOnIt() throws Exception {
		var update = open(lock().join());
		var waiter = lock();
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		waiter.orTimeout(1, TimeUnit.MINUTES).handleAsync((_, _) -> {
			entered.countDown();
			try {
				release.await();
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			return null;
		});
		assertTrue(admit(update, "one") != null);
		assertTrue(entered.await(10, TimeUnit.SECONDS));
		assertEquals(1, chain.chainLinks(), "the admission returned while the update opens");
		release.countDown();
	}

	/** What runs under the log lock must not wait for the session's monitor, held for a whole expiry scan. */
	@Test
	void whatRunsUnderTheLogLockNeverTakesTheSessionsMonitor() throws Exception {
		var update = open(lock().join());
		var next = lock();
		var finished = new CompletableFuture<LeaderSession.ChainLink>();
		synchronized (chain) {
			Thread.ofVirtual().start(() -> {
				var link = admit(update, "one");
				chain.chainCheck(new LogEntry.Guarded(3, 0, new long[0], new long[0],
						List.of(new LogEntry.Put(1, CHAIN_KEY, text("x"), RequestTypeId.NOTHING)), true, 123,
						LogEntry.Guard.of(1, CHAIN_KEY, null)));
				chain.settle(List.of(link), ok());
				chain.lockedKeys();
				finished.complete(link);
			});
			assertTrue(finished.get(10, TimeUnit.SECONDS).settled.isDone());
		}
		assertTrue(next.isDone());
	}

	@Test
	void withoutTheFlagNothingIsChained() {
		var lock = session.lockKey(KEY).join();
		var update = session.register(session.reserve(applied), -5, true, lock, LEASE_NANOS);
		var next = session.lockKey(KEY);
		var entry = update.updateEntry(3, new LogEntry.Put(1, key("x"), text("v"), RequestTypeId.NOTHING), List.of(key("x")),
				true);
		assertEquals(-5, entry.updateId());
		assertEquals(null, session.admitted(List.of(new Request(1, 1, 0, entry)), session.tracker().register(entry)));
		assertFalse(next.isDone(), "the key waits for the end of the update");
		session.remove(update);
		assertEquals(null, next.join().base);
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
