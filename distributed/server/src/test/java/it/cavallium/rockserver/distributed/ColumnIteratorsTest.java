package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Iterators and replication: a column deletion does not wait for them, and an unknown one asks for a retry. */
class ColumnIteratorsTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	@TempDir
	Path temp;

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	/** DD-7a: before, the deletion waited for the iterator, on the leader's apply thread, until its lease ended. */
	@Test
	void applyingAColumnDeletionClosesTheColumnsIteratorsInsteadOfWaitingForThem() throws Exception {
		try (var store = NodeStore.open(temp.resolve("db"), null, "iterators-test")) {
			var db = store.db();
			long column = db.createColumn("c", SCHEMA);
			long other = db.createColumn("other", SCHEMA);
			db.put(0, column, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
			long iterator = db.openIterator(0, column, null, null, false, Duration.ofHours(1));
			long kept = db.openIterator(0, other, null, null, false, Duration.ofHours(1));
			var applier = new Applier(store);
			var deletion = CompletableFuture.supplyAsync(() -> applier.apply(
					new Request(1, 0, 0, new LogEntry.DeleteColumn(column)), TermIndex.valueOf(1, 1), -1));
			assertEquals(ApplyResult.OK_NULL, deletion.get(10, TimeUnit.SECONDS));
			assertFalse(db.getAllColumnDefinitions().containsKey("c"));
			var gone = assertThrows(RocksDBException.class, () -> db.seekTo(iterator, key(1)));
			assertTrue(Router.isUnknownIterator(gone, iterator), gone::toString);
			// The other column's iterator is still there.
			db.seekTo(kept, key(1));

			long named = db.openIterator(0, other, null, null, false, Duration.ofHours(1));
			var byName = CompletableFuture.supplyAsync(() -> applier.apply(
					new Request(1, 1, 0, new LogEntry.DeleteColumnIfExists("other")), TermIndex.valueOf(1, 2), -1));
			assertEquals(new ApplyResult.Ok(true), byName.get(10, TimeUnit.SECONDS));
			assertTrue(Router.isUnknownIterator(assertThrows(RocksDBException.class, () -> db.seekTo(named, key(1))),
					named));
			assertEquals(0, db.closeIteratorsOfColumn("missing"));
			assertEquals(0, db.closeIteratorsOfColumn(column));
		}
	}

	/**
	 * DD-7f: an iterator id that the leader does not hold (opened on an earlier leader) answers {@code UPDATE_RETRY}, as
	 * a transaction lost with its leader does; the database's own error for it is pinned here.
	 */
	@Test
	void anUnknownIteratorOnTheLeaderAsksForARetry() throws Exception {
		try (var store = NodeStore.open(temp.resolve("db"), null, "iterators-test")) {
			var db = store.db();
			long column = db.createColumn("c", SCHEMA);
			long unknown = 123456789L;
			var error = assertThrows(RocksDBException.class, () -> db.seekTo(unknown, key(1)));
			assertTrue(Router.isUnknownIterator(error, unknown), error::toString);
			assertFalse(Router.isUnknownIterator(error, unknown + 1));
			var mapped = Router.iteratorCall(CompletableFuture.failedFuture(error), unknown);
			var cause = assertThrows(ExecutionException.class, () -> mapped.get(1, TimeUnit.SECONDS)).getCause();
			assertInstanceOf(RocksDBRetryException.class, cause);
			// Other failures pass unchanged.
			var other = RocksDBException.of(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND, "x");
			var passed = Router.iteratorCall(CompletableFuture.failedFuture(other), unknown);
			assertEquals(other, assertThrows(ExecutionException.class, () -> passed.get(1, TimeUnit.SECONDS)).getCause());
			long open = db.openIterator(0, column, null, null, false, Duration.ofHours(1));
			assertEquals(null, Router.iteratorCall(CompletableFuture.completedFuture(null), open).get());
		}
	}

	/**
	 * DD-7a: a range stream holds a use of its column from its start to its end, and the deletion waited for it on the
	 * apply thread (here up to the stream's deadline of an hour). The apply now ends the column's streams first: the
	 * deletion goes through, and the stream ends with COLUMN_NOT_FOUND instead of completing as if the range ended.
	 */
	@Test
	void applyingAColumnDeletionEndsTheColumnsStreamsInsteadOfWaitingForThem() throws Exception {
		try (var store = NodeStore.open(temp.resolve("db"), null, "streams-test")) {
			var db = store.db();
			long column = db.createColumn("c", SCHEMA);
			long other = db.createColumn("other", SCHEMA);
			for (long i = 0; i < 2_000; i++) {
				db.put(0, column, key(i), Buf.wrap(new byte[] {1}), RequestType.none());
				db.put(0, other, key(i), Buf.wrap(new byte[] {2}), RequestType.none());
			}
			var streams = new ColumnStreams();
			var api = store.connection().getAsyncApi(
					it.cavallium.rockserver.core.common.RequestContext.batch(Duration.ofHours(1)));
			var slow = new SlowSubscriber();
			streams.track(column, reactor.core.publisher.Flux.from(api.getRangeAsync(0, column, null, null, false,
					RequestType.<it.cavallium.rockserver.core.common.KV>allInRange()))).subscribe(slow);
			var kept = new SlowSubscriber();
			streams.track(other, reactor.core.publisher.Flux.from(api.getRangeAsync(0, other, null, null, false,
					RequestType.<it.cavallium.rockserver.core.common.KV>allInRange()))).subscribe(kept);
			// One item each, then no more demand: the streams stay open, holding their columns.
			assertTrue(slow.first.await(10, TimeUnit.SECONDS));
			assertTrue(kept.first.await(10, TimeUnit.SECONDS));
			assertEquals(1, streams.running(column));

			var applier = new Applier(store);
			applier.columnStreams(streams);
			var deletion = CompletableFuture.supplyAsync(() -> applier.apply(
					new Request(1, 0, 0, new LogEntry.DeleteColumn(column)), TermIndex.valueOf(1, 1), -1));
			assertEquals(ApplyResult.OK_NULL, deletion.get(10, TimeUnit.SECONDS));
			var error = slow.error.get(10, TimeUnit.SECONDS);
			assertInstanceOf(RocksDBException.class, error);
			assertEquals(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND,
					((RocksDBException) error).getErrorUniqueId());
			assertFalse(slow.completed, "never as if the range had ended");
			assertEquals(0, streams.running(column));
			// The other column's stream goes on to its end.
			kept.request(Long.MAX_VALUE);
			assertEquals(null, kept.error.get(10, TimeUnit.SECONDS));
			assertTrue(kept.completed);
			assertEquals(2_000, kept.items.get());

			// While a deletion runs, a new stream of the column fails at once.
			long again = db.createColumn("again", SCHEMA);
			streams.deleting(again);
			var refused = new SlowSubscriber();
			streams.track(again, reactor.core.publisher.Flux.never()).subscribe(refused);
			assertInstanceOf(RocksDBException.class, refused.error.get(10, TimeUnit.SECONDS));
			streams.deleted(again);
			assertEquals(0, streams.running(again));
		}
	}

	/** Asks for one item, then for nothing more until told. */
	private static final class SlowSubscriber extends reactor.core.publisher.BaseSubscriber<Object> {
		final java.util.concurrent.CountDownLatch first = new java.util.concurrent.CountDownLatch(1);
		final CompletableFuture<Throwable> error = new CompletableFuture<>();
		final java.util.concurrent.atomic.AtomicLong items = new java.util.concurrent.atomic.AtomicLong();
		volatile boolean completed;

		@Override
		protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
			request(1);
		}

		@Override
		protected void hookOnNext(Object value) {
			items.incrementAndGet();
			first.countDown();
		}

		@Override
		protected void hookOnError(Throwable throwable) {
			error.complete(throwable);
		}

		@Override
		protected void hookOnComplete() {
			completed = true;
			error.complete(null);
		}
	}
}
