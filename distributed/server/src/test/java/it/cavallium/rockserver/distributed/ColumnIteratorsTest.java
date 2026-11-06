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
}
