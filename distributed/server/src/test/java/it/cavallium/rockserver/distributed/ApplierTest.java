package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Apply semantics on one replica's database: results, at-most-once retries, rejections. */
class ApplierTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	@TempDir
	Path temp;

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private NodeStore open() throws Exception {
		var config = temp.resolve("rockserver.conf");
		if (!Files.exists(config)) {
			Files.writeString(config, InProcessCluster.MERGE_CONFIG);
		}
		return NodeStore.open(temp.resolve("db"), config, "applier-test");
	}

	private static String value(NodeStore store, long column, long key) {
		Buf value = store.db().get(0, column, key(key), RequestType.current());
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
	}

	@Test
	void aRetriedRequestTakesEffectOnceAndGetsTheFirstResult() throws Exception {
		long column;
		var merge = new Request(42, 7, 7, new LogEntry.Merge(0, key(1), text("x"), RequestTypeId.MERGED));
		try (var store = open()) {
			var applier = new Applier(store);
			column = store.db().createColumn("c", SCHEMA);
			merge = new Request(42, 7, 7, new LogEntry.Merge(column, key(1), text("x"), RequestTypeId.MERGED));
			var first = applier.apply(merge, TermIndex.valueOf(1, 10));
			assertEquals("x", ((Buf) ((ApplyResult.Ok) first).value()).toString(StandardCharsets.UTF_8));
			// The same request again, as a retry that reached another leader appends it a second time.
			var retried = applier.apply(merge, TermIndex.valueOf(2, 11));
			assertEquals(first, retried);
			assertEquals("x", value(store, column, 1));
		}
		// After a restart the record is still there.
		try (var store = open()) {
			var applier = new Applier(store);
			assertEquals(TermIndex.valueOf(2, 11), store.applied());
			applier.apply(merge, TermIndex.valueOf(3, 12));
			assertEquals("x", value(store, column, 1));
			// A different request of the same proposer does take effect.
			applier.apply(new Request(42, 8, 7, new LogEntry.Merge(column, key(1), text("y"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(3, 13));
			assertEquals("x,y", value(store, column, 1));
		}
	}

	@Test
	void recordsOfCompletedRequestsAreDeleted() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			for (long sequence = 0; sequence <= 256; sequence++) {
				long completedBelow = sequence;
				applier.apply(new Request(5, sequence, completedBelow,
						new LogEntry.Put(column, key(sequence), text("v"), RequestTypeId.NOTHING)),
						TermIndex.valueOf(1, sequence + 1));
			}
			// Sequence 256 reported everything below it complete: those records are gone, its own is kept.
			assertNull(applier.recordedResult(new Request(5, 0, 0, new LogEntry.Genesis("x", false))));
			assertNull(applier.recordedResult(new Request(5, 255, 0, new LogEntry.Genesis("x", false))));
			assertInstanceOf(ApplyResult.Ok.class,
					applier.recordedResult(new Request(5, 256, 0, new LogEntry.Genesis("x", false))));
		}
	}

	@Test
	void aLateCopyBelowTheProposersFloorNeverExecutes() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			var merge = new Request(9, 3, 3, new LogEntry.Merge(column, key(1), text("x"), RequestTypeId.NOTHING));
			applier.apply(merge, TermIndex.valueOf(1, 1));
			// The proposer finished with 3 and reported it: records below 512 are deleted.
			applier.apply(new Request(9, 512, 512, new LogEntry.Put(column, key(2), text("y"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 2));
			assertNull(applier.recordedResult(merge));
			// A copy of 3 that was still in flight is appended afterwards: it must not run again.
			var late = applier.apply(merge, TermIndex.valueOf(1, 3));
			assertInstanceOf(ApplyResult.Failed.class, late);
			assertEquals("x", value(store, column, 1));
		}
	}

	@Test
	void logicalRejectionsBecomeResultsAndTheLogMovesOn() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			var missingColumn = applier.apply(new Request(1, 0, 0,
					new LogEntry.Put(123456, key(1), text("v"), RequestTypeId.NOTHING)), TermIndex.valueOf(1, 1));
			assertInstanceOf(ApplyResult.Failed.class, missingColumn);
			assertEquals(TermIndex.valueOf(1, 1), store.applied());

			var internal = applier.apply(new Request(1, 1, 0,
					new LogEntry.Put(store.metaColumnId(), NodeStore.key(NodeStore.APPLIED), text("x"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 2));
			assertInstanceOf(ApplyResult.Failed.class, internal, "the internal column cannot be written");
			assertEquals(TermIndex.valueOf(1, 2), store.applied());
		}
	}

	@Test
	void aTransactionCommitIsAllOrNothing() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			var commit = new LogEntry.Guarded(1, 0, new long[0], new long[0], List.of(
					new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING),
					new LogEntry.Put(123456, key(2), text("b"), RequestTypeId.NOTHING)), false);
			var result = applier.apply(new Request(1, 0, 0, commit), TermIndex.valueOf(1, 1));
			assertInstanceOf(ApplyResult.Failed.class, result);
			assertNull(value(store, column, 1), "the first write of a rejected commit must be rolled back");
		}
	}

	@Test
	void columnIdDivergenceStopsTheReplica() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long c = store.db().createColumn("c", SCHEMA);
			long d = store.db().createColumn("d", SCHEMA);
			applier.apply(new Request(1, 0, 0, new LogEntry.VerifyColumn("c", c)), TermIndex.valueOf(1, 1));
			// The leader says "c" is d's id here: writes for "c" would land in "d".
			assertThrows(Applier.ReplicaFailedException.class, () -> applier.apply(
					new Request(1, 1, 0, new LogEntry.VerifyColumn("c", d)), TermIndex.valueOf(1, 2)));
		}
	}

	@Test
	void aColumnDeletedAndRecreatedBeforeItsVerificationIsNotDivergence() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long first = store.db().createColumn("c", SCHEMA);
			store.db().deleteColumn(first);
			store.db().createColumn("c", SCHEMA);
			applier.apply(new Request(1, 0, 0, new LogEntry.VerifyColumn("c", first)), TermIndex.valueOf(1, 1));
			assertEquals(TermIndex.valueOf(1, 1), store.applied());
		}
	}
}
