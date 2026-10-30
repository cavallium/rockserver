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
	void recordsOfCompletedRequestsAreDeletedInBulk() throws Exception {
		long column;
		try (var store = open()) {
			var applier = new Applier(store, null, 100, Long.MAX_VALUE);
			column = store.db().createColumn("c", SCHEMA);
			for (long sequence = 0; sequence <= 256; sequence++) {
				long completedBelow = sequence;
				applier.apply(new Request(5, sequence, completedBelow,
						new LogEntry.Put(column, key(sequence), text("v"), RequestTypeId.NOTHING)),
						TermIndex.valueOf(1, sequence + 1));
			}
			// The floor reached 100 and then 200: the records below 200 are gone. Those from 200 to 255 are below the
			// floor (256) too, and wait for the next deletion; the record of 256 is kept.
			assertNull(applier.recordedResult(record(5, 0)));
			assertNull(applier.recordedResult(record(5, 199)));
			assertInstanceOf(ApplyResult.Ok.class, applier.recordedResult(record(5, 200)));
			assertInstanceOf(ApplyResult.Ok.class, applier.recordedResult(record(5, 256)));
		}
		// After a restart, the records left below the floor are not loaded and never used: a copy is refused.
		try (var store = open()) {
			var applier = new Applier(store, null, 100, Long.MAX_VALUE);
			var late = applier.apply(new Request(5, 230, 230,
					new LogEntry.Put(column, key(230), text("late"), RequestTypeId.NOTHING)), TermIndex.valueOf(1, 258));
			assertInstanceOf(ApplyResult.Failed.class, late);
			assertEquals("v", value(store, column, 230));
			// The first entry of this run deleted what the previous run left below the floor.
			assertNull(applier.recordedResult(record(5, 230)));
			var retried = applier.apply(new Request(5, 256, 256,
					new LogEntry.Put(column, key(256), text("again"), RequestTypeId.NOTHING)), TermIndex.valueOf(1, 259));
			assertEquals(ApplyResult.OK_NULL, retried);
			assertEquals("v", value(store, column, 256));
		}
	}

	/** A request that names the record of {@code sequence} of {@code proposerId}, for {@link Applier#recordedResult}. */
	private static Request record(long proposerId, long sequence) {
		return new Request(proposerId, sequence, 0, new LogEntry.Genesis("x", false));
	}

	@Test
	void recordsAreDeletedOnceTheirResultsAddUpToTheByteLimit() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store, null, Long.MAX_VALUE, 4000);
			long column = store.db().createColumn("c", SCHEMA);
			var value = Buf.wrap(new byte[1000]);
			for (long sequence = 0; sequence < 3; sequence++) {
				var merge = new LogEntry.Merge(column, key(sequence), value, RequestTypeId.MERGED);
				applier.apply(new Request(11, sequence, sequence, merge), TermIndex.valueOf(1, sequence + 1));
			}
			// About 3000 bytes of results: the records below the floor (2) stay.
			assertInstanceOf(ApplyResult.Ok.class, applier.recordedResult(record(11, 0)));
			applier.apply(new Request(11, 3, 3, new LogEntry.Merge(column, key(3), value, RequestTypeId.MERGED)),
					TermIndex.valueOf(1, 4));
			// Over 4000 bytes: everything below the floor (3) is deleted, however little the floor moved.
			assertNull(applier.recordedResult(record(11, 0)));
			assertNull(applier.recordedResult(record(11, 2)));
			assertInstanceOf(ApplyResult.Ok.class, applier.recordedResult(record(11, 3)));
		}
	}

	@Test
	void recordsLeftBelowTheFloorOfAStoppedProposerAreDeletedAtTheNextStart() throws Exception {
		long column;
		try (var store = open()) {
			var applier = new Applier(store);
			column = store.db().createColumn("c", SCHEMA);
			for (long sequence = 0; sequence < 10; sequence++) {
				var put = new LogEntry.Put(column, key(sequence), text("v"), RequestTypeId.NOTHING);
				applier.apply(new Request(12, sequence, sequence, put), TermIndex.valueOf(1, sequence + 1));
			}
			// Proposer 12 stops here (its process exits): its own entries will never delete these.
			assertInstanceOf(ApplyResult.Ok.class, applier.recordedResult(record(12, 0)));
		}
		try (var store = open()) {
			var applier = new Applier(store);
			// Only another proposer writes after the restart.
			applier.apply(new Request(13, 0, 0, new LogEntry.Put(column, key(100), text("w"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 11));
			assertNull(applier.recordedResult(record(12, 0)));
			assertNull(applier.recordedResult(record(12, 8)));
			// The record at the floor stays: its request may still be in flight.
			assertInstanceOf(ApplyResult.Ok.class, applier.recordedResult(record(12, 9)));
		}
	}

	@Test
	void aResultTooLargeForAReplyIsRecordedAsAFailureAndTheReplicaGoesOn() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			// Two values whose previous values add up to more than a reply can carry.
			var large = Buf.wrap(new byte[ResultCodec.MAX_RESULT_BYTES / 2 + 1]);
			applier.applyBatch(List.of(
					new Request(14, 0, 0, new LogEntry.Put(column, key(1), large, RequestTypeId.NOTHING)),
					new Request(14, 1, 0, new LogEntry.Put(column, key(2), large, RequestTypeId.NOTHING))),
					TermIndex.valueOf(1, 1));
			var request = new Request(14, 2, 2, new LogEntry.PutMulti(column, List.of(key(1), key(2)),
					List.of(text("a"), text("b")), RequestTypeId.PREVIOUS));
			long size = ResultCodec.encodedSize(new ApplyResult.Ok(List.of(large, large)));
			var result = applier.apply(request, TermIndex.valueOf(1, 2));
			assertEquals(ResultCodec.tooLarge(size), result);
			assertEquals(result, applier.recordedResult(request), "the record holds what the reply carries");
			// The write took effect, and the replica keeps applying.
			assertEquals("a", value(store, column, 1));
			assertEquals("b", value(store, column, 2));
			assertEquals(TermIndex.valueOf(1, 2), store.applied());
			assertEquals(result, applier.apply(request, TermIndex.valueOf(1, 3)), "a copy gets the same result");
			assertEquals(ApplyResult.OK_NULL, applier.apply(new Request(14, 3, 3,
					new LogEntry.Put(column, key(3), text("c"), RequestTypeId.NOTHING)), TermIndex.valueOf(1, 4)));
			assertEquals(TermIndex.valueOf(1, 4), store.applied());
		}
	}

	@Test
	void aLateCopyBelowTheProposersFloorNeverExecutes() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			var merge = new Request(9, 3, 3, new LogEntry.Merge(column, key(1), text("x"), RequestTypeId.NOTHING));
			applier.apply(merge, TermIndex.valueOf(1, 1));
			// The proposer finished with 3 and reported it. The record of 3 is deleted only later, in bulk.
			applier.apply(new Request(9, 512, 512, new LogEntry.Put(column, key(2), text("y"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 2));
			assertInstanceOf(ApplyResult.Ok.class, applier.recordedResult(merge));
			// A copy of 3 that was still in flight is appended afterwards: it must not run again, nor get the record.
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

	@Test
	void aBatchAppliesEveryRequestOnceInOneEntry() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			var first = new Request(3, 10, 10, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING));
			applier.apply(first, TermIndex.valueOf(1, 1));
			var batch = List.of(
					// Already applied: gets its recorded result and does not run again.
					new Request(3, 10, 10, new LogEntry.Put(column, key(1), text("changed"), RequestTypeId.NOTHING)),
					new Request(3, 11, 10, new LogEntry.Put(column, key(2), text("b"), RequestTypeId.NOTHING)),
					// The same request twice in one entry runs once.
					new Request(3, 12, 10, new LogEntry.Merge(column, key(3), text("c"), RequestTypeId.NOTHING)),
					new Request(3, 12, 10, new LogEntry.Merge(column, key(3), text("c"), RequestTypeId.NOTHING)));
			var results = applier.applyBatch(batch.stream()
					.map(r -> new Request(r.proposerId(), r.sequence(), 10, r.entry())).toList(), TermIndex.valueOf(1, 2));
			assertEquals(4, results.size());
			results.forEach(result -> assertInstanceOf(ApplyResult.Ok.class, result));
			assertEquals("a", value(store, column, 1));
			assertEquals("b", value(store, column, 2));
			assertEquals("c", value(store, column, 3));
			assertEquals(TermIndex.valueOf(1, 2), store.applied());
		}
	}

	@Test
	void aRejectedRequestInABatchLeavesNoWritesAndTheOthersApply() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			var results = applier.applyBatch(List.of(
					new Request(4, 0, 0, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING)),
					// Writes key 2, then fails on a key of the wrong size: neither may stay.
					new Request(4, 1, 0, new LogEntry.PutMulti(column, List.of(key(2), new Keys(text("bad"))),
							List.of(text("b"), text("b")), RequestTypeId.NOTHING)),
					new Request(4, 2, 0, new LogEntry.Put(column, key(3), text("c"), RequestTypeId.NOTHING))),
					TermIndex.valueOf(1, 1));
			assertInstanceOf(ApplyResult.Ok.class, results.get(0));
			assertInstanceOf(ApplyResult.Failed.class, results.get(1));
			assertInstanceOf(ApplyResult.Ok.class, results.get(2));
			assertEquals("a", value(store, column, 1));
			assertNull(value(store, column, 2));
			assertEquals("c", value(store, column, 3));
			// The rejection is recorded like any result: a retry gets it again instead of running.
			assertInstanceOf(ApplyResult.Failed.class, applier.recordedResult(new Request(4, 1, 0,
					new LogEntry.Genesis("x", false))));
		}
	}

	@Test
	void aCrashKeepsAConsistentPrefixOfUnloggedApplies() throws Exception {
		long column;
		try (var store = open()) {
			column = store.db().createColumn("c", SCHEMA);
		}
		// Entries applied without the WAL, then a crash before any flush: data and position go away together.
		var crashed = open();
		var applier = new Applier(crashed);
		for (int i = 1; i <= 20; i++) {
			applier.apply(new Request(6, i, i, new LogEntry.Put(column, key(i), text("v" + i), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, i));
		}
		crashed.connection().close();
		try (var store = open()) {
			assertEquals(TermIndex.INITIAL_VALUE, store.applied());
			assertNull(value(store, column, 1));
			assertNull(value(store, column, 20));
			// Replaying the log reapplies them exactly once.
			var replay = new Applier(store);
			for (int i = 1; i <= 20; i++) {
				replay.apply(new Request(6, i, i, new LogEntry.Put(column, key(i), text("v" + i), RequestTypeId.NOTHING)),
						TermIndex.valueOf(1, i));
			}
			assertEquals("v20", value(store, column, 20));
		}
	}

	@Test
	void aCrashKeepsRecordDeletionsOnlyWithTheFloorThatCoversThem() throws Exception {
		long column;
		try (var store = open()) {
			column = store.db().createColumn("c", SCHEMA);
		}
		var crashed = open();
		var applier = new Applier(crashed, null, 4, Long.MAX_VALUE);
		for (int i = 1; i <= 10; i++) {
			applier.apply(new Request(8, i, i, new LogEntry.Put(column, key(i), text("v" + i), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, i));
			if (i == 5) {
				crashed.db().flush();
			}
		}
		crashed.connection().close();
		try (var store = open()) {
			// The flush kept entries 1 to 5, the floor 5 and the deletion of the records below 4.
			assertEquals(5, store.applied().getIndex());
			var replay = new Applier(store, null, 4, Long.MAX_VALUE);
			assertNull(replay.recordedResult(record(8, 3)));
			assertInstanceOf(ApplyResult.Ok.class, replay.recordedResult(record(8, 5)));
			for (int i = 6; i <= 10; i++) {
				replay.apply(new Request(8, i, i, new LogEntry.Put(column, key(i), text("v" + i), RequestTypeId.NOTHING)),
						TermIndex.valueOf(1, i));
			}
			assertEquals("v10", value(store, column, 10));
			assertInstanceOf(ApplyResult.Failed.class, replay.apply(new Request(8, 7, 7,
					new LogEntry.Put(column, key(7), text("late"), RequestTypeId.NOTHING)), TermIndex.valueOf(1, 11)));
			assertEquals("v7", value(store, column, 7));
		}
	}

	@Test
	void aLoggedEntryMakesTheUnloggedOnesBeforeItDurable() throws Exception {
		long column;
		try (var store = open()) {
			column = store.db().createColumn("c", SCHEMA);
		}
		var crashed = open();
		var applier = new Applier(crashed);
		for (int i = 1; i <= 5; i++) {
			applier.apply(new Request(7, i, i, new LogEntry.Put(column, key(i), text("v" + i), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, i));
		}
		// A schema change goes through the WAL: everything applied before it must survive with it.
		applier.apply(new Request(7, 6, 6, new LogEntry.CreateColumn("d", SCHEMA)), TermIndex.valueOf(1, 6));
		applier.apply(new Request(7, 7, 7, new LogEntry.Put(column, key(7), text("v7"), RequestTypeId.NOTHING)),
				TermIndex.valueOf(1, 7));
		crashed.connection().close();
		try (var store = open()) {
			var applied = store.applied().getIndex();
			assertEquals(6, applied, "the logged schema change and every entry before it survive, the later one not");
			assertEquals("v5", value(store, column, 5));
			assertNull(value(store, column, 7));
			store.db().getColumnId("d");
		}
	}
}
