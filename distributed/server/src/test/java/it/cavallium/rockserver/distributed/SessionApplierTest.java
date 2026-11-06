package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Client sessions at apply: dedupe by the session's numbering, expiry by leader time, rejection of gone sessions. */
class SessionApplierTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final long TIMEOUT = 1000;

	@TempDir
	Path temp;

	private NodeStore open(String name) throws Exception {
		var config = temp.resolve("rockserver.conf");
		if (!Files.exists(config)) {
			Files.writeString(config, InProcessCluster.MERGE_CONFIG);
		}
		return NodeStore.open(temp.resolve(name), config, "session-applier-test");
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String value(NodeStore store, long column, long key) {
		Buf value = store.db().get(0, column, key(key), RequestType.current());
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
	}

	private static Request merge(long proposerId, long sequence, long column, String text,
			ClientRequestId client) {
		return new Request(proposerId, sequence, 0, new LogEntry.Merge(column, key(1), text(text),
				RequestTypeId.MERGED), client);
	}

	private static String merged(ApplyResult result) {
		return ((Buf) assertInstanceOf(ApplyResult.Ok.class, result).value()).toString(StandardCharsets.UTF_8);
	}

	private static void assertGone(ApplyResult result) {
		var failed = assertInstanceOf(ApplyResult.Failed.class, result);
		assertEquals(RocksDBErrorType.SESSION_EXPIRED, failed.type(), failed.message());
	}

	@Test
	void aSessionWriteTakesEffectOnceWhicheverNodeProposesIt() throws Exception {
		long column;
		long session;
		try (var store = open("db")) {
			column = store.db().createColumn("c", SCHEMA);
			var applier = new Applier(store);
			session = (Long) ((ApplyResult.Ok) applier.apply(new Request(1, 0, 0, new LogEntry.RegisterClient(TIMEOUT)),
					TermIndex.valueOf(1, 10), 5000)).value();
			assertEquals(10, session, "the session id is the log index of its registration");
			var first = new ClientRequestId(session, 0, 0);
			assertEquals("x", merged(applier.apply(merge(2, 0, column, "x", first), TermIndex.valueOf(1, 11), 5100)));
			// The client resent it through another node: another proposer, another proposer sequence.
			assertEquals("x", merged(applier.apply(merge(3, 41, column, "x", first), TermIndex.valueOf(1, 12), 5200)));
			// One entry with a second copy, a write of the same session, and a write without a session.
			var batch = applier.applyBatch(List.of(
					merge(4, 7, column, "x", first),
					merge(4, 8, column, "y", new ClientRequestId(session, 1, 0)),
					new Request(4, 9, 0, new LogEntry.Put(column, key(2), text("plain"), RequestTypeId.NOTHING))),
					TermIndex.valueOf(1, 13), 5300);
			assertEquals("x", merged(batch.get(0)));
			assertEquals("x,y", merged(batch.get(1)));
			assertEquals(ApplyResult.OK_NULL, batch.get(2));
			assertEquals("x,y", value(store, column, 1));
		}
		try (var store = open("db")) {
			var applier = new Applier(store);
			// After a restart the record is still there; once the client reports it complete, a copy is refused.
			assertEquals("x,y", merged(applier.apply(merge(5, 0, column, "y", new ClientRequestId(session, 1, 0)),
					TermIndex.valueOf(2, 14), 5400)));
			var late = applier.apply(merge(5, 1, column, "x", new ClientRequestId(session, 0, 0)),
					TermIndex.valueOf(2, 15), 5500);
			assertInstanceOf(ApplyResult.Ok.class, late, "still above the floor, so the record answers");
			applier.apply(merge(5, 2, column, "z", new ClientRequestId(session, 2, 2)), TermIndex.valueOf(2, 16), 5600);
			var refused = assertInstanceOf(ApplyResult.Failed.class, applier.apply(
					merge(5, 3, column, "x", new ClientRequestId(session, 0, 0)), TermIndex.valueOf(2, 17), 5700));
			assertEquals(RocksDBErrorType.INVALID_REQUEST, refused.type());
			assertEquals("x,y,z", value(store, column, 1));
			assertNotNull(applier.recordedResult(merge(0, 0, column, "", new ClientRequestId(session, 2, 2))));
		}
	}

	@Test
	void anUnknownOrExpiredSessionRejectsItsWritesWithoutRecordingThem() throws Exception {
		try (var store = open("db")) {
			long column = store.db().createColumn("c", SCHEMA);
			var applier = new Applier(store);
			assertGone(applier.apply(merge(2, 0, column, "x", new ClientRequestId(99, 0, 0)), TermIndex.valueOf(1, 1),
					1000));
			long session = (Long) ((ApplyResult.Ok) applier.apply(new Request(1, 0, 0,
					new LogEntry.RegisterClient(TIMEOUT)), TermIndex.valueOf(1, 2), 1000)).value();
			// Active at 1500: alive until 2500.
			assertEquals("a", merged(applier.apply(merge(2, 1, column, "a", new ClientRequestId(session, 0, 0)),
					TermIndex.valueOf(1, 3), 1500)));
			assertEquals("a,b", merged(applier.apply(merge(2, 2, column, "b", new ClientRequestId(session, 1, 0)),
					TermIndex.valueOf(1, 4), 2500)));
			assertGone(applier.apply(merge(2, 3, column, "c", new ClientRequestId(session, 2, 0)),
					TermIndex.valueOf(1, 5), 3501));
			// A new leader whose clock is behind does not bring it back: the clock never goes back.
			assertGone(applier.apply(merge(2, 4, column, "c", new ClientRequestId(session, 2, 0)),
					TermIndex.valueOf(2, 6), 2600));
			// Not even a copy of a write it applied: its records went with it.
			assertGone(applier.apply(merge(2, 5, column, "b", new ClientRequestId(session, 1, 0)),
					TermIndex.valueOf(2, 7), 2700));
			assertNull(applier.recordedResult(merge(0, 0, column, "", new ClientRequestId(session, 1, 0))));
			assertEquals("a,b", value(store, column, 1));
			assertNull(store.db().get(0, store.metaColumnId(), NodeStore.sessionKey(session), RequestType.current()),
					"the expired session was deleted");
		}
	}

	/**
	 * A new leader whose wall clock is a year ahead stamps at most {@link LeaderClock#MAX_TERM_START_STEP_MILLIS} past the
	 * replicated clock, then the time elapsed on its monotonic clock: a session that is alive stays alive on every replica
	 * (the stamps are in the log, so a replay reads the same). A wall clock that jumps within a term changes nothing.
	 */
	@Test
	void aLeaderWithAClockFarAheadDoesNotExpireTheSessions() throws Exception {
		long timeout = LeaderClock.MAX_TERM_START_STEP_MILLIS * 3;
		long year = 365L * 24 * 3600 * 1000;
		try (var store = open("db")) {
			long column = store.db().createColumn("c", SCHEMA);
			var applier = new Applier(store);
			var oldLeader = new LeaderClock();
			long t0 = 1_000_000;
			long session = (Long) ((ApplyResult.Ok) applier.apply(new Request(1, 0, 0,
					new LogEntry.RegisterClient(timeout)), TermIndex.valueOf(1, 1),
					oldLeader.stamp(1, applier.clock(), t0, 0))).value();
			assertEquals(t0, applier.clock());
			// Term 2: the new leader's wall clock is a year ahead.
			var skewed = new LeaderClock();
			long first = skewed.stamp(2, applier.clock(), t0 + year, 5_000_000_000L);
			assertEquals(t0 + LeaderClock.MAX_TERM_START_STEP_MILLIS, first);
			assertEquals("a", merged(applier.apply(merge(2, 1, column, "a", new ClientRequestId(session, 0, 0)),
					TermIndex.valueOf(2, 2), first)));
			// Ten seconds later on its monotonic clock, while its wall clock jumps another year.
			long later = skewed.stamp(2, applier.clock(), t0 + 2 * year, 15_000_000_000L);
			assertEquals(first + 10_000, later);
			assertEquals("a,b", merged(applier.apply(merge(2, 2, column, "b", new ClientRequestId(session, 1, 0)),
					TermIndex.valueOf(2, 3), later)));
			assertEquals(later, applier.clock());
			// Without the cap the first stamp alone would have expired it.
			assertGone(new Applier(store).apply(merge(2, 3, column, "c", new ClientRequestId(session, 2, 0)),
					TermIndex.valueOf(2, 4), t0 + year));
		}
		try (var store = open("db")) {
			// A leader whose clock is behind stamps from the replicated clock on, so the clock keeps moving.
			var applier = new Applier(store);
			long clock = applier.clock();
			var behind = new LeaderClock();
			assertEquals(clock, behind.stamp(3, clock, 5, 0));
			assertEquals(clock + 2_000, behind.stamp(3, clock, 6, 2_000_000_000L));
			// No clock yet (a new cluster): the wall clock.
			assertEquals(42, new LeaderClock().stamp(1, 0, 42, 0));
		}
	}

	/**
	 * Two replicas apply the same entries, one of them restarting in the middle and sweeping expired sessions at other
	 * moments: every result is the same, because expiry reads only the leader time in the entries.
	 */
	@Test
	void expiryIsTheSameOnEveryReplicaWhateverItsRestarts() throws Exception {
		long column;
		try (var store = open("a")) {
			column = store.db().createColumn("c", SCHEMA);
		}
		try (var store = open("b")) {
			assertEquals(column, store.db().createColumn("c", SCHEMA));
		}
		var entries = new ArrayList<Request>();
		var times = new ArrayList<Long>();
		// Index 1 and 2 register sessions 1 and 2.
		entries.add(new Request(1, 0, 0, new LogEntry.RegisterClient(TIMEOUT)));
		times.add(10_000L);
		entries.add(new Request(1, 1, 0, new LogEntry.RegisterClient(3 * TIMEOUT)));
		times.add(10_000L);
		long[] stamps = {10_500, 11_400, 11_900, 12_200, 12_300, 9_000, 13_100, 13_500, 14_100, 15_000};
		for (int i = 0; i < stamps.length; i++) {
			long session = 1 + i % 2;
			entries.add(merge(2, i, column, "s" + session + "-" + i, new ClientRequestId(session, i, 0)));
			times.add(stamps[i]);
		}
		var straight = new ArrayList<String>();
		try (var store = open("a")) {
			var applier = new Applier(store);
			for (int i = 0; i < entries.size(); i++) {
				straight.add(render(applier.apply(entries.get(i), TermIndex.valueOf(1, i + 1), times.get(i))));
			}
		}
		var restarted = new ArrayList<String>();
		for (int from = 0, restart = 5; from < entries.size(); from = restart, restart = entries.size()) {
			try (var store = open("b")) {
				var applier = new Applier(store);
				for (int i = from; i < restart; i++) {
					restarted.add(render(applier.apply(entries.get(i), TermIndex.valueOf(1, i + 1), times.get(i))));
				}
			}
		}
		assertEquals(straight, restarted);
		// Session 1 (timeout 1 s) was idle from 10 500 to 11 900, so it expired; session 2 (3 s) lived throughout, also
		// through an entry stamped by a leader whose clock was behind.
		assertEquals("s1-0", merged(parse(straight.get(2))));
		assertGone(parse(straight.get(4)));
		assertGone(parse(straight.get(6)));
		assertInstanceOf(ApplyResult.Ok.class, parse(straight.get(entries.size() - 1)));
		try (var a = open("a"); var b = open("b")) {
			assertEquals(value(a, column, 1), value(b, column, 1));
		}
	}

	/** A result as text that compares by content. */
	private static String render(ApplyResult result) {
		return HexFormat.of().formatHex(ResultCodec.encode(result));
	}

	private static ApplyResult parse(String rendered) {
		return ResultCodec.decode(HexFormat.of().parseHex(rendered));
	}
}
