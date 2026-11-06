package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.ratis.server.protocol.TermIndex;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The partitioned apply gives the results and the database of the serial apply, byte for byte: two replicas apply the
 * same randomized log, one serially and one in lanes, and are compared after every group.
 */
class ParallelApplyTest {

	private static final ColumnSchema FIXED = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	/** Buckets where every tail of a fixed key collides: writes to different keys read and rewrite one RocksDB key. */
	private static final ColumnSchema BUCKETED = ColumnSchema.of(IntList.of(Long.BYTES),
			ObjectList.of(ColumnHashType.ALLSAME8), true);
	/** No fixed key part: the whole column is one lane. */
	private static final ColumnSchema TAIL_ONLY = ColumnSchema.of(IntList.of(), ObjectList.of(ColumnHashType.XXHASH8),
			true);

	@TempDir
	Path temp;

	private NodeStore open(String name) throws Exception {
		var config = temp.resolve("rockserver.conf");
		if (!Files.exists(config)) {
			Files.writeString(config, InProcessCluster.MERGE_CONFIG);
		}
		Files.createDirectories(temp.resolve(name));
		return NodeStore.open(temp.resolve(name).resolve("db"), config, name);
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Keys bucketKey(long fixed, String tail) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(fixed).array()),
				Buf.wrap(tail.getBytes(StandardCharsets.UTF_8)));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * A cluster whose nodes apply in lanes (two of them) or serially (the third) serves concurrent writes over several
	 * columns, a multi-column transaction included, and every replica holds the same data, also after a restart.
	 */
	@Test
	void aClusterWithPartitionedApplyConverges() throws Exception {
		Files.createDirectories(temp.resolve("cluster"));
		try (var cluster = new InProcessCluster(temp.resolve("cluster"), 3,
				c -> c.nodeId().equals("n2") ? c : c.withParallelApplyThreads(3)).startAll()) {
			var leader = cluster.awaitLeader(java.time.Duration.ofSeconds(30));
			var api = leader.connection().getSyncApi(
					it.cavallium.rockserver.core.common.RequestContext.batch(java.time.Duration.ofSeconds(20)));
			long[] columns = new long[3];
			for (int i = 0; i < columns.length; i++) {
				columns[i] = api.createColumn("c" + i, FIXED);
			}
			try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
				var futures = new ArrayList<java.util.concurrent.Future<?>>();
				for (int t = 0; t < 8; t++) {
					int thread = t;
					futures.add(executor.submit(() -> {
						for (int i = 0; i < 60; i++) {
							long column = columns[(thread + i) % columns.length];
							long k = (thread * 7L + i) % 16;
							if (i % 3 == 0) {
								api.merge(0, column, key(k), text("t" + thread + "." + i), RequestType.none());
							} else {
								api.put(0, column, key(k), text("t" + thread + "." + i), RequestType.none());
							}
							if (i % 20 == 0) {
								long tx = api.openTransaction(java.time.Duration.ofSeconds(10));
								api.put(tx, columns[0], key(100 + thread), text("tx" + i), RequestType.none());
								api.put(tx, columns[1], key(100 + thread), text("tx" + i), RequestType.none());
								api.closeTransaction(tx, true);
							}
						}
						return null;
					}));
				}
				for (var future : futures) {
					future.get();
				}
			}
			cluster.awaitConverged(java.time.Duration.ofSeconds(30));
			var expected = userRows(leader.stateMachine().store(), columns);
			assertTrue(expected.size() >= 48, expected.toString());
			for (var node : cluster.runningNodes()) {
				assertTrue(!node.isFailed(), node.config().nodeId());
				assertEquals(expected, userRows(node.stateMachine().store(), columns), node.config().nodeId());
			}
			// A node in lanes that replays entries after a restart reaches the same data.
			var restarted = cluster.anyFollower().config().nodeId();
			cluster.halt(restarted);
			api.put(0, columns[2], key(1), text("after"), RequestType.none());
			var node = cluster.start(restarted);
			cluster.awaitConverged(java.time.Duration.ofSeconds(30));
			assertTrue(!node.isFailed());
			assertEquals(userRows(leader.stateMachine().store(), columns), userRows(node.stateMachine().store(), columns));
		}
	}

	private static List<String> userRows(NodeStore store, long[] columns) {
		var rows = new ArrayList<String>();
		var hex = HexFormat.of();
		for (long column : columns) {
			try (var range = store.db().getRange(0L, column, null, null, false, RequestType.<KV>allInRange())) {
				range.forEach(kv -> rows.add(column + ":" + hex.formatHex(kv.keys().keys()[0].toByteArray()) + "="
						+ hex.formatHex(kv.value().toByteArray())));
			}
		}
		return rows;
	}

	@Test
	void parallelApplyMatchesSerialApplyByteForByte() throws Exception {
		for (int seed = 1; seed <= 3; seed++) {
			run(seed);
		}
	}

	private void run(long seed) throws Exception {
		var lanes = new ApplyLanes("parallel-test", 4);
		try (var a = open("serial-" + seed); var b = open("parallel-" + seed)) {
			// Small purge limits, so that purges (whose timing depends on the commits) run too.
			var serial = new Applier(a, null, 8, 2048);
			var parallel = new Applier(b, null, 8, 2048, lanes);
			long[] columns = createColumns(a, b);
			var log = new Workload(new Random(seed), columns, a.metaColumnId()).log(40);
			for (int group = 0; group < log.size(); group++) {
				serial.beginGroup();
				parallel.beginGroup();
				var expected = new ArrayList<List<ApplyResult>>();
				var actual = new ArrayList<List<ApplyResult>>();
				for (var step : log.get(group)) {
					var result = step.apply(serial);
					if (result != null) {
						expected.add(result);
					}
					result = step.apply(parallel);
					if (result != null) {
						actual.add(result);
					}
				}
				serial.endGroup();
				parallel.endGroup();
				for (int i = 0; i < expected.size(); i++) {
					assertEquals(expected.get(i), List.copyOf(actual.get(i)), "results of entry " + i + " of group " + group);
				}
				assertEquals(dump(a, columns), dump(b, columns), "databases after group " + group);
			}
			var results = dump(a, columns);
			assertTrue(results.size() > 100, "the workload wrote something: " + results.size());
			assertTrue(parallel.segmentedEntries > 100, "entries applied in lanes: " + parallel.segmentedEntries);
			assertTrue(parallel.segmentCuts > 5, "segments cut by a rejection: " + parallel.segmentCuts);
			System.out.println("Partitioned apply: " + parallel.segmentedEntries + " entries in lanes, "
					+ parallel.segmentCuts + " cuts");
		} finally {
			lanes.close();
		}
	}

	/** Creates the workload's columns in every store, with the same ids. */
	private static long[] createColumns(NodeStore... stores) {
		long[] columns = new long[5];
		var schemas = List.of(FIXED, FIXED, FIXED, BUCKETED, TAIL_ONLY);
		for (int i = 0; i < columns.length; i++) {
			columns[i] = stores[0].db().createColumn("c" + i, schemas.get(i));
			for (int s = 1; s < stores.length; s++) {
				assertEquals(columns[i], stores[s].db().createColumn("c" + i, schemas.get(i)));
			}
		}
		return columns;
	}

	/**
	 * A replica applying in lanes that crashes keeps a consistent prefix of the log, whichever of its commits was the
	 * last to reach the disk: the serial prefix of a group committed before a segment, a segment (the lanes' batches
	 * joined with the records and the position), the end of a group, an entry applied alone. The apply writes no WAL,
	 * so what survives is what the last flush kept: the test flushes right after one such commit, goes on applying
	 * (into the same group or past it) and then closes the database without a flush. Reopened, the replica holds
	 * exactly what was committed at the flush, the position included; replaying the rest of the log (serially, or in
	 * lanes) gives the results and every row, the internal column included, of a replica that never crashed.
	 */
	@Test
	void aCrashDuringPartitionedApplyKeepsAConsistentPrefix() throws Exception {
		var lanes = new ApplyLanes("crash-test", 4);
		try {
			for (int seed = 1; seed <= 2; seed++) {
				long[] columns;
				List<List<Step>> log;
				var expected = new HashMap<Long, List<ApplyResult>>();
				List<String> expectedRows;
				var commits = new HashMap<String, Integer>();
				try (var reference = open("reference-" + seed); var lanesOnly = open("lanes-" + seed)) {
					columns = createColumns(reference, lanesOnly);
					log = new Workload(new Random(seed), columns, reference.metaColumnId()).log(25);
					var serial = new Applier(reference, null, 8, 2048);
					var parallel = new Applier(lanesOnly, null, 8, 2048, lanes);
					parallel.afterCommit = what -> commits.merge(what, 1, Integer::sum);
					for (var group : log) {
						serial.beginGroup();
						parallel.beginGroup();
						for (var step : group) {
							var result = step.apply(serial);
							if (result != null) {
								expected.put(step.position().getIndex(), result);
							}
							step.apply(parallel);
						}
						serial.endGroup();
						parallel.endGroup();
					}
					expectedRows = dump(reference, columns);
					assertEquals(expectedRows, dump(lanesOnly, columns));
				}
				for (var what : List.of("prefix", "segment", "group")) {
					assertTrue(commits.getOrDefault(what, 0) > 3, "commits of the lanes replica: " + commits);
				}
				var random = new Random(seed * 31);
				var points = List.of("prefix", "segment", "group", "entry", "prefix", "segment", "prefix", "segment");
				for (int p = 0; p < points.size(); p++) {
					var what = points.get(p);
					int count = commits.getOrDefault(what, 0);
					if (count == 0) {
						continue;
					}
					crashAndReplay("crash-" + seed + "-" + p, columns, log, expected, expectedRows, lanes, what,
							1 + random.nextInt(count), random.nextInt(25), p % 2 == 1 ? lanes : null);
				}
			}
		} finally {
			lanes.close();
		}
	}

	/**
	 * Applies {@code log} in lanes, flushes right after the {@code at}-th commit of kind {@code what}, applies
	 * {@code after} more entries and crashes; then reopens, checks what survived and replays the rest of the log.
	 */
	private void crashAndReplay(String name, long[] columns, List<List<Step>> log,
			Map<Long, List<ApplyResult>> expected, List<String> expectedRows, ApplyLanes lanes, String what, int at,
			int after, @Nullable ApplyLanes replayLanes) throws Exception {
		var point = name + " (flush after " + what + " commit " + at + ", " + after + " entries more)";
		List<String> durable;
		TermIndex durablePosition;
		var crashed = open(name);
		try {
			assertEquals(java.util.Arrays.toString(columns), java.util.Arrays.toString(createColumns(crashed)));
			crashed.db().flush();
			var flushed = new Object[] {null, null};
			int[] seen = {0};
			var applier = new Applier(crashed, null, 8, 2048, lanes);
			applier.afterCommit = kind -> {
				if (kind.equals(what) && ++seen[0] == at) {
					crashed.db().flush();
					flushed[0] = dump(crashed, columns);
					flushed[1] = crashed.applied();
				}
			};
			int left = after;
			boolean inGroup = false;
			outer:
			for (var group : log) {
				applier.beginGroup();
				inGroup = true;
				for (var step : group) {
					step.apply(applier);
					if (flushed[0] != null && left-- <= 0) {
						break outer;
					}
				}
				applier.endGroup();
				inGroup = false;
			}
			if (inGroup) {
				applier.abortGroup();
			}
			assertTrue(flushed[0] != null, point + ": the flush point was reached");
			@SuppressWarnings("unchecked") var rows = (List<String>) flushed[0];
			durable = rows;
			durablePosition = (TermIndex) flushed[1];
		} finally {
			// Kill: no flush, no WAL; only what the flush above kept is on disk.
			crashed.connection().close();
		}
		long applied;
		List<String> replayed;
		String metaPrefix;
		try (var store = open(name)) {
			assertEquals(durablePosition, store.applied(), point);
			assertEquals(durable, dump(store, columns), point + ": the rows committed at the flush, no more, no less");
			applied = store.applied().getIndex();
			var results = applyLog(new Applier(store, null, 8, 2048, replayLanes), log, applied, Long.MAX_VALUE);
			results.forEach((index, result) -> assertEquals(expected.get(index), result,
					point + ": result of the replayed entry " + index));
			replayed = dump(store, columns);
			metaPrefix = store.metaColumnId() + ":";
		}
		assertEquals(expectedRows.stream().filter(row -> !row.startsWith(metaPrefix)).toList(),
				replayed.stream().filter(row -> !row.startsWith(metaPrefix)).toList(), point + ": user rows after the replay");
		// A restarted applier deletes the records below every floor at its start: the internal column matches a serial
		// replica restarted at the same position.
		try (var restarted = open(name + "-serial")) {
			assertEquals(java.util.Arrays.toString(columns), java.util.Arrays.toString(createColumns(restarted)));
			applyLog(new Applier(restarted, null, 8, 2048), log, 0, applied);
			applyLog(new Applier(restarted, null, 8, 2048), log, applied, Long.MAX_VALUE);
			assertEquals(dump(restarted, columns), replayed, point + ": every row after the replay");
		}
	}

	/**
	 * Applies the entries of {@code log} after {@code fromIndex} up to {@code toIndex}, in the log's groups (a group
	 * partly in the range is cut there), and returns their results by index.
	 */
	private static Map<Long, List<ApplyResult>> applyLog(Applier applier, List<List<Step>> log, long fromIndex,
			long toIndex) {
		var results = new HashMap<Long, List<ApplyResult>>();
		for (var group : log) {
			var pending = new HashMap<Long, List<ApplyResult>>();
			boolean open = false;
			for (var step : group) {
				long index = step.position().getIndex();
				if (index > fromIndex && index <= toIndex) {
					if (!open) {
						applier.beginGroup();
						open = true;
					}
					var result = step.apply(applier);
					if (result != null) {
						pending.put(index, result);
					}
				}
			}
			if (open) {
				applier.endGroup();
			}
			pending.forEach((index, result) -> results.put(index, List.copyOf(result)));
		}
		return results;
	}

	/** One log entry of the workload: a mark (no requests) or a batch, with the leader's time. */
	private record Step(TermIndex position, @Nullable List<Request> requests, long leaderTime) {

		@Nullable List<ApplyResult> apply(Applier applier) {
			if (requests == null) {
				applier.markApplied(position);
				return null;
			}
			return applier.applyBatch(requests, position, leaderTime);
		}
	}

	/** Every row of every column, the internal one included. */
	private static List<String> dump(NodeStore store, long[] columns) {
		var rows = new ArrayList<String>();
		var ids = new ArrayList<Long>();
		ids.add(store.metaColumnId());
		for (long column : columns) {
			ids.add(column);
		}
		var hex = HexFormat.of();
		for (long column : ids) {
			try (var range = store.db().getRange(0L, column, null, null, false, RequestType.<KV>allInRange())) {
				range.forEach(kv -> {
					var row = new StringBuilder().append(column).append(':');
					for (var part : kv.keys().keys()) {
						row.append(hex.formatHex(part.toByteArray())).append('/');
					}
					row.append('=').append(kv.value() == null ? "null" : hex.formatHex(kv.value().toByteArray()));
					rows.add(row.toString());
				});
			}
		}
		return rows;
	}

	/** A randomized log: entries of several proposers and sessions, in groups. */
	private static final class Workload {
		final Random random;
		final long[] columns;
		final long metaColumn;
		long index = 1;
		long leaderTime = 10_000;
		final long[] nextSequence = new long[4];
		final long[] completedBelow = new long[4];
		final Map<Integer, List<Request>> sent = new HashMap<>();
		final List<Long> sessions = new ArrayList<>();
		final Map<Long, long[]> sessionSequences = new HashMap<>();

		Workload(Random random, long[] columns, long metaColumn) {
			this.random = random;
			this.columns = columns;
			this.metaColumn = metaColumn;
		}

		List<List<Step>> log(int groups) {
			var log = new ArrayList<List<Step>>();
			for (int group = 0; group < groups; group++) {
				var steps = new ArrayList<Step>();
				int entries = 1 + random.nextInt(24);
				for (int e = 0; e < entries; e++) {
					var position = TermIndex.valueOf(1, index++);
					int kind = random.nextInt(100);
					if (kind < 4) {
						steps.add(new Step(position, null, 0));
						continue;
					}
					if (kind < 6) {
						leaderTime += random.nextInt(3) == 0 ? 3000 : 10;
					}
					List<Request> requests;
					if (kind < 9) {
						requests = List.of(new Request(0, nextSequence[0]++, completedBelow[0],
								new LogEntry.RegisterClient(2000)));
					} else if (kind < 11) {
						long column = columns[random.nextInt(3)];
						long from = random.nextInt(8);
						requests = List.of(new Request(0, nextSequence[0]++, completedBelow[0],
								new LogEntry.DeleteRange(column, key(from), key(from + 1 + random.nextInt(4)))));
					} else {
						requests = batch();
					}
					leaderTime += random.nextInt(50);
					steps.add(new Step(position, requests, leaderTime));
					if (requests.getFirst().entry() instanceof LogEntry.RegisterClient) {
						sessions.add(position.getIndex());
						sessionSequences.put(position.getIndex(), new long[2]);
					}
				}
				log.add(List.copyOf(steps));
			}
			return log;
		}

		/** One entry of a proposer: a mix of single-lane and cross-lane requests, rejections and copies. */
		List<Request> batch() {
			int proposer = 1 + random.nextInt(3);
			if (random.nextInt(4) == 0) {
				completedBelow[proposer] = Math.max(completedBelow[proposer], nextSequence[proposer] - random.nextInt(6));
			}
			var requests = new ArrayList<Request>();
			int size = 1 + random.nextInt(12);
			var history = sent.computeIfAbsent(proposer, _ -> new ArrayList<>());
			for (int i = 0; i < size; i++) {
				if (!history.isEmpty() && random.nextInt(12) == 0) {
					// A copy of an earlier request of this proposer: recorded, stale, or in this very entry.
					var old = history.get(history.size() - 1 - random.nextInt(Math.min(history.size(), 20)));
					if (old.sequence() < completedBelow[proposer]) {
						continue;
					}
					requests.add(new Request(proposer, old.sequence(), completedBelow[proposer], old.entry(), old.client()));
					continue;
				}
				ClientRequestId client = null;
				if (!sessions.isEmpty() && random.nextInt(4) == 0) {
					long session = sessions.get(random.nextInt(sessions.size()));
					var numbering = sessionSequences.get(session);
					if (random.nextInt(4) == 0) {
						numbering[1] = Math.max(numbering[1], numbering[0] - random.nextInt(3));
					}
					client = new ClientRequestId(session, numbering[0]++, numbering[1]);
				}
				var request = new Request(proposer, nextSequence[proposer]++, completedBelow[proposer], write(), client);
				history.add(request);
				requests.add(request);
			}
			if (requests.isEmpty()) {
				var request = new Request(proposer, nextSequence[proposer]++, completedBelow[proposer], write());
				history.add(request);
				requests.add(request);
			}
			return requests;
		}

		LogEntry write() {
			int kind = random.nextInt(100);
			long k = random.nextInt(12);
			long c0 = columns[0];
			long c1 = columns[1];
			long c2 = columns[2];
			if (kind < 15) {
				return new LogEntry.Merge(c0, key(k), text("m" + random.nextInt(100)),
						random.nextBoolean() ? RequestTypeId.MERGED : RequestTypeId.NOTHING);
			} else if (kind < 25) {
				return new LogEntry.Put(c0, key(k), text("p" + random.nextInt(100)),
						random.nextBoolean() ? RequestTypeId.PREVIOUS : RequestTypeId.NOTHING);
			} else if (kind < 40) {
				// Guarded update over a small set of values: some guards hold, some do not.
				return guarded(c1, k % 4, random.nextInt(5) == 0 ? null : "g" + random.nextInt(3), "g" + random.nextInt(3));
			} else if (kind < 48) {
				return new LogEntry.Delete(c2, key(k), random.nextBoolean() ? RequestTypeId.PREVIOUS : RequestTypeId.NOTHING);
			} else if (kind < 58) {
				// Several keys: one lane only when they all share the first key part.
				boolean sameKey = random.nextBoolean();
				var keys = new ArrayList<Keys>();
				var values = new ArrayList<Buf>();
				for (int i = 0; i < 1 + random.nextInt(4); i++) {
					keys.add(key(sameKey ? k : random.nextInt(12)));
					values.add(text("pm" + random.nextInt(100)));
				}
				return new LogEntry.PutMulti(c2, keys, values, RequestTypeId.NOTHING);
			} else if (kind < 66) {
				var keys = new ArrayList<Keys>();
				var values = new ArrayList<Buf>();
				for (int i = 0; i < 1 + random.nextInt(3); i++) {
					keys.add(key(random.nextInt(12)));
					values.add(text("mm" + random.nextInt(100)));
				}
				return new LogEntry.MergeMulti(c0, keys, values, RequestTypeId.MERGED);
			} else if (kind < 76) {
				return new LogEntry.Put(columns[3], bucketKey(k % 3, "t" + random.nextInt(5)), text("b" + random.nextInt(100)),
						RequestTypeId.PREVIOUS);
			} else if (kind < 82) {
				return new LogEntry.Delete(columns[3], bucketKey(k % 3, "t" + random.nextInt(5)), RequestTypeId.NOTHING);
			} else if (kind < 88) {
				return new LogEntry.Put(columns[4], new Keys(text("tail" + random.nextInt(6))), text("x" + random.nextInt(9)),
						RequestTypeId.PREVIOUS);
			} else if (kind < 93) {
				// A transaction over two columns: the serial path.
				return new LogEntry.Guarded(1, 0, new long[0], new long[0], List.of(
						new LogEntry.Put(c0, key(k), text("t" + random.nextInt(9)), RequestTypeId.NOTHING),
						new LogEntry.Put(c2, key(k + 1), text("t" + random.nextInt(9)), RequestTypeId.NOTHING)),
						false, 0, null);
			} else if (kind < 96) {
				// Rejected the same way everywhere: the internal column, a key of the wrong size.
				return new LogEntry.Put(random.nextBoolean() ? metaColumn : c0,
						new Keys(text("bad")), text("x"), RequestTypeId.NOTHING);
			} else {
				return new LogEntry.Merge(c0, key(k), text("q"), RequestTypeId.NOTHING);
			}
		}

		static LogEntry.Guarded guarded(long column, long key, @Nullable String read, String value) {
			return new LogEntry.Guarded(1, 0, new long[0], new long[0],
					List.of(new LogEntry.Put(column, key(key), text(value), RequestTypeId.NOTHING)), true, 99,
					LogEntry.Guard.of(column, key(key), read == null ? null : text(read)));
		}
	}
}
