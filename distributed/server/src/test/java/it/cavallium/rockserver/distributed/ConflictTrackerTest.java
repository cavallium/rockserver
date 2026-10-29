package it.cavallium.rockserver.distributed;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.MemorySegmentBuf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ConflictTrackerTest {

	private static final long EPOCH = 42;
	private static final long COLUMN = 1;
	private static final long OTHER_COLUMN = 2;
	private static final Buf VALUE = Buf.wrap(new byte[] {7});

	private final ConflictTracker tracker = new ConflictTracker();

	@Test
	void pendingWriteConflictsWithEveryOlderSnapshot() {
		var token = tracker.register(put(COLUMN, key("k")));

		assertTrue(tracker.conflicts(transaction(10, COLUMN, key("k"))));
		assertTrue(tracker.conflicts(transaction(1_000_000, COLUMN, key("k"))));
		assertFalse(tracker.conflicts(transaction(10, COLUMN, key("other"))));
		assertFalse(tracker.conflicts(transaction(10, OTHER_COLUMN, key("k"))));

		tracker.resolve(token, 12);
		assertTrue(tracker.conflicts(transaction(10, COLUMN, key("k"))));
		assertTrue(tracker.conflicts(transaction(11, COLUMN, key("k"))));
		assertFalse(tracker.conflicts(transaction(12, COLUMN, key("k"))));
		assertFalse(tracker.conflicts(transaction(13, COLUMN, key("k"))));
	}

	@Test
	void laterPendingWriterStaysPendingWhenEarlierWriterResolves() {
		var first = tracker.register(put(COLUMN, key("k")));
		var second = tracker.register(new LogEntry.Delete(COLUMN, key("k"), RequestTypeId.NOTHING));

		tracker.resolve(first, 12);
		assertTrue(tracker.conflicts(transaction(12, COLUMN, key("k"))), "second write is still pending");
		assertTrue(tracker.conflicts(transaction(100, COLUMN, key("k"))));

		tracker.resolve(second, 13);
		assertTrue(tracker.conflicts(transaction(12, COLUMN, key("k"))));
		assertFalse(tracker.conflicts(transaction(13, COLUMN, key("k"))));
	}

	@Test
	void conflictIsDetectedThroughAnyOfSeveralKeysAndColumns() {
		var token = tracker.register(put(OTHER_COLUMN, key("b")));
		tracker.resolve(token, 20);

		var twoColumns = new LogEntry.Guarded(EPOCH,
				15,
				new long[] {COLUMN, OTHER_COLUMN},
				new long[] {ConflictTracker.keyHash(COLUMN, key("a")), ConflictTracker.keyHash(OTHER_COLUMN, key("b"))},
				List.of(),
				false
		);
		assertTrue(tracker.conflicts(twoColumns));
	}

	@Test
	void writesInsideGuardedEntriesAndMultiWritesRegisterEveryKey() {
		var commit = new LogEntry.Guarded(EPOCH,
				5,
				new long[0],
				new long[0],
				List.of(put(COLUMN, key("a")),
						new LogEntry.MergeMulti(OTHER_COLUMN, List.of(key("b"), key("c")), List.of(VALUE, VALUE),
								RequestTypeId.NOTHING
						)
				),
				false
		);
		tracker.resolve(tracker.register(commit), 8);
		tracker.resolve(tracker.register(new LogEntry.PutMulti(COLUMN, List.of(key("d"), key("e")),
				List.of(VALUE, VALUE), RequestTypeId.NOTHING)), 9);
		var deleteMulti = new LogEntry.DeleteMulti(COLUMN, List.of(key("f")), RequestTypeId.NOTHING);
		tracker.resolve(tracker.register(deleteMulti), 10);

		for (var written : List.of(key("a"), key("d"), key("e"), key("f"))) {
			assertTrue(tracker.conflicts(transaction(7, COLUMN, written)), written.toString());
		}
		assertTrue(tracker.conflicts(transaction(7, OTHER_COLUMN, key("b"))));
		assertTrue(tracker.conflicts(transaction(7, OTHER_COLUMN, key("c"))));
		assertFalse(tracker.conflicts(transaction(7, COLUMN, key("b"))));
		assertFalse(tracker.conflicts(transaction(10, COLUMN, key("a"))));
	}

	@Test
	void deleteRangeAndDeleteColumnMarkTheWholeColumn() {
		var range = tracker.register(new LogEntry.DeleteRange(COLUMN, key("a"), key("m")));

		assertTrue(tracker.conflicts(transaction(10, COLUMN, key("z"))), "pending range covers every key");
		assertFalse(tracker.conflicts(transaction(10, OTHER_COLUMN, key("z"))));

		tracker.resolve(range, 12);
		assertTrue(tracker.conflicts(transaction(11, COLUMN, key("anything"))));
		assertFalse(tracker.conflicts(transaction(12, COLUMN, key("anything"))));

		tracker.resolve(tracker.register(new LogEntry.DeleteColumn(OTHER_COLUMN)), 13);
		assertTrue(tracker.conflicts(transaction(12, OTHER_COLUMN, key("x"))));
		assertFalse(tracker.conflicts(transaction(12, COLUMN, key("x"))));
	}

	@Test
	void columnCreationAndNameBasedDeletionConflictWithEveryColumn() {
		var schema = ColumnSchema.of(IntList.of(), ObjectList.of(), true);
		var create = tracker.register(new LogEntry.CreateColumn("new", schema));

		assertTrue(tracker.conflicts(transaction(10, COLUMN, key("k"))));
		assertTrue(tracker.conflicts(transaction(10, 999, key("k"))));
		tracker.resolve(create, 12);
		assertTrue(tracker.conflicts(transaction(11, OTHER_COLUMN, key("k"))));
		assertFalse(tracker.conflicts(transaction(12, OTHER_COLUMN, key("k"))));

		var delete = tracker.register(new LogEntry.DeleteColumnIfExists("old"));
		assertTrue(tracker.conflicts(transaction(12, COLUMN, key("k"))));
		tracker.resolve(delete, 13);
		assertFalse(tracker.conflicts(transaction(13, COLUMN, key("k"))));
	}

	@Test
	void entriesWithoutDataWritesConflictWithNothing() {
		var upload = tracker.register(new LogEntry.UploadMergeOperator("op", "a.B", new byte[] {1, 2}));
		var genesis = tracker.register(new LogEntry.Genesis("cluster", false));

		assertFalse(tracker.conflicts(transaction(0, COLUMN, key("k"))));
		assertEquals(0, tracker.size());
		tracker.resolve(upload, 1);
		tracker.resolve(genesis, 2);
		tracker.prune(2);
		assertFalse(tracker.conflicts(transaction(0, COLUMN, key("k"))));
	}

	@Test
	void pruneForgetsResolvedWritesAtOrBelowTheIndex() {
		tracker.resolve(tracker.register(put(COLUMN, key("a"))), 10);
		tracker.resolve(tracker.register(new LogEntry.DeleteRange(OTHER_COLUMN, null, null)), 11);
		tracker.resolve(tracker.register(put(COLUMN, key("b"))), 12);
		assertEquals(3, tracker.size());

		tracker.prune(11);
		assertEquals(1, tracker.size());
		assertTrue(tracker.conflicts(transaction(11, COLUMN, key("b"))));
		assertFalse(tracker.conflicts(transaction(11, COLUMN, key("a"))));
		assertFalse(tracker.conflicts(transaction(11, OTHER_COLUMN, key("a"))));

		tracker.prune(12);
		assertEquals(0, tracker.size());
	}

	@Test
	void pruneNeverLosesPendingWrites() {
		var old = tracker.register(put(COLUMN, key("k")));
		tracker.register(put(COLUMN, key("k")));
		tracker.register(new LogEntry.DeleteRange(OTHER_COLUMN, null, null));
		tracker.register(new LogEntry.CreateColumn("c", ColumnSchema.of(IntList.of(), ObjectList.of(), true)));
		tracker.resolve(old, 5);

		tracker.prune(Long.MAX_VALUE - 1);
		assertEquals(3, tracker.size());
		var allColumns = new LogEntry.Guarded(EPOCH, 100, new long[0], new long[0], List.of(), false);
		assertTrue(tracker.conflicts(allColumns), "pending column creation");
		assertTrue(tracker.conflicts(transaction(100, COLUMN, key("k"))));
	}

	@Test
	void entryThatFailedToAppendStopsConflictingOnceALaterEntryApplies() {
		// Ratis rejects both appends after preAppendTransaction registered them: they are never resolved.
		tracker.register(put(COLUMN, key("k")));
		tracker.register(new LogEntry.CreateColumn("c", ColumnSchema.of(IntList.of(), ObjectList.of(), true)));
		assertTrue(tracker.conflicts(transaction(20, COLUMN, key("k"))));
		assertTrue(tracker.conflicts(transaction(20, OTHER_COLUMN, key("x"))));

		var appended = tracker.register(put(OTHER_COLUMN, key("y")));
		var later = tracker.register(put(COLUMN, key("later")));
		tracker.resolve(appended, 21);

		assertFalse(tracker.conflicts(transaction(20, COLUMN, key("k"))));
		assertFalse(tracker.conflicts(transaction(20, OTHER_COLUMN, key("x"))));
		assertTrue(tracker.conflicts(transaction(19, COLUMN, key("k"))), "settled as if applied at index 20");
		assertTrue(tracker.conflicts(transaction(21, COLUMN, key("later"))), "entries registered later stay pending");

		tracker.resolve(later, 22);
		tracker.prune(22);
		assertEquals(0, tracker.size());
	}

	@Test
	void keyHashIsStable() {
		// Changing these values breaks transactions in flight during a rolling upgrade of the leader.
		assertEquals(0x6393D51C06C618DCL, ConflictTracker.keyHash(0, new Keys()));
		assertEquals(0x77BA7B72868EACCEL, ConflictTracker.keyHash(1, key("")));
		assertEquals(0xEA3A411CC4FEA65BL, ConflictTracker.keyHash(1, key("a")));
		assertEquals(0x81BD9B573565F7EDL, ConflictTracker.keyHash(2, key("a")));
		assertEquals(0x9BDBA4F8702F9050L, ConflictTracker.keyHash(1, key("a", "b")));
		assertEquals(0x25027BF41D245ED3L, ConflictTracker.keyHash(-1, key("0123456789abcdefXYZ")));
	}

	@Test
	void keyHashDependsOnColumnSegmentsAndLengths() {
		var hashes = new HashSet<Long>();
		var variants = List.of(new Keys(),
				key(""),
				key("", ""),
				key("a"),
				key("a", ""),
				key("", "a"),
				key("ab"),
				key("a", "b"),
				new Keys(Buf.wrap(new byte[] {0})),
				new Keys(Buf.wrap(new byte[8])),
				new Keys(Buf.wrap(new byte[9]))
		);
		for (long column : new long[] {0, 1, 2, -1}) {
			for (var keys : variants) {
				assertTrue(hashes.add(ConflictTracker.keyHash(column, keys)), column + " " + keys);
			}
		}
		assertNotEquals(ConflictTracker.keyHash(1, key("12345678a")), ConflictTracker.keyHash(1, key("12345678b")));
	}

	@Test
	void keyHashIgnoresHowTheBytesAreStored() {
		byte[] bytes = "--0123456789abcdefXYZ--".getBytes(UTF_8);
		long expected = ConflictTracker.keyHash(3, key("0123456789abcdefXYZ"));

		assertEquals(expected, ConflictTracker.keyHash(3, new Keys(Buf.wrap(bytes, 2, bytes.length - 2))));
		try (var arena = Arena.ofConfined()) {
			var segment = arena.allocate(bytes.length - 4);
			segment.copyFrom(MemorySegment.ofArray(bytes).asSlice(2, bytes.length - 4));
			assertEquals(expected, ConflictTracker.keyHash(3, new Keys(new MemorySegmentBuf(segment))));
		}
	}

	@Test
	void neverMissesAConflictOfARandomHistory() {
		for (int seed = 0; seed < 20; seed++) {
			new RandomHistory(new Random(seed), false).run(3_000);
			new RandomHistory(new Random(seed), true).run(3_000);
		}
	}

	/**
	 * Drives the tracker and a naive reference with the same random leader history: entries registered in log
	 * order, applied in log order, conflict checks with snapshots between the prune watermark and the applied index,
	 * and prunes up to some applied index. Some entries fail to append: they take no log index and never apply, and
	 * the reference treats them as applied just before the next entry that applies.
	 */
	private static final class RandomHistory {

		private static final long[] COLUMNS = {1, 2, 3};
		private static final String[] SEGMENTS = {"", "a", "b", "ab"};

		private final Random random;
		private final boolean withLostEntries;
		private final ConflictTracker tracker = new ConflictTracker();
		private final List<ReferenceEntry> registered = new ArrayList<>();
		private final ArrayDeque<ReferenceEntry> unapplied = new ArrayDeque<>();
		private final List<ReferenceEntry> unsettledLost = new ArrayList<>();
		private long nextIndex = 1;
		private long appliedIndex = 0;
		private long pruneWatermark = 0;
		private int conflictsSeen;
		private int nonConflictsSeen;

		RandomHistory(Random random, boolean withLostEntries) {
			this.random = random;
			this.withLostEntries = withLostEntries;
		}

		void run(int steps) {
			for (int step = 0; step < steps; step++) {
				int choice = random.nextInt(100);
				if (choice < 30) {
					register();
				} else if (choice < 65) {
					applyNext();
				} else if (choice < 95) {
					check(step);
				} else {
					pruneWatermark = between(pruneWatermark, appliedIndex);
					tracker.prune(pruneWatermark);
				}
			}
			assertTrue(conflictsSeen > 100 && nonConflictsSeen > 100,
					"history too one-sided: " + conflictsSeen + " conflicts, " + nonConflictsSeen + " not");
			// One more entry that applies settles the entries that failed to append, so nothing stays tracked.
			register(new LogEntry.Genesis("cluster", false), false);
			while (!unapplied.isEmpty()) {
				applyNext();
			}
			tracker.prune(appliedIndex);
			assertEquals(0, tracker.size(), "everything applied and pruned");
		}

		private void register() {
			register(randomEntry(), withLostEntries && random.nextInt(10) == 0);
		}

		private void register(LogEntry entry, boolean lost) {
			var reference = new ReferenceEntry(entry, tracker.register(entry), registered.size());
			registered.add(reference);
			if (lost) {
				unsettledLost.add(reference);
			} else {
				reference.index = nextIndex++;
				unapplied.add(reference);
			}
		}

		private void applyNext() {
			var next = unapplied.poll();
			if (next == null) {
				return;
			}
			next.writtenAt = next.index;
			appliedIndex = next.index;
			for (var lost : unsettledLost) {
				if (lost.sequence < next.sequence) {
					lost.writtenAt = next.index - 1;
				}
			}
			unsettledLost.removeIf(lost -> lost.writtenAt != ReferenceEntry.PENDING);
			tracker.resolve(next.token, next.index);
		}

		private void check(int step) {
			// Mostly recent snapshots, like short transactions.
			long snapshot = Math.max(pruneWatermark, appliedIndex - random.nextInt(20));
			int count = 1 + random.nextInt(3);
			long[] columns = new long[count];
			long[] hashes = new long[count];
			var keys = new ArrayList<ReferenceKey>();
			for (int i = 0; i < count; i++) {
				var key = randomKey();
				keys.add(key);
				columns[i] = key.columnId();
				hashes[i] = ConflictTracker.keyHash(key.columnId(), key.keys());
			}
			var guarded = new LogEntry.Guarded(EPOCH, snapshot, columns, hashes, List.of(), false);

			boolean expected = registered.stream()
					.anyMatch(entry -> entry.writtenAt > snapshot && entry.touchesAny(keys));
			boolean actual = tracker.conflicts(guarded);
			String context = "step " + step + ", snapshot " + snapshot + ", keys " + keys;
			if (expected) {
				assertTrue(actual, "missed conflict at " + context);
			} else {
				assertFalse(actual, "false conflict at " + context);
			}
			if (expected) {
				conflictsSeen++;
			} else {
				nonConflictsSeen++;
			}
		}

		private long between(long min, long max) {
			return min + random.nextLong(max - min + 1);
		}

		private ReferenceKey randomKey() {
			int segmentCount = 1 + random.nextInt(2);
			var segments = new String[segmentCount];
			for (int i = 0; i < segmentCount; i++) {
				segments[i] = SEGMENTS[random.nextInt(SEGMENTS.length)];
			}
			return new ReferenceKey(COLUMNS[random.nextInt(COLUMNS.length)], key(segments));
		}

		private List<Keys> randomKeys(int max) {
			var keys = new ArrayList<Keys>();
			int count = 1 + random.nextInt(max);
			for (int i = 0; i < count; i++) {
				keys.add(randomKey().keys());
			}
			return keys;
		}

		private LogEntry randomEntry() {
			int choice = random.nextInt(100);
			if (choice < 2) {
				return new LogEntry.CreateColumn("c", ColumnSchema.of(IntList.of(), ObjectList.of(), true));
			} else if (choice < 4) {
				return new LogEntry.DeleteColumnIfExists("c");
			} else if (choice < 6) {
				return new LogEntry.DeleteColumn(randomColumn());
			} else if (choice < 9) {
				return new LogEntry.UploadMergeOperator("op", "a.B", new byte[0]);
			} else if (choice < 10) {
				return new LogEntry.Genesis("cluster", false);
			} else if (choice < 30) {
				var writes = new ArrayList<LogEntry.Write>();
				int count = 1 + random.nextInt(3);
				for (int i = 0; i < count; i++) {
					writes.add(randomWrite());
				}
				return new LogEntry.Guarded(EPOCH, appliedIndex, new long[0], new long[0], writes, false);
			} else {
				return randomWrite();
			}
		}

		private LogEntry.Write randomWrite() {
			long column = randomColumn();
			var keys = randomKeys(3);
			var values = keys.stream().map(_ -> VALUE).toList();
			Keys single = keys.getFirst();
			if (random.nextInt(30) == 0) {
				return new LogEntry.DeleteRange(column, null, single);
			}
			return switch (random.nextInt(6)) {
				case 0 -> new LogEntry.Put(column, single, VALUE, RequestTypeId.PREVIOUS);
				case 1 -> new LogEntry.Delete(column, single, RequestTypeId.NOTHING);
				case 2 -> new LogEntry.Merge(column, single, VALUE, RequestTypeId.NOTHING);
				case 3 -> new LogEntry.PutMulti(column, keys, values, RequestTypeId.NOTHING);
				case 4 -> new LogEntry.DeleteMulti(column, keys, RequestTypeId.NOTHING);
				default -> new LogEntry.MergeMulti(column, keys, values, RequestTypeId.NOTHING);
			};
		}

		private long randomColumn() {
			return COLUMNS[random.nextInt(COLUMNS.length)];
		}
	}

	private record ReferenceKey(long columnId, Keys keys) {}

	/** What a log entry writes, computed independently of the tracker. */
	private static final class ReferenceEntry {

		static final long PENDING = Long.MAX_VALUE;

		final ConflictTracker.Token token;
		/** Registration order. */
		final int sequence;
		final Set<ReferenceKey> keys = new HashSet<>();
		final Set<Long> wholeColumns = new HashSet<>();
		final boolean allColumns;
		/** Log index, or 0 if the entry failed to append. */
		long index;
		/** Index of the write as conflict checks must see it. */
		long writtenAt = PENDING;

		ReferenceEntry(LogEntry entry, ConflictTracker.Token token, int sequence) {
			this.token = token;
			this.sequence = sequence;
			this.allColumns = entry instanceof LogEntry.CreateColumn || entry instanceof LogEntry.DeleteColumnIfExists;
			if (entry instanceof LogEntry.DeleteColumn deleteColumn) {
				wholeColumns.add(deleteColumn.columnId());
			} else if (entry instanceof LogEntry.Write write) {
				addWrite(write);
			} else if (entry instanceof LogEntry.Guarded guarded) {
				guarded.writes().forEach(this::addWrite);
			}
		}

		private void addWrite(LogEntry.Write write) {
			List<Keys> written = switch (write) {
				case LogEntry.Put put -> List.of(put.keys());
				case LogEntry.Delete delete -> List.of(delete.keys());
				case LogEntry.Merge merge -> List.of(merge.keys());
				case LogEntry.PutMulti putMulti -> putMulti.keys();
				case LogEntry.DeleteMulti deleteMulti -> deleteMulti.keys();
				case LogEntry.MergeMulti mergeMulti -> mergeMulti.keys();
				case LogEntry.DeleteRange deleteRange -> {
					wholeColumns.add(deleteRange.columnId());
					yield List.of();
				}
			};
			written.forEach(keys -> this.keys.add(new ReferenceKey(write.columnId(), keys)));
		}

		boolean touchesAny(List<ReferenceKey> candidates) {
			return allColumns || candidates.stream()
					.anyMatch(key -> keys.contains(key) || wholeColumns.contains(key.columnId()));
		}

		@Override
		public String toString() {
			return "entry " + sequence + " at index " + index;
		}
	}

	private static Keys key(String... segments) {
		return new Keys(Arrays.stream(segments).map(segment -> Buf.wrap(segment.getBytes(UTF_8))).toArray(Buf[]::new));
	}

	private static LogEntry.Put put(long columnId, Keys keys) {
		return new LogEntry.Put(columnId, keys, VALUE, RequestTypeId.NOTHING);
	}

	private static LogEntry.Guarded transaction(long snapshotIndex, long columnId, Keys keys) {
		return new LogEntry.Guarded(EPOCH,
				snapshotIndex,
				new long[] {columnId},
				new long[] {ConflictTracker.keyHash(columnId, keys)},
				List.of(),
				false
		);
	}
}
