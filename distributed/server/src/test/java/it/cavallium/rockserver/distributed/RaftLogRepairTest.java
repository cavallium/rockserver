package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.distributed.RaftLogRepair.Outcome;
import it.cavallium.rockserver.distributed.RaftLogRepair.Report;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.raftlog.segmented.LogSegment;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogFormat;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorage.StartupOption;
import org.apache.ratis.server.storage.StorageImplUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.util.PureJavaCrc32C;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link RaftLogRepair}. The file tests write their segments with Ratis' own {@link SegmentedRaftLogOutputStream} and
 * judge them with its own {@link LogSegment#readSegmentFile}, what a starting node reads a segment with. The cluster
 * tests tear the log of a real node, which then cannot start, and repair it through the command line.
 *
 * <p>The cluster tests depend on what a real torn write guarantees: an entry whose fsync did not finish was never
 * acknowledged by this node. Tearing an entry that the node did acknowledge is another fault, which the repair cannot
 * tell from this one: the leader's match index for that node would stay past the end of its log, so that the leader
 * keeps sending from there and the node never catches up under that leader.</p>
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class RaftLogRepairTest {

	private static final String GROUP = NodeConfig.DEFAULT_GROUP_ID;
	private static final int PREALLOCATED_BYTES = 1 << 20;
	private static final int PAYLOAD_BYTES = 3000;

	private static final List<String> SERVER_ARGS = List.of("--election-timeout-ms", "400:800");
	private static final String COLUMN = "versions";
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext SETUP_CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	private static final RequestContext OP_CONTEXT = RequestContext.latency(Duration.ofSeconds(2));
	private static final Duration RETRY_TIMEOUT = Duration.ofSeconds(30);
	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration CONVERGE_TIMEOUT = Duration.ofSeconds(60);

	@TempDir
	Path dir;

	// ---------------------------------------------------------------------------------------------------------
	// Logs that need nothing

	@Test
	void validLogWithPreallocatedTailIsLeftAlone() throws IOException {
		writeSegment("log_0-9", 0, 9);
		var open = writeOpenSegment(10, 19);
		var before = Files.readAllBytes(open);

		var report = repair();

		assertEquals(Outcome.OK, report.outcome(), report.lines().toString());
		assertArrayEquals(before, Files.readAllBytes(open));
		assertNoLockFile();
	}

	@Test
	void validLogWithoutPaddingIsLeftAlone() throws IOException {
		var open = writeSegment("log_inprogress_0", 0, 9);
		var before = Files.readAllBytes(open);

		assertEquals(Outcome.OK, repair().outcome());
		assertArrayEquals(before, Files.readAllBytes(open));
	}

	@Test
	void logWithoutOpenSegmentIsLeftAlone() throws IOException {
		writeSegment("log_0-9", 0, 9);

		assertEquals(Outcome.OK, repair().outcome());
	}

	@Test
	void openSegmentOfZerosNeedsNothing() throws IOException {
		var open = current().resolve("log_inprogress_0");
		Files.createDirectories(current());
		Files.write(open, new byte[PREALLOCATED_BYTES]);

		assertEquals(Outcome.OK, repair().outcome());
		assertEquals(PREALLOCATED_BYTES, Files.size(open));
	}

	@Test
	void emptyOpenSegmentIsLeftToRatis() throws IOException {
		writeSegment("log_0-9", 0, 9);
		var open = Files.createFile(current().resolve("log_inprogress_10"));

		var dryRun = RaftLogRepair.run(dir, GROUP, true);
		assertEquals(Outcome.OK, dryRun.outcome(), dryRun.lines().toString());
		assertTrue(Files.exists(open), "a dry run changes nothing");

		assertEquals(Outcome.OK, repair().outcome());
		assertTrue(Files.exists(open), "only Ratis deletes segments");
	}

	// ---------------------------------------------------------------------------------------------------------
	// Torn writes

	@Test
	void entryWhoseSecondHalfIsZeroIsDropped() throws IOException {
		writeSegment("log_0-9", 0, 9);
		var open = writeOpenSegment(10, 19);
		long start = entryStart(10, 19);
		long end = start + entrySize(19);
		zero(open, start + (end - start) / 2, end);
		assertThrows(IOException.class, () -> ratisEntries(open), "the node must not start from this file");

		var report = repair();

		assertEquals(Outcome.REPAIRED, report.outcome(), report.lines().toString());
		assertTrue(report.lines().getLast().contains("expected to have index 19"), report.lines().toString());
		assertTrue(report.lines().getLast().contains("at offset " + start), report.lines().toString());
		assertEquals(start, Files.size(open));
		assertEquals(9, ratisEntries(open));
		assertNoLockFile();
		assertEquals(Outcome.OK, repair().outcome(), "a repaired log needs nothing more");
	}

	@Test
	void entryWhoseChecksumEndsInZerosIsDropped() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		long end = start + entrySize(9);
		zero(open, end - 2, end);
		assertThrows(IOException.class, () -> ratisEntries(open));

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(start, Files.size(open));
		assertEquals(9, ratisEntries(open));
	}

	@Test
	void entryWithAZeroSectorInTheMiddleIsDropped() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		zero(open, start + 512, start + 1024);

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(start, Files.size(open));
	}

	@Test
	void onlyEntryOfASegmentLeavesTheHeader() throws IOException {
		var open = writeOpenSegment(5, 5);
		long start = entryStart(5, 5);
		assertEquals(SegmentedRaftLogFormat.getHeaderLength(), start);
		zero(open, start + 100, start + entrySize(5));

		var report = repair();

		assertEquals(Outcome.REPAIRED, report.outcome(), report.lines().toString());
		assertTrue(report.lines().getLast().contains("expected to have index 5"), report.lines().toString());
		assertEquals(start, Files.size(open));
		assertEquals(0, ratisEntries(open), "Ratis deletes an open segment without entries when it loads it");
	}

	/** The entries of a group commit reach the disk in order: the first torn one and all later ones are lost. */
	@Test
	void groupCommitTornInOneOfItsEntriesLosesTheRest() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 7);
		zero(open, start + 100, entryStart(0, 9) + entrySize(9));

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(start, Files.size(open));
		assertEquals(7, ratisEntries(open));
	}

	/** Ratis drops such an entry by itself; the repair does the same, and says so. */
	@Test
	void entryCutByTheEndOfTheFileIsDropped() throws IOException {
		var open = writeSegment("log_inprogress_0", 0, 9);
		long start = entryStart(0, 9);
		truncate(open, start + 100);
		assertEquals(9, ratisEntries(open));

		var report = repair();

		assertEquals(Outcome.REPAIRED, report.outcome(), report.lines().toString());
		assertEquals(start, Files.size(open));
	}

	@Test
	void entrySizeCutByTheEndOfTheFileIsDropped() throws IOException {
		var open = writeSegment("log_inprogress_0", 0, 9);
		long start = entryStart(0, 9);
		truncate(open, start + 1);
		assertThrows(IOException.class, () -> ratisEntries(open));

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(start, Files.size(open));
		assertEquals(9, ratisEntries(open));
	}

	// ---------------------------------------------------------------------------------------------------------
	// What must be refused

	@Test
	void validEntriesAfterAnInvalidOneAreRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 8);
		zero(open, start + 100, start + entrySize(8));

		assertRefused(open, "non-zero data at offset " + entryStart(0, 9));
	}

	@Test
	void nonZeroByteInThePreallocatedTailIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		zero(open, start + 100, start + entrySize(9));
		long garbage = Files.size(open) - 1;
		writeAt(open, garbage, new byte[] {1});

		assertRefused(open, "non-zero data at offset " + garbage);
	}

	/** The first sectors of an entry lost and later ones kept look the same as a damaged log, so this is refused. */
	@Test
	void nonZeroBytesAfterTheEndOfTheLogAreRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		zero(open, start, start + 100);

		assertRefused(open, "a zero byte ends the log here");
	}

	@Test
	void validChecksumOverWhatIsNotAnEntryIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		zero(open, start, start + entrySize(9));
		var junk = new byte[100];
		new Random(1).nextBytes(junk);
		junk[0] = (byte) 0xFF;
		writeAt(open, start, framed(junk));

		assertRefused(open, "does not parse");
	}

	@Test
	void sizeNoEntryCanHaveIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		zero(open, start, start + entrySize(9));
		var size = new byte[5];
		var coded = CodedOutputStream.newInstance(size);
		coded.writeUInt32NoTag(1 << 30);
		coded.flush();
		writeAt(open, start, size);

		assertRefused(open, "exceeds the limit");
	}

	@Test
	void malformedSizeIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		zero(open, start, start + entrySize(9));
		var bad = new byte[12];
		Arrays.fill(bad, (byte) 0xFF);
		writeAt(open, start, bad);

		assertRefused(open, "bad entry size");
	}

	@Test
	void entriesOutOfOrderAreRefused() throws IOException {
		var open = current().resolve("log_inprogress_0");
		Files.createDirectories(current());
		var bytes = new ByteArrayOutputStream();
		var header = new byte[SegmentedRaftLogFormat.getHeaderLength()];
		SegmentedRaftLogFormat.getHeaderBytebuffer().get(header);
		bytes.write(header);
		bytes.write(framed(entry(0).toByteArray()));
		bytes.write(framed(entry(2).toByteArray()));
		bytes.write(new byte[PREALLOCATED_BYTES]);
		Files.write(open, bytes.toByteArray());

		assertRefused(open, "index 2 after index 0");
	}

	@Test
	void openSegmentThatStartsElsewhereIsRefused() throws IOException {
		var open = current().resolve("log_inprogress_7");
		Files.createDirectories(current());
		Files.move(writeOpenSegment(0, 9), open);

		assertRefused(open, "starts with index 0");
	}

	@Test
	void corruptClosedSegmentIsRefusedAndNothingIsRepaired() throws IOException {
		var closed = writeSegment("log_0-9", 0, 9);
		long start = entryStart(0, 9);
		zero(closed, start + 100, start + entrySize(9));
		var open = writeOpenSegment(10, 19);
		long tornStart = entryStart(10, 19);
		zero(open, tornStart + 100, tornStart + entrySize(19));
		var closedBefore = Files.readAllBytes(closed);

		assertRefused(open, "closed segment log_0-9");
		assertArrayEquals(closedBefore, Files.readAllBytes(closed));
	}

	@Test
	void closedSegmentWithFewerEntriesThanItsNameIsRefused() throws IOException {
		var closed = writeSegment("log_0-9", 0, 9);
		truncate(closed, entryStart(0, 9));
		assertEquals(9, ratisEntries(closed), "Ratis itself does not complain while reading it");

		var report = repair();

		assertEquals(Outcome.REFUSED, report.outcome(), report.lines().toString());
		assertTrue(report.lines().getLast().contains("9 entries instead of 10"), report.lines().toString());
	}

	@Test
	void twoOpenSegmentsAreRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		writeOpenSegment(10, 19);

		assertRefused(open, "2 open segments");
	}

	@Test
	void segmentWithoutHeaderIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		zero(open, 0, SegmentedRaftLogFormat.getHeaderLength());

		assertRefused(open, "no size that a torn write could leave");
	}

	@Test
	void logOfARunningNodeIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		zero(open, start + 100, start + entrySize(9));
		var lockFile = current().resolveSibling("in_use.lock");
		try (var channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
				var lock = channel.lock()) {
			assertTrue(lock.isValid());
			assertRefused(open, "stop the node first");
		}
		assertTrue(Files.exists(lockFile), "someone else's lock file stays");
		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertTrue(Files.exists(lockFile), "someone else's lock file stays");
	}

	@Test
	void missingLogIsRefused() throws IOException {
		var report = repair();

		assertEquals(Outcome.REFUSED, report.outcome());
		assertTrue(report.lines().getLast().contains("does not exist"), report.lines().toString());
	}

	// ---------------------------------------------------------------------------------------------------------
	// Dry run

	@Test
	void dryRunChangesNothing() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		zero(open, start + 100, start + entrySize(9));
		var before = Files.readAllBytes(open);

		var report = RaftLogRepair.run(dir, GROUP, true);

		assertEquals(Outcome.WOULD_REPAIR, report.outcome(), report.lines().toString());
		assertTrue(report.lines().getLast().contains("offset " + start), report.lines().toString());
		assertArrayEquals(before, Files.readAllBytes(open));
		assertNoLockFile();

		assertEquals(Outcome.REPAIRED, repair().outcome(), "a dry run tells what the repair does");
		assertEquals(start, Files.size(open));
	}

	@Test
	void dryRunRefusesWhatTheRepairRefuses() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 8);
		zero(open, start + 100, start + entrySize(8));

		assertEquals(Outcome.REFUSED, RaftLogRepair.run(dir, GROUP, true).outcome());
	}

	// ---------------------------------------------------------------------------------------------------------
	// The reader

	/** The whole point of a torn tail: the node reads this far, and this class reads the same. */
	@Test
	void scanOfAValidSegmentAgreesWithRatisAndTheWriter() throws IOException {
		var open = writeOpenSegment(3, 12);

		var scan = RaftLogRepair.scan(Files.readAllBytes(open), RaftSettings.LOG_ENTRY_SIZE_MAX.getSizeInt());

		assertNull(scan.invalid());
		assertEquals(10, scan.entries());
		assertEquals(10, ratisEntries(open));
		assertEquals(3, scan.firstIndex());
		assertEquals(12, scan.lastIndex());
		assertEquals(entryStart(3, 12) + entrySize(12), scan.validEnd());
	}

	/** Cut anywhere, a segment keeps its whole entries, and Ratis reads the same ones. */
	@Test
	void everyCutOfASegmentKeepsItsWholeEntries() throws IOException {
		var open = writeSegment("log_inprogress_0", 0, 2);
		var whole = Files.readAllBytes(open);
		// Not from 0: LogSegmentPath, used to read the segment with Ratis here, deletes an empty one. Every size around
		// the header and the first entry size, then a sample, since Ratis logs a warning for each.
		for (int size = 1; size < whole.length; size += size < 64 ? 1 : 97) {
			Files.write(open, Arrays.copyOf(whole, size));
			var report = repair();
			assertTrue(report.success(), () -> report.lines().toString());
			int entries = 0;
			long kept = SegmentedRaftLogFormat.getHeaderLength();
			while (entries < 3 && kept + entrySize(entries) <= size) {
				kept += entrySize(entries++);
			}
			assertEquals(entries, ratisEntries(open), "cut at " + size);
			assertEquals(Math.min(kept, size), Files.size(open), "cut at " + size);
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Ratis on a repaired log

	@Test
	void ratisAppendsToARepairedSegment() throws IOException {
		formatStorage();
		writeSegment("log_0-9", 0, 9);
		var open = writeOpenSegment(10, 19);
		long start = entryStart(10, 19);
		zero(open, start + 100, start + entrySize(19));
		assertThrows(IOException.class, () -> openRaftLog().close(), "the node does not start from this log");

		assertEquals(Outcome.REPAIRED, repair().outcome());

		assertRaftLogTakesIndex(19);
	}

	/** Ratis deletes the segment and starts a new one at the same index. */
	@Test
	void ratisAppendsAfterARepairThatEmptiedTheSegment() throws IOException {
		formatStorage();
		writeSegment("log_0-9", 0, 9);
		var open = writeOpenSegment(10, 10);
		long start = entryStart(10, 10);
		zero(open, start + 100, start + entrySize(10));
		assertThrows(IOException.class, () -> openRaftLog().close(), "the node does not start from this log");

		assertEquals(Outcome.REPAIRED, repair().outcome());

		assertRaftLogTakesIndex(10);
	}

	/** The index that was dropped is free again, it survives a restart, and what is before it is still there. */
	private void assertRaftLogTakesIndex(long index) throws IOException {
		var replacement = LogEntryProto.newBuilder(entry(index)).setTerm(2).build();
		try (var log = openRaftLog()) {
			assertEquals(index - 1, log.getLastEntryTermIndex().getIndex());
			log.appendEntry(replacement).get(30, TimeUnit.SECONDS);
		} catch (ExecutionException | TimeoutException | InterruptedException e) {
			throw new AssertionError(e);
		}
		try (var log = openRaftLog()) {
			assertEquals(index, log.getLastEntryTermIndex().getIndex());
			assertEquals(replacement, log.get(index));
			assertEquals(entry(index - 1), log.get(index - 1));
		} catch (RaftLogIOException e) {
			throw new AssertionError(e);
		}
	}

	private void formatStorage() throws IOException {
		try (var storage = StorageImplUtils.newRaftStorage(current().getParent().toFile(), SizeInBytes.ONE_MB,
				StartupOption.FORMAT, CorruptionPolicy.EXCEPTION)) {
			storage.initialize();
		}
	}

	/** The Raft log as a starting node opens it, with the corruption policy of the node. Closing it closes both. */
	private RaftLogHandle openRaftLog() throws IOException {
		var storage = StorageImplUtils.newRaftStorage(current().getParent().toFile(), SizeInBytes.ONE_MB,
				StartupOption.RECOVER, CorruptionPolicy.EXCEPTION);
		storage.initialize();
		var log = SegmentedRaftLog.newBuilder()
				.setMemberId(RaftGroupMemberId.valueOf(RaftPeerId.valueOf("node"),
						RaftGroupId.valueOf(UUID.fromString(GROUP))))
				.setStorage(storage)
				.setSnapshotIndexSupplier(() -> RaftLog.INVALID_LOG_INDEX)
				.setProperties(new RaftProperties())
				.build();
		try {
			log.open(RaftLog.INVALID_LOG_INDEX, null);
		} catch (IOException | RuntimeException e) {
			storage.close();
			throw e;
		}
		return new RaftLogHandle(log, storage);
	}

	private record RaftLogHandle(SegmentedRaftLog log, RaftStorage storage) implements AutoCloseable {

		TermIndex getLastEntryTermIndex() {
			return log.getLastEntryTermIndex();
		}

		CompletableFuture<Long> appendEntry(LogEntryProto entry) {
			return log.appendEntry(entry);
		}

		LogEntryProto get(long index) throws RaftLogIOException {
			return log.get(index);
		}

		@Override
		public void close() throws IOException {
			try {
				log.close();
			} finally {
				storage.close();
			}
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// A real node

	/**
	 * The power of the leader goes out with writes in flight. An entry torn on the leader can be anything from an entry
	 * of a write not yet sent to one that a majority has: dropping it loses nothing, since a majority synced every
	 * acknowledged entry, and the new leader, elected by the only other member up, has all of them.
	 */
	@Test
	void leaderWithATornEntryRejoinsAfterTheRepair() throws Exception {
		try (var cluster = ProcessCluster.start(dir, 3, SERVER_ARGS); var raft = new RaftProbe(cluster)) {
			cluster.waitReady(READY_TIMEOUT);
			try (var writer = new Writer(client(cluster), createColumn(cluster))) {
				writer.awaitProgress(100);
				var victim = raft.leader(cluster.nodeIds());
				cluster.kill(victim);
				writer.awaitProgress(50);

				long dropped = tearRepairAndRestart(cluster, victim);

				writer.awaitProgress(50);
				assertConverged(cluster, raft, victim, dropped, writer.stop());
			}
		}
	}

	/**
	 * The power of a follower goes out after the fsync of an entry and before its answer reaches the leader, the same
	 * state as a torn write, which is never acknowledged either: the replies of the follower get lost from when the logs
	 * are equal, so that the leader never hears of any entry written after that.
	 */
	@Test
	void followerWithATornEntryItNeverAcknowledgedRejoinsAfterTheRepair() throws Exception {
		try (var cluster = ProcessCluster.start(dir, 3, SERVER_ARGS); var raft = new RaftProbe(cluster);
				var client = client(cluster)) {
			cluster.waitReady(READY_TIMEOUT);
			long columnId = createColumn(cluster);
			long version = 0;
			while (version < 100) {
				put(client, columnId, ++version);
			}
			var leader = raft.leader(cluster.nodeIds());
			var victim = raft.follower(cluster.nodeIds(), leader);
			long acknowledged = raft.awaitSameLog(leader, victim);
			cluster.network().dropReplies(leader, victim);

			for (int i = 0; i < 5; i++) {
				put(client, columnId, ++version);
			}
			await(() -> raft.lastEntry(victim) > acknowledged, CONVERGE_TIMEOUT, "the follower to get an entry");
			cluster.kill(victim);
			cluster.network().heal();
			put(client, columnId, ++version);

			long dropped = tearRepairAndRestart(cluster, victim);
			assertTrue(dropped > acknowledged, dropped + " should not have been acknowledged, as " + acknowledged + " was");

			for (int i = 0; i < 5; i++) {
				put(client, columnId, ++version);
			}
			assertConverged(cluster, raft, victim, dropped, version);
		}
	}

	/**
	 * Tears the last entry of the open segment of a killed node, and checks that the node then refuses to start and
	 * that it starts after a repair.
	 *
	 * @return the index of the torn entry
	 */
	private static long tearRepairAndRestart(ProcessCluster cluster, String victim) throws Exception {
		var log = cluster.dataDir(victim).resolve("raft").resolve(GROUP).resolve("current");
		var open = onlyOpenSegment(log);
		var entries = readEntries(open);
		var last = entries.getLast();
		long lastEnd = SegmentedRaftLogFormat.getHeaderLength()
				+ entries.stream().mapToLong(RaftLogRepairTest::entrySize).sum();
		long lastStart = lastEnd - entrySize(last);
		assertTrue(Files.size(open) > lastEnd, "preallocated tail expected in a node that did not stop");
		zero(open, lastStart + (lastEnd - lastStart) / 2, lastEnd);

		cluster.start(victim);
		var refused = assertThrows(IllegalStateException.class, () -> cluster.waitReady(victim, READY_TIMEOUT));
		assertTrue(refused.getMessage().contains("before printing READY"), refused.getMessage());
		assertTrue(cluster.output(victim).contains("Log entry corrupted"), cluster.logTail(victim));

		var running = cluster.nodeIds().stream().filter(id -> !id.equals(victim)).findFirst().orElseThrow();
		var locked = cluster.repairLog(running, "--dry-run");
		assertEquals(1, locked.exitCode(), locked.output());
		assertTrue(locked.output().contains("stop the node first"), locked.output());

		var before = Files.readAllBytes(open);
		var dryRun = cluster.repairLog(victim, "--dry-run");
		assertEquals(0, dryRun.exitCode(), dryRun.output());
		assertTrue(dryRun.output().contains("WOULD REPAIR"), dryRun.output());
		assertArrayEquals(before, Files.readAllBytes(open));

		var repaired = cluster.repairLog(victim);
		assertEquals(0, repaired.exitCode(), repaired.output());
		assertTrue(repaired.output().contains("REPAIRED"), repaired.output());
		assertTrue(repaired.output().contains("index " + last.getIndex()), repaired.output());
		assertEquals(lastStart, Files.size(open));
		assertEquals(entries.size() - 1, readEntries(open).size());

		cluster.start(victim);
		cluster.waitReady(victim, READY_TIMEOUT);
		return last.getIndex();
	}

	/** Waits for every node to apply the whole log, stops them and compares their databases to the acknowledged puts. */
	private static void assertConverged(ProcessCluster cluster, RaftProbe raft, String victim, long dropped,
			long acknowledged) throws Exception {
		try {
			raft.awaitAllApplied(cluster.nodeIds(), CONVERGE_TIMEOUT);
		} catch (AssertionError e) {
			throw new AssertionError(raft.describe(cluster.nodeIds()) + "\n" + cluster.logTail(victim), e);
		}
		assertTrue(raft.applied(victim) > dropped, "the node has the dropped index again");

		for (var id : cluster.nodeIds()) {
			cluster.stop(id);
		}
		var expected = new TreeMap<String, String>();
		for (long version = 1; version <= acknowledged; version++) {
			expected.put(hex(key(version).keys()[0]), hex(value(version)));
		}
		for (var id : cluster.nodeIds()) {
			assertEquals(expected, replicaContents(cluster, id), "replica " + id);
		}
	}

	private static long createColumn(ProcessCluster cluster) {
		try (var setup = client(cluster)) {
			var column = setup.createColumn(COLUMN, SCHEMA, SETUP_CONTEXT);
			assertTrue(column.acknowledged(), () -> "createColumn failed: " + column.error());
			return column.value();
		}
	}

	private static ClusterClient client(ProcessCluster cluster) {
		return new ClusterClient(cluster.apiAddresses(), RETRY_TIMEOUT, true);
	}

	private static void put(ClusterClient client, long columnId, long version) {
		var result = client.put(columnId, key(version), value(version), OP_CONTEXT);
		assertTrue(result.acknowledged(), () -> "put of version " + version + ": " + result.error());
	}

	// ---------------------------------------------------------------------------------------------------------
	// Helpers

	private Report repair() throws IOException {
		return RaftLogRepair.run(dir, GROUP, false);
	}

	private void assertRefused(Path file, String reason) throws IOException {
		var before = Files.readAllBytes(file);
		var report = repair();
		assertEquals(Outcome.REFUSED, report.outcome(), report.lines().toString());
		assertTrue(String.join("\n", report.lines()).contains(reason), report.lines().toString());
		assertArrayEquals(before, Files.readAllBytes(file));
	}

	private void assertNoLockFile() {
		assertFalse(Files.exists(current().resolveSibling("in_use.lock")));
	}

	private Path current() {
		return dir.resolve("raft").resolve(GROUP).resolve("current");
	}

	private Path writeSegment(String name, long first, long last) throws IOException {
		Files.createDirectories(current());
		var file = current().resolve(name);
		try (var out = new SegmentedRaftLogOutputStream(file.toFile(), false, 1L << 30, PREALLOCATED_BYTES,
				ByteBuffer.allocateDirect(1 << 20))) {
			for (long index = first; index <= last; index++) {
				out.write(entry(index));
			}
		}
		return file;
	}

	/** An open segment as a stopped node leaves it: Ratis cuts the preallocated tail only when it closes the file. */
	private Path writeOpenSegment(long first, long last) throws IOException {
		var file = writeSegment("log_inprogress_" + first, first, last);
		Files.write(file, new byte[PREALLOCATED_BYTES], StandardOpenOption.APPEND);
		return file;
	}

	private static LogEntryProto entry(long index) {
		var payload = new byte[PAYLOAD_BYTES];
		new Random(index).nextBytes(payload);
		return LogEntryProto.newBuilder()
				.setTerm(1)
				.setIndex(index)
				.setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(payload)))
				.build();
	}

	private static long entrySize(long index) {
		return entrySize(entry(index));
	}

	private static long entrySize(LogEntryProto entry) {
		int size = entry.getSerializedSize();
		return CodedOutputStream.computeUInt32SizeNoTag(size) + size + Integer.BYTES;
	}

	/** Offset of entry {@code index} in a segment that starts with entry {@code first}. */
	private static long entryStart(long first, long index) {
		long offset = SegmentedRaftLogFormat.getHeaderLength();
		for (long i = first; i < index; i++) {
			offset += entrySize(i);
		}
		return offset;
	}

	/** The bytes of an entry as the output stream writes them, around a proto of any content. */
	private static byte[] framed(byte[] proto) throws IOException {
		var bytes = new ByteArrayOutputStream();
		var coded = CodedOutputStream.newInstance(bytes);
		coded.writeUInt32NoTag(proto.length);
		coded.writeRawBytes(proto);
		coded.flush();
		var checksum = new PureJavaCrc32C();
		checksum.update(bytes.toByteArray(), 0, bytes.size());
		bytes.write(ByteBuffer.allocate(Integer.BYTES).putInt((int) checksum.getValue()).array());
		return bytes.toByteArray();
	}

	private static void zero(Path file, long from, long to) throws IOException {
		writeAt(file, from, new byte[Math.toIntExact(to - from)]);
	}

	private static void writeAt(Path file, long offset, byte[] bytes) throws IOException {
		try (var channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
			channel.write(ByteBuffer.wrap(bytes), offset);
		}
	}

	private static void truncate(Path file, long size) throws IOException {
		try (var channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
			channel.truncate(size);
		}
	}

	private static int ratisEntries(Path file) throws IOException {
		return readEntries(file).size();
	}

	/** What a starting node reads from the segment, or the error that stops it. */
	private static List<LogEntryProto> readEntries(Path file) throws IOException {
		var entries = new ArrayList<LogEntryProto>();
		LogSegment.readSegmentFile(file.toFile(), LogSegmentPath.matchLogSegment(file).getStartEnd(),
				RaftSettings.LOG_ENTRY_SIZE_MAX, CorruptionPolicy.EXCEPTION, null, entries::add);
		return entries;
	}

	private static Path onlyOpenSegment(Path log) throws IOException {
		try (var files = Files.list(log)) {
			var open = files.filter(file -> file.getFileName().toString().startsWith("log_inprogress_")).toList();
			assertEquals(1, open.size(), open.toString());
			return open.getFirst();
		}
	}

	private static Keys key(long version) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(version).array()));
	}

	private static Buf value(long version) {
		return Buf.wrap(("v" + version).getBytes(StandardCharsets.UTF_8));
	}

	private static String hex(Buf... parts) {
		var hex = new StringBuilder();
		for (var part : parts) {
			hex.append(HexFormat.of().formatHex(part.toByteArray())).append('/');
		}
		return hex.toString();
	}

	/** The keys and values of a stopped node's database. */
	private static TreeMap<String, String> replicaContents(ProcessCluster cluster, String id) throws IOException {
		var entries = new TreeMap<String, String>();
		try (var db = new EmbeddedConnection(cluster.dataDir(id).resolve("db"), "replica-" + id, null)) {
			var api = db.getSyncApi(SETUP_CONTEXT);
			long columnId = api.getColumnId(COLUMN);
			try (Stream<KV> range = api.getRange(0, columnId, null, null, false, RequestType.<KV>allInRange())) {
				range.forEach(kv -> entries.put(hex(kv.keys().keys()), hex(kv.value())));
			}
		}
		return entries;
	}

	private static void await(BooleanSupplier condition, Duration timeout, String what) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadline > 0) {
				throw new AssertionError("Timed out after " + timeout + " waiting for " + what);
			}
			Thread.sleep(20);
		}
	}

	/** Keeps doing puts of one key per version, so that a retried put is harmless. */
	private static final class Writer implements AutoCloseable {
		private final ClusterClient client;
		private final long columnId;
		private final AtomicLong acknowledged = new AtomicLong();
		private final Thread thread;
		private volatile boolean running = true;
		private volatile String failure;

		Writer(ClusterClient client, long columnId) {
			this.client = client;
			this.columnId = columnId;
			this.thread = Thread.ofPlatform().name("writer").start(this::write);
		}

		private void write() {
			for (long version = 1; running; version++) {
				var result = client.put(columnId, key(version), value(version), OP_CONTEXT);
				if (!result.acknowledged()) {
					failure = "put of version " + version + ": " + result.error();
					return;
				}
				acknowledged.set(version);
			}
		}

		void awaitProgress(int puts) throws InterruptedException {
			long target = acknowledged.get() + puts;
			await(() -> acknowledged.get() >= target || failure != null, RETRY_TIMEOUT.plusSeconds(30), "puts");
			assertNull(failure);
		}

		/** Stops the writer and returns how many puts were acknowledged: they are versions 1 to that. */
		long stop() throws InterruptedException {
			running = false;
			thread.join(RETRY_TIMEOUT.plusSeconds(30));
			assertFalse(thread.isAlive(), "the writer did not stop");
			assertNull(failure);
			return acknowledged.get();
		}

		@Override
		public void close() throws InterruptedException {
			running = false;
			thread.join(RETRY_TIMEOUT.plusSeconds(30));
			client.close();
		}
	}

	/** Queries the nodes' Raft group info directly on their Raft ports, without retries. */
	private static final class RaftProbe implements AutoCloseable {
		private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString(GROUP));
		private final RaftClient client;

		RaftProbe(ProcessCluster cluster) {
			var peers = cluster.nodeIds().stream().map(id -> {
				var address = cluster.raftAddress(id);
				return RaftPeer.newBuilder().setId(id).setAddress(address.host() + ":" + address.port()).build();
			}).toList();
			var properties = new RaftProperties();
			RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
			this.client = RaftClient.newBuilder()
					.setClientId(ClientId.randomId())
					.setRaftGroup(RaftGroup.valueOf(groupId, peers))
					.setProperties(properties)
					.setRetryPolicy(RetryPolicies.noRetry())
					.build();
		}

		private Optional<GroupInfoReply> info(String id) {
			try {
				return Optional.of(client.getGroupManagementApi(RaftPeerId.valueOf(id)).info(groupId));
			} catch (IOException | RuntimeException unreachable) {
				return Optional.empty();
			}
		}

		private Optional<String> findLeader(List<String> ids) {
			return ids.stream()
					.filter(id -> info(id).filter(i -> i.getRoleInfoProto().getRole() == RaftPeerRole.LEADER).isPresent())
					.findFirst();
		}

		String leader(List<String> ids) throws InterruptedException {
			await(() -> findLeader(ids).isPresent(), CONVERGE_TIMEOUT, "a leader");
			return findLeader(ids).orElseThrow();
		}

		String follower(List<String> ids, String leader) {
			return ids.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
		}

		long applied(String id) {
			return info(id).map(info -> info.getLogInfoProto().getApplied().getIndex()).orElse(-1L);
		}

		long lastEntry(String id) {
			return info(id).map(info -> info.getLogInfoProto().getLastEntry().getIndex()).orElse(-1L);
		}

		/** Waits until two nodes have the same last entry (call without load) and returns its index. */
		long awaitSameLog(String a, String b) throws InterruptedException {
			await(() -> lastEntry(a) == lastEntry(b), CONVERGE_TIMEOUT, "the logs of " + a + " and " + b + " to match");
			return lastEntry(a);
		}

		/** Role and log positions of every node, for failure messages. */
		String describe(List<String> ids) {
			var description = new StringBuilder();
			for (var id : ids) {
				description.append(id).append(": ").append(info(id).map(info -> info.getRoleInfoProto().getRole() + " "
						+ info.getLogInfoProto()).orElse("unreachable")).append('\n');
			}
			return description.toString();
		}

		/** Waits until every node applied the leader's whole log (call without load). */
		void awaitAllApplied(List<String> ids, Duration timeout) throws InterruptedException {
			await(() -> {
				var leader = findLeader(ids).flatMap(this::info);
				if (leader.isEmpty()) {
					return false;
				}
				long last = leader.get().getLogInfoProto().getLastEntry().getIndex();
				return ids.stream().allMatch(id -> applied(id) >= last);
			}, timeout, "every node to apply the whole log");
		}

		@Override
		public void close() throws IOException {
			client.close();
		}
	}
}
