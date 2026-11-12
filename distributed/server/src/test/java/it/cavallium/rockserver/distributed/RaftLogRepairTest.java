package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
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
import java.io.UncheckedIOException;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.regex.Pattern;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.MetadataProto;
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@link RaftLogRepair}. The file tests write their segments with Ratis' own {@link SegmentedRaftLogOutputStream} and
 * judge them with its own {@link LogSegment#readSegmentFile}, what a starting node reads a segment with. The cluster
 * tests tear the log of a real node, which then cannot start, and repair it through the command line.
 *
 * <p>A device loses whole sectors, so every tear here is aligned to {@value #SECTOR} or {@value #PAGE} bytes in the
 * file, and the tests of a damaged entry check that anything else is refused.</p>
 *
 * <p>The cluster tests depend on what a real torn write guarantees: an entry whose fsync did not finish was never
 * acknowledged by this node. Tearing an entry that the node did acknowledge is another fault, which the repair cannot
 * tell from this one and which it does not make safe: it removes that node's copy from the quorum that committed the
 * entry, and a leader elected without the entry loses an acknowledged write with no error. While the leader that heard
 * the acknowledgement lives, the node stays stuck behind that leader's match index, but after a power cut of the leader
 * too nothing shows. So these tests tear only entries that the node never acknowledged, or that two other live nodes
 * hold.</p>
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
@Tag("slow")
class RaftLogRepairTest {

	private static final String GROUP = NodeConfig.DEFAULT_GROUP_ID;
	private static final int PREALLOCATED_BYTES = 1 << 20;
	private static final int PAYLOAD_BYTES = 3000;
	private static final int SECTOR = 512;
	private static final int PAGE = 4096;
	/** More than a sector, so that every entry of a put can be torn at a sector boundary. */
	private static final int VALUE_BYTES = 1100;
	private static final String ASSUMPTION = "never fsynced";

	private static final List<String> SERVER_ARGS = List.of("--election-timeout-ms", "400:800");
	/** Long enough for a leader that gets no replies to outlast a test that wants it to stay the leader meanwhile. */
	private static final List<String> SLOW_ELECTION_ARGS = List.of("--election-timeout-ms", "2000:4000");
	/** Segments of about 50 puts, and snapshots often enough for the purge gap to be what holds the log back. */
	private static final List<String> SMALL_LOG_ARGS = List.of("--election-timeout-ms", "400:800",
			"--snapshot-every", "200",
			"--log-segment-bytes", String.valueOf(1 << 16));
	private static final String COLUMN = "versions";
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext SETUP_CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	private static final RequestContext OP_CONTEXT = RequestContext.latency(Duration.ofSeconds(2));
	private static final Duration RETRY_TIMEOUT = Duration.ofSeconds(30);
	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration CONVERGE_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration PURGE_TIMEOUT = Duration.ofMinutes(3);

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
	void entryTornAtASectorBoundaryIsDropped() throws IOException {
		writeSegment("log_0-9", 0, 9);
		var open = writeOpenSegment(10, 19);
		long start = entryStart(10, 19);
		tear(open, start, start + entrySize(19));
		assertThrows(IOException.class, () -> ratisEntries(open), "the node must not start from this file");

		var report = repair();

		assertEquals(Outcome.REPAIRED, report.outcome(), text(report));
		assertTrue(text(report).contains("expected to have index 19"), text(report));
		assertTrue(text(report).contains("at offset " + start), text(report));
		assertEquals(start, Files.size(open));
		assertEquals(9, ratisEntries(open));
		assertEquals(Outcome.OK, repair().outcome(), "a repaired log needs nothing more");
	}

	/**
	 * R47-1: a start that fails because Ratis cannot read the log (a torn or a damaged entry: the bytes do not tell
	 * which, {@code repair-log} decides) prints how to run {@code repair-log} on this node, whatever wraps the error;
	 * another failure of the start does not.
	 */
	@Test
	void aStartThatCannotReadTheLogPointsToRepairLog() throws IOException {
		writeSegment("log_0-9", 0, 9);
		var open = writeOpenSegment(10, 19);
		long start = entryStart(10, 19);
		tear(open, start, start + entrySize(19));
		var torn = assertThrows(IOException.class, () -> ratisEntries(open));
		var data = dir.resolve("data");
		var wrapped = new IOException("Failed to start", new java.util.concurrent.CompletionException(torn));
		var hint = ReplicatedServerMain.unreadableLogHint(wrapped, data, GROUP);
		assertTrue(hint != null && hint.contains("repair-log --data-dir " + data + " --dry-run"), String.valueOf(hint));
		assertFalse(hint.contains("--group-id"), "the default group needs no --group-id: " + hint);
		var group = UUID.randomUUID().toString();
		var other = ReplicatedServerMain.unreadableLogHint(wrapped, data, group);
		assertTrue(other != null && other.contains("repair-log --data-dir " + data + " --group-id " + group + " --dry-run"),
				String.valueOf(other));

		var damaged = writeSegment("log_20-29", 20, 29);
		flipBit(damaged, entryStart(20, 25) + 100, 0x01);
		var refused = assertThrows(IOException.class, () -> ratisEntries(damaged));
		assertTrue(ReplicatedServerMain.unreadableLogHint(refused, data, GROUP) != null, "damage reads the same way");

		assertNull(ReplicatedServerMain.unreadableLogHint(new java.net.BindException("Address already in use"), data, GROUP));
		assertNull(ReplicatedServerMain.unreadableLogHint(new IllegalStateException("refused", new IOException("disk")),
				data, GROUP));
	}

	@Test
	void entryTornAtAPageBoundaryIsDropped() throws IOException {
		var open = writeOpenSegment(0, 1);
		long start = entryStart(0, 1);
		long end = start + entrySize(1);
		assertTrue(alignedAfter(start, PAGE) < end, "the entry should cross a page");
		zero(open, alignedAfter(start, PAGE), end);
		assertThrows(IOException.class, () -> ratisEntries(open));

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(start, Files.size(open));
		assertEquals(1, ratisEntries(open));
	}

	/** The sector the entry ends in, only part of which the entry fills: the rest of it is a preallocated tail. */
	@Test
	void entryThatLostOnlyTheSectorItEndsInIsDropped() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		long end = start + entrySize(9);
		long lastSector = (end - 1) / SECTOR * SECTOR;
		assertTrue(lastSector > start && lastSector + SECTOR > end, "the entry should end inside a sector");
		zero(open, lastSector, end);
		assertThrows(IOException.class, () -> ratisEntries(open));

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(start, Files.size(open));
		assertEquals(9, ratisEntries(open));
	}

	/** Sectors of one write can reach the disk in any order. */
	@Test
	void entryWithAZeroSectorInTheMiddleIsDropped() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		long sector = alignedAfter(start, SECTOR);
		assertTrue(sector + SECTOR < start + entrySize(9));
		zero(open, sector, sector + SECTOR);

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(start, Files.size(open));
	}

	@Test
	void onlyEntryOfASegmentLeavesTheHeader() throws IOException {
		var open = writeOpenSegment(5, 5);
		long start = entryStart(5, 5);
		assertEquals(SegmentedRaftLogFormat.getHeaderLength(), start);
		tear(open, start, start + entrySize(5));

		var report = repair();

		assertEquals(Outcome.REPAIRED, report.outcome(), text(report));
		assertTrue(text(report).contains("expected to have index 5"), text(report));
		assertEquals(start, Files.size(open));
		assertEquals(0, ratisEntries(open), "Ratis deletes an open segment without entries when it loads it");
	}

	/** The entries of a group commit reach the disk in order: the first torn one and all later ones are lost. */
	@Test
	void groupCommitTornInOneOfItsEntriesLosesTheRest() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 7);
		tear(open, start, entryStart(0, 9) + entrySize(9));

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(start, Files.size(open));
		assertEquals(7, ratisEntries(open));
	}

	@Test
	void repairCountsTheBytesItDropsAndStatesWhatItAssumes() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		tear(open, start, start + entrySize(9));
		var bytes = Files.readAllBytes(open);
		long nonZero = 0;
		for (int i = (int) start; i < bytes.length; i++) {
			nonZero += bytes[i] != 0 ? 1 : 0;
		}
		var expected = (bytes.length - start) + " bytes from there to the end of the file, " + nonZero + " of them not zero";

		var dryRun = RaftLogRepair.run(dir, GROUP, true);
		assertTrue(text(dryRun).contains(expected), text(dryRun));
		assertTrue(text(dryRun).contains(ASSUMPTION), text(dryRun));

		var report = repair();
		assertTrue(text(report).contains(expected), text(report));
		assertTrue(text(report).contains(ASSUMPTION), text(report));
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
		tear(open, start, start + entrySize(8));

		assertRefused(open, "non-zero data at offset " + entryStart(0, 9));
	}

	/** Every byte of the entry is on disk, so a torn write cannot explain its checksum: this is damaged media. */
	@Test
	void bitFlipInACompleteLastEntryIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		long end = start + entrySize(9);
		for (long offset : new long[] {start + 100, (start + end) / 2, end - 1}) {
			flipBit(open, offset, 0x01);
			assertRefused(open, "no sector of it");
			flipBit(open, offset, 0x01);
		}
	}

	@Test
	void bitFlipInALastEntryThatEndsTheFileIsRefused() throws IOException {
		var open = writeSegment("log_inprogress_0", 0, 9);
		flipBit(open, entryStart(0, 9) + 100, 0x01);

		assertRefused(open, "no sector of it");
	}

	/** The zeros after the last entry look like a lost sector of it, but its first fields still give its real size. */
	@ParameterizedTest
	@ValueSource(longs = {0, 9})
	void sizeInflatedOverTheZerosIsRefused(long index) throws IOException {
		var open = writeOpenSegment(0, index);
		long start = entryStart(0, index);
		assertEquals(2, CodedOutputStream.computeUInt32SizeNoTag(entry(index).getSerializedSize()));
		assertEquals(0, entry(index).getSerializedSize() & (0x20 << (Byte.SIZE - 1)), "the flip must make the size larger");
		flipBit(open, start + 1, 0x20);

		assertRefused(open, "the fields at its start");
	}

	@Test
	void sizeOfAMetadataEntryInflatedOverTheZerosIsRefused() throws IOException {
		var entry = LogEntryProto.newBuilder()
				.setTerm(1)
				.setIndex(1)
				.setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(1))
				.build();
		var open = writeOpenSegmentEndingAt(SECTOR - 2, entry);
		flipBit(open, SECTOR - 2 - entrySize(entry), 0x40);

		assertRefused(open, "the fields at its start");
	}

	/** The fields that give the size of the proto, cut in the middle of the size of the body by a lost sector. */
	@Test
	void entryTornInsideTheFieldsAtItsStartIsDropped() throws IOException {
		int fieldsBeforeBodySize = 7;
		var entry = entry(1);
		long start = SECTOR - fieldsBeforeBodySize - 1;
		var open = writeOpenSegmentEndingAt(start + entrySize(entry), entry);
		zero(open, SECTOR, start + entrySize(entry));

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(start, Files.size(open));
	}

	/** Only the end of the checksum is lost, which leaves the entry whole: the bytes of the checksum before it match. */
	@ParameterizedTest
	@ValueSource(ints = {1, 2, 3, 4})
	void entryThatLostOnlyPartOfItsChecksumIsDropped(int lost) throws IOException {
		var entry = entry(1);
		long end = 2L * PAGE + lost;
		var open = writeOpenSegmentEndingAt(end, entry);
		zero(open, end - lost, end);

		assertEquals(Outcome.REPAIRED, repair().outcome());
		assertEquals(end - entrySize(entry), Files.size(open));
	}

	/** Zeros at the end of a checksum, in a sector of their own, do not excuse damage to the entry. */
	@Test
	void damagedEntryWhoseChecksumEndsInAZeroAloneInASectorIsRefused() throws IOException {
		var entry = LongStream.range(1, 1 << 16).mapToObj(RaftLogRepairTest::entry)
				.filter(candidate -> (crc32c(candidate) & 0xFF) == 0)
				.findFirst().orElseThrow();
		long end = 2L * PAGE + 1;
		var open = writeOpenSegmentEndingAt(end, entry);
		flipBit(open, end - entrySize(entry) + 100, 0x01);

		assertRefused(open, "no sector of it");
	}

	/** A write of less than a sector is either on disk or not. */
	@Test
	void damagedEntryInsideOneSectorIsRefused() throws IOException {
		var bytes = new ByteArrayOutputStream();
		bytes.write(header());
		bytes.write(framed(entry(0, 100).toByteArray()));
		bytes.write(new byte[PREALLOCATED_BYTES]);
		Files.createDirectories(current());
		var open = Files.write(current().resolve("log_inprogress_0"), bytes.toByteArray());
		flipBit(open, SegmentedRaftLogFormat.getHeaderLength() + 50L, 0x01);

		assertRefused(open, "no sector of it");
	}

	/** As many zeros as a sector holds, but in no sector: no device loses these. */
	@Test
	void zerosThatAreNotAlignedToASectorAreRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long from = alignedAfter(entryStart(0, 9), SECTOR) + 100;
		zero(open, from, from + SECTOR);

		assertRefused(open, "no sector of it");
	}

	/**
	 * Damage to the size of an entry makes it claim the next one, whole and valid, and the zeros after that, so that
	 * only the bytes inside it show that this is no torn write, if the size of its body too agrees with that.
	 */
	@Test
	void sizeInflatedOverAValidEntryIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 8);
		long next = entryStart(0, 9);
		int inflation = 0x20 << (Byte.SIZE - 1);
		assertEquals(2, CodedOutputStream.computeUInt32SizeNoTag(entry(8).getSerializedSize()));
		assertEquals(0, entry(8).getSerializedSize() & inflation, "the flip must make the size larger");
		assertTrue(start + entrySize(8) + inflation > next + entrySize(9), "the size must reach past the next entry");
		flipBit(open, start + 1, 0x20);
		assertThrows(IOException.class, () -> ratisEntries(open));
		assertRefused(open, "the fields at its start");

		int bodySizeSecondByte = 2 + 2 + 2 + 1 + 1;
		flipBit(open, start + bodySizeSecondByte, 0x20);
		assertRefused(open, "a frame with a valid checksum at offset " + next);
	}

	@Test
	void nonZeroByteInThePreallocatedTailIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		tear(open, start, start + entrySize(9));
		long garbage = Files.size(open) - 1;
		writeAt(open, garbage, new byte[] {1});

		assertRefused(open, "non-zero data at offset " + garbage);
	}

	/** The first sectors of an entry lost and later ones kept look the same as a damaged log, so this is refused. */
	@Test
	void nonZeroBytesAfterTheEndOfTheLogAreRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 9);
		zero(open, start, alignedAfter(start, SECTOR));

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
		bytes.write(header());
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
		tear(closed, start, start + entrySize(9));
		var open = writeOpenSegment(10, 19);
		long tornStart = entryStart(10, 19);
		tear(open, tornStart, tornStart + entrySize(19));
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
	void gapBeforeTheOpenSegmentIsRefused() throws IOException {
		formatStorage();
		writeSegment("log_0-9", 0, 9);
		var open = writeOpenSegment(15, 19);
		long start = entryStart(15, 19);
		tear(open, start, start + entrySize(19));

		assertRefused(open, "gap between segments log_0-9 and log_inprogress_15");
		truncate(open, start);
		assertRatisRefusesTheLog();
	}

	@Test
	void overlapWithTheOpenSegmentIsRefused() throws IOException {
		formatStorage();
		writeSegment("log_0-9", 0, 9);
		var open = writeOpenSegment(5, 14);
		long start = entryStart(5, 14);
		tear(open, start, start + entrySize(14));

		assertRefused(open, "overlap between segments log_0-9 and log_inprogress_5");
		truncate(open, start);
		assertRatisRefusesTheLog();
	}

	@Test
	void gapBetweenClosedSegmentsIsRefused() throws IOException {
		formatStorage();
		writeSegment("log_0-9", 0, 9);
		var after = writeSegment("log_20-29", 20, 29);
		var open = writeOpenSegment(30, 39);

		assertRefused(after, "gap between segments log_0-9 and log_20-29");
		assertRatisRefusesTheLog();
		assertTrue(Files.exists(open));
	}

	@Test
	void overlapBetweenClosedSegmentsIsRefused() throws IOException {
		formatStorage();
		writeSegment("log_0-9", 0, 9);
		var after = writeSegment("log_5-14", 5, 14);
		writeOpenSegment(15, 24);

		assertRefused(after, "overlap between segments log_0-9 and log_5-14");
		assertRatisRefusesTheLog();
	}

	@Test
	void openSegmentBeforeAClosedOneIsRefused() throws IOException {
		var open = writeOpenSegment(0, 9);
		writeSegment("log_10-19", 10, 19);

		assertRefused(open, "not the last segment");
	}

	/** Purging removes segments from the start of the log. */
	@Test
	void logThatStartsAfterAPurgeIsNotAGap() throws IOException {
		writeSegment("log_20-29", 20, 29);
		var open = writeOpenSegment(30, 39);
		long start = entryStart(30, 39);
		tear(open, start, start + entrySize(39));

		var report = repair();

		assertEquals(Outcome.REPAIRED, report.outcome(), text(report));
		assertEquals(start, Files.size(open));
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
		tear(open, start, start + entrySize(9));
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

	/** As after a killed node, which Ratis accepts: deleting the file could leave two processes with a lock. */
	@Test
	void theLockFileIsNeverDeleted() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 8);
		tear(open, start, start + entrySize(8));

		assertRefused(open, "non-zero data");

		assertTrue(Files.exists(current().resolveSibling("in_use.lock")));
	}

	@Test
	void lockFileOfAKilledNodeStaysAfterARefusal() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 8);
		tear(open, start, start + entrySize(8));
		var lockFile = Files.createFile(current().resolveSibling("in_use.lock"));

		assertRefused(open, "non-zero data");

		assertTrue(Files.exists(lockFile));
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
		tear(open, start, start + entrySize(9));
		var before = Files.readAllBytes(open);

		var report = RaftLogRepair.run(dir, GROUP, true);

		assertEquals(Outcome.WOULD_REPAIR, report.outcome(), text(report));
		assertTrue(text(report).contains("offset " + start), text(report));
		assertArrayEquals(before, Files.readAllBytes(open));
		assertNoLockFile();

		assertEquals(Outcome.REPAIRED, repair().outcome(), "a dry run tells what the repair does");
		assertEquals(start, Files.size(open));
	}

	@Test
	void dryRunRefusesWhatTheRepairRefuses() throws IOException {
		var open = writeOpenSegment(0, 9);
		long start = entryStart(0, 8);
		tear(open, start, start + entrySize(8));

		assertEquals(Outcome.REFUSED, RaftLogRepair.run(dir, GROUP, true).outcome());
	}

	// ---------------------------------------------------------------------------------------------------------
	// The verdict an operator reads (runbooks.md, "Node does not start after a power loss", step 2)

	/**
	 * R48-2: every run prints exactly one verdict line, the one that starts with {@code OK: }, {@code WOULD REPAIR },
	 * {@code REPAIRED: } or {@code REFUSED: }. {@code OK} and {@code REFUSED} end the output; {@code WOULD REPAIR} and
	 * {@code REPAIRED} are followed by one line, the assumption the repair rests on, so their output does not end on
	 * the verdict. The runbook names the verdict by its first word, not as the last line.
	 */
	@Test
	void everyRunPrintsOneVerdictLine() throws IOException {
		assertVerdict(RaftLogRepair.run(dir, GROUP, true), Outcome.REFUSED, "REFUSED: ");
		var open = writeOpenSegment(0, 9);
		assertVerdict(RaftLogRepair.run(dir, GROUP, true), Outcome.OK, "OK: every entry is valid");
		long start = entryStart(0, 9);
		tear(open, start, start + entrySize(9));
		assertVerdict(RaftLogRepair.run(dir, GROUP, true), Outcome.WOULD_REPAIR,
				"WOULD REPAIR (dry run, nothing changed): would drop the torn entry expected to have index 9");
		assertVerdict(repair(), Outcome.REPAIRED, "REPAIRED: dropped the torn entry expected to have index 9");
		assertVerdict(repair(), Outcome.OK, "OK: every entry is valid");
		long last = entryStart(0, 8);
		flipBit(open, last + 100, 0x01);
		var damaged = RaftLogRepair.run(dir, GROUP, true);
		assertVerdict(damaged, Outcome.REFUSED, "REFUSED: ");
		assertTrue(damaged.lines().getLast().contains("Nothing was changed: replace the node"), text(damaged));
		var lockFile = current().resolveSibling("in_use.lock");
		try (var channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
				var lock = channel.lock()) {
			assertTrue(lock.isValid());
			var locked = repair();
			assertVerdict(locked, Outcome.REFUSED, "REFUSED: ");
			assertTrue(locked.lines().getLast().endsWith("is locked: stop the node first."), text(locked));
		}
		Files.delete(open);
		writeSegment("log_0-9", 0, 9);
		assertVerdict(RaftLogRepair.run(dir, GROUP, true), Outcome.OK, "OK: no open segment");
	}

	/**
	 * R48-2: a log that cannot be read (a denied permission here; an {@code EIO} the same way) gives no verdict: the run
	 * throws, which the command prints as a stack trace and exit 1. The runbook's step 2 says what to do then.
	 */
	@Test
	void unreadableLogGivesNoVerdict() throws IOException {
		writeOpenSegment(0, 9);
		var current = current();
		var permissions = Files.getPosixFilePermissions(current);
		Files.setPosixFilePermissions(current, Set.of());
		try {
			Assumptions.assumeFalse(Files.isReadable(current), "running as a user who reads anything");
			assertThrows(IOException.class, () -> RaftLogRepair.run(dir, GROUP, true));
		} finally {
			Files.setPosixFilePermissions(current, permissions);
		}
	}

	/** R48-2: the runbook's step 2 names each verdict as {@link #everyRunPrintsOneVerdictLine} pins it. */
	@Test
	void runbookReadsTheVerdictLine() throws IOException {
		var runbook = Files.readString(docs().resolve("runbooks.md"));
		int from = runbook.indexOf("## Node does not start after a power loss");
		assertTrue(from >= 0, "section missing");
		int to = runbook.indexOf("\n## ", from + 1);
		var section = runbook.substring(from, to > 0 ? to : runbook.length());
		assertFalse(section.contains("last line"), "the output of WOULD REPAIR does not end on its verdict");
		for (var verdict : List.of("`WOULD REPAIR (dry run, nothing changed)", "`OK: every entry is valid`",
				"`OK: no open segment`", "does not exist: wrong --data-dir or --group-id?", "is locked: stop the node first.",
				"Nothing was changed: replace the node", "`REPAIRED: dropped", "This is safe only if",
				"No verdict line")) {
			assertTrue(section.contains(verdict), "step 2 does not name " + verdict);
		}
		// Review of R48, 2: a stack trace from the repair can come after the file was cut (the sync or the check).
		assertFalse(section.contains("a stack trace without a verdict too"), "step 3 reads a stack trace as step 2's");
		assertTrue(section.contains("after the file was already cut"), "step 3 does not say the file may be cut");
	}

	private static void assertVerdict(Report report, Outcome outcome, String verdictStart) {
		var lines = report.lines();
		var verdicts = lines.stream().filter(line -> VERDICT.matcher(line).lookingAt()).toList();
		assertEquals(1, verdicts.size(), "one verdict line: " + lines);
		var verdict = verdicts.getFirst();
		assertTrue(verdict.startsWith(verdictStart), verdict);
		assertEquals(outcome, report.outcome(), lines.toString());
		var after = lines.subList(lines.indexOf(verdict) + 1, lines.size());
		if (outcome == Outcome.WOULD_REPAIR || outcome == Outcome.REPAIRED) {
			assertEquals(1, after.size(), lines.toString());
			assertTrue(after.getFirst().startsWith("This is safe only if that entry was never fsynced"), after.getFirst());
		} else {
			assertEquals(List.of(), after, "the verdict ends the output");
		}
	}

	private static final Pattern VERDICT = Pattern.compile("OK: |WOULD REPAIR |REPAIRED: |REFUSED: ");

	/** docs/distributed, from the module's directory or any directory above it. */
	private static Path docs() {
		for (var at = Path.of("").toAbsolutePath(); at != null; at = at.getParent()) {
			var candidate = at.resolve("docs").resolve("distributed");
			if (Files.isRegularFile(candidate.resolve("runbooks.md"))) {
				return candidate;
			}
		}
		throw new AssertionError("docs/distributed/runbooks.md not found above " + Path.of("").toAbsolutePath());
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
		tear(open, start, start + entrySize(19));
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
		tear(open, start, start + entrySize(10));
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
	 * The power of the leader goes out with writes in flight. A real torn entry was not acknowledged by this node; the
	 * test cannot tell, and tears the last entry on disk even if the node acknowledged it. That loses nothing here only
	 * because the two other nodes stay up: they hold every acknowledged entry and elect the new leader among themselves.
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
	 * The power of the whole cluster goes out in the middle of a group commit that no quorum finished. The followers
	 * get the entries, but their answers get lost from when all the logs are equal, so that no entry after that is
	 * committed or acknowledged, and those entries, the only ones torn, are what a real power cut can tear. Several
	 * nodes then refuse to start at once, without a quorum among the others to replace them: once repaired they have to
	 * elect a leader among themselves, and every acknowledged write must be there. With two torn nodes, the third stays
	 * down until they did, and then has to give up the entries that it alone has.
	 */
	@ParameterizedTest
	@ValueSource(ints = {2, 3})
	void nodesTornByOnePowerCutRepairAndElectALeaderAmongThemselves(int tornNodes) throws Exception {
		try (var cluster = ProcessCluster.start(dir, 3, SLOW_ELECTION_ARGS); var raft = new RaftProbe(cluster);
				var client = client(cluster); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
			cluster.waitReady(READY_TIMEOUT);
			long columnId = createColumn(cluster);
			var acknowledged = new ConcurrentSkipListSet<Long>();
			long version = 0;
			while (version < 100) {
				put(client, columnId, ++version);
				acknowledged.add(version);
			}
			var nodes = cluster.nodeIds();
			var leader = raft.leader(nodes);
			var followers = nodes.stream().filter(id -> !id.equals(leader)).toList();
			for (var follower : followers) {
				raft.awaitSameLog(leader, follower);
			}
			long synced = raft.lastEntry(leader);
			followers.forEach(follower -> cluster.network().dropReplies(leader, follower));

			var unacknowledged = new ArrayList<CompletableFuture<Void>>();
			for (int i = 0; i < 5; i++) {
				long next = ++version;
				unacknowledged.add(CompletableFuture.runAsync(() -> {
					if (client.put(columnId, key(next), value(next), OP_CONTEXT).acknowledged()) {
						acknowledged.add(next);
					}
				}, executor));
			}
			await(() -> nodes.stream().allMatch(id -> lastIndexOnDisk(cluster, id) > synced), CONVERGE_TIMEOUT,
					"every node to write an entry that no quorum confirmed");
			assertEquals(100, acknowledged.size(), "an entry after the logs were equal was committed");
			cluster.killAll();
			cluster.network().heal();

			var torn = new ArrayList<>(List.of(leader));
			torn.addAll(followers.subList(0, tornNodes - 1));
			for (var id : torn) {
				var tear = tearFrom(cluster, id, synced + 1);
				assertRefusesToStart(cluster, id);
				repairOffline(cluster, id, tear);
			}
			for (var id : torn) {
				cluster.start(id);
			}
			for (var id : torn) {
				cluster.waitReady(id, READY_TIMEOUT);
			}
			assertTrue(torn.contains(raft.leader(torn)), "the repaired nodes elect one of them");
			for (int i = 0; i < 5; i++) {
				put(client, columnId, ++version);
				acknowledged.add(version);
			}
			for (var id : nodes) {
				if (!torn.contains(id)) {
					cluster.start(id);
					cluster.waitReady(id, READY_TIMEOUT);
				}
			}
			CompletableFuture.allOf(unacknowledged.toArray(CompletableFuture[]::new)).get(2, TimeUnit.MINUTES);
			assertHoldsEveryAcknowledgedWrite(cluster, raft, acknowledged, version);
		}
	}

	/**
	 * A real log, with closed segments, a snapshot and a purged start. The torn entry is the first of a new open segment
	 * of the leader, which the repair leaves empty, so that Ratis deletes it and starts the segment again. Whether this
	 * entry was acknowledged does not matter, as in {@link #leaderWithATornEntryRejoinsAfterTheRepair}.
	 */
	@Test
	void leaderLosesAWholeOpenSegmentAfterClosedSegmentsAndASnapshot() throws Exception {
		try (var cluster = ProcessCluster.start(dir, 3, SMALL_LOG_ARGS); var raft = new RaftProbe(cluster);
				var client = client(cluster)) {
			cluster.waitReady(READY_TIMEOUT);
			long columnId = createColumn(cluster);
			var nodes = cluster.nodeIds();
			long version;
			try (var writer = new Writer(client(cluster), columnId)) {
				await(() -> nodes.stream().allMatch(id -> hasSnapshot(cluster, id) && logIsPurged(cluster, id)), PURGE_TIMEOUT,
						"a snapshot and a purged log on every node");
				version = writer.stop();
			}
			var victim = raft.leader(nodes);
			var log = logDir(cluster, victim);
			for (int puts = 0; entriesInOpenSegment(log) != 1; puts++) {
				assertTrue(puts < 1000, "the leader's open segment never held one entry");
				put(client, columnId, ++version);
			}
			cluster.kill(victim);

			var open = onlyOpenSegment(log);
			long first = readEntries(open).getFirst().getIndex();
			var tear = tearFrom(cluster, victim, first);
			assertEquals(0, tear.entriesLeft());
			var closed = closedSegments(log);
			assertTrue(closed.size() >= 2, closed.toString());
			assertRefusesToStart(cluster, victim);
			repairOffline(cluster, victim, tear);
			assertEquals(closed, closedSegments(log));

			cluster.start(victim);
			cluster.waitReady(victim, READY_TIMEOUT);
			for (int i = 0; i < 5; i++) {
				put(client, columnId, ++version);
			}
			assertConverged(cluster, raft, victim, first, version);
		}
	}

	/** What {@link #tearFrom} did: the open segment, the first entry lost, where it starts and the entries before it. */
	private record Tear(Path segment, long firstLost, long offset, int entriesLeft) {}

	/**
	 * Tears the group commit that starts with entry {@code firstLost} of the open segment of a stopped node: from the
	 * first sector boundary in that entry, the log is lost.
	 */
	private static Tear tearFrom(ProcessCluster cluster, String victim, long firstLost) throws IOException {
		var open = onlyOpenSegment(logDir(cluster, victim));
		var entries = readEntries(open);
		long offset = SegmentedRaftLogFormat.getHeaderLength();
		int kept = 0;
		while (kept < entries.size() && entries.get(kept).getIndex() < firstLost) {
			offset += entrySize(entries.get(kept++));
		}
		assertTrue(kept < entries.size(), "node " + victim + " has no entry " + firstLost);
		long end = SegmentedRaftLogFormat.getHeaderLength() + entries.stream().mapToLong(RaftLogRepairTest::entrySize).sum();
		assertTrue(Files.size(open) > end, "preallocated tail expected in a node that did not stop");
		assertTrue(alignedAfter(offset, SECTOR) < offset + entrySize(entries.get(kept)), "the entry should cross a sector");
		tear(open, offset, end);
		return new Tear(open, firstLost, offset, kept);
	}

	private static void assertRefusesToStart(ProcessCluster cluster, String victim) throws IOException {
		cluster.start(victim);
		var refused = assertThrows(IllegalStateException.class, () -> cluster.waitReady(victim, READY_TIMEOUT));
		assertTrue(refused.getMessage().contains("before printing READY"), refused.getMessage());
		assertTrue(cluster.output(victim).contains("Log entry corrupted"), cluster.logTail(victim));
		// R47-1: the start failure names the command that decides, with this node's data directory.
		assertTrue(cluster.output(victim).contains("repair-log --data-dir " + cluster.dataDir(victim) + " --dry-run"),
				cluster.logTail(victim));
	}

	/** A dry run that changes nothing, then the repair of what {@link #tearFrom} did, through the command line. */
	private static void repairOffline(ProcessCluster cluster, String victim, Tear tear) throws IOException {
		var before = Files.readAllBytes(tear.segment());
		var dryRun = cluster.repairLog(victim, "--dry-run");
		assertEquals(0, dryRun.exitCode(), dryRun.output());
		assertTrue(dryRun.output().contains("WOULD REPAIR"), dryRun.output());
		assertTrue(dryRun.output().contains(ASSUMPTION), dryRun.output());
		assertArrayEquals(before, Files.readAllBytes(tear.segment()));

		var repaired = cluster.repairLog(victim);
		assertEquals(0, repaired.exitCode(), repaired.output());
		assertTrue(repaired.output().contains("REPAIRED"), repaired.output());
		assertTrue(repaired.output().contains("index " + tear.firstLost()), repaired.output());
		assertTrue(repaired.output().contains(ASSUMPTION), repaired.output());
		assertEquals(tear.offset(), Files.size(tear.segment()));
		assertEquals(tear.entriesLeft(), readEntries(tear.segment()).size());
	}

	/**
	 * Tears the last entry of the open segment of a killed node, and checks that the node then refuses to start and
	 * that it starts after a repair.
	 *
	 * @return the index of the torn entry
	 */
	private static long tearRepairAndRestart(ProcessCluster cluster, String victim) throws Exception {
		var last = readEntries(onlyOpenSegment(logDir(cluster, victim))).getLast().getIndex();
		var tear = tearFrom(cluster, victim, last);
		assertRefusesToStart(cluster, victim);

		var running = cluster.nodeIds().stream().filter(id -> !id.equals(victim)).findFirst().orElseThrow();
		var lockFile = logDir(cluster, running).resolveSibling("in_use.lock");
		var dryRunLocked = cluster.repairLog(running, "--dry-run");
		assertEquals(1, dryRunLocked.exitCode(), dryRunLocked.output());
		assertTrue(dryRunLocked.output().contains("stop the node first"), dryRunLocked.output());
		var locked = cluster.repairLog(running);
		assertEquals(1, locked.exitCode(), locked.output());
		assertTrue(locked.output().contains("stop the node first"), locked.output());
		assertTrue(Files.exists(lockFile), "a running node keeps its lock file");

		repairOffline(cluster, victim, tear);

		cluster.start(victim);
		cluster.waitReady(victim, READY_TIMEOUT);
		return last;
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

	/**
	 * Waits for every node to apply the whole log, stops them, and checks that their databases are the same, that they
	 * hold every acknowledged put, and that what else they hold is one of the puts that had no answer.
	 */
	private static void assertHoldsEveryAcknowledgedWrite(ProcessCluster cluster, RaftProbe raft, Set<Long> acknowledged,
			long attempted) throws Exception {
		var nodes = cluster.nodeIds();
		try {
			raft.awaitAllApplied(nodes, CONVERGE_TIMEOUT);
		} catch (AssertionError e) {
			throw new AssertionError(raft.describe(nodes), e);
		}
		for (var id : nodes) {
			cluster.stop(id);
		}
		var attemptedPuts = new TreeMap<String, String>();
		for (long version = 1; version <= attempted; version++) {
			attemptedPuts.put(hex(key(version).keys()[0]), hex(value(version)));
		}
		var contents = replicaContents(cluster, nodes.getFirst());
		for (var id : nodes) {
			assertEquals(contents, replicaContents(cluster, id), "replica " + id);
		}
		for (long version : acknowledged) {
			var key = hex(key(version).keys()[0]);
			assertEquals(attemptedPuts.get(key), contents.get(key), "acknowledged put of version " + version);
		}
		assertTrue(attemptedPuts.entrySet().containsAll(contents.entrySet()), "a value that nobody wrote");
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

	private static String text(Report report) {
		return String.join("\n", report.lines());
	}

	private void assertRefused(Path file, String reason) throws IOException {
		var before = Files.readAllBytes(file);
		var report = repair();
		assertEquals(Outcome.REFUSED, report.outcome(), report.lines().toString());
		assertTrue(text(report).contains(reason), text(report));
		assertArrayEquals(before, Files.readAllBytes(file));
	}

	/** Ratis too fails on the segments, once no torn entry fails it first. */
	private void assertRatisRefusesTheLog() {
		var failure = assertThrows(RuntimeException.class, () -> openRaftLog().close());
		assertTrue(String.valueOf(failure.getMessage()).contains("gap"), failure.toString());
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

	private static byte[] header() {
		var header = new byte[SegmentedRaftLogFormat.getHeaderLength()];
		SegmentedRaftLogFormat.getHeaderBytebuffer().get(header);
		return header;
	}

	private static LogEntryProto entry(long index) {
		return entry(index, PAYLOAD_BYTES);
	}

	private static LogEntryProto entry(long index, int payloadBytes) {
		var payload = new byte[payloadBytes];
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

	/** An open segment whose last entry ends at {@code end}, with one entry before it to fill the space. */
	private Path writeOpenSegmentEndingAt(long end, LogEntryProto last) throws IOException {
		long fill = end - SegmentedRaftLogFormat.getHeaderLength() - entrySize(last);
		var filler = IntStream.rangeClosed(0, Math.toIntExact(fill)).mapToObj(payload -> entry(0, payload))
				.filter(entry -> entrySize(entry) == fill)
				.findFirst().orElseThrow();
		var bytes = new ByteArrayOutputStream();
		bytes.write(header());
		bytes.write(framed(filler.toByteArray()));
		bytes.write(framed(last.toByteArray()));
		bytes.write(new byte[PREALLOCATED_BYTES]);
		Files.createDirectories(current());
		return Files.write(current().resolve("log_inprogress_0"), bytes.toByteArray());
	}

	private static long crc32c(LogEntryProto entry) {
		try {
			var framed = framed(entry.toByteArray());
			return ByteBuffer.wrap(framed, framed.length - Integer.BYTES, Integer.BYTES).getInt();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
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

	/** The first multiple of {@code alignment} after {@code offset}. */
	private static long alignedAfter(long offset, int alignment) {
		return (offset / alignment + 1) * alignment;
	}

	/** What a device leaves of an interrupted write of these bytes: everything from the first sector inside them on. */
	private static void tear(Path file, long start, long end) throws IOException {
		long from = alignedAfter(start, SECTOR);
		assertTrue(from < end, "no sector boundary in " + start + " to " + end);
		zero(file, from, end);
	}

	private static void flipBit(Path file, long offset, int mask) throws IOException {
		try (var channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
			var value = ByteBuffer.allocate(1);
			channel.read(value, offset);
			value.put(0, (byte) (value.get(0) ^ mask));
			channel.write(value.rewind(), offset);
		}
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

	private static Path logDir(ProcessCluster cluster, String id) {
		return cluster.dataDir(id).resolve("raft").resolve(GROUP).resolve("current");
	}

	/** The last whole entry a running node wrote to its open segment, whether or not it was synced. */
	private static long lastIndexOnDisk(ProcessCluster cluster, String id) {
		try {
			return scanOpenSegment(logDir(cluster, id)).lastIndex();
		} catch (IOException segmentIsRolling) {
			return -1;
		}
	}

	/** Entries that a running node wrote to its open segment, -1 if it is rolling to a new one. */
	private static long entriesInOpenSegment(Path log) {
		try {
			return scanOpenSegment(log).entries();
		} catch (IOException segmentIsRolling) {
			return -1;
		}
	}

	private static RaftLogRepair.Scan scanOpenSegment(Path log) throws IOException {
		return RaftLogRepair.scan(Files.readAllBytes(onlyOpenSegment(log)), RaftSettings.LOG_ENTRY_SIZE_MAX.getSizeInt());
	}

	private static List<String> closedSegments(Path log) throws IOException {
		try (var files = Files.list(log)) {
			return files.map(file -> file.getFileName().toString()).filter(name -> name.matches("log_\\d+-\\d+")).sorted()
					.toList();
		}
	}

	private static boolean logIsPurged(ProcessCluster cluster, String id) {
		try {
			var closed = closedSegments(logDir(cluster, id));
			return !closed.isEmpty() && closed.stream().noneMatch(name -> name.startsWith("log_0-"));
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}

	private static boolean hasSnapshot(ProcessCluster cluster, String id) {
		var snapshots = logDir(cluster, id).resolveSibling("sm").resolve("snapshots");
		try (var files = Files.list(snapshots)) {
			return files.anyMatch(file -> file.getFileName().toString().startsWith("snapshot-"));
		} catch (IOException noneYet) {
			return false;
		}
	}

	private static Path onlyOpenSegment(Path log) throws IOException {
		try (var files = Files.list(log)) {
			var open = files.filter(file -> file.getFileName().toString().startsWith("log_inprogress_")).toList();
			if (open.size() != 1) {
				throw new IOException("Expected one open segment: " + open);
			}
			return open.getFirst();
		}
	}

	private static Keys key(long version) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(version).array()));
	}

	private static Buf value(long version) {
		var value = new byte[VALUE_BYTES];
		Arrays.fill(value, (byte) 'x');
		var prefix = ("v" + version + ":").getBytes(StandardCharsets.UTF_8);
		System.arraycopy(prefix, 0, value, 0, prefix.length);
		return Buf.wrap(value);
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
