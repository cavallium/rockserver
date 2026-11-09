package it.cavallium.rockserver.distributed;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy;
import org.apache.ratis.server.raftlog.segmented.LogSegment;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogFormat;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.WireFormat;
import org.apache.ratis.util.PureJavaCrc32C;
import org.jetbrains.annotations.Nullable;

/**
 * Offline repair of a Raft log whose open segment ends in a write torn by a power loss.
 *
 * <p>Format of a Ratis 3.3.1 segment, from {@code SegmentedRaftLogFormat}, {@code SegmentedRaftLogOutputStream} and
 * {@code SegmentedRaftLogReader}: the 8 bytes {@code RaftLog1}, then for each entry a varint32 with the size of its
 * {@code LogEntryProto}, the proto, and a big-endian CRC32C ({@code PureJavaCrc32C}) of the varint and the proto
 * together. A zero byte where an entry would start ends the log, and only zeros may follow it: an open segment is
 * preallocated with zeros.</p>
 *
 * <p>A write torn in the middle of an entry therefore leaves a size followed by zeros, which fails the checksum, and
 * with the default corruption policy the node does not start. (An entry cut by the end of the file, instead, Ratis
 * drops by itself.) A torn entry had no finished fsync, so this node never acknowledged it and may lose it: if the
 * entry was committed, a majority without this node has it, and the leader sends it again.</p>
 *
 * <p><b>That is an assumption, and all the safety of the repair rests on it.</b> The bytes cannot prove it. The repair
 * removes this node's copy from whatever quorum counted it, so if the entry had been fsynced and acknowledged after all
 * (storage that lies about fsync, or damage that passes the checks below), a committed entry has one copy less. With
 * three nodes it may be left on the leader alone. If that leader then fails, or forgets the acknowledgement in the same
 * power cut, the other two can elect a leader without the entry, and an acknowledged write is gone with no error
 * anywhere. A node that never catches up behind a leader remembering the acknowledgement would give the loss away, but
 * only while that leader lives, so a node that does catch up proves nothing. The tool states the assumption whenever it
 * drops something.</p>
 *
 * <p>The rule is deliberately narrow. The first invalid entry of the open segment is dropped only if all of this holds,
 * so that it looks like a torn last write followed by preallocated zeros:</p>
 * <ul>
 *   <li>every byte after the place where that entry says it ends is zero;</li>
 *   <li>its size agrees with the first fields of its proto, unless a lost sector took those: a torn write does not
 *       change a size, damage can, and then the zeros after the entry would pass for a lost sector;</li>
 *   <li>the end of the file cuts it, or one of its sectors ({@value #SECTOR_BYTES} bytes, aligned to the offset in the
 *       file) is all zeros. Ratis' {@code BufferedWriteChannel} flushes only between whole entries and a device loses
 *       whole sectors, so a torn entry misses at least one. A complete entry that fails its checksum is damaged
 *       media. This tells the two apart unless the payload itself holds a sector of zeros. A zero sector with nothing
 *       but checksum bytes in it leaves the proto whole, so the checksum bytes before it must match the proto;</li>
 *   <li>no frame with a valid checksum starts within the bytes it claims: a size inflated by corruption would hide
 *       whole entries behind it.</li>
 * </ul>
 *
 * <p>Anything else is refused, because it is not a torn write and could be the loss of durable data: non-zero bytes
 * after the invalid entry (even if they are valid entries of the same group commit, since they cannot be told from
 * later data), a valid checksum over bytes that do not parse, a size that no valid entry has, entries out of order,
 * segments that do not follow each other, or any problem in a closed segment, which can hold committed entries. Such a
 * node must be replaced, which takes a leader. Without one, see "No quorum" in the design document: there is no
 * override, because a node that this rule refuses could hold the only copy of an acknowledged write.</p>
 *
 * <p>The segment is read twice. {@link LogSegment#readSegmentFile} is what the node will do at its next start and
 * decides which entries are valid, but it says neither where they end nor what follows them, which {@link #scan}
 * works out from the bytes. The repair goes ahead only if the two agree on every entry, and Ratis reads the segment
 * again after it. The file is truncated at the start of the invalid entry, as Ratis itself truncates an open segment
 * to its last entry when it loads one (it appends at the end of the file) and deletes one left without entries;
 * preallocation starts again from there.</p>
 */
final class RaftLogRepair {

	/** Not a public name in Ratis ({@code RaftStorageDirectoryImpl}); locked by a running node. */
	/** Ratis' lock file in a group directory, held by a node for as long as it runs that group. */
	static final String LOCK_FILE = "in_use.lock";
	private static final String CURRENT_DIR = "current";
	/**
	 * What {@code LogSegmentStartEnd} accepts. Its {@code LogSegmentPath.matchLogSegment} cannot list the segments: it
	 * deletes an empty open segment, and a dry run changes nothing.
	 */
	private static final Pattern CLOSED_SEGMENT = Pattern.compile("log_(\\d+)-(\\d+)");
	private static final Pattern OPEN_SEGMENT = Pattern.compile("log_inprogress_(\\d+)(?:\\..*)?");
	private static final int CHECKSUM_BYTES = Integer.BYTES;
	/** The unit a device writes whole. A device with larger sectors loses a multiple of it. */
	private static final int SECTOR_BYTES = 512;
	/**
	 * What every {@code LogEntryProto} starts with: the tag of its first field, the term, which no entry has as 0. It
	 * keeps {@link #validFrameIn} from computing a checksum of megabytes at each of millions of offsets.
	 */
	private static final byte ENTRY_FIRST_BYTE = 0x08;
	private static final int MAX_VARINT32_SHIFT = 28;
	private static final String ASSUMPTION = "This is safe only if that entry was never fsynced, so that this node never"
			+ " acknowledged it, which nothing in the file can prove. It removes this node's copy from any quorum that"
			+ " counted it: if the storage lost an fsynced entry, an acknowledged write has one copy less, and catching up"
			+ " afterwards does not show that it survived.";

	/** How a run ended. */
	enum Outcome {
		/** Ratis can read the log: nothing was changed. */
		OK,
		/** A torn last write was dropped. */
		REPAIRED,
		/** Dry run: {@link #REPAIRED} if it were not one. Nothing was changed. */
		WOULD_REPAIR,
		/** The log needs more than this tool may do. Nothing was changed. */
		REFUSED
	}

	/**
	 * @param outcome how the run ended
	 * @param lines what was found and done, for the operator
	 */
	record Report(Outcome outcome, List<String> lines) {

		boolean success() {
			return outcome != Outcome.REFUSED;
		}
	}

	/**
	 * The first entry of a segment that cannot be used.
	 *
	 * @param offset where the entry starts
	 * @param reason why it cannot be used
	 * @param end where its bytes end according to its own size, capped to the end of the file; -1 if it did not leave
	 *            a size that a torn write of a valid entry could have left, so that this cannot be a torn write
	 * @param cut the end of the file comes before the end of the entry, so that its checksum cannot be checked
	 * @param toleratedByRatis Ratis reads the segment as if it ended at {@code offset} instead of failing
	 */
	record InvalidEntry(long offset, String reason, long end, boolean cut, boolean toleratedByRatis) {}

	/**
	 * A segment read from its bytes.
	 *
	 * @param entries valid entries from the start of the file
	 * @param firstIndex log index of the first of them, -1 without entries
	 * @param lastIndex log index of the last of them, -1 without entries
	 * @param validEnd where the valid entries end
	 * @param invalid what stopped the reading, null if the segment holds nothing else
	 */
	record Scan(long entries, long firstIndex, long lastIndex, long validEnd, @Nullable InvalidEntry invalid) {}

	private record Segment(Path path, long start, @Nullable Long end) {

		boolean open() {
			return end == null;
		}

		String name() {
			return path.getFileName().toString();
		}
	}

	/** Entries Ratis read from a segment, where they end and what stopped it, if anything did. */
	private record RatisRead(long entries, long end, @Nullable Exception failure) {}

	private RaftLogRepair() {}

	/**
	 * Checks the Raft log of a stopped node and drops a torn last write from its open segment.
	 *
	 * @param dataDir the node's data directory
	 * @param groupId the Raft group id
	 * @param dryRun report what would happen without changing anything
	 * @throws IOException if the log cannot be read or written
	 */
	static Report run(Path dataDir, String groupId, boolean dryRun) throws IOException {
		var lines = new ArrayList<String>();
		var groupDir = dataDir.resolve("raft").resolve(groupId);
		var currentDir = groupDir.resolve(CURRENT_DIR);
		if (!Files.isDirectory(currentDir)) {
			lines.add("REFUSED: " + currentDir + " does not exist: wrong --data-dir or --group-id?");
			return new Report(Outcome.REFUSED, lines);
		}
		var lockFile = groupDir.resolve(LOCK_FILE);
		// Ratis deletes its lock file when it stops, unless it was killed. A dry run leaves no trace.
		if (dryRun && !Files.exists(lockFile)) {
			return inspect(currentDir, true, lines);
		}
		// The file stays, as a killed node leaves it, which Ratis accepts. If it were deleted here, a node that had opened
		// it meanwhile would hold a lock on a file without a name, and a second process could lock a new one.
		try (var lockChannel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
			try (var lock = lockChannel.tryLock()) {
				if (lock == null) {
					return nodeRunning(lockFile, lines);
				}
				return inspect(currentDir, dryRun, lines);
			} catch (OverlappingFileLockException e) {
				return nodeRunning(lockFile, lines);
			}
		}
	}

	/** Why a command that needs the node stopped refuses: the node holds {@code lockFile} ({@link #LOCK_FILE}). */
	static String nodeRunning(Path lockFile) {
		return lockFile + " is locked: stop the node first.";
	}

	private static Report nodeRunning(Path lockFile, List<String> lines) {
		lines.add("REFUSED: " + nodeRunning(lockFile));
		return new Report(Outcome.REFUSED, lines);
	}

	private static Report inspect(Path currentDir, boolean dryRun, List<String> lines) throws IOException {
		var segments = segments(currentDir, lines);
		lines.add("Raft log " + currentDir + ": " + segments.size() + " segments");
		var open = segments.stream().filter(Segment::open).toList();
		if (open.size() > 1) {
			lines.add("REFUSED: " + open.size() + " open segments (" + names(open) + "). Ratis keeps one at most: "
					+ "this is not the result of a torn write. " + replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		if (!open.isEmpty() && !open.getFirst().equals(segments.getLast())) {
			lines.add("REFUSED: open segment " + open.getFirst().name() + " is not the last segment. " + replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		var discontinuity = discontinuity(segments);
		if (discontinuity != null) {
			lines.add("REFUSED: " + discontinuity + ". Ratis does not load such a log, and a torn write does not leave one. "
					+ replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		for (var segment : segments) {
			if (!segment.open()) {
				var problem = closedSegmentProblem(segment);
				if (problem != null) {
					lines.add("REFUSED: closed segment " + segment.name() + ": " + problem + ". Closed segments can hold "
							+ "committed entries, so this tool never changes them. " + replaceTheNode());
					return new Report(Outcome.REFUSED, lines);
				}
			}
		}
		lines.add((segments.size() - open.size()) + " closed segments are valid");
		if (open.isEmpty()) {
			lines.add("OK: no open segment");
			return new Report(Outcome.OK, lines);
		}
		var last = open.getFirst();
		return inspectOpenSegment(last, dryRun, lines);
	}

	/**
	 * Where a segment does not start right after the one before it, null if they all do. The first one can start
	 * anywhere, since purging removes segments from the start, and only the last one can be open.
	 */
	private static @Nullable String discontinuity(List<Segment> segments) {
		for (int i = 1; i < segments.size(); i++) {
			var previous = segments.get(i - 1);
			var segment = segments.get(i);
			long expected = previous.end() + 1;
			if (segment.start() != expected) {
				return (segment.start() > expected ? "gap" : "overlap") + " between segments " + previous.name() + " and "
						+ segment.name() + ": the second one should start at index " + expected;
			}
		}
		return null;
	}

	private static Report inspectOpenSegment(Segment segment, boolean dryRun, List<String> lines) throws IOException {
		long size = Files.size(segment.path());
		if (size > Integer.MAX_VALUE - Long.BYTES) {
			lines.add("REFUSED: " + segment.name() + " has " + size + " bytes: Ratis would never write so many. "
					+ replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		var bytes = Files.readAllBytes(segment.path());
		int maxEntrySize = RaftSettings.LOG_ENTRY_SIZE_MAX.getSizeInt();
		var scan = scan(bytes, maxEntrySize);
		var ratis = readWithRatis(segment);
		lines.add("Open segment " + segment.name() + ": " + bytes.length + " bytes, " + scan.entries() + " valid entries"
				+ (scan.entries() > 0 ? " (indexes " + scan.firstIndex() + " to " + scan.lastIndex() + ")" : "")
				+ " ending at offset " + scan.validEnd());
		var disagreement = disagreement(scan, ratis);
		if (disagreement != null) {
			lines.add("REFUSED: this tool and Ratis read " + segment.name() + " differently: " + disagreement
					+ ". Ratis failure: " + ratis.failure() + ". " + replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		if (scan.entries() > 0 && scan.firstIndex() != segment.start()) {
			lines.add("REFUSED: " + segment.name() + " starts with index " + scan.firstIndex() + ". " + replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		var invalid = scan.invalid();
		if (invalid == null) {
			lines.add("OK: every entry is valid");
			return new Report(Outcome.OK, lines);
		}
		lines.add("Invalid entry at offset " + invalid.offset() + ": " + invalid.reason());
		var notTorn = notATornWrite(invalid, bytes, maxEntrySize);
		if (notTorn != null) {
			lines.add("REFUSED: " + notTorn + ". This is not a torn last write, and dropping it could lose entries "
					+ "this node reported as durable. " + replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		long expectedIndex = scan.entries() > 0 ? scan.lastIndex() + 1 : segment.start();
		var dropped = "the torn entry expected to have index " + expectedIndex + " at offset " + invalid.offset() + ", and "
				+ (bytes.length - invalid.offset()) + " bytes from there to the end of the file, "
				+ nonZeroBytes(bytes, (int) invalid.offset()) + " of them not zero";
		if (dryRun) {
			lines.add("WOULD REPAIR (dry run, nothing changed): would drop " + dropped);
			lines.add(ASSUMPTION);
			return new Report(Outcome.WOULD_REPAIR, lines);
		}
		try (var channel = FileChannel.open(segment.path(), StandardOpenOption.WRITE)) {
			channel.truncate(invalid.offset());
			channel.force(true);
		}
		var after = readWithRatis(segment);
		if (after.failure() != null || after.entries() != scan.entries()) {
			throw new IOException("Truncated " + segment.path() + " at offset " + invalid.offset() + ", but Ratis then read "
					+ after.entries() + " entries instead of " + scan.entries(), after.failure());
		}
		lines.add("REPAIRED: dropped " + dropped);
		lines.add(ASSUMPTION);
		return new Report(Outcome.REPAIRED, lines);
	}

	/** Why the entry is not a torn last write, null if it is one. */
	private static @Nullable String notATornWrite(InvalidEntry invalid, byte[] bytes, int maxEntrySize) {
		if (invalid.end() < 0) {
			return "the entry has no size that a torn write could leave";
		}
		int end = (int) invalid.end();
		int nonZero = SegmentedRaftLogFormat.indexOfNonTerminator(bytes, end, bytes.length - end);
		if (nonZero >= 0) {
			return "non-zero data at offset " + (end + nonZero) + ", after the end of the invalid entry";
		}
		var sizeDamage = sizeDamage(invalid, bytes);
		if (sizeDamage != null) {
			return sizeDamage;
		}
		if (!invalid.cut() && !hasZeroSector(invalid, bytes)) {
			return "the entry is complete on disk and damaged: the file does not cut it and no sector of it is all zeros, "
					+ "while a torn write misses at least one";
		}
		int frame = validFrameIn(invalid, bytes, maxEntrySize);
		return frame < 0 ? null : "a frame with a valid checksum at offset " + frame + ", inside the invalid entry";
	}

	/**
	 * Why the size of the entry cannot be right, null if nothing shows that. A proto is its term, its index unless that
	 * is 0, and one body, whose own size ends the first bytes of the entry: if those are there, they give the size of
	 * the proto, with no need for its checksum.
	 */
	private static @Nullable String sizeDamage(InvalidEntry invalid, byte[] bytes) {
		int offset = (int) invalid.offset();
		var in = CodedInputStream.newInstance(bytes, offset, bytes.length - offset);
		try {
			int size = in.readUInt32();
			int protoStart = in.getTotalBytesRead();
			if (WireFormat.getTagFieldNumber(in.readTag()) != LogEntryProto.TERM_FIELD_NUMBER) {
				return null;
			}
			in.readUInt64();
			int tag = in.readTag();
			if (WireFormat.getTagFieldNumber(tag) == LogEntryProto.INDEX_FIELD_NUMBER) {
				in.readUInt64();
				tag = in.readTag();
			}
			if (!isEntryBody(tag)) {
				return null;
			}
			long bodySize = Integer.toUnsignedLong(in.readUInt32());
			int read = in.getTotalBytesRead();
			long fieldsSize = read - protoStart + bodySize;
			// The zeros of a lost sector read as a valid end of a varint, so such fields say nothing.
			return fieldsSize == size || touchesZeroSector(bytes, offset, read) ? null
					: "its size " + size + " disagrees with the " + fieldsSize + " bytes that the fields at its start add up "
					+ "to, which a torn write cannot do";
		} catch (IOException e) {
			return null;
		}
	}

	private static boolean isEntryBody(int tag) {
		return WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED
				&& switch (WireFormat.getTagFieldNumber(tag)) {
					case LogEntryProto.STATEMACHINELOGENTRY_FIELD_NUMBER, LogEntryProto.CONFIGURATIONENTRY_FIELD_NUMBER,
							LogEntryProto.METADATAENTRY_FIELD_NUMBER -> true;
					default -> false;
				};
	}

	private static boolean touchesZeroSector(byte[] bytes, int offset, int length) {
		int lastSector = (offset + length - 1) / SECTOR_BYTES;
		for (int sector = offset / SECTOR_BYTES + 1; sector <= lastSector; sector++) {
			int start = sector * SECTOR_BYTES;
			if (SegmentedRaftLogFormat.indexOfNonTerminator(bytes, start, Math.min(SECTOR_BYTES, bytes.length - start)) < 0) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Whether a sector that holds bytes of the entry is all zeros. The one it starts in holds its first byte, which is
	 * not zero. The last one can go past the end of the entry, where everything is known to be zero already.
	 */
	private static boolean hasZeroSector(InvalidEntry invalid, byte[] bytes) {
		long firstSector = (invalid.offset() / SECTOR_BYTES + 1) * SECTOR_BYTES;
		long checksumStart = invalid.end() - CHECKSUM_BYTES;
		for (long sector = firstSector; sector < invalid.end(); sector += SECTOR_BYTES) {
			int length = (int) Math.min(SECTOR_BYTES, bytes.length - sector);
			if (SegmentedRaftLogFormat.indexOfNonTerminator(bytes, (int) sector, length) < 0) {
				return sector < checksumStart || checksumBeforeMatches(invalid, bytes, (int) sector);
			}
		}
		return false;
	}

	/** Whether the bytes of the stored checksum before {@code end} match the proto, as they do if only the rest is lost. */
	private static boolean checksumBeforeMatches(InvalidEntry invalid, byte[] bytes, int end) {
		int offset = (int) invalid.offset();
		int checksumStart = (int) invalid.end() - CHECKSUM_BYTES;
		var checksum = new PureJavaCrc32C();
		checksum.update(bytes, offset, checksumStart - offset);
		var computed = ByteBuffer.allocate(CHECKSUM_BYTES).putInt((int) checksum.getValue()).array();
		return Arrays.equals(bytes, checksumStart, end, computed, 0, end - checksumStart);
	}

	/** Where the first entry with a valid checksum starts inside the invalid entry, -1 if none does. */
	private static int validFrameIn(InvalidEntry invalid, byte[] bytes, int maxEntrySize) {
		var checksum = new PureJavaCrc32C();
		for (int start = (int) invalid.offset() + 1; start < invalid.end(); start++) {
			if (isValidFrame(bytes, start, maxEntrySize, checksum)) {
				return start;
			}
		}
		return -1;
	}

	private static boolean isValidFrame(byte[] bytes, int start, int maxEntrySize, PureJavaCrc32C checksum) {
		int protoStart = start;
		int protoSize = 0;
		for (int shift = 0; ; shift += Byte.SIZE - 1) {
			if (protoStart == bytes.length || shift > MAX_VARINT32_SHIFT) {
				return false;
			}
			byte next = bytes[protoStart++];
			protoSize |= (next & 0x7F) << shift;
			if (next >= 0) {
				break;
			}
		}
		if (protoSize <= 0 || protoSize > maxEntrySize || protoStart + (long) protoSize + CHECKSUM_BYTES > bytes.length
				|| bytes[protoStart] != ENTRY_FIRST_BYTE) {
			return false;
		}
		int checksumStart = protoStart + protoSize;
		checksum.reset();
		checksum.update(bytes, start, checksumStart - start);
		return (int) checksum.getValue() == ByteBuffer.wrap(bytes, checksumStart, CHECKSUM_BYTES).getInt();
	}

	private static long nonZeroBytes(byte[] bytes, int from) {
		long count = 0;
		for (int i = from; i < bytes.length; i++) {
			if (bytes[i] != 0) {
				count++;
			}
		}
		return count;
	}

	/** Why the two readings of an open segment differ, null if they do not. */
	private static @Nullable String disagreement(Scan scan, RatisRead ratis) {
		if (ratis.entries() != scan.entries()) {
			return ratis.entries() + " entries against " + scan.entries();
		}
		if (scan.entries() > 0 && ratis.end() != scan.validEnd()) {
			return "entries ending at " + ratis.end() + " against " + scan.validEnd();
		}
		boolean tolerated = scan.invalid() == null || scan.invalid().toleratedByRatis();
		if (tolerated != (ratis.failure() == null)) {
			return ratis.failure() == null ? "only this tool found a problem" : "only Ratis found a problem";
		}
		return null;
	}

	/** Why a closed segment cannot be used, null if it can. */
	private static @Nullable String closedSegmentProblem(Segment segment) {
		var read = readWithRatis(segment);
		if (read.failure() != null) {
			return read.failure().toString();
		}
		long expected = segment.end() - segment.start() + 1;
		return read.entries() == expected ? null : "it holds " + read.entries() + " entries instead of " + expected;
	}

	/** Reads a segment exactly as a starting node does, with the corruption policy of the node. */
	private static RatisRead readWithRatis(Segment segment) {
		long[] entries = {0};
		long[] end = {SegmentedRaftLogFormat.getHeaderLength()};
		Exception failure = null;
		try {
			LogSegment.readSegmentFile(segment.path().toFile(), LogSegmentPath.matchLogSegment(segment.path()).getStartEnd(),
					RaftSettings.LOG_ENTRY_SIZE_MAX, CorruptionPolicy.EXCEPTION, null, entry -> {
						entries[0]++;
						end[0] += entrySize(entry);
					});
		} catch (IOException | RuntimeException e) {
			failure = e;
		}
		return new RatisRead(entries[0], end[0], failure);
	}

	private static long entrySize(LogEntryProto entry) {
		int size = entry.getSerializedSize();
		return (long) CodedOutputStream.computeUInt32SizeNoTag(size) + size + CHECKSUM_BYTES;
	}

	/**
	 * Reads a segment from its bytes, the way {@code SegmentedRaftLogReader} does.
	 *
	 * @param maxEntrySize the limit that the node reads entries with
	 */
	static Scan scan(byte[] bytes, int maxEntrySize) {
		int headerLength = SegmentedRaftLogFormat.getHeaderLength();
		int headerRead = Math.min(headerLength, bytes.length);
		int matched = SegmentedRaftLogFormat.matchHeader(bytes, 0, headerRead);
		if (matched < headerLength) {
			if (SegmentedRaftLogFormat.isTerminator(bytes, matched, bytes.length - matched)) {
				return new Scan(0, -1, -1, 0, null);
			}
			return new Scan(0, -1, -1, 0, new InvalidEntry(0, "bad segment header", -1, false,
					SegmentedRaftLogFormat.isTerminator(bytes, matched, headerRead - matched)));
		}
		var scan = new ScanState(bytes, maxEntrySize, headerLength);
		InvalidEntry invalid;
		do {
			invalid = scan.next();
		} while (invalid == null && !scan.finished);
		return new Scan(scan.entries, scan.firstIndex, scan.lastIndex, scan.position, invalid);
	}

	private static final class ScanState {
		private final byte[] bytes;
		private final int maxEntrySize;
		private final PureJavaCrc32C checksum = new PureJavaCrc32C();
		int position;
		long entries;
		long firstIndex = -1;
		long lastIndex = -1;
		boolean finished;

		ScanState(byte[] bytes, int maxEntrySize, int position) {
			this.bytes = bytes;
			this.maxEntrySize = maxEntrySize;
			this.position = position;
		}

		/** Reads one entry, returning what is wrong with it if anything is. */
		@Nullable InvalidEntry next() {
			if (position == bytes.length) {
				finished = true;
				return null;
			}
			if (SegmentedRaftLogFormat.isTerminator(bytes[position])) {
				int nonZero = SegmentedRaftLogFormat.indexOfNonTerminator(bytes, position, bytes.length - position);
				if (nonZero < 0) {
					finished = true;
					return null;
				}
				return new InvalidEntry(position, "a zero byte ends the log here, but offset " + (position + nonZero)
						+ " is not zero", -1, false, false);
			}
			var in = new ByteArrayInputStream(bytes, position + 1, bytes.length - position - 1);
			int protoSize;
			try {
				protoSize = CodedInputStream.readRawVarint32(bytes[position], in);
			} catch (IOException e) {
				boolean cut = in.available() == 0;
				return new InvalidEntry(position, "bad entry size: " + e.getMessage(), cut ? bytes.length : -1, cut, false);
			}
			long framed = (long) CodedOutputStream.computeUInt32SizeNoTag(protoSize) + protoSize;
			if (protoSize < 0 || framed + CHECKSUM_BYTES > maxEntrySize) {
				return new InvalidEntry(position, "entry size " + protoSize + " exceeds the limit of " + maxEntrySize, -1,
						false, false);
			}
			long end = position + framed + CHECKSUM_BYTES;
			if (end > bytes.length) {
				return new InvalidEntry(position, "the entry needs " + (end - position) + " bytes, but the file ends after "
						+ (bytes.length - position), bytes.length, true, true);
			}
			int checksumStart = (int) (position + framed);
			checksum.reset();
			checksum.update(bytes, position, (int) framed);
			int computed = (int) checksum.getValue();
			int stored = ByteBuffer.wrap(bytes, checksumStart, CHECKSUM_BYTES).getInt();
			if (computed != stored) {
				return new InvalidEntry(position, String.format("checksum %08X, computed %08X", stored, computed), end, false,
						false);
			}
			LogEntryProto entry;
			try {
				entry = LogEntryProto.parseFrom(CodedInputStream.newInstance(bytes, checksumStart - protoSize, protoSize));
			} catch (IOException e) {
				return new InvalidEntry(position, "valid checksum over an entry that does not parse: " + e, -1, false, false);
			}
			if (lastIndex != -1 && entry.getIndex() != lastIndex + 1) {
				return new InvalidEntry(position, "index " + entry.getIndex() + " after index " + lastIndex, -1, false, false);
			}
			if (firstIndex == -1) {
				firstIndex = entry.getIndex();
			}
			lastIndex = entry.getIndex();
			entries++;
			position = (int) end;
			return null;
		}
	}

	/** The segments in the directory by start index, without empty open ones: Ratis deletes those when it starts. */
	private static List<Segment> segments(Path currentDir, List<String> lines) throws IOException {
		var segments = new ArrayList<Segment>();
		try (var files = Files.list(currentDir)) {
			for (var file : (Iterable<Path>) files::iterator) {
				var name = file.getFileName().toString();
				var closed = CLOSED_SEGMENT.matcher(name);
				var open = OPEN_SEGMENT.matcher(name);
				if (closed.matches()) {
					segments.add(new Segment(file, Long.parseLong(closed.group(1)), Long.parseLong(closed.group(2))));
				} else if (open.matches() && Files.size(file) > 0) {
					segments.add(new Segment(file, Long.parseLong(open.group(1)), null));
				} else if (open.matches()) {
					lines.add("Ignoring the empty open segment " + name + ": Ratis deletes it when it starts");
				}
			}
		}
		segments.sort(Comparator.comparingLong(Segment::start));
		return segments;
	}

	private static String names(List<Segment> segments) {
		return String.join(", ", segments.stream().map(Segment::name).toList());
	}

	private static String replaceTheNode() {
		return "Nothing was changed: replace the node under a new id (see \"Replacing a node\" in the design document, "
				+ "and \"No quorum\" there if too few nodes are left to elect a leader)";
	}
}
