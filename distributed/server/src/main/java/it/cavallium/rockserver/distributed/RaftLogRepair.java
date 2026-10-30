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
 * drops by itself.) An entry can only be torn if its fsync never finished, so this node never acknowledged it and may
 * lose it: if the entry was committed, the leader sends it again. Should the storage have lost an entry after its fsync
 * instead, the leader remembers the acknowledgement and keeps sending from past the end of this node's log, so the node
 * never catches up under that leader: nothing is lost by the repair, and the node shows that it must be replaced.</p>
 *
 * <p>The rule is deliberately narrow. The first invalid entry of the open segment is dropped only if every byte after
 * the place where that entry says it ends is zero: a torn last write followed by preallocated zeros, and nothing else.
 * Anything else is refused, because it is not a torn write and could be the loss of durable data: non-zero bytes after
 * the invalid entry (even if they are valid entries of the same group commit, since they cannot be told from later
 * data), a valid checksum over bytes that do not parse, a size that no valid entry has, entries out of order, or any
 * problem in a closed segment, which can hold committed entries. Such a node must be replaced.</p>
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
	private static final String LOCK_FILE = "in_use.lock";
	private static final String CURRENT_DIR = "current";
	/**
	 * What {@code LogSegmentStartEnd} accepts. Its {@code LogSegmentPath.matchLogSegment} cannot list the segments: it
	 * deletes an empty open segment, and a dry run changes nothing.
	 */
	private static final Pattern CLOSED_SEGMENT = Pattern.compile("log_(\\d+)-(\\d+)");
	private static final Pattern OPEN_SEGMENT = Pattern.compile("log_inprogress_(\\d+)(?:\\..*)?");
	private static final int CHECKSUM_BYTES = Integer.BYTES;

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
	 * @param toleratedByRatis Ratis reads the segment as if it ended at {@code offset} instead of failing
	 */
	record InvalidEntry(long offset, String reason, long end, boolean toleratedByRatis) {}

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
		boolean ownsLockFile = !Files.exists(lockFile);
		if (ownsLockFile && dryRun) {
			return inspect(currentDir, true, lines);
		}
		try (var lockChannel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
			try (var lock = lockChannel.tryLock()) {
				if (lock == null) {
					return nodeRunning(lockFile, lines);
				}
				return inspect(currentDir, dryRun, lines);
			} catch (OverlappingFileLockException e) {
				return nodeRunning(lockFile, lines);
			}
		} finally {
			if (ownsLockFile) {
				Files.deleteIfExists(lockFile);
			}
		}
	}

	private static Report nodeRunning(Path lockFile, List<String> lines) {
		lines.add("REFUSED: " + lockFile + " is locked: stop the node first.");
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
		if (!last.equals(segments.getLast())) {
			lines.add("REFUSED: open segment " + last.name() + " is not the last segment. " + replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		return inspectOpenSegment(last, dryRun, lines);
	}

	private static Report inspectOpenSegment(Segment segment, boolean dryRun, List<String> lines) throws IOException {
		long size = Files.size(segment.path());
		if (size > Integer.MAX_VALUE - Long.BYTES) {
			lines.add("REFUSED: " + segment.name() + " has " + size + " bytes: Ratis would never write so many. "
					+ replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		var bytes = Files.readAllBytes(segment.path());
		var scan = scan(bytes, RaftSettings.LOG_ENTRY_SIZE_MAX.getSizeInt());
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
		var notTorn = notATornWrite(invalid, bytes);
		if (notTorn != null) {
			lines.add("REFUSED: " + notTorn + ". This is not a torn last write, and dropping it could lose entries "
					+ "this node reported as durable. " + replaceTheNode());
			return new Report(Outcome.REFUSED, lines);
		}
		long expectedIndex = scan.entries() > 0 ? scan.lastIndex() + 1 : segment.start();
		var dropped = "the torn entry expected to have index " + expectedIndex + " at offset " + invalid.offset() + ", and "
				+ (bytes.length - invalid.offset()) + " bytes from there to the end of the file";
		if (dryRun) {
			lines.add("WOULD REPAIR (dry run, nothing changed): would drop " + dropped);
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
		return new Report(Outcome.REPAIRED, lines);
	}

	/** Why the entry is not a torn last write, null if it is one. */
	private static @Nullable String notATornWrite(InvalidEntry invalid, byte[] bytes) {
		if (invalid.end() < 0) {
			return "the entry has no size that a torn write could leave";
		}
		int end = (int) invalid.end();
		int nonZero = SegmentedRaftLogFormat.indexOfNonTerminator(bytes, end, bytes.length - end);
		return nonZero < 0 ? null : "non-zero data at offset " + (end + nonZero) + ", after the end of the invalid entry";
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
			return new Scan(0, -1, -1, 0, new InvalidEntry(0, "bad segment header", -1,
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
						+ " is not zero", -1, false);
			}
			var in = new ByteArrayInputStream(bytes, position + 1, bytes.length - position - 1);
			int protoSize;
			try {
				protoSize = CodedInputStream.readRawVarint32(bytes[position], in);
			} catch (IOException e) {
				boolean cut = in.available() == 0;
				return new InvalidEntry(position, "bad entry size: " + e.getMessage(), cut ? bytes.length : -1, false);
			}
			long framed = (long) CodedOutputStream.computeUInt32SizeNoTag(protoSize) + protoSize;
			if (protoSize < 0 || framed + CHECKSUM_BYTES > maxEntrySize) {
				return new InvalidEntry(position, "entry size " + protoSize + " exceeds the limit of " + maxEntrySize, -1,
						false);
			}
			long end = position + framed + CHECKSUM_BYTES;
			if (end > bytes.length) {
				return new InvalidEntry(position, "the entry needs " + (end - position) + " bytes, but the file ends after "
						+ (bytes.length - position), bytes.length, true);
			}
			int checksumStart = (int) (position + framed);
			checksum.reset();
			checksum.update(bytes, position, (int) framed);
			int computed = (int) checksum.getValue();
			int stored = ByteBuffer.wrap(bytes, checksumStart, CHECKSUM_BYTES).getInt();
			if (computed != stored) {
				return new InvalidEntry(position, String.format("checksum %08X, computed %08X", stored, computed), end, false);
			}
			LogEntryProto entry;
			try {
				entry = LogEntryProto.parseFrom(CodedInputStream.newInstance(bytes, checksumStart - protoSize, protoSize));
			} catch (IOException e) {
				return new InvalidEntry(position, "valid checksum over an entry that does not parse: " + e, -1, false);
			}
			if (lastIndex != -1 && entry.getIndex() != lastIndex + 1) {
				return new InvalidEntry(position, "index " + entry.getIndex() + " after index " + lastIndex, -1, false);
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
		return "Nothing was changed: replace the node under a new id (see \"Replacing a node\" in the design document)";
	}
}
