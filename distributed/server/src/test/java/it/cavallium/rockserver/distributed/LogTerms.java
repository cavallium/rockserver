package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy;
import org.apache.ratis.server.raftlog.segmented.LogSegment;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;

/**
 * Tests only (R3-G4b-3): the term of every entry in a stopped node's Raft log, read from its segment files the way a
 * starting node reads them. With it a test checks Raft's log matching after faults: two logs that hold the same index
 * hold the same term there, and entries that a deposed leader appended without replicating them were replaced.
 */
final class LogTerms {

	private static final Pattern SEGMENT = Pattern.compile("log_(\\d+)-(\\d+)|log_inprogress_(\\d+)");

	private LogTerms() {}

	/** Index to term of every entry in the node's log; the node must not run. An open segment is read up to its end. */
	static TreeMap<Long, Long> read(Path dataDir) throws IOException {
		var current = dataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID).resolve("current");
		var terms = new TreeMap<Long, Long>();
		try (var files = Files.list(current)) {
			for (var file : files.sorted().toList()) {
				if (!SEGMENT.matcher(file.getFileName().toString()).matches()) {
					continue;
				}
				// The open segment's preallocated zeros end it, as at a start; a torn tail is reported, not dropped.
				LogSegment.readSegmentFile(file.toFile(), LogSegmentPath.matchLogSegment(file).getStartEnd(),
						RaftSettings.LOG_ENTRY_SIZE_MAX, CorruptionPolicy.EXCEPTION, null,
						entry -> terms.put(entry.getIndex(), entry.getTerm()));
			}
		}
		return terms;
	}

	/**
	 * The first index that both logs hold with different terms, or -1 if they agree wherever they overlap (purged
	 * prefixes and unreplicated tails do not overlap).
	 */
	static long firstDisagreement(Map<Long, Long> a, Map<Long, Long> b) {
		var sorted = new TreeMap<>(a);
		for (var entry : sorted.entrySet()) {
			var other = b.get(entry.getKey());
			if (other != null && !other.equals(entry.getValue())) {
				return entry.getKey();
			}
		}
		return -1;
	}
}
