package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * R50-2: the operator documents describe the disk layouts a node refuses at start (design.md "Disk layout":
 * {@code NodeStore.checkVolumesInside}, {@code walDir}, {@code ReplicatedNode.requireSameFileSystem}) as refused,
 * not as degraded but accepted.
 */
class DiskLayoutDocsTest {

	@Test
	void performanceDisksSaysTheSplitLayoutsAreRefusedAtStart() throws IOException {
		var performance = Files.readString(docs().resolve("performance.md"));
		int at = performance.indexOf("- **Disks.**");
		assertTrue(at >= 0, "no Disks note in performance.md");
		var disks = performance.substring(at, performance.indexOf("\n- **", at + 1));
		assertFalse(disks.contains("passes the start"), disks);
		assertFalse(disks.contains("with\n  `data/raft` on another disk every snapshot copies"), disks);
		assertTrue(disks.contains("refuses to start"), disks);
		assertTrue(disks.contains("`data/raft` is on another file system"), disks);
		assertTrue(disks.contains("`wal-path` is outside `data/db`"), disks);
	}

	@Test
	void theSeedRunbookNamesNoVolumeOrWalOutsideDb() throws IOException {
		var runbooks = Files.readString(docs().resolve("runbooks.md"));
		assertFalse(runbooks.contains("configured outside `db/`"), "runbooks.md still names a volume or WAL outside db/");
	}

	/** docs/distributed, from the module's directory or any directory above it. */
	private static Path docs() {
		for (var at = Path.of("").toAbsolutePath(); at != null; at = at.getParent()) {
			var candidate = at.resolve("docs").resolve("distributed");
			if (Files.isRegularFile(candidate.resolve("performance.md"))) {
				return candidate;
			}
		}
		throw new AssertionError("docs/distributed/performance.md not found above " + Path.of("").toAbsolutePath());
	}
}
