package it.cavallium.rockserver.core.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.rockserver.core.common.Failures;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Recorded failure texts must not depend on the node: no data directory, address, hash or thread name. */
class FailuresSanitizeTest {

	private static String forReplica(String text) {
		return Failures.sanitizeForReplica(text, List.of());
	}

	@Test
	void rocksDbStatusWithTheNodesPathIsTheSameOnEveryNode() {
		var node1 = RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
				"IO error: While open a file for random read: /var/lib/rockserver/node-1/db/000012.sst: No such file or directory");
		var node2 = RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
				"IO error: While open a file for random read: /srv/data2/db/000012.sst: No such file or directory");
		assertEquals(forReplica(node1.getMessage()), forReplica(node2.getMessage()));
		assertEquals("IO error: While open a file for random read: <path>: No such file or directory",
				forReplica("IO error: While open a file for random read: /srv/data2/db/000012.sst: No such file or directory"));
	}

	@Test
	void pathsInQuotesBracketsAndWindowsForm() {
		assertEquals("cannot load '<path>' (<path>)",
				forReplica("cannot load '/home/a/.jni/rocksdb/x/librocksdbjni.so' (/tmp/b/c)"));
		assertEquals("file <path> missing", forReplica("file C:\\data\\node1\\db missing"));
		assertEquals("jar:file:<path>", forReplica("jar:file:/opt/app/lib/op.jar!/Op.class"));
	}

	@Test
	void textThatIsNotAPathStays() {
		for (var text : new String[] {"key/value", "1/2 of the keys", "ratio 3/4", "a / b", "/", "missing column /x",
				"expected 4 bytes, got 2"}) {
			assertEquals(text, forReplica(text), text);
		}
	}

	@Test
	void addressesHashesAndThreadsStillSanitized() {
		assertEquals("MemorySegment{ address: 0x? } of loader@? [in thread ?]",
				Failures.sanitize("MemorySegment{ address: 0x7f12ab } of loader@1a2b3c [in thread \"apply-1\"]"));
	}

	@Test
	void nodeDirectoriesWithWhiteSpaceAreRemovedWhole() {
		var node1 = "IO error: While open a file for random read: /srv/rock data/node1/db/000012.sst: No such file";
		var node2 = "IO error: While open a file for random read: /srv/rock data/node2/db/000012.sst: No such file";
		var sanitized1 = Failures.sanitizeForReplica(node1,
				List.of(Path.of("/srv/rock data/node1/db"), Path.of("/srv/rock data/node1")));
		var sanitized2 = Failures.sanitizeForReplica(node2,
				List.of(Path.of("/srv/rock data/node2/db"), Path.of("/srv/rock data/node2")));
		assertEquals("IO error: While open a file for random read: <path>: No such file", sanitized1);
		assertEquals(sanitized1, sanitized2);
	}

	@Test
	void windowsPathWithForwardSlashes() {
		assertEquals("file <path> missing", forReplica("file D:/data/node1/x missing"));
	}

	@Test
	void singleNodeMessagesKeepTheirPaths() {
		var text = "java.lang.IllegalArgumentException: bad record /users/42/name at 0x7f12 [in thread \"t-1\"]";
		assertEquals("java.lang.IllegalArgumentException: bad record /users/42/name at 0x? [in thread ?]",
				Failures.sanitize(text));
		assertEquals("java.lang.IllegalArgumentException: bad record /users/42/name",
				Failures.describe(new IllegalArgumentException("bad record /users/42/name")));
		assertEquals("java.lang.IllegalArgumentException: bad record <path>",
				Failures.describeForReplica(new IllegalArgumentException("bad record /users/42/name"), List.of()));
	}
}
