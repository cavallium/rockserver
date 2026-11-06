package it.cavallium.rockserver.core.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.rockserver.core.common.Failures;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import org.junit.jupiter.api.Test;

/** Recorded failure texts must not depend on the node: no data directory, address, hash or thread name. */
class FailuresSanitizeTest {

	@Test
	void rocksDbStatusWithTheNodesPathIsTheSameOnEveryNode() {
		var node1 = RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
				"IO error: While open a file for random read: /var/lib/rockserver/node-1/db/000012.sst: No such file or directory");
		var node2 = RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
				"IO error: While open a file for random read: /srv/data2/db/000012.sst: No such file or directory");
		assertEquals(Failures.sanitize(node1.getMessage()), Failures.sanitize(node2.getMessage()));
		assertEquals("IO error: While open a file for random read: <path>: No such file or directory",
				Failures.sanitize("IO error: While open a file for random read: /srv/data2/db/000012.sst: No such file or directory"));
	}

	@Test
	void pathsInQuotesBracketsAndWindowsForm() {
		assertEquals("cannot load '<path>' (<path>)",
				Failures.sanitize("cannot load '/home/a/.jni/rocksdb/x/librocksdbjni.so' (/tmp/b/c)"));
		assertEquals("file <path> missing", Failures.sanitize("file C:\\data\\node1\\db missing"));
		assertEquals("jar:file:<path>", Failures.sanitize("jar:file:/opt/app/lib/op.jar!/Op.class"));
	}

	@Test
	void textThatIsNotAPathStays() {
		for (var text : new String[] {"key/value", "1/2 of the keys", "ratio 3/4", "a / b", "/", "missing column /x",
				"expected 4 bytes, got 2"}) {
			assertEquals(text, Failures.sanitize(text), text);
		}
	}

	@Test
	void addressesHashesAndThreadsStillSanitized() {
		assertEquals("MemorySegment{ address: 0x? } of loader@? [in thread ?]",
				Failures.sanitize("MemorySegment{ address: 0x7f12ab } of loader@1a2b3c [in thread \"apply-1\"]"));
	}
}
