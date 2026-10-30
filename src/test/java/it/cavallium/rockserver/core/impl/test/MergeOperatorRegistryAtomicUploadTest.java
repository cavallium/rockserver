package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry.MergeOperatorInfo;
import it.cavallium.rockserver.core.impl.MyStringAppendOperator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

/**
 * Uploads must leave the registry in a state from which replaying the same upload (as the replicated log does
 * after a crash) assigns the same version.
 */
class MergeOperatorRegistryAtomicUploadTest {

	private static final String NAME = "append-op";
	private static final String CLASS_NAME = MyStringAppendOperator.class.getName();

	@TempDir
	Path directory;

	private Options options;
	private RocksDB db;

	@BeforeEach
	void setUp() throws Exception {
		RocksDB.loadLibrary();
		options = new Options().setCreateIfMissing(true);
		db = RocksDB.open(options, directory.resolve("registry").toString());
	}

	@AfterEach
	void tearDown() {
		if (db != null) {
			db.close();
		}
		if (options != null) {
			options.close();
		}
	}

	@Test
	void normalUploadAssignsVersionsAndDeduplicates() throws Exception {
		byte[] firstJar = jar("first");
		byte[] secondJar = jar("second");
		try (var registry = newRegistry()) {
			assertEquals(1, registry.upload(NAME, CLASS_NAME, firstJar));
			assertEquals(1, registry.upload(NAME, CLASS_NAME, firstJar), "identical upload reuses its version");
			assertEquals(2, registry.upload(NAME, CLASS_NAME, secondJar));
			assertEquals(1L, registry.check(NAME, sha256(firstJar)));
			assertEquals(2L, registry.check(NAME, sha256(secondJar)));
			assertEquals(List.of(1L, 2L), versions(registry));
			assertNotNull(registry.get(NAME, 2));
		}
		try (var restarted = newRegistry()) {
			assertEquals(2, restarted.upload(NAME, CLASS_NAME, secondJar), "deduplication survives a restart");
			assertEquals(3, restarted.upload(NAME, CLASS_NAME, jar("third")));
		}
	}

	@Test
	void replayAfterDataWrittenWithoutHashIndexReusesTheVersion() throws Exception {
		byte[] jar = jar("first");
		try (var registry = newRegistry()) {
			assertEquals(1, registry.upload(NAME, CLASS_NAME, jar));
		}
		// The state left by a crash between the data write and the hash index write of the old code
		db.delete(hashKey(jar));

		try (var restarted = newRegistry()) {
			assertNull(restarted.check(NAME, sha256(jar)));
			assertEquals(1, restarted.upload(NAME, CLASS_NAME, jar), "the replayed upload keeps its version");
			assertEquals(1L, restarted.check(NAME, sha256(jar)), "the missing hash index is repaired");
			assertEquals(List.of(1L), versions(restarted));
			assertEquals(2, restarted.upload(NAME, CLASS_NAME, jar("second")));
		}
	}

	@Test
	void replayAfterVersionCounterWrittenWithoutDataReusesTheVersion() throws Exception {
		// The state left by a crash between the version counter write and the data write of the old code
		db.put(("meta:" + NAME).getBytes(StandardCharsets.UTF_8), ByteBuffer.allocate(Long.BYTES).putLong(1).array());

		try (var registry = newRegistry()) {
			assertEquals(1, registry.upload(NAME, CLASS_NAME, jar("first")));
			assertEquals(List.of(1L), versions(registry));
			assertEquals(2, registry.upload(NAME, CLASS_NAME, jar("second")));
		}
	}

	@Test
	void invalidJarIsRejectedWithoutStorageCauseAndWritesNothing() throws Exception {
		byte[] valid = jar("first");
		byte[] truncated = Arrays.copyOf(valid, 60);
		try (var registry = newRegistry()) {
			var error = assertThrows(IllegalArgumentException.class, () -> registry.upload(NAME, CLASS_NAME, truncated));
			assertEquals("Invalid JAR data", error.getMessage());
			assertNoIOExceptionCause(error);
			assertEquals(List.of(), versions(registry));
			assertEquals(1, registry.upload(NAME, CLASS_NAME, valid));
		}
	}

	@Test
	void jarCorruptAfterItsFirstEntryFailsWithoutStorageCause() throws Exception {
		var baos = new ByteArrayOutputStream();
		int truncatedLength;
		try (var jos = new JarOutputStream(baos, new Manifest())) {
			jos.putNextEntry(new ZipEntry("first.txt"));
			jos.write("first".getBytes(StandardCharsets.UTF_8));
			jos.closeEntry();
			jos.putNextEntry(new ZipEntry("second.bin"));
			truncatedLength = baos.size() + 100;
			byte[] incompressible = new byte[4096];
			new Random(42).nextBytes(incompressible);
			jos.write(incompressible);
			jos.closeEntry();
		}
		byte[] corrupt = Arrays.copyOf(baos.toByteArray(), truncatedLength);
		try (var registry = newRegistry()) {
			var error = assertThrows(RocksDBException.class, () -> registry.upload(NAME, CLASS_NAME, corrupt));
			assertNoIOExceptionCause(error);
		}
	}

	@Test
	void failedWriteClosesTheInstantiatedOperator() throws Exception {
		db.close();
		db = RocksDB.openReadOnly(options, directory.resolve("registry").toString());
		int created = CountingAppendOperator.CREATED.get();
		int disposed = CountingAppendOperator.DISPOSED.get();
		try (var registry = newRegistry()) {
			assertThrows(RocksDBException.class,
					() -> registry.upload(NAME, CountingAppendOperator.class.getName(), jar("first")));
			assertEquals(List.of(), versions(registry));
		}
		assertEquals(created + 1, CountingAppendOperator.CREATED.get(), "the operator is instantiated before the write");
		assertEquals(disposed + 1, CountingAppendOperator.DISPOSED.get(), "an operator that is not cached is closed");
	}

	/** Counts its instances and the ones whose native resources were released. */
	public static class CountingAppendOperator extends MyStringAppendOperator {

		static final AtomicInteger CREATED = new AtomicInteger();
		static final AtomicInteger DISPOSED = new AtomicInteger();

		public CountingAppendOperator() {
			CREATED.incrementAndGet();
		}

		@Override
		public void disposeInternal(long handle) {
			try {
				super.disposeInternal(handle);
			} finally {
				DISPOSED.incrementAndGet();
			}
		}
	}

	private static void assertNoIOExceptionCause(Throwable error) {
		for (Throwable t = error; t != null; t = t.getCause()) {
			assertFalse(t instanceof IOException, "the rejection must not look like a storage failure: " + error);
		}
	}

	private MergeOperatorRegistry newRegistry() {
		return new MergeOperatorRegistry(db, db.getDefaultColumnFamily());
	}

	private static List<Long> versions(MergeOperatorRegistry registry) {
		return registry.listAll().stream()
				.filter(info -> info.name().equals(NAME))
				.map(MergeOperatorInfo::version)
				.sorted()
				.toList();
	}

	private static byte[] hashKey(byte[] jar) throws Exception {
		return ("hash:" + NAME + ":" + HexFormat.of().formatHex(sha256(jar))).getBytes(StandardCharsets.UTF_8);
	}

	private static byte[] sha256(byte[] data) throws Exception {
		return MessageDigest.getInstance("SHA-256").digest(data);
	}

	/** A JAR whose content differs by {@code marker}; the operator class itself comes from the parent loader. */
	private static byte[] jar(String marker) throws IOException {
		var manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		var baos = new ByteArrayOutputStream();
		try (var jos = new JarOutputStream(baos, manifest)) {
			jos.putNextEntry(new ZipEntry("marker.txt"));
			jos.write(marker.getBytes(StandardCharsets.UTF_8));
			jos.closeEntry();
		}
		return baos.toByteArray();
	}
}
