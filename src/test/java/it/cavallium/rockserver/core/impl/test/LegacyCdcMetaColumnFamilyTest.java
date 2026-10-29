package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBObjects;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

/**
 * Databases created before CDC was removed contain a {@code _cdc_meta_} column family. RocksDB must open every
 * existing column family, so it is still opened, but it is never created for new databases.
 */
class LegacyCdcMetaColumnFamilyTest {

	private static final String LEGACY_CDC_META_COLUMN = "_cdc_meta_";

	@Test
	void freshDatabaseDoesNotCreateTheLegacyCdcColumnFamily(@TempDir Path dbPath) throws Exception {
		var db = new EmbeddedDB(dbPath, "fresh-without-cdc", null);
		db.closeTesting();
		assertFalse(columnFamilies(dbPath).contains(LEGACY_CDC_META_COLUMN));

		var reopened = new EmbeddedDB(dbPath, "fresh-without-cdc-reopen", null);
		reopened.closeTesting();
		assertFalse(columnFamilies(dbPath).contains(LEGACY_CDC_META_COLUMN));
	}

	@Test
	void aWalArchiveLeftByTheOldCdcRetentionIsDeletedOnOpen(@TempDir Path dbPath) throws Exception {
		new EmbeddedDB(dbPath, "archive-layout", null).closeTesting();
		Path walDir = java.nio.file.Files.isDirectory(dbPath.resolve("wal")) ? dbPath.resolve("wal") : dbPath;
		Path archive = walDir.resolve("archive");
		java.nio.file.Files.createDirectories(archive);
		java.nio.file.Files.write(archive.resolve("000099.log"), new byte[1024]);

		new EmbeddedDB(dbPath, "archive-cleanup", null).closeTesting();
		assertFalse(java.nio.file.Files.exists(archive), "the archive of the removed CDC history must be deleted");
	}

	@Test
	void aConfigFileWithTheRemovedCdcSettingsStillLoads(@TempDir Path dbPath) throws Exception {
		Path config = dbPath.resolveSibling(dbPath.getFileName() + "-legacy.conf");
		java.nio.file.Files.writeString(config, """
				database: {
				  global: {
				    wal-ttl-seconds: 86400
				    wal-size-limit-mb: 102400
				  }
				  parallelism: {
				    workload: {
				      cdc-queue-capacity: 1024
				      read-cdc-reservation: 1
				      write-cdc-reservation: 1
				      cdc-drr-weight: 4
				      cdc-quantum-max-mutations: 4096
				      cdc-quantum-max-bytes: 8MiB
				      cdc-quantum-max-duration: PT0.008S
				    }
				  }
				}
				""");
		try {
			new EmbeddedDB(dbPath, "legacy-config", config).closeTesting();
		} finally {
			java.nio.file.Files.deleteIfExists(config);
		}
	}

	@Test
	void theLegacyColumnNameCannotBeCreatedAsAUserColumn(@TempDir Path dbPath) throws Exception {
		var db = new EmbeddedDB(dbPath, "reserved-name", null);
		try {
			org.junit.jupiter.api.Assertions.assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class,
					() -> db.createColumn(LEGACY_CDC_META_COLUMN, it.cavallium.rockserver.core.common.ColumnSchema.of(
							it.unimi.dsi.fastutil.ints.IntList.of(1), it.unimi.dsi.fastutil.objects.ObjectList.of(), true)));
		} finally {
			db.closeTesting();
		}
	}

	@Test
	void databaseWithTheLegacyCdcColumnFamilyStillOpensAndKeepsItsData(@TempDir Path dbPath) throws Exception {
		var key = new Keys(Buf.wrap(new byte[] {1}));
		var value = Buf.wrap(new byte[] {2, 3});
		byte[] legacyKey = "sub:legacy".getBytes(StandardCharsets.UTF_8);
		byte[] legacyValue = new byte[] {4, 5, 6};

		// Recreate the layout of an older database: older versions created _cdc_meta_ next to the
		// other internal column families, with the same internal column-family options.
		var refs = new RocksDBObjects();
		try {
			var db = new EmbeddedDB(dbPath, "legacy-cdc-create", null);
			try {
				long columnId = db.createColumn("data", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
				db.put(0, columnId, key, value, RequestType.none());
				var options = RocksDBLoader.getCompatibilityColumnOptions(refs, dbPath, dbPath.toAbsolutePath(),
						db.getConfig());
				var handle = db.getDb().get().createColumnFamily(new ColumnFamilyDescriptor(
						LEGACY_CDC_META_COLUMN.getBytes(StandardCharsets.UTF_8), options));
				try (var flush = new FlushOptions().setWaitForFlush(true)) {
					db.getDb().get().put(handle, legacyKey, legacyValue);
					db.getDb().get().flush(flush, handle);
				} finally {
					handle.close();
				}
			} finally {
				db.closeTesting();
			}
		} finally {
			refs.close();
		}
		assertTrue(columnFamilies(dbPath).contains(LEGACY_CDC_META_COLUMN));

		var reopened = new EmbeddedDB(dbPath, "legacy-cdc-reopen", null);
		try {
			assertEquals(value, reopened.get(0, reopened.getColumnId("data"), key, RequestType.current()));
			assertFalse(reopened.getAllColumnDefinitions().containsKey(LEGACY_CDC_META_COLUMN),
					"the legacy column family must not be exposed as a user column");
			assertArrayEquals(legacyValue, reopened.getDb().get().get(legacyHandle(reopened), legacyKey),
					"the legacy column family must be opened as-is, not dropped or rewritten");
		} finally {
			reopened.closeTesting();
		}
		assertTrue(columnFamilies(dbPath).contains(LEGACY_CDC_META_COLUMN));
	}

	private static ColumnFamilyHandle legacyHandle(EmbeddedDB db) {
		byte[] name = LEGACY_CDC_META_COLUMN.getBytes(StandardCharsets.UTF_8);
		return db.getDb().getStartupColumns().entrySet().stream()
				.filter(entry -> Arrays.equals(entry.getKey().getName(), name))
				.map(java.util.Map.Entry::getValue)
				.findAny()
				.orElseThrow();
	}

	private static List<String> columnFamilies(Path dbPath) throws Exception {
		try (var options = new Options()) {
			return RocksDB.listColumnFamilies(options, dbPath.toString()).stream()
					.map(name -> new String(name, StandardCharsets.UTF_8))
					.toList();
		}
	}
}
