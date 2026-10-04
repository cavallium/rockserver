package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBObjects;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.DbPath;

class RocksDBCompatibilityOptionsTest {

	private static final List<String> STARTUP_COLUMN_FAMILIES = List.of(
			"default",
			"_column_schemas_",
			"_merge_operators_",
			"_cdc_meta_"
	);

	@Test
	void persistsRocksDb10CompatibleTableFormatForEveryStartupColumnFamily(@TempDir Path dbPath)
			throws Exception {
		assertInternalColumnFamilyCompatibilityOptions();

		var db = new EmbeddedDB(dbPath, "compatibility-options", null);
		db.closeTesting();
		assertCompatibleStartupColumnFamilies(latestOptions(dbPath), "initial creation");

		var reopenedDb = new EmbeddedDB(dbPath, "compatibility-options-reopen", null);
		reopenedDb.closeTesting();
		assertCompatibleStartupColumnFamilies(latestOptions(dbPath), "reopen");
	}

	@Test
	void reopensFlushedInternalFamiliesWithDefaultVolume(@TempDir Path tempDir) throws Exception {
		assertFlushedInternalFamiliesReopen(tempDir, Fixture.DEFAULT);
	}

	@Test
	void reopensFlushedInternalFamiliesWithSeparateFallbackVolume(@TempDir Path tempDir) throws Exception {
		assertFlushedInternalFamiliesReopen(tempDir, Fixture.SEPARATE);
	}

	@Test
	void preservesExplicitInternalFamilyVolumesOnReopen(@TempDir Path tempDir) throws Exception {
		assertFlushedInternalFamiliesReopen(tempDir, Fixture.NAMED);
	}

	@Test
	void reopensLegacyExternalInternalFamiliesAfterRemovingNamedConfigs(@TempDir Path tempDir) throws Exception {
		assertFlushedInternalFamiliesReopen(tempDir, Fixture.LEGACY);
	}

	@Test
	void usesFallbackVolumesWhenDefaultHasDifferentNamedVolumes(@TempDir Path tempDir) throws Exception {
		assertFlushedInternalFamiliesReopen(tempDir, Fixture.NAMED_DEFAULT);
	}

	@Test
	void reopensInternalFamiliesWithDatabaseRootFallback(@TempDir Path tempDir) throws Exception {
		assertFlushedInternalFamiliesReopen(tempDir, Fixture.ROOT_FALLBACK);
	}

	@Test
	void honorsExplicitRootInternalOverrideWithExternalFallback(@TempDir Path tempDir) throws Exception {
		assertFlushedInternalFamiliesReopen(tempDir, Fixture.ROOT_OVERRIDE);
	}

	@Test
	void inMemoryCompatibilityOptionsDoNotConfigurePathsOrCreateDirectories(@TempDir Path tempDir) throws Exception {
		Path configPath = tempDir.resolve("memory.conf");
		Files.writeString(configPath, "database.global.fallback-column-options.volumes: ["
				+ "{ volume-path: \"unused-volume\", target-size: \"1GiB\" }]");
		try (var refs = new RocksDBObjects()) {
			var options = RocksDBLoader.getCompatibilityColumnOptions(refs, null, tempDir, ConfigParser.parse(configPath));
			assertTrue(options.cfPaths().isEmpty());
			assertFalse(Files.exists(tempDir.resolve("unused-volume")));
		}
	}

	private enum Fixture { DEFAULT, SEPARATE, NAMED, LEGACY, NAMED_DEFAULT, ROOT_FALLBACK, ROOT_OVERRIDE }

	private static void assertFlushedInternalFamiliesReopen(Path tempDir, Fixture fixture) throws Exception {
		boolean namedInternalFamilies = fixture == Fixture.NAMED || fixture == Fixture.LEGACY || fixture == Fixture.ROOT_OVERRIDE;
		Path dbPath = tempDir.resolve("db");
		Path configPath = null;
		String fallbackConfig = null;
		List<DbPath> internalPaths = null;
		Path expectedInternalDirectory = dbPath.resolve("volume");
		if (fixture != Fixture.DEFAULT) {
			configPath = tempDir.resolve("database.conf");
			String fallbackPath = fixture == Fixture.ROOT_FALLBACK ? "." : "../user-volume";
			fallbackConfig = "database.global: { fallback-column-options: { volumes: ["
					+ "{ volume-path: \"" + fallbackPath + "\", target-size: \"1GiB\" },"
					+ "{ volume-path: \"../user-overflow\", target-size: \"1GiB\" }] }, column-options: [";
			String namedConfigs = "";
			String internalPath = fixture == Fixture.NAMED ? "../internal-volume"
					: fixture == Fixture.ROOT_OVERRIDE ? "." : fallbackPath;
			String internalOverflow = fixture == Fixture.NAMED || fixture == Fixture.ROOT_OVERRIDE
					? "../internal-overflow" : "../user-overflow";
			internalPaths = List.of(new DbPath(dbPath.resolve(internalPath).toAbsolutePath(), 1L << 30),
					new DbPath(dbPath.resolve(internalOverflow).toAbsolutePath(), 1L << 30));
			expectedInternalDirectory = dbPath.resolve(internalPath).normalize();
			if (namedInternalFamilies) {
				for (String name : STARTUP_COLUMN_FAMILIES.subList(1, STARTUP_COLUMN_FAMILIES.size())) {
					namedConfigs += "{ name: \"" + name + "\", volumes: ["
							+ "{ volume-path: \"" + internalPath + "\", target-size: \"1GiB\" },"
							+ "{ volume-path: \"" + internalOverflow + "\", target-size: \"1GiB\" }], levels: [] },";
				}
			}
			if (fixture == Fixture.NAMED_DEFAULT) {
				namedConfigs += "{ name: \"default\", volumes: [{ volume-path: \"../default-volume\", target-size: \"1GiB\" }], levels: [] }";
			}
			Files.writeString(configPath, fallbackConfig + namedConfigs + "] }");
		}
		var key = new Keys(Buf.wrap(new byte[]{1}));
		var value = Buf.wrap(new byte[]{2, 3});
		Map<String, List<byte[]>> persistedMetadata = new LinkedHashMap<>();
		long cdcCommitted;
		long operatorVersion;
		byte[] operatorHash;
		var db = new EmbeddedDB(dbPath, "internal-paths", configPath);
		try {
			if (namedInternalFamilies) assertInternalPaths(db, internalPaths);
			long columnId = db.createColumn("data", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
			db.put(0, columnId, key, value, RequestType.none());
			var jarBytes = new ByteArrayOutputStream();
			try (var jar = new JarOutputStream(jarBytes, new Manifest())) {
			}
			operatorHash = java.security.MessageDigest.getInstance("SHA-256").digest(jarBytes.toByteArray());
			operatorVersion = db.uploadMergeOperator("reopen-operator", HotSwapMergeOperatorTest.OperatorA.class.getName(),
					jarBytes.toByteArray());
			db.cdcCreate("reopen-subscription", null, List.of(columnId), false, OptionalLong.empty());
			cdcCommitted = db.cdcGetLastCommittedSequence("reopen-subscription").orElseThrow();
			db.flush();
			for (String family : STARTUP_COLUMN_FAMILIES.subList(1, STARTUP_COLUMN_FAMILIES.size())) {
				var handle = internalHandle(db, family);
				try (var flush = new FlushOptions().setWaitForFlush(true)) {
					db.getDb().get().flush(flush, handle);
				}
				db.getDb().get().compactRange(handle);
				persistedMetadata.put(family, metadata(db, handle));
				assertFalse(persistedMetadata.get(family).isEmpty(), family + " must contain real metadata");
				assertSstDirectory(db, family, expectedInternalDirectory);
			}
			assertSstDirectory(db, "data", fixture == Fixture.DEFAULT ? dbPath.resolve("volume")
					: fixture == Fixture.ROOT_FALLBACK ? dbPath : tempDir.resolve("user-volume"));
		} finally {
			db.closeTesting();
		}

		if (fixture == Fixture.LEGACY) Files.writeString(configPath, fallbackConfig + "] }");
		var reopened = new EmbeddedDB(dbPath, "internal-paths-reopen", configPath);
		try {
			if (internalPaths != null) assertInternalPaths(reopened, internalPaths);
			assertEquals(value, reopened.get(0, reopened.getColumnId("data"), key, RequestType.current()));
			assertEquals(cdcCommitted, reopened.cdcGetLastCommittedSequence("reopen-subscription").orElseThrow());
			assertEquals(operatorVersion, reopened.checkMergeOperator("reopen-operator", operatorHash));
			for (var entry : persistedMetadata.entrySet()) {
				var restored = metadata(reopened, internalHandle(reopened, entry.getKey()));
				assertEquals(entry.getValue().size(), restored.size(), entry.getKey());
				for (int i = 0; i < restored.size(); i++) {
					assertArrayEquals(entry.getValue().get(i), restored.get(i), entry.getKey());
				}
			}
		} finally {
			reopened.closeTesting();
		}
	}

	private static ColumnFamilyHandle internalHandle(EmbeddedDB db, String family) throws Exception {
		String fieldName = switch (family) {
			case "_column_schemas_" -> "columnSchemasColumnDescriptorHandle";
			case "_merge_operators_" -> "mergeOperatorsColumnDescriptorHandle";
			case "_cdc_meta_" -> "cdcMetaColumnDescriptorHandle";
			default -> throw new IllegalArgumentException(family);
		};
		var field = EmbeddedDB.class.getDeclaredField(fieldName);
		field.setAccessible(true);
		return (ColumnFamilyHandle) field.get(db);
	}

	private static List<byte[]> metadata(EmbeddedDB db, ColumnFamilyHandle handle) throws Exception {
		var result = new ArrayList<byte[]>();
		try (var iterator = db.getDb().get().newIterator(handle)) {
			for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
				result.add(iterator.key());
				result.add(iterator.value());
			}
			iterator.status();
		}
		return result;
	}

	private static void assertInternalPaths(EmbeddedDB db, List<DbPath> expectedPaths) {
		for (var descriptor : db.getDb().getStartupColumns().keySet()) {
			String name = new String(descriptor.getName(), StandardCharsets.UTF_8);
			if (STARTUP_COLUMN_FAMILIES.subList(1, STARTUP_COLUMN_FAMILIES.size()).contains(name)) {
				assertEquals(expectedPaths, descriptor.getOptions().cfPaths(), name);
			}
		}
	}

	private static void assertSstDirectory(EmbeddedDB db, String family, Path directory) {
		var files = db.getDb().get().getLiveFilesMetaData().stream()
				.filter(file -> family.equals(new String(file.columnFamilyName(), StandardCharsets.UTF_8)))
				.toList();
		assertFalse(files.isEmpty(), family + " must have flushed SSTs");
		for (var file : files) {
			assertEquals(directory.toAbsolutePath().normalize(), Path.of(file.path()).toAbsolutePath().normalize(), family);
		}
	}

	private static String latestOptions(Path dbPath) throws Exception {
		try (var optionFiles = Files.list(dbPath)) {
			Path latestOptions = optionFiles
					.filter(file -> file.getFileName().toString().startsWith("OPTIONS-"))
					.max(Path::compareTo)
					.orElseThrow();
			return Files.readString(latestOptions);
		}
	}

	private static void assertCompatibleStartupColumnFamilies(String persistedOptions, String phase) {
		for (String columnFamily : STARTUP_COLUMN_FAMILIES) {
			assertEquals(1, occurrences(persistedOptions, "[CFOptions \"" + columnFamily + "\"]"),
					() -> "column family must be described exactly once after " + phase + ": " + columnFamily);
			String tableOptions = section(persistedOptions,
					"[TableOptions/BlockBasedTable \"" + columnFamily + "\"]");
			assertTrue(tableOptions.contains("format_version=6"),
					() -> "RocksDB 10.10-compatible format is missing after " + phase + " for " + columnFamily);
			assertFalse(tableOptions.contains("format_version=7"),
					() -> "RocksDB 11 table format leaked after " + phase + " into " + columnFamily);
			assertFalse(tableOptions.contains("uniform_cv_threshold=0.200000"),
					() -> "RocksDB 11 index footer default leaked after " + phase + " into " + columnFamily);
		}
	}

	private static void assertInternalColumnFamilyCompatibilityOptions() {
		try (var refs = new RocksDBObjects()) {
			var options = RocksDBLoader.getCompatibilityColumnOptions(refs);
			var tableOptions = assertInstanceOf(BlockBasedTableConfig.class, options.tableFormatConfig());
			assertEquals(6, tableOptions.formatVersion());
			assertEquals(-1.0d, tableOptions.uniformCvThreshold(),
					"internal column families must not emit the RocksDB 11 index-footer flag");
		}
	}

	private static int occurrences(String value, String needle) {
		int count = 0;
		int offset = 0;
		while ((offset = value.indexOf(needle, offset)) >= 0) {
			count++;
			offset += needle.length();
		}
		return count;
	}

	private static String section(String options, String header) {
		int start = options.indexOf(header);
		assertTrue(start >= 0, () -> "missing options section " + header);
		int end = options.indexOf("\n[", start + header.length());
		return end >= 0 ? options.substring(start, end) : options.substring(start);
	}
}
