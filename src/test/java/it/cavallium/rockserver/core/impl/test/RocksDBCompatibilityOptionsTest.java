package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
		var db = new EmbeddedDB(dbPath, "compatibility-options", null);
		db.closeTesting();
		assertCompatibleStartupColumnFamilies(latestOptions(dbPath), "initial creation");

		var reopenedDb = new EmbeddedDB(dbPath, "compatibility-options-reopen", null);
		reopenedDb.closeTesting();
		assertCompatibleStartupColumnFamilies(latestOptions(dbPath), "reopen");
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
			assertTrue(tableOptions.contains("uniform_cv_threshold=-1.000000"),
					() -> "RocksDB 10.10-compatible index footer setting is missing after " + phase
							+ " for " + columnFamily);
			assertFalse(tableOptions.contains("format_version=7"),
					() -> "RocksDB 11 table format leaked after " + phase + " into " + columnFamily);
			assertFalse(tableOptions.contains("uniform_cv_threshold=0.200000"),
					() -> "RocksDB 11 index footer default leaked after " + phase + " into " + columnFamily);
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
