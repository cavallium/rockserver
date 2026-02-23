package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for columns with no fixed keys, two XXHASH32 variable keys, and no value.
 * Schema: Fixed:0, Var:[XXHASH32,XXHASH32], Val:N
 */
class NoFixedKeyTwoVarKeyNoValueColumnTest {

	@TempDir
	Path tempDir;

	private EmbeddedDB db;

	private ColumnSchema schema() {
		return ColumnSchema.of(
				IntList.of(),
				ObjectArrayList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH32),
				false
		);
	}

	@BeforeEach
	void setUp() throws IOException {
		db = new EmbeddedDB(tempDir, "test-db", null);
	}

	@AfterEach
	void tearDown() throws IOException {
		if (db != null) db.closeTesting();
	}

	@Test
	void testCreateColumnDynamically() {
		var schema = schema();
		long colId = db.createColumn("file_locations_3", schema);
		assertTrue(colId > 0);

		// Idempotent: creating again with same schema returns same id
		long colId2 = db.createColumn("file_locations_3", schema);
		assertEquals(colId, colId2);
	}

	@Test
	void testLoadFromDisk() throws IOException {
		var schema = schema();
		long colId = db.createColumn("file_locations_3", schema);
		assertTrue(colId > 0);

		// Close and reopen
		db.closeTesting();
		db = new EmbeddedDB(tempDir, "test-db", null);

		// Column should be loadable and re-creatable with same schema
		long colId2 = db.createColumn("file_locations_3", schema);
		assertEquals(colId, colId2);
	}

	@Test
	void testMixedCreateAndLoad() throws IOException {
		var schema = schema();

		// Create first column
		long colId1 = db.createColumn("file_locations", schema);
		assertTrue(colId1 > 0);

		// Close and reopen
		db.closeTesting();
		db = new EmbeddedDB(tempDir, "test-db", null);

		// Loaded column should still work
		long loadedId1 = db.createColumn("file_locations", schema);
		assertEquals(colId1, loadedId1);

		// Create a second column dynamically after reload
		long colId2 = db.createColumn("file_locations_3", schema);
		assertTrue(colId2 > 0);
		assertNotEquals(colId1, colId2);

		// Close and reopen again
		db.closeTesting();
		db = new EmbeddedDB(tempDir, "test-db", null);

		// Both columns should be loadable
		long reloadedId1 = db.createColumn("file_locations", schema);
		long reloadedId2 = db.createColumn("file_locations_3", schema);
		assertEquals(colId1, reloadedId1);
		assertEquals(colId2, reloadedId2);
	}

	@Test
	void testPutAndGetWithNoValue() {
		var schema = schema();
		long colId = db.createColumn("file_locations_3", schema);

		Buf varKey1 = Buf.wrap("path/to/file".getBytes());
		Buf varKey2 = Buf.wrap("thumbnail.jpg".getBytes());

		// Put with no value (null value since hasValue=false)
		db.put(0, colId, new Keys(new Buf[]{varKey1, varKey2}), Buf.create(0), new RequestType.RequestNothing<>());

		// Verify existence via get
		var exists = db.get(0, colId, new Keys(new Buf[]{varKey1, varKey2}), new RequestType.RequestExists<>());
		assertTrue(exists);
	}

	@Test
	void testPutMultipleAndGetRange() {
		var schema = schema();
		long colId = db.createColumn("file_locations_3", schema);

		for (int i = 0; i < 5; i++) {
			Buf varKey1 = Buf.wrap(("path" + i).getBytes());
			Buf varKey2 = Buf.wrap(("file" + i).getBytes());
			db.put(0, colId, new Keys(new Buf[]{varKey1, varKey2}), Buf.create(0), new RequestType.RequestNothing<>());
		}

		List<KV> results = db.getRange(0, colId, null, null, false, new RequestType.RequestGetAllInRange<>(), 1000)
				.collect(Collectors.toList());

		assertEquals(5, results.size());
		for (KV kv : results) {
			assertEquals(2, kv.keys().keys().length);
		}
	}

	@Test
	void testEntriesCount() {
		var schema = schema();
		long colId = db.createColumn("file_locations_3", schema);

		for (int i = 0; i < 3; i++) {
			Buf varKey1 = Buf.wrap(("a" + i).getBytes());
			Buf varKey2 = Buf.wrap(("b" + i).getBytes());
			db.put(0, colId, new Keys(new Buf[]{varKey1, varKey2}), Buf.create(0), new RequestType.RequestNothing<>());
		}

		Long count = db.reduceRange(0, colId, null, null, false, new RequestType.RequestEntriesCount<>(), 1000);
		assertEquals(3L, count);
	}

	@Test
	void testPersistenceOfData() throws IOException {
		var schema = schema();
		long colId = db.createColumn("file_locations_3", schema);

		Buf varKey1 = Buf.wrap("persistent-path".getBytes());
		Buf varKey2 = Buf.wrap("persistent-file".getBytes());
		db.put(0, colId, new Keys(new Buf[]{varKey1, varKey2}), Buf.create(0), new RequestType.RequestNothing<>());

		// Close and reopen
		db.closeTesting();
		db = new EmbeddedDB(tempDir, "test-db", null);

		long colId2 = db.createColumn("file_locations_3", schema);
		assertEquals(colId, colId2);

		var exists = db.get(0, colId2, new Keys(new Buf[]{varKey1, varKey2}), new RequestType.RequestExists<>());
		assertTrue(exists);
	}
}
