package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.Callback.CallbackDelta;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Delta;
import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

class EmbeddedDBTest {

	private Path dir;
	private EmbeddedConnection db;

	@org.junit.jupiter.api.BeforeEach
	void setUp() throws IOException {
		this.dir = Files.createTempDirectory("db-test");
		db = new EmbeddedConnection(dir, "test", null);
	}

	@org.junit.jupiter.api.AfterEach
	void tearDown() throws IOException {
		try (Stream<Path> walk = Files.walk(dir)) {
			db.close();
			walk.sorted(Comparator.reverseOrder())
					.map(Path::toFile)
					.peek(System.out::println)
					.forEach(File::delete);
		}
	}

	@org.junit.jupiter.api.Test
	void put() {
		var colId = db.createColumn("put-1", new ColumnSchema(new int[]{16, 16, 16, 32, 32}, 2, true));
		db.put(0, colId, null, null, new CallbackDelta<MemorySegment>() {
			@Override
			public void onSuccess(Delta<MemorySegment> previous) {

			}
		});
		db.deleteColumn(colId);
	}

	@org.junit.jupiter.api.Test
	void get() {
	}
}