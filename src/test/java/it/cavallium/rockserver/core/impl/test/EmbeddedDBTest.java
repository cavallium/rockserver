package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.Callback;
import it.cavallium.rockserver.core.common.Callback.CallbackDelta;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Utils;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicInteger;

class EmbeddedDBTest {

	private EmbeddedConnection db;
	private long colId;

	@org.junit.jupiter.api.BeforeEach
	void setUp() throws IOException {
		if (System.getProperty("rockserver.core.print-config", null) == null) {
			System.setProperty("rockserver.core.print-config", "false");
		}
		db = new EmbeddedConnection(null, "test", null);
		this.colId = db.createColumn("put-1", new ColumnSchema(new int[]{1, 2, 1, Integer.BYTES, Integer.BYTES}, 2, true));
	}

	@org.junit.jupiter.api.AfterEach
	void tearDown() throws IOException {
		db.deleteColumn(colId);
		db.close();
	}

	@org.junit.jupiter.api.Test
	void put() {
		var key = new MemorySegment[] {
				Utils.toMemorySegment(new byte[] {3}),
				Utils.toMemorySegment(new byte[] {4, 6}),
				Utils.toMemorySegment(new byte[] {3}),
				Utils.toMemorySegment(new byte[] {1, 2, 3}),
				Utils.toMemorySegment(new byte[] {0, 0, 3, 6, 7, 8})
		};
		var value1 = MemorySegment.ofArray(new byte[] {0, 0, 3});
		var value2 = MemorySegment.ofArray(new byte[] {0, 0, 5});

		var delta = db.put(0, colId, key, value1, Callback.delta());
		Assertions.assertNull(delta.previous());
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value1));

		delta = db.put(0, colId, key, value2, Callback.delta());
		Assertions.assertTrue(Utils.valueEquals(delta.previous(), value1));
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value2));
	}

	@org.junit.jupiter.api.Test
	void get() {
		throw new UnsupportedOperationException();
	}
}