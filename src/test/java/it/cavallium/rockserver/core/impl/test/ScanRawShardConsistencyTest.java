package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(90)
class ScanRawShardConsistencyTest {

	private static final String DISABLE_AUTO_COMPACTIONS = "it.cavallium.dbengine.compactions.auto.disable";
	private static final String DISABLE_SLOWDOWN = "it.cavallium.dbengine.disableslowdown";

	@Test
	void nonNegativeShardsExactlyPartitionEverySstFile(@TempDir Path tempDir) throws Exception {
		String previousDisableCompactions = System.getProperty(DISABLE_AUTO_COMPACTIONS);
		String previousDisableSlowdown = System.getProperty(DISABLE_SLOWDOWN);
		System.setProperty(DISABLE_AUTO_COMPACTIONS, "true");
		System.setProperty(DISABLE_SLOWDOWN, "true");
		try {
			Path configFile = tempDir.resolve("scan-raw.conf");
			Files.writeString(configFile,
					"database: { global: { ingest-behind: false, optimistic: false } }");
			try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "scan-raw-shards", configFile)) {
				RocksDBSyncAPI api = connection.getSyncApi();
				long columnId = api.createColumn("events",
						ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));

				// RocksDB file numbers eventually cross digit/hash boundaries. Flushing one unique key per
				// file makes a dropped or duplicated file observable without depending on file ordering.
				for (int i = 0; i < 110; i++) {
					api.put(0, columnId, key(i), value(i * 17), RequestType.none());
					api.flush();
				}

				Map<Integer, Integer> unsharded = scan(api, columnId, 0, 1);
				assertEquals(110, unsharded.size());

				var combined = new LinkedHashMap<Integer, Integer>();
				for (int shard = 0; shard < 7; shard++) {
					for (var entry : scan(api, columnId, shard, 7).entrySet()) {
						assertNull(combined.put(entry.getKey(), entry.getValue()),
								() -> "SST entry appeared in multiple shards: " + entry.getKey());
					}
				}

				assertEquals(unsharded, combined,
						"The union of all non-negative shards must equal the unsharded scan");
			}
		} finally {
			restoreProperty(DISABLE_AUTO_COMPACTIONS, previousDisableCompactions);
			restoreProperty(DISABLE_SLOWDOWN, previousDisableSlowdown);
		}
	}

	private static Map<Integer, Integer> scan(RocksDBSyncAPI api, long columnId, int shard, int shardCount) {
		var result = new LinkedHashMap<Integer, Integer>();
		try (var batches = api.scanRaw(columnId, shard, shardCount)) {
			batches.forEach(batch -> batch.decode().forEach(kv -> {
				int key = ByteBuffer.wrap(kv.keys().keys()[0].toByteArray()).getInt();
				int value = ByteBuffer.wrap(kv.value().toByteArray()).getInt();
				assertNull(result.put(key, value), () -> "Duplicate raw key inside shard: " + key);
			}));
		}
		return result;
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private static void restoreProperty(String name, String previous) {
		if (previous == null) {
			System.clearProperty(name);
		} else {
			System.setProperty(name, previous);
		}
	}
}
