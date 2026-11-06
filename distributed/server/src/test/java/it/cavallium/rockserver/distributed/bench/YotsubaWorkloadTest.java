package it.cavallium.rockserver.distributed.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * WA-15: the bench's MERGE operation works against a database with {@link LoadBench#MERGE_CONFIG}; WA-1: each of its
 * DELETE_RANGE calls fills a window of its own and then deletes exactly that window, also when calls overlap.
 */
class YotsubaWorkloadTest {

	@TempDir
	Path temp;

	/**
	 * Each merge appends its operand, and no slot takes more than {@link YotsubaWorkload#MERGES_PER_SLOT} merges: past
	 * {@code MERGE_SLOTS * MERGES_PER_SLOT} merges a chat goes on to new slots, so that values stop growing.
	 */
	@Test
	void mergesAppendTheirOperandsToBoundedSlots() throws Exception {
		var config = temp.resolve("merge.conf");
		Files.writeString(config, LoadBench.MERGE_CONFIG);
		try (var connection = new EmbeddedConnection(temp.resolve("db"), "merge-bench-test", config)) {
			var api = connection.getAsyncApi(RequestContext.batch(Duration.ofSeconds(20)));
			// One chat: every merge goes to one of its slots.
			var workload = new YotsubaWorkload(1, 1, 16, 0);
			workload.createColumns(api);
			var stats = new BenchStats().op("MERGE");
			int window = YotsubaWorkload.MERGE_SLOTS * YotsubaWorkload.MERGES_PER_SLOT;
			int merges = window + 2 * YotsubaWorkload.MERGE_SLOTS;
			for (int i = 0; i < merges; i++) {
				assertEquals(20, workload.run(YotsubaWorkload.Kind.MERGE, api, stats));
			}
			int operands = 0;
			for (int slot = 0; slot < 3 * YotsubaWorkload.MERGE_SLOTS; slot++) {
				Buf value = api.getAsync(0, workload.mergeAppends,
						new Keys(YotsubaWorkload.fixed(8, 0), YotsubaWorkload.fixed(4, slot)),
						RequestType.current()).join();
				int count = 0;
				if (value != null) {
					var parts = new String(value.toByteArray(), StandardCharsets.US_ASCII).split(",");
					for (var part : parts) {
						assertTrue(part.matches("\\d{8}"), part);
					}
					count = parts.length;
				}
				int expected = slot < YotsubaWorkload.MERGE_SLOTS ? YotsubaWorkload.MERGES_PER_SLOT
						: slot < 2 * YotsubaWorkload.MERGE_SLOTS ? 2 : 0;
				assertEquals(expected, count, "operands in slot " + slot);
				operands += count;
			}
			assertEquals(merges, operands, "each merge appended its operand");
		}
	}

	/**
	 * Each call takes the next window of its chat, fills it and deletes it: when its deleteRange is sent the window
	 * holds its 16 keys (an empty-range delete would measure nothing), and after the call it is empty.
	 */
	@Test
	void eachCallFillsItsOwnWindowAndThenDeletesIt() throws Exception {
		try (var connection = new EmbeddedConnection(temp.resolve("db"), "delete-range-bench-test", null)) {
			var real = connection.getAsyncApi(RequestContext.batch(Duration.ofSeconds(20)));
			var workload = new YotsubaWorkload(1, 1, 16, 0);
			workload.createColumns(real);
			int keys = YotsubaWorkload.RANGE_DELETE_KEYS;
			var liveAtDelete = new java.util.ArrayList<Integer>();
			// Counts the live slots of all windows touched so far at the moment each deleteRange is sent.
			var api = (RocksDBAsyncAPI) java.lang.reflect.Proxy.newProxyInstance(RocksDBAsyncAPI.class.getClassLoader(),
					new Class<?>[] {RocksDBAsyncAPI.class}, (proxy, method, args) -> {
						if (method.getName().equals("deleteRangeAsync")) {
							liveAtDelete.add(liveSlots(real, workload, 0, 4 * keys));
						}
						try {
							return method.invoke(real, args);
						} catch (java.lang.reflect.InvocationTargetException e) {
							throw e.getCause();
						}
					});
			var stats = new BenchStats().op("DELETE_RANGE");
			for (int window = 0; window < 3; window++) {
				assertEquals(window, workload.rangeDeleteStep(0));
				assertEquals(window, YotsubaWorkload.rangeDeleteWindow(window));
				assertEquals(keys * 28L + 24, workload.run(YotsubaWorkload.Kind.DELETE_RANGE, api, stats));
				assertEquals(window + 1, liveAtDelete.size(), "one deleteRange per call");
				assertEquals(keys, liveAtDelete.get(window), "window " + window + " was filled before its delete");
				assertEquals(0, liveSlots(real, workload, 0, 4 * keys), "the call emptied window " + window);
			}
		}
	}

	/**
	 * Many workers calling {@code run} at once on one chat, as LoadBench's workers do on a hot chat: once every call
	 * returned, no window may still hold keys, however the calls overlapped, so that the column does not grow.
	 */
	@Test
	void overlappingCallsOnOneChatLeaveNoKeysBehind() throws Exception {
		try (var connection = new EmbeddedConnection(temp.resolve("db"), "delete-range-overlap-test", null)) {
			var api = connection.getAsyncApi(RequestContext.batch(Duration.ofSeconds(20)));
			var workload = new YotsubaWorkload(1, 1, 16, 0);
			workload.createColumns(api);
			var stats = new BenchStats().op("DELETE_RANGE");
			int workers = 16;
			int callsPerWorker = 200;
			var pool = java.util.concurrent.Executors.newFixedThreadPool(workers);
			try {
				var start = new java.util.concurrent.CountDownLatch(1);
				var done = new java.util.ArrayList<java.util.concurrent.Future<?>>();
				for (int w = 0; w < workers; w++) {
					done.add(pool.submit(() -> {
						start.await();
						for (int i = 0; i < callsPerWorker; i++) {
							workload.run(YotsubaWorkload.Kind.DELETE_RANGE, api, stats);
						}
						return null;
					}));
				}
				start.countDown();
				for (var future : done) {
					future.get();
				}
			} finally {
				pool.shutdownNow();
			}
			// workers * callsPerWorker is even: a fill/delete alternation would also have paired every window.
			int slots = (workload.rangeDeleteStep(0) + 1) * YotsubaWorkload.RANGE_DELETE_KEYS;
			assertEquals(0, liveSlots(api, workload, 0, slots), "keys left behind by overlapping calls");
		}
	}

	private static int liveSlots(RocksDBAsyncAPI api, YotsubaWorkload workload,
			int from, int to) {
		int live = 0;
		for (int slot = Math.max(0, from); slot < to; slot++) {
			if (api.getAsync(0, workload.rangeDeletes, new Keys(YotsubaWorkload.fixed(8, 0), YotsubaWorkload.fixed(4, slot)),
					RequestType.current()).join() != null) {
				live++;
			}
		}
		return live;
	}
}
