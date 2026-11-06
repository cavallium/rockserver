package it.cavallium.rockserver.distributed.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** WA-15: the bench's MERGE operation works against a database with {@link LoadBench#MERGE_CONFIG}. */
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
}
