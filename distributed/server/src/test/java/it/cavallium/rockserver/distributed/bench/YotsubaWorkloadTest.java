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

	@Test
	void mergesAppendTheirOperands() throws Exception {
		var config = temp.resolve("merge.conf");
		Files.writeString(config, LoadBench.MERGE_CONFIG);
		try (var connection = new EmbeddedConnection(temp.resolve("db"), "merge-bench-test", config)) {
			var api = connection.getAsyncApi(RequestContext.batch(Duration.ofSeconds(20)));
			// One chat: every merge goes to one of its 16 slots.
			var workload = new YotsubaWorkload(1, 1, 16, 0);
			workload.createColumns(api);
			var stats = new BenchStats().op("MERGE");
			int merges = 64;
			for (int i = 0; i < merges; i++) {
				assertEquals(20, workload.run(YotsubaWorkload.Kind.MERGE, api, stats));
			}
			int operands = 0;
			for (int slot = 0; slot < 16; slot++) {
				Buf value = api.getAsync(0, workload.mergeAppends,
						new Keys(YotsubaWorkload.fixed(8, 0), YotsubaWorkload.fixed(4, slot)),
						RequestType.current()).join();
				if (value != null) {
					var parts = new String(value.toByteArray(), StandardCharsets.US_ASCII).split(",");
					for (var part : parts) {
						assertTrue(part.matches("\\d{8}"), part);
					}
					operands += parts.length;
				}
			}
			assertEquals(merges, operands, "each merge appended its operand");
		}
	}
}
