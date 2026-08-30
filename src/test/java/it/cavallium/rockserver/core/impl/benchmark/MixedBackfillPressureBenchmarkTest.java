package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(90)
class MixedBackfillPressureBenchmarkTest {

	@TempDir
	Path temporary;

	@Test
	void realRocksDbSmokeCancelsResumesReopensAndRunsEveryMixedLane() throws Exception {
		Path root = temporary.resolve("mixed");
		var options = new MixedBackfillPressureBenchmark.Options(root,
				512, 64, 128, Duration.ofSeconds(1), 1, 1, 4, 4, 2, 128,
				Duration.ofMillis(50), Duration.ofSeconds(3), 20_000,
				Duration.ofSeconds(5), 10_000, 1.0d, 1.0d);

		var result = MixedBackfillPressureBenchmark.run(options);

		assertDoesNotThrow(result::assertCorrect);
		assertEquals(512L, result.cancelledRows() + result.resumedRows());
		assertTrue(result.durableCheckpoints() > 0);
		assertTrue(result.backfillRows() > 0L);
		assertTrue(result.cdcEvents() > 0L);
		assertTrue(result.maintenanceOperations() > 0L);
		assertTrue(Files.isRegularFile(root.resolve("checkpoint.txt")));
		assertThrows(IllegalArgumentException.class, () -> MixedBackfillPressureBenchmark.run(options),
				"an existing root must never be reused or deleted");
	}

	@Test
	void invalidOrUnknownOptionsFailBeforeCreatingRoot() {
		Path root = temporary.resolve("invalid");
		assertThrows(IllegalArgumentException.class, () -> MixedBackfillPressureBenchmark.Options.parse(
				new String[] {"--root=" + root, "--unknown=true"}));
		assertTrue(Files.notExists(root));
	}
}
