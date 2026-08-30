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
				512, 64, 128, Duration.ofSeconds(1), 1, 1, 4, 4, 2, 2, 128,
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
	void parserDefaultsToOneAndAcceptsOnlySafeExplicitPressuredCaps() {
		Path root = temporary.resolve("invalid");
		var defaults = MixedBackfillPressureBenchmark.Options.parse(new String[] {"--root=" + root});
		assertEquals(8_192L, defaults.cdcBatchSize());
		assertEquals(1, defaults.pressuredBatchMaximumActive(),
				"omitting the option must preserve the historical single-permit behavior");
		var explicit = MixedBackfillPressureBenchmark.Options.parse(new String[] {"--root=" + root,
				"--read-workers=3", "--write-workers=3", "--pressured-batch-maximum-active=6"});
		assertEquals(6, explicit.pressuredBatchMaximumActive(),
				"the combined data-pool capacity is the inclusive safe upper bound");
		assertThrows(IllegalArgumentException.class, () -> MixedBackfillPressureBenchmark.Options.parse(
				new String[] {"--root=" + root, "--unknown=true"}));
		assertThrows(IllegalArgumentException.class, () -> MixedBackfillPressureBenchmark.Options.parse(
				new String[] {"--root=" + root, "--pressured-batch-maximum-active=0"}));
		assertThrows(IllegalArgumentException.class, () -> MixedBackfillPressureBenchmark.Options.parse(
				new String[] {"--root=" + root, "--pressured-batch-maximum-active=-1"}));
		assertThrows(IllegalArgumentException.class, () -> MixedBackfillPressureBenchmark.Options.parse(
				new String[] {"--root=" + root, "--read-workers=3", "--write-workers=3",
						"--pressured-batch-maximum-active=7"}));
		assertThrows(IllegalArgumentException.class, () -> MixedBackfillPressureBenchmark.Options.parse(
				new String[] {"--root=" + root, "--pressured-batch-maximum-active=2",
						"--pressured-batch-maximum-active=3"}));
		assertTrue(Files.notExists(root));
	}

	@Test
	void generatedConfigRecordsTheExactPressuredBatchCapOnce() {
		var options = MixedBackfillPressureBenchmark.Options.parse(new String[] {
				"--root=" + temporary.resolve("config"), "--pressured-batch-maximum-active=3"});
		String config = MixedBackfillPressureBenchmark.configText(options);
		assertEquals(1L, config.lines()
				.filter(line -> line.startsWith("database.parallelism.workload.pressured-batch-maximum-active"))
				.count());
		assertTrue(config.contains("database.parallelism.workload.pressured-batch-maximum-active = 3"));
	}

	@Test
	void latencyDeadlineCoversRunsBeyondFiveSecondsAndSaturatesOnOverflow() {
		long now = 1_000L;
		assertTrue(MixedBackfillPressureBenchmark.latencyDeadlineEpochMillis(
				now, Duration.ofSeconds(6)) > now + Duration.ofSeconds(6).toMillis());
		assertEquals(Long.MAX_VALUE - 1L, MixedBackfillPressureBenchmark.latencyDeadlineEpochMillis(
				Long.MAX_VALUE - 1L, Duration.ofSeconds(6)));
		assertEquals(Long.MAX_VALUE - 1L, MixedBackfillPressureBenchmark.latencyDeadlineEpochMillis(
				0L, Duration.ofSeconds(Long.MAX_VALUE)));
	}
}
