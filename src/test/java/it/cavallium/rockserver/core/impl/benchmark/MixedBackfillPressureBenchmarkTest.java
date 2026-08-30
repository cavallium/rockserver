package it.cavallium.rockserver.core.impl.benchmark;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
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
		var options = options(root, 2);

		var result = MixedBackfillPressureBenchmark.run(options);

		assertDoesNotThrow(result::assertCorrect);
		assertEquals(512L, result.cancelledRows() + result.resumedRows());
		assertTrue(result.durableCheckpoints() > 0);
		assertTrue(result.backfillRows() > 0L);
		assertTrue(result.cdcEvents() > 0L);
		assertTrue(result.maintenanceOperations() > 0L);
		assertEquals(2, result.maximumPressuredBatchActive());
		assertEquals(3L, result.pressuredBatchCapWitnesses(),
				"three stable barrier snapshots must witness both pressured permits concurrently");
		assertTrue(Files.isRegularFile(root.resolve("checkpoint.txt")));
		assertTrue(Files.readString(root.resolve("rockserver.conf"))
				.contains("database.parallelism.workload.pressured-batch-maximum-active = 2"));
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
				"--read-workers=32", "--write-workers=32", "--pressured-batch-maximum-active=64"});
		assertEquals(64, explicit.pressuredBatchMaximumActive(),
				"the combined data-pool capacity and explicit safety ceiling are inclusive");
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
				new String[] {"--root=" + root, "--read-workers=100", "--write-workers=100",
						"--pressured-batch-maximum-active=65"}));
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
	void resultContractRequiresTheExactConfiguredConcurrentCapAndAStableWitness() {
		var options = options(temporary.resolve("result-contract"), 3);
		assertDoesNotThrow(() -> result(options, 3, 3L).assertCorrect());
		assertThrows(IllegalStateException.class, () -> result(options, 2, 3L).assertCorrect(),
				"merely observing some pressured work must not prove cap three was exercised");
		assertThrows(IllegalStateException.class, () -> result(options, 4, 3L).assertCorrect(),
				"a snapshot above the configured cap must fail rather than be truncated");
		assertThrows(IllegalStateException.class, () -> result(options, 3, 0L).assertCorrect(),
				"a configured value without a concurrent snapshot witness is not evidence");
	}

	@Test
	void v2SchemaRequiresEveryResultPropertyIncludingCapProvenance() throws Exception {
		var result = result(options(temporary.resolve("schema"), 3), 3, 3L);
		var properties = new Properties();
		properties.load(new StringReader(result.properties()));
		assertEquals("rockserver-mixed-backfill-pressure-v2", properties.getProperty("schema"));
		assertEquals("3", properties.getProperty("pressured-batch-maximum-active"));
		assertEquals("3", properties.getProperty("maximum-pressured-batch-active"));
		assertEquals("3", properties.getProperty("pressured-batch-cap-witnesses"));
		assertEquals("held-barrier-scheduler-snapshots",
				properties.getProperty("pressured-batch-witness-mode"));

		String schema = Files.readString(Path.of("benchmarks", "schemas",
				"mixed-backfill-pressure-v2.schema.json"));
		assertEquals(properties.stringPropertyNames(), requiredSchemaKeys(schema),
				"the strict V2 required set must neither omit emitted evidence nor require absent data");
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

	private static MixedBackfillPressureBenchmark.Options options(Path root, int pressuredCap) {
		return new MixedBackfillPressureBenchmark.Options(root,
				512, 64, 128, Duration.ofSeconds(1), 1, 1, 4, 4, 2, pressuredCap, 128,
				Duration.ofMillis(50), Duration.ofSeconds(3), 20_000,
				Duration.ofSeconds(5), 10_000, 1.0d, 1.0d);
	}

	private static MixedBackfillPressureBenchmark.Result result(
			MixedBackfillPressureBenchmark.Options options,
			int maximumPressuredActive,
			long capWitnesses) {
		return new MixedBackfillPressureBenchmark.Result(options,
				128L, 384L, 4, Duration.ofSeconds(1).toNanos(),
				1_000L, 4L, 1_000L, 1_000L, 1_000L, 2L, 2L,
				maximumPressuredActive, capWitnesses, 1L, 1L, 1L,
				1.0d, 1.0d, 0L, 0L, 0, 0, 0,
				new BenchmarkProcessTelemetry.Peaks(1L, 0L, 1L, 1, 1L),
				0L, options.root().resolve("mixed-results.properties"));
	}

	private static Set<String> requiredSchemaKeys(String schema) {
		int required = schema.indexOf("\"required\"");
		int start = schema.indexOf('[', required);
		int end = schema.indexOf(']', start);
		assertTrue(required >= 0 && start > required && end > start, "schema required array is missing");
		var keys = new LinkedHashSet<String>();
		var matcher = Pattern.compile("\"([^\"]+)\"").matcher(schema.substring(start + 1, end));
		while (matcher.find()) assertTrue(keys.add(matcher.group(1)), "duplicate required schema key");
		return Set.copyOf(keys);
	}
}
