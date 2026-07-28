package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.WorkloadSettings;
import it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadBenchmark;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GrpcOverloadBenchmarkConfigTest {

	@TempDir
	Path tempDir;

	@Test
	void generatedConfigurationParsesWithCurrentWorkloadKeysOnly() throws Exception {
		String generated = GrpcOverloadBenchmark.generatedConfigForTesting();
		Path config = tempDir.resolve("grpc-overload-benchmark.conf");
		Files.writeString(config, generated);

		var parsed = ConfigParser.parse(config);
		var settings = WorkloadSettings.resolve(parsed);
		assertEquals(4_096, settings.ingestQueueCapacity());
		assertEquals(512, settings.batchQueueCapacity());
		assertFalse(generated.contains("maintenance-write"));
		assertFalse(generated.contains("foreground-write-queue-capacity"));
		assertFalse(generated.contains("maintenance-write-queue-capacity"));
	}
}
