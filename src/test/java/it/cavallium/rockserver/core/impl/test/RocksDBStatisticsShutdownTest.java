package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.MetricsManager;
import it.cavallium.rockserver.core.impl.RocksDBStatistics;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.HyperClockCache;
import org.rocksdb.Statistics;

class RocksDBStatisticsShutdownTest {

	@TempDir
	Path tempDir;

	@Test
	void closedStatisticsGaugesNeverReadTheDatabaseAgain() throws Exception {
		var configPath = Files.writeString(tempDir.resolve("metrics.conf"), """
				database.metrics.jmx.enabled = false
				database.metrics.influx.enabled = false
				""");
		var config = ConfigParser.parse(configPath);
		var propertyReads = new AtomicInteger();
		var perColumnFamilyReads = new AtomicInteger();
		var firstCollection = new CountDownLatch(1);

		try (var nativeStatistics = new Statistics()) {
			var metrics = new MetricsManager(config);
			var registry = new SimpleMeterRegistry();
			((CompositeMeterRegistry) metrics.getRegistry()).add(registry);
			var statistics = new RocksDBStatistics(
					"shutdown-test",
					nativeStatistics,
					metrics,
					null,
					(_, _) -> {
						propertyReads.incrementAndGet();
						return BigInteger.ONE;
					},
					_ -> {
						perColumnFamilyReads.incrementAndGet();
						firstCollection.countDown();
						return Map.of("default", 1L);
					},
					new RocksDBStatistics.MemoryUpperBoundConfig(1, 0, 0));
			try {
				assertTrue(firstCollection.await(5, TimeUnit.SECONDS),
						"the initial statistics collection did not finish");
				statistics.close();
				int propertyReadsAtClose = propertyReads.get();
				int perColumnFamilyReadsAtClose = perColumnFamilyReads.get();

				var propertyGauges = registry.find("rocksdb.property.long").gauges();
				assertFalse(propertyGauges.isEmpty());
				propertyGauges.forEach(gauge -> gauge.value());
				assertTrue(Double.isNaN(registry
						.get("rocksdb.memory.total")
						.tag("database", "shutdown-test")
						.gauge()
						.value()));
				assertTrue(Double.isNaN(registry
						.get("rocksdb.memory.max-estimate")
						.tag("database", "shutdown-test")
						.gauge()
						.value()));

				assertEquals(propertyReadsAtClose, propertyReads.get());
				assertEquals(perColumnFamilyReadsAtClose, perColumnFamilyReads.get());
				statistics.close();
			} finally {
				statistics.close();
				metrics.close();
			}
		}
	}

	@Test
	void reportsNamedCachesAndUsesTheirCombinedCapacity(@TempDir Path cacheTempDir) throws Exception {
		var configPath = Files.writeString(cacheTempDir.resolve("named-cache-metrics.conf"), """
				database.metrics.jmx.enabled = false
				database.metrics.influx.enabled = false
				""");
		var config = ConfigParser.parse(configPath);
		try (var nativeStatistics = new Statistics();
				var mainCache = new HyperClockCache(2L << 20, 0, -1, false);
				var senderCache = new HyperClockCache(1L << 20, 0, -1, false)) {
			var metrics = new MetricsManager(config);
			var registry = new SimpleMeterRegistry();
			((CompositeMeterRegistry) metrics.getRegistry()).add(registry);
			var statistics = new RocksDBStatistics(
					"named-cache-test",
					nativeStatistics,
					metrics,
					Map.of("default", mainCache, "sender", senderCache),
					Map.of("default", 2L << 20, "sender", 1L << 20),
					(_, _) -> BigInteger.ONE,
					_ -> Map.of("default", 1L),
					new RocksDBStatistics.MemoryUpperBoundConfig(1, 0, 0));
			try {
				assertEquals(2L << 20, registry.get("rocksdb.cache.named")
						.tag("database", "named-cache-test")
						.tag("cache", "default")
						.tag("field", "capacity")
						.gauge().value());
				assertEquals(1L << 20, registry.get("rocksdb.cache.named")
						.tag("database", "named-cache-test")
						.tag("cache", "sender")
						.tag("field", "capacity")
						.gauge().value());
				assertEquals((3L << 20) + 2L, registry.get("rocksdb.memory.max-estimate")
						.tag("database", "named-cache-test")
						.gauge().value());
			} finally {
				statistics.close();
				metrics.close();
			}
		}
	}
}
