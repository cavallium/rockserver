package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.MetricsManager;
import it.cavallium.rockserver.core.impl.RocksDBStatistics;
import it.cavallium.rockserver.core.impl.RocksDBLongProperty;
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
	void metadataMetricsPreserveLogicalTotalsWithoutNameCollisions() throws Exception {
		var config = ConfigParser.parse(Files.writeString(tempDir.resolve("metadata-metrics.conf"), """
				database.metrics.jmx.enabled = false
				database.metrics.influx.enabled = false
				"""));
		try (var nativeStatistics = new Statistics();
				var data = org.mockito.Mockito.mock(org.rocksdb.Cache.class);
				var metadata = org.mockito.Mockito.mock(org.rocksdb.Cache.class);
				var named = org.mockito.Mockito.mock(org.rocksdb.Cache.class)) {
			org.mockito.Mockito.when(data.getUsage()).thenReturn(200L);
			org.mockito.Mockito.when(data.getPinnedUsage()).thenReturn(100L);
			org.mockito.Mockito.when(metadata.getUsage()).thenReturn(50L);
			org.mockito.Mockito.when(metadata.getPinnedUsage()).thenReturn(20L);
			org.mockito.Mockito.when(named.getUsage()).thenReturn(100L);
			org.mockito.Mockito.when(named.getPinnedUsage()).thenReturn(40L);
			var metrics = new MetricsManager(config);
			var registry = new SimpleMeterRegistry();
			((CompositeMeterRegistry) metrics.getRegistry()).add(registry);
			var statistics = new RocksDBStatistics("metadata-test", nativeStatistics, metrics,
					Map.of("default", data, "default:metadata", named),
					Map.of("default", 2L << 20, "default:metadata", 1L << 20),
					Map.of("default", metadata), Map.of("default", 1L << 20),
					(_, _) -> BigInteger.ONE, _ -> Map.of("default", 1L),
					new RocksDBStatistics.MemoryUpperBoundConfig(1, 0, 0));
			try {
				assertEquals(3L << 20, registry.get("rocksdb.cache.named").tag("cache", "default")
						.tag("field", "capacity").gauge().value());
				assertEquals(1L << 20, registry.get("rocksdb.cache.named").tag("cache", "default:metadata")
						.tag("field", "capacity").gauge().value());
				assertEquals(1L << 20, registry.get("rocksdb.cache.metadata").tag("cache", "default")
						.tag("field", "capacity").gauge().value());
				assertEquals((4L << 20) + 2L, registry.get("rocksdb.memory.max-estimate").gauge().value());
				assertEquals(250L, registry.get("rocksdb.cache.named").tag("cache", "default")
						.tag("field", "usage").gauge().value());
				assertEquals(120L, registry.get("rocksdb.cache.named").tag("cache", "default")
						.tag("field", "pinned_usage").gauge().value());
				assertEquals(350L, registry.get("rocksdb.cache").tag("field", "usage").gauge().value());
				assertEquals(160L, registry.get("rocksdb.cache").tag("field", "pinned_usage").gauge().value());
				assertEquals(352L, registry.get("rocksdb.memory.total").gauge().value());
				statistics.close();
				org.mockito.Mockito.clearInvocations(data, metadata, named);
				registry.find("rocksdb.cache.metadata").gauges().forEach(gauge -> gauge.value());
				registry.find("rocksdb.cache.named").gauges().forEach(gauge -> gauge.value());
				assertTrue(Double.isNaN(registry.get("rocksdb.memory.total").gauge().value()));
				org.mockito.Mockito.verifyNoInteractions(data, metadata, named);
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
			var pinnedPropertyReads = new AtomicInteger();
			var metrics = new MetricsManager(config);
			var registry = new SimpleMeterRegistry();
			((CompositeMeterRegistry) metrics.getRegistry()).add(registry);
			var statistics = new RocksDBStatistics(
					"named-cache-test",
					nativeStatistics,
					metrics,
					Map.of("default", mainCache, "sender", senderCache),
					Map.of("default", 2L << 20, "sender", 1L << 20),
					(property, _) -> {
						if (property.equals(RocksDBLongProperty.BLOCK_CACHE_PINNED_USAGE.getName())) {
							pinnedPropertyReads.incrementAndGet();
						}
						return BigInteger.ONE;
					},
					_ -> Map.of("default", 1L),
					new RocksDBStatistics.MemoryUpperBoundConfig(1, 0, 0));
			try {
				for (int poll = 0; poll < 2; poll++) {
					registry.find("rocksdb.property.long").gauges().forEach(gauge -> gauge.value());
					assertTrue(registry.find("rocksdb.property.long")
							.tag("property_name", RocksDBLongProperty.BLOCK_CACHE_PINNED_USAGE.getName())
							.gauges().isEmpty());
					for (String cacheName : new String[]{"default", "sender"}) {
						assertTrue(Double.isFinite(registry.get("rocksdb.cache.named")
								.tag("database", "named-cache-test").tag("cache", cacheName)
								.tag("field", "pinned_usage").gauge().value()));
					}
					assertTrue(Double.isFinite(registry.get("rocksdb.cache")
							.tag("database", "named-cache-test").tag("field", "pinned_usage")
							.gauge().value()));
				}
				assertEquals(0, pinnedPropertyReads.get());
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
