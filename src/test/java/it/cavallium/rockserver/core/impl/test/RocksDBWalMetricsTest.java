package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.rocksdb.WalFileType.kAliveLogFile;
import static org.rocksdb.WalFileType.kArchivedLogFile;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.WalFileType;

class RocksDBWalMetricsTest {

	private static final String LIVE_BYTES_METRIC = "rocksdb.wal.live.bytes";
	private static final String LIVE_FILES_METRIC = "rocksdb.wal.live.files";
	private static final String LIVE_OLDEST_AGE_METRIC = "rocksdb.wal.live.oldest.age.seconds";
	private static final String ARCHIVED_BYTES_METRIC = "rocksdb.wal.archived.bytes";
	private static final String ARCHIVED_FILES_METRIC = "rocksdb.wal.archived.files";
	private static final String ARCHIVED_OLDEST_AGE_METRIC = "rocksdb.wal.archived.oldest.age.seconds";
	private static final String MIN_LOG_NUMBER_TO_KEEP_METRIC = "rocksdb.wal.min.log.number.to.keep";
	private static final String CONFIGURED_LIVE_LIMIT_METRIC = "rocksdb.wal.configured.live.limit.bytes";
	private static final long LIVE_WAL_LIMIT = 4L * 1024L * 1024L * 1024L;

	@Test
	void rocksDbMetadataTypeClassifiesLiveAndArchivedWalFiles(@TempDir Path walDirectory)
			throws Exception {
		var livePath = walDirectory.resolve("archive-looking/000001.log");
		var olderLivePath = walDirectory.resolve("archive-looking/000000.log");
		var archivedPath = walDirectory.resolve("not-an-archive/000002.log");
		var newerArchivedPath = walDirectory.resolve("not-an-archive/000003.log");
		var timestamps = Map.of(
				livePath, 10_000L,
				olderLivePath, 5_000L,
				archivedPath, 20_000L,
				newerArchivedPath, 30_000L);
		var registry = new SimpleMeterRegistry();

		try (var walMetrics = newWalMetrics(
				registry,
				walDirectory,
				List.of(
						new WalMetadata("/archive-looking/000001.log", kAliveLogFile, 100L),
						new WalMetadata("/archive-looking/000000.log", kAliveLogFile, 150L),
						new WalMetadata("/not-an-archive/000002.log", kArchivedLogFile, 200L),
						new WalMetadata("/not-an-archive/000003.log", kArchivedLogFile, 250L)),
				0L,
				timestamps::get,
				Clock.fixed(Instant.ofEpochMilli(40_000L), ZoneOffset.UTC))) {
			walMetrics.refresh();

			assertEquals(250.0d, gauge(registry, LIVE_BYTES_METRIC));
			assertEquals(2.0d, gauge(registry, LIVE_FILES_METRIC));
			assertEquals(450.0d, gauge(registry, ARCHIVED_BYTES_METRIC));
			assertEquals(2.0d, gauge(registry, ARCHIVED_FILES_METRIC));
		}
	}

	@Test
	void exportsWalVisibilityWithoutChangingCdcReadableArchives(@TempDir Path walDirectory)
			throws Exception {
		var now = Instant.parse("2026-07-22T12:00:00Z");
		var livePath = walDirectory.resolve("000003.log");
		var archivedPath = walDirectory.resolve("archive/000001.log");
		Files.createDirectories(archivedPath.getParent());
		Files.write(livePath, new byte[] {1, 2, 3});
		var archivedContents = new byte[] {4, 5, 6, 7};
		Files.write(archivedPath, archivedContents);
		Files.setLastModifiedTime(livePath, FileTime.from(now.minusSeconds(90)));
		Files.setLastModifiedTime(archivedPath, FileTime.from(now.minusSeconds(3_600)));
		var registry = new SimpleMeterRegistry();

		try (var walMetrics = newWalMetrics(
				registry,
				walDirectory,
				List.of(
						new WalMetadata("/000003.log", kAliveLogFile, 1_024L),
						new WalMetadata("/archive/000001.log", kArchivedLogFile, 2_048L)),
				123L,
				path -> Files.getLastModifiedTime(path).toMillis(),
				Clock.fixed(now, ZoneOffset.UTC))) {
			walMetrics.refresh();

			assertEquals(1_024.0d, gauge(registry, LIVE_BYTES_METRIC));
			assertEquals(1.0d, gauge(registry, LIVE_FILES_METRIC));
			assertEquals(90.0d, gauge(registry, LIVE_OLDEST_AGE_METRIC));
			assertEquals(2_048.0d, gauge(registry, ARCHIVED_BYTES_METRIC));
			assertEquals(1.0d, gauge(registry, ARCHIVED_FILES_METRIC));
			assertEquals(3_600.0d, gauge(registry, ARCHIVED_OLDEST_AGE_METRIC));
			assertEquals(123.0d, gauge(registry, MIN_LOG_NUMBER_TO_KEEP_METRIC));
			assertEquals((double) LIVE_WAL_LIMIT, gauge(registry, CONFIGURED_LIVE_LIMIT_METRIC));
			assertArrayEquals(archivedContents, Files.readAllBytes(archivedPath),
					"metrics collection must not purge or rewrite archived WALs used by CDC");
		}
	}

	private static WalMetricsHandle newWalMetrics(MeterRegistry registry,
			Path walDirectory,
			List<WalMetadata> files,
			long minLogNumberToKeep,
			TimestampSource timestampSource,
			Clock clock) throws Exception {
		var metricsClass = Class.forName("it.cavallium.rockserver.core.impl.RocksDBWalMetrics");
		var metadataClass = Class.forName(
				"it.cavallium.rockserver.core.impl.RocksDBWalMetrics$WalFileMetadata");
		Constructor<?> metadataConstructor = metadataClass.getDeclaredConstructor(
				String.class, WalFileType.class, long.class);
		metadataConstructor.setAccessible(true);
		var nativeMetadata = new ArrayList<>(files.size());
		for (var file : files) {
			nativeMetadata.add(metadataConstructor.newInstance(
					file.pathName(), file.type(), file.sizeFileBytes()));
		}

		Constructor<?> metricsConstructor = Arrays.stream(metricsClass.getDeclaredConstructors())
				.filter(candidate -> candidate.getParameterCount() == 8)
				.findFirst()
				.orElseThrow();
		metricsConstructor.setAccessible(true);
		var parameterTypes = metricsConstructor.getParameterTypes();
		var instance = metricsConstructor.newInstance(
				"visibility-test",
				registry,
				walDirectory,
				functionalProxy(parameterTypes[3], _ -> List.copyOf(nativeMetadata)),
				functionalProxy(parameterTypes[4], _ -> minLogNumberToKeep),
				functionalProxy(parameterTypes[5], arguments ->
						timestampSource.lastModifiedMillis((Path) arguments[0])),
				clock,
				LIVE_WAL_LIMIT);
		Method refresh = metricsClass.getDeclaredMethod("refresh");
		refresh.setAccessible(true);
		return new WalMetricsHandle((AutoCloseable) instance, refresh);
	}

	private static Object functionalProxy(Class<?> type, ThrowingInvocation invocation) {
		return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
				(proxy, method, arguments) -> {
					if (method.getDeclaringClass() != Object.class) {
						return invocation.invoke(arguments == null ? new Object[0] : arguments);
					}
					return switch (method.getName()) {
						case "equals" -> proxy == arguments[0];
						case "hashCode" -> System.identityHashCode(proxy);
						case "toString" -> type.getName();
						default -> throw new UnsupportedOperationException(method.toString());
					};
				});
	}

	private static double gauge(MeterRegistry registry, String name) {
		return registry.get(name).tag("database", "visibility-test").gauge().value();
	}

	private record WalMetadata(String pathName, WalFileType type, long sizeFileBytes) {}

	@FunctionalInterface
	private interface TimestampSource {

		long lastModifiedMillis(Path path) throws Exception;
	}

	@FunctionalInterface
	private interface ThrowingInvocation {

		Object invoke(Object[] arguments) throws Throwable;
	}

	private record WalMetricsHandle(AutoCloseable instance, Method refreshMethod) implements AutoCloseable {

		void refresh() throws Exception {
			refreshMethod.invoke(instance);
		}

		@Override
		public void close() throws Exception {
			instance.close();
		}
	}
}
