package it.cavallium.rockserver.core.impl.test;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.reactivex.rxjava3.core.Completable;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.http.HttpClient;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.MetricsManager;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("slow")
class MetricsManagerTest {

	@TempDir
	Path tempDir;

	@Test
	void disabledExportersStillExposeTaggedRuntimeMetricsAndCloseCleanly() throws IOException {
		var config = ConfigParser.parse(writeConfig("""
				database.metrics.database-name = stage-3
				database.metrics.jmx.enabled = false
				database.metrics.influx.enabled = false
				"""));
		var manager = new MetricsManager(config);
		var registry = (CompositeMeterRegistry) manager.getRegistry();

		try {
			assertTrue(registry.getRegistries().isEmpty());
			var uptime = registry.find("yotsuba.uptime.millis").gauge();
			assertNotNull(uptime);
			assertEquals("rockserver", uptime.getId().getTag("appname"));
			assertEquals("stage-3", uptime.getId().getTag("database-name"));
			assertTrue(uptime.value() >= 0);
			assertFalse(registry.isClosed());
		} finally {
			manager.close();
		}

		assertTrue(registry.isClosed());
		assertTrue(registry.getMeters().isEmpty());
		assertDoesNotThrow(manager::close);
	}

	@Test
	void enabledJmxRegistryIsAttachedAndClosedWithManager() throws IOException {
		var config = ConfigParser.parse(writeConfig("""
				database.metrics.database-name = jmx-lifecycle
				database.metrics.jmx.enabled = true
				database.metrics.influx.enabled = false
				"""));
		var manager = new MetricsManager(config);
		var registry = (CompositeMeterRegistry) manager.getRegistry();

		var children = java.util.List.copyOf(registry.getRegistries());
		try {
			assertEquals(1, registry.getRegistries().size());
			assertEquals("JmxMeterRegistry", registry.getRegistries().iterator().next().getClass().getSimpleName());
			assertNotNull(registry.find("jvm.info").gauge());
		} finally {
			manager.close();
		}

		assertTrue(registry.isClosed());
		assertTrue(registry.getRegistries().isEmpty());
		assertTrue(registry.getMeters().isEmpty());
		assertTrue(children.stream().allMatch(child -> child.isClosed() && child.getMeters().isEmpty()));
	}

	@Test
	void registryClosesBeforeItsHttpTransportAndManagerCloseIsIdempotent() throws Exception {
		var config = ConfigParser.parse(writeConfig("""
				database.metrics.jmx.enabled = false
				database.metrics.influx.enabled = false
				"""));
		var manager = new MetricsManager(config);
		var registry = (CompositeMeterRegistry) manager.getRegistry();
		var exporter = spy(new SimpleMeterRegistry());
		var httpClient = mock(HttpClient.class);
		var vertx = mock(Vertx.class);
		when(httpClient.rxClose()).thenReturn(Completable.complete());
		when(vertx.rxClose()).thenReturn(Completable.complete());
		registry.add(exporter);
		exporter.counter("exporter.only").increment();
		doAnswer(call -> {
			assertFalse(exporter.getMeters().isEmpty(), "final publication must precede meter removal");
			return call.callRealMethod();
		}).when(exporter).close();
		setField(manager, "httpClient", httpClient);
		setField(manager, "vertx", vertx);

		manager.close();

		var closeOrder = inOrder(exporter, httpClient, vertx);
		closeOrder.verify(exporter).close();
		closeOrder.verify(httpClient).rxClose();
		closeOrder.verify(vertx).rxClose();
		assertTrue(registry.getMeters().isEmpty());
		assertTrue(exporter.getMeters().isEmpty());
		assertTrue(registry.getRegistries().isEmpty());

		manager.close();
		verify(exporter, times(1)).close();
		verify(httpClient, times(1)).rxClose();
		verify(vertx, times(1)).rxClose();
	}

	@Test
	void failingExporterDoesNotPreventOtherRegistriesFromClosingOrClearing() throws Exception {
		var config = ConfigParser.parse(writeConfig("""
				database.metrics.jmx.enabled = false
				database.metrics.influx.enabled = false
				"""));
		try (var manager = new MetricsManager(config)) {
			var registry = (CompositeMeterRegistry) manager.getRegistry();
			var failing = spy(new SimpleMeterRegistry());
			var healthy = new SimpleMeterRegistry();
			registry.add(failing);
			registry.add(healthy);
			failing.counter("exporter.only").increment();
			doAnswer(call -> {
				call.callRealMethod();
				throw new IllegalStateException("exporter close failed");
			}).when(failing).close();

			assertDoesNotThrow(manager::close);

			assertTrue(registry.isClosed());
			assertTrue(failing.isClosed());
			assertTrue(healthy.isClosed());
			assertTrue(registry.getMeters().isEmpty());
			assertTrue(failing.getMeters().isEmpty());
			assertTrue(healthy.getMeters().isEmpty());
			assertTrue(registry.getRegistries().isEmpty());
		}
	}

	@Test
	void databaseShutdownEmptiesRetainedRegistriesWithTableMetricsEnabled() throws Exception {
		var config = writeConfig("""
				database.metrics.jmx.enabled = false
				database.metrics.influx.enabled = false
				database.metrics.table-properties-enabled = true
				""");
		try (var database = new it.cavallium.rockserver.core.impl.EmbeddedDB(
				tempDir.resolve("database"), "metrics-shutdown", config)) {
			var registry = (CompositeMeterRegistry) database.getMetricsRegistry();
			var child = new SimpleMeterRegistry();
			registry.add(child);
			assertNotNull(child.find("rocksdb.table.properties.collection.success").gauge());
			assertFalse(child.getMeters().isEmpty());

			database.closeTesting();

			assertTrue(registry.isClosed());
			assertTrue(child.isClosed());
			assertTrue(registry.getMeters().isEmpty());
			assertTrue(child.getMeters().isEmpty());
			assertTrue(registry.getRegistries().isEmpty());
		}
	}

	private static void setField(Object target, String name, Object value) throws Exception {
		Field field = target.getClass().getDeclaredField(name);
		field.setAccessible(true);
		field.set(target, value);
	}

	@Test
	void aFailedInfluxInitialisationLogsItsCauseWithoutTheCredentials() throws Exception {
		var influx = mock(it.cavallium.rockserver.core.config.InfluxMetricsConfig.class);
		when(influx.enabled()).thenReturn(true);
		when(influx.url()).thenReturn("https://dbuser:p4ssw0rd-x@influx.example:8086/path");
		when(influx.user()).thenReturn("dbuser");
		when(influx.token()).thenReturn("tok-s3cret-1234");
		when(influx.bucket()).thenThrow(new org.github.gestalt.config.exceptions.GestaltException(
				"cannot read bucket with token tok-s3cret-1234 for dbuser at https://dbuser:p4ssw0rd-x@influx.example:8086/path"));
		var base = ConfigParser.parse(writeConfig("""
				database.metrics.jmx.enabled = false
				database.metrics.influx.enabled = false
				"""));
		var metrics = mock(it.cavallium.rockserver.core.config.MetricsConfig.class);
		when(metrics.jmx()).thenReturn(base.metrics().jmx());
		when(metrics.influx()).thenReturn(influx);
		when(metrics.databaseName()).thenReturn("influx-failure");
		var config = spy(base);
		when(config.metrics()).thenReturn(metrics);
		var log = mock(org.slf4j.Logger.class);
		var captor = org.mockito.ArgumentCaptor.forClass(Throwable.class);

		var manager = new MetricsManager(config, log);
		try {
			verify(log).error(org.mockito.ArgumentMatchers.eq("Failed to initialize influx metrics"), captor.capture());
		} finally {
			manager.close();
		}
		var text = new StringBuilder();
		for (Throwable t = captor.getValue(); t != null; t = t.getCause()) {
			text.append(t).append('\n');
		}
		assertTrue(text.toString().contains("cannot read bucket"), text.toString());
		assertTrue(text.toString().contains("GestaltException"), text.toString());
		for (var secret : new String[] {"tok-s3cret-1234", "p4ssw0rd-x", "dbuser"}) {
			assertFalse(text.toString().contains(secret), "the cause leaks " + secret + ": " + text);
		}
	}

	/**
	 * Review of R65: the user info of the url is percent-encoded there, and a failure can echo it decoded: that form is
	 * masked too.
	 */
	@Test
	void aFailedInfluxInitialisationMasksTheDecodedUserInfoOfTheUrl() throws Exception {
		var influx = mock(it.cavallium.rockserver.core.config.InfluxMetricsConfig.class);
		when(influx.enabled()).thenReturn(true);
		when(influx.url()).thenReturn("https://db%2Fuser:p%40ss%3Aw0rd@influx.example:8086/path");
		when(influx.bucket()).thenThrow(new org.github.gestalt.config.exceptions.GestaltException(
				"cannot log in as db/user with p@ss:w0rd at influx.example"));
		var base = ConfigParser.parse(writeConfig("""
				database.metrics.jmx.enabled = false
				database.metrics.influx.enabled = false
				"""));
		var metrics = mock(it.cavallium.rockserver.core.config.MetricsConfig.class);
		when(metrics.jmx()).thenReturn(base.metrics().jmx());
		when(metrics.influx()).thenReturn(influx);
		when(metrics.databaseName()).thenReturn("influx-failure-encoded");
		var config = spy(base);
		when(config.metrics()).thenReturn(metrics);
		var log = mock(org.slf4j.Logger.class);
		var captor = org.mockito.ArgumentCaptor.forClass(Throwable.class);

		var manager = new MetricsManager(config, log);
		try {
			verify(log).error(org.mockito.ArgumentMatchers.eq("Failed to initialize influx metrics"), captor.capture());
		} finally {
			manager.close();
		}
		var text = String.valueOf(captor.getValue());
		assertTrue(text.contains("cannot log in as") && text.contains("influx.example"), text);
		for (var secret : new String[] {"p@ss:w0rd", "db/user"}) {
			assertFalse(text.contains(secret), "the cause leaks " + secret + ": " + text);
		}
	}

	private Path writeConfig(String content) throws IOException {
		return Files.writeString(tempDir.resolve("metrics-" + System.nanoTime() + ".conf"), content);
	}
}
