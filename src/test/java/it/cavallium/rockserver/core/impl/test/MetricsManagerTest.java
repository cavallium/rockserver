package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
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

		try {
			assertEquals(1, registry.getRegistries().size());
			assertEquals("JmxMeterRegistry", registry.getRegistries().iterator().next().getClass().getSimpleName());
			assertNotNull(registry.find("jvm.info").gauge());
		} finally {
			manager.close();
		}

		assertTrue(registry.isClosed());
		assertTrue(registry.getRegistries().stream().allMatch(child -> child.isClosed()));
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
		setField(manager, "httpClient", httpClient);
		setField(manager, "vertx", vertx);

		manager.close();

		var closeOrder = inOrder(exporter, httpClient, vertx);
		closeOrder.verify(exporter).close();
		closeOrder.verify(httpClient).rxClose();
		closeOrder.verify(vertx).rxClose();

		manager.close();
		verify(httpClient, times(1)).rxClose();
		verify(vertx, times(1)).rxClose();
	}

	private static void setField(Object target, String name, Object value) throws Exception {
		Field field = target.getClass().getDeclaredField(name);
		field.setAccessible(true);
		field.set(target, value);
	}

	private Path writeConfig(String content) throws IOException {
		return Files.writeString(tempDir.resolve("metrics-" + System.nanoTime() + ".conf"), content);
	}
}
