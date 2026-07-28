package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class MandatoryContextApiSurfaceTest {

	private static final Set<Class<?>> CONNECTION_TYPES = Set.of(
			EmbeddedConnection.class,
			GrpcConnection.class,
			ThriftConnection.class);

	@Test
	void concreteConnectionsExposeNoContextFreeDatabaseApi() {
		var operationMethods = Stream.concat(
				Arrays.stream(RocksDBSyncAPI.class.getMethods()),
				Arrays.stream(RocksDBAsyncAPI.class.getMethods()))
				.map(MethodKey::of)
				.collect(Collectors.toUnmodifiableSet());

		for (var connectionType : CONNECTION_TYPES) {
			assertTrue(RocksDBConnection.class.isAssignableFrom(connectionType), connectionType.getName());
			assertFalse(RocksDBAPI.class.isAssignableFrom(connectionType), connectionType.getName());
			assertFalse(RocksDBSyncAPI.class.isAssignableFrom(connectionType), connectionType.getName());
			assertFalse(RocksDBAsyncAPI.class.isAssignableFrom(connectionType), connectionType.getName());

			var leakedMethods = Arrays.stream(connectionType.getMethods())
					.map(MethodKey::of)
					.filter(operationMethods::contains)
					.toList();
			assertTrue(leakedMethods.isEmpty(),
					() -> connectionType.getName() + " exposes context-free operations: " + leakedMethods);
		}
	}

	@Test
	void connectionInterfaceReturnsOnlyContextBoundViewTypes() throws Exception {
		assertEquals(RocksDBSyncAPI.class,
				RocksDBConnection.class.getMethod("getSyncApi", RequestContext.class).getReturnType());
		assertEquals(RocksDBAsyncAPI.class,
				RocksDBConnection.class.getMethod("getAsyncApi", RequestContext.class).getReturnType());

		var publicMethods = Arrays.stream(RocksDBConnection.class.getMethods())
				.map(Method::getName)
				.collect(Collectors.toUnmodifiableSet());
		assertEquals(Set.of("close", "getUrl", "getSyncApi", "getAsyncApi"), publicMethods);
	}

	@Test
	void rawImplementationsAndBaseClassAreInternal() throws Exception {
		for (var className : Set.of(
				"it.cavallium.rockserver.core.client.BaseConnection",
				"it.cavallium.rockserver.core.client.ContextBoundRocksDBAPI",
				"it.cavallium.rockserver.core.client.EmbeddedConnectionDelegate",
				"it.cavallium.rockserver.core.client.GrpcConnectionDelegate",
				"it.cavallium.rockserver.core.client.ThriftConnectionDelegate")) {
			var internalType = Class.forName(className);
			assertFalse(Modifier.isPublic(internalType.getModifiers()), className);
			assertFalse(Modifier.isProtected(internalType.getModifiers()), className);
		}

		for (var className : Set.of(
				"it.cavallium.rockserver.core.client.ContextBoundRocksDBAPI",
				"it.cavallium.rockserver.core.client.EmbeddedConnectionDelegate",
				"it.cavallium.rockserver.core.client.GrpcConnectionDelegate",
				"it.cavallium.rockserver.core.client.ThriftConnectionDelegate")) {
			var rawType = Class.forName(className);
			assertTrue(Modifier.isFinal(rawType.getModifiers()), className);
			assertTrue(RocksDBAPI.class.isAssignableFrom(rawType), className);
		}
	}

	@Test
	void implementationPackagesAreQualifiedToTestsOnly() {
		var coreModule = EmbeddedConnection.class.getModule();
		if (!coreModule.isNamed()) {
			return;
		}
		var testModule = getClass().getModule();
		for (var packageName : Set.of(
				"it.cavallium.rockserver.core.impl",
				"it.cavallium.rockserver.core.impl.rocksdb")) {
			assertFalse(coreModule.isExported(packageName), packageName);
			assertTrue(coreModule.isExported(packageName, testModule), packageName);
		}
	}

	private record MethodKey(String name, Class<?> returnType, java.util.List<Class<?>> parameterTypes) {

		private static MethodKey of(Method method) {
			return new MethodKey(method.getName(),
					method.getReturnType(),
					java.util.List.of(method.getParameterTypes()));
		}
	}
}
