package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterBoundary;
import it.cavallium.rockserver.core.replication.ratis.RatisServerLifecycle;
import it.cavallium.rockserver.core.replication.ratis.SharedRatisServerLifecycle;
import it.cavallium.rockserver.core.server.Server;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class ServerRatisLifecycleTest {

	@Test
	void serverStartsRatisLifecycleBeforePublicEndpointAndClosesItAfterEndpoint() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false);

		try (var server = new LifecycleAwareServer(events, lifecycle, false)) {
			server.start();
			assertEquals(List.of("ratis-start", "public-start"), events);
		}

		assertEquals(List.of("ratis-start", "public-start", "public-close", "ratis-close"), events);
	}

	@Test
	void closedRatisBackedServerRejectsLaterStartBeforePublicEndpoint() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false);
		var server = new LifecycleAwareServer(events, lifecycle, false);
		server.start();
		server.close();

		var error = assertThrows(IllegalStateException.class, server::start);
		assertEquals("Ratis lifecycle is closed", error.getMessage());
		assertEquals(List.of(
				"ratis-start",
				"public-start",
				"public-close",
				"ratis-close",
				"public-close"
		), events);
	}

	@Test
	void ratisLifecycleIsClosedWhenPublicEndpointStartFails() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false);

		try (var server = new LifecycleAwareServer(events, lifecycle, true)) {
			var error = assertThrows(IOException.class, server::start);
			assertEquals("public start failed", error.getMessage());
		}

		assertEquals(List.of("ratis-start", "public-start", "public-close", "ratis-close", "public-close"), events);
	}

	@Test
	void ratisLifecycleIsClosedWhenRatisStartFails() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, true);

		try (var server = new LifecycleAwareServer(events, lifecycle, false)) {
			var error = assertThrows(IOException.class, server::start);
			assertEquals("ratis start failed", error.getMessage());
		}

		assertEquals(List.of("ratis-start", "public-close", "ratis-close", "public-close"), events);
	}

	@Test
	void sharedLifecycleStartsDelegateOnceAndClosesAfterLastPublicEndpoint() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new SharedRatisServerLifecycle(new RecordingRatisLifecycle(events, false));

		var thrift = new LifecycleAwareServer("thrift", events, lifecycle, false);
		var grpc = new LifecycleAwareServer("grpc", events, lifecycle, false);
		thrift.start();
		grpc.start();
		assertEquals(2, lifecycle.activeReferences());
		assertEquals(List.of("ratis-start", "thrift-public-start", "grpc-public-start"), events);

		grpc.close();
		assertEquals(1, lifecycle.activeReferences());
		assertEquals(List.of(
				"ratis-start",
				"thrift-public-start",
				"grpc-public-start",
				"grpc-public-close"
		), events);

		thrift.close();
		assertEquals(0, lifecycle.activeReferences());
		assertEquals(List.of(
				"ratis-start",
				"thrift-public-start",
				"grpc-public-start",
				"grpc-public-close",
				"thrift-public-close",
				"ratis-close"
		), events);
	}

	@Test
	void sharedLifecycleKeepsDelegateRunningWhenSecondPublicEndpointFails() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new SharedRatisServerLifecycle(new RecordingRatisLifecycle(events, false));

		try (var thrift = new LifecycleAwareServer("thrift", events, lifecycle, false);
			 var grpc = new LifecycleAwareServer("grpc", events, lifecycle, true)) {
			thrift.start();
			var error = assertThrows(IOException.class, grpc::start);
			assertEquals("grpc public start failed", error.getMessage());
			assertEquals(1, lifecycle.activeReferences());
		}

		assertEquals(0, lifecycle.activeReferences());
		assertEquals(List.of(
				"ratis-start",
				"thrift-public-start",
				"grpc-public-start",
				"grpc-public-close",
				"grpc-public-close",
				"thrift-public-close",
				"ratis-close"
		), events);
	}

	@Test
	void sharedLifecycleRetriesFailedInitialStartCleanupWithoutPublishingAReference() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new SharedRatisServerLifecycle(new RecordingRatisLifecycle(events, true, 1, false));
		var server = new LifecycleAwareServer(events, lifecycle, false);

		var error = assertThrows(IOException.class, server::start);
		assertEquals("ratis start failed", error.getMessage());
		assertEquals(1, error.getSuppressed().length);
		assertEquals("ratis close failed", error.getSuppressed()[0].getMessage());
		assertEquals(0, lifecycle.activeReferences());
		var retryStartError = assertThrows(IllegalStateException.class, lifecycle::start);
		assertEquals("Shared Ratis lifecycle cleanup is pending", retryStartError.getMessage());

		server.close();
		assertEquals(List.of(
				"ratis-start",
				"public-close",
				"ratis-close",
				"public-close",
				"ratis-close"
		), events);
	}

	@Test
	void sharedLifecycleRejectsNewReferenceUntilFailedLastCloseIsRetried() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new SharedRatisServerLifecycle(new RecordingRatisLifecycle(events, false, 1, false));
		lifecycle.start();
		assertEquals(1, lifecycle.activeReferences());

		var closeError = assertThrows(IOException.class, lifecycle::close);
		assertEquals("ratis close failed", closeError.getMessage());
		assertEquals(0, lifecycle.activeReferences());
		var retryStartError = assertThrows(IllegalStateException.class, lifecycle::start);
		assertEquals("Shared Ratis lifecycle cleanup is pending", retryStartError.getMessage());

		lifecycle.close();
		lifecycle.start();
		assertEquals(1, lifecycle.activeReferences());
		lifecycle.close();
		assertEquals(0, lifecycle.activeReferences());
		assertEquals(List.of(
				"ratis-start",
				"ratis-close",
				"ratis-close",
				"ratis-start",
				"ratis-close"
		), events);
	}

	@Test
	void failedRatisLifecycleCloseCanBeRetried() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false, 1, false);
		var server = new LifecycleAwareServer(events, lifecycle, false);
		server.start();

		var error = assertThrows(IOException.class, server::close);
		assertEquals("ratis close failed", error.getMessage());

		server.close();
		assertEquals(List.of(
				"ratis-start",
				"public-start",
				"public-close",
				"ratis-close",
				"public-close",
				"ratis-close"
		), events);
	}

	@Test
	void runtimeRatisRollbackFailureIsSuppressedAndRetriedAfterPublicStartFailure() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false, 1, true);

		try (var server = new LifecycleAwareServer(events, lifecycle, true)) {
			var error = assertThrows(IOException.class, server::start);
			assertEquals("public start failed", error.getMessage());
			assertEquals(1, error.getSuppressed().length);
			assertEquals(IllegalStateException.class, error.getSuppressed()[0].getClass());
			assertEquals("ratis close failed", error.getSuppressed()[0].getMessage());
		}

		assertEquals(List.of(
				"ratis-start",
				"public-start",
				"public-close",
				"ratis-close",
				"public-close",
				"ratis-close"
		), events);
	}

	@Test
	void endpointAndRatisRollbackFailuresAreSuppressedOntoPublicStartFailure() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false, 1, true);
		var server = new FailingStartRollbackServer(events, lifecycle);

		var error = assertThrows(IOException.class, server::start);
		assertEquals("public start failed", error.getMessage());
		assertEquals(2, error.getSuppressed().length);
		assertEquals("public close failed", error.getSuppressed()[0].getMessage());
		assertEquals("ratis close failed", error.getSuppressed()[1].getMessage());

		server.close();
		assertEquals(List.of(
				"ratis-start",
				"public-start",
				"public-close",
				"ratis-close",
				"public-close",
				"ratis-close"
		), events);
	}

	@Test
	void errorFromPublicEndpointStartStillClosesEndpointAndRatisLifecycle() {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false);
		var server = new ErrorStartServer(events, lifecycle);

		var error = assertThrows(AssertionError.class, server::start);
		assertEquals("public start failed", error.getMessage());
		assertEquals(List.of("ratis-start", "public-start", "public-close", "ratis-close"), events);
	}

	@Test
	void errorRatisRollbackFailureIsSuppressedAndRetriedAfterPublicStartFailure() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false, 1, false, true);

		try (var server = new LifecycleAwareServer(events, lifecycle, true)) {
			var error = assertThrows(IOException.class, server::start);
			assertEquals("public start failed", error.getMessage());
			assertEquals(1, error.getSuppressed().length);
			assertEquals(AssertionError.class, error.getSuppressed()[0].getClass());
			assertEquals("ratis close failed", error.getSuppressed()[0].getMessage());
		}

		assertEquals(List.of(
				"ratis-start",
				"public-start",
				"public-close",
				"ratis-close",
				"public-close",
				"ratis-close"
		), events);
	}

	@Test
	void endpointResourceCloseContinuesAfterFailureAndSuppressesLaterFailures() throws Exception {
		var events = new ArrayList<String>();
		try (var server = new EndpointResourceClosingServer()) {
			var error = assertThrows(IOException.class, () -> server.closeResources(events));
			assertEquals("first close failed", error.getMessage());
			assertEquals(1, error.getSuppressed().length);
			assertEquals("second close failed", error.getSuppressed()[0].getMessage());
			assertEquals(List.of("first-close", "second-close", "third-close"), events);
		}
	}

	@Test
	void endpointResourceCloseContinuesAfterErrorAndSuppressesLaterFailures() throws Exception {
		var events = new ArrayList<String>();
		try (var server = new EndpointResourceClosingServer()) {
			var error = assertThrows(AssertionError.class, () -> server.closeResourcesAfterError(events));
			assertEquals("first close failed", error.getMessage());
			assertEquals(1, error.getSuppressed().length);
			assertEquals("second close failed", error.getSuppressed()[0].getMessage());
			assertEquals(List.of("first-close", "second-close", "third-close"), events);
		}
	}

	@Test
	void ratisLifecycleCloseIsAttemptedAndRetriedWhenPublicEndpointCloseFails() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false, 1, false);
		var server = new FailingCloseServer(events, lifecycle);
		server.start();

		var firstError = assertThrows(IOException.class, server::close);
		assertEquals("public close failed", firstError.getMessage());
		assertEquals(1, firstError.getSuppressed().length);
		assertEquals("ratis close failed", firstError.getSuppressed()[0].getMessage());

		var retryError = assertThrows(IOException.class, server::close);
		assertEquals("public close failed", retryError.getMessage());
		assertEquals(0, retryError.getSuppressed().length);
		assertEquals(List.of(
				"ratis-start",
				"public-close",
				"ratis-close",
				"public-close",
				"ratis-close"
		), events);
	}

	@Test
	void ratisLifecycleCloseIsAttemptedWhenPublicEndpointCloseThrowsError() throws Exception {
		var events = new ArrayList<String>();
		var lifecycle = new RecordingRatisLifecycle(events, false);
		var server = new ErrorCloseServer(events, lifecycle);
		server.start();

		var error = assertThrows(AssertionError.class, server::close);
		assertEquals("public close failed", error.getMessage());
		assertEquals(List.of("ratis-start", "public-close", "ratis-close"), events);
	}

	@Test
	void serverLifecycleHelpersRejectNullInputsWithExplicitDiagnostics() throws Exception {
		assertIllegalArgument("client must not be null", NullClientServer::new);
		try (var server = new EndpointResourceClosingServer()) {
			assertIllegalArgument("endpointStart must not be null", server::startWithNullEndpointStart);
			assertIllegalArgument("endpointClose must not be null", server::startWithNullEndpointClose);
			assertIllegalArgument("endpointClose must not be null", server::closePublicEndpointWithNullClose);
			assertIllegalArgument("endpointCloses must not be null", server::closeResourcesWithNullArray);
			assertIllegalArgument("endpointCloses[1] must not be null", server::closeResourcesWithNullEntry);
		}
	}

	private static final class RecordingRatisLifecycle implements RatisServerLifecycle {

		private final List<String> events;
		private final boolean failStart;
		private int remainingCloseFailures;
		private final boolean runtimeCloseFailure;
		private final boolean errorCloseFailure;

		private RecordingRatisLifecycle(List<String> events, boolean failStart) {
			this(events, failStart, 0, false, false);
		}

		private RecordingRatisLifecycle(List<String> events,
									   boolean failStart,
									   int remainingCloseFailures,
									   boolean runtimeCloseFailure) {
			this(events, failStart, remainingCloseFailures, runtimeCloseFailure, false);
		}

		private RecordingRatisLifecycle(List<String> events,
									   boolean failStart,
									   int remainingCloseFailures,
									   boolean runtimeCloseFailure,
									   boolean errorCloseFailure) {
			this.events = events;
			this.failStart = failStart;
			this.remainingCloseFailures = remainingCloseFailures;
			this.runtimeCloseFailure = runtimeCloseFailure;
			this.errorCloseFailure = errorCloseFailure;
		}

		@Override
		public RatisAdapterBoundary boundary() {
			return RatisAdapterBoundary.classpathIsolated("test-ratis-lifecycle");
		}

		@Override
		public void start() throws IOException {
			events.add("ratis-start");
			if (failStart) {
				throw new IOException("ratis start failed");
			}
		}

		@Override
		public void close() throws IOException {
			events.add("ratis-close");
			if (remainingCloseFailures > 0) {
				remainingCloseFailures--;
				if (errorCloseFailure) {
					throw new AssertionError("ratis close failed");
				}
				if (runtimeCloseFailure) {
					throw new IllegalStateException("ratis close failed");
				}
				throw new IOException("ratis close failed");
			}
		}
	}

	private static final class ErrorStartServer extends Server {

		private final List<String> events;

		private ErrorStartServer(List<String> events, RatisServerLifecycle lifecycle) {
			super(new TestConnection(), lifecycle);
			this.events = events;
		}

		@Override
		public void start() throws IOException {
			startRatisThenPublicEndpoint(() -> {
				events.add("public-start");
				throw new AssertionError("public start failed");
			}, () -> events.add("public-close"));
		}
	}

	private static final class LifecycleAwareServer extends Server {

		private final String name;
		private final List<String> events;
		private final boolean failPublicStart;

		private LifecycleAwareServer(List<String> events,
									 RatisServerLifecycle ratisLifecycle,
									 boolean failPublicStart) {
			this(null, events, ratisLifecycle, failPublicStart);
		}

		private LifecycleAwareServer(String name,
									 List<String> events,
									 RatisServerLifecycle ratisLifecycle,
									 boolean failPublicStart) {
			super(new TestConnection(), ratisLifecycle);
			this.name = name;
			this.events = events;
			this.failPublicStart = failPublicStart;
		}

		@Override
		public void start() throws IOException {
			startRatisThenPublicEndpoint(() -> {
				events.add(event("public-start"));
				if (failPublicStart) {
					throw new IOException(name == null ? "public start failed" : name + " public start failed");
				}
			}, () -> events.add(event("public-close")));
		}

		@Override
		public void close() throws IOException {
			closePublicEndpointThenRatis(() -> events.add(event("public-close")));
		}

		private String event(String event) {
			return name == null ? event : name + "-" + event;
		}
	}

	private static final class FailingStartRollbackServer extends Server {

		private final List<String> events;

		private FailingStartRollbackServer(List<String> events, RatisServerLifecycle lifecycle) {
			super(new TestConnection(), lifecycle);
			this.events = events;
		}

		@Override
		public void start() throws IOException {
			startRatisThenPublicEndpoint(() -> {
				events.add("public-start");
				throw new IOException("public start failed");
			}, () -> {
				events.add("public-close");
				throw new IllegalStateException("public close failed");
			});
		}

		@Override
		public void close() throws IOException {
			closePublicEndpointThenRatis(() -> events.add("public-close"));
		}
	}

	private static final class ErrorCloseServer extends Server {

		private final List<String> events;

		private ErrorCloseServer(List<String> events, RatisServerLifecycle lifecycle) {
			super(new TestConnection(), lifecycle);
			this.events = events;
		}

		@Override
		public void start() throws IOException {
			startRatisLifecycle();
		}

		@Override
		public void close() throws IOException {
			closePublicEndpointThenRatis(() -> {
				events.add("public-close");
				throw new AssertionError("public close failed");
			});
		}
	}

	private static final class EndpointResourceClosingServer extends Server {

		private EndpointResourceClosingServer() {
			super(new TestConnection());
		}

		@Override
		public void start() {
		}

		private void closeResources(List<String> events) throws IOException {
			closeEndpointResources(
					() -> {
						events.add("first-close");
						throw new IOException("first close failed");
					},
					() -> {
						events.add("second-close");
						throw new IllegalStateException("second close failed");
					},
					() -> events.add("third-close")
			);
		}

		private void closeResourcesAfterError(List<String> events) throws IOException {
			closeEndpointResources(
					() -> {
						events.add("first-close");
						throw new AssertionError("first close failed");
					},
					() -> {
						events.add("second-close");
						throw new IOException("second close failed");
					},
					() -> events.add("third-close")
			);
		}

		private void startWithNullEndpointStart() throws IOException {
			startRatisThenPublicEndpoint(null, () -> {
			});
		}

		private void startWithNullEndpointClose() throws IOException {
			startRatisThenPublicEndpoint(() -> {
			}, null);
		}

		private void closePublicEndpointWithNullClose() throws IOException {
			closePublicEndpointThenRatis(null);
		}

		private void closeResourcesWithNullArray() throws IOException {
			closeEndpointResources((EndpointCloseAction[]) null);
		}

		private void closeResourcesWithNullEntry() throws IOException {
			closeEndpointResources(
					() -> {
					},
					null
			);
		}
	}

	private static final class NullClientServer extends Server {

		private NullClientServer() {
			super(null);
		}

		@Override
		public void start() {
		}
	}

	private static final class FailingCloseServer extends Server {

		private final List<String> events;

		private FailingCloseServer(List<String> events, RatisServerLifecycle lifecycle) {
			super(new TestConnection(), lifecycle);
			this.events = events;
		}

		@Override
		public void start() throws IOException {
			startRatisLifecycle();
		}

		@Override
		public void close() throws IOException {
			closePublicEndpointThenRatis(() -> {
				events.add("public-close");
				throw new IOException("public close failed");
			});
		}
	}

	private static final class TestConnection implements RocksDBConnection {

		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
		};
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
		};

		@Override
		public URI getUrl() {
			return URI.create("memory://server-ratis-lifecycle-test");
		}

		@Override
		public RocksDBSyncAPI getSyncApi() {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi() {
			return asyncApi;
		}

		@Override
		public void close() {
		}
	}

	private static void assertIllegalArgument(String message, Executable executable) {
		var error = assertThrows(IllegalArgumentException.class, executable);
		assertEquals(message, error.getMessage());
	}
}
