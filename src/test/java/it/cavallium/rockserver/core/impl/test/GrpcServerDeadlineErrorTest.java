package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Flux;

@Timeout(30)
class GrpcServerDeadlineErrorTest {

	@Test
	void readDeadlineExceededSurvivesARealGrpcServerRoundTrip() throws Exception {
		var backend = new DeadlineBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-deadline-error",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var error = assertThrows(RocksDBException.class, () -> {
					try (var range = client.getSyncApi().getRange(0,
							0,
							null,
							null,
							false,
							RequestType.allInRangeNoCache(),
							1_000)) {
						range.toList();
					}
				});

				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
				assertEquals("Deadline exceeded", error.getMessage());
			}
		}
	}

	@Test
	void cancelledUnaryCallDoesNotDropLateNativeDeadlineError() throws Exception {
		var backend = new BlockingDeadlineBackendConnection();
		var droppedError = new AtomicReference<Throwable>();
		var lateWarning = new AtomicReference<LogCall>();
		var lateWarningLogged = new CountDownLatch(1);
		Logger recordingLogger = recordingLogger(lateWarning, lateWarningLogged);
		Logger originalLogger = swapGrpcServerLogger(recordingLogger);
		Hooks.onErrorDropped(error -> droppedError.compareAndSet(null, error));
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-cancelled-deadline-error",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var response = client.getAsyncApi().reduceRangeAsync(0,
						0,
						null,
						null,
						false,
						RequestType.firstAndLast(),
						1_000);

				assertTrue(backend.entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
				assertTrue(response.cancel(true));
				assertTrue(backend.cancelObserved.await(5, TimeUnit.SECONDS),
						"the server did not observe RPC cancellation before native completion");
				backend.release.countDown();
				assertTrue(backend.finished.await(5, java.util.concurrent.TimeUnit.SECONDS));

				assertTrue(lateWarningLogged.await(5, TimeUnit.SECONDS),
						"the late native failure never reached the request-scoped logger");
				assertNull(droppedError.get(), "the server must not emit a terminal error after RPC cancellation");
				var warning = lateWarning.get();
				assertEquals("Late gRPC request failure after call termination: operation={}, requestType={}, "
						+ "request={}, errorType={}, grpcStatus={}, message={}", warning.message());
				assertEquals("reduceRangeFirstAndLast", warning.arguments().get(0));
				assertTrue(warning.arguments().get(1).toString().endsWith(".GetRangeRequest"));
				assertTrue(warning.arguments().get(2).toString().contains("timeoutMs=1000"));
				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, warning.arguments().get(3));
				assertEquals(io.grpc.Status.Code.INTERNAL, warning.arguments().get(4));
				assertEquals("Deadline exceeded", warning.arguments().get(5));
			}
		} finally {
			backend.release.countDown();
			Hooks.resetOnErrorDropped();
			swapGrpcServerLogger(originalLogger);
		}
	}

	@Test
	void uncancelledUnaryDeadlineErrorRemainsVisible() throws Exception {
		var backend = new BlockingDeadlineBackendConnection();
		backend.release.countDown();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-visible-deadline-error",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var error = assertThrows(RocksDBException.class, () -> client.getSyncApi().reduceRange(0,
						0,
						null,
						null,
						false,
						RequestType.firstAndLast(),
						1_000));

				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
			}
		}
	}

	private static final class DeadlineBackendConnection implements RocksDBConnection {

		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {};
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?>) {
					return (RA) Flux.error(RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
							"Deadline exceeded"));
				}
				throw new UnsupportedOperationException("Unexpected request: " + request);
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://deadline-backend");
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

	private static final class BlockingDeadlineBackendConnection implements RocksDBConnection {

		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch cancelObserved = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final CountDownLatch finished = new CountDownLatch(1);
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				if (!(request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?>)) {
					throw new UnsupportedOperationException("Unexpected request: " + request);
				}
				var result = new NonCancellableFuture<>(cancelObserved);
				Thread.startVirtualThread(() -> {
					entered.countDown();
					while (true) {
						try {
							release.await();
							break;
						} catch (InterruptedException ignored) {
							// Model a native RocksDB call that does not return until its own deadline.
						}
					}
					finished.countDown();
					result.completeExceptionally(RocksDBException.of(
							RocksDBErrorType.READ_DEADLINE_EXCEEDED,
							"Deadline exceeded"));
				});
				return (RA) result;
			}
		};
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {};

		@Override
		public URI getUrl() {
			return URI.create("test://blocking-deadline-backend");
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

	/** Models JNI work that keeps running after the RPC cancellation signal. */
	private static final class NonCancellableFuture<T> extends CompletableFuture<T> {

		private final CountDownLatch cancelObserved;

		private NonCancellableFuture(CountDownLatch cancelObserved) {
			this.cancelObserved = cancelObserved;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			cancelObserved.countDown();
			return false;
		}
	}

	private record LogCall(String message, List<Object> arguments) {
	}

	private static Logger recordingLogger(AtomicReference<LogCall> lateWarning,
			CountDownLatch lateWarningLogged) {
		return (Logger) Proxy.newProxyInstance(Logger.class.getClassLoader(), new Class<?>[] {Logger.class},
				(_, method, arguments) -> {
					if (method.getName().equals("getName")) {
						return GrpcServer.class.getName();
					}
					if (method.getName().startsWith("is") && method.getReturnType() == boolean.class) {
						return true;
					}
					if (method.getName().equals("warn") && arguments != null && arguments.length >= 2
							&& arguments[0] instanceof String message
							&& message.startsWith("Late gRPC request failure after call termination:")) {
						Object[] logArguments = arguments.length == 2 && arguments[1] instanceof Object[] values
								? values
								: Arrays.copyOfRange(arguments, 1, arguments.length);
						lateWarning.compareAndSet(null, new LogCall(message, List.of(logArguments)));
						lateWarningLogged.countDown();
					}
					if (method.getReturnType() == boolean.class) {
						return false;
					}
					return null;
				});
	}

	/** Swap the private static logger without changing production code just for this assertion. */
	private static Logger swapGrpcServerLogger(Logger replacement) throws ReflectiveOperationException {
		Class.forName(GrpcServer.class.getName(), true, GrpcServer.class.getClassLoader());
		Field loggerField = GrpcServer.class.getDeclaredField("LOG");
		Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
		Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
		theUnsafeField.setAccessible(true);
		Object unsafe = theUnsafeField.get(null);
		Object fieldBase = unsafeClass.getMethod("staticFieldBase", Field.class).invoke(unsafe, loggerField);
		long fieldOffset = (long) unsafeClass.getMethod("staticFieldOffset", Field.class).invoke(unsafe, loggerField);
		var getObject = unsafeClass.getMethod("getObjectVolatile", Object.class, long.class);
		var putObject = unsafeClass.getMethod("putObjectVolatile", Object.class, long.class, Object.class);
		Logger previous = (Logger) getObject.invoke(unsafe, fieldBase, fieldOffset);
		putObject.invoke(unsafe, fieldBase, fieldOffset, replacement);
		return previous;
	}
}
