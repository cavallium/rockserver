package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse;
import it.cavallium.rockserver.core.common.api.proto.GetRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenIteratorRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenIteratorResponse;
import it.cavallium.rockserver.core.common.api.proto.OpenTransactionRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenTransactionResponse;
import it.cavallium.rockserver.core.common.api.proto.PutBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.UpdateBegin;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@Timeout(15)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class GrpcRetryPolicyTest {

	private static final String MAX_ATTEMPTS_PROPERTY =
			"it.cavallium.rockserver.grpc.client.max-retry-attempts";
	private static final String INITIAL_BACKOFF_PROPERTY =
			"it.cavallium.rockserver.grpc.client.initial-retry-backoff";
	private static final String MAX_BACKOFF_PROPERTY =
			"it.cavallium.rockserver.grpc.client.max-retry-backoff";
	private static final String BACKOFF_MULTIPLIER_PROPERTY =
			"it.cavallium.rockserver.grpc.client.retry-backoff-multiplier";
	private static final Set<String> RETRY_PROPERTIES = Set.of(
			MAX_ATTEMPTS_PROPERTY,
			INITIAL_BACKOFF_PROPERTY,
			MAX_BACKOFF_PROPERTY,
			BACKOFF_MULTIPLIER_PROPERTY);

	private Map<String, String> previousProperties;

	@BeforeEach
	void configureFastTwoAttemptRetry() {
		previousProperties = RETRY_PROPERTIES.stream()
				.filter(name -> System.getProperty(name) != null)
				.collect(java.util.stream.Collectors.toUnmodifiableMap(
						name -> name,
						System::getProperty));
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "2");
		System.setProperty(INITIAL_BACKOFF_PROPERTY, "0.001s");
		System.setProperty(MAX_BACKOFF_PROPERTY, "0.001s");
		System.setProperty(BACKOFF_MULTIPLIER_PROPERTY, "1");
	}

	@AfterEach
	void restoreRetryProperties() {
		for (var property : RETRY_PROPERTIES) {
			var previous = previousProperties.get(property);
			if (previous == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, previous);
			}
		}
	}

	@Test
	void nonIdempotentUnaryAndResourceCreationAreNotReplayed() throws Exception {
		var service = new FailFirstService();
		try (var fixture = start(service)) {
			var stub = RocksDBServiceGrpc.newFutureStub(fixture.channel());

			var transactionFailure = assertThrows(ExecutionException.class, () -> stub
					.openTransaction(OpenTransactionRequest.getDefaultInstance())
					.get(5, TimeUnit.SECONDS));
			assertUnavailable(transactionFailure);
			assertEquals(1, service.openTransactionSideEffects.get(),
					"automatic retry must not create a second transaction after an ambiguous transport failure");

			var updateFailure = assertThrows(ExecutionException.class, () -> stub
					.getForUpdate(GetRequest.getDefaultInstance())
					.get(5, TimeUnit.SECONDS));
			assertUnavailable(updateFailure);
			assertEquals(1, service.getForUpdateSideEffects.get(),
					"automatic retry must not allocate a second update token");

			var iteratorFailure = assertThrows(ExecutionException.class, () -> stub
					.openIterator(OpenIteratorRequest.getDefaultInstance())
					.get(5, TimeUnit.SECONDS));
			assertUnavailable(iteratorFailure);
			assertEquals(1, service.openIteratorSideEffects.get(),
					"automatic retry must not leak a second server iterator");
		}
	}

	@Test
	void clientStreamingMutationIsNotReplayed() throws Exception {
		var service = new FailFirstService();
		try (var fixture = start(service)) {
			var response = new CompletableFuture<Empty>();
			var request = RocksDBServiceGrpc.newStub(fixture.channel()).putBatch(futureObserver(response));
			request.onNext(PutBatchRequest.getDefaultInstance());
			request.onCompleted();

			var failure = assertThrows(ExecutionException.class,
					() -> response.get(5, TimeUnit.SECONDS));
			assertUnavailable(failure);
			assertEquals(1, service.putBatchCalls.get(),
					"automatic retry must not reconstruct a completed client stream");
			assertEquals(1, service.putBatchSideEffects.get(),
					"the buffered request must not be applied twice after UNAVAILABLE");
		}
	}

	@Test
	void allowlistedIdempotentReadRetriesAndSucceeds() throws Exception {
		var service = new FailFirstService();
		try (var fixture = start(service)) {
			var response = RocksDBServiceGrpc.newFutureStub(fixture.channel())
					.getColumnId(GetColumnIdRequest.getDefaultInstance())
					.get(5, TimeUnit.SECONDS);

			assertEquals(GetColumnIdResponse.getDefaultInstance(), response);
			assertEquals(2, service.getColumnIdCalls.get(),
					"an idempotent lookup should survive one pre-response transport failure");
		}
	}

	@Test
	void retryAllowlistUsesGeneratedUnaryMethodDescriptors() throws Exception {
		var expected = Set.of(
				RocksDBServiceGrpc.getGetCapabilitiesMethod().getFullMethodName(),
				RocksDBServiceGrpc.getGetColumnIdMethod().getFullMethodName(),
				RocksDBServiceGrpc.getEstimateNumKeysMethod().getFullMethodName(),
				RocksDBServiceGrpc.getGetMethod().getFullMethodName(),
				RocksDBServiceGrpc.getExistsMethod().getFullMethodName(),
				RocksDBServiceGrpc.getExistsMultiMethod().getFullMethodName(),
				RocksDBServiceGrpc.getReduceRangeFirstAndLastMethod().getFullMethodName(),
				RocksDBServiceGrpc.getReduceRangeEntriesCountMethod().getFullMethodName(),
				RocksDBServiceGrpc.getGetRangePageMethod().getFullMethodName(),
				RocksDBServiceGrpc.getGetAllColumnDefinitionsMethod().getFullMethodName(),
				RocksDBServiceGrpc.getCheckMergeOperatorMethod().getFullMethodName(),
				RocksDBServiceGrpc.getCdcGetEarliestAvailableSequenceMethod().getFullMethodName(),
				RocksDBServiceGrpc.getCdcGetLastCommittedSequenceMethod().getFullMethodName());

		assertEquals(expected, invokeDelegate("automaticRetryMethodFullNames"));
		var generatedMethods = RocksDBServiceGrpc.getServiceDescriptor().getMethods().stream()
				.map(method -> method.getFullMethodName())
				.collect(java.util.stream.Collectors.toUnmodifiableSet());
		assertTrue(generatedMethods.containsAll(expected),
				"every configured name must resolve to the generated service descriptor");
		var configuredDescriptors = assertInstanceOf(java.util.List.class,
				invokeDelegate("automaticRetryMethodDescriptors"));
		for (var configuredDescriptor : configuredDescriptors) {
			var method = assertInstanceOf(io.grpc.MethodDescriptor.class, configuredDescriptor);
			assertEquals(io.grpc.MethodDescriptor.MethodType.UNARY, method.getType(),
					"streaming calls are deliberately excluded even when read-only");
		}
	}

	private static RetryFixture start(FailFirstService service) throws Exception {
		var loopback = InetAddress.getLoopbackAddress();
		var server = NettyServerBuilder
				.forAddress(new InetSocketAddress(loopback, 0))
				.addService(service)
				.build()
				.start();
		var builder = NettyChannelBuilder
				.forAddress(new InetSocketAddress(loopback, server.getPort()))
				.usePlaintext();
		configureProductionRetry(builder);
		return new RetryFixture(server, builder.build());
	}

	private static void configureProductionRetry(NettyChannelBuilder builder) throws Exception {
		Class<?> delegate = Class.forName("it.cavallium.rockserver.core.client.GrpcConnectionDelegate");
		Method configure = delegate.getDeclaredMethod("configureRetry", NettyChannelBuilder.class);
		configure.setAccessible(true);
		try {
			configure.invoke(null, builder);
		} catch (InvocationTargetException failure) {
			if (failure.getCause() instanceof Exception cause) {
				throw cause;
			}
			throw failure;
		}
	}

	private static Object invokeDelegate(String methodName) throws Exception {
		Class<?> delegate = Class.forName("it.cavallium.rockserver.core.client.GrpcConnectionDelegate");
		Method method = delegate.getDeclaredMethod(methodName);
		method.setAccessible(true);
		try {
			return method.invoke(null);
		} catch (InvocationTargetException failure) {
			if (failure.getCause() instanceof Exception cause) {
				throw cause;
			}
			throw failure;
		}
	}

	private static <T> StreamObserver<T> futureObserver(CompletableFuture<T> future) {
		return new StreamObserver<>() {
			@Override
			public void onNext(T value) {
				future.complete(value);
			}

			@Override
			public void onError(Throwable error) {
				future.completeExceptionally(error);
			}

			@Override
			public void onCompleted() {
				if (!future.isDone()) {
					future.completeExceptionally(new AssertionError("call completed without a response"));
				}
			}
		};
	}

	private static void assertUnavailable(ExecutionException failure) {
		var statusFailure = assertInstanceOf(StatusRuntimeException.class, failure.getCause());
		assertEquals(Status.Code.UNAVAILABLE, statusFailure.getStatus().getCode());
	}

	private record RetryFixture(Server server, ManagedChannel channel) implements AutoCloseable {

		@Override
		public void close() throws Exception {
			channel.shutdownNow();
			server.shutdownNow();
			assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS), "channel did not terminate");
			assertTrue(server.awaitTermination(5, TimeUnit.SECONDS), "server did not terminate");
		}
	}

	private static final class FailFirstService extends RocksDBServiceGrpc.RocksDBServiceImplBase {

		private final AtomicInteger openTransactionSideEffects = new AtomicInteger();
		private final AtomicInteger getForUpdateSideEffects = new AtomicInteger();
		private final AtomicInteger openIteratorSideEffects = new AtomicInteger();
		private final AtomicInteger putBatchCalls = new AtomicInteger();
		private final AtomicInteger putBatchSideEffects = new AtomicInteger();
		private final AtomicInteger getColumnIdCalls = new AtomicInteger();

		@Override
		public void getCapabilities(CapabilitiesRequest request,
				StreamObserver<CapabilitiesResponse> responseObserver) {
			responseObserver.onNext(CapabilitiesResponse.getDefaultInstance());
			responseObserver.onCompleted();
		}

		@Override
		public void openTransaction(OpenTransactionRequest request,
				StreamObserver<OpenTransactionResponse> responseObserver) {
			if (openTransactionSideEffects.incrementAndGet() == 1) {
				responseObserver.onError(unavailable());
			} else {
				responseObserver.onNext(OpenTransactionResponse.getDefaultInstance());
				responseObserver.onCompleted();
			}
		}

		@Override
		public void openIterator(OpenIteratorRequest request,
				StreamObserver<OpenIteratorResponse> responseObserver) {
			if (openIteratorSideEffects.incrementAndGet() == 1) {
				responseObserver.onError(unavailable());
			} else {
				responseObserver.onNext(OpenIteratorResponse.getDefaultInstance());
				responseObserver.onCompleted();
			}
		}

		@Override
		public void getForUpdate(GetRequest request, StreamObserver<UpdateBegin> responseObserver) {
			if (getForUpdateSideEffects.incrementAndGet() == 1) {
				responseObserver.onError(unavailable());
			} else {
				responseObserver.onNext(UpdateBegin.getDefaultInstance());
				responseObserver.onCompleted();
			}
		}

		@Override
		public StreamObserver<PutBatchRequest> putBatch(StreamObserver<Empty> responseObserver) {
			int call = putBatchCalls.incrementAndGet();
			return new StreamObserver<>() {
				@Override
				public void onNext(PutBatchRequest request) {
					putBatchSideEffects.incrementAndGet();
				}

				@Override
				public void onError(Throwable error) {
					// The client abandoned the request; there is no additional server action.
				}

				@Override
				public void onCompleted() {
					if (call == 1) {
						responseObserver.onError(unavailable());
					} else {
						responseObserver.onNext(Empty.getDefaultInstance());
						responseObserver.onCompleted();
					}
				}
			};
		}

		@Override
		public void getColumnId(GetColumnIdRequest request,
				StreamObserver<GetColumnIdResponse> responseObserver) {
			if (getColumnIdCalls.incrementAndGet() == 1) {
				responseObserver.onError(unavailable());
			} else {
				responseObserver.onNext(GetColumnIdResponse.getDefaultInstance());
				responseObserver.onCompleted();
			}
		}

		private static StatusRuntimeException unavailable() {
			return Status.UNAVAILABLE.withDescription("response lost after side effect").asRuntimeException();
		}
	}
}
