package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.StringValue;
import io.grpc.CallOptions;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GrpcRetryLoggingInterceptorTest {

	private static final String SERVICE_NAME = "retry.logging.Test";
	private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
	private static final MethodDescriptor.Marshaller<StringValue> STRING_VALUE_MARSHALLER =
			new MethodDescriptor.Marshaller<>() {
				@Override
				public InputStream stream(StringValue value) {
					return new ByteArrayInputStream(value.toByteArray());
				}

				@Override
				public StringValue parse(InputStream stream) {
					try {
						return StringValue.parseFrom(stream);
					} catch (IOException ex) {
						throw new IllegalArgumentException("Invalid test response", ex);
					}
				}
			};
	private static final MethodDescriptor<StringValue, StringValue> RETRY_THEN_SUCCEED = method("RetryThenSucceed");
	private static final MethodDescriptor<StringValue, StringValue> ALWAYS_UNAVAILABLE = method("AlwaysUnavailable");
	private static final MethodDescriptor<StringValue, StringValue> NON_RETRYABLE = method("NonRetryable");

	private final AtomicInteger retryThenSucceedCalls = new AtomicInteger();
	private final AtomicInteger alwaysUnavailableCalls = new AtomicInteger();
	private final AtomicInteger nonRetryableCalls = new AtomicInteger();
	private final AtomicLong nanoTime = new AtomicLong();
	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private Server server;
	private ManagedChannel channel;

	@BeforeEach
	void setUp() throws Exception {
		server = NettyServerBuilder.forAddress(new InetSocketAddress(LOOPBACK, 0))
				.addService(ServerServiceDefinition.builder(SERVICE_NAME)
						.addMethod(RETRY_THEN_SUCCEED, ServerCalls.asyncUnaryCall((request, observer) -> {
							if (retryThenSucceedCalls.incrementAndGet() == 1) {
								observer.onError(Status.UNAVAILABLE.withDescription("transient detail").asRuntimeException());
							} else {
								observer.onNext(StringValue.of("ok"));
								observer.onCompleted();
							}
						}))
						.addMethod(ALWAYS_UNAVAILABLE, ServerCalls.asyncUnaryCall((request, observer) -> {
							int attempt = alwaysUnavailableCalls.incrementAndGet();
							observer.onError(Status.UNAVAILABLE
									.withDescription("sensitive attempt detail " + attempt)
									.asRuntimeException());
						}))
						.addMethod(NON_RETRYABLE, ServerCalls.asyncUnaryCall((request, observer) -> {
							nonRetryableCalls.incrementAndGet();
							observer.onError(Status.UNAVAILABLE.withDescription("non-retryable detail").asRuntimeException());
						}))
						.build())
				.build()
				.start();

		channel = NettyChannelBuilder.forAddress(new InetSocketAddress(LOOPBACK, server.getPort()))
				.usePlaintext()
				.enableRetry()
				.maxRetryAttempts(3)
				.defaultServiceConfig(retryServiceConfig())
				.intercept(retryLoggingInterceptor(warnings::add, nanoTime::get,
						TimeUnit.MINUTES.toNanos(1), 256))
				.build();
	}

	@AfterEach
	void tearDown() throws InterruptedException {
		if (channel != null) {
			channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
		}
		if (server != null) {
			server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
		}
	}

	@Test
	void successfulAutomaticRetryDoesNotLogAnIntermediateFailure() {
		var response = call(RETRY_THEN_SUCCEED);

		assertEquals("ok", response.getValue());
		assertEquals(2, retryThenSucceedCalls.get(), "the transport policy must actually retry once");
		assertTrue(warnings.isEmpty(), "the interceptor sees the successful logical close, not failed attempts");
	}

	@Test
	void exhaustedAutomaticRetriesLogOneTruthfulTerminalWarning() {
		var failure = assertThrows(StatusRuntimeException.class, () -> call(ALWAYS_UNAVAILABLE));

		assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
		assertEquals(3, alwaysUnavailableCalls.get(), "the assertion must cover an exhausted retry policy");
		assertEquals(List.of(
				"gRPC call to retry.logging.Test/AlwaysUnavailable reached terminal status UNAVAILABLE. "
						+ "No further automatic retry will be attempted by gRPC."
		), warnings);
		assertFalse(warnings.getFirst().contains("sensitive attempt detail"),
				"transport descriptions may contain backend details and must not be copied into the warning");
		assertFalse(warnings.getFirst().contains("Retry will be attempted automatically"),
				"onClose is terminal for the logical call");
	}

	@Test
	void methodOutsideRetryPolicyLogsFinalFailureWithoutClaimingItWasRetried() {
		var failure = assertThrows(StatusRuntimeException.class, () -> call(NON_RETRYABLE));

		assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
		assertEquals(1, nonRetryableCalls.get(), "this method is intentionally absent from retry methodConfig");
		assertEquals(List.of(
				"gRPC call to retry.logging.Test/NonRetryable reached terminal status UNAVAILABLE. "
						+ "No further automatic retry will be attempted by gRPC."
		), warnings);
	}

	@Test
	void burstOfTerminalFailuresLogsOnlyTheImmediateWarning() {
		for (int call = 0; call < 8; call++) {
			assertThrows(StatusRuntimeException.class, () -> call(ALWAYS_UNAVAILABLE));
		}

		assertEquals(24, alwaysUnavailableCalls.get(), "every logical call must still exhaust all three attempts");
		assertEquals(1, warnings.size(), "failures within one interval must be coalesced");
		assertFalse(warnings.getFirst().contains("Suppressed"),
				"the immediate warning has no earlier failures to summarize");
	}

	@Test
	void nextIntervalWarningReportsTheCoalescedSuppressedCount() {
		assertThrows(StatusRuntimeException.class, () -> call(ALWAYS_UNAVAILABLE));
		for (int call = 0; call < 6; call++) {
			assertThrows(StatusRuntimeException.class, () -> call(ALWAYS_UNAVAILABLE));
		}
		assertEquals(1, warnings.size());

		nanoTime.set(TimeUnit.MINUTES.toNanos(1));
		assertThrows(StatusRuntimeException.class, () -> call(ALWAYS_UNAVAILABLE));

		assertEquals(2, warnings.size());
		assertEquals(
				"gRPC call to retry.logging.Test/AlwaysUnavailable reached terminal status UNAVAILABLE. "
						+ "No further automatic retry will be attempted by gRPC. "
						+ "Suppressed 6 additional terminal warnings for this method and status since the previous warning.",
				warnings.get(1));
	}

	@Test
	void methodAndStatusCombinationsHaveDistinctRateLimitState() throws ReflectiveOperationException {
		var localWarnings = new CopyOnWriteArrayList<String>();
		var interceptor = retryLoggingInterceptor(localWarnings::add, () -> 0L, 100L, 8);
		var otherMethod = method("OtherUnavailable");

		logTerminalWarning(interceptor, ALWAYS_UNAVAILABLE, Status.UNAVAILABLE);
		logTerminalWarning(interceptor, ALWAYS_UNAVAILABLE, Status.UNAVAILABLE);
		logTerminalWarning(interceptor, ALWAYS_UNAVAILABLE, Status.RESOURCE_EXHAUSTED);
		logTerminalWarning(interceptor, ALWAYS_UNAVAILABLE, Status.RESOURCE_EXHAUSTED);
		logTerminalWarning(interceptor, otherMethod, Status.UNAVAILABLE);
		logTerminalWarning(interceptor, otherMethod, Status.UNAVAILABLE);

		assertEquals(3, localWarnings.size());
		assertTrue(localWarnings.get(0).contains("AlwaysUnavailable reached terminal status UNAVAILABLE"));
		assertTrue(localWarnings.get(1).contains("AlwaysUnavailable reached terminal status RESOURCE_EXHAUSTED"));
		assertTrue(localWarnings.get(2).contains("OtherUnavailable reached terminal status UNAVAILABLE"));
	}

	@Test
	void rateLimitStateRetainsOnlyTheConfiguredNumberOfKeys() throws ReflectiveOperationException {
		var localWarnings = new CopyOnWriteArrayList<String>();
		var interceptor = retryLoggingInterceptor(localWarnings::add, () -> 0L, 100L, 3);

		for (int index = 0; index < 20; index++) {
			logTerminalWarning(interceptor, method("Dynamic" + index), Status.UNAVAILABLE);
		}

		assertEquals(3, warningStateSize(interceptor));
		assertEquals(20, localWarnings.size(), "a newly observed key still receives its immediate warning");
	}

	private StringValue call(MethodDescriptor<StringValue, StringValue> method) {
		return ClientCalls.blockingUnaryCall(channel, method,
				CallOptions.DEFAULT.withDeadlineAfter(10, TimeUnit.SECONDS), StringValue.of("request"));
	}

	private static Map<String, ?> retryServiceConfig() {
		return Map.of("methodConfig", List.of(Map.of(
				"name", List.of(
						Map.of("service", SERVICE_NAME, "method", "RetryThenSucceed"),
						Map.of("service", SERVICE_NAME, "method", "AlwaysUnavailable")
				),
				"retryPolicy", Map.of(
						"maxAttempts", 3.0d,
						"initialBackoff", "0.001s",
						"maxBackoff", "0.001s",
						"backoffMultiplier", 1.0d,
						"retryableStatusCodes", List.of(Status.Code.UNAVAILABLE.name())
				)
		)));
	}

	private static MethodDescriptor<StringValue, StringValue> method(String name) {
		return MethodDescriptor.<StringValue, StringValue>newBuilder()
				.setType(MethodDescriptor.MethodType.UNARY)
				.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, name))
				.setRequestMarshaller(STRING_VALUE_MARSHALLER)
				.setResponseMarshaller(STRING_VALUE_MARSHALLER)
				.build();
	}

	private static ClientInterceptor retryLoggingInterceptor(Consumer<String> warningLogger,
			LongSupplier nanoTimeSource,
			long warningIntervalNanos,
			int maxWarningKeys) throws ReflectiveOperationException {
		Class<?> interceptorClass = Class.forName(
				"it.cavallium.rockserver.core.client.GrpcConnectionDelegate$RetryLoggingInterceptor");
		Constructor<?> constructor = interceptorClass.getDeclaredConstructor(
				Consumer.class, LongSupplier.class, long.class, int.class);
		constructor.setAccessible(true);
		return (ClientInterceptor) constructor.newInstance(
				warningLogger, nanoTimeSource, warningIntervalNanos, maxWarningKeys);
	}

	private static void logTerminalWarning(ClientInterceptor interceptor,
			MethodDescriptor<?, ?> method,
			Status status) throws ReflectiveOperationException {
		Method logMethod = interceptor.getClass().getDeclaredMethod(
				"logTerminalStatusWarning", MethodDescriptor.class, Status.class);
		logMethod.setAccessible(true);
		logMethod.invoke(interceptor, method, status);
	}

	private static int warningStateSize(ClientInterceptor interceptor) throws ReflectiveOperationException {
		Field statesField = interceptor.getClass().getDeclaredField("warningStates");
		statesField.setAccessible(true);
		return ((Map<?, ?>) statesField.get(interceptor)).size();
	}
}
