package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.RawScanEvent;
import it.cavallium.rockserver.core.common.RawSstToken;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.SerializedKVBatch;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.ScanRawRequest;
import it.cavallium.rockserver.core.common.api.proto.ScanRawResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Timeout(30)
class GrpcRawScanErrorMappingTest {

	private static final String ERROR_PREFIX = "RocksDBError: [uid:";
	private static final long CANCELLABLE_SCAN = 10_000L;
	private static final byte[] BATCH_BYTES = {0, 0, 0, 0};
	private static final RawSstToken COMPLETED_SST = new RawSstToken("000001.sst");

	private RecordingRawScanService service;
	private Server server;
	private GrpcConnection client;

	@BeforeEach
	void setUp() throws IOException {
		service = new RecordingRawScanService();
		server = ServerBuilder.forPort(0)
				.addService(service)
				.build()
				.start();
		client = GrpcConnection.forHostAndPort("grpc-raw-scan-error-mapping",
				new Utils.HostAndPort("127.0.0.1", server.getPort()));
	}

	@AfterEach
	void tearDown() throws IOException, InterruptedException {
		if (client != null) {
			client.close();
		}
		if (server != null) {
			server.shutdownNow();
			server.awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	@ParameterizedTest(name = "{0}: {1}")
	@MethodSource("typedFailures")
	void rawScanStreamsUseTheCommonTypedErrorContract(ScanFlavor flavor,
			FailureCase failureCase,
			RocksDBErrorType expectedType,
			String expectedMessage) {
		StepVerifier.create(scan(flavor, failureCase.columnId()))
				.expectErrorSatisfies(failure -> {
					var rocksFailure = assertInstanceOf(RocksDBException.class, failure);
					assertEquals(expectedType, rocksFailure.getErrorUniqueId());
					assertEquals(expectedMessage, rocksFailure.getMessage());
				})
				.verify(Duration.ofSeconds(5));

		assertEquals(1, service.scanCalls.get(),
				"error conversion must not resubscribe the raw-scan RPC");
	}

	@ParameterizedTest(name = "{0}: {1}")
	@MethodSource("fallbackFailures")
	void malformedUnknownAndUnrelatedStatusesRemainTransportErrors(ScanFlavor flavor,
			FailureCase failureCase,
			Status.Code expectedCode,
			String expectedDescription) {
		StepVerifier.create(scan(flavor, failureCase.columnId()))
				.expectErrorSatisfies(failure -> {
					var statusFailure = assertInstanceOf(StatusRuntimeException.class, failure);
					assertEquals(expectedCode, statusFailure.getStatus().getCode());
					assertEquals(expectedDescription, statusFailure.getStatus().getDescription());
				})
				.verify(Duration.ofSeconds(5));

		assertEquals(1, service.scanCalls.get(),
				"fallback conversion must not resubscribe the raw-scan RPC");
	}

	@ParameterizedTest(name = "{0}")
	@EnumSource(ScanFlavor.class)
	void cancellationStillPropagatesAfterARealDecodedEvent(ScanFlavor flavor) throws InterruptedException {
		StepVerifier.create(scan(flavor, CANCELLABLE_SCAN), 0)
				.thenRequest(1)
				.assertNext(event -> assertMappedBatch(flavor, event))
				.thenCancel()
				.verify(Duration.ofSeconds(5));

		assertTrue(service.cancellationObserved.await(5, TimeUnit.SECONDS),
				"the loopback server did not observe raw-scan cancellation");
		assertEquals(1, service.scanCalls.get(),
				"cancellation must not start a replacement raw-scan RPC");
	}

	private Publisher<?> scan(ScanFlavor flavor, long columnId) {
		var api = client.getAsyncApi(RequestContext.batch());
		return switch (flavor) {
			case LEGACY -> api.scanRawAsync(columnId, 0, 1);
			case RESUMABLE -> api.scanRawResumableAsync(columnId, 0, 1, Set.of());
		};
	}

	private static void assertMappedBatch(ScanFlavor flavor, Object event) {
		switch (flavor) {
			case LEGACY -> {
				var batch = assertInstanceOf(SerializedKVBatch.class, event);
				assertArrayEquals(BATCH_BYTES, batch.serialized().toByteArray());
			}
			case RESUMABLE -> {
				var batch = assertInstanceOf(RawScanEvent.Batch.class, event);
				assertArrayEquals(BATCH_BYTES, batch.serialized().toByteArray());
				assertEquals(COMPLETED_SST, batch.completedSstToken());
			}
		}
	}

	private static Stream<Arguments> typedFailures() {
		return Stream.of(ScanFlavor.values()).flatMap(flavor -> Stream.of(
				Arguments.of(flavor,
						FailureCase.OVERLOADED,
						RocksDBErrorType.SERVER_OVERLOADED,
						"raw scan admission full"),
				Arguments.of(flavor,
						FailureCase.DEADLINE,
						RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						"Deadline exceeded"),
				Arguments.of(flavor,
						FailureCase.DOMAIN,
						RocksDBErrorType.COLUMN_NOT_FOUND,
						"missing raw-scan column")));
	}

	private static Stream<Arguments> fallbackFailures() {
		return Stream.of(ScanFlavor.values()).flatMap(flavor -> Stream.of(
				Arguments.of(flavor,
						FailureCase.TRUNCATED,
						Status.Code.INTERNAL,
						ERROR_PREFIX),
				Arguments.of(flavor,
						FailureCase.FUTURE_DOMAIN,
						Status.Code.INTERNAL,
						ERROR_PREFIX + "FUTURE_RAW_SCAN_ERROR] future peer detail"),
				Arguments.of(flavor,
						FailureCase.UNRELATED,
						Status.Code.UNKNOWN,
						"plain peer failure")));
	}

	private enum ScanFlavor {
		LEGACY,
		RESUMABLE
	}

	private enum FailureCase {
		OVERLOADED(101L),
		DEADLINE(102L),
		DOMAIN(103L),
		TRUNCATED(104L),
		FUTURE_DOMAIN(105L),
		UNRELATED(106L);

		private final long columnId;

		FailureCase(long columnId) {
			this.columnId = columnId;
		}

		long columnId() {
			return columnId;
		}
	}

	private static final class RecordingRawScanService
			extends ReactorRocksDBServiceGrpc.RocksDBServiceImplBase {

		private final AtomicInteger scanCalls = new AtomicInteger();
		private final CountDownLatch cancellationObserved = new CountDownLatch(1);

		@Override
		public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
			return Mono.just(CapabilitiesResponse.newBuilder()
					.setWorkloadContractVersion(3)
					.build());
		}

		@Override
		public Flux<ScanRawResponse> scanRaw(ScanRawRequest request) {
			return Flux.defer(() -> {
				scanCalls.incrementAndGet();
				if (request.getColumnId() == CANCELLABLE_SCAN) {
					var response = ScanRawResponse.newBuilder()
							.setSerialized(ByteString.copyFrom(BATCH_BYTES));
					if (request.getResumable()) {
						response.setCompletedSstTokenAfterBatch(COMPLETED_SST.value());
					}
					return Flux.concat(Flux.just(response.build()), Flux.never())
							.doOnCancel(cancellationObserved::countDown);
				}
				return Flux.error(failureFor(request.getColumnId()));
			});
		}

		private static StatusRuntimeException failureFor(long columnId) {
			return switch ((int) columnId) {
				case 101 -> Status.RESOURCE_EXHAUSTED
						.withDescription(ERROR_PREFIX + "SERVER_OVERLOADED] raw scan admission full")
						.asRuntimeException();
				case 102 -> Status.DEADLINE_EXCEEDED
						.withDescription("transport deadline expired")
						.asRuntimeException();
				case 103 -> Status.INTERNAL
						.withDescription(ERROR_PREFIX + "COLUMN_NOT_FOUND] missing raw-scan column")
						.asRuntimeException();
				case 104 -> Status.INTERNAL
						.withDescription(ERROR_PREFIX)
						.asRuntimeException();
				case 105 -> Status.INTERNAL
						.withDescription(ERROR_PREFIX + "FUTURE_RAW_SCAN_ERROR] future peer detail")
						.asRuntimeException();
				default -> Status.UNKNOWN
						.withDescription("plain peer failure")
						.asRuntimeException();
			};
		}
	}
}
