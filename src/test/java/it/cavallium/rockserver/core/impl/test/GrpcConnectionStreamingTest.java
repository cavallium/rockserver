package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.netty.channel.EventLoopGroup;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.CdcPollRequest;
import it.cavallium.rockserver.core.common.api.proto.CdcPollResponse;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse;
import it.cavallium.rockserver.core.common.api.proto.GetRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.MergeMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Timeout(30)
class GrpcConnectionStreamingTest {

	private static final int BATCH_SIZE = 1_001;
	private static final int RANGE_SIZE = 1_025;
	private static final int CLEANUP_BATCH_SIZE = 256;
	private static final long STREAMING_RANGE_COLUMN_ID = 41;
	private static final long CANCELLABLE_RANGE_COLUMN_ID = 43;
	private static final long CLEANUP_WRITE_COLUMN_ID = 47;
	private static final long RANGE_TIMEOUT_MS = 1_000;

	private RecordingService service;
	private Server server;
	private GrpcConnection client;

	@BeforeEach
	void setUp() throws IOException {
		service = new RecordingService();
		server = ServerBuilder.forPort(0)
				.addService(service)
				.build()
				.start();
		client = GrpcConnection.forHostAndPort("grpc-connection-streaming",
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

	@Test
	void putMultiAboveLegacyChunkSizeUsesOneStreamingRpc() throws Exception {
		var response = client.getAsyncApi().putMultiAsync(
				0, 17, keys(), values(), RequestType.none()).get(10, TimeUnit.SECONDS);

		assertEquals(List.of(), response);
		assertEquals(1, service.putCalls.get());
		assertEquals(1, service.putInitialFrames.get());
		assertEquals(BATCH_SIZE, service.putDataFrames.get());
	}

	@Test
	void mergeMultiAboveLegacyChunkSizeUsesOneStreamingRpc() throws Exception {
		var response = client.getAsyncApi().mergeMultiAsync(
				0, 23, keys(), values(), RequestType.none()).get(10, TimeUnit.SECONDS);

		assertEquals(List.of(), response);
		assertEquals(1, service.mergeCalls.get());
		assertEquals(1, service.mergeInitialFrames.get());
		assertEquals(BATCH_SIZE, service.mergeDataFrames.get());
	}

	@Test
	void rangeVariantsUseTransportDeadlineAndTypedErrorMapping() throws InterruptedException {
		expectReadDeadline(client.getAsyncApi().getRangeAsync(
				0, 29, null, null, false, RequestType.allInRange(), RANGE_TIMEOUT_MS));
		expectReadDeadline(client.getAsyncApi().getRangeAsync(
				0, 31, null, null, false, RequestType.allInRangeNoCache(), RANGE_TIMEOUT_MS));

		assertEquals(1, service.rangeCalls.get());
		assertEquals(1, service.noCacheRangeCalls.get());
		assertEquals(RANGE_TIMEOUT_MS, service.rangePayloadTimeoutMs.get());
		assertEquals(RANGE_TIMEOUT_MS, service.noCacheRangePayloadTimeoutMs.get());
		assertTrue(service.rangeCancellations.await(5, TimeUnit.SECONDS),
				"the server did not observe both streaming RPC deadline cancellations");
	}

	@Test
	void rangeLargerThanReactiveGrpcQueueCanWriteBatchesThroughSameConnection() throws Exception {
		var callbacksOnVirtualThreads = new AtomicBoolean(true);
		var callbackCount = new AtomicInteger();
		Flux<it.cavallium.rockserver.core.common.KV> range = Flux.from(client.getAsyncApi().getRangeAsync(
				0, STREAMING_RANGE_COLUMN_ID, null, null, false,
				RequestType.allInRange(), Long.MAX_VALUE));
		var cleanup = range
				.doOnNext(_ -> {
					callbackCount.incrementAndGet();
					if (!Thread.currentThread().isVirtual()) {
						callbacksOnVirtualThreads.set(false);
					}
				})
				.buffer(CLEANUP_BATCH_SIZE)
				.concatMap(batch -> Mono.fromFuture(client.getAsyncApi().putMultiAsync(
						0,
						CLEANUP_WRITE_COLUMN_ID,
						batch.stream().map(it.cavallium.rockserver.core.common.KV::keys).toList(),
						batch.stream().map(it.cavallium.rockserver.core.common.KV::value).toList(),
						RequestType.none())))
				.then()
				.toFuture();

		assertTrue(service.firstCleanupWriteStarted.await(5, TimeUnit.SECONDS),
				"the range did not start its nested cleanup write");
		try {
			assertEquals(73, client.getAsyncApi().getColumnIdAsync("responsive-unary")
					.get(5, TimeUnit.SECONDS));
			var cdcBatch = client.getAsyncApi().cdcPollBatchAsync("responsive-cdc", null, 10)
					.toFuture()
					.get(5, TimeUnit.SECONDS);
			assertEquals(101, cdcBatch.nextSeq());
		} finally {
			service.releaseFirstCleanupWrite();
		}

		cleanup.get(10, TimeUnit.SECONDS);
		assertEquals(RANGE_SIZE, callbackCount.get());
		assertTrue(callbacksOnVirtualThreads.get(),
				"range callbacks ran on a Netty platform thread instead of the owned virtual-thread executor");
		assertEquals((RANGE_SIZE + CLEANUP_BATCH_SIZE - 1) / CLEANUP_BATCH_SIZE,
				service.cleanupWriteCalls.get());
		assertEquals(RANGE_SIZE, service.putDataFrames.get());
	}

	@Test
	void backpressuredCancellationReachesServerAndLeavesConnectionHealthy() throws Exception {
		var range = client.getAsyncApi().getRangeAsync(
				0, CANCELLABLE_RANGE_COLUMN_ID, null, null, false,
				RequestType.allInRange(), Long.MAX_VALUE);

		StepVerifier.create(range, 0)
				.thenRequest(1)
				.expectNextCount(1)
				.thenCancel()
				.verify(Duration.ofSeconds(5));

		assertTrue(service.cancellableRangeCancellation.await(5, TimeUnit.SECONDS),
				"the server did not observe downstream cancellation");
		assertEquals(73, client.getAsyncApi().getColumnIdAsync("healthy-after-cancellation")
				.get(5, TimeUnit.SECONDS));
	}

	@Test
	void closeTerminatesOwnedCallbackExecutorAndSingleEventLoop() throws Exception {
		var callbackExecutor = privateField(client, "callbackExecutor", ExecutorService.class);
		var eventLoopGroup = privateField(client, "eventLoopGroup", EventLoopGroup.class);
		var channel = privateField(client, "channel", ManagedChannel.class);
		assertEquals(1, StreamSupport.stream(eventLoopGroup.spliterator(), false).count());

		assertEquals(73, client.getAsyncApi().getColumnIdAsync("executor-started")
				.get(5, TimeUnit.SECONDS));
		client.close();
		client = null;

		assertTrue(channel.isTerminated(), "the channel leaked after close");
		assertTrue(callbackExecutor.isShutdown(), "the callback executor accepted work after close");
		assertTrue(callbackExecutor.isTerminated(), "the callback executor retained virtual-thread tasks");
		assertTrue(eventLoopGroup.isTerminated(), "the Netty event-loop thread leaked after close");
	}

	private static void expectReadDeadline(Publisher<?> range) {
		StepVerifier.create(range)
				.expectErrorSatisfies(error -> {
					var rocksError = assertInstanceOf(RocksDBException.class, error);
					assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
							rocksError.getErrorUniqueId());
					assertEquals("Deadline exceeded", rocksError.getMessage());
				})
				.verify(Duration.ofSeconds(5));
	}

	private static List<Keys> keys() {
		return IntStream.range(0, BATCH_SIZE)
				.mapToObj(value -> new Keys(intBuf(value)))
				.toList();
	}

	private static List<Buf> values() {
		return IntStream.range(0, BATCH_SIZE)
				.mapToObj(GrpcConnectionStreamingTest::intBuf)
				.toList();
	}

	private static Buf intBuf(int value) {
		return Buf.wrap(intBytes(value));
	}

	private static byte[] intBytes(int value) {
		return new byte[] {
				(byte) (value >>> 24),
				(byte) (value >>> 16),
				(byte) (value >>> 8),
				(byte) value
		};
	}

	private static <T> T privateField(GrpcConnection connection, String name, Class<T> fieldType)
			throws ReflectiveOperationException {
		Field field = GrpcConnection.class.getDeclaredField(name);
		field.setAccessible(true);
		return fieldType.cast(field.get(connection));
	}

	private static final class RecordingService extends ReactorRocksDBServiceGrpc.RocksDBServiceImplBase {

		private final AtomicInteger putCalls = new AtomicInteger();
		private final AtomicInteger putInitialFrames = new AtomicInteger();
		private final AtomicInteger putDataFrames = new AtomicInteger();
		private final AtomicInteger cleanupWriteCalls = new AtomicInteger();
		private final AtomicInteger mergeCalls = new AtomicInteger();
		private final AtomicInteger mergeInitialFrames = new AtomicInteger();
		private final AtomicInteger mergeDataFrames = new AtomicInteger();
		private final AtomicInteger rangeCalls = new AtomicInteger();
		private final AtomicInteger noCacheRangeCalls = new AtomicInteger();
		private final AtomicLong rangePayloadTimeoutMs = new AtomicLong(-1);
		private final AtomicLong noCacheRangePayloadTimeoutMs = new AtomicLong(-1);
		private final CountDownLatch rangeCancellations = new CountDownLatch(2);
		private final CountDownLatch cancellableRangeCancellation = new CountDownLatch(1);
		private final CountDownLatch firstCleanupWriteStarted = new CountDownLatch(1);
		private final CompletableFuture<Empty> firstCleanupWriteResponse = new CompletableFuture<>();

		@Override
		public Mono<Empty> putMulti(Flux<PutMultiRequest> requests) {
			putCalls.incrementAndGet();
			var cleanupWriteNumber = new AtomicInteger();
			return requests
					.doOnNext(request -> {
						recordFrames(request, putInitialFrames, putDataFrames);
						if (request.hasInitialRequest()
								&& request.getInitialRequest().getColumnId() == CLEANUP_WRITE_COLUMN_ID) {
							cleanupWriteNumber.set(cleanupWriteCalls.incrementAndGet());
						}
					})
					.then(Mono.defer(() -> {
						if (cleanupWriteNumber.get() == 1) {
							firstCleanupWriteStarted.countDown();
							return Mono.fromFuture(firstCleanupWriteResponse);
						}
						return Mono.just(Empty.getDefaultInstance());
					}));
		}

		@Override
		public Mono<Empty> mergeMulti(Flux<MergeMultiRequest> requests) {
			mergeCalls.incrementAndGet();
			return requests
					.doOnNext(request -> recordFrames(request,
							mergeInitialFrames, mergeDataFrames))
					.then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Flux<KV> getAllInRange(GetRangeRequest request) {
			rangeCalls.incrementAndGet();
			rangePayloadTimeoutMs.set(request.getTimeoutMs());
			if (request.getColumnId() == STREAMING_RANGE_COLUMN_ID) {
				return Flux.range(0, RANGE_SIZE).map(RecordingService::kv);
			}
			if (request.getColumnId() == CANCELLABLE_RANGE_COLUMN_ID) {
				return Flux.<KV, Integer>generate(() -> 0, (index, sink) -> {
					sink.next(kv(index));
					return index + 1;
				}).doOnCancel(cancellableRangeCancellation::countDown);
			}
			return neverCompletingRange();
		}

		@Override
		public Flux<KV> getAllInRangeNoCache(GetRangeRequest request) {
			noCacheRangeCalls.incrementAndGet();
			noCacheRangePayloadTimeoutMs.set(request.getTimeoutMs());
			return neverCompletingRange();
		}

		private Flux<KV> neverCompletingRange() {
			return Flux.<KV>never().doOnCancel(rangeCancellations::countDown);
		}

		@Override
		public Mono<GetColumnIdResponse> getColumnId(GetColumnIdRequest request) {
			return Mono.just(GetColumnIdResponse.newBuilder().setColumnId(73).build());
		}

		@Override
		public Mono<CdcPollResponse> cdcPollBatch(CdcPollRequest request) {
			return Mono.just(CdcPollResponse.newBuilder().setNextSeq(101).build());
		}

		private void releaseFirstCleanupWrite() {
			firstCleanupWriteResponse.complete(Empty.getDefaultInstance());
		}

		private static KV kv(int value) {
			return KV.newBuilder()
					.addKeys(ByteString.copyFrom(intBytes(value)))
					.setValue(ByteString.copyFrom(intBytes(value)))
					.build();
		}

		private static void recordFrames(PutMultiRequest request,
				AtomicInteger initialFrames,
				AtomicInteger dataFrames) {
			if (request.hasInitialRequest()) {
				initialFrames.incrementAndGet();
			}
			if (request.hasData()) {
				dataFrames.incrementAndGet();
			}
		}

		private static void recordFrames(MergeMultiRequest request,
				AtomicInteger initialFrames,
				AtomicInteger dataFrames) {
			if (request.hasInitialRequest()) {
				initialFrames.incrementAndGet();
			}
			if (request.hasData()) {
				dataFrames.incrementAndGet();
			}
		}
	}
}
