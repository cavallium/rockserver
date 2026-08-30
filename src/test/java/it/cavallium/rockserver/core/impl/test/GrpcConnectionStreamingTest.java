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
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.CdcPollRequest;
import it.cavallium.rockserver.core.common.api.proto.CdcPollResponse;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse;
import it.cavallium.rockserver.core.common.api.proto.GetRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.MergeMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiListRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
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
	private static final int MAX_UNARY_PUT_MULTI_ITEMS = 65_536;
	private static final int RANGE_SIZE = 1_025;
	private static final int CLEANUP_BATCH_SIZE = 256;
	private static final long STREAMING_RANGE_COLUMN_ID = 41;
	private static final long CANCELLABLE_RANGE_COLUMN_ID = 43;
	private static final long CLEANUP_WRITE_COLUMN_ID = 47;
	private static final long BULK_TRANSACTION_ID = 59;
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
	void boundedPutMultiUsesOneUnaryListRpcAndPreservesTheTransaction() throws Exception {
		var response = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).putMultiAsync(
				BULK_TRANSACTION_ID, 17, keys(), values(), RequestType.none()).get(10, TimeUnit.SECONDS);

		assertEquals(List.of(), response);
		assertEquals(1, service.putListCalls.get());
		assertEquals(0, service.putCalls.get());
		assertEquals(BATCH_SIZE, service.putListItems.get());
		assertEquals(BULK_TRANSACTION_ID, service.putListTransactionId.get());
		assertEquals(17, service.putListColumnId.get());
	}

	@Test
	void boundedEnsurePutMultiUsesTheUnaryListEnsureRpc() throws Exception {
		var response = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).putMultiAsync(
				BULK_TRANSACTION_ID, 19, keys(), values(), RequestType.ensure()).get(10, TimeUnit.SECONDS);

		assertEquals(List.of(), response);
		assertEquals(1, service.putListEnsureCalls.get());
		assertEquals(0, service.putListCalls.get());
		assertEquals(0, service.putCalls.get());
		assertEquals(BATCH_SIZE, service.putListItems.get());
	}

	@Test
	void maximumItemCountStillUsesOneUnaryListRpc() throws Exception {
		var response = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).putMultiAsync(
				BULK_TRANSACTION_ID,
				20,
				keys(MAX_UNARY_PUT_MULTI_ITEMS),
				values(MAX_UNARY_PUT_MULTI_ITEMS),
				RequestType.none()).get(10, TimeUnit.SECONDS);

		assertEquals(List.of(), response);
		assertEquals(1, service.putListCalls.get());
		assertEquals(0, service.putCalls.get());
		assertEquals(MAX_UNARY_PUT_MULTI_ITEMS, service.putListItems.get());
	}

	@Test
	void putMultiAboveTheUnaryByteBudgetFallsBackToOneStreamingRpc() throws Exception {
		int items = RangeBudget.DEFAULT_MAX_ITEMS;
		var response = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).putMultiAsync(
				BULK_TRANSACTION_ID, 21, keys(items), values(items, 2_048), RequestType.none())
				.get(10, TimeUnit.SECONDS);

		assertEquals(List.of(), response);
		assertEquals(0, service.putListCalls.get());
		assertEquals(1, service.putCalls.get());
		assertEquals(1, service.putInitialFrames.get());
		assertEquals(items, service.putDataFrames.get());
	}

	@Test
	void mergeMultiAboveLegacyChunkSizeUsesOneStreamingRpc() throws Exception {
		var response = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).mergeMultiAsync(
				0, 23, keys(), values(), RequestType.none()).get(10, TimeUnit.SECONDS);

		assertEquals(List.of(), response);
		assertEquals(1, service.mergeCalls.get());
		assertEquals(1, service.mergeInitialFrames.get());
		assertEquals(BATCH_SIZE, service.mergeDataFrames.get());
	}

	@Test
	void rangeVariantsUseTransportDeadlineAndTypedErrorMapping() throws InterruptedException {
		var context = it.cavallium.rockserver.core.common.RequestContext.batch(
				Duration.ofMillis(RANGE_TIMEOUT_MS));
		expectReadDeadline(client.getAsyncApi(context).getRangeAsync(
				0, 29, null, null, false, RequestType.allInRange()));
		expectReadDeadline(client.getAsyncApi(context).getRangeAsync(
				0, 31, null, null, false, RequestType.allInRangeNoCache()));

		assertEquals(1, service.rangeCalls.get());
		assertEquals(1, service.noCacheRangeCalls.get());
		assertTrue(service.rangeCancellations.await(5, TimeUnit.SECONDS),
				"the server did not observe both streaming RPC deadline cancellations");
	}

	@Test
	void rangeLargerThanReactiveGrpcQueueCanWriteBatchesThroughSameConnection() throws Exception {
		var callbacksOnVirtualThreads = new AtomicBoolean(true);
		var callbackCount = new AtomicInteger();
		Flux<it.cavallium.rockserver.core.common.KV> range = Flux.from(client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getRangeAsync(
				0, STREAMING_RANGE_COLUMN_ID, null, null, false,
				RequestType.allInRange()));
		var cleanup = range
				.doOnNext(_ -> {
					callbackCount.incrementAndGet();
					if (!Thread.currentThread().isVirtual()) {
						callbacksOnVirtualThreads.set(false);
					}
				})
				.buffer(CLEANUP_BATCH_SIZE)
				.concatMap(batch -> Mono.fromFuture(client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).putMultiAsync(
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
			assertEquals(73, client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getColumnIdAsync("responsive-unary")
					.get(5, TimeUnit.SECONDS));
			var cdcBatch = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).cdcPollBatchAsync("responsive-cdc", null, 10)
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
		assertEquals(RANGE_SIZE, service.putListItems.get());
		assertEquals(0, service.putDataFrames.get());
	}

	@Test
	void backpressuredCancellationReachesServerAndLeavesConnectionHealthy() throws Exception {
		var range = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getRangeAsync(
				0, CANCELLABLE_RANGE_COLUMN_ID, null, null, false,
				RequestType.allInRange());

		StepVerifier.create(range, 0)
				.thenRequest(1)
				.expectNextCount(1)
				.thenCancel()
				.verify(Duration.ofSeconds(5));

		assertTrue(service.cancellableRangeCancellation.await(5, TimeUnit.SECONDS),
				"the server did not observe downstream cancellation");
		assertEquals(73, client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getColumnIdAsync("healthy-after-cancellation")
				.get(5, TimeUnit.SECONDS));
	}

	@Test
	void closeTerminatesOwnedCallbackExecutorAndSingleEventLoop() throws Exception {
		var callbackExecutor = privateField(client, "callbackExecutor", ExecutorService.class);
		var eventLoopGroup = privateField(client, "eventLoopGroup", EventLoopGroup.class);
		var channel = privateField(client, "channel", ManagedChannel.class);
		assertEquals(1, StreamSupport.stream(eventLoopGroup.spliterator(), false).count());

		assertEquals(73, client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getColumnIdAsync("executor-started")
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
		return keys(BATCH_SIZE);
	}

	private static List<Keys> keys(int size) {
		return IntStream.range(0, size)
				.mapToObj(value -> new Keys(intBuf(value)))
				.toList();
	}

	private static List<Buf> values() {
		return values(BATCH_SIZE);
	}

	private static List<Buf> values(int size) {
		return IntStream.range(0, size)
				.mapToObj(GrpcConnectionStreamingTest::intBuf)
				.toList();
	}

	private static List<Buf> values(int size, int valueBytes) {
		return Collections.nCopies(size, Buf.createZeroes(valueBytes));
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

		@Override
		public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
			return Mono.just(CapabilitiesResponse.newBuilder()
					.setWorkloadContractVersion(3)
					.build());
		}

		private final AtomicInteger putCalls = new AtomicInteger();
		private final AtomicInteger putInitialFrames = new AtomicInteger();
		private final AtomicInteger putDataFrames = new AtomicInteger();
		private final AtomicInteger putListCalls = new AtomicInteger();
		private final AtomicInteger putListEnsureCalls = new AtomicInteger();
		private final AtomicInteger putListItems = new AtomicInteger();
		private final AtomicLong putListTransactionId = new AtomicLong(-1L);
		private final AtomicLong putListColumnId = new AtomicLong(-1L);
		private final AtomicInteger cleanupWriteCalls = new AtomicInteger();
		private final AtomicInteger mergeCalls = new AtomicInteger();
		private final AtomicInteger mergeInitialFrames = new AtomicInteger();
		private final AtomicInteger mergeDataFrames = new AtomicInteger();
		private final AtomicInteger rangeCalls = new AtomicInteger();
		private final AtomicInteger noCacheRangeCalls = new AtomicInteger();
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
		public Mono<Empty> putMultiList(PutMultiListRequest request) {
			putListCalls.incrementAndGet();
			return recordPutMultiList(request);
		}

		@Override
		public Mono<Empty> putMultiListEnsure(PutMultiListRequest request) {
			putListEnsureCalls.incrementAndGet();
			return recordPutMultiList(request);
		}

		private Mono<Empty> recordPutMultiList(PutMultiListRequest request) {
			putListItems.addAndGet(request.getDataCount());
			putListTransactionId.set(request.getInitialRequest().getTransactionOrUpdateId());
			putListColumnId.set(request.getInitialRequest().getColumnId());
			if (request.getInitialRequest().getColumnId() == CLEANUP_WRITE_COLUMN_ID
					&& cleanupWriteCalls.incrementAndGet() == 1) {
				firstCleanupWriteStarted.countDown();
				return Mono.fromFuture(firstCleanupWriteResponse);
			}
			return Mono.just(Empty.getDefaultInstance());
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
