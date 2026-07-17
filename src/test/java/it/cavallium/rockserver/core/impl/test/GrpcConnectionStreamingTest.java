package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.GetRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.MergeMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
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
		return Buf.wrap(new byte[] {
				(byte) (value >>> 24),
				(byte) (value >>> 16),
				(byte) (value >>> 8),
				(byte) value
		});
	}

	private static final class RecordingService extends ReactorRocksDBServiceGrpc.RocksDBServiceImplBase {

		private final AtomicInteger putCalls = new AtomicInteger();
		private final AtomicInteger putInitialFrames = new AtomicInteger();
		private final AtomicInteger putDataFrames = new AtomicInteger();
		private final AtomicInteger mergeCalls = new AtomicInteger();
		private final AtomicInteger mergeInitialFrames = new AtomicInteger();
		private final AtomicInteger mergeDataFrames = new AtomicInteger();
		private final AtomicInteger rangeCalls = new AtomicInteger();
		private final AtomicInteger noCacheRangeCalls = new AtomicInteger();
		private final AtomicLong rangePayloadTimeoutMs = new AtomicLong(-1);
		private final AtomicLong noCacheRangePayloadTimeoutMs = new AtomicLong(-1);
		private final CountDownLatch rangeCancellations = new CountDownLatch(2);

		@Override
		public Mono<Empty> putMulti(Flux<PutMultiRequest> requests) {
			putCalls.incrementAndGet();
			return requests
					.doOnNext(request -> recordFrames(request,
							putInitialFrames, putDataFrames))
					.then(Mono.just(Empty.getDefaultInstance()));
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
