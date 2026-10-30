package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.PutMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.WorkloadProfile;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;

/**
 * A server in front of a non-embedded connection must bound the streaming mutation paths too: each stream holds a
 * worker for its whole life, so they cannot use the proxy scheduler's unbounded workers.
 */
@Timeout(60)
class GrpcProxyStreamingAdmissionTest {

	/** GrpcServer.PROXY_STREAM_PARALLELISM */
	private static final int STREAM_PARALLELISM = 256;
	private static final int STREAMS = STREAM_PARALLELISM + 44;

	@Test
	void streamingPutMultiRunsAtMostTheStreamParallelismAtOnce() throws Exception {
		var backend = new BlockingPutBackend();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
					.usePlaintext()
					.build();
			try {
				var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
				var responses = new ArrayList<CompletableFuture<Empty>>(STREAMS);
				try {
					for (int i = 0; i < STREAMS; i++) {
						responses.add(stub.putMulti(putMulti(i)).toFuture());
					}
					// Let every stream reach the backend if nothing bounds them
					long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
					while (backend.running.get() < STREAMS && System.nanoTime() < end) {
						Thread.sleep(10);
					}
					assertTrue(backend.running.get() > 0, "no stream reached the backend");
					assertTrue(backend.maxRunning.get() <= STREAM_PARALLELISM,
							"streams running at once: " + backend.maxRunning.get());
				} finally {
					backend.release.countDown();
				}
				for (var response : responses) {
					response.get(20, TimeUnit.SECONDS);
				}
				assertEquals(STREAMS, backend.puts.get());
			} finally {
				channel.shutdownNow();
				assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
			}
		}
	}

	private static Flux<PutMultiRequest> putMulti(int index) {
		var context = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
				.setProfile(WorkloadProfile.BATCH)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.setTimeoutNanos(TimeUnit.SECONDS.toNanos(40));
		return Flux.just(PutMultiRequest.newBuilder()
						.setInitialRequest(PutMultiInitialRequest.newBuilder().setColumnId(1L).setContext(context))
						.build(),
				PutMultiRequest.newBuilder()
						.setData(KV.newBuilder()
								.addKeys(ByteString.copyFrom(new byte[] {(byte) index, (byte) (index >>> 8)}))
								.setValue(ByteString.copyFrom(new byte[] {1})))
						.build());
	}

	/** A downstream connection without an embedded fast path, whose puts wait until released. */
	private static final class BlockingPutBackend implements RocksDBConnection {

		private final CountDownLatch release = new CountDownLatch(1);
		private final AtomicInteger running = new AtomicInteger();
		private final AtomicInteger maxRunning = new AtomicInteger();
		private final AtomicInteger puts = new AtomicInteger();
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
				if (!(request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Put<?>)) {
					throw new UnsupportedOperationException("Unexpected request: " + request);
				}
				maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
				try {
					if (!release.await(30, TimeUnit.SECONDS)) {
						throw new AssertionError("put was not released");
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new AssertionError(e);
				} finally {
					running.decrementAndGet();
				}
				puts.incrementAndGet();
				return null;
			}
		};

		@Override
		public RockserverCapabilities getCapabilities() {
			return RockserverCapabilities.CURRENT;
		}

		@Override
		public URI getUrl() {
			return URI.create("test://blocking-put-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return new RocksDBAsyncAPI() {};
		}

		@Override
		public void close() {
		}
	}
}
