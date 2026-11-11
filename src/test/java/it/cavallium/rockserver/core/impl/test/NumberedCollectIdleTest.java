package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.api.proto.ClientRequestId;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.PutMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.RequestContext;
import it.cavallium.rockserver.core.common.api.proto.WorkloadProfile;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;

/**
 * R61-5: a numbered streamed multi write is collected whole before it is applied, and what it collected is taken from
 * a budget every such call of the server shares. A client that goes silent in the middle of its stream must not keep
 * its share: the collection ends when no item arrived for the idle limit
 * ({@code rockserver.grpc.numbered-multi-collect-idle-ms}), flagged not applied, and the server tells how much of the
 * budget is taken ({@code GrpcServer.numberedMultiCollectBytes}).
 */
@Timeout(60)
class NumberedCollectIdleTest {

	private static final String IDLE_PROPERTY = "rockserver.grpc.numbered-multi-collect-idle-ms";
	private static final Metadata.Key<String> APPLIED = Metadata.Key.of(RocksDBException.APPLIED_METADATA_KEY,
			Metadata.ASCII_STRING_MARSHALLER);

	private static PutMultiRequest initial() {
		return PutMultiRequest.newBuilder()
				.setInitialRequest(PutMultiInitialRequest.newBuilder()
						.setColumnId(1L)
						.setContext(RequestContext.newBuilder()
								.setProfile(WorkloadProfile.BATCH)
								.setWorkloadContractVersion(3)
								.setTimeoutNanos(Long.MAX_VALUE)
								.setRequestId(ClientRequestId.newBuilder().setSessionId(7L).setSequence(1L))))
				.build();
	}

	private static PutMultiRequest item(int key) {
		return PutMultiRequest.newBuilder()
				.setData(KV.newBuilder()
						.addKeys(ByteString.copyFrom(new byte[] {(byte) key}))
						.setValue(ByteString.copyFrom(new byte[1024])))
				.build();
	}

	/** The bytes the server's collections hold now: {@code GrpcServer.numberedMultiCollectBytes()}. */
	private static long collectBytes(GrpcServer server) throws ReflectiveOperationException {
		return (long) GrpcServer.class.getMethod("numberedMultiCollectBytes").invoke(server);
	}

	private static void awaitCollectBytes(GrpcServer server, boolean taken) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while ((collectBytes(server) > 0) != taken) {
			assertTrue(System.nanoTime() < deadline, "collect bytes: " + collectBytes(server));
			Thread.sleep(10);
		}
	}

	@Test
	void aSilentStreamGivesItsBytesBackAtTheIdleLimit() throws Exception {
		var before = System.getProperty(IDLE_PROPERTY);
		System.setProperty(IDLE_PROPERTY, "400");
		var backend = new Backend();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			assertEquals(0, collectBytes(server));
			var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
			try {
				var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
				// Two items, then silence: the stream is never completed.
				var silent = stub.putMulti(Flux.concat(Flux.just(initial(), item(1), item(2)), Flux.never())).toFuture();
				awaitCollectBytes(server, true);
				assertTrue(collectBytes(server) >= 2 * 1024, "two items of 1 KiB are held: " + collectBytes(server));
				var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> silent.get(10, TimeUnit.SECONDS));
				var status = (StatusRuntimeException) failure.getCause();
				assertEquals(Status.Code.DEADLINE_EXCEEDED, status.getStatus().getCode(), status.toString());
				assertTrue(status.getStatus().getDescription().contains("putMulti"), status.toString());
				// Nothing of a collected write is applied before its last item: its caller may send it again.
				assertEquals(RocksDBException.NOT_APPLIED, status.getTrailers().get(APPLIED));
				awaitCollectBytes(server, false);
				assertEquals(0, backend.writes.get(), "nothing was applied");

				// A stream whose items keep arriving within the limit is collected and applied, however long it takes.
				var slow = Flux.concat(Flux.just(initial()),
						Flux.interval(Duration.ofMillis(150)).take(8).map(i -> item(i.intValue())));
				stub.putMulti(slow).block(Duration.ofSeconds(20));
				assertEquals(1, backend.writes.get(), "one multi write");
				assertEquals(0, collectBytes(server));
			} finally {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
		} finally {
			if (before == null) {
				System.clearProperty(IDLE_PROPERTY);
			} else {
				System.setProperty(IDLE_PROPERTY, before);
			}
		}
	}

	/**
	 * The server keepalive ({@code it.cavallium.rockserver.grpc.server.keep-alive-time-ms}, off by default) pings a
	 * silent connection: a live client answers and keeps its connection and its open stream. (A client that vanished
	 * needs a real silent peer, not tested here.)
	 */
	@Test
	void aServerKeepaliveLeavesALiveClientConnected() throws Exception {
		System.setProperty("it.cavallium.rockserver.grpc.server.keep-alive-time-ms", "300");
		System.setProperty("it.cavallium.rockserver.grpc.server.keep-alive-timeout-ms", "300");
		var backend = new Backend();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
			try {
				var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
				// One stream open over several pings, with items 1 s apart (the collect idle limit is 30 s).
				var slow = Flux.concat(Flux.just(initial()),
						Flux.interval(Duration.ofSeconds(1)).take(3).map(i -> item(i.intValue())));
				stub.putMulti(slow).block(Duration.ofSeconds(20));
				assertEquals(1, backend.writes.get());
			} finally {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
		} finally {
			System.clearProperty("it.cavallium.rockserver.grpc.server.keep-alive-time-ms");
			System.clearProperty("it.cavallium.rockserver.grpc.server.keep-alive-timeout-ms");
		}
	}

	private static final class Backend implements RocksDBConnection {

		private final AtomicInteger writes = new AtomicInteger();

		public RockserverCapabilities getCapabilities() {
			return RockserverCapabilities.CURRENT;
		}

		@Override
		public URI getUrl() {
			return URI.create("test://collect-idle-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBSyncAPI() {};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBAsyncAPI() {
				@Override
				@SuppressWarnings("unchecked")
				public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
					writes.incrementAndGet();
					return (RA) CompletableFuture.completedFuture(List.of());
				}
			};
		}

		@Override
		public void close() {
		}
	}
}
