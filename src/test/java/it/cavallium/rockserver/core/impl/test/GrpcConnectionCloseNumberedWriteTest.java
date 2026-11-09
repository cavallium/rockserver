package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.OpenClientSessionRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenClientSessionResponse;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Mono;

/**
 * R50-3: {@code GrpcConnection.close()} shuts the channel down gracefully and waits, up to a minute, for the calls on
 * it. A numbered write started meanwhile on another thread was numbered and handed to the shut-down channel, which
 * failed it locally with {@code UNAVAILABLE}: a lost call to {@code SessionWrites}, so the write ended
 * {@code OUTCOME_UNKNOWN} although nothing left the client. It must end flagged not applied, as a write after the
 * close does, while the copy already in flight keeps its graceful drain.
 */
@Timeout(60)
class GrpcConnectionCloseNumberedWriteTest {

	@Test
	void aNumberedWriteStartedWhileTheConnectionClosesIsNotApplied() throws Exception {
		var puts = new AtomicInteger();
		var firstPut = new CountDownLatch(1);
		var release = new CompletableFuture<Empty>();
		var service = new ReactorRocksDBServiceGrpc.RocksDBServiceImplBase() {
			@Override
			public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
				return Mono.just(CapabilitiesResponse.newBuilder().setWorkloadContractVersion(3)
						.setClientSessions(true).build());
			}

			@Override
			public Mono<OpenClientSessionResponse> openClientSession(OpenClientSessionRequest request) {
				return Mono.just(OpenClientSessionResponse.newBuilder().setSessionId(5).build());
			}

			@Override
			public Mono<Empty> put(PutRequest request) {
				assertTrue(request.getContext().hasRequestId(), "the write is numbered");
				if (puts.incrementAndGet() == 1) {
					firstPut.countDown();
					// Held: the channel's graceful shutdown waits for it.
					return Mono.fromFuture(release);
				}
				return Mono.just(Empty.getDefaultInstance());
			}
		};
		Server server = ServerBuilder.forPort(0).addService(service).build().start();
		Thread closing = null;
		try {
			var client = GrpcConnection.forHostAndPort("close-numbered-write", new HostAndPort("127.0.0.1",
					server.getPort()));
			var held = client.getAsyncApi(RequestContext.latency(Duration.ofSeconds(30)))
					.putAsync(0, 1, new Keys(Buf.wrap(new byte[] {1})), Buf.wrap(new byte[] {1}), RequestType.none());
			assertTrue(firstPut.await(10, TimeUnit.SECONDS), "the first write reached the server");

			closing = Thread.ofPlatform().name("closing").start(client::close);
			// close() is now waiting for the held call (its awaitTermination is up to a minute).
			Thread.sleep(500);
			assertTrue(closing.isAlive(), "close() returned while a call was still running");

			var late = client.getAsyncApi(RequestContext.latency(Duration.ofSeconds(5)))
					.putAsync(0, 1, new Keys(Buf.wrap(new byte[] {2})), Buf.wrap(new byte[] {2}), RequestType.none());
			var error = assertThrows(ExecutionException.class, () -> late.get(15, TimeUnit.SECONDS));
			var rocks = rocks(error);
			assertEquals(RocksDBErrorType.SERVER_OVERLOADED, rocks.getErrorUniqueId(), rocks::toString);
			assertTrue(rocks.isNotApplied(), () -> "not flagged not applied: " + rocks);
			assertEquals(1, puts.get(), "the late write reached the server");

			// The copy in flight still drains: its answer comes through, and the close then ends.
			release.complete(Empty.getDefaultInstance());
			assertNull(held.get(10, TimeUnit.SECONDS));
			closing.join(TimeUnit.SECONDS.toMillis(20));
			assertTrue(!closing.isAlive(), "close() did not end after the held call was answered");
		} finally {
			release.complete(Empty.getDefaultInstance());
			if (closing != null) {
				closing.join(TimeUnit.SECONDS.toMillis(70));
			}
			server.shutdownNow();
			server.awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	private static RocksDBException rocks(Throwable error) {
		Throwable cause = error;
		while (cause.getCause() != null && !(cause instanceof RocksDBException)) {
			cause = cause.getCause();
		}
		return assertInstanceOf(RocksDBException.class, cause);
	}
}
