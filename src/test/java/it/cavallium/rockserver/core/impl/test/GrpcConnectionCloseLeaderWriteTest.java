package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Empty;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.LeaderHint;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse;
import it.cavallium.rockserver.core.common.api.proto.OpenClientSessionRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenClientSessionResponse;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Mono;

/**
 * R51-3: R50-3 promises that a numbered write already in flight when {@code GrpcConnection.close()} starts keeps its
 * answer. On a multi-address connection a numbered write runs on the leader's channel, which the close forced at once
 * (R49-1): the write was cut locally and ended {@code OUTCOME_UNKNOWN}, although the server applied it a moment later.
 * The close must let it drain, within a bounded wait, before it forces the leader channels.
 */
@Timeout(60)
class GrpcConnectionCloseLeaderWriteTest {

	private static final Metadata.Key<String> LEADER = Metadata.Key.of(LeaderHint.METADATA_KEY,
			Metadata.ASCII_STRING_MARSHALLER);

	/** The address both nodes name as the leader. */
	private final AtomicReference<String> leader = new AtomicReference<>();
	private Node a;
	private Node b;
	private Thread closing;

	private final class Node {
		final NodeService service = new NodeService();
		final Server server;

		Node() throws IOException {
			server = ServerBuilder.forPort(0)
					.addService(ServerInterceptors.intercept(service, new NamesTheLeader()))
					.build()
					.start();
		}

		String address() {
			return "127.0.0.1:" + server.getPort();
		}
	}

	/** Adds the current {@link #leader} to every response's headers, as a replicated node does. */
	private final class NamesTheLeader implements ServerInterceptor {

		@Override
		public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
				ServerCallHandler<Q, R> next) {
			return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
				private volatile boolean headersSent;

				@Override
				public void sendHeaders(Metadata responseHeaders) {
					headersSent = true;
					put(responseHeaders);
					super.sendHeaders(responseHeaders);
				}

				@Override
				public void close(Status status, Metadata trailers) {
					if (!headersSent) {
						put(trailers);
					}
					super.close(status, trailers);
				}

				private void put(Metadata metadata) {
					var address = leader.get();
					if (address != null) {
						metadata.put(LEADER, address);
					}
				}
			}, headers);
		}
	}

	private static final class NodeService extends ReactorRocksDBServiceGrpc.RocksDBServiceImplBase {

		final AtomicInteger columnIdCalls = new AtomicInteger();
		final AtomicInteger puts = new AtomicInteger();
		final CountDownLatch firstPut = new CountDownLatch(1);
		final CompletableFuture<Empty> release = new CompletableFuture<>();

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
		public Mono<GetColumnIdResponse> getColumnId(GetColumnIdRequest request) {
			columnIdCalls.incrementAndGet();
			return Mono.just(GetColumnIdResponse.newBuilder().setColumnId(73).build());
		}

		@Override
		public Mono<Empty> put(PutRequest request) {
			assertTrue(request.getContext().hasRequestId(), "the write is numbered");
			puts.incrementAndGet();
			firstPut.countDown();
			// Held: the close has to wait for it.
			return Mono.fromFuture(release);
		}
	}

	@AfterEach
	void tearDown() throws InterruptedException {
		for (var node : new Node[] {a, b}) {
			if (node != null) {
				node.service.release.complete(Empty.getDefaultInstance());
			}
		}
		if (closing != null) {
			closing.join(TimeUnit.SECONDS.toMillis(70));
		}
		for (var node : new Node[] {a, b}) {
			if (node != null) {
				node.server.shutdownNow();
				node.server.awaitTermination(5, TimeUnit.SECONDS);
			}
		}
	}

	@Test
	void aNumberedWriteInFlightOnTheLeaderChannelDrainsAtClose() throws Exception {
		a = new Node();
		b = new Node();
		leader.set(b.address());
		// The ordinary channel goes to a, the first address; b is named as the leader.
		var client = GrpcConnection.forHostsAndPorts("close-leader-write",
				List.of(new HostAndPort("127.0.0.1", a.server.getPort()),
						new HostAndPort("127.0.0.1", b.server.getPort())));
		var api = client.getAsyncApi(RequestContext.batch());
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
		while (b.service.columnIdCalls.get() == 0 && System.nanoTime() < deadline) {
			assertEquals(73L, api.getColumnIdAsync("find-the-leader").get(5, TimeUnit.SECONDS));
			Thread.sleep(10);
		}
		assertTrue(b.service.columnIdCalls.get() > 0, "the leader's channel to b never served a call");

		var held = client.getAsyncApi(RequestContext.latency(Duration.ofSeconds(30)))
				.putAsync(0, 1, new Keys(Buf.wrap(new byte[] {1})), Buf.wrap(new byte[] {1}), RequestType.none());
		assertTrue(b.service.firstPut.await(10, TimeUnit.SECONDS), "the write did not reach the leader b");
		assertEquals(0, a.service.puts.get(), "the write went to the ordinary channel, not the leader's");

		closing = Thread.ofPlatform().name("closing").start(client::close);
		// The close must wait for the write in flight on the leader channel, not cut it.
		long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
		while (System.nanoTime() < until && !held.isDone()) {
			Thread.sleep(10);
		}
		assertTrue(!held.isDone(), () -> "the close ended the write in flight: " + outcome(held));
		assertTrue(closing.isAlive(), "close() returned while a numbered write was still in flight");

		b.service.release.complete(Empty.getDefaultInstance());
		assertNull(held.get(10, TimeUnit.SECONDS));
		closing.join(TimeUnit.SECONDS.toMillis(20));
		assertTrue(!closing.isAlive(), "close() did not end after the write in flight was answered");
		assertEquals(1, b.service.puts.get(), "the write was sent again");
	}

	private static String outcome(CompletableFuture<?> future) {
		try {
			return String.valueOf(future.getNow(null));
		} catch (RuntimeException e) {
			return e.toString();
		}
	}
}
