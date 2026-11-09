package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.LeaderHint;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse;
import it.cavallium.rockserver.core.common.api.proto.GetRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * R49-1: a stream started on the leader's channel keeps running after the leader changes, since the channel is only
 * retired, gracefully (R46-1). Closing the connection must end it as it ends a call on the current leader's channel.
 * Before the fix it was left to the socket's teardown by the event loop's shutdown, which comes after the callback
 * executor's: the stream's listener was never told, and the caller's subscriber waited forever.
 *
 * <p>The connection owns its transport here: a shared {@link it.cavallium.rockserver.core.client.GrpcClientTransport}
 * is only taken by a one-address connection, which has no leader channels.</p>
 */
@Timeout(60)
class GrpcConnectionRetiredLeaderCloseTest {

	private static final Metadata.Key<String> LEADER = Metadata.Key.of(LeaderHint.METADATA_KEY,
			Metadata.ASCII_STRING_MARSHALLER);

	/** The address both nodes name as the leader. */
	private final AtomicReference<String> leader = new AtomicReference<>();
	private Node a;
	private Node b;
	private GrpcConnection client;
	private Disposable subscription;

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
		final AtomicInteger rangeCalls = new AtomicInteger();
		final CountDownLatch rangeCancelled = new CountDownLatch(1);

		@Override
		public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
			return Mono.just(CapabilitiesResponse.newBuilder().setWorkloadContractVersion(3).build());
		}

		@Override
		public Mono<GetColumnIdResponse> getColumnId(GetColumnIdRequest request) {
			columnIdCalls.incrementAndGet();
			return Mono.just(GetColumnIdResponse.newBuilder().setColumnId(73).build());
		}

		/** One entry, then nothing and no end: a long scan on a node that has stopped sending. */
		@Override
		public Flux<KV> getAllInRange(GetRangeRequest request) {
			rangeCalls.incrementAndGet();
			return Flux.just(KV.newBuilder().addKeys(ByteString.copyFromUtf8("k"))
							.setValue(ByteString.copyFromUtf8("v")).build())
					.concatWith(Flux.never())
					.doOnCancel(rangeCancelled::countDown);
		}
	}

	@AfterEach
	void tearDown() throws InterruptedException {
		if (subscription != null) {
			subscription.dispose();
		}
		if (client != null) {
			client.close();
		}
		for (var node : new Node[] {a, b}) {
			if (node != null) {
				node.server.shutdownNow();
				node.server.awaitTermination(5, TimeUnit.SECONDS);
			}
		}
	}

	@Test
	void closingTheConnectionEndsAStreamOnARetiredLeaderChannel() throws Exception {
		a = new Node();
		b = new Node();
		leader.set(b.address());
		// The ordinary channel goes to a, the first address; b is named as the leader.
		client = GrpcConnection.forHostsAndPorts("retired-leader-close",
				List.of(new HostAndPort("127.0.0.1", a.server.getPort()),
						new HostAndPort("127.0.0.1", b.server.getPort())));
		var api = client.getAsyncApi(RequestContext.batch());
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
		while (b.service.columnIdCalls.get() == 0 && System.nanoTime() < deadline) {
			assertEquals(73L, api.getColumnIdAsync("find-the-leader").get(5, TimeUnit.SECONDS));
			Thread.sleep(10);
		}
		assertTrue(b.service.columnIdCalls.get() > 0, "the leader's channel to b never served a call");

		var firstEntry = new CountDownLatch(1);
		var ended = new CompletableFuture<Throwable>();
		subscription = Flux.from(api.<it.cavallium.rockserver.core.common.KV>getRangeAsync(0, 41, null, null, false,
						RequestType.allInRange()))
				.subscribe(_ -> firstEntry.countDown(), ended::complete, () -> ended.complete(null));
		assertTrue(firstEntry.await(10, TimeUnit.SECONDS), "the stream did not start");
		assertEquals(1, b.service.rangeCalls.get(), "the stream did not go to the leader's channel");

		// The leader changes: b's next answer names a, which retires b's channel; the stream stays on it.
		leader.set(a.address());
		int onB = b.service.columnIdCalls.get();
		for (int i = 0; i < 3; i++) {
			assertEquals(73L, api.getColumnIdAsync("after-the-change").get(5, TimeUnit.SECONDS));
		}
		assertEquals(onB + 1, b.service.columnIdCalls.get(), "b's channel was not retired by the new leader's name");
		Thread.sleep(200);
		assertTrue(b.service.rangeCancelled.getCount() == 1 && !ended.isDone(),
				"the retirement ended the stream in flight (R46-1 keeps it until it ends)");

		client.close();
		client = null;

		Throwable error;
		try {
			error = ended.get(10, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			throw new AssertionError("the stream on the retired leader channel did not end when the connection closed",
					e);
		}
		assertNotNull(error, "the stream on the retired channel completed instead of failing");
		assertTrue(b.service.rangeCancelled.await(10, TimeUnit.SECONDS), "b did not see the stream cancelled");
	}
}
