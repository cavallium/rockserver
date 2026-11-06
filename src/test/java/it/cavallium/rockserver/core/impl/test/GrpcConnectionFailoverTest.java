package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.ClientBuilder;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse;
import it.cavallium.rockserver.core.common.api.proto.OpenTransactionRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenTransactionResponse;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

/**
 * A {@link GrpcConnection} to several addresses. Each server has its own database or service, so a test sees which
 * server took a call. Connections are never recreated: every switch happens inside the one channel.
 */
@Timeout(60)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class GrpcConnectionFailoverTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.latency(Duration.ofSeconds(10));
	private static final String CONNECT_TIMEOUT_PROPERTY = "it.cavallium.rockserver.grpc.client.connect-timeout-ms";
	private static final String KEEP_ALIVE_TIMEOUT_PROPERTY = "it.cavallium.rockserver.grpc.client.keep-alive-timeout-ms";
	private static final String MAX_ATTEMPTS_PROPERTY = "it.cavallium.rockserver.grpc.client.max-retry-attempts";
	private static final String MAX_RECONNECT_DELAY_PROPERTY
			= "it.cavallium.rockserver.grpc.client.max-reconnect-delay-ms";
	private static final String HANDSHAKE_WAIT_PROPERTY = "it.cavallium.rockserver.grpc.client.handshake-wait-ms";
	private static final String MAX_CONNECTION_AGE_PROPERTY = "it.cavallium.rockserver.grpc.server.max-connection-age-ms";
	private static final String MAX_CONNECTION_AGE_GRACE_PROPERTY
			= "it.cavallium.rockserver.grpc.server.max-connection-age-grace-ms";
	/** A slow failure: an answer of the resolver that arrives after the channel moved on. */
	private static final Duration SLOW_FAILURE = Duration.ofMillis(300);

	@TempDir
	Path tempDir;
	private final List<AutoCloseable> resources = new ArrayList<>();

	@AfterEach
	void closeResources() throws Exception {
		for (var resource : resources.reversed()) {
			resource.close();
		}
		System.clearProperty(CONNECT_TIMEOUT_PROPERTY);
		System.clearProperty(KEEP_ALIVE_TIMEOUT_PROPERTY);
		System.clearProperty(MAX_ATTEMPTS_PROPERTY);
		System.clearProperty(MAX_RECONNECT_DELAY_PROPERTY);
		System.clearProperty(HANDSHAKE_WAIT_PROPERTY);
		System.clearProperty(MAX_CONNECTION_AGE_PROPERTY);
		System.clearProperty(MAX_CONNECTION_AGE_GRACE_PROPERTY);
	}

	/** The first address serves as long as it is up; once it stops, new calls run on the second. */
	@Test
	void newCallsMoveToTheNextServerWhenTheFirstStops() throws Exception {
		var nodes = List.of(startNode("a"), startNode("b"), startNode("c"));
		var client = connect(nodes.stream().map(EmbeddedNode::address).toList());
		var api = client.getAsyncApi(CONTEXT);
		long column = nodes.getFirst().column;

		api.putAsync(0, column, key(1), value(1), RequestType.none()).get(10, TimeUnit.SECONDS);
		assertEquals(nodes.get(0).port(), remotePort(client));
		assertArrayEquals(value(1).toByteArray(), nodes.get(0).read(1));
		assertNull(nodes.get(1).read(1));
		assertNull(nodes.get(2).read(1));

		nodes.get(0).stopServer();

		long stoppedAt = System.nanoTime();
		api.putAsync(0, column, key(2), value(2), RequestType.none()).get(10, TimeUnit.SECONDS);
		long switchMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stoppedAt);
		assertTrue(switchMillis < 3_000, () -> "the switch took " + switchMillis + " ms");

		assertEquals(nodes.get(1).port(), remotePort(client));
		assertNull(nodes.get(0).read(2));
		assertArrayEquals(value(2).toByteArray(), nodes.get(1).read(2));
		assertNull(nodes.get(2).read(2));

		// The next one down the list, when the second stops too.
		nodes.get(1).stopServer();
		api.putAsync(0, column, key(3), value(3), RequestType.none()).get(10, TimeUnit.SECONDS);
		assertEquals(nodes.get(2).port(), remotePort(client));
		assertArrayEquals(value(3).toByteArray(), nodes.get(2).read(3));
	}

	/** A cluster that is starting: the connection waits for the first node to answer, without being told how long. */
	@Test
	void aConnectionToSeveralAddressesWaitsForTheFirstNodeToServe() throws Exception {
		// One attempt per handshake: the retries of the call must not stand in for the wait.
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		var late = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(late);
		late.stop();
		var addresses = List.of(new HostAndPort("127.0.0.1", unusedPort()), late.address());
		var connecting = CompletableFuture.supplyAsync(() -> connect(addresses));
		Thread.sleep(1_000);
		assertFalse(connecting.isDone());
		late.restart();
		assertEquals(late.port(), awaitServedBy(connecting.get(10, TimeUnit.SECONDS), late));
	}

	/** The wait ends: an application must not hang in a constructor. */
	@Test
	void aConnectionToSeveralAddressesFailsAfterTheConfiguredWait() throws Exception {
		System.setProperty(HANDSHAKE_WAIT_PROPERTY, "1500");
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		var addresses = List.of(new HostAndPort("127.0.0.1", unusedPort()), new HostAndPort("127.0.0.1", unusedPort()));
		long start = System.nanoTime();
		var failure = assertThrows(StatusRuntimeException.class, () -> connect(addresses));
		long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
		assertTrue(millis >= 1_500 && millis < 10_000, () -> millis + " ms");
	}

	/** One address has no wait of its own: nothing else serves this client, and its caller may have a plan. */
	@Test
	void aConnectionToOneAddressDoesNotWait() throws Exception {
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		var address = new HostAndPort("127.0.0.1", unusedPort());
		long start = System.nanoTime();
		assertThrows(StatusRuntimeException.class, () -> connect(List.of(address)));
		long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		assertTrue(millis < 3_000, () -> millis + " ms");
	}

	/** The property sets the wait of one address too. */
	@Test
	void aConnectionToOneAddressWaitsIfConfigured() throws Exception {
		System.setProperty(HANDSHAKE_WAIT_PROPERTY, "20000");
		var late = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(late);
		late.stop();
		var connecting = CompletableFuture.supplyAsync(() -> connect(List.of(late.address())));
		Thread.sleep(1_000);
		assertFalse(connecting.isDone());
		late.restart();
		assertEquals(late.port(), awaitServedBy(connecting.get(10, TimeUnit.SECONDS), late));
	}

	/** What a plain client gets by default: the retries of the call, 0.5, 1, 2 and 4 s with a jitter of 20%. */
	@Test
	void aRefusedConnectionToOneAddressFailsAfterTheRetriesOfTheCall() throws Exception {
		var address = new HostAndPort("127.0.0.1", unusedPort());
		long start = System.nanoTime();
		var failure = assertThrows(StatusRuntimeException.class, () -> connect(List.of(address)));
		long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
		assertTrue(millis >= 5_000 && millis < 10_500, () -> millis + " ms");
	}

	/** Those retries are why they stay: a client that starts with its server, or during a restart, connects. */
	@Test
	void aServerThatStartsDuringTheRetriesOfOneAddressIsReached() throws Exception {
		var late = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(late);
		late.stop();
		var connecting = CompletableFuture.supplyAsync(() -> connect(List.of(late.address())));
		Thread.sleep(1_500);
		assertFalse(connecting.isDone());
		late.restart();
		assertEquals(late.port(), awaitServedBy(connecting.get(10, TimeUnit.SECONDS), late));
	}

	/** Silent and frozen nodes use up whole attempts: the wait goes on after the deadline of one. */
	@Test
	void theWaitHoldsWithSilentAndFrozenNodes() throws Exception {
		var late = new FakeNode(FIRST_NODE_COLUMN_ID);
		var frozen = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(late);
		resources.add(frozen);
		late.stop();
		try (var silent = new SilentListener(); var process = new NodeProxy(frozen.port())) {
			process.freeze();
			var addresses = List.of(new HostAndPort("127.0.0.1", silent.port()),
					new HostAndPort("127.0.0.1", process.port()), late.address());
			var connecting = CompletableFuture.supplyAsync(() -> connect(addresses));
			Thread.sleep(12_000);
			assertFalse(connecting.isDone());
			late.restart();
			assertEquals(late.port(), awaitServedBy(connecting.get(15, TimeUnit.SECONDS), late));
		}
	}

	/** The list goes on through the attempts, so its silent nodes can take longer than one attempt. */
	@Test
	void aListCanStartWithMoreSilentNodesThanOneAttemptCovers() throws Exception {
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		var addresses = new ArrayList<HostAndPort>();
		for (int i = 0; i < 4; i++) {
			var silent = new SilentListener();
			resources.add(silent);
			addresses.add(new HostAndPort("127.0.0.1", silent.port()));
		}
		addresses.add(node.address());
		var client = connect(addresses);
		assertEquals(node.port(), remotePort(client));
	}

	/** The default wait is 30 s and it ends then, also with a node that takes up whole attempts. */
	@Test
	void theDefaultWaitEndsAfter30Seconds() throws Exception {
		try (var silent = new SilentListener()) {
			var addresses = List.of(new HostAndPort("127.0.0.1", silent.port()),
					new HostAndPort("127.0.0.1", unusedPort()), new HostAndPort("127.0.0.1", unusedPort()));
			long start = System.nanoTime();
			assertThrows(StatusRuntimeException.class, () -> connect(addresses));
			long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
			assertTrue(millis >= 29_900 && millis < 31_000, () -> millis + " ms");
		}
	}

	/** The end of the wait cuts the attempt in progress, which says nothing about the nodes: the failure stays theirs. */
	@Test
	void anAttemptCutByTheEndOfTheWaitKeepsTheFailureOfTheNodes() throws Exception {
		System.setProperty(HANDSHAKE_WAIT_PROPERTY, "12000");
		var addresses = List.of(new HostAndPort("127.0.0.1", unusedPort()), new HostAndPort("127.0.0.1", unusedPort()));
		long start = System.nanoTime();
		var failure = assertThrows(StatusRuntimeException.class, () -> connect(addresses));
		long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		assertEquals(Status.Code.UNAVAILABLE, failure.getStatus().getCode());
		assertTrue(millis >= 11_900 && millis < 13_000, () -> millis + " ms");
	}

	/** A wait shorter than an attempt leaves the attempt its time: a wait of 0 is one attempt, not none. */
	@Test
	void aShortWaitLeavesTheFirstAttemptItsTime() throws Exception {
		System.setProperty(HANDSHAKE_WAIT_PROPERTY, "100");
		System.setProperty(CONNECT_TIMEOUT_PROPERTY, "500");
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		try (var silent = new SilentListener()) {
			var client = connect(List.of(new HostAndPort("127.0.0.1", silent.port()), node.address()));
			assertEquals(node.port(), remotePort(client));
		}
	}

	/** Each attempt skips the growing delay of the channel, and attempts do not follow each other without a pause. */
	@Test
	void theWaitTriesEveryHalfSecond() throws Exception {
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		System.setProperty(HANDSHAKE_WAIT_PROPERTY, "6000");
		try (var first = new ConnectionCounter(unusedPort()); var second = new ConnectionCounter(unusedPort())) {
			assertThrows(StatusRuntimeException.class, () -> connect(List.of(new HostAndPort("127.0.0.1", first.port()),
					new HostAndPort("127.0.0.1", second.port()))));
			// gRPC alone would try 4 times: at once, and at about 1 s, 2.6 s and 5.2 s.
			assertTrue(first.count() >= 8 && first.count() <= 20, () -> first.count() + " attempts");
		}
	}

	/** Asking again would give the same answer: a server without the handshake fails the connection at once. */
	@Test
	void aServerWithoutTheHandshakeFailsAtOnce() throws Exception {
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		node.service.capabilitiesFailure = Status.UNIMPLEMENTED;
		assertFailsAtOnce(RocksDBException.class, node);
	}

	/** Only waiting for a node is waiting: no other failure of the handshake is asked again. */
	@Test
	void aFailedHandshakeFailsAtOnce() throws Exception {
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		node.service.capabilitiesFailure = Status.INTERNAL;
		var failure = assertFailsAtOnce(StatusRuntimeException.class, node);
		assertEquals(Status.Code.INTERNAL, failure.getStatus().getCode());
	}

	@Test
	void anIncompatibleServerFailsAtOnce() throws Exception {
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		node.service.workloadContractVersion = RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION + 1;
		var failure = assertFailsAtOnce(RocksDBException.class, node);
		assertEquals(RocksDBErrorType.NOT_IMPLEMENTED, failure.getErrorUniqueId());
	}

	private <T extends Throwable> T assertFailsAtOnce(Class<T> type, FakeNode node) throws Exception {
		var addresses = List.of(node.address(), new HostAndPort("127.0.0.1", unusedPort()));
		long start = System.nanoTime();
		var failure = assertThrows(type, () -> connect(addresses));
		long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		assertEquals(1, node.service.capabilitiesCalls.get());
		assertTrue(millis < 3_000, () -> millis + " ms");
		return failure;
	}

	/** Interrupting the constructor is how a caller gives up on the wait: nothing stays behind it, or is forgotten. */
	@Test
	void anInterruptEndsTheWait() throws Exception {
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		var addresses = List.of(new HostAndPort("127.0.0.1", unusedPort()), new HostAndPort("127.0.0.1", unusedPort()));
		var eventLoopsBefore = eventLoopThreads();
		var failure = new AtomicReference<RuntimeException>();
		var flagRestored = new AtomicBoolean();
		var connecting = Thread.ofPlatform().start(() -> {
			try {
				connect(addresses);
			} catch (RuntimeException e) {
				failure.set(e);
				flagRestored.set(Thread.currentThread().isInterrupted());
			}
		});
		Thread.sleep(1_000);
		assertTrue(connecting.isAlive());
		assertFalse(eventLoopsBefore.containsAll(eventLoopThreads()), "no channel is running");

		connecting.interrupt();
		connecting.join(Duration.ofSeconds(2));
		assertFalse(connecting.isAlive());
		var rocksDbFailure = assertInstanceOf(RocksDBException.class, failure.get());
		assertTrue(rocksDbFailure.getMessage().contains("Interrupted"), rocksDbFailure::getMessage);
		assertTrue(flagRestored.get(), "the interrupt was swallowed");
		awaitEventLoopsGone(eventLoopsBefore);
	}

	/** The caller of a deferred connection is released at once, and the handshake stops with it. */
	@Test
	void closingADeferredConnectionEndsItsHandshakeWait() throws Exception {
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		try (var attempts = new ConnectionCounter(unusedPort())) {
			var eventLoopsBefore = eventLoopThreads();
			var builder = new ClientBuilder();
			builder.setName("deferred-wait");
			builder.setDeferred(true);
			builder.setHttpAddresses(List.of(new HostAndPort("127.0.0.1", attempts.port()),
					new HostAndPort("127.0.0.1", unusedPort())));
			var connection = builder.build();
			var pending = connection.getAsyncApi(CONTEXT).getColumnIdAsync("x");
			Thread.sleep(1_000);
			assertFalse(pending.isDone());
			assertTrue(attempts.count() > 0);

			connection.close();
			assertThrows(ExecutionException.class, () -> pending.get(1, TimeUnit.SECONDS));
			awaitEventLoopsGone(eventLoopsBefore);
			int attemptsAtClose = attempts.count();
			Thread.sleep(1_500);
			assertEquals(attemptsAtClose, attempts.count(), "the handshake went on after close");
		}
	}

	/**
	 * The timeout of the future can end an attempt before the deadline of the call does, so the wait must go on after
	 * either. This call never ends, which leaves only the timeout.
	 */
	@Test
	void aHandshakeThatTimesOutIsAskedAgain() throws Exception {
		System.setProperty(HANDSHAKE_WAIT_PROPERTY, "15000");
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		var calls = new AtomicInteger();
		var channel = NettyChannelBuilder.forAddress("127.0.0.1", node.port()).usePlaintext().build();
		try {
			var stub = RocksDBServiceGrpc.newFutureStub(ClientInterceptors.intercept(channel, new ClientInterceptor() {
				@Override
				public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
						CallOptions callOptions, Channel next) {
					return calls.getAndIncrement() == 0 ? new CallThatNeverEnds<>() : next.newCall(method, callOptions);
				}
			}));
			var handshake = Class.forName("it.cavallium.rockserver.core.client.GrpcConnectionDelegate")
					.getDeclaredMethod("handshake", ManagedChannel.class, stub.getClass(), boolean.class);
			handshake.setAccessible(true);
			assertInstanceOf(CapabilitiesResponse.class, handshake.invoke(null, channel, stub, true));
			assertEquals(2, calls.get());
		} finally {
			channel.shutdownNow();
		}
	}

	private static final class CallThatNeverEnds<ReqT, RespT> extends ClientCall<ReqT, RespT> {

		@Override
		public void start(Listener<RespT> listener, Metadata headers) {
		}

		@Override
		public void request(int numMessages) {
		}

		@Override
		public void cancel(String message, Throwable cause) {
		}

		@Override
		public void halfClose() {
		}

		@Override
		public void sendMessage(ReqT message) {
		}
	}

	@Test
	void aDeadFirstAddressDoesNotStopTheConnection() throws Exception {
		var node = startNode("only");

		long startedAt = System.nanoTime();
		var client = connect(List.of(new HostAndPort("127.0.0.1", unusedPort()), node.address()));
		assertEquals(node.port(), remotePort(client));
		assertTrue(System.nanoTime() - startedAt < TimeUnit.SECONDS.toNanos(3), "a refused connection is not waited for");

		client.getAsyncApi(CONTEXT).putAsync(0, node.column, key(1), value(1), RequestType.none())
				.get(10, TimeUnit.SECONDS);
		assertArrayEquals(value(1).toByteArray(), node.read(1));
	}

	@Test
	void theConnectionStaysOnItsNodeWhenAnEarlierOneComesBack() throws Exception {
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(first);
		resources.add(second);
		var client = connect(List.of(first.address(), second.address()));
		assertEquals(first.port(), remotePort(client));

		first.stop();
		assertEquals(second.port(), awaitServedBy(client, second));
		first.restart();

		for (int i = 0; i < 20; i++) {
			assertEquals(second.port(), remotePort(client));
			Thread.sleep(25);
		}
	}

	/**
	 * A read in flight when its node dies is retried by the existing retry policy, on the next node, while a call the
	 * policy does not allow fails with UNAVAILABLE. The policy and the address list share one service config.
	 */
	@Test
	void callsInFlightFollowTheRetryPolicy() throws Exception {
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(first);
		resources.add(second);
		first.service.holdCalls = true;
		try (var process = new NodeProxy(first.port())) {
			var client = connect(List.of(new HostAndPort("127.0.0.1", process.port()), second.address()));
			var api = client.getAsyncApi(CONTEXT);

			var read = api.getColumnIdAsync("held");
			var write = api.openTransactionAsync(Duration.ofSeconds(30));
			assertTrue(first.service.heldReads.await(5, TimeUnit.SECONDS));
			assertTrue(first.service.heldWrites.await(5, TimeUnit.SECONDS));

			process.kill();

			assertEquals(SECOND_NODE_COLUMN_ID, read.get(10, TimeUnit.SECONDS),
					"the read was not retried on the next node");
			var failure = assertThrows(ExecutionException.class, () -> write.get(10, TimeUnit.SECONDS));
			var status = assertInstanceOf(StatusRuntimeException.class, failure.getCause());
			assertEquals(Status.Code.UNAVAILABLE, status.getStatus().getCode());
			assertEquals(1, first.service.openTransactionCalls.get());
			assertEquals(0, second.service.openTransactionCalls.get(), "a write must not be replayed on another node");
		}
	}

	@Test
	void withoutRetryAReadInFlightFailsToo() throws Exception {
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(first);
		resources.add(second);
		first.service.holdCalls = true;
		try (var process = new NodeProxy(first.port())) {
			var client = GrpcConnection.forHostsAndPortsWithoutRetry("no-retry-client",
					List.of(new HostAndPort("127.0.0.1", process.port()), second.address()));
			resources.add(client);
			var api = client.getAsyncApi(CONTEXT);

			var read = api.getColumnIdAsync("held");
			assertTrue(first.service.heldReads.await(5, TimeUnit.SECONDS));
			process.kill();

			var failure = assertThrows(ExecutionException.class, () -> read.get(10, TimeUnit.SECONDS));
			var status = assertInstanceOf(StatusRuntimeException.class, failure.getCause());
			assertEquals(Status.Code.UNAVAILABLE, status.getStatus().getCode());
			assertEquals(second.port(), remotePort(client));
		}
	}

	@Test
	void aHostThatDoesNotResolveIsSkipped() throws Exception {
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		var client = connect(List.of(new HostAndPort("no-such-host.invalid", 1), node.address()));
		assertEquals(node.port(), remotePort(client));
	}

	@Test
	void noHostResolvingFailsTheConnection() {
		System.setProperty(HANDSHAKE_WAIT_PROPERTY, "0");
		var error = assertThrows(StatusRuntimeException.class, () -> connect(List.of(
				new HostAndPort("no-such-host.invalid", 1),
				new HostAndPort("no-such-other-host.invalid", 2))));
		assertEquals(Status.Code.UNAVAILABLE, error.getStatus().getCode());
	}

	/** A name can exist only once its node runs, so a list where no host resolves waits like any other. */
	@Test
	void noHostResolvingWaitsBeforeItFails() {
		System.setProperty(HANDSHAKE_WAIT_PROPERTY, "1500");
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		long start = System.nanoTime();
		var error = assertThrows(StatusRuntimeException.class, () -> connect(List.of(
				new HostAndPort("no-such-host.invalid", 1),
				new HostAndPort("no-such-other-host.invalid", 2))));
		long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		assertEquals(Status.Code.UNAVAILABLE, error.getStatus().getCode());
		assertTrue(millis >= 1_500, () -> millis + " ms");
	}

	/** Waits for the JVM's cache of failed lookups: see {@link FakeHosts}. */
	@Test
	void aNameThatStartsToResolveDuringTheWaitIsUsed() throws Exception {
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		FakeHosts.fail("late-name-1.test", Duration.ZERO);
		FakeHosts.fail("late-name-2.test", Duration.ZERO);
		var connecting = CompletableFuture.supplyAsync(() -> connect(List.of(
				new HostAndPort("late-name-1.test", node.port()),
				new HostAndPort("late-name-2.test", node.port()))));
		Thread.sleep(1_000);
		assertFalse(connecting.isDone());
		FakeHosts.serve("late-name-1.test");
		assertEquals(node.port(), remotePort(connecting.get(25, TimeUnit.SECONDS)));
	}

	/** A resolver outage while the connection moves: the new connection is kept, on the last addresses known. */
	@Test
	void aResolverOutageDoesNotDropTheNodeTheConnectionMovedTo() throws Exception {
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(first);
		resources.add(second);
		var client = connect(List.of(fakeHost("outage-1.test", first), fakeHost("outage-2.test", second)));
		assertEquals(first.port(), remotePort(client));

		FakeHosts.fail("outage-1.test", SLOW_FAILURE);
		FakeHosts.fail("outage-2.test", SLOW_FAILURE);
		first.stop();

		assertEquals(second.port(), awaitServedBy(client, second));
		assertStaysOn(client, second);
	}

	/** One name failing is not that host leaving the cluster: the connection to it stays. */
	@Test
	void aHostWhoseLookupFailsKeepsItsConnection() throws Exception {
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		var third = new FakeNode(3);
		resources.add(first);
		resources.add(second);
		resources.add(third);
		var client = connect(List.of(fakeHost("partial-1.test", first), fakeHost("partial-2.test", second),
				fakeHost("partial-3.test", third)));
		assertEquals(first.port(), remotePort(client));

		FakeHosts.fail("partial-2.test", SLOW_FAILURE);
		first.stop();

		assertEquals(second.port(), awaitServedBy(client, second));
		assertStaysOn(client, second);
	}

	/** A lookup that hangs does not keep the channel from the others, and its host joins when it answers. */
	@Test
	void aHungNameDoesNotHoldBackTheOtherHosts() throws Exception {
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(first);
		resources.add(second);
		var hung = FakeHosts.hang("hung-1.test");
		resources.add(hung::countDown);
		FakeHosts.serve("hung-2.test");

		long startedAt = System.nanoTime();
		var client = connect(List.of(new HostAndPort("hung-1.test", first.port()),
				new HostAndPort("hung-2.test", second.port())));
		long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
		assertEquals(second.port(), remotePort(client));
		assertTrue(elapsedMillis < 5_000, () -> "the connection took " + elapsedMillis + " ms");

		// From now on the name only fails: the first host can only come from the lookup that was hanging.
		FakeHosts.fail("hung-1.test", Duration.ZERO);
		hung.countDown();
		Thread.sleep(500);
		second.stop();
		assertEquals(first.port(), awaitServedBy(client, first));
	}

	/** A channel gets a new resolver when it leaves idle mode: the last addresses are still there. */
	@Test
	void aChannelLeavingIdleModeDuringAnOutageKeepsItsAddresses() throws Exception {
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		var other = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(node);
		resources.add(other);
		var client = connect(List.of(fakeHost("idle-1.test", node), fakeHost("idle-2.test", other)));
		assertEquals(node.port(), remotePort(client));

		FakeHosts.fail("idle-1.test", Duration.ZERO);
		FakeHosts.fail("idle-2.test", Duration.ZERO);
		channel(client).enterIdle();

		assertEquals(node.port(), remotePort(client));
	}

	/** With every address failing, attempts follow the configured delay instead of gRPC's growing one. */
	@Test
	void reconnectingDoesNotWaitForTheGrowingBackoff() throws Exception {
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		System.setProperty(MAX_RECONNECT_DELAY_PROPERTY, "200");
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		assertReconnectsWithoutTheGrowingBackoff(node,
				List.of(node.address(), new HostAndPort("127.0.0.1", unusedPort())));
	}

	/** A client with one address, whose node stopped listening for a while, comes back as fast. */
	@Test
	void reconnectingOnOneAddressDoesNotWaitForTheGrowingBackoff() throws Exception {
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		System.setProperty(MAX_RECONNECT_DELAY_PROPERTY, "200");
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		assertReconnectsWithoutTheGrowingBackoff(node, List.of(node.address()));
	}

	private void assertReconnectsWithoutTheGrowingBackoff(FakeNode node, List<HostAndPort> addresses) throws Exception {
		var client = GrpcConnection.forHostsAndPorts("reconnect-client", addresses);
		try {
			assertEquals(node.port(), remotePort(client));
			node.stopNow();
			try (var attempts = new ConnectionCounter(node.port())) {
				failUntilReconnectTimer(client);
				Thread.sleep(3_000);
				// gRPC alone would try 3 times in this time: at about 1 s, 2.6 s and 5.2 s.
				int counted = attempts.count();
				assertTrue(counted >= 6, () -> counted + " connection attempts");

				client.close();
				Thread.sleep(100);
				int atClose = attempts.count();
				Thread.sleep(500);
				assertEquals(atClose, attempts.count(), "a closed connection kept reconnecting");
			}
		} finally {
			client.close();
		}
	}

	/** The timer runs only in {@code TRANSIENT_FAILURE}: a healthy or idle channel has nothing scheduled. */
	@Test
	void onlyAFailingChannelHasAReconnectTimer() throws Exception {
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		System.setProperty(MAX_RECONNECT_DELAY_PROPERTY, "200");
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		var client = GrpcConnection.forHostAndPort("timer-client", node.address());
		try {
			assertEquals(node.port(), remotePort(client));
			assertFalse(hasReconnectTimer(client));

			node.stopNow();
			failUntilReconnectTimer(client);

			node.restart();
			assertEquals(node.port(), awaitServedBy(client, node));
			awaitReconnectTimer(client, false);

			node.stopNow();
			failUntilReconnectTimer(client);
			channel(client).enterIdle();
			awaitReconnectTimer(client, false);
			// gRPC keeps the subchannel it let go for 5 s more, and that one can try once on its own backoff.
			Thread.sleep(5_500);
			try (var attempts = new ConnectionCounter(node.port())) {
				Thread.sleep(1_000);
				assertEquals(0, attempts.count(), "an idle channel kept reconnecting");
			}

			failUntilReconnectTimer(client);
		} finally {
			client.close();
		}
		assertFalse(hasReconnectTimer(client), "a closed connection kept its timer");
	}

	/** A paused server stops listening and its clients move, while what it was running ends normally. */
	@Test
	void aPausedServerMovesItsClientsAndEndsItsCalls() throws Exception {
		var backend = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(backend);
		resources.add(second);
		var proxy = proxyOf(backend);
		var client = connect(List.of(new HostAndPort("127.0.0.1", proxy.getPort()), second.address()));
		var api = client.getAsyncApi(CONTEXT);
		assertEquals(proxy.getPort(), remotePort(client));

		backend.service.holdCalls = true;
		var held = api.getColumnIdAsync("x");
		assertTrue(backend.service.heldReads.await(5, TimeUnit.SECONDS));

		proxy.pause();

		assertEquals(second.port(), awaitServedBy(client, second.port()));
		assertFalse(held.isDone(), "the running call ended with the pause");
		backend.service.releaseHeldReads();
		assertEquals(FIRST_NODE_COLUMN_ID, held.get(5, TimeUnit.SECONDS));
		assertThrows(IOException.class, () -> new Socket("127.0.0.1", proxy.getPort()).close());

		// Pausing twice changes nothing, and it serves again on the same port.
		proxy.pause();
		proxy.resume();
		proxy.resume();
		var late = connect(List.of(new HostAndPort("127.0.0.1", proxy.getPort()), second.address()));
		assertEquals(proxy.getPort(), remotePort(late));
		proxy.pause();
		proxy.resume();
		assertEquals(proxy.getPort(), awaitServedBy(late, proxy.getPort()));
	}

	/** With a maximum age, connections end, so a client that moved away returns to the first address of its list. */
	@Test
	void aClientReturnsToTheFirstAddressWhenItsConnectionGetsOld() throws Exception {
		System.setProperty(MAX_CONNECTION_AGE_PROPERTY, "1000");
		System.setProperty(MAX_CONNECTION_AGE_GRACE_PROPERTY, "1000");
		var firstBackend = new FakeNode(FIRST_NODE_COLUMN_ID);
		var secondBackend = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(firstBackend);
		resources.add(secondBackend);
		var first = proxyOf(firstBackend);
		var second = proxyOf(secondBackend);
		var client = connect(List.of(new HostAndPort("127.0.0.1", first.getPort()),
				new HostAndPort("127.0.0.1", second.getPort())));
		first.pause();
		assertEquals(second.getPort(), awaitServedBy(client, second.getPort()));

		first.resume();

		assertEquals(first.getPort(), awaitServedBy(client, first.getPort()));
	}

	@Test
	void connectionsDoNotEndWithAgeByDefault() throws Exception {
		assertEquals(Duration.ZERO, GrpcServer.configuredMaxConnectionAge());
		assertAClientStaysAfterItMoved();
	}

	@Test
	void aMaximumAgeOfZeroIsOff() throws Exception {
		System.setProperty(MAX_CONNECTION_AGE_PROPERTY, "0");
		assertEquals(Duration.ZERO, GrpcServer.configuredMaxConnectionAge());
		assertAClientStaysAfterItMoved();
	}

	private void assertAClientStaysAfterItMoved() throws Exception {
		var firstBackend = new FakeNode(FIRST_NODE_COLUMN_ID);
		var secondBackend = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(firstBackend);
		resources.add(secondBackend);
		var first = proxyOf(firstBackend);
		var second = proxyOf(secondBackend);
		var client = connect(List.of(new HostAndPort("127.0.0.1", first.getPort()),
				new HostAndPort("127.0.0.1", second.getPort())));
		first.pause();
		assertEquals(second.getPort(), awaitServedBy(client, second.getPort()));

		first.resume();

		long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
		while (System.nanoTime() < end) {
			assertEquals(second.getPort(), remotePort(client));
			Thread.sleep(20);
		}
	}

	@Test
	void aServerOverAnEmbeddedDatabaseDoesNotPause() throws Exception {
		var node = startNode("embedded");
		assertThrows(IllegalStateException.class, node.server::pause);
		assertThrows(IllegalStateException.class, node.server::resume);
	}

	/** With every node down calls fail fast with UNAVAILABLE, and the same connection recovers when one returns. */
	@Test
	void everyNodeDownIsUnavailableUntilOneReturns() throws Exception {
		System.setProperty(MAX_ATTEMPTS_PROPERTY, "1");
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(first);
		resources.add(second);
		var client = connect(List.of(first.address(), second.address()));
		var api = client.getAsyncApi(CONTEXT);
		assertEquals(FIRST_NODE_COLUMN_ID, api.getColumnIdAsync("x").get(10, TimeUnit.SECONDS));

		first.stopNow();
		second.stopNow();

		var failure = assertThrows(ExecutionException.class,
				() -> awaitFailure(() -> api.getColumnIdAsync("x")).get(10, TimeUnit.SECONDS));
		var status = assertInstanceOf(StatusRuntimeException.class, failure.getCause());
		assertEquals(Status.Code.UNAVAILABLE, status.getStatus().getCode());

		second.restart();
		assertEquals(second.port(), awaitServedBy(client, second));
	}

	/** A host that is down gives no answer at all: the connection attempt ends at the connect timeout. */
	@Test
	void aSilentFirstAddressCostsOneConnectTimeout() throws Exception {
		int connectTimeoutMillis = 500;
		System.setProperty(CONNECT_TIMEOUT_PROPERTY, Integer.toString(connectTimeoutMillis));
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		try (var silent = new SilentListener()) {
			long startedAt = System.nanoTime();
			var client = connect(List.of(new HostAndPort("127.0.0.1", silent.port()), node.address()));
			long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

			assertEquals(node.port(), remotePort(client));
			assertTrue(elapsedMillis >= connectTimeoutMillis, () -> "connected too soon: " + elapsedMillis + " ms");
			assertTrue(elapsedMillis < 5_000, () -> "the connection took " + elapsedMillis + " ms");
		}
	}

	/** A frozen process: the kernel accepts the connection, nothing answers on it. */
	@Test
	void aFrozenFirstAddressCostsOneConnectTimeout() throws Exception {
		int connectTimeoutMillis = 500;
		System.setProperty(CONNECT_TIMEOUT_PROPERTY, Integer.toString(connectTimeoutMillis));
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(first);
		resources.add(second);
		try (var process = new NodeProxy(first.port())) {
			process.freeze();
			long startedAt = System.nanoTime();
			var client = connect(List.of(new HostAndPort("127.0.0.1", process.port()), second.address()));
			long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

			assertEquals(second.port(), remotePort(client));
			assertTrue(elapsedMillis >= connectTimeoutMillis, () -> "connected too soon: " + elapsedMillis + " ms");
			assertTrue(elapsedMillis < 5_000, () -> "the connection took " + elapsedMillis + " ms");
		}
	}

	/**
	 * A node that stops answering without closing anything: keepalive ends the connection, and new calls go to the
	 * next address, past the frozen one, which accepts the new connection. Calls have a deadline of their own.
	 */
	@Test
	void aNodeThatFallsSilentIsLeftByKeepalive() throws Exception {
		System.setProperty(CONNECT_TIMEOUT_PROPERTY, "500");
		System.setProperty(KEEP_ALIVE_TIMEOUT_PROPERTY, "1000");
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(first);
		resources.add(second);
		try (var proxy = new NodeProxy(first.port())) {
			var client = connect(List.of(new HostAndPort("127.0.0.1", proxy.port()), second.address()));
			var api = client.getAsyncApi(RequestContext.latency(Duration.ofSeconds(1)));
			assertEquals(FIRST_NODE_COLUMN_ID, api.getColumnIdAsync("x").get(10, TimeUnit.SECONDS));

			proxy.freeze();
			long frozenAt = System.nanoTime();

			var served = new AtomicReference<Long>();
			while (System.nanoTime() - frozenAt < TimeUnit.SECONDS.toNanos(25)) {
				try {
					served.set(api.getColumnIdAsync("x").get(10, TimeUnit.SECONDS));
					break;
				} catch (ExecutionException e) {
					// The call's deadline, while keepalive has not ended the connection yet.
					Thread.sleep(100);
				}
			}
			assertEquals(SECOND_NODE_COLUMN_ID, served.get());
			long switchSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - frozenAt);
			assertTrue(switchSeconds <= 15, () -> "keepalive took " + switchSeconds + " s");
		}
	}

	@Test
	void clientBuilderBuildsAConnectionToSeveralAddresses() throws Exception {
		var first = new FakeNode(FIRST_NODE_COLUMN_ID);
		var second = new FakeNode(SECOND_NODE_COLUMN_ID);
		resources.add(first);
		resources.add(second);
		var builder = new ClientBuilder();
		builder.setName("builder-failover");
		builder.setHttpAddresses(List.of(first.address(), second.address()));
		try (var connection = builder.build()) {
			assertEquals("http://127.0.0.1:" + first.port() + ",127.0.0.1:" + second.port(),
					connection.getUrl().toString());
			assertEquals(FIRST_NODE_COLUMN_ID,
					connection.getAsyncApi(CONTEXT).getColumnIdAsync("x").get(10, TimeUnit.SECONDS));
			first.stop();
			var api = connection.getAsyncApi(CONTEXT);
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (true) {
				try {
					assertEquals(SECOND_NODE_COLUMN_ID, api.getColumnIdAsync("x").get(10, TimeUnit.SECONDS));
					break;
				} catch (ExecutionException e) {
					assertTrue(System.nanoTime() < deadline, () -> "no switch: " + e.getCause());
				}
			}
		}
	}

	@Test
	void aDeferredConnectionReportsItsAddressesBeforeConnecting() throws Exception {
		var addresses = List.of(new HostAndPort("::1", 7000), new HostAndPort("node-2", 7001));
		var builder = new ClientBuilder();
		builder.setName("deferred-failover");
		builder.setDeferred(true);
		builder.setHttpAddresses(addresses);
		try (var connection = builder.build()) {
			assertEquals(addresses, Utils.parseHostAndPorts(connection.getUrl()));
		}
	}

	@Test
	void anEmptyAddressListIsRefused() {
		assertThrows(IllegalArgumentException.class, () -> GrpcConnection.forHostsAndPorts("none", List.of()));
		assertThrows(IllegalArgumentException.class, () -> new ClientBuilder().setHttpAddresses(List.of()));
	}

	@Test
	void oneAddressIsAPlainConnection() throws Exception {
		var node = new FakeNode(FIRST_NODE_COLUMN_ID);
		resources.add(node);
		var client = connect(List.of(node.address()));
		assertEquals("http://127.0.0.1:" + node.port(), client.getUrl().toString());
		assertEquals(node.port(), remotePort(client));
	}

	// Harness

	private GrpcConnection connect(List<HostAndPort> addresses) {
		var client = GrpcConnection.forHostsAndPorts("failover-client", addresses);
		resources.add(client);
		return client;
	}

	private static HostAndPort fakeHost(String name, FakeNode node) {
		FakeHosts.serve(name);
		return new HostAndPort(name, node.port());
	}

	/** A server in front of another connection, as a replicated node is. */
	private GrpcServer proxyOf(FakeNode backend) throws IOException {
		var connection = GrpcConnection.forHostAndPort("proxy-client", backend.address());
		resources.add(connection);
		var server = new GrpcServer(connection, new InetSocketAddress("127.0.0.1", 0));
		resources.add(server);
		server.start();
		return server;
	}

	private EmbeddedNode startNode(String name) throws Exception {
		Path config = Files.createTempFile(tempDir, name, ".conf");
		Files.writeString(config, "database: { global: { ingest-behind: false, optimistic: false } }");
		var node = new EmbeddedNode(new EmbeddedConnection(null, "failover-" + name, config));
		resources.add(node);
		return node;
	}

	private static Keys key(long key) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(key).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(new byte[] {(byte) value, 1, 2, 3});
	}

	/** The threads of the event loop groups of gRPC clients and servers: a closed channel leaves none. */
	private static Set<Thread> eventLoopThreads() {
		return Thread.getAllStackTraces().keySet().stream()
				.filter(thread -> thread.getName().startsWith("nioEventLoopGroup")
						|| thread.getName().startsWith("rockserver-grpc-server-loop"))
				.collect(Collectors.toSet());
	}

	private static void awaitEventLoopsGone(Set<Thread> before) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!before.containsAll(eventLoopThreads())) {
			assertTrue(System.nanoTime() - deadline < 0, "a channel still runs");
			Thread.sleep(20);
		}
	}

	private static int unusedPort() throws IOException {
		try (var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
	}

	/** Calls {@code call} until it fails, which it does once the failure of the node reached the channel. */
	private static CompletableFuture<Long> awaitFailure(Supplier<CompletableFuture<Long>> call)
			throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (System.nanoTime() < deadline) {
			var attempt = call.get();
			try {
				attempt.get(10, TimeUnit.SECONDS);
			} catch (ExecutionException e) {
				return attempt;
			}
			Thread.sleep(10);
		}
		throw new AssertionError("the call never failed");
	}

	/** Waits until a call reaches {@code expected}; the calls before it may see the loss of the previous node. */
	private static int awaitServedBy(GrpcConnection client, FakeNode expected) throws Exception {
		return awaitServedBy(client, expected.port());
	}

	private static int awaitServedBy(GrpcConnection client, int expectedPort) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (true) {
			try {
				int port = remotePort(client);
				if (port == expectedPort || System.nanoTime() > deadline) {
					return port;
				}
			} catch (StatusRuntimeException e) {
				if (System.nanoTime() > deadline) {
					throw e;
				}
			}
			Thread.sleep(10);
		}
	}

	/** Every call for a while, long after the lookups of the move ended, stays on {@code node} and succeeds. */
	private static void assertStaysOn(GrpcConnection client, FakeNode node) throws Exception {
		long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		while (System.nanoTime() < end) {
			assertEquals(node.port(), remotePort(client));
			Thread.sleep(20);
		}
	}

	private static boolean hasReconnectTimer(GrpcConnection client) throws Exception {
		var delegateField = GrpcConnection.class.getDeclaredField("delegate");
		delegateField.setAccessible(true);
		var delegate = delegateField.get(client);
		var timerField = delegate.getClass().getDeclaredField("reconnectTimer");
		timerField.setAccessible(true);
		var reconnectTimer = timerField.get(delegate);
		var scheduled = reconnectTimer.getClass().getDeclaredField("timer");
		scheduled.setAccessible(true);
		synchronized (reconnectTimer) {
			return scheduled.get(reconnectTimer) != null;
		}
	}

	/**
	 * Only a failed attempt to connect starts the timer. A call can fail on the connection that is going away instead,
	 * and the channel then waits idle for the next call.
	 */
	private static void failUntilReconnectTimer(GrpcConnection client) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		do {
			assertThrows(StatusRuntimeException.class, () -> remotePort(client));
			assertTrue(System.nanoTime() < deadline, "the reconnect timer never started");
			Thread.sleep(10);
		} while (!hasReconnectTimer(client));
	}

	private static void awaitReconnectTimer(GrpcConnection client, boolean expected) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (hasReconnectTimer(client) != expected) {
			assertTrue(System.nanoTime() < deadline, () -> "the reconnect timer never became " + expected);
			Thread.sleep(10);
		}
	}

	private static ManagedChannel channel(GrpcConnection client) throws Exception {
		var field = GrpcConnection.class.getDeclaredField("channel");
		field.setAccessible(true);
		return (ManagedChannel) field.get(client);
	}

	/** The port of the server that answers a call on this connection's own channel. */
	private static int remotePort(GrpcConnection client) throws Exception {
		var channel = channel(client);
		var remote = new AtomicReference<SocketAddress>();
		var observer = new ClientInterceptor() {
			@Override
			public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
					CallOptions callOptions, Channel next) {
				var call = next.newCall(method, callOptions);
				return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
					@Override
					public void start(Listener<RespT> listener, Metadata headers) {
						super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
							@Override
							public void onHeaders(Metadata responseHeaders) {
								remote.set(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
								super.onHeaders(responseHeaders);
							}
						}, headers);
					}
				};
			}
		};
		RocksDBServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel, observer))
				.withDeadlineAfter(10, TimeUnit.SECONDS)
				.getCapabilities(CapabilitiesRequest.getDefaultInstance());
		assertNotNull(remote.get(), "the call had no transport address");
		return ((InetSocketAddress) remote.get()).getPort();
	}

	private static final long FIRST_NODE_COLUMN_ID = 1;
	private static final long SECOND_NODE_COLUMN_ID = 2;

	/** A real server over its own embedded database. */
	private static final class EmbeddedNode implements AutoCloseable {

		private final EmbeddedConnection db;
		private final GrpcServer server;
		private final long column;
		private boolean serverStopped;

		EmbeddedNode(EmbeddedConnection db) throws IOException {
			this.db = db;
			this.server = new GrpcServer(db, new InetSocketAddress("127.0.0.1", 0));
			this.server.start();
			this.column = db.getSyncApi(RequestContext.batch()).createColumn("data", SCHEMA);
		}

		int port() {
			return server.getPort();
		}

		HostAndPort address() {
			return new HostAndPort("127.0.0.1", port());
		}

		byte @Nullable [] read(long key) {
			var value = db.getSyncApi(RequestContext.batch()).get(0, column, key(key), RequestType.current());
			return value != null ? value.toByteArray() : null;
		}

		void stopServer() throws IOException {
			if (!serverStopped) {
				serverStopped = true;
				server.close();
			}
		}

		@Override
		public void close() throws IOException {
			stopServer();
			db.closeTesting();
		}
	}

	/** A gRPC server with a scripted service, restartable on its port. */
	private static final class FakeNode implements AutoCloseable {

		private final ScriptedService service;
		private final int port;
		private Server server;

		FakeNode(long columnId) throws IOException {
			service = new ScriptedService(columnId);
			server = serverOn(0).start();
			port = server.getPort();
		}

		private Server serverOn(int port) {
			return NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
					.addService(service)
					.permitKeepAliveTime(5, TimeUnit.SECONDS)
					.permitKeepAliveWithoutCalls(true)
					.build();
		}

		int port() {
			return port;
		}

		HostAndPort address() {
			return new HostAndPort("127.0.0.1", port);
		}

		/** A graceful stop: the client is told. */
		void stop() throws InterruptedException {
			server.shutdown();
			assertTrue(server.awaitTermination(10, TimeUnit.SECONDS));
		}

		/** Ends what runs at once, but still tells the client: {@link NodeProxy#kill()} stands for a crash. */
		void stopNow() throws InterruptedException {
			server.shutdownNow();
			assertTrue(server.awaitTermination(10, TimeUnit.SECONDS));
		}

		void restart() throws IOException {
			server = serverOn(port).start();
		}

		@Override
		public void close() throws InterruptedException {
			stopNow();
		}
	}

	private static final class ScriptedService extends RocksDBServiceGrpc.RocksDBServiceImplBase {

		private final long columnId;
		private final AtomicInteger openTransactionCalls = new AtomicInteger();
		private final AtomicInteger capabilitiesCalls = new AtomicInteger();
		private final CountDownLatch heldReads = new CountDownLatch(1);
		private final CountDownLatch heldWrites = new CountDownLatch(1);
		private final List<StreamObserver<GetColumnIdResponse>> heldReadObservers = new CopyOnWriteArrayList<>();
		private volatile boolean holdCalls;
		private volatile Status capabilitiesFailure;
		private volatile int workloadContractVersion = RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION;

		ScriptedService(long columnId) {
			this.columnId = columnId;
		}

		@Override
		public void getCapabilities(CapabilitiesRequest request,
				StreamObserver<CapabilitiesResponse> responseObserver) {
			capabilitiesCalls.incrementAndGet();
			if (capabilitiesFailure != null) {
				responseObserver.onError(capabilitiesFailure.asRuntimeException());
				return;
			}
			responseObserver.onNext(CapabilitiesResponse.newBuilder()
					.setWorkloadContractVersion(workloadContractVersion)
					.build());
			responseObserver.onCompleted();
		}

		@Override
		public void getColumnId(GetColumnIdRequest request, StreamObserver<GetColumnIdResponse> responseObserver) {
			if (holdCalls) {
				heldReadObservers.add(responseObserver);
				heldReads.countDown();
				return;
			}
			responseObserver.onNext(GetColumnIdResponse.newBuilder().setColumnId(columnId).build());
			responseObserver.onCompleted();
		}

		void releaseHeldReads() {
			holdCalls = false;
			for (var observer : heldReadObservers) {
				observer.onNext(GetColumnIdResponse.newBuilder().setColumnId(columnId).build());
				observer.onCompleted();
			}
		}

		@Override
		public void openTransaction(OpenTransactionRequest request,
				StreamObserver<OpenTransactionResponse> responseObserver) {
			openTransactionCalls.incrementAndGet();
			if (holdCalls) {
				heldWrites.countDown();
				return;
			}
			responseObserver.onNext(OpenTransactionResponse.getDefaultInstance());
			responseObserver.onCompleted();
		}
	}

	/**
	 * A port that never answers a connection attempt, as a host that is down: the accept queue of a listening socket
	 * that nobody accepts from is full, so the kernel ignores further SYNs.
	 */
	private static final class SilentListener implements AutoCloseable {

		private final ServerSocket listener;
		private final List<Socket> queueFillers = new ArrayList<>();

		SilentListener() throws IOException {
			listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
			for (int i = 0; i < 3; i++) {
				var filler = new Socket();
				try {
					filler.connect(listener.getLocalSocketAddress(), 200);
				} catch (SocketTimeoutException full) {
					filler.close();
					break;
				}
				queueFillers.add(filler);
			}
		}

		int port() {
			return listener.getLocalPort();
		}

		@Override
		public void close() throws IOException {
			for (var filler : queueFillers) {
				filler.close();
			}
			listener.close();
		}
	}

	/** Counts the connection attempts on a port, and closes each. */
	private static final class ConnectionCounter implements AutoCloseable {

		private final ServerSocket listener;
		private final AtomicInteger count = new AtomicInteger();

		ConnectionCounter(int port) throws IOException {
			listener = new ServerSocket(port, 16, InetAddress.getByName("127.0.0.1"));
			Thread.ofVirtual().start(() -> {
				try {
					while (true) {
						listener.accept().close();
						count.incrementAndGet();
					}
				} catch (IOException closed) {
					// Closed by the test.
				}
			});
		}

		int port() {
			return listener.getLocalPort();
		}

		int count() {
			return count.get();
		}

		@Override
		public void close() throws IOException {
			listener.close();
		}
	}

	/**
	 * A TCP proxy in front of a server, standing for its process: it can freeze (it stops copying, and keeps accepting
	 * connections and every connection open, as the kernel does for a stopped process) or die (every connection is
	 * reset and the port refuses new ones, as after SIGKILL).
	 */
	private static final class NodeProxy implements AutoCloseable {

		private final ServerSocket listener;
		private final int target;
		private final List<Socket> sockets = new CopyOnWriteArrayList<>();
		private final Thread acceptor;
		private volatile boolean frozen;

		NodeProxy(int target) throws IOException {
			this.target = target;
			this.listener = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
			this.acceptor = Thread.ofVirtual().start(this::acceptLoop);
		}

		int port() {
			return listener.getLocalPort();
		}

		void freeze() {
			frozen = true;
		}

		void kill() throws IOException, InterruptedException {
			frozen = false;
			listener.close();
			acceptor.join();
			for (var socket : sockets) {
				try {
					socket.setSoLinger(true, 0);
					socket.close();
				} catch (SocketException alreadyClosed) {
					// Its other direction closed it.
				}
			}
		}

		private void acceptLoop() {
			try {
				while (true) {
					var client = listener.accept();
					var upstream = new Socket(InetAddress.getLoopbackAddress(), target);
					sockets.add(client);
					sockets.add(upstream);
					Thread.ofVirtual().start(() -> copy(client, upstream));
					Thread.ofVirtual().start(() -> copy(upstream, client));
				}
			} catch (IOException closed) {
				// Closed by the test.
			}
		}

		private void copy(Socket from, Socket to) {
			var buffer = new byte[8192];
			try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
				int read;
				while ((read = in.read(buffer)) >= 0) {
					while (frozen) {
						Thread.sleep(50);
					}
					out.write(buffer, 0, read);
				}
			} catch (IOException closed) {
				// Closed by the test.
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
			}
		}

		@Override
		public void close() throws IOException, InterruptedException {
			kill();
		}
	}
}
