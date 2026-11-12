package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import io.grpc.StatusRuntimeException;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/**
 * One {@link GrpcConnection} created with the addresses of all three nodes, while the node it uses is killed
 * (SIGKILL): the client only sees errors it may retry, and keeps working on another node without a new connection.
 */
@Timeout(180)
@Tag("slow")
class ClientFailoverTest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.latency(Duration.ofSeconds(8));
	private static final RequestContext SETUP = RequestContext.batch(Duration.ofSeconds(30));
	/** Range streams are not latency work. */
	private static final RequestContext STREAM = RequestContext.analytical(Duration.ofSeconds(8));
	private static final int KEYS = 64;
	private static final List<String> MEMBERS = List.of("1", "2", "3");
	/** How long after the kill calls of every kind must be succeeding, far more than it takes on a quiet machine. */
	private static final Duration RESUME_BOUND = Duration.ofSeconds(20);
	/** How long calls keep running once every kind resumed. */
	private static final Duration AFTER_RESUME = Duration.ofSeconds(1);

	@TempDir
	Path root;
	private ProcessCluster cluster;

	@AfterEach
	void tearDown() {
		if (cluster != null) {
			cluster.close();
		}
	}

	/** The expected deployment: the leader is listed first, and it dies, so the client also waits for an election. */
	@Test
	void aClientThatListsTheLeaderFirstSurvivesItsDeath() throws Exception {
		startCluster();
		var leader = awaitLeader(cluster);
		runKilling(leader);
	}

	/** No election here: only the client moves. A connection made after the kill skips the dead node at once. */
	@Test
	void aClientThatListsAFollowerFirstSurvivesItsDeath() throws Exception {
		startCluster();
		var leader = awaitLeader(cluster);
		var follower = MEMBERS.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
		var addresses = runKilling(follower);

		try (var late = GrpcConnection.forHostsAndPorts("late-client", addresses)) {
			var api = late.getAsyncApi(SETUP);
			long column = api.getColumnIdAsync("data").join();
			assertNotEquals(cluster.apiAddress(follower).port(), remotePort(late));
			api.putAsync(0, column, key(0), Buf.wrap(new byte[] {3}), RequestType.none()).join();
		}
	}

	/** A transaction lives on the leader: losing the follower that carried its calls does not lose it. */
	@Test
	void aTransactionSurvivesTheLossOfTheFollowerItWentThrough() throws Exception {
		startCluster();
		var leader = awaitLeader(cluster);
		var order = new ArrayList<>(MEMBERS.stream().filter(id -> !id.equals(leader)).toList());
		order.add(leader);
		var follower = order.getFirst();

		try (var connection = GrpcConnection.forHostsAndPorts("transaction-client",
				order.stream().map(cluster::apiAddress).toList())) {
			var api = connection.getAsyncApi(SETUP);
			long column = api.createColumnAsync("data", SCHEMA).join();
			long transaction = api.openTransactionAsync(Duration.ofSeconds(60)).join();
			api.putAsync(transaction, column, key(1), Buf.wrap(new byte[] {7}), RequestType.none()).join();
			assertEquals(cluster.apiAddress(follower).port(), remotePort(connection));

			cluster.kill(follower);
			// The commit is not retried: send it once the channel knows, on a call that may be retried.
			awaitServedByAnother(connection, cluster.apiAddress(follower).port());

			assertTrue(api.closeTransactionAsync(transaction, true).join(), "the commit failed");
			assertNotEquals(cluster.apiAddress(follower).port(), remotePort(connection));
			var value = api.getAsync(0, column, key(1), RequestType.current()).join();
			assertNotNull(value, "the committed value is missing");
			assertEquals(Buf.wrap(new byte[] {7}), value);
		}
	}

	private void startCluster() throws Exception {
		cluster = ProcessCluster.start(root.resolve("cluster"), 3, List.of(
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000"));
		cluster.waitReady(READY_TIMEOUT);
	}

	/** Runs a mixed load through a client that lists {@code victim} first, and kills it. Returns the list. */
	private List<HostAndPort> runKilling(String victim) throws Exception {
		var order = new ArrayList<>(List.of(victim));
		MEMBERS.stream().filter(id -> !id.equals(victim)).forEach(order::add);
		var addresses = order.stream().map(cluster::apiAddress).toList();
		int victimPort = cluster.apiAddress(victim).port();

		try (var connection = GrpcConnection.forHostsAndPorts("failover-client", addresses)) {
			var api = connection.getAsyncApi(CONTEXT);
			var streamApi = connection.getAsyncApi(STREAM);
			long column = connection.getAsyncApi(SETUP).createColumnAsync("data", SCHEMA).join();
			for (int key = 0; key < KEYS; key++) {
				api.putAsync(0, column, key(key), Buf.wrap(new byte[] {1}), RequestType.none()).join();
			}
			assertEquals(victimPort, remotePort(connection), "the first address is preferred");

			var stop = new AtomicBoolean();
			var unexpected = new ConcurrentHashMap<String, String>();
			var retryable = new AtomicLong();
			var killedAtNanos = new AtomicLong(Long.MAX_VALUE);
			var firstSuccessAfterKillNanos = new AtomicLong(Long.MAX_VALUE);
			var lastReadNanos = new AtomicLong();
			var lastRangeNanos = new AtomicLong();
			var lastWriteNanos = new AtomicLong();
			var workers = new ArrayList<Thread>();
			for (int i = 0; i < 8; i++) {
				boolean reader = i % 4 == 0;
				boolean rangeReader = i % 4 == 2;
				workers.add(Thread.ofVirtual().start(() -> {
					var random = ThreadLocalRandom.current();
					while (!stop.get()) {
						long started = System.nanoTime();
						try {
							if (rangeReader) {
								var keys = Flux.from(streamApi.getRangeAsync(0, column, null, null, false,
												RequestType.<KV>allInRange()))
										.map(kv -> ByteBuffer.wrap(kv.keys().keys()[0].toByteArray()).getLong())
										.collectList()
										.block();
								// Writes only overwrite the keys written before: a complete read returns each once.
								long distinct = keys.stream().distinct().count();
								if (keys.size() != KEYS || distinct != KEYS) {
									unexpected.putIfAbsent("range", "range returned " + keys.size() + " keys, " + distinct
											+ " distinct, of " + KEYS);
								}
								lastRangeNanos.set(started);
							} else if (reader) {
								api.getAsync(0, column, key(random.nextInt(KEYS)), RequestType.current()).join();
								lastReadNanos.set(started);
							} else {
								api.putAsync(0, column, key(random.nextInt(KEYS)), Buf.wrap(new byte[] {2}),
										RequestType.none()).join();
								lastWriteNanos.set(started);
							}
							// Sent after the kill: an answer to an earlier call says nothing about the switch.
							if (started > killedAtNanos.get()) {
								firstSuccessAfterKillNanos.accumulateAndGet(System.nanoTime(), Math::min);
							}
						} catch (RuntimeException e) {
							var cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
							if (ClusterClient.isRetryable(cause)) {
								retryable.incrementAndGet();
							} else {
								unexpected.putIfAbsent(String.valueOf(cause), String.valueOf(cause.getMessage()));
							}
						}
					}
				}));
			}

			Thread.sleep(1000);
			cluster.kill(victim);
			long killedAt = System.nanoTime();
			killedAtNanos.set(killedAt);
			// Calls of every kind, sent after the kill, on another node of the same connection.
			long deadline = killedAt + RESUME_BOUND.toNanos();
			while (System.nanoTime() - deadline < 0 && !(lastReadNanos.get() - killedAt > 0
					&& lastRangeNanos.get() - killedAt > 0 && lastWriteNanos.get() - killedAt > 0)) {
				Thread.sleep(10);
			}
			Thread.sleep(AFTER_RESUME);
			stop.set(true);
			for (var worker : workers) {
				worker.join();
			}

			System.out.println("ClientFailoverTest: retryable errors " + retryable.get() + ", first success "
					+ Duration.ofNanos(firstSuccessAfterKillNanos.get() - killedAt).toMillis() + " ms after the kill");
			assertTrue(unexpected.isEmpty(), () -> "non-retryable errors: " + unexpected);
			assertTrue(lastReadNanos.get() - killedAt > 0, "reads did not resume");
			assertTrue(lastRangeNanos.get() - killedAt > 0, "range reads did not resume");
			assertTrue(lastWriteNanos.get() - killedAt > 0, "writes did not resume");
			assertNotEquals(victimPort, remotePort(connection));
		}
		return addresses;
	}

	/** Waits for a leader: every node printed READY, but the first election may still be running. */
	static String awaitLeader(ProcessCluster cluster) throws Exception {
		return awaitLeader(cluster, MEMBERS);
	}

	/** Waits for a leader among {@code members}, the nodes that run. */
	static String awaitLeader(ProcessCluster cluster, List<String> members) throws Exception {
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (true) {
			var status = cluster.admin(members, "status");
			var leader = status.lines()
					.filter(line -> line.contains(": LEADER"))
					.map(line -> line.substring(0, line.indexOf(':')))
					.findFirst();
			if (leader.isPresent()) {
				return leader.get();
			}
			if (System.nanoTime() - deadline > 0) {
				throw new AssertionError("No leader in status:\n" + status);
			}
			Thread.sleep(100);
		}
	}

	/** Waits until another node than {@code port} answers a call that may be retried. */
	private static void awaitServedByAnother(GrpcConnection connection, int port) throws Exception {
		long deadline = System.nanoTime() + RESUME_BOUND.toNanos();
		while (true) {
			try {
				if (remotePort(connection) != port) {
					return;
				}
			} catch (StatusRuntimeException e) {
				if (System.nanoTime() - deadline > 0) {
					throw e;
				}
			}
			assertTrue(System.nanoTime() - deadline < 0, "the connection stayed on its dead node");
			Thread.sleep(10);
		}
	}

	/** The API port of the node that answers a call on this connection's own channel. */
	static int remotePort(GrpcConnection connection) throws Exception {
		var field = GrpcConnection.class.getDeclaredField("channel");
		field.setAccessible(true);
		var channel = (ManagedChannel) field.get(connection);
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

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
