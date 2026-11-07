package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The forwarder without a Raft server: leader lookups against a {@link StubRaft} division, and connections to a
 * minimal gRPC server that answers the capability handshake.
 */
class LeaderForwarderTest {

	@TempDir
	Path temp;
	private final List<AutoCloseable> closing = new ArrayList<>();

	@AfterEach
	void tearDown() throws Exception {
		for (var resource : closing.reversed()) {
			resource.close();
		}
	}

	@Test
	void closingFailsTheCallsStillWaitingForALeader() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp);
		var metrics = new NodeMetrics("n0");
		try {
			// Not started: no Raft division, so no leader is known.
			var stateMachine = new RockserverStateMachine(config, _ -> {}, metrics);
			var forwarder = new LeaderForwarder(config);
			var pending = forwarder.leader(stateMachine);
			assertFalse(pending.isDone());
			forwarder.close();
			var error = assertThrows(ExecutionException.class, () -> pending.get(1, TimeUnit.SECONDS));
			var cause = assertInstanceOf(RocksDBException.class, error.getCause());
			assertEquals(RocksDBErrorType.SERVER_OVERLOADED, cause.getErrorUniqueId());

			var late = forwarder.leader(stateMachine);
			assertThrows(ExecutionException.class, () -> late.get(1, TimeUnit.SECONDS), "no wait after close");
		} finally {
			metrics.close();
		}
	}

	// -------------------------------------------------------------------------------------------------------------
	// Leader lookup

	private record Setup(StubRaft raft, RockserverStateMachine stateMachine, LeaderForwarder forwarder) {}

	private Setup setup(List<PeerConfig> peers, Duration requestTimeout) throws Exception {
		var config = NodeConfig.defaults("n0", peers, temp).withRequestTimeout(requestTimeout);
		var metrics = new NodeMetrics("n0");
		closing.add(metrics::close);
		var stateMachine = new RockserverStateMachine(config, _ -> {}, metrics);
		var raft = new StubRaft();
		raft.installInto(stateMachine);
		var forwarder = new LeaderForwarder(config);
		closing.add(forwarder);
		return new Setup(raft, stateMachine, forwarder);
	}

	private static List<PeerConfig> peers(int apiPortOfN1) {
		return List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2),
				new PeerConfig("n1", "127.0.0.1", 3, "127.0.0.1", apiPortOfN1));
	}

	@Test
	void anotherNodeKnownAsLeaderIsAnsweredAtOnceAndThisNodeOnlyOnceItIsAReadyLeader() throws Exception {
		var s = setup(peers(4), Duration.ofSeconds(5));
		s.raft().leaderId = "n1";
		assertEquals("n1", s.forwarder().leader(s.stateMachine()).getNow(null));

		s.raft().leaderId = "n0";
		s.raft().leader = true;
		var waiting = s.forwarder().leader(s.stateMachine());
		assertFalse(waiting.isDone(), "this node leads but is not ready yet");
		s.raft().leaderReady = true;
		s.forwarder().leaderChanged();
		assertEquals("n0", waiting.get(5, TimeUnit.SECONDS));
	}

	/**
	 * A leader that becomes known while a request waits wakes it through {@link LeaderForwarder#leaderChanged()},
	 * well before the safety poll (250 ms) would.
	 */
	@Test
	void aLeaderChangeWakesTheWaitingRequestsAtOnce() throws Exception {
		var s = setup(peers(4), Duration.ofSeconds(5));
		for (int round = 0; round < 5; round++) {
			s.raft().leaderId = null;
			var waiting = List.of(s.forwarder().leader(s.stateMachine()), s.forwarder().leader(s.stateMachine()));
			waiting.forEach(future -> assertFalse(future.isDone()));
			s.raft().leaderId = "n1";
			long start = System.nanoTime();
			s.forwarder().leaderChanged();
			for (var future : waiting) {
				assertEquals("n1", future.get(5, TimeUnit.SECONDS));
			}
			long tookMillis = (System.nanoTime() - start) / 1_000_000;
			assertTrue(tookMillis < 200, "woken after " + tookMillis + " ms: by the poll, not by the change");
		}
	}

	/**
	 * The lost wake-up race: the leader becomes known right after the request's first check read the leader id, and
	 * Ratis notifies the change before the request registered for wake-ups, so the notification wakes nothing. The
	 * check made after the registration must see the leader: the request completes at once, without waiting for the
	 * safety poll.
	 */
	@Test
	void aLeaderThatAppearsBetweenTheFirstCheckAndTheRegistrationIsNotMissed() throws Exception {
		var s = setup(peers(4), Duration.ofSeconds(5));
		var reads = new AtomicInteger();
		s.raft().leaderId = null;
		s.raft().onLeaderIdRead = () -> {
			if (reads.incrementAndGet() == 1) {
				// The first read answers null; then the leader changes and Ratis notifies, nobody registered yet.
				s.raft().leaderId = "n1";
				s.forwarder().leaderChanged();
			}
		};
		var result = s.forwarder().leader(s.stateMachine());
		assertTrue(reads.get() >= 2, "the request did not check again after registering");
		assertTrue(result.isDone(), "the request waits although the leader is known");
		assertEquals("n1", result.get(1, TimeUnit.SECONDS));
	}

	@Test
	void noLeaderWithinTheRequestTimeoutFailsAsRetryableOverload() throws Exception {
		var s = setup(peers(4), Duration.ofMillis(400));
		var error = assertThrows(ExecutionException.class,
				() -> s.forwarder().leader(s.stateMachine()).get(5, TimeUnit.SECONDS));
		var cause = assertInstanceOf(RocksDBException.class, error.getCause());
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, cause.getErrorUniqueId());
	}

	// -------------------------------------------------------------------------------------------------------------
	// Connections

	/** A gRPC server that answers only the capability handshake, as a Rockserver would. */
	private int handshakeServer() throws Exception {
		return startHandshakeServer().getPort();
	}

	private Server startHandshakeServer() throws Exception {
		return startHandshakeServer(0);
	}

	private Server startHandshakeServer(int port) throws Exception {
		return startHandshakeServer(port, new java.util.concurrent.atomic.AtomicBoolean());
	}

	/** While {@code failing} is set, the handshake fails after 300 ms instead of answering. */
	private Server startHandshakeServer(int port, java.util.concurrent.atomic.AtomicBoolean failing)
			throws Exception {
		Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
				.addService(new RocksDBServiceGrpc.RocksDBServiceImplBase() {
					@Override
					public void getCapabilities(CapabilitiesRequest request,
							StreamObserver<CapabilitiesResponse> response) {
						if (failing.get()) {
							try {
								Thread.sleep(300);
							} catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							}
							response.onError(io.grpc.Status.UNAVAILABLE.withDescription("failing").asRuntimeException());
							return;
						}
						var current = RockserverCapabilities.CURRENT;
						response.onNext(CapabilitiesResponse.newBuilder()
								.setWorkloadContractVersion(current.workloadContractVersion())
								.setClientSessions(current.clientSessions())
								.setNumberedBatches(current.numberedBatches())
								.build());
						response.onCompleted();
					}
				})
				.build()
				.start();
		closing.add(() -> server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
		return server;
	}

	/** A Raft port that answers only {@link TermService#API_ADDRESS}, with whatever {@code address} holds then. */
	private int apiAddressServer(AtomicReference<String> address) throws Exception {
		return apiAddressServer(address, new AtomicInteger(), new java.util.concurrent.atomic.AtomicBoolean());
	}

	/** Counts the asks in {@code asks}; while {@code refuse} is set, each fails after 300 ms instead of answering. */
	private int apiAddressServer(AtomicReference<String> address, AtomicInteger asks,
			java.util.concurrent.atomic.AtomicBoolean refuse) throws Exception {
		var definition = org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition.builder(TermService.SERVICE_NAME)
				.addMethod(TermService.API_ADDRESS, org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls.asyncUnaryCall(
						(byte[] request, org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver<byte[]> response) -> {
							asks.incrementAndGet();
							if (refuse.get()) {
								try {
									Thread.sleep(300);
								} catch (InterruptedException e) {
									Thread.currentThread().interrupt();
								}
								response.onError(org.apache.ratis.thirdparty.io.grpc.Status.UNAVAILABLE.asRuntimeException());
								return;
							}
							response.onNext(address.get().getBytes(StandardCharsets.UTF_8));
							response.onCompleted();
						}))
				.build();
		var server = org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder
				.forAddress(new InetSocketAddress("127.0.0.1", 0))
				.addService(definition)
				.build()
				.start();
		closing.add(() -> server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
		return server.getPort();
	}

	/**
	 * A listed member that answered its API address once, then moved its API to another port (restarted with another
	 * one while this node kept running): once a connection to the old port fails, the next connection after the backoff
	 * asks the member again and reaches the new port, and leader hints name it. Before the fix the first answer was
	 * kept until this node restarted (R18-2).
	 */
	@Test
	void aListedMemberThatMovesItsApiPortIsAskedAgainOnceAConnectionFails() throws Exception {
		var first = startHandshakeServer();
		var answer = new AtomicReference<>("127.0.0.1:" + first.getPort());
		int raftPort = apiAddressServer(answer);
		// The peer list names a wrong API port: only the member's answer leads to it.
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2),
				new PeerConfig("n1", "127.0.0.1", raftPort, "127.0.0.1", closedPort()));
		var s = setup(peers, Duration.ofSeconds(5));
		var targets = new ArrayList<LeaderForwarder.Target>();
		for (int i = 0; i < 4; i++) {
			targets.add(s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS));
		}
		assertEquals(new HostAndPort("127.0.0.1", first.getPort()), s.forwarder().apiAddress("n1"));
		long version = s.forwarder().addressVersion();

		// The member moves its API port: the old one closes, the member answers the new one.
		first.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
		var second = startHandshakeServer();
		answer.set("127.0.0.1:" + second.getPort());
		// Each call on the old connections fails in transport.
		targets.forEach(s.forwarder()::invalidate);
		var failed = assertThrows(ExecutionException.class,
				() -> s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS));
		assertInstanceOf(LeaderForwarder.NotConnectedException.class, failed.getCause());

		Thread.sleep(250);
		var moved = s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS);
		assertEquals("n1", moved.peerId());
		assertEquals(new HostAndPort("127.0.0.1", second.getPort()), s.forwarder().apiAddress("n1"),
				"leader hints still name the old port");
		assertTrue(s.forwarder().addressVersion() != version, "cached leader hints are not looked up again");
	}

	/**
	 * A listed member whose API stops answering for a moment at the same port (a pause, a restart on the same port):
	 * the failed connection keeps the member's answered address, so leader hints keep naming it instead of the wrong
	 * port of this node's peer list, and the connection after the backoff reaches the member there again. Before the
	 * fix the failure removed the answer and hints named the listed port until this node forwarded to the member again
	 * (R18 review finding 1).
	 */
	@Test
	void aListedMemberThatFailsOnceAtTheSamePortKeepsItsAnsweredAddress() throws Exception {
		// Bound twice, so taken below the ephemeral range: a kernel-assigned port can go to an outgoing connection (the
		// forwarder's own probes) between the shutdown and the second bind (R19-4).
		int apiPort = ProcessCluster.freePorts(1, List.of()).getFirst();
		var first = startHandshakeServer(apiPort);
		var answered = new HostAndPort("127.0.0.1", apiPort);
		var answer = new AtomicReference<>("127.0.0.1:" + apiPort);
		int raftPort = apiAddressServer(answer);
		// The peer list names a wrong API port: only the member's answer leads to it.
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2),
				new PeerConfig("n1", "127.0.0.1", raftPort, "127.0.0.1", closedPort()));
		var s = setup(peers, Duration.ofSeconds(5));
		var targets = new ArrayList<LeaderForwarder.Target>();
		for (int i = 0; i < 4; i++) {
			targets.add(s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS));
		}
		assertEquals(answered, s.forwarder().apiAddress("n1"));
		long version = s.forwarder().addressVersion();

		first.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
		targets.forEach(s.forwarder()::invalidate);
		var failed = assertThrows(ExecutionException.class,
				() -> s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS));
		assertInstanceOf(LeaderForwarder.NotConnectedException.class, failed.getCause());
		assertEquals(answered, s.forwarder().apiAddress("n1"), "a hint names the listed port after one failure");

		// Back at the same port.
		startHandshakeServer(apiPort);
		Thread.sleep(250);
		assertEquals(answered, s.forwarder().apiAddress("n1"));
		var back = s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS);
		assertEquals("n1", back.peerId());
		assertEquals(answered, s.forwarder().apiAddress("n1"));
		// Leader hints cache their text by this version: the same answer again leaves it, and the cached hint, alone.
		assertEquals(version, s.forwarder().addressVersion());
	}

	/**
	 * Many requests waiting on the same failed connections to a listed member ask the member for its API address once,
	 * not once each: every waiter used to start its own {@code TermService.apiAddress} call, and each call reset the
	 * lookup channel's backoff (R19-2). While the member stays stale, the connections made at once after the backoff
	 * share one ask too.
	 */
	@Test
	void requestsSharingAFailedConnectionAskTheMemberForItsApiAddressOnce() throws Exception {
		var failing = new java.util.concurrent.atomic.AtomicBoolean();
		var api = startHandshakeServer(0, failing);
		var answer = new AtomicReference<>("127.0.0.1:" + api.getPort());
		var asks = new AtomicInteger();
		var refuse = new java.util.concurrent.atomic.AtomicBoolean();
		int raftPort = apiAddressServer(answer, asks, refuse);
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2),
				new PeerConfig("n1", "127.0.0.1", raftPort, "127.0.0.1", closedPort()));
		var s = setup(peers, Duration.ofSeconds(5));
		var targets = new ArrayList<LeaderForwarder.Target>();
		for (int i = 0; i < 4; i++) {
			targets.add(s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS));
		}
		assertEquals(1, asks.get(), "asked once while connecting");

		// The member's API fails its handshakes, slowly: every request below waits on one of four failing connections.
		failing.set(true);
		targets.forEach(s.forwarder()::invalidate);
		var waiting = new ArrayList<java.util.concurrent.CompletableFuture<LeaderForwarder.Target>>();
		for (int i = 0; i < 16; i++) {
			waiting.add(s.forwarder().target("n1", s.stateMachine()));
		}
		for (var request : waiting) {
			var error = assertThrows(ExecutionException.class, () -> request.get(10, TimeUnit.SECONDS));
			assertInstanceOf(LeaderForwarder.NotConnectedException.class, error.getCause());
		}
		Thread.sleep(600);
		assertEquals(2, asks.get(), "each request waiting on a failed connection asked again");

		// The member does not answer the ask any more, so its answer stays stale; after the backoff, the connections
		// made at once share one ask.
		refuse.set(true);
		var answered = s.forwarder().apiAddress("n1");
		// The ask above found the member: one more failed connection marks it stale again, and its ask fails.
		var once = s.forwarder().target("n1", s.stateMachine());
		assertThrows(ExecutionException.class, () -> once.get(10, TimeUnit.SECONDS));
		Thread.sleep(600);
		int before = asks.get();
		var again = new ArrayList<java.util.concurrent.CompletableFuture<LeaderForwarder.Target>>();
		for (int i = 0; i < 8; i++) {
			again.add(s.forwarder().target("n1", s.stateMachine()));
		}
		for (var request : again) {
			assertThrows(ExecutionException.class, () -> request.get(10, TimeUnit.SECONDS));
		}
		Thread.sleep(600);
		assertEquals(before + 1, asks.get(), "each connection made while the member is stale asked on its own");
		assertEquals(answered, s.forwarder().apiAddress("n1"), "the last answer is kept while the member is stale");
	}

	/** A port nothing listens on. */
	private static int closedPort() throws Exception {
		try (var socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	@Test
	void requestsRotateOverFourConnectionsPerPeer() throws Exception {
		var s = setup(peers(handshakeServer()), Duration.ofSeconds(5));
		var targets = new ArrayList<LeaderForwarder.Target>();
		for (int i = 0; i < 8; i++) {
			targets.add(s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS));
		}
		var keys = new HashSet<String>();
		var connections = new HashSet<Object>();
		for (var target : targets) {
			assertEquals("n1", target.peerId());
			keys.add(target.key());
			connections.add(target.connection());
		}
		assertEquals(4, keys.size(), keys::toString);
		assertEquals(4, connections.size());
		// In turn: the fifth request uses the first request's connection again.
		for (int i = 0; i < 4; i++) {
			assertEquals(targets.get(i).key(), targets.get(i + 4).key());
			assertSame(targets.get(i).connection(), targets.get(i + 4).connection());
		}
	}

	@Test
	void anInvalidatedConnectionIsReplacedAndTheOtherSlotsAreKept() throws Exception {
		var s = setup(peers(handshakeServer()), Duration.ofSeconds(5));
		var first = new ArrayList<LeaderForwarder.Target>();
		for (int i = 0; i < 4; i++) {
			first.add(s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS));
		}
		var broken = first.get(1);
		s.forwarder().invalidate(broken);
		// Invalidating again, or a stale target of the same slot, changes nothing more.
		s.forwarder().invalidate(broken);
		var second = new ArrayList<LeaderForwarder.Target>();
		for (int i = 0; i < 4; i++) {
			second.add(s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS));
		}
		for (int i = 0; i < 4; i++) {
			assertEquals(first.get(i).key(), second.get(i).key());
			if (i == 1) {
				assertNotSame(broken.connection(), second.get(i).connection(), "the broken connection is reused");
			} else {
				assertSame(first.get(i).connection(), second.get(i).connection(), "slot " + i + " reconnected");
			}
		}
		s.forwarder().invalidate(broken);
		assertSame(second.get(1).connection(),
				nextOfSlot(s, second.get(1).key()).connection(), "a stale target invalidated a newer connection");
	}

	private static LeaderForwarder.Target nextOfSlot(Setup s, String key) throws Exception {
		for (int i = 0; i < 4; i++) {
			var target = s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS);
			if (target.key().equals(key)) {
				return target;
			}
		}
		throw new AssertionError("slot " + key + " not handed out in four requests");
	}

	/**
	 * A peer whose API port refuses connections fails at once (the probe, instead of the handshake's seconds of
	 * retries), and for the next 200 ms every request to it fails without trying; after that it is tried again.
	 */
	@Test
	void aPeerThatIsDownFailsFastAndIsNotRetriedDuringTheBackoff() throws Exception {
		int port = closedPort();
		var s = setup(peers(port), Duration.ofSeconds(5));
		long start = System.nanoTime();
		var error = assertThrows(ExecutionException.class,
				() -> s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS));
		assertInstanceOf(LeaderForwarder.NotConnectedException.class, error.getCause());
		long probeMillis = (System.nanoTime() - start) / 1_000_000;
		assertTrue(probeMillis < 2_000, "a refused connection took " + probeMillis + " ms to fail");

		// Now something listens there, but the peer is still marked down.
		try (var listening = new ServerSocket()) {
			listening.setReuseAddress(true);
			listening.bind(new InetSocketAddress("127.0.0.1", port));
			var during = s.forwarder().target("n1", s.stateMachine());
			assertTrue(during.isDone(), "a request during the backoff tried to connect");
			assertTrue(during.isCompletedExceptionally());
		}
		Thread.sleep(250);
		// After the backoff it tries again (and fails again: the port is closed once more).
		var after = s.forwarder().target("n1", s.stateMachine());
		var again = assertThrows(ExecutionException.class, () -> after.get(10, TimeUnit.SECONDS));
		assertInstanceOf(LeaderForwarder.NotConnectedException.class, again.getCause());
	}

	/** A peer that is back after its backoff gets a working connection. */
	@Test
	void aPeerThatComesBackIsConnectedAfterTheBackoff() throws Exception {
		int port = closedPort();
		var s = setup(peers(port), Duration.ofSeconds(5));
		assertThrows(ExecutionException.class, () -> s.forwarder().target("n1", s.stateMachine()).get(10,
				TimeUnit.SECONDS));
		Thread.sleep(250);
		var server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", port))
				.addService(new RocksDBServiceGrpc.RocksDBServiceImplBase() {
					@Override
					public void getCapabilities(CapabilitiesRequest request,
							StreamObserver<CapabilitiesResponse> response) {
						var current = RockserverCapabilities.CURRENT;
						response.onNext(CapabilitiesResponse.newBuilder()
								.setWorkloadContractVersion(current.workloadContractVersion())
								.setClientSessions(current.clientSessions())
								.setNumberedBatches(current.numberedBatches())
								.build());
						response.onCompleted();
					}
				})
				.build()
				.start();
		closing.add(() -> server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
		var target = s.forwarder().target("n1", s.stateMachine()).get(10, TimeUnit.SECONDS);
		assertEquals("n1", target.peerId());
	}
}
