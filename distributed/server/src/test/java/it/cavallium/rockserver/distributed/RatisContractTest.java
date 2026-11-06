package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.unimi.dsi.fastutil.ints.IntList;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.grpc.util.StreamObserverWithTimeout;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.raftlog.segmented.LogWorkerGate;
import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.LifeCycle;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.OrderedAsync;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogFormat;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.ratis.util.PureJavaCrc32C;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Behaviors of Ratis 3.3.1 that the design depends on, beyond its compile-time API, pinned against real servers in
 * this JVM (one, or three for a leader that steps down). A Ratis upgrade that changes one of them fails here, instead
 * of silently in production. Pinned elsewhere, because they need links that lose replies: the follower order behind
 * {@link LeaderStall#pairById} and the match index moving only on replies with entries ({@code LeaderStallTest}).
 */
class RatisContractTest {

	private static final Duration WAIT = Duration.ofSeconds(20);

	@TempDir
	Path tempDir;

	private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
	private RaftServer server;
	private RaftClient client;
	private final GateStateMachine stateMachine = new GateStateMachine();

	/** Servers of the tests with several members. */
	private final List<RaftServer> others = new ArrayList<>();
	/** Links in front of the members of a test, by member id. */
	private final Map<String, LinkProxy> proxies = new HashMap<>();

	@AfterEach
	void tearDown() throws IOException {
		stateMachine.gate.countDown();
		for (var member : others) {
			member.close();
		}
		proxies.values().forEach(LinkProxy::close);
		if (client != null) {
			client.close();
		}
		if (server != null) {
			server.close();
		}
	}

	/** {@code ReplicatedNode.startedOutsideJoin} reads these two files to refuse a pointless {@code --join}. */
	@Test
	void aStartWithAPeerListKeepsItInTheBootstrapFileUntilTheLogHasAConfiguration() throws Exception {
		var peer = start(Duration.ofSeconds(3), RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT);
		var current = tempDir.resolve(peer.getId().toString()).resolve(groupId.getUuid().toString()).resolve("current");
		// Before the first election, which this test delays.
		assertTrue(Files.exists(current.resolve("raft-meta.bootstrap")), "bootstrap file");
		assertFalse(Files.exists(current.resolve("raft-meta.conf")), "no configuration from a log yet");
		awaitLeader();
		waitUntil(() -> Files.exists(current.resolve("raft-meta.conf")), "configuration file after the first entry");
	}

	/** {@code RaftLogRepair} reads and rewrites open segments in this format. */
	@Test
	void anOpenSegmentHasTheHeaderAndTheEntryFramesThatTheRepairReads() throws Exception {
		var peer = start(Duration.ofMillis(300), RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT);
		awaitLeader();
		assertTrue(submitLocal("write").get(WAIT.toSeconds(), TimeUnit.SECONDS).isSuccess());
		var current = tempDir.resolve(peer.getId().toString()).resolve(groupId.getUuid().toString()).resolve("current");
		Path open;
		try (var files = Files.list(current)) {
			open = files.filter(f -> f.getFileName().toString().matches("log_inprogress_(\\d+)(?:\\..*)?"))
					.findFirst().orElse(null);
		}
		assertNotNull(open, "an open segment named as RaftLogRepair expects");
		byte[] bytes = Files.readAllBytes(open);
		int header = SegmentedRaftLogFormat.getHeaderLength();
		assertEquals(8, header);
		assertEquals("RaftLog1", new String(bytes, 0, header, StandardCharsets.US_ASCII));
		// First frame: varint32 size, a LogEntryProto starting with the tag of its term, a big-endian CRC32C of both.
		var in = CodedInputStream.newInstance(bytes, header, bytes.length - header);
		int size = in.readRawVarint32();
		int varint = in.getTotalBytesRead();
		assertTrue(size > 0, "first entry size");
		assertEquals(0x08, bytes[header + varint], "an entry starts with its term");
		var crc = new PureJavaCrc32C();
		crc.update(bytes, header, varint + size);
		assertEquals((int) crc.getValue(), ByteBuffer.wrap(bytes, header + varint + size, Integer.BYTES).getInt());
	}

	/**
	 * Ratis refuses a request beyond its pending-request limit with a {@link ResourceUnavailableException}, the same
	 * type it uses after an append: {@link Proposer} cannot tell them apart without the message, so it keeps its own
	 * permit, and {@link RaftSettings} keeps Ratis' limit out of reach.
	 */
	@Test
	void theRefusalOfAFullPendingQueueIsAResourceUnavailableException() throws Exception {
		start(Duration.ofMillis(300), 1);
		awaitLeader();
		stateMachine.blockApply();
		var first = submitLocal("held");
		waitUntil(() -> stateMachine.applying.getCount() == 0, "first request applying");
		RaftClientReply refused;
		try {
			refused = submitLocal("refused").get(WAIT.toSeconds(), TimeUnit.SECONDS);
		} catch (ExecutionException ex) {
			refused = null;
			assertInstanceOf(ResourceUnavailableException.class, ReplicatedConnection.unwrap(ex));
			assertTrue(Proposer.isRatisPendingLimit(ReplicatedConnection.unwrap(ex)), ex::toString);
		}
		if (refused != null) {
			assertFalse(refused.isSuccess());
			assertInstanceOf(ResourceUnavailableException.class, refused.getException());
			// Proposer resends it as refused before the append by this text alone.
			var refusal = refused.getException();
			assertTrue(Proposer.isRatisPendingLimit(refusal), refusal::toString);
		}
		// The refusal after the append must not read as one before it.
		assertFalse(Proposer.isRatisPendingLimit(new ResourceUnavailableException(
				"n0@group-1: Failed to add a pending write request for request")));
		stateMachine.gate.countDown();
		assertTrue(first.get(WAIT.toSeconds(), TimeUnit.SECONDS).isSuccess());
	}

	/**
	 * A state machine's refusal reaches a remote proposer as a {@link StateMachineException} whose cause Ratis rebuilt
	 * from its class name and message: {@link Proposer} recognizes conflicts and "not ready" by that class name, and
	 * reads the conflicting positions from that message ({@link ConflictPositions}). The leader does not step down.
	 */
	@Test
	void aRefusalBeforeTheAppendReachesTheClientByClassNameAndMessage() throws Exception {
		start(Duration.ofMillis(300), RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT);
		awaitLeader();
		// The blocking client throws the reply's exception.
		var refusal = Assertions.assertThrows(StateMachineException.class,
				() -> client.io().send(Message.valueOf("conflict")));
		assertNotNull(refusal.getCause());
		assertEquals(RockserverStateMachine.ConflictException.class.getName(), refusal.getCause().getClass().getName());
		assertArrayEquals(new int[] {1, 3}, ConflictPositions.parse(refusal.getCause().getMessage(), 4));

		var local = submitLocal("conflict").get(WAIT.toSeconds(), TimeUnit.SECONDS);
		var localRefusal = assertInstanceOf(StateMachineException.class, local.getException());
		assertArrayEquals(new int[] {1, 3}, ConflictPositions.parse(localRefusal.getCause().getMessage(), 4));
		assertTrue(server.getDivision(groupId).getInfo().isLeader(), "still the leader");
		assertTrue(client.io().send(Message.valueOf("write")).isSuccess());
	}

	/**
	 * A leader that steps down fails the entries it appended and could not commit with {@link NotLeaderException}, the
	 * same type as a refusal before the append: {@link Proposer} must not take it for "not applied".
	 */
	@Test
	void aLeaderThatStepsDownFailsItsAppendedEntriesWithNotLeaderException() throws Exception {
		var peers = new ArrayList<RaftPeer>();
		for (int port : ProcessCluster.freePorts(3, List.of())) {
			peers.add(RaftPeer.newBuilder().setId("m" + peers.size()).setAddress("127.0.0.1:" + port).build());
		}
		var group = RaftGroup.valueOf(groupId, peers);
		for (var peer : peers) {
			others.add(startMember(peer, group, new GateStateMachine()));
		}
		RaftServer leader = null;
		long deadline = System.nanoTime() + WAIT.toNanos();
		while (leader == null && System.nanoTime() - deadline < 0) {
			for (var member : others) {
				var info = member.getDivision(groupId).getInfo();
				if (info.isLeader() && info.isLeaderReady()) {
					leader = member;
				}
			}
			Thread.sleep(20);
		}
		assertNotNull(leader, "a ready leader");
		for (var member : others) {
			if (member != leader) {
				member.close();
			}
		}
		var reply = leader.submitClientRequestAsync(RaftClientRequest.newBuilder()
				.setClientId(ClientId.randomId())
				.setServerId(leader.getId())
				.setGroupId(groupId)
				.setCallId(1)
				.setMessage(Message.valueOf("appended"))
				.setType(RaftClientRequest.writeRequestType())
				.build());
		var leaderLog = leader.getDivision(groupId).getRaftLog();
		long before = leaderLog.getLastCommittedIndex();
		// Appended to the leader's log, never committed: no follower is left.
		waitUntil(() -> leaderLog.getNextIndex() - 1 > before, "the entry appended");
		var answer = reply.get(WAIT.toSeconds(), TimeUnit.SECONDS);
		assertFalse(answer.isSuccess());
		assertInstanceOf(NotLeaderException.class, answer.getException());
		assertFalse(Proposer.isRefusedBeforeAppend(answer.getException()), "an unknown outcome, not a refusal");
		assertEquals(before, leaderLog.getLastCommittedIndex());
	}

	/**
	 * A server started with an empty group, as {@code --join} does, stays in the initializing state ({@code STARTING}):
	 * no election timer, so no term of its own, and no bootstrap file that a later start would read.
	 */
	@Test
	void aServerWithAnEmptyGroupStaysInitializingWithoutElections() throws Exception {
		var peer = RaftPeer.newBuilder().setId("j0")
				.setAddress("127.0.0.1:" + ProcessCluster.freePorts(1, List.of()).getFirst()).build();
		var joining = startMember(peer, RaftGroup.valueOf(groupId), new GateStateMachine());
		others.add(joining);
		// Three maximum election timeouts of startMember's settings.
		Thread.sleep(3 * 600);
		var division = joining.getDivision(groupId);
		assertEquals(LifeCycle.State.STARTING, division.getInfo().getLifeCycleState());
		assertEquals(0, division.getInfo().getCurrentTerm(), "no election");
		assertFalse(division.getInfo().isLeader());
		var current = tempDir.resolve(peer.getId().toString()).resolve(groupId.getUuid().toString()).resolve("current");
		assertFalse(Files.exists(current.resolve("raft-meta.bootstrap")), "nothing for a later start");
	}

	/**
	 * A {@link TransferLeadershipRequest} with no new leader makes a leader step down, also one that is not ready
	 * ({@code LeaderStall} relies on that): with a single member, it is elected again in a later term.
	 */
	@Test
	void aTransferWithoutANewLeaderStepsDown() throws Exception {
		start(Duration.ofMillis(300), RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT);
		awaitLeader();
		long term = server.getDivision(groupId).getInfo().getCurrentTerm();
		var reply = server.transferLeadershipAsync(new TransferLeadershipRequest(ClientId.randomId(), server.getId(),
				groupId, 0, null, 3_000)).get(WAIT.toSeconds(), TimeUnit.SECONDS);
		assertTrue(reply.isSuccess(), () -> String.valueOf(reply.getException()));
		awaitLeader();
		assertTrue(server.getDivision(groupId).getInfo().getCurrentTerm() > term, "a new term");
	}

	/**
	 * Closing a client channel with a call in flight waits 3 s for it before forcing it
	 * ({@code GrpcUtil.shutdownManagedChannel}), which is what Ratis' clients and proxies do when they reset a peer: so
	 * {@link Proposer#resetConnection} runs it on a thread of its own, and a frozen leader does not hold the resend.
	 */
	@Test
	void closingAChannelWithACallInFlightWaitsThreeSeconds() throws Exception {
		var reached = new CountDownLatch(1);
		var silent = ServerServiceDefinition.builder(TermService.SERVICE_NAME)
				.addMethod(TermService.CURRENT_TERM, ServerCalls.asyncUnaryCall((request, observer) -> reached.countDown()))
				.build();
		var grpcServer = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0)).addService(silent).build()
				.start();
		try {
			var channel = NettyChannelBuilder.forAddress("127.0.0.1", grpcServer.getPort()).usePlaintext().build();
			var call = ClientCalls.futureUnaryCall(channel.newCall(TermService.CURRENT_TERM, CallOptions.DEFAULT),
					new byte[0]);
			assertTrue(reached.await(WAIT.toSeconds(), TimeUnit.SECONDS), "the call reached the server");
			long start = System.nanoTime();
			GrpcUtil.shutdownManagedChannel(channel);
			long millis = (System.nanoTime() - start) / 1_000_000;
			assertTrue(millis >= 2_900 && millis < 6_000, () -> "took " + millis + " ms");
			assertTrue(channel.isTerminated());
			assertTrue(call.isDone());
		} finally {
			grpcServer.shutdownNow();
		}
	}

	/**
	 * {@code StreamObserverWithTimeout}, which sends snapshot chunks, keeps each chunk it sends in the timer of the
	 * chunk's timeout until that timeout, whether or not a reply came: the heap a leader needs while it sends a
	 * snapshot ({@link RaftSettings}, "Heap" in the design document).
	 */
	@Test
	void aSnapshotChunkStaysOnTheHeapUntilItsTimeout() throws Exception {
		var timeout = TimeDuration.valueOf(1_500, TimeUnit.MILLISECONDS);
		var observer = StreamObserverWithTimeout.<Object>newInstance("contract", String::valueOf, () -> timeout, 8,
				_ -> new StreamObserver<>() {
					@Override
					public void onNext(Object value) {
						// Replied at once: the chunk is still held.
					}

					@Override
					public void onError(Throwable t) {
					}

					@Override
					public void onCompleted() {
					}
				});
		var chunk = new byte[8 << 20];
		var held = new WeakReference<>(chunk);
		long start = System.nanoTime();
		observer.onNext(chunk);
		chunk = null;
		System.gc();
		if (System.nanoTime() - start < timeout.toLong(TimeUnit.NANOSECONDS) / 2) {
			assertNotNull(held.get(), "held before its timeout");
		}
		Thread.sleep(timeout.toLong(TimeUnit.MILLISECONDS) * 2);
		waitUntil(() -> {
			System.gc();
			return held.get() == null;
		}, "the chunk released after its timeout");
	}

	/**
	 * A leader commits only what its own log flushed (whatever its followers hold), and its log worker can be stopped by
	 * a task in its queue ({@code LogWorkerGate}, which {@code LeaderStallTest} uses for a hung disk): the rule of a
	 * leader whose log stops in {@link LeaderStall}.
	 */
	@Test
	void aLeaderWhoseLogWorkerStopsCommitsNothing() throws Exception {
		start(Duration.ofMillis(300), RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT);
		awaitLeader();
		assertTrue(submitLocal("first").get(WAIT.toSeconds(), TimeUnit.SECONDS).isSuccess());
		var log = server.getDivision(groupId).getRaftLog();
		// Everything appended so far flushed and committed first.
		waitUntil(() -> log.getFlushIndex() == log.getNextIndex() - 1
				&& log.getLastCommittedIndex() == log.getFlushIndex(), "a settled log");
		var disk = new CountDownLatch(1);
		LogWorkerGate.block(log, disk);
		long committed = log.getLastCommittedIndex();
		long flushed = log.getFlushIndex();
		var held = submitLocal("held");
		Thread.sleep(1_000);
		assertFalse(held.isDone(), "nothing commits");
		assertEquals(committed, log.getLastCommittedIndex());
		assertEquals(flushed, log.getFlushIndex());
		assertTrue(log.getNextIndex() - 1 > flushed, "an entry waits for its flush");
		disk.countDown();
		assertTrue(held.get(WAIT.toSeconds(), TimeUnit.SECONDS).isSuccess());
	}

	/**
	 * RF-2d.1: Ratis' gRPC log appender has no wait while it has no stream to a follower
	 * ({@code GrpcLogAppender.getWaitTimeMs}), a spin until the next heartbeat after a stream reset. Measured here for a
	 * follower that is down: the appender threads of the leader must not take a whole core. The share is printed.
	 */
	@Test
	void theAppenderOfADeadFollowerDoesNotSpin() throws Exception {
		var peers = new ArrayList<RaftPeer>();
		for (int port : ProcessCluster.freePorts(3, List.of())) {
			peers.add(RaftPeer.newBuilder().setId("a" + peers.size()).setAddress("127.0.0.1:" + port).build());
		}
		var group = RaftGroup.valueOf(groupId, peers);
		for (var peer : peers) {
			others.add(startMember(peer, group, new GateStateMachine()));
		}
		RaftServer leader = null;
		long deadline = System.nanoTime() + WAIT.toNanos();
		while (leader == null && System.nanoTime() - deadline < 0) {
			for (var member : others) {
				var info = member.getDivision(groupId).getInfo();
				if (info.isLeader() && info.isLeaderReady()) {
					leader = member;
				}
			}
			Thread.sleep(20);
		}
		assertNotNull(leader, "a ready leader");
		var leaderServer = leader;
		// One follower down: the leader keeps its majority and its appender to the dead one.
		var follower = others.stream().filter(member -> member != leaderServer).findFirst().orElseThrow();
		var followerId = follower.getId().toString();
		follower.close();
		// Long enough for the stream to fail and be reset a few times.
		Thread.sleep(2_000);
		var threads = ManagementFactory.getThreadMXBean();
		long cpuBefore = appenderCpuNanos(threads, followerId);
		long start = System.nanoTime();
		Thread.sleep(3_000);
		long cpu = appenderCpuNanos(threads, followerId) - cpuBefore;
		assertTrue(appenderThreads(threads, followerId) > 0, "the appender threads are found by name");
		double share = (double) cpu / (System.nanoTime() - start);
		System.out.printf("Appender CPU for a dead follower: %.1f%% of a core%n", share * 100);
		assertTrue(share < 0.5, () -> "the appender takes " + share * 100 + "% of a core");
	}

	/** The three servers of the {@link CommitHeartbeats} tests, with the node's Raft settings. */
	private record HeartbeatCluster(RaftServer leader, CommitHeartbeats trigger, List<RaftServer> followers) {}

	/**
	 * Starts three servers with the node's Raft settings; with {@code proxied}, every member reaches each other one
	 * through a {@link LinkProxy} of its own ({@link #proxies}, by member id).
	 */
	private HeartbeatCluster startHeartbeatCluster(boolean enabled, boolean resend, boolean proxied) throws Exception {
		var peerConfigs = new ArrayList<PeerConfig>();
		var ports = ProcessCluster.freePorts(3, List.of());
		for (int port : ports) {
			peerConfigs.add(new PeerConfig("h" + peerConfigs.size(), "127.0.0.1", port, "127.0.0.1", 1));
		}
		var peers = new ArrayList<RaftPeer>();
		for (var peer : peerConfigs) {
			var address = new InetSocketAddress(peer.raftHost(), peer.raftPort());
			if (proxied) {
				var proxy = new LinkProxy(new InetSocketAddress(peer.raftHost(), 0), address);
				proxies.put(peer.id(), proxy);
				address = proxy.listenAddress();
			}
			peers.add(RaftPeer.newBuilder().setId(peer.id())
					.setAddress(address.getHostString() + ":" + address.getPort()).build());
		}
		var group = RaftGroup.valueOf(groupId, peers);
		var heartbeats = new ArrayList<CommitHeartbeats>();
		for (int i = 0; i < peers.size(); i++) {
			var config = NodeConfig.defaults(peerConfigs.get(i).id(), peerConfigs, tempDir.resolve(peerConfigs.get(i).id()))
					.withElectionTimeouts(Duration.ofSeconds(6), Duration.ofSeconds(12));
			var properties = RaftSettings.server(config);
			assertFalse(RaftServerConfigKeys.Log.logMetadataEnabled(properties));
			assertInstanceOf(CommitHeartbeats.Rpc.class, RaftConfigKeys.Rpc.type(properties, _ -> {}));
			RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
			RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties, TimeDuration.valueOf(600, TimeUnit.MILLISECONDS));
			var server = new AtomicReference<RaftServer>();
			var memberHeartbeats = new CommitHeartbeats(server::get, groupId);
			memberHeartbeats.enabled = enabled;
			memberHeartbeats.resendOnNewStream = resend;
			heartbeats.add(memberHeartbeats);
			var parameters = new Parameters();
			parameters.put(CommitHeartbeats.PARAMETER, memberHeartbeats, CommitHeartbeats.class);
			var member = RaftServer.newBuilder()
					.setServerId(peers.get(i).getId())
					.setGroup(group)
					.setProperties(properties)
					.setParameters(parameters)
					.setStateMachine(new GateStateMachine())
					.setOption(RaftStorage.StartupOption.FORMAT)
					.build();
			server.set(member);
			member.start();
			others.add(member);
		}
		RaftServer leader = null;
		long deadline = System.nanoTime() + WAIT.toNanos();
		while (leader == null && System.nanoTime() - deadline < 0) {
			for (var member : others) {
				var info = member.getDivision(groupId).getInfo();
				if (info.isLeader() && info.isLeaderReady()) {
					leader = member;
				}
			}
			Thread.sleep(20);
		}
		assertNotNull(leader, "a ready leader");
		var trigger = heartbeats.get(others.indexOf(leader));
		assertEquals(2, trigger.appenders(), "the leader appends to its followers with CommitHeartbeats.Appender");
		var leaderServer = leader;
		return new HeartbeatCluster(leader, trigger, others.stream().filter(member -> member != leaderServer).toList());
	}

	private long write(RaftServer leader, String value) throws Exception {
		var reply = leader.submitClientRequestAsync(RaftClientRequest.newBuilder()
				.setClientId(ClientId.randomId())
				.setServerId(leader.getId())
				.setGroupId(groupId)
				.setCallId(1)
				.setMessage(Message.valueOf(value))
				.setType(RaftClientRequest.writeRequestType())
				.build()).get(20, TimeUnit.SECONDS);
		assertTrue(reply.isSuccess());
		return leader.getDivision(groupId).getRaftLog().getLastCommittedIndex();
	}

	/** Waits until every follower knows {@code target} is committed; the wait since {@code start}, in microseconds. */
	private static long awaitCommit(List<RaftServer> followers, RaftGroupId groupId, long target, long start)
			throws Exception {
		for (var follower : followers) {
			var followerLog = follower.getDivision(groupId).getRaftLog();
			while (followerLog.getLastCommittedIndex() < target) {
				assertTrue(System.nanoTime() - start < WAIT.toNanos(), "the follower learns the commit index");
				Thread.sleep(0, 200_000);
			}
		}
		return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
	}

	/**
	 * CR2-3, R6-1: {@link CommitHeartbeats} relies on this. With the node's Raft settings ({@link RaftSettings}: no commit
	 * metadata entries, whose appends would carry the commit index by themselves, and {@link CommitHeartbeats.Rpc} as the
	 * transport), a follower learns that a new write is committed only with the next append or heartbeat, and
	 * {@link CommitHeartbeats#send} on the leader makes its appenders send that heartbeat at once. Heartbeats come every
	 * 3 s here (half the minimum election timeout of 6 s); twenty times in a row, both followers must learn the commit
	 * index of a new write within 600 ms of the trigger, and a write without a trigger must reach them only with the
	 * heartbeat, after more than a second (if Ratis ever sends the commit index alone, the class is no longer needed).
	 *
	 * <p>The system property {@code rockserver.test.commitTrigger} picks the trigger: {@code appender} (the default) is
	 * {@link CommitHeartbeats}; {@code ratis} is what it did before R6-1, a read with Ratis' dummy message on the leader,
	 * whose heartbeat round ({@code LogAppender.triggerHeartbeat}) carries commit index 0 to a follower with an append in
	 * flight, so that follower waits for the next heartbeat (with this, the test fails nearly every run); {@code none}
	 * sends no trigger, to see the wait it saves, and asserts nothing.</p>
	 */
	@Test
	void aCommitHeartbeatSendsTheCommitIndexAtOnce() throws Exception {
		var mode = System.getProperty("rockserver.test.commitTrigger", "appender");
		var cluster = startHeartbeatCluster(mode.equals("appender"), true, false);
		var leaderServer = cluster.leader();
		var trigger = cluster.trigger();
		var followers = cluster.followers();
		var dummyReads = new AtomicLong();
		var lags = new ArrayList<Long>();
		for (int i = 0; i < 21; i++) {
			long target = write(leaderServer, "w" + i);
			long start = System.nanoTime();
			// The last write gets no trigger: it shows the wait that the trigger saves.
			boolean last = i == 20;
			if (!last && mode.equals("appender")) {
				trigger.send();
			} else if (!last && mode.equals("ratis")) {
				leaderServer.submitClientRequestAsync(RaftClientRequest.newBuilder()
						.setClientId(ClientId.randomId())
						.setServerId(leaderServer.getId())
						.setGroupId(groupId)
						.setCallId(dummyReads.incrementAndGet())
						.setMessage(OrderedAsync.DUMMY)
						.setType(RaftClientRequest.readRequestType())
						.build());
			}
			lags.add(awaitCommit(followers, groupId, target, start));
		}
		var triggered = lags.subList(0, 20);
		long untriggered = lags.getLast();
		System.out.println("Commit index on both followers after a trigger (" + mode + "), us: " + triggered
				+ "; without one: " + untriggered + " us; appenders woken: " + trigger.woken() + ", heartbeats sent for it: "
				+ trigger.heartbeats());
		if (!mode.equals("none")) {
			assertTrue(triggered.stream().allMatch(lag -> lag < 600_000), triggered::toString);
		}
		if (mode.equals("appender")) {
			assertTrue(trigger.heartbeats() > 0, "the appenders sent heartbeats of their own");
			assertTrue(trigger.heartbeats() <= 2 * trigger.sent(), "at most one heartbeat per follower per commit index");
			assertTrue(trigger.woken() <= 2 * trigger.sent(), "send wakes each appender at most once per commit index");
			assertTrue(untriggered > 1_000_000, () -> "without a trigger, the commit index came after " + untriggered
					+ " us, before the heartbeat: Ratis sends it by itself now, and CommitHeartbeats may be unneeded");
		}
	}

	/**
	 * R6-1: a commit heartbeat whose stream breaks before it arrives is sent again on the new stream. Ratis drops every
	 * request in flight when the stream to a follower fails ({@code GrpcLogAppender.resetClient}), and the next request
	 * opens a new stream; without the resend, the follower would learn the commit index only with the next heartbeat,
	 * 3 s later. For each of 5 writes, the link to one follower holds every byte ({@link LinkProxy#blackhole}) while
	 * the leader sends the commit heartbeat, then resets its connections ({@link LinkProxy#block}), which drops the
	 * heartbeat and fails the leader's streams, and forwards again. {@code -Drockserver.test.commitResend=false} turns
	 * the resend off, to see the test fail.
	 */
	@Test
	void aCommitHeartbeatLostWithItsStreamIsSentAgain() throws Exception {
		boolean resend = Boolean.parseBoolean(System.getProperty("rockserver.test.commitResend", "true"));
		var cluster = startHeartbeatCluster(true, resend, true);
		var trigger = cluster.trigger();
		var link = proxies.get(cluster.followers().getFirst().getId().toString());
		var followerLog = cluster.followers().getFirst().getDivision(groupId).getRaftLog();
		var lags = new ArrayList<Long>();
		for (int i = 0; lags.size() < 5; i++) {
			assertTrue(i < 20, "rounds whose commit index only the commit heartbeat could carry");
			long target = write(cluster.leader(), "s" + i);
			// Let the write's appends get their replies, so that nothing but the heartbeat is in flight.
			Thread.sleep(50);
			link.blackhole();
			if (followerLog.getLastCommittedIndex() >= target) {
				// Ratis' own heartbeat came first: nothing for the commit heartbeat to do in this round.
				link.unblock();
				continue;
			}
			trigger.send();
			// The heartbeat leaves the leader and is held by the link.
			Thread.sleep(100);
			assertTrue(followerLog.getLastCommittedIndex() < target, "the held heartbeat did not arrive");
			long start = System.nanoTime();
			link.block();
			link.unblock();
			lags.add(awaitCommit(cluster.followers(), groupId, target, start));
		}
		System.out.println("Commit index on both followers after a trigger whose heartbeat was lost with its stream, us: "
				+ lags + "; heartbeats sent again: " + trigger.resent());
		assertTrue(lags.stream().allMatch(lag -> lag < 600_000), lags::toString);
		assertTrue(trigger.resent() >= 5, "every lost heartbeat was sent again");
	}

	/** CPU time of the leader's threads that append to {@code followerId} (named {@code ...->followerId-...}). */
	private static long appenderCpuNanos(ThreadMXBean threads, String followerId) {
		long total = 0;
		for (var info : threads.dumpAllThreads(false, false)) {
			if (info.getThreadName().contains("->" + followerId)) {
				long cpu = threads.getThreadCpuTime(info.getThreadId());
				if (cpu > 0) {
					total += cpu;
				}
			}
		}
		return total;
	}

	private static long appenderThreads(ThreadMXBean threads, String followerId) {
		return Arrays.stream(threads.dumpAllThreads(false, false))
				.filter(info -> info.getThreadName().contains("->" + followerId))
				.count();
	}

	/** A member of a group of several servers in this JVM, with this test's timeouts. */
	private RaftServer startMember(RaftPeer peer, RaftGroup group, GateStateMachine machine) throws IOException {
		var properties = memberProperties(peer);
		RaftServerConfigKeys.Log.Appender.setRetryPolicy(properties, RaftSettings.APPENDER_RETRY_POLICY);
		return start(peer, group, properties, machine);
	}

	private RaftServer start(RaftPeer peer, RaftGroup group, RaftProperties properties, GateStateMachine machine)
			throws IOException {
		var member = RaftServer.newBuilder()
				.setServerId(peer.getId())
				.setGroup(group)
				.setProperties(properties)
				.setStateMachine(machine)
				.setOption(RaftStorage.StartupOption.FORMAT)
				.build();
		member.start();
		return member;
	}

	private RaftProperties memberProperties(RaftPeer peer) {
		var properties = new RaftProperties();
		RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
		var address = peer.getAddress();
		GrpcConfigKeys.Server.setHost(properties, "127.0.0.1");
		GrpcConfigKeys.Server.setPort(properties, Integer.parseInt(address.substring(address.lastIndexOf(':') + 1)));
		RaftServerConfigKeys.setStorageDir(properties, List.of(tempDir.resolve(peer.getId().toString()).toFile()));
		RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
		RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(600, TimeUnit.MILLISECONDS));
		RaftServerConfigKeys.LeaderElection.setLeaderStepDownWaitTime(properties,
				TimeDuration.valueOf(600, TimeUnit.MILLISECONDS));
		return properties;
	}

	private RaftPeer start(Duration firstElection, int pendingLimit) throws IOException {
		var peer = RaftPeer.newBuilder().setId("n0")
				.setAddress("127.0.0.1:" + ProcessCluster.freePorts(1, List.of()).getFirst()).build();
		var properties = new RaftProperties();
		RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
		var address = peer.getAddress();
		GrpcConfigKeys.Server.setHost(properties, "127.0.0.1");
		GrpcConfigKeys.Server.setPort(properties, Integer.parseInt(address.substring(address.lastIndexOf(':') + 1)));
		RaftServerConfigKeys.setStorageDir(properties, List.of(tempDir.resolve(peer.getId().toString()).toFile()));
		RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
		RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(600, TimeUnit.MILLISECONDS));
		var first = TimeDuration.valueOf(firstElection.toMillis(), TimeUnit.MILLISECONDS);
		RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties, first);
		RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties, first.add(first));
		RaftServerConfigKeys.Write.setElementLimit(properties, pendingLimit);
		var group = RaftGroup.valueOf(groupId, peer);
		server = RaftServer.newBuilder()
				.setServerId(peer.getId())
				.setGroup(group)
				.setProperties(properties)
				.setStateMachine(stateMachine)
				.setOption(RaftStorage.StartupOption.FORMAT)
				.build();
		server.start();
		var clientProperties = new RaftProperties();
		RaftConfigKeys.Rpc.setType(clientProperties, SupportedRpcType.GRPC);
		client = RaftClient.newBuilder()
				.setProperties(clientProperties)
				.setRaftGroup(group)
				.setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(50,
						TimeDuration.valueOf(100, TimeUnit.MILLISECONDS)))
				.build();
		return peer;
	}

	private CompletableFuture<RaftClientReply> submitLocal(String text) throws IOException {
		return server.submitClientRequestAsync(RaftClientRequest.newBuilder()
				.setClientId(ClientId.randomId())
				.setServerId(server.getId())
				.setGroupId(groupId)
				.setCallId(1)
				.setMessage(Message.valueOf(text))
				.setType(RaftClientRequest.writeRequestType())
				.build());
	}

	private void awaitLeader() throws InterruptedException {
		waitUntil(() -> {
			try {
				var info = server.getDivision(groupId).getInfo();
				return info.isLeader() && info.isLeaderReady();
			} catch (IOException e) {
				return false;
			}
		}, "a ready leader");
	}

	private static void waitUntil(BooleanSupplier condition, String what) throws InterruptedException {
		long deadline = System.nanoTime() + WAIT.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadline > 0) {
				throw new AssertionError("Timed out waiting for " + what);
			}
			Thread.sleep(20);
		}
	}

	/** Refuses "conflict" before the append like the real state machine; can hold the apply of client writes. */
	private static final class GateStateMachine extends BaseStateMachine {

		private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
		final CountDownLatch gate = new CountDownLatch(1);
		volatile CountDownLatch applying = new CountDownLatch(0);
		private final AtomicReference<Boolean> blocking = new AtomicReference<>(false);

		void blockApply() {
			applying = new CountDownLatch(1);
			blocking.set(true);
		}

		@Override
		public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
			super.initialize(server, groupId, raftStorage);
			storage.init(raftStorage);
		}

		@Override
		public SimpleStateMachineStorage getStateMachineStorage() {
			return storage;
		}

		@Override
		public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
			var request = trx.getClientRequest();
			if (request != null && request.getMessage().getContent().toStringUtf8().equals("conflict")) {
				var conflict = RockserverStateMachine.ConflictException.of(IntList.of(1, 3));
				throw new StateMachineException(conflict.getMessage(), conflict, false);
			}
			return trx;
		}

		@Override
		public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
			var entry = trx.getLogEntry();
			updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
			if (blocking.compareAndSet(true, false)) {
				// The pending request stays pending until this future completes; the updater thread goes on.
				applying.countDown();
				return CompletableFuture.supplyAsync(() -> {
					try {
						gate.await();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					return Message.valueOf("ok");
				});
			}
			return CompletableFuture.completedFuture(Message.valueOf("ok"));
		}
	}
}
