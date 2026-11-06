package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.unimi.dsi.fastutil.ints.IntList;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.grpc.util.StreamObserverWithTimeout;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;
import org.apache.ratis.util.LifeCycle;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
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

	@AfterEach
	void tearDown() throws IOException {
		stateMachine.gate.countDown();
		for (var member : others) {
			member.close();
		}
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
		} catch (java.util.concurrent.ExecutionException ex) {
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
		var refusal = org.junit.jupiter.api.Assertions.assertThrows(StateMachineException.class,
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
				_ -> new org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver<>() {
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
		org.apache.ratis.server.raftlog.segmented.LogWorkerGate.block(log, disk);
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
		var threads = java.lang.management.ManagementFactory.getThreadMXBean();
		long cpuBefore = appenderCpuNanos(threads, followerId);
		long start = System.nanoTime();
		Thread.sleep(3_000);
		long cpu = appenderCpuNanos(threads, followerId) - cpuBefore;
		assertTrue(appenderThreads(threads, followerId) > 0, "the appender threads are found by name");
		double share = (double) cpu / (System.nanoTime() - start);
		System.out.printf("Appender CPU for a dead follower: %.1f%% of a core%n", share * 100);
		assertTrue(share < 0.5, () -> "the appender takes " + share * 100 + "% of a core");
	}

	/** CPU time of the leader's threads that append to {@code followerId} (named {@code ...->followerId-...}). */
	private static long appenderCpuNanos(java.lang.management.ThreadMXBean threads, String followerId) {
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

	private static long appenderThreads(java.lang.management.ThreadMXBean threads, String followerId) {
		return java.util.Arrays.stream(threads.dumpAllThreads(false, false))
				.filter(info -> info.getThreadName().contains("->" + followerId))
				.count();
	}

	/** A member of a group of several servers in this JVM, with this test's timeouts. */
	private RaftServer startMember(RaftPeer peer, RaftGroup group, GateStateMachine machine) throws IOException {
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
		RaftServerConfigKeys.Log.Appender.setRetryPolicy(properties, RaftSettings.APPENDER_RETRY_POLICY);
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
