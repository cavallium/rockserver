package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.unimi.dsi.fastutil.ints.IntList;
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
 * Behaviors of Ratis 3.3.1 that the design depends on, beyond its compile-time API, pinned against one real
 * single-node server. A Ratis upgrade that changes one of them fails here, instead of silently in production. Not
 * pinned here (they need several nodes, see the cluster tests): {@code NotLeaderException} for appended entries of a
 * leader that steps down, the follower order behind {@link LeaderStall#pairById}, the match index moving only on
 * replies with entries.
 */
class RatisContractTest {

	private static final Duration WAIT = Duration.ofSeconds(20);

	@TempDir
	Path tempDir;

	private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
	private RaftServer server;
	private RaftClient client;
	private final GateStateMachine stateMachine = new GateStateMachine();

	@AfterEach
	void tearDown() throws IOException {
		stateMachine.gate.countDown();
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
