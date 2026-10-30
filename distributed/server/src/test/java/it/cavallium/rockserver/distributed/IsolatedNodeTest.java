package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.ClientFailoverTest.awaitLeader;
import static it.cavallium.rockserver.distributed.ClientFailoverTest.remotePort;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * A node cut off from the Raft majority while clients still reach it, as in {@link PartitionTest}: it stops serving its
 * API, so that a client given every node's address moves to the majority, and it serves again after the heal.
 */
@Timeout(180)
class IsolatedNodeTest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	/** Far more than the 7 s it takes on a quiet machine: one election timeout, and the bound of {@link ApiGate}. */
	private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(40);
	private static final List<String> SERVER_ARGS = List.of("--election-timeout-ms", "400:800",
			"--request-timeout-ms", "3000");
	private static final RequestContext CONTEXT = RequestContext.latency(Duration.ofSeconds(2));
	private static final RequestContext SETUP = RequestContext.batch(Duration.ofSeconds(30));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final int CALLS_ON_THE_MAJORITY = 20;
	private static final List<String> MEMBERS = List.of("1", "2", "3");

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private ProcessCluster cluster;

	@AfterEach
	void tearDown() {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aClientLeavesAnIsolatedLeaderAndItServesAfterTheHeal() throws Exception {
		startCluster(SERVER_ARGS);
		runIsolating(awaitLeader(cluster));
	}

	@Test
	void aClientLeavesAnIsolatedFollowerAndItServesAfterTheHeal() throws Exception {
		startCluster(SERVER_ARGS);
		var leader = awaitLeader(cluster);
		runIsolating(MEMBERS.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow());
	}

	/**
	 * A node restarted while cut off from the majority has seen no leader since it started, and its port accepts
	 * connections: it pauses all the same, and the client that lists it first moves to a node that works.
	 */
	@Test
	void aClientLeavesAFollowerRestartedWhileCutOff() throws Exception {
		startCluster(SERVER_ARGS);
		var leader = awaitLeader(cluster);
		var victim = MEMBERS.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
		var order = new ArrayList<>(List.of(victim));
		MEMBERS.stream().filter(id -> !id.equals(victim)).forEach(order::add);
		var addresses = order.stream().map(cluster::apiAddress).toList();
		int victimPort = cluster.apiAddress(victim).port();

		long column;
		try (var setup = GrpcConnection.forHostsAndPorts("restarted-setup-client", addresses)) {
			column = setup.getAsyncApi(SETUP).createColumnAsync("data", SCHEMA).join();
		}
		cluster.kill(victim);
		cluster.network().isolate(victim);
		cluster.start(victim);
		cluster.waitReady(victim, READY_TIMEOUT);

		try (var connection = GrpcConnection.forHostsAndPorts("restarted-client", addresses)) {
			var api = connection.getAsyncApi(CONTEXT);
			awaitWorks(api, column);
			assertNotEquals(victimPort, remotePort(connection));
			assertFalse(serves(victim), "the isolated node still listens");

			cluster.network().heal();
			await(() -> serves(victim), "the node to serve again");
		}
	}

	/** Every node goes away at once, and comes back at once: it has no leader for a while, and must serve again. */
	@Test
	void aKilledClusterRestartedAtOnceServesAgain() throws Exception {
		restartWholeCluster(ProcessCluster::killAll);
	}

	@Test
	void aStoppedClusterRestartedAtOnceServesAgain() throws Exception {
		restartWholeCluster(stopped -> MEMBERS.forEach(stopped::stop));
	}

	private void restartWholeCluster(Consumer<ProcessCluster> stop) throws Exception {
		startCluster(SERVER_ARGS);
		awaitLeader(cluster);
		var addresses = MEMBERS.stream().map(cluster::apiAddress).toList();
		long column;
		try (var setup = GrpcConnection.forHostsAndPorts("restart-setup-client", addresses)) {
			column = setup.getAsyncApi(SETUP).createColumnAsync("data", SCHEMA).join();
			setup.getAsyncApi(CONTEXT).putAsync(0, column, key(1), Buf.wrap(new byte[] {1}), RequestType.none()).join();
		}
		stop.accept(cluster);
		for (var id : MEMBERS) {
			cluster.start(id);
		}
		cluster.waitReady(READY_TIMEOUT);
		await(() -> MEMBERS.stream().allMatch(this::serves), "every node to serve");
		try (var connection = GrpcConnection.forHostsAndPorts("restart-client", addresses)) {
			var api = connection.getAsyncApi(CONTEXT);
			awaitWorks(api, column);
			assertEquals(Buf.wrap(new byte[] {1}), api.getAsync(0, column, key(1), RequestType.current()).join());
		}
	}

	/**
	 * Node 1 restarts alone and pauses, with no majority to find. A second node is then needed for a leader, and the
	 * first takes part in that election and serves again: nothing that forms a cluster uses the API port.
	 */
	@Test
	void aPausedNodeTakesPartInTheElection() throws Exception {
		startCluster(SERVER_ARGS);
		awaitLeader(cluster);
		cluster.killAll();
		cluster.start("1");
		cluster.waitReady("1", READY_TIMEOUT);
		await(() -> !serves("1"), "the node alone to pause");

		cluster.start("2");
		cluster.waitReady("2", READY_TIMEOUT);
		awaitLeader(cluster, List.of("1", "2"));
		await(() -> serves("1"), "the node to serve again");
	}

	/** A member of a cluster that never formed has known no leader: it does not pause, however long it waits. */
	@Test
	void aNodeOfANewClusterDoesNotPause() throws Exception {
		cluster = ProcessCluster.create(root.resolve("cluster"), 3, 0, SERVER_ARGS);
		cluster.start("1");
		cluster.waitReady("1", READY_TIMEOUT);
		long end = System.nanoTime() + ApiGate.MIN_PAUSE_AFTER.plusSeconds(2).toNanos();
		while (System.nanoTime() - end < 0) {
			assertTrue(serves("1"), "the node paused");
			Thread.sleep(50);
		}

		cluster.start("2");
		cluster.start("3");
		cluster.waitReady(READY_TIMEOUT);
		awaitLeader(cluster);
		try (var connection = GrpcConnection.forHostsAndPorts("new-cluster-client",
				MEMBERS.stream().map(cluster::apiAddress).toList())) {
			awaitWorks(connection.getAsyncApi(CONTEXT),
					connection.getAsyncApi(SETUP).createColumnAsync("data", SCHEMA).join());
		}
	}

	/** A restart before any leader leaves a Raft directory and no entry: that node has known no leader either. */
	@Test
	void aNodeOfANewClusterRestartedBeforeItsFirstLeaderDoesNotPause() throws Exception {
		cluster = ProcessCluster.create(root.resolve("cluster"), 3, 0, SERVER_ARGS);
		cluster.start("1");
		cluster.waitReady("1", READY_TIMEOUT);
		cluster.kill("1");
		cluster.start("1");
		cluster.waitReady("1", READY_TIMEOUT);
		long end = System.nanoTime() + ApiGate.MIN_PAUSE_AFTER.plusSeconds(2).toNanos();
		while (System.nanoTime() - end < 0) {
			assertTrue(serves("1"), "the node paused");
			Thread.sleep(50);
		}
	}

	/** Every node pauses, a leader appears, every node resumes, and the client's connection recovers by itself. */
	@Test
	void aClusterThatLostItsLeaderForLongRecoversByItself() throws Exception {
		startCluster(SERVER_ARGS);
		awaitLeader(cluster);
		try (var connection = GrpcConnection.forHostsAndPorts("whole-cluster-client",
				MEMBERS.stream().map(cluster::apiAddress).toList())) {
			var api = connection.getAsyncApi(CONTEXT);
			long column = connection.getAsyncApi(SETUP).createColumnAsync("data", SCHEMA).join();
			api.putAsync(0, column, key(1), Buf.wrap(new byte[] {1}), RequestType.none()).join();

			MEMBERS.forEach(cluster.network()::isolate);
			await(() -> MEMBERS.stream().noneMatch(this::serves), "every node to pause");

			cluster.network().heal();
			await(() -> MEMBERS.stream().allMatch(this::serves), "every node to resume");

			awaitWorks(api, column);
		}
	}

	/** The node that loses its leader to an election, shorter than the bound, keeps listening all the time. */
	@Test
	void anElectionPausesNoNode() throws Exception {
		startCluster(List.of("--election-timeout-ms", "1000:2000", "--request-timeout-ms", "3000"));
		var leader = awaitLeader(cluster);
		var survivors = MEMBERS.stream().filter(id -> !id.equals(leader)).toList();

		cluster.kill(leader);
		long killedAt = System.nanoTime();
		var newLeader = new ArrayList<String>();
		var watcher = Thread.ofVirtual().start(() -> {
			try {
				newLeader.add(awaitLeader(cluster, survivors));
			} catch (Exception e) {
				newLeader.add(null);
			}
		});

		// Past the end of the election, and past the bound counted from the kill.
		long end = killedAt + ApiGate.MIN_PAUSE_AFTER.plusSeconds(2).toNanos();
		while (System.nanoTime() - end < 0) {
			for (var id : survivors) {
				assertTrue(serves(id), () -> "node " + id + " stopped serving during the election");
			}
			Thread.sleep(50);
		}
		watcher.join();
		assertNotNull(newLeader.getFirst(), "no new leader");
	}

	private void startCluster(List<String> args) throws Exception {
		cluster = ProcessCluster.start(root.resolve("cluster"), 3, args);
		cluster.waitReady(READY_TIMEOUT);
	}

	/** A client lists {@code victim} first, and it is cut off from the other nodes. */
	private void runIsolating(String victim) throws Exception {
		var order = new ArrayList<>(List.of(victim));
		MEMBERS.stream().filter(id -> !id.equals(victim)).forEach(order::add);
		int victimPort = cluster.apiAddress(victim).port();

		try (var connection = GrpcConnection.forHostsAndPorts("isolation-client",
				order.stream().map(cluster::apiAddress).toList())) {
			var api = connection.getAsyncApi(CONTEXT);
			long column = connection.getAsyncApi(SETUP).createColumnAsync("data", SCHEMA).join();
			api.putAsync(0, column, key(0), Buf.wrap(new byte[] {1}), RequestType.none()).join();
			assertEquals(victimPort, remotePort(connection), "the first address is preferred");

			cluster.network().isolate(victim);
			long isolatedAt = System.nanoTime();

			awaitWorks(api, column);
			System.out.println("IsolatedNodeTest: node " + victim + ", the client works again after "
					+ Duration.ofNanos(System.nanoTime() - isolatedAt).toMillis() + " ms");
			assertNotEquals(victimPort, remotePort(connection));
			assertFalse(serves(victim), "the isolated node still listens");
			for (int i = 1; i <= CALLS_ON_THE_MAJORITY; i++) {
				var value = Buf.wrap(new byte[] {(byte) i});
				api.putAsync(0, column, key(i), value, RequestType.none()).join();
				assertEquals(value, api.getAsync(0, column, key(i), RequestType.current()).join());
			}

			cluster.network().heal();
			await(() -> serves(victim), "the node to serve again");

			// Its own clients are back, with what the majority wrote.
			try (var direct = GrpcConnection.forHostAndPort("direct-client", cluster.apiAddress(victim))) {
				awaitWorks(direct.getAsyncApi(CONTEXT), column);
				assertEquals(Buf.wrap(new byte[] {CALLS_ON_THE_MAJORITY}),
						direct.getAsyncApi(CONTEXT).getAsync(0, column, key(CALLS_ON_THE_MAJORITY), RequestType.current())
								.join());
			}
		}
	}

	/** Waits until a write and a read succeed: until then calls may fail in every way a cut-off node fails them. */
	private static void awaitWorks(RocksDBAsyncAPI api, long column) throws Exception {
		long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
		while (true) {
			try {
				api.putAsync(0, column, key(-1), Buf.wrap(new byte[] {2}), RequestType.none()).join();
				api.getAsync(0, column, key(-1), RequestType.current()).join();
				return;
			} catch (RuntimeException e) {
				if (System.nanoTime() - deadline > 0) {
					throw new AssertionError("calls never worked again", e);
				}
				Thread.sleep(50);
			}
		}
	}

	private void await(BooleanSupplier condition, String what) throws InterruptedException {
		long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
		while (!condition.getAsBoolean()) {
			assertTrue(System.nanoTime() - deadline < 0, () -> "waited too long for " + what);
			Thread.sleep(20);
		}
	}

	/** Whether the node's API port accepts connections. */
	private boolean serves(String id) {
		var address = cluster.apiAddress(id);
		try (var socket = new Socket()) {
			socket.connect(new InetSocketAddress(address.host(), address.port()), 1_000);
			return true;
		} catch (IOException refused) {
			return false;
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
