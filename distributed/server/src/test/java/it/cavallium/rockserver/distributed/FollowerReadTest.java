package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import it.cavallium.rockserver.core.common.LeaderHint;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Point reads served by followers: linearizable, and forwarded rather than stale when a follower lags or is cut off. */
class FollowerReadTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private void start() throws Exception {
		cluster = new InProcessCluster(temp, 3, config -> config.withRequestTimeout(Duration.ofSeconds(3))).startAll();
		// Off by default.
		for (var node : cluster.runningNodes()) {
			routing(node).followerReadsEnabled = true;
		}
	}

	private static long commitIndex(ReplicatedNode node) {
		return node.stateMachine().division().getRaftLog().getLastCommittedIndex();
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(Buf value) {
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	private static RocksDBSyncAPI api(ReplicatedNode node) {
		return node.connection().getSyncApi(CONTEXT);
	}

	private static ReplicatedConnection routing(ReplicatedNode node) {
		return (ReplicatedConnection) node.connection();
	}

	private static String get(ReplicatedNode node, long column, long key) {
		return text(api(node).get(0, column, key(key), RequestType.current()));
	}

	@Test
	void followerReadsEveryWriteAcknowledgedOnTheLeader() throws Exception {
		start();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(leader).createColumn("c", SCHEMA);
		long servedBefore = routing(follower).followerReadsServedForTesting();
		long forwardedBefore = routing(follower).followerReadsForwardedForTesting();

		// Writes flow meanwhile, as in production: Ratis then tells followers the commit index with every append.
		var stop = new AtomicBoolean();
		var background = Thread.ofVirtual().start(() -> {
			for (long i = 1_000_000; !stop.get(); i++) {
				api(leader).put(0, column, key(i), text("background"), RequestType.none());
			}
		});
		int writes = 200;
		long writeNanos = 0;
		long readNanos = 0;
		try {
			for (int i = 0; i < writes; i++) {
				long t0 = System.nanoTime();
				api(leader).put(0, column, key(i), text("v" + i), RequestType.none());
				long t1 = System.nanoTime();
				// Acknowledged on the leader: the follower must see it at once.
				assertEquals("v" + i, get(follower, column, i));
				writeNanos += t1 - t0;
				readNanos += System.nanoTime() - t1;
			}
		} finally {
			stop.set(true);
			background.join();
		}

		var keys = List.of(key(0), key(writes - 1), key(writes + 1));
		var page = api(follower).getMulti(column, keys, new RangeBudget(128, RangeBudget.DEFAULT_MAX_BYTES));
		assertEquals(List.of("v0", "v" + (writes - 1)), page.values().subList(0, 2).stream().map(FollowerReadTest::text)
				.toList());
		assertEquals(null, page.values().get(2));
		assertEquals(List.of(true, true, false), api(follower).existsMulti(0, column, keys));

		long served = routing(follower).followerReadsServedForTesting() - servedBefore;
		long forwarded = routing(follower).followerReadsForwardedForTesting() - forwardedBefore;
		System.out.println("FollowerReadTest: " + writes + " writes took " + Duration.ofNanos(writeNanos).toMillis()
				+ " ms, the follower reads after them " + Duration.ofNanos(readNanos).toMillis() + " ms; " + served
				+ " served by the follower, " + forwarded + " forwarded");
		// A read that finds the commit index not known here yet is forwarded after a few milliseconds, so how many
		// the follower serves depends on timing: some do.
		assertTrue(served > 0, "served by the follower: " + served + ", forwarded: " + forwarded);
		assertEquals(writes + 2, served + forwarded);

		// Once it knows the commit index, a follower serves a read by itself.
		api(leader).put(0, column, key(writes), text("last"), RequestType.none());
		long committed = commitIndex(leader);
		InProcessCluster.await(TIMEOUT, () -> commitIndex(follower) >= committed);
		long servedNow = routing(follower).followerReadsServedForTesting();
		assertEquals("last", get(follower, column, writes));
		assertEquals(servedNow + 1, routing(follower).followerReadsServedForTesting());
	}

	@Test
	void clientOfSeveralNodesReadsItsWritesThroughAFollower() throws Exception {
		start();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		// The follower first: the client's ordinary channel sits on it, and the leader's address comes as a hint.
		var addresses = new ArrayList<HostAndPort>();
		addresses.add(new HostAndPort(follower.config().self().apiHost(), follower.config().self().apiPort()));
		for (var node : cluster.runningNodes()) {
			if (node != follower) {
				addresses.add(new HostAndPort(node.config().self().apiHost(), node.config().self().apiPort()));
			}
		}
		var leaderAddress = leader.config().self().apiHost() + ":" + leader.config().self().apiPort();
		assertEquals(leaderAddress, routing(follower).leaderApiAddress());
		assertEquals(leaderAddress, routing(leader).leaderApiAddress());

		// The follower names the leader in its responses.
		var channel = NettyChannelBuilder.forAddress(addresses.getFirst().host(), addresses.getFirst().port())
				.usePlaintext()
				.build();
		try {
			var headers = new AtomicReference<Metadata>();
			RocksDBServiceGrpc.newBlockingStub(channel)
					.withInterceptors(MetadataUtils.newCaptureMetadataInterceptor(headers, new AtomicReference<>()))
					.getCapabilities(CapabilitiesRequest.getDefaultInstance());
			assertEquals(leaderAddress,
					headers.get().get(Metadata.Key.of(LeaderHint.METADATA_KEY, Metadata.ASCII_STRING_MARSHALLER)));
		} finally {
			channel.shutdownNow();
		}

		// A client of several nodes: writes and transactions may go to the leader, reads stay on the follower.
		long readsBefore = routing(follower).followerReadsServedForTesting()
				+ routing(follower).followerReadsForwardedForTesting();
		try (var client = GrpcConnection.forHostsAndPorts("follower-read-test", addresses)) {
			var api = client.getSyncApi(CONTEXT);
			long column = api.createColumn("c", SCHEMA);
			for (int i = 0; i < 50; i++) {
				api.put(0, column, key(i), text("v" + i), RequestType.none());
				assertEquals("v" + i, text(api.get(0, column, key(i), RequestType.current())));
			}
			long transaction = api.openTransaction(Duration.ofSeconds(10));
			api.put(transaction, column, key(100), text("in a transaction"), RequestType.none());
			assertTrue(api.closeTransaction(transaction, true));
			assertEquals("in a transaction", text(api.get(0, column, key(100), RequestType.current())));
		}
		long reads = routing(follower).followerReadsServedForTesting()
				+ routing(follower).followerReadsForwardedForTesting() - readsBefore;
		assertEquals(51, reads, "point reads that reached the follower");
	}

	@Test
	void laggingFollowerForwardsInsteadOfServingStaleData() throws Exception {
		start();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(leader).createColumn("c", SCHEMA);
		api(leader).put(0, column, key(1), text("old"), RequestType.none());
		assertEquals("old", get(follower, column, 1));

		var gate = new CountDownLatch(1);
		follower.stateMachine().testHooks().applyGate = gate;
		try {
			api(leader).put(0, column, key(1), text("new"), RequestType.none());
			long forwardedBefore = routing(follower).followerReadsForwardedForTesting();

			assertEquals("new", get(follower, column, 1));

			assertEquals(forwardedBefore + 1, routing(follower).followerReadsForwardedForTesting());
		} finally {
			gate.countDown();
			follower.stateMachine().testHooks().applyGate = null;
		}
	}

	@Test
	void followerCutOffFromTheLeaderFailsInsteadOfServing() throws Exception {
		start();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(leader).createColumn("c", SCHEMA);
		api(leader).put(0, column, key(1), text("v"), RequestType.none());
		assertEquals("v", get(follower, column, 1));

		for (var node : cluster.runningNodes()) {
			if (node != follower) {
				cluster.halt(node.config().nodeId());
			}
		}
		long servedBefore = routing(follower).followerReadsServedForTesting();

		assertThrows(RuntimeException.class, () -> get(follower, column, 1));

		assertEquals(servedBefore, routing(follower).followerReadsServedForTesting());
	}
}
