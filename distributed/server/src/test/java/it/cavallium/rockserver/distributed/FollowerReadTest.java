package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import it.cavallium.rockserver.core.common.LeaderHint;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Point reads served by followers: linearizable, and forwarded rather than stale when a follower lags or is cut off. */
@Tag("slow")
class FollowerReadTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		try {
			if (cluster != null) {
				cluster.close();
			}
		} finally {
			System.clearProperty(Router.FOLLOWER_READS_PROPERTY);
		}
	}

	private void start() throws Exception {
		// Off by default: turned on on every node, as configured in production. Read when a node starts.
		System.setProperty(Router.FOLLOWER_READS_PROPERTY, "true");
		cluster = new InProcessCluster(temp, 3, config -> config.withRequestTimeout(Duration.ofSeconds(3))).startAll();
		for (var node : cluster.runningNodes()) {
			assertTrue(routing(node).followerReadsEnabled);
			assertTrue(node.readBarrier().followerReads);
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

	private static Router routing(ReplicatedNode node) {
		return ((ReplicatedConnection) node.connection()).router;
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
		// A read whose index the follower does not apply within Router.FOLLOWER_READ_WAIT is forwarded: with the
		// leader sending the commit index on every read index request, few are here, but how many depends on the disk
		// and the machine's load, so only the correctness above is asserted, and that the follower serves some.
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
	void followerOfAQuietClusterServesReadsOfTheWriteJustAcknowledged() throws Exception {
		start();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(leader).createColumn("c", SCHEMA);
		// Warm up the follower's channel to the leader and the code paths.
		for (int i = 0; i < 20; i++) {
			api(leader).put(0, column, key(-1), text("warm-up"), RequestType.none());
			get(follower, column, -1);
		}
		long servedBefore = routing(follower).followerReadsServedForTesting();

		// Nothing else goes on: without CommitHeartbeats the follower would learn each commit index only with the next
		// heartbeat, hundreds of milliseconds later, and forward every read.
		int writes = 50;
		for (int i = 0; i < writes; i++) {
			api(leader).put(0, column, key(i), text("v" + i), RequestType.none());
			assertEquals("v" + i, get(follower, column, i));
		}

		long served = routing(follower).followerReadsServedForTesting() - servedBefore;
		System.out.println("FollowerReadTest: quiet cluster, " + served + " of " + writes + " served by the follower");
		// Nearly all on an idle machine; without CommitHeartbeats none. A loose bound: a slow or shared disk may push
		// some past Router.FOLLOWER_READ_WAIT.
		assertTrue(served > writes / 2, "served by the follower: " + served + " of " + writes);
	}

	@Test
	void followerForwardsAReadThatMaySeeAnEntryOfALaterTerm() throws Exception {
		start();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(leader).createColumn("c", SCHEMA);
		api(leader).put(0, column, key(1), text("v"), RequestType.none());
		assertEquals("v", get(follower, column, 1));

		// As if the follower had applied an entry of the next leader, which may not have answered a read index yet: its
		// leader reads may not wait for that entry, so the follower must not serve it either.
		long term = leader.stateMachine().division().getInfo().getCurrentTerm();
		follower.stateMachine().raiseVisibleTermForTesting(term + 1);
		long servedBefore = routing(follower).followerReadsServedForTesting();
		long forwardedBefore = routing(follower).followerReadsForwardedForTesting();

		assertEquals("v", get(follower, column, 1));

		assertEquals(servedBefore, routing(follower).followerReadsServedForTesting());
		assertEquals(forwardedBefore + 1, routing(follower).followerReadsForwardedForTesting());
	}

	/**
	 * R2-RT-5, with a real leader change instead of {@code raiseVisibleTermForTesting}: a follower holds a read index of
	 * term T, and its apply reaches that index in the same group as an entry of term T+1 (its apply was held meanwhile,
	 * here by the test's apply gate). The new leader may not have its follower reads on yet, so the follower forwards
	 * the read instead of answering from a database that holds an entry of the later term.
	 */
	@Test
	void aReadIndexOfTheOldTermReachedTogetherWithAnEntryOfTheNewTermIsForwarded() throws Exception {
		start();
		var leader = cluster.awaitLeader(TIMEOUT);
		long column = api(leader).createColumn("c", SCHEMA);
		api(leader).put(0, column, key(1), text("v0"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		var follower = cluster.anyFollower();
		var successor = cluster.runningNodes().stream().filter(node -> node != leader && node != follower).findFirst()
				.orElseThrow();
		// The read waits for the follower's apply as long as the test needs, not the 20 ms of production.
		routing(follower).followerReadWaitNanos = TIMEOUT.toNanos();
		long term = leader.stateMachine().division().getInfo().getCurrentTerm();

		// The apply thread takes the next group, the entry of v1, and holds it at the gate.
		var gate = new CountDownLatch(1);
		follower.stateMachine().testHooks().applyGate = gate;
		api(leader).put(0, column, key(1), text("v1"), RequestType.none());
		var applyThread = follower.config().nodeId() + "-apply";
		InProcessCluster.await(TIMEOUT, () -> Thread.getAllStackTraces().entrySet().stream()
				.anyMatch(e -> e.getKey().getName().equals(applyThread) && e.getKey().getState() == Thread.State.WAITING
						&& Arrays.stream(e.getValue()).anyMatch(f -> f.getMethodName().equals("applyGroup"))));
		// Queued behind it: v2, of term T. The read index of the follower's read is at least its index, of term T.
		api(leader).put(0, column, key(1), text("v2"), RequestType.none());
		long servedBefore = routing(follower).followerReadsServedForTesting();
		long forwardedBefore = routing(follower).followerReadsForwardedForTesting();
		var read = CompletableFuture.supplyAsync(() -> get(follower, column, 1));
		// The read index arrives and the read waits for the apply.
		Thread.sleep(300);
		assertFalse(read.isDone());
		assertTrue(follower.stateMachine().appliedIndex().get() < commitIndex(leader));

		// A new leader in term T+1, on the third node; its first entries reach the follower's log, behind v2.
		var transfer = leader.stateMachine().division().getRaftServer().transferLeadershipAsync(
				new TransferLeadershipRequest(ClientId.randomId(),
						leader.stateMachine().division().getId(), leader.stateMachine().division().getGroup().getGroupId(),
						1, successor.stateMachine().division().getId(), 10_000));
		assertTrue(transfer.get(30, TimeUnit.SECONDS).isSuccess());
		InProcessCluster.await(TIMEOUT, () -> successor.isLeader()
				&& successor.stateMachine().division().getInfo().getCurrentTerm() > term);
		api(successor).put(0, column, key(2), text("t1"), RequestType.none());
		InProcessCluster.await(TIMEOUT, () -> {
			var log = follower.stateMachine().division().getRaftLog();
			var last = log.getLastEntryTermIndex();
			return last != null && last.getTerm() > term && log.getLastCommittedIndex() >= last.getIndex();
		});
		assertEquals(term, follower.stateMachine().visibleTerm(), "nothing of the new term applied yet");

		// The rest of the queue, v2 and the entries of term T+1, is applied as one group: the read index is reached
		// with an entry of the later term visible.
		gate.countDown();
		assertEquals("v2", read.get(30, TimeUnit.SECONDS));
		assertTrue(follower.stateMachine().visibleTerm() > term);
		assertEquals(servedBefore, routing(follower).followerReadsServedForTesting(), "not served by the follower");
		assertEquals(forwardedBefore + 1, routing(follower).followerReadsForwardedForTesting());
	}

	@Test
	void followerReadsStayLinearizableAcrossALeaderChange() throws Exception {
		start();
		var leader = cluster.awaitLeader(TIMEOUT);
		long column = api(leader).createColumn("c", SCHEMA);
		api(leader).put(0, column, key(1), text("before"), RequestType.none());
		for (var node : cluster.runningNodes()) {
			assertEquals("before", get(node, column, 1));
		}

		cluster.halt(leader.config().nodeId());
		var newLeader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long servedBefore = routing(follower).followerReadsServedForTesting();
		for (int i = 0; i < 50; i++) {
			api(newLeader).put(0, column, key(1), text("after" + i), RequestType.none());
			// Acknowledged by the new leader: both remaining nodes answer it.
			assertEquals("after" + i, get(follower, column, 1));
			assertEquals("after" + i, get(newLeader, column, 1));
		}
		// Reads under the new leader's read indexes are served by the follower again.
		assertTrue(routing(follower).followerReadsServedForTesting() > servedBefore);
	}

	@Test
	void leaderReadSeesWhatAFollowerAlreadyServed() throws Exception {
		start();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(leader).createColumn("c", SCHEMA);
		api(leader).put(0, column, key(1), text("old"), RequestType.none());
		assertEquals("old", get(follower, column, 1));

		// The leader commits the write but does not apply it, so it does not acknowledge it either; the followers apply
		// it, and a client reads it on one of them.
		// The follower must serve the read itself: forwarded, it would wait for the held apply of the leader.
		routing(follower).followerReadWaitNanos = TIMEOUT.toNanos();
		var gate = new CountDownLatch(1);
		leader.stateMachine().testHooks().applyGate = gate;
		try {
			var write = Thread.ofVirtual().start(() -> api(leader).put(0, column, key(1), text("new"),
					RequestType.none()));
			InProcessCluster.await(TIMEOUT, () -> "new".equals(get(follower, column, 1)));

			// A read that starts after that one ended must see the write too, on the leader as well: it waits for its own
			// apply of the commit index instead of answering "old" at once.
			var onLeader = new CompletableFuture<String>();
			Thread.ofVirtual().start(() -> {
				try {
					onLeader.complete(get(leader, column, 1));
				} catch (Throwable e) {
					onLeader.completeExceptionally(e);
				}
			});
			Thread.sleep(300);
			if (onLeader.isDone()) {
				assertEquals("new", onLeader.join(), "the leader answered while its apply was held");
			}
			gate.countDown();
			assertEquals("new", onLeader.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
			write.join();
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
		}
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
		assertEquals(leaderAddress, ((ReplicatedConnection) follower.connection()).leaderApiAddress());
		assertEquals(leaderAddress, ((ReplicatedConnection) leader.connection()).leaderApiAddress());

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
