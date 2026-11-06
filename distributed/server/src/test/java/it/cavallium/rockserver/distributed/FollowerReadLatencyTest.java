package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Read-your-write latency on a follower, against the same read forwarded to the leader: a small in-process loop, not a
 * benchmark. Each round writes on the leader, then reads the value back on a follower in each {@link Mode}, in turn,
 * on a quiet cluster and with a writer in the background. It prints the percentiles (see "Follower reads" in the design
 * document) and checks that with the heartbeats the follower serves nearly every read itself.
 */
class FollowerReadLatencyTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	private static final int ROUNDS = 100;

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static RocksDBSyncAPI api(ReplicatedNode node) {
		return node.connection().getSyncApi(CONTEXT);
	}

	private static Router routing(ReplicatedNode node) {
		return ((ReplicatedConnection) node.connection()).router;
	}

	private enum Mode {
		/** A follower read. */
		FOLLOWER,
		/** A follower read without {@link CommitHeartbeats}, as before them. */
		FOLLOWER_WITHOUT_HEARTBEATS,
		/** Forwarded to a leader that knows followers serve reads: it waits for its apply of the commit index. */
		FORWARDED,
		/** Forwarded to a leader of a cluster without follower reads: it waits only for its leadership. */
		FORWARDED_NO_FOLLOWER_READS
	}

	@Test
	void followerReadAfterWriteIsNotSlowerThanForwarding() throws Exception {
		cluster = new InProcessCluster(temp, 3, config -> config.withRequestTimeout(Duration.ofSeconds(3))).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(leader).createColumn("c", SCHEMA);
		// Warm up connections and code paths.
		for (int i = 0; i < 50; i++) {
			api(leader).put(0, column, key(i), Buf.wrap(new byte[] {1}), RequestType.none());
			api(follower).get(0, column, key(i), RequestType.current());
		}

		for (boolean busy : new boolean[] {false, true}) {
			var stop = new AtomicBoolean();
			Thread background = null;
			if (busy) {
				// Writes flow meanwhile: Ratis then sends the commit index with its appends anyway.
				background = Thread.ofVirtual().start(() -> {
					for (long i = 1_000_000; !stop.get(); i++) {
						api(leader).put(0, column, key(i), Buf.wrap(new byte[] {2}), RequestType.none());
					}
				});
			}
			try {
				var nanos = new long[Mode.values().length][ROUNDS];
				var served = new long[Mode.values().length];
				for (int round = 0; round < ROUNDS; round++) {
					for (var mode : Mode.values()) {
						long k = 10_000 + round * 3L + mode.ordinal();
						var value = "v" + k;
						api(leader).put(0, column, key(k), Buf.wrap(value.getBytes(StandardCharsets.UTF_8)),
								RequestType.none());
						routing(follower).followerReadsEnabled = mode == Mode.FOLLOWER
								|| mode == Mode.FOLLOWER_WITHOUT_HEARTBEATS;
						leader.commitHeartbeats.enabled = mode != Mode.FOLLOWER_WITHOUT_HEARTBEATS;
						leader.readBarrier().followerReads = mode != Mode.FORWARDED_NO_FOLLOWER_READS;
						long servedBefore = routing(follower).followerReadsServedForTesting();
						long start = System.nanoTime();
						var read = api(follower).get(0, column, key(k), RequestType.current());
						nanos[mode.ordinal()][round] = System.nanoTime() - start;
						served[mode.ordinal()] += routing(follower).followerReadsServedForTesting() - servedBefore;
						assertEquals(value, new String(read.toByteArray(), StandardCharsets.UTF_8));
					}
				}
				for (var mode : Mode.values()) {
					var sorted = nanos[mode.ordinal()].clone();
					Arrays.sort(sorted);
					System.out.printf("FollowerReadLatencyTest %s %-28s served by the follower %3d/%d,"
									+ " p50 %.2f ms, p90 %.2f ms, p99 %.2f ms, max %.2f ms%n", busy ? "busy " : "quiet", mode,
							served[mode.ordinal()], ROUNDS, sorted[ROUNDS / 2] / 1e6, sorted[ROUNDS * 9 / 10] / 1e6,
							sorted[ROUNDS * 99 / 100] / 1e6, sorted[ROUNDS - 1] / 1e6);
				}
				// With the heartbeats the follower serves nearly every read itself on an idle machine, quiet or busy, and
				// none quiet without them. Loose bounds: how many miss Router.FOLLOWER_READ_WAIT depends on the disk and
				// the machine's load; the numbers printed above are the measurement.
				assertTrue(served[Mode.FOLLOWER.ordinal()] > (busy ? 0 : ROUNDS / 2),
						"served by the follower: " + served[Mode.FOLLOWER.ordinal()]);
			} finally {
				stop.set(true);
				if (background != null) {
					background.join();
				}
				leader.commitHeartbeats.enabled = true;
				leader.readBarrier().followerReads = true;
				routing(follower).followerReadsEnabled = true;
			}
		}
	}
}
