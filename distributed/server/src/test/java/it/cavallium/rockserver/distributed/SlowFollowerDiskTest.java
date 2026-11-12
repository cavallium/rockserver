package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ratis.server.raftlog.segmented.LogWorkerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Scenario 14 of the design: one follower's disk is slow, not stopped (its log worker sleeps three quarters of the
 * time, {@link LogWorkerGate#pause}), under sustained writes. The leader must stay the leader of the same term, writes
 * must keep being acknowledged in every second (the other follower makes the majority), and the slow follower must
 * really fall behind. Once the disk is fast again the follower catches up.
 *
 * <p>Memory: what the leader keeps of its log in memory is its Ratis log cache, measured directly
 * ({@link LogWorkerGate#cache}). The nodes run with log segments of {@value #SEGMENT_BYTES} bytes, so that the cache's
 * bound (a few segments) is far below what the load writes: a leader that kept entries for the slow follower, or did
 * not evict at all, fails the check. The test also fails if the load wrote too little for the check to be able to
 * fail. The appender's own buffers (bounded by Ratis' outstanding-append and buffer limits) are not measured; the heap
 * of this JVM, shared by three nodes, is only printed.</p>
 */
@Timeout(300)
@Tag("slow")
class SlowFollowerDiskTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	private static final int WRITERS = 8;
	private static final int VALUE_BYTES = 8 * 1024;
	private static final int LOAD_SECONDS = Integer.getInteger("rockserver.test.slowDiskSeconds", 20);
	/** Each pause of the slow worker, and the time it then runs: about a tenth of a disk. */
	private static final long PAUSE_MILLIS = 180;
	private static final long RUN_MILLIS = 20;
	/** Log segment size of the nodes: small, so that the leader's log cache must evict during the load. */
	private static final long SEGMENT_BYTES = 1L << 20;
	/**
	 * Segments over Ratis' cached-segment limit the leader's cache may hold at a sample: the open segment, one closed
	 * segment awaiting eviction, and one read back from disk for the slow follower.
	 */
	private static final int SEGMENTS_OVER_LIMIT = 3;

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aSlowFollowerDiskKeepsTheLeaderLiveAndItsMemoryBounded() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withLogSegmentBytes(SEGMENT_BYTES)).startAll();
		var leader = cluster.awaitLeader(Duration.ofSeconds(30));
		var slow = cluster.anyFollower();
		long term = leader.stateMachine().division().getInfo().getCurrentTerm();
		var api = leader.connection().getSyncApi(CONTEXT);
		long column = api.createColumn("c", SCHEMA);

		var stop = new AtomicBoolean();
		var slowing = Thread.ofPlatform().name("slow-disk").start(() -> {
			try {
				var log = slow.stateMachine().division().getRaftLog();
				while (!stop.get()) {
					LogWorkerGate.pause(log, PAUSE_MILLIS).join();
					Thread.sleep(RUN_MILLIS);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ReflectiveOperationException e) {
				throw new IllegalStateException(e);
			}
		});

		var perSecond = new AtomicLongArray(LOAD_SECONDS + 1);
		var bytes = new AtomicLong();
		var failures = new ConcurrentLinkedQueue<Throwable>();
		long start = System.nanoTime();
		long end = start + Duration.ofSeconds(LOAD_SECONDS).toNanos();
		var writers = new ArrayList<Thread>();
		for (int w = 0; w < WRITERS; w++) {
			long base = (long) w << 40;
			writers.add(Thread.ofPlatform().name("writer-" + w).start(() -> {
				var value = new byte[VALUE_BYTES];
				try {
					for (long i = 0; System.nanoTime() - end < 0; i++) {
						ByteBuffer.wrap(value).putLong(base | i);
						api.put(0, column, key(base | i), Buf.wrap(value), RequestType.none());
						bytes.addAndGet(VALUE_BYTES);
						int second = (int) ((System.nanoTime() - start) / 1_000_000_000L);
						perSecond.incrementAndGet(Math.min(second, LOAD_SECONDS));
					}
				} catch (Throwable e) {
					failures.add(e);
				}
			}));
		}
		// How far the slow follower falls behind, and how much of its log the leader keeps in memory, sampled while the
		// load runs.
		var leaderLog = leader.stateMachine().division().getRaftLog();
		long maxLag = 0;
		long maxLeaderCache = 0;
		long maxCachedSegments = LogWorkerGate.cache(leaderLog)[1];
		long heapAtStart = usedHeapAfterGc();
		while (System.nanoTime() - end < 0) {
			maxLag = Math.max(maxLag, leader.appliedIndex() - slow.appliedIndex());
			maxLeaderCache = Math.max(maxLeaderCache, LogWorkerGate.cache(leaderLog)[0]);
			Thread.sleep(100);
		}
		for (var writer : writers) {
			writer.join();
		}
		long heapAtEnd = usedHeapAfterGc();
		long written = bytes.get();
		var leaderInfo = leader.stateMachine().division().getInfo();
		long slowApplied = slow.appliedIndex();
		long leaderApplied = leader.appliedIndex();
		stop.set(true);
		slowing.join();
		long cacheBound = (maxCachedSegments + SEGMENTS_OVER_LIMIT) * SEGMENT_BYTES;

		var seconds = new ArrayList<Long>();
		for (int i = 0; i < LOAD_SECONDS; i++) {
			seconds.add(perSecond.get(i));
		}
		System.out.println("Writes per second: " + seconds + "; " + (written >> 20) + " MiB written; leader log cache at"
				+ " most " + (maxLeaderCache >> 10) + " KiB (bound " + (cacheBound >> 10) + " KiB); heap " + (heapAtStart >> 20)
				+ " MiB -> " + (heapAtEnd >> 20) + " MiB; slow follower applied " + slowApplied + " of " + leaderApplied
				+ ", at most " + maxLag + " entries behind");
		assertTrue(failures.isEmpty(), () -> "writes failed: " + failures);
		assertTrue(leaderInfo.isLeader(), "the leader stepped down");
		assertEquals(term, leaderInfo.getCurrentTerm(), "an election took place");
		for (int i = 0; i < LOAD_SECONDS; i++) {
			assertTrue(seconds.get(i) > 0, "no write was acknowledged in second " + i + ": " + seconds);
		}
		assertTrue(maxLag >= 10, "the slow follower never fell behind: the scenario did not happen");
		assertTrue(written >= 4 * cacheBound, "the load wrote " + (written >> 20) + " MiB, too little for the leader's"
				+ " cache bound of " + (cacheBound >> 20) + " MiB to matter: the memory check could not fail");
		assertTrue(maxLeaderCache <= cacheBound, "the leader kept " + (maxLeaderCache >> 10) + " KiB of log in memory,"
				+ " more than " + (cacheBound >> 10) + " KiB, while " + (written >> 20) + " MiB were written");

		// Fast again: it catches up.
		cluster.awaitConverged(Duration.ofSeconds(120));
	}

	private static long usedHeapAfterGc() {
		var memory = ManagementFactory.getMemoryMXBean();
		long best = Long.MAX_VALUE;
		for (int i = 0; i < 3; i++) {
			System.gc();
			best = Math.min(best, memory.getHeapMemoryUsage().getUsed());
		}
		return best;
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
