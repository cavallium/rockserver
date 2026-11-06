package it.cavallium.rockserver.distributed;

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
 * must keep being acknowledged in every second (the other follower makes the majority), and the leader's memory must
 * stay bounded while the slow follower falls behind: the heap may not grow by anything like the bytes written, which
 * it would if the leader held the entries that the slow follower has not taken yet. Once the disk is fast again the
 * follower catches up.
 */
@Timeout(300)
class SlowFollowerDiskTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	private static final int WRITERS = 8;
	private static final int VALUE_BYTES = 8 * 1024;
	private static final int LOAD_SECONDS = Integer.getInteger("rockserver.test.slowDiskSeconds", 20);
	/** Each pause of the slow worker, and the time it then runs. */
	private static final long PAUSE_MILLIS = 30;
	private static final long RUN_MILLIS = 10;

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
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
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
		// The heap once the load runs, and at its end, each after a full collection.
		Thread.sleep(Duration.ofSeconds(LOAD_SECONDS / 4));
		long bytesAtFirst = bytes.get();
		long heapAtFirst = usedHeapAfterGc();
		for (var writer : writers) {
			writer.join();
		}
		long heapAtEnd = usedHeapAfterGc();
		long bytesInBetween = bytes.get() - bytesAtFirst;
		var leaderInfo = leader.stateMachine().division().getInfo();
		long slowApplied = slow.appliedIndex();
		long leaderApplied = leader.appliedIndex();
		stop.set(true);
		slowing.join();

		var seconds = new ArrayList<Long>();
		for (int i = 0; i < LOAD_SECONDS; i++) {
			seconds.add(perSecond.get(i));
		}
		System.out.println("Writes per second: " + seconds + "; heap " + (heapAtFirst >> 20) + " MiB -> "
				+ (heapAtEnd >> 20) + " MiB while " + (bytesInBetween >> 20) + " MiB were written; slow follower applied "
				+ slowApplied + " of " + leaderApplied);
		assertTrue(failures.isEmpty(), () -> "writes failed: " + failures);
		assertTrue(leaderInfo.isLeader(), "the leader stepped down");
		assertEquals(term, leaderInfo.getCurrentTerm(), "an election took place");
		for (int i = 0; i < LOAD_SECONDS; i++) {
			assertTrue(seconds.get(i) > 0, "no write was acknowledged in second " + i + ": " + seconds);
		}
		assertTrue(heapAtEnd - heapAtFirst < Math.max(64L << 20, bytesInBetween / 2),
				"the heap grew by " + ((heapAtEnd - heapAtFirst) >> 20) + " MiB while " + (bytesInBetween >> 20)
						+ " MiB were written");

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
