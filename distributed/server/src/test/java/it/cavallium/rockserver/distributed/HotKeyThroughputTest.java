package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/**
 * Updates per second of one key, back to back with pipelined updates off and on, on the same machine: a ratio, not a
 * rate. Slow, and it needs a quiet machine, so it runs only with {@code -Drockserver.test.slow=true}. For real numbers
 * use {@code LoadBench --hot-keys}, on real disks.
 */
@Tag("slow")
@EnabledIfSystemProperty(named = "rockserver.test.slow", matches = "true")
class HotKeyThroughputTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	private static final Duration WARMUP = Duration.ofSeconds(3);
	private static final Duration MEASURED = Duration.ofSeconds(5);
	private static final int CLIENTS = 32;
	/** Far below what the design expects (5 to 20 times): only says that the key no longer waits for the apply. */
	private static final double MINIMUM_RATIO = 2;

	@TempDir
	Path temp;

	@Test
	void aHotKeySustainsMoreUpdatesWithThePipeline() throws Exception {
		double off = updatesPerSecond(temp.resolve("off"), false, CLIENTS, null);
		double on = updatesPerSecond(temp.resolve("on"), true, CLIENTS, null);
		System.out.printf("Updates of one key per second: %.0f off, %.0f on, ratio %.1f%n", off, on, on / off);
		assertTrue(on / off >= MINIMUM_RATIO, () -> "off " + off + "/s, on " + on + "/s");
	}

	/**
	 * How much room a client that runs transactions on a key in a tight loop gets from the updates of that key, and what
	 * it costs them, a number and no check: a transaction conflicts with every commit of the key after its snapshot, and
	 * with a chain there are more of them.
	 */
	@Test
	void transactionsOnAHotKey() throws Exception {
		for (int clients : new int[] {1, 4, CLIENTS}) {
			for (boolean pipelined : new boolean[] {false, true}) {
				var result = new Transactions();
				double updates = updatesPerSecond(temp.resolve("tx-" + clients + "-" + pipelined), pipelined, clients, result);
				System.out.printf("Transactions on a key beside %d update clients, pipelined %s: %.0f updates/s, %.1f commits/s,"
								+ " %.1f failures/s, %.1f conflicts on unsettled links/s%n", clients, pipelined, updates,
						result.commits.get() / MEASURED_SECONDS, result.failures.get() / MEASURED_SECONDS,
						result.chainConflicts / MEASURED_SECONDS);
			}
		}
	}

	private static final double MEASURED_SECONDS = MEASURED.toMillis() / 1000.0;

	/** What one client running transactions on the hot key got. */
	private static final class Transactions {
		final AtomicLong commits = new AtomicLong();
		final AtomicLong failures = new AtomicLong();
		long chainConflicts;
	}

	/** Runs {@code clients} clients updating one key, and one client of {@code transactions} if it is not null. */
	private static double updatesPerSecond(Path root, boolean pipelined, int clients, Transactions transactions)
			throws Exception {
		java.nio.file.Files.createDirectories(root);
		try (var cluster = new InProcessCluster(root, 3, c -> c.withPipelinedUpdates(pipelined)).startAll()) {
			var leader = cluster.awaitLeader(Duration.ofSeconds(30));
			var api = leader.connection().getSyncApi(CONTEXT);
			long column = api.createColumn("c", SCHEMA);
			var key = new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(1).array()));
			var value = Buf.wrap("0".repeat(600).getBytes(StandardCharsets.UTF_8));
			var stop = new AtomicBoolean();
			var done = new AtomicLong();
			var measuring = new AtomicBoolean();
			var chainConflicts = leader.metrics().amount("transaction.chainConflict");
			try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
				var workers = new ArrayList<Future<?>>();
				if (transactions != null) {
					workers.add(executor.submit(() -> {
						while (!stop.get()) {
							long transaction = api.openTransaction(Duration.ofSeconds(10));
							boolean committed;
							try {
								api.put(transaction, column, key, value, RequestType.none());
								committed = api.closeTransaction(transaction, true);
							} catch (RocksDBRetryException refused) {
								// Commits this fast also fill Ratis' queue of pending requests.
								committed = false;
							}
							if (!committed) {
								api.closeTransaction(transaction, false);
							}
							if (measuring.get()) {
								(committed ? transactions.commits : transactions.failures).incrementAndGet();
							}
						}
					}));
				}
				for (int i = 0; i < clients; i++) {
					workers.add(executor.submit(() -> {
						while (!stop.get()) {
							try {
								var update = api.get(0, column, key, RequestType.<Buf>forUpdate());
								api.put(update.updateId(), column, key, value, RequestType.none());
								done.incrementAndGet();
							} catch (RocksDBRetryException retry) {
								// Counts for nothing.
							}
						}
					}));
				}
				Thread.sleep(WARMUP);
				long before = done.get();
				long chainConflictsBefore = chainConflicts.count();
				long start = System.nanoTime();
				measuring.set(true);
				Thread.sleep(MEASURED);
				measuring.set(false);
				double rate = (done.get() - before) / ((System.nanoTime() - start) / 1e9);
				if (transactions != null) {
					transactions.chainConflicts = chainConflicts.count() - chainConflictsBefore;
				}
				stop.set(true);
				for (var worker : workers) {
					worker.get();
				}
				return rate;
			}
		}
	}
}
