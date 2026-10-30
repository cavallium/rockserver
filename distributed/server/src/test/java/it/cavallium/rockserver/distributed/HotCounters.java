package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import org.jetbrains.annotations.Nullable;

/**
 * Workers incrementing a few counters with {@code getForUpdate} and a {@code put} on the update's id, under whatever
 * faults a test injects, so that updates of hot keys queue or, with pipelined updates, chain. Some updates end with a
 * {@code closeFailedUpdate}, which writes nothing but tells that what the update read was real.
 *
 * <p>{@link #check} asserts what must hold however the faults went: no acknowledged increment was lost and none
 * was invented (each counter is between its acknowledged increments and those plus the unknown ones), and the
 * history of every counter is linearizable. An update counts as one read-modify-write, a successful close as a read,
 * and an update that failed as nothing: what it read may never have been written.</p>
 */
final class HotCounters {

	private static final RequestContext CONTEXT = RequestContext.latency(Duration.ofSeconds(5));
	/** One update in this many closes instead of writing. */
	private static final int CLOSE_ONE_IN = 6;
	/** Past this, nothing a worker left behind holds a key: longer than an update's lease, plus its expiry. */
	private static final Duration LEASE = Duration.ofSeconds(11);
	/** Well under a lease: a counter that takes longer has a wedged key. */
	private static final Duration MAX_UPDATE_TIME = Duration.ofSeconds(5);

	private final Map<String, HostAndPort> addresses;
	private final long columnId;
	private final long firstKey;
	private final int counters;
	private final int workers;
	private final History history = new History();
	private final AtomicLong[] acknowledged;
	private final AtomicLong[] unknown;
	private final AtomicLong closes = new AtomicLong();
	private final AtomicLong closeRetries = new AtomicLong();
	private final AtomicLong retries = new AtomicLong();
	private final AtomicLong chained = new AtomicLong();
	private final ConcurrentLinkedQueue<Throwable> crashes = new ConcurrentLinkedQueue<>();
	private final List<Thread> threads = new ArrayList<>();
	private final List<ClusterClient> clients = new ArrayList<>();
	private volatile boolean running;
	private volatile long stoppedNanos;

	/** Counters at {@code firstKey} and after it, in {@code columnId}, whose key is a {@code long}. */
	HotCounters(Map<String, HostAndPort> addresses, long columnId, long firstKey, int counters, int workers) {
		this.addresses = addresses;
		this.columnId = columnId;
		this.firstKey = firstKey;
		this.counters = counters;
		this.workers = workers;
		this.acknowledged = new AtomicLong[counters];
		this.unknown = new AtomicLong[counters];
		for (int i = 0; i < counters; i++) {
			acknowledged[i] = new AtomicLong();
			unknown[i] = new AtomicLong();
		}
	}

	static Keys key(long key) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(key).array()));
	}

	private static Buf value(long count) {
		return Buf.wrap(Long.toString(count).getBytes(StandardCharsets.UTF_8));
	}

	static long count(@Nullable Buf value) {
		return value == null ? 0 : Long.parseLong(new String(value.toByteArray(), StandardCharsets.UTF_8));
	}

	void start() {
		running = true;
		var nodes = List.copyOf(addresses.keySet());
		for (int i = 0; i < workers; i++) {
			int worker = i;
			var client = new ClusterClient(addresses, Duration.ofSeconds(1), false);
			clients.add(client);
			threads.add(Thread.ofPlatform().daemon().name("hot-counter-" + i).start(() -> {
				try {
					work(client, nodes, worker);
				} catch (Throwable error) {
					crashes.add(error);
				}
			}));
		}
	}

	private void work(ClusterClient client, List<String> nodes, int worker) {
		var random = new Random(worker);
		int node = worker;
		while (running) {
			int counter = random.nextInt(counters);
			if (!update(client, nodes.get(node % nodes.size()), counter, random.nextInt(CLOSE_ONE_IN) == 0)) {
				// This node cannot serve updates now: the leader changed or it is cut off. Find another.
				node++;
				pause();
			}
		}
	}

	/** One update attempt; false if it found no leader there. */
	private boolean update(ClusterClient client, String node, int counter, boolean close) {
		var name = "hot" + firstKey + "-" + counter;
		var keys = key(firstKey + counter);
		long invoked = System.nanoTime();
		RocksDBSyncAPI api;
		UpdateContext<Buf> update;
		try {
			api = client.api(node, CONTEXT);
			update = api.get(0, columnId, keys, RequestType.<Buf>forUpdate());
		} catch (RuntimeException ex) {
			// Nothing was written, whatever failed.
			retries.incrementAndGet();
			return false;
		}
		if (update.updateId() > 0) {
			chained.incrementAndGet();
		}
		long previous = count(update.previous());
		var read = update.previous() == null ? null : String.valueOf(previous);
		if (close) {
			var pending = history.invokeGetAt(name, invoked);
			try {
				api.closeFailedUpdate(update.updateId());
				closes.incrementAndGet();
				pending.complete(OpStatus.OK, read);
			} catch (RuntimeException ex) {
				closeRetries.incrementAndGet();
				pending.complete(OpStatus.FAILED, null);
				return false;
			}
			return true;
		}
		var pending = history.invokeUpdateAt(name, read, String.valueOf(previous + 1), invoked);
		try {
			api.put(update.updateId(), columnId, keys, value(previous + 1), RequestType.none());
			pending.complete(OpStatus.OK);
			acknowledged[counter].incrementAndGet();
			return true;
		} catch (RuntimeException ex) {
			if (isRetry(ex)) {
				// Refused, or rejected by the guard: it wrote nothing.
				pending.complete(OpStatus.FAILED);
				retries.incrementAndGet();
				return true;
			}
			pending.complete(OpStatus.UNKNOWN);
			unknown[counter].incrementAndGet();
			return false;
		}
	}

	/** True for the failure of a commit that wrote nothing. */
	static boolean isRetry(RuntimeException error) {
		return error instanceof RocksDBException rocks && rocks.getErrorUniqueId() == RocksDBErrorType.UPDATE_RETRY;
	}

	private static void pause() {
		try {
			Thread.sleep(20);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	/** Stops the workers. */
	void stop() throws InterruptedException {
		running = false;
		for (var thread : threads) {
			thread.join(30_000);
		}
		clients.forEach(ClusterClient::close);
		stoppedNanos = System.nanoTime();
	}

	long acknowledged() {
		long total = 0;
		for (var count : acknowledged) {
			total += count.get();
		}
		return total;
	}

	/**
	 * Fails if a counter does not take a new update soon, on a settled cluster: faults must leave no key wedged. Plain
	 * reads ignore key locks, so nothing else shows that.
	 */
	private void assertKeysAreFree() throws InterruptedException {
		long sinceStop = System.nanoTime() - stoppedNanos;
		Thread.sleep(Duration.ofNanos(Math.max(0, LEASE.toNanos() - sinceStop)));
		var nodes = List.copyOf(addresses.keySet());
		try (var client = new ClusterClient(addresses, Duration.ofSeconds(1), false)) {
			for (int i = 0; i < counters; i++) {
				long deadline = System.nanoTime() + MAX_UPDATE_TIME.toNanos();
				var keys = key(firstKey + i);
				for (int node = 0; ; node++) {
					try {
						var api = client.api(nodes.get(node % nodes.size()), RequestContext.latency(MAX_UPDATE_TIME));
						api.closeFailedUpdate(api.get(0, columnId, keys, RequestType.<Buf>forUpdate()).updateId());
						break;
					} catch (RuntimeException ex) {
						if (System.nanoTime() > deadline) {
							throw new AssertionError("Counter " + i + " takes no update: " + this, ex);
						}
						pause();
					}
				}
			}
		}
	}

	/**
	 * Checks the counters, with their final values from {@code finalValue}, the history, and that no key was left
	 * wedged. Workers must be stopped, and the cluster settled.
	 *
	 * @param finalValue the database value of a key, read once the cluster settled
	 * @param minimumAcknowledged how many increments must have gone through for this to prove anything
	 * @param pipelined whether the cluster has pipelined updates, so that some update must have been chained
	 */
	void check(LongFunction<@Nullable Buf> finalValue, long minimumAcknowledged, boolean pipelined)
			throws InterruptedException {
		assertTrue(crashes.isEmpty(), () -> "Worker failures: " + crashes);
		assertTrue(acknowledged() >= minimumAcknowledged,
				() -> "Only " + acknowledged() + " increments acknowledged, " + this);
		assertTrue(!pipelined || chained.get() > 0, () -> "Pipelined updates, but no update was chained: " + this);
		assertKeysAreFree();
		for (int i = 0; i < counters; i++) {
			long value = count(finalValue.apply(firstKey + i));
			long low = acknowledged[i].get();
			long high = low + unknown[i].get();
			assertTrue(value >= low && value <= high, "Counter " + i + " holds " + value + ", expected between " + low
					+ " (acknowledged) and " + high + " (with the unknown ones): " + this);
		}
		var result = LinearizabilityChecker.check(history.operations());
		assertTrue(result.linearizable(), result::description);
		System.out.println("Hot counters: " + this);
	}

	@Override
	public String toString() {
		return "acknowledged=" + acknowledged() + " chained=" + chained + " closes=" + closes + " closeRetries="
				+ closeRetries + " retries=" + retries + " unknown=" + List.of(unknown);
	}
}
