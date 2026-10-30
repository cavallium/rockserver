package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A client attached to a follower while the leader is killed (SIGKILL): the follower forwards its reads and
 * proposes its writes to whichever node leads. The client must only ever see retryable errors, never a transport
 * or internal error from the forwarding, and reads must come back soon after the election.
 */
@Timeout(180)
class ForwardingFailoverTest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.latency(Duration.ofSeconds(8));
	private static final int KEYS = 64;

	@TempDir
	Path root;
	private ProcessCluster cluster;

	@AfterEach
	void tearDown() {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aFollowersClientSeesOnlyRetryableErrorsWhileTheLeaderIsKilled() throws Exception {
		cluster = ProcessCluster.start(root.resolve("cluster"), 3, List.of(
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000"));
		cluster.waitReady(READY_TIMEOUT);
		var members = List.of("1", "2", "3");
		var leader = leaderOf(members);
		var follower = members.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();

		try (var connection = GrpcConnection.forHostAndPort("follower-client", cluster.apiAddress(follower))) {
			var api = connection.getAsyncApi(CONTEXT);
			long column = api.createColumnAsync("data", SCHEMA).join();
			for (int key = 0; key < KEYS; key++) {
				api.putAsync(0, column, key(key), Buf.wrap(new byte[] {1}), RequestType.none()).join();
			}

			var stop = new AtomicBoolean();
			var unexpected = new ConcurrentHashMap<String, String>();
			var retryable = new AtomicLong();
			var lastReadNanos = new AtomicLong();
			var lastWriteNanos = new AtomicLong();
			var workers = new ArrayList<Thread>();
			for (int i = 0; i < 8; i++) {
				boolean reader = i % 2 == 0;
				workers.add(Thread.ofVirtual().start(() -> {
					var random = ThreadLocalRandom.current();
					while (!stop.get()) {
						try {
							if (reader) {
								api.getAsync(0, column, key(random.nextInt(KEYS)), RequestType.current()).join();
								lastReadNanos.set(System.nanoTime());
							} else {
								api.putAsync(0, column, key(random.nextInt(KEYS)), Buf.wrap(new byte[] {2}),
										RequestType.none()).join();
								lastWriteNanos.set(System.nanoTime());
							}
						} catch (CompletionException | RocksDBException e) {
							var cause = e instanceof CompletionException && e.getCause() != null ? e.getCause() : e;
							if (cause instanceof RocksDBException rocks
									&& (rocks.getErrorUniqueId() == RocksDBErrorType.SERVER_OVERLOADED
									|| rocks.getErrorUniqueId() == RocksDBErrorType.UPDATE_RETRY)) {
								retryable.incrementAndGet();
							} else {
								unexpected.putIfAbsent(String.valueOf(cause), String.valueOf(cause.getMessage()));
							}
						}
					}
				}));
			}

			Thread.sleep(1000);
			long killedAt = System.nanoTime();
			cluster.kill(leader);
			Thread.sleep(6000);
			stop.set(true);
			for (var worker : workers) {
				worker.join();
			}

			System.out.println("ForwardingFailoverTest: retryable errors " + retryable.get() + ", reads resumed "
					+ Duration.ofNanos(lastReadNanos.get() - killedAt).toMillis() + " ms and writes "
					+ Duration.ofNanos(lastWriteNanos.get() - killedAt).toMillis() + " ms after the kill (last success)");
			assertTrue(unexpected.isEmpty(), () -> "non-retryable errors through the follower: " + unexpected);
			// Requests kept succeeding until the end, well after the election.
			assertTrue(lastReadNanos.get() - killedAt > Duration.ofSeconds(4).toNanos(), "reads did not resume");
			assertTrue(lastWriteNanos.get() - killedAt > Duration.ofSeconds(4).toNanos(), "writes did not resume");
		}
	}

	private String leaderOf(List<String> members) throws Exception {
		var status = cluster.admin(members, "status");
		return status.lines()
				.filter(line -> line.contains(": LEADER"))
				.map(line -> line.substring(0, line.indexOf(':')))
				.findFirst()
				.orElseThrow(() -> new AssertionError("No leader in status:\n" + status));
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
