package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** DD-9: the probe of a hung disk. */
@Tag("slow")
class DiskProbeTest {

	@TempDir
	Path temp;

	/**
	 * R35-3: the probe reads a fake clock, so "not yet" and "hung" are decided by the clock values the test gives, not by
	 * how soon the test thread wakes up. Before, "not yet" was checked against the real clock and failed whenever the
	 * test thread woke up more than 200 ms after the second probe started (a GC pause, a loaded host).
	 */
	@Test
	void aProbeThatDoesNotEndWithinTheLimitMeansAHungDisk() throws Exception {
		var disk = new CountDownLatch(1);
		var started = new CountDownLatch(2);
		var probes = new AtomicInteger();
		long start = 1_000_000;
		var clock = new AtomicLong(start);
		try (var probe = new DiskProbe("test", () -> {
			probes.incrementAndGet();
			started.countDown();
			if (probes.get() == 2) {
				try {
					disk.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}, Duration.ofMillis(10), clock::get).start()) {
			assertTrue(started.await(10, TimeUnit.SECONDS));
			long limit = Duration.ofMillis(200).toNanos();
			// The second probe started at the clock's value, which has not moved since.
			assertFalse(probe.hung(start + limit, limit), "not yet");
			assertTrue(probe.hung(start + limit + 1, limit), "the second probe hangs");
			long later = start + limit + 1;
			clock.set(later);
			disk.countDown();
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			// The third probe starts once the second ended: at the clock's new value, so it is not hung at that time.
			while (probes.get() < 3 && System.nanoTime() - deadline < 0) {
				Thread.sleep(5);
			}
			assertTrue(probes.get() >= 3, "the second probe did not end");
			assertFalse(probe.hung(later, limit), "it answers again");
		}
	}

	/**
	 * R25 review: a probe that throws an Error once (out of memory) does not end the probe thread; before, the thread
	 * died, the probe looked idle for good and a disk that hung later never paused the API.
	 */
	@Test
	void anErrorInOneProbeDoesNotStopTheNextOnes() throws Exception {
		var disk = new CountDownLatch(1);
		var probes = new AtomicInteger();
		try (var probe = new DiskProbe("test", () -> {
			int n = probes.incrementAndGet();
			if (n == 1) {
				throw new OutOfMemoryError("test: direct buffer memory");
			}
			if (n == 3) {
				try {
					disk.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}, Duration.ofMillis(10), System::nanoTime).start()) {
			long limit = Duration.ofMillis(200).toNanos();
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (!probe.hung(System.nanoTime(), limit) && System.nanoTime() - deadline < 0) {
				Thread.sleep(20);
			}
			assertTrue(probe.hung(System.nanoTime(), limit), "the probe stopped after an Error (probes: " + probes.get() + ")");
			disk.countDown();
		}
	}

	@Test
	void theProbeOfADirectoryWritesAndSyncsAFileThere() throws Exception {
		try (var probe = DiskProbe.forDirectory("test", temp).start()) {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (!Files.exists(temp.resolve(DiskProbe.FILE_NAME)) && System.nanoTime() - deadline < 0) {
				Thread.sleep(5);
			}
			assertTrue(Files.exists(temp.resolve(DiskProbe.FILE_NAME)));
			assertFalse(probe.hung(System.nanoTime(), Duration.ofSeconds(5).toNanos()));
		}
	}

	/**
	 * R3-G2a-3, in process: a node whose disk probe hangs (a test hook in place of a hung fsync) stops listening on its
	 * API port once the request timeout passed, the other nodes go on serving, a client given every address goes on
	 * working, and the node listens again once the probe ends. The leader stepping down on a hung log is the process
	 * test's ({@code HungDiskTest}): here only the probe hangs.
	 */
	@Test
	@Timeout(120)
	void aNodeWhoseDiskProbeHangsPausesItsApiUntilTheProbeEnds() throws Exception {
		var timeout = Duration.ofSeconds(30);
		try (var cluster = new InProcessCluster(Files.createDirectory(temp.resolve("cluster")), 3,
				c -> c.withRequestTimeout(Duration.ofSeconds(2))).startAll()) {
			var leader = cluster.awaitLeader(timeout);
			var api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
			long column = api.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			var victim = cluster.anyFollower();
			var others = cluster.runningNodes().stream().filter(node -> node != victim).toList();
			var hang = new CountDownLatch(1);
			var key = victim.config().dataDir().toAbsolutePath().normalize();
			DiskProbe.HANGS_FOR_TESTING.put(key, hang);
			try {
				var addresses = cluster.runningNodes().stream()
						.map(node -> new HostAndPort(node.config().self().apiHost(), node.config().self().apiPort()))
						.toList();
				InProcessCluster.await(timeout, () -> !listens(victim));
				for (var node : others) {
					assertTrue(listens(node), node.config().nodeId() + " paused too");
				}
				try (var client = GrpcConnection.forHostsAndPorts("hung-disk-client", addresses)) {
					var sync = client.getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
					sync.put(0, column, new Keys(Buf.wrap(new byte[Long.BYTES])), Buf.wrap(new byte[] {1}),
							RequestType.none());
				}
				// Still paused while the probe hangs.
				Thread.sleep(1_000);
				assertFalse(listens(victim), "served again while the disk hangs");
			} finally {
				hang.countDown();
				DiskProbe.HANGS_FOR_TESTING.remove(key);
			}
			InProcessCluster.await(timeout, () -> listens(victim));
		}
	}

	private static boolean listens(ReplicatedNode node) {
		var self = node.config().self();
		try (var socket = new Socket()) {
			socket.connect(new InetSocketAddress(self.apiHost(), self.apiPort()), 1_000);
			return true;
		} catch (IOException refused) {
			return false;
		}
	}
}
