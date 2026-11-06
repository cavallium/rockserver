package it.cavallium.rockserver.distributed;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** DD-9: the probe of a hung disk. */
class DiskProbeTest {

	@TempDir
	Path temp;

	@Test
	void aProbeThatDoesNotEndWithinTheLimitMeansAHungDisk() throws Exception {
		var disk = new CountDownLatch(1);
		var started = new CountDownLatch(2);
		var probes = new AtomicInteger();
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
		}, Duration.ofMillis(10), System::nanoTime).start()) {
			assertTrue(started.await(10, TimeUnit.SECONDS));
			long limit = Duration.ofMillis(200).toNanos();
			assertFalse(probe.hung(System.nanoTime(), limit), "not yet");
			Thread.sleep(400);
			assertTrue(probe.hung(System.nanoTime(), limit), "the second probe hangs");
			disk.countDown();
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (probe.hung(System.nanoTime(), limit) && System.nanoTime() - deadline < 0) {
				Thread.sleep(5);
			}
			assertFalse(probe.hung(System.nanoTime(), limit), "it answers again");
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
