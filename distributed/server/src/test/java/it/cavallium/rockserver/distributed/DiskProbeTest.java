package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
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
}
