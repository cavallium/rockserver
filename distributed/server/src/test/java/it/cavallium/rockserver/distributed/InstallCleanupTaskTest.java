package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * R24 review: the cleanup of failed snapshot installations runs only as one task scheduled with a fixed delay on the
 * install cleanup thread ({@code ReplicatedNode.discardFailedInstalls}); Ratis no longer runs it. A
 * {@link java.util.concurrent.ScheduledExecutorService} cancels every later run of a task whose run throws, so a run
 * that fails with an {@link Error} (an {@link OutOfMemoryError} in a walk of a large installation, or when the virtual
 * thread that deletes a directory cannot start) must still be logged and followed by the next run.
 */
class InstallCleanupTaskTest {

	@Test
	void anErrorInOneRunDoesNotStopTheNextRuns() throws InterruptedException {
		var runs = new AtomicInteger();
		var later = new CountDownLatch(3);
		var executor = Executors.newSingleThreadScheduledExecutor();
		try {
			executor.scheduleWithFixedDelay(() -> ReplicatedNode.runInstallCleanup("n1", () -> {
				if (runs.incrementAndGet() == 1) {
					throw new OutOfMemoryError("test: Java heap space");
				}
				later.countDown();
			}), 0, 10, TimeUnit.MILLISECONDS);
			assertTrue(later.await(10, TimeUnit.SECONDS),
					"the install cleanup stopped after a run that threw an Error (runs: " + runs.get() + ")");
		} finally {
			executor.shutdownNow();
		}
	}
}
