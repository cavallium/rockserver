package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GrpcBatchCancellationIntegrationTest {

	private static final int CANCELLED_PUTS = 12;

	@TempDir
	Path tempDir;

	@Test
	void queuedBatchPutsCancelledOverGrpcAreRemovedExactlyAndNeverWritten() throws Exception {
		Path config = tempDir.resolve("rockserver.conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: {
				      batch-queue-capacity: 64
				      competing-batch-read-maximum-active: 3
				    }
				  }
				  global: { ingest-behind: false, optimistic: false }
				}
				""");
		EmbeddedConnection embedded = new EmbeddedConnection(tempDir.resolve("db"),
				"grpc-batch-cancellation", config);
		GrpcServer server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
		GrpcConnection client = null;
		var releaseWrites = new CountDownLatch(1);
		try {
			server.start();
			client = GrpcConnection.forHostAndPort("grpc-batch-cancellation-client",
					new Utils.HostAndPort("127.0.0.1", server.getPort()));
			long columnId = client.getSyncApi(RequestContext.batch()).createColumn("cancelled-batch",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			RWScheduler scheduler = embedded.getScheduler();
			int writeWorkers = scheduler.poolSnapshot(RWScheduler.Pool.WRITE).workerCount();
			var writesStarted = new CountDownLatch(writeWorkers);
			for (int index = 0; index < writeWorkers; index++) {
				scheduler.writeExecutor().execute(() -> {
					writesStarted.countDown();
					awaitUninterruptibly(releaseWrites);
				});
			}
			assertTrue(writesStarted.await(5, TimeUnit.SECONDS));
			await(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE)
					.activeByProfile().getOrDefault(WorkloadProfile.INGEST, 0) == writeWorkers,
					"INGEST blockers did not occupy every WRITE worker");

			RWScheduler.PoolSnapshot before = scheduler.poolSnapshot(RWScheduler.Pool.WRITE);
			var batch = client.getAsyncApi(RequestContext.batch());
			var keys = new ArrayList<Keys>(CANCELLED_PUTS);
			var futures = new ArrayList<CompletableFuture<Void>>(CANCELLED_PUTS);
			for (int index = 0; index < CANCELLED_PUTS; index++) {
				Keys key = key(10_000L + index);
				keys.add(key);
				futures.add(batch.putAsync(0L, columnId, key,
						Buf.wrap(new byte[] {(byte) index}), RequestType.none()));
			}
			await(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE)
					.queuedByProfile().getOrDefault(WorkloadProfile.BATCH, 0) == CANCELLED_PUTS,
					"BATCH puts did not all reach the saturated server queue");
			RWScheduler.PoolSnapshot queued = scheduler.poolSnapshot(RWScheduler.Pool.WRITE);
			assertEquals(CANCELLED_PUTS, queued.submissionAttempts() - before.submissionAttempts());
			assertEquals(CANCELLED_PUTS, queued.acceptedTasks() - before.acceptedTasks());
			assertEquals(CANCELLED_PUTS,
					queued.submissionAttemptsByProfile().get(WorkloadProfile.BATCH)
							- before.submissionAttemptsByProfile().get(WorkloadProfile.BATCH));
			assertEquals(CANCELLED_PUTS,
					queued.outstandingByProfile().get(WorkloadProfile.BATCH)
							- before.outstandingByProfile().get(WorkloadProfile.BATCH));
			assertEquals(before.startedTasks(), queued.startedTasks(),
					"queued BATCH puts must not claim dispatch while every worker is blocked");

			for (CompletableFuture<Void> future : futures) {
				assertTrue(future.cancel(true));
			}
			await(() -> {
				RWScheduler.PoolSnapshot snapshot = scheduler.poolSnapshot(RWScheduler.Pool.WRITE);
				return snapshot.queuedByProfile().getOrDefault(WorkloadProfile.BATCH, 0) == 0
						&& snapshot.outstandingByProfile().getOrDefault(WorkloadProfile.BATCH, 0)
						== before.outstandingByProfile().getOrDefault(WorkloadProfile.BATCH, 0)
						&& outcome(snapshot, RWScheduler.TerminalOutcome.CANCELLATION)
						- outcome(before, RWScheduler.TerminalOutcome.CANCELLATION) == CANCELLED_PUTS;
			}, "server-side BATCH cancellation/removal did not reach the exact terminal count");
			RWScheduler.PoolSnapshot cancelled = scheduler.poolSnapshot(RWScheduler.Pool.WRITE);
			assertEquals(before.startedTasks(), cancelled.startedTasks());
			assertEquals(before.completedTasks(), cancelled.completedTasks());
			assertEquals(CANCELLED_PUTS,
					outcome(cancelled, RWScheduler.TerminalOutcome.CANCELLATION)
							- outcome(before, RWScheduler.TerminalOutcome.CANCELLATION));

			releaseWrites.countDown();
			await(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).drainedAndConserved(),
					"WRITE pool did not drain and conserve terminal outcomes");
			var read = client.getSyncApi(RequestContext.latency(java.time.Duration.ofSeconds(5)));
			for (Keys key : keys) {
				assertFalse(read.get(0L, columnId, key, RequestType.exists()),
						"a cancelled queued BATCH put reached RocksDB");
			}
		} finally {
			releaseWrites.countDown();
			if (client != null) client.close();
			server.close();
			embedded.closeTesting();
		}
	}

	private static long outcome(RWScheduler.PoolSnapshot snapshot, RWScheduler.TerminalOutcome outcome) {
		return snapshot.outcomes().getOrDefault(outcome, 0L);
	}

	private static void await(BooleanSupplier condition, String failure) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
		do {
			if (condition.getAsBoolean()) return;
			Thread.sleep(10L);
		} while (System.nanoTime() < deadline);
		throw new AssertionError(failure);
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) Thread.currentThread().interrupt();
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
