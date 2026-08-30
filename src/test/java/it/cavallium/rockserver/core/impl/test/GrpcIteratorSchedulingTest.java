package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class GrpcIteratorSchedulingTest {

	@TempDir
	Path tempDir;

	@Test
	void nonMaterializingRemoteAdvanceUsesOneLogicalTaskWithBoundedNativeSteps() throws Exception {
		final int entries = 4_200;
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "grpc-iterator-scheduling", null)) {
			var backend = embedded.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
			long columnId = backend.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < entries; i++) {
				backend.put(0, columnId, key(i), value(i), RequestType.none());
			}

			try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				try (var client = GrpcConnection.forHostAndPort("grpc-iterator-scheduling",
						new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
					var api = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
					long iteratorId = api.openIterator(0, columnId, null, null, false, java.time.Duration.ofMillis( 10_000));
					try {
						var scheduler = embedded.getInternalDB().getScheduler();
						long tasksBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
						var nativeSteps = new AtomicInteger();
						embedded.getInternalDB()
								.setIteratorAdvanceStepCompletedObserverForTesting(nativeSteps::incrementAndGet);

						try {
							api.subsequent(iteratorId, 0, entries, RequestType.none());
						} finally {
							embedded.getInternalDB().setIteratorAdvanceStepCompletedObserverForTesting(null);
						}

						long scheduledTasks = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - tasksBefore;
						assertEquals(1L, scheduledTasks,
								"gRPC must preserve one logical iterator continuation");
						assertEquals(2, nativeSteps.get(),
								"4,200 entries must still use bounded 4,096-entry native steps");
					} finally {
						api.closeIterator(iteratorId);
					}
				}
			}
			assertEquals(0, embedded.getInternalDB().getOpenIteratorsCount());
			assertEquals(0L, embedded.getInternalDB().getPendingOpsCount());
		}
	}

	@Test
	void remoteMultiUsesEmbeddedConfiguredByteQuantumUnderCompetition() throws Exception {
		final int entries = 20;
		String database = "grpc-iterator-quantum";
		var config = tempDir.resolve("grpc-iterator-quantum.conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: {
				      competing-batch-read-maximum-active: 3
				      range-quantum-max-items: 4096
				      range-quantum-max-bytes: 16KiB
				      range-quantum-max-duration: PT0.008S
				    }
				  }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		try (var embedded = new EmbeddedConnection(tempDir.resolve("grpc-quantum-db"), database, config)) {
			var backend = embedded.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
			long columnId = backend.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < entries; i++) {
				backend.put(0, columnId, key(i), value(i, 8 * 1_024), RequestType.none());
			}

			try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				try (var client = GrpcConnection.forHostAndPort(database,
						new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
					var api = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
					long iteratorId = api.openIterator(0, columnId, null, null, false, java.time.Duration.ofMillis( 10_000));
					var scheduler = embedded.getInternalDB().getScheduler();
					var blockersEntered = new CountDownLatch(2);
					var releaseBlockers = new CountDownLatch(1);
					try {
						occupyLatencyWorkers(scheduler, 2, blockersEntered, releaseBlockers);
						assertTrue(blockersEntered.await(5, SECONDS));
						double quantumsBefore = rangeQuantums(embedded, database);

						var values = api.subsequent(iteratorId, 0L, entries, RequestType.<Buf>multi());

						assertEquals(entries, values.size());
						assertTrue(rangeQuantums(embedded, database) - quantumsBefore >= 10.0,
								"gRPC must preserve byte service bounds below the configured item limit");
					} finally {
						releaseBlockers.countDown();
						api.closeIterator(iteratorId);
					}
				}
			}
		}
	}

	private static void occupyLatencyWorkers(RWScheduler scheduler,
			int workers,
			CountDownLatch entered,
			CountDownLatch release) {
		var executor = scheduler.executor(WorkloadProfile.LATENCY,
				OperationFamily.POINT_LOOKUP,
				scheduler.bindTimeoutNanos(SECONDS.toNanos(30)));
		for (int i = 0; i < workers; i++) {
			executor.execute(() -> {
				entered.countDown();
				awaitUninterruptibly(release);
			});
		}
	}

	private static double rangeQuantums(EmbeddedConnection connection, String database) {
		return connection.getInternalDB().getMetricsRegistry()
				.get("rockserver.workload.quantums")
				.tags("database", database,
						"resource", "read",
						"profile", "batch",
						"operation", "range_page")
				.counter()
				.count();
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException _) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private static Buf value(int value, int bytes) {
		var data = new byte[bytes];
		ByteBuffer.wrap(data).putInt(value);
		return Buf.wrap(data);
	}
}
