package it.cavallium.rockserver.core.impl.test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBOpenProgressMonitor;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RocksDBOpenProgressMonitorTest {

	private static final String DATABASE = "startup-test";

	@Test
	void inventoryIncludesOnlyTopLevelNumericWalFilesInRecoveryOrder(@TempDir Path walDirectory)
			throws Exception {
		Files.write(walDirectory.resolve("000002.log"), new byte[20]);
		Files.write(walDirectory.resolve("000001.log"), new byte[10]);
		Files.write(walDirectory.resolve("LOG"), new byte[40]);
		Files.createDirectories(walDirectory.resolve("archive"));
		Files.write(walDirectory.resolve("archive/000000.log"), new byte[80]);

		var inventory = RocksDBOpenProgressMonitor.scanCandidateWalFiles(walDirectory, mock(Logger.class));

		assertTrue(inventory.available());
		assertEquals(2, inventory.files().size());
		assertEquals(List.of(1L, 2L), inventory.files().stream()
				.map(RocksDBOpenProgressMonitor.WalFile::logNumber)
				.toList());
		assertEquals(List.of(0L, 10L), inventory.files().stream()
				.map(RocksDBOpenProgressMonitor.WalFile::bytesBefore)
				.toList());
		assertEquals(30L, inventory.totalBytes());
	}

	@Test
	void exportsObservedWalAndRecoveryFlushProgressThenSuccess(@TempDir Path directory) {
		var now = new AtomicLong(1_000_000_000L);
		var probe = new AtomicReference<>(new RocksDBOpenProgressMonitor.ProbeSnapshot(true,
				List.of(new RocksDBOpenProgressMonitor.WalPosition(2L, 0L))));
		var threadIo = new AtomicReference<>(
				new RocksDBOpenProgressMonitor.ThreadIoSnapshot(true, 1_000L, 2_000L));
		var recoveryFlushBytes = new AtomicLong();
		var logger = mock(Logger.class);
		var registry = new SimpleMeterRegistry();
		try {
			var monitor = newMonitor(directory, logger, registry, now, probe, threadIo,
					recoveryFlushBytes);
			monitor.start();

			assertEquals(1.0d, gauge(registry, RocksDBOpenProgressMonitor.ACTIVE_METRIC));
			assertEquals(300.0d, gauge(registry, RocksDBOpenProgressMonitor.CANDIDATE_WAL_BYTES_METRIC));
			assertEquals(2.0d, gauge(registry, RocksDBOpenProgressMonitor.CANDIDATE_WAL_FILES_METRIC));

			threadIo.set(new RocksDBOpenProgressMonitor.ThreadIoSnapshot(true, 1_050L, 6_096L));
			now.addAndGet(1_000_000_000L);
			monitor.sampleNow();

			assertEquals(150.0d, gauge(registry, RocksDBOpenProgressMonitor.WAL_PROCESSED_BYTES_METRIC));
			assertEquals(0.5d, gauge(registry, RocksDBOpenProgressMonitor.WAL_PROGRESS_METRIC));
			assertEquals(50.0d, gauge(registry, RocksDBOpenProgressMonitor.WAL_READ_RATE_METRIC));
			assertEquals(1.0d, gauge(registry, RocksDBOpenProgressMonitor.WAL_PROGRESS_AVAILABLE_METRIC));
			assertEquals(1.0d, gauge(registry, RocksDBOpenProgressMonitor.THREAD_IO_AVAILABLE_METRIC));
			assertEquals(50.0d,
					gauge(registry, RocksDBOpenProgressMonitor.THREAD_LOGICAL_READ_BYTES_METRIC));
			assertEquals(4_096.0d,
					gauge(registry, RocksDBOpenProgressMonitor.THREAD_STORAGE_READ_BYTES_METRIC));
			assertEquals(2.0d, gauge(registry, RocksDBOpenProgressMonitor.CURRENT_WAL_NUMBER_METRIC));
			assertEquals(50.0d, gauge(registry, RocksDBOpenProgressMonitor.CURRENT_WAL_OFFSET_METRIC));
			assertEquals(200.0d, gauge(registry, RocksDBOpenProgressMonitor.CURRENT_WAL_SIZE_METRIC));
			assertEquals(1.0d, phaseGauge(registry, "wal-replay"));

			probe.set(new RocksDBOpenProgressMonitor.ProbeSnapshot(true, List.of()));
			recoveryFlushBytes.set(75L);
			now.addAndGet(1_000_000_000L);
			monitor.sampleNow();

			assertEquals(75.0d, gauge(registry, RocksDBOpenProgressMonitor.RECOVERY_FLUSH_BYTES_METRIC));
			assertEquals(1.0d, phaseGauge(registry, "recovery-flush"));
			assertEquals(0.0d,
					gauge(registry, RocksDBOpenProgressMonitor.LAST_ACTIVITY_AGE_METRIC));

			now.addAndGet(5_000_000_000L);
			monitor.sampleNow();
			assertEquals(1.0d, phaseGauge(registry, "native-open-other"));
			assertEquals(5.0d,
					gauge(registry, RocksDBOpenProgressMonitor.LAST_ACTIVITY_AGE_METRIC));

			now.addAndGet(23_000_000_000L);
			monitor.sampleNow();
			verify(logger).info(startsWith("RocksDB native open progress:"), any(Object[].class));

			monitor.succeeded();
			assertEquals(0.0d, gauge(registry, RocksDBOpenProgressMonitor.ACTIVE_METRIC));
			assertEquals(1.0d, gauge(registry, RocksDBOpenProgressMonitor.WAL_PROGRESS_METRIC));
			assertEquals(1.0d, phaseGauge(registry, "complete"));
			assertEquals(1.0d, counter(registry, RocksDBOpenProgressMonitor.COMPLETIONS_METRIC, "success"));
			assertEquals(0.0d, counter(registry, RocksDBOpenProgressMonitor.COMPLETIONS_METRIC, "failure"));
			assertEquals(1L, registry.get(RocksDBOpenProgressMonitor.DURATION_METRIC)
					.tag("database", DATABASE)
					.tag("outcome", "success")
					.timer()
					.count());
			verify(logger).info(startsWith("Starting RocksDB native open:"), any(Object[].class));
			verify(logger).info(startsWith("Completed RocksDB native open:"), any(Object[].class));
		} finally {
			registry.close();
		}
	}

	@Test
	void recordsFailureWithoutClaimingUnobservedWalCompletion(@TempDir Path directory) {
		var now = new AtomicLong(1_000_000_000L);
		var probe = new AtomicReference<>(new RocksDBOpenProgressMonitor.ProbeSnapshot(false, List.of()));
		var logger = mock(Logger.class);
		var registry = new SimpleMeterRegistry();
		try {
			var threadIo = new AtomicReference<>(
					new RocksDBOpenProgressMonitor.ThreadIoSnapshot(true, 0L, 0L));
			var monitor = newMonitor(directory, logger, registry, now, probe, threadIo,
					new AtomicLong());
			monitor.start();
			now.addAndGet(2_000_000_000L);
			monitor.failed(new IOException("recovery failed"));

			assertEquals(0.0d, gauge(registry, RocksDBOpenProgressMonitor.ACTIVE_METRIC));
			assertEquals(0.0d, gauge(registry, RocksDBOpenProgressMonitor.WAL_PROGRESS_METRIC));
			assertEquals(0.0d, gauge(registry, RocksDBOpenProgressMonitor.WAL_PROGRESS_AVAILABLE_METRIC));
			assertEquals(1.0d, phaseGauge(registry, "failed"));
			assertEquals(1.0d, counter(registry, RocksDBOpenProgressMonitor.COMPLETIONS_METRIC, "failure"));
			verify(logger).error(startsWith("Completed RocksDB native open:"), any(Object[].class));
		} finally {
			registry.close();
		}
	}

	@Test
	void linuxProbesReadCurrentWalDescriptorOffsetAndNativeThreadIo(@TempDir Path walDirectory)
			throws Exception {
		var walPath = walDirectory.resolve("000123.log");
		Files.write(walPath, new byte[256]);
		var inventory = RocksDBOpenProgressMonitor.scanCandidateWalFiles(walDirectory, mock(Logger.class));
		var walProbe = new RocksDBOpenProgressMonitor.ProcWalPositionProbe(inventory.files());
		var threadIoProbe = RocksDBOpenProgressMonitor.ProcThreadIoProbe.captureCurrentThread();
		var availability = walProbe.sample();
		Assumptions.assumeTrue(availability.available(), "Linux /proc fdinfo is unavailable");
		var beforeThreadIo = sampleFromAnotherThread(threadIoProbe);
		Assumptions.assumeTrue(beforeThreadIo.available(), "Linux task I/O counters are unavailable");

		try (var channel = FileChannel.open(walPath, StandardOpenOption.READ)) {
			assertEquals(64, channel.read(ByteBuffer.allocate(64), 0L));
			var afterThreadIo = sampleFromAnotherThread(threadIoProbe);
			assertTrue(afterThreadIo.logicalReadChars() - beforeThreadIo.logicalReadChars() >= 64L);

			channel.position(123L);
			var snapshot = walProbe.sample();

			assertTrue(snapshot.available());
			assertFalse(snapshot.positions().isEmpty());
			assertTrue(snapshot.positions().stream().anyMatch(position ->
					position.logNumber() == 123L && position.offsetBytes() == 123L));
		}
	}

	private static RocksDBOpenProgressMonitor newMonitor(Path directory,
	                                                     Logger logger,
	                                                     MeterRegistry registry,
	                                                     AtomicLong now,
	                                                     AtomicReference<RocksDBOpenProgressMonitor.ProbeSnapshot> probe,
	                                                     AtomicReference<RocksDBOpenProgressMonitor.ThreadIoSnapshot> threadIo,
	                                                     AtomicLong recoveryFlushBytes) {
		var first = new RocksDBOpenProgressMonitor.WalFile(
				1L, directory.resolve("000001.log"), 100L, 0L);
		var second = new RocksDBOpenProgressMonitor.WalFile(
				2L, directory.resolve("000002.log"), 200L, 100L);
		var inventory = new RocksDBOpenProgressMonitor.WalInventory(true, List.of(first, second));
		return new RocksDBOpenProgressMonitor(
				logger,
				DATABASE,
				directory,
				directory,
				inventory,
				probe::get,
				threadIo::get,
				recoveryFlushBytes::get,
				now::get,
				registry,
				false);
	}

	private static RocksDBOpenProgressMonitor.ThreadIoSnapshot sampleFromAnotherThread(
			RocksDBOpenProgressMonitor.ThreadIoProbe probe) throws InterruptedException {
		var result = new AtomicReference<RocksDBOpenProgressMonitor.ThreadIoSnapshot>();
		var sampler = Thread.ofPlatform().start(() -> result.set(probe.sample()));
		sampler.join();
		return result.get();
	}

	private static double gauge(MeterRegistry registry, String metric) {
		return registry.get(metric).tag("database", DATABASE).gauge().value();
	}

	private static double phaseGauge(MeterRegistry registry, String phase) {
		return registry.get(RocksDBOpenProgressMonitor.PHASE_METRIC)
				.tag("database", DATABASE)
				.tag("phase", phase)
				.gauge()
				.value();
	}

	private static double counter(MeterRegistry registry, String metric, String outcome) {
		return registry.get(metric)
				.tag("database", DATABASE)
				.tag("outcome", outcome)
				.counter()
				.count();
	}
}
