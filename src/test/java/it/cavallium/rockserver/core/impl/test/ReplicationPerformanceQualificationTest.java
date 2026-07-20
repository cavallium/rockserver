package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ReplicationPerformanceQualificationTest {

	private static final long SECOND_NANOS = 1_000_000_000L;
	private static final long KIB = 1024L;
	private static final long MIB = KIB * KIB;

	@Test
	void writeAndReadThroughputAreReportedPerStore() {
		var stores = List.of(
				new StoreProbe("store-a"),
				new StoreProbe("store-b"),
				new StoreProbe("store-c"),
				new StoreProbe("store-d")
		);

		for (var store : stores) {
			for (int i = 0; i < 1_000; i++) {
				store.write(512);
			}
			for (int i = 0; i < 2_000; i++) {
				store.read(256);
			}
		}

		for (var store : stores) {
			assertEquals(200_000L, store.writeThroughput().operationsPerSecond());
			assertEquals(500_000L, store.readThroughput().operationsPerSecond());
			assertEquals(102_400_000L, store.writeThroughput().bytesPerSecond());
			assertEquals(128_000_000L, store.readThroughput().bytesPerSecond());
		}
		assertEquals(4_000L, stores.stream().mapToLong(store -> store.writeThroughput().operations()).sum());
		assertEquals(8_000L, stores.stream().mapToLong(store -> store.readThroughput().operations()).sum());
	}

	@Test
	void partitionCountBudgetAndHotPartitionRankingAreDeterministic() {
		var partitions = new PartitionCountProbe(512, 3, 256 * KIB, 64 * KIB);
		for (int partition = 0; partition < partitions.partitionCount(); partition++) {
			partitions.recordQueueDepth(partition, partition % 9);
		}
		partitions.recordQueueDepth(427, 96);

		assertEquals(1_536, partitions.replicaCount());
		assertEquals(128L * MIB, partitions.replicaMemoryBytes());
		assertEquals(32L * MIB, partitions.routeMetadataBytes());
		assertTrue(partitions.replicaMemoryBytes() <= 192L * MIB);
		assertTrue(partitions.routeMetadataBytes() <= 48L * MIB);
		assertEquals(List.of(427, 8, 17), partitions.hottestPartitions(3));
	}

	@Test
	void snapshotTransferAndRestoreThroughputUseBytesChunksAndElapsedTime() {
		var snapshot = new SnapshotProbe(64 * MIB, 4 * MIB);

		var transfer = snapshot.transferAt(32 * MIB);
		var restore = snapshot.restoreAt(16 * MIB);

		assertEquals(16, transfer.chunks());
		assertEquals(2 * SECOND_NANOS, transfer.elapsedNanos());
		assertEquals(32 * MIB, transfer.throughput().bytesPerSecond());
		assertEquals(4 * SECOND_NANOS, restore.elapsedNanos());
		assertEquals(16 * MIB, restore.throughput().bytesPerSecond());
	}

	@Test
	void largeRangeScanAndCompactionDebtStayWithinConfiguredBounds() {
		var scan = new LargeRangeScanProbe(1_000_000, 160, 4_096, 500, 50_000);
		var throughput = scan.throughput();

		assertEquals(245, scan.batchCount());
		assertTrue(scan.maxBatchBytes() <= MIB);
		assertTrue(throughput.operationsPerSecond() >= 1_900_000L);
		assertEquals(160_000_000L, throughput.bytes());

		var compactionDebt = new CompactionDebtProbe(512 * MIB);
		compactionDebt.flush(160 * MIB);
		compactionDebt.compact(96 * MIB);
		assertEquals(64 * MIB, compactionDebt.pendingBytes());
		assertFalse(compactionDebt.requiresWriteThrottle());

		compactionDebt.flush(500 * MIB);
		assertTrue(compactionDebt.requiresWriteThrottle());
	}

	@Test
	void networkBackpressureCapsInFlightBytesAndRecoversAfterDrain() {
		var backpressure = new NetworkBackpressureProbe(64 * KIB);
		var accepted = new ArrayList<Boolean>();

		for (int i = 0; i < 5; i++) {
			accepted.add(backpressure.admit(16 * KIB));
		}

		assertEquals(List.of(true, true, true, true, false), accepted);
		assertEquals(64 * KIB, backpressure.inFlightBytes());
		assertEquals(1, backpressure.rejections());

		backpressure.drain(32 * KIB);
		assertTrue(backpressure.admit(16 * KIB));
		assertTrue(backpressure.admit(16 * KIB));
		assertFalse(backpressure.admit(16 * KIB));
		assertEquals(64 * KIB, backpressure.inFlightBytes());
		assertEquals(2, backpressure.rejections());
	}

	private record Throughput(long operations, long bytes, long elapsedNanos) {

		private long operationsPerSecond() {
			return operations * SECOND_NANOS / elapsedNanos;
		}

		private long bytesPerSecond() {
			return bytes * SECOND_NANOS / elapsedNanos;
		}
	}

	private static final class StoreProbe {

		private static final long WRITE_NANOS = 5_000L;
		private static final long READ_NANOS = 2_000L;

		private final String name;
		private long writes;
		private long writeBytes;
		private long writeNanos;
		private long reads;
		private long readBytes;
		private long readNanos;

		private StoreProbe(String name) {
			this.name = name;
		}

		private void write(int bytes) {
			writes++;
			writeBytes += bytes;
			writeNanos += WRITE_NANOS;
		}

		private void read(int bytes) {
			reads++;
			readBytes += bytes;
			readNanos += READ_NANOS;
		}

		private Throughput writeThroughput() {
			return new Throughput(writes, writeBytes, writeNanos);
		}

		private Throughput readThroughput() {
			return new Throughput(reads, readBytes, readNanos);
		}
	}

	private static final class PartitionCountProbe {

		private final int partitionCount;
		private final int replicasPerPartition;
		private final long replicaMemoryBytes;
		private final long routeMetadataBytes;
		private final Map<Integer, Integer> queueDepths = new HashMap<>();

		private PartitionCountProbe(int partitionCount,
									int replicasPerPartition,
									long replicaMemoryBytes,
									long routeMetadataBytes) {
			this.partitionCount = partitionCount;
			this.replicasPerPartition = replicasPerPartition;
			this.replicaMemoryBytes = replicaMemoryBytes;
			this.routeMetadataBytes = routeMetadataBytes;
		}

		private int partitionCount() {
			return partitionCount;
		}

		private int replicaCount() {
			return partitionCount * replicasPerPartition;
		}

		private long replicaMemoryBytes() {
			return partitionCount * replicaMemoryBytes;
		}

		private long routeMetadataBytes() {
			return partitionCount * routeMetadataBytes;
		}

		private void recordQueueDepth(int partition, int queueDepth) {
			queueDepths.put(partition, queueDepth);
		}

		private List<Integer> hottestPartitions(int limit) {
			return queueDepths.entrySet()
					.stream()
					.sorted(Map.Entry.<Integer, Integer>comparingByValue(Comparator.reverseOrder())
							.thenComparing(Map.Entry.comparingByKey()))
					.limit(limit)
					.map(Map.Entry::getKey)
					.toList();
		}
	}

	private record SnapshotProbe(long bytes, long chunkBytes) {

		private TransferResult transferAt(long bytesPerSecond) {
			return result(bytesPerSecond);
		}

		private TransferResult restoreAt(long bytesPerSecond) {
			return result(bytesPerSecond);
		}

		private TransferResult result(long bytesPerSecond) {
			var elapsedNanos = bytes * SECOND_NANOS / bytesPerSecond;
			var chunks = (int) ((bytes + chunkBytes - 1) / chunkBytes);
			return new TransferResult(chunks, new Throughput(1, bytes, elapsedNanos));
		}
	}

	private record TransferResult(int chunks, Throughput throughput) {

		private long elapsedNanos() {
			return throughput.elapsedNanos();
		}
	}

	private record LargeRangeScanProbe(int entries,
									   int bytesPerEntry,
									   int batchSize,
									   long entryNanos,
									   long batchOverheadNanos) {

		private int batchCount() {
			return (entries + batchSize - 1) / batchSize;
		}

		private long maxBatchBytes() {
			return (long) batchSize * bytesPerEntry;
		}

		private Throughput throughput() {
			var elapsedNanos = entries * entryNanos + batchCount() * batchOverheadNanos;
			return new Throughput(entries, (long) entries * bytesPerEntry, elapsedNanos);
		}
	}

	private static final class CompactionDebtProbe {

		private final long throttleBytes;
		private long pendingBytes;

		private CompactionDebtProbe(long throttleBytes) {
			this.throttleBytes = throttleBytes;
		}

		private void flush(long bytes) {
			pendingBytes += bytes;
		}

		private void compact(long bytes) {
			pendingBytes = Math.max(0, pendingBytes - bytes);
		}

		private long pendingBytes() {
			return pendingBytes;
		}

		private boolean requiresWriteThrottle() {
			return pendingBytes > throttleBytes;
		}
	}

	private static final class NetworkBackpressureProbe {

		private final long limitBytes;
		private long inFlightBytes;
		private int rejections;

		private NetworkBackpressureProbe(long limitBytes) {
			this.limitBytes = limitBytes;
		}

		private boolean admit(long bytes) {
			if (inFlightBytes + bytes > limitBytes) {
				rejections++;
				return false;
			}
			inFlightBytes += bytes;
			return true;
		}

		private void drain(long bytes) {
			inFlightBytes = Math.max(0, inFlightBytes - bytes);
		}

		private long inFlightBytes() {
			return inFlightBytes;
		}

		private int rejections() {
			return rejections;
		}
	}
}
