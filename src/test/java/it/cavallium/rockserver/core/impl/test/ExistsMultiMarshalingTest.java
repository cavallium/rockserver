package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import it.cavallium.buffer.Buf;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

/** Local cached-JNI qualification; timings are opt-in and have no pass/fail threshold. */
class ExistsMultiMarshalingTest {
	@TempDir Path directory;
	private static volatile long sink;

	@Test
	void sharedEmptyOutputsPreserveStatusesSizesAndIndependentWrappers() throws Exception {
		RocksDB.loadLibrary();
		try (var options = new Options().setCreateIfMissing(true);
				var db = RocksDB.open(options, directory.resolve("correctness").toString());
				var reads = new ReadOptions()) {
			var keys = new ArrayList<Buf>();
			keys.add(Buf.wrap(new byte[0]));
			keys.add(Buf.wrap(new byte[] {99, 1, 2, 99}).subList(1, 3));
			keys.add(Buf.wrap(new byte[] {3})); // missing
			keys.add(Buf.wrap(new byte[2 * 1024 * 1024 + 1]));
			keys.add(keys.get(1)); // duplicate
			db.put(keys.get(0).toByteArray(), new byte[0]);
			db.put(keys.get(1).toByteArray(), new byte[128 * 1024]);
			db.put(keys.get(3).toByteArray(), new byte[] {7});
			try (var arena = Arena.ofConfined()) {
				var nativeKeys = copyKeys(arena, keys);
				var empty = arena.allocate(0);
				var outputs = new MemorySegment[keys.size()];
				Arrays.fill(outputs, empty);
				var statuses = db.multiGetByteBuffers(reads, List.of(db.getDefaultColumnFamily()), nativeKeys, outputs);
				assertEquals(keys.size(), statuses.size());
				for (int i = 0; i < keys.size(); i++) {
					byte[] expected = db.get(keys.get(i).toByteArray());
					var status = statuses.get(i);
					assertEquals(expected == null ? Status.Code.NotFound : Status.Code.Ok, status.status.getCode());
					if (expected != null) {
						assertEquals(expected.length, status.requiredSize);
						assertEquals(0, status.value.capacity());
						assertEquals(0, status.value.remaining());
					}
				}
				assertNotSame(statuses.get(1).value, statuses.get(4).value);
				assertThrows(IllegalArgumentException.class, () -> db.multiGetByteBuffers(reads,
						List.of(db.getDefaultColumnFamily()), nativeKeys, new MemorySegment[0]));
			}
		}
	}

	@Test
	void boundedPairedCachedJniMeasurement() throws Exception {
		assumeTrue(Boolean.getBoolean("rockserver.measureExistsMarshaling"));
		RocksDB.loadLibrary();
		var bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
		bean.setThreadAllocatedMemoryEnabled(true);
		var random = new Random(6296L);
		try (var options = new Options().setCreateIfMissing(true);
				var db = RocksDB.open(options, directory.resolve("paired").toString());
				var reads = new ReadOptions().setFillCache(true)) {
			for (int count : new int[] {1, 71, 2048}) {
				var keys = new ArrayList<Buf>();
				for (int i = 0; i < count; i++) {
					var backing = ByteBuffer.allocate(12).putInt(-1).putInt(i).putInt(-1).array();
					var key = Buf.wrap(backing).subList(4, 8);
					keys.add(key);
					if ((i & 3) != 3) db.put(key.toByteArray(), new byte[64]);
				}
				var handles = List.of(db.getDefaultColumnFamily());
				for (int i = 0; i < 500; i++) {
					sink += call(db, reads, handles, keys, false);
					sink += call(db, reads, handles, keys, true);
				}
				int iterations = count == 1 ? 1000 : count == 71 ? 100 : 20;
				for (int round = 0; round < 15; round++) {
					boolean candidateFirst = random.nextBoolean();
					for (int arm = 0; arm < 2; arm++) {
						boolean shared = arm == 0 ? candidateFirst : !candidateFirst;
						long bytesBefore = bean.getCurrentThreadAllocatedBytes();
						long start = System.nanoTime();
						for (int i = 0; i < iterations; i++) sink += call(db, reads, handles, keys, shared);
						long elapsed = System.nanoTime() - start;
						long bytes = bean.getCurrentThreadAllocatedBytes() - bytesBefore;
						System.out.printf("EXISTS_PAIR keys=%d round=%d candidateFirst=%s shared=%s ns=%.1f allocatedBytes=%.1f%n",
								count, round, candidateFirst, shared, (double) elapsed / iterations, (double) bytes / iterations);
					}
				}
			}
		}
	}

	private static long call(RocksDB db, ReadOptions reads, List<ColumnFamilyHandle> handles,
			List<Buf> keys, boolean shared) throws Exception {
		try (var arena = Arena.ofConfined()) {
			var nativeKeys = copyKeys(arena, keys);
			var outputs = new MemorySegment[keys.size()];
			var empty = shared ? arena.allocate(0) : null;
			for (int i = 0; i < outputs.length; i++) outputs[i] = shared ? empty : arena.allocate(0);
			var statuses = db.multiGetByteBuffers(reads, handles, nativeKeys, outputs);
			long sum = 0;
			for (var status : statuses) {
				if (status.status.getCode() == Status.Code.Ok) sum += status.requiredSize;
				else if (status.status.getCode() != Status.Code.NotFound) throw new AssertionError(status.status);
			}
			return sum;
		}
	}

	private static MemorySegment[] copyKeys(Arena arena, List<Buf> keys) {
		var result = new MemorySegment[keys.size()];
		for (int i = 0; i < result.length; i++) {
			var key = keys.get(i);
			result[i] = arena.allocate(key.size());
			MemorySegment.copy(key.getBackingByteArray(), key.getBackingByteArrayOffset(), result[i],
					ValueLayout.JAVA_BYTE, 0, key.getBackingByteArrayLength());
		}
		return result;
	}
}
