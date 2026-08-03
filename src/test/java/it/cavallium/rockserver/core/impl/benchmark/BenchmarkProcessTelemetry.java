package it.cavallium.rockserver.core.impl.benchmark;

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import com.sun.management.UnixOperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

/** Process-wide benchmark telemetry sampled away from production hot paths. */
final class BenchmarkProcessTelemetry {

	private static final long MIN_SAMPLE_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(1L);
	private static final byte[] VM_RSS = "VmRSS:".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

	private BenchmarkProcessTelemetry() {
	}

	static void enableAllocationMeasurement() {
		ThreadMXBean threads = threadBean();
		if (!threads.isThreadAllocatedMemorySupported()) {
			throw new IllegalStateException("Thread allocation measurement is unavailable");
		}
		if (!threads.isThreadAllocatedMemoryEnabled()) {
			threads.setThreadAllocatedMemoryEnabled(true);
		}
	}

	static ProcessSnapshot processSnapshot() {
		OperatingSystemMXBean os = operatingSystemBean();
		long collections = 0L;
		long millis = 0L;
		for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
			collections += Math.max(0L, gc.getCollectionCount());
			millis += Math.max(0L, gc.getCollectionTime());
		}
		return new ProcessSnapshot(os.getProcessCpuTime(), threadBean().getTotalThreadAllocatedBytes(),
				collections, millis);
	}

	private static ThreadMXBean threadBean() {
		if (!(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean bean)) {
			throw new IllegalStateException("HotSpot thread telemetry is unavailable");
		}
		return bean;
	}

	private static OperatingSystemMXBean operatingSystemBean() {
		if (!(ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean bean)) {
			throw new IllegalStateException("HotSpot process CPU telemetry is unavailable");
		}
		return bean;
	}

	record ProcessSnapshot(long cpuNanos,
	                       long allocatedBytes,
	                       long gcCollections,
	                       long gcMillis) {

		ProcessDelta minus(ProcessSnapshot before) {
			return new ProcessDelta(nonNegative(cpuNanos - before.cpuNanos),
					nonNegative(allocatedBytes - before.allocatedBytes),
					nonNegative(gcCollections - before.gcCollections),
					nonNegative(gcMillis - before.gcMillis));
		}
	}

	record ProcessDelta(long cpuNanos,
	                    long allocatedBytes,
	                    long gcCollections,
	                    long gcMillis) {
	}

	record Peaks(long liveHeapBytes,
	             long directMemoryBytes,
	             long residentSetBytes,
	             int threadCount,
	             long nativeHandles) {

		boolean complete() {
			return liveHeapBytes > 0L && directMemoryBytes >= 0L && residentSetBytes > 0L
					&& threadCount > 0 && nativeHandles > 0L;
		}
	}

	static final class PeakSampler implements AutoCloseable {

		private final UnixOperatingSystemMXBean unixBean;
		private final MemoryPoolMXBean[] heapPools;
		private final BufferPoolMXBean directPool;
		private final java.lang.management.ThreadMXBean threads;
		private final FileChannel processStatus;
		private final ByteBuffer statusBuffer = ByteBuffer.allocate(4 * 1024);
		private long lastSampleNanos = Long.MIN_VALUE;
		private long liveHeapBytes;
		private long directMemoryBytes;
		private long residentSetBytes;
		private int threadCount;
		private long nativeHandles;

		PeakSampler() {
			var bean = ManagementFactory.getOperatingSystemMXBean();
			unixBean = bean instanceof UnixOperatingSystemMXBean unix ? unix : null;
			heapPools = ManagementFactory.getMemoryPoolMXBeans().stream()
					.filter(pool -> pool.getType() == MemoryType.HEAP)
					.toArray(MemoryPoolMXBean[]::new);
			BufferPoolMXBean direct = null;
			for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
				if (pool.getName().equalsIgnoreCase("direct")) {
					direct = pool;
					break;
				}
			}
			directPool = direct;
			threads = ManagementFactory.getThreadMXBean();
			Path status = Path.of("/proc/self/status");
			FileChannel opened = null;
			if (Files.isReadable(status)) {
				try {
					opened = FileChannel.open(status, StandardOpenOption.READ);
				} catch (IOException ignored) {
					// Enforced hardware runs reject an unavailable RSS sample via Peaks.complete().
				}
			}
			processStatus = opened;
		}

		void reset() {
			lastSampleNanos = Long.MIN_VALUE;
			liveHeapBytes = 0L;
			directMemoryBytes = 0L;
			residentSetBytes = 0L;
			threadCount = 0;
			nativeHandles = 0L;
			sample();
		}

		void sample() {
			long now = System.nanoTime();
			if (lastSampleNanos != Long.MIN_VALUE && now - lastSampleNanos < MIN_SAMPLE_INTERVAL_NANOS) {
				return;
			}
			lastSampleNanos = now;
			long liveHeap = 0L;
			for (MemoryPoolMXBean pool : heapPools) {
				var collection = pool.getCollectionUsage();
				var usage = collection != null ? collection : pool.getUsage();
				if (usage != null) liveHeap += Math.max(0L, usage.getUsed());
			}
			long direct = directPool == null ? 0L : Math.max(0L, directPool.getMemoryUsed());
			liveHeapBytes = Math.max(liveHeapBytes, liveHeap);
			directMemoryBytes = Math.max(directMemoryBytes, direct);
			residentSetBytes = Math.max(residentSetBytes, readResidentSetBytes());
			threadCount = Math.max(threadCount, threads.getThreadCount());
			if (unixBean != null) {
				nativeHandles = Math.max(nativeHandles, unixBean.getOpenFileDescriptorCount());
			}
		}

		Peaks peaks() {
			return new Peaks(liveHeapBytes, directMemoryBytes, residentSetBytes,
					threadCount, nativeHandles);
		}

		private long readResidentSetBytes() {
			if (processStatus == null) return -1L;
			try {
				statusBuffer.clear();
				processStatus.position(0L);
				int length = processStatus.read(statusBuffer);
				if (length <= VM_RSS.length) return -1L;
				byte[] bytes = statusBuffer.array();
				for (int index = 0; index <= length - VM_RSS.length; index++) {
					if (!matches(bytes, index, VM_RSS)) continue;
					int cursor = index + VM_RSS.length;
					while (cursor < length && (bytes[cursor] == ' ' || bytes[cursor] == '\t')) cursor++;
					long kibibytes = 0L;
					boolean digit = false;
					while (cursor < length && bytes[cursor] >= '0' && bytes[cursor] <= '9') {
						digit = true;
						kibibytes = Math.addExact(Math.multiplyExact(kibibytes, 10L), bytes[cursor] - '0');
						cursor++;
					}
					return digit ? Math.multiplyExact(kibibytes, 1024L) : -1L;
				}
				return -1L;
			} catch (IOException | ArithmeticException ignored) {
				return -1L;
			}
		}

		@Override
		public void close() {
			if (processStatus == null) return;
			try {
				processStatus.close();
			} catch (IOException failure) {
				throw new java.io.UncheckedIOException(failure);
			}
		}
	}

	private static boolean matches(byte[] value, int offset, byte[] expected) {
		for (int index = 0; index < expected.length; index++) {
			if (value[offset + index] != expected[index]) return false;
		}
		return true;
	}

	private static long nonNegative(long value) {
		if (value < 0L) throw new IllegalStateException("Monotonic process counter moved backwards");
		return value;
	}
}
