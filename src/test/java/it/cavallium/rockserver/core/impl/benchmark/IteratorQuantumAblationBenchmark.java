package it.cavallium.rockserver.core.impl.benchmark;

import com.sun.management.ThreadMXBean;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import reactor.core.publisher.Flux;

/**
 * Same-build causal ablation for cooperative explicit-iterator service bounds.
 *
 * <p>The idle arm compares the old fixed-page primitive with the bounded primitive while no peer
 * requests preemption. It isolates the per-item context polling cost on the same iterator, values,
 * checksum, thread, and counterbalanced forty-pair schedule. Allocation is diagnostic only: scheduler
 * quality, rather than a resource-only gain, is the reason for this change.</p>
 *
 * <p>The competitive arm uses a real single-worker {@link RWScheduler}. A BATCH scan is held after
 * dispatch, a LATENCY task is queued, and then the scan is released. The legacy task executes one
 * indivisible 4,096-value call; the bounded task checks the live scheduler context after each
 * RocksDB iterator step and yields at the configured byte/time checkpoint. A third arm sets both
 * bounds below one value and must still consume exactly one value, exposing the irreducible
 * one-native-step handoff floor. All arms finish the identical full scan and validate every value.</p>
 *
 * <p>This is a local mechanism proof, not representative hardware or whole-service acceptance.
 * It reports PASS only when idle throughput proves 2% non-inferiority and competitive peer handoff
 * proves at least a 2x improvement. It reports FAIL only for a demonstrated idle regression or a
 * missing peer-latency improvement; otherwise it reports INCONCLUSIVE.</p>
 */
public final class IteratorQuantumAblationBenchmark {

	private static final int DEFAULT_ENTRIES = 8_192;
	private static final int DEFAULT_VALUE_BYTES = 256;
	private static final int DEFAULT_WARMUP_SCANS = 24;
	private static final int DEFAULT_SCANS_PER_SAMPLE = 32;
	private static final int DEFAULT_COMPETITIVE_SAMPLES = 4;
	private static final int FIXED_PAIRS = 40;
	private static final int IDLE_INTERLEAVE_BLOCKS = 16;
	private static final int PAGE_ITEMS = 4_096;
	private static final int COMPETITIVE_VALUE_BYTES = 8 * 1_024;
	private static final long PAGE_BYTES = 8L * 1_024L * 1_024L;
	private static final long PAGE_NANOS = TimeUnit.MILLISECONDS.toNanos(8L);
	private static final long ONE_STEP_BYTES = 1L;
	private static final long ONE_STEP_NANOS = 1L;
	private static final long TIMEOUT_SECONDS = 30L;

	private IteratorQuantumAblationBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		int entries = integerArgument(args, 0, DEFAULT_ENTRIES);
		int valueBytes = integerArgument(args, 1, DEFAULT_VALUE_BYTES);
		int warmupScans = integerArgument(args, 2, DEFAULT_WARMUP_SCANS);
		int scansPerSample = integerArgument(args, 3, DEFAULT_SCANS_PER_SAMPLE);
		int competitiveSamples = integerArgument(args, 4, DEFAULT_COMPETITIVE_SAMPLES);
		if (entries < PAGE_ITEMS * 2 || valueBytes < Integer.BYTES
				|| warmupScans < 1 || scansPerSample < 1 || competitiveSamples < 1) {
			throw new IllegalArgumentException(
					"entries must cover at least two pages and all dimensions must be positive");
		}

		var allocation = threadAllocationBean();
		Path root = Files.createTempDirectory("rockserver-iterator-quantum-ablation-");
		try {
			Path config = root.resolve("benchmark.conf");
			Files.writeString(config, """
					database: {
					  parallelism: {
					    read: 3
					    write: 3
					    workload: { competing-batch-read-maximum-active: 3 }
					  }
					  global: {
					    enable-fast-get: false
					    ingest-behind: false
					    optimistic: false
					    disable-auto-compactions: true
					  }
					}
					""");
			try (var connection = new EmbeddedConnection(root.resolve("database"),
					"iterator-quantum-ablation", config)) {
				var sync = connection.getSyncApi(RequestContext.batch());
				long idleColumn = populate(sync, "idle", entries, valueBytes);
				long competitiveColumn = populate(sync,
						"competitive", PAGE_ITEMS, COMPETITIVE_VALUE_BYTES);
				long idleIterator = sync.openIterator(0L, idleColumn, new Keys(), null, false, java.time.Duration.ofMillis( 60_000L));
				long competitiveIterator = sync.openIterator(
						0L, competitiveColumn, new Keys(), null, false, java.time.Duration.ofMillis( 60_000L));
				try {
					var idle = measureIdle(connection,
							sync,
							idleIterator,
							entries,
							valueBytes,
							warmupScans,
							scansPerSample,
							allocation);
					var competitive = measureCompetitive(connection,
							sync,
							competitiveIterator,
							competitiveSamples);
					var decision = decide(idle.throughput(), competitive.handoff());
					String head = gitHead();
					String report = String.format(Locale.ROOT,
							"iterator_quantum_idle head=%s entries=%d value_bytes=%d pairs=%d "
									+ "scans_per_sample=%d interleave_blocks=%d "
									+ "throughput_ratio=%.6f throughput_ci95=[%.6f,%.6f] "
									+ "allocation_ratio=%.6f allocation_ci95=[%.6f,%.6f]%n"
									+ "iterator_quantum_competitive page_items=%d value_bytes=%d quantum_bytes=%d "
									+ "quantum_nanos=%d pairs=%d samples_per_arm=%d handoff_ratio=%.6f "
									+ "handoff_ci95=[%.6f,%.6f] one_step_to_bounded_ratio=%.6f "
									+ "one_step_ci95=[%.6f,%.6f] decision=%s%n",
							head, entries, valueBytes, FIXED_PAIRS, scansPerSample, IDLE_INTERLEAVE_BLOCKS,
							idle.throughput().mean(), idle.throughput().lower95(), idle.throughput().upper95(),
							idle.allocation().mean(), idle.allocation().lower95(), idle.allocation().upper95(),
							PAGE_ITEMS, COMPETITIVE_VALUE_BYTES, PAGE_BYTES, PAGE_NANOS,
							FIXED_PAIRS, competitiveSamples,
							competitive.handoff().mean(), competitive.handoff().lower95(),
							competitive.handoff().upper95(), competitive.oneStepToBounded().mean(),
							competitive.oneStepToBounded().lower95(),
							competitive.oneStepToBounded().upper95(), decision);
					System.out.print(report);
					if (args.length > 5) {
						Path artifact = Path.of(args[5]).toAbsolutePath();
						Files.writeString(artifact,
								report,
								StandardOpenOption.CREATE_NEW,
								StandardOpenOption.WRITE);
						System.out.println("iterator_quantum_artifact path=" + artifact);
					}
				} finally {
					sync.closeIterator(competitiveIterator);
					sync.closeIterator(idleIterator);
				}
			}
		} finally {
			Utils.deleteDirectory(root.toString());
		}
	}

	private static IdleEvidence measureIdle(EmbeddedConnection connection,
			it.cavallium.rockserver.core.common.RocksDBSyncAPI sync,
			long iteratorId,
			int entries,
			int valueBytes,
			int warmupScans,
			int scansPerSample,
			ThreadMXBean allocation) {
		var runner = new IdleRunner(connection, sync, iteratorId, entries, valueBytes, allocation);
		for (int index = 0; index < warmupScans; index++) {
			runner.run(index % 2 == 0 ? IdleMechanism.LEGACY : IdleMechanism.BOUNDED, 1);
		}

		double[] legacyThroughput = new double[FIXED_PAIRS];
		double[] boundedThroughput = new double[FIXED_PAIRS];
		double[] legacyAllocation = new double[FIXED_PAIRS];
		double[] boundedAllocation = new double[FIXED_PAIRS];
		for (int pair = 0; pair < FIXED_PAIRS; pair++) {
			var measured = runInterleavedIdlePair(runner, pair, scansPerSample);
			IdleSample legacy = measured.legacy();
			IdleSample bounded = measured.bounded();
			legacyThroughput[pair] = legacy.entriesPerSecond();
			boundedThroughput[pair] = bounded.entriesPerSecond();
			legacyAllocation[pair] = legacy.allocatedBytesPerEntry();
			boundedAllocation[pair] = bounded.allocatedBytesPerEntry();
		}
		return new IdleEvidence(
				PairedBenchmarkStatistics.pairedLogRatio(legacyThroughput, boundedThroughput),
				PairedBenchmarkStatistics.pairedLogRatio(legacyAllocation, boundedAllocation));
	}

	private static IdlePair runInterleavedIdlePair(IdleRunner runner,
			int pair,
			int scansPerMechanism) {
		int blocks = Math.min(IDLE_INTERLEAVE_BLOCKS, scansPerMechanism);
		int baseScans = scansPerMechanism / blocks;
		int remainder = scansPerMechanism % blocks;
		var legacy = new IdleTotals();
		var bounded = new IdleTotals();
		for (int block = 0; block < blocks; block++) {
			int scans = baseScans + (block < remainder ? 1 : 0);
			boolean legacyFirst = ((pair + block) & 1) == 0;
			IdleSample first = runner.run(
					legacyFirst ? IdleMechanism.LEGACY : IdleMechanism.BOUNDED,
					scans);
			IdleSample second = runner.run(
					legacyFirst ? IdleMechanism.BOUNDED : IdleMechanism.LEGACY,
					scans);
			(legacyFirst ? legacy : bounded).add(first);
			(legacyFirst ? bounded : legacy).add(second);
		}
		return new IdlePair(legacy.sample(), bounded.sample());
	}

	private static CompetitiveEvidence measureCompetitive(EmbeddedConnection connection,
			it.cavallium.rockserver.core.common.RocksDBSyncAPI sync,
			long iteratorId,
			int samplesPerArm) throws Exception {
		try (var runner = new CompetitiveRunner(connection, sync, iteratorId)) {
			for (var mechanism : CompetitiveMechanism.values()) {
				runner.run(mechanism, 1);
			}
			double[] legacyHandoff = new double[FIXED_PAIRS];
			double[] boundedHandoff = new double[FIXED_PAIRS];
			double[] oneStepHandoff = new double[FIXED_PAIRS];
			for (int pair = 0; pair < FIXED_PAIRS; pair++) {
				CompetitiveMechanism[] order = (pair & 1) == 0
						? new CompetitiveMechanism[] {
								CompetitiveMechanism.LEGACY,
								CompetitiveMechanism.BOUNDED,
								CompetitiveMechanism.ONE_STEP}
						: new CompetitiveMechanism[] {
								CompetitiveMechanism.ONE_STEP,
								CompetitiveMechanism.BOUNDED,
								CompetitiveMechanism.LEGACY};
				for (var mechanism : order) {
					var sample = runner.run(mechanism, samplesPerArm);
					switch (mechanism) {
						case LEGACY -> legacyHandoff[pair] = sample.averagePeerHandoffNanos();
						case BOUNDED -> boundedHandoff[pair] = sample.averagePeerHandoffNanos();
						case ONE_STEP -> oneStepHandoff[pair] = sample.averagePeerHandoffNanos();
					}
				}
			}
			return new CompetitiveEvidence(
					PairedBenchmarkStatistics.pairedLogRatio(legacyHandoff, boundedHandoff),
					PairedBenchmarkStatistics.pairedLogRatio(boundedHandoff, oneStepHandoff));
		}
	}

	private static Decision decide(PairedBenchmarkStatistics.RatioConfidenceInterval idleThroughput,
			PairedBenchmarkStatistics.RatioConfidenceInterval peerHandoff) {
		boolean idleProvesNonInferiority = idleThroughput.lower95() >= 0.98d;
		boolean idleProvesRegression = idleThroughput.upper95() < 0.98d;
		boolean handoffProvesImprovement = peerHandoff.upper95() <= 0.50d;
		boolean handoffMissesImprovement = peerHandoff.lower95() > 0.50d;
		if (idleProvesRegression || handoffMissesImprovement) {
			return Decision.FAIL;
		}
		if (idleProvesNonInferiority && handoffProvesImprovement) {
			return Decision.PASS;
		}
		return Decision.INCONCLUSIVE;
	}

	private static ThreadMXBean threadAllocationBean() {
		if (!(ManagementFactory.getThreadMXBean() instanceof ThreadMXBean allocation)
				|| !allocation.isThreadAllocatedMemorySupported()) {
			throw new IllegalStateException("Thread allocation accounting is unavailable");
		}
		if (!allocation.isThreadAllocatedMemoryEnabled()) {
			allocation.setThreadAllocatedMemoryEnabled(true);
		}
		return allocation;
	}

	private static long populate(it.cavallium.rockserver.core.common.RocksDBSyncAPI sync,
			String name,
			int entries,
			int valueBytes) {
		long columnId = sync.createColumn(name,
				ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
		var keys = new ArrayList<Keys>(entries);
		var values = new ArrayList<Buf>(entries);
		for (int index = 0; index < entries; index++) {
			keys.add(new Keys(intBuf(index)));
			var value = new byte[valueBytes];
			ByteBuffer.wrap(value).putInt(index);
			values.add(Buf.wrap(value));
		}
		sync.putBatch(columnId, Flux.just(new KVBatch.KVBatchRef(keys, values)),
				PutBatchMode.WRITE_BATCH_NO_WAL);
		return columnId;
	}

	private static Buf intBuf(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private static int integerArgument(String[] args, int index, int fallback) {
		return args.length > index ? Integer.parseInt(args[index]) : fallback;
	}

	private static String gitHead() {
		try {
			var process = new ProcessBuilder("git", "rev-parse", "HEAD").redirectErrorStream(true).start();
			String value = new String(process.getInputStream().readAllBytes()).trim();
			if (process.waitFor() != 0 || !value.matches("[0-9a-f]{40}")) {
				throw new IllegalStateException("Unable to resolve exact benchmark Git HEAD: " + value);
			}
			return value;
		} catch (Exception failure) {
			throw new IllegalStateException("Unable to resolve benchmark provenance", failure);
		}
	}

	private static void await(CountDownLatch latch) {
		try {
			if (!latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Timed out waiting for benchmark dispatch gate");
			}
		} catch (InterruptedException interruption) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while waiting for benchmark dispatch gate",
					interruption);
		}
	}

	private enum Decision {
		PASS,
		FAIL,
		INCONCLUSIVE
	}

	private enum IdleMechanism {
		LEGACY,
		BOUNDED
	}

	private enum CompetitiveMechanism {
		LEGACY,
		BOUNDED,
		ONE_STEP
	}

	private record IdleSample(long entries, long elapsedNanos, long allocatedBytes) {

		private double entriesPerSecond() {
			return entries * 1_000_000_000.0d / elapsedNanos;
		}

		private double allocatedBytesPerEntry() {
			return allocatedBytes / (double) entries;
		}
	}

	private record IdlePair(IdleSample legacy, IdleSample bounded) {
	}

	private record CompetitiveSample(double averagePeerHandoffNanos) {
	}

	private record IdleEvidence(PairedBenchmarkStatistics.RatioConfidenceInterval throughput,
			PairedBenchmarkStatistics.RatioConfidenceInterval allocation) {
	}

	private record CompetitiveEvidence(PairedBenchmarkStatistics.RatioConfidenceInterval handoff,
			PairedBenchmarkStatistics.RatioConfidenceInterval oneStepToBounded) {
	}

	private static final class IdleTotals {

		private long entries;
		private long elapsedNanos;
		private long allocatedBytes;

		private void add(IdleSample sample) {
			entries = Math.addExact(entries, sample.entries());
			elapsedNanos = Math.addExact(elapsedNanos, sample.elapsedNanos());
			allocatedBytes = Math.addExact(allocatedBytes, sample.allocatedBytes());
		}

		private IdleSample sample() {
			return new IdleSample(entries, elapsedNanos, allocatedBytes);
		}
	}

	private static final class IdleRunner {

		private final EmbeddedConnection connection;
		private final it.cavallium.rockserver.core.common.RocksDBSyncAPI sync;
		private final long iteratorId;
		private final int entries;
		private final int valueBytes;
		private final ThreadMXBean allocation;
		private final IdleContext idleContext = new IdleContext();
		private long blackhole;

		private IdleRunner(EmbeddedConnection connection,
				it.cavallium.rockserver.core.common.RocksDBSyncAPI sync,
				long iteratorId,
				int entries,
				int valueBytes,
				ThreadMXBean allocation) {
			this.connection = connection;
			this.sync = sync;
			this.iteratorId = iteratorId;
			this.entries = entries;
			this.valueBytes = valueBytes;
			this.allocation = allocation;
		}

		private IdleSample run(IdleMechanism mechanism, int scans) {
			long thread = Thread.currentThread().threadId();
			long allocatedBefore = allocation.getThreadAllocatedBytes(thread);
			long started = System.nanoTime();
			long consumed = 0L;
			for (int scan = 0; scan < scans; scan++) {
				sync.seekTo(iteratorId, new Keys());
				long remaining = entries;
				while (remaining > 0L) {
					int step = (int) Math.min(remaining, PAGE_ITEMS);
					List<Buf> values;
					if (mechanism == IdleMechanism.LEGACY) {
						values = connection.getInternalDB().subsequent(
								iteratorId, 0L, step, RequestType.<Buf>multi());
					} else {
						values = connection.getInternalDB().readIteratorQuantumInternal(
								iteratorId, step, PAGE_BYTES, PAGE_NANOS, idleContext).values();
					}
					if (values.isEmpty()) {
						break;
					}
					remaining -= values.size();
					for (var value : values) {
						consumed++;
						blackhole = Long.rotateLeft(blackhole, 7) ^ value.size() ^ value.getInt(0);
					}
				}
			}
			long elapsed = System.nanoTime() - started;
			long allocated = allocation.getThreadAllocatedBytes(thread) - allocatedBefore;
			long expected = (long) scans * entries;
			if (consumed != expected || blackhole == Long.MIN_VALUE || valueBytes < Integer.BYTES) {
				throw new IllegalStateException("Iterator idle ablation lost values: expected="
						+ expected + " actual=" + consumed);
			}
			return new IdleSample(consumed, elapsed, allocated);
		}
	}

	private static final class CompetitiveRunner implements AutoCloseable {

		private final EmbeddedConnection connection;
		private final it.cavallium.rockserver.core.common.RocksDBSyncAPI sync;
		private final long iteratorId;
		private final RWScheduler scheduler = RWScheduler.forTesting(
				3, 3, 1, 64, 64, "iterator-quantum-competitive");

		private CompetitiveRunner(EmbeddedConnection connection,
				it.cavallium.rockserver.core.common.RocksDBSyncAPI sync,
				long iteratorId) {
			this.connection = connection;
			this.sync = sync;
			this.iteratorId = iteratorId;
		}

		private CompetitiveSample run(CompetitiveMechanism mechanism, int samples) throws Exception {
			long totalPeerHandoff = 0L;
			for (int sample = 0; sample < samples; sample++) {
				totalPeerHandoff += runOnce(mechanism);
			}
			return new CompetitiveSample(totalPeerHandoff / (double) samples);
		}

		private long runOnce(CompetitiveMechanism mechanism) throws Exception {
			sync.seekTo(iteratorId, new Keys());
			var blockersStarted = new CountDownLatch(2);
			var releaseBlockers = new CountDownLatch(1);
			var blockersCompleted = new CountDownLatch(2);
			var latencyExecutor = scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					Long.MAX_VALUE);
			for (int blocker = 0; blocker < 2; blocker++) {
				latencyExecutor.execute(() -> {
					blockersStarted.countDown();
					try {
						await(releaseBlockers);
					} finally {
						blockersCompleted.countDown();
					}
				});
			}
			await(blockersStarted);
			try {
				var started = new CountDownLatch(1);
				var release = new CountDownLatch(1);
				var completed = new CompletableFuture<Long>();
				if (mechanism == CompetitiveMechanism.LEGACY) {
					scheduler.executor(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE,
							Long.MAX_VALUE).execute(() -> {
						try {
							started.countDown();
							await(release);
							var values = connection.getInternalDB().subsequent(
									iteratorId, 0L, PAGE_ITEMS, RequestType.<Buf>multi());
							completed.complete(validateValues(values, 0L));
						} catch (Throwable failure) {
							completed.completeExceptionally(failure);
						}
					}, PAGE_BYTES);
				} else {
					long bytes = mechanism == CompetitiveMechanism.ONE_STEP ? ONE_STEP_BYTES : PAGE_BYTES;
					long nanos = mechanism == CompetitiveMechanism.ONE_STEP ? ONE_STEP_NANOS : PAGE_NANOS;
					var task = new BoundedScanTask(started, release, completed, bytes, nanos, mechanism);
					scheduler.executor(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE,
							Long.MAX_VALUE).executeCooperatively(task, bytes);
				}

				await(started);
				var peer = new CompletableFuture<Long>();
				long peerSubmitted = System.nanoTime();
				latencyExecutor.execute(() -> peer.complete(System.nanoTime()));
				release.countDown();
				long peerStarted = peer.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
				releaseBlockers.countDown();
				long consumed = completed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
				if (consumed != PAGE_ITEMS) {
					throw new IllegalStateException("Competitive scan lost values: " + consumed);
				}
				return Math.max(1L, peerStarted - peerSubmitted);
			} finally {
				releaseBlockers.countDown();
				await(blockersCompleted);
			}
		}

		private static long validateValues(List<Buf> values, long offset) {
			for (int index = 0; index < values.size(); index++) {
				var value = values.get(index);
				if (value.size() != COMPETITIVE_VALUE_BYTES || value.getInt(0) != offset + index) {
					throw new IllegalStateException("Competitive iterator value mismatch at "
							+ (offset + index));
				}
			}
			return values.size();
		}

		@Override
		public void close() {
			scheduler.dispose();
		}

		private final class BoundedScanTask implements RWScheduler.CooperativeCompletionTask {

			private final CountDownLatch started;
			private final CountDownLatch release;
			private final CompletableFuture<Long> completed;
			private final long maximumBytes;
			private final long maximumNanos;
			private final CompetitiveMechanism mechanism;
			private boolean firstDispatch = true;
			private long consumed;

			private BoundedScanTask(CountDownLatch started,
					CountDownLatch release,
					CompletableFuture<Long> completed,
					long maximumBytes,
					long maximumNanos,
					CompetitiveMechanism mechanism) {
				this.started = started;
				this.release = release;
				this.completed = completed;
				this.maximumBytes = maximumBytes;
				this.maximumNanos = maximumNanos;
				this.mechanism = mechanism;
			}

			@Override
			public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
				if (firstDispatch) {
					firstDispatch = false;
					started.countDown();
					await(release);
				}
				var quantum = connection.getInternalDB().readIteratorQuantumInternal(
						iteratorId,
						PAGE_ITEMS - consumed,
						maximumBytes,
						maximumNanos,
						context);
				long quantumItems = validateValues(quantum.values(), consumed);
				if (consumed == 0L) {
					long maximumFirstQuantum = mechanism == CompetitiveMechanism.ONE_STEP
							? 1L
							: Math.max(1L, PAGE_BYTES / COMPETITIVE_VALUE_BYTES);
					if (quantumItems < 1L || quantumItems > maximumFirstQuantum) {
						throw new IllegalStateException("First competitive quantum consumed "
								+ quantumItems + " values; expected 1.." + maximumFirstQuantum);
					}
				}
				consumed += quantumItems;
				if (quantum.exhausted() || consumed == PAGE_ITEMS) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				if (!quantum.checkpointRequested() && !context.preemptionRequested()) {
					throw new IllegalStateException("Bounded iterator returned a partial scan without a checkpoint");
				}
				return RWScheduler.CooperativeResult.YIELD;
			}

			@Override
			public void completeCooperatively() {
				completed.complete(consumed);
			}

			@Override
			public void reject(RuntimeException failure) {
				completed.completeExceptionally(Objects.requireNonNull(failure, "failure"));
			}
		}
	}

	private static final class IdleContext implements RWScheduler.CooperativeContext {

		private volatile boolean preemptionRequested;
		private volatile boolean terminationRequested;

		@Override
		public boolean preemptionRequested() {
			return preemptionRequested;
		}

		@Override
		public boolean terminationRequested() {
			return terminationRequested;
		}

		@Override
		public RuntimeException terminationFailure() {
			return null;
		}

		@Override
		public boolean fail(RuntimeException failure) {
			throw new IllegalStateException("Idle benchmark context failed", failure);
		}
	}
}
