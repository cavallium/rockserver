package it.cavallium.rockserver.core.impl.benchmark;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.rocksdb.ReadOptions;
import org.rocksdb.ReadTier;
import org.rocksdb.Status;

/** Opt-in release benchmark for the bounded Ensure PutMulti cache probe. */
public final class EnsureWriteElisionBenchmark {

	private static final int DEFAULT_ENTRIES = 4_096;
	private static final int DEFAULT_HOT_ROUNDS = 5;
	private static final long MAX_PROBE_BYTES = 2L * 1024L * 1024L;

	private EnsureWriteElisionBenchmark() {
	}

	public static void main(String[] args) throws Exception {
		int entries = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_ENTRIES;
		int hotRounds = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_HOT_ROUNDS;
		if (entries < DEFAULT_ENTRIES || hotRounds <= 0) {
			throw new IllegalArgumentException("entries must be at least 4096 and hotRounds must be positive");
		}
		Path root = Files.createTempDirectory("rockserver-ensure-benchmark-");
		try {
			run(root, entries, hotRounds);
		} finally {
			Utils.deleteDirectory(root.toString());
		}
	}

	private static void run(Path root, int entries, int hotRounds) throws Exception {
		Path database = root.resolve("db");
		Path config = root.resolve("benchmark.conf");
		Files.writeString(config, "database: { global: { block-cache: \"64KiB\" } }");
		var keys = keys(entries);
		var oldBytes = new byte[1_024];
		var newBytes = new byte[1_024];
		Arrays.fill(oldBytes, (byte) 1);
		Arrays.fill(newBytes, (byte) 2);
		var oldValues = Collections.nCopies(entries, Buf.wrap(oldBytes));
		var newValues = Collections.nCopies(entries, Buf.wrap(newBytes));
		long hotColumn;
		long coldColumn;
		double hotRate;

		try (var connection = new EmbeddedConnection(database, "ensure-benchmark", config)) {
			var api = connection.getSyncApi(RequestContext.batch());
			hotColumn = api.createColumn("hot", schema());
			coldColumn = api.createColumn("cold", schema());
			api.cdcCreate("ensure-benchmark-hot", null, List.of(hotColumn));

			api.putMulti(0, hotColumn, keys, oldValues, RequestType.ensure());
			long hotStart = System.nanoTime();
			for (int round = 0; round < hotRounds; round++) {
				api.putMulti(0, hotColumn, keys, oldValues, RequestType.ensure());
			}
			hotRate = rate((long) entries * hotRounds, System.nanoTime() - hotStart);
			long hotEvents = cdcEventCount(api, "ensure-benchmark-hot");
			if (hotEvents != entries) {
				throw new IllegalStateException("hot equal batches published " + hotEvents
						+ " CDC events instead of " + entries);
			}

			api.putMulti(0, coldColumn, keys, oldValues, RequestType.none());
			api.flush();
		}

		double coldRate;
		double coldIncomplete;
		double coldDifferent;
		double coldNotFound;
		long maximumObservedBytes;
		int maximumObservedKeys;
		var metrics = new SimpleMeterRegistry();
		try (var connection = new EmbeddedConnection(database, "ensure-benchmark", config)) {
			var embedded = connection.getInternalDB();
			((CompositeMeterRegistry) embedded.getMetricsRegistry()).add(metrics);
			assertNativeMultiGetHonorsCacheTier(embedded, keys);
			var probeCalls = new ArrayList<ProbeCall>();
			embedded.setWriteElisionMultiGetObserverForTesting(
					(keysInCall, bytes) -> probeCalls.add(new ProbeCall(keysInCall, bytes)));
			var api = connection.getSyncApi(RequestContext.batch());
			api.cdcCreate("ensure-benchmark-cold", null, List.of(coldColumn));
			if (embedded.getPendingOpsCount() != 0L) {
				throw new IllegalStateException("benchmark started with pending work");
			}

			long coldStart = System.nanoTime();
			api.putMulti(0, coldColumn, keys, newValues, RequestType.ensure());
			coldRate = rate(entries, System.nanoTime() - coldStart);
			if (embedded.getPendingOpsCount() != 0L) {
				throw new IllegalStateException("benchmark left pending work after synchronous completion");
			}
			maximumObservedKeys = probeCalls.stream().mapToInt(ProbeCall::keys).max().orElseThrow();
			maximumObservedBytes = probeCalls.stream().mapToLong(ProbeCall::bytes).max().orElseThrow();
			if (maximumObservedKeys > DEFAULT_ENTRIES || maximumObservedBytes > MAX_PROBE_BYTES) {
				throw new IllegalStateException("probe bound exceeded: keys=" + maximumObservedKeys
						+ " bytes=" + maximumObservedBytes);
			}
			coldIncomplete = decisionCount(metrics, "fallback_incomplete");
			coldDifferent = decisionCount(metrics, "fallback_different");
			coldNotFound = decisionCount(metrics, "fallback_not_found");
			double elided = decisionCount(metrics, "elided");
			if (coldIncomplete + coldDifferent != entries
					|| coldIncomplete < entries * 0.9
					|| coldNotFound != 0.0
					|| elided != 0.0) {
				throw new IllegalStateException("cold distinct probe decisions were unexpected: incomplete="
						+ coldIncomplete + " different=" + coldDifferent + " notFound=" + coldNotFound + " elided=" + elided);
			}
			long coldEvents = cdcEventCount(api, "ensure-benchmark-cold");
			if (coldEvents != entries) {
				throw new IllegalStateException("cold distinct batch did not retain ordinary Put CDC semantics: "
						+ coldEvents);
			}
			assertValue(newValues.getFirst(), api.get(0, coldColumn, keys.getFirst(), RequestType.current()));
			assertValue(newValues.getLast(), api.get(0, coldColumn, keys.getLast(), RequestType.current()));
		} finally {
			metrics.close();
		}

		if (hotRate < DEFAULT_ENTRIES || coldRate < DEFAULT_ENTRIES) {
			throw new IllegalStateException("throughput below 4,096 entries/s: hot=" + hotRate
					+ " cold=" + coldRate);
		}
		System.out.printf(Locale.ROOT,
				"Ensure benchmark PASS entries=%d hotRounds=%d hot=%,.0f entries/s cold=%,.0f entries/s "
						+ "coldIncomplete=%.0f coldDifferent=%.0f coldNotFound=%.0f "
						+ "maxProbeKeys=%d maxProbeBytes=%d pendingOps=0%n",
				entries,
				hotRounds,
				hotRate,
				coldRate,
				coldIncomplete,
				coldDifferent,
				coldNotFound,
				maximumObservedKeys,
				maximumObservedBytes);
	}

	private static List<Keys> keys(int count) {
		var keys = new ArrayList<Keys>(count);
		for (int i = 0; i < count; i++) {
			keys.add(new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(i).array())));
		}
		return List.copyOf(keys);
	}

	private static ColumnSchema schema() {
		return ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true);
	}

	private static long cdcEventCount(RocksDBSyncAPI api, String subscription) {
		try (var events = api.cdcPoll(subscription, null, 100_000)) {
			return events.count();
		}
	}

	private static double rate(long entries, long elapsedNanos) {
		return entries / ((double) elapsedNanos / TimeUnit.SECONDS.toNanos(1));
	}

	private static double decisionCount(SimpleMeterRegistry metrics, String decision) {
		var counter = metrics.find("rockserver.write.elision.decisions")
				.tags("db", "ensure-benchmark", "request_type", "ensure", "decision", decision)
				.counter();
		return counter == null ? 0.0 : counter.count();
	}

	private static void assertNativeMultiGetHonorsCacheTier(EmbeddedDB embedded, List<Keys> logicalKeys)
			throws org.rocksdb.RocksDBException {
		var column = embedded.getDb().getStartupColumns().entrySet().stream()
				.filter(entry -> "cold".equals(new String(entry.getKey().getName())))
				.map(java.util.Map.Entry::getValue)
				.findFirst()
				.orElseThrow();
		try (var options = new ReadOptions().setReadTier(ReadTier.BLOCK_CACHE_TIER).setFillCache(false);
				var arena = Arena.ofConfined()) {
			byte[] coldKey = null;
			for (var keys : logicalKeys) {
				var candidate = keys.keys()[0].toByteArray();
				try {
					embedded.getDb().get().get(column, options, candidate);
				} catch (org.rocksdb.RocksDBException exception) {
					if (exception.getStatus().getCode() == Status.Code.Incomplete) {
						coldKey = candidate;
						break;
					}
					throw exception;
				}
			}
			if (coldKey == null) {
				throw new IllegalStateException("the cold benchmark dataset was entirely memory-resident");
			}
			var keySegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, coldKey);
			var valueSegment = arena.allocate(1_024);
			var result = embedded.getDb().get().multiGetByteBuffers(
					options,
					List.of(column),
					new MemorySegment[] {keySegment},
					new MemorySegment[] {valueSegment})
					.getFirst();
			if (result.status.getCode() != Status.Code.Incomplete) {
				throw new IllegalStateException("native MultiGet bypassed BLOCK_CACHE_TIER: "
						+ result.status.getCode() + " subCode=" + result.status.getSubCode());
			}
		}
	}

	private static void assertValue(Buf expected, Buf actual) {
		if (!Utils.valueEquals(expected, actual)) {
			throw new IllegalStateException("unexpected benchmark value");
		}
	}

	private record ProbeCall(int keys, long bytes) {
	}
}
