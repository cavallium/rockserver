package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.ExistsMultiPerfSampler;

import static org.junit.jupiter.api.Assertions.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

class ExistsMultiPerfSamplerNativeTest {
	@TempDir Path directory;

	@Test void sampledStatusOnlyMultiGetPreservesOrderedPresenceAndReportsCallerReads() throws Exception {
		RocksDB.loadLibrary();
		try (var options = new Options().setCreateIfMissing(true)
				.setTableFormatConfig(new BlockBasedTableConfig().setNoBlockCache(true))) {
			try (var db = RocksDB.open(options, directory.toString()); var flush = new FlushOptions().setWaitForFlush(true)) {
				for (int i = 0; i < 128; i++) db.put(new byte[]{(byte)i}, new byte[4096]);
				db.flush(flush);
			}
			try (var db = RocksDB.open(options, directory.toString()); var read = new ReadOptions().setFillCache(false);
					var arena = Arena.ofConfined()) {
				var observed = new AtomicReference<ExistsMultiPerfSampler.Observation>();
				var sampler = new ExistsMultiPerfSampler(1, Long.MAX_VALUE, 1, System::nanoTime, observed::set, () -> fail());
				var keys = new MemorySegment[]{arena.allocateFrom(java.lang.foreign.ValueLayout.JAVA_BYTE, new byte[]{3}), arena.allocateFrom(java.lang.foreign.ValueLayout.JAVA_BYTE, new byte[]{(byte)200}),
						arena.allocateFrom(java.lang.foreign.ValueLayout.JAVA_BYTE, new byte[]{7}), arena.allocateFrom(java.lang.foreign.ValueLayout.JAVA_BYTE, new byte[]{3})};
				var values = new MemorySegment[]{arena.allocate(0), arena.allocate(0), arena.allocate(0), arena.allocate(0)};
				var prior = db.getPerfLevel();
				var sample = sampler.begin(db); assertNotNull(sample);
				List<ByteBufferGetStatus> statuses;
				try { statuses = db.multiGetByteBuffers(read, List.of(db.getDefaultColumnFamily()), keys, values); }
				finally { sampler.finish(sample); }
				assertEquals(List.of(true, false, true, true), statuses.stream().map(s -> s.status.getCode() == Status.Code.Ok).toList());
				assertEquals(prior, db.getPerfLevel());
				assertTrue(observed.get().counters().reads() > 0);
				assertTrue(observed.get().counters().bytes() > 0);
				assertTrue(observed.get().nativeNanos() > 0);
			}
		}
	}
	@Test void embeddedHookSamplesOnlyLatencyStatusOnlyAndHonorsBudget() throws Exception {
		String prefix = "rockserver.exists-perf.";
		var properties = List.of(prefix + "interval-ms", prefix + "window-ms", prefix + "budget");
		var prior = properties.stream().map(System::getProperty).toList();
		try {
			System.setProperty(properties.get(0), "10");
			System.setProperty(properties.get(1), "300000");
			System.setProperty(properties.get(2), "1");
			try (var connection = new it.cavallium.rockserver.core.client.EmbeddedConnection(null, "sample-hook", null)) {
				var api = connection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
				var latency = connection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.latency(java.time.Duration.ofSeconds(5)));
				long column = api.createColumn("fixed", it.cavallium.rockserver.core.common.ColumnSchema.of(
						it.unimi.dsi.fastutil.ints.IntList.of(1), it.unimi.dsi.fastutil.objects.ObjectList.of(), true));
				var keys = List.of(new it.cavallium.rockserver.core.common.Keys(it.cavallium.buffer.Buf.wrap(new byte[]{1})));
				var counter = connection.getInternalDB().getMetricsRegistry().get("rockserver.exists.perf.caller.samples").counter();
				assertEquals(List.of(false), api.existsMulti(0, column, keys));
				assertEquals(0, counter.count());
				assertEquals(List.of(false), latency.existsMulti(0, column, keys));
				assertEquals(1, counter.count());
				assertEquals(List.of(false), connection.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.latency(java.time.Duration.ofSeconds(5))).existsMultiAsync(0, column, keys).join());
				assertEquals(1, counter.count());
			}
		} finally {
			for (int i = 0; i < properties.size(); i++) {
				if (prior.get(i) == null) System.clearProperty(properties.get(i));
				else System.setProperty(properties.get(i), prior.get(i));
			}
		}
	}

	@Test void invalidOrExpiredConfigurationCreatesNoDiagnosticMeters() throws Exception {
		String prefix = "rockserver.exists-perf.";
		var properties = List.of(prefix + "interval-ms", prefix + "window-ms", prefix + "budget", prefix + "not-before-epoch-millis");
		var prior = properties.stream().map(System::getProperty).toList();
		try {
			String[][] invalid = {{"0", "100", "1", ""}, {"10", "300001", "1", ""},
					{"10", "100", "1001", ""}, {"10", "100", "1", "bad"},
					{"10", "100", "1", "1"}, {"10", "100", "1", Long.toString(Long.MAX_VALUE)}};
			for (var values : invalid) {
				for (int i = 0; i < properties.size(); i++) {
					if (values[i].isEmpty()) System.clearProperty(properties.get(i));
					else System.setProperty(properties.get(i), values[i]);
				}
				try (var connection = new it.cavallium.rockserver.core.client.EmbeddedConnection(null, "invalid-sample", null)) {
					assertNull(connection.getInternalDB().getMetricsRegistry().find("rockserver.exists.perf.caller.samples").counter());
				}
			}
		} finally {
			for (int i = 0; i < properties.size(); i++) {
				if (prior.get(i) == null) System.clearProperty(properties.get(i));
				else System.setProperty(properties.get(i), prior.get(i));
			}
		}
	}

}
