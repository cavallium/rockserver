package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.ClientBuilder;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.IOException;
import java.io.PrintStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import reactor.core.publisher.Flux;

/**
 * {@code copy-columns}: copies whole columns from a running Rockserver to another, through the client API (gRPC).
 *
 * <p>It reads each column with {@code getRange} (the logical, merged, key-ordered view: it never returns the stale or
 * deleted versions that a raw SST scan can return, it has no per-page item or byte ceiling, and for a column with
 * hashed variable-length keys it returns the logical keys that {@code putBatch} accepts back), batches the entries up to
 * {@code batchBytes}, and writes each batch with {@code putBatch}. Reading and writing overlap through a small bounded
 * queue, so memory stays about three batches whatever the size of the column. Values of a merge-operator column are
 * written as plain puts. The source must have no writers; the copy is not a snapshot across columns.</p>
 */
final class CopyColumns {

	static final long DEFAULT_BATCH_BYTES = 4L << 20;
	/** Names of Rockserver's own columns: never copied (the API does not list them; this is a second guard). */
	private static final Set<String> INTERNAL = Set.of("_column_schemas_", "_merge_operators_", "_cdc_meta_",
			NodeStore.META_COLUMN);
	private static final long PROGRESS_NANOS = Duration.ofSeconds(5).toNanos();
	/** Per entry accounting overhead, so that many tiny entries still form bounded batches. */
	private static final int ENTRY_OVERHEAD = 16;
	private static final Duration WRITE_TIMEOUT = Duration.ofMinutes(10);

	record Config(HostAndPort from, List<HostAndPort> to, List<String> columns, List<String> excludedColumns,
			boolean verify, boolean dryRun, long batchBytes, boolean allowNonEmpty) {}

	/** Wrong command line, found once the source is known (an unknown column): exit 2. */
	static final class UsageException extends RuntimeException {
		UsageException(String message) {
			super(message);
		}
	}

	/** The copy cannot go on, or did not give the same data: exit 1. */
	static final class CopyException extends RuntimeException {
		CopyException(String message) {
			super(message);
		}

		CopyException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	private record Result(long keys, long bytes) {}

	private record Fingerprint(long keys, long bytes, String sha256) {
		@Override
		public String toString() {
			return keys + " keys, " + bytes + " bytes, sha256 " + sha256;
		}
	}

	private final Config config;
	private final PrintStream out;
	private final PrintStream err;
	private RocksDBSyncAPI source;
	private RocksDBSyncAPI target;
	/** No timeout: a scan of a big column runs as long as it needs, and is cancelled when the stream is closed. */
	private RocksDBSyncAPI sourceScan;
	private RocksDBSyncAPI targetScan;

	private CopyColumns(Config config, PrintStream out, PrintStream err) {
		this.config = config;
		this.out = out;
		this.err = err;
	}

	static void run(Config config, PrintStream out, PrintStream err) throws IOException {
		try (var from = connect("copy-columns-source", List.of(config.from()));
				var to = connect("copy-columns-target", config.to())) {
			var copy = new CopyColumns(config, out, err);
			var timeout = RequestContext.batch(WRITE_TIMEOUT);
			copy.source = from.getSyncApi(timeout);
			copy.target = to.getSyncApi(timeout);
			copy.sourceScan = from.getSyncApi(RequestContext.batch());
			copy.targetScan = to.getSyncApi(RequestContext.batch());
			copy.execute();
		}
	}

	private static RocksDBConnection connect(String name, List<HostAndPort> addresses) throws IOException {
		var builder = new ClientBuilder();
		builder.setName(name);
		builder.setHttpAddresses(addresses);
		return builder.build();
	}

	private void execute() {
		var selected = select();
		var targetDefinitions = target.getAllColumnDefinitions();

		// Everything that can refuse the copy is checked before anything is written or created.
		var targetIds = new TreeMap<String, Long>();
		for (var entry : selected.entrySet()) {
			var name = entry.getKey();
			var schema = entry.getValue();
			var existing = targetDefinitions.get(name);
			if (existing == null) {
				continue;
			}
			if (!existing.equals(schema)) {
				throw new CopyException("column " + name + " exists on the target with another schema: source " + schema
						+ ", target " + existing + ". Delete it on the target or exclude it");
			}
			long id = target.getColumnId(name);
			targetIds.put(name, id);
			if (!config.allowNonEmpty() && hasData(targetScan, id)) {
				throw new CopyException("column " + name + " already holds data on the target; nothing was copied."
						+ " Re-run with --allow-non-empty to overwrite the same keys (as after a failed copy)");
			}
		}

		if (config.dryRun()) {
			for (var entry : selected.entrySet()) {
				var name = entry.getKey();
				out.println("column " + name + ": ~" + source.estimateNumKeys(source.getColumnId(name))
						+ " keys (estimate), schema " + entry.getValue() + ", target "
						+ (targetIds.containsKey(name) ? "has it with the same schema" : "does not have it"));
			}
			out.println("DRY-RUN OK: " + selected.size() + " columns would be copied, nothing was written");
			return;
		}

		// Columns with a merge operator first: if the target lacks the operator, nothing else has been created or copied.
		var order = new ArrayList<>(selected.keySet());
		order.sort((a, b) -> Boolean.compare(!hasMergeOperator(selected.get(a)), !hasMergeOperator(selected.get(b))));
		for (var name : order) {
			if (targetIds.containsKey(name)) {
				continue;
			}
			var schema = selected.get(name);
			try {
				targetIds.put(name, target.createColumn(name, schema));
			} catch (RuntimeException ex) {
				if (hasMergeOperator(schema)) {
					throw new CopyException("column " + name + " uses merge operator "
							+ (schema.mergeOperatorName() != null
									? schema.mergeOperatorName() + " version " + schema.mergeOperatorVersion()
									: "class " + schema.mergeOperatorClass())
							+ ", which the target could not register (" + ex.getMessage() + "). The operator jar cannot be"
							+ " read back from the source through the API: upload it to the target first"
							+ " (uploadMergeOperator, same name, so that it gets the same version), then re-run", ex);
				}
				throw new CopyException("cannot create column " + name + " on the target: " + ex.getMessage(), ex);
			}
		}

		for (var name : selected.keySet()) {
			long sourceId = source.getColumnId(name);
			long targetId = targetIds.get(name);
			long start = System.nanoTime();
			Result result;
			try {
				result = copy(name, sourceId, targetId, start);
			} catch (CopyException ex) {
				throw ex;
			} catch (RuntimeException ex) {
				throw new CopyException("column " + name + " failed: " + ex
						+ ". Nothing is rolled back: re-run the same command with --allow-non-empty to overwrite the same keys", ex);
			}
			double seconds = Math.max(1e-9, (System.nanoTime() - start) / 1e9);
			out.println("column " + name + ": " + result.keys() + " keys, " + result.bytes() + " bytes, "
					+ String.format("%.1f s, %.0f keys/s, %.1f MiB/s", seconds, result.keys() / seconds,
							result.bytes() / seconds / (1 << 20)));
			if (config.verify()) {
				verify(name, sourceId, targetId);
			}
		}
		out.println("OK");
	}

	/** The columns to copy, with the source's schemas, in name order. */
	private TreeMap<String, ColumnSchema> select() {
		var all = new TreeMap<>(source.getAllColumnDefinitions());
		all.keySet().removeIf(INTERNAL::contains);
		for (var name : config.columns()) {
			if (!all.containsKey(name)) {
				throw new UsageException("unknown column " + name + " (the source has " + all.keySet() + ")");
			}
		}
		for (var name : config.excludedColumns()) {
			if (!all.containsKey(name)) {
				throw new UsageException("unknown --exclude-column " + name + " (the source has " + all.keySet() + ")");
			}
		}
		var selected = new TreeMap<String, ColumnSchema>();
		for (var entry : all.entrySet()) {
			if ((config.columns().isEmpty() || config.columns().contains(entry.getKey()))
					&& !config.excludedColumns().contains(entry.getKey())) {
				selected.put(entry.getKey(), entry.getValue());
			}
		}
		if (selected.isEmpty()) {
			throw new UsageException("no column to copy");
		}
		return selected;
	}

	private static boolean hasMergeOperator(ColumnSchema schema) {
		return schema.mergeOperatorName() != null || schema.mergeOperatorClass() != null;
	}

	private static boolean hasData(RocksDBSyncAPI api, long columnId) {
		try (var entries = api.getRange(0, columnId, null, null, false, RequestType.<KV>allInRange())) {
			return entries.findAny().isPresent();
		}
	}

	private static final Object END = new Object();

	private record Failed(Throwable error) {}

	private record Batch(List<Keys> keys, List<Buf> values, long bytes) {}

	private Result copy(String name, long sourceId, long targetId, long start) {
		BlockingQueue<Object> queue = new ArrayBlockingQueue<>(2);
		var reader = Thread.ofPlatform().name("copy-columns-reader-" + name).daemon().start(() -> {
			try (var entries = sourceScan.getRange(0, sourceId, null, null, false, RequestType.<KV>allInRange())) {
				var keys = new ArrayList<Keys>();
				var values = new ArrayList<Buf>();
				long bytes = 0;
				var iterator = entries.iterator();
				while (iterator.hasNext()) {
					var kv = iterator.next();
					keys.add(kv.keys());
					values.add(kv.value() != null ? kv.value() : Utils.emptyBuf());
					bytes += size(kv);
					if (bytes >= config.batchBytes()) {
						queue.put(new Batch(keys, values, bytes));
						keys = new ArrayList<>();
						values = new ArrayList<>();
						bytes = 0;
					}
				}
				if (!keys.isEmpty()) {
					queue.put(new Batch(keys, values, bytes));
				}
				queue.put(END);
			} catch (InterruptedException ex) {
				// The writer failed and stopped reading.
			} catch (Throwable ex) {
				try {
					queue.put(new Failed(ex));
				} catch (InterruptedException ignored) {
					// Nobody reads any more.
				}
			}
		});
		long totalKeys = 0;
		long totalBytes = 0;
		long lastProgress = start;
		try {
			while (true) {
				Object item = queue.take();
				if (item == END) {
					break;
				}
				if (item instanceof Failed failed) {
					throw new CopyException("column " + name + ": reading the source failed after " + totalKeys
							+ " keys: " + failed.error() + ". Re-run with --allow-non-empty", failed.error());
				}
				var batch = (Batch) item;
				target.putBatch(targetId, Flux.just(new KVBatch.KVBatchRef(batch.keys(), batch.values())),
						PutBatchMode.WRITE_BATCH);
				totalKeys += batch.keys().size();
				totalBytes += batch.bytes();
				long now = System.nanoTime();
				if (now - lastProgress >= PROGRESS_NANOS) {
					lastProgress = now;
					double seconds = (now - start) / 1e9;
					err.println("column " + name + ": " + totalKeys + " keys, " + totalBytes + " bytes, "
							+ String.format("%.0f keys/s, %.1f MiB/s", totalKeys / seconds,
									totalBytes / seconds / (1 << 20)));
				}
			}
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new CopyException("column " + name + ": interrupted", ex);
		} finally {
			reader.interrupt();
		}
		return new Result(totalKeys, totalBytes);
	}

	private static long size(KV kv) {
		long size = ENTRY_OVERHEAD + (kv.value() != null ? kv.value().size() : 0);
		for (var key : kv.keys().keys()) {
			size += key.size();
		}
		return size;
	}

	private void verify(String name, long sourceId, long targetId) {
		var sourceFingerprint = CompletableFuture.supplyAsync(() -> fingerprint(sourceScan, sourceId));
		var targetFingerprint = CompletableFuture.supplyAsync(() -> fingerprint(targetScan, targetId));
		Fingerprint expected;
		Fingerprint actual;
		try {
			expected = sourceFingerprint.get();
			actual = targetFingerprint.get();
		} catch (ExecutionException ex) {
			throw new CopyException("column " + name + ": verification could not read: " + ex.getCause(), ex.getCause());
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new CopyException("column " + name + ": verification interrupted", ex);
		}
		if (!expected.equals(actual)) {
			throw new CopyException("column " + name + ": verification FAILED: source has " + expected
					+ ", target has " + actual);
		}
		out.println("column " + name + ": verified (" + expected + ")");
	}

	/** Count and SHA-256 over every entry in key order: key parts and value, each with its length. */
	private static Fingerprint fingerprint(RocksDBSyncAPI api, long columnId) {
		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException ex) {
			throw new IllegalStateException(ex);
		}
		long keys = 0;
		long bytes = 0;
		var lengthBytes = new byte[4];
		try (var entries = api.getRange(0, columnId, null, null, false, RequestType.<KV>allInRange())) {
			var iterator = entries.iterator();
			while (iterator.hasNext()) {
				var kv = iterator.next();
				keys++;
				digest.update((byte) kv.keys().keys().length);
				for (var key : kv.keys().keys()) {
					bytes += update(digest, key, lengthBytes);
				}
				bytes += update(digest, kv.value() != null ? kv.value() : Utils.emptyBuf(), lengthBytes);
			}
		}
		return new Fingerprint(keys, bytes, HexFormat.of().formatHex(digest.digest()));
	}

	private static long update(MessageDigest digest, Buf buf, byte[] lengthBytes) {
		int size = buf.size();
		lengthBytes[0] = (byte) (size >>> 24);
		lengthBytes[1] = (byte) (size >>> 16);
		lengthBytes[2] = (byte) (size >>> 8);
		lengthBytes[3] = (byte) size;
		digest.update(lengthBytes);
		digest.update(buf.getBackingByteArray(), buf.getBackingByteArrayOffset(), buf.getBackingByteArrayLength());
		return size;
	}
}
