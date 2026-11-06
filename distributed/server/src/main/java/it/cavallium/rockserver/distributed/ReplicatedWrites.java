package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static it.cavallium.rockserver.distributed.ReplicatedConnection.remainingNanos;
import static it.cavallium.rockserver.distributed.ReplicatedConnection.unwrap;

/** Replicated writes proposed from this node: single writes, column creation, and batches split into chunks. */
final class ReplicatedWrites {

	/** Bytes of keys and values per replicated chunk of a batch write. */
	private static final long BATCH_CHUNK_BYTES = 4L * 1024 * 1024;
	/** Encoding overhead counted per key segment and per value, so many tiny items still fit in one entry. */
	private static final long ITEM_OVERHEAD_BYTES = 16;

	private final Proposer proposer;

	ReplicatedWrites(Proposer proposer) {
		this.proposer = proposer;
	}

	/**
	 * Proposes a replicated write of a request that arrived at {@code startNanos}: it is resent only until the
	 * caller's deadline, and is not sent at all once that passed.
	 */
	CompletableFuture<Object> propose(RequestContext context, long startNanos, LogEntry entry) {
		return proposer.propose(entry, remainingNanos(context, startNanos), context.requestId());
	}

	CompletableFuture<?> createColumn(RequestContext context, long startNanos, String name,
			ColumnSchema schema) {
		if (NodeStore.META_COLUMN.equals(name)) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"Reserved column name: " + name));
		}
		return propose(context, startNanos, new LogEntry.CreateColumn(name, schema)).thenCompose(id ->
				// Every replica checks that it assigned the same id; the leader's apply produced this one.
				proposer.propose(new LogEntry.VerifyColumn(name, (Long) id)).handle((_, _) -> id));
	}

	/**
	 * Splits a batch stream into bounded replicated entries, applied in order. The caller's deadline counts from
	 * the start of the call for every chunk. Each chunk is applied on its own, so once one was acknowledged, a later
	 * failure (even a retryable one, such as a chunk refused before being sent) leaves the call partly applied: it
	 * fails with {@code INVALID_REQUEST}, which clients do not resend, and says how much was applied.
	 */
	CompletableFuture<Void> batch(RequestContext context, long columnId, Publisher<? extends KVBatch> batches,
			boolean merge) {
		long start = System.nanoTime();
		var appliedChunks = new AtomicLong();
		var appliedKeys = new AtomicLong();
		return Flux.from(batches)
				.concatMapIterable(ReplicatedWrites::chunks)
				// One chunk at a time: a later chunk must not overtake an earlier one writing the same key.
				.concatMap(chunk -> Mono.fromFuture(() -> propose(context.withRequestId(null), start, merge
								? new LogEntry.MergeMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING)
								: new LogEntry.PutMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING)))
						.doOnSuccess(_ -> {
							appliedChunks.incrementAndGet();
							appliedKeys.addAndGet(chunk.keys().size());
						}))
				.then()
				.onErrorMap(error -> appliedChunks.get() == 0 ? error : RocksDBException.of(
						RocksDBErrorType.INVALID_REQUEST,
						(merge ? "mergeBatch" : "putBatch") + " was partly applied: " + appliedChunks.get()
								+ " replicated chunks (" + appliedKeys.get() + " keys) were applied before it failed, so"
								+ " it is not safe to resend as a whole: " + unwrap(error)))
				.toFuture();
	}

	private static List<KVBatch.KVBatchRef> chunks(KVBatch batch) {
		var result = new ArrayList<KVBatch.KVBatchRef>();
		var keys = batch.keys();
		var values = batch.values();
		int start = 0;
		long bytes = 0;
		for (int i = 0; i < keys.size(); i++) {
			long size = size(keys.get(i)) + values.get(i).size() + ITEM_OVERHEAD_BYTES;
			if (i > start && bytes + size > BATCH_CHUNK_BYTES) {
				result.add(new KVBatch.KVBatchRef(keys.subList(start, i), values.subList(start, i)));
				start = i;
				bytes = 0;
			}
			bytes += size;
		}
		if (start < keys.size()) {
			result.add(new KVBatch.KVBatchRef(keys.subList(start, keys.size()), values.subList(start, values.size())));
		}
		return result;
	}

	private static long size(Keys keys) {
		long size = 0;
		for (Buf key : keys.keys()) {
			size += key.size() + ITEM_OVERHEAD_BYTES;
		}
		return size;
	}
}
