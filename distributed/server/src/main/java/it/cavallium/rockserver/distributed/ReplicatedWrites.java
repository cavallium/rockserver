package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatchChunks;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static it.cavallium.rockserver.distributed.ReplicatedConnection.remainingNanos;
import static it.cavallium.rockserver.distributed.ReplicatedConnection.unwrap;

/** Replicated writes proposed from this node: single writes, column creation, and batches split into chunks. */
final class ReplicatedWrites {

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
	 * Splits a batch stream into bounded replicated entries ({@link KVBatchChunks}), applied in order. The caller's
	 * deadline counts from the start of the call for every chunk. Each chunk is applied on its own, so once one was
	 * acknowledged, a later failure (even a retryable one, such as a chunk refused before being sent) leaves the call
	 * partly applied: it fails with {@code INVALID_REQUEST}, which clients do not resend, and says how much was applied.
	 *
	 * <p>A call with a client request id is one numbered chunk of a client session (the client splits its batches the
	 * same way and numbers each chunk): it is proposed as one entry under that id, so a copy of it takes effect once. A
	 * numbered call that is more than one chunk is refused before anything is proposed.</p>
	 */
	CompletableFuture<Void> batch(RequestContext context, long columnId, Publisher<? extends KVBatch> batches,
			boolean merge) {
		long start = System.nanoTime();
		if (context.requestId() != null) {
			return Flux.from(batches)
					.concatMapIterable(KVBatchChunks::split)
					.collectList()
					.toFuture()
					.thenCompose(chunks -> {
						if (chunks.isEmpty()) {
							return CompletableFuture.completedFuture(null);
						}
						if (chunks.size() > 1) {
							return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
									"A numbered " + (merge ? "mergeBatch" : "putBatch") + " must fit one replicated chunk of "
											+ KVBatchChunks.CHUNK_BYTES + " bytes; it has " + chunks.size() + "; nothing was applied"));
						}
						return propose(context, start, entry(columnId, chunks.getFirst(), merge)).thenApply(_ -> null);
					});
		}
		var appliedChunks = new AtomicLong();
		var appliedKeys = new AtomicLong();
		return Flux.from(batches)
				.concatMapIterable(KVBatchChunks::split)
				// One chunk at a time: a later chunk must not overtake an earlier one writing the same key.
				.concatMap(chunk -> Mono.fromFuture(() -> propose(context, start, entry(columnId, chunk, merge)))
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

	private static LogEntry entry(long columnId, KVBatch chunk, boolean merge) {
		return merge
				? new LogEntry.MergeMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING)
				: new LogEntry.PutMulti(columnId, chunk.keys(), chunk.values(), RequestTypeId.NOTHING);
	}
}
