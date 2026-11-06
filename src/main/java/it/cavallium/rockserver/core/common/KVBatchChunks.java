package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits a {@link KVBatch} into bounded chunks, in order: the unit a replicated server writes as one log entry, and
 * the unit a client numbers when it sends a batch write under a client session. Both sides split with this class, so
 * a chunk made by the client is one chunk again on the server.
 *
 * <p>The split is greedy: a chunk ends before the item that would take it over {@link #CHUNK_BYTES}. A chunk holds at
 * least one item, so an item larger than the limit is a chunk of its own. Splitting a chunk again gives that chunk.</p>
 */
public final class KVBatchChunks {

	/** Bytes of keys and values per chunk, counting {@link #ITEM_OVERHEAD_BYTES} per key segment and per value. */
	public static final long CHUNK_BYTES = 4L * 1024 * 1024;
	/** Encoding overhead counted per key segment and per value, so many tiny items still fit in one entry. */
	public static final long ITEM_OVERHEAD_BYTES = 16;

	private KVBatchChunks() {
	}

	/** The chunks of {@code batch}, in order; none for an empty batch. */
	public static List<KVBatch.KVBatchRef> split(KVBatch batch) {
		var result = new ArrayList<KVBatch.KVBatchRef>();
		var keys = batch.keys();
		var values = batch.values();
		int start = 0;
		long bytes = 0;
		for (int i = 0; i < keys.size(); i++) {
			long size = size(keys.get(i)) + values.get(i).size() + ITEM_OVERHEAD_BYTES;
			if (i > start && bytes + size > CHUNK_BYTES) {
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
