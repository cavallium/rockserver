package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * A command in the replicated log. Every replica applies the same entries in the same order to its own
 * {@code EmbeddedDB}, so applying an entry must depend only on the entry and the database state.
 *
 * <p>Request types select what a write returns (previous value, delta, ...). Inside a {@link Guarded}
 * transaction commit they are always {@link RequestTypeId#NOTHING}, except for a single-write update commit.</p>
 */
public sealed interface LogEntry {

	/** A data write. */
	sealed interface Write extends LogEntry {
		long columnId();
	}

	record Put(long columnId, Keys keys, Buf value, RequestTypeId requestType) implements Write {}

	record PutMulti(long columnId, List<Keys> keys, List<Buf> values, RequestTypeId requestType) implements Write {
		public PutMulti {
			keys = List.copyOf(keys);
			values = List.copyOf(values);
			if (keys.size() != values.size()) {
				throw new IllegalArgumentException("keys and values sizes differ");
			}
		}
	}

	record Delete(long columnId, Keys keys, RequestTypeId requestType) implements Write {}

	record DeleteMulti(long columnId, List<Keys> keys, RequestTypeId requestType) implements Write {
		public DeleteMulti {
			keys = List.copyOf(keys);
		}
	}

	record Merge(long columnId, Keys keys, Buf value, RequestTypeId requestType) implements Write {}

	record MergeMulti(long columnId, List<Keys> keys, List<Buf> values, RequestTypeId requestType) implements Write {
		public MergeMulti {
			keys = List.copyOf(keys);
			values = List.copyOf(values);
			if (keys.size() != values.size()) {
				throw new IllegalArgumentException("keys and values sizes differ");
			}
		}
	}

	/** Deletes {@code [start, end)}; a null bound is open. */
	record DeleteRange(long columnId, @Nullable Keys start, @Nullable Keys end) implements Write {}

	record CreateColumn(String name, ColumnSchema schema) implements LogEntry {}

	record DeleteColumn(long columnId) implements LogEntry {}

	record DeleteColumnIfExists(String name) implements LogEntry {}

	record UploadMergeOperator(String name, String className, byte[] jarData) implements LogEntry {}

	/**
	 * Writes of a transaction (or of a {@code getForUpdate} update) opened on the leader of term epoch
	 * {@code leaderEpoch}, reading the database as of log index {@code snapshotIndex}.
	 *
	 * <p>The leader rejects the entry before appending it if the leader changed or if any conflict key (or its
	 * whole column, after a {@code deleteRange}) was written by an entry after {@code snapshotIndex}. Once
	 * appended, it is applied unconditionally: all writes atomically, in order.</p>
	 *
	 * @param leaderEpoch random id of the leadership that opened the transaction
	 * @param snapshotIndex last log index visible to the transaction's reads
	 * @param conflictColumns column of each conflict key
	 * @param conflictHashes {@link ConflictTracker#keyHash} of each conflict key
	 * @param writes the writes, applied in order
	 * @param singleResult true to return the result of the only write (update commit), false to return true
	 */
	record Guarded(long leaderEpoch,
			long snapshotIndex,
			long[] conflictColumns,
			long[] conflictHashes,
			List<Write> writes,
			boolean singleResult) implements LogEntry {
		public Guarded {
			Objects.requireNonNull(conflictColumns, "conflictColumns");
			Objects.requireNonNull(conflictHashes, "conflictHashes");
			writes = List.copyOf(writes);
			if (conflictColumns.length != conflictHashes.length) {
				throw new IllegalArgumentException("conflict arrays sizes differ");
			}
			if (singleResult && writes.size() != 1) {
				throw new IllegalArgumentException("singleResult requires exactly one write");
			}
		}
	}

	/**
	 * First entry of a cluster. Binds every replica's database to {@code clusterId}. {@code seeded} tells whether
	 * the replicas started from a copy of an existing database; a replica with an empty database must then get
	 * the data from a snapshot, never from replaying the log.
	 */
	record Genesis(String clusterId, boolean seeded) implements LogEntry {}
}
