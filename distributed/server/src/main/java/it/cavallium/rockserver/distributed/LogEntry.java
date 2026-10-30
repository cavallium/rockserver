package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
	 * What a chained update read: the value of one key, as a length and a digest. Every replica compares it with the
	 * key's value right before applying the entry that carries it, inside the apply transaction, so the verdict
	 * depends only on the entry and the database state at its log position.
	 *
	 * @param length the value's size, -1 if the key was absent
	 * @param sha256 the SHA-256 digest of the value, null if the key was absent
	 */
	record Guard(long columnId, Keys keys, int length, byte @Nullable [] sha256) {

		static final int DIGEST_BYTES = 32;

		public Guard {
			Objects.requireNonNull(keys, "keys");
			if (length < -1 || (length >= 0) != (sha256 != null) || (sha256 != null && sha256.length != DIGEST_BYTES)) {
				throw new IllegalArgumentException("Invalid guard: length " + length);
			}
		}

		/** The guard of {@code value}, null if the key is absent. Digests it: never call this under the log lock. */
		static Guard of(long columnId, Keys keys, @Nullable Buf value) {
			return value == null ? new Guard(columnId, keys, -1, null)
					: new Guard(columnId, keys, value.size(), digest(value));
		}

		/** True if {@code current} (null if absent) is the value this guard describes. */
		boolean matches(@Nullable Buf current) {
			if (current == null || sha256 == null) {
				return current == null && sha256 == null;
			}
			return current.size() == length && MessageDigest.isEqual(sha256, digest(current));
		}

		private static byte[] digest(Buf value) {
			try {
				var digest = MessageDigest.getInstance("SHA-256");
				digest.update(value.asMemorySegment().asByteBuffer());
				return digest.digest();
			} catch (NoSuchAlgorithmException ex) {
				throw new IllegalStateException("Every Java platform has SHA-256", ex);
			}
		}
	}

	/**
	 * Writes of a transaction (or of a {@code getForUpdate} update) opened on the leader of term epoch
	 * {@code leaderEpoch}, reading the database as of log index {@code snapshotIndex}.
	 *
	 * <p>The leader rejects the entry before appending it if the leader changed or if any conflict key (or its
	 * whole column, after a {@code deleteRange}) was written by an entry after {@code snapshotIndex}. Once appended,
	 * all writes apply atomically, in order, unless the entry carries a {@link Guard} that does not match: then every
	 * replica rejects it with {@code UPDATE_RETRY}, and it writes nothing.</p>
	 *
	 * @param leaderEpoch random id of the leadership that opened the transaction
	 * @param snapshotIndex last log index visible to the transaction's reads. A chained update reads no snapshot: its
	 *                      entry carries the index it pinned the write history at when it opened
	 * @param conflictColumns column of each conflict key
	 * @param conflictHashes {@link ConflictTracker#keyHash} of each conflict key
	 * @param writes the writes, applied in order
	 * @param singleResult true to return the result of the only write (update commit), false to return true
	 * @param updateId the update's id on its leader, non-zero on every update commit, 0 otherwise. Only that leader
	 *                 uses it: apply ignores it
	 * @param guard what a chained update read, null for every other entry
	 */
	record Guarded(long leaderEpoch,
			long snapshotIndex,
			long[] conflictColumns,
			long[] conflictHashes,
			List<Write> writes,
			boolean singleResult,
			long updateId,
			@Nullable Guard guard) implements LogEntry {
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
			if (guard != null && updateId == 0) {
				throw new IllegalArgumentException("a guard requires an update id");
			}
			if (updateId != 0 && !singleResult) {
				throw new IllegalArgumentException("an update id requires singleResult");
			}
		}

		/** An entry that is not the commit of an update. */
		Guarded(long leaderEpoch,
				long snapshotIndex,
				long[] conflictColumns,
				long[] conflictHashes,
				List<Write> writes,
				boolean singleResult) {
			this(leaderEpoch, snapshotIndex, conflictColumns, conflictHashes, writes, singleResult, 0, null);
		}
	}

	/**
	 * First entry of a cluster. Binds every replica's database to {@code clusterId}. {@code seeded} tells whether
	 * the replicas started from a copy of an existing database; a replica with an empty database must then get
	 * the data from a snapshot, never from replaying the log.
	 */
	record Genesis(String clusterId, boolean seeded) implements LogEntry {}

	/**
	 * Proposed by the leader after it created column {@code name} as {@code columnId}: every replica checks that it
	 * assigned the same id, and stops if not, instead of silently writing other columns' data into it.
	 */
	record VerifyColumn(String name, long columnId) implements LogEntry {}
}
