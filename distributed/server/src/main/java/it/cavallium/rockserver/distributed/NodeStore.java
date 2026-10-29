package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.ratis.server.protocol.TermIndex;
import org.jetbrains.annotations.Nullable;

/**
 * The node's local Rockserver database plus the replication metadata stored inside it.
 *
 * <p>Metadata lives in the internal column {@value #META_COLUMN} (one fixed 1-byte key, with values), so that
 * it is written in the same RocksDB WAL as the data and travels inside checkpoints:</p>
 * <ul>
 *   <li>{@link #APPLIED}: the last applied {@link TermIndex}, encoded by {@link #encodeTermIndex}</li>
 *   <li>{@link #CLUSTER_ID}: UTF-8 cluster id, written when the {@link LogEntry.Genesis} entry is applied</li>
 * </ul>
 */
final class NodeStore implements Closeable {

	static final String META_COLUMN = "__rockserver_replication";
	static final byte APPLIED = 1;
	static final byte CLUSTER_ID = 2;

	private NodeStore() {}

	/**
	 * Opens (creating if needed) the database in {@code dbDir} with the given Rockserver config, and creates the
	 * meta column if missing.
	 */
	static NodeStore open(Path dbDir, @Nullable Path rockserverConfig, String name) throws IOException {
		throw new UnsupportedOperationException("TODO");
	}

	EmbeddedConnection connection() {
		throw new UnsupportedOperationException("TODO");
	}

	EmbeddedDB db() {
		throw new UnsupportedOperationException("TODO");
	}

	long metaColumnId() {
		throw new UnsupportedOperationException("TODO");
	}

	/** True if the database has any column besides the meta column. */
	boolean hasUserColumns() {
		throw new UnsupportedOperationException("TODO");
	}

	/** The persisted applied position, or {@code TermIndex.INITIAL_VALUE} (term 0, index -1) if none. */
	TermIndex applied() {
		throw new UnsupportedOperationException("TODO");
	}

	/** The persisted cluster id, or null before genesis. */
	@Nullable String clusterId() {
		throw new UnsupportedOperationException("TODO");
	}

	/**
	 * Writes a consistent checkpoint of the live database to {@code target} (must not exist), using RocksDB
	 * {@code Checkpoint}: memtables are flushed and SST files hard-linked. The caller guarantees that no entry is
	 * being applied concurrently. Returns the applied position contained in the checkpoint.
	 */
	TermIndex checkpoint(Path target) throws IOException {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public void close() throws IOException {
		throw new UnsupportedOperationException("TODO");
	}

	/**
	 * Replaces the contents of {@code dbDir} (which must not be open) with the checkpoint in {@code checkpointDir}.
	 * Immutable files ({@code *.sst}, {@code *.blob}) are hard-linked, the rest copied, so the checkpoint itself
	 * stays intact. Crash-safe: the new contents are prepared next to {@code dbDir} and swapped in by rename.
	 */
	static void restore(Path checkpointDir, Path dbDir) throws IOException {
		throw new UnsupportedOperationException("TODO");
	}

	static Keys key(byte metaKey) {
		return new Keys(Buf.wrap(new byte[] {metaKey}));
	}

	/** 16 bytes: term then index, big-endian. */
	static Buf encodeTermIndex(TermIndex termIndex) {
		throw new UnsupportedOperationException("TODO");
	}

	static TermIndex decodeTermIndex(Buf value) {
		throw new UnsupportedOperationException("TODO");
	}
}
