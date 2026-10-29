package it.cavallium.rockserver.distributed;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.jetbrains.annotations.Nullable;

/**
 * Ratis snapshot storage holding exactly one snapshot: a RocksDB checkpoint.
 *
 * <p>Layout, under the Ratis state machine directory: {@code snapshot/db/} (the checkpoint) and
 * {@code snapshot/snapshot.properties} (term, index; written last and fsynced, so its presence marks a complete
 * snapshot). New snapshots are built in {@code tmp/} and swapped in by rename. Ratis' own snapshot installation
 * writes the leader's files into {@link #getSnapshotDir()}, so an installed snapshot has the same layout.</p>
 */
final class CheckpointStorage implements StateMachineStorage {

	@Override
	public void init(RaftStorage raftStorage) throws IOException {
		throw new UnsupportedOperationException("TODO");
	}

	/** The latest complete snapshot, listing every file under the snapshot directory by absolute path. */
	@Override
	public @Nullable FileListSnapshotInfo getLatestSnapshot() {
		throw new UnsupportedOperationException("TODO");
	}

	/** Re-reads the snapshot directory, e.g. after Ratis installed a snapshot from the leader. */
	void reload() throws IOException {
		throw new UnsupportedOperationException("TODO");
	}

	/** Checkpoints {@code store} into a new snapshot that replaces the previous one. */
	TermIndex take(NodeStore store) throws IOException {
		throw new UnsupportedOperationException("TODO");
	}

	/** Directory of the latest snapshot's checkpoint, for {@link NodeStore#restore}. */
	Path snapshotDbDir() {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public void format() throws IOException {
	}

	@Override
	public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) {
		// Only one snapshot is ever kept.
	}

	@Override
	public File getSnapshotDir() {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public File getTmpDir() {
		throw new UnsupportedOperationException("TODO");
	}
}
