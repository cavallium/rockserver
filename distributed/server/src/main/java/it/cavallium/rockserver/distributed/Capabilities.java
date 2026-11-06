package it.cavallium.rockserver.distributed;

import java.util.ArrayList;
import java.util.List;

/**
 * What a node's version can do, as bits, and the gate that keeps a cluster from using it before every member can.
 *
 * <p>A feature that writes log entries, or entry flags, that an older node cannot read must wait until every member
 * runs a version that can: such a node would stop on the first of them, and a majority of such nodes would stop the
 * cluster. So the cluster has a version, the {@link LogEntry.ClusterVersion} entries applied so far (kept in the meta
 * column of every replica): the capabilities that every member had when the leader proposed them. The leader asks every
 * member of its configuration what it supports ({@link ReplicaOps#capabilitiesAnswer}, on the Raft port), and proposes a
 * higher version once all of them answered with more than the cluster has; a member that does not answer, or predates
 * the question, holds the version where it is. The version never goes down: {@code admin add-peer} refuses a node that
 * lacks a capability of the cluster, and a member must not be downgraded below it.</p>
 *
 * <p>A cluster created by a version with this gate starts at {@link #GENESIS} ({@link LogEntry.Genesis} writes it):
 * every node of a new cluster must run the same version. A cluster created before has no version until the leader
 * proposes one, once every member was upgraded.</p>
 */
final class Capabilities {

	/** {@link LogEntry.Guarded} entries with an update id and a guard: {@code --pipelined-updates}. */
	static final long PIPELINED_UPDATES = 1;
	/** Client sessions: version 3 batches, leader stamps and {@link LogEntry.RegisterClient}. */
	static final long CLIENT_SESSIONS = 1 << 1;
	/** {@link LogEntry.Checksum}. */
	static final long CHECKSUM = 1 << 2;
	/** {@link LogEntry.Backup}. */
	static final long BACKUP = 1 << 3;

	/** Everything this version supports. */
	static final long SUPPORTED = PIPELINED_UPDATES | CLIENT_SESSIONS | CHECKSUM | BACKUP;
	/** The version of a cluster created by this version (see the class documentation). */
	static final long GENESIS = SUPPORTED;

	private Capabilities() {}

	static boolean has(long version, long capability) {
		return (version & capability) == capability;
	}

	/** The names of the bits of {@code capabilities}, for logs and messages. */
	static String describe(long capabilities) {
		var names = new ArrayList<String>();
		for (var bit : List.of(PIPELINED_UPDATES, CLIENT_SESSIONS, CHECKSUM, BACKUP)) {
			if (has(capabilities, bit)) {
				names.add(name(bit));
			}
		}
		long unknown = capabilities & ~SUPPORTED;
		if (unknown != 0) {
			names.add("0x" + Long.toHexString(unknown));
		}
		return names.isEmpty() ? "none" : String.join(",", names);
	}

	private static String name(long bit) {
		if (bit == PIPELINED_UPDATES) {
			return "pipelined-updates";
		} else if (bit == CLIENT_SESSIONS) {
			return "client-sessions";
		} else if (bit == CHECKSUM) {
			return "checksum";
		} else {
			return "backup";
		}
	}
}
