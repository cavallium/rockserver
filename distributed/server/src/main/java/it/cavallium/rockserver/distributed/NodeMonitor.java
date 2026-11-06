package it.cavallium.rockserver.distributed;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The state of a node that alerts watch, read once per {@link #REFRESH_MILLIS} on the node's maintenance thread and
 * exported as gauges ({@link NodeMetrics#exportedGauge}): Raft role, term and indices, the lags between them, each
 * follower's lag on the leader, the size of the Raft log, the age of the latest snapshot and the free space of the
 * disks of the database and of the Raft log. The metrics pipeline's thread only reads fields, never Ratis or the disk.
 *
 * <p>It also raises the alarms that a node can tell by itself, as log lines: a disk that runs out of space, a Raft log
 * that keeps growing (a member that is gone for good holds it back) and RocksDB stopping or slowing writes (a write
 * stall: the compactions do not keep up, or the disk is full). Each alarm logs a warning when it starts and every
 * {@link #REPEAT_MILLIS} ms while it lasts, and an info line when it ends. The rules for the metrics pipeline are in
 * {@code docs/distributed/alerts/}.</p>
 */
final class NodeMonitor {

	private static final Logger LOG = LoggerFactory.getLogger(NodeMonitor.class);
	static final long REFRESH_MILLIS = 1_000;
	private static final long REPEAT_MILLIS = 60_000;
	/** A disk with less than this share of its space free is low. */
	static final double LOW_DISK_RATIO = 0.10;
	/** The Raft log is too large past this many times the bytes after which a snapshot is taken (and the log purged). */
	static final int LOG_SIZE_SNAPSHOTS = 4;
	/** Raft log segment files: {@code log_<start>-<end>} and {@code log_inprogress_<start>}. */
	private static final String SEGMENT_PREFIX = "log_";

	/** Values of {@code raft.role}. */
	static final int FOLLOWER = 0;
	static final int CANDIDATE = 1;
	static final int LEADER = 2;
	static final int UNKNOWN = -1;

	private final NodeConfig config;
	private final Supplier<RaftServer.@Nullable Division> division;
	private final RockserverStateMachine stateMachine;
	private final Path raftLogDir;
	private final NodeMetrics metrics;

	private volatile double role = UNKNOWN;
	private volatile double term = Double.NaN;
	private volatile double lastIndex = Double.NaN;
	private volatile double commitIndex = Double.NaN;
	private volatile double flushIndex = Double.NaN;
	private volatile double startIndex = Double.NaN;
	private volatile double appliedIndex = Double.NaN;
	private volatile double logBytes = Double.NaN;
	private volatile double snapshotIndex = Double.NaN;
	private volatile double snapshotAgeSeconds = Double.NaN;
	private volatile double followerLagMax = Double.NaN;
	private final Disk dataDisk;
	private final Disk raftDisk;
	private final Map<String, double[]> followerLags = new ConcurrentHashMap<>();

	private final Alarm lowDisk = new Alarm("disk-space");
	private final Alarm largeLog = new Alarm("raft-log-size");
	private final Alarm writeStopped = new Alarm("write-stopped");
	private final Alarm writeDelayed = new Alarm("write-delayed");

	/** Free and total space of the file system of one directory. */
	private static final class Disk {
		final Path dir;
		volatile double usableBytes = Double.NaN;
		volatile double totalBytes = Double.NaN;

		Disk(Path dir) {
			this.dir = dir;
		}

		void refresh() {
			try {
				var store = Files.getFileStore(dir);
				usableBytes = store.getUsableSpace();
				totalBytes = store.getTotalSpace();
			} catch (IOException | RuntimeException ex) {
				usableBytes = Double.NaN;
				totalBytes = Double.NaN;
			}
		}

		double usablePercent() {
			return totalBytes > 0 ? 100 * usableBytes / totalBytes : Double.NaN;
		}

		boolean low() {
			return totalBytes > 0 && usableBytes < LOW_DISK_RATIO * totalBytes;
		}
	}

	/** One condition, logged when it starts, every {@link #REPEAT_MILLIS} ms while it lasts, and when it ends. */
	static final class Alarm {
		final String name;
		private boolean on;
		private long lastLoggedMillis;

		Alarm(String name) {
			this.name = name;
		}

		/** The condition holds: returns whether to log it now. */
		boolean raised(long nowMillis) {
			if (!on || nowMillis - lastLoggedMillis >= REPEAT_MILLIS) {
				on = true;
				lastLoggedMillis = nowMillis;
				return true;
			}
			return false;
		}

		/** The condition does not hold: returns whether it just ended. */
		boolean cleared() {
			boolean was = on;
			on = false;
			return was;
		}
	}

	NodeMonitor(NodeConfig config, Supplier<RaftServer.@Nullable Division> division, RockserverStateMachine stateMachine,
			Path raftLogDir, NodeMetrics metrics) {
		this.config = config;
		this.division = division;
		this.stateMachine = stateMachine;
		this.raftLogDir = raftLogDir;
		this.metrics = metrics;
		this.dataDisk = new Disk(config.dbDir().getParent());
		this.raftDisk = new Disk(config.raftDir());
		metrics.exportedGauge("raft.role", () -> role);
		metrics.exportedGauge("raft.term", () -> term);
		metrics.exportedGauge("raft.last_index", () -> lastIndex);
		metrics.exportedGauge("raft.commit_index", () -> commitIndex);
		metrics.exportedGauge("raft.flush_index", () -> flushIndex);
		metrics.exportedGauge("raft.start_index", () -> startIndex);
		metrics.exportedGauge("raft.applied_index", () -> appliedIndex);
		// Appended but not committed: on a leader, what its followers have not confirmed yet.
		metrics.exportedGauge("raft.commit_lag", () -> lastIndex - commitIndex);
		// Committed but not applied: a slow or stuck apply.
		metrics.exportedGauge("raft.apply_lag", () -> commitIndex - appliedIndex);
		metrics.exportedGauge("raft.flush_lag", () -> lastIndex - flushIndex);
		metrics.exportedGauge("raft.log.bytes", () -> logBytes);
		metrics.exportedGauge("raft.log.entries", () -> lastIndex - startIndex + 1);
		metrics.exportedGauge("raft.follower_lag_max", () -> followerLagMax);
		metrics.exportedGauge("snapshot.index", () -> snapshotIndex);
		metrics.exportedGauge("snapshot.age_seconds", () -> snapshotAgeSeconds);
		metrics.exportedGauge("disk.data.usable_bytes", () -> dataDisk.usableBytes);
		metrics.exportedGauge("disk.data.total_bytes", () -> dataDisk.totalBytes);
		metrics.exportedGauge("disk.data.usable_percent", dataDisk::usablePercent);
		metrics.exportedGauge("disk.raft.usable_bytes", () -> raftDisk.usableBytes);
		metrics.exportedGauge("disk.raft.total_bytes", () -> raftDisk.totalBytes);
		metrics.exportedGauge("disk.raft.usable_percent", raftDisk::usablePercent);
		metrics.exportedGauge("replica.failed", () -> stateMachine.isFailed() ? 1 : 0);
	}

	/** Reads the state and raises or clears the alarms. Maintenance thread only. */
	void refresh() {
		long now = System.currentTimeMillis();
		refreshRaft();
		refreshSnapshot(now);
		logBytes = raftLogBytes(raftLogDir);
		dataDisk.refresh();
		raftDisk.refresh();
		checkDisks(now);
		checkLogSize(now);
		checkWriteStall(now);
	}

	private void refreshRaft() {
		var division = this.division.get();
		appliedIndex = stateMachine.appliedIndex().get();
		if (division == null || !division.getInfo().isAlive()) {
			role = UNKNOWN;
			clearFollowerLags();
			return;
		}
		var info = division.getInfo();
		role = info.isLeader() ? LEADER : info.isCandidate() ? CANDIDATE : info.isFollower() ? FOLLOWER : UNKNOWN;
		term = info.getCurrentTerm();
		var log = division.getRaftLog();
		var last = log.getLastEntryTermIndex();
		lastIndex = last == null ? -1 : last.getIndex();
		commitIndex = log.getLastCommittedIndex();
		flushIndex = log.getFlushIndex();
		startIndex = log.getStartIndex();
		if (!info.isLeader()) {
			clearFollowerLags();
			return;
		}
		// The commit index that the leader knows of each member: how far behind each follower is.
		double max = 0;
		var seen = new HashSet<String>();
		for (var commitInfo : division.getCommitInfos()) {
			var id = RaftPeerId.valueOf(commitInfo.getServer().getId()).toString();
			if (id.equals(config.nodeId())) {
				continue;
			}
			double lag = Math.max(0, commitIndex - commitInfo.getCommitIndex());
			max = Math.max(max, lag);
			seen.add(id);
			followerLag(id)[0] = lag;
		}
		followerLags.forEach((id, lag) -> {
			if (!seen.contains(id)) {
				lag[0] = Double.NaN;
			}
		});
		followerLagMax = max;
	}

	private void clearFollowerLags() {
		followerLagMax = Double.NaN;
		followerLags.values().forEach(lag -> lag[0] = Double.NaN);
	}

	/** The gauge of a follower's lag, registered the first time this node leads it. */
	private double[] followerLag(String id) {
		return followerLags.computeIfAbsent(id, follower -> {
			var lag = new double[] {Double.NaN};
			metrics.exportedGauge("raft.follower_lag", "follower", follower, () -> lag[0]);
			return lag;
		});
	}

	private void refreshSnapshot(long nowMillis) {
		var snapshot = stateMachine.getLatestSnapshot();
		if (snapshot == null) {
			snapshotIndex = Double.NaN;
			snapshotAgeSeconds = Double.NaN;
			return;
		}
		snapshotIndex = snapshot.getIndex();
		long taken = stateMachine.latestSnapshotTimeMillis();
		snapshotAgeSeconds = taken > 0 ? Math.max(0, (nowMillis - taken) / 1000.0) : Double.NaN;
	}

	/** The bytes of the segment files of the Raft log in {@code dir}, NaN if it cannot be read. */
	static double raftLogBytes(Path dir) {
		try (Stream<Path> files = Files.list(dir)) {
			return files.filter(file -> file.getFileName().toString().startsWith(SEGMENT_PREFIX))
					.mapToLong(file -> {
						try {
							return Files.size(file);
						} catch (IOException deleted) {
							return 0;
						}
					})
					.sum();
		} catch (IOException | RuntimeException ex) {
			return Double.NaN;
		}
	}

	private void checkDisks(long now) {
		if (dataDisk.low() || raftDisk.low()) {
			if (lowDisk.raised(now)) {
				LOG.warn("{}: ALARM {}: {} free of {} for the database ({}), {} free of {} for the Raft log ({}). A snapshot"
								+ " keeps hard links to the files that compactions replaced, and a full disk stops this node's writes:"
								+ " free space or grow the disk. See docs/distributed/runbooks.md", config.nodeId(), lowDisk.name,
						bytes(dataDisk.usableBytes), bytes(dataDisk.totalBytes), dataDisk.dir, bytes(raftDisk.usableBytes),
						bytes(raftDisk.totalBytes), raftDisk.dir);
			}
		} else if (lowDisk.cleared()) {
			LOG.info("{}: alarm {} cleared", config.nodeId(), lowDisk.name);
		}
	}

	private void checkLogSize(long now) {
		double limit = (double) LOG_SIZE_SNAPSHOTS * config.snapshotEveryBytes();
		if (logBytes > limit) {
			if (largeLog.raised(now)) {
				LOG.warn("{}: ALARM {}: the Raft log holds {} ({} entries), more than {} snapshots' worth. Ratis purges it"
								+ " only up to the lowest commit index of any member, so a member that is gone for good holds it back:"
								+ " remove or replace it. See docs/distributed/runbooks.md", config.nodeId(), largeLog.name,
						bytes(logBytes), (long) (lastIndex - startIndex + 1), LOG_SIZE_SNAPSHOTS);
			}
		} else if (largeLog.cleared()) {
			LOG.info("{}: alarm {} cleared", config.nodeId(), largeLog.name);
		}
	}

	/** RocksDB's own stall state, from the gauges of the core server's storage pressure signal. */
	private void checkWriteStall(long now) {
		MeterRegistry registry;
		try {
			var store = stateMachine.store();
			registry = store == null ? null : store.db().getMetricsRegistry();
		} catch (RuntimeException closing) {
			registry = null;
		}
		if (registry == null) {
			return;
		}
		if (pressure(registry, "write_stopped")) {
			if (writeStopped.raised(now)) {
				LOG.warn("{}: ALARM {}: RocksDB stopped writes on this node (write stall). Its apply waits, and so do the"
						+ " cluster's writes if a majority stalls. Usually the compactions do not keep up, or the disk is full",
						config.nodeId(), writeStopped.name);
			}
		} else if (writeStopped.cleared()) {
			LOG.info("{}: alarm {} cleared", config.nodeId(), writeStopped.name);
		}
		if (pressure(registry, "delayed_write")) {
			if (writeDelayed.raised(now)) {
				LOG.warn("{}: ALARM {}: RocksDB slows writes on this node (delayed writes): compactions are behind",
						config.nodeId(), writeDelayed.name);
			}
		} else if (writeDelayed.cleared()) {
			LOG.info("{}: alarm {} cleared", config.nodeId(), writeDelayed.name);
		}
	}

	private static boolean pressure(MeterRegistry registry, String reason) {
		Gauge gauge = registry.find("rockserver.workload.storage.pressure.reason").tag("reason", reason).gauge();
		return gauge != null && gauge.value() > 0;
	}

	private static String bytes(double bytes) {
		return Double.isNaN(bytes) ? "?" : String.format(Locale.ROOT, "%.1f GiB", bytes / (1L << 30));
	}

	/** Tests only. */
	double logBytes() {
		return logBytes;
	}

	/** Tests only. */
	double role() {
		return role;
	}
}
