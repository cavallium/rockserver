package it.cavallium.rockserver.distributed;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds a hung disk. A node whose disk hangs still answers HTTP/2 and keepalive, so its clients stay, while every write
 * waits for an fsync of the Raft log that does not end, until the call's deadline. Reads and writes through the
 * database do not show it: they are served from the block cache and the memtables. So this probe does what every write
 * waits for: it writes a few bytes to a file in the node's data directory (the file system of the database and of the
 * Raft log, see "Disk layout" in the design document) and fsyncs it, on a thread of its own, one probe after the other.
 * A probe that has not ended after the limit ({@link #hung}) makes the node pause its API ({@link ApiGate}), so that its
 * clients move to another node; it serves again once the probe ends. A leader on such a disk also stops flushing its
 * log, which {@link LeaderStall} handles.
 */
final class DiskProbe implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(DiskProbe.class);
	static final String FILE_NAME = "disk-probe";
	static final Duration INTERVAL = Duration.ofSeconds(1);
	private static final long IDLE = Long.MIN_VALUE;

	@FunctionalInterface
	interface Probe {
		void run() throws IOException;
	}

	private final String name;
	private final Probe probe;
	private final Duration interval;
	private final LongSupplier nanoClock;
	/** When the probe that runs now started, {@link #IDLE} between probes. */
	private volatile long startedNanos = IDLE;
	private volatile boolean closed;
	private Thread thread;

	DiskProbe(String name, Probe probe, Duration interval, LongSupplier nanoClock) {
		this.name = name;
		this.probe = probe;
		this.interval = interval;
		this.nanoClock = nanoClock;
	}

	/** A probe that writes and fsyncs {@code dir}/{@value #FILE_NAME}. */
	static DiskProbe forDirectory(String name, Path dir) {
		Path file = dir.resolve(FILE_NAME);
		return new DiskProbe(name, () -> {
			try (var channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
				channel.write(ByteBuffer.allocate(Long.BYTES).putLong(0, System.nanoTime()), 0);
				channel.force(true);
			}
		}, INTERVAL, System::nanoTime);
	}

	DiskProbe start() {
		thread = Thread.ofPlatform().daemon().name("rockserver-" + name + "-disk-probe").start(this::run);
		return this;
	}

	private void run() {
		while (!closed) {
			startedNanos = nanoClock.getAsLong();
			try {
				probe.run();
			} catch (IOException | RuntimeException ex) {
				// A disk that fails at once is not a hung one: the database and the log report their own errors.
				LOG.warn("{}: the disk probe failed", name, ex);
			} finally {
				startedNanos = IDLE;
			}
			try {
				Thread.sleep(interval);
			} catch (InterruptedException ex) {
				return;
			}
		}
	}

	/** Whether the probe that runs now started more than {@code limitNanos} before {@code nowNanos}. */
	boolean hung(long nowNanos, long limitNanos) {
		long started = startedNanos;
		return started != IDLE && nowNanos - started > limitNanos;
	}

	@Override
	public void close() {
		closed = true;
		if (thread != null) {
			thread.interrupt();
		}
	}
}
