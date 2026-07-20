package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseTasks implements Closeable {

	private static final long WAL_FLUSH_THREAD_CLOSE_TIMEOUT_MILLIS = 5000L;

	private final RocksDB db;
	private final Logger logger;
	private final Duration delayWalFlushConfig;
	private Thread walFlushThread;

	public DatabaseTasks(RocksDB db, boolean inMemory, Duration delayWalFlushConfig) {
		if (db == null) {
			throw new IllegalArgumentException("db must not be null");
		}
		if (delayWalFlushConfig == null) {
			throw new IllegalArgumentException("delayWalFlushConfig must not be null");
		}
		if (delayWalFlushConfig.isNegative()) {
			throw new IllegalArgumentException("delayWalFlushConfig must not be negative");
		}
		this.db = db;
		this.logger = LoggerFactory.getLogger("db." + db.getName() + ".tasks");
		this.delayWalFlushConfig = inMemory ? Duration.ZERO : delayWalFlushConfig;
	}

	public synchronized void start() {
		if (delayWalFlushConfig.toMillis() > 0) {
			if (walFlushThread != null && walFlushThread.isAlive()) {
				return;
			}
			this.walFlushThread = Thread.ofPlatform().name("db." + db.getName() + ".tasks.wal.flush").start(() -> {
				logger.info("Database delayed flush thread is enabled, it will flush the database every {} seconds", delayWalFlushConfig.toMillis() / 1000d);
				while (!Thread.interrupted()) {
					try {
						//noinspection BusyWait
						Thread.sleep(delayWalFlushConfig.toMillis());
					} catch (InterruptedException ignored) {
						Thread.currentThread().interrupt();
						return;
					}
					try {
						db.flushWal(true);
					} catch (RocksDBException e) {
						logger.error("Failed to flush database \"{}\" wal", db.getName(), e);
					}
				}
			});
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (walFlushThread != null) {
			walFlushThread.interrupt();
			awaitWalFlushThreadTermination(walFlushThread, WAL_FLUSH_THREAD_CLOSE_TIMEOUT_MILLIS);
			walFlushThread = null;
		}
	}

	@SuppressWarnings("SameParameterValue")
	static void awaitWalFlushThreadTermination(Thread thread, long timeoutMillis) throws IOException {
		var timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		var startNanos = System.nanoTime();
		var interrupted = false;
		try {
			while (thread.isAlive()) {
				var elapsedNanos = System.nanoTime() - startNanos;
				var remainingNanos = timeoutNanos - elapsedNanos;
				if (remainingNanos <= 0L) {
					throw new IOException("Timed out while closing WAL flush thread");
				}
				try {
					var remainingMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
					var remainingSubMillisNanos = (int) (remainingNanos - TimeUnit.MILLISECONDS.toNanos(remainingMillis));
					thread.join(remainingMillis, remainingSubMillisNanos);
				} catch (InterruptedException _) {
					interrupted = true;
				}
			}
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
