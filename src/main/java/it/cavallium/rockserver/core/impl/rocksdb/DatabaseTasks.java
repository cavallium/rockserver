package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class DatabaseTasks implements Closeable {

	private final RocksDB db;
	private final boolean inMemory;
	private final Logger logger;
	private final Duration delayWalFlushConfig;
	private Thread walFlushThread;

	public DatabaseTasks(RocksDB db, boolean inMemory, Duration delayWalFlushConfig) {
		this.db = db;
		this.inMemory = inMemory;
		this.logger = Logger.getLogger("db." + db.getName() + ".tasks");
		this.delayWalFlushConfig = inMemory ? Duration.ZERO : delayWalFlushConfig;
	}

	public synchronized void start() {
		if (delayWalFlushConfig.toMillis() > 0) {
			this.walFlushThread = Thread.ofVirtual().name("db." + db.getName() + ".tasks.wal.flush").start(() -> {
				logger.info("Database delayed flush thread is enabled, it will flush the database every %.2f seconds".formatted(delayWalFlushConfig.toMillis() / 1000d));
				while (!Thread.interrupted()) {
					try {
						//noinspection BusyWait
						Thread.sleep(delayWalFlushConfig.toMillis());
					} catch (InterruptedException ignored) {
						return;
					}
					try {
						db.flushWal(true);
					} catch (RocksDBException e) {
						logger.log(Level.SEVERE, "Failed to flush database \"%s\" wal".formatted(db.getName()), e);
					}
				}
			});
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (walFlushThread != null) {
			walFlushThread.interrupt();
		}
	}
}
