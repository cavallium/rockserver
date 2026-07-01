package it.cavallium.rockserver.core.impl.rocksdb;

import java.util.List;
import org.rocksdb.Env;
import org.rocksdb.Priority;

final class RocksDBEnvLifecycle {

	private static final Object LOCK = new Object();
	private static final boolean SHUTDOWN_ENV_BACKGROUND_THREADS = Boolean.parseBoolean(
			System.getProperty("it.cavallium.rockserver.env.shutdown-background-threads", "true")
	);

	private static int openDatabases;
	private static Integer lowPriorityThreads;
	private static Integer highPriorityThreads;
	private static boolean backgroundThreadsStopped;

	private RocksDBEnvLifecycle() {
	}

	static void beforeOpen(Env env) {
		synchronized (LOCK) {
			if (openDatabases == 0) {
				rememberThreadPoolSizes(env);
				restoreThreadPoolsIfNeeded(env);
			}
			openDatabases++;
		}
	}

	static void openFailed() {
		synchronized (LOCK) {
			if (openDatabases > 0) {
				openDatabases--;
			}
		}
	}

	static boolean afterCloseAndIsLast() {
		synchronized (LOCK) {
			if (openDatabases > 0) {
				openDatabases--;
			}
			return openDatabases == 0;
		}
	}

	static void shutdownThreadPoolsOnLastClose(Env env, List<Exception> exceptions) {
		if (!SHUTDOWN_ENV_BACKGROUND_THREADS) {
			return;
		}
		synchronized (LOCK) {
			if (openDatabases != 0 || backgroundThreadsStopped) {
				return;
			}
			try {
				env.setBackgroundThreads(0, Priority.LOW);
				env.setBackgroundThreads(0, Priority.HIGH);
				backgroundThreadsStopped = true;
			} catch (Exception ex) {
				exceptions.add(ex);
			}
		}
	}

	private static void rememberThreadPoolSizes(Env env) {
		if (lowPriorityThreads == null) {
			lowPriorityThreads = env.getBackgroundThreads(Priority.LOW);
		}
		if (highPriorityThreads == null) {
			highPriorityThreads = env.getBackgroundThreads(Priority.HIGH);
		}
	}

	private static void restoreThreadPoolsIfNeeded(Env env) {
		if (!backgroundThreadsStopped) {
			return;
		}
		env.setBackgroundThreads(lowPriorityThreads, Priority.LOW);
		env.setBackgroundThreads(highPriorityThreads, Priority.HIGH);
		backgroundThreadsStopped = false;
	}
}
