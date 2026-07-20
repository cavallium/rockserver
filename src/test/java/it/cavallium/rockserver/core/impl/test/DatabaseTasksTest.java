package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.rocksdb.DatabaseTasks;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class DatabaseTasksTest {

	@TempDir
	Path tempDir;

	@Test
	void startIsIdempotentWhileWalFlushThreadIsAlive() throws Exception {
		Thread firstThread;
		try (var options = new Options().setCreateIfMissing(true);
			 var db = RocksDB.open(options, tempDir.resolve("idempotent-start").toString());
			 var tasks = new DatabaseTasks(db, false, Duration.ofSeconds(60))) {
			tasks.start();
			firstThread = walFlushThread(tasks);
			assertNotNull(firstThread);
			assertTrue(firstThread.isAlive());

			tasks.start();

			assertSame(firstThread, walFlushThread(tasks));
		}
		assertNotNull(firstThread);
		assertFalse(firstThread.isAlive());
	}

	@Test
	@SuppressWarnings("resource")
	void constructorRejectsInvalidInputsWithExplicitDiagnostics() throws Exception {
		var nullDb = assertThrows(IllegalArgumentException.class,
				() -> new DatabaseTasks(null, false, Duration.ZERO));
		assertEquals("db must not be null", nullDb.getMessage());

		try (var options = new Options().setCreateIfMissing(true);
			 var db = RocksDB.open(options, tempDir.resolve("invalid-inputs").toString())) {
			var nullDelay = assertThrows(IllegalArgumentException.class,
					() -> new DatabaseTasks(db, false, null));
			assertEquals("delayWalFlushConfig must not be null", nullDelay.getMessage());

			var negativeDelay = assertThrows(IllegalArgumentException.class,
					() -> new DatabaseTasks(db, false, Duration.ofMillis(-1)));
			assertEquals("delayWalFlushConfig must not be negative", negativeDelay.getMessage());
		}
	}

	@Test
	void awaitWalFlushThreadTerminationReportsStuckThread() throws Exception {
		var release = new CountDownLatch(1);
        var thread = Thread.ofPlatform().start(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            var failure = assertThrows(IOException.class, () -> awaitWalFlushThreadTermination(thread, 1L));

            assertEquals("Timed out while closing WAL flush thread", failure.getMessage());
        } finally {
            release.countDown();
            thread.join();
        }
    }

    @Test
    void awaitWalFlushThreadTerminationRetriesAfterInterruptionAndRestoresInterrupt() throws Exception {
        var thread = Thread.ofPlatform().start(() ->
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100)));

        try {
            Thread.currentThread().interrupt();
            awaitWalFlushThreadTermination(thread, TimeUnit.SECONDS.toMillis(2));

            assertTrue(Thread.interrupted());
            assertFalse(thread.isAlive());
        } finally {
            clearInterruptedStatus();
            thread.join();
        }
    }

	private static void awaitWalFlushThreadTermination(Thread thread, long timeoutMillis) throws Exception {
		var method = DatabaseTasks.class.getDeclaredMethod("awaitWalFlushThreadTermination", Thread.class, long.class);
		method.setAccessible(true);
		invokeReflected(() -> method.invoke(null, thread, timeoutMillis));
	}

	private static Thread walFlushThread(DatabaseTasks tasks) throws Exception {
		var field = DatabaseTasks.class.getDeclaredField("walFlushThread");
		field.setAccessible(true);
		return (Thread) field.get(tasks);
	}

	private static void invokeReflected(ReflectiveInvocation invocation) throws Exception {
        try {
            invocation.invoke();
        } catch (InvocationTargetException e) {
            var cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private static void clearInterruptedStatus() {
		Thread.interrupted();
	}

    @FunctionalInterface
    private interface ReflectiveInvocation {

        void invoke() throws Exception;
    }
}
