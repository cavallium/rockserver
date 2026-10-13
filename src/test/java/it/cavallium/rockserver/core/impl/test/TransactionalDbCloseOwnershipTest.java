package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.TransactionalDB;
import it.cavallium.rockserver.core.impl.rocksdb.DatabaseTasks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.rocksdb.Env;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Priority;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TransactionalDbCloseOwnershipTest {
    private static void beforeOpen(Env env) throws Exception {
        var method = Class.forName("it.cavallium.rockserver.core.impl.rocksdb.RocksDBEnvLifecycle")
                .getDeclaredMethod("beforeOpen", Env.class);
        method.setAccessible(true);
        method.invoke(null, env);
    }

    @Test void failedNativeCloseRetainsRegistrationAndAllowsRetry() throws Exception {
        Env env = mock(Env.class);
        when(env.getBackgroundThreads(any())).thenReturn(1);
        var owned = new AtomicBoolean(true);
        var db = mock(OptimisticTransactionDB.class);
        when(db.isOwningHandle()).thenAnswer(invocation -> owned.get());
        when(db.getEnv()).thenReturn(env);
        doThrow(new org.rocksdb.RocksDBException("close did not consume owner"))
                .doAnswer(invocation -> { owned.set(false); return null; }).when(db).closeE();
        beforeOpen(env);
        var owner = TransactionalDB.create("retry", db, List.of(), new ArrayList<>(), mock(DatabaseTasks.class));
        try {
            assertThrows(java.io.IOException.class, owner::close);
            verify(env, never()).setBackgroundThreads(0, Priority.LOW);
            verify(env, never()).setBackgroundThreads(0, Priority.HIGH);
        } finally { owner.close(); }
        verify(db, times(2)).closeE();
        verify(env).setBackgroundThreads(0, Priority.LOW);
        verify(env).setBackgroundThreads(0, Priority.HIGH);
    }

    @Test void lastCloseCannotStopSharedThreadsWhileAnotherNativeCloseIsRunning() throws Exception {
        Env env = mock(Env.class);
        when(env.getBackgroundThreads(any())).thenReturn(1);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var firstOwned = new AtomicBoolean(true);
        var secondOwned = new AtomicBoolean(true);
        var first = mock(OptimisticTransactionDB.class);
        var second = mock(OptimisticTransactionDB.class);
        when(first.isOwningHandle()).thenAnswer(invocation -> firstOwned.get());
        when(second.isOwningHandle()).thenAnswer(invocation -> secondOwned.get());
        when(first.getEnv()).thenReturn(env);
        when(second.getEnv()).thenReturn(env);
        doAnswer(invocation -> {
            entered.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            firstOwned.set(false);
            return null;
        }).when(first).closeE();
        doAnswer(invocation -> { secondOwned.set(false); return null; }).when(second).closeE();
        beforeOpen(env);
        beforeOpen(env);
        var firstOwner = TransactionalDB.create("first", first, List.of(), new ArrayList<>(), mock(DatabaseTasks.class));
        var secondOwner = TransactionalDB.create("second", second, List.of(), new ArrayList<>(), mock(DatabaseTasks.class));
        var closing = CompletableFuture.runAsync(() -> {
            try { firstOwner.close(); } catch (Exception failure) { throw new RuntimeException(failure); }
        });
        try {
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            secondOwner.close();
            verify(env, never()).setBackgroundThreads(0, Priority.LOW);
            verify(env, never()).setBackgroundThreads(0, Priority.HIGH);
            assertFalse(closing.isDone());
        } finally {
            release.countDown();
            closing.get(10, TimeUnit.SECONDS);
            secondOwner.close();
        }
        verify(env).setBackgroundThreads(0, Priority.LOW);
        verify(env).setBackgroundThreads(0, Priority.HIGH);
        firstOwner.close();
        verify(first, times(1)).closeE();
    }
}
