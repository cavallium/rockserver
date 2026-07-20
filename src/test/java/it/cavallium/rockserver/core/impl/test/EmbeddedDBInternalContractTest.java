package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.rocksdb.DBWriter;
import it.cavallium.rockserver.core.impl.rocksdb.Tx;

import java.lang.reflect.InvocationTargetException;

import org.junit.jupiter.api.Test;

class EmbeddedDBInternalContractTest {

    @Test
    void writeTransactionContractsRejectInvalidInternalStates() throws Throwable {
        var missingWriter = invokeRocksFailure("requireTransactionWriter",
                new Class<?>[] {String.class, DBWriter.class},
                "put bucketed write",
                (Object) null);
        assertEquals("put bucketed write requires a transaction writer", missingWriter.getMessage());

        var ownedUpdate = invokeRocksFailure("requireExternalUpdateTransaction",
                new Class<?>[] {String.class, boolean.class, DBWriter.class},
                "put",
                true,
                (Object) null);
        assertEquals("put update context unexpectedly owns an internal transaction", ownedUpdate.getMessage());

        var missingUpdateWriter = invokeRocksFailure("requireExternalUpdateTransaction",
                new Class<?>[] {String.class, boolean.class, DBWriter.class},
                "delete",
                false,
                (Object) null);
        assertEquals("delete update context requires a transaction writer", missingUpdateWriter.getMessage());
    }

    @Test
    void getContractsRejectInvalidInternalStates() throws Throwable {
        var keyExistsTx = invokeRocksFailure("requireNoTransactionForKeyExists",
                new Class<?>[] {Tx.class},
                new Tx(null, false, 0L, null));
        assertEquals("key-exists fast path received a transaction", keyExistsTx.getMessage());

        var missingUpdateId = invokeRocksFailure("requireAllocatedUpdateId",
                new Class<?>[] {long.class},
                0L);
        assertEquals("get-for-update did not allocate an update id", missingUpdateId.getMessage());

        assertEquals(42L, ((Long) invokeStatic("requireAllocatedUpdateId", new Class<?>[] {long.class}, 42L)).longValue());
    }

    private static RocksDBException invokeRocksFailure(String name, Class<?>[] parameterTypes, Object... args)
            throws Throwable {
        var failure = assertThrows(RocksDBException.class, () -> invokeStatic(name, parameterTypes, args));
        assertEquals(RocksDBException.RocksDBErrorType.INTERNAL_ERROR, failure.getErrorUniqueId());
        return failure;
    }

    private static Object invokeStatic(String name, Class<?>[] parameterTypes, Object... args) throws Throwable {
        var method = EmbeddedDB.class.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        try {
            return method.invoke(null, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
