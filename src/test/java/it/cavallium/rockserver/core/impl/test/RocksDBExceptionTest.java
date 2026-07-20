package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBRetryException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class RocksDBExceptionTest {

    @Test
    void genericFactoryCreatesPublicExceptionForRegularError() {
        var failure = RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, "unsupported request");

        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, failure.getErrorUniqueId());
        assertEquals("unsupported request", failure.getMessage());
        assertEquals("RocksDBError: [uid:NOT_IMPLEMENTED] unsupported request", failure.getLocalizedMessage());
    }

    @Test
    void genericFactoriesRejectUpdateRetryMisuseWithArgumentError() {
        assertUpdateRetryFactoryRejected(() -> RocksDBException.of(RocksDBException.RocksDBErrorType.UPDATE_RETRY, "retry"));
        assertUpdateRetryFactoryRejected(() -> RocksDBException.of(
                RocksDBException.RocksDBErrorType.UPDATE_RETRY,
                new RuntimeException("retry")));
        assertUpdateRetryFactoryRejected(() -> RocksDBException.of(
                RocksDBException.RocksDBErrorType.UPDATE_RETRY,
                "retry",
                new RuntimeException("retry")));
    }

    @Test
    void retryExceptionKeepsUpdateRetryPublicCode() {
        var failure = new RocksDBRetryException();

        assertEquals(RocksDBException.RocksDBErrorType.UPDATE_RETRY, failure.getErrorUniqueId());
        assertEquals("Please, retry the transaction", failure.getMessage());
        assertEquals("RocksDBError: [uid:UPDATE_RETRY] Please, retry the transaction", failure.getLocalizedMessage());
    }

    private static void assertUpdateRetryFactoryRejected(Executable executable) {
        var failure = assertThrows(IllegalArgumentException.class, executable);

        assertEquals("UPDATE_RETRY must be constructed with RocksDBRetryException", failure.getMessage());
    }
}
