package it.cavallium.rockserver.core.common;

public class CdcGapDetectedException extends RocksDBException {

    public CdcGapDetectedException(String message) {
        super(RocksDBErrorType.CDC_GAP_DETECTED, message);
    }

    public CdcGapDetectedException(String message, Throwable cause) {
        super(RocksDBErrorType.CDC_GAP_DETECTED, message, cause);
    }
}
