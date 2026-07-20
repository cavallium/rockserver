package it.cavallium.rockserver.core.impl.rocksdb;

import static org.rocksdb.AbstractEventListener.EnabledEventCallback.ON_BACKGROUND_ERROR;

import java.util.Objects;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.BackgroundErrorReason;
import org.rocksdb.Status;
import org.slf4j.Logger;

final class RocksLogger extends AbstractEventListener {

    private final Logger logger;

    RocksLogger(Logger logger) {
        super(ON_BACKGROUND_ERROR);
        this.logger = Objects.requireNonNull(logger);
    }

    @Override
    public void onBackgroundError(BackgroundErrorReason reason, Status status) {
        logger.error("RocksDB background error: reason={}, code={}, subcode={}, state={}",
                reason, status.getCode(), status.getSubCode(), status.getState());
    }
}
