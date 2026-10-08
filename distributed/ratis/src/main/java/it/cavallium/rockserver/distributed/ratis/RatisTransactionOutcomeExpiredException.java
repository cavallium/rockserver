package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.util.UUID;

/** A consumed transaction's original business result is no longer retained. */
public final class RatisTransactionOutcomeExpiredException extends IOException {
    public RatisTransactionOutcomeExpiredException(UUID transaction) { super("Transaction outcome has expired: "+transaction); }
}
