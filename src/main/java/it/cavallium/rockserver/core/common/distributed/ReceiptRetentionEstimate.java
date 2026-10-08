package it.cavallium.rockserver.core.common.distributed;

import java.util.Objects;

/** Nonnegative worst net growth for one immutable proposal, derived from the supplied completed usage cut. */
public record ReceiptRetentionEstimate(ReceiptUsage usage, boolean newReceipt, long additionalCount,
                                      long additionalBytes, boolean maintenance) {
    public ReceiptRetentionEstimate {
        Objects.requireNonNull(usage);
        if (additionalCount < 0 || additionalBytes < 0 || (!newReceipt && (additionalCount != 0 || additionalBytes != 0))) {
            throw new IllegalArgumentException("Invalid receipt retention estimate");
        }
    }
}
