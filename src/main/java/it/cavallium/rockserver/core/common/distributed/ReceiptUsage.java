package it.cavallium.rockserver.core.common.distributed;

/** Logical retained receipt usage at one completed native apply position (null before first apply). */
public record ReceiptUsage(ApplyPosition appliedPosition, long retainedCount, long retainedBytes) {
    public ReceiptUsage {
        if (retainedCount < 0 || retainedBytes < 0 || (retainedCount == 0 ? retainedBytes != 0 : retainedBytes / retainedCount < 84)) {
            throw new IllegalArgumentException("Invalid receipt usage");
        }
    }
}
