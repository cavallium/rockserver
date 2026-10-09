package it.cavallium.rockserver.core.common.distributed;

/** A new native pin cannot be acquired for an expired snapshot business epoch. */
public final class SnapshotRetentionExpiredException extends IllegalStateException {
    public SnapshotRetentionExpiredException() { super("Snapshot business epoch has expired"); }
}
