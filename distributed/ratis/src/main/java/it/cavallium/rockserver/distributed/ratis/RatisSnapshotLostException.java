package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;

/** The exact historical native snapshot is no longer available; callers must not reopen at latest. */
public final class RatisSnapshotLostException extends IOException {
    public RatisSnapshotLostException(String reason) { super("SNAPSHOT_LOST: "+reason); }
    public RatisSnapshotLostException(String reason,Throwable cause) { super("SNAPSHOT_LOST: "+reason,cause); }
}
