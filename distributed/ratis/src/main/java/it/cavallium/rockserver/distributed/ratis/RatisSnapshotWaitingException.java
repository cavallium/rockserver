package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;

/** The frozen INSTALL still owns its ready gate while predecessor proposals drain. */
public final class RatisSnapshotWaitingException extends IOException {
    public RatisSnapshotWaitingException() { super("Snapshot gate predecessors are still settling"); }
}
