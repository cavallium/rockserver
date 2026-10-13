package org.apache.ratis.server.leader;

import java.io.IOException;

/** Listener-only, nonblocking pre-materialization admission. Voter and heartbeat traffic bypasses it. */
public interface MigrationLogStateMachine {
    long MAX_TAIL_BYTES=(64L<<20)+65536;
    interface Lease { void materialized(long serializedBytes)throws IOException;void close(); }
    /** Null means retry later; a returned owner follows the actual transport through acknowledgement/drain. */
    Lease reserveListenerTail(long maximumBytes)throws IOException;
}
