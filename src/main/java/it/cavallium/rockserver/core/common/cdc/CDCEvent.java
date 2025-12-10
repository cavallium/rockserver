package it.cavallium.rockserver.core.common.cdc;

import it.cavallium.buffer.Buf;

/**
 * Internal CDC event model used by Sync/Async APIs and server implementations.
 * Seq must be strictly increasing and globally unique per DB instance.
 */
public record CDCEvent(long seq, long columnId, Buf key, Buf value, Op op) {

    public enum Op { PUT, DELETE, MERGE }
}
