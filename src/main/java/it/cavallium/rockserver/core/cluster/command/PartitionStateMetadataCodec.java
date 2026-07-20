package it.cavallium.rockserver.core.cluster.command;

import java.nio.ByteBuffer;

public final class PartitionStateMetadataCodec {

    private PartitionStateMetadataCodec() {
    }

    public static byte[] encodeLastAppliedIndex(long lastAppliedIndex) {
        if (lastAppliedIndex < 0) {
            throw new IllegalArgumentException("lastAppliedIndex must be >= 0");
        }
        return ByteBuffer.allocate(Long.BYTES)
                .putLong(lastAppliedIndex)
                .array();
    }

    public static long decodeLastAppliedIndex(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        if (payload.length != Long.BYTES) {
            throw new IllegalArgumentException("Invalid lastAppliedIndex payload length: " + payload.length);
        }
        long lastAppliedIndex = ByteBuffer.wrap(payload).getLong();
        if (lastAppliedIndex < 0) {
            throw new IllegalArgumentException("lastAppliedIndex must be >= 0");
        }
        return lastAppliedIndex;
    }
}
