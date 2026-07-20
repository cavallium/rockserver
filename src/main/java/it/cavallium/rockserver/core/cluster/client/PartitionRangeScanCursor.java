package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;

public record PartitionRangeScanCursor(long metadataEpoch,
                                       PartitionRangeReadCommand command,
                                       int nextSegmentIndex) {

    public PartitionRangeScanCursor {
        if (metadataEpoch < 0) {
            throw new IllegalArgumentException("metadataEpoch must be >= 0");
        }
        requireArgument(command, "command");
        if (nextSegmentIndex < 0) {
            throw new IllegalArgumentException("nextSegmentIndex must be >= 0");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
