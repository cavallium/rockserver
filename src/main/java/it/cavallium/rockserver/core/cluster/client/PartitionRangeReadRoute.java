package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;

public record PartitionRangeReadRoute(RouteTarget target,
                                      PartitionRangeReadCommand command) {

    public PartitionRangeReadRoute {
        requireArgument(target, "target");
        requireArgument(command, "command");
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
