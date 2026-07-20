package it.cavallium.rockserver.core.cluster.command;

import java.util.List;

@FunctionalInterface
public interface PartitionWriteApplier {

    List<Object> apply(long logIndex, PartitionWriteCommand command);
}
