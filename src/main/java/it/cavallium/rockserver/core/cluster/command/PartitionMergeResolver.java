package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;

import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface PartitionMergeResolver {

    Buf merge(Buf storageKey, @Nullable Buf previousValue, Buf operand);
}
