package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;

import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import static it.cavallium.rockserver.core.common.FutureUtils.awaitInterruptibly;
import static it.cavallium.rockserver.core.common.FutureUtils.unwrapCompletionException;

public final class MetadataLogCommandSubmitter implements MetadataCommandSubmitter {

    private final ReplicatedLogClient metadataLogClient;
    private final Supplier<ClusterMetadata> committedMetadataReader;

    public MetadataLogCommandSubmitter(ReplicatedLogClient metadataLogClient,
                                       Supplier<ClusterMetadata> committedMetadataReader) {
        this.metadataLogClient = requireArgument(metadataLogClient, "metadataLogClient");
        this.committedMetadataReader = requireArgument(committedMetadataReader, "committedMetadataReader");
    }

    @Override
    public ClusterMetadata submit(MetadataCommand command) {
        requireArgument(command, "command");
        try {
            var proposal = requireState(
                    metadataLogClient.propose(MetadataCommandCodec.encode(command)),
                    "metadata command proposal future"
            );
            var commit = awaitInterruptibly(
                    proposal,
                    "metadata command proposal"
            );
            requireState(commit, "metadata command commit");
            if (!commit.committed()) {
                throw new IllegalStateException("metadata command was not committed at log index "
                        + commit.index());
            }
            return requireState(committedMetadataReader.get(), "committed metadata");
        } catch (CompletionException e) {
            throw unwrapCompletionException(e, "metadata command proposal failed");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static <T> T requireState(T value, String name) {
        if (value == null) {
            throw new IllegalStateException(name + " must not be null");
        }
        return value;
    }
}
