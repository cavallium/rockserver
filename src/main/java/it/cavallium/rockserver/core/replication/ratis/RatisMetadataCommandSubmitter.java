package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandSubmitter;
import it.cavallium.rockserver.core.cluster.metadata.MetadataLogCommandSubmitter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletionException;

import static it.cavallium.rockserver.core.common.FutureUtils.awaitInterruptibly;
import static it.cavallium.rockserver.core.common.FutureUtils.unwrapCompletionException;

public final class RatisMetadataCommandSubmitter implements MetadataCommandSubmitter {

    private static final byte[] SNAPSHOT_QUERY =
            "ROCKSERVER_METADATA_SNAPSHOT_V1".getBytes(StandardCharsets.US_ASCII);

    private final MetadataLogCommandSubmitter delegate;

    public RatisMetadataCommandSubmitter(RatisPartitionClient metadataClient) {
        var resolvedMetadataClient = requireNonNull(metadataClient, "metadataClient");
        this.delegate = new MetadataLogCommandSubmitter(
                resolvedMetadataClient,
                () -> readCommittedMetadata(resolvedMetadataClient)
        );
    }

    @Override
    public ClusterMetadata submit(MetadataCommand command) {
        return delegate.submit(command);
    }

    public static ClusterMetadata readCommittedMetadata(RatisPartitionClient metadataClient) {
        requireNonNull(metadataClient, "metadataClient");
        try {
            var payload = awaitInterruptibly(
                    metadataClient.queryLinearizable(snapshotQueryPayload()),
                    "metadata snapshot query"
            );
            return decodeSnapshot(payload);
        } catch (CompletionException e) {
            throw unwrapCompletionException(e, "metadata snapshot query failed");
        }
    }

    public static byte[] snapshotQueryPayload() {
        return Arrays.copyOf(SNAPSHOT_QUERY, SNAPSHOT_QUERY.length);
    }

    public static byte[] encodeSnapshot(ClusterMetadata metadata) {
        return new RatisProcessAdapterMetadataConfig(metadata).toBytes();
    }

    public static ClusterMetadata decodeSnapshot(byte[] payload) {
        return RatisProcessAdapterMetadataConfig.fromBytes(payload).metadata();
    }

    public static void writeSnapshot(Path path, ClusterMetadata metadata) throws IOException {
        requireNonNull(path, "path");
        new RatisProcessAdapterMetadataConfig(metadata).write(path);
    }

    public static ClusterMetadata readSnapshot(Path path) throws IOException {
        requireNonNull(path, "path");
        return RatisProcessAdapterMetadataConfig.read(path).metadata();
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

}
