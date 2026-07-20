package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;

import java.util.concurrent.CompletableFuture;

public interface RatisPartitionClient extends ReplicatedLogClient, AutoCloseable {

    RatisPartitionDescriptor descriptor();

    CompletableFuture<ReplicatedLogCommit> waitForCommit(long index);

    CompletableFuture<byte[]> queryLinearizable(byte[] request);

    default CompletableFuture<byte[]> queryStale(byte[] request, long minAppliedIndex) {
        return CompletableFuture.failedFuture(new RatisFeatureUnavailableException("stale reads are not supported"));
    }

    default CompletableFuture<byte[]> queryLocal(byte[] request) {
        return CompletableFuture.failedFuture(new RatisFeatureUnavailableException("local reads are not supported"));
    }

    @Override
    void close();
}
