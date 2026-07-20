package it.cavallium.rockserver.core.cluster.command;

import java.util.concurrent.CompletableFuture;

public interface ReplicatedLogClient {

    CompletableFuture<ReplicatedLogCommit> propose(byte[] payload);
}
