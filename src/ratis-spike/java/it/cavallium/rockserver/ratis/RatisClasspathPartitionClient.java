package it.cavallium.rockserver.ratis;

import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcException;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

public final class RatisClasspathPartitionClient implements RatisPartitionClient {

    static final int MAX_REQUEST_ATTEMPTS = 3;
    static final TimeDuration REQUEST_TIMEOUT = TimeDuration.valueOf(3, TimeUnit.SECONDS);
    static final TimeDuration RETRY_SLEEP = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);

    private final RatisPartitionDescriptor descriptor;
    private final RaftPeerId localPeerId;
    private final RaftClient client;

    public RatisClasspathPartitionClient(RatisPartitionDescriptor descriptor) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.localPeerId = RaftPeerId.valueOf(descriptor.localPeerId());
        this.client = RaftClient.newBuilder()
                .setRaftGroup(raftGroup(descriptor))
                .setProperties(clientProperties())
                .setRetryPolicy(retryPolicy())
                .build();
    }

    @Override
    public RatisPartitionDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        return commit(() -> client.io().send(Message.valueOf(ByteString.copyFrom(payload))), true);
    }

    @Override
    public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
        return commit(() -> client.io().watch(index, ReplicationLevel.MAJORITY_COMMITTED), false);
    }

    @Override
    public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
        Objects.requireNonNull(request, "request");
        return query(() -> client.io().sendReadOnly(message(request)));
    }

    @Override
    public CompletableFuture<byte[]> queryStale(byte[] request, long minAppliedIndex) {
        Objects.requireNonNull(request, "request");
        return query(() -> client.io().sendStaleRead(message(request), minAppliedIndex, localPeerId));
    }

    @Override
    public CompletableFuture<byte[]> queryLocal(byte[] request) {
        Objects.requireNonNull(request, "request");
        return query(() -> client.io().sendStaleRead(message(request), 0, localPeerId));
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            throw new RatisProcessAdapterRpcException("RATIS_CLOSE_FAILED", e.getMessage());
        }
    }

    private CompletableFuture<ReplicatedLogCommit> commit(RatisOperation operation, boolean includeResponsePayload) {
        try {
            var reply = operation.send();
            requireSuccess(reply);
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(
                    reply.getLogIndex(),
                    true,
                    includeResponsePayload ? responsePayload(reply) : null
            ));
        } catch (IOException | RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<byte[]> query(RatisOperation operation) {
        try {
            var reply = operation.send();
            requireSuccess(reply);
            return CompletableFuture.completedFuture(reply.getMessage().getContent().toByteArray());
        } catch (IOException | RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private static void requireSuccess(RaftClientReply reply) {
        if (!reply.isSuccess()) {
            throw new RatisProcessAdapterRpcException("RATIS_REQUEST_FAILED", reply.toString());
        }
    }

    private static byte[] responsePayload(RaftClientReply reply) {
        var message = reply.getMessage();
        if (message == null) {
            return null;
        }
        var payload = message.getContent().toByteArray();
        return payload.length == 0 ? null : payload;
    }

    private static Message message(byte[] payload) {
        return Message.valueOf(ByteString.copyFrom(payload));
    }

    static RaftGroup raftGroup(RatisPartitionDescriptor descriptor) {
        var peers = descriptor.voters().stream()
                .map(peer -> RaftPeer.newBuilder()
                        .setId(RaftPeerId.valueOf(peer.peerId()))
                        .setAddress(ratisAddress(peer.endpoint()))
                        .build())
                .toList();
        return RaftGroup.valueOf(raftGroupId(descriptor.raftGroupId()), peers);
    }

    private static RaftGroupId raftGroupId(String value) {
        try {
            return RaftGroupId.valueOf(UUID.fromString(value));
        } catch (IllegalArgumentException _) {
            return RaftGroupId.valueOf(UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    private static String ratisAddress(String endpoint) {
        if (endpoint.contains("://")) {
            var uri = URI.create(endpoint);
            if (uri.getHost() == null || uri.getPort() < 0) {
                throw new IllegalArgumentException("Ratis endpoint must include host and port: " + endpoint);
            }
            return uri.getHost() + ":" + uri.getPort();
        }
        return endpoint;
    }

    static RaftProperties clientProperties() {
        var properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        RaftClientConfigKeys.Rpc.setRequestTimeout(properties, REQUEST_TIMEOUT);
        RaftClientConfigKeys.Rpc.setWatchRequestTimeout(properties, REQUEST_TIMEOUT);
        return properties;
    }

    static RetryPolicies.RetryLimited retryPolicy() {
        return RetryPolicies.retryUpToMaximumCountWithFixedSleep(MAX_REQUEST_ATTEMPTS, RETRY_SLEEP);
    }

    static long maxRequestEnvelopeMillis() {
        return REQUEST_TIMEOUT.toLong(TimeUnit.MILLISECONDS) * MAX_REQUEST_ATTEMPTS
                + RETRY_SLEEP.toLong(TimeUnit.MILLISECONDS) * (MAX_REQUEST_ATTEMPTS - 1L);
    }

    @FunctionalInterface
    private interface RatisOperation {

        RaftClientReply send() throws IOException;
    }
}
