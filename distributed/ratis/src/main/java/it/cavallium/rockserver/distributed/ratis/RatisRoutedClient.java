package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Initial bounded point/same-range client. Cross-group atomicity and automatic ambiguity retry are
 * unsupported. Per-operation Raft clients favor simple resource ownership; performance is pending.
 */
public final class RatisRoutedClient implements AutoCloseable {
    static final int MAX_CONCURRENT_CALLS = 32;

    public interface Transport extends AutoCloseable {
        byte[] read(RangeDescriptor descriptor, UUID columnId, byte[] physicalKey) throws IOException;
        CommandResult command(RangeDescriptor descriptor, DistributedCommand command) throws IOException;
        @Override default void close() throws IOException {}
    }

    /** Keeps the original command identity and route for explicit retries, including after refresh. */
    public static final class PreparedCommand {
        private final RangeDirectory source;
        private final RangeDescriptor descriptor;
        private final DistributedCommand command;
        private PreparedCommand(RangeDirectory source, RangeDescriptor descriptor, DistributedCommand command) {
            this.source = source;
            this.descriptor = descriptor;
            this.command = command;
        }
        public RangeDescriptor descriptor() { return descriptor; }
        public DistributedCommand command() { return command; }
    }

    private final RangeDirectory identity;
    private final Transport transport;
    private RangeDirectory directory;
    private int activeCalls;
    private boolean closing;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();

    public RatisRoutedClient(RangeDirectory directory, Transport transport) {
        this.identity = this.directory = Objects.requireNonNull(directory, "directory");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    public static RatisRoutedClient connect(RangeDirectory directory) {
        return new RatisRoutedClient(directory, new RaftTransport());
    }

    public synchronized RangeDirectory directory() { return directory; }

    public synchronized void refresh(RangeDirectory next) {
        requireOpen();
        Objects.requireNonNull(next, "next");
        requireIdentity(next);
        if (next.generation() < directory.generation()
                || (next.generation() == directory.generation() && !Arrays.equals(next.payloadDigest(), directory.payloadDigest()))) {
            throw new IllegalArgumentException("Stale or conflicting directory generation");
        }
        directory = next;
    }

    public byte[] read(UUID columnId, byte[] physicalKey) throws IOException {
        Objects.requireNonNull(physicalKey, "physicalKey");
        if (physicalKey.length > DistributedCommandCodec.MAX_ENCODED_BYTES - 16) throw new IllegalArgumentException("Point read key exceeds budget");
        byte[] key = physicalKey.clone();
        RangeDirectory observed = current();
        RangeDescriptor descriptor = observed.route(columnId, key);
        beginCall();
        try {
            byte[] value = transport.read(descriptor, columnId, key);
            if (value != null && value.length > DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IOException("Point value exceeds budget");
            return value == null ? null : value.clone();
        } finally { endCall(); }
    }

    public PreparedCommand prepare(OperationId operation, List<Mutation> mutations) {
        return prepare(operation, mutations, List.of());
    }

    public PreparedCommand prepare(OperationId operation, List<Mutation> mutations, List<KeyPredicate> predicates) {
        RangeDirectory observed = current();
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(mutations, "mutations");
        Objects.requireNonNull(predicates, "predicates");
        if (mutations.isEmpty() || (long) mutations.size() + predicates.size() > DistributedCommandCodec.MAX_ITEMS) {
            throw new IllegalArgumentException("Invalid atomic command item count");
        }
        mutations = List.copyOf(mutations);
        predicates = List.copyOf(predicates);
        RangeDescriptor target = null;
        for (Mutation mutation : mutations) {
            RangeDescriptor routed = observed.route(mutation.columnId(), mutation.key());
            target = sameRange(target, routed);
            if (mutation.kind() == Mutation.Kind.DELETE_RANGE) {
                for (RangeDescriptor covered : observed.intersect(mutation.columnId(), mutation.key(), mutation.value(), false)) {
                    target = sameRange(target, covered);
                }
            }
        }
        for (KeyPredicate predicate : predicates) target = sameRange(target, observed.route(predicate.columnId(), predicate.key()));
        var command = new DistributedCommand(target.groupId(), target.rangeEpoch(), observed.schemaDigest(), operation, mutations, predicates);
        return new PreparedCommand(observed, target, command);
    }

    public CommandResult execute(PreparedCommand prepared) throws IOException {
        Objects.requireNonNull(prepared, "prepared");
        requireIdentity(prepared.source);
        beginCall();
        try {
            CommandResult result = transport.command(prepared.descriptor(), prepared.command());
            if (result == null) throw new IOException("Missing command result");
            return result;
        } finally { endCall(); }
    }

    private synchronized RangeDirectory current() { requireOpen(); return directory; }
    private void requireOpen() { if (closing) throw new IllegalStateException("Routed client is closing"); }
    private void requireIdentity(RangeDirectory other) {
        if (!identity.clusterId().equals(other.clusterId()) || !identity.databaseId().equals(other.databaseId())
                || !identity.domainId().equals(other.domainId()) || identity.schemaEpoch() != other.schemaEpoch()
                || !Arrays.equals(identity.schemaDigest(), other.schemaDigest()) || !identity.columns().equals(other.columns())
                || identity.routingPrefixBytes() != other.routingPrefixBytes()) {
            throw new IllegalArgumentException("Directory belongs to another cluster/database/domain/schema");
        }
    }
    private static RangeDescriptor sameRange(RangeDescriptor target, RangeDescriptor next) {
        if (target != null && (!target.groupId().equals(next.groupId()) || target.rangeEpoch() != next.rangeEpoch())) {
            throw new IllegalArgumentException("Cross-group atomic commands require an unsupported coordination protocol");
        }
        return target == null ? next : target;
    }
    private synchronized void beginCall() throws IOException {
        requireOpen();
        if (activeCalls >= MAX_CONCURRENT_CALLS) throw new IOException("Routed client call budget exhausted");
        activeCalls++;
    }
    private synchronized void endCall() { activeCalls--; notifyAll(); }

    /** Stops admission and drains existing calls before releasing transport resources; it does not cancel committed work. */
    @Override public void close() throws IOException {
        boolean owner;
        boolean interrupted = false;
        synchronized (this) {
            owner = !closing;
            closing = true;
            if (owner) {
                while (activeCalls != 0) {
                    try { wait(); }
                    catch (InterruptedException interruption) { interrupted = true; }
                }
            }
        }
        if (owner) {
            try { transport.close(); closed.complete(null); }
            catch (IOException | RuntimeException | Error failure) { closed.completeExceptionally(failure); }
        }
        try { closed.join(); }
        catch (CompletionException failure) { throw new IOException("Failed to close routed transport", failure.getCause()); }
        finally { if (interrupted) Thread.currentThread().interrupt(); }
    }

    private static final class RaftTransport implements Transport {
        @Override public byte[] read(RangeDescriptor descriptor, UUID column, byte[] key) throws IOException {
            var request = ByteBuffer.allocate(16 + key.length).putLong(column.getMostSignificantBits()).putLong(column.getLeastSignificantBits()).put(key).array();
            return rpc(() -> { try (RaftClient client = connected(descriptor)) {
                byte[] encoded = content(client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(request))));
                if (encoded.length < 4) throw new IOException("Truncated point reply");
                var input = ByteBuffer.wrap(encoded);
                int size = input.getInt();
                if (size == -1 && !input.hasRemaining()) return null;
                if (size < 0 || size != input.remaining()) throw new IOException("Invalid point reply length");
                byte[] value = new byte[size]; input.get(value); return value;
            } });
        }

        @Override public CommandResult command(RangeDescriptor descriptor, DistributedCommand command) throws IOException {
            return rpc(() -> { try (RaftClient client = connected(descriptor)) {
                byte[] encoded = content(client.io().send(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))));
                try { return CommandResult.decode(encoded); }
                catch (IllegalArgumentException invalid) { throw new IOException("Invalid command reply", invalid); }
            } });
        }

        private static RaftClient connected(RangeDescriptor descriptor) throws IOException {
            var group = RaftGroup.valueOf(RaftGroupId.valueOf(descriptor.groupId()), descriptor.replicas().stream().map(replica ->
                    RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address())
                            .setStartupRole(replica.role() == ReplicaDescriptor.Role.VOTER ? RaftPeerRole.FOLLOWER : RaftPeerRole.LISTENER).build()).toList());
            var properties = new RaftProperties();
            RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
            RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(3, TimeUnit.SECONDS));
            GrpcConfigKeys.Client.setWorkerGroupSize(properties, 1);
            RaftPeerId leader = null;
            try (RaftClient probe = build(group, properties, null)) {
                for (ReplicaDescriptor replica : descriptor.replicas()) {
                    if (replica.role() != ReplicaDescriptor.Role.VOTER) continue;
                    try {
                        var info = rpc(() -> probe.getGroupManagementApi(RaftPeerId.valueOf(replica.peerId())).info(group.getGroupId()));
                        if (info.isSuccess() && info.isRaftStorageHealthy() && info.getGroup() != null
                                && info.getGroup().getGroupId().equals(group.getGroupId())
                                && info.getRoleInfoProto().getRole() == RaftPeerRole.LEADER) {
                            leader = RaftPeerId.valueOf(replica.peerId()); break;
                        }
                    } catch (IOException unavailable) {
                        // A bounded read-only probe can inspect another voter; no user mutation was submitted.
                    }
                }
            }
            if (leader == null) throw new IOException("No healthy range leader discovered");
            return build(group, properties, leader);
        }

        private static RaftClient build(RaftGroup group, RaftProperties properties, RaftPeerId leader) {
            var builder = RaftClient.newBuilder().setRaftGroup(group).setProperties(properties).setRetryPolicy(RetryPolicies.noRetry());
            if (leader != null) builder.setLeaderId(leader);
            return builder.build();
        }

        private static byte[] content(RaftClientReply reply) throws IOException {
            if (reply == null || !reply.isSuccess() || reply.getMessage() == null) {
                throw new IOException("Raft operation did not return a successful application reply", reply == null ? null : reply.getException());
            }
            var content = reply.getMessage().getContent();
            if (content == null || content.size() > DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IOException("Application reply exceeds budget");
            return content.toByteArray();
        }
    }

    @FunctionalInterface
    interface RpcOperation<T> { T call() throws IOException; }

    // Released shaded gRPC can throw unchecked status failures from admin/send/close calls.
    // Normalize only that transport failure; caller argument/programming errors remain visible.
    static <T> T rpc(RpcOperation<T> operation) throws IOException {
        try { return operation.call(); }
        catch (StatusRuntimeException unavailable) { throw new IOException("Raft gRPC transport failed", unavailable); }
    }
}
