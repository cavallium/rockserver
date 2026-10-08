package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Initial replicated directory CAS. The caller owns the client and installs validator on all catalogue replicas.
 * Publishing a directory does not fence or activate data groups; a topology controller must establish those facts. */
public final class RatisCatalogue {
    // Both old and new images must fit one conditional command, including its envelopes.
    public static final int MAX_DIRECTORY_BYTES = RangeDirectoryCodec.MAX_ENCODED_BYTES - 256;
    private final RaftClient client;
    private final UUID clusterId, databaseId, groupId, columnId;
    private final long rangeEpoch;
    private final byte[] schemaDigest;
    private final Consumer<DistributedCommand> validator;

    public RatisCatalogue(RaftClient client, UUID clusterId, UUID databaseId, UUID groupId,
                          long rangeEpoch, byte[] schemaDigest, UUID columnId) {
        this.client = Objects.requireNonNull(client);
        this.clusterId = Objects.requireNonNull(clusterId);
        this.databaseId = Objects.requireNonNull(databaseId);
        this.groupId = Objects.requireNonNull(groupId);
        this.columnId = Objects.requireNonNull(columnId);
        if (rangeEpoch < 0 || schemaDigest.length != 32 || !client.getGroupId().getUuid().equals(groupId)) {
            throw new IllegalArgumentException("Invalid catalogue identity/schema");
        }
        this.rangeEpoch = rangeEpoch;
        this.schemaDigest = schemaDigest.clone();
        this.validator = validator(clusterId, databaseId, groupId, rangeEpoch, schemaDigest, columnId);
    }

    /** Linearizable catalogue read; established data operations need no catalogue access. */
    public RangeDirectory read(UUID domainId) throws IOException {
        byte[] key = key(domainId);
        var request = ByteBuffer.allocate(32).putLong(columnId.getMostSignificantBits())
                .putLong(columnId.getLeastSignificantBits()).put(key).array();
        byte[] reply = content(client.io().sendReadOnly(message(request)));
        if (reply.length < 4 || reply.length > MAX_DIRECTORY_BYTES + 4) throw new IOException("Invalid catalogue read size");
        var input = ByteBuffer.wrap(reply);
        int length = input.getInt();
        if (length == -1 && !input.hasRemaining()) return null;
        if (length < 0 || length != input.remaining()) throw new IOException("Invalid catalogue value length");
        var encoded = new byte[length];
        input.get(encoded);
        try {
            var directory = RangeDirectoryCodec.decode(encoded);
            requireIdentity(directory, clusterId, databaseId, key);
            return directory;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid catalogue directory", malformed); }
    }

    /** Prepare once; retry this exact command/operation identity after an uncertain reply. */
    public DistributedCommand prepare(OperationId operationId, RangeDirectory expected, RangeDirectory next) {
        byte[] value = RangeDirectoryCodec.encode(Objects.requireNonNull(next));
        byte[] previous = expected == null ? null : RangeDirectoryCodec.encode(expected);
        byte[] key = key(next.domainId());
        var command = new DistributedCommand(groupId, rangeEpoch, schemaDigest, operationId,
                List.of(new Mutation(Mutation.Kind.PUT, columnId, key, value)),
                List.of(new KeyPredicate(columnId, key, previous)));
        validator.accept(command);
        return command;
    }

    public DistributedCommand prepareAcknowledgement(OperationId operationId, long acknowledgedThrough) {
        return new DistributedCommand(groupId, rangeEpoch, schemaDigest, operationId, List.of(), List.of(), acknowledgedThrough);
    }

    public CommandResult execute(DistributedCommand command) throws IOException {
        validator.accept(command);
        try { return CommandResult.decode(content(client.io().send(message(DistributedCommandCodec.encode(command))))); }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid catalogue command reply", malformed); }
    }

    /** Pure envelope rule. Native ordered predicates implement atomic expected-image comparison and saved conflict receipts. */
    public static Consumer<DistributedCommand> validator(UUID clusterId, UUID databaseId, UUID groupId,
            long rangeEpoch, byte[] schemaDigest, UUID columnId) {
        Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(groupId);
        Objects.requireNonNull(columnId);
        if (rangeEpoch < 0 || schemaDigest.length != 32) throw new IllegalArgumentException("Invalid catalogue validator schema");
        byte[] digest = schemaDigest.clone();
        return command -> {
            if (!command.groupId().equals(groupId) || command.rangeEpoch() != rangeEpoch
                    || !Arrays.equals(command.schemaDigest(), digest)) {
                throw new IllegalArgumentException("Wrong catalogue identity/schema");
            }
            if ((command.acknowledgedThrough() >= 0 || command.minimumSessionEpoch() >= 0) && command.mutations().isEmpty() && command.predicates().isEmpty()) return;
            if (command.mutations().size() != 1 || command.predicates().size() != 1) {
                throw new IllegalArgumentException("Catalogue requires one conditional directory replacement");
            }
            var mutation = command.mutations().getFirst();
            var predicate = command.predicates().getFirst();
            if (mutation.kind() != Mutation.Kind.PUT || mutation.returnMode() != Mutation.ReturnMode.NONE
                    || !mutation.columnId().equals(columnId) || !predicate.columnId().equals(columnId)
                    || mutation.key().length != 16 || !Arrays.equals(mutation.key(), predicate.key())) {
                throw new IllegalArgumentException("Invalid catalogue mutation/predicate");
            }
            var next = decode(mutation.value());
            requireIdentity(next, clusterId, databaseId, mutation.key());
            byte[] previous = predicate.expectedValue();
            if (previous == null) {
                if (next.generation() != 0) throw new IllegalArgumentException("Initial directory generation must be zero");
            } else {
                var expected = decode(previous);
                requireIdentity(expected, clusterId, databaseId, mutation.key());
                if (expected.generation() == Long.MAX_VALUE || next.generation() != expected.generation() + 1
                        || expected.schemaEpoch() != next.schemaEpoch()
                        || !Arrays.equals(expected.schemaDigest(), next.schemaDigest())
                        || !expected.columns().equals(next.columns())
                        || expected.routingPrefixBytes() != next.routingPrefixBytes()) {
                    throw new IllegalArgumentException("Directory update must advance generation with the same schema/projection");
                }
            }
        };
    }

    /** Combined rule: one ordinary CAS or the atomic initial directory/ledger pair. Install on every catalogue replica. */
    public static Consumer<DistributedCommand> validator(UUID clusterId, UUID databaseId, UUID groupId,
            long rangeEpoch, byte[] schemaDigest, UUID directoryColumn, UUID sessionColumn) {
        if (directoryColumn.equals(sessionColumn)) throw new IllegalArgumentException("Catalogue columns must be distinct");
        var directory = validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn);
        var sessions = RatisSessionEpochs.validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,sessionColumn);
        return command -> {
            if (command.mutations().size()==2) {
                if (command.formatVersion()!=2 || command.acknowledgedThrough()!=-1 || command.minimumSessionEpoch()!=-1
                        || command.predicates().size()!=2 || !command.mutations().get(0).columnId().equals(directoryColumn)
                        || !command.mutations().get(1).columnId().equals(sessionColumn)
                        || command.predicates().stream().anyMatch(predicate -> predicate.expectedValue()!=null)) {
                    throw new IllegalArgumentException("Genesis requires an ordered pair of absent directory/session keys");
                }
                directory.accept(new DistributedCommand(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.operationId(),
                        List.of(command.mutations().get(0)),List.of(command.predicates().get(0))));
                sessions.accept(new DistributedCommand(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.operationId(),
                        List.of(command.mutations().get(1)),List.of(command.predicates().get(1))));
            } else if (command.mutations().size()==1 && command.mutations().getFirst().columnId().equals(sessionColumn)) sessions.accept(command);
            else directory.accept(command);
        };
    }

    private static RangeDirectory decode(byte[] encoded) {
        if (encoded.length > MAX_DIRECTORY_BYTES) throw new IllegalArgumentException("Catalogue directory exceeds CAS budget");
        var directory = RangeDirectoryCodec.decode(encoded);
        if (!Arrays.equals(encoded, RangeDirectoryCodec.encode(directory))) throw new IllegalArgumentException("Noncanonical catalogue directory");
        return directory;
    }
    private static void requireIdentity(RangeDirectory directory, UUID cluster, UUID database, byte[] key) {
        if (!directory.clusterId().equals(cluster) || !directory.databaseId().equals(database)
                || !Arrays.equals(key(directory.domainId()), key)) throw new IllegalArgumentException("Catalogue directory identity mismatch");
    }
    private static byte[] key(UUID domain) {
        return ByteBuffer.allocate(16).putLong(domain.getMostSignificantBits()).putLong(domain.getLeastSignificantBits()).array();
    }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(RaftClientReply reply) throws IOException {
        if (!reply.isSuccess()) throw new IOException("Catalogue request failed", reply.getException());
        if (reply.getMessage() == null) throw new IOException("Missing catalogue reply");
        return reply.getMessage().getContent().toByteArray();
    }
}
