package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Initial replicated directory CAS. The caller owns the client and installs validator on all catalogue replicas.
 * Schema-bearing bootstrap must publish schema content and column issuance first.
 * Publishing a directory does not fence or activate data groups; a topology controller must establish those facts. */
public final class RatisCatalogue {
    // Both old and new images must fit one conditional command, including its envelopes.
    public static final int MAX_DIRECTORY_BYTES = (DistributedCommandCodec.MAX_ENCODED_BYTES-RatisSchemaAdministration.MAX_HEAD_BYTES-RatisRangeImportCatalogue.MAX_HEAD_BYTES-TopologyChange.MAX_STATUS_BYTES-2*DatabaseDomainRoster.MAX_BYTES-512)/2;
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
        var request=ScopedTransactionRpc.pointRequest(columnId,key);
        byte[] encoded;
        try { encoded=ScopedTransactionRpc.pointValue(content(client.io().sendReadOnly(message(request)),ScopedTransactionRpc.HEADER_BYTES+4+MAX_DIRECTORY_BYTES),MAX_DIRECTORY_BYTES); }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid catalogue read reply",malformed); }
        if (encoded==null) return null;
        try {
            var directory = RangeDirectoryCodec.decode(encoded);
            requireIdentity(directory, clusterId, databaseId, key);
            return directory;
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid catalogue directory", malformed); }
    }

    /** Prepare once; retry this exact command/operation identity after an uncertain reply. */
    public DistributedCommand prepare(OperationId operationId, RangeDirectory expected, RangeDirectory next) {
        return prepare(operationId,expected,next,null);
    }

    /** Null roster is an exact absent-row witness, never a lookup or implicit retry rebase. */
    public DistributedCommand prepare(OperationId operationId,RangeDirectory expected,RangeDirectory next,DatabaseDomainRoster previousRoster) {
        return prepareGuarded(operationId,expected,next,null,null,null,previousRoster);
    }
    /** Resume ordinary topology changes after the domain's schema operation has fully retired or aborted. */
    public DistributedCommand prepareAfterAdministration(OperationId operationId, RangeDirectory expected, RangeDirectory next,
                                                        RatisSchemaAdministration.Head administrativeHead) {
        Objects.requireNonNull(administrativeHead);
        return prepareGuarded(operationId,expected,next,administrativeHead.encode(),null,null,null);
    }

    /** Explicitly qualify both terminal administrative owners; null means an exact absent-head guard. */
    public DistributedCommand prepareAfterAdministration(OperationId operationId,RangeDirectory expected,RangeDirectory next,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead) {
        return prepareAfterAdministration(operationId,expected,next,schemaHead,importHead,null);
    }
    /** Every administrative owner is guarded by its exact absent or terminal domain head. */
    public DistributedCommand prepareAfterAdministration(OperationId operationId,RangeDirectory expected,RangeDirectory next,RatisSchemaAdministration.Head schemaHead,RatisRangeImportCatalogue.Head importHead,TopologyChange.State topologyHead) {
        return prepareGuarded(operationId,expected,next,schemaHead==null ? null : schemaHead.encode(),importHead==null ? null : importHead.encode(),topologyHead==null ? null : topologyHead.encode(),null);
    }
    private DistributedCommand prepareGuarded(OperationId operationId,RangeDirectory expected,RangeDirectory next,byte[] administrativeHead,byte[] importHead,byte[] topologyHead,DatabaseDomainRoster previousRoster) {
        byte[] value = RangeDirectoryCodec.encode(Objects.requireNonNull(next));
        byte[] previous = expected == null ? null : RangeDirectoryCodec.encode(expected);
        byte[] key = key(next.domainId());
        var mutations=new ArrayList<Mutation>(); mutations.add(new Mutation(Mutation.Kind.PUT,columnId,key,value));
        var predicates=new ArrayList<KeyPredicate>(List.of(new KeyPredicate(columnId,key,previous),new KeyPredicate(columnId,RatisSchemaAdministration.headKey(next.domainId()),administrativeHead),new KeyPredicate(columnId,RatisRangeImportCatalogue.headKey(next.domainId()),importHead),new KeyPredicate(columnId,RatisTopologyCatalogue.headKey(next.domainId()),topologyHead)));
        if(expected==null) {
            var roster=previousRoster==null ? DatabaseDomainRoster.initial(clusterId,databaseId,List.of(next.domainId())) : previousRoster.add(List.of(next.domainId()));
            mutations.add(new Mutation(Mutation.Kind.PUT,columnId,RatisDatabaseDomainRoster.key(databaseId),roster.encode())); predicates.add(new KeyPredicate(columnId,RatisDatabaseDomainRoster.key(databaseId),previousRoster==null ? null : previousRoster.encode()));
        } else if(previousRoster!=null) throw new IllegalArgumentException("Directory replacement cannot change the domain roster");
        var command=new DistributedCommand(groupId,rangeEpoch,schemaDigest,operationId,mutations,predicates);
        validator.accept(command);
        return command;
    }

    public DistributedCommand prepareAcknowledgement(OperationId operationId, long acknowledgedThrough) {
        return new DistributedCommand(groupId, rangeEpoch, schemaDigest, operationId, List.of(), List.of(), acknowledgedThrough);
    }

    public CommandResult execute(DistributedCommand command) throws IOException {
        validator.accept(command);
        try { return CommandResult.decode(content(client.io().send(message(DistributedCommandCodec.encode(command))),CommandResult.MAX_BYTES)); }
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
            if (command.hasBucketData() || command.acceptance()!=null || command.transactionRejection()) throw new IllegalArgumentException("Catalogue does not accept bucket or transaction data commands");
            if (!command.groupId().equals(groupId) || command.rangeEpoch() != rangeEpoch
                    || !Arrays.equals(command.schemaDigest(), digest)) {
                throw new IllegalArgumentException("Wrong catalogue identity/schema");
            }
            if ((command.acknowledgedThrough() >= 0 || command.minimumSessionEpoch() >= 0) && command.mutations().isEmpty() && command.predicates().isEmpty()) return;
            if(command.kind()!=DistributedCommand.Kind.DATA) throw new IllegalArgumentException("Directory publication requires DATA CAS");
            if ((command.mutations().size()!=1 && command.mutations().size()!=2) || (command.predicates().size()!=4 && command.predicates().size()!=5)) {
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
            var guard=command.predicates().get(1);
            if (!guard.columnId().equals(columnId) || !command.predicates().get(2).columnId().equals(columnId) || !command.predicates().get(3).columnId().equals(columnId)) throw new IllegalArgumentException("Wrong schema administration guard column");
            if (previous == null) {
                if (next.generation() != 0 || command.predicates().size()!=5) throw new IllegalArgumentException("Initial directory requires generation zero and a database roster witness");
                var rosterGuard=command.predicates().get(4); if(!rosterGuard.columnId().equals(columnId)) throw new IllegalArgumentException("Wrong database roster column"); RatisDatabaseDomainRoster.requirePublication(rosterGuard,command.mutations().size()==2 ? command.mutations().get(1) : null,next);
                RatisSchemaAdministration.requireTopologyGuard(guard,next);
                RatisRangeImportCatalogue.requireTopologyGuard(command.predicates().get(2),next);
                RatisTopologyCatalogue.requireAdministrationGuard(command.predicates().get(3),next);
            } else {
                if(command.mutations().size()!=1 || command.predicates().size()!=4) throw new IllegalArgumentException("Directory replacement cannot mutate its database roster");
                var expected = decode(previous);
                requireIdentity(expected, clusterId, databaseId, mutation.key());
                RatisSchemaAdministration.requireTopologyGuard(guard,expected);
                RatisRangeImportCatalogue.requireTopologyGuard(command.predicates().get(2),expected);
                RatisTopologyCatalogue.requireAdministrationGuard(command.predicates().get(3),expected);
                for(var range:expected.ranges()) if(range.topologyState()==RangeDescriptor.TopologyState.FENCED && next.ranges().stream().anyMatch(target -> target.groupId().equals(range.groupId()) && target.topologyState()==RangeDescriptor.TopologyState.ACTIVE)) throw new IllegalArgumentException("Fenced group activation requires distinct import publication");
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

    /** Combined rule: directory/session CAS, immutable schema publication or atomic genesis. Install on every catalogue replica. */
    public static Consumer<DistributedCommand> validator(UUID clusterId, UUID databaseId, UUID groupId,
            long rangeEpoch, byte[] schemaDigest, UUID directoryColumn, UUID sessionColumn) {
        return validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn,sessionColumn,
                command -> { throw new IllegalArgumentException("Operator catalogue admission is not configured"); });
    }
    /** Operator publication uses a configured trusted policy and committed native chunk reader. */
    public static Consumer<DistributedCommand> validator(UUID clusterId, UUID databaseId, UUID groupId,
            long rangeEpoch, byte[] schemaDigest, UUID directoryColumn, UUID sessionColumn,Consumer<DistributedCommand> operators) {
        return validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn,sessionColumn,operators,null);
    }
    public static Consumer<DistributedCommand> validator(UUID clusterId,UUID databaseId,UUID groupId,long rangeEpoch,byte[] schemaDigest,
            UUID directoryColumn,UUID sessionColumn,Consumer<DistributedCommand> operators,RatisSchemaAdministration.ImmutableReader immutable) {
        if (directoryColumn.equals(sessionColumn)) throw new IllegalArgumentException("Catalogue columns must be distinct");
        Objects.requireNonNull(operators);
        var directory = validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn);
        var sessions = RatisSessionEpochs.validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,sessionColumn);
        var schemas = RatisSchemaCatalogue.validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn);
        var administration = immutable==null ? RatisSchemaAdministration.validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn)
                : RatisSchemaAdministration.validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn,immutable);
        var imports=RatisRangeImportCatalogue.validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn,immutable==null ? null : immutable::read);
        var allocation=RatisColumnAllocation.validator(groupId,rangeEpoch,schemaDigest,directoryColumn);
        var topology=RatisTopologyCatalogue.validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn,immutable==null ? null : immutable::read);
        var cdc=RatisCdcSubscriptionCatalogue.validator(clusterId,databaseId,groupId,rangeEpoch,schemaDigest,directoryColumn);
        return command -> {
            if(RatisTopologyCatalogue.isCommand(command)) topology.accept(command);
            else if(RatisCdcSubscriptionCatalogue.isCommand(command)) cdc.accept(command);
            else if (RatisRangeImportCatalogue.isCommand(command)) imports.accept(command);
            else if (RatisOperatorCatalogue.isCommand(command)) operators.accept(command);
            else if (RatisColumnAllocation.isAllocationCommand(command)) allocation.accept(command);
            else if (RatisSchemaAdministration.isCommand(command)) administration.accept(command);
            else if (RatisSchemaCatalogue.isSchemaCommand(command) && command.mutations().getFirst().columnId().equals(directoryColumn)) schemas.accept(command);
            else if (!command.mutations().isEmpty() && command.mutations().getFirst().columnId().equals(directoryColumn) && !command.predicates().isEmpty()
                    && command.predicates().size()>1 && command.predicates().get(1).columnId().equals(sessionColumn)) {
                if(command.kind()!=DistributedCommand.Kind.DATA || command.acknowledgedThrough()!=-1 || command.minimumSessionEpoch()!=-1 || command.predicates().size()!=6 || command.predicates().getFirst().expectedValue()!=null) throw new IllegalArgumentException("Initial domain requires frozen directory/ledger/administration/roster witnesses");
                var ledgerGuard=command.predicates().get(1); if(ledgerGuard.bucket() || !Arrays.equals(ledgerGuard.key(),key(databaseId))) throw new IllegalArgumentException("Initial domain ledger key changed");
                int rosterIndex=1;
                if(ledgerGuard.expectedValue()==null) {
                    if(command.mutations().size()<2 || !command.mutations().get(1).columnId().equals(sessionColumn)) throw new IllegalArgumentException("Absent database ledger must be initialized atomically");
                    sessions.accept(new DistributedCommand(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.operationId(),List.of(command.mutations().get(1)),List.of(ledgerGuard))); rosterIndex=2;
                } else {
                    var ledger=SessionEpochLedger.decode(ledgerGuard.expectedValue()); if(!ledger.clusterId().equals(clusterId) || !ledger.databaseId().equals(databaseId)) throw new IllegalArgumentException("Initial domain changed database ledger identity");
                }
                if(command.mutations().size()<rosterIndex || command.mutations().size()>rosterIndex+1) throw new IllegalArgumentException("Initial domain has unrelated mutations");
                var mutations=new ArrayList<Mutation>(); mutations.add(command.mutations().getFirst()); if(command.mutations().size()>rosterIndex) mutations.add(command.mutations().get(rosterIndex));
                directory.accept(new DistributedCommand(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.operationId(),mutations,List.of(command.predicates().get(0),command.predicates().get(2),command.predicates().get(3),command.predicates().get(4),command.predicates().get(5))));
            } else if (command.mutations().size()==1 && command.mutations().getFirst().columnId().equals(sessionColumn)) sessions.accept(command);
            else if (command.mutations().size()==1 && command.mutations().getFirst().columnId().equals(directoryColumn)
                    && command.mutations().getFirst().key().length==33) schemas.accept(command);
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
    private static byte[] content(RaftClientReply reply,int maximum) throws IOException {
        if (reply==null || !reply.isSuccess()) throw new IOException("Catalogue request failed", reply==null ? null : reply.getException());
        if (reply.getMessage() == null) throw new IOException("Missing catalogue reply");
        var bytes=reply.getMessage().getContent(); if(bytes.size()>maximum) throw new IOException("Catalogue reply exceeds operation budget");
        return bytes.toByteArray();
    }
}
