package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/** Catalogue epoch CAS and opt-in lifetime authority. Caller owns the long-lived Raft client. */
public final class RatisSessionEpochs {
    private final RaftClient client;
    private final UUID cluster, database, group, column;
    private final long rangeEpoch;
    private final byte[] schema;
    private final Consumer<DistributedCommand> validator;
    public RatisSessionEpochs(RaftClient client,UUID cluster,UUID database,UUID group,long rangeEpoch,byte[] schema,UUID column) {
        this.client=Objects.requireNonNull(client); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database);
        this.group=Objects.requireNonNull(group); this.column=Objects.requireNonNull(column);
        if (rangeEpoch < 0 || schema.length!=32 || !client.getGroupId().getUuid().equals(group)) throw new IllegalArgumentException("Invalid epoch catalogue binding");
        this.rangeEpoch=rangeEpoch; this.schema=schema.clone(); this.validator=validator(cluster,database,group,rangeEpoch,schema,column);
    }
    void requireCatalogueBinding(UUID expectedCluster,UUID expectedDatabase,UUID expectedGroup,long expectedEpoch,byte[] expectedSchema,UUID expectedColumn) {
        if (!cluster.equals(expectedCluster) || !database.equals(expectedDatabase) || !group.equals(expectedGroup)
                || rangeEpoch!=expectedEpoch || !Arrays.equals(schema,expectedSchema) || !column.equals(expectedColumn)) throw new IllegalArgumentException("Wrong node catalogue authority binding");
    }
    void validatePrepared(DistributedCommand command) { validator.accept(command); }
    void requireDatabaseBinding(UUID expectedCluster,UUID expectedDatabase) {
        if (!cluster.equals(expectedCluster) || !database.equals(expectedDatabase)) throw new IllegalArgumentException("Wrong session authority database");
    }
    byte[] identityDigest() {
        byte[] bytes=ByteBuffer.allocate(104).put(uuid(cluster)).put(uuid(database)).put(uuid(group)).put(uuid(column)).putLong(rangeEpoch).put(schema).array();
        try { return java.security.MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    public SessionEpochLedger read() throws IOException {
        var query=ByteBuffer.allocate(32).put(uuid(column)).put(uuid(database)).array();
        byte[] encoded=content(RatisRoutedClient.rpc(() -> client.io().sendReadOnly(message(query))),SessionEpochLedger.MAX_BYTES+4);
        if (encoded.length<4) throw new IOException("Truncated epoch ledger reply");
        var input=ByteBuffer.wrap(encoded); int count=input.getInt();
        if (count==-1 && !input.hasRemaining()) return null;
        if (count<0 || count!=input.remaining()) throw new IOException("Invalid epoch ledger reply length");
        byte[] bytes=new byte[count]; input.get(bytes);
        try {
            var ledger=SessionEpochLedger.decode(bytes); identity(ledger,cluster,database); return ledger;
        } catch (IllegalArgumentException bad) { throw new IOException("Invalid epoch catalogue state",bad); }
    }
    public static final class PreparedGrant {
        private final DistributedCommand command;
        private final SessionEpochLedger.Grant grant;
        private PreparedGrant(DistributedCommand command,SessionEpochLedger.Grant grant) { this.command=command; this.grant=grant; }
        public DistributedCommand command() { return command; }
        public SessionEpochLedger.Grant grant() { return grant; }
    }
    public PreparedGrant prepareGrant(OperationId operation,SessionEpochLedger expected,UUID session,long expiresAtMillis) {
        var next=Objects.requireNonNull(expected).issue(session,expiresAtMillis);
        return new PreparedGrant(prepare(operation,expected,next),next.grants().getLast());
    }

    public DistributedCommand prepare(OperationId operation,SessionEpochLedger expected,SessionEpochLedger replacement) {
        return prepareInternal(operation,expected,replacement,-1);
    }
    public DistributedCommand prepare(OperationId operation,SessionEpochLedger expected,SessionEpochLedger replacement,long acknowledgedThrough) {
        if (acknowledgedThrough<0) throw new IllegalArgumentException("Negative acknowledgement");
        return prepareInternal(operation,expected,replacement,acknowledgedThrough);
    }
    private DistributedCommand prepareInternal(OperationId operation,SessionEpochLedger expected,SessionEpochLedger replacement,long acknowledgedThrough) {
        var mutations=List.of(new Mutation(Mutation.Kind.PUT,column,uuid(database),replacement.encode()));
        var predicates=List.of(new KeyPredicate(column,uuid(database),expected==null ? null : expected.encode()));
        var command=acknowledgedThrough<0 ? new DistributedCommand(group,rangeEpoch,schema,operation,mutations,predicates)
                : new DistributedCommand(group,rangeEpoch,schema,operation,mutations,predicates,acknowledgedThrough);
        validator.accept(command); return command;
    }
    public DistributedCommand prepareAcknowledgement(OperationId operation,long acknowledgedThrough) {
        return new DistributedCommand(group,rangeEpoch,schema,operation,List.of(),List.of(),acknowledgedThrough);
    }
    public CommandResult execute(DistributedCommand prepared) throws IOException {
        validator.accept(prepared);
        byte[] result=content(RatisRoutedClient.rpc(() -> client.io().send(message(DistributedCommandCodec.encode(prepared)))),CommandResult.MAX_BYTES);
        try { return CommandResult.decode(result); }
        catch (IllegalArgumentException bad) { throw new IOException("Invalid epoch catalogue command result",bad); }
    }
    /** Called only for new floor admission. No current-state read is repeated during committed apply. */
    public Consumer<DistributedCommand> floorAdmission(RangeDirectory directory,UUID dataGroup) {
        var descriptor=authorityRange(directory,dataGroup);
        return command -> {
            if (!command.groupId().equals(dataGroup) || command.rangeEpoch()!=descriptor.rangeEpoch()
                    || !Arrays.equals(command.schemaDigest(),directory.schemaDigest())) throw new IllegalArgumentException("Wrong epoch authority group/schema");
            admitFloor(command);
        };
    }
    /** Freeze one verified catalogue image into a range control; retries must retain the returned command. */
    public DistributedCommand prepareSessionBinding(RangeDirectory directory,UUID dataGroup,OperationId issuer,
            SessionEpochLedger.Grant grant,int outstandingWindow,long acknowledgedThrough) throws IOException {
        var descriptor=authorityRange(directory,dataGroup);
        var ledger=read();
        if (ledger==null) throw new IOException("Epoch catalogue is not initialized");
        return DistributedCommand.sessionControl(dataGroup,descriptor.rangeEpoch(),directory.schemaDigest(),issuer,
                RangeSessionControl.bind(ledger,grant,outstandingWindow),acknowledgedThrough);
    }
    public DistributedCommand prepareAuthorityAdvance(RangeDirectory directory,UUID dataGroup,OperationId issuer,
            long acknowledgedThrough) throws IOException {
        var descriptor=authorityRange(directory,dataGroup);
        var ledger=read();
        if (ledger==null) throw new IOException("Epoch catalogue is not initialized");
        return DistributedCommand.sessionControl(dataGroup,descriptor.rangeEpoch(),directory.schemaDigest(),issuer,
                RangeSessionControl.advance(ledger),acknowledgedThrough);
    }
    /** Validates controls only. Maintenance issuer authority does not depend on a live user grant. */
    public Consumer<DistributedCommand> sessionControlAdmission(RangeDirectory directory,UUID dataGroup) {
        var descriptor=authorityRange(directory,dataGroup);
        return command -> {
            var control=command.sessionControl();
            if (control==null || !command.groupId().equals(dataGroup) || command.rangeEpoch()!=descriptor.rangeEpoch()
                    || !Arrays.equals(command.schemaDigest(),directory.schemaDigest())) throw new IllegalArgumentException("Wrong session control binding");
            try {
                var ledger=read();
                if (ledger==null) throw new IllegalStateException("Absent session authority cut");
                var current=SessionAuthority.from(ledger);
                current.requireAtLeast(control.authority());
                if (control.binding()) {
                    try { RangeSessionControl.bind(ledger,control.grant(),control.outstandingWindow()); }
                    catch (IllegalArgumentException expired) { throw new IllegalStateException("Catalogue no longer authorizes this binding",expired); }
                }
            } catch (IOException unavailable) { throw new IllegalStateException("Catalogue session authority unavailable",unavailable); }
        };
    }
    private RangeDescriptor authorityRange(RangeDirectory directory,UUID dataGroup) {
        if (!directory.clusterId().equals(cluster) || !directory.databaseId().equals(database)) throw new IllegalArgumentException("Wrong authority directory");
        var descriptor=directory.ranges().stream().filter(range -> range.groupId().equals(dataGroup)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Authority group is absent from directory"));
        if (descriptor.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Authority group is inactive");
        return descriptor;
    }
    private void admitFloor(DistributedCommand command) {
        if (command.minimumSessionEpoch()<0 || command.sessionControl()!=null) throw new IllegalArgumentException("Not an epoch-floor command");
        try {
            var ledger=read();
            if (ledger==null || !ledger.isLive(command.operationId()) || command.minimumSessionEpoch()>ledger.minimumEpoch()) {
                throw new IllegalStateException("Catalogue does not authorize this epoch floor/issuer");
            }
        } catch (IOException unavailable) { throw new IllegalStateException("Catalogue lifetime authority unavailable",unavailable); }
    }
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column) {
        Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(group); Objects.requireNonNull(column);
        if (epoch<0 || schema.length!=32) throw new IllegalArgumentException("Invalid epoch validator binding");
        byte[] digest=schema.clone();
        return command -> {
            if (command.formatVersion()==6 || command.acceptance()!=null) throw new IllegalArgumentException("Session catalogue does not accept bucket or transaction data commands");
            if (!command.groupId().equals(group) || command.rangeEpoch()!=epoch || !Arrays.equals(command.schemaDigest(),digest)
                    || command.minimumSessionEpoch()!=-1) {
                throw new IllegalArgumentException("Wrong epoch catalogue identity/schema");
            }
            if (command.acknowledgedThrough()>=0 && command.mutations().isEmpty() && command.predicates().isEmpty()) return;
            if (command.mutations().size()!=1 || command.predicates().size()!=1) {
                throw new IllegalArgumentException("Epoch catalogue requires one conditional replacement");
            }
            var mutation=command.mutations().getFirst(); var predicate=command.predicates().getFirst();
            if (mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE || !mutation.columnId().equals(column)
                    || !predicate.columnId().equals(column) || !Arrays.equals(mutation.key(),uuid(database)) || !Arrays.equals(predicate.key(),mutation.key())) {
                throw new IllegalArgumentException("Invalid epoch catalogue mutation/predicate");
            }
            var next=SessionEpochLedger.decode(mutation.value()); identity(next,cluster,database);
            var previous=predicate.expectedValue()==null ? null : SessionEpochLedger.decode(predicate.expectedValue());
            if (previous!=null) identity(previous,cluster,database);
            next.requireSuccessorOf(previous);
        };
    }
    private static void identity(SessionEpochLedger ledger,UUID cluster,UUID database) {
        if (!ledger.clusterId().equals(cluster) || !ledger.databaseId().equals(database)) throw new IllegalArgumentException("Wrong epoch catalogue identity");
    }
    private static byte[] uuid(UUID id) { return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(); }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(RaftClientReply reply,int maximum) throws IOException {
        if (reply==null || !reply.isSuccess() || reply.getMessage()==null) throw new IOException("Epoch catalogue operation failed",reply==null ? null : reply.getException());
        var bytes=reply.getMessage().getContent();
        if (bytes==null || bytes.size()>maximum) throw new IOException("Epoch catalogue reply exceeds budget");
        return bytes.toByteArray();
    }
}
