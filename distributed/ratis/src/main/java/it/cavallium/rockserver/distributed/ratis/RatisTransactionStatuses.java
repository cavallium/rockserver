package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;

/** Home-record CAS over a dedicated data-plane directory, not the catalogue. Does not prove participant durability or install locks. */
public final class RatisTransactionStatuses {
    private final RatisRoutedClient routed;
    private final UUID cluster,database,column;
    private final byte[] schemaDigest;
    public RatisTransactionStatuses(RatisRoutedClient routed,UUID column) {
        this.routed=Objects.requireNonNull(routed); this.column=Objects.requireNonNull(column); var directory=routed.directory();
        if (!directory.columns().equals(Set.of(column))) throw new IllegalArgumentException("Status directory must have its dedicated status column");
        cluster=directory.clusterId(); database=directory.databaseId(); schemaDigest=directory.schemaDigest();
    }
    public record RegistrationRead(TransactionStatus status,RangeDescriptor descriptor) {}
    public TransactionStatus read(UUID transaction) throws IOException { return readRegistration(transaction).status(); }
    public RegistrationRead readRegistration(UUID transaction) throws IOException {
        var read=routed.readWithRoute(column,key(transaction)); byte[] bytes=read.value();
        if (bytes==null) return new RegistrationRead(null,read.descriptor());
        try { var status=TransactionStatus.decode(bytes); requireIdentity(status,cluster,database,key(transaction)); return new RegistrationRead(status,read.descriptor()); }
        catch (IllegalArgumentException malformed) { throw new IOException("Invalid transaction home record",malformed); }
    }
    /** Fresh committed decision and home authority captured by the same read; does not submit FINALIZE. */
    public ParticipantFinalization readFinalization(UUID transaction,UUID participant,OperationId recovery) throws IOException {
        var read=readRegistration(transaction);
        if (read.status()==null || read.status().phase()==TransactionStatus.Phase.PREPARING) throw new IOException("Home has no terminal decision");
        return new ParticipantFinalization(read.descriptor().groupId(),read.descriptor().rangeEpoch(),participant,recovery,read.status());
    }
    /** Each transition has its own caller-owned operation identity. An uncertain retry retains this exact command.
     * After a confirmed conflict, read the home again and use a new CAS operation identity. */
    public RatisRoutedClient.PreparedCommand prepare(OperationId operation,TransactionStatus expected,TransactionStatus next) {
        byte[] key=key(next.transactionId()); requireIdentity(next,cluster,database,key);
        var prepared=routed.prepare(operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),List.of(new KeyPredicate(column,key,expected==null ? null : expected.encode())));
        validate(prepared); return prepared;
    }
    /** Success means this CAS applied; it does not mean the complete distributed transaction finalized. */
    public CommandResult execute(RatisRoutedClient.PreparedCommand prepared) throws IOException { validate(prepared); return routed.execute(prepared); }
    private void validate(RatisRoutedClient.PreparedCommand prepared) {
        var descriptor=prepared.descriptor(); validator(cluster,database,descriptor.groupId(),descriptor.rangeEpoch(),schemaDigest,column).accept(prepared.command());
    }
    /** Install on every home replica. Native predicates compare the expected image in log order; no deletion/GC is admitted. */
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long rangeEpoch,byte[] schemaDigest,UUID column) {
        Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(group); Objects.requireNonNull(column);
        if (rangeEpoch<0 || schemaDigest.length!=32) throw new IllegalArgumentException("Invalid home validator identity"); byte[] digest=schemaDigest.clone();
        return command -> {
            if (!command.groupId().equals(group) || command.rangeEpoch()!=rangeEpoch || !Arrays.equals(command.schemaDigest(),digest)) throw new IllegalArgumentException("Wrong transaction home group/schema");
            // Maintenance does not modify the home record. Session authority is additionally checked by withSessions/native apply.
            if (command.mutations().isEmpty() && command.predicates().isEmpty()) {
                if (command.kind()==DistributedCommand.Kind.DATA && command.acknowledgedThrough()>=0 || command.kind()==DistributedCommand.Kind.SESSION_FLOOR) return;
                if (command.kind()==DistributedCommand.Kind.SESSION_CONTROL && command.sessionControl().authority().clusterId().equals(cluster) && command.sessionControl().authority().databaseId().equals(database)) return;
            }
            if (command.kind()!=DistributedCommand.Kind.DATA || command.mutations().size()!=1 || command.predicates().size()!=1 || command.hasBucketData()) throw new IllegalArgumentException("Home transitions require one physical DATA CAS");
            var mutation=command.mutations().getFirst(); var predicate=command.predicates().getFirst();
            if (mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE || !mutation.columnId().equals(column) || !predicate.columnId().equals(column)
                    || mutation.key().length!=16 || !Arrays.equals(mutation.key(),predicate.key())) throw new IllegalArgumentException("Invalid home CAS envelope");
            var next=TransactionStatus.decode(mutation.value()); requireIdentity(next,cluster,database,mutation.key());
            if (predicate.expectedValue()==null) next.requireInitial();
            else { var old=TransactionStatus.decode(predicate.expectedValue()); requireIdentity(old,cluster,database,mutation.key()); next.requireSuccessorOf(old); }
        };
    }
    private static void requireIdentity(TransactionStatus status,UUID cluster,UUID database,byte[] key) {
        if (!status.clusterId().equals(cluster) || !status.databaseId().equals(database) || !Arrays.equals(key(status.transactionId()),key)) throw new IllegalArgumentException("Wrong transaction home identity");
    }
    private static byte[] key(UUID transaction) { return ByteBuffer.allocate(16).putLong(transaction.getMostSignificantBits()).putLong(transaction.getLeastSignificantBits()).array(); }
}
