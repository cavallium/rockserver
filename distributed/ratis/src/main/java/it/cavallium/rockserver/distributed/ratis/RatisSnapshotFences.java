package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Snapshot fence home CAS over one dedicated data-plane column. Participant gate/capture proofs need separate verification. */
public final class RatisSnapshotFences {
    private final RatisRoutedClient routed;
    private final UUID cluster,database,column;
    private final byte[] schemaDigest;
    public RatisSnapshotFences(RatisRoutedClient routed,UUID column) {
        this.routed=Objects.requireNonNull(routed); this.column=Objects.requireNonNull(column); var directory=routed.directory();
        if (!directory.columns().equals(Set.of(column))) throw new IllegalArgumentException("Snapshot fence directory must have its dedicated column");
        cluster=directory.clusterId(); database=directory.databaseId(); schemaDigest=directory.schemaDigest();
    }
    void requireDirectoryBinding(RangeDirectory expected) {
        if (!Arrays.equals(routed.directory().payloadDigest(),expected.payloadDigest())) throw new IllegalArgumentException("Home reader differs from declared placement");
    }
    long futureRouteBytes()throws IOException {
        long maximum=0;for(var route:routed.directory().ranges())maximum=Math.max(maximum,2*RatisDurableSnapshot.futureRoute(route));return maximum;
    }
    byte[] identityDigest() {
        var bytes=ByteBuffer.allocate(48).put(routed.sessionIdentityDigest()).putLong(column.getMostSignificantBits()).putLong(column.getLeastSignificantBits()).array();
        try { return java.security.MessageDigest.getInstance("SHA-256").digest(bytes); } catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    RatisRoutedClient.PreparedCommand restore(DistributedCommand command,RangeDescriptor descriptor) {
        var restored=routed.restoreSessionCommand(command,descriptor); validate(restored); return restored;
    }
    public SnapshotHomeIdentity identity(SnapshotFence fence,RangeDescriptor descriptor) {
        requireDatabaseBinding(fence.clusterId(),fence.databaseId());
        return new SnapshotHomeIdentity(descriptor.groupId(),descriptor.rangeEpoch(),schemaDigest,column,fence);
    }
    public SnapshotHomeFenceProof fence(SnapshotHomeIdentity identity) throws IOException {
        requireDatabaseBinding(identity.fence().clusterId(),identity.fence().databaseId());
        if (!identity.columnId().equals(column) || !Arrays.equals(identity.schemaDigest(),schemaDigest)) throw new IllegalArgumentException("Snapshot HOME fence context changed");
        return routed.snapshotHomeFence(identity);
    }
    public CompletableFuture<SnapshotHomeFenceProof> fenceAsync(SnapshotHomeIdentity identity) {
        requireDatabaseBinding(identity.fence().clusterId(),identity.fence().databaseId());
        if(!identity.columnId().equals(column)||!Arrays.equals(identity.schemaDigest(),schemaDigest))throw new IllegalArgumentException("Snapshot HOME fence context changed");
        return routed.snapshotHomeFenceAsync(identity);
    }
    public void retire(SnapshotHomeRetirement retirement) throws IOException {
        var identity=retirement.identity(); requireDatabaseBinding(identity.fence().clusterId(),identity.fence().databaseId());
        if (!identity.columnId().equals(column) || !Arrays.equals(identity.schemaDigest(),schemaDigest)) throw new IllegalArgumentException("Snapshot HOME retirement context changed");
        routed.retireSnapshotHome(retirement);
    }
    void requireDatabaseBinding(UUID cluster,UUID database) {
        if (!this.cluster.equals(cluster) || !this.database.equals(database)) throw new IllegalArgumentException("Snapshot home reader belongs to another cluster/database");
    }
    public record RegistrationRead(SnapshotFenceStatus status,RangeDescriptor descriptor) {}
    /** Cached placement only; HOME mutation admission still verifies authoritative state. */
    RangeDescriptor route(UUID fence) { return routed.directory().route(column,key(fence)); }
    public SnapshotFenceStatus read(UUID fenceId) throws IOException { return readRegistration(fenceId).status(); }
    /** Fresh status and home authority captured by the same read; no participant gate is installed or released. */
    public RegistrationRead readRegistration(UUID fenceId) throws IOException {
        var read=routed.readWithRoute(column,key(fenceId)); byte[] bytes=read.value();
        if (bytes==null) return new RegistrationRead(null,read.descriptor());
        try {
            var status=SnapshotFenceStatus.decode(bytes); requireIdentity(status,cluster,database,read.descriptor().groupId(),read.descriptor().rangeEpoch(),key(fenceId));
            return new RegistrationRead(status,read.descriptor());
        } catch (IllegalArgumentException malformed) { throw new IOException("Invalid snapshot fence home record",malformed); }
    }
    public CompletableFuture<RegistrationRead> readRegistrationAsync(UUID fenceId) {
        return routed.readWithRouteAsync(column,key(fenceId)).thenApply(read->{
            if(read.value()==null)return new RegistrationRead(null,read.descriptor());
            try{var status=SnapshotFenceStatus.decode(read.value());requireIdentity(status,cluster,database,read.descriptor().groupId(),read.descriptor().rangeEpoch(),key(fenceId));return new RegistrationRead(status,read.descriptor());}
            catch(IllegalArgumentException malformed){throw new CompletionException(new IOException("Invalid snapshot fence home record",malformed));}
        });
    }
    public CompletableFuture<CommandResult> executeAsync(RatisRoutedClient.PreparedCommand prepared) {validate(prepared);return routed.executeAsync(prepared);}
    /** Retain this exact request for uncertain retries. A confirmed CAS conflict requires a fresh read and new operation identity. */
    public RatisRoutedClient.PreparedCommand prepare(OperationId operation,SnapshotFenceStatus expected,SnapshotFenceStatus next) {
        Objects.requireNonNull(next); byte[] key=key(next.fence().fenceId());
        var prepared=routed.prepare(operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),List.of(new KeyPredicate(column,key,expected==null ? null : expected.encode())));
        validate(prepared); return prepared;
    }
    /** SUCCESS proves only this home CAS, not installation, snapshot capture, or native gate release. */
    public CommandResult execute(RatisRoutedClient.PreparedCommand prepared) throws IOException { validate(prepared); return routed.execute(prepared); }
    private void validate(RatisRoutedClient.PreparedCommand prepared) {
        var descriptor=prepared.descriptor(); validator(cluster,database,descriptor.groupId(),descriptor.rangeEpoch(),schemaDigest,column).accept(prepared.command());
    }
    /** Install on every home replica. Native expected-image predicates order the proof/decision CAS; protected retirement supplies deletion separately. */
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long rangeEpoch,byte[] schemaDigest,UUID column) {
        Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(group); Objects.requireNonNull(column);
        if (rangeEpoch<0 || schemaDigest.length!=32) throw new IllegalArgumentException("Invalid snapshot home validator identity"); byte[] digest=schemaDigest.clone();
        return command -> {
            if (!command.groupId().equals(group) || command.rangeEpoch()!=rangeEpoch || !Arrays.equals(command.schemaDigest(),digest)) throw new IllegalArgumentException("Wrong snapshot home group/schema");
            if (command.mutations().isEmpty() && command.predicates().isEmpty()) {
                if (command.kind()==DistributedCommand.Kind.DATA && command.acknowledgedThrough()>=0 || command.kind()==DistributedCommand.Kind.SESSION_FLOOR) return;
                if (command.kind()==DistributedCommand.Kind.SESSION_CONTROL && command.sessionControl().authority().clusterId().equals(cluster)
                        && command.sessionControl().authority().databaseId().equals(database)) return;
            }
            if (command.kind()!=DistributedCommand.Kind.DATA || command.mutations().size()!=1 || command.predicates().size()!=1 || command.hasBucketData()) throw new IllegalArgumentException("Snapshot home transitions require one physical DATA CAS");
            var mutation=command.mutations().getFirst(); var predicate=command.predicates().getFirst();
            if (mutation.kind()!=Mutation.Kind.PUT || mutation.returnMode()!=Mutation.ReturnMode.NONE || !mutation.columnId().equals(column) || !predicate.columnId().equals(column)
                    || mutation.key().length!=16 || !Arrays.equals(mutation.key(),predicate.key())) throw new IllegalArgumentException("Invalid snapshot home CAS envelope");
            byte[] nextBytes=mutation.value(),previousBytes=predicate.expectedValue();
            var next=SnapshotFenceStatus.decode(nextBytes); requireIdentity(next,cluster,database,group,rangeEpoch,mutation.key());
            // DATA header includes operation/ACK fields. Remove only the two status bodies from this validated fixed envelope.
            long envelope=DistributedCommandCodec.encodedSize(command)-nextBytes.length-(previousBytes==null ? 0 : previousBytes.length);
            long maximumStatus=next.fence().encode().length+SnapshotFenceStatus.HEADER_BYTES+81L*next.fence().participants().size();
            if (2*maximumStatus+envelope>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Snapshot home lacks capacity for future proof/decision CAS");
            if (next.fence().operation().equals(command.operationId())) throw new IllegalArgumentException("Snapshot HOME CAS reused business retention identity");
            if (next.fence().operation().equals(command.operationId())) throw new IllegalArgumentException("Snapshot home CAS reused its original business identity");
            for (var participant:next.fence().participants()) if (participant.installOperation().equals(command.operationId()) || participant.releaseOperation().equals(command.operationId())) throw new IllegalArgumentException("Snapshot home CAS reused participant control identity");
            if (previousBytes==null) {
                if (next.phase()!=SnapshotFenceStatus.Phase.INSTALLING || next.generation()!=0) throw new IllegalArgumentException("Snapshot registration must freeze unproved INSTALLING definition");
            } else {
                var previous=SnapshotFenceStatus.decode(previousBytes); requireIdentity(previous,cluster,database,group,rangeEpoch,mutation.key()); next.requireSuccessorOf(previous);
            }
        };
    }
    static void validateRecord(UUID cluster,UUID database,UUID group,long epoch,byte[] key,byte[] value) { requireIdentity(SnapshotFenceStatus.decode(value),cluster,database,group,epoch,key); }
    private static void requireIdentity(SnapshotFenceStatus status,UUID cluster,UUID database,UUID home,long homeEpoch,byte[] key) {
        var fence=status.fence();
        if (!fence.clusterId().equals(cluster) || !fence.databaseId().equals(database) || !fence.homeGroup().equals(home) || fence.homeEpoch()!=homeEpoch
                || !Arrays.equals(key(fence.fenceId()),key)) throw new IllegalArgumentException("Wrong snapshot fence home identity");
    }
    private static byte[] key(UUID fence) { return ByteBuffer.allocate(16).putLong(fence.getMostSignificantBits()).putLong(fence.getLeastSignificantBits()).array(); }
}
