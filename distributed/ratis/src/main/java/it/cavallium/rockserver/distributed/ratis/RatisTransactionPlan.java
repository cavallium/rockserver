package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;

/** One frozen routing generation and original mutation/result order, including predicate-only participants. */
public final class RatisTransactionPlan {
    public static final long MAX_BYTES=16L*1024*1024;
    public record Partition(RangeDescriptor route,DistributedCommand command,TransactionScope scope,List<Integer> resultPositions) {
        public Partition { resultPositions=List.copyOf(resultPositions); }
    }
    public record Accepted(TransactionStatus initial,List<RatisDurableTransaction.Intent> intents) {
        public Accepted { intents=List.copyOf(intents); }
    }
    private static final class Builder {
        final RangeDescriptor route;
        final List<Mutation> mutations=new ArrayList<>(); final List<KeyPredicate> predicates=new ArrayList<>(); final List<Integer> returns=new ArrayList<>();
        Builder(RangeDescriptor route) { this.route=route; }
    }
    private final List<RangeDirectory> directories;
    private final OperationId operation;
    private final List<Partition> partitions;
    private record CapturedCut(RangeDescriptor route,TransactionScope scope,ScopedTransactionRpc.Handle handle) {}
    private final Map<UUID,CapturedCut> captures;
    private RatisTransactionPlan(List<RangeDirectory> directories,OperationId operation,List<Partition> partitions,Map<UUID,CapturedCut> captures) {
        this.directories=List.copyOf(directories); this.operation=operation; this.partitions=List.copyOf(partitions); this.captures=Map.copyOf(captures);
    }
    public List<RangeDirectory> directories() { return directories; }
    public OperationId operation() { return operation; }
    public List<Partition> partitions() { return partitions; }
    public static RatisTransactionPlan logical(List<DistributedSchema> schemas,List<RangeDirectory> directories,OperationId operation,List<RatisLogicalClient.Change> changes,List<RatisLogicalClient.Condition> conditions) {
        var plan=RatisLogicalClient.plan(column -> schemas.stream().filter(schema->schema.columns().stream().anyMatch(binding->binding.apiId()==column)).findFirst().orElseThrow(()->new IllegalArgumentException("Unknown transaction API column")),changes,conditions,MAX_BYTES);return create(directories,operation,plan.mutations(),plan.predicates());
    }
    public static RatisTransactionPlan create(List<RangeDirectory> directories,OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates) {
        return create(directories,operation,mutations,predicates,Map.of(),-1);
    }
    /** Normalize the complete original effectful DATA input without losing its piggyback ACK. */
    public static RatisTransactionPlan fromOriginal(RangeDirectory target,DistributedCommand original) {
        Objects.requireNonNull(original);
        if(original.kind()!=DistributedCommand.Kind.DATA || original.mutations().isEmpty() && original.predicates().isEmpty() || !Arrays.equals(original.schemaDigest(),target.schemaDigest()))throw new IllegalArgumentException("Reroute requires original effectful DATA with the target schema");
        return create(List.of(target),original.operationId(),original.mutations(),original.predicates(),Map.of(),original.acknowledgedThrough());
    }
    /** Every participant in the consistent original read projection must certify, including groups with no writes. */
    public static RatisTransactionPlan captured(List<RangeDirectory> directories,OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates,RatisDurableSnapshot.Snapshot snapshot) {
        Objects.requireNonNull(directories); Objects.requireNonNull(snapshot);
        var home=snapshot.observedHome(); var fence=snapshot.initial().fence();
        if (snapshot.captureMode()!=RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED || snapshot.releaseRequested()
                || home==null || home.phase()!=SnapshotFenceStatus.Phase.COMPLETE || !home.fence().equals(fence)
                || !directories.getFirst().clusterId().equals(fence.clusterId()) || !directories.getFirst().databaseId().equals(fence.databaseId())
                || !DomainBinding.canonical(directories.stream().map(DomainBinding::of).toList()).equals(fence.domains())) throw new IllegalArgumentException("Transaction requires a complete original writable snapshot projection");
        var captures=new HashMap<UUID,CapturedCut>();
        if (snapshot.participants().size()!=fence.participants().size()) throw new IllegalArgumentException("Incomplete original read projection");
        for (var progress:snapshot.participants()) {
            var route=progress.route(); var participant=fence.participant(route.groupId()); var request=progress.capture(); var token=progress.captureToken(); var handle=progress.certifiedHandle();
            if (request==null || token==null || handle==null || progress.captureLost() || progress.captureReleased() || progress.captureReleaseLost()
                    || progress.releaseReceipt()==null || progress.nativeGate()==null || !progress.releaseReceipt().equals(progress.nativeGate().released())
                    || !progress.nativeGate().fence().equals(fence) || !token.equals(home.progress(route.groupId()).capture())
                    || request.request().mode()!=RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED || !request.request().fence().equals(fence)
                    || !request.request().owner().equals(token.ownerIncarnation()) || !request.request().snapshotId().equals(token.snapshotId())
                    || !handle.owner().equals(token.ownerIncarnation()) || !handle.position().equals(token.position())) throw new IllegalArgumentException("Writable participant lacks its original released-gate capture and handle");
            var directory=RatisSnapshotPlan.group(directories,route.groupId());var current=directory.ranges().stream().filter(r -> r.groupId().equals(route.groupId())).findFirst().orElseThrow();
            var target=request.target().descriptor();
            if (!route.equals(current) || !target.groupId().equals(route.groupId()) || !target.rangeId().equals(route.rangeId()) || target.rangeEpoch()!=route.rangeEpoch()
                    || !Arrays.equals(target.lowerInclusive(),route.lowerInclusive()) || !Arrays.equals(target.upperExclusive(),route.upperExclusive())
                    || !route.rangeId().equals(participant.rangeId()) || route.rangeEpoch()!=participant.rangeEpoch()
                    || participant.scope().participants(directory,SnapshotFence.MAX_PARTICIPANTS).size()!=1) throw new IllegalArgumentException("Original read projection changed route");
            if (captures.putIfAbsent(route.groupId(),new CapturedCut(target,participant.scope(),handle))!=null) throw new IllegalArgumentException("Duplicate captured participant");
        }
        return create(directories,operation,mutations,predicates,captures,-1);
    }
    private static RatisTransactionPlan create(List<RangeDirectory> directories,OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates,Map<UUID,CapturedCut> captures,long acknowledgedThrough) {
        Objects.requireNonNull(directories); Objects.requireNonNull(operation); mutations=List.copyOf(mutations); predicates=List.copyOf(predicates);
        if (captures.isEmpty() && mutations.isEmpty() && predicates.isEmpty() || (long)mutations.size()+predicates.size()>DistributedCommandCodec.MAX_ITEMS) throw new IllegalArgumentException("Invalid global transaction input count");
        var groups=new TreeMap<UUID,Builder>(RangeDirectoryCodec.UUID_ORDER); int returned=0; long[] bodyBytes={0};
        for (var cut:captures.values()) group(groups,cut.route());
        for (var mutation:mutations) {
            var directory=RatisSnapshotPlan.column(directories,mutation.columnId());
            if (mutation.kind()==Mutation.Kind.DELETE_RANGE) {
                for (var route:directory.intersect(mutation.columnId(),mutation.key(),mutation.value(),false)) {
                    byte[] lower=maximum(mutation.key(),route.lowerInclusive()),upper=minimum(mutation.value(),route.upperExclusive());
                    if (Arrays.compareUnsigned(lower,upper)<0) addMutation(group(groups,route),new Mutation(Mutation.Kind.DELETE_RANGE,mutation.columnId(),lower,upper),bodyBytes);
                }
            } else {
                var group=group(groups,directory.route(mutation.columnId(),mutation.key())); addMutation(group,mutation,bodyBytes);
                if (mutation.returnMode()!=Mutation.ReturnMode.NONE) group.returns.add(returned++);
            }
        }
        for (var predicate:predicates) {
            var directory=RatisSnapshotPlan.column(directories,predicate.columnId());
            byte[] expected=predicate.expectedValue(); bodyBytes[0]+=26L+predicate.key().length+(expected==null ? 0 : expected.length);
            if (predicate.bucket()) for (var member:predicate.memberKeys()) bodyBytes[0]+=4L+member.length;
            requireBytes(bodyBytes[0]); group(groups,directory.route(predicate.columnId(),predicate.key())).predicates.add(predicate);
        }
        var partitions=new ArrayList<Partition>(); long bytes=0;
        if (!captures.isEmpty() && !groups.keySet().equals(captures.keySet())) throw new IllegalArgumentException("Writes or predicates extend beyond the original read projection");
        for (var group:groups.values()) {
            var directory=RatisSnapshotPlan.group(directories,group.route.groupId());
            var cut=captures.get(group.route.groupId());
            var command=cut!=null && group.mutations.isEmpty() && group.predicates.isEmpty()
                    ? DistributedCommand.readCertification(group.route.groupId(),group.route.rangeEpoch(),directory.schemaDigest(),operation,
                            new TransactionAcceptance(cut.handle().position(),cut.handle().generation(),cut.handle().owner(),cut.scope(),new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()))
                    : new DistributedCommand(group.route.groupId(),group.route.rangeEpoch(),directory.schemaDigest(),operation,group.mutations,group.predicates,acknowledgedThrough);
            bytes+=DistributedCommandCodec.encodedSize(command); if (bytes>MAX_BYTES) throw new IllegalArgumentException("Global transaction exceeds byte budget");
            var spans=new ArrayList<TransactionScope.Span>();
            for (var mutation:group.mutations) spans.add(mutation.kind()==Mutation.Kind.DELETE_RANGE ? TransactionScope.Span.range(mutation.columnId(),mutation.key(),mutation.value()) : TransactionScope.Span.point(mutation.columnId(),mutation.key()));
            for (var predicate:group.predicates) spans.add(TransactionScope.Span.point(predicate.columnId(),predicate.key()));
            var scope=cut==null ? new TransactionScope(spans) : cut.scope();
            for (var span:spans) if (!(span.isPoint() ? scope.contains(span.columnId(),span.lowerInclusive()) : scope.containsRange(span.columnId(),span.lowerInclusive(),span.upperExclusive()))) throw new IllegalArgumentException("Writes or predicates extend beyond the original captured scope");
            partitions.add(new Partition(group.route,command,scope,group.returns));
        }
        return new RatisTransactionPlan(directories,operation,partitions,captures);
    }
    private static void requireBytes(long bytes) { if (bytes>MAX_BYTES) throw new IllegalArgumentException("Global transaction exceeds byte budget"); }
    private static void addMutation(Builder group,Mutation mutation,long[] bytes) {
        byte[] value=mutation.value(); bytes[0]+=26L+mutation.key().length+(value==null ? 0 : value.length);
        if (mutation.bucket()) for (var member:mutation.memberKeys()) bytes[0]+=4L+member.length;
        requireBytes(bytes[0]); group.mutations.add(mutation);
    }
    private static Builder group(Map<UUID,Builder> groups,RangeDescriptor route) {
        if (route.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Inactive transaction participant");
        if (!groups.containsKey(route.groupId()) && groups.size()>=TransactionStatus.MAX_PARTICIPANTS) throw new IllegalArgumentException("Too many transaction participants");
        return groups.computeIfAbsent(route.groupId(),ignored -> new Builder(route));
    }
    private static byte[] maximum(byte[] value,byte[] bound) { return bound==null || Arrays.compareUnsigned(value,bound)>=0 ? value : bound; }
    private static byte[] minimum(byte[] value,byte[] bound) { return bound==null || Arrays.compareUnsigned(value,bound)<=0 ? value : bound; }
    /** Server acceptance must preserve each original partition exactly; all result mappings enter the home definition digest. */
    public Accepted accept(UUID transaction,Map<UUID,DistributedCommand> accepted,Map<UUID,OperationId> prepares,Map<UUID,OperationId> finalizes) {
        return accept(transaction,accepted,prepares,finalizes,Map.of(),false);
    }
    /** Freeze every original remote server capture after acceptance; no new capture is acquired here. */
    public Accepted accept(UUID transaction,Map<UUID,DistributedCommand> accepted,Map<UUID,OperationId> prepares,Map<UUID,OperationId> finalizes,Map<UUID,UUID> captureIds) {
        return accept(transaction,accepted,prepares,finalizes,Map.copyOf(captureIds),true);
    }
    public Accepted acceptCaptured(UUID transaction,Map<UUID,DistributedCommand> accepted,Map<UUID,OperationId> prepares,Map<UUID,OperationId> finalizes) {
        if (captures.isEmpty()) throw new IllegalStateException("Plan has no original captured projection");
        var ids=new HashMap<UUID,UUID>(); captures.forEach((group,cut) -> ids.put(group,cut.handle().id()));
        return accept(transaction,accepted,prepares,finalizes,ids,true);
    }
    private Accepted accept(UUID transaction,Map<UUID,DistributedCommand> accepted,Map<UUID,OperationId> prepares,Map<UUID,OperationId> finalizes,Map<UUID,UUID> captureIds,boolean remote) {
        Objects.requireNonNull(transaction); var groups=new HashSet<UUID>(); partitions.forEach(p -> groups.add(p.route().groupId()));
        if (!captures.isEmpty() && (!remote || captures.entrySet().stream().anyMatch(e -> !e.getValue().handle().id().equals(captureIds.get(e.getKey()))))) throw new IllegalArgumentException("Acceptance must retain every original captured handle");
        if (remote && !groups.equals(captureIds.keySet())) throw new IllegalArgumentException("Incomplete original remote capture set");
        if (!groups.equals(accepted.keySet()) || !groups.equals(prepares.keySet()) || !groups.equals(finalizes.keySet())) throw new IllegalArgumentException("Incomplete accepted transaction/control set");
        var intents=new ArrayList<RatisDurableTransaction.Intent>(); var participants=new ArrayList<TransactionStatus.Participant>();
        long acceptedBytes=0;
        for (var partition:partitions) {
            var route=partition.route(); var plan=Objects.requireNonNull(accepted.get(route.groupId()));
            var expected=partition.command().readCertification() ? partition.command() : plan.acceptance()==null ? null : DistributedCommand.acceptedTransaction(partition.command(),plan.acceptance());
            if (expected==null || !Arrays.equals(DistributedCommandCodec.encode(plan),DistributedCommandCodec.encode(expected))) throw new IllegalArgumentException("Accepted transaction partition changed original business plan");
            var cut=captures.get(route.groupId());
            if (cut!=null) {
                var audit=plan.acceptance();
                if (!audit.snapshot().equals(cut.handle().position()) || audit.leaderGeneration()!=cut.handle().generation() || !audit.ownerIncarnation().equals(cut.handle().owner())
                        || !Arrays.equals(ScopedTransactionRpc.scopeBytes(audit.scope()),ScopedTransactionRpc.scopeBytes(cut.scope()))) throw new IllegalArgumentException("Acceptance changed the original complete read cut");
                acceptedBytes+=DistributedCommandCodec.encodedSize(plan); requireBytes(acceptedBytes);
            }
            if (!partition.scope().spans().stream().allMatch(span -> span.isPoint() ? plan.acceptance().scope().contains(span.columnId(),span.lowerInclusive()) : plan.acceptance().scope().containsRange(span.columnId(),span.lowerInclusive(),span.upperExclusive()))) throw new IllegalArgumentException("Accepted snapshot omitted transaction inputs");
            intents.add(new RatisDurableTransaction.Intent(route,plan,prepares.get(route.groupId()),finalizes.get(route.groupId()),partition.resultPositions(),captureIds.get(route.groupId())));
            var directory=RatisSnapshotPlan.group(directories,route.groupId());
            participants.add(new TransactionStatus.Participant(directory.domainId(), route.rangeId(), route.groupId(), route.rangeEpoch(), directory.schemaDigest(), plan.payloadDigest(), null, false));
        }
        var namespace=directories.getFirst();var used=participants.stream().map(TransactionStatus.Participant::domainId).collect(java.util.stream.Collectors.toSet());
        var initial=TransactionStatus.begin(namespace.clusterId(),namespace.databaseId(),transaction,operation,DomainBinding.canonical(directories.stream().filter(d->used.contains(d.domainId())).map(DomainBinding::of).toList()),RatisDurableTransaction.planDigest(intents),participants);
        return new Accepted(initial,intents);
    }
}
