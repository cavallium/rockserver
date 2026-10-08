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
    private final RangeDirectory directory;
    private final OperationId operation;
    private final List<Partition> partitions;
    private RatisTransactionPlan(RangeDirectory directory,OperationId operation,List<Partition> partitions) { this.directory=directory; this.operation=operation; this.partitions=List.copyOf(partitions); }
    public RangeDirectory directory() { return directory; }
    public OperationId operation() { return operation; }
    public List<Partition> partitions() { return partitions; }
    public static RatisTransactionPlan logical(DistributedSchema schema,RangeDirectory directory,OperationId operation,
            List<RatisLogicalClient.Change> changes,List<RatisLogicalClient.Condition> conditions) {
        schema.requireMatches(directory); var plan=RatisLogicalClient.plan(schema,changes,conditions,MAX_BYTES);
        return create(directory,operation,plan.mutations(),plan.predicates());
    }
    public static RatisTransactionPlan create(RangeDirectory directory,OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates) {
        Objects.requireNonNull(directory); Objects.requireNonNull(operation); mutations=List.copyOf(mutations); predicates=List.copyOf(predicates);
        if (mutations.isEmpty() && predicates.isEmpty() || (long)mutations.size()+predicates.size()>DistributedCommandCodec.MAX_ITEMS) throw new IllegalArgumentException("Invalid global transaction input count");
        var groups=new TreeMap<UUID,Builder>(); int returned=0; long[] bodyBytes={0};
        for (var mutation:mutations) {
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
            byte[] expected=predicate.expectedValue(); bodyBytes[0]+=26L+predicate.key().length+(expected==null ? 0 : expected.length);
            if (predicate.bucket()) for (var member:predicate.memberKeys()) bodyBytes[0]+=4L+member.length;
            requireBytes(bodyBytes[0]); group(groups,directory.route(predicate.columnId(),predicate.key())).predicates.add(predicate);
        }
        var partitions=new ArrayList<Partition>(); long bytes=0;
        for (var group:groups.values()) {
            var command=new DistributedCommand(group.route.groupId(),group.route.rangeEpoch(),directory.schemaDigest(),operation,group.mutations,group.predicates);
            bytes+=DistributedCommandCodec.encodedSize(command); if (bytes>MAX_BYTES) throw new IllegalArgumentException("Global transaction exceeds byte budget");
            var spans=new ArrayList<TransactionScope.Span>();
            for (var mutation:group.mutations) spans.add(mutation.kind()==Mutation.Kind.DELETE_RANGE ? TransactionScope.Span.range(mutation.columnId(),mutation.key(),mutation.value()) : TransactionScope.Span.point(mutation.columnId(),mutation.key()));
            for (var predicate:group.predicates) spans.add(TransactionScope.Span.point(predicate.columnId(),predicate.key()));
            partitions.add(new Partition(group.route,command,new TransactionScope(spans),group.returns));
        }
        return new RatisTransactionPlan(directory,operation,partitions);
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
        Objects.requireNonNull(transaction); var groups=new HashSet<UUID>(); partitions.forEach(p -> groups.add(p.route().groupId()));
        if (!groups.equals(accepted.keySet()) || !groups.equals(prepares.keySet()) || !groups.equals(finalizes.keySet())) throw new IllegalArgumentException("Incomplete accepted transaction/control set");
        var intents=new ArrayList<RatisDurableTransaction.Intent>(); var participants=new ArrayList<TransactionStatus.Participant>();
        for (var partition:partitions) {
            var route=partition.route(); var plan=Objects.requireNonNull(accepted.get(route.groupId()));
            if (plan.acceptance()==null || !Arrays.equals(DistributedCommandCodec.encode(plan),DistributedCommandCodec.encode(DistributedCommand.acceptedTransaction(partition.command(),plan.acceptance())))) throw new IllegalArgumentException("Accepted transaction partition changed original business plan");
            if (!partition.scope().spans().stream().allMatch(span -> span.isPoint() ? plan.acceptance().scope().contains(span.columnId(),span.lowerInclusive()) : plan.acceptance().scope().containsRange(span.columnId(),span.lowerInclusive(),span.upperExclusive()))) throw new IllegalArgumentException("Accepted snapshot omitted transaction inputs");
            intents.add(new RatisDurableTransaction.Intent(route,plan,prepares.get(route.groupId()),finalizes.get(route.groupId()),partition.resultPositions()));
            participants.add(new TransactionStatus.Participant(route.rangeId(),route.groupId(),route.rangeEpoch(),directory.schemaDigest(),plan.payloadDigest(),null,false));
        }
        var initial=TransactionStatus.begin(directory.clusterId(),directory.databaseId(),directory.domainId(),transaction,operation,directory.schemaEpoch(),directory.schemaDigest(),RatisDurableTransaction.planDigest(intents),participants);
        return new Accepted(initial,intents);
    }
}
