package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;

/** Fresh catalogue/source authority plus exact deterministic projection. Does not certify participant DATA. */
final class RatisOperationRerouteAdmission {
    interface Authority {
        TopologyChange.State intent(UUID id)throws IOException;
        TopologyChange.Definition definition(byte[] digest)throws IOException;
        RangeDirectory directory(UUID domain)throws IOException;
        RatisTopologyOperationOutcomeRpc.Reply outcome(RangeDirectory source,RangeDescriptor descriptor,DistributedCommand original)throws IOException;
    }
    private final Authority authority;
    private final UUID catalogueGroup;
    RatisOperationRerouteAdmission(Authority authority,UUID catalogueGroup){this.authority=Objects.requireNonNull(authority);this.catalogueGroup=Objects.requireNonNull(catalogueGroup);}
    void validate(DistributedCommand command,RatisOperationRerouteAdmissionRpc.Evidence evidence)throws IOException {
        if(evidence==null)throw new IOException("Reroute requires original immutable input evidence");
        try {
            if(RatisOperationRerouteAdmissionRpc.acknowledgementOnly(evidence.original()))throw new IOException("Original-parent ACK proof is required before reroute bookkeeping");
            var next=TransactionStatus.decode(command.mutations().getFirst().value());var origin=Objects.requireNonNull(next.origin());var original=evidence.original();var target=evidence.targetDirectory();var saved=evidence.absence();var header=saved.freeze().header();
            if(original.kind()!=DistributedCommand.Kind.DATA && original.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED || original.kind()==DistributedCommand.Kind.DATA && original.mutations().isEmpty() && original.predicates().isEmpty() && !RatisOperationRerouteAdmissionRpc.acknowledgementOnly(original)
                    || !original.operationId().equals(origin.operation()) || !original.groupId().equals(origin.source().groupId()) || original.rangeEpoch()!=origin.source().rangeEpoch()
                    || !Arrays.equals(original.receiptDigest(),origin.originalReceiptDigest()) || !Arrays.equals(original.schemaDigest(),next.schemaDigest()) || target.schemaEpoch()!=next.schemaEpoch() || !Arrays.equals(target.schemaDigest(),next.schemaDigest())
                    || !target.clusterId().equals(origin.clusterId()) || !target.databaseId().equals(origin.databaseId()) || !target.domainId().equals(origin.domainId()) || !Arrays.equals(target.payloadDigest(),origin.targetDirectoryDigest())
                    || !header.clusterId().equals(origin.clusterId()) || !header.databaseId().equals(origin.databaseId()) || !header.domainId().equals(origin.domainId()))throw new IOException("Reroute original input/namespace/schema changed");
            var state=authority.intent(header.intentId());var definition=authority.definition(header.definitionDigest());
            if(state==null || definition==null || !Arrays.equals(state.header().encode(),header.encode()) || state.phase()!=TopologyChange.Phase.ACTIVE && state.phase()!=TopologyChange.Phase.RETIRED)throw new IOException("Original topology has no committed activation authority");
            state.requireDefinition(definition);header.requireDefinition(definition);
            var source=definition.sourceDirectory();var descriptor=source.ranges().stream().filter(r->r.groupId().equals(origin.source().groupId()) && r.rangeId().equals(origin.source().rangeId()) && r.rangeEpoch()==origin.source().rangeEpoch()).findFirst().orElseThrow(()->new IOException("Original source absent from immutable topology"));
            if(!Arrays.equals(source.schemaDigest(),original.schemaDigest()) || source.schemaEpoch()!=next.schemaEpoch() || !sameNamespace(evidence.sourceDirectory(),source)
                    || evidence.sourceDirectory().ranges().stream().noneMatch(r->sameLineageBounds(r,descriptor)))throw new IOException("Original frozen descriptor/schema changed");
            requireOriginalOwnership(original,descriptor,source.columns());
            if(original.kind()==DistributedCommand.Kind.COMMIT_ACCEPTED){var participants=original.acceptance().scope().participants(source,1);if(participants.size()!=1 || !participants.getFirst().groupId().equals(descriptor.groupId()))throw new IOException("Original certification changed source projection");}
            else {var sourcePlan=RatisTransactionPlan.create(source,original.operationId(),original.mutations(),original.predicates());if(sourcePlan.partitions().size()!=1 || !sourcePlan.partitions().getFirst().route().groupId().equals(descriptor.groupId()))throw new IOException("Original input crosses frozen source");}
            saved.freeze().requireRequest(header,admissionDigest(header));
            var request=new RatisTopologyOperationOutcomeRpc.Request(origin.clusterId(),origin.databaseId(),origin.domainId(),origin.source(),origin.operation(),origin.originalReceiptDigest());
            requireAbsence(origin,request,saved);
            var currentTarget=authority.directory(origin.domainId());if(currentTarget==null || !Arrays.equals(RangeDirectoryCodec.encode(target),RangeDirectoryCodec.encode(currentTarget)))throw new IOException("Reroute target is no longer current placement");
            var fresh=authority.outcome(source,descriptor,original);requireAbsence(origin,request,fresh);
            if(!Arrays.equals(RatisOperationRerouteAdmissionRpc.normalizedDigest(original,target),origin.planDigest()) || RatisOperationRerouteAdmissionRpc.targetParticipantCount(original,target)!=origin.targetParticipantCount())throw new IOException("Reroute normalized plan/count changed");
            if(RatisTransactionStatuses.isReservation(command)){if(!evidence.intents().isEmpty())throw new IOException("Reservation cannot carry accepted target captures");return;}
            if(original.kind()!=DistributedCommand.Kind.DATA || RatisOperationRerouteAdmissionRpc.acknowledgementOnly(original))throw new IOException("Accepted original certification cannot be recaptured after topology change");
            var plan=RatisTransactionPlan.fromOriginal(target,original);
            byte[] expected=command.predicates().getFirst().expectedValue();var reserved=TransactionStatus.decode(expected);
            var credit=fresh.credit();
            var home=new HomeIdentity(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.mutations().getFirst().columnId(),reserved);
            var promised=new NativeSourceOutcomeCredit.Request(original,origin,target,fresh.freeze(),home);
            if(credit==null || !credit.request().equals(promised))throw new IOException("Promotion lacks its exact durable source receipt capacity");
            var accepted=new HashMap<UUID,DistributedCommand>();var captures=new HashMap<UUID,UUID>();var prepares=new HashMap<UUID,OperationId>();var finalizes=new HashMap<UUID,OperationId>();
            for(var intent:evidence.intents()) {
                UUID group=intent.route().groupId();if(accepted.putIfAbsent(group,intent.acceptedBusiness())!=null || intent.captureId()==null)throw new IOException("Duplicate/missing original accepted capture");captures.put(group,intent.captureId());
            }
            int n=0;for(var partition:plan.partitions()) {
                UUID group=partition.route().groupId();var business=accepted.get(group);if(business==null || business.acceptance()==null || !Arrays.equals(ScopedTransactionRpc.scopeBytes(business.acceptance().scope()),ScopedTransactionRpc.scopeBytes(partition.scope())))throw new IOException("Accepted capture changed exact normalized scope");
                prepares.put(group,RatisOperationRerouteAdmissionRpc.control(origin,n));finalizes.put(group,RatisOperationRerouteAdmissionRpc.control(origin,64+n++));
            }
            var reconstructed=plan.accept(origin.transactionId(),accepted,prepares,finalizes,captures);
            RatisDurableTransaction.validateResultPlan(reconstructed.intents());
            if(!Arrays.equals(reserved.promote(reconstructed.initial()).encode(),next.encode()) || !promotionControl(origin,command.operationId()))throw new IOException("Promotion changed exact accepted definition/control");
            // Native PREPARE subsequently checks original live captures/results before any participant effects.
        }catch(IllegalArgumentException|NullPointerException invalid){throw new IOException("Invalid reroute admission evidence",invalid);}
    }
    private static boolean sameLineageBounds(RangeDescriptor a,RangeDescriptor b){return a.groupId().equals(b.groupId()) && a.rangeId().equals(b.rangeId()) && a.rangeEpoch()==b.rangeEpoch() && Arrays.equals(a.lowerInclusive(),b.lowerInclusive()) && Arrays.equals(a.upperExclusive(),b.upperExclusive());}
    static void requireOriginalOwnership(DistributedCommand original,RangeDescriptor source,Set<UUID> columns)throws IOException {
        for(var mutation:original.mutations()){
            if(!columns.contains(mutation.columnId()) || !contains(source,mutation.key(),mutation.kind()!=Mutation.Kind.DELETE_RANGE) || mutation.kind()==Mutation.Kind.DELETE_RANGE && !contains(source,mutation.value(),false))throw new IOException("Original mutation extends beyond frozen source ownership");
        }
        for(var predicate:original.predicates())if(!columns.contains(predicate.columnId()) || !contains(source,predicate.key(),true))throw new IOException("Original predicate extends beyond frozen source ownership");
        if(original.acceptance()!=null)for(var span:original.acceptance().scope().spans())if(!columns.contains(span.columnId()) || !(span.lowerInclusive()==null ? source.lowerInclusive()==null : contains(source,span.lowerInclusive(),span.isPoint())) || !span.isPoint() && !(span.upperExclusive()==null ? source.upperExclusive()==null : contains(source,span.upperExclusive(),false)))throw new IOException("Original certification extends beyond frozen source ownership");
    }
    private static boolean contains(RangeDescriptor source,byte[] key,boolean point){byte[] lower=source.lowerInclusive(),upper=source.upperExclusive();if(key==null)return !point && (lower==null && upper==null);return (lower==null || Arrays.compareUnsigned(key,lower)>=0) && (upper==null || (point ? Arrays.compareUnsigned(key,upper)<0 : Arrays.compareUnsigned(key,upper)<=0));}
    private static boolean promotionControl(OperationRerouteOrigin origin,OperationId operation){var first=origin.firstControl();return operation.sessionId().equals(first.sessionId()) && operation.epoch()==first.epoch() && operation.sequence()>=first.sequence()+128 && operation.sequence()<=first.sequence()+255;}
    private static boolean sameNamespace(RangeDirectory a,RangeDirectory b){return a.clusterId().equals(b.clusterId()) && a.databaseId().equals(b.databaseId()) && a.domainId().equals(b.domainId()) && a.schemaEpoch()==b.schemaEpoch() && Arrays.equals(a.schemaDigest(),b.schemaDigest());}
    private static void requireAbsence(OperationRerouteOrigin origin,RatisTopologyOperationOutcomeRpc.Request request,RatisTopologyOperationOutcomeRpc.Reply reply)throws IOException {
        if(reply==null || reply.outcome()!=RatisTopologyOperationOutcomeRpc.Outcome.ABSENT || !request.equals(reply.request()) || !Arrays.equals(OperatorUpload.sha256(reply.freeze().encode()),origin.absenceCommitment()) || reply.cut().index()<origin.absenceCut().index() || reply.cut().term()<origin.absenceCut().term() || reply.cut().index()==origin.absenceCut().index() && reply.cut().term()!=origin.absenceCut().term())throw new IOException("Source lacks exact original authoritative absence");
    }
    private byte[] admissionDigest(TopologyChange.Header header){byte[] state=TopologyChange.State.begin(header).beginFencing().encode();return OperatorUpload.sha256(ByteBuffer.allocate(16+state.length).putLong(catalogueGroup.getMostSignificantBits()).putLong(catalogueGroup.getLeastSignificantBits()).put(state).array());}
}
