package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Actual HOME/participant reads qualify fresh source records; replay never calls these readers. */
final class RatisSourceOutcomeAdmission {
    interface Authority {
        TransactionStatus home(HomeIdentity original)throws IOException;
        ParticipantOutcome outcome(RangeDirectory directory,RangeDescriptor route,UUID transaction)throws IOException;
        ParticipantFenceProof fence(RangeDirectory directory,RangeDescriptor route,ParticipantIdentity identity)throws IOException;
    }
    private final Authority authority;
    RatisSourceOutcomeAdmission(Authority authority){this.authority=Objects.requireNonNull(authority);}
    RatisSourceOutcomeRecord qualify(RatisSourceOutcomeRpc.Proposal proposal)throws IOException {
        try {
            var request=proposal.request();var original=request.original();var reserved=request.home().definition();var target=request.targetDirectory();var origin=request.origin();
            if(!Arrays.equals(origin.planDigest(),RatisOperationRerouteAdmissionRpc.normalizedDigest(original,target)) || origin.targetParticipantCount()!=RatisOperationRerouteAdmissionRpc.targetParticipantCount(original,target))throw new IOException("Source credited normalized input changed");
            var home=authority.home(request.home());if(home==null || !origin.equals(home.origin()) || !home.operation().equals(original.operationId()) || !home.transactionId().equals(reserved.transactionId()) || !home.clusterId().equals(reserved.clusterId()) || !home.databaseId().equals(reserved.databaseId()) || !home.domains().equals(reserved.domains()))throw new IOException("Source proof changed original forced HOME identity");
            if(proposal.kind()==RatisSourceOutcomeRecord.Kind.CREDIT){if(!Arrays.equals(home.encode(),reserved.encode()))throw new IOException("Source credit requires exact current HOME RESERVED before captures");return new RatisSourceOutcomeRecord(proposal.kind(),request,null);}
            if(!home.terminal() || home.participants().stream().anyMatch(p->!p.finalized()))throw new IOException("Source publication needs fully finalized terminal HOME");
            if(home.participants().isEmpty()){
                if(home.phase()!=TransactionStatus.Phase.ABORT || !Arrays.equals(home.asInitial().encode(),reserved.encode()))throw new IOException("Empty source terminal must retain original reservation");
                if(!proposal.intents().isEmpty())projection(request,proposal.intents());
                return new RatisSourceOutcomeRecord(proposal.kind(),request,home.terminalBusinessResult().encode());
            }
            if(original.kind()!=DistributedCommand.Kind.DATA)throw new IOException("Original accepted certification cannot acquire replacement participant cuts");
            var projection=projection(request,proposal.intents());var initial=reserved.promote(projection.initial());if(!Arrays.equals(initial.encode(),home.asInitial().encode()))throw new IOException("Source terminal HOME changed exact accepted projection");
            int returns=(int)original.mutations().stream().filter(m->m.returnMode()!=Mutation.ReturnMode.NONE).count();byte[][] values=new byte[returns][];boolean[] seen=new boolean[returns];
            for(var intent:projection.intents()){
                var outcome=authority.outcome(target,intent.route(),home.transactionId());var identity=new ParticipantIdentity(request.home().groupId(),request.home().rangeEpoch(),intent.route().groupId(),initial);
                if(outcome==null){if(home.phase()==TransactionStatus.Phase.COMMIT)throw new IOException("COMMIT source result requires original retained native outcome");var fence=authority.fence(target,intent.route(),identity);if(fence==null || !fence.identity().equals(identity))throw new IOException("ABORT participant has no exact native outcome/fence");continue;}
                var decision=outcome.finalization().decision();var participant=outcome.finalization().participant();var expected=home.participants().stream().filter(p->p.groupId().equals(intent.route().groupId())).findFirst().orElseThrow(()->new IOException("Source HOME participant missing"));
                if(!Arrays.equals(decision.asInitial().encode(),initial.encode()) || decision.phase()!=home.phase() || !participant.groupId().equals(expected.groupId()) || !Arrays.equals(participant.schemaDigest(),expected.schemaDigest()) || !participant.rangeId().equals(expected.rangeId()) || participant.rangeEpoch()!=expected.rangeEpoch() || !Arrays.equals(participant.planDigest(),intent.acceptedBusiness().payloadDigest()) || !Objects.equals(participant.prepared(),expected.prepared()) || !outcome.finalization().homeGroup().equals(request.home().groupId()) || outcome.finalization().homeEpoch()!=request.home().rangeEpoch())throw new IOException("Source participant outcome changed terminal HOME/native vote");
                if(home.phase()==TransactionStatus.Phase.COMMIT){if(outcome.vote()==null || !outcome.vote().prepared() || !outcome.vote().position().equals(expected.prepared()) || !Arrays.equals(outcome.result(),intent.acceptedBusiness().acceptance().result()))throw new IOException("Source committed result differs from native accepted result");var result=CommandResult.decode(outcome.result());if(result.valueCount()!=intent.resultPositions().size())throw new IOException("Source result projection changed slot count");for(int i=0;i<result.valueCount();i++){int slot=intent.resultPositions().get(i);if(slot>=returns || seen[slot])throw new IOException("Source result projection duplicates/escapes global order");seen[slot]=true;values[slot]=result.value(i);}}
            }
            byte[] result;if(home.phase()==TransactionStatus.Phase.ABORT)result=new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode();else{for(boolean present:seen)if(!present)throw new IOException("Source result omits original global return slot");result=new CommandResult(CommandResult.Status.SUCCESS,Arrays.asList(values)).encode();}
            // Read HOME again after participant RPCs; an authority change cannot be hidden by caller evidence.
            var after=authority.home(request.home());if(after==null || !Arrays.equals(home.encode(),after.encode()))throw new IOException("Source terminal HOME changed during proof");return new RatisSourceOutcomeRecord(proposal.kind(),request,result);
        }catch(IllegalArgumentException|NullPointerException invalid){throw new IOException("Invalid source publication proof",invalid);}
    }
    private static RatisTransactionPlan.Accepted projection(NativeSourceOutcomeCredit.Request request,List<RatisDurableTransaction.Intent> intents)throws IOException {
        if(request.original().kind()!=DistributedCommand.Kind.DATA)throw new IOException("Original accepted certification cannot carry replacement captures");
        var plan=RatisTransactionPlan.fromOriginal(request.targetDirectory(),request.original());var byGroup=new HashMap<UUID,RatisDurableTransaction.Intent>();long bytes=0;
        for(var intent:intents){bytes+=DistributedCommandCodec.encodedSize(intent.acceptedBusiness());if(bytes>RatisTransactionPlan.MAX_BYTES || byGroup.putIfAbsent(intent.route().groupId(),intent)!=null || intent.captureId()==null)throw new IOException("Source accepted images exceed bound or duplicate original captures");}
        var accepted=new HashMap<UUID,DistributedCommand>();var captures=new HashMap<UUID,UUID>();var prepares=new HashMap<UUID,OperationId>();var finalizes=new HashMap<UUID,OperationId>();int n=0;
        for(var partition:plan.partitions()){UUID group=partition.route().groupId();var intent=byGroup.get(group);if(intent==null || !intent.route().equals(partition.route()) || !intent.resultPositions().equals(partition.resultPositions()) || !intent.prepareControl().equals(RatisSourceOutcomeRpc.control(request,n)) || !intent.finalizeControl().equals(RatisSourceOutcomeRpc.control(request,64+n)))throw new IOException("Source accepted image changed original route/results/controls");var business=intent.acceptedBusiness();if(business.acceptance()==null || !Arrays.equals(ScopedTransactionRpc.scopeBytes(business.acceptance().scope()),ScopedTransactionRpc.scopeBytes(partition.scope())))throw new IOException("Source accepted image changed exact original scope");accepted.put(group,business);captures.put(group,intent.captureId());prepares.put(group,intent.prepareControl());finalizes.put(group,intent.finalizeControl());n++;}
        if(byGroup.size()!=plan.partitions().size())throw new IOException("Source accepted image contains foreign group");return plan.accept(request.origin().transactionId(),accepted,prepares,finalizes,captures);
    }

}
