package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import it.cavallium.rockserver.core.common.cdc.DistributedCdcSubscriptions;
import it.cavallium.rockserver.core.common.cdc.DistributedCdcSubscriptions.Envelope;

/** One bounded, caller-acknowledged lineage batch. Original envelopes stay private to the journal/controller. */
public final class RatisCdcSubscriptionBatch {
    private final UUID id;
    private final DistributedCdcSubscription.Definition definition;
    private final RatisCdcPollRpc.Result result;
    public RatisCdcSubscriptionBatch(UUID id,DistributedCdcSubscription state,RatisCdcPollRpc.Result result) {
        this.id=Objects.requireNonNull(id); Objects.requireNonNull(state); this.result=Objects.requireNonNull(result); definition=state.definition();
        var request=result.request(); var binding=request.binding();
        if(state.phase()!=DistributedCdcSubscription.Phase.ACTIVE || result.outcome()!=RatisCdcPollRpc.Outcome.DATA
                || !definition.clusterId().equals(binding.clusterId()) || !definition.databaseId().equals(binding.databaseId()) || !definition.domainId().equals(binding.domainId())
                || !request.from().equals(state.checkpoint().cursors().get(binding.lineage()))) throw new IllegalArgumentException("CDC batch differs from active subscription/checkpoint");
    }
    /** Projection is built on demand so a durable pending batch does not retain per-event wrapper objects. */
    public List<Envelope> envelopes() {
        var request=result.request(); var selected=new HashSet<>(definition.columns()); var projected=new ArrayList<Envelope>();
        for(var record:result.records()) {
            var events=new ArrayList<DistributedCdcRecord.Event>();
            for(int i=0;i<record.command().mutations().size();i++) {
                var mutation=record.command().mutations().get(i); var cursor=new DistributedCdcCheckpoint.Cursor(record.position(),i);
                if(selected.contains(mutation.columnId()) && !request.from().covers(cursor)) events.add(new DistributedCdcRecord.Event(cursor,mutation));
            }
            if(!events.isEmpty()) projected.add(new Envelope(record.position(),record.command().schemaDigest(),record.command().operationId(),record.finalization(),events));
        }
        return List.copyOf(projected);
    }
    public DistributedCdcSubscriptions.Batch projected() { return new DistributedCdcSubscriptions.Batch(id,definition,lineage(),from(),next(),cut(),envelopes()); }
    public UUID id() { return id; }
    public DistributedCdcSubscription.Definition definition() { return definition; }
    public DistributedCdcCheckpoint.Lineage lineage() { return result.request().binding().lineage(); }
    public DistributedCdcCheckpoint.Cursor from() { return result.request().from(); }
    public DistributedCdcCheckpoint.Cursor next() { return result.next(); }
    public ApplyPosition cut() { return result.cut(); }
    RatisCdcPollRpc.Result result() { return result; }
}
