package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.util.*;

/** Native CAT provenance lookup. The caller supplies its fresh catalogue-owner read barrier. */
final class RatisRecoveryLineage {
    record Request(UUID domain,UUID range,UUID group,long epoch) {
        Request {Objects.requireNonNull(domain);Objects.requireNonNull(range);Objects.requireNonNull(group);if(epoch<0)throw new IllegalArgumentException("Negative recovery lineage epoch");}
    }
    sealed interface Origin permits Bootstrap,Birth {
        ApplyPosition catalogueCut();
        RangeDescriptor range();
    }
    /** Original provenance only, not current ownership/readiness; requires a trusted immutable bootstrap plan. */
    record Bootstrap(ApplyPosition catalogueCut,RangeDirectory directory,RangeDescriptor range) implements Origin {}
    record Birth(ApplyPosition catalogueCut,TopologyChange.Definition definition,RangeDescriptor range) implements Origin {
        List<RangeDescriptor> parents(){return definition.parents();}
    }
    @FunctionalInterface interface Cut {ApplyPosition read()throws IOException;}
    private final UUID cluster,database;
    private final Cut cut;
    private final RatisCdcLineageDirectory.States states;
    private final RatisCdcDeliveryAuthority.DefinitionReader definitions;

    RatisRecoveryLineage(UUID cluster,UUID database,UUID catalogue,ReplicatedRangeStore store,UUID column) {
        this(cluster,database,()->{
            if(!store.ownership().owner().groupId().equals(catalogue))throw new IOException("Recovery lineage reader crossed catalogue group");
            return store.applied();
        },()->RatisCdcLineageDirectory.readStates(store,column),header->RatisCdcLineageDirectory.readDefinition(store,column,header));
        Objects.requireNonNull(catalogue);Objects.requireNonNull(store);Objects.requireNonNull(column);
    }
    RatisRecoveryLineage(UUID cluster,UUID database,Cut cut,RatisCdcLineageDirectory.States states,RatisCdcDeliveryAuthority.DefinitionReader definitions) {
        this.cluster=Objects.requireNonNull(cluster);this.database=Objects.requireNonNull(database);this.cut=Objects.requireNonNull(cut);this.states=Objects.requireNonNull(states);this.definitions=Objects.requireNonNull(definitions);
    }
    record Lookup(Request request,ApplyPosition catalogueCut,Birth birth) {}
    /** Null bootstrapWitness means absence must fail. This witness is trusted plan input, never request-derived. */
    Origin resolve(Request request,ApplyPosition minimumCatalogueCut,RangeDirectory bootstrapWitness)throws IOException {
        var lookup=lookup(request,minimumCatalogueCut);
        if(lookup.birth()!=null){if(bootstrapWitness!=null)throw new IOException("Born recovery group cannot claim ordinary bootstrap provenance");return lookup.birth();}
        if(bootstrapWitness==null || !cluster.equals(bootstrapWitness.clusterId()) || !database.equals(bootstrapWitness.databaseId()) || !request.domain().equals(bootstrapWitness.domainId()))throw new IOException("Recovery lineage has no committed birth or explicit ordinary bootstrap witness");
        var target=bootstrapWitness.ranges().stream().filter(r->matches(r,request)).findFirst().orElseThrow(()->new IOException("Ordinary bootstrap witness differs from requested lineage"));
        if(target.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)throw new IOException("Ordinary bootstrap witness is not active initial placement");
        return new Bootstrap(lookup.catalogueCut(),bootstrapWitness,target);
    }
    /** Absence is evidence only at this stable completed cut; it grants no bootstrap authority. */
    Lookup lookup(Request request,ApplyPosition minimumCatalogueCut)throws IOException {
        Objects.requireNonNull(request);Objects.requireNonNull(minimumCatalogueCut);var before=requireCut();if(minimumCatalogueCut.term()<1 || !RatisRelocationOwnerRpc.covers(before,minimumCatalogueCut))throw new IOException("Recovery lineage does not cover required catalogue cut");var retained=states.read();
        if(retained.size()>RatisTopologyCatalogue.MAX_INTENTS)throw new IOException("Recovery lineage exceeds bounded catalogue history");
        long bytes=0;var identities=new HashSet<UUID>();TopologyChange.Header selected=null;
        for(var state:retained) {
            var header=state.header();bytes=Math.addExact(bytes,state.encode().length);
            if(bytes>RatisTopologyCatalogue.MAX_BYTES || !identities.add(header.intentId()))throw new IOException("Recovery lineage has over-budget or duplicate intent history");
            if(!cluster.equals(header.clusterId()) || !database.equals(header.databaseId()))throw new IOException("Recovery lineage catalogue namespace differs");
            if(header.kind()==TopologyChange.Kind.RELOCATE || state.phase()!=TopologyChange.Phase.PUBLISHED && state.phase()!=TopologyChange.Phase.ACTIVE && state.phase()!=TopologyChange.Phase.RETIRED)continue;
            var target=header.targets().stream().filter(p->p.groupId().equals(request.group())).findFirst().orElse(null);if(target==null)continue;
            if(!header.domainId().equals(request.domain()) || !target.rangeId().equals(request.range()) || target.rangeEpoch()!=request.epoch())throw new IOException("Recovery group has conflicting birth lineage");
            if(selected!=null)throw new IOException("Recovery group has multiple committed births");selected=header;
        }
        Birth birth=null;
        if(selected!=null) {
            var definition=definitions.read(selected);if(definition==null)throw new IOException("Recovery birth immutable definition is absent");
            try{selected.requireDefinition(definition);}catch(IllegalArgumentException changed){throw new IOException("Recovery birth immutable definition changed",changed);}
            var target=definition.targets().stream().filter(r->matches(r,request)).findFirst().orElseThrow(()->new IOException("Recovery birth target differs from requested lineage"));
            birth=new Birth(before,definition,target);
        }
        if(!before.equals(requireCut()))throw new IOException("Recovery lineage catalogue changed during lookup");return new Lookup(request,before,birth);
    }
    private ApplyPosition requireCut()throws IOException{var current=cut.read();if(current==null || current.term()<1)throw new IOException("Recovery lineage lacks completed catalogue cut");return current;}
    private static boolean matches(RangeDescriptor range,Request request){return range.rangeId().equals(request.range()) && range.groupId().equals(request.group()) && range.rangeEpoch()==request.epoch();}
}
