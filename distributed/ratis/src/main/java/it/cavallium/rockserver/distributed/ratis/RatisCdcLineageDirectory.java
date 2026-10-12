package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.util.*;

/** Resolves retired CDC placement from bounded committed topology state, never caller supplied routes. */
final class RatisCdcLineageDirectory implements RatisCdcDeliveryAuthority.LineageReader {
    @FunctionalInterface interface States { List<TopologyChange.State> read() throws IOException; }
    private final RatisCdcDeliveryAuthority.DirectoryReader directories;
    private final States states;
    private final RatisCdcDeliveryAuthority.DefinitionReader definitions;

    RatisCdcLineageDirectory(RatisCdcDeliveryAuthority.DirectoryReader directories,States states,
                            RatisCdcDeliveryAuthority.DefinitionReader definitions) {
        this.directories=Objects.requireNonNull(directories);
        this.states=Objects.requireNonNull(states);
        this.definitions=Objects.requireNonNull(definitions);
    }

    @Override public RangeDirectory read(DistributedCdcSubscription.Definition subscription,
                                       DistributedCdcCheckpoint.Lineage lineage) throws IOException {
        var current=directories.read(subscription.domainId());
        if(matches(current,subscription,lineage)) return current;
        var retained=states.read();
        if(retained.size()>RatisTopologyCatalogue.MAX_INTENTS) throw new IOException("CDC topology history exceeds committed capacity");
        TopologyChange.Header selected=null; boolean source=false; long generation=-1;
        for(var state:retained) {
            var header=state.header();
            if(!header.clusterId().equals(subscription.clusterId()) || !header.databaseId().equals(subscription.databaseId())
                    || !header.domainId().equals(subscription.domainId())) continue;
            boolean published=state.phase()==TopologyChange.Phase.PUBLISHED || state.phase()==TopologyChange.Phase.ACTIVE || state.phase()==TopologyChange.Phase.RETIRED;
            if(!published) continue;
            boolean parent=header.parents().stream().anyMatch(p->matches(p,lineage));
            boolean child=header.targets().stream().anyMatch(p->matches(p,lineage));
            if(!parent && (!child || state.phase()==TopologyChange.Phase.PUBLISHED)) continue;
            long candidate=parent ? header.sourceGeneration() : header.targetGeneration();
            if(candidate>generation) { selected=header; source=parent; generation=candidate; }
            else if(candidate==generation && selected!=null) {
                if(!Arrays.equals(source ? selected.sourceDirectoryDigest() : selected.targetDirectoryDigest(),parent ? header.sourceDirectoryDigest() : header.targetDirectoryDigest()))
                    throw new IOException("CDC lineage has conflicting committed topology placement");
                if(parent && !source) { selected=header; source=true; }
            }
        }
        if(selected==null) throw new IOException("CDC lineage has no committed current or historical placement");
        var definition=definitions.read(selected);
        if(definition==null) throw new IOException("CDC historical topology definition is absent");
        try { selected.requireDefinition(definition); }
        catch(IllegalArgumentException invalid) { throw new IOException("CDC historical topology definition changed",invalid); }
        var directory=source ? definition.sourceDirectory() : definition.targetDirectory();
        if(!matches(directory,subscription,lineage)) throw new IOException("CDC historical placement differs from subscription lineage");
        return directory;
    }

    private static boolean matches(TopologyChange.Participant participant,DistributedCdcCheckpoint.Lineage lineage) {
        return participant.rangeId().equals(lineage.rangeId()) && participant.groupId().equals(lineage.groupId()) && participant.rangeEpoch()==lineage.rangeEpoch();
    }
    private static boolean matches(RangeDirectory directory,DistributedCdcSubscription.Definition subscription,DistributedCdcCheckpoint.Lineage lineage) {
        return directory!=null && directory.clusterId().equals(subscription.clusterId()) && directory.databaseId().equals(subscription.databaseId())
                && directory.domainId().equals(subscription.domainId()) && directory.columns().containsAll(subscription.columns())
                && directory.ranges().stream().anyMatch(r->r.rangeId().equals(lineage.rangeId()) && r.groupId().equals(lineage.groupId()) && r.rangeEpoch()==lineage.rangeEpoch());
    }

    static TopologyChange.Definition readDefinition(ReplicatedRangeStore store,UUID column,TopologyChange.Header header) throws IOException {
        try {
            var points=store.readSchemaAuthorityPoints(List.of(
                    new ReplicatedRangeStore.PhysicalPoint(column,RatisTopologyCatalogue.definitionKey(header.definitionDigest())),
                    new ReplicatedRangeStore.PhysicalPoint(column,RatisTopologyCatalogue.identityKey(header.intentId()))),
                    new int[]{TopologyChange.MAX_DEFINITION_BYTES,32});
            var values=points.values();
            if(points.position()==null || points.position().term()<1 || values.getFirst()==null || !Arrays.equals(values.get(1),header.definitionDigest()))
                throw new IOException("CDC topology lacks committed immutable definition identity");
            var definition=TopologyChange.Definition.decode(values.getFirst()); header.requireDefinition(definition); return definition;
        } catch(org.rocksdb.RocksDBException | IllegalArgumentException | IllegalStateException invalid) {
            throw new IOException("CDC topology immutable definition read failed",invalid);
        }
    }

    static List<TopologyChange.State> readStates(ReplicatedRangeStore store,UUID column) throws IOException {
        try {
            var scan=store.readCurrentMetadataPrefix(column,RatisTopologyCatalogue.statePrefix(),17,RatisTopologyCatalogue.MAX_INTENTS,
                    RatisTopologyCatalogue.MAX_INTENTS*(17+TopologyChange.MAX_STATUS_BYTES));
            if(scan.position()==null || scan.position().term()<1) throw new IOException("CDC topology history lacks completed catalogue cut");
            var states=new ArrayList<TopologyChange.State>(scan.entries().size());
            for(var entry:scan.entries()) {
                var state=TopologyChange.State.decode(entry.value());
                if(!Arrays.equals(entry.key(),RatisTopologyCatalogue.stateKey(state.header().intentId())))
                    throw new IOException("CDC retained topology state identity differs from its key");
                states.add(state);
            }
            return List.copyOf(states);
        } catch(org.rocksdb.RocksDBException | IllegalArgumentException | IllegalStateException invalid) {
            throw new IOException("CDC committed topology history read failed",invalid);
        }
    }
}
