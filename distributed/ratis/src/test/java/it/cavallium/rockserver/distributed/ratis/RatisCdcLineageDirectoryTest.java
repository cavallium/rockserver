package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisCdcLineageDirectoryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void committedSplitAndMergeResolveParentAfterPublicationAndChildrenFromCurrentDirectory() throws Exception {
        for(boolean merge:List.of(false,true)) {
            var helper=new RatisCdcTopologyHandoffTest(); helper.root=root.resolve("case-"+merge);
            try(var f=helper.new Fixture(merge)) {
                f.topology.env.catalogue.store.enableCatalogueScopeCompaction();
                f.activate(); var topology=f.topology.definition;
                var state=f.topology.catalogue.readIntent(topology.intentId());
                var resolver=new RatisCdcLineageDirectory(domain->topology.targetDirectory(),()->
                        RatisCdcLineageDirectory.readStates(f.topology.env.catalogue.store,f.topology.plan.catalogue().directoryColumn()),header->
                        RatisCdcLineageDirectory.readDefinition(f.topology.env.catalogue.store,f.topology.plan.catalogue().directoryColumn(),header));
                for(var parent:topology.parents()) assertArrayEquals(topology.sourceDirectory().payloadDigest(),resolver.read(f.definition(),lineage(parent)).payloadDigest());
                for(var child:topology.targets()) assertArrayEquals(topology.targetDirectory().payloadDigest(),resolver.read(f.definition(),lineage(child)).payloadDigest());
                var parent=topology.parents().getFirst();
                assertThrows(IOException.class,()->resolver.read(f.definition(),new DistributedCdcCheckpoint.Lineage(parent.rangeId(),parent.groupId(),parent.rangeEpoch()+1)));
                var uncommitted=new RatisCdcLineageDirectory(domain->topology.targetDirectory(),()->List.of(TopologyChange.State.begin(topology.header())),header->topology);
                assertThrows(IOException.class,()->uncommitted.read(f.definition(),lineage(parent)));
                var missing=new RatisCdcLineageDirectory(domain->topology.targetDirectory(),()->List.of(state),header->null);
                assertThrows(IOException.class,()->missing.read(f.definition(),lineage(parent)));
                var overflow=new RatisCdcLineageDirectory(domain->null,()->Collections.nCopies(RatisTopologyCatalogue.MAX_INTENTS+1,state),header->topology);
                assertThrows(IOException.class,()->overflow.read(f.definition(),lineage(parent)));
            }
        }
    }
    private static DistributedCdcCheckpoint.Lineage lineage(RangeDescriptor range) {
        return new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch());
    }
}
