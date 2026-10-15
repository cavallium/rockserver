package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisRecoveryLineageTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void actualCatalogueResolvesOriginalSplitAndBothMergeParentsAndExplicitBootstrap()throws Exception {
        for(boolean merge:List.of(false,true)) {
            var helper=new RatisCdcTopologyHandoffTest();helper.root=root.resolve("case-"+merge);
            try(var fixture=helper.new Fixture(merge)) {
                var store=fixture.topology.env.catalogue.store;store.enableCatalogueScopeCompaction();fixture.activate();
                var definition=fixture.topology.definition;var source=definition.sourceDirectory();var binding=fixture.topology.plan.catalogue();
                var resolver=new RatisRecoveryLineage(source.clusterId(),source.databaseId(),binding.groupId(),store,binding.directoryColumn());var cut=store.applied();
                for(var target:definition.targets()) {
                    var birth=assertInstanceOf(RatisRecoveryLineage.Birth.class,resolver.resolve(request(source,target),cut,null));
                    assertArrayEquals(definition.encode(),birth.definition().encode());assertEquals(definition.parents(),birth.parents());assertEquals(merge?2:1,birth.parents().size());assertEquals(cut,birth.catalogueCut());assertEquals(target,birth.range());
                    assertThrows(IOException.class,()->resolver.resolve(request(source,target),cut,definition.targetDirectory()),"A current born directory cannot be a bootstrap witness");
                }
                var parent=definition.parents().getFirst();var bootstrap=assertInstanceOf(RatisRecoveryLineage.Bootstrap.class,resolver.resolve(request(source,parent),cut,source));assertEquals(parent,bootstrap.range());assertArrayEquals(source.payloadDigest(),bootstrap.directory().payloadDigest());
                assertThrows(IOException.class,()->resolver.resolve(request(source,parent),cut,null),"Absence of birth is not bootstrap authority");
                assertThrows(IOException.class,()->resolver.resolve(new RatisRecoveryLineage.Request(source.domainId(),parent.rangeId(),parent.groupId(),parent.rangeEpoch()+1),cut,source));
                assertThrows(IOException.class,()->resolver.resolve(request(source,parent),new ApplyPosition(cut.term(),cut.index()+1),source));
                assertThrows(IOException.class,()->new RatisRecoveryLineage(source.clusterId(),source.databaseId(),new UUID(979,1),store,binding.directoryColumn()).resolve(request(source,parent),cut,source));
                rejectsInvalidRetainedEvidence(definition,fixture.topology.catalogue.readIntent(definition.intentId()),cut);
            }
        }
    }
    private static void rejectsInvalidRetainedEvidence(TopologyChange.Definition definition,TopologyChange.State state,ApplyPosition cut)throws Exception {
        var source=definition.sourceDirectory();var target=definition.targets().getFirst();var request=request(source,target);
        var missing=new RatisRecoveryLineage(source.clusterId(),source.databaseId(),()->cut,()->List.of(state),header->null);assertThrows(IOException.class,()->missing.resolve(request,cut,null));
        var uncommitted=new RatisRecoveryLineage(source.clusterId(),source.databaseId(),()->cut,()->List.of(TopologyChange.State.begin(definition.header())),header->definition);assertThrows(IOException.class,()->uncommitted.resolve(request,cut,null));
        var overflow=new RatisRecoveryLineage(source.clusterId(),source.databaseId(),()->cut,()->Collections.nCopies(RatisTopologyCatalogue.MAX_INTENTS+1,state),header->definition);assertThrows(IOException.class,()->overflow.resolve(request,cut,null));
        var duplicate=new RatisRecoveryLineage(source.clusterId(),source.databaseId(),()->cut,()->List.of(state,state),header->definition);assertThrows(IOException.class,()->duplicate.resolve(request,cut,null));
        var wrongNamespace=new RatisRecoveryLineage(new UUID(979,2),source.databaseId(),()->cut,()->List.of(state),header->definition);assertThrows(IOException.class,()->wrongNamespace.resolve(request,cut,null));
        var wrongIdentity=new RatisRecoveryLineage(source.clusterId(),source.databaseId(),()->cut,()->List.of(state),header->definition);assertThrows(IOException.class,()->wrongIdentity.resolve(new RatisRecoveryLineage.Request(source.domainId(),new UUID(979,3),target.groupId(),target.rangeEpoch()),cut,null));
        var reads=new AtomicInteger();var changedCut=new RatisRecoveryLineage(source.clusterId(),source.databaseId(),()->reads.getAndIncrement()==0?cut:new ApplyPosition(cut.term(),cut.index()+1),()->List.of(state),header->definition);assertThrows(IOException.class,()->changedCut.resolve(request,cut,null));
        var changedDefinition=new TopologyChange.Definition(new UUID(979,4),definition.sourceDirectory(),definition.targetDirectory());
        var changed=new RatisRecoveryLineage(source.clusterId(),source.databaseId(),()->cut,()->List.of(state),header->changedDefinition);assertThrows(IOException.class,()->changed.resolve(request,cut,null));
    }
    private static RatisRecoveryLineage.Request request(RangeDirectory directory,RangeDescriptor range){return new RatisRecoveryLineage.Request(directory.domainId(),range.rangeId(),range.groupId(),range.rangeEpoch());}
}
