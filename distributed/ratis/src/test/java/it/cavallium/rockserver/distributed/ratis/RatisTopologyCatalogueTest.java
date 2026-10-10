package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native catalogue CAS and reopen; no fencing, child startup or network claim. */
class RatisTopologyCatalogueTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisRangeImportCatalogueTest owner() { var owner=new RatisRangeImportCatalogueTest(); owner.root=root; return owner; }
    RangeDirectory source(RatisRangeImportCatalogueTest.Fixture f) { return new RatisCdcSubscriptionCatalogueTest().directory(f); }
    TopologyChange.Definition split(RangeDirectory source,int id,int boundary) {
        var parent=source.ranges().getFirst(); byte[] key={(byte)boundary};
        var left=new RangeDescriptor(new UUID(80,id*10L+1),new UUID(81,id*10L+1),1,parent.configurationVersion(),null,key,RangeDescriptor.TopologyState.ACTIVE,parent.replicas());
        var right=new RangeDescriptor(new UUID(80,id*10L+2),new UUID(81,id*10L+2),1,parent.configurationVersion(),key,null,RangeDescriptor.TopologyState.ACTIVE,parent.replicas());
        var target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),List.of(left,right));
        return new TopologyChange.Definition(new UUID(82,id),source,target);
    }
    RatisTopologyCatalogue catalogue(RatisRangeImportCatalogueTest.Fixture f) { return new RatisTopologyCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn); }
    @Test void exactDefinitionBeginAndAbortSurviveLostRepliesReopenAndImage() throws Exception {
        var owner=owner(); Path image=root.resolve("topology.image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,2); TopologyChange.State aborted;
        try(var f=owner.new Fixture()) {
            var source=source(f); f.seed(source); var definition=split(source,1,128); var catalogue=catalogue(f);
            assertNull(catalogue.readCapacity()); var reserve=catalogue.prepareDefinition(f.operation(1),definition,null);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(reserve).status()); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(reserve).status());
            assertArrayEquals(definition.encode(),catalogue.readDefinition(definition.digest()).encode()); var capacity=catalogue.readCapacity(); assertEquals(1,capacity.count());
            var begin=catalogue.prepareBegin(f.operation(2),definition,null,null,null); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(begin).status()); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(begin).status());
            var prepared=catalogue.readIntent(definition.header().intentId()); assertEquals(TopologyChange.Phase.PREPARED,prepared.phase()); assertEquals(prepared,catalogue.readHead(source.domainId()));
            var abort=catalogue.prepareAbort(f.operation(3),prepared); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(abort).status()); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(abort).status());
            aborted=catalogue.readIntent(definition.header().intentId()); assertTrue(aborted.terminal()); assertEquals(capacity,catalogue.readCapacity());
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(begin).status()); assertEquals(aborted,catalogue.readHead(source.domainId()));
            try(var pin=f.nativeCatalogue.store.capture()) { NativeRangeImage.write(image,pin,pin.interval(),limits); }
        }
        try(var f=owner.new Fixture()) {
            assertEquals(aborted,catalogue(f).readIntent(aborted.header().intentId())); f.nativeCatalogue.store.fence(); f.nativeCatalogue.store.installSnapshot(image,limits); f.nativeCatalogue.store.activate();
            assertEquals(aborted,catalogue(f).readHead(aborted.header().domainId())); assertEquals(1,catalogue(f).readCapacity().count());
        }
    }
    @Test void capacityAndImmutableIntentIdentityRejectConflictsWithoutPartialReservation() throws Exception {
        var owner=owner(); try(var f=owner.new Fixture()) {
            var source=source(f); f.seed(source); var catalogue=catalogue(f); var a=split(source,1,128); var b=split(source,2,96);
            var first=catalogue.prepareDefinition(f.operation(1),a,null); var raced=catalogue.prepareDefinition(f.operation(2),b,null);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(first).status()); var capacity=catalogue.readCapacity();
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(raced).status()); assertNull(catalogue.readDefinition(b.digest())); assertEquals(capacity,catalogue.readCapacity());
            var aliased=split(source,1,64); var duplicate=catalogue.prepareDefinition(f.operation(3),aliased,capacity);
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(duplicate).status()); assertNull(catalogue.readDefinition(aliased.digest())); assertEquals(capacity,catalogue.readCapacity());
            assertThrows(IllegalArgumentException.class,()->catalogue.prepareDefinition(f.operation(4),b,new RatisTopologyCatalogue.Capacity(RatisTopologyCatalogue.MAX_INTENTS,1)));
            assertThrows(IllegalArgumentException.class,()->catalogue.prepareDefinition(f.operation(4),b,new RatisTopologyCatalogue.Capacity(1,RatisTopologyCatalogue.MAX_BYTES)));
        }
    }
    @Test void fencingCommitsOnlyAgainstExactPreparedSourceAndSurvivesReopen() throws Exception {
        var owner=owner(); TopologyChange.State fencedIntent;
        try(var f=owner.new Fixture()) {
            var source=source(f); f.seed(source); var definition=split(source,1,128); var catalogue=catalogue(f);
            catalogue.execute(catalogue.prepareDefinition(f.operation(1),definition,null));
            catalogue.execute(catalogue.prepareBegin(f.operation(2),definition,null,null,null));
            var prepared=catalogue.readHead(source.domainId()); var fencing=catalogue.prepareFencing(f.operation(3),definition,prepared,null,null);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(fencing).status());
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(fencing).status());
            fencedIntent=catalogue.readHead(source.domainId()); assertEquals(TopologyChange.Phase.FENCING,fencedIntent.phase());
            assertEquals(fencedIntent,catalogue.readIntent(definition.header().intentId()));
            assertThrows(IllegalStateException.class,()->catalogue.prepareAbort(f.operation(4),fencedIntent));
            var staleAbort=catalogue.prepareAbort(f.operation(5),prepared);
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(staleAbort).status());
            assertEquals(fencedIntent,catalogue.readHead(source.domainId()));
        }
        try(var f=owner.new Fixture()) { assertEquals(fencedIntent,catalogue(f).readHead(fencedIntent.header().domainId())); }
    }
    @Test void sourceDirectoryCasRejectsStaleIntentAndNativeStageAuthorityCannotBeForged() throws Exception {
        var owner=owner(); try(var f=owner.new Fixture()) {
            var source=source(f); f.seed(source); var catalogue=catalogue(f); var definition=split(source,1,128);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareDefinition(f.operation(1),definition,null)).status());
            var begin=catalogue.prepareBegin(f.operation(2),definition,null,null,null);
            var next=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),source.ranges());
            var directories=new RatisCatalogue(f.client,source.clusterId(),source.databaseId(),f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(f.operation(3),source,next)).status());
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(begin).status()); assertNull(catalogue.readHead(source.domainId())); assertNull(catalogue.readIntent(definition.header().intentId()));
            var current=split(next,2,128); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareDefinition(f.operation(4),current,catalogue.readCapacity())).status());
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareBegin(f.operation(5),current,null,null,null)).status());
            var previous=catalogue.readHead(source.domainId()); byte[] following=previous.beginFencing().encode();
            var command=new DistributedCommand(f.outer.catalogue,1,new byte[32],f.operation(6),List.of(new Mutation(Mutation.Kind.PUT,f.outer.directoryColumn,RatisTopologyCatalogue.stateKey(previous.header().intentId()),following),new Mutation(Mutation.Kind.PUT,f.outer.directoryColumn,RatisTopologyCatalogue.headKey(source.domainId()),following)),
                    List.of(new KeyPredicate(f.outer.directoryColumn,RatisTopologyCatalogue.stateKey(previous.header().intentId()),previous.encode()),new KeyPredicate(f.outer.directoryColumn,RatisTopologyCatalogue.headKey(source.domainId()),previous.encode())));
            assertThrows(IllegalArgumentException.class,()->catalogue.execute(command)); assertEquals(previous,catalogue.readHead(source.domainId()));
            var second=catalogue.prepareDefinition(f.operation(7),split(next,3,96),catalogue.readCapacity()); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(second).status());
            assertThrows(IllegalArgumentException.class,()->catalogue.prepareBegin(f.operation(8),split(next,3,96),previous,null,null));
        }
    }
    @Test void proofProgressRequiresOrderedReciprocalGuardsAndExactSourceWitness() throws Exception {
        var owner=owner(); try(var f=owner.new Fixture()) {
            var source=source(f); f.seed(source); var definition=split(source,1,128); var catalogue=catalogue(f);
            catalogue.execute(catalogue.prepareDefinition(f.operation(1),definition,null)); catalogue.execute(catalogue.prepareBegin(f.operation(2),definition,null,null,null));
            catalogue.execute(catalogue.prepareFencing(f.operation(3),definition,catalogue.readHead(source.domainId()),null,null));
            var expected=catalogue.readHead(source.domainId()); var parent=definition.header().parents().getFirst(); var replica=parent.owners().getFirst();
            var proof=TopologyChange.Proof.of(TopologyChange.Stage.FREEZE,definition.header(),parent.groupId(),replica.replicaId(),new ApplyPosition(1,1),new byte[32],definition.header().sourceDirectoryDigest());
            var valid=catalogue.prepareProof(f.operation(4),definition,expected,proof,null,null); assertTrue(RatisTopologyCatalogue.requiresOwnerAdmission(valid)); assertDoesNotThrow(()->f.pure.accept(valid));
            for(int index:new int[]{2,3,4,5}) {
                var predicates=new ArrayList<>(valid.predicates()); var original=predicates.get(index);
                predicates.set(index,new KeyPredicate(f.outer.directoryColumn,new byte[]{42},original.expectedValue()));
                var malformed=new DistributedCommand(f.outer.catalogue,1,new byte[32],f.operation(5+index),valid.mutations(),predicates);
                assertThrows(IllegalArgumentException.class,()->f.pure.accept(malformed));
            }
            assertEquals(expected,catalogue.readHead(source.domainId()));
        }
    }

}
