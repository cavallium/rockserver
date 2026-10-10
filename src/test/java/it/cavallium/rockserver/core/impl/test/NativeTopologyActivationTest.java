package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeTopologyActivationTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologySeedTest fixture=new NativeTopologySeedTest();
    final UUID catalogue=new UUID(0,900);
    final ApplyPosition catalogueCut=new ApplyPosition(3,5);
    @Test void forcedActivationOwnPrefixRetryBusinessAndColdRecovery() throws Exception {
        var definition=fixture.topology.split(fixture.outer.schema(),100); NativeTopologySeed seed; NativeTopologyActivation receipt;
        byte[] fencing=TopologyChange.State.begin(definition.header()).beginFencing().encode();
        byte[] admission=OperatorUpload.sha256(java.nio.ByteBuffer.allocate(16+fencing.length).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).put(fencing).array());
        System.arraycopy(admission,0,fixture.topology.admission,0,admission.length);
        var path=root.resolve("activation");
        try(var pool=fixture.pool(path)) {
            var parent=fixture.open(pool,definition.parents().getFirst()); parent.applyNoop(new ApplyPosition(1,0)); parent.apply(new ApplyPosition(1,1),fixture.put(parent.ownership().owner().groupId(),0,1,9));
            var authority=fixture.freeze(definition,List.of(parent)); seed=pool.seedTopology(definition,authority,List.of(parent),fixture.topology.replica);
            var child=pool.reopenTopologyRange(seed.birth(definition.targets().getFirst().groupId()),fixture.outer.schema());
            assertThrows(IllegalStateException.class,child::activate); assertNull(child.applied());
            var common=authority.freezeDigest(); var publication=NativeTopologyActivation.publicationCommitment(definition,common,catalogue);
            assertThrows(IllegalStateException.class,()->child.applyTopologyActivation(new ApplyPosition(1,1),definition,common,catalogue,catalogueCut,publication));
            child.initializeConfiguration(new byte[]{1}); child.applyNoop(new ApplyPosition(1,0));
            byte[] wrong=publication.clone(); wrong[0]^=1;
            assertThrows(IllegalArgumentException.class,()->child.applyTopologyActivation(new ApplyPosition(1,1),definition,common,catalogue,new ApplyPosition(3,0),publication));
            assertThrows(IllegalArgumentException.class,()->child.applyTopologyActivation(new ApplyPosition(1,1),definition,common,catalogue,catalogueCut,wrong)); assertNull(child.topologyActivation()); assertEquals(new ApplyPosition(1,0),child.applied()); assertTrue(child.topologyBirthPending());
            receipt=child.applyTopologyActivation(new ApplyPosition(1,1),definition,common,catalogue,catalogueCut,publication);
            assertFalse(child.topologyBirthPending()); assertEquals(RangeOwnership.State.ACTIVE,child.ownership().state()); assertArrayEquals(seed.birth(child.ownership().owner().groupId()).proof(seed).digest(),receipt.proof().predecessorDigest());
            assertArrayEquals(receipt.encode(),NativeTopologyActivation.decode(receipt.encode()).encode());
            assertSame(receipt,child.applyTopologyActivation(new ApplyPosition(2,2),definition,common,catalogue,catalogueCut,publication)); assertEquals(new ApplyPosition(1,1),receipt.position());
            assertThrows(IllegalArgumentException.class,()->child.applyTopologyActivation(new ApplyPosition(2,3),definition,common,catalogue,new ApplyPosition(3,6),publication));
            assertThrows(IllegalArgumentException.class,()->child.applyTopologyActivation(new ApplyPosition(2,1),definition,common,catalogue,catalogueCut,publication));
            assertArrayEquals(fixture.helper.key(9),child.readCurrentPoint(fixture.outer.fixed,fixture.helper.key(1)));
            child.apply(new ApplyPosition(2,3),fixture.put(child.ownership().owner().groupId(),1,1,8));
            assertNotNull(NativeRangeImage.write(root.resolve("child.img"),child,new NativeRangeImage.Limits(4<<20,16<<20,4)).topologyOrigin());
            assertThrows(IllegalStateException.class,()->parent.applyTopologyActivation(new ApplyPosition(1,parent.applied().index()+1),definition,common,catalogue,catalogueCut,publication));
        }
        try(var pool=fixture.pool(path)) {
            var child=pool.reopenTopologyRange(seed.birth(definition.targets().getFirst().groupId()),fixture.outer.schema()); assertFalse(child.topologyBirthPending()); assertEquals(RangeOwnership.State.ACTIVE,child.ownership().state()); assertArrayEquals(receipt.encode(),child.topologyActivation().encode()); assertEquals(new ApplyPosition(2,3),child.applied()); assertArrayEquals(fixture.helper.key(8),child.readCurrentPoint(fixture.outer.fixed,fixture.helper.key(1)));
            var parent=fixture.open(pool,definition.parents().getFirst()); assertTrue(parent.topologyRetired()); assertThrows(IllegalStateException.class,parent::activate);
        }
    }
    @Test void commonDecisionProducesDifferentLocalReceiptsAndRejectsForeignBirth() throws Exception {
        var original=fixture.topology.replica; var second=new ReplicaDescriptor(new UUID(0,50),new UUID(0,51),new UUID(0,52),"second","127.0.0.1:9998",ReplicaDescriptor.Role.VOTER);
        var template=fixture.topology.split(fixture.outer.schema(),101);
        java.util.function.Function<RangeDescriptor,RangeDescriptor> placement=r->new RangeDescriptor(r.rangeId(),r.groupId(),r.rangeEpoch(),r.configurationVersion(),r.lowerInclusive(),r.upperExclusive(),r.topologyState(),List.of(original,second));
        var definition=new TopologyChange.Definition(template.intentId(),fixture.topology.directory(fixture.outer.schema(),1,template.parents().stream().map(placement).toList()),fixture.topology.directory(fixture.outer.schema(),2,template.targets().stream().map(placement).toList()));
        byte[] previous=null;
        for(var replica:List.of(original,second)) try(var pool=fixture.pool(root.resolve(replica.peerId()))) {
            var parent=fixture.open(pool,definition.parents().getFirst()); parent.applyNoop(new ApplyPosition(1,0)); var freeze=parent.applyTopologyFreeze(new ApplyPosition(1,1),definition,fixture.topology.admission);
            var authority=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(original.replicaId())).frozen(freeze.proof(second.replicaId()));
            var seed=pool.seedTopology(definition,authority,List.of(parent),replica); var child=pool.reopenTopologyRange(seed.birth(definition.targets().getFirst().groupId()),fixture.outer.schema()); child.initializeConfiguration(new byte[]{1}); child.applyNoop(new ApplyPosition(1,0));
            var receipt=child.applyTopologyActivation(new ApplyPosition(1,1),definition,authority.freezeDigest(),catalogue,catalogueCut,NativeTopologyActivation.publicationCommitment(definition,authority.freezeDigest(),catalogue));
            assertEquals(replica.replicaId(),receipt.proof().owner().replicaId()); if(previous!=null) assertFalse(Arrays.equals(previous,receipt.digest())); previous=receipt.digest();
            byte[] corrupt=receipt.encode(); corrupt[corrupt.length-1]^=1; assertThrows(IllegalArgumentException.class,()->NativeTopologyActivation.decode(corrupt));
        }
    }
    @Test void activatedChildCanRetireAsNextParentAndColdReopenNeverUnretiresIt() throws Exception {
        var definition=fixture.topology.split(fixture.outer.schema(),102); NativeTopologySeed seed; UUID childGroup=definition.targets().getFirst().groupId(); Path path=root.resolve("next-retirement");
        try(var pool=fixture.pool(path)) {
            var parent=fixture.open(pool,definition.parents().getFirst()); var authority=fixture.freeze(definition,List.of(parent)); seed=pool.seedTopology(definition,authority,List.of(parent),fixture.topology.replica);
            var child=pool.reopenTopologyRange(seed.birth(childGroup),fixture.outer.schema()); child.initializeConfiguration(new byte[]{1}); child.applyNoop(new ApplyPosition(1,0)); child.applyTopologyActivation(new ApplyPosition(1,1),definition,authority.freezeDigest(),catalogue,catalogueCut,NativeTopologyActivation.publicationCommitment(definition,authority.freezeDigest(),catalogue));
            var nextTargets=List.of(fixture.topology.range(61,new UUID(0,71),null,new byte[]{64}),fixture.topology.range(62,new UUID(0,72),new byte[]{64},new byte[]{(byte)128}),definition.targets().getLast());
            var next=new TopologyChange.Definition(new UUID(0,103),definition.targetDirectory(),fixture.topology.directory(fixture.outer.schema(),3,nextTargets));
            var nextAuthority=fixture.freeze(next,List.of(child)); pool.seedTopology(next,nextAuthority,List.of(child),fixture.topology.replica); assertTrue(child.topologyRetired()); assertNotNull(child.topologyActivation());
        }
        try(var pool=fixture.pool(path)) { var child=pool.reopenTopologyRange(seed.birth(childGroup),fixture.outer.schema()); assertTrue(child.topologyRetired()); assertNotNull(child.topologyActivation()); assertNotNull(child.retiredOutcomeState()); assertEquals(RangeOwnership.State.RETIRED,child.ownership().state()); assertThrows(IllegalStateException.class,child::activate); }
    }
    @Test void activationOriginSurvivesVerifiedLaterSchemaInstallationAndColdReopen() throws Exception {
        var source=fixture.outer.schema(); var definition=fixture.topology.split(source,104); NativeTopologySeed seed; var added=new UUID(0,991); var columns=new ArrayList<>(source.columns()); var prototype=columns.getFirst(); columns.add(new DistributedSchema.Column(991,added,"added",prototype.schema(),null));
        var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch()+1,source.routingPrefixBytes(),columns); Path path=root.resolve("schema-evolution");
        try(var pool=fixture.pool(path)) {
            var parent=fixture.open(pool,definition.parents().getFirst()); var authority=fixture.freeze(definition,List.of(parent)); seed=pool.seedTopology(definition,authority,List.of(parent),fixture.topology.replica);
            var change=new SchemaChange.Definition(new UUID(0,992),new OperationId(new UUID(0,993),1,0),definition.targetDirectory(),source,target); var names=new HashMap<>(fixture.outer.names); names.put(added,"added");
            var children=new ArrayList<ReplicatedRangeStore>();
            for(var descriptor:definition.targets()) {
                var child=pool.reopenTopologyRange(seed.birth(descriptor.groupId()),source); child.initializeConfiguration(new byte[]{1}); child.applyNoop(new ApplyPosition(1,0)); child.applyTopologyActivation(new ApplyPosition(1,1),definition,authority.freezeDigest(),catalogue,catalogueCut,NativeTopologyActivation.publicationCommitment(definition,authority.freezeDigest(),catalogue));
                children.add(child);
            }
            for(var child:children) {
                child.applySchemaFence(new ApplyPosition(1,2),change.header()); var install=pool.installSchema(child,new ApplyPosition(1,3),change,names,Set.of()); child.applySchemaActivation(new ApplyPosition(1,4),install.proof(),catalogue,catalogueCut); assertArrayEquals(target.digest(),child.schemaDigest());
            }
        }
        try(var pool=fixture.pool(path)) { for(var descriptor:definition.targets()) { var child=pool.reopenTopologyRange(seed.birth(descriptor.groupId()),source); assertNotNull(child.topologyActivation()); assertFalse(child.topologyBirthPending()); assertArrayEquals(target.digest(),child.schemaDigest()); assertEquals(target.columnIds(),child.ownership().interval().columns()); assertEquals(RangeOwnership.State.ACTIVE,child.ownership().state()); } }
    }

}
