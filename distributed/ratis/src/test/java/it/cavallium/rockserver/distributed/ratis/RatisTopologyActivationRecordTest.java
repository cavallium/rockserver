package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisTopologyActivationRecordTest {
    record Fixture(TopologyChange.Definition definition,TopologyChange.State seeded) {}
    Fixture fixture() {
        var template=RatisRangeImportCatalogueTest.definition(UUID.randomUUID(),UUID.randomUUID(),new OperationId(UUID.randomUUID(),0,1)).sourceDirectory();
        var old=template.ranges().getFirst();
        var source=new RangeDirectory(template.clusterId(),template.databaseId(),template.domainId(),template.schemaEpoch(),template.schemaDigest(),template.generation(),template.columns(),0,
                List.of(new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),old.configurationVersion(),null,null,RangeDescriptor.TopologyState.ACTIVE,old.replicas())));
        var definition=new RatisTopologyCatalogueTest().split(source,1,128);
        var state=TopologyChange.State.begin(definition.header()).beginFencing();
        var helper=new RatisTopologyAdministrationExclusionTest();
        for(var parent:definition.header().parents())state=state.frozen(helper.proof(state,TopologyChange.Stage.FREEZE,parent));
        for(var child:definition.header().targets())state=state.seeded(helper.proof(state,TopologyChange.Stage.SEED,child));
        return new Fixture(definition,state);
    }
    @Test void commonInputIsBoundToPublicationAndChildWithoutLocalReplicaIdentity() {
        var f=fixture();var catalogue=UUID.randomUUID();var cut=new ApplyPosition(2,10);
        var view=new RatisTopologyAuthority.View(catalogue,cut,f.definition(),f.seeded().publish());
        var child=f.definition().targets().getFirst().groupId();var record=RatisTopologyActivationRecord.of(view,child);
        assertEquals(record,RatisTopologyActivationRecord.decode(record.encode()));
        assertEquals(cut,record.catalogueCut());assertEquals(child,record.groupId());
        assertArrayEquals(f.seeded().freezeDigest(),record.freezeDigest());
        assertArrayEquals(NativeTopologyActivation.publicationCommitment(f.definition(),f.seeded().freezeDigest(),catalogue),record.publicationCommitment());
        // A later authority read changes its qualified cut, but preserves the immutable publication commitment.
        var later=RatisTopologyActivationRecord.of(new RatisTopologyAuthority.View(catalogue,new ApplyPosition(3,11),f.definition(),view.state()),child);
        assertArrayEquals(record.publicationCommitment(),later.publicationCommitment());
        assertNotEquals(record,later);
        assertDoesNotThrow(()->record.requireAuthority(new RatisTopologyAuthority.View(catalogue,new ApplyPosition(3,11),f.definition(),view.state())));
        assertThrows(IllegalArgumentException.class,()->record.requireAuthority(new RatisTopologyAuthority.View(catalogue,new ApplyPosition(2,9),f.definition(),view.state())));
        assertThrows(IllegalArgumentException.class,()->record.requireAuthority(new RatisTopologyAuthority.View(UUID.randomUUID(),cut,f.definition(),view.state())));
        var returned=record.freezeDigest();returned[0]^=1;assertArrayEquals(f.seeded().freezeDigest(),record.freezeDigest());
        assertThrows(IllegalArgumentException.class,()->RatisTopologyActivationRecord.of(new RatisTopologyAuthority.View(catalogue,cut,f.definition(),f.seeded()),child));
        assertThrows(IllegalArgumentException.class,()->RatisTopologyActivationRecord.of(view,f.definition().parents().getFirst().groupId()));
    }
    @Test void malformedCommitmentCutAndFramingCannotBecomeActivationInput() {
        var f=fixture();var catalogue=UUID.randomUUID();var child=f.definition().targets().getFirst().groupId();
        var record=RatisTopologyActivationRecord.of(new RatisTopologyAuthority.View(catalogue,new ApplyPosition(1,10),f.definition(),f.seeded().publish()),child);
        byte[] altered=record.encode();altered[52]^=1;
        assertThrows(IllegalArgumentException.class,()->RatisTopologyActivationRecord.decode(altered));
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyActivationRecord(f.definition(),child,record.freezeDigest(),catalogue,new ApplyPosition(0,0),record.publicationCommitment()));
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyActivationRecord(f.definition(),child,record.freezeDigest(),UUID.randomUUID(),record.catalogueCut(),record.publicationCommitment()));
        assertThrows(IllegalArgumentException.class,()->RatisTopologyActivationRecord.decode(Arrays.copyOf(record.encode(),record.encode().length-1)));
        assertThrows(IllegalArgumentException.class,()->RatisTopologyActivationRecord.decode(Arrays.copyOf(record.encode(),record.encode().length+1)));
        assertThrows(IllegalArgumentException.class,()->RatisTopologyActivationRecord.decode(new byte[RatisTopologyActivationRecord.MAX_BYTES+1]));
    }
}
