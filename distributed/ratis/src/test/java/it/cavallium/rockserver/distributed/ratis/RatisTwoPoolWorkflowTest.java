package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisTwoPoolWorkflowTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void sameServiceAcceptsCommittedServingSplitAcrossItsOriginalTwoPools()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root)){
            var definition=fixture.split();fixture.service.verifyCatalogue();
            for(var child:definition.targets()){assertEquals(fixture.fast,fixture.node.poolId(child.groupId()));assertNotNull(fixture.store(child.groupId()).topologyActivation());}
            var other=fixture.secondDirectory.ranges().getFirst();assertEquals(fixture.archive,fixture.node.poolId(other.groupId()));
            assertArrayEquals(new byte[]{22},fixture.store(other.groupId()).readCurrentPoint(fixture.second.columnIds().iterator().next(),new byte[]{7}));
            assertArrayEquals(new byte[]{11},fixture.store(definition.targets().getFirst().groupId()).readCurrentPoint(fixture.first.columnIds().iterator().next(),new byte[]{7}));
            assertNotSame(fixture.node.nativePool(fixture.fast),fixture.node.nativePool(fixture.archive));
        }
    }
}
