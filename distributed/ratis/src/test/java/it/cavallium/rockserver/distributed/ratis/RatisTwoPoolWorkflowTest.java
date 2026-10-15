package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisTwoPoolWorkflowTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void sameServiceAcceptsCommittedServingSplitAcrossItsOriginalTwoPools()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,false)){
            var definition=fixture.split();fixture.service.verifyCatalogue();fixture.assertSharedNodeResources();
            for(var child:definition.targets()){assertEquals(fixture.fast,fixture.node.poolId(child.groupId()));assertNotNull(fixture.store(child.groupId()).topologyActivation());}
            var other=fixture.secondDirectory.ranges().getFirst();assertEquals(fixture.archive,fixture.node.poolId(other.groupId()));
            assertArrayEquals(new byte[]{22},fixture.store(other.groupId()).readCurrentPoint(fixture.second.columnIds().iterator().next(),new byte[]{7}));
            assertArrayEquals(new byte[]{11},fixture.store(definition.targets().getFirst().groupId()).readCurrentPoint(fixture.first.columnIds().iterator().next(),new byte[]{7}));
            assertNotSame(fixture.node.nativePool(fixture.fast),fixture.node.nativePool(fixture.archive));
        }
    }
    @Test void sameServiceSplitsFirstPoolThenImportsSecondPoolUnderOneActualResourceGraph()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,true)){
            var split=fixture.split();var receipt=fixture.importArchivePool();fixture.assertSharedNodeResources();
            assertEquals(fixture.imported.importId(),receipt.importId());assertArrayEquals(new byte[]{44},fixture.store(receipt.groupId()).readCurrentPoint(fixture.second.columnIds().iterator().next(),new byte[]{7}));
            assertArrayEquals(new byte[]{11},fixture.store(split.targets().getFirst().groupId()).readCurrentPoint(fixture.first.columnIds().iterator().next(),new byte[]{7}));fixture.service.verifyCatalogue();
        }
    }
    @Test void sameServiceSplitImportSchemaOperatorAndNativeMergeShareTheTwoPools()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,true)){
            fixture.split();fixture.importArchivePool();var merged=fixture.createOperatorAndMerge();
            assertEquals(fixture.appendOperator,fixture.store(merged.last().groupId()).installedSchema().column(merged.api()).operator());
            assertArrayEquals(new byte[]{11,33},fixture.store(merged.last().groupId()).readCurrentPoint(merged.column(),new byte[]{9}));
            assertEquals(merged.grant().sessionId(),merged.last().operationId().sessionId());assertEquals(merged.grant().epoch(),merged.last().operationId().epoch());fixture.service.verifyCatalogue();
        }
    }
    @Test void sameServiceTwoPoolWorkflowCopiesCompleteBackupDrainsPinsAndColdReopensOriginalIdentity()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,true)){
            fixture.split();fixture.importArchivePool();var merged=fixture.createOperatorAndMerge();var backup=fixture.backupBothPools();fixture.coldReopen(merged,backup);
        }
    }

}
