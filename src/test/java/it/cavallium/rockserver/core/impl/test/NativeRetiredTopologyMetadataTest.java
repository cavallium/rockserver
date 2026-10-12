package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native parent metadata ownership; no Raft/server or final lineage GC claim. */
class NativeRetiredTopologyMetadataTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologyCdcTest topology=new NativeTopologyCdcTest();
    final NativeSourceOutcomePublicationTest source=new NativeSourceOutcomePublicationTest();
    void activateOther(NativeTopologyCdcTest.Fixture f)throws Exception {
        var target=f.definition.targets().getLast();var store=f.pool.reopenTopologyRange(f.seed.birth(target.groupId()),topology.helper.outer.schema());store.initializeConfiguration(new byte[]{1});store.applyNoop(new ApplyPosition(3,0));store.applyTopologyActivation(new ApplyPosition(3,1),f.definition,f.seed.authority().freezeDigest(),topology.catalogue,new ApplyPosition(1,10),NativeTopologyActivation.publicationCommitment(f.definition,f.seed.authority().freezeDigest(),topology.catalogue));
    }
    @Test void realReadonlyPinDelaysDetachThenMetadataOutcomesPublicationAckAndClockRemainUsable()throws Exception {
        var home=source.home;
        try(var f=topology.new Fixture(root.resolve("source"));var h=home.new Fixture(root.resolve("home"))) {
            var original=f.write(f.parent,0,9);byte[] result=f.parent.apply(new ApplyPosition(1,1),original);
            try(var pin=f.parent.captureReadable(topology.helper.helper.point(topology.helper.outer.fixed,1))) {
                f.freeze();f.parent.configureSourceOutcomeCapacity(8,736);var request=source.reserve(f,h,1,1);var credit=f.parent.applySourceOutcomeCredit(new ApplyPosition(1,3),request);f.seed();f.child.apply(new ApplyPosition(3,2),f.write(f.child,0,8));
                assertFalse(f.pool.detachRetiredUserHandles(f.parent));assertFalse(f.parent.userHandlesDetached());assertSame(f.parent,f.pool.openRetiredTopologyRange(original.groupId()));assertFalse(f.parent.userHandlesDetached());assertArrayEquals(topology.helper.helper.key(9),pin.get(topology.helper.outer.fixed,topology.helper.helper.key(1)));pin.close();
                assertTrue(f.pool.detachRetiredUserHandles(f.parent));assertTrue(f.pool.detachRetiredUserHandles(f.parent));assertTrue(f.parent.userHandlesDetached());assertSame(f.parent,f.pool.openRetiredTopologyRange(original.groupId()));assertEquals(topology.helper.outer.schema().columnIds(),f.parent.retiredOutcomeState().columns());assertArrayEquals(result,f.parent.operationResult(original.operationId(),original.receiptDigest()));
                assertEquals(credit,f.parent.readTopologyOperationOutcome(request.origin().clusterId(),request.origin().databaseId(),request.origin().domainId(),request.origin().source(),request.original().operationId(),request.original().receiptDigest()).credit());assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,f.parent.publishSourceOutcome(new ApplyPosition(1,4),request,source.success()).outcome());
                var ack=new DistributedCommand(original.groupId(),1,f.parent.schemaDigest(),new OperationId(original.operationId().sessionId(),1,2),List.of(),List.of(),1);f.parent.apply(new ApplyPosition(1,5),ack);assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(f.parent.operationResult(original.operationId(),original.receiptDigest())).status());assertEquals(2,f.retired(DistributedCdcCheckpoint.Cursor.beginning()).records().size());
                var schema=topology.helper.outer.schema();var ledger=SessionEpochLedger.initial(schema.clusterId(),schema.databaseId(),0);f.parent.apply(new ApplyPosition(1,6),f.clock(ledger,0));f.parent.apply(new ApplyPosition(1,7),f.clock(ledger.advanceTime(86_400_000),1));assertThrows(ReplicatedRangeStore.CdcGap.class,()->f.retired(DistributedCdcCheckpoint.Cursor.beginning()));assertArrayEquals(topology.helper.helper.key(8),f.child.readCurrentPoint(topology.helper.outer.fixed,topology.helper.helper.key(1)));assertThrows(IllegalStateException.class,f.parent::captureForRecovery);assertThrows(IllegalStateException.class,f.parent::activate);
            }
        }
    }
    @Test void childColumnDropAndColdParentMetadataOpenKeepOriginalProofsAndChildrenUserRows()throws Exception {
        Path path=root.resolve("drop");NativeTopologySeed seed;DistributedCommand original;byte[] result;var schema=topology.helper.outer.schema();
        try(var f=topology.new Fixture(path)) {
            original=f.write(f.parent,0,9);result=f.parent.apply(new ApplyPosition(1,1),original);f.freeze();f.seed();seed=f.seed;activateOther(f);assertTrue(f.pool.detachRetiredUserHandles(f.parent));f.child.apply(new ApplyPosition(3,2),f.write(f.child,0,8));
            var target=new DistributedSchema(schema.clusterId(),schema.databaseId(),schema.domainId(),2,schema.routingPrefixBytes(),schema.columns().stream().filter(column->!column.columnId().equals(topology.helper.outer.bucket)).toList());
            var change=new SchemaChange.Definition(new UUID(0,990),new OperationId(new UUID(0,991),1,0),f.definition.targetDirectory(),schema,target);var names=new HashMap<>(topology.helper.outer.names);names.remove(topology.helper.outer.bucket);
            for(var descriptor:f.definition.targets()) { var child=f.pool.reopenTopologyRange(seed.birth(descriptor.groupId()),schema);long index=child.applied().index();child.applySchemaFence(new ApplyPosition(3,index+1),change.header());var install=f.pool.installSchema(child,new ApplyPosition(3,index+2),change,names,Set.of());child.applySchemaActivation(new ApplyPosition(3,index+3),install.proof(),topology.catalogue,new ApplyPosition(1,20)); }
            assertNull(topology.helper.handles(f.pool).get("bucket"));assertArrayEquals(result,f.parent.operationResult(original.operationId(),original.receiptDigest()));assertArrayEquals(topology.helper.helper.key(8),f.child.readCurrentPoint(topology.helper.outer.fixed,topology.helper.helper.key(1)));
        }
        try(var pool=topology.helper.pool(path)) {
            assertNull(topology.helper.handles(pool).get("bucket"));var parent=pool.openRetiredTopologyRange(original.groupId());assertTrue(parent.userHandlesDetached());assertEquals(schema.columnIds(),parent.retiredOutcomeState().columns());assertArrayEquals(result,parent.operationResult(original.operationId(),original.receiptDigest()));parent.enableCdc(seed.definition().parents().getFirst().rangeId());assertEquals(2,parent.pollRetiredCdc(topology.lineage(seed.definition().parents().getFirst()),DistributedCdcCheckpoint.Cursor.beginning(),10,8<<20).records().size());
            var child=pool.reopenTopologyRange(seed.birth(seed.definition().targets().getFirst().groupId()),schema);assertArrayEquals(topology.helper.helper.key(8),child.readCurrentPoint(topology.helper.outer.fixed,topology.helper.helper.key(1)));assertSame(parent,pool.openRetiredTopologyRange(original.groupId()));assertThrows(IllegalArgumentException.class,()->pool.openRetiredTopologyRange(child.ownership().owner().groupId()));
        }
    }
    @Test void forgedRetirementPointerCannotOpenMetadataOrDetach()throws Exception {
        try(var f=topology.new Fixture(root.resolve("forged"))) {
            assertThrows(IllegalArgumentException.class,()->f.pool.openRetiredTopologyRange(f.parent.ownership().owner().groupId()));f.freeze();f.seed();var group=f.parent.ownership().owner().groupId();byte[] key=java.nio.ByteBuffer.allocate(17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)0x62).array();topology.helper.db(f.pool).put(topology.helper.handles(f.pool).get(DistributedStoragePool.STATE_COLUMN),key,new byte[32]);assertThrows(IllegalArgumentException.class,()->f.pool.openRetiredTopologyRange(group));assertThrows(IllegalArgumentException.class,()->f.pool.detachRetiredUserHandles(f.parent));assertFalse(f.parent.userHandlesDetached());
        }
    }
}
