package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual sealed native parent outcomes, never replanning the original batch against children. */
class NativeTopologyOperationOutcomeTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologyCdcTest nativeTest=new NativeTopologyCdcTest();
    final NativeTopologySeedTest helper=nativeTest.helper;
    ReplicatedRangeStore.TopologyOperationOutcome read(ReplicatedRangeStore parent,TopologyChange.Definition definition,OperationId operation,byte[] digest)throws Exception {
        var schema=helper.outer.schema();return parent.readTopologyOperationOutcome(schema.clusterId(),schema.databaseId(),schema.domainId(),nativeTest.lineage(definition.parents().getFirst()),operation,digest);
    }
    DistributedCommand batch(ReplicatedRangeStore parent) {
        return new DistributedCommand(parent.ownership().owner().groupId(),1,parent.schemaDigest(),new OperationId(helper.outer.session,1,0),List.of(
                new Mutation(Mutation.Kind.PUT,helper.outer.fixed,helper.helper.key(1),helper.helper.key(9),Mutation.ReturnMode.CURRENT),
                new Mutation(Mutation.Kind.PUT,helper.outer.fixed,helper.helper.key(200),helper.helper.key(8),Mutation.ReturnMode.CURRENT)));
    }
    @Test void lostResultAcrossBothChildIntervalsRemainsOriginalAndImmutableThroughRetirementAndReopen()throws Exception {
        Path path=root.resolve("original");TopologyChange.Definition definition;DistributedCommand command;byte[] result;
        try(var f=nativeTest.new Fixture(path)) {
            command=batch(f.parent);result=f.parent.apply(new ApplyPosition(1,1),command);definition=f.definition;
            try(var owned=f.parent.capture()) {
                f.freeze();f.seed();assertEquals(1,f.parent.activeSnapshotPins());var outcome=read(f.parent,definition,command.operationId(),command.receiptDigest());assertEquals(new ApplyPosition(1,2),outcome.cut());assertEquals(f.freeze,outcome.freeze());assertArrayEquals(result,outcome.result());assertEquals(1,f.parent.activeSnapshotPins());
                byte[] returned=outcome.result();returned[0]^=1;assertArrayEquals(result,outcome.result());byte[] cfg=outcome.configuration().data();cfg[0]^=1;assertArrayEquals(new byte[]{1},outcome.configuration().data());
                f.child.apply(new ApplyPosition(3,2),f.write(f.child,0,7));assertArrayEquals(helper.helper.key(9),owned.get(helper.outer.fixed,helper.helper.key(1)));assertArrayEquals(result,read(f.parent,definition,command.operationId(),command.receiptDigest()).result());
                byte[] changed=command.receiptDigest();changed[0]^=1;assertThrows(IllegalArgumentException.class,()->read(f.parent,definition,command.operationId(),changed));
            }
            assertEquals(0,f.parent.activeSnapshotPins());
        }
        try(var pool=helper.pool(path)) { var parent=helper.open(pool,definition.parents().getFirst());assertEquals(0,parent.activeSnapshotPins());assertArrayEquals(result,read(parent,definition,command.operationId(),command.receiptDigest()).result());assertEquals(0,parent.activeSnapshotPins());assertEquals(RangeOwnership.State.RETIRED,parent.ownership().state()); }
    }
    @Test void absenceRequiresPermanentSealAndAckOrEpochGcReturnsExpiredInstead()throws Exception {
        try(var f=nativeTest.new Fixture(root)) {
            var command=batch(f.parent);var unknown=new OperationId(new UUID(0,1001),3,0);assertThrows(IllegalStateException.class,()->read(f.parent,f.definition,unknown,new byte[32]));f.parent.apply(new ApplyPosition(1,1),command);f.freeze();assertNull(read(f.parent,f.definition,unknown,new byte[32]).result());
            var ack=new DistributedCommand(command.groupId(),1,f.parent.schemaDigest(),new OperationId(command.operationId().sessionId(),1,1),List.of(),List.of(),0);f.parent.apply(new ApplyPosition(1,3),ack);assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(read(f.parent,f.definition,command.operationId(),command.receiptDigest()).result()).status());
            f.parent.apply(new ApplyPosition(1,4),DistributedCommand.advanceSessionEpochFloor(command.groupId(),1,f.parent.schemaDigest(),new OperationId(new UUID(0,1002),2,0),2));var absentOldEpoch=new OperationId(new UUID(0,1003),1,100);assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(read(f.parent,f.definition,absentOldEpoch,new byte[32]).result()).status());assertNull(read(f.parent,f.definition,unknown,new byte[32]).result());assertEquals(0,f.parent.activeSnapshotPins());
        }
    }
    @Test void retiredParentAcknowledgesOriginalReceiptsWithoutChildEffectsAndReplaysExactly()throws Exception {
        Path path=root.resolve("ack-retired");TopologyChange.Definition definition;DistributedCommand command,ack;byte[] result;
        try(var f=nativeTest.new Fixture(path)) {
            command=batch(f.parent);f.parent.apply(new ApplyPosition(1,1),command);f.freeze();f.seed();definition=f.definition;
            assertEquals(RangeOwnership.State.RETIRED,f.parent.ownership().state());
            ack=new DistributedCommand(command.groupId(),1,f.parent.schemaDigest(),new OperationId(command.operationId().sessionId(),1,1),List.of(),List.of(),0);
            assertTrue(f.parent.metadataTopologyControl(ack));f.parent.validate(ack);assertTrue(f.parent.estimateReceiptRetention(ack).newReceipt());
            result=f.parent.apply(new ApplyPosition(1,3),ack);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(result).status());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(read(f.parent,definition,command.operationId(),command.receiptDigest()).result()).status());
            assertArrayEquals(result,f.parent.apply(new ApplyPosition(2,4),ack));assertArrayEquals(result,read(f.parent,definition,ack.operationId(),ack.receiptDigest()).result());
            assertArrayEquals(helper.helper.key(9),f.child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertEquals(0,f.parent.activeSnapshotPins());
        }
        try(var pool=helper.pool(path)){var parent=helper.open(pool,definition.parents().getFirst());assertArrayEquals(result,read(parent,definition,ack.operationId(),ack.receiptDigest()).result());assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(read(parent,definition,command.operationId(),command.receiptDigest()).result()).status());}
    }
    @Test void rejectedAcceptedTransactionRetainsOriginalBusinessReceiptDigest()throws Exception {
        try(var f=nativeTest.new Fixture(root)) {
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(helper.outer.fixed,null,null)));var accepted=DistributedCommand.acceptedTransaction(batch(f.parent),new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(0,1004),scope,new CommandResult(CommandResult.Status.SUCCESS,List.of(helper.helper.key(9),helper.helper.key(8))).encode()));var rejected=DistributedCommand.rejectAcceptedTransaction(accepted);byte[] original=f.parent.apply(new ApplyPosition(1,1),rejected);f.freeze();f.seed();assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(read(f.parent,f.definition,accepted.operationId(),accepted.receiptDigest()).result()).status());assertArrayEquals(original,read(f.parent,f.definition,accepted.operationId(),accepted.receiptDigest()).result());assertThrows(IllegalArgumentException.class,()->read(f.parent,f.definition,accepted.operationId(),rejected.payloadDigest()));
        }
    }
    @Test void namespaceFullLineagePendingBirthAndImageInstallationAreRejected()throws Exception {
        try(var f=nativeTest.new Fixture(root)) {
            var command=batch(f.parent);f.parent.apply(new ApplyPosition(1,1),command);f.freeze();var schema=helper.outer.schema();var lineage=nativeTest.lineage(f.definition.parents().getFirst());UUID wrong=new UUID(0,1005);
            assertThrows(IllegalArgumentException.class,()->f.parent.readTopologyOperationOutcome(wrong,schema.databaseId(),schema.domainId(),lineage,command.operationId(),command.receiptDigest()));assertThrows(IllegalArgumentException.class,()->f.parent.readTopologyOperationOutcome(schema.clusterId(),wrong,schema.domainId(),lineage,command.operationId(),command.receiptDigest()));assertThrows(IllegalArgumentException.class,()->f.parent.readTopologyOperationOutcome(schema.clusterId(),schema.databaseId(),wrong,lineage,command.operationId(),command.receiptDigest()));
            for(var changed:List.of(new DistributedCdcCheckpoint.Lineage(wrong,lineage.groupId(),1),new DistributedCdcCheckpoint.Lineage(lineage.rangeId(),wrong,1),new DistributedCdcCheckpoint.Lineage(lineage.rangeId(),lineage.groupId(),2)))assertThrows(IllegalArgumentException.class,()->f.parent.readTopologyOperationOutcome(schema.clusterId(),schema.databaseId(),schema.domainId(),changed,command.operationId(),command.receiptDigest()));
            Path image=root.resolve("sealed-image");NativeRangeImage.write(image,f.parent,nativeTest.limits);f.parent.fence();assertThrows(java.io.IOException.class,()->f.parent.installSnapshot(image,nativeTest.limits,batches->{throw new java.io.IOException("Controlled interruption");}));assertTrue(f.parent.pendingInstallation());assertThrows(IllegalStateException.class,()->read(f.parent,f.definition,command.operationId(),command.receiptDigest()));assertEquals(0,f.parent.activeSnapshotPins());
        }
        try(var f=nativeTest.new Fixture(root.resolve("birth"))) { f.parent.apply(new ApplyPosition(1,1),batch(f.parent));f.freeze();f.seed();var target=f.definition.targets().getLast();var pending=f.pool.reopenTopologyRange(f.seed.birth(target.groupId()),helper.outer.schema());pending.initializeConfiguration(new byte[]{1});pending.applyNoop(new ApplyPosition(1,0));assertThrows(IllegalStateException.class,()->pending.readTopologyOperationOutcome(helper.outer.schema().clusterId(),helper.outer.schema().databaseId(),helper.outer.schema().domainId(),nativeTest.lineage(target),new OperationId(helper.outer.session,1,0),new byte[32])); }
    }
    @Test void unpublishedImportAndOversizedDurableReceiptFailBeforeOutcomeExposure()throws Exception {
        try(var pool=helper.pool(root.resolve("import"))) { var schema=helper.outer.schema();UUID group=new UUID(0,1006);var columns=schema.columnIds().stream().map(id->new NativeSstImportManifest.Column(id,"import-"+helper.outer.names.get(id),false,List.of())).toList();var manifest=new NativeSstImportManifest(new UUID(0,1007),group,1,schema.domainId(),schema.digest(),null,null,columns,NativeSstImportManifest.Limits.defaults());var pending=pool.openNewFencedRange(manifest,schema);assertTrue(pending.importPending());assertThrows(IllegalStateException.class,()->pending.readTopologyOperationOutcome(schema.clusterId(),schema.databaseId(),schema.domainId(),new DistributedCdcCheckpoint.Lineage(new UUID(0,1008),group,1),new OperationId(helper.outer.session,1,0),new byte[32])); }
        try(var f=nativeTest.new Fixture(root.resolve("oversized"))) { var command=batch(f.parent);f.parent.apply(new ApplyPosition(1,1),command);f.freeze();byte[] key=ByteBuffer.allocate(48).putLong(command.groupId().getMostSignificantBits()).putLong(command.groupId().getLeastSignificantBits()).putLong(command.operationId().epoch()).putLong(command.operationId().sessionId().getMostSignificantBits()).putLong(command.operationId().sessionId().getLeastSignificantBits()).putLong(command.operationId().sequence()).array();helper.db(f.pool).put(helper.handles(f.pool).get(DistributedStoragePool.RECEIPTS_COLUMN),key,new byte[37+(2<<20)]);assertThrows(IllegalStateException.class,()->read(f.parent,f.definition,command.operationId(),command.receiptDigest()));assertEquals(new ApplyPosition(1,2),f.parent.applied());assertEquals(0,f.parent.activeSnapshotPins()); }
    }
}
