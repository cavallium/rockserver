package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Trusted native parent boundaries; catalogue/Ratis authority is a separate adapter contract. */
class NativeTopologyFreezeTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativePreparedParticipantTest helper=new NativePreparedParticipantTest();
    final NativeBucketApplyTest outer=helper.outer;
    final byte[] admission=new byte[32];
    final ReplicaDescriptor replica=new ReplicaDescriptor(new UUID(0,10),new UUID(0,11),new UUID(0,12),"parent","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
    RangeDescriptor range(long id,UUID group,byte[] lower,byte[] upper) { return new RangeDescriptor(new UUID(0,id),group,1,1,lower,upper,RangeDescriptor.TopologyState.ACTIVE,List.of(replica)); }
    RangeDirectory directory(DistributedSchema schema,long generation,List<RangeDescriptor> ranges) { return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),generation,schema.columnIds(),schema.routingPrefixBytes(),ranges); }
    TopologyChange.Definition split(DistributedSchema schema,long intent) {
        var parent=range(20,outer.group,null,null); var children=List.of(range(21,new UUID(0,31),null,new byte[]{(byte)128}),range(22,new UUID(0,32),new byte[]{(byte)128},null));
        return new TopologyChange.Definition(new UUID(0,intent),directory(schema,1,List.of(parent)),directory(schema,2,children));
    }
    @Test void freezeKeepsReadonlyAndOriginalOutcomesWhileRejectingNewWritableWorkAndRestoresThroughImage() throws Exception {
        Path source=root.resolve("source"),image=root.resolve("frozen-image"),oldImage=root.resolve("old-image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4); NativeTopologyFreeze freeze; TopologyChange.Definition definition;
        try(var f=outer.new Fixture(source,true)) {
            helper.enable(f); var original=helper.put(f,0,1,9); byte[] result=f.store.apply(new ApplyPosition(1,1),original); definition=split(f.schema,100);
            var points=List.of(new ReplicatedRangeStore.PhysicalPoint(outer.fixed,helper.key(1))); assertArrayEquals(helper.key(9),f.store.readSchemaAuthorityPoints(points,new int[]{1}).values().getFirst());
            assertEquals(0,f.store.activeSnapshotPins());
            var fresh=helper.preparation(f,helper.put(f,1,2,8),helper.point(outer.fixed,2),1);
            NativeRangeImage.write(oldImage,f.store,limits);
            try(var reader=f.store.captureReadable(helper.point(outer.fixed,1));var writer=f.store.captureTracked(helper.point(outer.fixed,1));var gate=f.store.prepareTopologyFreeze(definition,admission)) {
                assertEquals(1,gate.state().writablePins()); assertFalse(gate.ready()); assertFalse(gate.whenReady().toCompletableFuture().isDone());
                assertThrows(IllegalStateException.class,()->f.store.captureTracked(helper.point(outer.fixed,2)));
                assertThrows(IllegalStateException.class,()->writer.stage(helper.put(f,2,1,7)));
                assertThrows(IllegalStateException.class,()->f.store.planReadCertification(writer,new OperationId(outer.session,1,3),0,new UUID(0,99)));
                assertThrows(IllegalStateException.class,()->f.store.estimatePreparation(fresh)); assertThrows(IllegalStateException.class,()->f.store.applyPrepared(new ApplyPosition(1,2),fresh));
                assertThrows(IllegalStateException.class,()->f.store.applyTopologyFreeze(new ApplyPosition(1,2),definition,admission));
                assertArrayEquals(helper.key(9),writer.get(outer.fixed,helper.key(1))); assertArrayEquals(helper.key(9),reader.get(outer.fixed,helper.key(1)));
                writer.close(); gate.whenReady().toCompletableFuture().get(5,TimeUnit.SECONDS); assertTrue(gate.ready()); assertTrue(f.store.activeSnapshotPins()>0,"Readonly owners do not block a topology boundary");
                freeze=f.store.applyTopologyFreeze(new ApplyPosition(1,2),definition,admission); assertTrue(f.store.admissionOpen()); assertEquals(new ApplyPosition(1,2),freeze.position());
                f.store.validate(original); assertFalse(f.store.estimateReceiptRetention(original).newReceipt());
                assertThrows(IllegalStateException.class,()->f.store.validate(helper.put(f,4,1,7))); assertThrows(IllegalStateException.class,()->f.store.apply(new ApplyPosition(1,3),helper.put(f,4,1,7)));
                assertThrows(IllegalArgumentException.class,()->f.store.validate(helper.put(f,0,1,7)));
                assertArrayEquals(result,f.store.apply(new ApplyPosition(1,3),original)); assertArrayEquals(helper.key(9),f.store.readCurrentPoint(outer.fixed,helper.key(1))); assertArrayEquals(helper.key(9),reader.get(outer.fixed,helper.key(1)));
                assertThrows(IllegalStateException.class,()->f.store.applyFinalization(new ApplyPosition(1,4),helper.abort(fresh)));
                byte[] changed=admission.clone(); changed[0]=1; assertThrows(IllegalArgumentException.class,()->f.store.applyTopologyFreeze(new ApplyPosition(1,4),definition,changed));
                assertThrows(IllegalArgumentException.class,()->f.store.applyTopologyFreeze(new ApplyPosition(2,2),definition,admission));
                assertEquals(freeze,f.store.applyTopologyFreeze(new ApplyPosition(2,4),definition,admission));
                assertEquals(TopologyChange.Phase.FENCED,TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(replica.replicaId())).phase());
            }
            NativeRangeImage.write(image,f.store,limits); f.store.fence(); assertThrows(java.io.IOException.class,()->f.store.installSnapshot(oldImage,limits)); f.store.activate();
        }
        try(var f=outer.new Fixture(source,false)) { assertEquals(freeze,f.store.topologyFreeze()); f.store.enableLogicalSchema(f.schema); assertThrows(IllegalStateException.class,()->f.store.captureTracked(helper.point(outer.fixed,1))); assertArrayEquals(helper.key(9),f.store.readCurrentPoint(outer.fixed,helper.key(1))); }
        try(var f=outer.new Fixture(root.resolve("target"),true)) { f.store.fence(); f.store.installSnapshot(image,limits); f.store.activate(); assertEquals(freeze,f.store.topologyFreeze()); assertThrows(IllegalStateException.class,()->f.store.apply(new ApplyPosition(2,5),helper.put(f,5,1,8))); try(var pin=f.store.captureForRecovery()) { assertNotNull(pin.stateView()); } }
    }
    @Test void actualBusyWritableUserMustDrainAfterOwnerCloseWithoutForcedJniRelease() throws Exception {
        try(var f=outer.new Fixture(root.resolve("busy"),true)) {
            helper.enable(f); f.store.apply(new ApplyPosition(1,1),helper.put(f,0,1,9)); var writer=f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.range(outer.fixed,helper.key(1),helper.key(2))))); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try(var executor=Executors.newSingleThreadExecutor();var gate=f.store.prepareTopologyFreeze(split(f.schema,100),admission)) {
                var visit=executor.submit(()->{ writer.visitColumn(outer.fixed,helper.key(1),helper.key(2),(key,value)->{ entered.countDown(); try { if(!release.await(10,TimeUnit.SECONDS)) throw new AssertionError("Native user release timed out"); } catch(InterruptedException failure) { throw new java.io.IOException(failure); } }); return null; });
                try { assertTrue(entered.await(5,TimeUnit.SECONDS)); writer.close(); assertFalse(gate.ready()); assertEquals(1,gate.state().writablePins()); assertFalse(gate.whenReady().toCompletableFuture().isDone()); }
                finally { release.countDown(); }
                visit.get(5,TimeUnit.SECONDS); gate.whenReady().toCompletableFuture().get(5,TimeUnit.SECONDS); assertTrue(gate.ready());
                f.store.applyTopologyFreeze(new ApplyPosition(1,2),split(f.schema,100),admission);
            } finally { writer.close(); }
        }
    }
    @Test void preparedCommitAndAbortAndSnapshotGateReleaseMustPrecedeTheStableParentCut() throws Exception {
        for(boolean commit:List.of(false,true)) try(var f=outer.new Fixture(root.resolve("prepared-"+commit),true)) {
            helper.enable(f); var preparation=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1); f.store.applyPrepared(new ApplyPosition(1,1),preparation);
            var intent=commit ? new NativeCommittedParticipantTest().commit(f.store.preparedParticipant(preparation.transactionId())) : helper.abort(preparation); var definition=split(f.schema,100);
            try(var gate=f.store.prepareTopologyFreeze(definition,admission)) {
                assertFalse(gate.ready()); assertThrows(IllegalStateException.class,()->f.store.applyTopologyFreeze(new ApplyPosition(1,2),definition,admission));
                var result=f.store.applyFinalization(new ApplyPosition(1,2),intent); gate.whenReady().toCompletableFuture().get(5,TimeUnit.SECONDS); assertTrue(gate.ready());
                f.store.applyTopologyFreeze(new ApplyPosition(1,3),definition,admission); assertEquals(result,f.store.applyFinalization(new ApplyPosition(1,4),intent)); assertNotNull(f.store.recoveredFinalization(intent));
            }
        }
        try(var f=outer.new Fixture(root.resolve("gate"),true)) {
            helper.enable(f); var snapshot=new NativeSnapshotGateTest().fence(f,1,1); f.store.applyGateInstall(new ApplyPosition(1,1),snapshot);
            try(var gate=f.store.prepareTopologyFreeze(split(f.schema,100),admission)) { assertFalse(gate.ready()); f.store.applyGateRelease(new ApplyPosition(1,2),SnapshotFenceStatus.begin(snapshot).abort()); gate.whenReady().toCompletableFuture().get(5,TimeUnit.SECONDS); f.store.applyTopologyFreeze(new ApplyPosition(1,3),split(f.schema,100),admission); }
        }
    }
    @Test void cancelledPreparationUnblocksAdmissionAndCodecRejectsMalformedOrChangedBinding() throws Exception {
        try(var f=outer.new Fixture(root.resolve("cancel"),true)) {
            helper.enable(f); var writer=f.store.captureTracked(helper.point(outer.fixed,1)); var definition=split(f.schema,100); var gate=f.store.prepareTopologyFreeze(definition,admission); var ready=gate.whenReady().toCompletableFuture(); gate.close(); assertTrue(ready.isCompletedExceptionally()); writer.close();
            try(var fresh=f.store.captureTracked(helper.point(outer.fixed,1))) { assertNotNull(fresh); }
            var freeze=f.store.applyTopologyFreeze(new ApplyPosition(1,1),definition,admission); assertEquals(freeze,NativeTopologyFreeze.decode(freeze.encode())); assertTrue(freeze.encode().length<=NativeTopologyFreeze.MAX_BYTES);
            assertThrows(IllegalArgumentException.class,()->f.store.lookupTopologyFreeze(split(f.schema,101).header(),admission));
            var corrupt=freeze.encode(); corrupt[0]^=1; assertThrows(IllegalArgumentException.class,()->NativeTopologyFreeze.decode(corrupt));
            assertThrows(IllegalArgumentException.class,()->NativeTopologyFreeze.decode(Arrays.copyOf(freeze.encode(),freeze.encode().length-1)));
            assertThrows(IllegalArgumentException.class,()->freeze.requireParent(outer.group,2,outer.domain,f.schema.digest(),null,null));
            assertThrows(IllegalArgumentException.class,()->freeze.requireParent(outer.group,1,outer.domain,f.schema.digest(),new byte[]{1},null));
            assertThrows(IllegalArgumentException.class,()->freeze.requireCut(new ApplyPosition(2,1)));
            var points=List.of(new ReplicatedRangeStore.PhysicalPoint(outer.fixed,helper.key(1)));
            assertThrows(IllegalArgumentException.class,()->f.store.readSchemaAuthorityPoints(points,new int[0]));
            assertThrows(IllegalArgumentException.class,()->f.store.readSchemaAuthorityPoints(points,new int[]{0}));
        }
    }
    @Test void mergeParentsEachCommitTheirOwnOriginalBoundaryAndWrongNativeGeometryIsRejected() throws Exception {
        var schema=outer.schema(); var second=new UUID(0,40); var before=List.of(range(20,outer.group,null,new byte[]{(byte)128}),range(21,second,new byte[]{(byte)128},null));
        var definition=new TopologyChange.Definition(new UUID(0,100),directory(schema,1,before),directory(schema,2,List.of(range(22,new UUID(0,41),null,null))));
        for(var parent:before) try(var pool=DistributedStoragePool.open(root.resolve(parent.groupId().toString()),it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()))) {
            var store=pool.openRange(new RangeOwnership.Owner(parent.groupId(),1),new RangeOwnership.Interval(outer.domain,schema.columnIds(),parent.lowerInclusive(),parent.upperExclusive()),schema.digest(),outer.names,Set.of()); store.enableLogicalSchema(schema); store.applyNoop(new ApplyPosition(1,0));
            var freeze=store.applyTopologyFreeze(new ApplyPosition(1,1),definition,admission); assertEquals(parent.groupId(),freeze.groupId()); assertEquals(new ApplyPosition(1,1),freeze.proof(replica.replicaId()).position());
        }
        try(var f=outer.new Fixture(root.resolve("wrong-geometry"),true)) { helper.enable(f);
            var parent=range(20,outer.group,null,null); var listener=new ReplicaDescriptor(new UUID(0,13),new UUID(0,14),new UUID(0,15),"listener","127.0.0.1:9998",ReplicaDescriptor.Role.LISTENER);
            var relocated=new RangeDescriptor(parent.rangeId(),outer.group,1,2,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica,listener));
            var relocation=new TopologyChange.Definition(new UUID(0,102),directory(f.schema,1,List.of(parent)),directory(f.schema,2,List.of(relocated)));
            assertThrows(IllegalArgumentException.class,()->f.store.prepareTopologyFreeze(relocation,admission));
            assertThrows(IllegalArgumentException.class,()->f.store.applyTopologyFreeze(new ApplyPosition(1,1),relocation,admission)); assertThrows(IllegalArgumentException.class,()->f.store.prepareTopologyFreeze(definition,admission)); assertThrows(IllegalArgumentException.class,()->f.store.applyTopologyFreeze(new ApplyPosition(1,1),split(f.schema,100),new byte[31])); assertThrows(IllegalStateException.class,()->f.store.applyTopologyFreeze(new ApplyPosition(1,2),split(f.schema,100),admission)); }
    }
    @Test void frozenParentAllowsExplicitAcknowledgementAndEpochAccountingWithoutFreshBusiness() throws Exception {
        try(var f=outer.new Fixture(root.resolve("accounting"),true)) {
            helper.enable(f); var original=helper.put(f,0,1,9); f.store.apply(new ApplyPosition(1,1),original); var freeze=f.store.applyTopologyFreeze(new ApplyPosition(1,2),split(f.schema,100),admission);
            var ack=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,1),List.of(),List.of(),0);
            f.store.validate(ack); assertTrue(f.store.estimateReceiptRetention(ack).newReceipt()); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.store.apply(new ApplyPosition(1,3),ack)).status());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(f.store.operationResult(original.operationId(),original.receiptDigest())).status());
            var gc=DistributedCommand.advanceSessionEpochFloor(outer.group,1,f.schema.digest(),new OperationId(new UUID(0,500),2,0),2);
            f.store.validate(gc); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.store.apply(new ApplyPosition(1,4),gc)).status());
            assertArrayEquals(helper.key(9),f.store.readCurrentPoint(outer.fixed,helper.key(1))); assertEquals(freeze,f.store.topologyFreeze());
        }
    }

    @Test void authorityPointLimitsRejectOversizedIndividualRowsBeforeReturningValues() throws Exception {
        try(var f=outer.new Fixture(root.resolve("value-bounds"),true)) {
            helper.enable(f); var command=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,0),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,helper.key(1),new byte[]{9,8})));
            f.store.apply(new ApplyPosition(1,1),command); var points=List.of(new ReplicatedRangeStore.PhysicalPoint(outer.fixed,helper.key(1)));
            assertThrows(IllegalStateException.class,()->f.store.readSchemaAuthorityPoints(points,new int[]{1}));
            assertArrayEquals(new byte[]{9,8},f.store.readSchemaAuthorityPoints(points,new int[]{2}).values().getFirst()); assertEquals(0,f.store.activeSnapshotPins());
        }
    }

}
