package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled native committed fixtures, without servers or network admission. */
class ReplicatedSchemaFenceTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativePreparedParticipantTest helper=new NativePreparedParticipantTest();
    final NativeBucketApplyTest outer=helper.outer;
    SchemaChange.Header header(DistributedSchema schema,long change) {
        var columns=new ArrayList<>(schema.columns()); columns.removeLast();
        var target=new DistributedSchema(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch()+1,schema.routingPrefixBytes(),columns);
        var replica=new ReplicaDescriptor(new UUID(0,11),new UUID(0,12),new UUID(0,13),"peer","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(new UUID(0,14),outer.group,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory=new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),1,schema.columnIds(),schema.routingPrefixBytes(),List.of(range));
        return new SchemaChange.Definition(new UUID(0,change),new OperationId(new UUID(0,15),1,change),directory,schema,target).header();
    }
    @Test void durableFenceBlocksFreshDataButRetainsExactReceiptAndOwnedHistoricalPins() throws Exception {
        NativeSchemaFence proof; SchemaChange.Header header; DistributedCommand original;
        try (var f=outer.new Fixture(root.resolve("source"),true)) {
            helper.enable(f); original=helper.put(f,0,1,9); byte[] result=f.store.apply(new ApplyPosition(1,1),original);
            header=header(f.schema,1);
            try (var raw=f.store.capture(); var scoped=f.store.captureReadable(helper.point(outer.fixed,1)); var tracked=f.store.captureTracked(helper.point(outer.fixed,1))) {
                proof=f.store.applySchemaFence(new ApplyPosition(1,2),header); assertNotNull(f.store.schemaFence());
                assertFalse(f.store.schemaFenceReady().ready()); assertEquals(3,f.store.schemaFenceReady().snapshotPins());
                assertArrayEquals(helper.key(9),raw.get(outer.fixed,helper.key(1))); assertArrayEquals(helper.key(9),scoped.get(outer.fixed,helper.key(1))); assertArrayEquals(helper.key(9),tracked.get(outer.fixed,helper.key(1)));
                assertThrows(IllegalStateException.class,() -> f.store.apply(new ApplyPosition(1,3),helper.put(f,1,1,7)));
                assertThrows(IllegalStateException.class,() -> f.store.readCurrentPoint(outer.fixed,helper.key(1)));
                assertThrows(IllegalStateException.class,f.store::capture);
                assertThrows(IllegalStateException.class,() -> f.store.captureReadable(helper.point(outer.fixed,1)));
                assertThrows(IllegalStateException.class,() -> f.store.captureTracked(helper.point(outer.fixed,1)));
                assertArrayEquals(helper.key(9),tracked.get(outer.fixed,helper.key(1)),"Rejected new admission must not invalidate owned cuts");
                f.store.validate(original);
                var estimate=f.store.estimateReceiptRetention(original); assertFalse(estimate.newReceipt()); assertEquals(0,estimate.additionalBytes());
                assertThrows(IllegalStateException.class,() -> f.store.validate(helper.put(f,1,1,7)));
                assertThrows(IllegalStateException.class,() -> f.store.estimateReceiptRetention(helper.put(f,1,1,7)));
                assertThrows(IllegalArgumentException.class,() -> f.store.validate(helper.put(f,0,1,7)));
                assertThrows(IllegalArgumentException.class,() -> f.store.estimateReceiptRetention(helper.put(f,0,1,7)));
                assertArrayEquals(result,f.store.apply(new ApplyPosition(1,3),original));
                assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(1,4),helper.put(f,0,1,7)));
                assertEquals(proof,f.store.applySchemaFence(new ApplyPosition(2,4),header));
                assertEquals(new ApplyPosition(1,2),f.store.schemaFence().position());
                assertThrows(IllegalArgumentException.class,() -> f.store.applySchemaFence(new ApplyPosition(2,5),header(f.schema,2)));
                assertThrows(IllegalArgumentException.class,() -> f.store.applySchemaFence(new ApplyPosition(2,2),header));
            }
            assertTrue(f.store.schemaFenceReady().ready());
        }
        try (var f=outer.new Fixture(root.resolve("source"),false)) {
            assertEquals(proof,f.store.schemaFence()); assertNotNull(f.store.schemaFence());
            assertThrows(IllegalStateException.class,f.store::capture); f.store.enableLogicalSchema(f.schema);
            f.store.fence(); f.store.activate(); assertNotNull(f.store.schemaFence());
            assertEquals(proof,f.store.applySchemaFence(new ApplyPosition(2,5),header));
            assertThrows(IllegalStateException.class,() -> f.store.readCurrentPoint(outer.fixed,helper.key(1)));
        }
    }
    @Test void readinessWaitsForActualNativeUserAfterOwnerClose() throws Exception {
        try (var f=outer.new Fixture(root.resolve("busy"),true)) {
            helper.enable(f); f.store.apply(new ApplyPosition(1,1),helper.put(f,0,1,9));
            var pin=f.store.capture(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try (var executor=Executors.newSingleThreadExecutor()) {
                var visiting=executor.submit(() -> { pin.visitColumn(outer.fixed,null,null,(key,value) -> { entered.countDown(); try { if (!release.await(10,TimeUnit.SECONDS)) throw new AssertionError("Release timed out"); } catch (InterruptedException e) { throw new java.io.IOException(e); } }); return null; });
                try {
                    assertTrue(entered.await(10,TimeUnit.SECONDS)); f.store.applySchemaFence(new ApplyPosition(1,2),header(f.schema,1)); pin.close();
                    assertEquals(1,f.store.activeSnapshotPins()); assertFalse(f.store.schemaFenceReady().ready());
                } finally { release.countDown(); }
                visiting.get(10,TimeUnit.SECONDS); assertEquals(0,f.store.activeSnapshotPins()); assertTrue(f.store.schemaFenceReady().ready());
            } finally { pin.close(); }
        }
    }
    @Test void previouslyPreparedCommitAndAbortCanFinishAfterFence() throws Exception {
        for (boolean commit:List.of(false,true)) try (var f=outer.new Fixture(root.resolve("finalize-"+commit),true)) {
            helper.enable(f); var preparation=helper.preparation(f,helper.put(f,0,1,9),helper.point(outer.fixed,1),1);
            f.store.applyPrepared(new ApplyPosition(1,1),preparation); var prepared=f.store.preparedParticipant(preparation.transactionId());
            var intent=commit ? new NativeCommittedParticipantTest().commit(prepared) : helper.abort(preparation);
            f.store.applySchemaFence(new ApplyPosition(1,2),header(f.schema,1)); assertFalse(f.store.schemaFenceReady().ready());
            var outcome=f.store.applyFinalization(new ApplyPosition(1,3),intent); assertNotNull(outcome); assertTrue(f.store.schemaFenceReady().ready());
            assertEquals(outcome,f.store.applyFinalization(new ApplyPosition(1,4),intent));
            assertThrows(IllegalStateException.class,f.store::capture);
        }
    }
    @Test void readinessWaitsForCommittedGateReleaseAndRejectsNewGateInstallation() throws Exception {
        try (var f=outer.new Fixture(root.resolve("gate-drain"),true)) {
            helper.enable(f); var gates=new NativeSnapshotGateTest(); var gate=gates.fence(f,1,1);
            f.store.applyGateInstall(new ApplyPosition(1,1),gate); f.store.applySchemaFence(new ApplyPosition(1,2),header(f.schema,1));
            assertFalse(f.store.schemaFenceReady().ready()); assertEquals(1,f.store.schemaFenceReady().snapshotGates());
            assertThrows(IllegalStateException.class,() -> f.store.applyGateInstall(new ApplyPosition(1,3),gates.fence(f,2,2)));
            f.store.applyGateRelease(new ApplyPosition(1,3),SnapshotFenceStatus.begin(gate).abort()); assertTrue(f.store.schemaFenceReady().ready());
            assertNotNull(f.store.schemaFence()); assertTrue(f.store.admissionOpen(),"Protected drain admission retains physical ownership");
        }
    }
    @Test void recoveryImageRetainsFenceAndCannotReinstallAnOlderUnfencedCut() throws Exception {
        var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4); Path image=root.resolve("fenced-image"),old=root.resolve("old-image"); NativeSchemaFence proof;
        try (var f=outer.new Fixture(root.resolve("image-source"),true)) {
            helper.enable(f); NativeRangeImage.write(old,f.store,limits);
            proof=f.store.applySchemaFence(new ApplyPosition(1,1),header(f.schema,1));
            NativeRangeImage.write(image,f.store,limits); assertTrue(f.store.schemaFenceReady().ready()); assertEquals(new ApplyPosition(1,1),NativeRangeImage.inspect(image,limits).position());
            f.store.fence(); assertThrows(java.io.IOException.class,() -> f.store.installSnapshot(old,limits));
            assertEquals(proof,f.store.schemaFence()); f.store.activate(); assertNotNull(f.store.schemaFence());
        }
        try (var f=outer.new Fixture(root.resolve("image-target"),true)) {
            f.store.fence(); f.store.installSnapshot(image,limits); assertEquals(proof,f.store.schemaFence());
            assertThrows(IllegalStateException.class,f.store::capture); f.store.activate(); assertNotNull(f.store.schemaFence());
            assertThrows(IllegalStateException.class,() -> f.store.readCurrentPoint(outer.fixed,helper.key(1)));
            assertThrows(java.io.IOException.class,() -> NativeRangeImage.inspectHomeRecoveryReservations(image,limits,outer.cluster,outer.database,outer.fixed));
        }
        try (var f=outer.new Fixture(root.resolve("image-target"),false)) { assertEquals(proof,f.store.schemaFence()); assertNotNull(f.store.schemaFence()); }
    }
    @Test void atomicPointReadReturnsOrderedValuesAndNullsAtOneCutAndRejectsFencedAdmission() throws Exception {
        try (var f=outer.new Fixture(root.resolve("points"),true)) {
            helper.enable(f); f.store.apply(new ApplyPosition(1,1),helper.put(f,0,1,9));
            var points=List.of(new ReplicatedRangeStore.PhysicalPoint(outer.fixed,helper.key(1)),new ReplicatedRangeStore.PhysicalPoint(outer.fixed,helper.key(2)));
            var read=f.store.readCurrentPoints(points); assertEquals(new ApplyPosition(1,1),read.position()); assertArrayEquals(helper.key(9),read.values().getFirst()); assertNull(read.values().get(1));
            read.values().getFirst()[0]=77; assertArrayEquals(helper.key(9),read.values().getFirst());
            assertThrows(IllegalArgumentException.class,() -> f.store.readCurrentPoints(List.of()));
            assertThrows(IllegalArgumentException.class,() -> f.store.readCurrentPoints(Collections.nCopies(5,points.getFirst())));
            f.store.applySchemaFence(new ApplyPosition(1,2),header(f.schema,1)); assertThrows(IllegalStateException.class,() -> f.store.readCurrentPoints(points));
        }
    }
    @Test void wrongSchemaParticipantAndCommittedPositionCannotFence() throws Exception {
        try (var f=outer.new Fixture(root.resolve("wrong"),true)) {
            helper.enable(f); var header=header(f.schema,1); assertFalse(f.store.schemaFenceReady().ready());
            assertThrows(IllegalArgumentException.class,() -> f.store.applySchemaFence(new ApplyPosition(0,1),header));
            assertThrows(IllegalStateException.class,() -> f.store.applySchemaFence(new ApplyPosition(1,2),header));
            var foreign=new DistributedSchema(new UUID(5,5),f.schema.databaseId(),f.schema.domainId(),f.schema.schemaEpoch(),f.schema.routingPrefixBytes(),f.schema.columns());
            assertThrows(IllegalArgumentException.class,() -> f.store.applySchemaFence(new ApplyPosition(1,1),header(foreign,1)));
            byte[] wrongEpoch=header.encode(); java.nio.ByteBuffer.wrap(wrongEpoch).putLong(305+32,2);
            var wrongParticipant=SchemaChange.Header.decode(wrongEpoch);
            assertThrows(IllegalArgumentException.class,() -> f.store.applySchemaFence(new ApplyPosition(1,1),wrongParticipant));
            assertNull(f.store.schemaFence()); assertTrue(f.store.admissionOpen());
        }
    }
}
