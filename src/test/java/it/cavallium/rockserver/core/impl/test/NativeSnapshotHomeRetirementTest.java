package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeSnapshotHomeRetirementTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeHomeRecoveryTest helper=new NativeHomeRecoveryTest();
    final UUID cluster=new UUID(0,20),database=new UUID(0,21),dataGroup=new UUID(0,30);
    SnapshotFence fence(long epoch) {
        return new SnapshotFence(cluster,database,new UUID(0,22),new UUID(0,100),helper.group,1,new OperationId(new UUID(0,23),epoch,0),1,1,new byte[32],List.of(
                new SnapshotFence.Participant(new UUID(0,31),dataGroup,1,new byte[32],new OperationId(new UUID(0,32),epoch,0),new OperationId(new UUID(0,32),epoch,1),new TransactionScope(List.of(TransactionScope.Span.point(new UUID(0,33),new byte[]{1}))))));
    }
    byte[] key(SnapshotFence fence) { return ByteBuffer.allocate(16).putLong(fence.fenceId().getMostSignificantBits()).putLong(fence.fenceId().getLeastSignificantBits()).array(); }
    SnapshotHomeIdentity identity(SnapshotFence fence) { return new SnapshotHomeIdentity(helper.group,1,new byte[32],helper.column,fence); }
    DistributedCommand cas(long sequence,SnapshotFenceStatus before,SnapshotFenceStatus next) {
        return new DistributedCommand(helper.group,1,new byte[32],new OperationId(helper.user,2,sequence),List.of(new Mutation(Mutation.Kind.PUT,helper.column,key(next.fence()),next.encode())),List.of(new KeyPredicate(helper.column,key(next.fence()),before==null ? null : before.encode())));
    }
    SnapshotFenceStatus consumed(SnapshotFence fence,boolean complete) {
        var status=SnapshotFenceStatus.begin(fence);
        if (complete) status=status.ready(dataGroup,new ApplyPosition(1,1)).capture(dataGroup,new SnapshotFenceStatus.Capture(new UUID(0,40),new UUID(0,41),new ApplyPosition(1,2))).complete();
        else status=status.abort();
        return status.consume();
    }
    SnapshotHomeRetirement retirement(SnapshotFenceStatus consumed) { return new SnapshotHomeRetirement(identity(consumed.fence()),consumed,List.of(new SnapshotFenceProof(consumed.fence(),dataGroup,2,new ApplyPosition(1,10)))); }
    void configure(NativeHomeRecoveryTest.Fixture f) { f.store.configureSnapshotHome(cluster,database,helper.column); }
    void floor(NativeHomeRecoveryTest.Fixture f,int index) throws Exception { helper.apply(f,index,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,2,index),2)); }

    @Test void snapshotHomeCannotRegisterIndividuallyExpiredBusinessUnderFreshControlGrant() throws Exception {
        try(var f=helper.new Fixture(root)) {
            configure(f);f.store.enableSessionControls();var business=fence(2);
            var ledger=SessionEpochLedger.initial(cluster,database,0)
                    .issue(helper.admin,1000).issue(business.operation().sessionId(),100).issue(helper.user,1000);
            var grant=ledger.grants().get(1);var control=ledger.grants().getLast();
            helper.apply(f,1,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,control.epoch(),0),RangeSessionControl.bind(ledger,grant,32),-1));
            helper.apply(f,2,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,control.epoch(),1),RangeSessionControl.bind(ledger,control,32),0));
            helper.apply(f,3,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,control.epoch(),2),RangeSessionControl.advance(ledger.advanceTime(200)),1));
            var initial=SnapshotFenceStatus.begin(business);
            var command=new DistributedCommand(helper.group,1,new byte[32],new OperationId(control.sessionId(),control.epoch(),0),
                    List.of(new Mutation(Mutation.Kind.PUT,helper.column,key(business),initial.encode())),List.of(new KeyPredicate(helper.column,key(business),null)));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,helper.apply(f,4,command).status());
            assertNull(f.store.readCurrentPoint(helper.column,key(business)));
        }
    }

    @Test void completeAndAbortCollectWithoutRewritingHistoricalReceiptsAndSurvivePinsReopenAndImage() throws Exception {
        for (boolean complete:new boolean[]{false,true}) {
            var source=root.resolve("source-"+complete); var image=root.resolve("image-"+complete); var fence=fence(1); var initial=SnapshotFenceStatus.begin(fence); var consumed=consumed(fence,complete); var retirement=retirement(consumed); var original=cas(0,null,initial);
            try (var f=helper.new Fixture(source)) {
                configure(f); assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,1,original).status()); helper.apply(f,2,cas(1,initial,consumed));
                assertThrows(IllegalStateException.class,() -> f.store.estimateSnapshotHomeRetirement(retirement)); floor(f,3);
                try (var pin=f.store.capture()) {
                    assertNull(pin.snapshotHomeFence(identity(fence))); var usage=f.store.receiptUsage(); f.store.applySnapshotHomeRetirement(new ApplyPosition(1,4),retirement);
                    assertTrue(f.store.snapshotHomeRetired(retirement)); assertNull(pin.snapshotHomeFence(identity(fence))); assertArrayEquals(consumed.encode(),pin.get(helper.column,key(fence)));
                    assertEquals(usage.retainedCount(),f.store.receiptUsage().retainedCount()); assertFalse(f.store.estimateSnapshotHomeRetirement(retirement));
                    assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,5,original).status()); assertTrue(f.store.snapshotHomeRetired(retirement));
                    f.store.applySnapshotHomeRetirement(new ApplyPosition(1,4),retirement); assertEquals(new ApplyPosition(1,5),f.store.applied()); assertThrows(IllegalArgumentException.class,() -> f.store.applySnapshotHomeRetirement(new ApplyPosition(2,5),retirement));
                }
                try (var pin=f.store.capture()) { assertTrue(pin.snapshotHomeRetired(retirement)); NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits); }
            }
            NativeRangeImage.inspectSnapshotHome(image,helper.limits,cluster,database,helper.column);
            for (boolean install:new boolean[]{false,true}) try (var f=helper.new Fixture(install ? root.resolve("target-"+complete) : source)) {
                configure(f); if (install) { f.store.fence(); f.store.installSnapshot(image,helper.limits); f.store.activate(); }
                assertTrue(f.store.snapshotHomeRetired(retirement)); assertEquals(CommandResult.Status.SESSION_EXPIRED,helper.apply(f,6,cas(2,null,initial)).status()); assertTrue(f.store.snapshotHomeRetired(retirement));
            }
        }
    }
    @Test void expiredRetainedHomeNeedsExactConsumptionAndOldGcNeverDeletesNewBusiness() throws Exception {
        try (var f=helper.new Fixture(root.resolve("families"))) {
            configure(f); var old=fence(1); var initial=SnapshotFenceStatus.begin(old); var consumed=consumed(old,false); var retirement=retirement(consumed);
            helper.apply(f,1,cas(0,null,initial)); floor(f,2); assertNull(f.store.snapshotHomeFence(identity(old))); assertThrows(IllegalArgumentException.class,() -> f.store.applySnapshotHomeRetirement(new ApplyPosition(1,3),retirement));
            helper.apply(f,3,cas(1,initial,consumed)); f.store.applySnapshotHomeRetirement(new ApplyPosition(1,4),retirement);
            var newer=fence(2); var next=SnapshotFenceStatus.begin(newer); helper.apply(f,5,cas(2,null,next)); assertTrue(f.store.snapshotHomeRetired(retirement)); f.store.applySnapshotHomeRetirement(new ApplyPosition(1,6),retirement);
            try (var pin=f.store.capture()) { assertArrayEquals(next.encode(),pin.get(helper.column,key(newer))); assertTrue(pin.snapshotHomeRetired(retirement)); }
            var changed=new SnapshotFence(newer.clusterId(),newer.databaseId(),newer.domainId(),newer.fenceId(),newer.homeGroup(),newer.homeEpoch(),newer.operation(),2,newer.schemaEpoch(),newer.schemaDigest(),newer.participants()); assertThrows(IllegalArgumentException.class,() -> f.store.snapshotHomeFence(identity(changed)));
            var schema=new byte[32]; schema[0]=1; assertThrows(IllegalArgumentException.class,() -> f.store.snapshotHomeFence(new SnapshotHomeIdentity(helper.group,1,schema,helper.column,newer)));
            assertThrows(IllegalArgumentException.class,() -> f.store.configureHomeRecoveryReservations(cluster,database,helper.column));
        }
    }
    @Test void malformedHomeUserCannotActivateOrInstallQualifiedImage() throws Exception {
        var image=root.resolve("malformed.image");
        try (var f=helper.new Fixture(root.resolve("malformed-source"))) {
            var command=new DistributedCommand(helper.group,1,new byte[32],new OperationId(helper.user,2,0),List.of(new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{1},new byte[]{2}))); helper.apply(f,1,command);
            assertThrows(IllegalArgumentException.class,() -> configure(f));
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits); }
        }
        assertThrows(java.io.IOException.class,() -> NativeRangeImage.inspectSnapshotHome(image,helper.limits,cluster,database,helper.column));
        try (var f=helper.new Fixture(root.resolve("malformed-target"))) { configure(f); f.store.fence(); assertThrows(java.io.IOException.class,() -> f.store.installSnapshot(image,helper.limits)); assertFalse(f.store.pendingInstallation()); assertEquals(new ApplyPosition(1,0),f.store.applied()); }
    }
    @Test void emptyHomeForeignAuthorityRejectsActivationExportAndPreInstallValidation() throws Exception {
        var image=root.resolve("foreign-empty.image");
        var foreign=SessionEpochLedger.initial(new UUID(0,999),database,10);
        var command=DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,foreign.nextEpoch(),0),RangeSessionControl.advance(foreign),-1);
        try (var f=helper.new Fixture(root.resolve("foreign-empty-source"))) {
            f.store.enableSessionControls(); assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,1,command).status());
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits); }
            assertThrows(IllegalArgumentException.class,() -> configure(f));
        }
        NativeRangeImage.inspect(image,helper.limits);
        assertThrows(java.io.IOException.class,() -> NativeRangeImage.inspectSnapshotHome(image,helper.limits,cluster,database,helper.column));
        try (var f=helper.new Fixture(root.resolve("foreign-empty-target"))) {
            configure(f); f.store.fence(); assertThrows(java.io.IOException.class,() -> f.store.installSnapshot(image,helper.limits));
            assertFalse(f.store.pendingInstallation()); assertEquals(new ApplyPosition(1,0),f.store.applied()); f.store.activate();
        }
        try (var f=helper.new Fixture(root.resolve("foreign-empty-live"))) {
            configure(f); f.store.enableSessionControls();
            try (var before=f.store.capture()) {
                assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,1,command).status());
                NativeRangeImage.write(root.resolve("qualified-old-cut.image"),before,f.store.ownership().interval(),helper.limits);
            }
            try (var current=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("qualified-foreign-cut.image"),current,f.store.ownership().interval(),helper.limits)); }
            f.store.fence(); assertThrows(IllegalArgumentException.class,f.store::activate);
        }
    }

}
