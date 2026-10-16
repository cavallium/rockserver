package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeSchemaLifecycleTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeSchemaInstallationTest fixture=new NativeSchemaInstallationTest();
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,256);
    DistributedSchema empty(long epoch) { return new DistributedSchema(fixture.cluster,fixture.database,fixture.domain,epoch,0,List.of()); }
    DistributedSchema populated() { return new DistributedSchema(fixture.cluster,fixture.database,fixture.domain,1,0,fixture.source().columns()); }
    ReplicatedRangeStore range(DistributedStoragePool pool,DistributedSchema schema) throws Exception {
        return pool.openRange(new RangeOwnership.Owner(fixture.group,1),new RangeOwnership.Interval(fixture.domain,schema.columnIds(),null,null),schema.digest(),schema.columnIds().isEmpty() ? Map.of() : Map.of(fixture.column,"data"),Set.of());
    }
    SchemaChange.Definition create(DistributedSchema source,DistributedSchema target) { return fixture.change(source,target,List.of(fixture.descriptor(fixture.group,null,null))); }
    SchemaChange.Definition delete(SchemaChange.Definition create,DistributedSchema target) {
        return new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),create.targetDirectory(),create.targetSchema(),target);
    }
    @Test void createActivateDataDeleteActivateAndOriginalRetriesSurviveOriginalBootstrap() throws Exception {
        var initial=empty(0); var created=populated(); var terminal=empty(2); var create=create(initial,created); var delete=delete(create,terminal);
        UUID catalogue=UUID.randomUUID(); var catalogueCut=new ApplyPosition(2,10); var path=root.resolve("lifecycle");
        NativeSchemaInstallProof firstInstall,secondInstall; NativeSchemaActivationProof firstActivation,secondActivation;
        var command=new DistributedCommand(fixture.group,1,created.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,fixture.column,new byte[]{1},new byte[]{7})));
        byte[] result; NativeRangeImage.Manifest originalImage,middleImage;
        try(var pool=fixture.open(path)) {
            var store=range(pool,initial); store.enableLogicalSchema(initial); store.applyNoop(new ApplyPosition(1,0));
            originalImage=NativeRangeImage.write(root.resolve("original.image"),store,limits);
            store.applySchemaFence(new ApplyPosition(1,1),create.header());
            try(var gate=store.prepareSchemaInstallation(create.header()); var plan=pool.prepareSchemaInstallation(store,create,Map.of(fixture.column,"data"),Set.of())) {
                assertTrue(gate.ready()); firstInstall=pool.installSchema(store,new ApplyPosition(1,2),plan).proof();
            }
            firstActivation=store.applySchemaActivation(new ApplyPosition(1,3),firstInstall,catalogue,catalogueCut);
            assertNull(store.schemaFence()); result=store.apply(new ApplyPosition(1,4),command); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(result).status());
            assertArrayEquals(new byte[]{7},store.readCurrentPoint(fixture.column,new byte[]{1}));
            middleImage=NativeRangeImage.write(root.resolve("middle.image"),store,limits);
            store.applySchemaFence(new ApplyPosition(1,5),delete.header());
            try(var gate=store.prepareSchemaInstallation(delete.header()); var plan=pool.prepareSchemaInstallation(store,delete,Map.of(),Set.of())) {
                assertTrue(gate.ready()); secondInstall=pool.installSchema(store,new ApplyPosition(1,6),plan).proof();
            }
            secondActivation=store.applySchemaActivation(new ApplyPosition(1,7),secondInstall,catalogue,new ApplyPosition(2,20));
            assertNull(store.schemaFence()); assertEquals(Set.of(),store.ownership().interval().columns());
            assertEquals(firstInstall.fence(),store.applySchemaFence(new ApplyPosition(1,8),create.header())); assertNull(store.schemaFence());
            assertEquals(firstInstall,pool.installSchema(store,new ApplyPosition(1,9),create,Map.of(fixture.column,"data"),Set.of()).proof());
            assertEquals(firstActivation,store.applySchemaActivation(new ApplyPosition(1,10),firstInstall,catalogue,catalogueCut));
            assertArrayEquals(result,store.apply(new ApplyPosition(1,11),command));
            assertArrayEquals(terminal.digest(),store.schemaDigest()); assertNull(store.schemaFence());
            assertTrue(store.qualifiedHistoricalImage(originalImage)); assertTrue(store.qualifiedHistoricalImage(middleImage));
            assertEquals(2,store.activatedSchemas().size());
            NativeRangeImage.write(root.resolve("terminal.image"),store,limits); NativeRangeImage.inspect(root.resolve("terminal.image"),limits);
        }
        try(var pool=fixture.open(path)) {
            var store=range(pool,initial); assertArrayEquals(terminal.digest(),store.schemaDigest()); assertNull(store.schemaFence());
            assertEquals(secondActivation,store.schemaActivation()); assertEquals(firstInstall,store.lookupSchemaInstallation(create.header()));
            assertEquals(firstActivation,store.lookupSchemaActivation(create.header())); assertEquals(2,store.activatedSchemas().size());
            assertArrayEquals(result,store.apply(new ApplyPosition(2,12),command));
            assertTrue(store.qualifiedHistoricalImage(originalImage)); assertTrue(store.qualifiedHistoricalImage(middleImage));
            try(var pin=store.capture()) { assertTrue(pin.columns().isEmpty()); }
        }
        try(var pool=fixture.open(root.resolve("import"))) {
            var store=range(pool,terminal); store.enableLogicalSchema(terminal); store.fence(); store.installSnapshot(root.resolve("terminal.image"),limits); store.activate();
            assertNull(store.schemaFence()); assertEquals(secondActivation,store.schemaActivation()); assertEquals(firstActivation,store.lookupSchemaActivation(create.header()));
            assertArrayEquals(result,store.apply(new ApplyPosition(2,12),command)); assertEquals(2,store.activatedSchemas().size());
        }
    }
    @Test void captureGateWaitsForActualNativeUseAndObserverCancellationDoesNotReleaseIt() throws Exception {
        var source=fixture.source(); var target=empty(1); var change=create(source,target);
        try(var pool=fixture.open(root.resolve("capture"))) {
            var store=range(pool,source); store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0));
            var pin=store.captureForRecovery(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try(var executor=Executors.newSingleThreadExecutor()) {
                var reading=executor.submit(() -> { pin.visitState((key,value) -> { if(entered.getCount()!=0) { entered.countDown(); try { if(!release.await(10,TimeUnit.SECONDS)) throw new AssertionError("Release timeout"); } catch(InterruptedException failure) { throw new java.io.IOException(failure); } } }); return null; });
                assertTrue(entered.await(10,TimeUnit.SECONDS)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
                try(var gate=store.prepareSchemaInstallation(change.header()); var other=store.prepareSchemaInstallation(change.header())) {
                    var cancelled=gate.readyFuture().toCompletableFuture(); assertTrue(cancelled.cancel(false)); assertFalse(other.ready());
                    pin.close(); assertEquals(1,store.activeSnapshotPins()); assertThrows(IllegalStateException.class,store::captureForRecovery);
                    var ready=other.readyFuture().toCompletableFuture(); assertFalse(ready.isDone()); release.countDown(); reading.get(10,TimeUnit.SECONDS); ready.get(10,TimeUnit.SECONDS);
                    assertTrue(other.ready()); assertEquals(0,store.activeSnapshotPins()); assertThrows(IllegalStateException.class,store::captureForRecovery);
                    pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(),Set.of());
                } finally { release.countDown(); pin.close(); }
                try(var recovery=store.captureForRecovery()) { assertTrue(recovery.columns().isEmpty()); }
            } finally { release.countDown(); pin.close(); }
        }
    }
    @Test void historicalSessionControlSavesStaleWithoutClockBindingAckOrFloorEffects() throws Exception {
        var initial=empty(0); var created=populated(); var change=create(initial,created); UUID catalogue=UUID.randomUUID(),admin=UUID.randomUUID(),user=UUID.randomUUID();
        var ledger=SessionEpochLedger.initial(fixture.cluster,fixture.database,10); var issued=ledger.issue(user,100); var advanced=issued.advanceTime(200);
        var oldSuccess=DistributedCommand.sessionControl(fixture.group,1,initial.digest(),new OperationId(admin,1,0),RangeSessionControl.advance(ledger),-1);
        var stale=DistributedCommand.sessionControl(fixture.group,1,initial.digest(),new OperationId(admin,3,100),RangeSessionControl.advance(advanced),99);
        var future=advanced.issue(UUID.randomUUID(),300);
        var staleBinding=DistributedCommand.sessionControl(fixture.group,1,initial.digest(),new OperationId(admin,3,101),RangeSessionControl.bind(future,future.grants().getLast(),4),100);
        var path=root.resolve("stale-control");
        try(var pool=fixture.open(path)) {
            var store=range(pool,initial); store.enableLogicalSchema(initial); store.enableSessionControls(); store.applyNoop(new ApplyPosition(1,0));
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,1),oldSuccess)).status());
            store.applySchemaFence(new ApplyPosition(1,2),change.header()); assertFalse(store.canonicalStaleSchemaControl(stale)); store.validate(stale);
            var installed=pool.installSchema(store,new ApplyPosition(1,3),change,Map.of(fixture.column,"data"),Set.of()).proof();
            assertTrue(store.pendingSchemaControl(stale)); assertFalse(store.canonicalStaleSchemaControl(stale));
            var pendingClock=DistributedCommand.sessionControl(fixture.group,1,initial.digest(),new OperationId(admin,2,0),RangeSessionControl.advance(ledger),-1);
            var targetClock=DistributedCommand.sessionControl(fixture.group,1,created.digest(),new OperationId(admin,2,1),RangeSessionControl.advance(ledger),-1);
            try(var gate=store.prepareSchemaInstallation(change.header())) {
                store.validate(pendingClock); store.estimateReceiptRetention(pendingClock,true);
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,4),pendingClock)).status());
                store.validate(targetClock); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,5),targetClock)).status());
            }
            store.applySchemaActivation(new ApplyPosition(1,6),installed,catalogue,new ApplyPosition(2,20));
            assertTrue(store.canonicalStaleSchemaControl(stale)); store.validate(stale);
            var estimate=store.estimateReceiptRetention(stale,true); assertEquals(48+33+new CommandResult(CommandResult.Status.STALE_SCHEMA,List.of()).encode().length,estimate.additionalBytes()); assertEquals(1,estimate.additionalCount()); assertFalse(estimate.maintenance());
            assertEquals(CommandResult.Status.STALE_SCHEMA,CommandResult.decode(store.apply(new ApplyPosition(1,7),stale)).status());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,8),oldSuccess)).status());
            assertEquals(CommandResult.Status.STALE_SCHEMA,CommandResult.decode(store.apply(new ApplyPosition(1,9),stale)).status());
            store.validate(staleBinding); assertEquals(CommandResult.Status.STALE_SCHEMA,CommandResult.decode(store.apply(new ApplyPosition(1,10),staleBinding)).status());
            try(var pin=store.captureForRecovery()) {
                assertEquals(SessionAuthority.from(ledger),pin.stateView().sessionAuthority()); assertEquals(1,pin.stateView().minimumSessionEpoch());
                pin.visitState((key,value) -> { if(key.length>16) { assertNotEquals(0x14,key[16]&255,"Stale binding must not appear"); assertNotEquals(2,key[16]&255,"Stale ACK must not write a floor"); } });
            }
            assertThrows(IllegalArgumentException.class,() -> store.apply(new ApplyPosition(1,11),DistributedCommand.sessionControl(fixture.group,1,initial.digest(),stale.operationId(),RangeSessionControl.advance(ledger),99)));
            assertEquals(0,store.estimateReceiptRetention(stale).additionalBytes());
            NativeRangeImage.write(root.resolve("stale.image"),store,limits); NativeRangeImage.inspect(root.resolve("stale.image"),limits);
        }
        try(var pool=fixture.open(path)) {
            var store=range(pool,initial); store.enableSessionControls(); assertEquals(CommandResult.Status.STALE_SCHEMA,CommandResult.decode(store.apply(new ApplyPosition(2,11),stale)).status());
            try(var pin=store.captureForRecovery()) { assertEquals(SessionAuthority.from(ledger),pin.stateView().sessionAuthority()); assertEquals(1,pin.stateView().minimumSessionEpoch()); }
        }
    }
    @Test void delayedLocalFollowerCatchesUpAcrossEmptyColumnCycleAndOriginalManifestOrdering() throws Exception {
        var initial=empty(0); var created=populated(); var terminal=empty(2); UUID other=UUID.randomUUID(),catalogue=UUID.randomUUID(); byte[] boundary={(byte)128};
        var create=fixture.change(initial,created,List.of(fixture.descriptor(fixture.group,null,boundary),fixture.descriptor(other,boundary,null))); var delete=delete(create,terminal);
        var path=root.resolve("delayed-follower");
        try(var pool=fixture.open(path)) {
            var ahead=pool.openRange(new RangeOwnership.Owner(fixture.group,1),new RangeOwnership.Interval(fixture.domain,Set.of(),null,boundary),initial.digest(),Map.of(),Set.of());
            var behind=pool.openRange(new RangeOwnership.Owner(other,1),new RangeOwnership.Interval(fixture.domain,Set.of(),boundary,null),initial.digest(),Map.of(),Set.of());
            ahead.enableLogicalSchema(initial); behind.enableLogicalSchema(initial); ahead.applyNoop(new ApplyPosition(1,0)); behind.applyNoop(new ApplyPosition(1,0));
            ahead.applySchemaFence(new ApplyPosition(1,1),create.header()); var installed=pool.installSchema(ahead,new ApplyPosition(1,2),create,Map.of(fixture.column,"data"),Set.of()).proof();
            // Catalogue authority comes from the group leaders; this local other replica is deliberately behind.
            ahead.applySchemaActivation(new ApplyPosition(1,3),installed,catalogue,new ApplyPosition(2,10));
            ahead.applySchemaFence(new ApplyPosition(1,4),delete.header()); var deleted=pool.installSchema(ahead,new ApplyPosition(1,5),delete,Map.of(),Set.of()).proof();
            ahead.applySchemaActivation(new ApplyPosition(1,6),deleted,catalogue,new ApplyPosition(2,20)); assertArrayEquals(terminal.digest(),ahead.schemaDigest());
        }
        try(var pool=fixture.open(path)) {
            var ahead=pool.openRange(new RangeOwnership.Owner(fixture.group,1),new RangeOwnership.Interval(fixture.domain,Set.of(),null,boundary),initial.digest(),Map.of(),Set.of());
            var behind=pool.openRange(new RangeOwnership.Owner(other,1),new RangeOwnership.Interval(fixture.domain,Set.of(),boundary,null),initial.digest(),Map.of(),Set.of()); behind.enableLogicalSchema(initial);
            assertArrayEquals(terminal.digest(),ahead.schemaDigest()); assertArrayEquals(initial.digest(),behind.schemaDigest());
            behind.applySchemaFence(new ApplyPosition(1,1),create.header()); var installed=pool.installSchema(behind,new ApplyPosition(1,2),create,Map.of(fixture.column,"data"),Set.of()).proof();
            behind.applySchemaActivation(new ApplyPosition(1,3),installed,catalogue,new ApplyPosition(2,10));
            behind.applySchemaFence(new ApplyPosition(1,4),delete.header()); var deleted=pool.installSchema(behind,new ApplyPosition(1,5),delete,Map.of(),Set.of()).proof();
            behind.applySchemaActivation(new ApplyPosition(1,6),deleted,catalogue,new ApplyPosition(2,20)); assertArrayEquals(terminal.digest(),behind.schemaDigest());
        }
        try(var pool=fixture.open(path)) {
            // Reverse opening order still resolves the original bootstrap through the complete chain.
            var behind=pool.openRange(new RangeOwnership.Owner(other,1),new RangeOwnership.Interval(fixture.domain,Set.of(),boundary,null),initial.digest(),Map.of(),Set.of());
            var ahead=pool.openRange(new RangeOwnership.Owner(fixture.group,1),new RangeOwnership.Interval(fixture.domain,Set.of(),null,boundary),initial.digest(),Map.of(),Set.of());
            assertArrayEquals(terminal.digest(),ahead.schemaDigest()); assertArrayEquals(terminal.digest(),behind.schemaDigest());
        }
    }
    @Test void asyncCleanupCancellationRetainsOptionsUntilActualCloseAndShutdownDrainsRemainingPlans() throws Exception {
        var initial=empty(0); var change=create(initial,populated()); var pool=fixture.open(root.resolve("cleanup"));
        try(pool) {
        var store=range(pool,initial); store.enableLogicalSchema(initial); store.applyNoop(new ApplyPosition(1,0)); store.applySchemaFence(new ApplyPosition(1,1),change.header());
        var capture=store.prepareSchemaInstallation(change.header()); var plan=pool.prepareSchemaInstallation(store,change,Map.of(fixture.column,"data"),Set.of());
        var optionsField=plan.getClass().getDeclaredField("options"); optionsField.setAccessible(true); var prepared=((Map<?,?>)optionsField.get(plan)).values().iterator().next();
        var preparedOptions=prepared.getClass().getDeclaredField("options"); preparedOptions.setAccessible(true); var nativeOptions=((RocksDBLoader.ColumnOptionsWithMerge)preparedOptions.get(prepared)).options();
        var plansField=DistributedStoragePool.class.getDeclaredField("schemaPlans"); plansField.setAccessible(true);
        var mutationField=DistributedStoragePool.class.getDeclaredField("nativeMutation"); mutationField.setAccessible(true); var mutation=(Semaphore)mutationField.get(pool);
        mutation.acquire();
        try {
            var observer=pool.submitSchemaCleanup(plan,capture).toCompletableFuture(); assertTrue(observer.cancel(false));
            assertTrue(nativeOptions.isOwningHandle()); assertEquals(1,((Set<?>)plansField.get(pool)).size());
        } finally { mutation.release(); }
        pool.submitSchemaCleanup(plan,capture).toCompletableFuture().get(10,TimeUnit.SECONDS);
        assertFalse(nativeOptions.isOwningHandle()); assertEquals(0,((Set<?>)plansField.get(pool)).size());
        var secondCapture=store.prepareSchemaInstallation(change.header()); var secondPlan=pool.prepareSchemaInstallation(store,change,Map.of(fixture.column,"data"),Set.of());
        var secondPrepared=((Map<?,?>)optionsField.get(secondPlan)).values().iterator().next(); var secondOptions=((RocksDBLoader.ColumnOptionsWithMerge)preparedOptions.get(secondPrepared)).options();
        pool.close(); assertFalse(secondOptions.isOwningHandle()); assertEquals(0,((Set<?>)plansField.get(pool)).size());
        pool.submitSchemaCleanup(secondPlan,secondCapture).toCompletableFuture().get(10,TimeUnit.SECONDS);
        }
    }

}
