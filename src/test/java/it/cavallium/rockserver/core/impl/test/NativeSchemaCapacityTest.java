package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class NativeSchemaCapacityTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeSchemaInstallationTest fixture=new NativeSchemaInstallationTest();
    DistributedSchema empty(UUID domain,long epoch) { return new DistributedSchema(fixture.cluster,fixture.database,domain,epoch,0,List.of()); }
    DistributedSchema populated(DistributedSchema source,UUID column) {
        return new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch()+1,0,List.of(new DistributedSchema.Column(1,column,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
    }
    ReplicatedRangeStore range(DistributedStoragePool pool,UUID group,DistributedSchema original,Map<UUID,String> names) throws Exception {
        return range(pool,group,original,names,null,null);
    }
    ReplicatedRangeStore range(DistributedStoragePool pool,UUID group,DistributedSchema original,Map<UUID,String> names,byte[] lo,byte[] hi) throws Exception {
        var store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(original.domainId(),original.columnIds(),lo,hi),original.digest(),names,Set.of());
        store.enableLogicalSchema(store.installedSchema()==null ? original : store.installedSchema()); return store;
    }
    RocksDBLoader.LoadedDb loaded(DistributedStoragePool pool) throws Exception { var field=DistributedStoragePool.class.getDeclaredField("loaded"); field.setAccessible(true); return (RocksDBLoader.LoadedDb)field.get(pool); }
    DistributedStoragePool qualified(Path path,DistributedSchema original,Map<UUID,String> plain) { return DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),original,Map.of(),plain); }
    SchemaChange.Definition change(DistributedSchema source,DistributedSchema target,UUID group) {
        var directory=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.digest(),0,source.columnIds(),source.routingPrefixBytes(),List.of(fixture.descriptor(group,null,null)));
        return new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),directory,source,target);
    }
    SchemaCapacityRelease abort(SchemaChange.Definition definition) { return SchemaCapacityRelease.aborted(SchemaChange.Status.begin(definition.header()).abort(),UUID.randomUUID(),new ApplyPosition(1,1)); }
    void seedPermanentBirths(Path path,DistributedSchema source,Map<UUID,String> permanent) throws Exception {
        try(var pool=qualified(path,source,permanent)) {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(permanent.size());
            for(var entry:permanent.entrySet()) { out.writeLong(entry.getKey().getMostSignificantBits()); out.writeLong(entry.getKey().getLeastSignificantBits()); out.writeLong(0); out.writeLong(0); out.writeByte(1); out.writeBoolean(false); out.writeUTF(entry.getValue()); }
            var db=loaded(pool); var state=db.db().getStartupColumns().entrySet().stream().filter(entry -> Arrays.equals(entry.getKey().getName(),DistributedStoragePool.STATE_COLUMN.getBytes())).findFirst().orElseThrow().getValue();
            try(var writes=new org.rocksdb.WriteOptions().setSync(true)) { db.db().get().put(state,writes,"CF-BIRTH".getBytes(),bytes.toByteArray()); }
        }
    }
    @Test void durableReservationKeepsDataAndCaptureOpenAndTransfersOnlyWithOriginalInstall() throws Exception {
        var source=empty(fixture.domain,0); var target=populated(source,fixture.column); var change=change(source,target,fixture.group); var path=root.resolve("reservation"); NativeSchemaCapacityReservation proof;
        try(var pool=fixture.open(path)) {
            var store=range(pool,fixture.group,source,Map.of()); store.applyNoop(new ApplyPosition(1,0));
            var absent=pool.lookupSchemaCapacityStatus(store,change,"future",false); assertEquals(DistributedStoragePool.SchemaCapacityState.NONE,absent.state()); assertNull(absent.reservation()); assertFalse(absent.forcedDurable()); assertFalse(absent.ready());
            assertThrows(IllegalStateException.class,() -> pool.requireSchemaFenceCapacity(store,change.header()));
            proof=pool.reserveSchemaCapacity(store,change,Map.of(fixture.column,"future"),Set.of());
            assertEquals(proof,NativeSchemaCapacityReservation.decode(proof.encode())); assertNull(store.schemaFence());
            var active=pool.lookupSchemaCapacityStatus(store,change,"future",false); assertEquals(proof,active.reservation()); assertEquals(DistributedStoragePool.SchemaCapacityState.ACTIVE,active.state()); assertTrue(active.forcedDurable()); assertTrue(active.ready()); assertEquals(proof,pool.requireSchemaFenceCapacity(store,change.header()));
            try(var pin=store.captureForRecovery()) { assertTrue(pin.columns().isEmpty()); }
            store.applyNoop(new ApplyPosition(1,1)); assertFalse(loaded(pool).definitiveColumnFamilyOptionsMap().containsKey("future"));
            assertEquals(proof,pool.reserveSchemaCapacity(store,change,Map.of(fixture.column,"future"),Set.of()));
            assertThrows(IllegalArgumentException.class,() -> pool.reserveSchemaCapacity(store,change,Map.of(fixture.column,"changed"),Set.of()));
        }
        try(var pool=fixture.open(path)) {
            var store=range(pool,fixture.group,source,Map.of()); assertEquals(proof,pool.lookupSchemaCapacity(store,change.header())); assertNull(store.schemaFence());
            pool.applySchemaFence(store,new ApplyPosition(2,2),change.header(),proof);
            var fenced=pool.lookupSchemaCapacityStatus(store,change,"future",false); assertEquals(DistributedStoragePool.SchemaCapacityState.FENCED,fenced.state()); assertTrue(fenced.forcedDurable()); assertFalse(fenced.ready()); assertEquals(proof,pool.requireSchemaFenceCapacity(store,change.header()));
            assertThrows(IllegalStateException.class,() -> pool.releaseSchemaCapacity(store,proof,abort(change)));
            var installed=pool.installSchema(store,new ApplyPosition(2,3),change,Map.of(fixture.column,"future"),Set.of());
            pool.releaseSchemaCapacity(store,proof,SchemaCapacityRelease.completed(installed.proof()));
            UUID catalogue=UUID.randomUUID(); store.applySchemaActivation(new ApplyPosition(2,4),installed.proof(),catalogue,new ApplyPosition(2,1));
            var consumed=pool.lookupSchemaCapacityStatus(store,change,"future",false); assertEquals(proof,consumed.reservation()); assertEquals(DistributedStoragePool.SchemaCapacityState.CONSUMED,consumed.state()); assertTrue(consumed.forcedDurable()); assertFalse(consumed.ready()); assertThrows(IllegalStateException.class,() -> pool.requireSchemaFenceCapacity(store,change.header()));
            assertThrows(IllegalArgumentException.class,() -> pool.lookupSchemaCapacityStatus(store,change,"changed",false)); assertThrows(IllegalArgumentException.class,() -> pool.lookupSchemaCapacityStatus(store,change,"future",true));
            var changedDefinition=new SchemaChange.Definition(change.changeId(),new OperationId(UUID.randomUUID(),1,0),change.sourceDirectory(),source,target);
            assertThrows(IllegalArgumentException.class,() -> pool.lookupSchemaCapacityStatus(store,changedDefinition,"future",false));
            assertThrows(IllegalArgumentException.class,() -> pool.requireSchemaFenceCapacity(store,changedDefinition.header()));
            var retired=SchemaChange.Status.begin(change.header()).beginFencing(new byte[32]).fenced(fixture.group,installed.fence().position()).installed(fixture.group,installed.position()).publish().activated(fixture.group,new ApplyPosition(2,4)).retire();
            pool.releaseSchemaCapacity(store,proof,SchemaCapacityRelease.retired(retired,catalogue,new ApplyPosition(2,20)));
            var status=pool.lookupSchemaCapacityStatus(store,change,"future",false); assertEquals(DistributedStoragePool.SchemaCapacityState.RETIRED,status.state()); assertFalse(status.ready());
            assertEquals(proof,pool.reserveSchemaCapacity(store,change,Map.of(fixture.column,"future"),Set.of()));
        }
        try(var pool=fixture.open(path)) { var store=range(pool,fixture.group,source,Map.of()); assertEquals(proof,pool.lookupSchemaCapacity(store,change.header())); assertArrayEquals(target.digest(),store.schemaDigest());
            var status=pool.lookupSchemaCapacityStatus(store,change,"future",false); assertEquals(proof,status.reservation()); assertEquals(DistributedStoragePool.SchemaCapacityState.RETIRED,status.state()); assertTrue(status.forcedDurable()); assertFalse(status.ready()); assertThrows(IllegalStateException.class,() -> pool.requireSchemaFenceCapacity(store,change.header()));
            assertThrows(IllegalArgumentException.class,() -> pool.lookupSchemaCapacityStatus(store,change,"changed",false));
        }
    }
    @Test void lastPermanentPoolSlotIsSharedAcrossGroupsAndBlocksConcurrentOtherDomainsUntilAuthoritativeAbort() throws Exception {
        var source=empty(fixture.domain,0); var path=root.resolve("last-pool-slot"); var permanent=new HashMap<UUID,String>();
        for(int i=0;i<4095;i++) permanent.put(UUID.randomUUID(),"past-"+i);
        seedPermanentBirths(path,source,permanent);
        UUID second=UUID.randomUUID(),other=UUID.randomUUID(),otherDomain=UUID.randomUUID(),otherColumn=UUID.randomUUID();
        var target=populated(source,fixture.column); var shared=fixture.change(source,target,List.of(fixture.descriptor(fixture.group,null,new byte[]{10}),fixture.descriptor(second,new byte[]{10},null)));
        var foreignSource=empty(otherDomain,0); var foreign=change(foreignSource,populated(foreignSource,otherColumn),other);
        NativeSchemaCapacityReservation first,secondProof;
        try(var pool=qualified(path,source,permanent)) {
            var a=range(pool,fixture.group,source,Map.of(),null,new byte[]{10}); var b=range(pool,second,source,Map.of(),new byte[]{10},null); var c=range(pool,other,foreignSource,Map.of());
            first=pool.reserveSchemaCapacity(a,shared,Map.of(fixture.column,"last"),Set.of()); secondProof=pool.reserveSchemaCapacity(b,shared,Map.of(fixture.column,"last"),Set.of());
            assertNotEquals(first.participant(),secondProof.participant()); assertArrayEquals(first.bindingDigest(),secondProof.bindingDigest());
            assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(c,foreign,Map.of(otherColumn,"stolen"),Set.of()));
            pool.releaseSchemaCapacity(a,first,abort(shared)); assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(c,foreign,Map.of(otherColumn,"stolen"),Set.of()));
        }
        try(var pool=qualified(path,source,permanent)) {
            var a=range(pool,fixture.group,source,Map.of(),null,new byte[]{10}); var b=range(pool,second,source,Map.of(),new byte[]{10},null); var c=range(pool,other,foreignSource,Map.of());
            assertEquals(first,pool.lookupSchemaCapacity(a,shared.header())); pool.releaseSchemaCapacity(b,secondProof,abort(shared));
            UUID racerGroup=UUID.randomUUID(),racerColumn=UUID.randomUUID(); var racerSource=empty(UUID.randomUUID(),0); var racer=change(racerSource,populated(racerSource,racerColumn),racerGroup); var d=range(pool,racerGroup,racerSource,Map.of());
            var start=new CountDownLatch(1);
            try(var executor=Executors.newFixedThreadPool(2)) {
                var one=executor.submit(() -> { start.await(); try { return (Object)pool.reserveSchemaCapacity(c,foreign,Map.of(otherColumn,"stolen"),Set.of()); } catch(IllegalStateException failure) { return failure; } });
                var two=executor.submit(() -> { start.await(); try { return (Object)pool.reserveSchemaCapacity(d,racer,Map.of(racerColumn,"racer"),Set.of()); } catch(IllegalStateException failure) { return failure; } });
                start.countDown(); var results=List.of(one.get(10,TimeUnit.SECONDS),two.get(10,TimeUnit.SECONDS));
                assertEquals(1,results.stream().filter(value -> value instanceof NativeSchemaCapacityReservation).count()); assertEquals(1,results.stream().filter(value -> value instanceof IllegalStateException).count());
            }
            assertThrows(IllegalStateException.class,() -> pool.applySchemaFence(a,new ApplyPosition(1,0),shared.header(),first));
        }
    }
    @Test void unknownForceBeforeAndAfterPersistenceRequiresReforceAndNeverDonatesCapacity() throws Exception {
        for(boolean after:List.of(false,true)) {
            var source=empty(fixture.domain,0); var target=populated(source,fixture.column); var definition=change(source,target,fixture.group); var path=root.resolve("force-"+after);
            NativeSchemaCapacityReservation original;
            try(var pool=fixture.open(path)) {
                var store=range(pool,fixture.group,source,Map.of()); pool.schemaCapacityForceObserver(boundary -> { if(boundary==after) throw new org.rocksdb.RocksDBException("controlled force"); });
                assertThrows(org.rocksdb.RocksDBException.class,() -> pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"future"),Set.of()));
                original=pool.lookupSchemaCapacity(store,definition.header()); assertNotNull(original); assertNull(store.schemaFence());
                var unknown=pool.lookupSchemaCapacityStatus(store,definition,"future",false); assertEquals(original,unknown.reservation()); assertEquals(DistributedStoragePool.SchemaCapacityState.ACTIVE,unknown.state()); assertFalse(unknown.forcedDurable()); assertFalse(unknown.ready()); assertThrows(IllegalStateException.class,() -> pool.requireSchemaFenceCapacity(store,definition.header()));
                assertThrows(IllegalStateException.class,() -> pool.applySchemaFence(store,new ApplyPosition(1,0),definition.header(),original));
                pool.schemaCapacityForceObserver(boundary -> {}); assertEquals(original,pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"future"),Set.of())); assertTrue(pool.lookupSchemaCapacityStatus(store,definition,"future",false).ready()); assertEquals(original,pool.requireSchemaFenceCapacity(store,definition.header()));
            }
            try(var pool=fixture.open(path)) { var store=range(pool,fixture.group,source,Map.of()); assertEquals(original,pool.lookupSchemaCapacity(store,definition.header())); pool.releaseSchemaCapacity(store,original,abort(definition)); var released=pool.lookupSchemaCapacityStatus(store,definition,"future",false); assertEquals(original,released.reservation()); assertEquals(DistributedStoragePool.SchemaCapacityState.ABORTED,released.state()); assertTrue(released.forcedDurable()); assertFalse(released.ready()); assertThrows(IllegalStateException.class,() -> pool.requireSchemaFenceCapacity(store,definition.header())); assertThrows(IllegalStateException.class,() -> store.applySchemaFence(new ApplyPosition(1,0),definition.header())); }
        }
    }
    @Test void finalHistorySlotIsReservedBeforeFenceAndRecoveredWithoutClosingBusinessData() throws Exception {
        var source=empty(fixture.domain,0); var path=root.resolve("history-slot"); SchemaChange.Definition next,competitor,exhausted; NativeSchemaCapacityReservation reserved; long index=0; DistributedSchema current=source;
        var image=root.resolve("full-history.image"); var limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,256);
        try(var pool=fixture.open(path)) {
            var store=range(pool,fixture.group,source,Map.of()); store.applyNoop(new ApplyPosition(1,index)); SchemaChange.Definition previous=null;
            for(int n=0;n<63;n++) {
                var target=current.columnIds().isEmpty() ? populated(current,UUID.randomUUID()) : empty(fixture.domain,current.schemaEpoch()+1);
                var definition=previous==null ? change(current,target,fixture.group) : new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),previous.targetDirectory(),current,target);
                store.applySchemaFence(new ApplyPosition(1,++index),definition.header()); var names=new HashMap<UUID,String>(); for(var column:target.columnIds()) names.put(column,"column-"+column);
                var installed=pool.installSchema(store,new ApplyPosition(1,++index),definition,names,Set.of()); store.applySchemaActivation(new ApplyPosition(1,++index),installed.proof(),UUID.randomUUID(),new ApplyPosition(1,n+1)); previous=definition; current=target;
            }
            var target=empty(fixture.domain,current.schemaEpoch()+1); next=new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),previous.targetDirectory(),current,target);
            competitor=new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),previous.targetDirectory(),current,target);
            reserved=pool.reserveSchemaCapacity(store,next,Map.of(),Set.of()); assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(store,competitor,Map.of(),Set.of()));
            var column=current.columnIds().iterator().next(); var put=new DistributedCommand(fixture.group,1,current.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7})));
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,++index),put)).status()); try(var pin=store.capture()) { assertTrue(pin.columns().contains(column)); }
        }
        try(var pool=fixture.open(path)) {
            var store=range(pool,fixture.group,source,Map.of()); assertEquals(reserved,pool.lookupSchemaCapacity(store,next.header()));
            assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(store,competitor,Map.of(),Set.of()));
            pool.releaseSchemaCapacity(store,reserved,abort(next)); var replacement=pool.reserveSchemaCapacity(store,competitor,Map.of(),Set.of());
            pool.applySchemaFence(store,new ApplyPosition(2,++index),competitor.header(),replacement); var installed=pool.installSchema(store,new ApplyPosition(2,++index),competitor,Map.of(),Set.of());
            store.applySchemaActivation(new ApplyPosition(2,++index),installed.proof(),UUID.randomUUID(),new ApplyPosition(2,64));
            var last=populated(store.installedSchema(),UUID.randomUUID()); exhausted=new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),competitor.targetDirectory(),store.installedSchema(),last);
            assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(store,exhausted,Map.of(last.columnIds().iterator().next(),"over-limit"),Set.of())); assertNull(store.schemaFence());
            NativeRangeImage.write(image,store,limits);
        }
        try(var pool=fixture.open(root.resolve("same-generation-recipient"))) {
            var store=range(pool,fixture.group,exhausted.sourceSchema(),Map.of()); store.applyNoop(new ApplyPosition(1,0));
            var column=exhausted.targetSchema().columnIds().iterator().next(); var names=Map.of(column,"reserved"); var promise=pool.reserveSchemaCapacity(store,exhausted,names,Set.of());
            try(var pin=store.capture()) { assertTrue(pin.columns().isEmpty()); }
            store.fence(); assertThrows(IOException.class,() -> store.installSnapshot(image,limits));
            assertEquals(new ApplyPosition(1,0),store.applied()); assertNull(store.schemaInstallation()); assertEquals(promise,pool.lookupSchemaCapacity(store,exhausted.header()));
            store.activate(); pool.applySchemaFence(store,new ApplyPosition(1,1),exhausted.header(),promise);
            var installed=pool.installSchema(store,new ApplyPosition(1,2),exhausted,names,Set.of()); assertArrayEquals(exhausted.targetSchema().digest(),store.schemaDigest());
            pool.releaseSchemaCapacity(store,promise,SchemaCapacityRelease.completed(installed.proof()));
        }
    }
    @Test void sameCurrentSnapshotCannotDiscardConsumedReservationInstallHistory() throws Exception {
        var source=empty(fixture.domain,0); var middle=populated(source,fixture.column); var terminal=empty(fixture.domain,2);
        var first=change(source,middle,fixture.group); var second=new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),first.targetDirectory(),middle,terminal);
        UUID catalogue=UUID.randomUUID(); var path=root.resolve("consumed-recipient"); var image=root.resolve("short-history.image"); var limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,256);
        NativeSchemaCapacityReservation capacity; NativeSchemaInstallProof original;
        try(var pool=fixture.open(path)) {
            var store=range(pool,fixture.group,source,Map.of()); store.applyNoop(new ApplyPosition(1,0));
            capacity=pool.reserveSchemaCapacity(store,first,Map.of(fixture.column,"data"),Set.of()); pool.applySchemaFence(store,new ApplyPosition(1,1),first.header(),capacity);
            var installed=pool.installSchema(store,new ApplyPosition(1,2),first,Map.of(fixture.column,"data"),Set.of()); original=installed.proof();
            store.applySchemaActivation(new ApplyPosition(1,3),original,catalogue,new ApplyPosition(1,1));
            var other=pool.reserveSchemaCapacity(store,second,Map.of(),Set.of()); pool.applySchemaFence(store,new ApplyPosition(1,4),second.header(),other);
            var deleted=pool.installSchema(store,new ApplyPosition(1,5),second,Map.of(),Set.of());
            var deletedStatus=pool.lookupSchemaCapacityStatus(store,second,null,false); assertEquals(DistributedStoragePool.SchemaCapacityState.CONSUMED,deletedStatus.state()); assertFalse(deletedStatus.ready());
            assertThrows(IllegalArgumentException.class,() -> pool.lookupSchemaCapacityStatus(store,second,"data",false)); assertThrows(IllegalArgumentException.class,() -> pool.lookupSchemaCapacityStatus(store,second,null,true));
            store.applySchemaActivation(new ApplyPosition(1,6),deleted.proof(),catalogue,new ApplyPosition(1,2));
        }
        try(var pool=fixture.open(root.resolve("short-history-donor"))) {
            var store=range(pool,fixture.group,middle,Map.of(fixture.column,"data"));
            for(int i=0;i<=3;i++) store.applyNoop(new ApplyPosition(1,i));
            store.applySchemaFence(new ApplyPosition(1,4),second.header()); var deleted=pool.installSchema(store,new ApplyPosition(1,5),second,Map.of(),Set.of());
            store.applySchemaActivation(new ApplyPosition(1,6),deleted.proof(),catalogue,new ApplyPosition(1,2)); NativeRangeImage.write(image,store,limits);
        }
        try(var pool=fixture.open(path)) {
            var store=range(pool,fixture.group,source,Map.of()); store.fence();
            var rejected=assertThrows(IOException.class,() -> store.installSnapshot(image,limits)); assertTrue(rejected.getMessage().contains("original capacity INSTALL"));
            assertEquals(new ApplyPosition(1,6),store.applied()); assertEquals(original,store.lookupSchemaInstallation(first.header()));
            store.activate(); pool.releaseSchemaCapacity(store,capacity,SchemaCapacityRelease.completed(original));
            assertEquals(original.fence(),pool.applySchemaFence(store,new ApplyPosition(1,7),first.header(),capacity)); assertNull(store.schemaFence());
        }
        try(var pool=fixture.open(path)) { var store=range(pool,fixture.group,source,Map.of()); assertEquals(original,store.lookupSchemaInstallation(first.header())); assertArrayEquals(terminal.digest(),store.schemaDigest()); }
    }

    @Test void failedReleaseForceCannotDonateLastSlotBeforeMatchingAuthorityIsReforced() throws Exception {
        for(boolean after:List.of(false,true)) {
            var source=empty(fixture.domain,0); var path=root.resolve("release-force-"+after); var permanent=new HashMap<UUID,String>();
            for(int i=0;i<4095;i++) permanent.put(UUID.randomUUID(),"past-"+i); seedPermanentBirths(path,source,permanent);
            var holder=change(source,populated(source,fixture.column),fixture.group); var authority=abort(holder);
            UUID otherGroup=UUID.randomUUID(),otherColumn=UUID.randomUUID(); var otherSource=empty(UUID.randomUUID(),0); var other=change(otherSource,populated(otherSource,otherColumn),otherGroup);
            NativeSchemaCapacityReservation original;
            try(var pool=qualified(path,source,permanent)) {
                var a=range(pool,fixture.group,source,Map.of()); var b=range(pool,otherGroup,otherSource,Map.of());
                original=pool.reserveSchemaCapacity(a,holder,Map.of(fixture.column,"held"),Set.of());
                pool.schemaCapacityForceObserver(boundary -> { if(boundary==after) throw new org.rocksdb.RocksDBException("release force unknown"); });
                assertThrows(org.rocksdb.RocksDBException.class,() -> pool.releaseSchemaCapacity(a,original,authority));
                assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(b,other,Map.of(otherColumn,"winner"),Set.of()));
                pool.schemaCapacityForceObserver(boundary -> {});
                assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(b,other,Map.of(otherColumn,"winner"),Set.of()),"Resetting the observer cannot release unknown ownership");
                pool.releaseSchemaCapacity(a,original,authority);
                assertNotNull(pool.reserveSchemaCapacity(b,other,Map.of(otherColumn,"winner"),Set.of()));
                assertThrows(IllegalStateException.class,() -> pool.applySchemaFence(a,new ApplyPosition(1,0),holder.header(),original));
            }
            try(var pool=qualified(path,source,permanent)) { var a=range(pool,fixture.group,source,Map.of()); assertEquals(original,pool.lookupSchemaCapacity(a,holder.header())); assertThrows(IllegalStateException.class,() -> a.applySchemaFence(new ApplyPosition(1,0),holder.header())); }
        }
    }
    @Test void permanentBirthByteBudgetIsReservedIndependentlyOfCount() throws Exception {
        var source=empty(fixture.domain,0); var path=root.resolve("birth-bytes"); var permanent=new HashMap<UUID,String>();
        for(int i=0;i<2015;i++) { String prefix="past-"+i+"-"; permanent.put(UUID.randomUUID(),prefix+"x".repeat(4096-prefix.length())); }
        seedPermanentBirths(path,source,permanent); var accepted=new ArrayList<SchemaChange.Definition>(); var proofs=new ArrayList<NativeSchemaCapacityReservation>();
        var attempted=change(source,populated(source,UUID.randomUUID()),fixture.group); UUID attemptedColumn=attempted.targetSchema().columnIds().iterator().next(); String attemptedName="rejected-"+"x".repeat(4096-9);
        try(var pool=qualified(path,source,permanent)) {
            var store=range(pool,fixture.group,source,Map.of());
            for(int i=0;i<15;i++) { var definition=change(source,populated(source,UUID.randomUUID()),fixture.group); var column=definition.targetSchema().columnIds().iterator().next(); String prefix="future-"+i+"-";
                proofs.add(pool.reserveSchemaCapacity(store,definition,Map.of(column,prefix+"x".repeat(4096-prefix.length())),Set.of())); accepted.add(definition);
            }
            assertTrue(permanent.size()+accepted.size()<4096);
            var rejected=assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(store,attempted,Map.of(attemptedColumn,attemptedName),Set.of())); assertTrue(rejected.getMessage().contains("birth capacity")); assertNull(store.schemaFence());
            pool.releaseSchemaCapacity(store,proofs.getFirst(),abort(accepted.getFirst()));
            assertNotNull(pool.reserveSchemaCapacity(store,attempted,Map.of(attemptedColumn,attemptedName),Set.of()));
        }
        try(var pool=qualified(path,source,permanent)) { var store=range(pool,fixture.group,source,Map.of()); assertNotNull(pool.lookupSchemaCapacity(store,attempted.header())); assertNull(store.schemaFence()); }
    }

    @Test void groupHistoryByteBudgetIsReservedBeforeItsCountLimit() throws Exception {
        var columns=new ArrayList<DistributedSchema.Column>(); var names=new HashMap<UUID,String>();
        for(int i=0;i<200;i++) { var id=UUID.randomUUID(); String prefix="source-"+i+"-"; columns.add(new DistributedSchema.Column(i+1,id,prefix+"x".repeat(4096-prefix.length()),ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)); names.put(id,"base-"+i); }
        var source=new DistributedSchema(fixture.cluster,fixture.database,fixture.domain,0,0,columns); var path=root.resolve("history-bytes"); var definitions=new ArrayList<SchemaChange.Definition>(); var proofs=new ArrayList<NativeSchemaCapacityReservation>();
        try(var pool=fixture.open(path)) {
            var store=range(pool,fixture.group,source,names); store.applyNoop(new ApplyPosition(1,0));
            for(int i=0;i<3;i++) { var next=new ArrayList<>(columns); var id=UUID.randomUUID(); next.add(new DistributedSchema.Column(201,id,"new-"+i,ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null));
                var target=new DistributedSchema(fixture.cluster,fixture.database,fixture.domain,1,0,next); var definition=change(source,target,fixture.group); var bindings=new HashMap<>(names); bindings.put(id,"future-"+i);
                proofs.add(pool.reserveSchemaCapacity(store,definition,bindings,Set.of())); definitions.add(definition);
            }
            assertTrue(proofs.stream().mapToLong(NativeSchemaCapacityReservation::historyBytes).sum()<16L*1024*1024); assertTrue(proofs.size()<64);
            var extra=new ArrayList<>(columns); UUID extraId=UUID.randomUUID(); extra.add(new DistributedSchema.Column(201,extraId,"extra",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null));
            var attempt=change(source,new DistributedSchema(fixture.cluster,fixture.database,fixture.domain,1,0,extra),fixture.group); var bindings=new HashMap<>(names); bindings.put(extraId,"extra");
            assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(store,attempt,bindings,Set.of())); assertNull(store.schemaFence());
            try(var pin=store.capture()) { assertEquals(200,pin.columns().size()); }
            pool.releaseSchemaCapacity(store,proofs.getFirst(),abort(definitions.getFirst())); assertNotNull(pool.reserveSchemaCapacity(store,attempt,bindings,Set.of()));
        }
        try(var pool=fixture.open(path)) { var store=range(pool,fixture.group,source,names); assertNull(store.schemaFence()); try(var pin=store.captureForRecovery()) { assertEquals(200,pin.columns().size()); } }
    }

    @Test void operatorCapacityRequiresExactStagingBeforeAckAndReopensItsOriginalPromise() throws Exception {
        byte[] jar=DistributedMergeOperatorRegistryTest.classes(NativeOperatorInstallationTest.AppendOperator.class); var source=empty(fixture.domain,0); var plain=populated(source,fixture.column).column(fixture.column);
        var operator=new DistributedSchema.Operator("append",1,NativeOperatorInstallationTest.AppendOperator.class.getName(),java.security.MessageDigest.getInstance("SHA-256").digest(jar),1,1,"append-contract",1);
        var shape=new ColumnSchema(plain.schema().keys(),plain.schema().variableTailKeys(),true,operator.name(),operator.version(),operator.className());
        var target=new DistributedSchema(fixture.cluster,fixture.database,fixture.domain,1,0,List.of(new DistributedSchema.Column(plain.apiId(),plain.columnId(),plain.name(),shape,operator)));
        var definition=change(source,target,fixture.group); var path=root.resolve("operator-capacity"); NativeSchemaCapacityReservation proof;
        try(var pool=qualified(path,source,Map.of())) {
            var store=range(pool,fixture.group,source,Map.of()); store.applyNoop(new ApplyPosition(1,0)); int refs=loaded(pool).refs().asList().size();
            assertThrows(IllegalStateException.class,() -> pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"merge"),Set.of(fixture.column)));
            assertNull(pool.lookupSchemaCapacity(store,definition.header())); assertNull(store.schemaFence());
            assertThrows(IllegalArgumentException.class,() -> DistributedStoragePool.stageOperator(path,operator,new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("append-contract",2,true),jar));
            DistributedStoragePool.stageOperator(path,operator,new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
            proof=pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"merge"),Set.of(fixture.column));
            assertEquals(refs,loaded(pool).refs().asList().size()); assertFalse(loaded(pool).mergeOperators().containsKey("merge")); try(var pin=store.capture()) { assertTrue(pin.columns().isEmpty()); }
        }
        try(var pool=qualified(path,source,Map.of())) {
            var store=range(pool,fixture.group,source,Map.of()); assertEquals(proof,pool.lookupSchemaCapacity(store,definition.header()));
            pool.applySchemaFence(store,new ApplyPosition(2,1),definition.header(),proof); var installed=pool.installSchema(store,new ApplyPosition(2,2),definition,Map.of(fixture.column,"merge"),Set.of(fixture.column));
            assertEquals(proof.historyBytes(),3L*installed.encode().length+2L*NativeSchemaActivationProof.BYTES);
            store.applySchemaActivation(new ApplyPosition(2,3),installed.proof(),UUID.randomUUID(),new ApplyPosition(2,1));
            var merge=new DistributedCommand(fixture.group,1,target.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.MERGE,fixture.column,new byte[]{1},new byte[]{7})));
            store.apply(new ApplyPosition(2,4),merge); assertArrayEquals(new byte[]{7},store.readCurrentPoint(fixture.column,new byte[]{1}));
        }
        try(var pool=qualified(path,source,Map.of())) { var store=range(pool,fixture.group,source,Map.of()); assertEquals(proof,pool.lookupSchemaCapacity(store,definition.header())); assertArrayEquals(new byte[]{7},store.readCurrentPoint(fixture.column,new byte[]{1})); }
    }
    @Test void configuredUnqualifiedNewMergeCannotAcknowledgeImpossibleInstall() throws Exception {
        var configPath=root.resolve("configured.conf");
        Files.writeString(configPath,Files.readString(Path.of("src/main/resources/it/cavallium/rockserver/core/resources/default.conf"))+"""
                database.global.column-options: [
                  ${database.global.fallback-column-options} { name: "future", merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator" }
                ]
                """);
        var config=ConfigParser.parse(configPath); var source=empty(fixture.domain,0); var target=populated(source,fixture.column); var definition=change(source,target,fixture.group);
        try(var pool=DistributedStoragePool.open(root.resolve("configured-capacity"),config,LoggerFactory.getLogger(getClass()))) {
            var store=range(pool,fixture.group,source,Map.of()); store.applyNoop(new ApplyPosition(1,0));
            assertThrows(UnsupportedOperationException.class,() -> pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"future"),Set.of(fixture.column)));
            assertNull(pool.lookupSchemaCapacity(store,definition.header())); assertNull(store.schemaFence());
            assertFalse(loaded(pool).mergeOperators().containsKey("future")); try(var pin=store.capture()) { assertTrue(pin.columns().isEmpty()); }
        }
        try(var pool=DistributedStoragePool.open(root.resolve("configured-bootstrap"),config,LoggerFactory.getLogger(getClass()))) {
            var store=pool.openRange(new RangeOwnership.Owner(fixture.group,1),new RangeOwnership.Interval(fixture.domain,target.columnIds(),null,null),target.digest(),Map.of(fixture.column,"future"),Set.of(fixture.column));
            assertNotNull(loaded(pool).mergeOperators().get("future")); store.applyNoop(new ApplyPosition(1,0));
        }
    }

    SchemaAdmissionManifest manifest(SchemaChange.Definition definition,NativeSchemaCapacityReservation proof,String name) {
        var descriptor=definition.sourceDirectory().ranges().getFirst(); var replica=descriptor.replicas().getFirst();
        return new SchemaAdmissionManifest(definition,List.of(new SchemaAdmissionManifest.Binding(descriptor.groupId(),name,false)),
                List.of(new SchemaAdmissionManifest.ReplicaAdmission(descriptor.groupId(),replica.replicaId(),proof)));
    }
    @Test void manifestAdmissionRejectsMissingClaimWrongOwnerBindingAndLocalProof() throws Exception {
        var source=empty(fixture.domain,0); var definition=change(source,populated(source,fixture.column),fixture.group);
        var owner=definition.sourceDirectory().ranges().getFirst().replicas().getFirst();
        try(var pool=fixture.open(root.resolve("manifest-validation"))) {
            var store=range(pool,fixture.group,source,Map.of()); store.applyNoop(new ApplyPosition(1,0));
            var fake=new NativeSchemaCapacityReservation(definition.header(),fixture.group,new byte[32],1);
            assertThrows(IllegalStateException.class,() -> pool.requireSchemaFencingAdmission(store,definition.header(),manifest(definition,fake,"future").encode(),owner));
            var proof=pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"future"),Set.of()); var manifest=manifest(definition,proof,"future");
            assertEquals(172,proof.encode().length); assertEquals(proof,pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),owner));
            var wrongOwner=new ReplicaDescriptor(owner.nodeId(),UUID.randomUUID(),owner.replicaId(),owner.peerId(),owner.address(),owner.role());
            assertThrows(IllegalArgumentException.class,() -> pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),wrongOwner));
            assertThrows(IllegalArgumentException.class,() -> pool.requireSchemaFencingAdmission(store,definition.header(),manifest(definition,proof,"changed").encode(),owner));
            assertThrows(IllegalArgumentException.class,() -> pool.requireSchemaFencingAdmission(store,definition.header(),manifest(definition,fake,"future").encode(),owner));
            assertNull(store.schemaFence()); assertTrue(store.admissionOpen());
        }
    }
    @Test void manifestAdmissionCannotCertifyAnUnresolvedCapacityForce() throws Exception {
        var source=empty(fixture.domain,0); var definition=change(source,populated(source,fixture.column),fixture.group);
        var owner=definition.sourceDirectory().ranges().getFirst().replicas().getFirst();
        try(var pool=fixture.open(root.resolve("manifest-force"))) {
            var store=range(pool,fixture.group,source,Map.of()); store.applyNoop(new ApplyPosition(1,0));
            pool.schemaCapacityForceObserver(after -> { if(after) throw new org.rocksdb.RocksDBException("Controlled lost force acknowledgement"); });
            assertThrows(org.rocksdb.RocksDBException.class,() -> pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"future"),Set.of()));
            var proof=pool.lookupSchemaCapacity(store,definition.header()); assertNotNull(proof); var manifest=manifest(definition,proof,"future");
            assertThrows(IllegalStateException.class,() -> pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),owner));
            pool.schemaCapacityForceObserver(after -> {}); assertEquals(proof,pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"future"),Set.of()));
            assertEquals(proof,pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),owner));
        }
    }
    @Test void manifestDigestSurvivesFenceReopenInstallHistoryAndPortableImage() throws Exception {
        var source=empty(fixture.domain,0); var definition=change(source,populated(source,fixture.column),fixture.group);
        var owner=definition.sourceDirectory().ranges().getFirst().replicas().getFirst(); Path sourcePath=root.resolve("manifest-source"),image=root.resolve("manifest-image");
        var limits=new NativeRangeImage.Limits(3*1024*1024,32*1024*1024,256); NativeSchemaCapacityReservation proof; SchemaAdmissionManifest manifest; NativeSchemaInstallation installed;
        try(var pool=fixture.open(sourcePath)) {
            var store=range(pool,fixture.group,source,Map.of()); store.applyNoop(new ApplyPosition(1,0));
            proof=pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"future"),Set.of()); manifest=manifest(definition,proof,"future");
            pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),owner);
            var fence=pool.applySchemaFence(store,new ApplyPosition(1,1),definition.header(),proof,manifest.digest());
            assertArrayEquals(manifest.digest(),fence.admissionDigest()); assertEquals(proof,pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),owner));
            byte[] changed=manifest.digest(); changed[0]^=1;
            assertThrows(IllegalArgumentException.class,() -> pool.applySchemaFence(store,new ApplyPosition(1,2),definition.header(),proof,changed));
            assertEquals(new ApplyPosition(1,1),store.applied());
        }
        try(var pool=fixture.open(sourcePath)) {
            var store=range(pool,fixture.group,source,Map.of()); assertArrayEquals(manifest.digest(),store.schemaFence().admissionDigest());
            assertEquals(proof,pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),owner));
            installed=pool.installSchema(store,new ApplyPosition(1,2),definition,Map.of(fixture.column,"future"),Set.of());
            assertArrayEquals(manifest.digest(),installed.proof().fence().admissionDigest());
            var state=pool.lookupSchemaCapacityStatus(store,definition,"future",false); assertEquals(DistributedStoragePool.SchemaCapacityState.CONSUMED,state.state());
            assertEquals(proof,pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),owner));
            assertEquals(installed.fence(),pool.applySchemaFence(store,new ApplyPosition(1,3),definition.header(),proof,manifest.digest()));
            byte[] changed=manifest.digest(); changed[0]^=1;
            assertThrows(IllegalArgumentException.class,() -> store.applySchemaFence(new ApplyPosition(1,4),definition.header(),changed));
            assertThrows(IllegalArgumentException.class,() -> store.applySchemaFence(new ApplyPosition(1,4),definition.header()));
            assertEquals(new ApplyPosition(1,3),store.applied()); NativeRangeImage.write(image,store,limits);
        }
        try(var pool=fixture.open(sourcePath)) {
            var store=range(pool,fixture.group,source,Map.of()); assertArrayEquals(manifest.digest(),store.lookupSchemaFence(definition.header()).admissionDigest());
            assertEquals(proof,pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),owner));
        }
        try(var pool=fixture.open(root.resolve("manifest-target"))) {
            var store=range(pool,fixture.group,definition.targetSchema(),Map.of(fixture.column,"future")); store.fence(); store.installSnapshot(image,limits); store.activate();
            assertEquals(installed.proof(),store.lookupSchemaInstallation(definition.header())); assertArrayEquals(manifest.digest(),store.lookupSchemaFence(definition.header()).admissionDigest());
            byte[] changed=manifest.digest(); changed[0]^=1;
            assertThrows(IllegalArgumentException.class,() -> store.applySchemaFence(new ApplyPosition(1,4),definition.header(),changed));
            assertEquals(installed.fence(),store.applySchemaFence(new ApplyPosition(1,4),definition.header(),manifest.digest()));
        }
    }
    @Test void abortedCapacityClaimIsNeverFreshManifestAdmission() throws Exception {
        var source=empty(fixture.domain,0); var definition=change(source,populated(source,fixture.column),fixture.group);
        var owner=definition.sourceDirectory().ranges().getFirst().replicas().getFirst();
        try(var pool=fixture.open(root.resolve("manifest-aborted"))) {
            var store=range(pool,fixture.group,source,Map.of()); store.applyNoop(new ApplyPosition(1,0));
            var proof=pool.reserveSchemaCapacity(store,definition,Map.of(fixture.column,"future"),Set.of()); var manifest=manifest(definition,proof,"future");
            pool.releaseSchemaCapacity(store,proof,abort(definition));
            assertThrows(IllegalStateException.class,() -> pool.requireSchemaFencingAdmission(store,definition.header(),manifest.encode(),owner)); assertNull(store.schemaFence());
        }
    }

}
