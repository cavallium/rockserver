package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Flux;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

/** Actual native DATA/session journals through the concrete connection; transport is controlled. */
class RatisRocksDBConnectionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T> T await(CompletableFuture<T> value) throws Exception { return value.get(10,TimeUnit.SECONDS); }
    final class Fixture implements AutoCloseable {
        final RatisRocksDBDirectTest owner=new RatisRocksDBDirectTest();
        final RatisRocksDBDirectTest.Fixture direct;
        final Path connectionRoot;
        final RatisNodePlan.Catalogue binding;
        final RatisRocksDBConnection.GenerationFactory factory;
        final RatisRocksDBAdapter.AllocationSource allocations;
        boolean first=true;
        RatisSnapshotClient suppliedSnapshots;
        Fixture() throws Exception {
            owner.root=root; direct=owner.new Fixture(); connectionRoot=root.resolve("connection"); var f=direct.f;
            allocations=new RatisRocksDBAdapterTest.Allocations(f);
            binding=new RatisNodePlan.Catalogue(f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.outer.sessions,f.directory.ranges().getFirst().replicas());
            var schemas=new RatisSchemaCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(f.outer.operation(3),f.schema)).status());
            var catalogue=new RatisCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(f.outer.operation(4),null,f.directory)).status());
            factory=(schema,directory,path,ids) -> {
                var session=first ? direct.session : RatisDurableSessionClient.reopen(direct.window,direct.routed);
                var writer=first ? direct.direct : new RatisRocksDBDirect(schema,direct.routed,session,f.scheduler,direct.journal); first=false;
                var transactions=new RatisTransactionClient(schema,f.snapshots,f.homes,f.data,f.scheduler,RatisTransactionClient.Limits.defaults());
                var snapshots=suppliedSnapshots==null ? new RatisSnapshotClient(schema,f.snapshots,f.data,f.scheduler,RatisSnapshotClient.Limits.defaults()) : suppliedSnapshots;
                var ranges=new RatisRocksDBRanges(schema,directory,snapshots,allocations); var adapter=new RatisRocksDBAdapter(schema,directory,transactions,allocations,writer,ranges,ids);
                return new RatisRocksDBConnection.Generation(adapter,writer,ranges,transactions,snapshots,List.of(session));
            };
        }
        RatisRocksDBConnection open(int queuedData) throws Exception { return open(queuedData,8); }
        RatisRocksDBConnection open(int queuedData,int generations) throws Exception { var replicas=new RatisSchemaReplicaClient(1); return new RatisRocksDBConnection(URI.create("ratis://controlled/native"),connectionRoot,direct.f.schema,direct.routed,direct.f.scheduler,binding,direct.f.catalogue.client,replicas,null,null,factory,new RatisRocksDBConnection.Limits(generations,64,8,128L*1024*1024,queuedData),List.of(replicas)); }
        public void close() throws Exception { direct.close(); }
    }
    @Test void commonSyncAsyncDataSchemaNoopsAndReopenUseOwnedNativeGenerations() throws Exception {
        try (var f=new Fixture()) {
            long originalId;
            try (var connection=f.open(8)) {
                assertEquals(URI.create("ratis://controlled/native"),connection.getUrl()); assertEquals(RockserverCapabilities.BackendMode.DISTRIBUTED,connection.getCapabilities().backendMode()); assertEquals(RockserverCapabilities.RawScanMode.UNSUPPORTED,connection.getCapabilities().rawScanMode());
                var sync=connection.getSyncApi(RequestContext.batch()); var async=connection.getAsyncApi(RequestContext.batch());
                assertEquals(71,sync.createColumn("atomic-0",f.direct.f.schema.column(71).schema())); assertFalse(await(async.deleteColumnIfExistsAsync("never-created")));
                sync.put(0,71,key(2),value(7),RequestType.none()); assertArrayEquals(new byte[]{7},await(async.getAsync(0,71,key(2),RequestType.<Buf>current())).toByteArray());
                assertArrayEquals(new byte[]{7},await(async.putAsync(0,71,key(2),value(9),RequestType.<Buf>previous())).toByteArray()); assertArrayEquals(new byte[]{9},sync.get(0,71,key(2),RequestType.<Buf>current()).toByteArray());
                originalId=await(async.openTransactionAsync(Duration.ofSeconds(10))); assertTrue(await(async.closeTransactionAsync(originalId,false)));
                assertTrue(connection.pendingSchemaRequests().isEmpty());
            }
            try (var connection=f.open(8)) {
                assertArrayEquals(new byte[]{9},connection.getSyncApi(RequestContext.batch()).get(0,71,key(2),RequestType.<Buf>current()).toByteArray());
                var retained=await(connection.directRecoveryStatus(RequestContext.batch())).getFirst();
                assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,retained.state());
                var previous=(Buf)await(connection.resumeDirectRequest(retained.schemaDigest(),RequestContext.batch()));
                assertArrayEquals(new byte[]{7},previous.toByteArray());
                long next=await(connection.getAsyncApi(RequestContext.batch()).openTransactionAsync(Duration.ofSeconds(10))); assertTrue(next>originalId); assertTrue(await(connection.getAsyncApi(RequestContext.batch()).closeTransactionAsync(next,false)));
            }
        }
    }
    @Test void forcedTargetBindingConsumesReservedSlotEvenBeforeControllerCleanupFinishes() throws Exception {
        try(var f=new Fixture()) {
            try(var connection=f.open(8,2)) { assertNotNull(connection.getUrl()); }
            var source=f.direct.f.schema; var original=f.direct.routed.directory();
            var columns=new ArrayList<>(source.columns()); columns.add(new DistributedSchema.Column(900,UUID.randomUUID(),"capacity-target",source.column(71).schema(),null));
            var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch()+1,source.routingPrefixBytes(),columns);
            var directory=new RangeDirectory(original.clusterId(),original.databaseId(),original.domainId(),target.schemaEpoch(),target.digest(),original.generation()+1,target.columnIds(),original.routingPrefixBytes(),original.ranges());
            Path ledger=f.connectionRoot.resolve("generation-capacity"),bindings=f.connectionRoot.resolve("generations"),sdk=f.connectionRoot.resolve("schema");
            var capacity=new RatisGenerationRegistry.Capacity(ledger,bindings,sdk,2,true); UUID id=UUID.randomUUID(); var fields=new HashMap<String,byte[]>();
            fields.put("requestId",java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array()); fields.put("kind","CREATE".getBytes(java.nio.charset.StandardCharsets.UTF_8)); fields.put("changed",java.nio.ByteBuffer.allocate(8).putLong(1).array()); fields.put("aborted",new byte[8]); fields.put("phase","CLEANUP".getBytes(java.nio.charset.StandardCharsets.UTF_8)); fields.put("sourceSchema",DistributedSchemaCodec.encode(source)); fields.put("sourceDirectory",RangeDirectoryCodec.encode(original)); fields.put("targetSchema",DistributedSchemaCodec.encode(target));
            capacity.checkpoint(sdk.resolve(id+".request"),fields,RatisSchemaClientJournal::write);
            assertEquals(1,java.nio.ByteBuffer.wrap(SessionJournalFiles.read(ledger,4096)).getInt());
            // Crash after binding force, before capacity.bound and before controller DONE.
            var folder=Files.createDirectory(bindings.resolve(HexFormat.of().formatHex(target.digest()))); byte[] a=DistributedSchemaCodec.encode(target),b=RangeDirectoryCodec.encode(directory);
            SessionJournalFiles.write(folder.resolve("binding"),java.nio.ByteBuffer.allocate(8+a.length+b.length).putInt(a.length).put(a).putInt(b.length).put(b).array(),8*1024*1024); SessionJournalFiles.force(bindings);
            var forced=new ArrayList<Path>(); var failRoot=new java.util.concurrent.atomic.AtomicBoolean(true);
            RatisGenerationRegistry.Capacity.BindingForce force=path -> { forced.add(path); if(path.equals(bindings) && failRoot.getAndSet(false)) throw new IOException("Uncertain generations directory force"); SessionJournalFiles.force(path); };
            assertThrows(IOException.class,() -> new RatisGenerationRegistry.Capacity(ledger,bindings,sdk,2,true,force));
            assertEquals(List.of(folder.resolve("binding"),folder,bindings),forced);
            assertEquals(1,java.nio.ByteBuffer.wrap(SessionJournalFiles.read(ledger,4096)).getInt(),"Failed re-force cannot consume original reservation");
            forced.clear(); new RatisGenerationRegistry.Capacity(ledger,bindings,sdk,2,true,force);
            assertEquals(List.of(folder.resolve("binding"),folder,bindings),forced);
            assertEquals(0,java.nio.ByteBuffer.wrap(SessionJournalFiles.read(ledger,4096)).getInt());
        }
    }
    @Test void cancelledStreamReleaseFailureCanRetryThroughConnectionCloseWithoutLosingOwnership() throws Exception {
        try(var f=new Fixture()) {
            var snapshots=mock(RatisSnapshotClient.class); var lease=mock(RatisSnapshotClient.Lease.class); f.suppliedSnapshots=snapshots;
            var reading=new CompletableFuture<RangePage<KV>>(); var releasing=new CompletableFuture<Void>(); var requested=new CountDownLatch(1); var calls=new AtomicInteger(); var closes=new AtomicInteger();
            when(lease.completion()).thenReturn(CompletableFuture.completedFuture(null));
            when(lease.page(anyLong(),any(),any(),anyBoolean(),any(),any(),any())).thenAnswer(call -> { requested.countDown(); return reading; });
            when(lease.release(any())).thenAnswer(call -> calls.incrementAndGet()==1 ? CompletableFuture.failedFuture(new IOException("Unknown release outcome")) : releasing);
            doAnswer(call -> { closes.incrementAndGet(); return null; }).when(lease).close(); when(snapshots.open(any(),any(),any(),any(),any())).thenReturn(lease);
            var connection=f.open(1); var observer=new RatisRocksDBIteratorLifecycleTest.Observer();
            try {
                Flux.from(connection.getAsyncApi(RequestContext.batch()).getRangeAsync(0,71,key(1),key(3),false,RequestType.<KV>allInRange())).subscribe(observer);
                assertTrue(requested.await(5,TimeUnit.SECONDS)); observer.subscription.get().cancel();
                reading.complete(new RangePage<>(List.of(new KV(key(1),value(1))),key(1),false));
                assertEquals(1,calls.get()); assertEquals(0,closes.get());
                var rejected=assertThrows(ExecutionException.class,() -> await(connection.getAsyncApi(RequestContext.batch()).getAsync(0,71,key(1),RequestType.<Buf>current()))); assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,assertInstanceOf(RocksDBException.class,rejected.getCause()).getErrorUniqueId());
                assertThrows(IOException.class,connection::close); assertEquals(2,calls.get()); assertEquals(0,closes.get());
                releasing.complete(null); connection.close(); assertEquals(1,closes.get()); assertTrue(observer.values.isEmpty());
            } finally { reading.complete(new RangePage<>(List.of(),null,false)); releasing.complete(null); connection.close(); }
        }
    }
    @Test void actualSchemaCreateDeleteRecreateWaitsForOldNativePinAndColdReopensAllGenerations() throws Exception {
        try(var nativeSchema=new RatisSchemaClientWorkflowTest.Fixture(root.resolve("schema-native"),false); var homes=new RatisAtomicClientTest.Fixture(root.resolve("homes"),true)) {
            var routed=new java.util.concurrent.atomic.AtomicReference<>(nativeSchema.routed); var operations=new java.util.concurrent.atomic.AtomicLong(500);
            var epochs=new RatisSessionEpochs(nativeSchema.client,nativeSchema.outer.cluster,nativeSchema.outer.database,nativeSchema.catalogue.groupId(),nativeSchema.catalogue.epoch(),nativeSchema.catalogue.schemaDigest(),nativeSchema.catalogue.sessionsColumn());
            var genesis=SessionEpochLedger.initial(nativeSchema.outer.cluster,nativeSchema.outer.database,10);
            assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(nativeSchema.outer.operation(operations.getAndIncrement()),null,genesis)).status());
            nativeSchema.beforeDataCommand=command -> { if(command.sessionControl()!=null) epochs.sessionControlAdmission(routed.get().directory(),command.groupId()).accept(command); };
            // Native controls are activated only after fresh catalogue proposal authority is installed.
            for(var peer:nativeSchema.peers) peer.store.enableSessionControls();
            RatisRocksDBAdapter.AllocationSource unused=context -> CompletableFuture.failedFuture(new IOException("This schema fixture does not implement transaction capture RPC"));
            RatisRocksDBConnection.GenerationFactory factory=(schema,directory,path,ids) -> {
                var data=routed.get();
                // Production factory owns constructor unwind and all generation wrappers.
                return RatisRocksDBConnection.durableGenerationFactory(data,homes.scheduler,homes.snapshots,homes.homes,unused,(generationRoot,selected,placement) -> {
                    Path window=generationRoot.resolve("session"); if(Files.exists(window)) return RatisDurableSessionClient.reopen(window,data);
                    var before=epochs.read(); var issuance=before.issue(UUID.randomUUID(),1000); var grant=issuance.grants().getLast();
                    assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(nativeSchema.outer.operation(operations.getAndIncrement()),before,issuance)).status());
                    for(var range:placement.ranges()) {
                        var control=epochs.prepareSessionBinding(placement,range.groupId(),new OperationId(new UUID(940,1),issuance.nextEpoch(),operations.getAndIncrement()),grant,32,-1);
                        assertEquals(CommandResult.Status.SUCCESS,data.execute(data.prepareSessionControl(control)).status());
                    }
                    return RatisDurableSessionClient.create(window,data,issuance,grant,RatisSessionClient.Limits.defaults());
                }).create(schema,directory,path,ids);
            };
            Path connectionRoot=root.resolve("dynamic-connection"); var limits=new RatisRocksDBConnection.Limits(4,64,8,128L*1024*1024,8);
            long recreated;
            var dataKey=new Keys(Buf.wrap(new byte[]{0,0,0,0,0,0,0,2}));
            try(var connection=new RatisRocksDBConnection(URI.create("ratis://controlled/schema"),connectionRoot,nativeSchema.initial,routed.get(),homes.scheduler,nativeSchema.catalogue,nativeSchema.client,nativeSchema.replicas,null,null,factory,limits,List.of())) {
                var async=connection.getAsyncApi(RequestContext.batch()); var sync=connection.getSyncApi(RequestContext.batch());
                UUID pendingId;
                try(var pin=nativeSchema.peers.getFirst().store.capture()) {
                    var failure=assertThrows(ExecutionException.class,() -> await(async.createColumnAsync("dynamic",RatisSchemaClientTest.shape()))); var pending=assertInstanceOf(RatisSchemaRequestPendingException.class,failure.getCause()); pendingId=pending.requestId();
                    assertEquals(RatisSchemaRequests.State.PAUSED,connection.schemaRequestStatus(pendingId).state()); assertEquals(1,nativeSchema.routed.directory().schemaEpoch());
                    assertNotNull(pin); assertEquals(SchemaChange.Phase.FENCED,nativeSchema.admin.readHead(nativeSchema.initial.domainId()).status().phase());
                }
                var resumed=await(connection.resumeSchemaRequest(pendingId,RequestContext.batch())); assertTrue(resumed.changed()); long created=resumed.value(); assertEquals(2,nativeSchema.routed.directory().schemaEpoch());
                assertEquals(created,sync.createColumn("dynamic",RatisSchemaClientTest.shape()));
                for(var peer:nativeSchema.peers) assertArrayEquals(nativeSchema.routed.directory().schemaDigest(),peer.store.schemaDigest());
                assertNull(await(async.putAsync(0,created,dataKey,value(8),RequestType.<Buf>previous()))); assertArrayEquals(new byte[]{8},sync.get(0,created,dataKey,RequestType.<Buf>current()).toByteArray());
                sync.deleteColumn(created); assertEquals(3,nativeSchema.routed.directory().schemaEpoch());
                assertNull(sync.get(0,7,dataKey,RequestType.<Buf>current())); // Open and durably bind the deletion generation.
                recreated=sync.createColumn("dynamic",RatisSchemaClientTest.shape()); assertNotEquals(created,recreated); assertEquals(4,nativeSchema.routed.directory().schemaEpoch());
                sync.put(0,recreated,dataKey,value(9),RequestType.none()); assertArrayEquals(new byte[]{9},sync.get(0,recreated,dataKey,RequestType.<Buf>current()).toByteArray());
                assertTrue(connection.pendingSchemaRequests().isEmpty());
            }
            try(var cold=new RatisRoutedClient(nativeSchema.initialDirectory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor range,UUID column,byte[] key) throws IOException { try(var pin=nativeSchema.peers.getFirst().store.capture()) { return pin.get(column,key); } catch(Exception failure) { throw new IOException(failure); } }
                public CommandResult command(RangeDescriptor range,DistributedCommand command) throws IOException { try { if(command.sessionControl()!=null) epochs.sessionControlAdmission(routed.get().directory(),range.groupId()).accept(command); CommandResult first=null; for(var peer:nativeSchema.peers) { var result=CommandResult.decode(peer.store.apply(peer.next(),command)); if(first==null) first=result; } return first; } catch(Exception failure) { throw new IOException(failure); } }
            })) {
                routed.set(cold); long before=nativeSchema.peers.getFirst().store.applied().index();
                try(var connection=new RatisRocksDBConnection(URI.create("ratis://controlled/schema"),connectionRoot,nativeSchema.initial,cold,homes.scheduler,nativeSchema.catalogue,nativeSchema.client,nativeSchema.replicas,null,null,factory,limits,List.of())) {
                    assertEquals(4,cold.directory().schemaEpoch()); assertArrayEquals(new byte[]{9},connection.getSyncApi(RequestContext.batch()).get(0,recreated,dataKey,RequestType.<Buf>current()).toByteArray());
                    for(var recovery:await(connection.directRecoveryStatus(RequestContext.batch()))) if(recovery.state()==RatisRocksDBDirect.RecoveryState.COMPLETE) assertNull(await(connection.resumeDirectRequest(recovery.schemaDigest(),RequestContext.batch())));
                    assertEquals(before,nativeSchema.peers.getFirst().store.applied().index(),"Cold history and retained callbacks do not submit new DATA"); assertTrue(connection.pendingSchemaRequests().isEmpty());
                }
            }
        }
    }
    /** Controlled SDK lease fronts own real native captures; this checks facade routing, not capture RPC. */
    @Test void commonOldTransactionAndCursorPinsBlockInstallThenNewHandlesSelectActivatedGeneration() throws Exception {
        try(var nativeSchema=new RatisSchemaClientWorkflowTest.Fixture(root.resolve("handles-native"),false); var homes=new RatisAtomicClientTest.Fixture(root.resolve("handles-homes"),true)) {
            var operations=new java.util.concurrent.atomic.AtomicLong(500); var allocations=new RatisRocksDBAdapterTest.Allocations(homes);
            var epochs=new RatisSessionEpochs(nativeSchema.client,nativeSchema.outer.cluster,nativeSchema.outer.database,nativeSchema.catalogue.groupId(),nativeSchema.catalogue.epoch(),nativeSchema.catalogue.schemaDigest(),nativeSchema.catalogue.sessionsColumn());
            var genesis=SessionEpochLedger.initial(nativeSchema.outer.cluster,nativeSchema.outer.database,10); assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(nativeSchema.outer.operation(operations.getAndIncrement()),null,genesis)).status());
            nativeSchema.beforeDataCommand=command -> { if(command.sessionControl()!=null) epochs.sessionControlAdmission(nativeSchema.routed.directory(),command.groupId()).accept(command); };
            for(var peer:nativeSchema.peers) peer.store.enableSessionControls();
            var activePins=new AtomicInteger(); var transactionSchemas=new ArrayList<Long>(); var cursorSchemas=new ArrayList<Long>();
            var dataKey=new Keys(Buf.wrap(new byte[]{0,0,0,0,0,0,0,2}));
            RatisRocksDBConnection.GenerationFactory factory=(schema,directory,path,ids) -> {
                var base=RatisRocksDBConnection.durableGenerationFactory(nativeSchema.routed,homes.scheduler,homes.snapshots,homes.homes,allocations,(generationRoot,selected,placement) -> {
                    Path window=generationRoot.resolve("session"); if(Files.exists(window)) return RatisDurableSessionClient.reopen(window,nativeSchema.routed);
                    var before=epochs.read(); var issuance=before.issue(UUID.randomUUID(),1000); var grant=issuance.grants().getLast(); assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(nativeSchema.outer.operation(operations.getAndIncrement()),before,issuance)).status());
                    for(var range:placement.ranges()) { var control=epochs.prepareSessionBinding(placement,range.groupId(),new OperationId(new UUID(950,1),issuance.nextEpoch(),operations.getAndIncrement()),grant,32,-1); assertEquals(CommandResult.Status.SUCCESS,nativeSchema.routed.execute(nativeSchema.routed.prepareSessionControl(control)).status()); }
                    return RatisDurableSessionClient.create(window,nativeSchema.routed,issuance,grant,RatisSessionClient.Limits.defaults());
                }).create(schema,directory,path,ids);
                try {
                    var transactions=mock(RatisTransactionClient.class); var snapshots=mock(RatisSnapshotClient.class);
                    when(transactions.open(any(),any(),any(),any(),any(),any())).thenAnswer(call -> {
                        assertArrayEquals(schema.digest(),nativeSchema.peers.getFirst().store.schemaDigest()); transactionSchemas.add(schema.schemaEpoch());
                        var pin=nativeSchema.peers.getFirst().store.capture(); activePins.incrementAndGet(); var closed=new java.util.concurrent.atomic.AtomicBoolean(); var lease=mock(RatisTransactionClient.Lease.class);
                        when(lease.completion()).thenReturn(CompletableFuture.completedFuture(null));
                        when(lease.get(anyLong(),any(),any())).thenAnswer(read -> { long column=read.getArgument(0); Keys key=read.getArgument(1); var shape=schema.column(column); byte[] value=pin.get(shape.columnId(),schema.encodeLogicalKey(shape.columnId(),key).physicalKey()); return CompletableFuture.completedFuture(value==null ? null : Buf.wrap(value)); });
                        Runnable release=() -> { if(closed.compareAndSet(false,true)) { pin.close(); activePins.decrementAndGet(); } };
                        when(lease.rollback(any())).thenAnswer(rollback -> { release.run(); return CompletableFuture.completedFuture(null); }); doAnswer(close -> { release.run(); return null; }).when(lease).close(); return lease;
                    });
                    when(snapshots.open(any(),any(),any(),any(),any())).thenAnswer(call -> {
                        assertArrayEquals(schema.digest(),nativeSchema.peers.getFirst().store.schemaDigest()); cursorSchemas.add(schema.schemaEpoch());
                        var pin=nativeSchema.peers.getFirst().store.capture(); activePins.incrementAndGet(); var closed=new java.util.concurrent.atomic.AtomicBoolean(); var lease=mock(RatisSnapshotClient.Lease.class);
                        when(lease.completion()).thenReturn(CompletableFuture.completedFuture(null));
                        when(lease.page(anyLong(),any(),any(),anyBoolean(),any(),any(),any())).thenAnswer(read -> { long column=read.getArgument(0); var shape=schema.column(column); byte[] value=pin.get(shape.columnId(),schema.encodeLogicalKey(shape.columnId(),dataKey).physicalKey()); return CompletableFuture.completedFuture(value==null ? new RangePage<KV>(List.of(),null,false) : new RangePage<>(List.of(new KV(dataKey,Buf.wrap(value))),dataKey,false)); });
                        Runnable release=() -> { if(closed.compareAndSet(false,true)) { pin.close(); activePins.decrementAndGet(); } };
                        when(lease.release(any())).thenAnswer(releasing -> { release.run(); return CompletableFuture.completedFuture(null); }); doAnswer(close -> { release.run(); return null; }).when(lease).close(); return lease;
                    });
                    var ranges=new RatisRocksDBRanges(schema,directory,snapshots,allocations); var adapter=new RatisRocksDBAdapter(schema,directory,transactions,allocations,base.direct(),ranges,ids);
                    return new RatisRocksDBConnection.Generation(adapter,base.direct(),ranges,transactions,snapshots,List.of(base));
                } catch(Throwable failure) { try { base.close(); } catch(Throwable cleanup) { failure.addSuppressed(cleanup); } if(failure instanceof IOException io) throw io; throw new IOException(failure); }
            };
            try(var connection=new RatisRocksDBConnection(URI.create("ratis://controlled/old-handles"),root.resolve("handle-connection"),nativeSchema.initial,nativeSchema.routed,homes.scheduler,nativeSchema.catalogue,nativeSchema.client,nativeSchema.replicas,null,null,factory,new RatisRocksDBConnection.Limits(2,64,8,128L*1024*1024,8),List.of())) {
                var api=connection.getAsyncApi(RequestContext.batch()); await(api.putAsync(0,7,dataKey,value(1),RequestType.none()));
                long oldTransaction=await(api.openTransactionAsync(Duration.ofSeconds(10))); await(api.putAsync(0,7,dataKey,value(2),RequestType.none()));
                long oldCursor=await(api.openIteratorAsync(0,7,dataKey,null,false,Duration.ofSeconds(10))); assertNotEquals(oldTransaction,oldCursor); assertEquals(2,activePins.get());
                var failure=assertThrows(ExecutionException.class,() -> await(api.createColumnAsync("after-old-handles",RatisSchemaClientTest.shape()))); UUID token=assertInstanceOf(RatisSchemaRequestPendingException.class,failure.getCause()).requestId();
                assertEquals(SchemaChange.Phase.FENCED,nativeSchema.admin.readHead(nativeSchema.initial.domainId()).status().phase()); assertEquals(1,nativeSchema.routed.directory().schemaEpoch());
                assertArrayEquals(new byte[]{1},await(api.getAsync(oldTransaction,7,dataKey,RequestType.<Buf>current())).toByteArray());
                assertArrayEquals(new byte[]{2},await(api.subsequentAsync(oldCursor,0,1,RequestType.<Buf>multi())).getFirst().toByteArray());
                assertTrue(await(api.closeTransactionAsync(oldTransaction,false))); assertEquals(1,activePins.get());
                var stillBlocked=assertThrows(ExecutionException.class,() -> await(connection.resumeSchemaRequest(token,RequestContext.batch()))); assertEquals(token,assertInstanceOf(RatisSchemaRequestPendingException.class,stillBlocked.getCause()).requestId());
                await(api.closeIteratorAsync(oldCursor)); assertEquals(0,activePins.get()); assertTrue(await(connection.resumeSchemaRequest(token,RequestContext.batch())).changed()); assertEquals(2,nativeSchema.routed.directory().schemaEpoch());
                long newTransaction=await(api.openTransactionAsync(Duration.ofSeconds(10))); long newCursor=await(api.openIteratorAsync(0,7,dataKey,null,false,Duration.ofSeconds(10))); assertTrue(newTransaction>Math.max(oldTransaction,oldCursor)); assertTrue(newCursor>newTransaction);
                assertArrayEquals(new byte[]{2},await(api.getAsync(newTransaction,7,dataKey,RequestType.<Buf>current())).toByteArray()); assertArrayEquals(new byte[]{2},await(api.subsequentAsync(newCursor,0,1,RequestType.<Buf>multi())).getFirst().toByteArray());
                assertTrue(await(api.closeTransactionAsync(newTransaction,false))); await(api.closeIteratorAsync(newCursor)); assertEquals(0,activePins.get()); assertEquals(List.of(1L,2L),transactionSchemas); assertEquals(List.of(1L,2L),cursorSchemas);
            }
        }
    }
    @Test void exhaustedGenerationCapacityRejectsCreateBeforeEffectsAndSurvivesReopen() throws Exception {
        try(var f=new Fixture()) {
            byte[] original=f.direct.routed.directory().payloadDigest(); UUID rejectedId;
            try(var connection=f.open(8,1)) {
                var failure=assertThrows(ExecutionException.class,() -> await(connection.getAsyncApi(RequestContext.batch()).createColumnAsync("new-column",f.direct.f.schema.column(71).schema())));
                var pending=assertInstanceOf(RatisSchemaRequestPendingException.class,failure.getCause()); rejectedId=pending.requestId();
                assertEquals(RatisSchemaRequests.State.NOT_STARTED,connection.schemaRequestStatus(rejectedId).state());
                assertFalse(Files.exists(f.connectionRoot.resolve("schema").resolve(rejectedId+".request")));
                assertArrayEquals(original,f.direct.routed.directory().payloadDigest());
                assertFalse(await(connection.getAsyncApi(RequestContext.batch()).deleteColumnIfExistsAsync("missing-after-capacity-rejection")));
                connection.getSyncApi(RequestContext.batch()).put(0,71,key(4),value(5),RequestType.none());
            }
            try(var connection=f.open(8,1)) {
                assertEquals(RatisSchemaRequests.State.NOT_STARTED,connection.schemaRequestStatus(rejectedId).state());
                assertArrayEquals(original,f.direct.routed.directory().payloadDigest());
                assertArrayEquals(new byte[]{5},connection.getSyncApi(RequestContext.batch()).get(0,71,key(4),RequestType.<Buf>current()).toByteArray());
                assertFalse(await(connection.getAsyncApi(RequestContext.batch()).deleteColumnIfExistsAsync("still-missing")));
            }
        }
    }
    @Test void cancelledDirectObserverKeepsConnectionAdmissionUntilActualNativeWorkDrains() throws Exception {
        var entered=new CountDownLatch(1); var unblock=new CountDownLatch(1);
        try (var f=new Fixture()) {
            var connection=f.open(1);
            try {
                f.direct.direct.afterAcknowledgement(() -> { entered.countDown(); try { if (!unblock.await(5,TimeUnit.SECONDS)) throw new AssertionError("Direct fixture timeout"); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); } });
                var api=connection.getAsyncApi(RequestContext.batch()); var write=api.putAsync(0,71,key(3),value(4),RequestType.<Buf>previous()); assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(write.cancel(false));
                var rejected=assertThrows(ExecutionException.class,() -> await(api.getAsync(0,71,key(3),RequestType.<Buf>current()))); assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,assertInstanceOf(RocksDBException.class,rejected.getCause()).getErrorUniqueId());
                assertThrows(IOException.class,connection::close); unblock.countDown();
                var drained=new CompletableFuture<Void>(); f.direct.f.scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION).execute(() -> drained.complete(null)); await(drained); connection.close();
            } finally { unblock.countDown(); connection.close(); }
            try (var reopened=f.open(1)) {
                var states=await(reopened.directRecoveryStatus(RequestContext.batch())); assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,states.getFirst().state()); assertNull(await(reopened.resumeDirectRequest(states.getFirst().schemaDigest(),RequestContext.batch())));
                assertArrayEquals(new byte[]{4},await(reopened.getAsyncApi(RequestContext.batch()).getAsync(0,71,key(3),RequestType.<Buf>current())).toByteArray());
            }
        }
    }
}
