package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.export.*;
import it.cavallium.rockserver.core.server.ExportGrpcService;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

/** Public immutable job schema drives logical migration; native stores remain live across journal reopen. */
class RatisLogicalExportMigrationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T>T await(CompletableFuture<T> value)throws Exception{return value.get(20,TimeUnit.SECONDS);}
    enum Applied { COMMITTED, SKIPPED, CONFLICT }
    @FunctionalInterface interface BeforeCommit {void run()throws Exception;}
    static Buf fence(long epoch,UUID owner){return Buf.wrap(ByteBuffer.allocate(24).putLong(epoch).putLong(owner.getMostSignificantBits()).putLong(owner.getLeastSignificantBits()).array());}
    static Applied migrate(RocksDBAsyncAPI api,ExportJobs.Chunk chunk,List<KV> rows,Buf expectedFence,Keys checkpoint,BeforeCommit competing)throws Exception {
        long transaction=await(api.openTransactionAsync(Duration.ofSeconds(30)));boolean closed=false;
        try {
            var guard=await(api.getAsync(transaction,71,key(1),RequestType.<Buf>forUpdate()));assertEquals(transaction,guard.updateId());assertArrayEquals(expectedFence.toByteArray(),guard.previous().toByteArray(),"Migration epoch/owner must match the trusted lease");
            byte[] exact=ExportWire.id(chunk.id()).toByteArray();var saved=await(api.getAsync(transaction,73,checkpoint,RequestType.<Buf>current()));
            if(saved!=null){assertArrayEquals(exact,saved.toByteArray());assertTrue(await(api.closeTransactionAsync(transaction,false)));closed=true;return Applied.SKIPPED;}
            if(!rows.isEmpty())await(api.putMultiAsync(transaction,74,rows.stream().map(KV::keys).toList(),rows.stream().map(KV::value).toList(),RequestType.<Buf>none()));
            await(api.putAsync(transaction,73,checkpoint,Buf.wrap(exact),RequestType.<Buf>none()));competing.run();
            if(await(api.closeTransactionAsync(transaction,true))){closed=true;return Applied.COMMITTED;}
            assertTrue(await(api.closeTransactionAsync(transaction,false)));closed=true;return Applied.CONFLICT;
        }finally{if(!closed)await(api.closeTransactionAsync(transaction,false));}
    }
    static List<KV> decode(Path file,ExportJobs.Chunk chunk,DistributedSchema immutableSchema)throws Exception {
        assertEquals(74,chunk.columnId());assertArrayEquals(chunk.id().contentHash(),MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));var binding=immutableSchema.column(chunk.columnId());var shape=binding.schema();var rows=new ArrayList<KV>();long physicalRows=0;
        try(var options=new Options();var reader=new SstFileReader(options);var reads=new ReadOptions().setFillCache(false)){
            reader.open(file.toString());reader.verifyChecksum();var properties=reader.getTableProperties();assertEquals("leveldb.BytewiseComparator",properties.getComparatorName());assertEquals(0,properties.getNumDeletions());assertEquals(0,properties.getNumRangeDeletions());assertEquals(0,properties.getNumMergeOperands());assertEquals(chunk.rows(),properties.getNumEntries());
            try(var iterator=reader.newIterator(reads)){
                for(iterator.seekToFirst();iterator.isValid();iterator.next()){
                    var sizing=ByteBuffer.allocate(0);assertTrue(iterator.key(sizing)+(long)iterator.value(sizing)<=DistributedBucket.MAX_BYTES);byte[] physical=iterator.key(),value=iterator.value();assertTrue(chunk.lowerInclusive()==null || Arrays.compareUnsigned(physical,chunk.lowerInclusive())>=0);assertTrue(chunk.upperExclusive()==null || Arrays.compareUnsigned(physical,chunk.upperExclusive())<0);physicalRows++;
                    if(shape.variableLengthKeysCount()==0)rows.add(new KV(immutableSchema.decodeFixedKey(binding.columnId(),physical),Buf.wrap(value)));
                    else for(var member:immutableSchema.decodeBucket(binding.columnId(),physical,value).members()){
                        var parts=new Buf[shape.keysCount()];int offset=0;for(int n=0;n<shape.fixedLengthKeysCount();n++){int width=shape.key(n);parts[n]=Buf.wrap(Arrays.copyOfRange(physical,offset,offset+width));offset+=width;}for(int n=0;n<member.tails().size();n++)parts[shape.fixedLengthKeysCount()+n]=Buf.wrap(member.tails().get(n));rows.add(new KV(new Keys(parts),Buf.wrap(member.value())));
                    }
                    assertTrue(rows.size()<=RangeBudget.DEFAULT_MAX_ITEMS,"Never publish a partial collision bucket");
                }
                iterator.status();
            }
        }
        assertEquals(chunk.rows(),physicalRows);return List.copyOf(rows);
    }
    Path download(ExportGrpcService service,ExportJobs.Chunk chunk)throws Exception {
        Path file=root.resolve("logical-"+chunk.id().groupId()+"-"+chunk.id().ordinal()+".sst");long offset=0;
        try(var output=FileChannel.open(file,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){
            while(true){var request=new ExportJobs.ReadRequest(chunk.id(),offset,ExportJobs.MIN_PAGE_BYTES);var page=ExportWire.file(service.readExport(RatisPublicExportWorkflowTest.read(chunk.id(),offset)).block(RatisPublicExportWorkflowTest.WAIT),request);var bytes=ByteBuffer.wrap(page.bytes());while(bytes.hasRemaining())output.write(bytes);offset+=page.bytes().length;if(!page.hasMore())break;assertTrue(page.bytes().length>0);}output.force(true);
        }
        assertEquals(chunk.fileBytes(),offset);return file;
    }
    @Test void collisionMigrationCommitsFenceRowsAndCheckpointBeforeLostAckAndJournalReopen()throws Exception {
        var destinationOwner=new RatisDatabaseBackendFactoryTest();destinationOwner.root=root.resolve("destination");
        try(var source=new RatisAtomicClientTest.Fixture(root.resolve("source"),true);var snapshots=RatisExportClientTest.snapshots(source);var executor=Executors.newVirtualThreadPerTaskExecutor();var target=destinationOwner.new TransactionFixture()){
            var newerSchema=new DistributedSchema(source.schema.clusterId(),source.schema.databaseId(),source.schema.domainId(),source.schema.schemaEpoch()+1,source.schema.routingPrefixBytes(),source.schema.columns().stream().filter(column->column.apiId()!=74).toList());
            assertThrows(IllegalArgumentException.class,()->newerSchema.column(74));
            RatisRocksDBAdapterTest.seed(source,74,member(2,"alpha"),value(11),1900);RatisRocksDBAdapterTest.seed(source,74,member(2,"beta"),value(22),1901);
            var targets=new ArrayList<RatisBackendSessions.Target>();for(var range:source.directory.ranges())targets.add(new RatisBackendSessions.Target(source.data,range.groupId()));targets.add(new RatisBackendSessions.Target(source.homeClient,source.homeGroup));targets.add(new RatisBackendSessions.Target(source.snapshotClient,source.snapshotGroup));
            Path jobs=root.resolve("jobs"),connectionRoot=root.resolve("connection");UUID job;List<ExportJobs.Chunk> chunks;ExportJobs.Chunk full;List<KV> logical;int captures;byte[] canonicalSchema;
            Buf firstLease=fence(1,new UUID(998,1)),secondLease=fence(2,new UUID(998,2));Keys checkpoint=key(10);
            try(var allocations=new RatisBackendAllocations(root.resolve("export-allocations"),source.catalogue.epochs,source.scheduler,3600000,4,targets);var sdk=new RatisExportClient(snapshots,source.scheduler,RatisExportBackendTest.writer(source,executor),RatisExportClient.Limits.defaults());var backend=new RatisExportBackend(jobs,source.schema,snapshots,source.scheduler,sdk,allocations,RatisExportBackend.Limits.defaults())){
                var request=new ExportJobs.Create(new UUID(998,3),List.of(74L),RatisExportBackendTest.request().limits());var service=new ExportGrpcService(backend);var created=ExportWire.status(service.createExport(ExportWire.create(request)).block(RatisPublicExportWorkflowTest.WAIT));job=created.jobId();canonicalSchema=created.schemaBytes();assertArrayEquals(DistributedSchemaCodec.encode(source.schema),canonicalSchema);assertArrayEquals(source.schema.digest(),created.decodedSchema().digest());var copy=created.schemaBytes();copy[0]^=1;assertArrayEquals(canonicalSchema,created.schemaBytes());
                for(int n=0;n<32 && RatisPublicExportWorkflowTest.status(service,job).phase()!=ExportJobs.Phase.READY;n++)service.stepExport(RatisPublicExportWorkflowTest.job(job)).block(RatisPublicExportWorkflowTest.WAIT);var ready=RatisPublicExportWorkflowTest.status(service,job);assertEquals(ExportJobs.Phase.READY,ready.phase());assertArrayEquals(canonicalSchema,ready.schemaBytes());var witnessedSchema=ready.decodedSchema();assertEquals(List.of(74L),ready.columns());captures=source.captureRequests.get();
                chunks=RatisPublicExportWorkflowTest.manifest(service,job);full=chunks.stream().filter(chunk->chunk.rows()>0).findFirst().orElseThrow();assertEquals(1,full.rows());var entry=new RatisExportRequests(jobs,RatisExportBackend.Limits.defaults().requests(),RatisExportBackend.Limits.defaults().requestBytes()).job(job);assertArrayEquals(canonicalSchema,entry.schema());assertArrayEquals(witnessedSchema.digest(),entry.fence().schemaDigest());
                try(var nativeCapture=RatisDurableSnapshot.reopen(entry.snapshot(),source.snapshots,source.data)){for(var chunk:chunks){var participant=nativeCapture.snapshot().participant(chunk.id().groupId());assertEquals(participant.route().rangeId(),chunk.id().rangeLineage());assertEquals(participant.route().rangeEpoch(),chunk.id().rangeEpoch());assertEquals(participant.captureToken().position(),new ApplyPosition(chunk.id().snapshotTerm(),chunk.id().snapshotIndex()));assertTrue(witnessedSchema.columnIds().contains(witnessedSchema.column(chunk.columnId()).columnId()));}}
                logical=decode(download(service,full),full,witnessedSchema);assertEquals(List.of(member(2,"alpha"),member(2,"beta")),logical.stream().map(KV::keys).toList());
                try(var factory=target.factory(connectionRoot)){
                    var api=factory.open(URI.create("ratis://controlled/logical-migration")).getAsyncApi(RequestContext.batch());await(api.putAsync(0,71,key(1),firstLease,RequestType.none()));await(api.putAsync(0,74,member(2,"gamma"),value(33),RequestType.none()));
                    assertEquals(Applied.CONFLICT,migrate(api,full,logical,firstLease,checkpoint,()->await(api.putAsync(0,71,key(1),secondLease,RequestType.none()))));assertNull(await(api.getAsync(0,74,member(2,"alpha"),RequestType.<Buf>current())));assertNull(await(api.getAsync(0,73,checkpoint,RequestType.<Buf>current())));assertEquals(0,RatisPublicExportWorkflowTest.status(service,job).acknowledgedCount());
                    assertEquals(Applied.COMMITTED,migrate(api,full,logical,secondLease,checkpoint,()->{}));assertArrayEquals(ExportWire.id(full.id()).toByteArray(),await(api.getAsync(0,73,checkpoint,RequestType.<Buf>current())).toByteArray());assertEquals(0,RatisPublicExportWorkflowTest.status(service,job).acknowledgedCount());
                }
            }
            int targetCaptures=target.nativeState.captureRequests.get();
            // Connection/backend journals reopen while both sets of native stores remain live.
            try(var sdk=new RatisExportClient(null,source.scheduler,null,RatisExportClient.Limits.defaults());var backend=new RatisExportBackend(jobs,newerSchema,null,source.scheduler,sdk,null,RatisExportBackend.Limits.defaults());var factory=target.factory(connectionRoot)){
                var service=new ExportGrpcService(backend);var recovered=RatisPublicExportWorkflowTest.status(service,job);assertArrayEquals(canonicalSchema,recovered.schemaBytes());assertFalse(Arrays.equals(newerSchema.digest(),recovered.decodedSchema().digest()));assertEquals(1,recovered.decodedSchema().column(74).schema().variableLengthKeysCount());var manifest=RatisPublicExportWorkflowTest.manifest(service,job);assertEquals(chunks.size(),manifest.size());for(int n=0;n<chunks.size();n++)assertArrayEquals(ExportWire.chunk(chunks.get(n)).toByteArray(),ExportWire.chunk(manifest.get(n)).toByteArray());assertEquals(logical,decode(root.resolve("logical-"+full.id().groupId()+"-"+full.id().ordinal()+".sst"),full,recovered.decodedSchema()));var api=factory.open(URI.create("ratis://controlled/logical-migration")).getAsyncApi(RequestContext.batch());assertEquals(Applied.SKIPPED,migrate(api,full,logical,secondLease,checkpoint,()->{throw new AssertionError("Committed checkpoint must skip data writes");}));assertTrue(target.nativeState.captureRequests.get()>targetCaptures,"Fence/checkpoint reread is an actual certified transaction");
                assertThrows(IOException.class,()->RatisPublicExportWorkflowTest.lose(service.acknowledgeExport(RatisPublicExportWorkflowTest.acknowledge(full.id()))));
            }
            try(var sdk=new RatisExportClient(null,source.scheduler,null,RatisExportClient.Limits.defaults());var backend=new RatisExportBackend(jobs,newerSchema,null,source.scheduler,sdk,null,RatisExportBackend.Limits.defaults());var factory=target.factory(connectionRoot)){
                var service=new ExportGrpcService(backend);var recovered=RatisPublicExportWorkflowTest.status(service,job);assertArrayEquals(canonicalSchema,recovered.schemaBytes());assertEquals(1,recovered.decodedSchema().column(74).schema().variableLengthKeysCount());var api=factory.open(URI.create("ratis://controlled/logical-migration")).getAsyncApi(RequestContext.batch());assertEquals(1,recovered.acknowledgedCount());assertEquals(Applied.SKIPPED,migrate(api,full,logical,secondLease,checkpoint,()->{throw new AssertionError();}));service.acknowledgeExport(RatisPublicExportWorkflowTest.acknowledge(full.id())).block(RatisPublicExportWorkflowTest.WAIT);assertEquals(1,RatisPublicExportWorkflowTest.status(service,job).acknowledgedCount());
                assertArrayEquals(new byte[]{11},await(api.getAsync(0,74,member(2,"alpha"),RequestType.<Buf>current())).toByteArray());assertArrayEquals(new byte[]{22},await(api.getAsync(0,74,member(2,"beta"),RequestType.<Buf>current())).toByteArray());assertArrayEquals(new byte[]{33},await(api.getAsync(0,74,member(2,"gamma"),RequestType.<Buf>current())).toByteArray());
                int index=11;for(var chunk:chunks)if(chunk.rows()==0){assertEquals(Applied.COMMITTED,migrate(api,chunk,List.of(),secondLease,key(index++),()->{}));service.acknowledgeExport(RatisPublicExportWorkflowTest.acknowledge(chunk.id())).block(RatisPublicExportWorkflowTest.WAIT);}service.releaseExport(RatisPublicExportWorkflowTest.job(job)).block(RatisPublicExportWorkflowTest.WAIT);assertEquals(ExportJobs.Phase.RELEASED,RatisPublicExportWorkflowTest.status(service,job).phase());assertEquals(captures,source.captureRequests.get());for(var store:target.nativeState.stores.values())assertEquals(0,store.activeSnapshotPins());for(var store:source.stores.values())assertEquals(0,store.activeSnapshotPins());
            }
        }
    }
}
