package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.export.proto.*;
import it.cavallium.rockserver.core.common.export.*;
import it.cavallium.rockserver.core.server.ExportGrpcService;
import java.io.IOException;
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
import reactor.core.publisher.Mono;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisRocksDBAdapterTest.*;

/** Actual native export through direct gRPC service/protobuf boundaries; no listener/network/quorum claim. */
class RatisPublicExportWorkflowTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final Duration WAIT=Duration.ofSeconds(20);
    static ExportJobRequest job(UUID id){return ExportJobRequest.newBuilder().setJobId(ExportWire.uuid(id)).setContext(ExportWire.context(RequestContext.batch(),RequestContext.NO_TIMEOUT)).build();}
    static ExportJobs.Status status(ExportGrpcService service,UUID id){return ExportWire.status(service.exportStatus(job(id)).block(WAIT),id);}
    static void lose(Mono<?> response)throws IOException{assertNotNull(response.block(WAIT));throw new IOException("Discarded completed typed response");}
    static List<ExportJobs.Chunk> manifest(ExportGrpcService service,UUID id)throws Exception {
        var result=new ArrayList<ExportJobs.Chunk>();int offset=0;
        for(int n=0;n<32;n++){
            var request=new ExportJobs.ManifestRequest(id,offset,1,ExportJobs.MIN_PAGE_BYTES);
            var wire=ExportManifestRequest.newBuilder().setJobId(ExportWire.uuid(id)).setOffset(offset).setMaximumItems(1).setMaximumBytes(request.maximumBytes()).setContext(ExportWire.context(RequestContext.batch(),RequestContext.NO_TIMEOUT)).build();
            var page=ExportWire.manifest(ExportManifestPage.parseFrom(service.exportManifest(wire).block(WAIT).toByteArray()),request);assertTrue(page.chunks().size()<=1);result.addAll(page.chunks());if(!page.hasMore())return List.copyOf(result);assertTrue(page.next()>offset);offset=page.next();
        }
        throw new AssertionError("Bounded manifest did not terminate");
    }
    static ExportReadRequest read(ExportJobs.ChunkId id,long offset){return ExportReadRequest.newBuilder().setId(ExportWire.id(id)).setOffset(offset).setMaximumBytes(ExportJobs.MIN_PAGE_BYTES).setContext(ExportWire.context(RequestContext.batch(),RequestContext.NO_TIMEOUT)).build();}
    static ExportAcknowledgeRequest acknowledge(ExportJobs.ChunkId id){return ExportAcknowledgeRequest.newBuilder().setId(ExportWire.id(id)).setContext(ExportWire.context(RequestContext.batch(),RequestContext.NO_TIMEOUT)).build();}
    static final class Destination implements AutoCloseable {
        final Options options=new Options().setCreateIfMissing(true);final RocksDB db;final WriteOptions sync=new WriteOptions().setDisableWAL(false).setSync(true);
        Destination(Path path)throws Exception{db=RocksDB.open(options,path.toString());}
        static byte[] receipt(ExportJobs.ChunkId id){return ByteBuffer.allocate(33).put((byte)0).put(id.contentHash()).array();}
        boolean contains(ExportJobs.ChunkId id)throws Exception {byte[] saved=db.get(receipt(id));if(saved==null)return false;assertArrayEquals(ExportWire.id(id).toByteArray(),saved);return true;}
        void install(ExportJobs.Chunk chunk,Path file)throws Exception {
            assertFalse(contains(chunk.id()));assertArrayEquals(chunk.id().contentHash(),MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
            if(chunk.rows()>0)try(var ingest=new IngestExternalFileOptions().setMoveFiles(false);var flush=new FlushOptions().setWaitForFlush(true)){db.ingestExternalFile(List.of(file.toString()),ingest);db.flush(flush);}
            try(var batch=new WriteBatch()){byte[] exact=ExportWire.id(chunk.id()).toByteArray();batch.put(receipt(chunk.id()),exact);batch.put(new byte[]{0,2},exact);db.write(sync,batch);}assertTrue(contains(chunk.id()));
        }
        public void close(){db.close();sync.close();options.close();}
    }
    Path receive(ExportGrpcService service,ExportJobs.Chunk chunk)throws Exception {
        var file=root.resolve("received-"+chunk.id().groupId()+"-"+chunk.id().ordinal());long offset=0;int pages=0;
        try(var output=FileChannel.open(file,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){
            while(true){var request=new ExportJobs.ReadRequest(chunk.id(),offset,ExportJobs.MIN_PAGE_BYTES);var page=ExportWire.file(ExportFilePage.parseFrom(service.readExport(read(chunk.id(),offset)).block(WAIT).toByteArray()),request);assertTrue(page.bytes().length<=ExportJobs.MIN_PAGE_BYTES);var bytes=ByteBuffer.wrap(page.bytes());while(bytes.hasRemaining())output.write(bytes);offset+=page.bytes().length;pages++;if(!page.hasMore())break;assertTrue(page.bytes().length>0);}
            output.force(true);
        }
        assertEquals(chunk.fileBytes(),offset);if(chunk.rows()>0)assertTrue(pages>1,"Incompressible native SST must exercise bounded multi-page delivery");return file;
    }
    @Test void directGrpcNativeCapturePagedDeliveryDurableDestinationBeforeAckAndExactJournalReopen()throws Exception {
        Path jobs=root.resolve("jobs"),destination=root.resolve("destination");UUID id;int captures;List<ExportJobs.Chunk> chunks;byte[] lostPage;
        byte[] firstValue=new byte[256*1024],secondValue=new byte[256*1024];new Random(112).nextBytes(firstValue);new Random(113).nextBytes(secondValue);
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("source"),true);var snapshots=RatisExportClientTest.snapshots(f);var executor=Executors.newVirtualThreadPerTaskExecutor()){
            seed(f,71,RatisAtomicClientTest.key(2),Buf.wrap(firstValue),1900);seed(f,71,RatisAtomicClientTest.key(200),Buf.wrap(secondValue),1901);
            var targets=new ArrayList<RatisBackendSessions.Target>();for(var range:f.directory.ranges())targets.add(new RatisBackendSessions.Target(f.data,range.groupId()));targets.add(new RatisBackendSessions.Target(f.homeClient,f.homeGroup));targets.add(new RatisBackendSessions.Target(f.snapshotClient,f.snapshotGroup));
            var request=new ExportJobs.Create(new UUID(996,1),List.of(71L),new ExportJobs.Limits(8,16L<<20,32L<<20,1,1L<<20,2L<<20,Duration.ofSeconds(10),Duration.ofSeconds(60)));var create=ExportWire.create(request).toBuilder().setContext(ExportWire.context(RequestContext.batch(),RequestContext.NO_TIMEOUT)).build();
            try(var allocations=new RatisBackendAllocations(root.resolve("allocations"),f.catalogue.epochs,f.scheduler,3600000,4,targets);var sdk=new RatisExportClient(snapshots,f.scheduler,RatisExportBackendTest.writer(f,executor),RatisExportClient.Limits.defaults());var backend=new RatisExportBackend(jobs,f.schema,snapshots,f.scheduler,sdk,allocations,RatisExportBackend.Limits.defaults())){
                var service=new ExportGrpcService(backend);assertThrows(IOException.class,()->lose(service.createExport(create)));var created=ExportWire.status(ExportStatus.parseFrom(service.createExport(create).block(WAIT).toByteArray()));id=created.jobId();assertEquals(0,f.captureRequests.get());assertEquals(3,f.catalogue.epochs.read().nextEpoch(),"Export allocation is issued by the native catalogue");
                var progress=ExportWire.status(service.stepExport(job(id)).block(WAIT),id);assertTrue(f.captureRequests.get()>0);captures=f.captureRequests.get();for(var group:f.groups)assertEquals(1,f.machines.get(group).snapshotPinStats().pins());
                for(int n=0;n<32 && progress.phase()!=ExportJobs.Phase.READY;n++)progress=ExportWire.status(service.stepExport(job(id)).block(WAIT),id);assertEquals(ExportJobs.Phase.READY,progress.phase());assertEquals(0,progress.acknowledgedCount());assertEquals(captures,f.captureRequests.get());
                for(var group:f.groups){assertEquals(0,f.machines.get(group).snapshotPinStats().pins());assertEquals(0,f.stores.get(group).activeSnapshotPins());}
                chunks=manifest(service,id);assertEquals(2,chunks.size());assertTrue(chunks.stream().allMatch(chunk->chunk.rows()==1));
                var first=chunks.getFirst();lostPage=service.readExport(read(first.id(),0)).block(WAIT).toByteArray();
                try(var target=new Destination(destination)){target.install(first,receive(service,first));assertEquals(0,status(service,id).acknowledgedCount());}
            }
            long issued=f.catalogue.epochs.read().nextEpoch();
            // Materialized delivery recovery needs no capture source, and native source stores stay live.
            try(var sdk=new RatisExportClient(null,f.scheduler,null,RatisExportClient.Limits.defaults());var backend=new RatisExportBackend(jobs,f.schema,null,f.scheduler,sdk,null,RatisExportBackend.Limits.defaults());var target=new Destination(destination)){
                var service=new ExportGrpcService(backend);assertEquals(id,ExportWire.status(service.createExport(create).block(WAIT)).jobId());var recoveredManifest=manifest(service,id);assertEquals(chunks.size(),recoveredManifest.size());for(int n=0;n<chunks.size();n++)assertArrayEquals(ExportWire.chunk(chunks.get(n)).toByteArray(),ExportWire.chunk(recoveredManifest.get(n)).toByteArray());assertEquals(captures,f.captureRequests.get());assertEquals(issued,f.catalogue.epochs.read().nextEpoch());
                var first=chunks.getFirst();assertArrayEquals(lostPage,service.readExport(read(first.id(),0)).block(WAIT).toByteArray());assertTrue(target.contains(first.id()),"Destination commit survives before the original ACK");assertThrows(IOException.class,()->lose(service.acknowledgeExport(acknowledge(first.id()))));
            }
            try(var sdk=new RatisExportClient(null,f.scheduler,null,RatisExportClient.Limits.defaults());var backend=new RatisExportBackend(jobs,f.schema,null,f.scheduler,sdk,null,RatisExportBackend.Limits.defaults());var target=new Destination(destination)){
                var service=new ExportGrpcService(backend);assertEquals(1,status(service,id).acknowledgedCount());service.acknowledgeExport(acknowledge(chunks.getFirst().id())).block(WAIT);assertEquals(1,status(service,id).acknowledgedCount());
                for(var chunk:chunks)if(!target.contains(chunk.id())){target.install(chunk,receive(service,chunk));assertTrue(target.contains(chunk.id()));service.acknowledgeExport(acknowledge(chunk.id())).block(WAIT);}
                assertArrayEquals(firstValue,target.db.get(new byte[]{2}));assertArrayEquals(secondValue,target.db.get(new byte[]{(byte)200}));assertEquals(chunks.size(),status(service,id).acknowledgedCount());service.releaseExport(job(id)).block(WAIT);assertEquals(0,sdk.stats().jobs());assertEquals(ExportJobs.Phase.RELEASED,status(service,id).phase());assertFalse(status(service,id).cleanupPending());assertEquals(captures,f.captureRequests.get());assertEquals(issued,f.catalogue.epochs.read().nextEpoch());
                for(var group:f.groups){assertEquals(0,f.machines.get(group).snapshotPinStats().pins());assertEquals(0,f.stores.get(group).activeSnapshotPins());}
            }
        }
    }
}
