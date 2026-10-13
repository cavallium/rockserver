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
    @Test void crossDomainPublicSelectionReturnsBothSchemasAndOnlyRequestedColumnChunks()throws Exception {
        var workflow=new RatisDatabaseExportWorkflowTest();workflow.root=root;
        try(var source=workflow.new Source(root.resolve("aggregate-source"))){
            var api=source.factory.open(java.net.URI.create("ratis://controlled/aggregate-source")).getAsyncApi(RequestContext.batch());
            api.putAsync(0,74,RatisAtomicClientTest.member(2,"alpha"),Buf.wrap(new byte[]{7}),RequestType.none()).get(20,TimeUnit.SECONDS);api.putAsync(0,75,RatisAtomicClientTest.key(7),Buf.wrap(new byte[]{9}),RequestType.none()).get(20,TimeUnit.SECONDS);
            var request=RatisDatabaseExportWorkflowTest.request(new UUID(997,1),List.of(74L,75L));var service=new ExportGrpcService(source.backend);var created=ExportWire.status(service.createExport(ExportWire.create(request)).block(WAIT));var progress=created;
            for(int n=0;n<128 && progress.phase()!=ExportJobs.Phase.READY;n++)progress=ExportWire.status(service.stepExport(job(created.jobId())).block(WAIT));assertEquals(ExportJobs.Phase.READY,progress.phase());assertEquals(2,progress.domainCount());
            var definition=it.cavallium.rockserver.core.common.distributed.DatabaseExportDefinition.decode(RatisDatabaseExportWorkflowTest.witness(service,progress,ExportJobs.WitnessKind.DEFINITION));var captures=it.cavallium.rockserver.core.common.distributed.DatabaseExportCaptures.decode(RatisDatabaseExportWorkflowTest.witness(service,progress,ExportJobs.WitnessKind.CAPTURES),definition);assertEquals(1,captures.cohorts().size());assertEquals(3,RatisDatabaseExportWorkflowTest.tokens(captures).size());
            var chunks=manifest(service,created.jobId());assertEquals(Set.of(74L,75L),chunks.stream().map(ExportJobs.Chunk::columnId).collect(java.util.stream.Collectors.toSet()));for(var chunk:chunks){RatisDatabaseExportWorkflowTest.qualify(definition,captures,chunk);service.acknowledgeExport(acknowledge(chunk.id())).block(WAIT);}service.releaseExport(job(created.jobId())).block(WAIT);assertEquals(ExportJobs.Phase.RELEASED,status(service,created.jobId()).phase());assertEquals(0,source.ordinaryCaptures);assertEquals(0,source.f.catalogue.pool.snapshotAdmissionStats().pins());
        }
    }
}
