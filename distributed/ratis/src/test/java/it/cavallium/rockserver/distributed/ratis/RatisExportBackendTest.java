package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.export.*;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisRocksDBAdapterTest.*;

class RatisExportBackendTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static ExportJobs.Create request() {
        return new ExportJobs.Create(new UUID(290,1),List.of(71L,72L,73L,74L),new ExportJobs.Limits(32,16*1024*1024,32*1024*1024,1,1024,1024*1024,Duration.ofSeconds(5),Duration.ofSeconds(30)));
    }
    static RatisExportClient.ChunkSource writer(RatisAtomicClientTest.Fixture f,Executor executor) {
        return new RatisExportClient.ChunkSource() {
            public void bind(RatisExportJournal.Snapshot snapshot) {}
            public CompletableFuture<it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport.Chunk> write(RatisExportSourceRpc.BuildRequest request,Path file,RequestContext context) {
                return CompletableFuture.supplyAsync(() -> { try { var intent=request.intent(); return f.machines.get(intent.group()).exportSnapshotPinChunk(request.definition().capture(),pin -> f.catalogue.pool.exportSstChunk(file,pin,intent.column(),intent.scopeStart(),request.definition().scope(intent.column()).upperExclusive(),intent.limits())); } catch (Exception failure) { throw new CompletionException(failure); } },executor);
            }
            public CompletableFuture<Void> release(RatisExportSourceRpc.JobDefinition definition,RequestContext context) { return CompletableFuture.completedFuture(null); }
        };
    }
    @Test void oversizedFrozenInputRejectsBeforeParentMutationOrAllocation()throws Exception {
        var outer=new RatisSessionEpochsTest();
        try(var catalogue=outer.new Fixture(root.resolve("large-metadata"))){
            var columns=new ArrayList<DistributedSchema.Column>();var shape=ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true);for(int n=0;n<24;n++)columns.add(new DistributedSchema.Column(n+1,new UUID(291,n+1),"column-"+n+"x".repeat(4000),shape,null));var schema=new DistributedSchema(outer.cluster,outer.database,new UUID(292,1),1,0,columns);var replica=new ReplicaDescriptor(new UUID(293,1),new UUID(293,2),new UUID(293,3),"metadata","localhost:1234",ReplicaDescriptor.Role.VOTER);var directory=new RangeDirectory(outer.cluster,outer.database,schema.domainId(),1,schema.digest(),0,schema.columnIds(),0,List.of(new RangeDescriptor(new UUID(294,1),new UUID(294,2),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
            var schemas=new RatisSchemaCatalogue(catalogue.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(outer.operation(1),schema)).status());assertEquals(CommandResult.Status.SUCCESS,catalogue.epochs.catalogue(outer.directoryColumn).execute(catalogue.epochs.catalogue(outer.directoryColumn).prepare(outer.operation(2),null,directory)).status());var roster=new RatisDatabaseDomainRoster(catalogue.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);var view=roster.readComplete();assertTrue(DistributedSchemaCodec.encode(schema).length>65536);var effects=new java.util.concurrent.atomic.AtomicInteger();var scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"input-quota");
            try(var delivery=new RatisExportClient(null,scheduler,null,RatisExportClient.Limits.defaults());var backend=new RatisExportBackend(root.resolve("small-parent"),scheduler,RatisDatabaseExportWorkflowTest.denied(delivery,()->new RatisAggregateExportDriver.Vector(view.roster(),List.of(new DatabaseExportDefinition.Domain(view.directories().get(schema.domainId()),schemas.read(schema.digest()),List.of()))),effects),new RatisExportBackend.Limits(128,65536,8,16))){assertThrows(ExecutionException.class,()->await(backend.createExport(RatisDatabaseExportWorkflowTest.request(new UUID(295,1),List.of(1L)),RequestContext.batch())));assertEquals(0,effects.get());try(var entries=Files.list(root.resolve("small-parent"))){assertEquals(List.of("root.lock"),entries.map(path->path.getFileName().toString()).toList());}}
            finally{scheduler.disposeGracefully().block(Duration.ofSeconds(5));}
        }
    }
    @Test void aggregateNonceInputIsFrozenBeforeAnyAllocationOrCapture()throws Exception {
        var workflow=new RatisDatabaseExportWorkflowTest();workflow.root=root;
        try(var source=workflow.new Source(root.resolve("nonce"))){
            var original=RatisDatabaseExportWorkflowTest.request(new UUID(290,1),List.of(74L));var first=await(source.backend.createExport(original,RequestContext.batch()));var repeat=await(source.backend.createExport(original,RequestContext.batch()));assertEquals(first,repeat);assertEquals(2,first.domainCount());assertEquals(3,first.participantCount());assertEquals(0,source.ordinaryCaptures);assertEquals(0,source.f.catalogue.pool.snapshotAdmissionStats().pins());
            var changed=new ExportJobs.Create(original.requestId(),List.of(75L),original.limits());assertThrows(ExecutionException.class,()->await(source.backend.createExport(changed,RequestContext.batch())));assertArrayEquals(first.definitionDigest(),await(source.backend.exportStatus(first.jobId(),RequestContext.batch())).definitionDigest());
        }
    }
    @Test void unknownApiIdentityRejectsBeforeAParentOrNativeCaptureCanBePublished()throws Exception {
        var workflow=new RatisDatabaseExportWorkflowTest();workflow.root=root;
        try(var source=workflow.new Source(root.resolve("unknown"))){
            assertThrows(ExecutionException.class,()->await(source.backend.createExport(RatisDatabaseExportWorkflowTest.request(new UUID(290,2),List.of(99999L)),RequestContext.batch())));assertEquals(0,source.f.catalogue.pool.snapshotAdmissionStats().pins());assertEquals(0,source.ordinaryCaptures);
        }
    }
}
