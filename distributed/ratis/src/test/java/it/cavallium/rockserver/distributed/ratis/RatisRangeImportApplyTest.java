package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class RatisRangeImportApplyTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID group=UUID.randomUUID(),column=UUID.randomUUID(),cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID();
    DistributedSchema schema() { return new DistributedSchema(cluster,database,domain,0,0,List.of(new DistributedSchema.Column(1,column,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null))); }
    DistributedStoragePool pool(Path path) { return DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass())); }
    record Input(DistributedSchema schema,NativeSstImportManifest manifest,RangeImportDefinition definition,byte[] bytes) {}
    Input input(String name,boolean deletion) throws Exception {
        RocksDB.loadLibrary(); Path sst=root.resolve(name+".sst");
        try(var env=new EnvOptions();var options=new Options();var writer=new SstFileWriter(env,options)) { writer.open(sst.toString()); if(deletion)writer.delete(new byte[]{2}); else writer.put(new byte[]{2},new byte[]{7}); writer.finish(); }
        byte[] bytes=Files.readAllBytes(sst); var schema=schema(); UUID id=UUID.randomUUID();
        var file=new NativeSstImportManifest.File(UUID.randomUUID(),bytes.length,MessageDigest.getInstance("SHA-256").digest(bytes),1,new byte[]{2},new byte[]{2});
        var manifest=new NativeSstImportManifest(id,group,1,domain,schema.digest(),null,null,List.of(new NativeSstImportManifest.Column(column,"data",false,List.of(file))));
        var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"local","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(UUID.randomUUID(),group,1,1,null,null,RangeDescriptor.TopologyState.FENCED,List.of(replica));
        var directory=new RangeDirectory(cluster,database,domain,0,schema.digest(),0,schema.columnIds(),0,List.of(range));
        return new Input(schema,manifest,new RangeImportDefinition(id,new OperationId(UUID.randomUUID(),1,0),directory,List.of(manifest)),bytes);
    }
    RatisRangeImportRecord.Install publish(RatisReferencedFiles owner,Input input) throws Exception {
        UUID client=UUID.randomUUID(); owner.write(RatisReferencedFilesTest.entry(group,client,1,0,input.bytes(),true)).get(10,TimeUnit.SECONDS);
        owner.write(RatisReferencedFilesTest.entry(group,client,2,1,input.manifest().encode(),true)).get(10,TimeUnit.SECONDS); owner.flush(1).get(10,TimeUnit.SECONDS);
        return new RatisRangeImportRecord.Install(input.manifest().importId(),group,1,input.definition().digest(),owner.reference(1,1),Map.of(input.manifest().columns().getFirst().files().getFirst().fileId(),owner.reference(1,0)));
    }
    RatisRangeImportRecord.Publication publication(Input input) { return new RatisRangeImportRecord.Publication(UUID.randomUUID(),new ApplyPosition(2,10),RatisRangeImportCatalogue.Head.prepared(input.definition()).publish(new byte[32])); }
    ReplicatedRangeStore reopen(DistributedStoragePool pool,Input input) throws Exception { return pool.reopenImportRange(input.manifest(),input.schema()); }

    @Test void realReferencedSstInstallPublicationAndOriginalRetriesSurviveReferenceTruncateAndReopen() throws Exception {
        var input=input("workflow",false); Path nativePath=root.resolve("native"),filesPath=root.resolve("files"); RatisRangeImportRecord.Install install; RatisRangeImportRecord.Activate activate; NativeRangeSstImport.Activation originalActivation;
        try(var pool=pool(nativePath);var files=new RatisReferencedFiles(filesPath,group,RatisReferencedFiles.Limits.defaults(),Runnable::run)) {
            var store=pool.openNewFencedRange(input.manifest(),input.schema()); install=publish(files,input); store.applyNoop(new ApplyPosition(1,0)); store.applyNoop(new ApplyPosition(1,1)); var apply=new RatisRangeImportApply(pool,store,files,input.definition());
            assertArrayEquals(install.encode(),RatisRangeImportRecord.decode(install.encode()).encode()); apply.validateInstallProposal(install); assertEquals(new ApplyPosition(1,1),store.applied()); assertNull(store.importReceipt());
            var receipt=apply.applyInstall(new ApplyPosition(1,2),install); assertTrue(store.importPending()); assertEquals(RatisRangeImportApply.Phase.INSTALLED,apply.status().phase()); assertThrows(IllegalStateException.class,store::capture);
            var published=publication(input); activate=new RatisRangeImportRecord.Activate(receipt,published); assertArrayEquals(activate.encode(),RatisRangeImportRecord.decode(activate.encode()).encode());
            assertThrows(IllegalStateException.class,()->apply.validateActivationProposal(activate,null));
            apply.validateActivationProposal(activate,definition->new RatisRangeImportApply.PublicationView(published,definition.targetDirectory())); originalActivation=apply.applyActivate(new ApplyPosition(1,3),activate);
            try(var pin=store.capture()) { assertArrayEquals(new byte[]{7},pin.get(column,new byte[]{2})); }
            files.truncate(0).get(10,TimeUnit.SECONDS); apply.validateInstallProposal(install); assertArrayEquals(receipt.encode(),apply.applyInstall(new ApplyPosition(1,4),install).encode());
            apply.validateActivationProposal(activate,definition->{ throw new AssertionError("Saved activation must not query catalogue"); }); assertArrayEquals(originalActivation.encode(),apply.applyActivate(new ApplyPosition(1,5),activate).encode()); assertEquals(RatisRangeImportApply.Phase.PUBLISHED,apply.status().phase()); files.drain().get(10,TimeUnit.SECONDS);
        }
        try(var pool=pool(nativePath);var files=new RatisReferencedFiles(filesPath,group,RatisReferencedFiles.Limits.defaults(),Runnable::run)) {
            var store=reopen(pool,input); var apply=new RatisRangeImportApply(pool,store,files,input.definition()); assertArrayEquals(activate.receipt().encode(),apply.applyInstall(new ApplyPosition(2,6),install).encode());
            apply.validateActivationProposal(activate,null); assertArrayEquals(originalActivation.encode(),apply.applyActivate(new ApplyPosition(2,7),activate).encode());
            byte[] altered=new byte[32]; altered[0]=1; var publication=activate.publication(); var changed=new RatisRangeImportRecord.Activate(activate.receipt(),new RatisRangeImportRecord.Publication(publication.catalogueGroup(),publication.completedCatalogueCut(),RatisRangeImportCatalogue.Head.prepared(input.definition()).publish(altered)));
            assertThrows(IllegalArgumentException.class,()->apply.validateActivationProposal(changed,null)); assertThrows(IllegalArgumentException.class,()->apply.applyActivate(new ApplyPosition(2,8),changed)); assertEquals(new ApplyPosition(2,7),store.applied());
            try(var pin=store.capture()) { assertArrayEquals(new byte[]{7},pin.get(column,new byte[]{2})); }
        }
    }
    @Test void cancelledObserverDoesNotReleaseReferencePinsDuringActualNativeImport() throws Exception {
        var input=input("cancel",false); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        try(var executor=Executors.newSingleThreadExecutor();var pool=pool(root.resolve("cancel-native"));var files=new RatisReferencedFiles(root.resolve("cancel-files"),group,RatisReferencedFiles.Limits.defaults(),Runnable::run)) {
            var store=pool.openNewFencedRange(input.manifest(),input.schema()); var install=publish(files,input); store.applyNoop(new ApplyPosition(1,0)); store.applyNoop(new ApplyPosition(1,1));
            var apply=new RatisRangeImportApply(pool,store,files,input.definition(),stage->{ if(stage==NativeRangeSstImport.Stage.COLUMN_INGESTED) { entered.countDown(); try { if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Controlled import barrier timeout"); } catch(InterruptedException interruption) { throw new IOException(interruption); } } });
            var actual=CompletableFuture.supplyAsync(()->{ try { return apply.applyInstall(new ApplyPosition(1,2),install); } catch(Exception failure) { throw new CompletionException(failure); } },executor); var observer=actual.copy();
            try { assertTrue(entered.await(10,TimeUnit.SECONDS)); observer.cancel(false); files.truncate(0).get(10,TimeUnit.SECONDS); assertFalse(actual.isDone()); assertThrows(IOException.class,files::close); }
            finally { release.countDown(); }
            actual.get(10,TimeUnit.SECONDS); files.drain().get(10,TimeUnit.SECONDS); assertTrue(store.importPending()); assertEquals(new ApplyPosition(1,2),store.applied()); files.close();
        } finally { release.countDown(); }
    }
    @Test void invalidSstProposalAndForeignReferenceDoNotAdvanceNativeApplyOrConsumeImport() throws Exception {
        var input=input("reject",true);
        try(var pool=pool(root.resolve("reject-native"));var files=new RatisReferencedFiles(root.resolve("reject-files"),group,RatisReferencedFiles.Limits.defaults(),Runnable::run)) {
            var store=pool.openNewFencedRange(input.manifest(),input.schema()); var record=publish(files,input); store.applyNoop(new ApplyPosition(1,0)); store.applyNoop(new ApplyPosition(1,1)); var apply=new RatisRangeImportApply(pool,store,files,input.definition());
            assertThrows(IOException.class,()->apply.validateInstallProposal(record)); assertEquals(new ApplyPosition(1,1),store.applied()); assertNull(store.importReceipt());
            var foreign=new RatisReferencedFile(UUID.randomUUID(),1,0,UUID.randomUUID(),1,1,new byte[32]); assertThrows(IllegalArgumentException.class,()->new RatisRangeImportRecord.Install(record.importId(),group,1,record.definitionDigest(),foreign,record.fileReferences()));
            assertThrows(IllegalArgumentException.class,()->record.requireCut(new ApplyPosition(1,1))); files.drain().get(10,TimeUnit.SECONDS);
        }
    }
    @Test void recoveryRequiresExactBirthAndDoesNotCreateVirginOwnershipOrPoisonNewAdmission() throws Exception {
        var input=input("recover",false); Path path=root.resolve("recover-native");
        try(var pool=pool(path)) {
            assertThrows(IllegalStateException.class,()->pool.reopenImportRange(input.manifest(),input.schema()));
            var store=pool.openNewFencedRange(input.manifest(),input.schema()); assertTrue(store.importPending()); assertEquals(RangeOwnership.State.FENCED,store.ownership().state());
        }
        try(var pool=pool(path)) {
            var changed=new NativeSstImportManifest(UUID.randomUUID(),group,1,domain,input.schema().digest(),null,null,input.manifest().columns());
            assertThrows(IllegalStateException.class,()->pool.reopenImportRange(changed,input.schema()));
            var recovered=pool.reopenImportRange(input.manifest(),input.schema()); assertTrue(recovered.importPending()); assertEquals(RangeOwnership.State.FENCED,recovered.ownership().state()); assertThrows(IllegalStateException.class,recovered::activate);
        }
    }
}
