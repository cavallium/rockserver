package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstImport;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled data plane and actual native catalogue; no network or Ratis server. */
class RatisRangeImportControllerTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Data implements RatisRangeImportController.DataPlane {
        final RatisRangeImportCatalogueTest.Fixture fixture;
        final Map<RatisReferencedFile.Request,RatisReferencedFile> files=new HashMap<>();
        final List<RatisReferencedFile.Request> requests=new ArrayList<>();
        final List<byte[]> installs=new ArrayList<>(),activates=new ArrayList<>(),prepares=new ArrayList<>();
        boolean loseFile,loseInstall,loseActivation,missingListener,losePrepare,crossPrepare,loseCompletion;
        NativeRangeSstImport.Activation activation;
        @FunctionalInterface interface TransferObserver { void run() throws IOException; }
        TransferObserver transferObserver=() -> {};
        Data(RatisRangeImportCatalogueTest.Fixture fixture) { this.fixture=fixture; }
        @Override public RatisReferencedFile transfer(RangeImportDefinition d,RatisReferencedFile.Request request,Path source) throws IOException {
            transferObserver.run(); assertArrayEquals(request.sha256(),RangeImportAdmission.sha256(Files.readAllBytes(source))); assertEquals(request.length(),Files.size(source)); requests.add(request);
            var value=files.computeIfAbsent(request,r -> new RatisReferencedFile(r.groupId(),1,1,r.clientId(),r.callId(),r.length(),r.sha256()));
            if(loseFile) { loseFile=false; throw new IOException("Lost forced file reply"); } return value;
        }
        @Override public NativeSstImportManifest.Receipt install(RangeImportDefinition d,RatisRangeImportRecord.Install record) throws IOException {
            record.requireDefinition(d); installs.add(record.encode()); if(loseInstall) { loseInstall=false; throw new IOException("Lost install reply"); } return RatisRangeImportCatalogueTest.admission(d,10).owners().getFirst().receipt();
        }
        @Override public NativeRangeSstImport.Activation activate(RangeImportDefinition d,RatisRangeImportRecord.Activate record) throws IOException {
            activates.add(record.encode()); activation=new NativeRangeSstImport.Activation(record.receipt(),record.publication().catalogueGroup(),record.publication().completedCatalogueCut(),record.publication().digest(),new ApplyPosition(2,6));
            if(loseActivation) { loseActivation=false; throw new IOException("Lost activate reply"); } return activation;
        }
        @Override public RangeImportAdmission.OwnerReceipt receipt(RangeImportDefinition d,RangeDescriptor range,ReplicaDescriptor owner) throws IOException {
            if(missingListener && owner.role()==ReplicaDescriptor.Role.LISTENER) throw new IOException("Listener unavailable"); return RatisRangeImportCatalogueTest.admission(d,10).owner(range.groupId(),owner.replicaId());
        }
        @Override public RangeImportActivationAdmission.OwnerActivation activation(RangeImportDefinition d,RatisRangeImportCatalogue.Head published,RangeDescriptor range,ReplicaDescriptor owner) throws IOException {
            if(missingListener && owner.role()==ReplicaDescriptor.Role.LISTENER) throw new IOException("Listener activation unavailable");
            var owners=new ArrayList<RangeImportActivationAdmission.OwnerActivation>();
            for(var installed:RatisRangeImportCatalogueTest.admission(d,10).owners()) owners.add(new RangeImportActivationAdmission.OwnerActivation(installed.groupId(),installed.rangeId(),installed.configurationVersion(),installed.owner(),RangeImportActivationAdmission.Activation.decode(activation.encode()),new ApplyPosition(2,10)));
            fixture.activations=new RangeImportActivationAdmission(d,RatisRangeImportCatalogueTest.admission(d,10),fixture.outer.catalogue,RangeImportAdmission.sha256(published.encode()),owners);
            return owners.stream().filter(p -> p.owner().equals(owner)).findFirst().orElseThrow();
        }
        @Override public RatisRangeImportApply.PublicationView publication(RangeImportDefinition d) throws IOException {
            return new RatisRangeImportApply.PublicationView(new RatisRangeImportRecord.Publication(fixture.outer.catalogue,new ApplyPosition(1,fixture.nativeCatalogue.next.get()-1),fixture.catalogue.readImport(d.importId())),d.targetDirectory());
        }
    }
    private RatisNodePlan.Catalogue binding(RatisRangeImportCatalogueTest.Fixture f) {
        return new RatisNodePlan.Catalogue(f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.outer.sessions,List.of(RatisRangeImportCatalogueTest.owner(1,ReplicaDescriptor.Role.VOTER)));
    }
    private RatisRangeImportController controller(Path journal,RatisRangeImportCatalogueTest.Fixture f,Data data) throws IOException {
        byte[][] prepared={null};
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
            byte[] bytes=((Message)args[0]).getContent().toByteArray();
            if(method.getName().equals("sendReadOnly")) {
                var packet=ScopedTransactionRpc.parse(bytes,RatisImportPublicationRpc.MAX_BYTES);
                if(packet.kind()==ScopedTransactionRpc.IMPORT_PUBLICATION_PREPARE) {
                    var command=RatisImportPublicationRpc.request(bytes); f.pure.accept(command); data.prepares.add(DistributedCommandCodec.encode(command));
                    try {
                        if(RatisRangeImportCatalogue.isPublication(command)) RatisRangeImportCatalogue.admitPublication(command,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.immutable,(definition,range,owner) -> f.missingListener ? null : data.receipt(definition,range,owner));
                        else RatisRangeImportCatalogue.admitCompletion(command,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.immutable,(definition,published,range,owner) -> f.missingActivationListener ? null : data.activation(definition,published,range,owner));
                    } catch(IllegalArgumentException rejected) { throw new IOException("Controlled fresh owner admission rejected",rejected); }
                    prepared[0]=DistributedCommandCodec.encode(command);
                    if(data.losePrepare) { data.losePrepare=false; throw new IOException("Lost owner-admission ticket reply"); }
                    int kind=data.crossPrepare ? ScopedTransactionRpc.IMPORT_PUBLICATION_CANCEL : ScopedTransactionRpc.IMPORT_PUBLICATION_PREPARE;
                    return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("catalogue-fixture")).setGroupId(RaftGroupId.valueOf(f.outer.catalogue)).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(RatisImportPublicationRpc.preparedReply(kind,command)))).build();
                }
                return f.client.io().sendReadOnly((Message)args[0]);
            }
            if(method.getName().equals("send")) {
                var command=DistributedCommandCodec.decode(bytes);
                if(RatisRangeImportCatalogue.requiresOwnerAdmission(command)) { assertArrayEquals(bytes,prepared[0],"Publication/completion requires the exact fresh prepared command"); prepared[0]=null; }
                var reply=f.client.io().send((Message)args[0]); if(data.loseCompletion && RatisRangeImportCatalogue.isCompletion(command)) { data.loseCompletion=false; throw new IOException("Lost completion reply"); } return reply;
            }
            throw new AssertionError(method);
        });
        var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) { case "getGroupId" -> f.client.getGroupId(); case "io" -> io; default -> throw new AssertionError(method); });
        return new RatisRangeImportController(journal,f.outer.cluster,f.outer.database,binding(f),client,data);
    }
    private static void until(RatisRangeImportController c,RatisRangeImportController.Handle h,RatisRangeImportController.Phase phase) throws IOException {
        for(int i=0;i<200 && h.phase()!=phase;i++) c.advance(h,1); assertEquals(phase,h.phase());
    }
    @Test void unknownRepliesReopenExactInputsAndCompleteOnlyAfterAllOwnersActivate() throws Exception {
        var catalogueTests=new RatisRangeImportCatalogueTest(); catalogueTests.root=root.resolve("catalogue"); Files.createDirectory(catalogueTests.root);
        try(var f=catalogueTests.new Fixture()) {
            var d=RatisRangeImportCatalogueTest.definition(f.outer.cluster,f.outer.database,f.operation(20)); f.seed(d.sourceDirectory()); var data=new Data(f); Path journal=Files.createDirectory(root.resolve("journal"));
            try(var c=controller(journal,f,data)) { var h=c.prepare(d,Map.of()); until(c,h,RatisRangeImportController.Phase.FILES); c.advance(h,1); // Consume the durable BEGIN acknowledgement before testing transfer.
                data.loseFile=true; assertThrows(IOException.class,() -> c.advance(h,1)); }
            try(var c=controller(journal,f,data)) {
                var h=c.reopen(d.importId()); until(c,h,RatisRangeImportController.Phase.INSTALL); assertEquals(data.requests.getFirst(),data.requests.getLast());
                data.loseInstall=true; assertThrows(IOException.class,() -> c.advance(h,1));
            }
            try(var c=controller(journal,f,data)) {
                var h=c.reopen(d.importId()); until(c,h,RatisRangeImportController.Phase.ADMISSION); assertArrayEquals(data.installs.getFirst(),data.installs.getLast());
                data.missingListener=true; assertThrows(IOException.class,() -> c.advance(h,1)); assertEquals(RatisRangeImportCatalogue.Phase.PREPARED,f.catalogue.readImport(d.importId()).phase());
                data.missingListener=false; until(c,h,RatisRangeImportController.Phase.PUBLISH); c.advance(h,1);
                data.crossPrepare=true; assertThrows(IOException.class,() -> c.advance(h,1)); data.crossPrepare=false;
                data.losePrepare=true; assertThrows(IOException.class,() -> c.advance(h,1)); f.loseReply=true; assertThrows(IOException.class,() -> c.advance(h,1));
                assertEquals(RatisRangeImportCatalogue.Phase.PUBLISHED,f.catalogue.readImport(d.importId()).phase());
            }
            byte[] published;
            try(var c=controller(journal,f,data)) {
                var h=c.reopen(d.importId()); until(c,h,RatisRangeImportController.Phase.ACTIVATE); assertArrayEquals(f.publications.getFirst(),f.publications.getLast()); assertArrayEquals(data.prepares.getFirst(),data.prepares.getLast()); published=f.catalogue.readImport(d.importId()).encode();
                data.loseActivation=true; assertThrows(IOException.class,() -> c.advance(h,1));
            }
            try(var c=controller(journal,f,data)) {
                var h=c.reopen(d.importId()); until(c,h,RatisRangeImportController.Phase.ACTIVATION_ADMISSION); assertArrayEquals(data.activates.getFirst(),data.activates.getLast());
                data.missingListener=true; assertThrows(IOException.class,() -> c.advance(h,1)); assertEquals(RatisRangeImportCatalogue.Phase.PUBLISHED,f.catalogue.readHead(d.sourceDirectory().domainId()).phase());
                data.missingListener=false; until(c,h,RatisRangeImportController.Phase.COMPLETE); c.advance(h,1); data.loseCompletion=true;
                assertThrows(IOException.class,() -> c.advance(h,1)); assertEquals(RatisRangeImportCatalogue.Phase.COMPLETE,f.catalogue.readHead(d.sourceDirectory().domainId()).phase());
            }
            try(var c=controller(journal,f,data)) {
                var h=c.reopen(d.importId()); until(c,h,RatisRangeImportController.Phase.DONE); var result=c.advance(h,1); assertEquals(RatisRangeImportCatalogue.Phase.PUBLISHED,result.published().phase()); assertEquals(RatisRangeImportCatalogue.Phase.COMPLETE,result.completed().phase()); assertArrayEquals(published,f.catalogue.readImport(d.importId()).encode());
            }
            try(var c=controller(journal,f,data)) { var h=c.reopen(d.importId()); assertEquals(RatisRangeImportController.Phase.DONE,c.advance(h,8).phase()); }
        }
    }
    @Test void ownedForcedSourcesSurviveOriginalDeletionBeforeTransfer() throws Exception {
        var catalogueTests=new RatisRangeImportCatalogueTest(); catalogueTests.root=root.resolve("catalogue"); Files.createDirectory(catalogueTests.root);
        try(var f=catalogueTests.new Fixture()) {
            var empty=RatisRangeImportCatalogueTest.definition(f.outer.cluster,f.outer.database,f.operation(20)); var m=empty.manifests().getFirst(); byte[] bytes={1,2,3}; UUID fileId=RatisRangeImportCatalogueTest.id(400);
            var file=new NativeSstImportManifest.File(fileId,bytes.length,RangeImportAdmission.sha256(bytes),1,new byte[]{1},new byte[]{1});
            var manifest=new NativeSstImportManifest(m.importId(),m.groupId(),m.rangeEpoch(),m.domainId(),m.schemaDigest(),m.lowerInclusive(),m.upperExclusive(),List.of(new NativeSstImportManifest.Column(RatisRangeImportCatalogueTest.id(21),"imported",false,List.of(file))));
            var d=new RangeImportDefinition(empty.importId(),empty.operation(),empty.sourceDirectory(),List.of(manifest)); f.seed(d.sourceDirectory()); Path source=root.resolve("original.sst"); Files.write(source,bytes); Path journal=Files.createDirectory(root.resolve("journal")); var data=new Data(f);
            try(var c=controller(journal,f,data)) { c.prepare(d,Map.of(fileId,source)); } Files.delete(source);
            try(var c=controller(journal,f,data)) { var h=c.reopen(d.importId()); until(c,h,RatisRangeImportController.Phase.INSTALL); assertEquals(2,data.requests.size()); assertEquals(data.requests.getFirst().clientId(),data.requests.getLast().clientId()); assertEquals(data.requests.getFirst().callId()+1,data.requests.getLast().callId()); assertArrayEquals(bytes,Files.readAllBytes(NativeRangeSstImport.original(journal.resolve(d.importId()+".sources"),manifest,file))); }
        }
    }
    @Test void blockedTransferKeepsOwnershipAndDoesNotHoldControllerMonitor() throws Exception {
        var catalogueTests=new RatisRangeImportCatalogueTest(); catalogueTests.root=root.resolve("catalogue"); Files.createDirectory(catalogueTests.root);
        try(var f=catalogueTests.new Fixture()) {
            var d=RatisRangeImportCatalogueTest.definition(f.outer.cluster,f.outer.database,f.operation(20)); f.seed(d.sourceDirectory()); var data=new Data(f); Path journal=Files.createDirectory(root.resolve("journal"));
            try(var c=controller(journal,f,data)) {
                var h=c.prepare(d,Map.of()); until(c,h,RatisRangeImportController.Phase.FILES); c.advance(h,1); // Drain BEGIN acknowledgement.
                var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
                data.transferObserver=() -> { entered.countDown(); try { if(!release.await(10,TimeUnit.SECONDS)) throw new IOException("Controlled transfer timed out"); } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); } };
                var executor=Executors.newSingleThreadExecutor();
                try {
                    var transfer=executor.submit(() -> c.advance(h,1)); assertTrue(entered.await(5,TimeUnit.SECONDS));
                    assertTimeoutPreemptively(java.time.Duration.ofSeconds(2),() -> { assertThrows(IOException.class,c::close); assertThrows(IOException.class,h::close); assertThrows(IOException.class,() -> c.reopen(d.importId())); });
                    assertThrows(IOException.class,() -> controller(journal,f,data)); release.countDown(); assertEquals(RatisRangeImportController.Phase.FILES,transfer.get(5,TimeUnit.SECONDS).phase());
                } finally { release.countDown(); executor.shutdownNow(); assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS)); }
            }
            try(var c=controller(journal,f,data)) { assertEquals(RatisRangeImportController.Phase.FILES,c.reopen(d.importId()).phase()); }
        }
    }

    @Test void sourcesResumeRepublishesPartialManifestAtomicallyAndCachesDefinition() throws Exception {
        var catalogueTests=new RatisRangeImportCatalogueTest(); catalogueTests.root=root.resolve("catalogue"); Files.createDirectory(catalogueTests.root);
        try(var f=catalogueTests.new Fixture()) {
            var d=RatisRangeImportCatalogueTest.definition(f.outer.cluster,f.outer.database,f.operation(20)); var data=new Data(f); Path journal=Files.createDirectory(root.resolve("journal"));
            try(var c=controller(journal,f,data)) { var h=c.prepare(d,Map.of()); assertSame(h.definition(),h.definition()); }
            Path job=journal.resolve(d.importId()+".import"); var fields=RatisSchemaClientJournal.read(job);
            fields.put("phase",RatisRangeImportController.Phase.SOURCES.name().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            fields.put("sourcePaths",java.nio.ByteBuffer.allocate(4).putInt(0).array()); RatisSchemaClientJournal.write(job,fields);
            var manifest=d.manifests().getFirst(); Path target=journal.resolve(d.importId()+".sources").resolve(manifest.groupId()+".manifest");
            Files.write(target,new byte[]{1,2}); Path pending=target.resolveSibling(target.getFileName()+".pending"); Files.write(pending,new byte[]{3});
            try(var c=controller(journal,f,data)) {
                var h=c.reopen(d.importId()); assertSame(h.definition(),h.definition()); assertEquals(RatisRangeImportController.Phase.DEFINITION,c.advance(h,1).phase());
                assertArrayEquals(manifest.encode(),Files.readAllBytes(target)); assertFalse(Files.exists(pending)); assertTrue(data.requests.isEmpty());
            }
        }
    }

}
