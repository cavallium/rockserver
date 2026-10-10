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

    org.apache.ratis.statemachine.TransactionContext committed(RatisRangeStateMachine machine,long index,byte[] bytes) {
        return org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(machine).setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER)
                .setLogEntry(org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(1).setIndex(index)
                        .setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder()
                                .setLogData(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(bytes))).build()).build();
    }
    org.apache.ratis.protocol.Message message(byte[] bytes) { return org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(bytes)); }
    org.apache.ratis.statemachine.TransactionContext proposal(RatisRangeStateMachine machine,RatisRangeImportRecord record) throws Exception {
        return machine.startTransaction(org.apache.ratis.protocol.RaftClientRequest.newBuilder().setClientId(org.apache.ratis.protocol.ClientId.randomId())
                .setServerId(org.apache.ratis.protocol.RaftPeerId.valueOf("fixture")).setGroupId(org.apache.ratis.protocol.RaftGroupId.valueOf(group))
                .setCallId(1).setType(org.apache.ratis.protocol.RaftClientRequest.writeRequestType()).setMessage(message(record.encode())).build());
    }
    @Test void freshProposalsRequirePreparedCutAndCancellationRetainsActualCatalogueRead() throws Exception {
        var input=input("proposal",false); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var queries=new RatisProofQueryService();
        var reads=new java.util.concurrent.atomic.AtomicInteger(); var block=new java.util.concurrent.atomic.AtomicBoolean(true);
        var leader=new java.util.concurrent.atomic.AtomicReference<>(new RatisRangeStateMachine.Leadership(1,true));
        try(var pool=pool(root.resolve("proposal-native"));var files=new RatisReferencedFiles(root.resolve("proposal-files"),group,RatisReferencedFiles.Limits.defaults(),Runnable::run,request->RatisRangeImportRpc.requireFile(input.manifest(),request));
            var storage=org.apache.ratis.server.storage.RaftStorage.newBuilder().setDirectory(root.resolve("proposal-log").toFile())
                    .setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                    .setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build()) {
            storage.initialize(); var store=pool.openNewFencedRange(input.manifest(),input.schema()); var install=publish(files,input);
            var peer=org.apache.ratis.protocol.RaftPeer.newBuilder().setId("local").setAddress("127.0.0.1:9999").setDataStreamAddress("127.0.0.1:9997").build();
            var raft=org.apache.ratis.protocol.RaftGroup.valueOf(org.apache.ratis.protocol.RaftGroupId.valueOf(group),peer);
            var machine=new RatisRangeStateMachine(store,raft,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),command->{},command->{},leader::get);
            var published=publication(input);
            machine.rangeImportApply(new RatisRangeImportApply(pool,store,files,input.definition()),definition->{
                reads.incrementAndGet(); if(block.get()){entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Catalogue fixture timeout");}catch(InterruptedException e){throw new IOException(e);}}
                return new RatisRangeImportApply.PublicationView(published,definition.targetDirectory());
            },input.definition().sourceDirectory().ranges().getFirst().replicas().getFirst()); machine.referencedFiles(files); machine.proofQueries(queries);
            var server=(org.apache.ratis.server.RaftServer)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.server.RaftServer.class.getClassLoader(),new Class<?>[]{org.apache.ratis.server.RaftServer.class},(proxy,method,args)->{
                if(method.getName().equals("getId"))return peer.getId(); if(method.getName().equals("toString"))return "import-proposal-fixture"; throw new AssertionError(method);
            });
            machine.initialize(server,raft.getGroupId(),storage);
            var owner=input.definition().sourceDirectory().ranges().getFirst().replicas().getFirst();
            var member=org.apache.ratis.protocol.RaftPeer.newBuilder().setId(owner.peerId()).setAddress(owner.address()).setDataStreamAddress("127.0.0.1:9997").build();
            var membership=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder().addPeers(member.getRaftPeerProto()).build();
            machine.notifyConfigurationChanged(1,0,membership); machine.notifyTermIndexUpdated(1,0); machine.notifyTermIndexUpdated(1,1);
            try {
                var statusTarget=RatisRangeImportStatusRpc.Target.of(input.definition(),group,owner);
                var statusRequest=message(RatisRangeImportStatusRpc.request(statusTarget));
                var fenced=RatisRangeImportStatusRpc.decodeReply(machine.queryStale(statusRequest,0).get(5,TimeUnit.SECONDS).getContent().toByteArray(),statusTarget);
                assertEquals(RatisRangeImportApply.Phase.FENCED,fenced.phase()); assertNull(fenced.receipt());
                assertThrows(ExecutionException.class,()->machine.queryStale(statusRequest,1).get(5,TimeUnit.SECONDS));
                assertNotNull(proposal(machine,install).getException()); assertEquals(0,reads.get());
                assertThrows(ExecutionException.class,()->machine.query(message(RatisRangeImportRpc.prepare(install))).get(5,TimeUnit.SECONDS));
                var prepared=machine.queryLinearizable(message(RatisRangeImportRpc.prepare(install))).get(5,TimeUnit.SECONDS);
                RatisRangeImportRpc.requireReply(prepared.getContent().toByteArray(),ScopedTransactionRpc.IMPORT_APPLY_PREPARE,install);
                var stale=proposal(machine,install); assertNull(stale.getException()); machine.notifyConfigurationChanged(1,2,membership);
                assertThrows(IOException.class,()->machine.preAppendTransaction(stale));
                machine.notifyTermIndexUpdated(1,2);
                machine.queryLinearizable(message(RatisRangeImportRpc.prepare(install))).get(5,TimeUnit.SECONDS);
                var tx=proposal(machine,install); assertNull(tx.getException()); machine.preAppendTransaction(tx); tx.initLogEntry(1,3);
                var receipt=NativeSstImportManifest.Receipt.decode(machine.applyTransaction(tx).get(10,TimeUnit.SECONDS).getContent().toByteArray());
                leader.set(new RatisRangeStateMachine.Leadership(1,false));
                var installedStatus=RatisRangeImportStatusRpc.decodeReply(machine.queryStale(statusRequest,0).get(5,TimeUnit.SECONDS).getContent().toByteArray(),statusTarget);
                assertEquals(RatisRangeImportApply.Phase.INSTALLED,installedStatus.phase()); assertArrayEquals(receipt.encode(),installedStatus.receipt().encode());
                leader.set(new RatisRangeStateMachine.Leadership(1,true));
                var activate=new RatisRangeImportRecord.Activate(receipt,published); assertNotNull(proposal(machine,activate).getException()); assertEquals(0,reads.get());
                var pending=machine.queryLinearizable(message(RatisRangeImportRpc.prepare(activate))); assertTrue(entered.await(5,TimeUnit.SECONDS));
                machine.queryLinearizable(message(RatisRangeImportRpc.cancel(activate))).get(5,TimeUnit.SECONDS); assertNotNull(proposal(machine,activate).getException());
                assertFalse(pending.isDone()); assertEquals(1,queries.stats().jobs());
                release.countDown(); assertThrows(ExecutionException.class,()->pending.get(5,TimeUnit.SECONDS)); block.set(false);
                machine.queryLinearizable(message(RatisRangeImportRpc.prepare(activate))).get(5,TimeUnit.SECONDS);
                var activation=proposal(machine,activate); assertNull(activation.getException()); machine.preAppendTransaction(activation); activation.initLogEntry(1,4);
                var proof=machine.applyTransaction(activation).get(10,TimeUnit.SECONDS).getContent().toByteArray(); assertEquals(2,reads.get()); assertFalse(store.importPending());
                var ownerReply=machine.queryStale(statusRequest,0).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                var activatedStatus=RatisRangeImportStatusRpc.decodeReply(ownerReply,statusTarget);assertEquals(RatisRangeImportApply.Phase.PUBLISHED,activatedStatus.phase());assertArrayEquals(proof,activatedStatus.activation().encode());
                var changedOwner=new ReplicaDescriptor(owner.nodeId(),UUID.randomUUID(),owner.replicaId(),owner.peerId(),owner.address(),owner.role());
                var crossed=new RatisRangeImportStatusRpc.Target(statusTarget.cluster(),statusTarget.database(),statusTarget.domain(),statusTarget.importId(),statusTarget.group(),statusTarget.range(),statusTarget.epoch(),statusTarget.configuration(),statusTarget.definitionDigest(),statusTarget.manifestDigest(),changedOwner);
                assertThrows(IllegalArgumentException.class,()->RatisRangeImportStatusRpc.decodeReply(ownerReply,crossed));
                assertThrows(ExecutionException.class,()->machine.queryStale(message(RatisRangeImportStatusRpc.request(crossed)),0).get(5,TimeUnit.SECONDS));
                files.truncate(0).get(5,TimeUnit.SECONDS);
                machine.queryLinearizable(message(RatisRangeImportRpc.prepare(activate))).get(5,TimeUnit.SECONDS);
                var retry=proposal(machine,activate); assertNull(retry.getException()); machine.preAppendTransaction(retry); retry.initLogEntry(1,5);
                assertArrayEquals(proof,machine.applyTransaction(retry).get(5,TimeUnit.SECONDS).getContent().toByteArray()); assertEquals(2,reads.get());
                machine.queryLinearizable(message(RatisRangeImportRpc.prepare(activate))).get(5,TimeUnit.SECONDS);
                var unknown=proposal(machine,activate); assertNull(unknown.getException()); machine.preAppendTransaction(unknown); unknown.initLogEntry(1,6); machine.cancelTransaction(unknown); machine.notifyNotLeader(List.of());
                assertThrows(ExecutionException.class,()->machine.queryLinearizable(message(RatisRangeImportRpc.prepare(activate))).get(5,TimeUnit.SECONDS));
                leader.set(new RatisRangeStateMachine.Leadership(2,true));
                assertThrows(ExecutionException.class,()->machine.queryLinearizable(message(RatisRangeImportRpc.prepare(activate))).get(5,TimeUnit.SECONDS));
                machine.notifyTermIndexUpdated(2,6); machine.queryLinearizable(message(RatisRangeImportRpc.prepare(activate))).get(5,TimeUnit.SECONDS);
                var replacement=proposal(machine,activate); assertNull(replacement.getException()); machine.preAppendTransaction(replacement); replacement.initLogEntry(2,7);
                assertArrayEquals(proof,machine.applyTransaction(replacement).get(5,TimeUnit.SECONDS).getContent().toByteArray()); assertEquals(2,reads.get());
                var changed=org.apache.ratis.protocol.RaftPeer.newBuilder().setId(owner.peerId()).setAddress(owner.address()).setDataStreamAddress("127.0.0.1:9996").build();
                machine.notifyConfigurationChanged(2,8,org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder().addPeers(changed.getRaftPeerProto()).build()); machine.notifyTermIndexUpdated(2,8);
                assertThrows(ExecutionException.class,()->machine.queryStale(statusRequest,0).get(5,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,()->machine.queryLinearizable(message(RatisRangeImportRpc.prepare(activate))).get(5,TimeUnit.SECONDS)); assertEquals(2,reads.get());
                machine.notifyConfigurationChanged(2,9,membership); machine.notifyTermIndexUpdated(2,9);
                var file=input.manifest().columns().getFirst().files().getFirst(); UUID fileClient=UUID.randomUUID(); long fileCall=10;
                var referenceRequest=new RatisReferencedFile.Request(group,fileClient,fileCall,file.bytes(),file.sha256());
                var submit=org.apache.ratis.protocol.RaftClientRequest.newBuilder().setClientId(org.apache.ratis.protocol.ClientId.valueOf(fileClient))
                        .setServerId(peer.getId()).setGroupId(raft.getGroupId()).setCallId(fileCall).setType(org.apache.ratis.protocol.RaftClientRequest.writeRequestType()).setMessage(message(referenceRequest.encode())).build();
                assertNotNull(machine.startTransaction(submit).getException());
                var filePrepared=machine.queryLinearizable(message(RatisRangeImportRpc.prepare(referenceRequest))).get(5,TimeUnit.SECONDS);
                RatisRangeImportRpc.requireReply(filePrepared.getContent().toByteArray(),ScopedTransactionRpc.IMPORT_APPLY_PREPARE,referenceRequest);
                var stream=files.stream(RatisReferencedFilesTest.request(group,fileClient,fileCall,input.bytes())).get(5,TimeUnit.SECONDS);
                try {
                    var bytes=ByteBuffer.wrap(input.bytes()); while(bytes.hasRemaining())stream.getDataChannel().write(bytes); stream.getDataChannel().close();
                    var streamed=machine.startTransaction(submit); assertNull(streamed.getException()); machine.preAppendTransaction(streamed); streamed.initLogEntry(2,10);
                    files.link(stream,streamed.getLogEntry()).get(5,TimeUnit.SECONDS);
                    var reference=RatisReferencedFile.decode(machine.applyTransaction(streamed).get(5,TimeUnit.SECONDS).getContent().toByteArray());
                    assertEquals(files.reference(2,10),reference); assertEquals(referenceRequest,reference.request()); assertEquals(new ApplyPosition(2,10),store.applied());
                    try(var reader=files.pin(reference)){assertArrayEquals(input.bytes(),reader.read().toByteArray());}
                } finally { stream.cleanUp().get(5,TimeUnit.SECONDS); }
                var foreign=new RatisReferencedFile.Request(group,UUID.randomUUID(),11,file.bytes(),new byte[32]);
                assertThrows(ExecutionException.class,()->machine.queryLinearizable(message(RatisRangeImportRpc.prepare(foreign))).get(5,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,()->files.stream(RatisReferencedFilesTest.request(group,foreign.clientId(),foreign.callId(),new byte[]{1})).get(5,TimeUnit.SECONDS));
            } finally { release.countDown(); queries.stop().get(10,TimeUnit.SECONDS); machine.beginShutdown(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); }
        } finally { release.countDown(); queries.stop().get(10,TimeUnit.SECONDS); }
    }
    @Test void committedImportUsesOrderedAsyncLaneAndKeepsPinsAfterObserverCancellation() throws Exception {
        var input=input("raft-apply",false); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var queries=new RatisProofQueryService();
        var fileEntered=new CountDownLatch(1); var fileRelease=new CountDownLatch(1);
        try(var io=Executors.newSingleThreadExecutor();var pool=pool(root.resolve("raft-native"));var files=new RatisReferencedFiles(root.resolve("raft-files"),group,RatisReferencedFiles.Limits.defaults(),io);
            var storage=org.apache.ratis.server.storage.RaftStorage.newBuilder().setDirectory(root.resolve("raft-log").toFile())
                    .setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                    .setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build()) {
            storage.initialize(); var store=pool.openNewFencedRange(input.manifest(),input.schema());
            var peer=org.apache.ratis.protocol.RaftPeer.newBuilder().setId("fixture").setAddress("127.0.0.1:9999").build();
            var raft=org.apache.ratis.protocol.RaftGroup.valueOf(org.apache.ratis.protocol.RaftGroupId.valueOf(group),peer);
            var machine=new RatisRangeStateMachine(store,raft,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),command->{},command->{},()->new RatisRangeStateMachine.Leadership(1,true));
            var adapter=new RatisRangeImportApply(pool,store,files,input.definition(),stage->{if(stage==NativeRangeSstImport.Stage.COLUMN_INGESTED){entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Import fixture timeout");}catch(InterruptedException e){throw new IOException(e);}}});
            machine.rangeImportApply(adapter,definition->{throw new AssertionError("Committed replay must not query catalogue");}); machine.referencedFiles(files); machine.proofQueries(queries);
            var server=(org.apache.ratis.server.RaftServer)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.server.RaftServer.class.getClassLoader(),new Class<?>[]{org.apache.ratis.server.RaftServer.class},(proxy,method,args)->{
                if(method.getName().equals("getId"))return peer.getId(); if(method.getName().equals("toString"))return "import-apply-fixture"; throw new AssertionError(method);
            });
            machine.initialize(server,raft.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
            try {
                files.publicationObserver(stage->{if(stage==RatisReferencedFiles.PublishStage.FILE_FORCED){fileEntered.countDown();try{if(!fileRelease.await(10,TimeUnit.SECONDS))throw new IOException("File force fixture timeout");}catch(InterruptedException e){throw new IOException(e);}}});
                UUID client=UUID.randomUUID(); var fileEntry=RatisReferencedFilesTest.entry(group,client,1,1,input.bytes(),true);
                var writing=files.write(fileEntry); assertTrue(fileEntered.await(10,TimeUnit.SECONDS));
                var fileApply=org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(machine).setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).setLogEntry(fileEntry).build();
                var acknowledgment=machine.applyTransaction(fileApply); assertFalse(acknowledgment.isDone()); assertEquals(new ApplyPosition(1,0),store.applied());
                fileRelease.countDown(); writing.get(10,TimeUnit.SECONDS); var file=RatisReferencedFile.decode(acknowledgment.get(10,TimeUnit.SECONDS).getContent().toByteArray());
                assertEquals(files.reference(1,1),file); assertEquals(new ApplyPosition(1,1),store.applied());
                var manifestEntry=RatisReferencedFilesTest.entry(group,client,2,2,input.manifest().encode(),true); files.write(manifestEntry).get(10,TimeUnit.SECONDS);
                var manifestApply=org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(machine).setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).setLogEntry(manifestEntry).build();
                var manifest=RatisReferencedFile.decode(machine.applyTransaction(manifestApply).get(10,TimeUnit.SECONDS).getContent().toByteArray());
                var install=new RatisRangeImportRecord.Install(input.manifest().importId(),group,1,input.definition().digest(),manifest,Map.of(input.manifest().columns().getFirst().files().getFirst().fileId(),file));
                var pending=machine.applyTransaction(committed(machine,3,install.encode())); assertTrue(entered.await(10,TimeUnit.SECONDS));
                pending.cancel(false); files.truncate(0).get(10,TimeUnit.SECONDS); assertThrows(IOException.class,files::close);
                assertEquals(new ApplyPosition(1,2),store.applied()); assertEquals(2,machine.getLastAppliedTermIndex().getIndex());
                var expected=new NativeSstImportManifest.Receipt(input.manifest().importId(),group,1,input.manifest().digest(),new ApplyPosition(1,3));
                var activate=new RatisRangeImportRecord.Activate(expected,publication(input)); var activation=machine.applyTransaction(committed(machine,4,activate.encode()));
                machine.notifyTermIndexUpdated(1,5); assertFalse(activation.isDone()); assertEquals(new ApplyPosition(1,2),store.applied());
                release.countDown(); var proof=activation.get(10,TimeUnit.SECONDS).getContent().toByteArray(); assertEquals(new ApplyPosition(1,5),store.applied()); assertEquals(5,machine.getLastAppliedTermIndex().getIndex());
                assertFalse(store.importPending()); try(var pin=store.capture()){assertArrayEquals(new byte[]{7},pin.get(column,new byte[]{2}));}
                assertArrayEquals(expected.encode(),machine.applyTransaction(committed(machine,6,install.encode())).get(10,TimeUnit.SECONDS).getContent().toByteArray());
                assertArrayEquals(proof,machine.applyTransaction(committed(machine,7,activate.encode())).get(10,TimeUnit.SECONDS).getContent().toByteArray());
                assertEquals(new ApplyPosition(1,7),store.applied());
            } finally { fileRelease.countDown(); release.countDown(); queries.stop().get(10,TimeUnit.SECONDS); machine.beginShutdown(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); }
        } finally { fileRelease.countDown(); release.countDown(); queries.stop().get(10,TimeUnit.SECONDS); }
    }

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
