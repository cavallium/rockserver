package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual retained controller/native ownership behind controlled peer transports; no quorum claim. */
class RatisSchemaOperationOwnershipTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T>T await(CompletableFuture<T> future)throws Exception{return future.get(15,TimeUnit.SECONDS);}
    record Clock(RWScheduler value) implements AutoCloseable {public void close(){value.dispose();}}
    @FunctionalInterface interface AfterWrite {void accept(Path path,byte[] body)throws IOException;}
    static final class Journal implements RatisJournalAccess {
        final RatisJournalAccess files=RatisJournalAccess.standalone();
        volatile AfterWrite before=(path,body)->{},after=(path,body)->{};
        public RootLease backendRoot(Path path)throws IOException{return files.backendRoot(path);}
        public void register(Path path,int maximum)throws IOException{files.register(path,maximum);}
        public byte[] read(Path path,int maximum)throws IOException{return files.read(path,maximum);}
        public void write(Path path,byte[] body,int maximum)throws IOException{before.accept(path,body);files.write(path,body,maximum);after.accept(path,body);}
        public void remove(Path path,byte[] body,int maximum,SessionJournalFiles.Removal removal)throws IOException{files.remove(path,body,maximum,removal);}
        public void deleted(Path path)throws IOException{files.deleted(path);}
        public void reclaimedTree(Path path)throws IOException{files.reclaimedTree(path);}
        public void reserve(Reservation reservation)throws IOException{files.reserve(reservation);}
        public void fulfilled(Reservation reservation)throws IOException{files.fulfilled(reservation);}
    }
    static boolean terminal(Path path,byte[] body)throws IOException {
        if(!path.getFileName().toString().endsWith(".bridge"))return false;
        try(var input=new DataInputStream(new ByteArrayInputStream(body))){for(int n=0;n<4;n++)input.readLong();input.readUTF();String state=input.readUTF();return state.equals("DONE") || state.equals("ABORTED") || state.equals("CONFLICT");}
    }
    static void barrier(CountDownLatch entered,CountDownLatch release)throws IOException {
        entered.countDown();try{if(!release.await(15,TimeUnit.SECONDS))throw new IOException("Test barrier timed out");}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new IOException(failure);}
    }
    static void drained(RatisSchemaJobBridge bridge)throws Exception {
        var field=RatisSchemaJobBridge.class.getDeclaredField("lane");field.setAccessible(true);
        ((ExecutorService)field.get(bridge)).submit(()->{}).get(15,TimeUnit.SECONDS);
        assertFalse(bridge.busy(),"Actual schema owner must retire before test releases its native fixture");
    }
    static RatisSchemaJobBridge bridge(Path root,RatisSchemaClientWorkflowTest.Fixture nativeSchema,RatisRoutedClient data,RWScheduler clock,Journal files,RatisSchemaClient.TrustedOperatorResolver resolver)throws Exception {
        Path requests=root.resolve("requests"),controllers=root.resolve("schema");
        var trusted=resolver==null?(RatisSchemaClient.TrustedOperatorResolver)(name,type,hash,revision)->{throw new IOException("No operator resolution expected");}:resolver;
        return new RatisSchemaJobBridge(requests,controllers,clock,(domain,checkpoint)->RatisSchemaClient.journalOwned(controllers,nativeSchema.outer.cluster,nativeSchema.outer.database,domain,nativeSchema.catalogue,nativeSchema.client,data,nativeSchema.replicas,nativeSchema.operators,checkpoint,files),trusted,Map.of(nativeSchema.initial.domainId(),data),128,1,128L*1024*1024,files);
    }
    @Test void terminalPublicationWaitsForForceAndLostAckRetainsExactCandidate()throws Exception {
        try(var clockOwner=new Clock(RWScheduler.forTesting(1,1,1,4,4,"schema-ack"));var f=new RatisSchemaClientWorkflowTest.Fixture(root.resolve("native"),false)){
            var clock=clockOwner.value();var files=new Journal();var writes=new AtomicInteger();files.after=(path,body)->{if(terminal(path,body)){writes.incrementAndGet();throw new IOException("Lost terminal force acknowledgement");}};
            Path journals=root.resolve("bridge");UUID id;
            try(var bridge=bridge(journals,f,f.routed,clock,files,null)){
                var beforeForce=new CountDownLatch(1);var allowForce=new CountDownLatch(1);var first=new AtomicBoolean();
                files.before=(path,body)->{if(terminal(path,body) && first.compareAndSet(false,true))barrier(beforeForce,allowForce);};
                var operation=bridge.submit(f.initial.domainId(),RatisSchemaRequests.Kind.DELETE_IF_EXISTS,65536,RequestContext.batch(),(client,request)->client.prepareDeleteIfExists(request,"missing"));
                try{assertTrue(beforeForce.await(10,TimeUnit.SECONDS));var observed=bridge.inventory().getFirst();assertNull(observed.result());assertNotEquals(RatisSchemaRequests.State.DONE,observed.state());assertNull(bridge.status(observed.requestId()).result());assertFalse(operation.isDone());}finally{allowForce.countDown();}
                var failure=assertThrows(ExecutionException.class,()->await(operation));
                id=assertInstanceOf(RatisSchemaRequestPendingException.class,failure.getCause()).requestId();assertEquals(RatisSchemaRequests.State.PAUSED,bridge.status(id).state());assertNull(bridge.status(id).result());assertTrue(writes.get()>=2);
                int forced=writes.get();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
                files.after=(path,body)->{if(terminal(path,body)){writes.incrementAndGet();barrier(entered,release);}};
                var exact=bridge.resume(id,RequestContext.batch(),false);
                try{
                    assertTrue(entered.await(10,TimeUnit.SECONDS));assertFalse(exact.isDone(),"Retry cannot ACK cached result before exact row force retires");
                    assertThrows(ExecutionException.class,()->await(bridge.resume(id,RequestContext.batch(),false)));
                    var held=bridge.status(id);assertEquals(RatisSchemaRequests.State.PAUSED,held.state());assertNull(held.result());
                }finally{release.countDown();}
                var result=await(exact);assertEquals(id,result.requestId());assertFalse(result.changed());assertTrue(writes.get()>forced);
                drained(bridge);
                var verified=bridge.status(id);assertEquals(RatisSchemaRequests.State.DONE,verified.state());
                var terminalEntered=new CountDownLatch(1);var terminalRelease=new CountDownLatch(1);
                files.after=(path,body)->{if(terminal(path,body))barrier(terminalEntered,terminalRelease);};
                var terminalRetry=bridge.resume(id,RequestContext.batch(),false);
                try{assertTrue(terminalEntered.await(10,TimeUnit.SECONDS));assertThrows(ExecutionException.class,()->await(bridge.resume(id,RequestContext.batch(),false)));assertEquals(verified,bridge.status(id));}finally{terminalRelease.countDown();}
                await(terminalRetry);drained(bridge);
            }
            files.after=(path,body)->{};
            try(var cold=bridge(journals,f,f.routed,clock,files,null)){
                assertEquals(id,cold.inventory().getFirst().requestId());assertEquals(RatisSchemaRequests.State.DONE,cold.status(id).state());assertEquals(id,await(cold.resume(id,RequestContext.batch(),false)).requestId());
            }
        }
    }
    @Test void failedPrewriteTerminalForceColdReopensOriginalNonterminalOwner()throws Exception {
        try(var clockOwner=new Clock(RWScheduler.forTesting(1,1,1,4,4,"schema-before-force"));var f=new RatisSchemaClientWorkflowTest.Fixture(root.resolve("native"),false)){
            var files=new Journal();Path journals=root.resolve("bridge");UUID id;
            files.before=(path,body)->{if(terminal(path,body))throw new IOException("Terminal force never began");};
            try(var bridge=bridge(journals,f,f.routed,clockOwner.value(),files,null)){
                var failure=assertThrows(ExecutionException.class,()->await(bridge.submit(f.initial.domainId(),RatisSchemaRequests.Kind.DELETE_IF_EXISTS,65536,RequestContext.batch(),(client,request)->client.prepareDeleteIfExists(request,"missing"))));
                id=assertInstanceOf(RatisSchemaRequestPendingException.class,failure.getCause()).requestId();assertNull(bridge.status(id).result());assertEquals(RatisSchemaRequests.State.PAUSED,bridge.status(id).state());
            }
            files.before=(path,body)->{};
            try(var cold=bridge(journals,f,f.routed,clockOwner.value(),files,null)){
                assertEquals(List.of(id),cold.inventory().stream().map(RatisSchemaRequests.Status::requestId).toList());assertNull(cold.status(id).result());
                assertEquals(id,await(cold.resume(id,RequestContext.batch(),false)).requestId());assertEquals(RatisSchemaRequests.State.DONE,cold.status(id).state());assertFalse(cold.status(id).result().changed());
            }
        }
    }
    @Test void unadmittedFreshRequestHasNoRetainedTokenOrColdInventoryEntry()throws Exception {
        try(var clockOwner=new Clock(RWScheduler.forTesting(1,1,1,4,4,"schema-rejected"));var f=new RatisSchemaClientWorkflowTest.Fixture(root.resolve("native"),false)){
            var files=new Journal();Path journals=root.resolve("bridge");UUID id;var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var first=new AtomicBoolean();
            files.before=(path,body)->{if(path.getFileName().toString().endsWith(".bridge") && first.compareAndSet(false,true))barrier(entered,release);};
            try(var bridge=bridge(journals,f,f.routed,clockOwner.value(),files,null)){
                var original=bridge.submit(f.initial.domainId(),RatisSchemaRequests.Kind.DELETE_IF_EXISTS,65536,RequestContext.batch(),(client,request)->client.prepareDeleteIfExists(request,"missing"));
                try{
                    assertTrue(entered.await(10,TimeUnit.SECONDS));
                    var rejected=assertThrows(ExecutionException.class,()->await(bridge.submit(f.initial.domainId(),RatisSchemaRequests.Kind.CREATE,65536,RequestContext.batch(),(client,request)->{throw new AssertionError("Rejected work cannot create a controller");})));
                    assertEquals(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,assertInstanceOf(it.cavallium.rockserver.core.common.RocksDBException.class,rejected.getCause()).getErrorUniqueId());
                    assertTrue(bridge.inventory().isEmpty(),"Unforced initial row is not retained inventory");
                }finally{release.countDown();}
                id=await(original).requestId();assertEquals(List.of(id),bridge.inventory().stream().map(RatisSchemaRequests.Status::requestId).toList());
            }
            try(var cold=bridge(journals,f,f.routed,clockOwner.value(),files,null)){
                assertEquals(List.of(id),cold.inventory().stream().map(RatisSchemaRequests.Status::requestId).toList());assertEquals(RatisSchemaRequests.State.DONE,cold.status(id).state());
                try(var paths=Files.newDirectoryStream(journals.resolve("requests"),"*.bridge")){int count=0;for(var path:paths)count++;assertEquals(1,count);}
            }
        }
    }
    @Test void initialForceHidesTokenAndDeadlineUntilActualRetentionThenColdAbort()throws Exception {
        try(var clockOwner=new Clock(RWScheduler.forTesting(1,1,1,4,4,"schema-first-force"));var f=new RatisSchemaClientWorkflowTest.Fixture(root.resolve("native"),false)){
            var files=new Journal();Path journals=root.resolve("bridge");var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var selected=new AtomicReference<UUID>();
            files.before=(path,body)->{if(path.getFileName().toString().endsWith(".bridge") && selected.compareAndSet(null,UUID.fromString(path.getFileName().toString().replace(".bridge",""))))barrier(entered,release);};
            var nativeBefore=f.peers.getFirst().store.applied();
            try(var bridge=bridge(journals,f,f.routed,clockOwner.value(),files,null)){
                var original=bridge.submit(f.initial.domainId(),RatisSchemaRequests.Kind.CREATE,65536,RequestContext.batch(java.time.Duration.ofMillis(100)),(client,id)->{throw new AssertionError("Expired unretained work cannot create a controller");});
                try{
                    assertTrue(entered.await(10,TimeUnit.SECONDS));assertTrue(bridge.inventory().isEmpty());
                    var missing=assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class,()->bridge.status(selected.get()));assertEquals(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.SCHEMA_OPERATION_NOT_FOUND,missing.getErrorUniqueId());
                    var failure=assertThrows(ExecutionException.class,()->await(original));assertEquals(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,assertInstanceOf(it.cavallium.rockserver.core.common.RocksDBException.class,failure.getCause()).getErrorUniqueId());
                    assertFalse(failure.getCause() instanceof RatisSchemaRequestPendingException);assertTrue(bridge.inventory().isEmpty());assertFalse(Files.exists(journals.resolve("requests").resolve(selected.get()+".bridge")));
                }finally{release.countDown();}
                drained(bridge);assertEquals(RatisSchemaRequests.State.NOT_STARTED,bridge.status(selected.get()).state());assertEquals(nativeBefore,f.peers.getFirst().store.applied());
            }
            try(var cold=bridge(journals,f,f.routed,clockOwner.value(),files,null)){
                assertEquals(List.of(selected.get()),cold.inventory().stream().map(RatisSchemaRequests.Status::requestId).toList());assertEquals(RatisSchemaRequests.State.NOT_STARTED,cold.status(selected.get()).state());
                assertTrue(await(cold.resume(selected.get(),RequestContext.batch(),true)).aborted());assertEquals(nativeBefore,f.peers.getFirst().store.applied());
            }
        }
    }
    @Test void genuineCatalogueConflictIsPrivateUntilExactBridgeForceSettles()throws Exception {
        try(var clockOwner=new Clock(RWScheduler.forTesting(1,1,1,4,4,"schema-conflict-force"));var f=new RatisSchemaClientWorkflowTest.Fixture(root.resolve("native"),false)){
            var files=new Journal();Path journals=root.resolve("bridge");var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var first=new AtomicBoolean();UUID id;
            files.before=(path,body)->{if(terminal(path,body)){if(first.compareAndSet(false,true))barrier(entered,release);throw new IOException("Conflict force did not begin");}};
            try(var bridge=bridge(journals,f,f.routed,clockOwner.value(),files,null)){
                var operation=bridge.submit(f.initial.domainId(),RatisSchemaRequests.Kind.CREATE,65536,RequestContext.batch(),(client,request)->{
                    var handle=client.prepareCreate(request,"conflicted",RatisSchemaClientTest.shape());
                    var catalogue=new RatisCatalogue(f.client,f.outer.cluster,f.outer.database,f.catalogue.groupId(),f.catalogue.epoch(),f.catalogue.schemaDigest(),f.catalogue.directoryColumn());
                    var original=catalogue.read(f.initial.domainId());var next=new RangeDirectory(original.clusterId(),original.databaseId(),original.domainId(),original.schemaEpoch(),original.schemaDigest(),original.generation()+1,original.columns(),original.routingPrefixBytes(),original.ranges());
                    assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(f.outer.operation(500),original,next)).status());return handle;
                });
                try{assertTrue(entered.await(10,TimeUnit.SECONDS));var observed=bridge.inventory().getFirst();assertNotEquals(RatisSchemaRequests.State.CONFLICT,observed.state());assertNull(observed.result());assertFalse(operation.isDone());}finally{release.countDown();}
                var failure=assertThrows(ExecutionException.class,()->await(operation));id=assertInstanceOf(RatisSchemaRequestPendingException.class,failure.getCause()).requestId();
                assertEquals(RatisSchemaRequests.State.PAUSED,bridge.status(id).state());assertNull(bridge.status(id).result());
                files.before=(path,body)->{};
                assertThrows(ExecutionException.class,()->await(bridge.resume(id,RequestContext.batch(),false)));
                assertEquals(RatisSchemaRequests.State.CONFLICT,bridge.status(id).state());assertNull(bridge.status(id).result());drained(bridge);
            }
            try(var cold=bridge(journals,f,f.routed,clockOwner.value(),files,null)){
                assertEquals(RatisSchemaRequests.State.CONFLICT,cold.status(id).state());assertNull(cold.status(id).result());
                var refusal=assertThrows(ExecutionException.class,()->await(cold.resume(id,RequestContext.batch(),false)));assertEquals(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.SCHEMA_OPERATION_NOT_RESUMABLE,assertInstanceOf(it.cavallium.rockserver.core.common.RocksDBException.class,refusal.getCause()).getErrorUniqueId());
            }
        }
    }
    @Test void lostUploadBeginReplyResumesOriginalUuidAndBytesAcrossColdBridge()throws Exception {
        byte[] jar=RatisSchemaClientWorkflowTest.jar();var operator=new DistributedSchema.Operator("append",0,RatisSchemaClientWorkflowTest.AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"append-contract",1);
        try(var clockOwner=new Clock(RWScheduler.forTesting(1,1,1,4,4,"schema-upload"));var f=new RatisSchemaClientWorkflowTest.Fixture(root.resolve("native"),false,operator)){
            var clock=clockOwner.value();var files=new Journal();Path journals=root.resolve("bridge");UUID id;byte[] original;
            RatisSchemaClient.TrustedOperatorResolver resolver=(name,type,hash,revision)->operator;
            f.loseOperatorBeginReply=true;
            try(var bridge=bridge(journals,f,f.routed,clock,files,resolver)){
                var failure=assertThrows(ExecutionException.class,()->await(bridge.submit(f.initial.domainId(),RatisSchemaRequests.Kind.UPLOAD,jar.length+65536L,RequestContext.batch(),(client,request)->client.prepareUpload(request,"append",operator.className(),jar,resolver))));
                id=assertInstanceOf(RatisSchemaRequestPendingException.class,failure.getCause()).requestId();assertEquals(RatisSchemaRequests.State.PAUSED,bridge.status(id).state());assertEquals(RatisSchemaRequests.Kind.UPLOAD,bridge.status(id).kind());
                original=RatisSchemaClientJournal.read(journals.resolve("schema").resolve(id+".request")).get("pendingCommand");assertNotNull(original);
            }
            try(var cold=bridge(journals,f,f.routed,clock,files,(name,type,hash,revision)->{throw new AssertionError("Exact retained upload cannot allocate a replacement descriptor");})){
                var result=await(cold.resume(id,RequestContext.batch(),false));assertEquals(id,result.requestId());assertEquals(0,result.value());assertEquals(RatisSchemaRequests.State.DONE,cold.status(id).state());
                assertArrayEquals(original,f.operatorWrites.get(1));assertEquals(0L,await(cold.check(f.initial.domainId(),"append",OperatorUpload.sha256(jar),RequestContext.batch())));
                assertEquals(List.of(id),cold.inventory().stream().map(RatisSchemaRequests.Status::requestId).toList());drained(cold);
            }
        }
    }
    @Test void cancellationDuringActualNativeInstallRetainsOwnerAndRejectedResumePreservesRunning()throws Exception {
        try(var clockOwner=new Clock(RWScheduler.forTesting(1,1,1,4,4,"schema-native-owner"));var f=new RatisSchemaClientWorkflowTest.Fixture(root.resolve("native"),false)){
            var clock=clockOwner.value();var field=RatisRoutedClient.class.getDeclaredField("transport");field.setAccessible(true);var borrowed=(RatisRoutedClient.Transport)field.get(f.routed);
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var once=new AtomicBoolean();
            var transport=(RatisRoutedClient.Transport)java.lang.reflect.Proxy.newProxyInstance(RatisRoutedClient.Transport.class.getClassLoader(),new Class<?>[]{RatisRoutedClient.Transport.class},(proxy,method,args)->{
                if(method.getName().equals("close"))return null;
                if(method.getName().equals("participant") && RatisSchemaInstallRecord.isInstall((byte[])args[1])){
                    var record=RatisSchemaInstallRecord.decode((byte[])args[1]);NativeSchemaInstallation first=null;
                    for(var peer:f.peers){var names=new HashMap<>(peer.names);names.put(record.header().changedColumnId(),record.changedPhysicalName());var installed=peer.pool.installSchema(peer.store,peer.next(),record.definition(),names,RatisSchemaClientWorkflowTest.mergeColumns(record.definition().targetSchema()),stage->{if(stage==DistributedStoragePool.SchemaInstallStage.COMMITTED && once.compareAndSet(false,true))try{barrier(entered,release);}catch(IOException error){throw new java.io.UncheckedIOException(error);}});peer.names.clear();peer.names.putAll(names);if(first==null)first=installed;}
                    return new NativeSchemaInstallProof(first.fence(),record.definition().targetSchema().digest(),first.position()).encode();
                }
                try{return method.invoke(borrowed,args);}catch(java.lang.reflect.InvocationTargetException failure){throw failure.getCause();}
            });
            try(var data=new RatisRoutedClient(f.initialDirectory,transport);var bridge=bridge(root.resolve("bridge"),f,data,clock,new Journal(),null)){
                var action=bridge.submit(f.initial.domainId(),RatisSchemaRequests.Kind.CREATE,65536,RequestContext.batch(),(client,id)->client.prepareCreate(id,"added",RatisSchemaClientTest.shape()));
                UUID id;
                try{
                    assertTrue(entered.await(10,TimeUnit.SECONDS));id=bridge.inventory().getFirst().requestId();assertTrue(action.cancel(false));assertTrue(bridge.busy());
                    assertThrows(IOException.class,bridge::close);assertThrows(ExecutionException.class,()->await(bridge.resume(id,RequestContext.batch(),false)));
                    assertEquals(RatisSchemaRequests.State.RUNNING,bridge.status(id).state(),"Rejected observer cannot rewrite active owner state");
                }finally{release.countDown();}
                drained(bridge);
                // close() intentionally requests stop; a new bridge owns the retained exact UUID after drain.
                assertTrue(action.isCancelled());assertNotNull(f.peers.getFirst().store.installedSchema());
            }
            try(var cold=bridge(root.resolve("bridge"),f,f.routed,clock,new Journal(),null)){
                var retained=cold.inventory().getFirst();assertEquals(RatisSchemaRequests.Kind.CREATE,retained.kind());
                var done=await(cold.resume(retained.requestId(),RequestContext.batch(),false));assertTrue(done.changed());assertEquals(RatisSchemaRequests.State.DONE,cold.status(retained.requestId()).state());
            }
        }
    }
}
