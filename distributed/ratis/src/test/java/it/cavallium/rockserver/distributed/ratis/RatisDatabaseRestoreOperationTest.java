package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Production operation/service/controller and native stores; only transport boundaries are controlled, without sockets. */
class RatisDatabaseRestoreOperationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Harness implements AutoCloseable {
        final RatisDatabaseRestoreControllerTest fixture=new RatisDatabaseRestoreControllerTest();
        final RatisDatabaseRestoreControllerTest.Target target;
        RatisDatabaseRestoreOperation operation;boolean failDrain;int dataCloses,catalogueCloses;
        Harness(String name)throws Exception {fixture.root=Files.createDirectory(root.resolve(name));target=fixture.new Target();}
        RatisRoutedClient route(RangeDirectory directory){return new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
            public byte[] read(RangeDescriptor range,UUID column,byte[] key)throws IOException{return ScopedTransactionRpc.pointValue(transaction(range,ScopedTransactionRpc.pointRequest(column,key),true),DistributedCommandCodec.MAX_ENCODED_BYTES);}
            public CommandResult command(RangeDescriptor range,DistributedCommand command)throws IOException{try{return CommandResult.decode(target.append(range.groupId(),RatisDatabaseRestoreControllerTest.message(DistributedCommandCodec.encode(command))));}catch(Exception failure){throw new IOException(failure);}}
            public byte[] transaction(RangeDescriptor range,byte[] request,boolean fresh)throws IOException{try{return (fresh ? target.machine(range.groupId()).queryLinearizable(RatisDatabaseRestoreControllerTest.message(request)) : target.machine(range.groupId()).query(RatisDatabaseRestoreControllerTest.message(request))).get(10,TimeUnit.SECONDS).getContent().toByteArray();}catch(Exception failure){throw new IOException(failure);}}
            public RangeStateView stateView(RangeDescriptor range)throws IOException{return RangeStateView.decode(ScopedTransactionRpc.parse(transaction(range,ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]),true),ScopedTransactionRpc.MAX_REPLY_BYTES).body());}
        });}
        RatisDatabaseRestoreOperation.Dependencies dependencies(){return new RatisDatabaseRestoreOperation.Dependencies(manifest->{
            target.plan=manifest.plan();target.frozen=operation.plan();
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(p,m,a)->{
                try{var wire=(Message)a[0];var group=target.plan.catalogue().groupId();return switch(m.getName()){
                    case "sendReadOnly"->target.reply(group,target.machine(group).queryLinearizable(wire).get(10,TimeUnit.SECONDS).getContent().toByteArray());
                    case "send"->target.reply(group,target.append(group,wire));default->throw new AssertionError(m);
                };}catch(Exception failure){throw new IOException(failure);}
            });
            target.client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(p,m,a)->switch(m.getName()){
                case "getGroupId"->RaftGroupId.valueOf(target.plan.catalogue().groupId());case "io"->io;case "close"->{catalogueCloses++;yield null;}default->throw new AssertionError(m);
            });
            target.plan.domains().forEach(d->target.routes.put(d.directory().domainId(),route(d.directory())));
            return new RatisNodeService.Resources(target.client,target.routes,target.resources.scheduler,target.resources.timer);
        },target.fake::create,this::route,(manifest,resources)->{
            try{var service=RatisTopologySeedRuntimeTest.field(operation,"service",RatisNodeService.class);
            target.node=RatisTopologySeedRuntimeTest.field(service,"node",RatisNodeRuntime.class);
            var c=target.plan.catalogue();
            RatisTopologySeedRuntimeTest.field(target.machine(c.groupId()),"importPublicationAdmission",(RatisRangeStateMachine.ImportPublicationAdmission)command->{
                var read=(RatisRangeImportCatalogue.ImmutableReader)key->{try{return target.store(c.groupId()).readCurrentPoint(c.directoryColumn(),key);}catch(Exception failure){throw new IllegalStateException(failure);}};
                if(RatisRangeImportCatalogue.isPublication(command))RatisRangeImportCatalogue.admitPublication(command,target.plan.clusterId(),target.plan.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn(),read,target::receipt);
                else RatisRangeImportCatalogue.admitCompletion(command,target.plan.clusterId(),target.plan.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn(),read,target::activation);
            });
            return new RatisDatabaseRestoreOperation.Data(target,()->{dataCloses++;if(failDrain){failDrain=false;throw new IOException("Held actual stream close");}});
            }catch(Exception failure){throw new IOException(failure);}
        });}
        public void close()throws Exception {target.controlledUpdater.shutdown();assertTrue(target.controlledUpdater.awaitTermination(10,TimeUnit.SECONDS));target.resources.close();}
    }
    RatisDatabaseRestoreOperation.Placement placement(RatisDatabaseBackupFixture source,UUID cluster,ReplicaDescriptor owner){return placement(source.database,List.of(source.domain),cluster,owner);}
    RatisDatabaseRestoreOperation.Placement placement(UUID database,List<UUID> domains,UUID cluster,ReplicaDescriptor owner){
        UUID pool=new UUID(761,1);var homes=new ArrayList<RatisNodePlan.Home>();var groups=new HashMap<UUID,UUID>();
        for(var role:List.of(RatisNodePlan.Role.TRANSACTION_HOME,RatisNodePlan.Role.SNAPSHOT_HOME)){
            UUID domain=new UUID(762,role.ordinal()),column=new UUID(763,role.ordinal()),group=new UUID(764,role.ordinal());
            var range=new RangeDescriptor(group,group,0,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
            var directory=new RangeDirectory(cluster,database,domain,0,new byte[32],0,Set.of(column),0,List.of(range));
            homes.add(new RatisNodePlan.Home(role,directory,column,"restore-home-"+role));groups.put(group,pool);
        }
        var defaults=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,2),RatisRangeStateMachine.RetentionLimits.defaults(),java.time.Duration.ofSeconds(1),1,8,java.time.Duration.ofSeconds(5),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());
        return new RatisDatabaseRestoreOperation.Placement(owner,homes,Map.of(owner.peerId(),"127.0.0.1:9765"),List.of(RatisPoolFixtures.spec(pool,false)),domains.stream().collect(java.util.stream.Collectors.toMap(id->id,id->pool)),pool,groups,defaults,3,3,RatisSchemaReplicaService.Limits.defaults());
    }
    @Test void actualServiceRestoreReadyAdoptsFinalRoutesAndColdReopensOriginalIdentityGrant()throws Exception {
        Path archiveRoot=root.resolve("archive"),sourceRoot=root.resolve("source");var maker=new RatisDatabaseRestoreControllerTest();maker.root=root;
        var source=maker.twoDomainArchive(sourceRoot,archiveRoot,true);assertFalse(Files.exists(sourceRoot));
        UUID cluster=UUID.randomUUID(),session=UUID.randomUUID();var owner=new RatisDatabaseBackupFixture(1,2*1024*1024).fresh();
        var placement=placement(source.definition().roster().databaseId(),source.definition().roster().domainIds(),cluster,owner);
        var admitted=new java.util.concurrent.atomic.AtomicInteger();
        RatisSchemaReplicaService.OperatorPolicy policy=operator->{assertEquals("pure-append",operator.deterministicContract());admitted.incrementAndGet();return new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("pure-append",1,true);};
        Path journal=Files.createDirectory(root.resolve("restore")),nodeRoot=root.resolve("restored-node");byte[] originalIdentity,originalPlan,originalGrant,originalOwner;
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition(),source.destination());var h=new Harness("first")){
            assertTrue(archive.reopenComplete().chunks().stream().anyMatch(c->c.entries()>0));assertEquals(1,archive.reopenComplete().operators().size());
            h.operation=RatisDatabaseRestoreOperation.create(journal,nodeRoot,archive,cluster,List.of(owner),session,System.currentTimeMillis(),ConfigParser.parseDefault(),placement,policy,h.dependencies());
            try(var operation=h.operation){
                assertThrows(IOException.class,()->operation.provision(operation.plan(),Map.of()));assertTrue(Files.isRegularFile(journal.resolve("node.manifest")));assertFalse(Files.exists(nodeRoot.resolve("node.identity")));assertThrows(IOException.class,operation::service);
                for(int i=0;i<1000 && operation.phase()!=RatisDatabaseRestoreController.Phase.READY;i++)operation.step(1);
                assertEquals(RatisDatabaseRestoreController.Phase.READY,operation.phase());var readyService=operation.service();readyService.verifyCatalogue();assertThrows(IOException.class,readyService::close);
                var ledger=operation.issuedLedger();assertEquals(1,ledger.grants().size());originalGrant=ledger.encode();originalPlan=operation.plan().encode();originalIdentity=Files.readAllBytes(nodeRoot.resolve("node.identity"));originalOwner=Files.readAllBytes(nodeRoot.resolve("restore.owner"));
                requireLogicalRows(operation,h,ledger,false);
                var mergeDomain=operation.plan().domains().getFirst();var logical=new RatisLogicalClient(mergeDomain.schema(),h.target.routes.get(mergeDomain.schema().domainId()),ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());
                assertEquals(CommandResult.Status.SUCCESS,logical.execute(logical.prepareMerge(17,new it.cavallium.rockserver.core.common.Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7})),it.cavallium.buffer.Buf.wrap(new byte[]{33}),Mutation.ReturnMode.NONE)).status());
                requireLogicalRows(operation,h,ledger,true);
                for(var domain:operation.plan().domains()){
                    assertArrayEquals(domain.initialDirectory().payloadDigest(),h.target.plan.directory(domain.schema().domainId()).payloadDigest());
                    assertArrayEquals(domain.finalDirectory().payloadDigest(),h.target.routes.get(domain.schema().domainId()).directory().payloadDigest());
                }
                assertTrue(admitted.get()>0,"Independent archived operator must pass real locked staging policy");
                var backend=org.mockito.Mockito.mock(RatisDatabaseBackendFactory.class);var connection=org.mockito.Mockito.mock(RatisRocksDBConnection.class);
                org.mockito.Mockito.when(backend.open(org.mockito.ArgumentMatchers.any())).thenReturn(connection);org.mockito.Mockito.when(backend.exports()).thenReturn(org.mockito.Mockito.mock(RatisExportBackend.class));
                var accepting=new java.util.concurrent.atomic.AtomicBoolean();var listenerCloses=new java.util.concurrent.atomic.AtomicInteger();
                var listener=new it.cavallium.rockserver.core.server.Server(connection){
                    public void start(){accepting.set(true);}
                    public void close()throws IOException{if(listenerCloses.incrementAndGet()==1)throw new IOException("Held frontend listener drain");accepting.set(false);}
                };
                readyService.startFrontend(()->backend,(ignored,exports)->listener,java.net.URI.create("grpc://localhost:9770"));assertTrue(accepting.get());
                assertThrows(IOException.class,operation::close);assertEquals(1,listenerCloses.get());assertEquals(0,h.dataCloses);assertEquals(0,h.catalogueCloses);assertEquals(RatisNodeRuntime.State.RUNNING,h.target.node.state());
                h.failDrain=true;assertThrows(IOException.class,operation::close);assertFalse(accepting.get(),"Closed common listener cannot admit new USER work while native restore data remains held");assertEquals(2,listenerCloses.get());assertEquals(0,h.catalogueCloses);assertEquals(RatisNodeRuntime.State.RUNNING,h.target.node.state());assertThrows(IOException.class,readyService::verifyCatalogue);assertThrows(IOException.class,()->operation.step(1));operation.close();assertEquals(1,h.catalogueCloses);assertEquals(2,h.dataCloses);assertEquals(2,listenerCloses.get());org.mockito.Mockito.verify(backend).close();
            }
        }
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition(),source.destination());var h=new Harness("cold")){
            h.operation=RatisDatabaseRestoreOperation.reopen(journal,nodeRoot,archive,ConfigParser.parseDefault(),placement,policy,h.dependencies());
            try(var operation=h.operation){operation.step(1);operation.service().verifyCatalogue();assertArrayEquals(originalPlan,operation.plan().encode());assertArrayEquals(originalIdentity,Files.readAllBytes(nodeRoot.resolve("node.identity")));assertArrayEquals(originalOwner,Files.readAllBytes(nodeRoot.resolve("restore.owner")));var ledger=operation.issuedLedger();assertArrayEquals(originalGrant,ledger.encode());requireLogicalRows(operation,h,ledger,true);}
        }
    }
    void requireLogicalRows(RatisDatabaseRestoreOperation operation,Harness harness,SessionEpochLedger ledger,boolean merged)throws Exception {
        for(int n=0;n<operation.plan().domains().size();n++){
            var domain=operation.plan().domains().get(n);var client=new RatisLogicalClient(domain.schema(),harness.target.routes.get(domain.schema().domainId()),ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());
            assertArrayEquals(n==0 && merged?new byte[]{11,33}:new byte[]{(byte)(11*(n+1))},client.get(n==0?17:18,new it.cavallium.rockserver.core.common.Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}))).toByteArray());
            if(n==0)assertNull(client.get(99,new it.cavallium.rockserver.core.common.Keys(it.cavallium.buffer.Buf.wrap(new byte[]{7}))));
        }
    }
    @Test void partialGenesisRestoreCannotColdRecoverThroughOrdinaryServiceFrontend()throws Exception {
        var source=new RatisDatabaseBackupFixture(1,2*1024*1024);Path archiveRoot=root.resolve("archive");
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination)){archive.complete(source.manifest(source.emptyChunks()));}
        UUID cluster=UUID.randomUUID();var owner=source.fresh();var placement=placement(source,cluster,owner);Path journal=Files.createDirectory(root.resolve("restore")),node=root.resolve("node");byte[] original;
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination);var h=new Harness("genesis")){
            h.operation=RatisDatabaseRestoreOperation.create(journal,node,archive,cluster,List.of(owner),UUID.randomUUID(),System.currentTimeMillis(),ConfigParser.parseDefault(),placement,RatisNodeRuntime.SchemaReplicaConfig.defaults().policy(),h.dependencies());
            try(var operation=h.operation){for(int i=0;i<100 && operation.phase()!=RatisDatabaseRestoreController.Phase.GRANT;i++)operation.step(1);assertEquals(RatisDatabaseRestoreController.Phase.GRANT,operation.phase());assertThrows(IOException.class,operation::service);original=operation.plan().encode();assertEquals(2,SessionJournalFiles.read(node.resolve("node.identity"),65+RatisNativeExtentPolicy.BYTES)[0]);assertTrue(Files.isRegularFile(node.resolve("restore.owner")));}
        }
        var manifest=RatisNodeManifest.read(journal.resolve("node.manifest"));
        var refused=assertThrows(IOException.class,()->RatisNodeService.recover(manifest,node,ConfigParser.parseDefault()));assertTrue(refused.getMessage().contains("original operation reopen/adoption"));
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination);var h=new Harness("original-resume")){
            h.operation=RatisDatabaseRestoreOperation.reopen(journal,node,archive,ConfigParser.parseDefault(),placement,RatisNodeRuntime.SchemaReplicaConfig.defaults().policy(),h.dependencies());
            try(var operation=h.operation){for(int i=0;i<500 && operation.phase()!=RatisDatabaseRestoreController.Phase.READY;i++)operation.step(1);assertEquals(RatisDatabaseRestoreController.Phase.READY,operation.phase());assertArrayEquals(original,operation.plan().encode());operation.service().verifyCatalogue();assertEquals(1,operation.issuedLedger().grants().size());}
        }
        assertThrows(IOException.class,()->RatisNodeService.recover(manifest,node,ConfigParser.parseDefault()));
    }
    @Test void interruptedUnexposedServiceInitializationResumesExactFrozenOperation()throws Exception {
        var source=new RatisDatabaseBackupFixture(1,2*1024*1024);Path archiveRoot=root.resolve("archive");
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination)){archive.complete(source.manifest(source.emptyChunks()));}
        UUID cluster=UUID.randomUUID();var owner=source.fresh();var placement=placement(source,cluster,owner);Path journal=Files.createDirectory(root.resolve("restore")),node=root.resolve("node");byte[] frozen,identity;
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination);var h=new Harness("failed")){
            h.target.fake.failCreate=true;
            h.operation=RatisDatabaseRestoreOperation.create(journal,node,archive,cluster,List.of(owner),UUID.randomUUID(),System.currentTimeMillis(),ConfigParser.parseDefault(),placement,RatisNodeRuntime.SchemaReplicaConfig.defaults().policy(),h.dependencies());
            try(var operation=h.operation){frozen=operation.plan().encode();assertThrows(IOException.class,()->operation.step(1));identity=SessionJournalFiles.read(node.resolve("node.identity"),65+RatisNativeExtentPolicy.BYTES);assertEquals(1,identity[0]);assertEquals(RatisDatabaseRestoreController.Phase.PROVISION,operation.phase());}
        }
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination);var h=new Harness("resumed")){
            h.operation=RatisDatabaseRestoreOperation.reopen(journal,node,archive,ConfigParser.parseDefault(),placement,RatisNodeRuntime.SchemaReplicaConfig.defaults().policy(),h.dependencies());
            try(var operation=h.operation){operation.step(1);assertArrayEquals(frozen,operation.plan().encode());var resumed=SessionJournalFiles.read(node.resolve("node.identity"),65+RatisNativeExtentPolicy.BYTES);assertEquals(2,resumed[0]);assertArrayEquals(Arrays.copyOfRange(identity,1,identity.length),Arrays.copyOfRange(resumed,1,resumed.length));assertThrows(IOException.class,operation::service);}
        }
    }
    @Test void alreadyLockedNodeRejectsBeforeAuthorizedOperatorStagingCallback()throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root,false);var lockFile=java.nio.channels.FileChannel.open(env.nodeRoot.resolve("node.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);var lock=lockFile.lock()){
            var staged=new java.util.concurrent.atomic.AtomicInteger();var fake=new RatisNodeRuntimeTest.FakeServer();
            assertThrows(IOException.class,()->RatisNodeRuntime.open(env.plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer,()->20L,env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,null,null,null,RatisRoutedClient::connect,RatisNodeRuntime.SchemaReplicaConfig.defaults(),()->new RatisSchemaReplicaClient(RatisSchemaReplicaClient.MAX_CALLS),budget->{staged.incrementAndGet();Files.writeString(env.nodeRoot.resolve("operator-stage-effect"),"unexpected");}));
            assertEquals(0,staged.get());assertFalse(Files.exists(env.nodeRoot.resolve("operator-stage-effect")));assertFalse(Files.exists(env.nodeRoot.resolve("node.identity")));assertFalse(fake.created);
        }
    }
    @Test void originalManifestRejectsChangedHomeOrStreamBeforeNativeProvision()throws Exception {
        var source=new RatisDatabaseBackupFixture(1,2*1024*1024);Path archiveRoot=root.resolve("archive");
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination)){archive.complete(source.manifest(source.emptyChunks()));}
        UUID cluster=UUID.randomUUID();var owner=source.fresh();var placement=placement(source,cluster,owner);Path journal=Files.createDirectory(root.resolve("restore")),node=root.resolve("node");
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,source.definition,source.destination);var h=new Harness("unused")){
            h.operation=RatisDatabaseRestoreOperation.create(journal,node,archive,cluster,List.of(owner),UUID.randomUUID(),System.currentTimeMillis(),ConfigParser.parseDefault(),placement,RatisNodeRuntime.SchemaReplicaConfig.defaults().policy(),h.dependencies());h.operation.close();
            var changed=new RatisDatabaseRestoreOperation.Placement(owner,placement.homes(),Map.of(owner.peerId(),"127.0.0.1:9766"),placement.pools(),placement.domainPools(),placement.cataloguePool(),placement.groupPools(),placement.settings(),placement.readThreads(),placement.writeThreads(),placement.replicas());
            assertThrows(IOException.class,()->RatisDatabaseRestoreOperation.reopen(journal,node,archive,ConfigParser.parseDefault(),changed,RatisNodeRuntime.SchemaReplicaConfig.defaults().policy(),h.dependencies()));assertFalse(Files.exists(node.resolve("node.identity")));assertEquals(0,h.catalogueCloses);
        }
    }
}
