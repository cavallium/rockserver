package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** One actual node owns both pools. Only the remote catalogue and Ratis division transport are controlled. */
final class RatisTwoPoolWorkflowFixture implements AutoCloseable,RatisRangeImportController.DataPlane {
    final Path root;
    final Path importSource;
    final RangeImportDefinition imported;
    final byte[] operatorJar;
    final DistributedSchema.Operator appendOperator;
    final RatisSchemaReplicaClient schemaReplicas;
    final Environment env;
    final DistributedSchema first,second;
    final UUID fast=new UUID(960,1),archive=new UUID(960,2);
    final RangeDirectory firstDirectory,secondDirectory;
    final ReplicaDescriptor local;
    final RatisNodePlan plan;
    final RatisNodeManifest manifest;
    final Map<UUID,RatisRoutedClient> routes=new LinkedHashMap<>();
    final ExecutorService updater=Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon(true).name("two-pool-workflow-updater").factory());
    final RaftClient catalogue;
    RatisNodeRuntimeTest.FakeServer server;
    RatisNodeService service;
    RatisNodeRuntime node;
    long sequence;
    RatisRangeStateMachine catalogueMachine;
    org.apache.ratis.server.storage.RaftStorage catalogueStorage;
    RatisProofQueryService importQueries,authorityQueries,catalogueProofQueries;
    RatisCommittedApplyDispatcher catalogueDispatcher;
    final Map<UUID,RatisRangeStateMachine> remoteHomes=new LinkedHashMap<>();
    final List<org.apache.ratis.server.storage.RaftStorage> homeStorage=new ArrayList<>();
    final java.util.concurrent.ThreadPoolExecutor calls=new java.util.concurrent.ThreadPoolExecutor(2,2,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(32),Thread.ofPlatform().daemon(true).name("two-pool-control-",0).factory(),new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
    RatisNodeResourceBudget remoteResources;
    final UUID remotePool=new UUID(965,1);

    RatisTwoPoolWorkflowFixture(Path root,boolean pendingImport)throws Exception {
        this.root=root;operatorJar=RatisSchemaClientWorkflowTest.jar();appendOperator=new DistributedSchema.Operator("append",0,RatisSchemaClientWorkflowTest.AppendOperator.class.getName(),OperatorUpload.sha256(operatorJar),1,1,"append-contract",1);schemaReplicas=new RatisSchemaReplicaClient(4,this::schemaOwnerClient);importSource=root.resolve("original-import.sst");Files.createDirectories(root);first=new RatisNodeServiceTest().schema();env=new Environment(root);
        try {
        local=new ReplicaDescriptor(new UUID(9,1),new UUID(9,2),new UUID(9,3),"node9","127.0.0.1:4512",ReplicaDescriptor.Role.VOTER);
        second=new DistributedSchema(first.clusterId(),first.databaseId(),new UUID(960,3),1,0,List.of(new DistributedSchema.Column(72,new UUID(960,4),"archive",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
        firstDirectory=directory(first,new UUID(961,1),local,RangeDescriptor.TopologyState.ACTIVE);secondDirectory=directory(second,new UUID(961,2),new ReplicaDescriptor(local.nodeId(),local.nodeIncarnation(),new UUID(960,5),local.peerId(),local.address(),ReplicaDescriptor.Role.VOTER),pendingImport?RangeDescriptor.TopologyState.FENCED:RangeDescriptor.TopologyState.ACTIVE);
        imported=pendingImport?importDefinition():null;
        var domains=List.of(new RatisNodePlan.Domain(firstDirectory,Map.of(first.columnIds().iterator().next(),"data"),Set.of()),new RatisNodePlan.Domain(secondDirectory,Map.of(second.columnIds().iterator().next(),"archive"),Set.of()));
        var remote=new ReplicaDescriptor(new UUID(8,1),new UUID(8,2),new UUID(8,8),"node8","127.0.0.1:4511",ReplicaDescriptor.Role.VOTER);var binding=new RatisNodePlan.Catalogue(env.outer.catalogue,1,new byte[32],env.outer.directoryColumn,env.outer.sessions,List.of(remote));
        var homes=new ArrayList<RatisNodePlan.Home>();var controlPools=new HashMap<UUID,UUID>();controlPools.put(binding.groupId(),fast);int homeNumber=0;
        for(var role:List.of(RatisNodePlan.Role.SNAPSHOT_HOME,RatisNodePlan.Role.TRANSACTION_HOME)){int id=++homeNumber;UUID group=new UUID(966,id),column=new UUID(967,id);byte[] digest=new byte[32];digest[0]=(byte)id;var owner=new ReplicaDescriptor(remote.nodeId(),remote.nodeIncarnation(),new UUID(968,id),remote.peerId(),remote.address(),ReplicaDescriptor.Role.VOTER);var range=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));var directory=new RangeDirectory(first.clusterId(),first.databaseId(),new UUID(969,id),1,digest,0,Set.of(column),0,List.of(range));homes.add(new RatisNodePlan.Home(role,directory,column,"home-"+id));controlPools.put(group,fast);}
        plan=new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),domains,binding,homes,imported==null?List.of():List.of(imported),Map.of(),8,List.of(RatisPoolFixtures.spec(fast,false),RatisPoolFixtures.spec(archive,false)),Map.of(first.domainId(),fast,second.domainId(),archive),controlPools);
        var original=env.settings();var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4<<20,16<<20,4),original.retention(),original.clockPeriod(),original.propagationVisits(),8,java.time.Duration.ofSeconds(5),original.exports(),original.resources());
        manifest=new RatisNodeManifest(List.of(first,second),plan,settings,3,3);
        routes.put(first.domainId(),routed(firstDirectory));routes.put(second.domainId(),routed(secondDirectory));catalogue=catalogue();startCatalogueMachine();
        var epochs=new RatisSessionEpochs(catalogue,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(env.outer.operation(0),null,SessionEpochLedger.initial(first.clusterId(),first.databaseId(),10))).status());startHomes(epochs);
        server=new RatisNodeRuntimeTest.FakeServer();
        try(var bootstrap=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,routes,env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server::create,List.of(first,second),null,null,this::homeClient)){
            bootstrap.start();for(var schema:List.of(first,second)){if(pendingImport && schema==second)continue;var range=plan.directory(schema.domainId()).ranges().getFirst();var store=store(range.groupId());var put=new DistributedCommand(range.groupId(),range.rangeEpoch(),schema.digest(),new OperationId(new UUID(962,1),1,schema==first?0:1),List.of(new Mutation(Mutation.Kind.PUT,schema.columnIds().iterator().next(),new byte[]{7},new byte[]{(byte)(schema==first?11:22)})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),put)).status());}
        }
        publish();server=new RatisNodeRuntimeTest.FakeServer();
        var resources=new RatisNodeService.Resources(catalogue,routes,env.scheduler,env.timer);env.resourcesTransferred=true;
        service=RatisNodeService.recover(manifest,env.nodeRoot,ConfigParser.parseDefault(),resources,server::create,this::admitOperator,this::homeClient);node=field(service,"node",RatisNodeRuntime.class);service.start();field(node,"poolPressure",RatisPoolPressureService.class).refresh().get(10,TimeUnit.SECONDS);if(!pendingImport)service.verifyCatalogue();
        }catch(Exception|Error failure){try{close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}throw failure;}
    }
    private static RangeDirectory directory(DistributedSchema schema,UUID group,ReplicaDescriptor owner,RangeDescriptor.TopologyState state){return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),1,schema.columnIds(),schema.routingPrefixBytes(),List.of(new RangeDescriptor(group,group,1,0,null,null,state,List.of(owner))));}
    private OperationId operation(){return new OperationId(new UUID(962,2),0,sequence++);}
    private void publish()throws Exception {
        var o=env.outer;var schemas=new RatisSchemaCatalogue(catalogue,o.cluster,o.database,o.catalogue,1,new byte[32],o.directoryColumn);
        assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(operation(),first)).status());
        assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(operation(),second,RatisColumnAllocation.counterValue(71),Map.of(),Map.of(),Map.of())).status());
        var directories=new RatisCatalogue(catalogue,o.cluster,o.database,o.catalogue,1,new byte[32],o.directoryColumn);
        var roster=new RatisDatabaseDomainRoster(catalogue,o.cluster,o.database,plan.catalogue());
        for(var directory:List.of(firstDirectory,secondDirectory)){var initial=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),0,directory.columns(),directory.routingPrefixBytes(),directory.ranges());assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(operation(),null,initial,roster.read())).status());assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(operation(),initial,directory)).status());}
    }
    private RatisRoutedClient routed(RangeDirectory directory){return new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
        public byte[] read(RangeDescriptor range,UUID column,byte[] key)throws IOException{try{return store(range.groupId()).readCurrentPoint(column,key);}catch(Exception failure){throw new IOException(failure);}}
        public CommandResult command(RangeDescriptor range,DistributedCommand command)throws IOException{try{return CommandResult.decode(apply(range.groupId(),DistributedCommandCodec.encode(command)).getContent().toByteArray());}catch(Exception failure){throw new IOException(failure);}}
        public byte[] transaction(RangeDescriptor range,byte[] request,boolean fresh)throws IOException{try{var machine=machine(range.groupId());var message=Message.valueOf(ByteString.copyFrom(request));return (fresh?machine.queryLinearizable(message):machine.query(message)).get(10,TimeUnit.SECONDS).getContent().toByteArray();}catch(Exception failure){throw new IOException(failure);}}
        public byte[] participant(RangeDescriptor range,byte[] record)throws IOException{try{return apply(range.groupId(),record).getContent().toByteArray();}catch(Exception failure){throw new IOException(failure);}}
        public CompletableFuture<CommandResult> commandAsync(RangeDescriptor range,DistributedCommand command){return async(()->command(range,command));}
        public CompletableFuture<byte[]> readAsync(RangeDescriptor range,UUID column,byte[] key){return async(()->read(range,column,key));}
        public CompletableFuture<byte[]> transactionAsync(RangeDescriptor range,byte[] request,boolean fresh){return async(()->transaction(range,request,fresh));}
        public CompletableFuture<byte[]> participantAsync(RangeDescriptor range,byte[] record){return async(()->participant(range,record));}
        public CompletableFuture<byte[]> snapshot(RangeDescriptor range,ReplicaDescriptor owner,byte[] request)throws IOException{var machine=machine(range.groupId());var message=Message.valueOf(ByteString.copyFrom(request));return (owner==null || ScopedTransactionRpc.kind(request,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES)==RatisSnapshotCohortRpc.OWNER?machine.queryLinearizable(message):machine.queryStale(message,0)).thenApply(reply->reply.getContent().toByteArray());}
    });}
    private RaftClient catalogue(){
        var borrowed=env.catalogue.client;var binding=plan.catalogue();var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            if(method.getName().equals("sendReadOnly")){byte[] bytes=((Message)args[0]).getContent().toByteArray();var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);if(ScopedTransactionRpc.importPublication(packet.kind()) || RatisTopologyExecutionRpc.kind(packet.kind()) || packet.kind()==RatisTopologyAuthority.KIND || packet.kind()==RatisRangeImportAuthority.KIND || packet.kind()==ScopedTransactionRpc.SCHEMA_AUTHORITY || packet.kind()==ScopedTransactionRpc.SCHEMA_FENCING_PREPARE){assertNotNull(catalogueMachine);return reply(catalogueMachine.queryLinearizable((Message)args[0]).get(10,TimeUnit.SECONDS).getContent().toByteArray());}}
            if(method.getName().equals("send")){var command=DistributedCommandCodec.decode(((Message)args[0]).getContent().toByteArray());return reply(applyCatalogue(command).getContent().toByteArray());}
            return invoke(method,borrowed.io(),args);
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "close"->null;default->invoke(method,borrowed,args);});
    }
    private static Object invoke(java.lang.reflect.Method method,Object target,Object[] arguments)throws Throwable {try{return method.invoke(target,arguments);}catch(java.lang.reflect.InvocationTargetException failure){throw failure.getCause();}}
    private RaftClientReply reply(byte[] bytes){return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("catalogue")).setGroupId(RaftGroupId.valueOf(plan.catalogue().groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build();}
    private RatisRangeStateMachine machine(UUID group){var remote=remoteHomes.get(group);return remote!=null?remote:server.machines.get(RaftGroupId.valueOf(group));}
    ReplicatedRangeStore store(UUID group)throws Exception{return field(machine(group),"store",ReplicatedRangeStore.class);}
    private <T> CompletableFuture<T> async(Callable<T> action){try{return CompletableFuture.supplyAsync(()->{try{return action.call();}catch(Exception failure){throw new CompletionException(failure);}},calls);}catch(RuntimeException rejected){return CompletableFuture.failedFuture(rejected);}}
    Message apply(UUID group,byte[] bytes)throws Exception {
        var machine=machine(group);var transaction=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(group)).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build());assertNull(transaction.getException());machine.preAppendTransaction(transaction);transaction.initLogEntry(1,store(group).applied().index()+1);
        return committedApply(machine,transaction);
    }
    private Message committedApply(RatisRangeStateMachine machine,TransactionContext transaction)throws Exception {
        return updater.submit(()->{
            var admission=machine.committedApplyAdmission();org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor descriptor;long index=transaction.getLogEntry().getIndex();try(var read=admission.awaitRead(index)){descriptor=admission.describe(transaction.getLogEntry());}var permit=admission.awaitDispatch(descriptor);admission.offer(descriptor,permit);CompletableFuture<Message> result;
            try{result=machine.applyTransaction(transaction);}finally{admission.withdraw(descriptor,permit);if(!permit.claimed())permit.close();}try(var read=admission.awaitRead(index+1)){}return result.get(5,TimeUnit.SECONDS);
        }).get(10,TimeUnit.SECONDS);
    }

    private void topologyCommand(RatisTopologyCatalogue topology,DistributedCommand command)throws Exception {
        if(RatisTopologyCatalogue.requiresOwnerAdmission(command)){
            byte[] prepared=catalogue.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(RatisTopologyExecutionRpc.prepareRequest(command)))).getMessage().getContent().toByteArray();
            RatisTopologyExecutionRpc.requireEcho(prepared,RatisTopologyExecutionRpc.PREPARE,command);
        }
        assertEquals(CommandResult.Status.SUCCESS,topology.execute(command).status());
    }
    TopologyChange.Definition split()throws Exception {
        var parent=firstDirectory.ranges().getFirst();var left=new RangeDescriptor(new UUID(963,1),new UUID(963,2),1,0,null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,List.of(local));var right=new RangeDescriptor(new UUID(963,3),new UUID(963,4),1,0,new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));var target=new RangeDirectory(first.clusterId(),first.databaseId(),first.domainId(),first.schemaEpoch(),first.digest(),2,first.columnIds(),0,List.of(left,right));var definition=new TopologyChange.Definition(new UUID(963,5),firstDirectory,target);
        var binding=plan.catalogue();var topology=new RatisTopologyCatalogue(catalogue,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        topologyCommand(topology,topology.prepareDefinition(operation(),definition,topology.readCapacity()));
        topologyCommand(topology,topology.prepareBegin(operation(),definition,topology.readHead(first.domainId()),null,null));
        topologyCommand(topology,topology.prepareFencing(operation(),definition,topology.readHead(first.domainId()),null,null));
        var view=RatisTopologyAuthority.readNative(env.catalogue.store,env.outer.directoryColumn,first.clusterId(),first.databaseId(),RatisTopologyAuthority.Request.of(definition.header(),TopologyChange.Stage.FREEZE));var freeze=new RatisTopologyFreezeRecord(definition,parent.groupId(),view.admissionDigest());var machine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologyFreezeRpc.prepareRequest(freeze)))).get(5,TimeUnit.SECONDS);var frozen=NativeTopologyFreeze.decode(apply(parent.groupId(),freeze.encode()).getContent().toByteArray());
        topologyCommand(topology,topology.prepareProof(operation(),definition,topology.readHead(first.domainId()),frozen.proof(local.replicaId())));
        var seed=node.seedTopology(new RatisTopologySeedRpc.Request(definition,binding.groupId(),parent.groupId(),local.replicaId()));
        for(var child:definition.targets())topologyCommand(topology,topology.prepareProof(operation(),definition,topology.readHead(first.domainId()),seed.proof(child.groupId())));
        topologyCommand(topology,topology.preparePublication(operation(),definition,topology.readHead(first.domainId()),null,null));
        var published=RatisTopologyAuthority.readNative(env.catalogue.store,env.outer.directoryColumn,first.clusterId(),first.databaseId(),RatisTopologyAuthority.Request.of(definition.header(),TopologyChange.Stage.ACTIVATE));
        for(var child:definition.targets()){var record=RatisTopologyActivationRecord.of(published,child.groupId());var targetMachine=server.machines.get(RaftGroupId.valueOf(child.groupId()));targetMachine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologyActivationRpc.prepareRequest(record)))).get(5,TimeUnit.SECONDS);var activation=NativeTopologyActivation.decode(apply(child.groupId(),record.encode()).getContent().toByteArray());topologyCommand(topology,topology.prepareProof(operation(),definition,topology.readHead(first.domainId()),activation.proof()));}
        topologyCommand(topology,topology.prepareRetirement(operation(),definition,topology.readHead(first.domainId()),null,null));
        assertEquals(TopologyChange.Phase.RETIRED,topology.readHead(first.domainId()).phase());return definition;
    }
    private RangeImportDefinition importDefinition()throws Exception {
        try(var environment=new org.rocksdb.EnvOptions();var options=new org.rocksdb.Options();var writer=new org.rocksdb.SstFileWriter(environment,options)){writer.open(importSource.toString());writer.put(new byte[]{7},new byte[]{44});writer.finish();}
        if(Files.size(importSource)>1L<<20)throw new IOException("Controlled import asset exceeds bound");SessionJournalFiles.force(importSource);SessionJournalFiles.force(root);byte[] bytes=Files.readAllBytes(importSource);var range=secondDirectory.ranges().getFirst();
        var file=new NativeSstImportManifest.File(new UUID(964,1),bytes.length,OperatorUpload.sha256(bytes),1,new byte[]{7},new byte[]{7});var manifest=new NativeSstImportManifest(new UUID(964,2),range.groupId(),range.rangeEpoch(),second.domainId(),second.digest(),range.lowerInclusive(),range.upperExclusive(),List.of(new NativeSstImportManifest.Column(second.columnIds().iterator().next(),"archive",false,List.of(file))),RatisRangeImportRecord.DISTRIBUTED_LIMITS);
        return new RangeImportDefinition(manifest.importId(),new OperationId(new UUID(964,3),0,0),secondDirectory,List.of(manifest));
    }
    NativeSstImportManifest.Receipt importArchivePool()throws Exception {
        assertNotNull(imported);Path journals=Files.createDirectory(root.resolve("import-controller"));var file=imported.manifests().getFirst().columns().getFirst().files().getFirst();var group=imported.manifests().getFirst().groupId();
        try(var controller=new RatisRangeImportController(journals,first.clusterId(),first.databaseId(),plan.catalogue(),catalogue,this);var handle=controller.prepare(imported,Map.of(new RatisRangeImportController.Source(group,file.fileId()),importSource))){for(int visits=0;visits<256 && handle.phase()!=RatisRangeImportController.Phase.DONE;visits++)controller.advance(handle,1);assertEquals(RatisRangeImportController.Phase.DONE,handle.phase());}
        var nativeStore=store(group);var activation=nativeStore.importActivation();assertNotNull(activation);assertEquals(plan.catalogue().groupId(),activation.catalogueGroup());activation.receipt().requireManifest(imported.manifest(group));activation.requireCut(nativeStore.applied());assertFalse(nativeStore.importPending());
        assertEquals(RangeDescriptor.TopologyState.FENCED,secondDirectory.ranges().getFirst().topologyState());assertEquals(RangeDescriptor.TopologyState.ACTIVE,imported.targetDirectory().ranges().getFirst().topologyState());
        routes.get(second.domainId()).refresh(imported.targetDirectory());service.verifyCatalogue();return status(imported,imported.targetDirectory().ranges().getFirst(),secondDirectory.ranges().getFirst().replicas().getFirst()).receipt();
    }
    private void startCatalogueMachine()throws Exception {
        var binding=plan.catalogue();
        var operatorValidator=RatisOperatorCatalogue.validator(first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),new RatisOperatorCatalogue.Limits(4,4L<<20),upload->{if(!appendOperator.equals(upload.operator()))throw new IllegalArgumentException("Unconfigured operator tuple");},this::committedValue);
        var validator=RatisCatalogue.validator(first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),binding.sessionsColumn(),operatorValidator,this::committedValue);
        var peer=RaftPeer.newBuilder().setId("two-pool-catalogue").build();var group=RaftGroup.valueOf(RaftGroupId.valueOf(binding.groupId()),peer);
        catalogueStorage=org.apache.ratis.server.storage.RaftStorage.newBuilder().setDirectory(root.resolve("remote-catalogue-raft").toFile()).setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();catalogueStorage.initialize();
        catalogueMachine=new RatisRangeStateMachine(env.catalogue.store,group,manifest.settings().images(),validator,command->{},()->new RatisRangeStateMachine.Leadership(1,true));
        var resourcePolicy=manifest.settings().resources();
        remoteResources=new RatisNodeResourceBudget(resourcePolicy.node(),Map.of(remotePool,resourcePolicy.pool()));
        var dispatchPolicy=resourcePolicy.committedApply();catalogueDispatcher=new RatisCommittedApplyDispatcher(dispatchPolicy.node(),Map.of(remotePool,dispatchPolicy.pool()));
        long maximumCommittedEntry=NativeSstImportManifest.MAX_FILE_BYTES+4096;
        for(var bound:List.of(dispatchPolicy.node(),dispatchPolicy.pool())){assertTrue(bound.read().bytes()>=RatisRangeStateMachine.committedReadWeight(maximumCommittedEntry));assertTrue(Math.min(bound.ordinary().bytes(),bound.protectedLane().bytes())>=RatisRangeStateMachine.committedExecutionWeight(maximumCommittedEntry));}
        catalogueMachine.nodeResources(remoteResources,remotePool);catalogueMachine.committedApply(catalogueDispatcher,remotePool,maximumCommittedEntry);
        catalogueMachine.role(RatisNodePlan.Role.CATALOGUE);catalogueMachine.schemaFencingReplicaClient(schemaReplicas);catalogueProofQueries=new RatisProofQueryService(RatisProofFixtures.admission());catalogueMachine.proofQueries(catalogueProofQueries);catalogueMachine.schemaCatalogue(first.clusterId(),first.databaseId(),binding.directoryColumn());
        importQueries=RatisProofQueryService.importPublicationLane(RatisProofFixtures.admission());authorityQueries=RatisProofQueryService.authorityLane(RatisProofFixtures.admission());catalogueMachine.importPublicationQueries(importQueries);catalogueMachine.schemaAuthorityQueries(authorityQueries);
        catalogueMachine.topologyExecutionAdmission(command->RatisTopologyCatalogue.admitExecution(command,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),this::committedValue,this::topologyOwner));
        catalogueMachine.importPublicationAdmission(command->{if(RatisRangeImportCatalogue.isPublication(command))RatisRangeImportCatalogue.admitPublication(command,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),this::committedValue,this::receipt);else RatisRangeImportCatalogue.admitCompletion(command,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),this::committedValue,this::activation);});
        var identity=(org.apache.ratis.server.RaftServer)Proxy.newProxyInstance(org.apache.ratis.server.RaftServer.class.getClassLoader(),new Class<?>[]{org.apache.ratis.server.RaftServer.class},(proxy,method,args)->{if(method.getName().equals("getId"))return peer.getId();if(method.getName().equals("toString"))return "two-pool-catalogue";throw new AssertionError(method);});
        catalogueMachine.initialize(identity,group.getGroupId(),catalogueStorage);catalogueMachine.notifyTermIndexUpdated(1,0);env.catalogue.next.set(1);
    }
    private RatisRoutedClient homeClient(RangeDirectory directory){return routed(directory);}
    private void startHomes(RatisSessionEpochs epochs)throws Exception {
        for(var home:plan.homes()){
            var directory=home.directory();var range=directory.ranges().getFirst();var replica=range.replicas().getFirst();var peer=RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).build();var group=RaftGroup.valueOf(RaftGroupId.valueOf(range.groupId()),peer);
            var store=env.catalogue.pool.openRange(new RangeOwnership.Owner(range.groupId(),range.rangeEpoch()),new RangeOwnership.Interval(directory.domainId(),directory.columns(),null,null),directory.schemaDigest(),Map.of(home.column(),home.physicalName()),Set.of());
            var validator=home.role()==RatisNodePlan.Role.SNAPSHOT_HOME?RatisSnapshotFences.validator(directory.clusterId(),directory.databaseId(),range.groupId(),range.rangeEpoch(),directory.schemaDigest(),home.column()):RatisTransactionStatuses.validator(directory.clusterId(),directory.databaseId(),range.groupId(),range.rangeEpoch(),directory.schemaDigest(),home.column());
            var machine=RatisRangeStateMachine.withSessions(store,group,manifest.settings().images(),validator,epochs,directory,RatisRangeStateMachine.RetentionLimits.defaults(),()->new RatisRangeStateMachine.Leadership(1,true));
            if(home.role()==RatisNodePlan.Role.SNAPSHOT_HOME){machine.role(home.role(),(key,value)->RatisSnapshotFences.validateRecord(directory.clusterId(),directory.databaseId(),range.groupId(),range.rangeEpoch(),key,value));machine.snapshotHomeStorage(directory.clusterId(),directory.databaseId(),home.column());machine.snapshotHomeProofs((fence,participant)->routes.get(fence.domainId()).snapshotFence(fence,participant));}
            else{machine.role(home.role(),(key,value)->RatisTransactionStatuses.validateRecord(directory.clusterId(),directory.databaseId(),key,value));machine.homeRecoveryReservations(directory.clusterId(),directory.databaseId(),home.column());}
            machine.nodeResources(remoteResources,remotePool);machine.committedApply(catalogueDispatcher,remotePool,NativeSstImportManifest.MAX_FILE_BYTES+4096);machine.proofQueries(catalogueProofQueries);
            var storage=org.apache.ratis.server.storage.RaftStorage.newBuilder().setDirectory(root.resolve("remote-home-raft-"+range.groupId()).toFile()).setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();homeStorage.add(storage);
            var identity=(org.apache.ratis.server.RaftServer)Proxy.newProxyInstance(org.apache.ratis.server.RaftServer.class.getClassLoader(),new Class<?>[]{org.apache.ratis.server.RaftServer.class},(proxy,method,args)->{if(method.getName().equals("getId"))return peer.getId();if(method.getName().equals("toString"))return "two-pool-home";throw new AssertionError(method);});machine.initialize(identity,group.getGroupId(),storage);machine.notifyTermIndexUpdated(1,0);remoteHomes.put(range.groupId(),machine);
        }
    }
    private it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract admitOperator(DistributedSchema.Operator operator)throws IOException {
        if(!appendOperator.equals(operator))throw new IOException("Unconfigured operator tuple");return new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("append-contract",1,true);
    }
    private RaftClient schemaOwnerClient(RangeDescriptor descriptor,ReplicaDescriptor owner){
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            assertEquals("sendStaleRead",method.getName());assertEquals(RaftPeerId.valueOf(owner.peerId()),args[2]);
            var bytes=server.machines.get(RaftGroupId.valueOf(descriptor.groupId())).queryStale((Message)args[0],0).get(10,TimeUnit.SECONDS).getContent().toByteArray();
            return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(owner.peerId())).setGroupId(RaftGroupId.valueOf(descriptor.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build();
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "close"->null;default->throw new AssertionError(method);});
    }
    record SchemaMerge(UUID column,long api,UUID request,SessionEpochLedger.Grant grant,DistributedCommand last,RangeDirectory directory){}
    SchemaMerge createOperatorAndMerge()throws Exception {
        var binding=plan.catalogue();var route=routes.get(second.domainId());var operators=new RatisOperatorCatalogue(catalogue,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),new RatisOperatorCatalogue.Limits(4,4L<<20),upload->{if(!appendOperator.equals(upload.operator()))throw new IllegalArgumentException("Unconfigured operator tuple");});
        UUID request;long api;
        try(var client=new RatisSchemaClient(Files.createDirectory(root.resolve("schema-client")),first.clusterId(),first.databaseId(),second.domainId(),binding,catalogue,route,schemaReplicas,operators)){
            try(var upload=client.prepareUpload(appendOperator,operatorJar)){advance(client,upload);assertTrue(client.checkOperator(appendOperator.name(),appendOperator.version()));
                var shape=ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true,appendOperator.name(),appendOperator.version(),appendOperator.className());
                try(var create=client.prepareCreate("merged",shape,client.uploaded(upload))){request=create.requestId();advance(client,create);api=client.completed(create).columnId();assertTrue(client.completed(create).changed());}
            }
        }
        var schemas=new RatisSchemaCatalogue(catalogue,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());var current=schemas.read(route.directory().schemaDigest());var column=current.column(api).columnId();var epochs=new RatisSessionEpochs(catalogue,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());
        var target=new RatisBackendSessions.Target(route,route.directory().ranges().getFirst().groupId(),route.directory());
        SessionEpochLedger.Grant grant;DistributedCommand last=null;
        try(var session=RatisBackendSessions.window(root.resolve("merge-window"),route,epochs,60000,new RatisSessionClient.Limits(4,1L<<20,1L<<20),List.of(target),RatisJournalAccess.standalone())){
            grant=session.grant();for(byte operand:new byte[]{11,33}){var pending=session.prepare(List.of(new Mutation(Mutation.Kind.MERGE,column,new byte[]{9},new byte[]{operand})));assertEquals(CommandResult.Status.SUCCESS,session.execute(pending).status());last=pending.command();if(operand==11)session.acknowledge(pending);}
            assertArrayEquals(new byte[]{11,33},route.read(column,new byte[]{9}));assertEquals(1,session.pending().size());
        }
        service.verifyCatalogue();assertSharedNodeResources();return new SchemaMerge(column,api,request,grant,last,route.directory());
    }
    record Backup(DatabaseBackupDefinition definition,DatabaseBackupManifest manifest,Path archive){}
    Backup backupBothPools()throws Exception {
        var binding=plan.catalogue();var epochs=new RatisSessionEpochs(catalogue,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());
        var home=plan.home(RatisNodePlan.Role.SNAPSHOT_HOME);var homeRoute=homeClient(home.directory());
        try(homeRoute){
            var homes=new RatisSnapshotFences(homeRoute,home.column());var targets=new ArrayList<RatisBackendSessions.Target>();
            for(var route:routes.values())for(var range:route.directory().ranges())targets.add(new RatisBackendSessions.Target(route,range.groupId(),route.directory()));
            var homeGroup=home.directory().ranges().getFirst().groupId();targets.add(new RatisBackendSessions.Target(homeRoute,homeGroup,home.directory()));
            Path grantRoot=root.resolve("backup-grant");var issued=RatisBackendSessions.issueBound(grantRoot,epochs,600000,targets,RatisJournalAccess.standalone());RatisBackendSessions.bind(grantRoot,epochs,issued,4096,targets,RatisJournalAccess.standalone());
            var roster=new RatisDatabaseDomainRoster(catalogue,first.clusterId(),first.databaseId(),binding).readComplete();var schemas=new RatisSchemaCatalogue(catalogue,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
            var domains=new ArrayList<DatabaseBackupDefinition.Domain>();var access=new HashMap<UUID,RatisDatabaseBackupFactory.Domain>();var reservations=new HashMap<UUID,RatisDatabaseBackupFactory.Reservation>();long sequence=0;
            for(var domain:roster.roster().domainIds()){
                var route=routes.get(domain);var directory=roster.directories().get(domain);var schema=schemas.read(directory.schemaDigest());var identity=new RatisSnapshotClient.Identity(UUID.randomUUID(),new OperationId(issued.grant().sessionId(),issued.grant().epoch(),sequence),new OperationId(issued.grant().sessionId(),issued.grant().epoch(),sequence+1));sequence=Math.addExact(identity.lastControl().sequence(),1);
                var spans=directory.columns().stream().map(column->TransactionScope.Span.range(column,null,null)).toList();
                try(var snapshots=new RatisSnapshotClient(schema,homes,route,env.scheduler,RatisSnapshotClient.Limits.defaults())){var plan=snapshots.plan(identity,new TransactionScope(spans));domains.add(new DatabaseBackupDefinition.Domain(directory,schema,List.of(plan.fence())));}
                access.put(domain,new RatisDatabaseBackupFactory.Domain(homes,route,env.scheduler));reservations.put(identity.fence(),new RatisDatabaseBackupFactory.Reservation(issued.grant(),identity));
            }
            var definition=new DatabaseBackupDefinition(new UUID(970,1),roster.roster(),domains,new DatabaseBackupDefinition.Limits(8,16,32,32L<<20,1<<20));var destination=new DatabaseBackupManifest.Destination(new UUID(970,2),new UUID(970,3));var policy=new RatisDatabaseBackupDriver.Policy(java.time.Duration.ofSeconds(60),java.time.Duration.ofSeconds(30),2);
            var exportLimits=new RatisExportJournal.Limits(32,12L<<20,32L<<20,new NativeRangeSstExport.Limits(64,1L<<20,2L<<20,java.time.Duration.ofSeconds(5)));
            Path operatorInput=root.resolve("operator-input.jar");Files.write(operatorInput,operatorJar,StandardOpenOption.CREATE_NEW);SessionJournalFiles.force(operatorInput);SessionJournalFiles.force(root);
            Path factoryRoot=root.resolve("database-backup");
            try(var factory=new RatisDatabaseBackupFactory(factoryRoot,new RatisDatabaseBackupFactory.Limits(1,2,256L<<20));var operation=factory.create(definition,access,reservations,destination,policy,exportLimits,()->{
                var current=new RatisDatabaseDomainRoster(catalogue,first.clusterId(),first.databaseId(),binding).readComplete();var vector=new ArrayList<DatabaseBackupDefinition.Domain>();for(var declared:definition.domains()){var directory=current.directories().get(declared.directory().domainId());vector.add(new DatabaseBackupDefinition.Domain(directory,schemas.read(directory.schemaDigest()),declared.fences()));}return new RatisDatabaseBackupDriver.Vector(current.roster(),vector);
            },operator->{if(!appendOperator.equals(operator))throw new IOException("Unconfigured backup operator");return operatorInput;})){
                RatisDatabaseBackupDriver.Progress progress=null;for(int visits=0;visits<512;visits++){progress=operation.step(it.cavallium.rockserver.core.common.RequestContext.batch());if(progress.complete() && !progress.cleanupPending())break;}assertNotNull(progress);assertTrue(progress.complete());assertFalse(progress.cleanupPending());
                var captured=operation.snapshot();assertEquals(2,captured.requests().size());assertEquals(Set.of(fast,archive),captured.requests().stream().map(RatisDatabaseBackupJournal.CohortRequest::poolId).collect(java.util.stream.Collectors.toSet()));assertEquals(Set.of(plan.nodeIncarnation()),captured.requests().stream().map(RatisDatabaseBackupJournal.CohortRequest::nodeIncarnation).collect(java.util.stream.Collectors.toSet()));assertEquals(1,captured.requests().stream().map(RatisDatabaseBackupJournal.CohortRequest::processIncarnation).distinct().count());
            }
            Path archiveRoot=factoryRoot.resolve(definition.backupId().toString()).resolve("archive");DatabaseBackupManifest manifest;try(var copied=new RatisDatabaseBackupArchive(archiveRoot,definition,destination)){manifest=copied.reopenComplete();assertEquals(1,manifest.operators().size());assertTrue(manifest.chunks().stream().anyMatch(chunk->chunk.entries()>0));}
            for(var pool:List.of(fast,archive))assertEquals(0,node.nativePool(pool).snapshotAdmissionStats().pins());service.verifyCatalogue();return new Backup(definition,manifest,archiveRoot);
        }
    }
    void coldReopen(SchemaMerge merged,Backup backup)throws Exception {
        var originalBinding=plan.catalogue();var originalEpochs=new RatisSessionEpochs(catalogue,first.clusterId(),first.databaseId(),originalBinding.groupId(),originalBinding.epoch(),originalBinding.schemaDigest(),originalBinding.sessionsColumn());byte[] originalLedger=originalEpochs.read().encode();
        byte[] identity=Files.readAllBytes(env.nodeRoot.resolve("node.identity"));byte[] originalManifest=RatisNodeManifest.encode(manifest);UUID process=field(node,"snapshotProcessIncarnation",UUID.class);var directories=new HashMap<UUID,byte[]>();routes.forEach((domain,route)->directories.put(domain,route.directory().payloadDigest()));
        service.close();service=null;env.scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"two-pool-cold-workflow");env.timer=new RatisNodeRuntimeTest.ManualTimer();server=new RatisNodeRuntimeTest.FakeServer();routes.clear();routes.put(first.domainId(),routed(firstDirectory));routes.put(second.domainId(),routed(secondDirectory));
        var resources=new RatisNodeService.Resources(catalogue,routes,env.scheduler,env.timer);service=RatisNodeService.recover(manifest,env.nodeRoot,ConfigParser.parseDefault(),resources,server::create,this::admitOperator,this::homeClient);node=field(service,"node",RatisNodeRuntime.class);service.start();field(node,"poolPressure",RatisPoolPressureService.class).refresh().get(10,TimeUnit.SECONDS);service.verifyCatalogue();assertSharedNodeResources();
        assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));assertArrayEquals(originalManifest,RatisNodeManifest.encode(manifest));assertNotEquals(process,field(node,"snapshotProcessIncarnation",UUID.class));for(var entry:directories.entrySet())assertArrayEquals(entry.getValue(),routes.get(entry.getKey()).directory().payloadDigest());
        var route=routes.get(second.domainId());var binding=plan.catalogue();var epochs=new RatisSessionEpochs(catalogue,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());
        assertArrayEquals(originalLedger,epochs.read().encode());
        try(var session=RatisBackendSessions.window(root.resolve("merge-window"),route,epochs,60000,new RatisSessionClient.Limits(4,1L<<20,1L<<20),List.of(new RatisBackendSessions.Target(route,merged.last().groupId(),merged.directory())),RatisJournalAccess.standalone())){assertEquals(merged.grant(),session.grant());assertEquals(1,session.pending().size());var retained=session.pending().getFirst();assertArrayEquals(DistributedCommandCodec.encode(merged.last()),DistributedCommandCodec.encode(retained.command()));assertEquals(CommandResult.Status.SUCCESS,session.execute(retained).status());
            var retry=route.prepare(merged.last().operationId(),merged.last().mutations(),merged.last().predicates(),merged.last().acknowledgedThrough());assertArrayEquals(DistributedCommandCodec.encode(merged.last()),DistributedCommandCodec.encode(retry.command()));assertEquals(CommandResult.Status.SUCCESS,route.execute(retry).status());assertArrayEquals(new byte[]{11,33},route.read(merged.column(),new byte[]{9}));session.acknowledge(retained);}
        assertArrayEquals(originalLedger,epochs.read().encode());assertArrayEquals(new byte[]{44},route.read(second.columnIds().iterator().next(),new byte[]{7}));
        try(var copied=new RatisDatabaseBackupArchive(backup.archive(),backup.definition(),backup.manifest().destination())){
            var complete=copied.reopenComplete();assertArrayEquals(backup.manifest().encode(),complete.encode());
            var chunk=complete.chunks().stream().filter(candidate->candidate.columnId().equals(merged.column()) && candidate.entries()>0).findFirst().orElseThrow();assertEquals(1,chunk.entries());
            try(var options=new org.rocksdb.Options();var reader=new org.rocksdb.SstFileReader(options);var read=new org.rocksdb.ReadOptions().setFillCache(false)){
                reader.openBounded(copied.objectPath(chunk.objectId(),false).toString(),NativeRangeSstImport.MAX_TABLE_METADATA_BYTES,NativeRangeSstImport.MAX_TABLE_PROPERTY_ENTRIES,NativeRangeSstImport.MAX_TABLE_PROPERTY_STRING_BYTES);reader.verifyChecksum();
                try(var iterator=reader.newIterator(read)){iterator.seekToFirst();assertTrue(iterator.isValid());var sizing=ByteBuffer.allocate(0);assertEquals(1,iterator.key(sizing));assertEquals(2,iterator.value(sizing));assertArrayEquals(new byte[]{9},iterator.key());assertArrayEquals(new byte[]{11,33},iterator.value());iterator.next();assertFalse(iterator.isValid());iterator.status();}
            }
        }
    }
    private static void advance(RatisSchemaClient client,RatisSchemaClient.Handle handle)throws Exception {for(int visits=0;visits<256 && handle.phase()!=RatisSchemaClient.Phase.DONE;visits++){var progress=client.advance(handle,1);assertNotEquals(RatisSchemaClient.Phase.CONFLICT,progress.phase());assertNotEquals(RatisSchemaClient.Phase.ABORTED,progress.phase());}assertEquals(RatisSchemaClient.Phase.DONE,handle.phase());}
    private RatisTopologyCatalogue.QualifiedOwner topologyOwner(TopologyChange.Definition definition,TopologyChange.Proof expected)throws IOException {
        var target=new RatisTopologyOwnerRpc.Target(definition,expected.stage(),plan.catalogue().groupId(),expected.groupId(),expected.owner().replicaId());
        try{var reply=server.machines.get(RaftGroupId.valueOf(expected.groupId())).queryStale(Message.valueOf(ByteString.copyFrom(RatisTopologyOwnerRpc.request(target))),0).get(10,TimeUnit.SECONDS);var evidence=RatisTopologyOwnerRpc.decodeReply(reply.getContent().toByteArray(),target);return new RatisTopologyCatalogue.QualifiedOwner(evidence.proof(),evidence.completedCut());}catch(Exception failure){throw new IOException(failure);}
    }
    private Message applyCatalogue(DistributedCommand command)throws Exception {
        var transaction=catalogueMachine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("two-pool-catalogue")).setGroupId(RaftGroupId.valueOf(plan.catalogue().groupId())).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build());
        assertNull(transaction.getException());catalogueMachine.preAppendTransaction(transaction);transaction.initLogEntry(1,env.catalogue.next.getAndIncrement());return committedApply(catalogueMachine,transaction);
    }
    private byte[] committedValue(byte[] key){try{return env.catalogue.store.readCurrentPoint(plan.catalogue().directoryColumn(),key);}catch(Exception failure){throw new IllegalArgumentException(failure);}}
    @Override public RatisReferencedFile transfer(RangeImportDefinition definition,RatisReferencedFile.Request request,Path source)throws IOException {
        try{long index=store(request.groupId()).applied().index()+1;var files=(RatisReferencedFiles)field(node,"importFiles",Map.class).get(request.groupId());if(Files.size(source)>1L<<20)throw new IOException("Controlled import reference exceeds bound");var entry=RatisReferencedFilesTest.entry(request.groupId(),request.clientId(),request.callId(),index,Files.readAllBytes(source),true);files.write(entry).get(10,TimeUnit.SECONDS);files.flush(index).get(10,TimeUnit.SECONDS);var machine=server.machines.get(RaftGroupId.valueOf(request.groupId()));var transaction=TransactionContext.newBuilder().setStateMachine(machine).setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).setLogEntry(entry).build();committedApply(machine,transaction);return files.reference(1,index);}catch(Exception failure){throw new IOException(failure);}
    }
    @Override public NativeSstImportManifest.Receipt install(RangeImportDefinition definition,RatisRangeImportRecord.Install record)throws IOException {
        try{var machine=server.machines.get(RaftGroupId.valueOf(record.groupId()));machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisRangeImportRpc.prepare(record)))).get(10,TimeUnit.SECONDS);return NativeSstImportManifest.Receipt.decode(apply(record.groupId(),record.encode()).getContent().toByteArray());}catch(Exception failure){throw new IOException(failure);}
    }
    @Override public NativeRangeSstImport.Activation activate(RangeImportDefinition definition,RatisRangeImportRecord.Activate record)throws IOException {
        try{var group=record.receipt().groupId();server.machines.get(RaftGroupId.valueOf(group)).queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisRangeImportRpc.prepare(record)))).get(10,TimeUnit.SECONDS);return NativeRangeSstImport.Activation.decode(apply(group,record.encode()).getContent().toByteArray());}catch(Exception failure){throw new IOException(failure);}
    }
    private RatisRangeImportApply.Status status(RangeImportDefinition definition,RangeDescriptor range,ReplicaDescriptor owner)throws IOException {
        var target=RatisRangeImportStatusRpc.Target.of(definition,range.groupId(),owner);try{return RatisRangeImportStatusRpc.decodeReply(server.machines.get(RaftGroupId.valueOf(range.groupId())).queryStale(Message.valueOf(ByteString.copyFrom(RatisRangeImportStatusRpc.request(target))),0).get(10,TimeUnit.SECONDS).getContent().toByteArray(),target);}catch(Exception failure){throw new IOException(failure);}
    }
    @Override public RangeImportAdmission.OwnerReceipt receipt(RangeImportDefinition definition,RangeDescriptor range,ReplicaDescriptor owner)throws IOException {var status=status(definition,range,owner);return new RangeImportAdmission.OwnerReceipt(range.groupId(),range.rangeId(),range.configurationVersion(),owner,status.receipt(),status.applied());}
    @Override public RangeImportActivationAdmission.OwnerActivation activation(RangeImportDefinition definition,RatisRangeImportCatalogue.Head head,RangeDescriptor range,ReplicaDescriptor owner)throws IOException {var status=status(definition,range,owner);return new RangeImportActivationAdmission.OwnerActivation(range.groupId(),range.rangeId(),range.configurationVersion(),owner,RangeImportActivationAdmission.Activation.decode(status.activation().encode()),status.applied());}
    @Override public RatisRangeImportApply.PublicationView publication(RangeImportDefinition definition)throws IOException {return RatisRangeImportAuthority.readNative(env.catalogue.store,plan.catalogue().directoryColumn(),first.clusterId(),first.databaseId(),RatisRangeImportAuthority.Request.of(definition));}
    void assertSharedNodeResources()throws Exception {
        var graph=field(node,"resources",RatisNodeResourceBudget.class);var process=field(node,"snapshotProcessIncarnation",UUID.class);var pools=field(node,"pools",Map.class);assertEquals(2,pools.size());
        for(var id:List.of(fast,archive)){var cohorts=field(pools.get(id),"cohorts",RatisSnapshotCohortService.class);var host=field(cohorts,"host",RatisSnapshotCohortService.Host.class);assertSame(graph,field(cohorts,"budget",RatisNodeResourceBudget.class));assertEquals(plan.nodeId(),host.nodeId());assertEquals(plan.nodeIncarnation(),host.nodeIncarnation());assertEquals(process,host.processIncarnation());assertEquals(id,host.poolId());assertSame(node.nativePool(id),host.pool());}
    }
    private static final class Environment implements AutoCloseable {
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest();
        final RatisSessionEpochsTest.Fixture catalogue;
        it.cavallium.rockserver.core.impl.RWScheduler scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"two-pool-workflow");
        RatisNodeRuntimeTest.ManualTimer timer=new RatisNodeRuntimeTest.ManualTimer();
        final Path nodeRoot;boolean resourcesTransferred;
        Environment(Path root)throws Exception{catalogue=outer.new Fixture(root.resolve("catalogue"));nodeRoot=Files.createDirectory(root.resolve("node"));}
        RatisNodeRuntime.Settings settings(){return new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4<<20,16<<20,4),RatisRangeStateMachine.RetentionLimits.defaults(),java.time.Duration.ofSeconds(1),1,8,java.time.Duration.ofSeconds(5),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());}
        public void close()throws Exception{if(resourcesTransferred)catalogue.pool.close();else{scheduler.disposeGracefully().block(java.time.Duration.ofSeconds(5));catalogue.close();}}
    }
    @Override public void close()throws Exception {try{calls.shutdown();assertTrue(calls.awaitTermination(10,TimeUnit.SECONDS));if(service!=null)service.close();for(var home:remoteHomes.values()){home.close();home.awaitNativeReaders(java.time.Duration.ofSeconds(10));}for(var storage:homeStorage)storage.close();if(catalogueProofQueries!=null)catalogueProofQueries.stop().get(10,TimeUnit.SECONDS);if(authorityQueries!=null)authorityQueries.stop().get(10,TimeUnit.SECONDS);if(importQueries!=null)importQueries.stop().get(10,TimeUnit.SECONDS);if(catalogueMachine!=null){catalogueMachine.close();catalogueMachine.awaitNativeReaders(java.time.Duration.ofSeconds(10));}if(catalogueDispatcher!=null){catalogueDispatcher.awaitDrained(java.time.Duration.ofSeconds(10));catalogueDispatcher.close();}if(catalogueStorage!=null)catalogueStorage.close();schemaReplicas.close();}finally{updater.shutdown();assertTrue(updater.awaitTermination(10,TimeUnit.SECONDS));env.close();}}
}
