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
    RatisProofQueryService importQueries,authorityQueries;
    RatisCommittedApplyDispatcher catalogueDispatcher;

    RatisTwoPoolWorkflowFixture(Path root,boolean pendingImport)throws Exception {
        this.root=root;importSource=root.resolve("original-import.sst");Files.createDirectories(root);first=new RatisNodeServiceTest().schema();env=new Environment(root);
        try {
        local=new ReplicaDescriptor(new UUID(9,1),new UUID(9,2),new UUID(9,3),"node9","127.0.0.1:4512",ReplicaDescriptor.Role.VOTER);
        second=new DistributedSchema(first.clusterId(),first.databaseId(),new UUID(960,3),1,0,List.of(new DistributedSchema.Column(72,new UUID(960,4),"archive",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
        firstDirectory=directory(first,new UUID(961,1),local,RangeDescriptor.TopologyState.ACTIVE);secondDirectory=directory(second,new UUID(961,2),new ReplicaDescriptor(local.nodeId(),local.nodeIncarnation(),new UUID(960,5),local.peerId(),local.address(),ReplicaDescriptor.Role.VOTER),pendingImport?RangeDescriptor.TopologyState.FENCED:RangeDescriptor.TopologyState.ACTIVE);
        imported=pendingImport?importDefinition():null;
        var domains=List.of(new RatisNodePlan.Domain(firstDirectory,Map.of(first.columnIds().iterator().next(),"data"),Set.of()),new RatisNodePlan.Domain(secondDirectory,Map.of(second.columnIds().iterator().next(),"archive"),Set.of()));
        var remote=new ReplicaDescriptor(new UUID(8,1),new UUID(8,2),new UUID(8,8),"node8","127.0.0.1:4511",ReplicaDescriptor.Role.VOTER);var binding=new RatisNodePlan.Catalogue(env.outer.catalogue,1,new byte[32],env.outer.directoryColumn,env.outer.sessions,List.of(remote));
        plan=new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),domains,binding,List.of(),imported==null?List.of():List.of(imported),Map.of(),8,List.of(RatisPoolFixtures.spec(fast,false),RatisPoolFixtures.spec(archive,false)),Map.of(first.domainId(),fast,second.domainId(),archive),Map.of(binding.groupId(),fast));
        var original=env.settings();var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4<<20,16<<20,4),original.retention(),original.clockPeriod(),original.propagationVisits(),8,java.time.Duration.ofSeconds(5),original.exports(),original.resources());
        manifest=new RatisNodeManifest(List.of(first,second),plan,settings,3,3);
        routes.put(first.domainId(),routed(firstDirectory));routes.put(second.domainId(),routed(secondDirectory));catalogue=catalogue();startCatalogueMachine();
        var epochs=new RatisSessionEpochs(catalogue,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(env.outer.operation(0),null,SessionEpochLedger.initial(first.clusterId(),first.databaseId(),10))).status());
        server=new RatisNodeRuntimeTest.FakeServer();
        try(var bootstrap=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,routes,env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server::create,List.of(first,second))){
            bootstrap.start();for(var schema:List.of(first,second)){if(pendingImport && schema==second)continue;var range=plan.directory(schema.domainId()).ranges().getFirst();var store=store(range.groupId());var put=new DistributedCommand(range.groupId(),range.rangeEpoch(),schema.digest(),new OperationId(new UUID(962,1),1,schema==first?0:1),List.of(new Mutation(Mutation.Kind.PUT,schema.columnIds().iterator().next(),new byte[]{7},new byte[]{(byte)(schema==first?11:22)})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),put)).status());}
        }
        publish();server=new RatisNodeRuntimeTest.FakeServer();
        var resources=new RatisNodeService.Resources(catalogue,routes,env.scheduler,env.timer);env.resourcesTransferred=true;
        service=RatisNodeService.recover(manifest,env.nodeRoot,ConfigParser.parseDefault(),resources,server::create);node=field(service,"node",RatisNodeRuntime.class);service.start();field(node,"poolPressure",RatisPoolPressureService.class).refresh().get(10,TimeUnit.SECONDS);if(!pendingImport)service.verifyCatalogue();
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
    });}
    private RaftClient catalogue(){
        var borrowed=env.catalogue.client;var binding=plan.catalogue();var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            if(method.getName().equals("sendReadOnly")){byte[] bytes=((Message)args[0]).getContent().toByteArray();var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);if(ScopedTransactionRpc.importPublication(packet.kind()) || RatisTopologyExecutionRpc.kind(packet.kind()) || packet.kind()==RatisTopologyAuthority.KIND || packet.kind()==RatisRangeImportAuthority.KIND){assertNotNull(catalogueMachine);return reply(catalogueMachine.queryLinearizable((Message)args[0]).get(10,TimeUnit.SECONDS).getContent().toByteArray());}}
            if(method.getName().equals("send")){var command=DistributedCommandCodec.decode(((Message)args[0]).getContent().toByteArray());return reply(applyCatalogue(command).getContent().toByteArray());}
            return invoke(method,borrowed.io(),args);
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "close"->null;default->invoke(method,borrowed,args);});
    }
    private static Object invoke(java.lang.reflect.Method method,Object target,Object[] arguments)throws Throwable {try{return method.invoke(target,arguments);}catch(java.lang.reflect.InvocationTargetException failure){throw failure.getCause();}}
    private RaftClientReply reply(byte[] bytes){return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("catalogue")).setGroupId(RaftGroupId.valueOf(plan.catalogue().groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build();}
    ReplicatedRangeStore store(UUID group)throws Exception{return field(server.machines.get(RaftGroupId.valueOf(group)),"store",ReplicatedRangeStore.class);}
    Message apply(UUID group,byte[] bytes)throws Exception {
        var machine=server.machines.get(RaftGroupId.valueOf(group));var transaction=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(group)).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build());assertNull(transaction.getException());machine.preAppendTransaction(transaction);transaction.initLogEntry(1,store(group).applied().index()+1);
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
        var validator=RatisCatalogue.validator(first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),binding.sessionsColumn(),command->{throw new IllegalArgumentException("No operator upload is configured in this workflow phase");},this::committedValue);
        var peer=RaftPeer.newBuilder().setId("two-pool-catalogue").build();var group=RaftGroup.valueOf(RaftGroupId.valueOf(binding.groupId()),peer);
        catalogueStorage=org.apache.ratis.server.storage.RaftStorage.newBuilder().setDirectory(root.resolve("remote-catalogue-raft").toFile()).setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();catalogueStorage.initialize();
        catalogueMachine=new RatisRangeStateMachine(env.catalogue.store,group,manifest.settings().images(),validator,command->{},()->new RatisRangeStateMachine.Leadership(1,true));
        var resourcePolicy=manifest.settings().resources();var remotePool=new UUID(965,1);
        var remoteResources=new RatisNodeResourceBudget(resourcePolicy.node(),Map.of(remotePool,resourcePolicy.pool()));
        var dispatchPolicy=resourcePolicy.committedApply();catalogueDispatcher=new RatisCommittedApplyDispatcher(dispatchPolicy.node(),Map.of(remotePool,dispatchPolicy.pool()));
        long maximumCommittedEntry=NativeSstImportManifest.MAX_FILE_BYTES+4096;
        for(var bound:List.of(dispatchPolicy.node(),dispatchPolicy.pool())){assertTrue(bound.read().bytes()>=RatisRangeStateMachine.committedReadWeight(maximumCommittedEntry));assertTrue(Math.min(bound.ordinary().bytes(),bound.protectedLane().bytes())>=RatisRangeStateMachine.committedExecutionWeight(maximumCommittedEntry));}
        catalogueMachine.nodeResources(remoteResources,remotePool);catalogueMachine.committedApply(catalogueDispatcher,remotePool,maximumCommittedEntry);
        catalogueMachine.role(RatisNodePlan.Role.CATALOGUE);catalogueMachine.schemaCatalogue(first.clusterId(),first.databaseId(),binding.directoryColumn());
        importQueries=RatisProofQueryService.importPublicationLane(RatisProofFixtures.admission());authorityQueries=RatisProofQueryService.authorityLane(RatisProofFixtures.admission());catalogueMachine.importPublicationQueries(importQueries);catalogueMachine.schemaAuthorityQueries(authorityQueries);
        catalogueMachine.topologyExecutionAdmission(command->RatisTopologyCatalogue.admitExecution(command,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),this::committedValue,this::topologyOwner));
        catalogueMachine.importPublicationAdmission(command->{if(RatisRangeImportCatalogue.isPublication(command))RatisRangeImportCatalogue.admitPublication(command,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),this::committedValue,this::receipt);else RatisRangeImportCatalogue.admitCompletion(command,first.clusterId(),first.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),this::committedValue,this::activation);});
        var identity=(org.apache.ratis.server.RaftServer)Proxy.newProxyInstance(org.apache.ratis.server.RaftServer.class.getClassLoader(),new Class<?>[]{org.apache.ratis.server.RaftServer.class},(proxy,method,args)->{if(method.getName().equals("getId"))return peer.getId();if(method.getName().equals("toString"))return "two-pool-catalogue";throw new AssertionError(method);});
        catalogueMachine.initialize(identity,group.getGroupId(),catalogueStorage);catalogueMachine.notifyTermIndexUpdated(1,0);env.catalogue.next.set(1);
    }
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
        final it.cavallium.rockserver.core.impl.RWScheduler scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"two-pool-workflow");
        final RatisNodeRuntimeTest.ManualTimer timer=new RatisNodeRuntimeTest.ManualTimer();
        final Path nodeRoot;boolean resourcesTransferred;
        Environment(Path root)throws Exception{catalogue=outer.new Fixture(root.resolve("catalogue"));nodeRoot=Files.createDirectory(root.resolve("node"));}
        RatisNodeRuntime.Settings settings(){return new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4<<20,16<<20,4),RatisRangeStateMachine.RetentionLimits.defaults(),java.time.Duration.ofSeconds(1),1,8,java.time.Duration.ofSeconds(5),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());}
        public void close()throws Exception{if(resourcesTransferred)catalogue.pool.close();else{scheduler.disposeGracefully().block(java.time.Duration.ofSeconds(5));catalogue.close();}}
    }
    @Override public void close()throws Exception {try{if(service!=null)service.close();if(authorityQueries!=null)authorityQueries.stop().get(10,TimeUnit.SECONDS);if(importQueries!=null)importQueries.stop().get(10,TimeUnit.SECONDS);if(catalogueMachine!=null){catalogueMachine.close();catalogueMachine.awaitNativeReaders(java.time.Duration.ofSeconds(10));}if(catalogueDispatcher!=null){catalogueDispatcher.awaitDrained(java.time.Duration.ofSeconds(10));catalogueDispatcher.close();}if(catalogueStorage!=null)catalogueStorage.close();}finally{updater.shutdown();assertTrue(updater.awaitTermination(10,TimeUnit.SECONDS));env.close();}}
}
