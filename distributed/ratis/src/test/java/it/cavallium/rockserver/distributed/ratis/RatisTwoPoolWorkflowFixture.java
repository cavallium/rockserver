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
final class RatisTwoPoolWorkflowFixture implements AutoCloseable {
    final RatisNodeRuntimeTest.Env env;
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

    RatisTwoPoolWorkflowFixture(Path root)throws Exception {
        Files.createDirectories(root);first=new RatisNodeServiceTest().schema();env=new RatisNodeRuntimeTest.Env(root,false,first.digest());
        try {
        local=env.routed.directory().ranges().getFirst().replicas().stream().filter(r->r.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();
        second=new DistributedSchema(first.clusterId(),first.databaseId(),new UUID(960,3),1,0,List.of(new DistributedSchema.Column(72,new UUID(960,4),"archive",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
        firstDirectory=directory(first,new UUID(961,1),local);secondDirectory=directory(second,new UUID(961,2),new ReplicaDescriptor(local.nodeId(),local.nodeIncarnation(),new UUID(960,5),local.peerId(),local.address(),ReplicaDescriptor.Role.VOTER));
        var domains=List.of(new RatisNodePlan.Domain(firstDirectory,Map.of(first.columnIds().iterator().next(),"data"),Set.of()),new RatisNodePlan.Domain(secondDirectory,Map.of(second.columnIds().iterator().next(),"archive"),Set.of()));
        plan=new RatisNodePlan(env.plan.nodeId(),env.plan.nodeIncarnation(),domains,env.plan.catalogue(),List.of(),List.of(),Map.of(),8,List.of(RatisPoolFixtures.spec(fast,false),RatisPoolFixtures.spec(archive,false)),Map.of(first.domainId(),fast,second.domainId(),archive),Map.of(env.plan.catalogue().groupId(),fast));
        var original=env.settings();var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4<<20,16<<20,4),original.retention(),original.clockPeriod(),original.propagationVisits(),8,java.time.Duration.ofSeconds(5),original.exports(),original.resources());
        manifest=new RatisNodeManifest(List.of(first,second),plan,settings,3,3);
        routes.put(first.domainId(),routed(firstDirectory));routes.put(second.domainId(),routed(secondDirectory));catalogue=catalogue();
        server=new RatisNodeRuntimeTest.FakeServer();
        try(var bootstrap=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,routes,env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server::create,List.of(first,second))){
            bootstrap.start();for(var schema:List.of(first,second)){var range=plan.directory(schema.domainId()).ranges().getFirst();var store=store(range.groupId());var put=new DistributedCommand(range.groupId(),range.rangeEpoch(),schema.digest(),new OperationId(new UUID(962,1),1,schema==first?0:1),List.of(new Mutation(Mutation.Kind.PUT,schema.columnIds().iterator().next(),new byte[]{7},new byte[]{(byte)(schema==first?11:22)})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),put)).status());}
        }
        publish();server=new RatisNodeRuntimeTest.FakeServer();
        var resources=new RatisNodeService.Resources(catalogue,routes,env.scheduler,env.timer);env.resourcesTransferred=true;
        service=RatisNodeService.recover(manifest,env.nodeRoot,ConfigParser.parseDefault(),resources,server::create);node=field(service,"node",RatisNodeRuntime.class);service.start();field(node,"poolPressure",RatisPoolPressureService.class).refresh().get(10,TimeUnit.SECONDS);service.verifyCatalogue();
        }catch(Exception|Error failure){try{close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}throw failure;}
    }
    private static RangeDirectory directory(DistributedSchema schema,UUID group,ReplicaDescriptor owner){return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),1,schema.columnIds(),schema.routingPrefixBytes(),List.of(new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner))));}
    private OperationId operation(){return new OperationId(new UUID(962,2),0,sequence++);}
    private void publish()throws Exception {
        var o=env.outer;var schemas=new RatisSchemaCatalogue(env.catalogue.client,o.cluster,o.database,o.catalogue,1,new byte[32],o.directoryColumn);
        assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(operation(),first)).status());
        assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(operation(),second,RatisColumnAllocation.counterValue(71),Map.of(),Map.of(),Map.of())).status());
        var directories=new RatisCatalogue(env.catalogue.client,o.cluster,o.database,o.catalogue,1,new byte[32],o.directoryColumn);
        var roster=new RatisDatabaseDomainRoster(env.catalogue.client,o.cluster,o.database,plan.catalogue());
        for(var directory:List.of(firstDirectory,secondDirectory)){var initial=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),0,directory.columns(),directory.routingPrefixBytes(),directory.ranges());assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(operation(),null,initial,roster.read())).status());assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(operation(),initial,directory)).status());}
    }
    private RatisRoutedClient routed(RangeDirectory directory){return new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
        public byte[] read(RangeDescriptor range,UUID column,byte[] key)throws IOException{try{return store(range.groupId()).readCurrentPoint(column,key);}catch(Exception failure){throw new IOException(failure);}}
        public CommandResult command(RangeDescriptor range,DistributedCommand command)throws IOException{try{return CommandResult.decode(apply(range.groupId(),DistributedCommandCodec.encode(command)).getContent().toByteArray());}catch(Exception failure){throw new IOException(failure);}}
    });}
    private RaftClient catalogue(){
        var borrowed=env.catalogue.client;var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            if(method.getName().equals("sendReadOnly")){byte[] bytes=((Message)args[0]).getContent().toByteArray();var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);if(packet.kind()==RatisTopologyAuthority.KIND){var request=RatisTopologyAuthority.decodeRequest(bytes);var view=RatisTopologyAuthority.readNative(env.catalogue.store,env.outer.directoryColumn,first.clusterId(),first.databaseId(),request);return reply(RatisTopologyAuthority.reply(view));}}
            return method.invoke(borrowed.io(),args);
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "close"->null;default->method.invoke(borrowed,args);});
    }
    private RaftClientReply reply(byte[] bytes){return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("catalogue")).setGroupId(RaftGroupId.valueOf(plan.catalogue().groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build();}
    ReplicatedRangeStore store(UUID group)throws Exception{return field(server.machines.get(RaftGroupId.valueOf(group)),"store",ReplicatedRangeStore.class);}
    Message apply(UUID group,byte[] bytes)throws Exception {
        var machine=server.machines.get(RaftGroupId.valueOf(group));var transaction=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(group)).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build());assertNull(transaction.getException());machine.preAppendTransaction(transaction);transaction.initLogEntry(1,store(group).applied().index()+1);
        return updater.submit(()->{
            var admission=machine.committedApplyAdmission();org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor descriptor;long index=transaction.getLogEntry().getIndex();try(var read=admission.awaitRead(index)){descriptor=admission.describe(transaction.getLogEntry());}var permit=admission.awaitDispatch(descriptor);admission.offer(descriptor,permit);CompletableFuture<Message> result;
            try{result=machine.applyTransaction(transaction);}finally{admission.withdraw(descriptor,permit);if(!permit.claimed())permit.close();}try(var read=admission.awaitRead(index+1)){}return result.get(5,TimeUnit.SECONDS);
        }).get(10,TimeUnit.SECONDS);
    }
    void committed(TopologyChange.Definition definition,TopologyChange.State state,RangeDirectory head)throws Exception {
        var o=env.outer;byte[] domain=ByteBuffer.allocate(16).putLong(head.domainId().getMostSignificantBits()).putLong(head.domainId().getLeastSignificantBits()).array();var command=new DistributedCommand(o.catalogue,1,new byte[32],operation(),List.of(new Mutation(Mutation.Kind.PUT,o.directoryColumn,RatisTopologyCatalogue.definitionKey(definition.digest()),definition.encode()),new Mutation(Mutation.Kind.PUT,o.directoryColumn,RatisTopologyCatalogue.headKey(head.domainId()),state.encode()),new Mutation(Mutation.Kind.PUT,o.directoryColumn,RatisTopologyCatalogue.stateKey(definition.intentId()),state.encode()),new Mutation(Mutation.Kind.PUT,o.directoryColumn,domain,RangeDirectoryCodec.encode(head))));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(env.catalogue.store.apply(new ApplyPosition(1,env.catalogue.next.getAndIncrement()),command)).status());
    }
    TopologyChange.Definition split()throws Exception {
        var parent=firstDirectory.ranges().getFirst();var left=new RangeDescriptor(new UUID(963,1),new UUID(963,2),1,0,null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,List.of(local));var right=new RangeDescriptor(new UUID(963,3),new UUID(963,4),1,0,new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));var target=new RangeDirectory(first.clusterId(),first.databaseId(),first.domainId(),first.schemaEpoch(),first.digest(),2,first.columnIds(),0,List.of(left,right));var definition=new TopologyChange.Definition(new UUID(963,5),firstDirectory,target);
        var state=TopologyChange.State.begin(definition.header()).beginFencing();committed(definition,state,firstDirectory);var view=RatisTopologyAuthority.readNative(env.catalogue.store,env.outer.directoryColumn,first.clusterId(),first.databaseId(),RatisTopologyAuthority.Request.of(definition.header(),TopologyChange.Stage.FREEZE));var freeze=new RatisTopologyFreezeRecord(definition,parent.groupId(),view.admissionDigest());var machine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologyFreezeRpc.prepareRequest(freeze)))).get(5,TimeUnit.SECONDS);var frozen=NativeTopologyFreeze.decode(apply(parent.groupId(),freeze.encode()).getContent().toByteArray());state=state.frozen(frozen.proof(local.replicaId()));committed(definition,state,firstDirectory);
        var seed=node.seedTopology(new RatisTopologySeedRpc.Request(definition,plan.catalogue().groupId(),parent.groupId(),local.replicaId()));for(var child:definition.targets())state=state.seeded(seed.proof(child.groupId()));state=state.publish();committed(definition,state,target);
        var published=RatisTopologyAuthority.readNative(env.catalogue.store,env.outer.directoryColumn,first.clusterId(),first.databaseId(),RatisTopologyAuthority.Request.of(definition.header(),TopologyChange.Stage.ACTIVATE));
        for(var child:definition.targets()){var record=RatisTopologyActivationRecord.of(published,child.groupId());var targetMachine=server.machines.get(RaftGroupId.valueOf(child.groupId()));targetMachine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologyActivationRpc.prepareRequest(record)))).get(5,TimeUnit.SECONDS);var activation=NativeTopologyActivation.decode(apply(child.groupId(),record.encode()).getContent().toByteArray());state=state.activated(activation.proof());}
        committed(definition,state,target);return definition;
    }
    @Override public void close()throws Exception {try{if(service!=null)service.close();}finally{updater.shutdown();assertTrue(updater.awaitTermination(10,TimeUnit.SECONDS));env.close();}}
}
