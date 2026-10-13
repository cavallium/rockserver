package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Two independent native DATA domains sharing one native catalogue; controlled Raft dispatch, no sockets/quorum. */
class RatisMultiDomainBootstrapTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static Message message(byte[] bytes){return Message.valueOf(ByteString.copyFrom(bytes));}
    final class Fixture implements AutoCloseable {
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest();
        final RatisSessionEpochsTest.Fixture catalogue;
        final UUID node=new UUID(820,1),incarnation=new UUID(820,2);
        final List<Data> data=new ArrayList<>();final List<DistributedCommand> catalogueCommands=new ArrayList<>();
        final RaftClient client;boolean loseSecondSchema;int lostSecondSchema;
        Fixture()throws Exception {
            catalogue=outer.new Fixture(root.resolve("native"));var actual=catalogue.client;var actualIo=actual.io();
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                if(method.getName().equals("send")){
                    var command=DistributedCommandCodec.decode(((Message)args[0]).getContent().toByteArray());catalogueCommands.add(command);
                    Object reply=invoke(method,actualIo,args);
                    if(loseSecondSchema && command.mutations().stream().anyMatch(m->m.key().length==33 && m.key()[0]==0x53 && Arrays.equals(Arrays.copyOfRange(m.key(),1,33),data.get(1).schema.digest()))){loseSecondSchema=false;lostSecondSchema++;throw new IOException("Lost applied second-domain schema reply");}
                    return reply;
                }return invoke(method,actualIo,args);
            });
            client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->method.getName().equals("io")?io:invoke(method,actual,args));
            data.add(new Data(1));data.add(new Data(2));
        }
        Object invoke(java.lang.reflect.Method method,Object target,Object[] args)throws Throwable {try{return method.invoke(target,args);}catch(InvocationTargetException failure){throw failure.getCause();}}
        final class Data implements AutoCloseable {
            final UUID domain,column,group;final DistributedSchema schema;final RangeDirectory directory;final RangeDescriptor descriptor;
            final ReplicatedRangeStore store;final RatisRangeStateMachine machine;final RaftStorage storage;final RaftGroup raft;
            final RatisRoutedClient routed;final RatisNodePlan plan;boolean unavailable;int stateReads,commands;
            Data(int n)throws Exception {
                domain=new UUID(821,n);column=new UUID(822,n);group=new UUID(823,n);
                schema=new DistributedSchema(outer.cluster,outer.database,domain,1,0,List.of(new DistributedSchema.Column(70+n,column,"logical-"+n,ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
                var replica=new ReplicaDescriptor(node,incarnation,new UUID(824,n),"multi-domain","127.0.0.1:9900",ReplicaDescriptor.Role.VOTER);
                descriptor=new RangeDescriptor(new UUID(825,n),group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
                directory=new RangeDirectory(outer.cluster,outer.database,domain,1,schema.digest(),0,Set.of(column),0,List.of(descriptor));
                var catalogueReplica=new ReplicaDescriptor(node,incarnation,new UUID(824,0),replica.peerId(),replica.address(),ReplicaDescriptor.Role.VOTER);
                plan=new RatisNodePlan(node, incarnation, List.of(new RatisNodePlan.Domain(directory, Map.of(column,"physical-"+n), Set.of())), new RatisNodePlan.Catalogue(outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,List.of(catalogueReplica)), List.of(), List.of(), Map.of(), 2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory, Map.of(column,"physical-"+n), Set.of()))),RatisPoolFixtures.groups(new RatisNodePlan.Catalogue(outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,List.of(catalogueReplica)),List.of()));
                store=catalogue.pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,Set.of(column),null,null),schema.digest(),plan.columns(),Set.of());store.enableLogicalSchema(schema);
                var peer=RaftPeer.newBuilder().setId(replica.peerId()).build();raft=RaftGroup.valueOf(RaftGroupId.valueOf(group),peer);
                machine=RatisRangeStateMachine.withSessions(store,raft,RatisSessionBindingTest.Fixture.LIMITS,command->{},catalogue.epochs,directory,RatisRangeStateMachine.RetentionLimits.defaults(),()->new RatisRangeStateMachine.Leadership(1,true));
                storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft-"+n).toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build();storage.initialize();
                var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->switch(method.getName()){case "getId"->peer.getId();case "toString"->"multi-domain-native";default->throw new AssertionError(method);});machine.initialize(identity,raft.getGroupId(),storage);machine.notifyTermIndexUpdated(1,0);
                routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
                    public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{return append(command);}
                    public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return ScopedTransactionRpc.pointValue(query(ScopedTransactionRpc.pointRequest(column,key)),2<<20);}
                    public RangeStateView stateView(RangeDescriptor route)throws IOException{stateReads++;return RangeStateView.decode(ScopedTransactionRpc.parse(query(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0])),ScopedTransactionRpc.MAX_REPLY_BYTES).body());}
                    public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh)throws IOException{return query(request);}
                });
            }
            byte[] query(byte[] request)throws IOException {if(unavailable)throw new IOException("Domain unavailable");try{return machine.query(message(request)).get(5,TimeUnit.SECONDS).getContent().toByteArray();}catch(Exception failure){throw new IOException(failure);}}
            CommandResult append(DistributedCommand command)throws IOException {if(unavailable)throw new IOException("Domain unavailable");commands++;try{var tx=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(raft.getPeers().iterator().next().getId()).setGroupId(raft.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(DistributedCommandCodec.encode(command))).build());if(tx.getException()!=null)throw new IOException("Native admission rejected",tx.getException());machine.preAppendTransaction(tx);tx.initLogEntry(1,store.applied().index()+1);return CommandResult.decode(machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray());}catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException(failure);}}
            public void close()throws Exception{routed.close();machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();}
        }
        List<RatisDatabaseBootstrap.Domain> domains(){return List.of(new RatisDatabaseBootstrap.Domain(data.get(0).plan,data.get(0).routed,data.get(0).schema,null),new RatisDatabaseBootstrap.Domain(data.get(1).plan,data.get(1).routed,data.get(1).schema,RatisColumnAllocation.counterValue(71)));}
        RatisDatabaseBootstrap bootstrap(Path path,SessionEpochLedger existing,int maximum)throws IOException{return new RatisDatabaseBootstrap(path,domains(),client,existing,null,10,()->20L,maximum);}
        RatisCatalogue directories(){return catalogue.epochs.catalogue(outer.directoryColumn);}
        void ready(RatisDatabaseBootstrap bootstrap)throws IOException {for(int n=0;n<40;n++)if(bootstrap.step(1).phase()==RatisDatabaseBootstrap.Phase.READY)return;fail("Aggregate bootstrap did not reach READY");}
        void writesWithOneIssuedGrant()throws Exception {
            var ledger=catalogue.epochs.read();var request=catalogue.epochs.prepareGrant(outer.operation(100),ledger,new UUID(826,1),100);assertEquals(CommandResult.Status.SUCCESS,catalogue.epochs.execute(request.command()).status());var issued=catalogue.epochs.read();
            for(int n=0;n<data.size();n++){var d=data.get(n);var control=catalogue.epochs.prepareSessionBinding(d.directory,d.group,new OperationId(outer.maintenance,issued.minimumEpoch(),100+n),request.grant(),8,-1);assertEquals(CommandResult.Status.SUCCESS,d.routed.execute(d.routed.prepareSessionControl(control)).status());var command=new DistributedCommand(d.group,1,d.schema.digest(),new OperationId(request.grant().sessionId(),request.grant().epoch(),n),List.of(new Mutation(Mutation.Kind.PUT,d.column,new byte[]{7},new byte[]{(byte)(10+n)},Mutation.ReturnMode.CURRENT)));var result=d.routed.execute(d.routed.restoreSessionCommand(d.directory,command,d.descriptor));assertEquals(CommandResult.Status.SUCCESS,result.status());assertArrayEquals(new byte[]{(byte)(10+n)},result.value(0));assertArrayEquals(new byte[]{(byte)(10+n)},d.routed.read(d.column,new byte[]{7}));}
        }
        public void close()throws Exception{for(var d:data)d.close();catalogue.close();}
    }
    @Test void twoNativeDomainsShareOneGenesisAndClockThenSameGrantWritesBoth()throws Exception {
        try(var f=new Fixture();var boot=f.bootstrap(Files.createDirectory(root.resolve("bootstrap")),null,2)) {
            assertEquals(2,boot.progress().total());f.ready(boot);var ledger=f.catalogue.epochs.read();assertEquals(20,ledger.logicalTimeMillis());assertEquals(1,ledger.nextEpoch());assertTrue(ledger.grants().isEmpty());
            for(var d:f.data){assertArrayEquals(d.directory.payloadDigest(),f.directories().read(d.domain).payloadDigest());assertEquals(20,d.routed.stateView(d.group).sessionAuthority().logicalTimeMillis());assertEquals(0,d.store.activeSnapshotPins());}
            var creations=f.catalogueCommands.stream().filter(c->c.mutations().stream().anyMatch(m->m.columnId().equals(f.outer.sessions))).filter(c->c.predicates().stream().anyMatch(p->p.columnId().equals(f.outer.sessions) && p.expectedValue()==null)).count();assertEquals(1,creations);assertEquals(72,RatisColumnAllocation.counter(new RatisColumnAllocation(f.client,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn).readCounter()));f.writesWithOneIssuedGrant();
        }
    }
    @Test void interruptedSecondSchemaPublicationColdReopenKeepsExactCommandAndIdentity()throws Exception {
        try(var f=new Fixture()) {
            Path path=Files.createDirectory(root.resolve("restart"));f.loseSecondSchema=true;boolean interrupted=false;
            try(var boot=f.bootstrap(path,null,2)){for(int n=0;n<20;n++){try{boot.step(1);}catch(IOException failure){interrupted=true;break;}}assertTrue(interrupted);assertEquals(1,f.lostSecondSchema);}
            var first=f.catalogueCommands.stream().filter(c->c.mutations().stream().anyMatch(m->m.key().length==33 && m.key()[0]==0x53 && Arrays.equals(Arrays.copyOfRange(m.key(),1,33),f.data.get(1).schema.digest()))).findFirst().orElseThrow();byte[] exact=DistributedCommandCodec.encode(first);var operation=first.operationId();
            try(var boot=f.bootstrap(path,null,2)){assertEquals(RatisDatabaseBootstrap.Phase.PREFLIGHT,boot.progress().phase());f.ready(boot);}
            var retries=f.catalogueCommands.stream().filter(c->c.operationId().equals(operation)).toList();assertTrue(retries.size()>=2);for(var retry:retries)assertArrayEquals(exact,DistributedCommandCodec.encode(retry));assertEquals(20,f.catalogue.epochs.read().logicalTimeMillis());for(var d:f.data)assertArrayEquals(d.directory.payloadDigest(),f.directories().read(d.domain).payloadDigest());
        }
    }
    @Test void unavailableSecondDomainCannotBecomeReadyWhileFirstDomainAdvances()throws Exception {availability(1);}
    @Test void unavailableFirstDomainCannotBlockHealthyLaterDomainAuthority()throws Exception {availability(0);}
    private void availability(int blockedIndex)throws Exception {
        try(var f=new Fixture();var boot=f.bootstrap(Files.createDirectory(root.resolve("availability")),null,2)) {
            var blocked=f.data.get(blockedIndex);var healthy=f.data.get(1-blockedIndex);blocked.unavailable=true;assertNotEquals(RatisDatabaseBootstrap.Phase.READY,boot.step(1).phase());boot.step(1);assertTrue(healthy.stateReads>0);assertEquals(0,f.catalogue.next.get());blocked.unavailable=false;
            for(int n=0;n<20 && boot.progress().phase()!=RatisDatabaseBootstrap.Phase.PROPAGATE;n++)boot.step(1);assertEquals(RatisDatabaseBootstrap.Phase.PROPAGATE,boot.progress().phase());blocked.unavailable=true;for(int n=0;n<4;n++)assertNotEquals(RatisDatabaseBootstrap.Phase.READY,boot.step(1).phase());assertTrue(healthy.commands>0);assertEquals(0,blocked.commands);assertEquals(boot.progress().target(),healthy.routed.stateView(healthy.group).sessionAuthority());blocked.unavailable=false;f.ready(boot);blocked.unavailable=true;assertNotEquals(RatisDatabaseBootstrap.Phase.READY,boot.step(2).phase());assertNotEquals(RatisDatabaseBootstrap.Phase.READY,boot.progress().phase());
        }
    }
    @Test void existingLedgerIsGuardedAndRetainsItsGrantAcrossBothDomainPublications()throws Exception {
        try(var f=new Fixture()) {
            var initial=SessionEpochLedger.initial(f.outer.cluster,f.outer.database,10);assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(0),null,initial)).status());var existing=initial.issue(new UUID(826,2),100);assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(1),initial,existing)).status());
            try(var boot=f.bootstrap(Files.createDirectory(root.resolve("existing")),existing,2)){f.ready(boot);assertEquals(existing.advanceTime(20),f.catalogue.epochs.read());assertEquals(existing.grants(),f.catalogue.epochs.read().grants());for(var d:f.data)assertArrayEquals(d.directory.payloadDigest(),f.directories().read(d.domain).payloadDigest());}
        }
    }
    @Test void rootBindingRejectsChangedLaterConfigurationBeforeMissingJournalsCanBeRecreated()throws Exception {
        try(var f=new Fixture()) {
            Path path=Files.createDirectory(root.resolve("frozen-scope"));try(var boot=f.bootstrap(path,null,2)){assertEquals(RatisDatabaseBootstrap.Phase.PREFLIGHT,boot.progress().phase());}
            byte[] identity=Files.readAllBytes(path.resolve("bootstrap.identity"));var later=f.data.get(1);var journals=path.resolve("domains").resolve(later.domain.toString());assertTrue(Files.isDirectory(journals));try(var files=Files.walk(journals)){for(var file:files.sorted(Comparator.reverseOrder()).toList())Files.delete(file);}assertFalse(Files.exists(journals));
            var descriptor=new RangeDescriptor(later.descriptor.rangeId(),later.group,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,later.descriptor.replicas());var directory=new RangeDirectory(f.outer.cluster,f.outer.database,later.domain,1,later.schema.digest(),0,Set.of(later.column),0,List.of(descriptor));var plan=new RatisNodePlan(f.node, f.incarnation, List.of(new RatisNodePlan.Domain(directory, later.plan.columns(), Set.of())), later.plan.catalogue(), List.of(), List.of(), Map.of(), 2,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(directory, later.plan.columns(), Set.of()))),RatisPoolFixtures.groups(later.plan.catalogue(),List.of()));assertFalse(Arrays.equals(later.directory.payloadDigest(),directory.payloadDigest()));
            try(var routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return later.routed.read(column,key);}public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{return later.append(command);}public RangeStateView stateView(RangeDescriptor route)throws IOException{return later.routed.stateView(later.group);}})) {
                var changed=List.of(f.domains().getFirst(),new RatisDatabaseBootstrap.Domain(plan,routed,later.schema,RatisColumnAllocation.counterValue(71)));assertThrows(IOException.class,()->new RatisDatabaseBootstrap(path,changed,f.client,null,null,10,()->20L,2));assertFalse(Files.exists(journals));assertArrayEquals(identity,Files.readAllBytes(path.resolve("bootstrap.identity")));assertEquals(0,f.catalogue.next.get());
            }
        }
    }
    @Test void duplicateDomainsAndAggregateRangeBudgetFailBeforeWritingAnyIntent()throws Exception {
        try(var f=new Fixture()) {
            Path duplicates=Files.createDirectory(root.resolve("duplicate"));var first=f.domains().getFirst();assertThrows(IllegalArgumentException.class,()->new RatisDatabaseBootstrap(duplicates,List.of(first,first),f.client,null,null,10,()->20L,2));try(var files=Files.list(duplicates)){assertEquals(0,files.count());}
            Path limited=Files.createDirectory(root.resolve("limited"));assertThrows(IllegalArgumentException.class,()->f.bootstrap(limited,null,1));try(var files=Files.list(limited)){assertEquals(0,files.count());}assertEquals(0,f.catalogue.next.get());for(var d:f.data)assertEquals(0,d.commands);
        }
    }
}
