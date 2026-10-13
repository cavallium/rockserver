package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

/** Real native catalogue/DATA/HOME authority, with controlled Raft dispatch and no sockets/quorum. */
class RatisDatabaseBackendFactoryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T>T await(CompletableFuture<T> value)throws Exception{return value.get(20,TimeUnit.SECONDS);}
    final class Fixture implements AutoCloseable {
        final RatisMultiDomainBootstrapTest helper=new RatisMultiDomainBootstrapTest();
        final RatisMultiDomainBootstrapTest.Fixture nativeDomains;
        final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,4,1,"backend-factory");
        final List<Home> homes=new ArrayList<>();final AtomicInteger transportCloses=new AtomicInteger();
        final Map<UUID,RatisRoutedClient> routes=new LinkedHashMap<>();final RatisNodePlan plan;
        Fixture()throws Exception {
            helper.root=root;nativeDomains=helper.new Fixture();
            try(var bootstrap=nativeDomains.bootstrap(Files.createDirectory(root.resolve("bootstrap")),null,2)){nativeDomains.ready(bootstrap);}
            homes.add(new Home(RatisNodePlan.Role.SNAPSHOT_HOME,1));homes.add(new Home(RatisNodePlan.Role.TRANSACTION_HOME,2));
            for(var data:nativeDomains.data)routes.put(data.domain,data.routed);
            plan=new RatisNodePlan(nativeDomains.node,nativeDomains.incarnation,nativeDomains.data.stream().map(d->d.plan.domains().getFirst()).toList(),nativeDomains.data.getFirst().plan.catalogue(),homes.stream().map(h->h.placement).toList(),List.of(),Map.of(),5);
        }
        RatisDatabaseBackendFactory factory(Path path)throws IOException {
            return new RatisDatabaseBackendFactory(path,plan,nativeDomains.client,routes,scheduler,(name,shape)->nativeDomains.data.stream().filter(d->d.schema.columns().stream().anyMatch(c->c.name().equals(name))).findFirst().orElseThrow(()->new IOException("Undeclared placement")).domain,
                    (name,type,hash,revision)->{throw new IOException("No operator upload in this fixture");},RatisDatabaseBackendFactory.Policy.defaults(),directory->homes.stream().filter(h->h.directory.domainId().equals(directory.domainId())).findFirst().orElseThrow().client(),new RatisSchemaReplicaClient(1));
        }
        final class Home implements AutoCloseable {
            final UUID group,column;final RangeDirectory directory;final RatisNodePlan.Home placement;
            final ReplicatedRangeStore store;final RatisRangeStateMachine machine;final RaftStorage storage;final RaftGroup raft;
            final List<RangeSessionControl> bindings=new ArrayList<>();
            Home(RatisNodePlan.Role role,int n)throws Exception {
                group=new UUID(900,n);column=new UUID(901,n);var replica=nativeDomains.data.getFirst().descriptor.replicas().getFirst();
                var descriptor=new RangeDescriptor(new UUID(902,n),group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
                directory=new RangeDirectory(nativeDomains.outer.cluster,nativeDomains.outer.database,new UUID(903,n),1,new byte[32],0,Set.of(column),0,List.of(descriptor));
                placement=new RatisNodePlan.Home(role,directory,column,"backend-home-"+n);
                store=nativeDomains.catalogue.pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(directory.domainId(),Set.of(column),null,null),new byte[32],Map.of(column,placement.physicalName()),Set.of());
                raft=RaftGroup.valueOf(RaftGroupId.valueOf(group),RaftPeer.newBuilder().setId(replica.peerId()).build());
                machine=RatisRangeStateMachine.withSessions(store,raft,RatisSessionBindingTest.Fixture.LIMITS,role==RatisNodePlan.Role.TRANSACTION_HOME?RatisTransactionStatuses.validator(directory.clusterId(),directory.databaseId(),group,1,new byte[32],column):RatisSnapshotFences.validator(directory.clusterId(),directory.databaseId(),group,1,new byte[32],column),nativeDomains.catalogue.epochs,directory,RatisRangeStateMachine.RetentionLimits.defaults(),()->new RatisRangeStateMachine.Leadership(1,true));
                if(role==RatisNodePlan.Role.TRANSACTION_HOME){machine.role(role,(key,value)->RatisTransactionStatuses.validateRecord(directory.clusterId(),directory.databaseId(),key,value));machine.homeRecoveryReservations(directory.clusterId(),directory.databaseId(),column);}
                else{machine.role(role,(key,value)->RatisSnapshotFences.validateRecord(directory.clusterId(),directory.databaseId(),group,1,key,value));machine.snapshotHomeStorage(directory.clusterId(),directory.databaseId(),column);}
                storage=RaftStorage.newBuilder().setDirectory(root.resolve("home-raft-"+n).toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build();storage.initialize();
                var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->switch(method.getName()){case "getId"->raft.getPeers().iterator().next().getId();case "toString"->"backend-home-native";default->throw new AssertionError(method);});
                machine.initialize(server,raft.getGroupId(),storage);machine.notifyTermIndexUpdated(1,0);
            }
            byte[] query(byte[] bytes)throws IOException{try{return machine.query(RatisMultiDomainBootstrapTest.message(bytes)).get(10,TimeUnit.SECONDS).getContent().toByteArray();}catch(Exception failure){throw new IOException(failure);}}
            CommandResult append(DistributedCommand command)throws IOException{try{
                var tx=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(raft.getPeers().iterator().next().getId()).setGroupId(raft.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build());
                if(tx.getException()!=null)throw new IOException("HOME admission rejected",tx.getException());machine.preAppendTransaction(tx);tx.initLogEntry(1,store.applied().index()+1);var result=CommandResult.decode(machine.applyTransaction(tx).get(10,TimeUnit.SECONDS).getContent().toByteArray());if(command.sessionControl()!=null && result.status()==CommandResult.Status.SUCCESS)bindings.add(command.sessionControl());return result;
            }catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException(failure);}}
            RatisRoutedClient client(){return new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
                public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{return append(command);}
                public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return ScopedTransactionRpc.pointValue(query(ScopedTransactionRpc.pointRequest(column,key)),2<<20);}
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh)throws IOException{return query(request);}
                public RangeStateView stateView(RangeDescriptor route)throws IOException{return RangeStateView.decode(ScopedTransactionRpc.parse(query(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0])),ScopedTransactionRpc.MAX_REPLY_BYTES).body());}
                public void close(){transportCloses.incrementAndGet();}
            });}
            public void close()throws Exception{machine.close();machine.awaitNativeReaders(Duration.ofSeconds(5));storage.close();}
        }
        public void close()throws Exception{for(var home:homes)home.close();nativeDomains.close();scheduler.dispose();}
    }
    @Test void publicFactoryRoutesTwoDomainsIssuesDistinctNativeWindowsAndColdReopensExactGenerations()throws Exception {
        try(var fixture=new Fixture()){
            Path owned=root.resolve("backend");long before=fixture.nativeDomains.catalogue.epochs.read().nextEpoch();
            try(var factory=fixture.factory(owned)){
                var connection=factory.open(URI.create("ratis://controlled/backend"));var api=connection.getAsyncApi(RequestContext.batch());assertEquals(2,await(api.getAllColumnDefinitionsAsync()).size());
                for(int n=0;n<2;n++){long column=71+n;await(api.putAsync(0,column,key(7),value(10+n),RequestType.none()));await(api.putBatchAsync(column,RatisRocksDBBatchesTest.one(new KVBatch.KVBatchRef(List.of(key(8),key(9)),List.of(value(20+n),value(30+n)))),PutBatchMode.WRITE_BATCH));assertArrayEquals(new byte[]{(byte)(20+n)},await(api.getAsync(0,column,key(8),RequestType.<Buf>current())).toByteArray());}
                long transaction=await(api.openTransactionAsync(Duration.ofSeconds(10)));assertTrue(await(api.closeTransactionAsync(transaction,false)));
                assertEquals(before+6,fixture.nativeDomains.catalogue.epochs.read().nextEpoch(),"Each domain owns allocation, point and bulk exclusive grants");
                var grants=fixture.nativeDomains.catalogue.epochs.read().grants();assertEquals(6,grants.size());assertEquals(6,grants.stream().map(SessionEpochLedger.Grant::sessionId).distinct().count());
                var transactionHome=fixture.homes.stream().filter(h->h.placement.role()==RatisNodePlan.Role.TRANSACTION_HOME).findFirst().orElseThrow();assertEquals(6,transactionHome.bindings.size());
                var snapshotHome=fixture.homes.stream().filter(h->h.placement.role()==RatisNodePlan.Role.SNAPSHOT_HOME).findFirst().orElseThrow();assertEquals(2,snapshotHome.bindings.size());assertTrue(snapshotHome.bindings.stream().allMatch(b->b.outstandingWindow()==4096));
            }
            assertEquals(2,fixture.transportCloses.get());long issued=fixture.nativeDomains.catalogue.epochs.read().nextEpoch();
            try(var factory=fixture.factory(owned)){
                var connection=factory.open(URI.create("ratis://controlled/backend"));var api=connection.getSyncApi(RequestContext.batch());for(int n=0;n<2;n++){assertArrayEquals(new byte[]{(byte)(10+n)},api.get(0,71+n,key(7),RequestType.<Buf>current()).toByteArray());assertArrayEquals(new byte[]{(byte)(30+n)},api.get(0,71+n,key(9),RequestType.<Buf>current()).toByteArray());}
                assertEquals(issued,fixture.nativeDomains.catalogue.epochs.read().nextEpoch());assertEquals(2,await(connection.directRecoveryStatus(RequestContext.batch())).size());
            }
            assertEquals(4,fixture.transportCloses.get());for(var data:fixture.nativeDomains.data)assertArrayEquals(new byte[]{(byte)(10+fixture.nativeDomains.data.indexOf(data))},data.routed.read(data.column,new byte[]{7}));
        }
    }
}
