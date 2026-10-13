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
            var domains=nativeDomains.data.stream().map(d->d.plan.domains().getFirst()).toList();var catalogue=nativeDomains.data.getFirst().plan.catalogue();var placements=homes.stream().map(h->h.placement).toList();
            plan=new RatisNodePlan(nativeDomains.node,nativeDomains.incarnation,domains,catalogue,placements,List.of(),Map.of(),5,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(domains),RatisPoolFixtures.groups(catalogue,placements));
        }
        RatisDatabaseBackendFactory factory(Path path)throws IOException {return factory(path,nativeDomains.client);}
        RatisDatabaseBackendFactory factory(Path path,org.apache.ratis.client.RaftClient catalogue)throws IOException {
            return new RatisDatabaseBackendFactory(path,plan,catalogue,routes,scheduler,(name,shape)->nativeDomains.data.stream().filter(d->d.schema.columns().stream().anyMatch(c->c.name().equals(name))).findFirst().orElseThrow(()->new IOException("Undeclared placement")).domain,
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
                assertEquals(before+4,fixture.nativeDomains.catalogue.epochs.read().nextEpoch(),"Each used domain owns point and bulk grants; an untouched lazy transaction allocates none");
                var grants=fixture.nativeDomains.catalogue.epochs.read().grants();assertEquals(4,grants.size());assertEquals(4,grants.stream().map(SessionEpochLedger.Grant::sessionId).distinct().count());
                var transactionHome=fixture.homes.stream().filter(h->h.placement.role()==RatisNodePlan.Role.TRANSACTION_HOME).findFirst().orElseThrow();assertEquals(4,transactionHome.bindings.size());
                var snapshotHome=fixture.homes.stream().filter(h->h.placement.role()==RatisNodePlan.Role.SNAPSHOT_HOME).findFirst().orElseThrow();assertTrue(snapshotHome.bindings.isEmpty(),"No snapshot grant is issued until a transaction actually captures or stages data");
            }
            assertEquals(2,fixture.transportCloses.get());long issued=fixture.nativeDomains.catalogue.epochs.read().nextEpoch();
            try(var factory=fixture.factory(owned)){
                var connection=factory.open(URI.create("ratis://controlled/backend"));var api=connection.getSyncApi(RequestContext.batch());for(int n=0;n<2;n++){assertArrayEquals(new byte[]{(byte)(10+n)},api.get(0,71+n,key(7),RequestType.<Buf>current()).toByteArray());assertArrayEquals(new byte[]{(byte)(30+n)},api.get(0,71+n,key(9),RequestType.<Buf>current()).toByteArray());}
                assertEquals(issued,fixture.nativeDomains.catalogue.epochs.read().nextEpoch());assertEquals(2,await(connection.directRecoveryStatus(RequestContext.batch())).size());
            }
            assertEquals(4,fixture.transportCloses.get());for(var data:fixture.nativeDomains.data)assertArrayEquals(new byte[]{(byte)(10+fixture.nativeDomains.data.indexOf(data))},data.routed.read(data.column,new byte[]{7}));
        }
    }
    /** Connection journals reopen while native stores stay live. The extra domain shares the actual capture fixture catalogue. */
    final class TransactionFixture implements AutoCloseable {
        final RatisAtomicClientTest.Fixture nativeState;
        final UUID extraDomain=new UUID(910,1),extraColumn=new UUID(910,2),extraGroup=new UUID(910,3);
        final DistributedSchema extraSchema;final RangeDirectory extraDirectory;final RatisRoutedClient extra;
        final RatisNodePlan plan;final Map<UUID,RatisRoutedClient> routes;
        final AtomicInteger ownedHomeCloses=new AtomicInteger();
        TransactionFixture()throws Exception {this(null);}
        TransactionFixture(PoolSnapshotAdmission.Limits pins)throws Exception {
            nativeState=new RatisAtomicClientTest.Fixture(root.resolve("transaction-native"),true,Map.of(),pins);
            var f=nativeState;var replica=f.directory.ranges().getFirst().replicas().getFirst();
            extraSchema=new DistributedSchema(f.outer.cluster,f.outer.database,extraDomain,1,1,List.of(new DistributedSchema.Column(75,extraColumn,"second-domain",f.schema.column(71).schema(),null)));
            var descriptor=new RangeDescriptor(new UUID(910,4),extraGroup,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
            extraDirectory=new RangeDirectory(f.outer.cluster,f.outer.database,extraDomain,1,extraSchema.digest(),0,Set.of(extraColumn),1,List.of(descriptor));
            var store=f.catalogue.pool.openRange(new RangeOwnership.Owner(extraGroup,1),new RangeOwnership.Interval(extraDomain,Set.of(extraColumn),null,null),extraSchema.digest(),Map.of(extraColumn,"second-domain"),Set.of());store.enableLogicalSchema(extraSchema);f.stores.put(extraGroup,store);
            var raft=RaftGroup.valueOf(RaftGroupId.valueOf(extraGroup),RaftPeer.newBuilder().setId(replica.peerId()).build());f.raftGroups.put(extraGroup,raft);
            var machine=RatisRangeStateMachine.withSessions(store,raft,RatisSessionBindingTest.Fixture.LIMITS,command->{},f.catalogue.epochs,extraDirectory,RatisRangeStateMachine.RetentionLimits.defaults(),()->new RatisRangeStateMachine.Leadership(1,true));
            machine.transactionHomes(f.homes);machine.snapshotHomes(f.snapshots);f.machines.put(extraGroup,machine);
            var storage=RaftStorage.newBuilder().setDirectory(root.resolve("extra-raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build();storage.initialize();f.storages.add(storage);
            var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->switch(method.getName()){case "getId"->raft.getPeers().iterator().next().getId();case "toString"->"backend-extra-native";default->throw new AssertionError(method);});machine.initialize(server,raft.getGroupId(),storage);machine.notifyTermIndexUpdated(1,0);
            extra=new RatisRoutedClient(extraDirectory,f.transport());routes=Map.of(f.schema.domainId(),f.data,extraDomain,extra);
            var bindings=new LinkedHashMap<UUID,String>();for(var column:f.schema.columns())bindings.put(column.columnId(),column.name());
            var catalogue=new RatisNodePlan.Catalogue(f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.outer.sessions,List.of(replica));
            var domains=List.of(new RatisNodePlan.Domain(f.directory,bindings,Set.of()),new RatisNodePlan.Domain(extraDirectory,Map.of(extraColumn,"second-domain"),Set.of()));var homes=List.of(new RatisNodePlan.Home(RatisNodePlan.Role.SNAPSHOT_HOME,f.snapshotDirectory,f.snapshotColumn,"snapshot-home"),new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,f.homeDirectory,f.homeColumn,"atomic-home"));
            plan=new RatisNodePlan(replica.nodeId(),replica.nodeIncarnation(),domains,catalogue,homes,List.of(),Map.of(),6,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(domains),RatisPoolFixtures.groups(catalogue,homes));
            var bootstrapDomains=List.of(new RatisDatabaseBootstrap.Domain(plan,f.data,f.schema,null),new RatisDatabaseBootstrap.Domain(plan,extra,extraSchema,RatisColumnAllocation.counterValue(74)));
            try(var bootstrap=new RatisDatabaseBootstrap(Files.createDirectory(root.resolve("transaction-bootstrap")),bootstrapDomains,f.catalogue.client,f.catalogue.epochs.read(),null,10,()->10L,3)){
                for(int n=0;n<40 && bootstrap.progress().phase()!=RatisDatabaseBootstrap.Phase.READY;n++)bootstrap.step(3);assertEquals(RatisDatabaseBootstrap.Phase.READY,bootstrap.progress().phase());
            }
        }
        RatisDatabaseBackendFactory factory(Path owned)throws IOException {
            var f=nativeState;
            return new RatisDatabaseBackendFactory(owned,plan,f.catalogue.client,routes,f.scheduler,(name,shape)->name.equals("second-domain")?extraDomain:f.schema.domainId(),(name,type,hash,revision)->{throw new IOException("No operator upload in this fixture");},RatisDatabaseBackendFactory.Policy.defaults(),directory->new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
                final RatisRoutedClient.Transport transport=f.transport();
                public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{return transport.command(route,command);}
                public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return transport.read(route,column,key);}
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh)throws IOException{return transport.transaction(route,request,fresh);}
                public byte[] participant(RangeDescriptor route,byte[] record)throws IOException{return transport.participant(route,record);}
                public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request)throws IOException{return transport.snapshot(route,owner,request);}
                public RangeStateView stateView(RangeDescriptor route)throws IOException{return transport.stateView(route);}
                public void close(){ownedHomeCloses.incrementAndGet();}
            }),new RatisSchemaReplicaClient(1));
        }
        public void close()throws Exception{extra.close();nativeState.close();}
    }
    @Test void factoryIssuedAllocationCapturesAndCommitsAcrossColumnsThenReopensWithoutNewGrants()throws Exception {
        try(var fixture=new TransactionFixture()){
            var f=fixture.nativeState;Path owned=root.resolve("transaction-backend");long issued;
            long firstTransaction;
            try(var factory=fixture.factory(owned)){
                var connection=factory.open(URI.create("ratis://controlled/transaction-backend"));var api=connection.getAsyncApi(RequestContext.batch());assertEquals(5,await(api.getAllColumnDefinitionsAsync()).size());
                await(api.putAsync(0,75,key(7),value(55),RequestType.none()));int captures=f.captureRequests.get();
                firstTransaction=await(api.openTransactionAsync(Duration.ofSeconds(30)));assertEquals(captures,f.captureRequests.get());
                await(api.putAsync(firstTransaction,71,key(2),value(41),RequestType.none()));await(api.putAsync(firstTransaction,72,key(200),value(42),RequestType.none()));
                assertTrue(f.captureRequests.get()>captures,"Factory durable allocation must drive real native captures");assertArrayEquals(new byte[]{41},await(api.getAsync(firstTransaction,71,key(2),RequestType.<Buf>current())).toByteArray());
                assertNull(await(api.getAsync(0,71,key(2),RequestType.<Buf>current())),"Uncommitted cross-column state remains private");
                assertTrue(await(api.closeTransactionAsync(firstTransaction,true)));assertArrayEquals(new byte[]{41},await(api.getAsync(0,71,key(2),RequestType.<Buf>current())).toByteArray());assertArrayEquals(new byte[]{42},await(api.getAsync(0,72,key(200),RequestType.<Buf>current())).toByteArray());
                for(var store:f.stores.values())assertEquals(0,store.activeSnapshotPins());issued=f.catalogue.epochs.read().nextEpoch();
                var retainedGrant=f.catalogue.epochs.read().grants().stream().filter(grant->!grant.sessionId().equals(f.session)).toList();assertEquals(5,retainedGrant.size(),"Four point/bulk grants plus the one domain that actually stages a transaction");assertEquals(5,retainedGrant.stream().map(SessionEpochLedger.Grant::sessionId).distinct().count());
            }
            assertEquals(2,fixture.ownedHomeCloses.get());int captures=f.captureRequests.get();
            try(var factory=fixture.factory(owned)){
                var connection=factory.open(URI.create("ratis://controlled/transaction-backend"));var api=connection.getAsyncApi(RequestContext.batch());assertArrayEquals(new byte[]{41},await(api.getAsync(0,71,key(2),RequestType.<Buf>current())).toByteArray());assertArrayEquals(new byte[]{42},await(api.getAsync(0,72,key(200),RequestType.<Buf>current())).toByteArray());assertArrayEquals(new byte[]{55},await(api.getAsync(0,75,key(7),RequestType.<Buf>current())).toByteArray());
                long transaction=await(api.openTransactionAsync(Duration.ofSeconds(30)));assertTrue(transaction>firstTransaction);await(api.putAsync(transaction,71,key(3),value(99),RequestType.none()));assertTrue(f.captureRequests.get()>captures);assertTrue(await(api.closeTransactionAsync(transaction,false)));assertNull(await(api.getAsync(0,71,key(3),RequestType.<Buf>current())));assertEquals(issued,f.catalogue.epochs.read().nextEpoch());
                for(var store:f.stores.values())assertEquals(0,store.activeSnapshotPins());
            }
            assertEquals(4,fixture.ownedHomeCloses.get());assertArrayEquals(new byte[]{41},f.get(71,key(2)));assertArrayEquals(new byte[]{55},fixture.extra.read(fixture.extraColumn,new byte[]{7}));
        }
    }

    @Test void separateExportRootRemainsOwnedAndReopensWithoutAllocationOrSourceRpc()throws Exception {
        try(var fixture=new Fixture()) {
            Path owned=root.resolve("export-backend");long issued=fixture.nativeDomains.catalogue.epochs.read().nextEpoch();var borrowed=fixture.nativeDomains.client;
            var sourceAbsent=(org.apache.ratis.client.RaftClient)Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args)->{if(method.getName().equals("getGroupId"))return borrowed.getGroupId();throw new AssertionError("Source-free export open attempted catalogue RPC: "+method);});
            try(var factory=fixture.factory(owned,sourceAbsent)){var exports=factory.exports();assertSame(exports,factory.exports());assertThrows(java.nio.channels.OverlappingFileLockException.class,()->{try(var duplicate=fixture.factory(owned,sourceAbsent)){duplicate.exports();}});assertEquals(issued,fixture.nativeDomains.catalogue.epochs.read().nextEpoch());assertEquals(0,fixture.nativeDomains.catalogue.epochs.read().grants().size());assertFalse(Files.exists(owned.resolve("export-allocations")));factory.drainExports();}
            assertEquals(4,fixture.transportCloses.get());for(var data:fixture.nativeDomains.data)assertNull(data.routed.read(data.column,new byte[]{7}));
            try(var factory=fixture.factory(owned,sourceAbsent)){assertNotNull(factory.exports());assertEquals(issued,fixture.nativeDomains.catalogue.epochs.read().nextEpoch());assertEquals(0,fixture.nativeDomains.catalogue.epochs.read().grants().size());assertFalse(Files.exists(owned.resolve("export-allocations")));}
            assertEquals(6,fixture.transportCloses.get(),"The refused duplicate retires only its own HOME transports");
        }
    }

}
