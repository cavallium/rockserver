package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Whole backend/manager/session cold restoration with actual JNI and controlled Raft dispatch, no sockets/quorum. */
class RatisColdClosedPointBackendTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T>T await(CompletableFuture<T> future)throws Exception{return future.get(30,TimeUnit.SECONDS);}
    static Object invoke(Method method,Object target,Object[] args)throws Throwable{try{return method.invoke(target,args);}catch(InvocationTargetException failure){throw failure.getCause();}}
    @Test void coldBackendRecoversOriginalClosedPointBeforeNewCurrentGrantOrBindingAdmission()throws Exception {
        var helper=new RatisOperationRerouteCoordinatorTest();helper.root=root;
        try(var fixture=helper.new Fixture(false,true)){
            fixture.window.close();fixture.coordinator.close();var source=fixture.source;
            var schemas=new RatisSchemaCatalogue(fixture.catalogue.client,fixture.outer.cluster,fixture.outer.database,fixture.outer.catalogue,1,new byte[32],fixture.outer.directoryColumn);
            assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(new OperationId(new UUID(0,970),0,0),source.schema)).status());var directories=fixture.catalogue.epochs.catalogue(fixture.outer.directoryColumn);
            var rosters=new RatisDatabaseDomainRoster(fixture.catalogue.client,source.cluster,source.database,fixture.outer.catalogue,1,new byte[32],fixture.outer.directoryColumn);assertNull(rosters.read());
            // The reused topology fixture starts at generation one. Publish genuine genesis first, then its unchanged placement CAS.
            var genesis=new RangeDirectory(source.cluster,source.database,source.domain,source.directory.schemaEpoch(),source.directory.schemaDigest(),0,source.directory.columns(),source.directory.routingPrefixBytes(),source.directory.ranges());
            assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(new OperationId(new UUID(0,971),0,0),null,genesis,rosters.read())).status());
            assertEquals(DatabaseDomainRoster.initial(source.cluster,source.database,List.of(source.domain)),rosters.read());
            assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(new OperationId(new UUID(0,971),0,1),genesis,source.directory)).status());assertArrayEquals(source.directory.payloadDigest(),rosters.readComplete().directories().get(source.domain).payloadDigest());
            var transactionDirectory=fixture.homeClient.directory();UUID snapshotGroup=new UUID(0,972),snapshotColumn=new UUID(0,973),snapshotDomain=new UUID(0,974);var replica=transactionDirectory.ranges().getFirst().replicas().getFirst();
            var snapshotRange=new RangeDescriptor(snapshotGroup,snapshotGroup,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));var snapshotDirectory=new RangeDirectory(source.cluster,source.database,snapshotDomain,1,new byte[32],0,Set.of(snapshotColumn),0,List.of(snapshotRange));
            var snapshotStore=source.pool.openRange(new RangeOwnership.Owner(snapshotGroup,1),new RangeOwnership.Interval(snapshotDomain,Set.of(snapshotColumn),null,null),new byte[32],Map.of(snapshotColumn,"cold-point-snapshot-home"),Set.of());fixture.stores.put(snapshotGroup,snapshotStore);
            var snapshotMachine=RatisRangeStateMachine.withSessions(snapshotStore,fixture.group(snapshotGroup,replica.peerId()),RatisSessionBindingTest.Fixture.LIMITS,RatisSnapshotFences.validator(source.cluster,source.database,snapshotGroup,1,new byte[32],snapshotColumn),fixture.catalogue.epochs,snapshotDirectory,RatisRangeStateMachine.RetentionLimits.defaults(),()->new RatisRangeStateMachine.Leadership(1,true));snapshotMachine.role(RatisNodePlan.Role.SNAPSHOT_HOME,(key,value)->RatisSnapshotFences.validateRecord(source.cluster,source.database,snapshotGroup,1,key,value));snapshotMachine.snapshotHomeStorage(source.cluster,source.database,snapshotColumn);snapshotMachine.proofQueries(fixture.proofs);fixture.initialize(snapshotGroup,replica.peerId(),snapshotMachine);
            var domain=new RatisNodePlan.Domain(source.directory,Map.of(source.column,"data"),Set.of());var homes=List.of(new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,transactionDirectory,fixture.homeColumn,"reroute-home"),new RatisNodePlan.Home(RatisNodePlan.Role.SNAPSHOT_HOME,snapshotDirectory,snapshotColumn,"cold-point-snapshot-home"));
            var catReplica=new ReplicaDescriptor(new UUID(0,976),new UUID(0,977),new UUID(0,975),"fixture","127.0.0.1:9996",ReplicaDescriptor.Role.VOTER);var catalogue=new RatisNodePlan.Catalogue(fixture.outer.catalogue,1,new byte[32],fixture.outer.directoryColumn,fixture.outer.sessions,List.of(catReplica));var plan=new RatisNodePlan(source.local.nodeId(),source.local.nodeIncarnation(),List.of(domain),catalogue,homes,List.of(),Map.of(),4,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(catalogue,homes));
            // DATA and both HOME configurations come from their actual initialized groups, including endpoints.
            RatisRangeStateMachine.requireSchemaMembership(source.parent,source.store.configuration().data());RatisRangeStateMachine.requireSchemaMembership(transactionDirectory.ranges().getFirst(),fixture.stores.get(fixture.homeGroup).configuration().data());RatisRangeStateMachine.requireSchemaMembership(snapshotRange,snapshotStore.configuration().data());
            // The controlled native CAT fixture has no manufactured Raft CFG; bind its actual reply peer and group.
            assertNull(fixture.catalogue.store.configuration());assertEquals(catalogue.groupId(),fixture.catalogue.client.getGroupId().getUuid());var ledgerKey=java.nio.ByteBuffer.allocate(16).putLong(source.database.getMostSignificantBits()).putLong(source.database.getLeastSignificantBits()).array();var catReply=fixture.catalogue.client.io().sendReadOnly(RatisOperationRerouteCoordinatorTest.message(ScopedTransactionRpc.pointRequest(fixture.outer.sessions,ledgerKey)));assertEquals(catReplica.peerId(),catReply.getServerId().toString());
            var denyIssuance=new AtomicBoolean();var deniedIssuance=new AtomicInteger();var originalIo=fixture.catalogue.client.io();var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                if(denyIssuance.get() && method.getName().equals("send")){
                    var command=DistributedCommandCodec.decode(((Message)args[0]).getContent().toByteArray());for(var mutation:command.mutations())if(mutation.columnId().equals(fixture.outer.sessions)){
                        var next=SessionEpochLedger.decode(mutation.value());var before=command.predicates().stream().filter(p->p.columnId().equals(mutation.columnId()) && Arrays.equals(p.key(),mutation.key())).findFirst().orElseThrow().expectedValue();if(before==null || next.nextEpoch()>SessionEpochLedger.decode(before).nextEpoch()){deniedIssuance.incrementAndGet();throw new IOException("Fresh grant issuance deliberately unavailable");}
                    }
                }return invoke(method,originalIo,args);
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->method.getName().equals("io")?io:invoke(method,fixture.catalogue.client,args));
            Path backend=root.resolve("backend");RemoteMutationToken token;byte[] digest,row;long issued;
            try(var factory=factory(fixture,plan,client,backend)){
                var connection=factory.open(URI.create("ratis://controlled/cold-point"));var api=connection.getRemoteMutationApi();var owner=await(api.owners(1,RequestContext.batch())).stream().filter(o->o.kind()==RemoteMutationToken.Kind.POINT).findFirst().orElseThrow();token=await(api.preparePoint(owner,UUID.randomUUID(),owner.nextRequestSequence(),1,List.of(new Keys(Buf.wrap(new byte[]{10}))),List.of(Buf.wrap(new byte[]{77})),Mutation.Kind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()));
                fixture.activateSplit();fixture.beforeCredit=()->{try{fixture.closeSourceAndHome();}catch(Exception failure){throw new CompletionException(failure);}};
                var result=await(api.execute(token,RequestContext.batch()));assertEquals(RemoteMutationStatus.State.TERMINAL,result.state());assertEquals(CommandResult.Status.CONFLICT,result.outcome());digest=result.resultDigest();row=await(api.results(token,0,1,CommandResult.MAX_BYTES,RequestContext.batch())).rows().getFirst();assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(row).status());assertEquals(0,fixture.opens);assertEquals(1,fixture.closedAborts.size());issued=fixture.catalogue.epochs.read().nextEpoch();
            }
            fixture.sourceAvailable=false;fixture.denyChildBindings=true;denyIssuance.set(true);assertEquals(1,generations(backend));
            try(var factory=factory(fixture,plan,client,backend)){
                var connection=factory.open(URI.create("ratis://controlled/cold-point"));var api=connection.getRemoteMutationApi();assertEquals(0,deniedIssuance.get());assertEquals(0,fixture.deniedChildBindings);assertEquals(0,fixture.deniedSourceCalls);assertEquals(1,generations(backend),"Cold lookup must not create an absent current placement generation");
                var status=await(api.status(token,RequestContext.batch()));assertEquals(RemoteMutationStatus.State.TERMINAL,status.state());assertEquals(CommandResult.Status.CONFLICT,status.outcome());assertArrayEquals(digest,status.resultDigest());assertArrayEquals(row,await(api.results(token,0,1,CommandResult.MAX_BYTES,RequestContext.batch())).rows().getFirst());assertEquals(CommandResult.Status.CONFLICT,await(api.resume(token,RequestContext.batch())).outcome());
                assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,await(api.acknowledge(token,digest,RequestContext.batch())).state());assertEquals(1,fixture.consumes.size());assertTrue(fixture.ackCommands.isEmpty());assertEquals(issued,fixture.catalogue.epochs.read().nextEpoch());assertEquals(0,deniedIssuance.get());assertEquals(0,fixture.deniedSourceCalls);assertEquals(0,fixture.deniedChildBindings);
                try(var files=Files.walk(backend)){assertFalse(files.anyMatch(path->path.getFileName().toString().equals("input.bin")),"Exact caller ACK must reclaim retained reroute input");}
                assertThrows(ExecutionException.class,()->await(api.owners(1,RequestContext.batch())),"Fresh owner discovery remains the new-current admission boundary");assertTrue(deniedIssuance.get()>0 || fixture.deniedChildBindings>0);assertEquals(issued,fixture.catalogue.epochs.read().nextEpoch());assertEquals(0,fixture.deniedSourceCalls);assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,await(api.status(token,RequestContext.batch())).state());
            }
        }finally{helper.closePointBudget();}
    }
    static long generations(Path backend)throws IOException{try(var folders=Files.list(backend.resolve("generations"))){return folders.filter(Files::isDirectory).count();}}
    RatisDatabaseBackendFactory factory(RatisOperationRerouteCoordinatorTest.Fixture fixture,RatisNodePlan plan,RaftClient client,Path root)throws IOException {
        return new RatisDatabaseBackendFactory(root,plan,client,Map.of(fixture.source.domain,fixture.data),fixture.scheduler,(name,shape)->fixture.source.domain,(name,type,hash,revision)->{throw new IOException("No operator upload in this fixture");},RatisDatabaseBackendFactory.Policy.defaults(),directory->new RatisRoutedClient(directory,fixture.transport()),new RatisSchemaReplicaClient(1,(range,replica)->{throw new IOException("No schema replica operation in cold token recovery");}));
    }
}
