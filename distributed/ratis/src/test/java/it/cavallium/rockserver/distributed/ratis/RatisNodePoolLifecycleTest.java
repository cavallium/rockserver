package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Two actual native pools and controlled divisions. No sockets/quorum or successful learner-install claim. */
class RatisNodePoolLifecycleTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void uncertainSeedOwnsNodeCapacityWhileOtherPoolReadsAndColdDiscoveryRetainExactRouting()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        try(var f=helper.new Fixture()){
            UUID fast=new UUID(970,1),archive=new UUID(970,2);
            var local=f.plan.directory(f.first.domainId()).ranges().getFirst().replicas().stream().filter(r->r.nodeId().equals(f.plan.nodeId())).findFirst().orElseThrow();
            var parent=new RangeDescriptor(new UUID(971,1),new UUID(971,2),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));
            var first=new RangeDirectory(f.first.clusterId(),f.first.databaseId(),f.first.domainId(),f.first.schemaEpoch(),f.first.digest(),7,f.first.columnIds(),f.first.routingPrefixBytes(),List.of(parent));
            var domains=List.of(new RatisNodePlan.Domain(first,f.plan.domain(f.first.domainId()).columns(),Set.of()),f.plan.domain(f.second.domainId()));
            var plan=new RatisNodePlan(f.plan.nodeId(),f.plan.nodeIncarnation(),domains,f.plan.catalogue(),List.of(),List.of(),Map.of(),4,List.of(limited(fast,3),limited(archive,1)),Map.of(f.first.domainId(),fast,f.second.domainId(),archive),Map.of(f.plan.catalogue().groupId(),fast));
            try(var firstClient=new RatisRoutedClient(first,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor route,UUID column,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor route,DistributedCommand command){throw new AssertionError();}})){
                var clients=Map.of(f.first.domainId(),firstClient,f.second.domainId(),f.secondClient);
                var left=new RangeDescriptor(new UUID(972,1),new UUID(972,2),1,0,null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,List.of(local));var right=new RangeDescriptor(new UUID(972,3),new UUID(972,4),1,0,new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));
                var target=new RangeDirectory(first.clusterId(),first.databaseId(),first.domainId(),first.schemaEpoch(),first.schemaDigest(),8,first.columns(),first.routingPrefixBytes(),List.of(left,right));var split=new TopologyChange.Definition(new UUID(972,5),first,target);
                var state=new TopologyChange.State[]{TopologyChange.State.begin(split.header()).beginFencing()};var authorityReads=new AtomicInteger();var joinReads=new AtomicInteger();
                var remote=plan.catalogue().replicas().getFirst();var remoteRange=new RangeDescriptor(new UUID(973,1),new UUID(973,2),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(remote));
                var remoteDirectory=new RangeDirectory(f.second.clusterId(),f.second.databaseId(),f.second.domainId(),f.second.schemaEpoch(),f.second.digest(),1,f.second.columnIds(),f.second.routingPrefixBytes(),List.of(remoteRange));
                var receiver=new ReplicaDescriptor(plan.nodeId(),plan.nodeIncarnation(),new UUID(973,3),plan.peerId(),plan.address(),ReplicaDescriptor.Role.LISTENER);var receiverRange=new RangeDescriptor(remoteRange.rangeId(),remoteRange.groupId(),remoteRange.rangeEpoch(),1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(remote,receiver));
                var receiverDirectory=new RangeDirectory(remoteDirectory.clusterId(),remoteDirectory.databaseId(),remoteDirectory.domainId(),remoteDirectory.schemaEpoch(),remoteDirectory.schemaDigest(),2,remoteDirectory.columns(),remoteDirectory.routingPrefixBytes(),List.of(receiverRange));var relocation=new TopologyChange.Definition(new UUID(973,4),remoteDirectory,receiverDirectory);var joining=RatisTopologyJoinAuthority.Request.of(relocation,receiver.replicaId());
                commit(f,split,state[0]);commit(f,relocation,TopologyChange.State.begin(relocation.header()));
                var server=new RatisNodeRuntimeTest.FakeServer();RatisNodeRuntime node=RatisNodeRuntime.open(plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,clients,f.env.scheduler,f.env.timer,()->20L,f.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server::create,f.schemas);
                final RatisNodeRuntime.SeedAuthorityReader seedAuthority=(header,stage)->{authorityReads.incrementAndGet();return RatisTopologyAuthority.readNative(f.env.catalogue.store,f.env.outer.directoryColumn,plan.clusterId(),plan.databaseId(),RatisTopologyAuthority.Request.of(header,stage));};
                try{
                    field(node,"seedAuthority",seedAuthority);field(node,"joinAuthority",(RatisNodeRuntime.JoinAuthorityReader)request->{joinReads.incrementAndGet();return RatisTopologyJoinAuthority.readNative(f.env.catalogue.store,f.env.outer.directoryColumn,plan.clusterId(),plan.databaseId(),request);});node.learnerSource((view,owner)->{throw new AssertionError("Full node capacity must reject before taking a source pin");});
                    var parentMachine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));field(parentMachine,"topologyAuthority",(RatisRangeStateMachine.TopologyAuthorityReader)seedAuthority::read);node.start();
                    var parentStore=f.store(server,parent.groupId());var otherGroup=f.secondDirectory.ranges().getFirst().groupId();var other=f.store(server,otherGroup);var otherColumn=f.second.columnIds().iterator().next();
                    var write=new DistributedCommand(otherGroup,1,f.second.digest(),new OperationId(new UUID(974,1),1,0),List.of(new Mutation(Mutation.Kind.PUT,otherColumn,new byte[]{7},new byte[]{99})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(other.apply(new ApplyPosition(1,other.applied().index()+1),write)).status());
                    var record=new RatisTopologyFreezeRecord(split,parent.groupId(),seedAuthority.read(split.header(),TopologyChange.Stage.FREEZE).admissionDigest());parentMachine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologyFreezeRpc.prepareRequest(record)))).get(5,TimeUnit.SECONDS);
                    var tx=parentMachine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(parent.groupId())).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(record.encode()))).build());assertNull(tx.getException());parentMachine.preAppendTransaction(tx);tx.initLogEntry(1,parentStore.applied().index()+1);var frozen=NativeTopologyFreeze.decode(applyCommitted(parentMachine,tx).getContent().toByteArray());state[0]=state[0].frozen(frozen.proof(local.replicaId()));commit(f,split,state[0]);
                    var request=new RatisTopologySeedRpc.Request(split,plan.catalogue().groupId(),parent.groupId(),local.replicaId());var once=new AtomicBoolean(true);node.nativePool(fast).topologySeedForceObserver(after->{if(after && once.getAndSet(false))throw new org.rocksdb.RocksDBException("Unknown forced pool-A seed response");});
                    var active=node;assertThrows(IOException.class,()->active.seedTopology(request));assertNull(node.nativePool(fast).topologySeed(split.intentId()));assertNotNull(field(node,"seedAttempt",Object.class));int reads=authorityReads.get();
                    var blocked=assertThrows(IOException.class,()->active.joinTopologyLearner(joining));assertTrue(blocked.getMessage().contains("retained force attempt"));assertEquals(0,joinReads.get());assertTrue(node.nativePool(archive).topologyLearners().isEmpty());
                    try(var pin=other.capture()){assertArrayEquals(new byte[]{99},pin.get(otherColumn,new byte[]{7}));assertEquals(archive,node.poolId(otherGroup));assertEquals(1,node.nativePool(archive).snapshotAdmissionStats().pins());var saved=node.seedTopology(request);assertEquals(reads,authorityReads.get());assertArrayEquals(saved.encode(),node.seedTopology(request).encode());assertEquals(1,node.nativePool(fast).topologySeeds().size());assertEquals(4,field(node,"groups",RatisRuntimeGroups.class).raftGroups().size());for(var child:split.targets())assertEquals(fast,node.poolId(child.groupId()));assertArrayEquals(new byte[]{99},pin.get(otherColumn,new byte[]{7}));}
                    assertNull(field(node,"seedAttempt",Object.class));var capacity=assertThrows(IOException.class,()->active.joinTopologyLearner(joining));assertNotNull(capacity.getCause());assertTrue(capacity.getCause().getMessage().contains("local group capacity"));assertEquals(1,joinReads.get());assertFalse(server.machines.containsKey(RaftGroupId.valueOf(remoteRange.groupId())));assertTrue(node.nativePool(archive).topologyLearners().isEmpty());
                    var saved=node.nativePool(fast).topologySeed(split.intentId());node.close();server=new RatisNodeRuntimeTest.FakeServer();node=RatisNodeRuntime.open(plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,clients,f.env.scheduler,f.env.timer,()->20L,f.settings(),RatisNodeRuntime.Startup.RECOVER,server::create,f.schemas);node.start();
                    assertArrayEquals(saved.encode(),node.seedTopology(request).encode());assertEquals(4,field(node,"groups",RatisRuntimeGroups.class).raftGroups().size());for(var child:split.targets())assertEquals(fast,node.poolId(child.groupId()));assertEquals(archive,node.poolId(otherGroup));assertEquals(1,node.nativePool(fast).topologySeeds().size());assertTrue(node.nativePool(archive).topologySeeds().isEmpty());assertTrue(node.nativePool(archive).topologyLearners().isEmpty());try(var pin=f.store(server,otherGroup).capture()){assertArrayEquals(new byte[]{99},pin.get(otherColumn,new byte[]{7}));}
                }finally{node.close();}
            }
        }
    }
    private static Message applyCommitted(RatisRangeStateMachine machine,org.apache.ratis.statemachine.TransactionContext transaction)throws Exception {
        // This controlled server has no Ratis updater; preserve its real read/dispatch/native-pump ownership.
        try(var updater=java.util.concurrent.Executors.newSingleThreadExecutor()){
            return updater.submit(()->{
                var admission=machine.committedApplyAdmission();org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor descriptor;long index=transaction.getLogEntry().getIndex();
                try(var read=admission.awaitRead(index)){descriptor=admission.describe(transaction.getLogEntry());}
                var permit=admission.awaitDispatch(descriptor);admission.offer(descriptor,permit);java.util.concurrent.CompletableFuture<Message> result;
                try{result=machine.applyTransaction(transaction);}finally{admission.withdraw(descriptor,permit);if(!permit.claimed())permit.close();}
                try(var next=admission.awaitRead(index+1)){}return result.get(5,TimeUnit.SECONDS);
            }).get(5,TimeUnit.SECONDS);
        }
    }
    static RatisNativePoolSpec limited(UUID id,int maximum){var original=RatisPoolFixtures.spec(id,false);return new RatisNativePoolSpec(id,original.configuration(),maximum,original.images(),original.retention(),original.pins(),original.replicas(),original.resources(),original.maximumDurableMetadataBytes());}
    static void commit(RatisNodeMultiDomainTest.Fixture fixture,TopologyChange.Definition definition,TopologyChange.State state)throws Exception {
        var env=fixture.env;var directory=definition.sourceDirectory();byte[] domain=ByteBuffer.allocate(16).putLong(directory.domainId().getMostSignificantBits()).putLong(directory.domainId().getLeastSignificantBits()).array();var command=new DistributedCommand(env.outer.catalogue,1,new byte[32],new OperationId(new UUID(975,1),1,env.catalogue.next.get()),List.of(new Mutation(Mutation.Kind.PUT,env.outer.directoryColumn,RatisTopologyCatalogue.definitionKey(definition.digest()),definition.encode()),new Mutation(Mutation.Kind.PUT,env.outer.directoryColumn,RatisTopologyCatalogue.headKey(directory.domainId()),state.encode()),new Mutation(Mutation.Kind.PUT,env.outer.directoryColumn,RatisTopologyCatalogue.stateKey(definition.intentId()),state.encode()),new Mutation(Mutation.Kind.PUT,env.outer.directoryColumn,domain,RangeDirectoryCodec.encode(directory))));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(env.catalogue.store.apply(new ApplyPosition(1,env.catalogue.next.getAndIncrement()),command)).status());
    }
}
