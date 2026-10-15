package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual socket-free Raft dispatch to an empty fenced receiver, including cold re-force. */
class RatisRecoverySupersessionQueryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void emptyReceiverOnlyAcknowledgesItsForcedFenceAcrossColdReopen()throws Exception {
        var schema=new RatisNodeServiceTest().schema();
        var local=new ReplicaDescriptor(new UUID(12900,1),new UUID(12900,2),new UUID(12900,3),"receiver","127.0.0.1:4519",ReplicaDescriptor.Role.VOTER);
        var group=new UUID(12900,4);var parent=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));
        var directory=new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),1,schema.columnIds(),schema.routingPrefixBytes(),List.of(parent));
        var children=new ArrayList<RangeDescriptor>();for(int n=0;n<2;n++){var id=new UUID(12900,10+n);children.add(new RangeDescriptor(id,id,1,0,n==0?null:new byte[]{(byte)128},n==0?new byte[]{(byte)128}:null,RangeDescriptor.TopologyState.ACTIVE,List.of(local)));}
        var next=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),2,directory.columns(),directory.routingPrefixBytes(),children);
        var definition=new TopologyChange.Definition(new UUID(12900,20),directory,next);var catalogue=new UUID(12900,21);
        var scope=new NativeRecoverySupersession.Scope(catalogue,new ApplyPosition(1,20),definition.header(),directory,group,local,new byte[32],List.of(new byte[32]));
        var remote=new ReplicaDescriptor(new UUID(12900,30),new UUID(12900,31),new UUID(12900,32),"catalogue","127.0.0.1:4520",ReplicaDescriptor.Role.VOTER);
        var cat=new RatisNodePlan.Catalogue(catalogue,1,new byte[32],new UUID(12900,33),new UUID(12900,34),List.of(remote));
        var domain=new RatisNodePlan.Domain(directory,Map.of(schema.columnIds().iterator().next(),"data"),Set.of());
        var plan=new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),List.of(domain),cat,List.of(),List.of(),Map.of(),4,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(cat,List.of()));
        RatisRecoverySupersessionRpc.Request expected=null;
        for(int cold=0;cold<2;cold++)try(var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("native"))) {
            var store=pool.forceRecoverySupersession(scope);var target=RatisRecoverySupersessionRpc.Request.of(store.acknowledgedRecoverySupersession());
            if(expected!=null)assertEquals(expected,target);expected=target;
            var limits=RatisNodeRuntime.ResourceLimits.defaults();var budget=new RatisNodeResourceBudget(limits.node(),Map.of(RatisPoolFixtures.ID,limits.pool()));
            var raftGroup=plan.localGroups().getFirst();var machine=new RatisRangeStateMachine(store,raftGroup,RatisLocalMetadataDiscoveryTest.IMAGES);
            machine.topologyAuthority((header,stage)->{throw new AssertionError("Fence query performs no remote authority IO");},directory,local,pool);
            machine.nodeResources(budget,RatisPoolFixtures.ID);
            var logLimits=RatisLogResourceBudget.Limits.defaults();machine.logResources(new RatisLogResourceBudget(group,new RatisLogResourceBudget.Shared(logLimits),new RatisLogResourceBudget.Shared(logLimits),logLimits,(bytes,progress)->()->{}));
            var policy=RatisCommittedApplyDispatcher.Policy.defaults();var dispatcher=new RatisCommittedApplyDispatcher(policy.node(),Map.of(RatisPoolFixtures.ID,policy.pool()));machine.committedApply(dispatcher,RatisPoolFixtures.ID,NativeSstImportManifest.MAX_FILE_BYTES+4096);
            machine.deferRecoveryDiscovery();
            try(var server=RatisLocalMetadataDiscoveryTest.noSocketServer(plan,root.resolve("raft"),Map.of(raftGroup.getGroupId(),machine))) {
                server.start();assertNull(store.applied());assertNull(store.configuration());assertTrue(store.userHandlesDetached());assertFalse(store.admissionOpen());assertFalse(store.metadataRecoveryImageInstalled());assertNull(store.metadataRecoveryRegistration());
                assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertNull(machine.getLatestSnapshot());assertEquals(-1,machine.getLastAppliedTermIndex().getIndex());
                var before=budget.stats();byte[] wire=RatisRecoverySupersessionRpc.request(target);
                for(int retry=0;retry<2;retry++){var reply=stale(server,raftGroup,wire,0);assertTrue(reply.isSuccess(),()->String.valueOf(reply.getException()));assertEquals(RaftPeerId.valueOf(local.peerId()),reply.getServerId());RatisRecoverySupersessionRpc.reply(reply.getMessage().getContent().toByteArray(),target);}
                var acknowledgements=new java.util.concurrent.atomic.AtomicInteger();var sends=new java.util.concurrent.atomic.AtomicInteger();var wrongPeer=new java.util.concurrent.atomic.AtomicBoolean();
                var io=(org.apache.ratis.client.api.BlockingApi)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{org.apache.ratis.client.api.BlockingApi.class},(proxy,method,args)->{
                    if(!method.getName().equals("sendStaleRead"))throw new AssertionError(method);
                    assertEquals(0L,args[1]);assertEquals(server.getId(),args[2]);sends.incrementAndGet();var reply=stale(server,raftGroup,((Message)args[0]).getContent().toByteArray(),0);
                    return wrongPeer.get()?new RaftClientReply.Builder().setClientId(reply.getClientId()).setServerId(RaftPeerId.valueOf("foreign-receiver")).setGroupId(reply.getRaftGroupId()).setCallId(reply.getCallId()).setSuccess(true).setMessage(reply.getMessage()).build():reply;
                });
                var client=(org.apache.ratis.client.RaftClient)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->raftGroup.getGroupId();case "io"->io;default->throw new AssertionError(method);});
                var transport=new RatisTopologySourceSnapshotClient(client,bytes->{assertEquals(RatisRecoverySupersessionRpc.MAX_REPLY_BYTES,bytes);acknowledgements.incrementAndGet();});
                transport.superseded(target,local);assertEquals(1,acknowledgements.get());
                wrongPeer.set(true);assertThrows(java.io.IOException.class,()->transport.superseded(target,local));wrongPeer.set(false);
                var foreign=new ReplicaDescriptor(local.nodeId(),UUID.randomUUID(),local.replicaId(),local.peerId(),local.address(),local.role());
                assertThrows(java.io.IOException.class,()->transport.superseded(target,foreign));assertEquals(2,sends.get());assertEquals(1,acknowledgements.get());
                assertEquals(before,budget.stats());
                pool.recoverySupersessionForceObserver(after->{if(after)throw new org.rocksdb.RocksDBException("Lost fence force acknowledgement");});
                assertThrows(org.rocksdb.RocksDBException.class,()->pool.forceRecoverySupersession(scope));assertNull(store.acknowledgedRecoverySupersession());
                rejected(server,raftGroup,wire,0);
                pool.recoverySupersessionForceObserver(after->{});assertSame(store,pool.forceRecoverySupersession(scope));
                assertTrue(stale(server,raftGroup,wire,0).isSuccess());
                rejected(server,raftGroup,wire,1);
                var changed=wire.clone();changed[changed.length-1]^=1;rejected(server,raftGroup,changed,0);
                rejected(server,raftGroup,ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]),0);
                try(var denied=budget.reserve(RatisPoolFixtures.ID,RatisNodeResourceBudget.Kind.PROTECTED,Math.min(limits.node().protectedLane().bytes(),limits.pool().protectedLane().bytes())-before.protectedLane().bytes(),1)) {rejected(server,raftGroup,wire,0);}
                assertEquals(before,budget.stats());assertEquals(-1,machine.takeSnapshot());assertNull(store.applied());assertNull(store.configuration());assertEquals(-1,server.getDivision(raftGroup.getGroupId()).getRaftLog().getLastCommittedIndex());assertEquals(-1,machine.getLastAppliedTermIndex().getIndex());
                assertThrows(IllegalStateException.class,machine.recoveryEligibilityGate()::snapshotPermit);
                machine.beginShutdown();assertThrows(ExecutionException.class,()->machine.queryStale(message(wire),0).get(5,TimeUnit.SECONDS));
            }finally{machine.beginShutdown();machine.awaitNativeReaders(Duration.ofSeconds(5));dispatcher.awaitDrained(Duration.ofSeconds(5));dispatcher.close();}
        }
    }
    static void rejected(RaftServer server,RaftGroup group,byte[] bytes,long minimum)throws Exception {
        try{assertFalse(stale(server,group,bytes,minimum).isSuccess());}
        catch(ExecutionException denied){var cause=denied.getCause();assertTrue(cause instanceof java.io.IOException || cause instanceof IllegalArgumentException || cause instanceof IllegalStateException,()->String.valueOf(cause));}
    }
    static Message message(byte[] bytes){return Message.valueOf(ByteString.copyFrom(bytes));}
    static RaftClientReply stale(RaftServer server,RaftGroup group,byte[] bytes,long minimum)throws Exception {
        var request=RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(server.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.staleReadRequestType(minimum)).setMessage(message(bytes)).build();
        return server.submitClientRequestAsync(request).get(5,TimeUnit.SECONDS);
    }
}
