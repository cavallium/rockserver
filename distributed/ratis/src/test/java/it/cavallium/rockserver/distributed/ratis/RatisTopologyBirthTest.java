package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Local native handoff and real adapter bootstrap; group registration/network consensus remain separate. */
class RatisTopologyBirthTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    Message stateRequest() { return Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]))); }
    RangeStateView state(RatisRangeStateMachine machine) throws Exception {
        var packet=ScopedTransactionRpc.parse(machine.query(stateRequest()).get(5,TimeUnit.SECONDS).getContent().toByteArray(),ScopedTransactionRpc.MAX_REPLY_BYTES);
        assertEquals(ScopedTransactionRpc.STATE,packet.kind()); return RangeStateView.decode(packet.body());
    }
    @Test void seededChildBootstrapsItsOwnRaftPrefixWhileRetiredParentServesMetadata() throws Exception {
        try(var parent=new RatisTopologyFreezeTest.Fixture(root.resolve("parent"))) {
            var command=parent.put(0,9); parent.apply(parent.propose(DistributedCommandCodec.encode(command)),1).get(5,TimeUnit.SECONDS);
            assertTrue(parent.prepare().ready()); var freeze=parent.freeze(2); parent.machine.topologyCleanup().get(5,TimeUnit.SECONDS);
            var authority=TopologyChange.State.begin(parent.definition.header()).beginFencing().frozen(freeze.proof(parent.local.replicaId()));
            var seed=parent.pool.seedTopology(parent.definition,authority,List.of(parent.store),parent.local);
            assertEquals(RangeOwnership.State.RETIRED,parent.store.ownership().state());
            assertEquals(new ApplyPosition(1,2),state(parent.machine).applied()); assertEquals(-1,parent.machine.takeSnapshot());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(parent.store.operationResult(command.operationId(),command.receiptDigest())).status());
            parent.machine.notifyTermIndexUpdated(2,3); assertEquals(new ApplyPosition(2,3),state(parent.machine).applied());
            var target=parent.definition.targets().getFirst(); var birth=seed.birth(target.groupId());
            var child=parent.pool.reopenTopologyRange(birth,parent.schema); assertNull(child.applied()); assertNull(child.configuration()); assertTrue(child.topologyBirthPending());
            var group=RaftGroup.valueOf(RaftGroupId.valueOf(target.groupId()),parent.peer);
            var storage=RaftStorage.newBuilder().setDirectory(root.resolve("child-raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build(); storage.initialize();
            var borrowed=(org.apache.ratis.client.RaftClient)Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args)->{ if(method.getName().equals("getGroupId"))return RaftGroupId.valueOf(RatisTopologyFreezeTest.id(20)); throw new AssertionError("Unexpected catalogue I/O "+method); });
            var epochs=new RatisSessionEpochs(borrowed,parent.schema.clusterId(),parent.schema.databaseId(),RatisTopologyFreezeTest.id(20),1,new byte[32],RatisTopologyFreezeTest.id(21));
            var machine=RatisRangeStateMachine.withSessions(child,group,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),commandValidator->{},epochs,seed.definition().targetDirectory(),RatisRangeStateMachine.RetentionLimits.defaults(),()->new RatisRangeStateMachine.Leadership(1,true));
            machine.role(RatisNodePlan.Role.DATA);
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->{ if(method.getName().equals("getId"))return parent.peer.getId(); if(method.getName().equals("toString"))return "child-birth-fixture"; throw new AssertionError(method); });
            try {
                machine.initialize(identity,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
                assertNotNull(child.configuration()); assertEquals(new ApplyPosition(1,0),child.applied()); assertEquals(new ApplyPosition(0,0),birth.proof(seed).position());
                assertEquals(RangeOwnership.State.FENCED,child.ownership().state()); assertFalse(child.admissionOpen()); assertEquals(-1,machine.takeSnapshot());
                assertEquals(new ApplyPosition(1,0),state(machine).applied()); assertThrows(IllegalStateException.class,child::activate);
                var staleParent=new DistributedCommand(parent.groupId,1,parent.schema.digest(),new OperationId(RatisTopologyFreezeTest.id(30),1,1),List.of(new Mutation(Mutation.Kind.PUT,parent.column,new byte[]{1},new byte[]{8})));
                assertNotNull(parent.propose(DistributedCommandCodec.encode(staleParent)).getException());
            } finally { machine.close(); machine.awaitNativeReaders(Duration.ofSeconds(5)); storage.close(); }
            parent.machine.close(); parent.machine.awaitNativeReaders(Duration.ofSeconds(5));
            var retiredStorage=RaftStorage.newBuilder().setDirectory(root.resolve("retired-raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build(); retiredStorage.initialize();
            var retired=RatisRangeStateMachine.withSessions(parent.store,parent.group,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),commandValidator->{},epochs,parent.directory,RatisRangeStateMachine.RetentionLimits.defaults(),()->new RatisRangeStateMachine.Leadership(3,true));
            retired.role(RatisNodePlan.Role.DATA);
            try {
                retired.initialize(identity,parent.group.getGroupId(),retiredStorage); retired.notifyTermIndexUpdated(3,4);
                assertEquals(RangeOwnership.State.RETIRED,parent.store.ownership().state()); assertEquals(new ApplyPosition(3,4),state(retired).applied()); assertEquals(-1,retired.takeSnapshot());
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(parent.store.operationResult(command.operationId(),command.receiptDigest())).status());
            } finally { retired.close(); retired.awaitNativeReaders(Duration.ofSeconds(5)); retiredStorage.close(); }
        }
    }
}
