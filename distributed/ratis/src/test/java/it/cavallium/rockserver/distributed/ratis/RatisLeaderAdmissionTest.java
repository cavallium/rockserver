package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled leadership and real native markers; no Raft transport or server process. */
class RatisLeaderAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void nativeStartupAndAppendGenerationGateAdmissionWithoutDrainingSteadyProposals() throws Exception {
        UUID groupId=UUID.randomUUID(), column=UUID.randomUUID(), session=UUID.randomUUID();
        var peer=RaftPeer.newBuilder().setId("fixture").build();
        var group=RaftGroup.valueOf(RaftGroupId.valueOf(groupId),peer);
        var leadership=new AtomicReference<>(new RatisRangeStateMachine.Leadership(2,true));
        var leader=new java.util.concurrent.atomic.AtomicBoolean(true);
        var info=(org.apache.ratis.server.DivisionInfo)Proxy.newProxyInstance(org.apache.ratis.server.DivisionInfo.class.getClassLoader(),
                new Class<?>[]{org.apache.ratis.server.DivisionInfo.class},(proxy,method,args) -> switch(method.getName()) {
                    case "getCurrentTerm" -> leadership.get().term();
                    case "isLeader" -> leader.get();
                    case "isLeaderReady" -> leadership.get().ready();
                    default -> throw new AssertionError("Unexpected division info access "+method);
                });
        var division=(RaftServer.Division)Proxy.newProxyInstance(RaftServer.Division.class.getClassLoader(),new Class<?>[]{RaftServer.Division.class},
                (proxy,method,args) -> {
                    if (method.getName().equals("getInfo")) return info;
                    throw new AssertionError("Unexpected division access "+method);
                });
        var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> {
            if (method.getName().equals("getId")) return peer.getId();
            if (method.getName().equals("getDivision")) { assertEquals(group.getGroupId(),args[0]); return division; }
            if (method.getName().equals("toString")) return "identity-fixture";
            throw new AssertionError("Unexpected server access "+method);
        });
        try (var pool=DistributedStoragePool.open(root.resolve("native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
                var storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                        .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                        .setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build()) {
            storage.initialize();
            var store=pool.openRange(new RangeOwnership.Owner(groupId,1),new RangeOwnership.Interval(groupId,Set.of(column),null,null),new byte[32],Map.of(column,"data"),Set.of());
            var machine=new RatisRangeStateMachine(store,group,new NativeRangeImage.Limits(2*1024*1024,16*1024*1024,1));
            try {
                machine.initialize(identity,group.getGroupId(),storage);
                var first=command(groupId,column,session,0);
                assertNotNull(machine.startTransaction(request(group,peer,first)).getException(),"Ready callback can precede native startup");
                machine.notifyTermIndexUpdated(1,0);
                assertNotNull(machine.startTransaction(request(group,peer,first)).getException(),"Old-term native apply does not admit new leader");
                machine.notifyTermIndexUpdated(2,1);
                var a=machine.startTransaction(request(group,peer,first));
                var b=machine.startTransaction(request(group,peer,command(groupId,column,session,1)));
                assertNull(a.getException()); assertNull(b.getException());
                assertSame(a,machine.preAppendTransaction(a)); assertSame(b,machine.preAppendTransaction(b));
                assertEquals(1,store.applied().index(),"Multiple proposals do not force a native drain");
                leadership.set(new RatisRangeStateMachine.Leadership(3,true));
                machine.notifyTermIndexUpdated(3,2); // replacement leader is also natively ready
                var stale=assertThrows(StateMachineException.class,() -> machine.preAppendTransaction(a));
                assertFalse(stale.leaderShouldStepDown()); assertNull(a.getStateMachineContext());
                machine.cancelTransaction(a); machine.notifyNotLeader(List.of(a,b));
                assertNull(b.getStateMachineContext());
                leadership.set(new RatisRangeStateMachine.Leadership(3,false));
                assertNotNull(machine.startTransaction(request(group,peer,first)).getException());
                leadership.set(new RatisRangeStateMachine.Leadership(3,true));
                leader.set(false);
                assertNotNull(machine.startTransaction(request(group,peer,first)).getException());
                leader.set(true);
                var current=machine.startTransaction(request(group,peer,first)); assertNull(current.getException());
                assertSame(current,machine.preAppendTransaction(current));
                machine.notifyNotLeader(List.of()); // same-term invalidation must still reject the old generation
                var invalidated=assertThrows(StateMachineException.class,() -> machine.preAppendTransaction(current));
                assertFalse(invalidated.leaderShouldStepDown()); assertNull(current.getStateMachineContext());
                machine.cancelTransaction(current);
                var retry=machine.startTransaction(request(group,peer,first)); assertNull(retry.getException());
                machine.cancelTransaction(retry);
                assertEquals(0,admissions(machine));
            } finally { machine.close(); }
        }
    }
    private static int admissions(RatisRangeStateMachine machine) throws Exception {
        var field=RatisRangeStateMachine.class.getDeclaredField("admissions"); field.setAccessible(true);
        return ((Map<?,?>)field.get(machine)).size();
    }
    private static DistributedCommand command(UUID group,UUID column,UUID session,long sequence) {
        return new DistributedCommand(group,1,new byte[32],new OperationId(session,1,sequence),
                List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)sequence},new byte[]{7})));
    }
    private static RaftClientRequest request(RaftGroup group,RaftPeer peer,DistributedCommand command) {
        return RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1)
                .setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build();
    }
}
