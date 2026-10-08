package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Native node lifecycle with controlled Ratis interfaces. Production server factory is compiled, never executed here. */
class RatisNodeRuntimeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void explicitPlacementStartsAllLocalGroupsAndRecoveryKeepsBorrowedResourcesOpen() throws Exception {
        try (var env=new Env(root)) {
            var first=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,first)) {
                assertFalse(node.started()); node.start(); assertTrue(node.started()); assertEquals(2,first.storages.size());
                assertFalse(env.timer.cancelled); assertNull(node.maintenanceStatus());
                assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer()));
            }
            assertEquals(1,first.closes); assertTrue(first.allFencedAtClose); assertFalse(env.timer.cancelled); assertNull(env.timer.periodic);
            assertEquals(0,env.catalogue.closes.get()); assertEquals(0,env.transportCloses);
            try (var recovered=env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer())) { recovered.start(); assertTrue(recovered.started()); }
            assertNotNull(env.catalogue.epochs.read()); assertFalse(env.timer.isDisposed());
        }
    }
    @Test void unknownGroupsMissingIdentityAndWipedNativeStateFailBeforeServerConstruction() throws Exception {
        try (var env=new Env(root)) {
            assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer()));
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new FakeServer())) { node.start(); }
            Path unknown=Files.createDirectory(env.nodeRoot.resolve("raft").resolve(UUID.randomUUID().toString())); var fake=new FakeServer();
            assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,fake)); assertFalse(fake.created); Files.delete(unknown);
            Path identity=env.nodeRoot.resolve("node.identity"); byte[] saved=Files.readAllBytes(identity); Files.delete(identity);
            assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer()));
            assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new FakeServer()));
            Files.write(identity,saved); Files.delete(env.nodeRoot.resolve("native/CURRENT"));
            assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer()));
        }
    }
    @Test void equalPeerIdsCannotHideEndpointMismatchOrJointConfigurationAndPartialStartCleansUp() throws Exception {
        try (var env=new Env(root)) {
            var wrong=new FakeServer(); wrong.wrongEndpoint=true;
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,wrong)) {
                assertThrows(IOException.class,node::start); assertFalse(node.started());
            }
            assertTrue(wrong.allFencedAtClose);
            var joint=new FakeServer(); joint.joint=true;
            try (var node=env.open(RatisNodeRuntime.Startup.RECOVER,joint)) { assertThrows(IOException.class,node::start); }
            var add=new FakeServer(); add.rejectAdd=true;
            try (var node=env.open(RatisNodeRuntime.Startup.RECOVER,add)) { assertThrows(IOException.class,node::start); }
            assertEquals(1,add.closes); assertTrue(add.allFencedAtClose);
        }
    }
    @Test void heldNativePinRetainsPoolAndNodeOwnershipUntilRetryableCloseCompletes() throws Exception {
        try (var env=new Env(root)) {
            var fake=new FakeServer(); var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake); node.start();
            var field=RatisRangeStateMachine.class.getDeclaredField("store"); field.setAccessible(true);
            var store=(ReplicatedRangeStore)field.get(fake.machines.values().iterator().next());
            try (var pin=store.capture()) {
                assertThrows(IOException.class,node::close); assertFalse(node.started()); assertEquals(RatisNodeRuntime.State.STOPPING,node.state()); assertEquals(1,fake.closes);
                assertNull(pin.get(env.outer.data,new byte[]{1}),"Native handles stay valid while the old reader owns them");
                assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer()));
            }
            node.close(); node.close(); assertEquals(RatisNodeRuntime.State.CLOSED,node.state()); assertEquals(1,fake.closes);
            try (var recovered=env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer())) { recovered.start(); }
        }
    }
    @Test void changedLocalEpochIntervalOrReplicaCannotAdoptOldNativeStateUnderSameGroup() throws Exception {
        try (var env=new Env(root)) {
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new FakeServer())) { node.start(); }
            var directory=env.plan.directory(); var a=directory.ranges().getFirst(); var b=directory.ranges().getLast();
            var changedReplica=a.replicas().stream().map(replica -> replica.nodeId().equals(env.plan.nodeId())
                    ? new ReplicaDescriptor(replica.nodeId(),replica.nodeIncarnation(),UUID.randomUUID(),replica.peerId(),replica.address(),replica.role()) : replica).toList();
            var variants=List.of(
                    List.of(new RangeDescriptor(a.rangeId(),a.groupId(),2,0,a.lowerInclusive(),a.upperExclusive(),a.topologyState(),a.replicas()),b),
                    List.of(new RangeDescriptor(a.rangeId(),a.groupId(),1,0,null,new byte[]{100},a.topologyState(),a.replicas()),
                            new RangeDescriptor(b.rangeId(),b.groupId(),1,0,new byte[]{100},null,b.topologyState(),b.replicas())),
                    List.of(new RangeDescriptor(a.rangeId(),a.groupId(),1,0,a.lowerInclusive(),a.upperExclusive(),a.topologyState(),changedReplica),b));
            for (var ranges:variants) {
                var changed=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),2,directory.columns(),directory.routingPrefixBytes(),ranges);
                var plan=new RatisNodePlan(env.plan.nodeId(),env.plan.nodeIncarnation(),changed,env.plan.catalogue(),env.plan.columns(),env.plan.merges(),2);
                try (var routed=new RatisRoutedClient(changed,new RatisRoutedClient.Transport() {
                    public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                    public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError(); }
                })) {
                    var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),RatisRangeStateMachine.RetentionLimits.defaults(),Duration.ofSeconds(1),1,2,Duration.ofMillis(100));
                    var fake=new FakeServer();
                    assertThrows(IOException.class,() -> RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,routed,env.scheduler,env.timer,() -> 20L,settings,RatisNodeRuntime.Startup.RECOVER,fake::create));
                    assertFalse(fake.created);
                }
            }
        }
    }
    @Test void establishedGroupsRecoverWithoutCatalogueQuorumAndExposeMaintenanceFailure() throws Exception {
        try (var env=new Env(root,true)) {
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new FakeServer())) { node.start(); }
            env.catalogue.unavailable=true;
            try (var node=env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer())) {
                node.start(); assertTrue(node.started()); env.timer.fire();
                var settled=new java.util.concurrent.CompletableFuture<Void>();
                env.scheduler.executor(it.cavallium.rockserver.core.common.RequestContext.batch(),it.cavallium.rockserver.core.common.OperationFamily.MUTATION).execute(() -> settled.complete(null));
                settled.get(5,java.util.concurrent.TimeUnit.SECONDS);
                assertNotNull(node.maintenanceFailure()); assertNull(node.maintenanceStatus()); assertTrue(node.started());
            } finally { env.catalogue.unavailable=false; }
        }
    }
    @Test void onlyReadyCurrentNativeTermCatalogueLeaderMayDispatchClockWork() throws Exception {
        try (var env=new Env(root,true)) {
            var fake=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake)) {
                node.start(); env.catalogue.unavailable=true;
                fake.leader=false; env.timer.fire();
                fake.leader=true; fake.ready=false; env.timer.fire();
                fake.ready=true; fake.term=2; env.timer.fire();
                assertEquals(0,env.scheduler.queuedTasks(it.cavallium.rockserver.core.common.WorkloadProfile.BATCH));
                assertEquals(0,env.scheduler.activeTasks(it.cavallium.rockserver.core.common.WorkloadProfile.BATCH));
                assertNull(node.maintenanceFailure()); assertNull(node.maintenanceStatus());
                fake.term=1; env.timer.fire();
                var settled=new java.util.concurrent.CompletableFuture<Void>();
                env.scheduler.executor(it.cavallium.rockserver.core.common.RequestContext.batch(),it.cavallium.rockserver.core.common.OperationFamily.MUTATION).execute(() -> settled.complete(null));
                settled.get(5,java.util.concurrent.TimeUnit.SECONDS); assertNotNull(node.maintenanceFailure());
            } finally { env.catalogue.unavailable=false; }
        }
    }
    static final class Env implements AutoCloseable {
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest(); final RatisSessionEpochsTest.Fixture catalogue;
        final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,4,1,"node-runtime-test");
        final RatisSessionClockServiceTest.ManualTimer timer=new RatisSessionClockServiceTest.ManualTimer();
        final Path nodeRoot; final RatisNodePlan plan; final RatisRoutedClient routed; int transportCloses;
        Env(Path root) throws Exception { this(root,false); }
        Env(Path root,boolean localCatalogue) throws Exception {
            catalogue=outer.new Fixture(root.resolve("catalogue")); var ledger=SessionEpochLedger.initial(outer.cluster,outer.database,10);
            catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(0),null,ledger));
            UUID local=new UUID(9,1), incarnation=new UUID(9,2); var ranges=new ArrayList<RangeDescriptor>();
            for (int i=0;i<2;i++) {
                var replica=new ReplicaDescriptor(local,incarnation,new UUID(9,i+3),"node9","127.0.0.1:4512",i==0 ? ReplicaDescriptor.Role.VOTER : ReplicaDescriptor.Role.LISTENER);
                // A listener group also needs a remote voter.
                var remote=new ReplicaDescriptor(new UUID(8,1),new UUID(8,2),new UUID(8,i+3),"node8","127.0.0.1:4511",ReplicaDescriptor.Role.VOTER);
                ranges.add(new RangeDescriptor(new UUID(6,i+2),new UUID(3,i+1),1,0,i==0 ? null : new byte[]{(byte)128},i==0 ? new byte[]{(byte)128} : null,
                        RangeDescriptor.TopologyState.ACTIVE,List.of(replica,remote)));
            }
            var directory=new RangeDirectory(outer.cluster,outer.database,new UUID(6,1),1,new byte[32],1,Set.of(outer.data),0,ranges);
            var remote=new ReplicaDescriptor(new UUID(8,1),new UUID(8,2),new UUID(8,8),"node8","127.0.0.1:4511",ReplicaDescriptor.Role.VOTER);
            plan=new RatisNodePlan(local,incarnation,directory,new RatisNodePlan.Catalogue(outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,localCatalogue ? List.of(remote,new ReplicaDescriptor(local,incarnation,new UUID(9,8),"node9","127.0.0.1:4512",ReplicaDescriptor.Role.VOTER)) : List.of(remote)),Map.of(outer.data,"data"),Set.of(),localCatalogue ? 3 : 2);
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError("Timer has not fired"); }
                public void close() { transportCloses++; }
            });
            nodeRoot=Files.createDirectory(root.resolve("node"));
        }
        RatisNodeRuntime open(RatisNodeRuntime.Startup startup,FakeServer fake) throws IOException {
            var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),RatisRangeStateMachine.RetentionLimits.defaults(),
                    Duration.ofSeconds(1),1,2,Duration.ofMillis(100));
            return RatisNodeRuntime.open(plan,nodeRoot,ConfigParser.parseDefault(),catalogue.epochs,routed,scheduler,timer,() -> 20L,settings,startup,fake::create);
        }
        public void close() throws Exception { routed.close(); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); catalogue.close(); }
    }
    static final class FakeServer {
        boolean created,wrongEndpoint,joint,rejectAdd,allFencedAtClose; int closes;
        volatile boolean leader=true,ready=true; volatile long term=1;
        RatisNodePlan plan; Path logs; Map<RaftGroupId,RatisRangeStateMachine> machines; RaftServer server;
        final Map<RaftGroupId,RaftStorage> storages=new LinkedHashMap<>();
        RaftServer create(RatisNodePlan plan,Path logs,Map<RaftGroupId,RatisRangeStateMachine> machines) {
            created=true; this.plan=plan; this.logs=logs; this.machines=machines;
            server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> switch (method.getName()) {
                case "getId" -> RaftPeerId.valueOf(plan.peerId()); case "toString" -> "controlled-node-server";
                case "start" -> { initialize(plan.localGroups().getFirst()); yield null; }
                case "getGroupIds" -> List.copyOf(storages.keySet());
                case "getDivision" -> division((RaftGroupId)args[0]);
                case "groupManagement" -> {
                    var request=(GroupManagementRequest)args[0]; if (!rejectAdd) initialize(request.getAdd().getGroup());
                    yield new RaftClientReply.Builder().setClientId(request.getClientId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(request.getRaftGroupId())
                            .setCallId(request.getCallId()).setSuccess(!rejectAdd).build();
                }
                case "close" -> {
                    closes++; allFencedAtClose=machines.values().stream().allMatch(machine -> machine.query(Message.EMPTY).isCompletedExceptionally());
                    for (var storage:storages.values()) storage.close(); yield null;
                }
                default -> throw new AssertionError(method);
            }); return server;
        }
        void initialize(RaftGroup group) throws Exception {
            var storage=RaftStorage.newBuilder().setDirectory(logs.resolve(group.getGroupId().getUuid().toString()).toFile()).setOption(RaftStorage.StartupOption.RECOVER)
                    .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                    .setLogCorruptionPolicy(RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build(); storage.initialize(); storages.put(group.getGroupId(),storage);
            var machine=machines.get(group.getGroupId()); machine.initialize(server,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
        }
        RaftServer.Division division(RaftGroupId id) {
            var group=plan.localGroups().stream().filter(candidate -> candidate.getGroupId().equals(id)).findFirst().orElseThrow();
            var configuration=(RaftConfiguration)Proxy.newProxyInstance(RaftConfiguration.class.getClassLoader(),new Class<?>[]{RaftConfiguration.class},(proxy,method,args) -> {
                var role=(RaftPeerRole)args[0]; var peers=group.getPeers().stream().filter(peer -> peer.getStartupRole()==role).toList();
                if (method.getName().equals("getPreviousPeers")) return joint ? peers : List.of();
                if (method.getName().equals("getCurrentPeers")) return peers.stream().map(peer -> wrongEndpoint ? RaftPeer.newBuilder().setId(peer.getId()).setAddress("127.0.0.1:9999").setStartupRole(peer.getStartupRole()).build() : peer).toList();
                throw new AssertionError(method);
            });
            return (RaftServer.Division)Proxy.newProxyInstance(RaftServer.Division.class.getClassLoader(),new Class<?>[]{RaftServer.Division.class},(proxy,method,args) -> switch (method.getName()) {
                case "getRaftConf" -> configuration; case "getStateMachine" -> machines.get(id);
                case "getInfo" -> Proxy.newProxyInstance(DivisionInfo.class.getClassLoader(),new Class<?>[]{DivisionInfo.class},(p,m,a) -> switch (m.getName()) {
                    case "isLeader" -> leader; case "isLeaderReady" -> ready; case "getCurrentTerm" -> term; default -> throw new AssertionError(m);
                }); default -> throw new AssertionError(method);
            });
        }
    }
}
