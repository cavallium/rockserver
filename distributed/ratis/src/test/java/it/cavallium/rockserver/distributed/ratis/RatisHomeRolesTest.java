package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native HOME bindings with controlled Raft interfaces; no network or production server factory. */
class RatisHomeRolesTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID session=new UUID(90,1),admin=new UUID(90,2);
    RatisNodePlan plan(RatisNodeRuntimeTest.Env env) {
            var homes=new ArrayList<RatisNodePlan.Home>(); int n=0;
            for (var role:List.of(RatisNodePlan.Role.TRANSACTION_HOME,RatisNodePlan.Role.SNAPSHOT_HOME)) {
                int id=++n; UUID group=new UUID(91,id),column=new UUID(92,id); byte[] digest=new byte[32]; digest[0]=(byte)id;
                var local=env.plan.directory().ranges().getFirst().replicas().stream().filter(replica -> replica.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();
                var replica=new ReplicaDescriptor(local.nodeId(),local.nodeIncarnation(),new UUID(96,id),local.peerId(),local.address(),ReplicaDescriptor.Role.VOTER);
                var route=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
                var directory=new RangeDirectory(env.outer.cluster,env.outer.database,new UUID(93,id),id,digest,0,Set.of(column),0,List.of(route));
                homes.add(new RatisNodePlan.Home(role,directory,column,"home-"+id));
            }
            return new RatisNodePlan(env.plan.nodeId(),env.plan.nodeIncarnation(),env.plan.directory(),env.plan.catalogue(),env.plan.columns(),env.plan.merges(),homes,env.plan.localGroups().size()+2);
    }
    final class Fixture implements AutoCloseable {
        final RatisNodeRuntimeTest.Env env;
        final RatisNodeRuntimeTest.FakeServer server=new RatisNodeRuntimeTest.FakeServer();
        final RatisNodePlan plan;
        final Map<RatisNodePlan.Role,RatisRoutedClient> clients=new EnumMap<>(RatisNodePlan.Role.class);
        final Map<UUID,Long> indices=new HashMap<>();
        final RatisNodeRuntime node;
        final RatisRoutedClient data;
        Fixture(String name) throws Exception { this(name,false); }
        Fixture(String name,boolean catalogue) throws Exception {
            env=new RatisNodeRuntimeTest.Env(Files.createDirectory(root.resolve(name)),catalogue);
            plan=plan(env); data=client(plan.directory());
            node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,data,env.scheduler,env.timer,() -> 20L,new RatisNodeRuntime.Settings(new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.Limits(4*1024*1024,16*1024*1024,2),env.settings().retention(),env.settings().clockPeriod(),1,4,env.settings().shutdownTimeout()),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server::create,null,null,null,this::client);
            try {
            node.start(); var before=env.catalogue.epochs.read(); var live=before.issue(session,100); env.catalogue.epochs.execute(env.catalogue.epochs.prepare(env.outer.operation(1),before,live));
            for (var home:plan.homes()) {
                var group=home.directory().ranges().getFirst().groupId(); var binding=env.catalogue.epochs.prepareSessionBinding(home.directory(),group,new OperationId(admin,1,home.role().ordinal()),live.grants().getFirst(),32,-1);
                append(group,DistributedCommandCodec.encode(binding));
            }
            } catch (Exception | Error failure) { try { node.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); } try { env.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); } throw failure; }
        }
        RatisRoutedClient client(RangeDirectory directory) {
                var client=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                    public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return ScopedTransactionRpc.pointValue(transaction(route,ScopedTransactionRpc.pointRequest(column,key),true),2*1024*1024); }
                    public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException {
                        try { return (fresh ? machine(route.groupId()).queryLinearizable(Message.valueOf(ByteString.copyFrom(bytes))) : machine(route.groupId()).query(Message.valueOf(ByteString.copyFrom(bytes)))).get(5,TimeUnit.SECONDS).getContent().toByteArray(); } catch (Exception failure) { throw new IOException(failure); }
                    }
                    public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return CommandResult.decode(append(route.groupId(),DistributedCommandCodec.encode(command))); }
                    public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return append(route.groupId(),bytes); }
                }); clients.put(plan.role(directory.ranges().getFirst().groupId()),client); return client;
        }
        RatisRangeStateMachine machine(UUID group) { return server.machines.get(RaftGroupId.valueOf(group)); }
        byte[] append(UUID group,byte[] bytes) throws IOException {
            try {
                var machine=machine(group); var transaction=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(group)).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build());
                if (transaction.getException()!=null) throw new IOException("Admission rejected",transaction.getException()); machine.preAppendTransaction(transaction); transaction.initLogEntry(1,indices.merge(group,1L,Long::sum));
                return machine.applyTransaction(transaction).get(5,TimeUnit.SECONDS).getContent().toByteArray();
            } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
        }
        TransactionStatus transaction() {
            var data=plan.directory().ranges().getFirst();
            return TransactionStatus.begin(env.outer.cluster,env.outer.database,plan.directory().domainId(),new UUID(94,1),new OperationId(new UUID(94,2),1,0),1,plan.directory().schemaDigest(),new byte[32],List.of(new TransactionStatus.Participant(data.rangeId(),data.groupId(),data.rangeEpoch(),plan.directory().schemaDigest(),new byte[32],null,false)));
        }
        SnapshotFence fence() {
            var data=plan.directory().ranges().getFirst(); var home=plan.home(RatisNodePlan.Role.SNAPSHOT_HOME).directory().ranges().getFirst();
            return new SnapshotFence(env.outer.cluster,env.outer.database,plan.directory().domainId(),new UUID(95,1),home.groupId(),home.rangeEpoch(),plan.directory().generation(),plan.directory().schemaEpoch(),plan.directory().schemaDigest(),List.of(new SnapshotFence.Participant(data.rangeId(),data.groupId(),data.rangeEpoch(),plan.directory().schemaDigest(),new OperationId(new UUID(95,2),1,0),new OperationId(new UUID(95,2),1,1),new TransactionScope(List.of(TransactionScope.Span.point(env.outer.data,new byte[]{1}))))));
        }
        @Override public void close() throws Exception { node.close(); data.close(); env.close(); }
    }
    @Test void sameDatabaseWrongHomeDirectoriesFailBeforeBootstrapOrClientAllocation() throws Exception {
        try (var env=new RatisNodeRuntimeTest.Env(Files.createDirectory(root.resolve("wrong-home"))); var reader=new RatisNodeRuntimeTest.TransactionHomeReader(env,env.outer.cluster)) {
            var plan=plan(env); var fake=new RatisNodeRuntimeTest.FakeServer();
            var snapshot=new RatisSnapshotFences(reader.routed,reader.routed.directory().columns().iterator().next());
            assertThrows(IllegalArgumentException.class,() -> RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,env.routed,env.scheduler,env.timer,() -> 20L,env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,null,snapshot,null,directory -> { throw new AssertionError("Rejected before client allocation"); }));
            var recovery=new RatisNodeRuntime.RecoverySettings(reader.homes,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024),java.time.Duration.ofMillis(100),1);
            assertThrows(IllegalArgumentException.class,() -> RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,env.routed,env.scheduler,env.timer,() -> 20L,env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,null,null,recovery,directory -> { throw new AssertionError("Rejected before client allocation"); }));
            assertFalse(fake.created); try (var files=Files.list(env.nodeRoot)) { assertTrue(files.findAny().isEmpty()); }
        }
    }
    @Test void dedicatedHomesAcceptOnlyTheirCanonicalCasAndKeepNativeBindingsSeparate() throws Exception {
        try (var f=new Fixture("hosting")) {
            var txHome=f.plan.home(RatisNodePlan.Role.TRANSACTION_HOME); var tx=new RatisTransactionStatuses(f.clients.get(txHome.role()),txHome.column()); var initial=f.transaction();
            assertEquals(CommandResult.Status.SUCCESS,tx.execute(tx.prepare(new OperationId(session,1,0),null,initial)).status()); assertEquals(initial,tx.read(initial.transactionId()));
            var snapHome=f.plan.home(RatisNodePlan.Role.SNAPSHOT_HOME); var snaps=new RatisSnapshotFences(f.clients.get(snapHome.role()),snapHome.column()); var fence=SnapshotFenceStatus.begin(f.fence());
            assertEquals(CommandResult.Status.SUCCESS,snaps.execute(snaps.prepare(new OperationId(session,1,0),null,fence)).status()); assertEquals(fence,snaps.read(fence.fence().fenceId()));
            for (var home:f.plan.homes()) {
                var group=home.directory().ranges().getFirst().groupId(); var machine=f.machine(group);
                assertNull(machine.preparedRecoveries(null,1));
                assertThrows(ExecutionException.class,() -> machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_OWNER,new byte[0])))).get(5,TimeUnit.SECONDS));
                assertThrows(IllegalStateException.class,() -> machine.reserveProposalScope(new TransactionScope(List.of(TransactionScope.Span.point(home.column(),new byte[]{1})))));
                var bad=new DistributedCommand(group,1,home.directory().schemaDigest(),new OperationId(session,1,1),List.of(new Mutation(Mutation.Kind.PUT,home.column(),new byte[]{1},new byte[]{1})));
                assertThrows(IOException.class,() -> f.append(group,DistributedCommandCodec.encode(bad)));
            }
            for (var home:f.plan.homes()) { var machine=f.machine(home.directory().ranges().getFirst().groupId()); machine.takeSnapshot(); machine.pause(); machine.reinitialize(); }
            assertEquals(initial,tx.read(initial.transactionId())); assertEquals(fence,snaps.read(fence.fence().fenceId()));
            assertEquals(4,f.server.machines.size()); assertEquals(3,f.clients.size());
        }
    }
    @Test void declaredTransactionHomeAutomaticallyRecoversDecidedParticipantsAndWaitsForUndecidedHomes() throws Exception {
        try (var f=new Fixture("default-recovery")) {
            var route=f.plan.directory().ranges().getFirst(); var machine=f.machine(route.groupId());
            var field=RatisRangeStateMachine.class.getDeclaredField("store"); field.setAccessible(true); var store=(ReplicatedRangeStore)field.get(machine);
            var grants=f.env.catalogue.epochs.read();
            var binding=f.env.catalogue.epochs.prepareSessionBinding(f.plan.directory(),route.groupId(),new OperationId(admin,1,20),grants.grants().getFirst(),32,-1);
            f.append(route.groupId(),DistributedCommandCodec.encode(binding));
            var txPlan=f.plan.home(RatisNodePlan.Role.TRANSACTION_HOME); var homes=new RatisTransactionStatuses(f.clients.get(txPlan.role()),txPlan.column());
            var business=new DistributedCommand(route.groupId(),route.rangeEpoch(),f.plan.directory().schemaDigest(),new OperationId(session,1,0),List.of(new Mutation(Mutation.Kind.PUT,f.env.outer.data,new byte[]{1},new byte[]{9})));
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(f.env.outer.data,new byte[]{1})));
            TransactionStatus initial; ParticipantVote vote;
            try (var pin=store.captureTracked(scope)) {
                var accepted=machine.participantIntent(pin,business);
                var participant=new TransactionStatus.Participant(route.rangeId(),route.groupId(),route.rangeEpoch(),f.plan.directory().schemaDigest(),accepted.payloadDigest(),null,false);
                initial=TransactionStatus.begin(f.env.outer.cluster,f.env.outer.database,f.plan.directory().domainId(),new UUID(97,1),business.operationId(),f.plan.directory().schemaEpoch(),f.plan.directory().schemaDigest(),accepted.payloadDigest(),List.of(participant));
                homes.execute(homes.prepare(new OperationId(session,1,3),null,initial));
                try (var proposal=machine.prepareParticipant(pin,business,new OperationId(session,1,1),homes,initial.transactionId())) { vote=f.data.executeParticipant(f.data.prepareParticipant(proposal.preparation())); }
            }
            assertEquals(initial.transactionId(),machine.preparedRecoveries(null,1).records().getFirst().preparation().transactionId());
            var serviceField=RatisNodeRuntime.class.getDeclaredField("participantRecoveryService"); serviceField.setAccessible(true); var background=(RatisTransactionRecoveryService)serviceField.get(f.node);
            assertEquals(2,f.env.timer.activePeriods()); assertEquals(0,f.node.participantRecoveryStatus().persistedFiles());
            for (int n=0;n<4;n++) { f.env.timer.fire(); background.tick().get(5,TimeUnit.SECONDS); RatisTransactionRecoveryServiceTest.idle(f.env.scheduler); }
            assertNull(f.node.participantRecoveryFailure()); assertEquals(TransactionStatus.Phase.PREPARING,homes.read(initial.transactionId()).phase()); assertNull(store.finalizedParticipant(initial.transactionId())); assertEquals(1,f.node.participantRecoveryStatus().persistedFiles());
            var ready=initial.prepared(route.groupId(),initial.participants().getFirst().planDigest(),vote.position()); homes.execute(homes.prepare(new OperationId(session,1,4),initial,ready));
            var commit=ready.decide(TransactionStatus.Phase.COMMIT); homes.execute(homes.prepare(new OperationId(session,1,5),ready,commit));
            // Expire the real catalogue grant, then replicate its verified authority cut to both ranges.
            var live=f.env.catalogue.epochs.read(); var expired=live.advanceTime(100);
            f.env.catalogue.epochs.execute(f.env.catalogue.epochs.prepare(f.env.outer.operation(2),live,expired)); assertEquals(2,expired.minimumEpoch());
            for (var directory:List.of(f.plan.directory(),txPlan.directory())) {
                var target=directory.ranges().getFirst();
                var advance=f.env.catalogue.epochs.prepareAuthorityAdvance(directory,target.groupId(),new OperationId(admin,2,80),-1);
                f.append(target.groupId(),DistributedCommandCodec.encode(advance));
                try (var pin=((ReplicatedRangeStore)field.get(f.machine(target.groupId()))).capture()) { assertEquals(2,pin.stateView().sessionAuthority().minimumEpoch()); }
            }
            for (int n=0;n<16 && !homes.read(initial.transactionId()).fullyFinalized();n++) { f.env.timer.fire(); background.tick().get(5,TimeUnit.SECONDS); RatisTransactionRecoveryServiceTest.idle(f.env.scheduler); }
            var completed=homes.read(initial.transactionId()); assertTrue(completed.fullyFinalized()); assertFalse(completed.consumed()); assertNull(f.node.participantRecoveryFailure()); assertArrayEquals(new byte[]{9},f.data.read(f.env.outer.data,new byte[]{1}));
            var outcome=store.finalizedParticipant(initial.transactionId()); assertNotNull(outcome); assertEquals(0,outcome.finalization().operation().epoch()); assertEquals(0,outcome.finalization().operation().sequence());
        }
    }

    @Test void oneRuntimeClockPropagatesAcrossDataAndBothHomeDomainsWithinTheSharedBudget() throws Exception {
        try (var f=new Fixture("clock-domains",true)) {
            var serviceField=RatisNodeRuntime.class.getDeclaredField("service"); serviceField.setAccessible(true); var clock=(RatisSessionClockService)serviceField.get(f.node);
            for (int n=0;n<24;n++) { long before=f.indices.values().stream().mapToLong(Long::longValue).sum(); clock.tick().get(5,TimeUnit.SECONDS); TimeUnit.MILLISECONDS.sleep(1); assertTrue(f.indices.values().stream().mapToLong(Long::longValue).sum()-before<=1); }
            assertNull(f.node.maintenanceFailure()); var status=f.node.maintenanceStatus(); assertNotNull(status); assertEquals(3,status.scopes().size()); assertTrue(status.complete());
            assertTrue(status.scopes().stream().allMatch(scope -> scope.target()!=null));
            for (var home:f.plan.homes()) {
                var machine=f.machine(home.directory().ranges().getFirst().groupId());
                var field=RatisRangeStateMachine.class.getDeclaredField("store"); field.setAccessible(true); var store=(ReplicatedRangeStore)field.get(machine);
                try (var pin=store.capture()) { assertEquals(20,pin.stateView().sessionAuthority().logicalTimeMillis()); }
            }
        }
    }
    @Test void homeRejectsParticipantAndGateRecordsAtAdmissionAndCommittedApply() throws Exception {
        try (var f=new Fixture("capabilities")) {
            for (var home:f.plan.homes()) {
                var group=home.directory().ranges().getFirst().groupId(); var machine=f.machine(group);
                var finalization=new ParticipantFinalization(group,1,f.plan.directory().ranges().getFirst().groupId(),new OperationId(admin,2,0),f.transaction().decide(TransactionStatus.Phase.ABORT));
                byte[] encoded=RatisParticipantRecord.encode(finalization); assertThrows(IOException.class,() -> f.append(group,encoded));
                var gate=RatisSnapshotRecord.install(f.fence(),f.plan.directory().ranges().getFirst().groupId()); assertThrows(IOException.class,() -> f.append(group,gate.encode()));
                var before=machine.getLastAppliedTermIndex();
                var committed=TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(LogEntryProto.newBuilder().setTerm(1).setIndex(before.getIndex()+1).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(encoded))).build()).build();
                assertThrows(ExecutionException.class,() -> machine.applyTransaction(committed).get(5,TimeUnit.SECONDS)); assertEquals(before,machine.getLastAppliedTermIndex());
            }
        }
    }
}
