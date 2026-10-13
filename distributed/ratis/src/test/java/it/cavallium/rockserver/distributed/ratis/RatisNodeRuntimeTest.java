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
    public static final class AppendOperator extends it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator {
        public AppendOperator(String identity) { super(identity); }
        @Override public byte[] merge(byte[] key,byte[] existing,List<byte[]> operands) {
            var bytes=new java.io.ByteArrayOutputStream();
            if (existing!=null) bytes.writeBytes(existing);
            for (var operand:operands) bytes.writeBytes(operand);
            return bytes.toByteArray();
        }
    }
    private static byte[] appendArtifact() throws Exception {
        var bytes=new java.io.ByteArrayOutputStream();
        String entry=AppendOperator.class.getName().replace('.','/')+".class";
        try (var jar=new java.util.jar.JarOutputStream(bytes)) {
            jar.putNextEntry(new java.util.jar.JarEntry(entry));
            try (var input=AppendOperator.class.getClassLoader().getResourceAsStream(entry)) { assertNotNull(input); input.transferTo(jar); }
            jar.closeEntry();
        }
        return bytes.toByteArray();
    }
    @Test void qualifiedOperatorBootstrapAndReopenRetainExactArtifactAndMergeResults() throws Exception {
        byte[] artifact=appendArtifact();
        var operator=new DistributedSchema.Operator("append",1,AppendOperator.class.getName(),java.security.MessageDigest.getInstance("SHA-256").digest(artifact),1,1,"pure-append",1);
        var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true,"append",1L,AppendOperator.class.getName());
        UUID column=new UUID(2,3);
        var schema=new DistributedSchema(new UUID(1,1),new UUID(1,2),new UUID(6,1),1,0,List.of(new DistributedSchema.Column(1,column,"data",shape,operator)));
        try (var env=new Env(root,false,schema.digest(),Set.of(column))) {
            var unqualified=new FakeServer();
            assertThrows(IllegalArgumentException.class,() -> env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,unqualified));
            assertFalse(unqualified.created); assertFalse(Files.exists(env.nodeRoot.resolve("node.identity")));
            DistributedStoragePool.stageOperator(env.nodeRoot.resolve("native"),operator,new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("pure-append",1,true),artifact);
            var storeField=RatisRangeStateMachine.class.getDeclaredField("store"); storeField.setAccessible(true);
            var group=RaftGroupId.valueOf(env.plan.domains().getFirst().directory().ranges().getFirst().groupId());
            var command=new DistributedCommand(group.getUuid(),1,schema.digest(),new OperationId(new UUID(58,1),1,0),List.of(new Mutation(Mutation.Kind.MERGE,column,new byte[]{1},new byte[]{7})));
            var first=new FakeServer();
            try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, first::create, List.of(schema))) {
                node.start(); var store=(ReplicatedRangeStore)storeField.get(first.machines.get(group));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,1),command)).status());
                assertArrayEquals(new byte[]{7},store.readCurrentPoint(column,new byte[]{1}));
            }
            byte[] identity=Files.readAllBytes(env.nodeRoot.resolve("node.identity"));
            var recovered=new FakeServer();
            try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.RECOVER, recovered::create, List.of(schema))) {
                node.start(); var store=(ReplicatedRangeStore)storeField.get(recovered.machines.get(group));
                assertArrayEquals(new byte[]{7},store.readCurrentPoint(column,new byte[]{1}));
                var next=new DistributedCommand(group.getUuid(),1,schema.digest(),new OperationId(new UUID(58,1),1,1),List.of(new Mutation(Mutation.Kind.MERGE,column,new byte[]{1},new byte[]{9})));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(2,2),next)).status());
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(2,3),command)).status());
                assertArrayEquals(new byte[]{7,9},store.readCurrentPoint(column,new byte[]{1}));
            }
            assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
        }
    }
    @Test void qualifiedPoolHostsAllMetadataRolesThroughOperatorInstallAndOriginalManifestReopen() throws Exception {
        UUID column=new UUID(2,3),added=new UUID(2,4);
        var plain=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true);
        var source=new DistributedSchema(new UUID(1,1),new UUID(1,2),new UUID(6,1),1,0,List.of(new DistributedSchema.Column(1,column,"data",plain,null)));
        byte[] artifact=appendArtifact();
        var operator=new DistributedSchema.Operator("append",1,AppendOperator.class.getName(),java.security.MessageDigest.getInstance("SHA-256").digest(artifact),1,1,"pure-append",1);
        var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true,"append",1L,AppendOperator.class.getName());
        var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),2,0,List.of(source.column(column),new DistributedSchema.Column(2,added,"append",shape,operator)));
        try (var env=new Env(root,true,source.digest())) {
            var plan=new RatisHomeRolesTest().plan(env);
            var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,2),env.settings().retention(),env.settings().clockPeriod(),1,4,env.settings().shutdownTimeout(),env.settings().exports());
            RatisNodeRuntime.HomeClientFactory homes=directory -> new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID requested,byte[] key) { return null; }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError("Maintenance timer has not fired"); }
            });
            DistributedStoragePool.stageOperator(env.nodeRoot.resolve("native"),operator,new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("pure-append",1,true),artifact);
            var definition=new SchemaChange.Definition(new UUID(59,1),new OperationId(new UUID(59,2),1,0),plan.domains().getFirst().directory(),source,target);
            var poolField=RatisNodeRuntime.class.getDeclaredField("pool"); poolField.setAccessible(true);
            var storeField=RatisRangeStateMachine.class.getDeclaredField("store"); storeField.setAccessible(true);
            var first=new FakeServer(); NativeSchemaActivationProof activation=null;
            try (var node=RatisNodeRuntime.open(plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, settings, RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, first::create, List.of(source), null, null, homes)) {
                node.start(); assertEquals(5,first.machines.size()); var pool=(DistributedStoragePool)poolField.get(node);
                var data=new LinkedHashMap<UUID,ReplicatedRangeStore>();
                for (var entry:first.machines.entrySet()) {
                    var store=(ReplicatedRangeStore)storeField.get(entry.getValue());
                    if (plan.role(entry.getKey().getUuid())==RatisNodePlan.Role.DATA) data.put(entry.getKey().getUuid(),store);
                }
                for (var store:data.values()) store.applySchemaFence(new ApplyPosition(1,1),definition.header());
                var installations=new HashMap<UUID,NativeSchemaInstallProof>();
                for (var entry:data.entrySet()) installations.put(entry.getKey(),pool.installSchema(entry.getValue(),new ApplyPosition(1,2),definition,Map.of(column,"data",added,"append-data"),Set.of(added)).proof());
                for (var entry:data.entrySet()) {
                    var store=entry.getValue();
                    activation=store.applySchemaActivation(new ApplyPosition(1,3),installations.get(entry.getKey()),env.outer.catalogue,new ApplyPosition(1,20));
                    byte[] key=store.ownership().interval().lowerInclusive()==null ? new byte[]{1} : new byte[]{(byte)129};
                    var command=new DistributedCommand(entry.getKey(),1,target.digest(),new OperationId(new UUID(59,3),1,0),List.of(new Mutation(Mutation.Kind.MERGE,added,key,new byte[]{7})));
                    assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,4),command)).status());
                }
                env.routed.refreshSchema(definition,activation);
            }
            byte[] identity=Files.readAllBytes(env.nodeRoot.resolve("node.identity")); var recovered=new FakeServer();
            try (var node=RatisNodeRuntime.open(plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, settings, RatisNodeRuntime.Startup.RECOVER, recovered::create, List.of(source), null, null, homes)) {
                node.start(); assertEquals(5,recovered.machines.size());
                for (var entry:recovered.machines.entrySet()) {
                    var store=(ReplicatedRangeStore)storeField.get(entry.getValue());
                    switch (plan.role(entry.getKey().getUuid())) {
                        case DATA -> {
                            assertArrayEquals(target.digest(),store.schemaDigest());
                            byte[] key=store.ownership().interval().lowerInclusive()==null ? new byte[]{1} : new byte[]{(byte)129};
                            assertArrayEquals(new byte[]{7},store.readCurrentPoint(added,key));
                        }
                        case CATALOGUE -> assertEquals(Set.of(plan.catalogue().directoryColumn(),plan.catalogue().sessionsColumn()),store.ownership().interval().columns());
                        case TRANSACTION_HOME, SNAPSHOT_HOME -> assertEquals(Set.of(plan.home(entry.getKey().getUuid()).column()),store.ownership().interval().columns());
                    }
                }
            }
            assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
        }
    }
    @Test void activatedSchemasReopenFromOriginalManifestWithRefreshedBorrowedRouter() throws Exception {
        var source=new DistributedSchema(new UUID(1,1),new UUID(1,2),new UUID(6,1),1,0,List.of(
                new DistributedSchema.Column(1,new UUID(2,3),"data",it.cavallium.rockserver.core.common.ColumnSchema.of(
                        it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true),null)));
        try (var env=new Env(root,false,source.digest())) {
            var empty=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),2,0,List.of());
            UUID recreatedColumn=new UUID(2,4);
            var recreated=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),3,0,List.of(
                    new DistributedSchema.Column(2,recreatedColumn,"data",source.columns().getFirst().schema(),null)));
            var deletion=new SchemaChange.Definition(new UUID(56,1),new OperationId(new UUID(56,2),1,0),env.plan.domains().getFirst().directory(),source,empty);
            var creation=new SchemaChange.Definition(new UUID(56,3),new OperationId(new UUID(56,2),1,1),deletion.targetDirectory(),empty,recreated);
            var poolField=RatisNodeRuntime.class.getDeclaredField("pool"); poolField.setAccessible(true);
            var storeField=RatisRangeStateMachine.class.getDeclaredField("store"); storeField.setAccessible(true);
            var first=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,first)) {
                node.start(); var pool=(DistributedStoragePool)poolField.get(node);
                NativeSchemaActivationProof deleted=null,created=null;
                for (var machine:first.machines.values()) {
                    var store=(ReplicatedRangeStore)storeField.get(machine); store.enableLogicalSchema(source);
                    assertEquals(0,machine.takeSnapshot());
                    store.applySchemaFence(new ApplyPosition(1,1),deletion.header());
                    var installed=pool.installSchema(store,new ApplyPosition(1,2),deletion,Map.of(),Set.of()).proof();
                    deleted=store.applySchemaActivation(new ApplyPosition(1,3),installed,env.outer.catalogue,new ApplyPosition(1,20));
                }
                env.routed.refreshSchema(deletion,deleted);
                for (var machine:first.machines.values()) {
                    var store=(ReplicatedRangeStore)storeField.get(machine);
                    store.applySchemaFence(new ApplyPosition(1,4),creation.header());
                    var installed=pool.installSchema(store,new ApplyPosition(1,5),creation,Map.of(recreatedColumn,"data-recreated"),Set.of()).proof();
                    created=store.applySchemaActivation(new ApplyPosition(1,6),installed,env.outer.catalogue,new ApplyPosition(1,30));
                }
                env.routed.refreshSchema(creation,created);
            }
            byte[] identity=Files.readAllBytes(env.nodeRoot.resolve("node.identity"));
            var recovered=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.RECOVER,recovered)) {
                node.start(); assertArrayEquals(recreated.digest(),env.routed.directory().schemaDigest());
                for (var machine:recovered.machines.values()) {
                    var store=(ReplicatedRangeStore)storeField.get(machine);
                    assertArrayEquals(recreated.digest(),store.schemaDigest()); assertNull(store.schemaFence());
                    assertEquals(2,store.activatedSchemas().size());
                    try (var pin=store.capture()) { assertEquals(Set.of(recreatedColumn),pin.columns()); }
                }
            }
            assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
        }
    }
    @Test void installedEmptySchemaReopensUsingOriginalNodeManifestAndRemainsFenced() throws Exception {
        var schema=new DistributedSchema(new UUID(1,1),new UUID(1,2),new UUID(6,1),1,0,List.of(
                new DistributedSchema.Column(1,new UUID(2,3),"data",it.cavallium.rockserver.core.common.ColumnSchema.of(
                        it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true),null)));
        try (var env=new Env(root,false,schema.digest())) {
            var target=new DistributedSchema(schema.clusterId(),schema.databaseId(),schema.domainId(),2,0,List.of());
            var definition=new SchemaChange.Definition(new UUID(55,1),new OperationId(new UUID(55,2),1,0),env.plan.domains().getFirst().directory(),schema,target);
            var first=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,first)) {
                node.start();
                var poolField=RatisNodeRuntime.class.getDeclaredField("pool"); poolField.setAccessible(true);
                var pool=(DistributedStoragePool)poolField.get(node);
                var storeField=RatisRangeStateMachine.class.getDeclaredField("store"); storeField.setAccessible(true);
                for (var machine:first.machines.values()) {
                    var store=(ReplicatedRangeStore)storeField.get(machine); store.enableLogicalSchema(schema);
                    assertEquals(0,machine.takeSnapshot(),"Recovery must tolerate the preceding source-generation snapshot");
                    store.applySchemaFence(new ApplyPosition(1,1),definition.header());
                    pool.installSchema(store,new ApplyPosition(1,2),definition,Map.of(),Set.of());
                }
            }
            byte[] identity=Files.readAllBytes(env.nodeRoot.resolve("node.identity"));
            var recovered=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.RECOVER,recovered)) {
                node.start();
                var storeField=RatisRangeStateMachine.class.getDeclaredField("store"); storeField.setAccessible(true);
                for (var machine:recovered.machines.values()) {
                    var store=(ReplicatedRangeStore)storeField.get(machine);
                    assertArrayEquals(target.digest(),store.schemaDigest()); assertEquals(Set.of(),store.ownership().interval().columns());
                    assertEquals(new ApplyPosition(1,2),store.schemaInstallation().position());
                    assertThrows(IllegalStateException.class,store::capture,"Catalogue activation has not occurred");
                }
            }
            assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
        }
    }
    @Test void configuredParticipantRecoveryRunsOnControlAndDrainsBeforeNodeClose() throws Exception {
        try (var env=new Env(root); var homes=new TransactionHomeReader(env,env.outer.cluster)) {
            var recovery=new RatisNodeRuntime.RecoverySettings(homes.homes,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024),Duration.ofMillis(100),1);
            var fake=new FakeServer();
            RatisNodeRuntime.ServerFactory bound=(plan,logs,machines) -> {
                try {
                    var field=RatisRangeStateMachine.class.getDeclaredField("transactionHomes"); field.setAccessible(true);
                    for (var entry:machines.entrySet()) if (plan.role(entry.getKey().getUuid())==RatisNodePlan.Role.DATA) assertSame(homes.homes,field.get(entry.getValue()));
                } catch (ReflectiveOperationException failure) { throw new IOException(failure); }
                return fake.create(plan,logs,machines);
            };
            try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, bound, null, null, recovery)) {
                assertEquals(0,node.participantRecoveryStatus().openWorkers()); node.start(); assertEquals(2,env.timer.activePeriods());
                env.timer.fire(); RatisTransactionRecoveryServiceTest.idle(env.scheduler); assertNull(node.participantRecoveryFailure()); assertEquals(0,node.participantRecoveryStatus().persistedFiles());
            }
            assertEquals(0,env.timer.activePeriods()); assertEquals(0,homes.closes);
            try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.RECOVER, new FakeServer()::create, null, null, recovery)) { node.start(); env.timer.fire(); RatisTransactionRecoveryServiceTest.idle(env.scheduler); assertNull(node.participantRecoveryFailure()); }
            assertEquals(0,homes.closes);
        }
    }
    @Test void nodeCloseRetainsActualHelpOwnershipAfterCancelledReplyView() throws Exception {
        try (var env=new Env(root); var homes=new TransactionHomeReader(env,env.outer.cluster)) {
            var settings=new RatisNodeRuntime.RecoverySettings(homes.homes,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024),Duration.ofSeconds(1),1); var fake=new FakeServer();
            var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, fake::create, null, null, settings);
            var entered=new java.util.concurrent.CountDownLatch(1); var release=new java.util.concurrent.CountDownLatch(1);
            try {
                node.start(); var field=RatisNodeRuntime.class.getDeclaredField("snapshotHelp"); field.setAccessible(true); var help=(RatisSnapshotHelpService)field.get(node);
                var directory=env.plan.domains().getFirst().directory(); var route=directory.ranges().getFirst();
                var fence=new SnapshotFence(directory.clusterId(),directory.databaseId(),directory.domainId(),new UUID(13,1),new UUID(13,2),1,new OperationId(new UUID(13,1),1,0),directory.generation(),directory.schemaEpoch(),directory.schemaDigest(),List.of(
                        new SnapshotFence.Participant(route.rangeId(),route.groupId(),route.rangeEpoch(),directory.schemaDigest(),new OperationId(new UUID(13,3),0,0),new OperationId(new UUID(13,3),0,1),new TransactionScope(List.of(TransactionScope.Span.point(env.outer.data,new byte[]{1}))))));
                var view=help.submit(route.groupId(),ScopedTransactionRpc.SnapshotBinding.of(fence),() -> { entered.countDown(); if (!release.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return true; });
                assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)); assertTrue(view.cancel(false));
                assertThrows(IOException.class,node::close); assertEquals(1,help.activeJobs()); assertEquals(0,fake.closes);
                release.countDown(); help.stop().get(5,java.util.concurrent.TimeUnit.SECONDS); node.close(); assertEquals(1,fake.closes); assertEquals(0,help.activeJobs()); assertEquals(0,homes.closes);
            } finally { release.countDown(); node.close(); }
        }
    }

    @Test void defaultLocalHomeTimerRetiresConsumedUserOnlyAfterParticipantFenceExists() throws Exception {
        var helper=new RatisHomeRolesTest(); helper.root=root;
        try (var f=helper.new Fixture("automatic-retirement")) {
            var home=f.plan.home(RatisNodePlan.Role.TRANSACTION_HOME);
            var homes=new RatisTransactionStatuses(f.clients.get(home.role()),home.column());
            var initial=f.transaction(); var participant=f.plan.domains().getFirst().directory().ranges().getFirst();
            assertEquals(CommandResult.Status.SUCCESS,homes.execute(homes.prepare(new OperationId(helper.session,1,0),null,initial)).status());
            var aborted=initial.decide(TransactionStatus.Phase.ABORT);
            homes.execute(homes.prepare(new OperationId(helper.session,1,1),initial,aborted));
            ParticipantFinalization finalization; ApplyPosition finalized;
            try (var proposal=f.machine(participant.groupId()).prepareFinalization(homes,initial.transactionId(),new OperationId(new UUID(98,1),0,0))) {
                finalization=proposal.finalization(); finalized=f.data.executeFinalization(f.data.prepareFinalization(finalization));
            }
            var acknowledged=aborted.finalized(participant.groupId());
            homes.execute(homes.prepare(new OperationId(helper.session,1,2),aborted,acknowledged));
            var consumed=acknowledged.consume(); homes.execute(homes.prepare(new OperationId(helper.session,1,3),acknowledged,consumed));
            assertEquals(consumed,homes.read(initial.transactionId()));
            assertEquals(3,f.env.timer.activePeriods(),"DATA expiry, participant recovery and local HOME retirement timers are activated");
            var live=f.env.catalogue.epochs.read(); var expired=live.advanceTime(100);
            f.env.catalogue.epochs.execute(f.env.catalogue.epochs.prepare(f.env.outer.operation(2),live,expired));
            for (var directory:List.of(f.plan.domains().getFirst().directory(),home.directory())) {
                var route=directory.ranges().getFirst();
                var advance=f.env.catalogue.epochs.prepareAuthorityAdvance(directory,route.groupId(),new OperationId(helper.admin,2,100),-1);
                f.append(route.groupId(),DistributedCommandCodec.encode(advance));
            }
            // This fixture's transport supplies no generic stateView. Seed the real native participant
            // fence explicitly; automatic HOME retirement must discover and verify that durable proof.
            f.data.retireParticipant(new ParticipantRetirement(finalization,finalized,consumed));
            var identity=new ParticipantIdentity(home.directory().ranges().getFirst().groupId(),1,participant.groupId(),consumed);
            assertNotNull(f.data.participantFence(identity)); assertNull(f.data.finalizedParticipant(participant.groupId(),initial.transactionId()));
            var local=f.machine(home.directory().ranges().getFirst().groupId()).retirementCandidates(false,null,16);
            assertNotNull(local,"Current HOME owner exposes a bounded retirement page"); assertEquals(2,local.minimumEpoch());
            assertNotNull(local.candidate()); assertEquals(consumed,local.candidate().transaction());
            var background=retirementService(f.node);
            for (int n=0;n<12 && homes.read(initial.transactionId())!=null;n++) {
                f.env.timer.fire(); background.tick().get(5,java.util.concurrent.TimeUnit.SECONDS); RatisTransactionRecoveryServiceTest.idle(f.env.scheduler);
            }
            assertNull(f.node.retirementFailure()); assertNull(homes.read(initial.transactionId())); assertEquals(0,f.node.pendingRetirements());
            assertNotNull(homes.fence(homes.identity(consumed,home.directory().ranges().getFirst())));
            assertNotNull(f.data.participantFence(identity),"HOME retirement preserves the participant's original binding proof");
        }
    }
    @Test void automaticRetirementTimerAndDurableOwnershipDrainBeforeRuntimeReopen() throws Exception {
        var helper=new RatisHomeRolesTest(); helper.root=root;
        try (var f=helper.new Fixture("retirement-reopen")) {
            assertEquals(3,f.env.timer.activePeriods()); f.env.timer.fire(); retirementService(f.node).tick().get(5,java.util.concurrent.TimeUnit.SECONDS); RatisTransactionRecoveryServiceTest.idle(f.env.scheduler);
            assertNull(f.node.retirementFailure()); assertEquals(0,f.node.pendingRetirements());
            assertTrue(Files.isDirectory(f.env.nodeRoot.resolve("retirement")));
            f.node.close(); assertEquals(0,f.env.timer.activePeriods()); assertEquals(1,f.server.closes);
            assertEquals(0,f.env.catalogue.closes.get()); assertFalse(f.env.timer.isDisposed());
            // Reset only the controlled server's closed storage handles; the runtime reopens the
            // existing durable native groups and retirement journal under their original identities.
            f.server.storages.clear();
            var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,2),
                    f.env.settings().retention(),f.env.settings().clockPeriod(),1,4,f.env.settings().shutdownTimeout(),RatisNodeRuntime.ExportResources.defaults());
            try (var reopened=RatisNodeRuntime.open(f.plan, f.env.nodeRoot, ConfigParser.parseDefault(), f.env.catalogue.epochs, Map.of(f.data.directory().domainId(), f.data), f.env.scheduler, f.env.timer, () -> 20L, settings, RatisNodeRuntime.Startup.RECOVER, f.server::create, null, null, null, f::client)) {
                assertEquals(0,reopened.pendingRetirements()); reopened.start(); assertEquals(3,f.env.timer.activePeriods());
                f.env.timer.fire(); retirementService(reopened).tick().get(5,java.util.concurrent.TimeUnit.SECONDS); RatisTransactionRecoveryServiceTest.idle(f.env.scheduler);
                assertNull(reopened.retirementFailure()); assertEquals(0,reopened.pendingRetirements());
            }
            assertEquals(0,f.env.timer.activePeriods()); assertEquals(2,f.server.closes); assertEquals(0,f.env.catalogue.closes.get());
        }
    }

    private static RatisTransactionRecoveryService retirementService(RatisNodeRuntime node) throws Exception {
        var field=RatisNodeRuntime.class.getDeclaredField("retirementService"); field.setAccessible(true);
        return (RatisTransactionRecoveryService)field.get(node);
    }

    @Test void foreignRecoveryHomesAreRejectedBeforeBootstrapWrites() throws Exception {
        try (var env=new Env(root); var homes=new TransactionHomeReader(env,UUID.randomUUID())) {
            var recovery=new RatisNodeRuntime.RecoverySettings(homes.homes,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024),Duration.ofMillis(100),1);
            var fake=new FakeServer();
            assertThrows(IllegalArgumentException.class,() -> RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, fake::create, null, null, recovery));
            assertFalse(fake.created); try (var files=Files.list(env.nodeRoot)) { assertTrue(files.findAny().isEmpty()); }
        }
    }
    static final class TransactionHomeReader implements AutoCloseable {
        final RatisRoutedClient routed; final RatisTransactionStatuses homes; int closes;
        TransactionHomeReader(Env env,UUID cluster) {
            UUID column=new UUID(12,1),group=new UUID(12,2); var peer=env.plan.domains().getFirst().directory().ranges().getFirst().replicas().getFirst();
            var route=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
            var directory=new RangeDirectory(cluster,env.outer.database,new UUID(12,3),1,new byte[32],0,Set.of(column),0,List.of(route));
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError("No recovery journals"); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError("No recovery journals"); }
                public void close() { closes++; }
            }); homes=new RatisTransactionStatuses(routed,column);
        }
        @Override public void close() throws Exception { routed.close(); }
    }
    @Test void explicitPlacementStartsAllLocalGroupsAndRecoveryKeepsBorrowedResourcesOpen() throws Exception {
        try (var env=new Env(root)) {
            var first=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,first)) {
                assertFalse(node.started()); node.start(); assertTrue(node.started()); assertEquals(2,first.storages.size());
                assertEquals(1,env.timer.activePeriods()); assertNull(node.maintenanceStatus());
                assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer()));
            }
            assertEquals(1,first.closes); assertTrue(first.allFencedAtClose); assertEquals(0,env.timer.activePeriods());
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
            var store=(ReplicatedRangeStore)field.get(fake.machines.get(RaftGroupId.valueOf(env.plan.domains().getFirst().directory().ranges().getFirst().groupId())));
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
            var directory=env.plan.domains().getFirst().directory(); var a=directory.ranges().getFirst(); var b=directory.ranges().getLast();
            var changedReplica=a.replicas().stream().map(replica -> replica.nodeId().equals(env.plan.nodeId())
                    ? new ReplicaDescriptor(replica.nodeId(),replica.nodeIncarnation(),UUID.randomUUID(),replica.peerId(),replica.address(),replica.role()) : replica).toList();
            var variants=List.of(
                    List.of(new RangeDescriptor(a.rangeId(),a.groupId(),2,0,a.lowerInclusive(),a.upperExclusive(),a.topologyState(),a.replicas()),b),
                    List.of(new RangeDescriptor(a.rangeId(),a.groupId(),1,0,null,new byte[]{100},a.topologyState(),a.replicas()),
                            new RangeDescriptor(b.rangeId(),b.groupId(),1,0,new byte[]{100},null,b.topologyState(),b.replicas())),
                    List.of(new RangeDescriptor(a.rangeId(),a.groupId(),1,0,a.lowerInclusive(),a.upperExclusive(),a.topologyState(),changedReplica),b));
            for (var ranges:variants) {
                var changed=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),2,directory.columns(),directory.routingPrefixBytes(),ranges);
                var plan=new RatisNodePlan(env.plan.nodeId(), env.plan.nodeIncarnation(), List.of(new RatisNodePlan.Domain(changed, env.plan.columns(), env.plan.merges())), env.plan.catalogue(), List.of(), List.of(), Map.of(), 2);
                try (var routed=new RatisRoutedClient(changed,new RatisRoutedClient.Transport() {
                    public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                    public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError(); }
                })) {
                    var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),RatisRangeStateMachine.RetentionLimits.defaults(),Duration.ofSeconds(1),1,2,Duration.ofMillis(100),RatisNodeRuntime.ExportResources.defaults());
                    var fake=new FakeServer();
                    assertThrows(IOException.class,() -> RatisNodeRuntime.open(plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(routed.directory().domainId(), routed), env.scheduler, env.timer, () -> 20L, settings, RatisNodeRuntime.Startup.RECOVER, fake::create));
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
                node.start(); assertTrue(node.started()); assertEquals(2,env.timer.activePeriods()); env.timer.fire();
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
    @Test void interruptedPreparationResumesOnlyWithExactIdentityAndExplicitPlacementAuthority() throws Exception {
        try (var env=new Env(root)) {
            var failed=new FakeServer(); failed.failCreate=true;
            assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,failed));
            assertEquals(0,failed.starts);
            byte[] identity=SessionJournalFiles.read(env.nodeRoot.resolve("node.identity"),128);
            assertEquals(33,identity.length); assertEquals(0,identity[0]);
            assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer()));
            var resumed=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,resumed)) {
                node.start(); assertTrue(resumed.exposedBeforeStart); assertTrue(node.started());
            }
            assertEquals(1,SessionJournalFiles.read(env.nodeRoot.resolve("node.identity"),128)[0]);
            try (var recovered=env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer())) { recovered.start(); }
        }
    }
    @Test void sealFailureNeverStartsServerAndExposedStartFailureCannotReinitializeMissingVoters() throws Exception {
        try (var env=new Env(root)) {
            Path identity=env.nodeRoot.resolve("node.identity"); byte[] preparing; var fake=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake)) {
                preparing=Files.readAllBytes(identity); Files.delete(identity); Files.createDirectory(identity); Files.writeString(identity.resolve("block"),"fixture");
                assertThrows(IOException.class,node::start); assertEquals(0,fake.starts); assertEquals(RatisNodeRuntime.State.STOPPING,node.state());
            }
            Files.delete(identity.resolve("block")); Files.delete(identity); Files.write(identity,preparing);
            var failed=new FakeServer(); failed.failStart=true;
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,failed)) {
                assertThrows(IOException.class,node::start); assertTrue(failed.exposedBeforeStart);
            }
            assertEquals(1,SessionJournalFiles.read(identity,128)[0]);
            assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new FakeServer()));
            assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer()));
        }
    }
    @Test void currentIdentityIsCanonicalAndRejectsMalformedPhaseShapeAndBinding() throws Exception {
        try (var env=new Env(root)) {
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new FakeServer())) { node.start(); }
            Path identity=env.nodeRoot.resolve("node.identity");
            byte[] canonical=SessionJournalFiles.read(identity,128);
            assertEquals(33,canonical.length); assertEquals(1,canonical[0]);
            assertArrayEquals(env.plan.identityDigest(),Arrays.copyOfRange(canonical,1,33));
            for (int length=0;length<canonical.length;length++) assertIdentityRejected(env,Arrays.copyOf(canonical,length));
            assertIdentityRejected(env,Arrays.copyOf(canonical,canonical.length+1));
            byte[] phase=canonical.clone(); phase[0]=2; assertIdentityRejected(env,phase);
            byte[] binding=canonical.clone(); binding[1]^=1; assertIdentityRejected(env,binding);
            SessionJournalFiles.write(identity,canonical,128);
            try (var recovered=env.open(RatisNodeRuntime.Startup.RECOVER,new FakeServer())) { recovered.start(); }
            assertArrayEquals(canonical,SessionJournalFiles.read(identity,128));
        }
    }
    private static void assertIdentityRejected(Env env,byte[] identity) throws Exception {
        SessionJournalFiles.write(env.nodeRoot.resolve("node.identity"),identity,128);
        var fake=new FakeServer();
        assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.RECOVER,fake)); assertFalse(fake.created);
        assertThrows(IOException.class,() -> env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new FakeServer()));
    }
    @Test void trustedSnapshotHomeReaderIsBoundBeforeEagerStateMachineInitializationOnlyOnDataGroups() throws Exception {
        try (var env=new Env(root,true); var reader=new SnapshotReader(env.plan.domains().getFirst().directory(),env.outer.cluster,env.outer.database)) {
            var fake=new FakeServer(); var checked=new java.util.concurrent.atomic.AtomicInteger();
            RatisNodeRuntime.ServerFactory eager=(plan,logs,machines) -> {
                try {
                    for (var entry:machines.entrySet()) {
                        assertEquals("NEW",entry.getValue().getLifeCycleState().name());
                        boolean catalogue=entry.getKey().getUuid().equals(plan.catalogue().groupId());
                        assertSame(catalogue ? null : reader.homes,snapshotHomes(entry.getValue())); checked.incrementAndGet();
                    }
                    var server=fake.create(plan,logs,machines);
                    for (var group:plan.localGroups()) fake.initialize(group);
                    return server;
                } catch (Exception failure) { throw new IOException(failure); }
            };
            try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, eager, null, reader.homes)) {
                assertEquals(3,checked.get()); assertEquals(3,fake.storages.size()); assertEquals(0,fake.starts);
                var data=fake.machines.entrySet().stream().filter(e -> !e.getKey().getUuid().equals(env.plan.catalogue().groupId())).findFirst().orElseThrow().getValue();
                assertSame(reader.homes,snapshotHomes(data)); assertThrows(IllegalStateException.class,() -> data.snapshotHomes(reader.homes));
                node.start(); assertTrue(node.started()); assertTrue(fake.exposedBeforeStart);
            }
            assertEquals(0,reader.closes); assertNull(reader.homes.read(new UUID(0,3100))); assertEquals(0,env.transportCloses); assertEquals(0,env.catalogue.closes.get());
            assertTrue(fake.allFencedAtClose);
        }
    }
    @Test void wrongSnapshotHomeClusterOrDatabaseFailsBeforeAnyNodeBootstrapWrite() throws Exception {
        try (var env=new Env(root)) {
            for (boolean cluster:new boolean[]{false,true}) try (var reader=new SnapshotReader(env.plan.domains().getFirst().directory(),cluster ? new UUID(0,3101) : env.outer.cluster,cluster ? env.outer.database : new UUID(0,3102))) {
                var fake=new FakeServer();
                assertThrows(IllegalArgumentException.class,() -> env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake,reader.homes));
                assertFalse(fake.created); assertEquals(0,reader.closes); assertNull(reader.homes.read(new UUID(0,3100)));
                try (var files=Files.list(env.nodeRoot)) { assertTrue(files.findAny().isEmpty(),"Binding must fail before lock, identity, Raft or native bootstrap files are created"); }
            }
        }
    }
    @Test void ordinaryConvenienceLeavesSnapshotHomeReaderUnconfiguredOnAllRoles() throws Exception {
        try (var env=new Env(root,true)) {
            var fake=new FakeServer();
            try (var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake)) {
                for (var machine:fake.machines.values()) { assertEquals("NEW",machine.getLifeCycleState().name()); assertNull(snapshotHomes(machine)); }
                node.start(); assertTrue(node.started());
            }
        }
    }
    @Test void schemaReplicaServiceIsBoundBeforeEagerInitializationToDurableVoterAndListenerIdentities() throws Exception {
        try (var env=new Env(root,true)) {
            var fake=new FakeServer(); var owned=new java.util.concurrent.atomic.AtomicReference<Object>(); var groups=new java.util.concurrent.atomic.AtomicInteger();
            RatisNodeRuntime.ServerFactory eager=(plan,logs,machines) -> {
                try {
                    for (var entry:machines.entrySet()) {
                        assertEquals("NEW",entry.getValue().getLifeCycleState().name());
                        var replicas=schemaReplicaService(entry.getValue());
                        if (plan.role(entry.getKey().getUuid())==RatisNodePlan.Role.DATA) {
                            assertNotNull(replicas); if (owned.get()==null) owned.set(replicas); else assertSame(owned.get(),replicas); groups.incrementAndGet();
                            var own=plan.groupDescriptor(entry.getKey().getUuid()).replicas().stream().filter(replica -> replica.nodeId().equals(plan.nodeId())).findFirst().orElseThrow();
                            assertEquals(plan.nodeIncarnation(),own.nodeIncarnation());
                        } else assertNull(replicas);
                    }
                    var server=fake.create(plan,logs,machines); for (var group:plan.localGroups()) fake.initialize(group); return server;
                } catch (Exception failure) { throw new IOException(failure); }
            };
            try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, eager)) {
                assertEquals(2,groups.get()); assertEquals(0,fake.starts); node.start(); assertTrue(node.started());
                var listener=env.plan.domains().getFirst().directory().ranges().stream().filter(range -> range.replicas().stream().anyMatch(replica -> replica.nodeId().equals(env.plan.nodeId()) && replica.role()==ReplicaDescriptor.Role.LISTENER)).findFirst().orElseThrow();
                var machine=fake.machines.get(RaftGroupId.valueOf(listener.groupId())); var own=listener.replicas().stream().filter(replica -> replica.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();
                assertThrows(IllegalStateException.class,() -> machine.schemaReplicas((RatisSchemaReplicaService)owned.get(),own));
            }
            assertEquals(1,fake.closes); assertFalse(env.timer.isDisposed()); assertEquals(0,env.transportCloses);
        }
    }
    @Test void schemaReplicaDrainTimeoutRetainsNodePoolAndStagingOwnershipUntilActualPolicyWorkEnds() throws Exception {
        var outer=new RatisSessionEpochsTest(); var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true);
        var source=new DistributedSchema(outer.cluster,outer.database,new UUID(6,1),1,0,List.of(new DistributedSchema.Column(71,outer.data,"data",shape,null)));
        byte[] jar=appendArtifact(); var operator=new DistributedSchema.Operator("append",1,AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"pure-append",1);
        var addedShape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true,operator.name(),operator.version(),operator.className());
        var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),2,0,List.of(source.column(71),new DistributedSchema.Column(72,new UUID(77,1),"added",addedShape,operator)));
        var entered=new java.util.concurrent.CountDownLatch(1); var unblock=new java.util.concurrent.CountDownLatch(1);
        try (var env=new Env(root,false,source.digest())) {
            var fake=new FakeServer(); var contract=new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("pure-append",1,true);
            var config=new RatisNodeRuntime.SchemaReplicaConfig(new RatisSchemaReplicaService.Limits(4,64L*1024*1024,64L*1024*1024,4,4,0),descriptor -> { assertEquals(operator,descriptor); entered.countDown(); if (!unblock.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new IOException("Policy fixture timeout"); return contract; });
            var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, fake::create, List.of(source), null, null, RatisRoutedClient::connect, config);
            try {
                node.start(); var route=env.plan.domains().getFirst().directory().ranges().getLast(); var own=route.replicas().stream().filter(replica -> replica.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow(); assertEquals(ReplicaDescriptor.Role.LISTENER,own.role());
                var definition=new SchemaChange.Definition(new UUID(77,2),new OperationId(new UUID(77,3),1,0),env.plan.domains().getFirst().directory(),source,target);
                var upload=OperatorUpload.create(new UUID(77,4),source.clusterId(),source.databaseId(),operator,jar);
                var request=new RatisSchemaReplicaRpc.Request(ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE,new RatisSchemaReplicaRpc.Target(new RatisSchemaReplicaRpc.Binding(definition,route.groupId(),"added",true),own),upload,-1,null);
                var service=(RatisSchemaReplicaService)schemaReplicaService(fake.machines.get(RaftGroupId.valueOf(route.groupId())));
                var observer=service.submit(route.groupId(),request.encode()); assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)); var actual=service.submit(route.groupId(),request.encode()); assertTrue(observer.cancel(false));
                assertThrows(IOException.class,node::close); assertEquals(RatisNodeRuntime.State.STOPPING,node.state()); assertEquals(0,fake.closes);
                for (var path:List.of(env.nodeRoot.resolve("node.lock"),env.nodeRoot.resolve("schema-replicas/lock"))) try (var owner=java.nio.channels.FileChannel.open(path,StandardOpenOption.WRITE)) { assertThrows(java.nio.channels.OverlappingFileLockException.class,owner::tryLock); }
                assertFalse(env.timer.isDisposed()); assertEquals(0,env.transportCloses);
                unblock.countDown(); assertFalse(RatisSchemaReplicaRpc.decodeReply(actual.get(5,java.util.concurrent.TimeUnit.SECONDS),request).artifactReady());
                node.close(); assertEquals(RatisNodeRuntime.State.CLOSED,node.state()); assertEquals(1,fake.closes); assertTrue(fake.allFencedAtClose);
                try (var owner=java.nio.channels.FileChannel.open(env.nodeRoot.resolve("schema-replicas/lock"),StandardOpenOption.WRITE); var acquired=owner.tryLock()) { assertNotNull(acquired); }
            } finally { unblock.countDown(); node.close(); }
        }
    }
    @Test void catalogueValidatorsReadCommittedDefinitionAndArtifactWithExactConfiguredPolicy() throws Exception {
        var outer=new RatisSessionEpochsTest(); var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true);
        var source=new DistributedSchema(outer.cluster,outer.database,new UUID(6,1),1,0,List.of(new DistributedSchema.Column(71,outer.data,"data",shape,null)));
        var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),2,0,List.of());
        byte[] jar=appendArtifact(); var descriptor=new DistributedSchema.Operator("append",0,AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"pure-append",1);
        var policy=new java.util.concurrent.atomic.AtomicReference<it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract>();
        try (var env=new Env(root,true,source.digest())) {
            var config=new RatisNodeRuntime.SchemaReplicaConfig(RatisSchemaReplicaService.Limits.defaults(),operator -> { assertEquals(descriptor,operator); return policy.get(); }); var fake=new FakeServer();
            try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, fake::create, List.of(source), null, null, RatisRoutedClient::connect, config)) {
                node.start(); var c=env.plan.catalogue(); var machine=fake.machines.get(RaftGroupId.valueOf(c.groupId()));
                var validatorField=RatisRangeStateMachine.class.getDeclaredField("commandValidator"); validatorField.setAccessible(true);
                @SuppressWarnings("unchecked") var validator=(java.util.function.Consumer<DistributedCommand>)validatorField.get(machine);
                var storeField=RatisRangeStateMachine.class.getDeclaredField("store"); storeField.setAccessible(true); var store=(ReplicatedRangeStore)storeField.get(machine);
                var definition=new SchemaChange.Definition(new UUID(88,1),env.outer.operation(100),env.plan.domains().getFirst().directory(),source,target);
                var administration=new RatisSchemaAdministration(env.catalogue.client,source.clusterId(),source.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
                var begin=administration.prepareBegin(definition,null,null,null);
                assertThrows(IllegalArgumentException.class,() -> validator.accept(begin));
                var immutable=administration.prepareDefinition(env.outer.operation(101),definition,null,null,null); validator.accept(immutable);
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,1),immutable)).status());
                var schemas=new RatisSchemaCatalogue(env.catalogue.client,source.clusterId(),source.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
                var sourcePublication=schemas.prepare(env.outer.operation(110),source); validator.accept(sourcePublication); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,2),sourcePublication)).status());
                var targetPublication=schemas.prepare(env.outer.operation(111),target,store.readCurrentPoint(c.directoryColumn(),RatisColumnAllocation.counterKey()),Map.of(),Map.of(),Map.of()); validator.accept(targetPublication); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,3),targetPublication)).status());
                assertDoesNotThrow(() -> validator.accept(begin));
                var io=(org.apache.ratis.client.api.BlockingApi)Proxy.newProxyInstance(org.apache.ratis.client.api.BlockingApi.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.api.BlockingApi.class},(proxy,method,args) -> {
                    if (!method.getName().equals("sendReadOnly")) throw new AssertionError(method);
                    var packet=ScopedTransactionRpc.parse(((Message)args[0]).getContent().toByteArray(),ScopedTransactionRpc.MAX_REQUEST_BYTES); assertEquals(ScopedTransactionRpc.POINT,packet.kind()); var input=java.nio.ByteBuffer.wrap(packet.body()); assertEquals(c.directoryColumn(),ScopedTransactionRpc.uuid(input)); var key=ScopedTransactionRpc.blob(input,2*1024*1024,false);
                    byte[] value=store.readCurrentPoint(c.directoryColumn(),key); var body=value==null ? java.nio.ByteBuffer.allocate(4).putInt(-1).array() : java.nio.ByteBuffer.allocate(value.length+4).putInt(value.length).put(value).array();
                    return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(env.plan.peerId())).setGroupId(RaftGroupId.valueOf(c.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,body)))).build();
                });
                var client=(org.apache.ratis.client.RaftClient)Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args) -> switch (method.getName()) { case "getGroupId" -> RaftGroupId.valueOf(c.groupId()); case "io" -> io; default -> throw new AssertionError(method); });
                var catalogue=new RatisOperatorCatalogue(client,source.clusterId(),source.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn(),new RatisOperatorCatalogue.Limits(config.limits().maximumUploads(),config.limits().maximumDiskBytes()),upload -> {});
                var upload=OperatorUpload.create(new UUID(88,2),source.clusterId(),source.databaseId(),descriptor,jar); var operatorBegin=catalogue.prepareBegin(env.outer.operation(102),upload,null,null);
                assertThrows(IllegalArgumentException.class,() -> validator.accept(operatorBegin));
                policy.set(new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("pure-append",2,true)); assertThrows(IllegalArgumentException.class,() -> validator.accept(operatorBegin));
                policy.set(new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("pure-append",1,false)); assertThrows(IllegalArgumentException.class,() -> validator.accept(operatorBegin));
                policy.set(new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("pure-append",1,true)); validator.accept(operatorBegin);
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,4),operatorBegin)).status());
                var chunk=catalogue.prepareChunk(env.outer.operation(103),upload,0,jar); validator.accept(chunk); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,5),chunk)).status());
                var publication=catalogue.preparePublication(env.outer.operation(104),upload,null,null); validator.accept(publication); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,6),publication)).status());
                assertArrayEquals(upload.encode(),store.readCurrentPoint(c.directoryColumn(),RatisOperatorCatalogue.publishedKey(descriptor.name(),descriptor.version())));
                assertThrows(IOException.class,() -> RatisNodeRuntime.SchemaReplicaConfig.defaults().policy().admit(descriptor));
            }
        }
    }
    @Test void catalogueReplicaClientIsPreinitializedOnlyForCatalogueAndRetainedDuringActualOutboundCall() throws Exception {
        var entered=new java.util.concurrent.CountDownLatch(1); var unblock=new java.util.concurrent.CountDownLatch(1); var connections=new java.util.concurrent.atomic.AtomicInteger(); var closes=new java.util.concurrent.atomic.AtomicInteger();
        try (var env=new Env(root,true); var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var clients=new RatisSchemaReplicaClient(32,(range,owner) -> {
                connections.incrementAndGet();
                var io=(org.apache.ratis.client.api.BlockingApi)Proxy.newProxyInstance(org.apache.ratis.client.api.BlockingApi.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.api.BlockingApi.class},(proxy,method,args) -> {
                    if (!method.getName().equals("sendStaleRead")) throw new AssertionError(method); var request=RatisSchemaReplicaRpc.request(((Message)args[0]).getContent().toByteArray()); entered.countDown(); if (!unblock.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new IOException("Outbound fixture timeout");
                    return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(owner.peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(RatisSchemaReplicaRpc.reply(request,new BitSet(),false,null,RatisSchemaReplicaRpc.CapacityState.NONE,false)))).build();
                });
                return (org.apache.ratis.client.RaftClient)Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args) -> switch (method.getName()) { case "getGroupId" -> RaftGroupId.valueOf(range.groupId()); case "io" -> io; case "close" -> { closes.incrementAndGet(); yield null; } default -> throw new AssertionError(method); });
            });
            var fake=new FakeServer(); RatisNodeRuntime.ServerFactory factory=(plan,logs,machines) -> {
                try { for (var entry:machines.entrySet()) { assertEquals("NEW",entry.getValue().getLifeCycleState().name()); assertSame(plan.role(entry.getKey().getUuid())==RatisNodePlan.Role.CATALOGUE ? clients : null,schemaFencingClient(entry.getValue())); assertEquals(plan.role(entry.getKey().getUuid())==RatisNodePlan.Role.DATA,topologyAuthority(entry.getValue())!=null); var authority=proofLane(entry.getValue(),"schemaAuthorityQueries"); if (plan.role(entry.getKey().getUuid())==RatisNodePlan.Role.CATALOGUE) { assertNotNull(authority); assertEquals(1,configuredProofThreads(authority)); assertNotSame(proofLane(entry.getValue(),"proofQueries"),authority); } else assertNull(authority); } return fake.create(plan,logs,machines); }
                catch (ReflectiveOperationException failure) { throw new IOException(failure); }
            };
            var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, factory, null, null, null, RatisRoutedClient::connect, RatisNodeRuntime.SchemaReplicaConfig.defaults(), () -> clients);
            try {
                assertEquals(0,connections.get()); node.start(); assertEquals(0,connections.get());
                var source=new DistributedSchema(env.outer.cluster,env.outer.database,new UUID(6,1),1,0,List.of(new DistributedSchema.Column(71,env.outer.data,"data",it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true),null)));
                var directory=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),1,source.digest(),1,source.columnIds(),0,env.plan.domains().getFirst().directory().ranges()); var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),2,0,List.of());
                var definition=new SchemaChange.Definition(new UUID(89,1),env.outer.operation(100),directory,source,target); var route=directory.ranges().getLast(); var binding=new RatisSchemaReplicaRpc.Binding(definition,route.groupId(),null,false); var owner=route.replicas().stream().filter(replica -> replica.role()==ReplicaDescriptor.Role.LISTENER).findFirst().orElseThrow(); var request=RatisSchemaReplicaClient.capacity(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE,new RatisSchemaReplicaRpc.Target(binding,owner));
                var actual=executor.submit(() -> clients.execute(request)); assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)); assertThrows(IOException.class,node::close); assertEquals(RatisNodeRuntime.State.STOPPING,node.state()); assertEquals(0,closes.get());
                try (var ownerLock=java.nio.channels.FileChannel.open(env.nodeRoot.resolve("node.lock"),StandardOpenOption.WRITE)) { assertThrows(java.nio.channels.OverlappingFileLockException.class,ownerLock::tryLock); }
                unblock.countDown(); assertEquals(RatisSchemaReplicaRpc.CapacityState.NONE,actual.get(5,java.util.concurrent.TimeUnit.SECONDS).capacityState()); node.close(); assertEquals(1,closes.get()); assertEquals(RatisNodeRuntime.State.CLOSED,node.state());
            } finally { unblock.countDown(); node.close(); }
        }
        try (var env=new Env(root.resolve("data-only"))) {
            try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, new FakeServer()::create, null, null, null, RatisRoutedClient::connect, RatisNodeRuntime.SchemaReplicaConfig.defaults(), () -> { throw new AssertionError("DATA-only node must not allocate catalogue outbound clients"); })) { assertEquals(RatisNodeRuntime.State.OPEN,node.state()); }
        }
    }
    @Test void ordinaryProofDrainKeepsCatalogueAuthorityAliveForItsRunningNestedDependency() throws Exception {
        var entered=new java.util.concurrent.CountDownLatch(1); var continueNested=new java.util.concurrent.CountDownLatch(1);
        try (var env=new Env(root,true)) {
            var fake=new FakeServer(); var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake);
            try {
                node.start(); var catalogue=fake.machines.get(RaftGroupId.valueOf(env.plan.catalogue().groupId()));
                var ordinary=proofLane(catalogue,"proofQueries"); var authority=proofLane(catalogue,"schemaAuthorityQueries"); assertNotSame(ordinary,authority); assertEquals(4,configuredProofThreads(ordinary)); assertEquals(1,configuredProofThreads(authority));
                var query=ordinary.submit(env.plan.catalogue().groupId(),new byte[]{1},128,false,owned -> {
                    entered.countDown(); if (!continueNested.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new IOException("Nested fixture timeout");
                    return authority.submit(env.plan.catalogue().groupId(),new byte[]{2},RatisProofQueryService.MAX_BYTES,false,request -> Message.EMPTY).get(5,java.util.concurrent.TimeUnit.SECONDS);
                });
                assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)); assertThrows(IOException.class,node::close); assertEquals(RatisNodeRuntime.State.STOPPING,node.state()); assertEquals(0,fake.closes);
                assertEquals(Message.EMPTY,authority.submit(env.plan.catalogue().groupId(),new byte[]{3},64,false,owned -> Message.EMPTY).get(5,java.util.concurrent.TimeUnit.SECONDS));
                continueNested.countDown(); assertEquals(Message.EMPTY,query.get(5,java.util.concurrent.TimeUnit.SECONDS)); node.close(); assertEquals(RatisNodeRuntime.State.CLOSED,node.state()); assertEquals(1,fake.closes);
                assertTrue(authority.submit(env.plan.catalogue().groupId(),new byte[]{4},64,false,owned -> Message.EMPTY).isCompletedExceptionally());
            } finally { continueNested.countDown(); node.close(); }
        }
    }
    @Test void importPreparationDrainKeepsCatalogueAuthorityAliveUntilActualOwnerReadCompletes() throws Exception {
        var entered=new java.util.concurrent.CountDownLatch(1); var continueNested=new java.util.concurrent.CountDownLatch(1);
        try (var env=new Env(root,true)) {
            var fake=new FakeServer(); var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake);
            try {
                node.start(); var catalogue=fake.machines.get(RaftGroupId.valueOf(env.plan.catalogue().groupId()));
                var imports=proofLane(catalogue,"importPublicationQueries"); var authority=proofLane(catalogue,"schemaAuthorityQueries");
                assertNotSame(imports,authority); assertNotSame(imports,proofLane(catalogue,"proofQueries")); assertEquals(1,configuredProofThreads(imports));
                var query=imports.submit(env.plan.catalogue().groupId(),new byte[]{1},RatisProofQueryService.IMPORT_PUBLICATION_BYTES,false,owned -> {
                    entered.countDown(); if (!continueNested.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new IOException("Nested import fixture timeout");
                    return authority.submit(env.plan.catalogue().groupId(),new byte[]{2},128,false,request -> Message.EMPTY).get(5,java.util.concurrent.TimeUnit.SECONDS);
                });
                assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)); query.cancel(false);
                assertThrows(IOException.class,node::close); assertEquals(RatisNodeRuntime.State.STOPPING,node.state()); assertEquals(0,fake.closes);
                assertEquals(1,imports.stats().jobs()); assertEquals(RatisProofQueryService.IMPORT_PUBLICATION_BYTES,imports.stats().bytes());
                assertEquals(Message.EMPTY,authority.submit(env.plan.catalogue().groupId(),new byte[]{3},64,false,owned -> Message.EMPTY).get(5,java.util.concurrent.TimeUnit.SECONDS));
                continueNested.countDown(); imports.stop().get(5,java.util.concurrent.TimeUnit.SECONDS); node.close();
                assertEquals(RatisNodeRuntime.State.CLOSED,node.state()); assertEquals(1,fake.closes); assertEquals(0,imports.stats().jobs()); assertEquals(0,imports.stats().bytes());
                assertTrue(imports.submit(env.plan.catalogue().groupId(),new byte[]{4},64,false,owned -> Message.EMPTY).isCompletedExceptionally());
            } finally { continueNested.countDown(); node.close(); }
        }
    }
    @Test void importImageColumnLimitRejectsBeforePermanentNativeRangeBirth()throws Exception{
        var outer=new RatisSessionEpochsTest();var second=new UUID(6,99);var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true);var schema=new DistributedSchema(outer.cluster,outer.database,new UUID(6,1),1,0,List.of(new DistributedSchema.Column(71,outer.data,"data",shape,null),new DistributedSchema.Column(72,second,"second",shape,null)));
        try(var env=new Env(root,false,schema.digest())){
            var original=env.plan.domains().getFirst().directory();var target=original.ranges().getFirst();var ranges=new ArrayList<>(original.ranges());ranges.set(0,new RangeDescriptor(target.rangeId(),target.groupId(),target.rangeEpoch(),target.configurationVersion(),target.lowerInclusive(),target.upperExclusive(),RangeDescriptor.TopologyState.FENCED,target.replicas()));var directory=new RangeDirectory(original.clusterId(),original.databaseId(),original.domainId(),original.schemaEpoch(),schema.digest(),original.generation(),Set.of(outer.data,second),original.routingPrefixBytes(),ranges);var manifest=new NativeSstImportManifest(UUID.randomUUID(),target.groupId(),target.rangeEpoch(),directory.domainId(),schema.digest(),target.lowerInclusive(),target.upperExclusive(),List.of(new NativeSstImportManifest.Column(outer.data,"data",false,List.of()),new NativeSstImportManifest.Column(second,"second",false,List.of())),RatisRangeImportRecord.DISTRIBUTED_LIMITS);var definition=new RangeImportDefinition(manifest.importId(),new OperationId(UUID.randomUUID(),1,0),directory,List.of(manifest));var plan=new RatisNodePlan(env.plan.nodeId(), env.plan.nodeIncarnation(), List.of(new RatisNodePlan.Domain(directory, Map.of(outer.data,"data",second,"second"), Set.of())), env.plan.catalogue(), List.of(), List.of(definition), Map.of(), 2);var base=env.settings();var images=new NativeRangeImage.Limits(base.images().maxRowBytes(),base.images().maxImageBytes(),1);var settings=new RatisNodeRuntime.Settings(images,base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),base.exports());var fake=new FakeServer();
            try(var routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError();}})){
                assertThrows(IOException.class,()->RatisNodeRuntime.open(plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(routed.directory().domainId(), routed), env.scheduler, env.timer, ()->20L, settings, RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, fake::create, List.of(schema), null));assertNull(fake.server,"Import image preflight must reject before Ratis server creation");
            }
            try(var pool=DistributedStoragePool.open(env.nodeRoot.resolve("native"),ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()))){var field=DistributedStoragePool.class.getDeclaredField("ownershipHistory");field.setAccessible(true);for(var history:(List<?>)field.get(pool)){var owner=history.getClass().getDeclaredMethod("owner");owner.setAccessible(true);assertNotEquals(target.groupId(),((RangeOwnership.Owner)owner.invoke(history)).groupId(),"Rejected image geometry must not permanently reserve the imported native group");}}
        }
    }
    @Test void plannedImportStartsAndRecoversFencedWithDurableGroupFilesAndRetainedReaderDrain() throws Exception {
        var outer=new RatisSessionEpochsTest();
        var schema=new DistributedSchema(outer.cluster,outer.database,new UUID(6,1),1,0,List.of(new DistributedSchema.Column(71,outer.data,"data",it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true),null)));
        try (var env=new Env(root,false,schema.digest())) {
            var original=env.plan.domains().getFirst().directory(); var target=original.ranges().getFirst(); var ranges=new ArrayList<>(original.ranges());
            ranges.set(0,new RangeDescriptor(target.rangeId(),target.groupId(),target.rangeEpoch(),target.configurationVersion(),target.lowerInclusive(),target.upperExclusive(),RangeDescriptor.TopologyState.FENCED,target.replicas()));
            var directory=new RangeDirectory(original.clusterId(),original.databaseId(),original.domainId(),original.schemaEpoch(),original.schemaDigest(),original.generation(),original.columns(),original.routingPrefixBytes(),ranges);
            var manifest=new NativeSstImportManifest(UUID.randomUUID(),target.groupId(),target.rangeEpoch(),directory.domainId(),schema.digest(),target.lowerInclusive(),target.upperExclusive(),List.of(new NativeSstImportManifest.Column(outer.data,"data",false,List.of())),RatisRangeImportRecord.DISTRIBUTED_LIMITS);
            var definition=new RangeImportDefinition(manifest.importId(),new OperationId(UUID.randomUUID(),1,0),directory,List.of(manifest));
            var plan=new RatisNodePlan(env.plan.nodeId(), env.plan.nodeIncarnation(), List.of(new RatisNodePlan.Domain(directory, env.plan.columns(), env.plan.merges())), env.plan.catalogue(), List.of(), List.of(definition), Map.of(), 2);
            try (var routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor range,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor range,DistributedCommand command) { throw new AssertionError(); }
                public void close() {}
            })) {
                var fake=new FakeServer(); var field=RatisRangeStateMachine.class.getDeclaredField("store"); field.setAccessible(true);
                var node=RatisNodeRuntime.open(plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(routed.directory().domainId(), routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, fake::create, List.of(schema), null);
                RatisReferencedFiles.Reader retained=null;
                try {
                    var machine=fake.machines.get(RaftGroupId.valueOf(target.groupId())); var store=(ReplicatedRangeStore)field.get(machine);
                    assertTrue(store.importPending()); assertEquals(RangeOwnership.State.FENCED,store.ownership().state());
                    node.start(); assertTrue(store.importPending()); assertEquals(RangeOwnership.State.FENCED,store.ownership().state()); assertTrue(machine.query(Message.EMPTY).isCompletedExceptionally());
                    var files=(RatisReferencedFiles)machine.data();
                    files.write(RatisReferencedFilesTest.entry(target.groupId(),UUID.randomUUID(),1,1,manifest.encode(),true)).get(5,java.util.concurrent.TimeUnit.SECONDS); files.flush(1).get(5,java.util.concurrent.TimeUnit.SECONDS);
                    retained=files.pin(files.reference(1,1)); assertArrayEquals(manifest.encode(),retained.read().toByteArray());
                    assertThrows(IOException.class,node::close); assertEquals(RatisNodeRuntime.State.STOPPING,node.state()); assertEquals(1,fake.closes);
                    retained.close(); retained=null; node.close(); assertEquals(RatisNodeRuntime.State.CLOSED,node.state());
                } finally { if(retained!=null)retained.close(); node.close(); }
                fake.storages.clear();
                try (var recovered=RatisNodeRuntime.open(plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(routed.directory().domainId(), routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.RECOVER, fake::create, List.of(schema), null)) {
                    recovered.start(); var machine=fake.machines.get(RaftGroupId.valueOf(target.groupId())); var store=(ReplicatedRangeStore)field.get(machine);
                    assertTrue(store.importPending()); assertEquals(RangeOwnership.State.FENCED,store.ownership().state());
                    var files=(RatisReferencedFiles)machine.data(); try(var reader=files.pin(files.reference(1,1))) { assertArrayEquals(manifest.encode(),reader.read().toByteArray()); }
                }
            }
        }
    }
    @Test void runtimeAuthorityReaderRecoversExactAbortedOutcomeAfterAnotherCurrentHead() throws Exception {
        var outer=new RatisSessionEpochsTest(); var source=new DistributedSchema(outer.cluster,outer.database,new UUID(6,1),1,0,List.of(new DistributedSchema.Column(71,outer.data,"data",it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true),null)));
        var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),2,0,List.of());
        try (var env=new Env(root,true,source.digest())) {
            var fake=new FakeServer(); var reads=new ArrayList<RatisSchemaAuthority.Request>(); var c=env.plan.catalogue();
            var io=(org.apache.ratis.client.api.BlockingApi)Proxy.newProxyInstance(org.apache.ratis.client.api.BlockingApi.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.api.BlockingApi.class},(proxy,method,args) -> {
                if (!method.getName().equals("sendReadOnly") || ScopedTransactionRpc.parse(((Message)args[0]).getContent().toByteArray(),ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()!=ScopedTransactionRpc.SCHEMA_AUTHORITY) {
                    try { return method.invoke(env.catalogue.client.io(),args); } catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                }
                var message=(Message)args[0]; reads.add(RatisSchemaAuthority.request(message.getContent().toByteArray()));
                final Message reply;
                try { reply=fake.machines.get(RaftGroupId.valueOf(c.groupId())).queryLinearizable(message).get(5,java.util.concurrent.TimeUnit.SECONDS); }
                catch (java.util.concurrent.ExecutionException failure) { throw new IOException("Controlled authority cut rejected",failure.getCause()); }
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(env.plan.peerId())).setGroupId(RaftGroupId.valueOf(c.groupId())).setCallId(1).setSuccess(true).setMessage(reply).build();
            });
            var client=(org.apache.ratis.client.RaftClient)Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args) -> switch (method.getName()) { case "getGroupId" -> RaftGroupId.valueOf(c.groupId()); case "io" -> io; default -> throw new AssertionError(method); });
            var epochs=new RatisSessionEpochs(client,source.clusterId(),source.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, fake::create, List.of(source))) {
                node.start(); var old=new SchemaChange.Definition(new UUID(90,1),env.outer.operation(100),env.plan.domains().getFirst().directory(),source,target); var newer=new SchemaChange.Definition(new UUID(90,2),env.outer.operation(101),env.plan.domains().getFirst().directory(),source,target);
                var aborted=new RatisSchemaAdministration.Head(SchemaChange.Status.begin(old.header()).abort(),71); var current=new RatisSchemaAdministration.Head(SchemaChange.Status.begin(newer.header()),71);
                var catalogue=fake.machines.get(RaftGroupId.valueOf(c.groupId())); var storeField=RatisRangeStateMachine.class.getDeclaredField("store"); storeField.setAccessible(true); var store=(ReplicatedRangeStore)storeField.get(catalogue);
                var committed=new DistributedCommand(c.groupId(),c.epoch(),c.schemaDigest(),env.outer.operation(102),List.of(
                        new Mutation(Mutation.Kind.PUT,c.directoryColumn(),java.nio.ByteBuffer.allocate(16).putLong(source.domainId().getMostSignificantBits()).putLong(source.domainId().getLeastSignificantBits()).array(),RangeDirectoryCodec.encode(env.plan.domains().getFirst().directory())),
                        new Mutation(Mutation.Kind.PUT,c.directoryColumn(),RatisSchemaAdministration.definitionKey(old.digest()),old.encode()),
                        new Mutation(Mutation.Kind.PUT,c.directoryColumn(),RatisSchemaAdministration.definitionKey(newer.digest()),newer.encode()),
                        new Mutation(Mutation.Kind.PUT,c.directoryColumn(),RatisSchemaAdministration.outcomeKey(old.digest()),aborted.encode()),
                        new Mutation(Mutation.Kind.PUT,c.directoryColumn(),RatisSchemaAdministration.headKey(source.domainId()),current.encode())));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,1),committed)).status()); catalogue.notifyTermIndexUpdated(1,1);
                var data=fake.machines.get(RaftGroupId.valueOf(env.plan.domains().getFirst().directory().ranges().getFirst().groupId())); var field=RatisRangeStateMachine.class.getDeclaredField("schemaAuthority"); field.setAccessible(true); var reader=(RatisRangeStateMachine.SchemaAuthorityReader)field.get(data);
                var observed=reader.read(old.header(),null); assertEquals(SchemaChange.Phase.ABORTED,observed.head().status().phase()); assertArrayEquals(old.header().encode(),observed.head().status().header().encode());
                assertEquals(2,reads.size()); assertFalse(reads.get(0).outcome()); assertTrue(reads.get(1).outcome()); for (var request:reads) { assertArrayEquals(old.header().encode(),request.expectedHeader().encode()); assertNull(request.admissionDigest()); }
                var missing=new SchemaChange.Definition(new UUID(90,3),env.outer.operation(103),env.plan.domains().getFirst().directory(),source,target);
                var failure=assertThrows(IOException.class,() -> reader.read(missing.header(),null)); assertEquals(1,failure.getSuppressed().length);
            }
        }
    }
    private static int configuredProofThreads(RatisProofQueryService service) throws ReflectiveOperationException {
        var field=RatisProofQueryService.class.getDeclaredField("executor"); field.setAccessible(true); return ((java.util.concurrent.ThreadPoolExecutor)field.get(service)).getMaximumPoolSize();
    }
    private static RatisProofQueryService proofLane(RatisRangeStateMachine machine,String name) throws ReflectiveOperationException {
        var field=RatisRangeStateMachine.class.getDeclaredField(name); field.setAccessible(true); return (RatisProofQueryService)field.get(machine);
    }
    private static Object schemaFencingClient(RatisRangeStateMachine machine) throws ReflectiveOperationException {
        for (var field:RatisRangeStateMachine.class.getDeclaredFields()) if (field.getType()==RatisSchemaReplicaClient.class) { field.setAccessible(true); return field.get(machine); }
        throw new NoSuchFieldException("Catalogue schema replica client");
    }
    private static Object topologyAuthority(RatisRangeStateMachine machine) throws ReflectiveOperationException {
        var field=RatisRangeStateMachine.class.getDeclaredField("topologyAuthority"); field.setAccessible(true); return field.get(machine);
    }
    private static Object schemaReplicaService(RatisRangeStateMachine machine) throws ReflectiveOperationException {
        var field=RatisRangeStateMachine.class.getDeclaredField("schemaReplicas"); field.setAccessible(true); return field.get(machine);
    }
    private static Object snapshotHomes(RatisRangeStateMachine machine) throws ReflectiveOperationException {
        var field=RatisRangeStateMachine.class.getDeclaredField("snapshotHomes"); field.setAccessible(true); return field.get(machine);
    }
    /** A separate borrowed home directory can have another data domain and schema while sharing cluster/database identity. */
    static final class SnapshotReader implements AutoCloseable {
        final RatisRoutedClient routed; final RatisSnapshotFences homes; int closes;
        SnapshotReader(RangeDirectory source,UUID cluster,UUID database) {
            UUID column=new UUID(0,3110),group=new UUID(0,3111); byte[] schema=new byte[32]; schema[0]=9;
            var route=new RangeDescriptor(group,group,3,0,null,null,RangeDescriptor.TopologyState.ACTIVE,source.ranges().getFirst().replicas());
            var directory=new RangeDirectory(cluster,database,new UUID(0,3112),17,schema,0,Set.of(column),0,List.of(route));
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID requestedColumn,byte[] key) { assertEquals(column,requestedColumn); return null; }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError("Read-only home fixture"); }
                public void close() { closes++; }
            }); homes=new RatisSnapshotFences(routed,column);
        }
        @Override public void close() throws Exception { routed.close(); }
    }
    static final class Env implements AutoCloseable {
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest(); final RatisSessionEpochsTest.Fixture catalogue;
        final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,4,1,"node-runtime-test");
        final ManualTimer timer=new ManualTimer();
        final Path nodeRoot; final RatisNodePlan plan; final RatisRoutedClient routed; int transportCloses; boolean resourcesTransferred;
        Env(Path root) throws Exception { this(root,false); }
        Env(Path root,boolean localCatalogue) throws Exception { this(root,localCatalogue,new byte[32]); }
        Env(Path root,boolean localCatalogue,byte[] schemaDigest) throws Exception {
            this(root,localCatalogue,schemaDigest,Set.of());
        }
        Env(Path root,boolean localCatalogue,byte[] schemaDigest,Set<UUID> merges) throws Exception {
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
            var directory=new RangeDirectory(outer.cluster,outer.database,new UUID(6,1),1,schemaDigest,1,Set.of(outer.data),0,ranges);
            var remote=new ReplicaDescriptor(new UUID(8,1),new UUID(8,2),new UUID(8,8),"node8","127.0.0.1:4511",ReplicaDescriptor.Role.VOTER);
            plan=new RatisNodePlan(local, incarnation, List.of(new RatisNodePlan.Domain(directory, Map.of(outer.data,"data"), merges)), new RatisNodePlan.Catalogue(outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,localCatalogue ? List.of(remote,new ReplicaDescriptor(local,incarnation,new UUID(9,8),"node9","127.0.0.1:4512",ReplicaDescriptor.Role.VOTER)) : List.of(remote)), List.of(), List.of(), Map.of(), localCatalogue ? 3 : 2);
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError("Timer has not fired"); }
                public void close() { transportCloses++; }
            });
            nodeRoot=Files.createDirectory(root.resolve("node"));
        }
        RatisNodeRuntime.Settings settings() {
            return new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),RatisRangeStateMachine.RetentionLimits.defaults(),
                    Duration.ofSeconds(1),1,2,Duration.ofMillis(100),RatisNodeRuntime.ExportResources.defaults());
        }
        RatisNodeRuntime open(RatisNodeRuntime.Startup startup,FakeServer fake) throws IOException { return open(startup,fake,null); }
        RatisNodeRuntime open(RatisNodeRuntime.Startup startup,FakeServer fake,RatisSnapshotFences snapshotHomes) throws IOException {
            return RatisNodeRuntime.open(plan, nodeRoot, ConfigParser.parseDefault(), catalogue.epochs, Map.of(routed.directory().domainId(), routed), scheduler, timer, () -> 20L, settings(), startup, fake::create, null, snapshotHomes);
        }
        public void close() throws Exception {
            if (resourcesTransferred) catalogue.pool.close();
            else { routed.close(); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); catalogue.close(); }
        }
    }
    /** One borrowed node timer can register both catalogue maintenance and transaction expiry. */
    static final class ManualTimer implements reactor.core.scheduler.Scheduler {
        private final List<Scheduled> tasks=new java.util.concurrent.CopyOnWriteArrayList<>();
        private volatile boolean disposed;
        private static final class Scheduled implements reactor.core.Disposable {
            final Runnable task; volatile boolean cancelled;
            Scheduled(Runnable task) { this.task=task; }
            public void dispose() { cancelled=true; }
            public boolean isDisposed() { return cancelled; }
        }
        long activePeriods() { return disposed ? 0 : tasks.stream().filter(task -> !task.cancelled).count(); }
        void fire() { for (var scheduled:tasks) if (!disposed && !scheduled.cancelled) scheduled.task.run(); }
        public reactor.core.Disposable schedulePeriodically(Runnable task,long initial,long period,java.util.concurrent.TimeUnit unit) {
            if (disposed) throw new java.util.concurrent.RejectedExecutionException("Timer disposed");
            var scheduled=new Scheduled(task); tasks.add(scheduled); return scheduled;
        }
        public reactor.core.Disposable schedule(Runnable task) { throw new UnsupportedOperationException(); }
        public Worker createWorker() { throw new UnsupportedOperationException(); }
        public void dispose() { disposed=true; }
        public boolean isDisposed() { return disposed; }
    }
    static final class FakeServer {
        boolean created,wrongEndpoint,joint,rejectAdd,loseAddReply,skipFirstNoop,allFencedAtClose,failCreate,failStart,exposedBeforeStart; int closes,starts,adds;
        volatile boolean leader=true,ready=true; volatile long term=1;
        RatisNodePlan plan; Path logs; Map<RaftGroupId,RatisRangeStateMachine> machines; RaftServer server;
        final Map<RaftGroupId,RaftStorage> storages=new LinkedHashMap<>();
        final Map<RaftGroupId,RaftGroup> actualGroups=new LinkedHashMap<>();
        final Map<RaftGroupId,org.apache.ratis.server.raftlog.RaftLog> actualLogs=new LinkedHashMap<>();
        RaftServer create(RatisNodePlan plan,Path logs,Map<RaftGroupId,RatisRangeStateMachine> machines) throws IOException {
            created=true; if (failCreate) throw new IOException("fixture build failure"); this.plan=plan; this.logs=logs; this.machines=machines;
            server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> switch (method.getName()) {
                case "getId" -> RaftPeerId.valueOf(plan.peerId()); case "toString" -> "controlled-node-server";
                case "start" -> {
                    starts++; byte[] identity=SessionJournalFiles.read(logs.getParent().resolve("node.identity"),128);
                    exposedBeforeStart=identity.length==33 && identity[0]==1;
                    if (failStart) throw new IOException("fixture start failure");
                    initialize(plan.localGroups().getFirst()); yield null;
                }
                case "getGroupIds" -> List.copyOf(storages.keySet());
                case "getDivision" -> division((RaftGroupId)args[0]);
                case "groupManagement" -> {
                    var request=(GroupManagementRequest)args[0];adds++; if (!rejectAdd) initialize(request.getAdd().getGroup());if(loseAddReply){loseAddReply=false;throw new IOException("Lost added group reply");}
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
            if (storages.containsKey(group.getGroupId())) return;actualGroups.put(group.getGroupId(),group);
            var storage=RaftStorage.newBuilder().setDirectory(logs.resolve(group.getGroupId().getUuid().toString()).toFile()).setOption(RaftStorage.StartupOption.RECOVER)
                    .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                    .setLogCorruptionPolicy(RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build(); storage.initialize(); storages.put(group.getGroupId(),storage);
            var machine=machines.get(group.getGroupId()); machine.initialize(server,group.getGroupId(),storage); if(!skipFirstNoop)machine.notifyTermIndexUpdated(1,0);
        }
        RaftServer.Division division(RaftGroupId id) {
            var group=actualGroups.get(id);if(group==null)throw new IllegalArgumentException("No actual division for "+id);
            var configuration=(RaftConfiguration)Proxy.newProxyInstance(RaftConfiguration.class.getClassLoader(),new Class<?>[]{RaftConfiguration.class},(proxy,method,args) -> {
                if(method.getName().equals("getLogEntryIndex")){var nativeField=RatisRangeStateMachine.class.getDeclaredField("store");nativeField.setAccessible(true);var store=(ReplicatedRangeStore)nativeField.get(machines.get(id));var saved=store.configuration();return saved==null ? -1L : saved.index();}
                var role=(RaftPeerRole)args[0]; var peers=group.getPeers().stream().filter(peer -> peer.getStartupRole()==role).toList();
                if (method.getName().equals("getPreviousPeers")) return joint ? peers : List.of();
                if (method.getName().equals("getCurrentPeers")) return peers.stream().map(peer -> wrongEndpoint ? RaftPeer.newBuilder().setId(peer.getId()).setAddress("127.0.0.1:9999").setStartupRole(peer.getStartupRole()).build() : peer).toList();
                throw new AssertionError(method);
            });
            return (RaftServer.Division)Proxy.newProxyInstance(RaftServer.Division.class.getClassLoader(),new Class<?>[]{RaftServer.Division.class},(proxy,method,args) -> switch (method.getName()) {
                case "getRaftConf" -> configuration; case "getStateMachine" -> machines.get(id);
                case "getGroup" -> group;case "getId" -> server.getId();
                case "getRaftLog" -> actualLogs.computeIfAbsent(id,key->{var log=org.mockito.Mockito.mock(org.apache.ratis.server.raftlog.RaftLog.class);org.mockito.Mockito.when(log.getLastCommittedIndex()).thenAnswer(invocation->machines.get(key).getLastAppliedTermIndex().getIndex());return log;});
                case "getInfo" -> Proxy.newProxyInstance(DivisionInfo.class.getClassLoader(),new Class<?>[]{DivisionInfo.class},(p,m,a) -> switch (m.getName()) {
                    case "isLeader" -> leader; case "isLeaderReady" -> ready; case "getCurrentTerm" -> term;case "getLastAppliedIndex" -> machines.get(id).getLastAppliedTermIndex().getIndex();case "isAlive" -> true;case "getLifeCycleState" -> org.apache.ratis.util.LifeCycle.State.RUNNING; default -> throw new AssertionError(m);
                }); default -> throw new AssertionError(method);
            });
        }
    }
}
