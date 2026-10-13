package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Owned service resources with native recovery and controlled Ratis interfaces; production launcher is never run. */
class RatisNodeServiceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    DistributedSchema schema() {
        var o=new RatisSessionEpochsTest(); return new DistributedSchema(o.cluster,o.database,new UUID(6,1),1,0,List.of(
                new DistributedSchema.Column(71,o.data,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
    }
    RatisNodeManifest manifest(RatisNodeRuntimeTest.Env env,DistributedSchema schema) {
        return new RatisNodeManifest(List.of(schema),env.plan,new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),
                RatisRangeStateMachine.RetentionLimits.defaults(),Duration.ofSeconds(1),1,2,Duration.ofMillis(100),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults()),3,3);
    }
    RatisNodeService.Resources resources(RatisNodeRuntimeTest.Env env) {
        env.resourcesTransferred=true; return new RatisNodeService.Resources(env.catalogue.client,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer);
    }
    @Test void recoveryUsesPersistedReplicaBudgetsAndBorrowedPolicy() throws Exception {
        var schema=schema();
        try (var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            initialize(env,schema);
            var original=manifest(env,schema); var limits=new RatisSchemaReplicaService.Limits(3,96L*1024*1024,80L*1024*1024,5,7,0);
            var configured=RatisNodeManifest.decode(RatisNodeManifest.encode(new RatisNodeManifest(List.of(schema),env.plan,original.settings(),3,3,limits)));
            class Policy implements RatisSchemaReplicaService.OperatorPolicy,AutoCloseable {
                int closes;
                public it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract admit(DistributedSchema.Operator operator) throws IOException { throw new IOException("This node fixture has no approved operator"); }
                public void close() { closes++; }
            }
            var policy=new Policy(); var fake=new RatisNodeRuntimeTest.FakeServer();
            try (var service=RatisNodeService.recover(configured,env.nodeRoot,ConfigParser.parseDefault(),resources(env),fake::create,policy)) {
                var nodeField=RatisNodeService.class.getDeclaredField("node"); nodeField.setAccessible(true); var node=nodeField.get(service);
                var replicas=RatisPoolFixtures.schemaService((RatisNodeRuntime)node); assertNotNull(replicas);
                var limitsField=RatisSchemaReplicaService.class.getDeclaredField("limits"); limitsField.setAccessible(true); assertEquals(RatisPoolFixtures.pools().getFirst().replicas(),limitsField.get(replicas));var admissionField=RatisSchemaReplicaService.class.getDeclaredField("admission");admissionField.setAccessible(true);var nodeAdmission=admissionField.get(replicas);var nodeLimitsField=RatisSchemaReplicaAdmission.class.getDeclaredField("limits");nodeLimitsField.setAccessible(true);assertEquals(limits,nodeLimitsField.get(nodeAdmission));
                var policyField=RatisSchemaReplicaService.class.getDeclaredField("policy"); policyField.setAccessible(true); assertSame(policy,policyField.get(replicas));
                service.start();
            }
            assertEquals(0,policy.closes); assertEquals(1,fake.closes); assertTrue(env.timer.isDisposed());
        }
    }

    @Test void frontendDrainFailuresRetainBackendAndNodeForExactCloseRetry() throws Exception {
        var schema=schema();
        try(var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            initialize(env,schema);publish(env,schema);var fake=new RatisNodeRuntimeTest.FakeServer();
            try(var service=RatisNodeService.recover(manifest(env,schema),env.nodeRoot,ConfigParser.parseDefault(),resources(env),fake::create)) {
                service.start();var backend=org.mockito.Mockito.mock(RatisDatabaseBackendFactory.class);
                var connection=org.mockito.Mockito.mock(RatisRocksDBConnection.class);
                org.mockito.Mockito.when(backend.open(org.mockito.ArgumentMatchers.any())).thenReturn(connection);
                org.mockito.Mockito.when(backend.exports()).thenReturn(org.mockito.Mockito.mock(RatisExportBackend.class));var published=backend.exports();
                var exportDrains=new java.util.concurrent.atomic.AtomicInteger();org.mockito.Mockito.doAnswer(call->{if(exportDrains.incrementAndGet()==1)throw new IOException("Actual aggregate export work retained");return null;}).when(backend).drainExports();
                var backendCloses=new java.util.concurrent.atomic.AtomicInteger();
                org.mockito.Mockito.doAnswer(call -> {if(backendCloses.incrementAndGet()==1)throw new IOException("Backend actual work retained");return null;}).when(backend).close();
                var listenerCloses=new java.util.concurrent.atomic.AtomicInteger();var starts=new java.util.concurrent.atomic.AtomicInteger();
                var listener=new it.cavallium.rockserver.core.server.Server(connection) {
                    public void start(){starts.incrementAndGet();}
                    public void close() throws IOException {if(listenerCloses.incrementAndGet()==1)throw new IOException("Listener work retained");}
                };
                service.startFrontend(() -> backend,(ignored,api) -> {assertSame(published,api);return listener;},java.net.URI.create("grpc://localhost:5333"));
                assertEquals(1,starts.get());assertThrows(IOException.class,service::close);
                assertEquals(0,exportDrains.get());assertEquals(0,backendCloses.get());assertEquals(0,fake.closes);assertEquals(0,env.catalogue.closes.get());
                assertThrows(IOException.class,service::close);assertEquals(2,listenerCloses.get());assertEquals(1,exportDrains.get());assertEquals(0,backendCloses.get());assertEquals(0,fake.closes);assertFalse(env.timer.isDisposed());
                assertThrows(IOException.class,service::close);assertEquals(2,exportDrains.get());assertEquals(1,backendCloses.get());assertEquals(0,fake.closes);assertFalse(env.timer.isDisposed());
                service.close();assertEquals(2,listenerCloses.get());assertEquals(2,exportDrains.get());assertEquals(2,backendCloses.get());assertEquals(1,fake.closes);
            }
        }
    }
    @Test void backendOpenKeepsNodeResourcesOwnedUntilTheActualAttemptReturns() throws Exception {
        var schema=schema();
        try(var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest());var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            initialize(env,schema);publish(env,schema);var fake=new RatisNodeRuntimeTest.FakeServer();
            try(var service=RatisNodeService.recover(manifest(env,schema),env.nodeRoot,ConfigParser.parseDefault(),resources(env),fake::create)) {
                service.start();var backend=org.mockito.Mockito.mock(RatisDatabaseBackendFactory.class);
                var connection=org.mockito.Mockito.mock(RatisRocksDBConnection.class);
                var backendCloses=new java.util.concurrent.atomic.AtomicInteger();
                org.mockito.Mockito.doAnswer(call -> {backendCloses.incrementAndGet();return null;}).when(backend).close();
                var entered=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);
                org.mockito.Mockito.when(backend.open(org.mockito.ArgumentMatchers.any())).thenAnswer(call -> {
                    entered.countDown();if(!release.await(5,java.util.concurrent.TimeUnit.SECONDS))throw new IOException("Fixture open timed out");return connection;
                });
                var opening=executor.submit(() -> {service.startFrontend(() -> backend,(ignored,exports) -> new it.cavallium.rockserver.core.server.Server(connection){public void start(){}},java.net.URI.create("grpc://localhost:5333"));return null;});
                try {
                    assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS));assertThrows(IOException.class,service::close);
                    assertEquals(0,backendCloses.get());assertEquals(0,fake.closes);assertFalse(env.timer.isDisposed());
                } finally {release.countDown();}
                opening.get(5,java.util.concurrent.TimeUnit.SECONDS);service.close();org.mockito.Mockito.verify(backend).close();assertEquals(1,fake.closes);
            }
        }
    }
    @Test void failedListenerStartRetainsConstructedOwnersUntilServiceClose() throws Exception {
        var schema=schema();
        try(var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            initialize(env,schema);publish(env,schema);var fake=new RatisNodeRuntimeTest.FakeServer();
            try(var service=RatisNodeService.recover(manifest(env,schema),env.nodeRoot,ConfigParser.parseDefault(),resources(env),fake::create)) {
                service.start();var backend=org.mockito.Mockito.mock(RatisDatabaseBackendFactory.class);
                var connection=org.mockito.Mockito.mock(RatisRocksDBConnection.class);
                org.mockito.Mockito.when(backend.open(org.mockito.ArgumentMatchers.any())).thenReturn(connection);
                var closed=new java.util.concurrent.atomic.AtomicInteger();
                var listener=new it.cavallium.rockserver.core.server.Server(connection) {
                    public void start() throws IOException {throw new IOException("Listener start failed");}
                    public void close(){closed.incrementAndGet();}
                };
                assertThrows(IOException.class,() -> service.startFrontend(() -> backend,(ignored,exports) -> listener,java.net.URI.create("thrift://localhost:5333")));
                org.mockito.Mockito.verify(backend,org.mockito.Mockito.never()).close();assertEquals(0,fake.closes);
                service.close();assertEquals(1,closed.get());org.mockito.Mockito.verify(backend).close();assertEquals(1,fake.closes);
            }
        }
    }

    void publish(RatisNodeRuntimeTest.Env env,DistributedSchema schema) throws Exception {
        var o=env.outer; var c=new RatisSchemaCatalogue(env.catalogue.client,o.cluster,o.database,o.catalogue,1,new byte[32],o.directoryColumn);
        c.execute(c.prepare(o.operation(1),schema)); var d=env.plan.directory(env.routed.directory().domainId());
        var initial=new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),d.schemaEpoch(),d.schemaDigest(),0,d.columns(),d.routingPrefixBytes(),d.ranges());
        var directory=new RatisCatalogue(env.catalogue.client,o.cluster,o.database,o.catalogue,1,new byte[32],o.directoryColumn);
        directory.execute(directory.prepare(o.operation(2),null,initial)); directory.execute(directory.prepare(o.operation(3),initial,d));
    }
    void initialize(RatisNodeRuntimeTest.Env env,DistributedSchema schema) throws Exception {
        var fake=new RatisNodeRuntimeTest.FakeServer();
        try (var node=RatisNodeRuntime.open(env.plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, () -> 20L, env.settings(), RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT, fake::create, List.of(schema))) {
            node.start();
        }
    }
    @Test void establishedNativeRecoveryOwnedCloseAndFreshCatalogueChecks() throws Exception {
        var schema=schema();
        try (var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            initialize(env,schema);
            publish(env,schema); var fake=new RatisNodeRuntimeTest.FakeServer(); var manifest=manifest(env,schema);
            try (var service=RatisNodeService.recover(manifest,env.nodeRoot,ConfigParser.parseDefault(),resources(env),fake::create)) {
                assertEquals(RatisNodeRuntime.State.OPEN,service.state()); assertThrows(IOException.class,service::verifyCatalogue);
                var unopened=service.readiness();assertFalse(unopened.ready());
                assertEquals(env.plan.localGroups().size(),unopened.groups().size());
                assertTrue(unopened.issues().stream().anyMatch(issue -> issue.reason()==RatisNodeReadiness.Reason.NODE_NOT_RUNNING));
                service.start(); assertEquals(RatisNodeRuntime.State.RUNNING,service.state());
                var operational=service.readiness();assertEquals(RatisNodeRuntime.State.RUNNING,operational.state());
                assertEquals(env.plan.localGroups().size(),operational.groups().size());
                assertFalse(operational.ready(),"A started fixture without current-term Raft/native continuity cannot certify readiness");
                assertThrows(IOException.class,service::start); assertEquals(RatisNodeRuntime.State.RUNNING,service.state()); service.verifyCatalogue();
                env.catalogue.missingSchema=true; assertThrows(IOException.class,service::verifyCatalogue);
                assertEquals(RatisNodeRuntime.State.RUNNING,service.state(),"Local running state does not cache catalogue readiness");
                env.catalogue.missingSchema=false; service.verifyCatalogue(); assertFalse(env.timer.isDisposed());
            }
            assertEquals(1,env.catalogue.closes.get()); assertEquals(1,env.transportCloses); assertTrue(env.timer.isDisposed()); assertEquals(1,fake.closes);
        }
    }
    @Test void failedNativeDrainRetainsAllDependenciesUntilRetryableClose() throws Exception {
        var schema=schema();
        try (var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            initialize(env,schema);
            var fake=new RatisNodeRuntimeTest.FakeServer();
            try (var service=RatisNodeService.recover(manifest(env,schema),env.nodeRoot,ConfigParser.parseDefault(),resources(env),fake::create)) {
                service.start(); var field=RatisRangeStateMachine.class.getDeclaredField("store"); field.setAccessible(true);
                var store=(ReplicatedRangeStore)field.get(fake.machines.get(org.apache.ratis.protocol.RaftGroupId.valueOf(env.plan.directory(schema.domainId()).ranges().getFirst().groupId())));
                try (var pin=store.capture()) {
                    assertThrows(IOException.class,service::close); assertEquals(RatisNodeRuntime.State.STOPPING,service.state());
                    assertEquals(0,env.catalogue.closes.get()); assertEquals(0,env.transportCloses); assertFalse(env.timer.isDisposed());
                    assertNull(pin.get(env.outer.data,new byte[]{1})); assertThrows(IOException.class,service::verifyCatalogue);
                }
                service.close(); service.close(); assertEquals(RatisNodeRuntime.State.CLOSED,service.state());
                assertEquals(1,env.catalogue.closes.get()); assertEquals(1,env.transportCloses); assertTrue(env.timer.isDisposed()); assertEquals(1,fake.closes);
            }
        }
    }
    @Test void recoveryLauncherCannotCreateIdentityAndFailedOpenRetiresOwnedResources() throws Exception {
        var schema=schema();
        try (var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            var manifest=manifest(env,schema); var fake=new RatisNodeRuntimeTest.FakeServer();
            assertThrows(IOException.class,() -> RatisNodeService.recover(manifest,env.nodeRoot,ConfigParser.parseDefault()));
            assertFalse(Files.exists(env.nodeRoot.resolve("node.identity"))); assertFalse(fake.created);
            assertThrows(IOException.class,() -> RatisNodeService.recover(manifest,env.nodeRoot,ConfigParser.parseDefault(),resources(env),fake::create));
            assertEquals(1,env.catalogue.closes.get()); assertEquals(1,env.transportCloses); assertTrue(env.timer.isDisposed()); assertFalse(fake.created);
        }
    }
    @Test void ownedLauncherRejectsPersistedIdentityBindingAndRetiresResources() throws Exception {
        var schema=schema();
        try (var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            initialize(env,schema);
            Path identity=env.nodeRoot.resolve("node.identity"); byte[] changed=SessionJournalFiles.read(identity,128); changed[5]^=1;
            SessionJournalFiles.write(identity,changed,128);
            var fake=new RatisNodeRuntimeTest.FakeServer();
            assertThrows(IOException.class,() -> RatisNodeService.recover(manifest(env,schema),env.nodeRoot,ConfigParser.parseDefault(),resources(env),fake::create));
            assertFalse(fake.created); assertEquals(1,env.catalogue.closes.get()); assertEquals(1,env.transportCloses); assertTrue(env.timer.isDisposed());
        }
    }
    @Test void failedOpenRetiresIndependentResourcesEvenIfCatalogueCloseFails() throws Exception {
        var schema=schema();
        try (var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            env.resourcesTransferred=true;
            var client=(org.apache.ratis.client.RaftClient)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),
                    new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args) -> {
                        if (method.getName().equals("close")) { env.catalogue.client.close(); throw new IOException("fixture close failure"); }
                        try { return method.invoke(env.catalogue.client,args); }
                        catch (java.lang.reflect.InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var owned=new RatisNodeService.Resources(client,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer);
            var failure=assertThrows(IOException.class,() -> RatisNodeService.recover(manifest(env,schema),env.nodeRoot,ConfigParser.parseDefault(),owned,new RatisNodeRuntimeTest.FakeServer()::create));
            assertEquals(1,failure.getSuppressed().length); assertEquals("fixture close failure",failure.getSuppressed()[0].getMessage());
            assertEquals(1,env.catalogue.closes.get()); assertEquals(1,env.transportCloses); assertTrue(env.timer.isDisposed());
            assertThrows(java.util.concurrent.RejectedExecutionException.class,() -> env.scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION).execute(() -> fail("Retired scheduler executed work")));
        }
    }

}
