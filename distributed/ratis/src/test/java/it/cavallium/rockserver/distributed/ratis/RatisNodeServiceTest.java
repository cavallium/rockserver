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
        return new RatisNodeManifest(schema,env.plan,new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),
                RatisRangeStateMachine.RetentionLimits.defaults(),Duration.ofSeconds(1),1,2,Duration.ofMillis(100),RatisNodeRuntime.ExportResources.defaults()),3,3);
    }
    RatisNodeService.Resources resources(RatisNodeRuntimeTest.Env env) {
        env.resourcesTransferred=true; return new RatisNodeService.Resources(env.catalogue.client,env.routed,env.scheduler,env.timer);
    }
    void publish(RatisNodeRuntimeTest.Env env,DistributedSchema schema) throws Exception {
        var o=env.outer; var c=new RatisSchemaCatalogue(env.catalogue.client,o.cluster,o.database,o.catalogue,1,new byte[32],o.directoryColumn);
        c.execute(c.prepare(o.operation(1),schema)); var d=env.plan.directory();
        var initial=new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),d.schemaEpoch(),d.schemaDigest(),0,d.columns(),d.routingPrefixBytes(),d.ranges());
        var directory=new RatisCatalogue(env.catalogue.client,o.cluster,o.database,o.catalogue,1,new byte[32],o.directoryColumn);
        directory.execute(directory.prepare(o.operation(2),null,initial)); directory.execute(directory.prepare(o.operation(3),initial,d));
    }
    void initialize(RatisNodeRuntimeTest.Env env,DistributedSchema schema) throws Exception {
        var fake=new RatisNodeRuntimeTest.FakeServer();
        try (var node=RatisNodeRuntime.open(env.plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,env.routed,
                env.scheduler,env.timer,() -> 20L,env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,schema)) {
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
                service.start(); assertEquals(RatisNodeRuntime.State.RUNNING,service.state());
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
                var store=(ReplicatedRangeStore)field.get(fake.machines.values().iterator().next());
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
            var owned=new RatisNodeService.Resources(client,env.routed,env.scheduler,env.timer);
            var failure=assertThrows(IOException.class,() -> RatisNodeService.recover(manifest(env,schema),env.nodeRoot,ConfigParser.parseDefault(),owned,new RatisNodeRuntimeTest.FakeServer()::create));
            assertEquals(1,failure.getSuppressed().length); assertEquals("fixture close failure",failure.getSuppressed()[0].getMessage());
            assertEquals(1,env.catalogue.closes.get()); assertEquals(1,env.transportCloses); assertTrue(env.timer.isDisposed());
            assertThrows(java.util.concurrent.RejectedExecutionException.class,() -> env.scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION).execute(() -> fail("Retired scheduler executed work")));
        }
    }

}
