package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisNativeCachePreflightTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static RatisNativePoolSpec spec(UUID id,String config){var base=RatisPoolFixtures.spec(id,false);return new RatisNativePoolSpec(id,config.getBytes(StandardCharsets.UTF_8),base.maximumGroups(),base.images(),base.retention(),base.pins(),base.replicas(),base.resources(),base.maximumDurableMetadataBytes());}
    private static RatisNodeRuntime.Settings settings(RatisNodeRuntime.Settings base,long bytes){var r=base.resources();return new RatisNodeRuntime.Settings(base.images(),base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),base.exports(),new RatisNodeRuntime.ResourceLimits(r.node(),r.pool(),r.maximumSourceImageDiskBytes(),r.maximumDurableMetadataBytes(),r.maximumOperatorCacheBytes(),bytes,RatisCommittedApplyDispatcher.Policy.defaults(),RatisDiskRunway.Policy.defaults()));}
    private static RatisNodePlan plan(RatisNodeMultiDomainTest.Fixture f,RatisNativePoolSpec a,RatisNativePoolSpec b){return new RatisNodePlan(f.plan.nodeId(),f.plan.nodeIncarnation(),f.plan.domains(),f.plan.catalogue(),f.plan.homes(),f.plan.imports(),f.plan.streamAddresses(),f.plan.maxLocalGroups(),List.of(a,b),Map.of(f.first.domainId(),a.id(),f.second.domainId(),b.id()),Map.of(f.plan.catalogue().groupId(),a.id()));}
    @Test void twoFrozenPoolCapacitiesRejectBeforeOwnershipThenExactBoundaryRunsAndColdRecovers()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        try(var f=helper.new Fixture()) {
            String config="database.global {block-cache=8MiB,write-buffer-manager=4MiB,block-caches=[{name=unused,size=2MiB}]}";
            var a=spec(new UUID(997,1),config);var b=spec(new UUID(997,2),config);var plan=plan(f,a,b);long required=36L*1024*1024;assertEquals(required,a.nativeCacheCapacity().totalBytes()+b.nativeCacheCapacity().totalBytes());assertEquals(a.nativeCacheCapacity().totalBytes(),RatisNativePoolSpec.decode(a.encode()).nativeCacheCapacity().totalBytes());var denied=settings(f.settings(),required-1);var fake=new RatisNodeRuntimeTest.FakeServer();
            // Null constructor dependencies prove rejection precedes even runtime construction and HOME connection.
            var failure=assertThrows(IOException.class,()->RatisNodeRuntime.open(plan,f.env.nodeRoot,ConfigParser.parseDefault(),null,null,null,null,null,denied,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,f.schemas,null,null,directory->{throw new AssertionError("HOME connection before preflight");}));assertTrue(failure.getMessage().contains("native cache/WBM ceiling"));assertNull(fake.machines);assertFalse(fake.created);for(var name:List.of("node.lock","node.identity","metadata-claims","native"))assertFalse(Files.exists(f.env.nodeRoot.resolve(name)));
            var accepted=settings(f.settings(),required);var server=new RatisNodeRuntimeTest.FakeServer();
            try(var node=RatisNodeRuntime.open(plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,f.clients,f.env.scheduler,f.env.timer,()->20L,accepted,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server::create,f.schemas)) {node.start();for(var schema:f.schemas){var range=plan.directory(schema.domainId()).ranges().getFirst();var store=f.store(server,range.groupId());var command=new DistributedCommand(range.groupId(),range.rangeEpoch(),schema.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,schema.columnIds().iterator().next(),new byte[]{1},new byte[]{7})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),command)).status());}}
            var manifest=RatisNodeManifest.decode(RatisNodeManifest.encode(new RatisNodeManifest(f.schemas,plan,accepted,3,3)));assertEquals(required,manifest.settings().resources().maximumNativeCacheBytes());var cold=new RatisNodeRuntimeTest.FakeServer();var incoming=new ConfigParser();incoming.addSourceBytes("database.global.block-cache=64GiB".getBytes(StandardCharsets.UTF_8));
            try(var node=RatisNodeRuntime.open(manifest.plan(),f.env.nodeRoot,incoming.parse(),f.env.catalogue.epochs,f.clients,f.env.scheduler,f.env.timer,()->20L,manifest.settings(),RatisNodeRuntime.Startup.RECOVER,cold::create,f.schemas)){node.start();for(var schema:f.schemas){var range=plan.directory(schema.domainId()).ranges().getFirst();assertArrayEquals(new byte[]{7},f.store(cold,range.groupId()).readCurrentPoint(schema.columnIds().iterator().next(),new byte[]{1}));}}
        }
    }
    @Test void aggregateOverflowRejectsBeforeRuntimeConstruction()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;try(var f=helper.new Fixture()){var a=spec(new UUID(997,3),"database.global {block-cache=4EiB,write-buffer-manager=0B}");var b=spec(new UUID(997,4),"database.global {block-cache=4EiB,write-buffer-manager=0B}");var plan=plan(f,a,b);var failure=assertThrows(IOException.class,()->RatisNodeRuntime.open(plan,f.env.nodeRoot,ConfigParser.parseDefault(),null,null,null,null,null,f.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new RatisNodeRuntimeTest.FakeServer()::create));assertTrue(failure.getMessage().contains("capacity overflow"));assertFalse(Files.exists(f.env.nodeRoot.resolve("node.lock")));}
    }
}
