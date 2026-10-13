package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.*;
import java.util.*;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisCommittedApplyGroupCapacityTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static RatisNodeRuntime.Settings limits(RatisNodeRuntime.Settings base,int nodeGroups,int poolGroups){
        var r=base.resources();var p=r.committedApply();var n=p.node();var b=p.pool();var policy=new RatisCommittedApplyDispatcher.Policy(new RatisCommittedApplyDispatcher.Limits(n.read(),n.ordinary(),n.protectedLane(),nodeGroups),new RatisCommittedApplyDispatcher.Limits(b.read(),b.ordinary(),b.protectedLane(),poolGroups));
        return new RatisNodeRuntime.Settings(base.images(),base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),base.exports(),new RatisNodeRuntime.ResourceLimits(r.node(),r.pool(),r.maximumSourceImageDiskBytes(),r.maximumDurableMetadataBytes(),r.maximumOperatorCacheBytes(),r.maximumNativeCacheBytes(),policy,RatisDiskRunway.Policy.defaults()));
    }
    @Test void initialNodeAndPoolCeilingsRejectBeforeNodeOwnershipOrNativeOpen()throws Exception {
        for(boolean node:List.of(true,false))try(var env=new RatisNodeRuntimeTest.Env(root.resolve("initial-"+node))){
            var fake=new RatisNodeRuntimeTest.FakeServer();var settings=limits(env.settings(),node?1:16,node?16:1);
            assertThrows(IllegalArgumentException.class,()->RatisNodeRuntime.open(env.plan,env.nodeRoot,it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,null));
            try(var files=Files.list(env.nodeRoot)){assertEquals(0,files.count());}assertNull(fake.machines,"Server factory must not run before group capacity admission");
        }
    }
    @Test void seedNodeAndPoolCeilingsRejectBeforeAnyNativeBirthOrExposure()throws Exception {
        for(boolean node:List.of(true,false)){
            var helper=new RatisTopologySeedRuntimeTest();helper.root=root.resolve("seed-"+node);
            try(var fixture=helper.new Fixture(true,16,base->limits(base,node?1:16,node?16:1))){
                var before=fixture.pool().persistedRangeGroups(16);var storageCount=fixture.server.storages.size();
                assertThrows(IOException.class,()->fixture.node.seedTopology(fixture.request()));
                assertNull(fixture.pool().topologySeed(fixture.definition.intentId()));assertEquals(before,fixture.pool().persistedRangeGroups(16));assertEquals(storageCount,fixture.server.storages.size());
                assertFalse(Files.exists(fixture.env.nodeRoot.resolve("topology-exposure").resolve(fixture.definition.targets().getFirst().groupId().toString())));
            }
        }
    }
}
