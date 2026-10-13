package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedOperatorArtifacts;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisOperatorCacheEnvelopeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void declaredCacheEnvelopeRejectsBeforeNativeOpenOrServerCreation() throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root)) {
            var base=env.settings();var resources=base.resources();
            long required=env.plan.pools().size()*DistributedOperatorArtifacts.MAX_CACHE_BYTES;
            var limits=new RatisNodeRuntime.ResourceLimits(resources.node(),resources.pool(),resources.maximumSourceImageDiskBytes(),resources.maximumDurableMetadataBytes(),required-1,resources.maximumNativeCacheBytes(),RatisCommittedApplyDispatcher.Policy.defaults(),RatisDiskRunway.Policy.defaults());
            var settings=new RatisNodeRuntime.Settings(base.images(),base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),base.exports(),limits);
            var server=new RatisNodeRuntimeTest.FakeServer();
            var failure=assertThrows(IOException.class,()->RatisNodeRuntime.open(env.plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server::create));
            assertTrue(failure.getMessage().contains("operator cache ceiling"));assertNull(server.machines);
            for(var pool:env.plan.pools().values())assertFalse(Files.exists(pool.nativePath(env.nodeRoot).resolve("CURRENT")));
            var exact=new RatisNodeRuntime.ResourceLimits(resources.node(),resources.pool(),resources.maximumSourceImageDiskBytes(),resources.maximumDurableMetadataBytes(),required,resources.maximumNativeCacheBytes(),RatisCommittedApplyDispatcher.Policy.defaults(),RatisDiskRunway.Policy.defaults());
            var accepted=new RatisNodeRuntime.Settings(base.images(),base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),base.exports(),exact);
            try(var node=RatisNodeRuntime.open(env.plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer,()->20L,accepted,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new RatisNodeRuntimeTest.FakeServer()::create)){node.start();}
        }
    }
}
