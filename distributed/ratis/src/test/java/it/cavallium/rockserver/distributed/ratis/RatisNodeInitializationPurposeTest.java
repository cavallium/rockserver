package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** The node lock binds one initialization purpose before native or Raft exposure. */
class RatisNodeInitializationPurposeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void initialPlacementCannotAcquireGenesisOrRestorePurposeOnResume()throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root)) {
            var failed=new RatisNodeRuntimeTest.FakeServer();failed.failCreate=true;
            assertThrows(IOException.class,()->env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,failed));
            byte[] original=Files.readAllBytes(env.nodeRoot.resolve("node.identity"));
            for(var purpose:List.of(RatisNodeRuntime.Startup.AUTHORIZED_GENESIS,RatisNodeRuntime.Startup.AUTHORIZED_RESTORE_TARGET)) {
                var server=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->env.open(purpose,server));assertFalse(server.created);
                assertArrayEquals(original,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
            }
            try(var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new RatisNodeRuntimeTest.FakeServer())){node.start();}
            try(var node=env.open(RatisNodeRuntime.Startup.RECOVER,new RatisNodeRuntimeTest.FakeServer())){node.start();}
            var identity=RatisNodeIdentity.decode(SessionJournalFiles.read(env.nodeRoot.resolve("node.identity"),RatisNodeIdentity.MAX_BYTES),env.plan,env.settings().resources().nativeExtents());
            assertEquals(RatisNodeIdentity.Purpose.INITIAL_PLACEMENT,identity.purpose());assertTrue(identity.genesisDomains().isEmpty());
        }
    }
    @Test void genesisScopeIsForcedBeforeFactoryAndSurvivesInterruptedAndColdStartup()throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root)) {
            var original=env.plan.domains().getFirst();var d=original.directory();
            var directory=new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),d.schemaEpoch(),d.schemaDigest(),0,d.columns(),d.routingPrefixBytes(),d.ranges());
            var domain=new RatisNodePlan.Domain(directory,original.columns(),original.merges());
            var plan=new RatisNodePlan(env.plan.nodeId(),env.plan.nodeIncarnation(),List.of(domain),env.plan.catalogue(),env.plan.homes(),List.of(),env.plan.streamAddresses(),env.plan.maxLocalGroups(),env.plan.pools().values(),env.plan.domainPools(),env.plan.groupPools());
            try(var route=new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("No USER traffic");}
                public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("No USER traffic");}
            })) {
                RatisNodeRuntime.ServerFactory failed=(configuration,logs,machines)->{
                    var saved=RatisNodeIdentity.decode(SessionJournalFiles.read(env.nodeRoot.resolve("node.identity"),RatisNodeIdentity.MAX_BYTES),plan,env.settings().resources().nativeExtents());
                    assertEquals(1,saved.phase());assertEquals(RatisNodeIdentity.Purpose.GENESIS,saved.purpose());
                    for(var range:directory.ranges())saved.requireGenesisScope(directory,range,range.replicas().stream().filter(r->r.nodeId().equals(plan.nodeId())).findFirst().orElseThrow());
                    throw new IOException("Interrupted before factory exposure");
                };
                assertThrows(IOException.class,()->open(env,plan,route,RatisNodeRuntime.Startup.AUTHORIZED_GENESIS,failed));
                assertThrows(IOException.class,()->open(env,plan,route,RatisNodeRuntime.Startup.RECOVER,new RatisNodeRuntimeTest.FakeServer()::create));
                for(var purpose:List.of(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,RatisNodeRuntime.Startup.AUTHORIZED_RESTORE_TARGET))assertThrows(IOException.class,()->open(env,plan,route,purpose,new RatisNodeRuntimeTest.FakeServer()::create));
                try(var node=open(env,plan,route,RatisNodeRuntime.Startup.AUTHORIZED_GENESIS,new RatisNodeRuntimeTest.FakeServer()::create)){node.start();}
                byte[] exposed=Files.readAllBytes(env.nodeRoot.resolve("node.identity"));
                try(var node=open(env,plan,route,RatisNodeRuntime.Startup.RECOVER,new RatisNodeRuntimeTest.FakeServer()::create)){node.start();}
                assertArrayEquals(exposed,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
            }
        }
    }
    private static RatisNodeRuntime open(RatisNodeRuntimeTest.Env env,RatisNodePlan plan,RatisRoutedClient route,RatisNodeRuntime.Startup startup,RatisNodeRuntime.ServerFactory factory)throws IOException {
        return RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(route.directory().domainId(),route),env.scheduler,env.timer,()->20L,env.settings(),startup,factory);
    }
}
