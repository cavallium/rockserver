package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Genesis capability closure only. Voting/activation authority is exercised separately. */
class RatisNodeGenesisObservationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisNodeRuntimeTest.Env env;
        final RatisNodePlan plan;
        final RangeDirectory directory;
        final Map<UUID,RatisRoutedClient> routes;
        final AtomicInteger views=new AtomicInteger();
        final RaftClient catalogue;
        DatabaseDomainRoster roster;boolean published,unavailable;
        Fixture(boolean multiple)throws Exception {
            env=new RatisNodeRuntimeTest.Env(root);var original=env.plan.domains().getFirst();var d=original.directory();directory=new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),d.schemaEpoch(),d.schemaDigest(),0,d.columns(),d.routingPrefixBytes(),d.ranges());
            var domains=new ArrayList<RatisNodePlan.Domain>();domains.add(new RatisNodePlan.Domain(directory,original.columns(),original.merges()));var pools=new HashMap<>(env.plan.domainPools());
            if(multiple){var column=new UUID(12050,2);var group=new UUID(12050,3);var domain=new UUID(12050,4);var range=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,directory.ranges().getFirst().replicas());var other=new RangeDirectory(directory.clusterId(),directory.databaseId(),domain,1,new byte[32],0,Set.of(column),0,List.of(range));domains.add(new RatisNodePlan.Domain(other,Map.of(column,"second"),Set.of()));pools.put(domain,RatisPoolFixtures.ID);}
            plan=new RatisNodePlan(env.plan.nodeId(),env.plan.nodeIncarnation(),domains,env.plan.catalogue(),env.plan.homes(),List.of(),env.plan.streamAddresses(),env.plan.maxLocalGroups()+(multiple?1:0),env.plan.pools().values(),pools,env.plan.groupPools());
            var routed=new HashMap<UUID,RatisRoutedClient>();for(var domain:domains)routed.put(domain.domainId(),new RatisRoutedClient(domain.directory(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("No USER read");}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("No USER write");}}));routes=Map.copyOf(routed);
            var binding=plan.catalogue();var cut=new ApplyPosition(1,10);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                assertEquals("sendReadOnly",method.getName());if(unavailable)throw new IOException("CAT unavailable");byte[] query=((Message)args[0]).getContent().toByteArray();byte[] wire=switch(ScopedTransactionRpc.kind(query,ScopedTransactionRpc.MAX_REQUEST_BYTES)){
                    case RatisCatalogueViewRpc.KIND->{views.incrementAndGet();var request=RatisCatalogueViewRpc.request(query);yield RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(request,cut,published?plan.directory(request.domain()):null,roster));}
                    case RatisRecoveryLineageRpc.KIND->RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(RatisRecoveryLineageRpc.request(query),cut,null));
                    default->throw new AssertionError("No recovery DATA request");
                };
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("controlled-genesis")).setGroupId(RaftGroupId.valueOf(binding.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(wire))).build();
            });
            catalogue=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(binding.groupId());case "io"->io;case "close"->null;default->throw new AssertionError(method);});
        }
        RatisNodeRuntime open(RatisNodeRuntime.Startup startup)throws Exception {
            var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,routes,env.scheduler,env.timer,()->20L,env.settings(),startup,new RatisNodeRuntimeTest.FakeServer()::create);
            node.relocationTransport(range->{assertEquals(plan.catalogue().groupId(),range.groupId());return catalogue;},(range,owner)->{throw new AssertionError("Genesis observation grants no DATA source call");});node.start();return node;
        }
        RatisNodeIdentity saved()throws IOException{return RatisNodeIdentity.decode(SessionJournalFiles.read(env.nodeRoot.resolve("node.identity"),RatisNodeIdentity.MAX_BYTES),plan,env.settings().resources().nativeExtents());}
        void issue(){roster=DatabaseDomainRoster.initial(plan.clusterId(),plan.databaseId(),List.of(directory.domainId()));}
        public void close()throws Exception {for(var route:routes.values())route.close();env.close();}
    }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void rosterIssuanceClosesOriginalScopeEvenBeforeItsDirectoryAndSurvivesColdRead(boolean published)throws Exception {
        try(var f=new Fixture(false)) {
            try(var node=f.open(RatisNodeRuntime.Startup.AUTHORIZED_GENESIS)) {
                assertTrue(f.saved().genesisOpen(f.directory.domainId()));f.issue();f.published=published;
                if(published)node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);else assertThrows(ExecutionException.class,()->node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS));
                assertFalse(f.saved().genesisOpen(f.directory.domainId()));assertFalse(field(node,"nodeIdentity",RatisNodeIdentity.class).genesisOpen(f.directory.domainId()));assertEquals(2,f.saved().phase());
            }
            f.unavailable=true;
            try(var node=f.open(RatisNodeRuntime.Startup.RECOVER)){assertThrows(ExecutionException.class,()->node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS));assertFalse(f.saved().genesisOpen(f.directory.domainId()));}
        }
    }
    @Test void oneAtomicRosterClosesEveryOriginalScopeBeforeAbsentFirstDirectoryStopsDiscovery()throws Exception {
        try(var f=new Fixture(true);var node=f.open(RatisNodeRuntime.Startup.AUTHORIZED_GENESIS)) {
            var writes=new AtomicInteger();RatisTopologySeedRuntimeTest.field(node,"identityJournal",new RatisConfiguredRecoveryTest.Forwarding(){@Override public void write(Path path,byte[] body,int maximum)throws IOException{writes.incrementAndGet();super.write(path,body,maximum);}});
            var domains=f.plan.domains().stream().map(RatisNodePlan.Domain::domainId).toList();f.roster=DatabaseDomainRoster.initial(f.plan.clusterId(),f.plan.databaseId(),domains);
            assertThrows(ExecutionException.class,()->node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS));assertEquals(1,f.views.get());assertEquals(1,writes.get());
            for(var domain:domains){assertFalse(f.saved().genesisOpen(domain));assertFalse(field(node,"nodeIdentity",RatisNodeIdentity.class).genesisOpen(domain));}
        }
    }
    @Test void unissuedOrUnavailableObservationDoesNotInventAnIssuanceReceipt()throws Exception {
        try(var f=new Fixture(false);var node=f.open(RatisNodeRuntime.Startup.AUTHORIZED_GENESIS)) {
            byte[] original=Files.readAllBytes(f.env.nodeRoot.resolve("node.identity"));
            for(int mode=0;mode<3;mode++) {
                f.unavailable=mode==2;f.roster=mode==1?DatabaseDomainRoster.initial(f.plan.clusterId(),f.plan.databaseId(),List.of(new UUID(12050,1))):null;
                assertThrows(ExecutionException.class,()->node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS));assertTrue(f.saved().genesisOpen(f.directory.domainId()));assertArrayEquals(original,Files.readAllBytes(f.env.nodeRoot.resolve("node.identity")));
            }
        }
    }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void uncertainIdentityReplacementRetainsClosureAndRetriesBeforeAnotherCatCall(boolean afterForce)throws Exception {
        try(var f=new Fixture(false);var node=f.open(RatisNodeRuntime.Startup.AUTHORIZED_GENESIS)) {
            var first=new AtomicBoolean(true);var writes=new AtomicInteger();var files=new RatisConfiguredRecoveryTest.Forwarding(){
                @Override public void write(Path path,byte[] body,int maximum)throws IOException {
                    writes.incrementAndGet();boolean fail=first.getAndSet(false);if(fail && !afterForce)throw new IOException("Identity replacement refused before force");super.write(path,body,maximum);if(fail)throw new IOException("Lost identity force acknowledgement");
                }
            };
            RatisTopologySeedRuntimeTest.field(node,"identityJournal",files);f.issue();assertThrows(ExecutionException.class,()->node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS));
            assertFalse(field(node,"nodeIdentity",RatisNodeIdentity.class).genesisOpen(f.directory.domainId()));assertTrue(field(node,"genesisIdentityForcePending",Boolean.class));assertEquals(!afterForce,f.saved().genesisOpen(f.directory.domainId()));
            f.unavailable=true;assertThrows(ExecutionException.class,()->node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS));assertEquals(2,writes.get());assertFalse(field(node,"genesisIdentityForcePending",Boolean.class));assertFalse(f.saved().genesisOpen(f.directory.domainId()));
        }
    }
}
