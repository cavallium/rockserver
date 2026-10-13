package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual shared native node and cold service recovery; controlled Raft interfaces, no sockets/quorum. */
class RatisNodeMultiDomainTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final DistributedSchema first=new RatisNodeServiceTest().schema();
        final RatisNodeRuntimeTest.Env env;
        final DistributedSchema second;
        final RangeDirectory secondDirectory;
        final RatisNodePlan plan;
        final RatisRoutedClient secondClient;
        final Map<UUID,RatisRoutedClient> clients;
        final List<DistributedSchema> schemas;
        Fixture()throws Exception {
            env=new RatisNodeRuntimeTest.Env(root,false,first.digest());var source=env.routed.directory();UUID domain=new UUID(980,1),column=new UUID(980,2);
            second=new DistributedSchema(first.clusterId(),first.databaseId(),domain,1,0,List.of(new DistributedSchema.Column(72,column,"second",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
            var owners=source.ranges().getFirst().replicas().stream().map(r->new ReplicaDescriptor(r.nodeId(),r.nodeIncarnation(),UUID.randomUUID(),r.peerId(),r.address(),r.role())).toList();
            var range=new RangeDescriptor(new UUID(980,3),new UUID(980,4),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,owners);
            secondDirectory=new RangeDirectory(first.clusterId(),first.databaseId(),domain,1,second.digest(),1,Set.of(column),0,List.of(range));
            plan=new RatisNodePlan(env.plan.nodeId(),env.plan.nodeIncarnation(),List.of(env.plan.domain(first.domainId()),new RatisNodePlan.Domain(secondDirectory,Map.of(column,"second"),Set.of())),env.plan.catalogue(),List.of(),List.of(),Map.of(),8,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(env.plan.domain(first.domainId()),new RatisNodePlan.Domain(secondDirectory,Map.of(column,"second"),Set.of()))),RatisPoolFixtures.groups(env.plan.catalogue(),List.of()));
            secondClient=new RatisRoutedClient(secondDirectory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] k){throw new AssertionError("Unexpected read");}public CommandResult command(RangeDescriptor r,DistributedCommand c){throw new AssertionError("Timer has not fired");}});
            clients=Map.of(first.domainId(),env.routed,domain,secondClient);schemas=List.of(second,first);
        }
        RatisNodeRuntime.Settings settings(){var s=env.settings();return new RatisNodeRuntime.Settings(s.images(),s.retention(),s.clockPeriod(),s.propagationVisits(),3,s.shutdownTimeout(),s.exports(),RatisNodeRuntime.ResourceLimits.defaults());}
        RatisNodeManifest manifest(){return new RatisNodeManifest(schemas,plan,settings(),3,3);}
        ReplicatedRangeStore store(RatisNodeRuntimeTest.FakeServer server,UUID group)throws Exception{return RatisTopologySeedRuntimeTest.field(server.machines.get(RaftGroupId.valueOf(group)),"store",ReplicatedRangeStore.class);}
        void publish()throws Exception {
            new RatisNodeServiceTest().publish(env,first);var o=env.outer;var c=plan.catalogue();
            var schemas=new RatisSchemaCatalogue(env.catalogue.client,o.cluster,o.database,c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(o.operation(4),second,RatisColumnAllocation.counterValue(71),Map.of(),Map.of(),Map.of())).status());
            var d=secondDirectory;var initial=new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),d.schemaEpoch(),d.schemaDigest(),0,d.columns(),d.routingPrefixBytes(),d.ranges());
            var catalogue=new RatisCatalogue(env.catalogue.client,o.cluster,o.database,c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());var roster=new RatisDatabaseDomainRoster(env.catalogue.client,o.cluster,o.database,c);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(o.operation(5),null,initial,roster.read())).status());assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(o.operation(6),initial,d)).status());
        }
        public void close()throws Exception{secondClient.close();env.close();}
    }
    @Test void twoDomainsShareNativePoolAndRecoverCompleteManifestAndCatalogue()throws Exception {
        try(var f=new Fixture()){
            var fake=new RatisNodeRuntimeTest.FakeServer();UUID a=f.first.domainId(),b=f.second.domainId();var firstRange=f.plan.directory(a).ranges().getFirst();var secondRange=f.secondDirectory.ranges().getFirst();
            try(var node=RatisNodeRuntime.open(f.plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,f.clients,f.env.scheduler,f.env.timer,()->20L,f.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,f.schemas)){
                node.start();assertEquals(3,fake.machines.size());
                for(var domain:f.schemas){var range=f.plan.directory(domain.domainId()).ranges().getFirst();var store=f.store(fake,range.groupId());assertEquals(domain.domainId(),store.metadataOutcomeState().domainId());var command=new DistributedCommand(range.groupId(),range.rangeEpoch(),domain.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,domain.columnIds().iterator().next(),new byte[]{1},new byte[]{(byte)(domain==f.first?7:9)})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),command)).status());}
                assertThrows(IllegalArgumentException.class,()->f.store(fake,firstRange.groupId()).readCurrentPoint(f.second.columnIds().iterator().next(),new byte[]{1}));
            }
            f.publish();var manifest=RatisNodeManifest.decode(RatisNodeManifest.encode(f.manifest()));byte[] identity=Files.readAllBytes(f.env.nodeRoot.resolve("node.identity"));var recovered=new RatisNodeRuntimeTest.FakeServer();
            var resources=new RatisNodeService.Resources(f.env.catalogue.client,f.clients,f.env.scheduler,f.env.timer);f.env.resourcesTransferred=true;
            try(var service=RatisNodeService.recover(manifest,f.env.nodeRoot,ConfigParser.parseDefault(),resources,recovered::create)){
                service.start();service.verifyCatalogue();assertEquals(3,recovered.machines.size());assertArrayEquals(new byte[]{7},f.store(recovered,firstRange.groupId()).readCurrentPoint(f.first.columnIds().iterator().next(),new byte[]{1}));assertArrayEquals(new byte[]{9},f.store(recovered,secondRange.groupId()).readCurrentPoint(f.second.columnIds().iterator().next(),new byte[]{1}));f.env.catalogue.missingSchema=true;assertThrows(java.io.IOException.class,service::verifyCatalogue);f.env.catalogue.missingSchema=false;service.verifyCatalogue();
            }
            assertArrayEquals(identity,Files.readAllBytes(f.env.nodeRoot.resolve("node.identity")));
        }
    }
    @Test void runtimeOwnerDiscoveryBindsActualProcessAndPoolWithoutTakingSnapshots()throws Exception {
        try(var f=new Fixture()){
            var fake=new RatisNodeRuntimeTest.FakeServer();
            try(var node=RatisNodeRuntime.open(f.plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,f.clients,f.env.scheduler,f.env.timer,()->20L,f.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,f.schemas)){
                node.start();RatisSnapshotOwner previous=null;
                for(var schema:f.schemas){var range=f.plan.directory(schema.domainId()).ranges().getFirst();var id=RaftGroupId.valueOf(range.groupId());var machine=fake.machines.get(id);var store=f.store(fake,range.groupId());var cfg=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder();
                    for(var peer:fake.actualGroups.get(id).getPeers())if(peer.getStartupRole()==org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)cfg.addListeners(peer.getRaftPeerProto());else cfg.addPeers(peer.getRaftPeerProto());
                    long next=store.applied().index()+1;machine.notifyConfigurationChanged(1,next,cfg.build());machine.notifyTermIndexUpdated(1,next);
                    var request=org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_OWNER,new byte[0])));var response=machine.queryLinearizable(request).get(5,java.util.concurrent.TimeUnit.SECONDS);var owner=RatisSnapshotOwner.decode(ScopedTransactionRpc.parse(response.getContent().toByteArray(),ScopedTransactionRpc.MAX_REPLY_BYTES).body());
                    assertEquals(range.groupId(),owner.groupId());assertArrayEquals(schema.digest(),owner.schemaDigest());assertNotNull(owner.cohort());assertEquals(f.plan.nodeIncarnation(),owner.cohort().nodeIncarnation());assertEquals(0,owner.cohort().poolPins());assertEquals(0,store.activeSnapshotPins());
                    if(previous!=null){assertEquals(previous.cohort().processIncarnation(),owner.cohort().processIncarnation());assertEquals(previous.cohort().poolId(),owner.cohort().poolId());assertNotEquals(previous.incarnation(),owner.incarnation());}previous=owner;
                }
            }
        }
    }
    @Test void incompleteRoutesOrSchemasRejectBeforeNativeOrServerExposure()throws Exception {
        try(var f=new Fixture()){
            var fake=new RatisNodeRuntimeTest.FakeServer();assertThrows(IllegalArgumentException.class,()->RatisNodeRuntime.open(f.plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,Map.of(f.first.domainId(),f.env.routed),f.env.scheduler,f.env.timer,()->20L,f.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,f.schemas));
            assertThrows(IllegalArgumentException.class,()->RatisNodeRuntime.open(f.plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,f.clients,f.env.scheduler,f.env.timer,()->20L,f.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,List.of(f.first)));assertFalse(fake.created);assertFalse(Files.exists(RatisPoolFixtures.pools().getFirst().nativePath(f.env.nodeRoot).resolve("CURRENT")));assertFalse(Files.exists(f.env.nodeRoot.resolve("node.identity")));
        }
    }
}
