package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native placement/catalogue verification. Remote owner admission is controlled; no remote quorum claim. */
class RatisRemotePlacementTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final class Fixture implements AutoCloseable {
        final DistributedSchema schema;
        final RatisNodeRuntimeTest.Env env;
        final RangeDirectory source,target;
        final TopologyChange.Definition definition;
        final RatisTopologyCatalogue topologies;
        final RatisCatalogue catalogue;
        final RatisRoutedClient route;
        final RatisNodeRuntime node;
        long sequence;
        OperationId operation(){return new OperationId(new UUID(988,1),0,sequence++);}
        Fixture(Path root,boolean localRange)throws Exception {
            Files.createDirectories(root);var base=RatisTopologyControllerTest.logicalSchema();schema=new DistributedSchema(base.clusterId(),base.databaseId(),base.domainId(),base.schemaEpoch(),0,base.columns());
            env=new RatisNodeRuntimeTest.Env(root,false,schema.digest());
            var local=env.plan.domains().getFirst().directory().ranges().getFirst().replicas().stream().filter(r->r.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();
            var remote=new ReplicaDescriptor(new UUID(987,1),new UUID(987,2),new UUID(987,3),"remote-placement","127.0.0.1:4599",ReplicaDescriptor.Role.VOTER);
            var left=new RangeDescriptor(new UUID(987,4),new UUID(987,5),1,0,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(remote));
            var right=new RangeDescriptor(new UUID(987,6),new UUID(987,7),1,0,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));
            source=new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),1,schema.digest(),0,schema.columnIds(),0,List.of(left,right));
            var first=new RangeDescriptor(new UUID(987,8),new UUID(987,9),1,0,null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,List.of(remote));
            var second=new RangeDescriptor(new UUID(987,10),new UUID(987,11),1,0,new byte[]{64},new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(remote));
            target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),1,source.schemaDigest(),1,source.columns(),0,List.of(first,second,right));definition=new TopologyChange.Definition(new UUID(987,12),source,target);
            var metadata=env.catalogue.store;var binding=env.plan.catalogue();
            var pure=RatisCatalogue.validator(schema.clusterId(),schema.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),binding.sessionsColumn(),command->{throw new IllegalArgumentException("No test operators");},key->{try{return metadata.readCurrentPoint(binding.directoryColumn(),key);}catch(Exception failure){throw new IllegalStateException(failure);}});
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                if(method.getName().equals("sendReadOnly"))return env.catalogue.client.io().sendReadOnly((Message)args[0]);
                if(!method.getName().equals("send"))throw new AssertionError(method);
                var command=DistributedCommandCodec.decode(((Message)args[0]).getContent().toByteArray());pure.accept(command);
                if(RatisTopologyCatalogue.requiresOwnerAdmission(command))RatisTopologyCatalogue.admitExecution(command,schema.clusterId(),schema.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),key->{try{return metadata.readCurrentPoint(binding.directoryColumn(),key);}catch(Exception failure){throw new IllegalStateException(failure);}},(d,proof)->new RatisTopologyCatalogue.QualifiedOwner(proof,new ApplyPosition(1,100)));
                var result=metadata.apply(new ApplyPosition(1,env.catalogue.next.getAndIncrement()),command);
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("remote-publication-fixture")).setGroupId(RaftGroupId.valueOf(binding.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(result))).build();
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(binding.groupId());case "io"->io;default->throw new AssertionError(method);});
            topologies=new RatisTopologyCatalogue(client,schema.clusterId(),schema.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());catalogue=new RatisCatalogue(client,schema.clusterId(),schema.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
            var schemas=new RatisSchemaCatalogue(env.catalogue.client,schema.clusterId(),schema.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(operation(),schema)).status());assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(operation(),null,source)).status());
            var peer=localRange?local:binding.replicas().getFirst();var domain=new RatisNodePlan.Domain(source,Map.of(RatisTopologyControllerTest.COLUMN,"data"),Set.of());
            var plan=new RatisNodePlan(peer.nodeId(),peer.nodeIncarnation(),List.of(domain),binding,List.of(),List.of(),Map.of(),8,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(binding,List.of()));
            route=new RatisRoutedClient(source,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError();}});
            var server=new RatisNodeRuntimeTest.FakeServer();node=RatisNodeRuntime.open(plan,Files.createDirectory(root.resolve("verifier")),ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(schema.domainId(),route),env.scheduler,env.timer,()->20L,env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server::create,List.of(schema));
        }
        void verify(RangeDirectory directory)throws IOException{node.requireCurrentPlacement(Map.of(directory.domainId(),directory),Map.of(schema.domainId(),schema));}
        void publish()throws Exception {
            assertEquals(CommandResult.Status.SUCCESS,topologies.execute(topologies.prepareDefinition(operation(),definition,null)).status());
            assertEquals(CommandResult.Status.SUCCESS,topologies.execute(topologies.prepareBegin(operation(),definition,null,null,null)).status());
            assertEquals(CommandResult.Status.SUCCESS,topologies.execute(topologies.prepareFencing(operation(),definition,topologies.readHead(source.domainId()),null,null)).status());
            for(var stage:TopologyChange.Stage.values()) {
                if(stage==TopologyChange.Stage.ACTIVATE)assertEquals(CommandResult.Status.SUCCESS,topologies.execute(topologies.preparePublication(operation(),definition,topologies.readHead(source.domainId()),null,null)).status());
                for(var participant:stage==TopologyChange.Stage.FREEZE?definition.header().parents():definition.header().targets())for(var owner:participant.owners()) {
                    var previous=topologies.readHead(source.domainId());byte[] artifact=new byte[32];artifact[0]=(byte)(stage.ordinal()+1);
                    var predecessor=stage==TopologyChange.Stage.FREEZE?definition.header().sourceDirectoryDigest():stage==TopologyChange.Stage.SEED?previous.freezeDigest():previous.seeded().get(new TopologyChange.Key(participant.groupId(),owner.replicaId())).digest();
                    var proof=TopologyChange.Proof.of(stage,definition.header(),participant.groupId(),owner.replicaId(),stage==TopologyChange.Stage.SEED?new ApplyPosition(0,0):new ApplyPosition(1,stage==TopologyChange.Stage.FREEZE?5:6),artifact,predecessor);
                    assertEquals(CommandResult.Status.SUCCESS,topologies.execute(topologies.prepareProof(operation(),definition,previous,proof)).status());
                }
            }
            assertEquals(CommandResult.Status.SUCCESS,topologies.execute(topologies.prepareRetirement(operation(),definition,topologies.readHead(source.domainId()),null,null)).status());
        }
        @Override public void close()throws Exception{node.close();route.close();env.close();}
    }
    @Test void catalogueOnlyNodeAcceptsQualifiedRemotePublicationButNotRouteOrDirectoryCasAlone()throws Exception{exercise(false);}
    @Test void unchangedLocalRangeAcceptsOtherRemoteRangesPublicationButNotRouteOrDirectoryCasAlone()throws Exception{exercise(true);}
    private void exercise(boolean local)throws Exception {
        try(var fixture=new Fixture(root.resolve(local?"local":"catalogue"),local)) {
            fixture.verify(fixture.source);var unchanged=fixture.source.ranges().getLast();
            fixture.route.refresh(fixture.target);assertThrows(IOException.class,()->fixture.verify(fixture.target));
            fixture.publish();assertDoesNotThrow(()->fixture.verify(fixture.target));if(local)assertEquals(unchanged,fixture.node.groupDescriptor(unchanged.groupId()));
            var forged=new RangeDirectory(fixture.target.clusterId(),fixture.target.databaseId(),fixture.target.domainId(),fixture.target.schemaEpoch(),fixture.target.schemaDigest(),fixture.target.generation()+1,fixture.target.columns(),fixture.target.routingPrefixBytes(),fixture.target.ranges());
            assertEquals(CommandResult.Status.SUCCESS,fixture.catalogue.execute(fixture.catalogue.prepareAfterAdministration(fixture.operation(),fixture.target,forged,null,null,fixture.topologies.readHead(forged.domainId()))).status());
            fixture.route.refresh(forged);assertThrows(IOException.class,()->fixture.verify(forged));
        }
    }
}
