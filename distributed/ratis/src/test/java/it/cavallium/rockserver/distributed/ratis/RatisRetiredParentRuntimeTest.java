package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native DROP and controlled runtime cold open; no servers/sockets or schema quorum claim. */
class RatisRetiredParentRuntimeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static Message message(byte[] bytes){return Message.valueOf(ByteString.copyFrom(bytes));}
    @Test void coldRetiredParentKeepsMetadataAfterBothChildrenDropItsUserColumn()throws Exception {
        var harness=new RatisTopologySeedRuntimeTest();harness.root=root;
        try(var f=harness.new Fixture()){
            var seed=f.node.seedTopology(f.request());var state=f.catalogueState;for(var target:f.definition.targets())state=state.seeded(seed.proof(target.groupId()));f.commit(state.publish());
            var children=new LinkedHashMap<UUID,ReplicatedRangeStore>();
            for(var target:f.definition.targets()){
                var machine=f.server.machines.get(RaftGroupId.valueOf(target.groupId()));RatisTopologySeedRuntimeTest.field(machine,"topologyAuthority",(RatisRangeStateMachine.TopologyAuthorityReader)f::authority);
                var record=RatisTopologyActivationRecord.of(f.authority(f.definition.header(),TopologyChange.Stage.ACTIVATE),target.groupId());var ready=RatisTopologyActivationRpc.stateReply(machine.queryLinearizable(message(RatisTopologyActivationRpc.prepareRequest(record))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologyActivationRpc.PREPARE,record);assertTrue(ready.ready());
                var tx=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(f.plan.peerId())).setGroupId(RaftGroupId.valueOf(target.groupId())).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(record.encode())).build());assertNull(tx.getException());machine.preAppendTransaction(tx);var store=RatisTopologySeedRuntimeTest.field(machine,"store",ReplicatedRangeStore.class);tx.initLogEntry(1,store.applied().index()+1);machine.applyTransaction(tx).get(5,TimeUnit.SECONDS);children.put(target.groupId(),store);
            }
            assertTrue(f.pool().detachRetiredUserHandles(f.parentStore()));assertTrue(f.parentStore().userHandlesDetached());
            var source=RatisTopologySeedRuntimeTest.schema();var dropped=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),2,source.routingPrefixBytes(),List.of());var change=new SchemaChange.Definition(new UUID(90,1),new OperationId(new UUID(90,2),1,0),f.definition.targetDirectory(),source,dropped);
            var bindings=new ArrayList<SchemaAdmissionManifest.Binding>();var replicas=new ArrayList<SchemaAdmissionManifest.ReplicaAdmission>();
            for(var target:f.definition.targets()){var capacity=f.pool().reserveSchemaCapacity(children.get(target.groupId()),change,Map.of(),Set.of());bindings.add(new SchemaAdmissionManifest.Binding(target.groupId(),null,false));replicas.add(new SchemaAdmissionManifest.ReplicaAdmission(target.groupId(),f.local.replicaId(),capacity));}
            var admissions=new SchemaAdmissionManifest(change,bindings,replicas);
            for(var target:f.definition.targets()){
                var store=children.get(target.groupId());var capacity=f.pool().requireSchemaFencingAdmission(store,change.header(),admissions.encode(),f.local);f.pool().applySchemaFence(store,new ApplyPosition(1,store.applied().index()+1),change.header(),capacity,admissions.digest());NativeSchemaInstallProof install;
                try(var gate=store.prepareSchemaInstallation(change.header());var plan=f.pool().prepareSchemaInstallation(store,change,Map.of(),Set.of())){assertTrue(gate.ready());install=f.pool().installSchema(store,new ApplyPosition(1,store.applied().index()+1),plan).proof();}
                var activation=store.applySchemaActivation(new ApplyPosition(1,store.applied().index()+1),install,f.plan.catalogue().groupId(),f.env.catalogue.store.applied());var machine=f.server.machines.get(RaftGroupId.valueOf(target.groupId()));machine.notifyTermIndexUpdated(1,store.applied().index());
                @SuppressWarnings("unchecked") var observer=(java.util.function.BiConsumer<SchemaChange.Definition,NativeSchemaActivationProof>)RatisTopologySeedRuntimeTest.field(machine,"schemaActivationObserver",java.util.function.BiConsumer.class);observer.accept(change,activation);assertTrue(store.ownership().interval().columns().isEmpty());
            }
            assertEquals(2,f.routed.directory().schemaEpoch());
            assertFalse(RatisTopologySeedRuntimeTest.field(f.pool(),"handles",Map.class).containsKey("data"),"Both child DROP operations must remove the physical USER column family");
            f.node.close();f.open(RatisNodeRuntime.Startup.RECOVER);var parent=f.parentStore();assertTrue(parent.userHandlesDetached());assertNull(parent.installedSchema(),"Base parent has no installation decoder; global child DROP cannot be installed on it");assertTrue(RatisTopologySeedRuntimeTest.field(parent,"columns",Map.class).isEmpty());assertEquals(seed.parentFreezes().get(f.definition.parents().getFirst().groupId()),parent.topologyFreeze());
            assertFalse(RatisTopologySeedRuntimeTest.field(f.pool(),"handles",Map.class).containsKey("data"),"Cold retired-parent recovery must not recreate the dropped USER column family");
            for(var target:f.definition.targets()){var child=RatisTopologySeedRuntimeTest.field(f.server.machines.get(RaftGroupId.valueOf(target.groupId())),"store",ReplicatedRangeStore.class);assertArrayEquals(dropped.digest(),child.schemaDigest());assertTrue(child.ownership().interval().columns().isEmpty());}
            var original=new DistributedCommand(f.definition.parents().getFirst().groupId(),1,source.digest(),new OperationId(RatisTopologySeedRuntimeTest.SESSION,1,0),List.of(new Mutation(Mutation.Kind.PUT,RatisTopologySeedRuntimeTest.COLUMN,new byte[]{1},new byte[]{42})));var request=new RatisTopologyOperationOutcomeRpc.Request(source.clusterId(),source.databaseId(),source.domainId(),new DistributedCdcCheckpoint.Lineage(f.definition.parents().getFirst().rangeId(),original.groupId(),1),original.operationId(),original.receiptDigest());var machine=f.server.machines.get(RaftGroupId.valueOf(original.groupId()));var reply=RatisTopologyOperationOutcomeRpc.decodeReply(machine.queryLinearizable(message(RatisTopologyOperationOutcomeRpc.request(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),request);assertEquals(RatisTopologyOperationOutcomeRpc.Outcome.FOUND,reply.outcome());assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(reply.result()).status());assertEquals(0,parent.activeSnapshotPins());
        }
    }
}
