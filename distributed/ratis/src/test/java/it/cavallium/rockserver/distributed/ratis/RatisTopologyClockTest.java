package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Genuine one-voter native split/activation and bounded metadata clocks, without sockets or distributed quorum. */
class RatisTopologyClockTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final long DAY=24L*60*60*1000;
    RatisTopologySeedRuntimeTest.Fixture fixture()throws Exception{var test=new RatisTopologySeedRuntimeTest();test.root=root;return test.new Fixture();}
    RatisRoutedClient clocks(RatisTopologySeedRuntimeTest.Fixture f,AtomicBoolean lost,List<DistributedCommand> sent){
        return new RatisRoutedClient(f.definition.sourceDirectory(),new RatisRoutedClient.Transport(){
            public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("Clocks read no USER snapshot");}
            public CommandResult command(RangeDescriptor range,DistributedCommand command)throws IOException{
                assertEquals(range.groupId(),command.groupId());assertEquals(range.rangeEpoch(),command.rangeEpoch());assertNotNull(command.sessionControl());assertFalse(command.sessionControl().binding());sent.add(command);
                try{var machine=f.server.machines.get(RaftGroupId.valueOf(range.groupId()));var store=RatisTopologySeedRuntimeTest.field(machine,"store",ReplicatedRangeStore.class);var transaction=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(f.plan.peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(sent.size()).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build());if(transaction.getException()!=null)throw new IOException("Native clock proposal rejected",transaction.getException());machine.preAppendTransaction(transaction);transaction.initLogEntry(f.server.term,store.applied().index()+1);var result=CommandResult.decode(machine.applyTransaction(transaction).get(5,TimeUnit.SECONDS).getContent().toByteArray());if(lost.compareAndSet(true,false))throw new IOException("Lost applied clock reply");return result;}catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException("Native clock apply failed",failure);}
            }
        });
    }
    List<NativeTopologyActivation> splitAndActivate(RatisTopologySeedRuntimeTest.Fixture f)throws Exception{
        var seed=f.node.seedTopology(f.request());var state=f.catalogueState;for(var target:f.definition.targets())state=state.seeded(seed.proof(target.groupId()));f.commit(state.publish());var activations=new ArrayList<NativeTopologyActivation>();
        for(var target:f.definition.targets()){var machine=f.server.machines.get(RaftGroupId.valueOf(target.groupId()));RatisTopologySeedRuntimeTest.field(machine,"topologyAuthority",(RatisRangeStateMachine.TopologyAuthorityReader)f::authority);var view=f.authority(f.definition.header(),TopologyChange.Stage.ACTIVATE);var record=RatisTopologyActivationRecord.of(view,target.groupId());machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologyActivationRpc.prepareRequest(record)))).get(5,TimeUnit.SECONDS);var transaction=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(f.plan.peerId())).setGroupId(RaftGroupId.valueOf(target.groupId())).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(record.encode()))).build());assertNull(transaction.getException());machine.preAppendTransaction(transaction);var store=RatisTopologySeedRuntimeTest.field(machine,"store",ReplicatedRangeStore.class);transaction.initLogEntry(1,store.applied().index()+1);activations.add(NativeTopologyActivation.decode(machine.applyTransaction(transaction).get(5,TimeUnit.SECONDS).getContent().toByteArray()));assertTrue(store.admissionOpen());}
        return List.copyOf(activations);
    }
    void advance(RatisTopologySeedRuntimeTest.Fixture f,long time)throws Exception{var before=f.env.catalogue.epochs.read();var next=before.advanceTime(time);var operation=new OperationId(new UUID(72,1),1,next.generation());assertEquals(CommandResult.Status.SUCCESS,f.env.catalogue.epochs.execute(f.env.catalogue.epochs.prepare(operation,before,next)).status());}
    byte[] cell(ReplicatedRangeStore store,int tag,long index)throws Exception{var db=RatisTopologySeedRuntimeTest.field(store,"db",org.rocksdb.RocksDB.class);var state=RatisTopologySeedRuntimeTest.field(store,"state",org.rocksdb.ColumnFamilyHandle.class);var group=store.ownership().owner().groupId();var key=ByteBuffer.allocate(25).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)tag).putLong(index).array();return db.get(state,key);}
    @Test void splitRetainedClockAdvancesAndExpiresOriginalBarrierWithoutChangingChildUserRows()throws Exception{
        try(var f=fixture();var routed=clocks(f,new AtomicBoolean(),new ArrayList<>())){Path journals=Files.createDirectory(root.resolve("clock"));var parent=f.parentStore();assertNotNull(cell(parent,0x50,2));var activations=splitAndActivate(f);
            try(var propagation=new RatisSessionPropagation(journals,f.env.catalogue.epochs,routed,4)){for(var activation:activations)propagation.qualifyTopology(f.definition,activation);routed.refresh(f.definition.targetDirectory());var first=propagation.begin();assertEquals(3,first.total());assertTrue(propagation.step(3).complete());assertEquals(10,ByteBuffer.wrap(cell(parent,0x52,2)).getLong());advance(f,10+DAY);propagation.begin();assertTrue(propagation.step(3).complete());assertNull(cell(parent,0x50,2));assertNull(cell(parent,0x52,2));for(var target:f.definition.targets()){var child=RatisTopologySeedRuntimeTest.field(f.server.machines.get(RaftGroupId.valueOf(target.groupId())),"store",ReplicatedRangeStore.class);assertArrayEquals(target.lowerInclusive()==null ? new byte[]{42} : null,child.readCurrentPoint(RatisTopologySeedRuntimeTest.COLUMN,target.lowerInclusive()==null ? new byte[]{1} : new byte[]{65}));}assertTrue(Files.size(journals.resolve("propagation.bin"))<64*1024);}
            try(var coldRouted=clocks(f,new AtomicBoolean(),new ArrayList<>());var reopened=new RatisSessionPropagation(journals,f.env.catalogue.epochs,coldRouted,4)){assertArrayEquals(f.definition.targetDirectory().payloadDigest(),coldRouted.directory().payloadDigest());assertFalse(reopened.schemaRefreshPending());reopened.begin();assertTrue(reopened.step(3).complete());assertTrue(reopened.progress().complete());assertEquals(3,reopened.progress().total());assertNotNull(reopened.historicalRoute(new RatisSessionPropagation.Participant(f.request().anchorGroup(),1)));}
        }
    }
    @Test void unknownClockResponseDuringSplitRetainsOriginalOperationBeforeRosterReplacementAndColdRetry()throws Exception{unknownClockColdRetry(false);}
    @Test void coldClientAlreadyAtKnownTargetDoesNotRegressWhileOriginalSourceRoundDrains()throws Exception{unknownClockColdRetry(true);}
    void unknownClockColdRetry(boolean alreadyAtTarget)throws Exception{
        var sent=new ArrayList<DistributedCommand>();var lost=new AtomicBoolean(true);
        try(var f=fixture();var routed=clocks(f,lost,sent)){Path journals=Files.createDirectory(root.resolve("unknown"));byte[] original;var parent=f.request().anchorGroup();
            try(var propagation=new RatisSessionPropagation(journals,f.env.catalogue.epochs,routed,4)){propagation.begin();assertEquals(0,propagation.step(1).completed());original=DistributedCommandCodec.encode(sent.getFirst());var activations=splitAndActivate(f);for(var activation:activations)propagation.qualifyTopology(f.definition,activation);routed.refresh(f.definition.targetDirectory());assertTrue(propagation.schemaRefreshPending());}
            try(var coldRouted=clocks(f,lost,sent)) {
                if(alreadyAtTarget)coldRouted.refresh(f.definition.targetDirectory());
                try(var reopened=new RatisSessionPropagation(journals,f.env.catalogue.epochs,coldRouted,4)){assertArrayEquals(f.definition.targetDirectory().payloadDigest(),coldRouted.directory().payloadDigest(),"Archived ACTIVATE restores the published route before the unknown source round drains");assertEquals(parent,reopened.progress().participants().getFirst().group());assertEquals(1,sent.size(),"Restoring routes must not submit a child clock");var replaced=reopened.reconcileSchemaRefresh(1);assertEquals(3,replaced.total());assertEquals(2,sent.size(),"Only the original pending source command executes before roster replacement");assertEquals(parent,sent.get(1).groupId());assertArrayEquals(original,DistributedCommandCodec.encode(sent.get(1)),"Unknown control keeps its exact session/epoch/operation/payload");assertTrue(reopened.step(3).complete());}
            }
        }
    }
    @Test void arbitrarySameSchemaTopologyAndChangedOriginalReceiptDoNotQualifyNewClockParticipants()throws Exception{
        try(var f=fixture();var routed=clocks(f,new AtomicBoolean(),new ArrayList<>());var propagation=new RatisSessionPropagation(Files.createDirectory(root.resolve("qualification")),f.env.catalogue.epochs,routed,4)){propagation.begin();assertTrue(propagation.step(1).complete());routed.refresh(f.definition.targetDirectory());assertThrows(IOException.class,()->propagation.reconcileSchemaRefresh(1));var activations=splitAndActivate(f);for(var activation:activations)propagation.qualifyTopology(f.definition,activation);assertEquals(3,propagation.reconcileSchemaRefresh(1).total());var original=activations.getFirst();var changed=new NativeTopologyActivation(f.pool().topologySeed(f.definition.intentId()),original.birth(),new ApplyPosition(original.position().term(),original.position().index()+1),original.catalogueGroup(),original.catalogueCut(),original.publicationCommitment());assertThrows(IOException.class,()->propagation.qualifyTopology(f.definition,changed));}
    }
}
