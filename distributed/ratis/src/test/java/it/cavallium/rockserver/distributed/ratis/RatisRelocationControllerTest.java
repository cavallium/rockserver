package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Actual native images, tails, configuration cuts and catalogue CAS; controlled Ratis transport only. */
class RatisRelocationControllerTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Workflow implements AutoCloseable {
        final RatisTopologySourceSnapshotFixture source;
        final DistributedStoragePool receiverPool;
        final ReplicatedRangeStore catalogue;
        final UUID catalogueId=new UUID(200,1),directoryColumn=new UUID(200,2),sessionsColumn=new UUID(200,3);
        final RatisNodePlan.Catalogue binding;
        final RaftClient control=mock(RaftClient.class),raft=mock(RaftClient.class),receiverRaft=mock(RaftClient.class),receiverAnchorRaft=mock(RaftClient.class);
        final BlockingApi controlIo=mock(BlockingApi.class),dataIo=mock(BlockingApi.class),receiverIo=mock(BlockingApi.class);
        final AdminApi admin=mock(AdminApi.class);
        final RatisTopologySourceSnapshotService snapshots;
        final RatisRelocationDataClient data;
        final List<DistributedCommand> tail=new ArrayList<>();
        final Map<String,byte[]> actorRecords=new HashMap<>();
        ReplicatedRangeStore receiver;NativeTopologyLearnerRegistration registration;
        long catalogueIndex;int adminCalls;boolean loseConfiguration=true;
        Workflow()throws Exception {
            source=new RatisTopologySourceSnapshotFixture(root.resolve("source"));receiverPool=DistributedStoragePool.open(root.resolve("receiver"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            binding=new RatisNodePlan.Catalogue(catalogueId,1,new byte[32],directoryColumn,sessionsColumn,List.of(source.donor));
            catalogue=source.pool.openRange(new RangeOwnership.Owner(catalogueId,1),new RangeOwnership.Interval(catalogueId,Set.of(directoryColumn,sessionsColumn),null,null),new byte[32],Map.of(directoryColumn,"catalogue-directory",sessionsColumn,"catalogue-sessions"),Set.of());catalogue.enableCatalogueScopeCompaction();
            var initial=new DistributedCommand(catalogueId,1,new byte[32],new OperationId(new UUID(200,4),0,0),List.of(new Mutation(Mutation.Kind.PUT,directoryColumn,domainKey(),RangeDirectoryCodec.encode(source.definition.sourceDirectory()))));catalogue.apply(new ApplyPosition(1,catalogueIndex++),initial);
            when(control.getGroupId()).thenReturn(RaftGroupId.valueOf(catalogueId));when(control.io()).thenReturn(controlIo);
            when(controlIo.sendReadOnly(any())).thenAnswer(call->{byte[] input=bytes(call.getArgument(0));int kind=ScopedTransactionRpc.kind(input,RatisTopologySourceSnapshotRpc.MAX_REPLY_BYTES);byte[] result;
                if(kind==RatisTopologyJoinAuthority.KIND){var request=RatisTopologyJoinAuthority.decodeRequest(input);result=RatisTopologyJoinAuthority.reply(RatisTopologyJoinAuthority.readNative(catalogue,directoryColumn,source.schema.clusterId(),source.schema.databaseId(),request));}
                else if(kind==RatisTopologyExecutionRpc.PREPARE){var command=RatisTopologyExecutionRpc.decode(input);admit(command);result=input;}
                else {var packet=ScopedTransactionRpc.parse(input,ScopedTransactionRpc.MAX_REQUEST_BYTES);assertEquals(ScopedTransactionRpc.POINT,packet.kind());var in=ByteBuffer.wrap(packet.body());assertEquals(directoryColumn,ScopedTransactionRpc.uuid(in));var key=ScopedTransactionRpc.blob(in,65536,false);byte[] value=catalogue.readCurrentPoint(directoryColumn,key);var out=ByteBuffer.allocate(4+(value==null?0:value.length)).putInt(value==null?-1:value.length);if(value!=null)out.put(value);result=ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,out.array());}return reply(catalogueId,source.donor.peerId(),result);});
            when(controlIo.send(any())).thenAnswer(call->{var command=DistributedCommandCodec.decode(bytes(call.getArgument(0)));RatisTopologyCatalogue.validator(source.schema.clusterId(),source.schema.databaseId(),catalogueId,1,new byte[32],directoryColumn,this::read).accept(command);if(RatisTopologyCatalogue.requiresOwnerAdmission(command) && catalogue.operationResult(command.operationId(),command.receiptDigest())==null)admit(command);return reply(catalogueId,source.donor.peerId(),catalogue.apply(new ApplyPosition(1,catalogueIndex++),command));});
            snapshots=new RatisTopologySourceSnapshotService(new RatisTopologySourceSnapshotService.Host(){
                public RatisTopologySourceSnapshotService.Lease capture(RatisTopologySourceSnapshotRpc.Request request)throws Exception{return source.capture(request);}
                public void requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception{assertTrue(RatisRelocationOwnerRpc.covers(source.store.applied(),lease.prepared().image().position()));assertArrayEquals(source.store.configuration().data(),lease.prepared().sourceCfg().data());}
                public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception{assertNotNull(receiver);if(!receiver.topologyLearnerImageInstalled(lease.prepared().image().position(),lease.prepared().image().sha256()))throw new IOException("No forced receiver image");}
                public boolean isReleased(RatisTopologySourceSnapshotRpc.Request request){return false;}
                public void released(RatisTopologySourceSnapshotService.Lease lease){assertTrue(source.store.activeSnapshotPins()>0);}
                public InstallSnapshotReplyProto send(InstallSnapshotRequestProto request)throws IOException{try{var response=source.send(request);if(request.getSnapshotChunk().getDone()){Path received=root.resolve("received.image");Files.write(received,source.received.toByteArray());receiver.installSnapshot(received,new NativeRangeImage.Limits(4*1024*1024,8*1024*1024,16));}return response;}catch(org.rocksdb.RocksDBException failure){throw new IOException(failure);}}
            },RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024));
            when(raft.getGroupId()).thenReturn(RaftGroupId.valueOf(source.group));when(receiverRaft.getGroupId()).thenReturn(RaftGroupId.valueOf(source.group));when(receiverAnchorRaft.getGroupId()).thenReturn(RaftGroupId.valueOf(catalogueId));when(receiverAnchorRaft.io()).thenReturn(receiverIo);when(raft.io()).thenReturn(dataIo);when(raft.admin()).thenReturn(admin);when(receiverRaft.io()).thenReturn(receiverIo);
            when(dataIo.sendReadOnly(any())).thenAnswer(call->dispatch(bytes(call.getArgument(0)),false));when(dataIo.sendStaleRead(any(),anyLong(),any())).thenAnswer(call->dispatch(bytes(call.getArgument(0)),false));when(receiverIo.sendStaleRead(any(),anyLong(),any())).thenAnswer(call->dispatch(bytes(call.getArgument(0)),true));
            when(dataIo.send(any())).thenAnswer(call->{var record=RatisTopologyRelocationRecord.decode(bytes(call.getArgument(0)));String key=record.definition().intentId()+"."+record.action();if(!actorRecords.containsKey(key)){// Force actor intent before any native/config effect.
                    SessionJournalFiles.write(root.resolve(key),record.encode(),RatisTopologyRelocationRecord.MAX_BYTES);actorRecords.put(key,record.encode());var position=new ApplyPosition(1,source.store.applied().index()+1);source.store.applyNoop(position);if(receiver!=null){while(receiver.applied().index()<position.index()-1)receiver.applyNoop(new ApplyPosition(1,receiver.applied().index()+1));if(record.action()==RatisTopologyRelocationRecord.Action.COMPLETE)receiver.applyTopologyRelocationCompletion(position,record.definition(),record.targetConfiguration(),catalogueId,record.catalogueCut());else receiver.applyNoop(position);}}
                return reply(source.group,source.donor.peerId(),new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode());});
            when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(call->{SetConfigurationRequest.Arguments args=call.getArgument(0);assertEquals(SetConfigurationRequest.Mode.COMPARE_AND_SET,args.getMode());var expected=RaftConfigurationProto.newBuilder();args.getServersInCurrentConf().forEach(p->expected.addPeers(p.getRaftPeerProto()));args.getListenersInCurrentConf().forEach(p->expected.addListeners(p.getRaftPeerProto()));assertArrayEquals(source.store.configuration().data(),expected.build().toByteArray());assertTrue(actorRecords.keySet().stream().anyMatch(k->k.endsWith("AUTHORIZE")));var next=RaftConfigurationProto.newBuilder();args.getServersInNewConf().forEach(p->next.addPeers(p.getRaftPeerProto()));args.getPeersInNewConf(RaftPeerRole.LISTENER).forEach(p->next.addListeners(p.getRaftPeerProto()));for(var command:tail)if(receiver.applied().index()<source.store.applied().index())receiver.applyTopologyLearnerTail(new ApplyPosition(1,receiver.applied().index()+1),command);tail.clear();var position=new ApplyPosition(1,source.store.applied().index()+1);source.store.applyNoop(position,next.build().toByteArray());receiver.applyNoop(position,next.build().toByteArray());adminCalls++;if(loseConfiguration){loseConfiguration=false;throw new IOException("Lost committed CAS reply");}return reply(source.group,source.donor.peerId(),new byte[0]);});
            data=new RatisRelocationDataClient(source.schema.clusterId(),source.schema.databaseId(),catalogueId,r->raft,(r,o)->o.peerId().equals(source.receiver.peerId())?(r.groupId().equals(catalogueId)?receiverAnchorRaft:receiverRaft):raft);
        }
        byte[] domainKey(){return ByteBuffer.allocate(16).putLong(source.schema.domainId().getMostSignificantBits()).putLong(source.schema.domainId().getLeastSignificantBits()).array();}
        byte[] read(byte[] key){try{return catalogue.readCurrentPoint(directoryColumn,key);}catch(Exception failure){throw new IllegalStateException(failure);}}
        void admit(DistributedCommand command)throws IOException{RatisTopologyCatalogue.admitRelocation(command,source.schema.clusterId(),source.schema.databaseId(),catalogueId,1,new byte[32],directoryColumn,this::read,(definition,expected)->{try {var cfg=source.store.configuration();if(!Arrays.equals(OperatorUpload.sha256(cfg.data()),expected.configurationDigest()) || !new ApplyPosition(cfg.term(),cfg.index()).equals(expected.configurationPosition()) || !receiver.topologyLearnerReadiness(expected.requiredCatchupCut(),cfg.data()).ready())throw new IOException("Actual native target configuration not ready");return new RatisTopologyCatalogue.QualifiedRelocation(expected,source.store.applied());} catch(org.rocksdb.RocksDBException failure){throw new IOException("Cannot qualify native relocation target",failure);}});}
        RaftClientReply dispatch(byte[] input,boolean onReceiver)throws Exception{
            int kind=ScopedTransactionRpc.kind(input,Math.max(RatisTopologyLearnerJoinRpc.MAX_REQUEST_BYTES,RatisRelocationAdmissionRpc.MAX_BYTES));
            if(kind==RatisTopologyLearnerJoinRpc.KIND){var request=RatisTopologyLearnerJoinRpc.decode(input);var view=RatisTopologyJoinAuthority.readNative(catalogue,directoryColumn,source.schema.clusterId(),source.schema.databaseId(),request);var prepared=snapshots.prepare(RatisNodeRuntime.sourceRequest(view.definition(),catalogueId,source.receiver));registration=new NativeTopologyLearnerRegistration(view.definition(),prepared.image().topologyOrigin(),prepared.schema(),source.receiver,catalogueId,view.cut(),prepared.image().position(),prepared.image().sha256(),prepared.sourceCfg().data(),RatisRelocationDataClient.targetConfiguration(view.definition().targets().getFirst(),prepared.sourceCfg().data()),Map.of(source.column,"data"),Set.of(),prepared.schemaRows());receiver=receiverPool.openTopologyLearner(registration);assertFalse(receiver.topologyLearnerImageInstalled());var write=new DistributedCommand(source.group,1,source.schema.digest(),new OperationId(new UUID(200,20),1,0),List.of(new Mutation(Mutation.Kind.PUT,source.column,new byte[]{7},new byte[]{8})));source.store.apply(new ApplyPosition(1,source.store.applied().index()+1),write);tail.add(write);return reply(catalogueId,source.receiver.peerId(),RatisTopologyLearnerJoinRpc.reply(request,registration));}
            if(kind==RatisRelocationAdmissionRpc.PREPARE){var record=RatisRelocationAdmissionRpc.decode(input);var purpose=record.action()==RatisTopologyRelocationRecord.Action.AUTHORIZE?RatisTopologyJoinAuthority.Purpose.SOURCE_CONFIGURATION:RatisTopologyJoinAuthority.Purpose.PUBLICATION;var request=RatisTopologyJoinAuthority.Request.of(record.definition().header(),source.donor.replicaId(),purpose);RatisTopologyJoinAuthority.readNative(catalogue,directoryColumn,source.schema.clusterId(),source.schema.databaseId(),request);return reply(source.group,source.donor.peerId(),input);}
            if(kind==RatisRelocationOwnerRpc.KIND){var request=RatisRelocationOwnerRpc.decode(input);var store=onReceiver?receiver:source.store;var cfg=store.configuration();var range=request.expected()==RatisRelocationOwnerRpc.Expected.SOURCE || request.expected()==RatisRelocationOwnerRpc.Expected.CAUGHT_UP?request.definition().parents().getFirst():request.definition().targets().getFirst();if(request.expected()!=RatisRelocationOwnerRpc.Expected.IMAGE)RatisRangeStateMachine.requireSchemaMembership(range,cfg.data());var owner=range.replicas().stream().filter(r->r.peerId().equals(onReceiver?source.receiver.peerId():source.donor.peerId())).findFirst().orElseThrow();boolean installed=onReceiver?store.topologyLearnerImageInstalled():store.admissionOpen();boolean caught=request.expected()!=RatisRelocationOwnerRpc.Expected.IMAGE && installed && (request.requiredCut()==null || RatisRelocationOwnerRpc.covers(store.applied(),request.requiredCut()));return reply(source.group,owner.peerId(),RatisRelocationOwnerRpc.reply(new RatisRelocationOwnerRpc.Evidence(request.digest(),TopologyChange.Owner.of(owner),store.applied(),cfg,onReceiver?registration.digest():null,installed,caught)));}
            var request=RatisTopologySourceSnapshotRpc.request(input);byte[] output;if(kind==RatisTopologySourceSnapshotRpc.RELEASE){snapshots.release(request);output=input;}else output=RatisTopologySourceSnapshotRpc.reply(kind,kind==RatisTopologySourceSnapshotRpc.PREPARE?snapshots.prepare(request):snapshots.transfer(request));return reply(source.group,source.donor.peerId(),output);
        }
        @Override public void close()throws Exception{snapshots.close();data.close();receiverPool.close();source.close();}
    }
    static byte[] bytes(Message message){return message.getContent().toByteArray();}
    static RaftClientReply reply(UUID group,String peer,byte[] bytes){return RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(peer)).setGroupId(RaftGroupId.valueOf(group)).setCallId(0).setSuccess().setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build();}
    @Test void listenerImageTailAndPromotionPublishOnlyAfterNativeReadinessWithExactLostCasRecovery()throws Exception {
        try(var f=new Workflow()){
            var journal=Files.createDirectories(root.resolve("controller"));UUID id=f.source.definition.intentId();try(var controller=new RatisRelocationController(journal,f.source.schema.clusterId(),f.source.schema.databaseId(),f.binding,f.control,f.data,r->{var anchor=new ReplicaDescriptor(r.nodeId(),r.nodeIncarnation(),r.replicaId(),r.peerId(),r.address(),ReplicaDescriptor.Role.VOTER);return new RatisRelocationController.Anchor(new RangeDescriptor(f.catalogueId,f.catalogueId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(anchor)),anchor);})){var handle=controller.prepare(f.source.definition);assertThrows(IOException.class,()->controller.advance(handle,64));assertEquals(RatisRelocationController.Phase.CONFIGURE,handle.phase());assertEquals(1,f.adminCalls);assertTrue(f.receiver.topologyLearnerImageInstalled());assertFalse(f.receiver.admissionOpen());assertArrayEquals(f.source.definition.sourceDirectory().payloadDigest(),RangeDirectoryCodec.decode(f.read(f.domainKey())).payloadDigest());}
            try(var controller=new RatisRelocationController(journal,f.source.schema.clusterId(),f.source.schema.databaseId(),f.binding,f.control,f.data,r->{throw new AssertionError("Known receiver must not recapture");})){var handle=controller.reopen(id);assertEquals(RatisRelocationController.Phase.ACTIVE,controller.advance(handle,64));assertEquals(1,f.adminCalls,"Already committed target must resolve unknown CAS without another mutation");assertFalse(f.receiver.admissionOpen());assertEquals(0,f.source.store.activeSnapshotPins());assertArrayEquals(f.source.definition.targetDirectory().payloadDigest(),RangeDirectoryCodec.decode(f.read(f.domainKey())).payloadDigest());}
            var previous=f.source.definition.targetDirectory();var range=previous.ranges().getFirst();var voter=new ReplicaDescriptor(f.source.receiver.nodeId(),f.source.receiver.nodeIncarnation(),f.source.receiver.replicaId(),f.source.receiver.peerId(),f.source.receiver.address(),ReplicaDescriptor.Role.VOTER);var promoted=new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion()+1,null,null,range.topologyState(),List.of(f.source.donor,voter));var next=new RangeDirectory(previous.clusterId(),previous.databaseId(),previous.domainId(),previous.schemaEpoch(),previous.schemaDigest(),previous.generation()+1,previous.columns(),previous.routingPrefixBytes(),List.of(promoted));var definition=new TopologyChange.Definition(new UUID(200,30),previous,next);
            try(var controller=new RatisRelocationController(journal,f.source.schema.clusterId(),f.source.schema.databaseId(),f.binding,f.control,f.data,r->{throw new AssertionError("Promotion must preserve original registered image");})){var handle=controller.prepare(definition);assertEquals(RatisRelocationController.Phase.ACTIVE,controller.advance(handle,64));assertEquals(2,f.adminCalls);assertTrue(f.receiver.admissionOpen());assertNull(f.receiver.topologyBirth());assertNull(f.receiver.topologyActivation());assertArrayEquals(f.registration.encode(),f.receiver.topologyLearnerRegistration().encode());assertArrayEquals(next.payloadDigest(),RangeDirectoryCodec.decode(f.read(f.domainKey())).payloadDigest());}
        }
    }
    @Test void retainedJournalBoundSurvivesRestartAndFailedCheckpointCannotDispatch()throws Exception {
        try(var f=new Workflow()){
            var journal=Files.createDirectories(root.resolve("bounded-controller"));
            try(var controller=new RatisRelocationController(journal,f.source.schema.clusterId(),f.source.schema.databaseId(),f.binding,f.control,f.data,r->{throw new AssertionError();})){
                var handle=controller.prepare(f.source.definition);var originalCut=f.catalogue.applied();var hidden=root.resolve("hidden-controller");Files.move(journal,hidden);
                try{assertThrows(IOException.class,()->controller.advance(handle,1));assertThrows(IOException.class,()->controller.advance(handle,1));assertEquals(originalCut,f.catalogue.applied(),"Unforced in-memory proposal must never be dispatched");}finally{Files.move(hidden,journal);}
                controller.advance(handle,2);assertNotEquals(originalCut,f.catalogue.applied());
                for(int i=1;i<8;i++)controller.prepare(new TopologyChange.Definition(new UUID(201,i),f.source.definition.sourceDirectory(),f.source.definition.targetDirectory()));
            }
            try(var controller=new RatisRelocationController(journal,f.source.schema.clusterId(),f.source.schema.databaseId(),f.binding,f.control,f.data,r->{throw new AssertionError();})){
                assertThrows(IOException.class,()->controller.prepare(new TopologyChange.Definition(new UUID(201,9),f.source.definition.sourceDirectory(),f.source.definition.targetDirectory())));
                for(int i=1;i<8;i++)assertEquals(RatisRelocationController.Phase.DEFINITION,controller.reopen(new UUID(201,i)).phase());
            }
        }
    }

}
