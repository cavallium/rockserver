package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Actual native receipt queried through the owner-local DATA dispatcher; controlled Division, no socket. */
class RatisRecoveryImageReceiverTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisRecoveryImageRpc.Evidence probe(RatisRangeStateMachine machine,RatisRecoveryImageRpc.Request request)throws Exception {
        byte[] reply=machine.queryStale(Message.valueOf(ByteString.copyFrom(RatisRecoveryImageRpc.request(request))),0).get(5,TimeUnit.SECONDS).getContent().toByteArray();return RatisRecoveryImageRpc.reply(reply,request);
    }
    @Test void metadataReceiverRefusesRegistrationAndPartialInstallThenReportsExactReceiptBeforeReady()throws Exception {
        var helper=new RatisSourceOutcomeTest();var limits=new NativeRangeImage.Limits(4<<20,32<<20,16);
        try(var f=helper.new Fixture(root.resolve("source"),false);var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("receiver"))) {
            var source=f.home.source;var image=root.resolve("metadata.image");var manifest=NativeRangeImage.writeMetadata(image,source.store,limits);var cfg=source.store.configuration();
            var scope=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9500),source.definition.header(),source.catalogue,source.local,RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,source.groupId,source.directory);
            var registration=new NativeMetadataRecoveryRegistration(scope.transferId(),scope.catalogue(),new ApplyPosition(1,1),source.directory,source.freeze,source.local,source.local,cfg.term(),cfg.index(),cfg.data(),manifest.position(),manifest.sha256(),Set.of(),null);
            var target=pool.openMetadataRecovery(registration);target.enableSessionControls();target.enableCdc(source.rangeId);var machine=new RatisRangeStateMachine(target,source.group,limits);
            machine.topologyAuthority((header,stage)->{throw new IOException("Image receipt cannot borrow catalogue authority");},source.directory,source.local,pool);machine.proofQueries(source.queries);
            var storage=RaftStorage.newBuilder().setDirectory(root.resolve("receiver-raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();
            var server=mock(RaftServer.class);var division=mock(RaftServer.Division.class);var info=mock(DivisionInfo.class);var actual=mock(RaftConfiguration.class);
            when(server.getId()).thenReturn(source.peer.getId());when(server.getDivision(source.group.getGroupId())).thenReturn(division);when(division.getStateMachine()).thenReturn(machine);when(division.getInfo()).thenReturn(info);when(division.getRaftConf()).thenReturn(actual);when(info.isAlive()).thenReturn(true);when(info.getLifeCycleState()).thenReturn(LifeCycle.State.RUNNING);when(info.getCurrentTerm()).thenReturn(1L);
            for(var role:List.of(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER,org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)){when(actual.getPreviousPeers(role)).thenReturn(List.of());when(actual.getCurrentPeers(role)).thenReturn(role==org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER?List.of(source.peer):List.of());}when(actual.getLogEntryIndex()).thenAnswer(invocation->target.configuration().index());
            try {
                machine.initialize(server,source.group.getGroupId(),storage);var request=new RatisRecoveryImageRpc.Request(scope,manifest.position(),manifest.sha256());
                var absent=probe(machine,request);assertFalse(absent.imageInstalled());assertNotNull(absent.registrationDigest());assertNull(absent.completedCut());assertTrue(target.metadataRecoveryPending());
                assertThrows(IOException.class,()->target.installMetadataSnapshot(image,limits,batches->{throw new IOException("Controlled partial image");}));assertTrue(target.pendingInstallation());assertFalse(probe(machine,request).imageInstalled());
                target.installMetadataSnapshot(image,limits);machine.notifyTermIndexUpdated(manifest.position().term(),manifest.position().index());
                var installed=probe(machine,request);assertTrue(installed.imageInstalled());assertEquals(manifest.position(),installed.completedCut());assertArrayEquals(registration.digest(),installed.registrationDigest());assertArrayEquals(cfg.data(),installed.configuration().data());assertTrue(target.metadataRecoveryPending());
                assertThrows(IllegalStateException.class,target::metadataOutcomeState);assertThrows(IllegalStateException.class,()->target.estimateSourceOutcomeCredit(f.request));
                byte[] different=manifest.sha256();different[0]^=1;assertThrows(ExecutionException.class,()->probe(machine,new RatisRecoveryImageRpc.Request(scope,manifest.position(),different)));
                assertThrows(ExecutionException.class,()->probe(machine,new RatisRecoveryImageRpc.Request(scope,new ApplyPosition(1,manifest.position().index()+1),manifest.sha256())));
                assertThrows(ExecutionException.class,()->machine.queryStale(Message.valueOf(ByteString.copyFrom(RatisRecoveryImageRpc.request(request))),1).get(5,TimeUnit.SECONDS));
                var stranger=new ReplicaDescriptor(new UUID(0,9501),source.local.nodeIncarnation(),source.local.replicaId(),source.local.peerId(),source.local.address(),source.local.role());
                assertThrows(IllegalArgumentException.class,()->new RatisTopologySourceSnapshotRpc.Request(scope.transferId(),scope.header(),scope.catalogue(),stranger,scope.purpose(),scope.sourceGroup(),scope.sourceDirectory()));
            }finally{machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();}
        }
    }
}
