package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual JNI metadata image and committed Raft adapter replay; no network/quorum fixture. */
class RatisMetadataOutcomeReplayTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(4<<20,32<<20,16);
    static Message message(byte[] bytes){return RatisSourceOutcomeTest.message(bytes);}
    static byte[] replay(RatisRangeStateMachine machine,byte[] record,long index)throws Exception {
        var entry=LogEntryProto.newBuilder().setTerm(1).setIndex(index).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(record))).build();
        var tx=TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(entry).build();
        return machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray();
    }
    @Test void metadataArtifactColdLookupAndReleaseKeepSemanticImageKind()throws Exception {
        var helper=new RatisSourceOutcomeTest();
        try(var f=helper.new Fixture(root.resolve("artifact-source"),false)) {
            var source=f.home.source;var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9200),source.definition.header(),source.catalogue,source.local,RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,source.groupId,source.directory);
            assertEquals(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request).length,RatisTopologySourceSnapshotRpc.encodedRequestBytes(request));
            var current=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9201),null,source.catalogue,source.local,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,source.groupId,source.directory);
            assertEquals(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,current).length,RatisTopologySourceSnapshotRpc.encodedRequestBytes(current));
            var owned=java.nio.file.Files.createDirectory(root.resolve("artifact-ledger"));var image=root.resolve("source-metadata.image");
            RatisTopologySourceSnapshotRpc.Prepared prepared;
            try(var artifacts=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),128L<<20,1)) {
                artifacts.reserve(request);NativeRangeImage.Manifest manifest;var metadataFreeze=new NativeTopologyFreeze[1];ReplicatedRangeStore.Configuration configuration;
                try(var pin=source.store.captureMetadataForRecovery()) {
                    pin.visitState((key,value)->{if(RatisTopologySourceSnapshotRpc.metadataFreezeKey(pin.groupId(),key))metadataFreeze[0]=NativeTopologyFreeze.decode(value);});configuration=pin.configuration();
                    artifacts.capture(request,pin,source.schema,source.local);
                    manifest=NativeRangeImage.writeMetadataAdmitted(image,pin,pin.interval(),limits,NativeRangeImage.IoAdmission.UNLIMITED);
                }
                assertThrows(java.io.IOException.class,()->NativeRangeImage.inspect(image,limits));
                prepared=new RatisTopologySourceSnapshotRpc.Prepared(request,manifest,source.schema,source.local,configuration,Map.of(),metadataFreeze[0]);
                assertThrows(IllegalArgumentException.class,()->new RatisTopologySourceSnapshotRpc.Prepared(request,manifest,source.schema,source.local,configuration,Map.of(),null));
                var freeze=prepared.metadataFreeze();assertNotNull(freeze);var wrongBounds=new NativeTopologyFreeze(freeze.header(),freeze.groupId(),freeze.position(),NativeTopologyFreeze.boundsDigest(new byte[]{1},null),freeze.admissionDigest());
                assertThrows(IllegalArgumentException.class,()->new RatisTopologySourceSnapshotRpc.Prepared(request,manifest,source.schema,source.local,configuration,Map.of(),wrongBounds));
                var future=new NativeTopologyFreeze(freeze.header(),freeze.groupId(),new ApplyPosition(manifest.position().term(),manifest.position().index()+1),freeze.boundsDigest(),freeze.admissionDigest());assertThrows(IllegalArgumentException.class,()->new RatisTopologySourceSnapshotRpc.Prepared(request,manifest,source.schema,source.local,configuration,Map.of(),future));
                var changedDefinition=new TopologyChange.Definition(new UUID(0,9210),source.definition.sourceDirectory(),source.definition.targetDirectory());var changedHeader=new NativeTopologyFreeze(changedDefinition.header(),freeze.groupId(),freeze.position(),freeze.boundsDigest(),freeze.admissionDigest());assertThrows(IllegalArgumentException.class,()->new RatisTopologySourceSnapshotRpc.Prepared(request,manifest,source.schema,source.local,configuration,Map.of(),changedHeader));
                assertThrows(IllegalArgumentException.class,()->new RatisTopologySourceSnapshotRpc.Prepared(current,manifest,source.schema,source.local,configuration,Map.of(),freeze));
                var decoded=RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,prepared),RatisTopologySourceSnapshotRpc.PREPARE,request);assertEquals(freeze,decoded.metadataFreeze());
                var info=new org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo(new org.apache.ratis.server.storage.FileInfo(image,org.apache.ratis.util.MD5FileUtil.computeMd5ForFile(image.toFile())),manifest.position().term(),manifest.position().index());
                var changedAdmission=freeze.admissionDigest();changedAdmission[0]^=1;var foreignFreeze=new NativeTopologyFreeze(freeze.header(),freeze.groupId(),freeze.position(),freeze.boundsDigest(),changedAdmission);var foreign=new RatisTopologySourceSnapshotRpc.Prepared(request,manifest,source.schema,source.local,configuration,Map.of(),foreignFreeze);
                assertThrows(java.io.IOException.class,()->artifacts.retain(foreign,info));assertFalse(java.nio.file.Files.exists(owned.resolve(request.transferId()+".prepared")),"Mismatched image FREEZE cannot be published");
                artifacts.retain(prepared,info);
            }
            try(var artifacts=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),128L<<20,1)) {
                assertArrayEquals(prepared.image().sha256(),artifacts.lookup(request).prepared().image().sha256());assertEquals(prepared.metadataFreeze(),artifacts.lookup(request).prepared().metadataFreeze());
                artifacts.released(request);assertTrue(artifacts.isReleased(request));assertEquals(0,artifacts.reservedImageBytes());
            }
            try(var artifacts=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),128L<<20,1)){assertTrue(artifacts.isReleased(request));}
        }
    }
    @Test void coldCapturingMetadataPromotesOriginalImageWithItsExactFreeze()throws Exception {
        var helper=new RatisSourceOutcomeTest();
        try(var fixture=helper.new Fixture(root.resolve("capturing-source"),false)) {
            var source=fixture.home.source;var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9220),source.definition.header(),source.catalogue,source.local,RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,source.groupId,source.directory);
            var owned=java.nio.file.Files.createDirectory(root.resolve("capturing-ledger"));NativeRangeImage.Manifest image;
            try(var artifacts=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),128L<<20,1);var pin=source.store.captureMetadataForRecovery()) {
                artifacts.reserve(request);artifacts.capture(request,pin,source.schema,source.local);
                image=NativeRangeImage.writeMetadataAdmitted(artifacts.originalImagePath(request,pin.position()),pin,pin.interval(),limits,NativeRangeImage.IoAdmission.UNLIMITED);
                assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.CAPTURING,artifacts.publicationPhase(request));assertFalse(java.nio.file.Files.exists(owned.resolve(request.transferId()+".prepared")));
            }
            try(var cold=new RatisTopologySourceSnapshotArtifacts(owned,limits,2*limits.maxImageBytes(),128L<<20,1)) {
                var recovered=cold.lookup(request);assertNotNull(recovered);assertEquals(source.freeze,recovered.prepared().metadataFreeze());assertEquals(image.position(),recovered.prepared().image().position());assertArrayEquals(image.sha256(),recovered.prepared().image().sha256());
                assertEquals(RatisTopologySourceSnapshotArtifacts.PublicationPhase.READY,cold.publicationPhase(request));assertTrue(java.nio.file.Files.exists(owned.resolve(request.transferId()+".prepared")));
                cold.released(request);assertTrue(cold.isReleased(request));assertEquals(0,cold.reservedImageBytes());
            }
        }
    }
    @Test void pendingMetadataReplaysCreditPublicationDuplicatesAndFencedPublicationArtifact()throws Exception {
        var helper=new RatisSourceOutcomeTest();
        for(boolean expirePending:List.of(false,true))try(var f=helper.new Fixture(root.resolve("source-"+expirePending),false);var pool=new RatisTopologyRecoveryAnchorTest().pool(root.resolve("receiver-"+expirePending))) {
            var source=f.home.source;var image=root.resolve("metadata-"+expirePending+".image");
            var credit=f.prepare(new RatisSourceOutcomeRpc.Proposal(RatisSourceOutcomeRecord.Kind.CREDIT,f.request,List.of())).record();
            var manifest=NativeRangeImage.writeMetadata(image,source.store,limits);var cfg=source.store.configuration();
            var registration=new NativeMetadataRecoveryRegistration(new UUID(0,9100),source.catalogue,new ApplyPosition(1,1),source.directory,source.freeze,source.local,source.local,cfg.term(),cfg.index(),cfg.data(),manifest.position(),manifest.sha256(),Set.of());
            var target=pool.openMetadataRecovery(registration);target.enableSessionControls();target.enableCdc(source.rangeId);target.installMetadataSnapshot(image,limits);
            var machine=new RatisRangeStateMachine(target,source.group,limits);machine.cdcBinding(source.cluster,source.database,source.rangeId);
            var storage=RaftStorage.newBuilder().setDirectory(root.resolve("receiver-raft-"+expirePending).toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->{if(method.getName().equals("getId"))return source.peer.getId();if(method.getName().equals("toString"))return "metadata-tail-fixture";throw new AssertionError(method);});
            try {
                machine.initialize(identity,source.group.getGroupId(),storage);
                assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,()->target.estimateSourceOutcomeCredit(f.request));
                assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,f.apply(credit).outcome());long creditIndex=f.index;
                var applied=RatisSourceOutcomeRpc.applied(replay(machine,credit.encode(),creditIndex),credit);assertNotNull(applied.proof());
                assertArrayEquals(applied.proof(),RatisSourceOutcomeRpc.applied(replay(machine,credit.encode(),creditIndex),credit).proof());
                if(expirePending){var clock=source.clock(989,SessionEpochLedger.MAX_LIFETIME_MILLIS+20);f.applyControl(clock);replay(machine,DistributedCommandCodec.encode(clock),f.index);}
                f.abortHome(TransactionStatus.NoEffectOutcome.CONFLICT);var publication=f.prepare(new RatisSourceOutcomeRpc.Proposal(RatisSourceOutcomeRecord.Kind.PUBLISH,f.request,List.of())).record();
                var original=f.apply(publication);long publicationIndex=f.index;
                assertArrayEquals(original.proof(),RatisSourceOutcomeRpc.applied(replay(machine,publication.encode(),publicationIndex),publication).proof());
                assertArrayEquals(original.proof(),RatisSourceOutcomeRpc.applied(replay(machine,publication.encode(),publicationIndex),publication).proof());
                var creditRetry=RatisSourceOutcomeRpc.applied(replay(machine,credit.encode(),creditIndex),credit);
                if(expirePending){assertEquals(NativeSourceOutcomePublication.Outcome.FENCED,creditRetry.outcome());assertArrayEquals(applied.proof(),creditRetry.proof());assertNull(target.readCommittedSourceOutcome(f.request).credit());assertNotNull(target.readCommittedSourceOutcome(f.request).publication());assertNotNull(original.proof());}
                else assertNotNull(creditRetry.proof());
                assertThrows(IllegalStateException.class,()->target.sourceOutcomePublication(f.request,publication.result()));
                assertThrows(IllegalStateException.class,()->target.operationResult(f.request.original().operationId(),f.request.original().receiptDigest()));
                assertThrows(Exception.class,()->machine.queryLinearizable(message(RatisSourceOutcomeRpc.prepare(new RatisSourceOutcomeRpc.Proposal(RatisSourceOutcomeRecord.Kind.CREDIT,f.request,List.of())))).get(5,TimeUnit.SECONDS));
                if(!expirePending){var clock=source.clock(990,SessionEpochLedger.MAX_LIFETIME_MILLIS+20);f.applyControl(clock);replay(machine,DistributedCommandCodec.encode(clock),f.index);}
                var fenced=RatisSourceOutcomeRpc.applied(replay(machine,publication.encode(),publicationIndex),publication);assertEquals(NativeSourceOutcomePublication.Outcome.FENCED,fenced.outcome());var retained=target.readCommittedSourceOutcome(f.request).publication();
                if(expirePending){assertNotNull(retained);assertArrayEquals(original.proof(),fenced.proof());}
                else {assertNull(retained);assertNull(fenced.proof());}
                assertTrue(target.metadataRecoveryPending());target.completeMetadataRecovery(registration,target.applied(),target.configuration());
                assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(target.operationResult(f.request.original().operationId(),f.request.original().receiptDigest())).status());
                assertArrayEquals(fenced.proof(),RatisSourceOutcomeRpc.applied(replay(machine,publication.encode(),publicationIndex),publication).proof());
            }finally{machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();}
        }
    }
}
