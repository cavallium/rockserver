package it.cavallium.rockserver.distributed.ratis;

import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

class RatisTopologySourceSnapshotRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void canonicalImageMetadataRoundtripNeverCarriesAHostPath()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){
            var request=fixture.request(50);assertEquals(request(PREPARE,request).length,encodedRequestBytes(request));var prepared=service.prepare(request);
            for(int kind:new int[]{PREPARE,TRANSFER,RELEASE})assertArrayEquals(request(kind,request),request(kind,request(request(kind,request))));
            var bytes=reply(PREPARE,prepared);assertEquals(bytes.length,prepared.encodedSize());assertEquals(bytes.length,preparedBytes(prepared));var decoded=reply(bytes,PREPARE,request);assertEquals(bytes.length,decoded.encodedSize());
            assertArrayEquals(bytes,reply(PREPARE,decoded));assertArrayEquals(prepared.image().sha256(),decoded.image().sha256());assertArrayEquals(prepared.sourceCfg().data(),decoded.sourceCfg().data());assertTrue(decoded.schemaRows().isEmpty());
            assertFalse(new String(bytes,java.nio.charset.StandardCharsets.ISO_8859_1).contains(root.toString()));
            assertThrows(IllegalArgumentException.class,()->reply(bytes,TRANSFER,request));assertThrows(IllegalArgumentException.class,()->reply(bytes,PREPARE,fixture.request(51)));
            assertThrows(IllegalArgumentException.class,()->reply(Arrays.copyOf(bytes,bytes.length-1),PREPARE,request));
        }
    }
    @Test void measuredCurrentWithBirthAndSchemaHistoryAndMetadataFreezeMatchExactWireBytes()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("sizes"),false)){
            var split=source.split();source.createColumn(source.first.domainId(),"μήκος");var current=source.routes.get(source.first.domainId()).directory();var leaf=current.ranges().getFirst();var parent=split.parents().getFirst();var helper=new RatisNodeRecoverySourceTest();
            var user=new Request(new UUID(94,3),null,source.plan.catalogue().groupId(),leaf.replicas().getFirst(),Purpose.CURRENT_LEAF,leaf.groupId(),current);
            var metadata=new Request(new UUID(94,4),split.header(),source.plan.catalogue().groupId(),parent.replicas().getFirst(),Purpose.ANCESTOR_METADATA,parent.groupId(),split.sourceDirectory());
            for(var request:List.of(user,metadata)){
                var prepared=helper.prepare(source,request);if(request.metadataOnly())assertNotNull(prepared.metadataFreeze());else{assertNotNull(prepared.image().topologyOrigin());assertFalse(prepared.schemaRows().isEmpty());}
                for(int kind:new int[]{PREPARE,TRANSFER}){var encoded=reply(kind,prepared);assertEquals(encoded.length,prepared.encodedSize());var decoded=reply(encoded,kind,request);assertEquals(encoded.length,decoded.encodedSize());assertArrayEquals(encoded,reply(kind,decoded));}
                assertTrue(prepared.encodedSize()<maximumPreparedBytes(request));
            }
        }
    }
    @Test void malformedRequestLengthsAndKindsAreRejectedBeforeHostWork()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            byte[] bytes=request(PREPARE,fixture.request(50));var oversized=bytes.clone();ByteBuffer.wrap(oversized).putInt(ScopedTransactionRpc.HEADER_BYTES+33,Integer.MAX_VALUE);
            assertThrows(IllegalArgumentException.class,()->request(oversized));assertThrows(IllegalArgumentException.class,()->request(Arrays.copyOf(bytes,bytes.length-1)));
            assertThrows(IllegalArgumentException.class,()->request(108,fixture.request(50)));assertEquals(0,fixture.captures.get());
        }
    }
    @Test void registeredRecoveryHasExactCurrentPurposeAndReceiverBinding()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){
            var initial=service.prepare(fixture.request(50));var source=fixture.definition.targetDirectory();var before=source.ranges().getFirst();var listener=fixture.receiver;
            var voter=new ReplicaDescriptor(listener.nodeId(),listener.nodeIncarnation(),listener.replicaId(),listener.peerId(),listener.address(),ReplicaDescriptor.Role.VOTER);
            var targetRange=new RangeDescriptor(before.rangeId(),before.groupId(),before.rangeEpoch(),before.configurationVersion()+1,before.lowerInclusive(),before.upperExclusive(),before.topologyState(),List.of(fixture.donor,voter));
            var target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),List.of(targetRange));
            var definition=new TopologyChange.Definition(new UUID(94,1),source,target);
            var recovery=new Request(new UUID(94,2),definition.header(),initial.request().catalogue(),voter,Purpose.RECOVERY);
            assertEquals(Purpose.RECOVERY,request(request(PREPARE,recovery)).purpose());
            assertArrayEquals(RatisTopologyLearnerRecoveryRpc.request(recovery),RatisTopologyLearnerRecoveryRpc.request(RatisTopologyLearnerRecoveryRpc.decode(RatisTopologyLearnerRecoveryRpc.request(recovery))));
            var prepared=new Prepared(recovery,initial.image(),initial.schema(),initial.sourceOwner(),initial.sourceCfg(),initial.schemaRows(),initial.metadataFreeze());
            assertEquals(reply(PREPARE,prepared).length,prepared.encodedSize());var encoded=RatisTopologyLearnerRecoveryRpc.reply(prepared);assertArrayEquals(encoded,RatisTopologyLearnerRecoveryRpc.reply(RatisTopologyLearnerRecoveryRpc.reply(encoded,recovery)));
            assertThrows(IllegalArgumentException.class,()->new Request(recovery.transferId(),definition.header(),recovery.catalogue(),voter,Purpose.BOOTSTRAP));
            assertThrows(IllegalArgumentException.class,()->RatisTopologyLearnerRecoveryRpc.request(initial.request()));
            assertThrows(IllegalArgumentException.class,()->RatisTopologyLearnerRecoveryRpc.reply(encoded,fixture.request(51)));
            var unknown=request(PREPARE,recovery);unknown[ScopedTransactionRpc.HEADER_BYTES]=(byte)127;assertThrows(IllegalArgumentException.class,()->request(unknown));
            assertEquals(1,fixture.captures.get());
        }
    }

    @Test void actualBootstrapSourceConfigurationRoundtripsWithoutInventingACommittedIndex()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root,true);var service=new RatisTopologySourceSnapshotService(fixture,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){
            var request=fixture.request(50);assertEquals(request(PREPARE,request).length,encodedRequestBytes(request));var prepared=service.prepare(request);assertEquals(0,fixture.store.configuration().term());assertEquals(-1,fixture.store.configuration().index());
            var decoded=reply(reply(PREPARE,prepared),PREPARE,request);assertEquals(0,decoded.sourceCfg().term());assertEquals(-1,decoded.sourceCfg().index());assertTrue(decoded.image().position().term()>0);
            assertSame(prepared,service.transfer(request).get(5,TimeUnit.SECONDS));service.release(request);
            byte[] data=prepared.sourceCfg().data();
            for(var config:List.of(new it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.Configuration(0,0,data),new it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.Configuration(2,0,data),new it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.Configuration(1,2,data)))
                assertThrows(IllegalArgumentException.class,()->new Prepared(request,prepared.image(),prepared.schema(),prepared.sourceOwner(),config,prepared.schemaRows(),prepared.metadataFreeze()));
            assertThrows(IllegalArgumentException.class,()->new it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.Configuration(1,-1,data));
        }
    }

}
