package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/** Owner-directed historical completion proof, independent of the current catalogue HEAD. */
final class RatisRecoveryObligationRpc {
    static final int KIND=122,MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+TopologyChange.MAX_HEADER_BYTES+48;
    record Target(UUID catalogue,TopologyChange.Header header,TopologyChange.Key key){
        Target {Objects.requireNonNull(catalogue);RatisTopologyRecoveryObligations.reserve(header).requireOwner(key);}
    }
    record Local(RatisTopologySourceSnapshotRpc.Request request,RatisTopologyRecoveryObligations.Completion completion,NativeTopologyFreeze freeze){}
    static byte[] request(Target target){byte[] header=target.header().encode();var out=ByteBuffer.allocate(48+header.length);ScopedTransactionRpc.uuid(out,target.catalogue());ScopedTransactionRpc.uuid(out,target.key().groupId());ScopedTransactionRpc.uuid(out,target.key().replicaId());out.put(header);return ScopedTransactionRpc.frame(KIND,out.array());}
    static Target request(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);if(packet.kind()!=KIND || packet.body().length<48)throw new IllegalArgumentException("Invalid recovery obligation target");var in=ByteBuffer.wrap(packet.body());UUID catalogue=ScopedTransactionRpc.uuid(in);var key=new TopologyChange.Key(ScopedTransactionRpc.uuid(in),ScopedTransactionRpc.uuid(in));byte[] header=new byte[in.remaining()];in.get(header);return new Target(catalogue,TopologyChange.Header.decode(header),key);}
    static byte[] reply(Target target,RatisTopologyRecoveryObligations.Completion completion){byte[] request=request(target);return ScopedTransactionRpc.frame(KIND,ByteBuffer.allocate(request.length+97).put(request).put(RatisTopologyRecoveryObligations.encodeCompletion(completion)).array());}
    static RatisTopologyRecoveryObligations.Completion reply(byte[] bytes,Target target){var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES+ScopedTransactionRpc.HEADER_BYTES+97);byte[] request=request(target);if(packet.kind()!=KIND || packet.body().length!=request.length+97 || !Arrays.equals(request,Arrays.copyOf(packet.body(),request.length)))throw new IllegalArgumentException("Recovery completion changed original obligation");return RatisTopologyRecoveryObligations.decodeCompletion(ByteBuffer.wrap(packet.body(),request.length,97).slice());}
    static long proofBytes(ReplicatedRangeStore store){var metadata=store.metadataRecoveryRegistration();var current=store.currentRangeRecoveryRegistration();return 4096L+16L*(65620L+2L*RatisTopologyRecoveryObligations.MAX_BYTES+(metadata!=null?metadata.encodedSize():current!=null?current.encodedSize():0));}
    static Local local(ReplicatedRangeStore store)throws Exception {
        var metadata=store.metadataRecoveryRegistration();var current=store.currentRangeRecoveryRegistration();
        if(metadata==null && (current==null || store.localMetadataDiscoveryActive()))return null;
        var request=metadata!=null?new RatisTopologySourceSnapshotRpc.Request(metadata.transferId(),metadata.freeze().header(),metadata.catalogue(),metadata.receiver(),RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,metadata.groupId(),metadata.sourceDirectory()):new RatisTopologySourceSnapshotRpc.Request(current.transferId(),null,current.catalogue(),current.receiver(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,current.groupId(),current.directory());
        byte[] receipt=store.recoveryCompletionReceipt();if(receipt==null)return null;
        byte[] registration=metadata!=null?metadata.digest():current.digest();if(!Arrays.equals(registration,Arrays.copyOf(receipt,32)))throw new IOException("Recovery registration changed during completion proof");
        var cut=ByteBuffer.wrap(receipt,receipt.length-16,16);return new Local(request,new RatisTopologyRecoveryObligations.Completion(request.transferId(),request.metadataOnly()?RatisTopologyRecoveryObligations.ProofKind.METADATA_REGISTRATION:RatisTopologyRecoveryObligations.ProofKind.CURRENT_REGISTRATION,RatisConfiguredRecovery.requestDigest(request),OperatorUpload.sha256(receipt),new ApplyPosition(cut.getLong(),cut.getLong())),metadata==null?null:metadata.freeze());
    }
    static Local local(RatisConfiguredRecovery.LocalMetadataProof proof,NativeTopologyFreeze freeze){var request=proof.request();return new Local(request,new RatisTopologyRecoveryObligations.Completion(request.transferId(),RatisTopologyRecoveryObligations.ProofKind.LOCAL_METADATA,RatisConfiguredRecovery.requestDigest(request),proof.digest(),proof.completion().completedCut()),freeze);}
    static RatisTopologyRecoveryObligations.Completion evidence(ReplicatedRangeStore store,Target target,ReplicaDescriptor actual)throws Exception {return evidence(store,target,actual,local(store));}
    static RatisTopologyRecoveryObligations.Completion evidence(ReplicatedRangeStore store,Target target,ReplicaDescriptor actual,Local local)throws Exception {
        if(local==null)throw new IOException("Original recovery has no durable native completion");var request=local.request();
        if(actual==null || !request.receiver().equals(actual) || !request.catalogue().equals(target.catalogue()) || !request.sourceGroup().equals(target.key().groupId()) || !actual.replicaId().equals(target.key().replicaId()) || !request.clusterId().equals(target.header().clusterId()) || !request.databaseId().equals(target.header().databaseId()) || !request.domainId().equals(target.header().domainId()))throw new IOException("Recovery completion crossed original owner/namespace");
        var participant=java.util.stream.Stream.concat(target.header().parents().stream(),target.header().targets().stream()).filter(p->p.groupId().equals(target.key().groupId())).findFirst().orElseThrow();
        if(!participant.owner(actual.replicaId()).equals(TopologyChange.Owner.of(actual)) || request.rangeEpoch()!=participant.rangeEpoch() || !request.range().rangeId().equals(participant.rangeId()))throw new IOException("Recovery completion changed original obligation cohort");
        boolean parent=target.header().parents().stream().anyMatch(p->p.groupId().equals(target.key().groupId()));
        if(parent && (!request.metadataOnly() || !Arrays.equals(request.header().encode(),target.header().encode()) || !Arrays.equals(request.sourceDirectory().payloadDigest(),target.header().sourceDirectoryDigest())))throw new IOException("Parent recovery obligation requires exact consuming META/FREEZE authority");
        if(request.metadataOnly()){
            var registration=store.metadataRecoveryRegistration();var freeze=local.freeze();
            if(freeze==null || !Arrays.equals(freeze.header().encode(),request.header().encode()) || !Arrays.equals(request.sourceDirectory().payloadDigest(),request.header().sourceDirectoryDigest()) || !freeze.equals(store.topologyFreeze()))throw new IOException("META completion lost exact native consuming source authority");
            if(local.completion().kind()==RatisTopologyRecoveryObligations.ProofKind.METADATA_REGISTRATION && (registration==null || !registration.transferId().equals(request.transferId()) || !freeze.equals(registration.freeze())))throw new IOException("META completion lost exact native registration");
            var consumed=request.header().participant(TopologyChange.Stage.FREEZE,request.sourceGroup());
            if(!consumed.rangeId().equals(participant.rangeId()) || consumed.rangeEpoch()!=participant.rangeEpoch() || !consumed.owner(actual.replicaId()).equals(TopologyChange.Owner.of(actual)) || !parent && request.header().sourceGeneration()<target.header().targetGeneration())throw new IOException("META completion does not consume original target lineage");
            freeze.requireParent(request.sourceGroup(),request.rangeEpoch(),request.domainId(),request.schemaDigest(),request.range().lowerInclusive(),request.range().upperExclusive());freeze.requireCut(local.completion().completedCut());
        }else{
            var registration=store.currentRangeRecoveryRegistration();var origin=registration==null?null:registration.origin();
            if(origin==null || !registration.transferId().equals(request.transferId()) || !Arrays.equals(origin.seed().header().encode(),target.header().encode()) || !origin.activation().birth().groupId().equals(target.key().groupId()))throw new IOException("CURRENT target completion lacks its original native birth/activation provenance");
            origin.activation().requireCut(local.completion().completedCut());
        }
        var directory=request.sourceDirectory();long minimumGeneration=target.header().parents().stream().anyMatch(p->p.groupId().equals(target.key().groupId()))?target.header().sourceGeneration():target.header().targetGeneration();
        if(directory.generation()<minimumGeneration || directory.schemaEpoch()<target.header().schemaEpoch() || directory.schemaEpoch()==target.header().schemaEpoch() && !Arrays.equals(directory.schemaDigest(),target.header().schemaDigest()))throw new IOException("Recovery completion predates original obligation placement/schema");
        return local.completion();
    }
}
