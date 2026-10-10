package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** SEED executes at an existing parent owner; a local birth is not child-prefix readiness. */
public final class RatisTopologySeedRpc {
    public static final int KIND=92,MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+52+TopologyChange.MAX_DEFINITION_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+8+MAX_REQUEST_BYTES+NativeTopologySeed.MAX_BYTES;
    private RatisTopologySeedRpc() {}
    public record Request(TopologyChange.Definition definition,UUID catalogue,UUID anchorGroup,UUID replica){
        public Request{Objects.requireNonNull(definition);Objects.requireNonNull(catalogue);Objects.requireNonNull(anchorGroup);Objects.requireNonNull(replica);if(definition.kind()==TopologyChange.Kind.RELOCATE)throw new IllegalArgumentException("SEED requires split/merge");var range=definition.parents().stream().filter(parent->parent.groupId().equals(anchorGroup)).findFirst().orElseThrow(()->new IllegalArgumentException("SEED anchor is not a parent"));if(range.replicas().stream().noneMatch(owner->owner.replicaId().equals(replica)))throw new IllegalArgumentException("SEED owner is outside parent");}
        public RangeDescriptor range(){return definition.parents().stream().filter(parent->parent.groupId().equals(anchorGroup)).findFirst().orElseThrow();}
        public ReplicaDescriptor owner(){return range().replicas().stream().filter(owner->owner.replicaId().equals(replica)).findFirst().orElseThrow();}
        public byte[] originalAdmissionDigest(){byte[] state=TopologyChange.State.begin(definition.header()).beginFencing().encode();return OperatorUpload.sha256(ByteBuffer.allocate(16+state.length).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).put(state).array());}
        public void requireSeed(NativeTopologySeed seed){if(!Arrays.equals(seed.definition().encode(),definition.encode()) || !seed.localReplica().equals(owner()))throw new IllegalArgumentException("SEED artifact changed definition/local owner");for(var freeze:seed.parentFreezes().values())freeze.requireRequest(definition.header(),originalAdmissionDigest());}
    }
    public record Reply(Request request,NativeTopologySeed seed){public Reply{Objects.requireNonNull(request);Objects.requireNonNull(seed);request.requireSeed(seed);}}
    public static byte[] request(Request request){byte[] definition=request.definition().encode();var out=ByteBuffer.allocate(52+definition.length);ScopedTransactionRpc.putBlob(out,definition);ScopedTransactionRpc.uuid(out,request.catalogue());ScopedTransactionRpc.uuid(out,request.anchorGroup());ScopedTransactionRpc.uuid(out,request.replica());return ScopedTransactionRpc.frame(KIND,out.array());}
    public static Request decodeRequest(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES);if(packet.kind()!=KIND)throw new IllegalArgumentException("Wrong SEED request kind");var in=ByteBuffer.wrap(packet.body());var definition=TopologyChange.Definition.decode(ScopedTransactionRpc.blob(in,TopologyChange.MAX_DEFINITION_BYTES,false));var request=new Request(definition,ScopedTransactionRpc.uuid(in),ScopedTransactionRpc.uuid(in),ScopedTransactionRpc.uuid(in));if(in.hasRemaining() || !Arrays.equals(bytes,request(request)))throw new IllegalArgumentException("Noncanonical SEED request");return request;}
    public static byte[] reply(Reply reply){byte[] request=request(reply.request()),seed=reply.seed().encode();var out=ByteBuffer.allocate(8+request.length+seed.length);ScopedTransactionRpc.putBlob(out,request);ScopedTransactionRpc.putBlob(out,seed);return ScopedTransactionRpc.frame(KIND,out.array());}
    public static Reply decodeReply(byte[] bytes,Request expected){var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=KIND)throw new IllegalArgumentException("Wrong SEED reply kind");var in=ByteBuffer.wrap(packet.body());var request=decodeRequest(ScopedTransactionRpc.blob(in,MAX_REQUEST_BYTES,false));var seed=NativeTopologySeed.decode(ScopedTransactionRpc.blob(in,NativeTopologySeed.MAX_BYTES,false));if(in.hasRemaining() || !Arrays.equals(request(request),request(expected)))throw new IllegalArgumentException("SEED reply crossed original request");return new Reply(request,seed);}
}
