package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Owner-directed retirement of obsolete CURRENT capture authority, without an installed-image claim. */
public final class RatisRecoveryObsoleteRpc {
    public static final int KIND=121;
    public static final int MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+40+RatisTopologySourceSnapshotRpc.MAX_REQUEST_BYTES+NativeRecoverySupersession.MAX_SCOPE_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+32;
    private RatisRecoveryObsoleteRpc(){}
    public record Request(RatisTopologySourceSnapshotRpc.Request original,NativeRecoverySupersession.Scope scope,byte[] fenceDigest) {
        public Request {
            Objects.requireNonNull(original);Objects.requireNonNull(scope);
            if(fenceDigest==null || fenceDigest.length!=32)throw new IllegalArgumentException("Invalid receiver fence digest");fenceDigest=fenceDigest.clone();
            if(original.purpose()!=RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF)throw new IllegalArgumentException("Only CURRENT captures may become obsolete");
            var hash=OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,original));var old=original.range();var range=scope.range();var a=original.receiver();var b=scope.receiver();
            if(!Arrays.equals(hash,scope.originalCurrentRequestDigest()) && !Arrays.equals(hash,scope.effectiveCurrentRequestDigest()))throw new IllegalArgumentException("Obsolete request is outside original/effective CURRENT identities");
            if(!original.clusterId().equals(scope.sourceDirectory().clusterId()) || !original.databaseId().equals(scope.sourceDirectory().databaseId()) || !original.catalogue().equals(scope.catalogue()) || !original.domainId().equals(scope.sourceDirectory().domainId())
                    || !original.sourceGroup().equals(scope.groupId()) || !old.rangeId().equals(range.rangeId()) || old.rangeEpoch()!=range.rangeEpoch() || !Arrays.equals(old.lowerInclusive(),range.lowerInclusive()) || !Arrays.equals(old.upperExclusive(),range.upperExclusive())
                    || !a.nodeId().equals(b.nodeId()) || !a.nodeIncarnation().equals(b.nodeIncarnation()) || !a.replicaId().equals(b.replicaId()) || !a.peerId().equals(b.peerId()) || !a.address().equals(b.address())
                    || original.sourceDirectory().generation()>scope.sourceDirectory().generation())throw new IllegalArgumentException("Obsolete capture changed immutable receiver/range authority");
        }
        @Override public byte[] fenceDigest(){return fenceDigest.clone();}
        public int encodedSize(){return Math.addExact(ScopedTransactionRpc.HEADER_BYTES+40,Math.addExact(RatisTopologySourceSnapshotRpc.encodedRequestBytes(original),scope.encodedSize()));}
        public byte[] digest(){return OperatorUpload.sha256(request(this));}
        public RatisRecoverySupersessionRpc.Request evidence(){var directory=scope.sourceDirectory();var range=scope.range();return new RatisRecoverySupersessionRpc.Request(directory.clusterId(),directory.databaseId(),scope.catalogue(),directory.domainId(),range.rangeId(),scope.groupId(),range.rangeEpoch(),TopologyChange.Owner.of(scope.receiver()),scope.digest(),fenceDigest);}
        @Override public boolean equals(Object other){return other instanceof Request r && Arrays.equals(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,original),RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,r.original)) && scope.equals(r.scope) && Arrays.equals(fenceDigest,r.fenceDigest);}
        @Override public int hashCode(){return Arrays.hashCode(digest());}
    }
    public static byte[] request(Request request){byte[] original=RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request.original()),scope=request.scope().encode();var out=ByteBuffer.allocate(40+original.length+scope.length);ScopedTransactionRpc.putBlob(out,original);ScopedTransactionRpc.putBlob(out,scope);out.put(request.fenceDigest());return ScopedTransactionRpc.frame(KIND,out.array());}
    public static Request decode(byte[] bytes){var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES);if(packet.kind()!=KIND)throw new IllegalArgumentException("Wrong obsolete capture request kind");var in=ByteBuffer.wrap(packet.body());var original=RatisTopologySourceSnapshotRpc.request(ScopedTransactionRpc.blob(in,RatisTopologySourceSnapshotRpc.MAX_REQUEST_BYTES,false));var scope=NativeRecoverySupersession.Scope.decode(ScopedTransactionRpc.blob(in,NativeRecoverySupersession.MAX_SCOPE_BYTES,false));if(in.remaining()!=32)throw new IllegalArgumentException("Invalid obsolete capture fence digest");byte[] fence=new byte[32];in.get(fence);var request=new Request(original,scope,fence);if(!Arrays.equals(bytes,request(request)))throw new IllegalArgumentException("Noncanonical obsolete capture request");return request;}
    public static byte[] reply(Request request){return ScopedTransactionRpc.frame(KIND,request.digest());}
    public static void reply(byte[] bytes,Request expected){var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=KIND || packet.body().length!=32 || !Arrays.equals(packet.body(),expected.digest()))throw new IllegalArgumentException("Obsolete capture acknowledgement changed exact request");}
}
