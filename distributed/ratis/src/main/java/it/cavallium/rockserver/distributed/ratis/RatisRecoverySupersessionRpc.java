package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Exact owner query for an acknowledged local receiver fence; no absent or readiness response. */
public final class RatisRecoverySupersessionRpc {
    public static final int KIND=120;
    public static final int REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+217,MAX_REQUEST_BYTES=REQUEST_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+32;
    private RatisRecoverySupersessionRpc() {}

    public record Request(UUID cluster,UUID database,UUID catalogue,UUID domain,UUID range,UUID group,long epoch,
                          TopologyChange.Owner receiver,byte[] scopeDigest,byte[] fenceDigest) {
        public Request {
            Objects.requireNonNull(cluster);Objects.requireNonNull(database);Objects.requireNonNull(catalogue);Objects.requireNonNull(domain);Objects.requireNonNull(range);Objects.requireNonNull(group);Objects.requireNonNull(receiver);
            if(epoch<0 || scopeDigest==null || scopeDigest.length!=32 || fenceDigest==null || fenceDigest.length!=32)throw new IllegalArgumentException("Invalid receiver supersession query");
            scopeDigest=scopeDigest.clone();fenceDigest=fenceDigest.clone();
        }
        public static Request of(NativeRecoverySupersession value){
            Objects.requireNonNull(value);var directory=value.sourceDirectory();var range=value.range();
            return new Request(directory.clusterId(),directory.databaseId(),value.scope().catalogue(),directory.domainId(),range.rangeId(),value.groupId(),range.rangeEpoch(),TopologyChange.Owner.of(value.receiver()),value.scopeDigest(),value.digest());
        }
        @Override public byte[] scopeDigest(){return scopeDigest.clone();}
        @Override public byte[] fenceDigest(){return fenceDigest.clone();}
        public byte[] digest(){return OperatorUpload.sha256(request(this));}
        public void requireBinding(NativeRecoverySupersession value,ReplicaDescriptor actualReplica){
            if(!equals(of(value)) || !receiver.equals(TopologyChange.Owner.of(actualReplica)))throw new IllegalArgumentException("Supersession query changed fence scope or actual owner");
        }
        @Override public boolean equals(Object other){return other instanceof Request r && cluster.equals(r.cluster) && database.equals(r.database) && catalogue.equals(r.catalogue) && domain.equals(r.domain) && range.equals(r.range) && group.equals(r.group) && epoch==r.epoch && receiver.equals(r.receiver) && Arrays.equals(scopeDigest,r.scopeDigest) && Arrays.equals(fenceDigest,r.fenceDigest);}
        @Override public int hashCode(){return Objects.hash(cluster,database,catalogue,domain,range,group,epoch,receiver,Arrays.hashCode(scopeDigest),Arrays.hashCode(fenceDigest));}
    }
    public static byte[] request(Request request){
        var out=ByteBuffer.allocate(217);for(var id:List.of(request.cluster(),request.database(),request.catalogue(),request.domain(),request.range(),request.group()))ScopedTransactionRpc.uuid(out,id);
        out.putLong(request.epoch());ScopedTransactionRpc.uuid(out,request.receiver().replicaId());out.put((byte)request.receiver().role().ordinal()).put(request.receiver().identityDigest());out.put(request.scopeDigest()).put(request.fenceDigest());
        return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static Request decode(byte[] encoded){
        var packet=ScopedTransactionRpc.parse(encoded,MAX_REQUEST_BYTES);if(packet.kind()!=KIND || packet.body().length!=217)throw new IllegalArgumentException("Invalid supersession query frame");
        var in=ByteBuffer.wrap(packet.body());var cluster=ScopedTransactionRpc.uuid(in);var database=ScopedTransactionRpc.uuid(in);var catalogue=ScopedTransactionRpc.uuid(in);var domain=ScopedTransactionRpc.uuid(in);var range=ScopedTransactionRpc.uuid(in);var group=ScopedTransactionRpc.uuid(in);long epoch=in.getLong();var replica=ScopedTransactionRpc.uuid(in);int role=in.get()&255;
        if(role>=ReplicaDescriptor.Role.values().length)throw new IllegalArgumentException("Invalid supersession receiver role");
        byte[] identity=new byte[32],scope=new byte[32],fence=new byte[32];in.get(identity).get(scope).get(fence);
        var request=new Request(cluster,database,catalogue,domain,range,group,epoch,new TopologyChange.Owner(replica,ReplicaDescriptor.Role.values()[role],identity),scope,fence);
        if(!Arrays.equals(encoded,request(request)))throw new IllegalArgumentException("Noncanonical supersession query");return request;
    }
    /** Caller must first hold and qualify the exact acknowledged native fence and actual receiver. */
    public static byte[] reply(Request request){return ScopedTransactionRpc.frame(KIND,request.digest());}
    public static void reply(byte[] encoded,Request expected){
        var packet=ScopedTransactionRpc.parse(encoded,MAX_REPLY_BYTES);
        if(packet.kind()!=KIND || packet.body().length!=32 || !Arrays.equals(packet.body(),expected.digest()))throw new IllegalArgumentException("Supersession acknowledgement changed original request");
        if(!Arrays.equals(encoded,reply(expected)))throw new IllegalArgumentException("Noncanonical supersession acknowledgement");
    }
}
