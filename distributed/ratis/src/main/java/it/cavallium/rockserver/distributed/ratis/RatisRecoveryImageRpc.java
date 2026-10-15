package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.nio.ByteBuffer;
import java.util.*;

/** Exact receiver-local image receipt. This probe conveys no serving or voting authority. */
public final class RatisRecoveryImageRpc {
    public static final int KIND=116,MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+52+RatisTopologySourceSnapshotRpc.MAX_REQUEST_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+152+65536;
    private RatisRecoveryImageRpc() {}
    public record Request(RatisTopologySourceSnapshotRpc.Request source,ApplyPosition imageCut,byte[] imageHash) {
        public Request {Objects.requireNonNull(source);Objects.requireNonNull(imageCut);if(source.relocation() || imageCut.term()<1 || imageHash==null || imageHash.length!=32)throw new IllegalArgumentException("Invalid direct recovery image probe");imageHash=imageHash.clone();}
        @Override public byte[] imageHash(){return imageHash.clone();}
        public byte[] digest(){return OperatorUpload.sha256(request(this));}
    }
    public record Evidence(byte[] requestDigest,TopologyChange.Owner receiver,ApplyPosition completedCut,ReplicatedRangeStore.Configuration configuration,byte[] registrationDigest,boolean imageInstalled) {
        public Evidence {
            Objects.requireNonNull(receiver);
            if(requestDigest==null || requestDigest.length!=32 || registrationDigest!=null && registrationDigest.length!=32 || (completedCut==null)!=(configuration==null)
                    || completedCut!=null && (completedCut.term()<1 || configuration.term()>completedCut.term() || configuration.index()>completedCut.index() || configuration.index()==completedCut.index() && configuration.term()!=completedCut.term())
                    || imageInstalled && (registrationDigest==null || completedCut==null))throw new IllegalArgumentException("Invalid native image receipt evidence");
            requestDigest=requestDigest.clone();registrationDigest=registrationDigest==null?null:registrationDigest.clone();
        }
        @Override public byte[] requestDigest(){return requestDigest.clone();}
        @Override public byte[] registrationDigest(){return registrationDigest==null?null:registrationDigest.clone();}
        public void require(Request request){if(!Arrays.equals(requestDigest,request.digest()) || !receiver.equals(TopologyChange.Owner.of(request.source().receiver())) || imageInstalled && !RatisRelocationOwnerRpc.covers(completedCut,request.imageCut()))throw new IllegalArgumentException("Recovery image probe crossed receiver/request/prefix");}
    }
    public static byte[] request(Request request) {
        byte[] scope=RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request.source());
        var out=ByteBuffer.allocate(52+scope.length);ScopedTransactionRpc.putBlob(out,scope);out.putLong(request.imageCut().term()).putLong(request.imageCut().index()).put(request.imageHash());return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static Request decode(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES);if(packet.kind()!=KIND)throw new IllegalArgumentException("Wrong recovery image probe kind");var in=ByteBuffer.wrap(packet.body());
        var source=RatisTopologySourceSnapshotRpc.request(ScopedTransactionRpc.blob(in,RatisTopologySourceSnapshotRpc.MAX_REQUEST_BYTES,false));if(in.remaining()!=48)throw new IllegalArgumentException("Invalid image probe tail");
        var cut=new ApplyPosition(in.getLong(),in.getLong());byte[] hash=new byte[32];in.get(hash);var result=new Request(source,cut,hash);if(!Arrays.equals(bytes,request(result)))throw new IllegalArgumentException("Noncanonical recovery image probe");return result;
    }
    public static byte[] reply(Evidence evidence) {
        byte[] cfg=evidence.configuration()==null?new byte[0]:evidence.configuration().data();if(cfg.length>65536)throw new IllegalArgumentException("Recovery configuration exceeds bound");
        var out=ByteBuffer.allocate(152+cfg.length);out.put(evidence.requestDigest());ScopedTransactionRpc.uuid(out,evidence.receiver().replicaId());out.put((byte)evidence.receiver().role().ordinal()).put(evidence.receiver().identityDigest());out.put((byte)(evidence.completedCut()==null?0:1));
        out.putLong(evidence.completedCut()==null?0:evidence.completedCut().term()).putLong(evidence.completedCut()==null?0:evidence.completedCut().index()).putLong(evidence.configuration()==null?0:evidence.configuration().term()).putLong(evidence.configuration()==null?0:evidence.configuration().index());ScopedTransactionRpc.putBlob(out,cfg);
        out.put((byte)(evidence.registrationDigest()==null?0:1)).put(evidence.registrationDigest()==null?new byte[32]:evidence.registrationDigest()).put((byte)(evidence.imageInstalled()?1:0));return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static Evidence reply(byte[] bytes,Request request) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=KIND || packet.body().length<152)throw new IllegalArgumentException("Invalid recovery image evidence");var in=ByteBuffer.wrap(packet.body());byte[] digest=new byte[32];in.get(digest);UUID replica=ScopedTransactionRpc.uuid(in);int role=in.get()&255;if(role>=ReplicaDescriptor.Role.values().length)throw new IllegalArgumentException("Invalid receiver role");byte[] identity=new byte[32];in.get(identity);int completed=in.get()&255;long term=in.getLong(),index=in.getLong(),cfgTerm=in.getLong(),cfgIndex=in.getLong();byte[] cfg=ScopedTransactionRpc.blob(in,65536,false);if(in.remaining()!=34)throw new IllegalArgumentException("Invalid recovery image evidence tail");int registered=in.get()&255;byte[] registration=new byte[32];in.get(registration);int installed=in.get()&255;
        if(completed>1 || registered>1 || installed>1 || completed==0 && (term!=0 || index!=0 || cfgTerm!=0 || cfgIndex!=0 || cfg.length!=0) || registered==0 && !Arrays.equals(registration,new byte[32]))throw new IllegalArgumentException("Noncanonical recovery evidence flags");
        var result=new Evidence(digest,new TopologyChange.Owner(replica,ReplicaDescriptor.Role.values()[role],identity),completed==0?null:new ApplyPosition(term,index),completed==0?null:new ReplicatedRangeStore.Configuration(cfgTerm,cfgIndex,cfg),registered==0?null:registration,installed==1);result.require(request);if(!Arrays.equals(bytes,reply(result)))throw new IllegalArgumentException("Noncanonical recovery evidence");return result;
    }
}
