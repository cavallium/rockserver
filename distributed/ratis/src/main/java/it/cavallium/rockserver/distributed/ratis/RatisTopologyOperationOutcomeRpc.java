package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.nio.ByteBuffer;
import java.util.*;

/** Fresh sealed-parent receipt lookup. Unavailable native owners are errors, never authoritative absence. */
public final class RatisTopologyOperationOutcomeRpc {
    public static final int KIND=95,REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+152;
    public static final int MAX_CONFIGURATION_BYTES=64*1024;
    public static final int REPLY_BASE_BYTES=ScopedTransactionRpc.HEADER_BYTES+77;
    public static final int MAX_REPLY_BYTES=REPLY_BASE_BYTES+NativeTopologyFreeze.MAX_BYTES+MAX_CONFIGURATION_BYTES+CommandResult.MAX_BYTES;
    public enum Outcome { FOUND, EXPIRED, ABSENT }
    private RatisTopologyOperationOutcomeRpc() {}
    public record Request(UUID clusterId,UUID databaseId,UUID domainId,DistributedCdcCheckpoint.Lineage lineage,OperationId operation,byte[] receiptDigest) {
        public Request {
            Objects.requireNonNull(clusterId);Objects.requireNonNull(databaseId);Objects.requireNonNull(domainId);Objects.requireNonNull(lineage);Objects.requireNonNull(operation);
            if(receiptDigest==null || receiptDigest.length!=32)throw new IllegalArgumentException("Invalid original receipt digest");receiptDigest=receiptDigest.clone();
        }
        @Override public byte[] receiptDigest(){return receiptDigest.clone();}
        @Override public boolean equals(Object other){return other instanceof Request request && clusterId.equals(request.clusterId) && databaseId.equals(request.databaseId) && domainId.equals(request.domainId) && lineage.equals(request.lineage) && operation.equals(request.operation) && Arrays.equals(receiptDigest,request.receiptDigest);}
        @Override public int hashCode(){return Objects.hash(clusterId,databaseId,domainId,lineage,operation,Arrays.hashCode(receiptDigest));}
    }
    public record Reply(Request request,Outcome outcome,ApplyPosition cut,NativeTopologyFreeze freeze,long configurationTerm,long configurationIndex,byte[] configuration,byte[] result) {
        public Reply {
            Objects.requireNonNull(request);Objects.requireNonNull(outcome);Objects.requireNonNull(cut);Objects.requireNonNull(freeze);Objects.requireNonNull(configuration);
            var header=freeze.header();var parent=header.participant(TopologyChange.Stage.FREEZE,freeze.groupId());var lineage=request.lineage();
            if(cut.term()<1 || !header.clusterId().equals(request.clusterId()) || !header.databaseId().equals(request.databaseId()) || !header.domainId().equals(request.domainId())
                    || !parent.rangeId().equals(lineage.rangeId()) || !parent.groupId().equals(lineage.groupId()) || parent.rangeEpoch()!=lineage.rangeEpoch())throw new IllegalArgumentException("Outcome reply differs from original sealed parent namespace/lineage");
            freeze.requireCut(cut);
            if(configuration.length<1 || configuration.length>MAX_CONFIGURATION_BYTES || configurationTerm<0 || configurationIndex< -1 || configurationTerm>cut.term() || configurationIndex>cut.index() || configurationIndex==cut.index() && configurationTerm!=cut.term())throw new IllegalArgumentException("Outcome native configuration exceeds completed cut/budget");
            if(outcome==Outcome.FOUND) {
                if(result==null || result.length>CommandResult.MAX_BYTES || CommandResult.decode(result).status()==CommandResult.Status.SESSION_EXPIRED)throw new IllegalArgumentException("FOUND outcome lacks original retained result");
            } else if(result!=null)throw new IllegalArgumentException("Non-FOUND outcome carries result bytes");
            configuration=configuration.clone();result=result==null ? null : result.clone();
        }
        @Override public byte[] configuration(){return configuration.clone();}
        @Override public byte[] result(){return result==null ? null : result.clone();}
    }
    public static byte[] request(Request request) {
        var out=ByteBuffer.allocate(152);ScopedTransactionRpc.uuid(out,request.clusterId());ScopedTransactionRpc.uuid(out,request.databaseId());ScopedTransactionRpc.uuid(out,request.domainId());
        ScopedTransactionRpc.uuid(out,request.lineage().rangeId());ScopedTransactionRpc.uuid(out,request.lineage().groupId());out.putLong(request.lineage().rangeEpoch());
        ScopedTransactionRpc.uuid(out,request.operation().sessionId());out.putLong(request.operation().epoch()).putLong(request.operation().sequence()).put(request.receiptDigest());return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static Request decodeRequest(byte[] encoded) {
        var packet=ScopedTransactionRpc.parse(encoded,REQUEST_BYTES);if(packet.kind()!=KIND || packet.body().length!=152)throw new IllegalArgumentException("Invalid topology outcome request");var in=ByteBuffer.wrap(packet.body());
        UUID cluster=ScopedTransactionRpc.uuid(in),database=ScopedTransactionRpc.uuid(in),domain=ScopedTransactionRpc.uuid(in);var lineage=new DistributedCdcCheckpoint.Lineage(ScopedTransactionRpc.uuid(in),ScopedTransactionRpc.uuid(in),in.getLong());
        var operation=new OperationId(ScopedTransactionRpc.uuid(in),in.getLong(),in.getLong());byte[] digest=new byte[32];in.get(digest);return new Request(cluster,database,domain,lineage,operation,digest);
    }
    public static byte[] reply(Request request,ReplicatedRangeStore.TopologyOperationOutcome nativeOutcome) {
        byte[] result=nativeOutcome.result();Outcome outcome=result==null ? Outcome.ABSENT : CommandResult.decode(result).status()==CommandResult.Status.SESSION_EXPIRED ? Outcome.EXPIRED : Outcome.FOUND;
        var configuration=nativeOutcome.configuration();return reply(new Reply(request,outcome,nativeOutcome.cut(),nativeOutcome.freeze(),configuration.term(),configuration.index(),configuration.data(),outcome==Outcome.FOUND ? result : null));
    }
    public static byte[] reply(Reply reply) {
        byte[] freeze=reply.freeze().encode(),configuration=reply.configuration(),result=reply.result();
        var out=ByteBuffer.allocate(77+freeze.length+configuration.length+(result==null ? 0 : result.length));out.put(OperatorUpload.sha256(request(reply.request()))).put((byte)reply.outcome().ordinal()).putLong(reply.cut().term()).putLong(reply.cut().index()).putLong(reply.configurationTerm()).putLong(reply.configurationIndex());
        ScopedTransactionRpc.putBlob(out,freeze);ScopedTransactionRpc.putBlob(out,configuration);ScopedTransactionRpc.putBlob(out,result);return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static Reply decodeReply(byte[] encoded,Request expected) {
        var packet=ScopedTransactionRpc.parse(encoded,MAX_REPLY_BYTES);if(packet.kind()!=KIND || encoded.length<REPLY_BASE_BYTES)throw new IllegalArgumentException("Invalid topology outcome reply");var in=ByteBuffer.wrap(packet.body());byte[] digest=new byte[32];in.get(digest);
        if(!Arrays.equals(digest,OperatorUpload.sha256(request(expected))))throw new IllegalArgumentException("Topology outcome reply crossed original request");int tag=in.get()&255;if(tag>=Outcome.values().length)throw new IllegalArgumentException("Unknown topology outcome");
        var cut=new ApplyPosition(in.getLong(),in.getLong());long term=in.getLong(),index=in.getLong();var freeze=NativeTopologyFreeze.decode(ScopedTransactionRpc.blob(in,NativeTopologyFreeze.MAX_BYTES,false));byte[] configuration=ScopedTransactionRpc.blob(in,MAX_CONFIGURATION_BYTES,false),result=ScopedTransactionRpc.blob(in,CommandResult.MAX_BYTES,true);
        if(in.hasRemaining())throw new IllegalArgumentException("Trailing topology outcome bytes");var reply=new Reply(expected,Outcome.values()[tag],cut,freeze,term,index,configuration,result);
        if(!Arrays.equals(encoded,reply(reply)))throw new IllegalArgumentException("Noncanonical topology outcome reply");return reply;
    }
}
