package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstImport;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.*;
import org.apache.ratis.client.api.*;
import org.apache.ratis.conf.*;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Operator-qualified bounded controller transport. Catalogue connection and TLS parameters are borrowed. */
public final class RatisRangeImportDataClient implements RatisRangeImportController.DataPlane,AutoCloseable {
    public static final int MAX_CALLS=2,MAX_CLIENTS=16;
    @FunctionalInterface interface Connections { RaftClient open(RangeDescriptor range,ClientId client) throws IOException; }
    @FunctionalInterface interface Streams { DataStreamClient open(RaftClient control,RaftPeerId primary) throws IOException; }
    private final RatisNodePlan plan; private final RatisClientPool clients; private final Streams streams;
    private final RatisRangeImportOwnerClient owners; private final RatisRangeImportAuthority.Client authority;
    private final List<DataStreamClient> failedCloses=new ArrayList<>(); private int active; private boolean closing; private IOException broken;
    public RatisRangeImportDataClient(RatisNodePlan plan,RaftClient catalogue,Parameters parameters) {
        this(plan,catalogue,(range,id)->connected(plan,range,id),(control,primary)->RatisStreamingTransport.connect(plan,control,primary,parameters),
                new RatisRangeImportOwnerClient(plan.directory().clusterId(),plan.directory().databaseId(),plan.catalogue().groupId()));
    }
    RatisRangeImportDataClient(RatisNodePlan plan,RaftClient catalogue,Connections connections,Streams streams,RatisRangeImportOwnerClient owners) {
        this.plan=Objects.requireNonNull(plan);this.streams=Objects.requireNonNull(streams);this.owners=Objects.requireNonNull(owners);
        authority=new RatisRangeImportAuthority.Client(catalogue,plan.directory().clusterId(),plan.directory().databaseId(),plan.catalogue().groupId());
        clients=new RatisClientPool(MAX_CLIENTS,range->connections.open(range,ClientId.randomId()),null,(range,id)->connections.open(range,ClientId.valueOf(id)));
    }
    private RangeDescriptor range(RangeImportDefinition definition,UUID group) throws IOException {
        var qualified=plan.importDefinition(group);
        if(qualified==null || !Arrays.equals(qualified.digest(),definition.digest()) || !Arrays.equals(plan.directory().payloadDigest(),definition.sourceDirectory().payloadDigest()))throw new IOException("Import differs from operator-qualified placement");
        return plan.groupDescriptor(group);
    }
    private synchronized void enter()throws IOException{if(closing || broken!=null || active>=MAX_CALLS)throw new IOException("Import data transport unavailable",broken);active++;}
    private synchronized void leave(){active--;notifyAll();}
    @Override public RatisReferencedFile transfer(RangeImportDefinition definition,RatisReferencedFile.Request request,Path source)throws IOException {
        var range=range(definition,request.groupId());enter();final DataStreamClient[] opened={null};Throwable failure=null;
        try(var lease=clients.acquireClient(range,request.clientId())) {
            var control=lease.client();
            DataStreamRpcApi lazy=new DataStreamRpcApi(){
                public DataStreamOutput stream(RaftClientRequest header){try{if(opened[0]!=null)throw new IOException("One stream per transfer");opened[0]=streams.open(control,header.getServerId());return opened[0].stream(header);}catch(IOException problem){throw new UncheckedIOException(problem);}}
                public DataStreamOutput stream(ByteBuffer header){throw new UnsupportedOperationException("Exact stream request required");}
                public DataStreamOutput stream(ByteBuffer header,RoutingTable routing){throw new UnsupportedOperationException("Exact stream request required");}
                public DataStreamInput streamReadOnly(ByteBuffer header){throw new UnsupportedOperationException("Import transport writes only");}
                public void close(){ /* The enclosing transfer owns the actual connection. */ }
            };
            return new RatisRangeImportFileClient(control,lazy).transfer(definition,request,source);
        }catch(IOException|RuntimeException|Error problem){failure=problem;throw problem;}
        finally {
            try{if(opened[0]!=null)opened[0].close();}
            catch(IOException problem){synchronized(this){failedCloses.add(opened[0]);broken=problem;}if(failure!=null)failure.addSuppressed(problem);else throw problem;}
            finally{leave();}
        }
    }
    private byte[] execute(RangeImportDefinition definition,UUID group,RatisRangeImportRecord record,int maximum)throws IOException {
        var range=range(definition,group);record.requireDefinition(definition);enter();
        try(var lease=clients.acquire(range)) {
            var client=lease.client();var prepared=client.io().sendReadOnly(message(RatisRangeImportRpc.prepare(record)));
            if(prepared==null || !prepared.isSuccess() || prepared.getMessage()==null || !client.getGroupId().equals(prepared.getRaftGroupId()) || prepared.getMessage().getContent().size()>RatisRangeImportRpc.MAX_BYTES)throw new IOException("Import preparation failed",prepared==null?null:prepared.getException());
            RatisRangeImportRpc.requireReply(prepared.getMessage().getContent().toByteArray(),ScopedTransactionRpc.IMPORT_APPLY_PREPARE,record);
            var reply=client.io().send(message(record.encode()));
            if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !client.getGroupId().equals(reply.getRaftGroupId()) || reply.getMessage().getContent().size()!=maximum)throw new IOException("Import write outcome is unknown or invalid; retain original record",reply==null?null:reply.getException());return reply.getMessage().getContent().toByteArray();
        }catch(IllegalArgumentException malformed){throw new IOException("Invalid import transport reply",malformed);}finally{leave();}
    }
    @Override public NativeSstImportManifest.Receipt install(RangeImportDefinition definition,RatisRangeImportRecord.Install record)throws IOException {
        var receipt=NativeSstImportManifest.Receipt.decode(execute(definition,record.groupId(),record,NativeSstImportManifest.Receipt.BYTES));definition.requireReceipt(receipt);record.requireCut(receipt.position());return receipt;
    }
    @Override public NativeRangeSstImport.Activation activate(RangeImportDefinition definition,RatisRangeImportRecord.Activate record)throws IOException {
        var result=NativeRangeSstImport.Activation.decode(execute(definition,record.receipt().groupId(),record,168));var publication=record.publication();
        if(!Arrays.equals(result.receipt().encode(),record.receipt().encode()) || !result.catalogueGroup().equals(publication.catalogueGroup()) || !result.cataloguePosition().equals(publication.completedCatalogueCut()) || !Arrays.equals(result.publicationDigest(),publication.digest()))throw new IOException("Activation reply differs from original publication");return result;
    }
    @Override public RangeImportAdmission.OwnerReceipt receipt(RangeImportDefinition definition,RangeDescriptor range,ReplicaDescriptor owner)throws IOException{range(definition,range.groupId());enter();try{return owners.receipt(definition,range,owner);}finally{leave();}}
    @Override public RangeImportActivationAdmission.OwnerActivation activation(RangeImportDefinition definition,RatisRangeImportCatalogue.Head published,RangeDescriptor range,ReplicaDescriptor owner)throws IOException{range(definition,range.groupId());enter();try{return owners.activation(definition,published,range,owner);}finally{leave();}}
    @Override public RatisRangeImportApply.PublicationView publication(RangeImportDefinition definition)throws IOException{for(var manifest:definition.manifests())range(definition,manifest.groupId());enter();try{return authority.read(definition);}finally{leave();}}
    private static RaftClient connected(RatisNodePlan plan,RangeDescriptor range,ClientId id) {
        var peers=range.replicas().stream().map(replica->RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).setDataStreamAddress(plan.streamAddresses().get(replica.peerId()))
                .setStartupRole(replica.role()==ReplicaDescriptor.Role.VOTER?RaftPeerRole.FOLLOWER:RaftPeerRole.LISTENER).build()).toList();
        var properties=new RaftProperties();org.apache.ratis.RaftConfigKeys.Rpc.setType(properties,org.apache.ratis.rpc.SupportedRpcType.GRPC);RaftClientConfigKeys.Rpc.setRequestTimeout(properties,org.apache.ratis.util.TimeDuration.valueOf(3,TimeUnit.SECONDS));GrpcConfigKeys.Client.setWorkerGroupSize(properties,1);
        return RaftClient.newBuilder().setClientId(id).setRaftGroup(RaftGroup.valueOf(RaftGroupId.valueOf(range.groupId()),peers)).setProperties(properties).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build();
    }
    @Override public void close()throws IOException {
        synchronized(this){closing=true;if(active!=0)throw new IOException("Import data calls still active");}
        clients.close();owners.close();IOException failure=null;
        for(var stream:List.copyOf(failedCloses))try{stream.close();failedCloses.remove(stream);}catch(IOException problem){if(failure==null)failure=problem;else failure.addSuppressed(problem);}
        if(failure!=null)throw failure;
    }
    private static Message message(byte[] bytes){return Message.valueOf(ByteString.copyFrom(bytes));}
}
