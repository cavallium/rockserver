package it.cavallium.rockserver.core.impl.test;

import com.google.protobuf.ByteString;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.*;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.common.api.RocksDBRemoteMutations;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.server.*;
import java.lang.reflect.Proxy;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RemoteAtomicMutationTransportTest {
    private static final RequestContext CONTEXT=RequestContext.batch();
    private static final UUID REQUEST=new UUID(74,5);
    private static final List<DomainBinding> DOMAINS=List.of(new DomainBinding(new UUID(0,4),11,new byte[32],13,new byte[32]),new DomainBinding(new UUID(Long.MIN_VALUE,2),17,new byte[32],19,new byte[32]));
    private static final RemoteMutationOwner OWNER=new RemoteMutationOwner(new UUID(74,1),new UUID(74,2),new UUID(74,3),3,9,RemoteMutationToken.Kind.ATOMIC,DOMAINS);
    private static Keys key(){return new Keys(Buf.wrap(new byte[]{1}));}
    private static List<AtomicMutation> writes(){return List.of(new AtomicMutation(7,key(),Mutation.Kind.PUT,null,Mutation.ReturnMode.PREVIOUS),new AtomicMutation(8,key(),Mutation.Kind.MERGE,Buf.wrap(new byte[0]),Mutation.ReturnMode.CURRENT));}
    private static List<AtomicPredicate> predicates(){return List.of(new AtomicPredicate(7,key(),null),new AtomicPredicate(8,key(),Buf.wrap(new byte[0])));}
    private static RemoteMutationToken token(byte[] digest){return new RemoteMutationToken(OWNER.clusterId(),OWNER.databaseId(),OWNER.ownerId(),REQUEST,3,9,RemoteMutationToken.Kind.ATOMIC,DOMAINS,digest);}
    @Test void vectorAuthorityAndTokenRejectMalformedAndNoncanonicalLayouts()throws Exception{
        assertEquals(OWNER,RemoteMutationWire.owner(RemoteMutationWire.owner(OWNER)));var thrift=ThriftRemoteMutationWire.owner(OWNER);var decoded=new it.cavallium.rockserver.core.common.api.ThriftMutationOwner();new TDeserializer().deserialize(decoded,new TSerializer().serialize(thrift));assertEquals(OWNER,ThriftRemoteMutationWire.owner(decoded));
        var wire=RemoteMutationWire.owner(OWNER);assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.owner(wire.toBuilder().clearDomains().addDomains(wire.getDomains(1)).addDomains(wire.getDomains(0)).build()));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.owner(wire.toBuilder().setDomains(1,wire.getDomains(0)).build()));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.owner(wire.toBuilder().setKind("POINT").build()));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.owner(wire.toBuilder().setDomains(0,wire.getDomains(0).toBuilder().setSchemaDigest(ByteString.copyFrom(new byte[31]))).build()));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.owner(wire.toBuilder().setOwnerId(ByteString.copyFrom(new byte[15])).build()));
        var reversed=thrift.deepCopy();Collections.reverse(reversed.domains);assertThrows(IllegalArgumentException.class,()->ThriftRemoteMutationWire.owner(reversed));var tooMany=wire.toBuilder().clearDomains();for(int i=0;i<65;i++)tooMany.addDomains(wire.getDomains(0));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.owner(tooMany.build()));
        var token=token(new byte[32]);assertEquals(309,token.encode().length);assertEquals(token,RemoteMutationWire.token(RemoteMutationWire.token(token)));assertEquals(token,ThriftRemoteMutationWire.token(ThriftRemoteMutationWire.token(token)));var wrongCount=token.encode();ByteBuffer.wrap(wrongCount).putInt(81,65);assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.token(ByteString.copyFrom(wrongCount)));var unsorted=token.encode();System.arraycopy(token.encode(),85+96,unsorted,85,96);System.arraycopy(token.encode(),85,unsorted,85+96,96);assertThrows(IllegalArgumentException.class,()->RemoteMutationToken.decode(unsorted));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.token(ByteString.copyFrom(new byte[RemoteMutationToken.MAX_BYTES+1])));assertThrows(IllegalArgumentException.class,()->RemoteMutationToken.decode(Arrays.copyOf(token.encode(),token.encodedSize()+1)));
    }
    @Test void atomicNullAndEmptyAndCanonicalBudgetsArePreserved(){
        var input=RemoteMutationWire.atomic(RemoteMutationWire.mutations(writes()),RemoteMutationWire.predicates(predicates()),60000);assertNull(input.mutations().getFirst().value());assertEquals(0,input.mutations().getLast().value().size());assertNull(input.predicates().getFirst().expected());assertEquals(0,input.predicates().getLast().expected().size());var thrift=ThriftRemoteMutationWire.atomic(ThriftRemoteMutationWire.mutations(writes()),ThriftRemoteMutationWire.predicates(predicates()),60000);assertArrayEquals(RemoteMutationInputs.atomic(input.mutations(),input.predicates(),60000),RemoteMutationInputs.atomic(thrift.mutations(),thrift.predicates(),60000));
        assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.atomic(List.of(),List.of(),1));assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.atomic(List.of(),RemoteMutationWire.predicates(predicates()),60001));assertEquals(2,RemoteMutationWire.atomic(List.of(),RemoteMutationWire.predicates(predicates()),1).predicates().size());var excessive=Collections.nCopies(257,RemoteMutationWire.predicates(predicates()).getFirst());assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.atomic(List.of(),excessive,1));var big=RemoteMutationWire.mutations(writes()).getFirst().toBuilder().setValue(ByteString.copyFrom(new byte[2*1024*1024])).build();assertThrows(IllegalArgumentException.class,()->RemoteMutationWire.atomic(List.of(big),List.of(),1));
        var onlyAbsent=List.of(new AtomicPredicate(7,key(),null));var onlyEmpty=List.of(new AtomicPredicate(7,key(),Buf.wrap(new byte[0])));assertFalse(Arrays.equals(RemoteMutationInputs.atomic(List.of(),onlyAbsent,1),RemoteMutationInputs.atomic(List.of(),onlyEmpty,1)));
    }
    static final class Backend {
        RemoteMutationToken retained;int prepares,executes,acks;final byte[] row=new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{4},new byte[]{5})).encode();
        RemoteMutationStatus status(){return new RemoteMutationStatus(retained,RemoteMutationStatus.State.TERMINAL,2,0,1,CommandResult.Status.SUCCESS,RemoteMutationToken.digest(row));}
        RemoteMutationAsyncAPI api(){return (RemoteMutationAsyncAPI)Proxy.newProxyInstance(RemoteMutationAsyncAPI.class.getClassLoader(),new Class<?>[]{RemoteMutationAsyncAPI.class},(p,m,a)->{
            Object value=switch(m.getName()){
                case "atomicOwners"->{assertEquals(List.of(7L,8L),a[0]);yield List.of(OWNER);}
                case "prepareAtomic"->{assertEquals(OWNER,a[0]);assertEquals(REQUEST,a[1]);assertEquals(9L,a[2]);@SuppressWarnings("unchecked")var writes=(List<AtomicMutation>)a[3];@SuppressWarnings("unchecked")var predicates=(List<AtomicPredicate>)a[4];assertNull(writes.getFirst().value());assertEquals(0,writes.getLast().value().size());assertNull(predicates.getFirst().expected());assertEquals(0,predicates.getLast().expected().size());var token=token(RemoteMutationInputs.atomic(writes,predicates,(Long)a[5]));if(retained!=null && !retained.equals(token))throw new IllegalArgumentException("Changed original input");retained=token;prepares++;yield token;}
                case "execute"->{assertEquals(retained,a[0]);executes++;yield status();}
                case "status","resume"->{assertEquals(retained,a[0]);yield status();}
                case "results"->{assertEquals(retained,a[0]);yield new RemoteMutationResultPage(status(),0,1,List.of(row));}
                case "acknowledge"->{assertEquals(retained,a[0]);assertArrayEquals(RemoteMutationToken.digest(row),(byte[])a[1]);acks++;yield new RemoteMutationStatus(retained,RemoteMutationStatus.State.ACKNOWLEDGED,2,0,1,CommandResult.Status.SUCCESS,RemoteMutationToken.digest(row));}
                default->throw new AssertionError(m.getName());
            };return CompletableFuture.completedFuture(value);
        });}
    }
    private static void workflow(RemoteMutationAsyncAPI client,Backend backend)throws Exception{
        assertEquals(OWNER,client.atomicOwners(List.of(7L,8L),CONTEXT).get(5,TimeUnit.SECONDS).getFirst());var token=client.executeAtomic(OWNER,REQUEST,9,writes(),predicates(),1000,CONTEXT).get(5,TimeUnit.SECONDS).token();assertEquals(DOMAINS,token.domains());assertEquals(token,client.status(token,CONTEXT).get(5,TimeUnit.SECONDS).token());assertEquals(token,client.resume(token,CONTEXT).get(5,TimeUnit.SECONDS).token());assertEquals(2,CommandResult.decode(client.results(token,0,1,128,CONTEXT).get(5,TimeUnit.SECONDS).rows().getFirst()).valueCount());assertEquals(0,backend.acks);assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,client.acknowledge(token,RemoteMutationToken.digest(backend.row),CONTEXT).get(5,TimeUnit.SECONDS).state());assertEquals(1,backend.prepares);assertEquals(1,backend.executes);assertEquals(1,backend.acks);
    }
    @Test void actualGrpcSocketAtomicLifecycleCoexistsWithOrdinaryRpc()throws Exception{
        var backend=new Backend();var ordinary=new RocksDBServiceGrpc.RocksDBServiceImplBase(){@Override public void getColumnId(GetColumnIdRequest request,StreamObserver<GetColumnIdResponse> observer){observer.onNext(GetColumnIdResponse.newBuilder().setColumnId(7).build());observer.onCompleted();}};var server=NettyServerBuilder.forAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(),0)).addService(new RemoteMutationGrpcService(backend.api()).bindBoundedService()).addService(ordinary).build().start();var channel=NettyChannelBuilder.forAddress(InetAddress.getLoopbackAddress().getHostAddress(),server.getPort()).usePlaintext().build();
        try{workflow(new GrpcRemoteMutationClient(channel),backend);assertEquals(7,RocksDBServiceGrpc.newBlockingStub(channel).getColumnId(GetColumnIdRequest.newBuilder().setName("ordinary").build()).getColumnId());}finally{channel.shutdownNow().awaitTermination(5,TimeUnit.SECONDS);server.shutdownNow().awaitTermination(5,TimeUnit.SECONDS);}
    }
    @Test void actualThriftSocketAtomicLifecycleCoexistsWithOrdinaryRpc()throws Exception{
        var backend=new Backend();var mux=new TMultiplexedProcessor();mux.registerProcessor(RemoteMutationThriftService.SERVICE_NAME,new RemoteMutationThriftService(backend.api(),ThriftRemoteMutationWire.MAX_FRAME_BYTES).boundedProcessor());var ordinary=(RocksDB.Iface)Proxy.newProxyInstance(RocksDB.Iface.class.getClassLoader(),new Class<?>[]{RocksDB.Iface.class},(p,m,a)->{assertEquals("getColumnId",m.getName());return 7L;});mux.registerDefault(new RocksDB.Processor<>(ordinary));
        try(var listener=new ServerSocket(0,50,InetAddress.getLoopbackAddress());var io=Executors.newSingleThreadExecutor();var clientIo=Executors.newSingleThreadExecutor()){
            var serving=io.submit(()->{while(!listener.isClosed()){try(var socket=listener.accept();var transport=new TFramedTransport(new TSocket(socket),ThriftRemoteMutationWire.MAX_FRAME_BYTES)){var protocol=new TBinaryProtocol(transport);mux.process(protocol,protocol);}catch(Exception error){if(!listener.isClosed())throw new CompletionException(error);}}});
            try(var client=new ThriftRemoteMutationClient(InetAddress.getLoopbackAddress().getHostAddress(),listener.getLocalPort(),clientIo)){workflow(client,backend);try(var socket=new Socket(InetAddress.getLoopbackAddress(),listener.getLocalPort());var transport=new TFramedTransport(new TSocket(socket),ThriftRemoteMutationWire.MAX_FRAME_BYTES)){assertEquals(7,new RocksDB.Client(new TBinaryProtocol(transport)).getColumnId("ordinary",new it.cavallium.rockserver.core.common.api.RequestContext(it.cavallium.rockserver.core.common.api.WorkloadProfile.BATCH,Long.MAX_VALUE)));}}finally{listener.close();serving.get(5,TimeUnit.SECONDS);}
        }
    }
}
