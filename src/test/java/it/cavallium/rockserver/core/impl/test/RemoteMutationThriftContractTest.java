package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.ThriftRemoteMutationClient;
import it.cavallium.rockserver.core.client.ThriftRemoteMutationWire;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.api.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.server.RemoteMutationThriftService;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RemoteMutationThriftContractTest {
    private static final RequestContext CONTEXT=RequestContext.batch();
    private static final UUID REQUEST=new UUID(711,9);
    private static final long COLUMN=7,SEQUENCE=11;
    private static RemoteMutationOwner owner(RemoteMutationToken.Kind kind){return new RemoteMutationOwner(new UUID(711,1),new UUID(711,2),new UUID(711,4+kind.ordinal()),2,SEQUENCE,kind,List.of(new DomainBinding(new UUID(711,3),4,new byte[32],6,new byte[32])));}
    private static RemoteMutationToken token(RemoteMutationOwner owner,UUID id,long sequence,byte[] input){return new RemoteMutationToken(owner.clusterId(),owner.databaseId(),owner.ownerId(),id,owner.ownerEpoch(),sequence,owner.kind(),owner.domains(),input);}
    private static it.cavallium.rockserver.core.common.api.RequestContext context(){return new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.BATCH,RequestContext.NO_TIMEOUT);}
    private static RemoteMutationAsyncAPI backend(InvocationHandler handler){return (RemoteMutationAsyncAPI)Proxy.newProxyInstance(RemoteMutationAsyncAPI.class.getClassLoader(),new Class<?>[]{RemoteMutationAsyncAPI.class},handler);}
    private static RocksDBRemoteMutations.Iface binary(RemoteMutationThriftService service,AtomicBoolean loseExecute){
        var mux=new TMultiplexedProcessor();mux.registerProcessor(RemoteMutationThriftService.SERVICE_NAME,service.boundedProcessor());
        return (RocksDBRemoteMutations.Iface)Proxy.newProxyInstance(RocksDBRemoteMutations.Iface.class.getClassLoader(),new Class<?>[]{RocksDBRemoteMutations.Iface.class},(p,m,a)->{
            var request=new TMemoryBuffer(4096);var response=new TMemoryBuffer(4096);
            var client=new RocksDBRemoteMutations.Client(ThriftRemoteMutationWire.boundedProtocol(new TBinaryProtocol(response),false),
                    new TMultiplexedProtocol(new TBinaryProtocol(request),RemoteMutationThriftService.SERVICE_NAME));
            try{
                RocksDBRemoteMutations.Client.class.getMethod("send_"+m.getName(),m.getParameterTypes()).invoke(client,a);
                mux.process(new TBinaryProtocol(request),new TBinaryProtocol(response));
                if(m.getName().equals("execute") && loseExecute.compareAndSet(true,false))throw new TTransportException("Controlled lost reply after execution");
                return RocksDBRemoteMutations.Client.class.getMethod("recv_"+m.getName()).invoke(client);
            }catch(InvocationTargetException failure){throw failure.getCause();}
        });
    }
    @Test void lostReplyRecoversOriginalTokenPagesResultsAndAcknowledgesOnlyExplicitly()throws Exception{
        var calls=new ArrayList<String>();var current=new AtomicReference<RemoteMutationStatus>();var effects=new AtomicInteger();var acks=new AtomicInteger();
        var row=new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{4,5})).encode();var digest=RemoteMutationToken.digest(row);
        var api=backend((p,m,a)->{
            calls.add(m.getName());assertEquals(CONTEXT,a[a.length-1]);
            Object value=switch(m.getName()){
                case "owners"->List.of(owner(RemoteMutationToken.Kind.POINT));
                case "preparePoint"->{
                    assertEquals(REQUEST,a[1]);assertEquals(SEQUENCE,a[2]);assertEquals(COLUMN,a[3]);
                    @SuppressWarnings("unchecked") var keys=(List<Keys>)a[4];@SuppressWarnings("unchecked") var values=(List<Buf>)a[5];
                    var token=token((RemoteMutationOwner)a[0],(UUID)a[1],(Long)a[2],RemoteMutationInputs.point((Long)a[3],((Mutation.Kind)a[6]).ordinal(),(RequestType.RequestTypeId)a[7],(Boolean)a[8],keys,values));
                    current.set(new RemoteMutationStatus(token,RemoteMutationStatus.State.PREPARED,0,0,0,null,null));yield token;
                }
                case "execute","resume"->{assertEquals(current.get().token(),a[0]);if(current.get().state()==RemoteMutationStatus.State.PREPARED){effects.incrementAndGet();current.set(new RemoteMutationStatus(current.get().token(),RemoteMutationStatus.State.TERMINAL,1,0,1,CommandResult.Status.SUCCESS,digest));}yield current.get();}
                case "status"->{assertEquals(current.get().token(),a[0]);yield current.get();}
                case "results"->{assertEquals(current.get().token(),a[0]);assertEquals(0,a[1]);assertEquals(1,a[2]);assertEquals(row.length,a[3]);yield new RemoteMutationResultPage(current.get(),0,1,List.of(row));}
                case "acknowledge"->{assertEquals(current.get().token(),a[0]);assertArrayEquals(digest,(byte[])a[1]);acks.incrementAndGet();current.set(new RemoteMutationStatus(current.get().token(),RemoteMutationStatus.State.ACKNOWLEDGED,1,0,1,CommandResult.Status.SUCCESS,digest));yield current.get();}
                default->throw new AssertionError(m.getName());
            };return CompletableFuture.completedFuture(value);
        });
        var service=new RemoteMutationThriftService(api,ThriftRemoteMutationWire.MAX_FRAME_BYTES);
        try(var client=new ThriftRemoteMutationClient(binary(service,new AtomicBoolean(true)),Runnable::run)){
            var selected=client.owners(COLUMN,CONTEXT).join().getFirst();assertEquals(owner(RemoteMutationToken.Kind.POINT),selected);
            var token=client.preparePoint(selected,REQUEST,SEQUENCE,COLUMN,List.of(new Keys(Buf.wrap(new byte[]{1}))),List.of(Buf.wrap(new byte[]{2})),Mutation.Kind.PUT,RequestType.RequestTypeId.PREVIOUS,false,CONTEXT).join();
            assertEquals(REQUEST,token.requestId());assertEquals(SEQUENCE,token.requestSequence());
            assertThrows(CompletionException.class,()->client.execute(token,CONTEXT).join());assertEquals(1,effects.get());assertEquals(0,acks.get());
            assertEquals(RemoteMutationStatus.State.TERMINAL,client.status(token,CONTEXT).join().state());
            assertEquals(CommandResult.Status.SUCCESS,client.resume(token,CONTEXT).join().outcome());assertEquals(1,effects.get());
            var page=client.results(token,0,1,row.length,CONTEXT).join();assertArrayEquals(row,page.rows().getFirst());assertFalse(page.hasMore());assertArrayEquals(digest,page.status().resultDigest());assertEquals(0,acks.get());
            assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,client.acknowledge(token,digest,CONTEXT).join().state());assertEquals(1,acks.get());
            assertEquals(List.of("owners","preparePoint","execute","status","resume","results","acknowledge"),calls);
        }
    }
    @Test void rangeAndBulkInputsPreserveExplicitIdentityOrdinalAndLogicalDigest()throws Exception{
        var calls=new ArrayList<String>();var copied=new AtomicLong();
        var batch=new KVBatch.KVBatchRef(List.of(new Keys(Buf.wrap(new byte[]{8}))),List.of(Buf.wrap(new byte[]{9})));
        var expected=RemoteMutationInputs.batch(COLUMN,true,batch);
        var api=backend((p,m,a)->{
            assertEquals(CONTEXT,a[a.length-1]);calls.add(m.getName());Object result=switch(m.getName()){
                case "prepareRange"->token((RemoteMutationOwner)a[0],(UUID)a[1],(Long)a[2],RemoteMutationInputs.range((Long)a[3],(Keys)a[4],(Keys)a[5]));
                case "prepareBulk"->token((RemoteMutationOwner)a[0],(UUID)a[1],(Long)a[2],RemoteMutationInputs.bulkHeader((Long)a[3],(Boolean)a[4]));
                case "appendBatch"->{assertEquals(0L,a[1]);assertArrayEquals(expected,(byte[])a[3]);assertArrayEquals(expected,RemoteMutationInputs.batch(COLUMN,true,(KVBatch)a[2]));copied.incrementAndGet();yield new RemoteMutationStatus((RemoteMutationToken)a[0],RemoteMutationStatus.State.PREPARED,0,copied.get(),0,null,null);}
                case "finishInput"->new RemoteMutationStatus((RemoteMutationToken)a[0],RemoteMutationStatus.State.PREPARED,0,copied.get(),0,null,null);
                default->throw new AssertionError(m.getName());};return CompletableFuture.completedFuture(result);
        });
        try(var client=new ThriftRemoteMutationClient(binary(new RemoteMutationThriftService(api,ThriftRemoteMutationWire.MAX_FRAME_BYTES),new AtomicBoolean()),Runnable::run)){
            var range=client.prepareRange(owner(RemoteMutationToken.Kind.RANGE),REQUEST,SEQUENCE,COLUMN,null,new Keys(Buf.wrap(new byte[]{7})),CONTEXT).join();assertEquals(RemoteMutationToken.Kind.RANGE,range.kind());
            var bulk=client.prepareBulk(owner(RemoteMutationToken.Kind.BULK),REQUEST,SEQUENCE,COLUMN,true,CONTEXT).join();assertEquals(REQUEST,bulk.requestId());
            assertEquals(1,client.appendBatch(bulk,0,batch,expected,CONTEXT).join().copiedBatches());assertEquals(1,client.finishInput(bulk,CONTEXT).join().copiedBatches());
            assertEquals(List.of("prepareRange","prepareBulk","appendBatch","finishInput"),calls);
        }
    }
    @Test void malformedIdentityContextAndBudgetsAreRejectedBeforeBackendAdmission()throws Exception{
        var calls=new AtomicInteger();var api=backend((p,m,a)->{calls.incrementAndGet();throw new AssertionError("Backend must not be called");});
        var service=new RemoteMutationThriftService(api,ThriftRemoteMutationWire.MAX_FRAME_BYTES);
        var token=token(owner(RemoteMutationToken.Kind.POINT),REQUEST,SEQUENCE,new byte[32]);
        assertThrows(RocksDBThriftException.class,()->service.status(ByteBuffer.wrap(new byte[192]),context()));
        assertThrows(RocksDBThriftException.class,()->service.status(ThriftRemoteMutationWire.token(token),null));
        assertThrows(RocksDBThriftException.class,()->service.status(ThriftRemoteMutationWire.token(token),new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.CONTROL,1)));
        assertThrows(RocksDBThriftException.class,()->service.acknowledge(ThriftRemoteMutationWire.token(token),ByteBuffer.wrap(new byte[31]),context()));
        assertThrows(RocksDBThriftException.class,()->service.results(ThriftRemoteMutationWire.token(token),0,257,1,context()));
        assertThrows(RocksDBThriftException.class,()->service.results(ThriftRemoteMutationWire.token(token),0,1,CommandResult.MAX_BYTES+1,context()));
        assertThrows(RocksDBThriftException.class,()->service.prepareBulk(ThriftRemoteMutationWire.owner(owner(RemoteMutationToken.Kind.POINT)),ThriftRemoteMutationWire.uuid(REQUEST),SEQUENCE,COLUMN,false,context()));
        assertThrows(RocksDBThriftException.class,()->service.appendBatch(ThriftRemoteMutationWire.token(token),0,List.of(),ByteBuffer.wrap(new byte[32]),context()));
        assertThrows(RocksDBThriftException.class,()->service.finishInput(ThriftRemoteMutationWire.token(token),context()));
        var malformedOwner=ThriftRemoteMutationWire.owner(owner(RemoteMutationToken.Kind.BULK));malformedOwner.unsetOwnerEpoch();
        assertThrows(RocksDBThriftException.class,()->service.prepareBulk(malformedOwner,ThriftRemoteMutationWire.uuid(REQUEST),SEQUENCE,COLUMN,false,context()));
        var invalidDigest=ThriftRemoteMutationWire.owner(owner(RemoteMutationToken.Kind.BULK));invalidDigest.domains.getFirst().setSchemaDigest(new byte[33]);
        assertThrows(RocksDBThriftException.class,()->service.prepareBulk(invalidDigest,ThriftRemoteMutationWire.uuid(REQUEST),SEQUENCE,COLUMN,false,context()));
        var bounds=List.of(ByteBuffer.wrap(new byte[TransactionScope.MAX_BOUND_BYTES]));
        assertThrows(RocksDBThriftException.class,()->service.prepareRange(ThriftRemoteMutationWire.owner(owner(RemoteMutationToken.Kind.RANGE)),ThriftRemoteMutationWire.uuid(REQUEST),SEQUENCE,COLUMN,bounds,List.of(ByteBuffer.wrap(new byte[1])),context()));
        assertEquals(0,calls.get());
        var bytes=new TMemoryBuffer(32);var protocol=new TBinaryProtocol(bytes);protocol.writeFieldBegin(new TField("token",TType.STRING,(short)20));protocol.writeI32(RemoteMutationToken.MAX_BYTES+1);
        assertThrows(TProtocolException.class,()->new RocksDBRemoteMutations.status_args().read(ThriftRemoteMutationWire.boundedProtocol(new TBinaryProtocol(bytes),true)));
        var listBytes=new TMemoryBuffer(32);protocol=new TBinaryProtocol(listBytes);protocol.writeFieldBegin(new TField("entries",TType.LIST,(short)42));protocol.writeListBegin(new TList(TType.STRUCT,4097));
        var input=ThriftRemoteMutationWire.boundedProtocol(new TBinaryProtocol(listBytes),true);input.readFieldBegin();assertThrows(TProtocolException.class,input::readListBegin);
    }
    @Test void terminalOutcomeExpiryAndPageBoundsSurviveBinaryMapping()throws Exception{
        var token=token(owner(RemoteMutationToken.Kind.POINT),REQUEST,SEQUENCE,new byte[32]);
        for(var outcome:CommandResult.Status.values()){
            var status=new RemoteMutationStatus(token,RemoteMutationStatus.State.TERMINAL,0,0,1,outcome,new byte[32]);
            var wire=ThriftRemoteMutationWire.status(status);var decoded=new ThriftMutationObservation();new TDeserializer().deserialize(decoded,new TSerializer().serialize(wire));
            assertEquals(outcome,ThriftRemoteMutationWire.status(decoded,token).outcome());
        }
        var expired=new RemoteMutationStatus(token,RemoteMutationStatus.State.EXPIRED,-1,-1,0,null,null);assertFalse(ThriftRemoteMutationWire.status(ThriftRemoteMutationWire.status(expired),token).progressKnown());
        var crossed=token(owner(RemoteMutationToken.Kind.POINT),UUID.randomUUID(),SEQUENCE,new byte[32]);assertThrows(IllegalArgumentException.class,()->ThriftRemoteMutationWire.status(ThriftRemoteMutationWire.status(expired),crossed));
        assertThrows(IllegalArgumentException.class,()->ThriftRemoteMutationWire.prepared(ThriftRemoteMutationWire.token(crossed),owner(RemoteMutationToken.Kind.POINT),REQUEST,SEQUENCE,new byte[32]));
        var unset=ThriftRemoteMutationWire.status(expired);unset.unsetCopiedBatches();assertThrows(IllegalArgumentException.class,()->ThriftRemoteMutationWire.status(unset,token));

        var row=new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{1})).encode();
        var status=new RemoteMutationStatus(token,RemoteMutationStatus.State.TERMINAL,1,0,2,CommandResult.Status.SUCCESS,new byte[32]);var page=new RemoteMutationResultPage(status,0,1,List.of(row));
        var wire=ThriftRemoteMutationWire.page(page,token,0,1,row.length);assertTrue(ThriftRemoteMutationWire.page(wire,token,0,1,row.length).hasMore());
        assertThrows(IllegalArgumentException.class,()->ThriftRemoteMutationWire.page(wire,token,0,1,row.length-1));
        assertThrows(IllegalArgumentException.class,()->ThriftRemoteMutationWire.page(wire.deepCopy().setNextOffset(2),token,0,1,row.length));
        assertThrows(IllegalArgumentException.class,()->ThriftRemoteMutationWire.page(wire.deepCopy().setHasMore(false),token,0,1,row.length));
        var unavailable=new RemoteMutationThriftService(null,ThriftRemoteMutationWire.MAX_FRAME_BYTES);assertEquals(RocksDBErrorType.NOT_IMPLEMENTED,assertThrows(RocksDBThriftException.class,()->unavailable.status(ThriftRemoteMutationWire.token(token),context())).errorType);
    }
    @Test void unavailableDistributedMutationGetterDoesNotHideOtherBackendFailures()throws Exception{
        var capabilities=new AtomicReference<>(new it.cavallium.rockserver.core.common.RockserverCapabilities(
                it.cavallium.rockserver.core.common.RockserverCapabilities.BackendMode.DISTRIBUTED,
                it.cavallium.rockserver.core.common.RockserverCapabilities.RawScanMode.UNSUPPORTED,1,1024,
                it.cavallium.rockserver.core.common.RockserverCapabilities.CdcMode.UNSUPPORTED));
        var failure=new AtomicReference<it.cavallium.rockserver.core.common.RocksDBException>();var getters=new AtomicInteger();
        var connection=(it.cavallium.rockserver.core.client.RocksDBConnection)Proxy.newProxyInstance(
                it.cavallium.rockserver.core.client.RocksDBConnection.class.getClassLoader(),new Class<?>[]{it.cavallium.rockserver.core.client.RocksDBConnection.class},(proxy,method,args)->{
                    if(method.getName().equals("getCapabilities"))return capabilities.get();
                    if(method.getName().equals("getRemoteMutationApi")){getters.incrementAndGet();if(failure.get()!=null)throw failure.get();return InvocationHandler.invokeDefault(proxy,method,args==null?new Object[0]:args);}
                    throw new AssertionError(method.getName());
                });
        var resolve=it.cavallium.rockserver.core.server.ThriftServer.class.getDeclaredMethod("remoteMutationBackend",it.cavallium.rockserver.core.client.RocksDBConnection.class);resolve.setAccessible(true);
        assertNull(resolve.invoke(null,connection));assertEquals(1,getters.get());
        failure.set(it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.INTERNAL_ERROR,"Controlled backend fault"));
        assertSame(failure.get(),assertThrows(InvocationTargetException.class,()->resolve.invoke(null,connection)).getCause());assertEquals(2,getters.get());
        capabilities.set(it.cavallium.rockserver.core.common.RockserverCapabilities.STANDALONE);assertNull(resolve.invoke(null,connection));assertEquals(2,getters.get());
    }
    @Test void queuedDeadlineExpiresWithoutBackendAdmissionOrAcknowledgement()throws Exception{
        var queued=new ArrayList<Runnable>();var calls=new AtomicInteger();
        var api=backend((p,m,a)->{calls.incrementAndGet();throw new AssertionError("Expired queued call must not run");});
        var token=token(owner(RemoteMutationToken.Kind.POINT),REQUEST,SEQUENCE,new byte[32]);
        try(var client=new ThriftRemoteMutationClient(new RemoteMutationThriftService(api,ThriftRemoteMutationWire.MAX_FRAME_BYTES),queued::add)){
            var observation=client.execute(token,RequestContext.batch(java.time.Duration.ofMillis(20)));
            var failure=assertThrows(CompletionException.class,observation::join);
            assertEquals(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,((it.cavallium.rockserver.core.common.RocksDBException)failure.getCause()).getErrorUniqueId());
            assertEquals(1,queued.size());queued.getFirst().run();assertEquals(0,calls.get());
        }
    }
    @Test void cancellationAndCloseDoNotCancelBackendWorkOrAdmitQueuedCalls()throws Exception{
        var source=new CompletableFuture<RemoteMutationStatus>();var started=new CountDownLatch(1);var calls=new AtomicInteger();
        var token=token(owner(RemoteMutationToken.Kind.POINT),REQUEST,SEQUENCE,new byte[32]);
        var api=backend((p,m,a)->{assertEquals("execute",m.getName());calls.incrementAndGet();started.countDown();return source;});
        var service=new RemoteMutationThriftService(api,ThriftRemoteMutationWire.MAX_FRAME_BYTES);
        try(var executor=Executors.newSingleThreadExecutor();var client=new ThriftRemoteMutationClient(service,executor)){
            var observation=client.execute(token,CONTEXT);assertTrue(started.await(5,TimeUnit.SECONDS));assertTrue(observation.cancel(false));assertFalse(source.isCancelled());
            var queued=client.status(token,CONTEXT);client.close();assertTrue(queued.isCompletedExceptionally());assertTrue(client.status(token,CONTEXT).isCompletedExceptionally());assertFalse(source.isCancelled());
            source.complete(new RemoteMutationStatus(token,RemoteMutationStatus.State.TERMINAL,1,0,0,CommandResult.Status.SUCCESS,new byte[32]));
        }
        assertEquals(1,calls.get());
    }
}
