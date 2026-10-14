package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.ThriftReplicaPhysicalClient;
import it.cavallium.rockserver.core.client.ThriftReplicaPhysicalWire;
import it.cavallium.rockserver.core.client.ThriftTablePropertiesWire;
import it.cavallium.rockserver.core.common.ColumnTableProperties;
import it.cavallium.rockserver.core.common.ReplicaPhysicalAPI;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.api.*;
import it.cavallium.rockserver.core.server.ReplicaPhysicalThriftService;
import it.cavallium.rockserver.core.server.ThriftServer;
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

class ReplicaPhysicalThriftContractTest {
    private static UUID id(long n){return new UUID(731,n);}
    private static final ReplicaPhysicalAPI.Target TARGET=new ReplicaPhysicalAPI.Target(id(1),id(2),id(3),id(4),id(5),id(6),7);
    private static final ReplicaPhysicalAPI.Scope SCOPE=new ReplicaPhysicalAPI.Scope(TARGET.pool(),TARGET.column());
    private static final ColumnTableProperties TABLES=new ColumnTableProperties(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,
            Map.of(1L,2L),Map.of(2L,2L),Map.of(3L,2L),Map.of(4L,2L),Map.of(7L,2L),Map.of("filter",2L),Map.of("table-comparator",2L),Map.of("merge",2L),Map.of("prefix",2L),Map.of("collector",2L),Map.of("zstd",2L));
    private static final ReplicaPhysicalAPI.Observation OBSERVATION=new ReplicaPhysicalAPI.Observation(TARGET,42,"actual-native-comparator",TABLES);
    private static SstMaintenance.Metadata metadata(){return new SstMaintenance.Metadata(ThriftReplicaPhysicalWire.session(TARGET),TARGET.nativeColumnId(),"physical-cf",4,1,List.of("/pool/db"),List.of(new SstMaintenance.File("000001.sst",1,0,123,"0001","fefe",false)));}
    private static SstMaintenance.Request request(boolean execute){return new SstMaintenance.Request(TARGET.nativeColumnId(),ThriftReplicaPhysicalWire.session(TARGET),List.of("000001.sst"),1,0,1024,4096,2,execute);}
    private static ReplicaPhysicalAPI backend(InvocationHandler handler){return (ReplicaPhysicalAPI)Proxy.newProxyInstance(ReplicaPhysicalAPI.class.getClassLoader(),new Class<?>[]{ReplicaPhysicalAPI.class},handler);}
    private static TMultiplexedProcessor processor(ReplicaPhysicalAPI backend)throws Exception{
        var mux=new TMultiplexedProcessor();var register=ThriftServer.class.getDeclaredMethod("registerPhysical",TMultiplexedProcessor.class,ReplicaPhysicalAPI.class,int.class);register.setAccessible(true);register.invoke(null,mux,backend,ThriftReplicaPhysicalWire.MAX_FRAME_BYTES);return mux;
    }
    private static RocksDBReplicaPhysical.Iface binary(TMultiplexedProcessor processor){
        return (RocksDBReplicaPhysical.Iface)Proxy.newProxyInstance(RocksDBReplicaPhysical.Iface.class.getClassLoader(),new Class<?>[]{RocksDBReplicaPhysical.Iface.class},(p,m,a)->{
            var request=new TMemoryBuffer(4096);var response=new TMemoryBuffer(16384);
            var client=new RocksDBReplicaPhysical.Client(ThriftReplicaPhysicalWire.boundedProtocol(new TBinaryProtocol(response),false),new TMultiplexedProtocol(new TBinaryProtocol(request),ReplicaPhysicalThriftService.SERVICE_NAME));
            try{RocksDBReplicaPhysical.Client.class.getMethod("send_"+m.getName(),m.getParameterTypes()).invoke(client,a);processor.process(new TBinaryProtocol(request),new TBinaryProtocol(response));return RocksDBReplicaPhysical.Client.class.getMethod("recv_"+m.getName()).invoke(client);}catch(InvocationTargetException failure){throw failure.getCause();}
        });
    }
    @Test void explicitlyInjectedOperatorWorkflowPreservesQualificationAndAllProperties()throws Exception{
        var calls=new ArrayList<String>();var scopes=new ArrayList<ReplicaPhysicalAPI.Scope>();
        var api=backend((p,m,a)->{calls.add(m.getName());Object result=switch(m.getName()){
            case "discover"->{scopes.add((ReplicaPhysicalAPI.Scope)a[0]);yield List.of(TARGET);}
            case "observe"->{scopes.add((ReplicaPhysicalAPI.Scope)a[0]);yield List.of(OBSERVATION);}
            case "flush","compact"->{scopes.add((ReplicaPhysicalAPI.Scope)a[0]);yield null;}
            case "metadata"->{assertEquals(TARGET,a[0]);assertEquals(1,a[1]);yield metadata();}
            case "compactFiles"->{assertEquals(TARGET,a[0]);var request=(SstMaintenance.Request)a[1];assertEquals(request(request.execute()),request);yield new SstMaintenance.Result(request.execute(),request.files(),request.execute()?List.of("000003.sst"):List.of(),123,request.execute()?456:0,9);}
            default->throw new AssertionError(m.getName());};return CompletableFuture.completedFuture(result);});
        try(var client=new ThriftReplicaPhysicalClient(binary(processor(api)),Runnable::run,Long.MAX_VALUE)){
            assertEquals(List.of(TARGET),client.discover(SCOPE).join());var observation=client.observe(SCOPE).join().getFirst();assertEquals(OBSERVATION,observation);assertEquals(TABLES,observation.tables());assertEquals("actual-native-comparator",observation.actualCFComparator());
            client.flush(SCOPE).join();client.compact(SCOPE).join();assertEquals(metadata(),client.metadata(TARGET,1).join());
            var dry=client.compactFiles(TARGET,request(false)).join();assertFalse(dry.executed());assertEquals(List.of("000001.sst"),dry.inputFiles());assertTrue(dry.outputFiles().isEmpty());
            var executed=client.compactFiles(TARGET,request(true)).join();assertTrue(executed.executed());assertEquals(List.of("000003.sst"),executed.outputFiles());assertEquals(456,executed.outputBytes());
            var all=new ReplicaPhysicalAPI.Scope(null,null);assertEquals(List.of(TARGET),client.discover(all).join());
            assertEquals(List.of(SCOPE,SCOPE,SCOPE,SCOPE,all),scopes);assertEquals(List.of("discover","observe","flush","compact","metadata","compactFiles","compactFiles","discover"),calls);
        }
    }
    @Test void absentOperatorAuthorityLeavesDefaultServiceUsableAndNoPhysicalRoute()throws Exception{
        var mux=processor(null);var calls=new AtomicInteger();
        var ordinary=(RocksDB.Iface)Proxy.newProxyInstance(RocksDB.Iface.class.getClassLoader(),new Class<?>[]{RocksDB.Iface.class},(p,m,a)->{assertEquals("getCapabilities",m.getName());calls.incrementAndGet();return new Capabilities(BackendMode.STANDALONE,RawScanMode.PHYSICAL_SST,1,1024,CdcMode.CDC_UNSUPPORTED);});
        mux.registerDefault(new RocksDB.Processor<>(ordinary));var request=new TMemoryBuffer(1024);var response=new TMemoryBuffer(1024);var client=new RocksDB.Client(new TBinaryProtocol(response),new TBinaryProtocol(request));client.send_getCapabilities();mux.process(new TBinaryProtocol(request),new TBinaryProtocol(response));assertEquals(BackendMode.STANDALONE,client.recv_getCapabilities().backendMode);assertEquals(1,calls.get());
        assertThrows(TException.class,()->binary(mux).discover(ThriftReplicaPhysicalWire.scope(SCOPE),ThriftReplicaPhysicalWire.context(Long.MAX_VALUE)));assertEquals(1,calls.get());
    }
    @Test void malformedAuthorityContextAndSelectedFilesFailBeforeBackendAdmission()throws Exception{
        var calls=new AtomicInteger();var api=backend((p,m,a)->{calls.incrementAndGet();throw new AssertionError("Invalid request reached physical API");});var service=new ReplicaPhysicalThriftService(api,ThriftReplicaPhysicalWire.MAX_FRAME_BYTES);var target=ThriftReplicaPhysicalWire.target(TARGET);var context=ThriftReplicaPhysicalWire.context(Long.MAX_VALUE);
        assertThrows(RocksDBThriftException.class,()->service.discover(ThriftReplicaPhysicalWire.scope(SCOPE),null));
        assertThrows(RocksDBThriftException.class,()->service.discover(ThriftReplicaPhysicalWire.scope(SCOPE),new ThriftPhysicalContext()));
        assertThrows(RocksDBThriftException.class,()->service.discover(ThriftReplicaPhysicalWire.scope(SCOPE),new ThriftPhysicalContext(0)));
        assertThrows(RocksDBThriftException.class,()->service.discover(new ThriftPhysicalScope().setPool(new byte[17]),context));
        assertThrows(RocksDBThriftException.class,()->service.metadata(target.deepCopy().setNode(new byte[15]),1,context));
        var missing=target.deepCopy();missing.unsetNativeColumnId();assertThrows(RocksDBThriftException.class,()->service.metadata(missing,1,context));
        assertThrows(RocksDBThriftException.class,()->service.metadata(target.deepCopy().setNativeColumnId(-1),1,context));
        assertThrows(RocksDBThriftException.class,()->service.metadata(target,-2,context));
        var selection=ThriftReplicaPhysicalWire.request(request(false),TARGET);
        assertThrows(RocksDBThriftException.class,()->service.compactFiles(target,selection.deepCopy().setColumnId(8),context));
        assertThrows(RocksDBThriftException.class,()->service.compactFiles(target,selection.deepCopy().setSession("other-native-session"),context));
        assertThrows(RocksDBThriftException.class,()->service.compactFiles(target,selection.deepCopy().setFiles(List.of("../000001.sst")),context));
        assertEquals(0,calls.get());
    }
    @Test void crossedRepliesDistributionsAndMetadataNeverBecomePhysicalAuthority()throws Exception{
        var other=new ReplicaPhysicalAPI.Target(id(100),TARGET.incarnation(),TARGET.process(),TARGET.pool(),TARGET.session(),TARGET.column(),TARGET.nativeColumnId());
        var qualified=ThriftReplicaPhysicalWire.metadata(metadata(),TARGET,1);assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.metadata(qualified.deepCopy().setTarget(ThriftReplicaPhysicalWire.target(other)),TARGET,1));
        var stale=qualified.deepCopy();stale.metadata.setSession("stale");assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.metadata(stale,TARGET,1));
        var badPath=qualified.deepCopy();badPath.metadata.files.getFirst().setPathId(1);assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.metadata(badPath,TARGET,1));
        var reversed=qualified.deepCopy();reversed.metadata.files.getFirst().setSmallestKeyHex("ff").setLargestKeyHex("00");assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.metadata(reversed,TARGET,1));
        var wrongLevel=qualified.deepCopy();wrongLevel.metadata.files.getFirst().setLevel(2);assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.metadata(wrongLevel,TARGET,1));
        var wire=ThriftReplicaPhysicalWire.observations(List.of(OBSERVATION),SCOPE).getFirst();
        var missing=wire.deepCopy();missing.tables.unsetDataSize();assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.observationsWire(List.of(missing),SCOPE));
        var badDistribution=wire.deepCopy();badDistribution.tables.setFormatVersions(Map.of(1L,1L));assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.observationsWire(List.of(badDistribution),SCOPE));
        var negative=wire.deepCopy();negative.tables.setCompressions(Map.of("zstd",-1L));assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.observationsWire(List.of(negative),SCOPE));
        var large=wire.deepCopy().setActualCFComparator("x".repeat(ThriftReplicaPhysicalWire.MAX_STRING_BYTES+1));assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.observationsWire(List.of(large),SCOPE));
        assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.targetsWire(List.of(ThriftReplicaPhysicalWire.target(TARGET)),new ReplicaPhysicalAPI.Scope(id(99),null)));
        assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.observationsWire(List.of(wire,wire),SCOPE));
        var invalidDry=new SstMaintenance.Result(false,List.of("000001.sst"),List.of("000003.sst"),123,0,1);assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.result(invalidDry,TARGET,request(false)));
        var wrongSelection=new SstMaintenance.Result(false,List.of("000002.sst"),List.of(),123,0,1);assertThrows(IllegalArgumentException.class,()->ThriftReplicaPhysicalWire.result(wrongSelection,TARGET,request(false)));
        assertEquals(TABLES,ThriftTablePropertiesWire.properties(ThriftTablePropertiesWire.properties(TABLES)));
    }
    @Test void binaryLengthsCollectionsMapsAndConfiguredFramesAreBounded()throws Exception{
        var uuid=new TMemoryBuffer(32);var output=new TBinaryProtocol(uuid);output.writeFieldBegin(new TField("node",TType.STRING,(short)101));output.writeI32(17);
        assertThrows(TProtocolException.class,()->new ThriftPhysicalTarget().read(ThriftReplicaPhysicalWire.boundedProtocol(new TBinaryProtocol(uuid),true)));
        var map=new TMemoryBuffer(32){
            int consumed;
            @Override public int read(byte[] bytes,int offset,int length)throws TTransportException{int read=super.read(bytes,offset,length);consumed+=read;return read;}
        };
        output=new TBinaryProtocol(map);output.writeFieldBegin(new TField("formatVersions",TType.MAP,(short)19));output.writeMapBegin(new TMap(TType.I64,TType.I64,65537));
        var properties=new ColumnTablePropertiesData();
        // TBinaryProtocol rejects the declared minimum body bytes before the decorator count guard.
        var failure=assertThrows(TTransportException.class,()->properties.read(ThriftReplicaPhysicalWire.boundedProtocol(new TBinaryProtocol(map),false)));
        assertEquals(TTransportException.MESSAGE_SIZE_LIMIT,failure.getType());
        assertEquals(9,map.consumed,"Only the field and map headers may be consumed");
        assertFalse(properties.isSetFormatVersions(),"Generated map storage must remain unallocated");
        var names=new TMemoryBuffer(32);output=new TBinaryProtocol(names);output.writeFieldBegin(new TField("name",TType.STRING,(short)202));output.writeI32(25);var strings=ThriftReplicaPhysicalWire.boundedProtocol(new TBinaryProtocol(names),false);strings.readFieldBegin();assertThrows(TProtocolException.class,strings::readString);
        var values=new ArrayList<ReplicaPhysicalAPI.Observation>();for(int i=0;i<17;i++)values.add(new ReplicaPhysicalAPI.Observation(new ReplicaPhysicalAPI.Target(id(1),id(2),id(3),id(4),id(5),id(100+i),i),42,"x".repeat(65536),TABLES));
        var calls=new AtomicInteger();var api=backend((p,m,a)->{calls.incrementAndGet();return CompletableFuture.completedFuture(values);});var service=new ReplicaPhysicalThriftService(api,1024*1024);
        assertEquals(0,calls.get());
        var oversized=assertThrows(RocksDBThriftException.class,()->service.observe(new ThriftPhysicalScope(),ThriftReplicaPhysicalWire.context(Long.MAX_VALUE)));
        assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST,oversized.errorType);assertEquals(1,calls.get());
    }
    @Test void cancellationCloseAndDeadlinesDetachWithoutCancellingBackendWork()throws Exception{
        var source=new CompletableFuture<Void>();var started=new CountDownLatch(1);var calls=new AtomicInteger();var api=backend((p,m,a)->{assertEquals("flush",m.getName());calls.incrementAndGet();started.countDown();return source;});var service=new ReplicaPhysicalThriftService(api,ThriftReplicaPhysicalWire.MAX_FRAME_BYTES);
        try(var executor=Executors.newSingleThreadExecutor();var client=new ThriftReplicaPhysicalClient(service,executor,Long.MAX_VALUE)){
            var observation=client.flush(SCOPE);assertTrue(started.await(5,TimeUnit.SECONDS));assertTrue(observation.cancel(false));assertFalse(source.isCancelled());var queued=client.compact(SCOPE);client.close();assertTrue(queued.isCompletedExceptionally());assertTrue(client.discover(SCOPE).isCompletedExceptionally());assertFalse(source.isCancelled());source.complete(null);
        }
        assertEquals(1,calls.get());
        var pending=new CompletableFuture<Void>();var deadlineService=new ReplicaPhysicalThriftService(backend((p,m,a)->pending),ThriftReplicaPhysicalWire.MAX_FRAME_BYTES);
        var timeout=assertThrows(RocksDBThriftException.class,()->deadlineService.flush(ThriftReplicaPhysicalWire.scope(SCOPE),ThriftReplicaPhysicalWire.context(TimeUnit.MILLISECONDS.toNanos(20))));assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED,timeout.errorType);assertFalse(pending.isCancelled());pending.complete(null);
        var tasks=new ArrayList<Runnable>();var notAdmitted=new AtomicInteger();
        try(var client=new ThriftReplicaPhysicalClient(new ReplicaPhysicalThriftService(backend((p,m,a)->{notAdmitted.incrementAndGet();throw new AssertionError("Expired queued operation");}),ThriftReplicaPhysicalWire.MAX_FRAME_BYTES),tasks::add,TimeUnit.MILLISECONDS.toNanos(20))){assertThrows(CompletionException.class,()->client.flush(SCOPE).join());tasks.getFirst().run();assertEquals(0,notAdmitted.get());}
        assertThrows(IllegalArgumentException.class,()->new ThriftReplicaPhysicalClient(service,Runnable::run,0));
    }
}
