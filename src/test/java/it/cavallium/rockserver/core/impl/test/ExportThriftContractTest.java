package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.ThriftExportClient;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.api.*;
import it.cavallium.rockserver.core.common.export.*;
import it.cavallium.rockserver.core.server.ExportThriftService;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ExportThriftContractTest {
    private static final UUID JOB=UUID.randomUUID();
    private static final ExportJobs.ChunkId ID=new ExportJobs.ChunkId(JOB,UUID.randomUUID(),UUID.randomUUID(),1,1,1,0,new byte[32]);
    private static final ExportJobs.Chunk CHUNK=new ExportJobs.Chunk(ID,1,null,null,1,1,3,false);
    private static final ExportJobs.Status STATUS=new ExportJobs.Status(JOB,ExportJobs.Phase.READY,List.of(1L),1,1,0,3,false);
    private static ExportAsyncAPI backend(java.lang.reflect.InvocationHandler handler) {
        return (ExportAsyncAPI)Proxy.newProxyInstance(ExportAsyncAPI.class.getClassLoader(),new Class<?>[]{ExportAsyncAPI.class},handler);
    }
    @Test void completeLifecycleAndTypedSerialization() throws Exception {
        var calls=new java.util.ArrayList<String>();
        var api=backend((proxy,method,args)->{
            calls.add(method.getName());
            assertEquals(RequestContext.batch(),args[args.length-1]);
            return CompletableFuture.completedFuture(switch(method.getName()) {
                case "exportManifest" -> new ExportJobs.ManifestPage(List.of(CHUNK),1,false);
                case "readExport" -> new ExportJobs.FilePage(ID,0,new byte[]{1,2,3},false);
                case "acknowledgeExport","releaseExport","cancelExport" -> null;
                default -> STATUS;
            });
        });
        var service=new ExportThriftService(api,ThriftTransportLimits.DEFAULT_MAX_FRAME_SIZE);
        var client=new ThriftExportClient(service,Runnable::run);
        var context=RequestContext.batch();
        assertTrue(client.capabilities(context).join().available());
        var create=new ExportJobs.Create(UUID.randomUUID(),List.of(1L),ExportJobs.Limits.defaults());
        assertEquals(STATUS,client.createExport(create,context).join());
        assertEquals(STATUS,client.recoverExport(JOB,context).join());
        assertEquals(STATUS,client.stepExport(JOB,context).join());
        assertEquals(STATUS,client.exportStatus(JOB,context).join());
        var manifest=new ExportJobs.ManifestRequest(JOB,0,1,ExportJobs.MIN_PAGE_BYTES);
        assertEquals(ID,client.exportManifest(manifest,context).join().chunks().getFirst().id());
        var read=new ExportJobs.ReadRequest(ID,0,ExportJobs.MIN_PAGE_BYTES);
        assertArrayEquals(new byte[]{1,2,3},client.readExport(read,context).join().bytes());
        client.acknowledgeExport(ID,context).join(); client.releaseExport(JOB,context).join(); client.cancelExport(JOB,context).join();
        assertEquals(List.of("createExport","recoverExport","stepExport","exportStatus","exportManifest","readExport","acknowledgeExport","releaseExport","cancelExport"),calls);
        var encoded=ThriftExportWire.create(create);
        var decoded=new ExportCreate();
        new org.apache.thrift.TDeserializer().deserialize(decoded,new org.apache.thrift.TSerializer().serialize(encoded));
        assertEquals(create,ThriftExportWire.create(decoded));
        var page=ThriftExportWire.manifest(new ExportJobs.ManifestPage(List.of(CHUNK),1,false),manifest);
        var decodedPage=new ExportManifestPage();
        new org.apache.thrift.TDeserializer().deserialize(decodedPage,new org.apache.thrift.TSerializer().serialize(page));
        assertEquals(ID,ThriftExportWire.manifest(decodedPage,manifest).chunks().getFirst().id());
    }
    @Test void multiplexedProcessorRoundTripInMemory() throws Exception {
        var service=new ExportThriftService(backend((p,m,a)->CompletableFuture.completedFuture(STATUS)),ThriftTransportLimits.DEFAULT_MAX_FRAME_SIZE);
        var processor=new org.apache.thrift.TMultiplexedProcessor();
        processor.registerProcessor(ExportThriftService.SERVICE_NAME,new RocksDBExports.Processor<>(service));
        var request=new org.apache.thrift.transport.TMemoryBuffer(256);
        var response=new org.apache.thrift.transport.TMemoryBuffer(256);
        var client=new RocksDBExports.Client(new org.apache.thrift.protocol.TBinaryProtocol(response),
                new org.apache.thrift.protocol.TMultiplexedProtocol(new org.apache.thrift.protocol.TBinaryProtocol(request),ExportThriftService.SERVICE_NAME));
        client.send_exportStatus(ThriftExportWire.uuid(JOB),new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.BATCH,Long.MAX_VALUE));
        processor.process(new org.apache.thrift.protocol.TBinaryProtocol(request),new org.apache.thrift.protocol.TBinaryProtocol(response));
        assertEquals(STATUS,ThriftExportWire.status(client.recv_exportStatus(),JOB));
    }
    @Test void standaloneUnprefixedClientAndDisabledExportRouteShareProcessor() throws Exception {
        var calls=new AtomicInteger();
        var standalone=new it.cavallium.rockserver.core.common.api.Capabilities(
                BackendMode.STANDALONE,RawScanMode.UNSUPPORTED,128,65536,it.cavallium.rockserver.core.common.api.CdcMode.SCALAR);
        var database=(RocksDB.Iface)Proxy.newProxyInstance(RocksDB.Iface.class.getClassLoader(),
                new Class<?>[]{RocksDB.Iface.class},(proxy,method,args)->{
                    assertEquals("getCapabilities",method.getName());
                    calls.incrementAndGet();
                    return standalone;
                });
        var processor=new org.apache.thrift.TMultiplexedProcessor();
        processor.registerDefault(new RocksDB.Processor<>(database));
        processor.registerProcessor(ExportThriftService.SERVICE_NAME,
                new RocksDBExports.Processor<>(new ExportThriftService(null,ThriftTransportLimits.DEFAULT_MAX_FRAME_SIZE)));
        var request=new org.apache.thrift.transport.TMemoryBuffer(256);
        var response=new org.apache.thrift.transport.TMemoryBuffer(256);
        var client=new RocksDB.Client(new org.apache.thrift.protocol.TBinaryProtocol(response),
                new org.apache.thrift.protocol.TBinaryProtocol(request));
        client.send_getCapabilities();
        processor.process(new org.apache.thrift.protocol.TBinaryProtocol(request),new org.apache.thrift.protocol.TBinaryProtocol(response));
        assertEquals(standalone,client.recv_getCapabilities());
        assertEquals(1,calls.get());
        var exports=new RocksDBExports.Client(new org.apache.thrift.protocol.TBinaryProtocol(response),
                new org.apache.thrift.protocol.TMultiplexedProtocol(new org.apache.thrift.protocol.TBinaryProtocol(request),ExportThriftService.SERVICE_NAME));
        exports.send_getExportCapabilities();
        processor.process(new org.apache.thrift.protocol.TBinaryProtocol(request),new org.apache.thrift.protocol.TBinaryProtocol(response));
        assertFalse(exports.recv_getExportCapabilities().available);
        assertEquals(1,calls.get());
    }
    @Test void backendAdmissionAndEffectiveRequestCaps() throws Exception {
        var calls=new AtomicInteger();
        var service=new ExportThriftService(backend((p,m,a)->{calls.incrementAndGet();throw new java.util.concurrent.RejectedExecutionException("full");}),ThriftTransportLimits.MIN_MAX_FRAME_SIZE);
        var wire=new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.BATCH,Long.MAX_VALUE);
        var error=assertThrows(RocksDBThriftException.class,()->service.exportStatus(ThriftExportWire.uuid(JOB),wire));
        assertEquals(RocksDBErrorType.SERVER_OVERLOADED,error.errorType);
        calls.set(0);
        assertThrows(RocksDBThriftException.class,()->service.exportManifest(new ExportManifestRequest(ThriftExportWire.uuid(JOB),0,1,ExportJobs.MAX_MANIFEST_BYTES),wire));
        assertThrows(RocksDBThriftException.class,()->service.readExport(new ExportReadRequest(ThriftExportWire.id(ID),0,ExportJobs.MAX_FILE_PAGE_BYTES),wire));
        assertEquals(0,calls.get());
    }
    @Test void disabledRouteAndMalformedIdentity() throws Exception {
        var service=new ExportThriftService(null,ThriftTransportLimits.DEFAULT_MAX_FRAME_SIZE);
        assertFalse(service.getExportCapabilities().available);
        assertThrows(org.apache.thrift.TApplicationException.class,()->service.exportStatus(ThriftExportWire.uuid(JOB),new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.BATCH,Long.MAX_VALUE)));
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.uuid(ByteBuffer.allocate(17)));
        var request=new ExportJobs.ReadRequest(ID,0,ExportJobs.MIN_PAGE_BYTES);
        var wrong=new ExportFilePage(ThriftExportWire.id(ID),1,ByteBuffer.wrap(new byte[1]),false);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.file(wrong,request));
        var manifest=new ExportJobs.ManifestRequest(JOB,0,2,ExportJobs.MIN_PAGE_BYTES);
        var duplicate=new ExportManifestPage(List.of(ThriftExportWire.manifest(new ExportJobs.ManifestPage(List.of(CHUNK),1,false),manifest).chunks.getFirst(),ThriftExportWire.manifest(new ExportJobs.ManifestPage(List.of(CHUNK),1,false),manifest).chunks.getFirst()),2,false);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.manifest(duplicate,manifest));
    }
    @Test void cancellationAndDeadlineDoNotCancelBackend() throws Exception {
        var source=new CompletableFuture<ExportJobs.Status>();
        var calls=new AtomicInteger();
        var api=backend((p,m,a)->{calls.incrementAndGet();return source;});
        var service=new ExportThriftService(api,ThriftTransportLimits.DEFAULT_MAX_FRAME_SIZE);
        try(var executor=java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var client=new ThriftExportClient(service,executor);
            var result=client.exportStatus(JOB,RequestContext.batch(java.time.Duration.ofMillis(100)));
            assertThrows(java.util.concurrent.CompletionException.class,result::join);
            assertFalse(source.isCancelled());
            source.complete(STATUS);
            assertEquals(1,calls.get());
        }
        var pending=new CompletableFuture<ExportJobs.Status>();
        var started=new java.util.concurrent.CountDownLatch(1);
        var api2=backend((p,m,a)->{started.countDown();return pending;});
        try(var executor=java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var client=new ThriftExportClient(new ExportThriftService(api2,ThriftTransportLimits.DEFAULT_MAX_FRAME_SIZE),executor);
            var result=client.exportStatus(JOB,RequestContext.batch());
            assertTrue(started.await(5,java.util.concurrent.TimeUnit.SECONDS));
            assertTrue(result.cancel(false)); assertFalse(pending.isCancelled());
            pending.complete(STATUS);
        }
    }
    @Test void exactAdvertisedPayloadLimitFitsConfiguredFrame() throws Exception {
        var calls=new AtomicInteger();
        int maximum=ThriftTransportLimits.safeCdcResponseSize(ThriftTransportLimits.MIN_MAX_FRAME_SIZE);
        var api=backend((p,m,a)->{
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(m.getName().equals("readExport")
                    ? new ExportJobs.FilePage(ID,0,new byte[maximum],false)
                    : new ExportJobs.ManifestPage(List.of(new ExportJobs.Chunk(ID,1,
                            new byte[maximum-ExportJobs.CHUNK_METADATA_BYTES-9],null,1,1,3,false)),1,false));
        });
        var service=new ExportThriftService(api,ThriftTransportLimits.MIN_MAX_FRAME_SIZE);
        var context=new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.BATCH,Long.MAX_VALUE);
        var page=service.readExport(new ExportReadRequest(ThriftExportWire.id(ID),0,service.getExportCapabilities().maximumFilePageBytes),context);
        assertEquals(maximum,page.data.remaining());
        assertTrue(new org.apache.thrift.TSerializer().serialize(page).length<ThriftTransportLimits.MIN_MAX_FRAME_SIZE);
        var manifest=service.exportManifest(new ExportManifestRequest(ThriftExportWire.uuid(JOB),0,1,service.getExportCapabilities().maximumManifestBytes),context);
        assertTrue(new org.apache.thrift.TSerializer().serialize(manifest).length<ThriftTransportLimits.MIN_MAX_FRAME_SIZE);
        calls.set(0);
        assertThrows(RocksDBThriftException.class,()->service.readExport(new ExportReadRequest(ThriftExportWire.id(ID),0,maximum+1),context));
        assertThrows(RocksDBThriftException.class,()->service.exportManifest(new ExportManifestRequest(ThriftExportWire.uuid(JOB),0,1,maximum+1),context));
        assertEquals(0,calls.get());
    }
}
