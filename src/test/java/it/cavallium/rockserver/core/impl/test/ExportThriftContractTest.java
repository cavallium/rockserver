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
    private static final byte[] DEFINITION=new byte[ExportJobs.MIN_PAGE_BYTES+3];
    private static final byte[] CAPTURES=new byte[]{1,2,3};
    private static byte[] digest(byte[] bytes) {
        try { return java.security.MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch(java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static final ExportJobs.Status STATUS=new ExportJobs.Status(JOB,ExportJobs.Phase.READY,List.of(1L),1,1,0,3,false,
            digest(DEFINITION),DEFINITION.length,7,2,digest(CAPTURES),CAPTURES.length);
    private static ExportJobs.WitnessPage witnessPage(ExportJobs.WitnessRequest request) {
        var body=request.kind()==ExportJobs.WitnessKind.DEFINITION?DEFINITION:CAPTURES;
        assertArrayEquals(digest(body),request.expectedDigest());
        int end=(int)Math.min(body.length,request.offset()+request.maximumBytes());
        return new ExportJobs.WitnessPage(JOB,request.kind(),digest(body),body.length,request.offset(),
                java.util.Arrays.copyOfRange(body,(int)request.offset(),end),end<body.length);
    }
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
                case "exportWitness" -> witnessPage((ExportJobs.WitnessRequest)args[0]);
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
        for(var kind:ExportJobs.WitnessKind.values()) {
            byte[] expected=kind==ExportJobs.WitnessKind.DEFINITION?STATUS.definitionDigest():STATUS.captureDigest();
            var accumulated=new java.io.ByteArrayOutputStream(); long offset=0;
            while(true) {
                var witness=client.exportWitness(new ExportJobs.WitnessRequest(JOB,kind,expected,offset,ExportJobs.MIN_PAGE_BYTES),context).join();
                accumulated.writeBytes(witness.bytes()); offset+=witness.bytes().length;
                if(!witness.hasMore()) { assertEquals(witness.totalBytes(),offset); break; }
            }
            assertArrayEquals(expected,digest(accumulated.toByteArray()));
        }
        client.acknowledgeExport(ID,context).join(); client.releaseExport(JOB,context).join(); client.cancelExport(JOB,context).join();
        assertEquals(List.of("createExport","recoverExport","stepExport","exportStatus","exportManifest","readExport","exportWitness","exportWitness","exportWitness","acknowledgeExport","releaseExport","cancelExport"),calls);
        var encoded=ThriftExportWire.create(create);
        var decoded=new ExportCreate();
        new org.apache.thrift.TDeserializer().deserialize(decoded,new org.apache.thrift.TSerializer().serialize(encoded));
        assertEquals(create,ThriftExportWire.create(decoded));
        var page=ThriftExportWire.manifest(new ExportJobs.ManifestPage(List.of(CHUNK),1,false),manifest);
        var decodedPage=new ExportManifestPage();
        new org.apache.thrift.TDeserializer().deserialize(decodedPage,new org.apache.thrift.TSerializer().serialize(page));
        assertEquals(ID,ThriftExportWire.manifest(decodedPage,manifest).chunks().getFirst().id());
    }
    @Test void aggregateWitnessMetadataIsRequiredBoundedAndPreservedThroughSerialization() throws Exception {
        var decoded=new ExportStatus();
        new org.apache.thrift.TDeserializer().deserialize(decoded,new org.apache.thrift.TSerializer().serialize(ThriftExportWire.status(STATUS)));
        assertEquals(STATUS,ThriftExportWire.status(decoded,JOB));
        var capturing=new ExportJobs.Status(JOB,ExportJobs.Phase.CAPTURING,List.of(1L),0,0,0,0,false,
                STATUS.definitionDigest(),STATUS.definitionBytes(),STATUS.rosterGeneration(),STATUS.domainCount(),new byte[0],0);
        assertEquals(capturing,ThriftExportWire.status(ThriftExportWire.status(capturing),JOB));
        var missing=ThriftExportWire.status(STATUS); missing.unsetDefinitionDigest();
        assertThrows(org.apache.thrift.protocol.TProtocolException.class,missing::validate);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.status(missing));
        var missingCaptures=ThriftExportWire.status(STATUS); missingCaptures.unsetCaptureDigest();
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.status(missingCaptures));
        var invalidHash=ThriftExportWire.status(STATUS).setDefinitionDigest(new byte[33]);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.status(invalidHash));
        var oversized=ThriftExportWire.status(STATUS).setDefinitionBytes(ExportJobs.MAX_WITNESS_BYTES+1L);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.status(oversized));
        var wrongCapture=ThriftExportWire.status(STATUS).setCaptureBytes(0);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.status(wrongCapture));
        var noDomains=ThriftExportWire.status(STATUS).setDomainCount(0);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.status(noDomains));
    }
    @Test void pagedWitnessWireRejectsIdentityDigestOffsetAndProgressChangesBeforeCopy() throws Exception {
        for(var kind:ExportJobs.WitnessKind.values()) {
            var request=new ExportJobs.WitnessRequest(JOB,kind,kind==ExportJobs.WitnessKind.DEFINITION?STATUS.definitionDigest():STATUS.captureDigest(),0,ExportJobs.MIN_PAGE_BYTES);
            assertEquals(request,ThriftExportWire.witnessRequest(ThriftExportWire.witnessRequest(request)));
            var source=witnessPage(request); var wire=ThriftExportWire.witness(source,request); var decoded=new ExportWitnessPage();
            new org.apache.thrift.TDeserializer().deserialize(decoded,new org.apache.thrift.TSerializer().serialize(wire));
            assertEquals(source,ThriftExportWire.witness(decoded,request));
            var wrongJob=wire.deepCopy().setJobId(ThriftExportWire.uuid(UUID.randomUUID()));
            assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witness(wrongJob,request));
            var wrongHash=wire.deepCopy().setDigest(new byte[32]);
            assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witness(wrongHash,request));
            var wrongKind=wire.deepCopy().setKind(kind==ExportJobs.WitnessKind.DEFINITION?ExportWitnessKind.CAPTURES:ExportWitnessKind.DEFINITION);
            assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witness(wrongKind,request));
            var wrongOffset=wire.deepCopy().setOffset(1);
            assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witness(wrongOffset,request));
            var oversized=wire.deepCopy().setData(new byte[request.maximumBytes()+1]);
            assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witness(oversized,request));
            var wrongProgress=wire.deepCopy().setHasMore(!wire.hasMore);
            assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witness(wrongProgress,request));
            var missing=wire.deepCopy(); missing.unsetDigest();
            assertThrows(org.apache.thrift.protocol.TProtocolException.class,missing::validate);
            assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witness(missing,request));
        }
        var request=new ExportJobs.WitnessRequest(JOB,ExportJobs.WitnessKind.DEFINITION,STATUS.definitionDigest(),0,ExportJobs.MIN_PAGE_BYTES);
        var stalled=ThriftExportWire.witness(witnessPage(request),request).setData(new byte[0]);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witness(stalled,request));
        var eofRequest=new ExportJobs.WitnessRequest(JOB,ExportJobs.WitnessKind.DEFINITION,STATUS.definitionDigest(),DEFINITION.length,ExportJobs.MIN_PAGE_BYTES);
        var eof=new ExportJobs.WitnessPage(JOB,ExportJobs.WitnessKind.DEFINITION,STATUS.definitionDigest(),DEFINITION.length,DEFINITION.length,new byte[0],false);
        assertEquals(eof,ThriftExportWire.witness(ThriftExportWire.witness(eof,eofRequest),eofRequest));
        var oversizedTotal=ThriftExportWire.witness(witnessPage(request),request).setTotalBytes(ExportJobs.MAX_WITNESS_BYTES+1L);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witness(oversizedTotal,request));
        var invalidRequest=ThriftExportWire.witnessRequest(request).setExpectedDigest(new byte[31]);
        assertThrows(IllegalArgumentException.class,()->ThriftExportWire.witnessRequest(invalidRequest));
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
    @Test void witnessRouteRoundTripThroughMultiplexedProcessorInMemory() throws Exception {
        var service=new ExportThriftService(backend((p,m,a)->CompletableFuture.completedFuture(witnessPage((ExportJobs.WitnessRequest)a[0]))),ThriftTransportLimits.DEFAULT_MAX_FRAME_SIZE);
        var processor=new org.apache.thrift.TMultiplexedProcessor(); processor.registerProcessor(ExportThriftService.SERVICE_NAME,new RocksDBExports.Processor<>(service));
        var requestBuffer=new org.apache.thrift.transport.TMemoryBuffer(256); var responseBuffer=new org.apache.thrift.transport.TMemoryBuffer(256);
        var client=new RocksDBExports.Client(new org.apache.thrift.protocol.TBinaryProtocol(responseBuffer),
                new org.apache.thrift.protocol.TMultiplexedProtocol(new org.apache.thrift.protocol.TBinaryProtocol(requestBuffer),ExportThriftService.SERVICE_NAME));
        var request=new ExportJobs.WitnessRequest(JOB,ExportJobs.WitnessKind.CAPTURES,STATUS.captureDigest(),0,ExportJobs.MIN_PAGE_BYTES);
        client.send_exportWitness(ThriftExportWire.witnessRequest(request),new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.BATCH,Long.MAX_VALUE));
        processor.process(new org.apache.thrift.protocol.TBinaryProtocol(requestBuffer),new org.apache.thrift.protocol.TBinaryProtocol(responseBuffer));
        assertEquals(witnessPage(request),ThriftExportWire.witness(client.recv_exportWitness(),request));
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
        assertThrows(RocksDBThriftException.class,()->service.exportWitness(ThriftExportWire.witnessRequest(new ExportJobs.WitnessRequest(JOB,ExportJobs.WitnessKind.DEFINITION,STATUS.definitionDigest(),0,ExportJobs.MAX_WITNESS_PAGE_BYTES)),wire));
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
            return CompletableFuture.completedFuture(switch(m.getName()) {
                case "readExport" -> new ExportJobs.FilePage(ID,0,new byte[maximum],false);
                case "exportWitness" -> new ExportJobs.WitnessPage(JOB,ExportJobs.WitnessKind.DEFINITION,new byte[32],maximum,0,new byte[maximum],false);
                default -> new ExportJobs.ManifestPage(List.of(new ExportJobs.Chunk(ID,1,
                        new byte[maximum-ExportJobs.CHUNK_METADATA_BYTES-9],null,1,1,3,false)),1,false);
            });
        });
        var service=new ExportThriftService(api,ThriftTransportLimits.MIN_MAX_FRAME_SIZE);
        var context=new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.BATCH,Long.MAX_VALUE);
        var page=service.readExport(new ExportReadRequest(ThriftExportWire.id(ID),0,service.getExportCapabilities().maximumFilePageBytes),context);
        assertEquals(maximum,page.data.remaining());
        assertTrue(new org.apache.thrift.TSerializer().serialize(page).length<ThriftTransportLimits.MIN_MAX_FRAME_SIZE);
        var manifest=service.exportManifest(new ExportManifestRequest(ThriftExportWire.uuid(JOB),0,1,service.getExportCapabilities().maximumManifestBytes),context);
        assertTrue(new org.apache.thrift.TSerializer().serialize(manifest).length<ThriftTransportLimits.MIN_MAX_FRAME_SIZE);
        var witnessRequest=new ExportWitnessRequest(ThriftExportWire.uuid(JOB),ExportWitnessKind.DEFINITION,ByteBuffer.wrap(new byte[32]),0,maximum);
        var witness=service.exportWitness(witnessRequest,context);
        assertEquals(maximum,witness.data.remaining());
        assertTrue(new org.apache.thrift.TSerializer().serialize(witness).length<ThriftTransportLimits.MIN_MAX_FRAME_SIZE);
        calls.set(0);
        assertThrows(RocksDBThriftException.class,()->service.readExport(new ExportReadRequest(ThriftExportWire.id(ID),0,maximum+1),context));
        assertThrows(RocksDBThriftException.class,()->service.exportManifest(new ExportManifestRequest(ThriftExportWire.uuid(JOB),0,1,maximum+1),context));
        assertThrows(RocksDBThriftException.class,()->service.exportWitness(witnessRequest.deepCopy().setMaximumBytes(maximum+1),context));
        assertEquals(0,calls.get());
    }
}
