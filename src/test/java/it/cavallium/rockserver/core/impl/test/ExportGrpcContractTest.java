package it.cavallium.rockserver.core.impl.test;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.api.export.proto.*;
import it.cavallium.rockserver.core.common.export.*;
import it.cavallium.rockserver.core.server.ExportGrpcService;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ExportGrpcContractTest {
    private final UUID job = new UUID(0, 1);
    private ExportJobs.ChunkId id() { return new ExportJobs.ChunkId(job, new UUID(0, 2), new UUID(0, 3), 1, 1, 2, 0, new byte[32]); }
    private ExportJobs.Status status() { return new ExportJobs.Status(job, ExportJobs.Phase.READY, List.of(-1L), 1, 0, 0, 0, false); }
    @Test void pathFreeRoundTripPreservesSignedIdsAndBoundPresence() throws Exception {
        var create = new ExportJobs.Create(job, List.of(Long.MIN_VALUE, -1L, Long.MAX_VALUE), ExportJobs.Limits.defaults());
        assertEquals(create, ExportWire.create(ExportCreateRequest.parseFrom(ExportWire.create(create).toByteArray())));
        assertEquals(id(), ExportWire.id(ExportChunkId.parseFrom(ExportWire.id(id()).toByteArray())));
        var chunk = new ExportJobs.Chunk(id(), -1, new byte[0], null, 0, 0, 0, false);
        var wire = ExportChunk.parseFrom(ExportWire.chunk(chunk).toByteArray());
        assertTrue(wire.hasLowerInclusive()); assertFalse(wire.hasUpperExclusive());
        assertArrayEquals(new byte[0], ExportWire.chunk(wire).lowerInclusive());
        assertNull(ExportWire.chunk(wire).upperExclusive());
        for (var descriptor : Exports.getDescriptor().getMessageTypes()) for (var field : descriptor.getFields()) {
            assertFalse(field.getName().toLowerCase().contains("path"));
            assertFalse(field.getName().toLowerCase().contains("version"));
        }
    }
    @Test void malformedInputsNeverReachBackend() {
        var backend = mock(ExportAsyncAPI.class); var service = new ExportGrpcService(backend);
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> service.createExport(ExportCreateRequest.newBuilder()
                .setRequestId(ByteString.copyFrom(new byte[16])).addAllColumns(java.util.Collections.nCopies(257, 1L)).build()).block());
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> service.exportStatus(ExportJobRequest.newBuilder().setJobId(ByteString.copyFrom(new byte[17])).build()).block());
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> service.acknowledgeExport(ExportAcknowledgeRequest.newBuilder()
                .setId(ExportWire.id(id()).toBuilder().setContentHash(ByteString.copyFrom(new byte[33]))).build()).block());
        verifyNoInteractions(backend);
    }
    @Test void cancellationOnlyDiscardsCreateObservationAndRetryCanObserveJob() {
        var backend = mock(ExportAsyncAPI.class);
        var pending = new CompletableFuture<ExportJobs.Status>();
        when(backend.createExport(any(), any())).thenReturn(pending);
        when(backend.exportStatus(eq(job), any())).thenAnswer(invocation -> CompletableFuture.completedFuture(status()));
        var service = new ExportGrpcService(backend);
        var replies = new AtomicInteger();
        var subscription = service.createExport(ExportWire.create(new ExportJobs.Create(job, List.of(-1L), ExportJobs.Limits.defaults())))
                .subscribe(value -> replies.incrementAndGet());
        subscription.dispose();
        assertFalse(pending.isCancelled());
        pending.complete(status());
        assertEquals(0, replies.get());
        assertEquals(job, ExportWire.status(service.exportStatus(ExportJobRequest.newBuilder().setJobId(ExportWire.uuid(job)).build()).block()).jobId());
        verify(backend, never()).cancelExport(any(), any());
    }
    @Test void responsePagesMustEchoIdentityOffsetAndBudget() {
        var request = new ExportJobs.ReadRequest(id(), 10, ExportJobs.MIN_PAGE_BYTES);
        assertThrows(IllegalArgumentException.class, () -> ExportWire.file(new ExportJobs.FilePage(id(), 11, new byte[1], false), request));
        assertThrows(IllegalArgumentException.class, () -> ExportWire.file(new ExportJobs.FilePage(id(), 10, new byte[request.maximumBytes()+1], false), request));
        var manifest = new ExportJobs.ManifestRequest(job, 2, 1, ExportJobs.MIN_PAGE_BYTES);
        assertThrows(IllegalArgumentException.class, () -> ExportWire.manifest(new ExportJobs.ManifestPage(List.of(), 3, false), manifest));
        var backend = mock(ExportAsyncAPI.class);
        when(backend.readExport(any(), any())).thenReturn(CompletableFuture.completedFuture(new ExportJobs.FilePage(id(), 11, new byte[1], false)));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> new ExportGrpcService(backend).readExport(ExportReadRequest.newBuilder()
                .setId(ExportWire.id(id())).setOffset(10).setMaximumBytes(request.maximumBytes()).build()).block());
    }
    @Test void manifestOffsetSpansGroupsWhoseOrdinalsRestart() {
        var groupA = new ExportJobs.ChunkId(job, new UUID(0, 2), new UUID(0, 3), 1, 1, 2, 1, new byte[32]);
        var groupB = new ExportJobs.ChunkId(job, new UUID(0, 4), new UUID(0, 5), 1, 1, 2, 0, new byte[32]);
        var first = new ExportJobs.Chunk(groupA, -1, null, null, 0, 0, 0, false);
        var second = new ExportJobs.Chunk(groupB, -1, null, null, 0, 0, 0, false);
        var request = new ExportJobs.ManifestRequest(job, 1, 2, ExportJobs.MIN_PAGE_BYTES);
        var page = new ExportJobs.ManifestPage(List.of(first, second), 3, false);
        var wire = ExportWire.manifest(page, request);
        assertEquals(List.of(groupA, groupB), ExportWire.manifest(wire, request).chunks().stream().map(ExportJobs.Chunk::id).toList());
        var backend = mock(ExportAsyncAPI.class);
        when(backend.exportManifest(eq(request), any())).thenReturn(CompletableFuture.completedFuture(page));
        var response = new ExportGrpcService(backend).exportManifest(ExportManifestRequest.newBuilder()
                .setJobId(ExportWire.uuid(job)).setOffset(1).setMaximumItems(2).setMaximumBytes(request.maximumBytes()).build()).block();
        assertEquals(wire, response);
        var duplicate = new ExportJobs.ManifestPage(List.of(first, first), 3, false);
        assertThrows(IllegalArgumentException.class, () -> ExportWire.manifest(duplicate, request));
        assertThrows(IllegalArgumentException.class, () -> ExportWire.manifest(wire.toBuilder().setChunks(1, wire.getChunks(0)).build(), request));
    }
    @Test void backendAdmissionPressureIsResourceExhausted() {
        var backend = mock(ExportAsyncAPI.class);
        when(backend.exportStatus(eq(job), any())).thenReturn(CompletableFuture.failedFuture(
                new java.util.concurrent.RejectedExecutionException("Export work exhausted")));
        assertStatus(Status.Code.RESOURCE_EXHAUSTED, () -> new ExportGrpcService(backend).exportStatus(
                ExportJobRequest.newBuilder().setJobId(ExportWire.uuid(job)).build()).block());
        when(backend.exportStatus(eq(job), any())).thenReturn(CompletableFuture.failedFuture(
                new java.util.concurrent.TimeoutException("Export deadline expired")));
        assertStatus(Status.Code.DEADLINE_EXCEEDED, () -> new ExportGrpcService(backend).exportStatus(
                ExportJobRequest.newBuilder().setJobId(ExportWire.uuid(job)).build()).block());
    }
    @Test void protectedProfilesCannotBeSpoofedAndDisabledRouteAdvertisesUnavailable() {
        var backend = mock(ExportAsyncAPI.class);
        var request = ExportJobRequest.newBuilder().setJobId(ExportWire.uuid(job)).setContext(
                it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder().setProfileValue(1).setTimeoutNanos(1000)).build();
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> new ExportGrpcService(backend).releaseExport(request).block());
        verifyNoInteractions(backend);
        var disabled = new ExportGrpcService(null);
        assertFalse(disabled.getExportCapabilities(Empty.getDefaultInstance()).block().getAvailable());
        assertStatus(Status.Code.UNIMPLEMENTED, () -> disabled.exportStatus(request).block());
    }
    @Test void boundedMarshallerRejectsOversizedUnknownFieldsBeforeDispatch() {
        var backend = mock(ExportAsyncAPI.class);
        var service = new ExportGrpcService(backend).bindBoundedService();
        var method = service.getMethod(RocksDBExportServiceGrpc.getCreateExportMethod().getFullMethodName());
        var request = ExportWire.create(new ExportJobs.Create(job, List.of(-1L), ExportJobs.Limits.defaults()));
        var unknown = com.google.protobuf.UnknownFieldSet.newBuilder().addField(100,
                com.google.protobuf.UnknownFieldSet.Field.newBuilder()
                        .addLengthDelimited(ByteString.copyFrom(new byte[ExportGrpcService.MAX_REQUEST_BYTES])).build()).build();
        var oversized = request.toBuilder().setUnknownFields(unknown).build().toByteArray();
        var input = new java.io.ByteArrayInputStream(oversized);
        assertStatus(Status.Code.RESOURCE_EXHAUSTED, () -> method.getMethodDescriptor().getRequestMarshaller().parse(input));
        assertEquals(oversized.length - ExportGrpcService.MAX_REQUEST_BYTES - 1, input.available());
        verifyNoInteractions(backend);
    }
    @Test void boundedServicePreservesAllGeneratedMethodContracts() {
        var implementation = new ExportGrpcService(null);
        var generated = implementation.bindService();
        var bounded = implementation.bindBoundedService();
        assertEquals(generated.getServiceDescriptor().getName(), bounded.getServiceDescriptor().getName());
        assertSame(generated.getServiceDescriptor().getSchemaDescriptor(), bounded.getServiceDescriptor().getSchemaDescriptor());
        assertEquals(10, bounded.getMethods().size());
        for (var original : generated.getMethods()) {
            var descriptor = original.getMethodDescriptor();
            var wrapped = bounded.getMethod(descriptor.getFullMethodName()).getMethodDescriptor();
            assertEquals(descriptor.getType(), wrapped.getType());
            assertSame(descriptor.getSchemaDescriptor(), wrapped.getSchemaDescriptor());
            assertSame(descriptor.getResponseMarshaller(), wrapped.getResponseMarshaller());
        }
        var request = ExportWire.create(new ExportJobs.Create(job, List.of(-1L), ExportJobs.Limits.defaults()));
        var method = bounded.getMethod(RocksDBExportServiceGrpc.getCreateExportMethod().getFullMethodName());
        assertEquals(request, method.getMethodDescriptor().getRequestMarshaller().parse(new java.io.ByteArrayInputStream(request.toByteArray())));
    }
    private static void assertStatus(Status.Code code, Runnable operation) {
        var error = assertThrows(RuntimeException.class, operation::run);
        assertEquals(code, Status.fromThrowable(error).getCode());
    }
}
