package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.*;
import it.cavallium.rockserver.core.common.export.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/** Borrowed export backend; timeouts stop observing, never cancel durable backend work. */
public final class ExportThriftService implements RocksDBExports.Iface {
    public static final String SERVICE_NAME="RocksDBExports";
    private final ExportAsyncAPI backend;
    private final int responseLimit;
    private final int encodedDtoLimit;
    // Binary reply header (version, name length, sequence) plus success field and result STOP.
    // getExportCapabilities is the longest current method name.
    private static final int MAX_REPLY_ENVELOPE_BYTES=12+"getExportCapabilities".length()+4;
    public ExportThriftService(ExportAsyncAPI backend,int maxFrameSize) {
        this.backend=backend;
        responseLimit=ThriftTransportLimits.safeCdcResponseSize(ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize));
        encodedDtoLimit=maxFrameSize-MAX_REPLY_ENVELOPE_BYTES;
    }
    private <T,R> R invoke(it.cavallium.rockserver.core.common.api.RequestContext wire,Supplier<ExportCommand<T>> command,Function<T,R> mapper) throws org.apache.thrift.TException {
        if(backend==null) throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.UNKNOWN_METHOD,"Export route unavailable");
        try {
            if(wire==null || wire.profile==null) throw new IllegalArgumentException("Request context required");
            var context=new RequestContext(WorkloadProfile.fromWireValue(wire.profile.getValue()),wire.timeoutNanos);
            long start=System.nanoTime();
            var request=command.get();
            if(request instanceof ExportCommand.Manifest manifest && manifest.request().maximumBytes()>getExportCapabilities().maximumManifestBytes
                    || request instanceof ExportCommand.Read read && read.request().maximumBytes()>getExportCapabilities().maximumFilePageBytes
                    || request instanceof ExportCommand.Witness witness && witness.request().maximumBytes()>responseLimit)
                throw new IllegalArgumentException("Export request exceeds configured response budget");
            long admitted=context.hasTimeout()?context.timeoutNanos()-(System.nanoTime()-start):RequestContext.NO_TIMEOUT;
            if(admitted<=0) throw new java.util.concurrent.TimeoutException();
            var source=request.handleAsync(backend,new RequestContext(context.profile(),admitted));
            T result;
            if(context.hasTimeout()) {
                long remaining=context.timeoutNanos()-(System.nanoTime()-start);
                if(remaining<=0) throw new java.util.concurrent.TimeoutException();
                result=source.get(remaining,TimeUnit.NANOSECONDS);
            } else result=source.get();
            var mapped=mapper.apply(result);
            if(mapped instanceof org.apache.thrift.TBase<?,?> dto) {
                // The configured transport cap is authoritative; count before allocating a response frame.
                var counter=new org.apache.thrift.transport.TMemoryBuffer(0) {
                    private long count;
                    @Override public void write(byte[] bytes,int offset,int length) {
                        count+=length;
                        if(count>encodedDtoLimit) throw new IllegalArgumentException("Export response exceeds frame budget");
                    }
                };
                dto.write(new org.apache.thrift.protocol.TBinaryProtocol(counter));
            }
            if(context.hasTimeout() && context.timeoutNanos()-(System.nanoTime()-start)<=0)
                throw new java.util.concurrent.TimeoutException();
            return mapped;
        } catch(Exception error) {
            while(error instanceof java.util.concurrent.ExecutionException && error.getCause() instanceof Exception cause) error=cause;
            if(error instanceof InterruptedException) Thread.currentThread().interrupt();
            var type=error instanceof it.cavallium.rockserver.core.common.RocksDBException rocks ? RocksDBErrorType.valueOf(rocks.getErrorUniqueId().name())
                : error instanceof java.util.concurrent.RejectedExecutionException ? RocksDBErrorType.SERVER_OVERLOADED
                : error instanceof java.util.concurrent.TimeoutException ? RocksDBErrorType.READ_DEADLINE_EXCEEDED
                : error instanceof IllegalArgumentException || error instanceof NullPointerException ? RocksDBErrorType.PUT_INVALID_REQUEST : RocksDBErrorType.INTERNAL_ERROR;
            throw new RocksDBThriftException(type,error.getMessage()==null?error.getClass().getSimpleName():error.getMessage());
        }
    }
    @Override public ExportCapabilities getExportCapabilities() { return new ExportCapabilities(backend!=null,RawScanMode.IMMUTABLE_EXPORT_JOBS,ExportJobs.MAX_COLUMNS,ExportJobs.MAX_CHUNKS,ExportJobs.MAX_MANIFEST_ITEMS,Math.min(ExportJobs.MAX_MANIFEST_BYTES,responseLimit),Math.min(ExportJobs.MAX_FILE_PAGE_BYTES,responseLimit)); }
    @Override public ExportStatus createExport(ExportCreate request,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException { return invoke(c,()->new ExportCommand.Create(ThriftExportWire.create(request)),ThriftExportWire::status); }
    @Override public ExportStatus recoverExport(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException { return invoke(c,()->new ExportCommand.Recover(ThriftExportWire.uuid(id)),v->ThriftExportWire.status(v,ThriftExportWire.uuid(id))); }
    @Override public ExportStatus stepExport(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException { return invoke(c,()->new ExportCommand.Step(ThriftExportWire.uuid(id)),v->ThriftExportWire.status(v,ThriftExportWire.uuid(id))); }
    @Override public ExportStatus exportStatus(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException { return invoke(c,()->new ExportCommand.Status(ThriftExportWire.uuid(id)),v->ThriftExportWire.status(v,ThriftExportWire.uuid(id))); }
    @Override public ExportManifestPage exportManifest(ExportManifestRequest r,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException { return invoke(c,()->new ExportCommand.Manifest(ThriftExportWire.manifestRequest(r)),v->ThriftExportWire.manifest(v,ThriftExportWire.manifestRequest(r))); }
    @Override public ExportWitnessPage exportWitness(ExportWitnessRequest r,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException {
        return invoke(c,()->new ExportCommand.Witness(ThriftExportWire.witnessRequest(r)),v->ThriftExportWire.witness(v,ThriftExportWire.witnessRequest(r)));
    }
    @Override public ExportFilePage readExport(ExportReadRequest r,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException { return invoke(c,()->new ExportCommand.Read(ThriftExportWire.readRequest(r)),v->ThriftExportWire.file(v,ThriftExportWire.readRequest(r))); }
    @Override public void acknowledgeExport(ExportChunkId id,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException { invoke(c,()->new ExportCommand.Acknowledge(ThriftExportWire.id(id)),v->null); }
    @Override public void releaseExport(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException { invoke(c,()->new ExportCommand.Release(ThriftExportWire.uuid(id)),v->null); }
    @Override public void cancelExport(ByteBuffer id,it.cavallium.rockserver.core.common.api.RequestContext c) throws org.apache.thrift.TException { invoke(c,()->new ExportCommand.Cancel(ThriftExportWire.uuid(id)),v->null); }
}
