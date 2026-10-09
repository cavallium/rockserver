package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.api.*;
import it.cavallium.rockserver.core.common.export.*;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.apache.thrift.TException;

/** Export-only client. The caller owns the executor and any injected in-memory service. */
public final class ThriftExportClient implements ExportAsyncAPI {
    private final Executor executor;
    private final RocksDBExports.Iface borrowed;
    private final String host;
    private final int port;
    private final int maxFrameSize;
    public ThriftExportClient(RocksDBExports.Iface service,Executor executor) {
        borrowed=Objects.requireNonNull(service); this.executor=Objects.requireNonNull(executor);
        host=null; port=0; maxFrameSize=ThriftTransportLimits.configuredClientLimits().maxFrameSize();
    }
    public ThriftExportClient(String host,int port,Executor executor) {
        this(host,port,executor,ThriftTransportLimits.configuredClientLimits().maxFrameSize());
    }
    public ThriftExportClient(String host,int port,Executor executor,int maxFrameSize) {
        this.host=Objects.requireNonNull(host); this.port=port; this.executor=Objects.requireNonNull(executor);
        this.maxFrameSize=ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize); borrowed=null;
    }
    @FunctionalInterface private interface Call<W> { W call(RocksDBExports.Iface service,it.cavallium.rockserver.core.common.api.RequestContext context) throws TException; }
    private <W,T> CompletableFuture<T> invoke(RequestContext context,Call<W> call,Function<W,T> mapper) {
        var result=new CompletableFuture<T>() {
            @Override public boolean completeExceptionally(Throwable error) {
                return super.completeExceptionally(clientError(error));
            }
        };
        try {
            var bound=BoundRequestContext.bind(context);
            if(context.hasTimeout()) result.orTimeout(bound.remainingNanos(),java.util.concurrent.TimeUnit.NANOSECONDS);
            executor.execute(()->{
                if(result.isDone()) return;
                try {
                    W value;
                    if(borrowed!=null) {
                        synchronized(borrowed) {
                            if(result.isDone()) return;
                            value=call.call(borrowed,wire(bound));
                        }
                    } else {
                        long remaining=bound.remainingNanos();
                        if(remaining<=0) throw new java.util.concurrent.TimeoutException();
                        var config=org.apache.thrift.TConfiguration.custom().setMaxFrameSize(maxFrameSize).setMaxMessageSize(maxFrameSize).build();
                        int timeout=remaining==RequestContext.NO_TIMEOUT?0:(int)Math.min(Integer.MAX_VALUE,Math.max(1,remaining/1000000+(remaining%1000000==0?0:1)));
                        var socket=new org.apache.thrift.transport.TSocket(config,host,port,timeout);
                        try(var transport=new org.apache.thrift.transport.layered.TFramedTransport(socket,maxFrameSize)) {
                            transport.open();
                            long callRemaining=bound.remainingNanos();
                            if(callRemaining<=0) throw new java.util.concurrent.TimeoutException();
                            if(callRemaining!=RequestContext.NO_TIMEOUT) socket.setSocketTimeout((int)Math.min(Integer.MAX_VALUE,
                                    Math.max(1,callRemaining/1000000+(callRemaining%1000000==0?0:1))));
                            var protocol=new org.apache.thrift.protocol.TMultiplexedProtocol(new org.apache.thrift.protocol.TBinaryProtocol(transport),"RocksDBExports");
                            value=call.call(new RocksDBExports.Client(protocol),wire(bound));
                        }
                    }
                    result.complete(mapper.apply(value));
                } catch(Throwable error) { result.completeExceptionally(clientError(error)); }
            });
        } catch(Throwable error) { result.completeExceptionally(clientError(error)); }
        // Cancellation and timeout affect this observation only, never issue cancelExport.
        return result;
    }
    private static Throwable clientError(Throwable error) {
        if(error instanceof RocksDBThriftException rocks) return it.cavallium.rockserver.core.common.RocksDBException.of(
                it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.valueOf(rocks.errorType.name()),rocks.message);
        if(error instanceof java.util.concurrent.TimeoutException) return it.cavallium.rockserver.core.common.RocksDBException.of(
                it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Export deadline exceeded");
        return error;
    }
    private static it.cavallium.rockserver.core.common.api.RequestContext wire(BoundRequestContext bound) throws TException {
        long remaining=bound.remainingNanos();
        if(remaining<=0) throw it.cavallium.rockserver.core.common.RocksDBException.of(
                it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Export deadline exceeded");
        return new it.cavallium.rockserver.core.common.api.RequestContext(WorkloadProfile.findByValue(bound.value().profile().wireValue()),remaining);
    }
    private <T> T requirePage(T request,int maximumBytes) {
        if(maximumBytes>ThriftTransportLimits.safeCdcResponseSize(maxFrameSize))
            throw new IllegalArgumentException("Export request exceeds client response budget");
        return request;
    }
    @Override public CompletableFuture<ExportJobs.Status> createExport(ExportJobs.Create r,RequestContext c) { return invoke(c,(s,w)->s.createExport(ThriftExportWire.create(r),w),ThriftExportWire::status); }
    @Override public CompletableFuture<ExportJobs.Status> recoverExport(UUID id,RequestContext c) { return invoke(c,(s,w)->s.recoverExport(ThriftExportWire.uuid(id),w),v->ThriftExportWire.status(v,id)); }
    @Override public CompletableFuture<ExportJobs.Status> stepExport(UUID id,RequestContext c) { return invoke(c,(s,w)->s.stepExport(ThriftExportWire.uuid(id),w),v->ThriftExportWire.status(v,id)); }
    @Override public CompletableFuture<ExportJobs.Status> exportStatus(UUID id,RequestContext c) { return invoke(c,(s,w)->s.exportStatus(ThriftExportWire.uuid(id),w),v->ThriftExportWire.status(v,id)); }
    @Override public CompletableFuture<ExportJobs.ManifestPage> exportManifest(ExportJobs.ManifestRequest r,RequestContext c) { return invoke(c,(s,w)->s.exportManifest(ThriftExportWire.manifestRequest(requirePage(r,r.maximumBytes())),w),v->ThriftExportWire.manifest(v,r)); }
    @Override public CompletableFuture<ExportJobs.FilePage> readExport(ExportJobs.ReadRequest r,RequestContext c) { return invoke(c,(s,w)->s.readExport(ThriftExportWire.readRequest(requirePage(r,r.maximumBytes())),w),v->ThriftExportWire.file(v,r)); }
    @Override public CompletableFuture<Void> acknowledgeExport(ExportJobs.ChunkId id,RequestContext c) { return invoke(c,(s,w)->{s.acknowledgeExport(ThriftExportWire.id(id),w);return null;},v->null); }
    @Override public CompletableFuture<Void> releaseExport(UUID id,RequestContext c) { return invoke(c,(s,w)->{s.releaseExport(ThriftExportWire.uuid(id),w);return null;},v->null); }
    @Override public CompletableFuture<Void> cancelExport(UUID id,RequestContext c) { return invoke(c,(s,w)->{s.cancelExport(ThriftExportWire.uuid(id),w);return null;},v->null); }
    public record Capabilities(boolean available,
            it.cavallium.rockserver.core.common.RockserverCapabilities.RawScanMode rawScanMode,
            int maximumColumns,int maximumChunks,int maximumManifestItems,
            int maximumManifestBytes,int maximumFilePageBytes) {}
    public CompletableFuture<Capabilities> capabilities(RequestContext c) {
        return invoke(c,(s,w)->s.getExportCapabilities(),v->new Capabilities(v.available,
                it.cavallium.rockserver.core.common.RockserverCapabilities.RawScanMode.valueOf(v.rawScanMode.name()),
                v.maximumColumns,v.maximumChunks,v.maximumManifestItems,v.maximumManifestBytes,v.maximumFilePageBytes));
    }
}
