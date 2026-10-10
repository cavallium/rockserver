package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.*;
import java.io.*;
import java.net.URI;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongSupplier;
import org.apache.ratis.client.RaftClient;
import org.reactivestreams.Publisher;
import reactor.core.publisher.*;
import static it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.*;

/** Concrete common connection over proof-qualified generations and resumable durable schema jobs. */
public final class RatisRocksDBConnection implements RocksDBConnection {
    public record Limits(int generations,int schemaRequests,int queuedSchema,long schemaBytes,int queuedData) {
        public Limits { if (generations<1 || generations>16 || schemaRequests<1 || schemaRequests>128 || queuedSchema<1 || queuedSchema>8 || schemaBytes<16*1024*1024 || schemaBytes>256L*1024*1024 || queuedData<1 || queuedData>64) throw new IllegalArgumentException("Invalid connection limits"); }
        /** Each durable schema request additionally has the SDK's fixed 32MiB journal bound. */
        public static Limits defaults() { return new Limits(8,64,8,128L*1024*1024,32); }
    }
    @FunctionalInterface public interface GenerationFactory { Generation create(DistributedSchema schema,RangeDirectory directory,Path generationRoot,LongSupplier ids) throws IOException; }
    /** Return a generation-owned durable session with an exclusive installed grant; never reuse another sequence owner's grant. */
    @FunctionalInterface public interface SessionFactory { RatisDurableSessionClient open(Path generationRoot,DistributedSchema schema,RangeDirectory directory) throws IOException; }
    public static final class Generation implements AutoCloseable {
        private final RatisRocksDBAdapter adapter; private final RatisRocksDBDirect direct; private final RatisRocksDBBatches batches; private final RatisRocksDBRanges ranges; private final RatisTransactionClient transactions; private final RatisSnapshotClient snapshots; private final List<AutoCloseable> owned;
        private int closed;
        public Generation(RatisRocksDBAdapter adapter,RatisRocksDBDirect direct,RatisRocksDBRanges ranges,RatisTransactionClient transactions,RatisSnapshotClient snapshots,List<AutoCloseable> owned) {
            this(adapter,direct,ranges,transactions,snapshots,owned,null);
        }
        public Generation(RatisRocksDBAdapter adapter,RatisRocksDBDirect direct,RatisRocksDBRanges ranges,RatisTransactionClient transactions,RatisSnapshotClient snapshots,List<AutoCloseable> owned,RatisRocksDBBatches batches) {
            this.batches=batches; this.adapter=Objects.requireNonNull(adapter); this.direct=direct; this.ranges=ranges; this.transactions=transactions; this.snapshots=snapshots; this.owned=List.copyOf(owned);
        }
        public RatisRocksDBAdapter adapter() { return adapter; }
        public RatisRocksDBDirect direct() { return direct; }
        public RatisRocksDBBatches batches() { return batches; }
        @Override public synchronized void close() throws IOException {
            var resources=new ArrayList<AutoCloseable>(); if(batches!=null) resources.add(batches); if (direct!=null) resources.add(direct); resources.add(adapter); if (ranges!=null) resources.add(ranges); if (transactions!=null) resources.add(transactions); if (snapshots!=null) resources.add(snapshots); resources.addAll(owned);
            while (closed<resources.size()) { try { resources.get(closed).close(); closed++; } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException("Generation resource close failed",failure); } }
        }
    }
    public record DirectRecovery(byte[] schemaDigest,long schemaEpoch,RatisRocksDBDirect.RecoveryState state) {
        public DirectRecovery { schemaDigest=schemaDigest.clone(); }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
    }
    /** Bounds apply to one copied KVBatch's complete encoded group commands; chunks are independently atomic. */
    public record BatchLimits(int maximumInputItems,int maximumEncodedInputBytes,int maximumChunkItems,int maximumEncodedChunkBytes) {}
    public record BatchRecovery(byte[] schemaDigest,long schemaEpoch,RatisRocksDBBatches.RecoveryState state,RatisRocksDBBatches.Progress progress) {
        public BatchRecovery { schemaDigest=schemaDigest.clone(); }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
    }
    public BatchLimits batchLimits() { return new BatchLimits(RatisRocksDBBatches.MAX_INPUT_ITEMS,RatisRocksDBBatches.MAX_INPUT_BYTES,RatisRocksDBBatches.MAX_CHUNK_ITEMS,RatisRocksDBBatches.MAX_CHUNK_BYTES); }
    public static GenerationFactory durableGenerationFactory(RatisRoutedClient routed,RWScheduler scheduler,RatisSnapshotFences snapshotHomes,RatisTransactionStatuses transactionHomes,RatisRocksDBAdapter.AllocationSource allocations,SessionFactory sessions) {
        return (schema,directory,root,ids) -> {
            RatisDurableSessionClient session=null,bulkSession=null; RatisRocksDBBatches batches=null; RatisTransactionClient tx=null; RatisSnapshotClient snapshots=null; RatisRocksDBRanges ranges=null; RatisRocksDBDirect direct=null;
            try {
                session=sessions.open(Files.createDirectories(root.resolve("point-session")),schema,directory); bulkSession=sessions.open(Files.createDirectories(root.resolve("bulk-session")),schema,directory);
                if(session==bulkSession || session.grant().sessionId().equals(bulkSession.grant().sessionId()) && session.grant().epoch()==bulkSession.grant().epoch()) throw new IOException("Point and bulk sessions require distinct exclusively owned grants");
                batches=new RatisRocksDBBatches(schema,routed,bulkSession,scheduler,root.resolve("bulk"),RatisRocksDBBatches.SessionOwnership.OWNED); bulkSession=null;
                tx=new RatisTransactionClient(schema,snapshotHomes,transactionHomes,routed,scheduler,RatisTransactionClient.Limits.defaults()); snapshots=new RatisSnapshotClient(schema,snapshotHomes,routed,scheduler,RatisSnapshotClient.Limits.defaults()); ranges=new RatisRocksDBRanges(schema,directory,snapshots,allocations); direct=new RatisRocksDBDirect(schema,routed,session,scheduler,root.resolve("direct"));
                return new Generation(new RatisRocksDBAdapter(schema,directory,tx,allocations,direct,ranges,ids,batches),direct,ranges,tx,snapshots,List.of(session),batches);
            } catch (Throwable failure) { for (var resource:new AutoCloseable[]{batches,direct,ranges,snapshots,tx,bulkSession==session ? null : bulkSession,session}) if (resource!=null) try { resource.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); } if (failure instanceof IOException io) throw io; throw new IOException("Generation creation failed",failure); }
        };
    }
    private final URI uri; private final Path root; private final RWScheduler clock; private final RatisRoutedClient routed; private final Limits limits;
    private final FileChannel ownership; private final FileLock lock; private final RatisSchemaJobBridge schemas; private final RatisCdcBridge cdc; private final RatisGenerationRegistry generations;
    private final ExecutorService dataLane; private final List<AutoCloseable> ownedDependencies; private final RatisOperatorCatalogue operators;
    private volatile Thread dataWorker;
    private final java.util.concurrent.locks.ReentrantLock closeOwner=new java.util.concurrent.locks.ReentrantLock();
    private boolean stopping,closed; private int dataOwners,dependencyClosed; private long nextId,idLimit;
    /** All supplied transports/schedulers/allocators are borrowed unless listed in ownedDependencies. Factory generations are owned. */
    public RatisRocksDBConnection(URI uri,Path root,DistributedSchema initial,RatisRoutedClient routed,RWScheduler scheduler,RatisNodePlan.Catalogue binding,RaftClient catalogue,RatisSchemaReplicaClient replicas,RatisOperatorCatalogue operators,RatisSchemaClient.TrustedOperatorResolver resolver,GenerationFactory factory,Limits limits,List<AutoCloseable> ownedDependencies) throws IOException {
        this.uri=Objects.requireNonNull(uri); this.root=root.toAbsolutePath().normalize(); this.routed=Objects.requireNonNull(routed); clock=Objects.requireNonNull(scheduler); this.limits=Objects.requireNonNull(limits); this.ownedDependencies=List.copyOf(ownedDependencies); this.operators=operators;
        Files.createDirectories(this.root); boolean used=Files.exists(this.root.resolve("connection.lock"),LinkOption.NOFOLLOW_LINKS); ownership=FileChannel.open(this.root.resolve("connection.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS); FileLock acquired=null; RatisSchemaJobBridge bridge=null; RatisGenerationRegistry registry=null; RatisCdcBridge cdcBridge=null;
        try {
            try { acquired=ownership.tryLock(); } catch (OverlappingFileLockException conflict) { throw new IOException("Connection root already owned",conflict); } if (acquired==null) throw new IOException("Connection root already owned"); lock=acquired;
            var ids=this.root.resolve("handles"); if (used && !Files.exists(ids,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Original connection handle state is missing"); if (Files.exists(ids,LinkOption.NOFOLLOW_LINKS)) { byte[] value=SessionJournalFiles.read(ids,64); if (value.length!=8) throw new IOException("Invalid handle reservation"); nextId=idLimit=ByteBuffer.wrap(value).getLong(); if (nextId<1) throw new IOException("Invalid handle high-water mark"); } else { nextId=idLimit=1; SessionJournalFiles.write(ids,ByteBuffer.allocate(8).putLong(1).array(),64); }
            var trusted=resolver==null ? (RatisSchemaClient.TrustedOperatorResolver)(name,className,sha,revision) -> { throw new IOException("Trusted operator resolver unavailable"); } : resolver;
            Path sdk=Files.createDirectories(this.root.resolve("schema")); var capacity=new RatisGenerationRegistry.Capacity(this.root.resolve("generation-capacity"),Files.createDirectories(this.root.resolve("generations")),sdk,limits.generations(),used); bridge=new RatisSchemaJobBridge(Files.createDirectories(this.root.resolve("requests")),sdk,clock,checkpoint -> new RatisSchemaClient(sdk,initial.clusterId(),initial.databaseId(),initial.domainId(),binding,catalogue,routed,replicas,operators,(path,fields) -> capacity.checkpoint(path,fields,checkpoint)),trusted,routed,limits.schemaRequests(),limits.queuedSchema(),limits.schemaBytes());
            registry=new RatisGenerationRegistry(Files.createDirectories(this.root.resolve("generations")),routed,new RatisSchemaCatalogue(catalogue,initial.clusterId(),initial.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()),factory,this::nextHandle,limits.generations(),initial,capacity);
            var directories=new RatisCatalogue(catalogue,initial.clusterId(),initial.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
            cdcBridge=new RatisCdcBridge(new RatisCdcSubscriptionController(Files.createDirectories(this.root.resolve("cdc")),initial.clusterId(),initial.databaseId(),binding,catalogue,routed),clock,directories::read);
            schemas=bridge; generations=registry; cdc=cdcBridge;
        } catch (Throwable failure) { if(cdcBridge!=null) try { cdcBridge.close(); } catch(Exception cleanup) { failure.addSuppressed(cleanup); } if (registry!=null) try { registry.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); } if (bridge!=null) try { bridge.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); } if (acquired!=null) acquired.release(); ownership.close(); if (failure instanceof IOException io) throw io; throw new IOException("Connection open failed",failure); }
        dataLane=Executors.newSingleThreadExecutor(task -> Thread.ofPlatform().daemon(true).name("ratis-connection-dispatch").unstarted(() -> { dataWorker=Thread.currentThread(); task.run(); }));
    }
    private synchronized long nextHandle() {
        if (nextId==idLimit) {
            if (idLimit>Long.MAX_VALUE-64) throw new IllegalStateException("Connection handle IDs exhausted"); long reserved=idLimit+64;
            try { SessionJournalFiles.write(root.resolve("handles"),ByteBuffer.allocate(8).putLong(reserved).array(),64); } catch (IOException failure) { stopping=true; throw new UncheckedIOException(failure); } idLimit=reserved;
        }
        return nextId++;
    }
    @Override public it.cavallium.rockserver.core.common.cdc.DistributedCdcAsyncAPI getDistributedCdcApi() { requireOpen(); return cdc; }
    @Override public URI getUrl() { return uri; }
    @Override public RockserverCapabilities getCapabilities() { return new RockserverCapabilities(RockserverCapabilities.BackendMode.DISTRIBUTED,RockserverCapabilities.RawScanMode.UNSUPPORTED,RangeBudget.DEFAULT_MAX_ITEMS,RangeBudget.DEFAULT_MAX_BYTES,RockserverCapabilities.CdcMode.VECTOR_SUBSCRIPTIONS); }
    public List<RatisSchemaRequests.Status> pendingSchemaRequests() { return schemas.pending(); }
    public RatisSchemaRequests.Status schemaRequestStatus(UUID id) { return schemas.status(id); }
    public CompletableFuture<RatisSchemaRequests.Result> resumeSchemaRequest(UUID id,RequestContext context) { requireOpen(); return schemas.resume(id,context,false); }
    public CompletableFuture<RatisSchemaRequests.Status> abortSchemaRequest(UUID id,RequestContext context) { requireOpen(); return schemas.resume(id,context,true).thenApply(result -> schemas.status(id)); }
    @Override public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
        Objects.requireNonNull(context); return new RocksDBAsyncAPI() {
            @SuppressWarnings("unchecked") public <R,RS,RA> RA requestAsync(RocksDBAPICommand<R,RS,RA> request) {
                WorkloadAdmission.resolve(context,request);
                if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream<?>) return (RA)Flux.defer(() -> {
                    final StreamOwner owner; try { owner=new StreamOwner(); } catch (Throwable failure) { return Flux.error(failure); }
                    return Mono.fromFuture(dispatchData(request,context,owner)).flatMapMany(value -> Flux.from((Publisher<?>)value)).doFinally(signal -> owner.terminated());
                });
                return (RA)dispatch(request,context,false);
            }
            public Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch> cdcPollBatchAsync(String id,Long fromSeq,long maxEvents) { return Mono.error(unsupported()); }
        };
    }
    @Override public RocksDBSyncAPI getSyncApi(RequestContext context) {
        var async=getAsyncApi(context); return new RocksDBSyncAPI() {
            @SuppressWarnings("unchecked") public <R,RS,RA> RS requestSync(RocksDBAPICommand<R,RS,RA> request) {
                if (Thread.currentThread()==dataWorker || schemas.isWorkerThread()) throw RocksDBException.of(RocksDBException.RocksDBErrorType.CONFIG_ERROR,"Blocking sync calls cannot wait from a connection-owned lane");
                try { if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream<?>) return (RS)Flux.from((Publisher<?>)async.requestAsync(request)).toStream(); return (RS)((CompletableFuture<?>)async.requestAsync(request)).join(); }
                catch (CompletionException failure) { var cause=unwrap(failure); if (cause instanceof RuntimeException runtime) throw runtime; throw RocksDBException.of(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,cause); }
            }
        };
    }
    private synchronized void requireOpen() { if (closed || stopping) throw RocksDBException.of(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,"Connection is closing/closed"); }
    private long handle(RocksDBAPICommand<?,?,?> request) {
        return switch (request) {
            case CloseTransaction r -> r.transactionId(); case CloseFailedUpdate r -> r.updateId(); case Put<?> r -> r.transactionOrUpdateId(); case PutMulti<?> r -> r.transactionOrUpdateId(); case Delete<?> r -> r.transactionOrUpdateId(); case DeleteMulti<?> r -> r.transactionOrUpdateId(); case Merge<?> r -> r.transactionOrUpdateId(); case MergeMulti<?> r -> r.transactionOrUpdateId(); case Get<?> r -> r.transactionOrUpdateId(); case ExistsMulti r -> r.transactionId(); case OpenIterator r -> r.transactionId(); case GetRangePage<?> r -> r.transactionId(); case ReduceRange<?> r -> r.transactionId(); case RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> r -> r.transactionId(); default -> 0;
        };
    }
    private long iterator(RocksDBAPICommand<?,?,?> request) { return switch (request) { case CloseIterator r -> r.iteratorId(); case SeekTo r -> r.iterationId(); case Subsequent<?> r -> r.iterationId(); default -> 0; }; }
    private CompletableFuture<Object> dispatch(RocksDBAPICommand<?,?,?> original,RequestContext context,boolean stream) {
        try { requireOpen(); } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
        if (original instanceof CreateColumn r) { var shape=copy(r.schema()); return schemaObserver(schemas.submit(RatisSchemaRequests.Kind.CREATE,65536,context,(client,id) -> {
            OperatorUpload upload=null; var actual=shape;
            if (shape.mergeOperatorName()!=null) { if (operators==null || shape.mergeOperatorVersion()==null) throw new IOException("Published operator required"); upload=operators.readPublished(shape.mergeOperatorName(),shape.mergeOperatorVersion()); if (upload==null || shape.mergeOperatorClass()!=null && !shape.mergeOperatorClass().equals(upload.operator().className())) throw new IOException("Exact published operator missing/changed"); actual=new ColumnSchema(shape.keys(),shape.variableTailKeys(),shape.hasValue(),shape.mergeOperatorName(),shape.mergeOperatorVersion(),upload.operator().className()); }
            return client.prepareCreate(id,r.name(),actual,upload);
        }),result -> result.value()); }
        if (original instanceof DeleteColumn r) return schemaObserver(schemas.submit(RatisSchemaRequests.Kind.DELETE,65536,context,(client,id) -> client.prepareDelete(id,r.columnId())),result -> null);
        if (original instanceof DeleteColumnIfExists r) return schemaObserver(schemas.submit(RatisSchemaRequests.Kind.DELETE_IF_EXISTS,65536,context,(client,id) -> client.prepareDeleteIfExists(id,r.name())),result -> result.changed());
        if (original instanceof UploadMergeOperator r) { if (r.jarData().length>OperatorUpload.MAX_ARTIFACT_BYTES) return CompletableFuture.failedFuture(new IllegalArgumentException("Operator artifact exceeds durable upload bound")); byte[] jar=r.jarData().clone(); return schemaObserver(schemas.submit(RatisSchemaRequests.Kind.UPLOAD,jar.length+65536L,context,(client,id) -> client.prepareUpload(id,r.name(),r.className(),jar,schemasResolver())),result -> result.value()); }
        if (original instanceof CheckMergeOperator r) { byte[] hash=r.hash().clone(); if (hash.length!=32) return CompletableFuture.failedFuture(new IllegalArgumentException("Operator SHA must contain32bytes")); return (CompletableFuture<Object>)(CompletableFuture<?>)schemas.check(r.name(),hash,context); }
        return dispatchData(original,context,null);
    }
    private synchronized void acquireData() { requireOpen(); if (dataOwners>=limits.queuedData()) throw RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,"Connection dispatch capacity exhausted"); dataOwners++; }
    private final class StreamOwner {
        boolean selected,terminated,refunded;
        StreamOwner() { acquireData(); }
        void selected() { synchronized (RatisRocksDBConnection.this) { selected=true; refund(); } }
        void terminated() { synchronized (RatisRocksDBConnection.this) { terminated=true; refund(); } }
        private void refund() { if (selected && terminated && !refunded) { refunded=true; dataOwners--; } }
    }
    private static final class DataObserver extends CompletableFuture<Object> {
        final java.util.concurrent.atomic.AtomicInteger delivery=new java.util.concurrent.atomic.AtomicInteger();
        final CompletableFuture<Boolean> delivered=new CompletableFuture<>();
        @Override public boolean cancel(boolean interrupt) { if (!delivery.compareAndSet(0,1)) return false; boolean cancelled=super.cancel(false); delivered.complete(false); return cancelled; }
        boolean claimDelivery() { return delivery.compareAndSet(0,2); }
    }
    private CompletableFuture<Object> dispatchData(RocksDBAPICommand<?,?,?> original,RequestContext context,StreamOwner streamOwner) {
        final long deadline=clock.resolveMonotonicDeadline(context); final DataObserver view=new DataObserver();
        final java.util.concurrent.atomic.AtomicReference<Runnable> cancelDelegate=new java.util.concurrent.atomic.AtomicReference<>();
        if (streamOwner==null) { try { acquireData(); } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); } }
        view.whenComplete((value,failure) -> { if (view.isCancelled()) { var cancel=cancelDelegate.get(); if (cancel!=null) cancel.run(); } });
        Runnable dispatch=() -> {
            CompletableFuture<?> actual; CompletableFuture<Void> drained=CompletableFuture.completedFuture(null);
            final java.util.concurrent.atomic.AtomicReference<RatisGenerationRegistry.Owned> selected=new java.util.concurrent.atomic.AtomicReference<>();
            try {
                remaining(context,deadline); if (view.isCancelled()) throw new CancellationException("Connection observer cancelled before DATA dispatch");
                {
                    long iterator=iterator(original),handle=handle(original); var owner=iterator!=0 ? generations.iterator(iterator) : handle!=0 ? generations.transaction(handle) : generations.current(); selected.set(owner);
                    if (handle==0 && iterator==0 && !original.isReadOnly()) for (var generation:generations.all()) {
                        if(generation.generation().direct()!=null && generation.generation().direct().recoveryState()!=RatisRocksDBDirect.RecoveryState.NONE) throw new IOException("Resolve the original generation's retained direct request before fresh mutation");
                        if(generation.generation().batches()!=null && generation.generation().batches().recoveryState()!=RatisRocksDBBatches.RecoveryState.NONE) throw new IOException("Resolve the original generation's retained batch request before fresh mutation");
                    }
                    var dispatched=owner.generation().adapter().dispatchOwned(original,remaining(context,deadline)); cancelDelegate.set(dispatched.cancelObserver()); if (view.isCancelled()) dispatched.cancelObserver().run();
                    Object delegated=dispatched.observer(); drained=dispatched.drained(); actual=streamOwner!=null ? CompletableFuture.completedFuture(delegated) : (CompletableFuture<?>)delegated;
                }
            } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            record Acquired(Object value,Throwable failure) {}
            var cleanup=actual.handle((value,failure) -> new Acquired(value,failure)).thenCombine(view.delivered,(acquired,delivered) -> {
                var value=acquired.value(); var failure=acquired.failure();
                if (delivered || failure!=null || selected.get()==null) return CompletableFuture.<Void>completedFuture(null);
                RocksDBAPICommand<?,?,?> release=null;
                if (original instanceof OpenTransaction && value instanceof Long id) release=new CloseTransaction(id,false);
                else if (original instanceof OpenIterator && value instanceof Long id) release=new CloseIterator(id);
                else if (original instanceof Get<?> && value instanceof UpdateContext<?> update) release=new CloseFailedUpdate(update.updateId());
                if (release==null) return CompletableFuture.<Void>completedFuture(null);
                var closing=selected.get().generation().adapter().dispatchOwned(release,RequestContext.batch()); return closing.drained();
            }).thenCompose(value -> value);
            CompletableFuture.allOf(drained,cleanup).whenComplete((ignored,failure) -> { if (streamOwner!=null) streamOwner.selected(); else synchronized (RatisRocksDBConnection.this) { dataOwners--; } });
            actual.whenComplete((value,failure) -> { Runnable publish=() -> { if (view.claimDelivery()) { view.delivered.complete(true); if (failure==null) view.complete(value); else view.completeExceptionally(unwrap(failure)); } }; try { dataLane.execute(publish); } catch (RejectedExecutionException rejected) { publish.run(); } });
        };
        try { dataLane.execute(dispatch); } catch (RejectedExecutionException failure) { if (streamOwner!=null) streamOwner.selected(); else synchronized (this) { dataOwners--; } if (view.claimDelivery()) { view.delivered.complete(true); view.completeExceptionally(failure); } }
        return view;
    }
    private static <T> CompletableFuture<T> schemaObserver(CompletableFuture<RatisSchemaRequests.Result> original,java.util.function.Function<RatisSchemaRequests.Result,T> mapper) {
        var view=original.thenApply(mapper); view.whenComplete((value,failure) -> { if (view.isCancelled()) original.cancel(false); }); return view;
    }
    private RatisSchemaClient.TrustedOperatorResolver schemasResolver() { return schemas.resolver(); }
    private RequestContext remaining(RequestContext context,long deadline) { if (deadline==Long.MAX_VALUE) return context; long nanos=clock.remainingMonotonicDeadlineNanos(deadline); if (nanos<=0) throw RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Connection request deadline expired"); return new RequestContext(context.profile(),nanos); }
    public CompletableFuture<List<DirectRecovery>> directRecoveryStatus(RequestContext context) { return diagnostic(context,remaining -> { var states=new ArrayList<DirectRecovery>(); for (var generation:generations.all()) if (generation.generation().direct()!=null) states.add(new DirectRecovery(generation.schema().digest(),generation.schema().schemaEpoch(),generation.generation().direct().recoveryState())); return CompletableFuture.completedFuture(List.copyOf(states)); }); }
    public CompletableFuture<Object> resumeDirectRequest(byte[] schemaDigest,RequestContext context) { byte[] frozen=schemaDigest.clone(); return diagnostic(context,remaining -> { for (var generation:generations.all()) if (Arrays.equals(generation.schema().digest(),frozen) && generation.generation().direct()!=null) return generation.generation().direct().resumeRetained(remaining); return CompletableFuture.failedFuture(new IOException("Unknown original direct generation")); }); }
    public CompletableFuture<List<BatchRecovery>> batchRecoveryStatus(RequestContext context) {
        return diagnostic(context,remaining -> { var states=new ArrayList<BatchRecovery>(); for(var generation:generations.all()) if(generation.generation().batches()!=null) { var batches=generation.generation().batches(); states.add(new BatchRecovery(generation.schema().digest(),generation.schema().schemaEpoch(),batches.recoveryState(),batches.progress())); } return CompletableFuture.completedFuture(List.copyOf(states)); });
    }
    public CompletableFuture<RatisRocksDBBatches.Progress> resumeBatchRequest(byte[] schemaDigest,UUID requestId,RequestContext context) {
        byte[] frozen=Objects.requireNonNull(schemaDigest).clone(); Objects.requireNonNull(requestId);
        return diagnosticOwned(context,remaining -> {
            for(var generation:generations.all()) if(Arrays.equals(generation.schema().digest(),frozen) && generation.generation().batches()!=null) {
                var batches=generation.generation().batches(); var progress=batches.progress();
                if(progress==null || !requestId.equals(progress.requestId())) throw new IOException("Batch token differs from original retained request");
                var observer=batches.resumeRetained(remaining); return new OwnedDiagnostic<>(observer,batches.actualCompletion(observer),() -> observer.cancel(false));
            }
            throw new IOException("Unknown original batch generation");
        });
    }
    private record OwnedDiagnostic<T>(CompletableFuture<T> observer,CompletableFuture<Void> drained,Runnable cancelObserver) {}
    @FunctionalInterface private interface OwnedDiagnosticWork<T> { OwnedDiagnostic<T> run(RequestContext remaining) throws Exception; }
    private <T> CompletableFuture<T> diagnosticOwned(RequestContext context,OwnedDiagnosticWork<T> work) {
        final long deadline; try { Objects.requireNonNull(context); WorkloadAdmission.resolve(context,OperationFamily.METADATA); deadline=clock.resolveMonotonicDeadline(context); acquireData(); } catch(Throwable failure) { return CompletableFuture.failedFuture(failure); }
        var view=new CompletableFuture<T>(); var cancel=new java.util.concurrent.atomic.AtomicReference<Runnable>();
        view.whenComplete((value,failure) -> { if(view.isCancelled()) { var action=cancel.get(); if(action!=null) action.run(); } });
        Runnable dispatch=() -> {
            OwnedDiagnostic<T> owned;
            try { if(view.isCancelled()) throw new CancellationException("Diagnostic cancelled before dispatch"); owned=work.run(remaining(context,deadline)); }
            catch(Throwable failure) { owned=new OwnedDiagnostic<>(CompletableFuture.failedFuture(failure),CompletableFuture.completedFuture(null),() -> {}); }
            cancel.set(owned.cancelObserver()); if(view.isCancelled()) owned.cancelObserver().run();
            owned.drained().whenComplete((ignored,failure) -> { synchronized(RatisRocksDBConnection.this) { dataOwners--; } });
            owned.observer().whenComplete((value,failure) -> { Runnable publish=() -> { if(failure==null) view.complete(value); else view.completeExceptionally(unwrap(failure)); }; try { dataLane.execute(publish); } catch(RejectedExecutionException rejected) { publish.run(); } });
        };
        try { dataLane.execute(dispatch); } catch(RejectedExecutionException failure) { synchronized(this) { dataOwners--; } view.completeExceptionally(failure); } return view;
    }
    @FunctionalInterface private interface Diagnostic<T> { CompletableFuture<T> run(RequestContext remaining) throws Exception; }
    private <T> CompletableFuture<T> diagnostic(RequestContext context,Diagnostic<T> work) {
        final long deadline; try { Objects.requireNonNull(context); WorkloadAdmission.resolve(context,OperationFamily.METADATA); deadline=clock.resolveMonotonicDeadline(context); acquireData(); } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
        var result=new CompletableFuture<T>(); Runnable dispatch=() -> {
            CompletableFuture<T> actual; try { actual=work.run(remaining(context,deadline)); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            actual.whenComplete((value,failure) -> { Runnable publish=() -> { synchronized (RatisRocksDBConnection.this) { dataOwners--; } if (failure==null) result.complete(value); else result.completeExceptionally(unwrap(failure)); }; try { dataLane.execute(publish); } catch (RejectedExecutionException rejected) { publish.run(); } });
        };
        try { dataLane.execute(dispatch); } catch (RejectedExecutionException failure) { synchronized (this) { dataOwners--; } result.completeExceptionally(failure); } return result.copy();
    }
    private static ColumnSchema copy(ColumnSchema schema) { Objects.requireNonNull(schema); return new ColumnSchema(IntLists.unmodifiable(new IntArrayList(schema.keys())),ObjectLists.unmodifiable(new ObjectArrayList<>(schema.variableTailKeys())),schema.hasValue(),schema.mergeOperatorName(),schema.mergeOperatorVersion(),schema.mergeOperatorClass()); }
    private static RocksDBException unsupported() { return RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,"Distributed connection operation is not implemented"); }
    private static Throwable unwrap(Throwable failure) { while ((failure instanceof CompletionException || failure instanceof ExecutionException) && failure.getCause()!=null) failure=failure.getCause(); return failure; }
    @Override public void close() throws IOException {
        if (closeOwner.isHeldByCurrentThread()) throw new IOException("Connection close cannot recursively tear down an owned dependency");
        closeOwner.lock();
        try {
        synchronized (this) { if (closed) return; cdc.stopAdmission(); stopping=true; }
        // Retry only already stopped stream cleanup; actual owners remain charged through its drain.
        for(var generation:generations.all()) generation.generation().adapter().retryStoppedFlowsCleanup();
        synchronized(this) { if(dataOwners!=0) throw new IOException("Connection DATA/refresh work active; ownership retained"); }
        cdc.close(); schemas.close(); generations.close();
        while (dependencyClosed<ownedDependencies.size()) { try { ownedDependencies.get(dependencyClosed).close(); dependencyClosed++; } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException("Owned dependency close failed",failure); } }
        dataLane.shutdown(); lock.release(); ownership.close(); synchronized (this) { closed=true; }
        } finally { closeOwner.unlock(); }
    }
}
