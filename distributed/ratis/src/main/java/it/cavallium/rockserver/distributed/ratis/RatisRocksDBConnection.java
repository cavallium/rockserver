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
    @FunctionalInterface public interface GenerationFactory { Generation create(RatisTransactionBindings bindings,Path generationRoot,LongSupplier ids) throws IOException; }
    /** Trusted placement for a new named column; a null shape is an absent-name deletion lookup. */
    @FunctionalInterface public interface DomainPlacement { UUID domain(String name,ColumnSchema schema) throws IOException; }
    /** Return a generation-owned session with an exclusive installed grant. */
    @FunctionalInterface public interface SessionOpening {RatisDurableSessionClient open(Path root,DistributedSchema schema,RatisSessionBindingPlan plan)throws IOException;}
    public record SessionFactory(List<RatisRoutedClient> routes,SessionOpening opener) {
        public SessionFactory{routes=List.copyOf(routes);Objects.requireNonNull(opener);if(routes.isEmpty() || routes.size()>RatisSessionBindingPlan.MAX_NAMESPACES)throw new IllegalArgumentException("Invalid owner binding route count");}
        RatisSessionBindingPlan.Input allBindings(RangeDirectory directory){return RatisSessionBindingPlan.all(directory,routes);}
        RatisSessionBindingPlan.Input bindings(List<RatisSessionBindingPlan.Selection> selections){return RatisSessionBindingPlan.borrow(selections,routes);}
        RatisSessionBindingPlan restore(byte[] bytes)throws IOException{return RatisSessionBindingPlan.restore(bytes,routes);}
        RatisDurableSessionClient open(Path root,DistributedSchema schema,RatisSessionBindingPlan plan)throws IOException{return opener.open(root,schema,plan);}
    }
    public static final class Generation implements AutoCloseable {
        private final RatisRocksDBAdapter adapter; private final RatisRocksDBDirect direct; private final RatisRocksDBBatches batches; private final RatisRocksDBRanges ranges; private final RatisTransactionClient transactions; private final RatisSnapshotClient snapshots; private final List<AutoCloseable> owned;
        private RatisPointOwnerManager points;private RatisAtomicMutationOwner atomicMutations;
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
        private RatisRocksDBDirect point(RemoteMutationOwner owner)throws IOException {if(points!=null)return points.select(owner);if(direct!=null && direct.owner().sameAuthority(owner))return direct;throw new IOException("Original point owner unavailable");}
        private RatisRocksDBDirect point(RemoteMutationToken token)throws IOException {return point(new RemoteMutationOwner(token.clusterId(),token.databaseId(),token.ownerId(),token.ownerEpoch(),token.requestSequence(),token.kind(),token.domains()));}
        private CompletableFuture<List<RemoteMutationOwner>> pointOwners(RequestContext context)throws IOException{return points!=null?points.discover(context):CompletableFuture.completedFuture(direct==null?List.of():List.of(direct.owner()));}
        private boolean retainsPoint(RemoteMutationToken token){return points!=null && points.contains(token);}
        @Override public synchronized void close() throws IOException {
            var resources=new ArrayList<AutoCloseable>();if(atomicMutations!=null)resources.add(atomicMutations); if(batches!=null) resources.add(batches); if(points!=null)resources.add(points);else if (direct!=null) resources.add(direct); resources.add(adapter); if (ranges!=null) resources.add(ranges); if (transactions!=null) resources.add(transactions); if (snapshots!=null) resources.add(snapshots); resources.addAll(owned);
            while (closed<resources.size()) { try { resources.get(closed).close(); closed++; } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException("Generation resource close failed",failure); } }
        }
    }
    public record DirectRecovery(byte[] schemaDigest,byte[] directoryDigest,long schemaEpoch,RatisRocksDBDirect.RecoveryState state) {
        public DirectRecovery { schemaDigest=schemaDigest.clone(); directoryDigest=directoryDigest.clone(); }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
        @Override public byte[] directoryDigest() { return directoryDigest.clone(); }
    }
    /** Bounds apply to one copied KVBatch's complete encoded group commands; chunks are independently atomic. */
    public record BatchLimits(int maximumInputItems,int maximumEncodedInputBytes,int maximumChunkItems,int maximumEncodedChunkBytes) {}
    public record BatchRecovery(byte[] schemaDigest,byte[] directoryDigest,long schemaEpoch,RatisRocksDBBatches.RecoveryState state,RatisRocksDBBatches.Progress progress) {
        public BatchRecovery { schemaDigest=schemaDigest.clone(); directoryDigest=directoryDigest.clone(); }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
        @Override public byte[] directoryDigest() { return directoryDigest.clone(); }
    }
    public BatchLimits batchLimits() { return new BatchLimits(RatisRocksDBBatches.MAX_INPUT_ITEMS,RatisRocksDBBatches.MAX_INPUT_BYTES,RatisRocksDBBatches.MAX_CHUNK_ITEMS,RatisRocksDBBatches.MAX_CHUNK_BYTES); }
    public static GenerationFactory durableGenerationFactory(RatisRoutedClient routed,RWScheduler scheduler,RatisSnapshotFences snapshotHomes,RatisTransactionStatuses transactionHomes,RatisSessionEpochs epochs,RatisCatalogue directories,RatisRoutedClient.SourceAckDirectoryReader acknowledgementDirectories,RatisRocksDBAdapter.AllocationSource allocations,SessionFactory sessions,List<AutoCloseable> generationOwned,RatisRequestBudget budget) {
        var transferred=List.copyOf(generationOwned);
        return (bindings,root,ids) -> {
            if(bindings.domains().size()!=1)return transactionGeneration(bindings,root,ids,scheduler,snapshotHomes,transactionHomes,allocations,budget,transferred);
            var domain=bindings.domains().getFirst();var schema=domain.schema();var directory=domain.directory();
            RatisPointOwnerManager points=null; RatisDurableSessionClient bulkSession=null,bulkOwner=null; RatisRocksDBBatches batches=null; RatisTransactionClient tx=null; RatisSnapshotClient snapshots=null; RatisRocksDBRanges ranges=null; RatisRocksDBDirect direct=null;RatisRangeDeleteClient rangeDeletes=null;RatisRequestBudget.Owner bulkCredit=null,rangeCredit=null;
            try {
                points=new RatisPointOwnerManager(schema,directory,routed,scheduler,root.resolve("points"),budget,sessions,epochs,directories,acknowledgementDirectories,transactionHomes); direct=points.reader();
                var bulkBindings=sessions.allBindings(directory);Path bulkInput=root.resolve("bulk"),bulkWindow=root.resolve("bulk-session/window.bin");
                bulkCredit=budget.owner(RemoteMutationToken.Kind.BULK,4*DistributedSchemaCodec.encodedSize(schema)+bulkBindings.reservation(),RatisRequestBudget.files(bulkInput,bulkInput.resolveSibling("bulk.pending"),bulkInput.resolveSibling("bulk.released"),bulkInput.resolveSibling("bulk.released.pending"),bulkWindow,bulkWindow.resolveSibling("window.bin.pending")));
                var bulkPaths=List.of(new RatisJournalAccess.Reservation(root.resolve("bulk"),RatisBatchRequestJournal.MAX_BYTES,0),new RatisJournalAccess.Reservation(root.resolve("bulk.lock"),32,0),new RatisJournalAccess.Reservation(root.resolve("bulk.released"),128*1024,364),new RatisJournalAccess.Reservation(root.resolve("bulk-reroutes/coordinator.lock"),32,0));budget.preflight(bulkPaths);
                bulkSession=sessions.open(Files.createDirectories(root.resolve("bulk-session")),schema,bulkBindings.freeze());bulkOwner=bulkSession;
                for(var actor:points.all())if(actor.owner().ownerId().equals(bulkSession.grant().sessionId()) && actor.owner().ownerEpoch()==bulkSession.grant().epoch())throw new IOException("Point and bulk sessions require distinct exclusively owned grants");
                bulkSession.installReroutes(root.resolve("bulk-reroutes"),schema,epochs,routed,transactionHomes,scheduler,directories,acknowledgementDirectories,budget);
                batches=new RatisRocksDBBatches(schema,directory,routed,bulkSession,scheduler,root.resolve("bulk"),RatisRocksDBBatches.SessionOwnership.OWNED,budget,bulkCredit); bulkSession=null;bulkCredit=null;budget.fulfilled(bulkPaths);
                tx=RatisTransactionClient.journalOwned(RatisTransactionBindings.of(schema,directory,routed),snapshotHomes,transactionHomes,scheduler,RatisTransactionClient.Limits.defaults(),budget); snapshots=RatisSnapshotClient.journalOwned(RatisTransactionBindings.of(schema,directory,routed),snapshotHomes,scheduler,RatisSnapshotClient.Limits.defaults(),RatisSnapshotCaptureRequest.Mode.READ_ONLY,budget); ranges=new RatisRocksDBRanges(schema,directory,snapshots,allocations);
                Path rangeRoot=root.resolve("range-delete");
                rangeCredit=budget.owner(RemoteMutationToken.Kind.RANGE,4L*(DistributedSchemaCodec.encodedSize(schema)+RangeDirectoryCodec.encodedSize(directory)),RatisRequestBudget.files(rangeRoot.resolve("input"),rangeRoot.resolve("input.pending"),rangeRoot.resolve("atomic"),rangeRoot.resolve("atomic.pending")));
                rangeDeletes=new RatisRangeDeleteClient(schema,directory,routed,transactionHomes,scheduler,allocations,rangeRoot,budget,rangeCredit);rangeCredit=null;var generation=new Generation(new RatisRocksDBAdapter(bindings,tx,allocations,direct,ranges,ids,batches).rangeDeletes(rangeDeletes),direct,ranges,tx,snapshots,generationResources(rangeDeletes,bulkOwner,transferred),batches);generation.points=points;if(budget.limits().maximumAtomicOwners()>0)generation.atomicMutations=new RatisAtomicMutationOwner(bindings,snapshotHomes,transactionHomes,scheduler,allocations,root.resolve("atomic-mutations"),budget);return generation;
            } catch (Throwable failure) { var unwind=new ArrayList<AutoCloseable>(Arrays.asList(rangeDeletes,batches,points,ranges,snapshots,tx,bulkSession,bulkCredit,rangeCredit));unwind.addAll(transferred);var attempted=Collections.newSetFromMap(new IdentityHashMap<AutoCloseable,Boolean>());if(batches!=null && bulkOwner!=null)attempted.add(bulkOwner);for(var resource:unwind)if(resource!=null && attempted.add(resource))try{resource.close();}catch(Exception cleanup){failure.addSuppressed(cleanup);} if (failure instanceof IOException io) throw io; throw new IOException("Generation creation failed",failure); }
        };
    }
    static Generation transactionGeneration(RatisTransactionBindings bindings,Path root,LongSupplier ids,RWScheduler scheduler,RatisSnapshotFences snapshotHomes,RatisTransactionStatuses transactionHomes,RatisRocksDBAdapter.AllocationSource allocations,RatisJournalAccess access,List<AutoCloseable> owned)throws IOException{
        RatisTransactionClient transactions=null;RatisSnapshotClient snapshots=null;
        try{transactions=RatisTransactionClient.journalOwned(bindings,snapshotHomes,transactionHomes,scheduler,RatisTransactionClient.Limits.defaults(),access);snapshots=RatisSnapshotClient.journalOwned(bindings,snapshotHomes,scheduler,RatisSnapshotClient.Limits.defaults(),RatisSnapshotCaptureRequest.Mode.READ_ONLY,access);var adapter=new RatisRocksDBAdapter(bindings,transactions,allocations,null,null,ids);var generation=new Generation(adapter,null,null,transactions,snapshots,owned);if(access instanceof RatisRequestBudget budget && budget.limits().maximumAtomicOwners()>0)generation.atomicMutations=new RatisAtomicMutationOwner(bindings,snapshotHomes,transactionHomes,scheduler,allocations,root.resolve("atomic-mutations"),budget);return generation;}
        catch(Throwable failure){var unwind=new ArrayList<AutoCloseable>(Arrays.asList(snapshots,transactions));unwind.addAll(owned);for(var resource:unwind)if(resource!=null)try{resource.close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}if(failure instanceof IOException io)throw io;throw new IOException("Aggregate transaction generation failed",failure);}
    }
    private static List<AutoCloseable> generationResources(AutoCloseable rangeDelete,AutoCloseable bulk,List<AutoCloseable> transferred){var seen=Collections.newSetFromMap(new IdentityHashMap<AutoCloseable,Boolean>());seen.add(bulk);var resources=new ArrayList<AutoCloseable>();for(var resource:List.of(rangeDelete))if(seen.add(resource))resources.add(resource);for(var resource:transferred)if(seen.add(resource))resources.add(resource);return List.copyOf(resources);}
    private final RatisJournalAccess journalAccess;private final RatisJournalAccess.RootLease rootOwnership;private final URI uri; private final Path root; private final RWScheduler clock; private final Map<UUID,RatisRoutedClient> routed;private final DomainPlacement placement;private final UUID catalogueDomain; private final Limits limits;
    private final FileChannel ownership; private final FileLock lock; private final RatisSchemaJobBridge schemas; private final RatisCdcBridge cdc; private final RatisGenerationRegistry generations;
    private final ExecutorService dataLane; private final List<AutoCloseable> ownedDependencies; private final RatisOperatorCatalogue operators;
    private volatile Thread dataWorker;
    private final java.util.concurrent.locks.ReentrantLock closeOwner=new java.util.concurrent.locks.ReentrantLock();
    private boolean stopping,closed; private int dataOwners,dependencyClosed; private long nextId,idLimit;
    /** All supplied transports/schedulers/allocators are borrowed unless listed in ownedDependencies. Factory generations are owned. */
    public RatisRocksDBConnection(URI uri,Path root,Map<UUID,RatisRoutedClient> routed,DomainPlacement placement,RWScheduler scheduler,RatisNodePlan.Catalogue binding,RaftClient catalogue,RatisSchemaReplicaClient replicas,RatisOperatorCatalogue operators,RatisSchemaClient.TrustedOperatorResolver resolver,GenerationFactory factory,Limits limits,List<AutoCloseable> ownedDependencies,RatisJournalAccess journalAccess) throws IOException {
        this.journalAccess=Objects.requireNonNull(journalAccess);
        this.uri=Objects.requireNonNull(uri); this.root=root.toAbsolutePath().normalize(); this.routed=Map.copyOf(routed);this.placement=Objects.requireNonNull(placement);if(routed.isEmpty())throw new IllegalArgumentException("Database connection requires declared domains");catalogueDomain=routed.keySet().stream().min(RangeDirectoryCodec.UUID_ORDER).orElseThrow(); clock=Objects.requireNonNull(scheduler); this.limits=Objects.requireNonNull(limits); this.ownedDependencies=List.copyOf(ownedDependencies); this.operators=operators;
        var namespace=this.routed.get(catalogueDomain).bootstrapDirectory();var rosterReader=new RatisDatabaseDomainRoster(catalogue,namespace.clusterId(),namespace.databaseId(),binding);var complete=rosterReader.readComplete();if(!complete.directories().keySet().equals(this.routed.keySet()))throw new IOException("Connection routes must cover the complete authoritative database roster");
        for(var entry:this.routed.entrySet()){var source=entry.getValue().bootstrapDirectory();if(!source.domainId().equals(entry.getKey()) || !source.clusterId().equals(namespace.clusterId()) || !source.databaseId().equals(namespace.databaseId()))throw new IOException("Foreign database domain route");}
        rootOwnership=journalAccess.backendRoot(this.root);
        boolean used=Files.exists(this.root.resolve("connection.lock"),LinkOption.NOFOLLOW_LINKS); ownership=openOwnership(this.root,rootOwnership); FileLock acquired=null; RatisSchemaJobBridge bridge=null; RatisGenerationRegistry registry=null; RatisCdcBridge cdcBridge=null;
        try {
            try { acquired=ownership.tryLock(); } catch (OverlappingFileLockException conflict) { throw new IOException("Connection root already owned",conflict); } if (acquired==null) throw new IOException("Connection root already owned"); lock=acquired;
            if(journalAccess instanceof RatisRequestBudget shared)RatisBackendJournalInventory.inventory(this.root,shared);
            journalAccess.register(this.root.resolve("connection.lock"),32);
            var ids=this.root.resolve("handles"); if (used && !Files.exists(ids,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Original connection handle state is missing"); if (Files.exists(ids,LinkOption.NOFOLLOW_LINKS)) { byte[] value=journalAccess.read(ids,64); if (value.length!=8) throw new IOException("Invalid handle reservation"); nextId=idLimit=ByteBuffer.wrap(value).getLong(); if (nextId<1) throw new IOException("Invalid handle high-water mark"); } else { nextId=idLimit=1; journalAccess.write(ids,ByteBuffer.allocate(8).putLong(1).array(),64); }
            var trusted=resolver==null ? (RatisSchemaClient.TrustedOperatorResolver)(name,className,sha,revision) -> { throw new IOException("Trusted operator resolver unavailable"); } : resolver;
            Path sdk=Files.createDirectories(this.root.resolve("schema")); var capacity=new RatisGenerationRegistry.Capacity(this.root.resolve("generation-capacity"),Files.createDirectories(this.root.resolve("generations")),sdk,limits.generations(),used,journalAccess); bridge=new RatisSchemaJobBridge(Files.createDirectories(this.root.resolve("requests")),sdk,clock,(domain,checkpoint) -> RatisSchemaClient.journalOwned(sdk,namespace.clusterId(),namespace.databaseId(),domain,binding,catalogue,this.routed.get(domain),replicas,operators,(path,fields) -> capacity.checkpoint(path,fields,checkpoint),journalAccess),trusted,routed,limits.schemaRequests(),limits.queuedSchema(),limits.schemaBytes(),journalAccess);
            registry=new RatisGenerationRegistry(Files.createDirectories(this.root.resolve("generations")),routed,new RatisSchemaCatalogue(catalogue,namespace.clusterId(),namespace.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()),factory,this::nextHandle,limits.generations(),capacity,journalAccess);
            var directories=new RatisCatalogue(catalogue,namespace.clusterId(),namespace.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
            cdcBridge=new RatisCdcBridge(RatisCdcSubscriptionController.journalOwned(Files.createDirectories(this.root.resolve("cdc")),namespace.clusterId(),namespace.databaseId(),binding,catalogue,this.routed.get(catalogueDomain),journalAccess),clock,directories::read);
            rosterReader.requireUnchanged(complete.roster());for(var domain:this.routed.keySet())if(!Arrays.equals(this.routed.get(domain).directory().payloadDigest(),complete.directories().get(domain).payloadDigest()))throw new IOException("Routed domain differs from authoritative database placement");
            schemas=bridge; generations=registry; cdc=cdcBridge;
        } catch (Throwable failure) { if(cdcBridge!=null) try { cdcBridge.close(); } catch(Exception cleanup) { failure.addSuppressed(cleanup); } if (registry!=null) try { registry.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); } if (bridge!=null) try { bridge.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); } if (acquired!=null) acquired.release(); ownership.close();rootOwnership.close(); if (failure instanceof IOException io) throw io; throw new IOException("Connection open failed",failure); }
        dataLane=Executors.newSingleThreadExecutor(task -> Thread.ofPlatform().daemon(true).name("ratis-connection-dispatch").unstarted(() -> { dataWorker=Thread.currentThread(); task.run(); }));
    }
    private static FileChannel openOwnership(Path root,RatisJournalAccess.RootLease lease)throws IOException {
        try{Files.createDirectories(root);return FileChannel.open(root.resolve("connection.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);}catch(IOException|RuntimeException failure){try{lease.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}throw failure;}
    }
    private synchronized long nextHandle() {
        if (nextId==idLimit) {
            if (idLimit>Long.MAX_VALUE-64) throw new IllegalStateException("Connection handle IDs exhausted"); long reserved=idLimit+64;
            try { journalAccess.write(root.resolve("handles"),ByteBuffer.allocate(8).putLong(reserved).array(),64); } catch (IOException failure) { stopping=true; throw new UncheckedIOException(failure); } idLimit=reserved;
        }
        return nextId++;
    }
    @Override public it.cavallium.rockserver.core.common.cdc.DistributedCdcAsyncAPI getDistributedCdcApi() { requireOpen(); return cdc; }
    @Override public RemoteMutationAsyncAPI getRemoteMutationApi(){requireOpen();return new RatisRemoteMutationApi(this,clock);}
    @Override public it.cavallium.rockserver.core.common.schema.SchemaOperationsAsyncAPI getSchemaOperationsApi(){
        requireOpen();return new it.cavallium.rockserver.core.common.schema.SchemaOperationsAsyncAPI(){
            private void check(RequestContext context){Objects.requireNonNull(context);requireOpen();long deadline=clock.resolveMonotonicDeadline(context);if(deadline!=Long.MAX_VALUE && clock.remainingMonotonicDeadlineNanos(deadline)<=0)throw RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,"Schema operation deadline expired");}
            public CompletableFuture<List<it.cavallium.rockserver.core.common.schema.SchemaOperations.Status>> inventory(RequestContext context){try{check(context);return CompletableFuture.completedFuture(it.cavallium.rockserver.core.common.schema.SchemaOperations.inventory(schemas.inventory().stream().map(RatisRocksDBConnection::schemaOperation).toList()));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}}
            public CompletableFuture<it.cavallium.rockserver.core.common.schema.SchemaOperations.Status> status(UUID id,RequestContext context){try{check(context);return CompletableFuture.completedFuture(schemaOperation(schemas.status(id)));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}}
            private CompletableFuture<it.cavallium.rockserver.core.common.schema.SchemaOperations.Status> act(UUID id,RequestContext context,boolean abort){try{check(context);return schemas.resume(Objects.requireNonNull(id),context,abort).thenApply(ignored->schemaOperation(schemas.status(id)));}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}}
            public CompletableFuture<it.cavallium.rockserver.core.common.schema.SchemaOperations.Status> resume(UUID id,RequestContext context){return act(id,context,false);}
            public CompletableFuture<it.cavallium.rockserver.core.common.schema.SchemaOperations.Status> abort(UUID id,RequestContext context){return act(id,context,true);}
        };
    }
    private static it.cavallium.rockserver.core.common.schema.SchemaOperations.Status schemaOperation(RatisSchemaRequests.Status status){
        var kind=it.cavallium.rockserver.core.common.schema.SchemaOperations.Kind.valueOf(status.kind().name());var result=status.result();
        return new it.cavallium.rockserver.core.common.schema.SchemaOperations.Status(status.requestId(),kind,it.cavallium.rockserver.core.common.schema.SchemaOperations.State.valueOf(status.state().name()),status.phase()==null?null:it.cavallium.rockserver.core.common.schema.SchemaOperations.Phase.valueOf(status.phase().name()),result==null?null:new it.cavallium.rockserver.core.common.schema.SchemaOperations.Result(result.requestId(),it.cavallium.rockserver.core.common.schema.SchemaOperations.Kind.valueOf(result.kind().name()),result.value(),result.changed(),result.aborted()));
    }
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
    private static long column(RocksDBAPICommand<?,?,?> request) {
        return switch(request){case Put<?> r->r.columnId();case PutMulti<?> r->r.columnId();case Delete<?> r->r.columnId();case DeleteMulti<?> r->r.columnId();case DeleteRange r->r.columnId();case Merge<?> r->r.columnId();case MergeMulti<?> r->r.columnId();case PutBatch r->r.columnId();case MergeBatch r->r.columnId();case Get<?> r->r.columnId();case ExistsMulti r->r.columnId();case OpenIterator r->r.columnId();case GetRangePage<?> r->r.columnId();case ReduceRange<?> r->r.columnId();case RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> r->r.columnId();default->0;};
    }
    private synchronized int transactionCount(){int count=0;for(var owner:generations.all())count+=owner.generation().adapter().stats().transactions();return count;}
    private CompletableFuture<RatisGenerationRegistry.Owned> owner(RocksDBAPICommand<?,?,?> request,RequestContext context)throws IOException {
        long id=handle(request),column=column(request),iterator=iterator(request);
        if(iterator!=0)return CompletableFuture.completedFuture(generations.iterator(iterator));
        if(id!=0){var selected=generations.transaction(id);if(column!=0 && selected.bindings().columns().stream().noneMatch(c->c.apiId()==column))throw new IOException("Original handle cannot cross database domains");return CompletableFuture.completedFuture(selected);}
        if(column!=0)return CompletableFuture.completedFuture(generations.column(column));
        if(request instanceof GetColumnId named){var selected=generations.column(named.name());if(selected==null)throw new IOException("Unknown database column name");return CompletableFuture.completedFuture(selected);}
        throw new IOException("Request has no database column/domain owner");
    }
    private CompletableFuture<Object> dispatch(RocksDBAPICommand<?,?,?> original,RequestContext context,boolean stream) {
        try { return dispatchChecked(original,context,stream); }catch(Throwable failure){return CompletableFuture.failedFuture(failure);}
    }
    private UUID createDomain(String name,ColumnSchema schema)throws IOException {var existing=generations.column(name);if(existing!=null){if(!existing.schema().column(name).schema().equals(schema))throw new IllegalArgumentException("Existing database column definition differs");return existing.schema().domainId();}var domain=placement.domain(name,schema);if(!routed.containsKey(domain))throw new IOException("Column placement selected an undeclared domain");return domain;}
    private UUID deleteDomain(String name)throws IOException {var existing=generations.column(name);if(existing!=null)return existing.schema().domainId();var domain=placement.domain(name,null);if(!routed.containsKey(domain))throw new IOException("Column placement selected an undeclared domain");return domain;}
    private CompletableFuture<Object> dispatchChecked(RocksDBAPICommand<?,?,?> original,RequestContext context,boolean stream)throws IOException {
        try { requireOpen(); } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
        RatisRocksDBAdapter.rejectUnqualifiedPhysical(original);
        if (original instanceof CreateColumn r) { var shape=copy(r.schema()); return schemaObserver(schemas.submit(createDomain(r.name(),shape),RatisSchemaRequests.Kind.CREATE,65536,context,(client,id) -> {
            OperatorUpload upload=null; var actual=shape;
            if (shape.mergeOperatorName()!=null) { if (operators==null || shape.mergeOperatorVersion()==null) throw new IOException("Published operator required"); upload=operators.readPublished(shape.mergeOperatorName(),shape.mergeOperatorVersion()); if (upload==null || shape.mergeOperatorClass()!=null && !shape.mergeOperatorClass().equals(upload.operator().className())) throw new IOException("Exact published operator missing/changed"); actual=new ColumnSchema(shape.keys(),shape.variableTailKeys(),shape.hasValue(),shape.mergeOperatorName(),shape.mergeOperatorVersion(),upload.operator().className()); }
            return client.prepareCreate(id,r.name(),actual,upload);
        }),result -> result.value()); }
        if (original instanceof DeleteColumn r) return schemaObserver(schemas.submit(generations.column(r.columnId()).schema().domainId(),RatisSchemaRequests.Kind.DELETE,65536,context,(client,id) -> client.prepareDelete(id,r.columnId())),result -> null);
        if (original instanceof DeleteColumnIfExists r) return schemaObserver(schemas.submit(deleteDomain(r.name()),RatisSchemaRequests.Kind.DELETE_IF_EXISTS,65536,context,(client,id) -> client.prepareDeleteIfExists(id,r.name())),result -> result.changed());
        if (original instanceof UploadMergeOperator r) { if (r.jarData().length>OperatorUpload.MAX_ARTIFACT_BYTES) return CompletableFuture.failedFuture(new IllegalArgumentException("Operator artifact exceeds durable upload bound")); byte[] jar=r.jarData().clone(); return schemaObserver(schemas.submit(catalogueDomain,RatisSchemaRequests.Kind.UPLOAD,jar.length+65536L,context,(client,id) -> client.prepareUpload(id,r.name(),r.className(),jar,schemasResolver())),result -> result.value()); }
        if (original instanceof CheckMergeOperator r) { byte[] hash=r.hash().clone(); if (hash.length!=32) return CompletableFuture.failedFuture(new IllegalArgumentException("Operator SHA must contain32bytes")); return (CompletableFuture<Object>)(CompletableFuture<?>)schemas.check(catalogueDomain,r.name(),hash,context); }
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
                if(original instanceof RocksDBAPICommand.GetAllColumnDefinitions){var definitions=new LinkedHashMap<String,ColumnSchema>();for(var generation:generations.current())for(var binding:generation.schema().columns())if(definitions.putIfAbsent(binding.name(),binding.schema())!=null)throw new IOException("Ambiguous database column name");actual=CompletableFuture.completedFuture(Collections.unmodifiableMap(definitions));}
                else {
                    long handle=handle(original),iterator=iterator(original);if(handle==0 && iterator==0 && !original.isReadOnly() && !(original instanceof OpenTransaction))for(var generation:generations.all()){
                        if(generation.generation().direct()!=null && generation.generation().direct().recoveryState()!=RatisRocksDBDirect.RecoveryState.NONE)throw new IOException("Resolve the original generation's retained direct request before fresh mutation");
                        if(generation.generation().adapter().rangeDeletes()!=null && generation.generation().adapter().rangeDeletes().recoveryState()!=RatisRocksDBDirect.RecoveryState.NONE)throw new IOException("Resolve the original generation's retained range deletion before fresh mutation");
                        if(generation.generation().batches()!=null && generation.generation().batches().recoveryState()!=RatisRocksDBBatches.RecoveryState.NONE)throw new IOException("Resolve the original generation's retained batch request before fresh mutation");
                    }
                    var actualDrain=new CompletableFuture<Void>();drained=actualDrain;
                    var declaration=original instanceof OpenTransaction open ? generations.scope(open.scope()) : null;
                    actual=(declaration==null ? owner(original,remaining(context,deadline)) : CompletableFuture.completedFuture(declaration.owner())).thenCompose(owner->{selected.set(owner);try{remaining(context,deadline);if(view.isCancelled())throw new CancellationException("Connection observer cancelled before native dispatch");if((original instanceof OpenTransaction || original instanceof Get<?> get && get.requestType().getRequestTypeId()==RequestType.RequestTypeId.FOR_UPDATE) && transactionCount()>=RatisRocksDBAdapter.MAX_TRANSACTIONS)throw new IOException("Database transaction/update capacity exhausted");var dispatched=original instanceof OpenTransaction open ? owner.generation().adapter().openOwned(declaration.scope(),open.transactionLeaseTtl(),remaining(context,deadline)) : owner.generation().adapter().dispatchOwned(original,remaining(context,deadline));cancelDelegate.set(dispatched.cancelObserver());if(view.isCancelled())dispatched.cancelObserver().run();dispatched.drained().whenComplete((ignored,failure)->actualDrain.complete(null));Object delegated=dispatched.observer();return streamOwner!=null?CompletableFuture.completedFuture(delegated):((CompletableFuture<?>)delegated).thenApply(value->(Object)value);}catch(Throwable failure){actualDrain.complete(null);return CompletableFuture.failedFuture(failure);}});
                    actual.whenComplete((value,failure)->{if(failure!=null && selected.get()==null)actualDrain.complete(null);});
                }
            } catch (Throwable failure) { drained.complete(null);actual=CompletableFuture.failedFuture(failure); }
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
    private RatisGenerationRegistry.Owned originalMutationGeneration(RemoteMutationToken token) throws IOException {
        if(token.kind()==RemoteMutationToken.Kind.ATOMIC){for(var generation:generations.all())if(generation.bindings().clusterId().equals(token.clusterId()) && generation.bindings().databaseId().equals(token.databaseId()) && generation.bindings().authority().equals(token.domains()) && generation.generation().atomicMutations!=null && generation.generation().atomicMutations.owner().sameAuthority(new RemoteMutationOwner(token.clusterId(),token.databaseId(),token.ownerId(),token.ownerEpoch(),token.requestSequence(),token.kind(),token.domains())))return generation;throw new IOException("Original atomic complete generation unavailable");}
        for(var generation:generations.singletons()) { var schema=generation.schema(); if(schema.clusterId().equals(token.clusterId()) && schema.databaseId().equals(token.databaseId()) && schema.domainId().equals(token.domains().getFirst().domainId()) && Arrays.equals(schema.digest(),token.domains().getFirst().schemaDigest()) && (Arrays.equals(generation.directory().payloadDigest(),token.domains().getFirst().directoryDigest()) || token.kind()==RemoteMutationToken.Kind.POINT && generation.generation().retainsPoint(token)))return generation; }
        throw new IOException("Original mutation generation unavailable; a latest generation cannot replace it");
    }
    public CompletableFuture<List<RemoteMutationOwner>> remoteMutationOwners(long column,RequestContext context) {return diagnostic(context,remaining->{var generation=generations.column(column).generation();return generation.pointOwners(remaining).thenApply(points->{var owners=new ArrayList<>(points);try{if(generation.batches()!=null)owners.add(generation.batches().owner());if(generation.adapter().rangeDeletes()!=null)owners.add(generation.adapter().rangeDeletes().owner());return List.copyOf(owners);}catch(IOException failure){throw new CompletionException(failure);}});});}

    public CompletableFuture<List<RemoteMutationOwner>> atomicMutationOwners(List<Long> columns,RequestContext context){
        var frozen=List.copyOf(columns);if(frozen.isEmpty() || frozen.size()>256)return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid atomic selected columns"));
        return diagnostic(context,remaining->{var generation=generations.atomicColumns(frozen).generation();if(generation.atomicMutations==null)throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,"Durable atomic owner unavailable");return CompletableFuture.completedFuture(List.of(generation.atomicMutations.owner()));});
    }
    public CompletableFuture<RemoteMutationToken> prepareAtomicMutation(RemoteMutationOwner expected,UUID id,long sequence,List<AtomicMutation> mutations,List<AtomicPredicate> predicates,long ttlMillis,RequestContext context){
        try {
            Objects.requireNonNull(context);long bytes=RemoteMutationInputs.atomicSize(mutations,predicates,ttlMillis);
            // Admit the synchronous intake copy before the connection's asynchronous dispatch boundary.
            RatisRequestBudget.Lease intake=journalAccess instanceof RatisRequestBudget budget?budget.work(64*1024+4*bytes):null;
            try {
                var frozenMutations=mutations.stream().map(m->new AtomicMutation(m.column(),new Keys(Arrays.stream(m.key().keys()).map(v->it.cavallium.buffer.Buf.wrap(v.toByteArray())).toArray(it.cavallium.buffer.Buf[]::new)),m.kind(),m.value()==null?null:it.cavallium.buffer.Buf.wrap(m.value().toByteArray()),m.returns())).toList();
                var frozenPredicates=predicates.stream().map(p->new AtomicPredicate(p.column(),new Keys(Arrays.stream(p.key().keys()).map(v->it.cavallium.buffer.Buf.wrap(v.toByteArray())).toArray(it.cavallium.buffer.Buf[]::new)),p.expected()==null?null:it.cavallium.buffer.Buf.wrap(p.expected().toByteArray()))).toList();
                var columns=new ArrayList<Long>();frozenMutations.forEach(m->columns.add(m.column()));frozenPredicates.forEach(p->columns.add(p.column()));
                var result=diagnostic(context,remaining->{var generation=generations.atomicColumns(columns).generation();if(generation.atomicMutations==null)throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,"Durable atomic owner unavailable");return CompletableFuture.completedFuture(generation.atomicMutations.prepare(expected,id,sequence,frozenMutations,frozenPredicates,ttlMillis));},()->{if(intake!=null)intake.close();});
                return result;
            }catch(Throwable failure){if(intake!=null)intake.close();throw failure;}
        }catch(Throwable failure){return CompletableFuture.failedFuture(failure);}
    }
    public CompletableFuture<RemoteMutationToken> preparePointMutation(RemoteMutationOwner expectedOwner,UUID requestId,long requestSequence,long column,List<Keys> keys,List<it.cavallium.buffer.Buf> values,RatisRocksDBDirect.WriteKind kind,RequestType.RequestTypeId type,boolean multi,RequestContext context) {
        return startDiagnosticOwned(context,remaining -> {var actor=generations.column(column).generation().point(expectedOwner);var observer=actor.prepareSequence(expectedOwner,requestId,requestSequence,column,keys,values,kind,type,multi,remaining);return new OwnedDiagnostic<>(observer,actor.actualCompletion(observer),() -> observer.cancel(false));});
    }
    public CompletableFuture<RemoteMutationToken> prepareRangeMutation(RemoteMutationOwner expectedOwner,UUID requestId,long requestSequence,long column,Keys lower,Keys upper,RequestContext context) {return diagnostic(context,remaining -> CompletableFuture.completedFuture(generations.column(column).generation().adapter().rangeDeletes().prepare(expectedOwner,requestId,requestSequence,column,lower,upper)));}
    public CompletableFuture<RemoteMutationToken> prepareBulkMutation(RemoteMutationOwner expectedOwner,UUID requestId,long requestSequence,long column,boolean merge,RequestContext context) {return diagnostic(context,remaining -> CompletableFuture.completedFuture(generations.column(column).generation().batches().prepare(expectedOwner,requestId,requestSequence,column,merge)));}
    public CompletableFuture<Object> executeRemoteMutation(RemoteMutationToken token,RequestContext context) {
        return diagnosticOwned(context,remaining -> {var generation=originalMutationGeneration(token).generation();return switch(token.kind()) {
            case ATOMIC -> {var result=generation.atomicMutations.execute(token,remaining);yield new OwnedDiagnostic<>(result.observer().thenApply(value->(Object)value),result.drained(),result.cancel());}
            case POINT -> {var actor=generation.point(token);var observer=actor.execute(token,remaining);yield new OwnedDiagnostic<>(observer,actor.actualCompletion(observer),() -> observer.cancel(false));}
            case RANGE -> {var result=generation.adapter().rangeDeletes().execute(token,remaining);yield new OwnedDiagnostic<>(result.observer().thenApply(value -> (Object)value),result.drained(),result.cancel());}
            case BULK -> {var observer=generation.batches().resumeRetained(token,remaining);yield new OwnedDiagnostic<>(observer.thenApply(value -> (Object)value),generation.batches().actualCompletion(observer),() -> observer.cancel(false));}
        };});
    }
    public CompletableFuture<RemoteMutationStatus> remoteMutationStatus(RemoteMutationToken token,RequestContext context) {return diagnostic(context,remaining -> {RatisGenerationRegistry.Owned owner;try {owner=originalMutationGeneration(token);}catch(IOException absent){return CompletableFuture.completedFuture(new RemoteMutationStatus(token,RemoteMutationStatus.State.OWNER_UNAVAILABLE,-1,-1,0,null,null));}var generation=owner.generation();return CompletableFuture.completedFuture(switch(token.kind()){case ATOMIC -> generation.atomicMutations.status(token);case POINT -> {RatisRocksDBDirect actor;try{actor=generation.point(token);}catch(IOException absent){yield new RemoteMutationStatus(token,RemoteMutationStatus.State.OWNER_UNAVAILABLE,-1,-1,0,null,null);}yield actor.status(token);}case RANGE -> generation.adapter().rangeDeletes().status(token);case BULK -> generation.batches().status(token);});});}
    public CompletableFuture<List<byte[]>> remoteMutationResults(RemoteMutationToken token,int offset,int maximumItems,int maximumBytes,RequestContext context) {return diagnostic(context,remaining -> {var generation=originalMutationGeneration(token).generation();return CompletableFuture.completedFuture(switch(token.kind()){case ATOMIC -> generation.atomicMutations.resultPage(token,offset,maximumItems,maximumBytes);case POINT -> generation.point(token).resultPage(token,offset,maximumItems,maximumBytes);case RANGE -> generation.adapter().rangeDeletes().resultPage(token,offset,maximumItems,maximumBytes);case BULK -> List.of();});});}
    public CompletableFuture<Void> acknowledgeRemoteMutation(RemoteMutationToken token,byte[] resultDigest,RequestContext context) {byte[] frozen=resultDigest.clone();return diagnostic(context,remaining -> {var generation=originalMutationGeneration(token).generation();switch(token.kind()){case ATOMIC -> generation.atomicMutations.acknowledge(token,frozen);case POINT -> generation.point(token).acknowledgeResult(token,frozen);case RANGE -> generation.adapter().rangeDeletes().acknowledgeResult(token,frozen);case BULK -> generation.batches().acknowledgeResult(token,frozen);}return CompletableFuture.completedFuture(null);});}
    public CompletableFuture<RatisRocksDBBatches.Progress> appendRemoteBatch(RemoteMutationToken token,long ordinal,KVBatch batch,byte[] inputDigest,RequestContext context) {byte[] frozen=inputDigest.clone();return diagnosticOwned(context,remaining -> {if(token.kind()!=RemoteMutationToken.Kind.BULK)throw new IOException("Expected bulk token");var actor=originalMutationGeneration(token).generation().batches();var observer=actor.append(token,ordinal,batch,frozen,remaining);return new OwnedDiagnostic<>(observer,actor.actualCompletion(observer),() -> observer.cancel(false));});}
    public CompletableFuture<RatisRocksDBBatches.Progress> finishRemoteInput(RemoteMutationToken token,RequestContext context) {return diagnosticOwned(context,remaining -> {if(token.kind()!=RemoteMutationToken.Kind.BULK)throw new IOException("Expected bulk token");var actor=originalMutationGeneration(token).generation().batches();var observer=actor.finishInput(token,remaining);return new OwnedDiagnostic<>(observer,actor.actualCompletion(observer),() -> observer.cancel(false));});}
    public CompletableFuture<List<DirectRecovery>> rangeDeleteRecoveryStatus(RequestContext context){return diagnostic(context,remaining->{var states=new ArrayList<DirectRecovery>();for(var generation:generations.singletons()){var owner=generation.generation().adapter().rangeDeletes();if(owner!=null)states.add(new DirectRecovery(generation.schema().digest(),generation.directory().payloadDigest(),generation.schema().schemaEpoch(),owner.recoveryState()));}return CompletableFuture.completedFuture(List.copyOf(states));});}
    public CompletableFuture<Void> resumeRangeDelete(byte[] schemaDigest,byte[] directoryDigest,RequestContext context){byte[] frozen=schemaDigest.clone(),placement=directoryDigest.clone();return diagnosticOwned(context,remaining->{for(var generation:generations.singletons())if(Arrays.equals(generation.schema().digest(),frozen) && Arrays.equals(generation.directory().payloadDigest(),placement)){var owner=generation.generation().adapter().rangeDeletes();if(owner!=null){var result=owner.resume(remaining);return new OwnedDiagnostic<>(result.observer(),result.drained(),result.cancel());}}throw new IOException("Unknown original range deletion generation");});}
    public CompletableFuture<List<DirectRecovery>> directRecoveryStatus(RequestContext context) { return diagnostic(context,remaining -> { var states=new ArrayList<DirectRecovery>(); for (var generation:generations.singletons()) if (generation.generation().direct()!=null) states.add(new DirectRecovery(generation.schema().digest(),generation.directory().payloadDigest(),generation.schema().schemaEpoch(),generation.generation().direct().recoveryState())); return CompletableFuture.completedFuture(List.copyOf(states)); }); }
    public CompletableFuture<Object> resumeDirectRequest(byte[] schemaDigest,byte[] directoryDigest,RequestContext context) { byte[] frozen=schemaDigest.clone(),placement=directoryDigest.clone(); return diagnostic(context,remaining -> { for (var generation:generations.singletons()) if (Arrays.equals(generation.schema().digest(),frozen) && Arrays.equals(generation.directory().payloadDigest(),placement) && generation.generation().direct()!=null) return generation.generation().direct().resumeRetained(remaining); return CompletableFuture.failedFuture(new IOException("Unknown original direct generation")); }); }
    public CompletableFuture<List<BatchRecovery>> batchRecoveryStatus(RequestContext context) {
        return diagnostic(context,remaining -> { var states=new ArrayList<BatchRecovery>(); for(var generation:generations.singletons()) if(generation.generation().batches()!=null) { var batches=generation.generation().batches(); states.add(new BatchRecovery(generation.schema().digest(),generation.directory().payloadDigest(),generation.schema().schemaEpoch(),batches.recoveryState(),batches.progress())); } return CompletableFuture.completedFuture(List.copyOf(states)); });
    }
    private record OwnedDiagnostic<T>(CompletableFuture<T> observer,CompletableFuture<Void> drained,Runnable cancelObserver) {}
    @FunctionalInterface private interface OwnedDiagnosticWork<T> { OwnedDiagnostic<T> run(RequestContext remaining) throws Exception; }
    private <T> CompletableFuture<T> startDiagnosticOwned(RequestContext context,OwnedDiagnosticWork<T> work) {
        final OwnedDiagnostic<T> owned;try{WorkloadAdmission.resolve(context,OperationFamily.METADATA);long deadline=clock.resolveMonotonicDeadline(context);acquireData();try{owned=work.run(remaining(context,deadline));}catch(Throwable failure){synchronized(this){dataOwners--;}throw failure;}}catch(Throwable failure){return CompletableFuture.failedFuture(failure);}
        owned.drained().whenComplete((ignored,failure)->{synchronized(this){dataOwners--;}});var view=new CompletableFuture<T>();view.whenComplete((value,failure)->{if(view.isCancelled())owned.cancelObserver().run();});owned.observer().whenComplete((value,failure)->{if(failure==null)view.complete(value);else view.completeExceptionally(unwrap(failure));});return view;
    }
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
    private <T> CompletableFuture<T> diagnostic(RequestContext context,Diagnostic<T> work){return diagnostic(context,work,()->{});}
    private <T> CompletableFuture<T> diagnostic(RequestContext context,Diagnostic<T> work,Runnable drained) {
        final long deadline; try { Objects.requireNonNull(context); WorkloadAdmission.resolve(context,OperationFamily.METADATA); deadline=clock.resolveMonotonicDeadline(context); acquireData(); } catch (Throwable failure) { drained.run();return CompletableFuture.failedFuture(failure); }
        var result=new CompletableFuture<T>(); Runnable dispatch=() -> {
            CompletableFuture<T> actual; try { actual=work.run(remaining(context,deadline)); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            actual.whenComplete((value,failure) -> { drained.run();Runnable publish=() -> { synchronized (RatisRocksDBConnection.this) { dataOwners--; } if (failure==null) result.complete(value); else result.completeExceptionally(unwrap(failure)); }; try { dataLane.execute(publish); } catch (RejectedExecutionException rejected) { publish.run(); } });
        };
        try { dataLane.execute(dispatch); } catch (RejectedExecutionException failure) { drained.run();synchronized (this) { dataOwners--; } result.completeExceptionally(failure); } return result.copy();
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
        dataLane.shutdown(); if(lock.isValid())lock.release(); ownership.close();rootOwnership.close(); synchronized (this) { closed=true; }
        } finally { closeOwner.unlock(); }
    }
}
