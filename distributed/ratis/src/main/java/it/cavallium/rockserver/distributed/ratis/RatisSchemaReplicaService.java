package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import static it.cavallium.rockserver.distributed.ratis.RatisSchemaReplicaRpc.*;

/** Replica-addressed durable preparation; observer cancellation never cancels actual scheduler work. */
public final class RatisSchemaReplicaService {
    public record Limits(int maximumUploads,long maximumDiskBytes,long maximumHeapBytes,int maximumTasks,int maximumObservers,long minimumFreeBytes) {
        public Limits {
            if (maximumUploads<1 || maximumUploads>64 || maximumDiskBytes<16L*1024*1024 || maximumDiskBytes>1024L*1024*1024
                    || maximumHeapBytes<48L*1024*1024 || maximumHeapBytes>256L*1024*1024 || maximumTasks<1 || maximumTasks>64
                    || maximumObservers<1 || maximumObservers>64 || minimumFreeBytes<0) throw new IllegalArgumentException("Invalid schema replica limits");
        }
        public static Limits defaults() { return new Limits(16,64L*1024*1024,192L*1024*1024,16,16,1024L*1024*1024); }
    }
    @FunctionalInterface public interface OperatorPolicy { DeterministicContract admit(DistributedSchema.Operator operator) throws Exception; }
    @FunctionalInterface interface AuthorityReader { RatisSchemaAuthority.View read(SchemaChange.Header header) throws Exception; }
    @FunctionalInterface interface TargetValidator { long validate(Target target,boolean currentSource) throws Exception; }
    private record Local(ReplicatedRangeStore store,ReplicaDescriptor replica,AuthorityReader authority,TargetValidator validator) {}
    private record Key(int kind,String digest) {}
    private final DistributedStoragePool pool;
    private final Path poolPath;
    private final RatisSchemaArtifactRegistry registry;
    private final OperatorPolicy policy;
    private final Limits limits;
    private final RWScheduler scheduler;
    private final RWScheduler.WorkloadExecutor batch,control;
    private final Map<UUID,Local> locals=new HashMap<>();
    private final Map<Key,Work> work=new LinkedHashMap<>();
    private long heapBytes;
    private int parsing;
    private boolean stopping,closing;
    private CompletableFuture<Void> stopped=new CompletableFuture<>();
    private int stopObservers;
    public RatisSchemaReplicaService(Path poolPath,Path stagingRoot,DistributedStoragePool pool,RWScheduler scheduler,OperatorPolicy policy,Limits limits) throws IOException {
        this.poolPath=poolPath.toAbsolutePath().normalize(); this.pool=Objects.requireNonNull(pool); this.scheduler=Objects.requireNonNull(scheduler);
        this.policy=Objects.requireNonNull(policy); this.limits=Objects.requireNonNull(limits);
        registry=new RatisSchemaArtifactRegistry(stagingRoot,limits);
        batch=scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION);
        control=scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT);
    }
    synchronized void register(UUID group,ReplicatedRangeStore store,ReplicaDescriptor replica,AuthorityReader authority,TargetValidator validator) {
        if (stopping || parsing!=0 || !work.isEmpty() || locals.size()>=1024 || locals.putIfAbsent(group,new Local(store,replica,authority,validator))!=null) throw new IllegalStateException("Schema replica context already exposed or full");
    }
    public CompletableFuture<byte[]> submit(UUID addressedGroup,byte[] encoded) {
        boolean parsingReserved=false; long parsingCharge=0;
        try {
            if (encoded==null || encoded.length>MAX_BYTES) throw new IOException("Invalid schema replica frame size");
            parsingCharge=3L*encoded.length+2L*MAX_TARGET_BYTES+1024L;
            synchronized (this) {
                if (stopping || work.size()+parsing>=limits.maximumTasks() || parsingCharge>limits.maximumHeapBytes()-heapBytes) throw new IOException("Schema replica parsing capacity unavailable");
                parsing++; heapBytes+=parsingCharge; parsingReserved=true;
            }
            var request=RatisSchemaReplicaRpc.request(encoded); Local local;
            synchronized (this) { local=locals.get(addressedGroup); }
            if (local==null || !request.target().binding().groupId().equals(addressedGroup) || !request.target().replica().equals(local.replica())) throw new IOException("Schema request addressed another frozen replica");
            String owner=HexFormat.of().formatHex(OperatorUpload.sha256(request.target().encode()));
            var key=new Key(request.kind(),HexFormat.of().formatHex(request.digest()));
            long observer=MAX_TARGET_BYTES+1024L;
            long charge=3L*encoded.length+2L*MAX_TARGET_BYTES+1024L;
            if (request.kind()==ScopedTransactionRpc.SCHEMA_ARTIFACT_FINISH) charge+=3L*request.upload().artifactLength()+96L*1024*1024;
            if (request.kind()==ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE) {
                var definition=request.target().binding().definition();
                if (definition.sourceSchema().columns().stream().anyMatch(column -> column.operator()!=null)
                        || definition.targetSchema().columns().stream().anyMatch(column -> column.operator()!=null)) charge+=3L*MergeOperatorRegistry.MAX_DISTRIBUTED_JAR_BYTES;
                // Current source handles already exist; CREATE can instantiate only its single changed operator.
                if (definition.kind()==SchemaChange.Kind.CREATE_COLUMN && definition.targetSchema().column(definition.header().changedColumnId()).operator()!=null) charge+=96L*1024*1024;
            }
            if (request.kind()==ScopedTransactionRpc.SCHEMA_ARTIFACT_CHUNK) charge+=request.upload().chunks().get(request.chunkIndex()).length();
            final Work selected;
            synchronized (this) {
                if (stopping) throw new IOException("Schema replica service is stopping");
                var existing=work.get(key);
                if (existing!=null && existing.observers>=limits.maximumObservers()) throw new IOException("Schema observer capacity exhausted");
                long reservation=observer+(existing==null ? charge : 0);
                if (reservation>limits.maximumHeapBytes()-heapBytes+parsingCharge || existing==null && work.size()+parsing>limits.maximumTasks()) throw new IOException("Schema actual work capacity exhausted");
                if (existing==null) { selected=new Work(key,owner,request,local,charge); work.put(key,selected); } else selected=existing;
                selected.observers++; heapBytes+=reservation-parsingCharge; parsing--; parsingReserved=false;
            }
            var view=new CompletableFuture<byte[]>();
            selected.actual.whenComplete((reply,failure) -> {
                try { if (failure==null) { if (!view.isDone()) view.complete(reply.clone()); } else view.completeExceptionally(failure); }
                finally { synchronized (RatisSchemaReplicaService.this) { selected.observers--; heapBytes-=observer; } }
            });
            dispatch(); return view;
        } catch (Throwable failure) { return CompletableFuture.failedFuture(failure); }
        finally { if (parsingReserved) { synchronized (this) { parsing--; heapBytes-=parsingCharge; } closeIfStopped(); } }
    }
    private final class Work implements Runnable,RWScheduler.RejectionAwareTask {
        final Key key; final String owner; final Request request; final Local local; final long charge;
        final CompletableFuture<byte[]> actual=new CompletableFuture<>(); int observers; boolean scheduled,started,finished; Throwable rejection;
        Work(Key key,String owner,Request request,Local local,long charge) { this.key=key; this.owner=owner; this.request=request; this.local=local; this.charge=charge; }
        RWScheduler.WorkloadExecutor executor() { return request.kind()==ScopedTransactionRpc.SCHEMA_CAPACITY_RELEASE ? control : batch; }
        @Override public void run() {
            synchronized (RatisSchemaReplicaService.this) { if (finished) return; started=true; }
            try { finish(this,execute(request,local),null); } catch (Throwable failure) { finish(this,null,failure); }
        }
        @Override public void reject(RuntimeException failure) {
            synchronized (RatisSchemaReplicaService.this) { if (finished) return; rejection=failure; if (started) return; }
            finish(this,null,failure);
        }
    }
    private void dispatch() {
        var selected=new ArrayList<Work>();
        synchronized (this) {
            for (var next:work.values()) {
                if (next.scheduled || stopping) continue;
                if (work.values().stream().anyMatch(other -> other!=next && other.owner.equals(next.owner) && other.scheduled)) continue;
                next.scheduled=true; selected.add(next);
            }
        }
        for (var task:selected) {
            try { task.executor().execute(task,Math.max(1,task.request.upload()==null ? task.charge : task.request.upload().artifactLength())); }
            catch (RuntimeException failure) { task.reject(failure); }
        }
    }
    private byte[] execute(Request request,Local local) throws Exception {
        var binding=request.target().binding(); long stamp=local.validator().validate(request.target(),false);
        if (request.kind()<=ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE) {
            if (request.kind()!=ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE) prepared(request,local,stamp);
            var contract=policy.admit(request.upload().operator());
            requirePolicy(request.upload().operator(),contract);
            RatisSchemaArtifactRegistry.Progress progress=switch (request.kind()) {
                case ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN -> registry.begin(request);
                case ScopedTransactionRpc.SCHEMA_ARTIFACT_CHUNK -> registry.chunk(request);
                case ScopedTransactionRpc.SCHEMA_ARTIFACT_FINISH -> {
                    byte[] jar=registry.artifact(request);
                    requireStamp(local.validator().validate(request.target(),true),stamp);
                    DistributedStoragePool.stageOperator(poolPath,request.upload().operator(),contract,jar);
                    yield registry.ready(request);
                }
                case ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE -> registry.progress(request);
                default -> throw new IOException("Unknown artifact operation");
            };
            requireStamp(local.validator().validate(request.target(),false),stamp);
            return RatisSchemaReplicaRpc.reply(request,progress.chunks(),progress.ready(),null,CapacityState.NONE,false);
        }
        var status=pool.lookupSchemaCapacityStatus(local.store(),binding.definition(),binding.changedPhysicalName(),binding.mergeAdmission());
        if (request.kind()==ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE) {
            prepared(request,local,stamp);
            for (var column:binding.definition().targetSchema().columns()) if (column.operator()!=null) requirePolicy(column.operator(),policy.admit(column.operator()));
            var names=new HashMap<>(pool.schemaPhysicalNames(local.store())); var merges=new HashSet<>(local.store().deterministicMergeColumns());
            if (binding.header().kind()==SchemaChange.Kind.CREATE_COLUMN) { names.put(binding.header().changedColumnId(),binding.changedPhysicalName()); if (binding.mergeAdmission()) merges.add(binding.header().changedColumnId()); }
            else { names.remove(binding.header().changedColumnId()); merges.remove(binding.header().changedColumnId()); }
            pool.reserveSchemaCapacity(local.store(),binding.definition(),names,merges);
        } else if (request.kind()==ScopedTransactionRpc.SCHEMA_CAPACITY_RELEASE) {
            if (status.reservation()==null) throw new IOException("Capacity release lacks retained original reservation");
            if (status.forcedDurable() && (status.state()==DistributedStoragePool.SchemaCapacityState.ABORTED || status.state()==DistributedStoragePool.SchemaCapacityState.CONSUMED || status.state()==DistributedStoragePool.SchemaCapacityState.RETIRED)) {
                registry.release(request.target());
                return RatisSchemaReplicaRpc.reply(request,new BitSet(),false,status.reservation(),CapacityState.valueOf(status.state().name()),true);
            }
            var installed=local.store().lookupSchemaInstallation(binding.header()); SchemaCapacityRelease authority;
            if (installed!=null) authority=SchemaCapacityRelease.completed(installed);
            else {
                var view=local.authority().read(binding.header()); requireAuthority(binding,view);
                if (view.head().status().phase()==SchemaChange.Phase.ABORTED) authority=SchemaCapacityRelease.aborted(view.head().status(),view.catalogueGroup(),view.position());
                else if (view.head().status().phase()==SchemaChange.Phase.RETIRED) authority=SchemaCapacityRelease.retired(view.head().status(),view.catalogueGroup(),view.position());
                else throw new IOException("Capacity release lacks terminal catalogue or native INSTALL authority");
            }
            requireStamp(local.validator().validate(request.target(),false),stamp); pool.releaseSchemaCapacity(local.store(),status.reservation(),authority); registry.release(request.target());
        }
        requireStamp(local.validator().validate(request.target(),false),stamp);
        status=pool.lookupSchemaCapacityStatus(local.store(),binding.definition(),binding.changedPhysicalName(),binding.mergeAdmission());
        return RatisSchemaReplicaRpc.reply(request,new BitSet(),false,status.reservation(),CapacityState.valueOf(status.state().name()),status.forcedDurable());
    }
    private void prepared(Request request,Local local,long stamp) throws Exception {
        var view=local.authority().read(request.target().binding().header()); requireAuthority(request.target().binding(),view);
        if (view.head().status().phase()!=SchemaChange.Phase.PREPARED || view.directory()==null
                || !Arrays.equals(view.directory().payloadDigest(),request.target().binding().header().sourceDirectoryDigest())) throw new IOException("Replica preparation lacks fresh exact PREPARED catalogue authority");
        requireStamp(local.validator().validate(request.target(),true),stamp);
    }
    private static void requireStamp(long current,long expected) throws IOException {
        if (current!=expected) throw new IOException("Schema replica configuration changed during actual work");
    }
    private static void requireAuthority(Binding binding,RatisSchemaAuthority.View view) throws IOException {
        var header=binding.header();
        if (!view.request().equals(new RatisSchemaAuthority.Request(header.clusterId(),header.databaseId(),header.domainId())) || view.head()==null
                || !Arrays.equals(view.head().status().header().encode(),header.encode())) throw new IOException("Replica catalogue authority changed");
    }
    private static void requirePolicy(DistributedSchema.Operator operator,DeterministicContract contract) throws IOException {
        if (contract==null || !contract.admitted() || !contract.identifier().equals(operator.deterministicContract()) || contract.version()!=operator.deterministicContractVersion()) throw new IOException("Operator lacks configured deterministic admission");
    }
    private void finish(Work task,byte[] reply,Throwable failure) {
        synchronized (this) { if (task.finished) return; task.finished=true; work.remove(task.key); heapBytes-=task.charge; if (task.rejection!=null) failure=task.rejection; }
        if (failure==null) task.actual.complete(reply); else task.actual.completeExceptionally(failure);
        dispatch(); closeIfStopped();
    }
    public CompletableFuture<Void> stop() {
        final List<Work> pending; final CompletableFuture<Void> view,actualStop;
        synchronized (this) {
            stopping=true;
            if (stopped.isCompletedExceptionally()) { stopped=new CompletableFuture<>(); closing=false; }
            actualStop=stopped; pending=List.copyOf(work.values());
            if (!stopped.isDone() && stopObservers>=limits.maximumObservers()) return CompletableFuture.failedFuture(new IOException("Schema drain observer capacity exhausted"));
            stopObservers++; view=new CompletableFuture<>();
        }
        actualStop.whenComplete((unused,failure) -> { try { if (failure==null) view.complete(null); else view.completeExceptionally(failure); } finally { synchronized (this) { stopObservers--; } } });
        for (var task:pending) { if (!task.scheduled) task.reject(new RejectedExecutionException("Schema work stopped before execution")); else scheduler.removeQueuedTask(task.executor(),task); }
        closeIfStopped(); return view;
    }
    private void closeIfStopped() {
        final CompletableFuture<Void> actualStop;
        synchronized (this) { if (!stopping || parsing!=0 || !work.isEmpty() || closing) return; closing=true; actualStop=stopped; }
        try { registry.close(); actualStop.complete(null); } catch (Throwable failure) { actualStop.completeExceptionally(failure); }
    }
}
