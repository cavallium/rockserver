package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.OperationFamily;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.RockserverSnapshotRequests;
import org.apache.ratis.statemachine.SnapshotInfo;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Source-owned immutable image leases. Request cancellation never releases an actual capture/chunk RPC. */
public final class RatisTopologySourceSnapshotService implements AutoCloseable {
    public static final int CHUNK_BYTES=1024*1024,MAX_LEASES=4,MAX_RELEASED=1024;
    public static final long MAX_METADATA_BYTES=128L*1024*1024;
    // CRC reader copies, expected sidecar bytes and bounded sidecar comparison during cleanup.
    private static long releaseLookupBytes(Request request){return 4L*maximumPreparedBytes(request);}
    private static long publicationLookupBytes(Request request){return 4L*maximumPublicationBytes(request);}
    static long obsoletePromiseBytes(Request request){return request.purpose()==Purpose.CURRENT_LEAF?128L+2L*(ScopedTransactionRpc.HEADER_BYTES+40L+encodedRequestBytes(request)+NativeRecoverySupersession.MAX_SCOPE_BYTES):0;}
    private static long retainedMetadataBytes(Request request,long capture){return Math.max(capture,obsoletePromiseBytes(request));}
    public record Limits(int maximumLeases,long maximumImageBytes,long maximumReservedImageBytes,long maximumMetadataBytes) {
        public Limits {if(maximumLeases<1 || maximumLeases>MAX_LEASES || maximumImageBytes<1 || maximumReservedImageBytes<maximumImageBytes || maximumMetadataBytes<1 || maximumMetadataBytes>MAX_METADATA_BYTES)throw new IllegalArgumentException("Invalid source snapshot limits");}
        public static Limits defaults(long maximumImageBytes){return new Limits(MAX_LEASES,maximumImageBytes,Math.multiplyExact(MAX_LEASES,maximumImageBytes),MAX_METADATA_BYTES);}
    }
    public record Lease(Prepared prepared,RaftServer.Division source,SnapshotInfo snapshot,AutoCloseable retained) implements AutoCloseable {
        public Lease {
            Objects.requireNonNull(prepared);Objects.requireNonNull(source);Objects.requireNonNull(snapshot);Objects.requireNonNull(retained);
            var image=prepared.image();
            if(!source.getMemberId().getGroupId().getUuid().equals(image.owner().groupId()) || !source.getId().equals(RaftPeerId.valueOf(prepared.sourceOwner().peerId()))
                    || snapshot.getTerm()!=image.position().term() || snapshot.getIndex()!=image.position().index() || snapshot.getFiles().size()!=1 || snapshot.getFiles().getFirst().getFileSize()!=image.bytes())throw new IllegalArgumentException("Source lease changed snapshot/division/image");
        }
        @Override public void close()throws Exception{retained.close();}
    }
    public interface Host {
        default RatisSourceHeadRpc.Head head(RatisSourceHeadRpc.Request request)throws Exception{throw new IOException("Source head owner unavailable");}
        default boolean retryObsolete(RatisRecoveryObsoleteRpc.Request request)throws Exception{return false;}
        default boolean acknowledgeUnownedObsolete(RatisRecoveryObsoleteRpc.Request request)throws Exception{return false;}
        default void requireObsolete(RatisRecoveryObsoleteRpc.Request request)throws Exception{throw new IOException("Obsolete source authority unavailable");}
        default void forceObsolete(RatisRecoveryObsoleteRpc.Request request)throws Exception{throw new IOException("Obsolete source force unavailable");}
        default void obsolete(RatisRecoveryObsoleteRpc.Request request)throws Exception{throw new IOException("Obsolete source cleanup unavailable");}
        Lease capture(Request request,it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.IoAdmission admission)throws Exception;
        default boolean chunkReady(Request request,long bytes)throws IOException{return true;}
        default void transferFinished(Request request){}
        Abandoned abandoned(Request request)throws Exception;
        Abandoned abandon(Request request)throws Exception;
        org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Gate.SnapshotPermit requireOwner(Lease lease)throws Exception;
        void requireReceiverInstalled(Lease lease)throws Exception;
        void released(Lease lease)throws Exception;
        boolean isReleased(Request request)throws Exception;
        InstallSnapshotReplyProto send(Request scope,InstallSnapshotRequestProto request)throws IOException;
    }
    /** Minimum policy capacity for the bounded relocation request class; each task charges transferBytes(request). */
    public static final long TRANSFER_BYTES=256+4L*(MAX_REQUEST_BYTES-RatisCatalogue.MAX_DIRECTORY_BYTES+MAX_OWNER_BYTES+177)+4L*(CHUNK_BYTES+MAX_REQUEST_BYTES-RatisCatalogue.MAX_DIRECTORY_BYTES+MAX_REPLY_BYTES-RatisCatalogue.MAX_DIRECTORY_BYTES);
    static long transferBytes(Request request){return 256+publicationLookupBytes(request)+4L*(CHUNK_BYTES+encodedRequestBytes(request)+maximumPreparedBytes(request));}
    @FunctionalInterface public interface ResourceAdmission {RatisProofQueryService.ResourceLease reserve(Request request,long bytes,boolean control)throws IOException;}
    public record Stats(int leases,int active,long reservedImageBytes,long metadataBytes,int released) {}
    private static final class Entry {
        final Request request; long imageCharge,metadataCharge; Lease lease; boolean busy=true,installed,unusable;ObsoleteOwner obsolete;
        Entry(Request request,long imageCharge,long metadataCharge){this.request=request;this.imageCharge=imageCharge;this.metadataCharge=metadataCharge;}
    }
    private final Host host;private final Limits limits;private final ResourceAdmission admission;private final RWScheduler.WorkloadExecutor executor;
    private final ScheduledExecutorService rateWakeup=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon(true).name("ratis-migration-rate").factory());
    private final Set<Preparation> preparations=new LinkedHashSet<>();
    private final Set<Transfer> transfers=new LinkedHashSet<>();private final CompletableFuture<Void> stopped=new CompletableFuture<>();
    private final Map<UUID,Entry> entries=new LinkedHashMap<>();private final Map<UUID,byte[]> released=new HashMap<>();private final Set<UUID> releaseLookups=new HashSet<>();
    private static final int OBSOLETE_TERMINAL_BYTES=128;
    private final Map<UUID,ObsoleteOwner> obsoletePending=new HashMap<>();private final Map<UUID,byte[]> obsoleteTerminals=new HashMap<>();
    // At most MAX_LEASES transient controls, separately admitted by their protected graph leases.
    private long imageBytes,metadataBytes,obsoleteControlBytes;private int active;private boolean stopping;
    public RatisTopologySourceSnapshotService(Host host,Limits limits,RWScheduler scheduler,ResourceAdmission admission){this.host=Objects.requireNonNull(host);this.limits=Objects.requireNonNull(limits);this.admission=Objects.requireNonNull(admission);executor=Objects.requireNonNull(scheduler).executor(RequestContext.batch(),OperationFamily.FULL_SCAN_AGGREGATE);}
    /** The actual image write/inspect/hash runs in bounded BATCH work, outside the proof/control worker. */
    public CompletableFuture<Prepared> prepareAsync(Request request){
        final Preparation task;final RatisProofQueryService.ResourceLease resource;
        try{resource=Objects.requireNonNull(admission.reserve(request,transferBytes(request),false));}catch(Exception failure){return CompletableFuture.failedFuture(failure);}
        synchronized(this){if(stopping || preparations.size()>=MAX_LEASES || request.purpose()==Purpose.CURRENT_LEAF && !hasTerminalSlot(request.transferId()) || qualifiedObsolete(request.transferId()) || obsoleteTerminals.containsKey(request.transferId())){resource.close();return CompletableFuture.failedFuture(new IOException("Source image build admission exhausted"));}task=new Preparation(request,resource);preparations.add(task);}
        try{task.handle=executor.executeCooperatively(task,transferBytes(request));boolean stop;synchronized(this){stop=stopping || qualifiedObsolete(request.transferId());}if(stop)task.handle.cancel();}catch(RuntimeException rejected){task.reject(rejected);}
        return task.result.copy();
    }
    /** A bounded native owner waits off the scheduler worker; each IO quantum still enters the BATCH lane. */
    private final class Preparation implements RWScheduler.CooperativeCompletionTask {
        final Request request;final RatisProofQueryService.ResourceLease resource;final CompletableFuture<Prepared> result=new CompletableFuture<>();final CompletableFuture<Void> drained=new CompletableFuture<>();final Semaphore permit=new Semaphore(0);
        volatile RWScheduler.CooperativeHandle handle;boolean started,actualFinished,logicalFinished,settled;int requestedBytes;Prepared value;Throwable failure;
        Preparation(Request request,RatisProofQueryService.ResourceLease resource){this.request=request;this.resource=resource;}
        public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context){
            boolean start;int bytes;
            synchronized(RatisTopologySourceSnapshotService.this){start=!started;if(start)started=true;bytes=requestedBytes;if(actualFinished){if(failure!=null)context.fail(failure instanceof RuntimeException runtime?runtime:new CompletionException(failure));return RWScheduler.CooperativeResult.COMPLETE;}}
            if(start)try{Thread.ofVirtual().name("ratis-source-image-owner").start(this::build);}catch(RuntimeException rejected){synchronized(RatisTopologySourceSnapshotService.this){actualFinished=true;failure=rejected;}context.fail(rejected);return RWScheduler.CooperativeResult.COMPLETE;}
            if(bytes>0){try{if(!host.chunkReady(request,bytes)){rateWakeup.schedule(this::resume,10,TimeUnit.MILLISECONDS);return RWScheduler.CooperativeResult.PARK;}}
                catch(Exception failed){context.fail(new CompletionException(failed));return RWScheduler.CooperativeResult.COMPLETE;}
                synchronized(RatisTopologySourceSnapshotService.this){requestedBytes=0;}permit.release();
            }
            return RWScheduler.CooperativeResult.PARK;
        }
        private void build(){try{value=prepareOwned(request,this::beforeBytes);}catch(Throwable failed){synchronized(RatisTopologySourceSnapshotService.this){if(failure==null)failure=failed;}}finally{synchronized(RatisTopologySourceSnapshotService.this){actualFinished=true;}resume();settle();}}
        private void beforeBytes(int bytes)throws IOException{
            synchronized(RatisTopologySourceSnapshotService.this){requireOpen();requireNotReleased(request);if(logicalFinished)throw new IOException("Source image submission terminated",failure);requestedBytes=bytes;}
            resume();try{permit.acquire();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Source image owner interrupted",interrupted);}
            synchronized(RatisTopologySourceSnapshotService.this){requireOpen();requireNotReleased(request);if(logicalFinished)throw new IOException("Source image submission terminated",failure);}
        }
        private void resume(){RWScheduler.CooperativeHandle current;synchronized(RatisTopologySourceSnapshotService.this){if(logicalFinished)return;current=handle;}if(current==null)rateWakeup.schedule(this::resume,10,TimeUnit.MILLISECONDS);else current.resume();}
        public void completeCooperatively(){synchronized(RatisTopologySourceSnapshotService.this){logicalFinished=true;}settle();}
        public void reject(RuntimeException rejected){synchronized(RatisTopologySourceSnapshotService.this){if(failure==null)failure=rejected;logicalFinished=true;if(!started)actualFinished=true;}permit.release();settle();}
        private void settle(){Throwable problem;Prepared prepared;synchronized(RatisTopologySourceSnapshotService.this){if(settled || !logicalFinished || !actualFinished)return;settled=true;problem=failure;prepared=value;}
            resource.close();synchronized(RatisTopologySourceSnapshotService.this){preparations.remove(this);}drained.complete(null);checkStopped();if(problem==null)result.complete(prepared);else result.completeExceptionally(problem);
        }
    }
    public synchronized Stats stats(){return new Stats(entries.size(),active,imageBytes,Math.addExact(metadataBytes,obsoleteControlBytes),released.size());}
    private void awaitImageBytes(Request request,int bytes)throws IOException {
        while(true){synchronized(this){requireOpen();}if(host.chunkReady(request,bytes))return;
            try{Thread.sleep(10);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Source image admission interrupted",interrupted);}
        }
    }
    public RatisSourceHeadRpc.Head head(RatisSourceHeadRpc.Request request)throws Exception{synchronized(this){if(stopping)throw new IOException("Source service stopped");}return host.head(request);}
    Prepared prepare(Request request)throws IOException {return prepareOwned(request,bytes->awaitImageBytes(request,bytes));}
    private Prepared prepareOwned(Request request,it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.IoAdmission imageAdmission)throws IOException {
        Objects.requireNonNull(request);Entry entry;boolean capture;
        synchronized(this){
            requireOpen();requireNotReleased(request);if(releaseLookups.contains(request.transferId()))throw new IOException("Durable release lookup still owns this transfer");entry=entries.get(request.transferId());
            if(entry!=null){requireSame(entry,request);if(entry.unusable)throw new IOException("Failed source preparation requires cleanup drainage");if(entry.busy)throw new IOException("Source transfer owner is active");entry.busy=true;active++;capture=entry.lease==null;}
            else {
                long charge=retainedMetadataBytes(request,publicationLookupBytes(request));
                if(active>=MAX_LEASES || entries.size()>=limits.maximumLeases()+MAX_LEASES || !hasTerminalSlot(request.transferId()) || charge>limits.maximumMetadataBytes()-metadataBytes)throw new IOException("Source publication lookup admission exhausted");
                entry=new Entry(request,0,charge);entries.put(request.transferId(),entry);metadataBytes+=charge;active++;capture=true;
            }
        }
        IOException failure=null;
        try {
            if(capture){
                var terminal=host.abandoned(request);if(terminal!=null)throw new AbandonedException(terminal);
                synchronized(this){if(entry.imageCharge==0){long charge=retainedMetadataBytes(request,3L*maximumPreparedBytes(request));long captures=entries.values().stream().filter(value->value.imageCharge!=0).count();if(captures>=limits.maximumLeases() || limits.maximumImageBytes()>limits.maximumReservedImageBytes()-imageBytes || charge-entry.metadataCharge>limits.maximumMetadataBytes()-metadataBytes)throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Source snapshot admission exhausted");requireOpen();entry.imageCharge=limits.maximumImageBytes();imageBytes+=entry.imageCharge;metadataBytes+=charge-entry.metadataCharge;entry.metadataCharge=charge;}}
                Lease candidate=null;
                try {candidate=Objects.requireNonNull(host.capture(request,imageAdmission));requireRequest(request,candidate.prepared().request());
                    if(candidate.prepared().image().bytes()>limits.maximumImageBytes())throw new IOException("Captured source image exceeds reserved quota");
                    host.requireOwner(candidate);long charge=retainedMetadataBytes(request,3L*preparedBytes(candidate.prepared()));
                    synchronized(this){entry.lease=candidate;metadataBytes+=charge-entry.metadataCharge;entry.metadataCharge=charge;candidate=null;}
                }finally{if(candidate!=null)try{candidate.close();}catch(Exception cleanup){synchronized(this){entry.lease=candidate;entry.unusable=true;}throw cleanup;}}
            }else host.requireOwner(entry.lease);
            synchronized(this){requireOpen();}
            return entry.lease.prepared();
        }catch(Exception problem){
            if(problem instanceof RatisTopologySourceSnapshotArtifacts.OriginalCutUnavailable){try{problem=new AbandonedException(host.abandon(request));}catch(Exception qualification){problem.addSuppressed(qualification);}}
            if(problem instanceof AbandonedException || problem instanceof RatisTopologySourceSnapshotArtifacts.AdmissionRejected){synchronized(this){if(entry.lease==null && entries.remove(request.transferId(),entry)){imageBytes-=entry.imageCharge;metadataBytes-=removedEntryMetadata(entry);}}}
            failure=problem instanceof AbandonedException terminal?terminal:io("Source snapshot preparation failed; original owner retained",problem);throw failure;}
        finally{finish(entry,failure);}
    }
    public CompletableFuture<Prepared> transfer(Request request) {
        RatisProofQueryService.ResourceLease resource=null;Transfer task=null;
        try {
            Objects.requireNonNull(request);resource=Objects.requireNonNull(admission.reserve(request,transferBytes(request),false));
            synchronized(this){requireOpen();requireNotReleased(request);var entry=entries.get(request.transferId());if(entry!=null){requireSame(entry,request);if(entry.busy)throw new IOException("Source transfer owner is active");}
                if(active>=MAX_LEASES || publicationLookupBytes(request)>limits.maximumMetadataBytes()-metadataBytes || !releaseLookups.add(request.transferId()))throw new IOException("Source transfer lookup admission exhausted");
                active++;metadataBytes+=publicationLookupBytes(request);task=new Transfer(request,resource);transfers.add(task);resource=null;
            }
            try{var handle=executor.executeCooperatively(task,transferBytes(request));task.handle=handle;boolean stop;synchronized(this){stop=stopping || qualifiedObsolete(request.transferId());}if(stop)handle.cancel();}catch(RuntimeException rejected){task.reject(rejected);}
            return task.result.copy();
        }catch(Exception failed){if(resource!=null)resource.close();return CompletableFuture.failedFuture(failed);}
    }
    private final class Transfer implements RWScheduler.CooperativeCompletionTask {
        final Request request;final RatisProofQueryService.ResourceLease resource;final CompletableFuture<Prepared> result=new CompletableFuture<>();final CompletableFuture<Void> drained=new CompletableFuture<>();
        volatile RWScheduler.CooperativeHandle handle;Entry entry;RockserverSnapshotRequests.Requests chunks;boolean lookup=true,terminal,settled,settling;Exception failure;
        Transfer(Request request,RatisProofQueryService.ResourceLease resource){this.request=request;this.resource=resource;}
        @Override public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context){
            try {
                synchronized(RatisTopologySourceSnapshotService.this){requireOpen();}
                if(context.terminationRequested())throw context.terminationFailure();
                if(lookup){var abandoned=host.abandoned(request);if(abandoned!=null)throw new AbandonedException(abandoned);
                    synchronized(RatisTopologySourceSnapshotService.this){entry=entries.get(request.transferId());if(entry==null || entry.lease==null || entry.unusable)throw new IOException("Prepare the exact source image first");requireSame(entry,request);if(entry.busy)throw new IOException("Source transfer owner is active");entry.busy=true;releaseLookups.remove(request.transferId());metadataBytes-=publicationLookupBytes(request);lookup=false;}
                }
                var sourcePermit=host.requireOwner(entry.lease);
                if(entry.installed)return RWScheduler.CooperativeResult.COMPLETE;
                if(chunks==null)chunks=RockserverSnapshotRequests.openSource(entry.lease.source(),RaftPeerId.valueOf(request.receiver().peerId()),request.transferId().toString(),entry.lease.snapshot(),CHUNK_BYTES,sourcePermit);
                if(!chunks.hasNext())throw new IOException("Snapshot producer did not yield a final receiver response");
                if(!host.chunkReady(request,CHUNK_BYTES)){rateWakeup.schedule(this::resumeRate,10,TimeUnit.MILLISECONDS);return RWScheduler.CooperativeResult.PARK;}
                var chunk=chunks.next();var reply=host.send(request,chunk);requireReply(chunk,reply);host.requireOwner(entry.lease);
                synchronized(RatisTopologySourceSnapshotService.this){requireOpen();}
                if(reply.getResult()!=InstallSnapshotResult.ALREADY_INSTALLED && !chunk.getSnapshotChunk().getDone())return RWScheduler.CooperativeResult.YIELD;
                closeReader();
                host.requireReceiverInstalled(entry.lease);host.requireOwner(entry.lease);return RWScheduler.CooperativeResult.COMPLETE;
            }catch(Exception problem){failure=problem;context.fail(problem instanceof RuntimeException runtime?runtime:new CompletionException(problem));return RWScheduler.CooperativeResult.COMPLETE;}
        }
        private void resumeRate(){RWScheduler.CooperativeHandle current;synchronized(RatisTopologySourceSnapshotService.this){if(terminal)return;current=handle;}if(current==null)rateWakeup.schedule(this::resumeRate,10,TimeUnit.MILLISECONDS);else current.resume();}
        private void closeReader()throws IOException{if(chunks!=null){chunks.close();chunks=null;}}
        @Override public void completeCooperatively(){synchronized(RatisTopologySourceSnapshotService.this){if(stopping && failure==null)failure=new IOException("Source transfer owner stopping");if(failure==null)entry.installed=true;}settle();}
        @Override public void reject(RuntimeException rejected){synchronized(RatisTopologySourceSnapshotService.this){if(failure==null)failure=rejected;}settle();}
        void settle(){
            synchronized(RatisTopologySourceSnapshotService.this){if(settled || settling)return;settling=true;terminal=true;}
            try {closeReader();}
            catch(IOException unknown){synchronized(RatisTopologySourceSnapshotService.this){if(entry!=null)entry.unusable=true;settling=false;}result.completeExceptionally(unknown);return;}
            IOException problem=failure==null?null:io("Source transfer outcome unresolved; exact lease retained",failure);
            try {
                if(lookup){synchronized(RatisTopologySourceSnapshotService.this){releaseLookups.remove(request.transferId());metadataBytes-=publicationLookupBytes(request);active--;lookup=false;RatisTopologySourceSnapshotService.this.notifyAll();}}
                else finish(entry,problem);
            }catch(IOException cleanup){if(problem==null)problem=cleanup;else problem.addSuppressed(cleanup);}
            synchronized(RatisTopologySourceSnapshotService.this){settled=true;}
            try {if(problem==null)result.complete(entry.lease.prepared());else result.completeExceptionally(problem);}finally{try{host.transferFinished(request);}finally{resource.close();synchronized(RatisTopologySourceSnapshotService.this){transfers.remove(this);settling=false;}drained.complete(null);checkStopped();}}
        }
    }
    /** A qualified negative fence cancels old work, joins actual owners, and never claims image installation. */
    public CompletableFuture<byte[]> obsolete(RatisRecoveryObsoleteRpc.Request request) {
        Objects.requireNonNull(request);var original=request.original();RatisProofQueryService.ResourceLease resource=null;ObsoleteTask task;
        long weight=Math.addExact(4096L,Math.multiplyExact(8L,request.encodedSize()));long control=Math.addExact(128L,Math.multiplyExact(2L,request.encodedSize()));
        try {
            byte[] identity;
            synchronized(this){var retained=obsoletePending.get(original.transferId());if(retained!=null && retained.busy)throw new IOException("Actual obsolete cleanup is active");identity=retained==null?null:request.digest();}
            if(identity==null){resource=Objects.requireNonNull(admission.reserve(original,weight,true));identity=request.digest();}
            synchronized(this) {
                var terminal=obsoleteTerminals.get(original.transferId());if(terminal!=null){if(!Arrays.equals(terminal,identity))throw new IOException("Obsolete transfer changed exact terminal request");if(resource!=null)resource.close();return CompletableFuture.completedFuture(ScopedTransactionRpc.frame(RatisRecoveryObsoleteRpc.KIND,identity));}
                if(released.containsKey(original.transferId()))throw new IOException("Installed RELEASE cannot become OBSOLETE");
                var owner=obsoletePending.get(original.transferId());
                if(owner==null){requireOpen();if(obsoletePending.size()>=MAX_LEASES)throw new IOException("Obsolete control admission exhausted");
                    var entry=entries.get(original.transferId());if(entry!=null)requireSame(entry,original);
                    owner=new ObsoleteOwner(request,identity,resource);owner.controlCharge=control;owner.funding=entry;obsoletePending.put(original.transferId(),owner);obsoleteControlBytes=Math.addExact(obsoleteControlBytes,control);resource=null;
                }else if(!Arrays.equals(owner.identity,identity))throw new IOException("Obsolete source requires original pending request");
                if(owner.busy || obsoletePending.values().stream().filter(value->value.busy).count()>=MAX_LEASES)throw new IOException("Actual obsolete cleanup admission is active/full");owner.busy=true;active++;task=new ObsoleteTask(owner);
            }
            if(resource!=null){resource.close();resource=null;}
            try{executor.execute(task,weight);}catch(RuntimeException rejected){task.reject(rejected);}return task.result.copy();
        }catch(Exception failure){if(resource!=null)resource.close();return CompletableFuture.failedFuture(failure);}
    }
    private final class ObsoleteOwner {
        final RatisRecoveryObsoleteRpc.Request request;final byte[] identity;long charge,controlCharge;final RatisProofQueryService.ResourceLease resource;
        Entry entry,funding;boolean busy,qualified,forcePossible,leaseClosed,leaseClaimed,permanent,absent;
        ObsoleteOwner(RatisRecoveryObsoleteRpc.Request request,byte[] identity,RatisProofQueryService.ResourceLease resource){this.request=request;this.identity=identity;this.resource=resource;}
    }
    private final class ObsoleteTask implements Runnable,RWScheduler.RejectionAwareTask {
        final ObsoleteOwner owner;final CompletableFuture<byte[]> result=new CompletableFuture<>();final java.util.concurrent.atomic.AtomicInteger phase=new java.util.concurrent.atomic.AtomicInteger();
        ObsoleteTask(ObsoleteOwner owner){this.owner=owner;}
        public void run(){if(!phase.compareAndSet(0,1))return;try{Thread.ofVirtual().name("ratis-obsolete-source-owner").start(this::actual);}catch(RuntimeException rejected){finish(null,rejected);}}
        private void actual(){Throwable failure=null;byte[] reply=null;
            try {
                owner.absent=false;boolean durable;
                try{durable=host.retryObsolete(owner.request);if(durable)owner.forcePossible=true;}
                catch(RatisTopologySourceSnapshotArtifacts.AdmissionRejected conflict){owner.forcePossible=false;throw conflict;}
                catch(Exception unknown){owner.forcePossible=true;throw unknown;}
                if(durable)admitPermanentObsolete(owner);
                if(!durable && !owner.qualified)host.requireObsolete(owner.request);
                var original=owner.request.original();List<Preparation> builds;List<Transfer> chunks;
                synchronized(RatisTopologySourceSnapshotService.this) {
                    if(released.containsKey(original.transferId()))throw new IOException("Installed RELEASE already owns source capture");
                    owner.qualified=true;owner.entry=entries.get(original.transferId());if(owner.entry!=null){requireSame(owner.entry,original);owner.entry.obsolete=owner;}
                    builds=preparations.stream().filter(build->build.request.transferId().equals(original.transferId())).toList();chunks=transfers.stream().filter(chunk->chunk.request.transferId().equals(original.transferId())).toList();
                }
                for(var build:builds){var handle=build.handle;if(handle!=null)handle.cancel();}
                for(var chunk:chunks){var handle=chunk.handle;if(handle!=null)handle.cancel();}
                CompletableFuture.allOf(java.util.stream.Stream.concat(builds.stream().map(build->build.drained),chunks.stream().map(chunk->chunk.drained)).toArray(CompletableFuture[]::new)).get();
                synchronized(RatisTopologySourceSnapshotService.this){
                    while(releaseLookups.contains(original.transferId()) || entries.get(original.transferId())!=null && entries.get(original.transferId()).busy)RatisTopologySourceSnapshotService.this.wait();
                    if(released.containsKey(original.transferId()))throw new IOException("Installed RELEASE already settled source capture");
                    owner.entry=entries.get(original.transferId());if(owner.entry!=null){owner.entry.obsolete=owner;owner.entry.busy=true;owner.leaseClaimed=true;}
                }
                if(!durable && (owner.entry==null || owner.entry.lease==null)){
                    boolean previous=owner.forcePossible;owner.forcePossible=true;
                    try{owner.absent=host.acknowledgeUnownedObsolete(owner.request);owner.forcePossible=previous;}
                    catch(RatisTopologySourceSnapshotArtifacts.AdmissionRejected conflict){owner.forcePossible=false;throw conflict;}
                }
                if(!owner.absent){
                    admitPermanentObsolete(owner);
                    if(!durable){owner.forcePossible=true;try{host.forceObsolete(owner.request);}catch(RatisTopologySourceSnapshotArtifacts.AdmissionRejected conflict){owner.forcePossible=false;throw conflict;}}
                    if(owner.entry!=null && owner.entry.lease!=null && !owner.leaseClosed){owner.entry.lease.close();owner.leaseClosed=true;}
                    host.obsolete(owner.request);
                }
                reply=ScopedTransactionRpc.frame(RatisRecoveryObsoleteRpc.KIND,owner.identity);
            }catch(Throwable problem){if(problem instanceof InterruptedException)Thread.currentThread().interrupt();failure=problem;}finish(reply,failure);
        }
        public void reject(RuntimeException failure){if(phase.compareAndSet(0,2))finish(null,failure);}
        private void finish(byte[] reply,Throwable failure) {
            boolean success=failure==null,discard=!owner.forcePossible;
            if(success || discard)try{owner.resource.close();}catch(Throwable cleanup){failure=cleanup;success=false;discard=false;}
            synchronized(RatisTopologySourceSnapshotService.this){owner.busy=false;active--;if(owner.entry!=null){if(owner.leaseClaimed)owner.entry.busy=false;if(discard && owner.entry.obsolete==owner)owner.entry.obsolete=null;}
                if(success || discard){obsoletePending.remove(owner.request.original().transferId(),owner);metadataBytes-=owner.charge;obsoleteControlBytes-=owner.controlCharge;owner.controlCharge=0;}
                if(success){if(!owner.absent){obsoleteTerminals.put(owner.request.original().transferId(),owner.identity);metadataBytes+=OBSOLETE_TERMINAL_BYTES;}if(owner.entry!=null && entries.remove(owner.entry.request.transferId(),owner.entry)){imageBytes-=owner.entry.imageCharge;metadataBytes-=owner.entry.metadataCharge;}}
                RatisTopologySourceSnapshotService.this.notifyAll();
            }
            checkStopped();if(success)result.complete(reply);else result.completeExceptionally(failure);
        }
    }
    private synchronized void admitPermanentObsolete(ObsoleteOwner owner)throws IOException {
        if(owner.permanent)return;UUID id=owner.request.original().transferId();
        if(!hasTerminalSlot(id))throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Obsolete permanent identity quota exhausted");
        long needed=Math.addExact(OBSOLETE_TERMINAL_BYTES,Math.multiplyExact(2L,owner.request.encodedSize()));
        long funding=owner.charge+(owner.funding==null?0:obsoletePromiseBytes(owner.funding.request));long charge=Math.max(0,needed-funding);
        if(charge>limits.maximumMetadataBytes()-metadataBytes)throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Obsolete permanent metadata quota exhausted");
        owner.charge=Math.addExact(owner.charge,charge);metadataBytes=Math.addExact(metadataBytes,charge);owner.permanent=true;obsoleteControlBytes-=owner.controlCharge;owner.controlCharge=0;
    }
    /** A naturally retiring capture transfers prepaid terminal credit to its still-qualifying owner. */
    private long removedEntryMetadata(Entry entry) {
        var owner=obsoletePending.get(entry.request.transferId());long transfer=owner!=null && owner.funding==entry?Math.min(entry.metadataCharge,obsoletePromiseBytes(entry.request)):0;
        if(transfer!=0){owner.charge+=transfer;owner.funding=null;}return entry.metadataCharge-transfer;
    }
    private boolean qualifiedObsolete(UUID id){var owner=obsoletePending.get(id);return owner!=null && (owner.qualified || owner.forcePossible);}
    private boolean hasTerminalSlot(UUID id) {
        var identities=new HashSet<>(released.keySet());for(var owner:obsoletePending.values())if(owner.permanent)identities.add(owner.request.original().transferId());identities.addAll(obsoleteTerminals.keySet());identities.addAll(releaseLookups);
        for(var entry:entries.values())if(entry.request.purpose()==Purpose.CURRENT_LEAF)identities.add(entry.request.transferId());for(var build:preparations)if(build.request.purpose()==Purpose.CURRENT_LEAF)identities.add(build.request.transferId());
        return identities.contains(id) || identities.size()<MAX_RELEASED;
    }
    public void release(Request request)throws IOException {
        try(var resource=admission.reserve(request,releaseLookupBytes(request),true)){releaseOwned(request);}
    }
    private void releaseOwned(Request request)throws IOException {
        Entry entry;
        synchronized(this){
            if(qualifiedObsolete(request.transferId()) || obsoleteTerminals.containsKey(request.transferId()))throw new IOException("Obsolete capture cannot use installed RELEASE");
            if(releaseLookups.contains(request.transferId()))throw new IOException("Source publication lookup still owns this transfer");
            byte[] prior=released.get(request.transferId());if(prior!=null){requireReleased(request,prior);return;}
            entry=entries.get(request.transferId());
            if(entry!=null){requireSame(entry,request);if(entry.busy || !entry.installed)throw new IOException("Source snapshot has no settled receiver installation");entry.busy=true;active++;}
            else {requireOpen();if(active>=MAX_LEASES || !hasTerminalSlot(request.transferId()) || releaseLookupBytes(request)>limits.maximumMetadataBytes()-metadataBytes || !releaseLookups.add(request.transferId()))throw new IOException("Durable release lookup capacity/ownership unavailable");active++;metadataBytes+=releaseLookupBytes(request);}
        }
        if(entry!=null){retire(entry,true);return;}
        try {
            // This hook only reads a forced receipt; absent input cannot gain release authority.
            if(!host.isReleased(request))throw new IOException("Unknown source snapshot lease/release receipt");
            synchronized(this){released.put(request.transferId(),digest(request));}
        }catch(Exception failure){throw io("Durable source release retry failed",failure);}
        finally{synchronized(this){releaseLookups.remove(request.transferId());active--;metadataBytes-=releaseLookupBytes(request);notifyAll();}checkStopped();}
    }
    private void finish(Entry entry,IOException failure)throws IOException {
        boolean close;
        synchronized(this){entry.busy=false;active--;close=stopping && !qualifiedObsolete(entry.request.transferId());notifyAll();}
        if(close){synchronized(this){if(entry.busy || entries.get(entry.request.transferId())!=entry)return;entry.busy=true;active++;}try{retire(entry,false);}catch(IOException cleanup){if(failure!=null)failure.addSuppressed(cleanup);else throw cleanup;}}
        checkStopped();
    }
    private void retire(Entry entry,boolean remember)throws IOException {
        boolean success=false;
        try{if(entry.lease!=null){if(remember)host.released(entry.lease);entry.lease.close();}success=true;}
        catch(Exception problem){throw io("Source snapshot cleanup failed; lease retained",problem);}
        finally{synchronized(this){active--;entry.busy=false;if(success){entries.remove(entry.request.transferId(),entry);imageBytes-=entry.imageCharge;metadataBytes-=removedEntryMetadata(entry);if(remember)released.put(entry.request.transferId(),digest(entry.request));}notifyAll();}checkStopped();}
    }
    public CompletableFuture<Void> stop(){
        List<Transfer> tasks;List<Preparation> builds;List<Entry> idle;List<RatisRecoveryObsoleteRpc.Request> pendingObsolete;
        synchronized(this){stopping=true;tasks=List.copyOf(transfers);builds=List.copyOf(preparations);idle=entries.values().stream().filter(entry->!entry.busy && !qualifiedObsolete(entry.request.transferId())).toList();pendingObsolete=obsoletePending.values().stream().filter(owner->!owner.busy).map(owner->owner.request).toList();for(var entry:idle){entry.busy=true;active++;}}
        for(var build:builds){var handle=build.handle;if(handle!=null)handle.cancel();}
        for(var task:tasks){var handle=task.handle;if(handle!=null)handle.cancel();if(task.terminal)task.settle();}
        for(var entry:idle)try{retire(entry,false);}catch(IOException unknown){/* Retryable close retains the exact lease and its image quota. */}
        for(var request:pendingObsolete)obsolete(request);
        checkStopped();return stopped.copy();
    }
    private void checkStopped(){boolean complete;synchronized(this){complete=stopping && active==0 && entries.isEmpty() && transfers.isEmpty() && preparations.isEmpty() && obsoletePending.isEmpty();if(complete){metadataBytes-=OBSOLETE_TERMINAL_BYTES*(long)obsoleteTerminals.size();obsoleteTerminals.clear();}}if(complete){rateWakeup.shutdown();stopped.complete(null);}}
    @Override public void close()throws IOException{var completion=stop();if(!completion.isDone())throw new IOException("Source snapshot stop waits for actual capture/transfer/cleanup drainage");try{completion.join();}catch(CompletionException failed){throw new IOException("Source stop did not settle",failed.getCause());}}
    static void requireReply(InstallSnapshotRequestProto request,InstallSnapshotReplyProto reply)throws IOException {
        var expected=request.getServerRequest();
        if(reply==null || !reply.hasServerReply() || !reply.getServerReply().getSuccess() || !reply.hasRequestIndex() || reply.getRequestIndex()!=request.getSnapshotChunk().getRequestIndex()
                || reply.getTerm()!=request.getLeaderTerm() || !reply.getServerReply().getRequestorId().equals(expected.getRequestorId()) || !reply.getServerReply().getReplyId().equals(expected.getReplyId())
                || !reply.getServerReply().getRaftGroupId().equals(expected.getRaftGroupId()) || reply.getResult()!=InstallSnapshotResult.SUCCESS && reply.getResult()!=InstallSnapshotResult.ALREADY_INSTALLED)throw new IOException("Unknown, stale or crossed snapshot receiver reply");
    }
    private void requireOpen()throws IOException{if(stopping)throw new IOException("Source snapshot owner is stopping");}
    private void requireNotReleased(Request request)throws IOException{if(qualifiedObsolete(request.transferId()) || obsoleteTerminals.containsKey(request.transferId()))throw new IOException("Source capture is permanently obsolete or awaits exact obsolete cleanup");var prior=released.get(request.transferId());if(prior!=null){requireReleased(request,prior);throw new IOException("Source snapshot already released");}}
    private static void requireSame(Entry entry,Request request)throws IOException{try{requireRequest(entry.request,request);}catch(IllegalArgumentException changed){throw new IOException("Transfer ID already owns different input",changed);}}
    private static void requireReleased(Request request,byte[] expected)throws IOException{if(!Arrays.equals(expected,digest(request)))throw new IOException("Released transfer input changed");}
    private static byte[] digest(Request request){try{return java.security.MessageDigest.getInstance("SHA-256").digest(RatisTopologySourceSnapshotRpc.request(RELEASE,request));}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    private static IOException io(String message,Exception cause){return cause instanceof IOException failure?failure:new IOException(message,cause);}
}
