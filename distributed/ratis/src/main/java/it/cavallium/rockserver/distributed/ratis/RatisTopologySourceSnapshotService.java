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
    private static final long RELEASE_LOOKUP_BYTES=4L*MAX_REPLY_BYTES;
    private static final long PUBLICATION_LOOKUP_BYTES=4L*(MAX_REQUEST_BYTES+MAX_OWNER_BYTES+177);
    public record Limits(int maximumLeases,long maximumImageBytes,long maximumReservedImageBytes,long maximumMetadataBytes) {
        public Limits {if(maximumLeases<1 || maximumLeases>MAX_LEASES || maximumImageBytes<1 || maximumReservedImageBytes<maximumImageBytes || maximumMetadataBytes<3L*MAX_REPLY_BYTES || maximumMetadataBytes>MAX_METADATA_BYTES)throw new IllegalArgumentException("Invalid source snapshot limits");}
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
        Lease capture(Request request)throws Exception;
        Abandoned abandoned(Request request)throws Exception;
        Abandoned abandon(Request request)throws Exception;
        void requireOwner(Lease lease)throws Exception;
        void requireReceiverInstalled(Lease lease)throws Exception;
        void released(Lease lease)throws Exception;
        boolean isReleased(Request request)throws Exception;
        InstallSnapshotReplyProto send(InstallSnapshotRequestProto request)throws IOException;
    }
    public static final long TRANSFER_BYTES=256+PUBLICATION_LOOKUP_BYTES+4L*(CHUNK_BYTES+MAX_REQUEST_BYTES+MAX_REPLY_BYTES);
    @FunctionalInterface public interface ResourceAdmission {RatisProofQueryService.ResourceLease reserve(long bytes)throws IOException;}
    public record Stats(int leases,int active,long reservedImageBytes,long metadataBytes,int released) {}
    private static final class Entry {
        final Request request; long imageCharge,metadataCharge; Lease lease; boolean busy=true,installed,unusable;
        Entry(Request request,long imageCharge,long metadataCharge){this.request=request;this.imageCharge=imageCharge;this.metadataCharge=metadataCharge;}
    }
    private final Host host;private final Limits limits;private final ResourceAdmission admission;private final RWScheduler.WorkloadExecutor executor;
    private final Set<Transfer> transfers=new LinkedHashSet<>();private final CompletableFuture<Void> stopped=new CompletableFuture<>();
    private final Map<UUID,Entry> entries=new LinkedHashMap<>();private final Map<UUID,byte[]> released=new HashMap<>();private final Set<UUID> releaseLookups=new HashSet<>();
    private long imageBytes,metadataBytes;private int active;private boolean stopping;
    public RatisTopologySourceSnapshotService(Host host,Limits limits,RWScheduler scheduler,ResourceAdmission admission){this.host=Objects.requireNonNull(host);this.limits=Objects.requireNonNull(limits);this.admission=Objects.requireNonNull(admission);executor=Objects.requireNonNull(scheduler).executor(RequestContext.batch(),OperationFamily.FULL_SCAN_AGGREGATE);}
    public synchronized Stats stats(){return new Stats(entries.size(),active,imageBytes,metadataBytes,released.size());}
    public Prepared prepare(Request request)throws IOException {
        Objects.requireNonNull(request);Entry entry;boolean capture;
        synchronized(this){
            requireOpen();requireNotReleased(request);if(releaseLookups.contains(request.transferId()))throw new IOException("Durable release lookup still owns this transfer");entry=entries.get(request.transferId());
            if(entry!=null){requireSame(entry,request);if(entry.unusable)throw new IOException("Failed source preparation requires cleanup drainage");if(entry.busy)throw new IOException("Source transfer owner is active");entry.busy=true;active++;capture=entry.lease==null;}
            else {
                long charge=PUBLICATION_LOOKUP_BYTES;
                if(active>=MAX_LEASES || entries.size()>=limits.maximumLeases()+MAX_LEASES || released.size()>=MAX_RELEASED || charge>limits.maximumMetadataBytes()-metadataBytes)throw new IOException("Source publication lookup admission exhausted");
                entry=new Entry(request,0,charge);entries.put(request.transferId(),entry);metadataBytes+=charge;active++;capture=true;
            }
        }
        IOException failure=null;
        try {
            if(capture){
                var terminal=host.abandoned(request);if(terminal!=null)throw new AbandonedException(terminal);
                synchronized(this){if(entry.imageCharge==0){long charge=3L*MAX_REPLY_BYTES;long captures=entries.values().stream().filter(value->value.imageCharge!=0).count();if(captures>=limits.maximumLeases() || limits.maximumImageBytes()>limits.maximumReservedImageBytes()-imageBytes || charge-entry.metadataCharge>limits.maximumMetadataBytes()-metadataBytes)throw new RatisTopologySourceSnapshotArtifacts.AdmissionRejected("Source snapshot admission exhausted");requireOpen();entry.imageCharge=limits.maximumImageBytes();imageBytes+=entry.imageCharge;metadataBytes+=charge-entry.metadataCharge;entry.metadataCharge=charge;}}
                Lease candidate=null;
                try {candidate=Objects.requireNonNull(host.capture(request));requireRequest(request,candidate.prepared().request());
                    if(candidate.prepared().image().bytes()>limits.maximumImageBytes())throw new IOException("Captured source image exceeds reserved quota");
                    host.requireOwner(candidate);long charge=3L*preparedBytes(candidate.prepared());
                    synchronized(this){entry.lease=candidate;metadataBytes+=charge-entry.metadataCharge;entry.metadataCharge=charge;candidate=null;}
                }finally{if(candidate!=null)try{candidate.close();}catch(Exception cleanup){synchronized(this){entry.lease=candidate;entry.unusable=true;}throw cleanup;}}
            }else host.requireOwner(entry.lease);
            synchronized(this){requireOpen();}
            return entry.lease.prepared();
        }catch(Exception problem){
            if(problem instanceof RatisTopologySourceSnapshotArtifacts.OriginalCutUnavailable){try{problem=new AbandonedException(host.abandon(request));}catch(Exception qualification){problem.addSuppressed(qualification);}}
            if(problem instanceof AbandonedException || problem instanceof RatisTopologySourceSnapshotArtifacts.AdmissionRejected){synchronized(this){if(entry.lease==null && entries.remove(request.transferId(),entry)){imageBytes-=entry.imageCharge;metadataBytes-=entry.metadataCharge;}}}
            failure=problem instanceof AbandonedException terminal?terminal:io("Source snapshot preparation failed; original owner retained",problem);throw failure;}
        finally{finish(entry,failure);}
    }
    public CompletableFuture<Prepared> transfer(Request request) {
        RatisProofQueryService.ResourceLease resource=null;Transfer task=null;
        try {
            Objects.requireNonNull(request);resource=Objects.requireNonNull(admission.reserve(TRANSFER_BYTES));
            synchronized(this){requireOpen();requireNotReleased(request);var entry=entries.get(request.transferId());if(entry!=null){requireSame(entry,request);if(entry.busy)throw new IOException("Source transfer owner is active");}
                if(active>=MAX_LEASES || PUBLICATION_LOOKUP_BYTES>limits.maximumMetadataBytes()-metadataBytes || !releaseLookups.add(request.transferId()))throw new IOException("Source transfer lookup admission exhausted");
                active++;metadataBytes+=PUBLICATION_LOOKUP_BYTES;task=new Transfer(request,resource);transfers.add(task);resource=null;
            }
            try{var handle=executor.executeCooperatively(task,TRANSFER_BYTES);task.handle=handle;boolean stop;synchronized(this){stop=stopping;}if(stop)handle.cancel();}catch(RuntimeException rejected){task.reject(rejected);}
            return task.result.copy();
        }catch(Exception failed){if(resource!=null)resource.close();return CompletableFuture.failedFuture(failed);}
    }
    private final class Transfer implements RWScheduler.CooperativeCompletionTask {
        final Request request;final RatisProofQueryService.ResourceLease resource;final CompletableFuture<Prepared> result=new CompletableFuture<>();
        volatile RWScheduler.CooperativeHandle handle;Entry entry;RockserverSnapshotRequests.Requests chunks;boolean lookup=true,terminal,settled,settling;Exception failure;
        Transfer(Request request,RatisProofQueryService.ResourceLease resource){this.request=request;this.resource=resource;}
        @Override public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context){
            try {
                synchronized(RatisTopologySourceSnapshotService.this){requireOpen();}
                if(context.terminationRequested())throw context.terminationFailure();
                if(lookup){var abandoned=host.abandoned(request);if(abandoned!=null)throw new AbandonedException(abandoned);
                    synchronized(RatisTopologySourceSnapshotService.this){entry=entries.get(request.transferId());if(entry==null || entry.lease==null || entry.unusable)throw new IOException("Prepare the exact source image first");requireSame(entry,request);if(entry.busy)throw new IOException("Source transfer owner is active");entry.busy=true;releaseLookups.remove(request.transferId());metadataBytes-=PUBLICATION_LOOKUP_BYTES;lookup=false;}
                }
                if(entry.installed)return RWScheduler.CooperativeResult.COMPLETE;
                host.requireOwner(entry.lease);
                if(chunks==null)chunks=RockserverSnapshotRequests.openSource(entry.lease.source(),RaftPeerId.valueOf(request.receiver().peerId()),request.transferId().toString(),entry.lease.snapshot(),CHUNK_BYTES);
                if(!chunks.hasNext())throw new IOException("Snapshot producer did not yield a final receiver response");
                var chunk=chunks.next();var reply=host.send(chunk);requireReply(chunk,reply);host.requireOwner(entry.lease);
                synchronized(RatisTopologySourceSnapshotService.this){requireOpen();}
                if(reply.getResult()!=InstallSnapshotResult.ALREADY_INSTALLED && !chunk.getSnapshotChunk().getDone())return RWScheduler.CooperativeResult.YIELD;
                closeReader();
                host.requireReceiverInstalled(entry.lease);host.requireOwner(entry.lease);return RWScheduler.CooperativeResult.COMPLETE;
            }catch(Exception problem){failure=problem;context.fail(problem instanceof RuntimeException runtime?runtime:new CompletionException(problem));return RWScheduler.CooperativeResult.COMPLETE;}
        }
        private void closeReader()throws IOException{if(chunks!=null){chunks.close();chunks=null;}}
        @Override public void completeCooperatively(){synchronized(RatisTopologySourceSnapshotService.this){if(stopping && failure==null)failure=new IOException("Source transfer owner stopping");if(failure==null)entry.installed=true;}settle();}
        @Override public void reject(RuntimeException rejected){synchronized(RatisTopologySourceSnapshotService.this){if(failure==null)failure=rejected;}settle();}
        void settle(){
            synchronized(RatisTopologySourceSnapshotService.this){if(settled || settling)return;settling=true;terminal=true;}
            try {closeReader();}
            catch(IOException unknown){synchronized(RatisTopologySourceSnapshotService.this){if(entry!=null)entry.unusable=true;settling=false;}result.completeExceptionally(unknown);return;}
            IOException problem=failure==null?null:io("Source transfer outcome unresolved; exact lease retained",failure);
            try {
                if(lookup){synchronized(RatisTopologySourceSnapshotService.this){releaseLookups.remove(request.transferId());metadataBytes-=PUBLICATION_LOOKUP_BYTES;active--;lookup=false;RatisTopologySourceSnapshotService.this.notifyAll();}}
                else finish(entry,problem);
            }catch(IOException cleanup){if(problem==null)problem=cleanup;else problem.addSuppressed(cleanup);}
            synchronized(RatisTopologySourceSnapshotService.this){settled=true;}
            try {if(problem==null)result.complete(entry.lease.prepared());else result.completeExceptionally(problem);}finally{resource.close();synchronized(RatisTopologySourceSnapshotService.this){transfers.remove(this);settling=false;}checkStopped();}
        }
    }
    public void release(Request request)throws IOException {
        Entry entry;
        synchronized(this){
            if(releaseLookups.contains(request.transferId()))throw new IOException("Source publication lookup still owns this transfer");
            byte[] prior=released.get(request.transferId());if(prior!=null){requireReleased(request,prior);return;}
            entry=entries.get(request.transferId());
            if(entry!=null){requireSame(entry,request);if(entry.busy || !entry.installed)throw new IOException("Source snapshot has no settled receiver installation");entry.busy=true;active++;}
            else {requireOpen();if(active>=MAX_LEASES || released.size()>=MAX_RELEASED || RELEASE_LOOKUP_BYTES>limits.maximumMetadataBytes()-metadataBytes || !releaseLookups.add(request.transferId()))throw new IOException("Durable release lookup capacity/ownership unavailable");active++;metadataBytes+=RELEASE_LOOKUP_BYTES;}
        }
        if(entry!=null){retire(entry,true);return;}
        try {
            // This hook only reads a forced receipt; absent input cannot gain release authority.
            if(!host.isReleased(request))throw new IOException("Unknown source snapshot lease/release receipt");
            synchronized(this){released.put(request.transferId(),digest(request));}
        }catch(Exception failure){throw io("Durable source release retry failed",failure);}
        finally{synchronized(this){releaseLookups.remove(request.transferId());active--;metadataBytes-=RELEASE_LOOKUP_BYTES;notifyAll();}checkStopped();}
    }
    private void finish(Entry entry,IOException failure)throws IOException {
        boolean close;
        synchronized(this){entry.busy=false;active--;close=stopping;notifyAll();}
        if(close){synchronized(this){if(entry.busy || entries.get(entry.request.transferId())!=entry)return;entry.busy=true;active++;}try{retire(entry,false);}catch(IOException cleanup){if(failure!=null)failure.addSuppressed(cleanup);else throw cleanup;}}
        checkStopped();
    }
    private void retire(Entry entry,boolean remember)throws IOException {
        boolean success=false;
        try{if(entry.lease!=null){if(remember)host.released(entry.lease);entry.lease.close();}success=true;}
        catch(Exception problem){throw io("Source snapshot cleanup failed; lease retained",problem);}
        finally{synchronized(this){active--;entry.busy=false;if(success){entries.remove(entry.request.transferId(),entry);imageBytes-=entry.imageCharge;metadataBytes-=entry.metadataCharge;if(remember)released.put(entry.request.transferId(),digest(entry.request));}notifyAll();}checkStopped();}
    }
    public CompletableFuture<Void> stop(){
        List<Transfer> tasks;List<Entry> idle;
        synchronized(this){stopping=true;tasks=List.copyOf(transfers);idle=entries.values().stream().filter(entry->!entry.busy).toList();for(var entry:idle){entry.busy=true;active++;}}
        for(var task:tasks){var handle=task.handle;if(handle!=null)handle.cancel();if(task.terminal)task.settle();}
        for(var entry:idle)try{retire(entry,false);}catch(IOException unknown){/* Retryable close retains the exact lease and its image quota. */}
        checkStopped();return stopped.copy();
    }
    private void checkStopped(){boolean complete;synchronized(this){complete=stopping && active==0 && entries.isEmpty() && transfers.isEmpty();}if(complete)stopped.complete(null);}
    @Override public void close()throws IOException{var completion=stop();if(!completion.isDone())throw new IOException("Source snapshot stop waits for actual capture/transfer/cleanup drainage");try{completion.join();}catch(CompletionException failed){throw new IOException("Source stop did not settle",failed.getCause());}}
    static void requireReply(InstallSnapshotRequestProto request,InstallSnapshotReplyProto reply)throws IOException {
        var expected=request.getServerRequest();
        if(reply==null || !reply.hasServerReply() || !reply.getServerReply().getSuccess() || !reply.hasRequestIndex() || reply.getRequestIndex()!=request.getSnapshotChunk().getRequestIndex()
                || reply.getTerm()!=request.getLeaderTerm() || !reply.getServerReply().getRequestorId().equals(expected.getRequestorId()) || !reply.getServerReply().getReplyId().equals(expected.getReplyId())
                || !reply.getServerReply().getRaftGroupId().equals(expected.getRaftGroupId()) || reply.getResult()!=InstallSnapshotResult.SUCCESS && reply.getResult()!=InstallSnapshotResult.ALREADY_INSTALLED)throw new IOException("Unknown, stale or crossed snapshot receiver reply");
    }
    private void requireOpen()throws IOException{if(stopping)throw new IOException("Source snapshot owner is stopping");}
    private void requireNotReleased(Request request)throws IOException{var prior=released.get(request.transferId());if(prior!=null){requireReleased(request,prior);throw new IOException("Source snapshot already released");}}
    private static void requireSame(Entry entry,Request request)throws IOException{try{requireRequest(entry.request,request);}catch(IllegalArgumentException changed){throw new IOException("Transfer ID already owns different input",changed);}}
    private static void requireReleased(Request request,byte[] expected)throws IOException{if(!Arrays.equals(expected,digest(request)))throw new IOException("Released transfer input changed");}
    private static byte[] digest(Request request){try{return java.security.MessageDigest.getInstance("SHA-256").digest(RatisTopologySourceSnapshotRpc.request(RELEASE,request));}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    private static IOException io(String message,Exception cause){return cause instanceof IOException failure?failure:new IOException(message,cause);}
}
