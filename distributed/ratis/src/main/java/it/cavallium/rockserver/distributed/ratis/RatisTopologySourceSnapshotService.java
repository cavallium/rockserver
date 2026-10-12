package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.util.*;
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
        void requireOwner(Lease lease)throws Exception;
        void requireReceiverInstalled(Lease lease)throws Exception;
        InstallSnapshotReplyProto send(InstallSnapshotRequestProto request)throws IOException;
    }
    public record Stats(int leases,int active,long reservedImageBytes,long metadataBytes,int released) {}
    private static final class Entry {
        final Request request; long imageCharge,metadataCharge; Lease lease; boolean busy=true,installed,unusable;
        Entry(Request request,long imageCharge,long metadataCharge){this.request=request;this.imageCharge=imageCharge;this.metadataCharge=metadataCharge;}
    }
    private final Host host;private final Limits limits;
    private final Map<UUID,Entry> entries=new LinkedHashMap<>();private final Map<UUID,byte[]> released=new HashMap<>();
    private long imageBytes,metadataBytes;private int active;private boolean stopping;
    public RatisTopologySourceSnapshotService(Host host,Limits limits){this.host=Objects.requireNonNull(host);this.limits=Objects.requireNonNull(limits);}
    public synchronized Stats stats(){return new Stats(entries.size(),active,imageBytes,metadataBytes,released.size());}
    public Prepared prepare(Request request)throws IOException {
        Objects.requireNonNull(request);Entry entry;boolean capture;
        synchronized(this){
            requireOpen();requireNotReleased(request);entry=entries.get(request.transferId());
            if(entry!=null){requireSame(entry,request);if(entry.unusable)throw new IOException("Failed source preparation requires cleanup drainage");if(entry.busy)throw new IOException("Source transfer owner is active");entry.busy=true;active++;capture=entry.lease==null;}
            else {
                long charge=3L*MAX_REPLY_BYTES;
                if(entries.size()>=limits.maximumLeases() || released.size()>=MAX_RELEASED || limits.maximumImageBytes()>limits.maximumReservedImageBytes()-imageBytes || charge>limits.maximumMetadataBytes()-metadataBytes)throw new IOException("Source snapshot admission exhausted");
                entry=new Entry(request,limits.maximumImageBytes(),charge);entries.put(request.transferId(),entry);imageBytes+=entry.imageCharge;metadataBytes+=charge;active++;capture=true;
            }
        }
        IOException failure=null;
        try {
            if(capture){
                Lease candidate=null;
                try {candidate=Objects.requireNonNull(host.capture(request));requireRequest(request,candidate.prepared().request());
                    if(candidate.prepared().image().bytes()>limits.maximumImageBytes())throw new IOException("Captured source image exceeds reserved quota");
                    host.requireOwner(candidate);long charge=3L*preparedBytes(candidate.prepared());
                    synchronized(this){entry.lease=candidate;metadataBytes+=charge-entry.metadataCharge;entry.metadataCharge=charge;candidate=null;}
                }finally{if(candidate!=null)try{candidate.close();}catch(Exception cleanup){synchronized(this){entry.lease=candidate;entry.unusable=true;}throw cleanup;}}
            }else host.requireOwner(entry.lease);
            synchronized(this){requireOpen();}
            return entry.lease.prepared();
        }catch(Exception problem){failure=io("Source snapshot preparation failed; original owner retained",problem);throw failure;}
        finally{finish(entry,failure);}
    }
    public Prepared transfer(Request request)throws IOException {
        Entry entry=begin(request);IOException failure=null;
        try {
            if(entry.installed)return entry.lease.prepared();
            host.requireOwner(entry.lease);boolean received=false;
            try(var chunks=RockserverSnapshotRequests.open(entry.lease.source(),RaftPeerId.valueOf(request.receiver().peerId()),request.transferId().toString(),entry.lease.snapshot(),CHUNK_BYTES)){
                while(chunks.hasNext()){
                    synchronized(this){requireOpen();}
                    host.requireOwner(entry.lease);var chunk=chunks.next();var reply=host.send(chunk);requireReply(chunk,reply);
                    host.requireOwner(entry.lease);
                    if(reply.getResult()==InstallSnapshotResult.ALREADY_INSTALLED || chunk.getSnapshotChunk().getDone()){received=true;break;}
                }
            }
            if(!received)throw new IOException("Snapshot producer did not yield a final receiver response");
            // File-stream acknowledgement is not a native completed installation proof.
            synchronized(this){requireOpen();}
            host.requireReceiverInstalled(entry.lease);host.requireOwner(entry.lease);
            synchronized(this){entry.installed=true;}return entry.lease.prepared();
        }catch(Exception problem){failure=io("Source transfer outcome unresolved; exact lease retained",problem);throw failure;}
        finally{finish(entry,failure);}
    }
    public void release(Request request)throws IOException {
        Entry entry;
        synchronized(this){
            byte[] prior=released.get(request.transferId());if(prior!=null){requireReleased(request,prior);return;}
            entry=entries.get(request.transferId());if(entry==null)throw new IOException("Unknown source snapshot lease");requireSame(entry,request);
            if(entry.busy || !entry.installed)throw new IOException("Source snapshot has no settled receiver installation");entry.busy=true;active++;
        }
        retire(entry,true);
    }
    private Entry begin(Request request)throws IOException {
        synchronized(this){requireOpen();requireNotReleased(request);var entry=entries.get(request.transferId());if(entry==null || entry.lease==null || entry.unusable)throw new IOException("Prepare the exact source image first");requireSame(entry,request);if(entry.unusable)throw new IOException("Failed source preparation requires cleanup drainage");if(entry.busy)throw new IOException("Source transfer owner is active");entry.busy=true;active++;return entry;}
    }
    private void finish(Entry entry,IOException failure)throws IOException {
        boolean close;
        synchronized(this){entry.busy=false;active--;close=stopping;notifyAll();}
        if(close){synchronized(this){if(entry.busy || entries.get(entry.request.transferId())!=entry)return;entry.busy=true;active++;}try{retire(entry,false);}catch(IOException cleanup){if(failure!=null)failure.addSuppressed(cleanup);else throw cleanup;}}
    }
    private void retire(Entry entry,boolean remember)throws IOException {
        boolean success=false;
        try{if(entry.lease!=null)entry.lease.close();success=true;}
        catch(Exception problem){throw io("Source snapshot cleanup failed; lease retained",problem);}
        finally{synchronized(this){active--;entry.busy=false;if(success){entries.remove(entry.request.transferId(),entry);imageBytes-=entry.imageCharge;metadataBytes-=entry.metadataCharge;if(remember)released.put(entry.request.transferId(),digest(entry.request));}notifyAll();}}
    }
    @Override public void close()throws IOException {
        List<Entry> idle;
        synchronized(this){stopping=true;idle=entries.values().stream().filter(entry->!entry.busy).toList();for(var entry:idle){entry.busy=true;active++;}}
        IOException failure=null;for(var entry:idle)try{retire(entry,false);}catch(IOException cleanup){if(failure==null)failure=cleanup;else failure.addSuppressed(cleanup);}
        synchronized(this){if(active!=0 || !entries.isEmpty()){var busy=new IOException("Source snapshot stop waits for actual capture/transfer/cleanup drainage");if(failure==null)failure=busy;else failure.addSuppressed(busy);}}
        if(failure!=null)throw failure;
    }
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
