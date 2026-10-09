package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.*;
import java.util.*;
import static it.cavallium.rockserver.distributed.ratis.RatisExportSourceRpc.*;

/** Node-owned durable source kernel. Cancellation of a caller never releases native/file ownership. */
public final class RatisExportSourceRegistry implements AutoCloseable {
    public record Budgets(int maximumJobs,int maximumChunks,long maximumDiskBytes,long maximumMetadataBytes,long maximumBufferBytes,int concurrentBuilds) {
        public Budgets { if(maximumJobs<1 || maximumJobs>65536 || maximumChunks<1 || maximumChunks>65536 || maximumDiskBytes<1 || maximumMetadataBytes<4096 || maximumBufferBytes<MAX_READ_BYTES || concurrentBuilds<1) throw new IllegalArgumentException("Invalid source budgets"); }
        public static Budgets defaults(){return new Budgets(1024,8192,8L*1024*1024*1024,256L*1024*1024,8L*1024*1024,2);}
    }
    @FunctionalInterface public interface Completion { void persist(NativeRangeSstExport.Chunk chunk)throws IOException; }
    public interface NativeBuilder {
        NativeRangeSstExport.Chunk build(JobDefinition definition,RatisExportJournal.ChunkIntent intent,Path artifact,Completion completion)throws Exception;
        boolean tokenUsable(JobDefinition definition)throws Exception;
    }
    private static final int FILE_MAX=MAX_BYTES+32;
    private static final class Job {
        final JobDefinition definition; final Path path; final Map<Long,BuildRequest> intents=new TreeMap<>(); final Map<Long,Descriptor> descriptors=new TreeMap<>(); final Map<Long,BasicFileAttributes> verified=new HashMap<>();
        volatile boolean releasing; boolean released,releaseReserved=true,gcQueued;
        Job(JobDefinition definition,Path path){this.definition=definition;this.path=path;}
    }
    @FunctionalInterface interface FreeSpace { long usableBytes(Path path)throws IOException; }
    private final RatisExportSourceService.IoLimits ioLimits;private final FreeSpace freeSpace;private long outstandingFileBytes;
    private final SessionJournalFiles.Removal removal;private final ArrayDeque<String> releasedQueue=new ArrayDeque<>();
    private final Path root; private final Budgets budgets; private final NativeBuilder builder; private final FileChannel ownership; private final FileLock lock;
    private final Map<String,Job> jobs=new HashMap<>(); private long disk,metadata,buffers; private int chunks,builds,active; private boolean closed,fenced;
    public RatisExportSourceRegistry(Path root,Budgets budgets,RatisExportSourceService.IoLimits ioLimits,NativeBuilder builder)throws IOException {this(root,budgets,ioLimits,builder,SessionJournalFiles.FILESYSTEM,path->Files.getFileStore(path).getUsableSpace());}
    RatisExportSourceRegistry(Path root,Budgets budgets,RatisExportSourceService.IoLimits ioLimits,NativeBuilder builder,SessionJournalFiles.Removal removal)throws IOException {this(root,budgets,ioLimits,builder,removal,path->Files.getFileStore(path).getUsableSpace());}
    RatisExportSourceRegistry(Path root,Budgets budgets,RatisExportSourceService.IoLimits ioLimits,NativeBuilder builder,SessionJournalFiles.Removal removal,FreeSpace freeSpace)throws IOException {
        this.ioLimits=Objects.requireNonNull(ioLimits);this.freeSpace=Objects.requireNonNull(freeSpace);
        this.removal=Objects.requireNonNull(removal);
        this.root=root.toAbsolutePath().normalize();this.budgets=Objects.requireNonNull(budgets);this.builder=Objects.requireNonNull(builder);Files.createDirectories(this.root);if(!Files.isDirectory(this.root,LinkOption.NOFOLLOW_LINKS))throw new IOException("Source registry root type changed");
        ownership=FileChannel.open(this.root.resolve("ownership.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);FileLock acquired=null;
        try {try{acquired=ownership.tryLock();}catch(OverlappingFileLockException e){throw new IOException("Source registry already owned",e);}if(acquired==null)throw new IOException("Source registry already owned");lock=acquired;scan();}
        catch(IOException|RuntimeException|Error e){if(acquired!=null)acquired.release();ownership.close();throw e;}
    }
    private String key(JobDefinition d){return d.jobId()+"-"+d.group();}
    private void scan()throws IOException {
        int files=0,totalFiles=0;try(var listing=Files.newDirectoryStream(root)){for(Path path:listing){if(path.getFileName().toString().equals("ownership.lock"))continue;if(++files>budgets.maximumJobs() || !Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Source startup directory bound/type violated");if(path.getFileName().toString().startsWith("gc-")){int count=0;try(var gc=Files.newDirectoryStream(path)){for(Path file:gc){if(++count>3L*budgets.maximumChunks()+4 || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid source GC artifact");Files.delete(file);}}SessionJournalFiles.force(path);Files.delete(path);SessionJournalFiles.force(root);continue;}var def=JobDefinition.decode(read(path.resolve("definition")));if(!path.getFileName().toString().equals(key(def)))throw new IOException("Source directory binding changed");var job=new Job(def,path);jobs.put(key(def),job);chargeMetadata(64);int entries=0;try(var content=Files.newDirectoryStream(path)){for(Path file:content){if(++totalFiles>3L*budgets.maximumChunks()+3L*budgets.maximumJobs() || ++entries>3L*budgets.maximumChunks()+4 || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))throw new IOException("Source startup file bound/type violated");String name=file.getFileName().toString();long size=Files.size(file);if(name.equals("definition")){chargeMetadata(size);continue;}if(name.equals("released")){if(!Arrays.equals(read(file),def.digest()))throw new IOException("Release binding changed");job.released=job.releasing=true;job.releaseReserved=false;metadata-=64;chargeMetadata(size);continue;}if(name.endsWith(".intent")){var req=BuildRequest.decode(read(file));requireDefinition(job,req.definition());if(!name.equals(req.intent().ordinal()+".intent") || job.intents.put(req.intent().ordinal(),req)!=null)throw new IOException("Intent binding changed");chargeMetadata(size);}else if(name.endsWith(".descriptor")){var desc=Descriptor.decode(read(file));if(!name.equals(desc.ordinal()+".descriptor") || !Arrays.equals(desc.bindingDigest(),def.digest()))throw new IOException("Descriptor binding changed");job.descriptors.put(desc.ordinal(),desc);chargeMetadata(size);}else if(name.endsWith(".sst") || name.endsWith(".sst.building")){chargeDisk(size);}else if(name.endsWith(".pending")){chargeMetadata(size);}else throw new IOException("Unknown source artifact");}}
            for(var req:job.intents.values())if(++chunks>budgets.maximumChunks())throw new IOException("Source chunk capacity exceeded");
            if(job.released){
                // RELEASE is durable before artifact deletion; surviving files need no complete active sequence.
                for(var desc:job.descriptors.values())try{if(!def.fence().participant(def.group()).scope().containsRange(desc.column(),desc.lowerInclusive(),desc.upperExclusive()))throw new IOException("Released descriptor outside original coverage");validate(new BuildRequest(def,new RatisExportJournal.ChunkIntent(desc.group(),desc.column(),desc.cut(),desc.lowerInclusive(),desc.ordinal(),def.limits().chunk())),desc);}catch(IllegalArgumentException failure){throw new IOException("Released descriptor changed original binding",failure);}
                queueReleased(job);
            }else{
                long expectedOrdinal=0;for(var req:job.intents.values()){validateNext(job,req,expectedOrdinal++);var desc=job.descriptors.get(req.intent().ordinal());if(desc!=null)validate(req,desc);}
                for(long ordinal:job.descriptors.keySet())if(!job.intents.containsKey(ordinal))throw new IOException("Descriptor without durable intent");
            }
        }}
    }
    private byte[] read(Path path)throws IOException {if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Source metadata missing/type changed");return SessionJournalFiles.read(path,FILE_MAX);}
    private synchronized void chargeMetadata(long bytes)throws IOException{if(bytes>budgets.maximumMetadataBytes()-metadata)throw new IOException("Source metadata capacity unavailable");metadata+=bytes;}
    private synchronized void chargeDisk(long bytes)throws IOException{if(bytes>budgets.maximumDiskBytes()-disk)throw new IOException("Source disk capacity unavailable");disk+=bytes;}
    private void write(Path path,byte[] bytes)throws IOException {long delta=bytes.length+32L-(Files.exists(path,LinkOption.NOFOLLOW_LINKS)?Files.size(path):0);chargeMetadata(delta);try{SessionJournalFiles.write(path,bytes,FILE_MAX);}catch(IOException e){synchronized(this){fenced=true;}throw e;}}
    private void requireDefinition(Job job,JobDefinition definition)throws IOException{if(!Arrays.equals(job.definition.encode(),definition.encode()))throw new IOException("Source job definition changed");}
    private synchronized void open()throws IOException{if(closed || fenced || !lock.isValid())throw new IOException("Source registry closed/fenced");}
    private Job job(JobDefinition definition,boolean create)throws Exception {
        synchronized(this){open();Job old=jobs.get(key(definition));if(old!=null){requireDefinition(old,definition);return old;}}
        if(!create)return null;
        // A reclaimed tombstone may never recreate metadata for a dead/old-incarnation original token.
        if(!builder.tokenUsable(definition))throw new RatisSnapshotLostException("Original export token unavailable");
        synchronized(this){open();Job old=jobs.get(key(definition));if(old!=null){requireDefinition(old,definition);return old;}if(jobs.size()>=budgets.maximumJobs())throw new IOException("Source job capacity unavailable");chargeMetadata(64);Path path=root.resolve(key(definition));Files.createDirectory(path);try{write(path.resolve("definition"),definition.encode());SessionJournalFiles.force(root);}catch(IOException e){fenced=true;throw e;}Job job=new Job(definition,path);jobs.put(key(definition),job);return job;}
    }
    private synchronized void enter()throws IOException{open();active++;}private synchronized void leave(){active--;notifyAll();}
    public Descriptor build(BuildRequest request)throws Exception {
        enter();try{Job job=job(request.definition(),true);synchronized(job){if(job.releasing)throw new IOException("Source job released");long ordinal=request.intent().ordinal();BuildRequest saved=job.intents.get(ordinal);if(saved!=null && !Arrays.equals(saved.encode(),request.encode()))throw new IOException("Source intent changed");Descriptor completed=job.descriptors.get(ordinal);if(completed!=null){validate(request,completed);verify(job,completed);return completed;}
            Path artifact=artifact(job,ordinal);if(Files.exists(artifact,LinkOption.NOFOLLOW_LINKS) || Files.exists(NativeRangeSstExport.buildingPath(artifact),LinkOption.NOFOLLOW_LINKS))throw new IOException("Bare/partial SST lacks exact durable descriptor");
            if(!builder.tokenUsable(job.definition))throw new RatisSnapshotLostException("Original source pin lost");
            long reserve=request.intent().limits().maximumFileBytes(), descriptorReserve=(long)it.cavallium.rockserver.core.common.distributed.TransactionScope.MAX_BOUND_BYTES+512;boolean[] metadataReserved={false};long usable=freeSpace.usableBytes(root);if(usable<0)throw new IOException("Source filesystem space unavailable");synchronized(this){if(reserve>usable-ioLimits.minimumFreeBytes() || outstandingFileBytes>usable-ioLimits.minimumFreeBytes()-reserve)throw new IOException("Source filesystem free-space capacity unavailable");if(builds>=budgets.concurrentBuilds())throw new IOException("Source build capacity unavailable");chargeDisk(reserve);try{chargeMetadata(descriptorReserve);}catch(IOException failure){disk-=reserve;throw failure;}metadataReserved[0]=true;builds++;outstandingFileBytes+=reserve;}

            try {
            if(saved==null){validateNext(job,request);long futureMetadata=jobMetadata(job)+request.encode().length+32L+it.cavallium.rockserver.core.common.distributed.TransactionScope.MAX_BOUND_BYTES+512;long ownedDisk=job.descriptors.values().stream().mapToLong(Descriptor::fileBytes).sum();if(futureMetadata>job.definition.limits().maximumJournalBytes() || request.intent().limits().maximumFileBytes()>job.definition.limits().maximumDiskBytes()-ownedDisk)throw new IOException("Source job future capacity unavailable");synchronized(this){if(chunks>=budgets.maximumChunks())throw new IOException("Source chunk capacity unavailable");chunks++;}try{write(job.path.resolve(ordinal+".intent"),request.encode());job.intents.put(ordinal,request);}catch(Exception e){synchronized(this){chunks--;}throw e;}}
            long ownedDisk=job.descriptors.values().stream().mapToLong(Descriptor::fileBytes).sum(),ownedMetadata=jobMetadata(job);
            if(request.intent().limits().maximumFileBytes()>job.definition.limits().maximumDiskBytes()-ownedDisk || it.cavallium.rockserver.core.common.distributed.TransactionScope.MAX_BOUND_BYTES+512L>job.definition.limits().maximumJournalBytes()-ownedMetadata)throw new IOException("Source job disk/metadata capacity unavailable");
                Completion persist=result->{if(job.descriptors.containsKey(ordinal))throw new IOException("Native completion repeated");Descriptor descriptor=descriptor(request,result);validate(request,descriptor);if(result.manifest().file()!=null && !result.manifest().file().normalize().equals(artifact))throw new IOException("Native artifact path changed");verify(job,descriptor);synchronized(this){metadata-=descriptorReserve;metadataReserved[0]=false;write(job.path.resolve(ordinal+".descriptor"),descriptor.encode());}job.descriptors.put(ordinal,descriptor);};
                try{builder.build(job.definition,request.intent(),artifact,persist);}catch(Exception failure){Descriptor exact=job.descriptors.get(ordinal);if(exact!=null){verify(job,exact);return exact;}throw failure;}
                Descriptor exact=job.descriptors.get(ordinal);if(exact==null)throw new IOException("Native builder omitted durable completion");return exact;
            }finally{long actual=reserve;try{actual=0;for(Path p:List.of(artifact,NativeRangeSstExport.buildingPath(artifact)))if(Files.exists(p,LinkOption.NOFOLLOW_LINKS))actual=Math.addExact(actual,Files.size(p));}catch(IOException|RuntimeException failure){actual=reserve;synchronized(this){fenced=true;}throw failure;}finally{synchronized(this){disk-=reserve;disk+=actual;builds--;outstandingFileBytes-=reserve;if(metadataReserved[0])metadata-=descriptorReserve;if(disk>budgets.maximumDiskBytes())fenced=true;}}}
        }}finally{leave();}
    }
    private long jobMetadata(Job job){long bytes=job.definition.encode().length+96;for(var i:job.intents.values())bytes+=i.encode().length+32;for(var d:job.descriptors.values())bytes+=d.encode().length+32;return bytes;}
    private void validateNext(Job job,BuildRequest request)throws IOException {validateNext(job,request,job.intents.size());}
    private void validateNext(Job job,BuildRequest request,long expectedOrdinal)throws IOException {
        long n=request.intent().ordinal();var columns=job.definition.columnsForGroup();UUID column=columns.getFirst();byte[] lower=job.definition.scope(column).lowerInclusive();
        if(n!=expectedOrdinal) throw new IOException("Source ordinal changed");
        if(n!=0){
            Descriptor prior=job.descriptors.get(n-1);if(prior==null) throw new IOException("Source continuation lacks preceding descriptor");
            column=prior.column();
            if(prior.hasMore()) lower=prior.upperExclusive();
            else {int next=columns.indexOf(column)+1;if(next>=columns.size()) throw new IOException("Source columns already complete");column=columns.get(next);lower=job.definition.scope(column).lowerInclusive();}
        }
        if(!column.equals(request.intent().column()) || !Arrays.equals(lower,request.intent().scopeStart())) throw new IOException("Source column/coverage changed");
    }
    private Descriptor descriptor(BuildRequest r,NativeRangeSstExport.Chunk chunk){var m=chunk.manifest();return new Descriptor(r.definition().digest(),r.intent().ordinal(),m.groupId(),m.rangeEpoch(),m.schemaDigest(),m.columnId(),m.position(),m.lowerInclusive(),m.upperExclusive(),m.rows()==0?hash(new byte[0]):m.sha256(),m.rows(),m.payloadBytes(),m.fileBytes(),chunk.hasMore());}
    private void validate(BuildRequest r,Descriptor d)throws IOException {try{requireDescriptor(r,d);}catch(IllegalArgumentException failure){throw new IOException("Native descriptor changed original binding",failure);}}
    private Path artifact(Job j,long ordinal){return j.path.resolve(ordinal+".sst");}
    private void verify(Job job,Descriptor descriptor)throws IOException {
        Path file=artifact(job,descriptor.ordinal());if(descriptor.rows()==0){if(Files.exists(file,LinkOption.NOFOLLOW_LINKS) || !Arrays.equals(descriptor.contentHash(),hash(new byte[0])))throw new IOException("Empty chunk artifact changed");return;}
        BasicFileAttributes attrs=Files.readAttributes(file,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);if(!attrs.isRegularFile() || attrs.size()!=descriptor.fileBytes())throw new IOException("Source artifact size/type changed");BasicFileAttributes prior=job.verified.get(descriptor.ordinal());if(prior!=null && Objects.equals(prior.fileKey(),attrs.fileKey()) && prior.size()==attrs.size() && prior.lastModifiedTime().equals(attrs.lastModifiedTime()) && prior.creationTime().equals(attrs.creationTime()))return;
        reserveBuffer(64*1024);try{MessageDigest digest;try{digest=MessageDigest.getInstance("SHA-256");}catch(NoSuchAlgorithmException e){throw new AssertionError(e);}try(var channel=FileChannel.open(file,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){ByteBuffer buffer=ByteBuffer.allocate(64*1024);while(channel.read(buffer)!=-1){buffer.flip();digest.update(buffer);buffer.clear();}if(channel.size()!=descriptor.fileBytes() || !MessageDigest.isEqual(digest.digest(),descriptor.contentHash()))throw new IOException("Source artifact checksum changed");}var after=Files.readAttributes(file,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);if(!Objects.equals(attrs.fileKey(),after.fileKey()) || !attrs.lastModifiedTime().equals(after.lastModifiedTime()) || attrs.size()!=after.size())throw new IOException("Source artifact changed during verification");job.verified.put(descriptor.ordinal(),after);}finally{freeBuffer(64*1024);}
    }
    synchronized void reserveBuffer(long bytes)throws IOException{if(bytes>budgets.maximumBufferBytes()-buffers)throw new IOException("Source buffer capacity unavailable");buffers+=bytes;}synchronized void freeBuffer(long bytes){buffers-=bytes;}
    public byte[] read(ReadRequest request)throws Exception {enter();try{Job job=job(request.definition(),false);if(job==null)throw new IOException("Unknown export source");synchronized(job){if(job.releasing)throw new IOException("Source job released");Descriptor descriptor=job.descriptors.get(request.ordinal());if(descriptor==null || !Arrays.equals(descriptor.digest(),request.descriptorDigest()) || request.offset()>descriptor.fileBytes())throw new IOException("Source read descriptor/offset changed");verify(job,descriptor);int size=(int)Math.min(request.maxBytes(),descriptor.fileBytes()-request.offset());reserveBuffer(size);try{byte[] bytes=new byte[size];if(size!=0)try(var channel=FileChannel.open(artifact(job,request.ordinal()),StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){channel.position(request.offset());ByteBuffer buffer=ByteBuffer.wrap(bytes);while(buffer.hasRemaining())if(channel.read(buffer)<0)throw new IOException("Truncated source artifact");}return bytes;}finally{freeBuffer(size);}}}finally{leave();}}
    /** Admission fence is separate from cleanup so asynchronous dispatch can drain actual work first. */
    public void fenceRelease(ReleaseRequest request)throws IOException {synchronized(this){open();Job job=jobs.get(key(request.definition()));if(job!=null){requireDefinition(job,request.definition());job.releasing=true;}}}
    public void release(ReleaseRequest request)throws Exception {enter();try{Job job=job(request.definition(),false);if(job==null){if(!builder.tokenUsable(request.definition()))return;job=job(request.definition(),true);}job.releasing=true;synchronized(job){
            if(!job.released){synchronized(this){if(job.releaseReserved){metadata-=64;job.releaseReserved=false;}write(job.path.resolve("released"),job.definition.digest());}job.released=true;}
            // Any failed mutation fences this owner. Explicit reopen recovers the durable released or GC directory.
            try {
                try(var files=Files.newDirectoryStream(job.path)){for(Path path:files){String name=path.getFileName().toString();if(name.equals("definition") || name.equals("released"))continue;long size=Files.size(path);removal.delete(path);synchronized(this){if(name.endsWith(".sst") || name.endsWith(".sst.building"))disk-=size;else metadata-=size;}}}
                removal.force(job.path);synchronized(this){chunks-=job.intents.size();}job.intents.clear();job.descriptors.clear();job.verified.clear();
            }catch(IOException|RuntimeException failure){synchronized(this){fenced=true;}throw failure;}
            queueReleased(job);
            // An uncertain token probe retains the charged tombstone; it is not a failed filesystem mutation.
            if(!builder.tokenUsable(job.definition))try{
                long bytes=Files.size(job.path.resolve("definition"))+Files.size(job.path.resolve("released"));Path gc=root.resolve("gc-"+key(job.definition));Files.move(job.path,gc,StandardCopyOption.ATOMIC_MOVE);removal.force(root);removal.delete(gc.resolve("definition"));removal.delete(gc.resolve("released"));removal.force(gc);removal.delete(gc);removal.force(root);
                synchronized(this){metadata-=bytes+(job.releaseReserved?64:0);jobs.remove(key(job.definition),job);releasedQueue.remove(key(job.definition));job.gcQueued=false;}
            }catch(IOException|RuntimeException failure){synchronized(this){fenced=true;}throw failure;}
        }}finally{leave();}}
    private synchronized void queueReleased(Job job){if(!job.gcQueued){releasedQueue.addLast(key(job.definition));job.gcQueued=true;}}
    /** Fair bounded admission-triggered sweep; only the cleanup owner invokes this method. */
    void sweepReleased(int maximum)throws Exception {if(maximum<1 || maximum>64)throw new IllegalArgumentException("Invalid sweep bound");enter();try{int count;synchronized(this){count=Math.min(maximum,releasedQueue.size());}for(int i=0;i<count;i++){Job job;synchronized(this){String key=releasedQueue.pollFirst();job=jobs.get(key);if(job!=null)job.gcQueued=false;}if(job==null)continue;try{release(new ReleaseRequest(job.definition));}finally{synchronized(this){if(jobs.get(key(job.definition))==job)queueReleased(job);}}}}finally{leave();}}
    @Override public synchronized void close()throws IOException {if(active!=0)throw new IOException("Source work active; ownership retained");if(closed)return;closed=true;try{lock.release();}finally{ownership.close();}}
}
