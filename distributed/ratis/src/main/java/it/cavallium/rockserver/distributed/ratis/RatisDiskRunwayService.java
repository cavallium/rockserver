package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.*;
import it.cavallium.rockserver.core.impl.RWScheduler;

/** Rotating bounded metadata census. Logical file lengths are diagnostic; admission uses actual FS usable space. */
final class RatisDiskRunwayService {
    record Scope(UUID pool,UUID group,Path root) {Scope{root=root.toAbsolutePath().normalize();}}
    record Observation(long filesystem,long usableBytes) {}
    @FunctionalInterface interface Probe {Observation sample(Path path)throws IOException;}
    private record Directory(DirectoryStream<Path> stream,Iterator<Path> files) {}
    private final RatisDiskRunway runway;private final Supplier<List<Scope>> scopes;private final Probe probe;private final LongSupplier nanos;
    private final RatisTransactionRecoveryService polling;private List<Scope> cycle;private long started,finishedCut,logicalBytes,finalStarted,finalCut;private int index,files,finalIndex;private List<Long> finalFilesystems;private final Map<Long,Path> representatives=new LinkedHashMap<>();private final List<Path> scanned=new ArrayList<>();
    private final ArrayDeque<Directory> directories=new ArrayDeque<>();private final Map<Long,RatisDiskRunway.Filesystem> filesystems=new LinkedHashMap<>();private final Map<UUID,Set<Long>> pools=new HashMap<>();private final Set<Long> raft=new HashSet<>();private volatile boolean stopping;private List<Long> freeCycle;private int freeIndex;private long freeStarted,freeCut;private final Map<Long,RatisDiskRunway.Filesystem> freshFree=new HashMap<>(),finalFree=new HashMap<>();private Map<Long,Path> qualified=Map.of();
    RatisDiskRunwayService(RatisDiskRunway runway,Supplier<List<Scope>> scopes,RWScheduler scheduler){this(runway,scopes,scheduler,RatisDiskRunwayService::observe,System::nanoTime);}
    RatisDiskRunwayService(RatisDiskRunway runway,Supplier<List<Scope>> scopes,RWScheduler scheduler,Probe probe,LongSupplier nanos){this.runway=Objects.requireNonNull(runway);this.scopes=Objects.requireNonNull(scopes);this.probe=Objects.requireNonNull(probe);this.nanos=Objects.requireNonNull(nanos);polling=new RatisTransactionRecoveryService(scheduler,this::quantum);}
    static Observation observe(Path input)throws IOException {
        var path=input;while(path!=null && !Files.exists(path,LinkOption.NOFOLLOW_LINKS))path=path.getParent();if(path==null || Files.isSymbolicLink(path))throw new IOException("Unqualified disk census root");
        Object device=Files.getAttribute(path,"unix:dev",LinkOption.NOFOLLOW_LINKS);if(!(device instanceof Number id))throw new IOException("Filesystem identity unavailable");return new Observation(id.longValue(),Files.getFileStore(path).getUsableSpace());
    }
    CompletableFuture<Void> refresh(){return polling.tick();}
    void request(){polling.requestQuantum();}
    Throwable failure(){return polling.lastFailure();}
    /** Startup remains bounded by the published inventory limits, outside Raft/native monitors. */
    void initialCensus()throws IOException {try{do{quantum();}while(cycle!=null);}catch(Exception failed){throw failed instanceof IOException io?io:new IOException("Initial disk census failed",failed);}}
    private synchronized void quantum()throws Exception {
        if(stopping)return;
        try {
            if(cycle==null){var declared=declaredScopes();if(declared.isEmpty() || declared.size()>runway.policy().maximumScopes())throw new IOException("Disk census scope bound exceeded");cycle=declared;started=nanos.getAsLong();finishedCut=runway.beginCensus();index=0;files=0;logicalBytes=0;filesystems.clear();pools.clear();raft.clear();representatives.clear();scanned.clear();finalFilesystems=null;finalIndex=0;}
            int remaining=runway.policy().visits();
            while(remaining-->0 && index<cycle.size()){
                var scope=cycle.get(index);
                if(directories.isEmpty()){
                    var sample=probe.sample(scope.root());filesystems.merge(sample.filesystem(),new RatisDiskRunway.Filesystem(sample.filesystem(),sample.usableBytes()),(a,b)->a.usableBytes()<b.usableBytes()?a:b);
                    representatives.putIfAbsent(sample.filesystem(),scope.root());
                    if(scope.pool()!=null)pools.computeIfAbsent(scope.pool(),unused->new HashSet<>()).add(sample.filesystem());else raft.add(sample.filesystem());
                    if(!Files.exists(scope.root(),LinkOption.NOFOLLOW_LINKS) || scanned.stream().anyMatch(scope.root()::startsWith)){index++;continue;}scanned.add(scope.root());open(scope.root());
                }
                var current=directories.peek();if(!current.files().hasNext()){directories.pop().stream().close();if(directories.isEmpty())index++;continue;}
                var path=current.files().next();if(++files>runway.policy().maximumFiles())throw new IOException("Disk census file bound exceeded");var attributes=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
                if(attributes.isSymbolicLink() || !attributes.isDirectory() && !attributes.isRegularFile())throw new IOException("Unqualified disk census entry");
                if(attributes.isDirectory()){if(directories.size()>=16)throw new IOException("Disk census depth bound exceeded");open(path);}else logicalBytes=Math.addExact(logicalBytes,attributes.size());
            }
            if(index==cycle.size()){
                if(finalFilesystems==null){finalFilesystems=List.copyOf(filesystems.keySet());if(finalFilesystems.size()>4L*runway.policy().visits())throw new IOException("Filesystem sample set exceeds four rotating pressure quanta");freeCycle=null;finalStarted=nanos.getAsLong();finalCut=runway.beginFreeObservation();finalFree.clear();}
                while(remaining-->0 && finalIndex<finalFilesystems.size()){
                    long id=finalFilesystems.get(finalIndex++);var sample=probe.sample(representatives.get(id));if(id!=sample.filesystem())throw new IOException("Filesystem changed during census");finalFree.put(id,new RatisDiskRunway.Filesystem(id,sample.usableBytes()));
                }
                if(finalIndex==finalFilesystems.size()){
                    if(!cycle.equals(declaredScopes()))throw new IOException("Disk scopes changed during census");runway.publish(new RatisDiskRunway.Census(started,finishedCut,filesystems,pools,raft,logicalBytes));qualified=Map.copyOf(representatives);runway.observeFree(finalStarted,finalCut,finalFree);cycle=null;freeCycle=null;
                }
            }
        }catch(Exception failed){try{closeDirectories();}catch(IOException close){failed.addSuppressed(close);}finally{cycle=null;runway.invalidate();}throw failed;}
    }
    synchronized void sampleFree()throws IOException {
        if(stopping || qualified.isEmpty() || finalFilesystems!=null && cycle!=null)return;
        try {
        if(freeCycle==null){freeCycle=List.copyOf(qualified.keySet());freeIndex=0;freeStarted=nanos.getAsLong();freeCut=runway.beginFreeObservation();freshFree.clear();}
        for(int visit=0;visit<runway.policy().visits() && freeIndex<freeCycle.size();visit++){long id=freeCycle.get(freeIndex++);var observation=probe.sample(qualified.get(id));if(observation.filesystem()!=id)throw new IOException("Filesystem identity changed");freshFree.put(id,new RatisDiskRunway.Filesystem(id,observation.usableBytes()));}
        if(freeIndex==freeCycle.size()){runway.observeFree(freeStarted,freeCut,freshFree);freeCycle=null;}
        }catch(IOException failed){freeCycle=null;runway.invalidateFree();throw failed;}
    }
    private List<Scope> declaredScopes()throws IOException {var values=new ArrayList<>(scopes.get());if(values.isEmpty() || values.size()>runway.policy().maximumScopes())throw new IOException("Disk census scope bound exceeded");values.sort(Comparator.comparingInt((Scope scope)->scope.root().getNameCount()).thenComparing(scope->scope.root().toString()).thenComparing(scope->String.valueOf(scope.pool())).thenComparing(scope->String.valueOf(scope.group())));return List.copyOf(values);}
    private void open(Path path)throws IOException {var stream=Files.newDirectoryStream(path);directories.push(new Directory(stream,stream.iterator()));}
    private synchronized void closeDirectories()throws IOException {IOException failure=null;while(!directories.isEmpty())try{directories.pop().stream().close();}catch(IOException bad){failure=bad;}if(failure!=null)throw failure;}
    CompletableFuture<Void> stop(){stopping=true;return polling.stop().thenRun(()->{try{closeDirectories();}catch(IOException failed){throw new java.util.concurrent.CompletionException(failed);}});}
}
