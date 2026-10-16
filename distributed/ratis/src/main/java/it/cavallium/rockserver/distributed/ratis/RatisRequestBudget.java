package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.SERVER_OVERLOADED;

/** Credits precede owner construction and input copies, and outlive observers until actual ownership drains. */
final class RatisRequestBudget implements AutoCloseable,RatisJournalAccess {
    record Geometry(long inputBytes,long resultBytes,long maximumItemBytes,long maximumResultBytes,long logicalBytes,int items) {
        long bytes(){long journal=56+RemoteMutationToken.MAX_BYTES+36+32+8L*items+inputBytes+resultBytes;return Math.addExact(64L*1024+logicalBytes,Math.addExact(4*journal,4*(maximumItemBytes+maximumResultBytes)));}
    }
    private final RemoteMutationOwnerLimits limits;private final ThreadPoolExecutor recovery;
    private long bytes,diskBytes,pathBytes;private int owners,requests,works,diskOwners,recoveryOwners;private boolean closed;
    private final EnumMap<RemoteMutationToken.Kind,Integer> ownerCounts=new EnumMap<>(RemoteMutationToken.Kind.class),requestCounts=new EnumMap<>(RemoteMutationToken.Kind.class);
    private final Map<Path,Disk> journals=new HashMap<>();
    private RatisBackendRootOwnership backendRoot;
    RatisRequestBudget(RemoteMutationOwnerLimits limits){this.limits=Objects.requireNonNull(limits);recovery=new ThreadPoolExecutor(limits.recoveryWorkers(),limits.recoveryWorkers(),0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(limits.maximumRequests()),Thread.ofPlatform().daemon(true).name("ratis-request-recovery-",0).factory(),new ThreadPoolExecutor.AbortPolicy());}
    RemoteMutationOwnerLimits limits(){return limits;}
    private static void kind(RemoteMutationToken.Kind kind){if(kind!=RemoteMutationToken.Kind.POINT && kind!=RemoteMutationToken.Kind.BULK && kind!=RemoteMutationToken.Kind.RANGE && kind!=RemoteMutationToken.Kind.ATOMIC)throw new IllegalArgumentException("Unsupported shared owner kind");}
    private int maximumOwners(RemoteMutationToken.Kind kind){return switch(kind){case POINT->limits.maximumPointOwners();case BULK->limits.maximumBulkOwners();case RANGE->limits.maximumRangeOwners();case ATOMIC->limits.maximumAtomicOwners();default->throw new IllegalArgumentException("Unsupported owner kind");};}
    private void capacity(long amount){if(closed || amount<0 || amount>limits.maximumBytes()-limits.recoveryBytes()-limits.renewalBytes()-bytes)throw RocksDBException.of(SERVER_OVERLOADED,"Shared remote owner byte envelope exhausted");}
    synchronized Owner owner(RemoteMutationToken.Kind kind,long directoryBytes,long openingBytes){
        kind(kind);if(directoryBytes<0 || openingBytes<0)throw new IllegalArgumentException("Negative owner geometry");long base=Math.addExact(256L<<10,directoryBytes),opening=Math.addExact(base,Math.multiplyExact(4,openingBytes));
        if(ownerCounts.getOrDefault(kind,0)>=maximumOwners(kind))throw RocksDBException.of(SERVER_OVERLOADED,"Remote owner kind/history count exhausted");capacity(opening);owners++;ownerCounts.merge(kind,1,Integer::sum);bytes+=opening;return new Owner(kind,base,opening);
    }
    final class Owner implements AutoCloseable {
        final RemoteMutationToken.Kind kind;final long base;private long charged;private boolean released,opened;
        Owner(RemoteMutationToken.Kind kind,long base,long charged){this.kind=kind;this.base=base;this.charged=charged;}
        void requireOpening(long fileBytes){synchronized(RatisRequestBudget.this){if(released || opened || fileBytes<0)throw new IllegalStateException("Owner opening credit unavailable");long target=Math.addExact(base,Math.multiplyExact(6,fileBytes));if(target>charged){capacity(target-charged);bytes+=target-charged;charged=target;}}}
        Lease opened(long retainedBytes,boolean retained){synchronized(RatisRequestBudget.this){if(released || opened || retainedBytes<0 || !retained && retainedBytes!=0)throw new IllegalStateException("Owner opening credit unavailable");if(retained && requests>=limits.maximumRequests())throw RocksDBException.of(SERVER_OVERLOADED,"Shared retained request count exhausted");long replacement=Math.addExact(base,retainedBytes),delta=replacement-charged;if(delta>0)capacity(delta);bytes+=delta;charged=base;opened=true;if(retained){requests++;requestCounts.merge(kind,1,Integer::sum);return new Lease(kind,retainedBytes,true);}return null;}}
        /** ACKed cold state needs cleanup workspace, not a new retained public request or a second opening reservation. */
        Lease openedWorkspace(long minimum){synchronized(RatisRequestBudget.this){if(released || opened || minimum<0)throw new IllegalStateException("Owner opening credit unavailable");long amount=Math.max(minimum,charged-base),delta=Math.addExact(base,amount)-charged;if(delta>0)capacity(delta);bytes+=delta;charged=base;opened=true;works++;return new Lease(null,amount,false);}}
        @Override public void close(){synchronized(RatisRequestBudget.this){if(released)return;released=true;owners--;ownerCounts.merge(kind,-1,Integer::sum);bytes-=charged;}}
    }
    synchronized Lease request(RemoteMutationToken.Kind kind,long amount){kind(kind);if(requests>=limits.maximumRequests())throw RocksDBException.of(SERVER_OVERLOADED,"Shared remote request count exhausted");capacity(amount);requests++;requestCounts.merge(kind,1,Integer::sum);bytes+=amount;return new Lease(kind,amount,true);}
    synchronized Lease work(long amount){capacity(amount);works++;bytes+=amount;return new Lease(null,amount,false);}
    final class Lease implements AutoCloseable {
        volatile long amount;final RemoteMutationToken.Kind kind;final boolean request;private boolean released;
        private final RatisRequestBudget budget=RatisRequestBudget.this;
        Lease(RemoteMutationToken.Kind kind,long amount,boolean request){this.kind=kind;this.amount=amount;this.request=request;}
        void growTo(long target){synchronized(RatisRequestBudget.this){if(released || target<amount)throw new IllegalStateException("Invalid credit growth");capacity(target-amount);bytes+=target-amount;amount=target;}}
        void shrinkTo(long target){synchronized(RatisRequestBudget.this){if(released || target<0 || target>amount)throw new IllegalStateException("Invalid credit shrink");bytes-=amount-target;amount=target;}}
        void transferTo(Lease target,long transferred){synchronized(RatisRequestBudget.this){if(target==this || target.budget!=budget || released || target.released || transferred<0 || transferred>amount)throw new IllegalStateException("Invalid credit transfer");long replacement=Math.addExact(target.amount,transferred);amount-=transferred;target.amount=replacement;}}
        @Override public void close(){synchronized(RatisRequestBudget.this){if(released)return;released=true;bytes-=amount;amount=0;if(request){requests--;requestCounts.merge(kind,-1,Integer::sum);}else works--;}}
    }
    <T> CompletableFuture<T> recover(Callable<T> work){var result=new CompletableFuture<T>();try{recovery.execute(()->{try{result.complete(work.call());}catch(Throwable failure){result.completeExceptionally(failure);}});}catch(RuntimeException failure){result.completeExceptionally(failure);}return result;}
    RecoveryCredit recoveryCredit(){synchronized(this){if(closed || recoveryOwners>=limits.recoveryWorkers())throw RocksDBException.of(SERVER_OVERLOADED,"Shared recovery owners exhausted");recoveryOwners++;return new RecoveryCredit();}}
    final class RecoveryCredit implements AutoCloseable {
        private long extra;private boolean released;
        void require(long geometry){synchronized(RatisRequestBudget.this){if(released || geometry<0)throw new IllegalStateException("Recovery credit unavailable");long needed=Math.max(0,geometry-limits.recoveryBytes()/limits.recoveryWorkers());if(needed>extra){capacity(needed-extra);bytes+=needed-extra;extra=needed;}}}
        @Override public void close(){synchronized(RatisRequestBudget.this){if(released)return;released=true;bytes-=extra;recoveryOwners--;}}
    }
    /** Retained path debt survives actor close. Call while holding the journal's stable ownership lock to reconcile. */
    final class Disk implements AutoCloseable {
        final Path path,pending;private int maximum;private long amount,reservedPeak;private boolean owned;
        Disk(Path path,int maximum,long amount){this.path=path;this.pending=path.resolveSibling(path.getFileName()+".pending");this.maximum=maximum;this.amount=amount;}
        long inventory()throws IOException{return Math.addExact(file(path,maximum),file(pending,maximum));}
        void prepareReplacement(long encodedBytes){synchronized(RatisRequestBudget.this){if(!owned || encodedBytes<32 || encodedBytes>maximum)throw new IllegalStateException("Invalid journal replacement geometry");long peak=Math.addExact(amount,encodedBytes);diskCapacity(peak-amount);diskBytes+=peak-amount;amount=peak;}}
        void forcedReconcile()throws IOException{long actual=inventory();synchronized(RatisRequestBudget.this){if(!owned)throw new IllegalStateException("Journal disk ownership released");actual=Math.max(actual,reservedPeak);if(actual>amount){diskCapacity(actual-amount);diskBytes+=actual-amount;}else diskBytes-=amount-actual;amount=actual;}}
        void reservePeak(long peak){synchronized(RatisRequestBudget.this){if(!owned || peak<0)throw new IllegalStateException("Invalid journal promise");if(peak>amount){diskCapacity(peak-amount);diskBytes+=peak-amount;amount=peak;}reservedPeak=Math.max(reservedPeak,peak);}}
        void prepareOwnedReplacement(long encodedBytes)throws IOException {long actual=inventory();synchronized(RatisRequestBudget.this){if(!owned || encodedBytes<32 || encodedBytes>maximum)throw new IOException("Invalid journal replacement geometry");long peak=Math.max(amount,Math.addExact(actual,encodedBytes));diskCapacity(peak-amount);diskBytes+=peak-amount;amount=peak;}}
        void releasePromise()throws IOException {SessionJournalFiles.force(path.getParent());if(Files.exists(path,LinkOption.NOFOLLOW_LINKS))SessionJournalFiles.force(path);long actual=inventory();synchronized(RatisRequestBudget.this){if(!owned)throw new IllegalStateException("Journal ownership released");if(actual>amount){diskCapacity(actual-amount);diskBytes+=actual-amount;}else diskBytes-=amount-actual;amount=actual;reservedPeak=0;}}
        long debt(){synchronized(RatisRequestBudget.this){return amount;}}
        @Override public void close(){synchronized(RatisRequestBudget.this){if(!owned)return;owned=false;diskOwners--;}}
    }
    private void diskCapacity(long amount){if(closed || amount<0 || amount>limits.maximumDiskBytes()-diskBytes)throw RocksDBException.of(SERVER_OVERLOADED,"Shared journal replacement disk envelope exhausted");}
    Disk disk(Path input,int maximum)throws IOException {
        if(maximum<32)throw new IllegalArgumentException("Invalid journal maximum");Path path=input.toAbsolutePath().normalize();String text=path.toString();if(text.length()>4096 || path.getFileName().toString().endsWith(".pending"))throw new IOException("Invalid owned journal path");
        // Two UnixPath objects retain UTF-8 arrays, cached UTF-16 strings and component-offset arrays.
        // Include object/array/map headers and the longer pending suffix before registry retention.
        long actual=Math.addExact(file(path,maximum),file(path.resolveSibling(path.getFileName()+".pending"),maximum));long metadata=Math.addExact(2048L,Math.multiplyExact(32L,Math.addExact(text.length(),8L)));
        synchronized(this){if(closed)throw new IOException("Shared journal budget closed");var existing=journals.get(path);if(existing!=null){if(existing.owned)throw new IOException("Journal already owned");if(actual>existing.amount){diskCapacity(actual-existing.amount);diskBytes+=actual-existing.amount;existing.amount=actual;}existing.maximum=maximum;existing.owned=true;diskOwners++;return existing;}
            if(journals.size()>=limits.maximumJournalPaths())throw RocksDBException.of(SERVER_OVERLOADED,"Shared journal path registry exhausted");capacity(metadata);diskCapacity(actual);var created=new Disk(path,maximum,actual);created.owned=true;journals.put(path,created);bytes+=metadata;pathBytes+=metadata;diskBytes+=actual;diskOwners++;return created;}
    }
    @Override public synchronized RatisJournalAccess.RootLease backendRoot(Path input)throws IOException {if(closed)throw new IOException("Shared backend capability closed");Path root=input.toAbsolutePath().normalize();if(backendRoot==null || backendRoot.released())backendRoot=new RatisBackendRootOwnership(root,this);return backendRoot.retain(root);}
    @Override public void register(Path path,int maximum)throws IOException {try(var disk=disk(path,maximum)) {}}
    @Override public byte[] read(Path path,int maximum)throws IOException {Path base=path.getFileName().toString().endsWith(".pending")?path.resolveSibling(path.getFileName().toString().substring(0,path.getFileName().toString().length()-8)):path;try(var disk=disk(base,maximum)){var body=SessionJournalFiles.read(path,maximum);SessionJournalFiles.force(path);SessionJournalFiles.force(path.getParent());disk.forcedReconcile();return body;}}
    @Override public void write(Path path,byte[] body,int maximum)throws IOException {try(var disk=disk(path,maximum)){if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS) && Files.exists(disk.pending,LinkOption.NOFOLLOW_LINKS))throw new IOException("Unpublished original journal requires exact recovery");disk.prepareOwnedReplacement(Math.addExact(body.length,32L));SessionJournalFiles.write(path,body,maximum);disk.forcedReconcile();}}
    @Override public void remove(Path path,byte[] body,int maximum,SessionJournalFiles.Removal removal)throws IOException {try(var disk=disk(path,maximum)){SessionJournalFiles.remove(path,body,maximum,removal);disk.releasePromise();}}
    @Override public void deleted(Path path)throws IOException {Path exact=path.toAbsolutePath().normalize();synchronized(this){if(!journals.containsKey(exact))return;}try(var disk=disk(exact,Integer.MAX_VALUE)){if(Files.exists(exact,LinkOption.NOFOLLOW_LINKS) || Files.exists(disk.pending,LinkOption.NOFOLLOW_LINKS))throw new IOException("Deleted journal graph remains present");disk.releasePromise();}}
    @Override public void reclaimedTree(Path input)throws IOException {
        Path directory=input.toAbsolutePath().normalize();if(Files.exists(directory,LinkOption.NOFOLLOW_LINKS))throw new IOException("Reclaimed journal tree remains present");SessionJournalFiles.force(directory.getParent());
        synchronized(this){for(var disk:journals.values())if(disk.path.startsWith(directory) && disk.owned)throw new IOException("Reclaimed journal tree still has an actual writer");
            for(var iterator=journals.entrySet().iterator();iterator.hasNext();){var entry=iterator.next();if(!entry.getKey().startsWith(directory))continue;var disk=entry.getValue();diskBytes-=disk.amount;long metadata=2048L+32L*(disk.path.toString().length()+8L);bytes-=metadata;pathBytes-=metadata;disk.amount=0;disk.reservedPeak=0;iterator.remove();}}
    }
    private record PlannedJournal(Path path,int maximum,long actual,long peak,long metadata) {}
    /** Validate and charge the entire no-IO plan atomically. A denied plan cannot consume registry slots or promises. */
    @Override public void preflight(List<RatisJournalAccess.Reservation> reservations)throws IOException {
        if(reservations.size()>limits.maximumJournalPaths())throw RocksDBException.of(SERVER_OVERLOADED,"Shared journal plan path count exhausted");
        try(var staging=work(8192)){
            var planned=new LinkedHashMap<Path,PlannedJournal>();
            for(var reservation:reservations){
                var input=reservation.path();String original=input.toString();if(original.length()>4096)throw new IOException("Invalid owned journal path");
                long metadata=Math.addExact(2048L,Math.multiplyExact(32L,Math.addExact(original.length(),8L)));staging.growTo(Math.addExact(staging.amount,metadata));
                Path path=input.toAbsolutePath().normalize();String text=path.toString();if(text.length()>4096 || path.getFileName().toString().endsWith(".pending"))throw new IOException("Invalid owned journal path");
                long normalizedMetadata=Math.addExact(2048L,Math.multiplyExact(32L,Math.addExact(text.length(),8L)));if(normalizedMetadata>metadata)staging.growTo(Math.addExact(staging.amount,normalizedMetadata-metadata));
                long actual=Math.addExact(file(path,reservation.maximum()),file(path.resolveSibling(path.getFileName()+".pending"),reservation.maximum()));
                var previous=planned.get(path);if(previous!=null && previous.maximum()!=reservation.maximum())throw new IOException("Conflicting journal plan bounds");
                planned.put(path,new PlannedJournal(path,reservation.maximum(),actual,Math.max(reservation.peakBytes(),previous==null?0:previous.peak()),normalizedMetadata));
            }
            synchronized(this){
                long diskGrowth=0,retainedMetadata=0;int additions=0;
                for(var plan:planned.values()){var existing=journals.get(plan.path());if(existing!=null){if(existing.owned && plan.peak()>0)throw new IOException("Journal already owned during preflight");diskGrowth=Math.addExact(diskGrowth,Math.max(existing.amount,Math.max(plan.actual(),plan.peak()))-existing.amount);}else{additions++;retainedMetadata=Math.addExact(retainedMetadata,plan.metadata());diskGrowth=Math.addExact(diskGrowth,Math.max(plan.actual(),plan.peak()));}}
                if(additions>limits.maximumJournalPaths()-journals.size())throw RocksDBException.of(SERVER_OVERLOADED,"Shared journal path registry exhausted");
                capacity(0);diskCapacity(diskGrowth);if(retainedMetadata>staging.amount)throw new IllegalStateException("Unpaid journal path graph");
                // The admitted normalized path objects transfer from staging to the registry; no second graph/copy is created.
                staging.amount-=retainedMetadata;pathBytes+=retainedMetadata;diskBytes+=diskGrowth;
                for(var plan:planned.values()){var existing=journals.get(plan.path());if(existing==null){existing=new Disk(plan.path(),plan.maximum(),Math.max(plan.actual(),plan.peak()));journals.put(plan.path(),existing);}else{existing.amount=Math.max(existing.amount,Math.max(plan.actual(),plan.peak()));if(!existing.owned)existing.maximum=plan.maximum();}existing.reservedPeak=Math.max(existing.reservedPeak,plan.peak());}
            }
        }
    }
    @Override public void reserve(RatisJournalAccess.Reservation reservation)throws IOException {preflight(List.of(reservation));}
    @Override public void fulfilled(RatisJournalAccess.Reservation reservation)throws IOException {if(reservation.peakBytes()==0)return;try(var disk=disk(reservation.path(),reservation.maximum())){disk.releasePromise();}}
    private static long file(Path path,int maximum)throws IOException {if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS))return 0;if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Foreign journal object");long size=Files.size(path);if(size>maximum)throw new IOException("Journal inventory exceeds bound");return size;}
    synchronized long chargedBytes(){return bytes+limits.recoveryBytes()+limits.renewalBytes();}
    synchronized long chargedDiskBytes(){return diskBytes;}
    synchronized int retainedRequests(){return requests;}
    synchronized int retainedRequests(RemoteMutationToken.Kind kind){return requestCounts.getOrDefault(kind,0);}
    synchronized int retainedOwners(RemoteMutationToken.Kind kind){return ownerCounts.getOrDefault(kind,0);}
    static long files(Path... paths)throws IOException{long bytes=0;for(var path:paths)if(Files.exists(path,LinkOption.NOFOLLOW_LINKS)){if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Foreign owner journal object");bytes=Math.addExact(bytes,Files.size(path));}return bytes;}
    static long bulkBase(RangeDirectory directory){return Math.addExact(96L<<10,2*RangeDirectoryCodec.encodedSize(directory));}
    static long bulkRetained(RatisBatchRequestJournal.Request request){long commands=0,items=0;for(var chunk:request.chunks){commands=Math.addExact(commands,DistributedCommandCodec.encodedSize(chunk));items+=chunk.mutations().size();}return Math.addExact(bulkBase(request.directory),Math.addExact(3*commands,128*items));}
    static long bulkInput(DistributedSchema schema,long column,int[][] keys,int[] values,long retainedEncoded)throws IOException {
        if(keys.length!=values.length || keys.length>RatisRocksDBBatches.MAX_INPUT_ITEMS)throw new IllegalArgumentException("Invalid bulk input count");
        var shape=RatisLogicalClient.supported(schema,column).schema();long physical=0;for(int n=0;n<shape.keysCount();n++)physical+=shape.key(n);long logical=0,components=0,wire=0;
        for(int n=0;n<keys.length;n++){var parts=keys[n];if(parts.length!=shape.keysCount())throw new IllegalArgumentException("Bulk logical key shape changed");long members=shape.variableLengthKeysCount()==0?0:4;
            for(int k=0;k<parts.length;k++){int size=parts[k];if(size<0 || (k<shape.fixedLengthKeysCount()?size!=shape.key(k):size>65535))throw new IllegalArgumentException("Bulk logical key width changed");logical=Math.addExact(logical,4L+size);components++;if(k>=shape.fixedLengthKeysCount())members+=2L+size;}
            long size=Math.max(0,values[n]);logical=Math.addExact(logical,4+size);long item=DistributedCommandCodec.HEADER_BYTES+26+physical+members+size;if(item>RatisRocksDBBatches.MAX_CHUNK_BYTES)throw new IllegalArgumentException("Bulk item exceeds native chunk bound");wire=Math.addExact(wire,item);
        }
        if(logical>RatisRocksDBBatches.MAX_INPUT_BYTES)throw new IllegalArgumentException("Bulk logical input exceeds bound");
        return Math.addExact(128L<<10,Math.addExact(3*logical+64*components+512L*keys.length+3*wire,6*Math.addExact(retainedEncoded,wire)));
    }
    static Geometry geometry(DistributedSchema schema,long column,List<Keys> keys,List<Buf> values,RatisRocksDBDirect.WriteKind kind,RequestType.RequestTypeId type,boolean multi){
        if(keys.isEmpty() || keys.size()>256 || !multi && keys.size()!=1 || kind!=RatisRocksDBDirect.WriteKind.DELETE && keys.size()!=values.size())throw new IllegalArgumentException("Invalid point input count");var shape=RatisLogicalClient.supported(schema,column).schema();long physical=0;for(int n=0;n<shape.keysCount();n++)physical+=shape.key(n);long input=0,logical=0,maximum=0;
        for(int n=0;n<keys.size();n++){var components=keys.get(n).keys();if(components.length!=shape.keysCount())throw new IllegalArgumentException("Logical key shape changed");long members=shape.variableLengthKeysCount()==0?0:4;for(int part=0;part<components.length;part++){int size=components[part].size();if(part<shape.fixedLengthKeysCount()?size!=shape.key(part):size>65535)throw new IllegalArgumentException("Logical key width changed");logical+=size;if(part>=shape.fixedLengthKeysCount())members+=2L+size;}long value=kind==RatisRocksDBDirect.WriteKind.DELETE || values.get(n)==null?0:values.get(n).size();logical+=value;long item=DistributedCommandCodec.HEADER_BYTES+26+physical+members+value;if(item>DistributedCommandCodec.MAX_ENCODED_BYTES)throw new IllegalArgumentException("Point item exceeds command bound");input+=item;maximum=Math.max(maximum,item);}
        if(input>RatisDirectRequestJournal.MAX_INPUT)throw new IllegalArgumentException("Point input exceeds aggregate bound");boolean returns=type!=RequestType.RequestTypeId.NOTHING && type!=RequestType.RequestTypeId.ENSURE;long oneResult=!returns?8:shape.hasValue()?CommandResult.MAX_BYTES:12;long result=Math.min(RatisDirectRequestJournal.MAX_RESULTS+CommandResult.MAX_BYTES,keys.size()*oneResult);return new Geometry(input,result,maximum,oneResult,logical,keys.size());
    }
    static Geometry geometry(DistributedSchema schema,RatisDirectRequestJournal.Request request){long input=0,maximum=0,logical=0;for(var item:request.items){var mutation=item.mutations().getFirst();var shape=schema.column(mutation.columnId()).schema();long hashes=0;for(int n=shape.fixedLengthKeysCount();n<shape.keysCount();n++)hashes+=shape.key(n);long bytes=DistributedCommandCodec.encodedSize(item);input+=bytes;maximum=Math.max(maximum,bytes);logical+=mutation.encodedSize()-26-hashes-(shape.variableLengthKeysCount()==0?0:4+2L*shape.variableLengthKeysCount());}boolean returns=request.type!=RequestType.RequestTypeId.NOTHING && request.type!=RequestType.RequestTypeId.ENSURE;boolean hasValue=schema.column(request.items.getFirst().mutations().getFirst().columnId()).schema().hasValue();long one=!returns?8:hasValue?CommandResult.MAX_BYTES:12;return new Geometry(input,Math.min(RatisDirectRequestJournal.MAX_RESULTS+CommandResult.MAX_BYTES,request.items.size()*one),maximum,one,logical,request.items.size());}
    @Override public synchronized void close()throws IOException{if(owners!=0 || requests!=0 || works!=0 || recoveryOwners!=0 || diskOwners!=0 || bytes!=pathBytes)throw new IOException("Shared owners/requests/actual work remain charged");closed=true;recovery.shutdown();}
}
