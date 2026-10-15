package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Node-owned bounded journal inventory; CONTROL only schedules one asynchronous IO quantum. */
final class RatisConfiguredRecoveries {
    interface Owner {
        /** Pure scope validation, without disk or remote work. */
        UUID pool(Request request)throws IOException;
        /** Force the DATA group's durable slot promise before creating or opening its journal. */
        RatisJournalAccess journal(UUID pool,UUID group,Path directory)throws IOException;
    }
    @FunctionalInterface interface HostFactory {RatisConfiguredRecovery.Host create(RatisTopologySourceSnapshotClient.ReplyAdmission admission);}
    private static final int MAX_WAITERS=16,STEPS_PER_VISIT=8;
    private static final long INDEX_BASE_BYTES=4096,INDEX_SLOT_BYTES=512;
    private final class PoolIndex {
        final UUID pool;final int maximum;RatisNodeResourceBudget.Lease lease;int count;long bytes=INDEX_BASE_BYTES;
        PoolIndex(UUID pool,int maximum){this.pool=pool;this.maximum=maximum;}
        long reserve(Path directory)throws IOException{if(count>=maximum)throw new IOException("Pool configured recovery slots exhausted");long next=Math.addExact(bytes,INDEX_SLOT_BYTES+16L*directory.toString().length());if(lease==null)lease=resources.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,next,1);else lease.growTo(next);return next;}
        void restore(){if(count==0){lease.close();lease=null;}else lease.shrinkTo(bytes,1);}
        void close(){if(lease!=null)lease.close();}
    }
    private static final class Slot {
        final UUID pool,group;final Path directory;final RatisJournalAccess files;final int index;
        RatisConfiguredRecovery.Phase phase;
        Slot(UUID pool,UUID group,Path directory,RatisJournalAccess files,int index){this.pool=pool;this.group=group;this.directory=directory;this.files=files;this.index=index;}
    }
    private final class Metadata implements RatisConfiguredRecovery.Metadata {
        private final RatisNodeResourceBudget.Lease lease;
        Metadata(UUID pool)throws IOException{lease=resources.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,4096,1);}
        public synchronized void requireRawBytes(int bytes)throws IOException{if(bytes<0)throw new IllegalArgumentException("Invalid recovery metadata size");long required=Math.addExact(4096L,Math.multiplyExact(16L,bytes));if(required>retained){lease.growTo(required);retained=required;}}
        private long retained=4096;
        public synchronized void close(){lease.close();}
    }
    private final Path root;private final byte[] binding;private final int maximum;
    private final RatisNodeResourceBudget resources;private final Owner owner;private final HostFactory hosts;
    private final Map<UUID,PoolIndex> pools=new LinkedHashMap<>();
    private final Map<UUID,Slot> groups=new HashMap<>();private final ArrayList<Slot> slots=new ArrayList<>();
    private final ThreadPoolExecutor io;
    private final CompletableFuture<Void> stopped=new CompletableFuture<>();
    private CompletableFuture<Void> actual;private Request enqueued;private int waiters,stopWaiters;
    private RatisConfiguredRecovery active;private Slot activeSlot;private int cursor,visits;
    private boolean stopping,stopRequested,closed;private volatile Throwable lastFailure;private Throwable stopFailure;

    /** Called on startup IO. Existing journals acquire promises and index credits before any host is dispatched. */
    RatisConfiguredRecoveries(Path existingRoot,byte[] binding,int maxGroups,Map<UUID,Integer> poolMax,
                              RatisNodeResourceBudget resources,Owner owner,HostFactory hosts)throws IOException {
        this.root=Objects.requireNonNull(existingRoot).toAbsolutePath().normalize();this.binding=Objects.requireNonNull(binding).clone();
        this.resources=Objects.requireNonNull(resources);this.owner=Objects.requireNonNull(owner);this.hosts=Objects.requireNonNull(hosts);maximum=maxGroups;
        if(binding.length!=32 || maxGroups<1 || maxGroups>RatisNodeResourceBudget.MAX_OWNERS || poolMax.isEmpty() || poolMax.size()>RatisNodeResourceBudget.MAX_POOLS)throw new IllegalArgumentException("Invalid configured recovery inventory limits");
        if(!Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS))throw new IOException("Configured recovery inventory requires an existing owned directory");
        try {
            for(var entry:poolMax.entrySet()) {
                UUID pool=Objects.requireNonNull(entry.getKey());int limit=Objects.requireNonNull(entry.getValue());
                if(limit<1 || limit>maxGroups || !resources.poolIds().contains(pool))throw new IllegalArgumentException("Invalid configured recovery pool limits");
                pools.put(pool,new PoolIndex(pool,limit));
            }
            try(var traversal=resources.reserve(pools.keySet().iterator().next(),RatisNodeResourceBudget.Kind.PROTECTED,32768,1)) {
                inventory();
                for(var pool:pools.keySet()){Path directory=root.resolve(pool.toString());if(!Files.exists(directory,LinkOption.NOFOLLOW_LINKS)){Files.createDirectory(directory);SessionJournalFiles.force(root);}}
            }
            io=new ThreadPoolExecutor(1,1,0L,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(8),Thread.ofPlatform().daemon(true).name("ratis-configured-recovery-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
        }catch(IOException|RuntimeException|Error failure){groups.clear();slots.clear();for(var index:pools.values())index.close();pools.clear();throw failure;}
    }
    private void inventory()throws IOException {
        int foundPools=0;
        try(var entries=Files.newDirectoryStream(root)) {
            for(var poolPath:entries) {
                if(++foundPools>pools.size())throw new IOException("Foreign configured recovery pool");UUID pool=id(poolPath);
                if(!pools.containsKey(pool) || !Files.isDirectory(poolPath,LinkOption.NOFOLLOW_LINKS))throw new IOException("Undeclared or invalid configured recovery pool");
                try(var children=Files.newDirectoryStream(poolPath)){for(var directory:children){UUID group=id(directory);if(!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid configured recovery group directory");validateFiles(directory);addSlot(pool,group,directory);}}
            }
        }
    }
    private Slot addSlot(UUID pool,UUID group,Path directory)throws IOException {
        if(groups.containsKey(group))throw new IOException("Configured recovery group changed pool");if(slots.size()>=maximum)throw new IOException("Node configured recovery slots exhausted");var index=pools.get(pool);long bytes=index.reserve(directory);
        try {
            var files=Objects.requireNonNull(owner.journal(pool,group,directory));var slot=new Slot(pool,group,directory,files,slots.size());
            synchronized(this){groups.put(group,slot);slots.add(slot);}index.bytes=bytes;index.count++;return slot;
        }catch(IOException|RuntimeException|Error failure){index.restore();throw failure;}
    }
    private static UUID id(Path path)throws IOException {try{String name=path.getFileName().toString();UUID id=UUID.fromString(name);if(!name.equals(id.toString()))throw new IllegalArgumentException("Noncanonical UUID");return id;}catch(IllegalArgumentException invalid){throw new IOException("Invalid configured recovery directory identity",invalid);}}
    private static void validateFiles(Path directory)throws IOException {
        int count=0;try(var entries=Files.newDirectoryStream(directory)){for(var file:entries){String name=file.getFileName().toString();if(++count>3 || !Set.of("recovery","recovery.pending","recovery.lock").contains(name) || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))throw new IOException("Foreign configured recovery file");long size=Files.size(file),limit=name.equals("recovery.lock")?4096:RatisSchemaClientJournal.MAX_BYTES;if(size>limit)throw new IOException("Configured recovery file exceeds bound");}}
    }

    /** The returned observation acknowledges the forced initial journal, independently of host work. */
    CompletableFuture<Void> enqueue(Request request) {
        Objects.requireNonNull(request);final CompletableFuture<Void> pending;final Metadata metadata;final UUID pool;
        synchronized(this){
            if(stopping)return CompletableFuture.failedFuture(new IOException("Configured recoveries stopped"));
            if(actual!=null){if(enqueued!=request)return CompletableFuture.failedFuture(new RejectedExecutionException("Configured recovery operation already active"));return observe(actual);}
            try {if(request.relocation())throw new IOException("Wrong configured recovery purpose");pool=owner.pool(request);if(!pools.containsKey(pool))throw new IOException("Undeclared configured recovery pool");metadata=new Metadata(pool);try{metadata.requireRawBytes(Math.addExact(512,Math.multiplyExact(2,encodedRequestBytes(request))));}catch(Throwable denied){metadata.close();throw denied;}}
            catch(IOException|RuntimeException rejected){return CompletableFuture.failedFuture(rejected);}
            actual=pending=new CompletableFuture<>();enqueued=request;waiters=0;
        }
        try{io.execute(()->enqueueOnIo(request,pool,metadata,pending));}catch(RuntimeException rejected){metadata.close();finish(pending,rejected);}
        synchronized(this){return observe(pending);}
    }
    private void enqueueOnIo(Request request,UUID pool,Metadata metadata,CompletableFuture<Void> pending) {
        boolean borrowed=false;
        try {
            if(isStopping())throw new IOException("Configured recoveries stopped before enqueue");closeActive();
            Slot slot; synchronized(this){slot=groups.get(request.sourceGroup());}
            if(slot!=null && !slot.pool.equals(pool))throw new IOException("Configured recovery group changed pool");
            if(slot==null){Path directory=root.resolve(pool.toString()).resolve(request.sourceGroup().toString());slot=addSlot(pool,request.sourceGroup(),directory);}
            if(!Files.exists(slot.directory,LinkOption.NOFOLLOW_LINKS)){Files.createDirectory(slot.directory);SessionJournalFiles.force(slot.directory.getParent());}
            validateFiles(slot.directory);borrowed=true;open(slot,request,metadata);finish(pending,null);
        }catch(Throwable failure){if(!borrowed)metadata.close();finish(pending,failure);}
    }
    void cycle(){requestStep(false);}
    void advance(){cycle();}
    CompletableFuture<Void> step(){return requestStep(true);}
    private CompletableFuture<Void> requestStep(boolean observe) {
        final CompletableFuture<Void> pending;boolean start=false;
        synchronized(this){if(stopping)return CompletableFuture.failedFuture(new IOException("Configured recoveries stopped"));if(actual==null){actual=new CompletableFuture<>();enqueued=null;waiters=0;start=true;}pending=actual;if(observe && waiters>=MAX_WAITERS)return CompletableFuture.failedFuture(new RejectedExecutionException("Configured recovery observation bound exhausted"));if(observe)waiters++;}
        if(start){try{io.execute(()->stepOnIo(pending));}catch(RuntimeException rejected){finish(pending,rejected);}}
        return observe?pending.copy():pending;
    }
    private CompletableFuture<Void> observe(CompletableFuture<Void> pending){if(waiters>=MAX_WAITERS)return CompletableFuture.failedFuture(new RejectedExecutionException("Configured recovery observation bound exhausted"));waiters++;return pending.copy();}
    private void stepOnIo(CompletableFuture<Void> pending) {
        Slot selected=null;
        try {
            if(isStopping()){finish(pending,null);return;}
            if(active==null){selected=select();if(selected==null){finish(pending,null);return;}var metadata=new Metadata(selected.pool);open(selected,null,metadata);}
            var driver=active;var slot=activeSlot;
            driver.step().whenComplete((phase,failure)->post(()->{
                try {
                    synchronized(this){slot.phase=driver.phase();}visits++;
                    if(failure!=null || phase==RatisConfiguredRecovery.Phase.DONE || visits>=STEPS_PER_VISIT){cursor=(slot.index+1)%slots.size();if(!isStopping())closeActive();}
                    finish(pending,unwrap(failure));
                }catch(Throwable failed){finish(pending,failed);}
            }));
        }catch(Throwable failure){try{Slot failed=activeSlot!=null?activeSlot:selected;if(failed!=null && !slots.isEmpty())cursor=(failed.index+1)%slots.size();closeActive();}catch(Throwable cleanup){failure.addSuppressed(cleanup);}finish(pending,failure);}
    }
    private Slot select(){for(int checked=0;checked<slots.size();checked++){var slot=slots.get(cursor);if(slot.phase!=RatisConfiguredRecovery.Phase.DONE)return slot;cursor=(cursor+1)%slots.size();}return null;}
    private void open(Slot slot,Request request,Metadata metadata)throws IOException {
        try {
            validateFiles(slot.directory);
            var driver=new RatisConfiguredRecovery(slot.directory,binding,request,hosts.create(metadata),io,slot.files,metadata,initial->{if(!initial.sourceGroup().equals(slot.group) || !owner.pool(initial).equals(slot.pool))throw new IOException("Configured recovery journal escaped its pool/group");});
            active=driver;activeSlot=slot;visits=0;synchronized(this){slot.phase=driver.phase();}
        }catch(IOException|RuntimeException|Error failure){metadata.close();throw failure;}
    }
    private void closeActive()throws IOException {if(active==null)return;var previous=active;active=null;activeSlot=null;visits=0;previous.close();}
    private void post(Runnable action){try{io.execute(action);}catch(RuntimeException failure){lastFailure=failure;stopped.completeExceptionally(failure);}}
    private boolean isStopping(){synchronized(this){return stopping;}}
    private void finish(CompletableFuture<Void> pending,Throwable failure) {
        synchronized(this){if(actual!=pending)return;actual=null;enqueued=null;waiters=0;if(failure!=null)lastFailure=failure;}
        if(failure==null)pending.complete(null);else pending.completeExceptionally(failure);if(isStopping())maybeFinishStop();
    }
    synchronized RatisConfiguredRecovery.Phase phase(UUID group){var slot=groups.get(group);return slot==null?null:slot.phase;}
    Throwable failure(){return lastFailure;}
    synchronized int size(){return slots.size();}
    synchronized boolean running(){return actual!=null;}
    synchronized boolean idle(){return actual==null;}

    CompletableFuture<Void> stop() {
        boolean schedule=false;final CompletableFuture<Void> view;
        synchronized(this){stopping=true;if(!stopRequested){stopRequested=true;schedule=true;}if(!stopped.isDone() && stopWaiters>=MAX_WAITERS)return CompletableFuture.failedFuture(new RejectedExecutionException("Configured recovery stop observation bound exhausted"));if(!stopped.isDone())stopWaiters++;view=stopped.copy();}
        if(schedule)post(()->{synchronized(this){if(closed)return;}if(active!=null)active.stop().whenComplete((ignored,failure)->post(()->{if(failure!=null)stopFailure=unwrap(failure);maybeFinishStop();}));else maybeFinishStop();});return view;
    }
    private void maybeFinishStop() {
        synchronized(this){if(!stopping || closed || actual!=null)return;}
        if(active!=null){var drain=active.stop();if(!drain.isDone())return;try{drain.join();}catch(CompletionException failed){stopFailure=unwrap(failed);}active=null;activeSlot=null;}
        synchronized(this){closed=true;groups.clear();slots.clear();lastFailure=null;}
        for(var index:pools.values())index.close();pools.clear();io.shutdown();if(stopFailure==null)stopped.complete(null);else stopped.completeExceptionally(stopFailure);
    }
    void close(Duration timeout)throws IOException {
        Objects.requireNonNull(timeout);long nanos=timeout.toNanos();if(nanos<=0)throw new IllegalArgumentException("Positive configured recovery close timeout required");long deadline=System.nanoTime()+nanos;
        try {
            Throwable cleanup=null;try{stop().get(nanos,TimeUnit.NANOSECONDS);}catch(ExecutionException failed){cleanup=unwrap(failed);}
            long remaining=deadline-System.nanoTime();if(remaining<=0 || !io.awaitTermination(remaining,TimeUnit.NANOSECONDS))throw new IOException("Configured recovery IO did not terminate");
            if(cleanup!=null)throw new IOException("Configured recovery close failed",cleanup);
        }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Interrupted closing configured recoveries",interrupted);}catch(TimeoutException failed){throw new IOException("Configured recovery close timed out",failed);}
    }
    private static Throwable unwrap(Throwable failure){while((failure instanceof CompletionException || failure instanceof ExecutionException) && failure.getCause()!=null)failure=failure.getCause();return failure;}
}
