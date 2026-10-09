package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import static it.cavallium.rockserver.distributed.ratis.RatisExportSourceRpc.*;

/** Bounded observer views around actual scheduler-owned source work. */
public final class RatisExportSourceService {
    public static final int BUILD=52,READ=53,RELEASE=54;
    private record Key(int kind,String digest) {}
    private final class Work implements Runnable,RWScheduler.RejectionAwareTask {
        final Key key;final int kind;final String job;final long charge,cost;Object request;Throwable rejection;
        final CompletableFuture<byte[]> actual=new CompletableFuture<>();int observers;boolean scheduled,started,finished;
        Work(Key key,int kind,byte[] body,String job,long charge,long cost,Object request){this.key=key;this.kind=kind;this.job=job;this.charge=charge;this.cost=cost;this.request=request;}
        @Override public void run(){synchronized(RatisExportSourceService.this){if(finished)return;started=true;}try{byte[] result=switch(kind){case BUILD->{var r=(BuildRequest)request;try{yield new BuildReply(r,registry.build(r)).encode();}catch(RatisSnapshotLostException lost){yield new BuildReply(r,null).encode();}}case READ->{var r=(ReadRequest)request;yield new ReadReply(r,registry.read(r)).encode();}case RELEASE->{var r=(ReleaseRequest)request;yield new ReleaseReply(release(r)).encode();}default->throw new IllegalArgumentException("Unknown source operation");};finish(this,result,null);}catch(Throwable failure){finish(this,null,failure);}}
        @Override public void reject(RuntimeException failure){synchronized(RatisExportSourceService.this){if(finished)return;if(rejection==null)rejection=failure;if(started)return;}finish(this,null,failure,true);}
    }
    private final RatisExportSourceRegistry registry;private final RatisExportSourceRegistry.Budgets budgets;private final RWScheduler.WorkloadExecutor executor;
    private final ThreadPoolExecutor cleanup;private boolean sweepPending;
    private final Map<Key,Work> work=new LinkedHashMap<>();private final Set<String> releasing=new HashSet<>();private long bytes,releaseBytes;private int releaseCount,runningBuilds;private boolean stopping,closing;private final CompletableFuture<Void> stopped=new CompletableFuture<>();
    public RatisExportSourceService(Path root,RatisExportSourceRegistry.Budgets budgets,RatisExportSourceRegistry.NativeBuilder builder,RWScheduler scheduler)throws IOException {this(new RatisExportSourceRegistry(root,budgets,builder),budgets,scheduler);}
    public RatisExportSourceService(RatisExportSourceRegistry registry,RatisExportSourceRegistry.Budgets budgets,RWScheduler scheduler){this.registry=Objects.requireNonNull(registry);this.budgets=Objects.requireNonNull(budgets);executor=Objects.requireNonNull(scheduler).executor(RequestContext.batch(),OperationFamily.FULL_SCAN_AGGREGATE);cleanup=new ThreadPoolExecutor(1,1,0L,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(budgets.maximumJobs()+1),task->{var thread=new Thread(task,"rockserver-export-cleanup");thread.setDaemon(true);return thread;},new ThreadPoolExecutor.AbortPolicy());}
    private ReleaseRequest release(ReleaseRequest request)throws Exception{registry.release(request);return request;}
    public CompletableFuture<byte[]> submit(UUID addressedGroup,int kind,byte[] body){
        try{
            if(body==null || body.length>MAX_BYTES)throw new IOException("Invalid source request size");
            Object request;JobDefinition definition;long cost,charge;
            switch(kind){case BUILD->{var r=BuildRequest.decode(body);request=r;definition=r.definition();cost=r.intent().limits().maximumFileBytes();charge=2L*body.length+512L;}case READ->{var r=ReadRequest.decode(body);request=r;definition=r.definition();cost=r.maxBytes();charge=2L*body.length+2L*r.maxBytes()+512;}case RELEASE->{var r=ReleaseRequest.decode(body);request=r;definition=r.definition();cost=Math.min(r.definition().limits().maximumDiskBytes(),budgets.maximumDiskBytes());charge=2L*body.length+512L;}default->throw new IOException("Unknown source operation");}
            if(!definition.group().equals(Objects.requireNonNull(addressedGroup)))throw new IOException("Source request addressed group changed");
            String job=definition.jobId()+"-"+definition.group();Key key=new Key(kind,HexFormat.of().formatHex(hash(body)));Work current;
            long observerCharge=kind==READ?((ReadRequest)request).maxBytes()+512L:kind==BUILD?body.length+it.cavallium.rockserver.core.common.distributed.TransactionScope.MAX_BOUND_BYTES+512L:body.length+512L;
            synchronized(this){
                if(stopping)throw new IOException("Source service stopping");current=work.get(key);boolean fresh=current==null;
                if(!fresh && current.observers>=64)throw new IOException("Source observer capacity unavailable");
                if(kind!=RELEASE && releasing.contains(job))throw new IOException("Source job releasing");
                long reservation=observerCharge+(fresh?charge:0);
                if(kind==RELEASE){if(fresh && releaseCount>=budgets.maximumJobs() || fresh && !releasing.contains(job) && releasing.size()>=budgets.maximumJobs() || reservation>budgets.maximumBufferBytes()-releaseBytes)throw new IOException("Source release capacity unavailable");registry.fenceRelease((ReleaseRequest)request);releaseBytes+=reservation;}
                else{if(fresh && work.size()-releaseCount>=budgets.maximumChunks() || fresh && charge>budgets.maximumBufferBytes()-bytes)throw new IOException("Source request capacity unavailable");registry.reserveBuffer(reservation);}
                if(fresh){current=new Work(key,kind,body,job,charge,cost,request);work.put(key,current);if(kind==RELEASE){releasing.add(job);releaseCount++;}else bytes+=charge;}
                current.observers++;
            }
            if(kind==RELEASE)fenceQueued(job);
            Work selected=current;CompletableFuture<byte[]> view=new CompletableFuture<>();
            selected.actual.whenComplete((value,failure)->{try{if(failure==null){if(!view.isDone())view.complete(value.clone());}else view.completeExceptionally(failure);}finally{synchronized(this){selected.observers--;if(kind==RELEASE)releaseBytes-=observerCharge;}if(kind!=RELEASE)registry.freeBuffer(observerCharge);}});
            dispatch();requestSweep();return view;
        }catch(Throwable failure){return CompletableFuture.failedFuture(failure);}
    }
    private void fenceQueued(String job){List<Work> cancelled;synchronized(this){cancelled=work.values().stream().filter(w->w.kind!=RELEASE && w.job.equals(job) && !w.started).toList();}for(Work queued:cancelled)finish(queued,null,new IOException("Source job released before execution"),true);}
    private void dispatch(){List<Work> dispatch=new ArrayList<>();synchronized(this){for(Work next:work.values()){if(next.scheduled || next.kind!=RELEASE && releasing.contains(next.job))continue;
            if(next.kind==RELEASE && work.values().stream().anyMatch(w->w!=next && w.job.equals(next.job) && w.kind!=RELEASE))continue;
            if(next.kind!=RELEASE && work.values().stream().anyMatch(w->w!=next && w.job.equals(next.job) && w.scheduled && w.kind!=RELEASE))continue;
            if(next.kind==BUILD && runningBuilds>=budgets.concurrentBuilds())continue;
            next.scheduled=true;if(next.kind==BUILD)runningBuilds++;dispatch.add(next);
        }}for(Work next:dispatch)try{if(next.kind==RELEASE)cleanup.execute(next);else executor.execute(next,next.cost);}catch(RuntimeException failure){next.reject(failure);}}
    private void requestSweep(){synchronized(this){if(stopping || sweepPending)return;sweepPending=true;}try{cleanup.execute(()->{try{registry.sweepReleased(16);}catch(Exception ignored){/* Transient token failures retain the bounded cursor for the next admission. */}finally{synchronized(this){sweepPending=false;}checkStopped();}});}catch(RejectedExecutionException full){synchronized(this){sweepPending=false;}}}
    private void finish(Work task,byte[] result,Throwable failure){finish(task,result,failure,false);}
    private void finish(Work task,byte[] result,Throwable failure,boolean queuedOnly){synchronized(this){if(task.finished || queuedOnly && task.started)return;if(task.rejection!=null)failure=task.rejection;task.finished=true;task.request=null;work.remove(task.key);if(task.kind==BUILD && task.scheduled)runningBuilds--;if(task.kind==RELEASE){releaseCount--;releaseBytes-=task.charge;if(failure==null)releasing.remove(task.job);}else {bytes-=task.charge;registry.freeBuffer(task.charge);}}
        if(failure==null)task.actual.complete(result);else task.actual.completeExceptionally(failure);dispatch();checkStopped();}
    /** Fence submissions, drain actual work, and release the stable registry owner without cancelling it. */
    public CompletableFuture<Void> stop(){synchronized(this){stopping=true;}dispatch();checkStopped();return stopped.copy();}
    private void checkStopped(){synchronized(this){if(!stopping || !work.isEmpty() || sweepPending || closing)return;closing=true;}try{registry.close();cleanup.shutdown();stopped.complete(null);}catch(Throwable failure){cleanup.shutdown();stopped.completeExceptionally(failure);}}
}
