package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import reactor.core.scheduler.Scheduler;
import reactor.core.Disposable;
import java.util.concurrent.TimeUnit;

/** One coalesced CONTROL sampler; only new background admission is throttled. */
final class RatisPoolPressureService {
    @FunctionalInterface interface Sampler { DistributedStoragePool.WorkloadPressure sample() throws Exception; }
    private final RWScheduler scheduler;
    private final Sampler sampler;
    private final RatisDiskRunway runway;private final RatisDiskRunwayService census;private final RatisDiskRunwayRecovery recovery;
    private final RatisTransactionRecoveryService polling;
    private volatile DistributedStoragePool.WorkloadPressure latest;
    private Disposable timer;
    private boolean started,stopping;
    RatisPoolPressureService(RWScheduler scheduler,Sampler sampler,RatisDiskRunway runway,RatisDiskRunwayService census,RatisDiskRunwayRecovery recovery) {
        this.scheduler=Objects.requireNonNull(scheduler);this.sampler=Objects.requireNonNull(sampler);this.runway=runway;this.census=census;this.recovery=recovery;
        polling=new RatisTransactionRecoveryService(scheduler,this::sample);
    }
    void start(Scheduler clock) {
        synchronized(this){if(started || stopping)throw new IllegalStateException("Pressure sampler already started/stopped");started=true;}
        scheduler.setStoragePressure(true);
        var scheduled=clock.schedulePeriodically(this::request,0,1,TimeUnit.SECONDS);
        synchronized(this){if(stopping)scheduled.dispose();else timer=scheduled;}
    }
    private void request(){
        var sample=latest;long age=sample==null ? Long.MAX_VALUE : System.nanoTime()-sample.sampledAtNanos();
        if(age<0 || age>Duration.ofSeconds(5).toNanos())scheduler.setStoragePressure(true);
        if(census!=null)census.request();
        if(runway!=null && runway.state()!=RatisDiskRunway.State.CLEAR)scheduler.setStoragePressure(true);
        if(recovery!=null)recovery.request();
        polling.requestQuantum();
    }
    CompletableFuture<Void> refresh(){return polling.tick();}
    DistributedStoragePool.WorkloadPressure latest(){return latest;}
    Throwable failure(){var failure=polling.lastFailure();return failure!=null || census==null?failure:census.failure();}
    CompletableFuture<Void> stop(){synchronized(this){stopping=true;if(timer!=null){timer.dispose();timer=null;}}return CompletableFuture.allOf(polling.stop(),census==null?CompletableFuture.completedFuture(null):census.stop(),recovery==null?CompletableFuture.completedFuture(null):recovery.stop());}
    private void sample() throws Exception {
        try {
            var result=Objects.requireNonNull(sampler.sample());if(census!=null)census.sampleFree();long age=System.nanoTime()-result.sampledAtNanos();
            if(age<0 || age>Duration.ofSeconds(5).toNanos())throw new IOException("Pool pressure sample is stale");
            latest=result;scheduler.setStoragePressure(result.pressured() || runway!=null && runway.state()!=RatisDiskRunway.State.CLEAR);
        } catch(Exception failure){if(runway!=null)runway.invalidateFree();scheduler.setStoragePressure(true);throw failure;}
    }
}
