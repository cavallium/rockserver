package it.cavallium.rockserver.distributed.ratis;

import java.util.Objects;
import java.util.function.LongSupplier;

/** Fixed-point token bucket. Called only under the source service admission monitor. */
final class SourceReadRate {
    private static final long SECOND=1_000_000_000L;
    private final LongSupplier clock;private final long rate,capacity;private long credit,last;
    SourceReadRate(RatisExportSourceService.IoLimits limits,LongSupplier clock){this.clock=Objects.requireNonNull(clock);rate=limits.readBytesPerSecond();capacity=(long)limits.readBurstBytes()*SECOND;credit=capacity;last=clock.getAsLong();}
    private void refill(){long now=clock.getAsLong(),elapsed=now-last;if(elapsed<=0)return;last=now;long missing=capacity-credit;long full=missing/rate+(missing%rate==0?0:1);credit=elapsed>=full?capacity:credit+elapsed*rate;}
    long delay(long bytes){refill();long missing=bytes*SECOND-credit;return missing<=0?0:missing/rate+(missing%rate==0?0:1);}
    void consume(long bytes){credit-=bytes*SECOND;if(credit<0)throw new IllegalStateException("Read dispatch without rate admission");}
}
