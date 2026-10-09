package it.cavallium.rockserver.distributed.ratis;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisExportSourceRpc.MAX_READ_BYTES;

class SourceReadRateTest {
    @Test void fractionalRefillAndLongIdleAreOverflowSafe(){var clock=new AtomicLong();var bucket=new SourceReadRate(new RatisExportSourceService.IoLimits(0,3,MAX_READ_BYTES),clock::get);bucket.consume(MAX_READ_BYTES);assertEquals(333_333_334L,bucket.delay(1));clock.set(333_333_333);assertEquals(1,bucket.delay(1));clock.incrementAndGet();assertEquals(0,bucket.delay(1));bucket.consume(1);clock.set(Long.MAX_VALUE);assertEquals(0,bucket.delay(MAX_READ_BYTES));bucket.consume(MAX_READ_BYTES);assertEquals(349_525_333_333_334L,bucket.delay(MAX_READ_BYTES));}
    @Test void validatesFiniteOperationalBounds(){assertThrows(IllegalArgumentException.class,()->new RatisExportSourceService.IoLimits(-1,1,MAX_READ_BYTES));assertThrows(IllegalArgumentException.class,()->new RatisExportSourceService.IoLimits(0,0,MAX_READ_BYTES));assertThrows(IllegalArgumentException.class,()->new RatisExportSourceService.IoLimits(0,Long.MAX_VALUE,MAX_READ_BYTES));assertThrows(IllegalArgumentException.class,()->new RatisExportSourceService.IoLimits(0,1,MAX_READ_BYTES-1));assertThrows(IllegalArgumentException.class,()->new RatisExportSourceService.IoLimits(0,1,Integer.MAX_VALUE));}
}
