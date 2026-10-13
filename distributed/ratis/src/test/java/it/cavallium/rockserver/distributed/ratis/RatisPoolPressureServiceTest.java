package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisPoolPressureServiceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void actualNativePressureControlsBackgroundAdmissionAndFailedSamplesFailClosed() throws Exception {
        try(var fixture=new RatisTopologyFreezeTest.Fixture(root)) {
            var scheduler=RWScheduler.forTesting(1,1,1,32,1,"pool-pressure");var reserve=new AtomicLong();
            var service=new RatisPoolPressureService(scheduler,()->fixture.pool.currentWorkloadPressure(reserve.get()),null,null,null);
            try {
                service.refresh().get(5,TimeUnit.SECONDS);assertFalse(scheduler.isStoragePressure());assertNull(service.failure());
                reserve.set(Long.MAX_VALUE);service.refresh().get(5,TimeUnit.SECONDS);assertTrue(scheduler.isStoragePressure());
                assertTrue(service.latest().diskPressure());assertEquals(0,fixture.store.activeSnapshotPins());
                var stale=new RatisPoolPressureService(scheduler,()->new DistributedStoragePool.WorkloadPressure(false,0,false,1,System.nanoTime()-TimeUnit.SECONDS.toNanos(6)),null,null,null);
                try {assertThrows(ExecutionException.class,()->stale.refresh().get(5,TimeUnit.SECONDS));assertTrue(scheduler.isStoragePressure());}
                finally {stale.stop().get(5,TimeUnit.SECONDS);}
                reserve.set(0);service.refresh().get(5,TimeUnit.SECONDS);assertFalse(scheduler.isStoragePressure());
            } finally {service.stop().get(5,TimeUnit.SECONDS);scheduler.dispose();}
        }
    }
    @Test void stopRetainsActualSamplerOwnershipThroughBlockedObservation() throws Exception {
        var scheduler=RWScheduler.forTesting(1,1,1,32,1,"pressure-drain");
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var service=new RatisPoolPressureService(scheduler,()->{
            entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new TimeoutException("Fixture sample timed out");
            return new DistributedStoragePool.WorkloadPressure(false,0,false,1,System.nanoTime());
        },null,null,null);
        try {
            var observing=service.refresh();assertTrue(entered.await(5,TimeUnit.SECONDS));var stopped=service.stop();
            assertFalse(stopped.isDone());release.countDown();observing.get(5,TimeUnit.SECONDS);stopped.get(5,TimeUnit.SECONDS);
        } finally {release.countDown();service.stop().get(5,TimeUnit.SECONDS);scheduler.dispose();}
    }
}
