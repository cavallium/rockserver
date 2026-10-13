package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisDiskRunwayRecoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void blockedActualWholePoolFlushAndSnapshotReplyHoldSingleRecoveryOwnerThroughStop()throws Exception {
        var clock=new java.util.concurrent.atomic.AtomicLong(10);var runway=new RatisDiskRunway(RatisDiskRunwayTest.policy(),clock::get);RatisDiskRunwayTest.publish(runway,10,500);var scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(4,1,1,16,16,"runway-recovery");
        try(var pool=it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.open(root.resolve("native"),it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()))){
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var snapshotStarted=new CountDownLatch(1);var snapshot=new CompletableFuture<Void>();pool.localMaintenanceObserver(id->{entered.countDown();try{assertTrue(release.await(5,TimeUnit.SECONDS));}catch(InterruptedException failed){throw new AssertionError(failed);}});
            var target=new RatisDiskRunwayRecovery.Target(RatisDiskRunwayTest.POOL,new UUID(2,2),10,()->{try{pool.flushWholePool();}catch(Exception failed){throw new CompletionException(failed);}},()->{snapshotStarted.countDown();return snapshot;});var recovery=new RatisDiskRunwayRecovery(scheduler,runway,()->List.of(target));
            try {recovery.request();assertTrue(entered.await(5,TimeUnit.SECONDS));assertEquals(50,runway.stats().pendingProtectedBytes());var cancelled=recovery.stop();assertTrue(cancelled.cancel(false));var actual=recovery.stop();assertFalse(actual.isDone());release.countDown();assertTrue(snapshotStarted.await(5,TimeUnit.SECONDS));assertFalse(actual.isDone());snapshot.complete(null);actual.get(5,TimeUnit.SECONDS);assertEquals(50,runway.stats().pendingProtectedBytes());clock.incrementAndGet();RatisDiskRunwayTest.publish(runway,clock.get(),500);assertEquals(0,runway.stats().pendingProtectedBytes());}
            finally {release.countDown();snapshot.complete(null);recovery.stop().get(5,TimeUnit.SECONDS);scheduler.dispose();}
        }
    }
}
