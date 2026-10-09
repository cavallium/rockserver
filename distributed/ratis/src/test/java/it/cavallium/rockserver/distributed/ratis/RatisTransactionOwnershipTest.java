package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisTransactionOwnershipTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void cancellingOneCleanupObserverPreservesActualOwnershipAndLaterBoundedViews() throws Exception {
        var helper=new RatisTransactionClientTest(); helper.root=root;
        var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        try (var f=new RatisAtomicClientTest.Fixture(root,true)) {
            helper.seed(f); Path capture=root.resolve("cleanup-snapshot.bin");
            try (var original=helper.client(f); var lease=original.open(capture,root.resolve("cleanup-commit.bin"),helper.identity(f),helper.scope(f),RatisTransactionClientTest.TTL,RequestContext.batch())) {
                helper.acquired(lease,f); lease.stage(helper.prefix(),List.of(),RequestContext.batch()).get(5,TimeUnit.SECONDS);
            }
            int captures=f.captureRequests.get();
            f.beforeSnapshotRelease=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Cleanup barrier timed out"); } catch (InterruptedException interrupted) { throw new AssertionError(interrupted); } };
            try (var client=helper.client(f); var cleanup=client.reopenForRelease(capture,RequestContext.batch())) {
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); long bytes=client.stats().snapshots().bytes(); assertTrue(bytes>0);
                    var cancelled=cleanup.completion(); assertTrue(cancelled.cancel(false));
                    var next=cleanup.completion(); assertFalse(next.isCancelled()); assertFalse(next.isDone());
                    assertEquals(bytes,client.stats().snapshots().bytes()); assertThrows(IOException.class,cleanup::close); assertEquals(0,f.opens.get());
                    var waiters=new ArrayList<CompletableFuture<Void>>();
                    for (int n=0;n<RatisSnapshotClient.MAX_WAITERS+1;n++) waiters.add(cleanup.completion());
                    assertTrue(waiters.getLast().isCompletedExceptionally());
                    release.countDown(); next.get(5,TimeUnit.SECONDS);
                } finally { release.countDown(); }
                cleanup.close(); assertEquals(0,client.stats().snapshots().bytes()); assertEquals(captures,f.captureRequests.get());
                assertArrayEquals(new byte[]{1},f.get(71,RatisAtomicClientTest.key(2)));
                for (var group:f.groups) assertEquals(0,f.stores.get(group).activeTrackedSnapshotPins());
            }
        } finally { release.countDown(); }
    }
}
