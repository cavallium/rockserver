package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisDirectCompletionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void cancelledAdapterObserverRetainsActualCompletionAndOriginalResult() throws Exception {
        var tests=new RatisRocksDBDirectTest(); tests.root=root;
        try (var x=tests.new Fixture()) {
            var key=RatisAtomicClientTest.key(2);
            x.adapter.api(RequestContext.batch()).putAsync(0,71,key,RatisAtomicClientTest.value(1),RequestType.<Buf>none()).get(5,TimeUnit.SECONDS);
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            x.direct.afterAcknowledgement(() -> {
                entered.countDown();
                try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("ACK completion barrier timed out"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            });
            var request=new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0,71,key,RatisAtomicClientTest.value(2),RequestType.<Buf>previous());
            var dispatched=x.adapter.dispatchOwned(request,RequestContext.batch());
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertFalse(dispatched.drained().isDone());
                dispatched.cancelObserver().run();
                assertTrue(((CompletableFuture<?>)dispatched.observer()).isCancelled());
                assertFalse(dispatched.drained().isDone()); assertEquals(1,x.direct.activeRequests());
                assertThrows(IOException.class,x.direct::close);
            } finally { release.countDown(); }
            dispatched.drained().get(5,TimeUnit.SECONDS); x.direct.afterAcknowledgement(null);
            assertEquals(0,x.direct.activeRequests()); assertEquals(RatisRocksDBDirect.RecoveryState.COMPLETE,x.direct.recoveryState());
            var saved=(Buf)x.direct.resumeRetained(RequestContext.batch()).get(5,TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{1},saved.toByteArray()); assertArrayEquals(new byte[]{2},x.f.get(71,key));
            assertEquals(2,x.writes.size(),"Recovery returns the saved callback without another native command");
        }
    }
}
