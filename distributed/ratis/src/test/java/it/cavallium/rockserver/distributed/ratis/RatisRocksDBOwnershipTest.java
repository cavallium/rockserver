package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisRocksDBOwnershipTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void adapterShutdownResolvesAnUnknownCommitInsteadOfWedgeingItsOnlyRecoveryPath() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root,true); var transactions=RatisRocksDBAdapterTest.client(f,f.data);
             var identities=RatisTransactionIdentities.create(Files.createDirectory(root.resolve("ids")),f.grant,2048,f.scheduler);
             var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,identities,null,null)) {
            var api=adapter.api(RequestContext.batch()); long id=api.openTransactionAsync(RatisRocksDBAdapterTest.scope(71,72,73,74), Duration.ofSeconds(30)).get(10,TimeUnit.SECONDS);
            api.putAsync(id,71,RatisAtomicClientTest.key(2),RatisAtomicClientTest.value(7),RequestType.<Buf>none()).get(5,TimeUnit.SECONDS);
            f.losePrepareGroup=f.groups.getLast(); assertThrows(ExecutionException.class,() -> api.closeTransactionAsync(id,true).get(10,TimeUnit.SECONDS));
            long reserved=identities.nextSequence(); int captures=f.captureRequests.get();
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(10); boolean closed=false;
            while (!closed && System.nanoTime()<until) try { adapter.close(); closed=true; } catch (IOException owned) { Thread.sleep(1); }
            assertTrue(closed,"Shutdown must retain and resolve the same commit rather than reject its own recovery forever");
            assertEquals(0,adapter.stats().transactions()); assertEquals(reserved,identities.nextSequence()); assertEquals(captures,f.captureRequests.get()); assertEquals(0,f.opens.get());
            assertArrayEquals(new byte[]{7},f.get(71,RatisAtomicClientTest.key(2)));
        }
    }
    @Test void accumulatedCrossGroupPrefixUsesAggregateBudgetInsteadOfTheSingleCommandLimit() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root,true); var transactions=RatisRocksDBAdapterTest.client(f,f.data)) {
            var allocations=new RatisRocksDBAdapterTest.Allocations(f);
            try (var adapter=new RatisRocksDBAdapter(RatisTransactionBindings.of(f.schema,f.directory,(transactions).bindings().domain((f.schema).domainId()).client()),transactions,allocations,null,null)) {
                var api=adapter.api(RequestContext.batch()); long id=api.openTransactionAsync(RatisRocksDBAdapterTest.scope(71,72,73,74), Duration.ofSeconds(30)).get(10,TimeUnit.SECONDS);
                var value=Buf.wrap(new byte[1100*1024]);
                api.putAsync(id,71,RatisAtomicClientTest.key(2),value,RequestType.<Buf>none()).get(10,TimeUnit.SECONDS);
                api.putAsync(id,71,RatisAtomicClientTest.key(200),value,RequestType.<Buf>none()).get(10,TimeUnit.SECONDS);
                assertTrue(api.closeTransactionAsync(id,true).get(10,TimeUnit.SECONDS));
                assertEquals(value.size(),f.get(71,RatisAtomicClientTest.key(2)).length); assertEquals(value.size(),f.get(71,RatisAtomicClientTest.key(200)).length);
                assertEquals(1,allocations.count.get()); assertEquals(0,f.opens.get());
            }
        }
    }
}
