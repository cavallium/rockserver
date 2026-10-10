package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisConnectionCloseTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void concurrentCloseSerializesOwnedDependenciesAndDoesNotSkipTheNextOwner() throws Exception {
        var tests=new RatisRocksDBDirectTest(); tests.root=root;
        try (var x=tests.new Fixture(); var replicas=new RatisSchemaReplicaClient(1,(range,owner) -> { throw new IOException("No replica query during close"); }); var workers=Executors.newFixedThreadPool(2)) {
            var outer=x.f.outer;
            var binding=new RatisNodePlan.Catalogue(outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,x.f.directory.ranges().getFirst().replicas());
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var firstCloses=new AtomicInteger(); var nextCloses=new AtomicInteger();
            AutoCloseable first=() -> { firstCloses.incrementAndGet(); entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Dependency close barrier timed out"); };
            AutoCloseable next=nextCloses::incrementAndGet;
            var connection=new RatisRocksDBConnection(URI.create("rockserver+ratis://fixture"),root.resolve("connection"),x.f.schema,x.routed,x.f.scheduler,binding,x.f.catalogue.client,replicas,null,null,
                    (schema,directory,path,ids) -> new RatisRocksDBConnection.Generation(x.adapter,x.direct,null,x.transactions,null,List.of(x.session)),RatisRocksDBConnection.Limits.defaults(),List.of(first,next));
            try {
                var closing=workers.submit(() -> { connection.close(); return null; });
                assertTrue(entered.await(5,TimeUnit.SECONDS));
                var secondThread=new AtomicReference<Thread>(); var started=new CountDownLatch(1);
                var concurrent=workers.submit(() -> { secondThread.set(Thread.currentThread()); started.countDown(); connection.close(); return null; });
                assertTrue(started.await(5,TimeUnit.SECONDS));
                // Both the guard and a wrongly repeated dependency close park here; the counters distinguish them.
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                while (secondThread.get().getState()!=Thread.State.WAITING && secondThread.get().getState()!=Thread.State.TIMED_WAITING && !concurrent.isDone() && System.nanoTime()<deadline) Thread.sleep(1);
                assertTrue(secondThread.get().getState()==Thread.State.WAITING || secondThread.get().getState()==Thread.State.TIMED_WAITING);
                assertEquals(1,firstCloses.get()); assertEquals(0,nextCloses.get()); assertFalse(concurrent.isDone());
                release.countDown(); closing.get(5,TimeUnit.SECONDS); concurrent.get(5,TimeUnit.SECONDS);
                assertEquals(1,firstCloses.get()); assertEquals(1,nextCloses.get()); connection.close();
                assertEquals(1,firstCloses.get()); assertEquals(1,nextCloses.get());
                assertEquals(0,x.f.catalogue.closes.get(),"Borrowed catalogue remains open");
            } finally { release.countDown(); connection.close(); }
        }
    }
}
