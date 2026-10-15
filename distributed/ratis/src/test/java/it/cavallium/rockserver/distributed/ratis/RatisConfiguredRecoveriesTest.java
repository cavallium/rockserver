package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Manager ownership with the actual source image producer and controlled receiver ports. */
class RatisConfiguredRecoveriesTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static final UUID POOL=new UUID(993,1);
    private static RatisNodeResourceBudget budget(long bytes){var limits=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(1<<20,16),new RatisNodeResourceBudget.Lane(bytes,16));return new RatisNodeResourceBudget(limits,Map.of(POOL,limits));}
    private static void step(RatisConfiguredRecoveries manager)throws Exception{manager.step().get(10,TimeUnit.SECONDS);}
    final class Fixture implements AutoCloseable {
        final RatisConfiguredRecoveryTest helper=new RatisConfiguredRecoveryTest();
        final RatisConfiguredRecoveryTest.Fixture source;
        final AtomicInteger claims=new AtomicInteger();
        volatile CountDownLatch registered;
        Fixture()throws Exception{helper.root=root;source=helper.new Fixture();}
        RatisConfiguredRecoveries open(Path path,RatisNodeResourceBudget resources)throws Exception {
            if(!Files.exists(path))Files.createDirectory(path);
            return new RatisConfiguredRecoveries(path,source.binding,2,Map.of(POOL,2),resources,new RatisConfiguredRecoveries.Owner(){
                public UUID pool(Request request)throws IOException{try{requireRequest(source.initial,request);return POOL;}catch(IllegalArgumentException invalid){throw new IOException("Wrong fixture request",invalid);}}
                public RatisJournalAccess journal(UUID pool,UUID group,Path directory){assertEquals(POOL,pool);assertEquals(source.initial.sourceGroup(),group);claims.incrementAndGet();return RatisJournalAccess.standalone();}
            },admission->new RatisConfiguredRecovery.Host(){
                public CompletableFuture<Prepared> prepare(Request request){return source.prepare(request).thenApply(value->{try{admission.beforeReply(value.encodedSize());return value;}catch(IOException denied){throw new CompletionException(denied);}});}
                public CompletableFuture<byte[]> register(Prepared prepared){var pending=source.register(prepared);if(registered!=null)registered.countDown();return pending;}
                public CompletableFuture<Prepared> transfer(Request request){return source.transfer(request);}
                public CompletableFuture<RatisRecoveryImageRpc.Evidence> probe(RatisRecoveryImageRpc.Request request){return source.probe(request);}
                public CompletableFuture<RatisSourceHeadRpc.Head> head(RatisSourceHeadRpc.Request request){return source.head(request);}
                public CompletableFuture<Boolean> complete(Prepared prepared,RatisSourceHeadRpc.Head head){return source.complete(prepared,head);}
                public CompletableFuture<Void> release(Prepared prepared){return source.release(prepared);}
            });
        }
        public void close()throws Exception{source.close();}
    }
    @Test void coldRegisterReleaseAndCompleteResumeWithoutReplayingSettledPorts()throws Exception {
        try(var fixture=new Fixture()) {
            var resources=budget(64L<<20);var path=root.resolve("manager");UUID group=fixture.source.initial.sourceGroup();
            var manager=fixture.open(path,resources);manager.enqueue(fixture.source.initial).get(10,TimeUnit.SECONDS);assertEquals(RatisConfiguredRecovery.Phase.PREPARE,manager.phase(group));assertEquals(0,fixture.source.prepares);
            step(manager);assertEquals(RatisConfiguredRecovery.Phase.REGISTER,manager.phase(group));manager.close(Duration.ofSeconds(10));assertEquals(0,resources.stats().protectedLane().bytes());
            manager=fixture.open(path,resources);assertEquals(1,fixture.source.prepares,"Inventory does not dispatch hosts");assertNull(manager.phase(group));step(manager);assertEquals(RatisConfiguredRecovery.Phase.TRANSFER,manager.phase(group));
            for(int steps=0;steps<8 && manager.phase(group)!=RatisConfiguredRecovery.Phase.RELEASE;steps++)step(manager);assertEquals(RatisConfiguredRecovery.Phase.RELEASE,manager.phase(group));manager.close(Duration.ofSeconds(10));
            fixture.source.loseRelease=true;manager=fixture.open(path,resources);var releaseRetry=manager;assertThrows(ExecutionException.class,()->step(releaseRetry));assertEquals(RatisConfiguredRecovery.Phase.RELEASE,manager.phase(group));manager.close(Duration.ofSeconds(10));
            manager=fixture.open(path,resources);step(manager);assertEquals(RatisConfiguredRecovery.Phase.HEAD,manager.phase(group));step(manager);assertEquals(RatisConfiguredRecovery.Phase.COMPLETE,manager.phase(group));fixture.source.loseCompletion=true;var completionRetry=manager;assertThrows(ExecutionException.class,()->step(completionRetry));manager.close(Duration.ofSeconds(10));
            manager=fixture.open(path,resources);step(manager);assertEquals(RatisConfiguredRecovery.Phase.DONE,manager.phase(group));step(manager);assertEquals(1,fixture.source.prepares);assertEquals(1,fixture.source.registers);assertEquals(2,fixture.source.releases);assertEquals(2,fixture.source.completions);assertEquals(1,fixture.source.heads);assertEquals(1,fixture.source.source.pinCloses.get());assertEquals(1,fixture.source.source.durableReleases.get());assertEquals(5,fixture.claims.get());manager.close(Duration.ofSeconds(10));assertEquals(new RatisNodeResourceBudget.Usage(0,0),resources.stats().protectedLane());
        }
    }
    @Test void deniedWorkspaceNeitherCreatesInitialSlotNorDispatchesOrRewritesColdJournal()throws Exception {
        try(var fixture=new Fixture()) {
            var resources=budget(65536);var path=root.resolve("denied");var manager=fixture.open(path,resources);var occupied=resources.reserve(POOL,RatisNodeResourceBudget.Kind.PROTECTED,65536-4096,1);
            var initialDenied=manager;assertThrows(ExecutionException.class,()->initialDenied.enqueue(fixture.source.initial).get(10,TimeUnit.SECONDS));assertEquals(0,fixture.claims.get());assertEquals(0,fixture.source.prepares);assertFalse(Files.exists(path.resolve(POOL.toString()).resolve(fixture.source.initial.sourceGroup().toString())));occupied.close();manager.close(Duration.ofSeconds(10));
            Path slot=path.resolve(POOL.toString()).resolve(fixture.source.initial.sourceGroup().toString());
            try(var driver=fixture.source.open(slot,RatisJournalAccess.standalone())){assertEquals(RatisConfiguredRecovery.Phase.REGISTER,RatisConfiguredRecoveryTest.step(driver));}
            byte[] canonical=Files.readAllBytes(slot.resolve("recovery"));manager=fixture.open(path,resources);long indexBytes=resources.stats().protectedLane().bytes();occupied=resources.reserve(POOL,RatisNodeResourceBudget.Kind.PROTECTED,65536-indexBytes-4096,1);var cold=manager;assertThrows(ExecutionException.class,()->step(cold));assertArrayEquals(canonical,Files.readAllBytes(slot.resolve("recovery")));assertEquals(1,fixture.source.prepares);assertEquals(0,fixture.source.registers);occupied.close();assertEquals(indexBytes,resources.stats().protectedLane().bytes());manager.close(Duration.ofSeconds(10));assertEquals(0,resources.stats().protectedLane().owners());
        }
    }
    @Test void longRetainedPathsRequireCreditBeforeJournalAuthorityIsAcquired()throws Exception {
        try(var fixture=new Fixture()) {
            Path directory=root.resolve("long-index");for(int depth=0;depth<12;depth++)directory=directory.resolve("directory-"+"x".repeat(180));Files.createDirectories(directory.resolve(POOL.toString()).resolve(fixture.source.initial.sourceGroup().toString()));
            var resources=budget(65536);Path journals=directory;
            assertThrows(IOException.class,()->{var unexpected=fixture.open(journals,resources);unexpected.close(Duration.ofSeconds(10));fail("Long retained file paths require more than the available index credit");});assertEquals(0,fixture.claims.get());assertEquals(0,fixture.source.prepares);assertEquals(new RatisNodeResourceBudget.Usage(0,0),resources.stats().protectedLane());
        }
    }
    @Test void cancelledObserversAndStopWaitForActualBlockedHostAndReleaseAllHeapCredits()throws Exception {
        try(var fixture=new Fixture()) {
            var resources=budget(64L<<20);var manager=fixture.open(root.resolve("blocked"),resources);manager.enqueue(fixture.source.initial).get(10,TimeUnit.SECONDS);step(manager);
            fixture.source.blockedRegistration=new CompletableFuture<>();fixture.registered=new CountDownLatch(1);var observed=manager.step();assertTrue(fixture.registered.await(10,TimeUnit.SECONDS));assertTrue(manager.running());assertTrue(observed.cancel(false));
            for(int observers=0;observers<15;observers++)manager.step();assertThrows(ExecutionException.class,()->manager.step().get(10,TimeUnit.SECONDS));for(int tick=0;tick<100;tick++)manager.cycle();
            assertThrows(ExecutionException.class,()->manager.enqueue(fixture.source.initial).get(10,TimeUnit.SECONDS));var cancelledStop=manager.stop();cancelledStop.cancel(false);var actualStop=manager.stop();for(int observers=0;observers<14;observers++)manager.stop().cancel(false);assertThrows(ExecutionException.class,()->manager.stop().get(10,TimeUnit.SECONDS));assertFalse(actualStop.isDone());assertEquals(2,resources.stats().protectedLane().owners());assertTrue(resources.stats().protectedLane().bytes()>4096+512);
            fixture.source.blockedRegistration.complete(fixture.source.registration.clone());actualStop.get(10,TimeUnit.SECONDS);manager.close(Duration.ofSeconds(10));assertEquals(new RatisNodeResourceBudget.Usage(0,0),resources.stats().protectedLane());assertEquals(1,fixture.source.registers);
        }
    }
}
