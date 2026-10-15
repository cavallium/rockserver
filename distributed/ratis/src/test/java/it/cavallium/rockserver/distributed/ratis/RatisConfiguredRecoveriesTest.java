package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
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
    @Test void lostRenewalJournalAcknowledgementInvalidatesCachedDoneAndResumesWithoutAnotherEnqueue()throws Exception {
        for(boolean transition:List.of(false,true))try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("renewal-source-"+transition),false)) {
            var groupImage=NativeRangeImage.write(root.resolve("renewal-group-"+transition),source.store(source.firstDirectory.ranges().getFirst().groupId()),new NativeRangeImage.Limits(4<<20,32<<20,8));
            var split=source.split();var range=split.parents().getFirst();var store=source.store(range.groupId());
            var request=new Request(new UUID(993,3),split.header(),source.plan.catalogue().groupId(),source.local,Purpose.ANCESTOR_METADATA,range.groupId(),split.sourceDirectory());
            var image=NativeRangeImage.writeMetadata(root.resolve("renewal-meta-"+transition),store,new NativeRangeImage.Limits(4<<20,32<<20,8));var cfg=store.configuration();byte[] registration=new byte[32];registration[0]=1;
            var initial=transition?new Request(request.transferId(),null,request.catalogue(),request.receiver(),Purpose.CURRENT_LEAF,range.groupId(),split.sourceDirectory()):request;
            var lost=new java.util.concurrent.atomic.AtomicBoolean();var prepares=new AtomicInteger();var files=new RatisConfiguredRecoveryTest.Forwarding(){@Override public void write(Path path,byte[] body,int maximum)throws IOException{super.write(path,body,maximum);if(lost.getAndSet(false))throw new IOException("Lost forced renewal journal acknowledgement");}};
            Path path=Files.createDirectory(root.resolve("renewal-manager-"+transition));var resources=budget(64L<<20);
            var manager=new RatisConfiguredRecoveries(path,new byte[32],2,Map.of(POOL,2),resources,new RatisConfiguredRecoveries.Owner(){
                public UUID pool(Request value){requireRequest((value.metadataOnly()?request:initial).successor(value.transferId()),value);return POOL;}
                public RatisJournalAccess journal(UUID pool,UUID group,Path directory){return files;}
            },admission->new RatisConfiguredRecovery.Host(){
                Prepared prepared;
                public CompletableFuture<Prepared> prepare(Request value){prepares.incrementAndGet();prepared=new Prepared(value,value.metadataOnly()?image:groupImage,source.first,source.local,cfg,Map.of(),value.metadataOnly()?store.topologyFreeze():null);return CompletableFuture.completedFuture(prepared);}
                public CompletableFuture<byte[]> register(Prepared value){return CompletableFuture.completedFuture(registration.clone());}
                public CompletableFuture<Prepared> transfer(Request value){return CompletableFuture.completedFuture(prepared);}
                public CompletableFuture<RatisRecoveryImageRpc.Evidence> probe(RatisRecoveryImageRpc.Request value){return CompletableFuture.completedFuture(new RatisRecoveryImageRpc.Evidence(value.digest(),TopologyChange.Owner.of(value.source().receiver()),(value.source().metadataOnly()?image:groupImage).position(),cfg,registration,true));}
                public CompletableFuture<RatisSourceHeadRpc.Head> head(RatisSourceHeadRpc.Request value){return CompletableFuture.completedFuture(new RatisSourceHeadRpc.Head(value,new ApplyPosition(1,20),split.sourceDirectory(),source.local,(value.source().metadataOnly()?image:groupImage).position(),cfg));}
                public CompletableFuture<Boolean> complete(Prepared value,RatisSourceHeadRpc.Head head){return CompletableFuture.completedFuture(true);}
                public CompletableFuture<Void> release(Prepared value){return CompletableFuture.completedFuture(null);}
            });
            try {
                manager.enqueue(initial).get(10,TimeUnit.SECONDS);if(transition){assertThrows(ExecutionException.class,()->manager.enqueue(request.successor(new UUID(993,5))).get(10,TimeUnit.SECONDS),"Unfinished CURRENT keeps its original purpose");assertEquals(0,prepares.get());}for(int steps=0;steps<16 && manager.phase(range.groupId())!=RatisConfiguredRecovery.Phase.DONE;steps++)step(manager);assertEquals(RatisConfiguredRecovery.Phase.DONE,manager.phase(range.groupId()));assertEquals(1,prepares.get());
                lost.set(true);assertThrows(ExecutionException.class,()->manager.enqueue(request.successor(new UUID(993,4))).get(10,TimeUnit.SECONDS));assertNotNull(manager.failure());assertNull(manager.phase(range.groupId()),"The cached DONE must not hide a durably forced renewal");assertEquals(1,prepares.get());
                step(manager);assertEquals(RatisConfiguredRecovery.Phase.REGISTER,manager.phase(range.groupId()));assertEquals(2,prepares.get());assertNull(manager.failure());
            }finally{manager.close(Duration.ofSeconds(10));}
            assertEquals(new RatisNodeResourceBudget.Usage(0,0),resources.stats().protectedLane());
        }
    }

    @Test void discoveryBetweenSuccessfulStepsRotatesBothUnfinishedRecoverySlots()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("fair-source"),false)) {
            var prepared=new HashMap<UUID,Prepared>();var registrations=new HashMap<UUID,AtomicInteger>();
            for(var directory:List.of(source.firstDirectory,source.secondDirectory)) {
                var range=directory.ranges().getFirst();var store=source.store(range.groupId());
                var request=new Request(UUID.randomUUID(),null,source.plan.catalogue().groupId(),range.replicas().getFirst(),Purpose.CURRENT_LEAF,range.groupId(),directory);
                var image=NativeRangeImage.write(root.resolve(range.groupId()+".group"),store,new NativeRangeImage.Limits(4<<20,32<<20,8));
                prepared.put(range.groupId(),new Prepared(request,image,directory.domainId().equals(source.first.domainId())?source.first:source.second,range.replicas().getFirst(),store.configuration(),Map.of(),null));registrations.put(range.groupId(),new AtomicInteger());
            }
            var resources=budget(64L<<20);var path=Files.createDirectory(root.resolve("fair-manager"));byte[] identity=new byte[32];identity[0]=1;
            var manager=new RatisConfiguredRecoveries(path,new byte[32],2,Map.of(POOL,2),resources,new RatisConfiguredRecoveries.Owner(){
                public UUID pool(Request request){requireRequest(prepared.get(request.sourceGroup()).request(),request);return POOL;}
                public RatisJournalAccess journal(UUID pool,UUID group,Path directory){return RatisJournalAccess.standalone();}
            },admission->new RatisConfiguredRecovery.Host(){
                public CompletableFuture<Prepared> prepare(Request request){var value=prepared.get(request.sourceGroup());try{admission.beforeReply(value.encodedSize());return CompletableFuture.completedFuture(value);}catch(IOException denied){return CompletableFuture.failedFuture(denied);}}
                public CompletableFuture<byte[]> register(Prepared value){registrations.get(value.request().sourceGroup()).incrementAndGet();return CompletableFuture.completedFuture(identity.clone());}
                public CompletableFuture<Prepared> transfer(Request request){return CompletableFuture.completedFuture(prepared.get(request.sourceGroup()));}
                public CompletableFuture<RatisRecoveryImageRpc.Evidence> probe(RatisRecoveryImageRpc.Request request){var value=prepared.get(request.source().sourceGroup());return CompletableFuture.completedFuture(new RatisRecoveryImageRpc.Evidence(request.digest(),TopologyChange.Owner.of(value.request().receiver()),value.image().position(),value.sourceCfg(),identity,true));}
                public CompletableFuture<RatisSourceHeadRpc.Head> head(RatisSourceHeadRpc.Request request){var value=prepared.get(request.source().sourceGroup());return CompletableFuture.completedFuture(new RatisSourceHeadRpc.Head(request,new ApplyPosition(1,100),value.request().sourceDirectory(),value.sourceOwner(),value.image().position(),value.sourceCfg()));}
                public CompletableFuture<Boolean> complete(Prepared value,RatisSourceHeadRpc.Head head){return CompletableFuture.completedFuture(false);}
                public CompletableFuture<Void> release(Prepared value){return CompletableFuture.completedFuture(null);}
            });
            try {
                for(var value:prepared.values())manager.enqueue(value.request()).get(10,TimeUnit.SECONDS);
                for(int visit=0;visit<8;visit++){step(manager);manager.discover(()->null).get(10,TimeUnit.SECONDS);}
                for(var entry:registrations.entrySet()){assertEquals(1,entry.getValue().get(),"Discovery must not repeatedly restart only one slot");assertNotEquals(RatisConfiguredRecovery.Phase.DONE,manager.phase(entry.getKey()));}
            }finally{manager.close(Duration.ofSeconds(10));}
            assertEquals(new RatisNodeResourceBudget.Usage(0,0),resources.stats().protectedLane());
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
