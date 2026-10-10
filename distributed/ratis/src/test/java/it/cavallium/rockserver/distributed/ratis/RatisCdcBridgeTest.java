package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.cdc.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native catalogue and controller journals, controlled DATA transport; no network/quorum claim. */
class RatisCdcBridgeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T> T await(CompletableFuture<T> future) throws Exception { return future.get(5,TimeUnit.SECONDS); }
    RatisCdcBridge open(RatisCdcSubscriptionControllerTest.Fixture f,Path journal,RWScheduler scheduler) throws Exception {
        var directories=new RatisCatalogue(f.client,f.directory.clusterId(),f.directory.databaseId(),f.binding.groupId(),f.binding.epoch(),f.binding.schemaDigest(),f.binding.directoryColumn());
        return new RatisCdcBridge(f.controller(journal),scheduler,directories::read);
    }
    @Test void publicVectorWorkflowProjectsColumnsAndRecoversSavedBatchBeforeExplicitAck() throws Exception {
        var support=new RatisCdcSubscriptionControllerTest(); support.root=root;
        var scheduler=RWScheduler.forTesting(1,1,1,8,8,"cdc-public");
        try(var f=support.new Fixture(true)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1); UUID batchId; byte[] next;
            try(var api=open(f,journal,scheduler)) {
                assertArrayEquals(RangeDirectoryCodec.encode(f.directory),RangeDirectoryCodec.encode(await(api.context(f.directory.domainId(),RequestContext.batch()))));
                assertEquals(DistributedCdcSubscriptions.Phase.ACTIVE,await(api.create(definition,RequestContext.batch())).phase());
                var poll=await(api.poll(definition.subscriptionId(),16,4096,RequestContext.batch())); batchId=poll.batch().id(); next=await(api.status(definition.subscriptionId(),RequestContext.batch())).state().delivered().encode();
                assertEquals(2,poll.batch().envelopes().getFirst().events().size());
                assertTrue(poll.batch().envelopes().stream().flatMap(e->e.events().stream()).allMatch(e->e.mutation().columnId().equals(f.selected)));
                assertEquals(definition.start(),await(api.status(definition.subscriptionId(),RequestContext.batch())).state().checkpoint());
            }
            try(var api=open(f,journal,scheduler)) {
                assertArrayEquals(next,await(api.recover(definition.subscriptionId(),RequestContext.batch())).state().delivered().encode());
                assertEquals(batchId,await(api.poll(definition.subscriptionId(),1,512,RequestContext.batch())).batch().id());
                var ack=await(api.acknowledge(definition.subscriptionId(),batchId,RequestContext.batch())); assertEquals(ack.state().delivered(),ack.state().checkpoint());
                assertNotEquals(ack.state().checkpoint().cursors().keySet().iterator().next(),await(api.poll(definition.subscriptionId(),16,4096,RequestContext.batch())).batch().lineage());
                assertEquals(DistributedCdcSubscriptions.Phase.CLOSED,await(api.closeSubscription(definition.subscriptionId(),RequestContext.batch())).phase());
                assertEquals(0,api.actualOwners());
            }
            assertFalse(f.transportClosed);
        } finally { scheduler.dispose(); }
    }
    @Test void cancellationRetainsActualOwnershipAndOriginalDeliveryThroughCloseAndReopen() throws Exception {
        var support=new RatisCdcSubscriptionControllerTest(); support.root=root;
        var scheduler=RWScheduler.forTesting(1,1,1,8,8,"cdc-cancel"); var release=new CountDownLatch(1);
        try(var f=support.new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1);
            var api=open(f,journal,scheduler);
            try {
                await(api.create(definition,RequestContext.batch())); var entered=new CountDownLatch(1);
                f.pollObserver=()->{ entered.countDown(); try { if(!release.await(5,TimeUnit.SECONDS)) throw new IOException("Controlled source timed out"); } catch(InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); } };
                f.lose.add("DELIVER"); var observer=api.poll(definition.subscriptionId(),16,4096,RequestContext.batch()); assertTrue(entered.await(5,TimeUnit.SECONDS));
                var barrier=api.status(definition.subscriptionId(),RequestContext.batch()); assertTrue(observer.cancel(true)); assertEquals(2,api.actualOwners());
                assertThrows(IOException.class,api::close); assertThrows(IOException.class,()->f.controller(journal));
                release.countDown(); assertThrows(ExecutionException.class,()->await(barrier)); assertEquals(0,api.actualOwners()); api.close();
                f.pollObserver=()->{};
                try(var reopened=open(f,journal,scheduler)) {
                    await(reopened.recover(definition.subscriptionId(),RequestContext.batch())); var batch=await(reopened.poll(definition.subscriptionId(),16,4096,RequestContext.batch())).batch();
                    assertEquals(1,f.polls.size()); assertNotNull(batch); assertArrayEquals(f.writes.get("DELIVER").getFirst(),f.writes.get("DELIVER").getLast());
                    assertEquals(definition.start(),await(reopened.status(definition.subscriptionId(),RequestContext.batch())).state().checkpoint());
                }
            } finally { release.countDown(); api.close(); }
        } finally { scheduler.dispose(); }
    }
    @Test void timedOutObserverAndBoundedQueueLeaveActualPollChargedUntilReturn() throws Exception {
        var support=new RatisCdcSubscriptionControllerTest(); support.root=root;
        var scheduler=RWScheduler.forTesting(1,1,1,8,8,"cdc-timeout"); var release=new CountDownLatch(1);
        try(var f=support.new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1);
            try(var api=open(f,journal,scheduler)) {
                await(api.create(definition,RequestContext.batch())); var entered=new CountDownLatch(1);
                f.pollObserver=()->{ entered.countDown(); try { if(!release.await(5,TimeUnit.SECONDS)) throw new IOException("Controlled source timed out"); } catch(InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); } };
                var poll=api.poll(definition.subscriptionId(),16,4096,RequestContext.batch(Duration.ofMillis(100))); assertTrue(entered.await(5,TimeUnit.SECONDS));
                var queued=new ArrayList<CompletableFuture<DistributedCdcSubscriptions.Status>>(); for(int i=1;i<RatisCdcBridge.MAX_PENDING;i++) queued.add(api.status(definition.subscriptionId(),RequestContext.batch()));
                var rejected=assertThrows(ExecutionException.class,()->await(api.status(definition.subscriptionId(),RequestContext.batch()))); assertInstanceOf(RejectedExecutionException.class,rejected.getCause());
                var timeout=assertThrows(ExecutionException.class,()->await(poll)); assertInstanceOf(TimeoutException.class,timeout.getCause()); assertEquals(RatisCdcBridge.MAX_PENDING,api.actualOwners());
                release.countDown(); for(var pending:queued) await(pending); assertEquals(0,api.actualOwners());
                assertNotNull(await(api.poll(definition.subscriptionId(),16,4096,RequestContext.batch())).batch()); assertEquals(1,f.polls.size());
            } finally { release.countDown(); }
        } finally { scheduler.dispose(); }
    }
    @Test void publicGapAndBudgetOutcomesIdentifyLineageAndEmptyProjectionStillRequiresAck() throws Exception {
        var support=new RatisCdcSubscriptionControllerTest(); support.root=root;
        var scheduler=RWScheduler.forTesting(1,1,1,8,8,"cdc-outcomes");
        try(var f=support.new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1); var lineage=definition.start().cursors().keySet().iterator().next();
            try(var api=open(f,journal,scheduler)) {
                await(api.create(definition,RequestContext.batch())); f.outcome=RatisCdcPollRpc.Outcome.GAP;
                var gap=await(api.poll(definition.subscriptionId(),16,4096,RequestContext.batch())); assertEquals(lineage,gap.lineage()); assertEquals(DistributedCdcSubscriptions.Outcome.GAP,gap.outcome()); assertNull(gap.batch());
                f.outcome=RatisCdcPollRpc.Outcome.TOO_LARGE; var large=await(api.poll(definition.subscriptionId(),16,4096,RequestContext.batch()));
                assertEquals(lineage,large.lineage()); assertEquals(4196,large.requiredBytes()); assertEquals(DistributedCdcSubscriptions.Outcome.TOO_LARGE,large.outcome());
                assertEquals(definition.start(),await(api.status(definition.subscriptionId(),RequestContext.batch())).state().checkpoint());
                f.outcome=RatisCdcPollRpc.Outcome.DATA; f.unselectedOnly=true; var batch=await(api.poll(definition.subscriptionId(),16,4096,RequestContext.batch())).batch();
                assertTrue(batch.envelopes().isEmpty()); assertNotEquals(batch.from(),batch.next());
                assertEquals(definition.start(),await(api.status(definition.subscriptionId(),RequestContext.batch())).state().checkpoint());
                assertEquals(batch.next(),await(api.acknowledge(definition.subscriptionId(),batch.id(),RequestContext.batch())).state().checkpoint().cursors().get(lineage));
            }
        } finally { scheduler.dispose(); }
    }

}
