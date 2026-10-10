package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.client.ThriftDistributedCdcClient;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.cdc.proto.*;
import it.cavallium.rockserver.core.common.cdc.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native controller across both codecs and direct service calls; no servers, sockets or quorum. */
class RatisCdcTransportContractTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final Duration WAIT=Duration.ofSeconds(5);
    static final RequestContext CONTEXT=RequestContext.batch();
    static CdcSubscriptionRequest subscription(UUID id) { return CdcSubscriptionRequest.newBuilder().setSubscriptionId(DistributedCdcWire.uuid(id)).setContext(DistributedCdcWire.context(CONTEXT,RequestContext.NO_TIMEOUT)).build(); }
    static CdcPollRequest poll(UUID id,int records,int bytes) { return CdcPollRequest.newBuilder().setSubscriptionId(DistributedCdcWire.uuid(id)).setMaxRecords(records).setMaxBytes(bytes).setContext(DistributedCdcWire.context(CONTEXT,RequestContext.NO_TIMEOUT)).build(); }
    @Test void protobufDeliveryCanBeRecoveredAndAcknowledgedThroughThriftAfterJournalReopen() throws Exception {
        var support=new RatisCdcSubscriptionControllerTest(); support.root=root; var opener=new RatisCdcBridgeTest();
        var scheduler=RWScheduler.forTesting(1,1,1,8,8,"cdc-codecs");
        try(var f=support.new Fixture(true)) {
            var definition=f.definition(1); Path journal=Files.createDirectory(root.resolve("journal")); UUID batchId;
            try(var api=opener.open(f,journal,scheduler)) {
                var service=new DistributedCdcGrpcService(api);
                var create=CdcCreateRequest.newBuilder().setDefinition(DistributedCdcWire.definition(definition)).setContext(DistributedCdcWire.context(CONTEXT,RequestContext.NO_TIMEOUT)).build();
                assertEquals(definition,DistributedCdcWire.status(service.create(create).block(WAIT),definition.subscriptionId()).state().definition());
                var reply=service.poll(poll(definition.subscriptionId(),16,4096)).block(WAIT);
                var decoded=DistributedCdcWire.poll(CdcPoll.parseFrom(reply.toByteArray()),definition.subscriptionId(),16,4096); batchId=decoded.batch().id();
                assertEquals(2,decoded.batch().envelopes().getFirst().events().size());
                assertEquals(definition.start(),api.status(definition.subscriptionId(),CONTEXT).get(5,TimeUnit.SECONDS).state().checkpoint());
            }
            try(var api=opener.open(f,journal,scheduler); var client=new ThriftDistributedCdcClient(new DistributedCdcThriftService(api,32*1024*1024),Runnable::run)) {
                assertEquals(definition,client.recover(definition.subscriptionId(),CONTEXT).get(5,TimeUnit.SECONDS).state().definition());
                assertEquals(batchId,client.poll(definition.subscriptionId(),1,111,CONTEXT).get(5,TimeUnit.SECONDS).batch().id());
                var ack=client.acknowledge(definition.subscriptionId(),batchId,CONTEXT).get(5,TimeUnit.SECONDS); assertEquals(ack.state().delivered(),ack.state().checkpoint());
                assertEquals(1,f.polls.size());
                var next=client.poll(definition.subscriptionId(),16,4096,CONTEXT).get(5,TimeUnit.SECONDS);
                assertNotEquals(ack.state().checkpoint().cursors().keySet().iterator().next(),next.lineage());
                assertEquals(DistributedCdcSubscriptions.Phase.CLOSED,client.closeSubscription(definition.subscriptionId(),CONTEXT).get(5,TimeUnit.SECONDS).phase());
            }
        } finally { scheduler.dispose(); }
    }
}
