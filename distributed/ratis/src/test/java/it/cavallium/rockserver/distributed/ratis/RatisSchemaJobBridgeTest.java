package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisSchemaRequests.*;

class RatisSchemaJobBridgeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void cancellationBeforeFirstCheckpointKeepsNoEffectsTokenAndAllowsNextRequest() throws Exception {
        var scheduler=RWScheduler.forTesting(1,1,1,4,1,"schema-bridge-test");
        try (var f=new RatisSchemaClientTest.Fixture(root)) {
            Path requests=Files.createDirectory(root.resolve("requests")),sdk=Files.createDirectory(root.resolve("sdk"));
            RatisSchemaJobBridge.ClientFactory factory=(domain,checkpoint) -> new RatisSchemaClient(sdk,f.outer.cluster,f.outer.database,f.directory.domainId(),f.catalogue,f.client,f.routed,f.replicas,null,checkpoint);
            RatisSchemaClient.TrustedOperatorResolver resolver=(name,className,sha,revision) -> { throw new AssertionError("No operator resolution in this workflow"); };
            UUID cancelledId;
            long writes=f.nativeStore.next.get();
            try (var bridge=new RatisSchemaJobBridge(requests,sdk,scheduler,factory,resolver,java.util.Map.of(f.routed.directory().domainId(),f.routed),8,4,16L*1024*1024)) {
                var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var id=new AtomicReference<UUID>();
                var cancelled=bridge.submit(f.routed.directory().domainId(),Kind.DELETE_IF_EXISTS,65536,RequestContext.batch(),(client,request) -> {
                    id.set(request); entered.countDown();
                    if (!release.await(5,TimeUnit.SECONDS)) throw new TimeoutException("Test did not release initial checkpoint");
                    return client.prepareDeleteIfExists(request,"missing");
                });
                try { assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(cancelled.cancel(false)); }
                finally { release.countDown(); }
                var fresh=bridge.submit(f.routed.directory().domainId(),Kind.DELETE_IF_EXISTS,65536,RequestContext.batch(),(client,request) -> client.prepareDeleteIfExists(request,"missing"));
                var result=fresh.get(5,TimeUnit.SECONDS); assertFalse(result.changed());
                cancelledId=id.get(); assertNotEquals(cancelledId,result.requestId());
                assertEquals(State.NOT_STARTED,bridge.status(cancelledId).state());
                assertFalse(Files.exists(sdk.resolve(cancelledId+".request")));
                var failure=assertThrows(ExecutionException.class,() -> bridge.resume(cancelledId,RequestContext.batch(),false).get(5,TimeUnit.SECONDS));
                assertInstanceOf(RatisSchemaRequestPendingException.class,failure.getCause());
                assertEquals(State.DONE,bridge.status(result.requestId()).state());
                assertEquals(writes,f.nativeStore.next.get()); assertFalse(bridge.busy());
            }
            try (var reopened=new RatisSchemaJobBridge(requests,sdk,scheduler,factory,resolver,java.util.Map.of(f.routed.directory().domainId(),f.routed),8,4,16L*1024*1024)) {
                assertEquals(State.NOT_STARTED,reopened.status(cancelledId).state());
                var result=reopened.submit(f.directory.domainId(),Kind.DELETE_IF_EXISTS,65536,RequestContext.batch(),(client,request) -> client.prepareDeleteIfExists(request,"missing")).get(5,TimeUnit.SECONDS);
                assertFalse(result.changed()); assertEquals(writes,f.nativeStore.next.get());
            }
            assertEquals(0,f.nativeStore.closes.get());
        } finally { scheduler.dispose(); }
    }

    @Test void coldBootstrapReopenRestoresTwoCompletedSchemaTransitionsInOrder() throws Exception {
        var scheduler=RWScheduler.forTesting(1,1,1,4,1,"schema-history-test");
        try (var f=new RatisSchemaClientWorkflowTest.Fixture(root,false)) {
            Path requests=Files.createDirectory(root.resolve("requests")),sdk=Files.createDirectory(root.resolve("sdk"));
            RatisSchemaClient.TrustedOperatorResolver resolver=(name,className,sha,revision) -> { throw new AssertionError("No operator resolution in this workflow"); };
            DistributedSchema intermediate;
            byte[] finalDigest;
            RatisSchemaJobBridge.ClientFactory factory=(domain,checkpoint) -> new RatisSchemaClient(sdk,f.outer.cluster,f.outer.database,f.initial.domainId(),f.catalogue,f.client,f.routed,f.replicas,null,checkpoint);
            try (var bridge=new RatisSchemaJobBridge(requests,sdk,scheduler,factory,resolver,java.util.Map.of(f.routed.directory().domainId(),f.routed),8,4,16L*1024*1024)) {
                var first=bridge.submit(f.routed.directory().domainId(),Kind.CREATE,65536,RequestContext.batch(),(client,id) -> client.prepareCreate(id,"first",RatisSchemaClientTest.shape())).get(10,TimeUnit.SECONDS);
                assertTrue(first.changed());
                var catalogue=new RatisSchemaCatalogue(f.client,f.outer.cluster,f.outer.database,f.catalogue.groupId(),f.catalogue.epoch(),f.catalogue.schemaDigest(),f.catalogue.directoryColumn());
                intermediate=catalogue.read(f.routed.directory().schemaDigest());
                var second=bridge.submit(f.routed.directory().domainId(),Kind.CREATE,65536,RequestContext.batch(),(client,id) -> client.prepareCreate(id,"second",RatisSchemaClientTest.shape())).get(10,TimeUnit.SECONDS);
                assertTrue(second.changed()); assertNotEquals(first.value(),second.value());
                assertEquals(3,f.routed.directory().schemaEpoch()); finalDigest=f.routed.directory().schemaDigest();
            }
            try (var cold=new RatisRoutedClient(f.initialDirectory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError("History restoration must use forced local proofs"); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError("History restoration must not submit DATA"); }
            })) {
                RatisSchemaJobBridge.ClientFactory recovered=(domain,checkpoint) -> new RatisSchemaClient(sdk,f.outer.cluster,f.outer.database,f.initial.domainId(),f.catalogue,f.client,cold,f.replicas,null,checkpoint);
                try (var bridge=new RatisSchemaJobBridge(requests,sdk,scheduler,recovered,resolver,java.util.Map.of(cold.directory().domainId(),cold),8,4,16L*1024*1024)) {
                    assertArrayEquals(finalDigest,cold.directory().schemaDigest()); assertEquals(3,cold.directory().schemaEpoch());
                    assertArrayEquals(f.initialDirectory.payloadDigest(),cold.directory(f.initial).payloadDigest());
                    assertEquals(2,cold.directory(intermediate).schemaEpoch()); assertTrue(bridge.pending().isEmpty());
                }
            }
        } finally { scheduler.dispose(); }
    }
}
