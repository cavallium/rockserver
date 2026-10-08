package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

/** Metadata/native query composition; actual read-index quorum/transport acceptance remains a separate gate. */
class RatisRangeStateViewTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    Message request() { return Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]))); }
    RangeStateView view(RatisSessionBindingTest.Fixture f) throws Exception {
        return RangeStateView.decode(ScopedTransactionRpc.parse(f.machine.query(request()).get(5,TimeUnit.SECONDS).getContent().toByteArray(),ScopedTransactionRpc.MAX_REPLY_BYTES).body());
    }
    @Test void metadataUsesNoCustomerKeyAndSharesPauseReloadAndStaleIndexGuards() throws Exception {
        try (var f=new RatisSessionBindingTest.Fixture(root)) {
            var target=SessionAuthority.from(f.ledger); var initial=view(f);
            assertEquals(new ApplyPosition(1,0),initial.applied()); assertNull(initial.sessionAuthority());
            assertThrows(IllegalStateException.class,() -> initial.requireServing(target));
            var binding=f.catalogue.epochs.prepareSessionBinding(f.directory,f.outer.dataGroup,f.admin(0),f.ledger.grants().getFirst(),2,-1);
            f.routed.execute(f.routed.prepareSessionControl(binding));
            var bound=view(f); assertEquals(new ApplyPosition(1,1),bound.applied()); assertEquals(target,bound.sessionAuthority());
            bound.requireMatches(f.directory,f.directory.ranges().getFirst()); bound.requireServing(target);
            var usage=f.store.receiptUsage();
            byte[] invalid=ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]); invalid[0]^=1;
            assertTrue(f.machine.query(Message.valueOf(ByteString.copyFrom(invalid))).isCompletedExceptionally());
            assertTrue(f.machine.queryStale(request(),2).isCompletedExceptionally());
            byte[] point=ScopedTransactionRpc.pointRequest(f.outer.data,new byte[]{1});
            assertNull(ScopedTransactionRpc.pointValue(f.machine.query(Message.valueOf(ByteString.copyFrom(point))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),2*1024*1024));
            assertEquals(usage,f.store.receiptUsage()); assertEquals(0,f.store.activeSnapshotPins());
            f.machine.takeSnapshot(); f.machine.pause(); assertTrue(f.machine.query(request()).isCompletedExceptionally());
            f.machine.reinitialize(); assertEquals(bound,view(f)); assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void routedMetadataRejectsWrongScopeAndMissingRepliesWithoutLeakingCallAdmission() throws Exception {
        try (var f=new RatisSessionBindingTest.Fixture(root)) {
            var good=view(f); var reply=new AtomicReference<>(good);
            try (var client=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError("Customer read not permitted"); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError("Mutation not permitted"); }
                public RangeStateView stateView(RangeDescriptor descriptor) { return reply.get(); }
            })) {
                assertEquals(good,client.stateView(f.outer.dataGroup));
                reply.set(new RangeStateView(UUID.randomUUID(),good.rangeEpoch(),good.domainId(),good.schemaDigest(),good.columns(),good.lowerInclusive(),good.upperExclusive(),good.applied(),good.sessionAuthority()));
                assertThrows(IOException.class,() -> client.stateView(f.outer.dataGroup));
                reply.set(null); assertThrows(IOException.class,() -> client.stateView(f.outer.dataGroup));
                reply.set(good); for (int i=0;i<40;i++) assertEquals(good,client.stateView(f.outer.dataGroup));
                assertThrows(IllegalArgumentException.class,() -> client.stateView(UUID.randomUUID()));
            }
            try (var unsupported=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError(); }
            })) { assertThrows(IOException.class,() -> unsupported.stateView(f.outer.dataGroup)); }
        }
    }
}
