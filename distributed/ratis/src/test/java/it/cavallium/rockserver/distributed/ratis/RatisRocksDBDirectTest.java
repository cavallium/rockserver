package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisRocksDBDirectTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisAtomicClientTest.Fixture f=new RatisAtomicClientTest.Fixture(root,true);
        final SessionEpochLedger ledger;
        final SessionEpochLedger.Grant grant;
        final RatisDurableSessionClient session;
        final RatisRocksDBDirect direct;
        final RatisTransactionClient transactions;
        final RatisRocksDBAdapter adapter;
        final RatisRoutedClient routed;
        final Path window,journal;
        final AtomicBoolean lose=new AtomicBoolean(); final java.util.concurrent.atomic.AtomicInteger loseAfter=new java.util.concurrent.atomic.AtomicInteger(-1); final List<OperationId> writes=new ArrayList<>();
        boolean abandoned;
        Fixture() throws Exception {
            ledger=f.ledger.issue(new UUID(250,1),1000); grant=ledger.grants().getLast();
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(2),f.ledger,ledger)).status());
            int n=0; for (var group:f.groups) {
                var binding=f.catalogue.epochs.prepareSessionBinding(f.directory,group,new OperationId(f.admin,2,n++),grant,32,-1);
                assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepareSessionControl(binding)).status());
            }
            var transport=f.transport(); routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return transport.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException {
                    synchronized (writes) { writes.add(command.operationId()); }
                    var result=transport.command(route,command); if (lose.getAndSet(false) || loseAfter.getAndUpdate(left -> left<0 ? -1 : left-1)==0) throw new IOException("Lost direct applied reply"); return result;
                }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return transport.transaction(route,bytes,fresh); }
            });
            window=root.resolve("direct-window.bin"); session=RatisDurableSessionClient.create(window,routed,ledger,grant,RatisSessionClient.Limits.defaults());
            journal=root.resolve("direct-request.bin"); direct=new RatisRocksDBDirect(f.schema,routed,session,f.scheduler,journal); transactions=RatisRocksDBAdapterTest.client(f,f.data);
            adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,new RatisRocksDBAdapterTest.Allocations(f),direct,null);
        }
        public void close() throws Exception { if (!abandoned) adapter.close(); direct.close(); session.close(); routed.close(); transactions.close(); f.close(); }
    }
    @Test void directTypedPointAndSequentialMultiWritesNeedNoTransactionCapture() throws Exception {
        try (var x=new Fixture()) {
            var api=x.adapter.api(RequestContext.batch()); var key=RatisAtomicClientTest.key(2);
            assertNull(api.putAsync(0,71,key,RatisAtomicClientTest.value(1),RequestType.<Buf>previous()).get(5,TimeUnit.SECONDS));
            var previous=api.putMultiAsync(0,71,List.of(key,key,RatisAtomicClientTest.key(200)),List.of(RatisAtomicClientTest.value(2),RatisAtomicClientTest.value(3),RatisAtomicClientTest.value(4)),RequestType.<Buf>previous()).get(5,TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{1},previous.get(0).toByteArray()); assertArrayEquals(new byte[]{2},previous.get(1).toByteArray()); assertNull(previous.get(2));
            assertEquals(List.of(true,true),api.existsMultiAsync(0,71,List.of(key,RatisAtomicClientTest.key(200))).get(5,TimeUnit.SECONDS));
            assertArrayEquals(new byte[]{3},api.getAsync(0,71,key,RequestType.<Buf>current()).get(5,TimeUnit.SECONDS).toByteArray());
            assertEquals(List.of(),api.deleteMultiAsync(0,71,List.of(key),RequestType.<Buf>none()).get(5,TimeUnit.SECONDS));
            assertFalse(api.getAsync(0,71,key,RequestType.<Buf>exists()).get(5,TimeUnit.SECONDS));
            assertEquals(0,x.f.captureRequests.get()); assertEquals(0,x.f.opens.get()); assertEquals(0,x.adapter.stats().transactions()); assertTrue(x.session.pending().isEmpty());
        }
    }
    @Test void unknownLaterMultiWriteResumesOnlyItsOriginalPendingCommand() throws Exception {
        try (var x=new Fixture()) {
            var api=x.adapter.api(RequestContext.batch()); var first=RatisAtomicClientTest.key(2); var second=RatisAtomicClientTest.key(200);
            api.putAsync(0,71,first,RatisAtomicClientTest.value(1),RequestType.<Buf>none()).get(5,TimeUnit.SECONDS);
            x.loseAfter.set(1);
            var keys=List.of(first,second); var values=List.of(RatisAtomicClientTest.value(2),RatisAtomicClientTest.value(3));
            assertThrows(ExecutionException.class,() -> api.putMultiAsync(0,71,keys,values,RequestType.<Buf>previous()).get(5,TimeUnit.SECONDS));
            var retained=x.session.pending().getFirst().command().operationId();
            x.abandoned=true; x.direct.close(); x.session.close();
            try (var reopened=RatisDurableSessionClient.reopen(x.window,x.f.data); var direct=new RatisRocksDBDirect(x.f.schema,x.f.data,reopened,x.f.scheduler,x.journal);
                 var adapter=new RatisRocksDBAdapter(x.f.schema,x.f.directory,x.transactions,new RatisRocksDBAdapterTest.Allocations(x.f),direct,null)) {
            var result=adapter.api(RequestContext.batch()).putMultiAsync(0,71,keys,values,RequestType.<Buf>previous()).get(5,TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{1},result.get(0).toByteArray()); assertNull(result.get(1));
            synchronized (x.writes) { assertEquals(1,Collections.frequency(x.writes,retained)); assertEquals(3,x.writes.size(),"Seed plus two individual effects; restart reuses only the unresolved receipt"); }
            assertEquals(0,x.f.captureRequests.get()); assertTrue(reopened.pending().isEmpty());
            }
        }
    }
    @Test void durableDirectPendingSurvivesFrontendReopenWithoutNewIdentity() throws Exception {
        try (var x=new Fixture()) {
            x.lose.set(true); var key=RatisAtomicClientTest.key(2); var value=RatisAtomicClientTest.value(9);
            assertThrows(ExecutionException.class,() -> x.direct.write(71,key,value,RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,RequestContext.batch()).get(5,TimeUnit.SECONDS));
            var original=x.session.pending().getFirst().command().operationId();
            x.direct.close(); x.session.close();
            try (var reopened=RatisDurableSessionClient.reopen(x.window,x.f.data); var direct=new RatisRocksDBDirect(x.f.schema,x.f.data,reopened,x.f.scheduler,x.journal)) {
                assertEquals(original,reopened.pending().getFirst().command().operationId());
                assertThrows(ExecutionException.class,() -> direct.write(71,key,value,RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.CHANGED,RequestContext.batch()).get(5,TimeUnit.SECONDS));
                assertNull(direct.write(71,key,value,RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,RequestContext.batch()).get(5,TimeUnit.SECONDS));
                assertTrue(reopened.pending().isEmpty()); assertArrayEquals(new byte[]{9},x.f.get(71,key));
            }
        }
    }
    @Test void failureAfterAcknowledgementRetainsOriginalFrontendResultAcrossReopen() throws Exception {
        try (var x=new Fixture()) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            x.direct.afterAcknowledgement(() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("ACK barrier timed out"); } catch (InterruptedException interrupted) { throw new AssertionError(interrupted); } });
            var key=RatisAtomicClientTest.key(2); var value=RatisAtomicClientTest.value(9);
            var pending=x.direct.write(71,key,value,RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,RequestContext.batch(Duration.ofMillis(500)));
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(x.session.pending().isEmpty()); assertThrows(IOException.class,x.direct::close); Thread.sleep(550); }
            finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); int effects=x.f.writes();
            x.abandoned=true; x.direct.close(); x.session.close();
            try (var reopened=RatisDurableSessionClient.reopen(x.window,x.f.data); var direct=new RatisRocksDBDirect(x.f.schema,x.f.data,reopened,x.f.scheduler,x.journal)) {
                assertNull(direct.write(71,key,value,RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.PREVIOUS,RequestContext.batch()).get(5,TimeUnit.SECONDS));
                assertEquals(effects,x.f.writes()); assertArrayEquals(new byte[]{9},x.f.get(71,key));
            }
        }
    }
    @Test void definiteSessionExpiryDoesNotMonopolizeTheUnknownWriteSlot() throws Exception {
        try (var x=new Fixture()) {
            var expired=x.ledger.advanceTime(1000);
            assertEquals(CommandResult.Status.SUCCESS,x.f.catalogue.epochs.execute(x.f.catalogue.epochs.prepare(x.f.outer.operation(3),x.ledger,expired)).status());
            int n=0; for (var group:x.f.groups) {
                var clock=DistributedCommand.sessionControl(group,1,x.f.schema.digest(),new OperationId(x.f.admin,3,n++),RangeSessionControl.advance(expired),-1);
                assertEquals(CommandResult.Status.SUCCESS,x.f.data.execute(x.f.data.prepareSessionControl(clock)).status());
            }
            for (int key:new int[]{2,3}) {
                var failure=assertThrows(ExecutionException.class,() -> x.direct.write(71,RatisAtomicClientTest.key(key),RatisAtomicClientTest.value(9),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,RequestContext.batch()).get(5,TimeUnit.SECONDS));
                assertInstanceOf(RocksDBException.class,failure.getCause());
                assertTrue(x.session.pending().isEmpty(),"A durably known no-effects result must not wedge later requests");
            }
            synchronized (x.writes) { assertEquals(2,x.writes.size()); assertNotEquals(x.writes.getFirst(),x.writes.getLast()); }
        }
    }
}
