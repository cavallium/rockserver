package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

/** Public database dispatch against native independent domains and actual same-domain certified transactions. */
class RatisDatabaseConnectionTest {
    final RatisPointFixtureBudget pointBudget=new RatisPointFixtureBudget();
    @org.junit.jupiter.api.AfterEach void closePointBudget()throws Exception{pointBudget.close();}
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T>T await(CompletableFuture<T> future)throws Exception{return future.get(15,TimeUnit.SECONDS);}
    @Test void completeRosterRoutesBothNativeDomainsAndColdReopensOneSharedGenerationInventory()throws Exception {
        var helper=new RatisMultiDomainBootstrapTest();helper.root=root;
        try(var f=helper.new Fixture();var bootstrap=f.bootstrap(Files.createDirectory(root.resolve("bootstrap")),null,2);var homes=new RatisAtomicClientTest.Fixture(root.resolve("homes"),true);var replicas=new RatisSchemaReplicaClient(1)) {
            f.ready(bootstrap);var routes=new HashMap<UUID,RatisRoutedClient>();for(var d:f.data)routes.put(d.domain,d.routed);var issued=new AtomicInteger();var operations=new AtomicLong(400);var windows=new ArrayList<Path>();
            RatisRocksDBConnection.SessionOpening sessions=(path,schema,bindings)->{var directory=bindings.directory(schema.domainId());var routed=routes.get(schema.domainId());Path window=path.resolve("window.bin");if(Files.exists(window))return RatisDurableSessionClient.reopen(window,routed);var before=f.catalogue.epochs.read();var next=before.issue(UUID.randomUUID(),1000);var grant=next.grants().getLast();assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(operations.getAndIncrement()),before,next)).status());for(var range:directory.ranges()){var control=f.catalogue.epochs.prepareSessionBinding(directory,range.groupId(),new OperationId(f.outer.maintenance,next.minimumEpoch(),operations.getAndIncrement()),grant,32,-1);assertEquals(CommandResult.Status.SUCCESS,routed.execute(routed.prepareSessionControl(control)).status());}issued.incrementAndGet();windows.add(window);return RatisDurableSessionClient.create(window,routed,next,grant,RatisSessionClient.Limits.defaults());};
            RatisRocksDBAdapter.AllocationSource unused=context->CompletableFuture.failedFuture(new IOException("No transaction captures expected before a concrete column"));
            RatisRocksDBConnection.GenerationFactory factory=(schema,directory,path,ids)->RatisRocksDBConnection.durableGenerationFactory(routes.get(schema.domainId()),homes.scheduler,homes.snapshots,homes.homes,f.catalogue.epochs,f.directories(),header->{throw new IOException("No retired source in this fixture");},unused,new RatisRocksDBConnection.SessionFactory(List.of(routes.get(schema.domainId())),sessions),List.of(),pointBudget.shared).create(schema,directory,path,ids);
            var binding=f.data.getFirst().plan.catalogue();Path connectionRoot=root.resolve("database-connection");RatisRocksDBConnection.DomainPlacement placement=(name,shape)->f.data.stream().filter(d->d.schema.columns().stream().anyMatch(c->c.name().equals(name))).findFirst().orElseThrow(()->new IOException("Undeclared column placement")).domain;
            try(var connection=new RatisRocksDBConnection(URI.create("ratis://controlled/database"),connectionRoot,routes,placement,homes.scheduler,binding,f.client,replicas,null,null,factory,RatisRocksDBConnection.Limits.defaults(),List.of(), pointBudget.shared)) {
                var api=connection.getAsyncApi(RequestContext.batch());assertEquals(2,await(api.getAllColumnDefinitionsAsync()).size());assertEquals(71,await(api.getColumnIdAsync("logical-1")));assertEquals(72,await(api.getColumnIdAsync("logical-2")));
                RatisRocksDBConnectionTest.point(connection,71,key(7),value(11),RequestType.RequestTypeId.NOTHING);RatisRocksDBConnectionTest.point(connection,72,key(7),value(12),RequestType.RequestTypeId.NOTHING);assertArrayEquals(new byte[]{11},await(api.getAsync(0,71,key(7),RequestType.<Buf>current())).toByteArray());assertArrayEquals(new byte[]{12},await(api.getAsync(0,72,key(7),RequestType.<Buf>current())).toByteArray());
                var missing=assertThrows(ExecutionException.class,()->await(api.openTransactionAsync(null, Duration.ofSeconds(10))));assertEquals(RocksDBException.RocksDBErrorType.TX_SCOPE_REQUIRED,assertInstanceOf(RocksDBException.class,missing.getCause()).getErrorUniqueId());assertEquals(4,issued.get());
                int captures=homes.captureRequests.get();var cross=new TransactionOpenScope(List.of(TransactionOpenScope.Span.point(71,key(7)),TransactionOpenScope.Span.point(72,key(7))));
                assertThrows(ExecutionException.class,()->await(api.openTransactionAsync(cross,Duration.ofSeconds(10))));
                assertThrows(ExecutionException.class,()->await(api.openTransactionAsync(RatisRocksDBAdapterTest.scope(999),Duration.ofSeconds(10))));
                assertEquals(4,issued.get());assertEquals(captures,homes.captureRequests.get());assertEquals(2,await(connection.directRecoveryStatus(RequestContext.batch())).size());
            }
            try(var connection=new RatisRocksDBConnection(URI.create("ratis://controlled/database"),connectionRoot,routes,placement,homes.scheduler,binding,f.client,replicas,null,null,factory,RatisRocksDBConnection.Limits.defaults(),List.of(), pointBudget.shared)) {assertEquals(4,issued.get());var api=connection.getSyncApi(RequestContext.batch());assertArrayEquals(new byte[]{11},api.get(0,71,key(7),RequestType.<Buf>current()).toByteArray());assertArrayEquals(new byte[]{12},api.get(0,72,key(7),RequestType.<Buf>current()).toByteArray());}
            assertEquals(4,windows.size());assertThrows(IOException.class,()->new RatisRocksDBConnection(URI.create("ratis://controlled/incomplete"),root.resolve("incomplete"),Map.of(f.data.getFirst().domain,f.data.getFirst().routed),placement,homes.scheduler,binding,f.client,replicas,null,null,factory,RatisRocksDBConnection.Limits.defaults(),List.of(), pointBudget.shared));
        }
    }
    @Test void fourColumnsShareOneEagerNativeTransactionAndUnknownCommitRetainsOriginalCaptures()throws Exception {
        var helper=new RatisRocksDBConnectionTest();helper.root=root;
        try(var f=helper.new Fixture();var connection=f.open(8)) {
            var nativeState=f.direct.f;var api=connection.getAsyncApi(RequestContext.batch());int before=nativeState.captureRequests.get();long transaction=await(api.openTransactionAsync(RatisRocksDBAdapterTest.scope(71,72,73,74), Duration.ofSeconds(10)));assertTrue(nativeState.captureRequests.get()>before);
            var keys=new ArrayList<Keys>();for(int n=0;n<4;n++)keys.add(n==3?new Keys(Buf.wrap(new byte[]{2}),Buf.wrap(new byte[]{7})):key(2));
            for(int n=0;n<4;n++)await(api.putAsync(transaction,71+n,keys.get(n),value(20+n),RequestType.none()));int captured=nativeState.captureRequests.get();assertTrue(captured>before);
            nativeState.beforeHomeRead=()->{throw new UncheckedIOException(new IOException("Retain exact commit until HOME proof is available"));};assertThrows(ExecutionException.class,()->await(api.closeTransactionAsync(transaction,true)));nativeState.beforeHomeRead=null;
            assertTrue(await(api.closeTransactionAsync(transaction,true)));assertEquals(captured,nativeState.captureRequests.get());for(int n=0;n<4;n++)assertArrayEquals(new byte[]{(byte)(20+n)},await(api.getAsync(0,71+n,keys.get(n),RequestType.<Buf>current())).toByteArray());
        }
    }
}
