package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisRocksDBAdapterTest.*;

/** Implicit update aliases retain original native captures until an explicit retry read. */
class RatisImplicitUpdateTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void singlePutAndDeleteCommitAutomaticallyAndRejectReturningWriteModes() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("success"),true); var transactions=client(f,f.data)) {
            seed(f,71,key(2),value(1),1900); var allocations=new Allocations(f);
            try (var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,allocations,null,null)) {
                var api=adapter.api(CONTEXT); var update=await(api.getAsync(0,71,key(2),RequestType.<Buf>forUpdate()));
                assertTrue(update.updateId()>0); bytes(1,update.previous()); long id=update.updateId();
                error(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,api.putAsync(id,71,key(2),value(8),RequestType.<Buf>previous()));
                error(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,api.putAsync(id,71,key(2),value(8),RequestType.<Buf>delta()));
                error(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,api.putAsync(id,71,key(2),value(8),RequestType.<Buf>changed()));
                error(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,api.deleteAsync(id,71,key(2),RequestType.<Buf>previousPresence()));
                assertEquals(1,allocations.count.get()); assertNull(f.homes.read(allocations.issued.getFirst().identity().commit().transaction()));
                assertNull(await(api.putAsync(id,71,key(2),value(8),RequestType.<Buf>none())));
                assertArrayEquals(new byte[]{8},f.get(71,key(2))); assertEquals(0,adapter.stats().transactions());
                assertTrue(f.homes.read(allocations.issued.getFirst().identity().commit().transaction()).consumed());
                await(api.closeFailedUpdateAsync(id)); await(api.closeFailedUpdateAsync(id));
                var deletion=await(api.getAsync(0,71,key(2),RequestType.<Buf>forUpdate())); bytes(8,deletion.previous());
                assertNull(await(api.deleteAsync(deletion.updateId(),71,key(2),RequestType.<Buf>none())));
                assertNull(f.get(71,key(2))); assertEquals(2,allocations.count.get()); assertEquals(0,adapter.stats().transactions()); assertEquals(0,f.opens.get());
            }
        }
    }

    @Test void singleConflictKeepsAliasUntilExplicitFreshReadAndCertifiesReadOnlyColumns() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("single-conflict"),true); var transactions=client(f,f.data)) {
            seed(f,71,key(2),value(1),1900); var allocations=new Allocations(f);
            try (var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,allocations,null,null)) {
                var api=adapter.api(CONTEXT); var initial=await(api.getAsync(0,71,key(2),RequestType.<Buf>forUpdate())); long id=initial.updateId();
                bytes(1,initial.previous()); seed(f,72,key(200),value(9),1901);
                error(RocksDBException.RocksDBErrorType.UPDATE_RETRY,api.putAsync(id,71,key(2),value(6),RequestType.<Buf>none()));
                assertArrayEquals(new byte[]{1},f.get(71,key(2))); assertEquals(TransactionStatus.Phase.ABORT,f.homes.read(allocations.issued.getFirst().identity().commit().transaction()).phase());
                int captures=f.captureRequests.get();
                error(RocksDBException.RocksDBErrorType.UPDATE_RETRY,api.putAsync(id,71,key(2),value(6),RequestType.<Buf>none()));
                assertEquals(1,allocations.count.get()); assertEquals(captures,f.captureRequests.get()); assertEquals(1,adapter.stats().transactions());
                seed(f,71,key(2),value(7),1902);
                var refreshed=await(api.getAsync(id,71,key(2),RequestType.<Buf>forUpdate())); assertEquals(id,refreshed.updateId()); bytes(7,refreshed.previous());
                assertEquals(2,allocations.count.get()); assertTrue(f.captureRequests.get()>captures);
                await(api.putAsync(id,71,key(2),value(8),RequestType.<Buf>none())); assertArrayEquals(new byte[]{8},f.get(71,key(2))); assertEquals(0,adapter.stats().transactions()); assertEquals(0,f.opens.get());
                await(api.closeFailedUpdateAsync(id));
            }
        }
    }

    @Test void multiConflictRollsBackEveryGroupAndClosesAliasBeforeRetrySignal() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("multi-conflict"),true); var transactions=client(f,f.data)) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(200),value(3),1901); var allocations=new Allocations(f);
            try (var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,allocations,null,null)) {
                var api=adapter.api(CONTEXT); var update=await(api.getAsync(0,71,key(2),RequestType.<Buf>forUpdate())); seed(f,72,key(200),value(9),1902);
                error(RocksDBException.RocksDBErrorType.UPDATE_RETRY,api.putMultiAsync(update.updateId(),71,List.of(key(2),key(200)),List.of(value(7),value(8)),RequestType.<Buf>none()));
                assertEquals(0,adapter.stats().transactions()); assertEquals(1,allocations.count.get());
                assertArrayEquals(new byte[]{1},f.get(71,key(2))); assertArrayEquals(new byte[]{3},f.get(71,key(200)));
                var home=f.homes.read(allocations.issued.getFirst().identity().commit().transaction()); assertEquals(TransactionStatus.Phase.ABORT,home.phase()); assertTrue(home.consumed());
                error(RocksDBException.RocksDBErrorType.TX_NOT_FOUND,api.getAsync(update.updateId(),71,key(2),RequestType.<Buf>forUpdate()));
                await(api.closeFailedUpdateAsync(update.updateId())); await(api.closeFailedUpdateAsync(update.updateId())); assertEquals(0,f.opens.get());
            }
        }
    }

    @Test void unknownPrepareRetriesFrozenSingleWriteWithoutRestagingAndCleanupResolvesIt() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("unknown"),true)) {
            seed(f,71,key(2),value(1),1900); var stages=new AtomicInteger(); var base=f.transport();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return base.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return base.command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return base.participant(route,record); }
                public byte[] snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException { return base.snapshot(route,owner,request); }
                public RangeStateView stateView(RangeDescriptor route) throws IOException { return base.stateView(route); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    if (ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.STAGE) stages.incrementAndGet();
                    return base.transaction(route,request,fresh);
                }
            };
            try (var data=new RatisRoutedClient(f.directory,transport); var transactions=client(f,data)) {
                var allocations=new Allocations(f);
                try (var adapter=new RatisRocksDBAdapter(f.schema,f.directory,transactions,allocations,null,null)) {
                    var api=adapter.api(CONTEXT); long id=await(api.getAsync(0,71,key(2),RequestType.<Buf>forUpdate())).updateId();
                    f.losePrepareGroup=f.groups.getLast(); assertThrows(ExecutionException.class,() -> await(api.putAsync(id,71,key(2),value(8),RequestType.<Buf>none())));
                    int staged=stages.get(),captures=f.captureRequests.get(); var first=allocations.issued.getFirst();
                    assertNotNull(f.stores.get(f.groups.getLast()).preparedParticipant(first.identity().commit().transaction()));
                    await(api.putAsync(id,71,key(2),value(8),RequestType.<Buf>none()));
                    assertEquals(staged,stages.get()); assertEquals(captures,f.captureRequests.get()); assertEquals(1,allocations.count.get()); assertArrayEquals(new byte[]{8},f.get(71,key(2)));
                    try (var journal=RatisDurableTransaction.reopen(first.commit(),f.homes,data)) {
                        assertEquals(CommandResult.Status.SUCCESS,journal.result().status()); for (var participant:journal.snapshot().participants()) assertNotNull(participant.intent().captureId());
                    }
                    await(api.closeFailedUpdateAsync(id));
                    long cleanup=await(api.getAsync(0,71,key(2),RequestType.<Buf>forUpdate())).updateId(); f.losePrepareGroup=f.groups.getLast();
                    assertThrows(ExecutionException.class,() -> await(api.putAsync(cleanup,71,key(2),value(9),RequestType.<Buf>none())));
                    staged=stages.get(); captures=f.captureRequests.get(); await(api.closeFailedUpdateAsync(cleanup)); await(api.closeFailedUpdateAsync(cleanup));
                    assertEquals(staged,stages.get()); assertEquals(captures,f.captureRequests.get()); assertEquals(2,allocations.count.get()); assertEquals(0,adapter.stats().transactions()); assertArrayEquals(new byte[]{9},f.get(71,key(2))); assertEquals(0,f.opens.get());
                }
            }
        }
    }
}
