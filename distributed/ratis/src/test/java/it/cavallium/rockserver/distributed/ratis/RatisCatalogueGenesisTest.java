package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.Proxy;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.Message;
import static org.junit.jupiter.api.Assertions.*;

class RatisCatalogueGenesisTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisNodePlan plan(RatisSessionEpochsTest outer) {
        var source=outer.directory(outer.cluster);
        var directory=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),0,source.columns(),source.routingPrefixBytes(),source.ranges());
        var local=directory.ranges().getFirst().replicas().getFirst();
        var replica=new ReplicaDescriptor(local.nodeId(),local.nodeIncarnation(),new UUID(9,9),local.peerId(),local.address(),local.role());
        return new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),directory,
                new RatisNodePlan.Catalogue(outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,List.of(replica)),Map.of(outer.data,"data"),Set.of(),2);
    }
    @Test void lostAppliedReplyRestartsSameGenesisWithoutResettingLaterMetadata() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root.resolve("native"))) {
            var plan=plan(outer); Path journal=root.resolve("genesis.bin"); byte[] original;
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10)) {
                original=DistributedCommandCodec.encode(genesis.preparedCommand()); f.writeOverride=new byte[4];
                assertThrows(IOException.class,genesis::execute); assertNull(genesis.completedResult());
            }
            f.writeOverride=null;
            var initial=SessionEpochLedger.initial(outer.cluster,outer.database,10); assertEquals(initial,f.epochs.read());
            try (var pin=f.store.capture()) { assertArrayEquals(RangeDirectoryCodec.encode(plan.directory()),pin.get(outer.directoryColumn,RatisSessionEpochsTest.bytes(plan.directory().domainId()))); }
            var later=initial.advanceTime(20); f.epochs.execute(f.epochs.prepare(outer.operation(0),initial,later));
            var directory=plan.directory(); var changed=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),1,directory.columns(),directory.routingPrefixBytes(),directory.ranges());
            var catalogue=new RatisCatalogue(f.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
            catalogue.execute(catalogue.prepare(outer.operation(1),directory,changed));
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10)) {
                assertArrayEquals(original,DistributedCommandCodec.encode(genesis.preparedCommand()));
                assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); long submitted=f.next.get();
                assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); assertEquals(submitted,f.next.get());
            }
            assertEquals(later,f.epochs.read());
            try (var pin=f.store.capture()) { assertArrayEquals(RangeDirectoryCodec.encode(changed),pin.get(outer.directoryColumn,RatisSessionEpochsTest.bytes(changed.domainId()))); }
            assertThrows(IOException.class,() -> new RatisCatalogueGenesis(journal,f.client,plan,11));
            assertEquals(0,f.closes.get());
        }
    }
    @Test void eitherExistingKeyMakesCreationConflictWithoutWritingTheOtherKey() throws Exception {
        for (boolean session:List.of(false,true)) {
            var outer=new RatisSessionEpochsTest();
            try (var f=outer.new Fixture(root.resolve("partial-"+session))) {
                var plan=plan(outer); var ledger=SessionEpochLedger.initial(outer.cluster,outer.database,10);
                if (session) f.epochs.execute(f.epochs.prepare(outer.operation(0),null,ledger));
                else {
                    var catalogue=new RatisCatalogue(f.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
                    catalogue.execute(catalogue.prepare(outer.operation(0),null,plan.directory()));
                }
                try (var genesis=new RatisCatalogueGenesis(root.resolve("partial-"+session+".bin"),f.client,plan,10)) {
                    assertEquals(CommandResult.Status.CONFLICT,genesis.execute().status()); long submitted=f.next.get();
                    assertEquals(CommandResult.Status.CONFLICT,genesis.execute().status()); assertEquals(submitted,f.next.get());
                }
                try (var pin=f.store.capture()) {
                    assertEquals(session,pin.get(outer.sessions,RatisSessionEpochsTest.bytes(outer.database))!=null);
                    assertEquals(!session,pin.get(outer.directoryColumn,RatisSessionEpochsTest.bytes(plan.directory().domainId()))!=null);
                }
            }
        }
    }
    @Test void combinedRuleAcceptsOnlyCanonicalAbsentInitialPair() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root.resolve("rules")); var genesis=new RatisCatalogueGenesis(root.resolve("rules.bin"),f.client,plan(outer),10)) {
            var valid=genesis.preparedCommand(); assertDoesNotThrow(() -> f.validate.accept(valid));
            var mutations=valid.mutations(); var predicates=valid.predicates();
            assertThrows(IllegalArgumentException.class,() -> f.validate.accept(new DistributedCommand(valid.groupId(),1,new byte[32],valid.operationId(),
                    List.of(mutations.get(1),mutations.get(0)),List.of(predicates.get(1),predicates.get(0)))));
            assertThrows(IllegalArgumentException.class,() -> f.validate.accept(new DistributedCommand(valid.groupId(),1,new byte[32],valid.operationId(),mutations,
                    List.of(predicates.get(0),new KeyPredicate(outer.sessions,RatisSessionEpochsTest.bytes(outer.database),new byte[0])))));
            var issued=SessionEpochLedger.initial(outer.cluster,outer.database,10).issue(outer.a,100);
            assertThrows(IllegalArgumentException.class,() -> f.validate.accept(new DistributedCommand(valid.groupId(),1,new byte[32],valid.operationId(),
                    List.of(mutations.get(0),new Mutation(Mutation.Kind.PUT,outer.sessions,RatisSessionEpochsTest.bytes(outer.database),issued.encode())),predicates)));
            assertEquals(0,f.next.get());
        }
    }
    @Test void activeRpcRetainsExclusiveIntentUntilNativeAndJournalCompletion() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root.resolve("active"))) {
            var started=new CountDownLatch(1); var release=new CountDownLatch(1);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                if (!method.getName().equals("send")) throw new AssertionError(method);
                started.countDown();
                if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("fixture timeout");
                return f.client.io().send((Message)args[0]);
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch (method.getName()) {
                case "getGroupId" -> f.client.getGroupId(); case "io" -> io; default -> throw new AssertionError(method);
            });
            var plan=plan(outer); Path journal=root.resolve("active.bin");
            try (var genesis=new RatisCatalogueGenesis(journal,client,plan,10); var executor=Executors.newSingleThreadExecutor()) {
                var pending=executor.submit(genesis::execute);
                try {
                    assertTrue(started.await(5,TimeUnit.SECONDS)); assertThrows(IOException.class,genesis::close);
                    assertThrows(IllegalStateException.class,genesis::execute);
                    assertThrows(IOException.class,() -> new RatisCatalogueGenesis(journal,f.client,plan,10));
                    assertNull(genesis.completedResult());
                } finally { release.countDown(); }
                assertEquals(CommandResult.Status.SUCCESS,pending.get(5,TimeUnit.SECONDS).status());
            }
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10)) {
                assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); assertEquals(1,f.next.get());
            }
        }
    }
    @Test void failedCompletionPersistenceFencesAndReopenResolvesOriginalNativeReceipt() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root.resolve("persistence"))) {
            var plan=plan(outer); Path journal=root.resolve("failure.bin"); byte[] pending;
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10)) {
                assertThrows(IOException.class,() -> new RatisCatalogueGenesis(journal,f.client,plan,10));
                pending=Files.readAllBytes(journal); Files.delete(journal); Files.createDirectory(journal); Files.writeString(journal.resolve("block"),"fixture");
                assertThrows(IOException.class,genesis::execute); assertNull(genesis.completedResult());
                assertEquals(1,f.next.get()); assertThrows(IOException.class,genesis::execute);
            }
            Files.delete(journal.resolve("block")); Files.delete(journal); Files.write(journal,pending);
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10)) { assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); }
            assertEquals(1,f.store.receiptUsage().retainedCount());
            byte[] bytes=Files.readAllBytes(journal); bytes[bytes.length/2]^=1; Files.write(journal,bytes);
            assertThrows(IOException.class,() -> new RatisCatalogueGenesis(journal,f.client,plan,10)); assertArrayEquals(bytes,Files.readAllBytes(journal));
        }
    }
}
