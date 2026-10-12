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
    RatisNodePlan secondDomain(RatisSessionEpochsTest outer) {
        var first=plan(outer);var d=first.directory();var source=d.ranges().getFirst();UUID column=new UUID(22,3);
        var range=new RangeDescriptor(new UUID(26,2),new UUID(23,1),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,source.replicas());
        var directory=new RangeDirectory(d.clusterId(),d.databaseId(),new UUID(26,1),d.schemaEpoch(),d.schemaDigest(),0,Set.of(column),0,List.of(range));
        return new RatisNodePlan(first.nodeId(),first.nodeIncarnation(),directory,first.catalogue(),Map.of(column,"domain-two"),Set.of(),2);
    }
    @Test void additionalDomainRetainsLedgerAndOriginalPublicationAcrossLostReplyAndLaterIssuance() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try(var f=outer.new Fixture(root.resolve("two-domains"))) {
            var first=plan(outer);try(var genesis=new RatisCatalogueGenesis(root.resolve("first.bin"),f.client,first,10,null)){assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status());}
            var initial=f.epochs.read();var issued=initial.issue(outer.a,100);assertEquals(CommandResult.Status.SUCCESS,f.epochs.execute(f.epochs.prepare(outer.operation(0),initial,issued)).status());
            var second=secondDomain(outer);Path journal=root.resolve("second.bin");byte[] original;
            try(var genesis=new RatisCatalogueGenesis(journal,f.client,second,0,issued)) {
                assertEquals(1,genesis.preparedCommand().mutations().size());original=DistributedCommandCodec.encode(genesis.preparedCommand());
                f.writeOverride=new byte[4];assertThrows(IOException.class,genesis::execute);assertNull(genesis.completedResult());
            }
            f.writeOverride=null;assertEquals(issued,f.epochs.read());var later=issued.advanceTime(20).issue(outer.b,100);
            assertEquals(CommandResult.Status.SUCCESS,f.epochs.execute(f.epochs.prepare(outer.operation(1),issued,issued.advanceTime(20))).status());
            assertEquals(CommandResult.Status.SUCCESS,f.epochs.execute(f.epochs.prepare(outer.operation(2),issued.advanceTime(20),later)).status());
            try(var genesis=new RatisCatalogueGenesis(journal,f.client,second,999,issued)){assertArrayEquals(original,DistributedCommandCodec.encode(genesis.preparedCommand()));assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status());}
            assertEquals(later,f.epochs.read());try(var pin=f.store.capture()){assertArrayEquals(RangeDirectoryCodec.encode(first.directory()),pin.get(outer.directoryColumn,RatisSessionEpochsTest.bytes(first.directory().domainId())));assertArrayEquals(RangeDirectoryCodec.encode(second.directory()),pin.get(outer.directoryColumn,RatisSessionEpochsTest.bytes(second.directory().domainId())));}
            assertThrows(IOException.class,()->new RatisCatalogueGenesis(journal,f.client,second,0,later));
        }
    }
    @Test void additionalDomainRejectsStaleOrForeignLedgerWithoutResettingIt() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try(var f=outer.new Fixture(root.resolve("guarded-domain"))) {
            var ledger=SessionEpochLedger.initial(outer.cluster,outer.database,10);assertEquals(CommandResult.Status.SUCCESS,f.epochs.execute(f.epochs.prepare(outer.operation(0),null,ledger)).status());
            var next=ledger.issue(outer.a,100);var second=secondDomain(outer);Path journal=root.resolve("stale-domain.bin");
            try(var genesis=new RatisCatalogueGenesis(journal,f.client,second,0,ledger)) {
                var command=genesis.preparedCommand();var predicates=new ArrayList<>(command.predicates());predicates.set(1,new KeyPredicate(outer.sessions,RatisSessionEpochsTest.bytes(outer.database),SessionEpochLedger.initial(new UUID(90,90),outer.database,10).encode()));
                assertThrows(IllegalArgumentException.class,()->f.validate.accept(new DistributedCommand(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.operationId(),command.mutations(),predicates)));
                assertEquals(CommandResult.Status.SUCCESS,f.epochs.execute(f.epochs.prepare(outer.operation(1),ledger,next)).status());assertEquals(CommandResult.Status.CONFLICT,genesis.execute().status());
            }
            assertEquals(next,f.epochs.read());try(var pin=f.store.capture()){assertNull(pin.get(outer.directoryColumn,RatisSessionEpochsTest.bytes(second.directory().domainId())));}
            assertThrows(IOException.class,()->new RatisCatalogueGenesis(journal,f.client,second,0,next));
            assertThrows(IllegalArgumentException.class,()->new RatisCatalogueGenesis(root.resolve("foreign.bin"),f.client,second,0,SessionEpochLedger.initial(new UUID(90,90),outer.database,10)));
        }
    }
    @Test void lostAppliedReplyRestartsSameGenesisWithoutResettingLaterMetadata() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root.resolve("native"))) {
            var plan=plan(outer); Path journal=root.resolve("genesis.bin"); byte[] original;
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10,null)) {
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
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10,null)) {
                assertArrayEquals(original,DistributedCommandCodec.encode(genesis.preparedCommand()));
                assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); long submitted=f.next.get();
                assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); assertEquals(submitted,f.next.get());
            }
            assertEquals(later,f.epochs.read());
            try (var pin=f.store.capture()) { assertArrayEquals(RangeDirectoryCodec.encode(changed),pin.get(outer.directoryColumn,RatisSessionEpochsTest.bytes(changed.domainId()))); }
            assertThrows(IOException.class,() -> new RatisCatalogueGenesis(journal,f.client,plan,11,null));
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
                try (var genesis=new RatisCatalogueGenesis(root.resolve("partial-"+session+".bin"),f.client,plan,10,null)) {
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
        try (var f=outer.new Fixture(root.resolve("rules")); var genesis=new RatisCatalogueGenesis(root.resolve("rules.bin"),f.client,plan(outer),10,null)) {
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
            try (var genesis=new RatisCatalogueGenesis(journal,client,plan,10,null); var executor=Executors.newSingleThreadExecutor()) {
                var pending=executor.submit(genesis::execute);
                try {
                    assertTrue(started.await(5,TimeUnit.SECONDS)); assertThrows(IOException.class,genesis::close);
                    assertThrows(IllegalStateException.class,genesis::execute);
                    assertThrows(IOException.class,() -> new RatisCatalogueGenesis(journal,f.client,plan,10,null));
                    assertNull(genesis.completedResult());
                } finally { release.countDown(); }
                assertEquals(CommandResult.Status.SUCCESS,pending.get(5,TimeUnit.SECONDS).status());
            }
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10,null)) {
                assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); assertEquals(1,f.next.get());
            }
        }
    }
    @Test void failedCompletionPersistenceFencesAndReopenResolvesOriginalNativeReceipt() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root.resolve("persistence"))) {
            var plan=plan(outer); Path journal=root.resolve("failure.bin"); byte[] pending;
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10,null)) {
                assertThrows(IOException.class,() -> new RatisCatalogueGenesis(journal,f.client,plan,10,null));
                pending=Files.readAllBytes(journal); Files.delete(journal); Files.createDirectory(journal); Files.writeString(journal.resolve("block"),"fixture");
                assertThrows(IOException.class,genesis::execute); assertNull(genesis.completedResult());
                assertEquals(1,f.next.get()); assertThrows(IOException.class,genesis::execute);
            }
            Files.delete(journal.resolve("block")); Files.delete(journal); Files.write(journal,pending);
            try (var genesis=new RatisCatalogueGenesis(journal,f.client,plan,10,null)) { assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); }
            assertEquals(1,f.store.receiptUsage().retainedCount());
            byte[] bytes=Files.readAllBytes(journal); bytes[bytes.length/2]^=1; Files.write(journal,bytes);
            assertThrows(IOException.class,() -> new RatisCatalogueGenesis(journal,f.client,plan,10,null)); assertArrayEquals(bytes,Files.readAllBytes(journal));
        }
    }
}
