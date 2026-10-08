package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Native catalogue/journal composition, without transport/quorum/power-loss acceptance claims. */
class RatisSchemaCatalogueTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static DistributedSchema schema(RatisSessionEpochsTest outer) {
        var directory=outer.directory(outer.cluster);
        return new DistributedSchema(outer.cluster,outer.database,directory.domainId(),1,0,List.of(new DistributedSchema.Column(7,outer.data,"data",
                ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(),true),null)));
    }
    private static RatisNodePlan plan(RatisSessionEpochsTest outer,DistributedSchema schema) {
        var source=outer.directory(outer.cluster);
        var directory=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),schema.schemaEpoch(),schema.digest(),0,
                schema.columnIds(),schema.routingPrefixBytes(),source.ranges());
        var local=source.ranges().getFirst().replicas().getFirst();
        var replica=new ReplicaDescriptor(local.nodeId(),local.nodeIncarnation(),new UUID(9,9),local.peerId(),local.address(),local.role());
        return new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),directory,
                new RatisNodePlan.Catalogue(outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,List.of(replica)),Map.of(outer.data,"data"),Set.of(),2);
    }
    private static RatisSchemaCatalogue catalogue(RatisSessionEpochsTest outer,RaftClient client) {
        return new RatisSchemaCatalogue(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
    }

    @Test void lostAppliedReplyAndReopenRetryExactCommandThenReuseDurableResultWithoutRpc() throws Exception {
        var outer=new RatisSessionEpochsTest(); var schema=schema(outer); var plan=plan(outer,schema);
        try (var f=outer.new Fixture(root.resolve("native"))) {
            var catalogue=catalogue(outer,f.client); assertNull(catalogue.read(schema.digest()));
            Path journal=root.resolve("schema.bin"); byte[] frozen;
            try (var publication=new RatisSchemaPublication(journal,f.client,plan,schema)) {
                frozen=DistributedCommandCodec.encode(publication.preparedCommand());
                assertEquals(0,publication.preparedCommand().operationId().epoch()); assertEquals(0,publication.preparedCommand().operationId().sequence());
                f.writeOverride=new byte[4]; assertThrows(IOException.class,publication::execute); assertNull(publication.completedResult());
                assertArrayEquals(DistributedSchemaCodec.encode(schema),DistributedSchemaCodec.encode(catalogue.read(schema.digest())));
                assertEquals(1,f.next.get()); assertEquals(0,f.closes.get());
            }
            f.writeOverride=null;
            try (var publication=new RatisSchemaPublication(journal,f.client,plan,schema)) {
                assertArrayEquals(frozen,DistributedCommandCodec.encode(publication.preparedCommand()));
                assertEquals(CommandResult.Status.SUCCESS,publication.execute().status());
                long calls=f.next.get(); assertEquals(CommandResult.Status.SUCCESS,publication.execute().status()); assertEquals(calls,f.next.get());
                assertEquals(1,f.store.receiptUsage().retainedCount());
            }
            try (var publication=new RatisSchemaPublication(journal,f.client,plan,schema)) {
                long calls=f.next.get(); assertEquals(CommandResult.Status.SUCCESS,publication.completedResult().status());
                assertEquals(CommandResult.Status.SUCCESS,publication.execute().status()); assertEquals(calls,f.next.get());
            }
            assertEquals(0,f.closes.get());
        }
    }

    @Test void existingContentProducesSavedConflictAndNeverReplacesThePublishedBytes() throws Exception {
        var outer=new RatisSessionEpochsTest(); var schema=schema(outer); var plan=plan(outer,schema);
        try (var f=outer.new Fixture(root.resolve("conflict"))) {
            var catalogue=catalogue(outer,f.client);
            var first=catalogue.prepare(outer.operation(0),schema); assertTrue(RatisSchemaCatalogue.isSchemaCommand(first));
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(first).status());
            var second=catalogue.prepare(outer.operation(1),schema); byte[] frozen=DistributedCommandCodec.encode(second);
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(second).status());
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(second).status()); assertArrayEquals(frozen,DistributedCommandCodec.encode(second));
            assertArrayEquals(DistributedSchemaCodec.encode(schema),DistributedSchemaCodec.encode(catalogue.read(schema.digest())));
            try (var publication=new RatisSchemaPublication(root.resolve("conflict.bin"),f.client,plan,schema)) {
                assertEquals(CommandResult.Status.CONFLICT,publication.execute().status());
            }
            try (var publication=new RatisSchemaPublication(root.resolve("conflict.bin"),f.client,plan,schema)) {
                long calls=f.next.get(); assertEquals(CommandResult.Status.CONFLICT,publication.execute().status()); assertEquals(calls,f.next.get());
            }
        }
    }

    @Test void namespaceContentHashIdentityAndExactAbsentPredicateAreValidated() throws Exception {
        var outer=new RatisSessionEpochsTest(); var schema=schema(outer);
        try (var f=outer.new Fixture(root.resolve("validation"))) {
            var catalogue=catalogue(outer,f.client); var valid=catalogue.prepare(outer.operation(0),schema);
            var validator=RatisSchemaCatalogue.validator(outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
            assertDoesNotThrow(() -> validator.accept(valid)); assertDoesNotThrow(() -> f.validate.accept(valid));
            for (int offset:new int[] {0,1}) {
                byte[] wrong=valid.mutations().getFirst().key(); wrong[offset]^=1;
                var command=new DistributedCommand(outer.catalogue,1,new byte[32],outer.operation(0),
                        List.of(new Mutation(Mutation.Kind.PUT,outer.directoryColumn,wrong,valid.mutations().getFirst().value())),List.of(new KeyPredicate(outer.directoryColumn,wrong,null)));
                assertThrows(IllegalArgumentException.class,() -> validator.accept(command)); assertThrows(IllegalArgumentException.class,() -> f.validate.accept(command));
            }
            assertThrows(IllegalArgumentException.class,() -> validator.accept(new DistributedCommand(outer.catalogue,1,new byte[32],outer.operation(0),
                    valid.mutations(),List.of(new KeyPredicate(outer.directoryColumn,valid.mutations().getFirst().key(),new byte[0])))));
            assertDoesNotThrow(() -> validator.accept(new DistributedCommand(outer.catalogue,1,new byte[32],outer.operation(1),valid.mutations(),valid.predicates(),0)));
            assertThrows(IllegalArgumentException.class,() -> validator.accept(new DistributedCommand(outer.dataGroup,1,new byte[32],outer.operation(0),valid.mutations(),valid.predicates())));
            var returning=new Mutation(Mutation.Kind.PUT,outer.directoryColumn,valid.mutations().getFirst().key(),valid.mutations().getFirst().value(),Mutation.ReturnMode.PREVIOUS);
            assertThrows(IllegalArgumentException.class,() -> validator.accept(new DistributedCommand(outer.catalogue,1,new byte[32],outer.operation(0),List.of(returning),valid.predicates())));
            var foreign=new DistributedSchema(UUID.randomUUID(),outer.database,schema.domainId(),1,0,schema.columns());
            assertThrows(IllegalArgumentException.class,() -> catalogue.prepare(outer.operation(0),foreign));
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaCatalogue.key(new byte[31]));
            byte[] raw=RatisSchemaCatalogue.key(schema.digest()); raw[1]^=1; assertArrayEquals(schema.digest(),Arrays.copyOfRange(RatisSchemaCatalogue.key(schema.digest()),1,33));
            assertEquals(0,f.next.get());
            f.readOverride=ByteBuffer.allocate(4).putInt(999).array(); assertThrows(IOException.class,() -> catalogue.read(schema.digest()));
            byte[] wrongContent=DistributedSchemaCodec.encode(foreign);
            f.readOverride=ByteBuffer.allocate(4+wrongContent.length).putInt(wrongContent.length).put(wrongContent).array();
            assertThrows(IOException.class,() -> catalogue.read(schema.digest()));
            f.readOverride=ByteBuffer.allocate(5).putInt(-1).put((byte)0).array(); assertThrows(IOException.class,() -> catalogue.read(schema.digest()));
        }
    }

    @Test void validCoreSchemaLargerThanPublicationEnvelopeBudgetIsRejected() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root.resolve("budget"))) {
            String name="o".repeat(2048), className="c".repeat(1949);
            var operator=new DistributedSchema.Operator(name,1,className,new byte[32],1,1);
            var shape=ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true,name,1L,className);
            var columns=new ArrayList<DistributedSchema.Column>();
            for (int i=1;i<=256;i++) columns.add(new DistributedSchema.Column(i,new UUID(0,i),"n".repeat(4092)+String.format(Locale.ROOT,"%04d",i),shape,operator));
            var large=new DistributedSchema(outer.cluster,outer.database,outer.directory(outer.cluster).domainId(),1,0,columns);
            assertTrue(DistributedSchemaCodec.encode(large).length>RatisSchemaCatalogue.MAX_SCHEMA_BYTES);
            assertThrows(IllegalArgumentException.class,() -> catalogue(outer,f.client).prepare(outer.operation(0),large));
            f.readOverride=new byte[RatisSchemaCatalogue.MAX_SCHEMA_BYTES+5]; assertThrows(IOException.class,() -> catalogue(outer,f.client).read(large.digest()));
            assertEquals(0,f.next.get());
        }
    }

    @Test void expiredPublicationHasDurableTerminalResultAndPublishesNoContent() throws Exception {
        var outer=new RatisSessionEpochsTest(); var schema=schema(outer); var plan=plan(outer,schema);
        try (var f=outer.new Fixture(root.resolve("expired"))) {
            var floor=DistributedCommand.advanceSessionEpochFloor(outer.catalogue,1,new byte[32],new OperationId(outer.maintenance,1,0),1);
            f.store.apply(new ApplyPosition(1,f.next.getAndIncrement()),floor);
            Path journal=root.resolve("expired.bin");
            try (var publication=new RatisSchemaPublication(journal,f.client,plan,schema)) {
                assertEquals(CommandResult.Status.SESSION_EXPIRED,publication.execute().status());
                assertNull(catalogue(outer,f.client).read(schema.digest()));
            }
            try (var publication=new RatisSchemaPublication(journal,f.client,plan,schema)) {
                long calls=f.next.get(); assertEquals(CommandResult.Status.SESSION_EXPIRED,publication.completedResult().status());
                assertEquals(CommandResult.Status.SESSION_EXPIRED,publication.execute().status()); assertEquals(calls,f.next.get());
            }
        }
    }

    @Test void journalFailureLocksChangedIntentAndCorruptionFenceUntilSafeReopen() throws Exception {
        var outer=new RatisSessionEpochsTest(); var schema=schema(outer); var plan=plan(outer,schema);
        try (var f=outer.new Fixture(root.resolve("journal"))) {
            Path journal=root.resolve("schema.bin"); byte[] pending;
            try (var publication=new RatisSchemaPublication(journal,f.client,plan,schema)) {
                assertThrows(IOException.class,() -> new RatisSchemaPublication(journal,f.client,plan,schema));
                pending=Files.readAllBytes(journal); Files.delete(journal); Files.createDirectory(journal); Files.writeString(journal.resolve("block"),"fixture");
                assertThrows(IOException.class,publication::execute); assertNull(publication.completedResult()); assertThrows(IOException.class,publication::execute);
                assertEquals(1,f.next.get());
            }
            Files.delete(journal.resolve("block")); Files.delete(journal); Files.write(journal,pending);
            try (var publication=new RatisSchemaPublication(journal,f.client,plan,schema)) { assertEquals(CommandResult.Status.SUCCESS,publication.execute().status()); }
            var c=plan.catalogue(); var changed=new RatisNodePlan(plan.nodeId(),plan.nodeIncarnation(),plan.directory(),
                    new RatisNodePlan.Catalogue(c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn(),new UUID(99,99),c.replicas()),plan.columns(),plan.merges(),2);
            assertThrows(IOException.class,() -> new RatisSchemaPublication(journal,f.client,changed,schema),"Intent binds the unused catalogue sessions column too");
            byte[] valid=Files.readAllBytes(journal), damaged=valid.clone(); damaged[damaged.length/2]^=1; Files.write(journal,damaged);
            assertThrows(IOException.class,() -> new RatisSchemaPublication(journal,f.client,plan,schema));
            Files.write(journal,valid);
            try (var publication=new RatisSchemaPublication(journal,f.client,plan,schema)) { assertEquals(CommandResult.Status.SUCCESS,publication.execute().status()); }
            assertEquals(0,f.closes.get());
        }
    }

    @Test void concurrentCloseRetainsOwnershipThroughNativeAndJournalCompletion() throws Exception {
        var outer=new RatisSessionEpochsTest(); var schema=schema(outer); var plan=plan(outer,schema);
        try (var f=outer.new Fixture(root.resolve("active"))) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                if (!method.getName().equals("send")) throw new AssertionError(method);
                entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("fixture timeout");
                return f.client.io().send((Message)args[0]);
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) {
                case "getGroupId" -> f.client.getGroupId(); case "io" -> io; default -> throw new AssertionError(method);
            });
            Path journal=root.resolve("active.bin");
            try (var publication=new RatisSchemaPublication(journal,client,plan,schema); var executor=Executors.newSingleThreadExecutor()) {
                var pending=executor.submit(publication::execute);
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); assertThrows(IOException.class,publication::close);
                    assertThrows(IllegalStateException.class,publication::execute); assertNull(publication.completedResult());
                    assertThrows(IOException.class,() -> new RatisSchemaPublication(journal,f.client,plan,schema));
                } finally { release.countDown(); }
                assertEquals(CommandResult.Status.SUCCESS,pending.get(5,TimeUnit.SECONDS).status());
            }
        }
    }
}
