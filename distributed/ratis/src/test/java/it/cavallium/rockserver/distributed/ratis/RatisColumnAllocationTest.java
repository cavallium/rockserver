package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native catalogue CAS and process-style reopen fixtures; no transport/quorum/power-loss claim. */
class RatisColumnAllocationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(int n) { return new UUID(42,n); }
    static RatisColumnAllocation allocator(RatisSessionEpochsTest outer,RatisSessionEpochsTest.Fixture f) { return new RatisColumnAllocation(f.client,outer.catalogue,1,new byte[32],outer.directoryColumn); }
    static RatisSchemaCatalogue schemas(RatisSessionEpochsTest outer,RatisSessionEpochsTest.Fixture f) { return new RatisSchemaCatalogue(f.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn); }
    static DistributedSchema schema(RatisSessionEpochsTest outer,UUID domain,long epoch,long api,UUID column) {
        return new DistributedSchema(outer.cluster,outer.database,domain,epoch,0,List.of(new DistributedSchema.Column(api,column,"data",ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(),true),null)));
    }
    @Test void concurrentDifferentDomainsRaceFrozenCounterAndConsumeDistinctIds() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root); var executor=Executors.newFixedThreadPool(2)) {
            var allocation=allocator(outer,f);
            var left=executor.submit(() -> allocation.prepare(outer.operation(0),id(1),id(11),id(21),null));
            var right=executor.submit(() -> allocation.prepare(outer.operation(1),id(2),id(12),id(22),null));
            var a=left.get(5,TimeUnit.SECONDS); var b=right.get(5,TimeUnit.SECONDS);
            assertEquals(CommandResult.Status.SUCCESS,allocation.execute(a).status()); assertEquals(CommandResult.Status.CONFLICT,allocation.execute(b).status());
            assertEquals(CommandResult.Status.CONFLICT,allocation.execute(b).status(),"Retry keeps original failed CAS");
            assertEquals(1,allocation.read(id(1)).apiId()); assertNull(allocation.read(id(2)));
            var retry=allocation.prepare(outer.operation(2),id(2),id(12),id(22),allocation.readCounter());
            assertEquals(CommandResult.Status.SUCCESS,allocation.execute(retry).status()); assertEquals(2,allocation.read(id(2)).apiId());
            var catalogue=schemas(outer,f); var second=schema(outer,id(12),1,2,id(22));
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(outer.operation(3),second,allocation.readCounter(),Map.of(),Map.of(),Map.of(id(22),allocation.read(id(2))))).status());
            var first=schema(outer,id(11),1,1,id(21));
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(outer.operation(4),first,allocation.readCounter(),Map.of(),Map.of(),Map.of(id(21),allocation.read(id(1))))).status());
            assertEquals(2,RatisColumnAllocation.counter(allocation.readCounter())); assertEquals(0,f.closes.get());
        }
    }
    @Test void highIdBootstrapAndReservationRaceShareOneCounter() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            var allocation=allocator(outer,f); var catalogue=schemas(outer,f);
            var bootstrap=schema(outer,id(11),1,100,id(21)); var frozen=catalogue.prepare(outer.operation(0),bootstrap);
            var reservation=allocation.prepare(outer.operation(1),id(1),id(12),id(22),null);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(frozen).status());
            assertEquals(CommandResult.Status.CONFLICT,allocation.execute(reservation).status());
            assertEquals(100,RatisColumnAllocation.counter(allocation.readCounter()));
            assertEquals(CommandResult.Status.SUCCESS,allocation.execute(allocation.prepare(outer.operation(2),id(1),id(12),id(22),allocation.readCounter())).status());
            assertEquals(101,allocation.read(id(1)).apiId());
            assertThrows(IllegalArgumentException.class,() -> catalogue.prepare(outer.operation(3),schema(outer,id(13),1,99,id(23)),allocation.readCounter(),Map.of(),Map.of(),Map.of()));
            var conflictingBootstrap=catalogue.prepare(outer.operation(4),schema(outer,id(14),1,200,id(24)),allocation.readCounter(),Map.of(),Map.of(),Map.of());
            assertEquals(CommandResult.Status.SUCCESS,allocation.execute(allocation.prepare(outer.operation(5),id(2),id(15),id(25),allocation.readCounter())).status());
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(conflictingBootstrap).status()); assertEquals(102,RatisColumnAllocation.counter(allocation.readCounter()));
        }
    }
    @Test void lostReplyAndReopenRecoverOriginalRequestAndExactSavedCommand() throws Exception {
        var outer=new RatisSessionEpochsTest(); Path database=root.resolve("reopen"),journal=root.resolve("allocation.bin"); byte[] frozen;
        try (var f=outer.new Fixture(database)) {
            var allocation=allocator(outer,f); var command=allocation.prepare(outer.operation(0),id(1),id(11),id(21),null); frozen=DistributedCommandCodec.encode(command); Files.write(journal,frozen);
            f.writeOverride=new byte[4]; assertThrows(IOException.class,() -> allocation.execute(command));
            assertEquals(1,allocation.read(id(1)).apiId()); assertEquals(1,RatisColumnAllocation.counter(allocation.readCounter()));
        }
        try (var f=outer.new Fixture(database)) {
            f.next.set(f.store.applied().index()+1); var allocation=allocator(outer,f); var command=DistributedCommandCodec.decode(Files.readAllBytes(journal));
            assertArrayEquals(frozen,DistributedCommandCodec.encode(command)); assertEquals(CommandResult.Status.SUCCESS,allocation.execute(command).status());
            assertEquals(new RatisColumnAllocation.Reservation(id(1),id(11),id(21),1),allocation.read(id(1)));
            assertEquals(CommandResult.Status.CONFLICT,allocation.execute(allocation.prepare(outer.operation(1),id(1),id(12),id(22),allocation.readCounter())).status());
            assertEquals(1,RatisColumnAllocation.counter(allocation.readCounter()));
        }
    }
    @Test void abortedSchemaConsumesGlobalReservationPermanently() throws Exception {
        var fixture=new RatisSchemaAdministrationTest(); var outer=fixture.outer;
        try (var f=outer.new Fixture(root)) {
            var allocation=allocator(outer,f); var source=fixture.initial(); fixture.publishSchema(f,source);
            assertEquals(CommandResult.Status.SUCCESS,allocation.execute(allocation.prepare(fixture.operation(),id(1),source.domainId(),id(21),allocation.readCounter())).status());
            var reservation=allocation.read(id(1)); assertEquals(8,reservation.apiId());
            var target=fixture.next(source,List.of(source.column(7),fixture.column(8,id(21),"added")));
            var existing=fixture.admin(fixture.client(f)).readColumnIdentity(outer.data); var catalogue=schemas(outer,f);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(fixture.operation(),target,allocation.readCounter(),Map.of(outer.data,existing),Map.of(7L,existing),Map.of(id(21),reservation))).status());
            var admin=fixture.admin(fixture.client(f)); var directory=fixture.directory(source,0); var directories=fixture.catalogue(fixture.client(f));
            assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(fixture.operation(),null,directory)).status());
            var definition=new SchemaChange.Definition(id(2),fixture.operation(),directory,source,target); var head=fixture.begin(admin,definition,null);
            assertEquals(CommandResult.Status.SUCCESS,admin.execute(admin.prepareAdvance(fixture.operation(),head,head.status().abort())).status());
            assertEquals(8,RatisColumnAllocation.counter(allocation.readCounter())); assertNotNull(allocation.read(id(1)));
            assertEquals(CommandResult.Status.SUCCESS,allocation.execute(allocation.prepare(fixture.operation(),id(3),id(12),id(22),allocation.readCounter())).status());
            assertEquals(9,allocation.read(id(3)).apiId());
        }
    }
    @Test void all256ReservedColumnsPublish512MarkersAndFrozenReservationPredicatesWithinBudget() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            f.store.enableCatalogueScopeCompaction(); var allocation=allocator(outer,f); var catalogue=schemas(outer,f);
            var columns=new ArrayList<DistributedSchema.Column>(); var reservations=new HashMap<UUID,RatisColumnAllocation.Reservation>();
            long sequence=0; byte[] witness=null;
            for (int n=1;n<=256;n++) {
                UUID column=id(1000+n),request=id(2000+n);
                assertEquals(CommandResult.Status.SUCCESS,allocation.execute(allocation.prepare(outer.operation(sequence++),request,id(11),column,witness)).status());
                var reservation=allocation.read(request); reservations.put(column,reservation); witness=allocation.readCounter();
                columns.add(new DistributedSchema.Column(reservation.apiId(),column,"column"+n,ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true),null));
            }
            var schema=new DistributedSchema(outer.cluster,outer.database,id(11),1,0,columns);
            var command=catalogue.prepare(outer.operation(sequence),schema,witness,Map.of(),Map.of(),reservations);
            assertEquals(514,command.mutations().size()); assertEquals(770,command.predicates().size());
            byte[] encoded=DistributedCommandCodec.encode(command); assertTrue(encoded.length<=DistributedCommandCodec.MAX_ENCODED_BYTES);
            assertArrayEquals(encoded,DistributedCommandCodec.encode(DistributedCommandCodec.decode(encoded)));
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(command).status()); assertEquals(256,RatisColumnAllocation.counter(allocation.readCounter()));
        }
    }
    @Test void composedValidatorRejectsReceiptAcknowledgementsInAllocationAndSchemaPublication() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            var allocation=allocator(outer,f); var catalogue=schemas(outer,f);
            var reservation=allocation.prepare(outer.operation(1),id(1),id(11),id(21),null);
            var publication=catalogue.prepare(outer.operation(1),schema(outer,id(11),1,100,id(21)));
            for (var command:List.of(reservation,publication)) {
                var piggyback=new DistributedCommand(command.groupId(),command.rangeEpoch(),command.schemaDigest(),command.operationId(),command.mutations(),command.predicates(),0);
                assertThrows(IllegalArgumentException.class,() -> f.validate.accept(piggyback));
                if (command==reservation) assertThrows(IllegalArgumentException.class,() -> allocation.execute(piggyback));
                else assertThrows(IllegalArgumentException.class,() -> catalogue.execute(piggyback));
            }
            assertEquals(0,f.next.get()); assertNull(allocation.readCounter());
        }
    }
    @Test void malformedWitnessesOverflowAndCounterRollbackAreRejectedBeforeRpc() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            var allocation=allocator(outer,f);
            for (byte[] witness:new byte[][]{new byte[1],RatisColumnAllocation.counterValue(Long.MAX_VALUE)}) {
                assertThrows(RuntimeException.class,() -> allocation.prepare(outer.operation(0),id(1),id(11),id(21),witness));
            }
            byte[] negative=new byte[8]; negative[0]=(byte)0x80;
            assertThrows(IllegalArgumentException.class,() -> allocation.prepare(outer.operation(0),id(1),id(11),id(21),negative));
            var valid=allocation.prepare(outer.operation(0),id(1),id(11),id(21),null);
            var mutations=new ArrayList<>(valid.mutations()); mutations.set(1,new Mutation(Mutation.Kind.PUT,outer.directoryColumn,RatisColumnAllocation.counterKey(),RatisColumnAllocation.counterValue(0)));
            var malformed=new DistributedCommand(outer.catalogue,1,new byte[32],outer.operation(0),mutations,valid.predicates());
            assertThrows(IllegalArgumentException.class,() -> allocation.execute(malformed)); assertEquals(0,f.next.get());
            var catalogue=schemas(outer,f); var reserved=schema(outer,id(11),1,1,id(21));
            assertThrows(IllegalArgumentException.class,() -> catalogue.prepare(outer.operation(1),reserved,RatisColumnAllocation.counterValue(1),Map.of(),Map.of(),Map.of(id(21),new RatisColumnAllocation.Reservation(id(1),id(12),id(21),1))));
            var publication=catalogue.prepare(outer.operation(2),schema(outer,id(11),1,100,id(21))); var wrong=new ArrayList<>(publication.mutations());
            wrong.set(wrong.size()-1,new Mutation(Mutation.Kind.PUT,outer.directoryColumn,RatisColumnAllocation.counterKey(),RatisColumnAllocation.counterValue(99)));
            assertThrows(IllegalArgumentException.class,() -> catalogue.execute(new DistributedCommand(outer.catalogue,1,new byte[32],outer.operation(2),wrong,publication.predicates())));
        }
    }
}
