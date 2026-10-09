package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native catalogue allocation/issuance and durable client recovery; DATA lifecycle is tested separately. */
class RatisSchemaClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static ColumnSchema shape() { return ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(),true); }
    static final class Fixture implements AutoCloseable {
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest();
        final RatisSessionEpochsTest.Fixture nativeStore;
        final DistributedSchema schema;
        final RangeDirectory directory;
        final RatisRoutedClient routed;
        final RatisSchemaReplicaClient replicas;
        final RatisNodePlan.Catalogue catalogue;
        final org.apache.ratis.client.RaftClient client;
        Fixture(Path root) throws Exception {
            nativeStore=outer.new Fixture(root.resolve("native")); client=new RatisSchemaAdministrationTest().client(nativeStore); var original=outer.directory(outer.cluster);
            schema=new DistributedSchema(outer.cluster,outer.database,original.domainId(),1,0,List.of(new DistributedSchema.Column(7,outer.data,"existing",shape(),null)));
            directory=new RangeDirectory(outer.cluster,outer.database,original.domainId(),1,schema.digest(),0,schema.columnIds(),0,original.ranges());
            var schemas=new RatisSchemaCatalogue(nativeStore.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
            assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(outer.operation(0),schema)).status());
            var directories=new RatisCatalogue(nativeStore.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
            assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(outer.operation(1),null,directory)).status());
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor range,UUID column,byte[] key) { throw new AssertionError("No DATA read in metadata fixture"); }
                public CommandResult command(RangeDescriptor range,DistributedCommand command) { throw new AssertionError("No DATA command in metadata fixture"); }
            });
            replicas=new RatisSchemaReplicaClient(1,(range,owner) -> { throw new IOException("Replica unavailable"); });
            catalogue=new RatisNodePlan.Catalogue(outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,original.ranges().getFirst().replicas());
        }
        RatisSchemaClient client(Path journals) throws IOException { return new RatisSchemaClient(journals,outer.cluster,outer.database,directory.domainId(),catalogue,client,routed,replicas,null); }
        public void close() throws Exception { replicas.close(); routed.close(); nativeStore.close(); }
    }
    @Test void existingCreateAndAbsentDeleteAreDurableAndDoNotAllocate() throws Exception {
        try (var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID existing,absent;
            try (var client=f.client(journals)) {
                long writes=f.nativeStore.next.get(); var create=client.prepareCreate("existing",shape()); existing=create.requestId(); assertEquals(7,client.completed(create).columnId()); assertFalse(client.completed(create).changed());
                var deletion=client.prepareDeleteIfExists("missing"); absent=deletion.requestId(); assertFalse(client.completed(deletion).changed()); assertEquals(writes,f.nativeStore.next.get());
                assertThrows(IOException.class,() -> f.client(journals));
            }
            try (var client=f.client(journals)) { assertEquals(7,client.completed(client.reopen(existing)).columnId()); assertFalse(client.completed(client.reopen(absent)).changed()); }
            assertEquals(0,f.nativeStore.closes.get());
        }
    }
    @Test void lostAllocationReplyReopenRetainsOriginalCommandUuidAndApiReservation() throws Exception {
        try (var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID request; byte[] frozen;
            try (var client=f.client(journals)) {
                var handle=client.prepareCreate("added",shape()); request=handle.requestId(); f.nativeStore.writeOverride=new byte[4];
                assertThrows(IOException.class,() -> client.advance(handle,1)); var fields=RatisSchemaClientJournal.read(journals.resolve(request+".request")); frozen=fields.get("pendingCommand");
                assertNotNull(frozen); var allocation=new RatisColumnAllocation(f.nativeStore.client,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn); assertEquals(8,allocation.read(request).apiId());
            } finally { f.nativeStore.writeOverride=null; }
            try (var client=f.client(journals)) {
                var handle=client.reopen(request); assertArrayEquals(frozen,RatisSchemaClientJournal.read(journals.resolve(request+".request")).get("pendingCommand"));
                assertEquals(RatisSchemaClient.Phase.SCHEMA,client.advance(handle,1).phase());
                var allocation=new RatisColumnAllocation(f.nativeStore.client,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn); assertEquals(8,allocation.read(request).apiId());
                var fields=RatisSchemaClientJournal.read(journals.resolve(request+".request")); assertEquals(UUID.fromString(new String(fields.get("column"),java.nio.charset.StandardCharsets.UTF_8)),allocation.read(request).columnId());
            }
        }
    }
    @Test void deleteNameHandleFreezesOriginalColumnIdentityAcrossReopen() throws Exception {
        try (var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID request;
            try (var client=f.client(journals)) { var handle=client.prepareDeleteIfExists("existing"); request=handle.requestId(); }
            var original=RatisSchemaClientJournal.read(journals.resolve(request+".request"));
            try (var client=f.client(journals)) {
                var handle=client.reopen(request); assertEquals(RatisSchemaClient.Phase.SCHEMA,handle.phase());
                assertArrayEquals(original.get("column"),RatisSchemaClientJournal.read(journals.resolve(request+".request")).get("column"));
                assertEquals(f.outer.data.toString(),new String(original.get("column"),java.nio.charset.StandardCharsets.UTF_8));
            }
        }
    }
    @Test void checksumValidCopiedRequestCannotResumeUnderAnotherHandleId() throws Exception {
        try(var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID left,right;
            try(var client=f.client(journals)) { left=client.prepareDeleteIfExists("missing").requestId(); right=client.prepareDeleteIfExists("existing").requestId(); }
            Files.copy(journals.resolve(right+".request"),journals.resolve(left+".request"),StandardCopyOption.REPLACE_EXISTING);
            try(var client=f.client(journals)) { long writes=f.nativeStore.next.get(); assertThrows(IOException.class,() -> client.reopen(left)); assertEquals(writes,f.nativeStore.next.get()); assertEquals(RatisSchemaClient.Phase.SCHEMA,client.reopen(right).phase()); }
        }
    }

    @Test void originalRequestIdDoesNotAuthorizeForeignPendingCommandsOrDefinitions() throws Exception {
        try(var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID left,right; byte[] foreignCommand,foreignDefinition;
            try(var client=f.client(journals)) {
                left=client.prepareCreate("left",shape()).requestId(); var other=client.prepareCreate("right",shape()); right=other.requestId(); f.nativeStore.writeOverride=new byte[4];
                assertThrows(IOException.class,() -> client.advance(other,1)); foreignCommand=RatisSchemaClientJournal.read(journals.resolve(right+".request")).get("pendingCommand"); f.nativeStore.writeOverride=null;
                client.advance(other,1); client.advance(other,1); client.advance(other,1); foreignDefinition=RatisSchemaClientJournal.read(journals.resolve(right+".request")).get("definition"); assertNotNull(foreignDefinition);
            } finally { f.nativeStore.writeOverride=null; }
            var original=RatisSchemaClientJournal.read(journals.resolve(left+".request")); var changed=new HashMap<>(original); changed.put("pendingCommand",foreignCommand); RatisSchemaClientJournal.write(journals.resolve(left+".request"),changed);
            try(var client=f.client(journals)) { long writes=f.nativeStore.next.get(); assertThrows(IOException.class,() -> client.reopen(left)); assertEquals(writes,f.nativeStore.next.get()); }
            changed=new HashMap<>(original); changed.put("definition",foreignDefinition); RatisSchemaClientJournal.write(journals.resolve(left+".request"),changed);
            try(var client=f.client(journals)) { long writes=f.nativeStore.next.get(); assertThrows(IOException.class,() -> client.reopen(left)); assertEquals(writes,f.nativeStore.next.get()); }
        }
    }

    @Test void terminalSchemaSourceConflictRetiresAllocationReceiptAndReopensUnknownAckExactly() throws Exception {
        try(var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("journals")); UUID request; byte[] original,ack;
            try(var client=f.client(journals)) {
                var h=client.prepareCreate("added",shape()); request=h.requestId(); client.advance(h,1);
                original=RatisSchemaClientJournal.read(journals.resolve(request+".request")).get("command/0");
                var next=new RangeDirectory(f.directory.clusterId(),f.directory.databaseId(),f.directory.domainId(),f.directory.schemaEpoch(),f.directory.schemaDigest(),1,f.directory.columns(),f.directory.routingPrefixBytes(),f.directory.ranges());
                var directories=new RatisCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
                assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(f.outer.operation(2),f.directory,next)).status());
                assertEquals(RatisSchemaClient.Phase.CONFLICT,client.advance(h,1).phase()); ack=RatisSchemaClientJournal.read(journals.resolve(request+".request")).get("pendingAck"); assertNotNull(ack);
                f.nativeStore.writeOverride=new byte[4]; assertThrows(IOException.class,() -> client.advance(h,1));
            } finally { f.nativeStore.writeOverride=null; }
            try(var client=f.client(journals)) {
                var h=client.reopen(request); assertEquals(request,h.requestId()); assertArrayEquals(ack,RatisSchemaClientJournal.read(journals.resolve(request+".request")).get("pendingAck"));
                assertEquals(RatisSchemaClient.Phase.CONFLICT,client.advance(h,1).phase());
                var allocation=new RatisColumnAllocation(f.client,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
                assertEquals(CommandResult.Status.SESSION_EXPIRED,allocation.execute(DistributedCommandCodec.decode(original)).status()); assertEquals(8,allocation.read(request).apiId());
            }
        }
    }
    @Test void terminalDefinitionConflictAcknowledgesAllConsumedPreparationResults() throws Exception {
        try(var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("journals")); var inserted=new java.util.concurrent.atomic.AtomicBoolean(); UUID request; List<byte[]> originals;
            try(var client=new RatisSchemaClient(journals,f.outer.cluster,f.outer.database,f.directory.domainId(),f.catalogue,f.client,f.routed,f.replicas,null,(file,fields) -> {
                if(new String(fields.get("phase"),java.nio.charset.StandardCharsets.UTF_8).equals("DEFINITION") && fields.containsKey("pendingCommand") && inserted.compareAndSet(false,true)) {
                    var definition=SchemaChange.Definition.decode(fields.get("definition")); var admin=new RatisSchemaAdministration(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
                    var competing=admin.prepareDefinition(new OperationId(UUID.randomUUID(),0,0),definition,null,admin.readColumnIdentity(definition.header().changedColumnId()),admin.readApiIdentity(definition.header().changedApiId()));
                    assertEquals(CommandResult.Status.SUCCESS,admin.execute(competing).status());
                }
                RatisSchemaClientJournal.write(file,fields);
            })) {
                var h=client.prepareCreate("added",shape()); request=h.requestId(); client.advance(h,1); client.advance(h,1); assertEquals(RatisSchemaClient.Phase.CONFLICT,client.advance(h,1).phase());
                var fields=RatisSchemaClientJournal.read(journals.resolve(request+".request")); originals=List.of(fields.get("command/0"),fields.get("command/1"),fields.get("command/2")); assertNotNull(fields.get("pendingAck")); client.advance(h,1);
                for(var bytes:originals) {
                    var reply=f.client.io().send(org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(bytes)));
                    assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(reply.getMessage().getContent().toByteArray()).status());
                }
                assertEquals(request,h.requestId());
            }
        }
    }

}
