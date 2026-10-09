package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real native catalogue CAS; DATA fence/install positions are supplied by a controlled fixture. */
class RatisSchemaAdministrationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSessionEpochsTest outer=new RatisSessionEpochsTest();
    final AtomicLong sequence=new AtomicLong();
    OperationId operation() { return outer.operation(sequence.getAndIncrement()); }
    DistributedSchema.Column column(long id,UUID uuid,String name) { return new DistributedSchema.Column(id,uuid,name,
            ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(),true),null); }
    DistributedSchema initial() { return new DistributedSchema(outer.cluster,outer.database,new UUID(6,1),1,0,List.of(column(7,outer.data,"data"))); }
    DistributedSchema next(DistributedSchema previous,List<DistributedSchema.Column> columns) { return new DistributedSchema(previous.clusterId(),previous.databaseId(),previous.domainId(),previous.schemaEpoch()+1,0,columns); }
    RangeDirectory directory(DistributedSchema schema,long generation) { return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),generation,schema.columnIds(),0,outer.directory(outer.cluster).ranges()); }
    RaftClient client(RatisSessionEpochsTest.Fixture f) {
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
            if (!method.getName().equals("sendReadOnly")) return method.invoke(f.client.io(),args);
            var packet=ScopedTransactionRpc.parse(((Message)args[0]).getContent().toByteArray(),ScopedTransactionRpc.MAX_REQUEST_BYTES);
            assertEquals(ScopedTransactionRpc.POINT,packet.kind()); var in=ByteBuffer.wrap(packet.body());
            var column=ScopedTransactionRpc.uuid(in); byte[] key=ScopedTransactionRpc.blob(in,2*1024*1024,false); assertFalse(in.hasRemaining());
            byte[] value; try (var pin=f.store.capture()) { value=pin.get(column,key); }
            byte[] reply=value==null ? ByteBuffer.allocate(4).putInt(-1).array() : ByteBuffer.allocate(4+value.length).putInt(value.length).put(value).array();
            return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("fixture"))
                    .setGroupId(RaftGroupId.valueOf(outer.catalogue)).setCallId(1).setSuccess(true)
                    .setMessage(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,reply)))).build();
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch (method.getName()) {
            case "getGroupId" -> f.client.getGroupId(); case "io" -> io; case "close" -> throw new AssertionError("Borrowed client cannot close");
            default -> throw new AssertionError("Unexpected client operation "+method);
        });
    }
    RatisCatalogue catalogue(RaftClient client) { return new RatisCatalogue(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn); }
    RatisSchemaAdministration admin(RaftClient client) { return new RatisSchemaAdministration(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn); }
    void publishSchema(RatisSessionEpochsTest.Fixture f,DistributedSchema target) throws Exception {
        var schemas=new RatisSchemaCatalogue(f.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
        var identities=admin(client(f)); var columns=new HashMap<UUID,byte[]>(); var ids=new HashMap<Long,byte[]>();
        for (var column:target.columns()) {
            byte[] byColumn=identities.readColumnIdentity(column.columnId()),byId=identities.readApiIdentity(column.apiId());
            if (byColumn!=null) columns.put(column.columnId(),byColumn); if (byId!=null) ids.put(column.apiId(),byId);
        }
        assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(operation(),target,columns,ids)).status());
    }
    RatisSchemaAdministration.Head begin(RatisSchemaAdministration admin,SchemaChange.Definition definition,RatisSchemaAdministration.Head previous) throws Exception {
        byte[] oldColumn=admin.readColumnIdentity(definition.header().changedColumnId()),oldApi=admin.readApiIdentity(definition.header().changedApiId());
        var publish=admin.prepareDefinition(operation(),definition,previous,oldColumn,oldApi);
        assertEquals(CommandResult.Status.SUCCESS,admin.execute(publish).status());
        assertEquals(CommandResult.Status.SUCCESS,admin.execute(admin.prepareBegin(definition,previous,oldColumn,oldApi)).status());
        return admin.readHead(definition.sourceSchema().domainId());
    }
    RatisSchemaAdministration.Head complete(RatisSchemaAdministration admin,SchemaChange.Definition definition,RatisSchemaAdministration.Head head) throws Exception {
        var group=definition.sourceDirectory().ranges().getFirst().groupId();
        admin.execute(admin.prepareAdvance(operation(),head,head.status().fenced(group,new ApplyPosition(1,10)))); head=admin.readHead(head.status().header().domainId());
        admin.execute(admin.prepareAdvance(operation(),head,head.status().installed(group,new ApplyPosition(1,11)))); head=admin.readHead(head.status().header().domainId());
        admin.execute(admin.preparePublication(operation(),head,definition)); head=admin.readHead(head.status().header().domainId());
        admin.execute(admin.prepareAdvance(operation(),head,head.status().retire())); return admin.readHead(head.status().header().domainId());
    }

    @Test void activeAdministrationBlocksTopologyAndPublicationRetainsExactRestartOutcome() throws Exception {
        try (var f=outer.new Fixture(root)) {
            var client=client(f); var catalogue=catalogue(client); var admin=admin(client); var source=initial(); var original=directory(source,0);
            publishSchema(f,source);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(operation(),null,original)).status());
            var target=next(source,List.of(source.column(7),column(8,new UUID(9,1),"added")));
            var definition=new SchemaChange.Definition(new UUID(9,2),operation(),original,source,target);
            publishSchema(f,target);
            var head=begin(admin,definition,null);
            var topology=catalogue.prepare(operation(),original,directory(source,1));
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(topology).status());
            assertThrows(IllegalStateException.class,() -> admin.preparePublication(operation(),head,definition));
            var closed=complete(admin,definition,head); assertTrue(closed.terminal());
            assertArrayEquals(RangeDirectoryCodec.encode(definition.targetDirectory()),RangeDirectoryCodec.encode(catalogue.read(source.domainId())));
            var restored=admin(client); assertArrayEquals(closed.status().encode(),restored.readOutcome(source.domainId(),definition.digest()).status().encode());
            assertEquals(8,restored.readHead(source.domainId()).allocatedThrough()); assertArrayEquals(definition.encode(),restored.readDefinition(definition.digest()).encode());
            var published=definition.targetDirectory(); var later=directory(target,published.generation()+1);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareAfterAdministration(operation(),published,later,closed)).status());
            assertEquals(0,f.closes.get());
        }
    }
    @Test void deletedColumnIncarnationAndApiIdCannotBeReissuedAndEmptyDomainRemainsRoutableMetadata() throws Exception {
        try (var f=outer.new Fixture(root)) {
            var client=client(f); var catalogue=catalogue(client); var admin=admin(client); var source=initial(); var original=directory(source,0);
            publishSchema(f,source);
            catalogue.execute(catalogue.prepare(operation(),null,original));
            var empty=next(source,List.of()); var deletion=new SchemaChange.Definition(new UUID(10,1),operation(),original,source,empty);
            publishSchema(f,empty);
            var closed=complete(admin,deletion,begin(admin,deletion,null));
            assertTrue(catalogue.read(source.domainId()).columns().isEmpty()); assertFalse(catalogue.read(source.domainId()).ranges().isEmpty());
            var reusedApi=next(empty,List.of(column(7,new UUID(10,2),"data")));
            var reuse=new SchemaChange.Definition(new UUID(10,3),operation(),deletion.targetDirectory(),empty,reusedApi);
            assertThrows(IllegalArgumentException.class,() -> admin.prepareDefinition(operation(),reuse,closed,null,admin.readApiIdentity(7)));
            var reusedUuid=next(empty,List.of(column(8,outer.data,"data")));
            var sameIncarnation=new SchemaChange.Definition(new UUID(10,4),operation(),deletion.targetDirectory(),empty,reusedUuid);
            assertThrows(IllegalArgumentException.class,() -> admin.prepareDefinition(operation(),sameIncarnation,closed,admin.readColumnIdentity(outer.data),null));
            var created=next(empty,List.of(column(8,new UUID(10,5),"data")));
            var fresh=new SchemaChange.Definition(new UUID(10,6),operation(),deletion.targetDirectory(),empty,created);
            publishSchema(f,created);
            complete(admin,fresh,begin(admin,fresh,closed));
            assertEquals(8,admin.readHead(source.domainId()).allocatedThrough());
            assertEquals(SchemaChange.Phase.RETIRED,admin.readOutcome(source.domainId(),deletion.digest()).status().phase());
        }
    }
    @Test void losingBeginsAndLostAppliedRepliesResolveOriginalFrozenCommands() throws Exception {
        try (var f=outer.new Fixture(root)) {
            var client=client(f); var catalogue=catalogue(client); var admin=admin(client); var source=initial(); var original=directory(source,0);
            publishSchema(f,source);
            catalogue.execute(catalogue.prepare(operation(),null,original));
            var first=new SchemaChange.Definition(new UUID(11,1),operation(),original,source,next(source,List.of(source.column(7),column(8,new UUID(11,2),"first"))));
            var other=new SchemaChange.Definition(new UUID(11,3),operation(),original,source,next(source,List.of(source.column(7),column(9,new UUID(11,4),"other"))));
            publishSchema(f,first.targetSchema()); publishSchema(f,other.targetSchema());
            admin.execute(admin.prepareDefinition(operation(),first,null,null,null)); admin.execute(admin.prepareDefinition(operation(),other,null,null,null));
            var frozen=admin.prepareBegin(first,null,null,null); byte[] bytes=DistributedCommandCodec.encode(frozen);
            f.writeOverride=new byte[4]; assertThrows(IOException.class,() -> admin.execute(frozen)); f.writeOverride=null;
            assertEquals(CommandResult.Status.SUCCESS,admin.execute(frozen).status()); assertArrayEquals(bytes,DistributedCommandCodec.encode(frozen));
            var losing=admin.prepareBegin(other,null,null,null);
            assertEquals(CommandResult.Status.CONFLICT,admin.execute(losing).status()); assertEquals(CommandResult.Status.CONFLICT,admin.execute(losing).status());
            var head=admin.readHead(source.domainId());
            admin.execute(admin.prepareAdvance(operation(),head,head.status().abort()));
            assertEquals(SchemaChange.Phase.ABORTED,admin.readOutcome(source.domainId(),first.digest()).status().phase());
            assertEquals(CommandResult.Status.SUCCESS,admin.execute(frozen).status());
            assertEquals(SchemaChange.Phase.ABORTED,admin.readHead(source.domainId()).status().phase());
        }
    }
    @Test void largestAcceptedDirectoriesRemainReplaceableWithRetainedAdministrativeHead() throws Exception {
        try (var f=outer.new Fixture(root)) {
            var source=initial(); var target=next(source,List.of(source.column(7),column(8,new UUID(12,1),"added")));
            var definition=new SchemaChange.Definition(new UUID(12,2),operation(),directory(source,0),source,target);
            var status=SchemaChange.Status.begin(definition.header()).fenced(outer.dataGroup,new ApplyPosition(1,1))
                    .installed(outer.dataGroup,new ApplyPosition(1,2)).publish().retire();
            var head=new RatisSchemaAdministration.Head(status,8); var catalogue=catalogue(client(f));
            int low=1,high=RangeDirectoryCodec.MAX_ENCODED_BYTES/4;
            while (low<high) {
                int middle=(low+high+1)/2;
                if (RangeDirectoryCodec.encodedSize(largeDirectory(target,2,middle))<=RatisCatalogue.MAX_DIRECTORY_BYTES) low=middle;
                else high=middle-1;
            }
            var before=largeDirectory(target,2,low); var after=largeDirectory(target,3,low);
            assertTrue(RatisCatalogue.MAX_DIRECTORY_BYTES-RangeDirectoryCodec.encodedSize(before)<4);
            var command=catalogue.prepareAfterAdministration(operation(),before,after,head);
            assertTrue(DistributedCommandCodec.encode(command).length<=DistributedCommandCodec.MAX_ENCODED_BYTES);
            var tooLarge=largeDirectory(target,3,low+1);
            assertThrows(IllegalArgumentException.class,() -> catalogue.prepareAfterAdministration(operation(),before,tooLarge,head));
        }
    }
    private RangeDirectory largeDirectory(DistributedSchema schema,long generation,int width) {
        byte[] a=new byte[width],b=new byte[width]; a[0]=1; b[0]=2;
        var replicas=outer.directory(outer.cluster).ranges().getFirst().replicas();
        var ranges=List.of(new RangeDescriptor(new UUID(13,1),new UUID(14,1),1,1,null,a,RangeDescriptor.TopologyState.ACTIVE,replicas),
                new RangeDescriptor(new UUID(13,2),new UUID(14,2),1,1,a,b,RangeDescriptor.TopologyState.ACTIVE,replicas),
                new RangeDescriptor(new UUID(13,3),new UUID(14,3),1,1,b,null,RangeDescriptor.TopologyState.ACTIVE,replicas));
        return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),generation,schema.columnIds(),0,ranges);
    }
}
