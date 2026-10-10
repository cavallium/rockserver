package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisSchemaLifecycleClientTest {
    final RatisSchemaFenceClientTest helper=new RatisSchemaFenceClientTest();
    class Transport implements RatisRoutedClient.Transport {
        final SchemaChange.Definition definition=helper.definition();
        final NativeSchemaInstallProof install=new NativeSchemaInstallProof(definition.header(),helper.group,new ApplyPosition(1,5),new ApplyPosition(1,6));
        final NativeSchemaActivationProof activation=new NativeSchemaActivationProof(install,new UUID(8,1),new ApplyPosition(1,20),new ApplyPosition(1,7));
        int writes; boolean wrongCut,wrongAdmission; final List<DistributedCommand> commands=new ArrayList<>();
        public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { return new byte[]{7}; }
        public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { commands.add(command); return new CommandResult(CommandResult.Status.SUCCESS,List.of()); }
        public byte[] transaction(RangeDescriptor descriptor,byte[] bytes,boolean fresh) {
            assertEquals(definition.sourceDirectory().ranges().getFirst(),descriptor); assertTrue(fresh);
            int kind=ScopedTransactionRpc.kind(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);
            if (ScopedTransactionRpc.schemaInstall(kind)) {
                var record=RatisSchemaInstallRpc.request(bytes);
                if (kind==ScopedTransactionRpc.SCHEMA_INSTALL_STATE) return RatisSchemaInstallRpc.reply(kind,new RatisSchemaInstallRpc.State(record,install,true));
                return RatisSchemaInstallRpc.preparedReply(kind,record);
            }
            var request=RatisSchemaActivateRpc.request(bytes);
            if (kind==ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE) return RatisSchemaActivateRpc.reply(kind,new RatisSchemaActivateRpc.State(request,activation,true));
            var record=kind==ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL ? null : new RatisSchemaActivateRecord(request.header(),request.groupId(),request.expectedInstallPosition(),activation.catalogueGroup(),activation.cataloguePosition());
            return RatisSchemaActivateRpc.preparedReply(kind,request,record);
        }
        public byte[] participant(RangeDescriptor descriptor,byte[] bytes) {
            assertEquals(definition.sourceDirectory().ranges().getFirst(),descriptor); writes++;
            if (RatisSchemaInstallRecord.isInstall(bytes)) { var record=RatisSchemaInstallRecord.decode(bytes); assertEquals("added",record.changedPhysicalName()); return (wrongAdmission ? changedInstallation() : install).encode(); }
            var record=RatisSchemaActivateRecord.decode(bytes); assertEquals(activation.cataloguePosition(),record.cataloguePosition());
            if (wrongAdmission) return new NativeSchemaActivationProof(changedInstallation(),activation.catalogueGroup(),activation.cataloguePosition(),activation.position()).encode();
            return wrongCut ? new NativeSchemaActivationProof(install,activation.catalogueGroup(),new ApplyPosition(1,21),activation.position()).encode() : activation.encode();
        }
        NativeSchemaInstallProof changedInstallation() { return new NativeSchemaInstallProof(new NativeSchemaFence(definition.header(),helper.group,install.fence().position(),new byte[32]),definition.targetSchema().digest(),install.position()); }
    }

    @Test void qualifiedInstallActivationAndSchemaRefreshKeepOriginalOwnedCommands() throws Exception {
        var wire=new Transport(); var definition=wire.definition;
        try (var client=new RatisRoutedClient(definition.sourceDirectory(),wire)) {
            var original=client.prepare(new OperationId(new UUID(9,1),1,0),List.of(new Mutation(Mutation.Kind.PUT,helper.column,new byte[8],new byte[]{1})));
            var schema=client.schemaTarget(definition,helper.group);
            var target=client.schemaInstallTarget(schema,wire.install.fence(),"added",false);
            assertThrows(IllegalArgumentException.class,() -> client.refresh(definition.targetDirectory()));
            assertEquals(wire.install,client.installSchema(target)); assertTrue(client.schemaInstallationState(target).ready()); client.cancelSchemaInstallation(target);
            var activation=client.schemaActivationTarget(schema,wire.install);
            assertEquals(wire.activation,client.activateSchema(activation)); assertTrue(client.schemaActivationState(activation).ready()); client.cancelSchemaActivation(activation);
            client.refreshSchema(definition,wire.activation); assertArrayEquals(definition.targetDirectory().payloadDigest(),client.directory().payloadDigest());
            client.execute(original); assertSame(original.command(),wire.commands.getFirst());
            var fresh=client.prepare(new OperationId(new UUID(9,1),1,1),List.of(new Mutation(Mutation.Kind.PUT,helper.column,new byte[8],new byte[]{2})));
            assertArrayEquals(definition.targetSchema().digest(),fresh.command().schemaDigest()); client.execute(fresh);
            assertEquals(wire.install,client.installSchema(target)); assertEquals(wire.activation,client.activateSchema(activation));
            client.refreshSchema(definition,wire.activation); assertArrayEquals(definition.targetDirectory().payloadDigest(),client.directory().payloadDigest());
        }
    }

    @Test void activationReplyCannotChangeFrozenCatalogueQualification() throws Exception {
        var wire=new Transport(); wire.wrongCut=true;
        try (var client=new RatisRoutedClient(wire.definition.sourceDirectory(),wire)) {
            var target=client.schemaActivationTarget(client.schemaTarget(wire.definition,helper.group),wire.install);
            assertThrows(IOException.class,() -> client.activateSchema(target));
            assertEquals(1,wire.writes); assertArrayEquals(wire.definition.sourceDirectory().payloadDigest(),client.directory().payloadDigest());
        }
    }
    @Test void installationAndActivationRepliesRetainTheOriginalAdmission() throws Exception {
        var wire=new Transport(); wire.wrongAdmission=true;
        try (var client=new RatisRoutedClient(wire.definition.sourceDirectory(),wire)) {
            var source=client.schemaTarget(wire.definition,helper.group);
            var install=client.schemaInstallTarget(source,wire.install.fence(),"added",false);
            assertThrows(IOException.class,() -> client.installSchema(install));
            var activate=client.schemaActivationTarget(source,wire.install);
            assertThrows(IOException.class,() -> client.activateSchema(activate));
            assertArrayEquals(wire.definition.sourceDirectory().payloadDigest(),client.directory().payloadDigest());
        }
    }

    @Test void qualifiedHistoricalSchemaReopenRetainsOriginalUnallocatedRequestRoute() throws Exception {
        var wire=new Transport(); var definition=wire.definition;
        try (var client=new RatisRoutedClient(definition.targetDirectory(),wire)) {
            assertThrows(IllegalArgumentException.class,() -> client.requireKnownSchema(definition.sourceSchema()));
            client.refreshSchema(definition,wire.activation);
            assertArrayEquals(definition.targetDirectory().payloadDigest(),client.directory().payloadDigest());
            assertArrayEquals(definition.sourceDirectory().payloadDigest(),client.directory(definition.sourceSchema()).payloadDigest());
            var old=client.prepareAt(client.directory(definition.sourceSchema()),new OperationId(new UUID(91,1),1,0),List.of(new Mutation(Mutation.Kind.PUT,helper.column,new byte[8],new byte[]{1})),List.of(),-1);
            assertArrayEquals(definition.sourceSchema().digest(),old.command().schemaDigest());
            assertEquals(definition.sourceDirectory().ranges().getFirst(),old.descriptor());
            var forged=new RangeDirectory(definition.sourceDirectory().clusterId(),definition.sourceDirectory().databaseId(),definition.sourceDirectory().domainId(),definition.sourceDirectory().schemaEpoch(),definition.sourceDirectory().schemaDigest(),99,definition.sourceDirectory().columns(),definition.sourceDirectory().routingPrefixBytes(),definition.sourceDirectory().ranges());
            assertThrows(IllegalArgumentException.class,() -> client.prepareAt(forged,new OperationId(new UUID(91,1),1,1),old.command().mutations(),List.of(),-1));
        }
    }
    @Test void schemaViewBudgetIncludesBootstrapAndAllSixtyFourNativeTransitions() throws Exception {
        var wire=new Transport(); var source=wire.definition.sourceSchema(); long nextApi=2;
        try (var client=new RatisRoutedClient(wire.definition.sourceDirectory(),wire)) {
            for (int i=1;i<=65;i++) {
                var columns=new ArrayList<>(source.columns());
                if ((i&1)==1) columns.add(helper.column(nextApi++,new UUID(10,i),"added"+i)); else columns.removeLast();
                var target=helper.schema(source.schemaEpoch()+1,columns);
                var change=new SchemaChange.Definition(new UUID(20,i),new OperationId(new UUID(20,100),1,i),client.directory(),source,target);
                var install=new NativeSchemaInstallProof(change.header(),helper.group,new ApplyPosition(1,3L*i),new ApplyPosition(1,3L*i+1));
                var activated=new NativeSchemaActivationProof(install,new UUID(8,1),new ApplyPosition(1,20L+i),new ApplyPosition(1,3L*i+2));
                if (i<=64) { client.refreshSchema(change,activated); source=target; }
                else assertThrows(IllegalStateException.class,() -> client.refreshSchema(change,activated));
            }
            assertEquals(65,client.directory().schemaEpoch());
        }
    }
}
