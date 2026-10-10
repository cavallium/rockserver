package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native source/catalogue with trusted local qualification; no server, sockets or quorum. */
class RatisCdcDeliveryAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final UUID RANGE=new UUID(9,600);
    static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    TransactionContext propose(RatisImportPublicationAdmissionTest.Fixture f,DistributedCommand command) throws IOException {
        return f.machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(f.peer.getId()).setGroupId(f.group.getGroupId()).setCallId(1)
                .setType(RaftClientRequest.writeRequestType()).setMessage(message(DistributedCommandCodec.encode(command))).build());
    }
    @Test void qualifiedDeliveryPersistsBeforeAckAndExactRetriesNeedNoNewSourceRead() throws Exception {
        var authority=new AtomicReference<RatisCdcDeliveryAuthority>(); var checks=new AtomicInteger(); var unavailable=new AtomicBoolean();
        var helper=new RatisImportPublicationAdmissionTest(); helper.root=root.resolve("catalogue");
        try(var f=helper.new Fixture(null,machine->machine.cdcDeliveryAdmission(command->{
            if(unavailable.get()) throw new IOException("Source unavailable"); checks.incrementAndGet();
            authority.get().validate(command);
        })); var data=new RatisRetentionAdmissionTest.Fixture(root.resolve("source"),RatisRangeStateMachine.RetentionLimits.defaults(),command->{},System::nanoTime,null,
                machine->machine.cdcBinding(f.catalogue.outer.cluster,f.catalogue.outer.database,RANGE))) {
            data.apply(data.propose(data.put(0)),1);
            var owner=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"fixture","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
            var range=new RangeDescriptor(RANGE,data.groupId,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
            var directory=new RangeDirectory(f.catalogue.outer.cluster,f.catalogue.outer.database,data.groupId,1,new byte[32],1,Set.of(data.column),0,List.of(range));
            try(var client=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError(); }
                public byte[] transaction(RangeDescriptor descriptor,byte[] request,boolean fresh) throws IOException {
                    assertTrue(fresh); assertEquals(data.groupId,descriptor.groupId());
                    try { return data.machine.queryLinearizable(message(request)).get(5,TimeUnit.SECONDS).getContent().toByteArray(); }
                    catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
                    catch(ExecutionException | TimeoutException failure) { throw new IOException(failure); }
                }
            })) {
            authority.set(new RatisCdcDeliveryAuthority(client,domain->directory));
            byte[] key=ByteBuffer.allocate(16).putLong(directory.domainId().getMostSignificantBits()).putLong(directory.domainId().getLeastSignificantBits()).array();
            var seed=new DistributedCommand(f.catalogue.outer.catalogue,1,new byte[32],new OperationId(new UUID(9,601),1,0),List.of(new Mutation(Mutation.Kind.PUT,f.catalogue.outer.directoryColumn,key,RangeDirectoryCodec.encode(directory))));
            f.catalogue.nativeCatalogue.store.apply(new ApplyPosition(1,f.catalogue.nativeCatalogue.next.getAndIncrement()),seed);
            var catalogue=new RatisCdcSubscriptionCatalogue(f.catalogue.client,directory.clusterId(),directory.databaseId(),f.catalogue.outer.catalogue,1,new byte[32],f.catalogue.outer.directoryColumn);
            var lineage=new DistributedCdcCheckpoint.Lineage(RANGE,data.groupId,1);
            var definition=DistributedCdcSubscription.Definition.create(new UUID(9,602),new UUID(9,603),directory,directory.columns(),new DistributedCdcCheckpoint(Map.of(lineage,DistributedCdcCheckpoint.Cursor.beginning())));
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareCreate(f.catalogue.operation(700),definition,directory,null)).status());
            f.machine.notifyTermIndexUpdated(1,f.catalogue.nativeCatalogue.store.applied().index());
            var initial=catalogue.read(definition.subscriptionId()); var delivered=initial.delivered().advance(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1)));
            assertThrows(IllegalArgumentException.class,()->catalogue.prepareAcknowledgement(f.catalogue.operation(701),initial,delivered));
            var delivery=catalogue.prepareDelivery(f.catalogue.operation(702),initial,delivered); assertNotNull(propose(f,delivery).getException());
            byte[] prepare=RatisCdcDeliveryRpc.request(RatisCdcDeliveryRpc.PREPARE,delivery);
            assertThrows(ExecutionException.class,()->f.machine.query(message(prepare)).get(5,TimeUnit.SECONDS));
            var echo=f.machine.queryLinearizable(message(prepare)).get(5,TimeUnit.SECONDS).getContent().toByteArray(); RatisCdcDeliveryRpc.requireEcho(echo,RatisCdcDeliveryRpc.PREPARE,delivery);
            f.machine.queryLinearizable(message(prepare)).get(5,TimeUnit.SECONDS); assertEquals(1,checks.get());
            var proposal=propose(f,delivery); assertNull(proposal.getException()); f.apply(proposal);
            var issued=catalogue.read(definition.subscriptionId()); assertEquals(delivered,issued.delivered()); assertEquals(initial.checkpoint(),issued.checkpoint());
            var ack=catalogue.prepareAcknowledgement(f.catalogue.operation(703),issued,delivered); assertFalse(RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(ack));
            var acknowledgement=propose(f,ack); assertNull(acknowledgement.getException()); f.apply(acknowledgement);
            var confirmed=catalogue.read(definition.subscriptionId()); assertEquals(delivered,confirmed.checkpoint()); assertEquals(1,catalogue.readCapacity().count());
            unavailable.set(true); f.apply(propose(f,delivery)); f.apply(propose(f,ack));
            RatisCdcDeliveryRpc.requireEcho(f.machine.queryLinearizable(message(prepare)).get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisCdcDeliveryRpc.PREPARE,delivery);
            assertEquals(1,checks.get()); assertEquals(confirmed,catalogue.read(definition.subscriptionId())); assertEquals(0,f.machine.importPublicationStats().holds());
            var ahead=delivered.advance(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2)));
            var fresh=catalogue.prepareDelivery(f.catalogue.operation(704),confirmed,ahead);
            assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(message(RatisCdcDeliveryRpc.request(RatisCdcDeliveryRpc.PREPARE,fresh))).get(5,TimeUnit.SECONDS));
            assertEquals(0,f.machine.importPublicationStats().holds()); assertEquals(confirmed,catalogue.read(definition.subscriptionId()));
            unavailable.set(false);
            for(int i=1;i<4096;i++) data.apply(data.propose(data.put(i)),i+1L);
            data.apply(data.propose(data.ack(10000,4095)),4097);
            data.apply(data.propose(data.put(4096)),4098); data.apply(data.propose(data.put(4097)),4099);
            var afterGap=confirmed.delivered().advance(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,4099)));
            var gapClaim=catalogue.prepareDelivery(f.catalogue.operation(705),confirmed,afterGap);
            assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(message(RatisCdcDeliveryRpc.request(RatisCdcDeliveryRpc.PREPARE,gapClaim))).get(5,TimeUnit.SECONDS));
            assertEquals(0,f.machine.importPublicationStats().holds()); assertEquals(confirmed,catalogue.read(definition.subscriptionId()));
            }
        }
    }
    @Test void frozenDeliveryAfterNoEffectTailRemainsAdmissibleWhenSourceAdvancesToNewTerm() throws Exception {
        var authority=new AtomicReference<RatisCdcDeliveryAuthority>(); var checks=new AtomicInteger();
        var helper=new RatisImportPublicationAdmissionTest(); helper.root=root.resolve("catalogue");
        try(var f=helper.new Fixture(null,machine->machine.cdcDeliveryAdmission(command->{ checks.incrementAndGet(); authority.get().validate(command); }));
                var data=new RatisRetentionAdmissionTest.Fixture(root.resolve("source"),RatisRangeStateMachine.RetentionLimits.defaults(),command->{},System::nanoTime,null,
                        machine->machine.cdcBinding(f.catalogue.outer.cluster,f.catalogue.outer.database,RANGE))) {
            assertEquals(CommandResult.Status.SUCCESS,data.apply(data.propose(data.put(0)),1));
            assertEquals(CommandResult.Status.SUCCESS,data.apply(data.propose(data.ack(1,0)),2));
            var owner=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"fixture","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
            var range=new RangeDescriptor(RANGE,data.groupId,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
            var directory=new RangeDirectory(f.catalogue.outer.cluster,f.catalogue.outer.database,data.groupId,1,new byte[32],1,Set.of(data.column),0,List.of(range));
            try(var client=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError(); }
                public byte[] transaction(RangeDescriptor descriptor,byte[] request,boolean fresh) throws IOException {
                    assertTrue(fresh); assertEquals(data.groupId,descriptor.groupId());
                    try { return data.machine.queryLinearizable(message(request)).get(5,TimeUnit.SECONDS).getContent().toByteArray(); }
                    catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
                    catch(ExecutionException | TimeoutException failure) { throw new IOException(failure); }
                }
            })) {
                authority.set(new RatisCdcDeliveryAuthority(client,domain->directory));
                byte[] key=ByteBuffer.allocate(16).putLong(directory.domainId().getMostSignificantBits()).putLong(directory.domainId().getLeastSignificantBits()).array();
                var seed=new DistributedCommand(f.catalogue.outer.catalogue,1,new byte[32],new OperationId(new UUID(9,801),1,0),List.of(new Mutation(Mutation.Kind.PUT,f.catalogue.outer.directoryColumn,key,RangeDirectoryCodec.encode(directory))));
                f.catalogue.nativeCatalogue.store.apply(new ApplyPosition(1,f.catalogue.nativeCatalogue.next.getAndIncrement()),seed);
                var catalogue=new RatisCdcSubscriptionCatalogue(f.catalogue.client,directory.clusterId(),directory.databaseId(),f.catalogue.outer.catalogue,1,new byte[32],f.catalogue.outer.directoryColumn);
                var lineage=new DistributedCdcCheckpoint.Lineage(RANGE,data.groupId,1);
                var definition=DistributedCdcSubscription.Definition.create(new UUID(9,802),new UUID(9,803),directory,directory.columns(),new DistributedCdcCheckpoint(Map.of(lineage,DistributedCdcCheckpoint.Cursor.beginning())));
                assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareCreate(f.catalogue.operation(800),definition,directory,null)).status());
                f.machine.notifyTermIndexUpdated(1,f.catalogue.nativeCatalogue.store.applied().index());
                var initial=catalogue.read(definition.subscriptionId());
                var page=client.pollCdc(lineage,initial.checkpoint().cursors().get(lineage),16,4096);
                assertEquals(new ApplyPosition(1,2),page.cut()); assertEquals(1,page.records().size());
                assertEquals(DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1)),page.next(),"No-effect EOF must retain the last durable event cursor");
                var delivered=initial.delivered().advance(lineage,page.next()); var delivery=catalogue.prepareDelivery(f.catalogue.operation(801),initial,delivered); byte[] original=DistributedCommandCodec.encode(delivery);

                data.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); data.machine.notifyTermIndexUpdated(2,3);
                assertEquals(CommandResult.Status.SUCCESS,data.apply(data.propose(data.put(2)),4)); assertEquals(new ApplyPosition(2,4),data.store.applied());
                byte[] prepare=RatisCdcDeliveryRpc.request(RatisCdcDeliveryRpc.PREPARE,delivery);
                var echo=f.machine.queryLinearizable(message(prepare)).get(5,TimeUnit.SECONDS).getContent().toByteArray(); RatisCdcDeliveryRpc.requireEcho(echo,RatisCdcDeliveryRpc.PREPARE,delivery);
                assertEquals(1,checks.get()); assertArrayEquals(original,DistributedCommandCodec.encode(delivery));
                var proposal=propose(f,delivery); assertNull(proposal.getException()); f.apply(proposal);
                var issued=catalogue.read(definition.subscriptionId()); assertEquals(delivered,issued.delivered()); assertEquals(initial.checkpoint(),issued.checkpoint());
                var ack=catalogue.prepareAcknowledgement(f.catalogue.operation(802),issued,delivered); var acknowledgement=propose(f,ack); assertNull(acknowledgement.getException()); f.apply(acknowledgement);
                var confirmed=catalogue.read(definition.subscriptionId()); assertEquals(page.next(),confirmed.checkpoint().cursors().get(lineage));
                var following=client.pollCdc(lineage,confirmed.checkpoint().cursors().get(lineage),16,4096);
                assertEquals(1,following.records().size()); assertEquals(new ApplyPosition(2,4),following.records().getFirst().position());
                assertEquals(0,f.machine.importPublicationStats().holds());
            }
        }
    }
    @Test void conditionalDataKeysCannotEnterTheCatalogueDeliveryClassifier() throws Exception {
        for(int length:new int[]{17,1}) try(var data=new RatisRetentionAdmissionTest.Fixture(root.resolve("ordinary-"+length),RatisRangeStateMachine.RetentionLimits.defaults())) {
            byte[] key=new byte[length]; key[0]=(byte)(length==17 ? 0x80 : 0x81);
            var initial=new DistributedCommand(data.groupId,1,new byte[32],new OperationId(data.session,1,0),List.of(new Mutation(Mutation.Kind.PUT,data.column,key,new byte[]{6})));
            data.apply(data.propose(initial),1);
            var conditional=new DistributedCommand(data.groupId,1,new byte[32],new OperationId(data.session,1,1),List.of(new Mutation(Mutation.Kind.PUT,data.column,key,new byte[]{7})),List.of(new KeyPredicate(data.column,key,new byte[]{6})));
            var proposal=data.propose(conditional); assertNull(proposal.getException()); assertEquals(CommandResult.Status.SUCCESS,data.apply(proposal,2));
            assertArrayEquals(new byte[]{7},data.store.readCurrentPoint(data.column,key));
        }
    }
}
