package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native catalogue storage with controlled client replies; no network or quorum claim. */
class RatisCdcSubscriptionCatalogueTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisRangeImportCatalogueTest fixtureOwner() { var owner=new RatisRangeImportCatalogueTest(); owner.root=root; return owner; }
    RangeDirectory directory(RatisRangeImportCatalogueTest.Fixture f) {
        var original=RatisRangeImportCatalogueTest.definition(f.outer.cluster,f.outer.database,f.operation(100)).sourceDirectory(); var range=original.ranges().getFirst();
        var active=new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),range.lowerInclusive(),range.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,range.replicas());
        return new RangeDirectory(original.clusterId(),original.databaseId(),original.domainId(),original.schemaEpoch(),original.schemaDigest(),original.generation(),original.columns(),original.routingPrefixBytes(),List.of(active));
    }
    DistributedCdcSubscription.Definition definition(RangeDirectory directory,int id) {
        var range=directory.ranges().getFirst(); var start=new DistributedCdcCheckpoint(Map.of(new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()),DistributedCdcCheckpoint.Cursor.beginning()));
        return DistributedCdcSubscription.Definition.create(new UUID(10,id),new UUID(11,id),directory,directory.columns(),start);
    }
    RatisCdcSubscriptionCatalogue catalogue(RatisRangeImportCatalogueTest.Fixture f) {
        return new RatisCdcSubscriptionCatalogue(f.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
    }

    @Test void createCloseAndLostReplyRetryPersistTogetherWithFixedCapacityAndSnapshot() throws Exception {
        Path image=root.resolve("subscriptions.image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,2); var owner=fixtureOwner();
        DistributedCdcSubscription closed;
        try(var f=owner.new Fixture()) {
            var directory=directory(f); f.seed(directory); var catalogue=catalogue(f); var definition=definition(directory,1);
            assertNull(catalogue.readCapacity()); var create=catalogue.prepareCreate(f.operation(1),definition,directory,null);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(create).status()); // Successful response is lost by the caller.
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(create).status());
            var state=catalogue.read(definition.subscriptionId()); assertEquals(DistributedCdcSubscription.initial(definition),state);
            var capacity=catalogue.readCapacity(); assertEquals(1,capacity.count()); assertEquals(17+DistributedCdcSubscription.MAX_BYTES,capacity.bytes());
            var close=catalogue.prepareClose(f.operation(2),state); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(close).status());
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(close).status()); closed=catalogue.read(definition.subscriptionId());
            assertEquals(state.close(),closed); assertEquals(capacity,catalogue.readCapacity());
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(create).status()); assertEquals(closed,catalogue.read(definition.subscriptionId()));
            try(var pin=f.nativeCatalogue.store.capture()) { NativeRangeImage.write(image,pin,pin.interval(),limits); }
        }
        try(var f=owner.new Fixture()) {
            assertEquals(closed,catalogue(f).read(closed.definition().subscriptionId())); assertEquals(1,catalogue(f).readCapacity().count());
            f.nativeCatalogue.store.fence(); f.nativeCatalogue.store.installSnapshot(image,limits); f.nativeCatalogue.store.activate();
            assertEquals(closed,catalogue(f).read(closed.definition().subscriptionId())); assertEquals(1,catalogue(f).readCapacity().count());
        }
    }

    @Test void conflictingBirthReservesCapacityOnceAndAckForgeryIsRejectedBeforeEffects() throws Exception {
        var owner=fixtureOwner(); try(var f=owner.new Fixture()) {
            var directory=directory(f); f.seed(directory); var catalogue=catalogue(f); var first=definition(directory,1); var second=definition(directory,2);
            var a=catalogue.prepareCreate(f.operation(1),first,directory,null); var b=catalogue.prepareCreate(f.operation(2),second,directory,null);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(a).status()); assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(b).status());
            assertNull(catalogue.read(second.subscriptionId())); assertEquals(1,catalogue.readCapacity().count());
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(b).status());
            var retry=catalogue.prepareCreate(f.operation(3),second,directory,catalogue.readCapacity()); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(retry).status());
            var state=catalogue.read(first.subscriptionId()); var lineage=state.checkpoint().cursors().keySet().iterator().next();
            var next=state.checkpoint().advance(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,10))); var ack=state.deliver(next).acknowledge(next);
            byte[] key=RatisCdcSubscriptionCatalogue.stateKey(first.subscriptionId());
            var forged=new DistributedCommand(f.outer.catalogue,1,new byte[32],f.operation(4),List.of(new Mutation(Mutation.Kind.PUT,f.outer.directoryColumn,key,ack.encode())),List.of(new KeyPredicate(f.outer.directoryColumn,key,state.encode())));
            assertThrows(IllegalArgumentException.class,()->catalogue.execute(forged)); assertEquals(state,catalogue.read(first.subscriptionId())); assertEquals(2,catalogue.readCapacity().count());
        }
    }

    @Test void malformedCapacitySourceAndNamespaceCannotAcquireProtectedRows() throws Exception {
        var owner=fixtureOwner(); try(var f=owner.new Fixture()) {
            var directory=directory(f); f.seed(directory); var catalogue=catalogue(f); var definition=definition(directory,1);
            assertThrows(IllegalArgumentException.class,()->catalogue.prepareCreate(f.operation(1),definition,directory,new RatisCdcSubscriptionCatalogue.Capacity(RatisCdcSubscriptionCatalogue.MAX_SUBSCRIPTIONS,1)));
            assertThrows(IllegalArgumentException.class,()->catalogue.prepareCreate(f.operation(1),definition,directory,new RatisCdcSubscriptionCatalogue.Capacity(1,RatisCdcSubscriptionCatalogue.MAX_STATE_BYTES)));
            var create=catalogue.prepareCreate(f.operation(1),definition,directory,null);
            var changed=new ArrayList<>(create.mutations()); changed.set(1,new Mutation(Mutation.Kind.PUT,f.outer.directoryColumn,RatisCdcSubscriptionCatalogue.capacityKey(),new RatisCdcSubscriptionCatalogue.Capacity(1,1).encode()));
            var uncharged=new DistributedCommand(create.groupId(),create.rangeEpoch(),create.schemaDigest(),create.operationId(),changed,create.predicates());
            assertThrows(IllegalArgumentException.class,()->catalogue.execute(uncharged)); assertNull(catalogue.readCapacity());
            var namespace=new DistributedCdcSubscription.Definition(UUID.randomUUID(),definition.databaseId(),definition.domainId(),definition.subscriptionId(),definition.incarnationId(),definition.directoryDigest(),definition.columns(),definition.start());
            assertThrows(IllegalArgumentException.class,()->catalogue.prepareCreate(f.operation(2),namespace,directory,null));
            var rawDirectoryKey=ByteBuffer.allocate(16).putLong(0x8000000000000000L).putLong(1).array();
            var ordinary=new DistributedCommand(create.groupId(),create.rangeEpoch(),create.schemaDigest(),f.operation(3),List.of(new Mutation(Mutation.Kind.PUT,f.outer.directoryColumn,rawDirectoryKey,new byte[]{1})));
            assertFalse(RatisCdcSubscriptionCatalogue.isCommand(ordinary));
        }
    }
    @Test void oversizedRepliesAreRejectedBeforeDecodingAtEachOperationBudget() throws Exception {
        var owner=fixtureOwner(); try(var f=owner.new Fixture()) {
            var length=new AtomicInteger();
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) ->
                    new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("oversize-fixture"))
                            .setGroupId(RaftGroupId.valueOf(f.outer.catalogue)).setCallId(1).setSuccess(true)
                            .setMessage(Message.valueOf(ByteString.copyFrom(new byte[length.get()]))).build());
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) {
                case "getGroupId" -> RaftGroupId.valueOf(f.outer.catalogue); case "io" -> io; default -> throw new AssertionError(method);
            });
            var catalogue=new RatisCdcSubscriptionCatalogue(client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            var source=directory(f); var definition=definition(source,1); var create=catalogue.prepareCreate(f.operation(1),definition,source,null);
            length.set(ScopedTransactionRpc.HEADER_BYTES+4+12+1);
            assertEquals("CDC catalogue reply exceeds operation budget",assertThrows(IOException.class,catalogue::readCapacity).getMessage());
            length.set(ScopedTransactionRpc.HEADER_BYTES+4+DistributedCdcSubscription.MAX_BYTES+1);
            assertEquals("CDC catalogue reply exceeds operation budget",assertThrows(IOException.class,()->catalogue.read(definition.subscriptionId())).getMessage());
            length.set(9);
            assertEquals("CDC catalogue reply exceeds operation budget",assertThrows(IOException.class,()->catalogue.execute(create)).getMessage());
            var state=DistributedCdcSubscription.initial(definition); var lineage=state.delivered().cursors().keySet().iterator().next();
            var delivery=catalogue.prepareDelivery(f.operation(2),state,state.delivered().advance(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1))));
            length.set(RatisCdcDeliveryRpc.request(RatisCdcDeliveryRpc.PREPARE,delivery).length+1);
            assertEquals("CDC catalogue reply exceeds operation budget",assertThrows(IOException.class,()->catalogue.prepareDeliveryOwner(delivery)).getMessage());
            assertEquals("CDC catalogue reply exceeds operation budget",assertThrows(IOException.class,()->catalogue.cancelDelivery(delivery)).getMessage());
        }
    }

}
