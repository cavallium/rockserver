package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

/** Native CAS and facade composition, without transport, sockets or quorum claims. */
class RatisSessionEpochsTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID cluster=new UUID(1,1), database=new UUID(1,2), catalogue=new UUID(1,3), directoryColumn=new UUID(2,1),
            sessions=new UUID(2,2), data=new UUID(2,3), dataGroup=new UUID(3,1), a=new UUID(4,1), b=new UUID(4,2), maintenance=new UUID(4,3);
    OperationId operation(long seq) { return new OperationId(new UUID(7,7),20,seq); }
    RangeDirectory directory(UUID clusterId) {
        var replica=new ReplicaDescriptor(new UUID(5,1),new UUID(5,2),new UUID(5,3),"node","127.0.0.1:1",ReplicaDescriptor.Role.VOTER);
        return new RangeDirectory(clusterId,database,new UUID(6,1),1,new byte[32],0,Set.of(data),0,
                List.of(new RangeDescriptor(new UUID(6,2),dataGroup,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
    }
    @Test void nativeCatalogueCASFreezesAllocatedEpochAndOriginalOutcomeAcrossConflictsAndRetry() throws Exception {
        try (var fixture=new Fixture(root)) {
            assertNull(fixture.epochs.read());
            var initial=SessionEpochLedger.initial(cluster,database,10);
            assertEquals(CommandResult.Status.SUCCESS,fixture.epochs.execute(fixture.epochs.prepare(operation(0),null,initial)).status());
            var winner=fixture.epochs.prepareGrant(operation(1),initial,a,100);
            var loser=fixture.epochs.prepareGrant(operation(2),initial,b,80);
            byte[] original=DistributedCommandCodec.encode(winner.command());
            assertEquals(CommandResult.Status.SUCCESS,fixture.epochs.execute(winner.command()).status());
            assertEquals(CommandResult.Status.CONFLICT,fixture.epochs.execute(loser.command()).status());
            var current=fixture.epochs.read();
            assertEquals(2,current.nextEpoch()); assertEquals(a,current.grants().getFirst().sessionId());
            var later=fixture.epochs.prepareGrant(operation(3),current,b,200);
            fixture.epochs.execute(later.command());
            assertEquals(3,fixture.epochs.read().nextEpoch());
            assertEquals(CommandResult.Status.SUCCESS,fixture.epochs.execute(winner.command()).status());
            assertEquals(1,winner.grant().epoch(),"Successful retry resolves the frozen grant, not current nextEpoch");
            assertArrayEquals(original,DistributedCommandCodec.encode(winner.command()));
            assertEquals(3,fixture.epochs.read().nextEpoch());
            assertEquals(CommandResult.Status.CONFLICT,fixture.epochs.execute(loser.command()).status());
            var before=fixture.epochs.read();
            var heartbeat=before.advanceTime(20);
            assertEquals(CommandResult.Status.SUCCESS,fixture.epochs.execute(fixture.epochs.prepare(operation(4),before,heartbeat,3)).status());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,fixture.epochs.execute(winner.command()).status());
            assertEquals(heartbeat,fixture.epochs.read());
            assertEquals(CommandResult.Status.SUCCESS,fixture.epochs.execute(fixture.epochs.prepareAcknowledgement(operation(5),4)).status());
            assertEquals(heartbeat,fixture.epochs.read(),"ACK-only keeps the epoch ledger unchanged");
            assertEquals(0,fixture.closes.get(),"Facade never closes borrowed client");
        }
    }
    @Test void oneFreshCatalogueImageAuthorizesOnlyLiveIssuerAndProvenFloorInBoundDirectory() throws Exception {
        try (var f=new Fixture(root)) {
            var ledger=SessionEpochLedger.initial(cluster,database,10);
            f.epochs.execute(f.epochs.prepare(operation(0),null,ledger));
            var one=ledger.issue(a,100); f.epochs.execute(f.epochs.prepare(operation(1),ledger,one));
            var two=one.issue(b,50); f.epochs.execute(f.epochs.prepare(operation(2),one,two));
            var expired=two.advanceTime(50); f.epochs.execute(f.epochs.prepare(operation(3),two,expired));
            var maintained=expired.issue(maintenance,200); f.epochs.execute(f.epochs.prepare(operation(4),expired,maintained));
            var closed=maintained.advanceTime(100); f.epochs.execute(f.epochs.prepare(operation(5),maintained,closed));
            var latest=closed.issue(a,250); f.epochs.execute(f.epochs.prepare(operation(6),closed,latest));
            var authority=f.epochs.floorAdmission(directory(cluster),dataGroup);
            var valid=DistributedCommand.advanceSessionEpochFloor(dataGroup,1,new byte[32],new OperationId(a,4,0),3);
            int before=f.reads.get(); authority.accept(valid); assertEquals(before+1,f.reads.get());
            assertThrows(IllegalStateException.class,() -> authority.accept(DistributedCommand.advanceSessionEpochFloor(dataGroup,1,new byte[32],new OperationId(a,4,1),4)));
            assertThrows(IllegalStateException.class,() -> authority.accept(DistributedCommand.advanceSessionEpochFloor(dataGroup,1,new byte[32],new OperationId(b,4,1),3)));
            assertThrows(IllegalArgumentException.class,() -> f.epochs.floorAdmission(directory(UUID.randomUUID()),dataGroup));
            assertThrows(IllegalArgumentException.class,() -> authority.accept(DistributedCommand.advanceSessionEpochFloor(UUID.randomUUID(),1,new byte[32],new OperationId(a,4,1),3)));
            var dataStore=f.pool.openRange(new RangeOwnership.Owner(dataGroup,1),new RangeOwnership.Interval(new UUID(6,1),Set.of(data),null,null),
                    new byte[32],Map.of(data,"data"),Set.of());
            var peer=RaftPeer.newBuilder().setId("data-fixture").build();
            var group=RaftGroup.valueOf(RaftGroupId.valueOf(dataGroup),peer);
            var identity=(org.apache.ratis.server.RaftServer)Proxy.newProxyInstance(org.apache.ratis.server.RaftServer.class.getClassLoader(),
                    new Class<?>[]{org.apache.ratis.server.RaftServer.class},(proxy,method,args) -> {
                        if (method.getName().equals("getId")) return peer.getId();
                        if (method.getName().equals("toString")) return "identity-only-fixture";
                        throw new AssertionError("Unexpected server operation "+method);
                    });
            try (var storage=org.apache.ratis.server.storage.RaftStorage.newBuilder().setDirectory(root.resolve("data-raft").toFile())
                    .setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT)
                    .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                    .setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build()) {
                storage.initialize();
                var machine=new RatisRangeStateMachine(dataStore,group,new NativeRangeImage.Limits(2*1024*1024,16*1024*1024,1),command -> {},authority,
                        () -> new RatisRangeStateMachine.Leadership(1,true));
                try {
                    machine.initialize(identity,group.getGroupId(),storage);
                    machine.notifyTermIndexUpdated(1,0);
                    var request=RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId())
                            .setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(valid)))).build();
                    int count=f.reads.get(); var accepted=machine.startTransaction(request);
                    assertNull(accepted.getException()); assertEquals(count+1,f.reads.get()); accepted.initLogEntry(1,1);
                    f.unavailable=true;
                    assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(machine.applyTransaction(accepted).get(5,java.util.concurrent.TimeUnit.SECONDS).getContent().toByteArray()).status());
                    var old=new DistributedCommand(dataGroup,1,new byte[32],new OperationId(a,1,99),List.of(new Mutation(Mutation.Kind.PUT,data,new byte[]{1},new byte[]{9})));
                    var replay=org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(machine)
                            .setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).setLogEntry(org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder()
                            .setTerm(1).setIndex(2).setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder()
                            .setLogData(ByteString.copyFrom(DistributedCommandCodec.encode(old)))).build()).build();
                    assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(machine.applyTransaction(replay).get(5,java.util.concurrent.TimeUnit.SECONDS).getContent().toByteArray()).status());
                    try (var pin=dataStore.capture()) { assertNull(pin.get(data,new byte[]{1})); }
                    assertThrows(IllegalStateException.class,() -> authority.accept(valid));
                } finally { machine.close(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); }
            }
        }
    }
    @Test void combinedValidatorRejectsColumnKeyIdentityAndHistoryChangesWithoutWeakeningDirectoryCAS() throws Exception {
        try (var f=new Fixture(root)) {
            var initial=SessionEpochLedger.initial(cluster,database,10);
            var valid=f.epochs.prepare(operation(0),null,initial);
            assertDoesNotThrow(() -> f.validate.accept(valid));
            assertThrows(IllegalArgumentException.class,() -> f.validate.accept(new DistributedCommand(catalogue,1,new byte[32],operation(0),
                    valid.mutations(),List.of())));
            assertThrows(IllegalArgumentException.class,() -> f.validate.accept(new DistributedCommand(catalogue,1,new byte[32],operation(0),
                    List.of(new Mutation(Mutation.Kind.PUT,directoryColumn,valid.mutations().getFirst().key(),initial.encode())),valid.predicates())));
            var foreign=SessionEpochLedger.initial(UUID.randomUUID(),database,10);
            assertThrows(IllegalArgumentException.class,() -> f.epochs.prepare(operation(0),null,foreign));
            assertThrows(IllegalArgumentException.class,() -> f.epochs.prepare(operation(0),null,initial.issue(a,100)));
            var dir=directory(cluster); var key=bytes(dir.domainId());
            var dirCommand=new DistributedCommand(catalogue,1,new byte[32],operation(1),
                    List.of(new Mutation(Mutation.Kind.PUT,directoryColumn,key,RangeDirectoryCodec.encode(dir))),List.of(new KeyPredicate(directoryColumn,key,null),new KeyPredicate(directoryColumn,RatisSchemaAdministration.headKey(dir.domainId()),null)));
            assertDoesNotThrow(() -> f.validate.accept(dirCommand));
        }
    }
    @Test void missingMalformedOrUnavailableAuthorityFailsAndLostReplyRetriesOriginalCommand() throws Exception {
        try (var f=new Fixture(root)) {
            var authority=f.epochs.floorAdmission(directory(cluster),dataGroup);
            var floor=DistributedCommand.advanceSessionEpochFloor(dataGroup,1,new byte[32],new OperationId(a,1,0),1);
            assertThrows(IllegalStateException.class,() -> authority.accept(floor));
            f.readOverride=ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array();
            assertThrows(IOException.class,f.epochs::read);
            assertThrows(IllegalStateException.class,() -> authority.accept(floor));
            f.readOverride=null; f.grpcUnavailable=true;
            assertThrows(IOException.class,f.epochs::read);
            var initial=SessionEpochLedger.initial(cluster,database,10);
            var prepared=f.epochs.prepare(operation(0),null,initial);
            assertThrows(IOException.class,() -> f.epochs.execute(prepared));
            assertEquals(0,f.next.get(),"Unavailable transport submitted no native command");
            f.grpcUnavailable=false; f.writeOverride=new byte[4];
            assertThrows(IOException.class,() -> f.epochs.execute(prepared));
            assertEquals(initial,f.epochs.read(),"Malformed reply does not roll back an applied CAS");
            f.writeOverride=null;
            assertEquals(CommandResult.Status.SUCCESS,f.epochs.execute(prepared).status());
            assertEquals(initial,f.epochs.read());
            assertEquals(2,f.next.get(),"One explicit retry, no automatic submission");
        }
    }

    @Test void typedBindingAuthorityIsFreshAndMaintenanceSurvivesExpiryOfEveryUserGrant() throws Exception {
        try (var f=new Fixture(root)) {
            var dir=directory(cluster);
            assertThrows(IOException.class,() -> f.epochs.prepareAuthorityAdvance(dir,dataGroup,operation(0),-1));
            var initial=SessionEpochLedger.initial(cluster,database,10);
            f.epochs.execute(f.epochs.prepare(operation(0),null,initial));
            var live=initial.issue(a,100); f.epochs.execute(f.epochs.prepare(operation(1),initial,live));
            int reads=f.reads.get();
            var binding=f.epochs.prepareSessionBinding(dir,dataGroup,operation(10),live.grants().getFirst(),32,9);
            assertEquals(reads+1,f.reads.get());
            var admission=f.epochs.sessionControlAdmission(dir,dataGroup);
            assertThrows(IllegalArgumentException.class,() -> f.epochs.floorAdmission(dir,dataGroup).accept(binding));
            admission.accept(binding); assertEquals(reads+2,f.reads.get());
            var fake=new RangeSessionControl(SessionAuthority.from(live),new SessionEpochLedger.Grant(1,a,10,99),32);
            assertThrows(IllegalStateException.class,() -> admission.accept(DistributedCommand.sessionControl(dataGroup,1,new byte[32],operation(11),fake,-1)));
            var unrelated=live.issue(b,200); f.epochs.execute(f.epochs.prepare(operation(2),live,unrelated));
            assertDoesNotThrow(() -> admission.accept(binding),"Unrelated issuance must not starve an immutable live binding");
            var dataStore=f.pool.openRange(new RangeOwnership.Owner(dataGroup,1),new RangeOwnership.Interval(dataGroup,Set.of(data),null,null),
                    new byte[32],Map.of(data,"data"),Set.of());
            assertThrows(UnsupportedOperationException.class,() -> dataStore.apply(new ApplyPosition(1,0),binding));
            assertNull(dataStore.applied(),"Unsupported controls cannot silently become no-op receipts");
            var expired=unrelated.advanceTime(200); f.epochs.execute(f.epochs.prepare(operation(3),unrelated,expired));
            assertThrows(IllegalStateException.class,() -> admission.accept(binding));
            var advance=f.epochs.prepareAuthorityAdvance(dir,dataGroup,new OperationId(maintenance,expired.minimumEpoch(),12),11);
            assertFalse(advance.sessionControl().binding()); assertEquals(SessionAuthority.from(expired),advance.sessionControl().authority());
            assertFalse(expired.isLive(advance.operationId()));
            assertDoesNotThrow(() -> admission.accept(advance));
            assertThrows(IllegalArgumentException.class,() -> f.epochs.sessionControlAdmission(directory(UUID.randomUUID()),dataGroup));
            byte[] frozen=DistributedCommandCodec.encode(advance); f.unavailable=true;
            assertThrows(IllegalStateException.class,() -> admission.accept(advance));
            assertArrayEquals(frozen,DistributedCommandCodec.encode(advance));
        }
    }

    final class Fixture implements AutoCloseable {
        final DistributedStoragePool pool;
        final ReplicatedRangeStore store;
        final AtomicLong next=new AtomicLong();
        final AtomicInteger reads=new AtomicInteger(), closes=new AtomicInteger();
        final java.util.function.Consumer<DistributedCommand> validate=RatisCatalogue.validator(cluster,database,catalogue,1,new byte[32],directoryColumn,sessions);
        final RaftClient client;
        final RatisSessionEpochs epochs;
        boolean unavailable, grpcUnavailable, schemaUnavailable, missingSchema;
        byte[] readOverride, writeOverride;
        Fixture(Path path) throws Exception { this(path,null); }
        Fixture(Path path,it.cavallium.rockserver.core.impl.rocksdb.PoolSnapshotAdmission.Limits pins) throws Exception {
            pool=DistributedStoragePool.open(path.resolve("catalogue"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            if (pins!=null) pool.configureSnapshotAdmission(pins);
            store=pool.openRange(new RangeOwnership.Owner(catalogue,1),new RangeOwnership.Interval(catalogue,Set.of(directoryColumn,sessions),null,null),
                    new byte[32],Map.of(directoryColumn,"directories",sessions,"session_epochs"),Set.of());
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                if (unavailable) throw new IOException("catalogue unavailable");
                if (grpcUnavailable) throw org.apache.ratis.thirdparty.io.grpc.Status.UNAVAILABLE.asRuntimeException();
                byte[] reply;
                if (method.getName().equals("send")) {
                    var command=DistributedCommandCodec.decode(((Message)args[0]).getContent().toByteArray()); validate.accept(command);
                    reply=store.apply(new ApplyPosition(1,next.getAndIncrement()),command);
                    if (writeOverride!=null) reply=writeOverride;
                } else if (method.getName().equals("sendReadOnly")) {
                    reads.incrementAndGet(); byte[] query=((Message)args[0]).getContent().toByteArray();
                    var packet=ScopedTransactionRpc.parse(query,ScopedTransactionRpc.MAX_REQUEST_BYTES); assertEquals(ScopedTransactionRpc.POINT,packet.kind()); var input=ByteBuffer.wrap(packet.body());
                    UUID column=ScopedTransactionRpc.uuid(input); byte[] key=ScopedTransactionRpc.blob(input,2*1024*1024,false); assertFalse(input.hasRemaining());
                    assertTrue(column.equals(sessions) || column.equals(directoryColumn));
                    if (column.equals(sessions)) assertArrayEquals(bytes(database),key);
                    if (key.length==33) { assertEquals(directoryColumn,column); assertEquals(0x53,key[0]); if (schemaUnavailable) throw new IOException("schema unavailable"); }
                    try (var pin=store.capture()) {
                        byte[] value=key.length==33 && missingSchema ? null : pin.get(column,key);
                        reply=value==null ? ByteBuffer.allocate(4).putInt(-1).array() : ByteBuffer.allocate(4+value.length).putInt(value.length).put(value).array();
                    }
                    if (readOverride!=null) reply=readOverride;
                    reply=ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,reply);
                } else throw new AssertionError("Unexpected RPC "+method);
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("fixture"))
                        .setGroupId(RaftGroupId.valueOf(catalogue)).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(reply))).build();
            });
            client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> {
                return switch(method.getName()) {
                    case "getGroupId" -> RaftGroupId.valueOf(catalogue);
                    case "io" -> io;
                    case "close" -> { closes.incrementAndGet(); yield null; }
                    default -> throw new AssertionError("Unexpected client operation "+method);
                };
            });
            epochs=new RatisSessionEpochs(client,cluster,database,catalogue,1,new byte[32],sessions);
        }
        public void close() throws Exception { client.close(); pool.close(); }
    }
    static byte[] bytes(UUID id) { return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(); }
}
