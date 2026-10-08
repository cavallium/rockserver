package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Native multi-group workflow, bounded submissions and durable aggregate progress; no sockets/process acceptance. */
class RatisSessionPropagationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void boundedFailureFairnessAndRestartPreserveFrozenTargetAndAllParticipantCompletion() throws Exception {
        try (var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("journals")); var first=f.groups.getFirst();
            f.failed.add(first); RatisSessionPropagation.Progress frozen;
            try (var driver=f.driver(journals)) {
                frozen=driver.begin(); assertEquals(2,frozen.total()); assertFalse(frozen.complete());
                assertEquals(HexFormat.of().formatHex(f.directory.payloadDigest()),frozen.directoryDigest());
                assertThrows(IOException.class,() -> f.driver(journals));
                var progress=driver.step(1); assertEquals(0,progress.completed()); assertEquals(1,progress.failures().size()); assertEquals(1,f.submissions);
                assertThrows(IllegalStateException.class,driver::begin);
            }
            try (var driver=f.driver(journals)) {
                assertEquals(frozen.target(),driver.progress().target());
                var progress=driver.step(1); assertEquals(1,progress.completed()); assertFalse(progress.complete()); assertEquals(2,f.submissions);
                f.advance(20); f.failed.clear(); progress=driver.step(1);
                assertTrue(progress.complete()); assertEquals(10,progress.target().logicalTimeMillis(),"Old round cannot claim newer catalogue time");
                assertEquals(3,f.submissions);
            }
            try (var driver=f.driver(journals)) {
                assertTrue(driver.progress().complete()); assertEquals(2,driver.progress().participants().size());
                var next=driver.begin(); assertEquals(2,next.round()); assertEquals(20,next.target().logicalTimeMillis());
                assertTrue(driver.step(2).complete());
                for (var store:f.stores.values()) assertEquals(1,store.receiptUsage().retainedCount());
                int sent=f.submissions; assertTrue(driver.step(2).complete()); assertEquals(sent,f.submissions);
            }
        }
    }
    @Test void lostReplyBelowTargetNeedsAnotherVisitAndCannotPrematurelyCompleteTheRound() throws Exception {
        try (var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("older")); var first=f.groups.getFirst();
            // A previous clock round left an ambiguous command below the next round's frozen target.
            try (var stream=new RatisSessionMaintenance(journals.resolve(first+"-1.clock"),f.catalogue.epochs,f.routed,first)) {
                stream.prepareAdvance(); f.lost.add(first); assertThrows(IOException.class,stream::execute);
            }
            f.advance(20);
            try (var driver=f.driver(journals)) {
                driver.begin(); var one=driver.step(1); assertEquals(0,one.completed(),"Resolving an older success proves only the older cut");
                assertEquals(2,f.submissions); // first lost submission plus exactly one retry
                assertEquals(1,driver.step(1).completed());
                assertTrue(driver.step(1).complete());
                for (var store:f.stores.values()) assertEquals(1,store.receiptUsage().retainedCount());
            }
        }
    }
    @Test void directoryChangeRangeBudgetAndWrongIncarnationProofCannotReportCompletion() throws Exception {
        try (var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("scope"));
            try (var driver=new RatisSessionPropagation(journals,f.catalogue.epochs,f.routed,1)) {
                assertThrows(IOException.class,driver::begin); assertNull(driver.progress().target());
            }
            try (var driver=f.driver(journals)) {
                driver.begin(); assertTrue(driver.step(2).complete());
                var updated=f.updated(2,2); f.routed.refresh(updated); f.directory=updated;
                driver.begin();
                var first=f.groups.getFirst();
                Files.copy(journals.resolve(first+"-1.clock"),journals.resolve(first+"-2.clock"));
                var progress=driver.step(2); assertFalse(progress.complete()); assertEquals(1,progress.completed()); assertEquals(1,progress.failures().size());
                var later=f.updated(3,2); f.routed.refresh(later); f.directory=later;
                assertThrows(IOException.class,() -> driver.step(2)); assertThrows(IllegalStateException.class,driver::begin);
            }
        }
    }
    @Test void directoryRefreshDuringCatalogueReadDoesNotFreezeAnAlreadyObsoleteRound() throws Exception {
        try (var f=new Fixture(root)) {
            var refreshed=new AtomicBoolean();
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                if (!method.getName().equals("sendReadOnly")) throw new AssertionError(method);
                var reply=f.catalogue.client.io().sendReadOnly((Message)args[0]);
                if (!refreshed.getAndSet(true)) { f.directory=f.updated(2,1); f.routed.refresh(f.directory); }
                return reply;
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch (method.getName()) {
                case "getGroupId" -> f.catalogue.client.getGroupId(); case "io" -> io; default -> throw new AssertionError(method);
            });
            var epochs=new RatisSessionEpochs(client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.sessions);
            Path journals=Files.createDirectory(root.resolve("refresh"));
            try (var driver=new RatisSessionPropagation(journals,epochs,f.routed,2)) {
                assertThrows(IOException.class,driver::begin); assertNull(driver.progress().target()); assertEquals(0,f.submissions);
                assertEquals(2,driver.begin().directoryGeneration()); assertTrue(driver.step(2).complete());
            }
        }
    }
    @Test void manifestFailureAfterDurableChildCompletionRecoversWithoutAnotherSubmission() throws Exception {
        try (var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("manifest-failure")), manifest=journals.resolve("propagation.bin"); byte[] frozen;
            try (var driver=f.driver(journals)) {
                driver.begin(); frozen=Files.readAllBytes(manifest);
                Files.delete(manifest); Files.createDirectory(manifest); Files.writeString(manifest.resolve("block"),"fixture");
                assertThrows(IOException.class,() -> driver.step(1)); assertEquals(1,f.submissions);
                assertEquals(0,driver.progress().completed(),"Unconfirmed manifest completion remains unpublished");
                assertThrows(IOException.class,() -> driver.step(1));
            }
            Files.delete(manifest.resolve("block")); Files.delete(manifest); Files.write(manifest,frozen);
            try (var driver=f.driver(journals)) {
                assertEquals(0,driver.progress().completed()); assertEquals(1,driver.step(1).completed());
                assertEquals(1,f.submissions,"Durable per-range proof avoids resubmission after aggregate recovery");
                assertTrue(driver.step(1).complete()); assertEquals(2,f.submissions);
            }
        }
    }
    @Test void corruptManifestRejectsRecoveryAndActiveWorkRetainsExclusiveOwnership() throws Exception {
        try (var f=new Fixture(root)) {
            Path journals=Files.createDirectory(root.resolve("ownership"));
            var started=new CountDownLatch(1); var release=new CountDownLatch(1); f.started=started; f.release=release;
            try (var executor=Executors.newSingleThreadExecutor(); var driver=f.driver(journals)) {
                driver.begin(); var running=executor.submit(() -> driver.step(1));
                try {
                    assertTrue(started.await(5,TimeUnit.SECONDS));
                    assertThrows(IOException.class,driver::close); assertThrows(IOException.class,() -> f.driver(journals));
                    assertThrows(IllegalStateException.class,() -> driver.step(1));
                } finally { release.countDown(); }
                assertEquals(1,running.get(5,TimeUnit.SECONDS).completed());
            }
            Path manifest=journals.resolve("propagation.bin"); byte[] bytes=Files.readAllBytes(manifest); bytes[bytes.length/2]^=1; Files.write(manifest,bytes);
            assertThrows(IOException.class,() -> f.driver(journals)); assertArrayEquals(bytes,Files.readAllBytes(manifest));
        }
    }
    static final class Fixture implements AutoCloseable {
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest(); final RatisSessionEpochsTest.Fixture catalogue;
        final List<UUID> groups=List.of(new UUID(3,1),new UUID(3,2));
        final Map<UUID,ReplicatedRangeStore> stores=new LinkedHashMap<>(); final Map<UUID,RatisRangeStateMachine> machines=new LinkedHashMap<>();
        final Map<UUID,RaftGroup> raftGroups=new HashMap<>(); final List<RaftStorage> storage=new ArrayList<>();
        final Set<UUID> failed=new HashSet<>(),lost=new HashSet<>(); RangeDirectory directory; SessionEpochLedger ledger;
        java.util.function.Consumer<DistributedCommand> beforeCommand=command -> {};
        final RatisRoutedClient routed; final byte[] dataSchemaDigest; int submissions,metadataReads,customerReads; CountDownLatch started,release;
        Fixture(Path root) throws Exception { this(root,true); }
        Fixture(Path root,boolean initialized) throws Exception { this(root,initialized,new byte[32]); }
        Fixture(Path root,boolean initialized,byte[] dataSchemaDigest) throws Exception { this(root,initialized,dataSchemaDigest,null); }
        Fixture(Path root,boolean initialized,byte[] dataSchemaDigest,DistributedSchema schema) throws Exception {
            this.dataSchemaDigest=dataSchemaDigest.clone();
            catalogue=outer.new Fixture(root.resolve("catalogue"));
            var initial=SessionEpochLedger.initial(outer.cluster,outer.database,10);
            if (initialized) catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(0),null,initial));
            ledger=initialized ? initial.issue(outer.a,100) : initial;
            if (initialized) catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(1),initial,ledger));
            directory=updated(initialized ? 1 : 0,1);
            for (var descriptor:directory.ranges()) {
                var group=descriptor.groupId(); var peer=RaftPeer.newBuilder().setId(descriptor.replicas().getFirst().peerId()).build();
                var raft=RaftGroup.valueOf(RaftGroupId.valueOf(group),peer); raftGroups.put(group,raft);
                var store=catalogue.pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(directory.domainId(),Set.of(outer.data),descriptor.lowerInclusive(),descriptor.upperExclusive()),
                        this.dataSchemaDigest,Map.of(outer.data,"data"),Set.of()); if (schema!=null) store.enableLogicalSchema(schema); stores.put(group,store);
                var machine=RatisRangeStateMachine.withSessions(store,raft,RatisSessionBindingTest.Fixture.LIMITS,command -> {},catalogue.epochs,directory,
                        RatisRangeStateMachine.RetentionLimits.defaults(),() -> new RatisRangeStateMachine.Leadership(1,true)); machines.put(group,machine);
                var nativeStorage=RaftStorage.newBuilder().setDirectory(root.resolve(group.toString()).toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                        .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                        .setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build(); nativeStorage.initialize(); storage.add(nativeStorage);
                var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> {
                    if (method.getName().equals("getId")) return peer.getId();
                    if (method.getName().equals("toString")) return "identity-fixture"; throw new AssertionError(method);
                });
                machine.initialize(identity,raft.getGroupId(),nativeStorage); machine.notifyTermIndexUpdated(1,0);
            }
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] transaction(RangeDescriptor descriptor,byte[] request,boolean fresh) throws IOException {
                    try {
                        var machine=machines.get(descriptor.groupId()); var message=Message.valueOf(ByteString.copyFrom(request));
                        return (fresh ? machine.queryLinearizable(message) : machine.query(message)).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                    } catch (Exception failure) { throw new IOException(failure); }
                }
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) throws IOException {
                    customerReads++;
                    try {
                        byte[] request=ScopedTransactionRpc.pointRequest(column,key);
                        byte[] response=machines.get(descriptor.groupId()).query(Message.valueOf(ByteString.copyFrom(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                        return ScopedTransactionRpc.pointValue(response,2*1024*1024);
                    } catch (Exception failure) { throw new IOException(failure); }
                }
                public RangeStateView stateView(RangeDescriptor descriptor) throws IOException {
                    metadataReads++; if (failed.contains(descriptor.groupId())) throw new IOException("range unavailable");
                    try { return RangeStateView.decode(ScopedTransactionRpc.parse(machines.get(descriptor.groupId()).query(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0])))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),ScopedTransactionRpc.MAX_REPLY_BYTES).body()); }
                    catch (Exception failure) { throw new IOException(failure); }
                }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) throws IOException {
                    submissions++; beforeCommand.accept(command);
                    if (started!=null) {
                        started.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("fixture timeout"); }
                        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
                    }
                    if (failed.contains(descriptor.groupId())) throw new IOException("range unavailable");
                    try {
                        var machine=machines.get(descriptor.groupId()); var group=raftGroups.get(descriptor.groupId());
                        var context=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(group.getPeers().iterator().next().getId())
                                .setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType())
                                .setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build());
                        if (context.getException()!=null) throw new IOException("Admission rejected",context.getException());
                        machine.preAppendTransaction(context); context.initLogEntry(1,stores.get(descriptor.groupId()).applied().index()+1);
                        var result=CommandResult.decode(machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray());
                        if (lost.remove(descriptor.groupId())) throw new IOException("reply lost after native apply"); return result;
                    } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
                }
            });
        }
        RangeDirectory updated(long generation,long firstEpoch) {
            var ranges=new ArrayList<RangeDescriptor>();
            for (int i=0;i<2;i++) {
                var replica=new ReplicaDescriptor(new UUID(5,i+1),new UUID(5,i+11),new UUID(5,i+3),"range"+i,"127.0.0.1:"+(i+1),ReplicaDescriptor.Role.VOTER);
                ranges.add(new RangeDescriptor(new UUID(6,i+2),groups.get(i),i==0 ? firstEpoch : 1,0,i==0 ? null : new byte[]{(byte)128},
                        i==0 ? new byte[]{(byte)128} : null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica)));
            }
            return new RangeDirectory(outer.cluster,outer.database,new UUID(6,1),1,dataSchemaDigest,generation,Set.of(outer.data),0,ranges);
        }
        RatisSessionPropagation driver(Path journals) throws IOException { return new RatisSessionPropagation(journals,catalogue.epochs,routed,2); }
        void advance(long time) throws Exception {
            var next=ledger.advanceTime(time); catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(ledger.generation()+1),ledger,next)); ledger=next;
        }
        public void close() throws Exception {
            routed.close(); for (var machine:machines.values()) { machine.close(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); }
            for (var nativeStorage:storage) nativeStorage.close(); catalogue.close();
        }
    }
}
