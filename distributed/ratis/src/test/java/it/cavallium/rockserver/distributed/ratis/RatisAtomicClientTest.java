package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Public asynchronous workflows over real native groups and controlled dispatch; no sockets/quorum claims. */
class RatisAtomicClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final Duration TTL=Duration.ofSeconds(30);
    static Keys key(int prefix) { return new Keys(Buf.wrap(new byte[]{(byte)prefix})); }
    static Keys member(int prefix,String text) { return new Keys(Buf.wrap(new byte[]{(byte)prefix}),Buf.wrap(text.getBytes(java.nio.charset.StandardCharsets.UTF_8))); }
    static Buf value(int number) { return Buf.wrap(new byte[]{(byte)number}); }
    static List<RatisLogicalClient.Change> changes() {
        return List.of(RatisLogicalClient.Change.put(72,key(200),value(9),Mutation.ReturnMode.CURRENT),
                RatisLogicalClient.Change.put(71,key(2),value(1),Mutation.ReturnMode.PREVIOUS),
                RatisLogicalClient.Change.put(74,member(200,"first"),value(5),Mutation.ReturnMode.PREVIOUS),
                RatisLogicalClient.Change.put(73,key(2),value(7),Mutation.ReturnMode.CURRENT),
                RatisLogicalClient.Change.put(74,member(200,"second"),value(8),Mutation.ReturnMode.CURRENT));
    }
    static List<RatisLogicalClient.Condition> absentIdentity() { return List.of(new RatisLogicalClient.Condition(71,key(2),null)); }
    static final class Fixture implements AutoCloseable {
        final Path root;
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest();
        final RatisSessionEpochsTest.Fixture catalogue;
        final UUID session=new UUID(110,1),admin=new UUID(110,2),homeGroup=new UUID(111,3),homeColumn=new UUID(112,5),snapshotGroup=new UUID(111,4),snapshotColumn=new UUID(112,6);
        final List<UUID> groups=List.of(new UUID(111,1),new UUID(111,2));
        final DistributedSchema schema;
        final RangeDirectory directory,homeDirectory,snapshotDirectory;
        final RatisRoutedClient data,homeClient,snapshotClient;
        final RatisTransactionStatuses homes;
        final RatisSnapshotFences snapshots;
        final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,4,1,"public-atomic-test");
        final RatisProofQueryService proofs=new RatisProofQueryService();
        final RatisSnapshotHelpService snapshotHelp;
        RatisParticipantRecoveryRegistry snapshotRecoveries;
        final AtomicInteger helped=new AtomicInteger();
        final Map<UUID,ReplicatedRangeStore> stores=new LinkedHashMap<>();
        final Map<UUID,RatisRangeStateMachine> machines=new LinkedHashMap<>();
        final Map<UUID,RaftGroup> raftGroups=new HashMap<>();
        final List<RaftStorage> storages=new ArrayList<>();
        final AtomicInteger opens=new AtomicInteger(),submissions=new AtomicInteger(),rpcs=new AtomicInteger(),captureRequests=new AtomicInteger();
        volatile Runnable beforeAuthorize,beforeHomeRead,beforeSnapshotCapture,beforeSnapshotRead,beforeSnapshotRelease;
        volatile UUID losePrepareGroup,loseCaptureGroup;
        final SessionEpochLedger ledger;
        final SessionEpochLedger.Grant grant;
        Fixture(Path root) throws Exception { this(root,false); }
        Fixture(Path root,boolean snapshotHome) throws Exception { this(root,snapshotHome,Map.of()); }
        Fixture(Path root,boolean snapshotHome,Map<Long,ColumnSchema> schemaOverrides) throws Exception {
            this.root=root; Files.createDirectories(root); catalogue=outer.new Fixture(root.resolve("catalogue"));
            snapshotHelp=snapshotHome ? new RatisSnapshotHelpService(scheduler) : null;
            try {
            var initial=SessionEpochLedger.initial(outer.cluster,outer.database,10);
            catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(0),null,initial));
            ledger=initial.issue(session,1000); grant=ledger.grants().getFirst();
            catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(1),initial,ledger));
            var columns=new ArrayList<DistributedSchema.Column>();
            for (int n=0;n<4;n++) columns.add(new DistributedSchema.Column(71+n,new UUID(112,n+1),"atomic-"+n,
                    schemaOverrides.getOrDefault(71L+n,ColumnSchema.of(IntArrayList.of(1),n==3 ? ObjectArrayList.of(ColumnHashType.ALLSAME8) : new ObjectArrayList<>(),true)),null));
            schema=new DistributedSchema(outer.cluster,outer.database,new UUID(113,1),1,1,columns);
            var routes=new ArrayList<RangeDescriptor>();
            for (int n=0;n<2;n++) routes.add(route(groups.get(n),n==0 ? null : new byte[]{(byte)128},n==0 ? new byte[]{(byte)128} : null));
            directory=new RangeDirectory(outer.cluster,outer.database,schema.domainId(),1,schema.digest(),0,schema.columnIds(),1,routes);
            homeDirectory=new RangeDirectory(outer.cluster,outer.database,new UUID(113,2),1,new byte[32],0,Set.of(homeColumn),0,List.of(route(homeGroup,null,null)));
            snapshotDirectory=snapshotHome ? new RangeDirectory(outer.cluster,outer.database,new UUID(113,3),1,new byte[32],0,Set.of(snapshotColumn),0,List.of(route(snapshotGroup,null,null))) : null;
            data=new RatisRoutedClient(directory,transport()); homeClient=new RatisRoutedClient(homeDirectory,transport()); homes=new RatisTransactionStatuses(homeClient,homeColumn);
            snapshotClient=snapshotHome ? new RatisRoutedClient(snapshotDirectory,transport()) : null; snapshots=snapshotHome ? new RatisSnapshotFences(snapshotClient,snapshotColumn) : null;
            var sources=new ArrayList<>(List.of(directory,homeDirectory)); if (snapshotHome) sources.add(snapshotDirectory);
            for (var source:sources) for (var descriptor:source.ranges()) {
                var bindings=new LinkedHashMap<UUID,String>();
                if (source==directory) for (var column:schema.columns()) bindings.put(column.columnId(),column.name()); else if (source==homeDirectory) bindings.put(homeColumn,"atomic-home"); else bindings.put(snapshotColumn,"snapshot-home");
                var store=catalogue.pool.openRange(new RangeOwnership.Owner(descriptor.groupId(),1),new RangeOwnership.Interval(source.domainId(),source.columns(),descriptor.lowerInclusive(),descriptor.upperExclusive()),source.schemaDigest(),bindings,Set.of());
                if (source==directory) store.enableLogicalSchema(schema); stores.put(descriptor.groupId(),store);
                var peer=RaftPeer.newBuilder().setId("atomic-fixture").build(); var group=RaftGroup.valueOf(RaftGroupId.valueOf(descriptor.groupId()),peer); raftGroups.put(descriptor.groupId(),group);
                var machine=RatisRangeStateMachine.withSessions(store,group,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),
                        source==directory ? command -> {} : source==homeDirectory ? RatisTransactionStatuses.validator(outer.cluster,outer.database,homeGroup,1,new byte[32],homeColumn) : RatisSnapshotFences.validator(outer.cluster,outer.database,snapshotGroup,1,new byte[32],snapshotColumn),
                        catalogue.epochs,source,RatisRangeStateMachine.RetentionLimits.defaults(),() -> new RatisRangeStateMachine.Leadership(1,true));
                machine.proofQueries(proofs);
                if (source==directory) { machine.transactionHomes(homes); if (snapshotHome) {
                    machine.snapshotHomes(snapshots); machine.snapshotRecovery((groupId,prepared) -> {
                        if (snapshotRecoveries==null) throw new IOException("Snapshot recovery registry not initialized");
                        boolean progress=snapshotRecoveries.step(groupId,prepared); if (progress) helped.incrementAndGet(); return progress;
                    },snapshotHelp);
                } }
                else if (source==homeDirectory) { machine.role(RatisNodePlan.Role.TRANSACTION_HOME,(key,value) -> RatisTransactionStatuses.validateRecord(outer.cluster,outer.database,key,value)); machine.homeRecoveryReservations(outer.cluster,outer.database,homeColumn); machine.homeRecoveryData(data); }
                else { machine.role(RatisNodePlan.Role.SNAPSHOT_HOME,(key,value) -> RatisSnapshotFences.validateRecord(outer.cluster,outer.database,snapshotGroup,1,key,value)); machine.snapshotHomeStorage(outer.cluster,outer.database,snapshotColumn); machine.snapshotHomeData(data); }
                var storage=RaftStorage.newBuilder().setDirectory(root.resolve(descriptor.groupId().toString()).toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                        .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build();
                storage.initialize(); storages.add(storage);
                var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> switch (method.getName()) {
                    case "getId" -> peer.getId(); case "toString" -> "atomic-controlled-identity"; default -> throw new AssertionError(method);
                });
                machines.put(descriptor.groupId(),machine); machine.initialize(server,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
                var binding=catalogue.epochs.prepareSessionBinding(source,descriptor.groupId(),new OperationId(admin,1,source==directory ? groups.indexOf(descriptor.groupId()) : source==homeDirectory ? 2 : 3),grant,snapshotHome ? 2048 : 1024,-1);
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(append(descriptor,DistributedCommandCodec.encode(binding))).status());
            }
            if (snapshotHome) snapshotRecoveries=new RatisParticipantRecoveryRegistry(Files.createDirectory(root.resolve("snapshot-recovery")),homes,data,false,Set.copyOf(groups),RatisParticipantRecoveryRegistry.Limits.defaults());
            } catch (Exception | Error failure) {
                try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        static RangeDescriptor route(UUID group,byte[] lower,byte[] upper) {
            var replica=new ReplicaDescriptor(new UUID(114,1),new UUID(114,2),group,"atomic-fixture","127.0.0.1:12345",ReplicaDescriptor.Role.VOTER);
            return new RangeDescriptor(group,group,1,0,lower,upper,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        }
        RatisAtomicClient.Identity identity(int block) { return new RatisAtomicClient.Identity(new UUID(115,block+1),new OperationId(session,1,block*(RatisAtomicClient.CONTROL_SLOTS+1L)),new OperationId(session,1,block*(RatisAtomicClient.CONTROL_SLOTS+1L)+1)); }
        RatisAtomicClient client() { return new RatisAtomicClient(schema,homes,data,scheduler,RatisAtomicClient.Limits.defaults()); }
        int writes() { return submissions.get(); }
        RatisRoutedClient.Transport transport() {
            return new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException {
                    if (route.groupId().equals(homeGroup) && beforeHomeRead!=null) beforeHomeRead.run();
                    return ScopedTransactionRpc.pointValue(transaction(route,ScopedTransactionRpc.pointRequest(column,key),true),2*1024*1024);
                }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    rpcs.incrementAndGet(); int kind=ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                    if (kind==ScopedTransactionRpc.OPEN) opens.incrementAndGet();
                    if (kind==ScopedTransactionRpc.PARTICIPANT_AUTHORIZE && beforeAuthorize!=null) beforeAuthorize.run();
                    try { var message=Message.valueOf(ByteString.copyFrom(request)); var machine=machines.get(route.groupId());
                        return (fresh ? machine.queryLinearizable(message) : machine.query(message)).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                    } catch (Exception failure) { throw new IOException("Controlled transaction failed",failure); }
                }
                public byte[] snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException {
                    rpcs.incrementAndGet(); int kind=ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                    if (kind==ScopedTransactionRpc.SNAPSHOT_CAPTURE) { captureRequests.incrementAndGet(); if (beforeSnapshotCapture!=null) beforeSnapshotCapture.run(); }
                    if (kind==ScopedTransactionRpc.SNAPSHOT_RELEASE && beforeSnapshotRelease!=null) beforeSnapshotRelease.run();
                    if ((kind==ScopedTransactionRpc.SNAPSHOT_GET || kind==ScopedTransactionRpc.SNAPSHOT_PAGE) && beforeSnapshotRead!=null) beforeSnapshotRead.run();
                    assertEquals(route.replicas().getFirst(),owner);
                    try {
                        byte[] reply=machines.get(route.groupId()).queryStale(Message.valueOf(ByteString.copyFrom(request)),0).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                        if (kind==ScopedTransactionRpc.SNAPSHOT_CAPTURE && route.groupId().equals(loseCaptureGroup)) { loseCaptureGroup=null; throw new IOException("Lost native capture reply"); }
                        return reply;
                    } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException("Controlled owner query failed",failure); }
                }
                public RangeStateView stateView(RangeDescriptor route) throws IOException { return RangeStateView.decode(ScopedTransactionRpc.parse(transaction(route,ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]),true),ScopedTransactionRpc.MAX_REPLY_BYTES).body()); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return CommandResult.decode(append(route,DistributedCommandCodec.encode(command))); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException {
                    byte[] reply=append(route,bytes);
                    if (route.groupId().equals(losePrepareGroup) && RatisParticipantRecord.isParticipant(bytes) && !RatisParticipantRecord.isFinalization(bytes)) {
                        losePrepareGroup=null; throw new IOException("Lost applied PREPARE reply");
                    }
                    return reply;
                }
            };
        }
        synchronized byte[] append(RangeDescriptor route,byte[] bytes) throws IOException {
            try {
                var group=raftGroups.get(route.groupId()); var machine=machines.get(route.groupId());
                var context=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(group.getPeers().iterator().next().getId()).setGroupId(group.getGroupId())
                        .setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build());
                if (context.getException()!=null) throw new IOException("Controlled admission rejected",context.getException());
                machine.preAppendTransaction(context); context.initLogEntry(1,stores.get(route.groupId()).applied().index()+1); submissions.incrementAndGet();
                return machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray();
            } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
        }
        byte[] get(long column,Keys keys) throws IOException {
            var binding=schema.column(column); var encoded=schema.encodeLogicalKey(binding.columnId(),keys); byte[] raw=data.read(binding.columnId(),encoded.physicalKey());
            return binding.schema().variableLengthKeysCount()==0 ? raw : schema.decodeBucket(binding.columnId(),encoded.physicalKey(),raw).get(encoded.variableKeys());
        }
        void complete(RatisAtomicClient.Operation operation,CommandResult.Status status) throws Exception {
            var result=operation.completion().get(10,TimeUnit.SECONDS); assertEquals(status,result.status());
            if (operation.rejectedBeforeRegistration()) { assertEquals(CommandResult.Status.CONFLICT,status); assertThrows(IOException.class,operation::snapshot); return; }
            var saved=operation.snapshot(); assertTrue(saved.observedHome().fullyFinalized()); assertFalse(saved.observedHome().consumed());
            for (var participant:saved.participants()) { if (status==CommandResult.Status.SUCCESS) assertNotNull(participant.vote()); assertNotNull(participant.outcome()); }
        }
        public void close() throws Exception {
            if (snapshotHelp!=null) snapshotHelp.stop().get(5,TimeUnit.SECONDS);
            if (snapshotRecoveries!=null) snapshotRecoveries.close();
            proofs.stop().get(5,TimeUnit.SECONDS);
            for (var machine:machines.values()) { machine.close(); machine.awaitNativeReaders(Duration.ofSeconds(5)); }
            for (var storage:storages) storage.close(); if (data!=null) data.close(); if (homeClient!=null) homeClient.close(); if (snapshotClient!=null) snapshotClient.close(); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); catalogue.close();
        }
    }
    @Test void fourLogicalColumnsCommitTogetherWithOrderedResultsAndCompleteCollisionBuckets() throws Exception {
        try (var f=new Fixture(root); var client=f.client(); var operation=client.executeAtomic(root.resolve("commit.bin"),f.identity(0),changes(),absentIdentity(),TTL,RequestContext.batch())) {
            f.complete(operation,CommandResult.Status.SUCCESS); var result=operation.completion().get(5,TimeUnit.SECONDS);
            assertEquals(5,result.valueCount()); assertArrayEquals(new byte[]{9},result.value(0)); assertNull(result.value(1)); assertNull(result.value(2));
            assertArrayEquals(new byte[]{7},result.value(3)); assertArrayEquals(new byte[]{8},result.value(4));
            assertArrayEquals(new byte[]{1},f.get(71,key(2))); assertArrayEquals(new byte[]{9},f.get(72,key(200))); assertArrayEquals(new byte[]{7},f.get(73,key(2)));
            assertArrayEquals(new byte[]{5},f.get(74,member(200,"first"))); assertArrayEquals(new byte[]{8},f.get(74,member(200,"second")));
            var column=f.schema.column(74).columnId(); var physical=f.schema.encodeLogicalKey(column,member(200,"first")).physicalKey();
            assertEquals(2,f.schema.decodeBucket(column,physical,f.data.read(column,physical)).members().size());
            assertFalse(f.homes.read(f.identity(0).transaction()).consumed()); operation.consume(RequestContext.batch()).get(5,TimeUnit.SECONDS);
            assertTrue(f.homes.read(f.identity(0).transaction()).consumed()); assertEquals(2,f.opens.get());
        }
    }
    @Test void failedIdentityConditionPersistsPreflightConflictWithoutRegistrationOrPartialWrites() throws Exception {
        try (var f=new Fixture(root); var client=f.client()) {
            try (var seed=client.executeAtomic(root.resolve("seed.bin"),f.identity(0),changes(),absentIdentity(),TTL,RequestContext.batch())) { f.complete(seed,CommandResult.Status.SUCCESS); }
            var changes=List.of(RatisLogicalClient.Change.put(71,key(2),value(4),Mutation.ReturnMode.CURRENT),RatisLogicalClient.Change.put(72,key(200),value(4),Mutation.ReturnMode.CURRENT),
                    RatisLogicalClient.Change.delete(73,key(2),Mutation.ReturnMode.PREVIOUS),RatisLogicalClient.Change.put(74,member(200,"third"),value(4),Mutation.ReturnMode.CURRENT));
            Path path=root.resolve("conflict.bin"); int writes=f.writes();
            try (var conflict=client.executeAtomic(path,f.identity(1),changes,absentIdentity(),TTL,RequestContext.batch())) {
                f.complete(conflict,CommandResult.Status.CONFLICT); assertTrue(conflict.rejectedBeforeRegistration());
                assertNull(f.homes.read(f.identity(1).transaction())); assertEquals(writes,f.writes(),"False preflight emits no HOME, PREPARE or DATA mutation");
                for (var store:f.stores.values()) assertNull(store.preparedParticipant(f.identity(1).transaction()));
                assertArrayEquals(new byte[]{1},f.get(71,key(2))); assertArrayEquals(new byte[]{9},f.get(72,key(200))); assertArrayEquals(new byte[]{7},f.get(73,key(2)));
                assertNull(f.get(74,member(200,"third"))); assertArrayEquals(new byte[]{5},f.get(74,member(200,"first"))); assertArrayEquals(new byte[]{8},f.get(74,member(200,"second")));
                byte[] unconsumed=Files.readAllBytes(path); int calls=f.rpcs.get();
                conflict.consume(RequestContext.batch()).get(5,TimeUnit.SECONDS);
                assertFalse(Arrays.equals(unconsumed,Files.readAllBytes(path)),"Explicit consumption persists the local rejection acknowledgement");
                assertEquals(calls,f.rpcs.get()); assertEquals(writes,f.writes());
            }
            int opened=f.opens.get(),calls=f.rpcs.get(); byte[] consumed=Files.readAllBytes(path);
            try (var reopened=client.reopen(path,RequestContext.batch())) {
                f.complete(reopened,CommandResult.Status.CONFLICT); assertTrue(reopened.rejectedBeforeRegistration());
                reopened.consume(RequestContext.batch()).get(5,TimeUnit.SECONDS);
                assertArrayEquals(consumed,Files.readAllBytes(path),"Recovered consumption is idempotent");
                assertEquals(opened,f.opens.get()); assertEquals(calls,f.rpcs.get(),"Known rejected intent reopens without remote work"); assertEquals(writes,f.writes());
            }
        }
    }
    @Test void conflictingIdentityWriteAfterStagingDurablyAbortsBothGroupsWithoutPartialEffects() throws Exception {
        try (var f=new Fixture(root); var client=f.client()) {
            var intervened=new java.util.concurrent.atomic.AtomicBoolean();
            f.beforeAuthorize=() -> {
                if (!intervened.compareAndSet(false,true)) return;
                var column=f.schema.column(71).columnId(); var physical=f.schema.encodeLogicalKey(column,key(2)).physicalKey();
                var route=f.directory.route(column,physical);
                var concurrent=new DistributedCommand(route.groupId(),route.rangeEpoch(),f.schema.digest(),new OperationId(f.session,1,1000),
                        List.of(new Mutation(Mutation.Kind.PUT,column,physical,new byte[]{9})));
                try { assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepare(concurrent.operationId(),concurrent.mutations())).status()); }
                catch (IOException unavailable) { throw new java.io.UncheckedIOException(unavailable); }
            };
            try (var operation=client.executeAtomic(root.resolve("staged-conflict.bin"),f.identity(0),changes(),absentIdentity(),TTL,RequestContext.batch())) {
                f.complete(operation,CommandResult.Status.CONFLICT); assertTrue(intervened.get()); assertFalse(operation.rejectedBeforeRegistration());
                var saved=operation.snapshot(); assertEquals(TransactionStatus.Phase.ABORT,saved.observedHome().phase());
                assertTrue(saved.observedHome().fullyFinalized()); assertFalse(saved.observedHome().consumed());
                assertArrayEquals(new byte[]{9},f.get(71,key(2)),"The concurrent identity write survives the aborted transaction");
                assertNull(f.get(72,key(200))); assertNull(f.get(73,key(2)));
                assertNull(f.get(74,member(200,"first"))); assertNull(f.get(74,member(200,"second")));
                assertFalse(f.homes.read(f.identity(0).transaction()).consumed());
            }
        }
    }
    @Test void lostSecondPrepareReplyReopensTheFrozenJournalWithoutReplacementCaptures() throws Exception {
        try (var f=new Fixture(root)) {
            Path path=root.resolve("lost.bin"); List<UUID> captures; int opened;
            f.losePrepareGroup=f.groups.getLast();
            try (var client=f.client(); var operation=client.executeAtomic(path,f.identity(0),changes(),absentIdentity(),TTL,RequestContext.batch())) {
                var failure=assertThrows(ExecutionException.class,() -> operation.completion().get(10,TimeUnit.SECONDS)); assertInstanceOf(IOException.class,failure.getCause());
                var saved=operation.snapshot(); captures=saved.participants().stream().map(p -> p.intent().captureId()).toList(); opened=f.opens.get();
                assertEquals(2,opened); for (var group:f.groups) assertNotNull(f.stores.get(group).preparedParticipant(f.identity(0).transaction()));
            }
            try (var client=f.client(); var resumed=client.reopen(path,RequestContext.batch())) {
                f.complete(resumed,CommandResult.Status.SUCCESS); assertEquals(captures,resumed.snapshot().participants().stream().map(p -> p.intent().captureId()).toList());
                assertEquals(opened,f.opens.get(),"Recovery never substitutes new server captures for frozen lease IDs");
                resumed.consume(RequestContext.batch()).get(5,TimeUnit.SECONDS); assertTrue(resumed.snapshot().observedHome().consumed());
                assertArrayEquals(new byte[]{1},f.get(71,key(2))); assertArrayEquals(new byte[]{8},f.get(74,member(200,"second")));
            }
        }
    }
}
