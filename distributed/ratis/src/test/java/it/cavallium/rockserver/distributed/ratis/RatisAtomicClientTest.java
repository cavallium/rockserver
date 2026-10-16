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
        final RatisProofQueryService proofs=new RatisProofQueryService(RatisProofFixtures.admission());
        final RatisProofQueryService rerouteProofs=RatisProofQueryService.operationRerouteLane(RatisProofFixtures.admission());
        final RatisSnapshotHelpService snapshotHelp;
        RatisParticipantRecoveryRegistry snapshotRecoveries;
        final AtomicInteger helped=new AtomicInteger();
        final Map<UUID,ReplicatedRangeStore> stores=new LinkedHashMap<>();
        final Map<UUID,RatisRangeStateMachine> machines=new LinkedHashMap<>();
        final Map<UUID,RaftGroup> raftGroups=new HashMap<>();
        final List<RaftStorage> storages=new ArrayList<>();
        final AtomicInteger opens=new AtomicInteger(),captureCloses=new AtomicInteger(),submissions=new AtomicInteger(),rpcs=new AtomicInteger(),captureRequests=new AtomicInteger();
        volatile Runnable beforeAuthorize,beforeHomeRead,beforeHomeCommand,beforeSnapshotCapture,beforeSnapshotRead,beforeSnapshotRelease,beforePromotion;
        volatile UUID losePrepareGroup,loseCaptureGroup;
        volatile TransactionStatus allowedReservation;
        volatile boolean losePromotion;
        final AtomicInteger promotionChecks=new AtomicInteger(),promotionSubmissions=new AtomicInteger();
        final SessionEpochLedger ledger;
        final SessionEpochLedger.Grant grant;
        Fixture(Path root) throws Exception { this(root,false); }
        Fixture(Path root,boolean snapshotHome) throws Exception { this(root,snapshotHome,Map.of()); }
        Fixture(Path root,boolean snapshotHome,Map<Long,ColumnSchema> schemaOverrides) throws Exception {
            this(root,snapshotHome,schemaOverrides,null);
        }
        Fixture(Path root,boolean snapshotHome,Map<Long,ColumnSchema> schemaOverrides,it.cavallium.rockserver.core.impl.rocksdb.PoolSnapshotAdmission.Limits pins) throws Exception {
            this(root,snapshotHome,schemaOverrides,pins,(route,machine)->{});
        }
        Fixture(Path root,boolean snapshotHome,Map<Long,ColumnSchema> schemaOverrides,it.cavallium.rockserver.core.impl.rocksdb.PoolSnapshotAdmission.Limits pins,
                java.util.function.BiConsumer<RangeDescriptor,RatisRangeStateMachine> configureData) throws Exception {
            this.root=root; Files.createDirectories(root); catalogue=outer.new Fixture(root.resolve("catalogue"),pins);
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
                else if (source==homeDirectory) { machine.role(RatisNodePlan.Role.TRANSACTION_HOME,(key,value) -> RatisTransactionStatuses.validateRecord(outer.cluster,outer.database,key,value)); machine.homeRecoveryReservations(outer.cluster,outer.database,homeColumn); machine.homeRecoveryData(data); machine.operationRerouteQueries(rerouteProofs); machine.operationRerouteAdmission(command -> {
                    var next=TransactionStatus.decode(command.mutations().getFirst().value());
                    if(allowedReservation==null || !allowedReservation.equals(next))throw new IOException("Unqualified fixture reservation");
                },command -> {
                    if(beforePromotion!=null)beforePromotion.run();
                    var expected=TransactionStatus.decode(command.predicates().getFirst().expectedValue());var next=TransactionStatus.decode(command.mutations().getFirst().value());
                    if(allowedReservation==null || !allowedReservation.equals(expected) || !allowedReservation.origin().equals(next.origin()) || next.reserved() || next.generation()!=1 || opens.get()!=2)throw new IOException("Fixture promotion did not preserve reserved captured plan");
                    promotionChecks.incrementAndGet();
                }); }
                else { machine.role(RatisNodePlan.Role.SNAPSHOT_HOME,(key,value) -> RatisSnapshotFences.validateRecord(outer.cluster,outer.database,snapshotGroup,1,key,value)); machine.snapshotHomeStorage(outer.cluster,outer.database,snapshotColumn); machine.snapshotHomeData(data); }
                var storage=RaftStorage.newBuilder().setDirectory(root.resolve(descriptor.groupId().toString()).toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                        .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build();
                storage.initialize(); storages.add(storage);
                var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> switch (method.getName()) {
                    case "getId" -> peer.getId(); case "toString" -> "atomic-controlled-identity"; default -> throw new AssertionError(method);
                });
                if(source==directory)configureData.accept(descriptor,machine);
                machines.put(descriptor.groupId(),machine); machine.initialize(server,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
                var binding=catalogue.epochs.prepareSessionBinding(source,descriptor.groupId(),new OperationId(admin,1,source==directory ? groups.indexOf(descriptor.groupId()) : source==homeDirectory ? 2 : 3),grant,snapshotHome ? 2048 : 1024,-1);
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(append(descriptor,DistributedCommandCodec.encode(binding))).status());
            }
            if (snapshotHome) snapshotRecoveries=new RatisParticipantRecoveryRegistry(Files.createDirectory(root.resolve("snapshot-recovery")), homes, Map.of(data.directory().domainId(), data), group -> data.directory(), false, Set.copyOf(groups), RatisParticipantRecoveryRegistry.Limits.defaults());
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
        RatisAtomicClient.Identity separateIdentity(long time) throws Exception {
            var before=catalogue.epochs.read();
            if(time>before.logicalTimeMillis()) {
                var advanced=before.advanceTime(time);
                assertEquals(CommandResult.Status.SUCCESS,catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(2),before,advanced)).status());before=advanced;
            }
            UUID controls=new UUID(116,1);var issued=before.issue(controls,time+2000);
            assertEquals(CommandResult.Status.SUCCESS,catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(3),before,issued)).status());
            var controlGrant=issued.grants().getLast();int n=0;
            for(var source:List.of(directory,homeDirectory))for(var descriptor:source.ranges()) {
                var binding=catalogue.epochs.prepareSessionBinding(source,descriptor.groupId(),new OperationId(admin,issued.minimumEpoch(),4+n++),controlGrant,1024,-1);
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(append(descriptor,DistributedCommandCodec.encode(binding))).status());
            }
            return new RatisAtomicClient.Identity(new UUID(116,2),new OperationId(session,grant.epoch(),0),new OperationId(controls,controlGrant.epoch(),0));
        }
        RatisAtomicClient client() { return new RatisAtomicClient(RatisTransactionBindings.of(schema,data.directory(schema),data),homes,scheduler,RatisAtomicClient.Limits.defaults()); }
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
                    if (kind==ScopedTransactionRpc.CLOSE)captureCloses.incrementAndGet();
                    if (kind==ScopedTransactionRpc.PARTICIPANT_AUTHORIZE && beforeAuthorize!=null) beforeAuthorize.run();
                    try { var message=Message.valueOf(ByteString.copyFrom(request)); var machine=machines.get(route.groupId());
                        return (fresh ? machine.queryLinearizable(message) : machine.query(message)).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                    } catch (Exception failure) { throw new IOException("Controlled transaction failed",failure); }
                }
                public CompletableFuture<byte[]> readAsync(RangeDescriptor route,UUID column,byte[] key)throws IOException {
                    if(route.groupId().equals(homeGroup) && beforeHomeRead!=null)beforeHomeRead.run();
                    return transactionAsync(route,ScopedTransactionRpc.pointRequest(column,key),true).thenApply(reply->ScopedTransactionRpc.pointValue(reply,2*1024*1024));
                }
                public CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] request,boolean fresh)throws IOException {
                    rpcs.incrementAndGet();int kind=ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                    if(kind==ScopedTransactionRpc.OPEN)opens.incrementAndGet();if(kind==ScopedTransactionRpc.CLOSE)captureCloses.incrementAndGet();
                    if(kind==ScopedTransactionRpc.PARTICIPANT_AUTHORIZE && beforeAuthorize!=null)beforeAuthorize.run();
                    var input=Message.valueOf(ByteString.copyFrom(request));var target=machines.get(route.groupId());
                    return (fresh?target.queryLinearizable(input):target.query(input)).thenApply(reply->reply.getContent().toByteArray());
                }
                public CompletableFuture<CommandResult> commandAsync(RangeDescriptor route,DistributedCommand command)throws IOException {
                    if(route.groupId().equals(homeGroup) && beforeHomeCommand!=null && command.mutations().size()==1 && command.predicates().size()==1 && command.predicates().getFirst().expectedValue()==null){var initial=TransactionStatus.decode(command.mutations().getFirst().value());if(initial.origin()==null && initial.phase()==TransactionStatus.Phase.PREPARING && initial.generation()==0)beforeHomeCommand.run();}
                    if(route.groupId().equals(homeGroup) && command.mutations().size()==1) {var next=TransactionStatus.decode(command.mutations().getFirst().value());if(next.origin()!=null && next.phase()==TransactionStatus.Phase.PREPARING && next.generation()==1)promotionSubmissions.incrementAndGet();}
                    return appendAsync(route,DistributedCommandCodec.encode(command)).thenApply(bytes->{
                        var result=CommandResult.decode(bytes);
                        if(losePromotion && route.groupId().equals(homeGroup) && TransactionStatus.decode(command.mutations().getFirst().value()).origin()!=null && TransactionStatus.decode(command.mutations().getFirst().value()).generation()==1){losePromotion=false;throw new CompletionException(new IOException("Lost applied promotion reply"));}
                        return result;
                    });
                }
                public CompletableFuture<byte[]> participantAsync(RangeDescriptor route,byte[] bytes)throws IOException {
                    return appendAsync(route,bytes).thenApply(reply->{if(route.groupId().equals(losePrepareGroup) && RatisParticipantRecord.isParticipant(bytes) && !RatisParticipantRecord.isFinalization(bytes)){losePrepareGroup=null;throw new CompletionException(new IOException("Lost applied PREPARE reply"));}return reply;});
                }
                public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException {
                    rpcs.incrementAndGet(); int kind=ScopedTransactionRpc.kind(request,Math.max(ScopedTransactionRpc.MAX_REQUEST_BYTES,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES));
                    if (kind==ScopedTransactionRpc.SNAPSHOT_CAPTURE) { captureRequests.incrementAndGet(); if (beforeSnapshotCapture!=null) beforeSnapshotCapture.run(); }
                    if (kind==ScopedTransactionRpc.SNAPSHOT_RELEASE && beforeSnapshotRelease!=null) beforeSnapshotRelease.run();
                    if ((kind==ScopedTransactionRpc.SNAPSHOT_GET || kind==ScopedTransactionRpc.SNAPSHOT_PAGE) && beforeSnapshotRead!=null) beforeSnapshotRead.run();
                    if(owner!=null)assertEquals(route.replicas().getFirst(),owner);var machine=machines.get(route.groupId());var message=Message.valueOf(ByteString.copyFrom(request));
                    return (owner==null ? machine.queryLinearizable(message) : machine.queryStale(message,0)).thenApply(reply->{if(kind==ScopedTransactionRpc.SNAPSHOT_CAPTURE && route.groupId().equals(loseCaptureGroup)){loseCaptureGroup=null;throw new java.util.concurrent.CompletionException(new IOException("Lost native capture reply"));}return reply.getContent().toByteArray();});
                }
                public RangeStateView stateView(RangeDescriptor route) throws IOException { return RangeStateView.decode(ScopedTransactionRpc.parse(transaction(route,ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]),true),ScopedTransactionRpc.MAX_REPLY_BYTES).body()); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException {
                    if(route.groupId().equals(homeGroup) && beforeHomeCommand!=null && command.mutations().size()==1 && command.predicates().size()==1 && command.predicates().getFirst().expectedValue()==null){var initial=TransactionStatus.decode(command.mutations().getFirst().value());if(initial.origin()==null && initial.phase()==TransactionStatus.Phase.PREPARING && initial.generation()==0)beforeHomeCommand.run();}
                    if(route.groupId().equals(homeGroup) && command.mutations().size()==1) {var next=TransactionStatus.decode(command.mutations().getFirst().value());if(next.origin()!=null && next.phase()==TransactionStatus.Phase.PREPARING && next.generation()==1)promotionSubmissions.incrementAndGet();}
                    var result=CommandResult.decode(append(route,DistributedCommandCodec.encode(command)));
                    if(losePromotion && route.groupId().equals(homeGroup) && TransactionStatus.decode(command.mutations().getFirst().value()).origin()!=null && TransactionStatus.decode(command.mutations().getFirst().value()).generation()==1) {losePromotion=false;throw new IOException("Lost applied promotion reply");}
                    return result;
                }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException {
                    byte[] reply=append(route,bytes);
                    if (route.groupId().equals(losePrepareGroup) && RatisParticipantRecord.isParticipant(bytes) && !RatisParticipantRecord.isFinalization(bytes)) {
                        losePrepareGroup=null; throw new IOException("Lost applied PREPARE reply");
                    }
                    return reply;
                }
            };
        }
        private CompletableFuture<Void> appendTail=CompletableFuture.completedFuture(null);
        CompletableFuture<byte[]> appendAsync(RangeDescriptor route,byte[] bytes) {
            byte[] copy=bytes.clone();var result=new CompletableFuture<byte[]>();final CompletableFuture<Void> prior;
            synchronized(this){prior=appendTail;appendTail=result.handle((value,failure)->null);}
            prior.whenComplete((ignored,previousFailure)->{
                try {
                    var group=raftGroups.get(route.groupId());var machine=machines.get(route.groupId());
                    var context=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(group.getPeers().iterator().next().getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(copy))).build());
                    if(context.getException()!=null)throw new IOException("Controlled admission rejected",context.getException());
                    machine.preAppendTransaction(context);context.initLogEntry(1,stores.get(route.groupId()).applied().index()+1);submissions.incrementAndGet();
                    machine.applyTransaction(context).whenComplete((reply,failure)->{if(failure==null)result.complete(reply.getContent().toByteArray());else result.completeExceptionally(failure);});
                }catch(Throwable failure){result.completeExceptionally(failure);}
            });return result;
        }
        byte[] append(RangeDescriptor route,byte[] bytes)throws IOException {
            try{return appendAsync(route,bytes).get(5,TimeUnit.SECONDS);}catch(ExecutionException failure){if(failure.getCause() instanceof IOException io)throw io;throw new IOException(failure.getCause());}catch(Exception failure){throw new IOException(failure);}
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
            rerouteProofs.stop().get(5,TimeUnit.SECONDS);proofs.stop().get(5,TimeUnit.SECONDS);
            for (var machine:machines.values()) { machine.close(); machine.awaitNativeReaders(Duration.ofSeconds(5)); }
            for (var storage:storages) storage.close(); if (data!=null) data.close(); if (homeClient!=null) homeClient.close(); if (snapshotClient!=null) snapshotClient.close(); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); catalogue.close();
        }
    }
    static RatisOperationRerouteAdmissionRpc.Evidence operationEvidence(RatisAtomicClient.Operation operation)throws Exception {var field=operation.getClass().getDeclaredField("journal");field.setAccessible(true);return ((RatisDurableTransaction)field.get(operation)).promotionEvidence();}
    record ReservedInput(TransactionStatus reserved,RatisAtomicClient.Identity identity,RatisOperationRerouteAdmissionRpc.Evidence evidence) {}
    ReservedInput reservedInput(Fixture f,RatisAtomicClient.Identity allocated)throws Exception {return reservedInput(f,allocated,-1);}
    ReservedInput reservedInput(Fixture f,RatisAtomicClient.Identity allocated,long acknowledgedThrough)throws Exception {
        // Controlled source authority is supplied by the fixture callback; these canonical wire
        // fields exercise persisted proof plumbing, while native target captures and HOME are real.
        var owner=f.directory.ranges().getFirst().replicas().getFirst();var parent=new RangeDescriptor(new UUID(120,1),new UUID(120,2),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
        var source=new RangeDirectory(f.schema.clusterId(),f.schema.databaseId(),f.schema.domainId(),f.schema.schemaEpoch(),f.schema.digest(),0,f.schema.columnIds(),1,List.of(parent));
        var left=new RangeDescriptor(new UUID(120,3),new UUID(120,4),1,0,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(owner));var right=new RangeDescriptor(new UUID(120,5),new UUID(120,6),1,0,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
        var frozenTarget=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),1,source.columns(),source.routingPrefixBytes(),List.of(left,right));var definition=new TopologyChange.Definition(new UUID(120,7),source,frozenTarget);
        var physical=RatisLogicalClient.plan(scopeColumnId -> f.schema,changes(),absentIdentity(),RatisTransactionPlan.MAX_BYTES);
        var original=new DistributedCommand(parent.groupId(),1,f.schema.digest(),allocated.business(),physical.mutations(),physical.predicates(),acknowledgedThrough);
        var freeze=new NativeTopologyFreeze(definition.header(),parent.groupId(),new ApplyPosition(1,1),NativeTopologyFreeze.boundsDigest(null,null),new byte[32]);var lineage=new DistributedCdcCheckpoint.Lineage(parent.rangeId(),parent.groupId(),1);
        var request=new RatisTopologyOperationOutcomeRpc.Request(source.clusterId(),source.databaseId(),source.domainId(),lineage,allocated.business(),original.receiptDigest());var absence=new RatisTopologyOperationOutcomeRpc.Reply(request,RatisTopologyOperationOutcomeRpc.Outcome.ABSENT,freeze.position(),freeze,1,0,new byte[]{1},null,null);
        var evidence=new RatisOperationRerouteAdmissionRpc.Evidence(original,source,f.directory,absence,List.of());
        var origin=new OperationRerouteOrigin(source.clusterId(),source.databaseId(),source.domainId(),lineage,allocated.business(),original.receiptDigest(),OperatorUpload.sha256(freeze.encode()),absence.cut(),f.directory.payloadDigest(),RatisOperationRerouteAdmissionRpc.normalizedDigest(original,f.directory),RatisOperationRerouteAdmissionRpc.targetParticipantCount(original,f.directory),allocated.firstControl());
        var reserved=TransactionStatus.reserve(origin, DomainBinding.of(f.directory));return new ReservedInput(reserved,new RatisAtomicClient.Identity(reserved.transactionId(),allocated.business(),allocated.firstControl()),evidence);
    }
    @Test void inlineDriverOnSingleMutationWorkerReopensSameNativeCapturesWithoutNestedScheduling()throws Exception {
        try(var f=new Fixture(root);var client=f.client()) {
            var allocated=f.separateIdentity(10);var input=reservedInput(f,allocated);f.allowedReservation=input.reserved();var plan=RatisTransactionPlan.fromOriginal(f.directory,input.evidence().original());
            assertEquals(CommandResult.Status.SUCCESS,f.homes.execute(f.homes.prepare(input.identity().firstControl(),null,input.reserved()),input.evidence()).status());
            Path path=root.resolve("inline-lost.bin");var checks=new AtomicInteger();var first=new CompletableFuture<Void>();f.losePromotion=true;
            f.scheduler.executor(WorkloadProfile.BATCH,OperationFamily.MUTATION,Long.MAX_VALUE).execute(()-> {
                var worker=Thread.currentThread();
                try(var operation=client.executeReservedPlanInline(path,input.identity(),input.reserved(),plan,input.evidence(),TTL,RequestContext.batch(),()->{assertSame(worker,Thread.currentThread());checks.incrementAndGet();})) {
                    assertTrue(operation.completion().isDone());assertTrue(operation.completion().isCompletedExceptionally());assertEquals(1,client.stats().operations());assertTrue(client.stats().bytes()>0);assertThrows(IOException.class,()->client.reopen(path,RequestContext.batch()));
                }catch(Throwable failure){first.completeExceptionally(failure);return;}first.complete(null);
            },1<<20);
            first.get(10,TimeUnit.SECONDS);assertEquals(0,f.captureCloses.get());
            var second=new CompletableFuture<Void>();
            f.scheduler.executor(WorkloadProfile.BATCH,OperationFamily.MUTATION,Long.MAX_VALUE).execute(()-> {
                try(var operation=client.reopenInline(path,RequestContext.batch(),()->checks.incrementAndGet())) {
                    assertTrue(operation.completion().isDone());var result=operation.resumeInline(RequestContext.batch(),()->checks.incrementAndGet());assertEquals(CommandResult.Status.SUCCESS,result.status());assertEquals(5,result.valueCount());assertArrayEquals(new byte[]{9},result.value(0));assertNull(result.value(1));assertNull(result.value(2));assertArrayEquals(new byte[]{7},result.value(3));assertArrayEquals(new byte[]{8},result.value(4));assertEquals(input.reserved().origin(),operation.snapshot().initial().origin());
                }catch(Throwable failure){second.completeExceptionally(failure);return;}second.complete(null);
            },1<<20);
            second.get(10,TimeUnit.SECONDS);assertEquals(2,f.opens.get());assertEquals(2,f.captureCloses.get());assertTrue(checks.get()>0);assertEquals(0,client.stats().operations());
        }
    }
    @Test void inlineCancellationKeepsExclusiveChargeUntilActualTaskFinishesAndOwnerCloses()throws Exception {
        try(var f=new Fixture(root);var client=f.client()) {
            var allocated=f.separateIdentity(10);var input=reservedInput(f,allocated);f.allowedReservation=input.reserved();var plan=RatisTransactionPlan.fromOriginal(f.directory,input.evidence().original());
            assertEquals(CommandResult.Status.SUCCESS,f.homes.execute(f.homes.prepare(input.identity().firstControl(),null,input.reserved()),input.evidence()).status());
            var finished=new CompletableFuture<Void>();Path path=root.resolve("inline-cancel.bin");
            f.scheduler.executor(WorkloadProfile.BATCH,OperationFamily.MUTATION,Long.MAX_VALUE).execute(()-> {
                try(var operation=client.executeReservedPlanInline(path,input.identity(),input.reserved(),plan,input.evidence(),TTL,RequestContext.batch(),()->{assertEquals(1,client.stats().operations());throw new IOException("Outer admitted task cancelled");})) {
                    assertTrue(operation.completion().isDone());assertTrue(operation.completion().isCompletedExceptionally());assertEquals(1,client.stats().operations());assertEquals(0,f.opens.get());assertFalse(Files.exists(path));assertThrows(IOException.class,()->client.reopen(path,RequestContext.batch()));
                }catch(Throwable failure){finished.completeExceptionally(failure);return;}finished.complete(null);
            },1<<20);
            finished.get(10,TimeUnit.SECONDS);assertEquals(0,client.stats().operations());assertEquals(0,client.stats().bytes());assertEquals(0,f.captureCloses.get());assertEquals(input.reserved(),f.homes.read(input.identity().transaction()));
        }
    }
    @Test void participantAuthorizationKeepsSameAckAndRejectsChangedAckWithoutLosingOriginalHandle()throws Exception {
        try(var f=new Fixture(root)) {
            var allocation=f.separateIdentity(10);var identity=new RatisAtomicClient.Identity(new UUID(121,1),new OperationId(f.session,1,2),allocation.firstControl());var input=reservedInput(f,identity,0);var plan=RatisTransactionPlan.fromOriginal(f.directory,input.evidence().original());
            var captures=new ArrayList<RatisRoutedClient.ScopedTransaction>();var accepted=new HashMap<UUID,DistributedCommand>();var prepare=new HashMap<UUID,OperationId>();var finalize=new HashMap<UUID,OperationId>();var ids=new HashMap<UUID,UUID>();int n=0;
            try {
                for(var part:plan.partitions()) {var capture=f.data.openTransaction(part.scope(),TTL);captures.add(capture);var staged=capture.stage(part.command().mutations(),part.command().predicates());accepted.put(part.route().groupId(),capture.participantIntent(part.command(),staged));ids.put(part.route().groupId(),capture.captureId());prepare.put(part.route().groupId(),new OperationId(identity.firstControl().sessionId(),identity.firstControl().epoch(),n));finalize.put(part.route().groupId(),new OperationId(identity.firstControl().sessionId(),identity.firstControl().epoch(),64+n++));}
                var frozen=plan.accept(identity.transaction(),accepted,prepare,finalize,ids);assertEquals(CommandResult.Status.SUCCESS,f.homes.execute(f.homes.prepare(new OperationId(identity.firstControl().sessionId(),identity.firstControl().epoch(),128),null,frozen.initial())).status());
                var capture=captures.getFirst();var original=accepted.get(capture.descriptor().groupId());var control=prepare.get(capture.descriptor().groupId());
                var raw=new DistributedCommand(original.groupId(),original.rangeEpoch(),original.schemaDigest(),original.operationId(),original.mutations(),original.predicates(),1);var changed=DistributedCommand.acceptedTransaction(raw,original.acceptance());assertThrows(IOException.class,()->capture.authorizeParticipant(changed,control,identity.transaction()),"HOME bound the original ACK before authorization");
                var first=capture.authorizeParticipant(original,control,identity.transaction());assertEquals(0,first.preparation().business().acknowledgedThrough());assertArrayEquals(first.bytes(),capture.authorizeParticipant(original,control,identity.transaction()).bytes());
                assertThrows(IOException.class,()->capture.authorizeParticipant(changed,control,identity.transaction()));assertArrayEquals(first.bytes(),capture.authorizeParticipant(original,control,identity.transaction()).bytes());
                for(var group:f.groups)assertNull(f.stores.get(group).preparedParticipant(identity.transaction()),"Authorizing the retained image does not append PREPARE or USER effects");assertNull(f.get(71,key(2)));
            }finally{for(var capture:captures)capture.close();}
        }
    }
    @Test void originalPiggybackAckMakesBeyondWindowBusinessEligibleOnBothNativeChildren()throws Exception {
        try(var f=new Fixture(root);var client=f.client()) {
            var controls=f.separateIdentity(10);var allocated=new RatisAtomicClient.Identity(controls.transaction(),new OperationId(f.session,1,1040),controls.firstControl());var input=reservedInput(f,allocated,16);f.allowedReservation=input.reserved();var plan=RatisTransactionPlan.fromOriginal(f.directory,input.evidence().original());
            for(var part:plan.partitions()){assertEquals(16,part.command().acknowledgedThrough());assertEquals(allocated.business(),part.command().operationId());}
            assertEquals(CommandResult.Status.SUCCESS,f.homes.execute(f.homes.prepare(input.identity().firstControl(),null,input.reserved()),input.evidence()).status());
            try(var operation=client.executeReservedPlan(root.resolve("acked-children.bin"),input.identity(),input.reserved(),plan,input.evidence(),TTL,RequestContext.batch())) {
                f.complete(operation,CommandResult.Status.SUCCESS);assertEquals(5,operation.completion().get(5,TimeUnit.SECONDS).valueCount());for(var participant:operation.snapshot().participants()){assertTrue(participant.vote().prepared());assertEquals(16,participant.intent().acceptedBusiness().acknowledgedThrough());assertEquals(allocated.business(),participant.intent().acceptedBusiness().operationId());}assertArrayEquals(new byte[]{1},f.get(71,key(2)));assertArrayEquals(new byte[]{8},f.get(74,member(200,"second")));
                var column=f.schema.column(71).columnId();var physical=f.schema.encodeLogicalKey(column,key(3)).physicalKey();var follow=new OperationId(f.session,1,1041);
                // Child 2PC uses the ACK for eligibility; original-parent publication owns receipt GC.
                var rejected=assertThrows(IOException.class,()->f.data.execute(f.data.prepare(follow,List.of(new Mutation(Mutation.Kind.PUT,column,physical,new byte[]{99})))));
                assertEquals("Session admission rejected: CONFLICT",rejected.getCause().getMessage());assertNull(f.get(71,key(3)));
            }
        }
    }
    @Test void reservedPlanVerifiesBeforeCapturePromotesAndReopensLostOriginalPromotion() throws Exception {
        try(var f=new Fixture(root)) {
            var allocated=f.separateIdentity(10);var plan=RatisTransactionPlan.logical(List.of(f.schema),List.of(f.directory),allocated.business(),changes(),absentIdentity());
            var input=reservedInput(f,allocated);var reserved=input.reserved();var identity=input.identity();var origin=reserved.origin();var evidence=input.evidence();f.allowedReservation=reserved;
            try(var client=f.client();var missing=client.executeReservedPlan(root.resolve("missing-reservation.bin"),identity,reserved,plan,evidence,TTL,RequestContext.batch())) {assertInstanceOf(IOException.class,assertThrows(ExecutionException.class,()->missing.completion().get(5,TimeUnit.SECONDS)).getCause());assertEquals(0,f.opens.get());}
            assertEquals(CommandResult.Status.SUCCESS,f.homes.execute(f.homes.prepare(identity.firstControl(),null,reserved),evidence).status());assertEquals(reserved,f.homes.read(identity.transaction()));
            var changed=RatisTransactionPlan.logical(List.of(f.schema),List.of(f.directory),allocated.business(),List.of(RatisLogicalClient.Change.put(71,key(3),value(5),Mutation.ReturnMode.NONE)),List.of());
            try(var client=f.client()){assertThrows(IllegalArgumentException.class,()->client.executeReservedPlan(root.resolve("changed-plan.bin"),identity,reserved,changed,evidence,TTL,RequestContext.batch()));assertEquals(0,f.opens.get());}
            Path path=root.resolve("reserved-lost.bin");f.losePromotion=true;List<UUID> originalCaptures;byte[] originalEvidence;
            try(var client=f.client();var operation=client.executeReservedPlan(path,identity,reserved,plan,evidence,TTL,RequestContext.batch())) {
                assertInstanceOf(IOException.class,assertThrows(ExecutionException.class,()->operation.completion().get(10,TimeUnit.SECONDS)).getCause());
                originalCaptures=operation.snapshot().participants().stream().map(p->p.intent().captureId()).toList();
                var initial=operation.snapshot().initial();assertEquals(1,initial.generation());assertEquals(origin,initial.origin());assertFalse(initial.reserved());
            }
            assertEquals(0,f.captureCloses.get(),"Closing an unresolved forced journal preserves its TTL-owned original captures");
            try(var saved=RatisDurableTransaction.reopen(path,f.homes,RatisTransactionBindings.of(f.data))){originalEvidence=saved.promotionEvidence().encode();assertArrayEquals(evidence.encode(),new RatisOperationRerouteAdmissionRpc.Evidence(saved.promotionEvidence().original(),saved.promotionEvidence().sourceDirectory(),saved.promotionEvidence().targetDirectory(),saved.promotionEvidence().absence(),List.of()).encode());}
            try(var client=f.client();var resumed=client.reopen(path,RequestContext.batch())) {
                f.complete(resumed,CommandResult.Status.SUCCESS);assertArrayEquals(originalEvidence,operationEvidence(resumed).encode(),"Reopen reconstructs exact original proof plus existing accepted intents");assertEquals(originalCaptures,resumed.snapshot().participants().stream().map(p->p.intent().captureId()).toList());assertEquals(2,f.captureCloses.get());assertEquals(origin,resumed.snapshot().initial().origin());assertEquals(identity.business(),resumed.snapshot().initial().operation());assertEquals(2,f.opens.get());assertTrue(f.promotionChecks.get()>0);resumed.consume(RequestContext.batch()).get(5,TimeUnit.SECONDS);
            }
        }
    }
    @Test void expiredBusinessAfterCapturesAbortsExactReservationWithoutReplacementPromotion() throws Exception {
        try(var f=new Fixture(root)) {
            var allocated=f.separateIdentity(10);var plan=RatisTransactionPlan.logical(List.of(f.schema),List.of(f.directory),allocated.business(),changes(),absentIdentity());var input=reservedInput(f,allocated);var reserved=input.reserved();var identity=input.identity();var origin=reserved.origin();var evidence=input.evidence();f.allowedReservation=reserved;
            assertEquals(CommandResult.Status.SUCCESS,f.homes.execute(f.homes.prepare(identity.firstControl(),null,reserved),evidence).status());
            var once=new java.util.concurrent.atomic.AtomicBoolean();
            f.beforePromotion=()-> {
                if(!once.compareAndSet(false,true))return;assertEquals(2,f.opens.get());
                try {
                    var before=f.catalogue.epochs.read();var expired=before.advanceTime(1000);
                    assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(4),before,expired)).status());
                    var binding=f.catalogue.epochs.prepareSessionBinding(f.homeDirectory,f.homeGroup,new OperationId(f.admin,expired.minimumEpoch(),8),expired.grants().getLast(),1024,-1);
                    assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.append(f.homeDirectory.ranges().getFirst(),DistributedCommandCodec.encode(binding))).status());
                }catch(Exception failure){throw new RuntimeException(failure);}
            };
            Path path=root.resolve("expired-reserved-promotion.bin");
            try(var client=f.client();var operation=client.executeReservedPlan(path,identity,reserved,plan,evidence,TTL,RequestContext.batch())) {
                assertEquals(CommandResult.Status.SESSION_EXPIRED,operation.completion().get(10,TimeUnit.SECONDS).status());
                assertEquals(reserved.abortReservation(TransactionStatus.NoEffectOutcome.SESSION_EXPIRED),f.homes.read(identity.transaction()));assertEquals(1,f.promotionSubmissions.get());
                for(var group:f.groups)assertNull(f.stores.get(group).preparedParticipant(identity.transaction()));
                assertNull(f.get(71,key(2)));assertNull(f.get(72,key(200)));assertNull(f.get(73,key(2)));assertNull(f.get(74,member(200,"second")));
            }
            int calls=f.promotionSubmissions.get();
            try(var client=f.client();var reopened=client.reopen(path,RequestContext.batch())) {
                assertEquals(CommandResult.Status.SESSION_EXPIRED,reopened.completion().get(5,TimeUnit.SECONDS).status());assertEquals(calls,f.promotionSubmissions.get());assertEquals(origin,reopened.snapshot().initial().origin());
                reopened.consume(RequestContext.batch()).get(5,TimeUnit.SECONDS);assertTrue(f.homes.read(identity.transaction()).consumed());
            }
        }
    }
    @Test void possiblyPublishedJournalRetainsOriginalCapturesBeforeClientAssignment() throws Exception {
        try(var f=new Fixture(root)) {
            Path path=root.resolve("unknown-publication.bin");var identity=f.identity(0);var observed=new java.util.concurrent.atomic.AtomicBoolean();
            try(var client=new RatisAtomicClient(RatisTransactionBindings.of(f.schema,f.data.directory(f.schema),f.data),f.homes,f.scheduler,RatisAtomicClient.Limits.defaults(),published -> {
                assertEquals(path,published);assertTrue(Files.isRegularFile(published));observed.set(true);throw new IOException("Lost journal publication confirmation");
            });var operation=client.executeAtomic(path,identity,changes(),absentIdentity(),TTL,RequestContext.batch())) {
                assertInstanceOf(IOException.class,assertThrows(ExecutionException.class,()->operation.completion().get(10,TimeUnit.SECONDS)).getCause());assertTrue(observed.get());assertEquals(2,f.opens.get());assertNull(f.homes.read(identity.transaction()));
            }
            assertEquals(0,f.captureCloses.get(),"Possibly published journal retains its original remote handles");
            List<UUID> captures;
            try(var saved=RatisDurableTransaction.reopen(path,f.homes,RatisTransactionBindings.of(f.data))) {
                assertEquals(identity.business(),saved.snapshot().initial().operation());captures=saved.snapshot().participants().stream().map(part->part.intent().captureId()).toList();
            }
            try(var client=f.client();var resumed=client.reopen(path,RequestContext.batch())) {
                f.complete(resumed,CommandResult.Status.SUCCESS);assertEquals(captures,resumed.snapshot().participants().stream().map(part->part.intent().captureId()).toList());assertEquals(2,f.opens.get());assertEquals(2,f.captureCloses.get());assertArrayEquals(new byte[]{1},f.get(71,key(2)));assertArrayEquals(new byte[]{8},f.get(74,member(200,"second")));
            }
        }
    }
    @Test void separateIdentityKeepsBoundsAndRejectionJournalPreservesBothExactIds() throws Exception {
        var business=new OperationId(new UUID(117,1),1,100);var control=new OperationId(new UUID(117,2),2,0);
        var identity=new RatisAtomicClient.Identity(new UUID(117,3),business,control);
        assertEquals(new OperationId(control.sessionId(),2,255),identity.lastControl());
        assertThrows(IllegalArgumentException.class,()->new RatisAtomicClient.Identity(identity.transaction(),business,new OperationId(business.sessionId(),1,100)));
        assertThrows(IllegalArgumentException.class,()->new RatisAtomicClient.Identity(identity.transaction(),business,new OperationId(control.sessionId(),0,0)));
        assertThrows(IllegalArgumentException.class,()->new RatisAtomicClient.Identity(identity.transaction(),new OperationId(business.sessionId(),0,0),control));
        assertThrows(IllegalArgumentException.class,()->new RatisAtomicClient.Identity(identity.transaction(),business,new OperationId(control.sessionId(),2,Long.MAX_VALUE-RatisAtomicClient.CONTROL_SLOTS+1)));
        var sameSessionNewGrant=new RatisAtomicClient.Identity(identity.transaction(),business,new OperationId(business.sessionId(),2,0));assertEquals(0,sameSessionNewGrant.firstControl().sequence());
        Path path=root.resolve("separate-rejection.bin");byte[] binding=new byte[32],digest=new byte[32];digest[0]=1;
        try(var rejection=RatisAtomicRejection.create(path,binding,identity,digest)){assertEquals(identity,rejection.identity());}
        try(var restored=RatisAtomicRejection.reopen(path,binding)){assertEquals(identity,restored.identity());assertArrayEquals(digest,restored.requestDigest());restored.consume();}
        try(var restored=RatisAtomicRejection.reopen(path,binding)){assertEquals(identity,restored.identity());assertTrue(restored.consumed());}
    }
    @Test void separateBoundControlGrantCommitsOrderedBusinessAfterLostPrepareAndExactReopen() throws Exception {
        try(var f=new Fixture(root)) {
            var identity=f.separateIdentity(10);Path path=root.resolve("separate-lost.bin");List<RatisDurableTransaction.ParticipantProgress> original;
            f.losePrepareGroup=f.groups.getLast();
            try(var client=f.client();var operation=client.executeAtomic(path,identity,changes(),absentIdentity(),TTL,RequestContext.batch())) {
                assertInstanceOf(IOException.class,assertThrows(ExecutionException.class,()->operation.completion().get(10,TimeUnit.SECONDS)).getCause());
                var saved=operation.snapshot();assertEquals(identity.business(),saved.initial().operation());original=saved.participants();
                for(int n=0;n<original.size();n++) {
                    var intent=original.get(n).intent();assertEquals(identity.business(),intent.acceptedBusiness().operationId());
                    assertEquals(new OperationId(identity.firstControl().sessionId(),identity.firstControl().epoch(),n),intent.prepareControl());
                    assertEquals(new OperationId(identity.firstControl().sessionId(),identity.firstControl().epoch(),64+n),intent.finalizeControl());
                }
            }
            try(var client=f.client();var resumed=client.reopen(path,RequestContext.batch())) {
                f.complete(resumed,CommandResult.Status.SUCCESS);var saved=resumed.snapshot();assertEquals(identity.business(),saved.initial().operation());
                for(int n=0;n<original.size();n++){assertEquals(original.get(n).intent().prepareControl(),saved.participants().get(n).intent().prepareControl());assertEquals(original.get(n).intent().finalizeControl(),saved.participants().get(n).intent().finalizeControl());assertEquals(original.get(n).intent().captureId(),saved.participants().get(n).intent().captureId());}
                assertEquals(identity.firstControl().sessionId(),saved.lastHome().sessionId());assertEquals(identity.firstControl().epoch(),saved.lastHome().epoch());
                assertTrue(saved.lastHome().sequence()>=128 && saved.lastHome().sequence()<=255);
                var result=resumed.completion().get(5,TimeUnit.SECONDS);assertEquals(5,result.valueCount());assertArrayEquals(new byte[]{9},result.value(0));assertNull(result.value(1));assertNull(result.value(2));assertArrayEquals(new byte[]{7},result.value(3));assertArrayEquals(new byte[]{8},result.value(4));
                resumed.consume(RequestContext.batch()).get(5,TimeUnit.SECONDS);assertTrue(f.homes.read(identity.transaction()).consumed());assertEquals(identity.business(),f.homes.read(identity.transaction()).operation());
                assertArrayEquals(new byte[]{1},f.get(71,key(2)));assertArrayEquals(new byte[]{8},f.get(74,member(200,"second")));
            }
        }
    }
    @Test void freshSeparateControlsCannotResurrectExpiredOriginalBusinessGrant() throws Exception {
        try(var f=new Fixture(root);var client=f.client()) {
            var identity=f.separateIdentity(1000);
            assertFalse(f.catalogue.epochs.read().isLive(identity.business()));assertTrue(f.catalogue.epochs.read().isLive(identity.firstControl()));
            try(var operation=client.executeAtomic(root.resolve("expired-business.bin"),identity,changes(),absentIdentity(),TTL,RequestContext.batch())) {
                var expired=assertThrows(ExecutionException.class,()->operation.completion().get(10,TimeUnit.SECONDS));
                assertInstanceOf(RatisTransactionOutcomeExpiredException.class,expired.getCause());
                assertEquals(identity.business(),operation.snapshot().initial().operation());assertNotNull(operation.snapshot().homeRetirementProof());
                assertNull(f.get(71,key(2)));assertNull(f.get(72,key(200)));assertNull(f.get(73,key(2)));assertNull(f.get(74,member(200,"second")));
            }
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
