package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.export.ExportJobs;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Genuine Runtime/native cohorts and exclusive root recovery; Raft dispatch is controlled, without sockets/quorum. */
class RatisAggregateExportColdCleanupTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T>T await(CompletableFuture<T> future)throws Exception{return future.get(30,TimeUnit.SECONDS);}
    static Message message(byte[] bytes){return Message.valueOf(ByteString.copyFrom(bytes));}
    final class Source implements AutoCloseable {
        final RatisDatabaseBackendFactoryTest helper=new RatisDatabaseBackendFactoryTest();
        final RatisDatabaseBackendFactoryTest.TransactionFixture fixture;
        final RatisAtomicClientTest.Fixture f;
        final RatisNodePlan plan;
        final Path runtimeRoot,factoryRoot;
        final RatisNodeRuntimeTest.ManualTimer timer=new RatisNodeRuntimeTest.ManualTimer();
        final Map<UUID,RatisRoutedClient> routes=new LinkedHashMap<>();
        final AtomicInteger cohortCaptures=new AtomicInteger(),ordinaryCaptures=new AtomicInteger(),cancels=new AtomicInteger();
        final Set<UUID> cancelledProcesses=new HashSet<>();
        volatile RatisNodeRuntimeTest.FakeServer current;
        boolean denyDrain=true;
        RatisNodeRuntime node;
        RatisDatabaseBackendFactory factory;
        RatisExportBackend backend;
        Source()throws Exception {
            helper.root=Files.createDirectories(root.resolve("fixtures"));fixture=helper.new TransactionFixture();f=fixture.nativeState;
            plan=fixture.plan;
            runtimeRoot=Files.createDirectory(root.resolve("runtime"));factoryRoot=root.resolve("aggregate");
            for(var directory:List.of(f.directory,fixture.extraDirectory))routes.put(directory.domainId(),new RatisRoutedClient(directory,transport()));
            openRuntime(false);
            factory=new RatisDatabaseBackendFactory(factoryRoot,plan,f.catalogue.client,routes,f.scheduler,(name,shape)->name.equals("second-domain")?fixture.extraDomain:f.schema.domainId(),(name,type,hash,revision)->{throw new IOException("No operators in cleanup fixture");},RatisDatabaseBackendFactory.Policy.defaults(),directory->new RatisRoutedClient(directory,transport()),new RatisSchemaReplicaClient(1));
            backend=factory.exports();
        }
        void openRuntime(boolean recover)throws Exception {
            var fake=new RatisNodeRuntimeTest.FakeServer();current=fake;
            var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(8<<20,64L<<20,256),RatisRangeStateMachine.RetentionLimits.defaults(),Duration.ofSeconds(1),1,8,Duration.ofSeconds(5),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());
            node=RatisNodeRuntime.open(plan,runtimeRoot,ConfigParser.parseDefault(),f.catalogue.epochs,routes,f.scheduler,timer,()->20L,settings,recover?RatisNodeRuntime.Startup.RECOVER:RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,List.of(f.schema,fixture.extraSchema),null,null,directory->new RatisRoutedClient(directory,transport()));
            node.start();
        }
        RatisRangeStateMachine machine(UUID group)throws IOException {var fake=current;if(fake==null || !fake.machines.containsKey(RaftGroupId.valueOf(group)))throw new IOException("Actual original Runtime owner unavailable");return fake.machines.get(RaftGroupId.valueOf(group));}
        ReplicatedRangeStore store(UUID group)throws Exception{return field(machine(group),"store",ReplicatedRangeStore.class);}
        RatisRoutedClient.Transport transport(){return new RatisRoutedClient.Transport(){
            public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return ScopedTransactionRpc.pointValue(transaction(route,ScopedTransactionRpc.pointRequest(column,key),true),2<<20);}
            public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh)throws IOException{try{var target=machine(route.groupId());return (fresh?target.queryLinearizable(message(request)):target.query(message(request))).get(20,TimeUnit.SECONDS).getContent().toByteArray();}catch(Exception failed){throw new IOException("Controlled Runtime query failed",failed);}}
            public CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request)throws IOException{
                int kind=ScopedTransactionRpc.kind(request,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES);
                if(kind==RatisSnapshotCohortRpc.CAPTURE)cohortCaptures.incrementAndGet();
                if(kind==ScopedTransactionRpc.SNAPSHOT_CAPTURE)ordinaryCaptures.incrementAndGet();
                if(kind==RatisSnapshotCohortRpc.CANCEL){cancels.incrementAndGet();cancelledProcesses.add(RatisSnapshotCohortRpc.reference(ScopedTransactionRpc.parse(request,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES).body()).processIncarnation());if(denyDrain)throw new IOException("Controlled original cohort drain unavailable");}
                return machine(route.groupId()).queryLinearizable(message(request)).thenApply(reply->reply.getContent().toByteArray());
            }
            public RangeStateView stateView(RangeDescriptor route)throws IOException{return RangeStateView.decode(ScopedTransactionRpc.parse(transaction(route,ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]),true),ScopedTransactionRpc.MAX_REPLY_BYTES).body());}
            public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{return CommandResult.decode(append(route,DistributedCommandCodec.encode(command)));}
            public byte[] participant(RangeDescriptor route,byte[] bytes)throws IOException{return append(route,bytes);}
            public void close(){}
        };}
        synchronized byte[] append(RangeDescriptor route,byte[] bytes)throws IOException {
            try {
                var target=machine(route.groupId());var context=target.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(route.groupId())).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(bytes)).build());
                if(context.getException()!=null)throw new IOException("Controlled Runtime admission rejected",context.getException());
                target.preAppendTransaction(context);context.initLogEntry(1,store(route.groupId()).applied().index()+1);return target.applyTransaction(context).get(20,TimeUnit.SECONDS).getContent().toByteArray();
            }catch(IOException failed){throw failed;}catch(Exception failed){throw new IOException(failed);}
        }
        RatisAggregateExportJournal parent()throws Exception{var parents=field(backend,"parents",Map.class);assertEquals(1,parents.size());return (RatisAggregateExportJournal)parents.values().iterator().next();}
        DistributedStoragePool pool()throws Exception {var pools=field(node,"pools",Map.class);return field(pools.get(RatisPoolFixtures.ID),"pool",DistributedStoragePool.class);}
        long issued()throws Exception{return f.catalogue.epochs.read().nextEpoch();}
        void advancePublishedDirectories()throws Exception {
            var catalogue=new RatisCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            long sequence=1;
            for(var domain:routes.keySet().stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList()) {
                var routed=routes.get(domain);var old=routed.directory();var next=new RangeDirectory(old.clusterId(),old.databaseId(),old.domainId(),old.schemaEpoch(),old.schemaDigest(),old.generation()+1,old.columns(),old.routingPrefixBytes(),old.ranges());
                assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(new OperationId(new UUID(2000,1),1,sequence++),old,next)).status());routed.refresh(next);
            }
        }
        public void close()throws Exception{denyDrain=false;if(factory!=null)factory.close();if(node!=null)node.close();for(var route:routes.values())route.close();fixture.close();}
    }
    @Test void establishedRuntimeRootGoneAuthorityDrainsOriginalPendingParentAfterCurrentDirectoryAdvances()throws Exception {
        try(var source=new Source()) {
            var outside=root.resolve("other-owner.sentinel");byte[] protectedBytes=new byte[]{9,8,7};Files.write(outside,protectedBytes);
            var created=await(source.backend.createExport(RatisDatabaseExportWorkflowTest.request(new UUID(2000,2),List.of(74L,75L)),RequestContext.batch()));
            for(int visit=0;visit<128;visit++){await(source.backend.stepExport(created.jobId(),RequestContext.batch()));var saved=source.parent();if(saved.hasBarrier() && saved.snapshot().phase()==RatisDatabaseBackupJournal.Phase.REPORTED)break;}
            var parent=source.parent();assertEquals(RatisDatabaseBackupJournal.Phase.REPORTED,parent.snapshot().phase());var captures=parent.captures();assertNotNull(captures);assertEquals(2,parent.slots().size());assertEquals(1,captures.cohorts().size());assertEquals(1,source.cohortCaptures.get());assertEquals(0,source.ordinaryCaptures.get());
            var oldProcess=captures.cohorts().getFirst().processIncarnation();var request=parent.snapshot().requests().getFirst();var reference=RatisSnapshotCohortRpc.reference(request);
            var service=RatisPoolFixtures.cohortService(source.node);assertEquals(RatisSnapshotCohortRpc.Phase.CAPTURED,service.state(reference).phase());
            var pool=source.pool();assertEquals(1,pool.snapshotAdmissionStats().pins());
            byte[] definition=parent.input().encode(),witness=parent.captureBytes();var slots=List.copyOf(parent.slots());long issued=source.issued();
            var bindings=field(source.factory,"exportBindings",Map.class);assertEquals(slots.size(),bindings.size());
            for(var slot:slots){var binding=bindings.get(slot.fence().fenceId());var journal=field(binding,"journal",RatisDurableSnapshot.class);assertEquals(slot.fence(),journal.snapshot().initial().fence());assertTrue(journal.snapshot().participants().stream().allMatch(participant->participant.captureToken()!=null));assertTrue(Files.isRegularFile(slot.snapshot()));}
            assertThrows(ExecutionException.class,()->await(source.backend.cancelExport(created.jobId(),RequestContext.batch())));assertThrows(IOException.class,source.factory::close);assertEquals(1,pool.snapshotAdmissionStats().pins());
            source.node.close();source.node=null;assertEquals(0,pool.snapshotAdmissionStats().pins());
            source.openRuntime(true);var currentProcess=field(source.node,"snapshotProcessIncarnation",UUID.class);assertNotEquals(oldProcess,currentProcess);
            var newService=RatisPoolFixtures.cohortService(source.node);assertEquals(RatisSnapshotCohortRpc.Phase.GONE,newService.state(reference).phase());
            var foreign=new RatisSnapshotCohortRpc.Reference(reference.cohortId(),UUID.randomUUID(),reference.processIncarnation(),reference.poolId(),reference.digest());assertThrows(RatisSnapshotLostException.class,()->newService.cancel(foreign));
            source.advancePublishedDirectories();source.denyDrain=false;
            // Close already began while the old cohort owner was unavailable. Retry that same owner,
            // rather than dispatching fresh API requests through its permanently stopped admission gate.
            source.factory.close();source.factory=null;
            assertTrue(field(source.backend,"parents",Map.class).isEmpty());assertTrue(field(source.backend,"drivers",Map.class).isEmpty());
            try(var recovered=RatisAggregateExportJournal.reopen(parent.path())) {
                assertEquals(ExportJobs.Phase.CANCELLED,recovered.phase());assertTrue(recovered.drained());
                assertArrayEquals(definition,recovered.input().encode());assertArrayEquals(witness,recovered.captureBytes());assertEquals(slots,recovered.slots());
                try(var homeRoute=new RatisRoutedClient(source.f.snapshotDirectory,source.transport())) {
                    var homes=new RatisSnapshotFences(homeRoute,source.f.snapshotColumn);
                    for(var slot:slots) {
                        var domain=recovered.input().domain(slot.domainId());
                        try(var frozen=source.routes.get(slot.domainId()).borrowedSnapshotCleanupView(domain.directory(),slot.fence());var journal=RatisDurableSnapshot.reopen(slot.snapshot(),homes,frozen)) {
                            var saved=journal.snapshot();assertEquals(slot.fence(),saved.initial().fence());assertEquals(RatisSnapshotCaptureRequest.Mode.READ_ONLY,saved.captureMode());assertEquals(recovered.input().limits().snapshotTtl(),saved.requestedTtl());
                            assertNotNull(saved.observedHome());assertTrue(saved.observedHome().consumed());assertTrue(saved.releaseRequested());
                            for(var participant:saved.participants()) {
                                assertNotNull(participant.captureToken());assertEquals(captures.captures().stream().filter(status->status.fence().fenceId().equals(slot.fence().fenceId())).findFirst().orElseThrow().progress(participant.route().groupId()).capture(),participant.captureToken());assertTrue(participant.captureReleased()||participant.captureReleaseLost(),"Original alias cleanup must be durably settled");
                                assertEquals(domain.directory().ranges().stream().filter(range->range.groupId().equals(participant.route().groupId())).findFirst().orElseThrow(),participant.route());
                                assertTrue(participant.nativeGate()!=null && participant.nativeGate().released()!=null || participant.retirementProof()!=null,"Original native gate must be released or retired");
                            }
                        }
                    }
                }
            }
            assertEquals(issued,source.issued());assertEquals(1,source.cohortCaptures.get());assertEquals(0,source.ordinaryCaptures.get());assertEquals(Set.of(oldProcess),source.cancelledProcesses);
            assertTrue(field(RatisPoolFixtures.cohortService(source.node),"entries",Map.class).isEmpty());assertEquals(0,source.pool().snapshotAdmissionStats().pins());
            assertArrayEquals(protectedBytes,Files.readAllBytes(outside));assertNotNull(source.f.catalogue.epochs.read(),"Factory cleanup must leave borrowed catalogue ownership usable");
        }
    }
}
