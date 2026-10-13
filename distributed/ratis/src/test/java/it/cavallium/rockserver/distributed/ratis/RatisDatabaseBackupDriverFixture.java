package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Actual native pool cohorts and exports; controlled forced subjournal/HOME dispatch, no quorum or sockets. */
final class RatisDatabaseBackupDriverFixture implements AutoCloseable,RatisDatabaseBackupDriver.CohortTransport {
    final Path root;final UUID cluster=UUID.randomUUID(),database=UUID.randomUUID();final List<Pool> pools=new ArrayList<>();
    final DatabaseBackupDefinition definition;final DatabaseBackupManifest.Destination destination=new DatabaseBackupManifest.Destination(UUID.randomUUID(),UUID.randomUUID());
    final RatisDatabaseBackupDriver.Policy policy=new RatisDatabaseBackupDriver.Policy(Duration.ofMinutes(2),Duration.ofSeconds(105),4);
    final Map<UUID,RatisDatabaseBackupDriver.Binding> bindings=new HashMap<>();final AtomicReference<RatisDatabaseBackupJournal> global=new AtomicReference<>();
    final AtomicInteger reports=new AtomicInteger(),physical=new AtomicInteger();final AtomicBoolean vectorForced=new AtomicBoolean();boolean lostReply,changeVector;
    final CountDownLatch exportEntered=new CountDownLatch(1),exportExit=new CountDownLatch(1);volatile boolean blockExport;final ExecutorService executor=Executors.newSingleThreadExecutor();
    final class Pool {
        final UUID domain=UUID.randomUUID(),column=UUID.randomUUID(),poolId=UUID.randomUUID(),processId=UUID.randomUUID();
        final ReplicaDescriptor owner=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"owner"+pools.size(),"localhost:"+(20000+pools.size()),ReplicaDescriptor.Role.VOTER);
        final DistributedStoragePool nativePool;final DistributedSchema schema;final RangeDirectory directory;final SnapshotFence fence;SnapshotFenceStatus home;
        final Map<UUID,UUID> registryOwners=new HashMap<>();final Map<UUID,ReplicatedRangeStore> stores=new HashMap<>();final Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> prepared=new HashMap<>();
        final Map<UUID,SnapshotFenceStatus.Capture> tokens=new HashMap<>(),accepted=new HashMap<>();final Map<UUID,ReplicatedRangeStore.ScopedSnapshot> scoped=new HashMap<>();final Set<UUID> released=new HashSet<>();
        final RatisDurableSnapshot durable=mock(RatisDurableSnapshot.class);final RatisSnapshotCoordinator coordinator=mock(RatisSnapshotCoordinator.class);
        final RatisRoutedClient routed;final Path sourcePath;DistributedStoragePool.SnapshotCohort cohort;boolean started,releaseRequested;int gateChecks;
        Pool(int ordinal,int count)throws Exception{
            nativePool=DistributedStoragePool.open(root.resolve("native-"+ordinal),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            nativePool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(1,1L<<30,Duration.ofMinutes(5),0));
            schema=new DistributedSchema(cluster,database,domain,0,0,List.of(new DistributedSchema.Column(ordinal+1,column,"data"+ordinal,ColumnSchema.of(IntArrayList.of(2),new ObjectArrayList<>(),true),null)));
            var ranges=new ArrayList<RangeDescriptor>();UUID session=UUID.randomUUID();
            for(int i=0;i<count;i++){
                UUID group=UUID.randomUUID();byte[] lo=i==0?null:key(i),hi=i==count-1?null:key(i+1);var store=nativePool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,Set.of(column),lo,hi),schema.digest(),Map.of(column,"shared"),Set.of());
                store.enableLogicalSchema(schema);store.initializeConfiguration(new byte[]{1});store.enableScopedHistory(RangeMutationHistory.Limits.defaults());store.applyNoop(new ApplyPosition(1,0));store.apply(new ApplyPosition(1,1),new DistributedCommand(group,1,schema.digest(),new OperationId(session,1,i),List.of(new Mutation(Mutation.Kind.PUT,column,key(i),new byte[]{(byte)i}))));stores.put(group,store);registryOwners.put(group,UUID.randomUUID());
                ranges.add(new RangeDescriptor(group,group,1,0,lo,hi,RangeDescriptor.TopologyState.ACTIVE,List.of(owner)));
            }
            directory=new RangeDirectory(cluster,database,domain,0,schema.digest(),0,Set.of(column),0,ranges);
            var participants=ranges.stream().map(r->new SnapshotFence.Participant(r.rangeId(),r.groupId(),1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),new OperationId(UUID.randomUUID(),1,0),new TransactionScope(List.of(TransactionScope.Span.range(column,r.lowerInclusive(),r.upperExclusive()))))).toList();
            fence=new SnapshotFence(cluster,database,domain,UUID.randomUUID(),UUID.randomUUID(),1,new OperationId(UUID.randomUUID(),1,0),0,0,schema.digest(),participants);home=SnapshotFenceStatus.begin(fence);
            for(var p:participants){stores.get(p.groupId()).applyGateInstall(new ApplyPosition(1,2),fence);home=home.ready(p.groupId(),new ApplyPosition(1,2));}
            sourcePath=root.resolve("source-"+ordinal);
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] k){throw new AssertionError("Unexpected read");}public CommandResult command(RangeDescriptor r,DistributedCommand c){throw new AssertionError("Unexpected command");}public byte[] transaction(RangeDescriptor r,byte[] bytes,boolean fresh)throws IOException{var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);if(packet.kind()!=ScopedTransactionRpc.SNAPSHOT_OWNER)throw new AssertionError("Unexpected owner lookup");return ScopedTransactionRpc.frame(packet.kind(),new RatisSnapshotOwner(r.groupId(),1,schema.digest(),registryOwners.get(r.groupId()),owner.peerId(),null).encode());}public void close(){}});
            when(durable.snapshot()).thenAnswer(call->snapshot());when(durable.mode()).thenReturn(RatisSnapshotCaptureRequest.Mode.READ_ONLY);
            doAnswer(call->{var p=(RatisRoutedClient.PreparedSnapshotCapture)call.getArgument(0);var old=prepared.putIfAbsent(p.target().descriptor().groupId(),p);if(old!=null)assertArrayEquals(old.bytes(),p.bytes());save();return null;}).when(durable).freezeCapture(any());
            when(durable.acceptExternalCapture(any(),any(),any())).thenAnswer(call->{assertTrue(vectorForced.get(),"Global vector must be forced before local acceptance");UUID group=call.getArgument(0);var result=(RatisSnapshotCaptureRequest.Result)call.getArgument(2);var old=accepted.putIfAbsent(group,result.token());if(old==null)save();else assertEquals(old,result.token());return result.token();});
            when(coordinator.step(any())).thenAnswer(call->{var stop=(RatisSnapshotCoordinator.StopAt)call.getArgument(0);if(stop==RatisSnapshotCoordinator.StopAt.GATES_READY){gateChecks++;return new RatisSnapshotCoordinator.Step(RatisSnapshotCoordinator.Action.GATES_READY,null,false);}if(!home.terminal()){home=home.abort();save();return new RatisSnapshotCoordinator.Step(RatisSnapshotCoordinator.Action.ABORTED,null,false);}
                for(var store:stores.values())if(store.snapshotGate(fence.fenceId()).released()==null){store.applyGateRelease(new ApplyPosition(1,store.applied().index()+1),home);return new RatisSnapshotCoordinator.Step(RatisSnapshotCoordinator.Action.RELEASED,store.ownership().owner().groupId(),false);}
                return new RatisSnapshotCoordinator.Step(RatisSnapshotCoordinator.Action.COMPLETE,null,true);});
            when(coordinator.reportExternalCaptures(any())).thenAnswer(call->{assertTrue(vectorForced.get());assertEquals(RatisDatabaseBackupJournal.Phase.CAPTURED,global.get().snapshot().phase());reports.incrementAndGet();var expected=(SnapshotFenceStatus)call.getArgument(0);UUID missing=home.progress().stream().filter(p->p.capture()==null).map(SnapshotFenceStatus.Progress::groupId).findFirst().orElse(null);if(missing!=null){home=home.capture(missing,expected.progress(missing).capture());if(home.progress().stream().allMatch(p->p.capture()!=null)){home=home.complete();for(var store:stores.values())store.applyGateRelease(new ApplyPosition(1,3),home);}save();}return new RatisSnapshotCoordinator.Step(RatisSnapshotCoordinator.Action.CAPTURE_REPORTED,missing,false);});
            when(durable.releaseCapture(any())).thenAnswer(call->{UUID group=call.getArgument(0);var scope=scoped.remove(group);if(scope!=null)scope.close();released.add(group);if(released.size()==stores.size() && cohort!=null){cohort.close();cohort=null;}save();return RatisDurableSnapshot.CaptureReleaseProgress.RELEASED;});
            doAnswer(call->{releaseRequested=true;save();return null;}).when(durable).requestRelease();save();
            var limits=new RatisExportJournal.Limits(4*count,64L*1024*1024,16L*1024*1024,new NativeRangeSstExport.Limits(32,1024*1024,1024*1024,Duration.ofSeconds(5)));
            var exportDefinition=new RatisExportJournal.Definition(UUID.randomUUID(),fence,List.of(column),sourcePath,new byte[32],limits,policy.retention());
            var writer=new RatisExportClient.ChunkSource(){public void bind(RatisExportJournal.Snapshot s){assertEquals(fence,s.definition().fence());}public CompletableFuture<NativeRangeSstExport.Chunk> write(RatisExportSourceRpc.BuildRequest request,Path artifact,RequestContext context){return CompletableFuture.supplyAsync(()->{try{if(blockExport){exportEntered.countDown();exportExit.await();}var intent=request.intent();var scope=scoped.get(intent.group());if(scope==null)throw new RatisSnapshotLostException("Original native cohort pin lost");return nativePool.exportSstChunk(artifact,scope,column,intent.scopeStart(),request.definition().scope(column).upperExclusive(),intent.limits());}catch(Exception failure){throw new CompletionException(failure);}},executor);}public CompletableFuture<Void> release(RatisExportSourceRpc.JobDefinition d,RequestContext c){try{assertTrue(global.get().snapshot().chunks().stream().anyMatch(chunk->chunk.groupId().equals(d.group())),"Copied chunk progress must be forced before release");return CompletableFuture.completedFuture(null);}catch(IOException failure){return CompletableFuture.failedFuture(failure);}}};
            bindings.put(fence.fenceId(),new RatisDatabaseBackupDriver.Binding(durable,coordinator,routed,root.resolve("export-"+ordinal),exportDefinition,writer));
        }
        RatisDurableSnapshot.Snapshot snapshot()throws Exception{
            var progress=new ArrayList<RatisDurableSnapshot.ParticipantProgress>();for(var r:directory.ranges()){var gate=stores.get(r.groupId()).snapshotGate(fence.fenceId());progress.add(new RatisDurableSnapshot.ParticipantProgress(r,true,null,gate,new ApplyPosition(1,2),prepared.get(r.groupId()),accepted.get(r.groupId()),null,false,released.contains(r.groupId()),false,gate.released()!=null,gate.released(),null));}
            return new RatisDurableSnapshot.Snapshot(SnapshotFenceStatus.begin(fence),home,null,null,false,fence.operation(),progress,null,releaseRequested,policy.retention(),RatisSnapshotCaptureRequest.Mode.READ_ONLY);
        }
        void save()throws IOException{var fields=new HashMap<String,byte[]>();fields.put("home",home.encode());prepared.forEach((id,p)->fields.put("capture/"+id,p.bytes()));accepted.forEach((id,t)->fields.put("token/"+id,java.nio.ByteBuffer.allocate(48).putLong(t.ownerIncarnation().getMostSignificantBits()).putLong(t.ownerIncarnation().getLeastSignificantBits()).putLong(t.snapshotId().getMostSignificantBits()).putLong(t.snapshotId().getLeastSignificantBits()).putLong(t.position().term()).putLong(t.position().index()).array()));RatisSchemaClientJournal.write(sourcePath,fields);}
    }
    RatisDatabaseBackupDriverFixture(Path root,int first,int second)throws Exception{
        this.root=root;Files.createDirectories(root);pools.add(new Pool(0,first));pools.add(new Pool(1,second));
        var domains=pools.stream().map(p->new DatabaseBackupDefinition.Domain(p.directory,p.schema,List.of(p.fence))).toList();int ranges=first+second;
        definition=new DatabaseBackupDefinition(UUID.randomUUID(),new DatabaseDomainRoster(cluster,database,0,domains.stream().map(d->d.directory().domainId()).toList()),domains,new DatabaseBackupDefinition.Limits(ranges,ranges,4*ranges,64L*1024*1024,16*1024*1024));
    }
    static byte[] key(int n){return new byte[]{(byte)n,0};}
    RatisDatabaseBackupJournal journal(boolean reopen,boolean failAfterVector)throws IOException{
        RatisDatabaseBackupJournal.Force force=(path,bytes)->{SessionJournalFiles.write(path,bytes,RatisDatabaseBackupJournal.MAX_BYTES);int offset=68+definition.encode().length;int phase=java.nio.ByteBuffer.wrap(bytes).getInt(offset);if(phase==RatisDatabaseBackupJournal.Phase.CAPTURED.ordinal()){vectorForced.set(true);if(failAfterVector)throw new IOException("Unknown global force outcome");}};
        var journal=new RatisDatabaseBackupJournal(root.resolve("global"),definition,destination,policy,reopen,force);global.set(journal);return journal;
    }
    RatisDatabaseBackupDriver driver(RatisDatabaseBackupJournal journal,RatisDatabaseBackupArchive archive)throws IOException{return new RatisDatabaseBackupDriver(journal,archive,()->new RatisDatabaseBackupDriver.Vector(changeVector?new DatabaseDomainRoster(cluster,database,1,definition.roster().domainIds()):definition.roster(),definition.domains()),this,operator->{throw new IOException("No operator");},bindings);}
    @Override public void requireSupported(DatabaseBackupDefinition d,RatisDatabaseBackupDriver.Policy p)throws IOException{if(p.retention().compareTo(Duration.ofMinutes(5))>0 || d.domains().size()!=pools.size() || pools.stream().anyMatch(pool->pool.stores.size()>1024))throw new IOException("Actual native pool envelope unsupported");}
    @Override public List<RatisDatabaseBackupJournal.CohortRequest> qualify(DatabaseBackupDefinition d,Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> frozen,RatisDatabaseBackupDriver.Policy policy)throws IOException{
        assertTrue(pools.stream().allMatch(p->p.gateChecks>0));var requests=new ArrayList<RatisDatabaseBackupJournal.CohortRequest>();for(var pool:pools){var fields=RatisSchemaClientJournal.read(pool.sourcePath);var members=new ArrayList<RatisDatabaseBackupJournal.Member>();for(var group:pool.stores.keySet()){var p=frozen.get(group);assertArrayEquals(p.bytes(),fields.get("capture/"+group));members.add(new RatisDatabaseBackupJournal.Member(pool.fence.fenceId(),group,p.request().snapshotId(),p.request().owner(),pool.owner.replicaId(),policy.retention().toMillis(),RangeImportAdmission.sha256(p.bytes())));}requests.add(new RatisDatabaseBackupJournal.CohortRequest(UUID.randomUUID(),pool.owner.nodeIncarnation(),pool.processId,pool.poolId,members));}return requests;
    }
    Pool pool(RatisDatabaseBackupJournal.CohortRequest request){return pools.stream().filter(p->p.poolId.equals(request.poolId())).findFirst().orElseThrow();}
    @Override public Map<UUID,RatisSnapshotCaptureRequest.Result> capture(RatisDatabaseBackupJournal.CohortRequest request,Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> frozen)throws IOException{
        var pool=pool(request);try{
            if(pool.started && pool.cohort==null)throw new RatisSnapshotLostException("Cold original cohort pin lost");
            if(!pool.started){pool.cohort=pool.nativePool.captureGateCohort(pool.stores.values().stream().map(s->new DistributedStoragePool.GateCaptureRequest(s,pool.fence)).toList());pool.started=true;physical.incrementAndGet();for(var m:request.members()){var pin=pool.cohort.pins().get(m.groupId());pool.scoped.put(m.groupId(),pool.stores.get(m.groupId()).scopeCohortPin(pin,pool.fence));pool.tokens.put(m.groupId(),new SnapshotFenceStatus.Capture(m.owner(),m.snapshotId(),pin.position()));}}
            var results=new HashMap<UUID,RatisSnapshotCaptureRequest.Result>();for(var m:request.members()){var token=pool.tokens.get(m.groupId());if(!token.snapshotId().equals(m.snapshotId()))throw new IOException("Changed original capture retry");results.put(m.groupId(),new RatisSnapshotCaptureRequest.Result(token,null));}
            if(lostReply){lostReply=false;throw new IOException("Lost original cohort reply");}return results;
        }catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException(failure);}
    }
    @Override public void requireLive(RatisDatabaseBackupJournal.CohortRequest request,Map<UUID,SnapshotFenceStatus.Capture> tokens)throws IOException{var p=pool(request);if(p.cohort==null)throw new RatisSnapshotLostException("Original cohort disappeared");try{for(var m:request.members()){p.scoped.get(m.groupId()).requireLive();assertEquals(p.tokens.get(m.groupId()),tokens.get(m.groupId()));}}catch(IllegalStateException failure){throw new RatisSnapshotLostException("Original cohort expired");}}
    @Override public void cancelAndDrain(RatisDatabaseBackupJournal.CohortRequest request)throws IOException{var p=pool(request);p.scoped.values().forEach(ReplicatedRangeStore.ScopedSnapshot::close);p.scoped.clear();if(p.cohort!=null){p.cohort.close();p.cohort=null;}}
    @Override public void close()throws Exception{exportExit.countDown();executor.shutdown();assertTrue(executor.awaitTermination(10,TimeUnit.SECONDS));for(var p:pools){p.scoped.values().forEach(ReplicatedRangeStore.ScopedSnapshot::close);if(p.cohort!=null)p.cohort.close();p.routed.close();p.nativePool.close();}}
}
