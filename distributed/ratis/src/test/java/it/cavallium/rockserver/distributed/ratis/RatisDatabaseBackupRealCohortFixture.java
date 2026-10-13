package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real cohort and remote SST wire/service kernels over actual native pools; controlled HOME journals, no sockets/quorum. */
final class RatisDatabaseBackupRealCohortFixture implements AutoCloseable {
    final RatisDatabaseBackupDriverFixture base;final List<Endpoint> endpoints=new ArrayList<>();final Map<UUID,RatisDatabaseBackupDriver.Binding> bindings=new HashMap<>();
    final Map<UUID,RatisRoutedClient> groups=new HashMap<>();final Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> frozen=new HashMap<>();final RatisSnapshotCohortClient cohorts;
    volatile boolean loseReply,denyDrain;
    interface Control {byte[] transaction(UUID group,int kind,byte[] body)throws Exception;byte[] participant(UUID group,byte[] body)throws Exception;CommandResult session(UUID group,DistributedCommand command)throws Exception;}
    volatile Control control;
    final class Endpoint implements RatisSnapshotCohortService.Host,AutoCloseable {
        final RatisDatabaseBackupDriverFixture.Pool state;final RatisNodeResourceBudget graph;final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,128,1,"real-backup-cohort");
        final Map<UUID,RatisSnapshotPins> registries=new HashMap<>();RatisSnapshotCohortService service;final RatisExportSourceService exports;final Set<String> gone=new HashSet<>();UUID currentProcess;
        final RatisRoutedClient routed,homeRouted;final RatisSnapshotClient snapshots;
        Endpoint(RatisDatabaseBackupDriverFixture.Pool state,int index)throws Exception{
            this.state=state;currentProcess=state.processId;graph=new RatisNodeResourceBudget(RatisNodeRuntime.ResourceLimits.defaults().node(),Map.of(state.poolId,RatisNodeRuntime.ResourceLimits.defaults().pool()));
            for(var group:state.stores.keySet())registries.put(group,new RatisSnapshotPins(state.registryOwners.get(group),group,new RatisSnapshotPins.Limits(16,16L*1024*1024,base.policy.retention()),System::nanoTime,(homes,fence)->{throw new AssertionError("Ordinary capture factory must never run");}));
            service=new RatisSnapshotCohortService(this,scheduler,RatisSnapshotCohortService.Limits.defaults(),System::nanoTime,graph);
            var budgets=RatisExportSourceRegistry.Budgets.defaults();exports=new RatisExportSourceService(base.root.resolve("remote-"+index),budgets,new RatisExportSourceService.IoLimits(0,64L*1024*1024,1024*1024),new RatisExportSourceRegistry.NativeBuilder(){
                public NativeRangeSstExport.Chunk build(RatisExportSourceRpc.JobDefinition definition,RatisExportJournal.ChunkIntent intent,Path artifact,RatisExportSourceRegistry.Completion complete)throws Exception{
                    return registries.get(definition.group()).export(definition.capture(),pin->{var result=state.nativePool.exportSstChunk(artifact,pin,intent.column(),intent.scopeStart(),definition.scope(intent.column()).upperExclusive(),intent.limits());complete.persist(result);return result;});
                }
                public boolean tokenUsable(RatisExportSourceRpc.JobDefinition definition)throws Exception{try{registries.get(definition.group()).requireExport(definition.capture(),definition.fence());return true;}catch(RatisSnapshotLostException lost){return false;}}
            },scheduler);
            var transport=new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor r,UUID col,byte[] key){throw new AssertionError("Unexpected source read");}public CommandResult command(RangeDescriptor r,DistributedCommand c)throws IOException{try{if(control!=null && c.sessionControl()!=null)return control.session(r.groupId(),c);throw new AssertionError("Unexpected source write");}catch(Exception failure){throw new IOException(failure);}}
                public byte[] transaction(RangeDescriptor r,byte[] bytes,boolean fresh)throws IOException{try{var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);if(packet.kind()==ScopedTransactionRpc.SNAPSHOT_OWNER)return ScopedTransactionRpc.frame(packet.kind(),service.owner(r.groupId(),null).encode());if(control!=null)return ScopedTransactionRpc.frame(packet.kind(),control.transaction(r.groupId(),packet.kind(),packet.body()));throw new AssertionError("Unexpected transaction query");}catch(Exception failure){throw new IOException(failure);}}
                public byte[] participant(RangeDescriptor r,byte[] body)throws IOException{try{if(control==null)throw new AssertionError("Unexpected gate apply");return control.participant(r.groupId(),body);}catch(Exception failure){throw new IOException(failure);}}
                public byte[] snapshot(RangeDescriptor r,ReplicaDescriptor owner,byte[] bytes)throws IOException{
                    assertEquals(state.owner,owner);try{var packet=ScopedTransactionRpc.parse(bytes,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES);byte[] response;
                        if(packet.kind()>=RatisSnapshotCohortRpc.OWNER && packet.kind()<=RatisSnapshotCohortRpc.CANCEL){if(packet.kind()==RatisSnapshotCohortRpc.CANCEL && denyDrain)throw new IOException("Actual cohort drain blocked");response=service.query(r.groupId(),packet.kind(),packet.body()).get(20,TimeUnit.SECONDS);if(packet.kind()==RatisSnapshotCohortRpc.CAPTURE && loseReply){loseReply=false;throw new IOException("Lost real cohort reply");}}
                        else if(packet.kind()>=RatisExportSourceService.BUILD && packet.kind()<=RatisExportSourceService.RELEASE)response=exports.submit(r.groupId(),packet.kind(),packet.body()).get(20,TimeUnit.SECONDS);
                        else if(packet.kind()==ScopedTransactionRpc.SNAPSHOT_RELEASE){var input=ByteBuffer.wrap(packet.body());var binding=ScopedTransactionRpc.snapshotBinding(input);var token=ScopedTransactionRpc.snapshotToken(input);registries.get(r.groupId()).requireBinding(token,binding);registries.get(r.groupId()).release(token);response=ScopedTransactionRpc.snapshotReply(binding,token,new byte[0]);}
                        else throw new AssertionError("Unexpected frozen snapshot operation");return ScopedTransactionRpc.frame(packet.kind(),response);
                    }catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException(failure);}
                }
            };
            routed=new RatisRoutedClient(state.directory,transport);UUID homeColumn=UUID.randomUUID();var homeRange=new RangeDescriptor(UUID.randomUUID(),state.fence.homeGroup(),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(state.owner));
            homeRouted=new RatisRoutedClient(new RangeDirectory(base.cluster,base.database,UUID.randomUUID(),0,new byte[32],0,Set.of(homeColumn),0,List.of(homeRange)),transport);
            snapshots=new RatisSnapshotClient(state.schema,new RatisSnapshotFences(homeRouted,homeColumn),routed,scheduler,RatisSnapshotClient.Limits.defaults());
            var old=base.bindings.get(state.fence.fenceId());var d=old.exportDefinition();var definition=new RatisExportJournal.Definition(d.jobId(),d.fence(),d.columns(),d.sourceJournal(),snapshots.identityDigest(),d.limits(),d.snapshotTtl());
            bindings.put(state.fence.fenceId(),new RatisDatabaseBackupDriver.Binding(state.durable,state.coordinator,routed,old.exportPath(),definition,new RatisRemoteExportSource(snapshots,routed)));
            for(var group:state.stores.keySet())groups.put(group,routed);
            doAnswer(call->{var p=(RatisRoutedClient.PreparedSnapshotCapture)call.getArgument(0);var id=p.target().descriptor().groupId();var prior=state.prepared.putIfAbsent(id,p);if(prior!=null)assertArrayEquals(prior.bytes(),p.bytes());frozen.put(id,p);state.save();return null;}).when(state.durable).freezeCapture(any());
            when(state.durable.releaseCapture(any())).thenAnswer(call->{UUID group=call.getArgument(0);registries.get(group).release(state.accepted.get(group));state.released.add(group);state.save();return RatisDurableSnapshot.CaptureReleaseProgress.RELEASED;});
        }
        public UUID nodeId(){return state.owner.nodeId();}public UUID nodeIncarnation(){return state.owner.nodeIncarnation();}public UUID processIncarnation(){return currentProcess;}public UUID poolId(){return state.poolId;}public DistributedStoragePool pool(){return state.nativePool;}public PoolSnapshotAdmission.Limits poolLimits(){return new PoolSnapshotAdmission.Limits(1,1L<<30,Duration.ofMinutes(5),0);}
        public RatisSnapshotCohortService.Member member(UUID group,SnapshotFence fence,boolean ready)throws Exception{
            var store=state.stores.get(group);if(store==null)throw new IOException("Unknown actual member");if(fence!=null && (!fence.domainId().equals(state.domain) || !Arrays.equals(fence.schemaDigest(),state.schema.digest())))throw new IOException("Foreign fence");
            if(ready && fence!=null && !store.snapshotGate(fence.fenceId()).active())throw new IOException("Actual native gate is not READY");
            return new RatisSnapshotCohortService.Member(store,registries.get(group),new RatisSnapshotOwner(group,1,state.schema.digest(),state.registryOwners.get(group),state.owner.peerId(),null),state.owner,0,1,RangeImportAdmission.sha256(store.configuration().data()),store.applied());
        }
        public boolean originalGone(RatisSnapshotCohortRpc.Reference original){return !original.processIncarnation().equals(currentProcess) && gone.contains(HexFormat.of().formatHex(RatisSnapshotCohortRpc.reference(original))) && state.nativePool.snapshotAdmissionStats().pins()==0;}
        void restartAfterDrain(RatisDatabaseBackupJournal.CohortRequest original)throws Exception{
            service.close();assertEquals(0,state.nativePool.snapshotAdmissionStats().pins());assertEquals(0,graph.stats().protectedLane().bytes());gone.add(HexFormat.of().formatHex(RatisSnapshotCohortRpc.reference(RatisSnapshotCohortRpc.reference(original))));currentProcess=UUID.randomUUID();service=new RatisSnapshotCohortService(this,scheduler,RatisSnapshotCohortService.Limits.defaults(),System::nanoTime,graph);
        }
        public void close()throws Exception{denyDrain=false;exports.stop().get(20,TimeUnit.SECONDS);service.close();for(var registry:registries.values())registry.close();snapshots.close();routed.close();homeRouted.close();scheduler.disposeGracefully().block(Duration.ofSeconds(10));}
    }
    RatisDatabaseBackupRealCohortFixture(Path root,int first,int second)throws Exception{this(root,first,second,false);}
    RatisDatabaseBackupRealCohortFixture(Path root,int first,int second,boolean sharedPool)throws Exception{base=new RatisDatabaseBackupDriverFixture(root,first,second,sharedPool);for(int i=0;i<base.pools.size();i++)endpoints.add(new Endpoint(base.pools.get(i),i));cohorts=new RatisSnapshotCohortClient(groups,frozen::get);}
    RatisDatabaseBackupDriver driver(RatisDatabaseBackupJournal journal,RatisDatabaseBackupArchive archive)throws IOException{return new RatisDatabaseBackupDriver(journal,archive,()->new RatisDatabaseBackupDriver.Vector(base.definition.roster(),base.definition.domains()),cohorts,operator->{throw new IOException("No operator");},bindings);}
    @Override public void close()throws Exception{for(var endpoint:endpoints)endpoint.close();base.close();}
}
